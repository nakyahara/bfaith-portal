// ai-insights: 事実層 (週次レポート入力 JSON の組み立て)
//
// 大原則 (要件 §2): 事実は決定的 SQL で全て計算済みにして AI に渡す。AI に算術させない。
// このモジュールは読み取りのみ (mirror への書き込みなし)。
//
// 充足判定 (要件 §5.3/§5.4):
// - 「件数>0」は使わない。取得完了マーカー = sync_run_chunks (entity×scope×applied_at) で
//   「0件だが取得完了」と「取込欠損」を区別する
// - 売上/finance は per-mall 判定。1モール欠損 = 全停止ではなく該当論点の生成禁止に落とす
// - 在庫 (旧 /api/sync 経由で chunk 台帳なし) は mirror_inv_daily_summary の
//   (business_date, category) 行の存在自体を完了マーカーとみなす (行は同期時に必ず書かれる)

import { jstDateStr } from '../../lib/jst-date.js';

export const INPUT_SCHEMA_VERSION = '1.0';

const SALES_ENTITY = 'f_sales_by_listing';
const FINANCE_ENTITY_SUFFIX = '_finance_sku_daily';

// ── 日付ヘルパー (純粋な日付演算。TZ を経由しない) ──────────────────────

/** 'YYYY-MM-DD' に日数を加算 (UTC 固定で日付演算のみ。JST 変換は済んでいる前提) */
export function addDaysYmd(ymd, delta) {
  const [y, m, d] = ymd.split('-').map(Number);
  const t = Date.UTC(y, m - 1, d) + delta * 86400000;
  const dt = new Date(t);
  const pad = (n) => String(n).padStart(2, '0');
  return `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())}`;
}

/** 'YYYY-MM-DD' の曜日 (0=日..6=土)。TZ 非依存 */
export function dowOfYmd(ymd) {
  const [y, m, d] = ymd.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d)).getUTCDay();
}

/** JST 今日を基準に「先週月曜」(直近の完了した週の開始日) を返す */
export function lastCompletedWeekStart(now = new Date()) {
  const today = jstDateStr(now);
  const dow = dowOfYmd(today);
  const thisMonday = addDaysYmd(today, -((dow + 6) % 7));
  return addDaysYmd(thisMonday, -7);
}

/** period の 7 日リスト (period_end は排他的) */
function weekDays(periodStart) {
  return Array.from({ length: 7 }, (_, i) => addDaysYmd(periodStart, i));
}

function safeJsonParse(text) {
  if (typeof text !== 'string' || text === '') return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function roundYen(v) {
  return v == null ? null : Math.round(v);
}

// ── 充足判定 ──────────────────────────────────────────────────────────

/**
 * sync_run_chunks から entity の「取得完了済み日」集合を得る。
 * ⚠️ chunk 単位の applied_at だけでは不十分 (chunk_count > 1 の run は最初の chunk 適用時点で
 * scope 全体が完了に見えてしまう)。run 単位で「全 chunk が到着済みかつ applied 済み」の
 * 完了 run に属する chunk のみを対象にする (Codexレビュー2巡目 high 対応)。
 */
function coveredDaysForEntity(db, entity, days) {
  const first = days[0];
  const last = days[days.length - 1];
  const rows = db.prepare(`
    SELECT scope_from, scope_to FROM sync_run_chunks
    WHERE entity = ? AND applied_at IS NOT NULL
      AND scope_from IS NOT NULL AND scope_to IS NOT NULL
      AND scope_to >= ? AND scope_from <= ?
      AND run_id IN (
        SELECT run_id FROM sync_run_chunks
        WHERE entity = ?
        GROUP BY run_id
        HAVING COUNT(*) = MAX(chunk_count)
           AND SUM(CASE WHEN applied_at IS NULL THEN 1 ELSE 0 END) = 0
      )
  `).all(entity, first, last, entity);
  const covered = new Set();
  for (const r of rows) {
    for (const d of days) {
      if (d >= r.scope_from && d <= r.scope_to) covered.add(d);
    }
  }
  return covered;
}

/**
 * 充足判定の本体。expected sources (有効期間内) ごとに status を決める。
 * status: 'ok' | 'partial' (一部日欠損) | 'missing' | 'suspect' (マーカーはあるが実データ不審)
 */
function assessCoverage(db, periodStart, periodEnd, days) {
  const lastDay = days[days.length - 1];
  const sources = db.prepare(`
    SELECT source_id, source_kind, unit, requirement
    FROM ai_expected_sources
    WHERE valid_from <= ? AND (valid_to IS NULL OR valid_to >= ?)
    ORDER BY source_kind, unit
  `).all(lastDay, periodStart);

  const salesCovered = coveredDaysForEntity(db, SALES_ENTITY, days);
  const salesMissingDays = days.filter((d) => !salesCovered.has(d));

  // per-mall 実データ判定 (suspect 検出):
  // f_sales_by_listing の取得完了マーカーは全モール一体のため、1モールだけ上流 (miniPC側の
  // モール別sync) が欠落してもマーカー上は「完了」に見える。Render 側でできる補助判定として
  // 「過去4週の同曜日に売上があったのに今週その曜日が0行」の曜日パターン照合を行う。
  // ⚠️ 限界: 過去4週も売れていないモールの欠損は検出できない (休止モールは
  //    ai_expected_sources.valid_to で外す運用)。恒久策 = モール別 row_count meta を
  //    同期契約に追加 (miniPC 側変更が必要なため PR-1 スコープ外、要件書 §5.4 に追記済み)
  const presenceRows = db.prepare(`
    SELECT DISTINCT mall, date_jst FROM v_mall_sales_daily_unified
    WHERE date_jst >= ? AND date_jst < ?
  `).all(addDaysYmd(periodStart, -28), periodEnd);
  const presence = new Set(presenceRows.map((r) => `${r.mall}|${r.date_jst}`));
  const priorCnt = {};
  const periodCnt = {};
  for (const r of presenceRows) {
    if (r.date_jst >= periodStart) periodCnt[r.mall] = (periodCnt[r.mall] || 0) + 1;
    else priorCnt[r.mall] = (priorCnt[r.mall] || 0) + 1;
  }
  /** 過去4週の同曜日に3週以上実績があるのに今週0行の日を数える */
  const suspectDaysForMall = (mall) => {
    let suspect = 0;
    for (const d of days) {
      if (presence.has(`${mall}|${d}`)) continue;
      let priorHits = 0;
      for (let w = 1; w <= 4; w++) {
        if (presence.has(`${mall}|${addDaysYmd(d, -7 * w)}`)) priorHits++;
      }
      if (priorHits >= 3) suspect++;
    }
    return suspect;
  };

  const results = [];
  for (const s of sources) {
    let status = 'ok';
    let detail = null;
    if (s.source_kind === 'sales') {
      if (salesMissingDays.length >= 7) {
        status = 'missing';
        detail = `取得完了マーカーなし (${SALES_ENTITY})`;
      } else if (salesMissingDays.length > 0) {
        status = 'partial';
        detail = `未取込日: ${salesMissingDays.join(', ')}`;
      } else if ((periodCnt[s.unit] || 0) === 0 && (priorCnt[s.unit] || 0) > 0) {
        // マーカー上は完了だが、直近28日は売上があったモールが今週 0 行 → 上流欠落疑い
        status = 'suspect';
        detail = '取込完了マーカーはあるが当該モールの行が 0 (過去28日は実績あり)';
      } else {
        // 部分欠損: 過去4週の同曜日パターンと照合 (2日以上の不自然な空白で疑い)
        const sd = suspectDaysForMall(s.unit);
        if (sd >= 2) {
          status = 'suspect';
          detail = `過去4週は同曜日に実績があるのに今週 0 行の日が ${sd} 日 (上流欠落疑い)`;
        }
      }
    } else if (s.source_kind === 'finance') {
      const covered = coveredDaysForEntity(db, `${s.unit}${FINANCE_ENTITY_SUFFIX}`, days);
      const missing = days.filter((d) => !covered.has(d));
      if (missing.length >= 7) {
        status = 'missing';
        detail = '取得完了マーカーなし';
      } else if (missing.length > 0) {
        status = 'partial';
        detail = `未取込日: ${missing.join(', ')}`;
      }
    } else if (s.source_kind === 'inventory') {
      // 在庫は summary 行の存在 = 完了マーカー (qty/value 0 でも行は書かれる)
      const present = db.prepare(`
        SELECT COUNT(DISTINCT business_date) AS days FROM mirror_inv_daily_summary
        WHERE market = 'jp' AND category = ? AND business_date >= ? AND business_date < ?
      `).get(s.unit, periodStart, periodEnd);
      const lastPresent = db.prepare(`
        SELECT 1 AS x FROM mirror_inv_daily_summary
        WHERE market = 'jp' AND category = ? AND business_date = ? LIMIT 1
      `).get(s.unit, lastDay);
      if ((present?.days || 0) === 0) {
        status = 'missing';
        detail = '対象期間に日次スナップショットなし';
      } else if (!lastPresent || present.days < 5) {
        // 最終日欠損 or 半分以上の日が欠損 → 週次比較の信頼性なし
        status = 'partial';
        detail = `スナップショット ${present.days}/7日` + (lastPresent ? '' : ` (最終日 ${lastDay} なし)`);
      } else if (present.days < 7) {
        // 中間日の軽微な欠損: 週次は最新日+7日前比較が主のため ok のまま detail のみ記録
        detail = `スナップショット ${present.days}/7日 (中間日に欠損)`;
      }
    } else if (s.source_kind === 'po') {
      try {
        const pub = db.prepare(
          'SELECT run_id, status, as_of_date, synced_at FROM mirror_pml_published WHERE id = 1'
        ).get();
        if (!pub || !pub.run_id) {
          status = 'missing';
          detail = 'mirror_pml_published 未設定';
        } else if (pub.as_of_date && pub.as_of_date < addDaysYmd(lastDay, -3)) {
          status = 'partial';
          detail = `公開 run が古い (as_of=${pub.as_of_date})`;
        }
      } catch (e) {
        status = 'missing';
        detail = `PML 参照不可: ${e.message}`;
      }
    } else if (s.source_kind === 'mf') {
      const run = db.prepare(`
        SELECT MAX(run_id) AS run_id FROM mirror_mf_publish_runs WHERE status = 'success'
      `).get();
      if (!run?.run_id) {
        status = 'missing';
        detail = 'MF publish run (success) なし';
      }
    }
    results.push({ ...s, status, detail });
  }
  return results;
}

/**
 * coverage から生成可否と論点生成禁止領域を導出 (要件 §5.3/§5.4)。
 * - required の sales が全モール missing → generation 'blocked'
 * - sales の欠損モール → そのモールの論点 + 全社合計を使う論点を禁止
 * - finance (conditional) の欠損モール → 粗利系論点を禁止
 */
function deriveConstraints(coverage) {
  const prohibited = [];
  let companyTotalsAllowed = true;
  let blocked = false;
  let degraded = false;

  const salesRows = coverage.filter((c) => c.source_kind === 'sales');
  const badSales = salesRows.filter((c) => c.status !== 'ok');
  if (salesRows.length > 0 && badSales.length === salesRows.length) {
    blocked = true;
  }
  for (const c of badSales) {
    prohibited.push({ area: `sales:${c.unit}`, reason: c.detail || c.status });
    companyTotalsAllowed = false;
    degraded = true;
  }
  for (const c of coverage.filter((x) => x.source_kind === 'finance' && x.status !== 'ok')) {
    prohibited.push({ area: `margin:${c.unit}`, reason: c.detail || c.status });
    degraded = true;
  }
  for (const c of coverage.filter((x) => x.source_kind === 'inventory' && x.status !== 'ok')) {
    prohibited.push({ area: `inventory:${c.unit}`, reason: c.detail || c.status });
    if (c.requirement === 'required') degraded = true;
  }
  for (const c of coverage.filter((x) => x.source_kind === 'po' && x.status !== 'ok')) {
    prohibited.push({ area: 'po_outstanding', reason: c.detail || c.status });
  }
  if (!companyTotalsAllowed) {
    prohibited.push({ area: 'company_totals', reason: '売上欠損モールがあるため全社合計を使う論点は禁止' });
  }
  return {
    generation: blocked ? 'blocked' : 'allowed',
    data_quality_status: blocked ? 'blocked' : degraded ? 'degraded' : 'normal',
    prohibited_topic_areas: prohibited,
  };
}

// ── 事実の組み立て ────────────────────────────────────────────────────

/** モール別: 今週 vs 前週 vs 過去4週中央値 (売上・数量) */
function buildSalesFacts(db, periodStart, periodEnd) {
  const windowStart = addDaysYmd(periodStart, -28);
  const rows = db.prepare(`
    SELECT mall, date_jst,
           SUM(sales_gross_jpy_incl) AS sales,
           SUM(units_net_sold) AS units
    FROM v_mall_sales_daily_unified
    WHERE date_jst >= ? AND date_jst < ?
    GROUP BY mall, date_jst
  `).all(windowStart, periodEnd);

  // 週バケツ: 0=今週(period), 1=前週, ..., 4=4週前
  const byMallWeek = new Map();
  const dailyThisWeek = [];
  for (const r of rows) {
    let bucket = null;
    if (r.date_jst >= periodStart) bucket = 0;
    else {
      for (let w = 1; w <= 4; w++) {
        const ws = addDaysYmd(periodStart, -7 * w);
        if (r.date_jst >= ws && r.date_jst < addDaysYmd(ws, 7)) { bucket = w; break; }
      }
    }
    if (bucket === null) continue;
    const key = r.mall;
    if (!byMallWeek.has(key)) byMallWeek.set(key, [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]);
    const agg = byMallWeek.get(key)[bucket];
    agg[0] += r.sales || 0;
    agg[1] += r.units || 0;
    if (bucket === 0) {
      dailyThisWeek.push({
        mall: r.mall, date: r.date_jst,
        sales_yen: roundYen(r.sales), units: r.units,
      });
    }
  }

  const median = (arr) => {
    const s = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(s.length / 2);
    return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
  };

  const byMall = [];
  let companyThis = 0, companyPrev = 0, companyUnitsThis = 0;
  for (const [mall, weeks] of byMallWeek.entries()) {
    const thisW = weeks[0], prevW = weeks[1];
    const prior4 = weeks.slice(1).map((w) => w[0]);
    companyThis += thisW[0];
    companyPrev += prevW[0];
    companyUnitsThis += thisW[1];
    byMall.push({
      mall,
      sales_yen: roundYen(thisW[0]),
      units: thisW[1],
      prev_week_sales_yen: roundYen(prevW[0]),
      prev_week_units: prevW[1],
      prior_4w_median_sales_yen: roundYen(median(prior4)),
      wow_pct: prevW[0] > 0 ? +((thisW[0] / prevW[0] - 1) * 100).toFixed(1) : null,
    });
  }
  byMall.sort((a, b) => (b.sales_yen || 0) - (a.sales_yen || 0));

  return {
    by_mall: byMall,
    daily: dailyThisWeek.sort((a, b) => a.date.localeCompare(b.date) || a.mall.localeCompare(b.mall)),
    company: {
      sales_yen: roundYen(companyThis),
      units: companyUnitsThis,
      prev_week_sales_yen: roundYen(companyPrev),
      wow_pct: companyPrev > 0 ? +((companyThis / companyPrev - 1) * 100).toFixed(1) : null,
    },
  };
}

/** モール別粗利 (finance fact 統合ビュー) + ワーストSKU */
function buildMarginFacts(db, periodStart, periodEnd) {
  const prevStart = addDaysYmd(periodStart, -7);
  const byMall = db.prepare(`
    SELECT mall,
           SUM(CASE WHEN date_jst >= :ps THEN margin_jpy_for_reporting ELSE 0 END) AS margin,
           SUM(CASE WHEN date_jst >= :ps THEN sales_gross_jpy_incl ELSE 0 END) AS sales,
           SUM(CASE WHEN date_jst < :ps THEN margin_jpy_for_reporting ELSE 0 END) AS prev_margin,
           SUM(CASE WHEN date_jst < :ps THEN sales_gross_jpy_incl ELSE 0 END) AS prev_sales,
           SUM(CASE WHEN date_jst >= :ps AND cost_status = 'ok' THEN 1 ELSE 0 END) AS rows_cost_ok,
           SUM(CASE WHEN date_jst >= :ps THEN 1 ELSE 0 END) AS rows_total
    FROM v_mall_finance_daily_unified
    WHERE date_jst >= :prevStart AND date_jst < :pe
    GROUP BY mall
  `).all({ ps: periodStart, prevStart, pe: periodEnd });

  const marginByMall = byMall.map((r) => ({
    mall: r.mall,
    margin_yen: roundYen(r.margin),
    sales_yen: roundYen(r.sales),
    margin_pct: r.sales > 0 ? +((r.margin / r.sales) * 100).toFixed(1) : null,
    prev_week_margin_yen: roundYen(r.prev_margin),
    prev_week_margin_pct: r.prev_sales > 0 ? +((r.prev_margin / r.prev_sales) * 100).toFixed(1) : null,
    cost_ok_row_share_pct: r.rows_total > 0 ? +((r.rows_cost_ok / r.rows_total) * 100).toFixed(0) : null,
    note: r.mall === 'amazon' ? 'amazonのみ税抜基準 (margin_basis=excl_tax)' : undefined,
  }));

  const worstSkus = db.prepare(`
    SELECT mall, sku_key, ne_code, MAX(product_name) AS product_name,
           SUM(units_net_sold) AS units,
           SUM(sales_gross_jpy_incl) AS sales,
           SUM(margin_jpy_for_reporting) AS margin
    FROM v_mall_finance_daily_unified
    WHERE date_jst >= ? AND date_jst < ? AND cost_status = 'ok'
    GROUP BY mall, sku_key
    HAVING SUM(units_net_sold) > 0 AND SUM(margin_jpy_for_reporting) < 0
    ORDER BY SUM(margin_jpy_for_reporting) ASC
    LIMIT 10
  `).all(periodStart, periodEnd).map((r) => ({
    mall: r.mall,
    sku: r.sku_key,
    ne_code: r.ne_code,
    product_name: r.product_name,
    units: r.units,
    sales_yen: roundYen(r.sales),
    margin_yen: roundYen(r.margin),
  }));

  return { by_mall: marginByMall, negative_margin_skus: worstSkus,
    note: '粗利は原価整備済み行のみのワースト抽出。cost_ok_row_share_pct が低いモールは粗利の信頼度も低い' };
}

/** 在庫 (カテゴリ別 最新日 vs 7日前) + 滞留・欠品リスク件数 */
function buildInventoryFacts(db, periodStart, periodEnd) {
  const lastDay = addDaysYmd(periodEnd, -1);
  const summary = db.prepare(`
    SELECT category, business_date, total_qty, total_value
    FROM mirror_inv_daily_summary
    WHERE market = 'jp' AND business_date IN (?, ?)
    ORDER BY category, business_date
  `).all(addDaysYmd(lastDay, -7), lastDay);

  const byCategory = new Map();
  for (const r of summary) {
    if (!byCategory.has(r.category)) byCategory.set(r.category, {});
    const slot = r.business_date === lastDay ? 'now' : 'week_ago';
    byCategory.get(r.category)[slot] = r;
  }
  const categories = [...byCategory.entries()].map(([category, v]) => ({
    category,
    as_of: v.now?.business_date || null,
    qty: v.now?.total_qty ?? null,
    value_yen: roundYen(v.now?.total_value),
    week_ago_value_yen: roundYen(v.week_ago?.total_value),
  }));

  let risk = null;
  try {
    risk = db.prepare(`
      SELECT
        SUM(CASE WHEN is_stale = 1 THEN 1 ELSE 0 END) AS stale_skus,
        SUM(CASE WHEN days_of_supply IS NOT NULL AND days_of_supply <= 14 THEN 1 ELSE 0 END) AS low_supply_skus
      FROM v_mir_inv_daily_metrics
      WHERE business_date = ? AND market = 'jp'
    `).get(lastDay);
  } catch {
    risk = null; // detail 未同期日でも在庫サマリ自体は返す
  }

  return {
    by_category: categories,
    as_of: lastDay,
    stale_skus: risk?.stale_skus ?? null,
    low_supply_skus_14d: risk?.low_supply_skus ?? null,
  };
}

/** 発注残 (v_pml_rows_authoritative 契約経由。#502 SSoT) */
function buildPoFacts(db) {
  try {
    const pub = db.prepare(
      'SELECT as_of_date, synced_at FROM mirror_pml_published WHERE id = 1'
    ).get();
    const agg = db.prepare(`
      SELECT SUM(注残数) AS qty, SUM(注残数 * 原価) AS value
      FROM v_pml_rows_authoritative
      WHERE 注残数 > 0
    `).get();
    return {
      outstanding_qty: agg?.qty ?? 0,
      outstanding_value_yen: roundYen(agg?.value ?? 0),
      as_of: pub?.as_of_date || null,
      note: '発注残 = 将来の仕入支払コミットメント (原価ベース概算)',
    };
  } catch (e) {
    return { error: `発注残を取得できず: ${e.message}` };
  }
}

/** 現預金 + MF 暫定値 (常に「暫定」ラベル。確定宣言は月次 PR-3 で導入) */
function buildMfFacts(db, periodStart, periodEnd) {
  const top = db.prepare('SELECT * FROM v_mirror_mf_executive_top_latest').get();
  const prevStart = addDaysYmd(periodStart, -7);
  let cashFlow = [];
  try {
    cashFlow = db.prepare(`
      SELECT CASE WHEN movement_date >= ? THEN 'this_week' ELSE 'prev_week' END AS week,
             direction, SUM(amount_excl_tax) AS amount
      FROM v_mirror_mf_cash_events_daily_latest
      WHERE movement_date >= ? AND movement_date < ?
      GROUP BY week, direction
    `).all(periodStart, prevStart, periodEnd);
  } catch {
    cashFlow = [];
  }
  const flow = { this_week: { in: 0, out: 0 }, prev_week: { in: 0, out: 0 } };
  for (const r of cashFlow) {
    if (flow[r.week] && (r.direction === 'in' || r.direction === 'out')) {
      flow[r.week][r.direction] = roundYen(r.amount);
    }
  }
  if (!top) return { available: false };
  return {
    available: true,
    reliability: '暫定 (MF仕訳入力ラグあり。月次締め確定前の値)',
    snapshot_date: top.snapshot_date,
    current_month: top.current_month_ym,
    mtd: {
      sales_excl_tax_yen: roundYen(top.sales_mtd_excl_tax),
      gross_profit_excl_tax_yen: roundYen(top.gross_profit_mtd_excl_tax),
      operating_income_excl_tax_yen: roundYen(top.operating_income_mtd_excl_tax),
    },
    month_end_forecast: {
      sales_yen: roundYen(top.sales_month_end_forecast),
      gross_profit_yen: roundYen(top.gross_profit_month_end_forecast),
      operating_income_yen: roundYen(top.operating_income_month_end_forecast),
      status: top.forecast_status,
    },
    yoy_pct: {
      sales: top.yoy_sales_pct,
      gross_profit: top.yoy_gross_profit_pct,
      operating_income: top.yoy_operating_income_pct,
    },
    cash_balance_total_yen: roundYen(top.cash_balance_total),
    cash_balance_by_bank: safeJsonParse(top.cash_balance_json),
    cash_flow_weekly: {
      ...flow,
      note: '銀行口座の入出金 gross 集計。口座間振替を含むため純増減の解釈は不可',
    },
    danger_signals: safeJsonParse(top.danger_signals_json),
  };
}

/** 予算 (未入力月は entries が空 → AI は予算差論点を生成しない契約) */
function buildBudgetFacts(db, periodStart, periodEnd) {
  const months = new Set([periodStart.slice(0, 7), addDaysYmd(periodEnd, -1).slice(0, 7)]);
  const placeholders = [...months].map(() => '?').join(',');
  const rows = db.prepare(`
    SELECT year_month, metric, value_yen, budget_row_id
    FROM v_ai_budget_current
    WHERE budget_kind = 'initial' AND year_month IN (${placeholders})
    ORDER BY year_month, metric
  `).all(...months);
  return {
    available: rows.length > 0,
    entries: rows.map((r) => ({
      year_month: r.year_month, metric: r.metric, value_yen: r.value_yen,
      budget_row_id: r.budget_row_id,
    })),
    // 監査用: このレポートが実際に参照した予算行の集合 (ソート済み join。年度違いの編集に影響されない)
    budget_revision_id: rows.length > 0
      ? rows.map((r) => r.budget_row_id).sort().join(',')
      : null,
    note: rows.length === 0
      ? '予算未入力。予算差に言及する論点は生成禁止 (前年比・トレンドのみで論じる)'
      : '粗利率予算は保存しない設計 (gross_profit / sales で導出)',
  };
}

/** 期間に重なるイベントメモ */
function buildEventFacts(db, periodStart, periodEnd) {
  const rows = db.prepare(`
    SELECT event_date, event_end_date, description, tag, scope
    FROM ai_events
    WHERE deleted_at IS NULL
      AND event_date < ?
      AND COALESCE(event_end_date, event_date) >= ?
    ORDER BY event_date
  `).all(periodEnd, addDaysYmd(periodStart, -7));
  return {
    entries: rows,
    note: 'イベントは「実施された」事実のみ。売上への因果は仮説として扱うこと (断定禁止)',
  };
}

/** 前回未解決論点 (構造化して持ち越し = 毎回ゼロから分析させない) */
function buildPreviousTopics(db, reportType) {
  const rows = db.prepare(`
    SELECT t.topic_id, t.title, t.category, t.status, r.public_id, r.period_start
    FROM ai_report_topics t
    JOIN ai_reports r ON r.report_id = t.report_id
    WHERE r.report_type = ? AND t.status IN ('open', 'carried', 'worsened')
    ORDER BY r.period_start DESC, t.seq
    LIMIT 10
  `).all(reportType);
  return rows;
}

/** 直近レポート (重複抑制: 連日同一指摘を新規のように書かせない) */
function buildRecentReports(db, reportType, limit = 4) {
  const reports = db.prepare(`
    SELECT report_id, public_id, period_start, period_end, edition, data_quality_status
    FROM ai_reports
    WHERE report_type = ?
    ORDER BY period_start DESC, created_at DESC
    LIMIT ?
  `).all(reportType, limit);
  const topicStmt = db.prepare(
    'SELECT title, category, status FROM ai_report_topics WHERE report_id = ? ORDER BY seq'
  );
  return reports.map((r) => ({
    public_id: r.public_id,
    period_start: r.period_start,
    edition: r.edition,
    topics: topicStmt.all(r.report_id),
  }));
}

// ── エントリポイント ──────────────────────────────────────────────────

/**
 * 週次レポートの入力 JSON を組み立てる。
 * @param {object} db better-sqlite3 (warehouse-mirror.db)
 * @param {object} opts { periodStart?: 'YYYY-MM-DD' (月曜。省略時=先週月曜) }
 */
export function buildWeeklyReportInput(db, opts = {}) {
  const periodStart = opts.periodStart || lastCompletedWeekStart();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(periodStart)) {
    throw new Error(`invalid periodStart: ${periodStart}`);
  }
  if (dowOfYmd(periodStart) !== 1) {
    throw new Error(`periodStart must be a Monday (JST): ${periodStart}`);
  }
  const periodEnd = addDaysYmd(periodStart, 7);
  const days = weekDays(periodStart);

  const coverage = assessCoverage(db, periodStart, periodEnd, days);
  const constraints = deriveConstraints(coverage);

  const input = {
    meta: {
      report_type: 'weekly',
      period_start: periodStart,
      period_end: periodEnd,
      period_label: `${periodStart}〜${addDaysYmd(periodEnd, -1)}`,
      generated_at: new Date().toISOString(),
      data_as_of: jstDateStr(new Date()),
      input_schema_version: INPUT_SCHEMA_VERSION,
    },
    coverage,
    constraints,
    facts: null,
    events: null,
    budget: null,
    previous_open_topics: null,
    recent_reports: null,
  };

  // blocked (必須ソース全滅) でも coverage/constraints は返す (runner が「生成なし」投稿に使う)
  if (constraints.generation === 'blocked') return input;

  input.facts = {
    sales: buildSalesFacts(db, periodStart, periodEnd),
    margin: buildMarginFacts(db, periodStart, periodEnd),
    inventory: buildInventoryFacts(db, periodStart, periodEnd),
    po: buildPoFacts(db),
    mf: buildMfFacts(db, periodStart, periodEnd),
  };
  input.events = buildEventFacts(db, periodStart, periodEnd);
  input.budget = buildBudgetFacts(db, periodStart, periodEnd);
  input.previous_open_topics = buildPreviousTopics(db, 'weekly');
  input.recent_reports = buildRecentReports(db, 'weekly');
  return input;
}
