/**
 * yahoo-analytics queries.js — データ層 (Render warehouse-mirror.db 読み取り)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/Yahoo統合管理ダッシュボード_要件定義_20260706.md
 *
 * 使用テーブル (P1):
 *   mirror_yahoo_finance_sku_daily  受注ベース日次fact (whitelist: 出荷完了+入金済、税込、
 *                                   variant粒度、snapshot原価、mall_fee は 10% 一律の推定値、
 *                                   units_cancelled は Phase 1a では 0 固定 → キャンセル系は表示しない)
 *   mart_yahoo_monthly_summary      月次確定 (yahoo-accounting の請求明細取込由来:
 *                                   ad_cost=広告費実額 / pf_fee=請求合計(税込)−広告費)
 *
 * 精度ラベル方針 (要件 §11-2、税はすべて税込):
 *   速報   = 受注ベース fact (毎朝 daily-sync で当月 rebuild)
 *   推定   = mall_fee (10%一律) を含む値
 *   確定   = 請求明細取込済み月の実額 (ad_cost / pf_fee)
 *   確定寄せ実質利益 = variable_margin_partial + mall_fee(推定を戻す) − pf_fee(実額) − ad_cost(実額)
 *     ※ pf_fee はポイント原資・キャンペーン原資・決済手数料・プロモーションパッケージ料等の請求額。
 *       fact 側で控除済みのストアクーポン (価格値引・請求外) / ポイント利用 (受取減額・請求外)
 *       とは費目が重ならないため二重計上しない (要件 §2.5 L3)
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

// ─── アプリ専用テーブル (Render-local。yadash_ prefix、amazon-dashboard の amzdash_ と同パターン) ───
let _initialized = false;
export function ensureAppTables() {
  if (_initialized) return;
  const db = getMirrorDB();
  db.exec(`CREATE TABLE IF NOT EXISTS yadash_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);
  // 料率マスタ (改定日付き履歴 — 要件 F-5。2026-09 手数料改定を履歴として表現できる構造)
  db.exec(`CREATE TABLE IF NOT EXISTS yadash_rate_master (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    rate_key       TEXT NOT NULL CHECK (rate_key IN (
      'point_base', 'campaign_base', 'payment_fee', 'promo_package', 'sales_royalty', 'pr_option_default'
    )),
    rate_pct       REAL NOT NULL CHECK (rate_pct >= 0 AND rate_pct <= 100),
    effective_from TEXT NOT NULL CHECK (effective_from GLOB '????-??-??'),
    memo           TEXT NOT NULL DEFAULT '',
    created_at     TEXT NOT NULL,
    deleted_at     TEXT
  )`);
  _initialized = true;
}

// バリデーションエラー (router 側で 400 + メッセージ表示にする)
export function validationError(msg) {
  const e = new Error(msg);
  e.status = 400;
  return e;
}

// ─── 診断・分析閾値 (設定タブで変更可) ───
export const DEFAULT_SETTINGS = {
  abc_a_pct: 80,          // ABC: A = 売上累積 N% まで
  abc_b_pct: 95,
  new_product_days: 60,   // 新商品バッジ: 初売上から N 日
  movers_min_units: 5,    // 急上昇/急落: 当期 or 前期 N 個以上のみ (ノイズ除去)
};
// キーごとの許容範囲 (Codex P3-R1 medium: 範囲外値でランキング分類・NEW判定が壊れるのを防ぐ)
const SETTING_RANGES = {
  abc_a_pct: [1, 99],
  abc_b_pct: [2, 99.9],
  new_product_days: [1, 365],
  movers_min_units: [0, 1000],
};

export function getSettings() {
  ensureAppTables();
  const db = getMirrorDB();
  const rows = db.prepare(`SELECT key, value FROM yadash_settings`).all();
  const out = { ...DEFAULT_SETTINGS };
  for (const r of rows) {
    if (r.key in out) {
      const n = Number(r.value);
      if (Number.isFinite(n)) out[r.key] = n;
    }
  }
  return out;
}

export function saveSettings(patch) {
  ensureAppTables();
  const db = getMirrorDB();
  const now = new Date().toISOString();
  // 先に patch 全体を検証してから書く (部分適用しない)
  const validated = {};
  for (const [k, v] of Object.entries(patch)) {
    if (!(k in DEFAULT_SETTINGS)) continue;
    const n = Number(v);
    if (!Number.isFinite(n)) throw validationError(`${k} は数値で指定してください`);
    const [min, max] = SETTING_RANGES[k];
    if (n < min || n > max) throw validationError(`${k} は ${min}〜${max} の範囲で指定してください`);
    validated[k] = n;
  }
  const merged = { ...getSettings(), ...validated };
  if (merged.abc_a_pct >= merged.abc_b_pct) throw validationError('abc_a_pct は abc_b_pct より小さくしてください');
  const stmt = db.prepare(`
    INSERT INTO yadash_settings (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
  `);
  const tx = db.transaction(() => {
    for (const [k, n] of Object.entries(validated)) stmt.run(k, String(n), now);
  });
  tx();
  return getSettings();
}

// ─── 料率マスタ (F-5: 現状一覧 + 改定日付き履歴 — 専用管理画面原則) ───
export const RATE_KEYS = {
  point_base: 'ストアポイント原資 (既定%)',
  campaign_base: 'キャンペーン原資',
  payment_fee: '決済手数料',
  promo_package: 'プロモーションパッケージ料',
  sales_royalty: '売上ロイヤリティ (2026-09新設)',
  pr_option_default: 'PRオプション既定料率',
};

export function getRates() {
  ensureAppTables();
  const db = getMirrorDB();
  const today = jstToday();
  const history = db.prepare(`
    SELECT id, rate_key, rate_pct, effective_from, memo, created_at
    FROM yadash_rate_master
    WHERE deleted_at IS NULL
    ORDER BY rate_key, effective_from DESC, id DESC
  `).all();
  // 現在有効な料率 = rate_key ごとに effective_from <= 今日 の最新行
  const current = {};
  for (const r of history) {
    if (r.effective_from <= today && !(r.rate_key in current)) current[r.rate_key] = r;
  }
  // 予約済み改定 (未来日) も見えるように
  const upcoming = history.filter(r => r.effective_from > today);
  return { keys: RATE_KEYS, current, upcoming, history };
}

export function addRate({ rate_key, rate_pct, effective_from, memo }) {
  ensureAppTables();
  if (!(rate_key in RATE_KEYS)) throw validationError('rate_key が不正です');
  const pct = Number(rate_pct);
  if (!Number.isFinite(pct) || pct < 0 || pct > 100) throw validationError('料率は 0〜100 の数値で指定してください');
  if (!isValidDate(effective_from)) throw validationError('適用開始日は YYYY-MM-DD で指定してください');
  const db = getMirrorDB();
  db.prepare(`
    INSERT INTO yadash_rate_master (rate_key, rate_pct, effective_from, memo, created_at)
    VALUES (?, ?, ?, ?, ?)
  `).run(rate_key, pct, effective_from, String(memo || '').slice(0, 200), new Date().toISOString());
  return getRates();
}

export function deleteRate(id) {
  ensureAppTables();
  const db = getMirrorDB();
  const r = db.prepare(`UPDATE yadash_rate_master SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL`)
    .run(new Date().toISOString(), Number(id));
  if (r.changes === 0) throw validationError('対象の料率が見つかりません');
  return getRates();
}

// ─── 日付 helper (JST。Date.toISOString の UTC 罠に注意: feedback_jst_to_iso_string_trap) ───
export function jstToday() {
  return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
export function addDays(dateStr, n) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
export function monthOf(dateStr) { return dateStr.slice(0, 7); }
export function monthStart(ym) { return `${ym}-01`; }
export function monthEnd(ym) {
  const [y, m] = ym.split('-').map(Number);
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return `${ym}-${String(lastDay).padStart(2, '0')}`;
}
export function addMonths(ym, n) {
  const [y, m] = ym.split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export function isValidDate(s) {
  if (typeof s !== 'string' || !DATE_RE.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// カスタム期間の上限 (日)。巨大 range での全表スキャン抑止
const MAX_CUSTOM_RANGE_DAYS = 730;

// preset → {from, to} (JST)
export function resolvePeriod(preset, fromQ, toQ) {
  const today = jstToday();
  if (isValidDate(fromQ) && isValidDate(toQ) && fromQ <= toQ) {
    // 両端含みで MAX_CUSTOM_RANGE_DAYS 日に clamp (Codex R1 low)
    const minFrom = addDays(toQ, -(MAX_CUSTOM_RANGE_DAYS - 1));
    return { from: minFrom > fromQ ? minFrom : fromQ, to: toQ, preset: 'custom' };
  }
  const ym = monthOf(today);
  const map = {
    today: { from: today, to: today },
    yesterday: { from: addDays(today, -1), to: addDays(today, -1) },
    '7d': { from: addDays(today, -6), to: today },
    '14d': { from: addDays(today, -13), to: today },
    '30d': { from: addDays(today, -29), to: today },
    '90d': { from: addDays(today, -89), to: today },
    this_month: { from: monthStart(ym), to: today },
    last_month: { from: monthStart(addMonths(ym, -1)), to: monthEnd(addMonths(ym, -1)) },
    this_year: { from: `${today.slice(0, 4)}-01-01`, to: today },
    '12m': { from: monthStart(addMonths(ym, -11)), to: today },
  };
  const r = map[preset] || map['30d'];
  return { ...r, preset: map[preset] ? preset : '30d' };
}

// ─── 受注ベース fact の期間集計 (税込) ───
function factSummary(db, from, to) {
  return db.prepare(`
    SELECT
      COALESCE(SUM(gross_sales_jpy_incl),0)              AS sales_incl,
      COALESCE(SUM(units_ordered),0)                     AS units_ordered,
      COALESCE(SUM(units_net_sold),0)                    AS units_net,
      COALESCE(SUM(coupon_shop_jpy_incl),0)              AS coupon_shop,
      COALESCE(SUM(use_point_jpy_incl),0)                AS use_point,
      COALESCE(SUM(mall_fee_jpy_incl),0)                 AS mall_fee_est,
      COALESCE(SUM(shipping_cost_jpy_incl),0)            AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)              AS cogs,
      COALESCE(SUM(variable_margin_partial_jpy_incl),0)  AS variable_margin,
      COALESCE(SUM(CASE WHEN is_cost_complete = 1 THEN gross_sales_jpy_incl END),0) AS sales_cost_complete,
      COALESCE(SUM(CASE WHEN unresolved_sku_flag = 1 THEN gross_sales_jpy_incl END),0) AS sales_unresolved,
      COUNT(DISTINCT date_jst)                           AS days_with_data
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 月次確定 (yahoo-accounting の請求明細取込済み月のみ) ───
// pf_fee > 0 条件: yahoo-accounting の過去月一括取込 (/import-history) 行は pf_fee 未計算 (=0) の
// まま confirmed_at が入るため、それを「確定」扱いすると実質利益が過大表示になる (Codex R1 medium)。
// 実運用で請求合計−広告費が 0 円の月は存在しない (ポイント原資等が必ず発生する) ため 0 は未確定とみなす。
const CONFIRMED_COND = `confirmed_at IS NOT NULL AND COALESCE(pf_fee, 0) > 0`;
function confirmedMonth(db, ym) {
  return db.prepare(`
    SELECT year_month, COALESCE(ad_cost,0) AS ad_cost, COALESCE(pf_fee,0) AS pf_fee, confirmed_at
    FROM mart_yahoo_monthly_summary
    WHERE year_month = ? AND ${CONFIRMED_COND}
  `).get(ym) || null;
}

// ─── 概要タブ: タイル 4 枚 (今日/昨日/今月/先月) ───
export function getOverview() {
  const db = getMirrorDB();
  const today = jstToday();
  const ym = monthOf(today);
  const lastYm = addMonths(ym, -1);
  const periods = [
    { key: 'today', label: '今日', from: today, to: today },
    { key: 'yesterday', label: '昨日', from: addDays(today, -1), to: addDays(today, -1) },
    { key: 'this_month', label: '今月', from: monthStart(ym), to: today },
    { key: 'last_month', label: '先月', from: monthStart(lastYm), to: monthEnd(lastYm) },
  ];

  // データ鮮度 (fact は毎朝 07:00 build。今日タイルは翌朝まで 0 になるのが正常)
  const fresh = db.prepare(`
    SELECT MAX(date_jst) AS data_to, MAX(synced_at) AS last_synced
    FROM mirror_yahoo_finance_sku_daily
  `).get();

  const tiles = periods.map(p => {
    const s = factSummary(db, p.from, p.to);
    const daysInPeriod = Math.round((new Date(p.to + 'T00:00:00Z') - new Date(p.from + 'T00:00:00Z')) / 86400000) + 1;
    const tile = {
      ...p,
      sales_incl: Math.round(s.sales_incl),
      units_ordered: s.units_ordered,
      units_net: s.units_net,
      coupon_shop: Math.round(s.coupon_shop),
      use_point: Math.round(s.use_point),
      mall_fee_est: Math.round(s.mall_fee_est),
      shipping: Math.round(s.shipping),
      cogs: Math.round(s.cogs),
      variable_margin: Math.round(s.variable_margin),
      margin_pct: s.sales_incl > 0 ? Math.round(s.variable_margin / s.sales_incl * 1000) / 10 : null,
      cost_coverage_pct: s.sales_incl > 0 ? Math.round(s.sales_cost_complete / s.sales_incl * 1000) / 10 : null,
      days_with_data: s.days_with_data,
      days_in_period: daysInPeriod,
    };
    // 月タイル: 請求明細取込済みなら確定値 (広告費/PF手数料実額) と確定寄せ実質利益を載せる。
    // 確定寄せは「月全体の fact − 月全体の請求」の突合なので、期間が月を完全にカバーする
    // last_month のみ対象 (this_month は月の途中 = 部分 fact に全額請求を引くと過小表示になる。
    // Codex R1 medium)
    if (p.key === 'this_month' || p.key === 'last_month') {
      const c = p.key === 'last_month' ? confirmedMonth(db, lastYm) : null;
      if (c) {
        tile.confirmed = {
          ad_cost: Math.round(c.ad_cost),
          pf_fee: Math.round(c.pf_fee),
          confirmed_at: c.confirmed_at,
          // 実質利益(確定寄せ) = 粗利(速報) + 手数料推定を戻す − PF手数料実額 − 広告費実額
          full_margin: Math.round(s.variable_margin + s.mall_fee_est - c.pf_fee - c.ad_cost),
        };
      } else {
        tile.confirmed = null;
      }
    }
    return tile;
  });

  // SKU 未解決の可視化 (要件 F-4 の思想を前倒し: 未解決 = 原価 0 で粗利が過大に出る)
  const un = db.prepare(`
    SELECT COUNT(DISTINCT yahoo_sku_key) AS sku_count,
           COALESCE(SUM(gross_sales_jpy_incl),0) AS sales_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE unresolved_sku_flag = 1 AND date_jst >= ? AND date_jst <= ?
  `).get(monthStart(ym), today);
  const monthTile = tiles.find(t => t.key === 'this_month');
  const unresolved = {
    sku_count: un.sku_count,
    sales_incl: Math.round(un.sales_incl),
    share_pct: monthTile && monthTile.sales_incl > 0
      ? Math.round(un.sales_incl / monthTile.sales_incl * 1000) / 10 : null,
  };

  return {
    generated_at: new Date().toISOString(),
    today,
    data_to: fresh.data_to,
    last_synced: fresh.last_synced,
    tiles,
    unresolved,
  };
}

// ─── トレンド (日次/週次/月次) ───
export function getTrend(from, to, granularity) {
  const db = getMirrorDB();
  // 半年を超える day 指定は week に自動格上げ (点が潰れて読めない + 行数抑制)
  const spanDays = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  if (granularity === 'day' && spanDays > 183) granularity = 'week';

  // week バケットは「その週の月曜の日付」(-6 days して次の月曜へ進める SQLite idiom)
  const bucketExpr = granularity === 'month' ? `substr(date_jst, 1, 7)`
    : granularity === 'week' ? `date(date_jst, '-6 days', 'weekday 1')`
    : `date_jst`;
  const rows = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
      MIN(date_jst) AS bucket_start,
      SUM(gross_sales_jpy_incl)             AS sales_incl,
      SUM(variable_margin_partial_jpy_incl) AS variable_margin,
      SUM(units_net_sold)                   AS units_net,
      SUM(coupon_shop_jpy_incl)             AS coupon_shop,
      SUM(use_point_jpy_incl)               AS use_point,
      SUM(mall_fee_jpy_incl)                AS mall_fee_est
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to).map(r => ({
    ...r,
    sales_incl: Math.round(r.sales_incl),
    variable_margin: Math.round(r.variable_margin),
    coupon_shop: Math.round(r.coupon_shop),
    use_point: Math.round(r.use_point),
    mall_fee_est: Math.round(r.mall_fee_est),
    margin_pct: r.sales_incl > 0 ? Math.round(r.variable_margin / r.sales_incl * 1000) / 10 : null,
    avg_unit_price: r.units_net > 0 ? Math.round(r.sales_incl / r.units_net) : null,
  }));

  // 月次粒度のときは確定情報 (請求明細取込済み月) を重ねられるように返す。
  // 確定寄せは月全体の fact との突合なので、[from, to] が月初〜月末を完全に含む月のみ対象
  // (部分月に全額請求を引くと full_margin が過小表示になる。Codex R1 medium)
  let confirmed = [];
  if (granularity === 'month') {
    confirmed = db.prepare(`
      SELECT year_month, COALESCE(ad_cost,0) AS ad_cost, COALESCE(pf_fee,0) AS pf_fee
      FROM mart_yahoo_monthly_summary
      WHERE ${CONFIRMED_COND} AND year_month >= ? AND year_month <= ?
      ORDER BY year_month
    `).all(monthOf(from), monthOf(to))
      .filter(c => monthStart(c.year_month) >= from && monthEnd(c.year_month) <= to)
      .map(c => {
        const row = rows.find(r => r.bucket === c.year_month);
        return {
          year_month: c.year_month,
          ad_cost: Math.round(c.ad_cost),
          pf_fee: Math.round(c.pf_fee),
          full_margin: row ? Math.round(row.variable_margin + row.mall_fee_est - c.pf_fee - c.ad_cost) : null,
        };
      });
  }
  return { from, to, granularity, rows, confirmed_months: confirmed };
}

// ─── [from, to] に完全に含まれる暦月の列挙 ───
function completeMonthsInRange(from, to) {
  const months = [];
  let ym = monthOf(from);
  const last = monthOf(to);
  while (ym <= last) {
    if (monthStart(ym) >= from && monthEnd(ym) <= to) months.push(ym);
    ym = addMonths(ym, 1);
  }
  return months;
}

// ─── 利益分析タブ: ウォーターフォール (F-3) ───
// L1 (partial margin 6要素) までは常に返す。SKU 指定なし + 期間が「確定済みの完全月のみ」で
// 構成される場合に限り、確定寄せ (L3 相当: 手数料推定戻し→PF手数料実額→広告費実額) を延長表示。
export function getWaterfall(from, to, sku) {
  const db = getMirrorDB();
  const skuCond = sku ? `AND yahoo_sku_key = ?` : '';
  const params = sku ? [from, to, sku] : [from, to];
  const s = db.prepare(`
    SELECT
      COALESCE(SUM(gross_sales_jpy_incl),0)             AS sales_incl,
      COALESCE(SUM(coupon_shop_jpy_incl),0)             AS coupon_shop,
      COALESCE(SUM(use_point_jpy_incl),0)               AS use_point,
      COALESCE(SUM(mall_fee_jpy_incl),0)                AS mall_fee_est,
      COALESCE(SUM(shipping_cost_jpy_incl),0)           AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)             AS cogs,
      COALESCE(SUM(variable_margin_partial_jpy_incl),0) AS variable_margin
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ? ${skuCond}
  `).get(...params);

  const steps = [
    { key: 'sales', label: '売上 (税込)', amount: s.sales_incl, kind: 'total', precision: 'flash' },
    { key: 'coupon_shop', label: 'ストアクーポン', amount: s.coupon_shop, kind: 'cost', precision: 'flash' },
    { key: 'use_point', label: 'ポイント利用', amount: s.use_point, kind: 'cost', precision: 'flash' },
    { key: 'mall_fee', label: 'モール手数料', amount: s.mall_fee_est, kind: 'cost', precision: 'est' },
    { key: 'shipping', label: '送料', amount: s.shipping, kind: 'cost', precision: 'est' },
    { key: 'cogs', label: '原価 (snapshot)', amount: s.cogs, kind: 'cost', precision: 'flash' },
    { key: 'variable_margin', label: '粗利 (変動利益 L1)', amount: s.variable_margin, kind: sku ? 'total' : 'subtotal', precision: 'flash' },
  ];

  // 確定寄せ延長 (全体表示のみ)。部分月への全額請求適用を避けるため、
  // 期間 = 確定済み完全月の集合と完全一致する場合に限る (Codex R1 medium と同思想)
  let confirmedApplied = false;
  if (!sku) {
    const months = completeMonthsInRange(from, to);
    const coversWholeRange = months.length > 0
      && monthStart(months[0]) === from && monthEnd(months[months.length - 1]) === to;
    if (coversWholeRange) {
      const confirmed = months.map(ym => confirmedMonth(db, ym));
      if (confirmed.every(c => c !== null)) {
        const adCost = confirmed.reduce((sum, c) => sum + c.ad_cost, 0);
        const pfFee = confirmed.reduce((sum, c) => sum + c.pf_fee, 0);
        steps.push(
          { key: 'mall_fee_reverse', label: '手数料推定を戻す', amount: s.mall_fee_est, kind: 'income', precision: 'est' },
          { key: 'pf_fee', label: 'PF手数料 (請求明細実額)', amount: pfFee, kind: 'cost', precision: 'settled' },
          { key: 'ad_cost', label: '広告費 (請求明細実額)', amount: adCost, kind: 'cost', precision: 'settled' },
          { key: 'full_margin', label: '実質利益 (確定寄せ)', amount: s.variable_margin + s.mall_fee_est - pfFee - adCost, kind: 'total', precision: 'settled' },
        );
        confirmedApplied = true;
      }
    }
  }
  return {
    from, to, sku: sku || null, confirmed_applied: confirmedApplied,
    steps: steps.map(x => ({ ...x, amount: Math.round(x.amount) })),
  };
}

// ─── 利益分析タブ: SKU 別利益テーブル (F-4) ───
// 最新日の ne_code / resolution_method を取るための連結ソートキー。
// ne_code / resolution_method に '|' は現れない前提 (NE コードは英数記号、method は enum)
const LATEST_EXPR = `MAX(date_jst || '|' || COALESCE(ne_code,'') || '|' || resolution_method)`;

export function getSkuProfit(from, to, opts = {}) {
  const db = getMirrorDB();
  let rows = db.prepare(`
    SELECT
      yahoo_sku_key,
      MAX(variant_key)                                   AS variant_key,
      MAX(product_name)                                  AS product_name,
      ${LATEST_EXPR}                                     AS latest_key,
      MAX(unresolved_sku_flag)                           AS ever_unresolved,
      SUM(units_ordered)                                 AS units_ordered,
      SUM(units_net_sold)                                AS units_net,
      SUM(gross_sales_jpy_incl)                          AS sales_incl,
      SUM(coupon_shop_jpy_incl)                          AS coupon_shop,
      SUM(use_point_jpy_incl)                            AS use_point,
      SUM(mall_fee_jpy_incl)                             AS mall_fee_est,
      SUM(shipping_cost_jpy_incl)                        AS shipping,
      SUM(cogs_amount_jpy_incl)                          AS cogs,
      SUM(variable_margin_partial_jpy_incl)              AS variable_margin,
      MIN(is_cost_complete)                              AS all_cost_complete,
      MAX(CASE WHEN cost_status <> 'complete' THEN cost_status END) AS cost_status_sample
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY yahoo_sku_key
  `).all(from, to).map(r => {
    const parts = (r.latest_key || '||').split('|');
    const neCode = parts[1] || null;
    const resolution = parts[2] || 'unresolved';
    return {
      yahoo_sku_key: r.yahoo_sku_key,
      ne_code: neCode,
      variant_key: r.variant_key || '',
      product_name: r.product_name || '',
      resolution_method: resolution,
      units_net: r.units_net,
      sales_incl: Math.round(r.sales_incl),
      coupon_shop: Math.round(r.coupon_shop),
      use_point: Math.round(r.use_point),
      mall_fee_est: Math.round(r.mall_fee_est),
      shipping: Math.round(r.shipping),
      cogs: Math.round(r.cogs),
      variable_margin: Math.round(r.variable_margin),
      margin_pct: r.sales_incl > 0 ? Math.round(r.variable_margin / r.sales_incl * 1000) / 10 : null,
      cost_status: r.all_cost_complete === 1 ? 'complete' : (r.cost_status_sample || 'missing_cost'),
      // 色分け: 期間内に1日でも未解決 (=原価0円計上) があれば警告 (最新日だけ解決済みでも
      // 期間集計の粗利には過大分が混ざる — Codex P2-R1 High) / 赤字
      flag: r.ever_unresolved === 1 ? 'unresolved' : r.variable_margin < 0 ? 'loss' : 'ok',
    };
  });

  const q = (opts.q || '').trim().toLowerCase();
  if (q) {
    rows = rows.filter(r =>
      r.yahoo_sku_key.toLowerCase().includes(q)
      || (r.ne_code || '').toLowerCase().includes(q)
      || r.product_name.toLowerCase().includes(q));
  }

  const sortKey = ['sales_incl', 'units_net', 'coupon_shop', 'use_point', 'cogs', 'variable_margin', 'margin_pct', 'yahoo_sku_key'].includes(opts.sort) ? opts.sort : 'variable_margin';
  const dir = opts.dir === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    const av = a[sortKey], bv = b[sortKey];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return av < bv ? -dir : av > bv ? dir : 0;
  });

  const total = rows.length;
  const limit = Math.max(1, Math.min(Number(opts.limit) || 100, 20000));
  const offset = Math.max(Number(opts.offset) || 0, 0);
  return { from, to, total, rows: rows.slice(offset, offset + limit) };
}

// ─── SKU 詳細 (ドリルダウン: 日次トレンド + マスタ情報) ───
export function getSkuDetail(sku, from, to) {
  const db = getMirrorDB();
  const daily = db.prepare(`
    SELECT date_jst, units_net_sold AS units_net,
      gross_sales_jpy_incl AS sales_incl,
      coupon_shop_jpy_incl AS coupon_shop, use_point_jpy_incl AS use_point,
      mall_fee_jpy_incl AS mall_fee_est, shipping_cost_jpy_incl AS shipping,
      cogs_amount_jpy_incl AS cogs, variable_margin_partial_jpy_incl AS variable_margin,
      resolution_method, cost_status, unit_cost_snapshot_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE yahoo_sku_key = ? AND date_jst >= ? AND date_jst <= ?
    ORDER BY date_jst
  `).all(sku, from, to).map(r => ({
    ...r,
    sales_incl: Math.round(r.sales_incl),
    variable_margin: Math.round(r.variable_margin),
  }));

  const latest = db.prepare(`
    SELECT ne_code, variant_key, product_name, resolution_method, unit_cost_snapshot_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE yahoo_sku_key = ?
    ORDER BY date_jst DESC LIMIT 1
  `).get(sku) || null;

  // 統計 (PV/訪問者/購買率/カート) の日次 — mall-csv-fetcher 由来、fact とは別ソース
  let trafficDaily = [];
  try {
    trafficDaily = db.prepare(`
      SELECT date_jst, SUM(COALESCE(pv_premium_ship,0) + COALESCE(pv_normal,0)) AS pv,
        SUM(COALESCE(visitors,0)) AS visitors, SUM(COALESCE(buyers,0)) AS buyers,
        SUM(COALESCE(cart_adds,0)) AS cart_adds
      FROM mirror_yahoo_item_daily
      WHERE ${TRAFFIC_KEY} = ? AND date_jst >= ? AND date_jst <= ?
      GROUP BY date_jst ORDER BY date_jst
    `).all(sku, from, to);
  } catch { /* mirror表未作成 */ }

  let master = null;
  if (latest?.ne_code) {
    master = db.prepare(`
      SELECT 商品コード AS ne_code, 商品名 AS product_name, 原価 AS unit_cost, 標準売価 AS list_price, 消費税率 AS tax_rate
      FROM mirror_products WHERE 商品コード = ?
    `).get(latest.ne_code) || null;
  }
  return { sku, from, to, latest, master, daily, traffic_daily: trafficDaily };
}

// ─── 売れ筋分析タブ (F-4b) ───
// 軸: units (販売数) / sales (売上) / margin (粗利L1)。ABC分析・急上昇急落・曜日パターン
// (Yahoo独自: 5のつく日 = 毎月5/15/25日 と 日曜 のハイライト)・スパークライン・新商品バッジ。
// カテゴリ別売れ筋は分類SSoTテーブルが mirror 未同期のため将来PR (要件 F-4b-4)。
function bestsellerSkuAgg(db, from, to) {
  return db.prepare(`
    SELECT
      yahoo_sku_key,
      MAX(product_name)                     AS product_name,
      ${LATEST_EXPR}                        AS latest_key,
      MAX(unresolved_sku_flag)              AS ever_unresolved,
      SUM(units_net_sold)                   AS units_net,
      SUM(gross_sales_jpy_incl)             AS sales_incl,
      SUM(variable_margin_partial_jpy_incl) AS variable_margin
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY yahoo_sku_key
  `).all(from, to);
}

export function getBestsellers(from, to, axis) {
  const db = getMirrorDB();
  const settings = getSettings();
  const days = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  const prevTo = addDays(from, -1);
  const prevFrom = addDays(prevTo, -(days - 1));

  const cur = bestsellerSkuAgg(db, from, to);
  const prevMap = new Map(bestsellerSkuAgg(db, prevFrom, prevTo).map(r => [r.yahoo_sku_key, r]));

  // 初売上日 (新商品バッジ用)。当期に売上のある SKU に限定して全期間 MIN を取る
  // (全 SKU 全履歴 GROUP BY はデータ増で重くなる — Codex P3-R1 low)
  const firstSaleMap = new Map(db.prepare(`
    SELECT yahoo_sku_key, MIN(date_jst) AS first_date
    FROM mirror_yahoo_finance_sku_daily
    WHERE units_net_sold > 0
      AND yahoo_sku_key IN (
        SELECT DISTINCT yahoo_sku_key FROM mirror_yahoo_finance_sku_daily
        WHERE date_jst >= ? AND date_jst <= ?
      )
    GROUP BY yahoo_sku_key
  `).all(from, to).map(r => [r.yahoo_sku_key, r.first_date]));
  const newCutoff = addDays(jstToday(), -settings.new_product_days);

  const traffic = trafficBySku(from, to);
  let rows = cur.map(r => {
    const parts = (r.latest_key || '||').split('|');
    const prev = prevMap.get(r.yahoo_sku_key);
    const t = traffic.get(r.yahoo_sku_key);
    return {
      pv: t ? t.pv : null,
      cvr_pct: t ? t.cvr_pct : null,
      cart_adds: t ? t.cart_adds : null,
      yahoo_sku_key: r.yahoo_sku_key,
      ne_code: parts[1] || null,
      product_name: r.product_name || '',
      unresolved: r.ever_unresolved === 1,
      units_net: r.units_net,
      sales_incl: Math.round(r.sales_incl),
      variable_margin: Math.round(r.variable_margin),
      margin_pct: r.sales_incl > 0 ? Math.round(r.variable_margin / r.sales_incl * 1000) / 10 : null,
      prev_units: prev ? prev.units_net : 0,
      units_growth_pct: prev && prev.units_net > 0
        ? Math.round((r.units_net - prev.units_net) / prev.units_net * 1000) / 10
        : (r.units_net > 0 ? null : 0),   // null = 前期実績なし (新規扱い)
      is_new: (firstSaleMap.get(r.yahoo_sku_key) || '') >= newCutoff,
      first_sale_date: firstSaleMap.get(r.yahoo_sku_key) || null,
    };
  });

  // ABC 分析 (売上累積構成比)
  const byRevenue = [...rows].sort((a, b) => b.sales_incl - a.sales_incl);
  const totalRevenue = byRevenue.reduce((s, r) => s + Math.max(0, r.sales_incl), 0);
  // 分類は「加算前の累積比」で判定 (加算後だと売上が1SKUに集中した時にA群が0件になる —
  // 例: 構成比90%の先頭SKUが加算後90%>80でB落ち。Codex P3-R1 medium)
  let cum = 0;
  const abcCount = { A: 0, B: 0, C: 0 };
  const abcRevenue = { A: 0, B: 0, C: 0 };
  for (const r of byRevenue) {
    const pctBefore = totalRevenue > 0 ? cum / totalRevenue * 100 : 100;
    cum += Math.max(0, r.sales_incl);
    r.abc = pctBefore < settings.abc_a_pct ? 'A' : pctBefore < settings.abc_b_pct ? 'B' : 'C';
    abcCount[r.abc]++;
    abcRevenue[r.abc] += Math.max(0, r.sales_incl);
  }

  const axisKey = axis === 'units' ? 'units_net' : axis === 'margin' ? 'variable_margin' : 'sales_incl';
  rows = [...rows].sort((a, b) => b[axisKey] - a[axisKey]);

  // 急上昇 / 急落 (当期 or 前期 N 個以上のみ)
  const minU = settings.movers_min_units;
  const movers = rows.filter(r => r.prev_units >= minU || r.units_net >= minU);
  const risers = [...movers].filter(r => r.units_growth_pct !== null && r.units_growth_pct > 0).sort((a, b) => b.units_growth_pct - a.units_growth_pct).slice(0, 20);
  const fallers = [...movers].filter(r => r.units_growth_pct !== null && r.units_growth_pct < 0).sort((a, b) => a.units_growth_pct - b.units_growth_pct).slice(0, 20);

  // 曜日パターン (販売数と売上。日曜 = LYPプレミアム系イベントで強い想定 → UI でハイライト)
  const weekday = db.prepare(`
    SELECT CAST(strftime('%w', date_jst) AS INTEGER) AS dow,
      SUM(units_net_sold) AS units, SUM(gross_sales_jpy_incl) AS sales,
      COUNT(DISTINCT date_jst) AS days
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY dow ORDER BY dow
  `).all(from, to).map(r => ({
    dow: r.dow, days: r.days,
    avg_units: r.days > 0 ? Math.round(r.units / r.days * 10) / 10 : 0,
    avg_sales: r.days > 0 ? Math.round(r.sales / r.days) : 0,
  }));

  // 5のつく日 (毎月5/15/25日) vs 通常日 — Yahoo独自の販売リズム可視化
  const goen = db.prepare(`
    SELECT CASE WHEN substr(date_jst, 9, 2) IN ('05', '15', '25') THEN 1 ELSE 0 END AS is_goen,
      SUM(units_net_sold) AS units, SUM(gross_sales_jpy_incl) AS sales,
      COUNT(DISTINCT date_jst) AS days
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY is_goen
  `).all(from, to);
  const goenRow = goen.find(g => g.is_goen === 1);
  const normalRow = goen.find(g => g.is_goen === 0);
  const goenSummary = {
    goen_days: goenRow?.days || 0,
    goen_avg_sales: goenRow?.days > 0 ? Math.round(goenRow.sales / goenRow.days) : null,
    normal_days: normalRow?.days || 0,
    normal_avg_sales: normalRow?.days > 0 ? Math.round(normalRow.sales / normalRow.days) : null,
    lift_pct: goenRow?.days > 0 && normalRow?.days > 0 && normalRow.sales > 0
      ? Math.round((goenRow.sales / goenRow.days) / (normalRow.sales / normalRow.days) * 1000) / 10 - 100
      : null,
  };

  // 上位 30 SKU の日次スパークライン
  const topSkus = rows.slice(0, 30).map(r => r.yahoo_sku_key);
  const sparkMap = new Map();
  if (topSkus.length > 0) {
    const ph = topSkus.map(() => '?').join(',');
    const sparkRows = db.prepare(`
      SELECT yahoo_sku_key, date_jst, SUM(units_net_sold) AS units
      FROM mirror_yahoo_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ? AND yahoo_sku_key IN (${ph})
      GROUP BY yahoo_sku_key, date_jst ORDER BY date_jst
    `).all(from, to, ...topSkus);
    for (const r of sparkRows) {
      if (!sparkMap.has(r.yahoo_sku_key)) sparkMap.set(r.yahoo_sku_key, []);
      sparkMap.get(r.yahoo_sku_key).push({ d: r.date_jst, u: r.units });
    }
  }
  for (const r of rows.slice(0, 30)) r.spark = sparkMap.get(r.yahoo_sku_key) || [];

  return {
    from, to, prev_from: prevFrom, prev_to: prevTo, axis: axisKey,
    total_skus: rows.length,
    abc: { count: abcCount, revenue: { A: Math.round(abcRevenue.A), B: Math.round(abcRevenue.B), C: Math.round(abcRevenue.C) }, total_revenue: Math.round(totalRevenue) },
    ranking: rows.slice(0, 100),
    risers, fallers, weekday, goen: goenSummary,
  };
}

// ─── 統計 (mall-csv-fetcher 自動取得 → mirror同期済み) の統合 ───
// SKUキー: item_daily の sub_code!='' → sub_code else item_code = finance の yahoo_sku_key と同一規則
const TRAFFIC_KEY = `CASE WHEN sub_code <> '' THEN sub_code ELSE item_code END`;

// 期間×SKU の PV/CVR 集計 map (利益・売れ筋への JOIN 用)
export function trafficBySku(from, to) {
  const db = getMirrorDB();
  try {
    return new Map(db.prepare(`
      SELECT ${TRAFFIC_KEY} AS sku,
        SUM(COALESCE(pv_premium_ship,0) + COALESCE(pv_normal,0)) AS pv,
        SUM(COALESCE(visitors,0)) AS visitors, SUM(COALESCE(buyers,0)) AS buyers,
        SUM(COALESCE(cart_adds,0)) AS cart_adds, SUM(COALESCE(favorites,0)) AS favorites
      FROM mirror_yahoo_item_daily WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY sku
    `).all(from, to).map(r => [r.sku, {
      pv: r.pv, visitors: r.visitors, buyers: r.buyers, cart_adds: r.cart_adds, favorites: r.favorites,
      cvr_pct: r.visitors > 0 ? Math.round(r.buyers / r.visitors * 1000) / 10 : null,
    }]));
  } catch { return new Map(); /* mirror表未作成でも既存機能は動かす */ }
}

// 検索KWタブ: KW別ランキング + 前期比 (伸び/落ち)
export function getSearchKeywords(from, to) {
  const db = getMirrorDB();
  const days = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  const pTo = addDays(from, -1), pFrom = addDays(pTo, -(days - 1));
  const agg = (f, t) => db.prepare(`
    SELECT keyword, SUM(COALESCE(inflow,0)) AS inflow, SUM(COALESCE(sales_yen,0)) AS sales,
      SUM(COALESCE(orders,0)) AS orders, MIN(rank) AS best_rank
    FROM mirror_yahoo_keyword_daily WHERE date_jst >= ? AND date_jst <= ? GROUP BY keyword
  `).all(f, t);
  let cur = [], prevMap = new Map();
  try {
    cur = agg(from, to);
    prevMap = new Map(agg(pFrom, pTo).map(r => [r.keyword, r]));
  } catch { /* mirror表未作成 */ }
  const rows = cur.map(r => {
    const p = prevMap.get(r.keyword);
    return {
      keyword: r.keyword, inflow: r.inflow, sales: Math.round(r.sales), orders: r.orders,
      best_rank: r.best_rank,
      cvr_pct: r.inflow > 0 ? Math.round(r.orders / r.inflow * 1000) / 10 : null,
      prev_inflow: p ? p.inflow : 0,
      inflow_growth_pct: p && p.inflow > 0 ? Math.round((r.inflow - p.inflow) / p.inflow * 1000) / 10 : null,
    };
  }).sort((a, b) => b.inflow - a.inflow);
  const freshness = (() => {
    try { return db.prepare(`SELECT MAX(date_jst) AS d FROM mirror_yahoo_keyword_daily`).get().d; } catch { return null; }
  })();
  return { from, to, prev_from: pFrom, prev_to: pTo, data_to: freshness, rows: rows.slice(0, 300) };
}

// 集客タブ: デバイス別 / 流入・離脱 / 客層
export function getAcquisition(from, to) {
  const db = getMirrorDB();
  const safe = (fn) => { try { return fn(); } catch { return []; } };
  const device = safe(() => db.prepare(`
    SELECT date_jst, device, sales_yen, pageviews FROM mirror_yahoo_store_device_daily
    WHERE date_jst >= ? AND date_jst <= ? AND device <> 'all' ORDER BY date_jst
  `).all(from, to));
  const inflow = safe(() => db.prepare(`
    SELECT date_jst, inflow_visitors, purchase_visitors, purchase_ratio_pct, exit_ratio_pct
    FROM mirror_yahoo_inflow_daily WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst
  `).all(from, to));
  const userAttr = safe(() => db.prepare(`
    SELECT gender, age_band, buyer_class, SUM(COALESCE(visitors,0)) AS visitors
    FROM mirror_yahoo_user_attr_daily WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY gender, age_band, buyer_class ORDER BY visitors DESC
  `).all(from, to));
  return { from, to, device, inflow, user_attr: userAttr };
}

// 本日速報バンド (flash_hourly の最新日。今日タイル (finance fact) とは独立表示 — 精度混在防止)
export function getFlashLatest() {
  const db = getMirrorDB();
  try {
    const latest = db.prepare(`SELECT MAX(date_jst) AS d FROM mirror_yahoo_flash_hourly`).get().d;
    if (!latest) return { date: null, rows: [] };
    const rows = db.prepare(`
      SELECT hour_slot, sales_yen, orders, units, pageviews, visitors
      FROM mirror_yahoo_flash_hourly WHERE date_jst = ? AND device = 'all' ORDER BY hour_slot
    `).all(latest);
    const total = rows.reduce((s, r) => s + (r.sales_yen || 0), 0);
    return { date: latest, total_sales: total, rows };
  } catch { return { date: null, rows: [] }; }
}

// ─── SKU 未解決一覧 (F-4: f_yahoo_sku_map 登録の解消導線) ───
// 未解決 = 原価 0 円計上で粗利が過大に出る。miniPC の f_yahoo_sku_map へ手動登録すると
// 翌朝の fact rebuild (当月分) から解決される。過去月は再 build 時に反映。
//
// 登録キーについて (build SQL sku_resolved CTE で裏取り済み — Codex P2-R1 medium):
//   yahoo_sku_key = sub_code (variant あり) or item_id (variant なし) がそのまま入っており、
//   f_yahoo_sku_map の lookup も同じ値 (sub_code 優先 → item_id) で行われる。
//   したがって f_yahoo_sku_map.yahoo_key に登録すべき値は常に yahoo_sku_key そのもの。
//   item_id は fact に保存されていないため復元しない (variant_key = sub_code 全体)。
export function getUnresolved(daysBack = 180) {
  const db = getMirrorDB();
  const from = addDays(jstToday(), -Math.min(Math.max(Number(daysBack) || 180, 1), 730));
  const rows = db.prepare(`
    SELECT
      yahoo_sku_key,
      MAX(variant_key)              AS variant_key,
      MAX(product_name)             AS product_name,
      MIN(date_jst)                 AS first_seen,
      MAX(date_jst)                 AS last_seen,
      SUM(units_net_sold)           AS units_net,
      SUM(gross_sales_jpy_incl)     AS sales_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE unresolved_sku_flag = 1 AND date_jst >= ?
    GROUP BY yahoo_sku_key
    ORDER BY sales_incl DESC
    LIMIT 500
  `).all(from);

  // m_products の前方一致候補 (完全一致は build 時に試行済みなので LIKE で緩める)。
  // キー全体 + 末尾セグメント ('-'以降) を落としたヒューリスティック prefix の2通りで探す
  const candidateStmt = db.prepare(`
    SELECT 商品コード AS ne_code, 商品名 AS product_name
    FROM mirror_products
    WHERE LOWER(商品コード) LIKE LOWER(?) || '%'
    ORDER BY LENGTH(商品コード) LIMIT 3
  `);
  const out = rows.map(r => {
    const prefixes = [r.yahoo_sku_key];
    // sub_code の末尾セグメント違いで NE 未登録のケースが多いため、variant ありの時のみ
    // 末尾 '-xxx' を落とした prefix も試す (variant なしの短い item_id では誤ヒットが増えるだけ)
    const lastDash = r.yahoo_sku_key.lastIndexOf('-');
    if (r.variant_key && lastDash > 0) prefixes.push(r.yahoo_sku_key.slice(0, lastDash));
    const seen = new Set();
    const candidates = [];
    for (const p of prefixes) {
      let found = [];
      try { found = candidateStmt.all(p); } catch { /* mirror_products 無しでも一覧は返す */ }
      for (const c of found) {
        if (!seen.has(c.ne_code) && candidates.length < 3) { seen.add(c.ne_code); candidates.push(c); }
      }
    }
    return {
      yahoo_sku_key: r.yahoo_sku_key,           // = f_yahoo_sku_map.yahoo_key に登録する値
      variant_key: r.variant_key || '',          // = sub_code (variant なしは空)
      product_name: r.product_name || '',
      first_seen: r.first_seen,
      last_seen: r.last_seen,
      units_net: r.units_net,
      sales_incl: Math.round(r.sales_incl),
      candidates,
    };
  });
  return { from, days_back: daysBack, total: out.length, rows: out };
}
