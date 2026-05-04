/**
 * 商品収益性ダッシュボード タブB: 在庫整理・撤退判断支援 API
 *
 * マウント先: /apps/profit-analysis/api/inventory
 * feature flag: INVENTORY_DECISION_ENABLED（OFFなら全エンドポイント 503）
 *
 * 現状の実装範囲（PR2b）:
 *   GET    /thresholds            - 閾値マトリクス・早期警戒・処分率の取得
 *   PUT    /thresholds            - 上記の更新
 *   GET    /status/:code          - 商品コード単位の現ステータス取得
 *   POST   /status                - 撤退判断ステータス CRUD
 *   GET    /candidates            - 5分類ビュー（PR2c で実装、現状 501）
 */
import { Router } from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import {
  seedDefaultsIfMissing,
  getRetirementThresholds,
  getClassificationThresholds,
  getEarlyWarning,
  getDisposalRateDefault,
  setSetting,
  validateRetirementThresholds,
  validateEarlyWarning,
  KEYS,
} from './retirement-thresholds.js';
import { getCandidates } from './candidates.js';

const router = Router();

// ─── Feature flag middleware ───

// Dark Launch: INVENTORY_DECISION_ENABLED=true でのみ有効化。
// 未設定・'false' は全エンドポイント 503 を返す。
function featureEnabled() {
  const flag = process.env.INVENTORY_DECISION_ENABLED;
  return flag === 'true' || flag === '1';
}

router.use((req, res, next) => {
  if (!featureEnabled()) {
    return res.status(503).json({
      error: 'inventory-decision feature is disabled',
      hint: 'set INVENTORY_DECISION_ENABLED=true to enable',
    });
  }
  next();
});

// ─── 閾値 GET/PUT ───

/**
 * 現在の閾値・早期警戒・処分率を一括取得
 * 初回アクセス時はデフォルト値を seed する
 */
router.get('/thresholds', (req, res) => {
  try {
    const db = getMirrorDB();
    seedDefaultsIfMissing(db);
    res.json({
      retirement: getRetirementThresholds(db),
      classification: getClassificationThresholds(db),
      early_warning: getEarlyWarning(db),
      disposal_rate_default: getDisposalRateDefault(db),
    });
  } catch (e) {
    console.error('[inventory-decision] GET /thresholds error:', e);
    res.status(500).json({ error: e.message });
  }
});

/**
 * 閾値・早期警戒・処分率の更新（部分更新可）
 * body: { retirement?, classification?, early_warning?, disposal_rate_default?, updated_by? }
 */
router.put('/thresholds', (req, res) => {
  try {
    const db = getMirrorDB();
    const { retirement, classification, early_warning, disposal_rate_default, updated_by } = req.body || {};
    const who = typeof updated_by === 'string' && updated_by ? updated_by : (req.session?.email || 'admin');

    const tx = db.transaction(() => {
      if (retirement !== undefined) {
        validateRetirementThresholds(retirement);
        setSetting(db, KEYS.RETIREMENT, retirement, who);
      }
      if (classification !== undefined) {
        // classification は現時点で構造が柔軟なので最低限のオブジェクトチェックのみ
        if (!classification || typeof classification !== 'object') {
          throw new Error('classification はオブジェクトである必要があります');
        }
        setSetting(db, KEYS.CLASSIFICATION, classification, who);
      }
      if (early_warning !== undefined) {
        validateEarlyWarning(early_warning);
        setSetting(db, KEYS.EARLY_WARNING, early_warning, who);
      }
      if (disposal_rate_default !== undefined) {
        const v = Number(disposal_rate_default);
        if (!Number.isFinite(v) || v <= 0 || v > 1) {
          throw new Error('disposal_rate_default は 0 より大きく 1 以下の数値');
        }
        setSetting(db, KEYS.DISPOSAL_RATE, { value: v }, who);
      }
    });
    tx();
    res.json({ ok: true });
  } catch (e) {
    console.error('[inventory-decision] PUT /thresholds error:', e.message);
    res.status(400).json({ error: e.message });
  }
});

// ─── ステータス CRUD (product_retirement_status) ───

export const VALID_STATUSES = new Set([
  '継続', '値下げ検討', '撤退検討', '撤退確定',
  // 自社追加ステータス（設計書セクション14 / Codex 12回目 #9）
  '消化計画中', 'リブランディング検討', '再生産判断中',
]);

// 追加ステータスは next_review_date と reason 必須（設計書§14 §4-8）
// Codex PR2b review Medium #1 反映: 設計書では reason も必須
export const REVIEW_REQUIRED_STATUSES = new Set([
  '消化計画中', 'リブランディング検討', '再生産判断中',
]);

const RETIREMENT_STATUSES = new Set(['撤退検討', '撤退確定']);
// 消化計画中: plan_details（target_month, monthly_sales_target）必須
const PLAN_DETAILS_REQUIRED_STATUSES = new Set(['消化計画中']);

// Codex PR3 実装 R1 Medium 1 反映: 日付形式 regex（client と同じ境界）
const RE_YYYY_MM_DD = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/;
const RE_YYYY_MM = /^\d{4}-(0[1-9]|1[0-2])$/;

/**
 * YYYY-MM-DD 文字列が実在日か検証（Codex PR3 実装 R2 Low-Medium 2 反映）
 * 2026-02-31 や 2026-04-31 のような無効日を拒否する
 */
function isValidRealDate(str) {
  if (typeof str !== 'string' || !RE_YYYY_MM_DD.test(str)) return false;
  const [y, m, d] = str.split('-').map(Number);
  // Date オブジェクトに変換して round-trip 比較
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

/**
 * POST /status のリクエスト body を検証する（純関数、Test 12 が直接呼ぶ）
 * @param body リクエストボディ
 * @throws Error バリデーション失敗時
 */
export function validateStatusBody(body) {
  const b = body || {};
  if (!b.ne_product_code || typeof b.ne_product_code !== 'string') {
    throw new Error('ne_product_code は必須');
  }
  if (!VALID_STATUSES.has(b.status)) {
    throw new Error(`status が不正: ${b.status} (有効値: ${[...VALID_STATUSES].join(', ')})`);
  }
  if (REVIEW_REQUIRED_STATUSES.has(b.status)) {
    if (!b.next_review_date) {
      throw new Error(`status=${b.status} は next_review_date 必須`);
    }
    // Codex PR2b review Medium #1 反映: 追加3ステータスは reason も必須
    if (!b.reason) {
      throw new Error(`status=${b.status} は reason 必須`);
    }
  }
  if (RETIREMENT_STATUSES.has(b.status) && !b.reason) {
    throw new Error(`status=${b.status} は reason 必須`);
  }
  // Codex PR3 実装 R1 Medium 1 + R2 Low-Medium 2 反映:
  //   next_review_date が渡されていたら YYYY-MM-DD 形式 + 実在日
  if (b.next_review_date) {
    if (!isValidRealDate(b.next_review_date)) {
      throw new Error('next_review_date は YYYY-MM-DD 形式の実在日');
    }
  }
  // Codex PR2b review Medium #2 反映: disposal_rate の範囲チェック（PUT /thresholds と揃える）
  if (b.disposal_rate !== undefined && b.disposal_rate !== null) {
    const v = Number(b.disposal_rate);
    if (!Number.isFinite(v) || v <= 0 || v > 1) {
      throw new Error('disposal_rate は 0 より大きく 1 以下の数値');
    }
  }
  // Codex PR3 実装 R2 Medium 1 反映: 消化計画中 は plan_details 必須（API 直叩きでも担保）
  if (PLAN_DETAILS_REQUIRED_STATUSES.has(b.status)) {
    if (!b.plan_details || typeof b.plan_details !== 'object' || Array.isArray(b.plan_details)) {
      throw new Error(`status=${b.status} は plan_details オブジェクト必須`);
    }
    if (!b.plan_details.target_month) throw new Error(`status=${b.status} は plan_details.target_month 必須`);
    if (b.plan_details.monthly_sales_target === undefined || b.plan_details.monthly_sales_target === null) {
      throw new Error(`status=${b.status} は plan_details.monthly_sales_target 必須`);
    }
  }
  // plan_details のフィールド検証（渡された場合のみ、フォーマット・値域チェック）
  if (b.plan_details !== undefined && b.plan_details !== null) {
    if (typeof b.plan_details !== 'object' || Array.isArray(b.plan_details)) {
      throw new Error('plan_details はオブジェクト');
    }
    if (b.plan_details.target_month !== undefined) {
      if (typeof b.plan_details.target_month !== 'string' || !RE_YYYY_MM.test(b.plan_details.target_month)) {
        throw new Error('plan_details.target_month は YYYY-MM 形式（月は 01〜12）');
      }
    }
    if (b.plan_details.monthly_sales_target !== undefined) {
      const n = Number(b.plan_details.monthly_sales_target);
      if (!Number.isInteger(n) || n <= 0) {
        throw new Error('plan_details.monthly_sales_target は正の整数');
      }
    }
  }
}

/**
 * 商品単位の現ステータス取得
 * GET /status/:code
 */
router.get('/status/:code', (req, res) => {
  try {
    const db = getMirrorDB();
    const code = req.params.code;
    const row = db.prepare(`SELECT * FROM product_retirement_status WHERE ne_product_code = ?`).get(code);
    if (!row) return res.status(404).json({ error: 'not found', ne_product_code: code });
    // JSON カラムはそのまま文字列で返す（クライアント側でパース）
    res.json(row);
  } catch (e) {
    console.error('[inventory-decision] GET /status error:', e);
    res.status(500).json({ error: e.message });
  }
});

/**
 * 撤退判断ステータス CRUD（UPSERT）
 * POST /status
 * body: {
 *   ne_product_code, status, decided_by?, reason?, next_review_date?,
 *   plan_details, decision_metrics, thresholds,
 *   disposal_rate
 * }
 * 判断時メトリクス・閾値・処分率をスナップショットとして保存。
 * body のキーは `thresholds` / `decision_metrics` / `plan_details`（`*_json` サフィックス無し）、
 * DB カラムのみ `thresholds_json` / `decision_metrics_json` / `plan_details_json`。
 */
router.post('/status', (req, res) => {
  try {
    const db = getMirrorDB();
    const body = req.body || {};
    validateStatusBody(body);
    const {
      ne_product_code, status, decided_by, reason, next_review_date,
      plan_details, decision_metrics, thresholds, disposal_rate,
    } = body;

    const who = typeof decided_by === 'string' && decided_by ? decided_by : (req.session?.email || 'admin');
    const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);

    const stmt = db.prepare(`INSERT INTO product_retirement_status
      (ne_product_code, status, decided_by, decided_at, reason, next_review_date,
       plan_details_json, decision_metrics_json, thresholds_json, disposal_rate, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(ne_product_code) DO UPDATE SET
        status = excluded.status,
        decided_by = excluded.decided_by,
        decided_at = excluded.decided_at,
        reason = excluded.reason,
        next_review_date = excluded.next_review_date,
        plan_details_json = excluded.plan_details_json,
        decision_metrics_json = excluded.decision_metrics_json,
        thresholds_json = excluded.thresholds_json,
        disposal_rate = excluded.disposal_rate,
        updated_at = excluded.updated_at
    `);
    stmt.run(
      ne_product_code, status, who, ts,
      reason || null,
      next_review_date || null,
      plan_details ? JSON.stringify(plan_details) : null,
      decision_metrics ? JSON.stringify(decision_metrics) : null,
      thresholds ? JSON.stringify(thresholds) : null,
      (disposal_rate !== undefined && disposal_rate !== null) ? Number(disposal_rate) : null,
      ts,
    );

    res.json({ ok: true, ne_product_code, status });
  } catch (e) {
    console.error('[inventory-decision] POST /status error:', e.message);
    res.status(400).json({ error: e.message });
  }
});

// ─── 5分類ビュー（PR2c） ───

/**
 * 候補リスト取得。売上分類を一次軸として必須指定。
 * Query params:
 *   sales_class: 1|2|3 （必須）
 *   period_days: 既定 90（Render mirror の日次制約に合わせる）
 */
router.get('/candidates', (req, res) => {
  try {
    const db = getMirrorDB();
    seedDefaultsIfMissing(db);

    const salesClass = String(req.query.sales_class || '');
    if (!['1', '2', '3'].includes(salesClass)) {
      return res.status(400).json({
        error: 'sales_class は 1|2|3 のいずれか必須',
        hint: '1=自社, 2=取引先限定, 3=仕入れ',
      });
    }
    // Codex PR2c Round 1 High #1 反映:
    //   mirror_sales_daily は直近90日分のみ保持するため、period_days は 1〜90 に制限。
    //   90日超は分子だけ小さく分母だけ大きい不整合計算になる。
    // Codex PR2c Round 1 Low #5 反映: 未指定のみ default 90、指定済み不正値は 400
    let periodDays;
    if (req.query.period_days === undefined) {
      periodDays = 90;
    } else {
      const n = Number(req.query.period_days);
      if (!Number.isInteger(n) || n <= 0 || n > 90) {
        return res.status(400).json({
          error: 'period_days は 1〜90 の整数 (mirror日次データの保持期間に合わせた制約)',
        });
      }
      periodDays = n;
    }

    const retirement = getRetirementThresholds(db);
    const classification = getClassificationThresholds(db);
    const earlyWarning = getEarlyWarning(db);

    const candidates = getCandidates(db, { salesClass, periodDays },
      { retirement, classification, earlyWarning });

    // 集計サマリー（分類別 + 理由別）
    //   Codex PR2c Round 1 Medium-Low #4 反映:
    //     分類外 を 計算不能 / 新商品保留 / 季節性保留 / 閾値外 等に分解してカウント
    const summary = {};
    const reason_summary = {};
    for (const c of candidates) {
      summary[c.classification] = (summary[c.classification] || 0) + 1;
      const key = `${c.classification}: ${c.reason}`;
      reason_summary[key] = (reason_summary[key] || 0) + 1;
    }

    res.json({
      meta: {
        sales_class: parseInt(salesClass, 10),
        period_days: periodDays,
        total: candidates.length,
        summary,
        reason_summary,
        generated_at: new Date().toISOString(),
      },
      thresholds: { retirement: retirement[salesClass], classification, early_warning: earlyWarning },
      candidates,
    });
  } catch (e) {
    console.error('[inventory-decision] GET /candidates error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ─── 経営向け在庫サマリ集計（GChat通知用） ───

/**
 * 経営インサイトGChat通知向けの在庫サマリを集計する。
 *
 * profit-analysis 旧仕様の制約:
 *   - 判定対象は自社倉庫のNE在庫のみ (mirror_stock_monthly_snapshot ベース)
 *   - 利益計算は楽天売価ベース (rakutenUnitProfit)
 *   - Amazon FBA・FBA輸送中・米国FBA は判定対象外
 *
 * このため、本サマリでは「総在庫」と「判定対象」を明確に分離して返す。
 * 通知メッセージ整形側で、両方を併記する形式で出すことで誤読を防ぐ。
 *
 * @param {Database} db warehouse-mirror.db
 * @param {object} opts { businessDate?: 'YYYY-MM-DD', today?: Date }
 * @returns {object} GChat通知に必要な構造化データ
 */
export function getInventorySummary(db, opts = {}) {
  const today = opts.today ? new Date(opts.today) : new Date();
  // 業務日 (JST) — opts.businessDate が指定されていればそれを使い、
  // なければ mirror_inv_daily_summary の最新日付を採用
  const latestRow = db.prepare(
    `SELECT MAX(business_date) AS d FROM mirror_inv_daily_summary`
  ).get();
  const businessDate = opts.businessDate || latestRow?.d;
  if (!businessDate) {
    return { error: 'no inventory data available' };
  }

  // ─── 1. 総在庫・対象外内訳 (mirror_inv_daily_summary から) ───
  const summaryRows = db.prepare(`
    SELECT category, total_value, source_status
    FROM mirror_inv_daily_summary
    WHERE business_date = ?
  `).all(businessDate);

  let totalInventory = 0;
  let ownWarehouseInventory = 0;
  const outOfScopeBreakdown = { fba_warehouse: 0, fba_inbound: 0, fba_us_warehouse: 0, fba_us_inbound: 0 };
  const partialCategories = [];
  let hasFailure = false;

  for (const r of summaryRows) {
    const v = Number(r.total_value) || 0;
    totalInventory += v;
    if (r.category === 'own_warehouse') {
      ownWarehouseInventory = v;
    } else if (Object.prototype.hasOwnProperty.call(outOfScopeBreakdown, r.category)) {
      outOfScopeBreakdown[r.category] = v;
    }
    if (r.source_status === 'failed' || r.source_status === 'no_source') hasFailure = true;
    if (r.source_status !== 'ok') partialCategories.push(`${r.category}(${r.source_status})`);
  }
  const outOfScopeValue = totalInventory - ownWarehouseInventory;

  // ─── 2. 比較値 (前日 / 前週 / 月初) — 総在庫 ───
  function getTotalAt(targetDate) {
    const row = db.prepare(`
      SELECT SUM(total_value) AS v
      FROM mirror_inv_daily_summary
      WHERE business_date = ? AND source_status IN ('ok', 'partial')
    `).get(targetDate);
    return row?.v || null;
  }
  function dateMinus(dateStr, days) {
    const d = new Date(dateStr + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() - days);
    return d.toISOString().slice(0, 10);
  }
  function monthStartOf(dateStr) {
    return dateStr.slice(0, 7) + '-01';
  }
  const prevDayTotal = getTotalAt(dateMinus(businessDate, 1));
  const weekAgoTotal = getTotalAt(dateMinus(businessDate, 7));
  const monthStartTotal = getTotalAt(monthStartOf(businessDate));

  function diff(today, prev) {
    if (prev == null || prev === 0) return { abs: null, pct: null };
    return { abs: today - prev, pct: ((today - prev) / prev) * 100 };
  }

  // ─── 3. profit-analysis ロジックで候補集計 (sales_class 1, 2, 3) ───
  seedDefaultsIfMissing(db);
  const retirement = getRetirementThresholds(db);
  const classification = getClassificationThresholds(db);
  const earlyWarning = getEarlyWarning(db);
  const settings = { retirement, classification, earlyWarning };

  // 全候補をマージ
  const allCandidates = [];
  for (const cls of ['1', '2', '3']) {
    try {
      const list = getCandidates(db, { salesClass: cls, periodDays: 90, today }, settings);
      for (const c of list) allCandidates.push(c);
    } catch (e) {
      console.error(`[inventory-summary] sales_class=${cls} 集計失敗:`, e.message);
    }
  }

  // 4. 分類別集計
  const classificationCounts = {};
  const classificationValues = {};
  for (const c of allCandidates) {
    const k = c.classification;
    classificationCounts[k] = (classificationCounts[k] || 0) + 1;
    const v = (c.metrics?.latest_stock || 0) * (c.metrics?.avg_stock_value && c.metrics?.avg_stock
      ? (c.metrics.avg_stock_value / c.metrics.avg_stock)  // 単位原価
      : 0);
    classificationValues[k] = (classificationValues[k] || 0) + v;
  }

  // 5. 撤退検討 (撤退候補) サマリ
  const retirementCandidates = allCandidates.filter(c => c.classification === '撤退候補');
  const retirementValue = retirementCandidates.reduce((s, c) => {
    const stock = c.metrics?.latest_stock || 0;
    const unitCost = (c.metrics?.avg_stock && c.metrics.avg_stock > 0)
      ? (c.metrics.avg_stock_value / c.metrics.avg_stock)
      : 0;
    return s + stock * unitCost;
  }, 0);
  // 仕入先別集計 (上位3)
  const supplierBreakdown = {};
  for (const c of retirementCandidates) {
    const sup = c.supplier_code || '不明';
    const stock = c.metrics?.latest_stock || 0;
    const unitCost = (c.metrics?.avg_stock && c.metrics.avg_stock > 0)
      ? (c.metrics.avg_stock_value / c.metrics.avg_stock)
      : 0;
    if (!supplierBreakdown[sup]) supplierBreakdown[sup] = { count: 0, value: 0 };
    supplierBreakdown[sup].count++;
    supplierBreakdown[sup].value += stock * unitCost;
  }
  const supplierTop3 = Object.entries(supplierBreakdown)
    .sort((a, b) => b[1].value - a[1].value)
    .slice(0, 3)
    .map(([code, v]) => ({ supplier_code: code, count: v.count, value: v.value }));

  // 6. 警戒対象 (撤退警戒) サマリ
  const warningCandidates = allCandidates.filter(c => c.classification === '撤退警戒');
  const warningValue = warningCandidates.reduce((s, c) => {
    const stock = c.metrics?.latest_stock || 0;
    const unitCost = (c.metrics?.avg_stock && c.metrics.avg_stock > 0)
      ? (c.metrics.avg_stock_value / c.metrics.avg_stock)
      : 0;
    return s + stock * unitCost;
  }, 0);
  const warningTop1 = Math.max(...warningCandidates.map(c => {
    const stock = c.metrics?.latest_stock || 0;
    const unitCost = (c.metrics?.avg_stock && c.metrics.avg_stock > 0)
      ? (c.metrics.avg_stock_value / c.metrics.avg_stock)
      : 0;
    return stock * unitCost;
  }), 0);

  // 7. 急落検知 (early_warning.type === 'drop')
  const dropCount = allCandidates.filter(c => c.early_warning?.type === 'drop').length;

  return {
    business_date: businessDate,
    total_inventory: {
      value: totalInventory,
      diff_prev_day: diff(totalInventory, prevDayTotal),
      diff_week_ago: diff(totalInventory, weekAgoTotal),
      diff_month_start: diff(totalInventory, monthStartTotal),
    },
    judgement_target: {
      value: ownWarehouseInventory,
      ratio_pct: totalInventory > 0 ? (ownWarehouseInventory / totalInventory) * 100 : 0,
      note: '自社倉庫・楽天系ロジック',
    },
    out_of_scope: {
      value: outOfScopeValue,
      ratio_pct: totalInventory > 0 ? (outOfScopeValue / totalInventory) * 100 : 0,
      breakdown: outOfScopeBreakdown,
      note: 'FBA・FBA輸送中・米国FBA は別ロジック/別ツール管理',
    },
    retirement_candidates: {
      count: retirementCandidates.length,
      value: retirementValue,
      supplier_top3: supplierTop3,
    },
    warning: {
      count: warningCandidates.length,
      value: warningValue,
      top1_value: warningTop1,
    },
    early_warning: {
      drop_count: dropCount,
    },
    classification: {
      good_stock: { count: classificationCounts['優良在庫'] || 0, value: classificationValues['優良在庫'] || 0 },
      observe: { count: classificationCounts['観察継続'] || 0, value: classificationValues['観察継続'] || 0 },
      price_down: { count: classificationCounts['値下げ候補'] || 0, value: classificationValues['値下げ候補'] || 0 },
      bundle_candidate: { count: classificationCounts['セット候補'] || 0, value: classificationValues['セット候補'] || 0 },
      other: {
        count: (classificationCounts['分類外'] || 0) + (classificationCounts['評価不能'] || 0),
        value: (classificationValues['分類外'] || 0) + (classificationValues['評価不能'] || 0),
      },
    },
    data_quality: {
      has_failure: hasFailure,
      partial_categories: partialCategories,
    },
    generated_at: new Date().toISOString(),
  };
}

/**
 * 経営インサイト通知用 集計サマリAPI (GChat通知ジョブから内部的に呼ばれる、また閉域API)
 * GET /summary?date=YYYY-MM-DD
 */
router.get('/summary', (req, res) => {
  try {
    const db = getMirrorDB();
    const businessDate = req.query.date;
    const summary = getInventorySummary(db, businessDate ? { businessDate } : {});
    res.json(summary);
  } catch (e) {
    console.error('[inventory-decision] GET /summary error:', e);
    res.status(500).json({ error: e.message });
  }
});

/**
 * 通知メッセージのプレビュー (送信なし、安全)
 * GET /summary/preview?date=YYYY-MM-DD
 */
router.get('/summary/preview', async (req, res) => {
  try {
    const { formatNotificationMessage } = await import('./notify-job.js');
    const db = getMirrorDB();
    const businessDate = req.query.date;
    const summary = getInventorySummary(db, businessDate ? { businessDate } : {});
    const text = formatNotificationMessage(summary);
    res.type('text/plain; charset=utf-8').send(text);
  } catch (e) {
    console.error('[inventory-decision] GET /summary/preview error:', e);
    res.status(500).json({ error: e.message });
  }
});

/**
 * 手動で通知ジョブを実行する (実際にGChatへ送信、テスト用)
 * POST /summary/notify-test
 *   feature flag (INVENTORY_DECISION_ENABLED) でガード済み (router.use)
 *   GCHAT_WEBHOOK_INSIGHT 未設定なら 412 で拒否
 */
router.post('/summary/notify-test', async (req, res) => {
  if (!process.env.GCHAT_WEBHOOK_INSIGHT) {
    return res.status(412).json({
      error: 'GCHAT_WEBHOOK_INSIGHT が未設定',
      hint: 'Render管理画面で環境変数を設定してください',
    });
  }
  try {
    const { runNotificationJob } = await import('./notify-job.js');
    const result = await runNotificationJob();
    res.json(result);
  } catch (e) {
    console.error('[inventory-decision] POST /summary/notify-test error:', e);
    res.status(500).json({ error: e.message });
  }
});

export default router;
