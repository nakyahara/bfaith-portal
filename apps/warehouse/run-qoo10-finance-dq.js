#!/usr/bin/env node
/**
 * run-qoo10-finance-dq.js — Qoo10 Phase 1 A-2 DQ gate
 *
 * monthly validation: f_qoo10_finance_sku_daily_v1 (A-2 で構築) の品質指標を 16 check で評価し
 * `dq_run_results` に severity 付きで記録。severity='error' が 1 件以上あれば exit 1 (gate failure)。
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data node apps/warehouse/run-qoo10-finance-dq.js --month 2026-05
 *
 * 16 check (severity / threshold、設計書 v0.11 §8 + 2026-05-19 A-2 patch #16):
 *   1. row_count_drift                        (error: rows=0、当月 ratio<0.5 で warn)
 *   2. listing_diff_pct                       (info only、Codex R1 #4 反映で gate 解除) — f_sales_by_listing (qoo10) vs net_settlement_api 突合、構造的 diff 前提で監視のみ
 *   3. missing_cost_rate_pct                  (warn 5% / error 10%)
 *   4. shipping_missing_rate_pct              (Phase A 無効化: 'no_shipping_in_api' が常態)
 *   5. unresolved_sku_rate_pct                (warn 0.5% / error 1%、v0.7 で error 化、母集団は non-frozen 限定、v0.11 Codex R11 Medium)
 *   6. whitelist_coverage_pct                 (warn 95% / error 90%) — Delivered(5) 通常 97%
 *   7. resolved_but_zero_cost_count           (error: 1 件以上)
 *   8. settle_price_formula_match_pct         (Qoo10 特有、error: <99%) — silver 行レベル scope='domestic_non_cod' の SettlePrice = round(orderPrice × 0.9) × qty 成立比率
 *   9. shop_promo_burden_zero_check           (Qoo10 特有、warn: SellerDiscount > 0 or Cart_Discount_Seller > 0 が 1 件以上)
 *  10. promo_type_classification_health       (Qoo10 特有、info) — megawari/megapo/other_promo 月別件数比率
 *  11. horizon_frozen_observed_count          (Qoo10 特有、info、LINEギフト R5 critical 同型)
 *  12. delivered_missing_delivered_date_count (Qoo10 特有、warn: 1 件以上、Codex R1 #8 反映で error→warn) — orderDate ベース主集計なので audit
 *  13. legacy_fields_missing_active_count     (Qoo10 特有、info) — 旧 raw 由来で fact 対象外の件数監視 (移行完了度の見える化)
 *  14. gross_zero_row_count                   (Qoo10 特有、warn: 1件以上、Codex R3 Medium #4) — silver で gross = order_price × order_qty = 0 の異常行件数
 *  15. frozen_unresolved_observation          (Qoo10 特有、info、Codex R11 Low #1) — frozen 受容済 unresolved 行の可視化、vw_qoo10_finance_horizon_status 経由
 *  16. match_tier_distribution                (Qoo10 特有、info、2026-05-19 A-2 patch) — 3 段 fallback (combined/option_only/seller_only/unresolved) tier 分布、Qoo10 命名規則変化早期検知
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §8
 */
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthStr = getArg('--month');
const runId = getArg('--run-id') || `dq-qoo10-${monthStr}-${new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)}`;
if (!DATA_DIR || !monthStr) { console.error('FATAL: DATA_DIR and --month YYYY-MM are required.'); process.exit(2); }
if (!/^\d{4}-\d{2}$/.test(monthStr)) { console.error('FATAL: --month must be YYYY-MM'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const checkedAt = new Date().toISOString();
function isCurrentMonth(ym) { const nowJst = new Date(Date.now() + 9 * 3600 * 1000); return ym === nowJst.toISOString().slice(0, 7); }

const THRESHOLDS_PAST = {
  row_count_drift:                       { warn: 0,    error: 0 },
  // listing_diff_pct は info only (Codex R1 #4 反映で gate 解除、構造的 diff 前提)
  listing_diff_pct:                      { warn: 999,  error: 999 },
  missing_cost_rate_pct:                 { warn: 5.0,  error: 10.0 },
  shipping_missing_rate_pct:             { warn: 100,  error: 100 },  // Phase A 無効化
  unresolved_sku_rate_pct:               { warn: 0.5,  error: 1.0 },  // NE 在庫連携前提で原則 ~0%
  whitelist_coverage_pct:                { warn: 95.0, error: 90.0 },
  resolved_but_zero_cost_count:          { warn: 1,    error: 1 },
  settle_price_formula_match_pct:        { warn: 99.5, error: 99.0 },  // domestic_non_cod 母集団の公式成立比率
  shop_promo_burden_zero_check:          { warn: 1,    error: 999 },   // SellerDiscount > 0 件が 1 件でも warn
  promo_type_classification_health:      { warn: 999999, error: 999999 }, // info 固定
  horizon_frozen_observed_count:         { warn: 999999, error: 999999 }, // info 固定
  delivered_missing_delivered_date_count: { warn: 1,    error: 999 },  // 1 件以上 warn (Codex R1 #8 反映で error→warn)
  legacy_fields_missing_active_count:    { warn: 999999, error: 999999 }, // info 固定
  gross_zero_row_count:                  { warn: 1,    error: 999 },   // 1 件以上 warn
  frozen_unresolved_observation:         { warn: 999999, error: 999999 }, // info 固定
  match_tier_distribution:               { warn: 999999, error: 999999 }, // info 固定 (2026-05-19 A-2 patch)
};
const THRESHOLDS_CURRENT = {
  ...THRESHOLDS_PAST,
  whitelist_coverage_pct: { warn: 70.0, error: 60.0 },  // 当月 70% (Delivered 遷移ラグ考慮、LINEギフト 同型)
};
const isCur = isCurrentMonth(monthStr);
const THRESHOLDS = isCur ? THRESHOLDS_CURRENT : THRESHOLDS_PAST;

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
const issues = [];
let hasError = false;
function recordResult(name, severity, actual, threshold, details = null) {
  db.prepare(`INSERT OR REPLACE INTO dq_run_results (run_id, check_name, severity, actual_value, threshold_value, details_json, checked_at) VALUES (?,?,?,?,?,?,?)`)
    .run(runId, name, severity, actual, threshold, details ? JSON.stringify(details) : null, checkedAt);
  if (severity === 'error') hasError = true;
  if (severity !== 'info') issues.push({ name, severity, actual, threshold, details });
}

console.log(`=== Qoo10 DQ gate: ${runId} (month=${monthStr}, ${isCur ? 'CURRENT' : 'PAST'} mode) ===`);
db.prepare(`DELETE FROM dq_run_results WHERE run_id = ?`).run(runId);

// Check 1: row_count_drift
const dailyCount = db.prepare("SELECT COUNT(*) AS c FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr).c;
if (dailyCount === 0) {
  recordResult('row_count_drift', 'error', dailyCount, 0, { daily_row_count: dailyCount });
  console.error(`  ⚠️ CRITICAL: f_qoo10_finance_sku_daily_v1 に ${monthStr} のデータが 0 行`);
  printSummary();
  process.exit(1);
}
if (isCur) {
  // 当月のみ partial ingest 検知 (LINEギフト 同型)
  const recent = db.prepare(`
    WITH RECURSIVE date_spine(d) AS (
      SELECT date('now', '+9 hours', '-8 days')
      UNION ALL
      SELECT date(d, '+1 day') FROM date_spine WHERE d < date('now', '+9 hours', '-1 day')
    )
    SELECT date_spine.d AS date_jst, COALESCE(c.cnt, 0) AS cnt
    FROM date_spine
    LEFT JOIN (SELECT date_jst, COUNT(*) AS cnt FROM f_qoo10_finance_sku_daily_v1 GROUP BY date_jst) c
      ON c.date_jst = date_spine.d
    ORDER BY date_spine.d
  `).all();
  const latest = recent[recent.length - 1];
  const priorDays = recent.slice(0, -1);
  const avg7 = priorDays.reduce((sum, r) => sum + r.cnt, 0) / priorDays.length;
  const ratio = avg7 > 0 ? latest.cnt / avg7 : null;
  const allZero = recent.every(r => r.cnt === 0);
  if (allZero) {
    recordResult('row_count_drift', 'error', latest.cnt, 0, { expected_latest_date: latest.date_jst, latest_count: latest.cnt, spine_days: recent.length, note: '直近 8 日連続 row=0 (取り込み完全停止)' });
  } else if (avg7 > 0 && ratio < 0.5) {
    recordResult('row_count_drift', 'warn', latest.cnt, Math.round(avg7 * 0.5), { expected_latest_date: latest.date_jst, latest_count: latest.cnt, avg7_prior_days: avg7.toFixed(1), ratio: ratio.toFixed(3), spine_days: recent.length, note: '期待 latest (yesterday) row 数 < 7日平均×0.5 (partial ingest 疑い)' });
  } else if (avg7 === 0) {
    recordResult('row_count_drift', 'info', latest.cnt, 0, { expected_latest_date: latest.date_jst, latest_count: latest.cnt, note: '7日平均=0 だが latest>0 (新規運用)、ratio 比較スキップ' });
  } else {
    recordResult('row_count_drift', 'info', latest.cnt, Math.round(avg7 * 0.5), { expected_latest_date: latest.date_jst, latest_count: latest.cnt, avg7_prior_days: avg7.toFixed(1), ratio: ratio !== null ? ratio.toFixed(3) : 'n/a', spine_days: recent.length });
  }
} else {
  recordResult('row_count_drift', 'info', dailyCount, 0, { daily_row_count: dailyCount, note: 'PAST mode は rows>0 のみチェック' });
}

// Check 2: listing_diff_pct (info only、Codex R1 #4 反映で gate 解除)
// Codex R1 (A-2) Medium #1 反映: listingTotal===0 でもクエリ成功なら fact 側との diff を info 記録
//   (テーブル存在/クエリ成功) と (値=0) を分離、ゼロ売上月の整合性監視も維持
const factNetSettle = db.prepare("SELECT SUM(net_settlement_api_jpy_incl) AS p FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr).p || 0;
let listingTotal = null, listingQueryOk = false;
try { const r = db.prepare("SELECT SUM(売上金額) AS p FROM f_sales_by_listing WHERE モール='qoo10' AND substr(日付,1,7) = ?").get(monthStr); listingTotal = r?.p || 0; listingQueryOk = true; }
catch (e) { /* テーブル不在等、listingQueryOk=false のまま */ }
if (listingQueryOk) {
  const diffPct = listingTotal !== 0 ? Math.abs(factNetSettle - listingTotal) / Math.abs(listingTotal) * 100 : (factNetSettle !== 0 ? 100 : 0);
  // info 固定 (gate 解除、構造的 diff 前提)。listingTotal=0 でも fact diff を可視化
  recordResult('listing_diff_pct', 'info', diffPct, THRESHOLDS.listing_diff_pct.warn, { fact_net_settle_jpy: factNetSettle, listing_jpy: listingTotal, diff_jpy: factNetSettle - listingTotal, note: listingTotal === 0 ? 'listing=0 だが fact diff を記録 (ゼロ売上月監視)' : '構造的 diff 前提、gate 解除' });
} else { recordResult('listing_diff_pct', 'info', null, THRESHOLDS.listing_diff_pct.warn, { skipped: true, note: 'f_sales_by_listing テーブル不在' }); }

// Check 3: missing_cost_rate_pct
const cs = db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN cost_status='missing_cost' THEN 1 ELSE 0 END) AS missing FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const missCostRate = cs.total > 0 ? cs.missing / cs.total * 100 : 0;
recordResult('missing_cost_rate_pct',
  missCostRate >= THRESHOLDS.missing_cost_rate_pct.error ? 'error' : missCostRate >= THRESHOLDS.missing_cost_rate_pct.warn ? 'warn' : 'info',
  missCostRate, THRESHOLDS.missing_cost_rate_pct.error, { total_rows: cs.total, missing_count: cs.missing });

// Check 4: shipping_missing_rate_pct (Phase A 無効化)
const ss = db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN shipping_quality='missing' THEN 1 ELSE 0 END) AS missing FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const shipMissRate = ss.total > 0 ? ss.missing / ss.total * 100 : 0;
recordResult('shipping_missing_rate_pct', 'info', shipMissRate, THRESHOLDS.shipping_missing_rate_pct.error,
  { total_rows: ss.total, missing_count: ss.missing, note: 'Phase A 無効化 (no_shipping_in_api が常態)' });

// Check 5: unresolved_sku_rate_pct (母集団 = non-frozen、Codex R11 Medium 反映)
const us = db.prepare(`
  SELECT
    SUM(CASE WHEN is_frozen_after_horizon = 0 THEN 1 ELSE 0 END) AS total_non_frozen,
    SUM(CASE WHEN is_frozen_after_horizon = 0 AND unresolved_sku_flag = 1 THEN 1 ELSE 0 END) AS unresolved_non_frozen
  FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
`).get(monthStr);
const unresRate = us.total_non_frozen > 0 ? us.unresolved_non_frozen / us.total_non_frozen * 100 : 0;
recordResult('unresolved_sku_rate_pct',
  unresRate >= THRESHOLDS.unresolved_sku_rate_pct.error ? 'error' : unresRate >= THRESHOLDS.unresolved_sku_rate_pct.warn ? 'warn' : 'info',
  unresRate, THRESHOLDS.unresolved_sku_rate_pct.error, { total_non_frozen: us.total_non_frozen, unresolved_non_frozen: us.unresolved_non_frozen, note: 'frozen 受容済は #15 で別監視' });

// Check 6: whitelist_coverage_pct (Delivered(5) / 全 status、新 raw のみ)
const cov = db.prepare(`SELECT
  (SELECT COUNT(*) FROM raw_qoo10_orders WHERE substr(order_date,1,7)=? AND legacy_fields_missing=0) AS total,
  (SELECT COUNT(*) FROM raw_qoo10_orders WHERE substr(order_date,1,7)=? AND legacy_fields_missing=0 AND shipping_status='Delivered(5)') AS wl`).get(monthStr, monthStr);
const wlPct = cov.total > 0 ? cov.wl / cov.total * 100 : 0;
recordResult('whitelist_coverage_pct',
  wlPct <= THRESHOLDS.whitelist_coverage_pct.error ? 'error' : wlPct <= THRESHOLDS.whitelist_coverage_pct.warn ? 'warn' : 'info',
  wlPct, THRESHOLDS.whitelist_coverage_pct.warn, { total_lines: cov.total, whitelist_lines: cov.wl });

// Check 7: resolved_but_zero_cost_count
const zc = db.prepare("SELECT COUNT(*) AS cnt FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7)=? AND ne_code IS NOT NULL AND units_net_sold > 0 AND (cogs_amount_jpy_incl = 0 OR unit_cost_snapshot_incl IS NULL)").get(monthStr);
recordResult('resolved_but_zero_cost_count',
  zc.cnt >= THRESHOLDS.resolved_but_zero_cost_count.error ? 'error' : 'info',
  zc.cnt, THRESHOLDS.resolved_but_zero_cost_count.error, { count: zc.cnt });

// Check 8: settle_price_formula_match_pct (Qoo10 特有、Codex R3 High #1 反映で fact 集約後の domestic_non_cod_line_count/match_count を使う)
// 母集団 = 各行の domestic_non_cod_line_count 合計 (scope='mixed' fact 行に含まれる domestic 部分も漏れ防止)
const sm = db.prepare(`
  SELECT
    SUM(domestic_non_cod_line_count) AS total_dnoncod_lines,
    SUM(domestic_non_cod_formula_match_count) AS match_dnoncod_lines
  FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
`).get(monthStr);
const formulaMatchPct = sm.total_dnoncod_lines > 0 ? sm.match_dnoncod_lines / sm.total_dnoncod_lines * 100 : 100;
recordResult('settle_price_formula_match_pct',
  formulaMatchPct <= THRESHOLDS.settle_price_formula_match_pct.error ? 'error' : formulaMatchPct <= THRESHOLDS.settle_price_formula_match_pct.warn ? 'warn' : 'info',
  formulaMatchPct, THRESHOLDS.settle_price_formula_match_pct.error, { total_domestic_non_cod_lines: sm.total_dnoncod_lines, match_count: sm.match_dnoncod_lines, note: 'SettlePrice = round(orderPrice × 0.9) × qty 成立比率、 99% 未満で API 仕様変化警告' });

// Check 9: shop_promo_burden_zero_check (Qoo10 特有、SellerDiscount > 0 or Cart_Discount_Seller > 0 検知)
const sp = db.prepare(`
  SELECT
    SUM(CASE WHEN seller_discount_api_jpy_incl > 0 THEN 1 ELSE 0 END) AS positive_seller_count,
    ROUND(SUM(seller_discount_api_jpy_incl), 0) AS total_seller_discount
  FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
`).get(monthStr);
recordResult('shop_promo_burden_zero_check',
  sp.positive_seller_count >= THRESHOLDS.shop_promo_burden_zero_check.warn ? 'warn' : 'info',
  sp.positive_seller_count, THRESHOLDS.shop_promo_burden_zero_check.warn, { positive_seller_count: sp.positive_seller_count, total_seller_discount_jpy: sp.total_seller_discount, note: 'SellerDiscount/Cart_Discount_Seller > 0 検知 (現状常時 0、発火したら Phase B-2 で対応要)' });

// Check 10: promo_type_classification_health (info、月別 promo 件数比率)
const pc = db.prepare(`
  SELECT
    SUM(megawari_order_count) AS megawari_cnt,
    SUM(megapo_order_count) AS megapo_cnt,
    SUM(other_promo_order_count) AS other_cnt,
    SUM(order_count) AS total_orders
  FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
`).get(monthStr);
const megawariPct = pc.total_orders > 0 ? pc.megawari_cnt / pc.total_orders * 100 : 0;
recordResult('promo_type_classification_health', 'info', megawariPct, 100, { megawari_count: pc.megawari_cnt, megapo_count: pc.megapo_cnt, other_promo_count: pc.other_cnt, total_orders: pc.total_orders, note: 'megawari/megapo 月別件数比率、年4回 megawari 期間の自動検知' });

// Check 11: horizon_frozen_observed_count (info、LINEギフト R5 同型)
const hfc = db.prepare(`
  SELECT COUNT(*) AS cnt FROM raw_qoo10_orders
  WHERE is_frozen_after_horizon = 1
    AND legacy_fields_missing = 0
    AND substr(order_date, 1, 7) = ?
`).get(monthStr);
recordResult('horizon_frozen_observed_count', 'info',
  hfc.cnt, THRESHOLDS.horizon_frozen_observed_count.error, { count: hfc.cnt, note: 'monthStr 月の frozen 行数 (info 監視指標)' });

// Check 12: delivered_missing_delivered_date_count (warn 1件以上、Codex R1 #8 反映で error→warn)
const dmc = db.prepare(`
  SELECT COUNT(*) AS cnt FROM raw_qoo10_orders
  WHERE shipping_status = 'Delivered(5)'
    AND legacy_fields_missing = 0
    AND substr(order_date, 1, 7) = ?
    AND (delivered_date IS NULL OR delivered_date = '')
`).get(monthStr);
recordResult('delivered_missing_delivered_date_count',
  dmc.cnt >= THRESHOLDS.delivered_missing_delivered_date_count.warn ? 'warn' : 'info',
  dmc.cnt, THRESHOLDS.delivered_missing_delivered_date_count.warn, { count: dmc.cnt, note: 'Delivered(5) なのに delivered_date 未設定の audit 件数 (主集計は orderDate 基準なので warn)' });

// Check 13: legacy_fields_missing_active_count (info、移行完了度の見える化)
const lc = db.prepare(`
  SELECT COUNT(*) AS cnt FROM raw_qoo10_orders
  WHERE legacy_fields_missing = 1
    AND substr(order_date, 1, 7) = ?
`).get(monthStr);
recordResult('legacy_fields_missing_active_count', 'info',
  lc.cnt, THRESHOLDS.legacy_fields_missing_active_count.warn, { count: lc.cnt, note: '旧 raw 由来 (fact 対象外) の月内件数、新規 ingest 後は時間経過で 0 に近づく' });

// Check 14: gross_zero_row_count (warn 1件以上、Codex R3 Medium #4)
// silver で gross = order_price × order_qty = 0 の異常行件数 (raw 直接検索)
// Codex R1 (A-2) High #1 反映: 件数集計とサンプルID取得を分離 (LIMIT 内 COUNT(*) は最大10で頭打ちのバグ)
const grcCount = db.prepare(`
  SELECT COUNT(*) AS cnt
  FROM raw_qoo10_orders
  WHERE shipping_status = 'Delivered(5)'
    AND legacy_fields_missing = 0
    AND substr(order_date, 1, 7) = ?
    AND (CAST(order_price AS REAL) * CAST(order_qty AS INTEGER)) = 0
`).get(monthStr);
const grcSamples = db.prepare(`
  SELECT GROUP_CONCAT(order_id) AS order_ids FROM (
    SELECT order_id
    FROM raw_qoo10_orders
    WHERE shipping_status = 'Delivered(5)'
      AND legacy_fields_missing = 0
      AND substr(order_date, 1, 7) = ?
      AND (CAST(order_price AS REAL) * CAST(order_qty AS INTEGER)) = 0
    LIMIT 10
  )
`).get(monthStr);
recordResult('gross_zero_row_count',
  grcCount.cnt >= THRESHOLDS.gross_zero_row_count.warn ? 'warn' : 'info',
  grcCount.cnt, THRESHOLDS.gross_zero_row_count.warn, { count: grcCount.cnt, top10_order_ids: grcSamples.order_ids, note: 'gross = order_price × order_qty = 0 = 無償同梱/補償行/異常データ、other_platform_promo に逃がし済' });

// Check 15: frozen_unresolved_observation (info、Codex R11 Low #1)
const fuo = db.prepare(`
  SELECT
    frozen_unresolved_row_count,
    frozen_unresolved_gmv_jpy,
    oldest_frozen_date,
    frozen_resolved_row_count
  FROM vw_qoo10_finance_horizon_status
  WHERE month_ym = ?
`).get(monthStr);
recordResult('frozen_unresolved_observation', 'info',
  fuo?.frozen_unresolved_row_count || 0,
  THRESHOLDS.frozen_unresolved_observation.warn,
  { frozen_unresolved_row_count: fuo?.frozen_unresolved_row_count || 0, frozen_unresolved_gmv_jpy: fuo?.frozen_unresolved_gmv_jpy || 0, oldest_frozen_date: fuo?.oldest_frozen_date, frozen_resolved_row_count: fuo?.frozen_resolved_row_count || 0, note: 'frozen 受容済 unresolved 監視、Phase 2 移植判断指標' });

// Check 16: match_tier_distribution (2026-05-19 A-2 patch、3 段 fallback 監視、info)
// combined / option_only / seller_only / unresolved の月別件数比率を監視、
// option_only や seller_only が急増したら Qoo10 命名規則の変化を示唆 = API 仕様揺れ早期検知
// Codex R1 patch Low #1 反映: match_tier 列が未導入の DB (旧 schema) では skip して info 記録 (no such column 防止)
const factCols = db.prepare("PRAGMA table_info(f_qoo10_finance_sku_daily_v1)").all().map(c => c.name);
if (factCols.includes('match_tier')) {
  const mt = db.prepare(`
    SELECT
      SUM(CASE WHEN match_tier='combined' THEN 1 ELSE 0 END) AS combined_cnt,
      SUM(CASE WHEN match_tier='option_only' THEN 1 ELSE 0 END) AS option_only_cnt,
      SUM(CASE WHEN match_tier='seller_only' THEN 1 ELSE 0 END) AS seller_only_cnt,
      SUM(CASE WHEN match_tier='unresolved' THEN 1 ELSE 0 END) AS unresolved_cnt,
      COUNT(*) AS total
    FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
  `).get(monthStr);
  const combinedPct = mt.total > 0 ? mt.combined_cnt / mt.total * 100 : 0;
  recordResult('match_tier_distribution', 'info', combinedPct, 100, {
    combined_count: mt.combined_cnt,
    option_only_count: mt.option_only_cnt,
    seller_only_count: mt.seller_only_cnt,
    unresolved_count: mt.unresolved_cnt,
    total_rows: mt.total,
    combined_pct: combinedPct.toFixed(2),
    note: '3 段 fallback の tier 分布、option_only/seller_only 急増で Qoo10 命名規則変化警告 (将来 Qoo10 API 仕様揺れ早期検知)'
  });
} else {
  recordResult('match_tier_distribution', 'info', null, 100, { skipped: true, note: 'match_tier 列未導入 DB (旧 schema)、build 実行で ALTER 適用後に有効化' });
}

function printSummary() {
  console.log('\n--- DQ check summary ---');
  for (const r of db.prepare("SELECT check_name, severity, actual_value, threshold_value FROM dq_run_results WHERE run_id=? ORDER BY check_name").all(runId)) {
    const icon = r.severity === 'error' ? '❌' : r.severity === 'warn' ? '⚠️' : 'ℹ️';
    console.log(`  ${icon} ${r.check_name}: ${r.actual_value !== null ? r.actual_value.toFixed(3) : 'n/a'} (threshold=${r.threshold_value !== null ? r.threshold_value.toFixed(3) : 'n/a'}, ${r.severity})`);
  }
  console.log('');
  if (hasError) console.log(`❌ DQ gate FAILED (${issues.filter(i=>i.severity==='error').length} error, ${issues.filter(i=>i.severity==='warn').length} warn)`);
  else if (issues.length > 0) console.log(`⚠️  DQ gate passed with ${issues.filter(i=>i.severity==='warn').length} warning(s)`);
  else console.log(`✅ DQ gate passed (no error, no warn)`);
}
printSummary();
db.close();
process.exit(hasError ? 1 : 0);
