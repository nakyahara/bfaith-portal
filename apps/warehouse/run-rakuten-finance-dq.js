#!/usr/bin/env node
/**
 * run-rakuten-finance-dq.js — 楽天 Phase 1a #R-2 DQ gate / anomaly alert
 *
 * monthly validation: f_rakuten_finance_sku_daily_v1 (R-1 で構築) の品質指標を
 * 6 つの DQ check で評価し `dq_run_results` に severity 付きで記録。
 *
 * severity='error' の check が 1 件以上あれば exit code 1 で停止 (gate failure)。
 *
 * 使い方:
 *   DATA_DIR=C:\Users\bfaith\bfaith-portal\data \
 *     node apps/warehouse/run-rakuten-finance-dq.js --month 2026-04
 *   DATA_DIR=... node apps/warehouse/run-rakuten-finance-dq.js --month 2026-04 --run-id custom-id
 *
 * 6 つの DQ check (severity / threshold は config inline):
 *   1. row_count_drift               (error: rows = 0)
 *   2. listing_diff_pct              (warn: > 1%, error: > 5%)
 *   3. missing_cost_rate_pct         (warn: > 5%, error: > 10%)
 *   4. shipping_missing_rate_pct     (warn: > 5%, error: > 10%)
 *   5. unresolved_sku_rate_pct       (warn: > 5%, error: > 10%)
 *   6. date_mismatch_units           (info、Phase 1b 按分対象モニタリング、gate しない)
 *
 * Amazon との差:
 *   - v4 mart 比較なし → accounting_diff_buckets は使わない
 *   - listing 突合 (f_sales_by_listing) を「外部 reference 比較」として活用
 *   - 「日付不一致 SKU 数」を Phase 1b 按分対象モニタリングとして info 記録
 *
 * 設計参照:
 *   - 設計書: g:/共有ドライブ/AI_reference/システム設計/楽天Phase1a設計書_v0.4_20260509.md
 *   - Amazon 同等: apps/warehouse/run-amazon-finance-dq.js
 */

import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}

const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthStr = getArg('--month');
const runId = getArg('--run-id') || `dq-rakuten-${monthStr}-${new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)}`;

if (!DATA_DIR || !monthStr) {
  console.error('FATAL: DATA_DIR and --month YYYY-MM are required.');
  console.error('  Example: DATA_DIR=... node apps/warehouse/run-rakuten-finance-dq.js --month 2026-04');
  process.exit(2);
}
if (!/^\d{4}-\d{2}$/.test(monthStr)) {
  console.error('FATAL: --month must be YYYY-MM');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exit(2);
}

const checkedAt = new Date().toISOString();

// ============================================================
// Threshold config (Phase 1a #R-2 確定値)
// ============================================================
// listing_diff_pct は当月と前月以前で閾値を分ける (Issue #83 対応 2026-05-10)
// 当月: build と f_sales_by_listing の sync タイミング差で diff 8% 程度普通に出る (false positive)
// 前月以前: 月末確定後なので厳密 1%/5% で本物の品質悪化を検出
function isCurrentMonth(monthStr) {
  // JST の今日の YYYY-MM (UTC+9)
  const nowJst = new Date(Date.now() + 9 * 60 * 60 * 1000);
  const currentMonth = nowJst.toISOString().slice(0, 7);
  return monthStr === currentMonth;
}

const THRESHOLDS_PAST_MONTH = {
  row_count_drift:           { warn: 0,    error: 0 },        // 厳密一致 (0 行は error)
  listing_diff_pct:          { warn: 1.0,  error: 5.0 },      // %
  missing_cost_rate_pct:     { warn: 5.0,  error: 10.0 },     // %
  shipping_missing_rate_pct: { warn: 5.0,  error: 10.0 },     // %
  unresolved_sku_rate_pct:   { warn: 5.0,  error: 10.0 },     // %
  date_mismatch_units:       { warn: null, error: null },     // info only (Phase 1b 按分対象)
};

const THRESHOLDS_CURRENT_MONTH = {
  ...THRESHOLDS_PAST_MONTH,
  // 当月のみ listing_diff を緩和 (build と listing の sync タイミング差で false positive 多発防止)
  listing_diff_pct:          { warn: 5.0,  error: 15.0 },
};

const THRESHOLDS = isCurrentMonth(monthStr) ? THRESHOLDS_CURRENT_MONTH : THRESHOLDS_PAST_MONTH;

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

const issues = [];
let hasError = false;

function recordResult(checkName, severity, actualValue, thresholdValue, details = null) {
  db.prepare(`
    INSERT OR REPLACE INTO dq_run_results
      (run_id, check_name, severity, actual_value, threshold_value, details_json, checked_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(runId, checkName, severity, actualValue, thresholdValue, details ? JSON.stringify(details) : null, checkedAt);
  if (severity === 'error') hasError = true;
  if (severity !== 'info') {
    issues.push({ checkName, severity, actualValue, thresholdValue, details });
  }
}

const isCurMonth = isCurrentMonth(monthStr);
console.log(`=== Rakuten DQ gate run: ${runId} (month=${monthStr}, ${isCurMonth ? 'CURRENT month threshold' : 'PAST month threshold'}) ===`);
if (isCurMonth) {
  console.log(`  ℹ️  当月モード: listing_diff_pct を warn ${THRESHOLDS.listing_diff_pct.warn}%/error ${THRESHOLDS.listing_diff_pct.error}% に緩和 (Issue #83)`);
  console.log(`     理由: build と f_sales_by_listing の sync タイミング差で当月は diff 8% 程度普通に出る (false positive 防止)`);
}

// 既存 run_id の result があれば削除して再実行可
db.prepare(`DELETE FROM dq_run_results WHERE run_id = ?`).run(runId);

// ============================================================
// Check 1: row_count_drift (rows = 0 なら error)
// ============================================================
const dailyCount = db.prepare(`
  SELECT COUNT(*) AS c FROM f_rakuten_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr).c;

recordResult(
  'row_count_drift',
  dailyCount === 0 ? 'error' : 'info',
  dailyCount,
  0,
  { daily_row_count: dailyCount }
);

if (dailyCount === 0) {
  console.error(`  ⚠️  CRITICAL: f_rakuten_finance_sku_daily_v1 に ${monthStr} のデータが 0 行`);
  console.error(`  → R-1 build pipeline が動いていない可能性、原因調査必要`);
  // 続けて他 check は意味ないので exit
  printSummary();
  process.exit(1);
}

// ============================================================
// Check 2: listing_diff_pct (f_sales_by_listing 突合)
// ============================================================
const dailyTotal = db.prepare(`
  SELECT SUM(gross_sales_jpy_incl) AS p FROM f_rakuten_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr).p || 0;

let listingTotal = 0;
let listingAvailable = false;
try {
  const listingRow = db.prepare(`
    SELECT SUM(売上金額) AS p FROM f_sales_by_listing
    WHERE モール = 'rakuten' AND substr(日付, 1, 7) = ?
  `).get(monthStr);
  listingTotal = listingRow?.p || 0;
  listingAvailable = listingTotal > 0;
} catch (e) {
  console.log(`  (listing 突合スキップ: ${e.message})`);
}

if (listingAvailable) {
  const totalDiff = Math.abs(dailyTotal - listingTotal);
  const totalDiffPct = listingTotal !== 0 ? (totalDiff / Math.abs(listingTotal)) * 100 : 0;
  recordResult(
    'listing_diff_pct',
    totalDiffPct > THRESHOLDS.listing_diff_pct.error ? 'error' :
      totalDiffPct > THRESHOLDS.listing_diff_pct.warn ? 'warn' : 'info',
    totalDiffPct,
    THRESHOLDS.listing_diff_pct.error,
    { daily_total_jpy: dailyTotal, listing_total_jpy: listingTotal, diff_jpy: dailyTotal - listingTotal }
  );
} else {
  recordResult(
    'listing_diff_pct',
    'info',
    null,
    THRESHOLDS.listing_diff_pct.error,
    { skipped: true, reason: 'f_sales_by_listing data not available' }
  );
}

// ============================================================
// Check 3: missing_cost_rate_pct (cost_status='missing_cost' 比率)
// ============================================================
const costStats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN cost_status = 'missing_cost' THEN 1 ELSE 0 END) AS missing,
    SUM(CASE WHEN cost_status = 'missing_cost' THEN gross_sales_jpy_incl ELSE 0 END) AS missing_gross_sales
  FROM f_rakuten_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr);
const missingCostRate = costStats.total > 0 ? (costStats.missing / costStats.total) * 100 : 0;
recordResult(
  'missing_cost_rate_pct',
  missingCostRate > THRESHOLDS.missing_cost_rate_pct.error ? 'error' :
    missingCostRate > THRESHOLDS.missing_cost_rate_pct.warn ? 'warn' : 'info',
  missingCostRate,
  THRESHOLDS.missing_cost_rate_pct.error,
  { total_rows: costStats.total, missing_count: costStats.missing,
    missing_gross_sales_jpy: costStats.missing_gross_sales }
);

// ============================================================
// Check 4: shipping_missing_rate_pct
// ============================================================
const shipStats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN shipping_quality = 'missing' THEN 1 ELSE 0 END) AS missing
  FROM f_rakuten_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr);
const shipMissingRate = shipStats.total > 0 ? (shipStats.missing / shipStats.total) * 100 : 0;
recordResult(
  'shipping_missing_rate_pct',
  shipMissingRate > THRESHOLDS.shipping_missing_rate_pct.error ? 'error' :
    shipMissingRate > THRESHOLDS.shipping_missing_rate_pct.warn ? 'warn' : 'info',
  shipMissingRate,
  THRESHOLDS.shipping_missing_rate_pct.error,
  { total_rows: shipStats.total, missing_count: shipStats.missing }
);

// ============================================================
// Check 5: unresolved_sku_rate_pct
// ============================================================
const skuStats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN sku_resolution = 'unresolved' THEN 1 ELSE 0 END) AS unresolved
  FROM f_rakuten_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr);
const unresolvedRate = skuStats.total > 0 ? (skuStats.unresolved / skuStats.total) * 100 : 0;
recordResult(
  'unresolved_sku_rate_pct',
  unresolvedRate > THRESHOLDS.unresolved_sku_rate_pct.error ? 'error' :
    unresolvedRate > THRESHOLDS.unresolved_sku_rate_pct.warn ? 'warn' : 'info',
  unresolvedRate,
  THRESHOLDS.unresolved_sku_rate_pct.error,
  { total_rows: skuStats.total, unresolved_count: skuStats.unresolved }
);

// ============================================================
// Check 6: date_mismatch_units (Phase 1b 按分対象モニタリング、info only)
// fact_returns 注文日 と silver date_jst が日付不一致な (date, code) ペアの units
// ============================================================
const dateMismatch = db.prepare(`
  WITH silver AS (
    SELECT substr(order_date,1,10) AS date_jst, item_number AS rakuten_code
    FROM raw_rakuten_orders
    WHERE order_status IN (500,600,700) AND substr(order_date,1,7) = ?
    GROUP BY substr(order_date,1,10), item_number
  ),
  ret AS (
    SELECT 注文日 AS date_jst, モール商品コード AS rakuten_code, SUM(数量) AS units
    FROM fact_returns
    WHERE モール='rakuten' AND substr(注文日,1,7) = ?
      AND モール商品コード IN (SELECT DISTINCT rakuten_code FROM silver)
    GROUP BY 注文日, モール商品コード
  )
  SELECT
    COUNT(*) AS pair_count,
    COALESCE(SUM(ret.units), 0) AS lost_units
  FROM ret LEFT JOIN silver
    ON silver.date_jst = ret.date_jst AND silver.rakuten_code = ret.rakuten_code
  WHERE silver.rakuten_code IS NULL
`).get(monthStr, monthStr);

recordResult(
  'date_mismatch_units',
  'info',  // Phase 1b 対応予定なので gate しない
  dateMismatch.lost_units,
  null,
  { pair_count: dateMismatch.pair_count, lost_units: dateMismatch.lost_units,
    note: 'Phase 1b 按分案で回収予定 (g:/共有ドライブ/AI_reference/システム設計/楽天Phase1b案_C-lite検討メモ_20260509.md)' }
);

// ============================================================
// Summary
// ============================================================
function printSummary() {
  console.log('');
  console.log('--- DQ check summary ---');
  const allResults = db.prepare(`
    SELECT check_name, severity, actual_value, threshold_value, details_json
    FROM dq_run_results WHERE run_id = ? ORDER BY check_name
  `).all(runId);
  for (const r of allResults) {
    const icon = r.severity === 'error' ? '❌' : r.severity === 'warn' ? '⚠️' : 'ℹ️';
    const actualStr = r.actual_value !== null ? r.actual_value.toFixed(3) : 'n/a';
    const thresholdStr = r.threshold_value !== null ? r.threshold_value.toFixed(3) : 'n/a';
    console.log(`  ${icon} ${r.check_name}: ${actualStr} (threshold=${thresholdStr}, ${r.severity})`);
  }
  console.log('');
  if (hasError) {
    console.log(`❌ DQ gate FAILED (${issues.filter(i => i.severity === 'error').length} error, ${issues.filter(i => i.severity === 'warn').length} warn)`);
  } else if (issues.length > 0) {
    console.log(`⚠️  DQ gate passed with ${issues.filter(i => i.severity === 'warn').length} warning(s)`);
  } else {
    console.log(`✅ DQ gate passed (no error, no warn)`);
  }
}

printSummary();

db.close();
process.exit(hasError ? 1 : 0);
