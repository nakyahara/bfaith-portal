#!/usr/bin/env node
/**
 * run-yahoo-finance-dq.js — Yahoo Phase 1 Y-2 DQ gate
 *
 * monthly validation: f_yahoo_finance_sku_daily_v1 (Y-1 で構築) の品質指標を
 * 6 つの DQ check で評価し `dq_run_results` に severity 付きで記録。
 *
 * severity='error' の check が 1 件以上あれば exit code 1 で停止 (gate failure)。
 *
 * 使い方:
 *   DATA_DIR=C:\Users\bfaith\bfaith-portal\data \
 *     node apps/warehouse/run-yahoo-finance-dq.js --month 2026-04
 *
 * 6 つの DQ check (severity / threshold):
 *   1. row_count_drift             (error: rows = 0)
 *   2. listing_diff_pct            (warn 1% / error 5%、当月は warn 5% / error 15%)
 *   3. missing_cost_rate_pct       (warn 5% / error 10%)
 *   4. shipping_missing_rate_pct   (warn 5% / error 10%)
 *   5. unresolved_sku_rate_pct     (warn 35% / error 50%、Codex R2 確定)
 *   6. whitelist_coverage_pct      (warn < 95% / error < 90%、Codex R2 確定)
 *
 * 当月閾値緩和 (Issue #83 楽天と同方針 + Phase 1.2 Yahoo 拡張):
 *   - 当月: listing_diff warn 5% / error 15%
 *   - 当月: whitelist_coverage warn 80% / error 70%
 *     (5/10-11 等の最新日が status 未進行な間 false positive 防止)
 *   - 前月以前: listing_diff warn 1% / error 5%、whitelist_coverage warn 95% / error 90%
 *
 * 設計参照:
 *   - 設計書 v0.4: g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1a設計書_v0.4_20260510.md
 *   - 楽天同等: apps/warehouse/run-rakuten-finance-dq.js (PR #77 + #86)
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
const runId = getArg('--run-id') || `dq-yahoo-${monthStr}-${new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)}`;

if (!DATA_DIR || !monthStr) {
  console.error('FATAL: DATA_DIR and --month YYYY-MM are required.');
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

// 当月判定 (PR #86 楽天と同方針)
function isCurrentMonth(monthStr) {
  const nowJst = new Date(Date.now() + 9 * 60 * 60 * 1000);
  return monthStr === nowJst.toISOString().slice(0, 7);
}

const THRESHOLDS_PAST_MONTH = {
  row_count_drift:           { warn: 0,    error: 0 },
  listing_diff_pct:          { warn: 1.0,  error: 5.0 },
  missing_cost_rate_pct:     { warn: 5.0,  error: 10.0 },
  shipping_missing_rate_pct: { warn: 5.0,  error: 10.0 },
  unresolved_sku_rate_pct:   { warn: 35.0, error: 50.0 },  // Codex R2 確定 (Yahoo は楽天より高め許容)
  whitelist_coverage_pct:    { warn: 95.0, error: 90.0 },  // 下限、Codex R2 確定
};

const THRESHOLDS_CURRENT_MONTH = {
  ...THRESHOLDS_PAST_MONTH,
  listing_diff_pct: { warn: 5.0, error: 15.0 },
  // 当月は最新 1-2 日の注文が status=5/pay=1/ship=3 (発送完了) に到達してないため
  // whitelist 比率が自然に低くなる (5/10-11 で 39%/0%、月初は数日分の未発送が累積)
  // 月締め後は 98%+ に戻る (4月実績 98.77%)
  whitelist_coverage_pct: { warn: 80.0, error: 70.0 },
};

const isCurMonth = isCurrentMonth(monthStr);
const THRESHOLDS = isCurMonth ? THRESHOLDS_CURRENT_MONTH : THRESHOLDS_PAST_MONTH;

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

console.log(`=== Yahoo DQ gate: ${runId} (month=${monthStr}, ${isCurMonth ? 'CURRENT' : 'PAST'} mode) ===`);
if (isCurMonth) {
  console.log(`  ℹ️  当月モード: listing_diff_pct warn ${THRESHOLDS.listing_diff_pct.warn}%/error ${THRESHOLDS.listing_diff_pct.error}% に緩和`);
}

db.prepare(`DELETE FROM dq_run_results WHERE run_id = ?`).run(runId);

// ============================================================
// Check 1: row_count_drift
// ============================================================
const dailyCount = db.prepare(`
  SELECT COUNT(*) AS c FROM f_yahoo_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr).c;

recordResult(
  'row_count_drift',
  dailyCount === 0 ? 'error' : 'info',
  dailyCount, 0,
  { daily_row_count: dailyCount }
);

if (dailyCount === 0) {
  console.error(`  ⚠️  CRITICAL: f_yahoo_finance_sku_daily_v1 に ${monthStr} のデータが 0 行`);
  printSummary();
  process.exit(1);
}

// ============================================================
// Check 2: listing_diff_pct (estimated vs listing 突合)
// ============================================================
const dailyTotal = db.prepare(`
  SELECT SUM(listing_sales_estimated_jpy_incl) AS p FROM f_yahoo_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr).p || 0;

let listingTotal = 0, listingAvailable = false;
try {
  const listingRow = db.prepare(`
    SELECT SUM(売上金額) AS p FROM f_sales_by_listing
    WHERE モール = 'yahoo' AND substr(日付, 1, 7) = ?
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
    totalDiffPct, THRESHOLDS.listing_diff_pct.error,
    { daily_listing_estimated_jpy: dailyTotal, listing_total_jpy: listingTotal, diff_jpy: dailyTotal - listingTotal }
  );
} else {
  recordResult('listing_diff_pct', 'info', null, THRESHOLDS.listing_diff_pct.error, { skipped: true });
}

// ============================================================
// Check 3: missing_cost_rate_pct
// ============================================================
const costStats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN cost_status = 'missing_cost' THEN 1 ELSE 0 END) AS missing
  FROM f_yahoo_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr);
const missingCostRate = costStats.total > 0 ? (costStats.missing / costStats.total) * 100 : 0;
recordResult(
  'missing_cost_rate_pct',
  missingCostRate > THRESHOLDS.missing_cost_rate_pct.error ? 'error' :
    missingCostRate > THRESHOLDS.missing_cost_rate_pct.warn ? 'warn' : 'info',
  missingCostRate, THRESHOLDS.missing_cost_rate_pct.error,
  { total_rows: costStats.total, missing_count: costStats.missing }
);

// ============================================================
// Check 4: shipping_missing_rate_pct
// ============================================================
const shipStats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN shipping_quality = 'missing' THEN 1 ELSE 0 END) AS missing
  FROM f_yahoo_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr);
const shipMissingRate = shipStats.total > 0 ? (shipStats.missing / shipStats.total) * 100 : 0;
recordResult(
  'shipping_missing_rate_pct',
  shipMissingRate > THRESHOLDS.shipping_missing_rate_pct.error ? 'error' :
    shipMissingRate > THRESHOLDS.shipping_missing_rate_pct.warn ? 'warn' : 'info',
  shipMissingRate, THRESHOLDS.shipping_missing_rate_pct.error,
  { total_rows: shipStats.total, missing_count: shipStats.missing }
);

// ============================================================
// Check 5: unresolved_sku_rate_pct
// Phase 1.1 fix 後: case-insensitive 解決で unresolved 6.55% → ほぼ 0% 想定。
//   閾値はそのまま 35%/50% (将来的な variant 増 / SKU 命名乱れの早期警告用)
// ============================================================
const skuStats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(unresolved_sku_flag) AS unresolved,
    SUM(CASE WHEN resolution_method = 'sub_match' THEN 1 ELSE 0 END) AS sub_match,
    SUM(CASE WHEN resolution_method = 'parent_match' THEN 1 ELSE 0 END) AS parent_match
  FROM f_yahoo_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr);
const unresolvedRate = skuStats.total > 0 ? (skuStats.unresolved / skuStats.total) * 100 : 0;
recordResult(
  'unresolved_sku_rate_pct',
  unresolvedRate > THRESHOLDS.unresolved_sku_rate_pct.error ? 'error' :
    unresolvedRate > THRESHOLDS.unresolved_sku_rate_pct.warn ? 'warn' : 'info',
  unresolvedRate, THRESHOLDS.unresolved_sku_rate_pct.error,
  { total_rows: skuStats.total, unresolved: skuStats.unresolved, sub_match: skuStats.sub_match, parent_match: skuStats.parent_match }
);

// ============================================================
// Check 6: whitelist_coverage_pct (Yahoo 特有、Codex R2 確定閾値)
// ============================================================
const whitelistCheck = db.prepare(`
  WITH all_orders AS (
    SELECT COUNT(*) AS total FROM raw_yahoo_orders WHERE substr(order_time, 1, 7) = ?
  ),
  whitelist AS (
    SELECT COUNT(*) AS wl FROM raw_yahoo_orders
    WHERE substr(order_time, 1, 7) = ?
      AND order_status = '5' AND pay_status = '1' AND ship_status = '3'
  )
  SELECT total, wl, ROUND(wl * 100.0 / total, 2) AS pct
  FROM all_orders, whitelist
`).get(monthStr, monthStr);

const wlPct = whitelistCheck.pct || 0;
// 下限閾値: warn < 95%、error < 90% (低い方が NG なので比較反転)
recordResult(
  'whitelist_coverage_pct',
  wlPct < THRESHOLDS.whitelist_coverage_pct.error ? 'error' :
    wlPct < THRESHOLDS.whitelist_coverage_pct.warn ? 'warn' : 'info',
  wlPct, THRESHOLDS.whitelist_coverage_pct.warn,
  { total_lines: whitelistCheck.total, whitelist_lines: whitelistCheck.wl }
);

// ============================================================
// Summary
// ============================================================
function printSummary() {
  console.log('');
  console.log('--- DQ check summary ---');
  const allResults = db.prepare(`
    SELECT check_name, severity, actual_value, threshold_value
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
