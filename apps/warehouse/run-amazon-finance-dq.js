#!/usr/bin/env node
/**
 * run-amazon-finance-dq.js — Phase 1 #1-8a DQ gate / anomaly alert
 *
 * monthly validation: f_amazon_finance_sku_daily_v1 の日次 rollup と
 * v_amazon_sku_profit_actual_v4.gross_margin_with_reimbursement_excl_tax を
 * SKU x month で比較し、差分を 7 bucket に分類して `accounting_diff_buckets` に保存。
 * 6 つの DQ check を実行して `dq_run_results` に severity 付きで記録。
 *
 * severity='error' の check が 1 件以上あれば exit code 1 で停止 (gate failure)。
 *
 * 使い方:
 *   DATA_DIR=C:\Users\bfaith\bfaith-portal\data \
 *     node apps/warehouse/run-amazon-finance-dq.js --month 2026-04
 *   DATA_DIR=... node apps/warehouse/run-amazon-finance-dq.js --month 2026-04 --run-id custom-id
 *
 * 6 つの DQ check (severity / threshold は config inline):
 *   1. row_count_drift          (error: > 0)
 *   2. monthly_total_diff_pct   (warn: > 0.1%, error: > 0.5%)
 *   3. unbucketed_diff_jpy      (warn: > 500, error: > 5000)
 *   4. missing_cost_rate_pct    (warn: > 5%, error: > 10%)
 *   5. sku_null_count           (error: > 0)
 *   6. cost_late_binding_pct    (warn: > 1%, error: > 2%)
 *
 * 7 bucket (accounting_diff_buckets):
 *   - sku_null              : daily fact で seller_sku が NULL/empty
 *   - cost_late_binding     : cost_status='missing_cost' の影響額
 *   - legacy_only           : v4 にあるが daily fact に無い SKU/月
 *   - daily_only            : daily fact にあるが v4 に無い SKU/月
 *   - refund_effective_date : refund/adjustment の月跨ぎ (Phase 1.x で実装)
 *   - adjustment_diff       : v4 と daily の単純差 (説明済 bucket)
 *   - unbucketed            : 上記いずれでも説明できない残差 (alert 対象)
 *
 * GChat 通知:
 *   - GCHAT_WEBHOOK_INSIGHT 環境変数があれば、severity='error' breach で通知
 *   - 無ければ stdout に warning のみ
 */

import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}

const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthStr = getArg('--month');
const runId = getArg('--run-id') || `dq-${monthStr}-${new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)}`;

if (!DATA_DIR || !monthStr) {
  console.error('FATAL: DATA_DIR and --month YYYY-MM are required.');
  console.error('  Example: DATA_DIR=... node apps/warehouse/run-amazon-finance-dq.js --month 2026-04');
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

const yearMonthInt = parseInt(monthStr.replace('-', ''), 10);
const checkedAt = new Date().toISOString();

// ============================================================
// Threshold config (Phase 1 確定値、Codex Round 5/8 推奨)
// ============================================================
const THRESHOLDS = {
  row_count_drift:        { warn: 0,    error: 0 },        // 厳密一致
  monthly_total_diff_pct: { warn: 0.1,  error: 0.5 },      // %
  unbucketed_diff_jpy:    { warn: 500,  error: 5000 },     // 円
  missing_cost_rate_pct:  { warn: 5.0,  error: 10.0 },     // %
  sku_null_count:         { warn: 0,    error: 0 },        // 0 許容
  cost_late_binding_pct:  { warn: 1.0,  error: 2.0 },      // %
};

// ============================================================
// 7 bucket 定義
// ============================================================
const BUCKETS = {
  sku_null:              'daily fact で seller_sku が NULL/empty',
  cost_late_binding:     "cost_status='missing_cost' の影響額",
  legacy_only:           'v4 にあるが daily fact に無い SKU',
  daily_only:            'daily fact にあるが v4 に無い SKU',
  refund_effective_date: 'refund/adjustment の月跨ぎ (Phase 1.x で実装)',
  adjustment_diff:       'v4 と daily の単純差 (説明済)',
  unbucketed:            '上記いずれでも説明できない残差',
};

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

function recordBucket(monthJst, sellerSku, bucketCode, bucketAmount, rowCount, details = null) {
  db.prepare(`
    INSERT INTO accounting_diff_buckets
      (run_id, month_jst, seller_sku, asin_norm, bucket_code, bucket_amount, row_count, details_json, created_at)
    VALUES (?, ?, ?, '', ?, ?, ?, ?, ?)
  `).run(runId, monthJst, sellerSku, bucketCode, bucketAmount, rowCount, details ? JSON.stringify(details) : null, checkedAt);
}

console.log(`=== DQ gate run: ${runId} (month=${monthStr}) ===`);

// 既存 run_id の result があれば削除して再実行可
db.prepare(`DELETE FROM dq_run_results WHERE run_id = ?`).run(runId);
db.prepare(`DELETE FROM accounting_diff_buckets WHERE run_id = ?`).run(runId);

// ============================================================
// Check 1: row_count_drift
// ============================================================
const dailyCount = db.prepare(`
  SELECT COUNT(DISTINCT seller_sku) AS c FROM f_amazon_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr).c;
const v4Count = db.prepare(`
  SELECT COUNT(DISTINCT seller_sku) AS c FROM v_amazon_sku_profit_actual_v4
  WHERE year_month_int = ?
`).get(yearMonthInt).c;
const rowDrift = Math.abs(dailyCount - v4Count);
recordResult(
  'row_count_drift',
  rowDrift > THRESHOLDS.row_count_drift.error ? 'error' : (rowDrift > THRESHOLDS.row_count_drift.warn ? 'warn' : 'info'),
  rowDrift,
  THRESHOLDS.row_count_drift.error,
  { daily_sku_count: dailyCount, v4_sku_count: v4Count }
);

// ============================================================
// Check 2: monthly_total_diff_pct (gross_margin_with_reimbursement)
// ============================================================
const dailyTotal = db.prepare(`
  SELECT SUM(profit_amount) AS p FROM f_amazon_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr).p || 0;
const v4Total = db.prepare(`
  SELECT
    SUM(gross_margin_excl_tax)
    + SUM(warehouse_damage_jpy) + SUM(warehouse_lost_jpy) + SUM(safe_t_jpy)
    + SUM(refund_principal_jpy) + SUM(reversal_reimbursement_jpy) AS p
  FROM v_amazon_sku_profit_actual_v4
  WHERE year_month_int = ?
`).get(yearMonthInt).p || 0;
const totalDiff = Math.abs(dailyTotal - v4Total);
const totalDiffPct = v4Total !== 0 ? (totalDiff / Math.abs(v4Total)) * 100 : 0;
recordResult(
  'monthly_total_diff_pct',
  totalDiffPct > THRESHOLDS.monthly_total_diff_pct.error ? 'error' :
    totalDiffPct > THRESHOLDS.monthly_total_diff_pct.warn ? 'warn' : 'info',
  totalDiffPct,
  THRESHOLDS.monthly_total_diff_pct.error,
  { daily_total_jpy: dailyTotal, v4_total_jpy: v4Total, diff_jpy: dailyTotal - v4Total }
);

// ============================================================
// Check 3-7: bucket 分類 + sku_null / missing_cost / cost_late_binding
// ============================================================

// SKU null (daily fact で seller_sku が NULL/empty)
const skuNull = db.prepare(`
  SELECT COUNT(*) AS c FROM f_amazon_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ? AND (seller_sku IS NULL OR TRIM(seller_sku) = '')
`).get(monthStr).c;
recordResult(
  'sku_null_count',
  skuNull > THRESHOLDS.sku_null_count.error ? 'error' : 'info',
  skuNull,
  THRESHOLDS.sku_null_count.error,
  { count: skuNull }
);

if (skuNull > 0) {
  recordBucket(monthStr, '__null__', 'sku_null', 0, skuNull, { count: skuNull });
}

// missing_cost_rate
const costStats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN cost_status = 'missing_cost' THEN 1 ELSE 0 END) AS missing,
    SUM(CASE WHEN cost_status = 'late_bound_after_close' THEN 1 ELSE 0 END) AS late_bound,
    SUM(CASE WHEN cost_status = 'missing_cost' THEN profit_amount ELSE 0 END) AS missing_amount
  FROM f_amazon_finance_sku_daily_v1
  WHERE substr(date_jst, 1, 7) = ?
`).get(monthStr);
const missingRate = costStats.total > 0 ? (costStats.missing / costStats.total) * 100 : 0;
recordResult(
  'missing_cost_rate_pct',
  missingRate > THRESHOLDS.missing_cost_rate_pct.error ? 'error' :
    missingRate > THRESHOLDS.missing_cost_rate_pct.warn ? 'warn' : 'info',
  missingRate,
  THRESHOLDS.missing_cost_rate_pct.error,
  { total_rows: costStats.total, missing_count: costStats.missing }
);

if (costStats.missing > 0) {
  recordBucket(monthStr, '__multi__', 'cost_late_binding',
    costStats.missing_amount || 0, costStats.missing,
    { missing_count: costStats.missing, late_bound_count: costStats.late_bound });
}

// cost_late_binding_pct
const lateBindingPct = costStats.total > 0 ? (costStats.late_bound / costStats.total) * 100 : 0;
recordResult(
  'cost_late_binding_pct',
  lateBindingPct > THRESHOLDS.cost_late_binding_pct.error ? 'error' :
    lateBindingPct > THRESHOLDS.cost_late_binding_pct.warn ? 'warn' : 'info',
  lateBindingPct,
  THRESHOLDS.cost_late_binding_pct.error,
  { late_bound_count: costStats.late_bound, total: costStats.total }
);

// legacy_only / daily_only (SKU レベルの集合差)
const legacyOnly = db.prepare(`
  WITH daily AS (
    SELECT DISTINCT seller_sku FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
  ),
  legacy AS (
    SELECT DISTINCT seller_sku FROM v_amazon_sku_profit_actual_v4 WHERE year_month_int = ?
  )
  SELECT l.seller_sku FROM legacy l LEFT JOIN daily d ON d.seller_sku = l.seller_sku
  WHERE d.seller_sku IS NULL
`).all(monthStr, yearMonthInt);

const dailyOnly = db.prepare(`
  WITH daily AS (
    SELECT DISTINCT seller_sku FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
  ),
  legacy AS (
    SELECT DISTINCT seller_sku FROM v_amazon_sku_profit_actual_v4 WHERE year_month_int = ?
  )
  SELECT d.seller_sku FROM daily d LEFT JOIN legacy l ON l.seller_sku = d.seller_sku
  WHERE l.seller_sku IS NULL
`).all(monthStr, yearMonthInt);

if (legacyOnly.length > 0) {
  recordBucket(monthStr, '__multi__', 'legacy_only', 0, legacyOnly.length, {
    sample_skus: legacyOnly.slice(0, 5).map(r => r.seller_sku)
  });
}
if (dailyOnly.length > 0) {
  recordBucket(monthStr, '__multi__', 'daily_only', 0, dailyOnly.length, {
    sample_skus: dailyOnly.slice(0, 5).map(r => r.seller_sku)
  });
}

// adjustment_diff (説明可能な差)
recordBucket(monthStr, '__multi__', 'adjustment_diff',
  totalDiff - skuNull * 0 - (costStats.missing_amount || 0),  // 簡易計算
  1, { method: 'total_diff - sku_null_amount - cost_late_binding_amount' });

// unbucketed (説明できない残差)
const explainableSum = (costStats.missing_amount || 0);
const unbucketed = totalDiff - explainableSum;
recordBucket(monthStr, '__multi__', 'unbucketed', unbucketed, 1, {
  total_diff: totalDiff,
  explainable_sum: explainableSum,
  formula: 'total_diff - sum(known buckets)'
});

recordResult(
  'unbucketed_diff_jpy',
  unbucketed > THRESHOLDS.unbucketed_diff_jpy.error ? 'error' :
    unbucketed > THRESHOLDS.unbucketed_diff_jpy.warn ? 'warn' : 'info',
  unbucketed,
  THRESHOLDS.unbucketed_diff_jpy.error,
  { total_diff_jpy: totalDiff, explainable_sum_jpy: explainableSum }
);

// ============================================================
// 結果出力
// ============================================================
console.log('\n--- DQ check results ---');
const allResults = db.prepare(`
  SELECT check_name, severity, actual_value, threshold_value
  FROM dq_run_results WHERE run_id = ? ORDER BY severity DESC, check_name
`).all(runId);
for (const r of allResults) {
  const flag = r.severity === 'error' ? '✗ ERROR' : r.severity === 'warn' ? '⚠ WARN' : '✓ INFO';
  console.log(`  ${flag}: ${r.check_name}: actual=${r.actual_value?.toFixed(3)} threshold=${r.threshold_value}`);
}

console.log('\n--- bucket summary ---');
const bucketSummary = db.prepare(`
  SELECT bucket_code, SUM(bucket_amount) AS total_jpy, SUM(row_count) AS total_rows
  FROM accounting_diff_buckets WHERE run_id = ? GROUP BY bucket_code ORDER BY ABS(total_jpy) DESC
`).all(runId);
console.table(bucketSummary);

// ============================================================
// GChat 通知 (severity='error' breach 時のみ)
// ============================================================
if (hasError && process.env.GCHAT_WEBHOOK_INSIGHT) {
  const errorIssues = issues.filter(i => i.severity === 'error');
  const lines = [
    `*Amazon Finance DQ Gate ALERT*`,
    `run_id: ${runId}`,
    `month: ${monthStr}`,
    `error count: ${errorIssues.length}`,
    ``,
    ...errorIssues.map(i => `• ${i.checkName}: ${i.actualValue?.toFixed(3)} (threshold ${i.thresholdValue})`),
  ];
  const text = lines.join('\n');
  try {
    const res = await fetch(process.env.GCHAT_WEBHOOK_INSIGHT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    console.log(`\n→ GChat notified (${res.status})`);
  } catch (e) {
    console.error(`GChat notify failed: ${e.message}`);
  }
} else if (hasError) {
  console.log('\n(GCHAT_WEBHOOK_INSIGHT 未設定、ローカル通知のみ)');
}

db.close();

if (hasError) {
  console.error(`\n✗ DQ gate FAILED (${issues.filter(i => i.severity === 'error').length} error)`);
  process.exit(1);
} else if (issues.length > 0) {
  console.log(`\n⚠ DQ gate passed with ${issues.length} warning`);
  process.exit(0);
} else {
  console.log('\n✓ DQ gate PASSED');
  process.exit(0);
}
