-- ============================================================
-- Phase 1 Release Checks SQL
-- ============================================================
-- 用途: phase1-amazon-finance-release.md の section 1-6 で参照する判定 SQL 集
-- 実行: sqlite3 warehouse.db ".read sql/runbooks/phase1_release_checks.sql"

.print '=== 1-C. f_amazon_finance_sku_daily_v1 baseline (月別 row 数 + profit) ==='
SELECT
  substr(date_jst, 1, 7) AS month,
  COUNT(*) AS rows,
  COUNT(DISTINCT seller_sku) AS distinct_skus,
  ROUND(SUM(profit_amount), 0) AS profit_jpy,
  SUM(CASE WHEN cost_status = 'missing_cost' THEN 1 ELSE 0 END) AS missing_cost_rows
FROM f_amazon_finance_sku_daily_v1
GROUP BY 1
ORDER BY 1;

.print ''
.print '=== 3-B. DQ run results (latest 5 runs) ==='
SELECT
  run_id,
  check_name,
  severity,
  ROUND(actual_value, 3) AS actual,
  threshold_value AS threshold,
  checked_at
FROM dq_run_results
WHERE checked_at >= date('now', '-30 day')
ORDER BY checked_at DESC, severity DESC, check_name
LIMIT 50;

.print ''
.print '=== 3-C. accounting_diff_buckets (latest run, 月別 bucket 集計) ==='
SELECT
  bucket_code,
  ROUND(SUM(bucket_amount), 0) AS total_jpy,
  SUM(row_count) AS total_rows,
  COUNT(DISTINCT month_jst) AS months_covered
FROM accounting_diff_buckets
WHERE created_at >= date('now', '-7 day')
GROUP BY bucket_code
ORDER BY ABS(SUM(bucket_amount)) DESC;

.print ''
.print '=== 4. job_locks 現状 ==='
SELECT
  job_name,
  CASE WHEN expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
       THEN 'EXPIRED (stale)' ELSE 'ACTIVE' END AS status,
  acquired_at,
  expires_at,
  heartbeat_at,
  substr(holder_id, 1, 30) || '...' AS holder
FROM job_locks
ORDER BY job_name;

.print ''
.print '=== Validation: daily fact vs v4 (gross_margin_with_reimbursement) 月別比較 ==='
WITH daily AS (
  SELECT
    substr(date_jst, 1, 7) AS month,
    SUM(units_ordered) AS units_d,
    SUM(profit_amount) AS profit_d
  FROM f_amazon_finance_sku_daily_v1
  GROUP BY 1
),
v4 AS (
  SELECT
    year_month AS month,
    SUM(qty_ordered) AS units_v4,
    SUM(gross_margin_excl_tax)
      + SUM(warehouse_damage_jpy) + SUM(warehouse_lost_jpy) + SUM(safe_t_jpy)
      + SUM(refund_principal_jpy) + SUM(reversal_reimbursement_jpy) AS profit_v4
  FROM v_amazon_sku_profit_actual_v4
  GROUP BY 1
)
SELECT
  d.month,
  d.units_d,
  v.units_v4,
  d.units_d - v.units_v4 AS units_diff,
  ROUND(d.profit_d, 0) AS profit_d,
  ROUND(v.profit_v4, 0) AS profit_v4,
  ROUND(d.profit_d - v.profit_v4, 0) AS profit_diff,
  ROUND(ABS(d.profit_d - v.profit_v4) / NULLIF(ABS(v.profit_v4), 0) * 100, 3) AS profit_diff_pct
FROM daily d JOIN v4 v ON v.month = d.month
ORDER BY d.month;

.print ''
.print '=== cost_status 内訳 (5 ヶ月合計) ==='
SELECT
  cost_status,
  COUNT(*) AS rows,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM f_amazon_finance_sku_daily_v1), 2) AS pct
FROM f_amazon_finance_sku_daily_v1
GROUP BY cost_status
ORDER BY rows DESC;

.print ''
.print '=== sync_runs 状態 (Phase 1.x で sync 実装後参照) ==='
SELECT
  COALESCE(status, '__no_runs__') AS status,
  COUNT(*) AS run_count
FROM (
  SELECT 'sync_runs テーブルは Phase 1.x で追加 (#1-4a)' AS status WHERE NOT EXISTS (
    SELECT 1 FROM sqlite_master WHERE name = 'sync_runs'
  )
  UNION ALL
  SELECT status FROM (
    SELECT NULL AS status WHERE 1=0  -- placeholder
  )
)
GROUP BY status;
