-- ============================================================
-- v4 vs daily fact monthly validation SQL
-- ============================================================
-- Phase 1 ticket: #1-3
-- 用途: f_amazon_finance_sku_daily_v1 の日次集計を月次に丸めて
--       v_amazon_sku_profit_actual_v4 と SKU x 月で比較
-- 使い方:
--   sqlite3 warehouse.db < sql/amazon/validate_v4_vs_daily_monthly.sql
--
-- 比較対象: v4.gross_margin_with_reimbursement_excl_tax (PR #61 で追加した新列)
--           ad_cost を引かない、warehouse 補填あり、中原さん 2026-05-08 確定
--
-- 出力 4 セクション:
--   A. 月別合計比較 (revenue / cogs / profit)
--   B. SKU x 月で profit 差大きい行 TOP 30
--   C. 集合差 (legacy_only / daily_only)
--   D. cost_status 別内訳 (daily fact 側)

.print '=== A. 月別合計比較 ==='
WITH daily_monthly AS (
  SELECT
    substr(date_jst, 1, 7) AS month_jst,
    SUM(units_ordered) AS units,
    SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS revenue,
    SUM(cogs_amount) AS cogs,
    SUM(profit_amount) AS profit
  FROM f_amazon_finance_sku_daily_v1
  GROUP BY 1
),
v4_monthly AS (
  SELECT
    year_month AS month_jst,
    SUM(qty_ordered) AS units,
    SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS revenue,
    SUM(cogs_excl_tax) AS cogs,
    SUM(gross_margin_excl_tax)
      + SUM(warehouse_damage_jpy) + SUM(warehouse_lost_jpy) + SUM(safe_t_jpy)
      + SUM(refund_principal_jpy) + SUM(reversal_reimbursement_jpy) AS profit
  FROM v_amazon_sku_profit_actual_v4
  GROUP BY 1
)
SELECT
  d.month_jst,
  d.units AS units_d,
  v.units AS units_v4,
  d.units - v.units AS units_diff,
  ROUND(d.revenue, 0) AS revenue_d,
  ROUND(v.revenue, 0) AS revenue_v4,
  ROUND(d.revenue - v.revenue, 0) AS revenue_diff,
  ROUND(d.cogs, 0) AS cogs_d,
  ROUND(v.cogs, 0) AS cogs_v4,
  ROUND(d.cogs - v.cogs, 0) AS cogs_diff,
  ROUND(d.profit, 0) AS profit_d,
  ROUND(v.profit, 0) AS profit_v4,
  ROUND(d.profit - v.profit, 0) AS profit_diff,
  ROUND(ABS(d.profit - v.profit) / NULLIF(ABS(v.profit), 0) * 100, 3) AS profit_diff_pct
FROM daily_monthly d JOIN v4_monthly v ON v.month_jst = d.month_jst
ORDER BY d.month_jst;

.print ''
.print '=== B. SKU x 月で profit 差絶対値 TOP 30 ==='
WITH daily_sm AS (
  SELECT
    substr(date_jst, 1, 7) AS month_jst,
    seller_sku,
    SUM(profit_amount) AS profit_d
  FROM f_amazon_finance_sku_daily_v1
  GROUP BY 1, 2
),
v4_sm AS (
  SELECT
    year_month AS month_jst,
    seller_sku,
    SUM(gross_margin_excl_tax)
      + SUM(warehouse_damage_jpy) + SUM(warehouse_lost_jpy) + SUM(safe_t_jpy)
      + SUM(refund_principal_jpy) + SUM(reversal_reimbursement_jpy) AS profit_v4
  FROM v_amazon_sku_profit_actual_v4
  GROUP BY 1, 2
)
SELECT
  COALESCE(d.month_jst, v.month_jst) AS month_jst,
  COALESCE(d.seller_sku, v.seller_sku) AS seller_sku,
  ROUND(COALESCE(d.profit_d, 0), 0) AS profit_d,
  ROUND(COALESCE(v.profit_v4, 0), 0) AS profit_v4,
  ROUND(COALESCE(d.profit_d, 0) - COALESCE(v.profit_v4, 0), 0) AS diff
FROM daily_sm d
LEFT JOIN v4_sm v ON v.month_jst = d.month_jst AND v.seller_sku = d.seller_sku
WHERE ABS(COALESCE(d.profit_d, 0) - COALESCE(v.profit_v4, 0)) > 0
ORDER BY ABS(COALESCE(d.profit_d, 0) - COALESCE(v.profit_v4, 0)) DESC
LIMIT 30;

.print ''
.print '=== C. 集合差 (legacy_only / daily_only) — 月別件数 ==='
WITH daily_sku AS (
  SELECT DISTINCT substr(date_jst,1,7) AS month_jst, seller_sku FROM f_amazon_finance_sku_daily_v1
),
v4_sku AS (
  SELECT DISTINCT year_month AS month_jst, seller_sku FROM v_amazon_sku_profit_actual_v4
)
SELECT
  COALESCE(d.month_jst, v.month_jst) AS month_jst,
  SUM(CASE WHEN d.seller_sku IS NULL THEN 1 ELSE 0 END) AS legacy_only_count,
  SUM(CASE WHEN v.seller_sku IS NULL THEN 1 ELSE 0 END) AS daily_only_count
FROM daily_sku d
FULL OUTER JOIN v4_sku v ON v.month_jst = d.month_jst AND v.seller_sku = d.seller_sku
GROUP BY 1
ORDER BY 1;

.print ''
.print '=== D. cost_status 別内訳 (daily fact 側) ==='
SELECT
  substr(date_jst, 1, 7) AS month_jst,
  cost_status,
  COUNT(*) AS rows,
  ROUND(SUM(profit_amount), 0) AS profit_jpy,
  ROUND(SUM(cogs_amount), 0) AS cogs_jpy
FROM f_amazon_finance_sku_daily_v1
GROUP BY 1, 2
ORDER BY 1, 2;
