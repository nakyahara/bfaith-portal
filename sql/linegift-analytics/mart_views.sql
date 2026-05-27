-- mart_views.sql — LINEギフト v1.0 Commit 4
--   View 4 本 (mart materialized 由来 + シーン期間集計 + 新商品立ち上がり)
--
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §5 / §7

-- ─────────────────────────────────────────────────────────
-- 最新 as_of_date_jst の mart を返す view (UI 表示用)
-- ─────────────────────────────────────────────────────────

DROP VIEW IF EXISTS v_linegift_kpi_summary_latest;
CREATE VIEW v_linegift_kpi_summary_latest AS
SELECT * FROM mart_linegift_kpi_summary_daily
WHERE as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM mart_linegift_kpi_summary_daily);

DROP VIEW IF EXISTS v_linegift_sku_perf_latest_90d;
CREATE VIEW v_linegift_sku_perf_latest_90d AS
SELECT * FROM mart_linegift_sku_perf_daily
WHERE window_days = 90
  AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM mart_linegift_sku_perf_daily WHERE window_days = 90);

DROP VIEW IF EXISTS v_linegift_price_band_summary_latest_90d;
CREATE VIEW v_linegift_price_band_summary_latest_90d AS
SELECT * FROM mart_linegift_price_band_summary_daily
WHERE window_days = 90
  AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM mart_linegift_price_band_summary_daily WHERE window_days = 90)
ORDER BY sort_order;

-- ─────────────────────────────────────────────────────────
-- 月別トレンド (12ヶ月、Looker 月次グラフ再現、as-of スナップショット集計)
-- ※ frozen 行 (90日以前の凍結) も含む、再取込で過去月の値が補正される可能性あり
-- ※ 2026-05-27 中原さん指摘で送料引き算追加 (mirror_products.送料 を税込換算で控除、Amazon FBM パターン)
-- ─────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_linegift_monthly_trend;
CREATE VIEW v_linegift_monthly_trend AS
WITH base AS (
  SELECT
    substr(f.date_jst, 1, 7) AS year_month,
    f.gross_sales_jpy_incl, f.variable_margin_jpy_incl, f.order_count, f.units_net_sold,
    -- mirror_products.送料 (税抜) を税込換算 (消費税率 NULL なら 0.1 デフォルト)
    COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1)) AS unit_shipping_cost_jpy_incl
  FROM mirror_linegift_finance_sku_daily f
  LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
  WHERE f.date_jst >= date('now', '+9 hours', 'start of month', '-11 months')
)
SELECT
  year_month,
  ROUND(COALESCE(SUM(gross_sales_jpy_incl), 0)) AS sales_jpy_incl,
  -- 送料引き後粗利
  ROUND(COALESCE(SUM(variable_margin_jpy_incl), 0) - COALESCE(SUM(units_net_sold * unit_shipping_cost_jpy_incl), 0)) AS gross_profit_jpy_incl,
  COALESCE(SUM(order_count), 0) AS orders,
  CASE WHEN COALESCE(SUM(gross_sales_jpy_incl), 0) = 0 THEN 0
       ELSE ROUND((COALESCE(SUM(variable_margin_jpy_incl), 0) - COALESCE(SUM(units_net_sold * unit_shipping_cost_jpy_incl), 0)) * 1.0 / COALESCE(SUM(gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate
FROM base
GROUP BY year_month
ORDER BY year_month;

-- ─────────────────────────────────────────────────────────
-- シーン期間 SKU 別 3指標 (シーン × season_year で時系列比較可)
-- 2026-05-27 中原さん指摘で送料引き算追加
-- ─────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_linegift_season_sku_perf;
CREATE VIEW v_linegift_season_sku_perf AS
SELECT
  s.season_code, s.season_year,
  s.start_date_jst, s.end_date_jst,
  f.sku_code, f.ne_code, MAX(f.product_name) AS product_name,
  COALESCE(SUM(f.units_net_sold), 0) AS season_units,
  ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS season_sales,
  -- 送料引き後粗利 (mirror_products.送料 を税込換算で控除)
  ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) AS season_profit,
  COALESCE(SUM(f.order_count), 0) AS season_orders
FROM mart_gift_season_occurrences s
JOIN mirror_linegift_finance_sku_daily f
  ON f.date_jst BETWEEN s.start_date_jst AND s.end_date_jst
LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
WHERE s.is_active = 1
GROUP BY s.season_code, s.season_year, s.start_date_jst, s.end_date_jst, f.sku_code, f.ne_code;

-- ─────────────────────────────────────────────────────────
-- 新商品立ち上がりカーブ (first_sold_at 起点、day_from_launch 0-60)
-- ─────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_linegift_new_item_ramp;
CREATE VIEW v_linegift_new_item_ramp AS
WITH first_sale AS (
  SELECT sku_code, ne_code, MIN(date_jst) AS first_sale_date_jst
  FROM mirror_linegift_finance_sku_daily
  GROUP BY sku_code, ne_code
)
SELECT
  f.sku_code, f.ne_code, f.product_name, fs.first_sale_date_jst,
  CAST(julianday(f.date_jst) - julianday(fs.first_sale_date_jst) AS INTEGER) AS day_from_launch,
  f.units_net_sold AS units,
  f.gross_sales_jpy_incl AS sales,
  f.variable_margin_jpy_incl AS profit
FROM mirror_linegift_finance_sku_daily f
JOIN first_sale fs
  ON fs.sku_code = f.sku_code AND fs.ne_code = f.ne_code
WHERE (julianday(f.date_jst) - julianday(fs.first_sale_date_jst)) BETWEEN 0 AND 60;
