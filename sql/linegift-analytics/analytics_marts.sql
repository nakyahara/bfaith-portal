-- analytics_marts.sql — LINEギフト Phase 1 A-5c
--   分析 materialized table 4 種 + view 5 本
--
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-5
-- Codex 議論: 商品分析 Round 1-6 GO 判定、Round 3 Critical (percentile 代替) / Round 5 High-1 (as_of 統一)
--
-- 設計方針:
--   - 全 materialized table は同一 as_of_date_jst を共有 (build-linegift-analytics-mart.js が 1 回で生成)
--   - percentile (p50/p80) は SQLite に組み込み無し → Node.js で計算して materialized table へ書き出す (案3)
--   - 7/28/90 期間別を window_days PK で保持 (定型は事前集計、カスタム期間はアプリ層動的SQL)
--   - 価格帯 master/sales 2 系統分離 (master = m_product_classifications.price_band_code_master、sales = 本 view 内派生)

-- ─────────────────────────────────────────────────────────
-- 1) f_linegift_kpi_summary_daily — Looker トップカード 5 項目 (期間別)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS f_linegift_kpi_summary_daily (
  as_of_date_jst               TEXT NOT NULL,                  -- 'YYYY-MM-DD' (build batch 実行日 JST)
  window_days                  INTEGER NOT NULL CHECK (window_days IN (7, 28, 90)),
  total_sales_jpy_incl         INTEGER NOT NULL,
  total_orders                 INTEGER NOT NULL,
  avg_order_value_jpy_incl     INTEGER NOT NULL,
  total_gross_profit_jpy_incl  INTEGER NOT NULL,
  gross_margin_rate            REAL NOT NULL,                  -- 0.XX
  synced_at                    TEXT NOT NULL,
  PRIMARY KEY (as_of_date_jst, window_days)
);

-- ─────────────────────────────────────────────────────────
-- 2) f_linegift_sku_perf_daily — SKU 別期間集計 (7/28/90 共通)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS f_linegift_sku_perf_daily (
  as_of_date_jst             TEXT NOT NULL,
  window_days                INTEGER NOT NULL CHECK (window_days IN (7, 28, 90)),
  ne_code                    TEXT NOT NULL,
  sku_code                   TEXT NOT NULL,
  units                      INTEGER NOT NULL,
  sales_amount_jpy_incl      INTEGER NOT NULL,
  gross_profit_jpy_incl      INTEGER NOT NULL,
  orders                     INTEGER NOT NULL,
  active_days                INTEGER NOT NULL,                 -- 売上があった日数
  velocity_per_active_day    REAL NOT NULL,                    -- units / active_days
  velocity_per_calendar_day  REAL NOT NULL,                    -- units / window_days (固定分母)
  unit_price_jpy_incl_raw    REAL NOT NULL,                    -- 丸め前の単価 (境界判定用)
  unit_price_jpy_incl_display INTEGER NOT NULL,                -- 丸め後の表示用単価
  gross_margin_rate          REAL NOT NULL,
  synced_at                  TEXT NOT NULL,
  PRIMARY KEY (as_of_date_jst, window_days, ne_code, sku_code)
);

CREATE INDEX IF NOT EXISTS idx_f_linegift_sku_perf_window
  ON f_linegift_sku_perf_daily (as_of_date_jst, window_days, sales_amount_jpy_incl DESC);

-- ─────────────────────────────────────────────────────────
-- 3) f_linegift_segment_benchmark_daily — genre × price_band の p50/p80 (Node.js 計算)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS f_linegift_segment_benchmark_daily (
  as_of_date_jst            TEXT NOT NULL,
  window_days               INTEGER NOT NULL CHECK (window_days IN (7, 28, 90)),
  main_genre_id             TEXT,
  price_band_code_master    TEXT,
  sku_count                 INTEGER NOT NULL,
  median_velocity_per_calendar_day  REAL NOT NULL,
  p80_velocity_per_calendar_day     REAL NOT NULL,
  synced_at                 TEXT NOT NULL,
  PRIMARY KEY (as_of_date_jst, window_days, main_genre_id, price_band_code_master)
);

-- ─────────────────────────────────────────────────────────
-- 4) f_linegift_price_band_summary_daily — 価格帯 8段階別 (sales 系)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS f_linegift_price_band_summary_daily (
  as_of_date_jst            TEXT NOT NULL,
  window_days               INTEGER NOT NULL CHECK (window_days IN (7, 28, 90)),
  price_band_code_sales     TEXT NOT NULL,                     -- m_price_bands.band_code (実売単価ベース)
  band_label                TEXT NOT NULL,
  sort_order                INTEGER NOT NULL,
  sku_count                 INTEGER NOT NULL,
  orders_count              INTEGER NOT NULL,
  sales_jpy_incl            INTEGER NOT NULL,
  gross_profit_jpy_incl     INTEGER NOT NULL,
  gross_margin_rate         REAL NOT NULL,
  synced_at                 TEXT NOT NULL,
  PRIMARY KEY (as_of_date_jst, window_days, price_band_code_sales)
);

-- ─────────────────────────────────────────────────────────
-- View: 最新 as_of_date_jst の materialized table の参照
-- ─────────────────────────────────────────────────────────

DROP VIEW IF EXISTS v_linegift_kpi_summary_latest;
CREATE VIEW v_linegift_kpi_summary_latest AS
SELECT * FROM f_linegift_kpi_summary_daily
WHERE as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM f_linegift_kpi_summary_daily);

DROP VIEW IF EXISTS v_linegift_sku_perf_latest_90d;
CREATE VIEW v_linegift_sku_perf_latest_90d AS
SELECT * FROM f_linegift_sku_perf_daily
WHERE window_days = 90
  AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM f_linegift_sku_perf_daily WHERE window_days = 90);

DROP VIEW IF EXISTS v_linegift_segment_benchmark_latest_90d;
CREATE VIEW v_linegift_segment_benchmark_latest_90d AS
SELECT * FROM f_linegift_segment_benchmark_daily
WHERE window_days = 90
  AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM f_linegift_segment_benchmark_daily WHERE window_days = 90);

DROP VIEW IF EXISTS v_linegift_price_band_summary_latest_90d;
CREATE VIEW v_linegift_price_band_summary_latest_90d AS
SELECT * FROM f_linegift_price_band_summary_daily
WHERE window_days = 90
  AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM f_linegift_price_band_summary_daily WHERE window_days = 90)
ORDER BY sort_order;

-- ─────────────────────────────────────────────────────────
-- View: 月別トレンド (Looker 月次グラフ再現、過去 12ヶ月、as-of スナップショット集計)
-- ─────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_linegift_monthly_trend;
CREATE VIEW v_linegift_monthly_trend AS
WITH base AS (
  SELECT
    substr(f.date_jst, 1, 7) AS year_month,
    f.gross_sales_jpy_incl, f.variable_margin_jpy_incl, f.order_count
  FROM f_linegift_finance_sku_daily_v1 f
  WHERE f.date_jst >= date('now', '+9 hours', 'start of month', '-11 months')
)
SELECT
  year_month,
  ROUND(COALESCE(SUM(gross_sales_jpy_incl), 0)) AS sales_jpy_incl,
  ROUND(COALESCE(SUM(variable_margin_jpy_incl), 0)) AS gross_profit_jpy_incl,
  COALESCE(SUM(order_count), 0) AS orders,
  CASE WHEN COALESCE(SUM(gross_sales_jpy_incl), 0) = 0 THEN 0
       ELSE ROUND(COALESCE(SUM(variable_margin_jpy_incl), 0) * 1.0 / COALESCE(SUM(gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate
FROM base
GROUP BY year_month
ORDER BY year_month;

-- ─────────────────────────────────────────────────────────
-- View: SKU 相対ベロシティ (segment benchmark との比較 + percent_rank)
-- ─────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_linegift_sku_relative_score_90d;
CREATE VIEW v_linegift_sku_relative_score_90d AS
WITH perf AS (SELECT * FROM v_linegift_sku_perf_latest_90d),
cls AS (
  SELECT ne_product_code, ne_sku_code, main_genre_id, price_band_code_master
  FROM m_product_classifications
  WHERE is_active=1 AND valid_to IS NULL
),
bm AS (SELECT * FROM v_linegift_segment_benchmark_latest_90d)
SELECT
  p.ne_code, p.sku_code,
  c.main_genre_id, c.price_band_code_master,
  p.velocity_per_active_day,
  p.velocity_per_calendar_day,
  b.median_velocity_per_calendar_day,
  b.p80_velocity_per_calendar_day,
  CASE WHEN b.median_velocity_per_calendar_day IS NULL OR b.median_velocity_per_calendar_day <= 0 THEN NULL
       ELSE p.velocity_per_calendar_day / b.median_velocity_per_calendar_day END AS velocity_index_vs_median,
  PERCENT_RANK() OVER (
    PARTITION BY c.main_genre_id, c.price_band_code_master
    ORDER BY p.velocity_per_calendar_day
  ) AS velocity_percent_rank
FROM perf p
LEFT JOIN cls c
  ON c.ne_product_code = p.ne_code
 AND COALESCE(c.ne_sku_code,'') = COALESCE(p.sku_code,'')
LEFT JOIN bm b
  ON COALESCE(b.main_genre_id,'') = COALESCE(c.main_genre_id,'')
 AND COALESCE(b.price_band_code_master,'') = COALESCE(c.price_band_code_master,'');

-- ─────────────────────────────────────────────────────────
-- View: シーン期間 SKU 別 3指標 (シーン × year で時系列比較可)
-- ─────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_linegift_season_sku_perf;
CREATE VIEW v_linegift_season_sku_perf AS
SELECT
  s.season_code, s.season_year,
  s.start_date_jst, s.end_date_jst,
  f.sku_code, f.ne_code,
  COALESCE(SUM(f.units_net_sold), 0) AS season_units,
  ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS season_sales,
  ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0)) AS season_profit
FROM d_gift_season_occurrences s
JOIN f_linegift_finance_sku_daily_v1 f
  ON f.date_jst BETWEEN s.start_date_jst AND s.end_date_jst
WHERE s.is_active = 1
GROUP BY s.season_code, s.season_year, s.start_date_jst, s.end_date_jst, f.sku_code, f.ne_code;

-- ─────────────────────────────────────────────────────────
-- View: 新商品立ち上がりカーブ (first_sold_at 起点、day_from_launch 0-60)
-- ─────────────────────────────────────────────────────────
DROP VIEW IF EXISTS v_linegift_new_item_ramp;
CREATE VIEW v_linegift_new_item_ramp AS
WITH first_sale AS (
  SELECT sku_code, ne_code, MIN(date_jst) AS first_sale_date_jst
  FROM f_linegift_finance_sku_daily_v1
  GROUP BY sku_code, ne_code
)
SELECT
  f.sku_code, f.ne_code, fs.first_sale_date_jst,
  CAST(julianday(f.date_jst) - julianday(fs.first_sale_date_jst) AS INTEGER) AS day_from_launch,
  f.units_net_sold AS units,
  f.gross_sales_jpy_incl AS sales,
  f.variable_margin_jpy_incl AS profit
FROM f_linegift_finance_sku_daily_v1 f
JOIN first_sale fs
  ON fs.sku_code = f.sku_code AND fs.ne_code = f.ne_code
WHERE (julianday(f.date_jst) - julianday(fs.first_sale_date_jst)) BETWEEN 0 AND 60;
