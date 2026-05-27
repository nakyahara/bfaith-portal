-- mart_analytics_marts.sql — LINEギフト v1.0 Commit 3
--   分析 mart 3 表 (期間別 materialized、全て同一 as_of_date_jst で書く)
--
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §4 / §6
--
-- 入力: mirror_linegift_finance_sku_daily (既存稼働、miniPC f_linegift_finance_sku_daily_v1 の mirror)
--
-- 全テーブル mart_ prefix で sync_contracts 未登録 = mirror sync 対象外。
-- v0.9 の f_linegift_kpi_summary_daily 等を流用 (分類依存列を削除、prefix のみ変更)。

-- ─────────────────────────────────────────────────────────
-- mart_linegift_kpi_summary_daily — トップ KPI 5 項目 (期間別)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_linegift_kpi_summary_daily (
  as_of_date_jst                TEXT NOT NULL,
  window_days                   INTEGER NOT NULL CHECK (window_days IN (7, 28, 90)),
  total_sales_jpy_incl          INTEGER NOT NULL,
  total_orders                  INTEGER NOT NULL,
  avg_order_value_jpy_incl      INTEGER NOT NULL,
  total_gross_profit_jpy_incl   INTEGER NOT NULL,
  gross_margin_rate             REAL NOT NULL,
  synced_at                     TEXT NOT NULL,
  PRIMARY KEY (as_of_date_jst, window_days)
);

-- ─────────────────────────────────────────────────────────
-- mart_linegift_sku_perf_daily — SKU 別期間集計 (7/28/90 共通)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_linegift_sku_perf_daily (
  as_of_date_jst                TEXT NOT NULL,
  window_days                   INTEGER NOT NULL CHECK (window_days IN (7, 28, 90)),
  ne_code                       TEXT NOT NULL,
  sku_code                      TEXT NOT NULL,
  product_name                  TEXT,                                          -- 表示用 (mirror_linegift_finance_sku_daily.product_name)
  units                         INTEGER NOT NULL,
  sales_amount_jpy_incl         INTEGER NOT NULL,
  gross_profit_jpy_incl         INTEGER NOT NULL,
  orders                        INTEGER NOT NULL,
  active_days                   INTEGER NOT NULL,
  velocity_per_active_day       REAL NOT NULL,
  velocity_per_calendar_day     REAL NOT NULL,
  unit_price_jpy_incl_raw       REAL NOT NULL,                                 -- 境界判定用 (丸め前)
  unit_price_jpy_incl_display   INTEGER NOT NULL,                              -- 表示用 (丸め後)
  gross_margin_rate             REAL NOT NULL,
  synced_at                     TEXT NOT NULL,
  PRIMARY KEY (as_of_date_jst, window_days, ne_code, sku_code)
);

CREATE INDEX IF NOT EXISTS idx_mart_linegift_sku_perf_sort
  ON mart_linegift_sku_perf_daily (as_of_date_jst, window_days, sales_amount_jpy_incl DESC);

CREATE INDEX IF NOT EXISTS idx_mart_linegift_sku_perf_profit
  ON mart_linegift_sku_perf_daily (as_of_date_jst, window_days, gross_profit_jpy_incl DESC);

-- ─────────────────────────────────────────────────────────
-- mart_linegift_price_band_summary_daily — 価格帯 8 段階別 (sales 系)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_linegift_price_band_summary_daily (
  as_of_date_jst                TEXT NOT NULL,
  window_days                   INTEGER NOT NULL CHECK (window_days IN (7, 28, 90)),
  price_band_code_sales         TEXT NOT NULL,                                 -- mart_price_bands.band_code
  band_label                    TEXT NOT NULL,
  sort_order                    INTEGER NOT NULL,
  sku_count                     INTEGER NOT NULL,
  orders_count                  INTEGER NOT NULL,
  sales_jpy_incl                INTEGER NOT NULL,
  gross_profit_jpy_incl         INTEGER NOT NULL,
  gross_margin_rate             REAL NOT NULL,
  synced_at                     TEXT NOT NULL,
  PRIMARY KEY (as_of_date_jst, window_days, price_band_code_sales)
);
