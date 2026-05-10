-- f_yahoo_finance_sku_daily_v1 DDL
-- ticket: Yahoo Phase 1 Y-1
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1a設計書_v0.4_20260510.md
-- contract: docs/contracts/raw_yahoo_orders.contract.md (将来作成)
-- build SQL: sql/yahoo/build_f_yahoo_finance_sku_daily_v1.sql
--
-- 売上計上方針 (v0.4 確定):
--   - whitelist: order_status='5' AND pay_status='1' AND ship_status='3' (98.8% カバー)
--   - units_ordered = SUM(quantity) per (date, sku)
--   - units_cancelled = 0 (Phase 1a で fact_returns 取れない、Phase 1c-3 で本採用)
--   - units_net_sold = units_ordered (Phase 1a)
--
-- partial / full margin 2 段階設計 (v0.2-v0.3 確立):
--   - Phase 1a: partial margin (6 要素) を確定計上
--     売上 - 原価 - モール手数料 - 送料 - クーポン - use_point
--   - Phase 1c-3: full margin (10 要素) backfill
--     partial - 決済手数料 - PSR - 広告費 - 梱包材 - 同梱物 - 返品
--
-- SKU 解決 3 段階 fallback (v0.4 確定、Codex Critical 反映):
--   1. m_products.商品コード = sub_code (variant 別、56.80%)
--   2. m_products.商品コード = item_id (親 SKU、+11.24%)
--   3. f_yahoo_sku_map (手動 map、残り)
--   → unresolved_sku_flag = 1 を必ず保持 (variant 粒度 喪失防止)
--
-- 不変条件 (楽天 Phase 1a/1b から継承):
--   - PK: (date_jst, yahoo_sku_key)
--   - UPSERT で snapshot 列 (unit_cost_snapshot_incl, cost_snapshot_date_jst, is_cost_complete, cost_status) 温存
--   - cogs / margin は「既存 snapshot × 新 units_net_sold」で再計算

CREATE TABLE IF NOT EXISTS f_yahoo_finance_sku_daily_v1 (
  -- PK
  date_jst       TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
  yahoo_sku_key  TEXT NOT NULL CHECK(trim(yahoo_sku_key) <> ''),  -- = item_id-sub_code or item_id

  -- 紐付け (v0.4 Codex Critical 反映)
  ne_code              TEXT,
  variant_key          TEXT NOT NULL DEFAULT '',  -- = sub_code (variant 粒度保持)
  -- Phase 1.1: ラベル名は v1 互換のため旧名を継続。実態は CI (case-insensitive) join。
  resolution_method    TEXT NOT NULL CHECK (
    resolution_method IN ('sub_match', 'parent_match', 'manual_map', 'unresolved')
  ),
  unresolved_sku_flag  INTEGER NOT NULL DEFAULT 0,
  product_name         TEXT NOT NULL DEFAULT '',

  -- 数量 (Phase 1a では cancelled = 0 固定)
  units_ordered    INTEGER NOT NULL DEFAULT 0,
  units_cancelled  INTEGER NOT NULL DEFAULT 0,
  units_net_sold   INTEGER NOT NULL DEFAULT 0,

  -- 売上 3 列分離 (v0.3 Codex R4 #5 反映、暫定ロジックを売上列に混ぜない)
  sales_principal_jpy_incl         REAL NOT NULL DEFAULT 0,  -- = unit_price × quantity (税込)
  sales_postage_jpy_incl           REAL NOT NULL DEFAULT 0,  -- = 0 (中原さん全送料無料)
  gross_sales_jpy_incl             REAL NOT NULL DEFAULT 0,  -- = principal + postage
  net_sales_before_point_jpy_incl  REAL NOT NULL DEFAULT 0,  -- = gross - coupon_shop
  listing_sales_estimated_jpy_incl REAL NOT NULL DEFAULT 0,  -- = principal - use_point - coupon (TODO 1c-3 verify)

  -- クーポン (税込、商品単位、按分不要)
  coupon_shop_jpy_incl  REAL NOT NULL DEFAULT 0,  -- = SUM(coupon_discount)

  -- 顧客 PT 控除
  use_point_jpy_incl    REAL NOT NULL DEFAULT 0,  -- = SUM(use_point) (Yahoo + PayPay 混在)

  -- モール手数料 (税込) + 推計監視 (v0.3 Codex R4 #3 反映)
  -- 仮 10% (楽天と同レート、Phase 1c-3 で実 % verify)
  mall_fee_jpy_incl           REAL NOT NULL DEFAULT 0,
  mall_fee_calc_method        TEXT NOT NULL DEFAULT 'estimated_10pct' CHECK (
    mall_fee_calc_method IN ('estimated_10pct', 'actual_statement')
  ),
  mall_fee_estimate_delta_jpy REAL,  -- Phase 1c-3 で actual との差分、月次 alert

  -- 送料コスト (お店負担、税込)
  -- m_products.送料 推定 (楽天 Phase 1a と同型 shipping_lookup)
  shipping_cost_jpy_incl  REAL NOT NULL DEFAULT 0,
  shipping_quality        TEXT NOT NULL CHECK (
    shipping_quality IN ('actual', 'estimated_rates', 'estimated_fallback', 'missing')
  ),

  -- 原価 snapshot (楽天 Phase 1a と同型、初回 build 時固定、UPSERT で温存)
  unit_cost_snapshot_incl         REAL,
  cost_snapshot_date_jst          TEXT,
  -- ⚠️ snapshot ではない参照値: UPSERT で毎回 m_products の最新で上書き
  latest_unit_cost_reference_incl REAL,
  cogs_amount_jpy_incl            REAL NOT NULL DEFAULT 0,

  -- 利益 (partial / full 2 段階、v0.3 Codex R4 #1 反映)
  variable_margin_partial_jpy_incl    REAL NOT NULL DEFAULT 0,  -- ★ Phase 1a 確定 (NOT NULL)
  variable_margin_full_jpy_incl       REAL,                     -- ★ Phase 1c-3 で backfill
  refund_adjusted_net_sales_jpy_incl  REAL,                     -- Phase 1c-3 で backfill

  -- 限界利益 確定状態 (NULL 集計事故防止、view 経由で COALESCE 採用値)
  margin_confidence  TEXT NOT NULL DEFAULT 'partial' CHECK (
    margin_confidence IN ('partial', 'full')
  ),
  margin_full_finalized_at  TEXT,

  -- audit 列 (raw 由来、計算未使用、validation/reconcile 用に保持)
  pay_charge_audit_jpy_incl    REAL NOT NULL DEFAULT 0,  -- 顧客負担代引手数料
  ship_charge_audit_jpy_incl   REAL NOT NULL DEFAULT 0,  -- 顧客負担送料 (= 通常 0)
  discount_audit_jpy_incl      REAL NOT NULL DEFAULT 0,  -- 注文後手動値引き (= 通常 0)

  -- 品質 / 監視
  cost_status                  TEXT NOT NULL CHECK (
    cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
  ),
  is_cost_complete             INTEGER NOT NULL DEFAULT 0,
  data_quality_score           INTEGER NOT NULL DEFAULT 0
                               CHECK (data_quality_score BETWEEN 0 AND 100),
  price_variance_warning       INTEGER NOT NULL DEFAULT 0,

  -- メタ
  source_layer_summary  TEXT NOT NULL DEFAULT '',
  source_row_count      INTEGER NOT NULL DEFAULT 0,
  built_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (date_jst, yahoo_sku_key)
);

CREATE INDEX IF NOT EXISTS idx_f_yahoo_sku_v1_ne     ON f_yahoo_finance_sku_daily_v1 (ne_code);
CREATE INDEX IF NOT EXISTS idx_f_yahoo_sku_v1_month  ON f_yahoo_finance_sku_daily_v1 (substr(date_jst, 1, 7));
CREATE INDEX IF NOT EXISTS idx_f_yahoo_sku_v1_status ON f_yahoo_finance_sku_daily_v1 (cost_status);
CREATE INDEX IF NOT EXISTS idx_f_yahoo_sku_v1_unres  ON f_yahoo_finance_sku_daily_v1 (unresolved_sku_flag) WHERE unresolved_sku_flag = 1;
CREATE INDEX IF NOT EXISTS idx_f_yahoo_sku_v1_warn   ON f_yahoo_finance_sku_daily_v1 (price_variance_warning) WHERE price_variance_warning = 1;

-- v0.3 Codex R4 #1 反映: NULL 集計事故防止 reporting view
DROP VIEW IF EXISTS vw_yahoo_finance_for_reporting;
CREATE VIEW vw_yahoo_finance_for_reporting AS
SELECT
  *,
  -- 採用値: full が確定してれば full、まだなら partial (NULL 集計事故なし)
  COALESCE(variable_margin_full_jpy_incl, variable_margin_partial_jpy_incl) AS margin_value_for_reporting,
  -- 確定状態説明
  CASE
    WHEN variable_margin_full_jpy_incl IS NOT NULL THEN 'full_finalized'
    WHEN margin_confidence = 'partial' THEN 'partial_only'
    ELSE 'unknown'
  END AS margin_value_source
FROM f_yahoo_finance_sku_daily_v1;

-- v0.2 Codex R2 反映: provisional 別 view (whitelist 外、監視用、主 fact に混ぜない)
DROP VIEW IF EXISTS vw_yahoo_orders_provisional;
CREATE VIEW vw_yahoo_orders_provisional AS
SELECT * FROM raw_yahoo_orders
WHERE NOT (order_status='5' AND pay_status='1' AND ship_status='3')
  AND order_time >= date('now', '-90 days');
