-- f_linegift_finance_sku_daily_v1 DDL
-- ticket: LINEギフト Phase 1 A-2
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.5_20260515.md §6-2
-- build SQL: sql/linegift/build_f_linegift_finance_sku_daily_v1.sql
--
-- 売上計上方針 (v0.5 確定、A-0 verify 済 2,202 received / 2,416 注文 = 91.1%):
--   - whitelist: status = 'received' (recognized_on = received_on_jst)
--   - units_ordered = SUM(stock_count) per (date_jst, sku_code)、cancel/その他は別 view 監視 (主 fact 入れない)
--   - units_cancelled = 0 (Phase 1a)
--   - units_net_sold = units_ordered (Phase 1a)
--
-- partial → full margin の 3 段階 confidence (au PAY の 2 段階 'partial'/'full' と異なる):
--   - 'provisional_full_candidate' (Phase A): 5 要素確定 (gross - cogs - mall_fee_actual - shipping - 0)
--     ※ mall_fee は API 実額 (raw_linegift_orders.fee)、calc_method='actual_api'
--     ※ shipping は raw.shipping_fee (現状 NULL → 0)、quality='no_shipping_in_api'
--     ※ クーポン/ポイント/広告等の不確定要素なし → "実は full 候補" の意 (Codex Round 2 反映で名称弱化)
--   - 'full_minus_returns' (Phase B 着手後): cancel 注文の refund 額確定前
--   - 'full' (Phase B 完了): refund 確定 = margin 確定
--
-- SKU 解決 2 段 fallback (variation.code 100% master_match の前提、verify 済):
--   1. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(sku_code))  (= master_match、~100%)
--   2. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(parent_item_code))  (= parent_match、variation 不在時)
--   3. NULL → unresolved_sku_flag = 1
--
-- LINEギフト 特有 (vs au PAY):
--   - 按分処理なし (1注文1明細の grain そのまま)
--   - ポイント関連列なし
--   - 税率別売上列なし
--   - 送料は raw.shipping_fee (現状 LINEギフト API は送料 field 無し → NULL/0)
--   - mall_fee は API 実額 (rate 計算ではなく集計)
--   - 90日境界 frozen horizon 管理 (raw_linegift_orders.is_frozen_after_horizon と同期)
--
-- 不変条件 (au PAY 同型):
--   - PK: (date_jst, sku_code)
--   - UPSERT で snapshot 列 (unit_cost_snapshot_incl, cost_snapshot_date_jst, is_cost_complete, cost_status):
--     cost_status='complete' の行のみ凍結 (取引時点原価)。partial_cost / missing_cost は replaceable
--   - cogs / margin は「有効 snapshot × 新 units_net_sold」で再計算

CREATE TABLE IF NOT EXISTS f_linegift_finance_sku_daily_v1 (
  -- PK
  date_jst         TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),  -- = received_date_jst (= recognized_on)
  sku_code         TEXT NOT NULL CHECK(trim(sku_code) <> ''),         -- = variation.code (LOWER(TRIM())、100% master_match 想定)

  -- 紐付け
  ne_code             TEXT,
  parent_item_code    TEXT NOT NULL DEFAULT '',  -- = item.code (親軸分析用、Codex #6)
  variant_key         TEXT NOT NULL DEFAULT '',  -- = variation.name (option_info)
  resolution_method   TEXT NOT NULL CHECK (
    resolution_method IN ('master_match', 'parent_match', 'unresolved')
  ),
  unresolved_sku_flag INTEGER NOT NULL DEFAULT 0,
  product_name        TEXT NOT NULL DEFAULT '',  -- = item.name

  -- 数量 (Phase 1a では cancelled = 0 固定)
  units_ordered    INTEGER NOT NULL DEFAULT 0,
  units_cancelled  INTEGER NOT NULL DEFAULT 0,
  units_net_sold   INTEGER NOT NULL DEFAULT 0,

  -- 売上 (税込、API selling_price は税込・送料込みの統合価格)
  sales_principal_jpy_incl  REAL NOT NULL DEFAULT 0,  -- = SUM(selling_price)
  gross_sales_jpy_incl      REAL NOT NULL DEFAULT 0,  -- = sales_principal (LINEギフト は送料別 line がない)

  -- モール手数料 (税込、API 実額。calc_method='actual_api' が想定の常態)
  mall_fee_jpy_incl           REAL NOT NULL DEFAULT 0,  -- = SUM(raw.fee)
  mall_fee_calc_method        TEXT NOT NULL DEFAULT 'actual_api' CHECK (
    mall_fee_calc_method IN ('actual_api', 'actual_statement', 'estimated_rate', 'unknown')
  ),
  mall_fee_estimate_delta_jpy REAL,  -- Phase B で actual_statement との差分 (現状未使用)

  -- 送料コスト (お店負担、税込)
  -- Phase A: LINEギフト API に shipping_fee field なし → COALESCE(raw.shipping_fee, 0)、quality='no_shipping_in_api'
  -- Phase B: 仕様変更で API 提供開始したら 'actual_api' に自動切替
  shipping_cost_jpy_incl  REAL NOT NULL DEFAULT 0,
  shipping_quality        TEXT NOT NULL CHECK (
    shipping_quality IN ('no_shipping_in_api', 'actual_api', 'estimated_rates', 'estimated_fallback', 'missing')
  ),

  -- 原価 snapshot (初回 build 時固定、UPSERT で温存)
  unit_cost_snapshot_incl         REAL,
  cost_snapshot_date_jst          TEXT,
  latest_unit_cost_reference_incl REAL,  -- ⚠️ snapshot ではない: UPSERT で毎回 m_products 最新で上書き
  cogs_amount_jpy_incl            REAL NOT NULL DEFAULT 0,

  -- 利益 (3 段階 confidence、Codex Round 2 #2 反映)
  variable_margin_jpy_incl       REAL NOT NULL DEFAULT 0,  -- ★ Phase A 確定 = gross_sales - cogs - mall_fee_actual - shipping
  refund_adjusted_net_sales_jpy_incl  REAL,  -- Phase B で backfill
  margin_confidence  TEXT NOT NULL DEFAULT 'provisional_full_candidate' CHECK (
    margin_confidence IN ('provisional_full_candidate', 'full_minus_returns', 'full')
  ),
  margin_full_finalized_at  TEXT,

  -- LINEギフト 特有 (audit / 配送分析、Codex #6 + 設計書 §6-2)
  recognized_on_jst    TEXT,  -- = received_on_jst (= date_jst、明示化、Codex #3)
  bought_date_jst      TEXT,  -- 注文日 (audit、status flip 検知)
  delivered_lag_days   INTEGER,  -- 配送速度 (delivered - bought)、avg を mart で見る
  received_lag_days    INTEGER,  -- 受取完了率モニタ (received - bought)
  is_delivery_by_hand  INTEGER,  -- 手渡しフラグ (どれくらい来てるか)
  delivery_agent       TEXT,    -- 配送業者 (audit / 配送費分析)

  -- 90日境界凍結管理 (Codex Round 1 #1、設計書 §12)
  -- raw_linegift_orders 側で freeze 切替済み、ここはその時点の値の写像
  first_seen_in_api_at      TEXT,  -- 該当 (date_jst, sku_code) の MIN(raw.first_seen_at)
  last_seen_in_api_at       TEXT,  -- 該当 (date_jst, sku_code) の MAX(raw.last_seen_at)
  is_frozen_after_horizon   INTEGER NOT NULL DEFAULT 0,

  -- 品質 / 監視
  cost_status                  TEXT NOT NULL CHECK (
    cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
  ),
  is_cost_complete             INTEGER NOT NULL DEFAULT 0,
  data_quality_score           INTEGER NOT NULL DEFAULT 0
                               CHECK (data_quality_score BETWEEN 0 AND 100),

  -- メタ
  order_count           INTEGER NOT NULL DEFAULT 0,
  line_count            INTEGER NOT NULL DEFAULT 0,
  source_layer_summary  TEXT NOT NULL DEFAULT '',
  source_row_count      INTEGER NOT NULL DEFAULT 0,
  built_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (date_jst, sku_code)
);

CREATE INDEX IF NOT EXISTS idx_f_linegift_sku_v1_ne          ON f_linegift_finance_sku_daily_v1 (ne_code);
CREATE INDEX IF NOT EXISTS idx_f_linegift_sku_v1_month       ON f_linegift_finance_sku_daily_v1 (substr(date_jst, 1, 7));
CREATE INDEX IF NOT EXISTS idx_f_linegift_sku_v1_status      ON f_linegift_finance_sku_daily_v1 (cost_status);
CREATE INDEX IF NOT EXISTS idx_f_linegift_sku_v1_unres       ON f_linegift_finance_sku_daily_v1 (unresolved_sku_flag) WHERE unresolved_sku_flag = 1;
CREATE INDEX IF NOT EXISTS idx_f_linegift_sku_v1_frozen      ON f_linegift_finance_sku_daily_v1 (is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1;
CREATE INDEX IF NOT EXISTS idx_f_linegift_sku_v1_parent      ON f_linegift_finance_sku_daily_v1 (parent_item_code);

-- ─── reporting view (NULL 集計事故防止、3 段 confidence の COALESCE) ───
DROP VIEW IF EXISTS vw_linegift_finance_for_reporting;
CREATE VIEW vw_linegift_finance_for_reporting AS
SELECT
  *,
  -- margin_value_for_reporting: full → full_minus_returns → provisional_full_candidate の優先順位
  CASE
    WHEN margin_confidence = 'full' THEN variable_margin_jpy_incl
    WHEN margin_confidence = 'full_minus_returns' THEN variable_margin_jpy_incl
    ELSE variable_margin_jpy_incl  -- 'provisional_full_candidate' でも variable_margin_jpy_incl は計算済
  END AS margin_value_for_reporting,
  margin_confidence AS margin_value_source
FROM f_linegift_finance_sku_daily_v1;

-- ─── provisional view (whitelist 外、監視用、主 fact に混ぜない) ───
-- status='received' 以外 (cancel / payment / gift_message_send / cvs / gift_message_wait) を表示
DROP VIEW IF EXISTS vw_linegift_orders_provisional;
CREATE VIEW vw_linegift_orders_provisional AS
SELECT
  order_id, status, sku_code, parent_item_code, sku_name,
  selling_price, fee, stock_count,
  bought_date_jst, payed_date_jst, delivered_date_jst, received_date_jst,
  first_seen_at, last_seen_at, is_frozen_after_horizon
FROM raw_linegift_orders
WHERE status <> 'received'
  AND date(bought_date_jst) >= date('now', '+9 hours', '-90 days');

-- ─── frozen view (90日境界凍結状態の月別件数監視、Codex Round 1 #1) ───
DROP VIEW IF EXISTS vw_linegift_finance_horizon_status;
CREATE VIEW vw_linegift_finance_horizon_status AS
SELECT
  substr(date_jst, 1, 7) AS month,
  COUNT(*) AS total_rows,
  SUM(is_frozen_after_horizon) AS frozen_rows,
  SUM(CASE WHEN is_frozen_after_horizon = 0 THEN 1 ELSE 0 END) AS active_rows,
  ROUND(SUM(CASE WHEN is_frozen_after_horizon = 1 THEN variable_margin_jpy_incl ELSE 0 END), 2) AS frozen_margin_jpy,
  ROUND(SUM(CASE WHEN is_frozen_after_horizon = 0 THEN variable_margin_jpy_incl ELSE 0 END), 2) AS active_margin_jpy,
  MAX(last_seen_in_api_at) AS latest_api_observation
FROM f_linegift_finance_sku_daily_v1
GROUP BY month
ORDER BY month DESC;
