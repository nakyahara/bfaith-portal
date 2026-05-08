-- ============================================================
-- f_amazon_finance_sku_daily_v1 — Amazon daily finance fact (DDL)
-- ============================================================
-- Phase 1 ticket: #1-1
-- Source: raw_amazon_settlement_lines (contract: docs/contracts/raw_amazon_settlement_lines.contract.md v1.1)
-- Mapping: docs/amazon-finance/canonical-metric-mapping.md (v1)
-- Cost policy: snapshot 方式 (取引日時点の原価を fact 行に固定、不変)
--   詳細: memory project_amazon_cogs_snapshot_decision.md
--
-- Phase 1 では「build 時点の v_sku_costed snapshot」で実装。
-- Phase 1.x で m_products_history を使った「取引日時点 snapshot」にupgrade予定。
--
-- snapshot 不変条件 (Codex Round 6):
--   - 初回 build 時に date_jst 時点原価で確定、その後不変
--   - full rebuild 時も既存 row があれば unit_cost_snapshot/cost_snapshot_date_jst/cogs_amount を上書きしない
--   - latest_unit_cost_reference だけは別ジョブで更新可
--   - rebuild は INSERT OR IGNORE 戦略 (既存 PK は触らない)

CREATE TABLE IF NOT EXISTS f_amazon_finance_sku_daily_v1 (
  date_jst TEXT NOT NULL,                     -- economic_date
  seller_sku TEXT NOT NULL,                   -- seller_sku_normalized
  asin_norm TEXT NOT NULL DEFAULT '',         -- 現状空、Phase 2 で raw_sp_orders と join
  product_name TEXT NOT NULL DEFAULT '',

  -- 数量 5 列
  units_ordered REAL NOT NULL DEFAULT 0,
  units_refunded_customer REAL NOT NULL DEFAULT 0,
  units_marketplace_guarantee REAL NOT NULL DEFAULT 0,
  units_a_to_z_refund REAL NOT NULL DEFAULT 0,
  units_net_sold REAL NOT NULL DEFAULT 0,

  -- 売上 4 列
  sales_principal_jpy REAL NOT NULL DEFAULT 0,
  sales_shipping_jpy REAL NOT NULL DEFAULT 0,
  sales_giftwrap_jpy REAL NOT NULL DEFAULT 0,
  sales_tax_jpy REAL NOT NULL DEFAULT 0,

  -- 手数料 7 列
  commission_jpy REAL NOT NULL DEFAULT 0,
  fba_fulfillment_jpy REAL NOT NULL DEFAULT 0,
  fba_storage_jpy REAL NOT NULL DEFAULT 0,
  closing_fee_jpy REAL NOT NULL DEFAULT 0,    -- 実 DB に該当 type なし、常に 0
  shipping_chargeback_jpy REAL NOT NULL DEFAULT 0,
  giftwrap_chargeback_jpy REAL NOT NULL DEFAULT 0,
  promotion_jpy REAL NOT NULL DEFAULT 0,

  -- 補填・返金 5 列
  warehouse_damage_jpy REAL NOT NULL DEFAULT 0,
  warehouse_lost_jpy REAL NOT NULL DEFAULT 0,
  safe_t_jpy REAL NOT NULL DEFAULT 0,
  refund_principal_jpy REAL NOT NULL DEFAULT 0,
  reversal_reimbursement_jpy REAL NOT NULL DEFAULT 0,

  -- その他 3 列 (利益式に入れない、保持のみ)
  misc_fee_jpy REAL NOT NULL DEFAULT 0,
  other_fee_jpy REAL NOT NULL DEFAULT 0,
  other_amount_jpy REAL NOT NULL DEFAULT 0,

  -- 原価関連 4 列 (snapshot 方式)
  unit_cost_snapshot REAL,                    -- 取引日時点の原価 (一度書いたら不変)
  cost_snapshot_date_jst TEXT,                -- snapshot 取得日 (= build 日)
  latest_unit_cost_reference REAL,            -- 現在の原価 (operational simulation 用、別ジョブで更新可)
  cogs_amount REAL NOT NULL DEFAULT 0,        -- = unit_cost_snapshot * units_net_sold

  -- 利益 (= 広告費引く前 + warehouse 補填 = 経営層が頭で見ている利益)
  -- validation 対象: v4.gross_margin_with_reimbursement_excl_tax (PR #61 で v4 に追加した新列)
  -- 注: ad_cost (広告費) は別軸、Phase 1.x で別列 ad_cost_jpy を追加予定 (12 分析の広告効果分解で使用)
  profit_amount REAL NOT NULL DEFAULT 0,

  -- 品質ステータス
  is_cost_complete INTEGER NOT NULL DEFAULT 0,
  cost_status TEXT NOT NULL CHECK (
    cost_status IN ('complete','missing_cost','partial_cost','late_bound_after_close')
  ),

  -- メタ
  source_layer_summary TEXT NOT NULL DEFAULT '',
  source_row_count INTEGER NOT NULL DEFAULT 0,
  built_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (date_jst, seller_sku)
);

CREATE INDEX IF NOT EXISTS idx_f_amazon_finance_sku_daily_v1_sku
  ON f_amazon_finance_sku_daily_v1 (seller_sku);
CREATE INDEX IF NOT EXISTS idx_f_amazon_finance_sku_daily_v1_month
  ON f_amazon_finance_sku_daily_v1 (substr(date_jst, 1, 7));
CREATE INDEX IF NOT EXISTS idx_f_amazon_finance_sku_daily_v1_cost_status
  ON f_amazon_finance_sku_daily_v1 (cost_status);
