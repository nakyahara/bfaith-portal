-- ============================================================
-- sync_contracts seed for amazon_finance_sku_daily v1
-- ============================================================
-- Phase 1 ticket: #1-4
-- 用途: sync-amazon-finance-daily.js が読む contract を sync_contracts に登録
-- 実行: sqlite3 warehouse.db < sql/sync/seed_amazon_finance_sku_daily.sql
--
-- 関連:
--   docs reference: config/sync/contracts/amazon_finance_sku_daily.v1.json
--   sync script: apps/warehouse/sync-amazon-finance-daily.js

INSERT OR REPLACE INTO sync_contracts (
  entity,
  contract_version,
  source_system,
  source_object,
  target_table,
  grain_definition,
  key_columns_json,
  payload_schema_json,
  clear_strategy,
  apply_mode,
  enabled,
  owner,
  created_at,
  updated_at
) VALUES (
  'amazon_finance_sku_daily',
  1,
  'minipc-warehouse',
  'f_amazon_finance_sku_daily_v1',
  'mirror_amazon_finance_sku_daily',
  'one row = one (date_jst, seller_sku, asin_norm)',
  '["date_jst","seller_sku","asin_norm"]',
  '{"required":["date_jst","seller_sku"],"date_jst_pattern":"^\d{4}-\d{2}-\d{2}$","cost_status_enum":["complete","missing_cost","partial_cost","late_bound_after_close"]}',
  'scope_clear_per_run',
  'insert_or_replace',
  1,
  'phase1-amazon-finance',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
);

SELECT 'Seeded contract:' AS msg, entity, contract_version, source_object, target_table
FROM sync_contracts WHERE entity = 'amazon_finance_sku_daily';
