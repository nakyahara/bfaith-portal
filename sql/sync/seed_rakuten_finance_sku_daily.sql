-- ============================================================
-- sync_contracts seed for rakuten_finance_sku_daily v1
-- ============================================================
-- Phase 1a ticket: #R-3
-- 用途: sync-rakuten-finance-daily.js が読む contract を sync_contracts に登録
-- 実行: sqlite3 warehouse.db < sql/sync/seed_rakuten_finance_sku_daily.sql
--
-- 関連:
--   docs reference: config/sync/contracts/rakuten_finance_sku_daily.v1.json
--   sync script: apps/warehouse/sync-rakuten-finance-daily.js

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
  'rakuten_finance_sku_daily',
  1,
  'minipc-warehouse',
  'f_rakuten_finance_sku_daily_v1',
  'mirror_rakuten_finance_sku_daily',
  'one row = one (date_jst, rakuten_code)',
  '["date_jst","rakuten_code"]',
  '{"required":["date_jst","rakuten_code"],"date_jst_pattern":"^\d{4}-\d{2}-\d{2}$","cost_status_enum":["complete","missing_cost","partial_cost","late_bound_after_close"],"sku_resolution_enum":["resolved","unresolved","direct_master"],"shipping_quality_enum":["actual","estimated_rates","estimated_fallback","missing"]}',
  'scope_clear_per_run',
  'insert_or_replace',
  1,
  'phase1a-rakuten-finance',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
);

SELECT 'Seeded contract:' AS msg, entity, contract_version, source_object, target_table
FROM sync_contracts WHERE entity = 'rakuten_finance_sku_daily';
