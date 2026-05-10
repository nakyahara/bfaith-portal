-- ============================================================
-- sync_contracts seed for yahoo_finance_sku_daily v1
-- ============================================================
-- Yahoo Phase 1 ticket: Y-3a
-- 用途: sync-yahoo-finance-daily.js が読む contract を sync_contracts に登録
-- 実行: node 経由で直接 INSERT (sqlite3 CLI なしのため)
--
-- 関連:
--   docs reference: config/sync/contracts/yahoo_finance_sku_daily.v1.json
--   sync script: apps/warehouse/sync-yahoo-finance-daily.js
--   楽天同等: sql/sync/seed_rakuten_finance_sku_daily.sql

INSERT OR REPLACE INTO sync_contracts (
  entity, contract_version, source_system, source_object, target_table,
  grain_definition, key_columns_json, payload_schema_json,
  clear_strategy, apply_mode, enabled, owner, created_at, updated_at
) VALUES (
  'yahoo_finance_sku_daily', 1, 'minipc-warehouse',
  'f_yahoo_finance_sku_daily_v1', 'mirror_yahoo_finance_sku_daily',
  'one row = one (date_jst, yahoo_sku_key)',
  '["date_jst","yahoo_sku_key"]',
  '{"required":["date_jst","yahoo_sku_key"],"date_jst_pattern":"^\d{4}-\d{2}-\d{2}$","cost_status_enum":["complete","missing_cost","partial_cost","late_bound_after_close"],"resolution_method_enum":["sub_match","parent_match","manual_map","unresolved"],"shipping_quality_enum":["actual","estimated_rates","estimated_fallback","missing"],"margin_confidence_enum":["partial","full"],"mall_fee_calc_method_enum":["estimated_10pct","actual_statement"]}',
  'scope_clear_per_run', 'insert_or_replace', 1, 'phase1-yahoo-finance',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
);

SELECT 'Seeded contract:' AS msg, entity, contract_version, source_object, target_table
FROM sync_contracts WHERE entity = 'yahoo_finance_sku_daily';
