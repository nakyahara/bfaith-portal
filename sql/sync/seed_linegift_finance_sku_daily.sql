-- ============================================================
-- sync_contracts seed for linegift_finance_sku_daily v1
-- ============================================================
-- LINEギフト Phase 1 ticket: A-3
-- 用途: sync-linegift-finance-daily.js が読む contract を sync_contracts に登録
-- 実行: node 経由で直接 INSERT (sqlite3 CLI なしのため)
--
-- 関連:
--   docs reference: config/sync/contracts/linegift_finance_sku_daily.v1.json
--   sync script: apps/warehouse/sync-linegift-finance-daily.js
--   au PAY 同等: sql/sync/seed_aupay_finance_sku_daily.sql

INSERT OR REPLACE INTO sync_contracts (
  entity, contract_version, source_system, source_object, target_table,
  grain_definition, key_columns_json, payload_schema_json,
  clear_strategy, apply_mode, enabled, owner, created_at, updated_at
) VALUES (
  'linegift_finance_sku_daily', 1, 'minipc-warehouse',
  'f_linegift_finance_sku_daily_v1', 'mirror_linegift_finance_sku_daily',
  'one row = one (date_jst, sku_code) — sku_code = variation.code (LOWER(TRIM())、LINEギフト の variant 主キー、100% master_match 想定)',
  '["date_jst","sku_code"]',
  '{"required":["date_jst","sku_code"],"date_jst_pattern":"^\d{4}-\d{2}-\d{2}$","cost_status_enum":["complete","missing_cost","partial_cost","late_bound_after_close"],"resolution_method_enum":["master_match","parent_match","unresolved"],"shipping_quality_enum":["no_shipping_in_api","actual_api","estimated_rates","estimated_fallback","missing"],"margin_confidence_enum":["provisional_full_candidate","full_minus_returns","full"],"mall_fee_calc_method_enum":["actual_api","actual_statement","estimated_rate","unknown"]}',
  'scope_clear_per_run', 'insert_or_replace', 1, 'phase1-linegift-finance',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
);

SELECT 'Seeded contract:' AS msg, entity, contract_version, source_object, target_table
FROM sync_contracts WHERE entity = 'linegift_finance_sku_daily';
