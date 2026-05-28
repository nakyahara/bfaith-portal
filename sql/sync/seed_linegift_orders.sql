-- ============================================================
-- sync_contracts seed for linegift_orders v1
-- ============================================================
-- LINEギフト v1.2 (PR-H、2026-05-28、曜日 × 時間帯ヒートマップの基盤)
-- 用途: sync-linegift-orders.js が読む contract を sync_contracts に登録
-- 実行: node 経由で直接 INSERT (sqlite3 CLI なしのため)
--
-- 関連:
--   docs reference: config/sync/contracts/linegift_orders.v1.json
--   sync script: apps/warehouse/sync-linegift-orders.js
--   schema (Render): apps/warehouse-mirror/db.js (mirror_linegift_orders)
--
-- ★ PII 列 (user_name=LINE ID, address_*, delivery_*, sku_name, parent_item_name) は
--   sync 対象外 — sender (sync-linegift-orders.js) で SELECT 列を制限済み。

INSERT OR REPLACE INTO sync_contracts (
  entity, contract_version, source_system, source_object, target_table,
  grain_definition, key_columns_json, payload_schema_json,
  clear_strategy, apply_mode, enabled, owner, created_at, updated_at
) VALUES (
  'linegift_orders', 1, 'minipc-warehouse',
  'raw_linegift_orders', 'mirror_linegift_orders',
  'one row = one order (1注文1商品、order_id 単一 PK、miniPC raw と同じ grain)',
  '["order_id"]',
  '{"required":["order_id","bought_at_jst","bought_date_jst"],"bought_at_jst_pattern":"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}","bought_date_jst_pattern":"^\d{4}-\d{2}-\d{2}$","note":"PII columns (user_name, address_*, delivery_*, sku_name, parent_item_name) are NOT sent — sender filters SELECT. Seconds are required in bought_at_jst (DB CHECK enforces)."}',
  'no_clear', 'insert_or_replace', 1, 'phase1-linegift-orders',
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
);

SELECT 'Seeded contract:' AS msg, entity, contract_version, source_object, target_table
FROM sync_contracts WHERE entity = 'linegift_orders';
