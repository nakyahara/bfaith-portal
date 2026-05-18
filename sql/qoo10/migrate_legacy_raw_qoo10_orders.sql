-- migrate_legacy_raw_qoo10_orders.sql — Qoo10 Phase 1 A-1 (reference 用)
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §6-1.1
-- 旧 raw_qoo10_orders (11 cols, order_id = packNo as TEXT, id = auto INTEGER PK)
--   ↓ 新 raw_qoo10_orders (30+ cols, order_id = 'legacy:<packNo>:<id>' TEXT PK)
--
-- ★★ 実運用では apps/warehouse/migrate-qoo10-raw.js を使用 ★★
--    本 SQL は ALTER TABLE RENAME を無条件実行するため再実行で衝突する (Codex R1 Medium #1)。
--    Node script 版では PRAGMA で旧/新スキーマを判定して idempotent 化されている。
--    本 SQL は設計書 §6-1.1 のロジックを SQL で表現した reference であり、運用には Node 版を使うこと。
--
-- ★ 必ず 1 transaction で実行 (途中失敗時 ROLLBACK 完全戻し)
-- ★ 実行前に warehouse.db backup 必須 (warehouse.db.bak_pre_qoo10_a1_<yyyymmdd>)
-- ★ legacy_fields_missing=1 で旧 raw 由来を区別、fact 対象外
-- ★ is_frozen_after_horizon=1 (旧データは全て 90 日 horizon 外として保存)
--
-- 旧 11 列 → 新マッピング:
--   id (auto INT)     → order_id suffix
--   order_id (=pack)  → pack_no + source_order_key + order_id prefix の本体
--   order_date        → order_date
--   order_status      → shipping_status
--   item_code         → seller_item_code (旧データは sellerItemCode 相当として扱う)
--   item_name         → item_title
--   quantity          → order_qty
--   unit_price        → order_price
--   total_price       → total + settle_price (近似、再計算不可)
--   option_info       → option_info
--   synced_at         → first_seen_at + last_seen_at + last_api_snapshot_at + synced_at

BEGIN TRANSACTION;

-- Step 1: 新 DDL 作成 (CREATE TABLE IF NOT EXISTS は raw_qoo10_orders.sql 側で実行済み前提)
--         ここでは旧表が残っている前提で rename → backfill → drop の手順

-- 旧表を退避 (rollback 用)
ALTER TABLE raw_qoo10_orders RENAME TO raw_qoo10_orders_legacy_v0;

-- Step 2: 新 DDL を作成 (raw_qoo10_orders.sql の DDL をここに inline、idempotent 化)
CREATE TABLE IF NOT EXISTS raw_qoo10_orders (
  order_id            TEXT NOT NULL PRIMARY KEY CHECK (
    order_id LIKE 'legacy:%:%' OR order_id LIKE 'api:%'
  ),
  source_type         TEXT NOT NULL CHECK (
    source_type IN ('legacy_migration', 'api_v2', 'api_v3')
  ),
  source_order_key    TEXT NOT NULL,
  pack_no             INTEGER NOT NULL,
  seller_id           TEXT NOT NULL DEFAULT '',
  shipping_status     TEXT NOT NULL,
  payment_method      TEXT,
  item_code           TEXT NOT NULL,
  seller_item_code    TEXT NOT NULL,
  item_title          TEXT NOT NULL DEFAULT '',
  option_info         TEXT NOT NULL DEFAULT '',
  option_code         TEXT NOT NULL DEFAULT '',
  order_price         REAL NOT NULL DEFAULT 0,
  order_qty           INTEGER NOT NULL DEFAULT 0,
  discount            REAL NOT NULL DEFAULT 0,
  total               REAL NOT NULL DEFAULT 0,
  settle_price        REAL NOT NULL DEFAULT 0,
  seller_discount         REAL NOT NULL DEFAULT 0,
  cart_discount_seller    REAL NOT NULL DEFAULT 0,
  cart_discount_qoo10     REAL NOT NULL DEFAULT 0,
  voucher_code            TEXT NOT NULL DEFAULT '',
  shipping_rate       REAL NOT NULL DEFAULT 0,
  shipping_rate_type  TEXT NOT NULL DEFAULT '',
  shipping_way        TEXT NOT NULL DEFAULT '',
  delivery_company    TEXT NOT NULL DEFAULT '',
  tracking_no         TEXT NOT NULL DEFAULT '',
  order_date          TEXT NOT NULL,
  payment_date        TEXT,
  shipping_date       TEXT,
  delivered_date      TEXT,
  oversea_consignment TEXT NOT NULL DEFAULT 'N',
  cod_price           REAL NOT NULL DEFAULT 0,
  related_order       TEXT NOT NULL DEFAULT '',
  branch_name         TEXT NOT NULL DEFAULT '',
  first_seen_at         TEXT NOT NULL,
  last_seen_at          TEXT NOT NULL,
  last_api_snapshot_at  TEXT NOT NULL,
  is_frozen_after_horizon INTEGER NOT NULL DEFAULT 0,
  legacy_fields_missing  INTEGER NOT NULL DEFAULT 0,
  synced_at           TEXT NOT NULL
);

-- Step 3: 旧データを backfill
--   order_id = 'legacy:<packNo>:<id>' で衝突回避
--   pack_no は旧 order_id (= packNo TEXT) を INTEGER 変換 (失敗時 0、ただし旧データは全て数値)
--   item_code / seller_item_code は同値で埋める (旧は区別なし、新は seller_item_code が SKU 主キー)
--   shipping_status は旧 order_status をそのまま (例: 'Seller confirm(3)' / 'Delivered(5)')
--   total_price は顧客支払額に近いが原データ精度低、settle_price は再計算不可なので total と同値
--   first_seen_at = last_seen_at = last_api_snapshot_at = synced_at (旧データは初回 = 最終 と扱う)
--   is_frozen_after_horizon = 1 (旧は全て horizon 外として凍結)
--   legacy_fields_missing = 1 (fact 対象外マーカー)
INSERT INTO raw_qoo10_orders (
  order_id,
  source_type,
  source_order_key,
  pack_no,
  seller_id,
  shipping_status,
  payment_method,
  item_code,
  seller_item_code,
  item_title,
  option_info,
  option_code,
  order_price,
  order_qty,
  discount,
  total,
  settle_price,
  seller_discount,
  cart_discount_seller,
  cart_discount_qoo10,
  voucher_code,
  shipping_rate,
  shipping_rate_type,
  shipping_way,
  delivery_company,
  tracking_no,
  order_date,
  payment_date,
  shipping_date,
  delivered_date,
  oversea_consignment,
  cod_price,
  related_order,
  branch_name,
  first_seen_at,
  last_seen_at,
  last_api_snapshot_at,
  is_frozen_after_horizon,
  legacy_fields_missing,
  synced_at
)
SELECT
  'legacy:' || COALESCE(order_id, '0') || ':' || CAST(id AS TEXT) AS order_id,
  'legacy_migration'                                              AS source_type,
  COALESCE(order_id, '0')                                          AS source_order_key,
  CAST(COALESCE(order_id, '0') AS INTEGER)                         AS pack_no,
  ''                                                                AS seller_id,
  COALESCE(order_status, 'Unknown')                                AS shipping_status,
  NULL                                                              AS payment_method,
  COALESCE(item_code, '')                                          AS item_code,
  COALESCE(item_code, '')                                          AS seller_item_code,
  COALESCE(item_name, '')                                          AS item_title,
  COALESCE(option_info, '')                                        AS option_info,
  ''                                                                AS option_code,
  COALESCE(unit_price, 0)                                          AS order_price,
  COALESCE(quantity, 0)                                            AS order_qty,
  0                                                                 AS discount,
  COALESCE(total_price, 0)                                         AS total,
  COALESCE(total_price, 0)                                         AS settle_price,
  0                                                                 AS seller_discount,
  0                                                                 AS cart_discount_seller,
  0                                                                 AS cart_discount_qoo10,
  ''                                                                AS voucher_code,
  0                                                                 AS shipping_rate,
  ''                                                                AS shipping_rate_type,
  ''                                                                AS shipping_way,
  ''                                                                AS delivery_company,
  ''                                                                AS tracking_no,
  COALESCE(order_date, '1970-01-01 00:00:00')                      AS order_date,
  NULL                                                              AS payment_date,
  NULL                                                              AS shipping_date,
  NULL                                                              AS delivered_date,
  'N'                                                               AS oversea_consignment,
  0                                                                 AS cod_price,
  ''                                                                AS related_order,
  ''                                                                AS branch_name,
  COALESCE(synced_at, '1970-01-01 00:00:00')                       AS first_seen_at,
  COALESCE(synced_at, '1970-01-01 00:00:00')                       AS last_seen_at,
  COALESCE(synced_at, '1970-01-01 00:00:00')                       AS last_api_snapshot_at,
  1                                                                 AS is_frozen_after_horizon,
  1                                                                 AS legacy_fields_missing,
  COALESCE(synced_at, '1970-01-01 00:00:00')                       AS synced_at
FROM raw_qoo10_orders_legacy_v0;

-- Step 4: index 再作成
CREATE INDEX IF NOT EXISTS idx_qoo10_order_date ON raw_qoo10_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_qoo10_status     ON raw_qoo10_orders(shipping_status);
CREATE INDEX IF NOT EXISTS idx_qoo10_sku        ON raw_qoo10_orders(seller_item_code);
CREATE INDEX IF NOT EXISTS idx_qoo10_pack       ON raw_qoo10_orders(pack_no);
CREATE INDEX IF NOT EXISTS idx_qoo10_frozen     ON raw_qoo10_orders(is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1;
CREATE INDEX IF NOT EXISTS idx_qoo10_legacy     ON raw_qoo10_orders(legacy_fields_missing) WHERE legacy_fields_missing = 1;
CREATE INDEX IF NOT EXISTS idx_qoo10_source     ON raw_qoo10_orders(source_type);

-- Step 5: 旧表 drop (rollback 不可、必ず backup 確認後)
--   ★ コメントアウトのまま commit、smoke 確認後 別 transaction で DROP
-- DROP TABLE raw_qoo10_orders_legacy_v0;

COMMIT;

-- 確認クエリ (smoke 用、本 SQL では実行しない):
-- SELECT COUNT(*) FROM raw_qoo10_orders WHERE legacy_fields_missing = 1;  -- 期待: 17252
-- SELECT COUNT(*) FROM raw_qoo10_orders_legacy_v0;                         -- 期待: 17252
-- SELECT COUNT(*) FROM raw_qoo10_orders WHERE order_id NOT LIKE 'legacy:%';-- 期待: 0
-- SELECT MIN(order_date), MAX(order_date) FROM raw_qoo10_orders WHERE legacy_fields_missing = 1;
