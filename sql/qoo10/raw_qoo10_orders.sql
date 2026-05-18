-- raw_qoo10_orders DDL — Qoo10 Phase 1 A-1
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §6-1
-- migration SQL: sql/qoo10/migrate_legacy_raw_qoo10_orders.sql
--
-- ★ PK = TEXT order_id ('legacy:%' or 'api:%' で名前空間分離)
--   旧スキーマ (order_id INTEGER = packNo) は grain 崩壊 → 廃止
--   新スキーマは orderNo を主キーに (1 年 687 件 packNo 検証で確定)
--
-- 65 列 API レスポンスから 30 列を保持 (PII 非保存、Cart_Discount_* / SettlePrice 等含む)
-- 90日境界 frozen horizon (LINEギフト v0.5 §12 同型)
-- legacy_fields_missing=1 で旧 raw 由来を区別、fact 対象外

CREATE TABLE IF NOT EXISTS raw_qoo10_orders (
  -- ★ PK (Codex R2 Critical #1 + High #2 反映、衝突回避のため TEXT prefix)
  order_id            TEXT NOT NULL PRIMARY KEY CHECK (
    order_id LIKE 'legacy:%:%' OR order_id LIKE 'api:%'
  ),
  source_type         TEXT NOT NULL CHECK (
    source_type IN ('legacy_migration', 'api_v2', 'api_v3')
  ),
  source_order_key    TEXT NOT NULL,                  -- prefix 抜き本体値 (旧 packNo or 新 orderNo)

  -- 注文識別
  pack_no             INTEGER NOT NULL,               -- = packNo (同梱発送パッケージ単位、複数 order_id 同梱可)
  seller_id           TEXT NOT NULL DEFAULT '',       -- = sellerID (= 'b-faith')

  -- ステータス
  shipping_status     TEXT NOT NULL,                  -- = shippingStatus ('Delivered(5)' 等)
  payment_method      TEXT,                            -- = PaymentMethod

  -- 商品 (SKU 主キー = sellerItemCode)
  item_code           TEXT NOT NULL,                  -- = itemCode (Qoo10 内部商品 ID、数字)
  seller_item_code    TEXT NOT NULL,                  -- = sellerItemCode (★ SKU 主キー、master_match 90%+)
  item_title          TEXT NOT NULL DEFAULT '',
  option_info         TEXT NOT NULL DEFAULT '',       -- = option (variant 表示文字列)
  option_code         TEXT NOT NULL DEFAULT '',

  -- 売上 (税込、JPY)
  order_price         REAL NOT NULL DEFAULT 0,        -- = orderPrice (★ 割引前単価)
  order_qty           INTEGER NOT NULL DEFAULT 0,
  discount            REAL NOT NULL DEFAULT 0,        -- = discount (注文単位値引額、メガ割/メガポ含む)
  total               REAL NOT NULL DEFAULT 0,        -- = total (顧客支払額)
  settle_price        REAL NOT NULL DEFAULT 0,        -- = SettlePrice (★ ショップ受取 = 割引前 × 0.9)

  -- 割引内訳 (1 年 687 件で全部 0 だが将来発火に備えて raw 保持)
  seller_discount         REAL NOT NULL DEFAULT 0,    -- = SellerDiscount
  cart_discount_seller    REAL NOT NULL DEFAULT 0,    -- = Cart_Discount_Seller
  cart_discount_qoo10     REAL NOT NULL DEFAULT 0,    -- = Cart_Discount_Qoo10 (P/L 非算入)
  voucher_code            TEXT NOT NULL DEFAULT '',

  -- 送料 (現状 0)
  shipping_rate       REAL NOT NULL DEFAULT 0,
  shipping_rate_type  TEXT NOT NULL DEFAULT '',
  shipping_way        TEXT NOT NULL DEFAULT '',
  delivery_company    TEXT NOT NULL DEFAULT '',
  tracking_no         TEXT NOT NULL DEFAULT '',

  -- タイムスタンプ (recognized_on = order_date)
  order_date          TEXT NOT NULL,                  -- = orderDate ('YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD HH:MM' 形式)
  payment_date        TEXT,
  shipping_date       TEXT,
  delivered_date      TEXT,

  -- 海外/COD (SettlePrice 公式 scope 判別用)
  oversea_consignment TEXT NOT NULL DEFAULT 'N',     -- = OverseaConsignment ('N' / 'Y')
  cod_price           REAL NOT NULL DEFAULT 0,       -- = cod_price (代引手数料、通常 0)

  -- audit 列
  related_order       TEXT NOT NULL DEFAULT '',      -- = RelatedOrder
  branch_name         TEXT NOT NULL DEFAULT '',

  -- 90日境界凍結管理 (LINEギフト 同型)
  first_seen_at         TEXT NOT NULL,
  last_seen_at          TEXT NOT NULL,
  last_api_snapshot_at  TEXT NOT NULL,
  is_frozen_after_horizon INTEGER NOT NULL DEFAULT 0,

  -- migration audit (旧 raw 由来は legacy_fields_missing=1、fact 対象外)
  legacy_fields_missing  INTEGER NOT NULL DEFAULT 0,

  -- メタ
  synced_at           TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_qoo10_order_date ON raw_qoo10_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_qoo10_status     ON raw_qoo10_orders(shipping_status);
CREATE INDEX IF NOT EXISTS idx_qoo10_sku        ON raw_qoo10_orders(seller_item_code);
CREATE INDEX IF NOT EXISTS idx_qoo10_pack       ON raw_qoo10_orders(pack_no);
CREATE INDEX IF NOT EXISTS idx_qoo10_frozen     ON raw_qoo10_orders(is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1;
CREATE INDEX IF NOT EXISTS idx_qoo10_legacy     ON raw_qoo10_orders(legacy_fields_missing) WHERE legacy_fields_missing = 1;
CREATE INDEX IF NOT EXISTS idx_qoo10_source     ON raw_qoo10_orders(source_type);
