-- x_rakuten_item_product_map: 楽天商品コード ↔ NE商品コード マッピング
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2 / §13-6
-- 関連: Codex Round 2 Critical 解消 (商品同定キー不一致)
--
-- 第一キー: rakuten_shop_code + rakuten_item_code (= manageNumber、楽天 RMS items.search 検証済)
-- itemUrl は補助キー (変更されうるため正規キーにしない)
--
-- mapping_type で紐付け状態を吸収:
--   CONFIRMED   - 自動 or 手動で紐付け確定
--   PROVISIONAL - 自動紐付けだが confidence < 1.0、要レビュー
--   UNMAPPED    - 紐付け失敗、要手動 (売上は f_linegift_unmapped_sales_daily 退避)

CREATE TABLE IF NOT EXISTS x_rakuten_item_product_map (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  rakuten_shop_code   TEXT NOT NULL,                                       -- 店舗識別 (RAKUTEN_SHOP_CODE 既定: 'b-faith')
  rakuten_item_code   TEXT NOT NULL,                                       -- manageNumber (楽天 RMS items.search 検証済)
  rakuten_item_url    TEXT,                                                -- 補助キー、検索用
  rakuten_item_name   TEXT,                                                -- 楽天上の表示名 (audit)
  ne_product_code     TEXT NOT NULL,                                       -- m_products.商品コード
  ne_sku_code         TEXT,                                                -- SKU 運用時の枝番対応 (LINEギフトは variation.code に相当)
  mapping_type        TEXT NOT NULL DEFAULT 'CONFIRMED'
                       CHECK (mapping_type IN ('CONFIRMED','PROVISIONAL','UNMAPPED')),
  confidence          REAL NOT NULL DEFAULT 1.0,
  reason              TEXT,                                                -- 手動変更 / 無効化の理由 (API 層で必須化)
  is_active           INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  valid_from          TEXT NOT NULL,
  valid_to            TEXT,
  created_by          TEXT NOT NULL,                                       -- 'system' / username
  updated_by          TEXT NOT NULL,
  synced_at           TEXT NOT NULL,
  UNIQUE (rakuten_shop_code, rakuten_item_code, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_x_rakuten_item_map_active
  ON x_rakuten_item_product_map (rakuten_shop_code, rakuten_item_code)
  WHERE is_active = 1 AND valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_x_rakuten_item_map_ne
  ON x_rakuten_item_product_map (ne_product_code, ne_sku_code);

CREATE INDEX IF NOT EXISTS idx_x_rakuten_item_map_type
  ON x_rakuten_item_product_map (mapping_type, is_active);
