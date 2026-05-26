-- raw_rakuten_items_master: 楽天 RMS items.search 結果の生スナップショット
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2
--
-- 目的:
--   items.search で取得した商品マスタ (manageNumber / itemNumber / title / genreId 等) を
--   raw のまま保存。A-5a-2 (m_product_classifications 自動投入) で genreId を読み取って利用。
--
-- 構造:
--   - manageNumber を PK (1商品1行)
--   - 既存 active 行と内容が変わったら旧行を valid_to で閉じて新行 INSERT (SCD2)
--   - source_payload_json で楽天 API の field 追加に備える

CREATE TABLE IF NOT EXISTS raw_rakuten_items_master (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,
  rakuten_shop_code     TEXT NOT NULL,
  manage_number         TEXT NOT NULL,                                    -- 楽天 RMS items.search の manageNumber (検証済)
  item_number           TEXT,                                              -- itemNumber (補助)
  item_url              TEXT,
  title                 TEXT,
  genre_id              TEXT,                                              -- 楽天 genre_id (m_rakuten_genres.genre_id と紐付け)
  is_active             INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  source_payload_json   TEXT,                                              -- 生 JSON (将来の field 追加に備える)
  source_hash           TEXT,                                              -- SHA-256 (manageNumber+title+genreId+is_active)
  valid_from            TEXT NOT NULL,                                     -- JST ISO8601
  valid_to              TEXT,                                              -- JST ISO8601
  synced_at             TEXT NOT NULL,
  UNIQUE (rakuten_shop_code, manage_number, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_raw_rakuten_items_active
  ON raw_rakuten_items_master (rakuten_shop_code, manage_number)
  WHERE is_active = 1 AND valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_raw_rakuten_items_genre
  ON raw_rakuten_items_master (genre_id, is_active);
