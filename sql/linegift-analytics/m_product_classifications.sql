-- m_product_classifications: 全モール共通 商品分類マスタ (正本、SCD2)
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2
-- Codex 議論: Round 2-6
--
-- 設計方針:
--   - 楽天 Genre Tree を全モール共通分類 SSoT として流用 (中原さん 2026-05-26 確定)
--   - 自動分類 (RAKUTEN_AUTO): raw_rakuten_items_master.genre_id を読み取って投入
--   - 手動分類 (MANUAL): 楽天で未出品の商品 (LINEギフト/Amazon 専売等) のみ
--   - 価格帯 (price_band_code_master): m_products.標準売価 ベース、SKU sticky (master 系)
--   - SCD2: 変更時は valid_to で旧行を閉じて新行 INSERT
--
-- NOTE: SQLite では UNIQUE 制約内で COALESCE が使えないため、
--       UNIQUE INDEX を IFNULL 式で別途作成する (設計書 v0.9 の UNIQUE 制約定義を実装に合わせ調整)

CREATE TABLE IF NOT EXISTS m_product_classifications (
  id                       INTEGER PRIMARY KEY AUTOINCREMENT,
  ne_product_code          TEXT NOT NULL,                                  -- m_products.商品コード
  ne_sku_code              TEXT,                                            -- SKU 単位管理時のみ
  classification_level     TEXT NOT NULL DEFAULT 'PRODUCT'
                            CHECK (classification_level IN ('PRODUCT','SKU')),
  main_genre_id            TEXT,                                            -- 楽天 genre_id
  main_genre_name          TEXT,                                            -- denormalized cache
  sub_genre_id             TEXT,
  sub_genre_name           TEXT,
  price_band_code_master   TEXT,                                            -- m_price_bands.band_code、運用・表示用 master band
  source_type              TEXT NOT NULL
                            CHECK (source_type IN ('RAKUTEN_AUTO','MANUAL','RULE')),
  source_ref               TEXT,                                            -- rakuten_item_code (manageNumber) 等
  source_hash              TEXT,                                            -- SHA-256 (差分検知)
  is_provisional           INTEGER NOT NULL DEFAULT 0 CHECK (is_provisional IN (0,1)),
  is_classified            INTEGER NOT NULL DEFAULT 0 CHECK (is_classified IN (0,1)),
  is_active                INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  valid_from               TEXT NOT NULL,                                   -- JST ISO8601
  valid_to                 TEXT,                                            -- JST ISO8601
  created_by               TEXT NOT NULL,
  updated_by               TEXT NOT NULL,
  reason                   TEXT,                                            -- 手動変更時必須 (API 層で強制)
  synced_at                TEXT NOT NULL
);

-- ne_sku_code が NULL でも UNIQUE 制約を効かせるため IFNULL を使った UNIQUE INDEX
CREATE UNIQUE INDEX IF NOT EXISTS idx_m_product_classifications_uniq
  ON m_product_classifications (ne_product_code, IFNULL(ne_sku_code, ''), classification_level, valid_from);

CREATE INDEX IF NOT EXISTS idx_m_product_classifications_active
  ON m_product_classifications (ne_product_code, ne_sku_code)
  WHERE is_active = 1 AND valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_m_product_classifications_genre
  ON m_product_classifications (main_genre_id, is_active);

CREATE INDEX IF NOT EXISTS idx_m_product_classifications_band
  ON m_product_classifications (price_band_code_master, is_active);

CREATE INDEX IF NOT EXISTS idx_m_product_classifications_source
  ON m_product_classifications (source_type, is_active);
