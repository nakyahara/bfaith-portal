-- Phase E-11-b: 楽天 genre → Yahoo カテゴリ自動マッピング学習辞書。
--
-- 設計:
--   既存 Yahoo 出品 130 件 から逆引き学習:
--     1. yahoo_registered_items (yahoo_category_id, yahoo_path 入り)
--   × 2. migration_candidates / yahoo の 楽天 manage_number 突合 → 楽天 candidate の rakuten_genre_id
--     3. 集計: (rakuten_genre_id) → (yahoo_category_id, yahoo_path) の頻度
--     4. 各 genre で 最頻 mapping を 「推定値」 として保持
--
-- E-11-c で publish-pipeline がこれを参照する。
CREATE TABLE genre_yahoo_category_mapping (
  rakuten_genre_id       TEXT NOT NULL,
  yahoo_category_id      INTEGER NOT NULL,
  yahoo_path             TEXT NOT NULL,
  sample_count           INTEGER NOT NULL DEFAULT 1,       -- この (genre, category, path) で観測された件数
  is_primary             INTEGER NOT NULL DEFAULT 0,       -- この genre の最頻マッピング (各 genre で 1 行だけ 1)
  first_learned_at       TEXT NOT NULL,
  last_learned_at        TEXT NOT NULL,
  PRIMARY KEY (rakuten_genre_id, yahoo_category_id, yahoo_path)
);
CREATE INDEX idx_genre_yahoo_category_primary
  ON genre_yahoo_category_mapping(rakuten_genre_id, is_primary DESC, sample_count DESC);
