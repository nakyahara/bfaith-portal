-- Phase E-11-a: Yahoo!ショッピングカテゴリ自動推定の基盤。
-- 楽天 genre → Yahoo カテゴリ自動マッピングのためのデータ蓄積列を追加する (まだ使わない)。
--
-- 設計:
--   E-11-b で 学習辞書 (genre_yahoo_category_mapping) を作る、 その時に列を埋める。
--   E-11-c で publish-pipeline / readiness-check が参照する。
--
-- 楽天側:
--   migration_candidates に rakuten_genre_id / rakuten_genre_path を追加。
--   楽天 RMS getItem で取得 (既存 fetchItemDetailsBulk の response に含まれる)。
--
-- Yahoo 側:
--   yahoo_registered_items に yahoo_category_id / yahoo_path / last_detail_synced_at を追加。
--   Yahoo getItemDetail で取得 (E-11-b で vps-proxy endpoint 追加予定)。

ALTER TABLE migration_candidates ADD COLUMN rakuten_genre_id TEXT;
ALTER TABLE migration_candidates ADD COLUMN rakuten_genre_path TEXT;

ALTER TABLE yahoo_registered_items ADD COLUMN yahoo_category_id INTEGER;
ALTER TABLE yahoo_registered_items ADD COLUMN yahoo_path TEXT;
ALTER TABLE yahoo_registered_items ADD COLUMN last_detail_synced_at TEXT;
