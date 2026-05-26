-- m_rakuten_genres: 楽天市場 Genre Tree マスタ (スナップショット保持)
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2
-- 関連: Codex 商品分析 Round 1-6 議論結果
--
-- 目的:
--   全モール共通の商品ジャンル分類 SSoT を楽天 Genre Tree から構築する。
--   楽天 RakutenGenreSearch API (アプリID認証、RAKUTEN_APP_ID) で取得し、ここに保存。
--
-- 履歴管理:
--   tree_version 列で取得バッチ単位を識別。楽天が genre tree を再編した場合は
--   旧バージョンを valid_to で論理失効し、新バージョンを追加する SCD2 運用。

CREATE TABLE IF NOT EXISTS m_rakuten_genres (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  genre_id            TEXT NOT NULL,
  genre_name          TEXT NOT NULL,
  genre_path          TEXT,                                                 -- "食品 > スイーツ > ..." 表示用
  parent_genre_id     TEXT,
  level_no            INTEGER,                                              -- root=1
  is_leaf             INTEGER NOT NULL DEFAULT 0 CHECK (is_leaf IN (0,1)),
  tree_version        TEXT,                                                 -- 取得バッチ単位 ('YYYYMMDD' 等)
  is_active           INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  source_payload_json TEXT,                                                 -- raw JSON (将来の field 追加に備える)
  source_hash         TEXT,                                                 -- Node.js で SHA-256 (genre_id + name + is_active)
  valid_from          TEXT NOT NULL,                                        -- JST ISO8601
  valid_to            TEXT,                                                 -- JST ISO8601 (NULL = active)
  synced_at           TEXT NOT NULL,                                        -- JST ISO8601
  UNIQUE (genre_id, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_m_rakuten_genres_active
  ON m_rakuten_genres (genre_id)
  WHERE is_active = 1 AND valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_m_rakuten_genres_parent
  ON m_rakuten_genres (parent_genre_id);

CREATE INDEX IF NOT EXISTS idx_m_rakuten_genres_level
  ON m_rakuten_genres (level_no, is_active);
