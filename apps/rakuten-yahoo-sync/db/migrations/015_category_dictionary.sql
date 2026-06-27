-- Phase E-12 (2026-06-27): ローカル RYS (Downloads/RakutenYahooSync) で 2026-05-20〜26 に
-- 作成済の楽天 genre → Yahoo カテゴリ変換表を bfaith-portal に移植。
--
-- 経緯:
--   Phase E 移植時に bfaith-portal は overlap 自動学習 (genre_yahoo_category_mapping) のみ
--   実装され、 ローカル RYS の category_manual (中原さん人手確定 27 件) や
--   category_decisions (overlap 学習 562 件)、 category_default_path (515 件) は持ち込まれて
--   いなかった。 移行対象 269 SKU のうち 122 件が「Yahoo!カテゴリ未解決」 で blocked。
--
-- 設計 (Codex R1: critical/high なし stop / R2: import script schema fix)
--   - 5 テーブル新設 (category_manual / category_decisions / rakuten_genre /
--     category_default_path / rakuten_genre_stats)
--   - 既存 genre_yahoo_category_mapping は legacy fallback として残す
--   - resolver 参照順: caller > Notion override > category_manual > category_decisions (locked) >
--     legacy_learned (genre_yahoo_category_mapping) > unresolved
--   - conditional/review は import するが auto resolve には使わない (genre_stats.auto_mode で判定)
--
-- 移植元 (ローカル RYS のスキーマ確認結果):
--   category_manual:           genre_id PK, product_category, note
--   category_decisions:        genre_id PK, chosen_product_category, decision_source,
--                              confidence, sample_count, ambiguous, locked
--   genre_stats:               genre_id PK, top1_category, top1_prob, confidence_band, auto_mode
--   category_default_path:     yahoo_category_id PK, yahoo_path

-- 楽天 genre → Yahoo カテゴリ の人手確定値 (genre 単位、 最優先)
CREATE TABLE category_manual (
  rakuten_genre_id    TEXT PRIMARY KEY,
  yahoo_category_id   INTEGER NOT NULL,
  yahoo_path          TEXT,                                  -- nullable (path 別解決可能)
  note                TEXT,                                  -- 中原さんメモ (例: "突っ張り棒・突っ張り棚")
  source              TEXT NOT NULL DEFAULT 'imported_from_local_rys'
                         CHECK(source IN ('imported_from_local_rys', 'dashboard_input')),
  created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 楽天 genre → Yahoo カテゴリ の overlap 学習確定値 (genre 単位)
-- ローカル RYS で 3,484 件 観測から 562 genre 学習済。 chose_product_category が確定値。
-- locked=1 & ambiguous=0 のみ自動採用、 conditional/review は保存のみ (resolver は skip)
CREATE TABLE category_decisions (
  rakuten_genre_id     TEXT PRIMARY KEY,
  yahoo_category_id    INTEGER NOT NULL,
  yahoo_path           TEXT,                                  -- nullable (default_path から補完)
  decision_source      TEXT NOT NULL DEFAULT 'learned'
                          CHECK(decision_source IN ('learned', 'ai')),
  confidence           REAL,                                  -- 0..1
  sample_count         INTEGER NOT NULL DEFAULT 0 CHECK(sample_count >= 0),
  ambiguous            INTEGER NOT NULL DEFAULT 0 CHECK(ambiguous IN (0, 1)),
  locked               INTEGER NOT NULL DEFAULT 0 CHECK(locked IN (0, 1)),
  source               TEXT NOT NULL DEFAULT 'imported_from_local_rys',
  imported_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 楽天 genre の名前 + 階層パス (UI 表示用、 LIKE 検索用)
-- ローカル RYS の rakuten_genre 562 件
CREATE TABLE rakuten_genre (
  genre_id            TEXT PRIMARY KEY,
  name_ja             TEXT NOT NULL,                          -- "潤滑油・サビ止めオイル"
  path_ja             TEXT,                                   -- "花・ガーデン・DIY > DIY・工具 > 接着・補修用品 > 潤滑油・サビ止めオイル"
  level               INTEGER,
  source              TEXT NOT NULL DEFAULT 'imported_from_local_rys',
  imported_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Yahoo product_category → yahoo_path のデフォルト辞書
-- ローカル RYS の category_default_path 515 件 (店舗 b-faith01 の売り場割当)
-- manual / decisions の yahoo_path NULL を補完するための fallback
CREATE TABLE category_default_path (
  yahoo_category_id   INTEGER PRIMARY KEY,
  yahoo_path          TEXT NOT NULL,
  source              TEXT NOT NULL DEFAULT 'imported_from_local_rys',
  imported_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 楽天 genre の自動採用判定 (band/auto_mode)
-- locked  = strong band で 1:1 = 自動採用
-- conditional = 主占有 ≥85% 等で要条件付き = 保存のみ、 resolver は skip
-- review = ambiguous 等 = 保存のみ、 resolver は skip
CREATE TABLE rakuten_genre_stats (
  genre_id              TEXT PRIMARY KEY,
  top1_category         INTEGER,
  top1_prob             REAL,
  confidence_band       TEXT CHECK(confidence_band IS NULL OR confidence_band IN ('strong', 'single', 'ambiguous', 'low')),
  auto_mode             TEXT CHECK(auto_mode IS NULL OR auto_mode IN ('locked', 'conditional', 'review')),
  sample_count          INTEGER NOT NULL DEFAULT 0,
  source                TEXT NOT NULL DEFAULT 'imported_from_local_rys',
  imported_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_category_manual_yahoo
  ON category_manual(yahoo_category_id);
CREATE INDEX idx_category_decisions_yahoo
  ON category_decisions(yahoo_category_id);
CREATE INDEX idx_category_decisions_auto
  ON category_decisions(locked, ambiguous);
CREATE INDEX idx_rakuten_genre_name
  ON rakuten_genre(name_ja);
