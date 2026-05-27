-- mart_seed_dimensions.sql — LINEギフト Phase 1 v1.0 Commit 1
--   Render 派生ディメンション (mart_ prefix で sync_contracts 未登録 = mirror sync 除外)
--
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §3 / §4
-- Codex v1.0 議論: Round 2 Critical 1 (mirror sync 除外契約) 反映
--
-- 対象テーブル:
--   1. mart_price_bands              価格帯マスタ (8段階、Looker 既存と同一)
--   2. mart_gift_seasons             ギフトシーン定義 (14 seed)
--   3. mart_gift_season_occurrences  シーンの年別実体 (generate-gift-season-occurrences.js で展開)
--
-- 全テーブル mart_ prefix で「Render 派生、mirror sync 対象外」を明示。

-- ─────────────────────────────────────────────────────────
-- mart_price_bands
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_price_bands (
  band_code            TEXT PRIMARY KEY,
  band_label           TEXT NOT NULL,
  min_price_jpy_incl   INTEGER NOT NULL,                           -- 包含 (>=)
  max_price_jpy_incl   INTEGER,                                    -- 包含 (<=)、NULL = 上限なし
  sort_order           INTEGER NOT NULL,
  is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  valid_from           TEXT NOT NULL,
  valid_to             TEXT,
  synced_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mart_price_bands_active
  ON mart_price_bands (is_active, sort_order);

INSERT OR IGNORE INTO mart_price_bands (band_code, band_label, min_price_jpy_incl, max_price_jpy_incl, sort_order, valid_from, synced_at) VALUES
  ('B0_499',    '0〜499円',           0,    499,  1, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('B500_999',  '500〜999円',         500,  999,  2, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('B1000_1999','1,000〜1,999円',     1000, 1999, 3, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('B2000_2999','2,000〜2,999円',     2000, 2999, 4, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('B3000_3999','3,000〜3,999円',     3000, 3999, 5, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('B4000_4999','4,000〜4,999円',     4000, 4999, 6, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('B5000_5999','5,000〜5,999円',     5000, 5999, 7, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('B6000_PLUS','6,000円以上',        6000, NULL, 8, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00');

-- ─────────────────────────────────────────────────────────
-- mart_gift_seasons (14 シーン、母の日/クリスマス等)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_gift_seasons (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  season_code          TEXT NOT NULL UNIQUE,
  season_name          TEXT NOT NULL,
  rule_type            TEXT NOT NULL CHECK (rule_type IN ('FIXED_RANGE','NTH_WEEKDAY','CUSTOM')),
  rule_json            TEXT NOT NULL,
  priority             INTEGER NOT NULL DEFAULT 100,
  allow_overlap        INTEGER NOT NULL DEFAULT 1 CHECK (allow_overlap IN (0,1)),
  is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  valid_from           TEXT NOT NULL,
  valid_to             TEXT,
  created_by           TEXT NOT NULL DEFAULT 'system',
  updated_by           TEXT NOT NULL DEFAULT 'system',
  synced_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mart_gift_seasons_active
  ON mart_gift_seasons (is_active, priority);

INSERT OR IGNORE INTO mart_gift_seasons (season_code, season_name, rule_type, rule_json, priority, valid_from, synced_at) VALUES
  ('NEW_YEAR_CARD',  'お正月 (年賀ギフト)', 'CUSTOM',      '{"start_mmdd":"12-26","end_mmdd":"01-03","year_anchor":"end"}', 80, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('COMING_OF_AGE',  '成人の日',            'NTH_WEEKDAY', '{"month":1,"nth":2,"weekday":1,"pre_days":14,"post_days":0}',   90, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('VALENTINE',      'バレンタイン',        'FIXED_RANGE', '{"start_mmdd":"02-01","end_mmdd":"02-14"}',                     70, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('WHITE_DAY',      'ホワイトデー',        'FIXED_RANGE', '{"start_mmdd":"03-01","end_mmdd":"03-14"}',                     70, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('GRADUATION',     '卒業祝い',            'FIXED_RANGE', '{"start_mmdd":"02-15","end_mmdd":"03-31"}',                     60, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('ENTRANCE',       '入学・入園',          'FIXED_RANGE', '{"start_mmdd":"03-01","end_mmdd":"04-10"}',                     60, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('CHILDRENS_DAY',  'こどもの日',          'FIXED_RANGE', '{"start_mmdd":"04-20","end_mmdd":"05-05"}',                     65, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('MOTHERS_DAY',    '母の日',              'NTH_WEEKDAY', '{"month":5,"nth":2,"weekday":0,"pre_days":14,"post_days":0}',   95, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('FATHERS_DAY',    '父の日',              'NTH_WEEKDAY', '{"month":6,"nth":3,"weekday":0,"pre_days":14,"post_days":0}',   90, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('TANABATA',       '七夕',                'FIXED_RANGE', '{"start_mmdd":"06-25","end_mmdd":"07-07"}',                     50, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('OBON',           'お盆',                'FIXED_RANGE', '{"start_mmdd":"08-01","end_mmdd":"08-16"}',                     60, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('RESPECT_AGED',   '敬老の日',            'NTH_WEEKDAY', '{"month":9,"nth":3,"weekday":1,"pre_days":14,"post_days":0}',   85, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('HALLOWEEN',      'ハロウィン',          'FIXED_RANGE', '{"start_mmdd":"10-15","end_mmdd":"10-31"}',                     60, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00'),
  ('CHRISTMAS',      'クリスマス',          'FIXED_RANGE', '{"start_mmdd":"12-01","end_mmdd":"12-25"}',                     95, '2026-05-27T00:00:00+09:00', '2026-05-27T00:00:00+09:00');

-- ─────────────────────────────────────────────────────────
-- mart_gift_season_occurrences (年別実体)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_gift_season_occurrences (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  season_code       TEXT NOT NULL,
  season_year       INTEGER NOT NULL,
  start_date_jst    TEXT NOT NULL CHECK (start_date_jst GLOB '????-??-??'),
  end_date_jst      TEXT NOT NULL CHECK (end_date_jst GLOB '????-??-??'),
  anchor_date_jst   TEXT,
  is_active         INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  created_by        TEXT NOT NULL DEFAULT 'system',
  updated_by        TEXT NOT NULL DEFAULT 'system',
  synced_at         TEXT NOT NULL,
  UNIQUE (season_code, season_year),
  FOREIGN KEY (season_code) REFERENCES mart_gift_seasons(season_code)
);

CREATE INDEX IF NOT EXISTS idx_mart_gift_occ_date
  ON mart_gift_season_occurrences (start_date_jst, end_date_jst, is_active);

CREATE INDEX IF NOT EXISTS idx_mart_gift_occ_season
  ON mart_gift_season_occurrences (season_code, season_year);
