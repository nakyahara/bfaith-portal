-- m_gift_seasons: ギフトシーン定義マスタ
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-3
-- Codex 議論: Round 2 Critical (可変祝日の年別事前展開)
--
-- シーン期間集計方式 (中原さん 2026-05-26 確定):
--   シーンを商品属性として付与するのではなく、「期間でフィルタする」アプローチ。
--   1 商品が複数シーンで売れるケースを取りこぼさない。
--
-- rule_json で算出ルールを表現:
--   FIXED_RANGE  : {"start_mmdd":"12-01","end_mmdd":"12-25"} (年内範囲)
--   NTH_WEEKDAY  : {"month":5, "nth":2, "weekday":0, "pre_days":14, "post_days":0} (例: 母の日)
--   CUSTOM       : 年跨ぎ等の特殊ケース (year-aware)
--
-- 重複ポリシー:
--   allow_overlap=1 (既定): 母の日とホワイトデー等の重複は両方カウント
--   priority は exclusive attribution モード用 (将来追加)

CREATE TABLE IF NOT EXISTS m_gift_seasons (
  id                   INTEGER PRIMARY KEY AUTOINCREMENT,
  season_code          TEXT NOT NULL UNIQUE,                            -- MOTHERS_DAY, CHRISTMAS, ...
  season_name          TEXT NOT NULL,
  rule_type            TEXT NOT NULL CHECK (rule_type IN ('FIXED_RANGE','NTH_WEEKDAY','CUSTOM')),
  rule_json            TEXT NOT NULL,
  priority             INTEGER NOT NULL DEFAULT 100,                    -- exclusive attribution 用
  allow_overlap        INTEGER NOT NULL DEFAULT 1 CHECK (allow_overlap IN (0,1)),
  is_active            INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  valid_from           TEXT NOT NULL,
  valid_to             TEXT,
  created_by           TEXT NOT NULL DEFAULT 'system',
  updated_by           TEXT NOT NULL DEFAULT 'system',
  synced_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_m_gift_seasons_active
  ON m_gift_seasons (is_active, priority);

-- 初期 seed (Phase 1 デフォルト 15 シーン)
-- 全て allow_overlap=1、母の日等は anchor 前 14 日 + 当日まで (中原さん要望「母の日に売れたもの」)
INSERT OR IGNORE INTO m_gift_seasons (season_code, season_name, rule_type, rule_json, priority, valid_from, synced_at) VALUES
  ('NEW_YEAR_CARD',  'お正月 (年賀ギフト)',   'CUSTOM',       '{"start_mmdd":"12-26","end_mmdd":"01-03","year_anchor":"end"}', 80,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('COMING_OF_AGE',  '成人の日',              'NTH_WEEKDAY',  '{"month":1,"nth":2,"weekday":1,"pre_days":14,"post_days":0}',    90,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('VALENTINE',      'バレンタイン',          'FIXED_RANGE',  '{"start_mmdd":"02-01","end_mmdd":"02-14"}',                       70,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('WHITE_DAY',      'ホワイトデー',          'FIXED_RANGE',  '{"start_mmdd":"03-01","end_mmdd":"03-14"}',                       70,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('GRADUATION',     '卒業祝い',              'FIXED_RANGE',  '{"start_mmdd":"02-15","end_mmdd":"03-31"}',                       60,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('ENTRANCE',       '入学・入園',            'FIXED_RANGE',  '{"start_mmdd":"03-01","end_mmdd":"04-10"}',                       60,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('CHILDRENS_DAY',  'こどもの日',            'FIXED_RANGE',  '{"start_mmdd":"04-20","end_mmdd":"05-05"}',                       65,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('MOTHERS_DAY',    '母の日',                'NTH_WEEKDAY',  '{"month":5,"nth":2,"weekday":0,"pre_days":14,"post_days":0}',     95,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('FATHERS_DAY',    '父の日',                'NTH_WEEKDAY',  '{"month":6,"nth":3,"weekday":0,"pre_days":14,"post_days":0}',     90,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('TANABATA',       '七夕',                  'FIXED_RANGE',  '{"start_mmdd":"06-25","end_mmdd":"07-07"}',                       50,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('OBON',           'お盆',                  'FIXED_RANGE',  '{"start_mmdd":"08-01","end_mmdd":"08-16"}',                       60,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('RESPECT_AGED',   '敬老の日',              'NTH_WEEKDAY',  '{"month":9,"nth":3,"weekday":1,"pre_days":14,"post_days":0}',     85,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('HALLOWEEN',      'ハロウィン',            'FIXED_RANGE',  '{"start_mmdd":"10-15","end_mmdd":"10-31"}',                       60,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00'),
  ('CHRISTMAS',      'クリスマス',            'FIXED_RANGE',  '{"start_mmdd":"12-01","end_mmdd":"12-25"}',                       95,  '2026-05-26T00:00:00+09:00', '2026-05-26T00:00:00+09:00');
