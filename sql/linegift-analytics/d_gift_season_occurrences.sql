-- d_gift_season_occurrences: シーンの年別実体 (start/end 日付レンジ)
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-3
-- Codex 議論: Round 2 Critical (可変祝日の年別事前展開)
--
-- m_gift_seasons の rule_json を年単位で展開してここに INSERT。
-- 集計時は f_linegift_finance_sku_daily_v1.date_jst BETWEEN start_date_jst AND end_date_jst で絞り込む。
--
-- year 跨ぎ (NEW_YEAR_CARD: 12-26 〜 翌 01-03) は anchor_date_jst の年で season_year を固定する
-- (Codex Round 3 Medium 反映)。

CREATE TABLE IF NOT EXISTS d_gift_season_occurrences (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  season_code       TEXT NOT NULL,
  season_year       INTEGER NOT NULL,
  start_date_jst    TEXT NOT NULL CHECK (start_date_jst GLOB '????-??-??'),
  end_date_jst      TEXT NOT NULL CHECK (end_date_jst GLOB '????-??-??'),
  anchor_date_jst   TEXT,                                                -- 母の日当日等 (NULL = 範囲シーン)
  is_active         INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  created_by        TEXT NOT NULL DEFAULT 'system',
  updated_by        TEXT NOT NULL DEFAULT 'system',
  synced_at         TEXT NOT NULL,
  UNIQUE (season_code, season_year),
  FOREIGN KEY (season_code) REFERENCES m_gift_seasons(season_code)
);

CREATE INDEX IF NOT EXISTS idx_d_gift_occ_date
  ON d_gift_season_occurrences (start_date_jst, end_date_jst, is_active);

CREATE INDEX IF NOT EXISTS idx_d_gift_occ_season
  ON d_gift_season_occurrences (season_code, season_year);
