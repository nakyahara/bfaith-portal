-- Phase E-7-b: 楽天↔Yahoo 差分で出た移行候補テーブル (Codex R1/R2 確定)。
-- 設計原則:
--   - 楽天には居るが Yahoo (yahoo_registered_items) には居ない商品 = candidate
--   - 「楽天に居なくなった」 を即 stale にしない (RMS 一時欠落で候補が落ちないように 2 回連続欠落で flip)
--   - 「Yahoo に出た」 = resolved (publish 経由でも手動でも可、 観測ベース)
--   - excluded は migration_excluded を正、 candidates の status には持たない (E-7-c で導入、 active JOIN で判定)

CREATE TABLE migration_candidates (
  item_code                       TEXT PRIMARY KEY,                                          -- 楽天 itemNumber
  rakuten_manage_number           TEXT,                                                      -- N で始まる楽天 manage_number
  first_detected_at               TEXT NOT NULL,                                             -- 初検出日時
  last_detected_at                TEXT NOT NULL,                                             -- 最新で 「楽天有 + Yahoo無」 と確認した日時
  last_seen_in_rakuten_at         TEXT NOT NULL,                                             -- 最新で 「楽天で観測」 した日時 (status=stale 用)
  last_checked_yahoo_baseline_at  TEXT NOT NULL,                                             -- 最新で yahoo_registered_items と比較した日時
  -- Codex R2 Critical-5: stale 判定は連続 N 回欠落で flip (即時 stale にしない)
  missing_rakuten_count           INTEGER NOT NULL DEFAULT 0 CHECK(missing_rakuten_count >= 0),
  stale_at                        TEXT,
  status                          TEXT NOT NULL CHECK(status IN ('candidate', 'resolved', 'stale')),
  resolved_at                     TEXT,                                                      -- Yahoo に出たことが確認された日時
  source                          TEXT NOT NULL DEFAULT 'diff_detector'
);
CREATE INDEX idx_migration_candidates_status ON migration_candidates(status);
CREATE INDEX idx_migration_candidates_last_detected ON migration_candidates(last_detected_at);
