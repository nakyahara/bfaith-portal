-- Codex E-5a R2 H-1 対応: 既に migration 003 を適用済みの環境にも安全に lease_expires_at 列を追加する。
--   - 003 を編集して列を足すと、 既存環境では migration runner が「version <= 003 は適用済」 として skip
--     → 列追加されず、 executor の INSERT/UPDATE が runtime failure になる
--   - 解決策: 新規 migration 004 で ALTER TABLE ADD COLUMN
--
-- 既存 in_progress 行 (本 PR 適用前に作成された row) は lease_expires_at IS NULL になる。
-- executor 側で NULL は「stale 扱い (steal 可能)」 とすることで永続 lease 残留を防ぐ (R2 M-2)。

ALTER TABLE publish_idempotency ADD COLUMN lease_expires_at TEXT;
