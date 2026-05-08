-- ============================================================
-- job_locks — concurrency guard table
-- ============================================================
-- Phase 1 ticket: #1-7a
-- Helper: apps/warehouse/job-locks.js
-- CLI: apps/warehouse/show-job-locks.js
--
-- 用途: daily-sync / mart rebuild / sync 等の重複起動防止
-- 単一 miniPC 前提 (分散ロック想定外)
--
-- 注意: このファイルはリファレンス用 (人間が読む形)。
--       実 DDL は apps/warehouse/db.js の initDB() で CREATE される。
--       両者の整合は「IF NOT EXISTS」と列定義の手動同期で担保。

CREATE TABLE IF NOT EXISTS job_locks (
  job_name      TEXT PRIMARY KEY,
  holder_id     TEXT NOT NULL,    -- ${hostname}-${pid}-${uuid}
  acquired_at   TEXT NOT NULL,    -- ISO8601 UTC
  expires_at    TEXT NOT NULL,    -- ISO8601 UTC, acquired_at + ttlMs
  heartbeat_at  TEXT NOT NULL     -- ISO8601 UTC, 最終 heartbeat 時刻
);

-- 状態確認 SQL (show-job-locks.js が利用)
-- SELECT job_name, holder_id, acquired_at, expires_at, heartbeat_at,
--        CASE WHEN expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now') THEN 'EXPIRED (stale)' ELSE 'ACTIVE' END AS status
-- FROM job_locks ORDER BY job_name;

-- Stale lock 一括削除 (cleanupStaleLocks() が利用)
-- DELETE FROM job_locks WHERE expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now');
