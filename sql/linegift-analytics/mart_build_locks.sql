-- mart_build_locks.sql — LINEギフト Phase 1 v1.0 Commit 2
--   DB ロックテーブル (build スクリプトの排他制御)
--
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §4 / §6
-- Codex v1.0 Round 2 Critical 2 (排他/ロールバック): DB lock + lease_ttl + heartbeat
--
-- file lock より DB lock を採用する理由 (Codex Round 2 推奨):
--   - Render の re-deploy で file lock が残るケースがある (stale)
--   - DB lock なら heartbeat_at で生存確認、lease_ttl で自動失効が可能
--   - HTTP 二重起動を SQL の UNIQUE 違反で検知可能 (409 相当)

CREATE TABLE IF NOT EXISTS mart_build_locks (
  lock_name            TEXT PRIMARY KEY,                                         -- 'build-linegift-analytics-mart' 等
  acquired_by          TEXT NOT NULL,                                            -- pid@hostname or request_id
  acquired_at          TEXT NOT NULL,                                            -- JST ISO8601
  heartbeat_at         TEXT NOT NULL,                                            -- 直近 heartbeat (build 内 30s 間隔で更新)
  lease_ttl_seconds    INTEGER NOT NULL DEFAULT 600,                             -- 既定 10 分
  status               TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED','EXPIRED')),
  error_message        TEXT,                                                     -- status=FAILED 時のエラー
  synced_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mart_build_locks_status
  ON mart_build_locks (status, heartbeat_at);

-- 履歴テーブル (削除した RUNNING を残す、監査用)
CREATE TABLE IF NOT EXISTS mart_build_lock_history (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  lock_name         TEXT NOT NULL,
  acquired_by       TEXT NOT NULL,
  acquired_at       TEXT NOT NULL,
  released_at       TEXT NOT NULL,
  status            TEXT NOT NULL,
  duration_seconds  INTEGER,
  error_message     TEXT,
  synced_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mart_build_lock_history_name_date
  ON mart_build_lock_history (lock_name, released_at);
