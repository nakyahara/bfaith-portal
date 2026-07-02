-- 再設計 R4 (2026-07-02): 「全部更新」パイプラインの実行記録。
--
-- 目的: 手動ボタン連打だった日次運用 (差分再取得→backfill→Notionページ作成→下書き補完→Notion sync)
-- を 1 パイプラインに直列化し、 実行状態・各ステップ結果を UI に見せる。
-- 実行本体は services/refresh-pipeline.js。 同時実行は status='running' + lease で排他。

CREATE TABLE refresh_runs (
  run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  finished_at       TEXT,
  status            TEXT NOT NULL DEFAULT 'running'
                      CHECK(status IN ('running', 'success', 'failed')),
  triggered_by      TEXT NOT NULL DEFAULT 'manual',          -- 'manual' | 'cron'
  current_step      TEXT,                                    -- 実行中ステップ key
  steps_json        TEXT CHECK(steps_json IS NULL OR json_valid(steps_json)),  -- 各ステップの要約結果
  error_message     TEXT,
  lease_expires_at  TEXT                                     -- running の stale 判定 (steal 用)
);
CREATE INDEX idx_refresh_runs_status ON refresh_runs(status);
