-- Phase B1: 外部ソース sync の状態管理テーブル (Codex Phase B1 設計提案)。
--
-- 用途:
--   notion-sync の完走/失敗判定、 削除検出ガード、 将来の差分 sync (C 案) の since cursor 保管。
--
-- 設計原則:
--   - source = 'notion_overrides' のような論理名で PK (将来 yahoo store sync 等も同居可能)
--   - last_successful_sync_at: 完走時刻 (途中失敗は更新しない) → 削除反映の前提
--   - last_full_sync_at:       full mode 完走時刻 (delta mode 拡張時、 full の鮮度を別管理)
--   - last_cursor:             Notion API pagination cursor (中断時の resume 用、 当面未使用)
--   - last_sync_run_id:        各 sync 実行の UUID (audit_log と JOIN 可能)
--
-- C 案 (差分 sync) 拡張時:
--   delta mode は filter timestamp = last_successful_sync_at を渡す。
--   週次 full sync は last_full_sync_at を見て stale 判定。

CREATE TABLE sync_state (
  source                   TEXT PRIMARY KEY,
  last_successful_sync_at  TEXT,
  last_full_sync_at        TEXT,
  last_cursor              TEXT,
  last_sync_run_id         TEXT,
  updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
