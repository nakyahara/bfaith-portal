-- Phase B1 Codex R1 High-2/High-3 対応: sync 監査列の追加。
--
-- High-2: sync_run_id 監査列要件未達 (notion_overrides 行がどの sync run で書かれたか追跡できない)
--   → notion_overrides.sync_run_id (TEXT、 NULL 許容) 追加
--   既存行は次の sync で埋まる。 audit_log と JOIN 可能。
--
-- High-3: errors > 0 でも "成功完走" 扱いで sync_state 更新 → partial success 検出不能
--   → sync_state に last_attempted_sync_at + last_error_count 追加
--   ルール:
--     errors = 0 → last_successful_sync_at + last_attempted_sync_at 両方更新
--     errors > 0 → last_attempted_sync_at + last_error_count のみ更新
--                  (last_successful_sync_at は据え置き = 「最後に全件 OK な日時」 を保持)
--   再実行判定は「last_attempted_sync_at - last_successful_sync_at > threshold なら fail 状態」 で監視可。

ALTER TABLE notion_overrides ADD COLUMN sync_run_id TEXT;

ALTER TABLE sync_state ADD COLUMN last_attempted_sync_at TEXT;
ALTER TABLE sync_state ADD COLUMN last_error_count       INTEGER NOT NULL DEFAULT 0;
