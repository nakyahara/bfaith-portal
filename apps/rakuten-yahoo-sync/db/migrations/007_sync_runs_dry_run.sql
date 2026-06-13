-- Phase E-7-b: sync_runs.dry_run 列追加 (Codex R1 High-2)。
-- dry-run の success が次回 overlap 急減判定を汚染する穴を塞ぐ。
--   - dry-run は 「stats 算出だけして書き込まない」 ので、 履歴の overlap_rate を本番判定材料にしない。
--   - 既存 migration 005 を編集すると prod 環境では runner が再適用しないため、 新規 migration で ALTER (E-5a R2 H-1 と同パターン)。
ALTER TABLE sync_runs ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0 CHECK(dry_run IN (0, 1));
