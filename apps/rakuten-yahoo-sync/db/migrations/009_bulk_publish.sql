-- Phase E-6-3: 一括登録 + エラー再実行 (Codex 確定後).
-- 設計原則:
--   - 1 batch = 複数商品をまとめて publish する 1 回の操作 (UI から checkbox 選択 or 失敗再実行)
--   - 順次実行 (並列より直列、 Yahoo API レート制御を優先)
--   - batch 内の各 item は単独で記録、 失敗しても batch は継続
--   - publish 自体は publish-executor.executePublish を呼ぶ (E-5b-B の実 publish と同じ経路)
--   - publish_idempotency に元々記録される success/fail/lease_lost を bulk_batch_items にも reference
--   - kill-switch (RYS_PUBLISH_ENABLED) は publish-executor 側 dual check なので bulk endpoint も自動で禁止される

CREATE TABLE bulk_publish_batches (
  batch_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  total          INTEGER NOT NULL CHECK(total > 0),
  succeeded      INTEGER NOT NULL DEFAULT 0 CHECK(succeeded >= 0),
  failed         INTEGER NOT NULL DEFAULT 0 CHECK(failed >= 0),
  in_progress    INTEGER NOT NULL DEFAULT 0 CHECK(in_progress >= 0),
  status         TEXT NOT NULL CHECK(status IN ('running', 'completed', 'aborted', 'failed')),
  triggered_by   TEXT NOT NULL CHECK(triggered_by IN ('manual', 'retry_failed')),
  started_at     TEXT NOT NULL,
  finished_at    TEXT,
  CHECK(
    (status = 'running' AND finished_at IS NULL)
    OR (status IN ('completed', 'aborted', 'failed') AND finished_at IS NOT NULL)
  )
);
CREATE INDEX idx_bulk_publish_batches_status ON bulk_publish_batches(status, started_at DESC);

-- Codex E-6-3 R1 Medium-2: batch_id に FK + CASCADE (履歴削除時に items も連動削除)
CREATE TABLE bulk_publish_batch_items (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id       INTEGER NOT NULL REFERENCES bulk_publish_batches(batch_id) ON DELETE CASCADE,
  item_code      TEXT NOT NULL,
  status         TEXT NOT NULL CHECK(status IN ('pending', 'running', 'success', 'failed', 'skipped', 'dedupe', 'flag_off', 'readiness_blocked')),
  publish_status TEXT,                                                     -- publish-executor の status (idempotency_key 経由でも参照可)
  idempotency_key TEXT,
  error_message  TEXT,
  started_at     TEXT,
  finished_at    TEXT
);
CREATE INDEX idx_bulk_publish_batch_items_batch ON bulk_publish_batch_items(batch_id, id);
CREATE INDEX idx_bulk_publish_batch_items_item ON bulk_publish_batch_items(item_code);

-- 同時 running batch は 1 件まで (race 対策、 全 batch 横断で UNIQUE INDEX)
CREATE UNIQUE INDEX idx_bulk_publish_batches_one_running
  ON bulk_publish_batches(status) WHERE status = 'running';
