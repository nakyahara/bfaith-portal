-- Phase E-7-c: 移行除外管理テーブル (履歴型、 Codex R1/R2 確定)。
-- 設計原則:
--   - excludeKind: temporary / permanent (中原さん確定 R1 ②、 UI 表示は日本語 「一時除外」 / 「永続除外」)
--   - 履歴型 (id PK + UNIQUE INDEX (item_code) WHERE restored_at IS NULL):
--     「除外 → 復元 → 再除外」 を繰り返しても全イベント残る。 同時 active は 1 商品 1 行
--   - 種別変更 (temporary → permanent 等) は 「復元 + 新規除外」 を 1 transaction で実行 (Codex R2 C-2)
--   - excluded_by は 'manual' default、 将来 user_id 拡張余地
--   - 「除外商品を Yahoo に手動 publish された場合」 は自動解除しない (Codex R2 C-5)
--     UI で 「done だが除外履歴は残る」 と表示する判断は E-7-d で UI 起点書き換え時に対応

CREATE TABLE migration_excluded (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  item_code       TEXT NOT NULL,                                          -- 楽天 itemNumber
  exclude_kind    TEXT NOT NULL CHECK(exclude_kind IN ('temporary', 'permanent')),
  reason          TEXT NOT NULL,                                          -- 理由文 (UI 検索可能)
  excluded_at     TEXT NOT NULL,
  excluded_by     TEXT NOT NULL DEFAULT 'manual',
  restored_at     TEXT,                                                   -- 復元時に書く (NULL = active)
  restored_reason TEXT,
  restored_by     TEXT,
  CHECK(
    (restored_at IS NULL AND restored_reason IS NULL AND restored_by IS NULL)
    OR (restored_at IS NOT NULL)
  )
);

-- 同時 active は 1 商品 1 行 (履歴は累積、 active 1 行制約)
CREATE UNIQUE INDEX idx_migration_excluded_active
  ON migration_excluded(item_code) WHERE restored_at IS NULL;

-- 種別別フィルタ用 (UI で 「一時除外 N 件」 / 「永続除外 N 件」 の集計)
CREATE INDEX idx_migration_excluded_active_kind
  ON migration_excluded(exclude_kind) WHERE restored_at IS NULL;

-- 履歴順 (item_code 単位で時系列が欲しい時)
CREATE INDEX idx_migration_excluded_item_history
  ON migration_excluded(item_code, excluded_at DESC);
