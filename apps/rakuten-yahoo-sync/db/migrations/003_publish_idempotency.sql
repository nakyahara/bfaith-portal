-- Phase E-5a: 実 publish 用 idempotency + 監査テーブル。
--
-- 設計原則 (Codex Phase E R3-R4):
--   - 同一 (itemCode + 日付 など) で 2 回呼んでも 2 回 publish しない
--   - 既存結果があれば dedupe (HTTP 200 + dedupe=true で前回結果を返す)
--   - in_progress 状態の lease として機能 (同時実行検知)
--
-- caller (publish-executor) は idempotency_key を caller 責任で生成:
--   key = sha256(itemCode + manageNumber + iso_date + scope)
--   再試行は同じ key で投げると既存結果を返す。
--
-- Phase E-5a 時点では Yahoo API 呼び出し本体は未実装 (501 placeholder)、
-- でも idempotency / 状態遷移 / audit は本実装する。 E-5b で editItem を繋ぐ。

CREATE TABLE publish_idempotency (
  idempotency_key   TEXT PRIMARY KEY,
  item_code         TEXT NOT NULL,
  manage_number     TEXT NOT NULL,
  status            TEXT NOT NULL CHECK(status IN ('in_progress', 'success', 'failed', 'not_implemented')),
  result_json       TEXT CHECK(result_json IS NULL OR json_valid(result_json)),
  error_message     TEXT,
  attempt           INTEGER NOT NULL DEFAULT 1 CHECK(attempt >= 1),
  created_by        TEXT NOT NULL,
  created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  completed_at      TEXT
);
-- lease_expires_at は migration 004 で別 ALTER で追加 (Codex E-5a R2 H-1: 編集後 ALTER で安全に既存環境に適用)
CREATE INDEX idx_publish_idempotency_item ON publish_idempotency(item_code);
CREATE INDEX idx_publish_idempotency_status ON publish_idempotency(status, created_at DESC);
