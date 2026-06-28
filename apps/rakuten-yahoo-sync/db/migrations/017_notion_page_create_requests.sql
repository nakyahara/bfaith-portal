-- Phase E-14 (2026-06-28): 楽天 RMS → Notion master 新規 page 作成 endpoint の CAS lock テーブル。
--
-- 経緯:
--   PR #367 で「Notion master に居て空欄あり」 SKU の自動補完を実装したが、
--   dry-run で「Notion master に row 自体が無い」 SKU が ~276 件あることが判明。
--   楽天 RMS から取って Notion に新規 page 作成 endpoint が必要。
--
--   重複作成防止 (Codex Phase E-14 R7 stop): DB CAS lock + Notion pre-check の 2 段。
--   - 同一 SKU の連続押下や 並列 worker による Notion 重複 page 作成を防ぐ
--   - status='completed' は claim 不能 (ON CONFLICT WHERE で弾く)
--   - lease 期限切れの running は奪取可能
--   - finalize は lease_token 一致時のみ書込、 不一致は row 不変 + 'lease_lost' report

CREATE TABLE notion_page_create_requests (
  rakuten_manage_number  TEXT PRIMARY KEY,
  status                 TEXT NOT NULL CHECK(status IN ('running', 'completed', 'failed')),
  lease_token            TEXT,                                                       -- UUID4、 claim 時に生成
  lease_expires_at       TEXT,                                                       -- claim 時に +LEASE_TTL_MS
  notion_page_id         TEXT,                                                       -- completed 時の Notion page UUID
  error_message          TEXT,
  attempt                INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
  created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_notion_page_create_requests_status
  ON notion_page_create_requests(status);
CREATE INDEX idx_notion_page_create_requests_lease
  ON notion_page_create_requests(status, lease_expires_at);
