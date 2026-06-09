-- RakutenYahooSync (RYS) bfaith-portal app 専用 SQLite 初期 schema (Phase E-2)。
--
-- ローカル RYS (Downloads/RakutenYahooSync/) の migration 001-016 を整理して、
-- bfaith-portal app context での「新規 DB」 として 1 file にまとめる:
--   - delivery_mapping (Phase A-012 + Q17-Q19 確定 8 seed)
--   - notion_overrides (Phase A-013、 Notion 商品マスター cache、 UNIQUE(notion_page_id) + FK)
--   - publish_batches / jobs (Phase 001 minimum + Phase A-014 readiness_* 列)
--   - sync_state (Phase A-015 sync 状態管理 + Phase A-016 audit 列)
--   - audit_log (Phase 010、 bfaith-portal 全 app 共通の監査ログと別管理)
--
-- Codex Phase A-D で R1-R3 stop 済み、 ローカルで 271 件 Notion sync 実証済み。

-- ─── 1. delivery_mapping (Phase A-012) ───
CREATE TABLE delivery_mapping (
  notion_delivery_label  TEXT PRIMARY KEY,
  yahoo_delivery         INTEGER NOT NULL CHECK(yahoo_delivery IN (0, 1, 2, 3)),
  yahoo_postage_set      INTEGER NOT NULL CHECK(yahoo_postage_set BETWEEN 1 AND 20),
  applies_sagawa_price   INTEGER NOT NULL DEFAULT 0 CHECK(applies_sagawa_price IN (0, 1)),
  note                   TEXT,
  updated_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

INSERT INTO delivery_mapping (notion_delivery_label, yahoo_delivery, yahoo_postage_set, applies_sagawa_price, note) VALUES
  ('ネコポス',                                 1, 6,  0, 'ネコポスグループ'),
  ('ヤマト50サイズ',                            1, 10, 0, 'ヤマト50グループ'),
  ('ヤマト宅急便',                              1, 11, 0, 'ヤマト宅急便グループ'),
  ('沖縄北海道別送料_ヤマト宅急便',              0, 11, 0, '宅急便だが沖縄北海道別送料 (delivery=0=送料あり)'),
  ('クリックポスト',                            1, 9,  0, 'クリックポストグループ'),
  ('ゆうパケットパフ',                          1, 12, 0, 'ゆうパケットグループ'),
  ('定形外（ヤフーのみ宅急便50サイズ）',         1, 10, 1, '楽天=定形外、Yahoo=ヤマト50扱い。Notion Yahoo価格(佐川) 非空なら price 上書き'),
  ('定形外（ヤフーのみネコポス）',               1, 6,  1, '楽天=定形外、Yahoo=ネコポス扱い。Notion Yahoo価格(佐川) 非空なら price 上書き');

-- ─── 2. notion_overrides (Phase A-013 + UNIQUE/FK Codex R1 High-1 / Medium-1 反映) ───
CREATE TABLE notion_overrides (
  rakuten_manage_number  TEXT PRIMARY KEY,
  yahoo_title            TEXT,
  yahoo_price            INTEGER,
  yahoo_price_sagawa     INTEGER,
  notion_delivery_label  TEXT REFERENCES delivery_mapping(notion_delivery_label) ON UPDATE CASCADE ON DELETE RESTRICT,
  notion_tax_rate        TEXT CHECK(notion_tax_rate IS NULL OR notion_tax_rate IN ('10', '10%', '8', '#REF!')),
  notion_has_variation   TEXT CHECK(notion_has_variation IS NULL OR notion_has_variation IN ('バリエーション登録あり', 'バリエーションなし')),
  yahoo_headline         TEXT,
  yahoo_caption_text     TEXT,
  yahoo_caption_html     TEXT,
  yahoo_jan              TEXT,
  notion_status          TEXT,
  raw_properties_json    TEXT CHECK(raw_properties_json IS NULL OR json_valid(raw_properties_json)),
  source_hash            TEXT NOT NULL,
  notion_page_id         TEXT NOT NULL UNIQUE,
  source_updated_at      TEXT,
  sync_run_id            TEXT,
  synced_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX idx_notion_overrides_synced_at ON notion_overrides(synced_at DESC);

-- ─── 3. publish_batches / jobs (publish runner 用、 Phase A-014 readiness_* 含む) ───
CREATE TABLE publish_batches (
  batch_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  state         TEXT NOT NULL CHECK(state IN ('preparing', 'ready', 'running', 'success', 'failed', 'cancelled')),
  target_count  INTEGER NOT NULL DEFAULT 0,
  created_by    TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE jobs (
  item_code                TEXT PRIMARY KEY,
  batch_id                 INTEGER REFERENCES publish_batches(batch_id),
  current_state            TEXT NOT NULL CHECK(current_state IN (
                             'pending', 'item_staged', 'stock_set',
                             'publish_reserved', 'published_verified',
                             'verification_pending', 'failed'
                           )),
  attempt                  INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
  last_error               TEXT CHECK(last_error IS NULL OR json_valid(last_error)),
  payload_json             TEXT CHECK(payload_json IS NULL OR json_valid(payload_json)),
  readiness_status         TEXT NOT NULL DEFAULT 'pending'
                             CHECK(readiness_status IN ('pending', 'ok', 'blocked')),
  readiness_blocked_reasons TEXT CHECK(
    (readiness_status IN ('pending', 'ok') AND readiness_blocked_reasons IS NULL)
    OR
    (readiness_status = 'blocked'
      AND readiness_blocked_reasons IS NOT NULL
      AND json_valid(readiness_blocked_reasons)
      AND json_type(readiness_blocked_reasons) = 'array'
      AND json_array_length(readiness_blocked_reasons) > 0)
  ),
  last_readiness_at        TEXT,
  created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX idx_jobs_state ON jobs(current_state);
CREATE INDEX idx_jobs_batch ON jobs(batch_id);
CREATE INDEX idx_jobs_readiness_status ON jobs(readiness_status);

-- ─── 4. sync_state (Phase A-015 + Phase A-016 audit 列) ───
CREATE TABLE sync_state (
  source                   TEXT PRIMARY KEY,
  last_successful_sync_at  TEXT,
  last_full_sync_at        TEXT,
  last_cursor              TEXT,
  last_sync_run_id         TEXT,
  last_attempted_sync_at   TEXT,
  last_error_count         INTEGER NOT NULL DEFAULT 0,
  updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- ─── 5. audit_log (RYS 専用、 bfaith-portal の audit_log とは独立) ───
CREATE TABLE audit_log (
  audit_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  actor         TEXT NOT NULL,
  action        TEXT NOT NULL,
  target_type   TEXT,
  target_id     TEXT,
  after_json    TEXT CHECK(after_json IS NULL OR json_valid(after_json)),
  result        TEXT NOT NULL CHECK(result IN ('success', 'failed')),
  error_message TEXT,
  occurred_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX idx_audit_log_action ON audit_log(action, occurred_at DESC);
