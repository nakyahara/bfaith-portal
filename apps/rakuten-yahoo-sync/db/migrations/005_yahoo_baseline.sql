-- Phase E-7-a: Yahoo baseline 台帳 (Codex R1/R2 確定)。
-- スコープ:
--   1. yahoo_registered_items  — Yahoo!ショッピング側で観測された商品コード台帳 (publish 経路の証跡ではない)
--   2. sync_runs                — Yahoo baseline / 楽天 diff / RYS full の run 履歴 (E-7-b/e でも書き込む)
--   3. yahoo_baseline_state     — baseline 正本 (1 行制約、 R2 Critical-6: sync_runs.establishes_baseline だけだと弱い)
--
-- 設計原則:
--   - registered_items は 「観測された Yahoo 実存」 のみを表す。 publish 経路の証跡は publish_idempotency 側に分離。
--   - sync_runs の status='running' は per-sync_name で同時 1 件まで (partial UNIQUE INDEX、 R2 High E-2 race 対策)
--   - yahoo_baseline_state は GC 影響を受けない正本。 sync_runs は履歴。 assertYahooBaselineComplete は state を見る。

-- ─── 1. Yahoo 側で観測された商品コード台帳 ───
CREATE TABLE yahoo_registered_items (
  item_code         TEXT PRIMARY KEY,
  yahoo_item_code   TEXT NOT NULL,                                    -- Yahoo myItemList で観測した ItemCode
  has_sub_code      INTEGER NOT NULL DEFAULT 0 CHECK(has_sub_code IN (0, 1)),
  last_seen_at      TEXT NOT NULL,                                    -- 最新の baseline run でこの item_code が観測された ISO 時刻
  source            TEXT NOT NULL CHECK(source IN ('yahoo_existing', 'manual'))
                                                                      -- publish 経路の証跡は publish_idempotency 側 (R1 critical: source に sync_published 入れない)
);
CREATE INDEX idx_yahoo_registered_yahoo_code ON yahoo_registered_items(yahoo_item_code);
CREATE INDEX idx_yahoo_registered_last_seen ON yahoo_registered_items(last_seen_at);

-- ─── 2. sync 履歴 (Yahoo baseline / 楽天 diff / RYS full) ───
-- Codex E-7-a R1 Medium-1: sync_name に CHECK 追加。 typo が running guard をすり抜けるのを防ぐ。
-- Codex E-7-a R1 High-2: run_lease_expires_at 追加。 crash 後の running 行を steal できるよう lease 管理。
CREATE TABLE sync_runs (
  id                      INTEGER PRIMARY KEY AUTOINCREMENT,
  sync_name               TEXT NOT NULL CHECK(sync_name IN ('yahoo_baseline', 'rakuten_diff', 'rys_full')),
  started_at              TEXT NOT NULL,
  finished_at             TEXT,
  status                  TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed')),
  triggered_by            TEXT NOT NULL CHECK(triggered_by IN ('cron', 'manual')),
  trigger_request_id      TEXT,                                       -- on-demand sync の冪等 key (E-7-e で使う)
  run_lease_expires_at    TEXT,                                       -- running 行の lease 期限。 切れたら別 run で steal 可
  run_token               TEXT,                                       -- Codex R2 High-1: 「lease lost」検出用。 finishSyncRun は token 一致を要求
  -- baseline 用 detail
  baseline_query_hash     TEXT,
  baseline_query_count    INTEGER,
  item_count_observed     INTEGER,
  stale_rows_count        INTEGER,                                    -- 今回 last_seen_at 更新されなかった既存行
  establishes_baseline    INTEGER NOT NULL DEFAULT 0 CHECK(establishes_baseline IN (0, 1)),
  -- diff 用 detail (E-7-b で使う、 ここでは列だけ確保)
  rakuten_total           INTEGER,
  yahoo_total             INTEGER,
  overlap                 INTEGER,
  overlap_rate            REAL,
  candidates_new          INTEGER,
  candidates_resolved     INTEGER,
  -- mirror sync 結果 (E-7-e で使う、 列だけ確保)
  mirror_synced_at        TEXT,
  mirror_status           TEXT CHECK(mirror_status IS NULL OR mirror_status IN ('pending', 'success', 'failed')),
  -- 共通
  per_query_json          TEXT CHECK(per_query_json IS NULL OR json_valid(per_query_json)),
  error_message           TEXT,
  CHECK(
    (status = 'running' AND finished_at IS NULL)
    OR (status IN ('success', 'failed') AND finished_at IS NOT NULL)
  )
);
CREATE INDEX idx_sync_runs_name_started ON sync_runs(sync_name, started_at DESC);
CREATE INDEX idx_sync_runs_status ON sync_runs(status, started_at DESC);
-- R2 High E-2: 同一 sync_name で 'running' は同時 1 件まで (race 対策、 SELECT+INSERT race を防ぐ)
CREATE UNIQUE INDEX idx_sync_runs_one_running
  ON sync_runs(sync_name) WHERE status = 'running';

-- ─── 3. Yahoo baseline 正本 (1 行制約) ───
-- R2 Critical-6: sync_runs.establishes_baseline だけだと GC で根拠 row が消える / failed/running 判定が複雑。
--   yahoo_baseline_state を 「現時点の正本」 とし、 assertYahooBaselineComplete はここを見る。
-- Codex E-7-a R1 High-1: baseline_run_id に FK は付けない。
--   foreign_keys=ON 本番で 「正本が参照してる sync_runs が GC 削除できない」 / CASCADE で正本も消える、 のジレンマ。
--   baseline_run_id は trace 用、 必要な証跡 (established_at / observed_count / query_hash) は state 側に持つ。
CREATE TABLE yahoo_baseline_state (
  id                INTEGER PRIMARY KEY CHECK(id = 1),
  baseline_run_id   INTEGER NOT NULL,                                 -- trace 用 (FK 無し、 GC 独立)
  established_at    TEXT NOT NULL,
  observed_count    INTEGER NOT NULL,
  query_hash        TEXT NOT NULL,
  is_complete       INTEGER NOT NULL CHECK(is_complete IN (0, 1)),
  updated_at        TEXT NOT NULL
);
