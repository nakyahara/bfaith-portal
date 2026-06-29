-- Phase E-17: Yahoo カテゴリマスタ + 取得キュー
--
-- 目的:
--   楽天 genre → Yahoo カテゴリ の対応を、 診断 console でなく専用 UI (単語検索 + ツリー選択) で
--   登録できるようにする基盤。 Yahoo categorySearch API (appid のみ、 OAuth 不要) から取得して
--   yahoo_category_master にキャッシュする。
--
-- 設計 (Codex Phase E-17 R1 反映):
--   - path は category_default_path を「正規キャッシュ」とする。 manual/decisions は yahoo_category_id を
--     持ち、 path は default_path から COALESCE 補完 (既存 resolver の挙動)。
--   - is_leaf は単独で持たない (未取得ノードで誤判定するため)。 children_fetched_at + child_count で判定。
--   - 取得は fetch_queue で状態管理 (途中失敗・再開・重複防止)。 全ツリー一括取得は PR2 で本格運用、
--     PR1 では manual_path_missing の path repair で個別 category を取得して master + default_path に入れる。

-- ─── Yahoo カテゴリマスタ ───
CREATE TABLE IF NOT EXISTS yahoo_category_master (
  category_id          INTEGER PRIMARY KEY,             -- Yahoo product_category ID
  parent_id            INTEGER,                         -- 親カテゴリ ID (ルートは 1、 ルート直下の親は 1)
  title_short          TEXT,
  title_medium         TEXT,
  title_long           TEXT,
  name                 TEXT,                            -- 表示名 (title_medium を採用、 検索対象)
  path                 TEXT,                            -- 祖先 Title を ':' 連結したフルパス (Current.Path 由来)
  depth                INTEGER,                         -- ルートからの深さ (ルート=0)
  child_count          INTEGER,                         -- 直下の子カテゴリ数 (children_fetched_at 時点)
  children_fetched_at  TEXT,                            -- 子を fetch した時刻 (NULL=未取得、 leaf 判定に使う)
  raw_json             TEXT CHECK(raw_json IS NULL OR json_valid(raw_json)),
  fetch_status         TEXT NOT NULL DEFAULT 'fetched'
                         CHECK(fetch_status IN ('fetched', 'partial', 'error')),
  last_error           TEXT,
  fetched_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_yahoo_category_master_parent ON yahoo_category_master(parent_id);
CREATE INDEX IF NOT EXISTS idx_yahoo_category_master_name   ON yahoo_category_master(name);

-- ─── 取得キュー (BFS 全ツリー取得・再開用、 PR2 で本格運用) ───
CREATE TABLE IF NOT EXISTS yahoo_category_fetch_queue (
  category_id   INTEGER PRIMARY KEY,
  status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK(status IN ('pending', 'running', 'done', 'error')),
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT,
  next_retry_at TEXT,
  enqueued_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  fetched_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_yahoo_category_fetch_queue_status ON yahoo_category_fetch_queue(status);
