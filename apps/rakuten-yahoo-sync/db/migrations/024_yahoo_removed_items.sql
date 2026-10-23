-- Yahoo!側で見えなくなった商品の記録 (2026-10-22)。
--
-- 旧: yahoo_registered_items から行を消す処理が無く、Yahoo で 1 商品でも削除・終売すると
--     その行の last_seen_at が二度と進まない → 毎回 stale_rows >= 1 → baseline が確立しない
--     → 差分取得 (全部更新の 1 段目) が baseline_incomplete で永久に止まっていた。
-- 新: 36 クエリ全部が件数一致 (complete) の回に限り、見えなかった行を「Yahoo から消えた」として
--     この表へ移してから yahoo_registered_items から外す (lib/yahoo-store-sync.js)。
--     1 回で外す件数には上限がある (多すぎる回は取得側の異常を疑って止める = 従来どおり baseline 不成立)。
CREATE TABLE yahoo_removed_items (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  item_code        TEXT NOT NULL,
  yahoo_item_code  TEXT,
  last_seen_at     TEXT,                 -- 最後に Yahoo で見えた時刻 (yahoo_registered_items の値)
  removed_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  sync_run_id      INTEGER               -- 外した baseline の sync_runs.id
);
CREATE INDEX idx_yahoo_removed_items_code ON yahoo_removed_items(item_code, id DESC);
