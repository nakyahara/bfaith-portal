-- Phase E-9: 修正必要リストで 「何の商品か分からない」 (中原さん指摘) を解消するため、
-- migration_candidates に楽天 RMS の商品タイトルをキャッシュする列を追加する。
--
-- 設計:
--   - NULL 許容 (backfill 完了まで埋まらない / 楽天で削除された商品も NULL のまま)
--   - 楽天 fetch 時の itemName を素のまま保存 (HTML sanitize は表示側で行う)
--   - last_title_synced_at で 「いつ取った title か」 を覚えて、 別 cron で stale 解消できるようにする
ALTER TABLE migration_candidates ADD COLUMN rakuten_title TEXT;
ALTER TABLE migration_candidates ADD COLUMN last_title_synced_at TEXT;
