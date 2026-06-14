-- Phase E-11-d: notion_overrides に Yahoo!カテゴリ確定値 + path 確定値 列追加。
-- 設計:
--   ユーザー (中原さん) が Notion 商品マスターに 「Yahoo!カテゴリID」 列 + 「Yahoo!path」 列を追加して
--   入力すると、 notion-sync 経由でこの列に取り込まれる。
--   category-resolver が Notion 値 > 学習辞書 の順で参照する。
ALTER TABLE notion_overrides ADD COLUMN notion_product_category INTEGER;
ALTER TABLE notion_overrides ADD COLUMN notion_path TEXT;
