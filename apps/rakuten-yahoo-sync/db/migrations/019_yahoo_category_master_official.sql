-- 再設計 R1 (2026-07-02): Yahoo カテゴリマスタを「公式 CSV 由来」に置き換える。
--
-- 経緯:
--   migration 018 の yahoo_category_master は Yahoo categorySearch API (appid 公開 API) からの
--   キャッシュ前提だったが、 categorySearch は 2022-04-25 に廃止されており恒久 403 (PR #391 の負債)。
--   一方、 ローカル RYS (Downloads/RakutenYahooSync) で 2026-05-21 に downloadShopCategories
--   (SHPカテゴリ/スペックダウンロード API、 OAuth) から取得済みの公式マスタ 12,044 件が実在する。
--   id は editItem product_category と同一名前空間 (観測 515 コード中 501 = 97.3% 一致を実測確認済)。
--
-- 本 migration:
--   - categorySearch 前提の旧 yahoo_category_master / yahoo_category_fetch_queue を DROP
--     (中身は 403 API へのアクセス記録のみで、 有効なカテゴリデータは入っていない)
--   - 公式 CSV スキーマ (id,name,path_name,relation,updated_at) で作り直す
--   - データは migration 020 で 12,044 件 seed (ローカル RYS DB から機械生成)
--   - 用途: ①紐付け画面のキーワード検索候補 ②AI 初期紐づけ (R2) の候補生成・実在検証

DROP TABLE IF EXISTS yahoo_category_fetch_queue;
DROP TABLE IF EXISTS yahoo_category_master;

CREATE TABLE yahoo_category_master (
  product_category   TEXT PRIMARY KEY,      -- CSV id (= editItem product_category、 数値文字列)
  name               TEXT NOT NULL,         -- CSV name (カテゴリ名)
  path_name          TEXT,                  -- CSV path_name ("A > B > C" 階層フルパス)
  relation           TEXT,                  -- CSV relation (トップレベルグループ名)
  source_updated_at  TEXT,                  -- CSV updated_at (Yahoo 側の更新日)
  is_active          INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),  -- マスタ再取込時に廃止検知したら 0
  imported_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX idx_yahoo_category_master_name ON yahoo_category_master(name);
