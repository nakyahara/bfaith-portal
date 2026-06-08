-- Phase A: Notion 商品マスター DB の cache。
--
-- 設計原則 (Codex Round 1 critical-1):
--   静的ルール = コード/JSON / 運用データ = DB / 業務入力 = Notion (RYS が DB cache へ取り込み)
--   実行時は DB を読む、 Notion は直接参照しない (可用性・速度・監査のため)。
--
-- notion-sync が daily cron + manual /rys/notion/sync で全件取得 → upsert (source_hash 差分のみ)。
-- field-mapper が rakuten_manage_number で lookup して Yahoo 上書きルールを適用。
--
-- Notion property → 本テーブル列の対応 (Phase 0 確定):
--   商品コード                → rakuten_manage_number (PK)
--   Yahoo!タイトル            → yahoo_title (Q24: 空なら readiness blocked)
--   売価                      → yahoo_price
--   Yahoo価格（佐川の場合）   → yahoo_price_sagawa (定形外宅急便50/ネコポスのとき適用、Q15)
--   配送方法                  → notion_delivery_label (delivery_mapping の PK 参照)
--   税率                      → notion_tax_rate (select '10'/'10%'/'8'/'#REF!'、 楽天 payment.taxRate と一致確認、 Q21)
--   バリエーション有無        → notion_has_variation (select 'バリエーション登録あり'/'バリエーションなし'、 Q12 整合チェック)
--   キャッチコピー            → yahoo_headline
--   商品説明文                → yahoo_caption_text
--   HTML_商品説明文           → yahoo_caption_html (優先)
--   JANコード                 → yahoo_jan
--   Status                    → notion_status (Q13: 進捗管理用、 RYS publish 判定には使わない)

-- Codex R1 High-1: UNIQUE(notion_page_id) で Notion 「商品コード」変更時の重複行残留を防ぐ。
--   upsert 戦略: notion-sync は notion_page_id を一意キーとして UPDATE、 rakuten_manage_number 変化時は
--   旧行を移送 (UPDATE SET rakuten_manage_number=新) で対応。 同一 page_id が 2 行存在することは禁止。
-- Codex R1 Medium-1: notion_delivery_label を delivery_mapping への FK にして Notion 選択肢揺れを DB で弾く。
--   ON UPDATE CASCADE で delivery_mapping のラベル変更に追従。 ON DELETE RESTRICT で誤削除防止。
CREATE TABLE notion_overrides (
  rakuten_manage_number  TEXT PRIMARY KEY,
  yahoo_title            TEXT,                                   -- Q24 必須 (空なら readiness blocked)
  yahoo_price            INTEGER,                                -- Notion 売価
  yahoo_price_sagawa     INTEGER,                                -- Yahoo価格(佐川) 条件付き上書き用
  notion_delivery_label  TEXT REFERENCES delivery_mapping(notion_delivery_label) ON UPDATE CASCADE ON DELETE RESTRICT,
  notion_tax_rate        TEXT CHECK(notion_tax_rate IS NULL OR notion_tax_rate IN ('10', '10%', '8', '#REF!')),
  notion_has_variation   TEXT CHECK(notion_has_variation IS NULL OR notion_has_variation IN ('バリエーション登録あり', 'バリエーションなし')),
  yahoo_headline         TEXT,                                   -- (30字制限)
  yahoo_caption_text     TEXT,                                   -- 文字説明文
  yahoo_caption_html     TEXT,                                   -- HTML 説明文 (優先)
  yahoo_jan              TEXT,                                   -- (number だが文字列保管)
  notion_status          TEXT,                                   -- 進捗ステータス (RYS は使わない)
  raw_properties_json    TEXT CHECK(raw_properties_json IS NULL OR json_valid(raw_properties_json)),  -- Notion 生 properties (将来の項目追加に備え保存)
  source_hash            TEXT NOT NULL,                          -- Notion ページ内容の hash (差分 sync 用)
  notion_page_id         TEXT NOT NULL UNIQUE,                   -- Notion ページ識別子 (Codex R1 High-1: 同一ページ重複禁止)
  source_updated_at      TEXT,                                   -- Notion last_edited_time
  synced_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_notion_overrides_synced_at ON notion_overrides(synced_at DESC);
-- notion_page_id は UNIQUE 制約により自動的に index 作成されるため、 別途 idx は不要。
