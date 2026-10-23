-- Notion 廃止後の確定値 (2026-10-22、方針案 20260914 の D-2〜D-5 / PR 2・PR 3)。
--
-- 旧: Yahoo!タイトル・売価・配送方法・税率 (+カテゴリ) は Notion 商品マスター → notion_overrides に取り込んでいた。
--     Notion は 2026-09-14 に廃止。notion_overrides はそれ以後増えも直りもしない「旧運用の写し」。
-- 新: 人が「この値で確定」を押した値だけをこの表に持つ。出所 (source) は 2 つ:
--       manual = 移行アプリの商品詳細ドロワーで確定 (既存商品。商品登録ハブに無い楽天の商品)
--       portal = 商品登録ハブの商品詳細で確定 (新商品)。以後この商品は移行アプリでは読み取り専用 (D-5)
--     この表に行が無い商品だけ、旧 notion_overrides の値を「旧運用から継承」として使う (D-3。承認日時は作らない)。
--
-- notion_overrides は消さない・書き換えない (company-db の初期取込が Yahoo JAN を読む / 旧値の記録)。
-- headline・caption・JAN はこの表に持たない = 旧 notion_overrides に値があればそのまま使い続ける
-- (ハブが供給しない項目を空で上書きしない — Codex 2 巡目)。

CREATE TABLE yahoo_confirmed_values (
  rakuten_manage_number  TEXT PRIMARY KEY,
  source                 TEXT NOT NULL CHECK(source IN ('manual', 'portal')),
  yahoo_title            TEXT NOT NULL CHECK(length(trim(yahoo_title)) > 0),
  -- 確定した売価。楽天の値は「候補」として画面に出すだけで、ここに入るのは人が確定した金額 (D-2)
  yahoo_price            INTEGER NOT NULL CHECK(yahoo_price > 0),
  -- 佐川売価の扱い (配送方法が applies_sagawa_price=1 のときだけ意味を持つ):
  --   'price' = yahoo_price_sagawa を使う / 'same' = 通常売価と同額を明示的に選んだ / NULL = 対象外の配送
  sagawa_mode            TEXT CHECK(sagawa_mode IS NULL OR sagawa_mode IN ('price', 'same')),
  yahoo_price_sagawa     INTEGER CHECK(yahoo_price_sagawa IS NULL OR yahoo_price_sagawa > 0),
  delivery_label         TEXT NOT NULL REFERENCES delivery_mapping(notion_delivery_label) ON UPDATE CASCADE ON DELETE RESTRICT,
  tax_rate               TEXT NOT NULL CHECK(tax_rate IN ('10', '8')),
  -- カテゴリは任意。両方あれば辞書より優先・両方空なら辞書 (紐付け画面) で解決・片方だけは受け付けない
  product_category       INTEGER,
  path                   TEXT,
  portal_draft_id        INTEGER,            -- source='portal' のとき商品登録ハブの product_drafts.id (画面のリンク用)
  confirmed_by           TEXT NOT NULL,
  confirmed_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  version                INTEGER NOT NULL DEFAULT 1 CHECK(version >= 1),
  CHECK ((product_category IS NULL) = (path IS NULL OR trim(path) = '')),
  CHECK (source <> 'portal' OR portal_draft_id IS NOT NULL),
  CHECK (sagawa_mode IS NOT 'price' OR yahoo_price_sagawa IS NOT NULL)
);

-- 確定し直す前の値 (旧値は履歴へ — D-5)。最初の確定のときは旧 notion_overrides の値を 1 行残す (source_before='legacy_notion')
CREATE TABLE yahoo_confirmed_value_history (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  rakuten_manage_number  TEXT NOT NULL,
  source_before          TEXT NOT NULL,      -- 'manual' / 'portal' / 'legacy_notion' / 'none'
  version_before         INTEGER,
  values_before_json     TEXT CHECK(values_before_json IS NULL OR json_valid(values_before_json)),
  source_after           TEXT NOT NULL,
  version_after          INTEGER NOT NULL,
  values_after_json      TEXT NOT NULL CHECK(json_valid(values_after_json)),
  changed_by             TEXT NOT NULL,
  changed_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX idx_ycvh_manage ON yahoo_confirmed_value_history(rakuten_manage_number, id DESC);
