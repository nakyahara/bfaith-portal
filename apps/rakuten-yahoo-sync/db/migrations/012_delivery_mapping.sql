-- Phase A (Codex Round 8 stop 後): delivery_mapping 既存空テーブルを Phase 0 仕様で置換。
--
-- 旧 (001_initial_v8 の空テーブル): rakuten_delivery_key TEXT PK / postage_set CHECK(IN 0,1)
--   → Phase 0 仕様 (Yahoo postage_set = 1-20 / Notion 配送方法 8 値 + applies_sagawa_price flag)
--   と非互換のため DROP + CREATE で完全置換。 既存データは空 (W1 まで未使用) なので safe。
--
-- 8 行 seed: Notion DB の「配送方法」select 全 8 値 → (yahoo_delivery, yahoo_postage_set) マッピング。
-- 中原さん Q17-Q19 確定 (2026-05-27)。
--   ネコポス                                → (1, 6)
--   ヤマト50サイズ                          → (1, 10)
--   ヤマト宅急便                            → (1, 11)
--   沖縄北海道別送料_ヤマト宅急便            → (0, 11)   ← delivery=0=送料あり
--   クリックポスト                          → (1, 9)
--   ゆうパケットパフ                        → (1, 12)
--   定形外（ヤフーのみ宅急便50サイズ）       → (1, 10) + applies_sagawa_price=1
--   定形外（ヤフーのみネコポス）             → (1, 6)  + applies_sagawa_price=1
--
-- applies_sagawa_price=1 の 2 行: 商品の Notion「Yahoo価格（佐川の場合）」が非空なら、
-- field-mapper でその値を price に上書きする条件 (Q15 確定)。

DROP TABLE IF EXISTS delivery_mapping;

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
