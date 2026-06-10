-- Phase E-3: variation_spec_mapping (バリ品の Yahoo spec_id マッピング)。
--
-- readiness-check #11 (バリ品時のみ) で評価する mapping table。
-- 元ローカル RYS 001_initial_v8.sql の同名テーブルをそのまま流用。
--
-- PK = (axis_label, yahoo_product_category) で同一 axis を別カテゴリで別 spec_id 登録可能
-- (Codex B2 R1 M-1 反映)。 seed は E-4 で variation_spec seed CLI を作って手動入力する想定。

CREATE TABLE variation_spec_mapping (
  axis_label TEXT NOT NULL,
  yahoo_product_category INTEGER NOT NULL,
  spec_id TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (axis_label, yahoo_product_category)
);
