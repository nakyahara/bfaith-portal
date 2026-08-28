-- f_yahoo_sku_map DDL
-- ticket: Yahoo Phase 1 Y-0a
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1a設計書_v0.4_20260510.md
--
-- 用途:
--   Yahoo の (item_id, sub_code) から ne_code (NE 商品コード) への解決 fallback。
--   主は m_products 直結 (商品コード = sub_code or item_id) で 88.92% カバー、
--   残り 11% (258 SKU) のみ本テーブルで補完。
--
-- 解決優先順 (build SQL 内 sku_resolved CTE で 3 段階 fallback):
--   1. m_products WHERE 商品コード = sub_code (= variant 別 SKU、56.80% カバー)
--   2. m_products WHERE 商品コード = item_id (= 親 SKU、+11.24%)
--   3. f_yahoo_sku_map WHERE yahoo_key = sub_code OR item_id (= 手動 map、残り)
--   4. unresolved_sku_flag = 1 (= 32% 未解決対象、監視のみ)

-- 粒度メモ (価格一括改定ツール PR1、2026-08-28):
--   本テーブルの PK は yahoo_key 単独だが、意味的な粒度は (store_id, yahoo_key)。
--   Render mirror 側 (mirror_yahoo_sku_map) は複合 PK にしてある。
--   2 店舗目を持つ時は本テーブルも複合 PK へ移行すること (今の単独 PK は
--   「同じ yahoo_key を別ストアで持てない」という、現状より強い制約になっている)。

CREATE TABLE IF NOT EXISTS f_yahoo_sku_map (
  yahoo_key  TEXT NOT NULL PRIMARY KEY,  -- = item_id or item_id-sub_code 形式
  store_id   TEXT NOT NULL DEFAULT 'b-faith01',
  ne_code    TEXT NOT NULL,
  resolution_source TEXT NOT NULL CHECK (
    resolution_source IN ('manual', 'auto_pattern', 'fallback_parent')
  ),
  notes      TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_f_yahoo_sku_map_ne ON f_yahoo_sku_map(ne_code);
