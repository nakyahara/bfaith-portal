-- f_aupay_sku_map DDL
-- ticket: au PAY マーケット Phase 1 A-0
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md
--
-- 用途:
--   au PAY マーケットの item_code から ne_code (NE 商品コード) への解決 fallback。
--   主は m_products 直結 (商品コード = item_code、CI、90.7% カバー = 1,521/1,676 SKU)。
--   残り 9.3% (155 SKU) のうち:
--     - variant ありそうな親 SKU (例 nursewatch で option_info に「カラー=ピンク」) は unresolved 維持
--       (variant 粒度を勝手に潰さない、Yahoo Phase 1.1 の教訓 feedback_sku_case_normalization.md)
--     - 誤爆しない単純商品だけ本テーブルに手動 entry で補完
--   候補リストは apps/warehouse/list-aupay-unresolved-skus.js で生成 (中原さんが手動 entry の判断材料)
--
-- 解決優先順 (build SQL 内 sku_resolved CTE で 2 段階 fallback):
--   1. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(item_code))            (= master_match、CI)
--   2. f_aupay_sku_map WHERE LOWER(TRIM(aupay_key)) = LOWER(TRIM(item_code)) AND store_id  (= manual_map)
--   3. NULL → unresolved_sku_flag = 1 (DQ で監視、variant ありそうな親 SKU はここに残る)

CREATE TABLE IF NOT EXISTS f_aupay_sku_map (
  store_id   TEXT NOT NULL DEFAULT 'b-faith01',
  aupay_key  TEXT NOT NULL,                       -- = item_code
  ne_code    TEXT NOT NULL,
  resolution_source TEXT NOT NULL CHECK (
    resolution_source IN ('manual', 'auto_pattern', 'fallback_parent')
  ),
  notes      TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (store_id, aupay_key)
);

CREATE INDEX IF NOT EXISTS idx_f_aupay_sku_map_ne ON f_aupay_sku_map(ne_code);
