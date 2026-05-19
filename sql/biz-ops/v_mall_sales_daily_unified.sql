-- v_mall_sales_daily_unified — 全モール売上の日次統合 view (biz-ops-overview)
-- 2026-05-19、Codex consultation 反映 (Codex 推奨 view + 税込統一)
--
-- 用途: biz-ops-overview ダッシュボード + GChat 売上サマリ通知の単一データソース
-- 売上定義: 顧客支払額 (税込、手数料引かず、中原さん 2026-05-19 確定)
--
-- 各モールの売上列マッピング:
--   - Amazon (sales_principal_jpy 税抜): × 1.10 で税込換算
--     ⚠️ 軽減税率 8% SKU (食品系) は過大評価の可能性あり、Phase 2 で SKU 別税率対応検討
--     現状の m_products.消費税率 はモール売上では参照しない (生 fact 列のみ参照)
--   - 楽天/Yahoo/au PAY/LINEギフト: sales_principal_jpy_incl をそのまま (税込)
--   - Qoo10: gmv_list_price_jpy_incl をそのまま (= orderPrice × qty、税込、顧客支払い額)
--   - メルカリ: fact 無し、Phase 2 で NE 連携で追加
--
-- 出力列:
--   mall                  TEXT  ('amazon'/'rakuten'/'yahoo'/'aupay'/'linegift'/'qoo10')
--   date_jst              TEXT  YYYY-MM-DD
--   sales_gross_jpy_incl  REAL  顧客支払額 (税込、手数料引かず)
--   units_net_sold        INTEGER 数量 (cancel 引いた後)
--   source_fact           TEXT  audit 用、参照元 fact テーブル名

DROP VIEW IF EXISTS v_mall_sales_daily_unified;
CREATE VIEW v_mall_sales_daily_unified AS
SELECT
  'amazon' AS mall,
  date_jst,
  ROUND(sales_principal_jpy * 1.10, 0) AS sales_gross_jpy_incl,
  units_net_sold,
  'f_amazon_finance_sku_daily_v1' AS source_fact
FROM f_amazon_finance_sku_daily_v1
UNION ALL
SELECT 'rakuten', date_jst, sales_principal_jpy_incl, units_net_sold, 'f_rakuten_finance_sku_daily_v1'
FROM f_rakuten_finance_sku_daily_v1
UNION ALL
SELECT 'yahoo', date_jst, sales_principal_jpy_incl, units_net_sold, 'f_yahoo_finance_sku_daily_v1'
FROM f_yahoo_finance_sku_daily_v1
UNION ALL
SELECT 'aupay', date_jst, sales_principal_jpy_incl, units_net_sold, 'f_aupay_finance_sku_daily_v1'
FROM f_aupay_finance_sku_daily_v1
UNION ALL
SELECT 'linegift', date_jst, sales_principal_jpy_incl, units_net_sold, 'f_linegift_finance_sku_daily_v1'
FROM f_linegift_finance_sku_daily_v1
UNION ALL
SELECT 'qoo10', date_jst, gmv_list_price_jpy_incl, units_net_sold, 'f_qoo10_finance_sku_daily_v1'
FROM f_qoo10_finance_sku_daily_v1;
