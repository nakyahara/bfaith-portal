-- check_raw_rakuten_orders.sql
-- raw_rakuten_orders source contract v1 検査 SQL
-- ticket: Phase 1a #R-0a
-- 詳細: docs/contracts/raw_rakuten_orders.contract.md
--
-- 使い方:
--   sqlite3 data/warehouse.db < sql/contracts/check_raw_rakuten_orders.sql
--   結果を docs/contracts/source_distinct_values_baseline_rakuten_<DATE>.json に保存
--
-- このファイルは「raw_rakuten_orders の現在の状態を一発で把握する」目的の
-- 検査クエリ集合。assert は別途 scripts/contracts/assert-rakuten-source.js で。

-- ==================================================
-- 1. 行数 / 期間
-- ==================================================
.headers on
.mode column
.print '\n=== 1. 全体統計 ==='
SELECT
  COUNT(*)                                 AS total_rows,
  COUNT(DISTINCT order_number)             AS distinct_orders,
  MIN(order_date)                          AS min_date,
  MAX(order_date)                          AS max_date,
  ROUND(AVG(units), 2)                     AS avg_units_per_item
FROM raw_rakuten_orders;

-- ==================================================
-- 2. 列存在確認 (21 列、PRAGMA で取得)
-- ==================================================
.print '\n=== 2. 列定義 (21 列必須) ==='
SELECT name AS column_name, type, "notnull", dflt_value
FROM pragma_table_info('raw_rakuten_orders')
ORDER BY cid;

-- ==================================================
-- 3. order_status 分布
-- ==================================================
.print '\n=== 3. order_status 分布 (700=完了 / 900=キャンセル) ==='
SELECT
  order_status,
  COUNT(*) AS rows,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_rakuten_orders), 2) AS pct
FROM raw_rakuten_orders
GROUP BY order_status
ORDER BY rows DESC;

-- ==================================================
-- 4. delete_item_flag 分布 (全件 0 期待)
-- ==================================================
.print '\n=== 4. delete_item_flag (全件 0 期待) ==='
SELECT delete_item_flag, COUNT(*) AS rows
FROM raw_rakuten_orders
GROUP BY delete_item_flag
ORDER BY rows DESC;

-- ==================================================
-- 5. selected_choice カバレッジ (約 36.5% に値あり想定)
-- ==================================================
.print '\n=== 5. selected_choice カバレッジ ==='
SELECT
  CASE WHEN selected_choice IS NULL OR selected_choice = '' THEN 'empty' ELSE 'has_value' END AS state,
  COUNT(*) AS rows,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_rakuten_orders), 2) AS pct
FROM raw_rakuten_orders
GROUP BY state;

-- ==================================================
-- 6. selected_choice 起因の price_tax_incl バラつき (microscale 想定)
-- ==================================================
.print '\n=== 6. 同 item_number で複数 price_tax_incl (selected_choice 影響想定、microscale) ==='
SELECT
  COUNT(*) AS items_with_price_variance,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT item_number) FROM raw_rakuten_orders), 4) AS pct_of_distinct_items
FROM (
  SELECT item_number
  FROM raw_rakuten_orders
  WHERE order_date >= date('now', '-90 days')
  GROUP BY item_number
  HAVING COUNT(DISTINCT price_tax_incl) > 1
);

-- ==================================================
-- 7. tax_rate 分布 (0.10 / 0.08 のみ期待)
-- ==================================================
.print '\n=== 7. tax_rate 分布 (0.10 / 0.08 のみ想定) ==='
SELECT tax_rate, COUNT(*) AS rows
FROM raw_rakuten_orders
GROUP BY tax_rate
ORDER BY rows DESC;

-- ==================================================
-- 8. 月別件数 (直近 12 ヶ月、トレンド把握)
-- ==================================================
.print '\n=== 8. 月別件数 (直近 12 ヶ月) ==='
SELECT
  substr(order_date, 1, 7) AS month_jst,
  COUNT(*) AS rows,
  COUNT(DISTINCT order_number) AS orders,
  SUM(CASE WHEN order_status = 700 THEN 1 ELSE 0 END) AS rows_status_700,
  SUM(CASE WHEN order_status = 900 THEN 1 ELSE 0 END) AS rows_status_900
FROM raw_rakuten_orders
WHERE order_date >= date('now', '-12 months')
GROUP BY month_jst
ORDER BY month_jst DESC
LIMIT 12;

-- ==================================================
-- 9. NULL 率 (主要列)
-- ==================================================
.print '\n=== 9. 主要列 NULL 率 ==='
SELECT
  ROUND(SUM(CASE WHEN order_number IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS null_order_number_pct,
  ROUND(SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS null_order_date_pct,
  ROUND(SUM(CASE WHEN item_number IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS null_item_number_pct,
  ROUND(SUM(CASE WHEN price_tax_incl IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS null_price_tax_incl_pct,
  ROUND(SUM(CASE WHEN units IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS null_units_pct,
  ROUND(SUM(CASE WHEN tax_rate IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS null_tax_rate_pct
FROM raw_rakuten_orders;

-- ==================================================
-- 10. f_rakuten_sku_map カバー率 (rakuten_code → ne_code 解決率)
-- ==================================================
-- Codex PR #75 #2 対応: f_rakuten_sku_map.rakuten_code は一意でない可能性があるため
-- LEFT JOIN で 1:N 増殖して resolved_pct が 100% 超になり得る。EXISTS で 0/1 化する。
.print '\n=== 10. f_rakuten_sku_map カバー率 (raw_rakuten_orders.item_number → ne_code 解決) ==='
WITH src AS (SELECT DISTINCT item_number FROM raw_rakuten_orders WHERE order_status = 700)
SELECT
  COUNT(*) AS distinct_rakuten_codes,
  SUM(CASE WHEN EXISTS (SELECT 1 FROM f_rakuten_sku_map m WHERE m.rakuten_code = src.item_number) THEN 1 ELSE 0 END) AS resolved,
  SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM f_rakuten_sku_map m WHERE m.rakuten_code = src.item_number) THEN 1 ELSE 0 END) AS unresolved,
  ROUND(SUM(CASE WHEN EXISTS (SELECT 1 FROM f_rakuten_sku_map m WHERE m.rakuten_code = src.item_number) THEN 1 ELSE 0 END) * 100.0 /
        COUNT(*), 2) AS resolved_pct
FROM src;

-- 参考: f_rakuten_sku_map の rakuten_code 1:N 件数 (ne_code 重複あれば R-1 で要対処)
.print '  -- 参考: rakuten_code が複数 ne_code に紐付く件数 --'
SELECT COUNT(*) AS rakuten_codes_with_multiple_ne FROM (
  SELECT rakuten_code FROM f_rakuten_sku_map GROUP BY rakuten_code HAVING COUNT(DISTINCT ne_code) > 1
);

-- ==================================================
-- 11. fact_promotion_cost (mall=rakuten) 整合性
-- ==================================================
.print '\n=== 11. fact_promotion_cost (mall=rakuten) ==='
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT 注文番号) AS distinct_orders,
  COUNT(DISTINCT モール商品コード) AS distinct_skus,
  ROUND(SUM(クーポン金額), 0) AS sum_coupon_shop,
  ROUND(SUM(全店クーポン金額), 0) AS sum_coupon_all,
  ROUND(SUM(プロモーション金額), 0) AS sum_promotion
FROM fact_promotion_cost
WHERE モール = 'rakuten';

-- ==================================================
-- 12. fact_returns (mall=rakuten) 整合性
-- ==================================================
.print '\n=== 12. fact_returns (mall=rakuten) — 注文日 = 返品日 想定 ==='
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT 注文番号) AS distinct_orders,
  SUM(CASE WHEN substr(返品日, 1, 7) <> substr(注文日, 1, 7) THEN 1 ELSE 0 END) AS cross_month_count,
  ROUND(AVG(julianday(返品日) - julianday(注文日)), 2) AS avg_diff_days,
  ROUND(SUM(返金額), 0) AS sum_refund_amount,
  ROUND(SUM(数量), 0) AS sum_units_returned
FROM fact_returns
WHERE モール = 'rakuten' AND 返品日 IS NOT NULL AND 注文日 IS NOT NULL;
