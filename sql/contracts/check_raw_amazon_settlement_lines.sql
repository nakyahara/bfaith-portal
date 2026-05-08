-- ============================================================
-- raw_amazon_settlement_lines schema / baseline check (v1)
-- ============================================================
-- Phase 1 ticket: #1-0a
-- 用途:
--   1) source contract v1 と実 DB の列定義が一致しているか
--   2) baseline metrics (row count / month distribution) を取得
--   3) transaction_type / price_type / 各 fee_type の distinct 値を抽出
--
-- 実行方法 (miniPC SSH 経由):
--   sqlite3 -readonly C:/Users/bfaith/bfaith-portal/data/warehouse.db \
--     < sql/contracts/check_raw_amazon_settlement_lines.sql
--
-- ローカル better-sqlite3 で実行する場合は assert-raw-amazon-source.js を使う。

.print '=== 1. テーブル存在確認 ==='
SELECT type, name
FROM sqlite_master
WHERE name IN (
  'raw_amazon_settlement_lines',
  'raw_amazon_settlement_headers',
  'fact_amazon_settlement_monthly_wide',
  'fact_amazon_settlement_monthly_long',
  'v_amazon_sku_profit_actual_v4',
  'v_amazon_sku_profit_actual',
  'v_amazon_settlement_unified',
  'v_settlement_v3_v4_validation',
  'v_sku_costed',
  'v_sku_resolved'
)
ORDER BY name;

.print ''
.print '=== 2. raw_amazon_settlement_lines 列定義 (列数を contract と突合) ==='
SELECT
  cid,
  name,
  type,
  "notnull" AS not_null,
  dflt_value
FROM pragma_table_info('raw_amazon_settlement_lines')
ORDER BY cid;

.print ''
.print '=== 3. baseline: total row count ==='
SELECT COUNT(*) AS total_rows FROM raw_amazon_settlement_lines;

.print ''
.print '=== 4. baseline: economic_date 月別 row count ==='
SELECT
  substr(economic_date, 1, 7) AS month_jst,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
WHERE economic_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

.print ''
.print '=== 5. baseline: economic_date 範囲 ==='
SELECT
  MIN(economic_date) AS min_economic_date,
  MAX(economic_date) AS max_economic_date,
  COUNT(DISTINCT economic_date) AS distinct_economic_date_count
FROM raw_amazon_settlement_lines
WHERE economic_date IS NOT NULL;

.print ''
.print '=== 6. economic_date と year_month_int の整合性 spot check (100 行) ==='
SELECT
  economic_date,
  year_month_int,
  CAST(strftime('%Y', economic_date) AS INTEGER) * 100
    + CAST(strftime('%m', economic_date) AS INTEGER) AS derived_year_month_int,
  CASE
    WHEN year_month_int = CAST(strftime('%Y', economic_date) AS INTEGER) * 100
                          + CAST(strftime('%m', economic_date) AS INTEGER)
    THEN 'OK'
    ELSE 'MISMATCH'
  END AS check_result
FROM raw_amazon_settlement_lines
WHERE economic_date IS NOT NULL
LIMIT 100;

.print ''
.print '=== 7. economic_date と year_month_int の整合性 全件集計 ==='
SELECT
  COUNT(*) AS total_rows_with_economic_date,
  SUM(
    CASE
      WHEN year_month_int = CAST(strftime('%Y', economic_date) AS INTEGER) * 100
                            + CAST(strftime('%m', economic_date) AS INTEGER)
      THEN 1
      ELSE 0
    END
  ) AS rows_year_month_match,
  SUM(
    CASE
      WHEN year_month_int <> CAST(strftime('%Y', economic_date) AS INTEGER) * 100
                             + CAST(strftime('%m', economic_date) AS INTEGER)
      THEN 1
      ELSE 0
    END
  ) AS rows_year_month_mismatch
FROM raw_amazon_settlement_lines
WHERE economic_date IS NOT NULL;

.print ''
.print '=== 8. 行ハッシュ重複確認 (physical_line_hash unique 性) ==='
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT physical_line_hash) AS distinct_physical_line_hash,
  COUNT(*) - COUNT(DISTINCT physical_line_hash) AS duplicate_hash_rows
FROM raw_amazon_settlement_lines;

.print ''
.print '=== 9. business_line_key の cardinality ==='
SELECT
  COUNT(DISTINCT business_line_key) AS distinct_business_line_key,
  COUNT(*) AS total_rows
FROM raw_amazon_settlement_lines;

.print ''
.print '=== 10. transaction_type の distinct 値 (件数付き) ==='
SELECT
  transaction_type,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY transaction_type
ORDER BY row_count DESC;

.print ''
.print '=== 11. price_type の distinct 値 (件数付き) ==='
SELECT
  COALESCE(price_type, '__NULL__') AS price_type,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY 1
ORDER BY row_count DESC;

.print ''
.print '=== 12. item_related_fee_type の distinct 値 ==='
SELECT
  COALESCE(item_related_fee_type, '__NULL__') AS item_related_fee_type,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY 1
ORDER BY row_count DESC;

.print ''
.print '=== 13. shipment_fee_type の distinct 値 ==='
SELECT
  COALESCE(shipment_fee_type, '__NULL__') AS shipment_fee_type,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY 1
ORDER BY row_count DESC;

.print ''
.print '=== 14. order_fee_type の distinct 値 ==='
SELECT
  COALESCE(order_fee_type, '__NULL__') AS order_fee_type,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY 1
ORDER BY row_count DESC;

.print ''
.print '=== 15. promotion_type の distinct 値 ==='
SELECT
  COALESCE(promotion_type, '__NULL__') AS promotion_type,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY 1
ORDER BY row_count DESC;

.print ''
.print '=== 16. other_fee_reason_description の distinct 値 (TOP 50) ==='
SELECT
  COALESCE(other_fee_reason_description, '__NULL__') AS other_fee_reason_description,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY 1
ORDER BY row_count DESC
LIMIT 50;

.print ''
.print '=== 17. 金額列の min/max (micro 単位の妥当性確認) ==='
SELECT
  MIN(price_amount_micro) AS min_price_micro,
  MAX(price_amount_micro) AS max_price_micro,
  MIN(item_related_fee_amount_micro) AS min_item_fee_micro,
  MAX(item_related_fee_amount_micro) AS max_item_fee_micro,
  MIN(shipment_fee_amount_micro) AS min_shipment_fee_micro,
  MAX(shipment_fee_amount_micro) AS max_shipment_fee_micro,
  MIN(order_fee_amount_micro) AS min_order_fee_micro,
  MAX(order_fee_amount_micro) AS max_order_fee_micro,
  MIN(promotion_amount_micro) AS min_promotion_micro,
  MAX(promotion_amount_micro) AS max_promotion_micro,
  MIN(other_amount_micro) AS min_other_amount_micro,
  MAX(other_amount_micro) AS max_other_amount_micro
FROM raw_amazon_settlement_lines;

.print ''
.print '=== 18. seller_sku_normalized の null/空文字率 ==='
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN seller_sku_normalized IS NULL THEN 1 ELSE 0 END) AS null_count,
  SUM(CASE WHEN trim(seller_sku_normalized) = '' THEN 1 ELSE 0 END) AS empty_count,
  ROUND(
    100.0 * SUM(CASE WHEN seller_sku_normalized IS NULL OR trim(seller_sku_normalized) = '' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0)
  , 4) AS null_or_empty_rate_pct
FROM raw_amazon_settlement_lines;

.print ''
.print '=== 19. currency 列の distinct (JPY 以外混入確認) ==='
SELECT
  currency,
  COUNT(*) AS row_count
FROM raw_amazon_settlement_lines
GROUP BY currency
ORDER BY row_count DESC;

.print ''
.print '=== 20. 1 economic_date × seller_sku_normalized の grain サンプリング (TOP 20) ==='
SELECT
  economic_date,
  seller_sku_normalized,
  COUNT(*) AS line_count,
  COUNT(DISTINCT transaction_type) AS distinct_tx_types
FROM raw_amazon_settlement_lines
WHERE economic_date IS NOT NULL
  AND seller_sku_normalized IS NOT NULL
GROUP BY 1, 2
ORDER BY line_count DESC
LIMIT 20;
