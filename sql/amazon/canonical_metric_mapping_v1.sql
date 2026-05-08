-- ============================================================
-- Amazon Daily Finance Canonical Metric Mapping (v1)
-- ============================================================
-- Phase 1 ticket: #1-0
-- Source: raw_amazon_settlement_lines (contract: docs/contracts/raw_amazon_settlement_lines.contract.md)
-- 用途: line を canonical metric に展開する SQL の reference 実装
-- 注意: Ticket #1-1 (rebuild SQL) はこの mapping を組み込む。本ファイルは
--       人間が読める / SSH で coverage 検証できる standalone SQL として保持する。
--
-- 実行方法 (miniPC SSH):
--   sqlite3 -readonly C:/Users/bfaith/bfaith-portal/data/warehouse.db < sql/amazon/canonical_metric_mapping_v1.sql
-- (sqlite3 CLI が無い場合は scripts/amazon-finance/check-metric-mapping.js を使用)

-- ============================================================
-- Part A: tx_bucket 分類 + canonical metric 展開
-- ============================================================
.print '=== Part A: 月別 canonical metric 集計 (2026-04 のみ) ==='

WITH tx_bucket AS (
  SELECT
    rsl.economic_date,
    rsl.year_month_int,
    rsl.seller_sku_normalized,
    rsl.transaction_type,
    rsl.quantity_purchased,
    rsl.price_type,
    rsl.price_amount_micro,
    rsl.item_related_fee_type,
    rsl.item_related_fee_amount_micro,
    rsl.promotion_amount_micro,
    rsl.misc_fee_amount_micro,
    rsl.other_fee_amount_micro,
    rsl.other_amount_micro,
    CASE rsl.transaction_type
      WHEN 'Order' THEN 'order'
      WHEN 'Refund' THEN 'refund'
      WHEN 'A-to-z Guarantee Refund' THEN 'refund'
      WHEN 'Refund_Retrocharge' THEN 'refund'
      WHEN 'Order_Retrocharge' THEN 'refund'
      WHEN 'WAREHOUSE_DAMAGE' THEN 'reimbursement'
      WHEN 'WAREHOUSE_LOST' THEN 'reimbursement'
      WHEN 'WAREHOUSE_DAMAGE_EXCEPTION' THEN 'reimbursement'
      WHEN 'REVERSAL_REIMBURSEMENT' THEN 'reimbursement'
      WHEN 'SAFE-T Reimbursement' THEN 'reimbursement'
      WHEN 'Goodwill Concession' THEN 'reimbursement'
      WHEN 'Amazon Easy Ship Charges' THEN 'fee'
      WHEN 'RemovalComplete' THEN 'fee'
      WHEN 'Inbound Defect Fee - Barcode cannot be scanned' THEN 'fee'
      WHEN 'Subscription Fee' THEN 'fee'
      WHEN 'StorageRenewalBilling' THEN 'fee'
      WHEN 'Storage Fee' THEN 'fee'
      WHEN 'ServiceFee' THEN 'fee'
      WHEN 'Storage Fee - Reversal' THEN 'fee'
      WHEN 'Storage Fee - Correction' THEN 'fee'
      WHEN 'Fee Adjustment' THEN 'adjustment'
      WHEN 'Overpaid Fees Adjustment' THEN 'adjustment'
      WHEN 'BuyerRecharge' THEN 'reserve'
      WHEN 'Previous Reserve Amount Balance' THEN 'reserve'
      WHEN 'Current Reserve Amount' THEN 'reserve'
      ELSE '__UNKNOWN__'
    END AS tx_bucket
  FROM raw_amazon_settlement_lines rsl
  WHERE rsl.economic_date IS NOT NULL
    AND rsl.year_month_int = 202604  -- 例: 2026-04 のみ集計 (debug 用)
),
mapped AS (
  SELECT
    economic_date AS date_jst,
    seller_sku_normalized AS seller_sku,

    -- units
    SUM(CASE WHEN tx_bucket = 'order' THEN COALESCE(quantity_purchased, 0) ELSE 0 END) AS units_ordered,
    SUM(CASE WHEN tx_bucket = 'refund' AND transaction_type <> 'A-to-z Guarantee Refund'
         THEN ABS(COALESCE(quantity_purchased, 0)) ELSE 0 END) AS units_refunded_customer,
    SUM(CASE WHEN transaction_type = 'A-to-z Guarantee Refund'
         THEN ABS(COALESCE(quantity_purchased, 0)) ELSE 0 END) AS units_a_to_z_refund,

    -- sales (order bucket only)
    SUM(CASE WHEN tx_bucket = 'order' AND price_type = 'Principal'
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END) / 1000000.0 AS sales_principal_jpy,
    SUM(CASE WHEN tx_bucket = 'order' AND price_type = 'Shipping'
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END) / 1000000.0 AS sales_shipping_jpy,
    SUM(CASE WHEN tx_bucket = 'order' AND price_type = 'GiftWrap'
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END) / 1000000.0 AS sales_giftwrap_jpy,
    SUM(CASE WHEN price_type IN ('Tax','ShippingTax','GiftWrapTax')
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END) / 1000000.0 AS sales_tax_jpy,

    -- fees
    SUM(CASE WHEN item_related_fee_type = 'Commission'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS commission_jpy_gross,
    SUM(CASE WHEN item_related_fee_type = 'RefundCommission'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS refund_commission_jpy,
    SUM(CASE WHEN item_related_fee_type = 'FBAPerUnitFulfillmentFee'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS fba_fulfillment_jpy,
    SUM(CASE WHEN transaction_type IN ('Storage Fee','StorageRenewalBilling','Storage Fee - Reversal','Storage Fee - Correction')
         THEN ABS(COALESCE(other_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS fba_storage_jpy,
    0.0 AS closing_fee_jpy, -- 該当 type なし
    SUM(CASE WHEN item_related_fee_type = 'ShippingChargeback'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS shipping_chargeback_jpy,
    SUM(CASE WHEN item_related_fee_type = 'GiftwrapChargeback'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS giftwrap_chargeback_jpy,
    SUM(CASE WHEN promotion_amount_micro IS NOT NULL
         THEN ABS(COALESCE(promotion_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS promotion_jpy,
    SUM(CASE WHEN tx_bucket = 'refund' AND price_type = 'Principal'
         THEN ABS(COALESCE(price_amount_micro, 0)) ELSE 0 END) / 1000000.0 AS refund_principal_jpy,

    -- reimbursement
    SUM(CASE WHEN transaction_type IN ('WAREHOUSE_DAMAGE','WAREHOUSE_DAMAGE_EXCEPTION')
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END) / 1000000.0 AS warehouse_damage_jpy,
    SUM(CASE WHEN transaction_type = 'WAREHOUSE_LOST'
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END) / 1000000.0 AS warehouse_lost_jpy,
    SUM(CASE WHEN transaction_type = 'SAFE-T Reimbursement'
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END) / 1000000.0 AS safe_t_jpy,
    SUM(CASE WHEN transaction_type IN ('REVERSAL_REIMBURSEMENT','Goodwill Concession','Fee Adjustment','Overpaid Fees Adjustment')
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END) / 1000000.0 AS reversal_reimbursement_jpy,

    -- 保持のみ
    SUM(COALESCE(misc_fee_amount_micro, 0)) / 1000000.0 AS misc_fee_jpy,
    SUM(
      COALESCE(other_fee_amount_micro, 0)
      + CASE WHEN item_related_fee_type IN ('MFNPostageFee','MFNPostageFeeTax','PointsGranted','PointsReturned')
             THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END
    ) / 1000000.0 AS other_fee_jpy,
    SUM(
      CASE
        WHEN transaction_type IN (
          'WAREHOUSE_DAMAGE','WAREHOUSE_DAMAGE_EXCEPTION','WAREHOUSE_LOST','SAFE-T Reimbursement',
          'REVERSAL_REIMBURSEMENT','Goodwill Concession','Fee Adjustment','Overpaid Fees Adjustment',
          'Storage Fee','StorageRenewalBilling','Storage Fee - Reversal','Storage Fee - Correction'
        ) THEN 0
        WHEN tx_bucket = 'reserve' THEN 0
        ELSE COALESCE(other_amount_micro, 0)
      END
    ) / 1000000.0 AS other_amount_jpy

  FROM tx_bucket
  WHERE tx_bucket <> 'reserve'
  GROUP BY date_jst, seller_sku
)
SELECT
  date_jst,
  seller_sku,
  units_ordered,
  units_refunded_customer,
  units_a_to_z_refund,
  units_ordered - units_refunded_customer - units_a_to_z_refund AS units_net_sold,
  ROUND(sales_principal_jpy, 2) AS sales_principal_jpy,
  ROUND(sales_shipping_jpy, 2) AS sales_shipping_jpy,
  ROUND(sales_giftwrap_jpy, 2) AS sales_giftwrap_jpy,
  ROUND(sales_tax_jpy, 2) AS sales_tax_jpy,
  -- commission_jpy = gross commission − refund commission (RefundCommission は commission の戻し)
  ROUND(commission_jpy_gross - refund_commission_jpy, 2) AS commission_jpy,
  ROUND(fba_fulfillment_jpy, 2) AS fba_fulfillment_jpy,
  ROUND(fba_storage_jpy, 2) AS fba_storage_jpy,
  closing_fee_jpy,
  ROUND(shipping_chargeback_jpy, 2) AS shipping_chargeback_jpy,
  ROUND(giftwrap_chargeback_jpy, 2) AS giftwrap_chargeback_jpy,
  ROUND(promotion_jpy, 2) AS promotion_jpy,
  ROUND(refund_principal_jpy, 2) AS refund_principal_jpy,
  ROUND(warehouse_damage_jpy, 2) AS warehouse_damage_jpy,
  ROUND(warehouse_lost_jpy, 2) AS warehouse_lost_jpy,
  ROUND(safe_t_jpy, 2) AS safe_t_jpy,
  ROUND(reversal_reimbursement_jpy, 2) AS reversal_reimbursement_jpy,
  ROUND(misc_fee_jpy, 2) AS misc_fee_jpy,
  ROUND(other_fee_jpy, 2) AS other_fee_jpy,
  ROUND(other_amount_jpy, 2) AS other_amount_jpy,
  -- 利益式 (cogs 除く)
  ROUND(
    sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy
    - (commission_jpy_gross - refund_commission_jpy) - fba_fulfillment_jpy - fba_storage_jpy - closing_fee_jpy
    - shipping_chargeback_jpy - giftwrap_chargeback_jpy - promotion_jpy
    - refund_principal_jpy
    + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy
  , 2) AS profit_excl_cogs_jpy
FROM mapped
ORDER BY date_jst, seller_sku
LIMIT 30;

-- ============================================================
-- Part B: unmapped (UNKNOWN bucket) 検出
-- ============================================================
.print ''
.print '=== Part B: __UNKNOWN__ bucket の集計 ==='

SELECT
  transaction_type,
  COUNT(*) AS row_count,
  SUM(ABS(COALESCE(price_amount_micro, 0))
      + ABS(COALESCE(item_related_fee_amount_micro, 0))
      + ABS(COALESCE(promotion_amount_micro, 0))
      + ABS(COALESCE(other_amount_micro, 0))) / 1000000.0 AS abs_amount_total_jpy
FROM raw_amazon_settlement_lines
WHERE transaction_type NOT IN (
  'Order','Refund','A-to-z Guarantee Refund','Refund_Retrocharge','Order_Retrocharge',
  'WAREHOUSE_DAMAGE','WAREHOUSE_LOST','WAREHOUSE_DAMAGE_EXCEPTION','REVERSAL_REIMBURSEMENT',
  'SAFE-T Reimbursement','Goodwill Concession',
  'Amazon Easy Ship Charges','RemovalComplete','Inbound Defect Fee - Barcode cannot be scanned',
  'Subscription Fee','StorageRenewalBilling','Storage Fee','ServiceFee',
  'Storage Fee - Reversal','Storage Fee - Correction',
  'Fee Adjustment','Overpaid Fees Adjustment',
  'BuyerRecharge','Previous Reserve Amount Balance','Current Reserve Amount'
)
GROUP BY transaction_type
ORDER BY row_count DESC;

-- ============================================================
-- Part C: coverage 計測 (全 transaction_type を mapping 済か)
-- ============================================================
.print ''
.print '=== Part C: bucket 別件数 / abs amount ==='

WITH tx_bucket AS (
  SELECT
    transaction_type,
    CASE rsl.transaction_type
      WHEN 'Order' THEN 'order'
      WHEN 'Refund' THEN 'refund'
      WHEN 'A-to-z Guarantee Refund' THEN 'refund'
      WHEN 'Refund_Retrocharge' THEN 'refund'
      WHEN 'Order_Retrocharge' THEN 'refund'
      WHEN 'WAREHOUSE_DAMAGE' THEN 'reimbursement'
      WHEN 'WAREHOUSE_LOST' THEN 'reimbursement'
      WHEN 'WAREHOUSE_DAMAGE_EXCEPTION' THEN 'reimbursement'
      WHEN 'REVERSAL_REIMBURSEMENT' THEN 'reimbursement'
      WHEN 'SAFE-T Reimbursement' THEN 'reimbursement'
      WHEN 'Goodwill Concession' THEN 'reimbursement'
      WHEN 'Amazon Easy Ship Charges' THEN 'fee'
      WHEN 'RemovalComplete' THEN 'fee'
      WHEN 'Inbound Defect Fee - Barcode cannot be scanned' THEN 'fee'
      WHEN 'Subscription Fee' THEN 'fee'
      WHEN 'StorageRenewalBilling' THEN 'fee'
      WHEN 'Storage Fee' THEN 'fee'
      WHEN 'ServiceFee' THEN 'fee'
      WHEN 'Storage Fee - Reversal' THEN 'fee'
      WHEN 'Storage Fee - Correction' THEN 'fee'
      WHEN 'Fee Adjustment' THEN 'adjustment'
      WHEN 'Overpaid Fees Adjustment' THEN 'adjustment'
      WHEN 'BuyerRecharge' THEN 'reserve'
      WHEN 'Previous Reserve Amount Balance' THEN 'reserve'
      WHEN 'Current Reserve Amount' THEN 'reserve'
      ELSE '__UNKNOWN__'
    END AS tx_bucket,
    ABS(COALESCE(price_amount_micro, 0))
      + ABS(COALESCE(item_related_fee_amount_micro, 0))
      + ABS(COALESCE(promotion_amount_micro, 0))
      + ABS(COALESCE(other_amount_micro, 0)) AS abs_micro
  FROM raw_amazon_settlement_lines rsl
)
SELECT
  tx_bucket,
  COUNT(*) AS row_count,
  ROUND(SUM(abs_micro) / 1000000.0, 0) AS abs_amount_jpy
FROM tx_bucket
GROUP BY tx_bucket
ORDER BY row_count DESC;
