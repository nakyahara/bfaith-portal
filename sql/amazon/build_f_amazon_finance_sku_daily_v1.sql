-- ============================================================
-- f_amazon_finance_sku_daily_v1 BUILD SQL (A' 設計、silver dedup 込み)
-- ============================================================
-- Phase 1 ticket: #1-1 (Codex Round 6 + Round 8/9/10 反映)
--
-- 設計の芯:
--   1. silver dedup: (source_settlement_id, business_line_key) でユニーク化
--      理由: physical_line_hash unique でも、parser が同じ business line を
--           複数物理 row として生成 (Order × Principal で 1 注文 100 line 等)
--      既存実装: apps/warehouse/rebuild-amazon-settlement-mart.js L53-71
--   2. 5 系統正規化 (qty/price/fee/promotion/other)
--      qty 行は price_type/item_related_fee_type/promotion_type 全 NULL のみ
--   3. 月次 SKU 単価 (Principal price SUM / qty SUM) を refund 推定に使う
--   4. daily 集約 (economic_date, seller_sku) で snapshot 不変条件 (INSERT OR IGNORE)
--
-- バインドパラメータ:
--   :year_month_int  INTEGER   例: 202604
--   :build_date      TEXT      例: '2026-05-08'
--
-- 実行: better-sqlite3 で複数 statement 順次 exec、最後の INSERT は prepare/run。

-- ----------------------------
-- silver dedup を temp table に
-- ----------------------------
DROP TABLE IF EXISTS _silver_month_v1;

CREATE TEMP TABLE _silver_month_v1 AS
WITH dedup AS (
  SELECT l.*,
         ROW_NUMBER() OVER (
           PARTITION BY l.source_settlement_id, l.business_line_key
           ORDER BY CASE l.source_layer
                      WHEN 'sp_api_v1' THEN 1
                      WHEN 'manual_csv' THEN 2
                      ELSE 3
                    END,
                    l.ingested_at DESC
         ) AS rn
  FROM raw_amazon_settlement_lines l
  WHERE l.year_month_int = :year_month_int
    AND l.economic_date IS NOT NULL
    AND l.seller_sku_normalized IS NOT NULL
    AND TRIM(l.seller_sku_normalized) <> ''
    AND l.transaction_type NOT IN (
      'BuyerRecharge',
      'Previous Reserve Amount Balance',
      'Current Reserve Amount'
    )
)
SELECT
  economic_date AS date_jst,
  year_month_int,
  seller_sku_normalized AS seller_sku,
  source_layer,
  transaction_type,
  quantity_purchased,
  price_type, price_amount_micro,
  item_related_fee_type, item_related_fee_amount_micro,
  promotion_type, promotion_amount_micro,
  misc_fee_amount_micro, other_fee_amount_micro, other_amount_micro
FROM dedup
WHERE rn = 1;

CREATE INDEX _silver_month_v1_idx
  ON _silver_month_v1 (date_jst, seller_sku, transaction_type);

-- ----------------------------
-- INSERT 本体
-- ----------------------------
INSERT OR IGNORE INTO f_amazon_finance_sku_daily_v1 (
  date_jst, seller_sku, asin_norm, product_name,
  units_ordered, units_refunded_customer, units_marketplace_guarantee,
  units_a_to_z_refund, units_net_sold,
  sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy,
  commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy,
  shipping_chargeback_jpy, giftwrap_chargeback_jpy, promotion_jpy,
  warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy,
  refund_principal_jpy, reversal_reimbursement_jpy,
  misc_fee_jpy, other_fee_jpy, other_amount_jpy,
  unit_cost_snapshot, cost_snapshot_date_jst, latest_unit_cost_reference,
  cogs_amount, profit_amount,
  is_cost_complete, cost_status,
  source_layer_summary, source_row_count, built_at
)
WITH
-- 月次 SKU 単価 (refund qty 推定用)
unit_price_month AS (
  SELECT
    seller_sku,
    CAST(
      ROUND(
        SUM(CASE WHEN transaction_type = 'Order' AND price_type = 'Principal'
                 THEN COALESCE(price_amount_micro, 0) ELSE 0 END) * 1.0
        / NULLIF(
            SUM(CASE WHEN transaction_type = 'Order'
                          AND price_type IS NULL
                          AND item_related_fee_type IS NULL
                          AND promotion_type IS NULL
                     THEN COALESCE(quantity_purchased, 0) ELSE 0 END),
            0
          )
      ) AS INTEGER
    ) AS unit_price_micro
  FROM _silver_month_v1
  GROUP BY seller_sku
),
-- daily 集約 (raw 由来加算指標)
daily_base AS (
  SELECT
    s.date_jst,
    s.seller_sku,

    -- units (qty 専用行のみ集計)
    SUM(CASE WHEN s.transaction_type = 'Order'
                  AND s.price_type IS NULL
                  AND s.item_related_fee_type IS NULL
                  AND s.promotion_type IS NULL
             THEN COALESCE(s.quantity_purchased, 0) ELSE 0 END) AS units_ordered,

    -- sales (Order trx かつ price_type で絞る)
    SUM(CASE WHEN s.transaction_type = 'Order' AND s.price_type = 'Principal'
             THEN COALESCE(s.price_amount_micro, 0) ELSE 0 END) AS sales_principal_micro,
    SUM(CASE WHEN s.transaction_type = 'Order' AND s.price_type = 'Shipping'
             THEN COALESCE(s.price_amount_micro, 0) ELSE 0 END) AS sales_shipping_micro,
    SUM(CASE WHEN s.transaction_type = 'Order' AND s.price_type = 'GiftWrap'
             THEN COALESCE(s.price_amount_micro, 0) ELSE 0 END) AS sales_giftwrap_micro,
    SUM(CASE WHEN s.price_type IN ('Tax', 'ShippingTax', 'GiftWrapTax')
             THEN COALESCE(s.price_amount_micro, 0) ELSE 0 END) AS sales_tax_micro,

    -- commission (Commission - RefundCommission で正味)
    SUM(CASE WHEN s.item_related_fee_type = 'Commission'
             THEN ABS(COALESCE(s.item_related_fee_amount_micro, 0)) ELSE 0 END) AS commission_gross_micro,
    SUM(CASE WHEN s.item_related_fee_type = 'RefundCommission'
             THEN ABS(COALESCE(s.item_related_fee_amount_micro, 0)) ELSE 0 END) AS refund_commission_micro,

    -- FBA fulfillment / storage / chargeback
    SUM(CASE WHEN s.item_related_fee_type = 'FBAPerUnitFulfillmentFee'
             THEN ABS(COALESCE(s.item_related_fee_amount_micro, 0)) ELSE 0 END) AS fba_fulfillment_micro,
    SUM(CASE WHEN s.transaction_type IN ('Storage Fee', 'StorageRenewalBilling',
                                           'Storage Fee - Reversal', 'Storage Fee - Correction')
             THEN ABS(COALESCE(s.other_amount_micro, 0)) ELSE 0 END) AS fba_storage_micro,
    SUM(CASE WHEN s.item_related_fee_type = 'ShippingChargeback'
             THEN ABS(COALESCE(s.item_related_fee_amount_micro, 0)) ELSE 0 END) AS shipping_chargeback_micro,
    SUM(CASE WHEN s.item_related_fee_type = 'GiftwrapChargeback'
             THEN ABS(COALESCE(s.item_related_fee_amount_micro, 0)) ELSE 0 END) AS giftwrap_chargeback_micro,

    -- promotion
    SUM(CASE WHEN s.promotion_amount_micro IS NOT NULL
             THEN ABS(COALESCE(s.promotion_amount_micro, 0)) ELSE 0 END) AS promotion_micro,

    -- refund principal (customer + a_to_z 別集計)
    SUM(CASE WHEN s.transaction_type IN ('Refund', 'Refund_Retrocharge', 'Order_Retrocharge')
                  AND s.price_type = 'Principal'
             THEN ABS(COALESCE(s.price_amount_micro, 0)) ELSE 0 END) AS refund_principal_customer_micro,
    SUM(CASE WHEN s.transaction_type = 'A-to-z Guarantee Refund' AND s.price_type = 'Principal'
             THEN ABS(COALESCE(s.price_amount_micro, 0)) ELSE 0 END) AS refund_principal_atoz_micro,

    -- reimbursement (符号そのまま)
    SUM(CASE WHEN s.transaction_type IN ('WAREHOUSE_DAMAGE', 'WAREHOUSE_DAMAGE_EXCEPTION')
             THEN COALESCE(s.other_amount_micro, 0) ELSE 0 END) AS warehouse_damage_micro,
    SUM(CASE WHEN s.transaction_type = 'WAREHOUSE_LOST'
             THEN COALESCE(s.other_amount_micro, 0) ELSE 0 END) AS warehouse_lost_micro,
    SUM(CASE WHEN s.transaction_type = 'SAFE-T Reimbursement'
             THEN COALESCE(s.other_amount_micro, 0) ELSE 0 END) AS safe_t_micro,
    SUM(CASE WHEN s.transaction_type IN ('REVERSAL_REIMBURSEMENT', 'Goodwill Concession',
                                           'Fee Adjustment', 'Overpaid Fees Adjustment')
             THEN COALESCE(s.other_amount_micro, 0) ELSE 0 END) AS reversal_reimbursement_micro,

    -- 保持のみ (利益式に入れない)
    SUM(COALESCE(s.misc_fee_amount_micro, 0)) AS misc_fee_micro,
    SUM(
      COALESCE(s.other_fee_amount_micro, 0)
      + CASE WHEN s.item_related_fee_type IN ('MFNPostageFee', 'MFNPostageFeeTax', 'PointsGranted', 'PointsReturned')
             THEN ABS(COALESCE(s.item_related_fee_amount_micro, 0)) ELSE 0 END
    ) AS other_fee_micro,
    SUM(
      CASE
        WHEN s.transaction_type IN (
          'WAREHOUSE_DAMAGE', 'WAREHOUSE_DAMAGE_EXCEPTION', 'WAREHOUSE_LOST', 'SAFE-T Reimbursement',
          'REVERSAL_REIMBURSEMENT', 'Goodwill Concession', 'Fee Adjustment', 'Overpaid Fees Adjustment',
          'Storage Fee', 'StorageRenewalBilling', 'Storage Fee - Reversal', 'Storage Fee - Correction'
        ) THEN 0
        ELSE COALESCE(s.other_amount_micro, 0)
      END
    ) AS other_amount_micro,

    -- メタ
    GROUP_CONCAT(DISTINCT s.source_layer) AS source_layer_summary,
    COUNT(*) AS source_row_count

  FROM _silver_month_v1 s
  GROUP BY s.date_jst, s.seller_sku
),
-- refund qty 推定 (月次単価で割る)
refund_enriched AS (
  SELECT
    d.*,
    u.unit_price_micro,
    COALESCE(
      CAST(
        ROUND(d.refund_principal_customer_micro * 1.0 / NULLIF(u.unit_price_micro, 0)) AS INTEGER
      ),
      0
    ) AS units_refunded_customer,
    COALESCE(
      CAST(
        ROUND(d.refund_principal_atoz_micro * 1.0 / NULLIF(u.unit_price_micro, 0)) AS INTEGER
      ),
      0
    ) AS units_a_to_z_refund
  FROM daily_base d
  LEFT JOIN unit_price_month u ON u.seller_sku = d.seller_sku
),
-- snapshot cost (build 時点 v_sku_costed、SKU 単位)
cost_lookup AS (
  SELECT
    seller_sku,
    SUM(単価 * 数量) AS unit_cost_snapshot,
    SUM(CASE WHEN cost_status = 'ok' THEN 1 ELSE 0 END) AS ok_count,
    SUM(CASE WHEN cost_status IN ('ne_missing', 'cost_missing') THEN 1 ELSE 0 END) AS missing_count,
    COUNT(*) AS row_count
  FROM v_sku_costed
  GROUP BY seller_sku
)
SELECT
  r.date_jst,
  r.seller_sku,
  '' AS asin_norm,
  COALESCE(mp.商品名, '') AS product_name,

  -- units
  r.units_ordered,
  r.units_refunded_customer,
  0 AS units_marketplace_guarantee,
  r.units_a_to_z_refund,
  (r.units_ordered - r.units_refunded_customer - r.units_a_to_z_refund) AS units_net_sold,

  -- sales (micro → JPY)
  ROUND(r.sales_principal_micro / 1000000.0, 2) AS sales_principal_jpy,
  ROUND(r.sales_shipping_micro / 1000000.0, 2) AS sales_shipping_jpy,
  ROUND(r.sales_giftwrap_micro / 1000000.0, 2) AS sales_giftwrap_jpy,
  ROUND(r.sales_tax_micro / 1000000.0, 2) AS sales_tax_jpy,

  -- fees
  ROUND((r.commission_gross_micro - r.refund_commission_micro) / 1000000.0, 2) AS commission_jpy,
  ROUND(r.fba_fulfillment_micro / 1000000.0, 2) AS fba_fulfillment_jpy,
  ROUND(r.fba_storage_micro / 1000000.0, 2) AS fba_storage_jpy,
  0 AS closing_fee_jpy,
  ROUND(r.shipping_chargeback_micro / 1000000.0, 2) AS shipping_chargeback_jpy,
  ROUND(r.giftwrap_chargeback_micro / 1000000.0, 2) AS giftwrap_chargeback_jpy,
  ROUND(r.promotion_micro / 1000000.0, 2) AS promotion_jpy,

  -- reimbursement / refund
  ROUND(r.warehouse_damage_micro / 1000000.0, 2) AS warehouse_damage_jpy,
  ROUND(r.warehouse_lost_micro / 1000000.0, 2) AS warehouse_lost_jpy,
  ROUND(r.safe_t_micro / 1000000.0, 2) AS safe_t_jpy,
  ROUND((r.refund_principal_customer_micro + r.refund_principal_atoz_micro) / 1000000.0, 2) AS refund_principal_jpy,
  ROUND(r.reversal_reimbursement_micro / 1000000.0, 2) AS reversal_reimbursement_jpy,

  -- 保持のみ
  ROUND(r.misc_fee_micro / 1000000.0, 2) AS misc_fee_jpy,
  ROUND(r.other_fee_micro / 1000000.0, 2) AS other_fee_jpy,
  ROUND(r.other_amount_micro / 1000000.0, 2) AS other_amount_jpy,

  -- 原価 (build 時点 snapshot)
  c.unit_cost_snapshot,
  :build_date AS cost_snapshot_date_jst,
  c.unit_cost_snapshot AS latest_unit_cost_reference,
  ROUND(
    COALESCE(c.unit_cost_snapshot, 0)
    * (r.units_ordered - r.units_refunded_customer - r.units_a_to_z_refund),
    2
  ) AS cogs_amount,

  -- 利益 (税抜き、Codex Round 6 確定式)
  ROUND(
    r.sales_principal_micro / 1000000.0
    + r.sales_shipping_micro / 1000000.0
    + r.sales_giftwrap_micro / 1000000.0
    - (r.commission_gross_micro - r.refund_commission_micro) / 1000000.0
    - r.fba_fulfillment_micro / 1000000.0
    - r.fba_storage_micro / 1000000.0
    - 0  -- closing_fee_jpy
    - r.shipping_chargeback_micro / 1000000.0
    - r.giftwrap_chargeback_micro / 1000000.0
    - r.promotion_micro / 1000000.0
    - (r.refund_principal_customer_micro + r.refund_principal_atoz_micro) / 1000000.0
    + r.warehouse_damage_micro / 1000000.0
    + r.warehouse_lost_micro / 1000000.0
    + r.safe_t_micro / 1000000.0
    + r.reversal_reimbursement_micro / 1000000.0
    - COALESCE(c.unit_cost_snapshot, 0) * (r.units_ordered - r.units_refunded_customer - r.units_a_to_z_refund)
  , 2) AS profit_amount,

  -- 品質
  CASE WHEN c.unit_cost_snapshot IS NOT NULL AND c.missing_count = 0 THEN 1 ELSE 0 END AS is_cost_complete,
  CASE
    WHEN c.unit_cost_snapshot IS NULL THEN 'missing_cost'
    WHEN c.missing_count = 0 THEN 'complete'
    WHEN c.ok_count > 0 THEN 'partial_cost'
    ELSE 'missing_cost'
  END AS cost_status,

  -- メタ
  COALESCE(r.source_layer_summary, '') AS source_layer_summary,
  r.source_row_count,
  CURRENT_TIMESTAMP AS built_at

FROM refund_enriched r
LEFT JOIN cost_lookup c ON c.seller_sku = r.seller_sku
LEFT JOIN m_products mp ON mp.商品コード = r.seller_sku;

DROP TABLE IF EXISTS _silver_month_v1;
