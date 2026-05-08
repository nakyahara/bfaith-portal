# Amazon Daily Finance Canonical Metric Mapping (v1)

**Status**: frozen (2026-05-08)
**Phase 1 ticket**: #1-0
**Owner**: Render Amazon Phase 1
**Source contract**: `docs/contracts/raw_amazon_settlement_lines.contract.md` (v1.1)
**Transaction taxonomy seed**: `docs/contracts/transaction_taxonomy_seed.csv`

このドキュメントは `raw_amazon_settlement_lines` の line を `f_amazon_finance_sku_daily_v1` の **canonical metric** に落とすための mapping rule を凍結したもの。Ticket #1-1 (`f_amazon_finance_sku_daily_v1` の rebuild SQL) はこの mapping を唯一の source-of-truth として参照する。

## 1. 確定済みの利益式 (再掲、Codex Round 6)

```
profit_amount =
    sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy
  − commission_jpy − fba_fulfillment_jpy − fba_storage_jpy − closing_fee_jpy
  − shipping_chargeback_jpy − giftwrap_chargeback_jpy − promotion_jpy
  − refund_principal_jpy
  + warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy
  − cogs_amount
```

`sales_tax_jpy` は **保持のみ、利益式に入れない** (税抜き利益で v4 と整合)。
`misc_fee_jpy` / `other_fee_jpy` / `other_amount_jpy` は **保持のみ、利益式に入れない** (Phase 1 では retention のみ、Phase 2 以降で利益式に組み込み判断)。

## 2. canonical metric 一覧 (DDL 列と完全一致)

| 列名 | 型 | 単位 | mapping source |
|---|---|---|---|
| `units_ordered` | REAL | 個 | order bucket transaction の `quantity_purchased` SUM |
| `units_refunded_customer` | REAL | 個 | refund bucket (excl. a_to_z) の `ABS(quantity_purchased)` SUM |
| `units_marketplace_guarantee` | REAL | 個 | (該当なし、常に 0) |
| `units_a_to_z_refund` | REAL | 個 | `transaction_type='A-to-z Guarantee Refund'` の `ABS(quantity_purchased)` SUM |
| `units_net_sold` | REAL | 個 | `units_ordered - units_refunded_customer - units_marketplace_guarantee - units_a_to_z_refund` (派生) |
| `sales_principal_jpy` | REAL | 円 | order bucket AND `price_type='Principal'` の `price_amount_micro / 1e6` SUM |
| `sales_shipping_jpy` | REAL | 円 | order bucket AND `price_type='Shipping'` の `price_amount_micro / 1e6` SUM |
| `sales_giftwrap_jpy` | REAL | 円 | order bucket AND `price_type='GiftWrap'` の `price_amount_micro / 1e6` SUM |
| `sales_tax_jpy` | REAL | 円 | `price_type IN ('Tax','ShippingTax','GiftWrapTax')` の `price_amount_micro / 1e6` SUM |
| `commission_jpy` | REAL | 円 | `item_related_fee_type='Commission'` の `ABS(item_related_fee_amount_micro) / 1e6` SUM |
| `fba_fulfillment_jpy` | REAL | 円 | `item_related_fee_type='FBAPerUnitFulfillmentFee'` の `ABS / 1e6` SUM |
| `fba_storage_jpy` | REAL | 円 | `transaction_type IN ('Storage Fee','StorageRenewalBilling','Storage Fee - Reversal','Storage Fee - Correction')` の `ABS(other_amount_micro) / 1e6` SUM |
| `closing_fee_jpy` | REAL | 円 | (該当なし、常に 0)。Round 6 想定の `VariableClosingFee` は実 DB に存在しない |
| `shipping_chargeback_jpy` | REAL | 円 | `item_related_fee_type='ShippingChargeback'` の `ABS / 1e6` SUM |
| `giftwrap_chargeback_jpy` | REAL | 円 | `item_related_fee_type='GiftwrapChargeback'` の `ABS / 1e6` SUM |
| `promotion_jpy` | REAL | 円 | `promotion_amount_micro IS NOT NULL` の `ABS(promotion_amount_micro) / 1e6` SUM (promotion_type は問わず) |
| `warehouse_damage_jpy` | REAL | 円 | `transaction_type IN ('WAREHOUSE_DAMAGE','WAREHOUSE_DAMAGE_EXCEPTION')` の `other_amount_micro / 1e6` SUM (符号そのまま) |
| `warehouse_lost_jpy` | REAL | 円 | `transaction_type='WAREHOUSE_LOST'` の `other_amount_micro / 1e6` SUM |
| `safe_t_jpy` | REAL | 円 | `transaction_type='SAFE-T Reimbursement'` の `other_amount_micro / 1e6` SUM |
| `refund_principal_jpy` | REAL | 円 | refund bucket AND `price_type='Principal'` の `ABS(price_amount_micro) / 1e6` SUM |
| `reversal_reimbursement_jpy` | REAL | 円 | `transaction_type IN ('REVERSAL_REIMBURSEMENT','Goodwill Concession','Fee Adjustment','Overpaid Fees Adjustment')` の `other_amount_micro / 1e6` SUM |
| `misc_fee_jpy` | REAL | 円 | `misc_fee_amount_micro / 1e6` SUM (現状 0、保持のみ) |
| `other_fee_jpy` | REAL | 円 | `other_fee_amount_micro / 1e6` SUM (現状 0、保持のみ) |
| `other_amount_jpy` | REAL | 円 | `other_amount_micro / 1e6` SUM (上記で抽出した transaction を除く)。FBM配送系 / RemovalComplete / Inbound Defect / Subscription / Service Fee / Easy Ship 等の保持用 |

## 3. transaction bucket (taxonomy_seed.csv より、再掲)

| bucket | transaction_type 一覧 | 用途 |
|---|---|---|
| **order** | `Order` | sales / units 起点 |
| **refund** | `Refund`, `A-to-z Guarantee Refund`, `Refund_Retrocharge`, `Order_Retrocharge` | 返金 |
| **reimbursement** | `WAREHOUSE_DAMAGE`, `WAREHOUSE_LOST`, `WAREHOUSE_DAMAGE_EXCEPTION`, `REVERSAL_REIMBURSEMENT`, `SAFE-T Reimbursement`, `Goodwill Concession` | 補填 |
| **fee** | `Amazon Easy Ship Charges`, `RemovalComplete`, `Inbound Defect Fee...`, `Subscription Fee`, `StorageRenewalBilling`, `Storage Fee`, `ServiceFee`, `Storage Fee - Reversal`, `Storage Fee - Correction` | 各種手数料 |
| **adjustment** | `Fee Adjustment`, `Overpaid Fees Adjustment` | 調整 |
| **reserve** | `BuyerRecharge`, `Previous Reserve Amount Balance`, `Current Reserve Amount` | 準備金 (利益計算から **除外**) |
| **__UNKNOWN__** | (新規 transaction_type 検出時のフォールバック) | DQ alert 対象 |

## 4. canonical metric への展開ロジック (擬似コード)

```sql
-- mapping CTE (Ticket #1-1 の rebuild SQL に組み込み)
WITH tx_bucket AS (
  SELECT
    rsl.*,
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
  WHERE economic_date IS NOT NULL
),
-- canonical metric への展開
mapped AS (
  SELECT
    economic_date AS date_jst,
    seller_sku_normalized AS seller_sku,
    tx_bucket,

    -- units (order bucket の Order trx だけが個数を持つ)
    CASE WHEN tx_bucket = 'order' THEN COALESCE(quantity_purchased, 0) ELSE 0 END AS units_ordered_contrib,
    CASE WHEN tx_bucket = 'refund' AND transaction_type <> 'A-to-z Guarantee Refund'
         THEN ABS(COALESCE(quantity_purchased, 0)) ELSE 0 END AS units_refunded_customer_contrib,
    CASE WHEN transaction_type = 'A-to-z Guarantee Refund'
         THEN ABS(COALESCE(quantity_purchased, 0)) ELSE 0 END AS units_a_to_z_contrib,

    -- sales (order bucket only)
    CASE WHEN tx_bucket = 'order' AND price_type = 'Principal'
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END / 1000000.0 AS sales_principal_jpy_contrib,
    CASE WHEN tx_bucket = 'order' AND price_type = 'Shipping'
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END / 1000000.0 AS sales_shipping_jpy_contrib,
    CASE WHEN tx_bucket = 'order' AND price_type = 'GiftWrap'
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END / 1000000.0 AS sales_giftwrap_jpy_contrib,
    CASE WHEN price_type IN ('Tax','ShippingTax','GiftWrapTax')
         THEN COALESCE(price_amount_micro, 0) ELSE 0 END / 1000000.0 AS sales_tax_jpy_contrib,

    -- fees (item_related_fee 経由)
    CASE WHEN item_related_fee_type = 'Commission'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END / 1000000.0 AS commission_jpy_contrib,
    CASE WHEN item_related_fee_type = 'FBAPerUnitFulfillmentFee'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END / 1000000.0 AS fba_fulfillment_jpy_contrib,
    CASE WHEN transaction_type IN ('Storage Fee','StorageRenewalBilling','Storage Fee - Reversal','Storage Fee - Correction')
         THEN ABS(COALESCE(other_amount_micro, 0)) ELSE 0 END / 1000000.0 AS fba_storage_jpy_contrib,
    0.0 AS closing_fee_jpy_contrib, -- VariableClosingFee は実 DB に存在しない、常に 0
    CASE WHEN item_related_fee_type = 'ShippingChargeback'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END / 1000000.0 AS shipping_chargeback_jpy_contrib,
    CASE WHEN item_related_fee_type = 'GiftwrapChargeback'
         THEN ABS(COALESCE(item_related_fee_amount_micro, 0)) ELSE 0 END / 1000000.0 AS giftwrap_chargeback_jpy_contrib,

    -- promotion (promotion_amount_micro 経由、promotion_type 不問)
    CASE WHEN promotion_amount_micro IS NOT NULL
         THEN ABS(COALESCE(promotion_amount_micro, 0)) ELSE 0 END / 1000000.0 AS promotion_jpy_contrib,

    -- refund principal (refund bucket AND price_type='Principal')
    CASE WHEN tx_bucket = 'refund' AND price_type = 'Principal'
         THEN ABS(COALESCE(price_amount_micro, 0)) ELSE 0 END / 1000000.0 AS refund_principal_jpy_contrib,

    -- reimbursement 系 (other_amount_micro 経由、符号そのまま)
    CASE WHEN transaction_type IN ('WAREHOUSE_DAMAGE','WAREHOUSE_DAMAGE_EXCEPTION')
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END / 1000000.0 AS warehouse_damage_jpy_contrib,
    CASE WHEN transaction_type = 'WAREHOUSE_LOST'
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END / 1000000.0 AS warehouse_lost_jpy_contrib,
    CASE WHEN transaction_type = 'SAFE-T Reimbursement'
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END / 1000000.0 AS safe_t_jpy_contrib,
    CASE WHEN transaction_type IN ('REVERSAL_REIMBURSEMENT','Goodwill Concession','Fee Adjustment','Overpaid Fees Adjustment')
         THEN COALESCE(other_amount_micro, 0) ELSE 0 END / 1000000.0 AS reversal_reimbursement_jpy_contrib,

    -- 保持のみ (利益式に入れない)
    COALESCE(misc_fee_amount_micro, 0) / 1000000.0 AS misc_fee_jpy_contrib,
    COALESCE(other_fee_amount_micro, 0) / 1000000.0 AS other_fee_jpy_contrib,
    -- other_amount_jpy は上記で抽出済の transaction を除外して保持
    CASE
      WHEN transaction_type IN (
        'WAREHOUSE_DAMAGE','WAREHOUSE_DAMAGE_EXCEPTION','WAREHOUSE_LOST','SAFE-T Reimbursement',
        'REVERSAL_REIMBURSEMENT','Goodwill Concession','Fee Adjustment','Overpaid Fees Adjustment',
        'Storage Fee','StorageRenewalBilling','Storage Fee - Reversal','Storage Fee - Correction'
      ) THEN 0
      WHEN tx_bucket = 'reserve' THEN 0
      ELSE COALESCE(other_amount_micro, 0)
    END / 1000000.0 AS other_amount_jpy_contrib

  FROM tx_bucket
  WHERE tx_bucket <> 'reserve'
)
-- daily aggregate
SELECT
  date_jst, seller_sku,
  SUM(units_ordered_contrib) AS units_ordered,
  SUM(units_refunded_customer_contrib) AS units_refunded_customer,
  0 AS units_marketplace_guarantee,
  SUM(units_a_to_z_contrib) AS units_a_to_z_refund,
  SUM(sales_principal_jpy_contrib) AS sales_principal_jpy,
  SUM(sales_shipping_jpy_contrib) AS sales_shipping_jpy,
  SUM(sales_giftwrap_jpy_contrib) AS sales_giftwrap_jpy,
  SUM(sales_tax_jpy_contrib) AS sales_tax_jpy,
  SUM(commission_jpy_contrib) AS commission_jpy,
  SUM(fba_fulfillment_jpy_contrib) AS fba_fulfillment_jpy,
  SUM(fba_storage_jpy_contrib) AS fba_storage_jpy,
  SUM(closing_fee_jpy_contrib) AS closing_fee_jpy,
  SUM(shipping_chargeback_jpy_contrib) AS shipping_chargeback_jpy,
  SUM(giftwrap_chargeback_jpy_contrib) AS giftwrap_chargeback_jpy,
  SUM(promotion_jpy_contrib) AS promotion_jpy,
  SUM(warehouse_damage_jpy_contrib) AS warehouse_damage_jpy,
  SUM(warehouse_lost_jpy_contrib) AS warehouse_lost_jpy,
  SUM(safe_t_jpy_contrib) AS safe_t_jpy,
  SUM(refund_principal_jpy_contrib) AS refund_principal_jpy,
  SUM(reversal_reimbursement_jpy_contrib) AS reversal_reimbursement_jpy,
  SUM(misc_fee_jpy_contrib) AS misc_fee_jpy,
  SUM(other_fee_jpy_contrib) AS other_fee_jpy,
  SUM(other_amount_jpy_contrib) AS other_amount_jpy
FROM mapped
GROUP BY date_jst, seller_sku;
```

## 5. 重要な設計判断

### 5-1. price_type の `RestockingFee` (49 件)

実 DB で観測したが、利益式に直接入る列がない。`other_fee_jpy` に保持 (Phase 2 で `restocking_fee_jpy` 列追加検討)。

### 5-2. price_type の `SAFE-T Reimbursement` (48 件)

`transaction_type='SAFE-T Reimbursement'` と紛らわしいが、これは line 内の price_type なので `safe_t_jpy` には別経路で入る (`other_amount_micro` 経由)。

### 5-3. item_related_fee_type の `MFNPostageFee` / `MFNPostageFeeTax` (各 53,251 件)

FBM 配送料関連。`commission_jpy` には入れず、`misc_fee_jpy` か別軸。Phase 1 では `other_fee_jpy` に集約 (item_related_fee_amount_micro なので別 SUM が必要)。**修正**: `item_related_fee_type='MFNPostageFee' OR 'MFNPostageFeeTax'` の `ABS(item_related_fee_amount_micro)` を `misc_fee_jpy` に加算する。

### 5-4. item_related_fee_type の `RefundCommission` (3,617 件)

Refund 時の commission 戻し。`commission_jpy` から差し引くか別列にするか。Phase 1 では `commission_jpy` から減算扱い (`commission_jpy_contrib` の SUM 後に `RefundCommission` の ABS を引く)。

### 5-5. item_related_fee_type の `PointsGranted` (3,278 件) / `PointsReturned` (33 件)

Amazon ポイント原資。`promotion_jpy` には入れず、Phase 1 では `other_fee_jpy` に保持。Phase 2 で `points_jpy` 列追加検討。

### 5-6. transaction_type の `RemovalComplete` (395 件)

FBA 在庫除去手数料。`other_amount_micro` 経由で `other_amount_jpy` に保持 (利益式に入れない)。

### 5-7. transaction_type の `Inbound Defect Fee...` (48 件)

入荷不良手数料。`other_amount_micro` 経由で `other_amount_jpy` に保持。

### 5-8. transaction_type の `Amazon Easy Ship Charges` (106,502 件) — 大物

FBM 配送代行手数料。Phase 1 では `other_amount_micro` 経由で `other_amount_jpy` に集約 (ただしこの transaction は seller_sku が NULL の可能性あるので要検証)。Phase 2 で `easy_ship_fee_jpy` 列追加が望ましい。

### 5-9. transaction_type の `Subscription Fee` / `ServiceFee` / `Inbound Defect Fee...` 等

サブスク / サービス手数料系。`other_amount_micro` 経由で `other_amount_jpy` に保持 (利益式に入れない、Phase 2 で運営費按分検討)。

## 6. unmapped 検出 / coverage 検証

`scripts/amazon-finance/check-metric-mapping.js` で実 DB に対して以下を計測:

1. **unmapped row 率**: tx_bucket が `__UNKNOWN__` になる行が全行の 0.5% 未満
2. **unmapped abs amount 率**: `__UNKNOWN__` の abs amount 合計が全 abs amount 合計の 0.1% 未満
3. **fixture test**: 主要 15 transaction pattern の canonical metric 判定が期待値と一致

## 7. mapping version

`canonical_metric_mapping_v1` で凍結。今後の transaction_type 追加 / 列追加で v2 / v3 と incrementing。Ticket #1-1 の fact build はこの version を参照する。

## 8. 改訂履歴

| Version | Date | Changes |
|---|---|---|
| v1 | 2026-05-08 | 初回凍結。`docs/contracts/transaction_taxonomy_seed.csv` (25 transaction_type) と `source_distinct_values_baseline_20260508.json` の実 DB 値ベースで mapping を確定。Codex Round 6 の前提と異なる点 (shipment_fee_type/order_fee_type 全行 NULL、other_fee_reason_description 全行 NULL、Amazon Easy Ship Charges 大量) を反映 |
