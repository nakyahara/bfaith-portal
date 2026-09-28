-- ============================================================
-- f_amazon_finance_sku_daily_v1 BUILD SQL (A' 設計、silver dedup 込み)
-- ============================================================
-- Phase 1 ticket: #1-1 (Codex Round 6 + Round 8/9/10 反映)
--
-- 設計の芯:
--   1. silver dedup: (source_settlement_id, business_line_key, 同じ文書の中の出現順) でユニーク化
--      (2026-09-28 に出現順を足した: 同じ鍵の行は本物の別々の行 = 下の CREATE TEMP TABLE の注記)
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

-- 2026-09-28: 同じ決済の中で business_line_key が同じ行は parser の重複ではなく本物の別々の行だった
--   (全部の行の合計が振込額と 1 円まで一致・(決済, 鍵) で 1 行にすると 2 週間ごとに 55〜65 万円少ない)
--   → 同じ文書の中の出現順 (occ) を鍵に足す。db.js の v_amazon_settlement_unified と同じ形
--   (絞り込みは鍵に含まれる列だけ = 同じ鍵の行は全部残る = 出現順は崩れない)
CREATE TEMP TABLE _silver_month_v1 AS
WITH occ AS (
  SELECT l.*,
         DENSE_RANK() OVER (PARTITION BY l.source_settlement_id, l.business_line_key, l.source_document_id ORDER BY l.source_line_no) AS occ
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
),
dedup AS (
  SELECT l.*,
         ROW_NUMBER() OVER (
           PARTITION BY l.source_settlement_id, l.business_line_key, l.occ
           ORDER BY CASE l.source_layer
                      WHEN 'sp_api_v1' THEN 1
                      WHEN 'sp_api_v2' THEN 1
                      WHEN 'manual_csv' THEN 2
                      ELSE 3
                    END,
                    l.ingested_at DESC,
                    l.source_document_id
         ) AS rn
  FROM occ l
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
  misc_fee_amount_micro, other_fee_amount_micro, other_amount_micro,
  CAST(0 AS INTEGER) AS easy_ship_alloc_micro
FROM dedup
WHERE rn = 1;

-- ----------------------------
-- Easy Ship の配送料を SKU に割り振る (2026-09-28)
-- ----------------------------
-- 決済の Amazon Easy Ship Charges は SKU の無い行 (注文番号だけ) = 上の silver に入らず、SKU 別の利益に入っていなかった
-- (アカウント単位の手数料にもカスタム経費にも無く、Amazon 分析の確定利益が月 130〜230 万円多く出ていた)
-- 割り振り: その月 (料金の日の月) の料金 → 同じ注文番号の売上の行 (transaction_type = Order・SKU あり・どの月でも) の SKU へ
--   複数 SKU の注文は本体売上 (Principal) の割合・本体の合計が 0 以下なら SKU の数で等分・日付は料金の日
--   売上の行が無い注文 (まだ届いていない等) は割り振らない
--   1 円単位で割り振り、端数は小数部の大きい SKU から 1 円ずつ (料金ごとの合計が必ず元の額と一致)
-- 🚨 easy_ship_jpy は SKU ごとの利益を見るための列 = profit_amount から引かない (月の Easy Ship は全部アカウント単位の手数料で引く)
-- 金額は other-amount (古い月) と item-related-fee-amount (新しい月) の両方。重複除去は silver と同じ出現順つき
DROP TABLE IF EXISTS _easyship_alloc_v1;

CREATE TEMP TABLE _easyship_alloc_v1 AS
WITH es_occ AS (
  SELECT l.*,
         DENSE_RANK() OVER (PARTITION BY l.source_settlement_id, l.business_line_key, l.source_document_id ORDER BY l.source_line_no) AS occ
  FROM raw_amazon_settlement_lines l
  WHERE l.year_month_int = :year_month_int
    AND l.economic_date IS NOT NULL
    AND l.transaction_type = 'Amazon Easy Ship Charges'
),
es AS MATERIALIZED (
  SELECT l.economic_date, l.amazon_order_id, l.id AS charge_id,
         COALESCE(l.other_amount_micro, 0) + COALESCE(l.item_related_fee_amount_micro, 0) AS amt,
         ROW_NUMBER() OVER (
           PARTITION BY l.source_settlement_id, l.business_line_key, l.occ
           ORDER BY CASE l.source_layer
                      WHEN 'sp_api_v1' THEN 1
                      WHEN 'sp_api_v2' THEN 1
                      WHEN 'manual_csv' THEN 2
                      ELSE 3
                    END,
                    l.ingested_at DESC,
                    l.source_document_id
         ) AS rn
  FROM es_occ l
),
od_occ AS (
  SELECT l.*,
         DENSE_RANK() OVER (PARTITION BY l.source_settlement_id, l.business_line_key, l.source_document_id ORDER BY l.source_line_no) AS occ
  FROM raw_amazon_settlement_lines l INDEXED BY idx_settle_lines_order
  WHERE l.amazon_order_id IN (SELECT amazon_order_id FROM es WHERE rn = 1 AND amazon_order_id IS NOT NULL)
    AND l.transaction_type = 'Order'
    AND l.seller_sku_normalized IS NOT NULL
    AND TRIM(l.seller_sku_normalized) <> ''
),
od AS (
  SELECT l.amazon_order_id, l.seller_sku_normalized AS seller_sku, l.price_type, l.price_amount_micro,
         ROW_NUMBER() OVER (
           PARTITION BY l.source_settlement_id, l.business_line_key, l.occ
           ORDER BY CASE l.source_layer
                      WHEN 'sp_api_v1' THEN 1
                      WHEN 'sp_api_v2' THEN 1
                      WHEN 'manual_csv' THEN 2
                      ELSE 3
                    END,
                    l.ingested_at DESC,
                    l.source_document_id
         ) AS rn
  FROM od_occ l
),
w AS (
  SELECT amazon_order_id, seller_sku,
         SUM(CASE WHEN price_type = 'Principal' THEN COALESCE(price_amount_micro, 0) ELSE 0 END) AS principal
  FROM od WHERE rn = 1
  GROUP BY amazon_order_id, seller_sku
),
wt AS (
  SELECT w.*, SUM(principal) OVER (PARTITION BY amazon_order_id) AS total, COUNT(*) OVER (PARTITION BY amazon_order_id) AS n_sku
  FROM w
),
share AS (
  SELECT es.charge_id, es.economic_date, es.amt, wt.seller_sku,
         ABS(es.amt) / 1000000.0 * CASE WHEN wt.total > 0 THEN wt.principal * 1.0 / wt.total ELSE 1.0 / wt.n_sku END AS yen_exact
  FROM es JOIN wt ON wt.amazon_order_id = es.amazon_order_id
  WHERE es.rn = 1
),
base AS (
  SELECT share.*, CAST(yen_exact AS INTEGER) AS yen_floor,
         ROUND(ABS(amt) / 1000000.0) - SUM(CAST(yen_exact AS INTEGER)) OVER (PARTITION BY charge_id) AS remainder_yen,
         ROW_NUMBER() OVER (PARTITION BY charge_id ORDER BY yen_exact - CAST(yen_exact AS INTEGER) DESC, seller_sku) AS frac_rank
  FROM share
)
SELECT economic_date AS date_jst, seller_sku,
       (CASE WHEN amt < 0 THEN -1 ELSE 1 END) * (yen_floor + CASE WHEN frac_rank <= remainder_yen THEN 1 ELSE 0 END) * 1000000 AS alloc_micro
FROM base;

INSERT INTO _silver_month_v1 (date_jst, year_month_int, seller_sku, source_layer, transaction_type, easy_ship_alloc_micro)
SELECT date_jst, :year_month_int, seller_sku, 'easy_ship_alloc', 'Amazon Easy Ship Charges', SUM(alloc_micro)
FROM _easyship_alloc_v1
GROUP BY date_jst, seller_sku;

DROP TABLE IF EXISTS _easyship_alloc_v1;

CREATE INDEX _silver_month_v1_idx
  ON _silver_month_v1 (date_jst, seller_sku, transaction_type);

-- ----------------------------
-- INSERT 本体 (UPSERT、Codex Round 2 #1 対応)
-- ----------------------------
-- snapshot 不変条件を SQL レベルで保証:
--   ON CONFLICT で既存 row があれば snapshot 列 (unit_cost_snapshot, cost_snapshot_date_jst)
--   と built_at 以外の列を更新する。snapshot 列は既存値を温存。
--   cogs_amount は「既存 snapshot 原価 × 新 units」で再計算 (refund/adjustment が後追いで来るケース対応)。
--   profit_amount も同様に「新 sales/fees - 再計算 cogs + 新 reimbursement」。
INSERT INTO f_amazon_finance_sku_daily_v1 (
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
  source_layer_summary, source_row_count, built_at,
  easy_ship_jpy, promotion_tax_jpy
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

    -- 手数料・値引き = 符号つきの正味を反転 (費用を正・戻りを負。2026-09-29 Codex #1522 R1 High)
    --   前は行ごとに ABS を取っていた → 返品で戻る販売手数料 (+) を費用に数え、返品の管理手数料 (RefundCommission −) を費用から引いていた
    --   (チャージバック・値引きの戻り (+) も費用に数えていた) = 利益が月に 10〜20 万円少なかった
    --   正味 = −Σ(決済の額)。返品だけの日は負 (= 戻りの分だけ利益が増える) になりうる
    -- commission (Commission + RefundCommission の正味)
    -SUM(CASE WHEN s.item_related_fee_type IN ('Commission', 'RefundCommission')
              THEN COALESCE(s.item_related_fee_amount_micro, 0) ELSE 0 END) AS commission_micro,

    -- FBA fulfillment / storage / chargeback
    -SUM(CASE WHEN s.item_related_fee_type = 'FBAPerUnitFulfillmentFee'
              THEN COALESCE(s.item_related_fee_amount_micro, 0) ELSE 0 END) AS fba_fulfillment_micro,
    -SUM(CASE WHEN s.transaction_type IN ('Storage Fee', 'StorageRenewalBilling',
                                            'Storage Fee - Reversal', 'Storage Fee - Correction')
              THEN COALESCE(s.other_amount_micro, 0) ELSE 0 END) AS fba_storage_micro,
    -SUM(CASE WHEN s.item_related_fee_type = 'ShippingChargeback'
              THEN COALESCE(s.item_related_fee_amount_micro, 0) ELSE 0 END) AS shipping_chargeback_micro,
    -SUM(CASE WHEN s.item_related_fee_type = 'GiftwrapChargeback'
              THEN COALESCE(s.item_related_fee_amount_micro, 0) ELSE 0 END) AS giftwrap_chargeback_micro,

    -- promotion
    -SUM(COALESCE(s.promotion_amount_micro, 0)) AS promotion_micro,
    -- うち消費税の分 (TaxDiscount・2026-09-29。税抜の利益では値引きから除く)
    -SUM(CASE WHEN s.promotion_type = 'TaxDiscount'
              THEN COALESCE(s.promotion_amount_micro, 0) ELSE 0 END) AS promotion_tax_micro,

    -- refund principal (customer + a_to_z 別集計。返品数の推定にも使う = 本体だけ)
    --   2026-09-29: 符号つきの正味を反転
    --   カードの支払い取り消し (Chargeback Refund) は商品が戻らない = 返品数に入れない (入れると原価まで戻って利益が多く出る。Codex #1522 R2)
    --   → 本体の額は下の refund_other_micro (返金には入る・返品数の推定には使わない)
    -SUM(CASE WHEN s.transaction_type IN ('Refund', 'Refund_Retrocharge', 'Order_Retrocharge')
                   AND s.price_type = 'Principal'
              THEN COALESCE(s.price_amount_micro, 0) ELSE 0 END) AS refund_principal_customer_micro,
    -SUM(CASE WHEN s.transaction_type = 'A-to-z Guarantee Refund' AND s.price_type = 'Principal'
              THEN COALESCE(s.price_amount_micro, 0) ELSE 0 END) AS refund_principal_atoz_micro,
    -- 返品数の推定に使わない返金 (2026-09-29 から返金に入れる):
    --   送料・ギフト包装の返金 (−)・返品の手数料 RestockingFee (店に残る +)
    --   (返品のときは送料のチャージバックと送料の値引きも戻る (+) = 上の符号つきの正味で利益が増える。送料の返金 (−) を入れないと その分だけ利益が多い)
    --   + カードの支払い取り消し (Chargeback Refund) の本体 (前は数えていなかった・商品は戻らない)
    -SUM(CASE WHEN (s.transaction_type IN ('Refund', 'Refund_Retrocharge', 'Order_Retrocharge', 'Chargeback Refund', 'A-to-z Guarantee Refund')
                    AND s.price_type IN ('Shipping', 'GiftWrap', 'RestockingFee'))
                OR (s.transaction_type = 'Chargeback Refund' AND s.price_type = 'Principal')
              THEN COALESCE(s.price_amount_micro, 0) ELSE 0 END) AS refund_other_micro,

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

    -- Easy Ship の配送料 (割り振った行だけ。費用を正に = 符号を反転した正味)
    -SUM(COALESCE(s.easy_ship_alloc_micro, 0)) AS easy_ship_micro,

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
  ROUND(r.commission_micro / 1000000.0, 2) AS commission_jpy,
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
  ROUND((r.refund_principal_customer_micro + r.refund_principal_atoz_micro + r.refund_other_micro) / 1000000.0, 2) AS refund_principal_jpy,
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
    - r.commission_micro / 1000000.0
    - r.fba_fulfillment_micro / 1000000.0
    - r.fba_storage_micro / 1000000.0
    - 0  -- closing_fee_jpy
    - r.shipping_chargeback_micro / 1000000.0
    - r.giftwrap_chargeback_micro / 1000000.0
    - r.promotion_micro / 1000000.0
    - (r.refund_principal_customer_micro + r.refund_principal_atoz_micro + r.refund_other_micro) / 1000000.0
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
  CURRENT_TIMESTAMP AS built_at,
  ROUND(r.easy_ship_micro / 1000000.0, 2) AS easy_ship_jpy,
  ROUND(r.promotion_tax_micro / 1000000.0, 2) AS promotion_tax_jpy

FROM refund_enriched r
LEFT JOIN cost_lookup c ON c.seller_sku = r.seller_sku
LEFT JOIN m_products mp ON mp.商品コード = r.seller_sku
ON CONFLICT (date_jst, seller_sku) DO UPDATE SET
  -- snapshot 列は更新しない (不変条件)
  -- unit_cost_snapshot     ← 保持
  -- cost_snapshot_date_jst ← 保持
  asin_norm                  = excluded.asin_norm,
  product_name               = excluded.product_name,
  units_ordered              = excluded.units_ordered,
  units_refunded_customer    = excluded.units_refunded_customer,
  units_marketplace_guarantee = excluded.units_marketplace_guarantee,
  units_a_to_z_refund        = excluded.units_a_to_z_refund,
  units_net_sold             = excluded.units_net_sold,
  sales_principal_jpy        = excluded.sales_principal_jpy,
  sales_shipping_jpy         = excluded.sales_shipping_jpy,
  sales_giftwrap_jpy         = excluded.sales_giftwrap_jpy,
  sales_tax_jpy              = excluded.sales_tax_jpy,
  commission_jpy             = excluded.commission_jpy,
  fba_fulfillment_jpy        = excluded.fba_fulfillment_jpy,
  fba_storage_jpy            = excluded.fba_storage_jpy,
  closing_fee_jpy            = excluded.closing_fee_jpy,
  shipping_chargeback_jpy    = excluded.shipping_chargeback_jpy,
  giftwrap_chargeback_jpy    = excluded.giftwrap_chargeback_jpy,
  promotion_jpy              = excluded.promotion_jpy,
  warehouse_damage_jpy       = excluded.warehouse_damage_jpy,
  warehouse_lost_jpy         = excluded.warehouse_lost_jpy,
  safe_t_jpy                 = excluded.safe_t_jpy,
  refund_principal_jpy       = excluded.refund_principal_jpy,
  reversal_reimbursement_jpy = excluded.reversal_reimbursement_jpy,
  misc_fee_jpy               = excluded.misc_fee_jpy,
  other_fee_jpy              = excluded.other_fee_jpy,
  other_amount_jpy           = excluded.other_amount_jpy,
  easy_ship_jpy              = excluded.easy_ship_jpy,
  promotion_tax_jpy          = excluded.promotion_tax_jpy,
  -- latest_unit_cost_reference は最新の参考値として更新可
  latest_unit_cost_reference = excluded.latest_unit_cost_reference,
  -- cogs_amount は「既存 snapshot 原価 × 新 units_ordered/refund」で再計算
  cogs_amount = ROUND(
    COALESCE(f_amazon_finance_sku_daily_v1.unit_cost_snapshot, 0)
    * (excluded.units_ordered - excluded.units_refunded_customer - excluded.units_a_to_z_refund),
    2),
  -- profit_amount = 新 sales/fees/refund/reimbursement − 再計算 cogs
  profit_amount = ROUND(
    excluded.sales_principal_jpy
    + excluded.sales_shipping_jpy
    + excluded.sales_giftwrap_jpy
    - excluded.commission_jpy
    - excluded.fba_fulfillment_jpy
    - excluded.fba_storage_jpy
    - excluded.closing_fee_jpy
    - excluded.shipping_chargeback_jpy
    - excluded.giftwrap_chargeback_jpy
    - excluded.promotion_jpy
    - excluded.refund_principal_jpy
    + excluded.warehouse_damage_jpy
    + excluded.warehouse_lost_jpy
    + excluded.safe_t_jpy
    + excluded.reversal_reimbursement_jpy
    - COALESCE(f_amazon_finance_sku_daily_v1.unit_cost_snapshot, 0)
      * (excluded.units_ordered - excluded.units_refunded_customer - excluded.units_a_to_z_refund),
    2),
  -- 品質列 (is_cost_complete / cost_status) は snapshot と整合させて既存値保持
  -- (Codex Round 2 medium #1 対応: unit_cost_snapshot を温存しているのに品質列だけ
  --  excluded で上書きすると、後日 v_sku_costed 欠損で snapshot 有効 + missing_cost 矛盾になる)
  -- is_cost_complete ← 保持
  -- cost_status      ← 保持
  source_layer_summary = excluded.source_layer_summary,
  source_row_count = excluded.source_row_count,
  built_at         = excluded.built_at;

DROP TABLE IF EXISTS _silver_month_v1;
