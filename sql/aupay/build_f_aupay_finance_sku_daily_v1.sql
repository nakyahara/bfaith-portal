-- build_f_aupay_finance_sku_daily_v1.sql
-- ticket: au PAY マーケット Phase 1 A-1
-- design:   g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md
--
-- パラメータ (better-sqlite3 named):
--   :year_month_int  対象月 YYYYMM
--   :build_date      初回 build 日 YYYY-MM-DD (snapshot 日付)
--   :mall_fee_rate   成約手数料率 (REAL、設定 config/aupay_mall_fee_rates.json 由来。未確定なら NULL)
--
-- 売上計上方針 (v0.4 確定、A-0 verify 済):
--   - silver: orderStatus = '完了' AND itemCancelStatus = 'N' (96.1% カバー)
--   - units_cancelled = 0 (Phase B で fact_returns 投入後に再計算)
--   - クーポン (couponTotalPrice、注文単位、detail.discount とは別物) を明細に LRM 按分
--   - ポイント (usePoint / useAuPointPrice / premiumShopPrice、注文単位) も明細按分するが partial margin には入れない (Phase B で確定)
--   - giftPoint は明細単位なので按分不要、point_cost_pending に保留
--   - postagePrice / chargePrice は全件 0 (A-0 verify 済) だが念のため按分コードあり
--
-- partial margin (Phase A、5 要素):
--   gross_sales (= sales_principal + postage_allocated) - cogs - mall_fee - shipping_cost - coupon_shop
--   ※ mall_fee = (gross_sales - coupon_shop) × :mall_fee_rate (NULL なら mall_fee=NULL, calc_method='unknown')
--
-- SKU 解決 2 段 fallback (CI 最初から):
--   1. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(item_code))  (= master_match、90.7%)
--   2. f_aupay_sku_map WHERE LOWER(TRIM(aupay_key)) = LOWER(TRIM(item_code)) AND store_id  (= manual_map)
--   3. NULL → unresolved_sku_flag = 1
-- unresolved 行の原価/送料補完: 子 SKU (商品コードが親コードで始まる) 全部で 原価/送料 が一通りに定まる場合のみ
--   その値を採用 (resolution_method は 'unresolved' のまま、cost_status = 'partial_cost')。バラつけば補完しない。
--
-- UPSERT 不変条件 (楽天/Yahoo Phase 1 継承):
--   - snapshot 列 (unit_cost_snapshot_incl, cost_snapshot_date_jst, is_cost_complete, cost_status):
--     cost_status='complete' (正規 SKU 解決で原価確定) の行のみ凍結 = 取引時点原価を保持。
--     missing_cost / partial_cost (子から導出した推計) は replaceable:
--       原価が後から判明したら埋まる / 後で正規解決できたら 'complete' に昇格 / 導出失敗したら missing に戻る。
--   - cogs / variable_margin_partial は「有効 snapshot (凍結 'complete' 値 or 再評価値) × 新 units_net_sold」で再計算
--
-- 構造: Step 1 silver → Step 2 按分 (LRM) → Step 3 UPSERT 本体 → Step 4 device/settlement 別 fact → Step 5 DROP TEMP

-- ─── Step 1: silver (whitelist + 月絞り) ───
DROP TABLE IF EXISTS _silver_aupay_orders_v1;
CREATE TEMP TABLE _silver_aupay_orders_v1 AS
SELECT
  substr(replace(order_date, '/', '-'), 1, 10) AS date_jst,   -- 'YYYY/MM/DD HH:MM' → 'YYYY-MM-DD'
  order_id, order_detail_id,
  item_code AS aupay_sku_key,
  COALESCE(item_name, '') AS product_name,
  COALESCE(item_option, '') AS item_option,
  CAST(unit AS INTEGER) AS quantity,
  CAST(item_price AS REAL) AS unit_price,
  CAST(before_discount AS REAL) AS before_discount,
  CAST(discount AS REAL) AS discount,
  CAST(total_item_price AS REAL) AS total_item_price,
  CAST(tax_rate AS REAL) AS tax_rate,
  CAST(gift_point AS REAL) AS gift_point,
  COALESCE(site_and_device, '') AS site_and_device,
  COALESCE(settlement_name, '') AS settlement_name,
  -- 注文単位の値 (全明細に同値) — 按分用
  CAST(total_sale_price AS REAL) AS order_total_sale_price,
  CAST(coupon_total_price AS REAL) AS order_coupon_total,
  CAST(use_point AS REAL) AS order_use_point,
  CAST(use_au_point_price AS REAL) AS order_use_au_point,
  CAST(premium_shop_price AS REAL) AS order_premium_shop,
  CAST(total_item_option_price AS REAL) AS order_item_option,
  CAST(total_gift_wrapping_price AS REAL) AS order_gift_wrapping,
  CAST(charge_price AS REAL) AS order_charge,
  CAST(postage_price AS REAL) AS order_postage,
  CAST(request_price AS REAL) AS order_request_price
FROM raw_aupay_orders
WHERE order_status = '完了' AND item_cancel_status = 'N'
  AND CAST(strftime('%Y%m', replace(substr(order_date, 1, 10), '/', '-')) AS INTEGER) = :year_month_int
  AND unit IS NOT NULL AND unit > 0
  AND item_price IS NOT NULL
  AND item_code IS NOT NULL AND item_code <> '';
CREATE INDEX _silver_aupay_v1_idx ON _silver_aupay_orders_v1 (date_jst, aupay_sku_key);
CREATE INDEX _silver_aupay_v1_order_idx ON _silver_aupay_orders_v1 (order_id);

-- ─── Step 2: 注文単位の値を明細に LRM 按分 (floor 比例配分 + 残差を最大明細に寄せる) ───
DROP TABLE IF EXISTS _aupay_allocated_v1;
CREATE TEMP TABLE _aupay_allocated_v1 AS
WITH order_sums AS (
  SELECT order_id, SUM(total_item_price) AS sum_item FROM _silver_aupay_orders_v1 GROUP BY order_id
),
raw_alloc AS (
  SELECT s.*,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_coupon_total  * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS coupon_floor,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_use_point      * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS ponta_floor,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_use_au_point   * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS aupoint_floor,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_premium_shop   * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS premium_floor,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_postage        * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS postage_floor,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_charge         * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS charge_floor,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_item_option    * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS itemopt_floor,
    CASE WHEN os.sum_item > 0 THEN CAST(s.order_gift_wrapping  * s.total_item_price / os.sum_item AS INTEGER) ELSE 0 END AS giftwrap_floor,
    ROW_NUMBER() OVER (PARTITION BY s.order_id ORDER BY s.total_item_price DESC, s.order_detail_id) AS rn
  FROM _silver_aupay_orders_v1 s JOIN order_sums os ON os.order_id = s.order_id
)
SELECT *,
  coupon_floor   + CASE WHEN rn = 1 THEN order_coupon_total  - SUM(coupon_floor)   OVER (PARTITION BY order_id) ELSE 0 END AS coupon_allocated,
  ponta_floor    + CASE WHEN rn = 1 THEN order_use_point     - SUM(ponta_floor)    OVER (PARTITION BY order_id) ELSE 0 END AS ponta_allocated,
  aupoint_floor  + CASE WHEN rn = 1 THEN order_use_au_point  - SUM(aupoint_floor)  OVER (PARTITION BY order_id) ELSE 0 END AS aupoint_allocated,
  premium_floor  + CASE WHEN rn = 1 THEN order_premium_shop  - SUM(premium_floor)  OVER (PARTITION BY order_id) ELSE 0 END AS premium_allocated,
  postage_floor  + CASE WHEN rn = 1 THEN order_postage       - SUM(postage_floor)  OVER (PARTITION BY order_id) ELSE 0 END AS postage_allocated,
  charge_floor   + CASE WHEN rn = 1 THEN order_charge        - SUM(charge_floor)   OVER (PARTITION BY order_id) ELSE 0 END AS charge_allocated,
  itemopt_floor  + CASE WHEN rn = 1 THEN order_item_option   - SUM(itemopt_floor)  OVER (PARTITION BY order_id) ELSE 0 END AS itemopt_allocated,
  giftwrap_floor + CASE WHEN rn = 1 THEN order_gift_wrapping - SUM(giftwrap_floor) OVER (PARTITION BY order_id) ELSE 0 END AS giftwrap_allocated
FROM raw_alloc;

-- ─── Step 3: UPSERT 本体 ───
INSERT INTO f_aupay_finance_sku_daily_v1 (
  date_jst, aupay_sku_key, ne_code, variant_key, resolution_method, unresolved_sku_flag, product_name,
  units_ordered, units_cancelled, units_net_sold,
  sales_principal_jpy_incl, postage_allocated_jpy_incl, gross_sales_jpy_incl, net_sales_after_coupon_jpy_incl, request_price_jpy_incl,
  coupon_shop_jpy_incl,
  gift_point_jpy_incl, use_ponta_point_jpy_incl, use_au_point_jpy_incl, premium_member_point_jpy_incl, point_cost_pending_jpy_incl,
  tax_normal_sales_jpy_incl, tax_reduced_sales_jpy_incl, tax_free_sales_jpy_incl,
  mall_fee_jpy_incl, mall_fee_rate_applied, mall_fee_calc_method,
  shipping_cost_jpy_incl, shipping_quality,
  unit_cost_snapshot_incl, cost_snapshot_date_jst, latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
  variable_margin_partial_jpy_incl, margin_confidence,
  before_discount_jpy_incl, detail_discount_jpy_incl, charge_allocated_jpy_incl, item_option_jpy_incl, gift_wrapping_jpy_incl,
  cost_status, is_cost_complete, data_quality_score, price_variance_warning,
  order_count, line_count, source_layer_summary, source_row_count, built_at
)
WITH
daily_aggregated AS (
  SELECT
    date_jst, aupay_sku_key,
    MAX(product_name) AS product_name,
    MAX(item_option) AS item_option,  -- variant_key
    SUM(quantity) AS units_ordered,
    SUM(unit_price * quantity) AS sales_principal_jpy_incl,
    SUM(postage_allocated) AS postage_allocated_jpy_incl,
    SUM(coupon_allocated) AS coupon_shop_jpy_incl,
    SUM(gift_point) AS gift_point_jpy_incl,
    SUM(ponta_allocated) AS use_ponta_point_jpy_incl,
    SUM(aupoint_allocated) AS use_au_point_jpy_incl,
    SUM(premium_allocated) AS premium_member_point_jpy_incl,
    SUM(charge_allocated) AS charge_allocated_jpy_incl,
    SUM(itemopt_allocated) AS item_option_jpy_incl,
    SUM(giftwrap_allocated) AS gift_wrapping_jpy_incl,
    SUM(before_discount * quantity) AS before_discount_jpy_incl,
    SUM(discount * quantity) AS detail_discount_jpy_incl,
    SUM(order_request_price * total_item_price / NULLIF(order_total_sale_price, 0)) AS request_price_jpy_incl,  -- 検算用 (按分)
    SUM(CASE WHEN tax_rate = 0.1  THEN unit_price * quantity ELSE 0 END) AS tax_normal_sales_jpy_incl,
    SUM(CASE WHEN tax_rate = 0.08 THEN unit_price * quantity ELSE 0 END) AS tax_reduced_sales_jpy_incl,
    SUM(CASE WHEN tax_rate NOT IN (0.1, 0.08) OR tax_rate IS NULL THEN unit_price * quantity ELSE 0 END) AS tax_free_sales_jpy_incl,
    CASE WHEN COUNT(DISTINCT unit_price) > 1 THEN 1 ELSE 0 END AS price_variance_warning,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(*) AS line_count
  FROM _aupay_allocated_v1
  GROUP BY date_jst, aupay_sku_key
),
sku_resolved AS (
  SELECT
    aupay_sku_key,
    COALESCE(
      (SELECT 商品コード FROM m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(aupay_sku_key))),
      (SELECT ne_code FROM f_aupay_sku_map WHERE LOWER(TRIM(aupay_key)) = LOWER(TRIM(aupay_sku_key)) AND store_id = 'b-faith01')
    ) AS ne_code,
    CASE
      WHEN EXISTS (SELECT 1 FROM m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(aupay_sku_key))) THEN 'master_match'
      WHEN EXISTS (SELECT 1 FROM f_aupay_sku_map WHERE LOWER(TRIM(aupay_key)) = LOWER(TRIM(aupay_sku_key)) AND store_id = 'b-faith01') THEN 'manual_map'
      ELSE 'unresolved'
    END AS resolution_method
  FROM (SELECT DISTINCT aupay_sku_key FROM daily_aggregated)
),
-- unresolved 親 SKU の原価/送料を子 SKU (商品コードが親コードで始まる) から導出。
-- 子全部で 原価 (or 送料) が一通りに定まる場合のみ採用 = どの variant が売れたか不明でもコストは確定できるケース。
-- au PAY は itemCode に親コード・itemOption に variant を返すが m_products には子 SKU しか無い構造への対策 (Phase A 内の精度向上)。
-- 子のコストがバラつく場合 (sevendays-art の -db 830 vs -db-2 1660 等) は採用せず missing のまま (f_aupay_sku_map 手動マップ待ち)。
derived_parent_cost AS (
  SELECT sr.aupay_sku_key,
    MIN(mp.原価) * (1 + COALESCE(MIN(mp.消費税率), 0.10)) AS derived_unit_cost_incl
  FROM sku_resolved sr
  JOIN m_products mp
    -- 「商品コードが親コードで始まる」を LIKE のワイルドカード解釈なしで判定 (親コードに _ / % が入っても安全)
    ON substr(LOWER(mp.商品コード), 1, length(LOWER(TRIM(sr.aupay_sku_key)))) = LOWER(TRIM(sr.aupay_sku_key))
   AND LOWER(mp.商品コード) <> LOWER(TRIM(sr.aupay_sku_key))
  WHERE sr.resolution_method = 'unresolved'
  GROUP BY sr.aupay_sku_key
  HAVING COUNT(*) >= 1
     AND COUNT(mp.原価) = COUNT(*)                                            -- 子全部に原価あり (NULL 混入なら不採用)
     AND MIN(mp.原価) = MAX(mp.原価)                                           -- 原価が一通り
     AND MIN(mp.原価) > 0
     AND MIN(COALESCE(mp.消費税率, 0.10)) = MAX(COALESCE(mp.消費税率, 0.10))   -- 税率も一通り
),
derived_parent_shipping AS (
  SELECT sr.aupay_sku_key, MIN(mp.送料) AS derived_unit_shipping
  FROM sku_resolved sr
  JOIN m_products mp
    -- 「商品コードが親コードで始まる」を LIKE のワイルドカード解釈なしで判定 (親コードに _ / % が入っても安全)
    ON substr(LOWER(mp.商品コード), 1, length(LOWER(TRIM(sr.aupay_sku_key)))) = LOWER(TRIM(sr.aupay_sku_key))
   AND LOWER(mp.商品コード) <> LOWER(TRIM(sr.aupay_sku_key))
  WHERE sr.resolution_method = 'unresolved'
  GROUP BY sr.aupay_sku_key
  HAVING COUNT(*) >= 1
     AND COUNT(mp.送料) = COUNT(*)
     AND MIN(mp.送料) = MAX(mp.送料)
),
cost_lookup AS (
  SELECT sr.aupay_sku_key, sr.ne_code,
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL THEN COALESCE(eg.genka, mp.原価) * (1 + COALESCE(mp.消費税率, 0.10))
      WHEN sr.resolution_method = 'unresolved' AND dpc.derived_unit_cost_incl IS NOT NULL THEN dpc.derived_unit_cost_incl
      ELSE NULL
    END AS unit_cost_snapshot_incl,
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL THEN 'complete'
      WHEN sr.resolution_method = 'unresolved' AND dpc.derived_unit_cost_incl IS NOT NULL THEN 'partial_cost'
      ELSE 'missing_cost'
    END AS cost_status
  FROM sku_resolved sr
  LEFT JOIN m_products mp ON mp.商品コード = sr.ne_code
  LEFT JOIN exception_genka eg ON eg.sku = sr.ne_code
  LEFT JOIN derived_parent_cost dpc ON dpc.aupay_sku_key = sr.aupay_sku_key
),
shipping_lookup AS (
  SELECT sr.aupay_sku_key, sr.ne_code,
    COALESCE(ps.ship_cost, sr2.配送関係費合計, mp.送料,
             CASE WHEN sr.resolution_method = 'unresolved' THEN dps.derived_unit_shipping END) AS unit_shipping_cost,
    CASE
      WHEN ps.ship_cost IS NOT NULL THEN 'actual'
      WHEN sr2.配送関係費合計 IS NOT NULL THEN 'estimated_rates'
      WHEN mp.送料 IS NOT NULL THEN 'estimated_fallback'
      WHEN sr.resolution_method = 'unresolved' AND dps.derived_unit_shipping IS NOT NULL THEN 'estimated_fallback'
      ELSE 'missing'
    END AS shipping_quality
  FROM sku_resolved sr
  LEFT JOIN m_products mp ON mp.商品コード = sr.ne_code
  LEFT JOIN product_shipping ps ON ps.sku = sr.ne_code
  LEFT JOIN shipping_rates sr2 ON sr2.shipping_code = mp.送料コード
  LEFT JOIN derived_parent_shipping dps ON dps.aupay_sku_key = sr.aupay_sku_key
)
SELECT
  c.date_jst, c.aupay_sku_key, sr.ne_code, c.item_option AS variant_key, sr.resolution_method,
  CASE WHEN sr.ne_code IS NULL THEN 1 ELSE 0 END AS unresolved_sku_flag,
  c.product_name,
  c.units_ordered, 0 AS units_cancelled, c.units_ordered AS units_net_sold,
  -- 売上
  ROUND(c.sales_principal_jpy_incl, 2) AS sales_principal_jpy_incl,
  ROUND(c.postage_allocated_jpy_incl, 2) AS postage_allocated_jpy_incl,
  ROUND(c.sales_principal_jpy_incl + c.postage_allocated_jpy_incl, 2) AS gross_sales_jpy_incl,
  ROUND(c.sales_principal_jpy_incl + c.postage_allocated_jpy_incl - c.coupon_shop_jpy_incl, 2) AS net_sales_after_coupon_jpy_incl,
  ROUND(c.request_price_jpy_incl, 2) AS request_price_jpy_incl,
  ROUND(c.coupon_shop_jpy_incl, 2) AS coupon_shop_jpy_incl,
  -- ポイント (partial margin に入れない)
  ROUND(c.gift_point_jpy_incl, 2) AS gift_point_jpy_incl,
  ROUND(c.use_ponta_point_jpy_incl, 2) AS use_ponta_point_jpy_incl,
  ROUND(c.use_au_point_jpy_incl, 2) AS use_au_point_jpy_incl,
  ROUND(c.premium_member_point_jpy_incl, 2) AS premium_member_point_jpy_incl,
  ROUND(c.gift_point_jpy_incl + c.premium_member_point_jpy_incl, 2) AS point_cost_pending_jpy_incl,
  -- 税率別売上
  ROUND(c.tax_normal_sales_jpy_incl, 2) AS tax_normal_sales_jpy_incl,
  ROUND(c.tax_reduced_sales_jpy_incl, 2) AS tax_reduced_sales_jpy_incl,
  ROUND(c.tax_free_sales_jpy_incl, 2) AS tax_free_sales_jpy_incl,
  -- モール手数料 (成約手数料率は :mall_fee_rate、NULL なら mall_fee=NULL + calc_method='unknown')
  CASE WHEN :mall_fee_rate IS NOT NULL THEN ROUND((c.sales_principal_jpy_incl + c.postage_allocated_jpy_incl - c.coupon_shop_jpy_incl) * :mall_fee_rate, 2) ELSE NULL END AS mall_fee_jpy_incl,
  :mall_fee_rate AS mall_fee_rate_applied,
  CASE WHEN :mall_fee_rate IS NOT NULL THEN 'estimated_rate' ELSE 'unknown' END AS mall_fee_calc_method,
  -- 送料コスト
  ROUND(COALESCE(sl.unit_shipping_cost, 0) * c.units_ordered, 2) AS shipping_cost_jpy_incl,
  COALESCE(sl.shipping_quality, 'missing') AS shipping_quality,
  -- 原価 snapshot
  cl.unit_cost_snapshot_incl,
  :build_date AS cost_snapshot_date_jst,
  cl.unit_cost_snapshot_incl AS latest_unit_cost_reference_incl,
  ROUND(COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered, 2) AS cogs_amount_jpy_incl,
  -- partial margin (5 要素 = gross_sales - cogs - mall_fee - shipping - coupon、mall_fee が NULL なら 0 として計算)
  ROUND(
    (c.sales_principal_jpy_incl + c.postage_allocated_jpy_incl)
    - COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered
    - COALESCE(CASE WHEN :mall_fee_rate IS NOT NULL THEN (c.sales_principal_jpy_incl + c.postage_allocated_jpy_incl - c.coupon_shop_jpy_incl) * :mall_fee_rate ELSE 0 END, 0)
    - COALESCE(sl.unit_shipping_cost, 0) * c.units_ordered
    - c.coupon_shop_jpy_incl,
    2) AS variable_margin_partial_jpy_incl,
  'partial' AS margin_confidence,
  -- audit
  ROUND(c.before_discount_jpy_incl, 2) AS before_discount_jpy_incl,
  ROUND(c.detail_discount_jpy_incl, 2) AS detail_discount_jpy_incl,
  ROUND(c.charge_allocated_jpy_incl, 2) AS charge_allocated_jpy_incl,
  ROUND(c.item_option_jpy_incl, 2) AS item_option_jpy_incl,
  ROUND(c.gift_wrapping_jpy_incl, 2) AS gift_wrapping_jpy_incl,
  -- 品質
  COALESCE(cl.cost_status, 'missing_cost') AS cost_status,
  CASE WHEN cl.cost_status = 'complete' THEN 1 ELSE 0 END AS is_cost_complete,
  (CASE COALESCE(cl.cost_status, 'missing_cost')
        WHEN 'complete' THEN 25 WHEN 'partial_cost' THEN 10 ELSE 0 END
   + CASE COALESCE(sl.shipping_quality, 'missing')
        WHEN 'actual' THEN 25 WHEN 'estimated_rates' THEN 20 WHEN 'estimated_fallback' THEN 15 ELSE 0 END
   + CASE WHEN :mall_fee_rate IS NOT NULL THEN 25 ELSE 0 END
   + CASE sr.resolution_method WHEN 'master_match' THEN 25 WHEN 'manual_map' THEN 15 ELSE 0 END
  ) AS data_quality_score,
  c.price_variance_warning,
  -- メタ
  c.order_count, c.line_count,
  'raw_aupay_orders + m_products + f_aupay_sku_map (Phase A partial margin)' AS source_layer_summary,
  c.line_count AS source_row_count,
  CURRENT_TIMESTAMP AS built_at
FROM daily_aggregated c
LEFT JOIN sku_resolved sr ON sr.aupay_sku_key = c.aupay_sku_key
LEFT JOIN cost_lookup cl ON cl.aupay_sku_key = c.aupay_sku_key
LEFT JOIN shipping_lookup sl ON sl.aupay_sku_key = c.aupay_sku_key
ON CONFLICT (date_jst, aupay_sku_key) DO UPDATE SET
  -- snapshot 列 温存 (不変条件)
  ne_code                          = excluded.ne_code,
  variant_key                      = excluded.variant_key,
  resolution_method                = excluded.resolution_method,
  unresolved_sku_flag              = excluded.unresolved_sku_flag,
  product_name                     = excluded.product_name,
  units_ordered                    = excluded.units_ordered,
  units_cancelled                  = excluded.units_cancelled,
  units_net_sold                   = excluded.units_net_sold,
  sales_principal_jpy_incl         = excluded.sales_principal_jpy_incl,
  postage_allocated_jpy_incl       = excluded.postage_allocated_jpy_incl,
  gross_sales_jpy_incl             = excluded.gross_sales_jpy_incl,
  net_sales_after_coupon_jpy_incl  = excluded.net_sales_after_coupon_jpy_incl,
  request_price_jpy_incl           = excluded.request_price_jpy_incl,
  coupon_shop_jpy_incl             = excluded.coupon_shop_jpy_incl,
  gift_point_jpy_incl              = excluded.gift_point_jpy_incl,
  use_ponta_point_jpy_incl         = excluded.use_ponta_point_jpy_incl,
  use_au_point_jpy_incl            = excluded.use_au_point_jpy_incl,
  premium_member_point_jpy_incl    = excluded.premium_member_point_jpy_incl,
  point_cost_pending_jpy_incl      = excluded.point_cost_pending_jpy_incl,
  tax_normal_sales_jpy_incl        = excluded.tax_normal_sales_jpy_incl,
  tax_reduced_sales_jpy_incl       = excluded.tax_reduced_sales_jpy_incl,
  tax_free_sales_jpy_incl          = excluded.tax_free_sales_jpy_incl,
  mall_fee_jpy_incl                = excluded.mall_fee_jpy_incl,
  mall_fee_rate_applied            = excluded.mall_fee_rate_applied,
  mall_fee_calc_method             = excluded.mall_fee_calc_method,
  shipping_cost_jpy_incl           = excluded.shipping_cost_jpy_incl,
  shipping_quality                 = excluded.shipping_quality,
  latest_unit_cost_reference_incl  = excluded.latest_unit_cost_reference_incl,
  -- snapshot 列の凍結は「cost_status = 'complete' (= 正規 SKU 解決で原価確定)」の行のみ。
  -- それ以外 (missing_cost / partial_cost = 子から導出した推計) は replaceable:
  --   missing → 原価が後から判明したら埋まる / partial → 後で正規解決できたら 'complete' に昇格 / 導出失敗したら missing に戻る。
  unit_cost_snapshot_incl          = CASE WHEN f_aupay_finance_sku_daily_v1.cost_status = 'complete' THEN f_aupay_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END,
  cost_snapshot_date_jst           = CASE WHEN f_aupay_finance_sku_daily_v1.cost_status = 'complete' THEN f_aupay_finance_sku_daily_v1.cost_snapshot_date_jst ELSE excluded.cost_snapshot_date_jst END,
  cost_status                      = CASE WHEN f_aupay_finance_sku_daily_v1.cost_status = 'complete' THEN 'complete' ELSE excluded.cost_status END,
  is_cost_complete                 = CASE WHEN f_aupay_finance_sku_daily_v1.cost_status = 'complete' THEN 1 ELSE excluded.is_cost_complete END,
  -- cogs / margin は「有効 snapshot (凍結 'complete' 値 or 再評価値) × 新 units_net_sold」で再計算
  cogs_amount_jpy_incl             = ROUND(COALESCE(CASE WHEN f_aupay_finance_sku_daily_v1.cost_status = 'complete' THEN f_aupay_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END, 0) * excluded.units_net_sold, 2),
  variable_margin_partial_jpy_incl = ROUND(
    excluded.gross_sales_jpy_incl
    - COALESCE(CASE WHEN f_aupay_finance_sku_daily_v1.cost_status = 'complete' THEN f_aupay_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END, 0) * excluded.units_net_sold
    - COALESCE(excluded.mall_fee_jpy_incl, 0)
    - excluded.shipping_cost_jpy_incl
    - excluded.coupon_shop_jpy_incl, 2),
  before_discount_jpy_incl         = excluded.before_discount_jpy_incl,
  detail_discount_jpy_incl         = excluded.detail_discount_jpy_incl,
  charge_allocated_jpy_incl        = excluded.charge_allocated_jpy_incl,
  item_option_jpy_incl             = excluded.item_option_jpy_incl,
  gift_wrapping_jpy_incl           = excluded.gift_wrapping_jpy_incl,
  data_quality_score               = excluded.data_quality_score,
  price_variance_warning           = excluded.price_variance_warning,
  order_count                      = excluded.order_count,
  line_count                       = excluded.line_count,
  source_layer_summary             = excluded.source_layer_summary,
  source_row_count                 = excluded.source_row_count,
  built_at                         = excluded.built_at;

-- ─── Step 4: デバイス別 / 決済手段別 fact ───
-- 対象月の行を一度削除してから再 INSERT (これらは snapshot を持たないので clear-rebuild で OK)
DELETE FROM f_aupay_sku_device_daily WHERE substr(date_jst, 1, 7) = (
  SELECT printf('%04d-%02d', :year_month_int / 100, :year_month_int % 100)
);
INSERT INTO f_aupay_sku_device_daily (date_jst, aupay_sku_key, device, ne_code, product_name, units_net_sold, sales_principal_jpy_incl, order_count, built_at)
SELECT
  s.date_jst, s.aupay_sku_key, s.site_and_device AS device,
  (SELECT ne_code FROM f_aupay_finance_sku_daily_v1 f WHERE f.date_jst = s.date_jst AND f.aupay_sku_key = s.aupay_sku_key) AS ne_code,
  MAX(s.product_name) AS product_name,
  SUM(s.quantity) AS units_net_sold,
  ROUND(SUM(s.unit_price * s.quantity), 2) AS sales_principal_jpy_incl,
  COUNT(DISTINCT s.order_id) AS order_count,
  CURRENT_TIMESTAMP AS built_at
FROM _silver_aupay_orders_v1 s
GROUP BY s.date_jst, s.aupay_sku_key, s.site_and_device;

DELETE FROM f_aupay_sku_settlement_daily WHERE substr(date_jst, 1, 7) = (
  SELECT printf('%04d-%02d', :year_month_int / 100, :year_month_int % 100)
);
INSERT INTO f_aupay_sku_settlement_daily (date_jst, aupay_sku_key, settlement_name, ne_code, product_name, units_net_sold, sales_principal_jpy_incl, order_count, built_at)
SELECT
  s.date_jst, s.aupay_sku_key, s.settlement_name,
  (SELECT ne_code FROM f_aupay_finance_sku_daily_v1 f WHERE f.date_jst = s.date_jst AND f.aupay_sku_key = s.aupay_sku_key) AS ne_code,
  MAX(s.product_name) AS product_name,
  SUM(s.quantity) AS units_net_sold,
  ROUND(SUM(s.unit_price * s.quantity), 2) AS sales_principal_jpy_incl,
  COUNT(DISTINCT s.order_id) AS order_count,
  CURRENT_TIMESTAMP AS built_at
FROM _silver_aupay_orders_v1 s
GROUP BY s.date_jst, s.aupay_sku_key, s.settlement_name;

-- ─── Step 5: cleanup ───
DROP TABLE IF EXISTS _aupay_allocated_v1;
DROP TABLE IF EXISTS _silver_aupay_orders_v1;
