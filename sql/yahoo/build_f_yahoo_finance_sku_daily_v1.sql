-- build_f_yahoo_finance_sku_daily_v1.sql
-- ticket: Yahoo Phase 1 Y-1
-- contract: docs/contracts/raw_yahoo_orders.contract.md (将来作成)
-- design:   g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1a設計書_v0.4_20260510.md
--
-- パラメータ (better-sqlite3 named):
--   :year_month_int  対象月 YYYYMM
--   :build_date      初回 build 日 YYYY-MM-DD (snapshot 日付)
--
-- 売上計上方針 (v0.4):
--   - silver: order_status='5' AND pay_status='1' AND ship_status='3' (whitelist、98.8%)
--   - units_cancelled = 0 (Phase 1c-3 で fact_returns 投入後に再計算)
--   - 按分なし (中原さん全送料無料 + クーポンは商品単位)
--   - 月次フルリビルド loop 不要 (按分なしなので増分 UPSERT で OK)
--
-- 構造:
--   Step 1. silver TEMP TABLE (whitelist + 月絞り)
--   Step 2. INSERT ... ON CONFLICT で UPSERT (CTE A → B → C → D → SELECT)
--   Step 3. DROP TEMP TABLE
--
-- UPSERT 不変条件 (楽天 Phase 1a/1b と同型):
--   - snapshot 列 (unit_cost_snapshot_incl, cost_snapshot_date_jst, is_cost_complete, cost_status) 温存
--   - cogs / variable_margin_partial は「既存 snapshot × 新 units_net_sold」で再計算

-- ----------------------------
-- Step 1. silver: whitelist + 月絞り
-- ----------------------------
DROP TABLE IF EXISTS _silver_yahoo_orders_v1;

CREATE TEMP TABLE _silver_yahoo_orders_v1 AS
SELECT
  -- date_jst: order_time は 'YYYY-MM-DD HH:MM:SS' 想定 (要確認)、substr(1,10) で 'YYYY-MM-DD'
  substr(order_time, 1, 10) AS date_jst,
  order_id,
  line_id,
  item_id,
  COALESCE(sub_code, '') AS sub_code,
  -- yahoo_sku_key: variant 別を粒度として保持 (Codex Critical: 親 SKU 自動吸収禁止)
  -- sub_code あれば variant 別、なければ item_id (= 親 SKU で集計)
  CASE WHEN COALESCE(sub_code, '') <> '' THEN sub_code ELSE item_id END AS yahoo_sku_key,
  COALESCE(title, '') AS product_name,
  CAST(quantity AS INTEGER) AS quantity,
  CAST(unit_price AS REAL) AS unit_price,
  CAST(coupon_discount AS REAL) AS coupon_discount,
  CAST(use_point AS REAL) AS use_point,
  CAST(item_tax_ratio AS REAL) AS item_tax_ratio,
  -- audit 用 (注文単位、line に同値で乗ってる)
  CAST(pay_charge AS REAL) AS pay_charge,
  CAST(ship_charge AS REAL) AS ship_charge,
  CAST(discount AS REAL) AS discount
FROM raw_yahoo_orders
WHERE order_status = '5' AND pay_status = '1' AND ship_status = '3'  -- whitelist (Codex R2 確定)
  AND CAST(strftime('%Y%m', substr(order_time, 1, 10)) AS INTEGER) = :year_month_int
  AND quantity IS NOT NULL AND quantity > 0
  AND unit_price IS NOT NULL;

CREATE INDEX _silver_yahoo_orders_v1_idx
  ON _silver_yahoo_orders_v1 (date_jst, yahoo_sku_key);
CREATE INDEX _silver_yahoo_orders_v1_order_idx
  ON _silver_yahoo_orders_v1 (order_id);

-- ----------------------------
-- Step 2. UPSERT 本体
-- ----------------------------
INSERT INTO f_yahoo_finance_sku_daily_v1 (
  date_jst, yahoo_sku_key, ne_code, variant_key, resolution_method, unresolved_sku_flag, product_name,
  units_ordered, units_cancelled, units_net_sold,
  sales_principal_jpy_incl, sales_postage_jpy_incl, gross_sales_jpy_incl,
  net_sales_before_point_jpy_incl, listing_sales_estimated_jpy_incl,
  coupon_shop_jpy_incl, use_point_jpy_incl,
  mall_fee_jpy_incl, mall_fee_calc_method,
  shipping_cost_jpy_incl, shipping_quality,
  unit_cost_snapshot_incl, cost_snapshot_date_jst,
  latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
  variable_margin_partial_jpy_incl, margin_confidence,
  pay_charge_audit_jpy_incl, ship_charge_audit_jpy_incl, discount_audit_jpy_incl,
  cost_status, is_cost_complete, data_quality_score, price_variance_warning,
  source_layer_summary, source_row_count, built_at
)
WITH
-- CTE A: daily_aggregated (yahoo_sku_key 単位で集約、variant 粒度保持)
daily_aggregated AS (
  SELECT
    date_jst,
    yahoo_sku_key,
    -- 紐付けキー (item_id, sub_code) の代表値 (どれを取っても同 yahoo_sku_key 内で同じ)
    MAX(item_id) AS item_id,
    MAX(sub_code) AS sub_code,
    MAX(product_name) AS product_name,
    SUM(quantity) AS units_ordered,
    -- 売上 (税込、unit_price × quantity)
    SUM(unit_price * quantity) AS sales_principal_jpy_incl,
    -- クーポン / use_point (商品単位、按分不要)
    SUM(coupon_discount) AS coupon_shop_jpy_incl,
    SUM(use_point) AS use_point_jpy_incl,
    -- audit (注文単位の値、line に同値だが 1 yahoo_sku_key に複数 line ありの可能性、SUM で audit)
    SUM(pay_charge) AS pay_charge_audit,
    SUM(ship_charge) AS ship_charge_audit,
    SUM(discount) AS discount_audit,
    -- price_variance_warning: 同 (date, sku) で複数 unit_price あれば 1
    CASE WHEN COUNT(DISTINCT unit_price) > 1 THEN 1 ELSE 0 END AS price_variance_warning,
    -- audit
    COUNT(*) AS source_row_count,
    MAX(item_tax_ratio) AS tax_rate
  FROM _silver_yahoo_orders_v1
  GROUP BY date_jst, yahoo_sku_key
),
-- CTE B: SKU 解決 (3 段階 fallback、case-insensitive + TRIM 正規化)
-- Phase 1.1 fix (Codex 推奨 + 実データ verify): m_products は小文字 suffix 登録、
--   Yahoo sub_code は大文字 suffix で来るので LOWER(TRIM()) 化が必須。
--   この fix だけで unresolved 122 SKU 中 117 SKU (95.9%) を救済。
sku_resolved AS (
  SELECT
    yahoo_sku_key,
    item_id,
    sub_code,
    -- 優先順 1: m_products WHERE 商品コード ~= sub_code (variant 別、CI)
    -- 優先順 2: m_products WHERE 商品コード ~= item_id (親 / 単品 SKU、CI)
    -- 優先順 3: f_yahoo_sku_map (手動 map、CI)
    -- 4: NULL (= unresolved_sku_flag = 1)
    COALESCE(
      (SELECT 商品コード FROM m_products
        WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(sub_code))
          AND TRIM(COALESCE(sub_code, '')) <> ''),
      (SELECT 商品コード FROM m_products
        WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(item_id))),
      (SELECT ne_code FROM f_yahoo_sku_map
        WHERE LOWER(TRIM(yahoo_key)) = LOWER(TRIM(sub_code))
          AND TRIM(COALESCE(sub_code, '')) <> ''),
      (SELECT ne_code FROM f_yahoo_sku_map
        WHERE LOWER(TRIM(yahoo_key)) = LOWER(TRIM(item_id)))
    ) AS ne_code,
    -- 注: ラベル名は v1 互換のため旧名 (sub_match / parent_match / manual_map) を継続。
    --     実態は CI (case-insensitive) join、parent_match は実は item_id 一致 (単品商品が大半)。
    --     ラベル整理は別 PR (Phase 1.1.1) で扱う (Render mirror migration 必要なため)。
    CASE
      WHEN EXISTS (SELECT 1 FROM m_products
                    WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(sub_code))
                      AND TRIM(COALESCE(sub_code, '')) <> '') THEN 'sub_match'
      WHEN EXISTS (SELECT 1 FROM m_products
                    WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(item_id))) THEN 'parent_match'
      WHEN EXISTS (SELECT 1 FROM f_yahoo_sku_map
                    WHERE LOWER(TRIM(yahoo_key)) = LOWER(TRIM(sub_code))
                      AND TRIM(COALESCE(sub_code, '')) <> '')
        OR EXISTS (SELECT 1 FROM f_yahoo_sku_map
                    WHERE LOWER(TRIM(yahoo_key)) = LOWER(TRIM(item_id))) THEN 'manual_map'
      ELSE 'unresolved'
    END AS resolution_method,
    COALESCE(sub_code, '') AS variant_key
  FROM (SELECT DISTINCT yahoo_sku_key, item_id, sub_code FROM daily_aggregated)
),
-- CTE C: 原価 (m_products / exception_genka 経由、楽天と同型)
cost_lookup AS (
  SELECT
    sr.yahoo_sku_key,
    sr.ne_code,
    -- 優先: exception_genka.genka > m_products.原価 × (1+消費税率)
    COALESCE(eg.genka, mp.原価) * (1 + COALESCE(mp.消費税率, 0.10)) AS unit_cost_snapshot_incl,
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL THEN 'complete'
      ELSE 'missing_cost'
    END AS cost_status
  FROM sku_resolved sr
  LEFT JOIN m_products mp ON mp.商品コード = sr.ne_code
  LEFT JOIN exception_genka eg ON eg.sku = sr.ne_code
),
-- CTE D: 送料 (m_products.送料 fallback chain、楽天と同型)
shipping_lookup AS (
  SELECT
    sr.yahoo_sku_key,
    sr.ne_code,
    COALESCE(ps.ship_cost, sr2.配送関係費合計, mp.送料) AS unit_shipping_cost,
    CASE
      WHEN ps.ship_cost IS NOT NULL THEN 'actual'
      WHEN sr2.配送関係費合計 IS NOT NULL THEN 'estimated_rates'
      WHEN mp.送料 IS NOT NULL THEN 'estimated_fallback'
      ELSE 'missing'
    END AS shipping_quality
  FROM sku_resolved sr
  LEFT JOIN m_products mp ON mp.商品コード = sr.ne_code
  LEFT JOIN product_shipping ps ON ps.sku = sr.ne_code
  LEFT JOIN shipping_rates sr2 ON sr2.shipping_code = mp.送料コード
)
SELECT
  c.date_jst,
  c.yahoo_sku_key,
  sr.ne_code,
  sr.variant_key,
  sr.resolution_method,
  CASE WHEN sr.ne_code IS NULL THEN 1 ELSE 0 END AS unresolved_sku_flag,
  c.product_name,
  c.units_ordered,
  0 AS units_cancelled,                    -- Phase 1a 0 固定 (Phase 1c-3 で fact_returns 投入)
  c.units_ordered AS units_net_sold,        -- Phase 1a = units_ordered (cancel 不在)
  -- 売上 3 列分離 (v0.3 Codex R4 #5)
  ROUND(c.sales_principal_jpy_incl, 2) AS sales_principal_jpy_incl,
  0 AS sales_postage_jpy_incl,                                  -- 中原さん全送料無料
  ROUND(c.sales_principal_jpy_incl, 2) AS gross_sales_jpy_incl,  -- = principal + 0
  ROUND(c.sales_principal_jpy_incl - c.coupon_shop_jpy_incl, 2) AS net_sales_before_point_jpy_incl,
  ROUND(c.sales_principal_jpy_incl - c.use_point_jpy_incl - c.coupon_shop_jpy_incl, 2) AS listing_sales_estimated_jpy_incl,
  -- クーポン / use_point
  ROUND(c.coupon_shop_jpy_incl, 2) AS coupon_shop_jpy_incl,
  ROUND(c.use_point_jpy_incl, 2) AS use_point_jpy_incl,
  -- mall_fee 仮 10% (Phase 1c-3 で actual_statement に切替)
  -- 課金ベース = (principal - coupon_shop) × 0.10
  ROUND((c.sales_principal_jpy_incl - c.coupon_shop_jpy_incl) * 0.10, 2) AS mall_fee_jpy_incl,
  'estimated_10pct' AS mall_fee_calc_method,
  -- shipping_cost = m_products.送料 × units_net_sold (= units_ordered Phase 1a)
  ROUND(COALESCE(sl.unit_shipping_cost, 0) * c.units_ordered, 2) AS shipping_cost_jpy_incl,
  COALESCE(sl.shipping_quality, 'missing') AS shipping_quality,
  -- snapshot 原価
  cl.unit_cost_snapshot_incl,
  :build_date AS cost_snapshot_date_jst,
  cl.unit_cost_snapshot_incl AS latest_unit_cost_reference_incl,
  ROUND(COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered, 2) AS cogs_amount_jpy_incl,
  -- partial margin (6 要素、v0.4 確定)
  ROUND(
    c.sales_principal_jpy_incl
    - COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered
    - (c.sales_principal_jpy_incl - c.coupon_shop_jpy_incl) * 0.10
    - COALESCE(sl.unit_shipping_cost, 0) * c.units_ordered
    - c.coupon_shop_jpy_incl
    - c.use_point_jpy_incl,
    2) AS variable_margin_partial_jpy_incl,
  'partial' AS margin_confidence,
  -- audit 列
  ROUND(c.pay_charge_audit, 2) AS pay_charge_audit_jpy_incl,
  ROUND(c.ship_charge_audit, 2) AS ship_charge_audit_jpy_incl,
  ROUND(c.discount_audit, 2) AS discount_audit_jpy_incl,
  -- 品質
  COALESCE(cl.cost_status, 'missing_cost') AS cost_status,
  CASE WHEN cl.cost_status = 'complete' THEN 1 ELSE 0 END AS is_cost_complete,
  -- data_quality_score (cost / shipping / fee[Yahoo は estimated 固定で 10] / 解決状態)
  (CASE COALESCE(cl.cost_status, 'missing_cost')
        WHEN 'complete' THEN 25 WHEN 'partial_cost' THEN 10 ELSE 0 END
   + CASE COALESCE(sl.shipping_quality, 'missing')
        WHEN 'actual' THEN 25 WHEN 'estimated_rates' THEN 20 WHEN 'estimated_fallback' THEN 15 ELSE 0 END
   + 10  -- Yahoo mall_fee は estimated_10pct 固定
   + CASE sr.resolution_method
        WHEN 'sub_match' THEN 25 WHEN 'parent_match' THEN 18
        WHEN 'manual_map' THEN 12 ELSE 0 END
  ) AS data_quality_score,
  c.price_variance_warning,
  -- メタ
  'raw_yahoo_orders + m_products + f_yahoo_sku_map (Phase 1a partial margin)' AS source_layer_summary,
  c.source_row_count,
  CURRENT_TIMESTAMP AS built_at
FROM daily_aggregated c
LEFT JOIN sku_resolved sr ON sr.yahoo_sku_key = c.yahoo_sku_key
LEFT JOIN cost_lookup cl ON cl.yahoo_sku_key = c.yahoo_sku_key
LEFT JOIN shipping_lookup sl ON sl.yahoo_sku_key = c.yahoo_sku_key
ON CONFLICT (date_jst, yahoo_sku_key) DO UPDATE SET
  -- snapshot 列 温存 (不変条件、楽天 Phase 1a/1b 継承)
  -- unit_cost_snapshot_incl     ← 保持
  -- cost_snapshot_date_jst      ← 保持
  -- is_cost_complete            ← 保持
  -- cost_status                 ← 保持
  ne_code                          = excluded.ne_code,
  variant_key                      = excluded.variant_key,
  resolution_method                = excluded.resolution_method,
  unresolved_sku_flag              = excluded.unresolved_sku_flag,
  product_name                     = excluded.product_name,
  units_ordered                    = excluded.units_ordered,
  units_cancelled                  = excluded.units_cancelled,
  units_net_sold                   = excluded.units_net_sold,
  sales_principal_jpy_incl         = excluded.sales_principal_jpy_incl,
  sales_postage_jpy_incl           = excluded.sales_postage_jpy_incl,
  gross_sales_jpy_incl             = excluded.gross_sales_jpy_incl,
  net_sales_before_point_jpy_incl  = excluded.net_sales_before_point_jpy_incl,
  listing_sales_estimated_jpy_incl = excluded.listing_sales_estimated_jpy_incl,
  coupon_shop_jpy_incl             = excluded.coupon_shop_jpy_incl,
  use_point_jpy_incl               = excluded.use_point_jpy_incl,
  mall_fee_jpy_incl                = excluded.mall_fee_jpy_incl,
  mall_fee_calc_method             = excluded.mall_fee_calc_method,
  shipping_cost_jpy_incl           = excluded.shipping_cost_jpy_incl,
  shipping_quality                 = excluded.shipping_quality,
  -- latest 参考値は更新可
  latest_unit_cost_reference_incl  = excluded.latest_unit_cost_reference_incl,
  -- cogs / margin は「既存 snapshot × 新 units_net_sold」で再計算
  cogs_amount_jpy_incl             = ROUND(
    COALESCE(f_yahoo_finance_sku_daily_v1.unit_cost_snapshot_incl, 0)
    * excluded.units_net_sold, 2),
  variable_margin_partial_jpy_incl = ROUND(
    excluded.sales_principal_jpy_incl
    - COALESCE(f_yahoo_finance_sku_daily_v1.unit_cost_snapshot_incl, 0) * excluded.units_net_sold
    - excluded.mall_fee_jpy_incl
    - excluded.shipping_cost_jpy_incl
    - excluded.coupon_shop_jpy_incl
    - excluded.use_point_jpy_incl,
    2),
  pay_charge_audit_jpy_incl        = excluded.pay_charge_audit_jpy_incl,
  ship_charge_audit_jpy_incl       = excluded.ship_charge_audit_jpy_incl,
  discount_audit_jpy_incl          = excluded.discount_audit_jpy_incl,
  data_quality_score               = excluded.data_quality_score,
  price_variance_warning           = excluded.price_variance_warning,
  source_layer_summary             = excluded.source_layer_summary,
  source_row_count                 = excluded.source_row_count,
  built_at                         = excluded.built_at;

DROP TABLE IF EXISTS _silver_yahoo_orders_v1;
