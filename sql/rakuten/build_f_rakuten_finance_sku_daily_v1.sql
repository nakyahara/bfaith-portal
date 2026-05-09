-- build_f_rakuten_finance_sku_daily_v1.sql
-- ticket: Phase 1a #R-1
-- contract: docs/contracts/raw_rakuten_orders.contract.md (v1.1)
-- design:   g:/共有ドライブ/AI_reference/システム設計/楽天Phase1a設計書_v0.4_20260509.md
--
-- パラメータ (better-sqlite3 named):
--   :year_month_int  対象月 YYYYMM (例: 202604)
--   :build_date      初回 build 日 YYYY-MM-DD (snapshot 日付として使用)
--
-- 売上計上方針 (D 案、Codex セカンドオピニオン 2026-05-09 確定):
--   - status IN (500, 600, 700) を「受注確定」として売上計上 (発送前/発送後/お届け完了)
--   - status = 900 (キャンセル) は売上 fact から除外、fact_returns 経由で units_cancelled
--   - status = 200/300 (異常残骸、各月数百件) は除外
--   - 採用根拠: 楽天は status=700 が 2 ヶ月遅延、当月/先月のダッシュボード可視化要件を満たすため
--
-- 不変条件 (前提確認、2026-05-09 検証済):
--   - raw_rakuten_orders は (order_number, item_detail_id) 一意 = latest-state-only 型
--     (376,254 行 vs 376,254 distinct、重複ゼロ)
--   - fact_returns と silver(IN 500/600/700) は完全分離 (overlap=0)、二重計上リスクなし
--
-- 構造:
--   Step 1. CREATE TEMP TABLE _silver_rakuten_orders_v1 (status IN (500,600,700) 抽出)
--   Step 2. INSERT ... ON CONFLICT で UPSERT
--           CTE 構造: orders_filtered → items_with_postage → daily_aggregated
--                     → coupon_agg / refund_agg / sku_resolved / cost_lookup / shipping_lookup → SELECT
--   Step 3. DROP TEMP TABLE
--
-- 既知の限界 (Phase 1a 許容、Phase 1b で按分案で対応予定):
-- - fact_returns 注文日 と silver date_jst が日付不一致な (date, code) ペアは LEFT JOIN で漏れる
--   (4 月実測 88 units、全 cancel の 36%)
-- - 影響: units_cancelled / refund_amount が若干過小、cogs / variable_margin はほぼ影響なし
-- - 本 build script の validation で「日付不一致 SKU 数」を warning 表示
-- - Phase 1b 案: 同月・同 rakuten_code で units_ordered 比例 cancel 配賦
--   (詳細: g:/共有ドライブ/AI_reference/システム設計/楽天Phase1b案_C-lite検討メモ_20260509.md)
--
-- UPSERT の不変条件 (Codex Round 11-13 で Amazon Phase 1 から継承):
--   - snapshot 列 (unit_cost_snapshot_incl / cost_snapshot_date_jst /
--     is_cost_complete / cost_status) は既存値温存
--   - cogs_amount_jpy_incl / variable_margin_jpy_incl 等は「既存 snapshot × 新 units」で再計算

-- ----------------------------
-- Step 1. silver: status IN (500,600,700) + 月絞り込み (raw_rakuten_orders は item 単位 1 行、latest-state-only)
-- ----------------------------
DROP TABLE IF EXISTS _silver_rakuten_orders_v1;

CREATE TEMP TABLE _silver_rakuten_orders_v1 AS
SELECT
  -- date_jst: order_date は ISO8601 + JST offset (例 '2026-04-01T23:42:50+0900')
  -- substr(1,10) で 'YYYY-MM-DD' 抽出 (JST はそのまま使える)
  substr(order_date, 1, 10) AS date_jst,
  order_number,
  item_detail_id,
  item_number AS rakuten_code,
  COALESCE(item_name, '') AS product_name,
  CAST(units AS INTEGER) AS units,
  CAST(price_tax_incl AS REAL) AS price_tax_incl,
  CAST(tax_rate AS REAL) AS tax_rate,
  -- 注文単位 (同一 order_number で同値、CTE A の audit + CTE B の按分計算用)
  CAST(total_price AS REAL) AS order_total_price,
  CAST(postage_price AS REAL) AS order_postage_price
FROM raw_rakuten_orders
-- D 案: 受注ベース (発送前/発送後/お届け完了)、900 はキャンセル fact 経由で控除
-- 200/300 は異常残骸として除外 (contract §4)
WHERE order_status IN (500, 600, 700)
  AND CAST(strftime('%Y%m', substr(order_date, 1, 10)) AS INTEGER) = :year_month_int
  AND units IS NOT NULL AND units > 0
  AND price_tax_incl IS NOT NULL;

CREATE INDEX _silver_rakuten_orders_v1_idx
  ON _silver_rakuten_orders_v1 (date_jst, rakuten_code);
CREATE INDEX _silver_rakuten_orders_v1_order_idx
  ON _silver_rakuten_orders_v1 (order_number);

-- ----------------------------
-- Step 2. UPSERT 本体
-- ----------------------------
INSERT INTO f_rakuten_finance_sku_daily_v1 (
  date_jst, rakuten_code, ne_code, sku_resolution, product_name,
  units_ordered, units_cancelled, units_net_sold,
  sales_principal_jpy_incl, sales_postage_jpy_incl,
  coupon_shop_jpy_incl, coupon_all_jpy_incl, promotion_jpy_incl,
  refund_amount_jpy_incl,
  mall_fee_jpy_incl,
  shipping_cost_jpy_incl, shipping_quality,
  unit_cost_snapshot_incl, cost_snapshot_date_jst,
  latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
  gross_sales_jpy_incl, net_sales_jpy_incl,
  variable_margin_jpy_incl, refund_adjusted_net_sales_jpy_incl,
  cost_status, is_cost_complete, data_quality_score, price_variance_warning,
  source_layer_summary, source_row_count, built_at
)
WITH
-- CTE A: orders_filtered (注文単位、audit 用 + 按分分母計算)
orders_filtered AS (
  SELECT
    order_number,
    -- 同一 order_number で同値、audit
    MAX(order_total_price) AS order_total_price,
    MAX(order_postage_price) AS order_postage_price,
    -- Codex H1 修正: 按分分母 = SUM(price_tax_incl × units) (商品金額合計、goods_price 相当)
    -- total_price (商品+送料) を分母にすると按分合計 < postage_price で送料漏れ
    SUM(price_tax_incl * units) AS items_total_per_order
  FROM _silver_rakuten_orders_v1
  GROUP BY order_number
),
-- CTE B: items_with_postage (item 単位で postage 按分)
items_with_postage AS (
  SELECT
    s.date_jst,
    s.rakuten_code,
    s.product_name,
    s.units,
    s.price_tax_incl,
    s.tax_rate,
    -- principal: price_tax_incl × units
    s.price_tax_incl * s.units AS sales_principal_jpy_incl,
    -- postage_split: 按分 (zero-division ガード)
    CASE
      WHEN COALESCE(o.items_total_per_order, 0) = 0 THEN 0
      ELSE o.order_postage_price * (s.price_tax_incl * s.units) / o.items_total_per_order
    END AS sales_postage_jpy_incl
  FROM _silver_rakuten_orders_v1 s
  LEFT JOIN orders_filtered o ON o.order_number = s.order_number
),
-- CTE C: daily_aggregated (date_jst × rakuten_code に再集約、selected_choice 吸収)
daily_aggregated AS (
  SELECT
    date_jst,
    rakuten_code,
    -- product_name は最新 (任意の 1 つ)
    MAX(product_name) AS product_name,
    SUM(units) AS units_ordered,
    SUM(sales_principal_jpy_incl) AS sales_principal_jpy_incl,
    SUM(sales_postage_jpy_incl) AS sales_postage_jpy_incl,
    -- price_variance_warning: 同 (date, rakuten_code) で複数 price_tax_incl あれば 1
    CASE WHEN COUNT(DISTINCT price_tax_incl) > 1 THEN 1 ELSE 0 END AS price_variance_warning,
    -- audit (raw 行数)
    COUNT(*) AS source_row_count,
    -- tax_rate は最頻値ではなく MAX (将来 weighted 化検討)
    MAX(tax_rate) AS tax_rate
  FROM items_with_postage
  GROUP BY date_jst, rakuten_code
),
-- CTE D: enriched (fact JOIN + snapshot 原価 + 送料 fallback)
-- D-1: クーポン (fact_promotion_cost、楽天分は 30K 件投入済、既に SKU 別按分済)
coupon_agg AS (
  SELECT
    日付 AS date_jst,
    モール商品コード AS rakuten_code,
    SUM(クーポン金額) AS coupon_shop_jpy_incl,
    SUM(全店クーポン金額) AS coupon_all_jpy_incl,
    SUM(プロモーション金額) AS promotion_jpy_incl
  FROM fact_promotion_cost
  WHERE モール = 'rakuten'
    AND CAST(strftime('%Y%m', 日付) AS INTEGER) = :year_month_int
  GROUP BY 日付, モール商品コード
),
-- D-2: 返金 (fact_returns、楽天分は 7,138 件投入済、注文日基準で JOIN)
-- 注意 (Phase 1a 既知の限界): fact_returns 注文日 と silver date_jst が日付不一致な
-- (date, code) ペアは LEFT JOIN で漏れる (4 月実測 88 units)。Phase 1b で按分案対応予定
refund_agg AS (
  SELECT
    注文日 AS date_jst,
    モール商品コード AS rakuten_code,
    SUM(返金額) AS refund_amount_jpy_incl,
    SUM(数量) AS units_cancelled
  FROM fact_returns
  WHERE モール = 'rakuten'
    AND 注文日 IS NOT NULL
    AND CAST(strftime('%Y%m', 注文日) AS INTEGER) = :year_month_int
  GROUP BY 注文日, モール商品コード
),
-- D-3: ne_code 解決 (1 rakuten_code に複数 ne_code がある場合 NULL 化、要 R-1 後段監視)
sku_resolved AS (
  SELECT
    rakuten_code,
    -- 1 rakuten_code に複数 ne_code は NULL (warning)、unique なら採用
    CASE WHEN COUNT(DISTINCT ne_code) = 1 THEN MAX(ne_code) ELSE NULL END AS ne_code
  FROM f_rakuten_sku_map
  WHERE rakuten_code IS NOT NULL AND rakuten_code <> ''
  GROUP BY rakuten_code
),
-- D-4: 原価 (m_products 経由、direct_master fallback)
-- ne_code が解決されてれば m_products.原価 (税抜) × (1+消費税率) で税込換算
-- exception_genka 優先 (sku=ne_code)、無ければ m_products
cost_lookup AS (
  SELECT
    sr.rakuten_code,
    sr.ne_code,
    -- 優先: exception_genka.genka (税込前提) > m_products.原価 × (1+消費税率)
    -- exception_genka が税抜か税込かは要確認、ここでは m_products と同思想で「税抜原価」と仮定
    -- COALESCE(exception_genka.genka, m_products.原価) × (1 + COALESCE(消費税率, 0.10))
    COALESCE(eg.genka, mp.原価) * (1 + COALESCE(mp.消費税率, 0.10)) AS unit_cost_snapshot_incl,
    -- cost_status:
    --   complete  = 原価が判明
    --   missing_cost = NULL (m_products も exception も無い)
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL THEN 'complete'
      ELSE 'missing_cost'
    END AS cost_status
  FROM sku_resolved sr
  LEFT JOIN m_products mp ON mp.商品コード = sr.ne_code
  LEFT JOIN exception_genka eg ON eg.sku = sr.ne_code
),
-- D-5: 送料 (product_shipping → shipping_rates → m_products.送料 fallback chain)
shipping_lookup AS (
  SELECT
    sr.rakuten_code,
    sr.ne_code,
    -- 優先順: product_shipping > shipping_rates > m_products.送料
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
  c.rakuten_code,
  sr.ne_code,
  CASE
    WHEN sr.ne_code IS NOT NULL THEN 'resolved'
    -- direct_master: rakuten_code = ne_code (m_products に直接マッチする場合)
    WHEN EXISTS (SELECT 1 FROM m_products mp2 WHERE mp2.商品コード = c.rakuten_code) THEN 'direct_master'
    ELSE 'unresolved'
  END AS sku_resolution,
  c.product_name,
  c.units_ordered,
  COALESCE(rf.units_cancelled, 0) AS units_cancelled,
  -- units_net_sold: ordered - cancelled、負値ガード MAX(0, ...) (4月実測 6 件)
  MAX(0, c.units_ordered - COALESCE(rf.units_cancelled, 0)) AS units_net_sold,
  ROUND(c.sales_principal_jpy_incl, 2) AS sales_principal_jpy_incl,
  ROUND(c.sales_postage_jpy_incl, 2) AS sales_postage_jpy_incl,
  ROUND(COALESCE(cp.coupon_shop_jpy_incl, 0), 2) AS coupon_shop_jpy_incl,
  ROUND(COALESCE(cp.coupon_all_jpy_incl, 0), 2) AS coupon_all_jpy_incl,
  ROUND(COALESCE(cp.promotion_jpy_incl, 0), 2) AS promotion_jpy_incl,
  ROUND(COALESCE(rf.refund_amount_jpy_incl, 0), 2) AS refund_amount_jpy_incl,
  -- mall_fee (10%、課金ベース = principal + postage - coupon_all)
  ROUND(
    (c.sales_principal_jpy_incl + c.sales_postage_jpy_incl - COALESCE(cp.coupon_all_jpy_incl, 0)) * 0.10,
    2) AS mall_fee_jpy_incl,
  -- shipping_cost = 単価 × units_net_sold (cancel 後の数量)
  ROUND(
    COALESCE(sl.unit_shipping_cost, 0) * MAX(0, c.units_ordered - COALESCE(rf.units_cancelled, 0)),
    2) AS shipping_cost_jpy_incl,
  COALESCE(sl.shipping_quality, 'missing') AS shipping_quality,
  -- snapshot 原価 (初回 build 時点で固定、UPSERT で更新しない)
  cl.unit_cost_snapshot_incl,
  :build_date AS cost_snapshot_date_jst,
  cl.unit_cost_snapshot_incl AS latest_unit_cost_reference_incl,
  -- cogs = unit_cost_snapshot × units_net_sold
  ROUND(
    COALESCE(cl.unit_cost_snapshot_incl, 0) * MAX(0, c.units_ordered - COALESCE(rf.units_cancelled, 0)),
    2) AS cogs_amount_jpy_incl,
  -- 利益 5 階層
  ROUND(c.sales_principal_jpy_incl + c.sales_postage_jpy_incl, 2) AS gross_sales_jpy_incl,
  ROUND(
    c.sales_principal_jpy_incl + c.sales_postage_jpy_incl - COALESCE(cp.coupon_shop_jpy_incl, 0),
    2) AS net_sales_jpy_incl,
  -- variable_margin = net_sales - cogs - shipping - mall_fee
  ROUND(
    (c.sales_principal_jpy_incl + c.sales_postage_jpy_incl - COALESCE(cp.coupon_shop_jpy_incl, 0))
    - COALESCE(cl.unit_cost_snapshot_incl, 0) * MAX(0, c.units_ordered - COALESCE(rf.units_cancelled, 0))
    - COALESCE(sl.unit_shipping_cost, 0) * MAX(0, c.units_ordered - COALESCE(rf.units_cancelled, 0))
    - (c.sales_principal_jpy_incl + c.sales_postage_jpy_incl - COALESCE(cp.coupon_all_jpy_incl, 0)) * 0.10,
    2) AS variable_margin_jpy_incl,
  -- refund_adjusted_net_sales = net_sales - refund_amount
  ROUND(
    c.sales_principal_jpy_incl + c.sales_postage_jpy_incl
    - COALESCE(cp.coupon_shop_jpy_incl, 0)
    - COALESCE(rf.refund_amount_jpy_incl, 0),
    2) AS refund_adjusted_net_sales_jpy_incl,
  COALESCE(cl.cost_status, 'missing_cost') AS cost_status,
  CASE WHEN cl.cost_status = 'complete' THEN 1 ELSE 0 END AS is_cost_complete,
  -- data_quality_score (cost / shipping / fee[Amazon 以外は estimated 固定で 10] / coupon)
  (CASE COALESCE(cl.cost_status, 'missing_cost')
        WHEN 'complete' THEN 25 WHEN 'partial_cost' THEN 10 ELSE 0 END
   + CASE COALESCE(sl.shipping_quality, 'missing')
        WHEN 'actual' THEN 25
        WHEN 'estimated_rates' THEN 20
        WHEN 'estimated_fallback' THEN 15
        ELSE 0 END
   + 10  -- 楽天 mall_fee は estimated 固定 (10% 一律)
   + CASE WHEN cp.coupon_shop_jpy_incl IS NOT NULL THEN 25 ELSE 10 END) AS data_quality_score,
  c.price_variance_warning,
  -- メタ
  'raw_rakuten_orders + fact_promotion_cost + fact_returns + m_products + product_shipping' AS source_layer_summary,
  c.source_row_count,
  CURRENT_TIMESTAMP AS built_at
FROM daily_aggregated c
LEFT JOIN sku_resolved sr ON sr.rakuten_code = c.rakuten_code
LEFT JOIN cost_lookup cl ON cl.rakuten_code = c.rakuten_code
LEFT JOIN shipping_lookup sl ON sl.rakuten_code = c.rakuten_code
LEFT JOIN coupon_agg cp ON cp.date_jst = c.date_jst AND cp.rakuten_code = c.rakuten_code
LEFT JOIN refund_agg rf ON rf.date_jst = c.date_jst AND rf.rakuten_code = c.rakuten_code
ON CONFLICT (date_jst, rakuten_code) DO UPDATE SET
  -- snapshot 列は更新しない (不変条件、Codex Round 11-13 で確定)
  -- unit_cost_snapshot_incl     ← 保持
  -- cost_snapshot_date_jst      ← 保持
  -- is_cost_complete            ← 保持
  -- cost_status                 ← 保持
  ne_code                          = excluded.ne_code,
  sku_resolution                   = excluded.sku_resolution,
  product_name                     = excluded.product_name,
  units_ordered                    = excluded.units_ordered,
  units_cancelled                  = excluded.units_cancelled,
  units_net_sold                   = excluded.units_net_sold,
  sales_principal_jpy_incl         = excluded.sales_principal_jpy_incl,
  sales_postage_jpy_incl           = excluded.sales_postage_jpy_incl,
  coupon_shop_jpy_incl             = excluded.coupon_shop_jpy_incl,
  coupon_all_jpy_incl              = excluded.coupon_all_jpy_incl,
  promotion_jpy_incl               = excluded.promotion_jpy_incl,
  refund_amount_jpy_incl           = excluded.refund_amount_jpy_incl,
  mall_fee_jpy_incl                = excluded.mall_fee_jpy_incl,
  shipping_cost_jpy_incl           = excluded.shipping_cost_jpy_incl,
  shipping_quality                 = excluded.shipping_quality,
  -- latest 参考値は最新で更新可
  latest_unit_cost_reference_incl  = excluded.latest_unit_cost_reference_incl,
  -- cogs / 利益は「既存 snapshot × 新 units」で再計算
  cogs_amount_jpy_incl             = ROUND(
    COALESCE(f_rakuten_finance_sku_daily_v1.unit_cost_snapshot_incl, 0)
    * excluded.units_net_sold,
    2),
  gross_sales_jpy_incl             = excluded.gross_sales_jpy_incl,
  net_sales_jpy_incl               = excluded.net_sales_jpy_incl,
  variable_margin_jpy_incl         = ROUND(
    excluded.net_sales_jpy_incl
    - COALESCE(f_rakuten_finance_sku_daily_v1.unit_cost_snapshot_incl, 0) * excluded.units_net_sold
    - excluded.shipping_cost_jpy_incl
    - excluded.mall_fee_jpy_incl,
    2),
  refund_adjusted_net_sales_jpy_incl = excluded.refund_adjusted_net_sales_jpy_incl,
  data_quality_score               = excluded.data_quality_score,
  price_variance_warning           = excluded.price_variance_warning,
  source_layer_summary             = excluded.source_layer_summary,
  source_row_count                 = excluded.source_row_count,
  built_at                         = excluded.built_at;

DROP TABLE IF EXISTS _silver_rakuten_orders_v1;
