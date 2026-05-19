-- build_f_qoo10_finance_sku_daily_v1.sql
-- ticket: Qoo10 Phase 1 A-2
-- design: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §7
--
-- パラメータ (better-sqlite3 named):
--   :year_month_int  対象月 YYYYMM
--   :build_date      初回 build 日 YYYY-MM-DD (snapshot 日付)
--
-- 売上計上方針 (v0.11 確定):
--   - silver: shipping_status = 'Delivered(5)' AND legacy_fields_missing = 0 (whitelist + 旧 raw 除外)
--   - 集計日 = substr(order_date, 1, 10)
--   - is_frozen_after_horizon=0 OR =1 両方含める (frozen 行も過去の値で集計対象、LINEギフト R2 #1 同型)
--   - units_cancelled = 0 (Phase 1a)
--   - 按分なし (1 orderNo 1 明細の grain そのまま、SUM)
--
-- 行レベル判別 (Codex R1/R2/R3 反映):
--   1. settle_price_formula_scope_per_line ('mixed' を最優先):
--        'mixed'            if oversea_consignment='Y' AND cod_price > 0   ★ 最優先
--        'oversea'          if oversea_consignment='Y' AND cod_price = 0
--        'cod'              if oversea_consignment='N' AND cod_price > 0
--        'domestic_non_cod' if oversea_consignment='N' AND cod_price = 0
--      集約規則: COUNT(DISTINCT scope_per_line)=1 ならその値、複数なら 'mixed'
--   2. promo_type_row (0 除算ガード、Codex R2 Medium #6):
--        WITH gross = order_price * order_qty,
--             rate  = CASE WHEN gross > 0 THEN discount * 1.0 / gross ELSE NULL END
--        → 'megawari'             if discount > 0 AND gross > 0 AND ABS(rate - 0.20) < 0.005
--          'megapo'               if discount > 0 AND gross > 0 AND ABS(rate - 0.10) < 0.005
--          'other_platform_promo' if discount > 0 (上記以外 or gross = 0)
--          NULL                   otherwise (discount = 0)
--
-- partial margin (Phase A, partial_pending_settlement_csv):
--   variable_margin = net_settlement_api - cogs - shipping_cost
--   ※ ショップ負担 promo (メガ割 10% + メガポ 10%) は Phase A 未反映、Phase B-2 で精算 CSV 統合
--   ※ Qoo10 公式 SettlePrice = round(orderPrice × 0.9) × qty (= API 自動的に 10% 手数料引かれ済)
--
-- SKU 解決 1 段 (v0.7 で manual_map 削除、v0.8 で parent_match 削除):
--   1. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(seller_item_code))  (= master_match)
--   2. NULL → unresolved_sku_flag = 1、sku_code = '__UNRESOLVED__:' || seller_item_code
--      (= deterministic UPSERT key 化、Codex R3 High #3 反映)
--
-- UPSERT 不変条件 (au PAY/LINEギフト 同型):
--   cost_status='complete' 行のみ snapshot 凍結 (取引時点原価)。partial_cost / missing_cost は replaceable。
--
-- 構造: Step 0 raw freeze first → Step 1 silver → Step 2 daily_aggregated + sku_resolved + cost_lookup
--      → Step 3 UPSERT (resolved_keys TEMP 作成含む) → Step 3.6 fact orphan delete → Step 4 cleanup

-- ─── Step 0: raw freeze first (LINEギフト R4 Critical 同型、Codex R1 #3) ───
-- order_date < jst_today - 76d の raw 行を frozen=1 化 (Qoo10 特有: order_date 基準、76d = 90 - 14 安全マージン)
-- Step 1 (silver) より先に実行することで、silver の is_frozen_after_horizon が今回 freeze 分を含む
UPDATE raw_qoo10_orders
SET is_frozen_after_horizon = 1
WHERE is_frozen_after_horizon = 0
  AND substr(order_date, 1, 10) < strftime('%Y-%m-%d', date('now', '+9 hours', '-76 days'));

-- ─── Step 1: silver (whitelist=Delivered(5) + legacy 除外 + 月絞り + 行レベル判別) ───
DROP TABLE IF EXISTS _silver_qoo10_delivered_v1;
CREATE TEMP TABLE _silver_qoo10_delivered_v1 AS
WITH base AS (
  SELECT
    substr(order_date, 1, 10) AS date_jst,
    order_id,
    -- 2026-05-19 A-2 patch: 3 段 fallback 用の 3 つのキー候補を silver に保持
    -- Codex R2 patch High #1 反映: GROUP BY 用 internal key と m_products lookup 用 key を分離
    --   - combined_internal: 区切り文字 char(31) 入りで連結衝突不能 (('ab','c') vs ('a','bc') の事故防止)
    --   - lookup_combined  : 区切り無し (Qoo10 実 SKU 命名規則 = seller || opt、m_products と一致)
    LOWER(TRIM(seller_item_code)) AS raw_sku,                                 -- seller_item_code 単独 (tier3、lookup 兼用)
    LOWER(TRIM(COALESCE(option_code, ''))) AS raw_opt,                        -- option_code 単独 (tier2、空文字なら NULL 扱い)
    LOWER(TRIM(seller_item_code) || COALESCE(TRIM(option_code), '')) AS lookup_combined, -- m_products 突合用 (tier1 lookup key、命名規則準拠 = 区切り無し)
    LOWER(TRIM(seller_item_code)) || char(31) || LOWER(TRIM(COALESCE(option_code, ''))) AS combined_internal, -- GROUP BY 用、衝突不能 internal key
    LOWER(TRIM(COALESCE(item_code, ''))) AS parent_item_code,
    COALESCE(option_info, '') AS variant_key,
    COALESCE(item_title, '') AS product_name,
    CAST(pack_no AS INTEGER) AS qoo10_item_id_raw,  -- audit
    CAST(item_code AS INTEGER) AS qoo10_item_id,    -- = itemCode (Qoo10 内部数字 ID)
    CAST(order_qty AS INTEGER) AS quantity,
    CAST(order_price AS REAL) AS unit_price,
    CAST(total AS REAL) AS customer_paid,
    CAST(settle_price AS REAL) AS settle_price,
    CAST(discount AS REAL) AS discount,
    CAST(seller_discount AS REAL) AS seller_discount,
    CAST(cart_discount_seller AS REAL) AS cart_discount_seller,
    CAST(cart_discount_qoo10 AS REAL) AS cart_discount_qoo10,
    -- gross = order_price × order_qty (0 除算ガード用)
    (CAST(order_price AS REAL) * CAST(order_qty AS INTEGER)) AS gross,
    -- 送料 (Phase A: shipping_rate、現状 0)
    COALESCE(CAST(shipping_rate AS REAL), 0) AS shipping_cost_per_line,
    CASE
      WHEN shipping_rate IS NULL OR shipping_rate = 0 THEN 'no_shipping_in_api'
      ELSE 'actual_api'
    END AS shipping_quality_per_line,
    -- scope 行レベル判別 ('mixed' 最優先、Codex R2 High #4)
    CASE
      WHEN oversea_consignment = 'Y' AND COALESCE(cod_price, 0) > 0 THEN 'mixed'
      WHEN oversea_consignment = 'Y' AND COALESCE(cod_price, 0) = 0 THEN 'oversea'
      WHEN oversea_consignment = 'N' AND COALESCE(cod_price, 0) > 0 THEN 'cod'
      ELSE 'domestic_non_cod'
    END AS scope_per_line,
    -- 海外/COD 追加手数料 (Phase A 0、将来 API 実額)
    0 AS extra_fee_oversea_per_line,
    CAST(COALESCE(cod_price, 0) AS REAL) AS cod_fee_per_line,
    -- delivered/shipping lag (NULL 安全、JST 文字列比較)
    CASE
      WHEN delivered_date IS NOT NULL AND delivered_date <> '' AND order_date IS NOT NULL AND delivered_date >= order_date
      THEN CAST(julianday(substr(delivered_date, 1, 10)) - julianday(substr(order_date, 1, 10)) AS INTEGER)
      ELSE NULL
    END AS delivered_lag_days,
    CASE
      WHEN shipping_date IS NOT NULL AND shipping_date <> '' AND order_date IS NOT NULL AND shipping_date >= order_date
      THEN CAST(julianday(substr(shipping_date, 1, 10)) - julianday(substr(order_date, 1, 10)) AS INTEGER)
      ELSE NULL
    END AS shipping_lag_days,
    -- oversea/cod count audit
    CASE WHEN oversea_consignment = 'Y' THEN 1 ELSE 0 END AS oversea_count,
    payment_method,
    -- 90日境界 frozen 状態 (Step 0 で raw freeze 済の値)
    first_seen_at,
    last_seen_at,
    is_frozen_after_horizon
  FROM raw_qoo10_orders
  WHERE shipping_status = 'Delivered(5)'
    AND legacy_fields_missing = 0
    AND CAST(strftime('%Y%m', substr(order_date, 1, 10)) AS INTEGER) = :year_month_int
    AND seller_item_code IS NOT NULL AND TRIM(seller_item_code) <> ''
    AND order_qty IS NOT NULL AND order_qty > 0
    AND order_date IS NOT NULL AND order_date <> ''
)
SELECT
  *,
  -- promo_type_row (行レベル判別、Codex R1 #5 + R2 Medium #6)
  CASE
    WHEN discount > 0 AND gross > 0 AND ABS((discount * 1.0 / gross) - 0.20) < 0.005 THEN 'megawari'
    WHEN discount > 0 AND gross > 0 AND ABS((discount * 1.0 / gross) - 0.10) < 0.005 THEN 'megapo'
    WHEN discount > 0 THEN 'other_platform_promo'
    ELSE NULL
  END AS promo_type_row,
  -- SettlePrice 公式 match 判定 (domestic_non_cod scope の母集団検証用)
  -- 公式: SettlePrice = round(orderPrice × 0.9) × qty
  CASE
    WHEN ABS(settle_price - (ROUND(unit_price * 0.9) * quantity)) < 0.5 THEN 1
    ELSE 0
  END AS settle_price_formula_match
FROM base;

-- 2026-05-19 A-2 patch: GROUP BY 用に combined_internal で index (衝突不能 key)
CREATE INDEX _silver_qoo10_v1_idx ON _silver_qoo10_delivered_v1 (date_jst, combined_internal);

-- ─── Step 2 + 3: UPSERT 本体 (按分なし、daily_aggregated + sku_resolved + cost_lookup を CTE で組み立て) ───
-- 同時に Step 3 で _silver_qoo10_resolved_keys を作る (Step 3.6 orphan delete 用、Codex R1 #3)
DROP TABLE IF EXISTS _silver_qoo10_resolved_keys;
CREATE TEMP TABLE _silver_qoo10_resolved_keys (
  date_jst TEXT NOT NULL,
  sku_code TEXT NOT NULL,
  PRIMARY KEY (date_jst, sku_code)
);

-- daily_aggregated + sku_resolved + cost_lookup は CTE で組み立てて UPSERT に流す
INSERT INTO f_qoo10_finance_sku_daily_v1 (
  date_jst, sku_code, ne_code, qoo10_item_id, parent_item_code, variant_key, resolution_method, match_tier, unresolved_sku_flag, product_name,
  units_ordered, units_cancelled, units_net_sold,
  gmv_list_price_jpy_incl, customer_paid_jpy_incl, net_settlement_api_jpy_incl,
  platform_fee_jpy_incl, mall_fee_calc_method, settle_price_formula_scope,
  extra_fee_oversea_jpy_incl, cod_fee_jpy_incl,
  megawari_order_count, megawari_discount_amount_jpy_incl,
  megapo_order_count, megapo_discount_amount_jpy_incl,
  other_promo_order_count, other_promo_discount_jpy_incl,
  total_platform_promo_jpy_incl, qoo10_cart_discount_jpy_incl, seller_discount_api_jpy_incl,
  shop_promo_burden_jpy_incl, shop_promo_burden_status,
  domestic_non_cod_line_count, domestic_non_cod_formula_match_count,
  shipping_cost_jpy_incl, shipping_quality,
  unit_cost_snapshot_incl, cost_snapshot_date_jst, latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
  variable_margin_jpy_incl, margin_confidence,
  delivered_lag_days, shipping_lag_days, oversea_count, payment_methods_json,
  first_seen_in_api_at, last_seen_in_api_at, is_frozen_after_horizon,
  cost_status, is_cost_complete, data_quality_score,
  order_count, line_count, source_layer_summary, source_row_count, built_at
)
WITH
daily_aggregated AS (
  SELECT
    date_jst,
    -- 2026-05-19 A-2 patch: GROUP BY は combined_internal (区切り入り、衝突不能 = Codex R2 patch High #1)
    -- 旧 (seller_item_code 単独) では variation 商品が 1 行に潰れて variation 別売上が見えなかった。
    combined_internal,
    -- 各 tier 候補も保持 (sku_resolved CTE で 3 段 fallback を試すため、GROUP 内代表値 = 一意)
    MAX(lookup_combined) AS lookup_combined,
    MAX(raw_sku) AS raw_sku,
    MAX(raw_opt) AS raw_opt,
    MAX(parent_item_code) AS parent_item_code,
    MAX(variant_key) AS variant_key,
    MAX(product_name) AS product_name,
    MAX(qoo10_item_id) AS qoo10_item_id,
    SUM(quantity) AS units_ordered,
    SUM(gross) AS gmv_list_price_jpy_incl,
    SUM(customer_paid) AS customer_paid_jpy_incl,
    SUM(settle_price) AS net_settlement_api_jpy_incl,
    SUM(gross - settle_price) AS platform_fee_jpy_incl,
    -- scope 集約: COUNT(DISTINCT)=1 ならその値、複数なら 'mixed'
    CASE
      WHEN COUNT(DISTINCT scope_per_line) = 1 THEN MAX(scope_per_line)
      ELSE 'mixed'
    END AS settle_price_formula_scope,
    SUM(extra_fee_oversea_per_line) AS extra_fee_oversea_jpy_incl,
    SUM(cod_fee_per_line) AS cod_fee_jpy_incl,
    -- promo 別集計 (行レベル promo_type_row でグループ化)
    SUM(CASE WHEN promo_type_row = 'megawari' THEN 1 ELSE 0 END) AS megawari_order_count,
    SUM(CASE WHEN promo_type_row = 'megawari' THEN discount ELSE 0 END) AS megawari_discount_amount_jpy_incl,
    SUM(CASE WHEN promo_type_row = 'megapo' THEN 1 ELSE 0 END) AS megapo_order_count,
    SUM(CASE WHEN promo_type_row = 'megapo' THEN discount ELSE 0 END) AS megapo_discount_amount_jpy_incl,
    SUM(CASE WHEN promo_type_row = 'other_platform_promo' THEN 1 ELSE 0 END) AS other_promo_order_count,
    SUM(CASE WHEN promo_type_row = 'other_platform_promo' THEN discount ELSE 0 END) AS other_promo_discount_jpy_incl,
    SUM(discount) AS total_platform_promo_jpy_incl,
    SUM(cart_discount_qoo10) AS qoo10_cart_discount_jpy_incl,
    SUM(seller_discount + cart_discount_seller) AS seller_discount_api_jpy_incl,
    -- domestic_non_cod 別集計 (Codex R3 High #1 反映、scope='mixed' でも domestic 部分を DQ で検証可能化)
    SUM(CASE WHEN scope_per_line = 'domestic_non_cod' THEN 1 ELSE 0 END) AS domestic_non_cod_line_count,
    SUM(CASE WHEN scope_per_line = 'domestic_non_cod' AND settle_price_formula_match = 1 THEN 1 ELSE 0 END) AS domestic_non_cod_formula_match_count,
    -- 送料
    SUM(shipping_cost_per_line) AS shipping_cost_jpy_incl,
    CASE
      WHEN MIN(shipping_quality_per_line) = 'actual_api' AND MAX(shipping_quality_per_line) = 'actual_api' THEN 'actual_api'
      WHEN MIN(shipping_quality_per_line) = 'no_shipping_in_api' AND MAX(shipping_quality_per_line) = 'no_shipping_in_api' THEN 'no_shipping_in_api'
      ELSE 'estimated_fallback'
    END AS shipping_quality,
    AVG(delivered_lag_days) AS delivered_lag_days,
    AVG(shipping_lag_days) AS shipping_lag_days,
    SUM(oversea_count) AS oversea_count,
    -- payment_methods_json (重複除去のみ、件数上限なし - audit 目的、決済手段は数種に収束する前提)
    -- Codex R1 (A-2) Medium #2 反映: GROUP_CONCAT 全 NULL の場合 NULL になるので COALESCE で '[]' フォールバック
    '[' || COALESCE(GROUP_CONCAT(DISTINCT CASE WHEN payment_method IS NOT NULL AND payment_method <> '' THEN '"' || replace(payment_method, '"', '\"') || '"' END), '') || ']' AS payment_methods_json,
    MIN(first_seen_at) AS first_seen_in_api_at,
    MAX(last_seen_at) AS last_seen_in_api_at,
    MAX(is_frozen_after_horizon) AS is_frozen_after_horizon,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(*) AS line_count
  FROM _silver_qoo10_delivered_v1
  GROUP BY date_jst, combined_internal
),
-- 2026-05-19 A-2 patch: 3 段 fallback (combined → option_only → seller_only → unresolved)
-- Codex 助言: m_products を mp_norm で 1 回正規化、LEFT JOIN 3 本で tier と ne_code を 1 pass 解決
-- Codex R2 patch Medium #1 反映: 商品コード重複対策で GROUP BY で一意化、MIN で安定値選択
mp_norm AS (
  SELECT LOWER(TRIM(商品コード)) AS k, MIN(商品コード) AS ne_code
  FROM m_products
  GROUP BY LOWER(TRIM(商品コード))
),
sku_resolved AS (
  SELECT
    da.date_jst,
    da.combined_internal,
    da.lookup_combined,
    da.raw_sku,
    da.raw_opt,
    -- tier 判定: COALESCE で優先順位、JOIN 結果 (mp_*.ne_code) の NULL/NOT NULL で tier 確定
    CASE
      WHEN mp_c.ne_code IS NOT NULL THEN 'combined'
      WHEN da.raw_opt <> '' AND mp_o.ne_code IS NOT NULL THEN 'option_only'
      WHEN mp_s.ne_code IS NOT NULL THEN 'seller_only'
      ELSE 'unresolved'
    END AS match_tier,
    -- ne_code: hit したキーの 商品コード (tier 優先順位で COALESCE、unresolved 時は NULL)
    COALESCE(mp_c.ne_code,
             CASE WHEN da.raw_opt <> '' THEN mp_o.ne_code END,
             mp_s.ne_code) AS ne_code,
    -- resolution_method は後方互換 2 値 (master_match / unresolved)、内訳は match_tier で
    CASE
      WHEN mp_c.ne_code IS NOT NULL
        OR (da.raw_opt <> '' AND mp_o.ne_code IS NOT NULL)
        OR mp_s.ne_code IS NOT NULL
        THEN 'master_match'
      ELSE 'unresolved'
    END AS resolution_method,
    -- sku_code 確定: tier に応じた m_products 突合 lookup キー、unresolved 時は '__UNRESOLVED__:' || lookup_combined で deterministic
    CASE
      WHEN mp_c.ne_code IS NOT NULL THEN da.lookup_combined
      WHEN da.raw_opt <> '' AND mp_o.ne_code IS NOT NULL THEN da.raw_opt
      WHEN mp_s.ne_code IS NOT NULL THEN da.raw_sku
      ELSE '__UNRESOLVED__:' || da.lookup_combined
    END AS sku_code_resolved
  FROM daily_aggregated da
  LEFT JOIN mp_norm mp_c ON mp_c.k = da.lookup_combined
  LEFT JOIN mp_norm mp_o ON da.raw_opt <> '' AND mp_o.k = da.raw_opt
  LEFT JOIN mp_norm mp_s ON mp_s.k = da.raw_sku
),
cost_lookup AS (
  SELECT
    sr.date_jst, sr.combined_internal, sr.match_tier, sr.ne_code, sr.resolution_method, sr.sku_code_resolved,
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL
        THEN COALESCE(eg.genka, mp.原価) * (1 + COALESCE(mp.消費税率, 0.10))
      ELSE NULL
    END AS unit_cost_snapshot_incl,
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL THEN
        CASE WHEN sr.resolution_method = 'master_match' THEN 'complete'
             ELSE 'missing_cost' END
      ELSE 'missing_cost'
    END AS cost_status
  FROM sku_resolved sr
  LEFT JOIN m_products mp ON mp.商品コード = sr.ne_code
  LEFT JOIN exception_genka eg ON eg.sku = sr.ne_code
)
SELECT
  c.date_jst,
  cl.sku_code_resolved AS sku_code,
  cl.ne_code,
  c.qoo10_item_id,
  c.parent_item_code,
  c.variant_key,
  cl.resolution_method,
  cl.match_tier,
  CASE WHEN cl.ne_code IS NULL THEN 1 ELSE 0 END AS unresolved_sku_flag,
  c.product_name,
  c.units_ordered, 0 AS units_cancelled, c.units_ordered AS units_net_sold,
  -- 売上 3 列
  ROUND(c.gmv_list_price_jpy_incl, 2) AS gmv_list_price_jpy_incl,
  ROUND(c.customer_paid_jpy_incl, 2) AS customer_paid_jpy_incl,
  ROUND(c.net_settlement_api_jpy_incl, 2) AS net_settlement_api_jpy_incl,
  -- 手数料
  ROUND(c.platform_fee_jpy_incl, 2) AS platform_fee_jpy_incl,
  'actual_api' AS mall_fee_calc_method,
  c.settle_price_formula_scope,
  -- 海外/COD
  ROUND(c.extra_fee_oversea_jpy_incl, 2) AS extra_fee_oversea_jpy_incl,
  ROUND(c.cod_fee_jpy_incl, 2) AS cod_fee_jpy_incl,
  -- promo 集計
  c.megawari_order_count,
  ROUND(c.megawari_discount_amount_jpy_incl, 2) AS megawari_discount_amount_jpy_incl,
  c.megapo_order_count,
  ROUND(c.megapo_discount_amount_jpy_incl, 2) AS megapo_discount_amount_jpy_incl,
  c.other_promo_order_count,
  ROUND(c.other_promo_discount_jpy_incl, 2) AS other_promo_discount_jpy_incl,
  ROUND(c.total_platform_promo_jpy_incl, 2) AS total_platform_promo_jpy_incl,
  ROUND(c.qoo10_cart_discount_jpy_incl, 2) AS qoo10_cart_discount_jpy_incl,
  ROUND(c.seller_discount_api_jpy_incl, 2) AS seller_discount_api_jpy_incl,
  -- ショップ負担 (Phase A 0)
  0 AS shop_promo_burden_jpy_incl,
  'pending_settlement_csv' AS shop_promo_burden_status,
  -- domestic_non_cod 別集計
  c.domestic_non_cod_line_count,
  c.domestic_non_cod_formula_match_count,
  -- 送料
  ROUND(c.shipping_cost_jpy_incl, 2) AS shipping_cost_jpy_incl,
  c.shipping_quality,
  -- 原価 snapshot
  cl.unit_cost_snapshot_incl,
  :build_date AS cost_snapshot_date_jst,
  cl.unit_cost_snapshot_incl AS latest_unit_cost_reference_incl,
  ROUND(COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered, 2) AS cogs_amount_jpy_incl,
  -- variable margin (Phase A、partial_pending_settlement_csv = net_settlement - cogs - shipping)
  ROUND(
    c.net_settlement_api_jpy_incl
    - COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered
    - c.shipping_cost_jpy_incl,
    2) AS variable_margin_jpy_incl,
  'partial_pending_settlement_csv' AS margin_confidence,
  -- audit
  CAST(c.delivered_lag_days AS INTEGER) AS delivered_lag_days,
  CAST(c.shipping_lag_days AS INTEGER) AS shipping_lag_days,
  c.oversea_count,
  c.payment_methods_json,
  -- 90日境界 frozen 状態
  c.first_seen_in_api_at,
  c.last_seen_in_api_at,
  c.is_frozen_after_horizon,
  -- 品質
  COALESCE(cl.cost_status, 'missing_cost') AS cost_status,
  CASE WHEN cl.cost_status = 'complete' THEN 1 ELSE 0 END AS is_cost_complete,
  -- data_quality_score: cost (40) + shipping (25) + mall_fee (25) + sku_resolution (10) = 100
  (CASE COALESCE(cl.cost_status, 'missing_cost')
        WHEN 'complete' THEN 40 ELSE 0 END
   + CASE c.shipping_quality
        WHEN 'actual_api' THEN 25 WHEN 'no_shipping_in_api' THEN 20
        WHEN 'estimated_rates' THEN 15 WHEN 'estimated_fallback' THEN 10 ELSE 0 END
   + 25  -- mall_fee は actual_api 固定 (SettlePrice 公式由来) なので満点
   + CASE cl.resolution_method
        WHEN 'master_match' THEN 10 ELSE 0 END
  ) AS data_quality_score,
  -- メタ
  c.order_count, c.line_count,
  'raw_qoo10_orders + m_products (Phase A partial_pending_settlement_csv margin)' AS source_layer_summary,
  c.line_count AS source_row_count,
  CURRENT_TIMESTAMP AS built_at
FROM daily_aggregated c
JOIN cost_lookup cl ON cl.date_jst = c.date_jst AND cl.combined_internal = c.combined_internal
ON CONFLICT (date_jst, sku_code) DO UPDATE SET
  ne_code                              = excluded.ne_code,
  qoo10_item_id                        = excluded.qoo10_item_id,
  parent_item_code                     = excluded.parent_item_code,
  variant_key                          = excluded.variant_key,
  resolution_method                    = excluded.resolution_method,
  match_tier                           = excluded.match_tier,
  unresolved_sku_flag                  = excluded.unresolved_sku_flag,
  product_name                         = excluded.product_name,
  units_ordered                        = excluded.units_ordered,
  units_cancelled                      = excluded.units_cancelled,
  units_net_sold                       = excluded.units_net_sold,
  gmv_list_price_jpy_incl              = excluded.gmv_list_price_jpy_incl,
  customer_paid_jpy_incl               = excluded.customer_paid_jpy_incl,
  net_settlement_api_jpy_incl          = excluded.net_settlement_api_jpy_incl,
  platform_fee_jpy_incl                = excluded.platform_fee_jpy_incl,
  mall_fee_calc_method                 = excluded.mall_fee_calc_method,
  settle_price_formula_scope           = excluded.settle_price_formula_scope,
  extra_fee_oversea_jpy_incl           = excluded.extra_fee_oversea_jpy_incl,
  cod_fee_jpy_incl                     = excluded.cod_fee_jpy_incl,
  megawari_order_count                 = excluded.megawari_order_count,
  megawari_discount_amount_jpy_incl    = excluded.megawari_discount_amount_jpy_incl,
  megapo_order_count                   = excluded.megapo_order_count,
  megapo_discount_amount_jpy_incl      = excluded.megapo_discount_amount_jpy_incl,
  other_promo_order_count              = excluded.other_promo_order_count,
  other_promo_discount_jpy_incl        = excluded.other_promo_discount_jpy_incl,
  total_platform_promo_jpy_incl        = excluded.total_platform_promo_jpy_incl,
  qoo10_cart_discount_jpy_incl         = excluded.qoo10_cart_discount_jpy_incl,
  seller_discount_api_jpy_incl         = excluded.seller_discount_api_jpy_incl,
  shop_promo_burden_jpy_incl           = excluded.shop_promo_burden_jpy_incl,
  shop_promo_burden_status             = excluded.shop_promo_burden_status,
  domestic_non_cod_line_count          = excluded.domestic_non_cod_line_count,
  domestic_non_cod_formula_match_count = excluded.domestic_non_cod_formula_match_count,
  shipping_cost_jpy_incl               = excluded.shipping_cost_jpy_incl,
  shipping_quality                     = excluded.shipping_quality,
  latest_unit_cost_reference_incl      = excluded.latest_unit_cost_reference_incl,
  -- snapshot 列の凍結は cost_status='complete' 行のみ (au PAY 同型)
  unit_cost_snapshot_incl              = CASE WHEN f_qoo10_finance_sku_daily_v1.cost_status = 'complete' THEN f_qoo10_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END,
  cost_snapshot_date_jst               = CASE WHEN f_qoo10_finance_sku_daily_v1.cost_status = 'complete' THEN f_qoo10_finance_sku_daily_v1.cost_snapshot_date_jst ELSE excluded.cost_snapshot_date_jst END,
  cost_status                          = CASE WHEN f_qoo10_finance_sku_daily_v1.cost_status = 'complete' THEN 'complete' ELSE excluded.cost_status END,
  is_cost_complete                     = CASE WHEN f_qoo10_finance_sku_daily_v1.cost_status = 'complete' THEN 1 ELSE excluded.is_cost_complete END,
  -- cogs / margin は「有効 snapshot × 新 units_net_sold」で再計算
  cogs_amount_jpy_incl                 = ROUND(COALESCE(CASE WHEN f_qoo10_finance_sku_daily_v1.cost_status = 'complete' THEN f_qoo10_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END, 0) * excluded.units_net_sold, 2),
  variable_margin_jpy_incl             = ROUND(
    excluded.net_settlement_api_jpy_incl
    - COALESCE(CASE WHEN f_qoo10_finance_sku_daily_v1.cost_status = 'complete' THEN f_qoo10_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END, 0) * excluded.units_net_sold
    - excluded.shipping_cost_jpy_incl, 2),
  -- audit
  delivered_lag_days                   = excluded.delivered_lag_days,
  shipping_lag_days                    = excluded.shipping_lag_days,
  oversea_count                        = excluded.oversea_count,
  payment_methods_json                 = excluded.payment_methods_json,
  first_seen_in_api_at                 = excluded.first_seen_in_api_at,
  last_seen_in_api_at                  = excluded.last_seen_in_api_at,
  is_frozen_after_horizon              = excluded.is_frozen_after_horizon,
  data_quality_score                   = excluded.data_quality_score,
  order_count                          = excluded.order_count,
  line_count                           = excluded.line_count,
  source_layer_summary                 = excluded.source_layer_summary,
  source_row_count                     = excluded.source_row_count,
  built_at                             = excluded.built_at;

-- ─── Step 3 (続): _silver_qoo10_resolved_keys を埋める (orphan delete 用) ───
-- 2026-05-19 A-2 patch: 3 段 fallback で sku_code 確定 (UPSERT 側と完全同型のロジック)
-- Codex 助言: mp_norm CTE で m_products を 1 回正規化 (GROUP BY で重複排除)、LEFT JOIN 3 本で 1 pass 解決
-- lookup_combined (区切り無し、m_products 突合用) を JOIN キーに、combined_internal は不要
WITH mp_norm AS (
  SELECT LOWER(TRIM(商品コード)) AS k FROM m_products GROUP BY LOWER(TRIM(商品コード))
),
silver_keys AS (
  SELECT DISTINCT date_jst, lookup_combined, raw_opt, raw_sku FROM _silver_qoo10_delivered_v1
)
INSERT OR IGNORE INTO _silver_qoo10_resolved_keys (date_jst, sku_code)
SELECT
  da.date_jst,
  CASE
    WHEN mp_c.k IS NOT NULL THEN da.lookup_combined
    WHEN da.raw_opt <> '' AND mp_o.k IS NOT NULL THEN da.raw_opt
    WHEN mp_s.k IS NOT NULL THEN da.raw_sku
    ELSE '__UNRESOLVED__:' || da.lookup_combined
  END AS sku_code_resolved
FROM silver_keys da
LEFT JOIN mp_norm mp_c ON mp_c.k = da.lookup_combined
LEFT JOIN mp_norm mp_o ON da.raw_opt <> '' AND mp_o.k = da.raw_opt
LEFT JOIN mp_norm mp_s ON mp_s.k = da.raw_sku;

-- ─── Step 3.6: 集合ベース orphan delete (Codex R1 Critical #3 + R2 High #5 反映) ───
-- raw 直接突合をやめ、Step 3 で生成した _silver_qoo10_resolved_keys を使う
-- frozen 行は永久保持 (LINEギフト 同型)
DELETE FROM f_qoo10_finance_sku_daily_v1
WHERE substr(date_jst, 1, 7) = printf('%04d-%02d', :year_month_int / 100, :year_month_int % 100)
  AND is_frozen_after_horizon = 0
  AND NOT EXISTS (
    SELECT 1 FROM _silver_qoo10_resolved_keys k
    WHERE k.date_jst = f_qoo10_finance_sku_daily_v1.date_jst
      AND k.sku_code = f_qoo10_finance_sku_daily_v1.sku_code
  );

-- ─── Step 4: cleanup ───
DROP TABLE IF EXISTS _silver_qoo10_delivered_v1;
DROP TABLE IF EXISTS _silver_qoo10_resolved_keys;
