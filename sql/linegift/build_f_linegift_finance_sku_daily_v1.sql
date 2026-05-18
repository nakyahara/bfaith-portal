-- build_f_linegift_finance_sku_daily_v1.sql
-- ticket: LINEギフト Phase 1 A-2
-- design: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.5_20260515.md §7
--
-- パラメータ (better-sqlite3 named):
--   :year_month_int  対象月 YYYYMM
--   :build_date      初回 build 日 YYYY-MM-DD (snapshot 日付)
--
-- 売上計上方針 (v0.5 確定、A-0 verify 済):
--   - silver: status = 'received' (whitelist、recognized_on = received_on_jst)
--   - 集計日 = received_date_jst (= recognized_on)
--   - is_frozen_after_horizon=0 OR =1 両方含める (frozen 行も過去の値で集計対象、Round 2 #1)
--   - units_cancelled = 0 (Phase 1a)
--   - 按分なし (1注文1明細の grain そのまま、stock_count を SUM)
--
-- shipping_cost_per_line を silver で確定 (Round 2 #2、NULL 伝播防止):
--   shipping_cost_per_line = COALESCE(raw.shipping_fee, 0)
--   shipping_quality_per_line = CASE
--     WHEN raw.shipping_fee IS NULL OR raw.shipping_fee = 0 THEN 'no_shipping_in_api'
--     ELSE 'actual_api'
--   END
--
-- partial margin (Phase A、provisional_full_candidate):
--   variable_margin = gross_sales - cogs - mall_fee_actual - shipping_cost
--   ※ mall_fee は raw.fee の API 実額を集計 (rate 計算なし、calc_method='actual_api')
--
-- SKU 解決 2 段 fallback (variation.code 100% master_match の前提):
--   1. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(sku_code))  (= master_match)
--   2. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(parent_item_code))  (= parent_match、variation 不在時 fallback)
--   3. NULL → unresolved_sku_flag = 1
--
-- UPSERT 不変条件 (au PAY 同型):
--   cost_status='complete' (正規 SKU 解決で原価確定) の行のみ snapshot 凍結 = 取引時点原価を保持。
--   missing_cost / partial_cost (parent fallback で導出した推計) は replaceable:
--     原価が後から判明したら埋まる / 後で正規解決できたら 'complete' に昇格 / 導出失敗したら missing に戻る。
--
-- 構造: Step 0 raw freeze first → Step 1 silver → Step 2 daily_aggregated + sku_resolved + cost_lookup → Step 3 UPSERT
--      → Step 3.6 fact orphan delete (key 集合 NOT EXISTS) → Step 4 cleanup
--
-- ⚠️ Codex R1 medium #3 反映: freeze を Step 1 (silver) より先に実行することで、silver/UPSERT/fact の
--    is_frozen_after_horizon が「今回 freeze 化された行」を即時反映するようにする。
--    旧構造 (freeze を UPSERT 後に実行) では fact 列が 1 build 遅れ、frozen view が 1 run 遅れる問題があった。

-- ─── Step 0: raw freeze first (Round 4 Critical 反映、設計書 §12) ───
-- last_seen_at < jst_today - 14d の raw 行を frozen=1 化。
-- Step 1 (silver) より先に実行することで、silver の is_frozen_after_horizon が今回 freeze 分を含む。
UPDATE raw_linegift_orders
SET is_frozen_after_horizon = 1
WHERE is_frozen_after_horizon = 0
  AND substr(last_seen_at, 1, 10) < strftime('%Y-%m-%d', date('now', '+9 hours', '-14 days'));

-- ─── Step 1: silver (whitelist=received + 月絞り) ───
DROP TABLE IF EXISTS _silver_linegift_received_v1;
CREATE TEMP TABLE _silver_linegift_received_v1 AS
SELECT
  received_date_jst AS date_jst,
  order_id,
  LOWER(TRIM(sku_code)) AS sku_code,
  LOWER(TRIM(COALESCE(parent_item_code, ''))) AS parent_item_code,
  COALESCE(sku_name, '') AS variant_key,
  COALESCE(parent_item_name, '') AS product_name,
  CAST(stock_count AS INTEGER) AS quantity,
  CAST(selling_price AS REAL) AS unit_price,
  CAST(fee AS REAL) AS fee,
  -- shipping_cost_per_line を silver で確定 (NULL 伝播防止、Round 2 #2)
  COALESCE(CAST(shipping_fee AS REAL), 0) AS shipping_cost_per_line,
  CASE
    WHEN shipping_fee IS NULL OR shipping_fee = 0 THEN 'no_shipping_in_api'
    ELSE 'actual_api'
  END AS shipping_quality_per_line,
  -- LINEギフト 特有 audit 列
  bought_date_jst,
  -- delivered/received lag (NULL 安全)
  CASE
    WHEN delivered_on_unix IS NOT NULL AND bought_on_unix IS NOT NULL AND delivered_on_unix > bought_on_unix
    THEN CAST((delivered_on_unix - bought_on_unix) / 86400.0 AS INTEGER)
    ELSE NULL
  END AS delivered_lag_days,
  CASE
    WHEN received_on_unix IS NOT NULL AND bought_on_unix IS NOT NULL AND received_on_unix > bought_on_unix
    THEN CAST((received_on_unix - bought_on_unix) / 86400.0 AS INTEGER)
    ELSE NULL
  END AS received_lag_days,
  is_delivery_by_hand,
  COALESCE(delivery_agent, '') AS delivery_agent,
  -- 90日境界 frozen 状態 (raw 側で UPDATE 済み)
  first_seen_at,
  last_seen_at,
  is_frozen_after_horizon
FROM raw_linegift_orders
WHERE status = 'received'
  AND received_date_jst IS NOT NULL
  AND CAST(strftime('%Y%m', received_date_jst) AS INTEGER) = :year_month_int
  AND stock_count IS NOT NULL AND stock_count > 0
  AND selling_price IS NOT NULL AND selling_price > 0
  AND sku_code IS NOT NULL AND TRIM(sku_code) <> '';
CREATE INDEX _silver_linegift_v1_idx ON _silver_linegift_received_v1 (date_jst, sku_code);

-- ─── Step 2 + 3: UPSERT 本体 (按分なし、daily_aggregated + sku_resolved + cost_lookup を CTE で組み立て) ───
INSERT INTO f_linegift_finance_sku_daily_v1 (
  date_jst, sku_code, ne_code, parent_item_code, variant_key, resolution_method, unresolved_sku_flag, product_name,
  units_ordered, units_cancelled, units_net_sold,
  sales_principal_jpy_incl, gross_sales_jpy_incl,
  mall_fee_jpy_incl, mall_fee_calc_method,
  shipping_cost_jpy_incl, shipping_quality,
  unit_cost_snapshot_incl, cost_snapshot_date_jst, latest_unit_cost_reference_incl, cogs_amount_jpy_incl,
  variable_margin_jpy_incl, margin_confidence,
  recognized_on_jst, bought_date_jst, delivered_lag_days, received_lag_days, is_delivery_by_hand, delivery_agent,
  first_seen_in_api_at, last_seen_in_api_at, is_frozen_after_horizon,
  cost_status, is_cost_complete, data_quality_score,
  order_count, line_count, source_layer_summary, source_row_count, built_at
)
WITH
daily_aggregated AS (
  SELECT
    date_jst, sku_code,
    -- parent_item_code は基本一意のはず、念のため MAX で代表
    MAX(parent_item_code) AS parent_item_code,
    MAX(variant_key) AS variant_key,
    MAX(product_name) AS product_name,
    SUM(quantity) AS units_ordered,
    SUM(unit_price * quantity) AS sales_principal_jpy_incl,
    SUM(fee) AS mall_fee_jpy_incl,
    SUM(shipping_cost_per_line * quantity) AS shipping_cost_jpy_incl,
    -- shipping_quality 集約: 'no_shipping_in_api' のみなら 'no_shipping_in_api'、
    -- 'actual_api' が混じれば 'actual_api' (将来 API 仕様変更時)。今は全て 'no_shipping_in_api'
    CASE
      WHEN MIN(shipping_quality_per_line) = 'actual_api' AND MAX(shipping_quality_per_line) = 'actual_api' THEN 'actual_api'
      WHEN MIN(shipping_quality_per_line) = 'no_shipping_in_api' AND MAX(shipping_quality_per_line) = 'no_shipping_in_api' THEN 'no_shipping_in_api'
      ELSE 'estimated_fallback'  -- 混在は fallback (本来起きないが安全側)
    END AS shipping_quality,
    -- LINEギフト 特有 audit
    AVG(delivered_lag_days) AS delivered_lag_days,
    AVG(received_lag_days) AS received_lag_days,
    MAX(is_delivery_by_hand) AS is_delivery_by_hand,
    MAX(delivery_agent) AS delivery_agent,
    MIN(first_seen_at) AS first_seen_in_api_at,
    MAX(last_seen_at) AS last_seen_in_api_at,
    MAX(is_frozen_after_horizon) AS is_frozen_after_horizon,
    MIN(bought_date_jst) AS bought_date_jst,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(*) AS line_count
  FROM _silver_linegift_received_v1
  GROUP BY date_jst, sku_code
),
sku_resolved AS (
  SELECT
    da.date_jst, da.sku_code, da.parent_item_code,
    -- ne_code: master 優先、なければ parent fallback (variant 不在時の親代替)
    -- Codex R1 #2: parent fallback subquery にも parent_item_code <> '' guard を入れる。
    -- guard 無しだと m_products に空文字商品コードが万一 1 件あると resolution_method='unresolved'
    -- なのに ne_code だけ埋まる不整合が起きる (resolved_but_zero_cost_count 等を壊す)
    COALESCE(
      (SELECT 商品コード FROM m_products WHERE LOWER(TRIM(商品コード)) = da.sku_code LIMIT 1),
      (SELECT 商品コード FROM m_products WHERE da.parent_item_code <> '' AND LOWER(TRIM(商品コード)) = da.parent_item_code LIMIT 1)
    ) AS ne_code,
    CASE
      WHEN EXISTS (SELECT 1 FROM m_products WHERE LOWER(TRIM(商品コード)) = da.sku_code) THEN 'master_match'
      WHEN da.parent_item_code <> '' AND EXISTS (SELECT 1 FROM m_products WHERE LOWER(TRIM(商品コード)) = da.parent_item_code) THEN 'parent_match'
      ELSE 'unresolved'
    END AS resolution_method
  FROM daily_aggregated da
),
cost_lookup AS (
  SELECT
    sr.date_jst, sr.sku_code, sr.ne_code,
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL
        THEN COALESCE(eg.genka, mp.原価) * (1 + COALESCE(mp.消費税率, 0.10))
      ELSE NULL
    END AS unit_cost_snapshot_incl,
    CASE
      WHEN COALESCE(eg.genka, mp.原価) IS NOT NULL THEN
        CASE WHEN sr.resolution_method = 'master_match' THEN 'complete'
             WHEN sr.resolution_method = 'parent_match' THEN 'partial_cost'
             ELSE 'missing_cost' END
      ELSE 'missing_cost'
    END AS cost_status
  FROM sku_resolved sr
  LEFT JOIN m_products mp ON mp.商品コード = sr.ne_code
  LEFT JOIN exception_genka eg ON eg.sku = sr.ne_code
)
SELECT
  c.date_jst, c.sku_code, sr.ne_code, c.parent_item_code, c.variant_key, sr.resolution_method,
  CASE WHEN sr.ne_code IS NULL THEN 1 ELSE 0 END AS unresolved_sku_flag,
  c.product_name,
  c.units_ordered, 0 AS units_cancelled, c.units_ordered AS units_net_sold,
  -- 売上 (LINEギフト は送料込み統合価格、gross_sales = sales_principal)
  ROUND(c.sales_principal_jpy_incl, 2) AS sales_principal_jpy_incl,
  ROUND(c.sales_principal_jpy_incl, 2) AS gross_sales_jpy_incl,
  -- モール手数料 (API 実額)
  ROUND(c.mall_fee_jpy_incl, 2) AS mall_fee_jpy_incl,
  'actual_api' AS mall_fee_calc_method,
  -- 送料コスト (raw.shipping_fee × stock_count、現状 LINEギフト API は shipping_fee なし → 0)
  ROUND(c.shipping_cost_jpy_incl, 2) AS shipping_cost_jpy_incl,
  c.shipping_quality,
  -- 原価 snapshot
  cl.unit_cost_snapshot_incl,
  :build_date AS cost_snapshot_date_jst,
  cl.unit_cost_snapshot_incl AS latest_unit_cost_reference_incl,
  ROUND(COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered, 2) AS cogs_amount_jpy_incl,
  -- variable margin (Phase A, provisional_full_candidate, 4 要素 = gross - cogs - mall_fee - shipping)
  ROUND(
    c.sales_principal_jpy_incl
    - COALESCE(cl.unit_cost_snapshot_incl, 0) * c.units_ordered
    - c.mall_fee_jpy_incl
    - c.shipping_cost_jpy_incl,
    2) AS variable_margin_jpy_incl,
  'provisional_full_candidate' AS margin_confidence,
  -- LINEギフト 特有 audit
  c.date_jst AS recognized_on_jst,  -- = received_date_jst (= date_jst)、Codex #3 明示
  c.bought_date_jst,
  CAST(c.delivered_lag_days AS INTEGER) AS delivered_lag_days,
  CAST(c.received_lag_days AS INTEGER) AS received_lag_days,
  c.is_delivery_by_hand,
  c.delivery_agent,
  -- 90日境界 frozen 状態 (raw 側 freeze と同期、Step 3.5 で raw が先に UPDATE 済み)
  c.first_seen_in_api_at,
  c.last_seen_in_api_at,
  c.is_frozen_after_horizon,
  -- 品質
  COALESCE(cl.cost_status, 'missing_cost') AS cost_status,
  CASE WHEN cl.cost_status = 'complete' THEN 1 ELSE 0 END AS is_cost_complete,
  -- data_quality_score: cost (40) + shipping (25) + mall_fee (25) + sku_resolution (10) = 100
  (CASE COALESCE(cl.cost_status, 'missing_cost')
        WHEN 'complete' THEN 40 WHEN 'partial_cost' THEN 20 ELSE 0 END
   + CASE c.shipping_quality
        WHEN 'actual_api' THEN 25 WHEN 'no_shipping_in_api' THEN 20  -- Phase A 想定常態
        WHEN 'estimated_rates' THEN 15 WHEN 'estimated_fallback' THEN 10 ELSE 0 END
   + 25  -- mall_fee は actual_api 固定なので満点
   + CASE sr.resolution_method
        WHEN 'master_match' THEN 10 WHEN 'parent_match' THEN 5 ELSE 0 END
  ) AS data_quality_score,
  -- メタ
  c.order_count, c.line_count,
  'raw_linegift_orders + m_products (Phase A provisional_full_candidate margin)' AS source_layer_summary,
  c.line_count AS source_row_count,
  CURRENT_TIMESTAMP AS built_at
FROM daily_aggregated c
LEFT JOIN sku_resolved sr ON sr.date_jst = c.date_jst AND sr.sku_code = c.sku_code
LEFT JOIN cost_lookup cl ON cl.date_jst = c.date_jst AND cl.sku_code = c.sku_code
ON CONFLICT (date_jst, sku_code) DO UPDATE SET
  ne_code                          = excluded.ne_code,
  parent_item_code                 = excluded.parent_item_code,
  variant_key                      = excluded.variant_key,
  resolution_method                = excluded.resolution_method,
  unresolved_sku_flag              = excluded.unresolved_sku_flag,
  product_name                     = excluded.product_name,
  units_ordered                    = excluded.units_ordered,
  units_cancelled                  = excluded.units_cancelled,
  units_net_sold                   = excluded.units_net_sold,
  sales_principal_jpy_incl         = excluded.sales_principal_jpy_incl,
  gross_sales_jpy_incl             = excluded.gross_sales_jpy_incl,
  mall_fee_jpy_incl                = excluded.mall_fee_jpy_incl,
  mall_fee_calc_method             = excluded.mall_fee_calc_method,
  shipping_cost_jpy_incl           = excluded.shipping_cost_jpy_incl,
  shipping_quality                 = excluded.shipping_quality,
  latest_unit_cost_reference_incl  = excluded.latest_unit_cost_reference_incl,
  -- snapshot 列の凍結は cost_status='complete' 行のみ (au PAY 同型)
  unit_cost_snapshot_incl          = CASE WHEN f_linegift_finance_sku_daily_v1.cost_status = 'complete' THEN f_linegift_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END,
  cost_snapshot_date_jst           = CASE WHEN f_linegift_finance_sku_daily_v1.cost_status = 'complete' THEN f_linegift_finance_sku_daily_v1.cost_snapshot_date_jst ELSE excluded.cost_snapshot_date_jst END,
  cost_status                      = CASE WHEN f_linegift_finance_sku_daily_v1.cost_status = 'complete' THEN 'complete' ELSE excluded.cost_status END,
  is_cost_complete                 = CASE WHEN f_linegift_finance_sku_daily_v1.cost_status = 'complete' THEN 1 ELSE excluded.is_cost_complete END,
  -- cogs / margin は「有効 snapshot × 新 units_net_sold」で再計算
  cogs_amount_jpy_incl             = ROUND(COALESCE(CASE WHEN f_linegift_finance_sku_daily_v1.cost_status = 'complete' THEN f_linegift_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END, 0) * excluded.units_net_sold, 2),
  variable_margin_jpy_incl         = ROUND(
    excluded.gross_sales_jpy_incl
    - COALESCE(CASE WHEN f_linegift_finance_sku_daily_v1.cost_status = 'complete' THEN f_linegift_finance_sku_daily_v1.unit_cost_snapshot_incl ELSE excluded.unit_cost_snapshot_incl END, 0) * excluded.units_net_sold
    - excluded.mall_fee_jpy_incl
    - excluded.shipping_cost_jpy_incl, 2),
  -- LINEギフト 特有 audit (UPSERT で更新)
  recognized_on_jst                = excluded.recognized_on_jst,
  bought_date_jst                  = excluded.bought_date_jst,
  delivered_lag_days               = excluded.delivered_lag_days,
  received_lag_days                = excluded.received_lag_days,
  is_delivery_by_hand              = excluded.is_delivery_by_hand,
  delivery_agent                   = excluded.delivery_agent,
  first_seen_in_api_at             = excluded.first_seen_in_api_at,
  last_seen_in_api_at              = excluded.last_seen_in_api_at,
  is_frozen_after_horizon          = excluded.is_frozen_after_horizon,
  data_quality_score               = excluded.data_quality_score,
  order_count                      = excluded.order_count,
  line_count                       = excluded.line_count,
  source_layer_summary             = excluded.source_layer_summary,
  source_row_count                 = excluded.source_row_count,
  built_at                         = excluded.built_at;

-- ─── Step 3.6: key 集合ベース orphan delete (設計書 §11/§12) ───
-- raw freeze は Step 0 で実行済 (Codex R1 #3 反映で順序変更)、ここでは fact の orphan delete のみ
-- 「raw でまだ生きてる (date_jst, sku_code) キー集合」に存在しない fact 行を delete:
--   - status='received' で、かつ
--   - (a) 直近14日に観測されてる (= API 可視内、rebuild 対象)、または
--   - (b) frozen 済 (永久保持) — Step 3.5 で今回 frozen 化された行も含む
--
-- これにより:
--   - status flip (received → cancel/payment) → raw.status 条件で hit せず → fact 行 delete (正しい挙動)
--   - frozen 行 (古い) → (b) で keep
--   - 今回 frozen 化された行 → Step 3.5 で先に frozen=1、(b) で keep
--
-- 月絞りはしない (raw 全体の key 集合と比較) — 月跨ぎの status flip も拾う
DELETE FROM f_linegift_finance_sku_daily_v1
WHERE substr(date_jst, 1, 7) = printf('%04d-%02d', :year_month_int / 100, :year_month_int % 100)
  AND NOT EXISTS (
    SELECT 1 FROM raw_linegift_orders r
    WHERE r.received_date_jst = f_linegift_finance_sku_daily_v1.date_jst
      AND LOWER(TRIM(r.sku_code)) = f_linegift_finance_sku_daily_v1.sku_code
      AND r.status = 'received'
      AND r.received_date_jst IS NOT NULL
      AND (
        substr(r.last_seen_at, 1, 10) >= strftime('%Y-%m-%d', date('now', '+9 hours', '-14 days'))
        OR r.is_frozen_after_horizon = 1
      )
  );

-- ─── Step 4: cleanup ───
DROP TABLE IF EXISTS _silver_linegift_received_v1;
