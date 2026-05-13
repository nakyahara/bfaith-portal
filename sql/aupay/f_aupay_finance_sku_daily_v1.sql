-- f_aupay_finance_sku_daily_v1 DDL
-- ticket: au PAY マーケット Phase 1 A-1
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md
-- build SQL: sql/aupay/build_f_aupay_finance_sku_daily_v1.sql
--
-- 売上計上方針 (v0.4 確定、A-0 verify 済):
--   - whitelist: orderStatus = '完了' (注文) + itemCancelStatus = 'N' (明細) (96.1% カバー)
--   - units_ordered = SUM(unit) per (date_jst, aupay_sku_key)
--   - units_cancelled = 0 (Phase 1a で fact_returns 取れない、Phase B/C で本採用)
--   - units_net_sold = units_ordered (Phase 1a)
--
-- partial / full margin 2 段階設計 (Yahoo Phase 1 同型):
--   - Phase A: partial margin (5 要素) を確定計上
--     gross_sales (= sales_principal、全送料無料で postage=0) - 原価 - モール手数料 - 送料 - クーポン
--     ※ ポイント (giftPoint / usePoint / useAuPointPrice) は partial に入れず別列保持 → Phase B で確定
--     ※ au PAY は決済手数料コミコミプラン → full margin の要素数が楽天/Yahoo の 10 要素とは違う
--   - Phase B: full margin backfill
--     partial - 成約手数料実額補正 - 広告費 - ストアポイント原資実額 - 梱包材 - 同梱物 - 返品 + 補填
--
-- SKU 解決 2 段階 fallback (v0.4 確定、CI 最初から = Yahoo Phase 1.1 の教訓):
--   1. m_products WHERE LOWER(TRIM(商品コード)) = LOWER(TRIM(item_code))  (= master_match、90.7%)
--   2. f_aupay_sku_map WHERE LOWER(TRIM(aupay_key)) = LOWER(TRIM(item_code)) AND store_id  (= manual_map)
--   3. NULL → unresolved_sku_flag = 1 (variant ありそうな親 SKU はここに残す、粒度を勝手に潰さない)
--
-- 不変条件 (楽天 Phase 1a/1b / Yahoo Phase 1 から継承):
--   - PK: (date_jst, aupay_sku_key)
--   - UPSERT で snapshot 列 (unit_cost_snapshot_incl, cost_snapshot_date_jst, is_cost_complete, cost_status):
--     既存 non-NULL なら温存、初回 NULL なら今わかった原価を late-bind (m_products 後入れ / 手動マップ追加 / 子から親導出)
--   - cogs / margin は「有効 snapshot (温存値 or late-bind 値) × 新 units_net_sold」で再計算

CREATE TABLE IF NOT EXISTS f_aupay_finance_sku_daily_v1 (
  -- PK
  date_jst        TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
  aupay_sku_key   TEXT NOT NULL CHECK(trim(aupay_sku_key) <> ''),  -- = item_code (variant 粒度保持)

  -- 紐付け
  ne_code              TEXT,
  variant_key          TEXT NOT NULL DEFAULT '',  -- = item_option (option_info、variant 情報含む)
  resolution_method    TEXT NOT NULL CHECK (
    resolution_method IN ('master_match', 'manual_map', 'unresolved')
  ),
  unresolved_sku_flag  INTEGER NOT NULL DEFAULT 0,
  product_name         TEXT NOT NULL DEFAULT '',

  -- 数量 (Phase 1a では cancelled = 0 固定)
  units_ordered    INTEGER NOT NULL DEFAULT 0,
  units_cancelled  INTEGER NOT NULL DEFAULT 0,
  units_net_sold   INTEGER NOT NULL DEFAULT 0,

  -- 売上 (税込、A-0 verify 済: itemPrice = beforeDiscount - discount = 値引後単価)
  sales_principal_jpy_incl         REAL NOT NULL DEFAULT 0,  -- = SUM(item_price × unit)
  postage_allocated_jpy_incl       REAL NOT NULL DEFAULT 0,  -- = postagePrice の明細 LRM 按分 (全送料無料なら 0)
  gross_sales_jpy_incl             REAL NOT NULL DEFAULT 0,  -- = sales_principal + postage_allocated
  net_sales_after_coupon_jpy_incl  REAL NOT NULL DEFAULT 0,  -- = gross_sales - coupon_shop
  request_price_jpy_incl           REAL NOT NULL DEFAULT 0,  -- = SUM(requestPrice 按分) 検算用 (= totalPrice - coupon - usePoint - useAuPointPrice)

  -- クーポン (税込、注文単位 → 明細 LRM 按分。detail.discount とは別物 = 二重カウントなし、A-0 verify 済)
  coupon_shop_jpy_incl  REAL NOT NULL DEFAULT 0,  -- = couponTotalPrice の LRM 按分

  -- ポイント (Phase A では partial margin に入れない、Phase B で確定)
  gift_point_jpy_incl            REAL NOT NULL DEFAULT 0,  -- 付与ポイント (商品単位、giftPoint、率 ≈ 0.93%)
  use_ponta_point_jpy_incl       REAL NOT NULL DEFAULT 0,  -- Ponta 利用 (usePoint、顧客支払手段)
  use_au_point_jpy_incl          REAL NOT NULL DEFAULT 0,  -- au 利用 (useAuPointPrice、顧客支払手段)
  premium_member_point_jpy_incl  REAL NOT NULL DEFAULT 0,  -- プレミアム会員ポイント (premiumShopPrice、このストアでは 0)
  point_cost_pending_jpy_incl    REAL NOT NULL DEFAULT 0,  -- = gift_point + premium_member_point (Phase B でストアポイント原資と突合 → full margin へ)

  -- 税率別売上 (au PAY 特有、totalSalePriceNormalTax 等の注文値を明細按分)
  tax_normal_sales_jpy_incl   REAL NOT NULL DEFAULT 0,
  tax_reduced_sales_jpy_incl  REAL NOT NULL DEFAULT 0,
  tax_free_sales_jpy_incl     REAL NOT NULL DEFAULT 0,

  -- モール手数料 (税込) + 推計監視
  -- 成約手数料率は設定 (config/aupay_mall_fee_rates.json)、未確定なら NULL → calc_method='unknown' + data_quality_score 低下
  mall_fee_jpy_incl           REAL,
  mall_fee_rate_applied       REAL,  -- 適用した成約手数料率 (設定由来、NULL なら未確定)
  mall_fee_calc_method        TEXT NOT NULL DEFAULT 'unknown' CHECK (
    mall_fee_calc_method IN ('estimated_rate', 'actual_statement', 'unknown')
  ),
  mall_fee_estimate_delta_jpy REAL,  -- Phase B で actual との差分、月次 alert

  -- 送料コスト (お店負担、税込、m_products.送料 推定)
  shipping_cost_jpy_incl  REAL NOT NULL DEFAULT 0,
  shipping_quality        TEXT NOT NULL CHECK (
    shipping_quality IN ('actual', 'estimated_rates', 'estimated_fallback', 'missing')
  ),

  -- 原価 snapshot (初回 build 時固定、UPSERT で温存)
  unit_cost_snapshot_incl         REAL,
  cost_snapshot_date_jst          TEXT,
  latest_unit_cost_reference_incl REAL,  -- ⚠️ snapshot ではない: UPSERT で毎回 m_products 最新で上書き
  cogs_amount_jpy_incl            REAL NOT NULL DEFAULT 0,

  -- 利益 (partial / full 2 段階)
  variable_margin_partial_jpy_incl    REAL NOT NULL DEFAULT 0,  -- ★ Phase A 確定 = gross_sales - cogs - mall_fee - shipping - coupon (5 要素)
  variable_margin_full_jpy_incl       REAL,                     -- ★ Phase B で backfill
  refund_adjusted_net_sales_jpy_incl  REAL,                     -- Phase B で backfill
  margin_confidence  TEXT NOT NULL DEFAULT 'partial' CHECK (
    margin_confidence IN ('partial', 'full')
  ),
  margin_full_finalized_at  TEXT,

  -- audit 列 (raw 由来、計算未使用、validation/reconcile 用)
  before_discount_jpy_incl    REAL NOT NULL DEFAULT 0,  -- = SUM(before_discount × unit) 値引前売上
  detail_discount_jpy_incl    REAL NOT NULL DEFAULT 0,  -- = SUM(discount × unit) 商品個別値引 (item_price に既反映、クーポン重複検知用)
  charge_allocated_jpy_incl   REAL NOT NULL DEFAULT 0,  -- 代引手数料等 (chargePrice 按分、通常 0)
  item_option_jpy_incl        REAL NOT NULL DEFAULT 0,  -- オプション料 (totalItemOptionPrice 按分)
  gift_wrapping_jpy_incl      REAL NOT NULL DEFAULT 0,  -- ギフト包装料 (totalGiftWrappingPrice 按分)

  -- 品質 / 監視
  cost_status                  TEXT NOT NULL CHECK (
    cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
  ),
  is_cost_complete             INTEGER NOT NULL DEFAULT 0,
  data_quality_score           INTEGER NOT NULL DEFAULT 0
                               CHECK (data_quality_score BETWEEN 0 AND 100),
  price_variance_warning       INTEGER NOT NULL DEFAULT 0,

  -- メタ
  order_count           INTEGER NOT NULL DEFAULT 0,  -- この (date, sku) の注文数
  line_count            INTEGER NOT NULL DEFAULT 0,  -- 明細行数 (= source_row_count)
  source_layer_summary  TEXT NOT NULL DEFAULT '',
  source_row_count      INTEGER NOT NULL DEFAULT 0,
  built_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (date_jst, aupay_sku_key)
);

CREATE INDEX IF NOT EXISTS idx_f_aupay_sku_v1_ne     ON f_aupay_finance_sku_daily_v1 (ne_code);
CREATE INDEX IF NOT EXISTS idx_f_aupay_sku_v1_month  ON f_aupay_finance_sku_daily_v1 (substr(date_jst, 1, 7));
CREATE INDEX IF NOT EXISTS idx_f_aupay_sku_v1_status ON f_aupay_finance_sku_daily_v1 (cost_status);
CREATE INDEX IF NOT EXISTS idx_f_aupay_sku_v1_unres  ON f_aupay_finance_sku_daily_v1 (unresolved_sku_flag) WHERE unresolved_sku_flag = 1;
CREATE INDEX IF NOT EXISTS idx_f_aupay_sku_v1_warn   ON f_aupay_finance_sku_daily_v1 (price_variance_warning) WHERE price_variance_warning = 1;

-- ─── デバイス別 / 決済手段別 fact (案 A: 主 fact は SKU×日次、これらは units/sales のみの別軸 mart) ───
CREATE TABLE IF NOT EXISTS f_aupay_sku_device_daily (
  date_jst       TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
  aupay_sku_key  TEXT NOT NULL,
  device         TEXT NOT NULL,  -- siteAndDevice (例 "au PAY マーケット(SP(アプリ))" / "SP(Web)" / "PC")
  ne_code        TEXT,
  product_name   TEXT NOT NULL DEFAULT '',
  units_net_sold INTEGER NOT NULL DEFAULT 0,
  sales_principal_jpy_incl REAL NOT NULL DEFAULT 0,
  order_count    INTEGER NOT NULL DEFAULT 0,
  built_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date_jst, aupay_sku_key, device)
);
CREATE INDEX IF NOT EXISTS idx_f_aupay_device_month ON f_aupay_sku_device_daily (substr(date_jst, 1, 7));
CREATE INDEX IF NOT EXISTS idx_f_aupay_device_dev   ON f_aupay_sku_device_daily (device);

CREATE TABLE IF NOT EXISTS f_aupay_sku_settlement_daily (
  date_jst        TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
  aupay_sku_key   TEXT NOT NULL,
  settlement_name TEXT NOT NULL,  -- settlementName (例 "au PAY（auかんたん決済）")
  ne_code         TEXT,
  product_name    TEXT NOT NULL DEFAULT '',
  units_net_sold  INTEGER NOT NULL DEFAULT 0,
  sales_principal_jpy_incl REAL NOT NULL DEFAULT 0,
  order_count     INTEGER NOT NULL DEFAULT 0,
  built_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date_jst, aupay_sku_key, settlement_name)
);
CREATE INDEX IF NOT EXISTS idx_f_aupay_settle_month ON f_aupay_sku_settlement_daily (substr(date_jst, 1, 7));

-- ─── reporting view (NULL 集計事故防止、Yahoo Phase 1 同型) ───
DROP VIEW IF EXISTS vw_aupay_finance_for_reporting;
CREATE VIEW vw_aupay_finance_for_reporting AS
SELECT
  *,
  COALESCE(variable_margin_full_jpy_incl, variable_margin_partial_jpy_incl) AS margin_value_for_reporting,
  CASE
    WHEN variable_margin_full_jpy_incl IS NOT NULL THEN 'full_finalized'
    WHEN margin_confidence = 'partial' THEN 'partial_only'
    ELSE 'unknown'
  END AS margin_value_source
FROM f_aupay_finance_sku_daily_v1;

-- ─── provisional view (whitelist 外、監視用、主 fact に混ぜない) ───
DROP VIEW IF EXISTS vw_aupay_orders_provisional;
CREATE VIEW vw_aupay_orders_provisional AS
SELECT * FROM raw_aupay_orders
WHERE NOT (order_status = '完了' AND item_cancel_status = 'N')
  AND order_date >= strftime('%Y/%m/%d', date('now', '-90 days'));
