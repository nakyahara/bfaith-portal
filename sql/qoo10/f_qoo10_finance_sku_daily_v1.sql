-- f_qoo10_finance_sku_daily_v1 DDL
-- ticket: Qoo10 Phase 1 A-2
-- 設計書: g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §6-2
-- build SQL: sql/qoo10/build_f_qoo10_finance_sku_daily_v1.sql
--
-- 売上計上方針 (v0.11 確定、A-0 verify 済):
--   - whitelist: shipping_status = 'Delivered(5)' AND legacy_fields_missing = 0
--     (旧 raw 由来は除外、新 raw 以降 api_v2 のみ fact 対象)
--   - recognized_on = substr(order_date, 1, 10) (注文日基準)
--   - 按分なし (1 orderNo 1 明細の grain そのまま、SUM)
--
-- partial → full margin の 4 段階 confidence (Codex R7 設計書反映):
--   - 'partial_pending_settlement_csv' (Phase A): variable_margin = net_settlement_api - cogs - shipping
--     ※ SettlePrice 公式 = round(orderPrice × 0.9) × qty で API 自動的に 10% 手数料引かれ済 → platform_fee は別途引かない
--     ※ Qoo10 ショップ負担 (メガ割 10% + メガポ 10%) は別途精算 CSV で控除必要、Phase A 未反映
--   - 'partial_minus_returns' (Phase B 着手後): cancel 注文の refund 確定前
--   - 'full' (Phase B 完了): 精算 CSV 統合 + refund 確定 = margin 確定
--   - 'partial_legacy_fields_missing': 旧 raw 由来 (実質 0 件、fact 対象外 = この値は理論上発生しない)
--
-- SKU 解決 3 段 fallback (2026-05-19 A-2 後 patch、データ verify 後変更):
--   ★ Qoo10 variation 商品の実 SKU = seller_item_code + option_code (例 ayuveta + -pi = ayuveta-pi)
--   旧 v0.11 設計 (seller_item_code 単独) は variation 商品で unresolved 12% を生む不整合判明、
--   3 段 fallback に改修して unresolved ~0% を達成。match_tier 列で tier 分布を DQ 監視。
--   1. tier='combined'    : LOWER(TRIM(seller_item_code || option_code)) (variation 含む、第一優先)
--   2. tier='option_only' : LOWER(TRIM(option_code)) (option_code 側が完全 SKU 形式の例外、ana-001 等)
--   3. tier='seller_only' : LOWER(TRIM(seller_item_code)) (variation なし商品、旧ロジック互換)
--   4. tier='unresolved'  : 全 tier miss、'__UNRESOLVED__:' || combined_key で deterministic key 化
--     - NE 在庫連携前提で原則 ~0% 想定、>0 で NE 在庫連携設定要確認 alert
--     - frozen 過去 unresolved は受容 (R10 中原さん判断、Phase 1 で修復しない)
--   resolution_method は 'master_match' (= tier 1-3 いずれか hit) / 'unresolved' の 2 値のまま (後方互換)
--
-- Qoo10 特有 (vs LINEギフト):
--   - メガ割 (年4回×13日、20% off): discount > 0 AND discount/gross ≈ 0.20 を行レベル判別
--   - メガポ (不定期、10% off): discount > 0 AND discount/gross ≈ 0.10 を行レベル判別
--   - SettlePrice 公式 = round(orderPrice × 0.9) × qty (domestic_non_cod scope のみ成立)
--   - settle_price_formula_scope: domestic_non_cod / oversea / cod / mixed の行レベル判別
--   - mall_fee = SUM(gmv_list - settle_price) (LINEギフトは API fee 列、Qoo10 は導出)
--   - shop_promo_burden (Phase A 0): 精算 CSV で確定する想定、API 実額 = 0
--   - frozen horizon: order_date < today-76d (LINEギフトは last_seen_at < today-14d、 raw 側で freeze 済)
--
-- 不変条件 (au PAY/LINEギフト 同型):
--   - PK: (date_jst, sku_code)
--   - cost_status='complete' 行のみ snapshot 凍結 (取引時点原価)
--   - missing_cost / partial_cost は replaceable
--   - cogs / margin は「有効 snapshot × 新 units_net_sold」で再計算

CREATE TABLE IF NOT EXISTS f_qoo10_finance_sku_daily_v1 (
  -- PK
  date_jst         TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),  -- = substr(order_date, 1, 10)
  sku_code         TEXT NOT NULL CHECK(trim(sku_code) <> ''),        -- = master_match 後の解決値 or '__UNRESOLVED__:<sic>'

  -- 紐付け
  ne_code             TEXT,
  qoo10_item_id       INTEGER,                          -- = itemCode (Qoo10 内部 ID、audit)
  parent_item_code    TEXT NOT NULL DEFAULT '',         -- variant fallback (v0.8 で parent_match 削除済、audit 用途)
  variant_key         TEXT NOT NULL DEFAULT '',         -- = option_info
  resolution_method   TEXT NOT NULL CHECK (
    resolution_method IN ('master_match', 'unresolved')
  ),
  -- 2026-05-19 A-2 patch: 3 段 fallback (combined / option_only / seller_only / unresolved)
  -- resolution_method は後方互換のため 2 値のまま、tier 内訳は match_tier で監視
  match_tier          TEXT NOT NULL DEFAULT 'unresolved' CHECK (
    match_tier IN ('combined', 'option_only', 'seller_only', 'unresolved')
  ),
  unresolved_sku_flag INTEGER NOT NULL DEFAULT 0,
  product_name        TEXT NOT NULL DEFAULT '',

  -- 数量
  units_ordered    INTEGER NOT NULL DEFAULT 0,
  units_cancelled  INTEGER NOT NULL DEFAULT 0,
  units_net_sold   INTEGER NOT NULL DEFAULT 0,

  -- 売上 (税込、Codex R1 #4 命名修正: gross_sales 削除、net_settlement_api 一本化)
  gmv_list_price_jpy_incl      REAL NOT NULL DEFAULT 0,  -- = SUM(order_price × order_qty)
  customer_paid_jpy_incl       REAL NOT NULL DEFAULT 0,  -- = SUM(total)
  net_settlement_api_jpy_incl  REAL NOT NULL DEFAULT 0,  -- = SUM(settle_price)、ショップ受取

  -- 販売手数料 (API 実額、Codex R1 #6 反映)
  platform_fee_jpy_incl        REAL NOT NULL DEFAULT 0,  -- = SUM(gmv_list - settle_price)
  mall_fee_calc_method         TEXT NOT NULL DEFAULT 'actual_api' CHECK (
    mall_fee_calc_method IN ('actual_api', 'actual_statement', 'estimated_rate', 'unknown')
  ),
  settle_price_formula_scope   TEXT NOT NULL DEFAULT 'domestic_non_cod' CHECK (
    settle_price_formula_scope IN ('domestic_non_cod', 'oversea', 'cod', 'mixed')
  ),

  -- 海外/COD 追加手数料 (Phase A 0、将来 API で実額)
  extra_fee_oversea_jpy_incl   REAL NOT NULL DEFAULT 0,
  cod_fee_jpy_incl             REAL NOT NULL DEFAULT 0,

  -- メガ割/メガポ 関連 (Codex R1 #5 反映: 行レベル判別→集計時別列保持)
  megawari_order_count             INTEGER NOT NULL DEFAULT 0,  -- discount率 ≈ 0.20 の orderNo 件数
  megawari_discount_amount_jpy_incl REAL NOT NULL DEFAULT 0,    -- SUM(discount where promo_type='megawari')
  megapo_order_count               INTEGER NOT NULL DEFAULT 0,  -- discount率 ≈ 0.10 の orderNo 件数
  megapo_discount_amount_jpy_incl  REAL NOT NULL DEFAULT 0,
  other_promo_order_count          INTEGER NOT NULL DEFAULT 0,  -- discount > 0 だが ≈ 0.20/0.10 以外
  other_promo_discount_jpy_incl    REAL NOT NULL DEFAULT 0,
  total_platform_promo_jpy_incl    REAL NOT NULL DEFAULT 0,     -- = SUM(discount) 全部、Qoo10 負担分
  qoo10_cart_discount_jpy_incl     REAL NOT NULL DEFAULT 0,     -- = SUM(cart_discount_qoo10)、P/L 非算入、分析用
  seller_discount_api_jpy_incl     REAL NOT NULL DEFAULT 0,     -- = SUM(seller_discount + cart_discount_seller)、現状 0

  -- ショップ負担分 (Phase A 0、Phase B-2 で精算 CSV 統合)
  shop_promo_burden_jpy_incl       REAL NOT NULL DEFAULT 0,
  shop_promo_burden_status         TEXT NOT NULL DEFAULT 'pending_settlement_csv' CHECK (
    shop_promo_burden_status IN ('pending_settlement_csv', 'actual_statement', 'not_applicable')
  ),

  -- domestic_non_cod 別集計 (Codex R3 High #1 反映、'mixed' でも domestic 部分を DQ で検証可能化)
  domestic_non_cod_line_count           INTEGER NOT NULL DEFAULT 0,  -- silver 行レベル scope='domestic_non_cod' 件数
  domestic_non_cod_formula_match_count  INTEGER NOT NULL DEFAULT 0,  -- そのうち SettlePrice = round(orderPrice × 0.9) × qty が成立する件数

  -- 送料コスト (Phase A、Qoo10 は raw.shipping_rate、現状 0、quality='no_shipping_in_api')
  shipping_cost_jpy_incl  REAL NOT NULL DEFAULT 0,
  shipping_quality        TEXT NOT NULL CHECK (
    shipping_quality IN ('no_shipping_in_api', 'actual_api', 'estimated_rates', 'estimated_fallback', 'missing')
  ),

  -- 原価 snapshot (au PAY/LINEギフト 同型 不変条件)
  unit_cost_snapshot_incl         REAL,
  cost_snapshot_date_jst          TEXT,
  latest_unit_cost_reference_incl REAL,  -- snapshot ではない: UPSERT で毎回 m_products 最新で上書き
  cogs_amount_jpy_incl            REAL NOT NULL DEFAULT 0,

  -- 利益
  variable_margin_jpy_incl       REAL NOT NULL DEFAULT 0,  -- ★ Phase A 確定 = net_settlement - cogs - shipping (ショップ負担 promo は Phase B で控除)
  variable_margin_full_jpy_incl  REAL,                     -- Phase B で backfill
  margin_confidence  TEXT NOT NULL DEFAULT 'partial_pending_settlement_csv' CHECK (
    margin_confidence IN ('partial_pending_settlement_csv', 'partial_minus_returns', 'full', 'partial_legacy_fields_missing')
  ),
  margin_full_finalized_at  TEXT,

  -- audit
  delivered_lag_days   INTEGER,
  shipping_lag_days    INTEGER,
  oversea_count        INTEGER NOT NULL DEFAULT 0,
  payment_methods_json TEXT,

  -- 90日境界凍結管理 (raw 側 freeze と同期、Step 0 で raw freeze 済の値の写像)
  first_seen_in_api_at     TEXT,
  last_seen_in_api_at      TEXT,
  is_frozen_after_horizon  INTEGER NOT NULL DEFAULT 0,

  -- 品質
  cost_status                  TEXT NOT NULL CHECK (
    cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
  ),
  is_cost_complete             INTEGER NOT NULL DEFAULT 0,
  data_quality_score           INTEGER NOT NULL DEFAULT 0
                               CHECK (data_quality_score BETWEEN 0 AND 100),

  -- メタ
  order_count           INTEGER NOT NULL DEFAULT 0,
  line_count            INTEGER NOT NULL DEFAULT 0,
  source_layer_summary  TEXT NOT NULL DEFAULT '',
  source_row_count      INTEGER NOT NULL DEFAULT 0,
  built_at              TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (date_jst, sku_code)
);

CREATE INDEX IF NOT EXISTS idx_f_qoo10_sku_v1_ne          ON f_qoo10_finance_sku_daily_v1 (ne_code);
CREATE INDEX IF NOT EXISTS idx_f_qoo10_sku_v1_month       ON f_qoo10_finance_sku_daily_v1 (substr(date_jst, 1, 7));
CREATE INDEX IF NOT EXISTS idx_f_qoo10_sku_v1_status      ON f_qoo10_finance_sku_daily_v1 (cost_status);
CREATE INDEX IF NOT EXISTS idx_f_qoo10_sku_v1_unres       ON f_qoo10_finance_sku_daily_v1 (unresolved_sku_flag) WHERE unresolved_sku_flag = 1;
CREATE INDEX IF NOT EXISTS idx_f_qoo10_sku_v1_frozen      ON f_qoo10_finance_sku_daily_v1 (is_frozen_after_horizon) WHERE is_frozen_after_horizon = 1;
CREATE INDEX IF NOT EXISTS idx_f_qoo10_sku_v1_scope       ON f_qoo10_finance_sku_daily_v1 (settle_price_formula_scope);

-- ─── reporting view (gross_sales_jpy_incl alias 提供、NULL 集計事故防止) ───
DROP VIEW IF EXISTS vw_qoo10_finance_for_reporting;
CREATE VIEW vw_qoo10_finance_for_reporting AS
SELECT
  *,
  -- gross_sales_jpy_incl は net_settlement_api_jpy_incl と同義 (alias 提供、Codex R1 #4 反映)
  net_settlement_api_jpy_incl AS gross_sales_jpy_incl,
  -- margin_value_for_reporting: full → full_minus_returns → partial_pending_settlement_csv の優先順位
  CASE
    WHEN margin_confidence = 'full' THEN variable_margin_full_jpy_incl
    WHEN margin_confidence = 'partial_minus_returns' THEN variable_margin_full_jpy_incl
    ELSE variable_margin_jpy_incl  -- partial_pending_settlement_csv は Phase A 4 要素
  END AS margin_value_for_reporting,
  margin_confidence AS margin_value_source
FROM f_qoo10_finance_sku_daily_v1;

-- ─── provisional view (whitelist 外、監視用、主 fact に混ぜない) ───
-- shipping_status != 'Delivered(5)' (Order(1)/Payment(2)/Seller confirm(3)/Shipping(4)) を表示
-- (legacy_fields_missing=1 は新 raw 移行で 0 件残る想定、fact 対象外)
DROP VIEW IF EXISTS vw_qoo10_orders_provisional;
CREATE VIEW vw_qoo10_orders_provisional AS
SELECT
  order_id, source_type, shipping_status, seller_item_code, item_code, item_title,
  order_price, order_qty, total, settle_price, discount,
  order_date, payment_date, shipping_date, delivered_date,
  first_seen_at, last_seen_at, is_frozen_after_horizon
FROM raw_qoo10_orders
WHERE shipping_status <> 'Delivered(5)'
  AND legacy_fields_missing = 0
  AND date(substr(order_date, 1, 10)) >= date('now', '+9 hours', '-90 days');

-- ─── horizon status view (frozen unresolved 可視化、Codex R11 Low #1 反映) ───
DROP VIEW IF EXISTS vw_qoo10_finance_horizon_status;
CREATE VIEW vw_qoo10_finance_horizon_status AS
SELECT
  substr(date_jst, 1, 7) AS month_ym,
  COUNT(*) AS total_rows,
  SUM(is_frozen_after_horizon) AS frozen_rows,
  SUM(CASE WHEN is_frozen_after_horizon = 0 THEN 1 ELSE 0 END) AS active_rows,
  -- frozen 受容済 unresolved 監視 (Phase 2 移植判断のための観測指標)
  SUM(CASE WHEN is_frozen_after_horizon = 1 AND sku_code LIKE '\_\_UNRESOLVED\_\_:%' ESCAPE '\' THEN 1 ELSE 0 END) AS frozen_unresolved_row_count,
  ROUND(SUM(CASE WHEN is_frozen_after_horizon = 1 AND sku_code LIKE '\_\_UNRESOLVED\_\_:%' ESCAPE '\' THEN gmv_list_price_jpy_incl ELSE 0 END), 2) AS frozen_unresolved_gmv_jpy,
  MIN(CASE WHEN is_frozen_after_horizon = 1 AND sku_code LIKE '\_\_UNRESOLVED\_\_:%' ESCAPE '\' THEN date_jst END) AS oldest_frozen_date,
  -- 比較用: frozen 正常解決済
  SUM(CASE WHEN is_frozen_after_horizon = 1 AND sku_code NOT LIKE '\_\_UNRESOLVED\_\_:%' ESCAPE '\' THEN 1 ELSE 0 END) AS frozen_resolved_row_count,
  ROUND(SUM(CASE WHEN is_frozen_after_horizon = 1 THEN variable_margin_jpy_incl ELSE 0 END), 2) AS frozen_margin_jpy,
  ROUND(SUM(CASE WHEN is_frozen_after_horizon = 0 THEN variable_margin_jpy_incl ELSE 0 END), 2) AS active_margin_jpy,
  MAX(last_seen_in_api_at) AS latest_api_observation
FROM f_qoo10_finance_sku_daily_v1
GROUP BY month_ym
ORDER BY month_ym DESC;
