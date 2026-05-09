-- f_rakuten_finance_sku_daily_v1 DDL
-- ticket: Phase 1a #R-1 (楽天売上分析、AI_reference: 楽天Phase1a設計書_v0.4_20260509.md)
-- contract: docs/contracts/raw_rakuten_orders.contract.md (v1.1)
-- build SQL: sql/rakuten/build_f_rakuten_finance_sku_daily_v1.sql
--
-- 売上計上方針 (D 案、Codex セカンドオピニオン 2026-05-09 確定):
--   - units_ordered = status IN (500,600,700) 件数 (受注ベース、発送前/発送後/お届け完了)
--   - units_cancelled = fact_returns 楽天分の数量 (注文日 == silver date_jst の (date,code) ペア)
--   - units_net_sold = MAX(0, units_ordered - units_cancelled)
--   - 楽天 status=700 が 2 ヶ月遅延のため、当月/先月のダッシュボード可視化を優先
--
-- Phase 1a 既知の限界: refund 注文日 と silver date_jst が日付不一致な (date, code) ペア
-- は LEFT JOIN で漏れる (4 月実測 88 units、cancel 全体の 36%)。Phase 1b で按分案対応予定
-- (詳細: g:/共有ドライブ/AI_reference/システム設計/楽天Phase1b案_C-lite検討メモ_20260509.md)
--
-- 不変条件 (Codex Round 11-13 で Amazon Phase 1 #1-1 から継承):
--   - PK: (date_jst, rakuten_code)
--   - UPSERT で snapshot 列 (unit_cost_snapshot_incl / cost_snapshot_date_jst /
--     is_cost_complete / cost_status) は既存値温存、非 snapshot 列のみ更新
--   - cogs / profit は「既存 snapshot 原価 × 新 units」で UPSERT 時に再計算

CREATE TABLE IF NOT EXISTS f_rakuten_finance_sku_daily_v1 (
  -- PK
  date_jst                          TEXT NOT NULL CHECK(date_jst GLOB '????-??-??'),
  rakuten_code                      TEXT NOT NULL CHECK(trim(rakuten_code) <> ''),

  -- 紐付け
  ne_code                           TEXT,
  sku_resolution                    TEXT NOT NULL CHECK (
    sku_resolution IN ('resolved', 'unresolved', 'direct_master')
  ),
  product_name                      TEXT NOT NULL DEFAULT '',

  -- 数量 (整数前提)
  units_ordered                     INTEGER NOT NULL DEFAULT 0,
  units_cancelled                   INTEGER NOT NULL DEFAULT 0,
  units_net_sold                    INTEGER NOT NULL DEFAULT 0,

  -- 売上 (税込)
  sales_principal_jpy_incl          REAL NOT NULL DEFAULT 0,
  sales_postage_jpy_incl            REAL NOT NULL DEFAULT 0,

  -- クーポン (税込、SKU 別按分済 fact_promotion_cost 由来)
  coupon_shop_jpy_incl              REAL NOT NULL DEFAULT 0,
  coupon_all_jpy_incl               REAL NOT NULL DEFAULT 0,
  promotion_jpy_incl                REAL NOT NULL DEFAULT 0,

  -- 返金 (税込、注文日基準で fact_returns 楽天分から JOIN)
  refund_amount_jpy_incl            REAL NOT NULL DEFAULT 0,

  -- モール手数料 (税込、楽天 10%)
  -- 業務前提: 楽天手数料の課金ベース = (sales_principal + sales_postage - coupon_all)
  -- coupon_shop は楽天負担じゃないので控除しない
  mall_fee_jpy_incl                 REAL NOT NULL DEFAULT 0,

  -- 送料実コスト (税込、自社負担)
  shipping_cost_jpy_incl            REAL NOT NULL DEFAULT 0,
  shipping_quality                  TEXT NOT NULL CHECK (
    shipping_quality IN ('actual', 'estimated_rates', 'estimated_fallback', 'missing')
  ),

  -- 原価 (snapshot 方式、税込)
  -- snapshot の意味:「初回 build 時点で固定」(過去月 backfill が遅れた場合は build 時の原価で固定)
  unit_cost_snapshot_incl           REAL,
  cost_snapshot_date_jst            TEXT,
  -- ⚠️ snapshot ではない参照値: UPSERT で毎回 m_products の最新原価で上書きされる
  -- 用途: 「snapshot と最新原価がどれだけ乖離しているか」の比較・監視
  latest_unit_cost_reference_incl   REAL,
  cogs_amount_jpy_incl              REAL NOT NULL DEFAULT 0,

  -- 利益 5 階層 (税込、粗利定義書 v1)
  gross_sales_jpy_incl              REAL NOT NULL DEFAULT 0,
  net_sales_jpy_incl                REAL NOT NULL DEFAULT 0,
  variable_margin_jpy_incl          REAL NOT NULL DEFAULT 0,
  refund_adjusted_net_sales_jpy_incl REAL NOT NULL DEFAULT 0,

  -- 品質 (snapshot 列扱い、UPSERT で温存、Codex Round 12 #medium で確定済)
  cost_status                       TEXT NOT NULL CHECK (
    cost_status IN ('complete', 'missing_cost', 'partial_cost', 'late_bound_after_close')
  ),
  is_cost_complete                  INTEGER NOT NULL DEFAULT 0,
  data_quality_score                INTEGER NOT NULL DEFAULT 0
                                    CHECK (data_quality_score BETWEEN 0 AND 100),
  price_variance_warning            INTEGER NOT NULL DEFAULT 0,

  -- メタ
  source_layer_summary              TEXT NOT NULL DEFAULT '',
  source_row_count                  INTEGER NOT NULL DEFAULT 0,
  built_at                          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (date_jst, rakuten_code)
);

CREATE INDEX IF NOT EXISTS idx_f_rakuten_sku_v1_ne
  ON f_rakuten_finance_sku_daily_v1 (ne_code);
CREATE INDEX IF NOT EXISTS idx_f_rakuten_sku_v1_month
  ON f_rakuten_finance_sku_daily_v1 (substr(date_jst, 1, 7));
CREATE INDEX IF NOT EXISTS idx_f_rakuten_sku_v1_status
  ON f_rakuten_finance_sku_daily_v1 (cost_status);
CREATE INDEX IF NOT EXISTS idx_f_rakuten_sku_v1_warn
  ON f_rakuten_finance_sku_daily_v1 (price_variance_warning) WHERE price_variance_warning = 1;
