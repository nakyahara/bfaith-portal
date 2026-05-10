-- f_rakuten_finance_sku_daily_v1 DDL
-- ticket: Phase 1a #R-1 + Phase 1b (楽天売上分析、AI_reference: 楽天Phase1a設計書_v0.4_20260509.md)
-- contract: docs/contracts/raw_rakuten_orders.contract.md (v1.2 Phase 1b)
-- build SQL: sql/rakuten/build_f_rakuten_finance_sku_daily_v1.sql
--
-- 売上計上方針 (D 案、Codex セカンドオピニオン 2026-05-09 確定):
--   - units_ordered = status IN (500,600,700) 件数 (受注ベース、発送前/発送後/お届け完了)
--   - units_cancelled = Phase 1b 按分後の採用値 (= allocated_units_cancelled)
--   - units_net_sold = MAX(0, units_ordered - units_cancelled)
--   - 楽天 status=700 が 2 ヶ月遅延のため、当月/先月のダッシュボード可視化を優先
--
-- Phase 1b (2026-05-10、Codex 事前レビュー反映): cogs 補正のため月次按分実装
--   - refund_monthly = rakuten_code 単位で月集計
--   - sales_by_code_month = rakuten_code の月内 sales 合計 (按分分母)
--   - allocated_units_cancelled = Largest Remainder Method (床関数 + 残差を上位に +1 配賦)
--   - units_cancelled_same_day_matched = Phase 1a 旧 LEFT JOIN 結果 (audit 用)
--   - 月次フルリビルド前提 (build script で対象月 + 前 2 ヶ月を毎回再計算)
--   - 月次合計が SUM(allocated_units_cancelled) = SUM(fact_returns 数量) で完全一致保証
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
  -- units_cancelled は Phase 1b で「按分後の採用値」(= allocated_units_cancelled) に意味変更
  -- 旧仕様 (Phase 1a 同日マッチのみ) の値は audit 用に units_cancelled_same_day_matched に保存
  units_ordered                     INTEGER NOT NULL DEFAULT 0,
  units_cancelled                   INTEGER NOT NULL DEFAULT 0,
  units_net_sold                    INTEGER NOT NULL DEFAULT 0,

  -- Phase 1b 按分関連 (Codex 事前レビュー 2026-05-10 反映)
  -- allocated_units_cancelled: refund_monthly × (units_ordered / month_total_ordered) を Largest Remainder Method で配賦
  --   units_cancelled と同じ値だが、明示的に「按分由来」と分かるように別列で保存
  -- units_cancelled_same_day_matched: Phase 1a 旧仕様 (同日 LEFT JOIN) の結果、audit 用
  -- allocation_method: 'monthly_proportion' (按分採用) / 'no_refund' (refund_monthly に該当 SKU なし)
  -- cancel_exceeds_ordered_warning: month_total_cancelled > month_total_ordered の異常検知 (1=要調査)
  allocated_units_cancelled         INTEGER NOT NULL DEFAULT 0,
  units_cancelled_same_day_matched  INTEGER NOT NULL DEFAULT 0,
  allocation_method                 TEXT NOT NULL DEFAULT 'no_refund' CHECK (
    allocation_method IN ('monthly_proportion', 'no_refund')
  ),
  cancel_exceeds_ordered_warning    INTEGER NOT NULL DEFAULT 0,

  -- 売上 (税込)
  sales_principal_jpy_incl          REAL NOT NULL DEFAULT 0,
  sales_postage_jpy_incl            REAL NOT NULL DEFAULT 0,

  -- クーポン (税込、SKU 別按分済 fact_promotion_cost 由来)
  coupon_shop_jpy_incl              REAL NOT NULL DEFAULT 0,
  coupon_all_jpy_incl               REAL NOT NULL DEFAULT 0,
  promotion_jpy_incl                REAL NOT NULL DEFAULT 0,

  -- 返金 (税込)
  -- refund_amount_jpy_incl は Phase 1b で「按分後の採用値」(= allocated_refund_amount) に意味変更
  -- 旧仕様 (同日マッチ) は refund_amount_same_day_matched_jpy_incl に保存
  refund_amount_jpy_incl            REAL NOT NULL DEFAULT 0,
  allocated_refund_amount_jpy_incl  REAL NOT NULL DEFAULT 0,
  refund_amount_same_day_matched_jpy_incl REAL NOT NULL DEFAULT 0,

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
