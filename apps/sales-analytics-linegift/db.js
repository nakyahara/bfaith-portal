/**
 * apps/sales-analytics-linegift/db.js — v1.0 Render 完結 UI 用 DB アクセス層
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §7
 *
 * すべて warehouse-mirror.db への read アクセス。
 * 集計データ (mart_linegift_*) は別途 build-linegift-analytics-mart.js で生成済の前提。
 *
 * 入力テーブル / view (すべて warehouse-mirror.db):
 *   v_linegift_kpi_summary_latest
 *   v_linegift_sku_perf_latest_90d
 *   v_linegift_price_band_summary_latest_90d
 *   v_linegift_monthly_trend
 *   v_linegift_season_sku_perf
 *   v_linegift_new_item_ramp
 *   mart_gift_seasons + mart_gift_season_occurrences
 *   mart_build_locks (build 状態表示用)
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

const VALID_WINDOWS = new Set([7, 28, 90]);

function validateWindow(w) {
  const n = Number(w);
  if (!VALID_WINDOWS.has(n)) throw new Error(`invalid window: ${w} (must be 7/28/90)`);
  return n;
}

/**
 * トップ KPI 5枚 (期間別、materialized から)
 */
export function getKpiSummary(windowDays) {
  const w = validateWindow(windowDays);
  const db = getMirrorDB();
  return db.prepare(`
    SELECT total_sales_jpy_incl, total_orders, avg_order_value_jpy_incl,
           total_gross_profit_jpy_incl, gross_margin_rate, as_of_date_jst, synced_at
    FROM mart_linegift_kpi_summary_daily
    WHERE window_days = ?
      AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM mart_linegift_kpi_summary_daily WHERE window_days = ?)
  `).get(w, w) || null;
}

/**
 * 月別トレンド (12ヶ月、view 直)
 */
export function getMonthlyTrend() {
  const db = getMirrorDB();
  return db.prepare(`SELECT * FROM v_linegift_monthly_trend`).all();
}

/**
 * 商品ランキング (filters: window/season_code/season_year/sort/limit)
 * シーン指定時は mart_gift_season_occurrences + mirror_linegift_finance_sku_daily で動的集計、
 * 通常は mart_linegift_sku_perf_daily を参照。
 */
export function getSkuRanking(filters = {}) {
  const db = getMirrorDB();
  const limit = Math.max(1, Math.min(500, Number(filters.limit) || 100));
  const sort = ['sales', 'units', 'profit'].includes(filters.sort) ? filters.sort : 'sales';
  const orderCol = { sales: 'sales_amount_jpy_incl', units: 'units', profit: 'gross_profit_jpy_incl' }[sort];

  if (filters.season_code) {
    // シーン期間集計: window モード同様に mirror_products.送料 を引いた粗利を採用 (2026-05-27 中原さん指摘)
    return db.prepare(`
      SELECT
        f.ne_code, f.sku_code, MAX(f.product_name) AS product_name,
        SUM(f.units_net_sold) AS units,
        ROUND(SUM(f.gross_sales_jpy_incl)) AS sales_amount_jpy_incl,
        -- 送料引き後粗利 (Amazon FBM と同パターン、mirror_products.送料 × units を税込換算で引く)
        ROUND(SUM(f.variable_margin_jpy_incl) - SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1)))) AS gross_profit_jpy_incl,
        SUM(f.order_count) AS orders,
        CASE WHEN COALESCE(SUM(f.units_net_sold), 0) = 0 THEN 0
             ELSE ROUND(SUM(f.gross_sales_jpy_incl) * 1.0 / SUM(f.units_net_sold)) END AS unit_price_jpy_incl_display,
        CASE WHEN COALESCE(SUM(f.gross_sales_jpy_incl), 0) = 0 THEN 0
             ELSE ROUND((SUM(f.variable_margin_jpy_incl) - SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1)))) * 1.0 / SUM(f.gross_sales_jpy_incl), 4) END AS gross_margin_rate,
        s.season_code, s.season_year, s.start_date_jst, s.end_date_jst
      FROM mart_gift_season_occurrences s
      JOIN mirror_linegift_finance_sku_daily f
        ON f.date_jst BETWEEN s.start_date_jst AND s.end_date_jst
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE s.is_active = 1
        AND s.season_code = ?
        AND s.season_year = COALESCE(?, (SELECT MAX(season_year) FROM mart_gift_season_occurrences WHERE season_code = ? AND is_active = 1))
      GROUP BY f.ne_code, f.sku_code, s.season_code, s.season_year, s.start_date_jst, s.end_date_jst
      ORDER BY ${orderCol} DESC, f.sku_code ASC
      LIMIT ?
    `).all(
      filters.season_code,
      filters.season_year || null, filters.season_code,
      limit
    );
  }

  // 通常 window
  const w = validateWindow(filters.window);
  return db.prepare(`
    SELECT *
    FROM mart_linegift_sku_perf_daily
    WHERE window_days = ?
      AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM mart_linegift_sku_perf_daily WHERE window_days = ?)
    ORDER BY ${orderCol} DESC, sku_code ASC
    LIMIT ?
  `).all(w, w, limit);
}

/**
 * 価格帯テーブル (期間別 materialized)
 */
export function getPriceBandSummary(windowDays) {
  const w = validateWindow(windowDays);
  const db = getMirrorDB();
  return db.prepare(`
    SELECT *
    FROM mart_linegift_price_band_summary_daily
    WHERE window_days = ?
      AND as_of_date_jst = (SELECT MAX(as_of_date_jst) FROM mart_linegift_price_band_summary_daily WHERE window_days = ?)
    ORDER BY sort_order
  `).all(w, w);
}

/**
 * シーン一覧 (フィルタ用)
 */
export function listSeasons() {
  const db = getMirrorDB();
  return db.prepare(`
    SELECT
      s.season_code, s.season_name,
      (SELECT season_year       FROM mart_gift_season_occurrences WHERE season_code = s.season_code ORDER BY season_year DESC LIMIT 1) AS latest_year,
      (SELECT start_date_jst    FROM mart_gift_season_occurrences WHERE season_code = s.season_code ORDER BY season_year DESC LIMIT 1) AS latest_start,
      (SELECT end_date_jst      FROM mart_gift_season_occurrences WHERE season_code = s.season_code ORDER BY season_year DESC LIMIT 1) AS latest_end
    FROM mart_gift_seasons s
    WHERE s.is_active = 1 AND s.valid_to IS NULL
    ORDER BY s.priority DESC, s.season_name
  `).all();
}

/**
 * build 状態 (UI 上の lock 表示用)
 */
export function getBuildStatus() {
  const db = getMirrorDB();
  const current = db.prepare(`
    SELECT lock_name, status, acquired_at, heartbeat_at, error_message
    FROM mart_build_locks
    WHERE lock_name = 'build-linegift-analytics-mart'
  `).get() || null;
  const recent = db.prepare(`
    SELECT lock_name, status, acquired_at, released_at, duration_seconds, error_message
    FROM mart_build_lock_history
    WHERE lock_name = 'build-linegift-analytics-mart'
    ORDER BY released_at DESC
    LIMIT 5
  `).all();
  return { current, recent };
}
