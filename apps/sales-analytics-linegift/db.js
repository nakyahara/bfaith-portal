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
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const MAX_PERIOD_DAYS = 1100; // ~3年。ad-hoc 計算負荷上限

function validateWindow(w) {
  const n = Number(w);
  if (!VALID_WINDOWS.has(n)) throw new Error(`invalid window: ${w} (must be 7/28/90)`);
  return n;
}

// 実在カレンダー日かを round-trip で検証 (Codex Round 1 High: 2026-02-31 等を Date.parse は自動補正する)
function isRealCalendarDate(s) {
  if (!ISO_DATE_RE.test(s)) return false;
  const [yy, mm, dd] = s.split('-').map(Number);
  const d = new Date(Date.UTC(yy, mm - 1, dd));
  return d.getUTCFullYear() === yy && d.getUTCMonth() === mm - 1 && d.getUTCDate() === dd;
}

function validatePeriod(from, to) {
  if (!isRealCalendarDate(from)) throw new Error(`invalid from: ${from} (YYYY-MM-DD, real calendar date)`);
  if (!isRealCalendarDate(to))   throw new Error(`invalid to: ${to} (YYYY-MM-DD, real calendar date)`);
  if (from > to) throw new Error(`invalid period: from(${from}) > to(${to})`);
  const days = (Date.parse(to + 'T00:00:00Z') - Date.parse(from + 'T00:00:00Z')) / 86400000 + 1;
  if (days > MAX_PERIOD_DAYS) throw new Error(`period too long: ${days} days (max ${MAX_PERIOD_DAYS})`);
  return { from, to };
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
 * トップ KPI (任意期間、on-the-fly 集計、送料引き後粗利)
 * materialized 表は 7/28/90 のみのため、月別/カスタム期間は mirror から直接集計
 */
export function getKpiSummaryByPeriod(from, to) {
  validatePeriod(from, to);
  const db = getMirrorDB();
  const row = db.prepare(`
    WITH base AS (
      SELECT f.gross_sales_jpy_incl, f.variable_margin_jpy_incl, f.order_count, f.units_net_sold,
             COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1)) AS unit_shipping_cost_jpy_incl
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst BETWEEN ? AND ?
    )
    SELECT
      ROUND(COALESCE(SUM(gross_sales_jpy_incl), 0)) AS total_sales_jpy_incl,
      COALESCE(SUM(order_count), 0) AS total_orders,
      CASE WHEN COALESCE(SUM(order_count), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(gross_sales_jpy_incl), 0) / COALESCE(SUM(order_count), 0)) END AS avg_order_value_jpy_incl,
      ROUND(COALESCE(SUM(variable_margin_jpy_incl), 0) - COALESCE(SUM(units_net_sold * unit_shipping_cost_jpy_incl), 0)) AS total_gross_profit_jpy_incl,
      CASE WHEN COALESCE(SUM(gross_sales_jpy_incl), 0) = 0 THEN 0
           ELSE ROUND((COALESCE(SUM(variable_margin_jpy_incl), 0) - COALESCE(SUM(units_net_sold * unit_shipping_cost_jpy_incl), 0)) * 1.0 / COALESCE(SUM(gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate
    FROM base
  `).get(from, to);
  if (!row) return null;
  // KPI が全部 0 なら期間内データなしとして null を返す (UI 側は "(データなし)" 表示)
  if ((row.total_sales_jpy_incl || 0) === 0 && (row.total_orders || 0) === 0) return null;
  return {
    ...row,
    as_of_date_jst: to,
    synced_at: `期間: ${from} 〜 ${to}`,
  };
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

  // 任意期間 (月別 / カスタム期間) は mirror から on-the-fly 集計 (season 経路と同じ計算)
  if (filters.from && filters.to) {
    validatePeriod(filters.from, filters.to);
    return db.prepare(`
      SELECT
        f.ne_code, f.sku_code, MAX(f.product_name) AS product_name,
        COALESCE(SUM(f.units_net_sold), 0) AS units,
        ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS sales_amount_jpy_incl,
        ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) AS gross_profit_jpy_incl,
        COALESCE(SUM(f.order_count), 0) AS orders,
        CASE WHEN COALESCE(SUM(f.units_net_sold), 0) = 0 THEN 0
             ELSE ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0) * 1.0 / COALESCE(SUM(f.units_net_sold), 0)) END AS unit_price_jpy_incl_display,
        CASE WHEN COALESCE(SUM(f.gross_sales_jpy_incl), 0) = 0 THEN 0
             ELSE ROUND((COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) * 1.0 / COALESCE(SUM(f.gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst BETWEEN ? AND ?
      GROUP BY f.ne_code, f.sku_code
      HAVING COALESCE(SUM(f.units_net_sold), 0) > 0
      ORDER BY ${orderCol} DESC, f.sku_code ASC
      LIMIT ?
    `).all(filters.from, filters.to, limit);
  }

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
 * 価格帯テーブル (任意期間、on-the-fly 集計)
 * 各 SKU を「期間内の実売平均単価 = sales/units」で band に割り振り、band 別に集計
 */
export function getPriceBandSummaryByPeriod(from, to) {
  validatePeriod(from, to);
  const db = getMirrorDB();
  const bands = db.prepare(`
    SELECT band_code, band_label, sort_order, min_price_jpy_incl, max_price_jpy_incl
    FROM mart_price_bands
    WHERE is_active = 1 AND valid_to IS NULL
    ORDER BY sort_order
  `).all();
  const resolveBand = (priceRaw) => {
    if (priceRaw == null || Number.isNaN(priceRaw)) return null;
    for (const b of bands) {
      const ok = priceRaw >= b.min_price_jpy_incl &&
                 (b.max_price_jpy_incl == null || priceRaw <= b.max_price_jpy_incl);
      if (ok) return b.band_code;
    }
    return null;
  };

  const rows = db.prepare(`
    SELECT
      f.ne_code, f.sku_code,
      COALESCE(SUM(f.units_net_sold), 0) AS units,
      COALESCE(SUM(f.gross_sales_jpy_incl), 0) AS sales_jpy_incl,
      COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0) AS gross_profit_jpy_incl,
      COALESCE(SUM(f.order_count), 0) AS orders
    FROM mirror_linegift_finance_sku_daily f
    LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
    WHERE f.date_jst BETWEEN ? AND ?
    GROUP BY f.ne_code, f.sku_code
    HAVING units > 0
  `).all(from, to);

  const agg = new Map();
  for (const b of bands) {
    agg.set(b.band_code, {
      band_label: b.band_label, sort_order: b.sort_order,
      sku_count: 0, orders_count: 0, sales_jpy_incl: 0, gross_profit_jpy_incl: 0,
    });
  }
  for (const r of rows) {
    const priceRaw = r.units === 0 ? null : r.sales_jpy_incl / r.units;
    const band = resolveBand(priceRaw);
    if (!band || !agg.has(band)) continue;
    const a = agg.get(band);
    a.sku_count += 1;
    a.orders_count += Number(r.orders) || 0;
    a.sales_jpy_incl += Number(r.sales_jpy_incl) || 0;
    a.gross_profit_jpy_incl += Number(r.gross_profit_jpy_incl) || 0;
  }
  return bands.map((b) => {
    const a = agg.get(b.band_code);
    const gmr = a.sales_jpy_incl === 0 ? 0 : Math.round(a.gross_profit_jpy_incl / a.sales_jpy_incl * 10000) / 10000;
    return {
      price_band_code_sales: b.band_code, band_label: b.band_label, sort_order: b.sort_order,
      sku_count: a.sku_count, orders_count: a.orders_count,
      sales_jpy_incl: Math.round(a.sales_jpy_incl),
      gross_profit_jpy_incl: Math.round(a.gross_profit_jpy_incl),
      gross_margin_rate: gmr,
    };
  });
}

/**
 * 利用可能な年月一覧 (月別セレクタ用)
 * mirror_linegift_finance_sku_daily に集計が存在する年月を新しい順で返す
 */
export function listAvailableMonths() {
  const db = getMirrorDB();
  return db.prepare(`
    SELECT DISTINCT substr(date_jst, 1, 7) AS year_month
    FROM mirror_linegift_finance_sku_daily
    ORDER BY year_month DESC
  `).all().map((r) => r.year_month);
}

/**
 * 期間モードから (from, to) を解決
 *   - mode='window' (or absent + window present): null を返す (window 経路を使う)
 *   - mode='month' + month=YYYY-MM: その月の月初・月末
 *   - mode='custom' + from, to:    そのまま (validatePeriod 経由)
 * 戻り値: { from, to } or null
 */
export function resolvePeriod(query) {
  if (!query) return null;
  if (query.from && query.to) {
    return validatePeriod(String(query.from), String(query.to));
  }
  if (query.month) {
    const m = String(query.month);
    if (!/^\d{4}-\d{2}$/.test(m)) throw new Error(`invalid month: ${m} (YYYY-MM)`);
    // YYYY-MM-01 から翌月初の前日
    const [yy, mm] = m.split('-').map(Number);
    const last = new Date(Date.UTC(yy, mm, 0));
    const to = `${m}-${String(last.getUTCDate()).padStart(2,'0')}`;
    return validatePeriod(`${m}-01`, to);
  }
  return null;
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
