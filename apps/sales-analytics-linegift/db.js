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

// 前期比 共通: 期間内の KPI を on-the-fly 集計 (送料引き後粗利、税込換算)
function computeKpiInRange(db, from, to) {
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
  return row || {
    total_sales_jpy_incl: 0, total_orders: 0, avg_order_value_jpy_incl: 0,
    total_gross_profit_jpy_incl: 0, gross_margin_rate: 0,
  };
}

function shiftIsoDate(iso, days) {
  const t = Date.parse(iso + 'T00:00:00Z');
  return new Date(t + days * 86400000).toISOString().slice(0, 10);
}

// 現在期と前期の (from, to) を解決
// season: シーン期間 vs 前年同シーン期間 (PR-D Codex Round 2 High 修正: 季節絞り込み中の整合性)
// window: as_of (mirror の最新日) から N 日窓、前期は同じ N 日でその直前
// month:  当月 / 前月 (カレンダー基準)
// custom: 同期間ずらし (例: 5/1-5/15 → 4/16-4/30、15日 vs 15日)
function resolveComparisonRanges(periodSpec) {
  if (periodSpec.season_code) {
    const db = getMirrorDB();
    // 最新の season_year occurrence を current にする (year 指定時はその year、未指定なら最新)
    const occ = periodSpec.season_year
      ? db.prepare(`SELECT season_year, start_date_jst, end_date_jst FROM mart_gift_season_occurrences
                    WHERE season_code = ? AND season_year = ? AND is_active = 1`).get(periodSpec.season_code, periodSpec.season_year)
      : db.prepare(`SELECT season_year, start_date_jst, end_date_jst FROM mart_gift_season_occurrences
                    WHERE season_code = ? AND is_active = 1
                    ORDER BY season_year DESC LIMIT 1`).get(periodSpec.season_code);
    if (!occ) return null;
    // 前年同シーン
    const prevOcc = db.prepare(`SELECT start_date_jst, end_date_jst FROM mart_gift_season_occurrences
                                WHERE season_code = ? AND season_year = ? AND is_active = 1`)
      .get(periodSpec.season_code, occ.season_year - 1);
    return {
      curr: { from: occ.start_date_jst, to: occ.end_date_jst },
      // 前年 occurrence が無い場合は当年と同じ期間を渡しても集計は 0 件にはならないので、null sentinel として未来の 1 日範囲を使い前期 0 を保証
      prev: prevOcc
        ? { from: prevOcc.start_date_jst, to: prevOcc.end_date_jst }
        : { from: '9999-12-31', to: '9999-12-31' },
      mode: 'season',
      span_days: null,
    };
  }
  if (periodSpec.from && periodSpec.to) {
    validatePeriod(periodSpec.from, periodSpec.to);
    const days = (Date.parse(periodSpec.to + 'T00:00:00Z') - Date.parse(periodSpec.from + 'T00:00:00Z')) / 86400000 + 1;
    return {
      curr: { from: periodSpec.from, to: periodSpec.to },
      prev: { from: shiftIsoDate(periodSpec.from, -days), to: shiftIsoDate(periodSpec.from, -1) },
      mode: 'custom',
      span_days: days,
    };
  }
  if (periodSpec.month) {
    if (!/^\d{4}-\d{2}$/.test(periodSpec.month)) throw new Error(`invalid month: ${periodSpec.month}`);
    const [yy, mm] = periodSpec.month.split('-').map(Number);
    const currLast = new Date(Date.UTC(yy, mm, 0)).getUTCDate();
    const currFrom = `${yy}-${String(mm).padStart(2,'0')}-01`;
    const currTo   = `${yy}-${String(mm).padStart(2,'0')}-${String(currLast).padStart(2,'0')}`;
    const pYy = mm === 1 ? yy - 1 : yy;
    const pMm = mm === 1 ? 12 : mm - 1;
    const prevLast = new Date(Date.UTC(pYy, pMm, 0)).getUTCDate();
    const prevFrom = `${pYy}-${String(pMm).padStart(2,'0')}-01`;
    const prevTo   = `${pYy}-${String(pMm).padStart(2,'0')}-${String(prevLast).padStart(2,'0')}`;
    return { curr: { from: currFrom, to: currTo }, prev: { from: prevFrom, to: prevTo }, mode: 'month', span_days: currLast };
  }
  // window
  const w = validateWindow(periodSpec.window);
  const db = getMirrorDB();
  const row = db.prepare(`SELECT MAX(date_jst) AS d FROM mirror_linegift_finance_sku_daily`).get();
  if (!row?.d) return null;
  const asOf = row.d;
  return {
    curr: { from: shiftIsoDate(asOf, -(w - 1)), to: asOf },
    prev: { from: shiftIsoDate(asOf, -(2 * w - 1)), to: shiftIsoDate(asOf, -w) },
    mode: 'window',
    span_days: w,
  };
}

/**
 * KPI + 前期比 (中原さん指摘 2026-05-28)
 * 戻り値: { current, previous, delta_pct, ranges, mode }
 *   - delta_pct.* は変化率 (-1.0 〜 +N.NN、null = 前期 0 で計算不能)
 *   - gross_margin_rate_pt は pt 差 (比率の差、四桁丸め)
 */
export function getKpiWithPrevious(periodSpec) {
  const ranges = resolveComparisonRanges(periodSpec || {});
  if (!ranges) return null;
  const db = getMirrorDB();
  const current  = computeKpiInRange(db, ranges.curr.from, ranges.curr.to);
  const previous = computeKpiInRange(db, ranges.prev.from, ranges.prev.to);
  // current 全 0 = 期間内データなし
  if ((current.total_sales_jpy_incl || 0) === 0 && (current.total_orders || 0) === 0) return null;
  const deltaPct = (c, p) => (p === 0 ? null : Math.round(((c - p) / p) * 10000) / 10000);
  return {
    current:  { ...current,  from: ranges.curr.from, to: ranges.curr.to },
    previous: { ...previous, from: ranges.prev.from, to: ranges.prev.to },
    delta_pct: {
      total_sales_jpy_incl:        deltaPct(current.total_sales_jpy_incl,        previous.total_sales_jpy_incl),
      total_orders:                deltaPct(current.total_orders,                previous.total_orders),
      avg_order_value_jpy_incl:    deltaPct(current.avg_order_value_jpy_incl,    previous.avg_order_value_jpy_incl),
      total_gross_profit_jpy_incl: deltaPct(current.total_gross_profit_jpy_incl, previous.total_gross_profit_jpy_incl),
      // 粗利率は pt 差 (current.rate - previous.rate)
      gross_margin_rate_pt: Math.round((current.gross_margin_rate - previous.gross_margin_rate) * 10000) / 10000,
    },
    mode: ranges.mode,
    span_days: ranges.span_days,
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
 * 死筋 SKU 抽出 (中原さん指示 2026-05-28、撤退判断支援)
 *
 * 判定 (Codex 推奨「基準を明文化」、PR-E Codex Round 1 High 修正で 件数=注文件数 に統一):
 *   - 赤 (撤退候補):   粗利率 < 0 (期間内売上 > 0)            ← 売れば売るほど赤字
 *   - 黄 (要注意):     0 <= 粗利率 < 0.10 (期間内売上 > 0 かつ 注文件数 < 5)  ← 低粗利率 × 低回転
 *   - その他は除外 (健全 SKU はランキングで見る)
 *
 * 期間モードは既存と同一 (window/month/custom/season)。
 * ソート: 粗利額の少ない順 (赤字が大きい SKU が先頭)。
 */
export function getDeadStockSkus(periodSpec) {
  const ranges = resolveComparisonRanges(periodSpec || {});
  if (!ranges) return null;
  const db = getMirrorDB();
  const rows = db.prepare(`
    SELECT
      f.ne_code, f.sku_code, MAX(f.product_name) AS product_name,
      ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS sales_jpy_incl,
      ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) AS gross_profit_jpy_incl,
      COALESCE(SUM(f.units_net_sold), 0) AS units,
      COALESCE(SUM(f.order_count), 0) AS orders,
      CASE WHEN COALESCE(SUM(f.gross_sales_jpy_incl), 0) = 0 THEN 0
           ELSE ROUND((COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) * 1.0 / COALESCE(SUM(f.gross_sales_jpy_incl), 0), 4) END AS gross_margin_rate
    FROM mirror_linegift_finance_sku_daily f
    LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
    WHERE f.date_jst BETWEEN ? AND ?
    GROUP BY f.ne_code, f.sku_code
    HAVING sales_jpy_incl > 0
       AND (
         gross_margin_rate < 0
         OR (gross_margin_rate < 0.10 AND orders < 5)
       )
    ORDER BY gross_profit_jpy_incl ASC, sales_jpy_incl DESC
    LIMIT 200
  `).all(ranges.curr.from, ranges.curr.to);
  // フラグ付与
  return rows.map((r) => ({
    ...r,
    risk_level: r.gross_margin_rate < 0 ? 'red' : 'yellow',
    risk_label: r.gross_margin_rate < 0 ? '💀 赤字' : '⚠️ 低粗利率×低回転',
  }));
}

/**
 * 新商品立ち上がり ramp (中原さん指示 2026-05-28、新商品の初速・採用判断)
 *
 * v_linegift_new_item_ramp が day_from_launch 0-60 で units/sales/profit を持つ。
 * - 過去 launch_lookback_days 以内に first_sale_date_jst のある SKU を対象
 * - SKU 別に cumulative_sales (day0..day60) を返す + 全体中央値カーブ
 * - 中原さんが「初週ベンチ比 / 30日達成」で見られる
 */
export function getNewItemRamps(opts = {}) {
  const db = getMirrorDB();
  const lookback = Math.max(7, Math.min(365, Number(opts.launch_lookback_days) || 90));
  // 対象 SKU を絞る (first_sale_date_jst が直近 lookback 日以内)
  // PR-E Codex Round 3/4 High:
  //   - view v_linegift_new_item_ramp は内部で JOIN するため SKU 抽出には重い
  //   - 集約後 HAVING でなく、CTE materialize → WHERE 句で絞り込む形に変更 (集約後評価回避)
  //   - cutoff_date は JS で先に確定し、string 比較で渡す
  const jstNowMs = Date.now() + 9 * 3600 * 1000;
  const cutoffDate = new Date(jstNowMs - lookback * 86400000).toISOString().slice(0, 10);
  const skus = db.prepare(`
    WITH first_sale AS (
      SELECT sku_code, ne_code,
             MAX(product_name) AS product_name,
             MIN(date_jst) AS first_sale_date_jst
      FROM mirror_linegift_finance_sku_daily
      GROUP BY sku_code, ne_code
    )
    SELECT sku_code, ne_code, product_name, first_sale_date_jst
    FROM first_sale
    WHERE first_sale_date_jst >= ?
    ORDER BY first_sale_date_jst DESC
    LIMIT 50
  `).all(cutoffDate);
  if (skus.length === 0) return { skus: [], median: [] };

  // 各 SKU の day_from_launch × cumulative_sales (送料引き後粗利も)
  const rampRows = db.prepare(`
    SELECT
      r.sku_code, r.ne_code, r.day_from_launch,
      r.units, r.sales,
      -- 粗利は送料引き後 (LINEギフトの送料無料分を弊社負担)
      r.profit - COALESCE(r.units * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1)), 0) AS profit_adj
    FROM v_linegift_new_item_ramp r
    LEFT JOIN mirror_products p ON p.商品コード = r.ne_code
    WHERE r.sku_code IN (${skus.map(() => '?').join(',')})
    -- 同 sku_code に複数 ne_code がある場合、ne_code を含めないと day 単位で混ざり累積が壊れる (PR-E Codex Round 1 High)
    ORDER BY r.sku_code, r.ne_code, r.day_from_launch
  `).all(...skus.map((s) => s.sku_code));

  // SKU 単位で cumulative 配列を構築
  const byKey = new Map();
  for (const s of skus) {
    byKey.set(`${s.sku_code}|${s.ne_code}`, {
      sku_code: s.sku_code, ne_code: s.ne_code,
      product_name: s.product_name, first_sale_date_jst: s.first_sale_date_jst,
      curve: [], // [{ day, cum_sales, cum_profit, cum_units }]
    });
  }
  let lastKey = null, cumSales = 0, cumProfit = 0, cumUnits = 0, lastDay = -1;
  for (const r of rampRows) {
    const key = `${r.sku_code}|${r.ne_code}`;
    if (key !== lastKey) {
      cumSales = 0; cumProfit = 0; cumUnits = 0; lastDay = -1;
      lastKey = key;
    }
    // day_from_launch の抜けは前値で線形補間せず、抜けたまま (sparkline は line で描く)
    cumSales  += Number(r.sales) || 0;
    cumProfit += Number(r.profit_adj) || 0;
    cumUnits  += Number(r.units) || 0;
    const target = byKey.get(key);
    if (target) target.curve.push({
      day: Number(r.day_from_launch),
      cum_sales: Math.round(cumSales),
      cum_profit: Math.round(cumProfit),
      cum_units: cumUnits,
    });
    lastDay = r.day_from_launch;
  }

  // 中央値カーブ (day 0-60、各 day で各 SKU の cum_sales の median)
  const median = [];
  for (let day = 0; day <= 60; day++) {
    const vals = [];
    for (const s of byKey.values()) {
      const point = s.curve.find((c) => c.day === day);
      if (point) vals.push(point.cum_sales);
    }
    if (vals.length === 0) continue;
    vals.sort((a, b) => a - b);
    const mid = Math.floor(vals.length / 2);
    const med = vals.length % 2 === 0 ? Math.round((vals[mid - 1] + vals[mid]) / 2) : vals[mid];
    median.push({ day, median_cum_sales: med, sample_count: vals.length });
  }

  return {
    skus: Array.from(byKey.values()),
    median,
    lookback_days: lookback,
  };
}

/**
 * シーン × 前年同シーン SKU 比較 (中原さん指示 2026-05-28、PR-F)
 *
 * 当年シーン期間 (最新 season_year) と前年同シーン期間で SKU 単位の売上/粗利/件数を比較。
 * - 当年 OR 前年に売上のあった全 SKU を OUTER 風に union
 * - 並び: 当年売上の降順 (Top 50)
 * - 送料引き後粗利 (mirror_products.送料 を税込換算で控除、LINEギフト FBM パターン)
 */
export function getSeasonComparison(seasonCode, seasonYear) {
  if (!seasonCode) throw new Error('season_code is required');
  const db = getMirrorDB();
  // current occurrence (year 指定なければ最新)
  const occ = seasonYear
    ? db.prepare(`SELECT season_year, start_date_jst, end_date_jst FROM mart_gift_season_occurrences
                  WHERE season_code = ? AND season_year = ? AND is_active = 1`).get(seasonCode, seasonYear)
    : db.prepare(`SELECT season_year, start_date_jst, end_date_jst FROM mart_gift_season_occurrences
                  WHERE season_code = ? AND is_active = 1 ORDER BY season_year DESC LIMIT 1`).get(seasonCode);
  if (!occ) return null;
  const prev = db.prepare(`SELECT season_year, start_date_jst, end_date_jst FROM mart_gift_season_occurrences
                           WHERE season_code = ? AND season_year = ? AND is_active = 1`)
    .get(seasonCode, occ.season_year - 1);

  // 期間集計用 inner SQL を CTE で展開
  const buildSql = `
    WITH curr AS (
      SELECT f.sku_code, f.ne_code, MAX(f.product_name) AS product_name,
        COALESCE(SUM(f.units_net_sold), 0) AS units,
        ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS sales,
        ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) AS profit,
        COALESCE(SUM(f.order_count), 0) AS orders
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst BETWEEN ? AND ?
      GROUP BY f.sku_code, f.ne_code
    ), prev AS (
      SELECT f.sku_code, f.ne_code, MAX(f.product_name) AS product_name,
        COALESCE(SUM(f.units_net_sold), 0) AS units,
        ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS sales,
        ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) AS profit,
        COALESCE(SUM(f.order_count), 0) AS orders
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst BETWEEN ? AND ?
      GROUP BY f.sku_code, f.ne_code
    )
    SELECT
      COALESCE(c.sku_code, p.sku_code) AS sku_code,
      COALESCE(c.ne_code, p.ne_code) AS ne_code,
      COALESCE(c.product_name, p.product_name) AS product_name,
      COALESCE(c.units, 0)  AS curr_units,  COALESCE(p.units, 0)  AS prev_units,
      COALESCE(c.sales, 0)  AS curr_sales,  COALESCE(p.sales, 0)  AS prev_sales,
      COALESCE(c.profit, 0) AS curr_profit, COALESCE(p.profit, 0) AS prev_profit,
      COALESCE(c.orders, 0) AS curr_orders, COALESCE(p.orders, 0) AS prev_orders
    FROM curr c
    FULL OUTER JOIN prev p ON p.sku_code = c.sku_code AND p.ne_code = c.ne_code
    ORDER BY curr_sales DESC, prev_sales DESC
    LIMIT 50
  `;
  // SQLite は FULL OUTER JOIN を 3.39+ でサポート、念のため LEFT JOIN + UNION ALL 風に書き換える方が互換性高い
  const safeSql = `
    WITH curr AS (
      SELECT f.sku_code, f.ne_code, MAX(f.product_name) AS product_name,
        COALESCE(SUM(f.units_net_sold), 0) AS units,
        ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS sales,
        ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) AS profit,
        COALESCE(SUM(f.order_count), 0) AS orders
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst BETWEEN ? AND ?
      GROUP BY f.sku_code, f.ne_code
    ), prev AS (
      SELECT f.sku_code, f.ne_code, MAX(f.product_name) AS product_name,
        COALESCE(SUM(f.units_net_sold), 0) AS units,
        ROUND(COALESCE(SUM(f.gross_sales_jpy_incl), 0)) AS sales,
        ROUND(COALESCE(SUM(f.variable_margin_jpy_incl), 0) - COALESCE(SUM(f.units_net_sold * COALESCE(p.送料, 0) * (1 + COALESCE(p.消費税率, 0.1))), 0)) AS profit,
        COALESCE(SUM(f.order_count), 0) AS orders
      FROM mirror_linegift_finance_sku_daily f
      LEFT JOIN mirror_products p ON p.商品コード = f.ne_code
      WHERE f.date_jst BETWEEN ? AND ?
      GROUP BY f.sku_code, f.ne_code
    ), keys AS (
      SELECT sku_code, ne_code FROM curr
      UNION
      SELECT sku_code, ne_code FROM prev
    )
    SELECT
      k.sku_code, k.ne_code,
      COALESCE(c.product_name, p.product_name) AS product_name,
      COALESCE(c.units, 0)  AS curr_units,  COALESCE(p.units, 0)  AS prev_units,
      COALESCE(c.sales, 0)  AS curr_sales,  COALESCE(p.sales, 0)  AS prev_sales,
      COALESCE(c.profit, 0) AS curr_profit, COALESCE(p.profit, 0) AS prev_profit,
      COALESCE(c.orders, 0) AS curr_orders, COALESCE(p.orders, 0) AS prev_orders
    FROM keys k
    LEFT JOIN curr c ON c.sku_code = k.sku_code AND c.ne_code = k.ne_code
    LEFT JOIN prev p ON p.sku_code = k.sku_code AND p.ne_code = k.ne_code
    ORDER BY curr_sales DESC, prev_sales DESC
    LIMIT 50
  `;
  const params = prev
    ? [occ.start_date_jst, occ.end_date_jst, prev.start_date_jst, prev.end_date_jst]
    : [occ.start_date_jst, occ.end_date_jst, '9999-12-31', '9999-12-31'];
  const rows = db.prepare(safeSql).all(...params);
  // delta 計算と粗利率付与
  const enriched = rows.map((r) => {
    const dSales  = r.prev_sales  === 0 ? null : (r.curr_sales  - r.prev_sales)  / r.prev_sales;
    const dProfit = r.prev_profit === 0 ? null : (r.curr_profit - r.prev_profit) / r.prev_profit;
    const dUnits  = r.prev_units  === 0 ? null : (r.curr_units  - r.prev_units)  / r.prev_units;
    const currRate = r.curr_sales === 0 ? 0 : r.curr_profit / r.curr_sales;
    const prevRate = r.prev_sales === 0 ? 0 : r.prev_profit / r.prev_sales;
    return {
      ...r,
      curr_gross_margin_rate: Math.round(currRate * 10000) / 10000,
      prev_gross_margin_rate: Math.round(prevRate * 10000) / 10000,
      delta_sales_pct:  dSales  == null ? null : Math.round(dSales  * 10000) / 10000,
      delta_profit_pct: dProfit == null ? null : Math.round(dProfit * 10000) / 10000,
      delta_units_pct:  dUnits  == null ? null : Math.round(dUnits  * 10000) / 10000,
      delta_rate_pt:    Math.round((currRate - prevRate) * 10000) / 10000,
    };
  });
  return {
    season_code: seasonCode,
    curr: { season_year: occ.season_year, from: occ.start_date_jst, to: occ.end_date_jst },
    prev: prev ? { season_year: prev.season_year, from: prev.start_date_jst, to: prev.end_date_jst } : null,
    rows: enriched,
  };
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
