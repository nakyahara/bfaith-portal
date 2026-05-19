/**
 * 誤出荷ダッシュボード KPI 集計
 *
 * 期間を渡すと以下を返す:
 *   - 誤出荷件数 / 誤出荷注文数 / 出荷総数 / 誤出荷率
 *   - 月間損失額
 *   - Top 5 SKU (件数 / 損失額)
 *   - Top 3 根本原因 (root_cause_stage != 'unknown')
 *   - 月次推移 (直近 6 ヶ月)
 *   - 前期間比較 (前月 / 前週)
 *
 * 設計:
 *   - 分母 (出荷総数): mirror_sales_daily.注文数 SUM (期間内)
 *   - 分子 (誤出荷):   f_mis_shipments (deleted_at IS NULL)
 *   - JST 日付前提 (occurred_on は 'YYYY-MM-DD' JST 文字列、mirror_sales_daily.日付 も JST)
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_設計書_v5.md (v7.3)
 */

import { getMirrorDB } from '../warehouse-mirror/db.js';

// ─── JST 日付ユーティリティ (Date.toISOString / ローカル getFullYear 禁止) ───
const JST_DATE_FMT = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Tokyo',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

/** Date → 'YYYY-MM-DD' (JST) */
function jstDateString(date) {
  return JST_DATE_FMT.format(date);
}

/** 今日 (JST) の 'YYYY-MM-DD' */
export function jstToday() {
  return jstDateString(new Date());
}

/** 'YYYY-MM-DD' を Date オブジェクトに (JST 00:00 想定、UTC 環境でも正しく解釈) */
function parseJstDate(ymd) {
  const [y, m, d] = ymd.split('-').map(Number);
  // JST 00:00 = UTC 前日 15:00 だが、ここでは文字列比較で扱うので Date オブジェクトは内部計算用のみ
  return new Date(Date.UTC(y, m - 1, d));
}

/** ymd + n 日 → 'YYYY-MM-DD' (JST) */
function addDays(ymd, days) {
  const d = parseJstDate(ymd);
  d.setUTCDate(d.getUTCDate() + days);
  return jstDateString(new Date(d.getTime() + 9 * 3600 * 1000));
}

/** ymd の月初日 → 'YYYY-MM-DD' */
function startOfMonth(ymd) {
  return ymd.slice(0, 7) + '-01';
}

/** ymd の翌月初日 → 'YYYY-MM-DD' (排他的上限として使う) */
function startOfNextMonth(ymd) {
  const y = parseInt(ymd.slice(0, 4), 10);
  const m = parseInt(ymd.slice(5, 7), 10);
  const ny = m === 12 ? y + 1 : y;
  const nm = m === 12 ? 1 : m + 1;
  return `${ny}-${String(nm).padStart(2, '0')}-01`;
}

/** ymd の前月初日 */
function startOfPrevMonth(ymd) {
  const y = parseInt(ymd.slice(0, 4), 10);
  const m = parseInt(ymd.slice(5, 7), 10);
  const py = m === 1 ? y - 1 : y;
  const pm = m === 1 ? 12 : m - 1;
  return `${py}-${String(pm).padStart(2, '0')}-01`;
}

/** ymd の週初日 (月曜日) */
function startOfWeek(ymd) {
  const d = parseJstDate(ymd);
  const dow = d.getUTCDay(); // 0=Sun .. 6=Sat
  const offsetToMon = (dow + 6) % 7;
  return addDays(ymd, -offsetToMon);
}

// ─── 期間決定 (period 種別 → from/to) ───
/**
 * @param {string} period - 'month' | 'week' | 'custom'
 * @param {string} [from] - custom 時の開始日 'YYYY-MM-DD'
 * @param {string} [to]   - custom 時の終了日 'YYYY-MM-DD' (排他的上限ではない、終端含む)
 * @param {string} [today] - 基準日 (デフォルト JST 今日)
 */
export function resolvePeriod(period, from, to, today) {
  const t = today || jstToday();
  if (period === 'week') {
    const start = startOfWeek(t);
    const endExclusive = addDays(start, 7);
    return { from: start, to: endExclusive, label: `${start} 〜 ${addDays(endExclusive, -1)} (週)` };
  }
  if (period === 'custom' && from && to) {
    return { from, to: addDays(to, 1), label: `${from} 〜 ${to} (カスタム)` };
  }
  // default: month
  const start = startOfMonth(t);
  const endExclusive = startOfNextMonth(start);
  return { from: start, to: endExclusive, label: `${start.slice(0, 7)} (月)` };
}

/** 前期間 (前月 / 前週 / カスタム長さの直前期間) */
export function resolvePrevPeriod(period, from, to, today) {
  const cur = resolvePeriod(period, from, to, today);
  if (period === 'week') {
    const prevStart = addDays(cur.from, -7);
    return { from: prevStart, to: cur.from, label: `前週` };
  }
  if (period === 'custom' && from && to) {
    // 同じ長さの直前期間
    const lenDays = Math.round((parseJstDate(cur.to).getTime() - parseJstDate(cur.from).getTime()) / 86400000);
    const prevTo = cur.from;
    const prevFrom = addDays(prevTo, -lenDays);
    return { from: prevFrom, to: prevTo, label: `直前 ${lenDays} 日` };
  }
  // month
  const prevStart = startOfPrevMonth(cur.from);
  return { from: prevStart, to: cur.from, label: `前月 (${prevStart.slice(0, 7)})` };
}

// ─── KPI 集計 ───
/**
 * 期間 [from, to) で誤出荷 KPI を集計。
 * @param {object} db better-sqlite3 instance (warehouse-mirror.db)
 * @param {{from: string, to: string}} period 'YYYY-MM-DD' 文字列、to は排他的上限
 */
export function getKpiForPeriod(db, { from, to }) {
  // 1. 件数 / 損失額 (f_mis_shipments)
  const aggRow = db.prepare(`
    SELECT
      COUNT(*) AS incidents,
      COUNT(DISTINCT (COALESCE(mall, '') || '|' || COALESCE(mall_order_id, 'unk-' || id))) AS distinct_orders,
      COALESCE(SUM(loss_amount_jpy), 0) AS total_loss
    FROM f_mis_shipments
    WHERE deleted_at IS NULL
      AND occurred_on >= ?
      AND occurred_on < ?
  `).get(from, to);

  // 2. 出荷総数 (mirror_sales_daily.注文数 SUM)
  // 注: 「注文数」フィールドは モール×商品×日 単位の集計値で、複数 SKU 注文は重複可能性あり。
  // 業界の「Order Defect Rate」スタイルの近似値として SUM を使う。Codex 設計書の「分母定義の監査」項目。
  const shipRow = db.prepare(`
    SELECT COALESCE(SUM(注文数), 0) AS shipped_orders
    FROM mirror_sales_daily
    WHERE 日付 >= ?
      AND 日付 < ?
  `).get(from, to);

  const incidents = aggRow.incidents;
  const distinctOrders = aggRow.distinct_orders;
  const totalLoss = aggRow.total_loss;
  const shippedOrders = shipRow.shipped_orders;

  return {
    incidents,
    distinct_orders: distinctOrders,
    shipped_orders: shippedOrders,
    total_loss_jpy: totalLoss,
    error_rate_pct: shippedOrders > 0 ? (distinctOrders * 100 / shippedOrders) : null,
    incident_rate_pct: shippedOrders > 0 ? (incidents * 100 / shippedOrders) : null,
    loss_per_1000_orders_jpy: shippedOrders > 0 ? Math.round(totalLoss * 1000 / shippedOrders) : null,
  };
}

/** Top N SKU (件数順、tie-break: 損失額) */
export function getTopSkus(db, { from, to }, limit = 5) {
  return db.prepare(`
    SELECT
      sku_snapshot AS sku,
      mall,
      product_name_snapshot AS product_name,
      COUNT(*) AS incidents,
      COALESCE(SUM(loss_amount_jpy), 0) AS loss_jpy
    FROM f_mis_shipments
    WHERE deleted_at IS NULL
      AND occurred_on >= ?
      AND occurred_on < ?
      AND sku_snapshot IS NOT NULL
    GROUP BY sku_snapshot, mall, product_name_snapshot
    ORDER BY incidents DESC, loss_jpy DESC
    LIMIT ?
  `).all(from, to, limit);
}

/** Top N 根本原因 (件数順、root_cause_stage = 'unknown' は除外) */
export function getTopRootCauses(db, { from, to }, limit = 3) {
  return db.prepare(`
    SELECT
      root_cause_stage,
      COUNT(*) AS incidents,
      COALESCE(SUM(loss_amount_jpy), 0) AS loss_jpy
    FROM f_mis_shipments
    WHERE deleted_at IS NULL
      AND occurred_on >= ?
      AND occurred_on < ?
      AND root_cause_stage != 'unknown'
    GROUP BY root_cause_stage
    ORDER BY incidents DESC, loss_jpy DESC
    LIMIT ?
  `).all(from, to, limit);
}

/** 月次推移 (直近 N ヶ月、N=6 デフォルト) */
export function getMonthlyTrend(db, { today }, monthsBack = 6) {
  const t = today || jstToday();
  // 直近 N ヶ月の月初を生成
  const months = [];
  let cursor = startOfMonth(t);
  for (let i = 0; i < monthsBack; i++) {
    months.unshift(cursor);
    cursor = startOfPrevMonth(cursor);
  }
  // SQL で月毎集計
  const trend = months.map((monthStart) => {
    const monthEnd = startOfNextMonth(monthStart);
    const mis = db.prepare(`
      SELECT
        COUNT(*) AS incidents,
        COALESCE(SUM(loss_amount_jpy), 0) AS loss_jpy
      FROM f_mis_shipments
      WHERE deleted_at IS NULL
        AND occurred_on >= ?
        AND occurred_on < ?
    `).get(monthStart, monthEnd);
    const ship = db.prepare(`
      SELECT COALESCE(SUM(注文数), 0) AS shipped
      FROM mirror_sales_daily
      WHERE 日付 >= ?
        AND 日付 < ?
    `).get(monthStart, monthEnd);
    const rate = ship.shipped > 0 ? (mis.incidents * 100 / ship.shipped) : null;
    return {
      month: monthStart.slice(0, 7),  // 'YYYY-MM'
      incidents: mis.incidents,
      loss_jpy: mis.loss_jpy,
      shipped_orders: ship.shipped,
      error_rate_pct: rate,
    };
  });
  return trend;
}

// ─── 統合: ダッシュボード 1 画面分の全情報 ───
/**
 * @param {object} options
 *   - period: 'month' | 'week' | 'custom'
 *   - from, to: custom 時の 'YYYY-MM-DD'
 *   - today: 基準日 (デフォルト JST 今日)
 *   - topSkuLimit: default 5
 *   - topCauseLimit: default 3
 *   - monthsBack: default 6
 */
export function getMisShipmentDashboard(options = {}) {
  const db = getMirrorDB();
  const today = options.today || jstToday();
  const period = options.period || 'month';
  const cur = resolvePeriod(period, options.from, options.to, today);
  const prev = resolvePrevPeriod(period, options.from, options.to, today);

  const current = getKpiForPeriod(db, cur);
  const previous = getKpiForPeriod(db, prev);
  const topSkus = getTopSkus(db, cur, options.topSkuLimit || 5);
  const topRootCauses = getTopRootCauses(db, cur, options.topCauseLimit || 3);
  const monthlyTrend = getMonthlyTrend(db, { today }, options.monthsBack || 6);

  return {
    period: { type: period, ...cur },
    prev_period: { ...prev },
    current,
    previous,
    diff: {
      incidents: current.incidents - previous.incidents,
      total_loss_jpy: current.total_loss_jpy - previous.total_loss_jpy,
      error_rate_pct_diff: (current.error_rate_pct ?? 0) - (previous.error_rate_pct ?? 0),
    },
    top_skus: topSkus,
    top_root_causes: topRootCauses,
    monthly_trend: monthlyTrend,
    industry_target_pct: 0.10,  // 業界目標 (WERC ベンチマーク補数)
    generated_at: new Date().toISOString(),
  };
}

/** GChat 通知用の簡素サマリ (損失額抜き、業界目標との比較) */
export function getMisShipmentNotifySummary(options = {}) {
  const db = getMirrorDB();
  const today = options.today || jstToday();

  // 月間累計
  const cur = resolvePeriod('month', null, null, today);
  const kpi = getKpiForPeriod(db, cur);
  const topSkus = getTopSkus(db, cur, 1);
  const topRootCauses = getTopRootCauses(db, cur, 1);

  return {
    period_label: cur.label,
    period_start: cur.from,
    period_end_inclusive: addDays(cur.to, -1),
    error_rate_pct: kpi.error_rate_pct,
    incident_rate_pct: kpi.incident_rate_pct,
    incidents: kpi.incidents,
    distinct_orders: kpi.distinct_orders,
    shipped_orders: kpi.shipped_orders,
    industry_target_pct: 0.10,
    over_target: kpi.error_rate_pct != null && kpi.error_rate_pct > 0.10,
    top_sku: topSkus.length > 0 ? topSkus[0] : null,
    top_root_cause: topRootCauses.length > 0 ? topRootCauses[0] : null,
  };
}
