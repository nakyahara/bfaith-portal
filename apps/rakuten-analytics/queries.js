/**
 * rakuten-analytics queries.js — データ層 (Render warehouse-mirror.db 読み取り)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/楽天統合管理ダッシュボード_要件定義_20260706.md
 *
 * 使用テーブル (P1):
 *   mirror_rakuten_finance_sku_daily  受注ベース日次fact (D案 status 500/600/700、税込、cancel按分済、
 *                                     snapshot原価、mall_fee は 10% 一律の推定値)
 *   mart_rakuten_monthly_summary      月次確定 (店舗別仕訳書由来: ad_cost=広告費実額 / pf_fee=楽天手数料実額)
 *
 * 精度ラベル方針 (要件 §9-2、税はすべて税込):
 *   速報   = 受注ベース fact (毎朝 daily-sync で当月+2ヶ月再build)
 *   推定   = mall_fee (10%一律) を含む値
 *   確定   = 月次仕訳書取込済み月の実額 (ad_cost / pf_fee)
 *   確定寄せ実質利益 = variable_margin + mall_fee(推定を戻す) − pf_fee(実額) − ad_cost(実額)
 *     ※ pf_fee は「請求合計 − 広告費 − クーポン値引」なのでクーポン (factで控除済) と二重計上しない
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

// ─── 日付 helper (JST。Date.toISOString の UTC 罠に注意: feedback_jst_to_iso_string_trap) ───
export function jstToday() {
  return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
export function addDays(dateStr, n) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
export function monthOf(dateStr) { return dateStr.slice(0, 7); }
export function monthStart(ym) { return `${ym}-01`; }
export function monthEnd(ym) {
  const [y, m] = ym.split('-').map(Number);
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return `${ym}-${String(lastDay).padStart(2, '0')}`;
}
export function addMonths(ym, n) {
  const [y, m] = ym.split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export function isValidDate(s) {
  if (typeof s !== 'string' || !DATE_RE.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// カスタム期間の上限 (日数)。無制限だと day 粒度の巨大集計で DoS になる (Codex R1 Medium)
const MAX_CUSTOM_RANGE_DAYS = 731;

// preset → {from, to} (JST)
export function resolvePeriod(preset, fromQ, toQ) {
  const today = jstToday();
  if (isValidDate(fromQ) && isValidDate(toQ) && fromQ <= toQ) {
    // 上限超過は from を切り詰め (エラーにせず黙って clamp、UI 側は返却 from/to を表示)
    const clampedFrom = fromQ < addDays(toQ, -(MAX_CUSTOM_RANGE_DAYS - 1))
      ? addDays(toQ, -(MAX_CUSTOM_RANGE_DAYS - 1)) : fromQ;
    return { from: clampedFrom, to: toQ, preset: 'custom' };
  }
  const ym = monthOf(today);
  const map = {
    today: { from: today, to: today },
    yesterday: { from: addDays(today, -1), to: addDays(today, -1) },
    '7d': { from: addDays(today, -6), to: today },
    '14d': { from: addDays(today, -13), to: today },
    '30d': { from: addDays(today, -29), to: today },
    '90d': { from: addDays(today, -89), to: today },
    this_month: { from: monthStart(ym), to: today },
    last_month: { from: monthStart(addMonths(ym, -1)), to: monthEnd(addMonths(ym, -1)) },
    this_year: { from: `${today.slice(0, 4)}-01-01`, to: today },
    '12m': { from: monthStart(addMonths(ym, -11)), to: today },
  };
  const r = map[preset] || map['30d'];
  return { ...r, preset: map[preset] ? preset : '30d' };
}

// ─── 受注ベース fact の期間集計 (税込) ───
function factSummary(db, from, to) {
  return db.prepare(`
    SELECT
      COALESCE(SUM(gross_sales_jpy_incl),0)               AS sales_incl,
      COALESCE(SUM(units_ordered),0)                      AS units_ordered,
      COALESCE(SUM(units_net_sold),0)                     AS units_net,
      COALESCE(SUM(allocated_units_cancelled),0)          AS units_cancelled,
      COALESCE(SUM(coupon_shop_jpy_incl),0)               AS coupon_shop,
      COALESCE(SUM(allocated_refund_amount_jpy_incl),0)   AS refunds,
      COALESCE(SUM(mall_fee_jpy_incl),0)                  AS mall_fee_est,
      COALESCE(SUM(shipping_cost_jpy_incl),0)             AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)               AS cogs,
      COALESCE(SUM(variable_margin_jpy_incl),0)           AS variable_margin,
      COALESCE(SUM(CASE WHEN is_cost_complete = 1 THEN gross_sales_jpy_incl END),0) AS sales_cost_complete,
      COUNT(DISTINCT date_jst)                            AS days_with_data
    FROM mirror_rakuten_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 月次確定 (店舗別仕訳書取込済み月のみ) ───
function confirmedMonth(db, ym) {
  return db.prepare(`
    SELECT year_month, COALESCE(ad_cost,0) AS ad_cost, COALESCE(pf_fee,0) AS pf_fee, confirmed_at
    FROM mart_rakuten_monthly_summary
    WHERE year_month = ? AND confirmed_at IS NOT NULL
  `).get(ym) || null;
}

// ─── 概要タブ: タイル 4 枚 (今日/昨日/今月/先月) ───
export function getOverview() {
  const db = getMirrorDB();
  const today = jstToday();
  const ym = monthOf(today);
  const lastYm = addMonths(ym, -1);
  const periods = [
    { key: 'today', label: '今日', from: today, to: today },
    { key: 'yesterday', label: '昨日', from: addDays(today, -1), to: addDays(today, -1) },
    { key: 'this_month', label: '今月', from: monthStart(ym), to: today },
    { key: 'last_month', label: '先月', from: monthStart(lastYm), to: monthEnd(lastYm) },
  ];

  // データ鮮度 (fact は毎朝 07:00 build。今日タイルは翌朝まで 0 になるのが正常)
  // 最新日付の行の synced_at を対で取る (独立 MAX だと別行の時刻が混ざる — Codex R1 Low)
  const fresh = db.prepare(`
    SELECT date_jst AS data_to, synced_at AS last_synced
    FROM mirror_rakuten_finance_sku_daily
    ORDER BY date_jst DESC, synced_at DESC LIMIT 1
  `).get() || { data_to: null, last_synced: null };

  const tiles = periods.map(p => {
    const s = factSummary(db, p.from, p.to);
    const daysInPeriod = Math.round((new Date(p.to + 'T00:00:00Z') - new Date(p.from + 'T00:00:00Z')) / 86400000) + 1;
    const tile = {
      ...p,
      sales_incl: Math.round(s.sales_incl),
      units_ordered: s.units_ordered,
      units_net: s.units_net,
      units_cancelled: s.units_cancelled,
      coupon_shop: Math.round(s.coupon_shop),
      refunds: Math.round(s.refunds),
      mall_fee_est: Math.round(s.mall_fee_est),
      shipping: Math.round(s.shipping),
      cogs: Math.round(s.cogs),
      variable_margin: Math.round(s.variable_margin),
      margin_pct: s.sales_incl > 0 ? Math.round(s.variable_margin / s.sales_incl * 1000) / 10 : null,
      cost_coverage_pct: s.sales_incl > 0 ? Math.round(s.sales_cost_complete / s.sales_incl * 1000) / 10 : null,
      days_with_data: s.days_with_data,
      days_in_period: daysInPeriod,
    };
    // 月タイル: 仕訳書取込済みなら確定値 (広告費/手数料実額) と確定寄せ実質利益を載せる
    if (p.key === 'this_month' || p.key === 'last_month') {
      const c = confirmedMonth(db, p.key === 'this_month' ? ym : lastYm);
      if (c) {
        tile.confirmed = {
          ad_cost: Math.round(c.ad_cost),
          pf_fee: Math.round(c.pf_fee),
          confirmed_at: c.confirmed_at,
          // 実質利益(確定寄せ) = 粗利(速報) + 手数料推定を戻す − 手数料実額 − 広告費実額
          full_margin: Math.round(s.variable_margin + s.mall_fee_est - c.pf_fee - c.ad_cost),
        };
      } else {
        tile.confirmed = null;
      }
    }
    return tile;
  });
  return {
    generated_at: new Date().toISOString(),
    today,
    data_to: fresh.data_to,
    last_synced: fresh.last_synced,
    tiles,
  };
}

// ─── トレンド (日次/週次/月次) ───
export function getTrend(from, to, granularity) {
  const db = getMirrorDB();
  // 粗い自動格上げ: day で半年超はブラウザ描画も重いので week/month に落とす (Codex R1 Medium)
  const spanDays = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  if (granularity === 'day' && spanDays > 190) granularity = 'week';
  if (granularity === 'week' && spanDays > 550) granularity = 'month';
  // week は「その週の月曜日の日付」を bucket にする (%W は年跨ぎ週が分断される — Codex R1 Low)
  const bucketExpr = granularity === 'month' ? `substr(date_jst, 1, 7)`
    : granularity === 'week' ? `date(date_jst, '-' || ((CAST(strftime('%w', date_jst) AS INTEGER) + 6) % 7) || ' days')`
    : `date_jst`;
  const rows = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
      MIN(date_jst) AS bucket_start,
      SUM(gross_sales_jpy_incl)             AS sales_incl,
      SUM(variable_margin_jpy_incl)         AS variable_margin,
      SUM(units_net_sold)                   AS units_net,
      SUM(allocated_units_cancelled)        AS units_cancelled,
      SUM(coupon_shop_jpy_incl)             AS coupon_shop,
      SUM(allocated_refund_amount_jpy_incl) AS refunds,
      SUM(mall_fee_jpy_incl)                AS mall_fee_est
    FROM mirror_rakuten_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to).map(r => ({
    ...r,
    sales_incl: Math.round(r.sales_incl),
    variable_margin: Math.round(r.variable_margin),
    coupon_shop: Math.round(r.coupon_shop),
    refunds: Math.round(r.refunds),
    mall_fee_est: Math.round(r.mall_fee_est),
    margin_pct: r.sales_incl > 0 ? Math.round(r.variable_margin / r.sales_incl * 1000) / 10 : null,
    cancel_rate_pct: (r.units_net + r.units_cancelled) > 0
      ? Math.round(r.units_cancelled / (r.units_net + r.units_cancelled) * 1000) / 10 : null,
  }));

  // 月次粒度のときは確定情報 (仕訳書取込済み月) を重ねられるように返す
  let confirmed = [];
  if (granularity === 'month') {
    confirmed = db.prepare(`
      SELECT year_month, COALESCE(ad_cost,0) AS ad_cost, COALESCE(pf_fee,0) AS pf_fee
      FROM mart_rakuten_monthly_summary
      WHERE confirmed_at IS NOT NULL AND year_month >= ? AND year_month <= ?
      ORDER BY year_month
    `).all(monthOf(from), monthOf(to)).map(c => {
      const row = rows.find(r => r.bucket === c.year_month);
      return {
        year_month: c.year_month,
        ad_cost: Math.round(c.ad_cost),
        pf_fee: Math.round(c.pf_fee),
        full_margin: row ? Math.round(row.variable_margin + row.mall_fee_est - c.pf_fee - c.ad_cost) : null,
      };
    });
  }
  return { from, to, granularity, rows, confirmed_months: confirmed };
}
