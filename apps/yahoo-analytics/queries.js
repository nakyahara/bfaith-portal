/**
 * yahoo-analytics queries.js — データ層 (Render warehouse-mirror.db 読み取り)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/Yahoo統合管理ダッシュボード_要件定義_20260706.md
 *
 * 使用テーブル (P1):
 *   mirror_yahoo_finance_sku_daily  受注ベース日次fact (whitelist: 出荷完了+入金済、税込、
 *                                   variant粒度、snapshot原価、mall_fee は 10% 一律の推定値、
 *                                   units_cancelled は Phase 1a では 0 固定 → キャンセル系は表示しない)
 *   mart_yahoo_monthly_summary      月次確定 (yahoo-accounting の請求明細取込由来:
 *                                   ad_cost=広告費実額 / pf_fee=請求合計(税込)−広告費)
 *
 * 精度ラベル方針 (要件 §11-2、税はすべて税込):
 *   速報   = 受注ベース fact (毎朝 daily-sync で当月 rebuild)
 *   推定   = mall_fee (10%一律) を含む値
 *   確定   = 請求明細取込済み月の実額 (ad_cost / pf_fee)
 *   確定寄せ実質利益 = variable_margin_partial + mall_fee(推定を戻す) − pf_fee(実額) − ad_cost(実額)
 *     ※ pf_fee はポイント原資・キャンペーン原資・決済手数料・プロモーションパッケージ料等の請求額。
 *       fact 側で控除済みのストアクーポン (価格値引・請求外) / ポイント利用 (受取減額・請求外)
 *       とは費目が重ならないため二重計上しない (要件 §2.5 L3)
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

// カスタム期間の上限 (日)。巨大 range での全表スキャン抑止
const MAX_CUSTOM_RANGE_DAYS = 730;

// preset → {from, to} (JST)
export function resolvePeriod(preset, fromQ, toQ) {
  const today = jstToday();
  if (isValidDate(fromQ) && isValidDate(toQ) && fromQ <= toQ) {
    // 両端含みで MAX_CUSTOM_RANGE_DAYS 日に clamp (Codex R1 low)
    const minFrom = addDays(toQ, -(MAX_CUSTOM_RANGE_DAYS - 1));
    return { from: minFrom > fromQ ? minFrom : fromQ, to: toQ, preset: 'custom' };
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
      COALESCE(SUM(gross_sales_jpy_incl),0)              AS sales_incl,
      COALESCE(SUM(units_ordered),0)                     AS units_ordered,
      COALESCE(SUM(units_net_sold),0)                    AS units_net,
      COALESCE(SUM(coupon_shop_jpy_incl),0)              AS coupon_shop,
      COALESCE(SUM(use_point_jpy_incl),0)                AS use_point,
      COALESCE(SUM(mall_fee_jpy_incl),0)                 AS mall_fee_est,
      COALESCE(SUM(shipping_cost_jpy_incl),0)            AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)              AS cogs,
      COALESCE(SUM(variable_margin_partial_jpy_incl),0)  AS variable_margin,
      COALESCE(SUM(CASE WHEN is_cost_complete = 1 THEN gross_sales_jpy_incl END),0) AS sales_cost_complete,
      COALESCE(SUM(CASE WHEN unresolved_sku_flag = 1 THEN gross_sales_jpy_incl END),0) AS sales_unresolved,
      COUNT(DISTINCT date_jst)                           AS days_with_data
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 月次確定 (yahoo-accounting の請求明細取込済み月のみ) ───
// pf_fee > 0 条件: yahoo-accounting の過去月一括取込 (/import-history) 行は pf_fee 未計算 (=0) の
// まま confirmed_at が入るため、それを「確定」扱いすると実質利益が過大表示になる (Codex R1 medium)。
// 実運用で請求合計−広告費が 0 円の月は存在しない (ポイント原資等が必ず発生する) ため 0 は未確定とみなす。
const CONFIRMED_COND = `confirmed_at IS NOT NULL AND COALESCE(pf_fee, 0) > 0`;
function confirmedMonth(db, ym) {
  return db.prepare(`
    SELECT year_month, COALESCE(ad_cost,0) AS ad_cost, COALESCE(pf_fee,0) AS pf_fee, confirmed_at
    FROM mart_yahoo_monthly_summary
    WHERE year_month = ? AND ${CONFIRMED_COND}
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
  const fresh = db.prepare(`
    SELECT MAX(date_jst) AS data_to, MAX(synced_at) AS last_synced
    FROM mirror_yahoo_finance_sku_daily
  `).get();

  const tiles = periods.map(p => {
    const s = factSummary(db, p.from, p.to);
    const daysInPeriod = Math.round((new Date(p.to + 'T00:00:00Z') - new Date(p.from + 'T00:00:00Z')) / 86400000) + 1;
    const tile = {
      ...p,
      sales_incl: Math.round(s.sales_incl),
      units_ordered: s.units_ordered,
      units_net: s.units_net,
      coupon_shop: Math.round(s.coupon_shop),
      use_point: Math.round(s.use_point),
      mall_fee_est: Math.round(s.mall_fee_est),
      shipping: Math.round(s.shipping),
      cogs: Math.round(s.cogs),
      variable_margin: Math.round(s.variable_margin),
      margin_pct: s.sales_incl > 0 ? Math.round(s.variable_margin / s.sales_incl * 1000) / 10 : null,
      cost_coverage_pct: s.sales_incl > 0 ? Math.round(s.sales_cost_complete / s.sales_incl * 1000) / 10 : null,
      days_with_data: s.days_with_data,
      days_in_period: daysInPeriod,
    };
    // 月タイル: 請求明細取込済みなら確定値 (広告費/PF手数料実額) と確定寄せ実質利益を載せる。
    // 確定寄せは「月全体の fact − 月全体の請求」の突合なので、期間が月を完全にカバーする
    // last_month のみ対象 (this_month は月の途中 = 部分 fact に全額請求を引くと過小表示になる。
    // Codex R1 medium)
    if (p.key === 'this_month' || p.key === 'last_month') {
      const c = p.key === 'last_month' ? confirmedMonth(db, lastYm) : null;
      if (c) {
        tile.confirmed = {
          ad_cost: Math.round(c.ad_cost),
          pf_fee: Math.round(c.pf_fee),
          confirmed_at: c.confirmed_at,
          // 実質利益(確定寄せ) = 粗利(速報) + 手数料推定を戻す − PF手数料実額 − 広告費実額
          full_margin: Math.round(s.variable_margin + s.mall_fee_est - c.pf_fee - c.ad_cost),
        };
      } else {
        tile.confirmed = null;
      }
    }
    return tile;
  });

  // SKU 未解決の可視化 (要件 F-4 の思想を前倒し: 未解決 = 原価 0 で粗利が過大に出る)
  const un = db.prepare(`
    SELECT COUNT(DISTINCT yahoo_sku_key) AS sku_count,
           COALESCE(SUM(gross_sales_jpy_incl),0) AS sales_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE unresolved_sku_flag = 1 AND date_jst >= ? AND date_jst <= ?
  `).get(monthStart(ym), today);
  const monthTile = tiles.find(t => t.key === 'this_month');
  const unresolved = {
    sku_count: un.sku_count,
    sales_incl: Math.round(un.sales_incl),
    share_pct: monthTile && monthTile.sales_incl > 0
      ? Math.round(un.sales_incl / monthTile.sales_incl * 1000) / 10 : null,
  };

  return {
    generated_at: new Date().toISOString(),
    today,
    data_to: fresh.data_to,
    last_synced: fresh.last_synced,
    tiles,
    unresolved,
  };
}

// ─── トレンド (日次/週次/月次) ───
export function getTrend(from, to, granularity) {
  const db = getMirrorDB();
  // 半年を超える day 指定は week に自動格上げ (点が潰れて読めない + 行数抑制)
  const spanDays = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  if (granularity === 'day' && spanDays > 183) granularity = 'week';

  // week バケットは「その週の月曜の日付」(-6 days して次の月曜へ進める SQLite idiom)
  const bucketExpr = granularity === 'month' ? `substr(date_jst, 1, 7)`
    : granularity === 'week' ? `date(date_jst, '-6 days', 'weekday 1')`
    : `date_jst`;
  const rows = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
      MIN(date_jst) AS bucket_start,
      SUM(gross_sales_jpy_incl)             AS sales_incl,
      SUM(variable_margin_partial_jpy_incl) AS variable_margin,
      SUM(units_net_sold)                   AS units_net,
      SUM(coupon_shop_jpy_incl)             AS coupon_shop,
      SUM(use_point_jpy_incl)               AS use_point,
      SUM(mall_fee_jpy_incl)                AS mall_fee_est
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to).map(r => ({
    ...r,
    sales_incl: Math.round(r.sales_incl),
    variable_margin: Math.round(r.variable_margin),
    coupon_shop: Math.round(r.coupon_shop),
    use_point: Math.round(r.use_point),
    mall_fee_est: Math.round(r.mall_fee_est),
    margin_pct: r.sales_incl > 0 ? Math.round(r.variable_margin / r.sales_incl * 1000) / 10 : null,
    avg_unit_price: r.units_net > 0 ? Math.round(r.sales_incl / r.units_net) : null,
  }));

  // 月次粒度のときは確定情報 (請求明細取込済み月) を重ねられるように返す。
  // 確定寄せは月全体の fact との突合なので、[from, to] が月初〜月末を完全に含む月のみ対象
  // (部分月に全額請求を引くと full_margin が過小表示になる。Codex R1 medium)
  let confirmed = [];
  if (granularity === 'month') {
    confirmed = db.prepare(`
      SELECT year_month, COALESCE(ad_cost,0) AS ad_cost, COALESCE(pf_fee,0) AS pf_fee
      FROM mart_yahoo_monthly_summary
      WHERE ${CONFIRMED_COND} AND year_month >= ? AND year_month <= ?
      ORDER BY year_month
    `).all(monthOf(from), monthOf(to))
      .filter(c => monthStart(c.year_month) >= from && monthEnd(c.year_month) <= to)
      .map(c => {
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
