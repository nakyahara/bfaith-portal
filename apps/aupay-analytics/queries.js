/**
 * aupay-analytics queries.js — データ層 (Render warehouse-mirror.db 読み取り)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/auPAY統合管理ダッシュボード_要件定義_20260706.md
 *
 * 使用テーブル (P1):
 *   mirror_aupay_finance_sku_daily  受注ベース日次fact (whitelist: orderStatus='完了'+明細キャンセルなし、
 *                                   税込、variant粒度 (itemManagementId連結)、snapshot原価、
 *                                   mall_fee は成約手数料13%設定の推計 (決済手数料コミコミプラン)、
 *                                   units_cancelled は Phase A では 0 固定 → キャンセル系は表示しない)
 *   mirror_f_sales_by_listing       速報売上 (NE受注ベース、mall='aupay')。「完了」ステータス待ちの
 *                                   fact では今日/昨日が空になるため、タイルの速報売上はこちらが正
 *
 * 利益概念 (要件 §2.5、税はすべて税込):
 *   L1 = variable_margin_partial (売上(送料按分込) − 原価snapshot − 手数料13%推定 − 送料 − ストアクーポン)
 *   L2 = L1 − ポイント原資 (point_cost_pending_jpy_incl = 付与ポイント gift_point + プレミアム会員
 *        ショップ負担分。au API が明細単位で返す実額。SKU別ポイント原資が API で取れるのは
 *        4モール中 au PAY だけ — 本アプリの独自価値。Codex R1 medium: gift_point 単独でなく
 *        pending 集約列を使う — premium 分の取りこぼし防止)
 *   広告費は現在未出稿のため L2 に含めない (§13-②。出稿開始時に C-1 CSV 取込を有効化して控除を追加)
 *   L3 (月次確定) は P4 の請求明細取込で実装予定
 *
 * 精度ラベル方針:
 *   速報      = mirror_f_sales_by_listing (NE受注、全ステータス)
 *   自動(日次) = fact (完了ベース、毎朝 daily-sync で当月+前2ヶ月 rebuild → 直近数日は未計上が正常)
 *   実額(API)  = 付与ポイント (giftPoint)
 *   推定13%   = mall_fee (config/aupay_mall_fee_rates.json 由来、P4 の請求明細実額で毎月較正予定)
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
// 三太郎の日 = 毎月 3・13・23 日 (au PAY マーケット最大の月例イベント。要件 F-2-3)
export function isSantaroDay(dateStr) {
  return ['03', '13', '23'].includes(dateStr.slice(8, 10));
}

// カスタム期間の上限 (日)。巨大 range での全表スキャン抑止
const MAX_CUSTOM_RANGE_DAYS = 730;

// preset → {from, to} (JST)
export function resolvePeriod(preset, fromQ, toQ) {
  const today = jstToday();
  if (isValidDate(fromQ) && isValidDate(toQ) && fromQ <= toQ) {
    // 両端含みで MAX_CUSTOM_RANGE_DAYS 日に clamp
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
      COALESCE(SUM(point_cost_pending_jpy_incl),0)       AS point_cost,
      COALESCE(SUM(mall_fee_jpy_incl),0)                 AS mall_fee_est,
      COALESCE(SUM(shipping_cost_jpy_incl),0)            AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)              AS cogs,
      COALESCE(SUM(variable_margin_partial_jpy_incl),0)  AS variable_margin,
      COALESCE(SUM(CASE WHEN is_cost_complete = 1 THEN gross_sales_jpy_incl END),0) AS sales_cost_complete,
      COALESCE(SUM(CASE WHEN mall_fee_calc_method = 'unknown' THEN gross_sales_jpy_incl END),0) AS sales_fee_unknown,
      COUNT(DISTINCT date_jst)                           AS days_with_data
    FROM mirror_aupay_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 速報 (NE受注ベース、mall='aupay') ───
function listingSummary(db, from, to) {
  return db.prepare(`
    SELECT
      COALESCE(SUM(sales_jpy_incl),0) AS sales_incl,
      COALESCE(SUM(units),0)          AS units,
      COALESCE(SUM(order_count),0)    AS order_count
    FROM mirror_f_sales_by_listing
    WHERE mall = 'aupay' AND date_jst >= ? AND date_jst <= ?
  `).get(from, to);
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

  // データ鮮度: fact は「完了」注文のみ (出荷完了までのラグ数日) + 毎朝 07:00 build。
  // 速報 (NE) は当日分も入るため、今日/昨日タイルの売上は速報が正
  const freshFact = db.prepare(`
    SELECT MAX(date_jst) AS data_to, MAX(synced_at) AS last_synced
    FROM mirror_aupay_finance_sku_daily
  `).get();
  const freshListing = db.prepare(`
    SELECT MAX(date_jst) AS data_to FROM mirror_f_sales_by_listing WHERE mall = 'aupay'
  `).get();

  const tiles = periods.map(p => {
    const s = factSummary(db, p.from, p.to);
    const flash = listingSummary(db, p.from, p.to);
    const l2 = s.variable_margin - s.point_cost;
    const daysInPeriod = Math.round((new Date(p.to + 'T00:00:00Z') - new Date(p.from + 'T00:00:00Z')) / 86400000) + 1;
    return {
      ...p,
      flash_sales_incl: Math.round(flash.sales_incl),
      flash_units: flash.units,
      flash_orders: flash.order_count,
      sales_incl: Math.round(s.sales_incl),
      units_ordered: s.units_ordered,
      units_net: s.units_net,
      coupon_shop: Math.round(s.coupon_shop),
      mall_fee_est: Math.round(s.mall_fee_est),
      shipping: Math.round(s.shipping),
      cogs: Math.round(s.cogs),
      variable_margin: Math.round(s.variable_margin),
      point_cost: Math.round(s.point_cost),
      l2_margin: Math.round(l2),
      margin_pct: s.sales_incl > 0 ? Math.round(s.variable_margin / s.sales_incl * 1000) / 10 : null,
      l2_pct: s.sales_incl > 0 ? Math.round(l2 / s.sales_incl * 1000) / 10 : null,
      shipping_pct: s.sales_incl > 0 ? Math.round(s.shipping / s.sales_incl * 1000) / 10 : null,
      cost_coverage_pct: s.sales_incl > 0 ? Math.round(s.sales_cost_complete / s.sales_incl * 1000) / 10 : null,
      // 手数料が率未設定 (calc_method='unknown') のまま 0 円で合算されている売上の割合。
      // >0 なら L1/L2 が過大 (Codex R1 medium: COALESCE で隠さず可視化する)
      fee_unknown_pct: s.sales_incl > 0 ? Math.round(s.sales_fee_unknown / s.sales_incl * 1000) / 10 : null,
      days_with_data: s.days_with_data,
      days_in_period: daysInPeriod,
    };
  });

  // SKU 未解決の可視化 (未解決 = 原価 0 で粗利が過大に出る。残存は multipack 系 ~30 SKU 想定)
  const un = db.prepare(`
    SELECT COUNT(DISTINCT aupay_sku_key) AS sku_count,
           COALESCE(SUM(gross_sales_jpy_incl),0) AS sales_incl
    FROM mirror_aupay_finance_sku_daily
    WHERE unresolved_sku_flag = 1 AND date_jst >= ? AND date_jst <= ?
  `).get(monthStart(ym), today);
  const monthTile = tiles.find(t => t.key === 'this_month');
  const unresolved = {
    sku_count: un.sku_count,
    sales_incl: Math.round(un.sales_incl),
    share_pct: monthTile && monthTile.sales_incl > 0
      ? Math.round(un.sales_incl / monthTile.sales_incl * 1000) / 10 : null,
  };

  // 三太郎の日効果 (直近90日): 三太郎日の日次売上中央値 ÷ 平常日の日次売上中央値 (fact ベース)。
  // fact に行がない日は売上 0 としてカレンダー 0 埋めする (Codex R1 low: 欠損日を母集団から
  // 落とすと中央値が上振れする)。窓の終端は data_to (未同期の未来日を 0 と誤計上しない)
  const santaroFrom = addDays(today, -89);
  const santaroTo = freshFact.data_to && freshFact.data_to < today ? freshFact.data_to : today;
  const dailyRows = db.prepare(`
    SELECT date_jst, SUM(gross_sales_jpy_incl) AS sales
    FROM mirror_aupay_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY date_jst
  `).all(santaroFrom, santaroTo);
  const salesByDate = new Map(dailyRows.map(r => [r.date_jst, r.sales]));
  const daily = [];
  if (santaroFrom <= santaroTo) {
    for (let dt = santaroFrom; dt <= santaroTo; dt = addDays(dt, 1)) {
      daily.push({ date_jst: dt, sales: salesByDate.get(dt) || 0 });
    }
  }
  const median = (arr) => {
    if (!arr.length) return null;
    const s = [...arr].sort((a, b) => a - b);
    const mid = s.length >> 1;
    return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
  };
  const santaroSales = daily.filter(r => isSantaroDay(r.date_jst)).map(r => r.sales);
  const normalSales = daily.filter(r => !isSantaroDay(r.date_jst)).map(r => r.sales);
  const mSantaro = median(santaroSales);
  const mNormal = median(normalSales);
  const santaro = {
    days_santaro: santaroSales.length,
    days_normal: normalSales.length,
    median_santaro: mSantaro === null ? null : Math.round(mSantaro),
    median_normal: mNormal === null ? null : Math.round(mNormal),
    lift_ratio: (mSantaro !== null && mNormal > 0) ? Math.round(mSantaro / mNormal * 100) / 100 : null,
  };

  return {
    generated_at: new Date().toISOString(),
    today,
    data_to: freshFact.data_to,
    flash_data_to: freshListing.data_to,
    last_synced: freshFact.last_synced,
    tiles,
    unresolved,
    santaro,
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
      SUM(point_cost_pending_jpy_incl)      AS point_cost,
      COALESCE(SUM(mall_fee_jpy_incl),0)    AS mall_fee_est,
      SUM(shipping_cost_jpy_incl)           AS shipping,
      SUM(cogs_amount_jpy_incl)             AS cogs
    FROM mirror_aupay_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to).map(r => {
    const l2 = r.variable_margin - r.point_cost;
    return {
      ...r,
      sales_incl: Math.round(r.sales_incl),
      variable_margin: Math.round(r.variable_margin),
      coupon_shop: Math.round(r.coupon_shop),
      point_cost: Math.round(r.point_cost),
      mall_fee_est: Math.round(r.mall_fee_est),
      shipping: Math.round(r.shipping),
      cogs: Math.round(r.cogs),
      l2_margin: Math.round(l2),
      margin_pct: r.sales_incl > 0 ? Math.round(r.variable_margin / r.sales_incl * 1000) / 10 : null,
      l2_pct: r.sales_incl > 0 ? Math.round(l2 / r.sales_incl * 1000) / 10 : null,
      shipping_pct: r.sales_incl > 0 ? Math.round(r.shipping / r.sales_incl * 1000) / 10 : null,
      avg_unit_price: r.units_net > 0 ? Math.round(r.sales_incl / r.units_net) : null,
      // 三太郎ハイライトは日次バケットのみ意味を持つ (週次/月次は false 固定)
      is_santaro: granularity === 'day' ? isSantaroDay(r.bucket) : false,
    };
  });
  return { from, to, granularity, rows };
}
