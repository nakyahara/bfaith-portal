/**
 * qoo10-analytics queries.js — データ層 (Render warehouse-mirror.db 読み取り)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/Qoo10統合管理ダッシュボード_要件定義_20260706.md
 *
 * 使用テーブル (P1):
 *   mirror_qoo10_finance_sku_daily  確定fact (配送完了 Delivered(5) のみ計上、税込、日次×SKU。
 *                                   手数料は API 実額 (platform_fee = GMV − SettlePrice)、
 *                                   送料は m_products 想定値、原価 snapshot。
 *                                   ⚠️ 配送完了までのラグで直近数日は必ず小さく出る → 速報と併記)
 *   mirror_f_sales_by_listing       速報売上 (NE受注ベース、税込、mall='qoo10'。キャンセル除外、
 *                                   biz-ops-overview と同じ層。毎朝 daily-sync で同期)
 *
 * 精度ラベル方針 (要件 §11-2、税はすべて税込):
 *   速報       = NE受注ベース (mirror_f_sales_by_listing)
 *   自動(日次) = 確定fact (配送完了ベース、手数料実額)
 *   推定       = メガ割/メガポ等のセラー負担 (プラットフォーム割引全体 × 50%)
 *
 * メガ割セラー負担の扱い (要件 §2.5):
 *   SettlePrice は割引前価格基準のため、メガ割 20% 割引のセラー負担分 (10%) は fact の
 *   variable_margin (L1) に入っていない (精算時に「販売関連差引額」で先行差引される)。
 *   → L1 と併せて「セラー負担(推定) = total_platform_promo × SELLER_BURDEN_RATE」と
 *     「L2参考 = L1 − セラー負担推定」を常時表示する。精算Excel取込 (P4) で実額に置換予定。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

// メガ割/メガポのセラー負担率。公式仕様は「20%割引のうちセラー10%+Qoo10 10%」= 割引全体の 50%。
// メガポの負担率は要実機確認 (要件 §13 残confirm-2) — 精算内訳取込 (P4) で較正するまでの仮置き。
export const SELLER_BURDEN_RATE = 0.5;

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

// ─── 確定fact の期間集計 (配送完了ベース、税込) ───
function factSummary(db, from, to) {
  return db.prepare(`
    SELECT
      COALESCE(SUM(gmv_list_price_jpy_incl),0)       AS gmv,
      COALESCE(SUM(customer_paid_jpy_incl),0)        AS customer_paid,
      COALESCE(SUM(net_settlement_api_jpy_incl),0)   AS net_settlement,
      COALESCE(SUM(platform_fee_jpy_incl),0)         AS platform_fee,
      COALESCE(SUM(shipping_cost_jpy_incl),0)        AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)          AS cogs,
      COALESCE(SUM(variable_margin_jpy_incl),0)      AS variable_margin,
      COALESCE(SUM(units_ordered),0)                 AS units_ordered,
      COALESCE(SUM(units_net_sold),0)                AS units_net,
      COALESCE(SUM(order_count),0)                   AS orders,
      COALESCE(SUM(megawari_discount_amount_jpy_incl),0) AS megawari_discount,
      COALESCE(SUM(megawari_order_count),0)          AS megawari_orders,
      COALESCE(SUM(megapo_discount_amount_jpy_incl),0)   AS megapo_discount,
      COALESCE(SUM(other_promo_discount_jpy_incl),0) AS other_promo_discount,
      COALESCE(SUM(total_platform_promo_jpy_incl),0) AS promo_total,
      COALESCE(SUM(CASE WHEN is_cost_complete = 1 THEN gmv_list_price_jpy_incl END),0) AS gmv_cost_complete,
      COALESCE(SUM(CASE WHEN match_tier = 'unresolved' THEN gmv_list_price_jpy_incl END),0) AS gmv_unresolved,
      COUNT(DISTINCT date_jst)                       AS days_with_data
    FROM mirror_qoo10_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 速報売上 (NE受注ベース、mall='qoo10'、税込) ───
function flashSummary(db, from, to) {
  return db.prepare(`
    SELECT COALESCE(SUM(sales_jpy_incl),0) AS sales_incl,
           COALESCE(SUM(units),0)          AS units,
           COALESCE(SUM(order_count),0)    AS orders
    FROM mirror_f_sales_by_listing
    WHERE mall = 'qoo10' AND date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// fact 集計行 → API 向けタイル/バケット値 (丸め + 派生指標)
function shapeFact(s) {
  const burdenEst = s.promo_total * SELLER_BURDEN_RATE;
  return {
    gmv: Math.round(s.gmv),
    customer_paid: Math.round(s.customer_paid),
    net_settlement: Math.round(s.net_settlement),
    platform_fee: Math.round(s.platform_fee),
    // 手数料率(実測) = platform_fee ÷ GMV (要件 §10。9%/10% SKU 混在の実測値)
    fee_pct: s.gmv > 0 ? Math.round(s.platform_fee / s.gmv * 1000) / 10 : null,
    shipping: Math.round(s.shipping),
    cogs: Math.round(s.cogs),
    variable_margin: Math.round(s.variable_margin),
    // 粗利率 L1 = variable_margin ÷ ショップ受取 (要件 §10 の正本定義)
    margin_pct: s.net_settlement > 0 ? Math.round(s.variable_margin / s.net_settlement * 1000) / 10 : null,
    units_net: s.units_net,
    orders: s.orders,
    megawari_discount: Math.round(s.megawari_discount),
    megawari_orders: s.megawari_orders,
    megapo_discount: Math.round(s.megapo_discount),
    other_promo_discount: Math.round(s.other_promo_discount),
    promo_total: Math.round(s.promo_total),
    burden_est: Math.round(burdenEst),
    // L2参考 = L1 − セラー負担推定 (広告費は P5 取込後に加わる)
    l2_ref: Math.round(s.variable_margin - burdenEst),
    cost_coverage_pct: s.gmv > 0 ? Math.round(s.gmv_cost_complete / s.gmv * 1000) / 10 : null,
    days_with_data: s.days_with_data,
  };
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

  // データ鮮度 (fact = 配送完了ベースで数日ラグ / flash = NE受注ベースで前日まで)
  const factFresh = db.prepare(`
    SELECT MAX(date_jst) AS data_to, MAX(synced_at) AS last_synced
    FROM mirror_qoo10_finance_sku_daily
  `).get();
  const flashFresh = db.prepare(`
    SELECT MAX(date_jst) AS data_to FROM mirror_f_sales_by_listing WHERE mall = 'qoo10'
  `).get();

  const tiles = periods.map(p => {
    const fact = shapeFact(factSummary(db, p.from, p.to));
    const flash = flashSummary(db, p.from, p.to);
    const flashSales = Math.round(flash.sales_incl);
    return {
      key: p.key, label: p.label, from: p.from, to: p.to,
      flash: { sales_incl: flashSales, units: flash.units, orders: flash.orders },
      fact,
      // 確定待ち目安 = 速報売上 − 確定済み顧客支払額。基準が違う (NE受注 vs 配送完了×割引後)
      // ため厳密な差ではなく「まだ配送完了していない分のめやす」。負なら 0 に clamp。
      pending_est: Math.max(0, flashSales - fact.customer_paid),
    };
  });

  // SKU 未解決の可視化 (今月)。3段fallback 後は通常 ~0% (要件 付録A)。0 でも件数を返す
  const un = db.prepare(`
    SELECT COUNT(DISTINCT sku_code) AS sku_count,
           COALESCE(SUM(gmv_list_price_jpy_incl),0) AS gmv
    FROM mirror_qoo10_finance_sku_daily
    WHERE match_tier = 'unresolved' AND date_jst >= ? AND date_jst <= ?
  `).get(monthStart(ym), today);
  const monthTile = tiles.find(t => t.key === 'this_month');
  const unresolved = {
    sku_count: un.sku_count,
    gmv: Math.round(un.gmv),
    share_pct: monthTile && monthTile.fact.gmv > 0
      ? Math.round(un.gmv / monthTile.fact.gmv * 1000) / 10 : null,
  };

  return {
    generated_at: new Date().toISOString(),
    today,
    fact_data_to: factFresh.data_to,
    flash_data_to: flashFresh.data_to,
    last_synced: factFresh.last_synced,
    seller_burden_rate: SELLER_BURDEN_RATE,
    tiles,
    unresolved,
  };
}

// ─── トレンド (日次/週次/月次)。fact + flash を同一バケットで返す ───
export function getTrend(from, to, granularity) {
  const db = getMirrorDB();
  // 半年を超える day 指定は week に自動格上げ (点が潰れて読めない + 行数抑制)
  const spanDays = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  if (granularity === 'day' && spanDays > 183) granularity = 'week';

  // week バケットは「その週の月曜の日付」(-6 days して次の月曜へ進める SQLite idiom)
  const bucketExpr = granularity === 'month' ? `substr(date_jst, 1, 7)`
    : granularity === 'week' ? `date(date_jst, '-6 days', 'weekday 1')`
    : `date_jst`;

  const factRows = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
      SUM(gmv_list_price_jpy_incl)       AS gmv,
      SUM(customer_paid_jpy_incl)        AS customer_paid,
      SUM(net_settlement_api_jpy_incl)   AS net_settlement,
      SUM(platform_fee_jpy_incl)         AS platform_fee,
      SUM(variable_margin_jpy_incl)      AS variable_margin,
      SUM(units_net_sold)                AS units_net,
      SUM(megawari_discount_amount_jpy_incl) AS megawari_discount,
      SUM(megawari_order_count)          AS megawari_orders,
      SUM(megapo_discount_amount_jpy_incl)   AS megapo_discount,
      SUM(other_promo_discount_jpy_incl) AS other_promo_discount,
      SUM(total_platform_promo_jpy_incl) AS promo_total
    FROM mirror_qoo10_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to);

  const flashRows = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
      SUM(sales_jpy_incl) AS sales_incl,
      SUM(units)          AS units
    FROM mirror_f_sales_by_listing
    WHERE mall = 'qoo10' AND date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to);

  // bucket 合流 (fact/flash どちらか一方にしかない bucket も落とさない)
  const byBucket = new Map();
  for (const r of factRows) byBucket.set(r.bucket, { bucket: r.bucket, fact: r, flash: null });
  for (const r of flashRows) {
    if (byBucket.has(r.bucket)) byBucket.get(r.bucket).flash = r;
    else byBucket.set(r.bucket, { bucket: r.bucket, fact: null, flash: r });
  }
  const rows = [...byBucket.values()].sort((a, b) => a.bucket < b.bucket ? -1 : 1).map(({ bucket, fact, flash }) => {
    const gmv = fact ? fact.gmv : 0;
    const vm = fact ? fact.variable_margin : 0;
    const settlement = fact ? fact.net_settlement : 0;
    const promoTotal = fact ? fact.promo_total : 0;
    return {
      bucket,
      gmv: Math.round(gmv),
      flash_sales: flash ? Math.round(flash.sales_incl) : 0,
      flash_units: flash ? flash.units : 0,
      variable_margin: Math.round(vm),
      margin_pct: settlement > 0 ? Math.round(vm / settlement * 1000) / 10 : null,
      units_net: fact ? fact.units_net : 0,
      avg_unit_price: fact && fact.units_net > 0 ? Math.round(fact.customer_paid / fact.units_net) : null,
      platform_fee: fact ? Math.round(fact.platform_fee) : 0,
      megawari_discount: fact ? Math.round(fact.megawari_discount) : 0,
      megapo_discount: fact ? Math.round(fact.megapo_discount) : 0,
      other_promo_discount: fact ? Math.round(fact.other_promo_discount) : 0,
      burden_est: Math.round(promoTotal * SELLER_BURDEN_RATE),
      // メガ割開催バケット判定 (fact 実データ由来。開催回マスタは P3 で導入予定)
      is_megawari: !!(fact && fact.megawari_orders > 0),
    };
  });

  return { from, to, granularity, rows, seller_burden_rate: SELLER_BURDEN_RATE };
}
