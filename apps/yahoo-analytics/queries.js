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

// ─── [from, to] に完全に含まれる暦月の列挙 ───
function completeMonthsInRange(from, to) {
  const months = [];
  let ym = monthOf(from);
  const last = monthOf(to);
  while (ym <= last) {
    if (monthStart(ym) >= from && monthEnd(ym) <= to) months.push(ym);
    ym = addMonths(ym, 1);
  }
  return months;
}

// ─── 利益分析タブ: ウォーターフォール (F-3) ───
// L1 (partial margin 6要素) までは常に返す。SKU 指定なし + 期間が「確定済みの完全月のみ」で
// 構成される場合に限り、確定寄せ (L3 相当: 手数料推定戻し→PF手数料実額→広告費実額) を延長表示。
export function getWaterfall(from, to, sku) {
  const db = getMirrorDB();
  const skuCond = sku ? `AND yahoo_sku_key = ?` : '';
  const params = sku ? [from, to, sku] : [from, to];
  const s = db.prepare(`
    SELECT
      COALESCE(SUM(gross_sales_jpy_incl),0)             AS sales_incl,
      COALESCE(SUM(coupon_shop_jpy_incl),0)             AS coupon_shop,
      COALESCE(SUM(use_point_jpy_incl),0)               AS use_point,
      COALESCE(SUM(mall_fee_jpy_incl),0)                AS mall_fee_est,
      COALESCE(SUM(shipping_cost_jpy_incl),0)           AS shipping,
      COALESCE(SUM(cogs_amount_jpy_incl),0)             AS cogs,
      COALESCE(SUM(variable_margin_partial_jpy_incl),0) AS variable_margin
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ? ${skuCond}
  `).get(...params);

  const steps = [
    { key: 'sales', label: '売上 (税込)', amount: s.sales_incl, kind: 'total', precision: 'flash' },
    { key: 'coupon_shop', label: 'ストアクーポン', amount: s.coupon_shop, kind: 'cost', precision: 'flash' },
    { key: 'use_point', label: 'ポイント利用', amount: s.use_point, kind: 'cost', precision: 'flash' },
    { key: 'mall_fee', label: 'モール手数料', amount: s.mall_fee_est, kind: 'cost', precision: 'est' },
    { key: 'shipping', label: '送料', amount: s.shipping, kind: 'cost', precision: 'est' },
    { key: 'cogs', label: '原価 (snapshot)', amount: s.cogs, kind: 'cost', precision: 'flash' },
    { key: 'variable_margin', label: '粗利 (変動利益 L1)', amount: s.variable_margin, kind: sku ? 'total' : 'subtotal', precision: 'flash' },
  ];

  // 確定寄せ延長 (全体表示のみ)。部分月への全額請求適用を避けるため、
  // 期間 = 確定済み完全月の集合と完全一致する場合に限る (Codex R1 medium と同思想)
  let confirmedApplied = false;
  if (!sku) {
    const months = completeMonthsInRange(from, to);
    const coversWholeRange = months.length > 0
      && monthStart(months[0]) === from && monthEnd(months[months.length - 1]) === to;
    if (coversWholeRange) {
      const confirmed = months.map(ym => confirmedMonth(db, ym));
      if (confirmed.every(c => c !== null)) {
        const adCost = confirmed.reduce((sum, c) => sum + c.ad_cost, 0);
        const pfFee = confirmed.reduce((sum, c) => sum + c.pf_fee, 0);
        steps.push(
          { key: 'mall_fee_reverse', label: '手数料推定を戻す', amount: s.mall_fee_est, kind: 'income', precision: 'est' },
          { key: 'pf_fee', label: 'PF手数料 (請求明細実額)', amount: pfFee, kind: 'cost', precision: 'settled' },
          { key: 'ad_cost', label: '広告費 (請求明細実額)', amount: adCost, kind: 'cost', precision: 'settled' },
          { key: 'full_margin', label: '実質利益 (確定寄せ)', amount: s.variable_margin + s.mall_fee_est - pfFee - adCost, kind: 'total', precision: 'settled' },
        );
        confirmedApplied = true;
      }
    }
  }
  return {
    from, to, sku: sku || null, confirmed_applied: confirmedApplied,
    steps: steps.map(x => ({ ...x, amount: Math.round(x.amount) })),
  };
}

// ─── 利益分析タブ: SKU 別利益テーブル (F-4) ───
// 最新日の ne_code / resolution_method を取るための連結ソートキー。
// ne_code / resolution_method に '|' は現れない前提 (NE コードは英数記号、method は enum)
const LATEST_EXPR = `MAX(date_jst || '|' || COALESCE(ne_code,'') || '|' || resolution_method)`;

export function getSkuProfit(from, to, opts = {}) {
  const db = getMirrorDB();
  let rows = db.prepare(`
    SELECT
      yahoo_sku_key,
      MAX(variant_key)                                   AS variant_key,
      MAX(product_name)                                  AS product_name,
      ${LATEST_EXPR}                                     AS latest_key,
      MAX(unresolved_sku_flag)                           AS ever_unresolved,
      SUM(units_ordered)                                 AS units_ordered,
      SUM(units_net_sold)                                AS units_net,
      SUM(gross_sales_jpy_incl)                          AS sales_incl,
      SUM(coupon_shop_jpy_incl)                          AS coupon_shop,
      SUM(use_point_jpy_incl)                            AS use_point,
      SUM(mall_fee_jpy_incl)                             AS mall_fee_est,
      SUM(shipping_cost_jpy_incl)                        AS shipping,
      SUM(cogs_amount_jpy_incl)                          AS cogs,
      SUM(variable_margin_partial_jpy_incl)              AS variable_margin,
      MIN(is_cost_complete)                              AS all_cost_complete,
      MAX(CASE WHEN cost_status <> 'complete' THEN cost_status END) AS cost_status_sample
    FROM mirror_yahoo_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY yahoo_sku_key
  `).all(from, to).map(r => {
    const parts = (r.latest_key || '||').split('|');
    const neCode = parts[1] || null;
    const resolution = parts[2] || 'unresolved';
    return {
      yahoo_sku_key: r.yahoo_sku_key,
      ne_code: neCode,
      variant_key: r.variant_key || '',
      product_name: r.product_name || '',
      resolution_method: resolution,
      units_net: r.units_net,
      sales_incl: Math.round(r.sales_incl),
      coupon_shop: Math.round(r.coupon_shop),
      use_point: Math.round(r.use_point),
      mall_fee_est: Math.round(r.mall_fee_est),
      shipping: Math.round(r.shipping),
      cogs: Math.round(r.cogs),
      variable_margin: Math.round(r.variable_margin),
      margin_pct: r.sales_incl > 0 ? Math.round(r.variable_margin / r.sales_incl * 1000) / 10 : null,
      cost_status: r.all_cost_complete === 1 ? 'complete' : (r.cost_status_sample || 'missing_cost'),
      // 色分け: 期間内に1日でも未解決 (=原価0円計上) があれば警告 (最新日だけ解決済みでも
      // 期間集計の粗利には過大分が混ざる — Codex P2-R1 High) / 赤字
      flag: r.ever_unresolved === 1 ? 'unresolved' : r.variable_margin < 0 ? 'loss' : 'ok',
    };
  });

  const q = (opts.q || '').trim().toLowerCase();
  if (q) {
    rows = rows.filter(r =>
      r.yahoo_sku_key.toLowerCase().includes(q)
      || (r.ne_code || '').toLowerCase().includes(q)
      || r.product_name.toLowerCase().includes(q));
  }

  const sortKey = ['sales_incl', 'units_net', 'coupon_shop', 'use_point', 'cogs', 'variable_margin', 'margin_pct', 'yahoo_sku_key'].includes(opts.sort) ? opts.sort : 'variable_margin';
  const dir = opts.dir === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    const av = a[sortKey], bv = b[sortKey];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return av < bv ? -dir : av > bv ? dir : 0;
  });

  const total = rows.length;
  const limit = Math.max(1, Math.min(Number(opts.limit) || 100, 20000));
  const offset = Math.max(Number(opts.offset) || 0, 0);
  return { from, to, total, rows: rows.slice(offset, offset + limit) };
}

// ─── SKU 詳細 (ドリルダウン: 日次トレンド + マスタ情報) ───
export function getSkuDetail(sku, from, to) {
  const db = getMirrorDB();
  const daily = db.prepare(`
    SELECT date_jst, units_net_sold AS units_net,
      gross_sales_jpy_incl AS sales_incl,
      coupon_shop_jpy_incl AS coupon_shop, use_point_jpy_incl AS use_point,
      mall_fee_jpy_incl AS mall_fee_est, shipping_cost_jpy_incl AS shipping,
      cogs_amount_jpy_incl AS cogs, variable_margin_partial_jpy_incl AS variable_margin,
      resolution_method, cost_status, unit_cost_snapshot_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE yahoo_sku_key = ? AND date_jst >= ? AND date_jst <= ?
    ORDER BY date_jst
  `).all(sku, from, to).map(r => ({
    ...r,
    sales_incl: Math.round(r.sales_incl),
    variable_margin: Math.round(r.variable_margin),
  }));

  const latest = db.prepare(`
    SELECT ne_code, variant_key, product_name, resolution_method, unit_cost_snapshot_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE yahoo_sku_key = ?
    ORDER BY date_jst DESC LIMIT 1
  `).get(sku) || null;

  let master = null;
  if (latest?.ne_code) {
    master = db.prepare(`
      SELECT 商品コード AS ne_code, 商品名 AS product_name, 原価 AS unit_cost, 標準売価 AS list_price, 消費税率 AS tax_rate
      FROM mirror_products WHERE 商品コード = ?
    `).get(latest.ne_code) || null;
  }
  return { sku, from, to, latest, master, daily };
}

// ─── SKU 未解決一覧 (F-4: f_yahoo_sku_map 登録の解消導線) ───
// 未解決 = 原価 0 円計上で粗利が過大に出る。miniPC の f_yahoo_sku_map へ手動登録すると
// 翌朝の fact rebuild (当月分) から解決される。過去月は再 build 時に反映。
//
// 登録キーについて (build SQL sku_resolved CTE で裏取り済み — Codex P2-R1 medium):
//   yahoo_sku_key = sub_code (variant あり) or item_id (variant なし) がそのまま入っており、
//   f_yahoo_sku_map の lookup も同じ値 (sub_code 優先 → item_id) で行われる。
//   したがって f_yahoo_sku_map.yahoo_key に登録すべき値は常に yahoo_sku_key そのもの。
//   item_id は fact に保存されていないため復元しない (variant_key = sub_code 全体)。
export function getUnresolved(daysBack = 180) {
  const db = getMirrorDB();
  const from = addDays(jstToday(), -Math.min(Math.max(Number(daysBack) || 180, 1), 730));
  const rows = db.prepare(`
    SELECT
      yahoo_sku_key,
      MAX(variant_key)              AS variant_key,
      MAX(product_name)             AS product_name,
      MIN(date_jst)                 AS first_seen,
      MAX(date_jst)                 AS last_seen,
      SUM(units_net_sold)           AS units_net,
      SUM(gross_sales_jpy_incl)     AS sales_incl
    FROM mirror_yahoo_finance_sku_daily
    WHERE unresolved_sku_flag = 1 AND date_jst >= ?
    GROUP BY yahoo_sku_key
    ORDER BY sales_incl DESC
    LIMIT 500
  `).all(from);

  // m_products の前方一致候補 (完全一致は build 時に試行済みなので LIKE で緩める)。
  // キー全体 + 末尾セグメント ('-'以降) を落としたヒューリスティック prefix の2通りで探す
  const candidateStmt = db.prepare(`
    SELECT 商品コード AS ne_code, 商品名 AS product_name
    FROM mirror_products
    WHERE LOWER(商品コード) LIKE LOWER(?) || '%'
    ORDER BY LENGTH(商品コード) LIMIT 3
  `);
  const out = rows.map(r => {
    const prefixes = [r.yahoo_sku_key];
    // sub_code の末尾セグメント違いで NE 未登録のケースが多いため、variant ありの時のみ
    // 末尾 '-xxx' を落とした prefix も試す (variant なしの短い item_id では誤ヒットが増えるだけ)
    const lastDash = r.yahoo_sku_key.lastIndexOf('-');
    if (r.variant_key && lastDash > 0) prefixes.push(r.yahoo_sku_key.slice(0, lastDash));
    const seen = new Set();
    const candidates = [];
    for (const p of prefixes) {
      let found = [];
      try { found = candidateStmt.all(p); } catch { /* mirror_products 無しでも一覧は返す */ }
      for (const c of found) {
        if (!seen.has(c.ne_code) && candidates.length < 3) { seen.add(c.ne_code); candidates.push(c); }
      }
    }
    return {
      yahoo_sku_key: r.yahoo_sku_key,           // = f_yahoo_sku_map.yahoo_key に登録する値
      variant_key: r.variant_key || '',          // = sub_code (variant なしは空)
      product_name: r.product_name || '',
      first_seen: r.first_seen,
      last_seen: r.last_seen,
      units_net: r.units_net,
      sales_incl: Math.round(r.sales_incl),
      candidates,
    };
  });
  return { from, days_back: daysBack, total: out.length, rows: out };
}
