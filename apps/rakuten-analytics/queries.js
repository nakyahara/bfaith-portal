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

// ─── 広告費 日次 (mirror_rakuten_ads_rpp_daily = mall-csv-fetcher 自動取得、店舗全体) ───
function adCostDaily(db, from, to) {
  try {
    return db.prepare(`
      SELECT COALESCE(SUM(ad_cost_yen),0) AS ad_cost, COALESCE(SUM(sales_720h_yen),0) AS ad_sales,
             COALESCE(SUM(clicks),0) AS clicks, MAX(date_jst) AS data_to
      FROM mirror_rakuten_ads_rpp_daily WHERE date_jst >= ? AND date_jst <= ?
    `).get(from, to);
  } catch {
    return { ad_cost: 0, ad_sales: 0, clicks: 0, data_to: null };  // mirror 初期化前
  }
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
    const ads = adCostDaily(db, p.from, p.to);
    const daysInPeriod = Math.round((new Date(p.to + 'T00:00:00Z') - new Date(p.from + 'T00:00:00Z')) / 86400000) + 1;
    const tile = {
      ...p,
      ad_cost: Math.round(ads.ad_cost),
      // 広告後利益 (速報) = 変動利益 − RPP広告費実績。料率推定コスト控除は P4 (料率マスタ) で追加
      margin_after_ads: Math.round(s.variable_margin - ads.ad_cost),
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

// ============================================================
// P3: 広告×利益 (mirror_rakuten_ads_rpp = SKU×月次、mall-csv-fetcher 自動取得)
// ============================================================

// SKU×月の広告効率テーブル + 損益分岐ROAS判定。
// 損益分岐ROAS(%) = 100 ÷ 変動利益率。実ROAS(720h) がこれを下回ると広告で変動利益を食っている。
// 突合: ads.item_manage_number ↔ finance.rakuten_code (LOWER 突合 — feedback_sku_case_normalization)
export function getAdsAnalysis(monthYm) {
  const db = getMirrorDB();
  let months = [];
  try {
    months = db.prepare(`SELECT DISTINCT month_ym FROM mirror_rakuten_ads_rpp ORDER BY month_ym DESC`).all().map(r => r.month_ym);
  } catch { /* mirror 初期化前 */ }
  if (months.length === 0) return { months: [], month: null, skus: [], summary: null, daily: [], match: null };
  const ym = months.includes(monthYm) ? monthYm : months[0];

  const skus = db.prepare(`
    WITH fin AS (
      SELECT LOWER(rakuten_code) AS sku,
             SUM(gross_sales_jpy_incl) AS gross, SUM(variable_margin_jpy_incl) AS vm,
             SUM(units_net_sold) AS units, MAX(product_name) AS name
      FROM mirror_rakuten_finance_sku_daily
      WHERE substr(date_jst,1,7) = ?
      GROUP BY LOWER(rakuten_code)
    )
    SELECT a.item_manage_number AS sku, a.clicks, a.ad_cost_yen AS ad_cost,
           a.sales_720h_yen AS ad_sales, a.orders_720h AS ad_orders,
           a.cvr_720h_pct, a.roas_720h_pct, a.ctr_pct, a.cpc_actual,
           a.sales_720h_new_yen AS ad_sales_new, a.sales_720h_repeat_yen AS ad_sales_repeat,
           f.gross, f.vm, f.units, f.name
    FROM mirror_rakuten_ads_rpp a
    LEFT JOIN fin f ON f.sku = LOWER(a.item_manage_number)
    WHERE a.month_ym = ?
    ORDER BY a.ad_cost_yen DESC
  `).all(ym, ym).map(r => {
    const hasFact = r.gross !== null && r.gross !== undefined && r.gross > 0;
    const vmRate = hasFact && r.vm > 0 ? r.vm / r.gross : null;
    const beRoas = vmRate ? Math.round(100 / vmRate * 10) / 10 : null;
    const roas = r.roas_720h_pct;
    let verdict = 'no_baseline';
    if (hasFact && r.vm <= 0) {
      verdict = 'bleed_hard';   // 変動利益ゼロ以下に出稿 = 広告費全額損失 (rules.js の判定と整合)
    } else if (beRoas !== null && roas !== null) {
      verdict = roas < beRoas * 0.5 ? 'bleed_hard' : roas < beRoas ? 'bleed' : roas >= beRoas * 1.5 ? 'expand' : 'ok';
    }
    const adShare = r.gross > 0 ? Math.round(r.ad_sales / r.gross * 1000) / 10 : null;
    return {
      sku: r.sku, name: r.name || '',
      clicks: r.clicks, ad_cost: r.ad_cost, ad_sales: r.ad_sales, ad_orders: r.ad_orders,
      cvr_pct: r.cvr_720h_pct, roas_pct: roas, ctr_pct: r.ctr_pct, cpc: r.cpc_actual,
      ad_sales_new: r.ad_sales_new, ad_sales_repeat: r.ad_sales_repeat,
      gross: r.gross === null || r.gross === undefined ? null : Math.round(r.gross),
      vm: r.vm === null || r.vm === undefined ? null : Math.round(r.vm),
      vm_rate_pct: vmRate === null ? null : Math.round(vmRate * 1000) / 10,
      breakeven_roas_pct: beRoas,
      ad_share_pct: adShare,
      est_ad_profit: vmRate !== null ? Math.round(r.ad_sales * vmRate - r.ad_cost)
        : (hasFact && r.vm <= 0 ? -Math.round(r.ad_cost) : null),
      verdict,
    };
  });

  const totalAd = skus.reduce((s, r) => s + r.ad_cost, 0);
  // fact突合 (gross が取れた) と 損益分岐判定可能 (変動利益率>0) は別概念 (Codex R3 Medium)
  const factMatchedAd = skus.filter(r => r.gross !== null).reduce((s, r) => s + r.ad_cost, 0);
  const eligibleAd = skus.filter(r => r.verdict !== 'no_baseline').reduce((s, r) => s + r.ad_cost, 0);
  const summary = {
    month: ym,
    total_ad_cost: totalAd,
    total_ad_sales: skus.reduce((s, r) => s + r.ad_sales, 0),
    sku_count: skus.length,
    verdict_counts: skus.reduce((m, r) => { m[r.verdict] = (m[r.verdict] || 0) + 1; return m; }, {}),
    est_ad_profit_total: skus.reduce((s, r) => s + (r.est_ad_profit || 0), 0),
  };
  const match = {
    matched_ad_cost_pct: totalAd > 0 ? Math.round(factMatchedAd / totalAd * 1000) / 10 : null,
    breakeven_eligible_pct: totalAd > 0 ? Math.round(eligibleAd / totalAd * 1000) / 10 : null,
  };

  let daily = [];
  try {
    daily = db.prepare(`
      SELECT date_jst, SUM(ad_cost_yen) AS ad_cost, SUM(sales_720h_yen) AS ad_sales, SUM(clicks) AS clicks
      FROM mirror_rakuten_ads_rpp_daily
      WHERE date_jst >= ?
      GROUP BY date_jst ORDER BY date_jst
    `).all(addDays(jstToday(), -89)).map(r => ({
      ...r,
      roas_pct: r.ad_cost > 0 ? Math.round(r.ad_sales / r.ad_cost * 1000) / 10 : null,
    }));
  } catch { /* mirror 初期化前 */ }

  return { months, month: ym, skus, summary, daily, match };
}

// ============================================================
// P3: 売上方程式分解 (売上 = アクセス × CVR × 客単価)
// 逐次差分 (順序固定: アクセス → CVR → 客単価)。円建てで残差ゼロの会計的分解 (Codex R2)
//   Δ売上 = Δアクセス×CVR0×AOV0 + アクセス1×ΔCVR×AOV0 + アクセス1×CVR1×ΔAOV
// CVR/AOV は各窓の実測から導出 (恒等式なので残差は丸め分のみ)
// ============================================================
export function getEquation({ level = 'store', sku = null, windowDays = 7 } = {}) {
  const db = getMirrorDB();
  const w = [7, 14, 28].includes(Number(windowDays)) ? Number(windowDays) : 7;
  const table = level === 'sku' ? 'mirror_rakuten_item_daily' : 'mirror_rakuten_store_daily';
  let latest = null;
  try { latest = db.prepare(`SELECT MAX(date_jst) AS d FROM ${table}`).get()?.d; } catch { /* 初期化前 */ }
  if (!latest) return { available: false };

  const cur = { from: addDays(latest, -(w - 1)), to: latest };
  const prev = { from: addDays(latest, -(2 * w - 1)), to: addDays(latest, -w) };

  const agg = (win) => {
    if (level === 'sku') {
      return db.prepare(`
        SELECT COALESCE(SUM(sales_yen),0) AS sales, COALESCE(SUM(orders),0) AS orders,
               COALESCE(SUM(access_users),0) AS access
        FROM mirror_rakuten_item_daily
        WHERE item_manage_number = ? COLLATE NOCASE AND date_jst >= ? AND date_jst <= ?
      `).get(sku, win.from, win.to);
    }
    return db.prepare(`
      SELECT COALESCE(SUM(sales_all_yen),0) AS sales, COALESCE(SUM(orders_all),0) AS orders,
             COALESCE(SUM(access_all),0) AS access
      FROM mirror_rakuten_store_daily WHERE date_jst >= ? AND date_jst <= ?
    `).get(win.from, win.to);
  };
  const events = (win) => {
    try {
      return db.prepare(`
        SELECT DISTINCT campaign_type || ': ' || campaign_name AS label
        FROM mirror_rakuten_campaigns
        WHERE date_jst <= ? AND COALESCE(substr(end_at,1,10), date_jst) >= ? LIMIT 10
      `).all(win.to, win.from).map(r => r.label);
    } catch { return []; }
  };

  const c = agg(cur), p = agg(prev);
  const f = (x) => ({
    sales: x.sales, access: x.access, orders: x.orders,
    cvr: x.access > 0 ? x.orders / x.access : 0,
    aov: x.orders > 0 ? x.sales / x.orders : 0,
  });
  const c1 = f(c), p1 = f(p);
  const accessEffect = (c1.access - p1.access) * p1.cvr * p1.aov;
  const cvrEffect = c1.access * (c1.cvr - p1.cvr) * p1.aov;
  const aovEffect = c1.access * c1.cvr * (c1.aov - p1.aov);
  const delta = c1.sales - p1.sales;
  const residual = delta - (accessEffect + cvrEffect + aovEffect);

  const curEvents = events(cur), prevEvents = events(prev);
  const eventsDiffer = JSON.stringify([...curEvents].sort()) !== JSON.stringify([...prevEvents].sort());

  return {
    available: true, level, sku, window_days: w,
    current: { ...cur, sales: Math.round(c1.sales), access: c1.access,
      cvr_pct: Math.round(c1.cvr * 10000) / 100, aov: Math.round(c1.aov), events: curEvents },
    previous: { ...prev, sales: Math.round(p1.sales), access: p1.access,
      cvr_pct: Math.round(p1.cvr * 10000) / 100, aov: Math.round(p1.aov), events: prevEvents },
    delta: Math.round(delta),
    factors: {
      access: Math.round(accessEffect),
      cvr: Math.round(cvrEffect),
      aov: Math.round(aovEffect),
      residual: Math.round(residual),
    },
    confounded: eventsDiffer,
    note: eventsDiffer
      ? '⚠️ 前後でイベント構成が異なります (交絡あり)。変化を施策/実力と断定しないでください'
      : 'イベント構成は前後で同等です (交絡は比較的限定的)',
  };
}

// SKU別の方程式ワースト/ベスト (直近7日 vs 直前7日、売上変化の因子つき)
export function getEquationMovers({ limit = 15 } = {}) {
  const db = getMirrorDB();
  let latest = null;
  try { latest = db.prepare(`SELECT MAX(date_jst) AS d FROM mirror_rakuten_item_daily`).get()?.d; } catch { /* 初期化前 */ }
  if (!latest) return { available: false };
  const w = 7;
  const cur = { from: addDays(latest, -(w - 1)), to: latest };
  const prev = { from: addDays(latest, -(2 * w - 1)), to: addDays(latest, -w) };
  // 両期間の SKU 集合を基点に FULL OUTER 相当で結合 (今期間に行が無い=売上ゼロに落ちた SKU を
  // 取りこぼさない — Codex R3 Medium。最大級の急落こそ「今週の行が無い」商品)
  const rows = db.prepare(`
    WITH cur AS (
      SELECT item_manage_number AS sku, MAX(item_name) AS name,
             SUM(sales_yen) AS sales, SUM(orders) AS orders, SUM(access_users) AS access
      FROM mirror_rakuten_item_daily WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY item_manage_number
    ), prev AS (
      SELECT item_manage_number AS sku, MAX(item_name) AS name,
             SUM(sales_yen) AS sales, SUM(orders) AS orders, SUM(access_users) AS access
      FROM mirror_rakuten_item_daily WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY item_manage_number
    ), keys AS (
      SELECT sku FROM cur UNION SELECT sku FROM prev
    )
    SELECT k.sku, COALESCE(cur.name, prev.name) AS name,
           COALESCE(cur.sales,0) AS c_sales, COALESCE(cur.orders,0) AS c_orders, COALESCE(cur.access,0) AS c_access,
           COALESCE(prev.sales,0) AS p_sales, COALESCE(prev.orders,0) AS p_orders, COALESCE(prev.access,0) AS p_access
    FROM keys k
    LEFT JOIN cur ON cur.sku = k.sku
    LEFT JOIN prev ON prev.sku = k.sku
    WHERE COALESCE(cur.sales,0) + COALESCE(prev.sales,0) > 0
  `).all(cur.from, cur.to, prev.from, prev.to);

  const decomposed = rows.map(r => {
    const p = { access: r.p_access, cvr: r.p_access > 0 ? r.p_orders / r.p_access : 0, aov: r.p_orders > 0 ? r.p_sales / r.p_orders : 0 };
    const c = { access: r.c_access, cvr: r.c_access > 0 ? r.c_orders / r.c_access : 0, aov: r.c_orders > 0 ? r.c_sales / r.c_orders : 0 };
    const delta = r.c_sales - r.p_sales;
    const factors = {
      access: Math.round((c.access - p.access) * p.cvr * p.aov),
      cvr: Math.round(c.access * (c.cvr - p.cvr) * p.aov),
      aov: Math.round(c.access * c.cvr * (c.aov - p.aov)),
    };
    factors.residual = Math.round(delta - factors.access - factors.cvr - factors.aov);
    const main = Object.entries(factors).filter(([k]) => k !== 'residual')
      .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))[0];
    return {
      sku: r.sku, name: r.name || '',
      sales_cur: Math.round(r.c_sales), sales_prev: Math.round(r.p_sales), delta: Math.round(delta),
      factors, main_factor: main ? main[0] : null,
    };
  }).sort((a, b) => a.delta - b.delta);

  const n = Math.min(Math.max(Number(limit) || 15, 1), 50);
  return {
    available: true, window: { cur, prev },
    fallers: decomposed.slice(0, n),
    risers: decomposed.slice(-n).reverse(),
  };
}
