// ai-insights: 事実層の「Amazon の健康状態」節 (週次 facts.js・月次 facts-monthly.js で共用)
//
// 大原則は facts.js と同じ: 決まった SQL で計算済みの数字だけを渡す (AI に算術させない)。読み取りのみ。
//
// 読む表 (作るのはミニPC の apps/warehouse/ の取得。列は各スクリプトの CREATE TABLE のまま):
//   amazon_sales_traffic_daily / amazon_sales_traffic_sku_daily  fetch-amazon-sales-traffic.js (セッション・PV・カート獲得率・転換率)
//   amazon_listing_status_daily / amazon_listing_issue_daily / amazon_listing_fetch_runs  fetch-amazon-listing-issues.js
//   amazon_fba_returns / amazon_fba_reimbursements  fetch-amazon-returns.js / fetch-amazon-reimbursements.js
//   sp_report_fetch_days  sp-api-report.js (返品・補てんは「0 件で取れた日」をここで見分ける)
//   amazon_catalog_rank_daily  fetch-amazon-bsr.js (BSR)
//   fba_planning_daily / fba_planning_daily_days  fba-planning-history.js (在庫の年齢・長期保管料の見込み・過剰)
//   fact_amazon_ads_sku_attrib / ads_attrib_fetch_days  fetch-amazon-ads.js (広告の 1/7/14/30 日の帰属売上)
//   fact_amazon_price_snapshot  fetch-amazon-prices.js (カートが自社か)。Render の mirror では mirror_amazon_price_snapshot_daily
//
// 🚨 置き場所: この関数は渡された db (Render では warehouse-mirror.db) を読む。2026-10-24 時点で mirror に来ているのは
//    カート価格 (mirror_amazon_price_snapshot_daily) だけで、残りの表はミニPC の warehouse.db にしか無い。
//    → 表が無ければ「データなし (表が無い)」にするだけ (事実の JSON 全体は今までどおり作る)。
//    mirror に同じ列で `mirror_<表名>` (または同じ表名) を作れば、ここは変えずに読み始める (TABLES の候補順)。
//
// 「データなし」の決まり (各項目の status = 'ok' | 'no_data'、no_data は reason つき):
//   - 表が無い / 表が空 / 期間の中にデータが無い
//   - 古い: 期間の最後の日より STALE_AFTER_DAYS 日を超えて前で止まっている (= 取得が止まっている)
//   - 比べる相手 (前の期間) の日数が足りない (取り始めたばかり)
//   - 読めない (列が違う等の例外)。1 項目の失敗は他の項目と事実の JSON 全体を道連れにしない

export const STALE_AFTER_DAYS = 3;
export const TOP_N = 8;
const MIN_COVERAGE_RATIO = 0.85;      // 期間の日数のこの割合以上そろっていれば比べる (7 日 → 6 日・30 日 → 26 日)
const MIN_SESSIONS_PER_DAY = 5;       // SKU の転換率を比べる最低のセッション (7 日 → 35・30 日 → 150)
const CVR_DROP_RATIO = 0.7;           // 転換率が前の期間の 7 割以下
const CVR_DROP_MIN_PT = 1.0;          // かつ 1 ポイント以上下がった
const BUYBOX_DROP_MIN_PT = 10;        // カート獲得率が 10 ポイント以上下がった
const BSR_WORSE_RATIO = 1.5;          // BSR (順位の数) が 1.5 倍以上に悪化
const BSR_WORSE_MIN_RANKS = 100;      // かつ 100 位以上悪化 (上位の小さな揺れを拾わない)
const ADS_MIN_SPEND_PER_DAY = 300;    // 広告費がこれ×日数 (7 日 → 2,100 円) 以上の SKU だけ見る
const ADS_POOR_ROAS = 2.0;            // 14 日の帰属売上 ÷ 広告費 がこれ未満 = 悪い
const ADS_ATTRIB_SETTLE_DAYS = 14;    // 14 日の帰属は日 d の 14 日後まで伸びる (fetch-amazon-ads.js の説明)
const SALES_LOOKBACK_DAYS = 28;       // 「売れ筋」= 期間の最後の日までの 28 日に売上がある

/** 候補の表名 (先にあるものを使う)。mirror に同じ列で写したら mirror_ の名前で読む */
export const TABLES = {
  trafficStore: ['mirror_amazon_sales_traffic_daily', 'amazon_sales_traffic_daily'],
  trafficSku: ['mirror_amazon_sales_traffic_sku_daily', 'amazon_sales_traffic_sku_daily'],
  listingStatus: ['mirror_amazon_listing_status_daily', 'amazon_listing_status_daily'],
  listingIssue: ['mirror_amazon_listing_issue_daily', 'amazon_listing_issue_daily'],
  listingRuns: ['mirror_amazon_listing_fetch_runs', 'amazon_listing_fetch_runs'],
  returns: ['mirror_amazon_fba_returns', 'amazon_fba_returns'],
  reimbursements: ['mirror_amazon_fba_reimbursements', 'amazon_fba_reimbursements'],
  reportDays: ['mirror_sp_report_fetch_days', 'sp_report_fetch_days'],
  bsr: ['mirror_amazon_catalog_rank_daily', 'amazon_catalog_rank_daily'],
  planning: ['mirror_fba_planning_daily', 'fba_planning_daily'],
  planningDays: ['mirror_fba_planning_daily_days', 'fba_planning_daily_days'],
  adsAttrib: ['mirror_fact_amazon_ads_sku_attrib', 'fact_amazon_ads_sku_attrib'],
  adsAttribDays: ['mirror_ads_attrib_fetch_days', 'ads_attrib_fetch_days'],
  price: ['mirror_amazon_price_snapshot_daily', 'fact_amazon_price_snapshot'],
};
const RETURNS_REPORT = 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA';
const REIMB_REPORT = 'GET_FBA_REIMBURSEMENTS_DATA';

// ── 小さな道具 ────────────────────────────────────────────────────────

function addDays(ymd, delta) {
  const [y, m, d] = ymd.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d) + delta * 86400000);
  const pad = (n) => String(n).padStart(2, '0');
  return `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())}`;
}
function daysBetween(from, to) { // to は排他的
  return Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000);
}
const yen = (v) => (v == null || !Number.isFinite(Number(v)) ? null : Math.round(Number(v)));
const r1 = (v) => (v == null || !Number.isFinite(v) ? null : +v.toFixed(1));
const r2 = (v) => (v == null || !Number.isFinite(v) ? null : +v.toFixed(2));
const pctOf = (num, den) => (num != null && den > 0 ? r1((num / den) * 100) : null);
const changePct = (cur, prev) => (cur != null && prev > 0 ? r1((cur / prev - 1) * 100) : null);
const changePt = (cur, prev) => (cur != null && prev != null ? r1(cur - prev) : null);
function median(arr) {
  const s = arr.filter((v) => v != null).sort((a, b) => a - b);
  if (s.length === 0) return null;
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}
const noData = (reason, extra = {}) => ({ status: 'no_data', reason, ...extra });
const minDaysFor = (n) => Math.max(1, Math.ceil(n * MIN_COVERAGE_RATIO));

/** 候補の中で最初にある表 (view も可)。無ければ null */
export function resolveTable(db, candidates) {
  const stmt = db.prepare("SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?");
  for (const name of candidates) if (stmt.get(name)) return name;
  return null;
}

/**
 * 表があり・空でなく・期間の最後の日 (refDay) まで STALE_AFTER_DAYS 日以内のデータがあるか。
 * dateCol = 日付の列 ('YYYY-MM-DD')。where/params で絞る (市場など)。
 * fetchDays = 取得の記録 (sp_report_fetch_days 等) で新しさを決めるとき { table, dateCol, where, params }
 * @returns {{ ok: true, table, latest } | { ok: false, out }}
 */
function checkSource(db, candidates, { dateCol = 'date_jst', where = '', params = [], refDay, fetchDays = null, label }) {
  const table = resolveTable(db, candidates);
  if (!table) return { ok: false, out: noData(`表が無い (${candidates[candidates.length - 1]})`) };
  const w = where ? `AND ${where}` : '';
  let latest = null;
  let src = table;
  if (fetchDays) {
    const ft = resolveTable(db, fetchDays.candidates);
    if (ft) {
      const fw = fetchDays.where ? `AND ${fetchDays.where}` : '';
      latest = db.prepare(`SELECT MAX(${fetchDays.dateCol}) AS d FROM ${ft} WHERE ${fetchDays.dateCol} <= ? ${fw}`)
        .get(refDay, ...(fetchDays.params || []))?.d || null;
      src = ft;
    }
  }
  if (!latest) {
    latest = db.prepare(`SELECT MAX(${dateCol}) AS d FROM ${table} WHERE ${dateCol} <= ? ${w}`).get(refDay, ...params)?.d || null;
    src = table;
  }
  if (!latest) {
    const any = db.prepare(`SELECT MIN(${dateCol}) AS d FROM ${table} WHERE 1 = 1 ${w}`).get(...params)?.d || null;
    if (!any) return { ok: false, out: noData(`表が空 (${label || table} はまだ取れていない)`) };
    return { ok: false, out: noData(`期間のデータが無い (${label || table} の最初の日は ${any})`) };
  }
  if (latest < addDays(refDay, -STALE_AFTER_DAYS)) {
    return { ok: false, out: noData(`古い (${label || table} の最新は ${latest}・期間の最後の日 ${refDay} より ${STALE_AFTER_DAYS} 日を超えて前 = 取得が止まっている疑い)`, { latest }) };
  }
  return { ok: true, table, latest, src };
}

function guard(fn) {
  try {
    return fn();
  } catch (e) {
    return noData(`読めない (${String(e?.message || e).slice(0, 160)})`);
  }
}

function inList(arr) { return arr.map(() => '?').join(','); }

/**
 * SKU (小文字) → { sales, units }。注文ベース (アクセスのレポート) があればそれ・無ければ決済ベース (finance の統合 view)。
 * どちらも無ければ null。memo は同じ期間の呼び出しを 1 回にする
 */
function salesBySku(db, from, to, memo) {
  const key = `sku|${from}|${to}`;
  if (memo.has(key)) return memo.get(key);
  let out = null;
  try {
    const t = resolveTable(db, TABLES.trafficSku);
    if (t) {
      const rows = db.prepare(`SELECT LOWER(TRIM(seller_sku)) AS k, SUM(ordered_product_sales) AS sales, SUM(units_ordered) AS units
        FROM ${t} WHERE date_jst >= ? AND date_jst < ? GROUP BY LOWER(TRIM(seller_sku))`).all(from, to);
      if (rows.length > 0) out = { source: 'amazon_sales_traffic (注文ベース)', map: new Map(rows.map((r) => [r.k, r])) };
    }
  } catch { out = null; }
  if (!out) {
    try {
      const rows = db.prepare(`SELECT sku_key AS k, SUM(sales_gross_jpy_incl) AS sales, SUM(units_net_sold) AS units
        FROM v_mall_finance_daily_unified WHERE mall = 'amazon' AND date_jst >= ? AND date_jst < ? GROUP BY sku_key`).all(from, to);
      if (rows.length > 0) out = { source: 'v_mall_finance_daily_unified (決済ベース)', map: new Map(rows.map((r) => [r.k, r])) };
    } catch { out = null; }
  }
  memo.set(key, out);
  return out;
}

/** ASIN (大文字) → { sales, units }。考え方は salesBySku と同じ */
function salesByAsin(db, from, to, memo) {
  const key = `asin|${from}|${to}`;
  if (memo.has(key)) return memo.get(key);
  let out = null;
  try {
    const t = resolveTable(db, TABLES.trafficSku);
    if (t) {
      const rows = db.prepare(`SELECT UPPER(TRIM(child_asin)) AS k, SUM(ordered_product_sales) AS sales, SUM(units_ordered) AS units
        FROM ${t} WHERE date_jst >= ? AND date_jst < ? GROUP BY UPPER(TRIM(child_asin))`).all(from, to);
      if (rows.length > 0) out = { source: 'amazon_sales_traffic (注文ベース)', map: new Map(rows.map((r) => [r.k, r])) };
    }
  } catch { out = null; }
  if (!out) {
    try {
      const rows = db.prepare(`SELECT UPPER(TRIM(asin_norm)) AS k, SUM(sales_gross_jpy_incl) AS sales, SUM(units_net_sold) AS units
        FROM v_mall_finance_daily_unified WHERE mall = 'amazon' AND date_jst >= ? AND date_jst < ? AND asin_norm IS NOT NULL AND asin_norm <> ''
        GROUP BY UPPER(TRIM(asin_norm))`).all(from, to);
      if (rows.length > 0) out = { source: 'v_mall_finance_daily_unified (決済ベース)', map: new Map(rows.map((r) => [r.k, r])) };
    } catch { out = null; }
  }
  memo.set(key, out);
  return out;
}

/** SKU (小文字) → 商品名。出品の写し → finance の順。読めなければ空 */
function productNames(db, skus) {
  const names = new Map();
  if (skus.length === 0) return names;
  try {
    const t = resolveTable(db, TABLES.listingStatus);
    if (t) {
      for (const r of db.prepare(`SELECT sku_key AS k, MAX(item_name) AS n FROM ${t} WHERE sku_key IN (${inList(skus)}) GROUP BY sku_key`).all(...skus)) {
        if (r.n) names.set(r.k, r.n);
      }
    }
  } catch { /* 名前は無くてもよい */ }
  const rest = skus.filter((s) => !names.has(s));
  if (rest.length > 0) {
    try {
      for (const r of db.prepare(`SELECT sku_key AS k, MAX(product_name) AS n FROM v_mall_finance_daily_unified
        WHERE mall = 'amazon' AND sku_key IN (${inList(rest)}) GROUP BY sku_key`).all(...rest)) {
        if (r.n) names.set(r.k, r.n);
      }
    } catch { /* 同上 */ }
  }
  return names;
}
const cutName = (s) => (s == null ? null : String(s).length > 40 ? `${String(s).slice(0, 39)}…` : String(s));

// ── 各項目 ────────────────────────────────────────────────────────────

/** 店全体のセッション・転換率・カート獲得率 (今の期間 / 前の期間 / それより前の期間の中央値) */
function storeTraffic(db, ctx) {
  const chk = checkSource(db, TABLES.trafficStore, { refDay: ctx.refDay, label: 'アクセス (店全体)' });
  if (!chk.ok) return chk.out;
  const agg = ([from, to]) => {
    const r = db.prepare(`SELECT COUNT(sessions) AS days, SUM(sessions) AS sessions, SUM(page_views) AS pv,
        SUM(units_ordered) AS units, SUM(ordered_product_sales) AS sales,
        SUM(buy_box_percentage * page_views) AS bb_w, SUM(CASE WHEN buy_box_percentage IS NOT NULL THEN page_views END) AS bb_pv,
        AVG(buy_box_percentage) AS bb_avg
      FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ?`).get(from, to);
    const days = r?.days || 0;
    return {
      days,
      expected_days: daysBetween(from, to),
      sessions: r?.sessions ?? null,
      page_views: r?.pv ?? null,
      units_ordered: r?.units ?? null,
      ordered_sales_yen: yen(r?.sales),
      cvr_pct: pctOf(r?.units, r?.sessions),
      // カート獲得率は PV の重み付き平均 (PV が無い日だけなら単純平均)
      buy_box_pct: r?.bb_pv > 0 ? r1(r.bb_w / r.bb_pv) : r1(r?.bb_avg),
    };
  };
  const cur = agg(ctx.cur);
  if (cur.days < minDaysFor(cur.expected_days)) {
    return noData(`この期間の日数が足りない (${cur.days}/${cur.expected_days} 日)`, { latest: chk.latest });
  }
  const prevRaw = agg(ctx.prev);
  const prev = prevRaw.days >= minDaysFor(prevRaw.expected_days) ? prevRaw : null;
  const base = ctx.baseline.map(agg).filter((b) => b.days >= minDaysFor(b.expected_days));
  return {
    status: 'ok',
    as_of: chk.latest,
    [ctx.labels.cur]: cur,
    [ctx.labels.prev]: prev,
    [ctx.labels.base]: base.length > 0 ? {
      periods_used: base.length,
      sessions: median(base.map((b) => b.sessions)),
      cvr_pct: r1(median(base.map((b) => b.cvr_pct))),
      buy_box_pct: r1(median(base.map((b) => b.buy_box_pct))),
    } : null,
    sessions_change_pct: prev ? changePct(cur.sessions, prev.sessions) : null,
    cvr_change_pt: prev ? changePt(cur.cvr_pct, prev.cvr_pct) : null,
    buy_box_change_pt: prev ? changePt(cur.buy_box_pct, prev.buy_box_pct) : null,
    note: prev ? undefined : '前の期間のデータが足りないため比較なし (取り始めたばかり)',
  };
}

/** 転換率・カート獲得率が大きく落ちた SKU (前の期間の売上の大きい順) */
function skuConversionDrops(db, ctx) {
  const chk = checkSource(db, TABLES.trafficSku, { refDay: ctx.refDay, label: 'アクセス (SKU)' });
  if (!chk.ok) return chk.out;
  const dayCount = ([from, to]) => db.prepare(`SELECT COUNT(DISTINCT date_jst) AS n FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ?`).get(from, to)?.n || 0;
  const curDays = dayCount(ctx.cur);
  const prevDays = dayCount(ctx.prev);
  if (curDays < minDaysFor(daysBetween(...ctx.cur))) return noData(`この期間の日数が足りない (${curDays}/${daysBetween(...ctx.cur)} 日)`);
  if (prevDays < minDaysFor(daysBetween(...ctx.prev))) return noData(`前の期間の日数が足りない (${prevDays}/${daysBetween(...ctx.prev)} 日・取り始めたばかり)`);
  const rows = db.prepare(`SELECT LOWER(TRIM(seller_sku)) AS k, MAX(seller_sku) AS sku, MAX(child_asin) AS asin,
      SUM(CASE WHEN date_jst >= ? THEN sessions END) AS s_cur, SUM(CASE WHEN date_jst < ? THEN sessions END) AS s_prev,
      SUM(CASE WHEN date_jst >= ? THEN units_ordered END) AS u_cur, SUM(CASE WHEN date_jst < ? THEN units_ordered END) AS u_prev,
      SUM(CASE WHEN date_jst >= ? THEN ordered_product_sales END) AS y_cur, SUM(CASE WHEN date_jst < ? THEN ordered_product_sales END) AS y_prev,
      SUM(CASE WHEN date_jst >= ? THEN buy_box_percentage * page_views END) AS bbw_cur,
      SUM(CASE WHEN date_jst >= ? AND buy_box_percentage IS NOT NULL THEN page_views END) AS bbpv_cur,
      SUM(CASE WHEN date_jst < ? THEN buy_box_percentage * page_views END) AS bbw_prev,
      SUM(CASE WHEN date_jst < ? AND buy_box_percentage IS NOT NULL THEN page_views END) AS bbpv_prev
    FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ?
    GROUP BY LOWER(TRIM(seller_sku))`).all(...Array(10).fill(ctx.cur[0]), ctx.prev[0], ctx.cur[1]);
  const minPrev = MIN_SESSIONS_PER_DAY * daysBetween(...ctx.prev);
  const minCur = Math.ceil((MIN_SESSIONS_PER_DAY * daysBetween(...ctx.cur)) / 2);
  const hits = [];
  let checked = 0;
  for (const r of rows) {
    if (!(r.s_prev >= minPrev) || !(r.s_cur >= minCur)) continue;
    checked++;
    const cvrCur = pctOf(r.u_cur, r.s_cur);
    const cvrPrev = pctOf(r.u_prev, r.s_prev);
    const bbCur = r.bbpv_cur > 0 ? r1(r.bbw_cur / r.bbpv_cur) : null;
    const bbPrev = r.bbpv_prev > 0 ? r1(r.bbw_prev / r.bbpv_prev) : null;
    const cvrDrop = cvrCur != null && cvrPrev != null && cvrCur <= cvrPrev * CVR_DROP_RATIO && cvrPrev - cvrCur >= CVR_DROP_MIN_PT;
    const bbDrop = bbCur != null && bbPrev != null && bbPrev - bbCur >= BUYBOX_DROP_MIN_PT;
    if (!cvrDrop && !bbDrop) continue;
    hits.push({
      k: r.k, sku: r.sku, asin: r.asin,
      drop: [cvrDrop && 'cvr', bbDrop && 'buy_box'].filter(Boolean).join('+'),
      [`${ctx.labels.prevShort}_sales_yen`]: yen(r.y_prev),
      sales_yen: yen(r.y_cur),
      sessions: r.s_cur, [`${ctx.labels.prevShort}_sessions`]: r.s_prev,
      cvr_pct: cvrCur, [`${ctx.labels.prevShort}_cvr_pct`]: cvrPrev, cvr_change_pt: changePt(cvrCur, cvrPrev),
      buy_box_pct: bbCur, [`${ctx.labels.prevShort}_buy_box_pct`]: bbPrev, buy_box_change_pt: changePt(bbCur, bbPrev),
    });
  }
  hits.sort((a, b) => (b[`${ctx.labels.prevShort}_sales_yen`] || 0) - (a[`${ctx.labels.prevShort}_sales_yen`] || 0) || (a.k < b.k ? -1 : 1));
  const top = hits.slice(0, TOP_N);
  const names = productNames(db, top.map((h) => h.k));
  return {
    status: 'ok',
    as_of: chk.latest,
    checked_skus: checked,
    matched_skus: hits.length,
    rule: `前の期間のセッション ${minPrev} 以上の SKU で、転換率が前の期間の ${CVR_DROP_RATIO * 100}% 以下かつ ${CVR_DROP_MIN_PT} ポイント以上低下、またはカート獲得率が ${BUYBOX_DROP_MIN_PT} ポイント以上低下`,
    items: top.map(({ k, ...h }) => ({ ...h, product_name: cutName(names.get(k)) })),
  };
}

/** いま出品停止・ERROR がある売れ筋 SKU (期間の最後の日以前の最新の写し) */
function listingProblems(db, ctx) {
  const chk = checkSource(db, TABLES.listingStatus, {
    refDay: ctx.refDay, label: '出品の状態',
    fetchDays: { candidates: TABLES.listingRuns, dateCol: 'date_jst' },
  });
  if (!chk.ok) return chk.out;
  // 写しの日 = 状態の表で期間の最後の日以前の最新 (取得の記録の日と同じはずだが、表の側で決める)
  const asOf = db.prepare(`SELECT MAX(date_jst) AS d FROM ${chk.table} WHERE date_jst <= ?`).get(ctx.refDay)?.d;
  if (!asOf || asOf < addDays(ctx.refDay, -STALE_AFTER_DAYS)) return noData(`古い (出品の状態の最新は ${asOf || 'なし'})`);
  let snapshot = null;
  const runT = resolveTable(db, TABLES.listingRuns);
  if (runT) snapshot = db.prepare(`SELECT status FROM ${runT} WHERE date_jst = ?`).get(asOf)?.status || null;
  const rows = db.prepare(`SELECT sku_key, seller_sku, asin, item_name, status, is_buyable, suppressed, search_suppressed, error_count
    FROM ${chk.table} WHERE date_jst = ? AND (is_buyable = 0 OR suppressed = 1 OR error_count > 0)`).all(asOf);
  const total = db.prepare(`SELECT COUNT(*) AS n FROM ${chk.table} WHERE date_jst = ?`).get(asOf)?.n || 0;
  const sales = salesBySku(db, addDays(ctx.refDay, -(SALES_LOOKBACK_DAYS - 1)), addDays(ctx.refDay, 1), ctx.memo);
  const issueT = resolveTable(db, TABLES.listingIssue);
  const topIssue = new Map();
  if (issueT && rows.length > 0) {
    const iss = db.prepare(`SELECT sku_key, code, message FROM ${issueT} WHERE date_jst = ? AND severity = 'ERROR' ORDER BY sku_key, issue_key`).all(asOf);
    for (const i of iss) if (!topIssue.has(i.sku_key)) topIssue.set(i.sku_key, { code: i.code, message: cutName(i.message) });
  }
  const enriched = rows.map((r) => {
    const s = sales?.map.get(String(r.sku_key).toLowerCase());
    return {
      sku: r.seller_sku, asin: r.asin, product_name: cutName(r.item_name),
      listing_status: r.status, is_buyable: r.is_buyable === 1, suppressed: r.suppressed === 1, error_count: r.error_count,
      top_error: topIssue.get(r.sku_key) || null,
      sales_28d_yen: sales ? yen(s?.sales ?? 0) : null,
    };
  });
  const selling = sales ? enriched.filter((e) => e.sales_28d_yen > 0) : enriched;
  selling.sort((a, b) => (b.sales_28d_yen || 0) - (a.sales_28d_yen || 0) || (b.error_count || 0) - (a.error_count || 0) || (a.sku < b.sku ? -1 : 1));
  return {
    status: 'ok',
    as_of: asOf,
    snapshot: snapshot === 'complete' ? 'complete' : snapshot === 'partial' ? 'partial (一部の出品しか写せていない日)' : null,
    listings_total: total,
    problem_skus_total: rows.length,
    not_buyable_skus: rows.filter((r) => r.is_buyable === 0).length,
    selling_problem_skus: sales ? selling.length : null,
    sales_basis: sales ? `${sales.source}・${SALES_LOOKBACK_DAYS} 日` : '売上が分からないため ERROR の数の順 (売れ筋かは不明)',
    items: selling.slice(0, TOP_N),
  };
}

/** カート (Buy Box) を他社に取られている売れ筋 SKU (価格の写し) */
function buyboxLost(db, ctx) {
  const chk = checkSource(db, TABLES.price, { refDay: ctx.refDay, label: 'カート価格' });
  if (!chk.ok) return chk.out;
  const asOf = chk.latest;
  const [from] = ctx.cur;
  const rows = db.prepare(`SELECT LOWER(TRIM(seller_sku)) AS k, MAX(seller_sku) AS sku, MAX(asin) AS asin,
      SUM(CASE WHEN buybox_is_mine = 0 THEN 1 ELSE 0 END) AS lost_days,
      SUM(CASE WHEN buybox_is_mine IS NOT NULL THEN 1 ELSE 0 END) AS seen_days,
      MAX(CASE WHEN date_jst = ? THEN buybox_is_mine END) AS mine_now,
      MAX(CASE WHEN date_jst = ? THEN my_price END) AS my_price,
      MAX(CASE WHEN date_jst = ? THEN buybox_price END) AS buybox_price
    FROM ${chk.table} WHERE date_jst >= ? AND date_jst <= ? GROUP BY LOWER(TRIM(seller_sku))`).all(asOf, asOf, asOf, from, asOf);
  const lostNow = rows.filter((r) => r.mine_now === 0);
  const sales = salesBySku(db, addDays(ctx.refDay, -(SALES_LOOKBACK_DAYS - 1)), addDays(ctx.refDay, 1), ctx.memo);
  const items = lostNow.map((r) => ({
    k: r.k, sku: r.sku, asin: r.asin,
    lost_days: r.lost_days, seen_days: r.seen_days,
    my_price_yen: yen(r.my_price), buybox_price_yen: yen(r.buybox_price),
    sales_28d_yen: sales ? yen(sales.map.get(r.k)?.sales ?? 0) : null,
  }));
  const selling = sales ? items.filter((i) => i.sales_28d_yen > 0) : items;
  selling.sort((a, b) => (b.sales_28d_yen || 0) - (a.sales_28d_yen || 0) || (b.lost_days || 0) - (a.lost_days || 0) || (a.k < b.k ? -1 : 1));
  const top = selling.slice(0, TOP_N);
  const names = productNames(db, top.map((t) => t.k));
  return {
    status: 'ok',
    as_of: asOf,
    lost_now_skus: lostNow.length,
    selling_lost_now_skus: sales ? selling.length : null,
    sales_basis: sales ? `${sales.source}・${SALES_LOOKBACK_DAYS} 日` : '売上が分からないため取られた日数の順',
    note: 'lost_days = この期間に他社がカートだった日数 / seen_days = カートがあった日数。価格は as_of の日 (税込)',
    items: top.map(({ k, ...t }) => ({ ...t, product_name: cutName(names.get(k)) })),
  };
}

/** 返品の多い SKU と理由の上位 */
function returnsFacts(db, ctx) {
  const chk = checkSource(db, TABLES.returns, {
    dateCol: 'return_date_jst', refDay: ctx.refDay, label: '返品',
    fetchDays: { candidates: TABLES.reportDays, dateCol: 'date_jst', where: 'report_type = ?', params: [RETURNS_REPORT] },
  });
  // 返品が 0 件の期間は表が空でもありうる → 取得の記録 (sp_report_fetch_days) が新しければ「0 件で取れている」= ok
  if (!chk.ok) return chk.out;
  const [from, to] = ctx.cur;
  const total = db.prepare(`SELECT COALESCE(SUM(quantity), 0) AS q, COUNT(*) AS n FROM ${chk.table} WHERE return_date_jst >= ? AND return_date_jst < ?`).get(from, to);
  const prevTotal = db.prepare(`SELECT COALESCE(SUM(quantity), 0) AS q FROM ${chk.table} WHERE return_date_jst >= ? AND return_date_jst < ?`).get(...ctx.prev);
  const prevCovered = chk.src !== chk.table
    ? (db.prepare(`SELECT COUNT(*) AS n FROM ${chk.src} WHERE report_type = ? AND date_jst >= ? AND date_jst < ?`).get(RETURNS_REPORT, ...ctx.prev)?.n || 0)
    : null;
  const reasons = db.prepare(`SELECT COALESCE(NULLIF(reason, ''), '(理由なし)') AS reason, SUM(quantity) AS units FROM ${chk.table}
    WHERE return_date_jst >= ? AND return_date_jst < ? GROUP BY 1 ORDER BY units DESC, reason LIMIT 5`).all(from, to);
  const skuRows = db.prepare(`SELECT LOWER(TRIM(seller_sku)) AS k, MAX(seller_sku) AS sku, MAX(asin) AS asin, MAX(product_name) AS name, SUM(quantity) AS units
    FROM ${chk.table} WHERE return_date_jst >= ? AND return_date_jst < ? GROUP BY LOWER(TRIM(seller_sku))
    ORDER BY units DESC, k LIMIT ?`).all(from, to, TOP_N);
  const reasonStmt = db.prepare(`SELECT COALESCE(NULLIF(reason, ''), '(理由なし)') AS reason, SUM(quantity) AS units FROM ${chk.table}
    WHERE LOWER(TRIM(seller_sku)) = ? AND return_date_jst >= ? AND return_date_jst < ? GROUP BY 1 ORDER BY units DESC, reason LIMIT 3`);
  const sold = salesBySku(db, from, to, ctx.memo);
  const prevComparable = prevCovered === null || prevCovered >= minDaysFor(daysBetween(...ctx.prev));
  return {
    status: 'ok',
    as_of: chk.latest,
    returned_units: total.q,
    [`${ctx.labels.prevShort}_returned_units`]: prevComparable ? prevTotal.q : null,
    top_reasons: reasons,
    top_skus: skuRows.map((r) => {
      const s = sold?.map.get(r.k);
      return {
        sku: r.sku, asin: r.asin, product_name: cutName(r.name), returned_units: r.units,
        sold_units: sold ? (s?.units ?? 0) : null,
        // 返品は注文より後に来るため、同じ期間の販売数との比は目安
        return_rate_pct: sold && s?.units > 0 ? pctOf(r.units, s.units) : null,
        sales_yen: sold ? yen(s?.sales ?? 0) : null,
        reasons: reasonStmt.all(r.k, from, to),
      };
    }),
    note: '返品日の基準。return_rate_pct = 同じ期間の販売数に対する返品数 (返品は注文より遅れて来るので目安)。理由は Amazon のコードのまま',
  };
}

/** 補てん (紛失・破損などで Amazon から戻ったお金) */
function reimbursementFacts(db, ctx) {
  const chk = checkSource(db, TABLES.reimbursements, {
    dateCol: 'approval_date_jst', refDay: ctx.refDay, label: '補てん',
    fetchDays: { candidates: TABLES.reportDays, dateCol: 'date_jst', where: 'report_type = ?', params: [REIMB_REPORT] },
  });
  if (!chk.ok) return chk.out;
  const [from, to] = ctx.cur;
  const cur = db.prepare(`SELECT COUNT(*) AS n, COALESCE(SUM(amount_total), 0) AS amt, COALESCE(SUM(quantity_reimbursed_total), 0) AS q
    FROM ${chk.table} WHERE approval_date_jst >= ? AND approval_date_jst < ?`).get(from, to);
  const prev = db.prepare(`SELECT COALESCE(SUM(amount_total), 0) AS amt FROM ${chk.table} WHERE approval_date_jst >= ? AND approval_date_jst < ?`).get(...ctx.prev);
  const reasons = db.prepare(`SELECT COALESCE(NULLIF(reason, ''), '(理由なし)') AS reason, COUNT(*) AS cases, SUM(amount_total) AS amt
    FROM ${chk.table} WHERE approval_date_jst >= ? AND approval_date_jst < ? GROUP BY 1 ORDER BY amt DESC, reason LIMIT 5`).all(from, to);
  return {
    status: 'ok',
    as_of: chk.latest,
    cases: cur.n,
    units: cur.q,
    amount_yen: yen(cur.amt),
    [`${ctx.labels.prevShort}_amount_yen`]: yen(prev.amt),
    by_reason: reasons.map((r) => ({ reason: r.reason, cases: r.cases, amount_yen: yen(r.amt) })),
  };
}

/** BSR (順位の数) が大きく悪化した ASIN (前の期間の売上の大きい順) */
function bsrDrops(db, ctx) {
  const chk = checkSource(db, TABLES.bsr, { refDay: ctx.refDay, label: 'BSR' });
  if (!chk.ok) return chk.out;
  const dayCount = ([from, to]) => db.prepare(`SELECT COUNT(DISTINCT date_jst) AS n FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ?`).get(from, to)?.n || 0;
  const need = (w) => Math.max(1, Math.ceil(daysBetween(...w) / 2));
  const curDays = dayCount(ctx.cur);
  const prevDays = dayCount(ctx.prev);
  if (curDays < need(ctx.cur)) return noData(`この期間の日数が足りない (${curDays}/${daysBetween(...ctx.cur)} 日)`);
  if (prevDays < need(ctx.prev)) return noData(`前の期間の日数が足りない (${prevDays}/${daysBetween(...ctx.prev)} 日・取り始めたばかり)`);
  const rows = db.prepare(`SELECT asin, rank_type, category_id, MAX(category_title) AS title,
      AVG(CASE WHEN date_jst >= ? THEN rank END) AS r_cur, AVG(CASE WHEN date_jst < ? THEN rank END) AS r_prev
    FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ?
    GROUP BY asin, rank_type, category_id`).all(ctx.cur[0], ctx.cur[0], ctx.prev[0], ctx.cur[1]);
  // ASIN ごとに 1 つの分類: 両方の期間に順位がある中で、大分類 (display_group) → 無ければ前の期間で順位のいちばん良い分類
  const byAsin = new Map();
  for (const r of rows) {
    if (r.r_cur == null || r.r_prev == null) continue;
    const curBest = byAsin.get(r.asin);
    const better = !curBest
      || (r.rank_type === 'display_group' && curBest.rank_type !== 'display_group')
      || (r.rank_type === curBest.rank_type && r.r_prev < curBest.r_prev);
    if (better) byAsin.set(r.asin, r);
  }
  const sales = salesByAsin(db, ...ctx.prev, ctx.memo);
  const hits = [];
  for (const r of byAsin.values()) {
    if (!(r.r_cur >= r.r_prev * BSR_WORSE_RATIO && r.r_cur - r.r_prev >= BSR_WORSE_MIN_RANKS)) continue;
    hits.push({
      asin: r.asin, category: r.title || r.category_id, rank_type: r.rank_type,
      rank: Math.round(r.r_cur), [`${ctx.labels.prevShort}_rank`]: Math.round(r.r_prev),
      rank_worse_pct: changePct(r.r_cur, r.r_prev),
      [`${ctx.labels.prevShort}_sales_yen`]: sales ? yen(sales.map.get(String(r.asin).toUpperCase())?.sales ?? 0) : null,
    });
  }
  const sk = `${ctx.labels.prevShort}_sales_yen`;
  hits.sort((a, b) => (b[sk] || 0) - (a[sk] || 0) || (b.rank_worse_pct || 0) - (a.rank_worse_pct || 0) || (a.asin < b.asin ? -1 : 1));
  return {
    status: 'ok',
    as_of: chk.latest,
    checked_asins: byAsin.size,
    matched_asins: hits.length,
    rule: `期間の平均順位が前の期間の ${BSR_WORSE_RATIO} 倍以上かつ ${BSR_WORSE_MIN_RANKS} 位以上悪化 (数が大きいほど悪い)`,
    sales_basis: sales ? sales.source : '売上が分からないため悪化の大きい順',
    items: hits.slice(0, TOP_N),
  };
}

const known = (v) => v !== null && v !== undefined;
function sumKnown(xs) {
  const k = xs.filter(known);
  return k.length === 0 ? null : k.reduce((s, v) => s + Number(v), 0);
}
/** 長期保管料 (AIS) の見込み。365 日超は 365-plus が分かればそれ・無ければ 366-455 + 456+ (alert-fba-aged-inventory.js と同じ決め方) */
function aisEstOf(r) {
  const over365 = known(r.ais_est_365_plus) ? r.ais_est_365_plus : sumKnown([r.ais_est_366_455, r.ais_est_456_plus]);
  return sumKnown([r.ais_est_181_210, r.ais_est_211_240, r.ais_est_241_270, r.ais_est_271_300, r.ais_est_301_330, r.ais_est_331_365, over365]);
}
/** 181 日以上の個数 (alert-fba-aged-inventory.js の agedOver180 と同じ決め方: 区分の組を重ねない) */
function aged181Of(r) {
  const main = [r.inv_age_181_270, r.inv_age_271_365];
  const alt = [r.inv_age_181_330, r.inv_age_331_365];
  const mid = main.some(known) ? main : alt.some(known) ? alt : [];
  const over365 = known(r.inv_age_365_plus) ? r.inv_age_365_plus : sumKnown([r.inv_age_366_455, r.inv_age_456_plus]);
  return sumKnown([...mid, over365]);
}

/** 長期保管料の見込みの合計と上位 SKU (在庫計画の最新の写し・JP) */
function longTermStorage(db, ctx) {
  const chk = checkSource(db, TABLES.planning, {
    dateCol: 'snapshot_date', where: "market = 'jp'", refDay: ctx.refDay, label: '在庫計画',
    fetchDays: { candidates: TABLES.planningDays, dateCol: 'snapshot_date', where: "market = 'jp'" },
  });
  if (!chk.ok) return chk.out;
  const snap = (day) => db.prepare(`SELECT MAX(snapshot_date) AS d FROM ${chk.table} WHERE market = 'jp' AND snapshot_date <= ?`).get(day)?.d || null;
  const asOf = snap(ctx.refDay);
  if (!asOf || asOf < addDays(ctx.refDay, -STALE_AFTER_DAYS)) return noData(`古い (在庫計画の最新は ${asOf || 'なし'})`);
  const COLS = `amazon_sku, asin, available, your_price, estimated_excess_qty, estimated_storage_cost_next_month, recommended_action, units_shipped_t30,
    inv_age_181_270, inv_age_271_365, inv_age_181_330, inv_age_331_365, inv_age_365_plus, inv_age_366_455, inv_age_456_plus,
    ais_est_181_210, ais_est_211_240, ais_est_241_270, ais_est_271_300, ais_est_301_330, ais_est_331_365, ais_est_365_plus, ais_est_366_455, ais_est_456_plus`;
  const load = (d) => db.prepare(`SELECT ${COLS} FROM ${chk.table} WHERE market = 'jp' AND snapshot_date = ?`).all(d);
  const rows = load(asOf).map((r) => ({ ...r, ais: aisEstOf(r), aged181: aged181Of(r) }));
  const totals = (rs) => ({
    ais_est_yen: yen(sumKnown(rs.map((r) => r.ais))),
    storage_next_month_yen: yen(sumKnown(rs.map((r) => r.estimated_storage_cost_next_month))),
    aged_181_plus_units: sumKnown(rs.map((r) => r.aged181)),
    excess_units: sumKnown(rs.map((r) => r.estimated_excess_qty)),
  });
  const cur = totals(rows);
  // 比べる相手 = 前の期間の最後の日以前の最新の写し (STALE の幅の中にあるときだけ)
  const prevRef = addDays(ctx.cur[0], -1);
  const prevDay = snap(prevRef);
  const prev = prevDay && prevDay >= addDays(prevRef, -STALE_AFTER_DAYS)
    ? totals(load(prevDay).map((r) => ({ ...r, ais: aisEstOf(r), aged181: aged181Of(r) })))
    : null;
  const top = rows.filter((r) => (r.ais || 0) > 0 || (r.aged181 || 0) > 0)
    .sort((a, b) => (b.ais || 0) - (a.ais || 0) || (b.aged181 || 0) - (a.aged181 || 0) || (a.amazon_sku < b.amazon_sku ? -1 : 1))
    .slice(0, TOP_N);
  const names = productNames(db, top.map((t) => String(t.amazon_sku).toLowerCase()));
  return {
    status: 'ok',
    as_of: asOf,
    skus: rows.length,
    ...cur,
    [`${ctx.labels.prevShort}_snapshot`]: prevDay && prev ? { as_of: prevDay, ...prev } : null,
    note: 'Amazon の在庫計画レポートの見込み (請求の確定値ではない)。列がレポートに無ければ null',
    top_skus: top.map((r) => ({
      sku: r.amazon_sku, asin: r.asin, product_name: cutName(names.get(String(r.amazon_sku).toLowerCase())),
      ais_est_yen: yen(r.ais), aged_181_plus_units: r.aged181, available: r.available,
      units_shipped_30d: r.units_shipped_t30, recommended_action: r.recommended_action,
    })),
  };
}

/** 広告の 14 日 ROAS が悪い SKU (広告費の大きい順) */
function adsPoorRoas(db, ctx) {
  const chk = checkSource(db, TABLES.adsAttrib, {
    refDay: ctx.refDay, label: '広告の帰属売上',
    fetchDays: { candidates: TABLES.adsAttribDays, dateCol: 'date_jst' },
  });
  if (!chk.ok) return chk.out;
  const [from, to] = ctx.adsWindow;
  const days = daysBetween(from, to);
  const n = db.prepare(`SELECT COUNT(DISTINCT date_jst) AS n FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ?`).get(from, to)?.n || 0;
  if (n < minDaysFor(days)) return noData(`見る期間 (${from}〜${addDays(to, -1)}) の日数が足りない (${n}/${days} 日)`);
  const tot = db.prepare(`SELECT SUM(cost) AS cost, SUM(sales_14d) AS s14, SUM(CASE WHEN sales_14d IS NULL THEN 1 ELSE 0 END) AS nulls, COUNT(*) AS rows
    FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ?`).get(from, to);
  if (tot.rows > 0 && tot.nulls === tot.rows) return noData('14 日の帰属売上 (sales_14d) が全部空 (レポートが列を返していない)');
  const rows = db.prepare(`SELECT target_granularity AS g, target, MAX(advertised_sku) AS sku, MAX(advertised_asin) AS asin,
      SUM(cost) AS cost, SUM(clicks) AS clicks, SUM(sales_14d) AS s14, SUM(CASE WHEN sales_14d IS NULL THEN 1 ELSE 0 END) AS nulls
    FROM ${chk.table} WHERE date_jst >= ? AND date_jst < ? AND target_granularity IN ('sku', 'asin')
    GROUP BY target_granularity, target`).all(from, to);
  const minSpend = ADS_MIN_SPEND_PER_DAY * days;
  const hits = rows.filter((r) => r.nulls === 0 && r.cost >= minSpend && (r.s14 || 0) / r.cost < ADS_POOR_ROAS)
    .sort((a, b) => b.cost - a.cost || (a.target < b.target ? -1 : 1));
  const top = hits.slice(0, TOP_N);
  const names = productNames(db, top.filter((t) => t.g === 'sku').map((t) => t.target));
  return {
    status: 'ok',
    as_of: chk.latest,
    window: { from, to: addDays(to, -1) },
    window_note: ctx.adsWindowNote,
    spend_yen: yen(tot.cost),
    sales_14d_yen: yen(tot.s14),
    roas_14d: tot.cost > 0 && tot.s14 != null ? r2(tot.s14 / tot.cost) : null,
    rule: `広告費 ${minSpend} 円以上で 14 日の帰属売上 ÷ 広告費 が ${ADS_POOR_ROAS} 未満`,
    matched_targets: hits.length,
    items: top.map((r) => ({
      sku: r.sku || (r.g === 'sku' ? r.target : null), asin: r.asin || (r.g === 'asin' ? r.target.toUpperCase() : null),
      product_name: r.g === 'sku' ? cutName(names.get(r.target)) : null,
      spend_yen: yen(r.cost), sales_14d_yen: yen(r.s14), roas_14d: r2((r.s14 || 0) / r.cost), clicks: r.clicks,
    })),
  };
}

// ── 入口 ──────────────────────────────────────────────────────────────

const ITEMS = [
  ['store_traffic', storeTraffic],
  ['sku_conversion_drops', skuConversionDrops],
  ['listing_problems', listingProblems],
  ['buybox_lost', buyboxLost],
  ['returns', returnsFacts],
  ['reimbursements', reimbursementFacts],
  ['bsr_drops', bsrDrops],
  ['long_term_storage', longTermStorage],
  ['ads_poor_roas', adsPoorRoas],
];

/**
 * 「Amazon の健康状態」の節。どの表が無くても・壊れていても例外を外に出さない (項目ごとに no_data)。
 * @param {object} db better-sqlite3
 * @param {object} p
 *   periodStart / periodEnd  今の期間 (periodEnd は排他的)
 *   prevStart                前の期間の始まり (前の期間 = prevStart〜periodStart)
 *   baseline                 中央値を取る期間の配列 [[from, to), ...] (前の期間を含めてよい)
 *   granularity              'week' | 'month' (名前の付け方と広告の窓)
 *   suppressReason           入れると全項目を出さずにこの理由の no_data だけ返す (Amazon の売上が欠損した期間など)
 */
export function buildAmazonHealthFacts(db, p) {
  const isMonth = p.granularity === 'month';
  const labels = isMonth
    ? { cur: 'this_month', prev: 'prev_month', base: 'prior_3m_median', prevShort: 'prev_month' }
    : { cur: 'this_week', prev: 'prev_week', base: 'prior_4w_median', prevShort: 'prev_week' };
  const refDay = addDays(p.periodEnd, -1);
  const head = {
    period: { from: p.periodStart, to: refDay },
    prev_period: { from: p.prevStart, to: addDays(p.periodStart, -1) },
    stale_after_days: STALE_AFTER_DAYS,
    note: '各項目は status=ok のときだけ数字がある。no_data は reason の理由で数字が無い (推測で埋めない)。'
      + '売上 (sales_yen 等) は Amazon の注文ベース (アクセスのレポート) を優先し、無ければ決済ベース。facts.sales / margin の値とは基準が違う',
  };
  if (p.suppressReason) {
    return { ...head, status: 'no_data', reason: p.suppressReason, available_items: [], no_data_items: ITEMS.map(([k]) => k) };
  }
  // 広告: 週次は 14 日の帰属が出そろった週 (今の期間を 14 日前にずらす)。月次はその月 (月末の数日は帰属が伸びる途中)
  const adsWindow = isMonth
    ? [p.periodStart, p.periodEnd]
    : [addDays(p.periodStart, -ADS_ATTRIB_SETTLE_DAYS), addDays(p.periodEnd, -ADS_ATTRIB_SETTLE_DAYS)];
  const ctx = {
    refDay,
    cur: [p.periodStart, p.periodEnd],
    prev: [p.prevStart, p.periodStart],
    baseline: p.baseline || [],
    labels,
    memo: new Map(),
    adsWindow,
    adsWindowNote: isMonth
      ? 'その月の広告。月末の数日の 14 日の帰属売上はまだ伸びる途中 (ROAS が低めに出る)'
      : `14 日の帰属売上が出そろった週 (今週の ${ADS_ATTRIB_SETTLE_DAYS} 日前の 7 日)。今週の広告そのものではない`,
  };
  const out = { ...head };
  const ok = [];
  const missing = [];
  for (const [key, fn] of ITEMS) {
    const v = guard(() => fn(db, ctx));
    out[key] = v;
    (v.status === 'ok' ? ok : missing).push(key);
  }
  out.available_items = ok;
  out.no_data_items = missing;
  return out;
}

/**
 * Amazon の売上・粗利が禁止領域 (sales:amazon / margin:amazon) の期間は節ごと出さない理由。
 * runner の出力検査 (weekly-prompt.js validateOutput) は、その期間に「amazon」に触れた summary / 論点を落とす
 * → 健康状態の数字を渡しても論点にできず、再生成とフォールバックを招くだけのため
 */
export function amazonHealthSuppressReason(constraints) {
  const hit = (constraints?.prohibited_topic_areas || [])
    .find((a) => a.area === 'sales:amazon' || a.area === 'margin:amazon');
  return hit ? `Amazon の ${hit.area.startsWith('sales') ? '売上' : '粗利'}が禁止領域 (${hit.area}) の期間のため出さない` : null;
}
