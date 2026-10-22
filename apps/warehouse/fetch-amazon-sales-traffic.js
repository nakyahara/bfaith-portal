/**
 * fetch-amazon-sales-traffic.js — Amazon のアクセス・転換率 (ビジネスレポート) を日 × SKU で取る (AI の分析とアラート用・2026-10-22)
 *
 * レポート: GET_SALES_AND_TRAFFIC_REPORT (JSON)・reportOptions { dateGranularity: 'DAY', asinGranularity: 'SKU' }
 *   - salesAndTrafficByDate = 店全体の日ごと / salesAndTrafficByAsin = SKU (子 ASIN) ごと。🚨 ByAsin は頼んだ期間全体の合計で日が付かない
 *     → **1 日 = 1 レポート** (dataStartTime と dataEndTime をその日の始まりと終わり) で取る
 *   - 数字は後から確定する (Amazon が数日かけて直す) = ふだんは直近 5 日を毎朝取り直す。初回のさかのぼりは --from / --to
 *
 * 保存 (warehouse.db・この取得が CREATE TABLE IF NOT EXISTS で作る):
 *   amazon_sales_traffic_sku_daily  日 × SKU × 子 ASIN (sessions・pageViews・buyBoxPercentage・unitSessionPercentage・unitsOrdered・orderedProductSales・totalOrderItems と B2B)
 *   amazon_sales_traffic_daily      日の店全体 (salesAndTrafficByDate)
 *   amazon_sales_traffic_raw        元の JSON を日ごとに gzip で保存 (解釈を後で直せるように)
 *   sp_report_fetch_days            取得の完全性の記録 (sp-api-report.js)
 *
 * 🚨 決まり (fetch-amazon-ads.js と同じ考え方):
 *   - **日ごとに置き換える**: その日のレポートを最後まで取れて検査も通ったときだけ、その日の行を消して入れ直す (1 取引)
 *   - **値の検査**: 数でない・負の値・SKU や子 ASIN の無い行・SKU の重複・日付の合わない店全体の行があれば、その日は書かずに失敗
 *   - 項目が無ければ null (0 にしない)。項目名は公式のレポートの定義 (salesAndTrafficReport の JSON schema) のまま読む
 *   - 世代 (頼んだ時刻) が記録より古い取得では上書きしない
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-sales-traffic.js                         # JST の昨日まで直近 5 日
 *   node apps/warehouse/fetch-amazon-sales-traffic.js --days 10
 *   node apps/warehouse/fetch-amazon-sales-traffic.js --from 2026-07-01 --to 2026-07-31   # さかのぼり (1 日 = 1 レポート・混むと 1 分に 1 つ)
 *   node apps/warehouse/fetch-amazon-sales-traffic.js --dry-run                # 取る日を出すだけ (API を呼ばない)
 *   node apps/warehouse/fetch-amazon-sales-traffic.js --check --limit 1        # 1 日だけ取って検査し、要約を出す (DB に書かない)
 *   --concurrency N          同時に頼む日の数 (既定 3・1〜10)
 *   --boundary jst|utc|date  dataStartTime/dataEndTime の書き方 (既定 jst = その日の 00:00:00+09:00 〜 23:59:59+09:00)。
 *                            店全体の行の日付が頼んだ日と合わずに失敗するときに試す
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import {
  fetchSpReport, createSpClient, isDirectRun, parseCommonArgs, datesBetween, jstStart, jstEnd,
  numOrNull, strOrNull, ensureReportFetchDays, createRateGate, dayWriteDecision, recordFetchDay, checkSaveArgs, gzipText,
} from './sp-api-report.js';

export const REPORT_TYPE = 'GET_SALES_AND_TRAFFIC_REPORT';
export const REPORT_OPTIONS = Object.freeze({ dateGranularity: 'DAY', asinGranularity: 'SKU' });
const DEFAULT_DAYS = 5;
const POLL_INTERVAL_MS = 20000;
const MAX_POLLS = 45;   // 20 秒 × 45 = 15 分
const DEFAULT_CONCURRENCY = 3;   // 同時に頼むレポートの数 (--concurrency で 1〜10)
// 🚨 このレポートは「5 分に 3 回まで」(公式の report type values: Sales and Traffic Business Report。Reports API 全体の burst 15 とは別。Codex R3)。
//    少し余裕を持たせて 5 分 15 秒に 3 回 = 直近 5 日の朝は 3 つ → 5 分待って 2 つ (約 5〜10 分)・90 日のさかのぼりは約 2.6 時間
export const CREATE_QUOTA = Object.freeze({ max: 3, windowMs: 315000 });

/** 頼む期間の書き方 */
export function dayWindow(d, boundary = 'jst') {
  if (boundary === 'jst') return { dataStartTime: jstStart(d), dataEndTime: jstEnd(d) };
  if (boundary === 'utc') return { dataStartTime: `${d}T00:00:00Z`, dataEndTime: `${d}T23:59:59Z` };
  if (boundary === 'date') return { dataStartTime: d, dataEndTime: d };
  throw new Error(`--boundary が不正: ${boundary} (jst / utc / date)`);
}

export function ensureSalesTrafficTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_sales_traffic_sku_daily (
    date_jst                      TEXT NOT NULL,
    seller_sku                    TEXT NOT NULL,   -- Amazon の出品者 SKU (大文字小文字はレポートのまま。突き合わせは COLLATE NOCASE で)
    child_asin                    TEXT NOT NULL,
    parent_asin                   TEXT,
    sessions                      INTEGER,         -- trafficByAsin.sessions (無ければ NULL。0 にしない)
    sessions_b2b                  INTEGER,
    browser_sessions              INTEGER,
    mobile_app_sessions           INTEGER,
    session_percentage            REAL,
    page_views                    INTEGER,
    page_views_b2b                INTEGER,
    browser_page_views            INTEGER,
    mobile_app_page_views         INTEGER,
    page_views_percentage         REAL,
    buy_box_percentage            REAL,
    buy_box_percentage_b2b        REAL,
    unit_session_percentage       REAL,            -- 転換率 (ユニットセッション率・%)
    unit_session_percentage_b2b   REAL,
    units_ordered                 INTEGER,
    units_ordered_b2b             INTEGER,
    ordered_product_sales         REAL,            -- 注文商品売上 (amount)
    ordered_product_sales_b2b     REAL,
    currency_code                 TEXT,
    total_order_items             INTEGER,
    total_order_items_b2b         INTEGER,
    report_id                     TEXT NOT NULL,
    generation                    INTEGER NOT NULL,
    fetched_at                    TEXT NOT NULL,
    PRIMARY KEY (date_jst, seller_sku, child_asin)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_astsd_sku ON amazon_sales_traffic_sku_daily(seller_sku, date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_astsd_asin ON amazon_sales_traffic_sku_daily(child_asin, date_jst)');
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_sales_traffic_daily (
    date_jst                          TEXT PRIMARY KEY,
    ordered_product_sales             REAL,
    ordered_product_sales_b2b         REAL,
    currency_code                     TEXT,
    units_ordered                     INTEGER,
    units_ordered_b2b                 INTEGER,
    total_order_items                 INTEGER,
    total_order_items_b2b             INTEGER,
    units_refunded                    INTEGER,
    refund_rate                       REAL,
    units_shipped                     INTEGER,
    orders_shipped                    INTEGER,
    shipped_product_sales             REAL,
    claims_granted                    INTEGER,
    claims_amount                     REAL,
    sessions                          INTEGER,
    sessions_b2b                      INTEGER,
    browser_sessions                  INTEGER,
    mobile_app_sessions               INTEGER,
    page_views                        INTEGER,
    page_views_b2b                    INTEGER,
    browser_page_views                INTEGER,
    mobile_app_page_views             INTEGER,
    buy_box_percentage                REAL,
    buy_box_percentage_b2b            REAL,
    order_item_session_percentage     REAL,
    order_item_session_percentage_b2b REAL,
    unit_session_percentage           REAL,
    unit_session_percentage_b2b       REAL,
    average_offer_count               REAL,
    average_parent_items              REAL,
    feedback_received                 INTEGER,
    negative_feedback_received        INTEGER,
    received_negative_feedback_rate   REAL,
    report_id                         TEXT NOT NULL,
    generation                        INTEGER NOT NULL,
    fetched_at                        TEXT NOT NULL
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_sales_traffic_raw (
    date_jst    TEXT PRIMARY KEY,
    report_id   TEXT NOT NULL,
    generation  INTEGER NOT NULL,
    json_gzip   BLOB NOT NULL,      -- 元の JSON (gzip)。解釈を直したら ここから作り直せる
    json_bytes  INTEGER NOT NULL,
    fetched_at  TEXT NOT NULL
  )`);
  ensureReportFetchDays(db);
}

/** { amount, currencyCode } の amount (無ければ null) */
function money(m, label) {
  if (m === undefined || m === null) return { amount: null, currency: null };
  if (typeof m !== 'object') throw new Error(`${label} が金額の形 ({ amount, currencyCode }) でない: ${JSON.stringify(m)}`);
  return { amount: numOrNull(m.amount, `${label}.amount`), currency: strOrNull(m.currencyCode) };
}
const obj = (v, label) => {
  if (v === undefined || v === null) return {};
  if (typeof v !== 'object' || Array.isArray(v)) throw new Error(`${label} が項目の形でない`);
  return v;
};

/**
 * レポートの JSON を検査して 1 日分の行にする。読めない値があれば例外 (その日は書かない)
 * @returns {{ store: object, skus: object[] }}
 */
export function parseSalesTrafficDay(json, date) {
  if (!json || typeof json !== 'object' || Array.isArray(json)) throw new Error('レポートの形が想定外 (JSON の項目の形でない)');
  const byDate = json.salesAndTrafficByDate;
  const byAsin = json.salesAndTrafficByAsin;
  // 🚨 どちらの配列も必須 (公式の schema)。無い = 途中までのレポート = 失敗 (SKU の行を空で置き換えない。Codex R2)。空の配列 [] は受ける
  if (!Array.isArray(byDate)) throw new Error('salesAndTrafficByDate が無い・配列でない');
  if (!Array.isArray(byAsin)) throw new Error('salesAndTrafficByAsin が無い・配列でない');
  const dates = byDate.map((x) => x?.date);
  // 🚨 店全体の行は頼んだ日の 1 行だけ。無い (まだ集計されていない?) / 別の日がある (期間の書き方の時差のずれ) = 失敗 (空で置き換えない)
  if (dates.length !== 1 || dates[0] !== date) {
    throw new Error(`店全体の行 (salesAndTrafficByDate) の日付が頼んだ日 ${date} の 1 行でない: ${JSON.stringify(dates.slice(0, 5))}${dates.length > 5 ? ' …' : ''}`);
  }
  const d = byDate[0];
  const sd = obj(d.salesByDate, 'salesByDate');
  const td = obj(d.trafficByDate, 'trafficByDate');
  const I = (o, k, at) => numOrNull(o[k], `${at}.${k}`, { integer: true });
  const N = (o, k, at) => numOrNull(o[k], `${at}.${k}`);
  const ops = money(sd.orderedProductSales, 'salesByDate.orderedProductSales');
  const store = {
    date,
    ordered_product_sales: ops.amount,
    ordered_product_sales_b2b: money(sd.orderedProductSalesB2B, 'salesByDate.orderedProductSalesB2B').amount,
    currency_code: ops.currency,
    units_ordered: I(sd, 'unitsOrdered', 'salesByDate'),
    units_ordered_b2b: I(sd, 'unitsOrderedB2B', 'salesByDate'),
    total_order_items: I(sd, 'totalOrderItems', 'salesByDate'),
    total_order_items_b2b: I(sd, 'totalOrderItemsB2B', 'salesByDate'),
    units_refunded: I(sd, 'unitsRefunded', 'salesByDate'),
    refund_rate: N(sd, 'refundRate', 'salesByDate'),
    units_shipped: I(sd, 'unitsShipped', 'salesByDate'),
    orders_shipped: I(sd, 'ordersShipped', 'salesByDate'),
    shipped_product_sales: money(sd.shippedProductSales, 'salesByDate.shippedProductSales').amount,
    claims_granted: I(sd, 'claimsGranted', 'salesByDate'),
    claims_amount: money(sd.claimsAmount, 'salesByDate.claimsAmount').amount,
    sessions: I(td, 'sessions', 'trafficByDate'),
    sessions_b2b: I(td, 'sessionsB2B', 'trafficByDate'),
    browser_sessions: I(td, 'browserSessions', 'trafficByDate'),
    mobile_app_sessions: I(td, 'mobileAppSessions', 'trafficByDate'),
    page_views: I(td, 'pageViews', 'trafficByDate'),
    page_views_b2b: I(td, 'pageViewsB2B', 'trafficByDate'),
    browser_page_views: I(td, 'browserPageViews', 'trafficByDate'),
    mobile_app_page_views: I(td, 'mobileAppPageViews', 'trafficByDate'),
    buy_box_percentage: N(td, 'buyBoxPercentage', 'trafficByDate'),
    buy_box_percentage_b2b: N(td, 'buyBoxPercentageB2B', 'trafficByDate'),
    order_item_session_percentage: N(td, 'orderItemSessionPercentage', 'trafficByDate'),
    order_item_session_percentage_b2b: N(td, 'orderItemSessionPercentageB2B', 'trafficByDate'),
    unit_session_percentage: N(td, 'unitSessionPercentage', 'trafficByDate'),
    unit_session_percentage_b2b: N(td, 'unitSessionPercentageB2B', 'trafficByDate'),
    average_offer_count: N(td, 'averageOfferCount', 'trafficByDate'),
    average_parent_items: N(td, 'averageParentItems', 'trafficByDate'),
    feedback_received: I(td, 'feedbackReceived', 'trafficByDate'),
    negative_feedback_received: I(td, 'negativeFeedbackReceived', 'trafficByDate'),
    received_negative_feedback_rate: N(td, 'receivedNegativeFeedbackRate', 'trafficByDate'),
  };

  const seen = new Set();
  const skus = byAsin.map((r, i) => {
    const at = `salesAndTrafficByAsin[${i}]`;
    if (!r || typeof r !== 'object') throw new Error(`${at} が行の形でない`);
    const sku = strOrNull(r.sku);
    const childAsin = strOrNull(r.childAsin);
    if (!sku) throw new Error(`${at} に sku が無い (asinGranularity=SKU のはず)`);
    if (!childAsin) throw new Error(`${at} (${sku}) に childAsin が無い`);
    const key = `${sku}\u0000${childAsin}`;
    if (seen.has(key)) throw new Error(`${at}: SKU ${sku} / ${childAsin} が重複`);
    seen.add(key);
    const sa = obj(r.salesByAsin, `${at}.salesByAsin`);
    const ta = obj(r.trafficByAsin, `${at}.trafficByAsin`);
    const sales = money(sa.orderedProductSales, `${at}.salesByAsin.orderedProductSales`);
    return {
      date, seller_sku: sku, child_asin: childAsin, parent_asin: strOrNull(r.parentAsin),
      sessions: I(ta, 'sessions', at), sessions_b2b: I(ta, 'sessionsB2B', at),
      browser_sessions: I(ta, 'browserSessions', at), mobile_app_sessions: I(ta, 'mobileAppSessions', at),
      session_percentage: N(ta, 'sessionPercentage', at),
      page_views: I(ta, 'pageViews', at), page_views_b2b: I(ta, 'pageViewsB2B', at),
      browser_page_views: I(ta, 'browserPageViews', at), mobile_app_page_views: I(ta, 'mobileAppPageViews', at),
      page_views_percentage: N(ta, 'pageViewsPercentage', at),
      buy_box_percentage: N(ta, 'buyBoxPercentage', at), buy_box_percentage_b2b: N(ta, 'buyBoxPercentageB2B', at),
      unit_session_percentage: N(ta, 'unitSessionPercentage', at), unit_session_percentage_b2b: N(ta, 'unitSessionPercentageB2B', at),
      units_ordered: I(sa, 'unitsOrdered', at), units_ordered_b2b: I(sa, 'unitsOrderedB2B', at),
      ordered_product_sales: sales.amount,
      ordered_product_sales_b2b: money(sa.orderedProductSalesB2B, `${at}.salesByAsin.orderedProductSalesB2B`).amount,
      currency_code: sales.currency,
      total_order_items: I(sa, 'totalOrderItems', at), total_order_items_b2b: I(sa, 'totalOrderItemsB2B', at),
    };
  });
  // 店全体では注文があるのに SKU の行が 0 = 食い違い (途中のレポートを疑う) = 失敗 (今の SKU の行を消さない)
  if (skus.length === 0 && ((store.units_ordered || 0) > 0 || (store.sessions || 0) > 0)) {
    throw new Error(`SKU の行が 0 なのに店全体は注文 ${store.units_ordered} 個・セッション ${store.sessions} = 食い違い (書かない)`);
  }
  return { store, skus };
}

const SKU_COLS = ['seller_sku', 'child_asin', 'parent_asin', 'sessions', 'sessions_b2b', 'browser_sessions', 'mobile_app_sessions', 'session_percentage',
  'page_views', 'page_views_b2b', 'browser_page_views', 'mobile_app_page_views', 'page_views_percentage', 'buy_box_percentage', 'buy_box_percentage_b2b',
  'unit_session_percentage', 'unit_session_percentage_b2b', 'units_ordered', 'units_ordered_b2b', 'ordered_product_sales', 'ordered_product_sales_b2b',
  'currency_code', 'total_order_items', 'total_order_items_b2b'];
const STORE_COLS = ['ordered_product_sales', 'ordered_product_sales_b2b', 'currency_code', 'units_ordered', 'units_ordered_b2b', 'total_order_items',
  'total_order_items_b2b', 'units_refunded', 'refund_rate', 'units_shipped', 'orders_shipped', 'shipped_product_sales', 'claims_granted', 'claims_amount',
  'sessions', 'sessions_b2b', 'browser_sessions', 'mobile_app_sessions', 'page_views', 'page_views_b2b', 'browser_page_views', 'mobile_app_page_views',
  'buy_box_percentage', 'buy_box_percentage_b2b', 'order_item_session_percentage', 'order_item_session_percentage_b2b', 'unit_session_percentage',
  'unit_session_percentage_b2b', 'average_offer_count', 'average_parent_items', 'feedback_received', 'negative_feedback_received', 'received_negative_feedback_rate'];

/**
 * 1 日分を置き換える (1 取引)。検査はここで先に済ませる (例外なら何も書かない)
 * @returns {{ status: 'written'|'older'|'same', skuRows: number }}
 */
export function saveSalesTrafficDay(db, { date, json, rawText, generation, reportId, now = () => new Date().toISOString() }) {
  checkSaveArgs({ generation, reportId });
  ensureSalesTrafficTables(db);
  const parsed = parseSalesTrafficDay(json, date);   // 例外ならここで止まる = 何も書かない
  const raw = rawText ?? JSON.stringify(json);
  const ts = now();
  const delSku = db.prepare('DELETE FROM amazon_sales_traffic_sku_daily WHERE date_jst = ?');
  const insSku = db.prepare(`INSERT INTO amazon_sales_traffic_sku_daily (date_jst, ${SKU_COLS.join(', ')}, report_id, generation, fetched_at)
    VALUES (?, ${SKU_COLS.map(() => '?').join(', ')}, ?, ?, ?)`);
  const upStore = db.prepare(`INSERT OR REPLACE INTO amazon_sales_traffic_daily (date_jst, ${STORE_COLS.join(', ')}, report_id, generation, fetched_at)
    VALUES (?, ${STORE_COLS.map(() => '?').join(', ')}, ?, ?, ?)`);
  const upRaw = db.prepare('INSERT OR REPLACE INTO amazon_sales_traffic_raw (date_jst, report_id, generation, json_gzip, json_bytes, fetched_at) VALUES (?, ?, ?, ?, ?, ?)');
  let status = 'written';
  db.transaction(() => {
    const decision = dayWriteDecision(db, REPORT_TYPE, date, generation, reportId);
    if (decision !== 'write') { status = decision; return; }
    delSku.run(date);
    for (const r of parsed.skus) insSku.run(date, ...SKU_COLS.map((c) => r[c]), String(reportId), generation, ts);
    upStore.run(date, ...STORE_COLS.map((c) => parsed.store[c]), String(reportId), generation, ts);
    upRaw.run(date, String(reportId), generation, gzipText(raw), Buffer.byteLength(raw, 'utf8'), ts);
    recordFetchDay(db, { reportType: REPORT_TYPE, date, generation, reportId, windowFrom: date, windowTo: date, rowCount: parsed.skus.length, fetchedAt: ts });
  }).immediate();
  return { status, skuRows: status === 'written' ? parsed.skus.length : 0 };
}

export function parseArgs(argv = process.argv.slice(2), nowMs = Date.now()) {
  const r = parseCommonArgs(argv, { defaultDays: DEFAULT_DAYS, maxDays: 730, nowMs });   // dataStartTime は 2 年より前を頼めない (公式)
  r.boundary = r.extra.boundary || 'jst';
  r.concurrency = r.extra.concurrency === undefined ? DEFAULT_CONCURRENCY : Number(r.extra.concurrency);
  if (!Number.isInteger(r.concurrency) || r.concurrency < 1 || r.concurrency > 10) throw new Error(`--concurrency が不正 (1〜10): ${r.extra.concurrency}`);
  dayWindow('2026-01-01', r.boundary);   // 不正ならここで例外
  return r;
}

async function main() {
  const args = parseArgs();
  let days = datesBetween(args.from, args.to);
  if (args.limit > 0) days = days.slice(-args.limit);   // --limit N = 新しい方から N 日だけ
  console.log(`[SalesTraffic] 取る日: ${days[0]}〜${days[days.length - 1]} (${days.length} 日・1 日 = 1 レポート・期間の書き方 ${args.boundary})`);
  if (args.dryRun) { console.log('[SalesTraffic] --dry-run のため API を呼ばない'); return; }

  const db = args.check ? null : (await initDB(), getDB());
  if (db) ensureSalesTrafficTables(db);
  const sp = await createSpClient();
  let ok = 0, failed = 0, skipped = 0, rows = 0;
  // 何日かを同時に頼む (1 日ずつ順に待つと 5 日で 5 倍かかる)。createReport の burst (15) の内に収まる数にする
  const gate = createRateGate({ ...CREATE_QUOTA, log: console.log });
  const queue = [...days];
  const worker = async () => { for (let d = queue.shift(); d; d = queue.shift()) await oneDay(d); };
  const oneDay = async (d) => {
    try {
      const rep = await fetchSpReport({ sp, reportType: REPORT_TYPE, format: 'json', reportOptions: { ...REPORT_OPTIONS }, ...dayWindow(d, args.boundary),
        pollIntervalMs: POLL_INTERVAL_MS, maxPolls: MAX_POLLS, beforeCreate: gate });
      if (args.check) {
        const p = parseSalesTrafficDay(rep.json, d);
        console.log(`[SalesTraffic] --check ${d}: SKU ${p.skus.length} 行 / 店全体 sessions=${p.store.sessions} units=${p.store.units_ordered} sales=${p.store.ordered_product_sales} ${p.store.currency_code || ''}`);
        console.log(`  見本: ${JSON.stringify(p.skus.slice(0, 2))}`);
        ok++;
        return;
      }
      const r = saveSalesTrafficDay(db, { date: d, json: rep.json, rawText: rep.text, generation: rep.generation, reportId: rep.reportId });
      if (r.status === 'written') { ok++; rows += r.skuRows; console.log(`[SalesTraffic] ✅ ${d}: ${r.skuRows} SKU を置き換え`); }
      else { skipped++; console.log(`[SalesTraffic] ${d}: ${r.status === 'older' ? 'もっと新しい取得がある' : '同じレポートの取り直し'} = 触らない`); }
    } catch (e) {
      failed++;
      console.error(`[SalesTraffic] ❌ ${d}: ${e.message}`);
    }
  };
  await Promise.all(Array.from({ length: Math.min(args.concurrency, days.length) }, worker));
  const summary = `[SalesTraffic] ${failed ? '❌' : '✅'} ${ok}/${days.length} 日${args.check ? ' 検査 (書かない)' : ` 置き換え (${rows} 行)`}${skipped ? ` / 触らない ${skipped} 日` : ''}${failed ? ` / 失敗 ${failed} 日` : ''}`;
  console.log(summary);
  // 1 日でも失敗 = exit 1 (daily-sync が ❌ にする。取れた日は書いてある = 次の朝にまた直近を取り直す)
  process.exitCode = failed ? 1 : 0;
}

if (await isDirectRun(import.meta.url)) {
  main().catch((e) => { console.error('[SalesTraffic] FATAL:', e?.message || e); process.exitCode = 1; });
}
