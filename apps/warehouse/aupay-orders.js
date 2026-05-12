/**
 * au PAY マーケット (Wow!manager) 受注データ取得 — ミニPC側
 *
 * VPS proxy 経由で au PAY マーケット API (searchTradeInfoListProc) からデータを取得し、
 * warehouse.db の raw_aupay_orders に投入する。Yahoo/楽天 Phase 1 と同型 (fail-closed 込み)。
 *
 * 使い方:
 *   node apps/warehouse/aupay-orders.js [days]              直近 N 日 (default 7)
 *   node apps/warehouse/aupay-orders.js backfill [startYmd] [endYmd]   YYYYMMDD (or YYYY-MM-DD)
 *
 * env:
 *   AUPAY_PROXY_URL    (default http://133.167.122.198:8080)
 *   AUPAY_PROXY_SECRET
 *   AUPAY_SHOP_ID      (default 54318092)
 *   AUPAY_API_KEY      (proxy を使わず直接アクセスする場合のみ)
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md
 * fail-closed: <error> / 0件異常 / ページネーション欠落 / 必須キー欠落 / schema drift を検知
 *   fetched>0 AND inserted=0 → exit 1、skip_ratio>5% → exit 1 (Yahoo Phase 1.3 同型)
 */
import 'dotenv/config';
import { parseStringPromise } from 'xml2js';
import { initDB, getDB, updateSyncMeta } from './db.js';

const AUPAY_PROXY_URL = process.env.AUPAY_PROXY_URL || 'http://133.167.122.198:8080';
const AUPAY_PROXY_SECRET = process.env.AUPAY_PROXY_SECRET || '';
const AUPAY_SHOP_ID = process.env.AUPAY_SHOP_ID || '54318092';
const AUPAY_API_KEY = process.env.AUPAY_API_KEY || '';
const AUPAY_BASE = AUPAY_PROXY_URL ? `${AUPAY_PROXY_URL}/wmshopapi` : 'https://api.manager.wowma.jp/wmshopapi';
const PAGE_SIZE = 100;

function now() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function ymd(d) { return d.toISOString().slice(0, 10).replace(/-/g, ''); }
function normYmd(s) { return String(s).replace(/-/g, ''); }
const numVal = v => { const n = parseFloat((v && typeof v === 'object') ? (v._ ?? '') : v); return isNaN(n) ? 0 : n; };
const intVal = v => { const n = parseInt((v && typeof v === 'object') ? (v._ ?? '') : v, 10); return isNaN(n) ? 0 : n; };
const strVal = v => ((v && typeof v === 'object') ? (v._ ?? '') : (v ?? '')) + '';
const arrify = v => v == null ? [] : (Array.isArray(v) ? v : [v]);

// ─── テーブル確認 / migration ───
// 旧スキーマ (mall-orders.js の 4 モール共通最小スキーマ) → 新スキーマ (Phase 1 拡張) へ移行。
// 旧スキーマには order_detail_id 列が無いので、それで判定。
function ensureTables() {
  const db = getDB();
  const cols = db.prepare("PRAGMA table_info(raw_aupay_orders)").all().map(c => c.name);
  const isNewSchema = cols.includes('order_detail_id');
  if (cols.length > 0 && !isNewSchema) {
    // 旧スキーマ検出 → DROP & 新スキーマで CREATE (既存最小データは捨てる、backfill で埋め直す)
    console.log('[auPay] 旧スキーマ raw_aupay_orders を検出 → 新スキーマで rebuild (既存データは backfill で埋め直し)');
    db.exec('DROP TABLE IF EXISTS raw_aupay_orders');
  }
  db.exec(`CREATE TABLE IF NOT EXISTS raw_aupay_orders (
    -- PK
    order_id            TEXT NOT NULL,
    order_detail_id     TEXT NOT NULL,
    -- 注文レベル
    order_date          TEXT,                -- 'YYYY/MM/DD HH:MM'
    site_and_device     TEXT,                -- "au PAY マーケット(SP(アプリ))" 等 (デバイス分析軸)
    settlement_name     TEXT,                -- 決済手段名
    order_status        TEXT,                -- 完了/発送待ち/キャンセル/発送前入金待ち/新規受付
    payment_status      TEXT, ship_status TEXT, cancel_status TEXT,
    authorization_status TEXT, contact_status TEXT, print_status TEXT,
    total_sale_price             REAL,       -- 商品計
    total_sale_price_normal_tax  REAL, total_sale_price_reduced_tax REAL, total_sale_price_no_tax REAL,
    postage_price                REAL, charge_price REAL,
    total_item_option_price      REAL, total_gift_wrapping_price REAL, total_price REAL,
    coupon_total_price           REAL,       -- ストアクーポン値引 (detail.discount とは別物)
    use_point                    REAL,       -- Ponta ポイント利用
    use_au_point_price           REAL,       -- au ポイント利用
    premium_issue_price          REAL, premium_mall_price REAL, premium_shop_price REAL,
    request_price                REAL,       -- 実請求額 = total_price - coupon - usePoint - useAuPointPrice
    delivery_name                TEXT, delivery_method_id TEXT,
    point_fixed_date             TEXT, point_fixed_status TEXT,
    -- 明細レベル
    item_code           TEXT,
    item_name           TEXT,
    item_option         TEXT,                -- = 旧 option_info (variant 情報含む、楽天 selected_choice 相当)
    before_discount     REAL,                -- 値引前単価
    discount            REAL,                -- 値引額 (item_price に既反映)
    item_price          REAL,                -- 値引後単価
    unit                INTEGER,             -- 数量
    total_item_price    REAL,                -- = item_price × unit
    tax_type            TEXT, reduced_tax REAL, tax_rate REAL,
    gift_point          REAL,                -- 付与ポイント (商品単位、率 ≈ 0.93%)
    item_cancel_status  TEXT,
    shipping_day_disp_text TEXT, shipping_timelimit_date TEXT,
    -- メタ
    synced_at           TEXT,
    PRIMARY KEY (order_id, order_detail_id)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_order_id ON raw_aupay_orders(order_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_date ON raw_aupay_orders(order_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_item ON raw_aupay_orders(item_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_status ON raw_aupay_orders(order_status)');
}

// ─── VPS proxy 呼び出し ───
async function proxyGet(path) {
  const headers = AUPAY_PROXY_URL
    ? { 'X-Proxy-Secret': AUPAY_PROXY_SECRET }
    : { 'Authorization': `Bearer ${AUPAY_API_KEY}` };
  const res = await fetch(`${AUPAY_BASE}${path}`, { headers });
  if (!res.ok) throw new Error(`Proxy error ${res.status}: ${await res.text()}`);
  return res.text();
}

// ─── searchTradeInfoListProc を期間指定でページング取得 ───
async function fetchOrdersInRange(startDate, endDate) {
  const orders = [];
  let startCount = 1;
  let totalAvailable = null;
  while (true) {
    const qs = new URLSearchParams({
      shopId: AUPAY_SHOP_ID, totalCount: String(PAGE_SIZE), startCount: String(startCount),
      startDate, endDate, dateType: '0',  // dateType=0 = 受注日
    });
    console.log(`[auPay] searchTradeInfoListProc: ${startDate}〜${endDate} (startCount=${startCount})`);
    const xml = await proxyGet(`/searchTradeInfoListProc?${qs}`);
    const parsed = await parseStringPromise(xml, { explicitArray: false });
    const resp = parsed?.response;
    // fail-closed: <error> 検知
    if (!resp || resp.result?.status !== '0') {
      const err = resp?.result?.error;
      throw new Error(`API error: code=${strVal(err?.code) || '?'} msg=${strVal(err?.message) || JSON.stringify(resp)}`);
    }
    const cnt = parseInt(resp.resultCount) || 0;
    if (totalAvailable == null) totalAvailable = parseInt(resp.totalCount ?? resp.totalResultsAvailable ?? '0') || null;
    if (cnt === 0) break;
    for (const oi of arrify(resp.orderInfo)) if (oi) orders.push(oi);
    if (cnt < PAGE_SIZE) break;
    startCount += PAGE_SIZE;
    await sleep(900);
  }
  return { orders, totalAvailable };
}

// ─── DB 投入 (注文単位 DELETE→INSERT、全フィールド) ───
function insertOrders(db, orders) {
  const ts = now();
  const del = db.prepare('DELETE FROM raw_aupay_orders WHERE order_id = ?');
  const ins = db.prepare(`INSERT INTO raw_aupay_orders (
    order_id, order_detail_id, order_date, site_and_device, settlement_name, order_status,
    payment_status, ship_status, cancel_status, authorization_status, contact_status, print_status,
    total_sale_price, total_sale_price_normal_tax, total_sale_price_reduced_tax, total_sale_price_no_tax,
    postage_price, charge_price, total_item_option_price, total_gift_wrapping_price, total_price,
    coupon_total_price, use_point, use_au_point_price, premium_issue_price, premium_mall_price, premium_shop_price, request_price,
    delivery_name, delivery_method_id, point_fixed_date, point_fixed_status,
    item_code, item_name, item_option, before_discount, discount, item_price, unit, total_item_price,
    tax_type, reduced_tax, tax_rate, gift_point, item_cancel_status, shipping_day_disp_text, shipping_timelimit_date,
    synced_at
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);

  let inserted = 0, skippedInvalid = 0;
  const tx = db.transaction(() => {
    for (const o of orders) {
      const orderId = strVal(o.orderId);
      if (!orderId) { skippedInvalid++; console.log('[auPay] skip: orderId 空'); continue; }
      // 必須キー欠落チェック (fail-closed)
      const orderDate = strVal(o.orderDate);
      const orderStatus = strVal(o.orderStatus);
      if (!orderDate || !orderStatus) { skippedInvalid++; console.log(`[auPay] skip ${orderId}: orderDate/orderStatus 空`); continue; }
      const details = arrify(o.detail);
      if (!details.length || !details[0]) { skippedInvalid++; console.log(`[auPay] skip ${orderId}: detail 空`); continue; }
      // 明細必須キーチェック
      let itemValid = true;
      for (const d of details) {
        const ic = strVal(d.itemCode), ip = numVal(d.itemPrice), un = intVal(d.unit), odi = strVal(d.orderDetailId);
        if (!ic || ip <= 0 || un <= 0 || !odi) { itemValid = false; console.log(`[auPay] skip ${orderId}: 明細キー欠落 (itemCode='${ic}' itemPrice=${ip} unit=${un} orderDetailId='${odi}')`); break; }
      }
      if (!itemValid) { skippedInvalid++; continue; }

      del.run(orderId);
      for (const d of details) {
        ins.run(
          orderId, strVal(d.orderDetailId), orderDate, strVal(o.siteAndDevice), strVal(o.settlementName), orderStatus,
          strVal(o.paymentStatus), strVal(o.shipStatus), strVal(o.cancelStatus), strVal(o.authorizationStatus), strVal(o.contactStatus), strVal(o.printStatus),
          numVal(o.totalSalePrice), numVal(o.totalSalePriceNormalTax), numVal(o.totalSalePriceReducedTax), numVal(o.totalSalePriceNoTax),
          numVal(o.postagePrice), numVal(o.chargePrice), numVal(o.totalItemOptionPrice), numVal(o.totalGiftWrappingPrice), numVal(o.totalPrice),
          numVal(o.couponTotalPrice), numVal(o.usePoint), numVal(o.useAuPointPrice), numVal(o.premiumIssuePrice), numVal(o.premiumMallPrice), numVal(o.premiumShopPrice), numVal(o.requestPrice),
          strVal(o.deliveryName), strVal(o.deliveryMethodId), strVal(o.pointFixedDate), strVal(o.pointFixedStatus),
          (strVal(d.itemCode) || '').toLowerCase(), strVal(d.itemName), strVal(d.itemOption), numVal(d.beforeDiscount), numVal(d.discount), numVal(d.itemPrice), intVal(d.unit), numVal(d.totalItemPrice),
          strVal(d.taxType), numVal(d.reducedTax), numVal(d.taxRate), numVal(d.giftPoint), strVal(d.itemCancelStatus), strVal(d.shippingDayDispText), strVal(d.shippingTimelimitDate),
          ts
        );
        inserted++;
      }
    }
  });
  tx();
  return { inserted, skippedInvalid };
}

// ─── fail-closed 判定 (Yahoo Phase 1.3 同型) ───
const SKIP_RATIO_FATAL = 0.05;
function evaluateFetchResult(label, fetched, inserted, skippedInvalid, totalAvailable) {
  const skipRatio = fetched > 0 ? skippedInvalid / fetched : 0;
  console.log(`[auPay] ${label}: fetched=${fetched} inserted=${inserted} skipped_invalid=${skippedInvalid} (skip_ratio=${(skipRatio * 100).toFixed(1)}%${totalAvailable != null ? `, totalAvailable=${totalAvailable}` : ''})`);
  // ページネーション欠落 (totalAvailable と取得件数の不一致)
  if (totalAvailable != null && totalAvailable > 0 && fetched < totalAvailable) {
    console.error(`[auPay] FATAL: ページネーション欠落 — fetched=${fetched} < totalAvailable=${totalAvailable}`);
    return 'fatal';
  }
  if (fetched > 0 && inserted === 0) {
    console.error(`[auPay] FATAL: fetched=${fetched} だが inserted=0 (API 障害 / schema drift の可能性)`);
    return 'fatal';
  }
  if (fetched > 0 && skipRatio > SKIP_RATIO_FATAL) {
    console.error(`[auPay] FATAL: skip_ratio=${(skipRatio * 100).toFixed(1)}% (${SKIP_RATIO_FATAL * 100}% 超、系統的問題の可能性)`);
    return 'fatal';
  }
  if (skippedInvalid > 0) {
    console.log(`[auPay] ⚠️  semantic warning: skipped_invalid=${skippedInvalid}`);
    return 'warning';
  }
  return 'ok';
}

// ─── メイン: 日次取得 ───
async function fetchAuPay(days = 7) {
  if (!AUPAY_PROXY_URL && !AUPAY_API_KEY) { console.log('[auPay] AUPAY_PROXY_URL / AUPAY_API_KEY 未設定'); return; }
  console.log(`[auPay] 受注取得開始 (直近 ${days} 日)`);
  const db = getDB();
  ensureTables();
  const end = new Date();
  const start = new Date(); start.setDate(start.getDate() - days);
  const { orders, totalAvailable } = await fetchOrdersInRange(ymd(start), ymd(end));
  const { inserted, skippedInvalid } = insertOrders(db, orders);
  updateSyncMeta('aupay_last_sync', now());
  console.log(`[auPay] 受注取得完了: ${orders.length} 注文 / ${inserted} 明細投入`);
  const verdict = evaluateFetchResult('fetchAuPay', orders.length, inserted, skippedInvalid, totalAvailable);
  if (verdict === 'fatal') process.exit(1);
  return inserted;
}

// ─── バックフィル (期間指定、月単位で遡る) ───
async function backfill(startYmdStr, endYmdStr) {
  const startStr = normYmd(startYmdStr), endStr = normYmd(endYmdStr);
  console.log(`[auPay] バックフィル: ${startStr}〜${endStr}`);
  const db = getDB();
  ensureTables();
  // au PAY API は期間指定で一度に取れる (orderList の上限はあるが totalCount でページング) ので、月単位で分割
  const startD = new Date(`${startStr.slice(0,4)}-${startStr.slice(4,6)}-${startStr.slice(6,8)}`);
  const endD = new Date(`${endStr.slice(0,4)}-${endStr.slice(4,6)}-${endStr.slice(6,8)}`);
  let totalFetched = 0, totalInserted = 0, totalSkipped = 0;
  let cur = new Date(endD);
  while (cur >= startD) {
    const monthEnd = new Date(cur);
    const monthStart = new Date(cur.getFullYear(), cur.getMonth(), 1);
    if (monthStart < startD) monthStart.setTime(startD.getTime());
    try {
      const { orders, totalAvailable } = await fetchOrdersInRange(ymd(monthStart), ymd(monthEnd));
      const { inserted, skippedInvalid } = insertOrders(db, orders);
      totalFetched += orders.length; totalInserted += inserted; totalSkipped += skippedInvalid;
      console.log(`[auPay] バックフィル ${ymd(monthStart)}-${ymd(monthEnd)}: ${orders.length} 注文 / ${inserted} 明細 (累計 ${totalInserted})`);
      if (totalAvailable != null && totalAvailable > 0 && orders.length < totalAvailable) {
        console.error(`[auPay] バックフィル WARNING: ${ymd(monthStart)}-${ymd(monthEnd)} で ${orders.length} < totalAvailable ${totalAvailable}`);
      }
      updateSyncMeta('aupay_backfill_progress', ymd(monthStart));
    } catch (e) {
      console.error(`[auPay] バックフィル エラー ${ymd(monthStart)}: ${e.message}`);
      console.error('[auPay] 途中再開可能: sync_meta.aupay_backfill_progress 確認');
      break;
    }
    cur = new Date(monthStart.getFullYear(), monthStart.getMonth() - 1, monthStart.getDate());
    await sleep(2000);
  }
  console.log(`[auPay] バックフィル完了: ${totalFetched} 注文 / ${totalInserted} 明細`);
  const verdict = evaluateFetchResult('backfill', totalFetched, totalInserted, totalSkipped, null);
  if (verdict === 'fatal') process.exit(1);
  return totalInserted;
}

// ─── エントリポイント ───
async function main() {
  const args = process.argv.slice(2);
  await initDB();
  if (args[0] === 'backfill') {
    const start = args[1] || ymd(new Date(Date.now() - 90 * 86400000));
    const end = args[2] || ymd(new Date());
    await backfill(start, end);
  } else {
    await fetchAuPay(parseInt(args[0]) || 7);
  }
}

export { fetchAuPay, backfill };

main().catch(e => { console.error('[auPay] エラー:', e.message); process.exit(1); });
