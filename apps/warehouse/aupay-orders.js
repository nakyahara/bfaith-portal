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
// API レスポンス detail/order の全フィールドを保存 (中原さん方針「取れる情報は全部とっておく」、2026-05-13)。
// 旧スキーマ (item_management_id 列なし = Phase 1 A-0 の最小拡張版) を検出したら DROP & 新スキーマで rebuild。
// item_management_id は variant 解決 (Yahoo の sub_code 相当) に必須なので、これを判定キーにする。
function ensureTables() {
  const db = getDB();
  const cols = db.prepare("PRAGMA table_info(raw_aupay_orders)").all().map(c => c.name);
  const hasMgmtId = cols.includes('item_management_id');
  if (cols.length > 0 && !hasMgmtId) {
    // 旧スキーマ検出 → DROP & 新スキーマで CREATE (既存データは捨てる、backfill で埋め直す)
    console.log('[auPay] 旧スキーマ raw_aupay_orders 検出 (item_management_id 列なし) → 新スキーマで rebuild (既存データは backfill で埋め直し)');
    db.exec('DROP TABLE IF EXISTS raw_aupay_orders');
  }
  db.exec(`CREATE TABLE IF NOT EXISTS raw_aupay_orders (
    -- PK
    order_id            TEXT NOT NULL,
    order_detail_id     TEXT NOT NULL,

    -- ─── 注文レベル: 基本 ───
    order_date                TEXT,           -- 'YYYY/MM/DD HH:MM'
    sell_method_segment       TEXT,
    site_and_device           TEXT,           -- "au PAY マーケット(SP(アプリ))" 等
    cross_border_ec_trade_kbn TEXT,

    -- ─── 注文レベル: 顧客情報 (PII) ───
    mail_address          TEXT,
    orderer_name          TEXT,
    orderer_kana          TEXT,
    orderer_zip_code      TEXT,
    orderer_address       TEXT,
    orderer_phone_number1 TEXT,
    orderer_phone_number2 TEXT,
    nickname              TEXT,
    sender_name           TEXT,
    sender_kana           TEXT,
    sender_zip_code       TEXT,
    sender_address        TEXT,
    sender_phone_number1  TEXT,
    sender_phone_number2  TEXT,

    -- ─── 注文レベル: オプション/コメント ───
    order_option   TEXT,
    user_comment   TEXT,
    trade_remarks  TEXT,
    memo           TEXT,

    -- ─── 注文レベル: 決済 + ステータス ───
    settlement_name      TEXT,
    order_status         TEXT,
    contact_status       TEXT,
    authorization_status TEXT,
    payment_status       TEXT,
    ship_status          TEXT,
    print_status         TEXT,
    cancel_status        TEXT,

    -- ─── 注文レベル: 商品計売上 (税率別) ───
    total_sale_price              REAL,
    total_sale_price_normal_tax   REAL,
    total_sale_price_reduced_tax  REAL,
    total_sale_price_no_tax       REAL,
    total_sale_unit               INTEGER,

    -- ─── 注文レベル: 送料 / 代引手数料 ───
    postage_price          REAL,
    postage_price_tax_rate REAL,
    charge_price           REAL,
    charge_price_tax_rate  REAL,

    -- ─── 注文レベル: オプション料 / ラッピング料 (注文計) ───
    total_item_option_price            REAL,
    total_item_option_price_tax_rate   REAL,
    total_gift_wrapping_price          REAL,
    total_gift_wrapping_price_tax_rate REAL,

    -- ─── 注文レベル: 合計 (税率別) ───
    total_price             REAL,
    total_price_normal_tax  REAL,
    total_price_reduced_tax REAL,
    total_price_no_tax      REAL,

    -- ─── 注文レベル: プレミアム / Pontaパス ───
    premium_member               TEXT,
    premium_type                 TEXT,
    premium_issue_price          REAL,
    premium_mall_price           REAL,
    premium_shop_price           REAL,
    pontapass_campaign_apply_flg TEXT,

    -- ─── 注文レベル: クーポン (税率別) ───
    coupon_total_price             REAL,      -- ストアクーポン値引 (detail.discount とは別物)
    coupon_total_price_normal_tax  REAL,
    coupon_total_price_reduced_tax REAL,
    coupon_total_price_no_tax      REAL,

    -- ─── 注文レベル: Ponta ポイント利用 (税率別) ───
    use_point             REAL,
    use_point_normal_tax  REAL,
    use_point_reduced_tax REAL,
    use_point_no_tax      REAL,

    -- ─── 注文レベル: au ポイント利用 (税率別 + ポイント数) ───
    use_au_point_price             REAL,
    use_au_point_price_normal_tax  REAL,
    use_au_point_price_reduced_tax REAL,
    use_au_point_price_no_tax      REAL,
    use_au_point                   REAL,

    -- ─── 注文レベル: 請求 (税率別 + 税額別) ───
    request_price                  REAL,      -- 実請求額 = total_price - coupon - usePoint - useAuPointPrice
    request_price_normal_tax       REAL,
    request_tax_price_normal_tax   REAL,
    request_price_reduced_tax      REAL,
    request_tax_price_reduced_tax  REAL,
    request_price_no_tax           REAL,
    request_tax_price_no_tax       REAL,

    -- ─── 注文レベル: ポイント確定 ───
    point_fixed_date   TEXT,
    point_fixed_status TEXT,

    -- ─── 注文レベル: 決済処理結果 (PG) ───
    settle_status                TEXT,
    pg_result                    TEXT,
    pg_order_id                  TEXT,
    pg_request_price             REAL,
    pg_request_price_normal_tax  REAL,
    pg_request_price_reduced_tax REAL,
    pg_request_price_no_tax      REAL,

    -- ─── 注文レベル: 配送 / 電子領収書 ───
    delivery_name             TEXT,
    delivery_method_id        TEXT,
    elec_receipt_issue_status TEXT,
    elec_receipt_issue_times  INTEGER,

    -- ─── 明細レベル ───
    item_management_id      TEXT,            -- ★variant 識別 (m_products 子SKU suffix と対応、例 '-wt' / 'co')
    item_code               TEXT,
    lotnumber               TEXT,
    item_name               TEXT,
    item_option             TEXT,            -- 表示用 variant 文字列 '香り=金木犀'
    item_option_price       REAL,            -- 商品単位のオプション料
    gift_wrapping_price     REAL,            -- 商品単位のラッピング料
    gift_message            TEXT,
    noshi_presenter_name1   TEXT,
    noshi_presenter_name2   TEXT,
    noshi_presenter_name3   TEXT,
    item_cancel_status      TEXT,
    before_discount         REAL,            -- 値引前単価
    discount                REAL,            -- 値引額 (item_price に既反映)
    item_price              REAL,            -- 値引後単価
    unit                    INTEGER,         -- 数量
    total_item_price        REAL,            -- = item_price × unit
    total_item_charge_price REAL,            -- 商品単位の代引手数料
    tax_type                TEXT,
    reduced_tax             REAL,
    tax_rate                REAL,
    gift_point              REAL,            -- 付与ポイント (商品単位、率 ≈ 0.93%)
    shipping_day_disp_text  TEXT,
    shipping_timelimit_date TEXT,

    -- メタ
    synced_at TEXT,
    PRIMARY KEY (order_id, order_detail_id)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_order_id ON raw_aupay_orders(order_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_date ON raw_aupay_orders(order_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_item ON raw_aupay_orders(item_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_status ON raw_aupay_orders(order_status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_aupay_mgmt ON raw_aupay_orders(item_management_id)');
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
  // 注文 114 列 (2 PK + 86 注文 + 25 detail + 1 メタ) — CREATE TABLE と同順
  const COLS = [
    'order_id', 'order_detail_id',
    // 注文レベル: 基本
    'order_date', 'sell_method_segment', 'site_and_device', 'cross_border_ec_trade_kbn',
    // 注文レベル: 顧客情報 (PII)
    'mail_address', 'orderer_name', 'orderer_kana', 'orderer_zip_code', 'orderer_address',
    'orderer_phone_number1', 'orderer_phone_number2', 'nickname',
    'sender_name', 'sender_kana', 'sender_zip_code', 'sender_address',
    'sender_phone_number1', 'sender_phone_number2',
    // 注文レベル: オプション/コメント
    'order_option', 'user_comment', 'trade_remarks', 'memo',
    // 注文レベル: 決済 + ステータス
    'settlement_name', 'order_status', 'contact_status', 'authorization_status',
    'payment_status', 'ship_status', 'print_status', 'cancel_status',
    // 商品計売上 (税率別)
    'total_sale_price', 'total_sale_price_normal_tax', 'total_sale_price_reduced_tax', 'total_sale_price_no_tax', 'total_sale_unit',
    // 送料 / 代引手数料
    'postage_price', 'postage_price_tax_rate', 'charge_price', 'charge_price_tax_rate',
    // オプション料 / ラッピング料 (注文計)
    'total_item_option_price', 'total_item_option_price_tax_rate', 'total_gift_wrapping_price', 'total_gift_wrapping_price_tax_rate',
    // 合計 (税率別)
    'total_price', 'total_price_normal_tax', 'total_price_reduced_tax', 'total_price_no_tax',
    // プレミアム / Pontaパス
    'premium_member', 'premium_type', 'premium_issue_price', 'premium_mall_price', 'premium_shop_price', 'pontapass_campaign_apply_flg',
    // クーポン (税率別)
    'coupon_total_price', 'coupon_total_price_normal_tax', 'coupon_total_price_reduced_tax', 'coupon_total_price_no_tax',
    // Ponta ポイント利用 (税率別)
    'use_point', 'use_point_normal_tax', 'use_point_reduced_tax', 'use_point_no_tax',
    // au ポイント利用 (税率別 + ポイント数)
    'use_au_point_price', 'use_au_point_price_normal_tax', 'use_au_point_price_reduced_tax', 'use_au_point_price_no_tax', 'use_au_point',
    // 請求 (税率別 + 税額別)
    'request_price', 'request_price_normal_tax', 'request_tax_price_normal_tax',
    'request_price_reduced_tax', 'request_tax_price_reduced_tax',
    'request_price_no_tax', 'request_tax_price_no_tax',
    // ポイント確定
    'point_fixed_date', 'point_fixed_status',
    // 決済処理結果 (PG)
    'settle_status', 'pg_result', 'pg_order_id',
    'pg_request_price', 'pg_request_price_normal_tax', 'pg_request_price_reduced_tax', 'pg_request_price_no_tax',
    // 配送 / 電子領収書
    'delivery_name', 'delivery_method_id', 'elec_receipt_issue_status', 'elec_receipt_issue_times',
    // ─── 明細レベル ───
    'item_management_id', 'item_code', 'lotnumber', 'item_name', 'item_option',
    'item_option_price', 'gift_wrapping_price', 'gift_message',
    'noshi_presenter_name1', 'noshi_presenter_name2', 'noshi_presenter_name3',
    'item_cancel_status', 'before_discount', 'discount', 'item_price', 'unit', 'total_item_price', 'total_item_charge_price',
    'tax_type', 'reduced_tax', 'tax_rate', 'gift_point',
    'shipping_day_disp_text', 'shipping_timelimit_date',
    // メタ
    'synced_at',
  ];
  const ins = db.prepare(`INSERT INTO raw_aupay_orders (${COLS.join(', ')}) VALUES (${COLS.map(() => '?').join(',')})`);

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
        // COLS と同順 (114 値)
        ins.run(
          orderId, strVal(d.orderDetailId),
          // 注文レベル: 基本
          orderDate, strVal(o.sellMethodSegment), strVal(o.siteAndDevice), strVal(o.crossBorderEcTradeKbn),
          // 顧客情報 (PII)
          strVal(o.mailAddress),
          strVal(o.ordererName), strVal(o.ordererKana), strVal(o.ordererZipCode), strVal(o.ordererAddress),
          strVal(o.ordererPhoneNumber1), strVal(o.ordererPhoneNumber2), strVal(o.nickname),
          strVal(o.senderName), strVal(o.senderKana), strVal(o.senderZipCode), strVal(o.senderAddress),
          strVal(o.senderPhoneNumber1), strVal(o.senderPhoneNumber2),
          // オプション/コメント
          strVal(o.orderOption), strVal(o.userComment), strVal(o.tradeRemarks), strVal(o.memo),
          // 決済 + ステータス
          strVal(o.settlementName), orderStatus, strVal(o.contactStatus), strVal(o.authorizationStatus),
          strVal(o.paymentStatus), strVal(o.shipStatus), strVal(o.printStatus), strVal(o.cancelStatus),
          // 商品計売上 (税率別 + 数量計)
          numVal(o.totalSalePrice), numVal(o.totalSalePriceNormalTax), numVal(o.totalSalePriceReducedTax), numVal(o.totalSalePriceNoTax), intVal(o.totalSaleUnit),
          // 送料 / 代引手数料
          numVal(o.postagePrice), numVal(o.postagePriceTaxRate), numVal(o.chargePrice), numVal(o.chargePriceTaxRate),
          // オプション料 / ラッピング料 (注文計)
          numVal(o.totalItemOptionPrice), numVal(o.totalItemOptionPriceTaxRate), numVal(o.totalGiftWrappingPrice), numVal(o.totalGiftWrappingPriceTaxRate),
          // 合計 (税率別)
          numVal(o.totalPrice), numVal(o.totalPriceNormalTax), numVal(o.totalPriceReducedTax), numVal(o.totalPriceNoTax),
          // プレミアム / Pontaパス
          strVal(o.premiumMember), strVal(o.premiumType), numVal(o.premiumIssuePrice), numVal(o.premiumMallPrice), numVal(o.premiumShopPrice), strVal(o.pontapassCampaignApplyFlg),
          // クーポン (税率別)
          numVal(o.couponTotalPrice), numVal(o.couponTotalPriceNormalTax), numVal(o.couponTotalPriceReducedTax), numVal(o.couponTotalPriceNoTax),
          // Ponta ポイント利用 (税率別)
          numVal(o.usePoint), numVal(o.usePointNormalTax), numVal(o.usePointReducedTax), numVal(o.usePointNoTax),
          // au ポイント利用 (税率別 + ポイント数)
          numVal(o.useAuPointPrice), numVal(o.useAuPointPriceNormalTax), numVal(o.useAuPointPriceReducedTax), numVal(o.useAuPointPriceNoTax), numVal(o.useAuPoint),
          // 請求 (税率別 + 税額別)
          numVal(o.requestPrice), numVal(o.requestPriceNormalTax), numVal(o.requestTaxPriceNormalTax),
          numVal(o.requestPriceReducedTax), numVal(o.requestTaxPriceReducedTax),
          numVal(o.requestPriceNoTax), numVal(o.requestTaxPriceNoTax),
          // ポイント確定
          strVal(o.pointFixedDate), strVal(o.pointFixedStatus),
          // 決済処理結果 (PG)
          strVal(o.settleStatus), strVal(o.pgResult), strVal(o.pgOrderId),
          numVal(o.pgRequestPrice), numVal(o.pgRequestPriceNormalTax), numVal(o.pgRequestPriceReducedTax), numVal(o.pgRequestPriceNoTax),
          // 配送 / 電子領収書
          strVal(o.deliveryName), strVal(o.deliveryMethodId), strVal(o.elecReceiptIssueStatus), intVal(o.elecReceiptIssueTimes),
          // ─── 明細レベル ───
          strVal(d.itemManagementId),
          (strVal(d.itemCode) || '').toLowerCase(),
          strVal(d.lotnumber),
          strVal(d.itemName), strVal(d.itemOption),
          numVal(d.itemOptionPrice), numVal(d.giftWrappingPrice),
          strVal(d.giftMessage),
          strVal(d.noshiPresenterName1), strVal(d.noshiPresenterName2), strVal(d.noshiPresenterName3),
          strVal(d.itemCancelStatus),
          numVal(d.beforeDiscount), numVal(d.discount), numVal(d.itemPrice), intVal(d.unit), numVal(d.totalItemPrice), numVal(d.totalItemChargePrice),
          strVal(d.taxType), numVal(d.reducedTax), numVal(d.taxRate), numVal(d.giftPoint),
          strVal(d.shippingDayDispText), strVal(d.shippingTimelimitDate),
          // メタ
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
  // 月単位で endD の月から startD の月まで遡る。各月は「月初〜月末」(端の月は startD/endD でクランプ)
  let curMonth = new Date(endD.getFullYear(), endD.getMonth(), 1);  // 対象月の 1 日
  while (curMonth >= new Date(startD.getFullYear(), startD.getMonth(), 1)) {
    const monthStart = curMonth < startD ? new Date(startD) : new Date(curMonth);
    const monthLastDay = new Date(curMonth.getFullYear(), curMonth.getMonth() + 1, 0);  // その月の末日
    const monthEnd = monthLastDay > endD ? new Date(endD) : monthLastDay;
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
    curMonth = new Date(curMonth.getFullYear(), curMonth.getMonth() - 1, 1);  // 前月の 1 日
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
