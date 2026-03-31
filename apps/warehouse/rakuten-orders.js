/**
 * 楽天RMS API 受注データ取得 → warehouse.dbに投入
 *
 * 2層構造:
 *   raw_rakuten_orders_log  — append-only（全取得ログ）
 *   raw_rakuten_orders      — current snapshot（注文番号単位で上書き）
 *
 * 使い方:
 *   node apps/warehouse/rakuten-orders.js                     → 直近7日
 *   node apps/warehouse/rakuten-orders.js 30                   → 直近30日
 *   node apps/warehouse/rakuten-orders.js 2025-01-01 2025-01-31 → 期間指定
 *   node apps/warehouse/rakuten-orders.js backfill 2024-04-01   → バックフィル（63日刻み）
 *
 * 個人情報対策:
 *   - APIレスポンスから必要フィールドのみallowlistで抽出
 *   - 個人情報はログにも出力しない
 */
import 'dotenv/config';
import { initDB, getDB, updateSyncMeta } from './db.js';

const SERVICE_SECRET = process.env.RAKUTEN_SERVICE_SECRET;
const LICENSE_KEY = process.env.RAKUTEN_LICENSE_KEY;
const AUTH = SERVICE_SECRET && LICENSE_KEY
  ? Buffer.from(`${SERVICE_SECRET}:${LICENSE_KEY}`).toString('base64')
  : null;

const BASE_URL = 'https://api.rms.rakuten.co.jp/es/2.0/order';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function now() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19);
}

// ─── API呼び出し ───

async function callRMS(endpoint, body) {
  const url = `${BASE_URL}/${endpoint}/`;
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `ESA ${AUTH}`,
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60000),
  });

  if (!response.ok) {
    throw new Error(`RMS API ${endpoint} HTTP ${response.status}`);
  }

  const data = await response.json();

  // エラーチェック（レスポンス本文はログに出さない＝個人情報対策）
  if (data.MessageModelList) {
    const errors = data.MessageModelList.filter(m => m.messageType === 'ERROR');
    if (errors.length > 0) {
      throw new Error(`RMS API ${endpoint}: ${errors.map(e => e.messageCode + ' ' + e.message).join(', ')}`);
    }
  }

  return data;
}

// ─── searchOrder ───

async function searchOrders(startDate, endDate, page = 1) {
  console.log(`[楽天] searchOrder: ${startDate} 〜 ${endDate} (page ${page})`);

  // 日付フォーマット: "2026-03-31T00:00:00+0900"
  const startDatetime = `${startDate}T00:00:00+0900`;
  const endDatetime = `${endDate}T23:59:59+0900`;

  const data = await callRMS('searchOrder', {
    dateType: 1, // 注文日
    startDatetime,
    endDatetime,
    orderProgressList: [100, 200, 300, 400, 500, 600, 700, 800, 900],
    PaginationRequestModel: {
      requestRecordsAmount: 1000,
      requestPage: page,
    },
  });

  const orderNumbers = data.orderNumberList || [];
  const totalRecords = data.PaginationResponseModel?.totalRecordsAmount || 0;
  const totalPages = data.PaginationResponseModel?.totalPages || 1;

  console.log(`[楽天] 検索結果: ${totalRecords}件 (page ${page}/${totalPages})`);

  return { orderNumbers, totalRecords, totalPages };
}

// ─── getOrder → 個人情報除外 ───

async function getOrderDetails(orderNumbers) {
  const data = await callRMS('getOrder', {
    orderNumberList: orderNumbers,
    version: 7,
  });

  const orders = data.OrderModelList || [];
  const items = [];

  for (const order of orders) {
    // allowlist: 注文レベルの必要フィールドだけ抽出
    const orderInfo = {
      orderNumber: order.orderNumber || '',
      orderDatetime: order.orderDatetime || '',
      orderProgress: order.orderProgress || 0,
      goodsPrice: order.goodsPrice ?? -9999,
      goodsTax: order.goodsTax ?? -9999,
      totalPrice: order.totalPrice ?? -9999,
      requestPrice: order.requestPrice ?? -9999,
      postagePrice: order.postagePrice ?? -9999,
      couponShopPrice: order.couponShopPrice ?? 0,
      couponAllTotalPrice: order.couponAllTotalPrice ?? 0,
    };

    // 商品明細を展開（PackageModelList → ItemModelList）
    const packages = order.PackageModelList || [];
    for (const pkg of packages) {
      const pkgItems = pkg.ItemModelList || [];
      for (const item of pkgItems) {
        items.push({
          ...orderInfo,
          itemDetailId: item.itemDetailId || 0,
          itemNumber: item.itemNumber || '',
          itemName: item.itemName || '',
          price: item.price ?? 0,
          priceTaxIncl: item.priceTaxIncl ?? 0,
          units: item.units ?? 0,
          taxRate: item.taxRate ?? 0,
          selectedChoice: item.selectedChoice || '',
          deleteItemFlag: item.deleteItemFlag ?? 0,
        });
      }
    }
  }

  return items;
}

// ─── DB投入（2層構造）───

function importToDb(items, batchId, windowStart, windowEnd) {
  const db = getDB();
  const ts = now();

  const logStmt = db.prepare(`
    INSERT INTO raw_rakuten_orders_log (
      batch_id, source_window_start, source_window_end,
      order_number, order_date, order_status,
      goods_price, goods_tax, total_price, request_price, postage_price,
      coupon_shop_price, coupon_all_total_price,
      item_detail_id, item_number, item_name,
      price, price_tax_incl, units, tax_rate,
      selected_choice, delete_item_flag, ingested_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const deleteStmt = db.prepare('DELETE FROM raw_rakuten_orders WHERE order_number = ?');
  const insertStmt = db.prepare(`
    INSERT INTO raw_rakuten_orders (
      order_number, order_date, order_status,
      goods_price, goods_tax, total_price, request_price, postage_price,
      coupon_shop_price, coupon_all_total_price,
      item_detail_id, item_number, item_name,
      price, price_tax_incl, units, tax_rate,
      selected_choice, delete_item_flag, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const tx = db.transaction(() => {
    let logCount = 0;
    let currentCount = 0;
    const orderNumbers = new Set();

    // ステップ1: log に append
    for (const item of items) {
      orderNumbers.add(item.orderNumber);
      logStmt.run(
        batchId, windowStart, windowEnd,
        item.orderNumber, item.orderDatetime, item.orderProgress,
        item.goodsPrice, item.goodsTax, item.totalPrice, item.requestPrice, item.postagePrice,
        item.couponShopPrice, item.couponAllTotalPrice,
        item.itemDetailId, item.itemNumber, item.itemName,
        item.price, item.priceTaxIncl, item.units, item.taxRate,
        item.selectedChoice, item.deleteItemFlag, ts
      );
      logCount++;
    }

    // ステップ2: current を注文番号単位で DELETE → INSERT
    for (const orderNum of orderNumbers) {
      deleteStmt.run(orderNum);
    }
    for (const item of items) {
      insertStmt.run(
        item.orderNumber, item.orderDatetime, item.orderProgress,
        item.goodsPrice, item.goodsTax, item.totalPrice, item.requestPrice, item.postagePrice,
        item.couponShopPrice, item.couponAllTotalPrice,
        item.itemDetailId, item.itemNumber, item.itemName,
        item.price, item.priceTaxIncl, item.units, item.taxRate,
        item.selectedChoice, item.deleteItemFlag, ts
      );
      currentCount++;
    }

    return { logCount, currentCount, uniqueOrders: orderNumbers.size };
  });

  return tx();
}

// ─── 期間分の全注文を取得・投入 ───

async function fetchAndImport(startDate, endDate, batchId) {
  // Step 1: searchOrder（全ページ）
  let allOrderNumbers = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages) {
    const result = await searchOrders(startDate, endDate, page);
    allOrderNumbers = allOrderNumbers.concat(result.orderNumbers);
    totalPages = result.totalPages;
    page++;
    if (page <= totalPages) await sleep(1000);
  }

  if (allOrderNumbers.length === 0) {
    console.log(`[楽天] ${startDate}〜${endDate}: データなし`);
    return { logCount: 0, currentCount: 0, uniqueOrders: 0 };
  }

  console.log(`[楽天] 注文番号取得: ${allOrderNumbers.length}件`);

  // Step 2: getOrder（100件ずつ）
  let allItems = [];
  for (let i = 0; i < allOrderNumbers.length; i += 100) {
    const batch = allOrderNumbers.slice(i, i + 100);
    console.log(`[楽天] getOrder: ${i + 1}〜${Math.min(i + 100, allOrderNumbers.length)} / ${allOrderNumbers.length}`);
    const items = await getOrderDetails(batch);
    allItems = allItems.concat(items);
    if (i + 100 < allOrderNumbers.length) await sleep(1000);
  }

  console.log(`[楽天] 商品明細取得: ${allItems.length}件`);

  // Step 3: DB投入
  const result = importToDb(allItems, batchId, startDate, endDate);
  console.log(`[楽天] 投入完了: log=${result.logCount}件, current=${result.currentCount}件, 注文数=${result.uniqueOrders}`);

  return result;
}

// ─── メイン ───

async function main() {
  const args = process.argv.slice(2);

  // 認証チェック
  if (!SERVICE_SECRET || !LICENSE_KEY) {
    console.error('[楽天] 環境変数が不足: RAKUTEN_SERVICE_SECRET, RAKUTEN_LICENSE_KEY');
    process.exit(1);
  }

  await initDB();

  // テーブル作成（db.jsに定義がない場合はここで作成）
  ensureTables();

  if (args[0] === 'backfill') {
    const startFrom = args[1] || '2024-04-01';
    await runBackfill(startFrom);
  } else if (args.length === 2 && args[0].includes('-')) {
    await runPeriod(args[0], args[1]);
  } else {
    const days = parseInt(args[0]) || 7;
    await runDaily(days);
  }
}

function ensureTables() {
  const db = getDB();

  db.exec(`CREATE TABLE IF NOT EXISTS raw_rakuten_orders_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    source_window_start TEXT,
    source_window_end TEXT,
    order_number TEXT NOT NULL,
    order_date TEXT,
    order_status INTEGER,
    goods_price REAL,
    goods_tax REAL,
    total_price REAL,
    request_price REAL,
    postage_price REAL,
    coupon_shop_price REAL,
    coupon_all_total_price REAL,
    item_detail_id INTEGER,
    item_number TEXT,
    item_name TEXT,
    price REAL,
    price_tax_incl REAL,
    units INTEGER,
    tax_rate REAL,
    selected_choice TEXT,
    delete_item_flag INTEGER,
    ingested_at TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_rk_log_batch ON raw_rakuten_orders_log(batch_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_rk_log_order ON raw_rakuten_orders_log(order_number)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_rk_log_date ON raw_rakuten_orders_log(order_date)');

  db.exec(`CREATE TABLE IF NOT EXISTS raw_rakuten_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_number TEXT NOT NULL,
    order_date TEXT,
    order_status INTEGER,
    goods_price REAL,
    goods_tax REAL,
    total_price REAL,
    request_price REAL,
    postage_price REAL,
    coupon_shop_price REAL,
    coupon_all_total_price REAL,
    item_detail_id INTEGER,
    item_number TEXT,
    item_name TEXT,
    price REAL,
    price_tax_incl REAL,
    units INTEGER,
    tax_rate REAL,
    selected_choice TEXT,
    delete_item_flag INTEGER,
    synced_at TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_rk_orders_order ON raw_rakuten_orders(order_number)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_rk_orders_date ON raw_rakuten_orders(order_date)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_rk_orders_item ON raw_rakuten_orders(item_number)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_rk_orders_status ON raw_rakuten_orders(order_status)');
}

async function runDaily(days) {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - days);
  const startDate = start.toISOString().slice(0, 10);
  const endDate = end.toISOString().slice(0, 10);
  const batchId = `${endDate}_rakuten_daily_${days}d`;

  console.log(`[楽天] 日次取得: ${startDate} 〜 ${endDate}`);
  await fetchAndImport(startDate, endDate, batchId);
  updateSyncMeta('rakuten_last_daily', now());
  updateSyncMeta('rakuten_daily_range', `${startDate} ~ ${endDate}`);
}

async function runPeriod(startDate, endDate) {
  const batchId = `${now().slice(0, 10)}_rakuten_period_${startDate}_${endDate}`;
  console.log(`[楽天] 期間取得: ${startDate} 〜 ${endDate}`);
  await fetchAndImport(startDate, endDate, batchId);
  updateSyncMeta('rakuten_last_period', now());
}

async function runBackfill(startFrom) {
  console.log(`[楽天] バックフィル開始: ${startFrom} 〜 現在`);

  const start = new Date(startFrom);
  const today = new Date();
  let current = new Date(start);

  while (current < today) {
    const chunkStart = current.toISOString().slice(0, 10);
    const chunkEndDate = new Date(Math.min(
      current.getTime() + 62 * 24 * 60 * 60 * 1000, // 63日
      today.getTime()
    ));
    const chunkEnd = chunkEndDate.toISOString().slice(0, 10);
    const batchId = `backfill_rakuten_${chunkStart}_${chunkEnd}`;

    await fetchAndImport(chunkStart, chunkEnd, batchId);

    current = new Date(chunkEndDate.getTime() + 1 * 24 * 60 * 60 * 1000);

    if (current < today) {
      console.log('[楽天] 30秒待機（レート制限回避）...');
      await sleep(30000);
    }
  }

  updateSyncMeta('rakuten_last_backfill', now());
  console.log('[楽天] バックフィル完了');
}

main().catch(e => {
  console.error('[楽天] エラー:', e.message);
  process.exit(1);
});
