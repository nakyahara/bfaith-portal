/**
 * 小規模モール受注データ取得 — Qoo10 / au PAY / メルカリShops / LINEギフト
 *
 * 使い方:
 *   node apps/warehouse/mall-orders.js qoo10 [days]
 *   node apps/warehouse/mall-orders.js aupay [days]
 *   node apps/warehouse/mall-orders.js mercari [days]
 *   node apps/warehouse/mall-orders.js linegift [days]
 *   node apps/warehouse/mall-orders.js all [days]       → 全モール一括
 *
 * デフォルト: 直近7日分
 */
import 'dotenv/config';
import { parseStringPromise } from 'xml2js';
import { initDB, getDB, updateSyncMeta } from './db.js';

function now() { return new Date().toISOString().replace('T', ' ').slice(0, 19); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── テーブル作成 ───

function ensureTables() {
  const db = getDB();

  // 各モール共通の受注テーブル（モール名をテーブル名に含める）
  for (const mall of ['qoo10', 'aupay', 'mercari', 'linegift']) {
    db.exec(`CREATE TABLE IF NOT EXISTS raw_${mall}_orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id TEXT NOT NULL,
      order_date TEXT,
      order_status TEXT,
      item_code TEXT,
      item_name TEXT,
      quantity INTEGER,
      unit_price REAL,
      total_price REAL,
      option_info TEXT,
      synced_at TEXT
    )`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_${mall}_order_id ON raw_${mall}_orders(order_id)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_${mall}_date ON raw_${mall}_orders(order_date)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_${mall}_item ON raw_${mall}_orders(item_code)`);
  }
}

// ─── Qoo10 ───

async function fetchQoo10(days = 7) {
  const apiKey = process.env.QOO10_CERT_KEY;
  if (!apiKey) { console.log('[Qoo10] QOO10_CERT_KEY未設定'); return; }

  console.log(`[Qoo10] 受注取得開始（直近${days}日）`);
  const db = getDB();
  const ts = now();
  const end = new Date();
  const start = new Date(); start.setDate(start.getDate() - days);
  const startStr = start.toISOString().slice(0, 10).replace(/-/g, '');
  const endStr = end.toISOString().slice(0, 10).replace(/-/g, '');

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_qoo10_orders (order_id, order_date, order_status, item_code, item_name, quantity, unit_price, total_price, option_info, synced_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
  `);

  let total = 0;
  // ステータス1-5を全取得
  for (const stat of ['1', '2', '3', '4', '5']) {
    let page = 1;
    while (true) {
      const url = `https://api.qoo10.jp/GMKT.INC.Front.QAPIService/ebayjapan.qapi/ShippingBasic.GetShippingInfo_v2?key=${apiKey}&ShippingStat=${stat}&search_Sdate=${startStr}&search_Edate=${endStr}&Page=${page}&PageSize=200`;
      const res = await fetch(url);
      const data = await res.json();

      if (data.ResultCode !== 0 || !data.ResultObject) break;

      const items = Array.isArray(data.ResultObject) ? data.ResultObject : [data.ResultObject];
      if (items.length === 0) break;

      const tx = db.transaction(() => {
        for (const item of items) {
          const packNo = String(item.packNo || '');
          if (!packNo) continue;
          stmt.run(
            packNo,
            item.orderDate || '',
            item.shippingStatus || stat,
            (item.sellerItemCode || item.itemCode || '').toLowerCase(),
            item.itemTitle || '',
            parseInt(item.orderQty) || 0,
            parseFloat(item.orderPrice) || 0,
            parseFloat(item.total) || 0,
            item.option || '',
            ts
          );
          total++;
        }
      });
      tx();

      page++;
      await sleep(1000);
      if (items.length < 200) break;
    }
  }

  updateSyncMeta('qoo10_last_sync', now());
  console.log(`[Qoo10] 受注取得完了: ${total}件`);
  return total;
}

// ─── au PAY Market（Wow! manager API — wmshopapi） ───

const AUPAY_SHOP_ID = process.env.AUPAY_SHOP_ID || '54318092';
const AUPAY_BASE = 'https://api.manager.wowma.jp/wmshopapi';

async function fetchAuPay(days = 7) {
  const apiKey = process.env.AUPAY_API_KEY;
  if (!apiKey) { console.log('[auPay] AUPAY_API_KEY未設定'); return; }

  console.log(`[auPay] 受注取得開始（直近${days}日）`);
  const db = getDB();
  const ts = now();
  const end = new Date();
  const start = new Date(); start.setDate(start.getDate() - days);
  const startDate = start.toISOString().slice(0, 10).replace(/-/g, '');
  const endDate = end.toISOString().slice(0, 10).replace(/-/g, '');

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_aupay_orders (order_id, order_date, order_status, item_code, item_name, quantity, unit_price, total_price, option_info, synced_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
  `);

  let total = 0;
  let startCount = 1;
  const pageSize = 100;

  while (true) {
    const qs = new URLSearchParams({
      shopId: AUPAY_SHOP_ID,
      totalCount: String(pageSize),
      startCount: String(startCount),
      startDate,
      endDate,
      dateType: '0',  // 0=受注日
    });

    const url = `${AUPAY_BASE}/searchTradeInfoListProc?${qs}`;
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    });

    const xml = await res.text();
    const parsed = await parseStringPromise(xml, { explicitArray: false });
    const response = parsed.response;

    if (response.result?.status !== '0') {
      const err = response.result?.error;
      console.log(`[auPay] APIエラー: ${err?.code} ${err?.message}`);
      break;
    }

    const resultCount = parseInt(response.resultCount) || 0;
    if (resultCount === 0) break;

    // orderInfoが1件の場合はオブジェクト、複数の場合は配列
    const orders = Array.isArray(response.orderInfo) ? response.orderInfo : [response.orderInfo];

    const tx = db.transaction(() => {
      for (const order of orders) {
        if (!order) continue;
        const orderId = order.orderId || '';
        const orderDate = order.orderDate || '';
        const orderStatus = order.orderStatus || '';

        // detailが1件の場合はオブジェクト、複数の場合は配列
        const details = order.detail
          ? (Array.isArray(order.detail) ? order.detail : [order.detail])
          : [];

        for (const detail of details) {
          stmt.run(
            orderId,
            orderDate,
            orderStatus,
            (detail.itemCode || '').toLowerCase(),
            detail.itemName || '',
            parseInt(detail.unit) || 0,
            parseFloat(detail.itemPrice) || 0,
            parseFloat(detail.totalItemPrice) || 0,
            detail.itemOption || '',
            ts
          );
          total++;
        }
      }
    });
    tx();

    console.log(`[auPay] ${total}件取得 (startCount: ${startCount}, resultCount: ${resultCount})`);

    if (resultCount < pageSize) break;
    startCount += pageSize;
    await sleep(2000);
  }

  updateSyncMeta('aupay_last_sync', now());
  console.log(`[auPay] 受注取得完了: ${total}件`);
  return total;
}

// ─── メルカリShops ───

async function fetchMercari(days = 7) {
  const token = process.env.MERCARI_API_TOKEN;
  if (!token) { console.log('[メルカリ] MERCARI_API_TOKEN未設定'); return; }

  console.log(`[メルカリ] 受注取得開始（直近${days}日）`);
  const db = getDB();
  const ts = now();
  const start = new Date(); start.setDate(start.getDate() - days);

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_mercari_orders (order_id, order_date, order_status, item_code, item_name, quantity, unit_price, total_price, option_info, synced_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
  `);

  let total = 0;
  let after = null;

  const query = `
    query($first: Int, $after: String, $filter: OrderTransactionFilterInput) {
      orderTransactions(first: $first, after: $after, filter: $filter) {
        edges {
          node {
            id
            orderNumber
            status
            createdAt
            totalAmount
            orderItems {
              productName
              sku
              price
              quantity
              variationName
            }
          }
          cursor
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  `;

  while (true) {
    try {
      const res = await fetch('https://api.mercari-shops.com/v1/graphql', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query,
          variables: {
            first: 50,
            after,
            filter: { createdAtFrom: start.toISOString() },
          },
        }),
      });

      const json = await res.json();
      if (json.errors) {
        console.log('[メルカリ] GraphQLエラー:', json.errors[0]?.message);
        break;
      }

      const edges = json.data?.orderTransactions?.edges || [];
      if (edges.length === 0) break;

      const tx = db.transaction(() => {
        for (const { node } of edges) {
          const items = node.orderItems || [];
          for (const item of items) {
            stmt.run(
              node.orderNumber || node.id || '',
              node.createdAt || '',
              node.status || '',
              (item.sku || '').toLowerCase(),
              item.productName || '',
              parseInt(item.quantity) || 0,
              parseFloat(item.price) || 0,
              parseFloat(item.price) * (parseInt(item.quantity) || 0),
              item.variationName || '',
              ts
            );
            total++;
          }
        }
      });
      tx();

      const pageInfo = json.data?.orderTransactions?.pageInfo;
      if (!pageInfo?.hasNextPage) break;
      after = pageInfo.endCursor;
      await sleep(1000);
    } catch (e) {
      console.log('[メルカリ] エラー:', e.message);
      break;
    }
  }

  updateSyncMeta('mercari_last_sync', now());
  console.log(`[メルカリ] 受注取得完了: ${total}件`);
  return total;
}

// ─── LINEギフト ───

async function fetchLineGift(days = 7) {
  let accessToken = process.env.LINEGIFT_ACCESS_TOKEN;
  const refreshToken = process.env.LINEGIFT_REFRESH_TOKEN;
  const clientId = process.env.LINEGIFT_CLIENT_ID;
  const clientSecret = process.env.LINEGIFT_CLIENT_SECRET;

  if (!accessToken || !clientId) { console.log('[LINEギフト] LINEGIFT認証情報未設定'); return; }

  console.log(`[LINEギフト] 受注取得開始（直近${days}日）`);
  const db = getDB();
  const ts = now();
  const start = new Date(); start.setDate(start.getDate() - days);

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_linegift_orders (order_id, order_date, order_status, item_code, item_name, quantity, unit_price, total_price, option_info, synced_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
  `);

  // トークンリフレッシュ
  if (refreshToken && clientId && clientSecret) {
    try {
      const tokenRes = await fetch('https://gift-shop-cms.line.biz/api/v1/oauth2/token/refresh', {
        method: 'POST',
        headers: { 'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          grant_type: 'refresh_token',
          client_id: clientId,
          client_secret: clientSecret,
          refresh_token: refreshToken,
        }).toString(),
      });
      const tokenData = await tokenRes.json();
      if (tokenData.access_token) {
        accessToken = tokenData.access_token;
        console.log('[LINEギフト] トークンリフレッシュ成功');
      }
    } catch (e) {
      console.log('[LINEギフト] トークンリフレッシュ失敗:', e.message);
    }
  }

  let total = 0;
  let page = 1;

  while (true) {
    try {
      const url = `https://shop-mall.line.me/shop/api/1/order/search?access_token=${accessToken}&page=${page}&per_page=100`;
      const res = await fetch(url);

      if (!res.ok) {
        console.log('[LINEギフト] HTTP', res.status);
        break;
      }

      const data = await res.json();
      const orders = data.orders || [];
      if (orders.length === 0) break;

      const tx = db.transaction(() => {
        for (const order of orders) {
          // 日付フィルタ（bought_onがUnixTimestamp）
          const orderDate = order.bought_on ? new Date(order.bought_on * 1000) : null;
          if (orderDate && orderDate < start) continue;

          const items = order.items || [];
          if (items.length > 0) {
            for (const item of items) {
              stmt.run(
                String(order.id || ''),
                orderDate ? orderDate.toISOString() : '',
                order.status || '',
                (item.variation_code || item.item_code || '').toLowerCase(),
                item.item_name || '',
                parseInt(item.quantity) || 1,
                parseFloat(item.selling_price || item.price) || 0,
                parseFloat(item.selling_price || item.price) * (parseInt(item.quantity) || 1),
                item.variation_name || '',
                ts
              );
              total++;
            }
          } else {
            // itemsがない場合は注文レベルで1行
            stmt.run(
              String(order.id || ''),
              orderDate ? orderDate.toISOString() : '',
              order.status || '',
              '',
              '',
              1,
              parseFloat(order.selling_price) || 0,
              parseFloat(order.selling_price) || 0,
              '',
              ts
            );
            total++;
          }
        }
      });
      tx();

      const paging = data.paging;
      if (!paging || page >= paging.total_pages) break;
      page++;
      await sleep(1000);
    } catch (e) {
      console.log('[LINEギフト] エラー:', e.message);
      break;
    }
  }

  updateSyncMeta('linegift_last_sync', now());
  console.log(`[LINEギフト] 受注取得完了: ${total}件`);
  return total;
}

// ─── メイン ───

async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'all';
  const days = parseInt(args[1]) || 7;

  await initDB();
  ensureTables();

  const handlers = {
    qoo10: () => fetchQoo10(days),
    aupay: () => fetchAuPay(days),
    mercari: () => fetchMercari(days),
    linegift: () => fetchLineGift(days),
    all: async () => {
      await fetchQoo10(days);
      await fetchAuPay(days);
      await fetchMercari(days);
      await fetchLineGift(days);
    },
  };

  if (handlers[command]) {
    await handlers[command]();
  } else {
    console.log('使い方: node apps/warehouse/mall-orders.js [qoo10|aupay|mercari|linegift|all] [days]');
  }
}

main().catch(e => {
  console.error('エラー:', e.message);
  process.exit(1);
});
