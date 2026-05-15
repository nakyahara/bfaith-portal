/**
 * 小規模モール受注データ取得 — Qoo10 / メルカリShops
 * (au PAY は apps/warehouse/aupay-orders.js に分離 — Phase 1 で受注 API 全フィールド + fail-closed 化)
 * (LINEギフト は apps/warehouse/linegift-orders.js に分離 — Phase 1 A-1、2026-05-15。
 *  旧 fetchLineGift は item_code 等空文字保存のバグ持ちで廃止)
 *
 * 使い方:
 *   node apps/warehouse/mall-orders.js qoo10 [days]
 *   node apps/warehouse/mall-orders.js mercari [days]
 *   node apps/warehouse/mall-orders.js all [days]       → 上記 2 モール一括
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
  for (const mall of ['qoo10', 'mercari']) {
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

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO raw_qoo10_orders (order_id, order_date, order_status, item_code, item_name, quantity, unit_price, total_price, option_info, synced_at)
    VALUES (?,?,?,?,?,?,?,?,?,?)
  `);

  let total = 0;
  let lastApiError = null;

  // Qoo10 APIは日付範囲90日上限 → 90日チャンクで分割取得
  // またページングが機能しない（Page1で全件返る）ためPage=1のみ取得
  let chunkEnd = new Date(end);
  while (chunkEnd > start) {
    let chunkStart = new Date(chunkEnd);
    chunkStart.setDate(chunkStart.getDate() - 89);
    if (chunkStart < start) chunkStart = new Date(start);

    const startStr = chunkStart.toISOString().slice(0, 10).replace(/-/g, '');
    const endStr = chunkEnd.toISOString().slice(0, 10).replace(/-/g, '');
    let chunkTotal = 0;

    for (const stat of ['1', '2', '3', '4', '5']) {
      const url = `https://api.qoo10.jp/GMKT.INC.Front.QAPIService/ebayjapan.qapi/ShippingBasic.GetShippingInfo_v2?key=${apiKey}&ShippingStat=${stat}&search_Sdate=${startStr}&search_Edate=${endStr}&Page=1&PageSize=200`;
      const res = await fetch(url);
      const data = await res.json();

      if (data.ResultCode !== 0) {
        lastApiError = `[Qoo10] APIエラー stat=${stat}: ResultCode=${data.ResultCode} ${data.ResultMsg || data.ResultMessage || ''}`;
        console.log(lastApiError);
        continue;
      }
      if (!data.ResultObject) continue;

      const items = Array.isArray(data.ResultObject) ? data.ResultObject : [data.ResultObject];

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
          chunkTotal++;
        }
      });
      tx();

      await sleep(300);
    }

    total += chunkTotal;
    console.log(`[Qoo10] ${startStr}-${endStr}: ${chunkTotal}件 (累計${total})`);

    chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() - 1);
    await sleep(300);
  }

  if (total === 0 && lastApiError) {
    throw new Error(lastApiError);
  }

  updateSyncMeta('qoo10_last_sync', now());
  console.log(`[Qoo10] 受注取得完了: ${total}件`);
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
        throw new Error(`[メルカリ] GraphQLエラー: ${json.errors[0]?.message || JSON.stringify(json.errors)}`);
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
      if (e.message.startsWith('[メルカリ]')) throw e;
      throw new Error(`[メルカリ] ${e.message}`);
    }
  }

  updateSyncMeta('mercari_last_sync', now());
  console.log(`[メルカリ] 受注取得完了: ${total}件`);
  return total;
}

// ─── メイン ───

async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'all';
  const days = parseInt(args[1]) || 7;

  await initDB();
  ensureTables();

  // au PAY は apps/warehouse/aupay-orders.js に分離 (Phase 1 で受注 API 全フィールド + fail-closed 化)
  // LINEギフト は apps/warehouse/linegift-orders.js に分離 (Phase 1 A-1)
  const handlers = {
    qoo10: () => fetchQoo10(days),
    mercari: () => fetchMercari(days),
    all: async () => {
      await fetchQoo10(days);
      await fetchMercari(days);
    },
  };

  if (handlers[command]) {
    await handlers[command]();
  } else {
    console.log('使い方: node apps/warehouse/mall-orders.js [qoo10|mercari|all] [days]  (au PAY は aupay-orders.js / LINEギフト は linegift-orders.js)');
  }
}

main().catch(e => {
  console.error('エラー:', e.message);
  process.exit(1);
});
