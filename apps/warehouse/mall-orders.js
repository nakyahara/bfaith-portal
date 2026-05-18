/**
 * 小規模モール受注データ取得 — メルカリShops
 * (au PAY  は apps/warehouse/aupay-orders.js に分離 — Phase 1 で受注 API 全フィールド + fail-closed 化)
 * (LINEギフト は apps/warehouse/linegift-orders.js に分離 — Phase 1 A-1、2026-05-15)
 * (Qoo10   は apps/warehouse/qoo10-orders.js に分離 — Phase 1 A-1、2026-05-18。
 *  旧 fetchQoo10 は packNo を PK 使用していて grain 崩壊バグの原因、新規 ingest に置換)
 *
 * 使い方:
 *   node apps/warehouse/mall-orders.js mercari [days]
 *   node apps/warehouse/mall-orders.js all [days]       → メルカリのみ
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
  // qoo10 は qoo10-orders.js が新スキーマで管理、ここでは触らない
  for (const mall of ['mercari']) {
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

// ─── Qoo10 は apps/warehouse/qoo10-orders.js に分離 (Phase 1 A-1) ───


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
  // Qoo10 は apps/warehouse/qoo10-orders.js に分離 (Phase 1 A-1、2026-05-18)
  const handlers = {
    mercari: () => fetchMercari(days),
    all: async () => {
      await fetchMercari(days);
    },
  };

  if (handlers[command]) {
    await handlers[command]();
  } else {
    console.log('使い方: node apps/warehouse/mall-orders.js [mercari|all] [days]  (au PAY は aupay-orders.js / LINEギフト は linegift-orders.js / Qoo10 は qoo10-orders.js)');
  }
}

main().catch(e => {
  console.error('エラー:', e.message);
  process.exit(1);
});
