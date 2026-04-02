/**
 * SP-API Amazon注文データ取得 → warehouse.db に投入
 *
 * 2層構造:
 *   raw_sp_orders_log  — append-only（全取得ログ、削除しない）
 *   raw_sp_orders      — current snapshot（注文ID単位で最新状態に上書き）
 *
 * レポート:
 *   BY_LAST_UPDATE — 日次差分（直近7日、キャンセル・ステータス変更を反映）
 *   BY_ORDER_DATE  — バックフィル・月次再同期
 *
 * 使い方:
 *   node apps/warehouse/sp-api-orders.js                    → 直近7日（BY_LAST_UPDATE）
 *   node apps/warehouse/sp-api-orders.js 30                  → 直近30日（BY_LAST_UPDATE）
 *   node apps/warehouse/sp-api-orders.js 2025-01-01 2025-01-31  → 期間指定（BY_ORDER_DATE）
 *   node apps/warehouse/sp-api-orders.js backfill 2024-04-01    → バックフィル（30日刻みで現在まで）
 */
import 'dotenv/config';
import SellingPartner from 'amazon-sp-api';
import iconv from 'iconv-lite';
import { gunzipSync } from 'zlib';
import { initDB, getDB, saveToFile, updateSyncMeta } from './db.js';

let spClient = null;

function getClient() {
  if (!spClient) {
    spClient = new SellingPartner({
      region: 'fe',
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return spClient;
}

const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
const MAX_RETRIES = 3;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── レポート取得 ───

async function fetchReport(reportType, startDate, endDate) {
  const sp = getClient();
  const isLastUpdate = reportType.includes('LAST_UPDATE');
  const label = isLastUpdate ? 'BY_LAST_UPDATE' : 'BY_ORDER_DATE';

  console.log(`[SP-API] ${label} レポート取得: ${startDate} 〜 ${endDate}`);

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // レポート作成
      const createResult = await sp.callAPI({
        operation: 'createReport',
        endpoint: 'reports',
        body: {
          reportType,
          marketplaceIds: [MARKETPLACE_ID],
          dataStartTime: new Date(startDate).toISOString(),
          dataEndTime: new Date(endDate + 'T23:59:59').toISOString(),
        },
        options: { version: '2021-06-30' },
      });

      const reportId = createResult.reportId;
      console.log(`[SP-API] レポートID: ${reportId} (試行 ${attempt}/${MAX_RETRIES})`);

      // ポーリング（最大10分）
      let report;
      for (let i = 0; i < 120; i++) {
        await sleep(5000);
        report = await sp.callAPI({
          operation: 'getReport',
          endpoint: 'reports',
          path: { reportId },
          options: { version: '2021-06-30' },
        });
        if (i % 6 === 0) console.log(`[SP-API] ポーリング ${i + 1}: ${report.processingStatus}`);
        if (['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) break;
      }

      if (report.processingStatus === 'CANCELLED') {
        console.log(`[SP-API] レポートCANCELLED（試行 ${attempt}）`);
        if (attempt < MAX_RETRIES) {
          console.log(`[SP-API] ${10 * attempt}秒待って再試行...`);
          await sleep(10000 * attempt);
          continue;
        }
        // 3回CANCELLEDなら該当期間にデータなしの可能性 → 空配列を返す
        console.log(`[SP-API] ${MAX_RETRIES}回CANCELLED → データなしとして処理`);
        return [];
      }

      if (report.processingStatus === 'FATAL') {
        throw new Error(`レポート生成FATAL: ${reportId}`);
      }

      if (report.processingStatus !== 'DONE') {
        throw new Error(`レポートタイムアウト: ${report.processingStatus}`);
      }

      // ドキュメントダウンロード
      const doc = await sp.callAPI({
        operation: 'getReportDocument',
        endpoint: 'reports',
        path: { reportDocumentId: report.reportDocumentId },
        options: { version: '2021-06-30' },
      });

      const response = await fetch(doc.url, { signal: AbortSignal.timeout(120000) });
      const rawBuf = Buffer.from(await response.arrayBuffer());
      let dataBuf = rawBuf;
      if (doc.compressionAlgorithm === 'GZIP') {
        dataBuf = gunzipSync(rawBuf);
      }

      const utf8Text = dataBuf.toString('utf-8');
      const text = utf8Text.includes('\ufffd') ? iconv.decode(dataBuf, 'Shift_JIS') : utf8Text;

      return parseTsv(text);

    } catch (e) {
      console.error(`[SP-API] エラー（試行 ${attempt}）: ${e.message}`);
      if (attempt < MAX_RETRIES) {
        await sleep(10000 * attempt);
        continue;
      }
      throw e;
    }
  }
  return [];
}

function parseTsv(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  const headers = lines[0].split('\t').map(h => h.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split('\t');
    const row = {};
    headers.forEach((h, j) => { row[h] = (cols[j] || '').trim(); });
    rows.push(row);
  }
  return rows;
}

// ─── テーブル作成 ───
// テーブルはdb.jsのcreateTablesで作成済み。ここでは不要。
function ensureTables() {}

// ─── DB投入（2層構造）───

function importToDb(rows, batchId, reportType, windowStart, windowEnd) {
  const db = getDB();
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);

  const logStmt = db.prepare(`
    INSERT INTO raw_sp_orders_log (
      batch_id, source_report_type, source_window_start, source_window_end,
      amazon_order_id, merchant_order_id, purchase_date, last_updated_date,
      order_status, fulfillment_channel, sales_channel,
      asin, seller_sku, title, quantity,
      item_price, item_tax, shipping_price, shipping_tax,
      promotion_discount, currency, item_status, ingested_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const deleteStmt = db.prepare('DELETE FROM raw_sp_orders WHERE amazon_order_id = ?');
  const insertStmt = db.prepare(`
    INSERT INTO raw_sp_orders (
      amazon_order_id, merchant_order_id, purchase_date, last_updated_date,
      order_status, fulfillment_channel, sales_channel,
      asin, seller_sku, title, quantity,
      item_price, item_tax, shipping_price, shipping_tax,
      promotion_discount, currency, item_status, synced_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);

  const tx = db.transaction(() => {
    let logCount = 0;
    let currentCount = 0;
    const orderIds = new Set();

    // ステップ1: log に append + 注文IDを収集
    for (const row of rows) {
      const orderId = row['amazon-order-id'] || '';
      if (!orderId) continue;
      orderIds.add(orderId);
      logStmt.run(batchId, reportType, windowStart, windowEnd,
        orderId, row['merchant-order-id']||'', row['purchase-date']||'',
        row['last-updated-date']||'', row['order-status']||'',
        row['fulfillment-channel']||'', row['sales-channel']||'',
        row['asin']||'', (row['sku'] || '').toLowerCase(), row['product-name']||'',
        parseInt(row['quantity'])||0, parseFloat(row['item-price'])||0,
        parseFloat(row['item-tax'])||0, parseFloat(row['shipping-price'])||0,
        parseFloat(row['shipping-tax'])||0, parseFloat(row['item-promotion-discount'])||0,
        row['currency']||'JPY', row['item-status']||'', ts);
      logCount++;
    }

    // ステップ2: current を注文ID単位で DELETE → INSERT
    for (const orderId of orderIds) {
      deleteStmt.run(orderId);
    }
    for (const row of rows) {
      const orderId = row['amazon-order-id'] || '';
      if (!orderId) continue;
      insertStmt.run(orderId, row['merchant-order-id']||'', row['purchase-date']||'',
        row['last-updated-date']||'', row['order-status']||'',
        row['fulfillment-channel']||'', row['sales-channel']||'',
        row['asin']||'', (row['sku'] || '').toLowerCase(), row['product-name']||'',
        parseInt(row['quantity'])||0, parseFloat(row['item-price'])||0,
        parseFloat(row['item-tax'])||0, parseFloat(row['shipping-price'])||0,
        parseFloat(row['shipping-tax'])||0, parseFloat(row['item-promotion-discount'])||0,
        row['currency']||'JPY', row['item-status']||'', ts);
      currentCount++;
    }

    return { logCount, currentCount, uniqueOrders: orderIds.size };
  });

  return tx();
}

// ─── メイン ───

async function main() {
  const args = process.argv.slice(2);

  // 環境変数チェック
  const required = ['SP_API_CLIENT_ID', 'SP_API_CLIENT_SECRET', 'SP_API_REFRESH_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'];
  const missing = required.filter(k => !process.env[k]);
  if (missing.length > 0) {
    console.error(`[SP-API] 環境変数が不足: ${missing.join(', ')}`);
    process.exit(1);
  }

  await initDB();
  ensureTables();

  // コマンド解析
  if (args[0] === 'backfill') {
    // バックフィルモード: 30日刻みで過去から現在まで
    const startFrom = args[1] || '2024-04-01';
    await runBackfill(startFrom);
  } else if (args.length === 2 && args[0].includes('-')) {
    // 期間指定: BY_ORDER_DATE
    await runByOrderDate(args[0], args[1]);
  } else {
    // 日数指定（デフォルト7日）: BY_LAST_UPDATE
    const days = parseInt(args[0]) || 7;
    await runByLastUpdate(days);
  }
}

async function runByLastUpdate(days) {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - days);
  const startDate = start.toISOString().slice(0, 10);
  const endDate = end.toISOString().slice(0, 10);
  const batchId = `${endDate}_daily_${days}d`;

  console.log(`[SP-API] BY_LAST_UPDATE: ${startDate} 〜 ${endDate}`);

  const rows = await fetchReport(
    'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL',
    startDate, endDate
  );

  console.log(`[SP-API] レポート取得完了: ${rows.length}行`);

  if (rows.length > 0) {
    const result = importToDb(rows, batchId, 'BY_LAST_UPDATE', startDate, endDate);
    console.log(`[SP-API] 投入完了: log=${result.logCount}件, current=${result.currentCount}件, 注文数=${result.uniqueOrders}`);
    updateSyncMeta('sp_orders_last_daily', new Date().toISOString());
    updateSyncMeta('sp_orders_daily_range', `${startDate} ~ ${endDate}`);
  } else {
    console.log('[SP-API] データなし');
  }
}

async function runByOrderDate(startDate, endDate) {
  const batchId = `${new Date().toISOString().slice(0, 10)}_bydate_${startDate}_${endDate}`;

  console.log(`[SP-API] BY_ORDER_DATE: ${startDate} 〜 ${endDate}`);

  const rows = await fetchReport(
    'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
    startDate, endDate
  );

  console.log(`[SP-API] レポート取得完了: ${rows.length}行`);

  if (rows.length > 0) {
    const result = importToDb(rows, batchId, 'BY_ORDER_DATE', startDate, endDate);
    console.log(`[SP-API] 投入完了: log=${result.logCount}件, current=${result.currentCount}件, 注文数=${result.uniqueOrders}`);
    updateSyncMeta('sp_orders_last_backfill', new Date().toISOString());
  } else {
    console.log('[SP-API] データなし');
  }
}

async function runBackfill(startFrom) {
  console.log(`[SP-API] バックフィル開始: ${startFrom} 〜 現在`);

  const start = new Date(startFrom);
  const now = new Date();
  let current = new Date(start);

  while (current < now) {
    const chunkStart = current.toISOString().slice(0, 10);
    const chunkEnd = new Date(Math.min(
      current.getTime() + 29 * 24 * 60 * 60 * 1000,
      now.getTime()
    )).toISOString().slice(0, 10);

    await runByOrderDate(chunkStart, chunkEnd);

    // 次の30日チャンクへ
    current = new Date(current.getTime() + 30 * 24 * 60 * 60 * 1000);

    // レート制限回避: チャンク間で30秒待機
    if (current < now) {
      console.log('[SP-API] 30秒待機（レート制限回避）...');
      await sleep(30000);
    }
  }

  console.log('[SP-API] バックフィル完了');
}

main().catch(e => {
  console.error('[SP-API] エラー:', e.message);
  process.exit(1);
});
