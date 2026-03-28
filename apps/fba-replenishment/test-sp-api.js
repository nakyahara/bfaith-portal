/**
 * SP-API データ取得検証スクリプト
 *
 * 目的: FBA在庫補充システムに必要なデータがSP-APIから取得できるか検証
 *
 * 実行方法:
 *   cd C:\Users\info\Downloads\bfaith-portal
 *   node --env-file=.env apps/fba-replenishment/test-sp-api.js
 *
 * 検証項目:
 *   1. GET_RESTOCK_INVENTORY_RECOMMENDATIONS レポート
 *   2. FBA Inventory API (getInventorySummaries)
 *   3. FBA販売データ 90日 (getSalesCountBySku)
 */

import SellingPartner from 'amazon-sp-api';

const spClient = new SellingPartner({
  region: 'fe',
  refresh_token: process.env.SP_API_REFRESH_TOKEN,
  credentials: {
    SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
    SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
    AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ====================================================================
// テスト1: 発注推奨レポート
// ====================================================================
async function testRestockReport() {
  console.log('\n========================================');
  console.log('テスト1: GET_RESTOCK_INVENTORY_RECOMMENDATIONS');
  console.log('========================================\n');

  try {
    // レポート作成
    const createResult = await spClient.callAPI({
      operation: 'createReport',
      endpoint: 'reports',
      body: {
        reportType: 'GET_RESTOCK_INVENTORY_RECOMMENDATIONS',
        marketplaceIds: [MARKETPLACE_ID],
      },
      options: { version: '2021-06-30' },
    });

    const reportId = createResult.reportId;
    console.log(`レポート作成成功: reportId=${reportId}`);

    // ポーリング
    let report;
    for (let i = 0; i < 60; i++) {
      await sleep(5000);
      report = await spClient.callAPI({
        operation: 'getReport',
        endpoint: 'reports',
        path: { reportId },
        options: { version: '2021-06-30' },
      });
      console.log(`ステータス: ${report.processingStatus} (${i + 1}/60)`);
      if (report.processingStatus === 'DONE') break;
      if (report.processingStatus === 'FATAL' || report.processingStatus === 'CANCELLED') {
        throw new Error(`レポート生成失敗: ${report.processingStatus}`);
      }
    }

    if (report.processingStatus !== 'DONE') {
      throw new Error('タイムアウト');
    }

    // ドキュメント取得
    const doc = await spClient.callAPI({
      operation: 'getReportDocument',
      endpoint: 'reports',
      path: { reportDocumentId: report.reportDocumentId },
      options: { version: '2021-06-30' },
    });

    const response = await fetch(doc.url);
    const rawBuf = Buffer.from(await response.arrayBuffer());

    let dataBuf = rawBuf;
    if (doc.compressionAlgorithm === 'GZIP') {
      const { gunzipSync } = await import('zlib');
      dataBuf = gunzipSync(rawBuf);
    }

    // エンコーディング
    let text;
    try {
      const iconv = await import('iconv-lite');
      text = iconv.default.decode(dataBuf, 'Shift_JIS');
    } catch {
      text = dataBuf.toString('utf-8');
    }

    const lines = text.split('\n').filter(l => l.trim());
    const headers = lines[0].split('\t').map(h => h.trim());

    console.log(`\n取得成功！ ${lines.length - 1}行のデータ`);
    console.log(`\nヘッダー (${headers.length}列):`);
    headers.forEach((h, i) => console.log(`  ${i}: ${h}`));

    // 先頭5行のデータをサンプル表示
    console.log('\nサンプルデータ（先頭5行）:');
    for (let i = 1; i <= Math.min(5, lines.length - 1); i++) {
      const cols = lines[i].split('\t');
      console.log(`\n  行${i}:`);
      headers.forEach((h, j) => {
        if (cols[j] && cols[j].trim()) {
          console.log(`    ${h}: ${cols[j].trim()}`);
        }
      });
    }

    return { success: true, headers, rowCount: lines.length - 1 };

  } catch (e) {
    console.error(`エラー: ${e.message}`);
    if (e.code) console.error(`  コード: ${e.code}`);
    if (e.details) console.error(`  詳細: ${JSON.stringify(e.details)}`);
    return { success: false, error: e.message };
  }
}

// ====================================================================
// テスト2: FBA在庫サマリー
// ====================================================================
async function testFbaInventory() {
  console.log('\n========================================');
  console.log('テスト2: FBA Inventory API (getInventorySummaries)');
  console.log('========================================\n');

  try {
    const result = await spClient.callAPI({
      operation: 'getInventorySummaries',
      endpoint: 'fbaInventory',
      query: {
        granularityType: 'Marketplace',
        granularityId: MARKETPLACE_ID,
        marketplaceIds: [MARKETPLACE_ID],
        details: true,
      },
      options: { version: 'v1' },
    });

    const summaries = result.inventorySummaries || result.payload?.inventorySummaries || [];
    console.log(`取得成功！ ${summaries.length}件の在庫データ`);

    // 先頭5件をサンプル表示
    console.log('\nサンプルデータ（先頭5件）:');
    summaries.slice(0, 5).forEach((item, i) => {
      console.log(`\n  ${i + 1}. SKU: ${item.sellerSku}`);
      console.log(`     ASIN: ${item.asin}`);
      console.log(`     FNSKU: ${item.fnSku}`);
      console.log(`     商品名: ${item.productName}`);
      console.log(`     販売可能(fulfillable): ${item.inventoryDetails?.fulfillableQuantity ?? item.totalQuantity ?? 'N/A'}`);
      console.log(`     予約済(reserved): ${item.inventoryDetails?.reservedQuantity?.totalReservedQuantity ?? 'N/A'}`);
      console.log(`     入荷作業中(inboundWorking): ${item.inventoryDetails?.researchingQuantity?.totalResearchingQuantity ?? 'N/A'}`);

      // inbound系のフィールドを探索
      const details = item.inventoryDetails || {};
      console.log(`     --- inventoryDetails キー ---`);
      Object.keys(details).forEach(key => {
        const val = details[key];
        if (typeof val === 'object') {
          console.log(`     ${key}: ${JSON.stringify(val)}`);
        } else {
          console.log(`     ${key}: ${val}`);
        }
      });
    });

    // ページネーション情報
    const pagination = result.pagination || result.payload?.pagination;
    if (pagination?.nextToken) {
      console.log(`\n※ nextTokenあり（データは続きがあります）`);
    }
    console.log(`\n取得件数: ${summaries.length}`);

    return { success: true, count: summaries.length };

  } catch (e) {
    console.error(`エラー: ${e.message}`);
    if (e.code) console.error(`  コード: ${e.code}`);

    // 403の場合、ロール不足の可能性
    if (e.statusCode === 403 || e.message?.includes('403')) {
      console.error('\n※ 403エラー: SP-APIアプリのロールに「在庫と注文の追跡」が必要です');
    }

    return { success: false, error: e.message };
  }
}

// ====================================================================
// テスト3: FBA販売データ（90日）→ 30日/90日分割集計
// ====================================================================
async function testSalesData() {
  console.log('\n========================================');
  console.log('テスト3: 注文レポート（90日） → 30日/90日分割');
  console.log('========================================\n');

  try {
    const now = new Date();
    const start = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);

    console.log(`期間: ${start.toISOString().slice(0,10)} ～ ${now.toISOString().slice(0,10)}`);

    const createResult = await spClient.callAPI({
      operation: 'createReport',
      endpoint: 'reports',
      body: {
        reportType: 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE',
        marketplaceIds: [MARKETPLACE_ID],
        dataStartTime: start.toISOString(),
        dataEndTime: now.toISOString(),
      },
      options: { version: '2021-06-30' },
    });

    const reportId = createResult.reportId;
    console.log(`レポート作成: reportId=${reportId}`);

    let report;
    for (let i = 0; i < 60; i++) {
      await sleep(5000);
      report = await spClient.callAPI({
        operation: 'getReport',
        endpoint: 'reports',
        path: { reportId },
        options: { version: '2021-06-30' },
      });
      console.log(`ステータス: ${report.processingStatus} (${i + 1}/60)`);
      if (report.processingStatus === 'DONE') break;
      if (report.processingStatus === 'FATAL' || report.processingStatus === 'CANCELLED') {
        throw new Error(`レポート生成失敗: ${report.processingStatus}`);
      }
    }

    if (report.processingStatus !== 'DONE') throw new Error('タイムアウト');

    const doc = await spClient.callAPI({
      operation: 'getReportDocument',
      endpoint: 'reports',
      path: { reportDocumentId: report.reportDocumentId },
      options: { version: '2021-06-30' },
    });

    const response = await fetch(doc.url);
    const rawBuf = Buffer.from(await response.arrayBuffer());
    let dataBuf = rawBuf;
    if (doc.compressionAlgorithm === 'GZIP') {
      const { gunzipSync } = await import('zlib');
      dataBuf = gunzipSync(rawBuf);
    }
    let text;
    try {
      const iconv = await import('iconv-lite');
      text = iconv.default.decode(dataBuf, 'Shift_JIS');
    } catch {
      text = dataBuf.toString('utf-8');
    }

    const lines = text.split('\n').filter(l => l.trim());
    const headers = lines[0].split('\t').map(h => h.trim());

    // SKU列・数量列・日付列を探す
    const findIdx = (...candidates) => {
      for (const c of candidates) {
        const idx = headers.findIndex(h => h.toLowerCase().replace(/\s+/g, '-') === c.toLowerCase());
        if (idx >= 0) return idx;
      }
      return -1;
    };
    const skuIdx = findIdx('sku', 'seller-sku');
    const qtyIdx = findIdx('quantity', 'quantity-purchased');
    const dateIdx = findIdx('purchase-date', 'last-updated-date');
    const statusIdx = findIdx('order-status', 'item-status');

    console.log(`\nヘッダー: SKU列=${skuIdx}, 数量列=${qtyIdx}, 日付列=${dateIdx}`);

    // 7日/30日/90日分割集計
    const salesMap = {};
    const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split('\t');
      const sku = (cols[skuIdx] || '').trim();
      const qty = parseInt(cols[qtyIdx]) || 0;
      const dateStr = (cols[dateIdx] || '').trim();
      const status = statusIdx >= 0 ? (cols[statusIdx] || '').trim().toLowerCase() : '';

      if (!sku || qty <= 0) continue;
      if (status === 'cancelled' || status === 'キャンセル') continue;

      if (!salesMap[sku]) salesMap[sku] = { sold_7d: 0, sold_30d: 0, sold_90d: 0 };
      salesMap[sku].sold_90d += qty;

      const orderDate = new Date(dateStr);
      if (orderDate >= thirtyDaysAgo) salesMap[sku].sold_30d += qty;
      if (orderDate >= sevenDaysAgo) salesMap[sku].sold_7d += qty;
    }

    // トレンド分析サンプル
    const skus = Object.entries(salesMap)
      .filter(([_, v]) => v.sold_90d >= 10)
      .sort((a, b) => b[1].sold_90d - a[1].sold_90d)
      .slice(0, 15);

    console.log(`\n集計完了: ${Object.keys(salesMap).length} SKU`);
    console.log('\nトレンド分析（上位15 SKU）:');
    console.log('SKU                              |  7日 | 30日 | 90日 | 月平均 | 月トレンド | 週間急変');
    console.log('-'.repeat(100));
    for (const [sku, data] of skus) {
      const avg30 = Math.round(data.sold_90d / 3);
      const sold7m = Math.round(data.sold_7d * 30 / 7); // 7日→月換算
      const monthTrend = avg30 > 0 ? (data.sold_30d / avg30).toFixed(2) : 'N/A';
      const weeklySurge = data.sold_30d > 0 ? (sold7m / data.sold_30d).toFixed(2) : 'N/A';
      const mLabel = monthTrend > 1.5 ? '↑↑' : monthTrend > 1.1 ? '↑' : monthTrend < 0.5 ? '↓↓' : monthTrend < 0.9 ? '↓' : '→';
      const wLabel = weeklySurge > 2.0 ? '🔥急上昇' : weeklySurge < 0.3 ? '⚠急停止' : weeklySurge > 1.3 ? '↑' : weeklySurge < 0.7 ? '↓' : '→';
      console.log(`${sku.padEnd(33)}| ${String(data.sold_7d).padStart(4)} | ${String(data.sold_30d).padStart(4)} | ${String(data.sold_90d).padStart(4)} | ${String(avg30).padStart(6)} | ${monthTrend.padStart(5)} ${mLabel.padEnd(3)}| ${weeklySurge.padStart(5)} ${wLabel}`);
    }

    return { success: true, skuCount: Object.keys(salesMap).length };

  } catch (e) {
    console.error(`エラー: ${e.message}`);
    return { success: false, error: e.message };
  }
}

// ====================================================================
// 全テスト実行
// ====================================================================
async function main() {
  console.log('============================================================');
  console.log('FBA在庫補充システム SP-APIデータ取得検証');
  console.log(`実行日時: ${new Date().toLocaleString('ja-JP')}`);
  console.log(`マーケットプレイス: ${MARKETPLACE_ID}`);
  console.log('============================================================');

  const results = {};

  results.restockReport = await testRestockReport();
  results.fbaInventory = await testFbaInventory();
  results.salesData = await testSalesData();

  console.log('\n\n============================================================');
  console.log('検証結果サマリー');
  console.log('============================================================');
  console.log(`1. 発注推奨レポート:  ${results.restockReport.success ? '✓ 成功' : '✗ 失敗'}`);
  if (results.restockReport.success) {
    console.log(`   → ${results.restockReport.rowCount}行, ${results.restockReport.headers?.length}列`);
  } else {
    console.log(`   → ${results.restockReport.error}`);
  }

  console.log(`2. FBA在庫サマリー:   ${results.fbaInventory.success ? '✓ 成功' : '✗ 失敗'}`);
  if (results.fbaInventory.success) {
    console.log(`   → ${results.fbaInventory.count}件`);
  } else {
    console.log(`   → ${results.fbaInventory.error}`);
  }

  console.log(`3. 販売データ(90日):  ${results.salesData.success ? '✓ 成功' : '✗ 失敗'}`);
  if (results.salesData.success) {
    console.log(`   → ${results.salesData.skuCount} SKU（30日/90日分割集計OK）`);
  } else {
    console.log(`   → ${results.salesData.error}`);
  }

  console.log('\n============================================================');
  const allSuccess = results.restockReport.success && results.fbaInventory.success && results.salesData.success;
  if (allSuccess) {
    console.log('→ SP-APIから必要なデータは全て取得可能。Phase 1bに進行可能。');
  } else {
    console.log('→ 一部取得に失敗。エラー内容を確認して対応が必要。');
  }
  console.log('============================================================\n');
}

main().catch(e => {
  console.error('予期せぬエラー:', e);
  process.exit(1);
});
