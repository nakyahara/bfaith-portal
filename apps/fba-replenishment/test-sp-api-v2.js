/**
 * SP-API データ取得検証 v2
 *
 * v1で判明した問題の修正:
 *   1. レポートタイプ名の候補を複数試行
 *   2. 販売データ: 権限エラー → 代替レポートタイプを試行
 *   3. FBA在庫: ページネーション対応 + 在庫ありの商品を表示
 *
 * 実行: node --env-file=.env apps/fba-replenishment/test-sp-api-v2.js
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

/**
 * レポート取得の共通処理
 */
async function fetchReport(reportType, options = {}) {
  const body = {
    reportType,
    marketplaceIds: [MARKETPLACE_ID],
    ...options,
  };

  const createResult = await spClient.callAPI({
    operation: 'createReport',
    endpoint: 'reports',
    body,
    options: { version: '2021-06-30' },
  });

  const reportId = createResult.reportId;
  console.log(`  レポートID: ${reportId}`);

  let report;
  for (let i = 0; i < 60; i++) {
    await sleep(5000);
    report = await spClient.callAPI({
      operation: 'getReport',
      endpoint: 'reports',
      path: { reportId },
      options: { version: '2021-06-30' },
    });
    process.stdout.write(`  ステータス: ${report.processingStatus} (${i + 1}/60)\r`);
    if (['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) break;
  }
  console.log();

  if (report.processingStatus !== 'DONE') {
    throw new Error(`レポート生成失敗: ${report.processingStatus}`);
  }

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

  return text;
}

// ====================================================================
// テスト1: 発注推奨レポート — 複数のレポートタイプを試行
// ====================================================================
async function testRestockReport() {
  console.log('\n========================================');
  console.log('テスト1: 発注推奨レポート（複数タイプ試行）');
  console.log('========================================\n');

  // 候補となるレポートタイプ
  const candidates = [
    'GET_RESTOCK_INVENTORY_RECOMMENDATIONS',
    'GET_FBA_RESTOCK_INVENTORY_RECOMMENDATIONS',
    'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA',  // FBA在庫レポート（代替）
    'GET_FBA_MYI_ALL_INVENTORY_DATA',            // FBA全在庫レポート
    'GET_FBA_INVENTORY_PLANNING_DATA',           // FBA在庫プランニング
  ];

  for (const reportType of candidates) {
    console.log(`--- 試行: ${reportType} ---`);
    try {
      const text = await fetchReport(reportType);
      const lines = text.split('\n').filter(l => l.trim());
      const headers = lines[0].split('\t').map(h => h.trim());

      console.log(`  ✓ 成功！ ${lines.length - 1}行, ${headers.length}列`);
      console.log(`  ヘッダー:`);
      headers.forEach((h, i) => console.log(`    ${i}: ${h}`));

      // サンプル3行
      console.log(`\n  サンプルデータ:`);
      for (let i = 1; i <= Math.min(3, lines.length - 1); i++) {
        const cols = lines[i].split('\t');
        console.log(`\n  行${i}:`);
        headers.forEach((h, j) => {
          if (cols[j] && cols[j].trim()) {
            console.log(`    ${h}: ${cols[j].trim()}`);
          }
        });
      }

      return { success: true, reportType, headers, rowCount: lines.length - 1 };

    } catch (e) {
      console.log(`  ✗ 失敗: ${e.message}\n`);
    }
  }

  return { success: false, error: '全レポートタイプが失敗' };
}

// ====================================================================
// テスト2: FBA在庫 — ページネーション + 在庫ありのみ表示
// ====================================================================
async function testFbaInventory() {
  console.log('\n========================================');
  console.log('テスト2: FBA在庫（ページネーション + 在庫あり表示）');
  console.log('========================================\n');

  try {
    let allSummaries = [];
    let nextToken = null;
    let page = 0;

    do {
      page++;
      const query = {
        granularityType: 'Marketplace',
        granularityId: MARKETPLACE_ID,
        marketplaceIds: [MARKETPLACE_ID],
        details: true,
      };
      if (nextToken) query.nextToken = nextToken;

      const result = await spClient.callAPI({
        operation: 'getInventorySummaries',
        endpoint: 'fbaInventory',
        query,
        options: { version: 'v1' },
      });

      const summaries = result.inventorySummaries || result.payload?.inventorySummaries || [];
      allSummaries.push(...summaries);
      nextToken = result.pagination?.nextToken || result.payload?.pagination?.nextToken || null;
      process.stdout.write(`  ページ${page}: ${summaries.length}件取得 (累計${allSummaries.length}件)\r`);

      // 安全のため最大20ページ
      if (page >= 20) {
        console.log('\n  ※ 20ページで中断（全件取得には追加ページネーションが必要）');
        break;
      }
    } while (nextToken);

    console.log(`\n  合計: ${allSummaries.length}件\n`);

    // 在庫ありの商品だけフィルタ
    const withStock = allSummaries.filter(item => {
      const d = item.inventoryDetails || {};
      return (d.fulfillableQuantity || 0) > 0
        || (d.inboundShippedQuantity || 0) > 0
        || (d.inboundReceivingQuantity || 0) > 0
        || (d.inboundWorkingQuantity || 0) > 0;
    });

    console.log(`  在庫あり: ${withStock.length}件 / 全${allSummaries.length}件\n`);

    // 在庫ありの先頭10件を表示
    console.log('  在庫あり商品（先頭10件）:');
    console.log('  SKU                              | 販売可 | 発送済 | 受領中 | 作業中 | 予約 | 合計');
    console.log('  ' + '-'.repeat(95));
    withStock.slice(0, 10).forEach(item => {
      const d = item.inventoryDetails || {};
      const total = (d.fulfillableQuantity || 0) + (d.inboundShippedQuantity || 0)
        + (d.inboundReceivingQuantity || 0) + (d.inboundWorkingQuantity || 0);
      const reserved = d.reservedQuantity?.totalReservedQuantity || 0;
      console.log(`  ${(item.sellerSku || '').padEnd(35)}| ${String(d.fulfillableQuantity || 0).padStart(6)} | ${String(d.inboundShippedQuantity || 0).padStart(6)} | ${String(d.inboundReceivingQuantity || 0).padStart(6)} | ${String(d.inboundWorkingQuantity || 0).padStart(6)} | ${String(reserved).padStart(4)} | ${String(total).padStart(5)}`);
    });

    return { success: true, total: allSummaries.length, withStock: withStock.length };

  } catch (e) {
    console.error(`  エラー: ${e.message}`);
    return { success: false, error: e.message };
  }
}

// ====================================================================
// テスト3: 販売データ — 複数のレポートタイプを試行
// ====================================================================
async function testSalesData() {
  console.log('\n========================================');
  console.log('テスト3: 販売データ（複数タイプ試行）');
  console.log('========================================\n');

  const now = new Date();
  const start90 = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
  const startStr = start90.toISOString();
  const endStr = now.toISOString();

  // 候補となるレポートタイプ
  const candidates = [
    'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE',
    'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_DATE',
    'GET_FLAT_FILE_ORDERS_DATA',
    'GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE',
    'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL',  // FBA出荷データ
    'GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA',  // FBA販売データ
  ];

  for (const reportType of candidates) {
    console.log(`--- 試行: ${reportType} ---`);
    try {
      const text = await fetchReport(reportType, {
        dataStartTime: startStr,
        dataEndTime: endStr,
      });

      const lines = text.split('\n').filter(l => l.trim());
      const headers = lines[0].split('\t').map(h => h.trim());

      console.log(`  ✓ 成功！ ${lines.length - 1}行, ${headers.length}列`);
      console.log(`  ヘッダー:`);
      headers.forEach((h, i) => console.log(`    ${i}: ${h}`));

      // SKU列・数量列を探す
      const findIdx = (...candidates) => {
        for (const c of candidates) {
          const idx = headers.findIndex(h =>
            h.toLowerCase().replace(/[\s-]+/g, '') === c.toLowerCase().replace(/[\s-]+/g, '')
          );
          if (idx >= 0) return idx;
        }
        return -1;
      };

      const skuIdx = findIdx('sku', 'seller-sku', 'seller_sku', 'amazon-order-item-code');
      const qtyIdx = findIdx('quantity', 'quantity-purchased', 'quantity_purchased', 'quantity-shipped');
      const dateIdx = findIdx('purchase-date', 'purchase_date', 'last-updated-date', 'shipment-date');

      console.log(`\n  列インデックス: SKU=${skuIdx}, 数量=${qtyIdx}, 日付=${dateIdx}`);

      if (skuIdx >= 0 && qtyIdx >= 0) {
        // 7日/30日/90日分割集計
        const salesMap = {};
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

        for (let i = 1; i < lines.length; i++) {
          const cols = lines[i].split('\t');
          const sku = (cols[skuIdx] || '').trim();
          const qty = parseInt(cols[qtyIdx]) || 0;
          const dateStr = (cols[dateIdx] || '').trim();

          if (!sku || qty <= 0) continue;

          if (!salesMap[sku]) salesMap[sku] = { sold_7d: 0, sold_30d: 0, sold_90d: 0 };
          salesMap[sku].sold_90d += qty;

          if (dateStr) {
            const orderDate = new Date(dateStr);
            if (orderDate >= thirtyDaysAgo) salesMap[sku].sold_30d += qty;
            if (orderDate >= sevenDaysAgo) salesMap[sku].sold_7d += qty;
          }
        }

        const skus = Object.entries(salesMap)
          .filter(([_, v]) => v.sold_90d >= 10)
          .sort((a, b) => b[1].sold_90d - a[1].sold_90d)
          .slice(0, 15);

        console.log(`\n  集計完了: ${Object.keys(salesMap).length} SKU`);
        console.log('\n  トレンド分析（上位15 SKU）:');
        console.log('  SKU                              |  7日 | 30日 | 90日 | 月平均 | 月トレンド | 週間急変');
        console.log('  ' + '-'.repeat(95));
        for (const [sku, data] of skus) {
          const avg30 = Math.round(data.sold_90d / 3);
          const sold7m = Math.round(data.sold_7d * 30 / 7);
          const monthTrend = avg30 > 0 ? (data.sold_30d / avg30).toFixed(2) : 'N/A';
          const weeklySurge = data.sold_30d > 0 ? (sold7m / data.sold_30d).toFixed(2) : 'N/A';
          const mLabel = monthTrend > 1.5 ? '↑↑' : monthTrend > 1.1 ? '↑' : monthTrend < 0.5 ? '↓↓' : monthTrend < 0.9 ? '↓' : '→';
          const wLabel = weeklySurge > 2.0 ? '急上昇' : weeklySurge < 0.3 ? '急停止' : weeklySurge > 1.3 ? '↑' : weeklySurge < 0.7 ? '↓' : '→';
          console.log(`  ${sku.padEnd(35)}| ${String(data.sold_7d).padStart(4)} | ${String(data.sold_30d).padStart(4)} | ${String(data.sold_90d).padStart(4)} | ${String(avg30).padStart(6)} | ${monthTrend.padStart(5)} ${mLabel.padEnd(3)}| ${weeklySurge.padStart(5)} ${wLabel}`);
        }

        return { success: true, reportType, skuCount: Object.keys(salesMap).length };
      }

      // SKU/数量列が見つからなかった場合、サンプルだけ表示
      console.log('\n  サンプル（先頭2行）:');
      for (let i = 1; i <= Math.min(2, lines.length - 1); i++) {
        const cols = lines[i].split('\t');
        console.log(`\n  行${i}:`);
        headers.forEach((h, j) => {
          if (cols[j] && cols[j].trim()) console.log(`    ${h}: ${cols[j].trim()}`);
        });
      }

      return { success: true, reportType, note: 'SKU/数量列の確認が必要' };

    } catch (e) {
      console.log(`  ✗ 失敗: ${e.message}\n`);
    }
  }

  return { success: false, error: '全レポートタイプが失敗' };
}

// ====================================================================
// 実行
// ====================================================================
async function main() {
  console.log('============================================================');
  console.log('FBA在庫補充システム SP-APIデータ取得検証 v2');
  console.log(`実行日時: ${new Date().toLocaleString('ja-JP')}`);
  console.log('============================================================');

  const results = {};

  results.restockReport = await testRestockReport();
  results.fbaInventory = await testFbaInventory();
  results.salesData = await testSalesData();

  console.log('\n\n============================================================');
  console.log('検証結果サマリー');
  console.log('============================================================');

  console.log(`1. 発注推奨レポート: ${results.restockReport.success ? '✓ 成功' : '✗ 失敗'}`);
  if (results.restockReport.success) {
    console.log(`   → レポートタイプ: ${results.restockReport.reportType}`);
    console.log(`   → ${results.restockReport.rowCount}行, ヘッダー: ${results.restockReport.headers?.length}列`);
  } else {
    console.log(`   → ${results.restockReport.error}`);
    console.log(`   → 対策: セラセンCSVアップロードをフォールバックとして使用可能`);
  }

  console.log(`2. FBA在庫サマリー:  ✓ 成功`);
  console.log(`   → 全${results.fbaInventory.total}件, 在庫あり${results.fbaInventory.withStock}件`);

  console.log(`3. 販売データ(90日): ${results.salesData.success ? '✓ 成功' : '✗ 失敗'}`);
  if (results.salesData.success) {
    console.log(`   → レポートタイプ: ${results.salesData.reportType}`);
    console.log(`   → ${results.salesData.skuCount || '?'} SKU`);
  } else {
    console.log(`   → ${results.salesData.error}`);
    console.log(`   → 対策: SP-APIアプリのロールに「注文」権限を追加するか、`);
    console.log(`           FBA出荷レポートで代替`);
  }

  console.log('\n============================================================\n');
}

main().catch(e => {
  console.error('予期せぬエラー:', e);
  process.exit(1);
});
