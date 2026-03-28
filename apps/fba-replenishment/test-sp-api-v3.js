/**
 * SP-API データ取得検証 v3
 *
 * v2からの修正:
 *   1. 発注推奨: GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT（末尾_REPORT）
 *   2. 販売データ: Sales API getOrderMetrics（PII不要）
 *   3. 販売レポート: GET_SALES_AND_TRAFFIC_REPORT（PII不要）
 *   4. FBA計画: GET_FBA_INVENTORY_PLANNING_DATA
 *
 * 実行: node --env-file=.env apps/fba-replenishment/test-sp-api-v3.js
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

async function fetchReport(reportType, options = {}) {
  const body = { reportType, marketplaceIds: [MARKETPLACE_ID], ...options };
  const createResult = await spClient.callAPI({
    operation: 'createReport', endpoint: 'reports', body,
    options: { version: '2021-06-30' },
  });
  const reportId = createResult.reportId;
  console.log(`  レポートID: ${reportId}`);

  let report;
  for (let i = 0; i < 60; i++) {
    await sleep(5000);
    report = await spClient.callAPI({
      operation: 'getReport', endpoint: 'reports',
      path: { reportId }, options: { version: '2021-06-30' },
    });
    process.stdout.write(`  ステータス: ${report.processingStatus} (${i + 1}/60)\r`);
    if (['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) break;
  }
  console.log();
  if (report.processingStatus !== 'DONE') throw new Error(`レポート失敗: ${report.processingStatus}`);

  const doc = await spClient.callAPI({
    operation: 'getReportDocument', endpoint: 'reports',
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
  } catch { text = dataBuf.toString('utf-8'); }
  return text;
}

function showReportSample(text, maxRows = 3) {
  const lines = text.split('\n').filter(l => l.trim());
  const headers = lines[0].split('\t').map(h => h.trim());
  console.log(`  ${lines.length - 1}行, ${headers.length}列`);
  console.log(`  ヘッダー:`);
  headers.forEach((h, i) => console.log(`    ${i}: ${h}`));
  for (let i = 1; i <= Math.min(maxRows, lines.length - 1); i++) {
    const cols = lines[i].split('\t');
    console.log(`\n  行${i}:`);
    headers.forEach((h, j) => {
      if (cols[j] && cols[j].trim()) console.log(`    ${h}: ${cols[j].trim()}`);
    });
  }
  return { headers, rowCount: lines.length - 1 };
}

// ====================================================================
// テスト1: 発注推奨レポート（正しい名前で再試行）
// ====================================================================
async function testRestockReport() {
  console.log('\n========================================');
  console.log('テスト1: 発注推奨レポート');
  console.log('========================================\n');

  const candidates = [
    'GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT',
    'GET_FBA_INVENTORY_PLANNING_DATA',
  ];

  for (const reportType of candidates) {
    console.log(`--- ${reportType} ---`);
    try {
      const text = await fetchReport(reportType);
      console.log(`  ✓ 成功！`);
      const result = showReportSample(text);
      return { success: true, reportType, ...result };
    } catch (e) {
      console.log(`  ✗ 失敗: ${e.message}\n`);
    }
  }
  return { success: false, error: '全タイプ失敗' };
}

// ====================================================================
// テスト2: Sales API (getOrderMetrics) — PII不要
// ====================================================================
async function testSalesAPI() {
  console.log('\n========================================');
  console.log('テスト2: Sales API (getOrderMetrics)');
  console.log('========================================\n');

  try {
    const now = new Date();
    const start90 = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
    const interval = `${start90.toISOString().split('.')[0]}Z--${now.toISOString().split('.')[0]}Z`;

    console.log(`  期間: ${start90.toISOString().slice(0,10)} ～ ${now.toISOString().slice(0,10)}`);
    console.log(`  interval: ${interval}`);

    const result = await spClient.callAPI({
      operation: 'getOrderMetrics',
      endpoint: 'sales',
      query: {
        marketplaceIds: [MARKETPLACE_ID],
        interval,
        granularity: 'Day',
      },
    });

    const metrics = result.payload || result || [];
    console.log(`\n  ✓ 成功！ ${Array.isArray(metrics) ? metrics.length : '?'}件のメトリクス`);

    // サンプル表示
    const arr = Array.isArray(metrics) ? metrics : [metrics];
    arr.slice(0, 5).forEach((m, i) => {
      console.log(`\n  ${i + 1}. ${JSON.stringify(m, null, 2).slice(0, 300)}`);
    });

    return { success: true, count: arr.length };

  } catch (e) {
    console.error(`  ✗ 失敗: ${e.message}`);
    if (e.details) console.error(`    詳細: ${JSON.stringify(e.details).slice(0, 300)}`);
    return { success: false, error: e.message };
  }
}

// ====================================================================
// テスト3: 販売レポート（PII不要の候補）
// ====================================================================
async function testSalesReports() {
  console.log('\n========================================');
  console.log('テスト3: 販売レポート（PII不要）');
  console.log('========================================\n');

  const now = new Date();
  const start90 = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);

  const candidates = [
    { type: 'GET_SALES_AND_TRAFFIC_REPORT', options: { reportOptions: { dateGranularity: 'DAY', asinGranularity: 'SKU' }, dataStartTime: start90.toISOString(), dataEndTime: now.toISOString() } },
    { type: 'GET_FBA_SNS_FORECAST_DATA', options: {} },
    { type: 'GET_FBA_INVENTORY_PLANNING_DATA', options: {} },
  ];

  for (const { type, options } of candidates) {
    console.log(`--- ${type} ---`);
    try {
      const text = await fetchReport(type, options);
      console.log(`  ✓ 成功！`);
      const result = showReportSample(text, 2);
      return { success: true, reportType: type, ...result };
    } catch (e) {
      console.log(`  ✗ 失敗: ${e.message}\n`);
    }
  }
  return { success: false, error: '全タイプ失敗' };
}

// ====================================================================
// 実行
// ====================================================================
async function main() {
  console.log('============================================================');
  console.log('FBA在庫補充システム SP-APIデータ取得検証 v3');
  console.log(`実行日時: ${new Date().toLocaleString('ja-JP')}`);
  console.log('============================================================');

  const r = {};
  r.restock = await testRestockReport();
  r.salesApi = await testSalesAPI();
  r.salesReport = await testSalesReports();

  console.log('\n\n============================================================');
  console.log('検証結果サマリー');
  console.log('============================================================');
  console.log(`1. 発注推奨レポート: ${r.restock.success ? '✓ 成功 → ' + r.restock.reportType : '✗ ' + r.restock.error}`);
  console.log(`2. Sales API:        ${r.salesApi.success ? '✓ 成功 → ' + r.salesApi.count + '件' : '✗ ' + r.salesApi.error}`);
  console.log(`3. 販売レポート:     ${r.salesReport.success ? '✓ 成功 → ' + r.salesReport.reportType : '✗ ' + r.salesReport.error}`);
  console.log('============================================================\n');
}

main().catch(e => { console.error('エラー:', e); process.exit(1); });
