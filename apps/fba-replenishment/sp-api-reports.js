/**
 * SP-API レポート取得 — FBA在庫補充システム
 *
 * 3つのレポートを取得:
 *   1. GET_FBA_INVENTORY_PLANNING_DATA (85列) — メインデータソース
 *   2. GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT (28列) — Amazon推奨数
 *   3. GET_FBA_MYI_ALL_INVENTORY_DATA (22列) — FBA在庫内訳
 */
import SellingPartner from 'amazon-sp-api';
import iconv from 'iconv-lite';
import { gunzipSync } from 'zlib';

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

const MARKETPLACE_ID = () => process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * レポートをリクエスト → ポーリング → ダウンロード → TSV解析
 */
async function fetchReport(reportType, options = {}) {
  const sp = getClient();
  const body = { reportType, marketplaceIds: [MARKETPLACE_ID()], ...options };

  // レポート作成
  const createResult = await sp.callAPI({
    operation: 'createReport', endpoint: 'reports', body,
    options: { version: '2021-06-30' },
  });
  const reportId = createResult.reportId;
  console.log(`[SP-API] ${reportType} レポートID: ${reportId}`);

  // ポーリング（最大5分）
  let report;
  for (let i = 0; i < 60; i++) {
    await sleep(5000);
    report = await sp.callAPI({
      operation: 'getReport', endpoint: 'reports',
      path: { reportId }, options: { version: '2021-06-30' },
    });
    if (['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) break;
  }

  if (report.processingStatus !== 'DONE') {
    throw new Error(`レポート失敗: ${reportType} → ${report.processingStatus}`);
  }

  // ドキュメント取得
  const doc = await sp.callAPI({
    operation: 'getReportDocument', endpoint: 'reports',
    path: { reportDocumentId: report.reportDocumentId },
    options: { version: '2021-06-30' },
  });

  const response = await fetch(doc.url);
  const rawBuf = Buffer.from(await response.arrayBuffer());
  let dataBuf = rawBuf;
  if (doc.compressionAlgorithm === 'GZIP') {
    dataBuf = gunzipSync(rawBuf);
  }

  // 文字コード判定: UTF-8を優先、無効ならShift_JIS
  let text;
  const utf8Text = dataBuf.toString('utf-8');
  if (utf8Text.includes('\ufffd') || /[\x80-\xff]/.test(utf8Text.slice(0, 500).replace(/[\u0080-\uffff]/g, ''))) {
    // UTF-8で不正文字がある → Shift_JISとしてデコード
    text = iconv.decode(dataBuf, 'Shift_JIS');
  } else {
    text = utf8Text;
  }

  return parseTsv(text);
}

/**
 * TSVテキストをオブジェクト配列に変換
 */
function parseTsv(text) {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];

  const headers = lines[0].split('\t').map(h => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split('\t');
    const row = {};
    headers.forEach((h, j) => {
      row[h] = (cols[j] || '').trim();
    });
    rows.push(row);
  }

  return rows;
}

// ======================================================
// レポート1: FBA在庫計画データ（85列、メインデータソース）
// ======================================================
export async function fetchPlanningData() {
  console.log('[SP-API] FBA在庫計画データを取得中...');
  const rows = await fetchReport('GET_FBA_INVENTORY_PLANNING_DATA');
  console.log(`[SP-API] FBA在庫計画: ${rows.length}件取得`);
  return rows;
}

// ======================================================
// レポート2: 発注推奨レポート（28列）
// ======================================================
export async function fetchRestockRecommendations() {
  console.log('[SP-API] 発注推奨レポートを取得中...');
  const rows = await fetchReport('GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT');
  console.log(`[SP-API] 発注推奨: ${rows.length}件取得`);
  return rows;
}

// ======================================================
// レポート3: FBA在庫内訳（全SKU、22列）
// ======================================================
export async function fetchFbaInventory() {
  // ALL_INVENTORY_DATAがFATALになることがあるのでフォールバック付き
  const reportTypes = [
    'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA',
    'GET_FBA_MYI_ALL_INVENTORY_DATA',
  ];
  for (const reportType of reportTypes) {
    try {
      console.log(`[SP-API] FBA在庫内訳を取得中... (${reportType})`);
      const rows = await fetchReport(reportType);
      console.log(`[SP-API] FBA在庫内訳: ${rows.length}件取得 (${reportType})`);
      return rows;
    } catch (e) {
      console.error(`[SP-API] ${reportType} 失敗: ${e.message}、次のレポートを試行`);
    }
  }
  throw new Error('FBA在庫内訳: すべてのレポートタイプが失敗');
}

// ======================================================
// 全レポートまとめて取得
// ======================================================
export async function fetchAllReports() {
  const results = { planning: null, restock: null, inventory: null, errors: [] };

  // 順番に取得（API制限回避のため並列にしない）
  try {
    results.planning = await fetchPlanningData();
  } catch (e) {
    console.error('[SP-API] PLANNING_DATA 失敗:', e.message);
    results.errors.push({ report: 'planning', error: e.message });
  }

  try {
    results.restock = await fetchRestockRecommendations();
  } catch (e) {
    console.error('[SP-API] RESTOCK 失敗:', e.message);
    results.errors.push({ report: 'restock', error: e.message });
  }

  try {
    results.inventory = await fetchFbaInventory();
  } catch (e) {
    console.error('[SP-API] INVENTORY 失敗:', e.message);
    results.errors.push({ report: 'inventory', error: e.message });
  }

  return results;
}

/**
 * PLANNINGデータから必要列を正規化して返す
 * （列名はレポートのヘッダーに依存するため、柔軟にマッピング）
 */
export function normalizePlanningRow(raw) {
  return {
    sku: raw['sku'] || raw['merchant-sku'] || '',
    asin: raw['asin'] || '',
    product_name: raw['product-name'] || '',
    // 在庫
    fba_available: parseInt(raw['available'] || raw['afn-fulfillable-quantity'] || 0),
    fba_inbound_working: parseInt(raw['inbound-working'] || 0),
    fba_inbound_shipped: parseInt(raw['inbound-shipped'] || 0),
    fba_inbound_received: parseInt(raw['inbound-received'] || 0),
    fba_reserved: parseInt(raw['Total Reserved Quantity'] || raw['reserved-quantity'] || 0),
    fba_unfulfillable: parseInt(raw['unfulfillable-quantity'] || 0),
    // 販売データ
    units_sold_7d: parseInt(raw['units-shipped-t7'] || 0),
    units_sold_30d: parseInt(raw['units-shipped-t30'] || 0),
    units_sold_60d: parseInt(raw['units-shipped-t60'] || 0),
    units_sold_90d: parseInt(raw['units-shipped-t90'] || 0),
    sales_7d: parseFloat(raw['sales-shipped-last-7-days'] || 0),
    sales_30d: parseFloat(raw['sales-shipped-last-30-days'] || 0),
    sales_60d: parseFloat(raw['sales-shipped-last-60-days'] || 0),
    sales_90d: parseFloat(raw['sales-shipped-last-90-days'] || 0),
    // 在庫計画
    days_of_supply: parseFloat(raw['days-of-supply'] || 0),
    weeks_of_cover_t30: parseFloat(raw['weeks-of-cover-t30'] || 0),
    weeks_of_cover_t90: parseFloat(raw['weeks-of-cover-t90'] || 0),
    recommended_ship_qty: parseInt(raw['Recommended ship-in quantity'] || raw['recommended-ship-in-quantity'] || 0),
    recommended_ship_date: raw['Recommended ship-in date'] || raw['recommended-ship-in-date'] || '',
    // 手数料
    short_term_dos: parseFloat(raw['Short term historical days of supply'] || 0),
    long_term_dos: parseFloat(raw['Long term historical days of supply'] || 0),
    low_inv_fee_exempt: raw['Exempted from Low-Inventory-Level fee?'] || '',
    low_inv_fee_applied: raw['Low-Inventory-Level fee applied in current week?'] || '',
    estimated_storage_cost: parseFloat(raw['estimated-storage-cost-next-month'] || 0),
    estimated_excess_qty: parseInt(raw['estimated-excess-quantity'] || 0),
    // 季節商品
    is_seasonal: raw['is-seasonal-in-next-3-months'] || '',
    season_name: raw['season-name'] || '',
    season_start: raw['season-start-date'] || '',
    season_end: raw['season-end-date'] || '',
    // カート価格
    your_price: parseFloat(raw['your-price'] || 0),
    featured_offer_price: parseFloat(raw['featuredoffer-price'] || 0),
    lowest_price: parseFloat(raw['lowest-price-new-plus-shipping'] || 0),
    sales_rank: parseInt(raw['sales-rank'] || 0),
    // サイズ
    per_unit_volume: parseFloat(raw['per-unit-volume'] || 0),
    storage_type: raw['storage-type'] || '',
  };
}
