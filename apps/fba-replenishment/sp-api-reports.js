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

function withTimeout(promise, ms, label = '') {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`タイムアウト (${ms/1000}秒): ${label}`)), ms)
    ),
  ]);
}

/**
 * レポートをリクエスト → ポーリング → ダウンロード → TSV解析
 */
async function fetchReport(reportType, options = {}) {
  const sp = getClient();
  const body = { reportType, marketplaceIds: [MARKETPLACE_ID()], ...options };

  // レポート作成（60秒タイムアウト）
  const createResult = await withTimeout(
    sp.callAPI({ operation: 'createReport', endpoint: 'reports', body, options: { version: '2021-06-30' } }),
    60000, `${reportType} createReport`
  );
  const reportId = createResult.reportId;
  console.log(`[SP-API] ${reportType} レポートID: ${reportId}`);

  // ポーリング（最大5分、各API呼び出し15秒タイムアウト）
  let report;
  for (let i = 0; i < 60; i++) {
    await sleep(5000);
    report = await withTimeout(
      sp.callAPI({ operation: 'getReport', endpoint: 'reports', path: { reportId }, options: { version: '2021-06-30' } }),
      15000, `${reportType} getReport`
    );
    console.log(`[SP-API] ${reportType} ポーリング ${i+1}/60: ${report.processingStatus}`);
    if (['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) break;
  }

  if (report.processingStatus !== 'DONE') {
    throw new Error(`レポート失敗: ${reportType} → ${report.processingStatus}`);
  }

  // ドキュメント取得（30秒タイムアウト）
  const doc = await withTimeout(
    sp.callAPI({ operation: 'getReportDocument', endpoint: 'reports', path: { reportDocumentId: report.reportDocumentId }, options: { version: '2021-06-30' } }),
    30000, `${reportType} getReportDocument`
  );

  const response = await fetch(doc.url, { signal: AbortSignal.timeout(60000) });
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
// INVENTORY レポートは RESTOCK の部分集合で冗長 + 未使用のため取得停止
export async function fetchAllReports() {
  const results = { planning: null, restock: null, inventory: null, errors: [] };

  // 順次実行（SP-APIのcreateReport競合を完全に回避）
  console.log('[SP-API] RESTOCK + PLANNING を順次取得開始...');

  // 主軸: RESTOCK (必須)
  try {
    results.restock = await fetchRestockRecommendations();
  } catch (e) {
    console.error('[SP-API] RESTOCK 失敗:', e.message);
    results.errors.push({ report: 'restock', error: e.message });
  }

  // 補助: PLANNING (欠落許容、60/90日販売などの参考情報用)
  try {
    results.planning = await fetchPlanningData();
  } catch (e) {
    console.error('[SP-API] PLANNING_DATA 失敗:', e.message);
    results.errors.push({ report: 'planning', error: e.message });
  }

  console.log('[SP-API] 順次取得完了');
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

/**
 * RESTOCKデータから必要列を正規化して返す
 * 列名は英語/日本語どちらでも来るため両対応
 *
 * NOTE: amazon_recommended_qty は `null` と `0` を区別する (0は明示的推奨、nullは列欠損)
 */
export function normalizeRestockRow(raw) {
  // 数値フィールドで「空文字は null、それ以外はパース」を区別するユーティリティ
  const parseIntOrNull = (v) => {
    if (v === undefined || v === null || v === '') return null;
    const n = parseInt(v);
    return Number.isFinite(n) ? n : null;
  };
  const parseFloatOrNull = (v) => {
    if (v === undefined || v === null || v === '') return null;
    const n = parseFloat(v);
    return Number.isFinite(n) ? n : null;
  };
  const pick = (...keys) => {
    for (const k of keys) {
      if (raw[k] !== undefined && raw[k] !== '') return raw[k];
    }
    return undefined;
  };

  const sku = pick('Merchant SKU', 'sku', 'merchant-sku') || '';
  const asin = pick('ASIN', 'asin') || '';
  const fnsku = pick('FNSKU', 'fnsku') || '';
  const productName = pick('Product Name', '商品名', 'product-name') || '';

  return {
    amazon_sku: sku,
    fnsku,
    asin,
    product_name: productName,

    // 在庫内訳 (在庫にある = Available)
    fba_available: parseInt(pick('Available', '在庫にある', 'available') || 0),
    // 進行中 (Working) — レポート側は信頼せず、Inbound API override で上書き前提
    fba_inbound_working: parseInt(pick('Working', '進行中', 'working') || 0),
    fba_inbound_shipped: parseInt(pick('Shipped', '出荷済み', 'shipped') || 0),
    fba_inbound_received: parseInt(pick('Receiving', '受領中', 'receiving') || 0),
    fba_unfulfillable: parseInt(pick('Unfulfillable', '販売不可', 'unfulfillable') || 0),
    // FBA倉庫内の追加3区分 (月末棚卸しツールの fba_warehouse 計算に必須):
    //   fba_warehouse = fba_available + fc_transfer + fc_processing + customer_order
    fba_fc_transfer: parseInt(pick('FC Transfer', 'FC移管中', 'fc-transfer') || 0),
    fba_fc_processing: parseInt(pick('FC Processing', '入出荷作業中 - FC処理中', '入出荷作業中-FC処理中', 'fc-processing') || 0),
    fba_customer_order: parseInt(pick('Customer Order', '入出荷作業中 - 出荷待ち', '入出荷作業中-出荷待ち', 'customer-order') || 0),

    // 販売データ (RESTOCKは30日のみ、7/60/90日はPLANNING補助)
    units_sold_30d: parseInt(pick('Units Sold Last 30 Days', '過去30日間に販売されたユニット数', 'units-sold-last-30-days') || 0),

    // Amazon推奨数: null許容 (0 と未取得を区別)
    amazon_recommended_qty: parseIntOrNull(pick('Recommended replenishment qty', '推奨される在庫補充数', 'recommended-replenishment-qty')),
    amazon_recommended_date: pick('Recommended ship date', '推奨発送日', 'recommended-ship-date') || null,

    // 警告 (out_of_stock / low_stock / null)
    alert_type: pick('Alert', '警告', 'alert') || null,

    // 価格
    your_price: parseFloatOrNull(pick('Price', '価格', 'price')),

    // 供給日数 (AmazonのML計算値)
    days_of_supply: parseFloatOrNull(pick('Days of Supply at Amazon Fulfillment Network', 'Amazonフルフィルメントセンターでの在庫日数', 'days-of-supply-at-amazon-fulfillment-network')),
  };
}
