/**
 * SP-API クライアント — ES module wrapper
 *
 * Environment variables:
 *   SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, SP_API_REFRESH_TOKEN
 *   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
 *   SP_API_MARKETPLACE_ID (default: A1VC38T7YXB528)
 */
import SellingPartner from 'amazon-sp-api';

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

/**
 * 商品情報取得（カタログ + 価格 + ランキング）
 */
export async function getProduct(asin) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  // カタログ情報
  const catalog = await sp.callAPI({
    operation: 'getCatalogItem',
    endpoint: 'catalogItems',
    path: { asin },
    query: {
      marketplaceIds: [marketplaceId],
      includedData: ['summaries', 'images', 'dimensions', 'identifiers', 'attributes'],
    },
    options: { version: '2022-04-01' },
  });

  const summary = catalog.summaries?.[0] || {};
  const images = catalog.images?.[0]?.images || [];
  const dims = catalog.dimensions?.[0]?.package || {};
  const identifiers = catalog.identifiers?.[0]?.identifiers || [];
  const attrs = catalog.attributes || {};

  const mainImage = images.find(img => img.variant === 'MAIN' && img.height === 500)?.link
    || images.find(img => img.variant === 'MAIN')?.link || '';

  // JAN/EAN
  const ean = identifiers.find(i => i.identifierType === 'EAN')?.identifier || '';
  // 型番
  const partNumber = attrs.part_number?.[0]?.value || '';
  // メーカー名
  const manufacturer = attrs.manufacturer?.[0]?.value || '';

  const toCm = (inches) => inches ? (inches * 2.54).toFixed(1) : '-';
  const toKg = (pounds) => pounds ? (pounds * 0.4536).toFixed(2) : '-';

  const lengthCm = dims.length?.value ? dims.length.value * 2.54 : 0;
  const widthCm = dims.width?.value ? dims.width.value * 2.54 : 0;
  const heightCm = dims.height?.value ? dims.height.value * 2.54 : 0;
  const volumeCm3 = lengthCm * widthCm * heightCm;

  // 競合価格・ランキング
  let currentPrice = 0;
  let salesRank = '-';
  let offerCount = 0;
  try {
    const pricing = await sp.callAPI({
      operation: 'getCompetitivePricing',
      endpoint: 'productPricing',
      query: { MarketplaceId: marketplaceId, Asins: [asin], ItemType: 'Asin' },
    });
    const product = pricing?.[0]?.Product;
    if (product) {
      const cps = product.CompetitivePricing?.CompetitivePrices || [];
      // カート価格（BuyBoxPrice）を優先、なければ最安値
      const buyBox = cps.find(c => c.CompetitivePriceId === '1' && c.condition === 'New');
      const lowest = cps.find(c => c.CompetitivePriceId === '2' && c.condition === 'New');
      const bestPrice = buyBox || lowest || cps[0];
      currentPrice = bestPrice?.Price?.ListingPrice?.Amount || 0;
      salesRank = product.SalesRankings?.[0]?.Rank || '-';
      offerCount = product.CompetitivePricing?.NumberOfOfferListings
        ?.find(n => n.condition === 'New')?.Count || 0;
    }
  } catch (e) {
    console.error('[SP-API] 価格取得エラー:', e.message);
  }

  return {
    asin,
    itemName: summary.itemName || '',
    brand: summary.brandName || summary.brand || '',
    category: summary.browseClassification?.displayName || '',
    size: summary.size || '',
    image: mainImage,
    dimensions: {
      length: toCm(dims.length?.value),
      width: toCm(dims.width?.value),
      height: toCm(dims.height?.value),
      weight: toKg(dims.weight?.value),
    },
    volumeCm3,
    currentPrice,
    salesRank,
    offerCount,
    jan: ean,
    partNumber: partNumber !== 'unknown' ? partNumber : '',
    manufacturer,
  };
}

/**
 * FBA/FBM手数料取得
 */
export async function getFees(asin, price, isFba = true) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  const result = await sp.callAPI({
    operation: 'getMyFeesEstimateForASIN',
    endpoint: 'productFees',
    path: { Asin: asin },
    body: {
      FeesEstimateRequest: {
        MarketplaceId: marketplaceId,
        IsAmazonFulfilled: isFba,
        PriceToEstimateFees: {
          ListingPrice: { CurrencyCode: 'JPY', Amount: price },
          Shipping: { CurrencyCode: 'JPY', Amount: 0 },
        },
        Identifier: `${isFba ? 'fba' : 'fbm'}-${asin}-${Date.now()}`,
      },
    },
  });

  const r = result.FeesEstimateResult;
  if (r.Status !== 'Success') {
    throw new Error(`手数料取得失敗: ${r.Error?.Code} - ${r.Error?.Message}`);
  }

  const feeList = r.FeesEstimate.FeeDetailList;
  return {
    referralFee: feeList.find(f => f.FeeType === 'ReferralFee')?.FeeAmount?.Amount || 0,
    fbaFee: feeList.find(f => f.FeeType === 'FBAFees')?.FeeAmount?.Amount || 0,
    variableClosingFee: feeList.find(f => f.FeeType === 'VariableClosingFee')?.FeeAmount?.Amount || 0,
    perItemFee: feeList.find(f => f.FeeType === 'PerItemFee')?.FeeAmount?.Amount || 0,
    totalFee: r.FeesEstimate.TotalFeesEstimate.Amount,
  };
}

const SELLER_ID = () => process.env.SP_API_SELLER_ID || 'A6HMLHKUUJC27';

/**
 * SKU自動採番（FBA用）: pr_YYYYMMDD_連番
 */
function generateFbaSku() {
  const d = new Date();
  const date = d.toISOString().slice(0, 10).replace(/-/g, '');
  const serial = String(d.getTime()).slice(-4);
  return `pr_${date}_${serial}`;
}

/**
 * 配送テンプレート一覧取得（Product Type Definitions API）
 */
export async function getShippingTemplates() {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();
  const sellerId = SELLER_ID();

  // Product Type Definitions APIでスキーマ取得
  const result = await sp.callAPI({
    operation: 'getDefinitionsProductType',
    endpoint: 'productTypeDefinitions',
    path: { productType: 'PRODUCT' },
    query: {
      marketplaceIds: [marketplaceId],
      sellerId,
      requirements: 'LISTING',
      locale: 'ja_JP',
    },
    options: { version: '2020-09-01' },
  });

  console.log('[SP-API] getDefinitionsProductType: schema link =', result.schema?.link?.resource);

  // スキーマURLからJSONスキーマを取得
  const schemaUrl = result.schema?.link?.resource;
  if (!schemaUrl) throw new Error('スキーマURLが取得できません');

  const schemaRes = await fetch(schemaUrl);
  const schema = await schemaRes.json();

  // merchant_shipping_group の有効値を探索
  const prop = schema.properties?.merchant_shipping_group;
  if (!prop) {
    console.log('[SP-API] merchant_shipping_group not found in schema. Available keys:', Object.keys(schema.properties || {}).filter(k => k.includes('shipping')));
    throw new Error('配送テンプレート定義がスキーマに見つかりません');
  }

  // items.properties.value にenum/enumNamesがある想定
  const valueProp = prop.items?.properties?.value || prop.properties?.value || {};
  const values = valueProp.enum || [];
  const names = valueProp.enumNames || values;

  console.log(`[SP-API] 配送テンプレート: ${values.length}件取得`);

  return values.map((v, i) => ({ value: v, label: names[i] || v }));
}

/**
 * JANコード or キーワードで Amazon商品を検索 → ASIN取得
 */
export async function searchByJan(jan) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  const result = await sp.callAPI({
    operation: 'searchCatalogItems',
    endpoint: 'catalogItems',
    query: {
      marketplaceIds: [marketplaceId],
      identifiers: [jan],
      identifiersType: 'EAN',
      includedData: ['summaries', 'images'],
      pageSize: 5,
    },
    options: { version: '2022-04-01' },
  });

  const items = result.items || [];
  return items.map(item => ({
    asin: item.asin,
    itemName: item.summaries?.[0]?.itemName || '',
    image: item.images?.[0]?.images?.find(i => i.variant === 'MAIN')?.link || '',
  }));
}

/**
 * キーワードで Amazon商品を検索
 */
export async function searchByKeyword(keyword) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  const result = await sp.callAPI({
    operation: 'searchCatalogItems',
    endpoint: 'catalogItems',
    query: {
      marketplaceIds: [marketplaceId],
      keywords: [keyword],
      includedData: ['summaries', 'images'],
      pageSize: 3,
    },
    options: { version: '2022-04-01' },
  });

  const items = result.items || [];
  return items.map(item => ({
    asin: item.asin,
    itemName: item.summaries?.[0]?.itemName || '',
    image: item.images?.[0]?.images?.find(i => i.variant === 'MAIN')?.link || '',
  }));
}

/**
 * BSR（ランキング）から月間販売数を推定
 * Amazon Japan向けの概算式（あくまで目安）
 */
export function estimateMonthlySales(rank) {
  if (!rank || rank <= 0 || rank === '-') return null;
  const r = Number(rank);
  if (isNaN(r)) return null;
  // 対数スケールの概算式
  return Math.max(1, Math.round(Math.pow(10, 3.8 - 0.7 * Math.log10(r))));
}

/**
 * Amazon出品登録（Listings Items API）
 */
export async function createListing({ asin, price, isFba, sku, condition = 'new_new', shippingTemplate = null, paymentRestriction = 'none' }) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();
  const sellerId = SELLER_ID();

  // SKU決定
  const finalSku = sku || (isFba ? generateFbaSku() : null);
  if (!finalSku) throw new Error('SKUが必要です（FBMの場合はNE商品コードを指定）');
  if (!asin) throw new Error('ASINが必要です');
  if (!price || price <= 0) throw new Error('販売価格が必要です');

  // Listings Items API - putListingsItem
  const attributes = {
    condition_type: [{ value: condition, marketplace_id: marketplaceId }],
    merchant_suggested_asin: [{ value: asin, marketplace_id: marketplaceId }],
    purchasable_offer: [{
      marketplace_id: marketplaceId,
      currency: 'JPY',
      our_price: [{ schedule: [{ value_with_tax: price }] }],
    }],
    fulfillment_availability: [{
      fulfillment_channel_code: isFba ? 'AMAZON_JP' : 'DEFAULT',
      marketplace_id: marketplaceId,
    }],
  };

  // FBM: 配送テンプレートのみputで設定（LISTING_OFFER_ONLYモード）
  // 支払い制限はLISTING_OFFER_ONLYでは適用されないため、出品後にpatchListingsItemで設定する
  if (!isFba) {
    if (shippingTemplate) {
      attributes.merchant_shipping_group = [{ value: shippingTemplate, marketplace_id: marketplaceId }];
    }
  }

  const body = {
    productType: 'PRODUCT',
    requirements: 'LISTING_OFFER_ONLY',
    attributes,
  };

  console.log(`[SP-API] putListingsItem: ASIN=${asin}, SKU=${finalSku}, price=${price}, fulfillment=${isFba ? 'FBA' : 'FBM'}, sellerId=${sellerId}`);

  const result = await sp.callAPI({
    operation: 'putListingsItem',
    endpoint: 'listingsItems',
    path: { sellerId, sku: finalSku },
    query: { marketplaceIds: [marketplaceId] },
    body,
    options: { version: '2021-08-01' },
  });

  console.log(`[SP-API] putListingsItem result:`, JSON.stringify(result, null, 2));

  return {
    sku: finalSku,
    asin,
    status: result.status,
    submissionId: result.submissionId,
    issues: result.issues || [],
  };
}

/**
 * 既存出品の属性をパッチ更新（支払い制限・配送設定等）
 */
/**
 * 競合出品者のオファー一覧取得（価格改定用）
 * ItemCondition: New, Used, Collectible, Refurbished, Club
 */
export async function getItemOffers(asin, itemCondition = 'New') {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  const result = await sp.callAPI({
    operation: 'getItemOffers',
    endpoint: 'productPricing',
    path: { Asin: asin },
    query: {
      MarketplaceId: marketplaceId,
      ItemCondition: itemCondition,
    },
  });

  const summary = result.Summary || {};
  const offers = result.Offers || [];

  // オファーを整理
  const parsedOffers = offers.map(o => ({
    sellerId: o.SellerId,
    subCondition: o.SubCondition,
    isFba: o.IsFulfilledByAmazon,
    isBuyBoxWinner: o.IsBuyBoxWinner,
    isFeaturedMerchant: o.IsFeaturedMerchantByAmazon || o.IsFeaturedMerchant,
    listingPrice: o.ListingPrice?.Amount || 0,
    shipping: o.Shipping?.Amount || 0,
    totalPrice: (o.ListingPrice?.Amount || 0) + (o.Shipping?.Amount || 0),
    points: o.Points?.PointsNumber || 0,
    sellerFeedbackRating: o.SellerFeedbackRating?.SellerPositiveFeedbackRating || null,
    sellerFeedbackCount: o.SellerFeedbackRating?.FeedbackCount || 0,
    shipsFrom: o.ShipsFrom?.Country || 'JP',
    shipsDomestically: (o.ShipsFrom?.Country || 'JP') === 'JP',
  }));

  // FBA・FBMに分離
  const fbaOffers = parsedOffers.filter(o => o.isFba);
  const fbmOffers = parsedOffers.filter(o => !o.isFba);

  return {
    asin,
    totalOfferCount: summary.TotalOfferCount || offers.length,
    buyBoxPrice: summary.BuyBoxPrices?.find(b => b.condition === 'New')?.LandedPrice?.Amount || null,
    buyBoxShipping: summary.BuyBoxPrices?.find(b => b.condition === 'New')?.Shipping?.Amount || null,
    lowestFbaPrice: summary.LowestPrices?.find(p => p.condition === 'New' && p.fulfillmentChannel === 'Amazon')?.LandedPrice?.Amount || null,
    lowestFbmPrice: summary.LowestPrices?.find(p => p.condition === 'New' && p.fulfillmentChannel === 'Merchant')?.LandedPrice?.Amount || null,
    numberOfOffers: {
      fbaNew: summary.NumberOfOffers?.find(n => n.condition === 'New' && n.fulfillmentChannel === 'Amazon')?.OfferCount || 0,
      fbmNew: summary.NumberOfOffers?.find(n => n.condition === 'New' && n.fulfillmentChannel === 'Merchant')?.OfferCount || 0,
    },
    listPrice: summary.ListPrice?.Amount || null,
    offers: parsedOffers,
    fbaOffers,
    fbmOffers,
  };
}

/**
 * 出品価格を更新（価格改定用）
 */
export async function updatePrice({ sku, price }) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();
  const sellerId = SELLER_ID();

  if (!sku) throw new Error('SKUが必要です');
  if (!price || price <= 0) throw new Error('価格が正の数である必要があります');

  const result = await sp.callAPI({
    operation: 'patchListingsItem',
    endpoint: 'listingsItems',
    path: { sellerId, sku },
    query: { marketplaceIds: [marketplaceId] },
    body: {
      productType: 'PRODUCT',
      patches: [{
        op: 'replace',
        path: '/attributes/purchasable_offer',
        value: [{
          marketplace_id: marketplaceId,
          currency: 'JPY',
          our_price: [{ schedule: [{ value_with_tax: price }] }],
        }],
      }],
    },
    options: { version: '2021-08-01' },
  });

  return {
    sku,
    newPrice: price,
    status: result.status,
    submissionId: result.submissionId,
    issues: result.issues || [],
  };
}

/**
 * 出品中の全商品レポートを取得（SKU数の確認用）
 * GET_MERCHANT_LISTINGS_DATA レポート
 */
export async function getActiveListingsReport() {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  // レポート作成リクエスト
  const createResult = await sp.callAPI({
    operation: 'createReport',
    endpoint: 'reports',
    body: {
      reportType: 'GET_MERCHANT_LISTINGS_ALL_DATA',
      marketplaceIds: [marketplaceId],
    },
    options: { version: '2021-06-30' },
  });

  const reportId = createResult.reportId;
  console.log(`[SP-API] レポート作成: reportId=${reportId}`);

  // レポート完了を待機（最大5分）
  let report;
  for (let i = 0; i < 60; i++) {
    await sleep(5000);
    report = await sp.callAPI({
      operation: 'getReport',
      endpoint: 'reports',
      path: { reportId },
      options: { version: '2021-06-30' },
    });
    console.log(`[SP-API] レポートステータス: ${report.processingStatus}`);
    if (report.processingStatus === 'DONE') break;
    if (report.processingStatus === 'FATAL' || report.processingStatus === 'CANCELLED') {
      throw new Error(`レポート処理失敗: ${report.processingStatus}`);
    }
  }

  if (report.processingStatus !== 'DONE') {
    throw new Error('レポート取得タイムアウト');
  }

  // レポートドキュメント取得
  const doc = await sp.callAPI({
    operation: 'getReportDocument',
    endpoint: 'reports',
    path: { reportDocumentId: report.reportDocumentId },
    options: { version: '2021-06-30' },
  });

  // ドキュメントダウンロード（GZIP圧縮 + Shift_JIS対応）
  const response = await fetch(doc.url);
  const rawBuf = Buffer.from(await response.arrayBuffer());

  let dataBuf = rawBuf;
  if (doc.compressionAlgorithm === 'GZIP') {
    const { gunzipSync } = await import('zlib');
    dataBuf = gunzipSync(rawBuf);
    console.log('[SP-API] GZIP解凍完了, サイズ:', dataBuf.length);
  }

  // エンコーディング判定: UTF-8で読めなければShift_JISとして変換
  let text;
  try {
    const iconv = await import('iconv-lite');
    // まずShift_JISとして試す（日本のAmazonレポートはShift_JIS）
    text = iconv.default.decode(dataBuf, 'Shift_JIS');
    console.log('[SP-API] Shift_JISとしてデコード');
  } catch {
    text = dataBuf.toString('utf-8');
    console.log('[SP-API] UTF-8としてデコード');
  }

  // TSV解析（ヘッダーを正規化: 小文字・ハイフン統一）
  const lines = text.split('\n').filter(l => l.trim());
  const rawHeaders = lines[0].split('\t').map(h => h.trim());
  console.log('[SP-API] レポートヘッダー:', rawHeaders.join(', '));

  const rows = lines.slice(1).map(line => {
    const values = line.split('\t');
    const obj = {};
    rawHeaders.forEach((h, i) => {
      const val = (values[i] || '').trim();
      obj[h] = val;                          // オリジナルキー
      obj[h.toLowerCase()] = val;            // 小文字キー
      obj[h.toLowerCase().replace(/\s+/g, '-')] = val; // スペース→ハイフン
    });
    return obj;
  });

  return {
    totalCount: rows.length,
    headers: rawHeaders,
    listings: rows,
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 注文レポートからSKU別販売個数を集計
 * @param {number} days - 過去何日分（デフォルト30日）
 * @returns {Object} { [sku]: totalQuantity, ... }
 */
export async function getSalesCountBySku(days = 30) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  const now = new Date();
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  const startStr = start.toISOString();
  const endStr = now.toISOString();

  console.log(`[SP-API] 注文レポート取得: ${startStr} ～ ${endStr} (${days}日間)`);

  const createResult = await sp.callAPI({
    operation: 'createReport',
    endpoint: 'reports',
    body: {
      reportType: 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE',
      marketplaceIds: [marketplaceId],
      dataStartTime: startStr,
      dataEndTime: endStr,
    },
    options: { version: '2021-06-30' },
  });

  const reportId = createResult.reportId;
  console.log(`[SP-API] 注文レポートID: ${reportId}`);

  // ポーリング
  let report;
  for (let i = 0; i < 60; i++) {
    await sleep(5000);
    report = await sp.callAPI({
      operation: 'getReport',
      endpoint: 'reports',
      path: { reportId },
      options: { version: '2021-06-30' },
    });
    console.log(`[SP-API] 注文レポートステータス: ${report.processingStatus}`);
    if (report.processingStatus === 'DONE') break;
    if (report.processingStatus === 'FATAL' || report.processingStatus === 'CANCELLED') {
      throw new Error(`注文レポート生成失敗: ${report.processingStatus}`);
    }
  }

  if (report.processingStatus !== 'DONE') {
    throw new Error('注文レポート取得タイムアウト');
  }

  const doc = await sp.callAPI({
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
  console.log(`[SP-API] 注文レポートヘッダー: ${headers.join(', ')}`);

  // SKU列とquantity列を探す
  const findIdx = (...candidates) => {
    for (const c of candidates) {
      const idx = headers.findIndex(h => h.toLowerCase().replace(/\s+/g, '-') === c.toLowerCase());
      if (idx >= 0) return idx;
    }
    return -1;
  };

  const skuIdx = findIdx('sku', 'seller-sku');
  const qtyIdx = findIdx('quantity', 'quantity-purchased');
  const statusIdx = findIdx('order-status', 'item-status');

  if (skuIdx < 0 || qtyIdx < 0) {
    console.log(`[SP-API] SKU列=${skuIdx}, 数量列=${qtyIdx} — ヘッダー不一致`);
    // フォールバック: 全ヘッダーをインデックスで探す
    console.log('[SP-API] ヘッダー詳細:', JSON.stringify(headers));
    throw new Error('注文レポートのSKUまたは数量列が見つかりません');
  }

  // SKU別に数量を集計（キャンセル除外）
  const salesMap = {};
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split('\t');
    const sku = (cols[skuIdx] || '').trim();
    const qty = parseInt(cols[qtyIdx]) || 0;
    const status = statusIdx >= 0 ? (cols[statusIdx] || '').trim().toLowerCase() : '';

    if (!sku || qty <= 0) continue;
    if (status === 'cancelled' || status === 'キャンセル') continue;

    salesMap[sku] = (salesMap[sku] || 0) + qty;
  }

  console.log(`[SP-API] 販売集計完了: ${Object.keys(salesMap).length}SKU, 合計${Object.values(salesMap).reduce((a, b) => a + b, 0)}個`);
  return salesMap;
}

export async function patchListing({ sku, patches }) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();
  const sellerId = SELLER_ID();

  if (!sku) throw new Error('SKUが必要です');

  const body = {
    productType: 'PRODUCT',
    patches,
  };

  const result = await sp.callAPI({
    operation: 'patchListingsItem',
    endpoint: 'listingsItems',
    path: { sellerId, sku },
    query: { marketplaceIds: [marketplaceId] },
    body,
    options: { version: '2021-08-01' },
  });

  return {
    sku,
    status: result.status,
    submissionId: result.submissionId,
    issues: result.issues || [],
  };
}
