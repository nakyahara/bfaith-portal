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
 * 商品名のノイズを除去してコアキーワードを抽出
 */
function cleanKeyword(raw) {
  let kw = raw;
  // 括弧内の付属情報を除去: （旧品番XXX）、【ケース販売】等
  kw = kw.replace(/[（(][^）)]*[）)]/g, ' ');
  kw = kw.replace(/[【\[][^】\]]*[】\]]/g, ' ');
  // 容量・入数パターンを除去: 500ml, 300g, 1.5L, 10個入, ×12, x24本 等
  kw = kw.replace(/[\d.,]+\s*(ml|mL|ML|ℓ|l|L|g|kg|KG|Kg|mg|cc|CC)\b/g, ' ');
  kw = kw.replace(/[\d.,]+\s*(個入|個入り|本入|本入り|枚入|枚入り|袋入|包入|粒入|錠入|個|本|枚|袋|包|粒|錠|カプセル|シート|巻|丁)\b/g, ' ');
  kw = kw.replace(/[×x]\s*\d+/gi, ' ');
  // JANっぽい13桁数字を除去
  kw = kw.replace(/\b\d{13}\b/g, ' ');
  // 記号を除去（ハイフン・スラッシュ・ドットは残す）
  kw = kw.replace(/[★☆●◆■□◇△▲※♪#＃&＆!！?？〜~]/g, ' ');
  // 連続スペースを整理
  kw = kw.replace(/\s+/g, ' ').trim();
  // 空になったら元の文字列を返す
  return kw || raw.trim();
}

/**
 * 2つの文字列の類似度スコア（0〜1）
 * 共通する文字のbigram(2文字組)の割合で判定
 */
function similarityScore(a, b) {
  if (!a || !b) return 0;
  const normalize = s => s.toLowerCase().replace(/[\s\-\/・,、。]/g, '');
  const na = normalize(a);
  const nb = normalize(b);
  if (na === nb) return 1;
  if (na.length < 2 || nb.length < 2) return 0;
  const bigrams = s => { const bg = new Set(); for (let i = 0; i < s.length - 1; i++) bg.add(s.slice(i, i + 2)); return bg; };
  const ba = bigrams(na);
  const bb = bigrams(nb);
  let common = 0;
  for (const b of ba) { if (bb.has(b)) common++; }
  return (2 * common) / (ba.size + bb.size);
}

/**
 * キーワードで Amazon商品を検索（精度向上版）
 * - ノイズ除去した検索クエリ
 * - 候補5件取得 → 元の商品名との類似度でベスト候補を選定
 */
export async function searchByKeyword(keyword) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  const cleanedKw = cleanKeyword(keyword);

  const result = await sp.callAPI({
    operation: 'searchCatalogItems',
    endpoint: 'catalogItems',
    query: {
      marketplaceIds: [marketplaceId],
      keywords: [cleanedKw],
      includedData: ['summaries', 'images'],
      pageSize: 5,
    },
    options: { version: '2022-04-01' },
  });

  const items = result.items || [];
  if (items.length === 0) return [];

  // 類似度スコアで並べ替え（元の商品名に近い順）
  const scored = items.map(item => {
    const itemName = item.summaries?.[0]?.itemName || '';
    return {
      asin: item.asin,
      itemName,
      image: item.images?.[0]?.images?.find(i => i.variant === 'MAIN')?.link || '',
      score: similarityScore(keyword, itemName),
    };
  });
  scored.sort((a, b) => b.score - a.score);

  return scored.map(({ asin, itemName, image }) => ({ asin, itemName, image }));
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
 * BSRから売れ行き目安レベル（1〜5）を算出
 * カテゴリー差を考慮しない概算なので「目安」表示用
 */
export function getSalesLevel(rank) {
  if (!rank || rank <= 0 || rank === '-') return { level: 0, label: '不明' };
  const r = Number(rank);
  if (isNaN(r)) return { level: 0, label: '不明' };
  if (r <= 500)    return { level: 5, label: '爆売れ' };
  if (r <= 3000)   return { level: 4, label: 'よく売れる' };
  if (r <= 15000)  return { level: 3, label: '普通' };
  if (r <= 50000)  return { level: 2, label: '少ない' };
  return { level: 1, label: 'ほぼ売れない' };
}

/**
 * 型番を正規化（比較用）
 * 全角→半角、ハイフン類除去、スペース除去、大文字統一
 */
export function normalizePartNumber(pn) {
  if (!pn) return '';
  let s = pn;
  // 全角英数字→半角
  s = s.replace(/[Ａ-Ｚａ-ｚ０-９]/g, c => String.fromCharCode(c.charCodeAt(0) - 0xFEE0));
  // ハイフン類を除去（‐ ー − ‑ - ）
  s = s.replace(/[\u002D\u2010\u2011\u2012\u2013\u2014\u2015\u2212\u30FC\uFF0D\uFF70]/g, '');
  // スペース除去
  s = s.replace(/\s+/g, '');
  // 大文字統一
  s = s.toUpperCase();
  return s;
}

/**
 * 型番で Amazon商品を検索（正規化対応版）
 * 返却時に matchConfidence を含む: 'exact'(完全一致) or 'partial'(部分一致→keyword降格)
 */
export async function searchByPartNumber(partNumber) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();

  const result = await sp.callAPI({
    operation: 'searchCatalogItems',
    endpoint: 'catalogItems',
    query: {
      marketplaceIds: [marketplaceId],
      keywords: [partNumber],
      includedData: ['summaries', 'images', 'identifiers', 'attributes'],
      pageSize: 5,
    },
    options: { version: '2022-04-01' },
  });

  const items = result.items || [];
  if (items.length === 0) return [];

  const normalizedInput = normalizePartNumber(partNumber);

  return items.map(item => {
    const attrs = item.attributes || {};
    const amazonPn = attrs.part_number?.[0]?.value || '';
    const itemName = item.summaries?.[0]?.itemName || '';
    const normalizedAmazon = normalizePartNumber(amazonPn);

    // 完全一致判定（正規化後）
    let matchConfidence = 'partial';
    if (normalizedInput && normalizedAmazon && normalizedInput === normalizedAmazon) {
      matchConfidence = 'exact';
    }

    return {
      asin: item.asin,
      itemName,
      image: item.images?.[0]?.images?.find(i => i.variant === 'MAIN')?.link || '',
      amazonPartNumber: amazonPn,
      matchConfidence,
    };
  });
}

/**
 * Amazon出品登録（Listings Items API）
 */
export async function createListing({ asin, price, isFba, sku, condition = 'new_new', shippingTemplate = null, paymentRestriction = 'none', pointRate = 0, conditionNote = '', batteriesRequired = 'false', hazmatRegulation = 'not_applicable' }) {
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
      ...(pointRate > 0 ? { points: [{ points_number: Math.floor(price * pointRate / 100) }] } : {}),
    }],
    fulfillment_availability: [{
      fulfillment_channel_code: isFba ? 'AMAZON_JP' : 'DEFAULT',
      marketplace_id: marketplaceId,
    }],
  };

  // 安全関連属性（電池・危険物）— LISTING_OFFER_ONLYでも必須の場合がある
  // batteriesRequired / hazmatRegulation が明示指定されていなければデフォルト値を設定
  if (!attributes.batteries_required) {
    attributes.batteries_required = [{ value: batteriesRequired || 'false', marketplace_id: marketplaceId }];
  }
  if (!attributes.supplier_declared_dg_hz_regulation) {
    attributes.supplier_declared_dg_hz_regulation = [{ value: hazmatRegulation || 'not_applicable', marketplace_id: marketplaceId }];
  }

  // コンディション説明（中古品の場合のみ — LISTING_OFFER_ONLYモードでは新品にcondition_noteを付けるとエラーになる場合がある）
  if (conditionNote && condition !== 'new_new') {
    attributes.condition_note = [{ value: conditionNote, marketplace_id: marketplaceId }];
  }

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

  // エンコーディング判定: UTF-8 BOMがあればUTF-8、なければShift_JISとして変換
  let text;
  const hasBOM = dataBuf[0] === 0xEF && dataBuf[1] === 0xBB && dataBuf[2] === 0xBF;
  if (hasBOM || isValidUtf8(dataBuf)) {
    text = dataBuf.toString('utf-8');
    // BOM除去
    if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
    console.log('[SP-API] UTF-8としてデコード');
  } else {
    try {
      const iconv = await import('iconv-lite');
      text = iconv.default.decode(dataBuf, 'Shift_JIS');
      console.log('[SP-API] Shift_JISとしてデコード');
    } catch {
      text = dataBuf.toString('utf-8');
      console.log('[SP-API] フォールバック: UTF-8としてデコード');
    }
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

/** UTF-8として有効なバイト列か簡易チェック（先頭1024バイトを検査） */
function isValidUtf8(buf) {
  const len = Math.min(buf.length, 1024);
  for (let i = 0; i < len;) {
    const b = buf[i];
    if (b <= 0x7F) { i++; continue; }
    let extra = b < 0xE0 ? 1 : b < 0xF0 ? 2 : b < 0xF8 ? 3 : -1;
    if (extra < 0 || i + extra >= len) return false;
    for (let j = 1; j <= extra; j++) {
      if ((buf[i + j] & 0xC0) !== 0x80) return false;
    }
    i += extra + 1;
  }
  return true;
}

/**
 * 注文レポートからSKU別販売個数・最終販売日を集計
 * _GENERALレポートは最大30日制限のため、30日ずつ分割取得して合算
 * @param {number} days - 過去何日分（デフォルト365日）
 * @returns {Object} { [sku]: { count, lastDate }, ... }
 */
export async function getSalesCountBySku(days = 365) {
  const sp = getClient();
  const marketplaceId = MARKETPLACE_ID();
  const MAX_DAYS_PER_REQUEST = 30;

  const now = new Date();
  const salesMap = {};

  // 30日ごとの期間リストを作成
  const periods = [];
  let end = now;
  let remaining = days;
  while (remaining > 0) {
    const chunkDays = Math.min(remaining, MAX_DAYS_PER_REQUEST);
    const start = new Date(end.getTime() - chunkDays * 24 * 60 * 60 * 1000);
    periods.push({ start, end: new Date(end) });
    end = start;
    remaining -= chunkDays;
  }

  console.log(`[SP-API] 販売データ取得: 過去${days}日を${periods.length}回に分割`);

  for (let p = 0; p < periods.length; p++) {
    const { start, end: periodEnd } = periods[p];
    const startStr = start.toISOString();
    const endStr = periodEnd.toISOString();
    console.log(`[SP-API] 注文レポート (${p + 1}/${periods.length}): ${startStr.slice(0, 10)} ～ ${endStr.slice(0, 10)}`);

    try {
      const reportData = await fetchOrderReport(sp, marketplaceId, startStr, endStr);
      // 結果をsalesMapにマージ
      for (const [sku, data] of Object.entries(reportData)) {
        if (!salesMap[sku]) {
          salesMap[sku] = { count: 0, lastDate: '' };
        }
        salesMap[sku].count += data.count;
        if (data.lastDate && data.lastDate > salesMap[sku].lastDate) {
          salesMap[sku].lastDate = data.lastDate;
        }
      }
    } catch (err) {
      console.error(`[SP-API] 期間 ${p + 1} エラー (続行):`, err.message);
    }
  }

  const totalItems = Object.values(salesMap).reduce((a, b) => a + b.count, 0);
  console.log(`[SP-API] 販売集計完了: ${Object.keys(salesMap).length}SKU, 合計${totalItems}個`);
  return salesMap;
}

/**
 * 単一期間の注文レポートを取得してSKU別に集計
 */
async function fetchOrderReport(sp, marketplaceId, startStr, endStr) {
  const createResult = await sp.callAPI({
    operation: 'createReport',
    endpoint: 'reports',
    body: {
      reportType: 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
      marketplaceIds: [marketplaceId],
      dataStartTime: startStr,
      dataEndTime: endStr,
    },
    options: { version: '2021-06-30' },
  });

  const reportId = createResult.reportId;

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
  if (lines.length <= 1) {
    console.log('[SP-API] この期間の注文データ: 0件');
    return {};
  }

  // BOM除去
  const headers = lines[0].replace(/^\uFEFF/, '').split('\t').map(h => h.trim());

  // カラム名検索
  const findIdx = (...candidates) => {
    for (const c of candidates) {
      const idx = headers.findIndex(h => h.toLowerCase().replace(/[\s_]+/g, '-') === c.toLowerCase().replace(/[\s_]+/g, '-'));
      if (idx >= 0) return idx;
    }
    for (const c of candidates) {
      const idx = headers.findIndex(h => h.toLowerCase().includes(c.toLowerCase()));
      if (idx >= 0) return idx;
    }
    return -1;
  };

  const skuIdx = findIdx('sku', 'seller-sku', '出品者SKU');
  const qtyIdx = findIdx('quantity', 'quantity-purchased', '数量');
  const statusIdx = findIdx('order-status', 'item-status', '注文ステータス', '商品ステータス');
  const dateIdx = findIdx('purchase-date', 'last-updated-date', 'payments-date', '購入日', '購入日時', '最終更新日');

  if (skuIdx < 0 || qtyIdx < 0) {
    console.log('[SP-API] ヘッダー詳細:', JSON.stringify(headers));
    throw new Error(`SKUまたは数量列が見つかりません。ヘッダー: ${headers.slice(0, 15).join(', ')}`);
  }

  console.log(`[SP-API] レポートヘッダーOK (${headers.length}列): SKU=${skuIdx}, qty=${qtyIdx}, status=${statusIdx}, date=${dateIdx}`);

  const chunkMap = {};
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split('\t');
    const sku = (cols[skuIdx] || '').trim();
    const qty = parseInt(cols[qtyIdx]) || 0;
    const status = statusIdx >= 0 ? (cols[statusIdx] || '').trim().toLowerCase() : '';
    const orderDate = dateIdx >= 0 ? (cols[dateIdx] || '').trim() : '';

    if (!sku || qty <= 0) continue;
    if (status === 'cancelled' || status === 'キャンセル') continue;

    if (!chunkMap[sku]) {
      chunkMap[sku] = { count: 0, lastDate: '' };
    }
    chunkMap[sku].count += qty;
    if (orderDate && orderDate > chunkMap[sku].lastDate) {
      chunkMap[sku].lastDate = orderDate.slice(0, 10);
    }
  }

  console.log(`[SP-API] この期間: ${Object.keys(chunkMap).length}SKU, ${Object.values(chunkMap).reduce((a, b) => a + b.count, 0)}個`);
  return chunkMap;
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
