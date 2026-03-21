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

  // FBM: 配送テンプレート・支払方法制限
  if (!isFba) {
    if (shippingTemplate) {
      attributes.merchant_shipping_group = [{ value: shippingTemplate, marketplace_id: marketplaceId }];
    }
    if (paymentRestriction && paymentRestriction !== 'none') {
      const exclusions = [];
      if (paymentRestriction === 'cod' || paymentRestriction === 'cod_cvs') {
        exclusions.push({ value: 'COD', marketplace_id: marketplaceId });
      }
      if (paymentRestriction === 'cvs' || paymentRestriction === 'cod_cvs') {
        exclusions.push({ value: 'CVS', marketplace_id: marketplaceId });
      }
      if (exclusions.length > 0) {
        attributes.payment_option_exclusion = exclusions;
      }
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
