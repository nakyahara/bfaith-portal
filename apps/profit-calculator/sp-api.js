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
      const cp = product.CompetitivePricing?.CompetitivePrices?.[0];
      currentPrice = cp?.Price?.ListingPrice?.Amount || 0;
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
