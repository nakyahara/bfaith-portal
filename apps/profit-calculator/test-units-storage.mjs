/**
 * test-units-storage.mjs — Amazon カタログの寸法・重さの単位換算と、一括リサーチの概算保管コスト
 *
 * SP-API は叩かない。getCatalogItem / getCompetitivePricing の応答の形をした固定データを差し込む。
 * 実行: node apps/profit-calculator/test-units-storage.mjs
 */
import assert from 'node:assert/strict';

const { getProduct, normalizePackageDimensions, _setClientForTest } = await import('./sp-api.js');
const { estimateStorageCost, STORAGE_UNKNOWN_COST } = await import('./storage-estimate.js');

let passed = 0;
async function t(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

// getCatalogItem (2022-04-01) の応答の形。dimensions[0].package に { unit, value }
function catalogFixture(pkg) {
  return {
    asin: 'B000TEST01',
    summaries: [{ marketplaceId: 'A1VC38T7YXB528', itemName: 'テスト商品', brandName: 'テスト' }],
    images: [{ marketplaceId: 'A1VC38T7YXB528', images: [{ variant: 'MAIN', link: 'https://m.media-amazon.com/images/I/x.jpg', height: 500, width: 500 }] }],
    dimensions: pkg === undefined ? [] : [{ marketplaceId: 'A1VC38T7YXB528', package: pkg }],
    identifiers: [{ marketplaceId: 'A1VC38T7YXB528', identifiers: [{ identifierType: 'EAN', identifier: '4900000000000' }] }],
    attributes: {},
  };
}
// getCompetitivePricing の応答の形 (今回の確認には関係しないので最小限)
const pricingFixture = [{ Product: { CompetitivePricing: { CompetitivePrices: [], NumberOfOfferListings: [] }, SalesRankings: [] } }];

function useCatalog(pkg) {
  _setClientForTest({
    async callAPI({ operation }) {
      if (operation === 'getCatalogItem') return catalogFixture(pkg);
      if (operation === 'getCompetitivePricing') return pricingFixture;
      throw new Error(`想定外の呼び出し: ${operation}`);
    },
  });
}

const near = (a, b, eps = 1e-6) => assert.ok(Math.abs(a - b) < eps, `${a} ≈ ${b}`);

console.log('寸法・重さの単位換算 (getProduct)');

await t('インチ + ポンド (これまでどおり・実物で見た形)', async () => {
  useCatalog({
    length: { unit: 'inches', value: 11.811 }, width: { unit: 'inches', value: 7.874 }, height: { unit: 'inches', value: 3.937 },
    weight: { unit: 'pounds', value: 0.0440924524 },
  });
  const p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '30.0', width: '20.0', height: '10.0', weight: '0.02' });
  near(p.volumeCm3, 11.811 * 2.54 * 7.874 * 2.54 * 3.937 * 2.54);
});

await t('センチ + キログラム (以前は 2.54 倍・0.45 倍になっていた)', async () => {
  useCatalog({
    length: { unit: 'centimeters', value: 30 }, width: { unit: 'centimeters', value: 20 }, height: { unit: 'centimeters', value: 10 },
    weight: { unit: 'kilograms', value: 0.45 },
  });
  const p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '30.0', width: '20.0', height: '10.0', weight: '0.45' });
  near(p.volumeCm3, 6000);
});

await t('センチ + グラム (以前は 450g が 204kg になっていた)', async () => {
  useCatalog({
    length: { unit: 'centimeters', value: 15.5 }, width: { unit: 'centimeters', value: 10 }, height: { unit: 'centimeters', value: 2 },
    weight: { unit: 'grams', value: 450 },
  });
  const p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '15.5', width: '10.0', height: '2.0', weight: '0.45' });
  near(p.volumeCm3, 310);
});

await t('ミリ + グラム', async () => {
  useCatalog({
    length: { unit: 'millimeters', value: 120 }, width: { unit: 'millimeters', value: 80 }, height: { unit: 'millimeters', value: 25 },
    weight: { unit: 'grams', value: 85 },
  });
  const p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '12.0', width: '8.0', height: '2.5', weight: '0.09' });
  near(p.volumeCm3, 240);
});

await t('インチ + キログラム / センチ + ポンド / オンスの混在', async () => {
  useCatalog({
    length: { unit: 'inches', value: 10 }, width: { unit: 'centimeters', value: 10 }, height: { unit: 'Centimeters', value: 5 },
    weight: { unit: 'kilograms', value: 1.2 },
  });
  let p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '25.4', width: '10.0', height: '5.0', weight: '1.20' });
  useCatalog({ length: { unit: 'centimeters', value: 40 }, width: { unit: 'centimeters', value: 30 }, height: { unit: 'centimeters', value: 20 }, weight: { unit: 'pounds', value: 2 } });
  p = await getProduct('B000TEST01');
  assert.equal(p.dimensions.weight, '0.91');
  useCatalog({ length: { unit: 'centimeters', value: 1 }, width: { unit: 'centimeters', value: 1 }, height: { unit: 'centimeters', value: 1 }, weight: { unit: 'ounces', value: 16 } });
  p = await getProduct('B000TEST01');
  assert.equal(p.dimensions.weight, '0.45');
});

await t('単位の欄が無いときだけ、これまでどおりインチ・ポンドとみなす', async () => {
  useCatalog({ length: { value: 10 }, width: { value: 5 }, height: { value: 2 }, weight: { value: 1 } });
  const p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '25.4', width: '12.7', height: '5.1', weight: '0.45' });
});

await t('知らない単位は推測しない (- にして体積 0)', async () => {
  useCatalog({
    length: { unit: 'cubits', value: 10 }, width: { unit: 'centimeters', value: 5 }, height: { unit: 'centimeters', value: 2 },
    weight: { unit: 'stones', value: 1 },
  });
  const p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '-', width: '5.0', height: '2.0', weight: '-' });
  assert.equal(p.volumeCm3, 0);
});

await t('寸法が無い商品は全部 - (以前と同じ形)', async () => {
  useCatalog(undefined);
  const p = await getProduct('B000TEST01');
  assert.deepEqual(p.dimensions, { length: '-', width: '-', height: '-', weight: '-' });
  assert.equal(p.volumeCm3, 0);
  // 0 や読めない値も - にする
  const r = normalizePackageDimensions({ length: { unit: 'centimeters', value: 0 }, width: { unit: 'centimeters', value: 'abc' }, height: null, weight: { unit: 'grams', value: -1 } });
  assert.deepEqual(r.dimensions, { length: '-', width: '-', height: '-', weight: '-' });
});

console.log('\n概算保管コスト (一括リサーチ)');

await t('小型 (3 辺合計 60cm 以下) = 30 円', async () => {
  assert.equal(estimateStorageCost({ length: '20.0', width: '15.0', height: '10.0', weight: '0.30' }), 30);
  assert.equal(estimateStorageCost({ length: '30.0', width: '20.0', height: '10.0', weight: '-' }), 30); // ちょうど 60
});

await t('標準 (60cm 超〜100cm 以下) = 80 円', async () => {
  assert.equal(estimateStorageCost({ length: '30.1', width: '20.0', height: '10.0', weight: '0.5' }), 80);
  assert.equal(estimateStorageCost({ length: '40.0', width: '35.0', height: '25.0', weight: '2.0' }), 80); // ちょうど 100
});

await t('大型 (100cm 超) = 250 円', async () => {
  assert.equal(estimateStorageCost({ length: '60.0', width: '40.0', height: '30.0', weight: '8.0' }), 250);
});

await t('寸法が分からないときは 50 円', async () => {
  assert.equal(STORAGE_UNKNOWN_COST, 50);
  assert.equal(estimateStorageCost(null), 50);
  assert.equal(estimateStorageCost(undefined), 50);
  assert.equal(estimateStorageCost({ length: '-', width: '-', height: '-', weight: '-' }), 50);
  assert.equal(estimateStorageCost({ length: '20.0', width: '-', height: '10.0', weight: '0.3' }), 50);
});

await t('🚨 以前のキー名 (lengthCm など) の誤りの再発防止: getProduct の dimensions をそのまま渡して大きさ別に分かれる', async () => {
  useCatalog({ length: { unit: 'centimeters', value: 20 }, width: { unit: 'centimeters', value: 15 }, height: { unit: 'centimeters', value: 10 }, weight: { unit: 'grams', value: 300 } });
  assert.equal(estimateStorageCost((await getProduct('B000TEST01')).dimensions), 30);
  useCatalog({ length: { unit: 'inches', value: 15 }, width: { unit: 'inches', value: 10 }, height: { unit: 'inches', value: 8 }, weight: { unit: 'pounds', value: 3 } });
  assert.equal(estimateStorageCost((await getProduct('B000TEST01')).dimensions), 80); // 38.1 + 25.4 + 20.3 = 83.8
  useCatalog({ length: { unit: 'centimeters', value: 70 }, width: { unit: 'centimeters', value: 50 }, height: { unit: 'centimeters', value: 40 }, weight: { unit: 'kilograms', value: 9 } });
  assert.equal(estimateStorageCost((await getProduct('B000TEST01')).dimensions), 250);
});

console.log(`\n${passed} 件 OK${process.exitCode ? ' (失敗あり)' : ''}`);
