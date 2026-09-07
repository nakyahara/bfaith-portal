/**
 * test-calc.mjs — 想定利益の計算 受入試験
 *
 * 正本 = AI_reference『商品別想定利益_要件定義_20260907.md』§10.1 (PR受入試験)
 * 期待値は仕様の写しではなく、独立した手計算で作る (Codex R2-12)。
 *
 * 実行: node apps/expected-profit/test-calc.mjs
 */
import assert from 'node:assert/strict';
import {
  taxMultiplier, effectiveTaxRate, exTax, SERVICE_TAX_RATE, TAX_RATE_FALLBACK,
  resolveCost, shippingCostExTax, normalizeFeeEstimate, canReuseFeeEstimate,
  computeProfit, requiredInputs, isRankEligible, expenseScopeVersion,
} from './calc.js';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
const near = (a, b, eps = 1e-6) => Math.abs(a - b) < eps;

// ─── §10.1-1 税区分 ───
console.log('§10.1-1 税区分');

t('標準10%: 税込1,100 → 税抜1,000', () => {
  assert.ok(near(exTax(1100, effectiveTaxRate(0.1)), 1000));
});

t('軽減8%: 税込1,080 → 税抜1,000', () => {
  assert.ok(near(exTax(1080, effectiveTaxRate(0.08)), 1000));
});

t('🚨 送料の税率に商品の軽減税率を流用しない', () => {
  // 8%商品でも送料は10%。198円(税込)の税抜は 180 であって 183.3 ではない
  assert.ok(near(198 / (1 + SERVICE_TAX_RATE), 180));
  assert.ok(!near(198 / 1.08, 180));
});

t('消費税率が未登録なら10%扱い', () => {
  assert.equal(effectiveTaxRate(null), TAX_RATE_FALLBACK);
  assert.equal(effectiveTaxRate(0), TAX_RATE_FALLBACK);
  assert.equal(taxMultiplier(null), 1.1);
});

t('🚨 整数10が来ても11倍にしない (単位の取り違え防止)', () => {
  assert.equal(taxMultiplier(10), 1.1);
  assert.equal(effectiveTaxRate(10), TAX_RATE_FALLBACK);
});

// ─── §10.1-2 送料込みと別途 ───
console.log('\n§10.1-2 送料収入と配送費');

t('送料込み: 送料収入0でも配送関係費は引く', () => {
  // 税込1,100 (税抜1,000) / 原価税抜600 / ネコポス配送関係費237(送料198税込+作業料等39)
  const ship = shippingCostExTax({ 送料: 198, 出荷作業料: 20, 想定梱包資材費: 10, 想定人件費: 9 });
  assert.equal(ship.ok, true);
  assert.ok(near(ship.fee, 180));            // 198 ÷ 1.1
  assert.ok(near(ship.total, 180 + 39));     // 作業料等はそのまま
  const r = computeProfit({ priceExTax: 1000, postageRevenueExTax: 0, costExTax: 600, shippingTotalExTax: ship.total, feeTotalExTax: 100 });
  assert.ok(near(r.expectedProfit, 1000 - 600 - 219 - 100)); // = 81
});

t('🚨 送料別途: 送料収入を足したうえで配送費も引く (片方だけにしない)', () => {
  const r = computeProfit({ priceExTax: 1000, postageRevenueExTax: 300, costExTax: 600, shippingTotalExTax: 500, feeTotalExTax: 100 });
  // 1300 - 600 - 500 - 100 = 100。送料を無視すると 300、配送費だけ消すと 600 になる
  assert.ok(near(r.expectedProfit, 100));
  assert.ok(near(r.revenueExTax, 1300));
});

t('配送マスタが無ければ計算不能 (0で埋めない)', () => {
  assert.equal(shippingCostExTax(null).ok, false);
  assert.equal(shippingCostExTax({ 出荷作業料: 20 }).reason, 'shipping_fee_missing');
});

// ─── §10.1-3..6 Amazon 手数料 ───
console.log('\n§10.1-3..6 Amazon 手数料');

const feeSample = (opts = {}) => ({
  TotalFeesEstimate: { CurrencyCode: 'JPY', Amount: opts.total ?? 666 },
  FeeDetailList: [
    { FeeType: 'ReferralFee', FeeAmount: { Amount: 236 }, FinalFee: { Amount: opts.referral ?? 236 }, FeePromotion: { Amount: 0 } },
    { FeeType: 'VariableClosingFee', FeeAmount: { Amount: 0 }, FinalFee: { Amount: 0 }, FeePromotion: { Amount: 0 } },
    { FeeType: 'PerItemFee', FeeAmount: { Amount: 0 }, FinalFee: { Amount: 0 }, FeePromotion: { Amount: 0 } },
    ...(opts.fba === null ? [] : [{
      FeeType: 'FBAFees', FeeAmount: { Amount: 430 }, FinalFee: { Amount: opts.fba ?? 430 }, FeePromotion: { Amount: 0 },
      IncludedFeeDetailList: [{ FeeType: 'FBAPickAndPack', FeeAmount: { Amount: 430 }, FinalFee: { Amount: 430 }, FeePromotion: { Amount: 0 } }],
    }]),
    ...(opts.extra || []),
  ],
});

t('🚨 IncludedFeeDetailList を二重加算しない', () => {
  const n = normalizeFeeEstimate(feeSample());
  assert.equal(n.sum, 666);          // 236 + 0 + 0 + 430。子の430を足すと1,096になる
  assert.equal(n.status, 'ok');
});

t('ReferralFee は税抜のまま / FBAFees は ÷1.1 で税抜に', () => {
  const n = normalizeFeeEstimate(feeSample());
  assert.equal(n.referral, 236);                    // 税抜。請求時に ×1.1 されるが控除は税抜
  assert.ok(near(n.fbaExTax, 430 / 1.1));           // FBA は税込表示なので税抜へ
});

t('FBM は FBAFees 行が無い → NULL (0で代用しない)', () => {
  const n = normalizeFeeEstimate(feeSample({ fba: null, total: 236 }));
  assert.equal(n.fbaInclTax, null);
  assert.equal(n.fbaExTax, null);
});

t('行があれば 0 は有効なゼロ', () => {
  const n = normalizeFeeEstimate(feeSample());
  assert.equal(n.closing, 0);
  assert.equal(n.perItem, 0);
});

t('🚨 内訳合計と TotalFeesEstimate が食い違えば inconsistent', () => {
  const n = normalizeFeeEstimate(feeSample({ total: 999 }));
  assert.equal(n.status, 'inconsistent');
});

t('🚨 未知の FeeType は無視せず unknown_fee_type', () => {
  const n = normalizeFeeEstimate(feeSample({
    total: 766,
    extra: [{ FeeType: 'SomeNewFee', FeeAmount: { Amount: 100 }, FinalFee: { Amount: 100 }, FeePromotion: { Amount: 0 } }],
  }));
  assert.equal(n.status, 'unknown_fee_type');
  assert.deepEqual(n.unknownTypes, ['SomeNewFee']);
  assert.equal(n.sum, 766);   // 合計には含める
});

t('FeesEstimate が無ければ missing', () => {
  assert.equal(normalizeFeeEstimate(null).status, 'missing');
});

// ─── 手数料の再利用条件 (§7.3) ───
console.log('\n手数料見積の再利用条件');

const now = new Date('2026-09-07T12:00:00+09:00');
const baseCached = {
  seller_id: 'S1', marketplace_id: 'M1', seller_sku: 'sku1', asin: 'B001',
  in_listing_price: 1000, in_shipping: 0, in_points: 0, in_fulfillment: 'FBM', in_currency: 'JPY',
  fetched_at: '2026-09-05T00:00:00Z', valid_until: '2026-09-19T00:00:00Z',
};
const wantedSame = { ...baseCached };

t('全入力一致 + 期限内 → 再利用する', () => {
  assert.equal(canReuseFeeEstimate(baseCached, wantedSame, now).reuse, true);
});

t('🚨 749円 → 751円 は差0.27%でも再見積もり (料率帯をまたぐ)', () => {
  const c = { ...baseCached, in_listing_price: 749 };
  const w = { ...wantedSame, in_listing_price: 751 };
  const r = canReuseFeeEstimate(c, w, now);
  assert.equal(r.reuse, false);
  assert.equal(r.reason, 'input_mismatch:in_listing_price');
});

t('🚨 価格が同じでも送料が変われば再見積もり (送料も算定基礎)', () => {
  const w = { ...wantedSame, in_shipping: 230 };
  assert.equal(canReuseFeeEstimate(baseCached, w, now).reuse, false);
});

t('🚨 ASIN が変われば再見積もり (PK に無いが比較する)', () => {
  const w = { ...wantedSame, asin: 'B002' };
  const r = canReuseFeeEstimate(baseCached, w, now);
  assert.equal(r.reuse, false);
  assert.equal(r.reason, 'input_mismatch:asin');
});

t('通貨が変われば再見積もり', () => {
  assert.equal(canReuseFeeEstimate(baseCached, { ...wantedSame, in_currency: 'USD' }, now).reuse, false);
});

t('期限切れ → 再見積もり (入力が全部一致でも)', () => {
  const c = { ...baseCached, valid_until: '2026-09-06T00:00:00Z' };
  assert.equal(canReuseFeeEstimate(c, wantedSame, now).reason, 'expired');
});

t('🚨 料率改定日を跨いだキャッシュは無条件失効', () => {
  const c = { ...baseCached, fetched_at: '2026-03-20T00:00:00Z', valid_until: '2099-01-01T00:00:00Z' };
  const r = canReuseFeeEstimate(c, wantedSame, new Date('2026-04-02T00:00:00+09:00'));
  assert.equal(r.reuse, false);
  assert.equal(r.reason, 'fee_revision_crossed');
});

t('期限切れは入力不一致より先に判定される', () => {
  const c = { ...baseCached, valid_until: '2026-09-06T00:00:00Z' };
  const w = { ...wantedSame, in_listing_price: 9999 };
  assert.equal(canReuseFeeEstimate(c, w, now).reason, 'expired');
});

// ─── §10.1-7..9 セット原価 ───
console.log('\n§10.1-7..9 原価とセット');

t('単品: 原価ソース NE → single', () => {
  const r = resolveCost({ 原価: 600, 原価ソース: 'NE', 原価状態: 'COMPLETE', 消費税率: 0.1 });
  assert.equal(r.ok, true);
  assert.equal(r.method, 'single');
  assert.equal(r.costExTax, 600);
});

t('セット: 原価ソース セット計算 + COMPLETE → set_master (再展開しない)', () => {
  const r = resolveCost({ 原価: 1800, 原価ソース: 'セット計算', 原価状態: 'COMPLETE', 消費税率: 0.1 });
  assert.equal(r.ok, true);
  assert.equal(r.method, 'set_master');
  assert.equal(r.costExTax, 1800);   // rebuild が 数量×構成品原価 で1セット分を計算済み
});

t('🚨 セットで原価状態が COMPLETE でなければ計算不能', () => {
  const r = resolveCost({ 原価: 1800, 原価ソース: 'セット計算', 原価状態: 'PARTIAL', 消費税率: 0.1 });
  assert.equal(r.ok, false);
  assert.equal(r.reason, 'set_cost_incomplete');
});

t('🚨 原価0は未登録扱い (有効なゼロは無い)', () => {
  assert.equal(resolveCost({ 原価: 0, 原価ソース: 'NE', 原価状態: 'COMPLETE' }).reason, 'cost_missing');
});

t('商品が見つからない → product_not_found', () => {
  assert.equal(resolveCost(null).reason, 'product_not_found');
});

// ─── §10.1 経路別の必須条件と適格判定 ───
console.log('\n経路別の必須条件 / ランキング適格');

t('Amazon FBA: 自社配送区分は not_applicable', () => {
  const r = requiredInputs({ mall: 'amazon', fulfillment: 'FBA' });
  assert.ok(r.required.includes('fee'));
  assert.ok(r.notApplicable.includes('shipping_master'));
  assert.ok(!r.required.includes('shipping_master'));
});

t('Amazon FBM: 自社配送区分が必須', () => {
  const r = requiredInputs({ mall: 'amazon', fulfillment: 'FBM' });
  assert.ok(r.required.includes('shipping_master'));
});

t('他モール: 手数料は簡易料率なので not_applicable', () => {
  const r = requiredInputs({ mall: 'rakuten' });
  assert.ok(r.notApplicable.includes('fee'));
  assert.ok(r.required.includes('shipping_master'));
});

const okRow = {
  mall: 'amazon', fulfillment: 'FBA', listing_status: 'active',
  calculation_status: 'ok', scenario_fit: 'ok', price_source: 'own_listing',
  listing_enum_status: 'ok', price_status: 'ok', cost_status: 'ok', fee_status: 'ok',
  shipping_master_status: 'not_applicable',
};

t('🚨 FBA が「自社配送区分なし」で除外されない', () => {
  assert.equal(isRankEligible(okRow).eligible, true);
});

t('🚨 列挙が期限切れならランキング外', () => {
  const r = isRankEligible({ ...okRow, listing_enum_status: 'expired' });
  assert.equal(r.eligible, false);
  assert.equal(r.reason, 'listing_enum_expired');
});

t('価格が期限切れならランキング外', () => {
  assert.equal(isRankEligible({ ...okRow, price_status: 'expired' }).eligible, false);
});

t('Inactive はランキング外 (計算はする)', () => {
  const r = isRankEligible({ ...okRow, listing_status: 'inactive' });
  assert.equal(r.eligible, false);
  assert.equal(r.reason, 'listing_inactive');
});

t('buybox 価格に代替した行はランキング外', () => {
  assert.equal(isRankEligible({ ...okRow, price_source: 'buybox' }).eligible, false);
});

t('FBM で送料収入が不明ならランキング外 (参考値)', () => {
  const row = { ...okRow, fulfillment: 'FBM', shipping_master_status: 'ok', shipping_revenue_status: 'unknown' };
  const r = isRankEligible(row);
  assert.equal(r.eligible, false);
  assert.equal(r.reason, 'shipping_revenue_unknown');
});

t('シナリオ不適合 (送料が一意に決まらない) はランキング外', () => {
  assert.equal(isRankEligible({ ...okRow, scenario_fit: 'undecidable' }).eligible, false);
});

t('incomplete はランキング外', () => {
  assert.equal(isRankEligible({ ...okRow, calculation_status: 'incomplete' }).eligible, false);
});

t('費用範囲: FBA と自社配送を別の版として識別する', () => {
  assert.equal(expenseScopeVersion({ mall: 'amazon', fulfillment: 'FBA' }), 'fba_v1');
  assert.equal(expenseScopeVersion({ mall: 'amazon', fulfillment: 'FBM' }), 'self_v1');
  assert.equal(expenseScopeVersion({ mall: 'rakuten' }), 'self_v1');
});

// ─── 通し計算 (手計算と突合) ───
console.log('\n通し計算 (独立した手計算と突合)');

t('Amazon FBA 標準10%: 手計算と一致', () => {
  // 税込1,980 → 税抜1,800 / 原価税抜1,000 / ReferralFee 166(税抜) / FBAFees 462(税込)→420(税抜)
  const price = exTax(1980, 0.1);                 // 1800
  const fba = 462 / 1.1;                          // 420
  const r = computeProfit({ priceExTax: price, costExTax: 1000, fbaFeeExTax: fba, feeTotalExTax: 166 });
  assert.ok(near(r.expectedProfit, 1800 - 1000 - 420 - 166));  // 214
  assert.ok(near(r.expectedMarginRate, 214 / 1800));
});

t('楽天 軽減8% 送料込み: 手計算と一致', () => {
  // 税込1,080 → 税抜1,000 / 原価税抜600 / 手数料 = 1080×10% = 108(税込) → 98.18(税抜)
  // 配送 ネコポス: 送料198(税込)→180 + 作業料等39 = 219
  const price = exTax(1080, 0.08);                // 1000
  const fee = (1080 * 0.10) / 1.1;                // 98.18...
  const ship = shippingCostExTax({ 送料: 198, 出荷作業料: 20, 想定梱包資材費: 10, 想定人件費: 9 });
  const r = computeProfit({ priceExTax: price, costExTax: 600, shippingTotalExTax: ship.total, feeTotalExTax: fee });
  assert.ok(near(r.expectedProfit, 1000 - 600 - 219 - 98.1818181818, 1e-6));
});

t('🚨 FBA費用を手数料合計にも入れると二重控除になる (境界の確認)', () => {
  const correct = computeProfit({ priceExTax: 1800, costExTax: 1000, fbaFeeExTax: 420, feeTotalExTax: 166 });
  const doubled = computeProfit({ priceExTax: 1800, costExTax: 1000, fbaFeeExTax: 420, feeTotalExTax: 166 + 420 });
  assert.ok(near(correct.expectedProfit - doubled.expectedProfit, 420));
});

t('売上が0以下なら計算不能', () => {
  assert.equal(computeProfit({ priceExTax: 0, costExTax: 100 }).ok, false);
});

console.log(`\n${passed} 件 PASS`);
