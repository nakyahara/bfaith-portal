/**
 * test-fees.mjs — 手数料の再見積もり 受入試験 (§10.1-3〜6)
 *
 * SP-API は叩かない。deps 差し替えで検証する。
 * 実行: node apps/expected-profit/test-fees.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-fee-'));
process.env.SP_API_MARKETPLACE_ID = 'A1VC38T7YXB528';

const { initExpectedProfitDB } = await import('./db.js');
const { planRefresh, buildFeeRequest, toEstimateRow, saveEstimates, loadCache, refreshFees, cacheKey } = await import('./refresh-fees.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const target = (over = {}) => ({
  seller_id: 'S1', marketplace_id: 'M1', seller_sku: 'sku1', asin: 'B001',
  in_listing_price: 1000, in_shipping: 0, in_points: 0, in_fulfillment: 'FBM', in_currency: 'JPY',
  ...over,
});

const feeResponse = (identifier, { referral = 84, fba = null, total = null } = {}) => ([{
  Status: 'Success',
  FeesEstimateIdentifier: { SellerInputIdentifier: identifier },
  FeesEstimate: {
    TotalFeesEstimate: { CurrencyCode: 'JPY', Amount: total ?? (referral + (fba ?? 0)) },
    FeeDetailList: [
      { FeeType: 'ReferralFee', FeeAmount: { Amount: referral }, FinalFee: { Amount: referral }, FeePromotion: { Amount: 0 } },
      ...(fba == null ? [] : [{ FeeType: 'FBAFees', FeeAmount: { Amount: fba }, FinalFee: { Amount: fba }, FeePromotion: { Amount: 0 } }]),
    ],
  },
}]);

console.log('リクエストの組み立て');

t('🚨 送料を算定基礎に渡す (Shipping 0 固定にしない)', () => {
  const body = buildFeeRequest([target({ in_shipping: 230 })], 'M1');
  assert.equal(body[0].FeesEstimateRequest.PriceToEstimateFees.Shipping.Amount, 230);
});

t('🚨 ポイントも見積入力として渡す', () => {
  const body = buildFeeRequest([target({ in_points: 5 })], 'M1');
  assert.equal(body[0].FeesEstimateRequest.PriceToEstimateFees.Points.PointsNumber, 5);
});

t('FBA / FBM を IsAmazonFulfilled に反映する', () => {
  assert.equal(buildFeeRequest([target({ in_fulfillment: 'FBA' })], 'M1')[0].FeesEstimateRequest.IsAmazonFulfilled, true);
  assert.equal(buildFeeRequest([target()], 'M1')[0].FeesEstimateRequest.IsAmazonFulfilled, false);
});

console.log('\n再利用の計画 (§7.3)');

const db = initExpectedProfitDB();

t('キャッシュが無ければ対象になる', () => {
  const { need, reuse } = planRefresh([target()], new Map());
  assert.equal(need.length, 1);
  assert.equal(reuse.length, 0);
  assert.equal(need[0].reason, 'missing');
});

t('同じ入力で期限内なら再利用する', () => {
  const cached = new Map([[cacheKey(target()), {
    ...target(), fetched_at: '2026-09-06T00:00:00Z', valid_until: '2099-01-01T00:00:00Z',
  }]]);
  const { need, reuse } = planRefresh([target()], cached, new Date('2026-09-07T00:00:00Z'));
  assert.equal(reuse.length, 1);
  assert.equal(need.length, 0);
});

t('🚨 価格が 749 → 751 に変わったら取り直す', () => {
  const cached = new Map([[cacheKey(target({ in_listing_price: 749 })), {
    ...target({ in_listing_price: 749 }), fetched_at: '2026-09-06T00:00:00Z', valid_until: '2099-01-01T00:00:00Z',
  }]]);
  const { need } = planRefresh([target({ in_listing_price: 751 })], cached, new Date('2026-09-07T00:00:00Z'));
  assert.equal(need.length, 1);
});

t('🚨 送料だけ変わっても取り直す', () => {
  const base = target();
  const cached = new Map([[cacheKey(base), { ...base, fetched_at: '2026-09-06T00:00:00Z', valid_until: '2099-01-01T00:00:00Z' }]]);
  const { need } = planRefresh([target({ in_shipping: 230 })], cached, new Date('2026-09-07T00:00:00Z'));
  assert.equal(need.length, 1);
});

console.log('\n保存形への変換 (§15-1)');

t('ReferralFee は税抜のまま / FBAFees は税込のまま保存し、税抜換算は計算時に行う', () => {
  const row = toEstimateRow(target(), feeResponse('x', { referral: 84, fba: 462 })[0].FeesEstimate, '2026-09-07T00:00:00Z');
  assert.equal(row.referral_fee_ex_tax, 84);
  assert.equal(row.fba_fee_incl_tax, 462);
});

t('FBM は FBAFees が無いので NULL (0で代用しない)', () => {
  const row = toEstimateRow(target(), feeResponse('x', { referral: 84 })[0].FeesEstimate, '2026-09-07T00:00:00Z');
  assert.equal(row.fba_fee_incl_tax, null);
});

t('有効期限 14 日が入る', () => {
  const row = toEstimateRow(target(), feeResponse('x')[0].FeesEstimate, '2026-09-07T00:00:00Z');
  assert.equal(row.valid_until, '2026-09-21T00:00:00.000Z');
});

t('生の内訳を残す (表示・監査用)', () => {
  const row = toEstimateRow(target(), feeResponse('x', { referral: 84, fba: 462 })[0].FeesEstimate, '2026-09-07T00:00:00Z');
  const parsed = JSON.parse(row.fee_breakdown);
  assert.equal(parsed.length, 2);
});

console.log('\n実行');

await ta('見積を取って保存する', async () => {
  const r = await refreshFees(db, [target()], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { referral: 84 }),
  });
  assert.equal(r.refreshed, 1);
  assert.equal(r.failed, 0);
  const saved = db.prepare('SELECT * FROM amazon_fee_estimate').all();
  assert.equal(saved.length, 1);
  assert.equal(saved[0].referral_fee_ex_tax, 84);
});

await ta('2回目は再利用してAPIを叩かない', async () => {
  let called = 0;
  const r = await refreshFees(db, [target()], {
    sleepMs: 0,
    callFeesApi: async (body) => { called++; return feeResponse(body[0].FeesEstimateRequest.Identifier); },
  });
  assert.equal(called, 0);
  assert.equal(r.reused, 1);
  assert.equal(r.refreshed, 0);
});

await ta('🚨 価格が変われば同じSKUでも取り直す (別の行として貯まる)', async () => {
  const r = await refreshFees(db, [target({ in_listing_price: 2000 })], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { referral: 168 }),
  });
  assert.equal(r.refreshed, 1);
  const rows = db.prepare('SELECT * FROM amazon_fee_estimate WHERE seller_sku = ? ORDER BY in_listing_price').all('sku1');
  assert.equal(rows.length, 2);       // 1000円の見積と2000円の見積が別々に残る
});

await ta('🚨 内訳合計が合わない見積は inconsistent として記録する', async () => {
  const r = await refreshFees(db, [target({ seller_sku: 'skuBad' })], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { referral: 84, total: 999 }),
  });
  assert.equal(r.inconsistent, 1);
  const row = db.prepare("SELECT fee_status FROM amazon_fee_estimate WHERE seller_sku='skuBad'").get();
  assert.equal(row.fee_status, 'inconsistent');
});

await ta('API が Status != Success を返した SKU は失敗として数える (静かに落とさない)', async () => {
  const r = await refreshFees(db, [target({ seller_sku: 'skuErr' })], {
    sleepMs: 0,
    callFeesApi: async (body) => ([{
      Status: 'ClientError',
      FeesEstimateIdentifier: { SellerInputIdentifier: body[0].FeesEstimateRequest.Identifier },
      Error: { Message: 'Invalid ASIN' },
    }]),
  });
  assert.equal(r.failed, 1);
  assert.equal(r.refreshed, 0);
  assert.match(r.errors[0].error, /Invalid ASIN/);
});

await ta('API が投げ続けたらリトライ上限で諦め、失敗として記録する', async () => {
  let calls = 0;
  const r = await refreshFees(db, [target({ seller_sku: 'skuThrow' })], {
    sleepMs: 0,
    callFeesApi: async () => { calls++; throw new Error('429 Too Many Requests'); },
  });
  assert.equal(calls, 3);            // MAX_RETRIES
  assert.equal(r.failed, 1);
});

await ta('🚨 全体終了期限を過ぎたら残りを翌日に回す (途中で止めても壊れない)', async () => {
  const many = Array.from({ length: 60 }, (_, i) => target({ seller_sku: `bulk${i}` }));
  let calls = 0;
  const r = await refreshFees(db, many, {
    sleepMs: 0,
    deadline: new Date('2000-01-01T00:00:00Z'),   // 既に過ぎている
    callFeesApi: async (body) => { calls++; return feeResponse(body[0].FeesEstimateRequest.Identifier); },
  });
  assert.equal(calls, 0);
  assert.equal(r.stoppedByDeadline, true);
  assert.equal(r.refreshed, 0);
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
