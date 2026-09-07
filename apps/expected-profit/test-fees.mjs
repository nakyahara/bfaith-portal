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
const { planRefresh, buildFeeRequest, toEstimateRow, saveEstimates, loadCache, refreshFees, cacheKey,
  storedSellerId, withResolvedSeller, rememberSellerId,
  validityOffsetDays, feeValidDays,
  BATCH_SIZE, BATCH_SLEEP_MS, SP_API_FEES_RATE_PER_SEC, SP_API_FEES_MAX_BATCH,
  looksLikeAsin, feeIdentifierOf, backoffDays, recordFailure, clearFailure, loadFailures,
  allocateFetchBudget, retryAfterOf } = await import('./refresh-fees.js');
const { getSetting, setSetting, SETTING_AMAZON_SELLER_ID } = await import('./db.js');
const forgetSeller = () => setSetting(db, SETTING_AMAZON_SELLER_ID, null);

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const MARKETPLACE = process.env.SP_API_MARKETPLACE_ID;
const target = (over = {}) => ({
  seller_id: 'S1', marketplace_id: MARKETPLACE, seller_sku: 'sku1', asin: 'B001',
  in_listing_price: 1000, in_shipping: 0, in_points: 0, in_fulfillment: 'FBM', in_currency: 'JPY',
  ...over,
});

const feeResponse = (identifier, { referral = 84, fba = null, total = null, sellerId } = {}) => ([{
  Status: 'Success',
  FeesEstimateIdentifier: { SellerInputIdentifier: identifier, ...(sellerId ? { SellerId: sellerId } : {}) },
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
  const body = buildFeeRequest([target({ in_shipping: 230 })]);
  assert.equal(body[0].FeesEstimateRequest.PriceToEstimateFees.Shipping.Amount, 230);
});

t('🚨 marketplace は target のものを送る (環境変数を混ぜない)', () => {
  const body = buildFeeRequest([target()]);
  assert.equal(body[0].FeesEstimateRequest.MarketplaceId, MARKETPLACE);
});

t('🚨 ポイントも見積入力として渡す', () => {
  const body = buildFeeRequest([target({ in_points: 5 })]);
  assert.equal(body[0].FeesEstimateRequest.PriceToEstimateFees.Points.PointsNumber, 5);
});

t('FBA / FBM を IsAmazonFulfilled に反映する', () => {
  assert.equal(buildFeeRequest([target({ in_fulfillment: 'FBA' })])[0].FeesEstimateRequest.IsAmazonFulfilled, true);
  assert.equal(buildFeeRequest([target()])[0].FeesEstimateRequest.IsAmazonFulfilled, false);
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

t('[!] 有効期限は SKU ごとに 7〜14 日にばらける (一斉失効を防ぐ)', () => {
  // 🚨 全部を同じ日数にすると、同じ晩に取ったものが同じ晩に一斉失効し、
  //    14 日ごとに 7,385 件の取り直しが起きる。SP-API は 0.5 req/s なので 13 分かかる
  const row = toEstimateRow(target(), feeResponse('x')[0].FeesEstimate, '2026-09-07T00:00:00Z');
  const days = feeValidDays('sku1');
  assert.ok(days >= 7 && days <= 14, `7〜14日のはず (実際 ${days})`);
  const expected = new Date(Date.UTC(2026, 8, 7 + days)).toISOString();
  assert.equal(row.valid_until, expected);
});

t('[!] 同じ SKU なら毎回同じズレになる (毎晩ズレ直さない)', () => {
  assert.equal(validityOffsetDays('sku1'), validityOffsetDays('sku1'));
  assert.equal(feeValidDays('sku1'), feeValidDays('sku1'));
});

t('[!] ズレは 0〜7 の範囲に散る (どれか1日に固まらない)', () => {
  const seen = new Set();
  for (let i = 0; i < 400; i++) seen.add(validityOffsetDays('pr_1272115_F_2025_' + i));
  assert.equal(seen.size, 8, `8種類に散るはず (実際 ${seen.size})`);
  // 偏りも見る: 400件を8つに分けて、どれも極端に少なくない
  const counts = new Array(8).fill(0);
  for (let i = 0; i < 400; i++) counts[validityOffsetDays('pr_1272115_F_2025_' + i)]++;
  assert.ok(Math.min(...counts) >= 20, `偏りすぎ: ${JSON.stringify(counts)}`);
});

t('SKU が空でも落ちない', () => {
  assert.ok(Number.isInteger(validityOffsetDays(null)));
  assert.ok(feeValidDays(undefined) >= 7);
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
  assert.equal(r.failedTargets, 0);
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
  // SKU 単位の失敗 (バッチは通っている)
  assert.equal(r.failedTargets, 1);
  assert.equal(r.failedBatches, 0);
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
  // バッチ単位の失敗。対象数はバッチに含まれる SKU 数で数える (Codex R1-9)
  assert.equal(r.failedBatches, 1);
  assert.equal(r.failedTargets, 1);
});

await ta('[!] 20SKU のバッチ失敗を「1件の失敗」と数えない (取得率の判断に使えなくなる)', async () => {
  const many = Array.from({ length: 20 }, (_, i) => target({ seller_sku: `batch${i}` }));
  const r = await refreshFees(db, many, {
    sleepMs: 0,
    callFeesApi: async () => { throw new Error('500'); },
  });
  assert.equal(r.failedBatches, 1);
  assert.equal(r.failedTargets, 20);   // 対象単位では20件
});

await ta('部分成功: 一部の SKU だけ失敗したときも対象単位で数える', async () => {
  const targets = [target({ seller_sku: 'okSku' }), target({ seller_sku: 'ngSku' })];
  const r = await refreshFees(db, targets, {
    sleepMs: 0,
    callFeesApi: async (body) => body.map((b, i) => (i === 0
      ? feeResponse(b.FeesEstimateRequest.Identifier, { referral: 84 })[0]
      : { Status: 'ClientError', FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier }, Error: { Message: 'bad' } })),
  });
  assert.equal(r.refreshed, 1);
  assert.equal(r.failedTargets, 1);
  assert.equal(r.failedBatches, 0);
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
  assert.equal(r.pendingTargets, 60);   // 未処理として残す (失敗ではない)
});

await ta('[!] marketplace が環境設定と違う対象は拒否する (送った条件と保存する条件をずらさない)', async () => {
  await assert.rejects(
    () => refreshFees(db, [target({ marketplace_id: 'ATVPDKIKX0DER' })], { sleepMs: 0, callFeesApi: async () => [] }),
    /marketplace_id が環境設定/,
  );
});

await ta('[!] FBA なのに FBAFees が無い見積は fee_status に残る (DB まで確認)', async () => {
  // toEstimateRow から fulfillment 引数を外したら、この試験が落ちる
  const r = await refreshFees(db, [target({ seller_sku: 'fbaNoFee', in_fulfillment: 'FBA' })], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { referral: 84 }),
  });
  assert.equal(r.refreshed, 1);
  const row = db.prepare("SELECT fee_status FROM amazon_fee_estimate WHERE seller_sku='fbaNoFee'").get();
  assert.equal(row.fee_status, 'missing_fba_fee');
});

await ta('[!] FBM なのに FBAFees が来た見積も fee_status に残る', async () => {
  const r = await refreshFees(db, [target({ seller_sku: 'fbmWithFba' })], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { referral: 84, fba: 462 }),
  });
  assert.equal(r.refreshed, 1);
  const row = db.prepare("SELECT fee_status FROM amazon_fee_estimate WHERE seller_sku='fbmWithFba'").get();
  assert.equal(row.fee_status, 'unexpected_fba_fee');
});

await ta('[!] 同じ FeeType が重複したら duplicate_fee_type (過少控除を防ぐ)', async () => {
  const r = await refreshFees(db, [target({ seller_sku: 'dupFee' })], {
    sleepMs: 0,
    callFeesApi: async (body) => ([{
      Status: 'Success',
      FeesEstimateIdentifier: { SellerInputIdentifier: body[0].FeesEstimateRequest.Identifier },
      FeesEstimate: {
        TotalFeesEstimate: { Amount: 300 },
        FeeDetailList: [
          { FeeType: 'ReferralFee', FinalFee: { Amount: 100 } },
          { FeeType: 'ReferralFee', FinalFee: { Amount: 200 } },
        ],
      },
    }]),
  });
  const row = db.prepare("SELECT fee_status FROM amazon_fee_estimate WHERE seller_sku='dupFee'").get();
  assert.equal(row.fee_status, 'duplicate_fee_type');
});

await ta('[!] API が例外を投げず null を返してもバッチ失敗として数える', async () => {
  const r = await refreshFees(db, [target({ seller_sku: 'nullRes' })], {
    sleepMs: 0,
    callFeesApi: async () => null,
  });
  assert.equal(r.failedBatches, 1);
  assert.equal(r.failedTargets, 1);
  assert.equal(r.refreshed, 0);
});

await ta('[!] ポイントが取れていない対象は API を呼ばずに弾く (0で埋めない)', async () => {
  let called = 0;
  const r = await refreshFees(db, [target({ seller_sku: 'noPoints', in_points: null })], {
    sleepMs: 0,
    callFeesApi: async () => { called++; return []; },
  });
  assert.equal(called, 0);
  assert.equal(r.invalidTargets, 1);
  assert.deepEqual(r.invalid[0].missing, ['in_points']);
});

await ta('ASIN が無い対象も API を呼ばずに弾く', async () => {
  const r = await refreshFees(db, [target({ seller_sku: 'noAsin', asin: null })], {
    sleepMs: 0, callFeesApi: async () => [],
  });
  assert.equal(r.invalidTargets, 1);
  assert.deepEqual(r.invalid[0].missing, ['asin']);
});


await ta('[!] 欠損対象が混ざっても正常対象は処理される (検証 → 除外 → marketplace 比較の順)', async () => {
  // marketplace 比較を先にすると、marketplace_id が欠けた1件で全体が例外になり
  // 正常な対象まで処理されず invalidTargets にも残らない
  let called = 0;
  const r = await refreshFees(db, [
    target({ seller_sku: 'mixOk', in_listing_price: 3000 }),
    target({ seller_sku: 'mixNg', marketplace_id: null }),
  ], {
    sleepMs: 0,
    callFeesApi: async (body) => { called++; return feeResponse(body[0].FeesEstimateRequest.Identifier, { referral: 252 }); },
  });
  assert.equal(called, 1);                 // 正常対象は API まで届く
  assert.equal(r.refreshed, 1);
  assert.equal(r.invalidTargets, 1);       // 欠損対象は記録される
  assert.deepEqual(r.invalid[0].missing, ['marketplace_id']);
});

await ta('[!] fulfillment が未解決 (null) の対象は API を呼ばずに弾く', async () => {
  let called = 0;
  const r = await refreshFees(db, [target({ seller_sku: 'noChannel', in_fulfillment: null })], {
    sleepMs: 0, callFeesApi: async () => { called++; return []; },
  });
  assert.equal(called, 0);
  assert.equal(r.invalidTargets, 1);
  assert.deepEqual(r.invalid[0].missing, ['in_fulfillment']);
});

await ta('通貨が欠けた対象も弾く (JPY で埋めない)', async () => {
  const r = await refreshFees(db, [target({ seller_sku: 'noCur', in_currency: null })], {
    sleepMs: 0, callFeesApi: async () => [],
  });
  assert.equal(r.invalidTargets, 1);
  assert.deepEqual(r.invalid[0].missing, ['in_currency']);
});


console.log('');
console.log('セラーID の解決 (実データで判明: miniPC の env に SP_API_SELLER_ID が無い)');

// 🚨 ここから先は保存済みの見積を消してから試す。
//    storedSellerId が前のテストで入った 'S1' を拾うと、何を見ているのか分からなくなる
db.exec('DELETE FROM amazon_fee_estimate');

t('覚えたセラーを返す', () => {
  forgetSeller();
  rememberSellerId(db, 'A6HMLHKUUJC27');
  assert.equal(storedSellerId(db, undefined), 'A6HMLHKUUJC27');
});

t('[!] 見積テーブルに別セラーの行が残っていても、覚え書きが勝つ', () => {
  // 🚨 DISTINCT から推測する作りだと、旧セラーの行が1つ残っただけで
  //    「決められない」に落ち、env が空の環境では二度とキーを作れない (Codex R7-3)
  saveEstimates(db, [toEstimateRow(target({ seller_id: 'OLD_SELLER', seller_sku: 'x2' }),
    { TotalFeesEstimate: { Amount: 84 }, FeeDetailList: [{ FeeType: 'ReferralFee', FinalFee: { Amount: 84 } }] }, new Date().toISOString())]);
  assert.equal(storedSellerId(db, undefined), 'A6HMLHKUUJC27');
});

t('覚え書きが無ければ env を使う / どちらも無ければ null', () => {
  forgetSeller();
  assert.equal(storedSellerId(db, 'FROM_ENV'), 'FROM_ENV');
  assert.equal(storedSellerId(db, undefined), null);
});

t('[!] セラーが変わったら覚え直す (認証アカウントの切り替え)', () => {
  forgetSeller();
  assert.deepEqual(rememberSellerId(db, 'S_A'), { changed: true, previous: null });
  assert.deepEqual(rememberSellerId(db, 'S_A'), { changed: false, previous: 'S_A' });
  assert.deepEqual(rememberSellerId(db, 'S_B'), { changed: true, previous: 'S_A' });
  assert.equal(storedSellerId(db, undefined), 'S_B');
  forgetSeller();
});

t('withResolvedSeller は分かっている値で上書きする / 分からなければ触らない', () => {
  assert.equal(withResolvedSeller([target({ seller_id: null })], 'A6')[0].seller_id, 'A6');
  assert.equal(withResolvedSeller([target({ seller_id: 'WRONG' })], 'A6')[0].seller_id, 'A6');
  assert.equal(withResolvedSeller([target({ seller_id: 'S1' })], null)[0].seller_id, 'S1');
});

await ta('[!] env に seller_id が無くても見積を取り、レスポンスの SellerId で保存する', async () => {
  db.exec('DELETE FROM amazon_fee_estimate');
  forgetSeller();
  const r = await refreshFees(db, [target({ seller_id: null, seller_sku: 'noSeller' })], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { sellerId: 'A6HMLHKUUJC27' }),
  });
  assert.equal(r.invalidTargets, 0, 'seller_id が無いだけで弾いてはいけない');
  assert.equal(r.refreshed, 1);
  assert.equal(r.sellerId, 'A6HMLHKUUJC27');
  assert.equal(db.prepare("SELECT seller_id FROM amazon_fee_estimate WHERE seller_sku = 'noSeller'").get().seller_id,
    'A6HMLHKUUJC27');
  // 次回の照合に使えるよう覚えている
  assert.equal(getSetting(db, SETTING_AMAZON_SELLER_ID), 'A6HMLHKUUJC27');
});

await ta('[!] env の seller_id が保存済みと違っても、保存済みの値でキーを作る (取り直さない)', async () => {
  let called = 0;
  const r = await refreshFees(db, [target({ seller_id: 'WRONG_FROM_ENV', seller_sku: 'noSeller' })], {
    sleepMs: 0, callFeesApi: async (body) => { called++; return feeResponse(body[0].FeesEstimateRequest.Identifier); },
  });
  assert.equal(called, 0, '保存済みキーと一致するので API を叩かない');
  assert.equal(r.reused, 1);
});

await ta('[!] レスポンスにも保存済みにもセラーが無い見積は保存しない (キーが作れない)', async () => {
  db.exec('DELETE FROM amazon_fee_estimate');
  forgetSeller();
  const r = await refreshFees(db, [target({ seller_id: null, seller_sku: 'unknownSeller' })], {
    sleepMs: 0, callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier),   // SellerId 無し
  });
  assert.equal(r.refreshed, 0);
  assert.equal(r.errors[0].error, 'seller_id_unresolved');
  assert.equal(db.prepare("SELECT COUNT(*) c FROM amazon_fee_estimate WHERE seller_sku = 'unknownSeller'").get().c, 0);
});

await ta('[!] 1回の実行で複数のセラーが返ってきたら補完も記憶もしない', async () => {
  // 🚨 どれが正か決められない状態で最後の1件に揃えると、誤ったキーで保存してしまう (Codex R7-3)
  db.exec('DELETE FROM amazon_fee_estimate');
  forgetSeller();
  const r = await refreshFees(db, [
    target({ seller_id: null, seller_sku: 'multiA' }),
    target({ seller_id: null, seller_sku: 'multiB' }),
  ], {
    sleepMs: 0,
    callFeesApi: async (body) => body.map((b, i) => ({
      Status: 'Success',
      FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier, SellerId: i === 0 ? 'S_A' : 'S_B' },
      FeesEstimate: {
        TotalFeesEstimate: { CurrencyCode: 'JPY', Amount: 84 },
        FeeDetailList: [{ FeeType: 'ReferralFee', FeeAmount: { Amount: 84 }, FinalFee: { Amount: 84 }, FeePromotion: { Amount: 0 } }],
      },
    })),
  });
  assert.equal(r.sellerConflict, true);
  assert.equal(r.sellerId, null);
  assert.equal(getSetting(db, SETTING_AMAZON_SELLER_ID), null, '食い違ったまま覚えてはいけない');
  // 応答が自分で名乗った行は保存される (補完しないだけ)
  assert.equal(r.refreshed, 2);
});

await ta('marketplace_id は引き続き必須 (これが無いとキーが作れない)', async () => {
  let called = 0;
  const r = await refreshFees(db, [target({ marketplace_id: null, seller_sku: 'noMk' })], {
    sleepMs: 0, callFeesApi: async () => { called++; return []; },
  });
  assert.equal(called, 0);
  assert.equal(r.invalidTargets, 1);
  assert.deepEqual(r.invalid[0].missing, ['marketplace_id']);
});


await ta('[!] 応答のセラーが1つに変わったら覚え直す (refreshFees を通す)', async () => {
  // 🚨 rememberSellerId を直接呼ぶ試験では、observedSellerId を既知で初期化した
  //    「正常な切り替えまで競合扱い」を検出できない (Codex R8-1)
  db.exec('DELETE FROM amazon_fee_estimate');
  forgetSeller();
  rememberSellerId(db, 'S_OLD');
  const r = await refreshFees(db, [target({ seller_id: null, seller_sku: 'switched' })], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { sellerId: 'S_NEW' }),
  });
  assert.equal(r.sellerConflict, false, '応答が1種類なら競合ではない');
  assert.equal(r.sellerId, 'S_NEW');
  assert.equal(getSetting(db, SETTING_AMAZON_SELLER_ID), 'S_NEW', '覚え書きが古いままだと世代構築が古いキーを使う');
  assert.equal(db.prepare("SELECT seller_id FROM amazon_fee_estimate WHERE seller_sku = 'switched'").get().seller_id, 'S_NEW');
  forgetSeller();
});

await ta('[!] 応答が誰も名乗らないときだけ、既知のセラーで補う', async () => {
  db.exec('DELETE FROM amazon_fee_estimate');
  forgetSeller();
  rememberSellerId(db, 'S_KNOWN');
  const r = await refreshFees(db, [target({ seller_id: null, seller_sku: 'noName' })], {
    sleepMs: 0,
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier),   // SellerId 無し
  });
  assert.equal(r.refreshed, 1);
  assert.equal(db.prepare("SELECT seller_id FROM amazon_fee_estimate WHERE seller_sku = 'noName'").get().seller_id, 'S_KNOWN');
  assert.equal(getSetting(db, SETTING_AMAZON_SELLER_ID), 'S_KNOWN', '名乗っていないのに覚え直してはいけない');
  forgetSeller();
});


console.log('');
t('[!] SP-API の公式レート制限を守っている (詰めると 429 になる)', () => {
  // 公式: getMyFeesEstimates は rate 0.5 req/s / burst 1 / batch 上限 20
  // https://developer-docs.amazon.com/sp-api/reference/getmyfeesestimates
  // 🚨 ここを速くしたくなったら、先に公式ドキュメントを見直すこと。
  //    burst が 1 なので、連続呼び出しは 1/0.5 = 2,000ms 以上あけないと必ず弾かれる
  const minGapMs = 1000 / SP_API_FEES_RATE_PER_SEC;
  assert.equal(minGapMs, 2000);
  assert.ok(BATCH_SLEEP_MS >= minGapMs, `バッチ間隔 ${BATCH_SLEEP_MS}ms は ${minGapMs}ms 以上でなければならない`);
  assert.ok(BATCH_SIZE <= SP_API_FEES_MAX_BATCH, `1リクエスト ${BATCH_SIZE} 件は上限 ${SP_API_FEES_MAX_BATCH} を超えている`);
});

t('リトライのバックオフもレート制限より短くならない', () => {
  // 実装は sleepMs * (attempt + 1) で伸ばす。1回目が既に 2,000ms 以上あればよい
  assert.ok(BATCH_SLEEP_MS * 1 >= 1000 / SP_API_FEES_RATE_PER_SEC);
});

console.log('再利用が効いているか (SP-API は 0.5 req/s。取り直しは 13 分かかる)');

// 見積を n 件、キャッシュに入れる
function seedCache(n, { price = 1000, validUntil = '2099-01-01T00:00:00Z' } = {}) {
  db.exec('DELETE FROM amazon_fee_estimate');
  const rows = [];
  for (let i = 0; i < n; i++) {
    const r = toEstimateRow(target({ seller_sku: 'c' + i, in_listing_price: price }),
      feeResponse('x')[0].FeesEstimate, '2026-09-07T00:00:00Z');
    r.valid_until = validUntil;
    rows.push(r);
  }
  saveEstimates(db, rows);
  return Array.from({ length: n }, (_, i) => target({ seller_sku: 'c' + i, in_listing_price: price }));
}

await ta('[!] 入力が変わっていなければ 1 件も取りに行かない', async () => {
  forgetSeller();
  const targets = seedCache(50);
  let called = 0;
  const r = await refreshFees(db, targets, { sleepMs: 0, callFeesApi: async () => { called++; return []; } });
  assert.equal(called, 0, 'API を叩いてはいけない');
  assert.equal(r.reused, 50);
  assert.equal(r.refreshed, 0);
  assert.equal(r.refetchAnomaly, false);
});

await ta('[!] 1 晩に取り直す件数に上限がある (枠を使い切らない)', async () => {
  forgetSeller();
  const targets = seedCache(50, { validUntil: '2000-01-01T00:00:00Z' });   // 全部期限切れ
  let batches = 0;
  const r = await refreshFees(db, targets, {
    sleepMs: 0, maxFetch: 20,
    callFeesApi: async (body) => { batches++; return body.map(b => feeResponse(b.FeesEstimateRequest.Identifier)[0]); },
  });
  assert.equal(batches, 1, '20件 = 1バッチだけ');
  assert.equal(r.deferred, 30, '残り30件は翌晩に回る');
  assert.equal(r.plannedRefetch, 50);
});

await ta('[!] キャッシュが温まっているのに大量に取り直すなら警告する', async () => {
  // 🚨 これが seller_id の不具合を検出できる形。静かに 370 回叩かせない
  forgetSeller();
  seedCache(50);
  // 価格を全部変えた = 全件キー不一致
  const targets = Array.from({ length: 50 }, (_, i) => target({ seller_sku: 'c' + i, in_listing_price: 9999 }));
  const r = await refreshFees(db, targets, {
    sleepMs: 0, maxFetch: 0,
    callFeesApi: async () => { throw new Error('叩かせない'); },
  });
  assert.equal(r.refetchAnomaly, true);
  assert.equal(r.plannedRefetch, 50);
});

await ta('キャッシュが空 (初回) なら警告しない', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  const r = await refreshFees(db, [target({ seller_sku: 'first' })], {
    sleepMs: 0, maxFetch: 0, callFeesApi: async () => { throw new Error('叩かせない'); },
  });
  assert.equal(r.refetchAnomaly, false, '初回の全件取得は異常ではない');
});

await ta('[!] 取り直しは「入力が変わった/見積が無い」を先にやる (期限切れは後)', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  // 期限切れ 2 件 (入力は一致) と、見積が無い 1 件
  const expired = [];
  for (const sku of ['e1', 'e2']) {
    const r = toEstimateRow(target({ seller_sku: sku }), feeResponse('x')[0].FeesEstimate, '2026-09-07T00:00:00Z');
    r.valid_until = '2000-01-01T00:00:00Z';
    expired.push(r);
  }
  saveEstimates(db, expired);
  const targets = [target({ seller_sku: 'e1' }), target({ seller_sku: 'e2' }), target({ seller_sku: 'new1' })];
  const asked = [];
  await refreshFees(db, targets, {
    sleepMs: 0, maxFetch: 1,
    callFeesApi: async (body) => {
      for (const b of body) asked.push(String(b.FeesEstimateRequest.Identifier).split("|")[0]);
      return body.map(b => feeResponse(b.FeesEstimateRequest.Identifier)[0]);
    },
  });
  assert.deepEqual(asked, ['new1'], '見積が無いものを先に取る (期限切れは数字が出るだけまし)');
});


console.log('');
console.log('商品の指し方 (実データ: JAN を ASIN として送っていた 5 件)');

t('[!] ASIN の形なら ASIN で指す', () => {
  assert.ok(looksLikeAsin('B09WMM1G2S'));
  assert.deepEqual(feeIdentifierOf(target({ asin: 'B09WMM1G2S' })), { IdType: 'ASIN', IdValue: 'B09WMM1G2S' });
});

t('[!] 13桁の JAN は ASIN ではない → 自社SKU で指す', () => {
  // 🚨 実データ: 4901267220001 を IdType:ASIN で送って必ず client-side error になっていた。
  //    公式も「ASIN か SellerSKU。UPC/ISBN 等の識別子は不可」
  assert.equal(looksLikeAsin('4901267220001'), false);
  assert.deepEqual(feeIdentifierOf(target({ asin: '4901267220001', seller_sku: '3M-3JSM-8UDX' })),
    { IdType: 'SellerSKU', IdValue: '3M-3JSM-8UDX' });
});

t('書籍の ISBN10 は ASIN として正しい (英数10桁)', () => {
  assert.ok(looksLikeAsin('4062748223'));
  assert.equal(looksLikeAsin('406274822'), false, '9桁は違う');
  assert.equal(looksLikeAsin('40627482231'), false, '11桁は違う');
  assert.equal(looksLikeAsin('b09wmm1g2s'), false, '小文字は ASIN ではない');
});

t('リクエストにも反映される', () => {
  const body = buildFeeRequest([target({ asin: '4901267220001', seller_sku: 'sku-jan' })]);
  assert.equal(body[0].IdType, 'SellerSKU');
  assert.equal(body[0].IdValue, 'sku-jan');
});

console.log('');
console.log('毎晩失敗し続ける対象を止める (実データ: 86件が毎晩必ず失敗)');

t('待ち日数は 1 → 3 → 7 → 14 で伸びる (1回目は翌晩＝一時的な失敗はすぐ回復)', () => {
  assert.equal(backoffDays(1), 1);
  assert.equal(backoffDays(2), 3);
  assert.equal(backoffDays(3), 7);
  assert.equal(backoffDays(4), 14);
  assert.equal(backoffDays(99), 14, '無限に伸ばさない');
});

await ta('[!] 失敗した対象は記録され、待ち時間の間は試さない', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  const t1 = target({ seller_sku: 'alwaysFail' });
  const fail = async (body) => body.map(b => ({
    Status: 'ClientError',
    FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier },
    Error: { Message: 'There is an client-side error.' },
  }));
  // 1晩目: 試して失敗し、記録される
  const r1 = await refreshFees(db, [t1], { sleepMs: 0, callFeesApi: fail, now: () => new Date('2026-09-08T00:00:00Z') });
  assert.equal(r1.failedTargets, 1);
  assert.equal(loadFailures(db).size, 1);

  // 同じ晩にもう一度回しても、待ちが残っているので叩かない
  let called = 0;
  const r2 = await refreshFees(db, [t1], {
    sleepMs: 0, now: () => new Date('2026-09-08T01:00:00Z'),
    callFeesApi: async () => { called++; return []; },
  });
  assert.equal(called, 0, '待ち時間の間は叩いてはいけない');
  assert.equal(r2.waitingOnFailure, 1);
});

await ta('[!] 待ち時間が過ぎたらまた試す (諦めっぱなしにしない)', async () => {
  let called = 0;
  const t1 = target({ seller_sku: 'alwaysFail' });
  await refreshFees(db, [t1], {
    sleepMs: 0, now: () => new Date('2026-09-20T00:00:00Z'),   // 1日どころか十分後
    callFeesApi: async (body) => { called++; return body.map(b => feeResponse(b.FeesEstimateRequest.Identifier, { sellerId: 'S1' })[0]); },
  });
  assert.equal(called, 1);
});

await ta('[!] 成功したら失敗記録を消す', async () => {
  assert.equal(loadFailures(db).size, 0, '直ったのに記録が残っている');
});

await ta('[!] 入力が変われば別キーなので、待ち時間に関係なくすぐ試す', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  const bad = target({ seller_sku: 'priceChanged', in_listing_price: 1000 });
  await refreshFees(db, [bad], {
    sleepMs: 0, now: () => new Date('2026-09-08T00:00:00Z'),
    callFeesApi: async (body) => body.map(b => ({
      Status: 'ClientError',
      FeesEstimateIdentifier: { SellerInputIdentifier: b.FeesEstimateRequest.Identifier },
      Error: { Message: 'bad' },
    })),
  });
  let called = 0;
  await refreshFees(db, [target({ seller_sku: 'priceChanged', in_listing_price: 1200 })], {
    sleepMs: 0, now: () => new Date('2026-09-08T01:00:00Z'),
    callFeesApi: async (body) => { called++; return body.map(b => feeResponse(b.FeesEstimateRequest.Identifier, { sellerId: 'S1' })[0]); },
  });
  assert.equal(called, 1, '値段が直ったのに待たされてはいけない');
  db.exec('DELETE FROM amazon_fee_failure');
});


console.log('');
console.log('1晩の枠の配り方 (Codex R9-1: 飢餓を防ぐ)');

const nd = (reason, sku, fetchedAt = null) =>
  ({ reason, target: target({ seller_sku: sku }), cached: fetchedAt ? { fetched_at: fetchedAt } : null });

t('上限に収まるなら全部やる', () => {
  const need = [nd('missing', 'a'), nd('expired', 'b', '2026-09-01T00:00:00Z')];
  assert.equal(allocateFetchBudget(need, 10).length, 2);
});

t('[!] 新規が大量にあっても、期限切れに枠を残す (飢餓を防ぐ)', () => {
  // 🚨 毎晩4,000件以上の新規が出続けると、期限切れに永久に枠が回らない (Codex R9-1)
  const need = [];
  for (let i = 0; i < 100; i++) need.push(nd('missing', 'new' + i));
  for (let i = 0; i < 100; i++) need.push(nd('expired', 'old' + i, '2026-09-01T00:00:00Z'));
  const picked = allocateFetchBudget(need, 20);
  assert.equal(picked.length, 20);
  const expired = picked.filter(n => n.reason === 'expired').length;
  assert.equal(expired, 5, `20 の 25% = 5 は期限切れに残すはず (実際 ${expired})`);
});

t('期限切れが少なければ、余った枠は新規に回す (無駄にしない)', () => {
  const need = [];
  for (let i = 0; i < 100; i++) need.push(nd('missing', 'new' + i));
  need.push(nd('expired', 'old1', '2026-09-01T00:00:00Z'));
  const picked = allocateFetchBudget(need, 20);
  assert.equal(picked.length, 20);
  assert.equal(picked.filter(n => n.reason === 'expired').length, 1);
});

t('新規が少なければ、余った枠は期限切れに回す', () => {
  const need = [nd('missing', 'new1')];
  for (let i = 0; i < 100; i++) need.push(nd('expired', 'old' + i, '2026-09-01T00:00:00Z'));
  const picked = allocateFetchBudget(need, 20);
  assert.equal(picked.length, 20);
  assert.equal(picked.filter(n => n.reason === 'expired').length, 19);
});

t('[!] 同じ種類のなかは古い順 (待たされたものから)', () => {
  // 🚨 入力順のままだと、先頭側が毎晩変わるときに末尾側が永久に進まない
  const need = [
    nd('expired', 'newest', '2026-09-07T00:00:00Z'),
    nd('expired', 'oldest', '2026-01-01T00:00:00Z'),
    nd('expired', 'middle', '2026-05-01T00:00:00Z'),
  ];
  const picked = allocateFetchBudget(need, 1);
  assert.equal(picked[0].target.seller_sku, 'oldest');
});

t('[!] 見積が1件も無いものが最優先 (いちばん待たされている)', () => {
  const need = [nd('expired', 'has', '2026-01-01T00:00:00Z'), nd('missing', 'none')];
  assert.equal(allocateFetchBudget(need, 1)[0].target.seller_sku, 'none');
});

console.log('');
console.log('失敗の待ちは業務日で数える (Codex R9-4)');

t('[!] 00:10 に失敗しても「翌日の 00:00 JST」から試せる (翌々晩にならない)', () => {
  // 2026-09-08 09:10 JST = 2026-09-08T00:10:00Z
  const r = retryAfterOf('2026-09-08T00:10:00Z', 1);
  // 2026-09-09 00:00 JST = 2026-09-08T15:00:00Z
  assert.equal(r, '2026-09-08T15:00:00.000Z');
});

t('3日待ちも同じ数え方', () => {
  assert.equal(retryAfterOf('2026-09-08T00:10:00Z', 3), '2026-09-10T15:00:00.000Z');
});

t('読めない日時でも落ちない', () => {
  assert.ok(retryAfterOf('ごみ', 1));
});

console.log('');
console.log('取れたが使えない見積 (Codex R9-2)');

await ta('[!] Success でも FBA なのに FBAFees が無ければ、待たせる (毎晩取り直さない)', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  const t1 = target({ seller_sku: 'noFbaFee', in_fulfillment: 'FBA' });
  const r1 = await refreshFees(db, [t1], {
    sleepMs: 0, now: () => new Date('2026-09-08T00:00:00Z'),
    // ReferralFee だけ返す = FBA なのに FBAFees が無い
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { sellerId: 'S1' }),
  });
  assert.equal(r1.unusable, 1, '使えない見積として数えるはず');
  assert.equal(loadFailures(db).size, 1, '待ち記録が作られていない');

  let called = 0;
  const r2 = await refreshFees(db, [t1], {
    sleepMs: 0, now: () => new Date('2026-09-08T01:00:00Z'),
    callFeesApi: async () => { called++; return []; },
  });
  assert.equal(called, 0, '使えない見積を毎晩取り直してはいけない');
  assert.equal(r2.waitingOnFailure, 1);
  db.exec('DELETE FROM amazon_fee_failure');
});

await ta('ちゃんと使える見積が返れば待ち記録は消える', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  const t1 = target({ seller_sku: 'okFba', in_fulfillment: 'FBA' });
  const r = await refreshFees(db, [t1], {
    sleepMs: 0, now: () => new Date('2026-09-08T00:00:00Z'),
    callFeesApi: async (body) => feeResponse(body[0].FeesEstimateRequest.Identifier, { sellerId: 'S1', fba: 462 }),
  });
  assert.equal(r.unusable, 0);
  assert.equal(r.refreshed, 1);
  assert.equal(loadFailures(db).size, 0);
});

console.log('');
console.log('本番の呼び出し間隔 (Codex R9-3: 定数の比較だけでは足りない)');

await ta('[!] バッチとバッチの間を必ずあける (本番の refreshFees を通す)', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  // 21件 = 2バッチ。時計と sleep を差し替えて、実時間を待たずに間隔を測る
  const targets = Array.from({ length: 21 }, (_, i) => target({ seller_sku: 'gap' + i }));
  let t = Date.parse('2026-09-08T00:00:00Z');
  const callTimes = [];
  await refreshFees(db, targets, {
    now: () => new Date(t),
    sleep: async (ms) => { t += ms; },
    callFeesApi: async (body) => {
      callTimes.push(t);
      return body.map(b => feeResponse(b.FeesEstimateRequest.Identifier, { sellerId: 'S1' })[0]);
    },
  });
  assert.equal(callTimes.length, 2, '21件 = 2バッチ');
  const gap = callTimes[1] - callTimes[0];
  assert.ok(gap >= 1000 / SP_API_FEES_RATE_PER_SEC,
    `バッチ間隔 ${gap}ms が 0.5 req/s (2,000ms) を下回っている`);
});

await ta('[!] リトライのあとも次のバッチまで間隔をあける', async () => {
  forgetSeller();
  db.exec('DELETE FROM amazon_fee_estimate');
  db.exec('DELETE FROM amazon_fee_failure');
  const targets = Array.from({ length: 21 }, (_, i) => target({ seller_sku: 'retry' + i }));
  let t = Date.parse('2026-09-08T00:00:00Z');
  const callTimes = [];
  let n = 0;
  await refreshFees(db, targets, {
    now: () => new Date(t),
    sleep: async (ms) => { t += ms; },
    callFeesApi: async (body) => {
      callTimes.push(t);
      n++;
      if (n === 1) throw new Error('429 Too Many Requests');   // 1回目だけ失敗させる
      return body.map(b => feeResponse(b.FeesEstimateRequest.Identifier, { sellerId: 'S1' })[0]);
    },
  });
  assert.ok(callTimes.length >= 3, `リトライを含めて3回以上のはず (実際 ${callTimes.length})`);
  const minGap = 1000 / SP_API_FEES_RATE_PER_SEC;
  for (let i = 1; i < callTimes.length; i++) {
    const gap = callTimes[i] - callTimes[i - 1];
    assert.ok(gap >= minGap, `${i}回目の間隔 ${gap}ms が ${minGap}ms を下回っている`);
  }
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
