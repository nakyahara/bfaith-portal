#!/usr/bin/env node
/**
 * test-amazon-fees-outcome.mjs — Amazon手数料の取得の「終了コードと最後の 1 行」の試験。
 *   1 SKU の ClientError でステップ全体を落とさない・でも黙って緑にもしない / ClientError というだけでは見逃さない (**同じ要求 (batch) の中で** ほかが取れている・上限以内) /
 *   応答は要求と 1 対 1 で突き合わせる (返ってこなかった SKU を失敗に数える) / 1 件だけの batch を作らない /
 *   通信・サーバ側の失敗は今までどおり落とす / 警告つきの成功を、再試行の通知と朝の未達の検知から消さない
 * 取得の本体 (fetchAmazonFees / runFeesCli) を、DB = メモリ上の SQLite・API = 差し替えた関数 で実際に回す。
 * 🚨 試験に無いもの: SP-API への本物の要求 (fetchFeesBatch)・daily-sync.js の実行 (ソースの形だけ見る)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { summarizeFeeOutcome, splitErrors, scopeBatchErrors, isWarnSummary, INPUT_FAIL_MIN_LIMIT, NO_ASIN_LIMIT, SYSTEMIC_CODES } from '../apps/warehouse/amazon-fees-outcome.js';
import { fetchAmazonFees, runFeesCli, parseFeesResponse, makeBatches } from '../apps/warehouse/fetch-amazon-fees.js';
import { warnLines } from '../apps/warehouse/retry-failed-jobs.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

// ─── メモリ上の warehouse.db (fetch-amazon-fees.js が読む・書く表だけ。列は本番と同じ) ───
function openDb() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_sp_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, amazon_order_id TEXT, purchase_date TEXT, order_status TEXT, fulfillment_channel TEXT, asin TEXT, seller_sku TEXT, quantity INTEGER, item_price REAL);
    CREATE TABLE amazon_sku_fees (seller_sku TEXT PRIMARY KEY, asin TEXT, fulfillment_channel TEXT, referral_fee REAL, referral_fee_rate REAL, fba_fee REAL, variable_closing_fee REAL, per_item_fee REAL, total_fee REAL, price_used REAL, fetched_at TEXT);
    CREATE TABLE m_sku_master (seller_sku TEXT PRIMARY KEY, 商品名 TEXT)`);
  return db;
}
const today = new Date().toISOString().slice(0, 10);
const sold = (db, sku, { asin = `B0${sku}`, price = 1000 } = {}) => db.prepare(`insert into raw_sp_orders (amazon_order_id, purchase_date, order_status, fulfillment_channel, asin, seller_sku, quantity, item_price) values (?, ?, 'Shipped', 'Amazon', ?, ?, 1, ?)`)
  .run(`o-${sku}`, `${today}T10:00:00+09:00`, asin, sku, price);
const cachedFresh = (db, sku) => db.prepare(`insert into amazon_sku_fees (seller_sku, asin, fulfillment_channel, referral_fee, total_fee, price_used, fetched_at) values (?, ?, 'FBA', 100, 400, 1000, ?)`).run(sku, `B0${sku}`, new Date().toISOString().replace('T', ' ').slice(0, 19));
/** SP-API の代わり。plan[sku] = 'ClientError' | 'ServiceError' | { error, code } | 'throw'。無ければ成功。呼ばれた batch を記録する */
function fakeApi(plan = {}) {
  const calls = [];
  const fn = async (items) => {
    calls.push(items.map((x) => x.seller_sku));
    if (items.some((x) => plan[x.seller_sku] === 'throw')) throw new Error('fetch failed (network)');
    const results = [], errors = [];
    for (const it of items) {
      const p = plan[it.seller_sku];
      if (!p) { results.push({ seller_sku: it.seller_sku, asin: it.asin, channel: it.channel, referralFee: 100, referralFeeRate: 0.1, fbaFee: 300, variableClosingFee: 0, perItemFee: 0, totalFee: 400, price_used: it.last_price, refresh_reason: it.refresh_reason }); continue; }
      const e = typeof p === 'string' ? { error: p, code: p === 'ClientError' ? 'InvalidParameterValue' : 'InternalFailure' } : p;
      errors.push({ sku: it.seller_sku, error: e.error, code: e.code ?? null, type: 'Sender', errorMsg: 'There is an client-side error. Please verify your inputs.', detail: null });
    }
    return { results, errors };
  };
  fn.calls = calls;
  return fn;
}
const run = (db, api, mode = 'recent', param = 30) => fetchAmazonFees(mode, param, { db, fetchBatch: api, sleepFn: async () => {} });
const skus = (n, prefix = 's') => Array.from({ length: n }, (_, i) => `${prefix}${String(i).padStart(4, '0')}`);

console.log('Amazon手数料: 取得の本体を回す (DB = メモリ・API = 差し替え)');
await t('🚨 9/18〜19 の形 (朝の回): 25 SKU を取り直して 1 SKU だけ ClientError → 落とさない (exit 0)。最後の行は ⚠️ + SKU + Amazon のエラーコード + 何を確かめるか。取れなかった SKU を 0 円で書かない', async () => {
  const db = openDb(); for (const s of skus(25)) sold(db, s);
  const r = await run(db, fakeApi({ s0007: 'ClientError' }));
  assert.deepEqual([r.refreshed, r.failed, r.input_failed, r.hard_failed, r.outcome.exitCode, r.outcome.level], [24, 1, 1, 0, 0, 'warn']);
  assert.match(r.outcome.line, /^⚠️ Amazon手数料: 取り直し 24 \/ 期限内で見送り 0 \/ 手数料を取り直せていない: Amazon が ClientError を返す SKU 1 件 \(s0007 \(ClientError: InvalidParameterValue\)\)/);
  assert.match(r.outcome.line, /Amazon で確かめる/);
  assert.deepEqual([db.prepare(`select count(*) n from amazon_sku_fees`).get().n, db.prepare(`select count(*) n from amazon_sku_fees where seller_sku = 's0007'`).get().n], [24, 0]);
  assert.deepEqual([r.errors[0].scope, r.errors[0].batch, r.errors[0].code], ['sku', 1, 'InvalidParameterValue']);
});
await t('🚨 ClientError というだけでは見逃さない ①: その回で 1 件も取れていない (取り直す物がその SKU だけ = 自動再試行の回の形) → 落とす (成功ゼロの要求 = API・設定が動いている証拠が無い)', async () => {
  const db = openDb(); for (const s of skus(30)) { sold(db, s); if (s !== 's0007') cachedFresh(db, s); }
  const r = await run(db, fakeApi({ s0007: 'ClientError' }));
  assert.deepEqual([r.refreshed, r.skipped, r.outcome.exitCode, r.hard_failed, r.input_failed, r.errors[0].scope], [0, 29, 1, 1, 0, 'batch']);
  assert.match(r.outcome.line, /^❌ Amazon手数料: .*取得に失敗 1 件/);
  // 判定の関数そのものの歯止め (scope = sku なのに 1 件も取れていない、は本体からは来ない形。来ても落とす)
  const o = summarizeFeeOutcome({ refreshed: 0, skipped: 29, inputErrors: [{ sku: 's0007', error: 'ClientError', scope: 'sku' }] });
  assert.deepEqual([o.exitCode, /この回は 1 件も取れていない/.test(o.line)], [1, true]);
});
await t('🚨 ClientError というだけでは見逃さない ②: batch が 2 件以上あって全部 ClientError → batch の側の問題として落とす (5 件全部 ClientError / 380 件成功 + 別の 20 件の batch が全滅。Codex R1 #1 の再現)', async () => {
  const db1 = openDb(); for (const s of skus(5)) sold(db1, s);
  const r1 = await run(db1, fakeApi(Object.fromEntries(skus(5).map((s) => [s, 'ClientError']))));
  assert.deepEqual([r1.outcome.exitCode, r1.hard_failed, r1.input_failed, r1.errors.every((e) => e.scope === 'batch')], [1, 5, 0, true]);
  const db2 = openDb(); const all = skus(400); for (const s of all) sold(db2, s);
  const api = fakeApi(Object.fromEntries(all.slice(380).map((s) => [s, 'ClientError'])));   // order by の並びで最後の batch がちょうど 20 件
  const r2 = await run(db2, api);
  const lastBatch = api.calls[api.calls.length - 1];
  assert.equal(lastBatch.every((s) => all.slice(380).includes(s)), true, '前提: 失敗する 20 件が同じ batch に入っていない');
  assert.deepEqual([r2.refreshed, r2.outcome.exitCode, r2.hard_failed], [380, 1, 20]);
  assert.match(r2.outcome.line, /^❌ Amazon手数料: .*取得に失敗 20 件/);
});
await t('🚨 通信・サーバ側の失敗は 1 件でも今までどおり落とす: ServiceError (SP-API の Status は Success / ClientError / ServiceError) / batch ごとの例外 (3 回やり直して駄目) / 知らない Status', async () => {
  for (const plan of [{ s0003: 'ServiceError' }, { s0003: 'throw' }, { s0003: { error: 'SomethingNew', code: 'X' } }]) {
    const db = openDb(); for (const s of skus(25)) sold(db, s);
    const r = await run(db, fakeApi(plan));
    assert.deepEqual([r.outcome.exitCode, r.outcome.level, r.hard_failed > 0], [1, 'fail', true], JSON.stringify(plan));
    assert.match(r.outcome.line, /^❌ Amazon手数料: .*取得に失敗/);
  }
});
await t('🚨 判定は「先頭 50 件に切る前の全部の失敗」で: 1,200 SKU・ClientError 55 件 (上限 60 の中) の後ろに ServiceError が 1 件 → 落とす。結果に載せる errors は 50 件のまま', async () => {
  const db = openDb(); const all = skus(1200); for (const s of all) sold(db, s);
  const plan = {}; for (let i = 0; i < 55; i++) plan[all[i * 20]] = 'ClientError';   // batch ごとに 1 件ずつ
  plan[all[1199]] = 'ServiceError';
  const r = await run(db, fakeApi(plan));
  assert.deepEqual([r.errors.length, r.input_failed, r.hard_failed, r.outcome.exitCode], [50, 55, 1, 1]);
  assert.equal(r.errors.some((e) => e.error === 'ServiceError'), false, '前提: ServiceError が先頭 50 件の外にいない');
  delete plan[all[1199]];
  const db2 = openDb(); for (const s of all) sold(db2, s);
  assert.equal((await run(db2, fakeApi(plan))).outcome.exitCode, 0, 'ServiceError が無ければ 55 / 1,200 は上限 60 の中');
});
await t('上限 = 5 件 と「API に送った数の 5%」の大きいほうを **超えたら** 落とす (5% は切り上げない: 101 件中 6 件 = 5.94% は落とす。Codex R1 #5)', async () => {
  const mk = async (n, bad) => { const db = openDb(); const all = skus(n); for (const s of all) sold(db, s); const plan = {}; for (let i = 0; i < bad; i++) plan[all[i * 20 < n ? i * 20 : i]] = 'ClientError'; return run(db, fakeApi(plan)); };
  assert.equal((await mk(101, 5)).outcome.exitCode, 0);
  const six = await mk(121, 6);   // 6 batch に 1 件ずつ。121 × 5% = 6.05 → 6 は中
  assert.deepEqual([six.input_failed, six.outcome.exitCode], [6, 0]);
  const o = summarizeFeeOutcome({ refreshed: 95, skipped: 0, inputErrors: skus(6).map((s) => ({ sku: s, error: 'ClientError', scope: 'sku' })) });
  assert.deepEqual([o.exitCode, o.attempted], [1, 101], '101 件中 6 件 (5.94%) が通った');
  assert.equal(summarizeFeeOutcome({ refreshed: 380, inputErrors: skus(20).map((s) => ({ sku: s, error: 'ClientError', scope: 'sku' })) }).exitCode, 0, '400 件中 20 件 = 5% ちょうどは中');
  assert.equal(summarizeFeeOutcome({ refreshed: 379, inputErrors: skus(21).map((s) => ({ sku: s, error: 'ClientError', scope: 'sku' })) }).exitCode, 1);
  assert.equal(INPUT_FAIL_MIN_LIMIT, 5);
});
await t('ASIN の分からない SKU は別に数える (API に送っていない = 5% の分母に入れない): 5 件までは ⚠️ (「SKU と ASIN の対応を確かめる」)・6 件で落とす。全部が ASIN なし・取り直す物が無い ときも最後の行が出る', async () => {
  const mk = async (nNoAsin, nOk) => { const db = openDb(); for (const s of skus(nOk, 'ok')) sold(db, s); for (const s of skus(nNoAsin, 'na')) sold(db, s, { asin: null }); return run(db, fakeApi()); };
  const five = await mk(NO_ASIN_LIMIT, 10);
  assert.deepEqual([five.no_asin, five.outcome.exitCode, five.outcome.level, five.outcome.attempted], [5, 0, 'warn', 10]);
  assert.match(five.outcome.line, /ASIN の分からない SKU 5 件 .* → SKU と ASIN の対応を確かめる/);
  assert.equal((await mk(NO_ASIN_LIMIT + 1, 10)).outcome.exitCode, 1);
  const onlyNoAsin = await mk(2, 0);   // ASIN 付きが 1 件も無い = 早い return
  assert.deepEqual([onlyNoAsin.outcome.exitCode, onlyNoAsin.outcome.level, onlyNoAsin.errors.length], [0, 'warn', 2]);
  assert.deepEqual([/ほかの SKU は取れている/.test(five.outcome.line), /ほかの SKU は取れている/.test(onlyNoAsin.outcome.line)], [true, false], '取れた SKU が無い回に「取れている」と言っている (Codex R2 #4)');
  const db = openDb(); for (const s of skus(3)) { sold(db, s); cachedFresh(db, s); }   // 取り直す物が無い = もう 1 つの早い return
  const none = await run(db, fakeApi());
  assert.deepEqual([none.outcome.exitCode, none.outcome.line], [0, '✅ Amazon手数料: 取り直し 0 / 期限内で見送り 3']);
});
await t('🚨 成功ゼロの batch は、別の batch がどれだけ取れていても落とす (Codex R2 #1): 399 件成功 + 最後の 2 件の batch が両方 ClientError → exit 1。逆に 401 件で最後の 1 件だけ ClientError → 1 件だけの batch を作らない (19 + 2) ので、同じ要求の相方が取れていて exit 0', async () => {
  const all = skus(401);
  const db1 = openDb(); for (const s of all) sold(db1, s);
  const api1 = fakeApi({ [all[400]]: 'ClientError' });
  const r1 = await run(db1, api1);
  assert.equal(api1.calls.some((c) => c.length === 1), false, '1 件だけの batch を作っている');
  assert.deepEqual([api1.calls.length, api1.calls.at(-2).length, api1.calls.at(-1)], [21, 19, [all[399], all[400]]]);
  assert.deepEqual([r1.refreshed, r1.input_failed, r1.hard_failed, r1.outcome.exitCode, r1.outcome.level], [400, 1, 0, 0, 'warn']);
  const db2 = openDb(); for (const s of all) sold(db2, s);
  const r2 = await run(db2, fakeApi({ [all[399]]: 'ClientError', [all[400]]: 'ClientError' }));
  assert.deepEqual([r2.refreshed, r2.input_failed, r2.hard_failed, r2.outcome.exitCode], [399, 0, 2, 1]);
  // 仕組みの側のコードは、同じ要求でほかが取れていても落とす
  const db3 = openDb(); for (const s of skus(25)) sold(db3, s);
  const r3 = await run(db3, fakeApi({ s0007: { error: 'ClientError', code: 'QuotaExceeded' } }));
  assert.deepEqual([r3.refreshed, r3.hard_failed, r3.outcome.exitCode], [24, 1, 1]);
});
await t('makeBatches: 1 件だけの batch を作らない (端数 1 → 19 + 2)・並びと総数は変えない・元の配列を壊さない。全部で 1 件のときはそのまま (= その回は落ちる。証拠が無い)', async () => {
  const sizes = (n) => makeBatches(skus(n)).map((b) => b.length);
  assert.deepEqual([sizes(0), sizes(1), sizes(2), sizes(20), sizes(21), sizes(22), sizes(40), sizes(41)], [[], [1], [2], [20], [19, 2], [20, 2], [20, 20], [20, 19, 2]]);
  const src = skus(41); const copy = [...src];
  assert.deepEqual(makeBatches(src).flat(), copy);
  assert.deepEqual(src, copy);
  assert.deepEqual(makeBatches(skus(7), 3).map((b) => b.length), [3, 2, 2]);
});
await t('🚨 応答は要求と 1 対 1 で突き合わせる (parseFeesResponse。Codex R2 #2): 返ってこなかった SKU = NoResponse・同じ Identifier の 2 つ目 = DuplicateResponse・知らない Identifier・配列でない応答 → どれも失敗に数える', async () => {
  const item = (sku) => ({ seller_sku: sku, asin: 'B0' + sku, channel: 'FBA', last_price: 1000, refresh_reason: 'new' });
  const reqMap = new Map(['a', 'b', 'c'].map((k, i) => [k + '|' + i, item(k)]));
  const okRes = (id) => ({ Status: 'Success', FeesEstimateIdentifier: { SellerInputIdentifier: id }, FeesEstimate: { TotalFeesEstimate: { Amount: 400 }, FeeDetailList: [{ FeeType: 'ReferralFee', FeeAmount: { Amount: 100 } }, { FeeType: 'FBAFees', FeeAmount: { Amount: 300 } }, { FeeType: 'VariableClosingFee', FeeAmount: { Amount: 7 } }, { FeeType: 'PerItemFee', FeeAmount: { Amount: 3 } }] } });
  const ngRes = (id) => ({ Status: 'ClientError', FeesEstimateIdentifier: { SellerInputIdentifier: id }, Error: { Type: 'Sender', Code: 'InvalidParameterValue', Message: 'verify your inputs', Detail: [] } });
  const full = parseFeesResponse(reqMap, [okRes('a|0'), ngRes('b|1'), okRes('c|2')]);
  assert.deepEqual([full.results.map((r) => [r.seller_sku, r.referralFee, r.fbaFee, r.totalFee, r.referralFeeRate]), full.errors.map((e) => [e.sku, e.error, e.code])], [[['a', 100, 300, 400, 0.1], ['c', 100, 300, 400, 0.1]], [['b', 'ClientError', 'InvalidParameterValue']]]);
  assert.deepEqual([full.results[0].variableClosingFee, full.results[0].perItemFee, full.results[0].price_used, full.results[0].channel], [7, 3, 1000, 'FBA'], '手数料の列の取り違え (Codex R3 Low)');
  const missing = parseFeesResponse(reqMap, [ngRes('b|1')]);
  assert.deepEqual([missing.results.length, missing.errors.map((e) => [e.sku, e.error])], [0, [['b', 'ClientError'], ['a', 'NoResponse'], ['c', 'NoResponse']]]);
  const dup = parseFeesResponse(reqMap, [okRes('a|0'), okRes('a|0'), okRes('b|1'), okRes('c|2')]);
  assert.deepEqual([dup.results.length, dup.errors.map((e) => [e.sku, e.error])], [3, [['a', 'DuplicateResponse']]]);
  const stray = parseFeesResponse(reqMap, [okRes('a|0'), okRes('b|1'), okRes('c|2'), okRes('zzz')]);
  assert.deepEqual([stray.results.length, stray.errors.map((e) => e.error)], [3, ['Cannot match identifier to original SKU']]);
  assert.deepEqual(parseFeesResponse(reqMap, { errors: [] }).errors.map((e) => e.error), ['Response is not an array']);
  assert.deepEqual(parseFeesResponse(reqMap, [{ Status: 'Success', FeesEstimateIdentifier: { SellerInputIdentifier: 'a|0' } }, okRes('b|1'), okRes('c|2')]).errors.map((e) => [e.sku, e.error]), [['a', 'NoEstimate']]);
});
await t('🚨 R2 #2 の再現を本体で: 40 件を取り直す → 2 つ目の batch の応答が「ClientError 1 件だけ」(19 件は返ってこない) → exit 1・19 件が失敗数と errors に出る。「成功 1 + ClientError 1 + 18 件返ってこない」でも落とす', async () => {
  const all = skus(40);
  const partialApi = (keepOk) => {
    const base = fakeApi();
    const fn = async (items) => {
      if (!items.some((x) => x.seller_sku === all[25])) return base(items);
      const reqMap = new Map(items.map((it, i) => [it.seller_sku + '|' + i, it]));
      const idOf = (sku) => [...reqMap].find(([, it]) => it.seller_sku === sku)[0];
      const res = [{ Status: 'ClientError', FeesEstimateIdentifier: { SellerInputIdentifier: idOf(all[25]) }, Error: { Code: 'InvalidParameterValue', Type: 'Sender', Message: 'verify your inputs' } }];
      if (keepOk) res.push({ Status: 'Success', FeesEstimateIdentifier: { SellerInputIdentifier: idOf(all[26]) }, FeesEstimate: { TotalFeesEstimate: { Amount: 400 }, FeeDetailList: [] } });
      return parseFeesResponse(reqMap, res);
    };
    return fn;
  };
  const db1 = openDb(); for (const s of all) sold(db1, s);
  const r1 = await run(db1, partialApi(false));
  assert.deepEqual([r1.refreshed, r1.failed, r1.input_failed, r1.hard_failed, r1.outcome.exitCode], [20, 20, 0, 20, 1]);
  assert.equal(r1.errors.filter((e) => e.error === 'NoResponse').length, 19);
  assert.match(r1.outcome.line, /^❌ Amazon手数料: 取り直し 20 .*取得に失敗 20 件/);
  const db2 = openDb(); for (const s of all) sold(db2, s);
  const r2 = await run(db2, partialApi(true));
  assert.deepEqual([r2.refreshed, r2.input_failed, r2.hard_failed, r2.outcome.exitCode], [21, 1, 18, 1]);
});
await t('CLI (runFeesCli): 終了コードは判定どおり・**最後に出す行は判定の 1 行** (daily-sync は最後の行を朝の通知に載せる)。--sku に値が無ければ ❌', async () => {
  const db = openDb(); for (const s of skus(25)) sold(db, s);
  const lines = [];
  const r = await runFeesCli(['--recent', '30'], { init: false, db, fetchBatch: fakeApi({ s0001: 'ClientError' }), sleepFn: async () => {}, print: (m) => lines.push(String(m)) });
  assert.equal(r.exitCode, 0);
  assert.equal(lines[lines.length - 1].trim(), r.lastLine);
  assert.match(r.lastLine, /^⚠️ Amazon手数料: /);
  assert.ok(lines.some((l) => l.includes('s0001: ClientError [InvalidParameterValue]')), 'エラーの詳細に Amazon のコードが出ていない');
  const db2 = openDb(); for (const s of skus(25)) sold(db2, s);
  const lines2 = [];
  const f = await runFeesCli(['--recent', '30'], { init: false, db: db2, fetchBatch: fakeApi({ s0001: 'ServiceError' }), sleepFn: async () => {}, print: (m) => lines2.push(String(m)) });
  assert.deepEqual([f.exitCode, lines2[lines2.length - 1].trim().startsWith('❌ Amazon手数料: ')], [1, true]);
  assert.equal((await runFeesCli(['--sku'], { init: false, print: () => {} })).exitCode, 1);
});

console.log('Amazon手数料: 判定の部品と、通知の側');
await t('分類: SKU の ClientError と見てよいのは scope = sku だけ。SKU の無い失敗・知らない種類・継承プロパティの名前・scope の無い ClientError は「それ以外」(= 落とす側)', async () => {
  assert.deepEqual(scopeBatchErrors(20, 19, [{ sku: 'a', error: 'ClientError' }]).map((e) => e.scope), ['sku']);
  assert.deepEqual(scopeBatchErrors(1, 0, [{ sku: 'a', error: 'ClientError' }]).map((e) => e.scope), ['batch'], '成功が 1 件も無い要求は、1 件だけでも batch の側 (別の batch の成功は証拠にならない。Codex R2 #1)');
  assert.deepEqual(scopeBatchErrors(20, 19, [{ sku: 'a', error: 'ClientError', code: 'QuotaExceeded' }]).map((e) => e.scope), ['batch'], '仕組みの側のコードは、ほかが取れていても落とす');
  assert.deepEqual(scopeBatchErrors(20, 19, [{ sku: 'a', error: 'ClientError', code: 'InvalidParameterValue' }, { sku: 'b', error: 'ClientError', code: null }]).map((e) => e.scope), ['sku', 'sku']);
  assert.equal([...SYSTEMIC_CODES].every((c) => c === c.toLowerCase()), true, 'SYSTEMIC_CODES は小文字で持つ (比較は toLowerCase)');
  assert.deepEqual(scopeBatchErrors(2, 0, [{ sku: 'a', error: 'ClientError' }, { sku: 'b', error: 'ClientError' }]).map((e) => e.scope), ['batch', 'batch']);
  assert.deepEqual(scopeBatchErrors(3, 1, [{ sku: 'a', error: 'ClientError' }, { sku: 'b', error: 'ServiceError' }, { error: 'ClientError' }]).map((e) => e.scope), ['sku', 'batch', 'batch']);
  const s = splitErrors([{ sku: 'a', error: 'ClientError', scope: 'sku' }, { sku: 'b', error: 'ClientError' }, { sku: 'c', error: 'No ASIN' }, { sku: 'd', error: 'toString', scope: 'sku' }, { identifier: 'all', error: 'Response is not an array' }, null]);
  assert.deepEqual([s.input.map((e) => e.sku), s.noAsin.map((e) => e.sku), s.hard.length], [['a'], ['c'], 4]);
  assert.deepEqual(splitErrors(undefined), { input: [], noAsin: [], hard: [] });
});
await t('🚨 警告つきの成功を通知から消さない: 再試行の通知に ⚠️ の要約を足す (復旧の通知はジョブ名しか載せない。Codex R1 #2) / 朝の回は warn を「全部 OK」に数えない (通知が落ちた朝に未達の検知を外さない。R1 #3)', async () => {
  const warn = '⚠️ Amazon手数料: 取り直し 24 / 期限内で見送り 0 / 手数料を取り直せていない: …';
  assert.deepEqual([isWarnSummary(warn), isWarnSummary('  ' + warn), isWarnSummary('✅ Amazon手数料: 取り直し 24'), isWarnSummary('取り直し ⚠️'), isWarnSummary(null)], [true, true, false, false, false]);
  assert.deepEqual(warnLines([{ name: 'Amazon手数料', success: true, summary: warn }, { name: 'f_sales', success: true, summary: 'ok' }, { name: 'Render同期', success: false, summary: '⚠️ x' }]), [`Amazon手数料: ${warn}`]);
  const retry = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'retry-failed-jobs.js'), 'utf8');
  assert.equal((retry.match(/for \(const w of warnLines\(results\)\) msg \+= /g) || []).length, 3, '成功・部分復旧・最終失敗 の 3 つの通知に足していない');
  const daily = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'daily-sync.js'), 'utf8');
  assert.match(daily, /results\.push\(\{ name: 'Amazon手数料', \.\.\.feeResult, warn: feeResult\.success && isWarnSummary\(feeResult\.summary\) \}\)/);
  assert.match(daily, /const allOk = results\.every\(r => r\.success && r\.warn !== true\)/);
  // lock は「最後まで走ったが通知だけ落ちた朝」にも残る → 翌朝の文言は途中終了と断定しない (Codex R2 #3)
  assert.match(daily, /前回の daily-sync の完了通知を確認できない/);
  assert.equal(daily.includes('当該朝のジョブは途中までしか実行されていない'), false);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
