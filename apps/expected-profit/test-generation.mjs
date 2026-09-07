/**
 * test-generation.mjs — 世代ビルダーと公開前検証の受入試験
 *
 * Codex R3 の受入条件:
 *   - partial 時は前回の完全集合と UNION する
 *   - 引き継いだ行の期限を延長しない
 *   - 世代は検証を通ってから公開する
 *
 * 実行: node apps/expected-profit/test-generation.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-gen-'));

const { initExpectedProfitDB } = await import('./db.js');
const {
  mergeWithPreviousComplete, buildGeneration, validateGeneration, enumValidUntil,
} = await import('./build-generation.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const FUTURE = '2099-01-01T00:00:00Z';
const NOW = new Date('2026-09-07T12:00:00Z');
const NEKOPOSU = { 送料: 198, 出荷作業料: 20, 想定梱包資材費: 10, 想定人件費: 9 };

console.log('partial 時の前回集合との UNION (Codex R3)');

const snap = (key, over = {}) => ({
  shop_id: '1', mall_item_key: key, valid_until: FUTURE, fetched_at: '2026-09-01T00:00:00Z', ...over,
});

t('列挙が ok なら今夜の集合だけ使う', () => {
  const r = mergeWithPreviousComplete([snap('a')], [snap('b')], 'ok');
  assert.equal(r.rows.length, 1);
  assert.equal(r.carriedOver, 0);
});

t('[!] partial なら前回の完全集合と UNION する (消えた出品を削除扱いにしない)', () => {
  const r = mergeWithPreviousComplete([snap('a')], [snap('a'), snap('b')], 'partial');
  assert.equal(r.rows.length, 2);
  assert.equal(r.carriedOver, 1);
  assert.ok(r.rows.find(x => x.mall_item_key === 'b'));
});

t('[!] 引き継いだ行の期限を延長しない (コピー日時から延ばさない)', () => {
  const old = snap('b', { valid_until: '2026-09-02T00:00:00Z', fetched_at: '2026-08-30T00:00:00Z' });
  const r = mergeWithPreviousComplete([snap('a')], [old], 'partial');
  const carried = r.rows.find(x => x.mall_item_key === 'b');
  assert.equal(carried.valid_until, '2026-09-02T00:00:00Z');   // そのまま
  assert.equal(carried.fetched_at, '2026-08-30T00:00:00Z');
  assert.equal(carried._carried_over, 1);
});

t('今夜の行が優先される (前回の古い値で上書きしない)', () => {
  const r = mergeWithPreviousComplete(
    [snap('a', { price_incl_tax: 200 })],
    [snap('a', { price_incl_tax: 100 })], 'partial');
  assert.equal(r.rows.length, 1);
  assert.equal(r.rows[0].price_incl_tax, 200);
});

t('出品列挙の期限は run の完了時刻 + 7日', () => {
  assert.equal(enumValidUntil('2026-09-07T00:00:00Z'), '2026-09-14T00:00:00.000Z');
  assert.equal(enumValidUntil('壊れた日時'), null);
});

console.log('\n世代の組み立て');

const db = initExpectedProfitDB();

const warehouseInputs = {
  products: new Map([['ne001', {
    商品コード: 'ne001', 商品名: 'テスト商品', 原価: 600, 原価ソース: 'NE',
    原価状態: 'COMPLETE', 消費税率: 0.1, 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3,
  }]]),
  shippingRates: new Map([['501', NEKOPOSU]]),
  skuMaps: { rakuten: new Map([['sku1', [{ ne_code: 'ne001' }]]]), amazon: new Map() },
  masterFreshness: { costValidUntil: FUTURE, shippingMasterValidUntil: FUTURE },
};

function seedRun(mall, runId, enumStatus, listings) {
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, finished_at, status, listing_enum_status, expected_count)
              VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run(runId, mall, nowStamp(), '2026-09-07T00:00:00Z', enumStatus === 'ok' ? 'ok' : enumStatus, enumStatus, listings.length);
  const stmt = db.prepare(`INSERT INTO mall_price_snapshot
    (run_id, mall, shop_id, mall_item_key, mall_item_ref, fulfillment, price_incl_tax, price_tax_included,
     mall_tax_rate, postage_included, postage_revenue_incl_tax, points, listing_status,
     fetch_status, resolve_status, valid_until, source, fetched_at)
    VALUES (@run_id, @mall, @shop_id, @mall_item_key, @mall_item_ref, @fulfillment, @price_incl_tax, 1,
     @mall_tax_rate, @postage_included, @postage_revenue_incl_tax, 0, @listing_status,
     'ok', 'unresolved', @valid_until, 'test', '2026-09-07T00:00:00Z')`);
  for (const l of listings) stmt.run({ run_id: runId, mall, shop_id: '1', mall_item_ref: 'sku1',
    fulfillment: 'self', price_incl_tax: 1100, mall_tax_rate: 0.1, postage_included: 1,
    postage_revenue_incl_tax: 0, listing_status: 'active', valid_until: FUTURE, ...l });
}
let stampCounter = 0;
function nowStamp() { return new Date(Date.now() + (stampCounter++)).toISOString(); }

t('世代を作ると行と集計が入る', () => {
  seedRun('rakuten', 'run1', 'ok', [{ mall_item_key: 'item/sku1' }, { mall_item_key: 'item2/sku1' }]);
  const r = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  assert.equal(r.rowCount, 2);
  assert.equal(r.okCount, 2);
  assert.equal(r.rankEligibleCount, 2);
  assert.deepEqual(r.mallsIncluded, ['rakuten']);
  const rows = db.prepare('SELECT * FROM mart_listing_expected_profit WHERE generation_id = ?').all(r.generationId);
  assert.equal(rows.length, 2);
  assert.ok(Math.abs(rows[0].expected_profit - 81) < 1e-6);
});

t('[!] 列挙が failed のモールは世代に入れない (欠落した集合で上書きしない)', () => {
  seedRun('rakuten', 'runFail', 'failed', []);
  const r = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  assert.equal(r.rowCount, 0);
  assert.deepEqual(r.mallsIncluded, []);
  assert.equal(r.mallsDegraded[0].reason, 'enum_failed');
});

t('[!] partial のモールは前回の完全集合と UNION して世代に入る', () => {
  // run1 (ok, 2件) が完全集合。今夜は partial で1件しか取れなかった
  seedRun('rakuten', 'run3', 'partial', [{ mall_item_key: 'item/sku1' }]);
  const r = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  assert.equal(r.rowCount, 2);                         // 引き継いだ1件を含む
  assert.equal(r.perMall.rakuten.carriedOver, 1);
  assert.equal(r.mallsDegraded[0].reason, 'partial');
  const rows = db.prepare('SELECT listing_enum_status FROM mart_listing_expected_profit WHERE generation_id = ?').all(r.generationId);
  assert.ok(rows.every(x => x.listing_enum_status === 'partial'));
});

t('世代ごとに seq が増える (逆転公開の防止に使う)', () => {
  const a = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  const b = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  assert.ok(b.seq > a.seq);
});

console.log('\n公開前検証 (§10.2)');

t('正常な世代は validated になる', () => {
  seedRun('rakuten', 'runOk', 'ok', [{ mall_item_key: 'v/sku1' }]);
  const g = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  const v = validateGeneration(db, g.generationId);
  assert.equal(v.ok, true, JSON.stringify(v.errors));
  const gen = db.prepare('SELECT local_status FROM expected_profit_generation WHERE generation_id = ?').get(g.generationId);
  assert.equal(gen.local_status, 'validated');
});

t('[!] 行が0件の世代は公開しない', () => {
  seedRun('rakuten', 'runEmpty', 'failed', []);
  const g = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  const v = validateGeneration(db, g.generationId);
  assert.equal(v.ok, false);
  assert.ok(v.errors.some(e => /0件/.test(e)));
  const gen = db.prepare('SELECT local_status FROM expected_profit_generation WHERE generation_id = ?').get(g.generationId);
  assert.equal(gen.local_status, 'rejected');
});

t('[!] 費用内訳と利益が合わない世代は公開しない', () => {
  seedRun('rakuten', 'runTamper', 'ok', [{ mall_item_key: 'x/sku1' }]);
  const g = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  // 利益だけ書き換える (内訳と合わなくなる)
  db.prepare('UPDATE mart_listing_expected_profit SET expected_profit = 99999 WHERE generation_id = ?').run(g.generationId);
  const v = validateGeneration(db, g.generationId);
  assert.equal(v.ok, false);
  assert.ok(v.errors.some(e => /内訳と利益が合わない/.test(e)));
});

t('[!] 計算できた行があるのにランキング対象が0なら公開しない (判定の取り違え検知)', () => {
  seedRun('rakuten', 'runRank', 'ok', [{ mall_item_key: 'y/sku1' }]);
  const g = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  db.prepare('UPDATE expected_profit_generation SET rank_eligible_count = 0 WHERE generation_id = ?').run(g.generationId);
  const v = validateGeneration(db, g.generationId);
  assert.equal(v.ok, false);
  assert.ok(v.errors.some(e => /ランキング対象が0件/.test(e)));
});

t('[!] 利益が有限値でない行があれば公開しない', () => {
  seedRun('rakuten', 'runNaN', 'ok', [{ mall_item_key: 'z/sku1' }]);
  const g = buildGeneration(db, { warehouseInputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  db.prepare("UPDATE mart_listing_expected_profit SET expected_profit = 'NaN' WHERE generation_id = ?").run(g.generationId);
  const v = validateGeneration(db, g.generationId);
  assert.equal(v.ok, false);
});

t('入力不足による incomplete は「正常」として公開できる (構造異常と分ける)', () => {
  // 原価が無い商品 → incomplete になるが、世代としては公開できる
  const inputs = { ...warehouseInputs, products: new Map([['ne001', {
    商品コード: 'ne001', 商品名: 'x', 原価: 600, 原価ソース: 'NE', 原価状態: 'COMPLETE',
    消費税率: 0.1, 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3,
  }]]) };
  seedRun('rakuten', 'runMix', 'ok', [
    { mall_item_key: 'ok/sku1' },
    { mall_item_key: 'ng/unknown', mall_item_ref: 'unknownSku' },   // 対応表に無い
  ]);
  const g = buildGeneration(db, { warehouseInputs: inputs, now: NOW, malls: ['rakuten'], codeVersion: 'test' });
  const v = validateGeneration(db, g.generationId);
  assert.equal(v.ok, true, JSON.stringify(v.errors));
  assert.equal(g.okCount, 1);
  assert.equal(g.rowCount, 2);
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
