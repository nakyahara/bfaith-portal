/**
 * test-nightly.mjs — 夜間ジョブの通し 受入試験 (§8 / §10.1 障害)
 *
 * API も HTTP も叩かない。deps 差し替えで一本通す。
 * 実行: node apps/expected-profit/test-nightly.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ep-n-'));
process.env.SP_API_MARKETPLACE_ID = 'A1VC38T7YXB528';
process.env.SP_API_SELLER_ID = 'S1';

const { initExpectedProfitDB } = await import('./db.js');
const { runNightly, deadlineOf, feeTargetsFrom } = await import('./nightly.js');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

console.log('全体終了期限 (§8.4)');

t('[!] 23:30 に始まったら期限は翌日の 06:00 JST', () => {
  const start = new Date('2026-09-07T14:30:00Z');       // JST 23:30
  const d = deadlineOf(start);
  assert.equal(d.toISOString(), '2026-09-07T21:00:00.000Z');  // JST 翌06:00
  assert.ok(d > start);
});

t('期限は必ず未来になる', () => {
  const d = deadlineOf(new Date('2026-09-07T22:00:00Z'));
  assert.ok(d > new Date('2026-09-07T22:00:00Z'));
});

console.log('\n手数料の見積対象');

t('[!] Amazon の FBA/FBM だけを対象にする', () => {
  const rows = [
    { mall: 'amazon', fetch_status: 'ok', fulfillment: 'FBA', mall_item_key: 'a', mall_item_ref: 'B1', price_incl_tax: 1000, points: 0, postage_revenue_incl_tax: 0 },
    { mall: 'amazon', fetch_status: 'ok', fulfillment: null, mall_item_key: 'b', mall_item_ref: 'B2', price_incl_tax: 1000, points: 0 },
    { mall: 'rakuten', fetch_status: 'ok', fulfillment: 'self', mall_item_key: 'c', price_incl_tax: 1000, points: 0 },
  ];
  const targets = feeTargetsFrom(rows, { sellerId: 'S1', marketplaceId: 'M1' });
  assert.equal(targets.length, 1);
  assert.equal(targets[0].seller_sku, 'a');
});

t('[!] 送料を算定基礎に渡す (送料込みなら0)', () => {
  const rows = [
    { mall: 'amazon', fetch_status: 'ok', fulfillment: 'FBM', mall_item_key: 'a', mall_item_ref: 'B1', price_incl_tax: 1198, points: 0, postage_revenue_incl_tax: 230 },
  ];
  const targets = feeTargetsFrom(rows, { sellerId: 'S1', marketplaceId: 'M1' });
  assert.equal(targets[0].in_shipping, 230);
});

t('価格が取れなかった行は対象にしない', () => {
  const rows = [{ mall: 'amazon', fetch_status: 'not_found', fulfillment: 'FBA', mall_item_key: 'a', price_incl_tax: null, points: 0 }];
  assert.equal(feeTargetsFrom(rows, { sellerId: 'S1', marketplaceId: 'M1' }).length, 0);
});

console.log('\n通し (deps 差し替え)');

const db = initExpectedProfitDB();

const warehouseDb = {
  prepare(sql) {
    return {
      all: () => {
        if (sql.includes('m_products')) return [{
          商品コード: 'ne001', 商品名: 'テスト商品', 原価: 600, 原価ソース: 'NE', 原価状態: 'COMPLETE',
          消費税率: 0.1, 税区分: 'STANDARD_10', 送料コード: '501', 配送方法: 'ネコポス', 売上分類: 3, 取扱区分: '取扱中',
        }];
        if (sql.includes('shipping_rates')) return [{
          shipping_code: '501', 大分類区分: 'ネコポス', 小分類区分名称: 'ネコポス',
          送料: 198, 出荷作業料: 20, 想定梱包資材費: 10, 想定人件費: 9, 配送関係費合計: 237,
        }];
        if (sql.includes('v_sku_resolved')) return [{ seller_sku: 'sku1', ne_code: 'ne001' }];
        if (sql.includes('f_rakuten_sku_map')) return [{ rakuten_code: 'sku1', ne_code: 'ne001' }];
        return [];
      },
      get: () => ({ v: '2026-09-07 00:00:00' }),
    };
  },
  pragma() {},
  close() {},
};

const rakutenPage = async () => ({
  results: [{ item: { manageNumber: 'item1', variants: {
    sku1: { standardPrice: '1100', payment: { taxIncluded: true, taxRate: '0.1' }, shipping: { postageIncluded: true }, merchantDefinedSkuId: 'sku1' },
  } } }],
  nextCursorMark: null,
});

await ta('[!] 一本通ると世代ができて公開される', async () => {
  const published = [];
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'),
    malls: ['rakuten'], skipFees: true,
    fetchDeps: { rakuten: { searchPage: rakutenPage } },
    publishDeps: {
      postChunk: async () => ({ ok: true }),
      postPublish: async (b) => { published.push(b); return { ok: true }; },
      getPublished: async () => ({ generation_id: published[0]?.generation_id, seq: published[0]?.seq }),
    },
    log: () => {},
  });
  assert.equal(r.ok, true, r.error);
  assert.ok(r.generationId);
  const gen = db.prepare('SELECT * FROM expected_profit_generation WHERE generation_id = ?').get(r.generationId);
  assert.equal(gen.remote_status, 'published');
  assert.equal(gen.local_status, 'validated');
});

await ta('[!] 1モールの取得が失敗しても、他モールで世代を作る (fail-soft)', async () => {
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'),
    malls: ['amazon', 'rakuten'], skipFees: true,
    fetchDeps: {
      amazon: { getActiveListingsReport: async () => { throw new Error('SP-API 500'); } },
      rakuten: { searchPage: rakutenPage },
    },
    publishDeps: {
      postChunk: async () => ({ ok: true }),
      postPublish: async () => ({ ok: true }),
      getPublished: async () => null,   // 確認できない
    },
    log: () => {},
  });
  const amazonStep = r.steps.find(s => s.step === 'fetch:amazon');
  const buildStep = r.steps.find(s => s.step === 'build');
  assert.equal(amazonStep.ok, false);          // Amazon は失敗
  assert.equal(buildStep.ok, true);            // でも世代は作る
  assert.ok(buildStep.rowCount > 0);
});

await ta('[!] 読み戻しで確認できなければ ok にしない', async () => {
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'),
    malls: ['rakuten'], skipFees: true,
    fetchDeps: { rakuten: { searchPage: rakutenPage } },
    publishDeps: {
      postChunk: async () => ({ ok: true }),
      postPublish: async () => ({ ok: true }),
      getPublished: async () => ({ generation_id: '別の世代', seq: 9999 }),
    },
    log: () => {},
  });
  assert.equal(r.ok, undefined ?? false);
  assert.equal(r.error, 'publish_not_confirmed');
});

await ta('[!] 検証で拒否された世代は転送しない', async () => {
  let sent = false;
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'),
    malls: ['rakuten'], skipFees: true,
    // 列挙が空 → 世代0行 → 検証で拒否
    fetchDeps: { rakuten: { searchPage: async () => ({ results: [], nextCursorMark: null }) } },
    publishDeps: {
      postChunk: async () => { sent = true; return { ok: true }; },
      postPublish: async () => ({ ok: true }),
      getPublished: async () => null,
    },
    log: () => {},
  });
  assert.equal(sent, false);
  assert.equal(r.error, 'validation_failed');
});

await ta('--skip-publish なら世代だけ作って転送しない', async () => {
  let sent = false;
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'),
    malls: ['rakuten'], skipFees: true, skipPublish: true,
    fetchDeps: { rakuten: { searchPage: rakutenPage } },
    publishDeps: { postChunk: async () => { sent = true; return { ok: true }; } },
    log: () => {},
  });
  assert.equal(r.ok, true);
  assert.equal(r.skippedPublish, true);
  assert.equal(sent, false);
});

await ta('[!] 期限を過ぎたら新しい取得を始めない (全工程に伝播している)', async () => {
  let fetched = false;
  const r = await runNightly({
    db, warehouseDb, now: new Date('2026-09-07T15:00:00Z'),
    deadline: new Date('2000-01-01T00:00:00Z'),   // 既に過ぎている
    malls: ['rakuten'], skipFees: true,
    fetchDeps: { rakuten: { searchPage: async () => { fetched = true; return { results: [], nextCursorMark: null }; } } },
    publishDeps: { postChunk: async () => ({ ok: true }), postPublish: async () => ({ ok: true }), getPublished: async () => null },
    log: () => {},
  });
  assert.equal(fetched, false, '期限後なのに取得を始めた');
  assert.equal(r.error, 'deadline_exceeded');
  const step = r.steps.find(s => s.step === 'fetch:rakuten');
  assert.equal(step.error, 'deadline_exceeded');
});

db.close();
fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });
console.log(`\n${passed} 件 PASS`);
