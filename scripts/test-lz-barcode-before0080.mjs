/**
 * test-lz-barcode-before0080.mjs — 当てる順 (#1719 Codex R4 Medium) = アプリ (新しい画面) → 0080 → ロールの流し直し。途中のどの組み合わせでも新商品の登録が続く
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs を 0079 まで)。本物の router を HTTP 越しに通す。
 *   A 新しいアプリ + 0080 の前の DB: 欄を出さない (単品・まとまり・商品の画面)・値なしで単品 / まとまりを登録できる・商品の画面の保存 = 503 (何も書かない)
 *   B 0080 を当てた後・ロールの流し直しの前 (master_edit に書く関数の実行権が無い): A と同じ (欄を出さない・値なしで登録できる)
 *   C ロールの流し直しの後: 欄を出す・値か空欄が要る (無い = 400 lzbc_missing)・値で登録できる
 * 使い方: node scripts/test-lz-barcode-before0080.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbarcode2-b80-'));

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const T = await setupVariationDb({ migrateTo: '0079' });
const { pg, db, q, one } = T;
const { applyMigrations } = await import('./company-db/migrate.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/lzb80';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/lzb80';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const MR = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
MR.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
MR.__setShippingRatesProvider(async () => new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]));
MR.__setPhDraftLookup(async () => null);
const fresh = new Date().toISOString();
MR.__setLzMirrorProvider(async () => ({ ok: true, reason: null, keys: new Set(), ids: new Map(), barcodes: new Map(), list_as_of: fresh, bc_as_of: fresh, rows: 1,
  bc: { ok: true, reason: null, rows: 1, empty: false } }));
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] }; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, body) {
  const r = await fetch(ORIGIN + '/apps/master-edit' + url, { method, headers: { 'Content-Type': 'application/json', Accept: 'application/json', Origin: ORIGIN }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(60e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* */ }
  return { status: r.status, j, text };
}
const VALUES = { standard_price: '3980', cost: { jpy: '1650' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' };
function single(code, lz) {
  const b = { request_id: crypto.randomUUID(), kind: 'single', code, values: { ...VALUES, name: `試し ${code}` }, card: { create: false }, ...(lz === undefined ? {} : { logizard_barcode: lz }) };
  b.blanks = RB.missingBlankKeys('single', b, { orderApplicable: true });
  return b;
}
function variation(group, kids) {
  const b = {
    request_id: crypto.randomUUID(), group: { mode: 'new', code: group, name: `試し ${group}` }, axes: [{ axis: 1, name: 'カラー' }],
    options: kids.map(([c]) => ({ axis: 1, code: c, name: `色${c}` })),
    children: kids.map(([c, lz]) => ({ code: group + c, choices: { 1: c }, name: `試し ${group}【色${c}】`, jan: '', ...(lz === undefined ? {} : { lz_barcode: lz }) })),
    values: { ...VALUES }, card: { official_url: '', amazon_url: '', asin: '' },
  };
  b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
  return b;
}
const lzRows = async () => (await one('select count(*)::int as n from ops.sku_logizard_barcode_rules')).n;

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('当てる順 (アプリ → 0080 → ロールの流し直し)');

/** 欄を出さない・値なしで登録できる (A と B で同じ) */
async function oldDbStillWorks(tag) {
  assert.ok(!(await call('GET', '/new?kind=single')).text.includes('id="f-lzbc-in"'), `${tag}: 単品の欄が出た`);
  const nv = await call('GET', '/new?kind=variation');
  assert.equal(nv.status, 200);
  assert.match(nv.text, /"lzReady":false/, `${tag}: まとまりの欄が出る印`);
  const n0 = await lzRows();
  const r1 = await call('POST', '/api/new', single(`lzb80-${tag}-s`));
  assert.equal(r1.status, 200, `${tag}: ${r1.text}`);
  assert.equal(r1.j.lz_unchecked, undefined);
  const r2 = await call('POST', '/api/new/variation', variation(`lzb80${tag}`, [['-A'], ['-B']]));
  assert.equal(r2.status, 200, `${tag}: ${r2.text}`);
  assert.equal(await lzRows(), n0, `${tag}: ロジザードのバーコードの行を書いた`);
  const sku = await call('GET', `/sku/lzb80-${tag}-s`);
  assert.equal(sku.status, 200);
  assert.match(sku.text, /がまだ入っていないか、権限の流し直しの前です/, `${tag}: 商品の画面の一言`);
  assert.ok(!sku.text.includes('id="lzbc-in"'), `${tag}: 商品の画面の欄が出た`);
  const w = await call('POST', `/api/sku/lzb80-${tag}-s/logizard-barcode`, { request_id: crypto.randomUUID(), barcode: { kind: 'lz_assign' } });
  assert.deepEqual([w.status, w.j && w.j.reason], [503, 'db_not_ready'], w.text);
}

await ta('[A] 新しいアプリ + 0080 の前の DB (0079): 欄を出さない・値なしで単品 / まとまりを登録できる・商品の画面の保存 = 503', async () => {
  assert.equal((await one("select to_regprocedure('ops.set_sku_logizard_barcode(uuid, text, bigint, text, text, text, bigint, text, timestamp with time zone)') is null as ok")).ok, true, '0080 が入っている');
  await oldDbStillWorks('a');
});
await ta('[B] 0080 を当てた後・ロールの流し直しの前 (master_edit に実行権なし): 欄を出さない・値なしで登録できる', async () => {
  const r = await applyMigrations(db, { log: () => {} });
  assert.ok(r.applied.includes('0080'), JSON.stringify(r.applied));
  assert.equal((await one(`select has_function_privilege('master_edit', 'ops.set_sku_logizard_barcode(uuid, text, bigint, text, text, text, bigint, text, timestamp with time zone)', 'EXECUTE') as ok`)).ok, false);
  await oldDbStillWorks('b');
});
await ta('[C] ロールの流し直しの後: 欄を出す・値か空欄が要る (無い = 400 lzbc_missing)・値で登録できる', async () => {
  await createMasterEditRoles(pg, {});
  assert.ok((await call('GET', '/new?kind=single')).text.includes('id="f-lzbc-in"'));
  const r0 = await call('POST', '/api/new', single('lzb80-c-s0'));
  assert.deepEqual([r0.status, r0.j && r0.j.reason], [400, 'lzbc_missing'], r0.text);
  const r1 = await call('POST', '/api/new', single('lzb80-c-s1', { kind: 'value', value: 'X00LZB8001' }));
  assert.equal(r1.status, 200, r1.text);
  const r2 = await call('POST', '/api/new/variation', variation('lzb80c', [['-A', { kind: 'value', value: 'X00LZB8002' }], ['-B', { kind: 'lz_assign' }]]));
  assert.equal(r2.status, 200, r2.text);
  assert.deepEqual((await q(`select s.code, r.rule, r.value from ops.sku_logizard_barcode_rules r join core.skus s on s.sku_id = r.sku_id where s.code like 'lzb80%' order by s.code`)).map((x) => [x.code, x.rule, x.value]),
    [['lzb80-c-s1', 'fnsku', 'X00LZB8001'], ['lzb80c-A', 'fnsku', 'X00LZB8002'], ['lzb80c-B', 'lz_assign', null]]);
});

server.close();
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
