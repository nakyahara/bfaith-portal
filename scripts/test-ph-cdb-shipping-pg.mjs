/**
 * product-hub が開いたときに Company DB から読む送料 (送料コード) の実 PostgreSQL の試験 (2026-10-21)。
 * 税率と同じ問い合わせ (services/cdb-tax-rate.mjs の readCdbTaxRates) に core.skus.shipping_* を足したので、
 * その SQL が本物の表で通ること・送料の答えの形を確かめる。持ち主のロール (本番の COMPANY_DB_URL と同じ) で読む。
 * 流し方: node C:/tmp/pg-embed/run-pg.mjs scripts/test-ph-cdb-shipping-pg.mjs <作業の木>
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (product-hub の送料の実 PostgreSQL の試験は飛ばす。画面の試験は scripts/test-master-legacy-gate.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ph-cdb-shipping-pg-'));
process.env.DATA_DIR = DATA_DIR;
const { openPgClient, pgAdapter, applyMigrations } = await import('./company-db/migrate.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const { readCdbTaxRates, shippingFromByCode } = await import('../apps/product-hub/services/cdb-tax-rate.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.08, taxClass: 'REDUCED_8', handling: 'active', salesClass: 3,
  cost: { jpy: 410, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, ...x });
const plan = {
  skus: [sku('ship-a', '送料あり'), sku('ship-b', '送料なし')],
  variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
};
const dbName = `cdb_phship_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const M = await openPgClient(u.toString());

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_phship_pg', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  // マスタ編集で送料コードを入れた形 (送料コード → 配送方法・送料)
  await M.query(`update core.skus set shipping_code = '5', shipping_method = 'ネコポス', shipping_cost_jpy = 237 where code = 'ship-a'`);
  await M.query(`update core.skus set shipping_code = null, shipping_method = null, shipping_cost_jpy = null where code = 'ship-b'`);
  const env = { COMPANY_DB_URL: u.toString() };

  await ta('🚨 税率と同じ問い合わせで送料も読める (本物の core.skus で SQL が通る・税率の答えは今までどおり)', async () => {
    const r = await readCdbTaxRates(['SHIP-A', 'ship-b', 'nope'], { env });
    assert.equal(r.ok, true, r.reason);
    assert.deepEqual([r.byCode.get('SHIP-A').found, r.byCode.get('SHIP-A').tax_rate], [true, 0.08]);
    assert.deepEqual(r.byCode.get('SHIP-A').shipping, { code: '5', method: 'ネコポス', cost: 237 });
    assert.deepEqual(r.byCode.get('ship-b').shipping, { code: null, method: null, cost: null }, '送料コードが空 = 未登録');
    assert.equal(r.byCode.get('nope').found, false);
  });
  await ta('送料の答え: Company DB にあるコードだけ・小文字のコードで引ける', async () => {
    const r = await readCdbTaxRates(['SHIP-A', 'ship-b', 'nope'], { env });
    const s = shippingFromByCode(['SHIP-A', 'ship-b', 'nope'], r.byCode);
    assert.deepEqual(Object.keys(s.byCode).sort(), ['ship-a', 'ship-b']);
    assert.equal(s.byCode['ship-a'].cost, 237);
  });
} finally {
  try { await M.end(); } catch { /* */ }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
