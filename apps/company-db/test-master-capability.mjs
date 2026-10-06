/**
 * test-master-capability.mjs — このコードが扱える company のキー (COMPANY_CAPABLE) と持ち主の読み方の版 (protocol) の定義元
 *   (config/master-capability.mjs・広げる道 PR-0・設計 newentry_widen v10 G8・Codex R1 M8 / R2 M5)
 *
 * 固定する契約:
 *   1 一覧の形: 知らないキー・重なり・並びの乱れ・説明の無い protocol = 落とす (typo で「扱えるつもり」を作らない)
 *   2 本番の active (2026-10-05 の 13 キー) は全部 capable (外すと本番の保存が code_behind で止まる)・configured の company は capable の中
 *   3 code_behind = 持ち主表 (DB の active / prepared) の company のキーで capable の外 (知らないキーも)。load のキーは見ない
 *   4 能力のハッシュはキーの順によらず、一覧が変わると変わる
 *   5 prepare (master-ownership-epoch.mjs) は capable の外のキーを用意しない (何も記録しない)・status に能力を出す
 * 使い方: node apps/company-db/test-master-capability.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { MASTER_OWNERSHIP, OWNED_COLUMNS, companyOwned } from '../../config/master-ownership.mjs';
import {
  COMPANY_CAPABLE, MASTER_OWNER_PROTOCOL, PROTOCOL_HISTORY, validateCapability, codeBehindKeys, configuredBeyondCapable, validateConfiguredCapable,
  capableHash, capabilityFingerprint,
} from '../../config/master-capability.mjs';

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const throwsCode = (fn, re) => assert.throws(fn, (e) => e.code === 'CAPABILITY_INVALID' && (!re || re.test(e.message)));

/** 本番の active (2026-10-05 の切替・AI_reference 17)。ここから外す = 本番の画面の保存が code_behind で止まる */
const PROD_ACTIVE_COMPANY_20261005 = Object.freeze([
  'external_ids.jan', 'products.name', 'products.sales_class', 'products.status', 'sku_costs', 'skus.handling', 'skus.name',
  'skus.reorder_months', 'skus.shipping', 'skus.standard_price', 'skus.tax_class', 'skus.tax_rate', 'supplier_skus.is_primary',
]);
const ALL_LOAD = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));

await ta('[1] 一覧の形: 今の一覧は通る・知らないキー・重なり・並び・protocol を落とす', async () => {
  assert.equal(validateCapability(), COMPANY_CAPABLE);
  assert.ok(Object.isFrozen(COMPANY_CAPABLE));
  for (const k of COMPANY_CAPABLE) assert.ok(OWNED_COLUMNS.includes(k), k);
  throwsCode(() => validateCapability(['skus.nmae']), /知らないキー: skus\.nmae/);
  throwsCode(() => validateCapability(['skus.name', 'skus.name']), /重なり/);
  throwsCode(() => validateCapability(['skus.name', 'products.name']), /キーの順/);
  throwsCode(() => validateCapability('skus.name'), /配列でない/);
  throwsCode(() => validateCapability(COMPANY_CAPABLE, 0), /protocol/);
  throwsCode(() => validateCapability(COMPANY_CAPABLE, MASTER_OWNER_PROTOCOL + 1), /protocol/);   // 説明の無い版
  assert.ok(Number.isSafeInteger(MASTER_OWNER_PROTOCOL) && Object.hasOwn(PROTOCOL_HISTORY, String(MASTER_OWNER_PROTOCOL)));
});

await ta('[2] 本番の active の 13 キーは全部 capable・configured の company は capable の中', async () => {
  assert.deepEqual(codeBehindKeys(Object.fromEntries(PROD_ACTIVE_COMPANY_20261005.map((k) => [k, 'company']))), []);
  assert.deepEqual(configuredBeyondCapable(), []);
  assert.equal(validateConfiguredCapable(), MASTER_OWNERSHIP);
  for (const k of companyOwned(MASTER_OWNERSHIP)) assert.ok(COMPANY_CAPABLE.includes(k), `configured が company なのに capable でない: ${k}`);
  // 予定が能力を追い越す = 落とす
  throwsCode(() => validateConfiguredCapable({ ...MASTER_OWNERSHIP, 'skus.sku_kind': 'company' }), /skus\.sku_kind/);
});

await ta('[3] code_behind: capable の外の company のキー (知らないキーも)・load は見ない', async () => {
  assert.deepEqual(codeBehindKeys(ALL_LOAD), []);
  assert.deepEqual(codeBehindKeys({ ...ALL_LOAD, 'skus.sku_kind': 'company', 'skus.name': 'company' }), ['skus.sku_kind']);
  assert.deepEqual(codeBehindKeys({ ...ALL_LOAD, 'future.key': 'company', 'other.future': 'load' }), ['future.key']);
  assert.deepEqual(codeBehindKeys({ 'skus.sku_kind': 'company' }, [...COMPANY_CAPABLE, 'skus.sku_kind'].sort()), []);
  assert.deepEqual(codeBehindKeys(null), []);
});

await ta('[4] 能力のハッシュ: 順によらない・一覧が変わると変わる・指紋', async () => {
  assert.match(capableHash(), /^[0-9a-f]{64}$/);
  assert.equal(capableHash([...COMPANY_CAPABLE].reverse()), capableHash());
  assert.notEqual(capableHash([...COMPANY_CAPABLE, 'skus.sku_kind']), capableHash());
  const fp = capabilityFingerprint();
  assert.deepEqual(fp, { protocol: MASTER_OWNER_PROTOCOL, capable_hash: capableHash(), capable: [...COMPANY_CAPABLE] });
});

await ta('[5] prepare は capable の外のキーを用意しない (何も残さない)・status に能力を出す', async () => {
  const EP = await import('../../scripts/company-db/master-ownership-epoch.mjs');
  const pg = new PGlite();
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: () => {} });
  const logs = [];
  const cli = (argv, extra = {}) => EP.cli(argv, { env: {}, connect: async () => ({ db, close: async () => {} }), log: (m) => logs.push(m), ...extra });
  const own = { ...ALL_LOAD, 'skus.tax_rate': 'company', 'skus.tax_class': 'company' };   // 写しで扱える組 (checkPublishOwnership は通る)
  // このコードの能力から税率を外したつもり = 用意しない
  assert.equal(await cli(['prepare'], { ownership: own, capable: COMPANY_CAPABLE.filter((k) => !k.startsWith('skus.tax_')) }), 1);
  assert.match(logs.at(-1), /company として扱えないキー \(skus\.tax_class・skus\.tax_rate\)/);
  assert.equal((await db.query('select count(*)::int as n from ops.master_ownership_state')).rows[0].n, 0);
  // 能力の中なら用意する (今までどおり)
  assert.equal(await cli(['prepare'], { ownership: own }), 0, logs.at(-1));
  assert.equal(await cli(['cancel']), 0);
  assert.equal(await cli(['status'], { env: { COMPANY_DB_URL: 'postgres://test' } }), 0);   // status は URL を見る (つなぐのは connect)
  const st = JSON.parse(logs.at(-1));
  assert.deepEqual(st.capability, capabilityFingerprint());
  await pg.close();
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
