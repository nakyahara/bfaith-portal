/**
 * test-master-capability.mjs — このコードが扱える company のキー (COMPANY_CAPABLE) と持ち主の読み方の版 (protocol) の定義元
 *   (config/master-capability.mjs・広げる道 PR-0・設計 newentry_widen v10 G8・Codex R1 M8 / R2 M5)
 *
 * 固定する契約:
 *   1 一覧の形: 知らないキー・重なり・並びの乱れ・説明の無い protocol = 落とす (typo で「扱えるつもり」を作らない)
 *   2 本番の active (2026-10-05 の 13 キー + 10/7 の widen の skus.sku_kind = 14 キー) は全部 capable (外すと本番の保存が code_behind で止まる)・
 *     configured の company は capable の中・configured − active = listing_components.amazon だけ (⑦-2 PR-C・0059 の広げてよいキーの中)
 *   3 code_behind = 持ち主表 (DB の active / prepared) の company のキーで capable の外 (知らないキーも)。load のキーは見ない
 *   4 能力のハッシュはキーの順によらず、一覧が変わると変わる
 *   5 prepare (master-ownership-epoch.mjs) は capable の外のキーを用意しない (何も記録しない)・status に能力を出す
 * 使い方: node apps/company-db/test-master-capability.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
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
/** 本番の active (2026-10-07 13:48 の widen の後 = 13 キー + skus.sku_kind)。⑦-2 PR-C の prepare --widen はここから listing_components.amazon だけを足す */
const PROD_ACTIVE_COMPANY_20261007 = Object.freeze([...PROD_ACTIVE_COMPANY_20261005, 'skus.sku_kind'].sort());
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

await ta('[2] 本番の active (10/5 の 13 キー・10/7 の 14 キー) は全部 capable・configured の company は capable の中・configured − active = listing_components.amazon だけ', async () => {
  assert.deepEqual(codeBehindKeys(Object.fromEntries(PROD_ACTIVE_COMPANY_20261005.map((k) => [k, 'company']))), []);
  assert.deepEqual(codeBehindKeys(Object.fromEntries(PROD_ACTIVE_COMPANY_20261007.map((k) => [k, 'company']))), []);
  assert.deepEqual(configuredBeyondCapable(), []);
  assert.equal(validateConfiguredCapable(), MASTER_OWNERSHIP);
  for (const k of companyOwned(MASTER_OWNERSHIP)) assert.ok(COMPANY_CAPABLE.includes(k), `configured が company なのに capable でない: ${k}`);
  // 予定が能力を追い越す = 落とす (sku_components はまだ扱う PR が無い)。products.parent は CompanyDB構想/20 の PR-7 から扱える (configured は PR-8 まで load)
  throwsCode(() => validateConfiguredCapable({ ...MASTER_OWNERSHIP, sku_components: 'company' }), /sku_components/);
  assert.equal(MASTER_OWNERSHIP['products.parent'], 'load');
  assert.deepEqual(configuredBeyondCapable({ ...MASTER_OWNERSHIP, 'products.parent': 'company' }), []);
  // skus.sku_kind は #1641 で扱える = 予定に入れても能力の中。10/7 から configured も 'company' (広げる道の手順の 1)・10/7 13:48 の widen で active も company
  assert.equal(MASTER_OWNERSHIP['skus.sku_kind'], 'company');
  assert.deepEqual(configuredBeyondCapable({ ...MASTER_OWNERSHIP, 'skus.sku_kind': 'company' }), []);
  // ⑦-2 PR-C (10/8): configured の listing_components.amazon も 'company' (= 15 キー)
  assert.equal(MASTER_OWNERSHIP['listing_components.amazon'], 'company');
  assert.equal(companyOwned(MASTER_OWNERSHIP).length, 15);
  // prepare --widen の足すキー (configured の company − 本番の active (14 キー)) = listing_components.amazon だけ (0059 の ops.master_widen_allowed_keys の中)・
  //   active の company を load に戻すキーは無い (widen_not_additive に当たらない)
  assert.deepEqual(companyOwned(MASTER_OWNERSHIP).filter((k) => !PROD_ACTIVE_COMPANY_20261007.includes(k)).sort(), ['listing_components.amazon']);
  assert.deepEqual(PROD_ACTIVE_COMPANY_20261007.filter((k) => MASTER_OWNERSHIP[k] !== 'company'), []);
  const sql0059 = fs.readFileSync(new URL('../../db/company/migrations/0059_master_widen_amazon.sql', import.meta.url), 'utf8');
  assert.match(sql0059, /select array\['listing_components\.amazon', 'skus\.sku_kind'\]::text\[\]/);   // 0059 の広げてよいキー
});

await ta('[2b] skus.sku_kind を capable に足しても、今の本番 (DB の active = 2026-10-05 の 13 キー) の動きは変わらない: code_behind 0・写しの列・写しの持ち主の確かめ・夜間ロードの区分は load のまま', async () => {
  const MP = await import('../warehouse/master-publish.js');
  const active = { ...ALL_LOAD, ...Object.fromEntries(PROD_ACTIVE_COMPANY_20261005.map((k) => [k, 'company'])) };
  assert.equal(active['skus.sku_kind'], 'load');
  assert.deepEqual(codeBehindKeys(active), []);
  assert.deepEqual(codeBehindKeys(active, COMPANY_CAPABLE.filter((k) => k !== 'skus.sku_kind')), []);   // 足す前の能力でも同じ = 足したことで変わる所が無い
  assert.deepEqual(MP.checkPublishOwnership(active), []);
  assert.ok(!MP.publishCols(active).includes('kind'));   // 区分は写さない (load)
  // 区分も company にした持ち主表 (広げる道の後) = 写しの持ち主の確かめも能力も通る
  const widened = { ...active, 'skus.sku_kind': 'company' };
  assert.deepEqual([codeBehindKeys(widened), MP.checkPublishOwnership(widened), MP.publishCols(widened).includes('kind')], [[], [], true]);
});

await ta('[2c] ⑦-2 PR-A: listing_components.amazon を capable に足す = 写し (amazon-map.mjs) と同じ PR。configured は PR-C (10/8) で company・今の本番 (active の Amazon = load) の動きは変わらない・Amazon を company にした持ち主表でも ④a の写しは止まらない', async () => {
  const MP = await import('../warehouse/master-publish.js');
  const F = await import('./publish/fetch.mjs');
  assert.ok(COMPANY_CAPABLE.includes('listing_components.amazon'));
  assert.equal(MASTER_OWNERSHIP['listing_components.amazon'], 'company');   // configured は PR-C (10/8) で company。DB の active に入るのは widen の後
  const active = { ...ALL_LOAD, ...Object.fromEntries(PROD_ACTIVE_COMPANY_20261007.map((k) => [k, 'company'])) };
  assert.equal(active['listing_components.amazon'], 'load');
  // 今の本番 (Amazon = load): 足す前の能力と同じ答え
  assert.deepEqual(codeBehindKeys(active), []);
  assert.deepEqual(codeBehindKeys(active, COMPANY_CAPABLE.filter((k) => k !== 'listing_components.amazon')), []);
  assert.deepEqual(MP.checkPublishOwnership(active), []);
  // Amazon を company にした持ち主表 (PR-B・PR-C の後の広げる道): 能力 = 扱える・④a = 写さない列として通す (ownership_not_supported にならない)・④a の列は増えない
  const widened = { ...active, 'listing_components.amazon': 'company' };
  assert.deepEqual(widened, { ...ALL_LOAD, ...Object.fromEntries(companyOwned(MASTER_OWNERSHIP).map((k) => [k, 'company'])) });   // widen の後 = configured
  assert.deepEqual(codeBehindKeys(widened), []);
  assert.deepEqual(MP.checkPublishOwnership(widened), []);
  assert.ok(Object.hasOwn(MP.NO_OLD_TABLE_COPY, 'listing_components.amazon'));
  assert.match(MP.NO_OLD_TABLE_COPY['listing_components.amazon'], /amazon-map\.mjs/);
  assert.deepEqual(MP.publishCols(widened), MP.publishCols({ ...widened, 'listing_components.amazon': 'load' }));
  const src = { cdbReadAt: '2030-01-01T00:00:00.000000Z', has0027: true, skus: [], costs: new Map(), primary: new Map(), watermark: 1, load: { ingest_run_id: 'x' } };
  const gen = F.buildGeneration({ source: src, ownership: widened });
  assert.deepEqual([gen.problems, gen.ownership_problems], [[], []]);
  // configured を Amazon = company にしても能力の外に出ない (PR-C の 1 行が通る)
  assert.deepEqual(configuredBeyondCapable({ ...MASTER_OWNERSHIP, 'listing_components.amazon': 'company' }), []);
});

await ta('[2d] CompanyDB構想/20 PR-7: products.parent を capable に足す = まとまりの登録の画面・saveSku から代表の欄を外す と同じ PR。configured は load のまま (PR-8)・今の本番の動きは変わらない・products.parent を company にした持ち主表でも ④a の写しは止まらない (写さない列)', async () => {
  const MP = await import('../warehouse/master-publish.js');
  assert.ok(COMPANY_CAPABLE.includes('products.parent'));
  assert.equal(MASTER_OWNERSHIP['products.parent'], 'load');
  const active = { ...ALL_LOAD, ...Object.fromEntries(PROD_ACTIVE_COMPANY_20261007.map((k) => [k, 'company'])) };
  assert.equal(active['products.parent'], 'load');
  assert.deepEqual(codeBehindKeys(active), []);
  assert.deepEqual(MP.checkPublishOwnership(active), []);
  const widened = { ...active, 'products.parent': 'company' };
  assert.deepEqual(codeBehindKeys(widened), []);
  assert.deepEqual(MP.checkPublishOwnership(widened), []);
  assert.ok(Object.hasOwn(MP.NO_OLD_TABLE_COPY, 'products.parent'));
  assert.match(MP.NO_OLD_TABLE_COPY['products.parent'], /照合 ②/);
  assert.deepEqual(MP.publishCols(widened), MP.publishCols(active));
  // 新商品の画面の保存 (saveSku) の欄に代表は無い (代表は登録の時に 1 回だけ・まとまりの関数だけ)
  const W = await import('../../lib/master-write.mjs');
  assert.ok(!Object.hasOwn(W.SINGLE_FIELDS, 'parent_code'));
  assert.ok(!Object.values(W.SINGLE_FIELDS).some((d) => d.keys.includes('products.parent')));
});

await ta('[3] code_behind: capable の外の company のキー (知らないキーも)・load は見ない', async () => {
  assert.deepEqual(codeBehindKeys(ALL_LOAD), []);
  assert.deepEqual(codeBehindKeys({ ...ALL_LOAD, sku_components: 'company', 'skus.name': 'company' }), ['sku_components']);
  assert.deepEqual(codeBehindKeys({ ...ALL_LOAD, 'products.parent': 'company', 'skus.name': 'company' }), []);   // PR-7 から扱える
  assert.deepEqual(codeBehindKeys({ ...ALL_LOAD, 'future.key': 'company', 'other.future': 'load' }), ['future.key']);
  assert.deepEqual(codeBehindKeys({ 'products.parent': 'company' }, COMPANY_CAPABLE.filter((k) => k !== 'products.parent')), ['products.parent']);   // PR-7 の前の能力 = code_behind
  assert.deepEqual(codeBehindKeys({ 'skus.sku_kind': 'company' }), []);   // #1641 から扱える
  assert.deepEqual(codeBehindKeys(null), []);
});

await ta('[4] 能力のハッシュ: 順によらない・一覧が変わると変わる・指紋', async () => {
  assert.match(capableHash(), /^[0-9a-f]{64}$/);
  assert.equal(capableHash([...COMPANY_CAPABLE].reverse()), capableHash());
  assert.notEqual(capableHash([...COMPANY_CAPABLE, 'sku_components']), capableHash());
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
  // 区分 (skus.sku_kind) を company にした持ち主表も用意できる (#1641 = 能力と写しの両方が知っている)
  assert.equal(await cli(['prepare'], { ownership: { ...own, 'skus.sku_kind': 'company' } }), 0, logs.at(-1));
  assert.equal((await db.query('select prepared_map ->> \'skus.sku_kind\' as k from ops.master_ownership_state')).rows[0].k, 'company');
  assert.equal(await cli(['cancel']), 0);
  assert.equal(await cli(['status'], { env: { COMPANY_DB_URL: 'postgres://test' } }), 0);   // status は URL を見る (つなぐのは connect)
  const st = JSON.parse(logs.at(-1));
  assert.deepEqual(st.capability, capabilityFingerprint());
  await pg.close();
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
