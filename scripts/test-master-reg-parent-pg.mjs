/**
 * test-master-reg-parent-pg.mjs — 新商品の「色違い・サイズ違いの代表」(0061) を実 PostgreSQL の独立した接続で確かめる (PGlite は 1 接続なので書けない)
 *
 * 接続は本番と同じロールでログインする (create-master-edit-roles.mjs で作る・パスワードは試験の回ごと = SET ROLE でない本物のログイン):
 *   画面 (A・B) = master_edit / 段階 (P) = master_ops / 門の記録 (GR・GM) = master_gate_render・master_gate_minipc / 持ち主 (O2) = 夜間ロード・migration の代わり
 * 固定する契約:
 *   1 本物のログインの画面のロール: 登録で代表を選ぶ = 表に 1 行・約束の db_user = master_edit・core.products の親には書かない /
 *     表を直接書けない (42501)・権限を足しても約束 reg_parent_set の外では書けない (trigger)・関数の実行はできる・表は読める
 *   2 同じ商品の代表を 2 人が同時に直す: 後の人は SKU の鍵で待ち、前の人の commit の後に 409 version_conflict (見ていた代表が古い)
 *   3 代表を直す取引と NE 登録の CSV を作る取引: CSV の鍵で並ぶ (作っただけの CSV は使わないになる・後から作った CSV は新しい代表)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-reg-parent-pg.mjs
 *   (この PC では C:/tmp/pg-embed の run-variant.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
/** 切替の後 = 全部 company・代表 (products.parent) だけ load (本番の今) */
const OWN_MAP = Object.freeze({ ...Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company'])), 'products.parent': 'load' });

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (新商品の代表の実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-master-reg-parent.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const R = await import('../lib/master-register.mjs');
const C = await import('../lib/master-cutover.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const V = await import('../lib/master-reg-parent.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const codeOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e.code; } return null; };

const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }] };
const manualStopped = () => [{ id: 'ne:item-screen', by: 't', at: new Date().toISOString() }];
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const sku = (code, name) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const plan = () => ({
  skus: ['p001', 'p002', 'p003'].map((c) => sku(c, `単品 ${c}`)),
  variationGroups: [{ code: 'grp1', name: '名札', childCodes: ['p001', 'p002'], status: 'active' }], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: ['p001', 'p002', 'p003'].map((c) => ({ supplierCode: '0001', skuCode: c })),
  primarySuppliers: ['p001', 'p002', 'p003'].map((c) => ({ skuCode: c, supplierCode: '0001' })),
});
const dbName = `cdb_rp_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  await createMasterEditRoles(M, { pw: PW });
  await W2.useReal0058(M, { leases: ['single'] });
  const [A, B, P, GR, GM] = [await open('master_edit'), await open('master_edit'), await open('master_ops'), await open('master_gate_render'), await open('master_gate_minipc')];
  clients.push(A, B, P, GR, GM);
  const [dbA, dbB, dbP] = [A, B, P].map(pgAdapter);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const r0 = await runInitialLoad(dbM, plan(), { log: () => {}, runId: 'load_rp_1', now: new Date(Date.now() - 5 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  // 切替 (⑤-1 の本物の関数): frozen → backfill → company_owner → new_open
  const h = C.ownershipHash(OWN_MAP), legacy = C.ownershipHash(MASTER_OWNERSHIP);
  const mh = await C.manifestHashOf(dbM, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy, manual_entries_stopped: manualStopped(), drain: { done: true, checked_by: 't', checked_at: new Date().toISOString() } } });
  const p0 = (await P.query('select * from ops.registration_backfill_plan()')).rows[0];
  await P.query('select ops.backfill_sku_registrations($1, $2, $3)', [p0.sku_count, p0.snapshot_hash, 't@test']);
  await acks(OWN_MAP, 'frozen');
  await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(dbM, OWN_MAP);
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(OWN_MAP, 'company_owner');
  await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  const RUN = 'mc_20300110T000000000Z_aaaaaa';
  await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN]);
  await M.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN, entries: [...['p001', 'p002', 'p003'].map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })),
    { code_norm: 'grp1', kind: 'rep', state: 'ok', ne_code: 'GRP1', spellings: ['GRP1'] }] })]);
  await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(dbM, { runId: RUN });
  const reg = (db, code, parent) => R.registerNewSku(db, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code,
    values: { name: `新商品 ${code}`, standard_price: '1000', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', cost: { jpy: '300' }, ...(parent ? { variation_parent: parent } : {}) }, card: { create: false } },
  { open: true, now: new Date(), shippingRates: RATES });
  const setParent = (db, code, seen, parent) => V.setRegistrationParent(db, { actor: 'naka@test', requestId: crypto.randomUUID(), code, seen, parent }, { open: true });
  const parentOf = async (code) => (await q('select x.parent_code from ops.registration_parents x join core.skus s on s.sku_id = x.sku_id where s.code = $1', [code]))[0]?.parent_code ?? null;

  await ta('[1] 本物のログインの画面のロール: 登録で代表 = 表に 1 行 (約束の db_user = master_edit)・Company DB の親は書かない・表を直接書けない・約束の外は trigger が拒む', async () => {
    const r = await reg(dbA, 'pv-1', 'GRP1');
    assert.deepEqual(r.variation_parent, { code: 'grp1', kind: 'tag', name: '単品 p' });
    assert.equal(await parentOf('pv-1'), 'grp1');
    assert.equal((await q(`select p.parent_product_id from core.skus s join core.products p on p.product_id = s.product_id where s.code = 'pv-1'`))[0].parent_product_id, null);
    assert.deepEqual((await q(`select operation, db_user from ops.master_write_sessions where request_id = $1`, [V.regParentRequestId(r.request_id)]))[0], { operation: 'reg_parent_set', db_user: 'master_edit' });
    const id = (await q(`select sku_id::text as id from core.skus where code = 'pv-1'`))[0].id;
    const ins = `insert into ops.registration_parents (sku_id, parent_code, parent_norm, request_id, set_by) values ($1, 'p003', 'p003', gen_random_uuid(), 'x')`;
    assert.equal(await codeOf(A, ins, [id]), '42501');
    assert.equal(await codeOf(A, 'delete from ops.registration_parents'), '42501');
    assert.equal((await A.query('select count(*)::int as n from ops.registration_parents')).rows[0].n, 1, '画面のロールは読める');
    await M.query('grant insert, update on ops.registration_parents to master_edit');
    try {
      await A.query('begin');
      const e = await A.query(ins, [id]).catch((x) => x);
      await A.query('rollback');
      assert.match(String(e.message), /master_write_session_required/);
    } finally { await M.query('revoke insert, update on ops.registration_parents from master_edit'); }
    assert.equal(await parentOf('pv-1'), 'grp1');
  });

  await ta('[2] 同じ商品の代表を 2 人が同時に直す: 後の人は SKU の鍵で待ち、前の人の commit の後に 409 version_conflict', async () => {
    const id = (await q(`select sku_id::text as id from core.skus where code = 'pv-1'`))[0].id;
    await A.query('begin');
    await A.query(`select ops.set_registration_parent(gen_random_uuid(), 'a@test', null, $1::jsonb, $2::bigint, 'grp1', 'p003')`, [JSON.stringify(OWN_MAP), id]);
    const b = launch(setParent(dbB, 'pv-1', 'grp1', 'grp1'));
    await sleep(700);
    assert.equal(b.done, false, 'B は SKU の鍵で待つ');
    await A.query('commit');
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'version_conflict', rb.err?.message);
    assert.equal(await parentOf('pv-1'), 'p003');
  });

  await ta('[3] 代表を直す取引と NE 登録の CSV を作る取引は CSV の鍵で並ぶ: 先に作った CSV は使わないに・後から作った CSV は新しい代表', async () => {
    await reg(dbA, 'pv-2', 'grp1');
    const built = await G.buildRegExport(dbA, { actor: 'boss@test', kind: 'products', codes: ['pv-2'], requestId: crypto.randomUUID() }, { open: true, nowMs: Date.parse('2030-01-10T03:00:00Z') });
    const id = (await q(`select sku_id::text as id from core.skus where code = 'pv-2'`))[0].id;
    await A.query('begin');
    const r = (await A.query(`select ops.set_registration_parent(gen_random_uuid(), 'a@test', null, $1::jsonb, $2::bigint, 'grp1', 'p003') as r`, [JSON.stringify(OWN_MAP), id])).rows[0].r;
    assert.deepEqual(r.superseded, [String(built.export.export_id)]);
    const b = launch(G.buildRegExport(dbB, { actor: 'boss@test', kind: 'products', codes: ['pv-2'], requestId: crypto.randomUUID() }, { open: true, nowMs: Date.parse('2030-01-10T03:00:00Z') }));
    await sleep(700);
    assert.equal(b.done, false, 'CSV を作る取引は SKU / CSV の鍵で待つ');
    await A.query('commit');
    const rb = await b.promise;
    assert.ok(rb.ok, rb.err?.message);
    const t = Buffer.from((await q('select file_bytes from ops.ne_reg_exports where export_id = $1', [rb.ok.export.export_id]))[0].file_bytes).toString('utf8');
    assert.match(t, /\r\npv-2,新商品 pv-2,0001,300,1000,10,0,p003,empty\r\n/);
    assert.equal((await q('select close_reason from ops.ne_reg_exports where export_id = $1', [built.export.export_id]))[0].close_reason, 'superseded');
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
