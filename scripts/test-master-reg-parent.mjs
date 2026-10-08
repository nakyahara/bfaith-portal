/**
 * test-master-reg-parent.mjs — 新商品の「色違い・サイズ違いの代表」(migration 0061・lib/master-reg-parent.mjs・2026-10-08 中原さんの決定 a)
 *
 * Company DB = PGlite (持ち主のロール deploy で migration)。書き込みは画面だけのロール master_edit・照合の確かめは watch_writer。
 * 持ち主表 = 切替の後 (全部 company) のうち、代表 (products.parent) だけ load = 本番の今と同じ (代表の持ち主は NE のまま)
 * 固定する契約:
 *   P 登録: 代表を選ぶ = ops.registration_parents に 1 行 (Company DB の書き方)・core.products の親には書かない・約束 reg_parent_set と done・同じ request_id = 同じ答え /
 *     代表なし = 行なし (今までと同じ)・Company DB に無い・セット・まとまりに入っている単品・自分 = 400 で何も保存しない
 *   C NE 登録の CSV: 代表の列 = NE の元の書き方 (名札は rep を先に・単品は product)・expected.parent = norm・代表なし = empty・
 *     NE に無い代表 = 止まる理由 (lib と DB の関数の両方)・照合 ② で合えば verified・違えば partial
 *   S 下書きの間の商品の画面: 変える / 外す・画面が見ていた代表と違えば 409・作っただけの CSV は使わないにする・配った後 = 409・下書きでなくなったら 409
 *   D DB の守り: 画面のロールは表を直接書けない・関数は public に実行権なし・0053 / 0054 の本文との差は決めた所だけ
 *   L 夜間ロード: NE の代表を写すと Company DB の親が登録の代表と同じ (商品の画面が「同じ」と出す)
 *   H 画面: 新商品の登録・商品の画面の描画 (router を通す)・候補を探す API・保存の API
 * 使い方: node scripts/test-master-reg-parent.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'master-reg-parent-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const OWNED = (await import('../config/master-ownership.mjs')).OWNED_COLUMNS;
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED.map((k) => [k, 'load'])));
/** 切替の後の持ち主表 = 全部 company・代表 (products.parent) だけ load (本番の今 = 中原さんの決定 a: 代表の持ち主は NE のまま) */
const OWN_MAP = Object.freeze({ ...Object.fromEntries(OWNED.map((k) => [k, 'company'])), 'products.parent': 'load' });
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const R = await import('../lib/master-register.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const V = await import('../lib/master-reg-parent.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const rejectsWith = async (p, status, reason) => {
  try { await p; } catch (e) {
    assert.ok(e instanceof W.MasterWriteError, `MasterWriteError でない: ${e && e.stack}`);
    assert.equal(e.status, status, `${e.reason}: ${e.message}`);
    if (reason) assert.equal(e.reason, reason, e.message);
    return e;
  }
  assert.fail(`${status} ${reason || ''} にならなかった`);
};
const pgErr = async (p, re) => { try { await p; } catch (e) { if (re) assert.match(String(e.message), re); return e; } assert.fail('拒まれなかった'); };
const uuid = () => crypto.randomUUID();
const LOAD_NOW = new Date(Date.now() - 5 * 86400e3);
const NOW = new Date('2030-01-10T03:00:00Z');
const NOW_MS = NOW.getTime();
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }], ['S02', { method: '宅急便', cost: 520 }]]);
const RUN1 = 'mc_20300110T000000000Z_aaaaaa';
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }] };
const manualStopped = () => [{ id: 'ne.product_screen', by: 'naka@test', at: new Date().toISOString() }];
const drain = () => ({ done: true, checked_by: 'naka@test', checked_at: new Date().toISOString() });
const BUILDS = { render: ['r1'], minipc: ['m1'] };

const skuOf = (code, name, kind, taxRate, cost, x = {}) => ({
  code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : taxRate === 0.1 ? 'STANDARD_10' : null, handling: 'active', salesClass: 3,
  cost: cost == null ? null : { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' },
  standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...x,
});
/**
 * NE の材料 (夜間ロード): 単品 7 つ + セット 1 つ。まとまり grp1 = s006・s007 (NE にある名札 GRP1)・grp2 = s004 (Company DB にはあるが NE の元のコードに無い名札)。
 * s005 = まとまりに入っていない単品
 */
function makePlan({ extraSkus = [], groups = null } = {}) {
  const singles = ['s001', 's002', 's003', 's004', 's005', 's006', 's007'].map((c, i) => skuOf(c, `単品 ${i + 1}`, 'single', 0.1, 100 * (i + 1)));
  return {
    skus: [...singles, skuOf('set001', 'セット 1', 'set', 0.1, 400), ...extraSkus],
    variationGroups: groups ?? [{ code: 'grp1', name: '名札 1', childCodes: ['s006', 's007'], status: 'active' }, { code: 'grp2', name: '名札 2', childCodes: ['s004'], status: 'active' }],
    setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 2, source: 'ne' }],
    listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }],
    supplierSkus: singles.map((s) => ({ supplierCode: '0001', skuCode: s.code })),
    primarySuppliers: singles.map((s) => ({ skuCode: s.code, supplierCode: '0001' })),
    reorder: { available: true, runId: 'pml_test' },
  };
}

async function setupDb() {
  const pg = new PGlite();
  const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  await W2.useReal0058(pg, { leases: ['single'] });
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: 'load_setup', now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  const E0 = { pg, db, sessionUser };
  await toPhase(E0, 'frozen');
  const p = (await pg.query('select * from ops.registration_backfill_plan()')).rows[0];
  await as(E0, 'master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 'naka@test']));
  await toPhase(E0, 'company_owner');
  await toPhase(E0, 'new_open');
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
  await recordNeCodes(pg, RUN1);
  return E0;
}
async function asGate(E, host, fn) {
  await E.pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await E.pg.query(`set session authorization ${E.sessionUser}`); await E.pg.query('set role deploy'); }
}
async function toPhase(E, to) {
  const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
  const own = to === 'frozen' ? MASTER_OWNERSHIP : OWN_MAP;
  for (const [host, inst, buildId] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) {
    await asGate(E, host, () => C.recordLegacyGateAck(E.db, { host, instanceId: inst, buildId, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
  }
  const mh = await C.manifestHashOf(E.db, MANIFEST);
  if (to !== 'frozen') await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(E.db, OWN_MAP);
  const evidence = to === 'frozen' ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(MASTER_OWNERSHIP), manual_entries_stopped: manualStopped(), drain: drain() }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(OWN_MAP) };
  return as(E, 'master_ops', () => C.advanceCutoverPhase(E.db, { to, actor: 'naka@test', evidence }));
}
/** NE の元のコード (0041): 前からある商品 + 名札 GRP1 (NE の書き方は大文字)。grp2 は NE に無い (+ extra の商品) */
async function recordNeCodes(pg, run, extra = []) {
  const codes = ['s001', 's002', 's003', 's004', 's005', 's006', 's007', 'set001', ...extra];
  const entries = [...codes.map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })), { code_norm: 'grp1', kind: 'rep', state: 'ok', ne_code: 'GRP1', spellings: ['GRP1'] }];
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries })]);
  await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(pgliteAdapter(pg), { runId: run });
}
async function as(E, role, fn) { await E.pg.query(`set role ${role}`); try { return await fn(); } finally { await E.pg.query('set role deploy'); } }

const E = await setupDb();
const { pg, db } = E;
const q = async (sql, params) => (await db.query(sql, params)).rows;
const one = async (sql, params) => (await q(sql, params))[0];
const skuId = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code]))?.id;
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = $1', [code]))?.state;
const parentRow = async (code) => one('select x.parent_code, x.parent_norm, x.set_by from ops.registration_parents x join core.skus s on s.sku_id = x.sku_id where s.code = $1', [code]);
const cdbParentOf = async (code) => (await one(`select pp.display_code from core.skus s join core.products p on p.product_id = s.product_id
   left join core.products pp on pp.product_id = p.parent_product_id where s.code = $1`, [code]))?.display_code ?? null;
const single = (over = {}) => ({ name: '新しい単品', standard_price: '1500', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', cost: { jpy: '300' }, ...over });
const reg = (code, values, o = {}) => as(E, 'master_edit', () => R.registerNewSku(db, { actor: 'naka@test', requestId: o.requestId ?? uuid(), kind: 'single', code, values, card: { create: false } },
  { open: true, now: new Date(), shippingRates: RATES }));
const opts = () => ({ open: true, nowMs: NOW_MS });
const build = (codes) => as(E, 'master_edit', () => G.buildRegExport(db, { actor: 'boss@test', kind: 'products', codes, requestId: uuid() }, opts()));
const issue = (id) => as(E, 'master_edit', () => G.issueRegExport(db, { actor: 'boss@test', exportId: id }, opts()));
const declare = (id, sha) => as(E, 'master_edit', () => G.declareRegExport(db, { actor: 'boss@test', exportId: id, sha256: sha, result: 'ok', neMessage: '2件成功しました。' }, opts()));
const supersede = (id) => as(E, 'master_edit', () => G.supersedeRegExport(db, { actor: 'boss@test', exportId: id, reason: '直したい', correction: 'NE には取り込んでいない', confirm: true }, opts()));
const setParent = (code, seen, parent, o = {}) => as(E, 'master_edit', () => V.setRegistrationParent(db, { actor: 'naka@test', requestId: o.requestId ?? uuid(), code, seen, parent }, { open: true }));
const itemsOf = async (id) => q('select i.state, s.code, i.expected from ops.ne_reg_export_items i join core.skus s on s.sku_id = i.sku_id where i.export_id = $1 order by i.row_from', [id]);
const fileText = async (id) => Buffer.from((await one('select file_bytes from ops.ne_reg_exports where export_id = $1', [id])).file_bytes).toString('utf8');
const candidate = async (code) => (await G.regSummary(db, { nowMs: NOW_MS })).candidates.find((c) => c.code === code);
let runSeq = 0;
async function newRun() {
  runSeq++;
  const id = `mc_20300109T${String(runSeq).padStart(9, '0')}Z_bbbbbb`;
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)`, [id, `2030-01-09T0${runSeq % 10}:00:00Z`]);
  return id;
}
const FETCH = (run) => ({ generation_id: `gen_${run}`, products_rev: '7', sets_rev: '8', raw_hash: crypto.createHash('sha256').update(run).digest('hex') });
async function check(run, observations) {
  await as(E, 'watch_writer', () => pg.query('select ops.snapshot_ne_reg_targets($1) as r', [run]));
  const at = new Date(Date.now() + 60000).toISOString();
  const w = await as(E, 'watch_writer', async () => (await pg.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run, fetch: FETCH(run),
    products_at: at, sets_at: at, absence_trusted: true, observations })])).rows[0].r);
  await as(E, 'watch_writer', () => pg.query('select ops.seal_ne_registration_run($1, $2, $3) as r', [run, w.observation_hash, 'e'.repeat(64)]));
  return as(E, 'watch_writer', async () => (await pg.query('select ops.record_ne_registration_check($1) as r', [run])).rows[0].r);
}
const ok = (v) => ({ st: 'ok', v });
const obsSingle = (code, over = {}) => ({ code_norm: code, present: true, trusted: true, kind: 'single', cols: {
  name: ok('新しい単品'), supplier: ok('0001'), cost: ok(300), price: ok(1500), tax_rate: ok(0.1), handling: ok('active'), parent: ok(null), ...over } });

console.log('登録で代表を選ぶ (P)');

let V1 = null;
await ta('[P1] 名札を選ぶ (大文字で打っても Company DB の書き方)・ops.registration_parents に 1 行・core.products の親には書かない・約束 reg_parent_set の done・同じ request_id = 同じ答え', async () => {
  const rid = uuid();
  V1 = await reg('new-v1', single({ variation_parent: 'GRP1' }), { requestId: rid });
  assert.equal(V1.ok, true);
  assert.deepEqual(V1.variation_parent, { code: 'grp1', kind: 'tag', name: '単品' });
  assert.deepEqual(await parentRow('new-v1'), { parent_code: 'grp1', parent_norm: 'grp1', set_by: 'naka@test' });
  assert.equal(await cdbParentOf('new-v1'), null, '🚨 Company DB の親 (持ち主 = NE・夜間ロード) には書かない');
  assert.equal(await regOf('new-v1'), 'draft');
  const prid = V.regParentRequestId(rid);
  assert.deepEqual(await one('select operation, status, target_code from ops.master_edit_requests where request_id = $1', [prid]), { operation: 'reg_parent_set', status: 'done', target_code: 'new-v1' });
  assert.equal((await one('select operation from ops.master_write_sessions where request_id = $1', [prid])).operation, 'reg_parent_set');
  // 同じ request_id の押し直し = 登録の記録の答え (2 回目の行は作らない)
  const again = await reg('new-v1', single({ variation_parent: 'GRP1' }), { requestId: rid });
  assert.equal(again.replayed, true);
  assert.equal((await one("select count(*)::int as n from ops.registration_parents x join core.skus s on s.sku_id = x.sku_id where s.code = 'new-v1'")).n, 1);
  // 中身が違う (代表だけ違う) = 409
  await rejectsWith(reg('new-v1', single({ variation_parent: 's005' }), { requestId: rid }), 409, 'request_id_reused');
});

await ta('[P2] 単品を代表にする (まとまりの無い s005 = 最初の色違い)・代表なし = 行なし (今までと同じ)', async () => {
  const r = await reg('new-v2', single({ variation_parent: 's005' }));
  assert.deepEqual(r.variation_parent, { code: 's005', kind: 'single', name: '単品 5' });
  assert.equal((await parentRow('new-v2')).parent_code, 's005');
  const n = await reg('new-v0', single({ variation_parent: '' }));
  assert.equal(n.variation_parent, undefined, '代表なしの登録の答えは今までと同じ');
  assert.equal(await parentRow('new-v0'), undefined);
  assert.equal((await one("select count(*)::int as n from ops.master_edit_requests where operation = 'reg_parent_set' and target_code = 'new-v0'")).n, 0);
});

await ta('[P3] 選べない代表 = 400 で何も保存しない (Company DB に無い・セット・まとまりに入っている単品・自分・形)', async () => {
  for (const [code, parent, reason, re] of [
    ['new-x1', 'nope', 'parent_not_found', /Company DB に無い/],
    ['new-x2', 'set001', 'parent_not_single', /セット/],
    ['new-x3', 's006', 'parent_nested', /grp1/],
    ['new-x4', 'new-x4', 'invalid_input', /自分自身/],
    ['new-x5', 'a b', 'invalid_input', /NE のコードの形/],
  ]) {
    const e = await rejectsWith(reg(code, single({ variation_parent: parent })), 400, reason);
    assert.match(e.message, re);
    assert.equal(e.extra.field, 'variation_parent');
    assert.equal(await skuId(code), undefined, `${code}: 登録ごと巻き戻る`);
  }
});

await ta('[P4] 0061 の前の DB (コードを先に配った) = 代表を選んだ登録は 409 not_applied で何も保存しない・代表なしの登録は今までどおり', async () => {
  await pg.query('alter function ops.set_registration_parent(uuid, text, text, jsonb, bigint, text, text) rename to set_registration_parent__hidden_variant');
  try {
    await rejectsWith(reg('new-x6', single({ variation_parent: 'grp1' })), 409, 'not_applied');
    assert.equal(await skuId('new-x6'), undefined);
    assert.equal((await reg('new-x7', single())).ok, true);
  } finally { await pg.query('alter function ops.set_registration_parent__hidden_variant(uuid, text, text, jsonb, bigint, text, text) rename to set_registration_parent'); }
});

console.log('\nNE 登録の CSV と照合 ② (C)');

let EX = null;
await ta('[C1] CSV の代表の列 = NE の元の書き方 (名札 GRP1 / 単品 s005)・代表なし = empty・expected.parent = norm (lib と DB の関数が同じ行)', async () => {
  const r = await build(['new-v1', 'new-v2', 'new-v0']);
  EX = r.export;
  const text = await fileText(EX.export_id);
  assert.match(text, /\r\nnew-v1,新しい単品,0001,300,1500,10,0,GRP1,empty\r\n/);
  assert.match(text, /\r\nnew-v2,新しい単品,0001,300,1500,10,0,s005,empty\r\n/);
  assert.match(text, /\r\nnew-v0,新しい単品,0001,300,1500,10,0,empty,empty\r\n/);
  const items = await itemsOf(EX.export_id);
  assert.deepEqual(items.map((i) => [i.code, i.expected.values.parent]), [['new-v1', 'grp1'], ['new-v2', 's005'], ['new-v0', null]]);
});

await ta('[C2] 照合 ②: NE の代表が同じ = verified (NE 確認済み)・違う (NE に代表なし) = partial・代表なし = 今までどおり verified', async () => {
  await issue(EX.export_id);
  await declare(EX.export_id, EX.sha256);
  const run = await newRun();
  const r = await check(run, [obsSingle('new-v1', { parent: ok('grp1') }), obsSingle('new-v2', { parent: ok(null) }), obsSingle('new-v0')]);
  assert.deepEqual(r.counts, { verified: 2, partial: 1 });
  assert.deepEqual((await itemsOf(EX.export_id)).map((i) => [i.code, i.state]), [['new-v1', 'verified'], ['new-v2', 'partial'], ['new-v0', 'verified']]);
  assert.deepEqual([await regOf('new-v1'), await regOf('new-v2'), await regOf('new-v0')], ['ne_confirmed', 'ne_pending', 'ne_confirmed']);
  const d = (await one(`select c.detail from ops.ne_reg_checks c join core.skus s on s.sku_id = c.sku_id where s.code = 'new-v2' and c.compare_run_id = $1`, [run])).detail;
  assert.deepEqual([d.compare.cols.parent.ok, d.compare.cols.parent.expected], [false, 's005']);
});

await ta('[C3] NE に無い代表 (grp2 = Company DB にはある名札) = 登録はできる・CSV は作らない (止まる理由・lib)', async () => {
  await reg('new-v3', single({ variation_parent: 'grp2' }));
  const c = await candidate('new-v3');
  assert.deepEqual(c.blockers, ['選んだ代表 grp2 が NE に無い (NE の代表・商品のコードに無いか、書き方が 1 つに決まらない)']);
  const e = await rejectsWith(build(['new-v3']), 409, 'not_ready');
  assert.match(JSON.stringify(e.extra.items), /grp2 が NE に無い/);
  assert.equal((await one(`select count(*)::int as n from ops.ne_reg_export_items i join core.skus s on s.sku_id = i.sku_id where s.code = 'new-v3'`)).n, 0);
});

await ta('[C4] DB の関数 (ops.ne_reg_canonical) も NE に無い代表で止める・NE にある代表は NE の書き方', async () => {
  const canon = async (code) => (await one('select ops.ne_reg_canonical($1::bigint, current_date) as r', [await skuId(code)])).r;
  const a = await canon('new-v3');
  assert.ok(a.blockers.includes('選んだ代表 grp2 が NE に無い (NE の代表・商品のコードに無いか、書き方が 1 つに決まらない)'), JSON.stringify(a.blockers));
  assert.equal(a.cells[0][7], 'empty');
  await reg('new-v4', single({ variation_parent: 'grp1' }));
  const b = await canon('new-v4');
  assert.deepEqual([b.blockers, b.cells[0][7], b.expected.values.parent], [[], 'GRP1', 'grp1']);
  // 名札の rep が「1 つに決まらない」(collided) = 止める (product が ok でも rep を先に見る = lib と同じ)
  const run = 'mc_20300110T050000000Z_eeeeee';
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T05:00:00Z', 0)`, [run]);
  const codes = ['s001', 's002', 's003', 's004', 's005', 's006', 's007', 'set001'];
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: [...codes.map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })),
    { code_norm: 'grp1', kind: 'rep', state: 'collided', ne_code: null, spellings: ['GRP1', 'grp1'] }, { code_norm: 'grp1', kind: 'product', state: 'ok', ne_code: 'grp1', spellings: ['grp1'] }] })]);
  try {
    assert.match((await canon('new-v4')).blockers.join(), /grp1 が NE に無い/);
    assert.match((await candidate('new-v4')).blockers.join(), /grp1 が NE に無い/);
  } finally {
    const back = 'mc_20300110T060000000Z_ffffff';
    await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T06:00:00Z', 0)`, [back]);
    await recordNeCodes(pg, back);
  }
});

console.log('\n下書きの間の商品の画面 (S)');

await ta('[S1] 変える・外す・画面が見ていた代表と違う = 409 version_conflict・同じ = no_change (記録しない)', async () => {
  await rejectsWith(setParent('new-v4', null, 's005'), 409, 'version_conflict');
  const a = await setParent('new-v4', 'GRP1', 's005');   // 見ていた代表は大文字小文字を問わない
  assert.deepEqual([a.changed[0].from, a.changed[0].to], ['grp1', 's005']);
  assert.equal((await parentRow('new-v4')).parent_code, 's005');
  const same = await setParent('new-v4', 's005', 'S005');
  assert.equal(same.no_change, true);
  const b = await setParent('new-v4', 's005', null);
  assert.deepEqual([b.changed[0].from, b.changed[0].to, b.variation_parent], ['s005', null, null]);
  assert.equal(await parentRow('new-v4'), undefined);
  await setParent('new-v4', null, 'grp1');
});

await ta('[S2] 作っただけの CSV は直すと使わないに・配った後は 409 reg_csv_issued (使わないにすれば直せる)・下書きでない = 409 not_draft', async () => {
  const b = await build(['new-v4']);
  const r = await setParent('new-v4', 'grp1', 's005');
  assert.deepEqual(r.superseded, [b.export.export_id]);
  assert.equal((await one('select state, close_reason from ops.ne_reg_exports where export_id = $1', [b.export.export_id])).close_reason, 'superseded');
  const b2 = await build(['new-v4']);
  assert.match(await fileText(b2.export.export_id), /,s005,empty\r\n/);
  await issue(b2.export.export_id);
  await rejectsWith(setParent('new-v4', 's005', 'grp1'), 409, 'reg_csv_issued');
  const v4 = Number(await skuId('new-v4'));
  await pgErr(as(E, 'master_edit', () => pg.query("select ops.set_registration_parent(gen_random_uuid(), 'naka@test', null, $1::jsonb, $2::bigint, 's005', 'grp1')",
    [JSON.stringify(OWN_MAP), v4])), /reg_csv_issued/);
  await supersede(b2.export.export_id);
  assert.equal((await setParent('new-v4', 's005', 'grp1')).ok, true);
  // 申告した後 (NE 登録待ち) = 下書きでない
  await rejectsWith(setParent('new-v2', 's005', 'grp1'), 409, 'not_draft');
  // 同じ request_id の押し直し = 残した答え・中身が違えば 409
  const rid = uuid();
  const x = await setParent('new-v4', 'grp1', 's005', { requestId: rid });
  assert.equal((await setParent('new-v4', 'grp1', 's005', { requestId: rid })).replayed, true);
  await rejectsWith(setParent('new-v4', 'grp1', 'grp1', { requestId: rid }), 409, 'request_id_reused');
  assert.equal(x.ok, true);
});

console.log('\nDB の守り (D)');

await ta('[D1] 画面のロールは表を直接書けない・関数の外で書く行は約束が要る・関数は public に実行権なし・security definer', async () => {
  const id = await skuId('new-v0');
  await as(E, 'master_edit', () => pgErr(pg.query("insert into ops.registration_parents (sku_id, parent_code, parent_norm, request_id, set_by) values ($1, 'grp1', 'grp1', gen_random_uuid(), 'x')", [id]), /permission denied/));
  await as(E, 'master_edit', () => pgErr(pg.query("delete from ops.registration_parents"), /permission denied/));
  assert.equal((await one("select has_table_privilege('master_edit', 'ops.registration_parents', 'SELECT') as s")).s, true);
  // 守りの trigger: 画面のロールで (権限を一時的に渡しても) 約束 reg_parent_set の外では書けない
  await pg.query('grant insert on ops.registration_parents to master_edit');
  try {
    await as(E, 'master_edit', () => pgErr(pg.query("insert into ops.registration_parents (sku_id, parent_code, parent_norm, request_id, set_by) values ($1, 'grp1', 'grp1', gen_random_uuid(), 'x')", [id]), /master_write_session_required/));
  } finally { await pg.query('revoke insert on ops.registration_parents from master_edit'); }
  const fns = await q(`select p.proname, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub,
       has_function_privilege('master_edit', p.oid, 'execute') as me
     from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname in ('set_registration_parent', 'guard_registration_parents', 'ne_reg_canonical', 'open_reg_write') order by 1`);
  assert.deepEqual(fns.map((f) => [f.proname, f.d, f.c, f.pub, f.me]), [
    ['guard_registration_parents', true, 'search_path=pg_catalog, pg_temp', false, false],
    ['ne_reg_canonical', true, 'search_path=pg_catalog, pg_temp', false, false],
    ['open_reg_write', true, 'search_path=pg_catalog, pg_temp', false, false],
    ['set_registration_parent', true, 'search_path=pg_catalog, pg_temp', false, true]]);
  // 操作の一覧: lib (REG_WRITE_OPERATIONS) に reg_parent_set・DB の CHECK 2 つ・書いてよい表
  assert.ok(Object.prototype.hasOwnProperty.call(G.REG_WRITE_OPERATIONS, 'reg_parent_set'));
  for (const c of ['ck_mws_operation', 'ck_mer_operation']) assert.match((await one('select pg_get_constraintdef(oid) as d from pg_constraint where conname = $1', [c])).d, /'reg_parent_set'/);
  const allowed = async (op, t, a) => (await one('select ops.master_write_allowed($1, $2, $3) as x', [op, t, a])).x;
  assert.deepEqual([await allowed('reg_parent_set', 'ops.registration_parents', 'INSERT'), await allowed('reg_parent_set', 'ops.ne_reg_export_items', 'UPDATE'),
    await allowed('reg_parent_set', 'core.products', 'UPDATE'), await allowed('sku_edit', 'ops.registration_parents', 'INSERT')], [true, true, false, false]);
});

await ta('[D2] 0061 の作り直し: 0053 の ops.ne_reg_canonical・ops.open_reg_write / 0054 の ops.master_write_allowed との差は決めた所だけ', async () => {
  const MIG = path.join(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '..', 'db', 'company', 'migrations');
  const rd = (f) => fs.readFileSync(path.join(MIG, f), 'utf8').replace(/\r\n/g, '\n');
  const fnOf = (t, start, end) => { const i = t.indexOf(start); assert.ok(i >= 0, start); return t.slice(i, t.indexOf(end, i) + end.length).split('\n'); };
  const m61 = fs.readdirSync(MIG).find((x) => /^\d{4}_master_register_variation_parent\.sql$/.test(x));
  assert.ok(m61, '0061 の migration が無い');
  const now = rd(m61);
  const diffOf = (a, b) => { const added = []; let i = 0; for (const line of b) { if (i < a.length && line === a[i]) i++; else added.push(line.trim()); } assert.equal(i, a.length, '前の版の行が全部同じ順で残っていない'); return added; };
  const canonOld = fnOf(rd('0053_ne_registration_csv.sql'), 'create function ops.ne_reg_canonical(', '\nend $$;\n');
  const canonNew = fnOf(now, 'create or replace function ops.ne_reg_canonical(', '\nend $$;\n');
  canonOld[0] = canonOld[0].replace('create function', 'create or replace function');
  const add = diffOf(canonOld.map((l) => l.replace(/^  select (k\.sku_id.*as parent_code)$/, '  select $1,').replace(/^    if s\.parent_product_id is not null then$/, '    elsif s.parent_product_id is not null then')),
    canonNew);
  assert.deepEqual(add.map((x) => x.slice(0, 24)), ['rp.parent_code as reg_pa', 'left join ops.registrati', '--   🆕 0061: 新商品の登録で選んだ代表', 'if s.reg_parent_code is ', 'v_par := core.norm_code(', 'select c.state, c.ne_cod', 'order by (c.kind = \'rep\')', 'if v_pr.state is distinc', 'else v_parc := v_pr.ne_c'].map((x) => x.slice(0, 24)));
  const openOld = fnOf(rd('0053_ne_registration_csv.sql'), 'create function ops.open_reg_write(', '\nend $$;\n');
  const openNew = fnOf(now, 'create or replace function ops.open_reg_write(', '\nend $$;\n');
  openOld[0] = openOld[0].replace('create function', 'create or replace function');
  assert.deepEqual(diffOf(openOld.map((l) => l.replace("'supplier_deactivate') then", "'supplier_deactivate', 'reg_parent_set') then")), openNew), []);
  const allowOld = fnOf(rd('0054_amazon_sku_maps.sql'), 'create or replace function ops.master_write_allowed(', '\n$$;\n');
  const allowNew = fnOf(now, 'create or replace function ops.master_write_allowed(', '\n$$;\n');
  const addA = diffOf(allowOld.map((l) => l.replace("'DELETE')) as m(op, tbl, act)", "'DELETE'),")), allowNew);
  assert.deepEqual(addA.map((x) => x.slice(0, 40)), ['-- 0061 (新商品の代表 = 色違い・サイズ違い): 登録の代表の表と「作っ', "('reg_parent_set', 'ops.registration_par", "('reg_parent_set', 'ops.ne_reg_exports', "].map((x) => x.slice(0, 40)));
  assert.ok(!/register_new_sku/.test(now), '登録の関数 (並行の PR が直す) は作り直さない');
});

console.log('\n夜間ロード (L)');

await ta('[L1] NE に取り込んだ後の夜間ロード: NE の代表 (名札 GRP1・単品 s005) を写すと Company DB の親 = 登録の代表 (商品の画面は「同じ」)・違えば「違う」', async () => {
  const extra = [skuOf('new-v1', '新しい単品', 'single', 0.1, 300), skuOf('new-v2', '新しい単品', 'single', 0.1, 300), skuOf('new-v0', '新しい単品', 'single', 0.1, 300)];
  // NE の代表商品コードは大文字の GRP1 (名札の norm は grp1 = Company DB の名札 grp1 と同じ)。new-v2 は NE で代表を s006 にしてしまった (選んだ s005 と違う)
  const groups = [{ code: 'GRP1', name: '名札 1', childCodes: ['s006', 's007', 'new-v1'], status: 'active' }, { code: 'grp2', name: '名札 2', childCodes: ['s004'], status: 'active' },
    { code: 's006', name: 'x', childCodes: ['new-v2'], status: 'active' }];
  const r = await runInitialLoad(db, makePlan({ extraSkus: extra, groups }), { log: quiet, runId: 'load_after_ne', now: new Date() });
  assert.equal(r.ok, true, r.error);
  assert.equal(await cdbParentOf('new-v1'), 'grp1', '夜間ロードが NE の代表から Company DB の親を付ける');
  const rp1 = await V.readRegistrationParent(db, await skuId('new-v1'));
  assert.deepEqual([rp1.code, rp1.kind, rp1.cdb_parent, rp1.matches, rp1.ne.ok, rp1.ne.ne_code], ['grp1', 'tag', 'grp1', true, true, 'GRP1']);
  const rp2 = await V.readRegistrationParent(db, await skuId('new-v2'));
  assert.deepEqual([rp2.code, rp2.cdb_parent, rp2.matches], ['s005', 's006', false]);
  const rp0 = await V.readRegistrationParent(db, await skuId('new-v0'));
  assert.deepEqual([rp0.code, rp0.cdb_parent], [null, null]);
});

await ta('[L2] 候補を探す: 名札 (NE にある / 無い)・兄弟の商品コードでその名札・まとまりの無い単品・自分とセットは出さない', async () => {
  const s = (text, o) => as(E, 'master_edit', () => V.searchVariationParents(db, text, o));
  let r = await s('grp');
  const g1 = r.find((x) => x.code === 'grp1'); const g2 = r.find((x) => x.code === 'grp2');
  assert.deepEqual([g1.kind, g1.children, g1.ne.ok, g1.ne.ne_code, g1.disabled], ['tag', 3, true, 'GRP1', null]);
  assert.deepEqual([g2.kind, g2.ne.ok, g2.disabled], ['tag', false, 'NE にまだ無い (NE 登録の CSV を作れない)']);
  r = await s('s007');
  assert.deepEqual(r.map((x) => [x.code, x.via]), [['grp1', 's007']]);
  r = await s('s005');
  assert.deepEqual(r.map((x) => [x.code, x.kind, x.disabled]), [['s005', 'single', null]]);
  r = await s('set0');
  assert.deepEqual(r, []);
  r = await s('new-v9', { self: 'new-v9' });
  assert.ok(!r.some((x) => x.code === 'new-v9'));
  assert.deepEqual(await s('  '), []);
});

console.log('\n画面 (H)');
const MR = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_DECISION_APPROVERS = 'boss@test';
process.env.MASTER_EDIT_OPEN = '1';
MR.__setPgClientFactory(async (url) => {
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
MR.__setClock(() => NOW_MS);
MR.__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s ? { authenticated: true, email: s, displayName: s, role: 'user', allowedApps: ['master-edit'] } : null;
  next();
});
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'naka@test' } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session, Origin: ORIGIN };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
async function scriptsOf(html) {
  const out = [];
  for (const m of html.matchAll(/<script\b([^>]*)><\/script>/gi)) {
    const src = /\bsrc="([^"]+)"/.exec(m[1]); if (!src || !/\/public\//.test(src[1])) continue;
    const r = await fetch(ORIGIN + src[1].replace(/&amp;/g, '&'), { headers: { 'x-test-session': 'naka@test' } });
    assert.equal(r.status, 200, src[1]);
    const t = await r.text(); new vm.Script(t); assert.ok(!/<%|%>/.test(t)); out.push(t);
  }
  return out;
}

try {
  await ta('[H1] 新商品の登録 (単品): 代表の欄 (router を通して描画)・隠しの欄が登録で送る値・画面の JS (文法)・セットには出さない', async () => {
    let r = await call('GET', '/apps/master-edit/new?kind=single');
    assert.equal(r.status, 200, r.text.slice(0, 300));
    assert.match(r.text, /id="vp" data-base="\/apps\/master-edit" data-mode="new" data-can="1"/);
    assert.match(r.text, /<input type="hidden" id="f-variation_parent" value="" data-field="variation_parent" data-dirty-field="variation_parent"/);
    assert.match(r.text, /ほかの商品の色違い・サイズ違い/);
    const js = await scriptsOf(r.text);
    const vj = js.find((x) => x.includes('me-variant.js — 色違い・サイズ違いの代表')) || '';
    for (const api of ["'/api/variation-parents?q='", "'/variation-parent'"]) assert.ok(vj.includes(api), `画面が ${api} を呼んでいない`);
    assert.ok(js.some((x) => x.includes("'inbound_date_managed', 'variation_parent']")), 'me-new.js が代表を登録で送っていない');
    r = await call('GET', '/apps/master-edit/new?kind=set');
    assert.ok(!/id="vp"/.test(r.text) && !/me-variant\.js/.test(r.text), 'セットには代表の欄を出さない');
  });

  await ta('[H2] API: 候補を探す・登録 (values.variation_parent)・商品の画面 (下書き = 選べる / NE 確認済み = 出さない)・代表を保存', async () => {
    let r = await call('GET', '/apps/master-edit/api/variation-parents?q=s006&self=new-h1');
    assert.equal(r.status, 200, r.text);
    assert.deepEqual(r.j.items.map((x) => [x.code, x.via, x.ne.ne_code]), [['grp1', 's006', 'GRP1']]);
    r = await call('POST', '/apps/master-edit/api/new', { body: { request_id: uuid(), kind: 'single', code: 'new-h1', values: single({ variation_parent: 'grp1', cost: undefined }), card: { create: false } } });
    assert.equal(r.status, 200, r.text);
    assert.deepEqual(r.j.variation_parent, { code: 'grp1', kind: 'tag', name: '単品' });
    r = await call('GET', '/apps/master-edit/sku/new-h1');
    assert.equal(r.status, 200, r.text.slice(0, 300));
    assert.match(r.text, /id="vp" data-base="\/apps\/master-edit" data-mode="sku" data-can="1" data-self="new-h1" data-seen="grp1"/);
    assert.match(r.text, /NE にある \(GRP1\)/);
    assert.ok(!/data-field="variation_parent"/.test(r.text), '商品の画面の保存 (saveSku) には混ぜない');
    await scriptsOf(r.text);
    r = await call('POST', '/apps/master-edit/api/sku/new-h1/variation-parent', { body: { request_id: uuid(), seen: 'grp1', parent: 's005' } });
    assert.equal(r.status, 200, r.text);
    assert.equal((await parentRow('new-h1')).parent_code, 's005');
    r = await call('POST', '/apps/master-edit/api/sku/new-h1/variation-parent', { body: { request_id: uuid(), seen: 'grp1', parent: 'grp1' } });
    assert.deepEqual([r.status, r.j.reason], [409, 'version_conflict']);
    r = await call('POST', '/apps/master-edit/api/sku/new-h1/variation-parent', { body: { request_id: uuid(), seen: 's005', parent: 'grp1' }, session: 'someone@test' });
    assert.equal(r.status, 403);
    // NE 確認済み (下書きでない)・代表なし = 欄を出さない / 代表あり = 見るだけ
    r = await call('GET', '/apps/master-edit/sku/new-v0');
    assert.ok(!/id="vp"/.test(r.text));
    r = await call('GET', '/apps/master-edit/sku/new-v1');
    assert.match(r.text, /id="vp" data-base="\/apps\/master-edit" data-mode="sku" data-can="0"/);
    assert.match(r.text, /NE から来た代表 \(夜間の取り込み\) と同じです/);
    r = await call('GET', '/apps/master-edit/sku/s001');
    assert.ok(!/id="vp"/.test(r.text), '前からある商品には出さない');
  });

  await ta('[H3] つかいかた: 色違い・サイズ違いの節', async () => {
    const r = await call('GET', '/apps/master-edit/manual');
    assert.match(r.text, /id="variation"/);
    assert.match(r.text, /選んだ代表 \(色違い・サイズ違い\) が NE に無い/);
  });
} finally {
  server.close();
}

try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows で開いたままのことがある */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.log('NG があります');
