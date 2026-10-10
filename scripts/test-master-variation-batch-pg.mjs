/**
 * test-master-variation-batch-pg.mjs — まとまりの登録の lib (lib/master-variation.mjs・PR-7) を、実 PostgreSQL の独立した接続と本物のログイン (画面のロール master_edit・
 *   画面と同じ statement_timeout 20 秒・lock_timeout 10 秒) で、まとめての登録 → NE 登録の CSV まで通す (PGlite = 1 接続 = 同時・鍵の待ち・時間は試せない)
 *
 * 固定する契約:
 *   1 40 色 × 3 サイズ = 120 子・子ごとに JAN を lib で 1 回の取引に (画面のロールの時間の上限より十分短い・測った値を出す)・子の名前・共通の欄・まとまりの知らせ 1 件
 *   2 同じまとまりに 2 人が lib で同時に足す: 後の人は待って、同じ文字 = 409 option_exists (何も残さない)・違う文字 = 通る (revision は 1 つずつ)
 *   3 同じ request_id を 2 回同時に (押し直し・2 つのタブ): 1 回だけ入る・もう 1 つは前の答え (replayed)
 *   4 1 つの子が断られた (コードが同時にほかの登録で入った) = 全部巻き戻す (札・予約・子・知らせ・done が残らない)・失敗の記録
 *   5 NE 登録の CSV: 回ごとに並ぶ → 1 回目のファイル → 2 回目を登録 → 2 回目のファイル (代表商品コード = まとまりのコード・JAN = empty・行の数)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-variation-batch-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロール master_* をクラスタに作る)。localhost 以外の URL は拒む。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (まとまりの登録の実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-master-variation-batch.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vg7-batch-pg-'));
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const C = await import('../lib/master-cutover.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const V = await import('../lib/master-variation.mjs');
const { BASE_SKUS, RUN1, NOW_MS, MANIFEST, jan13 } = await import('./fixtures/master-variation-db.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const uuid = () => crypto.randomUUID();
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const plan = {
  skus: BASE_SKUS, variationGroups: [{ code: 'ws100', name: 'ウールストール', childCodes: ['ws100-BR', 'ws100-NV', 'ws100-GY'], status: 'active' }],
  setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }, { code: '0034', name: '三河' }], supplierSkus: BASE_SKUS.map((s) => ({ supplierCode: '0001', skuCode: s.code })),
  primarySuppliers: BASE_SKUS.map((s) => ({ skuCode: s.code, supplierCode: '0001' })), reorder: { available: true, runId: 'pml_vg7_pg' },
};
const dbName = `cdb_vg7_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
/** 画面と同じログインの設定 (apps/master-edit/router.mjs の connect) */
const openEditor = async () => { const c = await open('master_edit'); await c.query(`set statement_timeout = '20s'`); await c.query(`set lock_timeout = '10s'`); await c.query(`set idle_in_transaction_session_timeout = '60s'`); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  await createRoles(M, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(M, { pw: PW });
  await W2.useReal0058(M, { leases: ['single', 'set'], futureSetLease: true });
  const [A, B, P, GR, GM] = [await openEditor(), await openEditor(), await open('master_ops'), await open('master_gate_render'), await open('master_gate_minipc')];
  clients.push(A, B, P, GR, GM);
  const dbP = pgAdapter(P);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_vg7_pg', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(MASTER_OWNERSHIP);
  const mh = await C.manifestHashOf(dbM, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy, manual_entries_stopped: [{ id: 'ne.product_screen', by: 't', at: new Date().toISOString() }], drain: { done: true, checked_by: 't', checked_at: new Date().toISOString() } } });
  await acks(ALL_COMPANY, 'frozen');
  await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(dbM, ALL_COMPANY);
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(ALL_COMPANY, 'company_owner');
  const bp = (await P.query('select * from ops.registration_backfill_plan()')).rows[0];
  await P.query('select ops.backfill_sku_registrations($1, $2, $3)', [bp.sku_count, bp.snapshot_hash, 't@test']);
  await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
  await M.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN1, entries: [...BASE_SKUS.map((s) => ({ code_norm: s.code.toLowerCase(), kind: 'product', state: 'ok', ne_code: s.code, spellings: [s.code] })),
    { code_norm: 'ws100', kind: 'rep', state: 'ok', ne_code: 'ws100', spellings: ['ws100'] }] })]);
  await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(dbM, { runId: RUN1, withSet: true });
  assert.equal((await q(`select ops.variation_parent_company() as c`))[0].c, true);

  const RATES = new Map([['A1', { method: 'ネコポス', cost: 280 }]]);
  const VALUES = { standard_price: '2980', cost: { jpy: '1200' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'A1' };
  const reg = (c, input) => V.registerVariationBatch(pgAdapter(c), { actor: 'naka@test', requestId: uuid(), ...input }, { open: true, shippingRates: RATES });
  const counts = async () => (await q(`select (select count(*) from core.products)::int as p, (select count(*) from core.skus)::int as s, (select count(*) from ops.variation_group_codes)::int as c,
    (select count(*) from core.variation_options)::int as o, (select count(*) from ops.product_hub_outbox)::int as ob, (select count(*) from ops.master_edit_requests where status = 'done')::int as r`))[0];
  const colors = Array.from({ length: 40 }, (_, i) => [`色${i + 1}`, `-C${String(i + 1).padStart(2, '0')}`]);
  const sizes = [['S', '-S'], ['M', '-M'], ['L', '-L']];
  const bigInput = (code, { jan = true } = {}) => {
    const children = [];
    let n = 0;
    for (const [hn, hc] of colors) for (const [vn, vc] of sizes) {
      n++;
      const j = jan ? jan13(`4580${String(n).padStart(8, '0')}`) : '';
      children.push({ code: `${code}${hc}${vc}`, choices: { 1: hc, 2: vc }, name: `ルームウェア【${hn}】【${vn}】${j ? `【${j}】` : ''}`, jan: j });
    }
    return { group: { mode: 'new', code, name: 'ルームウェア' }, axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
      options: [...colors.map(([n0, c]) => ({ axis: 1, code: c, name: n0 })), ...sizes.map(([n0, c]) => ({ axis: 2, code: c, name: n0 }))], children, values: VALUES };
  };

  let BIG = null;
  await ta('[1] 40 色 × 3 サイズ = 120 子・子ごとに JAN を lib で 1 回の取引 (画面のロール・statement_timeout 20 秒): 全部入る・時間を測る・まとまりの知らせ 1 件', async () => {
    const before = await counts();
    const t0 = performance.now();
    const r = await reg(A, bigInput('rw120'));
    const ms = performance.now() - t0;
    BIG = r;
    console.log(`      120 子 + JAN 120 = ${Math.round(ms)} ms (画面のロールの statement_timeout 20 秒・取引の合間 60 秒)`);
    assert.ok(ms < 20000, `${ms} ms`);
    assert.equal(r.children.length, 120);
    const after = await counts();
    assert.deepEqual([after.p - before.p, after.s - before.s, after.c - before.c, after.o - before.o, after.ob - before.ob], [121, 120, 1, 43, 1]);
    assert.equal((await q(`select count(*)::int as n from core.external_ids e join core.skus s on s.product_id = e.entity_id and e.entity_type = 'product' where s.code like 'rw120-%' and e.system = 'jan' and e.valid_to is null`))[0].n, 120);
    assert.equal((await q(`select name from core.skus where code = 'rw120-C07-M'`))[0].name, `ルームウェア【色7】【M】【${jan13('458000000020')}】`);
  });

  await ta('[2] 同じまとまりに 2 人が lib で同時に足す: 後の人は待つ → 同じ文字 = 409 option_exists (何も残さない)・違う文字 = 通る (revision は 1 つずつ)', async () => {
    const gid = BIG.group_product_id;
    const add = (c, code, name) => reg(c, { group: { mode: 'add', product_id: gid }, options: [{ axis: 1, code, name }],
      children: sizes.map(([vn, vc]) => ({ code: `rw120${code}${vc}`, choices: { 1: code, 2: vc }, name: `ルームウェア【${name}】【${vn}】` })), values: VALUES });
    // A の取引を先に止めておく (まとまりの鍵を持ったまま) = beforeCommit で待つ
    let release; const gate = new Promise((r) => { release = r; });
    const a = launch(V.registerVariationBatch(pgAdapter(A), { actor: 'naka@test', requestId: uuid(), group: { mode: 'add', product_id: gid }, options: [{ axis: 1, code: '-RD', name: 'レッド' }],
      children: sizes.map(([vn, vc]) => ({ code: `rw120-RD${vc}`, choices: { 1: '-RD', 2: vc }, name: `ルームウェア【レッド】【${vn}】` })), values: VALUES }, { open: true, shippingRates: RATES, beforeCommit: () => gate }));
    await sleep(800);
    const before = await counts();
    const b = launch(add(B, '-rd', 'レッド 2'));
    await sleep(600);
    assert.equal(b.done, false, '前の人の commit まで待つ');
    release();
    const ra = await a.promise; const rb = await b.promise;
    assert.ok(ra.ok, ra.err?.message);
    assert.equal(rb.err?.reason, 'option_exists', rb.err?.message);
    assert.equal((await counts()).s - before.s, 3, 'A の 3 子だけ');
    // 違う文字を同時に = 両方通る
    const [r1, r2] = await Promise.all([add(A, '-BL', 'ブルー'), add(B, '-YE', 'イエロー')]);
    assert.deepEqual([r1.revision, r2.revision].sort(), [BIG.revision + 2, BIG.revision + 3]);
  });

  await ta('[3] 同じ request_id を 2 回同時に (押し直し・2 つのタブ): 1 回だけ入る・もう 1 つは前の答え (replayed)', async () => {
    const rid = uuid();
    const input = { requestId: rid, group: { mode: 'new', code: 'dbl', name: '2 回押し' }, axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-A', name: 'A' }],
      children: [{ code: 'dbl-A', choices: { 1: '-A' }, name: '2 回押し【A】' }], values: VALUES };
    const before = await counts();
    const [x, y] = await Promise.all([reg(A, input), reg(B, input)]);
    assert.equal(x.group_product_id, y.group_product_id);
    assert.deepEqual([!!x.replayed, !!y.replayed].sort(), [false, true]);
    const after = await counts();
    assert.deepEqual([after.p - before.p, after.s - before.s], [2, 1]);
  });

  await ta('[4] 1 つの子が断られた (同じコードの単品をほかの人が先に) = 全部巻き戻す (札・予約・子・知らせ・done が残らない)・失敗の記録', async () => {
    // B が単品 rb-C02 を先に登録する (lib の単品の登録と同じ関数)
    const SUP = (await q(`select supplier_id::text as id from core.suppliers where code = '0001'`))[0].id;
    await B.query('select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r', [uuid(), 'naka@test', null, JSON.stringify(ALL_COMPANY), 'e'.repeat(64), JSON.stringify({
      kind: 'single', code: 'rb-C02', started_at: null, product: { name: '先に', sales_class: 3, expiry_managed: false, inbound_date_managed: null },
      sku: { name: '先に', tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', standard_price_jpy: 1000, shipping_code: null, shipping_method: null, shipping_cost_jpy: null, reorder_months: 1, set_sales_class_override: null, handling_own: null },
      supplier_id: SUP, cost: null, component_request: null, card: null })]);
    const before = await counts();
    const rid = uuid();
    const e = await V.registerVariationBatch(pgAdapter(A), { actor: 'naka@test', requestId: rid, group: { mode: 'new', code: 'rb', name: 'x' }, axes: [{ axis: 1, name: 'カラー' }],
      options: [{ axis: 1, code: '-C01', name: '1' }, { axis: 1, code: '-C02', name: '2' }], children: [{ code: 'rb-C01', choices: { 1: '-C01' }, name: 'x1' }, { code: 'rb-C02', choices: { 1: '-C02' }, name: 'x2' }], values: VALUES },
    { open: true, shippingRates: RATES }).then(() => null, (x) => x);
    assert.equal(e?.reason, 'code_taken', e?.message);
    assert.equal(e.extra.code, 'rb-C02');
    assert.deepEqual(await counts(), before);
    assert.equal((await q('select status from ops.master_edit_requests where request_id = $1', [rid]))[0].status, 'failed');
  });

  await ta('[5] NE 登録の CSV: 回ごとに並ぶ → 1 回目のファイル (120 子 + 足した 9 子は 1 回目と別の回) …まとまりの全部の NE 登録待ちを 1 ファイル / 新しいまとまりは回ごとに 1 ファイル (代表 = まとまりのコード・JAN = empty)', async () => {
    const o = { open: true, nowMs: NOW_MS };
    const s = await G.regSummary(pgAdapter(A), { nowMs: NOW_MS });
    const g = s.variation.find((x) => x.group_code === 'rw120');
    assert.deepEqual(g.rounds.map((r) => r.kids.length), [120, 3, 3, 3]);
    const all = g.rounds.flatMap((r) => r.codes);
    const f = await G.buildRegExport(pgAdapter(A), { actor: 'boss@test', kind: 'products', variation: true, codes: all, requestId: uuid() }, o);
    assert.deepEqual([f.export.schema_version, f.export.row_count], ['ne-reg-variation-v1', 129]);
    const rows = await q('select cells from ops.ne_reg_export_rows where export_id = $1 order by row_no', [f.export.export_id]);
    assert.ok(rows.every((r) => r.cells[7] === 'rw120' && r.cells[8] === 'empty'));
    // 新しいまとまり dbl: 1 回目のファイル → 2 回目を登録 → 2 回目のファイル
    const s1 = await G.regSummary(pgAdapter(A), { nowMs: NOW_MS });
    const d1 = s1.variation.find((x) => x.group_code === 'dbl');
    const f1 = await G.buildRegExport(pgAdapter(A), { actor: 'boss@test', kind: 'products', variation: true, codes: d1.rounds[0].codes, requestId: uuid() }, o);
    await reg(B, { group: { mode: 'add', product_id: d1.group_product_id }, options: [{ axis: 1, code: '-B', name: 'B' }], children: [{ code: 'dbl-B', choices: { 1: '-B' }, name: '2 回押し【B】' }], values: VALUES });
    const s2 = await G.regSummary(pgAdapter(A), { nowMs: NOW_MS });
    const d2 = s2.variation.find((x) => x.group_code === 'dbl');
    assert.deepEqual(d2.rounds.map((r) => [r.no, r.pending]), [[1, 0], [2, 1]]);
    const f2 = await G.buildRegExport(pgAdapter(A), { actor: 'boss@test', kind: 'products', variation: true, codes: d2.rounds[1].codes, requestId: uuid() }, o);
    assert.deepEqual([f1.export.item_count, f2.export.item_count], [1, 1]);
    assert.notEqual(f1.export.export_id, f2.export.export_id);
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} ok`);
