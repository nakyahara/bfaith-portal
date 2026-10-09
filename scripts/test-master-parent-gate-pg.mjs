/**
 * test-master-parent-gate-pg.mjs — 0068 (代表 (親) の数えと門・AI_reference CompanyDB構想/20 v7 §②・§⑩ PR-6) を実 PostgreSQL の独立した接続と本物のロールで確かめる
 *
 * 固定する契約:
 *   [R]  ロール: watcher = 生の数え・門の状態を読める / 記録・判定の本体は呼べない (42501)・watch_writer = 記録できる・master_edit = どれも呼べない
 *   [W1] products.parent を足す試み: 止める手の入口 = ne:item-screen・照合 ② の代表の数えの記録が無い = 断る
 *   [W2] widen の判定 (読むだけの判定と apply が同じ答え): 記録の 6 つの数えが 0 でない・prepared のロードの前の記録・材料の世代が違う・NE の取得が手の入口の停止の前・
 *        全部の商品の 2 段 / 循環 (その場で数える) = 断る / 揃えば widen が通る (active の products.parent = company)
 *   [W3] 広げた後 = 門が効く: 今朝の照合の回の数えが無い = 新しい NE 登録の CSV を作らない / 同じ回の数えが 0 = 作れる (別の接続・本物の trigger)
 *   [K]  sku_kind・Amazon だけの試みは代表の記録を見ない (今までどおり)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-parent-gate-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロールをクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles as createWatchRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import * as OS from '../apps/company-db/load/ownership-state.mjs';
import * as W from '../apps/company-db/load/widen-state.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { ownershipHash, recordLegacyGateAckV2 } from '../lib/master-cutover.mjs';
import { forceNewOpen, fakeLoad, seedParentGate, hex, ZERO_GATE } from './fixtures/master-widen.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0068 の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const errOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e; } return null; };

const KEY_A = 'listing_components.amazon', KEY_K = 'skus.sku_kind', KEY_P = 'products.parent';
const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', [KEY_K]: 'company', [KEY_A]: 'company' };   // 10/9 の本番の形 (代表は load)
const WIDEN_P = { ...BASE, [KEY_P]: 'company' };
const MANIFEST = { schema: 'test', entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual', owner_cols: ['skus.name', KEY_P] },
  { id: 'ne:set-kind', kind: 'manual', owner_cols: [KEY_K] }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual', owner_cols: [KEY_A] }] };
const CAPABLE = [KEY_A, KEY_K, KEY_P, 'skus.name', 'products.name'];
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const PLAN = { skus: ['p01', 'p02', 'p03', 'p04'].map(single), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] };
const GEN = 'mat_20301010T000000000Z_aaaaaaaa_aaaaaa';   // fakeLoad の材料の世代 (既定)
const runId = () => `mc_${new Date().toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
async function setupDb(base = BASE) {
  const name = `cdb_vg6_${crypto.randomBytes(4).toString('hex')}`;
  await admin.query(`create database ${name}`);
  dbNames.push(name);
  const u = new URL(url); u.pathname = `/${name}`;
  const roleUrl = (role, pw) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
  const open = async (role) => { const c = await openPgClient(role ? roleUrl(role, PW[role] ?? WPW[role]) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); clients.push(c); return c; };
  const O = await open(null);
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: PW });
  await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  const [WA, WW, ME, GR, GM] = [await open('watcher'), await open('watch_writer'), await open('master_edit'), await open('master_gate_render'), await open('master_gate_minipc')];
  const r0 = await runInitialLoad(dbO, PLAN, { log: () => {}, runId: `vg6_load_${name}`, host: 'test' });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, base);
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const acks = async (prepared = null, active = base) => {
    for (const [host, inst] of [['render', 'r-a'], ['minipc', 'm-a']]) {
      await recordLegacyGateAckV2(dbGate[host], { host, instanceId: inst, buildId: 'b1', manifest: MANIFEST, ownership: active, phaseSeen: 'new_open',
        activeHashSeen: (await q('select active_hash from ops.master_ownership_state'))[0].active_hash, preparedHashSeen: prepared, capable: CAPABLE });
    }
  };
  await acks();
  return { O, dbO, WA, WW, ME, dbWA: pgAdapter(WA), q, acks };
}
async function readyAttempt(E, { widen, stops }) {
  const a = await W.prepareWiden(E.dbO, { companyId: 1, map: widen, loaderFingerprint: hex('f'), manifest: MANIFEST, actor: 't' });
  for (const entryId of stops) await W.recordWidenManualStop(E.dbO, { attemptId: a.widen_prepare_id, entryId, stoppedBy: '中原' });
  const last = (await E.q('select max(stopped_at) as t from ops.master_widen_manual_stops where widen_prepare_id = $1', [a.widen_prepare_id]))[0].t;
  const stopAt = new Date(last ?? a.prepared_at);
  await E.acks(ownershipHash(widen));
  const at = new Date(stopAt.getTime() + 1000).toISOString();
  const recovery = await fakeLoad(E.dbO, { epoch: 'active', hash: ownershipHash(BASE), completeAt: at });
  const prepared = await fakeLoad(E.dbO, { epoch: 'prepared', hash: ownershipHash(widen), completeAt: at });
  return { ...a, id: a.widen_prepare_id, stopAt, recovery, prepared, ev: { load_commit_seq: prepared.commitSeq, build_id: 'b1', generation_id: 'g1', generation_no: 1 } };
}
const check = (E, id) => W.widenCheck(E.dbWA, { attemptId: id, companyId: 1 });
const has = (r, re) => r.problems.some((x) => re.test(x));
/** 取引の中で mutate してから、読むだけの判定と apply の答えを比べる (apply は拒まれる = 取引ごと巻き戻す) */
async function variant(E, id, label, mutate, re) {
  await E.O.query('begin');
  try {
    await mutate(E.O);
    const r = (await E.O.query('select ops.widen_check_readonly($1::uuid, 1) as r', [id])).rows[0].r;
    assert.equal(r.ok, false, `${label}: 通ってしまった`);
    assert.ok(has(r, re), `${label}: ${JSON.stringify(r.problems)}`);
    await E.O.query('savepoint s');
    const e = await errOf(E.O, "select ops.widen_master_ownership($1::uuid, 1, 't', $2::jsonb)", [id, JSON.stringify({ load_commit_seq: '0', build_id: 'b', generation_id: 'g' })]);
    assert.ok(e && /widen_rejected/.test(e.message), `${label}: apply ${e?.message}`);
    assert.deepEqual(JSON.parse(e.detail).problems, r.problems, `${label}: 読むだけの判定と apply の答えが違う`);
    await E.O.query('rollback to savepoint s');
  } finally { await E.O.query('rollback'); }
}
/** 照合 ② の記録の形の行を直接置く (取引の中・試験だけ: 印を立てて)。over = 列の上書き */
const putRecord = async (c, over) => {
  await c.query("select set_config('ops.parent_gate_protocol', '1', true)");
  const f = { compare_run_id: `${runId()}_x`, ne_generation_id: 'ne_x', ne_raw_hash: hex('1'), products_complete_at: new Date().toISOString(), setproducts_complete_at: new Date().toISOString(),
    material_generation_id: GEN, evidence_sha256: hex('2'), obs_hash: hex('3'),
    counts: JSON.stringify({ parent_mismatch: 0, parent_incomparable: 0, parent_ambiguous: 0, parent_missing: 0, parent_two_level: 0, parent_loop: 0 }), counted: 4, excluded: '{}', samples: '{}',
    owner_at_record: 'load', recorded_by: 't', ...over };
  const cols = Object.keys(f);
  await c.query(`insert into ops.master_parent_gate_results (${cols.join(', ')}) values (${cols.map((k, i) => `$${i + 1}`).join(', ')})`, cols.map((k) => f[k]));
};
const mirrorObs = async (E) => {
  const rows = await E.q(`select k.code_norm, nullif(core.norm_code(pp.display_code), '') as rep, pp.display_code as raw from core.skus k
    left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id where k.sku_kind = 'single' order by 1`);
  return { format: 'parent-obs-v1', complete: true, untrusted: [], rows: rows.map((r) => [r.code_norm, 'single', 'ok', r.rep ?? null, r.rep ? r.raw : null]) };
};

try {
  const E = await setupDb();

  await ta('[R] ロール: watcher = 生の数え・門の状態を読める・記録と判定の本体は 42501 / watch_writer = 記録できる (数えは DB) / master_edit = どれも呼べない', async () => {
    const obs = await mirrorObs(E);
    const r = (await E.WA.query('select ops.parent_raw_gate(1, $1::jsonb, true) as r, ops.parent_gate_state() as g', [JSON.stringify(obs)])).rows[0];
    assert.equal(r.r.counted, 4); assert.deepEqual(r.r.items, []); assert.equal(r.g.enforced, false);
    const f = { generation_id: 'ne_r', raw_hash: hex('1'), products_complete_at: new Date(Date.now() - 60000).toISOString(), setproducts_complete_at: new Date(Date.now() - 60000).toISOString() };
    const rec = (c, run) => c.query('select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb) as r', [run, JSON.stringify(f), GEN, hex('2'), JSON.stringify(obs)]);
    for (const [c, label] of [[E.WA, 'watcher'], [E.ME, 'master_edit']]) {
      const e = await errOf(c, 'select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb)', [runId(), JSON.stringify(f), GEN, hex('2'), JSON.stringify(obs)]);
      assert.equal(e?.code, '42501', `${label}: ${e?.message}`);
    }
    for (const sql of ["select ops._widen_judge(gen_random_uuid(), 1)", 'select ops._parent_gate_problems()', 'select ops.parent_structure_counts(1)']) {
      assert.equal((await errOf(E.WA, sql))?.code, '42501', sql);
    }
    for (const sql of ["select ops.parent_raw_gate(1, '{}'::jsonb)", 'select ops.parent_gate_state()']) assert.equal((await errOf(E.ME, sql))?.code, '42501', `master_edit: ${sql}`);
    // 表を直接は書けない (watch_writer も・印を立てても権限が無い)
    assert.equal((await errOf(E.WW, "select set_config('ops.parent_gate_protocol', '1', false); insert into ops.master_parent_gate_results (compare_run_id) values ('x')"))?.code, '42501');
    const w = (await rec(E.WW, runId())).rows[0].r;
    assert.deepEqual([w.owner, w.counted, Object.values(w.counts).every((n) => n === 0)], ['load', 4, true]);
    assert.equal((await E.q('select recorded_by from ops.master_parent_gate_results order by result_id desc limit 1'))[0].recorded_by, 'watch_writer');
  });

  let AT;
  await ta('[W1] products.parent を足す試み: 止める手の入口 = ne:item-screen・prepared のロードの後の照合 ② の代表の数えの記録が無い = 断る', async () => {
    AT = await readyAttempt(E, { widen: WIDEN_P, stops: ['ne:item-screen'] });
    assert.deepEqual(AT.added_keys, [KEY_P]);
    assert.deepEqual(AT.required_manual_entries, ['ne:item-screen']);
    const r = await check(E, AT.id);
    assert.equal(r.ok, false);
    // [R] で残した記録は prepared のロードの前 = 使えない
    assert.ok(has(r, /parent_gate: 代表の数えの記録 .* が prepared のロード .* の前/), JSON.stringify(r.problems));
    assert.ok(has(r, /parent_gate: 照合の NE の取得 .* が手の入口の停止 .* の前/), JSON.stringify(r.problems));
    assert.deepEqual(r.counts.parent_structure, { two_level: 0, loop: 0 });
    assert.ok(!has(r, /amazon_map|decisions|shape/), `ほかのキーの検査で止まらない: ${JSON.stringify(r.problems)}`);
  });

  await ta('[W2] widen の判定: 6 つの数え・prepared のロードの前・材料の世代・停止の前の取得・構造 = 断る (読むだけと apply が同じ) / 揃えば widen が通る', async () => {
    // 照合 ② (封をした回) = prepared のロードの後・同じ材料の世代・停止の後の取得・NE = Company DB (数え 0)。本番 = run.mjs が watch_writer で
    const f = { generation_id: 'ne_w2', raw_hash: hex('1'), products_complete_at: new Date().toISOString(), setproducts_complete_at: new Date().toISOString() };
    const run = runId();
    await E.WW.query('select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb)', [run, JSON.stringify(f), GEN, hex('2'), JSON.stringify(await mirrorObs(E))]);
    const r = await check(E, AT.id);
    assert.equal(r.ok, true, JSON.stringify(r.problems));
    assert.equal(r.counts.parent_gate_compare_run, run); assert.equal(r.counts.parent_counted, 4);
    assert.deepEqual(r.counts.parent_raw, { parent_mismatch: 0, parent_incomparable: 0, parent_ambiguous: 0, parent_missing: 0, parent_two_level: 0, parent_loop: 0 });
    // 断る形 (取引の中で新しい記録を置く = 一番新しい記録が判定の材料)
    await variant(E, AT.id, '数えが 0 でない', (c) => putRecord(c, { counts: JSON.stringify({ parent_mismatch: 2, parent_incomparable: 0, parent_ambiguous: 0, parent_missing: 1, parent_two_level: 0, parent_loop: 0 }) }),
      /parent_raw: 代表のずれが 0 でない \(parent_mismatch 2・parent_missing 1/);
    await variant(E, AT.id, '数えの形が違う (5 つ)', (c) => putRecord(c, { counts: JSON.stringify({ parent_mismatch: 0, parent_incomparable: 0, parent_ambiguous: 0, parent_missing: 0, parent_two_level: 0 }) }),
      /parent_raw: 代表のずれが 0 でない \(数えの形が違う/);
    await variant(E, AT.id, '材料の世代が違う', (c) => putRecord(c, { material_generation_id: 'mat_other' }), /parent_gate: 照合が読んだ材料の世代 \(mat_other\) が prepared のロードの材料の世代/);
    await variant(E, AT.id, '停止の前の取得', (c) => putRecord(c, { products_complete_at: new Date(AT.stopAt.getTime() - 1000).toISOString() }), /parent_gate: 照合の NE の取得 .* が手の入口の停止/);
    await variant(E, AT.id, 'prepared のロードの前の記録', (c) => putRecord(c, { created_at: new Date(Date.now() - 3600000).toISOString() }), /parent_gate: 代表の数えの記録 .* が prepared のロード .* の前/);
    await variant(E, AT.id, '2 段 (その場で数える)', async (c) => {
      await c.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
      const t1 = (await c.query("insert into core.products (company_id, display_code, name) values (1, 'g1', 'g1') returning product_id")).rows[0].product_id;
      const t2 = (await c.query("insert into core.products (company_id, display_code, name) values (1, 'g2', 'g2') returning product_id")).rows[0].product_id;
      await c.query("update core.products set parent_product_id = $1, parent_set_by = 'manual' where product_id = $2", [t2, t1]);
      await c.query("update core.products set parent_product_id = $1, parent_set_by = 'manual' where product_id = (select product_id from core.skus where code = 'p01')", [t1]);
    }, /parent_structure: 2 段の商品 2・循環の商品 0/);
    // 揃った = widen が通る (active の products.parent = company)
    const w = await W.widenOwnership(E.dbO, { attemptId: AT.id, companyId: 1, actor: '中原', evidence: AT.ev });
    assert.deepEqual([w.widened, w.added_keys], [true, [KEY_P]]);
    assert.equal((await OS.readOwnershipState(E.dbO)).active.map[KEY_P], 'company');
    assert.equal((await E.WA.query('select ops.parent_gate_state() as g')).rows[0].g.enforced, true);
  });

  await ta('[W3] 広げた後 = 門が効く (本物の trigger・画面のロールの取引でなく持ち主の直接の書き込みでも): 今朝の新商品の許可の回の数えが無い = 作らない / 同じ回の数え 0 = 作れる', async () => {
    await E.q("select set_config('ops.registration_protocol', '1', false)");
    await E.q("update ops.master_registrations set state = 'draft', state_changed_by = 't' where sku_id = (select sku_id from core.skus where code = 'p04')");
    await E.q("select set_config('ops.registration_protocol', '', false)");
    const mk = () => E.O.query(`with e as (insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
        values ('products', 'ne-reg-single-v2', 'syohin_code', 'utf8', false, 1, 1, repeat('a', 64), repeat('b', 64), repeat('c', 64), '\\x00', gen_random_uuid(), 'x', current_date, 't') returning export_id)
      insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by)
      select e.export_id, k.sku_id, k.code_norm, k.code, k.sku_kind, repeat('e', 64), '{}'::jsonb, repeat('f', 64), 1, 1, 't' from e, core.skus k where k.code = 'p04' returning item_id`);
    const run = runId();
    await E.WW.query('select ops.close_new_entry_for_compare($1)', [run]);
    await E.WW.query('select ops.record_new_entry_gate($1, $2, $2, $3::jsonb)', [run, new Date(Date.now() - 1000).toISOString(), JSON.stringify(ZERO_GATE)]);
    const e1 = await mk().then(() => null, (e) => e);
    assert.match(e1?.message ?? '', /parent_gate_closed: .*parent_gate_other_run/);
    await seedParentGate(E.dbO, { runId: run });
    const ok = await mk();
    assert.equal(ok.rows.length, 1);
    const g = (await E.WA.query('select ops.parent_gate_state() as g')).rows[0].g;
    assert.deepEqual([g.enforced, g.open, g.compare_run_id], [true, true, run]);
  });

  await ta('[K] sku_kind・Amazon だけの試みは代表の記録を見ない (今までどおり)・数えに parent の数を出さない', async () => {
    const base0 = { ...BASE, [KEY_A]: 'load' };
    const B = await setupDb(base0);
    const a = await W.prepareWiden(B.dbO, { companyId: 1, map: { ...base0, [KEY_A]: 'company' }, loaderFingerprint: hex('f'), manifest: MANIFEST, actor: 't' });
    const r = await check(B, a.widen_prepare_id);
    assert.equal(r.ok, false);
    assert.ok(!has(r, /parent_/), JSON.stringify(r.problems));
    assert.equal(r.counts.parent_raw, undefined); assert.equal(r.counts.parent_structure, undefined);
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB を消せない ${n}: ${e.message}`); } }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
