/**
 * test-master-outbox-ns-pg.mjs — product-hub の知らせの名前空間 (0066・Company DB構想 20 v7 §⑤ / §⑩ の PR-3) を、実 PostgreSQL の独立した接続と本物のログインで確かめる
 *   (PGlite は 1 接続 = 同時の取引・鍵の待ち・ログインのロールは書けない。PGlite の試験は scripts/test-master-outbox-ns.mjs)
 *
 * 固定する契約:
 *   1 移行の間: 0065 までの DB で画面のロール (本物のログイン master_edit) が登録 → 知らせ (pending・借りている途中の行も) → 0066 を流す →
 *     前からの行は entity sku・revision 1・借りの持ち主のまま = 借りた人が結果を書ける・今の借りる関数で残りも取り込める・0066 の後の登録も今までどおり
 *   2 まとまりの revision の同時: 同じまとまりの知らせを 2 つの取引が同時に書く = まとまりの鍵で後の取引は待つ・前が commit した後に
 *     同じ / 小さい revision は断る (group_revision_not_newer)・大きい revision は通る
 *   3 名前空間つきで借りる関数を 2 つの接続が同時に呼ぶ = 同じ知らせは 1 つの接続だけが借りる (skip locked)・今の借りる関数はまとまりの知らせを借りない
 *   4 本物のログイン: 画面のロールは知らせを直接足せない・部品を実行できない・security definer の関数を通してもまとまりの知らせは書けない (PR-5 まで)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-outbox-ns-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロール master_* をクラスタに作る)。localhost 以外の URL は拒む。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS as OWNED_COLUMNS_FOR_BASE } from '../config/master-ownership.mjs';
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS_FOR_BASE.map((k) => [k, 'load'])));

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (知らせの名前空間の実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-master-outbox-ns.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const R = await import('../lib/master-register.mjs');
const O = await import('../lib/product-hub-outbox.mjs');
const C = await import('../lib/master-cutover.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const codeOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e.code; } return null; };

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const NOW = new Date();
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }] };
const manualStopped = () => [{ id: 'ne:item-screen', by: 't', at: new Date().toISOString() }];
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, ...x });
const plan = {
  skus: [sku('p001', '子 1', { representativeCode: 'grp1', representativeState: 'value' }), sku('p002', '子 2', { representativeCode: 'grp1', representativeState: 'value' }), sku('p003', '単品')],
  variationGroups: [{ code: 'grp1', name: '札', childCodes: ['p001', 'p002'], status: 'active' }], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
};
const dbName = `cdb_vg3_${crypto.randomBytes(4).toString('hex')}`;
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
  await applyMigrations(dbM, { log: () => {}, to: '0065' });   // 🚨 0066 の前の形から
  await createMasterEditRoles(M, { pw: PW });
  await W2.useReal0058(M, { leases: ['single', 'set'], futureSetLease: true });
  const [A, B, P, GR, GM, O2] = [await open('master_edit'), await open('master_edit'), await open('master_ops'), await open('master_gate_render'), await open('master_gate_minipc'), await open(null)];
  clients.push(A, B, P, GR, GM, O2);
  const [dbA, dbB, dbP] = [A, B, P].map(pgAdapter);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const reg = (db, code) => R.registerNewSku(db, {
    actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code,
    values: { name: `新商品 ${code}`, standard_price: '1000', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', sales_class: '3', expiry_managed: '0', reorder_months: '1' }, card: {},
  }, { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES });
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_vg3_pg', now: new Date(Date.now() - 5 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(MASTER_OWNERSHIP);
  const mh = await C.manifestHashOf(dbM, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy, manual_entries_stopped: manualStopped(), drain: { done: true, checked_by: 't', checked_at: new Date().toISOString() } } });
  await acks(ALL_COMPANY, 'frozen');
  await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(dbM, ALL_COMPANY);
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(ALL_COMPANY, 'company_owner');
  const bp = (await P.query('select * from ops.registration_backfill_plan()')).rows[0];
  await P.query('select ops.backfill_sku_registrations($1, $2, $3)', [bp.sku_count, bp.snapshot_hash, 't@test']);
  await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(dbM, { withSet: true });

  const created = (ev) => ({ outcome: 'created', draft_id: Number(ev.sku_id) });
  const grp1 = (await q(`select product_id::text as id from core.products where display_code = 'grp1'`))[0].id;

  await ta('[1] 移行の間: 0065 の DB で登録 (本物のログイン) → 借りている途中に 0066 → 借りた人が結果を書ける・残りも今の関数で取り込める・0066 の後の登録も今までどおり', async () => {
    const a = await reg(dbA, 'pg-a1');
    const b = await reg(dbA, 'pg-a2');
    // A が pg-a1 を借りたまま (結果はまだ書かない)
    const lease = (await A.query(`select event_id::text as id from ops.claim_card_events('vg3-a', 'auto', $1::uuid, null, 1, 600, 5)`, [a.card.event_id])).rows;
    assert.equal(lease.length, 1);
    await applyMigrations(dbM, { log: () => {} });
    assert.equal((await q(`select count(*)::int as n from ops.schema_migrations where version = '0066'`))[0].n, 1);
    await createMasterEditRoles(M, { pw: PW });   // ロールの流し直し (claim_outbox_events の実行)
    const rows = await q(`select sku_id::text as sid, entity_kind, entity_id::text as eid, revision, status, lease_owner from ops.product_hub_outbox order by created_at`);
    assert.deepEqual(rows.map((r) => [r.sid, r.entity_kind, r.eid, r.revision, r.status, r.lease_owner]),
      [[a.sku_id, 'sku', a.sku_id, 1, 'pending', 'vg3-a'], [b.sku_id, 'sku', b.sku_id, 1, 'pending', null]]);
    assert.equal((await A.query(`select ops.finish_card_event($1::uuid, 'vg3-a', 'done', '{"outcome":"created"}'::jsonb, null) as ok`, [a.card.event_id])).rows[0].ok, true);
    const res = await O.runCardOutbox(dbB, created, {});
    assert.deepEqual(res.map((r) => [r.sku_id, r.status, r.recorded]), [[b.sku_id, 'done', true]]);
    const c = await reg(dbA, 'pg-a3');
    assert.deepEqual((await q('select entity_kind, entity_id::text as eid, revision from ops.product_hub_outbox where sku_id = $1', [c.sku_id]))[0], { entity_kind: 'sku', eid: c.sku_id, revision: 1 });
    assert.deepEqual((await O.runCardOutbox(dbA, created, {})).map((r) => [r.sku_id, r.status]), [[c.sku_id, 'done']]);
  });

  async function snap(gid, revision) {
    const g = (await q(`select p.product_id::text as pid, p.display_code, p.name from core.products p where p.product_id = $1`, [gid]))[0];
    const kids = await q(`select s.sku_id::text as sku_id, s.code, s.name, s.standard_price_jpy::text as price from core.products c join core.skus s on s.product_id = c.product_id and s.sku_kind = 'single'
      where c.parent_product_id = $1 order by s.sku_id`, [gid]);
    return { schema: 'ph-group-v1', revision, created_by: 'naka@test', group: { product_id: g.pid, sku_id: null, code: g.display_code, name: g.name, kind: 'tag' }, axes: [], options: [],
      children: kids.map((k) => ({ sku_id: k.sku_id, code: k.code, name: k.name, price: k.price == null ? null : Number(k.price), choices: {}, jans: [] })), cancelled_children: [],
      common: { shipping: null, amazon_url: null, asin: null, official_url: null, reference_urls: [], yahoo: null } };
  }
  const enq = (c, rev) => c.query('select ops.enqueue_group_snapshot($1, $2, $3::jsonb, gen_random_uuid(), $4)', [grp1, rev, JSON.stringify(snap.cache[rev]), 'naka@test']);
  snap.cache = {};
  for (const rev of [1, 2, 3, 4, 5]) snap.cache[rev] = await snap(grp1, rev);
  const revs = async () => (await q('select revision from ops.product_hub_outbox where group_product_id = $1 order by revision', [grp1])).map((r) => r.revision);

  await ta('[2] 同じまとまりの知らせを 2 つの取引が同時に書く: 後の取引はまとまりの鍵で待つ・前の commit の後に同じ / 小さい revision は断る・大きい revision は通る', async () => {
    // (a) 同じ revision 1
    await O2.query('begin');
    await enq(O2, 1);
    let s = launch(enq(M, 1));
    await sleep(500);
    assert.equal(s.done, false, '後の取引はまとまりの鍵で待つ');
    await O2.query('commit');
    let r = await s.promise;
    assert.match(String(r.err?.message), /group_revision_not_newer/);
    // (b) 前が 3・後が 2 (小さい)
    await O2.query('begin');
    await enq(O2, 3);
    s = launch(enq(M, 2));
    await sleep(500);
    assert.equal(s.done, false);
    await O2.query('commit');
    r = await s.promise;
    assert.match(String(r.err?.message), /group_revision_not_newer/);
    // (c) 前が 4・後が 5 (大きい) = 待ってから通る
    await O2.query('begin');
    await enq(O2, 4);
    s = launch(enq(M, 5));
    await sleep(500);
    assert.equal(s.done, false);
    await O2.query('commit');
    r = await s.promise;
    assert.equal(r.err, undefined, String(r.err?.message));
    // (d) 前が巻き戻した (rollback) = 後の取引の同じ revision は通る (失敗した取引では revision は残らない)
    snap.cache[6] = await snap(grp1, 6);
    await O2.query('begin');
    await enq(O2, 6);
    s = launch(enq(M, 6));
    await sleep(500);
    assert.equal(s.done, false);
    await O2.query('rollback');
    r = await s.promise;
    assert.equal(r.err, undefined, String(r.err?.message));
    assert.deepEqual(await revs(), [1, 3, 4, 5, 6]);
  });

  await ta('[3] 名前空間つきで借りる関数を 2 つの接続 (画面のロール) が同時に呼ぶ = 同じ知らせは 1 つだけが借りる・今の借りる関数はまとまりの知らせを借りない', async () => {
    assert.equal((await A.query(`select 1 from ops.claim_card_events('vg3-x', 'manual', null, null, 200, 60, 5)`)).rows.length, 0);
    await A.query('begin');
    await B.query('begin');
    const ra = (await A.query(`select event_id::text as id, revision from ops.claim_outbox_events('vg3-a', 'auto', 'variation_group', null, null, 3, 60, 5)`)).rows;
    const rb = (await B.query(`select event_id::text as id, revision from ops.claim_outbox_events('vg3-b', 'auto', 'variation_group', null, null, 10, 60, 5)`)).rows;
    await A.query('commit');
    await B.query('commit');
    assert.equal(ra.length, 3); assert.equal(rb.length, 2);
    assert.equal(new Set([...ra, ...rb].map((x) => x.id)).size, 5);
    assert.deepEqual([...ra, ...rb].map((x) => x.revision).sort((x, y) => x - y), [1, 3, 4, 5, 6]);
    for (const [c, who, rows] of [[A, 'vg3-a', ra], [B, 'vg3-b', rb]]) {
      for (const x of rows) assert.equal((await c.query(`select ops.finish_card_event($1::uuid, $2, 'done', '{"outcome":"created"}'::jsonb, null) as ok`, [x.id, who])).rows[0].ok, true);
    }
    assert.deepEqual((await q(`select distinct status from ops.product_hub_outbox where group_product_id = $1`, [grp1])).map((r) => r.status), ['done']);
  });

  await ta('[4] 本物のログイン: 画面のロールは知らせを直接足せない・部品と確かめを実行できない (42501)・security definer の関数を通してもまとまりの知らせは書けない', async () => {
    snap.cache[7] = await snap(grp1, 7);
    const p = JSON.stringify(snap.cache[7]);
    assert.equal(await codeOf(A, `insert into ops.product_hub_outbox (company_id, group_product_id, kind, schema_version, payload, payload_hash, revision, request_id, created_by)
      values (1, $1, 'group_snapshot', 'ph-group-v1', $2::jsonb, $3, 7, gen_random_uuid(), 'naka@test')`, [grp1, p, O.groupPayloadHash(snap.cache[7])]), '42501');
    assert.equal(await codeOf(A, 'select ops.enqueue_group_snapshot($1, 7, $2::jsonb, gen_random_uuid(), $3)', [grp1, p, 'naka@test']), '42501');
    assert.equal(await codeOf(A, 'select ops.group_snapshot_problem(1::smallint, $1, 7, $2::jsonb)', [grp1, p]), '42501');
    assert.equal(await codeOf(P, `select * from ops.claim_outbox_events('vg3-p', 'auto', 'sku')`), '42501');
    await M.query(`create function public.vg3_pg_enqueue(p_gid bigint, p_rev integer, p jsonb) returns uuid language sql security definer set search_path = pg_catalog, pg_temp as
      $$ select ops.enqueue_group_snapshot(p_gid, p_rev, p, gen_random_uuid(), 'naka@test') $$`);
    await M.query('grant execute on function public.vg3_pg_enqueue(bigint, integer, jsonb) to master_edit');
    let e = null;
    try { await A.query('select public.vg3_pg_enqueue($1, 7, $2::jsonb)', [grp1, p]); } catch (x) { e = x; }
    assert.equal(e?.code, '42501'); assert.match(e.message, /group_snapshot_session_required/);
    await M.query('select public.vg3_pg_enqueue($1, 7, $2::jsonb)', [grp1, p]);   // 持ち主は書ける
    assert.deepEqual(await revs(), [1, 3, 4, 5, 6, 7]);
  });

  await ta('[5] 選択肢名の重なりは NFKC でそろえて見る (実 PostgreSQL の normalize・UTF8)・lib と同じ答え', async () => {
    assert.equal((await q('show server_encoding'))[0].server_encoding, 'UTF8');
    const base = { ...snap.cache[1], axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-WH', name: 'ホワイト', sort: 0 }, { axis: 1, code: '-BK', name: 'ﾎﾜｲﾄ', sort: 1 }] };
    for (const [p, want] of [[base, 'option_name_dup'], [{ ...base, options: [base.options[0], { ...base.options[1], name: 'ブラック' }] }, null],
      [{ ...base, options: [base.options[0], { ...base.options[1], name: '　ホワイト　' }] }, 'option_name_dup']]) {
      assert.equal(O.groupSnapshotShapeProblem(p), want);
      assert.equal((await q('select ops.group_snapshot_shape_problem($1::jsonb) as p', [JSON.stringify(p)]))[0].p, want);
    }
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
