/**
 * test-master-variation-pg.mjs — まとまりの DB (0067・Company DB構想 20 v7 §⑤ / §⑩ の PR-5) を、実 PostgreSQL の独立した接続と本物のログインで確かめる
 *   (PGlite は 1 接続 = 同時の取引・鍵の待ち・デッドロック・ログインのロールの設定 (statement_timeout 20s・lock_timeout 10s) は試せない。PGlite の試験は scripts/test-master-variation.mjs)
 *
 * 固定する契約:
 *   1 同じまとまりに 2 人が同時に足す (中原さんの答え a = 鍵で順番待ち): 後の人は前の人の commit まで待つ → 読み直して、同じ選択肢番号 = option_exists で断る /
 *     違う選択肢 = 通る (revision は 1 つずつ・知らせは revision ごとに 1 つ)
 *   2 鍵の順 (デッドロックしない): まとめての登録 (JAN つき = CSV の鍵まで持つ) と、同じまとまりの名前を直す・子の廃止・NE 登録の CSV を作る・翌朝の照合の確かめ ([2b])・
 *     夜間ロード (マスタの書き込みの排他) が同時に走っても 40P01 にならない (待って順に終わる)
 *   3 1 つの取引で全部か何も無いか (本物のログイン): 子の 1 つが断られた・閉じないで commit = 札・予約・子・知らせ・done が残らない
 *   4 子の数の上限の時間: 2 軸・20 子・子ごとに JAN のまとめての登録を本物の PG で測る = 1 つの文が画面のロールの statement_timeout (20 秒) より十分短く・
 *     取引全体も短い (測った値を出す = 上限を上げるときの材料)
 *   5 本物のログイン: 画面のロールはまとまりの表を直接書けない・部品を実行できない・関数は実行できる
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-variation-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロール master_* をクラスタに作る)。localhost 以外の URL は拒む。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS as OWNED_COLUMNS_FOR_BASE } from '../config/master-ownership.mjs';
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS_FOR_BASE.map((k) => [k, 'load'])));

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (まとまりの実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-master-variation.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const C = await import('../lib/master-cutover.mjs');
const G = await import('../lib/master-reg-csv.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const uuid = () => crypto.randomUUID();
const jan13 = (b) => { const d = b.split('').map(Number).reverse(); const s = d.reduce((a, x, i) => a + x * (i % 2 === 0 ? 3 : 1), 0); return b + ((10 - (s % 10)) % 10); };

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const OWN = JSON.stringify(ALL_COMPANY);
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }] };
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, ...x });
const plan = {
  skus: [sku('p001', '札の子 1【赤】', { representativeCode: 'grp1', representativeState: 'value' }), sku('p002', '札の子 2【青】', { representativeCode: 'grp1', representativeState: 'value' }), sku('p003', '単品')],
  variationGroups: [{ code: 'grp1', name: '札', childCodes: ['p001', 'p002'], status: 'active' }], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [{ supplierCode: '0001', skuCode: 'p003' }], primarySuppliers: [{ skuCode: 'p003', supplierCode: '0001' }],
};
const dbName = `cdb_vg5_${crypto.randomBytes(4).toString('hex')}`;
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
  await createRoles(M, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(M, { pw: PW });
  await W2.useReal0058(M, { leases: ['single', 'set'], futureSetLease: true });
  const [A, B, P, GR, GM] = [await open('master_edit'), await open('master_edit'), await open('master_ops'), await open('master_gate_render'), await open('master_gate_minipc')];
  clients.push(A, B, P, GR, GM);
  const dbP = pgAdapter(P);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_vg5_pg', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(MASTER_OWNERSHIP);
  const mh = await C.manifestHashOf(dbM, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy, manual_entries_stopped: [{ id: 'ne:item-screen', by: 't', at: new Date().toISOString() }], drain: { done: true, checked_by: 't', checked_at: new Date().toISOString() } } });
  await acks(ALL_COMPANY, 'frozen');
  await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(dbM, ALL_COMPANY);
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(ALL_COMPANY, 'company_owner');
  const bp = (await P.query('select * from ops.registration_backfill_plan()')).rows[0];
  await P.query('select ops.backfill_sku_registrations($1, $2, $3)', [bp.sku_count, bp.snapshot_hash, 't@test']);
  await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  // 今日の照合の回と NE の元のコード・許可 (NE 登録の CSV を作る試験 [2] のため)
  const RUN1 = 'mc_20300110T000000000Z_aaaaaa';
  await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
  await M.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN1, entries: [...['p001', 'p002', 'p003'].map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })),
    { code_norm: 'grp1', kind: 'rep', state: 'ok', ne_code: 'grp1', spellings: ['grp1'] }] })]);
  await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(dbM, { runId: RUN1, withSet: true });

  const GRP1 = (await q(`select product_id::text as id from core.products p where display_code = 'grp1' and not exists (select 1 from core.skus k where k.product_id = p.product_id)`))[0].id;
  const SUP1 = (await q(`select supplier_id::text as id from core.suppliers where code = '0001'`))[0].id;
  const TODAY_DB = (await q(`select (now() at time zone 'Asia/Tokyo')::date::text as d`))[0].d;
  const entry = (code, name) => ({
    kind: 'single', code, started_at: null, product: { name, sales_class: 3, expiry_managed: false, inbound_date_managed: null },
    sku: { name, tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', standard_price_jpy: 1500, shipping_code: null, shipping_method: null, shipping_cost_jpy: null,
      reorder_months: 1, set_sales_class_override: null, handling_own: null },
    supplier_id: SUP1, cost: { jpy: 300, source: 'manual', status: 'COMPLETE', valid_from: TODAY_DB, reason: '試験' }, component_request: null, card: null,
  });
  const timed = async (c, sql, p, stat) => { const t0 = performance.now(); try { return (await c.query(sql, p)).rows[0]; } finally { const ms = performance.now() - t0; if (stat) { stat.n++; stat.max = Math.max(stat.max, ms); stat.sum += ms; } } };
  /** まとめての登録の取引を開いたまま返す (commit / rollback は呼び手)。opts.jans = 子のコード → JAN */
  async function openBatch(c, spec, { actor = 'naka@test', rid = uuid(), jans = {}, stat = null, close = true } = {}) {
    await c.query('begin');
    const o = (await timed(c, 'select ops.variation_batch_open($1::uuid, $2, $3, $4::jsonb, $5::jsonb) as r', [rid, actor, null, OWN, JSON.stringify(spec)], stat)).r;
    for (const ch of o.children) {
      const r = (await timed(c, 'select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r', [ch.request_id, actor, null, OWN, 'e'.repeat(64), JSON.stringify(entry(ch.code, `子 ${ch.code}`))], stat)).r;
      if (jans[ch.code]) await timed(c, 'select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb)', [uuid(), actor, null, OWN, r.sku_id, '[]', JSON.stringify([jans[ch.code]])], stat);
    }
    const cl = close ? (await timed(c, 'select ops.variation_batch_close($1::uuid, $2, $3::jsonb, $4::jsonb) as r', [rid, actor, OWN, null], stat)).r : null;
    return { open: o, close: cl, rid };
  }
  const batchTx = async (c, spec, o = {}) => { try { const r = await openBatch(c, spec, o); await c.query('commit'); return r; } catch (e) { try { await c.query('rollback'); } catch { /* */ } throw e; } };
  const revOf = async (gid) => (await q('select revision from ops.variation_group_revisions where group_product_id = $1', [gid]))[0]?.revision ?? 0;
  const counts = async () => (await q(`select (select count(*) from core.products)::int as p, (select count(*) from core.skus)::int as s, (select count(*) from ops.variation_group_codes)::int as c,
    (select count(*) from core.variation_options)::int as o, (select count(*) from ops.product_hub_outbox)::int as ob, (select count(*) from ops.master_edit_requests)::int as r`))[0];

  await ta('[1] 同じまとまりに 2 人が同時に足す: 後の人は前の commit まで待つ → 同じ選択肢番号 = option_exists・違う選択肢 = 通る (revision は 1 つずつ・知らせは revision ごとに 1 つ)', async () => {
    // A が grp1 に初めて軸と -RD を足して、閉じたまま commit しない
    const a = await openBatch(A, { group: { product_id: GRP1 }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-RD', name: '赤' }], children: [{ code: 'grp1-RD', choices: { 1: '-RD' } }] });
    assert.equal(a.close.revision, 1);
    // B が同じまとまりに同じ選択肢番号 (大文字小文字違い) → 待つ
    const b = launch(batchTx(B, { group: { product_id: GRP1 }, options: [{ axis: 1, code: '-rd', name: '赤 2' }], children: [{ code: 'grp1-rd', choices: { 1: '-rd' } }] }));
    await sleep(400);
    assert.equal(b.done, false, '前の人の commit まで待つ');
    await A.query('commit');
    const rb = await b.promise;
    assert.match(String(rb.err?.message), /^option_exists/, rb.err?.message ?? JSON.stringify(rb.ok));
    // 違う選択肢を同時に = 両方通る (revision 2 → 3)
    const a2 = await openBatch(A, { group: { product_id: GRP1 }, options: [{ axis: 1, code: '-BL', name: '青' }], children: [{ code: 'grp1-BL', choices: { 1: '-BL' } }] });
    const b2 = launch(batchTx(B, { group: { product_id: GRP1 }, options: [{ axis: 1, code: '-YE', name: '黄' }], children: [{ code: 'grp1-YE', choices: { 1: '-YE' } }] }));
    await sleep(400);
    assert.equal(b2.done, false);
    await A.query('commit');
    const rb2 = await b2.promise;
    assert.ok(rb2.ok, rb2.err?.message);
    assert.deepEqual([a2.close.revision, rb2.ok.close.revision], [2, 3]);
    assert.deepEqual((await q('select revision from ops.product_hub_outbox where group_product_id = $1 order by revision', [GRP1])).map((r) => r.revision), [1, 2, 3]);
    assert.equal(await revOf(GRP1), 3);
  });

  await ta('[2] 鍵の順 = デッドロックしない: まとめての登録 (JAN つき) と、同じまとまりの名前を直す・子の廃止・NE 登録の CSV を作る・夜間ロードのマスタの書き込みの排他が同時に走っても 40P01 にならない', async () => {
    const rev0 = await revOf(GRP1);
    // A: grp1 に JAN つきで足して閉じたまま (親子・まとまり・コード・CSV の鍵を持つ)
    const a = await openBatch(A, { group: { product_id: GRP1 }, options: [{ axis: 1, code: '-GR', name: '緑' }], children: [{ code: 'grp1-GR', choices: { 1: '-GR' } }] }, { jans: { 'grp1-GR': jan13('490000000201') } });
    // B: 同じまとまりの名前を直す (まとまりの鍵で待つ → A の後の revision を見て version_conflict か通る)
    const lab = launch((async () => { try { return (await B.query('select ops.edit_variation_labels($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7::jsonb) as r', [uuid(), 'naka@test', null, OWN, GRP1, rev0, '{"name":"新しい札"}'])).rows[0].r; } catch (e) { throw e; } })());
    // M (持ち主 = 夜間ロードの形): マスタの書き込みの排他 → 親子の鍵
    const loadLike = launch((async () => { const c = await open(null); clients.push(c); await c.query('begin'); await c.query('select pg_advisory_xact_lock(core.master_write_lock_key())'); await c.query('select pg_advisory_xact_lock(core.parent_lock_key())'); await c.query('commit'); return true; })());
    await sleep(500);
    assert.equal(lab.done, false);
    await A.query('commit');
    const rl = await lab.promise; const rL = await loadLike.promise;
    for (const r of [rl, rL]) assert.notEqual(r.err?.code, '40P01', r.err?.message);
    assert.match(String(rl.err?.message ?? ''), /^version_conflict/);   // A が revision を上げた後 = 見た revision が古い (待って読み直した結果)
    assert.ok(rL.ok);
    // 子の廃止 (SKU → まとまり) と まとめての登録 (まとまり → 新しい子の鍵) を同時に
    const kid = (await q(`select sku_id::text as id from core.skus where code = 'grp1-YE'`))[0].id;
    const a2 = await openBatch(A, { group: { product_id: GRP1 }, options: [{ axis: 1, code: '-PK', name: '桃' }], children: [{ code: 'grp1-PK', choices: { 1: '-PK' } }] });
    const cancel = launch(B.query('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', OWN, kid]));
    await sleep(400);
    assert.equal(cancel.done, false);
    await A.query('commit');
    const rc = await cancel.promise;
    assert.ok(rc.ok, rc.err?.message);
    assert.equal(rc.ok.rows[0].r.revision, a2.close.revision + 1);
    // NE 登録の CSV を作る (SKU → CSV) と JAN つきのまとめての登録 (… → CSV) を同時に
    const a3 = await openBatch(A, { group: { product_id: GRP1 }, options: [{ axis: 1, code: '-WH', name: '白' }], children: [{ code: 'grp1-WH', choices: { 1: '-WH' } }] }, { jans: { 'grp1-WH': jan13('490000000202') } });
    const build = launch(G.buildRegExport(pgAdapter(B), { actor: 'boss@test', kind: 'products', codes: ['grp1-RD', 'grp1-BL', 'grp1-GR', 'grp1-PK'], requestId: uuid(), variation: true },
      { ownership: ALL_COMPANY, open: true, nowMs: new Date('2030-01-10T03:00:00Z').getTime() }));
    await sleep(400);
    assert.equal(build.done, false, 'CSV を作る取引は CSV の鍵で待つ');
    await A.query('commit');
    const rbuild = await build.promise;
    assert.notEqual(rbuild.err?.code, '40P01');
    // A の commit の後に DB の関数が読む = grp1-WH が入った後 = まとまりで 1 ファイルの決まりで grp1-WH が欠ける = variation_incomplete
    assert.equal(rbuild.err?.reason, 'variation_incomplete', rbuild.err?.message);
    assert.ok(a3.close.revision > a2.close.revision);
  });

  await ta('[2b] 鍵の順 = デッドロックしない: まとめての登録 (親子の排他・JAN つき = CSV の鍵) と 翌朝の照合の確かめ (確かめ → SKU → 親子の共有 → CSV) を同時に = 照合が待って終わる', async () => {
    // grp1 の NE 登録待ちの子を全部 1 つのまとまりの版のファイルにして配る (照合の確かめ待ちの品目を作る)
    const kids = (await q(`select s.code from core.skus s join core.products p on p.product_id = s.product_id join ops.master_registrations r on r.sku_id = s.sku_id
       where p.parent_product_id = $1 and r.state = 'draft' order by s.code`, [GRP1])).map((r) => r.code);
    const o = { ownership: ALL_COMPANY, open: true, nowMs: new Date('2030-01-10T03:00:00Z').getTime() };
    const f = await G.buildRegExport(pgAdapter(B), { actor: 'boss@test', kind: 'products', codes: kids, requestId: uuid(), variation: true }, o);
    await G.issueRegExport(pgAdapter(B), { actor: 'boss@test', exportId: f.export.export_id }, o);
    const RUN2 = 'mc_20300110T010000000Z_bbbbbb';
    await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T01:00:00Z', 0)`, [RUN2]);
    const W = await openPgClient((() => { const x = new URL(u.toString()); x.username = 'watch_writer'; x.password = 'b'; return x.toString(); })());
    clients.push(W);
    const snap = (await W.query('select ops.snapshot_ne_reg_targets($1) as r', [RUN2])).rows[0].r;
    assert.ok(snap.targets.length >= kids.length, JSON.stringify(snap.targets));
    const at = new Date(Date.now() + 60000).toISOString();
    const w = (await W.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: RUN2, fetch: { generation_id: 'gen_pg', products_rev: '1', sets_rev: '1', raw_hash: 'c'.repeat(64) },
      products_at: at, sets_at: at, absence_trusted: true, observations: snap.targets.map((t) => ({ code_norm: t.code_norm, present: false, trusted: true, kind: null })) })])).rows[0].r;
    await W.query('select ops.seal_ne_registration_run($1, $2, $3)', [RUN2, w.observation_hash, 'e'.repeat(64)]);
    // A がまとめての登録 (JAN つき) を閉じたまま → 照合の確かめは親子の共有の鍵で待つ → A の commit の後に終わる (40P01 にならない)
    const a = await openBatch(A, { group: { product_id: GRP1 }, options: [{ axis: 1, code: '-NV', name: '紺' }], children: [{ code: 'grp1-NV', choices: { 1: '-NV' } }] }, { jans: { 'grp1-NV': jan13('490000000501') } });
    const chk = launch(W.query('select ops.record_ne_registration_check($1) as r', [RUN2]));
    await sleep(500);
    assert.equal(chk.done, false, '照合の確かめは親子の鍵で待つ');
    await A.query('commit');
    const rc = await chk.promise;
    assert.ok(rc.ok, rc.err?.message);
    assert.deepEqual(rc.ok.rows[0].r.counts, { waiting: kids.length });
    assert.ok(a.close.revision >= 1);
  });

  await ta('[3] 1 つの取引で全部か何も無いか (本物のログイン): 子の 1 つが断られた = 巻き戻す・閉じないで commit = variation_batch_unfinished (札・予約・子・知らせ・done が残らない)', async () => {
    const before = await counts();
    const e1 = await batchTx(A, { group: { code: 'PgHalf', name: '半分' }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-1', name: '1' }, { axis: 1, code: '-2', name: '2' }],
      children: [{ code: 'PgHalf-1', choices: { 1: '-1' } }, { code: 'PgHalf-2', choices: { 1: '-2' } }] }, { jans: { 'PgHalf-1': jan13('490000000301'), 'PgHalf-2': jan13('490000000301') } }).catch((e) => e);
    assert.match(String(e1?.message), /^jan_taken/);
    const e2 = await (async () => { const r = await openBatch(A, { group: { code: 'PgOpen', name: '閉じない' }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-1', name: '1' }],
      children: [{ code: 'PgOpen-1', choices: { 1: '-1' } }] }, { close: false }); try { await A.query('commit'); return r; } catch (e) { return e; } })();
    assert.match(String(e2?.message), /^variation_batch_unfinished/);
    assert.deepEqual(await counts(), before);
  });

  await ta('[4] 子の数の上限の時間: 2 軸・20 子・子ごとに JAN = 1 つの文は statement_timeout (20 秒) より十分短い・取引全体も測る (上限を上げる材料)', async () => {
    const colors = Array.from({ length: 10 }, (_, i) => ({ axis: 1, code: `-C${i}`, name: `色 ${i}` }));
    const sizes = [{ axis: 2, code: '-S', name: 'S' }, { axis: 2, code: '-M', name: 'M' }];
    const children = colors.flatMap((c) => sizes.map((s) => ({ code: `PgBig${c.code}${s.code}`, choices: { 1: c.code, 2: s.code } })));
    assert.equal(children.length, 20);
    const jans = Object.fromEntries(children.map((c, i) => [c.code, jan13(`4900000004${String(i).padStart(2, '0')}`)]));
    const stat = { n: 0, max: 0, sum: 0 };
    const t0 = performance.now();
    const r = await batchTx(A, { group: { code: 'PgBig', name: '大きいまとまり' }, axes: [{ axis: 1, name: '色' }, { axis: 2, name: 'サイズ' }], options: [...colors, ...sizes], children }, { jans, stat });
    const total = performance.now() - t0;
    console.log(`      測った値: 20 子 (2 軸・JAN つき) = 文 ${stat.n} 個・1 文の最大 ${stat.max.toFixed(0)} ms・合計 ${total.toFixed(0)} ms`);
    assert.equal(r.close.revision, 1);
    assert.equal((await q('select jsonb_array_length(payload -> \'children\') as n from ops.product_hub_outbox where group_product_id = $1', [r.close.group_product_id]))[0].n, 20);
    assert.ok(stat.max < 5000, `1 文 ${stat.max} ms (statement_timeout 20 秒の 1/4 より短い)`);
    assert.ok(total < 30000, `取引全体 ${total} ms`);
    // 21 子 = 断る
    const e = await batchTx(A, { group: { code: 'PgBig2', name: 'x' }, axes: [{ axis: 1, name: '色' }], options: Array.from({ length: 21 }, (_, i) => ({ axis: 1, code: `-D${i}`, name: `d${i}` })),
      children: Array.from({ length: 21 }, (_, i) => ({ code: `PgBig2-D${i}`, choices: { 1: `-D${i}` } })) }).catch((x) => x);
    assert.match(String(e?.message), /^too_many/);
  });

  await ta('[5] 本物のログイン: 画面のロールはまとまりの表を直接書けない・部品を実行できない・関数は実行できる (ロールの script を流した後)', async () => {
    const codeOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e.code; } return null; };
    assert.equal(await codeOf(A, `insert into core.variation_axes (group_product_id, axis, name) values ($1, 2, 'x')`, [GRP1]), '42501');
    assert.equal(await codeOf(A, `update ops.variation_group_revisions set revision = 99`), '42501');
    assert.equal(await codeOf(A, `select ops._variation_bump($1, gen_random_uuid(), 'x')`, [GRP1]), '42501');
    assert.equal(await codeOf(A, `select ops.reserve_existing_variation_groups('x')`), '42501');
    assert.equal((await A.query(`select ops.variation_group_code_problem('NewOne') as p, ops.variation_parent_company() as c, ops.variation_max_children() as m`)).rows[0].m, 20);
    assert.ok((await A.query('select count(*)::int as n from ops.variation_group_codes')).rows[0].n >= 1);
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} passed`);
