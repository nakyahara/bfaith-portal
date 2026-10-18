/**
 * test-master-draft-cancel-pg.mjs — 0071 (新商品の下書きを消す) を実 PostgreSQL の独立した接続と本物のログインのロールで確かめる (PGlite は 1 接続 = 競り合いを書けない)
 *
 * 固定する契約:
 *   [R]  本物のログイン: master_edit (画面の lib) = 消せる・保存の記録の db_user = master_edit / 登録の状態・知らせ・JAN の表を直接書けない (42501) /
 *        master_ops・watcher は消す関数を実行できない (watcher は確かめだけ)・master_ops が transition_sku_registration を直に呼んで下書きをやめる = 42501 (Codex #1688 R1 High 1)
 *   [1]  NE 登録の CSV を配る取引と競る: (a) 配るが先 (commit 前) = 消すは SKU の鍵で待ち、配るの commit の後に「一度配った」で断る /
 *        (b) 消すが先 = 配るは SKU の鍵で待ち、消すの commit の後にファイルはもう閉じている (使わない) = 消した商品を NE に配らない
 *   [2]  下書きを構成品にするセットの登録と競る: (a) 消すが先 = セットの登録は待って「使えない商品」で断る / (b) セットの登録が先 = 消すは待って「構成の依頼に入っている」で断る
 *   [3]  カードの取り込みと競る: 消すの取引の間、取り込み (借りる) はその知らせを取らない (skip locked)・commit の後は「済み (作らない)」= 取らない
 *   [4]  同じ番号の押し直しが同時に 2 つ: 後の方は SKU の鍵で待ち、前の commit の後に前の答え (replayed)・登録の状態の履歴は 1 行
 *   [5]  夜間ロード (マスタの書き込みの鍵の排他) の間 = 画面の lib は少し待って 409 nightly_load (何もしない)
 *   [7]  出品の構成の insert が先 (commit 前) = 消すは SKU の行 (外部キーの共有の鍵と並ぶ) で待って「出品の構成」で断る
 *   🆕 [8] (0099) ロジザード登録の CSV を出す取引と競る: (a) 出すが先 (commit 前) = 消すは SKU の鍵で待ち、出すの commit の後に「ロジザード登録の CSV に出しています」で断る /
 *        (b) 消すが先 = 出すは SKU の鍵で待ち、消すの commit の後に「使える商品でない」で断る / (c) 同時に押す = いつも片方だけ・deadlock にならない
 *   [6]  0070 の参照の守り (ops.guard_sku_ref_not_cancelled・#1683) がある DB だけ: 消すが先 = 出品の構成の insert は登録の状態の行で待ち、commit の後に sku_cancelled
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-draft-cancel-pg.mjs
 *   (使い捨ての PG: node C:/tmp/pg-embed/run-deldraft.mjs scripts/test-master-draft-cancel-pg.mjs <リポジトリ>)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロールをクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0071 の実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-master-draft-cancel.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const C = await import('../lib/master-cutover.mjs');
const R = await import('../lib/master-register.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const D = await import('../lib/master-draft-cancel.mjs');

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };
const uuid = () => crypto.randomUUID();
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };

const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }] };
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const NOW_MS = new Date('2030-01-10T03:00:00Z').getTime();
const RUN1 = 'mc_20300110T000000000Z_aaaaaa';
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
const sku = (code, name) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' },
  standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2 });
const plan = { skus: [sku('p001', '単品 1'), sku('p002', '単品 2')], variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [{ supplierCode: '0001', skuCode: 'p001' }], primarySuppliers: [{ skuCode: 'p001', supplierCode: '0001' }] };
const dbName = `cdb_deldraft_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role] ?? (role === 'watcher' ? 'wa' : 'wb'); return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  await createRoles(M, { watcherPw: 'wa', writerPw: 'wb' });
  await createMasterEditRoles(M, { pw: PW });
  await W2.useReal0058(M, { leases: ['single', 'set'], futureSetLease: true });
  const [A, B, Cc, P, GR, GM, WA] = [await open('master_edit'), await open('master_edit'), await open('master_edit'), await open('master_ops'), await open('master_gate_render'), await open('master_gate_minipc'), await open('watcher')];
  clients.push(A, B, Cc, P, GR, GM, WA);
  const [dbA, dbB, dbC, dbP] = [A, B, Cc, P].map(pgAdapter);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_deldraft_pg', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  // 切替 (本物の関数・門の記録は場所ごとのログイン・段階は master_ops) → backfill → new_open
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
  await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
  await M.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN1, entries: ['p001', 'p002'].map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
  const FX = await import('./fixtures/master-widen.mjs');
  await FX.seedNewEntryLease(dbM, { runId: RUN1, withSet: true });
  await FX.seedParentGate(dbM, { runId: RUN1 });

  const draft = (db, code, { card = {}, beforeCommit } = {}) => R.registerNewSku(db, { actor: 'naka@test', requestId: uuid(), kind: 'single', code,
    values: { name: `下書き ${code}`, standard_price: '1980', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', sales_class: '3', expiry_managed: '0', reorder_months: '1', cost: { jpy: '500' } }, card },
  { open: true, shippingRates: RATES, beforeCommit });
  const setDraft = (db, code, comps, { beforeCommit } = {}) => R.registerNewSku(db, { actor: 'naka@test', requestId: uuid(), kind: 'set', code,
    values: { name: `セット ${code}`, standard_price: '3980', shipping_code: 'S01', reorder_months: '1', components: comps.map((c) => ({ code: c, qty: 1 })) }, card: { create: false } },
  { open: true, shippingRates: RATES, beforeCommit });
  const cancel = (db, code, o = {}) => D.cancelSkuDraft(db, { actor: 'naka@test', code, reason: o.reason ?? '入れ間違い', requestId: o.requestId ?? uuid(), confirm: true }, { open: true });
  const sid = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0]?.id;
  const regOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))[0]?.state ?? null;
  /** 画面のロールで関数を直に呼んで、取引を開いたままにする (commit / rollback は呼び手) */
  const holdCancel = async (c, code, rid = uuid(), reason = '入れ間違い') => {
    await c.query('begin');
    return (await c.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint) as r', [rid, 'naka@test', reason, await sid(code)])).rows[0].r;
  };
  for (const c of ['q-r', 'q-1a', 'q-1b', 'q-2a', 'q-2b', 'q-3', 'q-4', 'q-5', 'q-6']) await draft(dbA, c);

  await ta('[R] 本物のログイン: master_edit = 消せる (db_user = master_edit)・表を直接書けない / master_ops・watcher は消せない (watcher は確かめだけ)', async () => {
    const r = await cancel(dbA, 'q-r');
    assert.equal(r.state, 'cancelled');
    assert.equal((await q('select db_user from ops.master_write_sessions where request_id = $1', [r.request_id]))[0].db_user, 'master_edit');
    const id = await sid('q-5');
    for (const sql of ["update ops.master_registrations set state = 'cancelled' where sku_id = $1", "update ops.product_hub_outbox set status = 'done' where sku_id = $1",
      "update core.external_ids set valid_to = now() where entity_id = $1 and system = 'jan'", "update ops.master_registration_events set reason = 'x' where sku_id = $1"]) {
      assert.equal((await errOf(A.query(sql, [id])))?.code, '42501', sql);
    }
    for (const c of [P, WA]) assert.equal((await errOf(c.query('select ops.cancel_sku_draft($1::uuid, $2, $3, $4::bigint)', [uuid(), 'x@test', '理由', id])))?.code, '42501');
    assert.equal((await WA.query('select ops.sku_draft_cancel_check($1::bigint) as r', [id])).rows[0].r.ok, true);
    assert.equal((await errOf(P.query('select ops.sku_draft_cancel_check($1::bigint)', [id])))?.code, '42501');
    // 🆕 Codex #1688 R1 High 1: master_ops (本物のログイン) が ops.transition_sku_registration を直に呼んで下書きをやめる = 42501 (消せる条件と片付けを迂回しない)
    const e = await errOf(P.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'ops@test', '運用でやめる')`, [id]));
    assert.equal(e?.code, '42501', e?.message);
    assert.match(String(e.message), /^draft_cancel_session_required/);
    assert.equal(await regOf('q-5'), 'draft');
  });

  await ta('[1a] 配るが先 (commit 前) = 消すは SKU の鍵で待ち、配るの commit の後に「一度配った」で断る', async () => {
    const f = await G.buildRegExport(dbA, { actor: 'naka@test', kind: 'products', codes: ['q-1a'], requestId: uuid() }, { open: true, nowMs: NOW_MS });
    const hold = gate(); const atCommit = gate();
    const iss = launch(G.issueRegExport(dbA, { actor: 'naka@test', exportId: f.export.export_id, requestId: uuid() }, { open: true, nowMs: NOW_MS, beforeCommit: async () => { atCommit.open(); await hold.wait(); } }));
    await atCommit.wait();
    const c = launch(cancel(dbB, 'q-1a'));
    await sleep(500);
    assert.equal(c.done, false, '消すは配るの commit を待つ');
    hold.open();
    assert.equal((await iss.promise).ok.export.state, 'issued');
    const rc = await c.promise;
    assert.equal(rc.err?.reason, 'not_cancellable', rc.err?.message ?? JSON.stringify(rc.ok));
    assert.match(rc.err.message, /一度配った/);
    assert.equal(await regOf('q-1a'), 'draft');
  });
  await ta('[1b] 消すが先 = 配るは SKU の鍵で待ち、消すの commit の後にファイルはもう閉じている (消した商品を NE に配らない)', async () => {
    const f = await G.buildRegExport(dbA, { actor: 'naka@test', kind: 'products', codes: ['q-1b'], requestId: uuid() }, { open: true, nowMs: NOW_MS });
    await holdCancel(B, 'q-1b');
    const iss = launch(G.issueRegExport(dbA, { actor: 'naka@test', exportId: f.export.export_id, requestId: uuid() }, { open: true, nowMs: NOW_MS }));
    await sleep(500);
    assert.equal(iss.done, false, '配るは消すの commit を待つ');
    await B.query('commit');
    const ri = await iss.promise;
    assert.ok(ri.err ? /閉じて/.test(ri.err.message) : ri.ok.refused === true, ri.err?.message ?? JSON.stringify(ri.ok));
    assert.equal((await q(`select count(*)::int as n from ops.ne_reg_export_items i join core.skus k on k.sku_id = i.sku_id where k.code = 'q-1b' and i.state <> 'superseded'`))[0].n, 0);
    assert.equal((await q('select issued_at from ops.ne_reg_exports where export_id = $1', [f.export.export_id]))[0].issued_at, null);
    assert.equal(await regOf('q-1b'), 'cancelled');
  });

  await ta('[2a] 消すが先 = その下書きを構成品にするセットの登録は待って「使えない商品」で断る', async () => {
    await holdCancel(B, 'q-2a');
    const s = launch(setDraft(dbA, 'qs-2a', ['q-2a']));
    await sleep(500);
    assert.equal(s.done, false, 'セットの登録は消すの commit を待つ');
    await B.query('commit');
    const rs = await s.promise;
    assert.ok(rs.err, `登録できてしまった: ${JSON.stringify(rs.ok)}`);
    assert.match(`${rs.err.reason} ${rs.err.message}`, /cancelled|やめた|使えない/);
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'qs-2a'"))[0].n, 0);
  });
  await ta('[2b] セットの登録が先 (commit 前) = 消すは待って「構成の依頼に入っている」で断る', async () => {
    const hold = gate(); const atCommit = gate();
    const s = launch(setDraft(dbA, 'qs-2b', ['q-2b'], { beforeCommit: async () => { atCommit.open(); await hold.wait(); } }));
    await atCommit.wait();
    const c = launch(cancel(dbB, 'q-2b'));
    await sleep(500);
    assert.equal(c.done, false, '消すはセットの登録の commit を待つ');
    hold.open();
    assert.equal((await s.promise).ok.state, 'draft');
    const rc = await c.promise;
    assert.equal(rc.err?.reason, 'not_cancellable', rc.err?.message ?? JSON.stringify(rc.ok));
    assert.match(rc.err.message, /構成の依頼/);
  });

  await ta('[3] 消すの取引の間、カードの取り込みはその知らせを取らない (skip locked)・commit の後は「済み (作らない)」で取らない', async () => {
    const id = await sid('q-3');
    await holdCancel(B, 'q-3');
    const t0 = Date.now();
    const got = (await A.query("select event_id from ops.claim_card_events('t-3', 'manual', null, $1::bigint, 5, 120, 5)", [id])).rows;
    assert.deepEqual([got.length, Date.now() - t0 < 3000], [0, true], '待たずに 0 件');
    await B.query('commit');
    assert.equal((await A.query("select event_id from ops.claim_card_events('t-3', 'manual', null, $1::bigint, 5, 120, 5)", [id])).rows.length, 0);
    const o = (await q("select status, result ->> 'outcome' as outcome from ops.product_hub_outbox where sku_id = $1 and kind = 'card_create'", [id]))[0];
    assert.deepEqual([o.status, o.outcome], ['done', 'cancelled']);
  });

  await ta('[4] 同じ番号の押し直しが同時に 2 つ = 後の方は SKU の鍵で待って前の答え (replayed)・履歴は 1 行', async () => {
    const rid = uuid();
    await holdCancel(B, 'q-4', rid);
    const c = launch(cancel(dbC, 'q-4', { requestId: rid }));
    await sleep(500);
    assert.equal(c.done, false);
    await B.query('commit');
    const rc = await c.promise;
    assert.equal(rc.ok?.replayed, true, rc.err?.message ?? JSON.stringify(rc.ok));
    assert.equal((await q(`select count(*)::int as n from ops.master_registration_events where sku_id = $1 and to_state = 'cancelled'`, [await sid('q-4')]))[0].n, 1);
  });

  await ta('[5] 夜間ロード (マスタの書き込みの鍵の排他) の間 = 画面の lib は少し待って 409 nightly_load (何もしない)', async () => {
    await M.query('begin');
    await M.query('select pg_advisory_xact_lock(core.master_write_lock_key())');
    try {
      const e = await errOf(cancel(dbA, 'q-5'));
      assert.equal(e?.reason, 'nightly_load', e?.message);
    } finally { await M.query('rollback'); }
    assert.equal(await regOf('q-5'), 'draft');
    assert.equal((await cancel(dbA, 'q-5')).state, 'cancelled');
  });

  await ta('[7] 出品の構成の insert が先 (commit 前) = 消すは SKU の行で待ち、insert の commit の後に「出品の構成に使われている」で断る (確かめの後に参照が増えない)', async () => {
    await draft(dbA, 'q-7');
    await M.query('begin');
    const lid = (await M.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', 'r-q7', 'active') returning listing_id`)).rows[0].listing_id;
    await M.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, (select sku_id from core.skus where code = 'q-7'), 1, 0, 'exact', 'system')`, [lid]);
    const c = launch(cancel(dbB, 'q-7'));
    await sleep(500);
    assert.equal(c.done, false, '消すは出品の構成の commit を待つ');
    await M.query('commit');
    const rc = await c.promise;
    assert.equal(rc.err?.reason, 'not_cancellable', rc.err?.message ?? JSON.stringify(rc.ok));
    assert.match(rc.err.message, /出品の構成/);
    assert.equal(await regOf('q-7'), 'draft');
  });

  // 🆕 [8] (0099) ロジザード登録の CSV を出す取引 (ops.lz_new_export_issue) と競る = 同時でも片方だけが通る
  /** 画面のロールで: バーコード = 空欄 を入れて (別の取引)、CSV に出す品目を返す */
  const lzPrep = async (c, code) => {
    const id = await sid(code);
    const b = (await c.query("select ops.set_sku_logizard_barcode($1::uuid, $2, $3::bigint, 'lz_assign', null, 'sku_page', null, null, null) as r", [uuid(), 'naka@test', id])).rows[0].r;
    return JSON.stringify([{ sku_id: id, code, rule_row: String(b.rule_row), kind: 'lz_assign', value: null, expiry_code: '01', inbound_code: '0' }]);
  };
  const LZ_ISSUE = 'select ops.lz_new_export_issue($1::uuid, $2, $3::jsonb, now(), null) as r';
  const lzIssued = async (code) => (await q(`select count(*)::int as n from ops.lz_new_export_items i join core.skus k on k.sku_id = i.sku_id where k.code = $1 and i.state = 'issued'`, [code]))[0].n;
  await ta('[8a] (0099) CSV を出すが先 (commit 前) = 消すは SKU の鍵で待ち、出すの commit の後に「ロジザード登録の CSV に出しています (ファイル #N)」で断る', async () => {
    await draft(dbA, 'q-8a');
    const items = await lzPrep(A, 'q-8a');
    let c = null;
    try {
      await A.query('begin');
      const r = (await A.query(LZ_ISSUE, [uuid(), 'naka@test', items])).rows[0].r;
      c = launch(cancel(dbB, 'q-8a'));
      await sleep(500);
      assert.equal(c.done, false, '消すは CSV を出すの commit を待つ');
      await A.query('commit');
      const rc = await c.promise;
      assert.equal(rc.err?.reason, 'not_cancellable', rc.err?.message ?? JSON.stringify(rc.ok));
      assert.match(rc.err.message, new RegExp(`ロジザード登録の CSV に出しています \\(ファイル #${r.export_id}\\)`));
    } finally {
      // 途中で落ちても開いた取引を残さない (後の試験に aborted を渡さない)・相手の終わりを待つ
      try { await A.query('rollback'); } catch { /* commit の後 = 何もしない */ }
      if (c) await c.promise;
    }
    assert.deepEqual([await regOf('q-8a'), await lzIssued('q-8a')], ['draft', 1]);
  });
  await ta('[8b] (0099) 消すが先 (commit 前) = CSV を出すは SKU の鍵で待ち、消すの commit の後に「使える商品でない」で断る (やめた下書きをロジザードに出さない)', async () => {
    await draft(dbA, 'q-8b');
    const items = await lzPrep(A, 'q-8b');
    let iss = null;
    try {
      await holdCancel(B, 'q-8b');
      iss = launch(A.query(LZ_ISSUE, [uuid(), 'naka@test', items]));
      await sleep(500);
      assert.equal(iss.done, false, 'CSV を出すは消すの commit を待つ');
      await B.query('commit');
      const ri = await iss.promise;
      assert.match(String(ri.err?.message ?? JSON.stringify(ri.ok)), /^invalid_input: q-8b は単品で使える商品でない/);
    } finally {
      try { await B.query('rollback'); } catch { /* commit の後 = 何もしない */ }
      if (iss) await iss.promise;
    }
    assert.deepEqual([await regOf('q-8b'), await lzIssued('q-8b')], ['cancelled', 0]);
  });
  await ta('[8c] (0099) 同時に押す (4 回) = いつもどちらか片方だけが通る (両方通る・両方断る = 無い)・待ちの輪 (deadlock) にならない', async () => {
    for (let i = 0; i < 4; i++) {
      const code = `q-8c${i}`;
      await draft(dbA, code);
      const items = await lzPrep(A, code);
      const [ri, rc] = await Promise.all([launch(A.query(LZ_ISSUE, [uuid(), 'naka@test', items])).promise, launch(cancel(dbB, code)).promise]);
      for (const x of [ri.err, rc.err]) if (x) assert.doesNotMatch(String(x.message), /deadlock/i);
      const issued = !ri.err, cancelled = !rc.err;
      assert.equal(issued !== cancelled, true, `${code}: 出す ${issued ? '通った' : ri.err.message} / 消す ${cancelled ? '通った' : rc.err.message}`);
      assert.deepEqual([await regOf(code), await lzIssued(code)], issued ? ['draft', 1] : ['cancelled', 0]);
      if (!cancelled) assert.match(rc.err.message, /ロジザード登録の CSV に出しています/);
      else assert.match(ri.err.message, /使える商品でない/);
    }
  });

  const hasRefGuard = (await q("select to_regprocedure('ops.guard_sku_ref_not_cancelled()') is not null as ok"))[0].ok;
  if (!hasRefGuard) console.log('  ⏭️ [6] 0070 の参照の守り (ops.guard_sku_ref_not_cancelled) が無い DB = 飛ばす (#1683 のマージの後に流す)');
  else {
    await ta('[6] (0070 の参照の守り) 消すが先 = 出品の構成の insert は登録の状態の行で待ち、commit の後に sku_cancelled', async () => {
      await holdCancel(B, 'q-6');
      const ins = launch((async () => {
        await M.query('begin');
        try {
          const lid = (await M.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', 'r-q6', 'active') returning listing_id`)).rows[0].listing_id;
          await M.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, (select sku_id from core.skus where code = 'q-6'), 1, 0, 'exact', 'system')`, [lid]);
          await M.query('commit');
        } catch (e) { await M.query('rollback'); throw e; }
      })());
      await sleep(500);
      assert.equal(ins.done, false, '出品の構成は消すの commit を待つ');
      await B.query('commit');
      const ri = await ins.promise;
      assert.match(String(ri.err?.message), /sku_cancelled/);
    });
  }
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
