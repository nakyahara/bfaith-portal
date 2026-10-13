/**
 * test-lz-barcode-pg.mjs — ロジザードのバーコードの「同じ request_id の同時の再送」(#1719 Codex R5 Medium) を、実 PostgreSQL の独立した接続で確かめる
 *   (PGlite は 1 接続 = 鍵で待つ形を試せない。PGlite の試験は scripts/test-lz-barcode.mjs・test-lz-barcode-variation.mjs・test-master-register.mjs)
 *
 * 本物の router を HTTP 越しに通す (接続は要求ごとに master_edit でログイン = 本番と同じ)。3 か所 (単品の登録・まとまりの登録・商品の画面) で同じ形:
 *   1 回目の要求がロジザードの写しを読むところで止まる (barrier) → 写しに同じ値のほかの商品が現れる → 同じ request_id の再送 = request_id の鍵で待つ
 *   → 1 回目が保存して終わる → 再送は保存済みの答え (409 lzbc_taken にならない・2 回書かない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-lz-barcode-pg.mjs
 *   (この PC では C:/tmp/pg-embed の run-lzbarcode.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (ロジザードのバーコードの同時の再送の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lzbarcode2-pg-'));
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const C = await import('../lib/master-cutover.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const { BASE_SKUS, RUN1, MANIFEST } = await import('./fixtures/master-variation-db.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return r; }); return s; };
const uuid = () => crypto.randomUUID();
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const plan = {
  skus: BASE_SKUS, variationGroups: [{ code: 'ws100', name: 'ウールストール', childCodes: ['ws100-BR', 'ws100-NV', 'ws100-GY'], status: 'active' }],
  setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: BASE_SKUS.map((s) => ({ supplierCode: '0001', skuCode: s.code })),
  primarySuppliers: BASE_SKUS.map((s) => ({ skuCode: s.code, supplierCode: '0001' })), reorder: { available: true, runId: 'pml_lzb_pg' },
};
const dbName = `cdb_lzb_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role, extra) => { const c = await openPgClient(role ? roleUrl(role) : u.toString(), extra); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;
let server = null;

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  await createRoles(M, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(M, { pw: PW });
  await W2.useReal0058(M, { leases: ['single', 'set'], futureSetLease: true });
  const [P, GR, GM] = [await open('master_ops'), await open('master_gate_render'), await open('master_gate_minipc')];
  clients.push(P, GR, GM);
  const dbP = pgAdapter(P);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_lzb_pg', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
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
  await (await import('./fixtures/master-widen.mjs')).seedParentGate(dbM, { runId: RUN1 });

  // ─── 本物の router (接続は要求ごとに本物のログイン) ───
  const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
  initMirrorDB();
  const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
  initPurchaseOrders();
  process.env.COMPANY_DB_URL = 'postgres://owner@localhost/lzbpg';
  process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/lzbpg';
  process.env.MASTER_EDITORS = 'naka@test';
  process.env.MASTER_EDIT_OPEN = '1';
  const MR = await import('../apps/master-edit/router.mjs');
  MR.__setPgClientFactory(async (x, extra) => open(/master_edit@/.test(x) ? 'master_edit' : null, extra));
  MR.__setShippingRatesProvider(async () => new Map([['A1', { method: 'ネコポス', cost: 280 }]]));
  MR.__setPhDraftLookup(async () => null);
  // ロジザードの写し: BC = [商品ID, 値] の並び・HOLD = 次の 1 回の読みを止める barrier
  let BC = [];
  let HOLD = null;
  MR.__setLzMirrorProvider(async () => {
    const rows = BC;   // 読んだ時の写し (止まっている間に BC を変えても、1 回目は読んだ時の写しで確かめる)
    if (HOLD) { const hd = HOLD; HOLD = null; hd.entered(); await hd.wait; }
    const at = new Date().toISOString();
    return { ok: true, reason: null, keys: new Set(rows.map(([id]) => id.toLowerCase())), ids: new Map(), list_as_of: at, bc_as_of: at, rows: 1,
      barcodes: rows.reduce((m, [id, v]) => m.set(v, new Set([...(m.get(v) || []), id])), new Map()), bc: { ok: true, reason: null, rows: Math.max(1, rows.length), empty: false } };
  });
  const barrier = () => { let entered, release; const e = new Promise((r) => { entered = r; }); const w = new Promise((r) => { release = r; }); return { entered, wait: w, reached: e, release }; };
  const app = express();
  app.set('view engine', 'ejs');
  app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] }; next(); });
  app.use('/apps/master-edit', MR.default);
  server = http.createServer(app);
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const ORIGIN = `http://127.0.0.1:${server.address().port}`;
  const post = async (p, body) => {
    const r = await fetch(`${ORIGIN}/apps/master-edit${p}`, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json', Origin: ORIGIN }, body: JSON.stringify(body), signal: AbortSignal.timeout(60e3) });
    const text = await r.text();
    let j = null; try { j = JSON.parse(text); } catch { /* */ }
    return { status: r.status, j, text };
  };
  const VALUES = { standard_price: '2980', cost: { jpy: '1200' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'A1' };
  const single = (code, value) => { const b = { request_id: uuid(), kind: 'single', code, values: { ...VALUES, name: `試し ${code}` }, card: { create: false }, logizard_barcode: { kind: 'value', value } }; b.blanks = RB.missingBlankKeys('single', b, { orderApplicable: true }); return b; };
  const variation = (group, value) => {
    const b = { request_id: uuid(), group: { mode: 'new', code: group, name: `試し ${group}` }, axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-A', name: '色A' }, { axis: 1, code: '-B', name: '色B' }],
      children: [{ code: `${group}-A`, choices: { 1: '-A' }, name: `試し ${group}【色A】`, jan: '', lz_barcode: { kind: 'value', value } }, { code: `${group}-B`, choices: { 1: '-B' }, name: `試し ${group}【色B】`, jan: '', lz_barcode: { kind: 'lz_assign' } }],
      values: { ...VALUES }, card: { official_url: '', amazon_url: '', asin: '' } };
    b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
    return b;
  };
  const lzCount = async (value) => (await q('select count(*)::int as n from ops.sku_logizard_barcode_rules where value = $1', [value]))[0].n;

  /** 1 回目を写しの読みで止める → 写しに同じ値のほかの商品 → 同じ request_id の再送は鍵で待つ → 1 回目を進める → 両方 200・再送は保存済みの答え */
  async function concurrentResend(apiPath, body, value) {
    BC = [];
    const hd = barrier();
    HOLD = hd;
    const a = launch(post(apiPath, body));
    await hd.reached;   // 1 回目は request_id の鍵を持ったまま写しの読みで止まっている
    BC = [['zz-new', value]];
    const b = launch(post(apiPath, body));
    await sleep(800);
    assert.equal(b.done, false, '再送が鍵で待たない (1 回目の保存の前に今の写しで確かめた)');
    hd.release();
    const [ra, rb] = [await a.promise, await b.promise];
    assert.equal(ra.status, 200, ra.text);
    assert.equal(rb.status, 200, `再送が保存済みの答えにならない: ${rb.text}`);
    assert.equal(rb.j.replayed, true, rb.text);
    return { ra, rb };
  }

  await ta('[1] 単品の登録: 1 回目が写しの読みで止まっている間の同じ request_id の再送 = 鍵で待ち、保存済みの答え (409 にならない・1 回だけ書く)', async () => {
    const { ra, rb } = await concurrentResend('/api/new', single('lzpg-s1', 'X00LZPG001'), 'X00LZPG001');
    assert.deepEqual([ra.j.code, rb.j.code], ['lzpg-s1', 'lzpg-s1']);
    assert.equal(await lzCount('X00LZPG001'), 1);
    // 鍵の後: 新しい request_id でほかの商品に同じ値 = 409 (今の写し)
    const r3 = await post('/api/new', single('lzpg-s2', 'X00LZPG001'));
    assert.deepEqual([r3.status, r3.j && r3.j.reason], [409, 'lzbc_taken'], r3.text);
  });
  await ta('[2] まとまりの登録: 同じ形 (止まっている間の再送 = 待って保存済みの答え・子は 1 回だけ)', async () => {
    const { rb } = await concurrentResend('/api/new/variation', variation('lzpgv1', 'X00LZPG002'), 'X00LZPG002');
    assert.deepEqual((rb.j.children || []).map((k) => k.code), ['lzpgv1-A', 'lzpgv1-B']);
    assert.equal(await lzCount('X00LZPG002'), 1);
    assert.equal((await q("select count(*)::int as n from core.skus where code like 'lzpgv1-%'"))[0].n, 2);
  });
  await ta('[3] 商品の画面の保存: 同じ形 (止まっている間の再送 = 待って保存済みの答え・行は 1 つ)', async () => {
    const body = { request_id: uuid(), barcode: { kind: 'value', value: 'X00LZPG003' }, seen_row: null };
    const { ra, rb } = await concurrentResend('/api/sku/s001/logizard-barcode', body, 'X00LZPG003');
    assert.deepEqual([ra.j.changed, rb.j.rule_row], [true, ra.j.rule_row]);
    assert.equal(await lzCount('X00LZPG003'), 1);
  });
  // [4] の注: 今は ops.variation_batch_open が親子の鍵 (core.parent_lock_key) を取引の終わりまで持つ = まとまりの登録どうしは先に 1 つずつになる。
  //   値の鍵を先に並べ替えて取る (lockLzValuesInTx) はその上の守り (親子の鍵の取り方が変わっても値の鍵で取り合わない) = 部品そのものは [5] で確かめる
  await ta('[4] まとまりを 2 つ同時に登録 (値の順が [A, B] と [B, A]・#1719 R7): デッドロックしない = 片方 200・もう片方 409 lzbc_taken (何も残さない)', async () => {
    BC = [];
    const A = 'X00LZPGA01', B = 'X00LZPGB01';
    const two = (group, v1, v2) => {
      const b = variation(group, v1);
      b.children[1].lz_barcode = { kind: 'value', value: v2 };
      b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
      return b;
    };
    const waiting = async () => (await q("select count(*)::int as n from pg_locks where locktype = 'advisory' and not granted"))[0].n;
    const until = async (n) => { for (let i = 0; i < 200 && (await waiting()) < n; i++) await sleep(50); assert.ok((await waiting()) >= n, `鍵を待つ要求が ${n} にならない`); };
    // 外で B の鍵を持つ = 2 つの登録を「値の鍵のところ」で重ねる
    const X = await open(null);
    clients.push(X);
    await X.query("select pg_advisory_lock(hashtextextended('ops.lzbc:' || $1::text, 0))", [B]);
    const r2 = launch(post('/api/new/variation', two('lzpgd2', B, A)));   // [B, A]
    await until(1);
    const r1 = launch(post('/api/new/variation', two('lzpgd1', A, B)));   // [A, B]
    await until(2);
    await X.query("select pg_advisory_unlock(hashtextextended('ops.lzbc:' || $1::text, 0))", [B]);
    const res = [await r1.promise, await r2.promise];
    const ok = res.filter((r) => r.status === 200), taken = res.filter((r) => r.status === 409 && r.j && r.j.reason === 'lzbc_taken');
    assert.deepEqual([ok.length, taken.length], [1, 1], res.map((r) => r.text).join(' / '));
    assert.equal(await lzCount(A) + await lzCount(B), 2);
    const lost = taken[0] === res[0] ? 'lzpgd1' : 'lzpgd2';
    assert.equal((await q('select count(*)::int as n from core.skus where code like $1', [`${lost}-%`]))[0].n, 0, '断られた登録の子が残った');
  });
  await ta('[5] 値の鍵を先に取る部品 (lockLzValuesInTx・#1719 R7): 2 つの取引が [B, A] と [A, B] で取っても、並べ替えた順で取る = デッドロックしない (画面のロール)', async () => {
    const LB = await import('../lib/logizard-barcode.mjs');
    const A = 'X00LZPGA05', B = 'X00LZPGB05';
    const waiting = async () => (await q("select count(*)::int as n from pg_locks where locktype = 'advisory' and not granted"))[0].n;
    const until = async (n) => { for (let i = 0; i < 200 && (await waiting()) < n; i++) await sleep(50); assert.ok((await waiting()) >= n, `鍵を待つ取引が ${n} にならない`); };
    const [X, T1, T2] = [await open(null), await open('master_edit'), await open('master_edit')];
    clients.push(X, T1, T2);
    const tx = (c, vals) => (async () => {
      await c.query('begin');
      try { const got = await LB.lockLzValuesInTx(pgAdapter(c), vals); await c.query('commit'); return got; } catch (e) { try { await c.query('rollback'); } catch { /* */ } throw e; }
    })().then((v) => ({ ok: v }), (e) => ({ err: e }));
    await X.query("select pg_advisory_lock(hashtextextended('ops.lzbc:' || $1::text, 0))", [B]);
    const r2 = launch(tx(T2, [B, A]));
    await until(1);
    const r1 = launch(tx(T1, [A, B, A]));
    await until(2);
    await X.query("select pg_advisory_unlock(hashtextextended('ops.lzbc:' || $1::text, 0))", [B]);
    const [a1, a2] = [await r1.promise, await r2.promise];
    assert.ok(!a1.err && !a2.err, `${a1.err?.code} ${a1.err?.message} / ${a2.err?.code} ${a2.err?.message}`);
    assert.deepEqual([a1.ok, a2.ok], [[A, B], [A, B]], '並べ替えた順・重なりは 1 つ');
  });
  await ta('[6] 逆の向きを同時に (#1719 R8 High): ロジザードのバーコードを書く取引と、ほかの商品の JAN を同じ値にする取引 = 後の方が鍵で待ち、先の commit の後に断られる (両方の向き)', async () => {
    const { jan13 } = await import('./fixtures/master-variation-db.mjs');
    const idOf = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id;
    const s1 = await idOf('s001'), s3 = await idOf('s003');
    const latest = async (id) => (await q('select max(rule_row)::text as r from ops.sku_logizard_barcode_rules where sku_id = $1', [id]))[0].r;
    const jansOf = async (id) => (await q(`select e.external_value as v from core.external_ids e join core.skus s on s.product_id = e.entity_id and e.entity_type = 'product' where s.sku_id = $1 and e.system = 'jan' and e.valid_to is null order by 1`, [id])).map((x) => x.v);
    const waiting = async () => (await q("select count(*)::int as n from pg_locks where locktype = 'advisory' and not granted"))[0].n;
    const until = async (n) => { for (let i = 0; i < 200 && (await waiting()) < n; i++) await sleep(50); assert.ok((await waiting()) >= n, '鍵で待たない'); };
    const [L, J] = [await open('master_edit'), await open('master_edit')];
    clients.push(L, J);
    const lzSql = "select ops.set_sku_logizard_barcode($1::uuid, 'naka@test', $2::bigint, 'jan', $3, 'sku_page', $4::bigint, 'checked', now()) as r";
    const janSql = 'select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb) as r';
    const tail = (c, sql, p) => launch((async () => { try { const r = (await c.query(sql, p)).rows[0].r; await c.query('commit'); return { ok: r }; } catch (e) { await c.query('rollback'); return { err: e }; } })());
    // ① ロジザードのバーコード (s001 = X) が先 (まだ commit していない) → s003 の JAN を X に = 待つ → commit の後に断られる
    const X = jan13('458777700401');
    await L.query('begin');
    await L.query(lzSql, [uuid(), s1, X, await latest(s1)]);
    await J.query('begin');
    const seen3 = await jansOf(s3);
    const j = tail(J, janSql, [uuid(), 'naka@test', '試験', JSON.stringify(ALL_COMPANY), s3, JSON.stringify(seen3), JSON.stringify([...seen3, X])]);
    await until(1);
    assert.equal(j.done, false);
    await L.query('commit');
    const rj = await j.promise;
    assert.match(String(rj.err && rj.err.message), /^jan_taken: .*s001/, rj.err ? rj.err.message : JSON.stringify(rj.ok));
    assert.deepEqual(await jansOf(s3), seen3);
    // ② JAN (s003 = Y) が先 (まだ commit していない) → s001 のロジザードのバーコードを Y に = 待つ → commit の後に断られる
    const Y = jan13('458777700402');
    await J.query('begin');
    const seen3b = await jansOf(s3);
    await J.query(janSql, [uuid(), 'naka@test', '試験', JSON.stringify(ALL_COMPANY), s3, JSON.stringify(seen3b), JSON.stringify([...seen3b, Y])]);
    await L.query('begin');
    const l = tail(L, lzSql, [uuid(), s1, Y, await latest(s1)]);
    await until(1);
    assert.equal(l.done, false);
    await J.query('commit');
    const rl = await l.promise;
    assert.match(String(rl.err && rl.err.message), /^lzbc_taken: .*s003/, rl.err ? rl.err.message : JSON.stringify(rl.ok));
  });
} finally {
  if (server) server.close();
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
