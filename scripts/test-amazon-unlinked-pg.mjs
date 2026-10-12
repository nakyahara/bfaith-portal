#!/usr/bin/env node
/**
 * test-amazon-unlinked-pg.mjs — Amazon の出品一覧 (0076 (仮の番号)) と「まだつないでいない SKU」の**同時実行**・**ロールの権限**・**本物の件数の所要時間**を、
 *   実 PostgreSQL の独立した接続で確かめる (PGlite は 1 接続 = test-amazon-unlinked.mjs では書けない分)
 *
 * 接続は本番と同じロールでログインする (create-master-edit-roles.mjs で作る・パスワードは試験の回ごと): 画面 = master_edit / 受け口・夜間ロード = 持ち主
 * 固定する契約:
 *   1 今のデータのある DB (0076 の前 + 夜間ロード) に 0076 を流しても失敗しない・何も変えない (表は空)
 *   2 画面のロールのログイン: 3 つの表を読める・最後に売れた日と隠すの関数を呼べる / 表に直に書けない・記録の表は読めない (42501)
 *   3 受け口が同じ店に 2 つ並んで来る: 後の方は advisory lock が取れず LOCKED (待たない = 送り手が次の朝にやり直す)・先の方は最後まで (行は 1 回分だけ)
 *   4 同じ SKU を 2 人が同時に隠す: 関数の中の鍵で並び、片方は hidden・もう片方は same (印は 1 行・記録は 1 行)
 *   5 対応の保存と隠すの競り合い (#1691 Codex R1 M4): 同じ SKU の鍵で並ぶ。保存が先 = 隠すは保存の commit の後に「つないでいる」で断る (409 linked) / 隠すが先 = 保存は待って最後まで
 *   6 本物の件数 (8,000 行・長い商品名) の入れ替えが 1 要求 = 1 取引で数秒以内 (受け口の statement_timeout 60 秒より十分短い)・まだつないでいない SKU の一覧の読みも数秒以内
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-amazon-unlinked-pg.mjs
 *   (この PC では node C:/tmp/pg-embed/run-amzunlinked.mjs scripts/test-amazon-unlinked-pg.mjs <作業の木> が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { openPgClient, pgAdapter, applyMigrations, DEFAULT_DIR } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-amazon-unlinked.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const F = await import('./fixtures/amazon-unlinked.mjs');
const I = await import('../apps/company-db/ingest/amazon-listings.mjs');
const P = await import('../apps/company-db/push/amazon-listings.mjs');
const U = await import('../apps/master-edit/amazon-unlinked.mjs');
const C = await import('../lib/master-cutover.mjs');
const A = await import('../lib/amazon-map-write.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const MIG = F.migrationsDir();
const BEFORE = F.versionBefore0076(MIG);
const NOW = Date.now();
const TODAY = F.jstToday(NOW);
const PW = Object.fromEntries(['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'].map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual' }] };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const x = { done: false }; x.promise = p.then((r) => { x.done = true; return { ok: r }; }, (e) => { x.done = true; return { err: e }; }); return x; };
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };
const dbName = `cdb_unl_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); c.__role = role; return c; };
/** そのロールの接続を閉じる (master_edit は connection limit 5 = 試験の間に閉じる) */
const endRole = async (role) => { for (const c of clients.filter((x) => x.__role === role)) { try { await c.end(); } catch { /* */ } clients.splice(clients.indexOf(c), 1); } };
const O = await open(null);
const clients = [O];
const body = (rows, gen, run) => ({ shop_code: I.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: run, checksum: I.listingChecksum(rows), row_count: rows.length, skipped_count: 0, rows });
/** 本物の件数: 8,000 行 (FBA 半分・長い商品名・出品日はばらばら) */
function bigRows(n = 8000) {
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push(I.listingRowOf({ seller_sku: `big-${String(i).padStart(5, '0')}`, item_name: `大きな出品一覧の商品 ${i} `.repeat(6).trim(), asin: `B0${String(i).padStart(8, '0')}`, open_date: F.addDays(TODAY, -(i % 900)),
      open_date_raw: `${F.addDays(TODAY, -(i % 900))} 10:00:00 JST`, fulfillment: i % 2 ? 'FBA' : 'FBM', channel_raw: i % 2 ? 'AMAZON_JP' : 'DEFAULT', status: i % 3 ? 'active' : 'inactive', status_raw: i % 3 ? 'Active' : 'Inactive', quantity: i % 2 ? null : 3, price: `${1000 + i}` }));
  }
  return out;
}
try {
  const db = pgAdapter(O);
  await applyMigrations(db, { log: quiet, dir: MIG, to: BEFORE });
  await createMasterEditRoles(O, { pw: PW });
  assert.equal((await runInitialLoad(db, F.makePlan(), { log: quiet, runId: 'load_pg', ownership: MASTER_OWNERSHIP, now: new Date(NOW - 3600e3), host: 'test' })).ok, true);

  await ta('[1] 今のデータのある DB (0076 の前 + 夜間ロード) に 0076 を流しても失敗しない・新しい表は空・出品と構成は変わらない', async () => {
    const snap = async () => (await O.query(`select l.listing_code, count(c.*)::int as comps from core.listings l left join core.listing_components c on c.listing_id = l.listing_id group by 1 order by 1`)).rows;
    const before = await snap();
    await applyMigrations(db, { log: quiet, dir: MIG });
    await createMasterEditRoles(O, { pw: PW });   // 表ができた後に流し直す = 権限が付く
    assert.deepEqual(await snap(), before);
    for (const t of ['ops.amazon_listing_report', 'ops.amazon_listing_report_loads', 'ops.amazon_sku_hidden', 'ops.amazon_sku_hidden_events']) {
      assert.equal((await O.query(`select count(*)::int as n from ${t}`)).rows[0].n, 0, t);
    }
  });
  const parsed = P.parseListingReportTsv(F.buildTsv(F.reportRows(TODAY))).rows;
  assert.equal((await I.ingestAmazonListings(db, body(parsed, NOW - 9 * 3600e3, 'r_pg_1'))).status, 'applied');

  await ta('[2] 画面のロールのログイン: 読める・関数を呼べる / 表に直に書けない・記録の表は読めない (42501)', async () => {
    const M = await open('master_edit'); clients.push(M);
    const mdb = pgAdapter(M);
    const l = await U.listUnlinked(mdb, U.normalizeUnlinkedFilters({ ch: 'all' }, { today: TODAY }), { nowMs: NOW });
    assert.equal(l.state, 'ok'); assert.deepEqual(l.rows.map((r) => r.seller_sku).sort(), ['PR_NOCOMP', 'fba_new1', 'fba_old', 'parent_1'], 'FBA だけ (自社発送 fbm_1 は出ない)');
    assert.equal(l.sold.ok, true);
    const r = await U.setHidden(mdb, { actor: 'naka@test', seller_sku: 'fba_old', hide: true, reason: 'unused', note: null });
    assert.equal(r.status, 'hidden');
    for (const sql of [`insert into ops.amazon_sku_hidden (shop_code, sku_norm, seller_sku, reason, hidden_by) values ('x', 'y', 'y', 'unused', 'z')`, `delete from ops.amazon_sku_hidden`,
      `select * from ops.amazon_sku_hidden_events`, `update ops.amazon_listing_report set item_name = 'x'`, `insert into ops.amazon_listing_report_loads (shop_code, generation, report_run_id, snapshot_at, checksum, row_count, ingest_run_id) values ('x', 1, 'r', now(), repeat('a', 64), 0, 'x')`,
      `select * from core.orders limit 1`]) {
      await assert.rejects(M.query(sql), (e) => e.code === '42501', sql);
    }
    await U.setHidden(mdb, { actor: 'naka@test', seller_sku: 'fba_old', hide: false });
  });

  await ta('[3] 受け口が同じ店に 2 つ並んで来る: 後の方は LOCKED (待たない)・先の方は最後まで', async () => {
    const A = await open(null); const B = await open(null); clients.push(A, B);
    let release; const hold = new Promise((r) => { release = r; });
    let entered; const inA = new Promise((r) => { entered = r; });
    const pa = I.ingestAmazonListings(pgAdapter(A), body(parsed.slice(0, 5), NOW - 8 * 3600e3, 'r_pg_a'), { afterWrite: async () => { entered(); await hold; } });
    await inA;
    await assert.rejects(I.ingestAmazonListings(pgAdapter(B), body(parsed, NOW - 7 * 3600e3, 'r_pg_b')), (e) => e.code === 'LOCKED');
    release();
    assert.equal((await pa).status, 'applied');
    const st = await I.amazonListingsStatus(db);
    assert.deepEqual([st.load.report_run_id, st.rows], ['r_pg_a', 5]);
    // 戻す
    assert.equal((await I.ingestAmazonListings(db, body(parsed, NOW - 7 * 3600e3, 'r_pg_c'))).status, 'applied');
  });

  await ta('[4] 同じ SKU を 2 人が同時に隠す: 関数の中の鍵で並び、片方は hidden・もう片方は same (印も記録も 1 行)', async () => {
    const M1 = await open('master_edit'); const M2 = await open('master_edit'); clients.push(M1, M2);
    await M1.query('begin');
    const r1 = (await M1.query(`select ops.set_amazon_sku_hidden('a@test', 'parent_1', true, 'variation_parent', null) as r`)).rows[0].r;
    assert.equal(r1.status, 'hidden');
    const p2 = M2.query(`select ops.set_amazon_sku_hidden('b@test', 'PARENT_1', true, 'unused', null) as r`);
    let done = false; p2.then(() => { done = true; }, () => { done = true; });
    await new Promise((r) => setTimeout(r, 400));
    assert.equal(done, false, '後の人は鍵で待つ');
    await M1.query('commit');
    const r2 = (await p2).rows[0].r;
    assert.equal(r2.status, 'same'); assert.equal(r2.by, 'a@test');
    assert.equal((await O.query(`select count(*)::int as n from ops.amazon_sku_hidden`)).rows[0].n, 1);
    assert.equal((await O.query(`select count(*)::int as n from ops.amazon_sku_hidden_events where action = 'hide' and sku_norm = 'parent_1'`)).rows[0].n, 1);
  });

  await ta('[5] 対応の保存と隠すの競り合い (#1691 Codex R1 M4): 同じ SKU の鍵で並ぶ。保存が先 = 隠すは保存の commit の後に 409 linked (印は付かない) / 隠すが先 = 保存は待って最後まで', async () => {
    await endRole('master_edit');
    // 切替を開く (本物の権限・本番の関数そのまま = test-amazon-map-pg.mjs と同じ手順)
    await W2.useReal0058(O);
    const [EA, GR, GM, PO, M2, M3] = [await open('master_edit'), await open('master_gate_render'), await open('master_gate_minipc'), await open('master_ops'), await open('master_edit'), await open('master_edit')];
    clients.push(EA, GR, GM, PO, M2, M3);
    const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) }; const dbP = pgAdapter(PO);
    const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(MASTER_OWNERSHIP);
    const mh = await C.manifestHashOf(db, MANIFEST);
    const builds = { render: ['r1'], minipc: ['m1'] };
    const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
    const stamp = () => new Date().toISOString();
    await acks(MASTER_OWNERSHIP, 'legacy_open');
    await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy,
      manual_entries_stopped: [{ id: 'gas:logizard-sheet-and-sku-map', by: 't', at: stamp() }, { id: 'ne:item-screen', by: 't', at: stamp() }], drain: { done: true, checked_by: 't', checked_at: stamp() } } });
    await acks(ALL_COMPANY, 'frozen');
    await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(db, ALL_COMPANY);
    await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
    await acks(ALL_COMPANY, 'company_owner');
    { const p = (await dbP.query('select * from ops.registration_backfill_plan()')).rows[0]; await dbP.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 't@test']); }
    await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
    const save = (cdb, sku, { beforeCommit } = {}) => A.readAmazonMap(db, sku).then((cur) => A.saveAmazonMap(cdb, { actor: 'naka@test', requestId: crypto.randomUUID(), sellerSku: sku, name: `${sku} の対応`,
      components: [{ code: 'a001', qty: 1 }], reason: 'pg', seen: { versions: cur.versions } }, { ownership: ALL_COMPANY, open: true, beforeCommit }));
    const hidden = async (norm) => Number((await O.query('select count(*)::int as n from ops.amazon_sku_hidden where sku_norm = $1', [norm])).rows[0].n);
    // ① 保存が先 (commit の前で止める) → 隠すは鍵で待つ → 保存の commit の後に「つないでいる」で断る
    const g = gate();
    const a = launch(save(pgAdapter(EA), 'fba_new1', { beforeCommit: g.wait }));
    await sleep(400);
    const hd = launch(U.setHidden(pgAdapter(M2), { actor: 'b@test', seller_sku: 'FBA_NEW1', hide: true, reason: 'unused', note: null }));
    await sleep(700);
    assert.equal(a.done, false); assert.equal(hd.done, false, '隠すは保存と同じ SKU の鍵で待つ');
    g.open();
    const ra = await a.promise; const rh = await hd.promise;
    assert.ok(ra.ok, ra.err?.message);
    assert.deepEqual([rh.err?.status, rh.err?.reason], [409, 'linked'], rh.err?.message || 'ok になった');
    assert.equal(await hidden('fba_new1'), 0);
    // ② 隠すが先 (取引を開けたまま) → 保存は鍵で待つ → 隠すの commit の後に最後まで。つないだ SKU は一覧から消える (隠す印が残っても表示だけ)
    await M3.query('begin');
    assert.equal((await M3.query(`select ops.set_amazon_sku_hidden('c@test', 'fba_old', true, 'unused', null) as r`)).rows[0].r.status, 'hidden');
    const s2 = launch(save(pgAdapter(EA), 'fba_old'));
    await sleep(700);
    assert.equal(s2.done, false, '保存は隠すと同じ SKU の鍵で待つ');
    await M3.query('commit');
    const r2 = await s2.promise;
    assert.ok(r2.ok, r2.err?.message);
    const l = await U.listUnlinked(pgAdapter(M2), U.normalizeUnlinkedFilters({ hidden: '1' }, { today: TODAY }), { nowMs: NOW });
    assert.ok(!l.rows.some((r) => r.seller_sku === 'fba_old'), 'つないだ SKU は「隠した SKU」にも出ない');
    // ③ もうつないでいる SKU を直に隠そうとしても断る
    await assert.rejects(M2.query(`select ops.set_amazon_sku_hidden('d@test', 'fba_new1', true, 'unused', null)`), (e) => e.code === 'P0001' && /^linked: /.test(e.message));
  });

  await ta('[6] 本物の件数: 8,000 行の入れ替え (1 要求 = 1 取引) が 20 秒以内・同じ中身の次の夜 = refreshed・一覧の読み (画面のロール) が 5 秒以内', async () => {
    await endRole('master_edit');
    const rows = bigRows();
    const b = body(rows, NOW - 3600e3, 'r_pg_big');
    const bytes = Buffer.byteLength(JSON.stringify(b));
    assert.ok(bytes < P.MAX_BODY_BYTES, `送る中身 ${bytes} バイト`);
    let t = Date.now();
    await O.query(`set statement_timeout = '60s'`);
    const r = await I.ingestAmazonListings(db, b);
    const ms = Date.now() - t;
    assert.equal(r.status, 'applied'); assert.equal(r.rows, 8000);
    assert.ok(ms < 20000, `入れ替え ${ms} ms`);
    t = Date.now();
    assert.equal((await I.ingestAmazonListings(db, body(rows, NOW - 1800e3, 'r_pg_big2'))).status, 'refreshed');
    const msR = Date.now() - t;
    const M = await open('master_edit'); clients.push(M);
    t = Date.now();
    const l = await U.listUnlinked(pgAdapter(M), U.normalizeUnlinkedFilters({ sort: 'sold_desc' }, { today: TODAY }), { nowMs: NOW });
    const msL = Date.now() - t;
    assert.equal(l.state, 'ok'); assert.equal(l.total, 4000, '自社発送 (半分) は出さない'); assert.equal(l.rows.length, U.UNLINKED_PAGE);
    assert.ok(msL < 5000, `一覧 ${msL} ms`);
    console.log(`      (入れ替え ${ms} ms・refreshed ${msR} ms・一覧の読み ${msL} ms・送る中身 ${(bytes / 1048576).toFixed(2)} MB)`);
  });
} finally {
  for (const c of clients.reverse()) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  await admin.end();
  if (MIG !== DEFAULT_DIR) fs.rmSync(MIG, { recursive: true, force: true });
}
console.log(`\n${passed} ok`);
