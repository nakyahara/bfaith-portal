/**
 * test-amazon-map-pg.mjs — Amazon SKU の対応の編集 (0054・lib/amazon-map-write.mjs) の**同時実行**と**ロールの権限**を、実 PostgreSQL の独立した接続で確かめる
 *   (PGlite は 1 接続なので書けない。Company DB構想 16 §7 v2 H5・§8 契約 v3。PR ⑦-1)
 *
 * 接続は本番と同じロールでログインする (create-master-edit-roles.mjs で作る・パスワードは試験の回ごと):
 *   保存 (A・B) = master_edit / 段階 (P) = master_ops / 門 (GR・GM) / 持ち主 (O・O2) = 夜間ロード・migration・ほかの処理の代わり
 * 固定する契約:
 *   1 今のデータのある DB (0053 まで + 夜間ロード) に 0054 を流しても失敗しない・何も変わらない (実 PostgreSQL)
 *   2 同じ seller SKU を 2 人が同じ画面から保存: 後の人は出品ごとの鍵で待ち、前の人の commit の後に 409 (版が違う)。両方は書かない
 *   3 同じ request_id が 2 つ並んで来る (押し直し): 後の方は request_id の鍵で待ち、前の結果をそのまま返す (記録は 1 行)
 *   4 夜間ロードが先 = 保存はマスタの書き込みの鍵で短く待って 409 nightly_load → ロードは最後まで (対応の構成は変えない)
 *   5 保存が先 = 夜間ロードは鍵で待ち、保存の commit の後に最後まで (対応の出品の構成は触らない)
 *   6 墓標 × 夜間ロードの自動の候補 (FBM の完全一致): どちらが先でも、墓標の出品に構成を作り直さない
 *   7 画面のロールのログイン: 対応・構成・出品を直接書けない・消せない・部品の関数を呼べない (42501)
 *   8 不変条件は commit のときに効く (実 PostgreSQL の deferred の constraint trigger)・UTC / 東京の session で写しのハッシュが同じ
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-amazon-map-pg.mjs
 *   (この PC では C:/tmp/pg-embed の run-conc.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { MASTER_OWNERSHIP } from '../config/master-ownership.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の同時実行の試験は飛ばす。PGlite の試験は scripts/test-amazon-map.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const C = await import('../lib/master-cutover.mjs');
const A = await import('../lib/amazon-map-write.mjs');
const K = await import('../lib/sku-map-canonical.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };

const AMZ = 'main@A1VC38T7YXB528';
const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual' }] };
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const dbName = `cdb_am_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };

function makePlan() {
  const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
  const amazon = (listingCode, comps, evidenceSource) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title: listingCode, status: 'active', components: comps, asinCandidates: [], fnskuCandidates: [], evidenceSource });
  const fbm = (code) => [{ code, qty: 1, resolution: 'exact', evidence: { source: 'fbm_ne_code' } }];
  return {
    skus: ['p001', 'p002', 'p003', 'p004', 'p005'].map(single),
    variationGroups: [], setComponents: [],
    listings: [
      amazon('pm_1', [{ code: 'p001', qty: 1, sortOrder: 0, resolution: 'imported', evidence: { source: 'm_sku_master' } }, { code: 'p002', qty: 1, sortOrder: 3, resolution: 'imported', evidence: { source: 'm_sku_master' } }], 'mirror_sku_master'),
      amazon('p003', fbm('p003'), 'amazon_fees_fbm'),
      amazon('p004', fbm('p004'), 'amazon_fees_fbm'),
      amazon('p005', fbm('p005'), 'amazon_fees_fbm'),
    ],
    observations: [], physicals: [], compliance: [], workers: [], suppliers: [], supplierSkus: [],
  };
}

const O = await open(null);
const clients = [O];
try {
  const dbO = pgAdapter(O);
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const loadWith = (db, runId, ownership = ALL_COMPANY) => runInitialLoad(db, makePlan(), { log: () => {}, runId, ownership, now: new Date('2030-01-05T03:00:00Z') });
  const snap = async () => ({
    comps: await q(`select l.listing_code, k.code, c.qty, c.sort_order, c.resolution from core.listing_components c join core.listings l on l.listing_id = c.listing_id join core.skus k on k.sku_id = c.sku_id order by 1, 2`),
    events: Number((await q('select count(*)::int as n from events.master_change_events'))[0].n),
    listings: await q('select listing_code, version::text as v from core.listings order by 1'),
  });

  await ta('[1] 今のデータのある DB (0053 まで + 夜間ロード・並びの隙間・FBM の完全一致) に 0054 を流す: 失敗しない・何も変わらない・その後のロードも同じ', async () => {
    await applyMigrations(dbO, { log: () => {}, to: '0053' });
    const r = await loadWith(dbO, 'load_pg_0', MASTER_OWNERSHIP);
    assert.equal(r.ok, true, r.error);
    const before = await snap();
    assert.ok(before.comps.some((c) => c.listing_code === 'pm_1' && c.sort_order === 3));
    const res = await applyMigrations(dbO, { log: () => {} });
    assert.deepEqual(res.applied, ['0054', '0055']);   // 0055 = ④a の持ち主の epoch (表を足すだけ = ここも何も変えない)
    assert.deepEqual(await snap(), before);
    const r2 = await loadWith(dbO, 'load_pg_1', MASTER_OWNERSHIP);
    assert.equal(r2.ok, true, r2.error); assert.equal(r2.summary.listing_components.applied, 0);
    assert.deepEqual(await snap(), before);
  });

  await createMasterEditRoles(O, { pw: PW });
  const [EA, EB, GR, GM, P, O2] = [await open('master_edit'), await open('master_edit'), await open('master_gate_render'), await open('master_gate_minipc'), await open('master_ops'), await open(null)];
  clients.push(EA, EB, GR, GM, P, O2);
  const [dbA, dbB, dbP] = [EA, EB, P].map(pgAdapter);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  // 切替を開く (本物の権限・本番の関数そのまま)
  const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(MASTER_OWNERSHIP);
  const mh = await C.manifestHashOf(dbO, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  const stamp = () => new Date().toISOString();
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy,
    manual_entries_stopped: [{ id: 'gas:logizard-sheet-and-sku-map', by: 't', at: stamp() }, { id: 'ne:item-screen', by: 't', at: stamp() }], drain: { done: true, checked_by: 't', checked_at: stamp() } } });
  await acks(ALL_COMPANY, 'frozen');
  await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(dbO, ALL_COMPANY);   // 0055 (④a): 段階の持ち主表 = 持ち主の epoch (本番 = ④a の activate)
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(ALL_COMPANY, 'company_owner');
  { const p = (await dbP.query('select * from ops.registration_backfill_plan()')).rows[0]; await dbP.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 't@test']); }
  await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  assert.equal((await q('select phase from ops.master_cutover_state'))[0].phase, 'new_open');

  const versionsOf = async (sku) => (await A.readAmazonMap(dbO, sku)).versions;
  const save = (db, sku, name, components, { versions, requestId = crypto.randomUUID(), beforeCommit } = {}) =>
    A.saveAmazonMap(db, { actor: 'naka@test', requestId, sellerSku: sku, name, components, reason: 'pg', seen: { versions } }, { ownership: ALL_COMPANY, open: true, beforeCommit });
  const del = (db, sku, { versions, requestId = crypto.randomUUID(), beforeCommit } = {}) =>
    A.deleteAmazonMap(db, { actor: 'naka@test', requestId, sellerSku: sku, reason: 'pg の墓標', seen: { versions } }, { ownership: ALL_COMPANY, open: true, beforeCommit });
  const comps = async (code) => (await q(`select k.code, c.qty, c.sort_order from core.listing_components c join core.skus k on k.sku_id = c.sku_id
    where c.listing_id = (select listing_id from core.listings where mall = 'amazon' and listing_norm = core.norm_code($1)) order by c.sort_order`, [code])).map((r) => `${r.code}x${r.qty}@${r.sort_order}`);
  const pauseAfterLock = (client, g) => ({
    query: async (text, params) => { const r = await client.query(text, params); if (text === C.MASTER_WRITE_EXCLUSIVE_LOCK_SQL) await g.wait(); return r; },
    exec: (text) => client.query(text),
  });

  await ta('[2] 同じ seller SKU を 2 人が同じ版から保存: 後の人は出品ごとの鍵で待ち、前の人の commit の後に 409 version_conflict (両方は書かない)', async () => {
    const v = await versionsOf('pm_1');
    const g = gate();
    const a = launch(save(dbA, 'pm_1', 'A さん', [{ code: 'p001', qty: 2 }, { code: 'p002', qty: 1 }], { versions: v, beforeCommit: g.wait }));
    await sleep(300);
    const ridB = crypto.randomUUID();
    const b = launch(save(dbB, 'pm_1', 'B さん', [{ code: 'p001', qty: 5 }], { versions: v, requestId: ridB }));
    await sleep(700);
    assert.equal(b.done, false, '後の人は鍵で待っている');
    g.open();
    const ra = await a.promise; const rb = await b.promise;
    assert.ok(ra.ok, ra.err?.message);
    assert.deepEqual([rb.err?.status, rb.err?.reason], [409, 'version_conflict'], rb.err?.message || 'ok になった');
    assert.deepEqual(await comps('pm_1'), ['p001x2@0', 'p002x1@1']);
    assert.equal((await q('select name from core.amazon_sku_maps where seller_sku = $1', ['pm_1']))[0].name, 'A さん');
    assert.equal((await q('select status from ops.master_edit_requests where request_id = $1', [ridB]))[0].status, 'failed');
  });

  await ta('[3] 同じ request_id が 2 つ並ぶ (押し直し): 後の方は request_id の鍵で待ち、前の結果をそのまま返す (記録は 1 行・変更は 1 回)', async () => {
    const v = await versionsOf('pm_1');
    const rid = crypto.randomUUID();
    const g = gate();
    const a = launch(save(dbA, 'pm_1', 'A さん', [{ code: 'p001', qty: 3 }, { code: 'p002', qty: 1 }], { versions: v, requestId: rid, beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'pm_1', 'A さん', [{ code: 'p001', qty: 3 }, { code: 'p002', qty: 1 }], { versions: v, requestId: rid }));
    await sleep(500);
    assert.equal(b.done, false);
    g.open();
    const ra = await a.promise; const rb = await b.promise;
    assert.ok(ra.ok && rb.ok, `${ra.err?.message} ${rb.err?.message}`);
    assert.equal(rb.ok.replayed, true);
    assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [rid]))[0].n), 1);
    assert.deepEqual(await comps('pm_1'), ['p001x3@0', 'p002x1@1']);
  });

  await ta('[4] 夜間ロードが先 = 保存はマスタの書き込みの鍵で短く待って 409 nightly_load → ロードは最後まで (対応の構成は変えない)', async () => {
    const g = gate();
    const l = launch(loadWith(pauseAfterLock(O2, g), 'load_pg_lock1'));
    await sleep(300);
    assert.equal(l.done, false);
    const t0 = Date.now();
    const r = await save(dbA, 'pm_1', 'ロード中', [{ code: 'p001', qty: 9 }], { versions: await versionsOf('pm_1') }).then((ok) => ({ ok }), (err) => ({ err }));
    const waited = Date.now() - t0;
    assert.deepEqual([r.err?.status, r.err?.reason], [409, 'nightly_load'], r.err?.message || 'ok になった');
    assert.ok(waited >= 2000 && waited < 9000, `待った長さ ${waited}ms`);
    g.open();
    const rl = await l.promise;
    assert.equal(rl.ok?.ok, true, rl.err?.stack || JSON.stringify(rl.ok?.error));
    assert.deepEqual(await comps('pm_1'), ['p001x3@0', 'p002x1@1']);
  });

  await ta('[5] 保存が先 = 夜間ロードは鍵で待ち、保存の commit の後に最後まで。対応の出品 (FBM の完全一致の候補があっても) の構成は触らない', async () => {
    const g = gate();
    const a = launch(save(dbA, 'p003', 'FBM を対応に', [{ code: 'p003', qty: 2 }], { versions: await versionsOf('p003'), beforeCommit: g.wait }));
    await sleep(300);
    const l = launch(loadWith(pgAdapter(O2), 'load_pg_lock2'));
    await sleep(700);
    assert.equal(l.done, false, 'ロードは保存が終わるまで鍵で待つ');
    g.open();
    const ra = await a.promise;
    assert.ok(ra.ok, ra.err?.message);
    const rl = await l.promise;
    assert.equal(rl.ok?.ok, true, rl.err?.stack || JSON.stringify(rl.ok?.error));
    assert.deepEqual(await comps('p003'), ['p003x2@0']);
  });

  await ta('[6] 墓標が先 × 夜間ロード: ロードは鍵で待ち、墓標の後に最後まで・墓標の出品に FBM の完全一致を作り直さない (もう 1 回のロードも)', async () => {
    await save(dbA, 'p004', 'FBM 4', [{ code: 'p004', qty: 1 }], { versions: await versionsOf('p004') });
    const g = gate();
    const d = launch(del(dbA, 'p004', { versions: await versionsOf('p004'), beforeCommit: g.wait }));
    await sleep(300);
    const l = launch(loadWith(pgAdapter(O2), 'load_pg_tomb1'));
    await sleep(700);
    assert.equal(l.done, false);
    g.open();
    const rd = await d.promise; assert.ok(rd.ok, rd.err?.message);
    const rl = await l.promise; assert.equal(rl.ok?.ok, true, rl.err?.stack || JSON.stringify(rl.ok?.error));
    assert.deepEqual(await comps('p004'), []);
    const rl2 = await loadWith(pgAdapter(O2), 'load_pg_tomb2');
    assert.equal(rl2.ok, true);
    assert.deepEqual(await comps('p004'), []);
  });

  await ta('[6] 夜間ロードが先 × 墓標: 墓標は 409 nightly_load (何も変えない) → ロードの後にもう一度 = 墓標 → 次のロードも作り直さない', async () => {
    await save(dbA, 'p005', 'FBM 5', [{ code: 'p005', qty: 1 }], { versions: await versionsOf('p005') });
    const g = gate();
    const l = launch(loadWith(pauseAfterLock(O2, g), 'load_pg_tomb3'));
    await sleep(300);
    const r = await del(dbA, 'p005', { versions: await versionsOf('p005') }).then((ok) => ({ ok }), (err) => ({ err }));
    assert.deepEqual([r.err?.status, r.err?.reason], [409, 'nightly_load'], r.err?.message || 'ok になった');
    g.open();
    assert.equal((await l.promise).ok?.ok, true);
    assert.equal((await q("select state from core.amazon_sku_maps where seller_sku = 'p005'"))[0].state, 'active');
    const r2 = await del(dbA, 'p005', { versions: await versionsOf('p005') });
    assert.equal(r2.state, 'deleted');
    assert.equal((await loadWith(pgAdapter(O2), 'load_pg_tomb4')).ok, true);
    assert.deepEqual(await comps('p005'), []);
  });

  await ta('[7] 画面のロールのログイン: 対応・構成・出品を直接書けない・消せない・部品の関数を呼べない (42501)・保存の関数は呼べる', async () => {
    for (const sql of ["delete from core.amazon_sku_maps where seller_sku = 'p005'", 'truncate core.amazon_sku_maps', "update core.amazon_sku_maps set name = 'x'",
      "update core.listing_components set qty = 9", "delete from core.listing_components", "update core.listings set title = 'x'",
      `select ops.amazon_map_begin('amazon_map_save', gen_random_uuid(), 'x', null, '{}', 'x', '{}')`, `select ops.amazon_map_components('[]')`,
      "insert into ops.master_write_sessions (session_id, txid, request_id, operation, listing_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions, actor_id, source_system, db_user, phase, owner_hash, ownership) values (gen_random_uuid(), txid_current(), gen_random_uuid(), 'amazon_map_save', 1, '{}', '{}', repeat('a', 64), repeat('a', 64), '{}', 'x', 'portal_amazon_map', 'x', 'new_open', repeat('a', 64), '{}')"]) {
      await assert.rejects(() => EA.query(sql), (e) => { assert.equal(e.code, '42501', `${sql}: ${e.code} ${e.message}`); return true; });
    }
    assert.equal((await EA.query(`select has_function_privilege('ops.save_amazon_sku_map(uuid, text, text, jsonb, text, jsonb)', 'execute') as x`)).rows[0].x, true);
    // 持ち主でも墓標は消せない
    await assert.rejects(() => O.query("delete from core.amazon_sku_maps where seller_sku = 'p005'"), (e) => e.code === '42501' && /amazon_map_no_delete/.test(e.message));
  });

  await ta('[8] 不変条件は commit のときに効く (実 PostgreSQL)・書き手の守り (夜間ロードの名前 = 42501)・UTC / 東京の session で写しのハッシュが同じ', async () => {
    await O.query('begin');
    await O.query(`select set_config('core.source_system', 'amazon_map_migration', true)`);
    await O.query(`update core.listing_components set sort_order = 7 where listing_id = (select listing_id from core.amazon_sku_maps where seller_sku = 'p003')`);
    const e = await O.query('commit').then(() => null, (x) => x);
    assert.equal(e?.code, '23514'); assert.match(e.message, /amazon_map_invariant/);
    await O.query('begin');
    try {
      await O.query(`select set_config('core.source_system', 'company_db_load', true)`);
      await assert.rejects(() => O.query(`update core.listing_components set qty = 9 where listing_id = (select listing_id from core.amazon_sku_maps where seller_sku = 'p003')`), (x) => x.code === '42501');
    } finally { await O.query('rollback'); }
    assert.deepEqual(await comps('p003'), ['p003x2@0']);
    await O.query(`set timezone = 'UTC'`);
    const d1 = K.skuMapDigest(await A.readCompanyAmazonMapCanon(dbO));
    await O.query(`set timezone = 'Asia/Tokyo'`);
    const d2 = K.skuMapDigest(await A.readCompanyAmazonMapCanon(dbO));
    assert.equal(d2.content_hash, d1.content_hash);
    assert.deepEqual(K.validateSkuMap(await A.readCompanyAmazonMapCanon(dbO)), []);
  });
  await ta('[9] 影運転の先の確かめ (実 PostgreSQL・Codex #1586 R1 M2): 同じ DB を別の名前 (127.0.0.1)・別のユーザーで指しても断る / 同じサーバーの別の DB は通す', async () => {
    const CLI = await import('./company-db/amazon-map-migrate.mjs');
    const code = async (p) => { try { await p; return 'ok'; } catch (e) { return `${e.code}: ${e.message}`; } };
    const prod = u.toString();
    const alias = new URL(roleUrl('master_edit')); alias.hostname = '127.0.0.1';
    assert.match(await code(CLI.assertShadowTarget({ targetUrl: alias.toString(), productionUrl: prod })), /^AMAZON_MAP_MIGRATE_PRODUCTION/);
    const other = new URL(url); other.hostname = '127.0.0.1';   // 同じサーバーの別の DB (postgres)
    assert.equal(await code(CLI.assertShadowTarget({ targetUrl: other.toString(), productionUrl: prod })), 'ok');
    assert.match(await code(CLI.assertShadowTarget({ targetUrl: other.toString(), productionUrl: undefined })), /^AMAZON_MAP_MIGRATE_ARGS/);
  });
} finally {
  for (const c of clients.reverse()) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
