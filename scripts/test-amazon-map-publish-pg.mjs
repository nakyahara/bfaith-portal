/**
 * test-amazon-map-publish-pg.mjs — Amazon SKU の対応の写し (⑦-2 PR-A・apps/company-db/publish/amazon-map.mjs) を**実 PostgreSQL**と**本番と同じログイン**で確かめる
 *   (PGlite の試験 = scripts/test-amazon-map-publish.mjs。ここは PGlite では確かめられない所: パスワードでログインする watcher・本物の connectWatcher
 *    (statement_timeout・読むだけ)・別々の接続の同時実行・ロールの既定の時差 (東京))
 *
 * 固定する契約:
 *   1 切替の前 (持ち主 load): 入口 (cli --daily) を本物の watcher のログインで流す = ⏭️ exit 0・古い表を開かない
 *   2 移行 (H0) → 切替 → 写し = 変わった行 0・ハッシュ = H0
 *   3 watcher のロールの既定の時差が東京でも、写した古い表のハッシュ = UTC の session で読んだ Company DB のハッシュ
 *   4 別々の接続で 2 つの写しが並ぶ: 後の方は鍵で断られる (exit 73・書かない)・前の方は鍵の後に読んだ Company DB を写す
 *   5 watcher のログインは対応を書けない
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:<port>/postgres node scripts/test-amazon-map-publish-pg.mjs
 *   (この PC では C:/tmp/pg-embed の使い捨ての PostgreSQL の起動役が TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-amazon-map-publish.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'amzA-publish-pg-'));
process.env.DATA_DIR = tmp;
delete process.env.CDB_AMAZON_MAP_PUBLISH_PAUSE;

const { openPgClient, pgAdapter, applyMigrations } = await import('./company-db/migrate.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { OWNED_COLUMNS } = await import('../config/master-ownership.mjs');
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const C = await import('../lib/master-cutover.mjs');
const A = await import('../lib/amazon-map-write.mjs');
const M = await import('../lib/amazon-map-migrate.mjs');
const K = await import('../lib/sku-map-canonical.js');
const P = await import('../apps/company-db/publish/amazon-map.mjs');
const EPOCH = await import('./fixtures/master-epoch.mjs');
const { initDB, getDB } = await import('../apps/warehouse/db.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
{ const l = console.log; console.log = quiet; try { await initDB(); } finally { console.log = l; } }
const sq = getDB();
const WH_FILE = path.join(tmp, 'warehouse.db');
const T1 = '2026-05-01T01:02:03.456Z'; const T2 = '2026-06-01T00:00:00.000Z';
const MASTERS = [['pr_a001', 'SKU マスタの 1', T1, T2], ['pr_pack2', 'SKU マスタの 2 個組', T1, T1]];
const COMPS = [['pr_a001', 'p001', 1, 0, T1, T1], ['pr_pack2', 'p001', 2, 0, T1, T1], ['pr_pack2', 'p002', 1, 1, T1, T2]];
for (let i = 0; i < 20; i++) { const s = `pr_f${String(i).padStart(2, '0')}`; MASTERS.push([s, `埋め草 ${s}`, T1, T1]); COMPS.push([s, 'p003', 1, 0, T1, T1]); }
for (const m of MASTERS) sq.prepare('INSERT INTO m_sku_master (seller_sku, 商品名, created_at, updated_at, created_by) VALUES (?, ?, ?, ?, ?)').run(m[0], m[1], m[2], m[3], 'legacy@test');
for (const c of COMPS) sq.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)').run(...c);
const sqSnap = () => JSON.stringify({ m: sq.prepare('SELECT * FROM m_sku_master ORDER BY seller_sku').all(), c: sq.prepare('SELECT * FROM m_sku_components ORDER BY seller_sku, ne_code').all(),
  meta: sq.prepare('SELECT * FROM sync_meta WHERE key = ?').all(P.META_KEY), locks: sq.prepare('SELECT * FROM job_locks WHERE job_name = ?').all(P.LOCK_NAME) });
const legacyHash = () => P.legacyDigestOf(sq).content_hash;
const H0 = M.legacyDigest(M.readLegacyAmazonMaps(WH_FILE)).content_hash;

const ALL_LOAD = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual' }] };
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WATCH_PW = `w_${crypto.randomBytes(12).toString('hex')}`;
const dbName = `cdb_amza_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role, pw = PW[role]) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
const WATCH_URL = roleUrl('watcher', WATCH_PW);
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };

function makePlan() {
  const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
  return { skus: ['p001', 'p002', 'p003', 'p004'].map(single), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [], suppliers: [], supplierSkus: [] };
}
/** 入口そのもの (本物の connectWatcher = パスワードでログイン・statement_timeout・読むだけ)。古い表はこの試験の warehouse.db */
const cliReal = (argv = ['--daily'], deps = {}) => P.cli(argv, { env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: WATCH_URL }, openSqlite: async () => sq, log: quiet, runLockHeld: () => ({ what: '試験の回', pid: 1, token: 't', file: 'x' }), runLockStill: () => true, ...deps });

const O = await open(null);
const clients = [O];
try {
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: quiet });
  const r = await runInitialLoad(dbO, makePlan(), { log: quiet, runId: 'load_pg', ownership: ALL_LOAD, now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r.ok, true, r.error);
  await createRoles(O, { watcherPw: WATCH_PW, writerPw: `x_${crypto.randomBytes(8).toString('hex')}` });
  await createMasterEditRoles(O, { pw: PW });
  await W2.useReal0058(O);

  await ta('[1] 切替の前 (持ち主 load): 本物の watcher のログインで入口を流す = ⏭️ exit 0・古い表を開かない・書かない', async () => {
    const before = sqSnap();
    let opened = 0;
    const c = await cliReal(['--daily'], { openSqlite: async () => { opened++; return sq; } });
    assert.equal(c.code, 0, c.last); assert.match(c.last, /^⏭️ /);
    assert.equal(opened, 0);
    assert.equal(sqSnap(), before);
  });

  // 切替 (本物の権限・本番の関数そのまま): frozen → 移行 (H0) → company_owner → new_open
  const [GR, GM, Pc, ED] = [await open('master_gate_render'), await open('master_gate_minipc'), await open('master_ops'), await open('master_edit')];
  clients.push(GR, GM, Pc, ED);
  const dbP = pgAdapter(Pc), dbE = pgAdapter(ED);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(ALL_LOAD);
  const mh = await C.manifestHashOf(dbO, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  const stamp = () => new Date().toISOString();
  await acks(ALL_LOAD, 'legacy_open');
  await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy,
    manual_entries_stopped: [{ id: 'gas:logizard-sheet-and-sku-map', by: 't', at: stamp() }, { id: 'ne:item-screen', by: 't', at: stamp() }], drain: { done: true, checked_by: 't', checked_at: stamp() } } });
  const mig = await M.runAmazonMapMigration(dbO, M.readLegacyAmazonMaps(WH_FILE), { mode: 'apply', expectHash: H0, actor: 't@test', sheetOnly: [] });
  assert.equal(mig.committed, true);
  await acks(ALL_COMPANY, 'frozen');
  await EPOCH.seedActiveEpoch(dbO, ALL_COMPANY);
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(ALL_COMPANY, 'company_owner');
  { const p = (await dbP.query('select * from ops.registration_backfill_plan()')).rows[0]; await dbP.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 't@test']); }
  await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  const save = async (sku, name, components) => {
    const versions = (await A.readAmazonMap(dbO, sku)).versions;
    return A.saveAmazonMap(dbE, { actor: 'naka@test', requestId: crypto.randomUUID(), sellerSku: sku, name, components, reason: 'pg', seen: { versions } }, { open: true });
  };
  const cdbHashUtc = async () => { await O.query(`set timezone = 'UTC'`); return K.skuMapDigest(await A.readCompanyAmazonMapCanon(dbO)).content_hash; };

  await ta('[2] 移行 (H0) → 切替 → 写し (本物の watcher) = 変わった行 0・ハッシュ = H0・記録が残る', async () => {
    assert.equal(await cdbHashUtc(), H0);
    const c = await cliReal();
    assert.equal(c.code, 0, c.last); assert.match(c.last, /^✅ .*変わった行 0 \/ 対応 22 件・構成 23 行/);
    assert.equal(legacyHash(), H0);
    assert.equal(P.readMeta(sq).content_hash, H0);
  });

  await ta('[3] watcher のロールの既定の時差が東京でも、写した古い表のハッシュ = UTC の session の Company DB のハッシュ (時刻は UTC・Z・ミリ秒)', async () => {
    await O.query(`alter role watcher set timezone = 'Asia/Tokyo'`);
    try {
      const W = await openPgClient(WATCH_URL);
      try { assert.equal((await W.query('show timezone')).rows[0].TimeZone, 'Asia/Tokyo'); } finally { await W.end(); }
      await save('pr_pack2', 'SKU マスタの 2 個組 (並べ替え)', [{ code: 'p002', qty: 1 }, { code: 'p001', qty: 4 }]);
      await save('pr_new_pg', '実 PG で登録', [{ code: 'p004', qty: 2 }]);
      const c = await cliReal();
      assert.equal(c.code, 0, c.last); assert.match(c.last, /^✅ .*写した/);
      assert.equal(legacyHash(), await cdbHashUtc());
      const row = sq.prepare('SELECT created_at, updated_at FROM m_sku_master WHERE seller_sku = ?').get('pr_new_pg');
      assert.ok(K.isCanonicalTimestamp(row.created_at) && K.isCanonicalTimestamp(row.updated_at), JSON.stringify(row));
      assert.deepEqual(sq.prepare('SELECT ne_code FROM v_sku_components_first WHERE seller_sku = ?').all('pr_pack2'), [{ ne_code: 'p002' }]);
    } finally { await O.query('alter role watcher reset timezone'); }
  });

  await ta('[4] 別々の接続で 2 つの写しが並ぶ: 後の方は鍵で断られる (exit 73・書かない)・前の方は鍵の後に読んだ Company DB を写す', async () => {
    let release; const held = new Promise((r) => { release = r; });
    let entered; const inLock = new Promise((r) => { entered = r; });
    const first = cliReal(['--daily'], { afterLock: async () => { entered(); await held; } });
    await inLock;
    await save('pr_a001', 'SKU マスタの 1 (鍵の間に直した)', [{ code: 'p001', qty: 1 }]);
    const before = sqSnap();
    const second = await cliReal(['--daily']);
    assert.equal(second.code, 73); assert.match(second.last, /別の写しが動いている/);   // 73 = daily-sync は f_sales 以降を retry に残す
    assert.equal(sqSnap(), before);
    release();
    const r1 = await first;
    assert.equal(r1.code, 0, r1.last);
    assert.equal(sq.prepare('SELECT 商品名 FROM m_sku_master WHERE seller_sku = ?').get('pr_a001').商品名, 'SKU マスタの 1 (鍵の間に直した)');
    assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM job_locks WHERE job_name = ?').get(P.LOCK_NAME).n, 0);
  });

  await ta('[5] watcher のログインは対応・構成を書けない (読むだけ・select だけ)', async () => {
    const W = await openPgClient(WATCH_URL);
    try {
      await assert.rejects(W.query(`update core.amazon_sku_maps set name = 'x'`), /read-only|permission denied/);
      await W.query('set default_transaction_read_only = off');
      await assert.rejects(W.query(`update core.amazon_sku_maps set name = 'x'`), /permission denied/);
      await assert.rejects(W.query(`delete from core.listing_components`), /permission denied/);
    } finally { await W.end(); }
  });
} finally {
  for (const c of clients.reverse()) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
  try { sq.close(); } catch { /* */ }
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* */ }
}
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
