/**
 * test-amazon-map-publish.mjs — Company DB の Amazon SKU の対応 → 古い表 (m_sku_master / m_sku_components) の写し (⑦-2 PR-A・apps/company-db/publish/amazon-map.mjs)
 *
 * Company DB = PGlite (持ち主のロール deploy で migration・本物の 0058・見張りと画面のロール)。古い表 = 本物の warehouse.db の形 (db.js の initDB・一時の DATA_DIR)。
 * 固定する契約:
 *   1 今の本番 (持ち主 listing_components.amazon = load) は何もしない: SQLite を開かない・書かない・exit 0 (行が無い DB も・13 キーが company の DB も)。
 *     持ち主を読めない (未設定・届かない) = config も前の写しの記録も load なら ⚠️ exit 0 (retry に載せない)・どちらかが company なら ❌ exit 1。止める env = ⚠️ exit 0
 *   2 移行 (H0) → 写し = 変わった行 0 (total_changes 0)・ハッシュ = H0・記録 (sync_meta) が残る。2 回目も変わった行 0
 *   3 画面で並べ替え・新しく登録 → 写し = 差だけ・v_sku_components_first が SKU ごとに 1 行・UTC と東京の session で同じハッシュ・pr_ の SKU も
 *   4 墓標 → 写し = 親も構成も消える
 *   5 安全弁: 0 件 / 今の 90% 未満 / 変更の記録の番号が戻った / 記録が読めない = 断る (古い表は 1 バイトも変わらない・exit 1)。
 *     --allow-shrink --expect-hash (手だけ) = 0 件・90% 未満を通す (違うハッシュ = 断る・--daily と一緒は引数の誤り) / --accept-restore --expect-hash = 番号の戻りを通す
 *   6 途中で落ちる (commit の前に投げる・読み直したハッシュが違う・鍵を取られた) = 全部巻き戻る
 *   7 前の写しの後に古い表を誰かが書き換えた = ⚠️ + Company DB の値で上書き
 *   8 鍵: 別の写しが鍵を持つ = 何もしない (exit 1)・鍵は PG の対応を読む前に取る (鍵を取った後の Company DB の変更が写る)・終わったら外す・試し (--dry-run) は鍵も取らず書かない
 *   9 本番と同じ watcher のロール (select だけ・読むだけ) で読める
 * 使い方: node scripts/test-amazon-map-publish.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'amzA-publish-'));
process.env.DATA_DIR = tmp;
delete process.env.COMPANY_DB_WATCH_URL;
delete process.env.CDB_AMAZON_MAP_PUBLISH_PAUSE;

const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { OWNED_COLUMNS } = await import('../config/master-ownership.mjs');
const C = await import('../lib/master-cutover.mjs');
const A = await import('../lib/amazon-map-write.mjs');
const M = await import('../lib/amazon-map-migrate.mjs');
const K = await import('../lib/sku-map-canonical.js');
const P = await import('../apps/company-db/publish/amazon-map.mjs');
const { acquireLock, releaseLock } = await import('../apps/warehouse/job-locks.js');
const EPOCH = await import('./fixtures/master-epoch.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = quiet; console.warn = quiet; try { return await fn(); } finally { console.log = l; console.warn = w; } };

// ── 古い表 (warehouse.db の本物の形) ──
const { initDB, getDB } = await import('../apps/warehouse/db.js');
await quietly(() => initDB());
const sq = getDB();
const WH_FILE = path.join(tmp, 'warehouse.db');
const T1 = '2026-05-01T01:02:03.456Z'; const T2 = '2026-06-01T00:00:00.000Z';
const CLEAN_MASTERS = [['pr_a001', 'SKU マスタの 1', T1, T2], ['pr_pack2', 'SKU マスタの 2 個組', T1, T1], ['a003', '単品 3 を FBA でも', T1, T2], ['pr_new1', '新しい組 (出品なし)', T1, T1]];
const CLEAN_COMPS = [['pr_a001', 'a001', 1, 0, T1, T1], ['pr_pack2', 'a001', 2, 0, T1, T1], ['pr_pack2', 'a002', 1, 1, T1, T2], ['a003', 'a003', 2, 0, T1, T1],
  ['pr_new1', 'a005', 1, 0, T1, T1], ['pr_new1', 'a006', 3, 1, T2, T2]];
/** 本番の数 (約 2 万) に比べて小さすぎると 1 件の墓標で 90% を割る = 埋め草の対応 20 件 (pr_ の SKU・出品は移行が作る) */
const FILLERS = Array.from({ length: 20 }, (_, i) => `pr_f${String(i).padStart(2, '0')}`);
for (const s of FILLERS) { CLEAN_MASTERS.push([s, `埋め草 ${s}`, T1, T1]); CLEAN_COMPS.push([s, 'a007', 1, 0, T1, T1]); }
for (const m of CLEAN_MASTERS) sq.prepare('INSERT INTO m_sku_master (seller_sku, 商品名, created_at, updated_at, created_by, updated_by) VALUES (?, ?, ?, ?, ?, ?)').run(m[0], m[1], m[2], m[3], 'legacy@test', null);
for (const c of CLEAN_COMPS) sq.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)').run(...c);
/** 古い表と記録・鍵の全部 (1 バイトでも変われば違う) */
const sqSnap = () => JSON.stringify({ m: sq.prepare('SELECT * FROM m_sku_master ORDER BY seller_sku').all(), c: sq.prepare('SELECT * FROM m_sku_components ORDER BY seller_sku, ne_code').all(),
  meta: sq.prepare('SELECT * FROM sync_meta WHERE key = ?').all(P.META_KEY), locks: sq.prepare('SELECT * FROM job_locks WHERE job_name = ?').all(P.LOCK_NAME) });
const legacyHash = () => P.legacyDigestOf(sq).content_hash;
const H0 = M.legacyDigest(M.readLegacyAmazonMaps(WH_FILE)).content_hash;
assert.match(H0, /^[0-9a-f]{64}$/);

// ── Company DB (PGlite)。test-amazon-map.mjs と同じ作り ──
const AMZ = 'main@A1VC38T7YXB528';
const ALL_LOAD = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const PROD_ACTIVE_20261005 = ['external_ids.jan', 'products.name', 'products.sales_class', 'products.status', 'sku_costs', 'skus.handling', 'skus.name',
  'skus.reorder_months', 'skus.shipping', 'skus.standard_price', 'skus.tax_class', 'skus.tax_rate', 'supplier_skus.is_primary'];
const PROD_ACTIVE = { ...ALL_LOAD, ...Object.fromEntries(PROD_ACTIVE_20261005.map((k) => [k, 'company'])) };
const LOAD_NOW = new Date('2030-01-05T03:00:00Z');
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual' }] };
const BUILDS = { render: ['r1'], minipc: ['m1'] };
const LEGACY_HASH = C.ownershipHash(ALL_LOAD);
const stamp = () => new Date().toISOString();

function makePlan() {
  const single = (code, name) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
  const amazon = (listingCode, title, comps, evidenceSource) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title, status: 'active', components: comps, asinCandidates: [], fnskuCandidates: [], evidenceSource });
  const fromMaster = (code, qty, sortOrder) => ({ code, qty, sortOrder, resolution: 'imported', evidence: { source: 'm_sku_master' } });
  return {
    skus: [single('a001', '単品 1'), single('a002', '単品 2'), single('a003', '単品 3'), single('a004', '単品 4'), single('a005', '単品 5'), single('a006', '単品 6'), single('a007', '単品 7')],
    variationGroups: [], setComponents: [],
    listings: [
      amazon('pr_a001', 'SKU マスタの 1', [fromMaster('a001', 1, 0)], 'mirror_sku_master'),
      amazon('pr_pack2', 'SKU マスタの 2 個組', [fromMaster('a001', 2, 0), fromMaster('a002', 1, 1)], 'mirror_sku_master'),
      amazon('a003', '単品 3 (FBM)', [{ code: 'a003', qty: 1, resolution: 'exact', evidence: { source: 'fbm_ne_code', seller_sku: 'a003' } }], 'amazon_fees_fbm'),
    ],
    observations: [], physicals: [], compliance: [], workers: [], suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
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
  const E = { pg, db, sessionUser };
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  await W2.useReal0058(pg);
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: 'load_fixed', ownership: ALL_LOAD, now: LOAD_NOW, host: 'test' });
  assert.equal(r.ok, true, r.error);
  return E;
}
async function asRole(E, role, fn) { await E.pg.query(`set role ${role}`); try { return await fn(); } finally { await E.pg.query('set role deploy'); } }
async function asGate(E, host, fn) {
  await E.pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await E.pg.query(`set session authorization ${E.sessionUser}`); await E.pg.query('set role deploy'); }
}
const gateAck = (E, host, inst, build, ownership, phase) => asGate(E, host, () => C.recordLegacyGateAck(E.db, { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }));
const advance = (E, to, evidence) => asRole(E, 'master_ops', () => C.advanceCutoverPhase(E.db, { to, actor: 'naka@test', evidence }));
const acks = async (E, own, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, own, phase); };
async function toFrozen(E) {
  const mh = await C.manifestHashOf(E.db, MANIFEST);
  await acks(E, ALL_LOAD, 'legacy_open');
  await advance(E, 'frozen', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH,
    manual_entries_stopped: [{ id: 'gas:logizard-sheet-and-sku-map', by: 't', at: stamp() }, { id: 'ne:item-screen', by: 't', at: stamp() }], drain: { done: true, checked_by: 't', checked_at: stamp() } });
}
async function frozenToNewOpen(E, ownership) {
  const h = C.ownershipHash(ownership);
  const mh = await C.manifestHashOf(E.db, MANIFEST);
  await acks(E, ownership, 'frozen');
  await EPOCH.seedActiveEpoch(E.db, ownership);
  await advance(E, 'company_owner', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
  await acks(E, ownership, 'company_owner');
  await asRole(E, 'master_ops', async () => {
    const p = (await E.db.query('select * from ops.registration_backfill_plan()')).rows[0];
    await E.db.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 'naka@test']);
  });
  await advance(E, 'new_open', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
}
/** 写しの接続 = 本番と同じ watcher のロール・読むだけ (PGlite は 1 つの session = 開くときにロールを変え、閉じるときに戻す) */
const watcherConnect = (E, { tz = null, counter = null } = {}) => async () => {
  if (counter) counter.n++;
  await E.pg.query('set role watcher');
  await E.pg.query('set default_transaction_read_only = on');
  if (tz) await E.pg.query(`set timezone = '${tz}'`);
  return { db: E.db, close: async () => { await E.pg.query('reset timezone'); await E.pg.query('set default_transaction_read_only = off'); await E.pg.query('set role deploy'); } };
};
const sqliteOpener = (counter = null) => async () => { if (counter) counter.n++; return sq; };
/** 回の鍵を持つ親から起動された写し (daily-sync・自動再試行・手の口の子) の代わり */
const HELD = () => ({ what: '試験の回', pid: 1, token: 't', file: 'x' });
const STILL = () => true;   // commit の前の確かめ直し (親が生きていて同じ token の回の lock を持つ) の代わり
const publish = (E, opts = {}) => P.runAmazonMapPublish({ connect: watcherConnect(E, opts), getSqlite: sqliteOpener(opts.sqCounter), runLockHeld: HELD, runLockStill: STILL, ...opts });
const cli = (E, argv, { env = {}, ...deps } = {}) => P.cli(argv, { env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://watcher@localhost/x', ...env },
  connectFor: () => watcherConnect(E), openSqlite: async () => sq, log: quiet, runLockHeld: HELD, runLockStill: STILL, ...deps });
const vFirst = () => sq.prepare('SELECT seller_sku, ne_code, 数量 FROM v_sku_components_first ORDER BY seller_sku').all();

console.log('今の本番 (持ち主 listing_components.amazon = load) は何もしない');
const E1 = await setupDb();
await ta('[1] 持ち主が load (記録の行が無い DB・10/5 の 13 キーが company の DB): SQLite を開かない・書かない・exit 0 (daily も手も)', async () => {
  const before = sqSnap();
  const opened = { n: 0 };
  const r0 = await publish(E1, { sqCounter: opened });
  assert.deepEqual([r0.state, r0.code, opened.n], ['not_applied', 0, 0]);
  assert.match(r0.line, /^⏭️ CompanyDB写し\(Amazon SKU\): 持ち主が load/);
  await EPOCH.seedActiveEpoch(E1.db, PROD_ACTIVE);   // 今の本番 = 13 キーが company・Amazon は load
  const r1 = await publish(E1, { sqCounter: opened });
  assert.deepEqual([r1.state, r1.code, opened.n], ['not_applied', 0, 0]);
  const c1 = await cli(E1, ['--daily'], { openSqlite: async () => { opened.n++; return sq; } });
  assert.deepEqual([c1.code, opened.n], [0, 0]);
  assert.match(c1.last, /^⏭️ /);
  assert.equal(sqSnap(), before);
});
await ta('[1] 持ち主を読めない: config も前の写しの記録も load = ⚠️ exit 0 (retry に載せない) / config が company・記録がある = ❌ exit 1。止める env = ⚠️ exit 0 (試しは流せる)', async () => {
  const before = sqSnap();
  const down = () => async () => { throw Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:5432'), { code: 'ECONNREFUSED' }); };
  const a = await cli(E1, ['--daily'], { connectFor: down, readHintMeta: async () => null });
  assert.equal(a.code, 0); assert.match(a.last, /^⚠️ .*持ち主を読めない .*ECONNREFUSED.*config も前の写しの記録も load/);
  const b = await cli(E1, ['--daily'], { env: { COMPANY_DB_WATCH_URL: '' }, readHintMeta: async () => null });
  assert.equal(b.code, 0); assert.match(b.last, /^⚠️ .*未設定 COMPANY_DB_WATCH_URL/);
  const c = await cli(E1, ['--daily'], { connectFor: down, readHintMeta: async () => ({ content_hash: 'x' }) });
  assert.equal(c.code, 1); assert.match(c.last, /^❌ .*前に写した記録がある/);
  const d = await cli(E1, ['--daily'], { connectFor: down, readHintMeta: async () => null, ownership: { ...ALL_LOAD, 'listing_components.amazon': 'company' } });
  assert.equal(d.code, 1); assert.match(d.last, /config が company/);
  // 本物の手がかりの読み手 (warehouse.db を読むだけで開く): 今は記録が無い = null
  assert.equal(await P.readMetaReadonly(tmp), null);
  assert.equal(await P.readMetaReadonly(path.join(tmp, 'nothing-here')), null);
  // 止める env
  const p = await cli(E1, ['--daily'], { env: { [P.PAUSE_ENV]: '1' }, connectFor: () => { throw new Error('つながない'); } });
  assert.equal(p.code, 0); assert.match(p.last, /^⚠️ .*止めている \(CDB_AMAZON_MAP_PUBLISH_PAUSE=1\)/);
  // 引数の誤り
  for (const argv of [['--daily', '--allow-shrink', '--expect-hash', 'a'.repeat(64)], ['--allow-shrink'], ['--accept-restore'], ['--expect-hash', 'xyz'], ['--dry-run', '--allow-shrink', '--expect-hash', 'a'.repeat(64)], ['--nope'],
    [], ['--allow-shrink', '--expect-hash', 'a'.repeat(64)], ['--daily', '--chain'], ['--dry-run', '--chain'], ['--chain', '--allow-shrink']]) {   // 人が直接書く口は無い (#1649 Codex R3 High)
    const x = await cli(E1, argv);
    assert.equal(x.code, 1, argv.join(' ')); assert.match(x.last, /^❌ /);
  }
  assert.equal(sqSnap(), before);
});
await E1.pg.close();

console.log('\n切替の後 (持ち主 company)');
const E = await setupDb();
const { db } = E;
const q = async (sql, params) => (await db.query(sql, params)).rows;
const uuid = () => crypto.randomUUID();
const versionsOf = async (sku) => (await A.readAmazonMap(db, sku)).versions;
async function save(sellerSku, name, components, { actor = 'naka@test' } = {}) {
  const seen = { versions: await versionsOf(sellerSku) };
  return asRole(E, 'master_edit', () => A.saveAmazonMap(db, { actor, requestId: uuid(), sellerSku, name, components, reason: 'テスト', seen }, { open: true }));
}
async function del(sellerSku) {
  const seen = { versions: await versionsOf(sellerSku) };
  return asRole(E, 'master_edit', () => A.deleteAmazonMap(db, { actor: 'naka@test', requestId: uuid(), sellerSku, reason: 'テストで消す', seen }, { open: true }));
}
const cdbHash = async () => K.skuMapDigest(await A.readCompanyAmazonMapCanon(db)).content_hash;

await toFrozen(E);
const mig = await M.runAmazonMapMigration(db, M.readLegacyAmazonMaps(WH_FILE), { mode: 'apply', expectHash: H0, actor: 'naka@test', sheetOnly: [] });
assert.equal(mig.committed, true);
await frozenToNewOpen(E, ALL_COMPANY);

await ta('[2] 移行 (H0) → 写し = 変わった行 0 (total_changes 0)・ハッシュ = H0・記録が残る・2 回目も 0 / v_sku_components_first は SKU ごとに 1 行 / pr_ の SKU もそのまま', async () => {
  assert.equal(await cdbHash(), H0);
  const before = sq.prepare('SELECT * FROM m_sku_master ORDER BY seller_sku').all();
  const r = await publish(E);
  assert.deepEqual([r.state, r.code, r.changed, r.total_changes, r.tampered], ['unchanged', 0, 0, 0, false], r.line);
  assert.match(r.line, /^✅ CompanyDB写し\(Amazon SKU\): 変わった行 0 \/ 対応 24 件・構成 26 行/);
  assert.equal(legacyHash(), H0);
  assert.deepEqual(sq.prepare('SELECT * FROM m_sku_master ORDER BY seller_sku').all(), before);   // created_by なども触らない
  const meta = P.readMeta(sq);
  assert.equal(meta.content_hash, H0);
  assert.deepEqual([meta.master_rows, meta.component_rows, meta.by, meta.format], [24, 26, 'daily', 'sku-map-canon-v1']);
  assert.ok(Number.isSafeInteger(meta.watermark) && meta.watermark > 0);
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM job_locks WHERE job_name = ?').get(P.LOCK_NAME).n, 0);   // 鍵は外した
  const r2 = await publish(E);
  assert.deepEqual([r2.state, r2.changed, r2.total_changes], ['unchanged', 0, 0]);
  assert.deepEqual(vFirst().map((x) => x.seller_sku), ['a003', 'pr_a001', ...FILLERS, 'pr_new1', 'pr_pack2']);
  assert.equal(sq.prepare("SELECT COUNT(*) AS n FROM m_sku_master WHERE substr(seller_sku, 1, 3) = 'pr_'").get().n, 23);
});

await ta('[3] 画面で並べ替え・数量を変える・新しく登録 → 写し = 差だけ (親・構成)・v_sku_components_first が 1 行で新しい先頭・UTC と東京の session で同じハッシュ・作った人', async () => {
  await save('pr_pack2', 'SKU マスタの 2 個組', [{ code: 'a002', qty: 1 }, { code: 'a001', qty: 3 }]);
  await save('pr_new2', '新しい 3 点', [{ code: 'a004', qty: 1 }, { code: 'a005', qty: 2 }, { code: 'a007', qty: 1 }], { actor: 'Staff@Test' });
  const hUtc = (await publish(E, { dryRun: true })).digest.content_hash;
  const dTokyo = await publish(E, { dryRun: true, tz: 'Asia/Tokyo' });
  assert.equal(dTokyo.digest.content_hash, hUtc);
  assert.equal(dTokyo.state, 'dry_run');
  assert.deepEqual(dTokyo.counts, { master: { inserted: 1, updated: 1, deleted: 0, same: 23 }, components: { inserted: 3, updated: 2, deleted: 0, same: 24 } });
  const r = await publish(E, { tz: 'Asia/Tokyo' });
  assert.deepEqual([r.state, r.changed], ['applied', 7]);
  assert.equal(legacyHash(), hUtc);
  assert.equal(legacyHash(), await cdbHash());
  const first = vFirst();
  assert.equal(first.length, 25);
  assert.deepEqual(first.find((x) => x.seller_sku === 'pr_pack2'), { seller_sku: 'pr_pack2', ne_code: 'a002', 数量: 1 });
  assert.deepEqual(sq.prepare('SELECT ne_code, 数量, sort_order FROM m_sku_components WHERE seller_sku = ? ORDER BY sort_order').all('pr_pack2'), [{ ne_code: 'a002', 数量: 1, sort_order: 0 }, { ne_code: 'a001', 数量: 3, sort_order: 1 }]);
  const n2 = sq.prepare('SELECT 商品名, created_by, updated_by, created_at FROM m_sku_master WHERE seller_sku = ?').get('pr_new2');
  assert.deepEqual([n2.商品名, n2.created_by, n2.updated_by], ['新しい 3 点', 'staff@test', 'staff@test']);
  assert.ok(K.isCanonicalTimestamp(n2.created_at));
  // 変わらない行 (pr_a001) の作った人はそのまま
  assert.equal(sq.prepare('SELECT created_by FROM m_sku_master WHERE seller_sku = ?').get('pr_a001').created_by, 'legacy@test');
  assert.deepEqual([(await publish(E)).changed], [0]);
});

await ta('[4] 墓標 → 写し = 親も構成も消える (ほかは変わらない)', async () => {
  await del('pr_new1');
  const r = await publish(E);
  assert.equal(r.state, 'applied');
  assert.deepEqual(r.counts, { master: { inserted: 0, updated: 0, deleted: 1, same: 24 }, components: { inserted: 0, updated: 0, deleted: 2, same: 27 } });
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM m_sku_master WHERE seller_sku = ?').get('pr_new1').n, 0);
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM m_sku_components WHERE seller_sku = ?').get('pr_new1').n, 0);
  assert.equal(legacyHash(), await cdbHash());
});

await ta('[5] 安全弁: 今の 90% 未満 = 断る (1 バイトも変わらない・exit 1) → 手の --allow-shrink --expect-hash (違うハッシュ = 断る) で通す', async () => {
  for (const s of ['a003', 'pr_f00', 'pr_f01']) await del(s);   // 24 → 21 件 (87.5%)
  const before = sqSnap();
  const r = await publish(E);
  assert.deepEqual([r.state, r.code, r.problems], ['refused', 1, ['shrunk']]);
  assert.match(r.line, /^❌ .*断った \(shrunk\) = 古い表は前のまま \/ 古い表 24 件 → Company DB 21 件 .*--allow-shrink --expect-hash/);
  assert.equal(sqSnap(), before);
  const c1 = await cli(E, ['--daily']);
  assert.equal(c1.code, 1);
  const h = (await publish(E, { dryRun: true })).digest.content_hash;
  const bad = await cli(E, ['--chain', '--allow-shrink', '--expect-hash', 'f'.repeat(64)]);
  assert.equal(bad.code, 1); assert.match(bad.last, /expect_hash_mismatch/);
  assert.equal(sqSnap(), before);
  const ok = await cli(E, ['--chain', '--allow-shrink', '--expect-hash', h]);
  assert.equal(ok.code, 0, ok.last); assert.match(ok.last, /^✅ .*写した .*--allow-shrink/);
  assert.equal(legacyHash(), h);
  assert.deepEqual([P.readMeta(sq).by, P.readMeta(sq).allow_shrink], ['manual', true]);
});

await ta('[5] 安全弁: 0 件 = 断る (--allow-shrink でだけ通す = 古い表が空になる) → 登録し直すと次の写しで戻る', async () => {
  for (const s of ['pr_a001', 'pr_pack2', 'pr_new2', ...FILLERS.slice(2)]) await del(s);
  const before = sqSnap();
  const r = await publish(E);
  assert.deepEqual([r.state, r.code], ['refused', 1]);
  assert.ok(r.problems.includes('empty'), r.line);
  assert.equal(sqSnap(), before);
  const h = (await publish(E, { dryRun: true })).digest.content_hash;
  assert.equal((await cli(E, ['--chain', '--allow-shrink', '--expect-hash', h])).code, 0);
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM m_sku_master').get().n, 0);
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM m_sku_components').get().n, 0);
  // 墓標から戻す (登録し直す) → 普段の写しで戻る (古い表が 0 件 = 90% の比べようが無い)
  await save('pr_a001', 'SKU マスタの 1', [{ code: 'a001', qty: 1 }]);
  await save('pr_pack2', 'SKU マスタの 2 個組', [{ code: 'a001', qty: 2 }, { code: 'a002', qty: 1 }]);
  await save('a003', '単品 3 を FBA でも', [{ code: 'a003', qty: 2 }]);
  for (const s of FILLERS.slice(2)) await save(s, `埋め草 ${s}`, [{ code: 'a007', qty: 1 }]);
  const back = await publish(E);
  assert.deepEqual([back.state, back.code], ['applied', 0], back.line);
  assert.equal(legacyHash(), await cdbHash());
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM m_sku_master').get().n, 21);
});

await ta('[5] 安全弁: 変更の記録の番号が前の写しより小さい (Company DB を戻した?)・記録が読めない = 断る → --accept-restore --expect-hash で再開', async () => {
  const meta = P.readMeta(sq);
  const setMeta = (v) => sq.prepare('UPDATE sync_meta SET value = ? WHERE key = ?').run(v, P.META_KEY);
  setMeta(JSON.stringify({ ...meta, watermark: meta.watermark + 1000 }));
  await save('pr_a001', 'SKU マスタの 1 (直した)', [{ code: 'a001', qty: 1 }]);
  const before = sqSnap();
  const r = await publish(E);
  assert.deepEqual([r.state, r.code, r.problems], ['refused', 1, ['watermark_backward']]);
  assert.match(r.line, /変更の記録の番号 \d+ → \d+ .*--accept-restore --expect-hash/);
  assert.equal(sqSnap(), before);
  setMeta('{壊れた');
  const before2 = sqSnap();
  const r2 = await publish(E);
  assert.deepEqual([r2.state, r2.problems], ['refused', ['meta_unreadable']]);
  assert.equal(sqSnap(), before2);
  const h = (await publish(E, { dryRun: true })).digest.content_hash;
  const ok = await cli(E, ['--chain', '--accept-restore', '--expect-hash', h]);
  assert.equal(ok.code, 0, ok.last); assert.match(ok.last, /--accept-restore/);
  const m2 = P.readMeta(sq);
  assert.ok(m2.watermark <= meta.watermark + 1000 && m2.accept_restore === true);
  assert.equal(sq.prepare('SELECT 商品名 FROM m_sku_master WHERE seller_sku = ?').get('pr_a001').商品名, 'SKU マスタの 1 (直した)');
  assert.equal((await publish(E)).state, 'unchanged');   // 次からは普段どおり
});

await ta('[5] 受け手の決まりに合わない対応 (並びの隙間・0 件の構成など) は --allow-shrink でも断る (単体)', async () => {
  const canon = { master: [{ seller_sku: 'x1', name: 'x', created_at: T1, updated_at: T1 }], components: [{ seller_sku: 'x1', ne_code: 'a001', quantity: 1, sort_order: 2, created_at: T1, updated_at: T1 }] };
  const digest = K.skuMapDigest(canon);
  for (const allowShrink of [false, true]) assert.deepEqual(P.checkSafety({ canon, digest, legacyCount: 1, meta: null, watermark: 1, allowShrink }).problems, ['invalid_canon']);
  const empty = { master: [], components: [] };
  assert.deepEqual(P.checkSafety({ canon: empty, digest: K.skuMapDigest(empty), legacyCount: 0, meta: null, watermark: 1 }).problems, ['empty']);
  assert.deepEqual(P.checkSafety({ canon: empty, digest: K.skuMapDigest(empty), legacyCount: 5, meta: null, watermark: 1, allowShrink: true }).problems, []);
  // 90% ちょうどは通す・未満は断る / 前の写しに番号があるのに今は無い = 戻ったと同じ
  const ten = { master: Array.from({ length: 9 }, (_, i) => ({ seller_sku: `s${i}`, name: 'n', created_at: T1, updated_at: T1 })),
    components: Array.from({ length: 9 }, (_, i) => ({ seller_sku: `s${i}`, ne_code: 'a001', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 })) };
  assert.deepEqual(P.checkSafety({ canon: ten, digest: K.skuMapDigest(ten), legacyCount: 10, meta: null, watermark: 5 }).problems, []);
  assert.deepEqual(P.checkSafety({ canon: ten, digest: K.skuMapDigest(ten), legacyCount: 11, meta: null, watermark: 5 }).problems, ['shrunk']);
  assert.deepEqual(P.checkSafety({ canon: ten, digest: K.skuMapDigest(ten), legacyCount: 9, meta: { watermark: 5 }, watermark: null }).problems, ['watermark_backward']);
  assert.deepEqual(P.checkSafety({ canon: ten, digest: K.skuMapDigest(ten), legacyCount: 9, meta: { watermark: 5 }, watermark: 5 }).problems, []);
});

await ta('[6] 途中で落ちる = 全部巻き戻る: commit の前に投げる・読み直したハッシュが違う・鍵を取られた (古い表・記録・鍵が前のまま)', async () => {
  await save('pr_pack2', 'SKU マスタの 2 個組 (6)', [{ code: 'a002', qty: 5 }]);
  const before = sqSnap();
  await assert.rejects(() => publish(E, { beforeCommit: () => { throw new Error('試験で落とす'); } }), /試験で落とす/);
  assert.equal(sqSnap(), before);
  // 読み直したハッシュが違う (入れた中身と Company DB が違う) = 巻き戻す
  const canon = await A.readCompanyAmazonMapCanon(db);
  const wrong = { ...K.skuMapDigest(canon), content_hash: 'e'.repeat(64) };
  assert.throws(() => P.applyCanon(sq, { canon, digest: wrong, metaValue: { published_at: stamp() } }), (e) => e.code === 'REREAD_MISMATCH');
  assert.equal(sqSnap(), before);
  // 鍵が自分のものでない = 巻き戻す
  assert.throws(() => P.applyCanon(sq, { canon, digest: K.skuMapDigest(canon), metaValue: { published_at: stamp() }, lock: { jobName: P.LOCK_NAME, holderId: 'someone-else' } }), (e) => e.code === 'LOCK_LOST');
  assert.equal(sqSnap(), before);
  const r = await publish(E);
  assert.equal(r.state, 'applied');
  assert.equal(legacyHash(), await cdbHash());
});

await ta('[7] 前の写しの後に古い表を誰かが書き換えた (SKU タブ・CSV・GAS) = ⚠️ で知らせて Company DB の値で上書き', async () => {
  sq.prepare('UPDATE m_sku_master SET 商品名 = ? WHERE seller_sku = ?').run('手で変えた', 'pr_a001');
  sq.prepare('INSERT INTO m_sku_master (seller_sku, 商品名, created_at, updated_at) VALUES (?, ?, ?, ?)').run('hand_only', '手で足した', T1, T1);
  sq.prepare('INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)').run('hand_only', 'a001', 1, 0, T1, T1);
  const r = await publish(E);
  assert.deepEqual([r.state, r.code, r.tampered], ['applied', 0, true]);
  assert.match(r.line, /^⚠️ .*前の写しの後に古い表が書き換えられていた .*Company DB の値で上書きした/);
  assert.equal(legacyHash(), await cdbHash());
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM m_sku_master WHERE seller_sku = ?').get('hand_only').n, 0);
  assert.equal((await publish(E)).tampered, false);
});

await ta('[8] 鍵: 別の写しが鍵を持つ = 何もしない (exit 73)・鍵は PG の対応を読む前に取る・終わったら外す・同じ鍵の 2 つめは断られる・試しは鍵を取らず書かない', async () => {
  const other = acquireLock(sq, P.LOCK_NAME, { ttlMs: 60000 });
  assert.ok(other);
  await save('pr_a001', 'SKU マスタの 1 (鍵の間)', [{ code: 'a001', qty: 1 }]);
  const before = sqSnap();
  const r = await publish(E);
  assert.deepEqual([r.state, r.code], ['lock_busy', 73]);
  assert.match(r.line, /別の写しが動いている/);
  const c = await cli(E, ['--daily']);
  assert.equal(c.code, 73);
  assert.equal(sqSnap(), before);
  // 試しは鍵があっても読める (書かない)
  const d = await publish(E, { dryRun: true });
  assert.deepEqual([d.state, d.counts.master.updated], ['dry_run', 1]);
  assert.equal(sqSnap(), before);
  releaseLock(sq, other);
  // 鍵を取った後・PG を読む前に Company DB が変わる = その変更が写る (読みは鍵の後) / その間の 2 つめの写し = 断られる
  let inner = null, lockSeen = null;
  const r2 = await publish(E, { afterLock: async () => {
    lockSeen = sq.prepare('SELECT COUNT(*) AS n FROM job_locks WHERE job_name = ?').get(P.LOCK_NAME).n;
    await E.pg.query('reset role'); await E.pg.query('set default_transaction_read_only = off'); await E.pg.query('set role deploy');
    await save('pr_a001', 'SKU マスタの 1 (鍵の後に直した)', [{ code: 'a001', qty: 1 }]);
    inner = await publish(E);   // 同じ鍵 = 断られる (外側の接続を閉じて戻すので、また watcher にする)
    await E.pg.query('set role watcher'); await E.pg.query('set default_transaction_read_only = on');
  } });
  assert.equal(lockSeen, 1);
  assert.deepEqual([inner.state, inner.code], ['lock_busy', 73]);
  assert.equal(r2.state, 'applied', r2.line);
  assert.equal(sq.prepare('SELECT 商品名 FROM m_sku_master WHERE seller_sku = ?').get('pr_a001').商品名, 'SKU マスタの 1 (鍵の後に直した)');
  assert.equal(sq.prepare('SELECT COUNT(*) AS n FROM job_locks WHERE job_name = ?').get(P.LOCK_NAME).n, 0);
  // 時間切れの鍵は取り直せる (前の回が落ちた)
  sq.prepare('INSERT INTO job_locks (job_name, holder_id, acquired_at, expires_at, heartbeat_at) VALUES (?, ?, ?, ?, ?)').run(P.LOCK_NAME, 'dead', T1, T1, T1);
  assert.equal((await publish(E)).state, 'unchanged');
});

await ta('[8b] 書くのは回の鍵 (daily-sync / 再試行) を持つ親の子だけ (#1649 Codex R3 High): 親が鍵を持たない起動 (人が直接流す・--daily・--chain でも) = 書かずに断る・試しは流せる・本物の確かめ (親の pid と lock の pid)', async () => {
  await save('pr_a001', 'SKU マスタの 1 (回の外)', [{ code: 'a001', qty: 1 }]);
  const before = sqSnap();
  for (const argv of [['--daily'], ['--chain'], ['--chain', '--allow-shrink', '--expect-hash', 'a'.repeat(64)]]) {
    const r = await cli(E, argv, { runLockHeld: () => null });
    assert.equal(r.code, 1, argv.join(' ')); assert.match(r.last, /回の鍵 \(daily-sync \/ 再試行\) を持つ親から起動されていない = 書かない。手で写すのは .*retry-failed-jobs\.js --amazon-map-chain/);
  }
  // 差し替えない (本物の確かめ) = この試験のプロセスの親は回の鍵を持たない = 断る
  const real = await P.cli(['--daily'], { env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://watcher@localhost/x' }, connectFor: () => watcherConnect(E), openSqlite: async () => sq, log: quiet });
  assert.equal(real.code, 1); assert.match(real.last, /持つ親から起動されていない/);
  assert.equal(sqSnap(), before);
  // 試しは読むだけ = 鍵が無くても流せる
  assert.equal((await cli(E, ['--dry-run'], { runLockHeld: () => null })).code, 0);
  // 持ち主が load の今は、鍵が無くても何もしない (⏭️ exit 0 = 今の本番の動きのまま) は [1] と [9]
  // 本物の確かめの部品: lock の pid が親の pid = 持つ / 違う・読めない・無い = 持たない
  const files = { 'daily-sync': 'ds.json', '再試行': 'rt.json' };
  const read = (m) => (f) => m[f] ?? null;
  assert.deepEqual(P.runLockHeldByParent({ files, ppid: 42, read: read({ 'rt.json': { pid: 42, token: 't' } }) }), { what: '再試行', pid: 42, token: 't', file: 'rt.json' });
  assert.deepEqual(P.runLockHeldByParent({ files, ppid: 42, read: read({ 'ds.json': { pid: 42, run_id: 'r' } }) }), { what: 'daily-sync', pid: 42, token: 'r', file: 'ds.json' });
  assert.equal(P.runLockHeldByParent({ files, ppid: 42, read: read({ 'rt.json': { pid: 42 } }) }), null);   // token の無い lock = 確かめ直せない = 持たない
  assert.equal(P.runLockHeldByParent({ files, ppid: 42, read: read({ 'ds.json': { pid: 43 }, 'rt.json': { pid: '42' } }) }), null);
  assert.equal(P.runLockHeldByParent({ files, ppid: 42, read: read({}) }), null);
  assert.ok(/daily-sync\.lock\.json$/.test(P.RUN_LOCK_FILES['daily-sync']) && /retry-failed-jobs\.lock\.json$/.test(P.RUN_LOCK_FILES['再試行']));
  const ds = fs.readFileSync(new URL('../apps/warehouse/daily-sync.js', import.meta.url), 'utf8');
  const rt = fs.readFileSync(new URL('../apps/warehouse/retry-failed-jobs.js', import.meta.url), 'utf8');
  assert.ok(ds.includes("path.join(PROJECT_DIR, 'data', 'daily-sync.lock.json')") && rt.includes("path.join(PROJECT_DIR, 'data', 'retry-failed-jobs.lock.json')"));   // 回の lock と同じ場所
  // 本物の起動: 回の鍵 (lock の pid) を持つ親が子として写しを起動 = 書く / 持たない親 = 断る (子の process.ppid を本当に見る)
  const { spawnSync } = await import('node:child_process');
  const os2 = await import('node:os');
  const d = fs.mkdtempSync(path.join(os2.tmpdir(), 'amzA-ppid-'));
  try {
    const child = path.join(d, 'child.mjs');
    fs.writeFileSync(child, `import { runLockHeldByParent } from ${JSON.stringify(new URL('../apps/company-db/publish/amazon-map.mjs', import.meta.url).href)};\nconsole.log(JSON.stringify(runLockHeldByParent({ files: { '再試行': process.argv[2] } })));\n`);
    const lockF = path.join(d, 'retry-failed-jobs.lock.json');
    fs.writeFileSync(lockF, JSON.stringify({ token: 't', pid: process.pid, started_at: new Date().toISOString() }));
    const out1 = spawnSync(process.execPath, [child, lockF], { encoding: 'utf8' });
    assert.deepEqual(JSON.parse(out1.stdout.trim()), { what: '再試行', pid: process.pid, token: 't', file: lockF }, out1.stderr);
    fs.writeFileSync(lockF, JSON.stringify({ token: 't', pid: process.pid + 100000, started_at: new Date().toISOString() }));
    const out2 = spawnSync(process.execPath, [child, lockF], { encoding: 'utf8' });
    assert.equal(JSON.parse(out2.stdout.trim()), null);
  } finally { fs.rmSync(d, { recursive: true, force: true }); }
});

await ta('[8d] commit の直前に回の鍵を確かめ直す (#1649 Codex R4 Medium): 親が落ちた後に残った子・回が替わった (token が違う)・lock が消えた = commit しない (巻き戻す)', async () => {
  await save('pr_a001', 'SKU マスタの 1 (親が落ちた)', [{ code: 'a001', qty: 1 }]);
  const before = sqSnap();
  await assert.rejects(() => publish(E, { runLockStill: () => false }), (e) => e.code === 'RUN_LOCK_LOST');
  assert.equal(sqSnap(), before);
  const c = await cli(E, ['--daily'], { runLockStill: () => false });
  assert.equal(c.code, 1); assert.match(c.last, /回の鍵を持つ親がもういない/);
  assert.equal(sqSnap(), before);
  // 確かめ直しは commit の直前 (読み直しの後) = 始めの照合が通っていても、その後に落ちた親を見つける
  let asked = 0;
  await assert.rejects(() => publish(E, { runLockStill: () => { asked++; return false; } }), (e) => e.code === 'RUN_LOCK_LOST');
  assert.equal(asked, 1);
  // 部品: 同じ pid・同じ token・pid が生きている = 持つ / token が違う・pid が死んだ・lock が消えた・token が無い = 持たない
  const held = { what: '再試行', pid: 42, token: 't1', file: 'rt.json' };
  const rd = (j) => () => j;
  assert.equal(P.runLockStillHeld(held, { read: rd({ pid: 42, token: 't1' }), alive: () => true }), true);
  assert.equal(P.runLockStillHeld(held, { read: rd({ pid: 42, token: 't2' }), alive: () => true }), false);
  assert.equal(P.runLockStillHeld(held, { read: rd({ pid: 42, token: 't1' }), alive: () => false }), false);
  assert.equal(P.runLockStillHeld(held, { read: rd(null), alive: () => true }), false);
  assert.equal(P.runLockStillHeld({ ...held, token: null }, { read: rd({ pid: 42 }), alive: () => true }), false);
  assert.equal(P.runLockStillHeld({ what: 'daily-sync', pid: 42, token: 'r1', file: 'ds.json' }, { read: rd({ pid: 42, run_id: 'r1' }), alive: () => true }), true);
  // 本物: 終わったプロセス (= 落ちた親) の pid は生きていない = 持たない / 生きている自分の pid = 持つ
  const { spawnSync } = await import('node:child_process');
  const os2 = await import('node:os');
  const d = fs.mkdtempSync(path.join(os2.tmpdir(), 'amzA-dead-'));
  try {
    const dead = spawnSync(process.execPath, ['-e', ''], { encoding: 'utf8' }).pid;
    const lockF = path.join(d, 'retry-failed-jobs.lock.json');
    fs.writeFileSync(lockF, JSON.stringify({ token: 'tok', pid: dead, started_at: new Date().toISOString() }));
    assert.equal(P.runLockStillHeld({ what: '再試行', pid: dead, token: 'tok', file: lockF }), false);
    fs.writeFileSync(lockF, JSON.stringify({ token: 'tok', pid: process.pid, started_at: new Date().toISOString() }));
    assert.equal(P.runLockStillHeld({ what: '再試行', pid: process.pid, token: 'tok', file: lockF }), true);
    assert.equal(P.runLockStillHeld({ what: '再試行', pid: process.pid, token: 'other', file: lockF }), false);
  } finally { fs.rmSync(d, { recursive: true, force: true }); }
  assert.equal((await publish(E)).state, 'applied');
});

await ta('[8e] 断る起動は SQLite を開かない・--dry-run は既にある warehouse.db を読むだけで開く (#1649 Codex R5 Medium)', async () => {
  // 回の鍵を持たない起動 = 開く前に断る (開く口を 1 度も呼ばない)
  let opened = 0;
  const r = await cli(E, ['--daily'], { runLockHeld: () => null, openSqlite: async () => { opened++; return sq; } });
  assert.deepEqual([r.code, opened], [1, 0]); assert.match(r.last, /持つ親から起動されていない/);
  // 読むだけで開く口: readonly・書けない・無いファイルは作らずに投げる
  const ro = P.openWarehouseReadonly(tmp);
  try {
    assert.equal(ro.readonly, true);
    assert.throws(() => ro.prepare("INSERT INTO sync_meta (key, value) VALUES ('x', 'y')").run(), /readonly/i);
  } finally { ro.close(); }
  const fresh = fs.mkdtempSync(path.join(os.tmpdir(), 'amzA-ro-'));
  try {
    assert.throws(() => P.openWarehouseReadonly(fresh));
    assert.equal(fs.existsSync(path.join(fresh, 'warehouse.db')), false);
    // 本物の入口 (別のプロセス・開く口を差し替えない): 持ち主 company の Company DB の代わりで、断る直接実行と試しを流す = 無い warehouse.db を作らない
    const child = path.join(fresh, 'child.mjs');
    fs.writeFileSync(child, [
      `import { cli } from ${JSON.stringify(new URL('../apps/company-db/publish/amazon-map.mjs', import.meta.url).href)};`,
      `import { ownershipHash } from ${JSON.stringify(new URL('../lib/master-cutover.mjs', import.meta.url).href)};`,
      `import { OWNED_COLUMNS } from ${JSON.stringify(new URL('../config/master-ownership.mjs', import.meta.url).href)};`,
      `const map = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));`,
      `const db = { query: async (q) => (/to_regclass/.test(q) ? { rows: [{ ok: true }] } : /from ops\\.master_ownership_state/.test(q) ? { rows: [{ active_hash: ownershipHash(map), active_map: map, activated_at: null, activated_by: null, prepared_hash: null, prepared_map: null }] } : { rows: [] }) };`,
      `const r = await cli(process.argv.slice(3), { env: { DATA_DIR: process.argv[2], COMPANY_DB_WATCH_URL: 'postgres://x' }, connectFor: () => async () => ({ db, close: async () => {} }), log: () => {} });`,
      `console.log(JSON.stringify(r));`,
    ].join('\n'));
    const { spawnSync } = await import('node:child_process');
    const dataDir = path.join(fresh, 'data');
    fs.mkdirSync(dataDir);
    for (const argv of [['--daily'], ['--chain'], ['--dry-run']]) {
      const out = spawnSync(process.execPath, [child, dataDir, ...argv], { encoding: 'utf8', env: { ...process.env, DATA_DIR: dataDir } });
      const res = JSON.parse(out.stdout.trim().split('\n').pop());
      assert.equal(res.code, 1, `${argv} ${out.stderr}`);
      if (argv[0] !== '--dry-run') assert.match(res.last, /持つ親から起動されていない/);
      assert.deepEqual(fs.readdirSync(dataDir), [], `${argv}: warehouse.db を作った`);
    }
  } finally { fs.rmSync(fresh, { recursive: true, force: true }); }
  // 試しは今の warehouse.db を書かない (ファイルの中身が同じ)
  sq.pragma('wal_checkpoint(TRUNCATE)');
  const sha = () => crypto.createHash('sha256').update(fs.readFileSync(WH_FILE)).digest('hex');
  const h0 = sha();
  const dr = await P.cli(['--dry-run'], { env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://watcher@localhost/x' }, connectFor: () => watcherConnect(E), log: quiet });
  assert.equal(dr.code, 0, dr.last); assert.match(dr.last, /試し・書かない/);
  sq.pragma('wal_checkpoint(TRUNCATE)');
  assert.equal(sha(), h0);
});

await ta('[8c] 肯定の手がかり (#1649 Codex R3 Medium 2): config が company・有効な写しの記録 = あり / どちらも無い・読めない記録 = なし。daily-sync は手がかりが無い朝の写しの失敗 (子の timeout など) を retry に載せない', async () => {
  const LOAD = { ...ALL_LOAD };
  assert.equal(P.amazonMapHint({ dataDir: tmp, ownership: { ...LOAD, 'listing_components.amazon': 'company' }, readMeta: () => null }), 'config');
  assert.equal(P.amazonMapHint({ dataDir: tmp, ownership: LOAD, readMeta: () => null }), null);
  assert.equal(P.amazonMapHint({ dataDir: tmp, ownership: LOAD, readMeta: () => ({ unreadable: true }) }), null);
  assert.equal(P.amazonMapHint({ dataDir: tmp, ownership: LOAD, readMeta: () => { throw new Error('x'); } }), null);
  assert.equal(P.amazonMapHint({ dataDir: tmp, ownership: LOAD }), 'meta');   // 本物: この warehouse.db には有効な記録がある (写した後)
  assert.equal(P.amazonMapHint({ dataDir: path.join(tmp, 'nothing'), ownership: LOAD }), null);
  const timeout = { success: false, summary: 'status=null signal=SIGTERM code=ETIMEDOUT elapsed=300s | Command failed', exitCode: null };
  const r0 = P.dailyMapResultForRetry(timeout, null);
  assert.deepEqual([r0.success, r0.blocked], [false, true]); assert.match(r0.summary, /retry に載せない/);
  assert.equal(P.dailyMapResultForRetry(timeout, 'meta'), timeout);
  const ok = { success: true, summary: '⏭️' };
  assert.equal(P.dailyMapResultForRetry(ok, null), ok);
  const ds = fs.readFileSync(new URL('../apps/warehouse/daily-sync.js', import.meta.url), 'utf8');
  assert.match(ds, /cdbAmazonMapResult = AM\.dailyMapResultForRetry\(cdbAmazonMapResult, AM\.amazonMapHint\(/);
  assert.ok(ds.indexOf('AM.dailyMapResultForRetry') < ds.indexOf("results.push({ name: 'CompanyDB写し(Amazon SKU)'"));
});

await ta('[5b] 前の写しの記録の形が壊れている ({}・欄の欠け・型・ハッシュの形・SELECT の失敗) = 読めない = 断る → --accept-restore --expect-hash でだけ通す。行が無い = null (#1649 Codex R1 Medium 3)', async () => {
  const good = P.readMeta(sq);
  assert.ok(P.validMeta(good), JSON.stringify(good));
  const setMeta = (v) => sq.prepare('UPDATE sync_meta SET value = ? WHERE key = ?').run(v, P.META_KEY);
  const bads = ['{}', 'null', '[]', '""', '', JSON.stringify({ ...good, format: undefined }), JSON.stringify({ ...good, content_hash: 'abc' }), JSON.stringify({ ...good, content_hash: 'A'.repeat(64) }),
    JSON.stringify({ ...good, watermark: '12' }), JSON.stringify({ ...good, watermark: undefined }), JSON.stringify({ ...good, master_rows: -1 }), JSON.stringify({ ...good, component_rows: 1.5 }),
    JSON.stringify({ ...good, published_at: undefined }), JSON.stringify({ format: good.format, content_hash: good.content_hash })];
  await save('pr_a001', 'SKU マスタの 1 (記録が壊れた)', [{ code: 'a001', qty: 1 }]);
  for (const v of bads) {
    setMeta(v);
    assert.deepEqual(P.readMeta(sq), { unreadable: true }, v);
    const before = sqSnap();
    const r = await publish(E);
    assert.deepEqual([r.state, r.code, r.problems], ['refused', 1, ['meta_unreadable']], v);
    assert.equal(sqSnap(), before, v);
  }
  // SELECT が落ちる = 読めない (行が無いのと同じにしない)
  assert.deepEqual(P.readMeta({ prepare: () => { throw new Error('SQLITE_BUSY'); } }), { unreadable: true });
  assert.deepEqual(P.readMeta({ prepare: () => ({ get: () => undefined }) }), null);
  // --accept-restore --expect-hash でだけ通す (記録は正しい形に書き直される)
  const h = (await publish(E, { dryRun: true })).digest.content_hash;
  assert.equal((await cli(E, ['--chain', '--allow-shrink', '--expect-hash', h])).code, 1);
  const ok = await cli(E, ['--chain', '--accept-restore', '--expect-hash', h]);
  assert.equal(ok.code, 0, ok.last);
  assert.ok(P.validMeta(P.readMeta(sq)));
  assert.equal((await publish(E)).state, 'unchanged');
});

await ta('[9] watcher のロールは対応を読めるが書けない・入口 (cli) から daily で流す = 同じ答え・持ち主が load に戻る = 何もしない', async () => {
  await E.pg.query('set role watcher');
  try {
    await assert.rejects(E.db.query(`update core.amazon_sku_maps set name = 'x'`), /permission denied|read-only/);
  } finally { await E.pg.query('set role deploy'); }
  await save('pr_a001', 'SKU マスタの 1 (cli)', [{ code: 'a001', qty: 1 }]);
  const c = await cli(E, ['--daily']);
  assert.equal(c.code, 0, c.last); assert.match(c.last, /^✅ .*写した/);
  assert.equal(legacyHash(), await cdbHash());
  const dr = await cli(E, ['--dry-run']);
  assert.equal(dr.code, 0); assert.match(dr.last, /試し・書かない/);
  // 持ち主を load に戻した (DB の active だけ) = 何もしない
  await W2.setActiveMapOnly(E.pg, { ...ALL_COMPANY, 'listing_components.amazon': 'load' });
  await assert.rejects(() => save('pr_pack2', 'x', [{ code: 'a001', qty: 1 }]), (e) => e.status === 409);   // 画面も閉じる (持ち主が load)
  const before = sqSnap();
  const r = await publish(E);
  assert.deepEqual([r.state, r.code], ['not_applied', 0]);
  assert.equal(sqSnap(), before);
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
try { sq.close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* */ }
process.exit(process.exitCode || 0);
