/**
 * test-po-settings-snapshot-pg.mjs — 発注の設定の写し (0076・CompanyDB構想 21 C2) の**同時実行**と**ロールの権限**を実 PostgreSQL の独立した接続で確かめる
 * 固定する契約:
 *   P1 同時に 2 本の写し = 順番 (鍵)・後の方は前の世代と比べる (同じ値 = unchanged・行は 1 回分)・遅れて来た古い読み = 入れ替えない
 *   P2 写しを入れている取引の途中 = 見張り (watcher の本物のログイン) は前の世代を読む (入れ替わるのは commit の時に一度に)
 *   P3 本物のログインの権限: watcher は読める・書けない / master_edit は purchasing を読めない・書けない
 * 使い方: node C:/tmp/pg-embed/run-posync.mjs scripts/test-po-settings-snapshot-pg.mjs <作業の木>
 *   🚨 localhost 以外の URL は拒む。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import path from 'node:path';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (発注の設定の写しの実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-po-settings-snapshot.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
const killer = setTimeout(() => { console.error('NG 時間切れ (300 秒)'); process.exit(3); }, 300e3);
killer.unref();
const DATA_DIR = await temporaryTestDataDir(import.meta.url, 'posync-snappg-');
process.env.DATA_DIR = DATA_DIR;
const S = await import('../apps/company-db/load/po-settings-snapshot.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const podb = (await import('../apps/purchase-orders/db.js')).getDB();
const FILE = path.join(DATA_DIR, 'warehouse-mirror.db');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const codeOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e.code; } return null; };

const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `x_${crypto.randomBytes(12).toString('hex')}` };
const CODES = Array.from({ length: 25 }, (_, i) => `sp-${String(i + 1).padStart(2, '0')}`);
const sku = (code) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const dbName = `posyncsnap_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role] || WPW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const T0 = '2026-10-01T00:00:00.000Z';
const ins = podb.prepare('INSERT INTO po_product_attrs (product_key, product_code, order_lot, created_at, updated_at) VALUES (?,?,?,?,?)');
CODES.forEach((c, i) => ins.run(c, c, i + 1, T0, T0));
podb.prepare('INSERT INTO po_order_conditions (condition_id, supplier_code, display_name, condition_type, condition_value, unit, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)').run('c-1', '1', '条件', '金額', 1, '円', T0, T0);
podb.prepare('INSERT INTO po_material_groups (group_id, name, created_at, updated_at) VALUES (?,?,?,?)').run('m-1', '原料', T0, T0);

try {
  await applyMigrations(pgAdapter(M), { log: () => {} });
  await createRoles(M, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  await createMasterEditRoles(M, { pw: PW });
  const lr = await runInitialLoad(pgAdapter(M), { skus: CODES.map(sku), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] }, { log: () => {}, runId: 'load_snappg', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(lr.ok, true, lr.error);
  const [A, B, W, E] = [await open(null), await open(null), await open('watcher'), await open('master_edit')];
  clients.push(A, B, W, E);

  await ta('[P1] 同時に 2 本の写し = 鍵で順番・後の方は前の世代と比べて unchanged (行は 1 回分)・遅れて来た古い読み = 入れ替えない', async () => {
    const s1 = S.readPoSettingsSnapshot(FILE);
    await sleep(5);
    const s2 = S.readPoSettingsSnapshot(FILE);
    const [r1, r2] = await Promise.all([S.applyPoSettingsSnapshot(pgAdapter(A), s1), S.applyPoSettingsSnapshot(pgAdapter(B), s2)]);
    const st = [r1, r2].map((r) => r.status).sort();
    // どちらが先に鍵を取っても: 先 = applied / 後 = (新しい読みなら) unchanged・(古い読みなら) rejected (older_read)
    assert.ok(JSON.stringify(st) === JSON.stringify(['applied', 'unchanged']) || JSON.stringify(st) === JSON.stringify(['applied', 'rejected']), JSON.stringify([r1, r2].map((r) => [r.status, r.problems])));
    const rej = [r1, r2].find((r) => r.status === 'rejected');
    if (rej) assert.match(rej.problems.join(), /^older_read/);
    assert.equal((await M.query('select count(*)::int as n from purchasing.sku_order_settings')).rows[0].n, 25);
    assert.equal((await M.query(`select count(distinct run_id)::int as n from purchasing.sku_order_settings`)).rows[0].n, 1, '同じ行を 2 回入れた');
    // 後から古い読みを入れる = 断る (新しい値を戻さない)
    podb.prepare(`UPDATE po_product_attrs SET order_lot = 99, updated_at = ? WHERE product_key = 'sp-01'`).run('2026-10-02T00:00:00.000Z');
    const old = S.readPoSettingsSnapshot(FILE);
    podb.prepare(`UPDATE po_product_attrs SET order_lot = 100, updated_at = ? WHERE product_key = 'sp-01'`).run('2026-10-03T00:00:00.000Z');
    await sleep(5);
    assert.equal((await S.applyPoSettingsSnapshot(pgAdapter(A), S.readPoSettingsSnapshot(FILE))).status, 'applied');
    const r3 = await S.applyPoSettingsSnapshot(pgAdapter(B), old);
    assert.equal(r3.status, 'rejected');
    assert.equal(Number((await M.query(`select order_lot from purchasing.sku_order_settings where product_key = 'sp-01'`)).rows[0].order_lot), 100);
  });

  await ta('[P2] 写しを入れている取引の途中 = 見張りは前の世代を読む (commit の時に一度に入れ替わる)', async () => {
    podb.prepare(`UPDATE po_product_attrs SET order_lot = 7, updated_at = ? WHERE product_key = 'sp-02'`).run('2026-10-04T00:00:00.000Z');
    const snap = S.readPoSettingsSnapshot(FILE);
    // A の取引で入れる途中 (commit の前) に止める: applyPoSettingsSnapshot は自分で begin / commit するので、commit を遅らせる包みで
    let release; const hold = new Promise((r) => { release = r; });
    const slow = { query: async (t, p) => { if (t === 'commit') await hold; return A.query(t, p); }, exec: (t) => A.query(t) };
    const run = S.applyPoSettingsSnapshot(slow, snap);
    await sleep(500);
    const seen = Number((await W.query(`select order_lot from purchasing.sku_order_settings where product_key = 'sp-02'`)).rows[0].order_lot);
    assert.equal(seen, 2, '入れる途中の値が見えた');
    release();
    assert.equal((await run).status, 'applied');
    assert.equal(Number((await W.query(`select order_lot from purchasing.sku_order_settings where product_key = 'sp-02'`)).rows[0].order_lot), 7);
  });

  await ta('[P3] 本物のログインの権限: watcher = 読める・書けない / master_edit = purchasing を読めない・書けない', async () => {
    assert.ok((await W.query('select count(*)::int as n from purchasing.snapshot_runs')).rows[0].n >= 2);
    assert.ok((await W.query(`select po_order_lot from mart.v_product_360 where sku_code = 'sp-02'`)).rows[0].po_order_lot);
    assert.equal(await codeOf(W, `update purchasing.sku_order_settings set order_lot = 1`), '25006', '読むだけのロールが書けた (read only の取引)');
    await W.query('begin read write').catch(() => {});
    assert.ok(['42501', '25006'].includes(await codeOf(W, `delete from purchasing.material_groups`)));
    await W.query('rollback').catch(() => {});
    assert.equal(await codeOf(E, 'select 1 from purchasing.sku_order_settings limit 1'), '42501');
    assert.equal(await codeOf(E, `insert into purchasing.material_groups (group_id, name, run_id) values ('x', 'x', 'x')`), '42501');
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.log('NG があります');
setTimeout(() => process.exit(process.exitCode || 0), 50).unref();
