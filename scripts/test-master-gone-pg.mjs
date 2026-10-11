/**
 * test-master-gone-pg.mjs — 0070 (業務で使える商品を「NE から消えた」でやめる) を実 PostgreSQL の独立した接続と本物のログインのロールで確かめる
 *
 * 固定する契約:
 *   [R]  本物のログイン: master_edit (画面の lib) と master_ops (CLI) = やめられる / master_ops が ops.transition_sku_registration を直に呼んでも available はやめられない /
 *        watcher = 確かめは読める・動かす関数は 42501 / どのロールも ops.master_ne_absence を直に読めない (watcher だけ読める)
 *   [C1] 競り合い (照合 ② の記録が先): 記録の取引 (watch_writer・その回にコードがある) が開いている間、やめる関数は許可の鍵で待つ → 記録が commit された後に
 *        「一番新しい回にある」で断る (待つ前に読んだ古い回の数で通さない)
 *   [C2] 競り合い (やめるが先): やめる取引が開いている間、照合 ② の記録は待つ → やめるが commit された後に記録が進む (どちらも止まらない・デッドロックしない)
 *   [C3] 夜間ロード (マスタの書き込みの鍵の排他) の間は、やめる関数は待つ (lock_timeout = 55P03 = 画面の「ほかの処理が同じ商品を使っています」)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-gone-pg.mjs
 *   (使い捨ての PG: node C:/tmp/pg-embed/run-gone.mjs scripts/test-master-gone-pg.mjs <リポジトリ>)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロールをクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles as createWatchRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen, hex } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { cancelSkuGoneFromNe, listGoneChecks } from '../lib/master-gone.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0070 の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/** p が ms の間に終わらないこと (= 鍵で待っている) */
async function stillWaiting(p, ms = 600) { const t = Symbol('t'); return (await Promise.race([p.then(() => 'done', () => 'done'), sleep(ms).then(() => t)])) === t; }

const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', 'skus.sku_kind': 'company', 'listing_components.amazon': 'company' };   // 10/9 の本番の形
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const CODES = ['p01', 'p02', 'p03', 'p04', 'p05', 'keep'];
const PLAN = { skus: CODES.map(single), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] };
let runN = 0;
const runId = () => `mc_20301011T${String(++runN).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
try {
  const name = `cdb_gone_${crypto.randomBytes(4).toString('hex')}`;
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
  const r0 = await runInitialLoad(dbO, PLAN, { log: () => {}, runId: `gone_load_${name}`, host: 'test' });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, BASE);
  const [WA, WW, ME, MO] = [await open('watcher'), await open('watch_writer'), await open('master_edit'), await open('master_ops')];
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const sid = async (code) => String((await q('select sku_id from core.skus where code = $1', [code]))[0].sku_id);
  const regOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))[0]?.state;
  const obs = async (gone) => {
    const rows = await q("select code_norm from core.skus where company_id = 1 and sku_kind = 'single' order by 1");
    return { format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' }, trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' },
      rows: rows.filter((r) => !gone.includes(r.code_norm)).map((r) => [r.code_norm, 'single', 'ok', null, null]) };
  };
  const recordSql = 'select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb) as r';
  const recordArgs = async (gone) => {
    const when = new Date(Date.now() - 1000).toISOString();
    return [runId(), JSON.stringify({ generation_id: `ne_${crypto.randomBytes(3).toString('hex')}`, raw_hash: hex('c'), products_complete_at: when, setproducts_complete_at: when }), 'mat_x', hex('d'), JSON.stringify(await obs(gone))];
  };
  const record = async (gone) => WW.query(recordSql, await recordArgs(gone));
  for (let i = 0; i < 3; i++) await record(['p01', 'p02', 'p03', 'p04', 'p05']);
  const cancel = (client, skuId, reason = 'NE で消えた') => client.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint) as r', [crypto.randomUUID(), 'naka@test', reason, skuId]);

  await ta('[R] 本物のログイン: master_edit (画面の lib)・master_ops (CLI の lib) = やめる / master_ops の transition を直に = 断る / watcher = 確かめだけ / 表は watcher だけ読める', async () => {
    const r1 = await cancelSkuGoneFromNe(pgAdapter(ME), { actor: 'naka@test', code: 'p01', reason: 'NE で消えた', requestId: crypto.randomUUID(), confirm: true }, { open: true });
    assert.equal(r1.state, 'cancelled'); assert.equal(await regOf('p01'), 'cancelled');
    const checks = await listGoneChecks(pgAdapter(MO), ['p02']);
    assert.equal(checks[0].ok, true);
    const r2 = await cancelSkuGoneFromNe(pgAdapter(MO), { actor: 'naka@test', skuId: checks[0].sku_id, reason: 'NE で消えた', requestId: crypto.randomUUID() }, { open: true, requireConfirm: false });
    assert.equal(r2.state, 'cancelled');
    assert.equal((await q('select db_user from ops.master_write_sessions where request_id = $1', [r2.request_id]))[0].db_user, 'master_ops');
    const e1 = await errOf(MO.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', 'NE で消えた')`, [await sid('p03')]));
    assert.match(e1?.message ?? '', /gone_session_required/);
    assert.equal((await WA.query('select ops.sku_gone_from_ne_check($1::bigint) as r', [await sid('p03')])).rows[0].r.ok, true);
    assert.equal((await errOf(cancel(WA, await sid('p03'))))?.code, '42501');
    for (const [c, label] of [[ME, 'master_edit'], [MO, 'master_ops'], [WW, 'watch_writer']]) {
      assert.equal((await errOf(c.query('select count(*) from ops.master_ne_absence')))?.code, '42501', label);
    }
    assert.ok(Number((await WA.query('select count(*)::int as n from ops.master_ne_absence')).rows[0].n) >= 3);
    for (const [c, label] of [[ME, 'master_edit'], [MO, 'master_ops']]) {
      assert.equal((await errOf(c.query("select ops._record_ne_absence(1, '{}'::jsonb)")))?.code, '42501', label);
    }
  });

  await ta('[C1] 照合 ② の記録が先 (p03 が NE に戻った回・未 commit): やめるは許可の鍵で待つ → 記録の commit の後に「一番新しい回にある」で断る', async () => {
    const W2 = await open('watch_writer');
    await W2.query('begin');
    await W2.query(recordSql, await recordArgs(['p04', 'p05']));   // p03 がある回
    const p = cancel(ME, await sid('p03'));
    p.catch(() => {});
    assert.equal(await stillWaiting(p), true, 'やめるが記録を待たずに進んだ');
    await W2.query('commit');
    const e = await errOf(p);
    assert.match(e?.message ?? '', /not_gone_from_ne: .*一番新しい完全な NE の取得/);
    assert.equal(await regOf('p03'), 'available');
  });

  await ta('[C2] やめるが先 (未 commit): 照合 ② の記録は待つ → やめるの commit の後に記録が進む (デッドロックしない)', async () => {
    const M2 = await open('master_edit');
    await M2.query('begin');
    await cancel(M2, await sid('p04'));
    const p = record(['p04', 'p05']);
    p.catch(() => {});
    assert.equal(await stillWaiting(p), true, '記録がやめるを待たずに進んだ');
    await M2.query('commit');
    const r = (await p).rows[0].r;
    assert.ok(r.result_id);
    assert.equal(await regOf('p04'), 'cancelled');
    assert.equal((await q("select streak from ops.master_ne_absence where code_norm = 'p04'"))[0].streak, 5);
  });

  await ta('[C3] 夜間ロード (マスタの書き込みの鍵の排他) の間はやめるが待つ = lock_timeout で 55P03 (画面の 409 locked)・何も変えない', async () => {
    const L = await open(null);
    await L.query('begin');
    await L.query('select pg_advisory_xact_lock(core.master_write_lock_key())');
    const M3 = await open('master_edit');
    await M3.query(`set lock_timeout = '500ms'`);
    const e = await errOf(cancel(M3, await sid('p05')));
    assert.equal(e?.code, '55P03', e?.message);
    await L.query('rollback');
    assert.equal(await regOf('p05'), 'available');
    const ok = (await cancel(M3, await sid('p05'))).rows[0].r;
    assert.equal(ok.state, 'cancelled');
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB ${n} を消せない: ${e.message}`); } }
  try { await admin.end(); } catch { /* */ }
}
console.log(`${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
