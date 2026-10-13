/**
 * test-master-gone-day-pg.mjs — 0076 (「NE から消えた」の日数の許可) を実 PostgreSQL の本物のログインのロールで確かめる
 *
 * 固定する契約:
 *   [R]  master_ops (CLI) = 許可を足せる (db_user = master_ops)・許可のある SKU を 1 日でやめられる (CLI の dry-run → --apply --expect-hash) /
 *        master_edit・watcher・watch_writer = 許可を足す関数は 42501・表に直接は書けない / 許可の無い SKU は 3 日のまま
 *   [C]  許可を足す取引 (未 commit) の間に CLI の --apply が流れても、commit の前の許可は効かない (確かめ直しは commit した許可だけ = 計画どおり)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-gone-day-pg.mjs
 *   (使い捨ての PG: node C:/tmp/pg-embed/run-nedel.mjs scripts/test-master-gone-day-pg.mjs <リポジトリ>)
 *   🚨 使い捨ての PostgreSQL だけ。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles as createWatchRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { runGone } from './company-db/master-gone-from-ne.mjs';
import { runDayOverride, parseDayArgs } from './company-db/master-gone-day-override.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0076 の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
const watchdog = setTimeout(() => { console.error('  NG  試験が 600 秒で終わらない (止める)'); process.exit(1); }, 600000);
watchdog.unref();

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
const quiet = () => {};
const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', 'skus.sku_kind': 'company', 'listing_components.amazon': 'company' };   // 10/9 の本番の形
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const CODES = ['agnbathp-hw', 'smbhcg', 'smbhcs', 'keep1'];
const PLAN = { skus: CODES.map(single), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] };
const jstToday = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
try {
  const name = `cdb_goneday_${crypto.randomBytes(4).toString('hex')}`;
  await admin.query(`create database ${name}`);
  dbNames.push(name);
  const u = new URL(url); u.pathname = `/${name}`;
  const roleUrl = (role, pw) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
  const open = async (role) => { const c = await openPgClient(role ? roleUrl(role, PW[role] ?? WPW[role]) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); clients.push(c); return c; };
  const O = await open(null);
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: quiet });
  await createMasterEditRoles(O, { pw: PW });
  await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  const r0 = await runInitialLoad(dbO, PLAN, { log: quiet, runId: `goneday_load_${name}`, host: 'test' });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, BASE);
  const [MO, ME, WA, WW] = [await open('master_ops'), await open('master_edit'), await open('watcher'), await open('watch_writer')];
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const regOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))[0]?.state;
  // 今日の完全な NE の取得 1 回だけ (keep1 のほかは無い) = 1 / 3 日
  {
    const rows = (await q("select code_norm from core.skus where company_id = 1 order by 1")).filter((r) => r.code_norm === 'keep1').map((r) => [r.code_norm, 'single', 'ok', null, null]);
    const obs = { format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' }, trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' }, rows };
    const when = new Date(Math.max(Date.now() - 20 * 60e3, Date.parse(`${jstToday()}T00:01:00+09:00`))).toISOString();
    await O.query('begin');
    await O.query("select set_config('ops.parent_gate_protocol', '1', true)");
    const id = (await O.query(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
        evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by) values (1, 'mc_20301013T000000001Z_aaaaaa', 'ne_x1', repeat('1', 64), $1, $1, 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`, [when])).rows[0].result_id;
    await O.query('select ops._record_ne_absence($1, $2::jsonb)', [id, JSON.stringify(obs)]);
    await O.query('commit');
  }

  await ta('[R] 本物のログイン: master_ops = 許可を足す → 1 日で CLI (dry-run → --apply --expect-hash) でやめられる / ほかのロール = 42501 / 許可の無い SKU は 3 日のまま', async () => {
    const sig = 'select ops.add_sku_gone_day_overrides(gen_random_uuid(), $1, $2, $3::text[], 1, 7)';
    for (const [c, label] of [[ME, 'master_edit'], [WA, 'watcher'], [WW, 'watch_writer']]) {
      assert.equal((await errOf(c.query(sig, ['naka@test', 'x', ['smbhcg']])))?.code, '42501', label);
    }
    assert.equal((await errOf(MO.query(`insert into ops.sku_gone_from_ne_day_overrides (sku_id, required_days, actor_id, reason, request_id, db_user, expires_at)
      values ((select 1), 1, 'a@b', 'x', gen_random_uuid(), 't', now() + interval '1 day')`)))?.code, '42501');
    const dry0 = await runGone(pgAdapter(MO), { apply: false, codes: ['agnbathp-hw', 'smbhcg', 'smbhcs'] }, { log: quiet });
    assert.ok(dry0.checks.every((r) => !r.ok && r.problems.length === 1 && r.problems[0].reason === 'absent_short'), JSON.stringify(dry0.checks.map((r) => r.problems)));
    const add = await runDayOverride(pgAdapter(MO), parseDayArgs(['--codes', 'agnbathp-hw,smbhcg', '--days', '1', '--apply', '--actor', 'naka@test', '--reason', '決定 B (1 日前倒し)', '--yes']), { log: quiet });
    assert.equal(add.exitCode, 0, JSON.stringify(add.error));
    assert.deepEqual((await q('select distinct db_user from ops.sku_gone_from_ne_day_overrides')).map((r) => r.db_user), ['master_ops']);
    const dry = await runGone(pgAdapter(MO), { apply: false, codes: ['agnbathp-hw', 'smbhcg', 'smbhcs'] }, { log: quiet });
    assert.deepEqual(dry.checks.map((r) => [r.code, r.ok]), [['agnbathp-hw', true], ['smbhcg', true], ['smbhcs', false]]);
    const ap = await runGone(pgAdapter(MO), { apply: true, codes: ['agnbathp-hw', 'smbhcg', 'smbhcs'], expectHash: dry.planHash, actor: 'naka@test', reason: 'NE で消えた (決定 B)' }, { log: quiet });
    assert.equal(ap.exitCode, 0, JSON.stringify(ap.error));
    assert.deepEqual([await regOf('agnbathp-hw'), await regOf('smbhcg'), await regOf('smbhcs')], ['cancelled', 'cancelled', 'available']);
    const ev = (await q(`select e.evidence from ops.master_registration_events e join core.skus k on k.sku_id = e.sku_id where k.code = 'smbhcg' and e.to_state = 'cancelled'`))[0].evidence;
    assert.equal(ev.day_override.required_days, 1); assert.equal(ev.day_override.actor_id, 'naka@test');
  });

  await ta('[C] 許可を足す取引 (未 commit) の間の CLI の --apply = commit の前の許可は見えない (計画どおり 1 件も変えない・待たない)', async () => {
    const dry = await runGone(pgAdapter(MO), { apply: false, codes: ['smbhcs'] }, { log: quiet });
    assert.equal(dry.checks[0].ok, false);
    const A = await open('master_ops');
    try {
      await A.query('begin');
      await A.query('select ops.add_sku_gone_day_overrides(gen_random_uuid(), $1, $2, $3::text[], 1, 7)', ['naka@test', '決定 B', ['smbhcs']]);
      const ap = await runGone(pgAdapter(MO), { apply: true, codes: ['smbhcs'], expectHash: dry.planHash, actor: 'naka@test', reason: 'x' }, { log: quiet });
      assert.equal(ap.exitCode, 0);   // やめてよい 0 件の計画 = 計画どおり何もしない
      assert.equal(await regOf('smbhcs'), 'available');
      await A.query('commit');
    } finally { try { await A.end(); } catch { /* */ } }
    const dry2 = await runGone(pgAdapter(MO), { apply: false, codes: ['smbhcs'] }, { log: quiet });
    assert.equal(dry2.checks[0].ok, true);
    assert.notEqual(dry2.planHash, dry.planHash);
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB ${n} を消せない: ${e.message}`); } }
  try { await admin.end(); } catch { /* */ }
}
console.log(`${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
