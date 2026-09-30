/**
 * test-master-legacy-gate-pg.mjs — マスタの古い入口の門の「段階の読み方」を、実 PostgreSQL と本物の接続 (pg のプール) で確かめる
 *   (PGlite は TCP の接続・プール・文の打ち切り・接続の切断を試せない。PR #1565 Codex R1 H1・M6 の試験)
 *
 * 固定する契約:
 *   1 miniPC と同じ照会用のロール watcher (select だけ・既定で読むだけの取引) で、本物の読み方 (プール) が段階を読める。legacy_open = 書ける
 *   2 段階を frozen にした直後の書き込みは閉じる (前に読めた legacy_open を使わない = 毎回読む)
 *   3 同時に 20 件来ても、この門の接続は 2 本まで (watcher の接続は 3 本まで = 見張り・照合と食い合わない)
 *   4 DB 側から門の接続を切られても、次の読みはつなぎ直して読める (途切れで止まり続けない)
 *   5 段階の表を別の取引がつかんで返事が無い = 文の打ち切り (5 秒) + 1 回の読み直しで閉じる (待ち続けない・通さない)
 *   6 CLI の門も本物の読み方で閉じる (終了コード 3)
 *   7 門の記録: 書く前の確かめ (段階を読める・build の番号・書く接続先 = 記録用のロール watch_writer) を通り、⑤-1 の書き手の関数がまだ無い = no_function (何も書かない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-legacy-gate-pg.mjs
 *   (cd C:/tmp/pg-embed && node run-conc.mjs scripts/test-master-legacy-gate-pg.mjs C:/tmp/sor53-work)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。package.json の test:company-db には入れない
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createRoles, urlFor } from './company-db/create-watch-roles.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の門の試験は飛ばす。PGlite と偽の読み方の試験は scripts/test-master-legacy-gate.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
for (const k of ['COMPANY_DB_URL', 'COMPANY_DB_WATCH_URL', 'COMPANY_DB_WATCH_WRITER_URL']) delete process.env[k];

const G = await import('../lib/master-legacy-gate.mjs');
let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = async (fn) => { const w = console.warn, l = console.log, e = console.error; console.warn = console.log = console.error = () => {}; try { return await fn(); } finally { console.warn = w; console.log = l; console.error = e; } };

const dbName = `cdb_lg_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const M = await openPgClient(u.toString());
M.on('error', (e) => console.error(`[pg] ${e.message}`));
const db = pgAdapter(M);
const setPhase = async (phase) => {
  await M.query('begin');
  await M.query(`select set_config('ops.cutover_protocol', '1', true)`);
  await M.query(`update ops.master_cutover_state set phase = $1, owner_hash = $2 where id = 1`, [phase, ['company_owner', 'new_open'].includes(phase) ? 'a'.repeat(64) : null]);
  await M.query('commit');
};
const gateConns = async () => Number((await M.query(`select count(*)::int as n from pg_stat_activity where datname = $1 and application_name = 'master-legacy-gate'`, [dbName])).rows[0].n);
try {
  await applyMigrations(db, { log: () => {} });
  await createRoles(M, { watcherPw: 'w-pw', writerPw: 'ww-pw' });
  process.env.COMPANY_DB_WATCH_URL = urlFor(u.toString(), 'watcher', 'w-pw');
  G.__setLegacyPhaseReader(null);   // 本物の読み方 (プール)

  await ta('[1] 照会用のロール watcher と本物の読み方 (プール) で段階を読める。legacy_open = 書ける', async () => {
    const s = await G.checkLegacyGate();
    assert.deepEqual([s.readable, s.phase, s.writable, s.source], [true, 'legacy_open', true, 'db'], s.error);
    const who = (await M.query(`select usename from pg_stat_activity where datname = $1 and application_name = 'master-legacy-gate'`, [dbName])).rows.map((r) => r.usename);
    assert.ok(who.length >= 1 && who.every((x) => x === 'watcher'), JSON.stringify(who));
  });
  await ta('[2] frozen にした直後の書き込みは閉じる (前に読めた legacy_open を使わない)', async () => {
    assert.equal((await G.checkLegacyGate()).writable, true);
    await setPhase('frozen');
    const s = await G.checkLegacyGate();
    assert.deepEqual([s.phase, s.writable], ['frozen', false]);
    await setPhase('legacy_open');
    assert.equal((await G.checkLegacyGate()).writable, true);
  });
  await ta('[3] 同時に 20 件来ても門の接続は 2 本まで (プールを使い回す)', async () => {
    const rs = await Promise.all(Array.from({ length: 20 }, () => G.checkLegacyGate()));
    assert.ok(rs.every((s) => s.writable === true));
    const n = await gateConns();
    assert.ok(n >= 1 && n <= 2, `門の接続 ${n} 本`);
  });
  await ta('[4] DB 側から門の接続を切られても、次の読みはつなぎ直して読める', async () => {
    await M.query(`select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and application_name = 'master-legacy-gate'`, [dbName]);
    await new Promise((r) => setTimeout(r, 200));
    const s = await quiet(() => G.checkLegacyGate());
    assert.deepEqual([s.readable, s.writable], [true, true], s.error);
  });
  await ta('[5] 段階の表を別の取引がつかんで返事が無い = 文の打ち切り (5 秒) + 1 回の読み直しで閉じる (待ち続けない・通さない)', async () => {
    const L = await openPgClient(u.toString());
    L.on('error', () => {});
    try {
      await L.query('begin');
      await L.query('lock table ops.master_cutover_state in access exclusive mode');
      const t0 = Date.now();
      const s = await quiet(() => G.checkLegacyGate());
      const ms = Date.now() - t0;
      assert.deepEqual([s.readable, s.writable], [false, false]);
      assert.ok(ms >= 4500 && ms < 15000, `${ms}ms`);
      assert.ok(G.legacyGateStats().retries >= 1);
    } finally { try { await L.query('rollback'); } catch { /* */ } await L.end(); }
    assert.equal((await G.checkLegacyGate()).writable, true, '放したら読める');
  });
  await ta('[6] CLI の門も本物の読み方で閉じる (frozen = 終了コード 3)', async () => {
    await setPhase('frozen');
    const saved = process.exitCode;
    try {
      assert.equal(await quiet(() => G.legacyCliGate('cli:import-sales-class.js', { log: () => {} })), false);
      assert.equal(process.exitCode, G.CLI_EXIT_CODE);
    } finally { process.exitCode = saved; }
    await setPhase('legacy_open');
    assert.equal(await G.legacyCliGate('cli:import-sales-class.js', { log: () => {} }), true);
  });
  await ta('[7] 門の記録: 書く前の確かめ (段階を読める・build の番号・記録用のロール watch_writer の接続) を通り、書き手の関数がまだ無い = no_function (何も書かない)', async () => {
    const env = { ...process.env, COMPANY_DB_WATCH_WRITER_URL: urlFor(u.toString(), 'watch_writer', 'ww-pw'), RENDER_GIT_COMMIT: 'b'.repeat(40) };
    const before = Number((await M.query('select count(*)::int as n from ops.master_legacy_gate_acks')).rows[0].n);
    const r = await quiet(() => G.ackLegacyGates({ host: 'minipc', env }));
    assert.equal(r.state, 'no_function', JSON.stringify(r));
    assert.equal(Number((await M.query('select count(*)::int as n from ops.master_legacy_gate_acks')).rows[0].n), before);
  });
} finally {
  G.__setLegacyPhaseReader(null);
  await G.closeLegacyGatePool();
  await M.end();
  try { await admin.query(`select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()`, [dbName]); await admin.query(`drop database ${dbName}`); } catch (e) { console.error(`後始末: ${e.message}`); }
  await admin.end();
}
console.log(process.exitCode ? `\n❌ 失敗あり (${passed} 件 OK)` : `\n✅ ${passed} 件 OK`);
