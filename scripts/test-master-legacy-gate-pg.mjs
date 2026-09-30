/**
 * test-master-legacy-gate-pg.mjs — マスタの古い入口の門を、実 PostgreSQL と本物の接続 (pg のプール) と ⑤-1 の本物の関数で確かめる
 *   (PGlite は TCP の接続・プール・文の打ち切り・接続の切断・ログインの役を試せない。PR #1565 Codex R1 H1・M6 / 中間レビューの試験)
 *
 * 固定する契約:
 *   1 照会用のロール watcher (select だけ) で、本物の読み方 (プール) が段階を読める。legacy_open = 書ける
 *   2 段階を frozen にした直後の書き込みは閉じる (前に読めた legacy_open を使わない = 毎回読む)
 *   3 同時に 20 件来ても、この門の接続は 1 本 (プールは 1 本・見張り・照合の watcher の接続と食い合わない)
 *   4 DB 側から門の接続を切られても、次の読みはつなぎ直して読める (途切れで止まり続けない)
 *   5 段階の表を別の取引がつかんで返事が無い = 文の打ち切り (5 秒) + 1 回の読み直しで閉じる (待ち続けない・通さない)
 *   6 CLI の門も本物の読み方で閉じる (終了コード 3)
 *   7 CLI は書いている間ずっと段階の共有の鍵を持つ = 段階を変える (排他の鍵) は CLI が書き終わるまで待つ。
 *     段階を変えている最中に始めた CLI は、変え終わるのを待ってから読む = frozen なら書かない
 *   8 門の記録: 場所ごとの門のログイン (master_gate_minipc = env COMPANY_DB_MASTER_GATE_MINIPC_URL) で ⑤-1 の本物の関数
 *     ops.record_legacy_gate_ack に書く。返事 (ack_id・DB が計算した manifest_hash・acked_at) を確かめる。段階の読みもこのログイン
 *   9 書く間に段階が変わった (関数が stale_phase で拒む) = 読み直して 1 回書き直す
 *  10 止めるとき (SIGTERM / SIGINT) の「止めた」の記録と、人が別のプロセスを「止めた」にする (scripts/company-db/master-legacy-instance.mjs)
 *  11 配る前の確かめ (scripts/company-db/master-legacy-readiness.mjs): そろっていれば ok・場所と役が違えば「足りない」
 *  12 門のログインの接続は、同時に 20 件の読み + 記録を書いても 2 本まで (プール 1 本 + 記録 1 本)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-legacy-gate-pg.mjs
 *   (cd C:/tmp/pg-embed && node run-conc.mjs scripts/test-master-legacy-gate-pg.mjs C:/tmp/sor53-work)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。package.json の test:company-db には入れない
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createRoles, urlFor } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles, MASTER_EDIT_ROLES } from './company-db/create-master-edit-roles.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の門の試験は飛ばす。PGlite と偽の読み方の試験は scripts/test-master-legacy-gate.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
for (const k of ['COMPANY_DB_URL', 'COMPANY_DB_WATCH_URL', 'COMPANY_DB_WATCH_WRITER_URL', 'COMPANY_DB_MASTER_GATE_RENDER_URL', 'COMPANY_DB_MASTER_GATE_MINIPC_URL', 'COMPANY_DB_MASTER_OPS_URL', 'RENDER_GIT_COMMIT', 'RENDER_INSTANCE_ID']) delete process.env[k];

const G = await import('../lib/master-legacy-gate.mjs');
const { readCutoverPhase } = await import('../lib/master-cutover.mjs');
let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = async (fn) => { const w = console.warn, l = console.log, e = console.error; console.warn = console.log = console.error = () => {}; try { return await fn(); } finally { console.warn = w; console.log = l; console.error = e; } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
const connsOf = async (where, params) => Number((await M.query(`select count(*)::int as n from pg_stat_activity where datname = $1 and ${where}`, [dbName, ...params])).rows[0].n);
const gateConns = () => connsOf(`application_name = 'master-legacy-gate'`, []);
const ackCount = async () => Number((await M.query('select count(*)::int as n from ops.master_legacy_gate_acks')).rows[0].n);
try {
  await applyMigrations(db, { log: () => {} });
  await createRoles(M, { watcherPw: 'w-pw', writerPw: 'ww-pw' });
  // ⑤-1 の門のロール。場所ごとのログイン (master_gate_render / master_gate_minipc = master_gate の権限を継ぐ) が
  // まだ作られない版の ⑤-1 なら、ここで作る (⑤-1 の新しい形に合わせる。ある版ならそのまま使う)
  const pw = Object.fromEntries([...MASTER_EDIT_ROLES, 'master_gate_render', 'master_gate_minipc'].map((r) => [r, `${r}-pw`]));
  await createMasterEditRoles(M, { pw });
  for (const h of ['render', 'minipc']) {
    const role = `master_gate_${h}`;
    if (!(await M.query('select 1 from pg_roles where rolname = $1', [role])).rowCount) await M.query(`create role ${role} login inherit password '${role}-pw' in role master_gate`);
  }
  const gateUrl = (h) => urlFor(u.toString(), `master_gate_${h}`, `master_gate_${h}-pw`);
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
  await ta('[3] 同時に 20 件来ても門の接続は 1 本 (watcher の接続を食わない)', async () => {
    const rs = await Promise.all(Array.from({ length: 20 }, () => G.checkLegacyGate()));
    assert.ok(rs.every((s) => s.writable === true));
    const n = await gateConns();
    assert.equal(n, 1, `門の接続 ${n} 本`);
  });
  await ta('[4] DB 側から門の接続を切られても、次の読みはつなぎ直して読める', async () => {
    await M.query(`select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and application_name = 'master-legacy-gate'`, [dbName]);
    await sleep(200);
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
  await ta('[7] CLI は書いている間ずっと段階の共有の鍵を持つ (段階を変える排他の鍵は待つ)・段階を変えている最中に始めた CLI は待ってから読む = frozen なら書かない', async () => {
    const L = await openPgClient(u.toString());
    L.on('error', () => {});
    try {
      // (a) CLI が書いている間 = 段階を変える側 (排他の鍵) は取れない
      let release;
      const hold = new Promise((r) => { release = r; });
      let entered = false;
      const run = G.runWithLegacyCliLock('cli:import-sales-class.js', async () => { entered = true; await hold; return 'wrote'; }, { log: () => {} });
      for (let i = 0; i < 50 && !entered; i++) await sleep(50);
      assert.ok(entered, 'legacy_open = 書く');
      await L.query('begin');
      await L.query(`set local lock_timeout = '300ms'`);
      await assert.rejects(() => L.query(`select pg_advisory_xact_lock(hashtext('ops.master_cutover'))`), /lock timeout|canceling statement/i);
      await L.query('rollback');
      release();
      assert.deepEqual(await run, { ran: true, result: 'wrote' });
      await L.query('begin');
      await L.query(`set local lock_timeout = '2s'`);
      await L.query(`select pg_advisory_xact_lock(hashtext('ops.master_cutover'))`);
      await L.query('rollback');
      // (b) 段階を変えている最中 (排他の鍵を持って frozen に変えた・まだ commit していない) に始めた CLI = 待つ → commit 後に frozen を読む = 書かない
      await L.query('begin');
      await L.query(`select pg_advisory_xact_lock(hashtext('ops.master_cutover'))`);
      await L.query(`select set_config('ops.cutover_protocol', '1', true)`);
      await L.query(`update ops.master_cutover_state set phase = 'frozen' where id = 1`);
      let called = false;
      const saved = process.exitCode;
      const run2 = G.runWithLegacyCliLock('cli:import-sales-class.js', async () => { called = true; }, { log: () => {} });
      await sleep(500);
      assert.equal(called, false, '鍵を待っている');
      await L.query('commit');
      const r2 = await run2;
      assert.equal(r2.ran, false); assert.equal(called, false, 'frozen = 書かない');
      assert.equal(process.exitCode, G.CLI_EXIT_CODE);
      process.exitCode = saved;
    } finally { try { await L.query('rollback'); } catch { /* */ } await L.end(); }
    await setPhase('legacy_open');
  });

  // ─── 門の記録 (場所ごとの門のログイン・⑤-1 の本物の関数) ───
  process.env.COMPANY_DB_MASTER_GATE_MINIPC_URL = gateUrl('minipc');
  await G.closeLegacyGatePool();
  await sleep(300);   // 閉じた接続が pg_stat_activity から消えるまで
  const env = { ...process.env, RENDER_GIT_COMMIT: 'b'.repeat(40), RENDER_INSTANCE_ID: 'pg-test' };
  await ta('[8] 門の記録: miniPC の門のログイン (master_gate_minipc) で ⑤-1 の関数に書く・返事を確かめる・段階の読みもこのログイン', async () => {
    G.__resetLegacyAck();
    const before = await ackCount();
    const r = await G.ackLegacyGates({ host: 'minipc', env });
    assert.equal(r.state, 'acked', JSON.stringify(r));
    assert.equal(await ackCount(), before + 1);
    const row = (await M.query(`select a.host, a.instance_id, a.build_id, a.phase_seen, a.inflight_count, a.manifest_hash, a.owner_hash, m.entries
      from ops.master_legacy_gate_acks a join ops.master_legacy_manifests m using (manifest_hash) where a.ack_id = $1`, [r.ack_id])).rows[0];
    assert.deepEqual([row.host, row.build_id, row.phase_seen, row.inflight_count], ['minipc', 'b'.repeat(40), 'legacy_open', 0]);
    assert.match(row.instance_id, /^pg-test:\d+:[0-9a-f]{8}$/);
    assert.equal(row.manifest_hash, r.manifest_hash);
    assert.equal(row.manifest_hash, (await M.query('select ops.legacy_manifest_hash($1::jsonb) as h', [JSON.stringify(G.legacyManifest())])).rows[0].h, 'DB が同じ一覧から計算した値');
    assert.deepEqual(row.entries.entries.filter((x) => x.kind === 'manual').map((x) => x.id), ['ne:item-screen', 'gas:logizard-sheet-and-sku-map']);
    const who = (await M.query(`select distinct usename from pg_stat_activity where datname = $1 and application_name = 'master-legacy-gate'`, [dbName])).rows.map((x) => x.usename);
    assert.deepEqual(who, ['master_gate_minipc'], '段階の読みも門のログイン');
    // 読み戻しの manifest_hash = 最後に DB が受け取った一覧
    assert.equal((await G.legacyGateFingerprint()).manifest_hash, r.manifest_hash);
  });
  await ta('[9] 書く間に段階が変わった (⑤-1 の関数が stale_phase で拒む) = 読み直して 1 回書き直す', async () => {
    await setPhase('frozen');
    let n = 0;
    G.__setLegacyPhaseReader(async () => (n++ === 0 ? { readable: true, phase: 'legacy_open' } : readCutoverPhase(db)));
    try {
      const r = await quiet(() => G.ackLegacyGates({ host: 'minipc', env }));
      assert.equal(r.state, 'acked', JSON.stringify(r));
      assert.equal((await M.query('select phase_seen from ops.master_legacy_gate_acks where ack_id = $1', [r.ack_id])).rows[0].phase_seen, 'frozen');
      assert.ok(n >= 2, '読み直した');
    } finally { G.__setLegacyPhaseReader(null); await setPhase('legacy_open'); }
  });
  await ta('[10] 止めるときの「止めた」(書きかけ 0) と、人が別のプロセスを「止めた」にする (名札を指定)・一覧で見える', async () => {
    const end = G.beginLegacyWrite('warehouse:POST:/api/shipping');
    let r;
    try { r = await G.ackLegacyGatesStopped({ host: 'minipc', reason: 'SIGTERM (試験)', env, timeoutMs: 5000 }); } finally { end(); }
    assert.equal(r.state, 'stopped', JSON.stringify(r));
    assert.equal((await M.query('select inflight_count from ops.master_legacy_gate_acks where ack_id = $1', [r.ack_id])).rows[0].inflight_count, 0);
    const { markStopped, listInstances } = await import('./company-db/master-legacy-instance.mjs');
    const r2 = await markStopped({ host: 'minipc', instance: 'ghost-pc:999:deadbeef', reason: '電源が切れて戻らない (試験)', env });
    assert.equal(r2.state, 'stopped', JSON.stringify(r2));
    const rows = await listInstances(M, { hours: 24 });
    assert.ok(rows.some((x) => x.instance_id === 'ghost-pc:999:deadbeef' && x.host === 'minipc' && x.fresh === true), JSON.stringify(rows));
    await assert.rejects(() => markStopped({ host: 'render', instance: 'x:1:2', reason: '試験', env: { ...env, COMPANY_DB_MASTER_GATE_RENDER_URL: '' } }), /COMPANY_DB_MASTER_GATE_RENDER_URL/);
  });
  await ta('[11] 配る前の確かめ: そろっていれば ok・場所と役が違えば「足りない」・build が分からなければ「足りない」(何も書かない)', async () => {
    const { checkReadiness } = await import('./company-db/master-legacy-readiness.mjs');
    const before = await ackCount();
    let r = await checkReadiness({ host: 'minipc', env });
    assert.equal(r.ok, true, r.lines.join('\n'));
    r = await checkReadiness({ host: 'render', env: { ...env, COMPANY_DB_MASTER_GATE_RENDER_URL: gateUrl('minipc') } });
    assert.equal(r.ok, false); assert.ok(r.problems.some((x) => /master_gate_minipc/.test(x) && /期待 master_gate_render/.test(x)), r.lines.join('\n'));
    r = await checkReadiness({ host: 'minipc', env: { COMPANY_DB_WATCH_URL: process.env.COMPANY_DB_WATCH_URL } });
    assert.equal(r.ok, false); assert.ok(r.problems.some((x) => /COMPANY_DB_MASTER_GATE_MINIPC_URL/.test(x)));
    assert.equal(await ackCount(), before, '確かめは書かない');
  });
  await ta('[12] 門のログインの接続は、同時に 20 件の読み + 記録を書いても 2 本まで (プール 1 本 + 記録 1 本)', async () => {
    await sleep(300);   // 前の試験の接続が消えるまで
    let peak = 0;
    let stop = false;
    const watch = (async () => { while (!stop) { peak = Math.max(peak, await connsOf('usename = $2', ['master_gate_minipc'])); await sleep(5); } })();
    try {
      G.__resetLegacyAck();
      const [rs, ack] = await Promise.all([Promise.all(Array.from({ length: 20 }, () => G.checkLegacyGate())), G.ackLegacyGates({ host: 'minipc', env })]);
      assert.ok(rs.every((s) => s.writable === true)); assert.equal(ack.state, 'acked', JSON.stringify(ack));
    } finally { stop = true; await watch; }
    assert.ok(peak >= 1 && peak <= 2, `門のログインの接続 ${peak} 本`);
  });
} finally {
  G.__setLegacyPhaseReader(null);
  await G.closeLegacyGatePool();
  await M.end();
  try { await admin.query(`select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()`, [dbName]); await admin.query(`drop database ${dbName}`); } catch (e) { console.error(`後始末: ${e.message}`); }
  await admin.end();
}
console.log(process.exitCode ? `\n❌ 失敗あり (${passed} 件 OK)` : `\n✅ ${passed} 件 OK`);
