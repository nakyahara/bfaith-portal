/**
 * test-master-legacy-gate-pg.mjs — マスタの古い入口の門を、実 PostgreSQL と本物の接続 (pg のプール) と ⑤-1 の本物の関数で確かめる
 *   (PGlite は TCP の接続・プール・文の打ち切り・接続の切断・ログインの役を試せない。PR #1565 Codex R1 H1・M6 / 中間レビューの試験)
 *
 * 固定する契約:
 *   1 門のログイン (master_gate_minipc) で、本物の読み方 (プール) が段階を読める。watcher の env があっても使わない (接続 3 本までを食わない)。legacy_open = 書ける
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
 *  13 env の取り違え (miniPC の env に Render のログイン) = ⑤-1 の関数が gate_host_mismatch で拒む・何も書かない
 *  14 ⑤-3 の記録で ⑤-1 の段階の関数が frozen に進める。黙っているプロセス (今までに記録・最後が 15 分より前 (何日前でも)・止めたでもない) があれば拒む
 *     → scripts/company-db/master-legacy-instance.mjs で「止めた」を書けば進める
 *  15 読む時間を測る (scripts/company-db/master-legacy-latency.mjs・読むだけ)
 *  16 配り直し (古いプロセスの普通の記録が書いている途中に SIGTERM・新しいプロセスが起動): 古いプロセスは途中の記録を待ってから「止めた」を 1 回。
 *     門のログインの接続は 古い 1 本 + 新しい 1 本 = 2 本まで。古いプロセスの最後の記録は stopped (Codex #1565 R2 Medium 3)
 * ロールは ⑤-1 の本物の作り (scripts/company-db/create-master-edit-roles.mjs) だけで作る (試験で足さない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-legacy-gate-pg.mjs
 *   (cd C:/tmp/pg-embed && node run-conc.mjs scripts/test-master-legacy-gate-pg.mjs C:/tmp/sor53-work)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。package.json の test:company-db には入れない
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createRoles, urlFor } from './company-db/create-watch-roles.mjs';
import { createMasterEditRoles, GATE_LOGIN_ROLES } from './company-db/create-master-edit-roles.mjs';

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
  // ⑤-1 のロールの作り (本物の scripts/company-db/create-master-edit-roles.mjs = まとめの master_gate (ログインなし) と場所ごとのログイン)。
  // 試験は門のログインのパスワードだけ決めて渡す (ほかのロールは作りに任せる)
  await createMasterEditRoles(M, { pw: Object.fromEntries(Object.values(GATE_LOGIN_ROLES).map((r) => [r, `${r}-pw`])) });
  const gateUrl = (h) => urlFor(u.toString(), `master_gate_${h}`, `master_gate_${h}-pw`);
  process.env.COMPANY_DB_WATCH_URL = urlFor(u.toString(), 'watcher', 'w-pw');   // --stop の確かめ (見るだけ) に使う。門の段階の読みには使わない
  process.env.COMPANY_DB_MASTER_GATE_MINIPC_URL = gateUrl('minipc');
  G.__setLegacyPhaseReader(null);   // 本物の読み方 (プール)

  await ta('[1] 門のログイン (master_gate_minipc) と本物の読み方 (プール) で段階を読める。watcher の env があっても使わない。legacy_open = 書ける', async () => {
    const s = await G.checkLegacyGate();
    assert.deepEqual([s.readable, s.phase, s.writable, s.source], [true, 'legacy_open', true, 'db'], s.error);
    const who = (await M.query(`select usename from pg_stat_activity where datname = $1 and application_name = 'master-legacy-gate'`, [dbName])).rows.map((r) => r.usename);
    assert.ok(who.length >= 1 && who.every((x) => x === 'master_gate_minipc'), JSON.stringify(who));
    // 門の env が無い = COMPANY_DB_URL だけ (watcher には落ちない)
    assert.equal(G.phaseUrlFrom({ COMPANY_DB_WATCH_URL: 'postgres://w@x/y' }), null);
    assert.equal(G.phaseUrlFrom({ COMPANY_DB_WATCH_URL: 'postgres://w@x/y', COMPANY_DB_URL: 'postgres://o@x/y' }), 'postgres://o@x/y');
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
    assert.deepEqual(Object.values((await M.query('select inflight_count, stopped, stopped_reason, session_role from ops.master_legacy_gate_acks where ack_id = $1', [r.ack_id])).rows[0]), [0, true, 'SIGTERM (試験)', 'master_gate_minipc']);
    const { markStopped, listInstances } = await import('./company-db/master-legacy-instance.mjs');
    const r2 = await markStopped({ host: 'minipc', instance: 'ghost-pc:999:deadbeef', reason: '電源が切れて戻らない (試験)', env });
    assert.equal(r2.state, 'stopped', JSON.stringify(r2));
    const rows = await listInstances(M, { hours: 24 });
    assert.ok(rows.some((x) => x.instance_id === 'ghost-pc:999:deadbeef' && x.host === 'minipc' && x.stopped === true && x.stopped_reason === '電源が切れて戻らない (試験)'), JSON.stringify(rows));
    await assert.rejects(() => markStopped({ host: 'render', instance: 'x:1:2', reason: '試験', env: { ...env, COMPANY_DB_MASTER_GATE_RENDER_URL: '' } }), /COMPANY_DB_MASTER_GATE_RENDER_URL/);
    // 15 分以内に記録があるプロセス (= 動いているかもしれない) は --force なしでは「止めた」にしない (見る接続 = watcher)
    G.__resetLegacyAck();
    const live = await G.ackLegacyGates({ host: 'minipc', env, instance: null });
    assert.equal(live.state, 'acked');
    const liveId = (await M.query('select instance_id from ops.master_legacy_gate_acks where ack_id = $1', [live.ack_id])).rows[0].instance_id;
    const before = await ackCount();
    await assert.rejects(() => markStopped({ host: 'minipc', instance: liveId, reason: '試験', env }), /15 分以内/);
    assert.equal(await ackCount(), before, '書かない');
  });
  await ta('[11] 配る前の確かめ: そろっていれば ok・場所と役が違えば「足りない」・build が分からなければ「足りない」(何も書かない)', async () => {
    const { checkReadiness } = await import('./company-db/master-legacy-readiness.mjs');
    const before = await ackCount();
    let r = await checkReadiness({ host: 'minipc', env });
    assert.equal(r.ok, true, r.lines.join('\n'));
    assert.ok(r.lines.some((l) => l.includes('黙っているプロセスは無い')), r.lines.join('\n'));
    r = await checkReadiness({ host: 'render', env: { ...env, COMPANY_DB_MASTER_GATE_RENDER_URL: gateUrl('minipc') } });
    assert.equal(r.ok, false); assert.ok(r.problems.some((x) => /master_gate_minipc/.test(x) && /期待 master_gate_render/.test(x)), r.lines.join('\n'));
    r = await checkReadiness({ host: 'minipc', env: { COMPANY_DB_WATCH_URL: process.env.COMPANY_DB_WATCH_URL } });
    assert.equal(r.ok, false); assert.ok(r.problems.some((x) => /COMPANY_DB_MASTER_GATE_MINIPC_URL/.test(x)));
    assert.ok(r.problems.some((x) => /段階を読む接続先が無い/.test(x)), 'watcher だけでは段階を読まない');
    assert.equal(await ackCount(), before, '確かめは書かない');
  });
  await ta('[12] 門のログインの接続は、同時に 20 件の読み + 記録を書いても 1 本 (記録も段階を読むプールの 1 本で書く。Codex #1565 R2 Medium 3)', async () => {
    await sleep(300);   // 前の試験の接続が消えるまで
    let peak = 0;
    let stop = false;
    const watch = (async () => { while (!stop) { peak = Math.max(peak, await connsOf('usename = $2', ['master_gate_minipc'])); await sleep(5); } })();
    try {
      G.__resetLegacyAck();
      const [rs, ack] = await Promise.all([Promise.all(Array.from({ length: 20 }, () => G.checkLegacyGate())), G.ackLegacyGates({ host: 'minipc', env })]);
      assert.ok(rs.every((s) => s.writable === true)); assert.equal(ack.state, 'acked', JSON.stringify(ack));
    } finally { stop = true; await watch; }
    assert.equal(peak, 1, `門のログインの接続 ${peak} 本`);
  });
  await ta('[13] env の取り違え (miniPC の env に Render のログイン) = ⑤-1 の関数が gate_host_mismatch (42501) で拒む・何も書かない・直し方を出す', async () => {
    const before = await ackCount();
    const r = await quiet(() => G.ackLegacyGates({ host: 'minipc', env: { ...env, COMPANY_DB_MASTER_GATE_MINIPC_URL: gateUrl('render') } }));
    assert.equal(r.state, 'error'); assert.match(r.detail, /gate_host_mismatch/); assert.match(r.detail, /COMPANY_DB_MASTER_GATE_MINIPC_URL は master_gate_minipc/);
    assert.equal(await ackCount(), before);
  });
  await ta('[14] ⑤-3 の記録で ⑤-1 の段階の関数が frozen に進める。黙っているプロセスがあれば拒む → master-legacy-instance.mjs で「止めた」を書けば進める', async () => {
    const { markStopped } = await import('./company-db/master-legacy-instance.mjs');
    const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
    const C = await import('../lib/master-cutover.mjs');
    // Render の記録も ⑤-3 の書き方で (Render のログイン)
    G.__resetLegacyAck();
    const rr = await G.ackLegacyGates({ host: 'render', env: { ...env, COMPANY_DB_MASTER_GATE_RENDER_URL: gateUrl('render') } });
    assert.equal(rr.state, 'acked', JSON.stringify(rr));
    G.__resetLegacyAck();
    assert.equal((await G.ackLegacyGates({ host: 'minipc', env })).state, 'acked');
    const manifest = G.legacyManifest();
    const mh = await C.manifestHashOf(db, manifest);
    const oh = C.ownershipHash(MASTER_OWNERSHIP);
    // 2 日前の記録だけのプロセス (落ちて「止めた」を書けなかった) = 黙っている (年齢では外れない = ⑤-1 #1563 R3)
    await M.query(`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, session_role, acked_at)
      values ('minipc', 'old-pc:1:aaaaaaaa', $1, $2, $3, 'legacy_open', 0, 'master_gate_minipc', clock_timestamp() - interval '2 days')`, ['b'.repeat(40), mh, oh]);
    // 証拠の時刻は今の段階に入った後・サーバーの今以前 (⑤-1 #1563 R3) = サーバーの今を使う
    const at = (await M.query('select clock_timestamp()::text as t')).rows[0].t;
    const evidence = {
      expected_builds: { render: ['b'.repeat(40)], minipc: ['b'.repeat(40)] }, manifest_hash: mh, owner_hash: oh,
      manual_entries_stopped: manifest.entries.filter((x) => x.kind === 'manual').map((x) => ({ id: x.id, by: 'test', at })),
      drain: { done: true, checked_by: 'test', checked_at: at },
    };
    await assert.rejects(() => C.advanceCutoverPhase(db, { to: 'frozen', actor: 'test', evidence }), /old-pc:1:aaaaaaaa: 黙っている/);
    // 配る前の確かめにも出る (見るだけ = 足りないにはしない) / --list にも出る (2 日前でも)
    const { checkReadiness } = await import('./company-db/master-legacy-readiness.mjs');
    const ready = await checkReadiness({ host: 'minipc', env });
    assert.equal(ready.ok, true); assert.ok(ready.lines.some((l) => l.includes('黙っているプロセス 1 件') && l.includes('old-pc:1:aaaaaaaa')), ready.lines.join('\n'));
    const { listInstances } = await import('./company-db/master-legacy-instance.mjs');
    assert.ok((await listInstances(M)).some((x) => x.instance_id === 'old-pc:1:aaaaaaaa' && !x.stopped && !x.fresh), '2 日前でも止めていなければ一覧に出る');
    const st = await markStopped({ host: 'minipc', instance: 'old-pc:1:aaaaaaaa', reason: '再起動で消えたのを確かめた (試験)', env });
    assert.equal(st.state, 'stopped', JSON.stringify(st));
    const r = await C.advanceCutoverPhase(db, { to: 'frozen', actor: 'test', evidence });
    assert.ok(r.acks.some((a) => a.instance_id === 'old-pc:1:aaaaaaaa' && a.stopped === true), JSON.stringify(r.acks));
    assert.ok(r.acks.some((a) => a.host === 'render' && !a.stopped) && r.acks.some((a) => a.host === 'minipc' && !a.stopped));
    assert.equal((await G.checkLegacyGate()).writable, false, '進めた直後から古い入口は閉じる');
  });
  await ta('[15] 読む時間を測る (master-legacy-latency.mjs・読むだけ): つなぎ直し + 読む / つないだまま読む の p50・p95・いちばん遅い', async () => {
    const { measureLatency } = await import('./company-db/master-legacy-latency.mjs');
    const before = await ackCount();
    const r = await measureLatency({ url: gateUrl('minipc'), n: 3, gapMs: 0 });
    assert.deepEqual(r.errors, []);
    assert.equal(r.role, 'master_gate_minipc'); assert.equal(r.phase, 'frozen');
    assert.equal(r.cold.total.n, 3); assert.equal(r.warm.n, 3);
    assert.ok(r.cold.total.p95 >= r.cold.connect.p50 && r.warm.max >= 0);
    assert.equal(await ackCount(), before, '書かない');
  });
  await ta('[16] 配り直し: 古いプロセスの記録が書いている途中に SIGTERM = 待ってから「止めた」を 1 回・新しいプロセスと合わせて接続 2 本まで・古いプロセスの最後の記録は stopped', async () => {
    await G.closeLegacyGatePool();
    await sleep(300);
    const G2 = await import('../lib/master-legacy-gate.mjs?new-process');   // 別のモジュール = 別のプロセスの代わり (プール・名札・記録の状態が別)
    let peak = 0;
    let stop = false;
    const watch = (async () => { while (!stop) { peak = Math.max(peak, await connsOf('usename = $2', ['master_gate_minipc'])); await sleep(5); } })();
    const L = await openPgClient(u.toString());
    L.on('error', () => {});
    try {
      G.__resetLegacyAck();
      // 記録の表をつかむ = 古いプロセスの普通の記録が書いている途中で止まる
      await L.query('begin');
      await L.query('lock table ops.master_legacy_gate_acks in exclusive mode');
      const normal = G.maybeRefreshLegacyAck({ host: 'minipc', env, force: true });
      await sleep(300);
      const stopping = G.ackLegacyGatesStopped({ host: 'minipc', reason: 'SIGTERM で止めた (試験)', env, timeoutMs: 15000 });
      // 新しいプロセスが起動して記録を書く・段階を読む
      const startNew = G2.ackLegacyGates({ host: 'minipc', env });
      const reads = await Promise.all(Array.from({ length: 10 }, () => G2.checkLegacyGate()));
      assert.ok(reads.every((s) => s.readable === true));
      await sleep(300);
      await L.query('rollback');   // 放す
      const [n, s, a2] = await Promise.all([normal, stopping, startNew]);
      assert.equal(n.state, 'acked', JSON.stringify(n)); assert.equal(s.state, 'stopped', JSON.stringify(s)); assert.equal(a2.state, 'acked', JSON.stringify(a2));
      const oldId = (await M.query('select instance_id from ops.master_legacy_gate_acks where ack_id = $1', [s.ack_id])).rows[0].instance_id;
      const last = (await M.query('select stopped, ack_id from ops.master_legacy_gate_acks where instance_id = $1 order by acked_at desc, ack_id desc limit 1', [oldId])).rows[0];
      assert.equal(last.stopped, true, '古いプロセスの最後の記録は「止めた」'); assert.equal(String(last.ack_id), s.ack_id);
      assert.ok(Number(n.ack_id) < Number(s.ack_id), '普通の記録 → 「止めた」の順');
      const newId = (await M.query('select instance_id from ops.master_legacy_gate_acks where ack_id = $1', [a2.ack_id])).rows[0].instance_id;
      assert.notEqual(newId, oldId);
      assert.equal(G.maybeRefreshLegacyAck({ host: 'minipc', env, force: true }), null, '古いプロセスはもう書き直さない');
    } finally {
      stop = true; await watch;
      try { await L.query('rollback'); } catch { /* */ }
      await L.end();
      await G2.closeLegacyGatePool();
      G.__resetLegacyAck();
    }
    assert.ok(peak >= 1 && peak <= 2, `門のログインの接続 ${peak} 本 (古い 1 + 新しい 1 まで)`);
  });
} finally {
  G.__setLegacyPhaseReader(null);
  await G.closeLegacyGatePool();
  await M.end();
  try { await admin.query(`select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()`, [dbName]); await admin.query(`drop database ${dbName}`); } catch (e) { console.error(`後始末: ${e.message}`); }
  await admin.end();
}
console.log(process.exitCode ? `\n❌ 失敗あり (${passed} 件 OK)` : `\n✅ ${passed} 件 OK`);
