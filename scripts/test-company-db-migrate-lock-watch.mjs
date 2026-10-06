#!/usr/bin/env node
/**
 * test-company-db-migrate-lock-watch.mjs — migrate の lock の 45 分の見張り (scripts/company-db/migrate-lock-watch.mjs) の試験
 *   (設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10 v3.9「見張り」・v3.12「45 分の見張り」)
 *
 * 固定する契約:
 *   U0 既定 = 45 分 (migrate.mjs の MIGRATE_LOCK_ALERT_MINUTES と同じ値)・60 秒ごと・引数の誤りは投げる
 *   U1 lock が一度も現れない → 待ちの時間で終わる (送らない・exit 0)
 *   U2 45 分より前に外れる → 送らない・exit 0
 *   U3 backend_start が見える → server の時計の「接続から」で数え、45 分を超えた最初の見回りで 1 回だけ送る・repeat ごとにもう一度・外れたら「外れた」・exit 0
 *   U4 backend_start が見えない・直前の見回りで lock が無かった → その見回りの時刻から数える (長めに数える = 見えた時刻から数えるより 1 回早く鳴る)
 *   U5 backend_start が見えない・最初の見回りでもう持たれている → 見張りを始めた時から数え、知らせに「実際はもっと長い」
 *   U6 送れなかった知らせは次の見回りで送り直す (送れた時だけ「知らせた」にする)
 *   U7 DB を 3 回続けて読めない → 「見張れていない」を 1 回だけ送る・読めなかった見回りは「lock が無かった」と数えない
 *   U8 max-hours を超えてもまだ持たれている → 「打ち切る」を送って exit 1
 *   U9 持ち主が替わった (A → B) → A は外れた扱い (知らせていれば「外れた」)・B を続けて見張る (B は直前の見回りから数える)
 *   R1 本物の PG 18.4: watcher のような権限の無い役割から、別の役割の runner (migrate.mjs の withMigrateLock) が持つ lock の pid と
 *      application_name は見え、backend_start は見えない (設計 v3.12 の前提)・同じ役割なら見える・migrate.mjs の describeLockHolder と同じ pid
 *   R2 別の DB で同じ鍵の lock を持っていても数えない (同じ DB だけ)
 *   R3 本物の PG で、runner の withMigrateLock が lock を持つ間に見張りが鳴り、外れたら「外れた」を送って終わる (送り先は試験の差し替え)
 *   R4 見張りの読み手は lock を取らない・application_name で見える・同じ設定 (default_transaction_read_only) の接続では書けない
 *   C1 CLI: 送り先 (GCHAT_WEBHOOK_JOBS) が無い = 接続せずに exit 2 / 引数の誤り = exit 2
 *   C2 CLI --dry-run: 本物の PG で鳴る文を画面に出し、外れたら exit 0
 * 使い方: node scripts/test-company-db-migrate-lock-watch.mjs   (npm run test:company-db にも入っている)
 *   使い捨てのクラスタを embedded-postgres で起動し、最後に止めて消す (test-company-db-migrate-lock-pg.mjs と同じ作り)
 */
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs, runLockWatch, readLockHolder, alertText, DEFAULTS, WATCH_APPLICATION_NAME } from './company-db/migrate-lock-watch.mjs';
import { openPgClient, pgAdapter, withMigrateLock, describeLockHolder, MIGRATE_LOCK_NAME, MIGRATE_LOCK_ALERT_MINUTES } from './company-db/migrate.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const WATCH_CLI = path.join(ROOT, 'scripts', 'company-db', 'migrate-lock-watch.mjs');
let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const MIN = 60000;

/**
 * 時計を差し替えた見張り。timeline(tMs) = その時刻 (起動からの ms) の読みの結果 (null・持ち主・Error)。
 * sendOk(i) = i 回目の送りが届くか。戻り = { result, sends: [{ at, text, ok }], logs }
 */
async function sim(timeline, opts = {}, sendOk = () => true) {
  let clock = 0;
  const sends = [], logs = [];
  const result = await runLockWatch({
    now: () => clock,
    sleep: async (ms) => { clock += ms; },
    readHolder: async () => { const r = timeline(clock); if (r instanceof Error) throw r; return r; },
    send: async (text) => { const okk = sendOk(sends.length); sends.push({ at: clock, text, ok: okk }); return okk; },
    log: (m) => logs.push(m),
    dbName: 'testdb',
    ...opts,
  });
  return { result, sends, logs, delivered: sends.filter((s) => s.ok) };
}
const H = (pid, extra = {}) => ({ pid, applicationName: 'company-db-migrate', usename: 'deployer', startVisible: false, heldMs: null, phase: null, relation: null, ...extra });

console.log('— 単体 (時計を差し替え) —');
await t('U0 既定 = 45 分・60 秒ごと (runner の定数と同じ)・引数の誤りは投げる', async () => {
  assert.equal(DEFAULTS.alertMin, 45);
  assert.equal(DEFAULTS.alertMin, MIGRATE_LOCK_ALERT_MINUTES);
  assert.equal(DEFAULTS.intervalSec, 60);
  const a = parseArgs([]);
  assert.equal(a.alertMin, 45); assert.equal(a.dryRun, false); assert.equal(a.url, null);
  assert.equal(parseArgs(['--alert-min', '0.05', '--interval-sec', '1', '--dry-run']).alertMin, 0.05);
  for (const bad of [['--alert-min', '0'], ['--alert-min'], ['--interval-sec', 'x'], ['--interval-sec', '0.5'], ['--max-hours', '-1'], ['--what'], ['--url', 'http://x'], ['--url']]) assert.throws(() => parseArgs(bad), undefined, bad.join(' '));
});
await t('U1 lock が一度も現れない → 待ちの時間 (30 分) で終わる・送らない・exit 0', async () => {
  const r = await sim(() => null);
  assert.equal(r.result.outcome, 'never_seen'); assert.equal(r.result.code, 0);
  assert.equal(r.sends.length, 0);
  assert.ok(r.logs.some((m) => /一度も現れなかった/.test(m)));
});
await t('U2 45 分より前に外れる → 送らない・exit 0', async () => {
  const r = await sim((c) => (c >= 2 * MIN && c < 40 * MIN ? H(101) : null));
  assert.equal(r.result.outcome, 'released'); assert.equal(r.result.code, 0);
  assert.equal(r.sends.length, 0);
});
await t('U3 backend_start が見える → 接続から数え、45 分を超えた最初の見回りで 1 回・repeat (60 分) ごとにもう一度・外れたら「外れた」', async () => {
  // 見張りの 5 分前に接続した runner (heldMs = server の時計)。起動の時刻 c = 0 で接続から 5 分
  const r = await sim((c) => (c < 130 * MIN ? H(202, { startVisible: true, heldMs: c + 5 * MIN, phase: 'building index: scanning table', relation: 'core.listings' }) : null));
  assert.equal(r.result.outcome, 'released'); assert.equal(r.result.code, 0);
  const at = r.sends.map((s) => s.at / MIN);
  assert.deepEqual(at, [40, 100, 130], `送った時刻 (起動からの分) = ${at}`);   // 接続から 45 分 = 起動から 40 分 / repeat 60 分 / 外れた
  assert.match(r.sends[0].text, /migrate の lock \(company_db_migrate\) を 45 分持っている \(45 分を超えた\)/);
  assert.match(r.sends[0].text, /pid 202 \(company-db-migrate・deployer\)/);
  assert.match(r.sends[0].text, /DB testdb/);
  assert.match(r.sends[0].text, /接続から \(backend_start\)/);
  assert.match(r.sends[0].text, /core\.listings の building index: scanning table/);
  assert.match(r.sends[0].text, /pg_cancel_backend/);
  assert.match(r.sends[1].text, /まだ外れていない/);
  assert.match(r.sends[2].text, /^✅ .*lock \(company_db_migrate\) が外れた: pid 202/);
});
await t('U4 backend_start が見えない・直前の見回りで lock が無かった → その見回りから数える (見えた時刻から数えるより 1 回早い)', async () => {
  // 起動 0 分・lock を取ったのは 10.5 分 (見回りは 10 分で無し・11 分で有り)。直前の見回り (10 分) から数える = 55 分の見回りで鳴る
  const r = await sim((c) => (c >= 10.5 * MIN && c < 70 * MIN ? H(303) : null));
  const alerts = r.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.equal(alerts.length, 1);
  assert.equal(alerts[0].at / MIN, 55, `鳴った時刻 = ${alerts[0].at / MIN} 分 (見えた 11 分から数えると 56 分)`);
  assert.match(alerts[0].text, /前の見回りで lock が無かった時から/);
  assert.ok(r.sends.at(-1).text.startsWith('✅'));
  assert.equal(r.result.outcome, 'released');
});
await t('U5 backend_start が見えない・最初の見回りでもう持たれている → 始めた時から数え「実際はもっと長い」', async () => {
  const r = await sim((c) => (c < 50 * MIN ? H(404) : null));
  const alerts = r.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.equal(alerts.length, 1); assert.equal(alerts[0].at / MIN, 45);
  assert.match(alerts[0].text, /実際はもっと長い/);
  assert.ok(r.logs.some((m) => /migrate の前に見張りを起動する/.test(m)));
});
await t('U6 送れなかった知らせは次の見回りで送り直す (届いた時だけ知らせた扱い)', async () => {
  const r = await sim((c) => (c < 60 * MIN ? H(505, { startVisible: true, heldMs: c }) : null), {}, (i) => i >= 2);   // 1・2 回目は届かない
  const alerts = r.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.deepEqual(alerts.map((s) => [s.at / MIN, s.ok]), [[45, false], [46, false], [47, true]]);
  assert.ok(r.logs.some((m) => /送れなかった/.test(m)));
  assert.ok(r.sends.at(-1).text.startsWith('✅'));   // 知らせが届いた後なので「外れた」も送る
});
await t('U7 DB を 3 回続けて読めない → 「見張れていない」を 1 回だけ・読めなかった見回りは「lock が無かった」と数えない', async () => {
  // 0〜9 分 = 無し / 10〜19 分 = 読めない (10 回) / 20 分〜 = 持たれている (見えない)。lock が無かった最後の見回りは 9 分 = 9 分から数える = 54 分で鳴る
  const r = await sim((c) => (c < 10 * MIN ? null : c < 20 * MIN ? new Error('connection refused') : c < 70 * MIN ? H(606) : null));
  const fails = r.sends.filter((s) => /読めない/.test(s.text));
  assert.equal(fails.length, 1); assert.equal(fails[0].at / MIN, 12);
  assert.match(fails[0].text, /45 分の見張りが効いていない/);
  const alerts = r.sends.filter((s) => s.text.startsWith('⚠️'));
  assert.equal(alerts.length, 1); assert.equal(alerts[0].at / MIN, 54);
  assert.ok(r.logs.some((m) => /また読めるようになった/.test(m)));
});
await t('U8 max-hours を超えてもまだ持たれている → 「打ち切る」を送って exit 1', async () => {
  const r = await sim(() => H(707), { maxHours: 2 });
  assert.equal(r.result.outcome, 'max_hours'); assert.equal(r.result.code, 1);
  assert.deepEqual(r.sends.map((s) => s.at / MIN), [45, 105, 120]);
  assert.match(r.sends.at(-1).text, /見張りを打ち切る \(2 時間\): lock はまだ pid 707/);
});
await t('U9 持ち主が替わった (A → B) → A は「外れた」・B を続けて見張り、B は直前の見回りから数える', async () => {
  const r = await sim((c) => (c < 50 * MIN ? H(801) : c < 120 * MIN ? H(802) : null));
  const at = r.sends.map((s) => [s.at / MIN, s.text.slice(0, 1), /pid (\d+)/.exec(s.text)[1]]);
  // A (801) = 45 分で ⚠️・50 分で ✅ / B (802) = 49 分 (A を見た最後の見回り) から数える = 94 分で ⚠️・120 分で ✅
  assert.deepEqual(at, [[45, '⚠', '801'], [50, '✅', '801'], [94, '⚠', '802'], [120, '✅', '802']]);
  assert.equal(r.result.outcome, 'released');
});
await t('U10 知らせの文 = 秘密の値 (接続文字列) を含まない形だけ', async () => {
  const txt = alertText({ h: H(9), elapsedMs: 46 * MIN, countFrom: 'prev_poll', dbName: 'cdb', alertMin: 45, repeat: false });
  assert.doesNotMatch(txt, /postgres:\/\/|password/i);
  assert.match(txt, /46 分持っている/);
});

// ─── 本物の PG (embedded-postgres) ───
const PINNED_EMBEDDED_PG = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).devDependencies['embedded-postgres'];
async function loadEmbeddedPostgres() {
  const bases = [path.join(ROOT, 'package.json'), ...(process.env.EMBEDDED_PG_DIR ? [path.join(process.env.EMBEDDED_PG_DIR, 'package.json')] : []), 'C:/tmp/pg-embed/package.json'];
  const seen = [];
  for (const b of bases) {
    let main, ver;
    try { const req = createRequire(b); main = req.resolve('embedded-postgres'); let d = path.dirname(main); while (path.basename(d) !== 'embedded-postgres' && path.dirname(d) !== d) d = path.dirname(d); ver = JSON.parse(fs.readFileSync(path.join(d, 'package.json'), 'utf8')).version; } catch { continue; }
    if (ver !== PINNED_EMBEDDED_PG) { seen.push(path.dirname(b) + ' = ' + ver); continue; }
    return { EmbeddedPostgres: (await import(pathToFileURL(main).href)).default, from: path.dirname(b) };
  }
  return { why: seen.length ? '版が ' + PINNED_EMBEDDED_PG + ' でない (' + seen.join(' / ') + ')' : '見つからない' };
}
const loaded = await loadEmbeddedPostgres();
if (!loaded.EmbeddedPostgres) {
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の見張りの試験を流せない (飛ばさない)。リポジトリで npm ci');
  process.exit(1);
}
const clusterDir = path.join(os.tmpdir(), `cdb-migrate-lock-watch-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
const cluster = new loaded.EmbeddedPostgres({ databaseDir: clusterDir, user: 'postgres', password: SU_PW, port, persistent: false, onLog: () => {}, onError: () => {} });
const suUrl = (db = 'postgres') => `postgres://postgres:${SU_PW}@127.0.0.1:${port}/${db}`;
const hex = crypto.randomBytes(4).toString('hex');
const WATCHER = `w45_watch_${hex}`, RUNNER = `w45_run_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const roleUrl = (role, db = 'postgres') => `postgres://${role}:${PW}@127.0.0.1:${port}/${db}`;
let cleanupFailed = false;
console.log('— 本物の PG (embedded-postgres ' + PINNED_EMBEDDED_PG + ' / ' + loaded.from + ') —');
await cluster.initialise();
await cluster.start();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const conns = [];
const open = async (url, extra) => { const c = await openPgClient(url, extra); c.on('error', () => {}); conns.push(c); return c; };
try {
  const su = await open(suUrl());
  // watcher = 照会用の役割の代わり (LOGIN だけ・pg_read_all_stats なし) / runner = migrate を流す役割の代わり (deployer)
  await su.query(`create role ${WATCHER} login password '${PW}'`);
  await su.query(`create role ${RUNNER} login password '${PW}'`);
  await su.query('create database w45_other');

  await t('R1 権限の無い役割から: 別の役割の runner (withMigrateLock) が持つ lock の pid・application_name は見え、backend_start は見えない / 同じ役割なら見える / describeLockHolder と同じ pid', async () => {
    const run = await open(roleUrl(RUNNER));   // application_name = company-db-migrate (runner と同じ接続の作り)
    const runPid = (await run.query('select pg_backend_pid() as p')).rows[0].p;
    const w = await open(roleUrl(WATCHER));
    const same = await open(roleUrl(RUNNER));
    assert.equal(await readLockHolder(w), null);
    let release; const held = new Promise((r) => { release = r; });
    let inside; const entered = new Promise((r) => { inside = r; });
    const p = withMigrateLock(pgAdapter(run), async () => { inside(); await held; return 'done'; });
    await entered;
    try {
      const hw = await readLockHolder(w);
      assert.equal(hw.pid, runPid);
      assert.equal(hw.applicationName, 'company-db-migrate');
      assert.equal(hw.startVisible, false, '見張りの役割から backend_start が見えている (設計 v3.12 の前提と違う)');
      assert.equal(hw.heldMs, null);
      const hs = await readLockHolder(same);
      assert.equal(hs.pid, runPid); assert.equal(hs.startVisible, true); assert.ok(hs.heldMs >= 0 && hs.heldMs < 60000, `heldMs = ${hs.heldMs}`);
      const d = await describeLockHolder(pgAdapter(su));
      assert.deepEqual(d.map((x) => x.pid), [runPid]);
    } finally { release(); }
    assert.equal(await p, 'done');
    assert.equal(await readLockHolder(w), null);
  });

  await t('R2 別の DB で同じ鍵の lock を持っていても数えない', async () => {
    const other = await open(suUrl('w45_other'));
    await other.query('select pg_advisory_lock(hashtextextended($1, 0))', [MIGRATE_LOCK_NAME]);
    const w = await open(roleUrl(WATCHER));
    assert.equal(await readLockHolder(w), null);
    const wo = await open(roleUrl(WATCHER, 'w45_other'));
    assert.ok(await readLockHolder(wo));
    await other.query('select pg_advisory_unlock(hashtextextended($1, 0))', [MIGRATE_LOCK_NAME]);
  });

  await t('R3 本物の PG: runner の withMigrateLock が持つ間に鳴り (見張りの役割・見えない = 前の見回りから数える)、外れたら「外れた」を送って終わる', async () => {
    const run = await open(roleUrl(RUNNER));
    const w = await open(roleUrl(WATCHER));
    const sends = [];
    const watch = runLockWatch({ readHolder: () => readLockHolder(w), send: async (x) => { sends.push(x); return true; }, dbName: 'postgres', intervalSec: 0.2, alertMin: 1.5 / 60, repeatMin: 60, waitStartMin: 1, maxHours: 1 });
    await sleep(600);   // 見張りが「lock が無い」を何回か見てから取る
    await withMigrateLock(pgAdapter(run), () => sleep(3000));
    const r = await watch;
    assert.equal(r.outcome, 'released'); assert.equal(r.code, 0);
    assert.equal(sends.length, 2, sends.join('\n---\n'));
    assert.match(sends[0], /^⚠️ Company DB の migrate の lock \(company_db_migrate\)/);
    assert.match(sends[0], /前の見回りで lock が無かった時から/);
    assert.match(sends[0], /company-db-migrate/);
    assert.match(sends[1], /^✅/);
  });

  await t('R4 見張りの読み手は lock を取らない・application_name で見える・同じ設定 (default_transaction_read_only) の接続では書けない', async () => {
    const { pgHolderReader } = await import('./company-db/migrate-lock-watch.mjs');
    const reader = pgHolderReader(roleUrl(WATCHER));
    assert.equal(await reader.dbName(), 'postgres');
    assert.equal(await reader.read(), null);
    const a = await describeLockHolder(pgAdapter(su));
    assert.equal(a.length, 0);
    const seen = (await su.query('select pid from pg_stat_activity where application_name = $1', [WATCH_APPLICATION_NAME])).rows;
    assert.equal(seen.length, 1, '見張りの接続が application_name で見える');
    const locks = (await su.query(`select count(*)::int as n from pg_locks l join pg_stat_activity a on a.pid = l.pid where a.application_name = $1 and l.locktype = 'advisory'`, [WATCH_APPLICATION_NAME])).rows[0].n;
    assert.equal(locks, 0);
    await reader.close();
    // 同じ設定の接続で書くと落ちる (default_transaction_read_only = on)
    const w = await open(roleUrl(WATCHER));
    await w.query('set default_transaction_read_only = on');
    await assert.rejects(w.query('create temp table w45_x (a int)'), /read-only/);
  });

  await t('C1 CLI: 送り先 (GCHAT_WEBHOOK_JOBS) が無い = 接続せずに exit 2 / 引数の誤り = exit 2', async () => {
    const env = { ...process.env }; delete env.GCHAT_WEBHOOK_JOBS; env.COMPANY_DB_WATCH_URL = 'postgres://nobody:x@127.0.0.1:1/none';
    const r = spawnSync(process.execPath, [WATCH_CLI], { cwd: os.tmpdir(), env, encoding: 'utf8', timeout: 60000 });
    assert.equal(r.status, 2, r.stdout + r.stderr); assert.match(r.stderr, /GCHAT_WEBHOOK_JOBS/);
    const r2 = spawnSync(process.execPath, [WATCH_CLI, '--alert-min', 'abc'], { cwd: os.tmpdir(), env, encoding: 'utf8', timeout: 60000 });
    assert.equal(r2.status, 2, r2.stdout + r2.stderr);
    const env3 = { ...env }; delete env3.COMPANY_DB_WATCH_URL;
    const r3 = spawnSync(process.execPath, [WATCH_CLI, '--dry-run'], { cwd: os.tmpdir(), env: env3, encoding: 'utf8', timeout: 60000 });
    assert.equal(r3.status, 2, r3.stdout + r3.stderr); assert.match(r3.stderr, /COMPANY_DB_WATCH_URL/);
  });

  await t('C2 CLI --dry-run: 本物の PG で鳴る文を画面に出し、外れたら exit 0', async () => {
    const env = { ...process.env }; delete env.GCHAT_WEBHOOK_JOBS; delete env.COMPANY_DB_WATCH_URL;
    const child = spawn(process.execPath, [WATCH_CLI, '--url', roleUrl(WATCHER), '--dry-run', '--interval-sec', '1', '--alert-min', String(2 / 60), '--wait-start-min', '1'], { cwd: os.tmpdir(), env });
    let out = '';
    child.stdout.on('data', (d) => { out += d; }); child.stderr.on('data', (d) => { out += d; });
    const exited = new Promise((r) => child.on('exit', (code) => r(code)));
    await sleep(2500);
    const run = await open(roleUrl(RUNNER));
    await withMigrateLock(pgAdapter(run), () => sleep(5000));
    const code = await Promise.race([exited, sleep(30000).then(() => 'timeout')]);
    if (code === 'timeout') child.kill();
    assert.equal(code, 0, out);
    assert.match(out, /見張りを始める: DB postgres/);
    assert.match(out, /\(dry-run・送らない\)\n⚠️ Company DB の migrate の lock/);
    assert.match(out, /\(dry-run・送らない\)\n✅/);
    assert.match(out, /終わり: released/);
    assert.doesNotMatch(out, new RegExp(PW));   // 接続文字列 (password) を出さない
  });
} finally {
  for (const c of conns) { try { await c.end(); } catch { /* */ } }
  try { await cluster.stop(); } catch (e) { console.error('使い捨てのクラスタを止めるときの誤り: ' + e.message); }
  for (let i = 0; i < 10 && fs.existsSync(clusterDir); i++) { try { fs.rmSync(clusterDir, { recursive: true, force: true }); } catch { await sleep(500); } }
  if (fs.existsSync(clusterDir)) { cleanupFailed = true; console.error('❌ 使い捨てのクラスタのフォルダが消えない: ' + clusterDir); }
}
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng || cleanupFailed ? 1 : 0;
setTimeout(() => process.exit(process.exitCode), 10000).unref();
