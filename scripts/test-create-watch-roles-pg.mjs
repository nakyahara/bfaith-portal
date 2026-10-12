/**
 * test-create-watch-roles-pg.mjs — scripts/company-db/create-watch-roles.mjs の CLI を、実 PostgreSQL と本物のログイン (パスワード) で確かめる
 *   (PGlite はパスワードでのログインを試せない。2026-10-12 の事故 = migration 0070 の後に素で流してパスワードが変わり、
 *    miniPC の .env が古いまま 11:30 から照合・見張りが password authentication failed で止まった の再発防止)
 *
 * 固定する契約:
 *   1 初回 (ロールが無い) = 2 つ作って新しいパスワードの 2 行を出す → その 2 行で入れる (違うパスワードは 28P01 で拒まれる = 試験の DB がパスワードを見ている)
 *   2 既定で 2 回目 = パスワードを変えない (接続の行を出さない)・1 回目のパスワードのまま入れる。最後に .env の 2 本を確かめて exit 0
 *   3 --dry-run = パスワードを出さない (今のパスワードも、新しいパスワードも)・何も変えない
 *   4 --rotate-passwords = 2 つとも変わる (前のパスワードは 28P01・新しい 2 行で入れる)・大きな警告を出す
 *   5 既定の実行で .env が古い (rotate の前のパスワード) = 権限はそろえるがパスワードは変えず exit 1 (--rotate-passwords が要る)
 *   6 もうあるロールが nologin = 既定では直さず exit 1 (--rotate-passwords が要る)・nologin のまま
 *   7 実行者は Render と同じ形 (superuser でない・CREATEROLE だけ・DB とテーブルの owner)
 *   8 (#1696 Codex R1 M1) 片方だけ作り直した (無かった) ときも、もうあった方の .env の URL は必ず確かめる (古い = exit 1・未設定 = exit 1)
 *   9 (#1696 Codex R1 M2) .env の URL が別の DB (同じ作りの古い DB) を指している = 入れても exit 1 (既定・--verify とも)。パスワードは出さない
 *  10 (#1696 Codex R1 L1) 同時に 2 本: 後の方は advisory lock で待ち「もうある」と見てパスワードを変えない /
 *     確かめの後に鍵の外で作られた = 素の create role が 42710 で取り消す (そのロールのパスワードは変えない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-create-watch-roles-pg.mjs
 *   (cd C:/tmp/pg-embed && node run-watchroles.mjs scripts/test-create-watch-roles-pg.mjs C:/tmp/watchroles-work)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロール watcher / watch_writer / 実行者も作って消す)。localhost 以外の URL は拒む。package.json の test:company-db には入れない
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { urlFor, createRoles, ROLES_LOCK_NAME } from './company-db/create-watch-roles.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL のロールの試験は飛ばす。PGlite の試験は scripts/test-company-db-watch.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const SCRIPT = path.join(root, 'scripts', 'company-db', 'create-watch-roles.mjs');
let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const admin = await openPgClient(url);
const existingRoles = (await admin.query(`select count(*)::int as n from pg_roles where rolname in ('watcher', 'watch_writer')`)).rows[0].n;
if (existingRoles) { console.error('この PostgreSQL にはもう watcher / watch_writer がある = 使い捨ての PG ではない。流さない'); await admin.end(); process.exit(2); }
const tag = crypto.randomBytes(4).toString('hex');
const dbName = `cdb_wr_${tag}`, deploy = `deploy_${tag}`, deployPw = `d-${tag}`;
await admin.query(`create role ${deploy} with login createrole nocreatedb nosuperuser password '${deployPw}'`);
await admin.query(`create database ${dbName} owner ${deploy}`);
const dbUrl = (() => { const u = new URL(url); u.pathname = `/${dbName}`; return u.toString(); })();
const deployUrl = urlFor(dbUrl, deploy, deployPw);
const dbName2 = `cdb_wr2_${tag}`;
const db2Url = (() => { const u = new URL(url); u.pathname = `/${dbName2}`; return u.toString(); })();
let writerNow;
/** ロールを消す (試験の DB それぞれで持ち物・権限を外してから)。superuser の接続で */
async function dropWatchRoles(roles) {
  for (const d of [dbName, dbName2]) {
    if (!(await admin.query('select 1 from pg_database where datname = $1', [d])).rows.length) continue;
    const c = await openPgClient((() => { const u = new URL(url); u.pathname = `/${d}`; return u.toString(); })());
    try { for (const r of roles) if ((await c.query('select 1 from pg_roles where rolname = $1', [r])).rows.length) await c.query(`drop owned by ${r}`); } finally { await c.end(); }
  }
  for (const r of roles) await admin.query(`drop role if exists ${r}`);
}


/** CLI を子のプロセスで流す (dotenv は使わない = env は渡したものだけ)。60 秒で打ち切る */
function cli(args, env = {}) {
  return new Promise((res) => {
    const base = { ...process.env };
    for (const k of ['COMPANY_DB_URL', 'COMPANY_DB_WATCH_URL', 'COMPANY_DB_WATCH_WRITER_URL']) delete base[k];
    const p = spawn(process.execPath, [SCRIPT, ...args], { cwd: root, env: { ...base, COMPANY_DB_URL: deployUrl, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    p.stdout.on('data', (c) => { out += c; });
    p.stderr.on('data', (c) => { err += c; });
    const t = setTimeout(() => p.kill(), 60000);
    p.on('close', (code) => { clearTimeout(t); res({ code, out, err, all: out + err }); });
  });
}
/** 出力から .env の行 (COMPANY_DB_WATCH_URL= / _WRITER_URL=) を読む */
const envLines = (out) => {
  const m = {};
  for (const l of out.split(/\r?\n/)) { const x = /^(COMPANY_DB_WATCH_URL|COMPANY_DB_WATCH_WRITER_URL)=(\S+)/.exec(l); if (x) m[x[1]] = x[2]; }
  return m;
};
/** その URL で入れるか: 'ok' / SQLSTATE (28P01 = パスワードが違う / 28000 = login できない) */
async function login(u) {
  let c;
  try { c = await openPgClient(u); } catch (e) { return e.code || e.message; }
  try { return (await c.query('select current_user as u')).rows[0].u === new URL(u).username ? 'ok' : 'other-user'; } finally { await c.end(); }
}
const pwOf = (u) => decodeURIComponent(new URL(u).password);

try {
  const D = await openPgClient(deployUrl);
  try { await applyMigrations(pgAdapter(D), { log: () => {} }); } finally { await D.end(); }
  let first;
  await ta('[1] 初回 (ロールが無い) = 2 つ作って新しいパスワードの 2 行を出す → その 2 行で入れる・違うパスワードは 28P01 (試験の DB がパスワードを見ている)', async () => {
    const r = await cli([]);
    assert.equal(r.code, 0, r.all);
    first = envLines(r.out);
    assert.deepEqual(Object.keys(first).sort(), ['COMPANY_DB_WATCH_URL', 'COMPANY_DB_WATCH_WRITER_URL'], r.all);
    assert.ok(/ロールを作った \(watcher \/ watch_writer\)/.test(r.out), r.out);
    assert.equal(await login(first.COMPANY_DB_WATCH_URL), 'ok');
    assert.equal(await login(first.COMPANY_DB_WATCH_WRITER_URL), 'ok');
    assert.equal(await login(urlFor(dbUrl, 'watcher', 'wrong-password')), '28P01');
    const v = await cli(['--verify'], first);
    assert.equal(v.code, 0, v.all);
    assert.equal((v.out.match(/^✅ /gm) || []).length, 2, v.out);
  });
  await ta('[2] 🚨 既定で 2 回目 (ロールがある) = パスワードを変えない・接続の行を出さない・1 回目のパスワードのまま入れる。最後に .env の 2 本を確かめて exit 0', async () => {
    const r = await cli([], first);
    assert.equal(r.code, 0, r.all);
    assert.deepEqual(envLines(r.out), {}, r.out);
    assert.ok(/パスワードは変えていない/.test(r.out), r.out);
    assert.ok(!r.all.includes(pwOf(first.COMPANY_DB_WATCH_URL)) && !r.all.includes(pwOf(first.COMPANY_DB_WATCH_WRITER_URL)), 'パスワードを画面に出さない');
    assert.equal((r.out.match(/^✅ COMPANY_DB_WATCH(_WRITER)?_URL: /gm) || []).length, 2, `.env の 2 本を確かめた: ${r.out}`);
    // 3 回目 (.env が無い) = もうあるロールの URL は要る = exit 1 (#1696 Codex R1 M1)。パスワードは変えない
    const r3 = await cli([]);
    assert.equal(r3.code, 1, r3.all);
    assert.ok(/❌ COMPANY_DB_WATCH_URL: 未設定/.test(r3.out) && /❌ COMPANY_DB_WATCH_WRITER_URL: 未設定/.test(r3.out), r3.out);
    assert.deepEqual(envLines(r3.out), {}, r3.out);
    assert.equal(await login(first.COMPANY_DB_WATCH_URL), 'ok', '2 回流しても同じパスワードで入れる (watcher)');
    assert.equal(await login(first.COMPANY_DB_WATCH_WRITER_URL), 'ok', '2 回流しても同じパスワードで入れる (watch_writer)');
  });
  await ta('[3] --dry-run = パスワードを出さない (今のも新しいのも)・何も変えない', async () => {
    for (const args of [['--dry-run'], ['--dry-run', '--rotate-passwords']]) {
      const r = await cli(args, first);
      assert.equal(r.code, 0, r.all);
      assert.deepEqual(envLines(r.out), {}, r.out);
      assert.ok(!r.all.includes(pwOf(first.COMPANY_DB_WATCH_URL)) && !r.all.includes(pwOf(first.COMPANY_DB_WATCH_WRITER_URL)), args.join(' '));
      for (const l of r.out.split(/\r?\n/).filter((x) => /password '/.test(x))) assert.ok(/password '\*\*\*'/.test(l), l);
      const pwLines = r.out.split(/\r?\n/).filter((x) => /password/.test(x));
      if (args.length === 1) {
        assert.equal(pwLines.filter((x) => /^alter role/.test(x)).length, 0, `既定の dry-run の文に password は無い: ${pwLines.join(' | ')}`);
        assert.ok(/パスワードを付けるロール: なし/.test(r.out), r.out);
      } else {
        assert.equal(pwLines.filter((x) => /^alter role \w+ with login password '\*\*\*'/.test(x)).length, 2, pwLines.join(' | '));
        assert.ok(/パスワードを付けるロール: watcher \(作り直す\) \/ watch_writer \(作り直す\)/.test(r.out), r.out);
        assert.ok(!/🚨🚨🚨/.test(r.out), 'dry-run では作り直さない = 警告は出さない');
      }
    }
    assert.equal(await login(first.COMPANY_DB_WATCH_URL), 'ok');
    assert.equal(await login(first.COMPANY_DB_WATCH_WRITER_URL), 'ok');
  });
  let rotated;
  await ta('[4] --rotate-passwords = 2 つとも変わる (前のパスワードは 28P01・新しい 2 行で入れる)・大きな警告を出す', async () => {
    const r = await cli(['--rotate-passwords'], first);
    assert.equal(r.code, 0, r.all);
    rotated = envLines(r.out);
    assert.deepEqual(Object.keys(rotated).sort(), ['COMPANY_DB_WATCH_URL', 'COMPANY_DB_WATCH_WRITER_URL'], r.out);
    assert.ok(pwOf(rotated.COMPANY_DB_WATCH_URL) !== pwOf(first.COMPANY_DB_WATCH_URL) && pwOf(rotated.COMPANY_DB_WATCH_WRITER_URL) !== pwOf(first.COMPANY_DB_WATCH_WRITER_URL));
    assert.ok((r.out.match(/🚨🚨🚨 --rotate-passwords/g) || []).length >= 1 && /\.env の COMPANY_DB_WATCH_URL \/ COMPANY_DB_WATCH_WRITER_URL を下の 2 行に書き換えるまで/.test(r.out) && /password authentication failed で止まる/.test(r.out), r.out);
    assert.ok(/パスワードを作り直した/.test(r.out), r.out);
    assert.equal(await login(first.COMPANY_DB_WATCH_URL), '28P01');
    assert.equal(await login(first.COMPANY_DB_WATCH_WRITER_URL), '28P01');
    assert.equal(await login(rotated.COMPANY_DB_WATCH_URL), 'ok');
    assert.equal(await login(rotated.COMPANY_DB_WATCH_WRITER_URL), 'ok');
  });
  await ta('[5] 🚨 既定の実行で .env が古い (rotate の前のパスワード) = パスワードは変えずに exit 1 (--rotate-passwords が要る)。新しいパスワードのまま', async () => {
    const r = await cli([], first);
    assert.equal(r.code, 1, r.all);
    assert.ok(/28P01/.test(r.out) && /--rotate-passwords が要る/.test(r.err), r.all);
    assert.deepEqual(envLines(r.out), {}, r.out);
    assert.equal(await login(rotated.COMPANY_DB_WATCH_URL), 'ok');
    assert.equal(await login(rotated.COMPANY_DB_WATCH_WRITER_URL), 'ok');
    const v = await cli(['--verify'], first);
    assert.equal(v.code, 1, v.all);
    assert.ok(/--rotate-passwords で作り直して/.test(v.err), v.all);
  });
  let again;
  await ta('[6] もうあるロールが nologin = 既定では直さず exit 1 (--rotate-passwords が要る)・nologin のまま / --rotate-passwords で直る', async () => {
    await admin.query('alter role watcher with nologin');
    const r = await cli([], rotated);
    assert.equal(r.code, 1, r.all);
    assert.ok(/watcher が login できない \(nologin\)/.test(r.err) && /--rotate-passwords が要る/.test(r.err), r.all);
    assert.equal((await admin.query(`select rolcanlogin from pg_roles where rolname = 'watcher'`)).rows[0].rolcanlogin, false);
    assert.equal(await login(rotated.COMPANY_DB_WATCH_URL), '28000');
    const r2 = await cli(['--rotate-passwords']);
    assert.equal(r2.code, 0, r2.all);
    again = envLines(r2.out);
    assert.equal(await login(again.COMPANY_DB_WATCH_URL), 'ok');
    assert.equal(await login(again.COMPANY_DB_WATCH_WRITER_URL), 'ok');
  });
  await ta('[7] 🚨 片方 (watch_writer) だけ無くて作った回も、もうあった watcher の .env の URL を確かめる: 古い = exit 1・未設定 = exit 1・正しい = exit 0。watcher のパスワードは変えない', async () => {
    await dropWatchRoles(['watch_writer']);
    const r = await cli([], { COMPANY_DB_WATCH_URL: first.COMPANY_DB_WATCH_URL });   // 古い (rotate の前)
    assert.equal(r.code, 1, r.all);
    const made = envLines(r.out);
    assert.deepEqual(Object.keys(made), ['COMPANY_DB_WATCH_WRITER_URL'], r.out);
    assert.ok(/❌ COMPANY_DB_WATCH_URL: 接続できない \(28P01/.test(r.out) && /--rotate-passwords が要る/.test(r.err), r.all);
    assert.ok(!/COMPANY_DB_WATCH_WRITER_URL: 未設定/.test(r.out), '新しく作った側は確かめない (.env に足してから --verify)');
    assert.equal(await login(again.COMPANY_DB_WATCH_URL), 'ok', 'watcher のパスワードは変えていない');
    assert.equal(await login(made.COMPANY_DB_WATCH_WRITER_URL), 'ok');
    // 同じ形でもう一度 (watch_writer を消して) .env の watcher が未設定 = exit 1
    await dropWatchRoles(['watch_writer']);
    const r2 = await cli([]);
    assert.equal(r2.code, 1, r2.all);
    assert.ok(/❌ COMPANY_DB_WATCH_URL: 未設定/.test(r2.out), r2.out);
    writerNow = envLines(r2.out).COMPANY_DB_WATCH_WRITER_URL;
    assert.equal(await login(writerNow), 'ok');
    // watcher の正しい URL = exit 0 (watch_writer はもうある = 今度は両方確かめる)
    const r3 = await cli([], { COMPANY_DB_WATCH_URL: again.COMPANY_DB_WATCH_URL, COMPANY_DB_WATCH_WRITER_URL: writerNow });
    assert.equal(r3.code, 0, r3.all);
    assert.equal((r3.out.match(/^✅ COMPANY_DB_WATCH(_WRITER)?_URL: /gm) || []).length, 2, r3.out);
  });
  await ta('[8] 🚨 .env の URL が別の DB (同じ作りの古い DB) を指している = 入れても exit 1 (既定・--verify)。接続先 (host:port/db) は出すがパスワードは出さない', async () => {
    // 同じ作り (migration・ロールの権限) の 2 つ目の DB = 復元・移設の前の古い DB の代わり
    await admin.query(`create database ${dbName2} owner ${deploy}`);
    const D2 = await openPgClient(urlFor(db2Url, deploy, deployPw));
    try { await applyMigrations(pgAdapter(D2), { log: () => {} }); } finally { await D2.end(); }
    const env2 = { COMPANY_DB_WATCH_URL: urlFor(db2Url, 'watcher', pwOf(again.COMPANY_DB_WATCH_URL)), COMPANY_DB_WATCH_WRITER_URL: urlFor(db2Url, 'watch_writer', pwOf(writerNow)) };
    const ok2 = await cli([], { ...env2, COMPANY_DB_URL: urlFor(db2Url, deploy, deployPw) });   // 2 つ目の DB の権限をそろえる (そこを指していれば通る)
    assert.equal(ok2.code, 0, ok2.all);
    assert.equal(await login(env2.COMPANY_DB_WATCH_URL), 'ok', '2 つ目の DB にも入れる = 接続先を照らさないと通ってしまう形');
    for (const args of [[], ['--verify']]) {
      const r = await cli(args, env2);   // COMPANY_DB_URL = 1 つ目の DB
      assert.equal(r.code, 1, `${args.join(' ')}: ${r.all}`);
      assert.ok(/❌ COMPANY_DB_WATCH_URL: 接続先が COMPANY_DB_URL と違う/.test(r.out) && /❌ COMPANY_DB_WATCH_WRITER_URL: 接続先が COMPANY_DB_URL と違う/.test(r.out), r.out);
      assert.ok(r.out.includes(`/${dbName2}`) && r.out.includes(`/${dbName}`), r.out);
      for (const p of [pwOf(again.COMPANY_DB_WATCH_URL), pwOf(writerNow), deployPw]) assert.ok(!r.all.includes(p), 'パスワードを出さない');
    }
  });
  await ta('[9] 🚨 同時に 2 本 (どちらもロールが無いと思って始める): 後の方は advisory lock で待ち、先の方が作った後に「もうある」と見てパスワードを変えない', async () => {
    await dropWatchRoles(['watcher', 'watch_writer']);
    const A0 = await openPgClient(deployUrl), B = await openPgClient(deployUrl);
    let bPromise = null, sawWait = false;
    const A = { query: async (sql, p) => {
      if (!bPromise && /^create role watcher$/.test(sql)) {
        // A は鍵を持って作り始めた。ここで B を始め、B が鍵で待つのを見てから A を進める (鍵が無い作りなら B は先へ進んでしまう)
        bPromise = createRoles(B, {}).then((v) => ({ v }), (e) => ({ e }));
        for (let i = 0; i < 100 && !sawWait; i++) {
          sawWait = Number((await admin.query(`select count(*)::int as n from pg_locks where locktype = 'advisory' and not granted`)).rows[0].n) > 0;
          if (!sawWait) await new Promise((r) => setTimeout(r, 50));
        }
      }
      return A0.query(sql, p);
    } };
    try {
      const ra = await createRoles(A, {});
      const rb = await bPromise;
      assert.ok(sawWait, 'B は advisory lock で待った');
      assert.deepEqual(ra.created.sort(), ['watch_writer', 'watcher']);
      assert.ok(!rb.e, rb.e && rb.e.message);
      assert.deepEqual([rb.v.created, rb.v.pw], [[], {}], '後の方は「もうある」と見てパスワードを付けない');
      assert.equal(await login(urlFor(dbUrl, 'watcher', ra.pw.watcher)), 'ok', '先の方のパスワードのまま');
      assert.equal(await login(urlFor(dbUrl, 'watch_writer', ra.pw.watch_writer)), 'ok');
      assert.equal(typeof ROLES_LOCK_NAME, 'string');
    } finally { await A0.end(); await B.end(); }
  });
  await ta('[10] 🚨 確かめの後に鍵の外で (人の手などで) ロールが作られた = 素の create role が 42710 で全部取り消す・そのロールのパスワードは変えない', async () => {
    await dropWatchRoles(['watcher', 'watch_writer']);
    const C0 = await openPgClient(deployUrl);
    let injected = false;
    const C = { query: async (sql, p) => {
      if (!injected && /^create role watcher$/.test(sql)) { injected = true; await admin.query(`create role watcher with login password 'by-hand-${tag}'`); }
      return C0.query(sql, p);
    } };
    try {
      await assert.rejects(createRoles(C, {}), (e) => e.code === '42710');
      assert.ok(injected);
      assert.equal(await login(urlFor(dbUrl, 'watcher', `by-hand-${tag}`)), 'ok', '人が付けたパスワードのまま');
      assert.equal(Number((await admin.query(`select count(*)::int as n from pg_roles where rolname = 'watch_writer'`)).rows[0].n), 0, '取り消した = watch_writer も作っていない');
    } finally { await C0.end(); }
  });
} finally {
  try {
    for (const d of [dbName, dbName2]) {
      await admin.query(`select pg_terminate_backend(pid) from pg_stat_activity where datname = $1 and pid <> pg_backend_pid()`, [d]);
      await admin.query(`drop database if exists ${d}`);
    }
    for (const r of ['watcher', 'watch_writer', deploy]) await admin.query(`drop role if exists ${r}`);
  } catch (e) { console.error(`[後片付け] ${e.message}`); }
  await admin.end();
}
console.log(`${passed} ok`);
