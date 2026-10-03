#!/usr/bin/env node
/**
 * test-company-db-migrate-lock-pg.mjs — migrate.mjs の全体の排他と concurrent-index の migration を本物の PostgreSQL で確かめる
 *   (D-60 PR 3a-i・設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「migrate の runner の契約 (3a-i)」)
 *
 * 固定する契約:
 *   L1 2 本の runner を同時に起動 → 2 本目は待たずに MIGRATE_LOCKED (CLI は exit 1・「別の migrate が動いている」)・何も流さない。--list は lock を取らずに読める・--dry-run は止まる
 *   L2 1 本目が途中の SQL で失敗 → ROLLBACK を終えてから unlock (順を記録で確かめる)・次の runner が取れる・接続は取引の外
 *   L3 接続が切れた (backend を terminate・socket を切る) → lock が外れて次の runner が取れる
 *   L4 本物の migrations (0001〜) を全部流す = 今までどおり (applied = 全部 → 2 回目は 0)・lock は残らない
 *   C1 concurrent-index: 正常 → valid と属性の一致で記録・もう一度流しても何もしない
 *   C2 許さない文が混じる (と各種の形) → 流す前に止まり何もしない (前の番号のふつうの migration も流さない)
 *   C3 lock_timeout で invalid が残る → 記録しない・回収の手順が出る → 長い取引が終わってからもう一度流すと invalid を作り直して記録
 *   C4 同じ名前で定義が違う index がある → 止まる (今の index に触らない・記録しない)
 *   C5 作り済みの valid (source の空白・大文字が違っても属性が同じ) は飛ばして続きを作る
 *   C6 容量: 上限が無い・見込みが上限 × 0.8 を超える → 流さない (dry-run も止まる)
 *   C7 PostgreSQL の major が期待と違う → 流さない
 *   C8 同じ表の index の作りが別の接続で動いている → 止まる
 *   C9 drop index concurrently の migration
 *   P1 PGlite の adapter = concurrent-index は既定で止まる (何も流さない)・'skip' なら明示で飛ばす
 *   A1 属性の比べ方 = 空白・大文字が違っても同じ / 部分 index の条件・演算子のクラス・並びが違えば違う
 * 使い方: node scripts/test-company-db-migrate-lock-pg.mjs   (npm run test:company-db にも入っている)
 *   使い捨てのクラスタを embedded-postgres で起動し、最後に止めて消す (test-company-db-profit-fn-revoke-pg.mjs と同じ作り)。
 *   見つからない・版が違う・起動できない・フォルダが消えない = 失敗 (exit 1)
 */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import {
  openPgClient, pgAdapter, pgliteAdapter, applyMigrations, withMigrateLock, listMigrationFiles, buildIndexExpect, readIndexAttrs, attrDiff,
  splitSqlStatements, parseConcurrentIndexStatement, planConcurrentIndexFile, MIGRATE_LOCK_NAME, DEFAULT_DIR,
} from './company-db/migrate.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
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
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の migrate の試験を流せない (飛ばさない)。リポジトリで npm ci');
  process.exit(1);
}
const { EmbeddedPostgres } = loaded;
const clusterDir = path.join(os.tmpdir(), `cdb-migrate-lock-pg-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
const cluster = new EmbeddedPostgres({ databaseDir: clusterDir, user: 'postgres', password: SU_PW, port, persistent: false, onLog: () => {}, onError: () => {} });
let cleanupFailed = false;
const stopCluster = async () => {
  try { await cluster.stop(); } catch (e) { console.error('使い捨てのクラスタを止めるときの誤り: ' + e.message); }
  for (let i = 0; i < 10 && fs.existsSync(clusterDir); i++) { try { fs.rmSync(clusterDir, { recursive: true, force: true }); } catch { await new Promise((r) => setTimeout(r, 500)); } }
  if (fs.existsSync(clusterDir)) { cleanupFailed = true; console.error('❌ 使い捨てのクラスタのフォルダが消えない: ' + clusterDir); }
};
const suUrl = `postgres://postgres:${SU_PW}@127.0.0.1:${port}/postgres`;
console.log('使い捨てのクラスタ: embedded-postgres ' + PINNED_EMBEDDED_PG + ' (' + loaded.from + ')');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const BIG_DISK = { limitBytes: 100 * 1024 ** 3 };

const hex = crypto.randomBytes(4).toString('hex');
const OWNER = `cdb_mig_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const tmpDirs = [];
/** 試験用の migrations のフォルダ (files = { 'NNNN_name.sql': text, ... }) */
const mkDir = (files) => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-miglock-'));
  tmpDirs.push(d);
  for (const [n, txt] of Object.entries(files)) fs.writeFileSync(path.join(d, n), txt);
  return d;
};
const BASE = `create schema app;
create table app.t (id bigint primary key, a text not null, b int not null, c date);
insert into app.t select g, 'x' || g, g % 100, date '2026-01-01' + (g % 30) from generate_series(1, 3000) g;
`;
const CI_SQL = `-- migrate:concurrent-index
-- 試験の index 2 つ (部分 index・include・desc・式・演算子のクラス)
create index concurrently if not exists t_a_b_idx on app.t using btree (a, b desc) include (c) where b > 10;
create unique index concurrently if not exists t_lower_a_uidx on app.t (lower(a) text_pattern_ops) where a is not null;
`;
const CI_STMTS = splitSqlStatements(CI_SQL).map((s) => s.sql);

const clients = [];
let setupError = null;
try {
  await cluster.initialise();
  await cluster.start();
  const su = await openPgClient(suUrl);
  clients.push(su);
  await su.query(`create role ${OWNER} login createrole password '${PW}'`);   // Render の default user と同じ = superuser でない
  let dbSeq = 0;
  const newDb = async () => { const name = `cdb_mig_${hex}_${++dbSeq}`; await su.query(`create database ${name} owner ${OWNER}`); return name; };
  const urlOf = (dbName) => { const x = new URL(suUrl); x.username = OWNER; x.password = PW; x.pathname = `/${dbName}`; return x.toString(); };
  const open = async (dbName) => { const c = await openPgClient(urlOf(dbName)); c.on('error', () => {}); clients.push(c); return c; };
  /** その DB で migrate の lock を持っている pid (無ければ []) */
  const lockHolders = async (dbName) => (await su.query(`select l.pid from pg_locks l
     where l.locktype = 'advisory' and l.granted and l.objsubid = 1 and l.database = (select oid from pg_database where datname = $1)
       and ((l.classid::bigint << 32) | l.objid::bigint) = hashtextextended($2, 0)`, [dbName, MIGRATE_LOCK_NAME])).rows.map((r) => r.pid);
  const waitFor = async (cond, ms = 10000, what = '') => { const end = Date.now() + ms; while (Date.now() < end) { if (await cond()) return; await sleep(50); } throw new Error('待っても来ない: ' + what); };
  const pidOf = async (c) => (await c.query('select pg_backend_pid() as p')).rows[0].p;
  const versions = async (c) => (await c.query('select version from ops.schema_migrations order by 1')).rows.map((r) => r.version);
  const runCli = (args) => spawnSync(process.execPath, ['scripts/company-db/migrate.mjs', ...args], { cwd: ROOT, env: { ...process.env, COMPANY_DB_URL: '', CDB_DB_LIMIT_BYTES: String(BIG_DISK.limitBytes) }, encoding: 'utf8', timeout: 60000 });
  /** 記録の残る adapter (順を確かめる) */
  const recording = (c) => { const a = pgAdapter(c); const logs = []; return { logs, caps: a.caps, query: (q, p) => { logs.push(q); return a.query(q, p); }, exec: (q) => { logs.push(q); return a.exec(q); } }; };
  /** 手で作る (取引の外・1 文ずつ) */
  const runStmts = async (c, stmts) => { for (const s of stmts) await c.query(s); };

  // ─── 期待の属性 (expect.json) は使い捨ての DB で同じ文を流して作る (手で書かない) ───
  const dbX = await newDb();
  const cx = await open(dbX);
  await applyMigrations(pgAdapter(cx), { dir: mkDir({ '0001_base.sql': BASE }), log: quiet });
  await runStmts(cx, CI_STMTS);
  const ciFile = { version: '0002', name: 'idx', file: '0002_idx.sql', text: CI_SQL, concurrentIndex: true };
  const EXPECT = await buildIndexExpect(pgAdapter(cx), ciFile);
  const ciDir = (extra = {}) => mkDir({ '0001_base.sql': BASE, '0002_idx.sql': CI_SQL, '0002_idx.expect.json': JSON.stringify(EXPECT, null, 2), ...extra });

  console.log('全体の排他 (lock)');
  await t('L0 鍵は設計の値 (hashtextextended(company_db_migrate, 0))・company_db_heavy と別・ほかのコードで同じ名前を使っていない', async () => {
    const r = (await su.query(`select hashtextextended('company_db_migrate', 0) = hashtextextended('company_db_heavy', 0) as same`)).rows[0];
    assert.equal(r.same, false);
    assert.equal(MIGRATE_LOCK_NAME, 'company_db_migrate');
    const hits = [];
    const walk = (d) => { for (const e of fs.readdirSync(d, { withFileTypes: true })) { if (['node_modules', '.git'].includes(e.name)) continue; const p = path.join(d, e.name); if (e.isDirectory()) walk(p); else if (/\.(m?js|sql)$/.test(e.name) && fs.readFileSync(p, 'utf8').includes('company_db_migrate')) hits.push(path.relative(ROOT, p).replace(/\\/g, '/')); } };
    for (const d of ['apps', 'lib', 'db', 'scripts']) walk(path.join(ROOT, d));
    assert.deepEqual(hits.sort(), ['scripts/company-db/migrate.mjs', 'scripts/test-company-db-migrate-lock-pg.mjs']);
  });

  await t('L1 2 本を同時に起動 → 2 本目は待たずに MIGRATE_LOCKED・何も流さない / 1 本目は最後まで流す', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE, '0002_sleep.sql': 'select pg_sleep(2);\ncreate table app.s (x int);\n' });
    const c1 = await open(dbName), c2 = await open(dbName);
    const p1 = applyMigrations(pgAdapter(c1), { dir, log: quiet });
    const pid1 = await pidOf(c1).catch(() => null);
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, '1 本目が lock を取る');
    const t0 = Date.now();
    await assert.rejects(applyMigrations(pgAdapter(c2), { dir, log: quiet }), (e) => e.code === 'MIGRATE_LOCKED' && /別の migrate が動いている/.test(e.message) && /pid \d+/.test(e.message));
    assert.ok(Date.now() - t0 < 1500, '2 本目が待った');
    const r1 = await p1;
    assert.deepEqual(r1.applied, ['0001', '0002']);
    assert.deepEqual(await versions(c2), ['0001', '0002']);
    assert.deepEqual(await lockHolders(dbName), []);
    void pid1;
  });

  await t('L1b CLI: lock を持たれている間 = 流す・--dry-run は exit 1 (「別の migrate が動いている」) / --list は exit 0 (lock を取らない) / 外れた後は流せる', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE });
    const c1 = await open(dbName);
    let release;
    const held = withMigrateLock(pgAdapter(c1), () => new Promise((r) => { release = r; }));
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, 'lock');
    const run = runCli(['--url', urlOf(dbName), '--dir', dir]);
    assert.equal(run.status, 1, run.stdout + run.stderr);
    assert.match(run.stderr, /FAILED \(MIGRATE_LOCKED\): 別の migrate が動いている/);
    const dry = runCli(['--url', urlOf(dbName), '--dir', dir, '--dry-run']);
    assert.equal(dry.status, 1); assert.match(dry.stderr, /MIGRATE_LOCKED/);
    const list = runCli(['--url', urlOf(dbName), '--dir', dir, '--list']);
    assert.equal(list.status, 0, list.stderr); assert.match(list.stdout, /0001 pending/);
    release(); await held;
    assert.deepEqual(await lockHolders(dbName), []);
    const run2 = runCli(['--url', urlOf(dbName), '--dir', dir]);
    assert.equal(run2.status, 0, run2.stderr); assert.match(run2.stdout, /applied=1/); assert.match(run2.stdout, /lock company_db_migrate を外した/);
  });

  await t('L2 途中の SQL で失敗 → そのファイルは巻き戻り、ROLLBACK を終えてから unlock・接続は取引の外 → 次の runner が取れる', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE, '0002_bad.sql': 'create table app.u (x int);\nselect 1/0;\n', '0003_after.sql': 'create table app.v (x int);\n' });
    const c1 = await open(dbName), c2 = await open(dbName);
    const rec = recording(c1);
    await assert.rejects(applyMigrations(rec, { dir, log: quiet }), (e) => e.code === 'MIGRATION_FAILED' && e.version === '0002' && /division by zero/.test(e.message));
    const iRollback = rec.logs.lastIndexOf('rollback');
    const iUnlock = rec.logs.findIndex((q) => /pg_advisory_unlock/.test(q));
    assert.ok(iRollback >= 0 && iUnlock > iRollback, `rollback (${iRollback}) の後に unlock (${iUnlock})`);
    assert.deepEqual(await lockHolders(dbName), [], 'lock が残った');
    // c1 はつながったまま = lock が外れたのは unlock のおかげ (接続が切れたからではない)
    const st = (await su.query('select state from pg_stat_activity where pid = $1', [await pidOf(c1)])).rows[0].state;
    assert.equal(st, 'idle', '取引が開いたまま');
    assert.equal((await c1.query(`select to_regclass('app.u') is null as gone`)).rows[0].gone, true);
    assert.deepEqual(await versions(c1), ['0001']);
    // 次の runner (別の接続) が取れる
    await withMigrateLock(pgAdapter(c2), async () => { assert.deepEqual(await lockHolders(dbName), [await pidOf(c2)]); });
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('L2b 流す前の検査で止まっても (concurrent-index の許さない文) lock は外れる', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE, '0002_x.sql': '-- migrate:concurrent-index\ncreate table app.evil (x int);\n' });
    const c1 = await open(dbName);
    await assert.rejects(applyMigrations(pgAdapter(c1), { dir, log: quiet }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED');
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('L3 接続が切れたら lock が外れる (backend を terminate / client の socket を切る)', async () => {
    const dbName = await newDb();
    const c3 = await open(dbName), c4 = await open(dbName), c5 = await open(dbName);
    withMigrateLock(pgAdapter(c3), () => new Promise(() => {})).catch(() => {});
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, 'lock');
    await assert.rejects(withMigrateLock(pgAdapter(c5), async () => {}), (e) => e.code === 'MIGRATE_LOCKED');
    await su.query('select pg_terminate_backend($1)', [await lockHolders(dbName).then((p) => p[0])]);
    await waitFor(async () => (await lockHolders(dbName)).length === 0, 10000, 'terminate で外れる');
    await withMigrateLock(pgAdapter(c5), async () => {});
    // socket を切る (client が落ちた = Postgres の backend が EOF で終わる)
    withMigrateLock(pgAdapter(c4), () => new Promise(() => {})).catch(() => {});
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, 'lock (2)');
    c4.connection.stream.destroy();
    await waitFor(async () => (await lockHolders(dbName)).length === 0, 10000, 'socket を切って外れる');
    await withMigrateLock(pgAdapter(c5), async () => {});
  });

  await t('L4 本物の migrations (0001〜) を全部流す = 今までどおり (2 回目は 0 本)・lock は残らない', async () => {
    const dbName = await newDb();
    const c1 = await open(dbName);
    const all = listMigrationFiles().map((f) => f.version);
    const r = await applyMigrations(pgAdapter(c1), { log: quiet });
    assert.deepEqual(r.applied, all);
    assert.equal(r.unsupported.length, 0);
    const r2 = await applyMigrations(pgAdapter(c1), { log: quiet });
    assert.deepEqual([r2.applied, r2.skipped.length], [[], all.length]);
    assert.deepEqual(await lockHolders(dbName), []);
  });

  console.log('concurrent-index の migration');
  await t('A1 属性の比べ方: 空白・大文字が違っても同じ / 部分 index の条件・演算子のクラス・並びが違えば違う', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await applyMigrations(pgAdapter(c), { dir: mkDir({ '0001_base.sql': BASE }), log: quiet });
    await c.query('CREATE   INDEX CONCURRENTLY IF NOT EXISTS T_A_B_IDX ON APP.T USING BTREE ( A , B DESC ) INCLUDE ( C ) WHERE B>10');
    assert.deepEqual(attrDiff((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).attrs, EXPECT.indexes['app.t_a_b_idx']), []);
    await c.query('create index concurrently w1 on app.t (a, b desc) include (c) where b > 11');
    await c.query('create index concurrently w2 on app.t (a text_pattern_ops, b desc) include (c) where b > 10');
    await c.query('create index concurrently w3 on app.t (a, b) include (c) where b > 10');
    await c.query('create index concurrently w4 on app.t (a, b desc) where b > 10');
    for (const [w, k] of [['w1', 'predicate'], ['w2', 'columns'], ['w3', 'columns'], ['w4', 'all_columns']]) {
      const d = attrDiff((await readIndexAttrs(pgAdapter(c), 'app', w)).attrs, EXPECT.indexes['app.t_a_b_idx']);
      assert.ok(d.some((x) => x.startsWith(k + ':')), `${w} の違いに ${k} が無い: ${d.join(' / ')}`);
    }
    assert.equal(EXPECT.pg_major, 18);
    assert.equal(EXPECT.indexes['app.t_lower_a_uidx'].unique, true);
    assert.match(EXPECT.indexes['app.t_lower_a_uidx'].expressions, /lower/);
  });

  await t('C1 正常: 取引の外で流し、valid と属性の一致の後にだけ記録 / もう一度流しても何もしない', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    const logs = [];
    const r = await applyMigrations(pgAdapter(c), { dir: ciDir(), log: (m) => logs.push(m), disk: BIG_DISK });
    assert.deepEqual(r.applied, ['0001', '0002']);
    for (const k of ['t_a_b_idx', 't_lower_a_uidx']) {
      const a = await readIndexAttrs(pgAdapter(c), 'app', k);
      assert.deepEqual([a.valid, a.ready, a.live], [true, true, true]);
      assert.deepEqual(attrDiff(a.attrs, EXPECT.indexes[`app.${k}`]), []);
    }
    assert.ok(logs.some((m) => /容量: 今 .* index の見込み .*上限/.test(m)), '容量を出していない');
    assert.deepEqual(await versions(c), ['0001', '0002']);
    // session の設定は戻っている
    assert.equal((await c.query('show lock_timeout')).rows[0].lock_timeout, '0');
    assert.equal((await c.query('show statement_timeout')).rows[0].statement_timeout, '0');
    const r2 = await applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, disk: BIG_DISK });
    assert.deepEqual(r2.applied, []);
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('C2 許さない文が混じる → 流す前に止まる・何もしない (前の番号のふつうの migration も流さない)', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    const bad = `-- migrate:concurrent-index\ncreate index concurrently if not exists t_a_b_idx on app.t (a);\ncreate table app.evil (x int);\n`;
    const dir = mkDir({ '0001_base.sql': BASE, '0002_bad.sql': bad, '0002_bad.expect.json': JSON.stringify({ format: 'company-db-index-expect/1', pg_major: 18, indexes: { 'app.t_a_b_idx': EXPECT.indexes['app.t_a_b_idx'] } }) });
    await assert.rejects(applyMigrations(pgAdapter(c), { dir, log: quiet, disk: BIG_DISK }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED' && /許す文は/.test(e.message));
    assert.deepEqual(await versions(c), []);
    assert.equal((await c.query(`select count(*)::int as n from pg_namespace where nspname = 'app'`)).rows[0].n, 0, '0001 を流した');
    // 各種の形 (DB なし)
    const reject = [
      ['begin', 'begin'],
      ['set', "set lock_timeout = '1s'"],
      ['ふつうの create index', 'create index if not exists x on app.t (a)'],
      ['if not exists なし', 'create index concurrently x on app.t (a)'],
      ['schema なしの表', 'create index concurrently if not exists x on t (a)'],
      ['引用の名前', 'create index concurrently if not exists "X" on app.t (a)'],
      ['with (…)', 'create index concurrently if not exists x on app.t (a) with (fillfactor = 50)'],
      ['tablespace', 'create index concurrently if not exists x on app.t (a) tablespace pg_default'],
      ['gin', 'create index concurrently if not exists x on app.t using gin (a)'],
      ['nulls not distinct', 'create unique index concurrently if not exists x on app.t (a) nulls not distinct'],
      ['drop cascade', 'drop index concurrently if exists app.x cascade'],
      ['drop に schema なし', 'drop index concurrently if exists x'],
      ['drop if exists なし', 'drop index concurrently app.x'],
      ['ドルの引用', 'create index concurrently if not exists x on app.t (a) where a <> $$q$$'],
      ['reindex', 'reindex index concurrently app.x'],
      ['alter', 'alter index app.x rename to y'],
      ['空の列', 'create index concurrently if not exists x on app.t ()'],
    ];
    for (const [what, sql] of reject) {
      assert.throws(() => parseConcurrentIndexStatement(splitSqlStatements(sql)[0], 'f.sql'), (e) => e.code === 'CONCURRENT_INDEX_REJECTED', what);
    }
    // 許す形 (コメント・文字列の中の ; と 'concurrently' は数えない)
    const okSql = `create unique index concurrently if not exists x on only app.t using btree (lower(a), (b + 1) desc nulls last) include (c) where a <> 'p;q' /* ; */ and b > 0 -- ;\n`;
    const p = parseConcurrentIndexStatement(splitSqlStatements(okSql)[0], 'f.sql');
    assert.deepEqual([p.kind, p.unique, p.schema, p.table, p.name], ['create', true, 'app', 't', 'x']);
    assert.equal(splitSqlStatements(okSql).length, 1);
    // expect.json が無い・別の index がある → 止まる
    const noExpect = mkDir({ '0001_base.sql': BASE, '0002_idx.sql': CI_SQL });
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: noExpect, log: quiet, disk: BIG_DISK }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED' && /expect\.json が無い/.test(e.message));
    const f2 = { version: '0002', name: 'idx', file: '0002_idx.sql', text: CI_SQL, concurrentIndex: true };
    const d3 = mkDir({ '0002_idx.expect.json': JSON.stringify({ ...EXPECT, indexes: { ...EXPECT.indexes, 'app.other': EXPECT.indexes['app.t_a_b_idx'] } }) });
    assert.throws(() => planConcurrentIndexFile(f2, d3), /app\.other はこのファイルで作らない/);
    // 印の無いファイルに concurrently = 止まる (取引の中に入る前)
    const unmarked = mkDir({ '0001_base.sql': BASE, '0002_u.sql': '-- index\ncreate index concurrently if not exists t_a_idx on app.t (a);\n' });
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: unmarked, log: quiet }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED' && /1 行目が/.test(e.message));
    const marker2 = mkDir({ '0001_base.sql': BASE, '0002_u.sql': '-- a\n-- migrate:concurrent-index\ncreate index concurrently if not exists t_a_idx on app.t (a);\n' });
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: marker2, log: quiet }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED');
    assert.deepEqual(await versions(c), []);
  });

  await t('C3 lock_timeout で invalid が残る → 記録しない・回収の手順を出す → 長い取引が終わってから流すと作り直して記録', async () => {
    const dbName = await newDb();
    const c = await open(dbName), blocker = await open(dbName);
    await applyMigrations(pgAdapter(c), { dir: ciDir(), to: '0001', log: quiet });
    await blocker.query('begin');
    await blocker.query(`insert into app.t values (100000, 'y100000', 1, null)`);   // RowExclusive の取引を開いたまま = CIC は書き手を待つ
    let err = null;
    try { await applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, disk: BIG_DISK, concurrentIndexSettings: { lockTimeout: '1s' } }); } catch (e) { err = e; }
    assert.ok(err, '止まらなかった');
    assert.equal(err.code, 'MIGRATION_FAILED'); assert.match(err.message, /lock timeout/); assert.match(err.message, /回収の手順/);
    assert.deepEqual(err.invalidIndexes, ['app.t_a_b_idx']);
    const inv = await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx');
    assert.equal(inv.valid, false);
    assert.deepEqual(await versions(c), ['0001']);
    assert.deepEqual(await lockHolders(dbName), []);
    assert.equal((await c.query('show lock_timeout')).rows[0].lock_timeout, '0', 'session の設定が戻っていない');
    await blocker.query('commit');
    const logs = [];
    const r = await applyMigrations(pgAdapter(c), { dir: ciDir(), log: (m) => logs.push(m), disk: BIG_DISK });
    assert.deepEqual(r.applied, ['0002']);
    assert.ok(logs.some((m) => /app\.t_a_b_idx は invalid .* drop index concurrently してから作り直す/.test(m)), logs.join('\n'));
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).valid, true);
    assert.equal((await c.query(`select count(*)::int as n from pg_index where not indisvalid`)).rows[0].n, 0);
  });

  await t('C4 同じ名前で定義が違う index がある → 止まる (今の index に触らない・記録しない)', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await applyMigrations(pgAdapter(c), { dir: ciDir(), to: '0001', log: quiet });
    await c.query('create index t_a_b_idx on app.t (a)');
    const before = (await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).attrs;
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, disk: BIG_DISK }), (e) => e.code === 'MIGRATION_FAILED' && e.reason === 'DEFINITION_MISMATCH' && /同じ名前で定義が違う/.test(e.message));
    assert.deepEqual((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).attrs, before);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_lower_a_uidx'), null, '後ろの文を流した');
    assert.deepEqual(await versions(c), ['0001']);
    // 名前が表に取られている
    const db2 = await newDb(); const c2 = await open(db2);
    await applyMigrations(pgAdapter(c2), { dir: ciDir(), to: '0001', log: quiet });
    await c2.query('create table app.t_a_b_idx (x int)');
    await assert.rejects(applyMigrations(pgAdapter(c2), { dir: ciDir(), log: quiet, disk: BIG_DISK }), (e) => e.reason === 'NAME_TAKEN');
  });

  await t('C5 作り済みの valid (空白・大文字が違う SQL で作った) は飛ばして続きを作り、記録する', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await applyMigrations(pgAdapter(c), { dir: ciDir(), to: '0001', log: quiet });
    await c.query('CREATE INDEX t_a_b_idx ON app.t (A, B DESC) INCLUDE (C) WHERE (B > 10)');
    const logs = [];
    const r = await applyMigrations(pgAdapter(c), { dir: ciDir(), log: (m) => logs.push(m), disk: BIG_DISK });
    assert.deepEqual(r.applied, ['0002']);
    assert.ok(logs.some((m) => /app\.t_a_b_idx は作り済み .* 飛ばす/.test(m)));
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_lower_a_uidx')).valid, true);
  });

  await t('C6 容量: 上限が無い・見込みが上限 × 0.8 を超える → 流さない (dry-run も止まる)・CLI は env の上限を読む', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await applyMigrations(pgAdapter(c), { dir: ciDir(), to: '0001', log: quiet });
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet }), (e) => e.code === 'DISK_CHECK_FAILED' && /CDB_DB_LIMIT_BYTES/.test(e.message));
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, disk: { limitBytes: 10 * 1024 ** 2 } }), (e) => e.code === 'DISK_CHECK_FAILED' && /を超える/.test(e.message) && e.disk && e.disk.estimateBytes > 0);
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, dryRun: true, disk: { limitBytes: 10 * 1024 ** 2 } }), (e) => e.code === 'DISK_CHECK_FAILED');
    // 見込み = 表の大きさ × 3 (2 つの index で 2 回) が効く: 上限の境い目
    const size = Number((await c.query(`select pg_table_size('app.t')::text as b`)).rows[0].b);
    const used = Number((await c.query(`select coalesce(sum(pg_database_size(oid)), 0)::text as b from pg_database where datallowconn and has_database_privilege(oid, 'CONNECT')`)).rows[0].b);
    const need = used + 1024 ** 3 + size * 3 * 2 + 512 * 1024 ** 2;   // WAL は予約 1GB (この役割は pg_ls_waldir を読めない)
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, disk: { limitBytes: Math.floor((need - size) / 0.8) } }), (e) => e.code === 'DISK_CHECK_FAILED');
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null, '止まったのに作った');
    const dry = await applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, dryRun: true, disk: { limitBytes: Math.ceil((need + 64 * 1024 ** 2) / 0.8) } });
    assert.deepEqual(dry.pending, ['0002']);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null, 'dry-run で作った');
    const cliDry = runCli(['--url', urlOf(dbName), '--dir', ciDir(), '--dry-run']);
    assert.equal(cliDry.status, 0, cliDry.stderr); assert.match(cliDry.stdout, /dry-run: 0002_idx\.sql を流す予定 \(concurrent-index/);
    const cliNoLimit = spawnSync(process.execPath, ['scripts/company-db/migrate.mjs', '--url', urlOf(dbName), '--dir', ciDir()], { cwd: ROOT, env: { ...process.env, CDB_DB_LIMIT_BYTES: '' }, encoding: 'utf8', timeout: 60000 });
    assert.equal(cliNoLimit.status, 1); assert.match(cliNoLimit.stderr, /DISK_CHECK_FAILED/);
    const cli = runCli(['--url', urlOf(dbName), '--dir', ciDir()]);
    assert.equal(cli.status, 0, cli.stderr); assert.match(cli.stdout, /applied=1/);
  });

  await t('C7 PostgreSQL の major が期待と違う → 流さない', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await applyMigrations(pgAdapter(c), { dir: ciDir(), to: '0001', log: quiet });
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, disk: BIG_DISK, expectedPgMajor: 17 }), (e) => e.code === 'PG_MAJOR_MISMATCH');
    const d = ciDir({ '0002_idx.expect.json': JSON.stringify({ ...EXPECT, pg_major: 17 }) });
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: d, log: quiet, disk: BIG_DISK }), (e) => e.code === 'PG_MAJOR_MISMATCH' && /expect\.json の pg_major/.test(e.message));
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null);
  });

  await t('C8 同じ表の index の作りが別の接続で動いている → 止まる (何も作らない)', async () => {
    const dbName = await newDb();
    const c = await open(dbName), blocker = await open(dbName), other = await open(dbName);
    await applyMigrations(pgAdapter(c), { dir: ciDir(), to: '0001', log: quiet });
    await blocker.query('begin');
    await blocker.query(`insert into app.t values (100001, 'y100001', 1, null)`);
    const otherPid = await pidOf(other);
    const building = other.query('create index concurrently other_idx on app.t (b)').catch((e) => e);
    await waitFor(async () => (await su.query('select count(*)::int as n from pg_stat_progress_create_index where pid = $1', [otherPid])).rows[0].n > 0, 10000, '別の作りが始まる');
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, disk: BIG_DISK }), (e) => e.code === 'MIGRATION_FAILED' && e.reason === 'BUILD_IN_PROGRESS' && new RegExp(`pid ${otherPid}`).test(e.message));
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null);
    await su.query('select pg_cancel_backend($1)', [otherPid]);
    await building;
    await blocker.query('rollback');
    assert.deepEqual(await versions(c), ['0001']);
  });

  await t('C9 drop index concurrently の migration (expect.json 無しでよい・消えたことを確かめて記録)', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    const d = ciDir({ '0003_drop.sql': '-- migrate:concurrent-index\ndrop index concurrently if exists app.t_lower_a_uidx;\n' });
    const r = await applyMigrations(pgAdapter(c), { dir: d, log: quiet, disk: BIG_DISK });
    assert.deepEqual(r.applied, ['0001', '0002', '0003']);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_lower_a_uidx'), null);
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).valid, true);
  });

  await t('P1 PGlite の adapter: concurrent-index は既定で止まる (何も流さない)・skip なら明示で飛ばして記録しない', async () => {
    const pg = new PGlite();
    try {
      const db = pgliteAdapter(pg);
      await assert.rejects(applyMigrations(db, { dir: ciDir(), log: quiet }), (e) => e.code === 'CONCURRENT_INDEX_UNSUPPORTED');
      assert.equal((await db.query('select count(*)::int as n from ops.schema_migrations')).rows[0].n, 0);
      const logs = [];
      const r = await applyMigrations(db, { dir: ciDir(), log: (m) => logs.push(m), onUnsupportedConcurrentIndex: 'skip' });
      assert.deepEqual([r.applied, r.unsupported], [['0001'], ['0002']]);
      assert.ok(logs.some((m) => /skip 0002_idx\.sql/.test(m)));
      assert.equal((await db.query(`select count(*)::int as n from pg_class where relname = 't_a_b_idx'`)).rows[0].n, 0);
      // PGlite でも許さない文は先に止まる (skip でも検査は飛ばさない)
      const bad = mkDir({ '0001_base.sql': BASE, '0002_x.sql': '-- migrate:concurrent-index\ncreate table app.evil (x int);\n' });
      const pg2 = new PGlite();
      try { await assert.rejects(applyMigrations(pgliteAdapter(pg2), { dir: bad, log: quiet, onUnsupportedConcurrentIndex: 'skip' }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED'); } finally { await pg2.close(); }
    } finally { await pg.close(); }
  });

  await t('X1 --index-expect は使い捨ての DB から expect.json の中身を出す (試験で作ったものと同じ)', async () => {
    const d = ciDir();
    const r = runCli(['--url', urlOf(dbX), '--dir', d, '--index-expect', '0002']);
    assert.equal(r.status, 0, r.stderr);
    assert.deepEqual(JSON.parse(r.stdout), EXPECT);
  });
} catch (e) {
  setupError = e;
  console.error('❌ 準備か試験の外で落ちた (飛ばさない): ' + (e.stack || e.message));
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* */ } }
  await stopCluster();
}
console.log(`\n${ok} ok / ${ng} NG`);
// 🚨 embedded-postgres の async-exit-hook が exitCode を上書きする = 明示で process.exit
process.exit(ng || setupError || cleanupFailed ? 1 : 0);
