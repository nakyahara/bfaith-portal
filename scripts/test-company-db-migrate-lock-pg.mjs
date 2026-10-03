#!/usr/bin/env node
/**
 * test-company-db-migrate-lock-pg.mjs — migrate.mjs の全体の排他と concurrent-index の migration を本物の PostgreSQL で確かめる
 *   (D-60 PR 3a-i・設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「migrate の runner の契約 (3a-i)」v3.9)
 *
 * 固定する契約:
 *   L0b runner は db/company/migrations/ の番号つきの file だけを読む (migrations-pending/ は読まない)
 *   L1 2 本の runner (CLI の入口 = migrateWithLock) を同時に起動 → 2 本目は待たずに MIGRATE_LOCKED・何も流さない
 *   L1b CLI: lock を持たれている間 = 流す・--dry-run は exit 1 (「別の migrate が動いている」) / --list は exit 0 で持ち主の pid を出す / 外れた後は流せる
 *   L2 1 本目が途中の SQL で失敗 → ROLLBACK が終わってから unlock (順を記録で確かめる)・pg_locks に残らない・次の runner がすぐ取れる
 *   L2c ROLLBACK も失敗 (接続が死んだ) → unlock は呼ばない・接続を捨てれば外れる
 *   L3 接続が切れた (backend を terminate・socket を切る) → lock が外れて次の runner が取れる
 *   L4 本物の migrations (0001〜) を全部流す = 今までどおり (2 回目は 0 本)・applied_by に migrate-v2・lock は残らない
 *   L5 applyMigrations() は lock を取らない (使い回す関数に Postgres 専用の SQL を無条件で入れない) / dry-run は DDL を流さない
 *   C1 concurrent-index: 正常 → valid と属性の一致で記録・容量を出す・もう一度流しても何もしない
 *   C1b lock を持たずに (applyMigrations を直に) concurrent-index を流す → MIGRATE_LOCK_REQUIRED
 *   C2 許さない文が混じる (と各種の形) → 流す前に止まり何もしない (前の番号のふつうの migration も流さない)
 *   C3 lock_timeout で invalid が残る → 記録しない・回収の手順が出る → 長い取引が終わってから流すと invalid を作り直して記録
 *   C4 同じ名前で定義が違う index がある → 止まる (今の index に触らない・記録しない)
 *   C5 作り済みの valid (source の空白・大文字が違っても属性が同じ) は飛ばして続きを作る
 *   C6 容量: メトリクスが読めない・空きが 予想 × 3 + 2GB に満たない・ANALYZE していない → 流さない (fail-closed)。CLI は RENDER_API_KEY が無ければ流さない
 *   C7 PostgreSQL の major が期待と違う → 流さない
 *   C8 同じ表の index の作りが別の接続で動いている → 止まる
 *   C9 drop index concurrently の migration
 *   P1 PGlite の adapter = concurrently を外してふつうの取引で流し、属性の検証は同じ (PG 18 で作った expect.json に PGlite でも一致)
 *   A1 属性の比べ方 = 空白・大文字が違っても同じ / 部分 index の条件・演算子のクラス・並びが違えば違う
 *   O0 migration の file は許さない役割の切り替えを持たない (grep の縛り・拒む形と許す形の一覧) / O1〜O4 持ち主の mode (Codex R-D60-v3-10 H2) = ① PR 1b の前 (印なし = 接続の役割) / ② PR 1b 自身 (owner-transition = 接続の役割で移し、記録は印の役割・
 *     同じ回の後の file は owner mode) / ③ PR 1b の後 (SET ROLE で流す・接続の役割は記録表に書けない・失敗しても役割が戻り lock が外れる・SET できなければ止まる) /
 *     印を作ってよいのは owner-transition の file だけ・owner-transition が印を作らなければ巻き戻す・2 回目の owner-transition は流さない
 *   O5 印と owner-transition の適用を両方向で確かめる (印が消えた・印だけある → 止まる) / O6 本文が役割を戻しても記録は印の役割で入る (INSERT の時の current_user を trigger で読む)
 *   O7 owner の状態の file の SET LOCAL ROLE = 許す一覧は通る・一覧の外は止まる (設計 13 v3.11 ③)
 *   🆕 Codex R1 (PR #1606): O8 引用の名前の役割の切り替え (RESET "role" ほか) = 本物の PG と PGlite で役割が本当に変わる形を流す前に止める (H1) /
 *     C10 6 本を続けて作る = この回に作った分を後の空きから引く (H3) / C11 容量の resource と接続先の結び付け (H2) / O9 --list の両方向の検査 (M1) /
 *     C12 既存の index の tablespace・fillfactor (M2) / M3 最上位の取引の制御を拒む (0001〜 には無い) / C13 setSession の途中の失敗でも設定が戻る (Low) /
 *     C14 同じ index の名前の操作が 2 つ = 拒む (Low)
 *   🆕 Codex R-D60-v3-13 H1: O0・O6・O8 = ドルの引用の中 (DO の本文) の set_config('role', …) も拒む / 動的 SQL (EXECUTE format) は見ない = ⚠️ を出して流し、記録は印の役割 (補助で sandbox ではない)
 *   🆕 設計 13 v3.14 (形 B): 持ち主の mode の試験は CREATEROLE の無い deployer (DEPLOYER) で流す (役割は superuser が作る) / O10 = 到達の検査の禁止の集合 (owner の状態では必須・本番の道・--dry-run・--list)・
 *     役割の図の比較 / O11 = 記録の INSERT の後の deferred の trigger / C10 ③ = 前の回の valid・未記録の index を含む予約 / C11 = host の対応の fixture・未確認の間は CIC を流さない
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
  openPgClient, pgAdapter, pgliteAdapter, applyMigrations, migrateWithLock, withMigrateLock, listMigrationFiles, buildIndexExpect, readIndexAttrs, attrDiff,
  splitSqlStatements, parseConcurrentIndexStatement, planConcurrentIndexFile, estimateIndexBytes, roleSwitchStatements, ALLOWED_SET_LOCAL_ROLES, MIGRATE_LOCK_NAME, DISK_ESTIMATE, DEFAULT_DIR,
  txControlStatements, renderResourceMatchesUrl, renderDiskMetricsReader, migrationStatus, RENDER_PG_HOST_MAPPING, roleAdminReachable, ROLE_ADMIN_FORBIDDEN_PREDEFINED,
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
const GB = 1024 ** 3;
/** 試験の容量の読み手 (Render のメトリクスの代わり) */
const disk = (capacityBytes, usedBytes) => async () => ({ ok: true, capacityBytes, usedBytes });
const BIG_DISK = disk(100 * GB, 1 * GB);
const NO_METRICS = async () => ({ ok: false, reason: 'METRICS_CONFIG' });

const hex = crypto.randomBytes(4).toString('hex');
const OWNER = `cdb_mig_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const RUNTIME = `cdb_rt_${hex}`;
const DEPLOYER = `cdb_dep_${hex}`;   // 持ち主の mode の試験の接続の役割 (形 B = CREATEROLE なし・役割は superuser (Render / operator の代わり) が作る)   // 夜のバックアップが ops.schema_migrations と ops.migrate_owner を読む役割 (設計 13 v3.10)
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
analyze app.t;
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
  await su.query(`create role ${RUNTIME} login password '${PW}'`);
  await su.query(`create role ${DEPLOYER} login password '${PW}'`);   // 形 B の deployer = 役割の管理・危険な権限に届かない (設計 13 v3.14 ②)
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
  const runCli = (args, env = {}) => spawnSync(process.execPath, ['scripts/company-db/migrate.mjs', ...args], { cwd: ROOT, env: { ...process.env, COMPANY_DB_URL: '', RENDER_API_KEY: '', CDB_RENDER_PG_RESOURCE_ID: '', ...env }, encoding: 'utf8', timeout: 60000 });
  /** 記録の残る adapter (順を確かめる)。failRollback = rollback を接続の死として失敗させる */
  const recording = (c, { failRollback = false } = {}) => {
    const a = pgAdapter(c); const logs = [];
    return { logs, supportsConcurrentIndex: true, query: (q, p) => { logs.push(q); return a.query(q, p); },
      exec: (q) => { logs.push(q); if (failRollback && q === 'rollback') return Promise.reject(new Error('Connection terminated (試験)')); return a.exec(q); } };
  };
  /** 手で作る (取引の外・1 文ずつ) */
  const runStmts = async (c, stmts) => { for (const s of stmts) await c.query(s); };
  const migrate = (c, opts) => migrateWithLock(pgAdapter(c), { log: quiet, readDiskMetrics: BIG_DISK, ...opts });

  // ─── 期待の属性 (expect.json) は使い捨ての DB で同じ文を流して作る (手で書かない) ───
  const dbX = await newDb();
  const cx = await open(dbX);
  await migrate(cx, { dir: mkDir({ '0001_base.sql': BASE }) });
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

  await t('L0b runner は db/company/migrations/ の番号つきの file だけを読む = 番号の無い置き場 db/company/migrations-pending/ (と下のフォルダ) は読まない (設計 19 v8 の F4-2a)', async () => {
    assert.equal(path.relative(ROOT, DEFAULT_DIR).replace(/\\/g, '/'), 'db/company/migrations');
    const parent = mkDir({});
    fs.mkdirSync(path.join(parent, 'migrations'));
    fs.mkdirSync(path.join(parent, 'migrations-pending'));
    fs.mkdirSync(path.join(parent, 'migrations', 'pending'));
    fs.writeFileSync(path.join(parent, 'migrations', '0001_base.sql'), BASE);
    fs.writeFileSync(path.join(parent, 'migrations-pending', '0002_f4_2a.sql'), 'create table app.pending_should_not_run (x int);\n');
    fs.writeFileSync(path.join(parent, 'migrations-pending', 'f4-2a_amazon_finance_copy.sql'), 'create table app.pending2 (x int);\n');   // 設計 19 §6.0 の 3b の 1 の名前
    fs.writeFileSync(path.join(parent, 'migrations', 'pending', '0002_sub.sql'), 'create table app.pending3 (x int);\n');
    fs.writeFileSync(path.join(parent, 'migrations', 'f4_2a_draft.sql'), 'create table app.pending4 (x int);\n');   // 番号の無い file も読まない
    assert.deepEqual(listMigrationFiles(path.join(parent, 'migrations')).map((f) => f.file), ['0001_base.sql']);
    const dbName = await newDb();
    const c = await open(dbName);
    const r = await migrate(c, { dir: path.join(parent, 'migrations') });
    assert.deepEqual(r.applied, ['0001']);
    assert.equal((await c.query(`select count(*)::int as n from pg_class where relname like 'pending%'`)).rows[0].n, 0);
    // --list・--dry-run も数えない
    const list = runCli(['--url', urlOf(dbName), '--dir', path.join(parent, 'migrations'), '--list']);
    assert.equal(list.status, 0, list.stderr);
    assert.deepEqual(list.stdout.split('\n').filter((l) => /^\d{4} /.test(l)).map((l) => l.slice(0, 4)), ['0001']);
    const dry = runCli(['--url', urlOf(dbName), '--dir', path.join(parent, 'migrations'), '--dry-run']);
    assert.equal(dry.status, 0, dry.stderr); assert.match(dry.stdout, /applied=0 skipped=1 pending=0/);
    // PGlite の道も
    const pgl = new PGlite();
    try { assert.deepEqual((await applyMigrations(pgliteAdapter(pgl), { dir: path.join(parent, 'migrations'), log: quiet })).applied, ['0001']); } finally { await pgl.close(); }
    // リポジトリの本物の置き場: migrations-pending があっても、DEFAULT_DIR の一覧に入らない
    const pend = path.join(ROOT, 'db', 'company', 'migrations-pending');
    if (fs.existsSync(pend)) { const names = new Set(listMigrationFiles().map((f) => f.file)); for (const x of fs.readdirSync(pend)) assert.equal(names.has(x), false, x); }
  });

  await t('L1 2 本を同時に起動 → 2 本目は待たずに MIGRATE_LOCKED・何も流さない / 1 本目は最後まで流す', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE, '0002_sleep.sql': 'select pg_sleep(2);\ncreate table app.s (x int);\n' });
    const c1 = await open(dbName), c2 = await open(dbName);
    const p1 = migrate(c1, { dir });
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, '1 本目が lock を取る');
    const t0 = Date.now();
    await assert.rejects(migrate(c2, { dir }), (e) => e.code === 'MIGRATE_LOCKED' && /別の migrate が動いている/.test(e.message) && /pid \d+/.test(e.message));
    assert.ok(Date.now() - t0 < 1500, '2 本目が待った');
    const r1 = await p1;
    assert.deepEqual(r1.applied, ['0001', '0002']);
    assert.deepEqual(await versions(c2), ['0001', '0002']);
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('L1b CLI: lock を持たれている間 = 流す・--dry-run は exit 1 / --list は exit 0 で持ち主の pid を出す / 外れた後は流せる', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE });
    const c1 = await open(dbName);
    let release;
    const held = withMigrateLock(pgAdapter(c1), () => new Promise((r) => { release = r; }));
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, 'lock');
    const pid1 = await pidOf(c1);
    const run = runCli(['--url', urlOf(dbName), '--dir', dir]);
    assert.equal(run.status, 1, run.stdout + run.stderr);
    assert.match(run.stderr, /FAILED \(MIGRATE_LOCKED\): 別の migrate が動いている/);
    const dry = runCli(['--url', urlOf(dbName), '--dir', dir, '--dry-run']);
    assert.equal(dry.status, 1); assert.match(dry.stderr, /MIGRATE_LOCKED/);
    const list = runCli(['--url', urlOf(dbName), '--dir', dir, '--list']);
    assert.equal(list.status, 0, list.stderr); assert.match(list.stdout, /0001 pending/);
    assert.match(list.stdout, new RegExp(`lock \\(company_db_migrate\\) を持っている: pid ${pid1} .*接続から \\d+ 分`));
    release(); await held;
    assert.deepEqual(await lockHolders(dbName), []);
    const list2 = runCli(['--url', urlOf(dbName), '--dir', dir, '--list']);
    assert.match(list2.stdout, /持っている接続は無い/);
    const run2 = runCli(['--url', urlOf(dbName), '--dir', dir]);
    assert.equal(run2.status, 0, run2.stderr); assert.match(run2.stdout, /applied=1/); assert.match(run2.stdout, /lock company_db_migrate を外した/);
  });

  await t('L2 途中の SQL で失敗 → そのファイルは巻き戻り、ROLLBACK が終わってから unlock・接続は取引の外 → 次の runner がすぐ取れる', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE, '0002_bad.sql': 'create table app.u (x int);\nselect 1/0;\n', '0003_after.sql': 'create table app.v (x int);\n' });
    const c1 = await open(dbName), c2 = await open(dbName);
    const rec = recording(c1);
    await assert.rejects(migrateWithLock(rec, { dir, log: quiet }), (e) => e.code === 'MIGRATION_FAILED' && e.version === '0002' && /division by zero/.test(e.message) && !e.connectionDead);
    const iRollback = rec.logs.lastIndexOf('rollback');
    const iUnlock = rec.logs.findIndex((q) => /pg_advisory_unlock/.test(q));
    assert.ok(iRollback >= 0 && iUnlock > iRollback, `rollback (${iRollback}) の後に unlock (${iUnlock})`);
    assert.deepEqual(await lockHolders(dbName), [], 'lock が残った');
    // c1 はつながったまま = lock が外れたのは unlock のおかげ (接続が切れたからではない)
    const st = (await su.query('select state from pg_stat_activity where pid = $1', [await pidOf(c1)])).rows[0].state;
    assert.equal(st, 'idle', '取引が開いたまま');
    assert.equal((await c1.query(`select to_regclass('app.u') is null as gone`)).rows[0].gone, true);
    assert.deepEqual(await versions(c1), ['0001']);
    await withMigrateLock(pgAdapter(c2), async () => { assert.deepEqual(await lockHolders(dbName), [await pidOf(c2)]); });
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('L2b 流す前の検査で止まっても (concurrent-index の許さない文) lock は外れる', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE, '0002_x.sql': '-- migrate:concurrent-index\ncreate table app.evil (x int);\n' });
    const c1 = await open(dbName);
    await assert.rejects(migrate(c1, { dir }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED');
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('L2c ROLLBACK も失敗 (接続が死んだ) → unlock は呼ばない (connectionDead)・接続を捨てれば外れる', async () => {
    const dbName = await newDb();
    const dir = mkDir({ '0001_base.sql': BASE, '0002_bad.sql': 'select 1/0;\n' });
    const c1 = await open(dbName), c2 = await open(dbName);
    const rec = recording(c1, { failRollback: true });
    await assert.rejects(migrateWithLock(rec, { dir, log: quiet }), (e) => e.code === 'MIGRATION_FAILED' && e.connectionDead === true);
    assert.equal(rec.logs.some((q) => /pg_advisory_unlock/.test(q)), false, 'ROLLBACK が失敗したのに unlock を呼んだ');
    assert.equal((await lockHolders(dbName)).length, 1, '(試験の前提) まだ持っている');
    c1.connection.stream.destroy();
    await waitFor(async () => (await lockHolders(dbName)).length === 0, 10000, '接続を捨てて外れる');
    await withMigrateLock(pgAdapter(c2), async () => {});
  });

  await t('L3 接続が切れたら lock が外れる (backend を terminate / client の socket を切る)', async () => {
    const dbName = await newDb();
    const c3 = await open(dbName), c4 = await open(dbName), c5 = await open(dbName);
    withMigrateLock(pgAdapter(c3), () => new Promise(() => {})).catch(() => {});
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, 'lock');
    await assert.rejects(withMigrateLock(pgAdapter(c5), async () => {}), (e) => e.code === 'MIGRATE_LOCKED');
    await su.query('select pg_terminate_backend($1)', [(await lockHolders(dbName))[0]]);
    await waitFor(async () => (await lockHolders(dbName)).length === 0, 10000, 'terminate で外れる');
    await withMigrateLock(pgAdapter(c5), async () => {});
    withMigrateLock(pgAdapter(c4), () => new Promise(() => {})).catch(() => {});
    await waitFor(async () => (await lockHolders(dbName)).length > 0, 10000, 'lock (2)');
    c4.connection.stream.destroy();
    await waitFor(async () => (await lockHolders(dbName)).length === 0, 10000, 'socket を切って外れる');
    await withMigrateLock(pgAdapter(c5), async () => {});
  });

  await t('L4 本物の migrations (0001〜) を全部流す = 今までどおり (2 回目は 0 本)・applied_by に migrate-v2・lock は残らない', async () => {
    const dbName = await newDb();
    const c1 = await open(dbName);
    const all = listMigrationFiles().map((f) => f.version);
    const r = await migrate(c1, {});
    assert.deepEqual(r.applied, all);
    const r2 = await migrate(c1, {});
    assert.deepEqual([r2.applied, r2.skipped.length], [[], all.length]);
    assert.deepEqual(await lockHolders(dbName), []);
    const by = (await c1.query('select distinct applied_by from ops.schema_migrations')).rows.map((x) => x.applied_by);
    assert.equal(by.length, 1); assert.match(by[0], / migrate-v2$/);
  });

  await t('L5 applyMigrations() は lock を取らない / dry-run は DDL を流さない (記録表も作らない)・try の lock を取って外す', async () => {
    const dbName = await newDb();
    const c1 = await open(dbName);
    const rec = recording(c1);
    await applyMigrations(rec, { dir: mkDir({ '0001_base.sql': BASE }), log: quiet });
    assert.equal(rec.logs.some((q) => /advisory/.test(q)), false, 'applyMigrations が lock の SQL を流した');
    const db2 = await newDb(); const c2 = await open(db2);
    const rec2 = recording(c2);
    const dry = await migrateWithLock(rec2, { dir: ciDir(), dryRun: true, log: quiet });
    assert.deepEqual(dry.pending, ['0001', '0002']);
    assert.equal((await c2.query(`select to_regclass('ops.schema_migrations') is null as none`)).rows[0].none, true, 'dry-run が記録表を作った');
    assert.ok(rec2.logs.some((q) => /pg_try_advisory_lock/.test(q)) && rec2.logs.some((q) => /pg_advisory_unlock/.test(q)));
    assert.deepEqual(await lockHolders(db2), []);
  });

  console.log('concurrent-index の migration');
  await t('A1 属性の比べ方: 空白・大文字が違っても同じ / 部分 index の条件・演算子のクラス・並びが違えば違う', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await migrate(c, { dir: mkDir({ '0001_base.sql': BASE }) });
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

  await t('C1 正常: 取引の外で流し、valid と属性の一致の後にだけ記録・容量を出す / もう一度流しても何もしない', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    const logs = [];
    const r = await migrate(c, { dir: ciDir(), log: (m) => logs.push(m) });
    assert.deepEqual(r.applied, ['0001', '0002']);
    for (const k of ['t_a_b_idx', 't_lower_a_uidx']) {
      const a = await readIndexAttrs(pgAdapter(c), 'app', k);
      assert.deepEqual([a.valid, a.ready, a.live], [true, true, true]);
      assert.deepEqual(attrDiff(a.attrs, EXPECT.indexes[`app.${k}`]), []);
    }
    assert.equal(logs.filter((m) => /容量: app\.t_.* の予想 .* × 3 \+ .* \/ 空き /.test(m)).length, 2, '各文の前に容量を出していない');
    assert.deepEqual(await versions(c), ['0001', '0002']);
    assert.equal((await c.query('show lock_timeout')).rows[0].lock_timeout, '0');
    assert.equal((await c.query('show statement_timeout')).rows[0].statement_timeout, '0');
    const r2 = await migrate(c, { dir: ciDir() });
    assert.deepEqual(r2.applied, []);
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('C1b lock を持たずに (applyMigrations を直に) concurrent-index を流す → MIGRATE_LOCK_REQUIRED・何も作らない', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await assert.rejects(applyMigrations(pgAdapter(c), { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK }), (e) => e.code === 'MIGRATE_LOCK_REQUIRED');
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null);
    assert.deepEqual(await versions(c), ['0001']);
  });

  await t('C2 許さない文が混じる → 流す前に止まる・何もしない (前の番号のふつうの migration も流さない)', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    const bad = `-- migrate:concurrent-index\ncreate index concurrently if not exists t_a_b_idx on app.t (a);\ncreate table app.evil (x int);\n`;
    const dir = mkDir({ '0001_base.sql': BASE, '0002_bad.sql': bad, '0002_bad.expect.json': JSON.stringify({ format: 'company-db-index-expect/1', pg_major: 18, indexes: { 'app.t_a_b_idx': EXPECT.indexes['app.t_a_b_idx'] } }) });
    await assert.rejects(migrate(c, { dir }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED' && /許す文は/.test(e.message));
    assert.deepEqual(await versions(c), []);
    assert.equal((await c.query(`select count(*)::int as n from pg_namespace where nspname = 'app'`)).rows[0].n, 0, '0001 を流した');
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
      ['create table', 'create table app.x (a int)'],
      ['空の列', 'create index concurrently if not exists x on app.t ()'],
    ];
    for (const [what, sql] of reject) {
      assert.throws(() => parseConcurrentIndexStatement(splitSqlStatements(sql)[0], 'f.sql'), (e) => e.code === 'CONCURRENT_INDEX_REJECTED', what);
    }
    // 許す形 (コメント・文字列の中の ; と 'concurrently' は数えない)・列か式か・concurrently を外した形
    const okSql = `create unique index concurrently if not exists x on only app.t using btree (lower(a), (b + 1) desc nulls last, c) include (id) where a <> 'p;q' /* ; */ and b > 0 -- ;\n`;
    const p = parseConcurrentIndexStatement(splitSqlStatements(okSql)[0], 'f.sql');
    assert.deepEqual([p.kind, p.unique, p.schema, p.table, p.name], ['create', true, 'app', 't', 'x']);
    assert.deepEqual(p.elements, [{ column: null, expression: true }, { column: null, expression: true }, { column: 'c', expression: false }, { column: 'id', expression: false }]);
    assert.match(p.sqlTx, /^create unique index +if not exists x on only app\.t/);
    assert.equal(/concurrently/i.test(p.sqlTx), false);
    assert.equal(splitSqlStatements(okSql).length, 1);
    // expect.json が無い・余分 → 止まる
    const noExpect = mkDir({ '0001_base.sql': BASE, '0002_idx.sql': CI_SQL });
    await assert.rejects(migrate(c, { dir: noExpect }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED' && /expect\.json が無い/.test(e.message));
    const f2 = { version: '0002', name: 'idx', file: '0002_idx.sql', text: CI_SQL, concurrentIndex: true };
    const d3 = mkDir({ '0002_idx.expect.json': JSON.stringify({ ...EXPECT, indexes: { ...EXPECT.indexes, 'app.other': EXPECT.indexes['app.t_a_b_idx'] } }) });
    assert.throws(() => planConcurrentIndexFile(f2, d3), /app\.other はこのファイルで作らない/);
    // 印の無いファイルに concurrently = 止まる (取引の中に入る前)
    const unmarked = mkDir({ '0001_base.sql': BASE, '0002_u.sql': '-- index\ncreate index concurrently if not exists t_a_idx on app.t (a);\n' });
    await assert.rejects(migrate(c, { dir: unmarked }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED' && /1 行目が/.test(e.message));
    const marker2 = mkDir({ '0001_base.sql': BASE, '0002_u.sql': '-- a\n-- migrate:concurrent-index\ncreate index concurrently if not exists t_a_idx on app.t (a);\n' });
    await assert.rejects(migrate(c, { dir: marker2 }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED');
    // dry-run でも同じ検査
    await assert.rejects(migrate(c, { dir, dryRun: true }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED');
    assert.deepEqual(await versions(c), []);
  });

  await t('C3 lock_timeout で invalid が残る → 記録しない・回収の手順を出す → 長い取引が終わってから流すと作り直して記録', async () => {
    const dbName = await newDb();
    const c = await open(dbName), blocker = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await blocker.query('begin');
    await blocker.query(`insert into app.t values (100000, 'y100000', 1, null)`);   // RowExclusive の取引を開いたまま = CIC は書き手を待つ
    const rec = recording(c);
    let err = null;
    try { await migrateWithLock(rec, { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK, concurrentIndexSettings: { lockTimeout: '1s' } }); } catch (e) { err = e; }
    assert.ok(err, '止まらなかった');
    assert.equal(err.code, 'MIGRATION_FAILED'); assert.match(err.message, /lock timeout/); assert.match(err.message, /回収の手順/);
    assert.deepEqual(err.invalidIndexes, ['app.t_a_b_idx']);
    // 取引の外の失敗 = ROLLBACK をせずに unlock
    const afterFail = rec.logs.slice(rec.logs.findIndex((q) => /create index concurrently if not exists t_a_b_idx/.test(q)));
    assert.equal(afterFail.includes('rollback'), false, '取引の外の失敗で ROLLBACK を流した');
    assert.ok(afterFail.some((q) => /pg_advisory_unlock/.test(q)));
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).valid, false);
    assert.deepEqual(await versions(c), ['0001']);
    assert.deepEqual(await lockHolders(dbName), []);
    assert.equal((await c.query('show lock_timeout')).rows[0].lock_timeout, '0', 'session の設定が戻っていない');
    await blocker.query('commit');
    const logs = [];
    const r = await migrate(c, { dir: ciDir(), log: (m) => logs.push(m) });
    assert.deepEqual(r.applied, ['0002']);
    assert.ok(logs.some((m) => /app\.t_a_b_idx は invalid .* drop index concurrently してから作り直す/.test(m)), logs.join('\n'));
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).valid, true);
    assert.equal((await c.query(`select count(*)::int as n from pg_index where not indisvalid`)).rows[0].n, 0);
  });

  await t('C4 同じ名前で定義が違う index がある → 止まる (今の index に触らない・記録しない)', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await c.query('create index t_a_b_idx on app.t (a)');
    const before = (await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).attrs;
    await assert.rejects(migrate(c, { dir: ciDir() }), (e) => e.code === 'MIGRATION_FAILED' && e.reason === 'DEFINITION_MISMATCH' && /同じ名前で定義が違う/.test(e.message));
    assert.deepEqual((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).attrs, before);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_lower_a_uidx'), null, '後ろの文を流した');
    assert.deepEqual(await versions(c), ['0001']);
    const db2 = await newDb(); const c2 = await open(db2);
    await migrate(c2, { dir: ciDir(), to: '0001' });
    await c2.query('create table app.t_a_b_idx (x int)');
    await assert.rejects(migrate(c2, { dir: ciDir() }), (e) => e.reason === 'NAME_TAKEN');
  });

  await t('C5 作り済みの valid (空白・大文字が違う SQL で作った) は飛ばして続きを作り、記録する', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await c.query('CREATE INDEX t_a_b_idx ON app.t (A, B DESC) INCLUDE (C) WHERE (B > 10)');
    const logs = [];
    const r = await migrate(c, { dir: ciDir(), log: (m) => logs.push(m) });
    assert.deepEqual(r.applied, ['0002']);
    assert.ok(logs.some((m) => /app\.t_a_b_idx は作り済み .* 飛ばす/.test(m)));
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_lower_a_uidx')).valid, true);
  });

  await t('C6 容量: 読めない・空きが 予想 × 3 + 2GB に満たない・ANALYZE していない → 流さない / CLI は RENDER_API_KEY が無ければ流さない (dry-run は流す予定を出す)', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await assert.rejects(migrate(c, { dir: ciDir(), readDiskMetrics: NO_METRICS }), (e) => e.code === 'DISK_CHECK_FAILED' && /空き容量が読めない \(METRICS_CONFIG/.test(e.message));
    await assert.rejects(migrate(c, { dir: ciDir(), readDiskMetrics: async () => { throw new Error('網'); } }), (e) => e.code === 'DISK_CHECK_FAILED' && e.reason === 'METRICS_INTERNAL');
    await assert.rejects(migrate(c, { dir: ciDir(), readDiskMetrics: undefined }), (e) => e.code === 'DISK_CHECK_FAILED');
    // 予想 = reltuples × (列の avg_width の和 + 式 64 + 16) × 1.3
    const st = parseConcurrentIndexStatement(splitSqlStatements(CI_STMTS[0])[0]);
    const est = await estimateIndexBytes(pgAdapter(c), st);
    const w = Object.fromEntries((await c.query(`select attname::text as a, avg_width from pg_stats where schemaname = 'app' and tablename = 't'`)).rows.map((r) => [r.a, Number(r.avg_width)]));
    const tuples = Number((await c.query(`select reltuples::float8 as r from pg_class where oid = 'app.t'::regclass`)).rows[0].r);
    assert.equal(est, Math.ceil(tuples * (w.a + w.b + w.c + 16) * 1.3));
    const st2 = parseConcurrentIndexStatement(splitSqlStatements(CI_STMTS[1])[0]);
    const est2 = await estimateIndexBytes(pgAdapter(c), st2);
    assert.equal(est2, Math.ceil(tuples * (64 + 16) * 1.3));
    // 🆕 Codex R1 H3: file の 2 つの文の合計 (予想 × 3 の和 + 2GB) で先に 1 回判定 = 1 文ずつなら足りる空きでも止まる (何も作らない)
    const need = est * DISK_ESTIMATE.safetyFactor + DISK_ESTIMATE.fixedReserveBytes;
    const needTotal = (est + est2) * DISK_ESTIMATE.safetyFactor + DISK_ESTIMATE.fixedReserveBytes;
    for (const free of [need, needTotal - 1]) {
      await assert.rejects(migrate(c, { dir: ciDir(), readDiskMetrics: disk(10 * GB, 10 * GB - free) }), (e) => e.code === 'DISK_CHECK_FAILED' && e.reason === 'NOT_ENOUGH' && e.needBytes === needTotal && /合計/.test(e.message));
      assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null, '止まったのに作った');
    }
    assert.deepEqual(await versions(c), ['0001']);
    // ちょうど足りる (空き = 合計) → 流す (2 つ目の文の前の関門 = 予想 × 3 + 1 つ目の予想 × 3 + 2GB = 合計 とちょうど同じ)
    const logsOk = [];
    const r = await migrate(c, { dir: ciDir(), readDiskMetrics: disk(10 * GB, 10 * GB - needTotal), log: (m) => logsOk.push(m) });
    assert.deepEqual(r.applied, ['0002']);
    assert.ok(logsOk.some((m) => /この回に先に作った分/.test(m)), logsOk.join('\n'));
    // ANALYZE していない表 (reltuples = -1) → 見積もれない = 流さない
    const db2 = await newDb(); const c2 = await open(db2);
    const noAnalyze = BASE.replace('analyze app.t;\n', '');
    await migrate(c2, { dir: mkDir({ '0001_base.sql': noAnalyze }) });
    await assert.rejects(migrate(c2, { dir: mkDir({ '0001_base.sql': noAnalyze, '0002_idx.sql': CI_SQL, '0002_idx.expect.json': JSON.stringify(EXPECT) }) }), (e) => e.code === 'DISK_CHECK_FAILED' && /ANALYZE/.test(e.message));
    // CLI: RENDER_API_KEY / CDB_RENDER_PG_RESOURCE_ID が無い = 流さない・dry-run は容量を見ない
    const db3 = await newDb();
    const cliDry = runCli(['--url', urlOf(db3), '--dir', ciDir(), '--dry-run']);
    assert.equal(cliDry.status, 0, cliDry.stderr); assert.match(cliDry.stdout, /dry-run: 0002_idx\.sql を流す予定 \(concurrent-index・2 文・取引の外\)/);
    const cli = runCli(['--url', urlOf(db3), '--dir', ciDir()]);
    assert.equal(cli.status, 1); assert.match(cli.stderr, /DISK_CHECK_FAILED.*空き容量が読めない \(METRICS_CONFIG/);
    const c3 = await open(db3);
    assert.deepEqual(await versions(c3), ['0001']);
    assert.deepEqual(await lockHolders(db3), []);
  });

  await t('C7 PostgreSQL の major が期待と違う → 流さない', async () => {
    const dbName = await newDb();
    const c = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await assert.rejects(migrate(c, { dir: ciDir(), expectedPgMajor: 17 }), (e) => e.code === 'PG_MAJOR_MISMATCH');
    const d = ciDir({ '0002_idx.expect.json': JSON.stringify({ ...EXPECT, pg_major: 17 }) });
    await assert.rejects(migrate(c, { dir: d }), (e) => e.code === 'PG_MAJOR_MISMATCH' && /expect\.json の pg_major/.test(e.message));
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null);
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('C8 同じ表の index の作りが別の接続で動いている → 止まる (何も作らない)', async () => {
    const dbName = await newDb();
    const c = await open(dbName), blocker = await open(dbName), other = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await blocker.query('begin');
    await blocker.query(`insert into app.t values (100001, 'y100001', 1, null)`);
    const otherPid = await pidOf(other);
    const building = other.query('create index concurrently other_idx on app.t (b)').catch((e) => e);
    await waitFor(async () => (await su.query('select count(*)::int as n from pg_stat_progress_create_index where pid = $1', [otherPid])).rows[0].n > 0, 10000, '別の作りが始まる');
    await assert.rejects(migrate(c, { dir: ciDir() }), (e) => e.code === 'MIGRATION_FAILED' && e.reason === 'BUILD_IN_PROGRESS' && new RegExp(`pid ${otherPid}`).test(e.message));
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
    const r = await migrate(c, { dir: d });
    assert.deepEqual(r.applied, ['0001', '0002', '0003']);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_lower_a_uidx'), null);
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).valid, true);
  });

  await t('C10 (Codex R1 H3) 6 本を続けて作る: 1 本ずつなら足りるが合計で 2GB の予約を割る空き → file の合計で先に止まる (何も作らない) / 古いメトリクス (作った分が使用に入らない) でも、この回に作った分を引いて途中で止まる', async () => {
    const SIX = '-- migrate:concurrent-index\n' + [
      'create index concurrently if not exists t6_a_idx on app.t (a)',
      'create index concurrently if not exists t6_b_idx on app.t (b)',
      'create index concurrently if not exists t6_c_idx on app.t (c)',
      'create index concurrently if not exists t6_ab_idx on app.t (a, b)',
      'create index concurrently if not exists t6_bc_idx on app.t (b, c)',
      'create index concurrently if not exists t6_lower_idx on app.t (lower(a))',
    ].map((x) => x + ';\n').join('');
    // expect.json は使い捨ての DB で同じ文を流して作る (手で書かない)
    const dbE = await newDb(); const ce = await open(dbE);
    await migrate(ce, { dir: mkDir({ '0001_base.sql': BASE }) });
    await runStmts(ce, splitSqlStatements(SIX).map((x) => x.sql));
    const EXP6 = await buildIndexExpect(pgAdapter(ce), { version: '0002', name: 'six', file: '0002_six.sql', text: SIX, concurrentIndex: true });
    const dir6 = mkDir({ '0001_base.sql': BASE, '0002_six.sql': SIX, '0002_six.expect.json': JSON.stringify(EXP6) });
    const dbName = await newDb(); const c = await open(dbName);
    await migrate(c, { dir: dir6, to: '0001' });
    const sts = splitSqlStatements(SIX).map((x) => parseConcurrentIndexStatement(x));
    const ests = [];
    for (const st of sts) ests.push(await estimateIndexBytes(pgAdapter(c), st));
    const F = DISK_ESTIMATE.safetyFactor, R = DISK_ESTIMATE.fixedReserveBytes;
    const maxOne = Math.max(...ests) * F + R;                      // 1 本ずつの関門なら、この空きで 6 本とも通る (旧の runner)
    const total = ests.reduce((a, b) => a + b, 0) * F + R;
    assert.ok(ests.every((x) => x > 0) && maxOne < total);
    const made = async () => { let n = 0; for (const st of sts) { const a = await readIndexAttrs(pgAdapter(c), st.schema, st.name); if (a && a.valid) n++; } return n; };
    // ① 空きが一定 = maxOne → file の合計で止まる・何も作らない
    await assert.rejects(migrate(c, { dir: dir6, readDiskMetrics: disk(10 * GB, 10 * GB - maxOne) }),
      (e) => e.code === 'DISK_CHECK_FAILED' && e.reason === 'NOT_ENOUGH' && e.needBytes === total && /index 6 本の合計/.test(e.message));
    assert.equal(await made(), 0, '合計で止まったのに作った');
    assert.deepEqual(await versions(c), ['0001']);
    // ② 古いメトリクス: 最初の読み (合計の判定) だけ空きが多く、後は maxOne のまま (作った index が使用に入らない)
    //    → 先に作った分の予想 × 3 を引いて、和が空きを超える k 本目の前で止まる (旧の runner は 6 本とも作った)
    let k = 0, acc = 0;
    while (acc + ests[k] * F + R <= maxOne) { acc += ests[k] * F; k++; }
    assert.ok(k >= 1 && k < 6, `(試験の前提) k = ${k}`);
    let n = 0;
    const stale = async () => (n++ === 0 ? { ok: true, capacityBytes: 100 * GB, usedBytes: 0 } : { ok: true, capacityBytes: 10 * GB, usedBytes: 10 * GB - maxOne });
    let err = null;
    try { await migrate(c, { dir: dir6, readDiskMetrics: stale }); } catch (e) { err = e; }
    assert.ok(err && err.code === 'DISK_CHECK_FAILED' && err.reason === 'NOT_ENOUGH', String(err && err.message));
    assert.equal(await made(), k, `${k} 本だけ作って止まる`);
    assert.equal(err.reservedBytes, acc);
    assert.equal(err.needBytes, ests[k] * F + acc + R);
    assert.match(err.message, /この回に先に作った分/);
    assert.deepEqual(await versions(c), ['0001']);
    assert.deepEqual(await lockHolders(dbName), []);
    // ③ 🆕 Codex R-D60-v3-14 M4: 前の回が作った k 本 (valid・記録の前) を含めて合計する = 残りの 6 − k 本だけなら足りる空き (メトリクスがまだ k 本を含まない) でも止まる
    const restOnly = ests.slice(k).reduce((a, b) => a + b, 0) * F + R;
    await assert.rejects(migrate(c, { dir: dir6, readDiskMetrics: disk(10 * GB, 10 * GB - restOnly) }),
      (e) => e.code === 'DISK_CHECK_FAILED' && e.reason === 'NOT_ENOUGH' && e.needBytes === total && /index 6 本の合計/.test(e.message));
    assert.equal(await made(), k, '止まったのに作った');
    // 合計ちょうどの空き → 流す (飛ばす k 本も予約に入る = 各文の前の必要は合計を超えない)
    const logs3 = [];
    assert.deepEqual((await migrate(c, { dir: dir6, readDiskMetrics: disk(10 * GB, 10 * GB - total), log: (m) => logs3.push(m) })).applied, ['0002']);
    assert.equal(logs3.filter((m) => /は作り済み .* 飛ばす/.test(m)).length, k);
    assert.equal(await made(), 6);
  });

  await t('C11 (Codex R1 H2・設計 13 v3.13 ④) 容量を読む resource は接続先と同じでなければ流さない: host が違う → 要求を送らずに止まる / API の名札の databaseName が current_database() と違う・名札が読めない → 止まる (fail-closed・RESOURCE_MISMATCH)', async () => {
    const A = 'dpg-aaaaaaaaaaaaaaaaaaaa-a', B = 'dpg-bbbbbbbbbbbbbbbbbbbb-a';
    const ext = (id) => `postgres://u:p@${id}.singapore-postgres.render.com/company_db`;
    // host の対応の fixture (Codex R-D60-v3-14 M5) = internal・external・pool の関係を固定。Render に確かめるまで confirmed = false (定数と同じ)
    const HOSTS = JSON.parse(fs.readFileSync(path.join(ROOT, 'scripts', 'fixtures', 'render-postgres-hosts.json'), 'utf8'));
    assert.equal(RENDER_PG_HOST_MAPPING.confirmed, HOSTS.confirmed);
    assert.equal(HOSTS.confirmed, false, 'Render の回答の前に confirmed を true にしない');
    for (const k of HOSTS.cases) assert.equal(renderResourceMatchesUrl(HOSTS.resourceId, `postgres://u:p@${k.host}:5432/company_db`).ok, k.match, `${k.kind} ${k.host}`);
    assert.ok(HOSTS.cases.some((k) => /^pool/.test(k.kind) && k.match === false));
    assert.deepEqual(renderResourceMatchesUrl(A, ext(A)), { ok: true });
    assert.deepEqual(renderResourceMatchesUrl(A, `postgres://u:p@${A}:5432/company_db`), { ok: true });   // 内部の host
    for (const [id, url] of [[B, ext(A)], [A, ext(B)], ['dpg-aaaaaaaaaaaaaaaaaaaa', ext(A)], [A, `postgres://u:p@${A}x.singapore-postgres.render.com/x`], [A, `postgres://u:p@${A}.evil.example.com/x`],
      [A, `postgres://u:p@${A}.singapore-postgres.render.com.evil.example/x`], [A, 'postgres://u:p@127.0.0.1/x'], [A, null], [A, 'not a url'], ['', ext(A)]]) {
      assert.deepEqual(renderResourceMatchesUrl(id, url), { ok: false, reason: 'RESOURCE_MISMATCH' }, `${id} ${url}`);
    }
    // 読み手 (試験の fetch): /postgres/{ID} = 名札・/metrics/… = 401。password を返す connection-info は呼ばない
    const realFetch = globalThis.fetch;
    const calls = [];
    let identity = { id: B, databaseName: 'company_db', databaseUser: 'u' };
    let identityStatus = 200;
    globalThis.fetch = async (url) => {
      calls.push(String(url));
      if (/\/postgres\/[^/?]+$/.test(String(url).split('?')[0])) return new Response(JSON.stringify(identity), { status: identityStatus, headers: { 'content-type': 'application/json' } });
      return new Response('', { status: 401 });
    };
    try {
      const env = { RENDER_API_KEY: 'rnd_dummyKEY12345', CDB_RENDER_PG_RESOURCE_ID: B };
      // 🆕 M5: host の対応が未確認の間は、host が合っても要求を送らずに止まる (CLI の道)
      assert.deepEqual(await (await renderDiskMetricsReader(env, ext(B), { currentDatabase: 'company_db' }))(), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'HOST_MAPPING_UNCONFIRMED' });
      assert.deepEqual(calls, []);
      const src = fs.readFileSync(path.join(ROOT, 'scripts', 'company-db', 'migrate.mjs'), 'utf8');
      assert.equal(src.split('assumeHostMappingConfirmedForTest').length - 1, 2, '試験の印を CLI の道で渡していない (引数の定義と判定の 2 か所だけ)');
      // 以下は「確かめた後」の照合の道を試験の印で通す (CLI は渡さない)
      const read = async (url, currentDatabase) => (await renderDiskMetricsReader(env, url, { currentDatabase, assumeHostMappingConfirmedForTest: true }))();
      // host が違う・接続先を渡さない = 要求を送らない
      assert.deepEqual(await read(ext(A), 'company_db'), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'HOST' });
      assert.deepEqual(await read(undefined, 'company_db'), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'HOST' });
      assert.deepEqual(calls, [], '合わないのに Render に要求を送った');
      // host は合うが current_database() を渡さない = 照合できない
      assert.deepEqual(await read(ext(B), null), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'NO_CURRENT_DATABASE' });
      assert.deepEqual(calls, []);
      // 名札の DB の名前が違う
      assert.deepEqual(await read(ext(B), 'other_db'), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'DATABASE_NAME' });
      // 名札が読めない (401)・別の resource の名札・databaseName が無い
      identityStatus = 401;
      assert.deepEqual(await read(ext(B), 'company_db'), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'METRICS_AUTH' });
      identityStatus = 200; identity = { id: A, databaseName: 'company_db' };
      assert.deepEqual(await read(ext(B), 'company_db'), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'METRICS_WRONG_RESOURCE' });
      identity = { id: B };
      assert.deepEqual(await read(ext(B), 'company_db'), { ok: false, reason: 'RESOURCE_MISMATCH', detail: 'METRICS_SHAPE' });
      // 全部合う → メトリクスの要求に進む (試験の fetch は 401)
      identity = { id: B, databaseName: 'company_db', databaseUser: 'u' };
      calls.length = 0;
      assert.deepEqual(await read(ext(B), 'company_db'), { ok: false, reason: 'METRICS_AUTH' });
      assert.ok(calls.some((u) => u.includes(`/postgres/${B}`)) && calls.some((u) => u.includes('/metrics/')), calls.join(' '));
      assert.equal(calls.some((u) => /connection-info/.test(u)), false, 'connection-info (password) を呼んだ');
    } finally { globalThis.fetch = realFetch; }
    // CLI: 使い捨ての DB (127.0.0.1) に Render の resource id を設定して流す → 止まる・何も作らない
    const db3 = await newDb();
    const cli = runCli(['--url', urlOf(db3), '--dir', ciDir()], { RENDER_API_KEY: 'rnd_dummyKEY12345', CDB_RENDER_PG_RESOURCE_ID: A });
    assert.equal(cli.status, 1, cli.stdout + cli.stderr);
    assert.match(cli.stderr, /DISK_CHECK_FAILED.*RESOURCE_MISMATCH\/HOST/);
    const c3 = await open(db3);
    assert.deepEqual(await versions(c3), ['0001']);
    assert.equal(await readIndexAttrs(pgAdapter(c3), 'app', 't_a_b_idx'), null);
    assert.deepEqual(await lockHolders(db3), []);
  });

  await t('C12 (Codex R1 M2・設計 13 v3.13 ⑧) 属性に置き場 (reltablespace) と storage の設定 (reloptions): 既存の同じ名前の index が fillfactor つき・別の置き場 → 違う定義として止まる', async () => {
    // 設計 13 v3.13 ⑧ = 期待はいつも reltablespace 0 (DB の既定) と reloptions [] (null は空の配列・名前=値 を並べ替え)
    assert.equal(EXPECT.indexes['app.t_a_b_idx'].reltablespace, 0);
    assert.deepEqual(EXPECT.indexes['app.t_a_b_idx'].reloptions, []);
    const SAME = (opt) => `create index t_a_b_idx on app.t (a, b desc) include (c) ${opt} where b > 10`;
    const dbName = await newDb(); const c = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await c.query(SAME('with (fillfactor = 70)'));
    await assert.rejects(migrate(c, { dir: ciDir() }), (e) => e.reason === 'DEFINITION_MISMATCH' && e.diff.length === 1 && e.diff[0].startsWith('reloptions:') && /fillfactor=70/.test(e.diff[0]));
    assert.deepEqual(await versions(c), ['0001']);
    // 別の置き場 (superuser が作る tablespace)。🚨 Windows の server は ASCII でない path (ユーザーのフォルダの全角の名前) の tablespace を「無い」と言う = ASCII の場所に作る
    const tsBase = /^[\x20-\x7e]*$/.test(os.tmpdir()) ? os.tmpdir() : path.join(path.parse(os.tmpdir()).root, 'tmp');
    fs.mkdirSync(tsBase, { recursive: true });
    const tsDir = fs.mkdtempSync(path.join(tsBase, 'cdb-miglock-ts-'));
    tmpDirs.push(tsDir);
    const ts = `cdb_ts_${hex}`;
    await su.query(`create tablespace ${ts} location '${tsDir.replace(/\\/g, '/')}'`);
    const db2 = await newDb(); const c2 = await open(db2);
    try {
      await su.query(`grant create on tablespace ${ts} to ${OWNER}`);
      await migrate(c2, { dir: ciDir(), to: '0001' });
      await c2.query(SAME(`tablespace ${ts}`));
      await assert.rejects(migrate(c2, { dir: ciDir() }), (e) => e.reason === 'DEFINITION_MISMATCH' && e.diff.length === 1 && e.diff[0].startsWith('reltablespace:'));
      assert.deepEqual(await versions(c2), ['0001']);
    } finally {
      try { await c2.query('drop index if exists app.t_a_b_idx'); await su.query(`drop tablespace if exists ${ts}`); } catch { /* クラスタごと消える */ }
    }
  });

  await t('C13 (Codex R1 Low) session の設定の途中で落ちても (statement_timeout を server が拒む) 先に SET した lock_timeout が戻る・lock が外れる・何も作らない', async () => {
    const dbName = await newDb(); const c = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await assert.rejects(migrate(c, { dir: ciDir(), concurrentIndexSettings: { lockTimeout: '7s', statementTimeout: '99999999999h' } }),
      (e) => e.code === 'MIGRATION_FAILED' && e.noTransaction === true && /statement_timeout/.test(e.message));
    assert.equal((await c.query('show lock_timeout')).rows[0].lock_timeout, '0', 'SET の途中の失敗で lock_timeout が残った');
    assert.deepEqual(await lockHolders(dbName), []);
    assert.equal(await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx'), null);
    assert.deepEqual(await versions(c), ['0001']);
  });

  await t('C14 (Codex R1 Low) 同じ index の名前の操作が 2 つ (create を 2 回・drop と create) = 流す前に止まる (expect.json を作る道も)', async () => {
    const f = (text) => ({ version: '0002', name: 'd', file: '0002_d.sql', text, concurrentIndex: true });
    const two = '-- migrate:concurrent-index\ncreate index concurrently if not exists t_a_b_idx on app.t (a);\ncreate index concurrently if not exists t_a_b_idx on app.t (b);\n';
    const dropCreate = '-- migrate:concurrent-index\ndrop index concurrently if exists app.t_a_b_idx;\ncreate index concurrently if not exists t_a_b_idx on app.t (b);\n';
    const createDrop = '-- migrate:concurrent-index\ncreate index concurrently if not exists t_a_b_idx on app.t (b);\ndrop index concurrently if exists app.t_a_b_idx;\n';
    for (const text of [two, dropCreate, createDrop]) {
      assert.throws(() => planConcurrentIndexFile(f(text), mkDir({ '0002_d.expect.json': JSON.stringify(EXPECT) })), (e) => e.code === 'CONCURRENT_INDEX_REJECTED' && /操作が 2 つ以上/.test(e.message));
      await assert.rejects(buildIndexExpect(pgAdapter(cx), f(text)), /操作が 2 つ以上/);
    }
    const dbName = await newDb(); const c = await open(dbName);
    await assert.rejects(migrate(c, { dir: mkDir({ '0001_base.sql': BASE, '0002_d.sql': two, '0002_d.expect.json': JSON.stringify(EXPECT) }) }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED');
    assert.equal((await c.query(`select count(*)::int as n from pg_namespace where nspname = 'app'`)).rows[0].n, 0, '0001 を流した');
  });

  await t('C15 (設計 13 v3.13 ⑩) session の設定を戻せない (RESET が落ちる) → 接続を使い回さない (connectionDead)。成功の後 = SESSION_RESET_FAILED (記録は済み) / 失敗の後 = 元の誤りに connectionDead', async () => {
    const brokenReset = (c) => { const a = pgAdapter(c); return { supportsConcurrentIndex: true, query: (q, p) => a.query(q, p), exec: (q) => (q === 'reset lock_timeout' ? Promise.reject(new Error('Connection terminated (試験)')) : a.exec(q)) }; };
    const dbName = await newDb(); const c = await open(dbName);
    await migrate(c, { dir: ciDir(), to: '0001' });
    await assert.rejects(migrateWithLock(brokenReset(c), { dir: ciDir(), log: quiet, readDiskMetrics: BIG_DISK }),
      (e) => e.code === 'SESSION_RESET_FAILED' && e.connectionDead === true && e.resetFailed.includes('lock_timeout') && /使い回さない/.test(e.message));
    assert.deepEqual(await versions(c), ['0001', '0002'], '記録は済んでいる');
    const db2 = await newDb(); const c2 = await open(db2);
    await migrate(c2, { dir: ciDir(), to: '0001' });
    await assert.rejects(migrateWithLock(brokenReset(c2), { dir: ciDir(), log: quiet, readDiskMetrics: NO_METRICS }),
      (e) => e.code === 'DISK_CHECK_FAILED' && e.connectionDead === true && /も戻せない = この接続は捨てる/.test(e.message));
    assert.deepEqual(await versions(c2), ['0001']);
  });

  await t('P1 PGlite の adapter: concurrently を外してふつうの取引で流し、属性の検証は同じ (PG 18 の expect.json に一致) / 定義が違えば巻き戻す', async () => {
    const pg = new PGlite();
    try {
      const db = pgliteAdapter(pg);
      const logs = [];
      const r = await applyMigrations(db, { dir: ciDir({ '0003_drop.sql': '-- migrate:concurrent-index\ndrop index concurrently if exists app.t_lower_a_uidx;\n' }), log: (m) => logs.push(m) });
      assert.deepEqual(r.applied, ['0001', '0002', '0003']);
      assert.ok(logs.some((m) => /0002_idx\.sql \(concurrent-index を取引の中で/.test(m)));
      const a = await readIndexAttrs(db, 'app', 't_a_b_idx');
      assert.deepEqual(attrDiff(a.attrs, EXPECT.indexes['app.t_a_b_idx']), []);
      assert.equal(await readIndexAttrs(db, 'app', 't_lower_a_uidx'), null);
      // 定義が違う同じ名前 → 巻き戻す (記録しない)
      const pg2 = new PGlite();
      try {
        const db2 = pgliteAdapter(pg2);
        await applyMigrations(db2, { dir: ciDir(), to: '0001', log: quiet });
        await db2.exec('create index t_a_b_idx on app.t (a)');
        await assert.rejects(applyMigrations(db2, { dir: ciDir(), log: quiet }), (e) => e.code === 'MIGRATION_FAILED' && e.reason === 'DEFINITION_MISMATCH');
        assert.deepEqual((await db2.query('select version from ops.schema_migrations order by 1')).rows.map((x) => x.version), ['0001']);
        assert.equal(await readIndexAttrs(db2, 'app', 't_lower_a_uidx'), null, '巻き戻していない');
        // 許さない文は PGlite でも先に止まる
        const bad = mkDir({ '0001_base.sql': BASE, '0002_x.sql': '-- migrate:concurrent-index\ncreate table app.evil (x int);\n' });
        await assert.rejects(applyMigrations(db2, { dir: bad, log: quiet }), (e) => e.code === 'CONCURRENT_INDEX_REJECTED');
      } finally { await pg2.close(); }
    } finally { await pg.close(); }
  });

  console.log('持ち主の mode (Codex R-D60-v3-10 H2: ① PR 1b の前 / ② PR 1b 自身 / ③ PR 1b の後)');
  let roleSeq = 0;
  /** PR 1b の形の owner-transition の file (持ち主の役割を作り・表と記録表の持ち主を移し・印を作る)。makeMarker = false なら印を作らない (壊れた形) */
  // 🆕 設計 13 v3.14 ② = owner の状態の試験は形 B の deployer (DEPLOYER) で流す。DB の持ち主 = DEPLOYER・持ち主の役割は superuser が作って DEPLOYER に SET で付ける
  const newDepDb = async () => { const name = `cdb_mig_${hex}_${++dbSeq}`; await su.query(`create database ${name} owner ${DEPLOYER}`); return name; };
  const urlDep = (dbName) => { const x = new URL(urlOf(dbName)); x.username = DEPLOYER; return x.toString(); };
  const openDep = async (dbName) => { const c = await openPgClient(urlDep(dbName)); c.on('error', () => {}); clients.push(c); return c; };
  const newOwnerRole = async () => { const r = `cdb_owner_${hex}_${++roleSeq}`; await su.query(`create role ${r} nologin`); await su.query(`grant ${r} to ${DEPLOYER} with inherit false, set true`); return r; };
  const transitionSql = (dbName, role, { makeMarker = true, header = true, tail = '' } = {}) => `${header ? '-- migrate:owner-transition\n' : ''}-- PR 1b の試験の形
grant create on database ${dbName} to ${role};
grant usage, create on schema app, ops to ${role};
grant usage on schema ops to ${RUNTIME};
grant select on ops.schema_migrations to ${RUNTIME};   -- 夜のバックアップ (runtime) が読む
alter table app.t owner to ${role};
alter table ops.schema_migrations owner to ${role};
${makeMarker ? `create table ops.migrate_owner (singleton boolean primary key default true check (singleton), owner_role name not null, since timestamptz not null default now());
insert into ops.migrate_owner (owner_role) values ('${role}');
grant select on ops.migrate_owner to ${RUNTIME};
alter table ops.migrate_owner owner to ${role};` : ''}
alter schema app owner to ${role};
alter schema ops owner to ${role};
${tail}`;
  /** 持ち主 (catalog だけ = schema の USAGE が無くても読める) */
  const ownerOf = async (c, rel) => { const [sc, nm] = rel.split('.'); return (await c.query('select pg_get_userbyid(c.relowner)::text as o from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 and c.relname = $2', [sc, nm])).rows[0].o; };
  const whoami = async (c) => (await c.query('select current_user::text as cu, session_user::text as su')).rows[0];

  await t('O0 migration の file は全部、許さない役割の切り替えを持たない (grep の縛り) / 拒む形 = RESET ROLE・session の SET ROLE・SET SESSION ROLE・ROLE NONE・SESSION AUTHORIZATION・set_config(role)・DISCARD・一覧の外の SET LOCAL ROLE / 許す = 一覧の SET LOCAL ROLE (設計 13 v3.11 ③)', async () => {
    for (const f of listMigrationFiles()) assert.deepEqual(roleSwitchStatements(f.text), [], f.file);
    assert.deepEqual(ALLOWED_SET_LOCAL_ROLES, ['cdb_owner', 'profit_definer', 'heavy_guard_definer', 'heavy_read_definer', 'd60_calib_definer', 'finance_revision_definer']);
    assert.deepEqual(roleSwitchStatements(["-- reset role", "select 'set role x'; /* set local role y */ select 1;"].join(String.fromCharCode(10))), []);
    // 🆕 Codex R-D60-v3-13 H1: ドルの引用の中は (データの文字列でも) 見る = 拒む向き
    assert.ok(roleSwitchStatements('select $$reset role$$;').length > 0);
    const rejected = ['reset role', 'RESET ROLE;', 'SET ROLE cdb_owner', 'set session role cdb_owner', 'set local role none', 'SET LOCAL ROLE NONE;', 'set local role watcher', 'SET LOCAL ROLE postgres',
      'set session authorization default', 'reset session authorization', "select set_config('role', 'x', true)", "SELECT SET_CONFIG('role', 'none', true)", 'discard all', 'DISCARD PLANS',
      // 🆕 Codex R1 H1: 引用の名前・大文字小文字・空白・コメントを挟む形
      'RESET "role";', 'reset "ROLE"', 'RESET /* x */ "Role"', "SET LOCAL \"role\" = 'none';", 'set local "role" to cdb_owner', "set\n  local -- c\n \"ROLE\" = 'none'", 'SET "role" cdb_owner', 'set session "role" cdb_owner',
      "SELECT \"set_config\"('role', 'none', true);", "select pg_catalog.\"set_config\"( 'role' , 'none', true)", "select \"SET_CONFIG\" /* c */ ('ROLE', 'none', true)",
      "select set_config(U&'role', 'none', true)", "select set_config('ro' || 'le', 'none', true)", "select set_config(E'role', 'none', true)", '"reset" role', 'reset "session" "authorization"',
      // 🆕 Codex R-D60-v3-13 H1: ドルの引用の中 (DO の本文・関数の本文・入れ子) も見る
      "do $$ begin perform set_config('role', 'none', true); end $$", 'DO $x$ BEGIN RESET ROLE; END $x$', "create function app.f() returns void language plpgsql as $f$ begin set local role postgres; end $f$",
      'do $a$ begin execute $b$ reset "role" $b$; end $a$', "do $$ begin perform pg_catalog.set_config(v_name, 'none', true); end $$", 'do $$ begin execute \'x\'; reset role; end $$',
      'do $$ begin raise notice $q$ unclosed; end $$'];
    for (const x of rejected) assert.ok(roleSwitchStatements(x).length > 0, `拒まない: ${x}`);
    const allowed = ['set local role cdb_owner', 'SET LOCAL ROLE profit_definer;', 'set local role heavy_guard_definer', 'set local role heavy_read_definer', 'set local role d60_calib_definer', 'set local role finance_revision_definer',
      'SET LOCAL ROLE "profit_definer"', "set local role 'cdb_owner'", 'reset all', "select set_config('search_path', 'pg_catalog', true)",
      // DO の本文の許す形 (今の migration の書き方)・動的 SQL の文字列の中は見ない (= 補助で sandbox ではない・runner は ⚠️ を出す)
      "do $$ begin perform set_config('app.x', 'y', true); execute format('grant select on %I to x', 't'); end $$", 'do $$ begin set local role cdb_owner; end $$',
      "do $$ begin execute format('select set_config(%L, %L, true)', 'role', 'none'); end $$",
      "select \"set_config\"('search_path', 'pg_catalog', true)", 'select "role" from app.t', "comment on column app.t.a is 'reset role'"];
    for (const x of allowed) assert.deepEqual(roleSwitchStatements(x), [], `拒んだ: ${x}`);
  });

  await t('O1 ① PR 1b の前 (印が無い) = legacy = 接続の役割のまま流す (今までどおり)・記録表の持ち主は接続の役割', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const logs = [];
    await migrate(c, { dir: ciDir(), log: (m) => logs.push(m) });
    assert.ok(logs.some((m) => /持ち主の mode = legacy/.test(m)));
    assert.equal(await ownerOf(c, 'ops.schema_migrations'), DEPLOYER);
    assert.equal(await ownerOf(c, 'app.t_a_b_idx'), DEPLOYER);
  });

  await t('O2 ② PR 1b 自身 (owner-transition) = 接続の役割で移し、記録は印の役割で / 同じ回の後の file (ふつう・CIC) は owner mode', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const dir = ciDir({ '0002_idx.sql': transitionSql(dbName, role), '0003_after.sql': 'create table app.after (x int);\n', '0004_idx.sql': CI_SQL, '0004_idx.expect.json': JSON.stringify(EXPECT) });
    fs.rmSync(path.join(dir, '0002_idx.expect.json'));
    const logs = [];
    const r = await migrate(c, { dir, log: (m) => logs.push(m) });
    assert.deepEqual(r.applied, ['0001', '0002', '0003', '0004']);
    assert.ok(logs.some((m) => /持ち主の mode = legacy/.test(m)) && logs.some((m) => new RegExp(`owner \\(SET ROLE ${role}\\) に移った`).test(m)), logs.join('\n'));
    assert.equal(await ownerOf(c, 'ops.schema_migrations'), role);
    assert.equal(await ownerOf(c, 'app.after'), role, '0003 が持ち主の役割で流れていない');
    assert.equal((await readIndexAttrs(pgAdapter(c), 'app', 't_a_b_idx')).valid, true);
    assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER }, 'SET ROLE が残った');
    assert.deepEqual(await lockHolders(dbName), []);
    // 接続の役割はもう記録表に書けない = SET ROLE が要る (③ の前提)
    await assert.rejects(c.query(`insert into ops.schema_migrations (version, name, checksum) values ('9999', 'x', 'x')`), (e) => e.code === '42501');
    // 記録は印の役割で読める (--list も)
    // 夜のバックアップ (runtime) は記録表と印の表を読める (持ち主を移した後も GRANT が残る)
    const rtUrl = new URL(urlDep(dbName)); rtUrl.username = RUNTIME;
    const rt = await openPgClient(rtUrl.toString()); rt.on('error', () => {}); clients.push(rt);
    assert.equal((await rt.query('select count(*)::int as n from ops.schema_migrations')).rows[0].n, 4);
    assert.equal((await rt.query('select owner_role::text as r from ops.migrate_owner')).rows[0].r, role);
    const list = runCli(['--url', urlDep(dbName), '--dir', dir, '--list']);
    assert.equal(list.status, 0, list.stderr);
    assert.match(list.stdout, /0004 applied/); assert.match(list.stdout, new RegExp(`持ち主の mode = owner \\(SET ROLE ${role}\\)`));
  });

  await t('O3 ③ PR 1b の後 = 印の役割で流す (ふつう・CIC・記録表)・失敗しても役割が戻り lock が外れる / SET できない・印が壊れている → 止まる', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const base = { '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role) };
    await migrate(c, { dir: mkDir(base) });
    // ふつうの file と CIC の file (別の回 = 印を読んで owner mode から始まる)
    const dir2 = mkDir({ ...base, '0003_more.sql': 'create table app.more (x int);\n', '0004_idx.sql': CI_SQL, '0004_idx.expect.json': JSON.stringify(EXPECT) });
    const logs = [];
    const r = await migrate(c, { dir: dir2, log: (m) => logs.push(m) });
    assert.deepEqual(r.applied, ['0003', '0004']);
    assert.ok(logs.some((m) => new RegExp(`持ち主の mode = owner \\(SET ROLE ${role}\\)`).test(m)));
    assert.equal(await ownerOf(c, 'app.more'), role);
    assert.equal((await c.query(`select applied_by from ops.schema_migrations where version = '0004'`).catch((e) => e)).code, '42501', '(前提) 接続の役割では読めない');
    assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER });
    // ふつうの file が落ちる → 巻き戻り・役割が戻る・lock が外れる
    const dir3 = mkDir({ ...base, '0003_more.sql': 'create table app.more (x int);\n', '0004_idx.sql': CI_SQL, '0004_idx.expect.json': JSON.stringify(EXPECT), '0005_bad.sql': 'create table app.bad (x int);\nselect 1/0;\n' });
    await assert.rejects(migrate(c, { dir: dir3 }), (e) => e.code === 'MIGRATION_FAILED' && e.version === '0005');
    assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER });
    assert.deepEqual(await lockHolders(dbName), []);
    // CIC が落ちる (容量が読めない) → RESET ROLE・lock が外れる
    const dir4 = mkDir({ ...base, '0003_more.sql': 'create table app.more (x int);\n', '0004_idx.sql': CI_SQL, '0004_idx.expect.json': JSON.stringify(EXPECT), '0005_idx2.sql': '-- migrate:concurrent-index\ncreate index concurrently if not exists t_c_idx on app.t (c);\n',
      '0005_idx2.expect.json': JSON.stringify({ format: 'company-db-index-expect/1', pg_major: 18, indexes: { 'app.t_c_idx': { table: 'app.t' } } }) });
    await assert.rejects(migrate(c, { dir: dir4, readDiskMetrics: NO_METRICS }), (e) => e.code === 'DISK_CHECK_FAILED');
    assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER }, 'CIC の失敗で SET ROLE が残った');
    assert.deepEqual(await lockHolders(dbName), []);
    // owner mode の file に RESET ROLE / SET ROLE = 流す前に止まる (何も流さない)
    for (const body of [`create table app.r1 (x int);
reset role;
create table app.r2 (x int);
`, `set local role postgres;
`, `SET SESSION AUTHORIZATION DEFAULT;
`]) {
      const d = mkDir({ ...base, '0003_more.sql': `create table app.more (x int);
`, '0004_idx.sql': CI_SQL, '0004_idx.expect.json': JSON.stringify(EXPECT), '0005_role.sql': body });
      await assert.rejects(migrate(c, { dir: d }), (e) => e.code === 'OWNER_MODE_INVALID' && /許さない役割の切り替え/.test(e.message), body);
    }
    assert.equal((await c.query(`select count(*)::int as n from pg_class where relname = 'r1'`)).rows[0].n, 0);
    // owner-transition をもう一度 = 流さない (前の検査で止まる)
    const dir5 = mkDir({ ...base, '0003_more.sql': 'create table app.more (x int);\n', '0004_idx.sql': CI_SQL, '0004_idx.expect.json': JSON.stringify(EXPECT), '0005_again.sql': transitionSql(dbName, role + 'x') });
    await assert.rejects(migrate(c, { dir: dir5 }), (e) => e.code === 'OWNER_MODE_INVALID' && /2 つある|もう owner mode/.test(e.message));
    // 接続の役割が印の役割に SET できない → 止まる (何も流さない)
    await su.query(`grant ${role} to ${DEPLOYER} with set false`);   // 同じ付与 (superuser が付けた) の SET を外す
    await assert.rejects(migrate(c, { dir: dir2 }), (e) => e.code === 'OWNER_MODE_INVALID' && /SET ROLE できない/.test(e.message));
    assert.deepEqual(await lockHolders(dbName), []);
    await su.query(`grant ${role} to ${DEPLOYER} with set true`);
    assert.deepEqual((await migrate(c, { dir: dir2 })).applied, []);
  });

  await t('O4 印を作ってよいのは owner-transition の file だけ / owner-transition が印を作らない → 巻き戻す', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    await migrate(c, { dir: mkDir({ '0001_base.sql': BASE }) });
    const r1 = await newOwnerRole();
    await assert.rejects(migrate(c, { dir: mkDir({ '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, r1, { header: false }) }) }), (e) => e.code === 'MIGRATION_FAILED' && e.reason === 'OWNER_MODE_INVALID' && /owner-transition/.test(e.message));
    const r2 = await newOwnerRole();
    await assert.rejects(migrate(c, { dir: mkDir({ '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, r2, { makeMarker: false }) }) }), (e) => e.code === 'MIGRATION_FAILED' && e.reason === 'OWNER_MODE_INVALID' && /印 .* を作らなかった/.test(e.message));
    // どちらも巻き戻った = 印も無い・持ち主は接続の役割のまま (役割は migration の外で作った = 残る)
    assert.equal((await c.query(`select count(*)::int as n from pg_class where relname = 'migrate_owner'`)).rows[0].n, 0);
    assert.equal(await ownerOf(c, 'ops.schema_migrations'), DEPLOYER);
    assert.equal(await ownerOf(c, 'app.t'), DEPLOYER);
    assert.deepEqual(await versions(c), ['0001']);
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('O5 印と owner-transition の適用を両方向で確かめる (Codex R-D60-v3-11 M-new-2): transition 適用済み・印が消えた → 止まる / 印がある・transition が未適用 (file なし・未適用) → 止まる', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const base = { '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role) };
    await migrate(c, { dir: mkDir(base) });
    const more = mkDir({ ...base, '0003_more.sql': 'create table app.more (x int);\n' });
    // 印が消えた (人が誤って drop) = legacy に戻らない
    const dbSu = new URL(suUrl); dbSu.pathname = `/${dbName}`;
    const s2 = await openPgClient(dbSu.toString()); s2.on('error', () => {}); clients.push(s2);
    await s2.query('drop table ops.migrate_owner');
    await assert.rejects(migrate(c, { dir: more }), (e) => e.code === 'OWNER_MODE_INVALID' && /印が消えた可能性/.test(e.message));
    // 接続の役割が記録表を読めても (権限を足しても) ① の検査で止まる
    await s2.query(`grant usage on schema ops to ${DEPLOYER}; grant select, insert on ops.schema_migrations to ${DEPLOYER}`);
    await assert.rejects(migrate(c, { dir: more }), (e) => e.code === 'OWNER_MODE_INVALID' && /適用済みなのに印/.test(e.message));
    assert.equal((await s2.query(`select count(*)::int as n from pg_class where relname = 'more'`)).rows[0].n, 0, 'legacy で流した');
    assert.deepEqual(await lockHolders(dbName), []);
    // 逆: 印だけがある (transition の file が無い・未適用) → 止まる
    const db2 = await newDepDb();
    const c2 = await openDep(db2);
    await migrate(c2, { dir: mkDir({ '0001_base.sql': BASE }) });
    const db2Su = new URL(suUrl); db2Su.pathname = `/${db2}`;
    const s3 = await openPgClient(db2Su.toString()); s3.on('error', () => {}); clients.push(s3);
    await s3.query(`create table ops.migrate_owner (x int); alter table ops.migrate_owner owner to ${DEPLOYER}`);
    await assert.rejects(migrate(c2, { dir: mkDir({ '0001_base.sql': BASE, '0002_more.sql': 'create table app.more (x int);\n' }) }), (e) => e.code === 'OWNER_MODE_INVALID' && /file に無い/.test(e.message));
    const r2 = await newOwnerRole();
    await assert.rejects(migrate(c2, { dir: mkDir({ '0001_base.sql': BASE, '0002_owner.sql': transitionSql(db2, r2) }) }), (e) => e.code === 'OWNER_MODE_INVALID' && /が適用されていない \(0002_owner\.sql\)/.test(e.message));
    assert.deepEqual(await versions(c2), ['0001']);
    assert.deepEqual(await lockHolders(db2), []);
  });

  await t('O6 記録の INSERT は必ず印の役割 (Codex R-D60-v3-11 M-new-3・設計 13 v3.11 ②) = owner-transition の本文が RESET ROLE しても・owner の状態の本文が (字句の検査を逃れる DO の中で) set_config(role) しても、INSERT の時の current_user = 印の役割', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    await migrate(c, { dir: mkDir({ '0001_base.sql': BASE }) });
    // 試験の trigger = 記録の INSERT の時の current_user を残す (superuser が作る)
    const dbSu = new URL(suUrl); dbSu.pathname = `/${dbName}`;
    const s2 = await openPgClient(dbSu.toString()); s2.on('error', () => {}); clients.push(s2);
    await s2.query(`create table ops.mig_who (version text, who text);
      grant insert on ops.mig_who to public;
      create function ops.mig_who_f() returns trigger language plpgsql as $f$ begin insert into ops.mig_who values (new.version, current_user::text); return new; end $f$;
      create trigger mig_who before insert on ops.schema_migrations for each row execute function ops.mig_who_f();`);
    // 🆕 Codex R-D60-v3-13 H1: DO の本文の set_config('role', …) はもう字句の検査が見つける (ドルの引用の中も見る)
    assert.ok(roleSwitchStatements("do $$ begin perform set_config('role', 'none', true); end $$;\n").length > 0);
    // それでも動的 SQL (EXECUTE format(…) の文字列) の中は見ない = 字句の検査は補助で sandbox ではない (= 守りの本体は記録の直前の SET LOCAL ROLE)。
    // 下の形は SET LOCAL ROLE NONE と同じ。接続の役割は記録表に書けない = 直前の SET LOCAL ROLE が無ければ 42501。runner は ⚠️ を出して流す
    const evade = "do $$ begin execute format('select set_config(%L, %L, true)', 'role', 'none'); end $$;\n";
    assert.deepEqual(roleSwitchStatements(evade), []);
    const logs = [];
    const r = await migrate(c, { dir: mkDir({ '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role, { tail: 'reset role;\n' }), '0003_evade.sql': evade }), log: (m) => logs.push(m) });
    assert.deepEqual(r.applied, ['0002', '0003']);
    assert.ok(logs.some((m) => /⚠️ 0003_evade\.sql: .*DO 1 個・ドルの引用の中の EXECUTE \(動的 SQL\) 1 個.*sandbox ではない/.test(m)), logs.join('\n'));
    const who = Object.fromEntries((await s2.query('select version, who from ops.mig_who')).rows.map((x) => [x.version, x.who]));
    assert.deepEqual(who, { '0002': role, '0003': role });
    assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER });
  });

  await t('O7 owner の状態の file の SET LOCAL ROLE = 許す一覧の役割は通る (記録は印の役割) / 一覧の外の役割は流す前に止まる (設計 13 v3.11 ③)', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const base = { '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role) };
    await migrate(c, { dir: mkDir(base) });
    // NOLOGIN の持ち主 (一覧の 1 つ) を作り、印の役割から SET できるようにする (クラスタに 1 回)
    if (!(await su.query(`select 1 from pg_roles where rolname = 'profit_definer'`)).rows.length) await su.query('create role profit_definer nologin');
    await su.query(`grant profit_definer to ${role} with inherit false, set true`);
    const ok1 = 'set local role profit_definer;\nselect 1;\n';
    const r = await migrate(c, { dir: mkDir({ ...base, '0003_definer.sql': ok1 }) });
    assert.deepEqual(r.applied, ['0003']);
    assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER });
    await assert.rejects(migrate(c, { dir: mkDir({ ...base, '0003_definer.sql': ok1, '0004_watcher.sql': 'set local role watcher;\nselect 1;\n' }) }),
      (e) => e.code === 'OWNER_MODE_INVALID' && /set local role watcher \(許す一覧の外\)/.test(e.message));
    assert.deepEqual(await lockHolders(dbName), []);
  });

  // 🆕 Codex R1 H1: 引用の名前・大文字小文字・空白・コメントを挟む役割の切り替え。全部、Postgres で本当に役割を戻す形 (下の「前提」で確かめる)
  const QUOTED_ROLE_SWITCHES = ['RESET "role";', "SET LOCAL \"role\" = 'none';", "SELECT \"set_config\"('role', 'none', true);",
    'reset /* c */ "ROLE";', "set\n  local -- c\n  \"Role\" = 'none';", "select pg_catalog.\"set_config\"( 'role' , 'none' , true );", 'RESET\n"role"',
    // 🆕 Codex R-D60-v3-13 H1: DO の本文の中
    "do $$ begin perform set_config('role', 'none', true); end $$;"];
  await t('O8 (Codex R1 H1) 引用の名前の役割の切り替え (RESET "role"・SET LOCAL "role" = \'none\'・"set_config"(\'role\', …) と大文字小文字・空白・コメント) = 本物の PG と PGlite で本当に役割が戻る形 → owner の状態では流す前に止まる (current_user は接続の役割に戻らない = 何も作らない)', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const base = { '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role) };
    await migrate(c, { dir: mkDir(base) });
    for (const x of QUOTED_ROLE_SWITCHES) assert.ok(roleSwitchStatements(x).length > 0, `拒まない: ${x}`);
    // (前提) 本物の PG: 各形は SET LOCAL ROLE <印の役割> を本当に接続の役割へ戻す (= 検査を逃れると後の DDL が接続の役割の物になる)
    for (const x of QUOTED_ROLE_SWITCHES) {
      await c.query('begin');
      try {
        await c.query(`set local role ${role}`);
        assert.equal((await whoami(c)).cu, role);
        await c.query(x);
        assert.equal((await whoami(c)).cu, DEPLOYER, `(前提) ${x} で役割が戻らない`);
      } finally { await c.query('rollback'); }
    }
    const dbSu = new URL(suUrl); dbSu.pathname = `/${dbName}`;
    const s2 = await openPgClient(dbSu.toString()); s2.on('error', () => {}); clients.push(s2);
    for (const x of QUOTED_ROLE_SWITCHES) {
      const d = mkDir({ ...base, '0003_evade.sql': `create table app.o8_before (x int);\n${x}\ncreate table app.o8_after (x int);\n` });
      await assert.rejects(migrate(c, { dir: d }), (e) => e.code === 'OWNER_MODE_INVALID' && /許さない役割の切り替え/.test(e.message), x);
      assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER });
      assert.equal((await s2.query(`select count(*)::int as n from pg_class where relname like 'o8\\_%'`)).rows[0].n, 0, `${x} を流した`);
      assert.deepEqual((await s2.query('select version from ops.schema_migrations order by 1')).rows.map((r) => r.version), ['0001', '0002']);
      assert.deepEqual(await lockHolders(dbName), []);
    }
    // PGlite (superuser の 1 つの session) でも同じ: 各形で役割が戻る (前提) / owner の状態の file にあれば流す前に止まる
    const pgl = new PGlite();
    try {
      const db = pgliteAdapter(pgl);
      const prole = 'cdb_owner_pgl_o8';
      await db.exec(`create role ${prole} nologin`);   // 役割は migration の外で作る (形 B・owner-transition の役割の図の比較)
      const transPgl = `-- migrate:owner-transition
grant usage, create on schema app, ops to ${prole};
alter table app.t owner to ${prole};
alter table ops.schema_migrations owner to ${prole};
create table ops.migrate_owner (singleton boolean primary key default true check (singleton), owner_role name not null, since timestamptz not null default now());
insert into ops.migrate_owner (owner_role) values ('${prole}');
alter table ops.migrate_owner owner to ${prole};
`;
      const pbase = { '0001_base.sql': BASE, '0002_owner.sql': transPgl };
      assert.deepEqual((await applyMigrations(db, { dir: mkDir(pbase), log: quiet })).applied, ['0001', '0002']);
      const cu = async () => (await db.query('select current_user::text as cu')).rows[0].cu;
      const su0 = await cu();
      for (const x of QUOTED_ROLE_SWITCHES) {
        await db.exec('begin');
        try {
          await db.exec(`set local role ${prole}`);
          assert.equal(await cu(), prole);
          await db.exec(x);
          assert.equal(await cu(), su0, `(前提・PGlite) ${x} で役割が戻らない`);
        } finally { await db.exec('rollback'); }
      }
      for (const x of QUOTED_ROLE_SWITCHES) {
        await assert.rejects(applyMigrations(db, { dir: mkDir({ ...pbase, '0003_evade.sql': `create table app.o8_before (x int);\n${x}\ncreate table app.o8_after (x int);\n` }), log: quiet }),
          (e) => e.code === 'OWNER_MODE_INVALID' && /許さない役割の切り替え/.test(e.message), x);
        assert.equal((await db.query(`select count(*)::int as n from pg_class where relname like 'o8\\_%'`)).rows[0].n, 0, `PGlite: ${x} を流した`);
        assert.equal(await cu(), su0);
      }
    } finally { await pgl.close(); }
  });

  await t('O9 (Codex R1 M1) --list も印と owner-transition の適用を両方向で確かめる: 印が消えた (接続の役割に記録表の SELECT が残る) → 一覧を出して exit 1 (OWNER_MODE_INVALID) / 印だけある → exit 1 / 整っていれば exit 0', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const base = { '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role) };
    const dir = mkDir(base);
    await migrate(c, { dir });
    const ok1 = runCli(['--url', urlDep(dbName), '--dir', dir, '--list']);
    assert.equal(ok1.status, 0, ok1.stderr); assert.match(ok1.stdout, /0002 applied/);
    // 印が消えた + 接続の役割が記録表を読める (旧 --list は「全部 applied・legacy・exit 0」だった)
    const dbSu = new URL(suUrl); dbSu.pathname = `/${dbName}`;
    const s2 = await openPgClient(dbSu.toString()); s2.on('error', () => {}); clients.push(s2);
    await s2.query(`drop table ops.migrate_owner; grant usage on schema ops to ${DEPLOYER}; grant select on ops.schema_migrations to ${DEPLOYER}`);
    const lost = runCli(['--url', urlDep(dbName), '--dir', dir, '--list']);
    assert.equal(lost.status, 1, lost.stdout + lost.stderr);
    assert.match(lost.stdout, /0002 applied/);   // 一覧は出す
    assert.match(lost.stdout, /lock \(company_db_migrate\) を持っている接続は無い/);
    assert.match(lost.stderr, /FAILED \(OWNER_MODE_INVALID\).*適用済みなのに印/);
    await assert.rejects(migrationStatus(pgAdapter(c), { dir }), (e) => e.code === 'OWNER_MODE_INVALID' && Array.isArray(e.statusRows) && e.statusRows.length === 2);
    // 逆: 印だけがある (owner-transition の file が無い・未適用)
    const db2 = await newDepDb();
    const c2 = await openDep(db2);
    await migrate(c2, { dir: mkDir({ '0001_base.sql': BASE }) });
    const db2Su = new URL(suUrl); db2Su.pathname = `/${db2}`;
    const s3 = await openPgClient(db2Su.toString()); s3.on('error', () => {}); clients.push(s3);
    await s3.query(`create table ops.migrate_owner (x int); alter table ops.migrate_owner owner to ${DEPLOYER}`);
    const only = runCli(['--url', urlDep(db2), '--dir', mkDir({ '0001_base.sql': BASE }), '--list']);
    assert.equal(only.status, 1, only.stdout + only.stderr); assert.match(only.stderr, /OWNER_MODE_INVALID.*file に無い/);
    const only2 = runCli(['--url', urlDep(db2), '--dir', mkDir({ '0001_base.sql': BASE, '0002_owner.sql': transitionSql(db2, `cdb_owner_${hex}_${++roleSeq}`) }), '--list']);
    assert.equal(only2.status, 1); assert.match(only2.stderr, /OWNER_MODE_INVALID.*適用されていない/);
  });

  await t('M3 (Codex R1 M3) ふつうの file と owner-transition の file の最上位の取引の制御 = 流す前に止まる (前の番号も流さない) / 0001〜 の本物の file には無い (後方の互換) / begin atomic と DO の中は数えない', async () => {
    const real = listMigrationFiles();
    assert.ok(real.length >= 56);
    for (const f of real) assert.deepEqual(txControlStatements(f.text), [], f.file);
    const rejected = ['commit', 'COMMIT;', 'select 1; commit', 'rollback', 'ROLLBACK TO SAVEPOINT s', 'begin', 'BEGIN TRANSACTION', 'start transaction', 'end', 'abort', 'savepoint s',
      'release savepoint s', 'release s', "prepare transaction 'x'", "commit prepared 'x'", "rollback prepared 'x'", 'create table app.a (x int); /* c */ commit; create table app.b (x int)', 'Commit /* c */ ;'];
    for (const x of rejected) assert.ok(txControlStatements(x).length > 0, `拒まない: ${x}`);
    const allowed = ['do $$ begin commit; end $$', "select 'commit'", '-- commit', 'create function app.f() returns int language sql begin atomic select 1; select 2; end; select 3',
      'create function app.g() returns int language sql begin atomic end', 'prepare p as select 1', 'create table app.x ("commit" int)', 'select case when true then 1 end', "comment on table app.t is 'begin; commit'"];
    for (const x of allowed) assert.deepEqual(txControlStatements(x), [], `拒んだ: ${x}`);
    // 本物の PG: ふつうの file の commit → 何も流さない (前の番号も)
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const evil = 'create table app.m3a (x int);\ncommit;\ncreate table app.m3b (x int);\n';
    await assert.rejects(migrate(c, { dir: mkDir({ '0001_base.sql': BASE, '0002_evil.sql': evil }) }), (e) => e.code === 'TX_CONTROL_REJECTED' && e.version === '0002' && /commit/.test(e.message));
    assert.equal((await c.query(`select count(*)::int as n from pg_namespace where nspname = 'app'`)).rows[0].n, 0, '0001 を流した');
    // 拒む形の全部を本物の PG の runner に通す (Codex R-D60-v3-14 L3) = どれも流す前に止まり、前の番号も流さない
    for (const x of rejected) {
      await assert.rejects(migrate(c, { dir: mkDir({ '0001_base.sql': BASE, '0002_tx.sql': `create table app.m3c (x int);\n${x};\n` }) }), (e) => e.code === 'TX_CONTROL_REJECTED' && e.version === '0002', x);
    }
    assert.equal((await c.query(`select count(*)::int as n from pg_namespace where nspname = 'app'`)).rows[0].n, 0, '0001 を流した');
    // 許す形 (DO の中の commit・begin atomic など) は本物の PG でも流れる (前提の作り)
    const okBody = "create table app.m3ok (x int);\ndo $$ begin perform 1; end $$;\ncreate function app.m3f() returns int language sql begin atomic select 1; select 2; end;\nselect 'commit';\n";
    const cOk = await openDep(await newDepDb());
    assert.deepEqual((await migrate(cOk, { dir: mkDir({ '0001_base.sql': BASE, '0002_ok.sql': okBody }) })).applied, ['0001', '0002']);
    // owner-transition の file の commit / savepoint も
    for (const tail of ['commit;\n', 'savepoint s1;\n']) {
      const role = `cdb_owner_${hex}_${++roleSeq}`;   // 流す前に止まる = 役割は作らない
      await assert.rejects(migrate(c, { dir: mkDir({ '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role, { tail }) }) }), (e) => e.code === 'TX_CONTROL_REJECTED', tail);
      assert.equal((await c.query(`select count(*)::int as n from pg_namespace where nspname = 'app'`)).rows[0].n, 0, '0001 を流した');
    }
    assert.deepEqual(await lockHolders(dbName), []);
    // PGlite の道も
    const pgl = new PGlite();
    try { await assert.rejects(applyMigrations(pgliteAdapter(pgl), { dir: mkDir({ '0001_base.sql': BASE, '0002_evil.sql': evil }), log: quiet }), (e) => e.code === 'TX_CONTROL_REJECTED'); } finally { await pgl.close(); }
  });

  await t('O10 (設計 13 v3.14 ②・③) owner の状態: 禁止の集合の各項目を deployer の届く役割に 1 つずつ付けると止まる (本番の道・--dry-run・--list)・付けなければ空 / 本文が動的 SQL で字句の検査を逃れて役割の図を変える (deployer 自身・印の役割の設定) → ROLE_GRAPH_CHANGED で巻き戻す', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const base = { '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role) };
    await migrate(c, { dir: mkDir(base) });
    const dbSu = new URL(suUrl); dbSu.pathname = `/${dbName}`;
    const s2 = await openPgClient(dbSu.toString()); s2.on('error', () => {}); clients.push(s2);
    const count = async (rel) => (await s2.query('select count(*)::int as n from pg_class where relname = $1', [rel])).rows[0].n;
    const recorded = async () => (await s2.query('select version from ops.schema_migrations order by 1')).rows.map((r) => r.version);
    const okDir = mkDir({ ...base, '0003_ok.sql': 'create table app.o10_ok (x int);\n' });
    // 形 B の deployer = 届かない = 検査は空 (3 つの道)
    assert.deepEqual(await roleAdminReachable(pgAdapter(c)), []);
    assert.deepEqual((await migrate(c, { dir: okDir, dryRun: true })).pending, ['0003']);
    const list0 = runCli(['--url', urlDep(dbName), '--dir', okDir, '--list']);
    assert.equal(list0.status, 0, list0.stdout + list0.stderr);
    // ② 禁止の集合 (定数 = 正本) の各項目を、deployer が MEMBER で届く役割 (INHERIT / SET なし) に 1 つずつ付ける → 3 つの道とも止まる → 外す
    const helper = `cdb_o10_helper_${hex}`;
    await su.query(`create role ${helper} nologin`);
    await su.query(`grant ${helper} to ${DEPLOYER} with inherit false, set false`);
    const items = [
      ['superuser', `alter role ${helper} superuser`, `alter role ${helper} nosuperuser`, /superuser/],
      ['CREATEROLE', `alter role ${helper} createrole`, `alter role ${helper} nocreaterole`, /CREATEROLE/],
      ['CREATEDB', `alter role ${helper} createdb`, `alter role ${helper} nocreatedb`, /CREATEDB/],
      ['REPLICATION', `alter role ${helper} replication`, `alter role ${helper} noreplication`, /REPLICATION/],
      ['BYPASSRLS', `alter role ${helper} bypassrls`, `alter role ${helper} nobypassrls`, /BYPASSRLS/],
      ['ADMIN の行', `grant ${role} to ${helper} with admin option`, `revoke ${role} from ${helper}`, /ADMIN の membership/],
      ['deployer 自身の CREATEROLE', `alter role ${DEPLOYER} createrole`, `alter role ${DEPLOYER} nocreaterole`, /CREATEROLE/],
      ...ROLE_ADMIN_FORBIDDEN_PREDEFINED.map((pr) => [pr, `grant ${pr} to ${helper}`, `revoke ${pr} from ${helper}`, /危険な定義済みの役割/]),
    ];
    assert.deepEqual(ROLE_ADMIN_FORBIDDEN_PREDEFINED, ['pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program', 'pg_create_subscription', 'pg_checkpoint', 'pg_signal_backend']);
    for (const [what, on, off, re] of items) {
      await su.query(on);
      try {
        const isReach = (e) => e.code === 'OWNER_MODE_INVALID' && e.reason === 'ROLE_ADMIN_REACHABLE' && re.test(e.message);
        await assert.rejects(migrate(c, { dir: okDir }), isReach, `本番の道: ${what}`);
        await assert.rejects(migrate(c, { dir: okDir, dryRun: true }), isReach, `dry-run: ${what}`);
        const list = runCli(['--url', urlDep(dbName), '--dir', okDir, '--list']);
        assert.equal(list.status, 1, `--list: ${what} ${list.stdout}`);
        assert.match(list.stderr, /FAILED \(OWNER_MODE_INVALID\).*ROLE_ADMIN_REACHABLE/, what);
        assert.equal(await count('o10_ok'), 0, `流した: ${what}`);
        assert.deepEqual(await lockHolders(dbName), []);
      } finally { await su.query(off); }
    }
    assert.deepEqual(await roleAdminReachable(pgAdapter(c)), []);
    assert.deepEqual((await migrate(c, { dir: okDir })).applied, ['0003']);
    // ③ 本文が動的 SQL で役割を戻して (字句の検査は見つけない) 役割の図を変える = deployer 自身・印の役割の既定の設定 (形 B でも自分の設定は変えられる) → 巻き戻す
    const hide = "execute format('select set_config(%L, %L, true)', 'role', 'none');";
    const settings = async () => (await su.query('select count(*)::int as n from pg_db_role_setting s join pg_roles r on r.oid = s.setrole where r.rolname = any($1)', [[DEPLOYER, role]])).rows[0].n;
    for (const [what, body] of [
      ['deployer 自身の設定', `create table app.o10_evil (x int);\ndo $$ begin ${hide} execute format('alter role %I set work_mem = %L', '${DEPLOYER}', '64MB'); end $$;\n`],
      ['印の役割の設定', `create table app.o10_evil (x int);\ndo $$ begin execute format('alter role %I set work_mem = %L', current_user, '32MB'); end $$;\n`],
    ]) {
      assert.deepEqual(roleSwitchStatements(body), [], `(前提) 字句の検査は見つけない: ${what}`);
      let err = null;
      try { await migrate(c, { dir: mkDir({ ...base, '0003_ok.sql': 'create table app.o10_ok (x int);\n', '0004_evil.sql': body }) }); } catch (e) { err = e; }
      assert.ok(err && err.code === 'MIGRATION_FAILED' && err.version === '0004' && err.reason === 'ROLE_GRAPH_CHANGED' && /ROLE_GRAPH_CHANGED/.test(err.message), `${what}: ${err && err.code} ${err && err.reason} ${err && err.message}`);
      assert.equal(await settings(), 0, what);
      assert.equal(await count('o10_evil'), 0, what);
      assert.deepEqual(await recorded(), ['0001', '0002', '0003'], what);
      assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER });
      assert.deepEqual(await lockHolders(dbName), []);
    }
  });

  await t('O11 (Codex R-D60-v3-14 M1・L3) 本文が記録表に deferred の制約の trigger を作り、runner の記録の INSERT で役割の図を変える → 記録の INSERT の後の SET CONSTRAINTS ALL IMMEDIATE → 最後の hash で止まる (本文も記録も全部巻き戻す)', async () => {
    const dbName = await newDepDb();
    const c = await openDep(dbName);
    const role = await newOwnerRole();
    const base = { '0001_base.sql': BASE, '0002_owner.sql': transitionSql(dbName, role) };
    await migrate(c, { dir: mkDir(base) });
    const dbSu = new URL(suUrl); dbSu.pathname = `/${dbName}`;
    const s2 = await openPgClient(dbSu.toString()); s2.on('error', () => {}); clients.push(s2);
    // trigger の関数の本文 = 動的 SQL で役割を戻して deployer 自身の既定の設定を変える (字句の検査は文字列の中を見ない)
    const body = `create function app.o11_f() returns trigger language plpgsql as $f$ begin
  execute format('select set_config(%L, %L, true)', 'role', 'none');
  execute format('alter role %I set work_mem = %L', '${DEPLOYER}', '64MB');
  return null;
end $f$;
create constraint trigger o11_t after insert on ops.schema_migrations deferrable initially deferred for each row execute function app.o11_f();
`;
    assert.deepEqual(roleSwitchStatements(body), [], '(前提) 字句の検査は見つけない');
    let err = null;
    try { await migrate(c, { dir: mkDir({ ...base, '0003_deferred.sql': body }) }); } catch (e) { err = e; }
    assert.ok(err && err.code === 'MIGRATION_FAILED' && err.version === '0003' && err.reason === 'ROLE_GRAPH_CHANGED', `${err && err.code} ${err && err.reason} ${err && err.message}`);
    assert.equal((await su.query('select count(*)::int as n from pg_db_role_setting s join pg_roles r on r.oid = s.setrole where r.rolname = $1', [DEPLOYER])).rows[0].n, 0, 'commit の時に trigger が設定を変えた');
    assert.equal((await s2.query(`select count(*)::int as n from pg_trigger where tgname = 'o11_t'`)).rows[0].n, 0);
    assert.deepEqual((await s2.query('select version from ops.schema_migrations order by 1')).rows.map((r) => r.version), ['0001', '0002']);
    assert.deepEqual(await whoami(c), { cu: DEPLOYER, su: DEPLOYER });
    assert.deepEqual(await lockHolders(dbName), []);
  });

  await t('X1 --index-expect は使い捨ての DB から expect.json の中身を出す (試験で作ったものと同じ)', async () => {
    const r = runCli(['--url', urlOf(dbX), '--dir', ciDir(), '--index-expect', '0002']);
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
