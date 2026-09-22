#!/usr/bin/env node
/**
 * test-sqljs-guard.mjs — lib/sqljs-guard.js (sql.js の DB を「ほかのプロセスが書いた行を黙って上書きする」事故から守る共通の歯止め) の試験。
 *   ① 歯止めそのもの: 保存で世代の印が変わる / 外から書かれたら上書きしない (SQLJS_DB_EXTERNAL_WRITE) / 読み直せば通る /
 *      書きかけのファイルは読まない / 印の無い古いファイルも読めて最初の保存で印が付く / lock は毎回外れる
 *   ② 呼び出し元: apps/profit-calculator/db.js と apps/mercari-sync/settings-db.js の saveToFile() が歯止めを通っている
 *      (保存後のファイルに _file_gen がある・外から書かれたら SQLJS_DB_EXTERNAL_WRITE で保存しない)
 * 一時ディレクトリで完結する (本番・開発の data/ には触れない。ネットワークも無し)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import initSqlJs from 'sql.js';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sqljs-guard-'));
process.env.DATA_DIR = tmp;                  // 呼び出し元 (db.js / settings-db.js) は import の時点で DATA_DIR を読む
process.env.SQLJS_DB_LOCK_WAIT_MS = '300';   // 🚨 SQLite の busy の待ちは Windows では 1 秒刻み = 実際には 1 秒ほど待つ

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const codeOf = (fn) => { try { fn(); return null; } catch (e) { return e.code || e.message; } };
const tick = () => new Promise((r) => setTimeout(r, 25));   // 更新時刻が確実に変わるまで待つ
/** ファイルの中身を、歯止めを通さずに読む */
const readFile = (file, sql) => { const f = new Database(file, { readonly: true, fileMustExist: true }); try { return f.prepare(sql).all(); } finally { f.close(); } };
const tokenOf = (file) => { const rows = readFile(file, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_file_gen'`); return rows.length ? readFile(file, 'SELECT token FROM _file_gen WHERE id = 1')[0].token : null; };

try {
  const { loadGuarded, saveGuarded, withSqljsFileLock, isSqljsGuardError, lockDbFileOf } = await import(pathToFileURL(path.join(root, 'lib', 'sqljs-guard.js')).href);
  const SQL = await initSqlJs();
  const file = path.join(tmp, 'guard.db');

  console.log('① 歯止めそのもの (lib/sqljs-guard.js)');
  let A = null;   // 常駐サーバの役 (読んだメモリを持ち続ける)
  await t('(a) 無いファイルから読む → 保存 → 世代の印が付く・gen が変わる・ファイルに _file_gen がある', async () => {
    A = loadGuarded({ file, SQL });
    assert.deepEqual(A.gen, { token: null, stamp: null }, 'ファイルが無い = 失うものが無い');
    A.db.run('CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)');
    A.db.run(`INSERT INTO kv VALUES ('a', '1')`);
    const gen1 = saveGuarded({ file, db: A.db, gen: A.gen });
    assert.ok(gen1.token && gen1.stamp && gen1.stamp.size > 0, 'gen = { token, stamp }');
    assert.notEqual(gen1.token, A.gen.token);
    assert.equal(tokenOf(file), gen1.token, 'ファイルの中の印 = 返った gen の印');
    A.gen = gen1;
    const gen2 = saveGuarded({ file, db: A.db, gen: A.gen });
    assert.notEqual(gen2.token, gen1.token, '保存のたびに印は新しくなる');
    assert.equal(tokenOf(file), gen2.token);
    A.gen = gen2;
    assert.ok(!fs.readdirSync(tmp).some((n) => n.endsWith('.tmp')), '一時ファイルが残らない');
  });
  await t('(b) 🚨 本番で起きた形: A が読む → B が読んで書く → A が保存 … A は上書きせず SQLJS_DB_EXTERNAL_WRITE。ファイルは B の書いたまま (A のメモリの印も変えない)', async () => {
    await tick();
    const B = loadGuarded({ file, SQL });
    B.db.run(`INSERT INTO kv VALUES ('b', '2')`);
    B.gen = saveGuarded({ file, db: B.db, gen: B.gen });
    A.db.run(`INSERT INTO kv VALUES ('a2', '3')`);
    await tick();
    let e = null;
    try { saveGuarded({ file, db: A.db, gen: A.gen }); } catch (x) { e = x; }
    assert.equal(e && e.code, 'SQLJS_DB_EXTERNAL_WRITE', `例外: ${e && e.message}`);
    assert.ok(isSqljsGuardError(e));
    assert.match(e.message, /guard\.db がほかのプロセスに書き換えられていた/);
    assert.deepEqual(readFile(file, 'SELECT k FROM kv ORDER BY k').map((r) => r.k), ['a', 'b'], 'ファイルは B が書いたまま (a2 は無い・b は消えていない)');
    assert.equal(tokenOf(file), B.gen.token, 'ファイルの印も B のまま');
    assert.equal(A.db.exec('SELECT token FROM _file_gen')[0].values[0][0], A.gen.token, 'A のメモリの印は保存前のまま (= やり直しても同じ判定になる)');
  });
  await t('(c) 読み直してから保存すれば通る。B の行は消えない', async () => {
    A.db.close();
    A = loadGuarded({ file, SQL });
    assert.deepEqual(A.db.exec('SELECT k FROM kv ORDER BY k')[0].values.map((r) => r[0]), ['a', 'b'], '読み直したメモリに B の行が入っている');
    A.db.run(`INSERT INTO kv VALUES ('a3', '4')`);
    A.gen = saveGuarded({ file, db: A.db, gen: A.gen });
    assert.deepEqual(readFile(file, 'SELECT k FROM kv ORDER BY k').map((r) => r.k), ['a', 'a3', 'b']);
  });
  await t('(c2) 更新時刻もサイズも同じ書き換えでも見つける (判定はファイルの中の印): 同じ大きさの書き換えを stamp を偽って再現', async () => {
    const C = loadGuarded({ file, SQL });
    C.db.run(`UPDATE kv SET v = '9' WHERE k = 'a'`);   // 行の大きさを変えない
    C.gen = saveGuarded({ file, db: C.db, gen: C.gen });
    C.db.close();
    const fakeGen = { token: A.gen.token, stamp: { ...C.gen.stamp } };   // 更新時刻とサイズは「同じ」・印だけ古い
    assert.equal(codeOf(() => saveGuarded({ file, db: A.db, gen: fakeGen })), 'SQLJS_DB_EXTERNAL_WRITE');
    A.db.close();
    A = loadGuarded({ file, SQL });
  });
  await t('(d) 書きかけのファイル (10 バイトに切る) は読まない = SQLJS_DB_FILE_TORN。ファイルは触らない', async () => {
    const torn = path.join(tmp, 'torn.db');
    fs.copyFileSync(file, torn);
    fs.truncateSync(torn, 10);
    let e = null;
    try { loadGuarded({ file: torn, SQL }); } catch (x) { e = x; }
    assert.equal(e && e.code, 'SQLJS_DB_FILE_TORN', `例外: ${e && e.message}`);
    assert.match(e.message, /torn\.db が書きかけで止まっている/);
    assert.match(e.message, /控え/);
    assert.equal(fs.statSync(torn).size, 10, '読み込みはファイルを書き換えない');
    // ヘッダはそろっているが、ヘッダの言う大きさに足りない (途中で止まった) 形
    const torn2 = path.join(tmp, 'torn2.db');
    const whole = fs.readFileSync(file);
    fs.writeFileSync(torn2, whole.subarray(0, whole.length - 1));
    assert.equal(codeOf(() => loadGuarded({ file: torn2, SQL })), 'SQLJS_DB_FILE_TORN');
    // 保存側: 書きかけのファイルには守る中身が無い = メモリで書き直す (警告)
    const warned = [];
    const D = loadGuarded({ file, SQL });
    fs.truncateSync(file, 10);
    D.gen = saveGuarded({ file, db: D.db, gen: D.gen, log: { warn: (m) => warned.push(m) } });
    assert.match(warned.join('\n'), /書きかけで止まっていた/);
    assert.deepEqual(readFile(file, 'SELECT k FROM kv ORDER BY k').map((r) => r.k), ['a', 'a3', 'b'], '書き直された');
    D.db.close();
    A.db.close();
    A = loadGuarded({ file, SQL });
  });
  await t('(e) _file_gen の無い古いファイルも読める。最初の保存で印が付く。古い版の書き手 (印を書かない) の書き換えも見つける', async () => {
    const legacy = path.join(tmp, 'legacy.db');
    { const l = new Database(legacy); l.exec(`CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT); INSERT INTO config VALUES ('x', '1')`); l.close(); }
    assert.equal(tokenOf(legacy), null, '前提: 印が無い');
    const L = loadGuarded({ file: legacy, SQL });
    assert.deepEqual(L.gen, { token: null, stamp: { mtimeMs: fs.statSync(legacy).mtimeMs, size: fs.statSync(legacy).size } });
    L.gen = saveGuarded({ file: legacy, db: L.db, gen: L.gen });
    assert.ok(L.gen.token, '印が付いた');
    assert.equal(tokenOf(legacy), L.gen.token);
    assert.deepEqual(readFile(legacy, 'SELECT key, value FROM config'), [{ key: 'x', value: '1' }], '中身は残っている');
    // 印を書かない古い版の書き手 (= better-sqlite3 で直接書く) が書き換えた → 更新時刻とサイズで見つける
    await tick();
    { const l = new Database(legacy); l.exec(`INSERT INTO config VALUES ('y', '2')`); l.close(); }
    assert.equal(codeOf(() => saveGuarded({ file: legacy, db: L.db, gen: L.gen })), 'SQLJS_DB_EXTERNAL_WRITE');
    assert.deepEqual(readFile(legacy, 'SELECT key FROM config ORDER BY key').map((r) => r.key), ['x', 'y'], 'y は消えていない');
    L.db.close();
  });
  await t('(f) lock は呼ぶたびに外れる (次の呼び出し = 別プロセスの代わり が取れる)。持っている間は、ほかは待って SQLJS_DB_LOCK_TIMEOUT', async () => {
    assert.ok(fs.existsSync(lockDbFileOf(file)), 'lock 用の DB はデータファイルの隣 (<file>.lockdb)');
    assert.equal(withSqljsFileLock(file, () => 'got', { waitMs: 300 }), 'got', 'saveGuarded の後に取れる');
    A.gen = saveGuarded({ file, db: A.db, gen: A.gen });
    assert.equal(withSqljsFileLock(file, () => 'got', { waitMs: 300 }), 'got', '保存の直後にも取れる');
    const inner = withSqljsFileLock(file, () => codeOf(() => withSqljsFileLock(file, () => 'x', { waitMs: 300 })), { waitMs: 300 });
    assert.equal(inner, 'SQLJS_DB_LOCK_TIMEOUT', '持っている間は取れない (= lock は本物)');
    const held = withSqljsFileLock(file, () => codeOf(() => saveGuarded({ file, db: A.db, gen: A.gen, waitMs: 300 })), { waitMs: 300 });
    assert.equal(held, 'SQLJS_DB_LOCK_TIMEOUT', '相手が持っている間の保存は SQLJS_DB_LOCK_TIMEOUT (= 保存していない)');
    assert.equal(withSqljsFileLock(file, () => 'got', { waitMs: 300 }), 'got', '失敗の後も外れている');
    let e = null;
    try { withSqljsFileLock(file, () => { throw new Error('boom'); }); } catch (x) { e = x; }
    assert.equal(e && e.message, 'boom', 'fn の例外はそのまま');
    assert.equal(withSqljsFileLock(file, () => 'got', { waitMs: 300 }), 'got', 'fn が投げても外れている');
  });
  await t('(g) 保存の途中のどんな失敗も SQLJS_DB_* (= 保存していない) にそろえる: 書き出しの失敗 = SAVE_FAILED (ファイルは元のまま)、lock を開けない = LOCK_ERROR', async () => {
    const tokBefore = tokenOf(file);
    const broken = { run: () => {}, export: () => { throw new Error('export boom'); } };   // export で死ぬ = 書き込みの失敗の代わり
    let e = null;
    try { saveGuarded({ file, db: broken, gen: A.gen }); } catch (x) { e = x; }
    assert.equal(e && e.code, 'SQLJS_DB_SAVE_FAILED', `例外: ${e && e.message}`);
    assert.match(e.message, /export boom/);
    assert.match(e.message, /この操作は保存されていない/);
    assert.equal(tokenOf(file), tokBefore, 'ファイルは元のまま');
    assert.ok(!fs.readdirSync(tmp).some((n) => n.endsWith('.tmp')), '一時ファイルが残らない');
    e = null;
    try { saveGuarded({ file: path.join(tmp, 'no-such-dir', 'x.db'), db: A.db, gen: null }); } catch (x) { e = x; }
    assert.equal(e && e.code, 'SQLJS_DB_LOCK_ERROR', `例外: ${e && e.message}`);
    assert.match(e.message, /この操作は保存されていない/);
    assert.equal(withSqljsFileLock(file, () => 'got', { waitMs: 300 }), 'got', '失敗の後も lock は外れている');
  });
  A.db.close();

  console.log('② 呼び出し元 (profit-calculator/db.js・mercari-sync/settings-db.js) が歯止めを通っている');
  await t('profit-calculator/db.js: initDb → profit.db に _file_gen。外から書かれたら saveToFile は SQLJS_DB_EXTERNAL_WRITE で上書きしない → 読み直しているので、やり直せば通る', async () => {
    const P = await import(pathToFileURL(path.join(root, 'apps', 'profit-calculator', 'db.js')).href);
    const pfile = path.join(tmp, 'profit.db');
    await P.initDb();
    assert.ok(fs.existsSync(pfile));
    assert.ok(tokenOf(pfile), 'initDb の保存で印が付いている');
    P.setSyncMeta('k1', 'v1');
    const tok1 = tokenOf(pfile);
    assert.ok(tok1);
    assert.equal(readFile(pfile, `SELECT value FROM sync_meta WHERE key = 'k1'`)[0].value, 'v1');
    // 外のプロセスの役: 歯止めを通して別の行を書く
    await tick();
    const X = loadGuarded({ file: pfile, SQL });
    X.db.run(`INSERT OR REPLACE INTO sync_meta (key, value) VALUES ('from_x', 'x')`);
    saveGuarded({ file: pfile, db: X.db, gen: X.gen });
    X.db.close();
    await tick();
    const warned = [];
    const origWarn = console.warn;
    console.warn = (...a) => warned.push(a.join(' '));
    let e = null;
    try { P.setSyncMeta('k2', 'v2'); } catch (x) { e = x; } finally { console.warn = origWarn; }
    assert.equal(e && e.code, 'SQLJS_DB_EXTERNAL_WRITE', `例外: ${e && e.message}`);
    assert.match(warned.join('\n'), /profit\.db/, '警告がログに出る');
    assert.equal(readFile(pfile, `SELECT value FROM sync_meta WHERE key = 'from_x'`)[0].value, 'x', 'X の行は消えていない');
    assert.equal(readFile(pfile, `SELECT count(*) c FROM sync_meta WHERE key = 'k2'`)[0].c, 0, 'k2 は保存されていない');
    assert.equal(P.getSyncMeta('from_x'), 'x', '読み直したメモリに X の行が入っている');
    P.setSyncMeta('k2', 'v2');   // やり直し
    assert.deepEqual(readFile(pfile, `SELECT key FROM sync_meta WHERE key IN ('from_x', 'k2') ORDER BY key`).map((r) => r.key), ['from_x', 'k2'], '両方残る');
  });
  await t('🚨 profit-calculator/db.js: 読み直しでメモリの世代が進む。溜めた未保存の行を抱える処理 (一括リサーチ) は persistToDisk({ expectGeneration }) で世代を確かめ、違えば保存せずに SQLJS_DB_MEMORY_RELOADED (「消えたのに保存した」と言わない。Codex #1407 R1 #2)', async () => {
    const P = await import(pathToFileURL(path.join(root, 'apps', 'profit-calculator', 'db.js')).href);
    const pfile = path.join(tmp, 'profit.db');
    const g0 = P.getDbGeneration();
    assert.equal(P.persistToDisk({ expectGeneration: g0 }), g0, '同じ世代なら保存して世代を返す');
    // 外のプロセスが書く → このプロセスの次の保存は EXTERNAL_WRITE → 読み直し = 世代が 1 進む
    await tick();
    const X = loadGuarded({ file: pfile, SQL });
    X.db.run(`INSERT OR REPLACE INTO sync_meta (key, value) VALUES ('from_x2', 'x')`);
    saveGuarded({ file: pfile, db: X.db, gen: X.gen });
    X.db.close();
    await tick();
    const origWarn = console.warn; console.warn = () => {};
    let e = null;
    try { P.setSyncMeta('k3', 'v3'); } catch (x) { e = x; } finally { console.warn = origWarn; }
    assert.equal(e && e.code, 'SQLJS_DB_EXTERNAL_WRITE');
    assert.equal(P.getDbGeneration(), g0 + 1, '読み直しで世代が進む');
    // 一括処理の役: 古い世代のまま保存しようとする → 保存せずに MEMORY_RELOADED (ファイルは触らない)
    const before = fs.statSync(pfile).mtimeMs;
    let e2 = null;
    try { P.persistToDisk({ expectGeneration: g0 }); } catch (x) { e2 = x; }
    assert.equal(e2 && e2.code, 'SQLJS_DB_MEMORY_RELOADED', `例外: ${e2 && e2.message}`);
    assert.equal(fs.statSync(pfile).mtimeMs, before, '保存していない');
    assert.equal(P.persistToDisk({ expectGeneration: g0 + 1 }), g0 + 1, '新しい世代で始め直せば保存できる');
    assert.equal(P.persistToDisk(), g0 + 1, 'expectGeneration を渡さなければ従来どおり');
  });
  await t('🚨 profit-calculator/db.js: 保存も読み直しも失敗 (lock が取れない) → 未保存のメモリを検索にも保存にも使わせない (SQLJS_DB_NEEDS_RELOAD)。復旧したら次の操作で読み直し、失敗した変更は永続化されず、もう一度実行すれば通る (Codex #1407 R3)', async () => {
    const P = await import(pathToFileURL(path.join(root, 'apps', 'profit-calculator', 'db.js')).href);
    const pfile = path.join(tmp, 'profit.db');
    const origWarn = console.warn; console.warn = () => {};
    const holder = new Database(lockDbFileOf(pfile));   // 別プロセスの役: lock を持ち続ける
    holder.exec('BEGIN EXCLUSIVE');
    let e = null;
    try {
      try { P.setSyncMeta('k5', 'v5'); } catch (x) { e = x; }
      assert.equal(e && e.code, 'SQLJS_DB_LOCK_TIMEOUT', `保存: ${e && e.message}`);
      assert.equal(P.discardUnsavedChanges(), false, '読み直しも lock で失敗 = false を返す (握りつぶさない)');
      e = null; try { P.getSyncMeta('k5'); } catch (x) { e = x; }
      assert.equal(e && e.code, 'SQLJS_DB_NEEDS_RELOAD', '検索にも使わせない');
      e = null; try { P.setSyncMeta('k5', 'v5'); } catch (x) { e = x; }
      assert.equal(e && e.code, 'SQLJS_DB_NEEDS_RELOAD', '保存にも使わせない');
    } finally { try { holder.exec('ROLLBACK'); } catch { /* */ } holder.close(); }
    try {
      // 復旧: 次の操作で読み直す。失敗した変更 (k5) はメモリから消え、永続化されていない
      assert.ok(!P.getSyncMeta('k5'), '読み直した = 失敗した変更は残らない');
      assert.equal(readFile(pfile, `SELECT count(*) AS c FROM sync_meta WHERE key = 'k5'`)[0].c, 0, 'ファイルにも無い');
      P.setSyncMeta('k5', 'v5');   // やり直し = 通る
      assert.equal(readFile(pfile, `SELECT value FROM sync_meta WHERE key = 'k5'`)[0].value, 'v5');
    } finally { console.warn = origWarn; }
  });
  await t('mercari-sync/settings-db.js: initDb → mercari-settings.db に _file_gen。外から書かれたら setConfig は SQLJS_DB_EXTERNAL_WRITE で上書きしない → やり直せば通る', async () => {
    const M = await import(pathToFileURL(path.join(root, 'apps', 'mercari-sync', 'settings-db.js')).href);
    const mfile = path.join(tmp, 'mercari-settings.db');
    await M.initDb();
    assert.ok(tokenOf(mfile), 'initDb の保存で印が付いている');
    M.setConfig('operation_mode', 'api');
    assert.equal(readFile(mfile, `SELECT value FROM config WHERE key = 'operation_mode'`)[0].value, 'api');
    await tick();
    const X = loadGuarded({ file: mfile, SQL });
    X.db.run(`INSERT OR REPLACE INTO config (key, value) VALUES ('from_x', 'x')`);
    saveGuarded({ file: mfile, db: X.db, gen: X.gen });
    X.db.close();
    await tick();
    const warned = [];
    const origWarn = console.warn;
    console.warn = (...a) => warned.push(a.join(' '));
    let e = null;
    try { M.setConfig('operation_mode', 'csv'); } catch (x) { e = x; } finally { console.warn = origWarn; }
    assert.equal(e && e.code, 'SQLJS_DB_EXTERNAL_WRITE', `例外: ${e && e.message}`);
    assert.match(warned.join('\n'), /mercari-settings\.db/, '警告がログに出る');
    assert.equal(readFile(mfile, `SELECT value FROM config WHERE key = 'from_x'`)[0].value, 'x', 'X の行は消えていない');
    assert.equal(readFile(mfile, `SELECT value FROM config WHERE key = 'operation_mode'`)[0].value, 'api', '保存されていない');
    assert.equal(M.getConfig('from_x'), 'x', '読み直したメモリに X の行が入っている');
    M.setConfig('operation_mode', 'csv');   // やり直し
    assert.equal(readFile(mfile, `SELECT value FROM config WHERE key = 'operation_mode'`)[0].value, 'csv');
    assert.equal(readFile(mfile, `SELECT value FROM config WHERE key = 'from_x'`)[0].value, 'x');
  });
  await t('🚨 mercari-sync/settings-db.js: 保存が lock で失敗 → 読み直すまで検索にも保存にも使わせない (SQLJS_DB_NEEDS_RELOAD) → 復旧したら次の操作で読み直し、失敗した変更は残らない', async () => {
    const M = await import(pathToFileURL(path.join(root, 'apps', 'mercari-sync', 'settings-db.js')).href);
    const mfile = path.join(tmp, 'mercari-settings.db');
    const origWarn = console.warn; console.warn = () => {};
    const holder = new Database(lockDbFileOf(mfile));
    holder.exec('BEGIN EXCLUSIVE');
    let e = null;
    try {
      try { M.setConfig('operation_mode', 'api'); } catch (x) { e = x; }
      assert.equal(e && e.code, 'SQLJS_DB_LOCK_TIMEOUT', `保存: ${e && e.message}`);
      e = null; try { M.getConfig('operation_mode'); } catch (x) { e = x; }
      assert.equal(e && e.code, 'SQLJS_DB_NEEDS_RELOAD', '検索にも使わせない');
    } finally { try { holder.exec('ROLLBACK'); } catch { /* */ } holder.close(); }
    try {
      assert.equal(M.getConfig('operation_mode'), 'csv', '読み直した = 失敗した変更 (api) は残らない');
      M.setConfig('operation_mode', 'api');
      assert.equal(readFile(mfile, `SELECT value FROM config WHERE key = 'operation_mode'`)[0].value, 'api');
      M.setConfig('operation_mode', 'csv');
    } finally { console.warn = origWarn; }
  });
  await t('🚨 mercari-sync の設定 POST (Express 4 の async): カテゴリ JSON の誤りは saved=1 にせず ?error= / 保存の失敗 (外から書かれた) は 500 で応答する (ぶら下がらない。Codex #1407 R2 #1)', async () => {
    const express = (await import('express')).default;
    const http = await import('node:http');
    const routerMod = await import(pathToFileURL(path.join(root, 'apps', 'mercari-sync', 'router.js')).href);
    const app = express();
    app.use(express.urlencoded({ extended: true }));
    app.use('/apps/mercari-sync', routerMod.default);
    const server = http.createServer(app);
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    const port = server.address().port;
    const post = (body) => new Promise((resolve, reject) => {
      const req = http.request({ host: '127.0.0.1', port, path: '/apps/mercari-sync/settings', method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' } }, (res) => { let d = ''; res.on('data', (c) => { d += c; }); res.on('end', () => resolve({ status: res.statusCode, location: res.headers.location || '', body: d })); });
      req.setTimeout(5000, () => { req.destroy(new Error('応答が無い (ぶら下がっている)')); });
      req.on('error', reject);
      req.end(body);
    });
    const mfile = path.join(tmp, 'mercari-settings.db');
    const origError = console.error; const origWarn = console.warn; console.error = () => {}; console.warn = () => {};
    try {
      const okRes = await post('operation_mode=csv&category_mappings_json=%5B%5D');
      assert.deepEqual([okRes.status, /saved=1/.test(okRes.location)], [302, true]);
      const badJson = await post('operation_mode=csv&category_mappings_json=%7Bnot');
      assert.deepEqual([badJson.status, /error=category_mappings_json/.test(badJson.location), /saved=1/.test(badJson.location)], [302, true, false]);
      // 外のプロセスが書いた後の POST = setConfig が SQLJS_DB_EXTERNAL_WRITE → next(err) → 500 (応答が返る)
      await tick();
      const X = loadGuarded({ file: mfile, SQL });
      X.db.run(`INSERT OR REPLACE INTO config (key, value) VALUES ('from_y', 'y')`);
      saveGuarded({ file: mfile, db: X.db, gen: X.gen });
      X.db.close();
      await tick();
      const failed = await post('operation_mode=api&category_mappings_json=%5B%5D');
      assert.deepEqual([failed.status, /saved=1/.test(failed.location), /SQLJS_DB_EXTERNAL_WRITE|上書きしなかった/.test(failed.body)], [500, false, true]);
      assert.equal(readFile(mfile, `SELECT value FROM config WHERE key = 'from_y'`)[0].value, 'y', 'Y の行は消えていない');
      const again = await post('operation_mode=api&category_mappings_json=%5B%5D');   // 読み直した後なので通る
      assert.deepEqual([again.status, /saved=1/.test(again.location)], [302, true]);
    } finally { console.error = origError; console.warn = origWarn; await new Promise((r) => server.close(r)); }
  });
} finally {
  // 🚨 Windows では開いたままの DB があると消せない。開いた接続は各試験で閉じている
  try { fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 }); } catch (e) { console.log('  (一時ディレクトリを消せなかった: ' + e.message + ')'); }
}

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
