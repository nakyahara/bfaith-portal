/**
 * sqljs-guard.js — sql.js の DB (= ファイル全体をメモリに読み、保存は **ファイル全体** を書き戻す) を、
 * 「ほかのプロセスが書いた行を黙って上書きして消す」事故から守る共通の歯止め。
 *
 * 事故 (2026-09-20, fba.db): cron と常駐サーバの 2 プロセスが同じ sql.js のファイルをメモリに持ち、後から保存した側が
 * 相手の入れた行をファイルごと消した (27 日ぶん)。PR #1376 が apps/fba-replenishment/db.js に歯止めを入れた。
 * 同じ形のファイルが profit.db (apps/profit-calculator/db.js) と mercari-settings.db (apps/mercari-sync/settings-db.js) にもある
 * (書き手は今のところ常駐サーバ 1 つ = 潜在の危険) ので、その歯止めをここに共通化した。
 *
 * 仕組み (fba-replenishment/db.js と同じ):
 *   ① プロセス間の lock = データファイルの隣の小さな SQLite (<file>.lockdb) に BEGIN EXCLUSIVE (apps/fba-replenishment/file-lock.js)。
 *      「読む」「確かめて書く」を全部この lock の中でやる (確かめた後に相手が書く窓を無くす)。render-backup の VACUUM も同じ lock を取る
 *   ② ファイルの中の世代の印 (_file_gen.token = 保存のたびに新しい乱数) + 更新時刻とサイズ。
 *      読んだ / 書いた時点の印と、いま保存しようとしているファイルの印が違えば「外から書かれた」= **上書きしない** (SQLJS_DB_EXTERNAL_WRITE)
 *   ③ 書きかけのファイル (ヘッダの言う大きさに足りない) は読まない (SQLJS_DB_FILE_TORN)。保存は一時ファイルに書いて rename (途中で死んでも元のファイルは残る)
 *
 * 🚨 fba-replenishment/db.js はこのモジュールに **切り替えていない** (判断 2026-09-22):
 *    あちらの歯止めはモジュールの状態 (競合したら読み直して未保存の変更を捨てる・memGeneration・_testHooks・FBA_DB_* のコード) と
 *    絡んでいて、関数をそのまま移すだけでは済まない。稼働中の fba.db を退行させる危険のほうが大きいので、同じ意味の実装をここに置き、
 *    新しい呼び出し元 (profit-calculator / mercari-sync / render-backup) だけがこれを使う。lock 本体 (file-lock.js) は共有 = 同じファイルには同じ lock。
 *
 * 使い方 (呼び出し元は gen の中を見ない):
 *   const { db, gen } = loadGuarded({ file, SQL });   // SQL = await initSqlJs()。lock の中で読む。書きかけなら SQLJS_DB_FILE_TORN
 *   gen = saveGuarded({ file, db, gen });             // 外から書かれていたら SQLJS_DB_EXTERNAL_WRITE を投げて **書かない**
 *   → 投げられたら、呼び出し元は loadGuarded で読み直してから (= 未保存の変更は捨てて) やり直す
 * 例外のコードは全部 SQLJS_DB_* (EXTERNAL_WRITE / FILE_TORN / LOCK_TIMEOUT / LOCK_ERROR / SAVE_FAILED) = どれも「その操作は保存されていない」。
 * 🚨 保存の失敗を警告に落として先へ進む catch は、これだけは投げ直す (握りつぶすと「保存していないのに成功」になる)
 */
import BetterSqlite from 'better-sqlite3';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { withSqliteFileLock, lockDbFileOf } from '../apps/fba-replenishment/file-lock.js';

export { lockDbFileOf };

// 相手の保存 (100MB で 1〜2 秒) を待つ上限。🚨 待つ間は同期 = このプロセスのイベントループが止まる。長く待たずに失敗させる。env は試験用
export const DEFAULT_LOCK_WAIT_MS = Number(process.env.SQLJS_DB_LOCK_WAIT_MS) || 5000;

const err = (code, message, cause) => Object.assign(new Error(message), cause ? { code, cause } : { code });

/** この歯止めの例外か (= その操作は保存されていない) */
export const isSqljsGuardError = (e) => !!e && typeof e.code === 'string' && e.code.startsWith('SQLJS_DB_');

/**
 * データファイルの lock (<file>.lockdb) を取って fn を実行する。読み手 (render-backup の VACUUM) も書き手もこれを通す。
 * file-lock.js の例外は FBA_DB_LOCK_* (fba.db 前提の文言) なので、ここで SQLJS_DB_LOCK_* + 実際のファイル名に読み替える
 */
export function withSqljsFileLock(file, fn, { lockFile = lockDbFileOf(file), waitMs = DEFAULT_LOCK_WAIT_MS } = {}) {
  try {
    return withSqliteFileLock(lockFile, waitMs, fn);
  } catch (e) {
    const name = path.basename(file);
    if (e && e.code === 'FBA_DB_LOCK_TIMEOUT') throw err('SQLJS_DB_LOCK_TIMEOUT', `${name} をほかのプロセスが使っている最中で、${waitMs / 1000} 秒待っても順番が来なかった。この操作は保存されていない。もう一度実行する`, e);
    if (e && e.code === 'FBA_DB_LOCK_ERROR') throw err('SQLJS_DB_LOCK_ERROR', `${name} の lock を取れない (${lockFile}: ${(e.cause && e.cause.code) || ''} ${(e.cause && e.cause.message) || e.message})。この操作は保存されていない`, e);
    throw e;
  }
}

/** ファイルのいまの姿 { mtimeMs, size }。無ければ null */
function stampOf(file) {
  try { const st = fs.statSync(file); return { mtimeMs: st.mtimeMs, size: st.size }; }
  catch (e) { if (e.code === 'ENOENT') return null; throw e; }
}
const sameStamp = (a, b) => (a === null || b === null ? a === b : a.mtimeMs === b.mtimeMs && a.size === b.size);

/** SQLite のヘッダが言う大きさ (ページの大きさ × ページ数) に、実際の長さが足りているか。足りない = 書いている途中で止まったファイル */
export function isTornSqlite(headerBytes, actualSize) {
  if (actualSize < 100 || headerBytes.length < 100) return true;
  if (headerBytes.toString('latin1', 0, 15) !== 'SQLite format 3') return true;
  const ps = headerBytes.readUInt16BE(16);
  const pageSize = ps === 1 ? 65536 : ps;
  const pages = headerBytes.readUInt32BE(28);
  return pages > 0 && actualSize < pageSize * pages;
}

/**
 * いまのファイル: null (無い) | { stamp, token, torn }。lock の中で呼ぶ。
 * 世代の印 1 行だけを better-sqlite3 で読む (sql.js はファイル全体を読まないと開けない)。接続はすぐ閉じる (🚨 Windows では開いたままだと rename できない)
 */
export function inspectSqljsFile(file) {
  const stamp = stampOf(file);
  if (!stamp) return null;
  const head = Buffer.alloc(100);
  const fd = fs.openSync(file, 'r');
  try { fs.readSync(fd, head, 0, 100, 0); } finally { fs.closeSync(fd); }
  if (isTornSqlite(head, stamp.size)) return { stamp, token: null, torn: true };
  let f = null;
  try {
    f = new BetterSqlite(file, { readonly: true, fileMustExist: true });
    const has = f.prepare(`SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = '_file_gen'`).get();
    const row = has ? f.prepare('SELECT token FROM _file_gen WHERE id = 1').get() : null;
    return { stamp, token: row ? row.token : null, torn: false };
  } catch (e) {
    if (e && (e.code === 'SQLITE_NOTADB' || e.code === 'SQLITE_CORRUPT')) return { stamp, token: null, torn: true };
    throw e;
  } finally {
    try { f?.close(); } catch { /* 閉じられなくても読めた結果は使える */ }
  }
}

/** 前回の保存が途中で死んで残った一時ファイル (<file>.<pid>.<乱数>.tmp) を片づける。lock の中で呼ぶ (= いま書いている相手はいない) */
function removeStaleTemps(file) {
  const dir = path.dirname(file);
  const prefix = path.basename(file) + '.';
  let names = [];
  try { names = fs.readdirSync(dir); } catch { return; }
  for (const n of names) {
    if (n.startsWith(prefix) && /\.\d+\.[0-9a-f]+\.tmp$/.test(n)) {
      try { fs.unlinkSync(path.join(dir, n)); } catch { /* 消せなくても保存には影響しない */ }
    }
  }
}

/**
 * ファイルを lock の中で読んで sql.js の DB にする → { db, gen }。ファイルが無ければ空の DB (gen.stamp = null = 失うものが無い)。
 * 書きかけのファイルは読まない (SQLJS_DB_FILE_TORN)。_file_gen の無い古いファイルも読める (印は最初の保存で付く)。
 * 🚨 ファイルは書き換えない (読むだけ)
 */
export function loadGuarded({ file, SQL, lockFile = lockDbFileOf(file), waitMs = DEFAULT_LOCK_WAIT_MS }) {
  if (!file) throw new Error('loadGuarded: file が要る');
  if (!SQL || typeof SQL.Database !== 'function') throw new Error('loadGuarded: SQL (= await initSqlJs() の結果) が要る');
  return withSqljsFileLock(file, () => {
    removeStaleTemps(file);
    const f = inspectSqljsFile(file);
    if (f && f.torn) {
      throw err('SQLJS_DB_FILE_TORN', `${path.basename(file)} が書きかけで止まっている (長さ ${f.stamp.size} がヘッダの言う大きさに足りない、または SQLite として読めない)。読み込まない = 控え (render-backup) から戻すか、正しいメモリを持つプロセスに保存させる (${file})`);
    }
    const db = f ? new SQL.Database(fs.readFileSync(file)) : new SQL.Database();
    return { db, gen: { token: f ? f.token : null, stamp: f ? f.stamp : null } };
  }, { lockFile, waitMs });
}

/**
 * メモリの DB をファイルへ書き戻す → 新しい gen。lock の中で「確かめる → 書く」:
 *   ファイルの印 / 更新時刻 / サイズが gen と違う = 読んだ後に外から書かれた → **書かずに** SQLJS_DB_EXTERNAL_WRITE (メモリの印も変えない)
 *   ファイルが無い → 失うものが無いので書く / 書きかけ → 守る中身が無いので、このメモリで書き直す
 * 書き方: 新しい印を _file_gen に入れて export → 一時ファイルに書いて rename。
 *   🚨 Windows では、ほかのプロセスがファイルを開いたままだと rename が EPERM になる (SQLite は FILE_SHARE_DELETE を付けない)。
 *   lock を取らない読み手 (DB ビューアなど) がいる間も保存できなくならないよう、そのときだけ従来どおり直接書く (警告を出す)
 */
export function saveGuarded({ file, db, gen, lockFile = lockDbFileOf(file), waitMs = DEFAULT_LOCK_WAIT_MS, log = console }) {
  if (!file) throw new Error('saveGuarded: file が要る');
  if (!db) throw new Error('saveGuarded: db が要る');
  const known = gen || { token: null, stamp: null };
  const name = path.basename(file);
  try {
    return withSqljsFileLock(file, () => {
      const f = inspectSqljsFile(file);
      if (f && f.torn) {
        log.warn(`[sqljs-guard] ${name} が書きかけで止まっていた (長さ ${f.stamp.size})。このプロセスのメモリで書き直す`);
      } else if (f && (f.token !== known.token || !sameStamp(f.stamp, known.stamp))) {
        throw err('SQLJS_DB_EXTERNAL_WRITE', `${name} がほかのプロセスに書き換えられていたので上書きしなかった (最後に読んだ / 書いた時点 ${known.stamp ? new Date(known.stamp.mtimeMs).toISOString() : 'なし'} → いま ${new Date(f.stamp.mtimeMs).toISOString()})。この操作は保存されていない = ファイルを読み直してから、もう一度実行する`);
      }
      const token = crypto.randomUUID();
      db.run('CREATE TABLE IF NOT EXISTS _file_gen (id INTEGER PRIMARY KEY CHECK (id = 1), token TEXT NOT NULL)');
      db.run('INSERT OR REPLACE INTO _file_gen (id, token) VALUES (1, ?)', [token]);
      const data = Buffer.from(db.export());
      const tmp = `${file}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`;
      try {
        fs.writeFileSync(tmp, data);
        fs.renameSync(tmp, file);
      } catch (e) {
        try { fs.unlinkSync(tmp); } catch { /* 無ければよい */ }
        if (process.platform === 'win32' && f && (e.code === 'EPERM' || e.code === 'EBUSY' || e.code === 'EACCES')) {
          log.warn(`[sqljs-guard] ${name} を rename で置き換えられない (${e.code}: ほかのプロセスが開いたまま?)。直接書き戻す`);
          fs.writeFileSync(file, data);
        } else {
          throw e;
        }
      }
      return { token, stamp: stampOf(file) };
    }, { lockFile, waitMs });
  } catch (e) {
    // 保存の途中のどんな失敗 (ファイルの検査・書き込みの I/O) も「保存されていない」= SQLJS_DB_* にそろえる
    if (isSqljsGuardError(e)) throw e;
    throw err('SQLJS_DB_SAVE_FAILED', `${name} を保存できなかった (${(e && e.code) || ''} ${(e && e.message) || e})。この操作は保存されていない`, e);
  }
}
