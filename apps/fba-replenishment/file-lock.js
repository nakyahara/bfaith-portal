/**
 * file-lock.js — fba.db の「読む」「確かめて書く」をプロセス間で 1 つずつにする lock (db.js から切り出した。中身は同じ)。
 *
 * 使う所:
 *   - apps/fba-replenishment/db.js (保存・読み込み)
 *   - apps/company-db/push/stock-daily.mjs (FBA の在庫の日次を fba.db から **読むだけ** の別プロセス。常駐サーバが保存している最中のファイルを読まないために同じ lock を取る)
 *
 * 🚨 自前の lock ファイル (排他で作る → 古ければ捨てる) にしない (Codex #1376 R2 #1・#2): 「古い」と判定してから捨てるまでの間に、
 *    別の待ち手が作り直した **有効な lock を消してしまう** 順序があり、捨てるのに失敗すると待ちが終わらない。
 *    → **SQLite のファイルロックに任せる**: lock 専用の小さな DB (中身は使わない) に BEGIN EXCLUSIVE。OS が管理し、持ち主のプロセスが死ねば自動で外れる
 *      (= 古い lock の回収という処理そのものが要らない)。待つのは better-sqlite3 の timeout (waitMs。Windows では 1 秒刻み) まで。同期
 *    接続は毎回開いて閉じる (1ms ほど): 開いたままだと Windows では data フォルダを消せない・閉じれば lock は確実に外れる
 * 🚨 lock を取れない理由は全部 FBA_DB_* にする (開けない = SQLITE_CANTOPEN・読み取り専用・I/O も)。素の SQLITE_* のまま出すと、
 *    保存の失敗を警告に落とす catch (isFbaDbConflict で投げ直す側) を素通りして「保存していないのに成功」になる (Codex #1376 R3 #1)
 */
import BetterSqlite from 'better-sqlite3';

/** fba.db のパス → lock 専用の DB のパス (db.js と送り手で同じ決め方) */
export const lockDbFileOf = (dbFile) => dbFile + '.lockdb';

export function withSqliteFileLock(lockDbFile, waitMs, fn) {
  let l;
  try { l = new BetterSqlite(lockDbFile, { timeout: waitMs }); }
  catch (e) { throw Object.assign(new Error(`fba.db の lock 用のファイルを開けない (${lockDbFile}: ${e.code || ''} ${e.message})。この操作は保存されていない`), { code: 'FBA_DB_LOCK_ERROR', cause: e }); }
  try {
    try {
      l.exec('BEGIN EXCLUSIVE');
    } catch (e) {
      if (e && (e.code === 'SQLITE_BUSY' || e.code === 'SQLITE_BUSY_SNAPSHOT')) {
        throw Object.assign(new Error(`fba.db をほかのプロセスが保存中で、${waitMs / 1000} 秒待っても順番が来なかった。この操作は保存されていない (メモリには残っている = 次の保存で一緒に書かれる)。もう一度実行する`), { code: 'FBA_DB_LOCK_TIMEOUT', cause: e });
      }
      throw Object.assign(new Error(`fba.db の lock を取れない (${e.code || ''} ${e.message})。この操作は保存されていない`), { code: 'FBA_DB_LOCK_ERROR', cause: e });
    }
    return fn();
  } finally {
    // 閉じれば (取引が開いていても) OS が lock を外す。閉じ損ねは記録する (元の例外は隠さない)
    try { l.close(); } catch (e) { console.error('[fba-db] lock 用の接続を閉じられない:', e.message); }
  }
}
