/**
 * job-locks.js — concurrency guard (Phase 1 #1-7a)
 *
 * SQLite (warehouse.db) ベースの軽量 lock。daily-sync / mart rebuild / sync 等の
 * 重複起動防止を目的とする。分散ロックは想定外、単一 miniPC 前提。
 *
 * 使い方:
 *   import { acquireLock, heartbeatLock, releaseLock, listLocks } from './job-locks.js';
 *
 *   const lock = acquireLock(db, 'daily-sync', { ttlMs: 30 * 60 * 1000 });
 *   if (!lock) {
 *     console.error('Another instance is running');
 *     process.exit(73);  // EX_CANTCREAT
 *   }
 *   try {
 *     // ジョブ実行
 *     // 長時間ジョブの場合は定期的に heartbeat
 *     setInterval(() => heartbeatLock(db, lock), 60 * 1000);
 *   } finally {
 *     releaseLock(db, lock);
 *   }
 *
 * Lock 仕様:
 *   - PK: job_name (1 job 1 lock)
 *   - holder_id = `${hostname}-${pid}-${uuid}`
 *   - acquired_at: 取得時刻 (ISO8601 UTC)
 *   - expires_at: 取得時刻 + ttlMs
 *   - heartbeat_at: 最終 heartbeat 時刻
 *
 * acquire 戦略:
 *   1. 既存 lock が無い → INSERT
 *   2. 既存 lock があり expires_at < now() → DELETE して再 INSERT (takeover)
 *   3. 既存 lock があり expires_at >= now() → 失敗 (null 返す)
 *
 * heartbeat 戦略:
 *   - holder_id 一致時のみ expires_at と heartbeat_at を更新
 *   - holder_id 不一致なら何もしない (takeover 済の可能性)
 *
 * Exit code 規約 (caller 側):
 *   - 73: EX_CANTCREAT (lock 取得失敗、既に別 instance 実行中)
 */

import os from 'node:os';
import crypto from 'node:crypto';

const DEFAULT_TTL_MS = 30 * 60 * 1000;  // 30 分

function nowIso() {
  return new Date().toISOString();
}

function generateHolderId() {
  const hostname = os.hostname();
  const pid = process.pid;
  const uuid = crypto.randomUUID();
  return `${hostname}-${pid}-${uuid}`;
}

/**
 * Lock を取得。
 *
 * @param {Database} db - better-sqlite3 instance (warehouse.db)
 * @param {string} jobName - lock 名 (例: 'daily-sync', 'rebuild-amazon-finance-mart')
 * @param {object} opts
 * @param {number} opts.ttlMs - lock 有効期限 (default: 30 分)
 * @param {string} opts.holderId - 任意の holder ID (省略時は自動生成)
 * @returns {object|null} - 成功時 {jobName, holderId, acquiredAt, expiresAt}、失敗時 null
 */
export function acquireLock(db, jobName, opts = {}) {
  const ttlMs = opts.ttlMs || DEFAULT_TTL_MS;
  const holderId = opts.holderId || generateHolderId();
  const acquiredAt = nowIso();
  const expiresAt = new Date(Date.now() + ttlMs).toISOString();

  // 既存 lock 確認 + takeover/insert を同一 transaction で
  const result = db.transaction(() => {
    const existing = db.prepare(`SELECT * FROM job_locks WHERE job_name = ?`).get(jobName);
    const now = nowIso();

    if (existing) {
      if (existing.expires_at >= now) {
        // 有効な lock 存在、取得失敗
        return null;
      }
      // TTL 切れ → takeover
      db.prepare(`DELETE FROM job_locks WHERE job_name = ?`).run(jobName);
    }

    db.prepare(`
      INSERT INTO job_locks (job_name, holder_id, acquired_at, expires_at, heartbeat_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(jobName, holderId, acquiredAt, expiresAt, acquiredAt);

    return { jobName, holderId, acquiredAt, expiresAt };
  })();

  return result;
}

/**
 * Lock の有効期限を延長 (heartbeat)。holder_id 一致時のみ更新。
 *
 * @param {Database} db
 * @param {object} lock - acquireLock() の戻り値
 * @param {object} opts
 * @param {number} opts.extendMs - 延長時間 (default: 30 分)
 * @returns {boolean} - 成功時 true、holder_id 不一致 (lock 失効済) 時 false
 */
export function heartbeatLock(db, lock, opts = {}) {
  const extendMs = opts.extendMs || DEFAULT_TTL_MS;
  const heartbeatAt = nowIso();
  const newExpiresAt = new Date(Date.now() + extendMs).toISOString();

  const info = db.prepare(`
    UPDATE job_locks
       SET expires_at = ?, heartbeat_at = ?
     WHERE job_name = ? AND holder_id = ?
  `).run(newExpiresAt, heartbeatAt, lock.jobName, lock.holderId);

  if (info.changes > 0) {
    lock.expiresAt = newExpiresAt;
    return true;
  }
  return false;  // takeover されてた or release 済
}

/**
 * Lock を release。holder_id 一致時のみ DELETE。
 *
 * @param {Database} db
 * @param {object} lock - acquireLock() の戻り値
 * @returns {boolean} - 成功時 true、holder_id 不一致時 false
 */
export function releaseLock(db, lock) {
  const info = db.prepare(`
    DELETE FROM job_locks WHERE job_name = ? AND holder_id = ?
  `).run(lock.jobName, lock.holderId);
  return info.changes > 0;
}

/**
 * 現在の lock 一覧を取得。
 *
 * @param {Database} db
 * @returns {Array<object>}
 */
export function listLocks(db) {
  return db.prepare(`
    SELECT
      job_name,
      holder_id,
      acquired_at,
      expires_at,
      heartbeat_at,
      CASE WHEN expires_at < ? THEN 'EXPIRED (stale)' ELSE 'ACTIVE' END AS status
    FROM job_locks
    ORDER BY job_name
  `).all(nowIso());
}

/**
 * Stale lock (TTL 切れ) を一括削除。
 * 通常 acquire 時に takeover されるが、長期間 acquire されない job の lock 残骸を掃除。
 *
 * @param {Database} db
 * @returns {number} - 削除件数
 */
export function cleanupStaleLocks(db) {
  const info = db.prepare(`DELETE FROM job_locks WHERE expires_at < ?`).run(nowIso());
  return info.changes;
}
