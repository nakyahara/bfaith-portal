/**
 * build-lock.js — LINEギフト v1.0 Commit 2
 *   mart_build_locks ベースの排他制御ヘルパー
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §6
 * Codex v1.0 Round 2 Critical 2 (排他 + ロールバック) 反映
 *
 * file lock を採用しない理由:
 *   - Render の re-deploy で file lock が残るケースがある (stale)
 *   - DB lock なら heartbeat_at で生存確認、lease_ttl で自動失効が可能
 *   - HTTP 二重起動を SQL UNIQUE 違反で検知可能 (409 相当)
 *
 * 使い方:
 *   import { acquireLock, releaseLock, updateHeartbeat, withLock } from './build-lock.js';
 *
 *   const lock = acquireLock(db, 'build-linegift-analytics-mart', { acquired_by: 'pid:' + process.pid });
 *   if (!lock) process.exit(2);   // 409 Conflict (他プロセスが保持中)
 *   try {
 *     const hb = setInterval(() => updateHeartbeat(db, lock.lock_name), 30_000);
 *     // ... build logic ...
 *     clearInterval(hb);
 *     releaseLock(db, lock.lock_name, 'COMPLETED');
 *   } catch (e) {
 *     releaseLock(db, lock.lock_name, 'FAILED', e.message);
 *     throw e;
 *   }
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const SQL_FILE = path.join(REPO_ROOT, 'sql/linegift-analytics/mart_build_locks.sql');

function jstWallclock() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

export function nowJstIso() {
  const j = jstWallclock();
  const yy = j.getUTCFullYear();
  const mm = String(j.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(j.getUTCDate()).padStart(2, '0');
  const hh = String(j.getUTCHours()).padStart(2, '0');
  const mi = String(j.getUTCMinutes()).padStart(2, '0');
  const ss = String(j.getUTCSeconds()).padStart(2, '0');
  return `${yy}-${mm}-${dd}T${hh}:${mi}:${ss}+09:00`;
}

/**
 * テーブル存在確認 + 未作成なら DDL 適用 (idempotent、初回起動安全)
 */
export function ensureLockTables(db) {
  const exists = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='mart_build_locks'`).get();
  if (exists) return;
  if (!fs.existsSync(SQL_FILE)) {
    throw new Error(`[build-lock] DDL file not found: ${SQL_FILE}`);
  }
  const sql = fs.readFileSync(SQL_FILE, 'utf8');
  db.exec(sql);
}

/**
 * stale lock を EXPIRED に更新 (heartbeat_at + lease_ttl_seconds < now の RUNNING を失効)
 * 内部で acquireLock の前段として呼ばれる。
 */
function reapStaleLocks(db, now) {
  // SQLite: heartbeat_at は ISO8601 文字列、lease_ttl_seconds は数値
  // datetime(heartbeat_at, '+' || lease_ttl_seconds || ' seconds') < datetime(:now) で判定
  const stmt = db.prepare(`
    UPDATE mart_build_locks
    SET status = 'EXPIRED',
        synced_at = ?
    WHERE status = 'RUNNING'
      AND datetime(heartbeat_at, '+' || lease_ttl_seconds || ' seconds') < datetime(?)
  `);
  const info = stmt.run(now, now);
  return info.changes;
}

/**
 * lock 取得 (RUNNING な lock が既にあれば失敗、stale は EXPIRED 経由で取り直す)
 * @returns { lock_name, acquired_by, acquired_at } | null (null = 既存 RUNNING あり、409 相当)
 */
export function acquireLock(db, lockName, opts = {}) {
  ensureLockTables(db);
  const now = nowJstIso();
  const acquired_by = opts.acquired_by || `pid:${process.pid}`;
  const lease_ttl_seconds = opts.lease_ttl_seconds || 600;

  // SCD2: stale 検知 → RUNNING 確認 → 新規 acquire を transaction 内で atomic に
  const txn = db.transaction(() => {
    reapStaleLocks(db, now);

    // 既存 RUNNING がいるか
    const running = db.prepare(`SELECT lock_name FROM mart_build_locks WHERE lock_name=? AND status='RUNNING'`).get(lockName);
    if (running) {
      return null; // 既に保持中
    }

    // 過去 record (COMPLETED/FAILED/EXPIRED) があれば DELETE してから INSERT
    // PK が lock_name なので、INSERT OR REPLACE でも可能だが、履歴を残すため
    // mart_build_lock_history に移してから新規 INSERT
    const old = db.prepare(`SELECT * FROM mart_build_locks WHERE lock_name=?`).get(lockName);
    if (old) {
      const duration = Math.max(0, Math.floor((Date.parse(now) - Date.parse(old.acquired_at)) / 1000));
      db.prepare(`
        INSERT INTO mart_build_lock_history
          (lock_name, acquired_by, acquired_at, released_at, status, duration_seconds, error_message, synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(old.lock_name, old.acquired_by, old.acquired_at, now, old.status, duration, old.error_message ?? null, now);
      db.prepare(`DELETE FROM mart_build_locks WHERE lock_name=?`).run(lockName);
    }

    db.prepare(`
      INSERT INTO mart_build_locks
        (lock_name, acquired_by, acquired_at, heartbeat_at, lease_ttl_seconds, status, synced_at)
      VALUES (?, ?, ?, ?, ?, 'RUNNING', ?)
    `).run(lockName, acquired_by, now, now, lease_ttl_seconds, now);

    return { lock_name: lockName, acquired_by, acquired_at: now, lease_ttl_seconds };
  });

  return txn();
}

/**
 * heartbeat 更新 (build 中 30s 間隔で呼ぶ、stale 失効を防止)
 */
export function updateHeartbeat(db, lockName) {
  const now = nowJstIso();
  const info = db.prepare(`
    UPDATE mart_build_locks
    SET heartbeat_at = ?, synced_at = ?
    WHERE lock_name = ? AND status = 'RUNNING'
  `).run(now, now, lockName);
  return info.changes > 0;
}

/**
 * lock 解放 (status を COMPLETED または FAILED に更新)
 * 履歴は次回 acquireLock 時に mart_build_lock_history へ移動する。
 */
export function releaseLock(db, lockName, status = 'COMPLETED', errorMessage = null) {
  if (!['COMPLETED', 'FAILED'].includes(status)) {
    throw new Error(`invalid release status: ${status} (must be COMPLETED|FAILED)`);
  }
  const now = nowJstIso();
  db.prepare(`
    UPDATE mart_build_locks
    SET status = ?, error_message = ?, synced_at = ?
    WHERE lock_name = ? AND status = 'RUNNING'
  `).run(status, errorMessage, now, lockName);
}

/**
 * 高水準ヘルパー: lock を取って fn 実行、終了時に必ず release
 * fn 内で throw した場合は status='FAILED' で release してから re-throw
 * @returns fn の戻り値 (取得失敗時は { ok: false, reason: 'lock_held' })
 */
export async function withLock(db, lockName, fn, opts = {}) {
  const lock = acquireLock(db, lockName, opts);
  if (!lock) {
    return { ok: false, reason: 'lock_held', lockName };
  }

  let heartbeat = null;
  if (opts.heartbeat_interval_ms !== 0) {
    const interval = opts.heartbeat_interval_ms || 30_000;
    heartbeat = setInterval(() => {
      try { updateHeartbeat(db, lockName); } catch (_) { /* swallow */ }
    }, interval);
  }

  try {
    const result = await fn(lock);
    if (heartbeat) clearInterval(heartbeat);
    releaseLock(db, lockName, 'COMPLETED');
    return { ok: true, lock, result };
  } catch (e) {
    if (heartbeat) clearInterval(heartbeat);
    releaseLock(db, lockName, 'FAILED', e?.message || String(e));
    throw e;
  }
}

/**
 * 現在の lock 状態を取得 (UI 表示用)
 */
export function getLockStatus(db, lockName) {
  return db.prepare(`SELECT * FROM mart_build_locks WHERE lock_name=?`).get(lockName) || null;
}

/**
 * lock 履歴を取得 (UI 表示用、直近 N 件)
 */
export function getLockHistory(db, lockName, limit = 10) {
  return db.prepare(`
    SELECT * FROM mart_build_lock_history
    WHERE lock_name = ?
    ORDER BY released_at DESC
    LIMIT ?
  `).all(lockName, limit);
}
