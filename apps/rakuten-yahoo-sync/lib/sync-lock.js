/**
 * file-based lock (notion-sync の同時起動防止)。
 * ローカル RYS src/lib/sync-lock.js の ES module 翻訳 (Codex Phase B1 R1-R4 完成版)。
 *
 * 設計:
 *   - fs.openSync(path, 'wx') で原子的に lockfile を作る (race-free)
 *   - 既存 lockfile があれば 生死 + TTL 判定:
 *       alive (pid 生存)              → 奪取拒否 (SyncLockError reason='alive')
 *       dead pid                       → 奪取 (stale lock cleanup → 再 atomic create)
 *       alive かつ TTL 超過            → 奪取拒否 reason='alive-stale' (操作員 kill 待ち)
 *       破損 lockfile (parse 不能)     → fail-close reason='invalid-lockfile'
 *       IO エラー (EACCES 等)          → fail-close reason='lockfile-io-error'
 */

import fs from 'fs';
import path from 'path';
import os from 'os';

export const DEFAULT_TTL_MS = 30 * 60 * 1000;

export class SyncLockError extends Error {
  constructor(message, { holder = null, reason = null } = {}) {
    super(message);
    this.name = 'SyncLockError';
    this.holder = holder;
    this.reason = reason;
  }
}

export function isProcessAlive(pid) {
  if (!pid || !Number.isInteger(pid)) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return e.code === 'EPERM';
  }
}

export function readLockfile(lockPath) {
  try {
    const raw = fs.readFileSync(lockPath, 'utf8');
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

export function inspectLockfile(lockPath) {
  let raw;
  try {
    raw = fs.readFileSync(lockPath, 'utf8');
  } catch (e) {
    if (e.code === 'ENOENT') return { present: false };
    return { present: true, valid: false, kind: 'io', error: e };
  }
  try {
    return { present: true, valid: true, data: JSON.parse(raw) };
  } catch (e) {
    return { present: true, valid: false, kind: 'parse', error: e };
  }
}

function tryAtomicCreate(lockPath, payload) {
  const fd = fs.openSync(lockPath, 'wx');
  try {
    fs.writeSync(fd, JSON.stringify(payload));
  } finally {
    fs.closeSync(fd);
  }
}

function makeReleaser(lockPath, myPid) {
  let released = false;
  return function release() {
    if (released) return;
    released = true;
    try {
      const current = readLockfile(lockPath);
      if (current && current.pid === myPid) {
        fs.unlinkSync(lockPath);
      }
    } catch (_) {
      // best-effort
    }
  };
}

export function acquire(lockPath, { ttlMs = DEFAULT_TTL_MS } = {}) {
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  const myPid = process.pid;
  const payload = {
    pid: myPid,
    host: os.hostname(),
    acquiredAt: new Date().toISOString(),
  };

  // 1st attempt: atomic create
  try {
    tryAtomicCreate(lockPath, payload);
    return makeReleaser(lockPath, myPid);
  } catch (e) {
    if (e.code !== 'EEXIST') throw e;
  }

  // EEXIST → 破損 / 生死 / 古さ判定
  const inspected = inspectLockfile(lockPath);
  if (!inspected.present) {
    // inspect→create の間に他 process が release した稀ケース
    try {
      tryAtomicCreate(lockPath, payload);
      return makeReleaser(lockPath, myPid);
    } catch (e2) {
      if (e2.code === 'EEXIST') {
        const winner = readLockfile(lockPath);
        throw new SyncLockError(
          `lockfile ${lockPath} taken by another process between inspect and create (winner pid=${winner?.pid})`,
          { holder: winner, reason: 'alive' }
        );
      }
      throw e2;
    }
  }
  if (!inspected.valid) {
    const reason = inspected.kind === 'io' ? 'lockfile-io-error' : 'invalid-lockfile';
    const what = inspected.kind === 'io' ? 'IO error' : 'unparseable';
    throw new SyncLockError(
      `lockfile ${lockPath} present but ${what} (${inspected.error?.message}). manual inspection required.`,
      { holder: null, reason }
    );
  }
  const existing = inspected.data;
  const ageMs = existing?.acquiredAt ? Date.now() - new Date(existing.acquiredAt).getTime() : Infinity;
  const stale = ageMs > ttlMs;
  const alive = existing ? isProcessAlive(existing.pid) : false;
  if (alive && !stale) {
    throw new SyncLockError(
      `lockfile ${lockPath} held by pid=${existing.pid} (host=${existing.host}, acquired=${existing.acquiredAt})`,
      { holder: existing, reason: 'alive' }
    );
  }
  if (alive && stale) {
    throw new SyncLockError(
      `lockfile ${lockPath} held by ALIVE pid=${existing.pid} but acquired=${existing.acquiredAt} is older than ttl (${ttlMs}ms). manual kill recommended.`,
      { holder: existing, reason: 'alive-stale' }
    );
  }
  // dead → cleanup + 再 atomic create
  try { fs.unlinkSync(lockPath); } catch (e) { if (e.code !== 'ENOENT') throw e; }
  try {
    tryAtomicCreate(lockPath, payload);
    return makeReleaser(lockPath, myPid);
  } catch (e) {
    if (e.code === 'EEXIST') {
      const winner = readLockfile(lockPath);
      throw new SyncLockError(
        `lockfile ${lockPath} taken by another process during stale cleanup (winner pid=${winner?.pid})`,
        { holder: winner, reason: 'alive' }
      );
    }
    throw e;
  }
}
