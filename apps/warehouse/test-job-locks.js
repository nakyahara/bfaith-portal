#!/usr/bin/env node
/**
 * test-job-locks.js — job_locks の concurrency / takeover / heartbeat テスト
 *
 * Phase 1 #1-7a Acceptance Criteria:
 *   1. 同一 job_name に対する同時 acquire 2 回のうち成功がちょうど 1 回、失敗がちょうど 1 回
 *   2. TTL 超過後の stale lock takeover が 1 回で成功し、旧 holder の再 acquire は 0 回成功
 *   3. heartbeat 後の lock expiry 延長が 100% 反映
 *
 * 使い方:
 *   DATA_DIR=C:\Users\bfaith\bfaith-portal\data node apps/warehouse/test-job-locks.js
 *
 * 注意: テスト用 job_name (`__test_lock_*`) は実行終了時に自動 cleanup。
 * 万一残った場合は: `DATA_DIR=... node apps/warehouse/show-job-locks.js --cleanup`
 */

import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { acquireLock, heartbeatLock, releaseLock, listLocks, cleanupStaleLocks } from './job-locks.js';

const DATA_DIR = (process.env.DATA_DIR || '').trim();
if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR environment variable is required.');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
const db = new Database(dbPath);

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    console.log(`  ✓ ${msg}`);
    passed++;
  } else {
    console.error(`  ✗ ${msg}`);
    failed++;
  }
}

function cleanupTestLocks() {
  db.prepare(`DELETE FROM job_locks WHERE job_name LIKE '__test_lock_%'`).run();
}

try {
  cleanupTestLocks();

  // ============================================================
  // Test 1: 同時 acquire — 1 成功 1 失敗
  // ============================================================
  console.log('\n=== Test 1: 同時 acquire (排他) ===');
  const lockA = acquireLock(db, '__test_lock_concurrency', { ttlMs: 60000 });
  const lockB = acquireLock(db, '__test_lock_concurrency', { ttlMs: 60000 });
  assert(lockA !== null, 'A: 1 回目 acquire 成功');
  assert(lockB === null, 'B: 2 回目 acquire 失敗 (既存 lock 有効)');
  assert(lockA?.holderId !== undefined, 'A: holderId が生成されている');

  // release で次の acquire が成功することを確認
  releaseLock(db, lockA);
  const lockC = acquireLock(db, '__test_lock_concurrency', { ttlMs: 60000 });
  assert(lockC !== null, 'C: A release 後 再 acquire 成功');
  releaseLock(db, lockC);

  // ============================================================
  // Test 2: TTL 切れ takeover
  // ============================================================
  console.log('\n=== Test 2: TTL 超過後の takeover ===');
  // TTL 100ms で acquire
  const lockX = acquireLock(db, '__test_lock_takeover', { ttlMs: 100 });
  assert(lockX !== null, 'X: 短 TTL acquire 成功');

  // 200ms 待つ (TTL 切れ)
  await new Promise(r => setTimeout(r, 200));

  // takeover が 1 回で成功することを確認
  const lockY = acquireLock(db, '__test_lock_takeover', { ttlMs: 60000 });
  assert(lockY !== null, 'Y: TTL 切れ後 takeover 成功');
  assert(lockY?.holderId !== lockX?.holderId, 'Y: 新 holder ID で取得 (X とは別)');

  // 旧 holder (X) の release は changes=0 (= 失敗扱い、既に Y が hold)
  const oldRelease = releaseLock(db, lockX);
  assert(oldRelease === false, 'X: 旧 holder の release は失敗 (takeover 済)');

  // 旧 holder の再 acquire は失敗 (Y が hold 中)
  const lockX2 = acquireLock(db, '__test_lock_takeover', { ttlMs: 60000, holderId: lockX.holderId });
  assert(lockX2 === null, 'X2: 旧 holder の再 acquire 失敗 (Y が hold 中)');

  releaseLock(db, lockY);

  // ============================================================
  // Test 3: heartbeat による expiry 延長
  // ============================================================
  console.log('\n=== Test 3: heartbeat による expiry 延長 ===');
  const lockH = acquireLock(db, '__test_lock_heartbeat', { ttlMs: 60000 });
  assert(lockH !== null, 'H: acquire 成功');
  const initialExpires = lockH.expiresAt;

  // 100ms 待つ → heartbeat
  await new Promise(r => setTimeout(r, 100));
  const hbResult = heartbeatLock(db, lockH, { extendMs: 60000 });
  assert(hbResult === true, 'H: heartbeat 成功');
  assert(lockH.expiresAt !== initialExpires, 'H: expiresAt が更新された');
  assert(new Date(lockH.expiresAt).getTime() > new Date(initialExpires).getTime(), 'H: expiresAt が将来に延長された');

  // DB 上の値も更新されているか確認
  const dbState = db.prepare(`SELECT expires_at FROM job_locks WHERE job_name = ?`).get('__test_lock_heartbeat');
  assert(dbState.expires_at === lockH.expiresAt, 'H: DB の expires_at と lock の expiresAt が一致');

  // 別 holder の heartbeat は失敗
  const otherLock = { jobName: '__test_lock_heartbeat', holderId: 'other-host-pid-uuid' };
  const otherHb = heartbeatLock(db, otherLock);
  assert(otherHb === false, 'H: 別 holder の heartbeat は失敗');

  releaseLock(db, lockH);

  // ============================================================
  // Test 4: cleanup stale locks
  // ============================================================
  console.log('\n=== Test 4: cleanupStaleLocks ===');
  const lockS1 = acquireLock(db, '__test_lock_stale_1', { ttlMs: 50 });
  const lockS2 = acquireLock(db, '__test_lock_stale_2', { ttlMs: 50 });
  const lockS3 = acquireLock(db, '__test_lock_stale_3', { ttlMs: 60000 });  // active
  await new Promise(r => setTimeout(r, 150));

  const removed = cleanupStaleLocks(db);
  assert(removed >= 2, `S: stale 2 件以上削除 (実際: ${removed} 件)`);

  // active な S3 は残っているはず
  const remaining = db.prepare(`SELECT job_name FROM job_locks WHERE job_name LIKE '__test_lock_stale_%'`).all();
  const remainingNames = remaining.map(r => r.job_name);
  assert(remainingNames.includes('__test_lock_stale_3'), 'S: active な lock は残っている');
  assert(!remainingNames.includes('__test_lock_stale_1'), 'S: stale lock_1 は削除');
  assert(!remainingNames.includes('__test_lock_stale_2'), 'S: stale lock_2 は削除');

  releaseLock(db, lockS3);

} finally {
  cleanupTestLocks();
  db.close();
}

console.log('\n=== Test Summary ===');
console.log(`  ✓ PASS: ${passed}`);
console.log(`  ✗ FAIL: ${failed}`);

if (failed > 0) {
  console.error('\n❌ Test failed');
  process.exit(1);
} else {
  console.log('\n✅ All tests passed');
  process.exit(0);
}
