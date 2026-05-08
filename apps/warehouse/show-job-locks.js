#!/usr/bin/env node
/**
 * show-job-locks.js — job_locks の現在状態を表示する CLI
 *
 * Phase 1 #1-7a
 *
 * 使い方:
 *   node apps/warehouse/show-job-locks.js              # active + stale 全て
 *   node apps/warehouse/show-job-locks.js --cleanup    # stale (TTL 切れ) を一括削除
 *
 * env:
 *   DATA_DIR=C:\Users\bfaith\bfaith-portal\data    必須
 */

import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { listLocks, cleanupStaleLocks } from './job-locks.js';

const args = process.argv.slice(2);
const isCleanup = args.includes('--cleanup');

const DATA_DIR = (process.env.DATA_DIR || '').trim();
if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR environment variable is required.');
  console.error('  Example: DATA_DIR=C:\\Users\\bfaith\\bfaith-portal\\data node apps/warehouse/show-job-locks.js');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exit(2);
}

const db = new Database(dbPath);

try {
  // 1. 現状表示
  const locks = listLocks(db);
  console.log('=== job_locks 現在状態 ===');
  if (locks.length === 0) {
    console.log('  (lock なし)');
  } else {
    console.table(locks.map(l => ({
      job_name: l.job_name,
      status: l.status,
      holder: l.holder_id.slice(0, 30) + (l.holder_id.length > 30 ? '...' : ''),
      acquired_at: l.acquired_at,
      expires_at: l.expires_at,
      heartbeat_at: l.heartbeat_at,
    })));
  }

  // 2. cleanup 指定時
  if (isCleanup) {
    const removed = cleanupStaleLocks(db);
    console.log(`\n=== cleanup ===`);
    console.log(`  removed ${removed} stale locks`);
  }
} finally {
  db.close();
}
