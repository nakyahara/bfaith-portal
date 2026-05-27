/**
 * migrate-linegift-analytics-v1-locks.js — Commit 2 / 設計書 v1.0 §4 / §6
 *   build 排他制御テーブル (mart_build_locks + mart_build_lock_history) の schema migration
 *
 * 配置場所: apps/warehouse-mirror/  (Render 側ジョブ、warehouse-mirror.db に書く)
 *
 * Codex v1.0 Round 2 Critical 2 (排他/ロールバック): DB lock + lease_ttl + heartbeat
 *
 * idempotent: CREATE TABLE IF NOT EXISTS。再実行は安全。
 *
 * 使い方:
 *   node apps/warehouse-mirror/migrate-linegift-analytics-v1-locks.js              実行
 *   node apps/warehouse-mirror/migrate-linegift-analytics-v1-locks.js --dry-run    判定のみ
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initMirrorDB, getMirrorDB } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');

const SQL_FILE = 'sql/linegift-analytics/mart_build_locks.sql';
const TABLE_NAMES = ['mart_build_locks', 'mart_build_lock_history'];

function tableExists(db, name) {
  return Boolean(db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name));
}

function main() {
  const dryRun = process.argv.includes('--dry-run');
  console.log(`[migrate-linegift-analytics-v1-locks] start (dry-run=${dryRun})`);

  initMirrorDB();
  const db = getMirrorDB();

  for (const t of TABLE_NAMES) {
    console.log(`  - ${t}: ${tableExists(db, t) ? 'EXISTS (no-op)' : 'WILL CREATE'}`);
  }
  if (dryRun) {
    console.log('[migrate-linegift-analytics-v1-locks] dry-run end');
    return;
  }

  const sqlPath = path.join(REPO_ROOT, SQL_FILE);
  if (!fs.existsSync(sqlPath)) throw new Error(`SQL file not found: ${sqlPath}`);
  const sql = fs.readFileSync(sqlPath, 'utf8');

  db.transaction(() => {
    db.exec(sql);
  })();

  const missing = TABLE_NAMES.filter((t) => !tableExists(db, t));
  if (missing.length) throw new Error(`missing tables: ${missing.join(', ')}`);

  console.log('[migrate-linegift-analytics-v1-locks] done');
}

main();
