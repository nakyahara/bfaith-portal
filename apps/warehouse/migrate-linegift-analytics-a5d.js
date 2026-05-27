/**
 * migrate-linegift-analytics-a5d.js — LINEギフト Phase 1 A-5d
 *   ギフトシーン基盤 (m_gift_seasons + d_gift_season_occurrences) schema migration
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-3
 *
 * 投入: m_gift_seasons 14 シーン seed (バレンタイン / 母の日 / 父の日 / クリスマス 等)
 * 年別展開は別途 generate-gift-season-occurrences.js で実施。
 *
 * idempotent:
 *   CREATE TABLE IF NOT EXISTS + INSERT OR IGNORE。再実行は安全。
 *
 * 使い方:
 *   node apps/warehouse/migrate-linegift-analytics-a5d.js              実行
 *   node apps/warehouse/migrate-linegift-analytics-a5d.js --dry-run    判定のみ
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initDB, getDB } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');

const SQL_FILES = [
  'sql/linegift-analytics/m_gift_seasons.sql',
  'sql/linegift-analytics/d_gift_season_occurrences.sql',
];

const TABLE_NAMES = ['m_gift_seasons', 'd_gift_season_occurrences'];

function tableExists(db, name) {
  const row = db
    .prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`)
    .get(name);
  return Boolean(row);
}

function readSqlFile(relPath) {
  const full = path.join(REPO_ROOT, relPath);
  if (!fs.existsSync(full)) throw new Error(`SQL file not found: ${full}`);
  return fs.readFileSync(full, 'utf8');
}

function main() {
  const dryRun = process.argv.includes('--dry-run');
  console.log(`[migrate-linegift-analytics-a5d] start (dry-run=${dryRun})`);

  initDB();
  const db = getDB();

  const states = TABLE_NAMES.map((t) => ({ table: t, exists: tableExists(db, t) }));
  for (const s of states) {
    console.log(`  - ${s.table}: ${s.exists ? 'EXISTS (no-op CREATE)' : 'WILL CREATE'}`);
  }

  if (dryRun) {
    console.log('[migrate-linegift-analytics-a5d] dry-run end (no writes)');
    return;
  }

  db.transaction(() => {
    for (const relPath of SQL_FILES) {
      const sql = readSqlFile(relPath);
      console.log(`  applying: ${relPath} (${sql.length} chars)`);
      db.exec(sql);
    }
  })();

  const after = TABLE_NAMES.map((t) => ({ table: t, exists: tableExists(db, t) }));
  const failed = after.filter((s) => !s.exists);
  if (failed.length > 0) {
    throw new Error(`migration failed, missing tables: ${failed.map((s) => s.table).join(', ')}`);
  }

  const seasonsCount = db.prepare(`SELECT COUNT(*) AS n FROM m_gift_seasons`).get().n;
  console.log(`  m_gift_seasons seed: ${seasonsCount} 件`);

  console.log('[migrate-linegift-analytics-a5d] done');
}

main();
