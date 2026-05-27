/**
 * migrate-linegift-analytics-v1-marts.js — Commit 3 / 設計書 v1.0 §4
 *   analytics mart 3 表 (mart_linegift_kpi_summary_daily / mart_linegift_sku_perf_daily / mart_linegift_price_band_summary_daily)
 *
 * 配置場所: apps/warehouse-mirror/  (Render warehouse-mirror.db)
 *
 * 投入データは別途 build-linegift-analytics-mart.js で実施 (本スクリプトはスキーマのみ)。
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initMirrorDB, getMirrorDB } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');

const SQL_FILE = 'sql/linegift-analytics/mart_analytics_marts.sql';
const TABLE_NAMES = [
  'mart_linegift_kpi_summary_daily',
  'mart_linegift_sku_perf_daily',
  'mart_linegift_price_band_summary_daily',
];

function tableExists(db, name) {
  return Boolean(db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name));
}

function main() {
  const dryRun = process.argv.includes('--dry-run');
  console.log(`[migrate-linegift-analytics-v1-marts] start (dry-run=${dryRun})`);

  initMirrorDB();
  const db = getMirrorDB();

  for (const t of TABLE_NAMES) {
    console.log(`  - ${t}: ${tableExists(db, t) ? 'EXISTS (no-op)' : 'WILL CREATE'}`);
  }
  if (dryRun) return;

  const sqlPath = path.join(REPO_ROOT, SQL_FILE);
  if (!fs.existsSync(sqlPath)) throw new Error(`SQL file not found: ${sqlPath}`);
  const sql = fs.readFileSync(sqlPath, 'utf8');

  db.transaction(() => {
    db.exec(sql);
  })();

  const missing = TABLE_NAMES.filter((t) => !tableExists(db, t));
  if (missing.length) throw new Error(`missing tables: ${missing.join(', ')}`);

  console.log('[migrate-linegift-analytics-v1-marts] done');
}

main();
