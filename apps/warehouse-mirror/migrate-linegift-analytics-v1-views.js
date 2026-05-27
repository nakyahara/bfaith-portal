/**
 * migrate-linegift-analytics-v1-views.js — Commit 4 / 設計書 v1.0 §5
 *   View 4 本投入 (DROP + CREATE で再実行安全)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initMirrorDB, getMirrorDB } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');

const SQL_FILE = 'sql/linegift-analytics/mart_views.sql';
const VIEW_NAMES = [
  'v_linegift_kpi_summary_latest',
  'v_linegift_sku_perf_latest_90d',
  'v_linegift_price_band_summary_latest_90d',
  'v_linegift_monthly_trend',
  'v_linegift_season_sku_perf',
  'v_linegift_new_item_ramp',
];

function viewExists(db, name) {
  return Boolean(db.prepare(`SELECT name FROM sqlite_master WHERE type='view' AND name=?`).get(name));
}

function main() {
  const dryRun = process.argv.includes('--dry-run');
  console.log(`[migrate-linegift-analytics-v1-views] start (dry-run=${dryRun})`);

  initMirrorDB();
  const db = getMirrorDB();

  for (const v of VIEW_NAMES) {
    console.log(`  - ${v}: ${viewExists(db, v) ? 'EXISTS (drop+recreate)' : 'WILL CREATE'}`);
  }
  if (dryRun) return;

  const sqlPath = path.join(REPO_ROOT, SQL_FILE);
  if (!fs.existsSync(sqlPath)) throw new Error(`SQL file not found: ${sqlPath}`);
  const sql = fs.readFileSync(sqlPath, 'utf8');

  db.transaction(() => {
    db.exec(sql);
  })();

  const missing = VIEW_NAMES.filter((v) => !viewExists(db, v));
  if (missing.length) throw new Error(`missing views: ${missing.join(', ')}`);

  console.log('[migrate-linegift-analytics-v1-views] done');
}

main();
