/**
 * migrate-linegift-analytics-a5c.js — LINEギフト Phase 1 A-5c
 *   分析 materialized table 4 種 + view 5 本の schema migration
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-5
 *
 * 投入対象:
 *   - f_linegift_kpi_summary_daily
 *   - f_linegift_sku_perf_daily
 *   - f_linegift_segment_benchmark_daily
 *   - f_linegift_price_band_summary_daily
 *   - v_linegift_kpi_summary_latest / v_linegift_sku_perf_latest_90d /
 *     v_linegift_segment_benchmark_latest_90d / v_linegift_price_band_summary_latest_90d
 *   - v_linegift_monthly_trend / v_linegift_sku_relative_score_90d /
 *     v_linegift_season_sku_perf / v_linegift_new_item_ramp
 *
 * 集計データ投入は別途 build-linegift-analytics-mart.js で実施。
 *
 * idempotent: CREATE TABLE IF NOT EXISTS + DROP VIEW IF EXISTS → CREATE VIEW (再実行可)
 *
 * 使い方:
 *   node apps/warehouse/migrate-linegift-analytics-a5c.js              実行
 *   node apps/warehouse/migrate-linegift-analytics-a5c.js --dry-run    判定のみ
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initDB, getDB } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');

const SQL_FILE = 'sql/linegift-analytics/analytics_marts.sql';

const TABLE_NAMES = [
  'f_linegift_kpi_summary_daily',
  'f_linegift_sku_perf_daily',
  'f_linegift_segment_benchmark_daily',
  'f_linegift_price_band_summary_daily',
];
const VIEW_NAMES = [
  'v_linegift_kpi_summary_latest',
  'v_linegift_sku_perf_latest_90d',
  'v_linegift_segment_benchmark_latest_90d',
  'v_linegift_price_band_summary_latest_90d',
  'v_linegift_monthly_trend',
  'v_linegift_sku_relative_score_90d',
  'v_linegift_season_sku_perf',
  'v_linegift_new_item_ramp',
];

function objExists(db, name, type) {
  return Boolean(db.prepare(`SELECT name FROM sqlite_master WHERE type=? AND name=?`).get(type, name));
}

function main() {
  const dryRun = process.argv.includes('--dry-run');
  console.log(`[migrate-linegift-analytics-a5c] start (dry-run=${dryRun})`);

  initDB();
  const db = getDB();

  for (const t of TABLE_NAMES) {
    console.log(`  table ${t}: ${objExists(db, t, 'table') ? 'EXISTS' : 'WILL CREATE'}`);
  }
  for (const v of VIEW_NAMES) {
    console.log(`  view  ${v}: ${objExists(db, v, 'view') ? 'EXISTS (drop+recreate)' : 'WILL CREATE'}`);
  }

  if (dryRun) {
    console.log('[migrate-linegift-analytics-a5c] dry-run end');
    return;
  }

  const sqlFile = path.join(REPO_ROOT, SQL_FILE);
  const sql = fs.readFileSync(sqlFile, 'utf8');
  db.transaction(() => {
    db.exec(sql);
  })();

  const missingTables = TABLE_NAMES.filter((t) => !objExists(db, t, 'table'));
  const missingViews = VIEW_NAMES.filter((v) => !objExists(db, v, 'view'));
  if (missingTables.length || missingViews.length) {
    throw new Error(`migration failed: missing tables=${missingTables.join(',')} views=${missingViews.join(',')}`);
  }

  console.log('[migrate-linegift-analytics-a5c] done');
}

main();
