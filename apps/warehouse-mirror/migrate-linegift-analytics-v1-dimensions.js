/**
 * migrate-linegift-analytics-v1-dimensions.js — Commit 1 / 設計書 v1.0 §3-§4
 *   Render 派生ディメンション (mart_price_bands / mart_gift_seasons / mart_gift_season_occurrences) の schema migration
 *
 * 配置場所: apps/warehouse-mirror/  ← Render 側ジョブとして動作 (warehouse-mirror.db に書く)
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md
 *
 * 重要原則:
 *   - mart_ prefix で sync_contracts 未登録 → mirror sync 対象外 (miniPC の sync runner が触らない)
 *   - 集計・分析・UI は全部 Render 完結 (miniPC は既存稼働の raw 取込以外触らない)
 *   - 既存慣習: mirror_* = miniPC から同期、mart_* = ツール用 Render 派生 (warehouse-mirror.db ヘッダコメント参照)
 *
 * idempotent: CREATE TABLE IF NOT EXISTS + INSERT OR IGNORE。再実行は安全。
 *
 * 使い方:
 *   node apps/warehouse-mirror/migrate-linegift-analytics-v1-dimensions.js              実行
 *   node apps/warehouse-mirror/migrate-linegift-analytics-v1-dimensions.js --dry-run    判定のみ
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { initMirrorDB, getMirrorDB } from './db.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '..', '..');

const SQL_FILE = 'sql/linegift-analytics/mart_seed_dimensions.sql';

const TABLE_NAMES = ['mart_price_bands', 'mart_gift_seasons', 'mart_gift_season_occurrences'];

function tableExists(db, name) {
  return Boolean(db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(name));
}

function main() {
  const dryRun = process.argv.includes('--dry-run');
  console.log(`[migrate-linegift-analytics-v1-dimensions] start (dry-run=${dryRun})`);

  initMirrorDB();
  const db = getMirrorDB();

  for (const t of TABLE_NAMES) {
    console.log(`  - ${t}: ${tableExists(db, t) ? 'EXISTS (no-op)' : 'WILL CREATE'}`);
  }

  if (dryRun) {
    console.log('[migrate-linegift-analytics-v1-dimensions] dry-run end');
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

  const bands = db.prepare(`SELECT COUNT(*) AS n FROM mart_price_bands`).get().n;
  const seasons = db.prepare(`SELECT COUNT(*) AS n FROM mart_gift_seasons`).get().n;
  console.log(`  mart_price_bands seed: ${bands} 件 (期待 8)`);
  console.log(`  mart_gift_seasons seed: ${seasons} 件 (期待 14)`);

  console.log('[migrate-linegift-analytics-v1-dimensions] done');
}

main();
