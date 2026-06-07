/**
 * migrate-linegift-analytics-a5a2.js — LINEギフト Phase 1 A-5a-2
 *   商品分類マスタ + 価格帯マスタ schema migration
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2 / §13-4
 * 参照 SQL:
 *   sql/linegift-analytics/m_price_bands.sql              (8段階 seed 含む)
 *   sql/linegift-analytics/m_product_classifications.sql
 *
 * 目的:
 *   A-5a-1 で raw_rakuten_items_master / x_rakuten_item_product_map を作成済。
 *   本 migration で全モール共通 m_product_classifications + m_price_bands を追加。
 *   後続の build-product-classifications-from-rakuten.js が、これらに RAKUTEN_AUTO 行を投入する。
 *
 * idempotent:
 *   CREATE TABLE IF NOT EXISTS + INSERT OR IGNORE。再実行は安全。
 *
 * 使い方:
 *   node apps/warehouse/migrate-linegift-analytics-a5a2.js              実行
 *   node apps/warehouse/migrate-linegift-analytics-a5a2.js --dry-run    判定のみ
 *
 * ★ 実行前に warehouse.db backup 推奨 (warehouse.db.bak_pre_linegift_a5a2_<yyyymmdd>)
 * ★ A-5a-1 (migrate-linegift-analytics-a5a1.js) を先に実行しておくこと (依存はないが順序として推奨)
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
  'sql/linegift-analytics/m_price_bands.sql',
  'sql/linegift-analytics/m_product_classifications.sql',
];

const TABLE_NAMES = ['m_price_bands', 'm_product_classifications'];

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
  console.log(`[migrate-linegift-analytics-a5a2] start (dry-run=${dryRun})`);

  initDB();
  const db = getDB();

  const states = TABLE_NAMES.map((t) => ({ table: t, exists: tableExists(db, t) }));
  for (const s of states) {
    console.log(`  - ${s.table}: ${s.exists ? 'EXISTS (no-op CREATE)' : 'WILL CREATE'}`);
  }

  if (dryRun) {
    console.log('[migrate-linegift-analytics-a5a2] dry-run end (no writes)');
    return;
  }

  db.transaction(() => {
    for (const relPath of SQL_FILES) {
      const sql = readSqlFile(relPath);
      console.log(`  applying: ${relPath} (${sql.length} chars)`);
      db.exec(sql);
    }
  })();

  // 確認
  const after = TABLE_NAMES.map((t) => ({ table: t, exists: tableExists(db, t) }));
  const failed = after.filter((s) => !s.exists);
  if (failed.length > 0) {
    throw new Error(`migration failed, missing tables: ${failed.map((s) => s.table).join(', ')}`);
  }

  // seed 件数確認 (m_price_bands は 8 段階)
  const bandCount = db.prepare(`SELECT COUNT(*) AS n FROM m_price_bands`).get().n;
  console.log(`  m_price_bands seed: ${bandCount} 行 (期待 = 8)`);

  console.log('[migrate-linegift-analytics-a5a2] done');
}

main();
