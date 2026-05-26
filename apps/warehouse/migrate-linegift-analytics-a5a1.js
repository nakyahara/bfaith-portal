/**
 * migrate-linegift-analytics-a5a1.js — LINEギフト Phase 1 A-5a-1
 *   楽天 RMS genre 取込 + キー解決マッピング schema migration
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2
 * 参照 SQL:
 *   sql/linegift-analytics/m_rakuten_genres.sql
 *   sql/linegift-analytics/x_rakuten_item_product_map.sql
 *
 * 目的:
 *   全モール共通商品分類マスタの土台として、楽天 Genre Tree (m_rakuten_genres) と
 *   楽天商品コード ↔ NE商品コード のマッピング表 (x_rakuten_item_product_map) を新設。
 *
 * Codex 議論 (Round 1-6) を踏まえた設計:
 *   - 楽天 genre を全モール共通分類 SSoT に流用 (中原さん 2026-05-26 確定)
 *   - 紐付けキー = rakuten_shop_code + rakuten_item_code (= manageNumber、items.search 検証済)
 *   - itemUrl は補助 (変更されうる)
 *   - SCD2 (valid_from / valid_to) で履歴管理
 *
 * idempotent:
 *   既にテーブルが存在する場合は何もしない (CREATE TABLE IF NOT EXISTS)。
 *   再実行は安全。
 *
 * 使い方:
 *   node apps/warehouse/migrate-linegift-analytics-a5a1.js              実行
 *   node apps/warehouse/migrate-linegift-analytics-a5a1.js --dry-run    判定のみ
 *
 * ★ 実行前に warehouse.db backup 推奨 (warehouse.db.bak_pre_linegift_a5a1_<yyyymmdd>)
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
  'sql/linegift-analytics/m_rakuten_genres.sql',
  'sql/linegift-analytics/x_rakuten_item_product_map.sql',
  'sql/linegift-analytics/raw_rakuten_items_master.sql',
];

const TABLE_NAMES = ['m_rakuten_genres', 'x_rakuten_item_product_map', 'raw_rakuten_items_master'];

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
  console.log(`[migrate-linegift-analytics-a5a1] start (dry-run=${dryRun})`);

  initDB();
  const db = getDB();

  const states = TABLE_NAMES.map((t) => ({ table: t, exists: tableExists(db, t) }));
  for (const s of states) {
    console.log(`  - ${s.table}: ${s.exists ? 'EXISTS (no-op)' : 'WILL CREATE'}`);
  }

  if (dryRun) {
    console.log('[migrate-linegift-analytics-a5a1] dry-run end (no writes)');
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

  console.log('[migrate-linegift-analytics-a5a1] done');
}

main();
