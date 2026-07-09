/**
 * migrate-audit-pr12-cleanup.js — 監査PR-12(a)(d): dead表DROP + 冗長index削除 (一回きり・手動実行)
 *
 * 対象 (2026-07-09 本番実測・コード参照0件をgrepで再確認済み):
 *   dead表 8本 (計約29K行/5MB):
 *     _backup_yahoo_orders_empty_rows_20260511 (1,489行) … PR #94期の退避
 *     raw_linegift_orders_legacy_buggy_*        (10,027行) … linegift-orders.js移行時の退避(取込済)
 *     raw_qoo10_orders_legacy_v0                (17,252行) … migrate-qoo10-raw.jsの退避(source_type='legacy_migration'として新表に取込済を確認)
 *     fact_access / fact_review / fact_returns_unresolved (0行) … 旧sales-analytics残骸
 *     amazon_financial_line_reconciliation      (0行)
 *     sync_runs_bak_20260508                    (0行)
 *   冗長index 1本:
 *     idx_settle_lines_settlement (67MB) … idx_settle_lines_dedup の完全prefix (INV-22)。
 *     db.js の作成行は削除済み (boot で再作成されない)
 *
 * 安全策:
 *   ・非空テーブルは DROP 前に data/backup/pr12-dead-tables-<日付>/ へ jsonl.gz 全量ダンプ
 *   ・ダンプ検証 (行数一致) 後に同一トランザクションで DROP
 *   ・ロールバック: ダンプから CREATE+INSERT で復元可能 (DDL も dump ファイル先頭に記録)
 *
 * 実行: cd C:\Users\bfaith\bfaith-portal && node apps/warehouse/migrate-audit-pr12-cleanup.js
 *       --dry-run で対象表示のみ
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { initDB, getDB } from './db.js';

const isDryRun = process.argv.includes('--dry-run');
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const stamp = new Date().toISOString().slice(0, 10).replace(/-/g, '');
const BACKUP_DIR = path.join(DATA_DIR, 'backup', `pr12-dead-tables-${stamp}`);

const DEAD_TABLES = [
  '_backup_yahoo_orders_empty_rows_20260511',
  'raw_qoo10_orders_legacy_v0',
  'fact_access',
  'fact_review',
  'fact_returns_unresolved',
  'amazon_financial_line_reconciliation',
  'sync_runs_bak_20260508',
];
const DROP_INDEXES = ['idx_settle_lines_settlement'];

await initDB();
const db = getDB();

// raw_linegift_orders_legacy_buggy_* はタイムスタンプ付き名なので動的検出
const legacyLinegift = db.prepare(
  "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'raw_linegift_orders_legacy_buggy_%'"
).all().map(r => r.name);
const targets = [...DEAD_TABLES, ...legacyLinegift];

console.log(`[pr12-cleanup] dry-run=${isDryRun}`);
const plan = [];
for (const t of targets) {
  const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(t);
  if (!exists) { console.log(`  skip (不存在): ${t}`); continue; }
  const n = db.prepare(`SELECT COUNT(*) c FROM "${t}"`).get().c;
  plan.push({ t, n });
  console.log(`  DROP対象: ${t} (${n}行)`);
}
for (const ix of DROP_INDEXES) {
  const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?").get(ix);
  console.log(exists ? `  DROP INDEX対象: ${ix}` : `  skip (index不存在): ${ix}`);
}

if (isDryRun) { console.log('[pr12-cleanup] dry-run 終了 (変更なし)'); process.exit(0); }

// ─── 1) 非空テーブルをダンプ (DDL + 全行 jsonl.gz) ───
fs.mkdirSync(BACKUP_DIR, { recursive: true });
for (const { t, n } of plan) {
  if (n === 0) continue;
  const ddl = db.prepare("SELECT sql FROM sqlite_master WHERE name=?").get(t).sql;
  const rows = db.prepare(`SELECT * FROM "${t}"`).all();
  if (rows.length !== n) throw new Error(`ABORT: ${t} ダンプ行数不一致 (${rows.length} != ${n})`);
  const payload = `-- DDL\n${ddl};\n-- ROWS(jsonl)\n` + rows.map(r => JSON.stringify(r)).join('\n') + '\n';
  const out = path.join(BACKUP_DIR, `${t}.dump.gz`);
  fs.writeFileSync(out + '.tmp', zlib.gzipSync(payload, { level: 6 }));
  fs.renameSync(out + '.tmp', out);
  console.log(`[pr12-cleanup] dump: ${t} (${n}行) → ${out}`);
}

// ─── 2) DROP (1トランザクション) ───
const tx = db.transaction(() => {
  for (const { t } of plan) db.exec(`DROP TABLE "${t}"`);
  for (const ix of DROP_INDEXES) db.exec(`DROP INDEX IF EXISTS "${ix}"`);
});
tx();

// ─── 3) 検証 ───
let ok = true;
for (const { t } of plan) {
  if (db.prepare("SELECT 1 FROM sqlite_master WHERE name=?").get(t)) { console.error(`NG: ${t} が残存`); ok = false; }
}
for (const ix of DROP_INDEXES) {
  if (db.prepare("SELECT 1 FROM sqlite_master WHERE name=?").get(ix)) { console.error(`NG: ${ix} が残存`); ok = false; }
}
try { console.log(`[pr12-cleanup] wal_checkpoint: ${JSON.stringify(db.pragma('wal_checkpoint(TRUNCATE)'))}`); } catch {}
console.log(ok ? `[pr12-cleanup] ✅ 完了: ${plan.length}表+index削除。バックアップ=${BACKUP_DIR}` : '[pr12-cleanup] ❌ 検証NG');
process.exitCode = ok ? 0 : 1;
