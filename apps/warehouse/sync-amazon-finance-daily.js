#!/usr/bin/env node
/**
 * sync-amazon-finance-daily.js — Phase 1 #1-4 entity-driven sync
 *
 * f_amazon_finance_sku_daily_v1 → mirror_amazon_finance_sku_daily に sync する
 * contract-driven 実装。既存 sync-to-render.js は触らず、新 entity 専用 script。
 *
 * 設計 (Codex Round 8/10 推奨):
 *   - sync_contracts に登録された contract version で送信
 *   - first chunk で `meta.clear_amazon_finance_dates` を送って Render 側で DELETE
 *   - INSERT OR REPLACE on PK で同一 run の再送は idempotent
 *   - source_run_id / source_row_hash で audit trail
 *   - dry-run / scoped sync (--from / --to / --month) サポート
 *   - DATA_DIR 必須 (fail-fast)
 *
 * 受信側 (Render の warehouse-mirror/router.js) は **本 PR では未実装**。
 * Phase 1 #1-4a (sync ledger) で sync_runs / sync_run_chunks と一緒に組み込む。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-amazon-finance-daily.js --month 2026-04 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-amazon-finance-daily.js --from 2026-04-01 --to 2026-04-30
 *   DATA_DIR=... node apps/warehouse/sync-amazon-finance-daily.js --month 2026-04
 *
 * env:
 *   DATA_DIR             必須
 *   RENDER_MIRROR_URL    送信先 base URL (例: https://bfaith-portal.onrender.com)
 *   MIRROR_SYNC_KEY      認証 (x-sync-key header)
 *
 * exit code:
 *   0: success
 *   1: contract mismatch / payload error
 *   2: env error
 *   73: lock 取得失敗 (job_locks 経由、Phase 1.x で組み込み予定)
 */

import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

const ENTITY_NAME = 'amazon_finance_sku_daily';
const CONTRACT_VERSION = 1;
const CHUNK_SIZE = 5000;

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}

const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthStr = getArg('--month');
const fromStr = getArg('--from');
const toStr = getArg('--to');
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR is required');
  process.exit(2);
}

let dateRange;
if (monthStr) {
  if (!/^\d{4}-\d{2}$/.test(monthStr)) {
    console.error('FATAL: --month must be YYYY-MM');
    process.exit(2);
  }
  dateRange = { from: `${monthStr}-01`, to: `${monthStr}-31` };
} else if (fromStr && toStr) {
  dateRange = { from: fromStr, to: toStr };
} else {
  console.error('FATAL: --month YYYY-MM or --from/--to YYYY-MM-DD required');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exit(2);
}

if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run');
  process.exit(2);
}

const db = new Database(dbPath, { readonly: true });

// ============================================================
// 1. contract 確認
// ============================================================
const contract = db.prepare(`SELECT * FROM sync_contracts WHERE entity = ?`).get(ENTITY_NAME);
if (!contract) {
  console.error(`FATAL: contract for entity '${ENTITY_NAME}' not found in sync_contracts`);
  console.error(`  Run: sqlite3 warehouse.db < config/sync/contracts/seed-amazon-finance-sku-daily.sql`);
  process.exit(1);
}
if (contract.contract_version !== CONTRACT_VERSION) {
  console.error(`FATAL: contract version mismatch: code=${CONTRACT_VERSION}, db=${contract.contract_version}`);
  console.error('  Update sync_contracts row, or update CONTRACT_VERSION constant in this script');
  process.exit(1);
}
console.log(`=== sync ${ENTITY_NAME} v${CONTRACT_VERSION} ===`);
console.log(`  source: ${contract.source_object}`);
console.log(`  target: ${contract.target_table}`);
console.log(`  scope: ${dateRange.from} 〜 ${dateRange.to}`);
console.log(`  dry-run: ${isDryRun}`);

// ============================================================
// 2. payload build
// ============================================================
const rows = db.prepare(`
  SELECT * FROM f_amazon_finance_sku_daily_v1
  WHERE date_jst >= ? AND date_jst <= ?
  ORDER BY date_jst, seller_sku
`).all(dateRange.from, dateRange.to);

console.log(`\n  Rows to sync: ${rows.length.toLocaleString()}`);

if (rows.length === 0) {
  console.log('  (no rows in range, nothing to sync)');
  process.exit(0);
}

// 全 distinct date_jst を抽出 (clear meta 用)
const distinctDates = [...new Set(rows.map(r => r.date_jst))].sort();
console.log(`  Distinct dates: ${distinctDates.length} (${distinctDates[0]} 〜 ${distinctDates[distinctDates.length - 1]})`);

// run_id 生成
const runId = `${ENTITY_NAME}-v${CONTRACT_VERSION}-${new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)}`;
console.log(`  run_id: ${runId}`);

// payload に source_run_id / source_row_hash / synced_at を付与
const syncedAt = new Date().toISOString();
const enrichedRows = rows.map(r => {
  // source_row_hash: 主要列の SHA256 (audit trail)
  const hashInput = JSON.stringify({
    date_jst: r.date_jst,
    seller_sku: r.seller_sku,
    asin_norm: r.asin_norm || '',
    profit_amount: r.profit_amount,
    cogs_amount: r.cogs_amount,
    units_ordered: r.units_ordered,
  });
  const hash = crypto.createHash('sha256').update(hashInput).digest('hex').slice(0, 16);
  return {
    ...r,
    source_run_id: runId,
    source_row_hash: hash,
    synced_at: syncedAt,
  };
});

// ============================================================
// 3. chunk + send (or dry-run)
// ============================================================
const chunks = [];
for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) {
  chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
}
console.log(`\n  Chunks: ${chunks.length} (chunk_size=${CHUNK_SIZE})`);

if (isDryRun) {
  console.log('\n--- DRY RUN ---');
  console.log(`  Would send ${chunks.length} chunks (${enrichedRows.length} rows total) to ${renderUrl || '<RENDER_MIRROR_URL not set>'}`);
  console.log(`  First chunk meta: clear_amazon_finance_dates=[${distinctDates.length} dates]`);
  console.log(`  Sample row keys: ${Object.keys(enrichedRows[0]).join(', ')}`);
  console.log(`  Sample row[0]: date=${enrichedRows[0].date_jst} sku=${enrichedRows[0].seller_sku} profit=${enrichedRows[0].profit_amount} hash=${enrichedRows[0].source_row_hash}`);
  db.close();
  process.exit(0);
}

// 実 send (Render 受信側は #1-4a で実装、現状は dry-run まで)
console.log('\n  ⚠ 実 send は #1-4a (sync ledger) で warehouse-mirror/router.js に受信ロジック追加後に実行可能');
console.log('  本 PR では payload build + dry-run までを scope とする');

db.close();
process.exit(0);
