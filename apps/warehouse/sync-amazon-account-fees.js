#!/usr/bin/env node
/**
 * sync-amazon-account-fees.js — アカウント単位フィー月次 mirror sync (amazon-dashboard PR-C)
 *
 * f_amazon_account_fees_monthly_v1 → mirror_amazon_account_fees_monthly。
 * grain は月次だが、clear 機構 (date_range) と整合させるため date_jst = 月初日 (YYYY-MM-01) で送る。
 * scope 内の全月初日を clear 対象に含める (0 行月の stale 削除)。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-amazon-account-fees.js --months 14 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-amazon-account-fees.js --months 14
 */
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

const ENTITY_NAME = 'amazon_account_fees_monthly';
const CONTRACT_VERSION = 1;
const CHUNK_SIZE = Math.min(parseInt(process.env.CHUNK_SIZE, 10) || 1000, 1000);

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthsBack = Math.min(Math.max(parseInt(getArg('--months'), 10) || 14, 1), 60);
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2);
}

// scope: monthsBack ヶ月前の月初 〜 当月月初 (JST)
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const monthStarts = [];
for (let i = monthsBack - 1; i >= 0; i--) {
  const d = new Date(Date.UTC(nowJst.getUTCFullYear(), nowJst.getUTCMonth() - i, 1));
  monthStarts.push(d.toISOString().slice(0, 10));
}
const dateRange = { from: monthStarts[0], to: monthStarts[monthStarts.length - 1] };

const db = new Database(dbPath, { readonly: true });
const contract = db.prepare(`SELECT * FROM sync_contracts WHERE entity = ?`).get(ENTITY_NAME);
if (!contract) {
  console.error(`FATAL: contract for entity '${ENTITY_NAME}' not found in sync_contracts`);
  console.error('  initDB() の auto-seed が走ってないか、apps/warehouse/db.js を最新版に更新してください');
  process.exit(1);
}
if (contract.contract_version !== CONTRACT_VERSION) {
  console.error(`FATAL: contract version mismatch: code=${CONTRACT_VERSION}, db=${contract.contract_version}`);
  process.exit(1);
}
console.log(`=== sync ${ENTITY_NAME} v${CONTRACT_VERSION} ===`);
console.log(`  scope: ${dateRange.from} 〜 ${dateRange.to} (${monthStarts.length} months)`);
console.log(`  dry-run: ${isDryRun}`);

const rows = db.prepare(`
  SELECT month_start_jst AS date_jst, fee_type, amount_jpy, row_count
  FROM f_amazon_account_fees_monthly_v1
  WHERE month_start_jst >= ? AND month_start_jst <= ?
  ORDER BY month_start_jst, fee_type
`).all(dateRange.from, dateRange.to);
db.close();
console.log(`  Rows to sync: ${rows.length}`);

const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
const runId = `${ENTITY_NAME}-v${CONTRACT_VERSION}-${tsCompact}-${crypto.randomBytes(3).toString('hex')}`;
const syncedAt = new Date().toISOString();
const enrichedRows = rows.map(r => {
  const hash = crypto.createHash('sha256')
    .update(JSON.stringify({ date_jst: r.date_jst, fee_type: r.fee_type, amount_jpy: r.amount_jpy }))
    .digest('hex').slice(0, 16);
  return { ...r, source_run_id: runId, source_row_hash: hash, synced_at: syncedAt };
});

const chunks = [];
for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
if (chunks.length === 0) chunks.push([]);
console.log(`  Chunks: ${chunks.length} / run_id: ${runId}`);

if (isDryRun) {
  console.log('  --- DRY RUN ---');
  console.log(`  First chunk meta: clear_amazon_account_fees_dates=[${monthStarts.length} month starts]`);
  if (enrichedRows.length > 0) {
    const r0 = enrichedRows[0];
    console.log(`  Sample row[0]: ${r0.date_jst} ${r0.fee_type} ¥${Math.round(r0.amount_jpy).toLocaleString()}`);
  }
  process.exit(0);
}

const writeDb = new Database(dbPath);
writeDb.prepare(`
  INSERT INTO sync_runs (
    run_id, entity, contract_version, source_host,
    scope_from, scope_to, chunk_count_expected, status, started_at, replay_of_run_id
  ) VALUES (?, ?, ?, ?, ?, ?, ?, 'started', ?, ?)
`).run(runId, ENTITY_NAME, CONTRACT_VERSION, os.hostname(),
        dateRange.from, dateRange.to, chunks.length, new Date().toISOString(),
        (process.env.REPLAY_OF_RUN_ID || '').trim() || null);
writeDb.close();

let totalRowsSent = 0, chunksApplied = 0, lastError = null;
for (let i = 0; i < chunks.length; i++) {
  const chunk = chunks[i];
  const payload = { rows: chunk };
  const payloadChecksum = crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  const body = {
    sync_run_id: runId, contract_version: CONTRACT_VERSION,
    scope_from: dateRange.from, scope_to: dateRange.to,
    chunk_index: i, chunk_count: chunks.length,
    is_first: i === 0, is_last: i === chunks.length - 1,
    row_count: chunk.length, payload_checksum: payloadChecksum,
    meta: i === 0 ? { clear_amazon_account_fees_dates: monthStarts } : {},
    payload,
  };
  console.log(`  [${i + 1}/${chunks.length}] sending ${chunk.length} rows...`);
  const headers = { 'Content-Type': 'application/json' };
  if (syncKey) headers['x-sync-key'] = syncKey;
  try {
    const res = await fetch(`${renderUrl}/api/sync/${ENTITY_NAME}/chunk`, {
      method: 'POST', headers, body: JSON.stringify(body),
    });
    if (!res.ok) {
      lastError = `HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`;
      console.error(`  ✗ chunk ${i} failed: ${lastError}`);
      break;
    }
    const result = await res.json();
    totalRowsSent += chunk.length;
    chunksApplied += 1;
    console.log(`  ✓ chunk ${i} applied (request_id=${result.request_id})`);
  } catch (e) {
    lastError = e.message;
    console.error(`  ✗ chunk ${i} error: ${e.message}`);
    break;
  }
}

const finishDb = new Database(dbPath);
if (lastError) {
  finishDb.prepare(`
    UPDATE sync_runs SET status = 'failed', error_message = ?, completed_at = ?,
      chunk_count_received = ?, row_count_received = ? WHERE run_id = ?
  `).run(lastError, new Date().toISOString(), chunksApplied, totalRowsSent, runId);
  finishDb.close();
  console.log(`✗ sync FAILED: ${lastError}`);
  process.exit(1);
}
finishDb.prepare(`
  UPDATE sync_runs SET status = 'applied', chunk_count_received = ?, row_count_received = ?,
    completed_at = ?, applied_at = ? WHERE run_id = ?
`).run(chunksApplied, totalRowsSent, new Date().toISOString(), new Date().toISOString(), runId);
finishDb.close();
console.log(`✓ sync complete (run_id=${runId}, ${totalRowsSent} rows)`);
process.exit(0);
