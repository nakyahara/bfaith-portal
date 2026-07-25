#!/usr/bin/env node
/**
 * sync-amazon-price-snapshot.js — カート価格スナップショット mirror sync (amazon-dashboard PR-D)
 *
 * fact_amazon_price_snapshot → mirror_amazon_price_snapshot_daily。
 * f_sales_by_listing 同型 (chunk POST + ledger + scope 全日付 clear)。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-amazon-price-snapshot.js --days 3 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-amazon-price-snapshot.js --days 35
 */
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

const ENTITY_NAME = 'amazon_price_snapshot_daily';
const CONTRACT_VERSION = 1;
const CHUNK_SIZE = Math.min(parseInt(process.env.CHUNK_SIZE, 10) || 3000, 5000);

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const days = Math.min(Math.max(parseInt(getArg('--days'), 10) || 3, 1), 90);
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2);
}

const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const toDate = nowJst.toISOString().slice(0, 10);
const fromDate = new Date(nowJst.getTime() - (days - 1) * 86400000).toISOString().slice(0, 10);
const scopeDates = [];
{
  let cur = fromDate;
  while (cur <= toDate) {
    scopeDates.push(cur);
    const d = new Date(cur + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 1);
    cur = d.toISOString().slice(0, 10);
  }
}

const db = new Database(dbPath, { readonly: true });
const contract = db.prepare(`SELECT * FROM sync_contracts WHERE entity = ?`).get(ENTITY_NAME);
if (!contract) {
  console.error(`FATAL: contract for entity '${ENTITY_NAME}' not found in sync_contracts (db.js auto-seed を確認)`);
  process.exit(1);
}
if (contract.contract_version !== CONTRACT_VERSION) {
  console.error(`FATAL: contract version mismatch: code=${CONTRACT_VERSION}, db=${contract.contract_version}`);
  process.exit(1);
}
console.log(`=== sync ${ENTITY_NAME} v${CONTRACT_VERSION} ===`);
console.log(`  scope: ${fromDate} 〜 ${toDate} / dry-run: ${isDryRun}`);

const rows = db.prepare(`
  SELECT date_jst, seller_sku, asin, channel, my_price, buybox_price, buybox_is_mine, fetched_at
  FROM fact_amazon_price_snapshot
  WHERE date_jst >= ? AND date_jst <= ?
  ORDER BY date_jst, seller_sku
`).all(fromDate, toDate);
db.close();
console.log(`  Rows to sync: ${rows.length.toLocaleString()}`);

const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
const runId = `${ENTITY_NAME}-v${CONTRACT_VERSION}-${tsCompact}-${crypto.randomBytes(3).toString('hex')}`;
const syncedAt = new Date().toISOString();
const enrichedRows = rows.map(r => {
  const hash = crypto.createHash('sha256')
    .update(JSON.stringify({ date_jst: r.date_jst, seller_sku: r.seller_sku, my_price: r.my_price, buybox_price: r.buybox_price, buybox_is_mine: r.buybox_is_mine }))
    .digest('hex').slice(0, 16);
  return { ...r, source_run_id: runId, source_row_hash: hash, synced_at: syncedAt };
});

const chunks = [];
for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
if (chunks.length === 0) chunks.push([]);
console.log(`  Chunks: ${chunks.length} / run_id: ${runId}`);

if (isDryRun) {
  console.log('  --- DRY RUN ---');
  console.log(`  First chunk meta: clear_amazon_price_snapshot_dates=[${scopeDates.length} dates]`);
  if (enrichedRows.length > 0) {
    const r0 = enrichedRows[0];
    console.log(`  Sample row[0]: ${r0.date_jst} ${r0.seller_sku} my=${r0.my_price} bb=${r0.buybox_price} mine=${r0.buybox_is_mine}`);
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
        fromDate, toDate, chunks.length, new Date().toISOString(),
        (process.env.REPLAY_OF_RUN_ID || '').trim() || null);
writeDb.close();

let totalRowsSent = 0, chunksApplied = 0, lastError = null;
for (let i = 0; i < chunks.length; i++) {
  const chunk = chunks[i];
  const payload = { rows: chunk };
  const payloadChecksum = crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  const body = {
    sync_run_id: runId, contract_version: CONTRACT_VERSION,
    scope_from: fromDate, scope_to: toDate,
    chunk_index: i, chunk_count: chunks.length,
    is_first: i === 0, is_last: i === chunks.length - 1,
    row_count: chunk.length, payload_checksum: payloadChecksum,
    meta: i === 0 ? { clear_amazon_price_snapshot_dates: scopeDates } : {},
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
// 通信 (mirror POST / GChat通知) 直後の process.exit() は Windows node で libuv assertion
// (UV_HANDLE_CLOSING / abort) を踏み、成功しているのに失敗扱いになる → exitCode + 自然終了 (#614 と同根)
process.exitCode = 0;
