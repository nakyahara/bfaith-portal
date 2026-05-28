#!/usr/bin/env node
/**
 * sync-linegift-orders.js — LINEギフト v1.2 (PR-H) orders 集計用 subset sync
 *
 * miniPC raw_linegift_orders → Render mirror_linegift_orders に sync。
 * 時間帯ヒートマップ (曜日 × 00-23 時) と将来のリピート/同梱分析の基盤。
 *
 * ★ PII 列 (user_name=LINE ID, address_*, delivery_*, sku_name, parent_item_name) は
 *   SELECT 列を制限し Render に送らない。
 *
 * sync-linegift-finance-daily.js (entity-driven sync) と同型、差分は:
 *   - ENTITY_NAME = 'linegift_orders'
 *   - clear_strategy = 'no_clear' (order_id PK で INSERT OR REPLACE)
 *   - SELECT は raw_linegift_orders subset
 *   - scope は bought_date_jst で絞る
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-linegift-orders.js --month 2026-05 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-linegift-orders.js --from 2026-03-01 --to 2026-05-31
 *
 * env:
 *   DATA_DIR             必須
 *   RENDER_MIRROR_URL    送信先 base URL
 *   MIRROR_SYNC_KEY      認証 (x-sync-key header)
 */

import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

const ENTITY_NAME = 'linegift_orders';
const CONTRACT_VERSION = 1;
const CHUNK_SIZE = Math.min(parseInt(process.env.CHUNK_SIZE, 10) || 500, 500);

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
  console.error(`  Run: sql/sync/seed_linegift_orders.sql を適用`);
  process.exit(1);
}
if (contract.contract_version !== CONTRACT_VERSION) {
  console.error(`FATAL: contract version mismatch: code=${CONTRACT_VERSION}, db=${contract.contract_version}`);
  process.exit(1);
}
console.log(`=== sync ${ENTITY_NAME} v${CONTRACT_VERSION} ===`);
console.log(`  source: ${contract.source_object}`);
console.log(`  target: ${contract.target_table}`);
console.log(`  scope: bought_date_jst ${dateRange.from} 〜 ${dateRange.to}`);
console.log(`  dry-run: ${isDryRun}`);

// ============================================================
// 2. payload build (PII 列を SELECT に含めない)
// ============================================================
const rows = db.prepare(`
  SELECT
    order_id, status, sku_code, parent_item_code,
    selling_price, fee,
    bought_at_jst, bought_date_jst, received_date_jst,
    is_frozen_after_horizon
  FROM raw_linegift_orders
  WHERE bought_date_jst >= ? AND bought_date_jst <= ?
  ORDER BY bought_at_jst, order_id
`).all(dateRange.from, dateRange.to);

console.log(`\n  Rows to sync: ${rows.length.toLocaleString()}`);

const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
const runIdSalt = crypto.randomBytes(3).toString('hex');
const runId = `${ENTITY_NAME}-v${CONTRACT_VERSION}-${tsCompact}-${runIdSalt}`;
console.log(`  run_id: ${runId}`);

const syncedAt = new Date().toISOString();
const enrichedRows = rows.map(r => ({ ...r, source_run_id: runId, synced_at: syncedAt }));

// ============================================================
// 3. chunk + ledger + send
// ============================================================
const chunks = [];
for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) {
  chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
}
// no_clear なので 0-row 時は何もしない (空 chunk 送信不要、Render 側 clear なし)
console.log(`\n  Chunks: ${chunks.length} (chunk_size=${CHUNK_SIZE})`);
if (chunks.length === 0) {
  console.log('  (0 rows in scope, skipping send)');
  process.exit(0);
}

import('node:os').then(async (os) => {
  const sourceHost = os.hostname();
  const startedAt = new Date().toISOString();

  const replayOfRunId = (process.env.REPLAY_OF_RUN_ID || '').trim() || null;
  if (!isDryRun) {
    db.close();
    const writeDb = new Database(dbPath);
    writeDb.prepare(`
      INSERT INTO sync_runs (
        run_id, entity, contract_version, source_host,
        scope_from, scope_to, chunk_count_expected,
        status, started_at, replay_of_run_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 'started', ?, ?)
    `).run(runId, ENTITY_NAME, CONTRACT_VERSION, sourceHost,
            dateRange.from, dateRange.to, chunks.length, startedAt, replayOfRunId);
    writeDb.close();
    if (replayOfRunId) console.log(`  replay_of_run_id: ${replayOfRunId}`);
  }

  if (isDryRun) {
    console.log('\n--- DRY RUN ---');
    console.log(`  Would send ${chunks.length} chunks (${enrichedRows.length} rows total) to ${renderUrl || '<RENDER_MIRROR_URL not set>'}`);
    if (enrichedRows.length > 0) {
      const r0 = enrichedRows[0];
      console.log(`  Sample row[0]: order_id=${r0.order_id} status=${r0.status} sku=${r0.sku_code} bought_at_jst=${r0.bought_at_jst} price=${r0.selling_price}`);
    }
    process.exit(0);
  }

  const cryptoMod = await import('node:crypto');
  let totalRowsSent = 0;
  let chunksApplied = 0;
  let lastError = null;

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const isFirst = i === 0;
    const isLast = i === chunks.length - 1;
    const payload = { rows: chunk };
    const payloadStr = JSON.stringify(payload);
    const payloadChecksum = cryptoMod.createHash('sha256').update(payloadStr).digest('hex');

    const body = {
      sync_run_id: runId,
      contract_version: CONTRACT_VERSION,
      scope_from: dateRange.from,
      scope_to: dateRange.to,
      chunk_index: i,
      chunk_count: chunks.length,
      is_first: isFirst,
      is_last: isLast,
      row_count: chunk.length,
      payload_checksum: payloadChecksum,
      meta: {},                       // no_clear なので空 meta
      payload,
    };

    console.log(`  [${i + 1}/${chunks.length}] sending ${chunk.length} rows...`);
    const chunkHeaders = { 'Content-Type': 'application/json' };
    if (syncKey) chunkHeaders['x-sync-key'] = syncKey;
    try {
      const res = await fetch(`${renderUrl}/api/sync/${ENTITY_NAME}/chunk`, {
        method: 'POST', headers: chunkHeaders, body: JSON.stringify(body),
      });
      if (!res.ok) {
        const text = await res.text();
        if (res.status === 409 && isLast) {
          let parsed = null;
          try { parsed = JSON.parse(text); } catch { /* parse 失敗 */ }
          if (parsed && parsed.status === 'incomplete' && Number.isInteger(parsed.chunks_received)) {
            chunksApplied = parsed.chunks_received;
            totalRowsSent = Number.isInteger(parsed.rows_received) ? parsed.rows_received : totalRowsSent + chunk.length;
            lastError = `HTTP 409 incomplete: missing_chunks=${JSON.stringify(parsed.missing_chunks)} chunks_received=${parsed.chunks_received}/${chunks.length}`;
          } else {
            chunksApplied += 1; totalRowsSent += chunk.length;
            lastError = `HTTP 409 (body parse failed, conservatively counted as applied): ${text.slice(0, 300)}`;
          }
          console.error(`  ✗ chunk ${i} accepted but run incomplete: ${lastError}`);
          break;
        }
        lastError = `HTTP ${res.status}: ${text.slice(0, 300)}`;
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
        chunk_count_received = ?, row_count_received = ?
       WHERE run_id = ?
    `).run(lastError, new Date().toISOString(), chunksApplied, totalRowsSent, runId);
    console.log(`\n✗ sync FAILED at chunk ${chunksApplied}/${chunks.length} (${totalRowsSent} rows applied): ${lastError}`);
    finishDb.close();
    process.exit(1);
  } else {
    finishDb.prepare(`
      UPDATE sync_runs SET status = 'applied', chunk_count_received = ?,
        row_count_received = ?, completed_at = ?, applied_at = ?
       WHERE run_id = ?
    `).run(chunksApplied, totalRowsSent, new Date().toISOString(), new Date().toISOString(), runId);
    console.log(`\n✓ sync complete (run_id=${runId}, ${totalRowsSent} rows in ${chunksApplied} chunks)`);
    finishDb.close();
    process.exit(0);
  }
}).catch(e => {
  console.error(`FATAL: ${e.message}`);
  process.exit(1);
});
