#!/usr/bin/env node
/**
 * sync-yahoo-finance-daily.js — Yahoo Phase 1 Y-3a entity-driven sync
 *
 * f_yahoo_finance_sku_daily_v1 → mirror_yahoo_finance_sku_daily に sync。
 * 楽天 Phase 1a #R-3a (sync-rakuten-finance-daily.js、PR #78) と同型。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-yahoo-finance-daily.js --month 2026-04 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-yahoo-finance-daily.js --month 2026-04
 *
 * env:
 *   DATA_DIR             必須
 *   RENDER_MIRROR_URL    送信先 base URL
 *   MIRROR_SYNC_KEY      認証 (x-sync-key header、Phase 1a.1 #82 で必須化)
 */

import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

const ENTITY_NAME = 'yahoo_finance_sku_daily';
const CONTRACT_VERSION = 1;
const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE, 10) || 5000;

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

// Phase 1a.1 (Issue #82) 完了: KEY 必須に再 strict 化
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
  console.error(`  Run: sqlite3 warehouse.db < sql/sync/seed_yahoo_finance_sku_daily.sql`);
  process.exit(1);
}
if (contract.contract_version !== CONTRACT_VERSION) {
  console.error(`FATAL: contract version mismatch: code=${CONTRACT_VERSION}, db=${contract.contract_version}`);
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
  SELECT * FROM f_yahoo_finance_sku_daily_v1
  WHERE date_jst >= ? AND date_jst <= ?
  ORDER BY date_jst, yahoo_sku_key
`).all(dateRange.from, dateRange.to);

console.log(`\n  Rows to sync: ${rows.length.toLocaleString()}`);

if (rows.length === 0) {
  console.log('  (no rows in range, nothing to sync)');
  process.exit(0);
}

const distinctDates = [...new Set(rows.map(r => r.date_jst))].sort();
console.log(`  Distinct dates: ${distinctDates.length} (${distinctDates[0]} 〜 ${distinctDates[distinctDates.length - 1]})`);

const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
const runIdSalt = crypto.randomBytes(3).toString('hex');
const runId = `${ENTITY_NAME}-v${CONTRACT_VERSION}-${tsCompact}-${runIdSalt}`;
console.log(`  run_id: ${runId}`);

const syncedAt = new Date().toISOString();
const enrichedRows = rows.map(r => {
  const hashInput = JSON.stringify({
    date_jst: r.date_jst,
    yahoo_sku_key: r.yahoo_sku_key,
    ne_code: r.ne_code || '',
    variable_margin_partial_jpy_incl: r.variable_margin_partial_jpy_incl,
    cogs_amount_jpy_incl: r.cogs_amount_jpy_incl,
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
// 3. chunk + ledger 記録 + send
// ============================================================
const chunks = [];
for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) {
  chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
}
console.log(`\n  Chunks: ${chunks.length} (chunk_size=${CHUNK_SIZE})`);

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
    console.log(`  First chunk meta: clear_yahoo_finance_dates=[${distinctDates.length} dates]`);
    console.log(`  Sample row keys: ${Object.keys(enrichedRows[0]).join(', ')}`);
    const r0 = enrichedRows[0];
    console.log(`  Sample row[0]: date=${r0.date_jst} yahoo_sku_key=${r0.yahoo_sku_key} ne_code=${r0.ne_code} margin_partial=${r0.variable_margin_partial_jpy_incl} hash=${r0.source_row_hash}`);
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
      meta: isFirst ? { clear_yahoo_finance_dates: distinctDates } : {},
      payload,
    };

    console.log(`  [${i + 1}/${chunks.length}] sending ${chunk.length} rows...`);
    const chunkHeaders = { 'Content-Type': 'application/json' };
    if (syncKey) chunkHeaders['x-sync-key'] = syncKey;
    try {
      const res = await fetch(`${renderUrl}/api/sync/${ENTITY_NAME}/chunk`, {
        method: 'POST',
        headers: chunkHeaders,
        body: JSON.stringify(body),
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
            chunksApplied += 1;
            totalRowsSent += chunk.length;
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

    const rebuildHeaders = {};
    if (syncKey) rebuildHeaders['x-sync-key'] = syncKey;
    try {
      const rebuildRes = await fetch(`${renderUrl}/api/sync/runs/${runId}/rebuild-marts`, {
        method: 'POST',
        headers: rebuildHeaders,
        signal: AbortSignal.timeout(30000),
      });
      if (rebuildRes.ok) {
        const rb = await rebuildRes.json();
        console.log(`✓ rebuild-marts trigger: entities=${(rb.triggered_entities || []).join(',')} (${rb.note || 'ok'})`);
      } else {
        const text = await rebuildRes.text();
        console.warn(`⚠ rebuild-marts trigger failed (HTTP ${rebuildRes.status}): ${text.slice(0, 200)}`);
      }
    } catch (e) {
      console.warn(`⚠ rebuild-marts trigger error (sync は applied 維持): ${e.message}`);
    }

    process.exit(0);
  }
}).catch(e => {
  console.error(`FATAL: ${e.message}`);
  process.exit(1);
});
