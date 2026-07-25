#!/usr/bin/env node
/**
 * sync-f-sales-by-listing.js — biz-ops-overview 業務目線売上 mirror sync (PR #155)
 *
 * f_sales_by_listing → mirror_f_sales_by_listing に sync。
 * Qoo10 / LINEギフト Phase 1 A-3 同型 (chunk POST + ledger)、CHUNK_SIZE=500。
 *
 * f_sales_by_listing は「業務目線の真の売上」(API 直接集計、税込、全モール統合)。
 * Phase 1 fact (f_*_finance_sku_daily_v1) とは別系統。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-f-sales-by-listing.js --month 2026-05 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-f-sales-by-listing.js --from 2026-02-19 --to 2026-05-19
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

const ENTITY_NAME = 'f_sales_by_listing';
const CONTRACT_VERSION = 1;
const CHUNK_SIZE = Math.min(parseInt(process.env.CHUNK_SIZE, 10) || 500, 500);

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthStr = getArg('--month');
const fromStr = getArg('--from');
const toStr = getArg('--to');
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }

let dateRange;
if (monthStr) {
  if (!/^\d{4}-\d{2}$/.test(monthStr)) { console.error('FATAL: --month must be YYYY-MM'); process.exit(2); }
  dateRange = { from: `${monthStr}-01`, to: `${monthStr}-31` };
} else if (fromStr && toStr) {
  function isRealDate(s) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
    const d = new Date(s + 'T00:00:00Z');
    return !isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
  }
  if (!isRealDate(fromStr) || !isRealDate(toStr)) {
    console.error(`FATAL: --from/--to must be real YYYY-MM-DD date`); process.exit(2);
  }
  if (fromStr > toStr) { console.error('FATAL: --from must be <= --to'); process.exit(2); }
  dateRange = { from: fromStr, to: toStr };
} else {
  console.error('FATAL: --month YYYY-MM or --from/--to YYYY-MM-DD required'); process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2);
}

const db = new Database(dbPath, { readonly: true });

const contract = db.prepare(`SELECT * FROM sync_contracts WHERE entity = ?`).get(ENTITY_NAME);
if (!contract) {
  console.error(`FATAL: contract for entity '${ENTITY_NAME}' not found in sync_contracts`);
  console.error(`  initDB() の auto-seed が走ってないか、apps/warehouse/db.js を最新版に更新してください`);
  process.exit(1);
}
if (contract.contract_version !== CONTRACT_VERSION) {
  console.error(`FATAL: contract version mismatch: code=${CONTRACT_VERSION}, db=${contract.contract_version}`); process.exit(1);
}
console.log(`=== sync ${ENTITY_NAME} v${CONTRACT_VERSION} ===`);
console.log(`  source: ${contract.source_object}`);
console.log(`  target: ${contract.target_table}`);
console.log(`  scope: ${dateRange.from} 〜 ${dateRange.to}`);
console.log(`  dry-run: ${isDryRun}`);

// 日本語列を英語キーに mapping (PR #156 hotfix: mirror 側英語列に統一、payload も英語)
const rows = db.prepare(`
  SELECT
    日付 AS date_jst, 月 AS month_ym, モール AS mall, モール商品コード AS item_code,
    チャネル AS channel, 商品名 AS item_name, 数量 AS units, 売上金額 AS sales_jpy_incl,
    注文数 AS order_count, データソース AS data_source, updated_at AS source_updated_at
  FROM f_sales_by_listing
  WHERE 日付 >= ? AND 日付 <= ?
  ORDER BY 日付, モール, モール商品コード, チャネル
`).all(dateRange.from, dateRange.to);

console.log(`\n  Rows to sync: ${rows.length.toLocaleString()}`);

// scope 全日付列挙 (0 行日付の Render 側 stale 削除のため)
const scopeDates = [];
{
  let cur = dateRange.from;
  while (cur <= dateRange.to) {
    scopeDates.push(cur);
    const d = new Date(cur + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 1);
    cur = d.toISOString().slice(0, 10);
  }
}
const distinctDates = scopeDates;
const presentDates = [...new Set(rows.map(r => r.date_jst))].sort();
console.log(`  Distinct dates in scope: ${scopeDates.length} (clear対象、${scopeDates[0]} 〜 ${scopeDates[scopeDates.length - 1]}) / 実 row 日付: ${presentDates.length}`);

const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
const runIdSalt = crypto.randomBytes(3).toString('hex');
const runId = `${ENTITY_NAME}-v${CONTRACT_VERSION}-${tsCompact}-${runIdSalt}`;
console.log(`  run_id: ${runId}`);

const syncedAt = new Date().toISOString();
const enrichedRows = rows.map(r => {
  const hashInput = JSON.stringify({
    date_jst: r.date_jst, mall: r.mall, item_code: r.item_code, channel: r.channel,
    units: r.units, sales_jpy_incl: r.sales_jpy_incl, order_count: r.order_count,
  });
  const hash = crypto.createHash('sha256').update(hashInput).digest('hex').slice(0, 16);
  return { ...r, source_run_id: runId, source_row_hash: hash, synced_at: syncedAt };
});

const chunks = [];
for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) {
  chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
}
if (chunks.length === 0) chunks.push([]);
console.log(`\n  Chunks: ${chunks.length} (chunk_size=${CHUNK_SIZE}${enrichedRows.length === 0 ? ', empty chunk for scope-clear only' : ''})`);

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
  }

  if (isDryRun) {
    console.log('\n--- DRY RUN ---');
    console.log(`  Would send ${chunks.length} chunks (${enrichedRows.length} rows total)`);
    console.log(`  First chunk meta: clear_f_sales_by_listing_dates=[${distinctDates.length} dates]`);
    if (enrichedRows.length > 0) {
      const r0 = enrichedRows[0];
      console.log(`  Sample row[0]: date=${r0.date_jst} mall=${r0.mall} item=${r0.item_code} units=${r0.units} sales=¥${r0.sales_jpy_incl} hash=${r0.source_row_hash}`);
    }
    process.exit(0);
  }

  let totalRowsSent = 0;
  let chunksApplied = 0;
  let lastError = null;

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const isFirst = i === 0;
    const isLast = i === chunks.length - 1;
    const payload = { rows: chunk };
    const payloadStr = JSON.stringify(payload);
    const payloadChecksum = crypto.createHash('sha256').update(payloadStr).digest('hex');

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
      meta: isFirst ? { clear_f_sales_by_listing_dates: distinctDates } : {},
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
    console.log(`\n✗ sync FAILED: ${lastError}`);
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
    // 通信 (mirror POST / GChat通知) 直後の process.exit() は Windows node で libuv assertion
    // (UV_HANDLE_CLOSING / abort) を踏み、成功しているのに失敗扱いになる → exitCode + 自然終了 (#614 と同根)
    process.exitCode = 0;
  }
}).catch(e => {
  console.error(`FATAL: ${e.message}`);
  process.exit(1);
});
