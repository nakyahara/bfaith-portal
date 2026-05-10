#!/usr/bin/env node
/**
 * sync-mf-marts-to-render.js — MFクラウド会計 Phase 1a sync (entity-driven + finalize)
 *
 * miniPC warehouse.db の mart_mf_* + mf_publish_runs を Render warehouse-mirror.db に sync。
 *
 * 設計 (Codex 5ラウンド確定):
 *   1. 直近 success の mf_publish_runs を 1 つ選ぶ (--run-id で override 可)
 *   2. mf_publish_runs entity を status='pending_sync' で先送信 (FK 親 row 作成)
 *   3. 6 mart entities (executive_top / pl_monthly / channel_sales /
 *                      cash_events_daily / balance_snapshot_monthly / anomaly_signals)
 *      を chunk 分割で送信、第1 chunk meta.mf_source_run_id=run_id
 *   4. POST /api/sync/mf/runs/:run_id/finalize で status='success' に flip
 *
 * 公開 VIEW v_mirror_mf_*_latest が finalize 完了瞬間にこの run を表示。
 *
 * 使い方:
 *   DATA_DIR=... node sync-mf-marts-to-render.js                      # 最新 success run を sync
 *   DATA_DIR=... node sync-mf-marts-to-render.js --run-id 42          # 指定 run_id を sync
 *   DATA_DIR=... node sync-mf-marts-to-render.js --dry-run            # 送信せず内容確認
 *
 * env:
 *   DATA_DIR             必須 (warehouse.db のディレクトリ)
 *   RENDER_MIRROR_URL    送信先 base URL (例: https://bfaith-portal.onrender.com)
 *   MIRROR_SYNC_KEY      認証 (x-sync-key header)
 *
 * exit code:
 *   0: success / dry-run
 *   1: 送信失敗 (chunk error / finalize 409)
 *   2: env error
 */

import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import os from 'node:os';
import Database from 'better-sqlite3';

const CONTRACT_VERSION = 1;
const CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE, 10) || 5000;

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}

const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const overrideRunId = getArg('--run-id');
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR is required');
  process.exit(2);
}
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exit(2);
}
if (!isDryRun && !renderUrl) {
  console.error('FATAL: RENDER_MIRROR_URL required for non-dry-run');
  process.exit(2);
}
if (!isDryRun && !syncKey) {
  console.warn('⚠️  MIRROR_SYNC_KEY 未設定: Render 側 ALLOW_INSECURE_MIRROR_SYNC=1 に依存');
}

const db = new Database(dbPath, { readonly: true });
db.pragma('busy_timeout = 5000');

// ============================================================
// 1. 対象 publish_run を選択
// ============================================================
let runRow;
if (overrideRunId) {
  runRow = db.prepare(`SELECT * FROM mf_publish_runs WHERE run_id = ?`).get(parseInt(overrideRunId, 10));
  if (!runRow) {
    console.error(`FATAL: run_id=${overrideRunId} が mf_publish_runs に存在しません`);
    process.exit(1);
  }
  if (runRow.status !== 'success') {
    console.error(`FATAL: run_id=${overrideRunId} は status=${runRow.status} (success のみ sync 可)`);
    process.exit(1);
  }
} else {
  runRow = db.prepare(`
    SELECT * FROM mf_publish_runs
    WHERE status = 'success'
    ORDER BY run_id DESC LIMIT 1
  `).get();
  if (!runRow) {
    console.log('[mf-sync] sync 対象の success run なし、終了');
    process.exit(0);
  }
}

const runId = runRow.run_id;
const scope = runRow.scope;
console.log(`=== MF mart sync (run_id=${runId}, scope=${scope}) ===`);
console.log(`  miniPC mf_publish_runs: status=${runRow.status} started=${runRow.started_at} finished=${runRow.finished_at}`);
console.log(`  dry-run: ${isDryRun}`);

// ============================================================
// 2. Entity 配列定義 + payload build
// ============================================================
const syncedAt = new Date().toISOString();
const sourceHost = os.hostname();
const tsCompact = syncedAt.replace(/[:.]/g, '').replace('Z', '');
const runSalt = crypto.randomBytes(3).toString('hex');

function makeSyncRunId(entity) {
  return `${entity}-v${CONTRACT_VERSION}-run${runId}-${tsCompact}-${runSalt}`;
}

function rowHash(obj) {
  return crypto.createHash('sha256').update(JSON.stringify(obj)).digest('hex').slice(0, 16);
}

function enrich(rows, hashKeys) {
  return rows.map(r => {
    const h = {};
    for (const k of hashKeys) h[k] = r[k];
    return { ...r, source_row_hash: rowHash(h), synced_at: syncedAt };
  });
}

// 7 entity の row 取得 + enrichment
// 親 mf_publish_runs は status を 'pending_sync' に override (Render 側で finalize 時 flip)
const entities = [];

entities.push({
  name: 'mf_publish_runs',
  rows: enrich([{
    run_id: runRow.run_id,
    scope: runRow.scope,
    status: 'pending_sync',                           // ← override
    started_at: runRow.started_at,
    finished_at: runRow.finished_at,
    error_message: runRow.error_message,
    source_run_hash: null,
    finalized_at: null,
  }], ['run_id', 'scope', 'started_at']),
});

entities.push({
  name: 'mf_executive_top',
  rows: enrich(
    db.prepare(`SELECT * FROM mart_mf_executive_top WHERE run_id = ?`).all(runId),
    ['run_id', 'snapshot_date', 'current_month_ym', 'sales_mtd_excl_tax']
  ),
});

entities.push({
  name: 'mf_pl_monthly',
  rows: enrich(
    db.prepare(`SELECT * FROM mart_mf_pl_monthly WHERE run_id = ?`).all(runId),
    ['run_id', 'month_ym', 'role_key', 'amount_excl_tax']
  ),
});

entities.push({
  name: 'mf_channel_sales',
  rows: enrich(
    db.prepare(`SELECT * FROM mart_mf_channel_sales WHERE run_id = ?`).all(runId),
    ['run_id', 'month_ym', 'channel_key', 'gross_sales_excl_tax']
  ),
});

entities.push({
  name: 'mf_cash_events_daily',
  rows: enrich(
    db.prepare(`SELECT * FROM mart_mf_cash_events_daily WHERE run_id = ?`).all(runId),
    ['run_id', 'movement_date', 'bank_account_key', 'direction', 'amount_excl_tax']
  ),
});

entities.push({
  name: 'mf_balance_snapshot_monthly',
  rows: enrich(
    db.prepare(`SELECT * FROM mart_mf_balance_snapshot_monthly WHERE run_id = ?`).all(runId),
    ['run_id', 'month_ym', 'account_key', 'closing_balance_excl_tax']
  ),
});

entities.push({
  name: 'mf_anomaly_signals',
  rows: enrich(
    db.prepare(`SELECT * FROM mart_mf_anomaly_signals WHERE run_id = ?`).all(runId),
    ['run_id', 'signal_id', 'signal_code', 'severity']
  ),
});

console.log('\n  Entities to sync:');
for (const e of entities) {
  console.log(`    ${e.name.padEnd(35)} ${e.rows.length.toString().padStart(6)} rows`);
}

if (isDryRun) {
  console.log('\n--- DRY RUN ---');
  console.log(`  Would send ${entities.length} entities for run_id=${runId} to ${renderUrl || '<unset>'}`);
  console.log(`  Would call POST /api/sync/mf/runs/${runId}/finalize after success`);
  process.exit(0);
}

// ============================================================
// 3. 各 entity を chunk 分割で送信
// ============================================================
const entityRunIds = {};                               // finalize 時に渡す
let totalRowsSent = 0;

const baseHeaders = { 'Content-Type': 'application/json' };
if (syncKey) baseHeaders['x-sync-key'] = syncKey;

async function sendChunk(entity, syncRunId, chunkIndex, chunkCount, chunk, isFirst, isLast, includeMfMeta) {
  const payload = { rows: chunk };
  const payloadStr = JSON.stringify(payload);
  const payloadChecksum = crypto.createHash('sha256').update(payloadStr).digest('hex');
  // scope は run_id 数値を文字列に (chunk endpoint validation: scope_from/to YYYY-MM-DD)
  // 受信側は scope を業務的には使わないので、started_at の日付を流用
  const scopeDate = (runRow.started_at || syncedAt).slice(0, 10);
  const meta = {};
  if (isFirst && includeMfMeta) {
    meta.mf_source_run_id = runId;
  }
  const body = {
    sync_run_id: syncRunId,
    contract_version: CONTRACT_VERSION,
    scope_from: scopeDate,
    scope_to: scopeDate,
    chunk_index: chunkIndex,
    chunk_count: chunkCount,
    is_first: isFirst,
    is_last: isLast,
    row_count: chunk.length,
    payload_checksum: payloadChecksum,
    meta,
    payload,
  };
  const res = await fetch(`${renderUrl}/api/sync/${entity}/chunk`, {
    method: 'POST',
    headers: baseHeaders,
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(120000),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text.slice(0, 400)}`);
  }
  return res.json();
}

console.log('\n[sync] 各 entity を順次送信...');
for (const e of entities) {
  const syncRunId = makeSyncRunId(e.name);
  entityRunIds[e.name] = syncRunId;
  // child (mart) entity のみ mf_source_run_id meta を含める
  // mf_publish_runs entity は親なので不要
  const includeMfMeta = e.name !== 'mf_publish_runs';
  // chunk 分割 (0 rows でも 1 chunk 送信して finalize の entity_run_ids に登場させる)
  const chunks = [];
  if (e.rows.length === 0) {
    chunks.push([]);
  } else {
    for (let i = 0; i < e.rows.length; i += CHUNK_SIZE) {
      chunks.push(e.rows.slice(i, i + CHUNK_SIZE));
    }
  }
  console.log(`  [${e.name}] ${e.rows.length} rows / ${chunks.length} chunks (sync_run_id=${syncRunId})`);
  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    try {
      const result = await sendChunk(e.name, syncRunId, i, chunks.length, chunk, i === 0, i === chunks.length - 1, includeMfMeta);
      totalRowsSent += chunk.length;
      console.log(`    ✓ chunk ${i + 1}/${chunks.length} (${chunk.length} rows) request_id=${result.request_id}`);
    } catch (err) {
      console.error(`    ✗ chunk ${i + 1}/${chunks.length} 失敗: ${err.message}`);
      console.error(`[mf-sync] FAIL: entity=${e.name} chunk=${i} run_id=${runId}`);
      process.exit(1);
    }
  }
}

// ============================================================
// 4. finalize endpoint POST
// ============================================================
console.log(`\n[sync] finalize POST /api/sync/mf/runs/${runId}/finalize`);
try {
  const finalizeRes = await fetch(`${renderUrl}/api/sync/mf/runs/${runId}/finalize`, {
    method: 'POST',
    headers: baseHeaders,
    body: JSON.stringify({ entity_run_ids: entityRunIds }),
    signal: AbortSignal.timeout(60000),
  });
  if (!finalizeRes.ok) {
    const text = await finalizeRes.text();
    console.error(`[mf-sync] finalize FAILED HTTP ${finalizeRes.status}: ${text.slice(0, 600)}`);
    process.exit(1);
  }
  const finalizeBody = await finalizeRes.json();
  console.log(`  ✓ finalize OK: status=${finalizeBody.status} finalized_at=${finalizeBody.finalized_at || 'already'} request_id=${finalizeBody.request_id}`);
} catch (err) {
  console.error(`[mf-sync] finalize 通信エラー: ${err.message}`);
  process.exit(1);
}

console.log(`\n✅ MF sync 完了 (run_id=${runId}, ${totalRowsSent} rows in ${entities.length} entities)`);
process.exit(0);
