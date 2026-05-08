#!/usr/bin/env node
/**
 * backout-sync-run.js — Phase 1 #1-4a sync run backout
 *
 * 指定 run_id の sync を取り消す:
 *   1. Render 側 mirror テーブルから source_run_id 一致 row を DELETE
 *   2. Render 側 sync_run_chunks から ledger 削除
 *   3. miniPC 側 sync_runs の status を 'backed_out' に更新
 *
 * 使い方:
 *   DATA_DIR=... RENDER_MIRROR_URL=... MIRROR_SYNC_KEY=... \
 *     node apps/warehouse/backout-sync-run.js --run-id <run_id>
 *   --dry-run で実行内容のみ表示
 *
 * env: DATA_DIR / RENDER_MIRROR_URL / MIRROR_SYNC_KEY
 */

import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}

const DATA_DIR = (process.env.DATA_DIR || '').trim();
const runId = getArg('--run-id');
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR || !runId) {
  console.error('FATAL: DATA_DIR env and --run-id required');
  process.exit(2);
}
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
const db = new Database(dbPath);

const run = db.prepare(`SELECT * FROM sync_runs WHERE run_id = ?`).get(runId);
if (!run) {
  console.error(`FATAL: run_id ${runId} not found in sync_runs`);
  process.exit(1);
}

console.log(`=== backout sync run ===`);
console.log(`  run_id: ${runId}`);
console.log(`  entity: ${run.entity}`);
console.log(`  status: ${run.status}`);
console.log(`  scope: ${run.scope_from} 〜 ${run.scope_to}`);
console.log(`  rows: ${run.row_count_received}`);
console.log(`  dry-run: ${isDryRun}`);

if (isDryRun) {
  console.log('\n  (dry-run) would:');
  console.log(`    1. POST ${renderUrl}/apps/mirror/api/sync/runs/${runId}/backout`);
  console.log(`    2. UPDATE sync_runs SET status='backed_out' WHERE run_id='${runId}'`);
  process.exit(0);
}

// 1. Render 側 backout API 呼び出し
console.log('\n  Calling Render backout API...');
try {
  const res = await fetch(`${renderUrl}/apps/mirror/api/sync/runs/${runId}/backout`, {
    method: 'POST',
    headers: { 'x-sync-key': syncKey },
  });
  if (!res.ok) {
    const text = await res.text();
    console.error(`  ✗ HTTP ${res.status}: ${text.slice(0, 300)}`);
    process.exit(1);
  }
  const result = await res.json();
  console.log(`  ✓ Render backout: ${JSON.stringify(result.deleted)}`);
} catch (e) {
  console.error(`  ✗ Render API error: ${e.message}`);
  process.exit(1);
}

// 2. miniPC 側 sync_runs status 更新
db.prepare(`
  UPDATE sync_runs SET status = 'backed_out' WHERE run_id = ?
`).run(runId);
console.log(`\n✓ Backout complete (run_id=${runId})`);

db.close();
process.exit(0);
