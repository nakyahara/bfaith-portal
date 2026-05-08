#!/usr/bin/env node
/**
 * replay-sync-run.js — Phase 1 #1-4a sync run replay
 *
 * 既存 run_id の scope を再 sync する。新 run_id を生成し、
 * sync_runs.replay_of_run_id に元 run_id を記録。
 *
 * Render 側は INSERT OR REPLACE on PK で idempotent (mirror テーブル PK は
 * date_jst × seller_sku × asin_norm)。同一 row が再送される。
 *
 * 使い方:
 *   DATA_DIR=... RENDER_MIRROR_URL=... MIRROR_SYNC_KEY=... \
 *     node apps/warehouse/replay-sync-run.js --run-id <original_run_id>
 *   --dry-run でシミュレーション
 *
 * 注: 内部で sync-amazon-finance-daily.js を呼び出して再 sync を実行。
 */

import path from 'node:path';
import fs from 'node:fs';
import { spawn } from 'node:child_process';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}

const DATA_DIR = (process.env.DATA_DIR || '').trim();
const originalRunId = getArg('--run-id');
const isDryRun = args.includes('--dry-run');

if (!DATA_DIR || !originalRunId) {
  console.error('FATAL: DATA_DIR env and --run-id required');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
const db = new Database(dbPath, { readonly: true });

const run = db.prepare(`SELECT * FROM sync_runs WHERE run_id = ?`).get(originalRunId);
if (!run) {
  console.error(`FATAL: run_id ${originalRunId} not found`);
  process.exit(1);
}

console.log(`=== replay sync run ===`);
console.log(`  original run_id: ${originalRunId}`);
console.log(`  entity: ${run.entity}`);
console.log(`  scope: ${run.scope_from} 〜 ${run.scope_to}`);
console.log(`  original status: ${run.status}`);
console.log(`  dry-run: ${isDryRun}`);

db.close();

// 内部で sync-amazon-finance-daily.js を呼び出し
const syncScript = path.join(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), 'sync-amazon-finance-daily.js');
const childArgs = [
  syncScript,
  '--from', run.scope_from,
  '--to', run.scope_to,
];
if (isDryRun) childArgs.push('--dry-run');

console.log(`\n  Spawning: node ${childArgs.join(' ')}`);

// child 側で REPLAY_OF_RUN_ID を読んで sync_runs INSERT 時に記録するため、
// replay 後の SELECT/UPDATE は不要 (Codex 指摘 #6 race 解消)
const child = spawn('node', childArgs, { stdio: 'inherit', env: { ...process.env, REPLAY_OF_RUN_ID: originalRunId } });
child.on('close', (code) => {
  if (code !== 0) {
    console.error(`\n✗ Replay child exited with code ${code}`);
    process.exit(code);
  }
  console.log(`\n✓ Replay complete (replay_of=${originalRunId}, child exited 0)`);
  process.exit(0);
});
