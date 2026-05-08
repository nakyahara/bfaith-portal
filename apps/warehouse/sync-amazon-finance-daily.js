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
// CHUNK_SIZE は env で override 可能 (Render 側 timeout / payload size 調整用)
// 5000 だと Render 側で connection terminated になる事象あり、3000 推奨
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
// 3. chunk + ledger 記録 + send (or dry-run)
// ============================================================
const chunks = [];
for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) {
  chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
}
console.log(`\n  Chunks: ${chunks.length} (chunk_size=${CHUNK_SIZE})`);

// payload checksum 計算 (chunk 別)
import('node:os').then(async (os) => {
  const sourceHost = os.hostname();
  const startedAt = new Date().toISOString();

  // 3a. ledger に sync_runs 記録 (started)
  // replay_of_run_id は REPLAY_OF_RUN_ID env から読み込み (replay-sync-run.js が設定)
  // child 側で INSERT 時点で記録することで race を回避 (Codex 指摘 #6)
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
    console.log(`  First chunk meta: clear_amazon_finance_dates=[${distinctDates.length} dates]`);
    console.log(`  Sample row keys: ${Object.keys(enrichedRows[0]).join(', ')}`);
    console.log(`  Sample row[0]: date=${enrichedRows[0].date_jst} sku=${enrichedRows[0].seller_sku} profit=${enrichedRows[0].profit_amount} hash=${enrichedRows[0].source_row_hash}`);
    process.exit(0);
  }

  // 3b. 実 send (chunk loop)
  const cryptoMod = await import('node:crypto');
  let totalRowsSent = 0;
  let chunksApplied = 0;  // partial apply 記録用 (Codex Round 12 #4)
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
      meta: isFirst ? { clear_amazon_finance_dates: distinctDates } : {},
      payload,
    };

    console.log(`  [${i + 1}/${chunks.length}] sending ${chunk.length} rows...`);
    try {
      const res = await fetch(`${renderUrl}/api/sync/${ENTITY_NAME}/chunk`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-sync-key': syncKey,
        },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const text = await res.text();
        // 409 incomplete (is_last 受信後の欠番検出) は当該 chunk 自体は DB 反映済みなので
        // partial 計上に含める (Codex Round 13 #1)
        // parse 失敗時も保守的に当該 chunk を加算 (Codex Round 14 #1)
        if (res.status === 409 && isLast) {
          let parsed = null;
          try { parsed = JSON.parse(text); } catch { /* parse 失敗 */ }
          if (parsed && parsed.status === 'incomplete' && Number.isInteger(parsed.chunks_received)) {
            chunksApplied = parsed.chunks_received;
            totalRowsSent = Number.isInteger(parsed.rows_received) ? parsed.rows_received : totalRowsSent + chunk.length;
            lastError = `HTTP 409 incomplete: missing_chunks=${JSON.stringify(parsed.missing_chunks)} chunks_received=${parsed.chunks_received}/${chunks.length}`;
          } else {
            // parse 失敗 / 形式不一致でも 409 + isLast なら当該 chunk は反映済みと推定
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

  // 3c. ledger に最終 status 記録 (failed でも partial apply 状況を残す、Codex Round 12 #4)
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
