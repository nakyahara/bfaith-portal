#!/usr/bin/env node
/**
 * sync-rakuten-review-daily.js — 楽天レビュー 日次集計 mirror sync (mall-csv-fetcher P2 PR-A)
 *
 * ★mirror へ送るのは非PII の日次集計 projection のみ (Codex R1/R2 確定):
 *   fact_rakuten_reviews (本文・注文番号・URL を含む) は miniPC 内に留め、
 *   Render には 投稿日×種別×item_id×★ の件数だけを渡す。
 *   本文/タイトル/注文番号/レビューURL は絶対に payload に含めない。
 *
 * sync-rakuten-data-daily.js 同型 (chunk POST + ledger + scope clear)。
 * レビューは当日投稿があり得る (投稿時間=JST 即日CSVに出る) ため scope 終端=当日。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-review-daily.js --days 35 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-review-daily.js --from 2019-01-01 --to 2026-07-16
 *
 * env: DATA_DIR (必須) / RENDER_MIRROR_URL / MIRROR_SYNC_KEY / CHUNK_SIZE (default 3000)
 * exit code: 0=成功 / 1=失敗 / 2=env・引数エラー
 */
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

const CHUNK_SIZE = Math.min(parseInt(process.env.CHUNK_SIZE, 10) || 3000, 5000);

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const fromStr = getArg('--from');
const toStr = getArg('--to');
const daysStr = getArg('--days');
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }

function isRealDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

const todayJst = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
let dateRange;
if (fromStr && toStr) {
  if (!isRealDate(fromStr) || !isRealDate(toStr)) { console.error('FATAL: --from/--to must be real YYYY-MM-DD'); process.exit(2); }
  if (fromStr > toStr) { console.error('FATAL: --from must be <= --to'); process.exit(2); }
  dateRange = { from: fromStr, to: toStr };
} else if (daysStr) {
  const days = parseInt(daysStr, 10);
  if (!Number.isInteger(days) || days <= 0 || days > 400) { console.error('FATAL: --days must be 1..400'); process.exit(2); }
  // レビューは当日投稿分が即日CSVに出る → scope 終端=当日JST
  const fromDate = new Date(Date.now() + 9 * 3600 * 1000 - (days - 1) * 86400000).toISOString().slice(0, 10);
  dateRange = { from: fromDate, to: todayJst };
} else {
  console.error('FATAL: --days N or --from/--to YYYY-MM-DD required');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2);
}

function enumerateScopeDates(from, to) {
  const dates = [];
  let cur = from;
  while (cur <= to) {
    dates.push(cur);
    const d = new Date(cur + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 1);
    cur = d.toISOString().slice(0, 10);
  }
  return dates;
}

const ENTITY = {
  name: 'rakuten_review_daily',
  contractVersion: 1,
  clearMetaKey: 'clear_rakuten_review_daily_dates',
  // 非PII projection: 日×種別×item×★ の件数。item_name は公開商品名 (最新値) を代表値で付与
  selectSql: `
    SELECT
      date_jst,
      review_type,
      COALESCE(item_id, 0) AS item_id,
      CASE WHEN review_type = 'item' THEN MAX(item_name) ELSE NULL END AS item_name,
      rating,
      COUNT(*) AS review_count
    FROM fact_rakuten_reviews
    WHERE is_deleted = 0 AND date_jst >= ? AND date_jst <= ?
    GROUP BY date_jst, review_type, COALESCE(item_id, 0), rating
    ORDER BY date_jst, review_type, item_id, rating
  `,
};

async function syncEntity(entity) {
  const db = new Database(dbPath, { readonly: true });
  let rows;
  try {
    const contract = db.prepare(`SELECT * FROM sync_contracts WHERE entity = ?`).get(entity.name);
    if (!contract) {
      console.error(`FATAL: contract for entity '${entity.name}' not found in sync_contracts`);
      console.error('  initDB() の auto-seed が走ってないか、apps/warehouse/db.js を最新版に更新してください');
      return { entity: entity.name, ok: false, error: 'contract_not_found' };
    }
    if (contract.contract_version !== entity.contractVersion) {
      console.error(`FATAL: contract version mismatch: code=${entity.contractVersion}, db=${contract.contract_version}`);
      return { entity: entity.name, ok: false, error: 'contract_version_mismatch' };
    }
    console.log(`\n=== sync ${entity.name} v${entity.contractVersion} ===`);
    console.log(`  source: ${contract.source_object} (集計projection)`);
    console.log(`  target: ${contract.target_table}`);
    console.log(`  scope: ${dateRange.from} 〜 ${dateRange.to}`);
    console.log(`  dry-run: ${isDryRun}`);

    const exists = db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(contract.source_object);
    if (!exists) {
      console.error(`  ✗ source table ${contract.source_object} が未作成。先に import-rakuten-review.js を実行してください`);
      return { entity: entity.name, ok: false, error: 'source_table_missing' };
    }
    rows = db.prepare(entity.selectSql).all(dateRange.from, dateRange.to);
  } finally {
    db.close();
  }

  console.log(`  Rows to sync: ${rows.length.toLocaleString()}`);
  const scopeDates = enumerateScopeDates(dateRange.from, dateRange.to);
  console.log(`  Scope dates: ${scopeDates.length} (clear対象)`);

  const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const runIdSalt = crypto.randomBytes(3).toString('hex');
  const runId = `${entity.name}-v${entity.contractVersion}-${tsCompact}-${runIdSalt}`;
  console.log(`  run_id: ${runId}`);

  const syncedAt = new Date().toISOString();
  const enrichedRows = rows.map(r => {
    const hash = crypto.createHash('sha256')
      .update(JSON.stringify({ d: r.date_jst, t: r.review_type, i: r.item_id, s: r.rating, c: r.review_count }))
      .digest('hex').slice(0, 16);
    return { ...r, source_run_id: runId, source_row_hash: hash, synced_at: syncedAt };
  });

  const chunks = [];
  for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
  if (chunks.length === 0) chunks.push([]); // empty chunk: scope-clear のみ
  console.log(`  Chunks: ${chunks.length} (chunk_size=${CHUNK_SIZE}${enrichedRows.length === 0 ? ', empty chunk for scope-clear only' : ''})`);

  if (isDryRun) {
    console.log('  --- DRY RUN ---');
    console.log(`  Would send ${chunks.length} chunks (${enrichedRows.length} rows total)`);
    if (enrichedRows.length > 0) {
      const r = enrichedRows[0];
      console.log(`  Sample row[0]: date=${r.date_jst} type=${r.review_type} item=${r.item_id} ★${r.rating} ×${r.review_count}`);
    }
    return { entity: entity.name, ok: true, dryRun: true, rows: enrichedRows.length };
  }

  const sourceHost = os.hostname();
  const startedAt = new Date().toISOString();
  const replayOfRunId = (process.env.REPLAY_OF_RUN_ID || '').trim() || null;
  {
    const writeDb = new Database(dbPath);
    writeDb.prepare(`
      INSERT INTO sync_runs (
        run_id, entity, contract_version, source_host,
        scope_from, scope_to, chunk_count_expected,
        status, started_at, replay_of_run_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 'started', ?, ?)
    `).run(runId, entity.name, entity.contractVersion, sourceHost,
            dateRange.from, dateRange.to, chunks.length, startedAt, replayOfRunId);
    writeDb.close();
  }

  let totalRowsSent = 0;
  let chunksApplied = 0;
  let lastError = null;

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const payload = { rows: chunk };
    const payloadChecksum = crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex');
    const body = {
      sync_run_id: runId,
      contract_version: entity.contractVersion,
      scope_from: dateRange.from,
      scope_to: dateRange.to,
      chunk_index: i,
      chunk_count: chunks.length,
      is_first: i === 0,
      is_last: i === chunks.length - 1,
      row_count: chunk.length,
      payload_checksum: payloadChecksum,
      meta: i === 0 ? { [ENTITY.clearMetaKey]: scopeDates } : {},
      payload,
    };

    console.log(`  [${i + 1}/${chunks.length}] sending ${chunk.length} rows...`);
    const chunkHeaders = { 'Content-Type': 'application/json' };
    if (syncKey) chunkHeaders['x-sync-key'] = syncKey;
    try {
      const res = await fetch(`${renderUrl}/api/sync/${entity.name}/chunk`, {
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
  try {
    if (lastError) {
      finishDb.prepare(`
        UPDATE sync_runs SET status = 'failed', error_message = ?, completed_at = ?,
          chunk_count_received = ?, row_count_received = ?
         WHERE run_id = ?
      `).run(lastError, new Date().toISOString(), chunksApplied, totalRowsSent, runId);
      console.log(`✗ ${entity.name} sync FAILED: ${lastError}`);
      return { entity: entity.name, ok: false, error: lastError };
    }
    finishDb.prepare(`
      UPDATE sync_runs SET status = 'applied', chunk_count_received = ?,
        row_count_received = ?, completed_at = ?, applied_at = ?
       WHERE run_id = ?
    `).run(chunksApplied, totalRowsSent, new Date().toISOString(), new Date().toISOString(), runId);
    console.log(`✓ ${entity.name} sync complete (run_id=${runId}, ${totalRowsSent} rows in ${chunksApplied} chunks)`);
    return { entity: entity.name, ok: true, rows: totalRowsSent };
  } finally {
    finishDb.close();
  }
}

let outcome;
try {
  outcome = await syncEntity(ENTITY);
} catch (e) {
  console.error(`FATAL (${ENTITY.name}): ${e.message}`);
  outcome = { entity: ENTITY.name, ok: false, error: e.message };
}

console.log(`\n=== summary: ${outcome.ok ? '1/1 entities OK' : 'FAILED'} ===`);
// fetch 直後の process.exit() は Windows libuv assertion crash で exit code が化ける (#464 と同根)
process.exitCode = outcome.ok ? 0 : 1;
