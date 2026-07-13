#!/usr/bin/env node
/**
 * sync-aupay-data-daily.js — au PAYマーケット分析CSV mirror sync (mall-csv-fetcher P1-A)
 *
 * fact_aupay_* 13種を Render mirror へ sync。sync-yahoo-data-daily.js と同型
 * (chunk POST + ledger + scope_clear_per_run)。過去分は再DLで変動しうるため
 * scope 内は 0 行日付も含めて clear → 再投入。月次 entity の clear キーは月初日。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-aupay-data-daily.js --days 110 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-aupay-data-daily.js --from 2020-08-01 --to 2026-07-12
 *   DATA_DIR=... node apps/warehouse/sync-aupay-data-daily.js --days 110 --entity aupay_sales_daily
 *
 * env: DATA_DIR (必須) / RENDER_MIRROR_URL / MIRROR_SYNC_KEY / CHUNK_SIZE (default 3000)
 * exit code: 0=全 entity 成功 / 1=いずれか失敗 / 2=env・引数エラー
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
const monthStr = getArg('--month');
const fromStr = getArg('--from');
const toStr = getArg('--to');
const daysStr = getArg('--days');
const entityFilter = getArg('--entity');
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }

function isRealDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

let globalDateRange;
if (monthStr) {
  if (!/^\d{4}-\d{2}$/.test(monthStr)) { console.error('FATAL: --month must be YYYY-MM'); process.exit(2); }
  const [y, m] = monthStr.split('-').map(Number);
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  globalDateRange = { from: `${monthStr}-01`, to: `${monthStr}-${String(lastDay).padStart(2, '0')}` };
} else if (fromStr && toStr) {
  if (!isRealDate(fromStr) || !isRealDate(toStr)) {
    console.error('FATAL: --from/--to must be real YYYY-MM-DD date'); process.exit(2);
  }
  if (fromStr > toStr) { console.error('FATAL: --from must be <= --to'); process.exit(2); }
  globalDateRange = { from: fromStr, to: toStr };
} else if (daysStr) {
  const days = parseInt(daysStr, 10);
  if (!Number.isInteger(days) || days <= 0 || days > 400) {
    console.error('FATAL: --days must be 1..400'); process.exit(2);
  }
  // JST 基準。分析CSVの集計は昨日まで → scope 終端=昨日
  const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
  const yesterdayJst = new Date(nowJst.getTime() - 86400000);
  const toDate = yesterdayJst.toISOString().slice(0, 10);
  const fromDate = new Date(yesterdayJst.getTime() - (days - 1) * 86400000).toISOString().slice(0, 10);
  globalDateRange = { from: fromDate, to: toDate };
} else {
  console.error('FATAL: --days N or --month YYYY-MM or --from/--to YYYY-MM-DD required'); process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2);
}

// scope 全日付列挙 (0 行日付の Render 側 stale 削除のため)
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

// scope 内の月初日列挙 (月次 entity の clear キー)
function enumerateScopeMonthStarts(from, to) {
  const starts = [];
  let ym = from.slice(0, 7);
  const toYm = to.slice(0, 7);
  while (ym <= toYm) {
    starts.push(`${ym}-01`);
    const [y, m] = ym.split('-').map(Number);
    const ny = m === 12 ? y + 1 : y;
    const nm = m === 12 ? 1 : m + 1;
    ym = `${ny}-${String(nm).padStart(2, '0')}`;
  }
  return starts;
}

// ============================================================
// entity 定義 (全て SELECT * + dropCols。日次/月次で clear キーの列挙が異なる)
// ============================================================
const AUPAY_DROP_COLS = ['source_file', 'import_id', 'updated_at'];

function shopEntity(base, keyCols, sampleFn) {
  const out = [];
  for (const grain of ['daily', 'monthly']) {
    out.push({
      name: `aupay_${base}_${grain}`,
      clearMetaKey: `clear_aupay_${base}_${grain === 'daily' ? 'dates' : 'months'}`,
      monthly: grain === 'monthly',
      selectSql: `SELECT * FROM fact_aupay_${base}_${grain} WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst, ${keyCols.join(', ')}`,
      hashRow: (r) => Object.fromEntries(['date_jst', ...keyCols, 'sales_yen', 'visits', 'pageviews', 'orders']
        .filter(k => k in r).map(k => [k, r[k]])),
      sampleLog: sampleFn,
    });
  }
  return out;
}

const ENTITIES = [
  ...shopEntity('sales', ['segment_code'], (r) => `date=${r.date_jst} seg=${r.segment_code} sales=¥${r.sales_yen}`),
  ...shopEntity('referer', ['segment_code', 'channel'], (r) => `date=${r.date_jst} ${r.segment_code}/${r.channel} visits=${r.visits}`),
  ...shopEntity('search', ['segment_code', 'keyword'], (r) => `date=${r.date_jst} kw=${String(r.keyword).slice(0, 16)} visits=${r.visits}`),
  ...shopEntity('page', ['page_url'], (r) => `date=${r.date_jst} url=${String(r.page_url).slice(0, 30)} pv=${r.pageviews}`),
  ...shopEntity('product', ['segment_code', 'lot_number'], (r) => `date=${r.date_jst} lot=${r.lot_number} sales=¥${r.sales_yen}`),
  {
    name: 'aupay_repeat_cohort_monthly',
    clearMetaKey: 'clear_aupay_repeat_cohort_months',
    monthly: true,
    selectSql: `SELECT * FROM fact_aupay_repeat_cohort_monthly WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst, segment_code, target_month_jst`,
    hashRow: (r) => ({ d: r.date_jst, s: r.segment_code, t: r.target_month_jst, o: r.orders }),
    sampleLog: (r) => `base=${r.date_jst} seg=${r.segment_code} tgt=${r.target_month_jst} orders=${r.orders}`,
  },
  {
    name: 'aupay_pm_ad_daily',
    clearMetaKey: 'clear_aupay_pm_ad_dates',
    selectSql: `SELECT * FROM fact_aupay_pm_ad_daily WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst`,
    hashRow: (r) => ({ d: r.date_jst, i: r.impressions, c: r.clicks, cost: r.cost_yen }),
    sampleLog: (r) => `date=${r.date_jst} imp=${r.impressions} cost=¥${r.cost_yen}`,
  },
  {
    name: 'aupay_pm_query_weekly',
    clearMetaKey: 'clear_aupay_pm_query_dates',
    selectSql: `SELECT * FROM fact_aupay_pm_query_weekly WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst, keyword`,
    hashRow: (r) => ({ d: r.date_jst, k: r.keyword, i: r.impressions, c: r.clicks }),
    sampleLog: (r) => `week=${r.date_jst} kw=${String(r.keyword).slice(0, 16)} imp=${r.impressions}`,
  },
].map((e) => ({
  contractVersion: 1,
  scopeDates: (range) => (e.monthly ? enumerateScopeMonthStarts(range.from, range.to) : enumerateScopeDates(range.from, range.to)),
  selectParams: (range) => [range.from, range.to],
  dropCols: AUPAY_DROP_COLS,
  ...e,
}));

const targetEntities = entityFilter
  ? ENTITIES.filter(e => e.name === entityFilter)
  : ENTITIES;
if (targetEntities.length === 0) {
  console.error(`FATAL: unknown --entity '${entityFilter}' (valid: ${ENTITIES.map(e => e.name).join(', ')})`);
  process.exit(2);
}

async function syncEntity(entity) {
  const dateRange = globalDateRange;
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
    console.log(`  source: ${contract.source_object}`);
    console.log(`  target: ${contract.target_table}`);
    console.log(`  scope: ${dateRange.from} 〜 ${dateRange.to}`);
    console.log(`  dry-run: ${isDryRun}`);

    const tableName = contract.source_object;
    const exists = db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(tableName);
    if (!exists) {
      console.error(`  ✗ source table ${tableName} が未作成。先に import-aupay-data.js を実行してください`);
      return { entity: entity.name, ok: false, error: 'source_table_missing' };
    }
    rows = db.prepare(entity.selectSql).all(...entity.selectParams(dateRange));
    if (entity.dropCols) {
      for (const r of rows) for (const c of entity.dropCols) delete r[c];
    }
  } finally {
    db.close();
  }

  console.log(`  Rows to sync: ${rows.length.toLocaleString()}`);
  const scopeDates = entity.scopeDates(dateRange);
  const presentDates = [...new Set(rows.map(r => r.date_jst))].sort();
  console.log(`  Scope dates: ${scopeDates.length} (clear対象) / 実 row 日付: ${presentDates.length}`);

  const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const runIdSalt = crypto.randomBytes(3).toString('hex');
  const runId = `${entity.name}-v${entity.contractVersion}-${tsCompact}-${runIdSalt}`;
  console.log(`  run_id: ${runId}`);

  const syncedAt = new Date().toISOString();
  const enrichedRows = rows.map(r => {
    const hash = crypto.createHash('sha256')
      .update(JSON.stringify(entity.hashRow(r))).digest('hex').slice(0, 16);
    return { ...r, source_run_id: runId, source_row_hash: hash, synced_at: syncedAt };
  });

  const chunks = [];
  for (let i = 0; i < enrichedRows.length; i += CHUNK_SIZE) {
    chunks.push(enrichedRows.slice(i, i + CHUNK_SIZE));
  }
  if (chunks.length === 0) chunks.push([]); // empty chunk: scope-clear のみ実行
  console.log(`  Chunks: ${chunks.length} (chunk_size=${CHUNK_SIZE}${enrichedRows.length === 0 ? ', empty chunk for scope-clear only' : ''})`);

  if (isDryRun) {
    console.log('  --- DRY RUN ---');
    console.log(`  Would send ${chunks.length} chunks (${enrichedRows.length} rows total)`);
    console.log(`  First chunk meta: ${entity.clearMetaKey}=[${scopeDates.length} dates]`);
    if (enrichedRows.length > 0) console.log(`  Sample row[0]: ${entity.sampleLog(enrichedRows[0])}`);
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
    const isFirst = i === 0;
    const isLast = i === chunks.length - 1;
    const payload = { rows: chunk };
    const payloadChecksum = crypto.createHash('sha256')
      .update(JSON.stringify(payload)).digest('hex');

    const body = {
      sync_run_id: runId,
      contract_version: entity.contractVersion,
      scope_from: dateRange.from,
      scope_to: dateRange.to,
      chunk_index: i,
      chunk_count: chunks.length,
      is_first: isFirst,
      is_last: isLast,
      row_count: chunk.length,
      payload_checksum: payloadChecksum,
      meta: isFirst ? { [entity.clearMetaKey]: scopeDates } : {},
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

// entity を直列実行 (warehouse.db 書き込みはシリアル原則)
const outcomes = [];
for (const entity of targetEntities) {
  try {
    outcomes.push(await syncEntity(entity));
  } catch (e) {
    console.error(`FATAL (${entity.name}): ${e.message}`);
    outcomes.push({ entity: entity.name, ok: false, error: e.message });
  }
}

const failed = outcomes.filter(o => !o.ok);
console.log(`\n=== summary: ${outcomes.length - failed.length}/${outcomes.length} entities OK ===`);
process.exitCode = failed.length > 0 ? 1 : 0;
