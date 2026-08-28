#!/usr/bin/env node
/**
 * sync-sku-maps.js — SKUマップ 2種の mirror sync (価格一括改定ツール PR1)
 *
 * f_yahoo_sku_map / f_aupay_sku_map (= モール出品コード → NEコード の手動 map) を
 * Render mirror へ全置換で送る。価格一括改定ツール (apps/price-update) の出品引き当てが参照する。
 *
 * ★全置換 (clear_strategy='full_snapshot') な理由:
 *   no_clear の upsert だと「miniPC で誤りとして削除した map」が mirror に残り続け、
 *   別商品の NEコードに値付けする事故になる。map は数百行なので毎回そのまま入れ替える。
 *   受け側は単一 chunk しか受けない (分割は 400) ので、chunk 分割される規模になったら気づける。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-sku-maps.js --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-sku-maps.js --entity yahoo_sku_map
 * env: DATA_DIR (必須) / RENDER_MIRROR_URL / MIRROR_SYNC_KEY
 */
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

// 受け側が単一 chunk しか受けないため、分割が必要な規模になったら送らずに止める。
// (黙って一部だけ送ると mirror が「全置換」の名のもとに欠けた状態で確定する)
const MAX_ROWS = 5000;

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const entityFilter = getArg('--entity');
const isDryRun = args.includes('--dry-run');
const renderUrl = process.env.RENDER_MIRROR_URL;
const syncKey = process.env.MIRROR_SYNC_KEY;

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2);
}

// scope は日付必須 (sync_runs / 受け側とも YYYY-MM-DD 検証あり) だが、マスタ全置換なので
// 期間の意味は無い。実行日 (JST) を from=to で入れる
const todayJst = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
const dateRange = { from: todayJst, to: todayJst };

const ENTITIES = [
  {
    name: 'yahoo_sku_map',
    contractVersion: 1,
    selectSql: 'SELECT yahoo_key, store_id, ne_code, resolution_source, notes, created_at, updated_at'
      + ' FROM f_yahoo_sku_map ORDER BY yahoo_key',
    hashRow: (r) => ({ k: r.yahoo_key, s: r.store_id, n: r.ne_code, r: r.resolution_source }),
    sampleLog: (r) => `yahoo_key=${r.yahoo_key} ne=${r.ne_code} src=${r.resolution_source}`,
  },
  {
    name: 'aupay_sku_map',
    contractVersion: 1,
    selectSql: 'SELECT store_id, aupay_key, ne_code, resolution_source, notes, created_at, updated_at'
      + ' FROM f_aupay_sku_map ORDER BY store_id, aupay_key',
    hashRow: (r) => ({ s: r.store_id, k: r.aupay_key, n: r.ne_code, r: r.resolution_source }),
    sampleLog: (r) => `aupay_key=${r.aupay_key} ne=${r.ne_code} src=${r.resolution_source}`,
  },
];

const targetEntities = entityFilter ? ENTITIES.filter(e => e.name === entityFilter) : ENTITIES;
if (targetEntities.length === 0) {
  console.error(`FATAL: unknown --entity '${entityFilter}' (valid: ${ENTITIES.map(e => e.name).join(', ')})`);
  process.exit(2);
}

async function syncEntity(entity) {
  const db = new Database(dbPath, { readonly: true });
  let rows;
  try {
    const contract = db.prepare('SELECT * FROM sync_contracts WHERE entity = ?').get(entity.name);
    if (!contract) {
      console.error(`FATAL: contract for entity '${entity.name}' not found in sync_contracts`);
      console.error('  initDB() の auto-seed が走ってないか、apps/warehouse/db.js を最新版に更新してください');
      return { entity: entity.name, ok: false, error: 'contract_not_found' };
    }
    if (contract.contract_version !== entity.contractVersion) {
      console.error(`FATAL: contract version mismatch: code=${entity.contractVersion}, db=${contract.contract_version}`);
      return { entity: entity.name, ok: false, error: 'contract_version_mismatch' };
    }
    console.log(`\n=== sync ${entity.name} v${entity.contractVersion} (full_snapshot) ===`);
    console.log(`  source: ${contract.source_object}`);
    console.log(`  target: ${contract.target_table}`);
    console.log(`  dry-run: ${isDryRun}`);

    const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(contract.source_object);
    if (!exists) {
      console.error(`  ✗ source table ${contract.source_object} が未作成 (sql/*/${contract.source_object}.sql を適用してください)`);
      return { entity: entity.name, ok: false, error: 'source_table_missing' };
    }
    rows = db.prepare(entity.selectSql).all();
  } finally {
    db.close();
  }

  console.log(`  Rows to sync: ${rows.length.toLocaleString()}`);
  // 0件は送らない。受け側も empty_snapshot_rejected で弾くが、こちら側でも失敗として見せる
  // (手動 map が全部消えている = 引き当てが静かに壊れている状態なので ok で流さない)
  if (rows.length === 0) {
    console.error('  ✗ 0件のため送信しない (map が空 = 引き当てが壊れている可能性)。意図的に空にした場合は手動で対応してください');
    return { entity: entity.name, ok: false, error: 'empty_source' };
  }
  if (rows.length > MAX_ROWS) {
    console.error(`  ✗ ${rows.length} 行 > MAX_ROWS=${MAX_ROWS}。受け側が単一 chunk しか受けないため送信しない (分割対応が必要)`);
    return { entity: entity.name, ok: false, error: 'too_many_rows' };
  }

  const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const runIdSalt = crypto.randomBytes(3).toString('hex');
  const runId = `${entity.name}-v${entity.contractVersion}-${tsCompact}-${runIdSalt}`;
  console.log(`  run_id: ${runId}`);

  const syncedAt = new Date().toISOString();
  const enrichedRows = rows.map(r => {
    const hash = crypto.createHash('sha256').update(JSON.stringify(entity.hashRow(r))).digest('hex').slice(0, 16);
    return { ...r, source_run_id: runId, source_row_hash: hash, synced_at: syncedAt };
  });

  if (isDryRun) {
    console.log('  --- DRY RUN ---');
    console.log(`  Would send 1 chunk (${enrichedRows.length} rows, 全置換)`);
    console.log(`  Sample row[0]: ${entity.sampleLog(enrichedRows[0])}`);
    return { entity: entity.name, ok: true, dryRun: true, rows: enrichedRows.length };
  }

  const sourceHost = os.hostname();
  const startedAt = new Date().toISOString();
  {
    const writeDb = new Database(dbPath);
    writeDb.prepare(`
      INSERT INTO sync_runs (
        run_id, entity, contract_version, source_host,
        scope_from, scope_to, chunk_count_expected,
        status, started_at
      ) VALUES (?, ?, ?, ?, ?, ?, 1, 'started', ?)
    `).run(runId, entity.name, entity.contractVersion, sourceHost,
           dateRange.from, dateRange.to, startedAt);
    writeDb.close();
  }

  const payload = { rows: enrichedRows };
  const payloadChecksum = crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  const body = {
    sync_run_id: runId,
    contract_version: entity.contractVersion,
    scope_from: dateRange.from,
    scope_to: dateRange.to,
    chunk_index: 0,
    chunk_count: 1,
    is_first: true,
    is_last: true,
    row_count: enrichedRows.length,
    payload_checksum: payloadChecksum,
    meta: {},
    payload,
  };

  let lastError = null;
  console.log(`  sending ${enrichedRows.length} rows (single chunk)...`);
  try {
    const headers = { 'Content-Type': 'application/json' };
    if (syncKey) headers['x-sync-key'] = syncKey;
    const res = await fetch(`${renderUrl}/api/sync/${entity.name}/chunk`, {
      method: 'POST', headers, body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text();
      lastError = `HTTP ${res.status}: ${text.slice(0, 300)}`;
      console.error(`  ✗ chunk failed: ${lastError}`);
    } else {
      const result = await res.json();
      console.log(`  ✓ chunk applied (request_id=${result.request_id}, status=${result.status}, rows_received=${result.rows_received})`);
    }
  } catch (e) {
    lastError = e.message;
    console.error(`  ✗ chunk error: ${e.message}`);
  }

  const finishDb = new Database(dbPath);
  try {
    if (lastError) {
      finishDb.prepare(`
        UPDATE sync_runs SET status = 'failed', error_message = ?, completed_at = ?,
          chunk_count_received = 0, row_count_received = 0
         WHERE run_id = ?
      `).run(lastError, new Date().toISOString(), runId);
      console.log(`✗ ${entity.name} sync FAILED: ${lastError}`);
      return { entity: entity.name, ok: false, error: lastError };
    }
    const nowIso = new Date().toISOString();
    finishDb.prepare(`
      UPDATE sync_runs SET status = 'applied', chunk_count_received = 1,
        row_count_received = ?, completed_at = ?, applied_at = ?
       WHERE run_id = ?
    `).run(enrichedRows.length, nowIso, nowIso, runId);
    console.log(`✓ ${entity.name} sync complete (run_id=${runId}, ${enrichedRows.length} rows)`);
    return { entity: entity.name, ok: true, rows: enrichedRows.length };
  } finally {
    finishDb.close();
  }
}

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
