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
 *   DATA_DIR=... node apps/warehouse/sync-sku-maps.js --allow-shrink   # map を実際に整理した時だけ
 * env: DATA_DIR (必須) / RENDER_MIRROR_URL / MIRROR_SYNC_KEY
 *
 * 送る前に落とすもの (全置換なので「一部だけ送る」が一番危ない):
 *   - ne_code が m_products に無い / store_id・resolution_source が想定外 / 前後空白 / PK重複
 *   - 前回適用より 20% 超減っている (--allow-shrink で解除)
 *   - 0件・単一 chunk に収まらない行数
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
// 大幅減少ゲートの明示解除。map を実際に整理した時だけ人が付ける
const allowShrink = args.includes('--allow-shrink');
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

// 許可ストア / resolution_source の enum。受け側でも同じものを検証する (二重の網)
const ALLOWED_STORES = new Set(['b-faith01']);
const RESOLUTION_SOURCES = new Set(['manual', 'auto_pattern', 'fallback_parent']);
// 前回適用件数からこの割合を超えて減る snapshot は送らない (欠損 SELECT の事故を素通しさせない)
const MAX_SHRINK_RATIO = 0.2;

const ENTITIES = [
  {
    name: 'yahoo_sku_map',
    contractVersion: 1,
    keyCol: 'yahoo_key',
    selectSql: 'SELECT store_id, yahoo_key, ne_code, resolution_source, notes, created_at, updated_at'
      + ' FROM f_yahoo_sku_map ORDER BY store_id, yahoo_key',
    sampleLog: (r) => `yahoo_key=${r.yahoo_key} ne=${r.ne_code} src=${r.resolution_source}`,
  },
  {
    name: 'aupay_sku_map',
    contractVersion: 1,
    keyCol: 'aupay_key',
    selectSql: 'SELECT store_id, aupay_key, ne_code, resolution_source, notes, created_at, updated_at'
      + ' FROM f_aupay_sku_map ORDER BY store_id, aupay_key',
    sampleLog: (r) => `aupay_key=${r.aupay_key} ne=${r.ne_code} src=${r.resolution_source}`,
  },
];

/**
 * 送信前の意味検証。map が壊れたまま mirror に入ると、価格改定で別商品に値付けする事故になる。
 * ここで落とす = 一部だけ送って「全置換」を成立させない (fail-closed、Codex R1 High #3)。
 * ne_code の実在確認は NE商品マスタ (m_products) を持つ miniPC 側にしかできない。
 */
function validateRows(db, entity, rows) {
  const problems = [];
  const known = new Set(
    db.prepare('SELECT LOWER(TRIM(商品コード)) AS c FROM m_products').all().map((r) => r.c)
  );
  const seen = new Set();
  for (const r of rows) {
    const where = `${entity.keyCol}=${JSON.stringify(r[entity.keyCol])}`;
    for (const col of [entity.keyCol, 'ne_code', 'store_id']) {
      const v = r[col];
      if (typeof v !== 'string' || v === '' || v !== v.trim()) {
        problems.push(`${where}: ${col} が空 or 前後空白あり (${JSON.stringify(v)})`);
      }
    }
    if (!ALLOWED_STORES.has(r.store_id)) problems.push(`${where}: 未知の store_id=${r.store_id}`);
    if (!RESOLUTION_SOURCES.has(r.resolution_source)) problems.push(`${where}: 未知の resolution_source=${r.resolution_source}`);
    if (typeof r.ne_code === 'string' && !known.has(r.ne_code.trim().toLowerCase())) {
      problems.push(`${where}: ne_code=${r.ne_code} が m_products に無い (廃番 or タイポ)`);
    }
    const pk = JSON.stringify([r.store_id, r[entity.keyCol]]);
    if (seen.has(pk)) problems.push(`${where}: PK 重複`);
    seen.add(pk);
  }
  return problems;
}

/** 送信のたびに単調増加する世代番号を採る (受け側が古い run を 409 で弾くための番号) */
function nextGeneration(dbPathArg, entityName) {
  const wdb = new Database(dbPathArg);
  try {
    const tx = wdb.transaction(() => {
      wdb.prepare(`INSERT INTO sync_snapshot_generations (entity, generation, updated_at) VALUES (?, 1, ?)
        ON CONFLICT(entity) DO UPDATE SET generation = generation + 1, updated_at = excluded.updated_at`)
        .run(entityName, new Date().toISOString());
      return wdb.prepare('SELECT generation FROM sync_snapshot_generations WHERE entity = ?').get(entityName).generation;
    });
    return tx();
  } finally {
    wdb.close();
  }
}

const targetEntities = entityFilter ? ENTITIES.filter(e => e.name === entityFilter) : ENTITIES;
if (targetEntities.length === 0) {
  console.error(`FATAL: unknown --entity '${entityFilter}' (valid: ${ENTITIES.map(e => e.name).join(', ')})`);
  process.exit(2);
}

async function syncEntity(entity) {
  const db = new Database(dbPath, { readonly: true });
  let rows;
  let problems;
  let lastApplied;
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
    problems = validateRows(db, entity, rows);
    // 前回 applied の件数 (大幅減少ゲートの基準)
    lastApplied = db.prepare(`
      SELECT row_count_received FROM sync_runs
       WHERE entity = ? AND status = 'applied' ORDER BY applied_at DESC LIMIT 1
    `).get(entity.name);
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
  if (problems.length > 0) {
    console.error(`  ✗ 内容の検証で ${problems.length} 件の問題。全置換なので一部だけ送らずに中断する:`);
    for (const p of problems.slice(0, 20)) console.error(`      - ${p}`);
    if (problems.length > 20) console.error(`      ... 他 ${problems.length - 20} 件`);
    return { entity: entity.name, ok: false, error: 'invalid_rows' };
  }
  // 大幅減少ゲート: 「本来500件が3件」の欠損 snapshot を全置換として通さない
  const prevRows = lastApplied ? lastApplied.row_count_received : null;
  if (prevRows && rows.length < prevRows * (1 - MAX_SHRINK_RATIO) && !allowShrink) {
    console.error(`  ✗ 前回 ${prevRows} 件 → 今回 ${rows.length} 件 (許容 ${Math.round(MAX_SHRINK_RATIO * 100)}% 減)。`
      + ' 欠損の可能性があるため送信しない。意図した削除なら --allow-shrink を付けて実行してください');
    return { entity: entity.name, ok: false, error: 'shrink_rejected' };
  }

  const tsCompact = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const runIdSalt = crypto.randomBytes(3).toString('hex');
  const runId = `${entity.name}-v${entity.contractVersion}-${tsCompact}-${runIdSalt}`;
  console.log(`  run_id: ${runId}`);

  // 監査メタ (source_row_hash / synced_at) は受け側が付け直す。ここでは run の同一性だけ載せる
  // (受け側は row.source_run_id != sync_run_id を 400 で弾く)
  const enrichedRows = rows.map(r => ({ ...r, source_run_id: runId }));

  if (isDryRun) {
    console.log('  --- DRY RUN ---');
    console.log(`  Would send 1 chunk (${enrichedRows.length} rows, 全置換)`);
    console.log(`  Sample row[0]: ${entity.sampleLog(enrichedRows[0])}`);
    return { entity: entity.name, ok: true, dryRun: true, rows: enrichedRows.length };
  }

  // 世代採番は「実際に送る」と決めた後に行う (dry-run や検証落ちで番号を飛ばさない)
  const generation = nextGeneration(dbPath, entity.name);
  console.log(`  snapshot_generation: ${generation}`);

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
    meta: { snapshot_generation: generation, ...(allowShrink ? { allow_shrink: true } : {}) },
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
      // 2xx というだけで applied にしない。プロキシ差し替えや将来の API 変更で
      // 「送信元は applied、mirror は未反映」になるのを防ぐ (Codex R1 Low #8)
      const mismatches = [];
      if (result.ok !== true) mismatches.push(`ok=${JSON.stringify(result.ok)}`);
      if (result.status !== 'completed') mismatches.push(`status=${JSON.stringify(result.status)}`);
      if (result.sync_run_id !== runId) mismatches.push(`sync_run_id=${JSON.stringify(result.sync_run_id)}`);
      if (result.entity !== entity.name) mismatches.push(`entity=${JSON.stringify(result.entity)}`);
      if (result.rows_received !== enrichedRows.length) mismatches.push(`rows_received=${JSON.stringify(result.rows_received)} (expected ${enrichedRows.length})`);
      if (mismatches.length > 0) {
        lastError = `unexpected response: ${mismatches.join(', ')}`;
        console.error(`  ✗ 応答が契約と違う: ${lastError}`);
      } else {
        console.log(`  ✓ chunk applied (request_id=${result.request_id}, rows_received=${result.rows_received})`);
      }
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
