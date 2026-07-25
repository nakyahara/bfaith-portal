#!/usr/bin/env node
/**
 * sync-amazon-ads-daily.js — Amazon 広告費 mirror sync (amazon-dashboard PR-A)
 *
 * fact_ad_spend (SKU別、spAdvertisedProduct) と fact_ad_spend_campaign
 * (キャンペーン単位、spCampaigns) の 2 entity を Render mirror へ sync する。
 *
 * f-sales-by-listing 同型 (chunk POST + ledger + scope 全日付 clear)。
 * 広告レポートは attribution 遡及で過去日の値が更新されるため、
 * scope 内は 0 行日付も含めて clear → 再投入 (stale 削除)。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ads-daily.js --days 35 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ads-daily.js --month 2026-06
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ads-daily.js --from 2026-01-01 --to 2026-06-30
 *   DATA_DIR=... node apps/warehouse/sync-amazon-ads-daily.js --days 35 --entity amazon_ads_sku_daily
 *
 * env:
 *   DATA_DIR             必須
 *   RENDER_MIRROR_URL    送信先 base URL
 *   MIRROR_SYNC_KEY      認証 (x-sync-key header)
 *   CHUNK_SIZE           default 3000
 *
 * exit code: 0=全 entity 成功 / 1=いずれか失敗 / 2=env・引数エラー
 */
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

// 上限 cap: 巨大値の env 設定で Render 側 payload/timeout を踏む事故防止 (Codex R1 Low)
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

let dateRange;
if (monthStr) {
  if (!/^\d{4}-\d{2}$/.test(monthStr)) { console.error('FATAL: --month must be YYYY-MM'); process.exit(2); }
  dateRange = { from: `${monthStr}-01`, to: `${monthStr}-31` };
} else if (fromStr && toStr) {
  if (!isRealDate(fromStr) || !isRealDate(toStr)) {
    console.error('FATAL: --from/--to must be real YYYY-MM-DD date'); process.exit(2);
  }
  if (fromStr > toStr) { console.error('FATAL: --from must be <= --to'); process.exit(2); }
  dateRange = { from: fromStr, to: toStr };
} else if (daysStr) {
  const days = parseInt(daysStr, 10);
  if (!Number.isInteger(days) || days <= 0 || days > 400) {
    console.error('FATAL: --days must be 1..400'); process.exit(2);
  }
  // JST 基準の today (fetch-amazon-ads* と同じく Ads API の日付は JST 概念で運用)
  const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
  const toDate = nowJst.toISOString().slice(0, 10);
  const fromDate = new Date(nowJst.getTime() - (days - 1) * 86400000).toISOString().slice(0, 10);
  dateRange = { from: fromDate, to: toDate };
} else {
  console.error('FATAL: --days N or --month YYYY-MM or --from/--to YYYY-MM-DD required'); process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }
if (!isDryRun && (!renderUrl || !syncKey)) {
  console.error('FATAL: RENDER_MIRROR_URL and MIRROR_SYNC_KEY required for non-dry-run'); process.exit(2);
}

// ============================================================
// entity 定義 (mirror 側は英語列、payload も英語キー)
// ============================================================
const ENTITIES = [
  {
    name: 'amazon_ads_sku_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_amazon_ads_sku_dates',
    // fact_ad_spend PK: (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度)
    selectSql: `
      SELECT
        日付 AS date_jst, モール AS mall, キャンペーンID AS campaign_id,
        広告タイプ AS ad_type, ターゲット AS target, ターゲット粒度 AS target_granularity,
        COALESCE(クリック数, 0) AS clicks, COALESCE(インプレッション, 0) AS impressions,
        COALESCE(広告費, 0) AS ad_cost, COALESCE(広告経由売上, 0) AS ad_sales,
        COALESCE(広告経由数量, 0) AS ad_units
      FROM fact_ad_spend
      WHERE モール = 'amazon' AND 日付 >= ? AND 日付 <= ?
      ORDER BY 日付, キャンペーンID, ターゲット
    `,
    hashRow: (r) => ({
      date_jst: r.date_jst, campaign_id: r.campaign_id, target: r.target,
      target_granularity: r.target_granularity, ad_cost: r.ad_cost, ad_sales: r.ad_sales,
    }),
    sampleLog: (r) => `date=${r.date_jst} campaign=${r.campaign_id} target=${r.target} cost=¥${r.ad_cost}`,
  },
  {
    name: 'amazon_ads_campaign_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_amazon_ads_campaign_dates',
    // fact_ad_spend_campaign PK: (日付, モール, キャンペーンID, 広告タイプ)
    selectSql: `
      SELECT
        日付 AS date_jst, モール AS mall, キャンペーンID AS campaign_id,
        COALESCE(キャンペーン名, '') AS campaign_name, 広告タイプ AS ad_type,
        COALESCE(キャンペーンステータス, '') AS campaign_status,
        COALESCE(クリック数, 0) AS clicks, COALESCE(インプレッション, 0) AS impressions,
        COALESCE(広告費, 0) AS ad_cost,
        COALESCE(広告経由売上_1d, 0) AS ad_sales_1d, COALESCE(広告経由売上_7d, 0) AS ad_sales_7d,
        COALESCE(広告経由売上_14d, 0) AS ad_sales_14d, COALESCE(広告経由売上_30d, 0) AS ad_sales_30d,
        COALESCE(広告経由数量_1d, 0) AS ad_units_1d
      FROM fact_ad_spend_campaign
      WHERE モール = 'amazon' AND 日付 >= ? AND 日付 <= ?
      ORDER BY 日付, キャンペーンID
    `,
    hashRow: (r) => ({
      date_jst: r.date_jst, campaign_id: r.campaign_id,
      ad_cost: r.ad_cost, ad_sales_14d: r.ad_sales_14d, clicks: r.clicks,
    }),
    sampleLog: (r) => `date=${r.date_jst} campaign=${r.campaign_id} "${r.campaign_name.slice(0, 20)}" cost=¥${r.ad_cost}`,
  },
];

const targetEntities = entityFilter
  ? ENTITIES.filter(e => e.name === entityFilter)
  : ENTITIES;
if (targetEntities.length === 0) {
  console.error(`FATAL: unknown --entity '${entityFilter}' (valid: ${ENTITIES.map(e => e.name).join(', ')})`);
  process.exit(2);
}

// scope 全日付列挙 (0 行日付の Render 側 stale 削除のため、f_sales_by_listing 同型)
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
    console.log(`  source: ${contract.source_object}`);
    console.log(`  target: ${contract.target_table}`);
    console.log(`  scope: ${dateRange.from} 〜 ${dateRange.to}`);
    console.log(`  dry-run: ${isDryRun}`);

    rows = db.prepare(entity.selectSql).all(dateRange.from, dateRange.to);
  } finally {
    db.close();
  }

  console.log(`  Rows to sync: ${rows.length.toLocaleString()}`);
  const scopeDates = enumerateScopeDates(dateRange.from, dateRange.to);
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

// entity を直列実行 (warehouse.db 書き込みはシリアル原則、feedback_warehouse_db_serial_write)
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
// 通信 (mirror POST / GChat通知) 直後の process.exit() は Windows node で libuv assertion
// (UV_HANDLE_CLOSING / abort) を踏み、成功しているのに失敗扱いになる → exitCode + 自然終了 (#614 と同根)
process.exitCode = failed.length > 0 ? 1 : 0;
