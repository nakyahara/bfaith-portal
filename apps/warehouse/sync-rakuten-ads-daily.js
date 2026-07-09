#!/usr/bin/env node
/**
 * sync-rakuten-ads-daily.js — 楽天RPP広告費 mirror sync (mall-csv-fetcher P1)
 *
 * fact_rakuten_ads_rpp (月次×商品) と fact_rakuten_ads_rpp_daily (日次×キャンペーン合計)
 * の 2 entity を Render mirror へ sync する。sync-amazon-ads-daily.js 同型
 * (chunk POST + ledger + scope clear)。
 *
 * RPPレポートは過去分が変動する (不正クリック控除、720h遡及) ため、
 * scope 内は 0 行日付も含めて clear → 再投入 (stale 削除)。
 *
 * 月次 entity の clear キーは月初日 date_jst=YYYY-MM-01
 * (mirror の date_range clear 機構と整合、amazon_account_fees_monthly と同方針)。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-ads-daily.js --days 70 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-ads-daily.js --month 2026-06
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-ads-daily.js --from 2026-01-01 --to 2026-06-30
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-ads-daily.js --days 70 --entity rakuten_ads_rpp_monthly
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

// 上限 cap: 巨大値の env 設定で Render 側 payload/timeout を踏む事故防止
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
  // JST 基準。RPPの集計は昨日まで (downloader も昨日までを取得) なので scope 終端=昨日。
  // 今日を clear 対象に含めると、当日行が fact に入った場合に mirror 側だけ消える (Codex R1 Medium)
  const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
  const yesterdayJst = new Date(nowJst.getTime() - 86400000);
  const toDate = yesterdayJst.toISOString().slice(0, 10);
  const fromDate = new Date(yesterdayJst.getTime() - (days - 1) * 86400000).toISOString().slice(0, 10);
  dateRange = { from: fromDate, to: toDate };
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

// scope 内の月初日列挙 (月次 entity の clear キー。from の属する月から to の属する月まで)
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
// entity 定義 (mirror 側は英語列、payload も英語キー)
// ============================================================
const ENTITIES = [
  {
    name: 'rakuten_ads_rpp_monthly',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_ads_rpp_months',
    // 月次: date_jst=月初日 (clear/scope キー)、month_ym は substr で導出可だが明示列で持つ
    scopeDates: (range) => enumerateScopeMonthStarts(range.from, range.to),
    selectSql: `
      SELECT
        (month_ym || '-01') AS date_jst, month_ym, item_manage_number, raw_sku_code,
        clicks, ad_cost_yen, cpc_actual, ctr_pct, bid_cpc_yen, item_cpc_yen,
        sales_720h_yen, orders_720h, cvr_720h_pct, roas_720h_pct,
        sales_12h_yen, orders_12h, sales_720h_new_yen, sales_720h_repeat_yen,
        source_report_type, report_start, report_end,
        attribution_window_hours, is_tax_included, imported_at
      FROM fact_rakuten_ads_rpp
      WHERE month_ym >= ? AND month_ym <= ?
      ORDER BY month_ym, item_manage_number
    `,
    selectParams: (range) => [range.from.slice(0, 7), range.to.slice(0, 7)],
    hashRow: (r) => ({
      month_ym: r.month_ym, item: r.item_manage_number,
      ad_cost_yen: r.ad_cost_yen, sales_720h_yen: r.sales_720h_yen, clicks: r.clicks,
    }),
    sampleLog: (r) => `month=${r.month_ym} item=${r.item_manage_number} cost=¥${r.ad_cost_yen}`,
  },
  {
    name: 'rakuten_ads_rpp_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_ads_rpp_daily_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    selectSql: `
      SELECT
        date_jst, campaign_id, campaign_name,
        clicks, ad_cost_yen, ad_cost_discounted_yen, cpc_actual, ctr_pct,
        sales_720h_yen, orders_720h, cvr_720h_pct, roas_720h_pct,
        sales_12h_yen, orders_12h, sales_720h_new_yen, sales_720h_repeat_yen,
        source_report_type, attribution_window_hours, is_tax_included, imported_at
      FROM fact_rakuten_ads_rpp_daily
      WHERE date_jst >= ? AND date_jst <= ?
      ORDER BY date_jst, campaign_id
    `,
    selectParams: (range) => [range.from, range.to],
    hashRow: (r) => ({
      date_jst: r.date_jst, campaign_id: r.campaign_id,
      ad_cost_yen: r.ad_cost_yen, sales_720h_yen: r.sales_720h_yen, clicks: r.clicks,
    }),
    sampleLog: (r) => `date=${r.date_jst} campaign=${r.campaign_id || '(all)'} cost=¥${r.ad_cost_yen}`,
  },
];

const targetEntities = entityFilter
  ? ENTITIES.filter(e => e.name === entityFilter)
  : ENTITIES;
if (targetEntities.length === 0) {
  console.error(`FATAL: unknown --entity '${entityFilter}' (valid: ${ENTITIES.map(e => e.name).join(', ')})`);
  process.exit(2);
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

    // fact 未作成 (取込が一度も走っていない) 場合は 0 行扱いにせず明示スキップ
    const tableName = contract.source_object;
    const exists = db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(tableName);
    if (!exists) {
      console.log(`  ⏭️ source table ${tableName} が未作成 (取込前) のためスキップ`);
      return { entity: entity.name, ok: true, skipped: true, rows: 0 };
    }
    rows = db.prepare(entity.selectSql).all(...entity.selectParams(dateRange));
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
process.exit(failed.length > 0 ? 1 : 0);
