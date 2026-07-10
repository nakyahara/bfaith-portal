#!/usr/bin/env node
/**
 * sync-rakuten-data-daily.js — 楽天データ分析 (店舗日次/SKU日次) mirror sync (mall-csv-fetcher P1-R2)
 *
 * fact_rakuten_item_daily (SKU×日次 アクセス/CVR/レビュー/在庫) と
 * fact_rakuten_store_daily (店舗×日次 KPI+商圏ベンチ+費用内訳) を Render mirror へ sync。
 * sync-rakuten-ads-daily.js 同型 (chunk POST + ledger + scope clear)。
 *
 * 過去分は再DLで変動しうるため scope 内は 0 行日付も含めて clear → 再投入。
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-data-daily.js --days 70 --dry-run
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-data-daily.js --from 2026-07-01 --to 2026-07-09
 *   DATA_DIR=... node apps/warehouse/sync-rakuten-data-daily.js --days 70 --entity rakuten_item_daily
 *
 * env: DATA_DIR (必須) / RENDER_MIRROR_URL / MIRROR_SYNC_KEY / CHUNK_SIZE (default 3000)
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
  // 月末は実在日で計算 (固定 -31 だと 2月等で不正日付が ledger/payload に流れる — Codex R1 medium)
  const [y, m] = monthStr.split('-').map(Number);
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  dateRange = { from: `${monthStr}-01`, to: `${monthStr}-${String(lastDay).padStart(2, '0')}` };
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
    name: 'rakuten_item_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_item_daily_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    // 商品名/ジャンルは m_rakuten_items の最新値を JOIN で付与 (分析タブでの表示用)
    selectSql: `
      SELECT
        f.date_jst, f.item_manage_number, f.raw_sku_code,
        f.sales_yen, f.orders, f.units, f.access_users, f.unique_users, f.cvr_pct, f.aov_yen,
        f.buyers_total, f.buyers_new, f.buyers_repeat, f.nonbuyer_access,
        f.review_posts, f.review_avg, f.review_total,
        f.stay_seconds, f.bounce_count, f.exit_count, f.exit_rate_pct,
        f.favorites_added, f.favorites_total, f.stock_qty,
        f.is_tax_included, f.imported_at,
        m.item_name, m.genre_path, m.item_id, m.catalog_id, m.item_number
      FROM fact_rakuten_item_daily f
      LEFT JOIN m_rakuten_items m ON m.item_manage_number = f.item_manage_number
      WHERE f.date_jst >= ? AND f.date_jst <= ?
      ORDER BY f.date_jst, f.item_manage_number
    `,
    selectParams: (range) => [range.from, range.to],
    hashRow: (r) => ({
      date_jst: r.date_jst, item: r.item_manage_number,
      sales_yen: r.sales_yen, access: r.access_users, stock: r.stock_qty,
    }),
    sampleLog: (r) => `date=${r.date_jst} item=${r.item_manage_number} sales=¥${r.sales_yen} access=${r.access_users}`,
  },
  {
    name: 'rakuten_store_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_store_daily_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    selectSql: `
      SELECT
        date_jst,
        sales_all_yen, sales_pc_yen, sales_app_yen, sales_sp_yen,
        orders_all, orders_pc, orders_app, orders_sp,
        access_all, access_pc, access_app, access_sp,
        cvr_all_pct, cvr_pc_pct, cvr_app_pct, cvr_sp_pct,
        aov_all_yen, aov_pc_yen, aov_app_yen, aov_sp_yen,
        bench_top10_sales_yen, bench_top10_orders, bench_top10_access, bench_top10_cvr_pct, bench_top10_aov_yen,
        bench_class_label, bench_class_sales_yen, bench_class_orders, bench_class_access, bench_class_cvr_pct, bench_class_aov_yen,
        tax_out_yen, shipping_yen, coupon_store_yen, coupon_rakuten_yen,
        free_ship_coupon_yen, wrapping_yen, settlement_fee_yen,
        is_tax_included, imported_at
      FROM fact_rakuten_store_daily
      WHERE date_jst >= ? AND date_jst <= ?
      ORDER BY date_jst
    `,
    selectParams: (range) => [range.from, range.to],
    hashRow: (r) => ({
      date_jst: r.date_jst, sales: r.sales_all_yen, access: r.access_all, coupon: r.coupon_store_yen,
    }),
    sampleLog: (r) => `date=${r.date_jst} sales=¥${r.sales_all_yen} access=${r.access_all} cvr=${r.cvr_all_pct}%`,
  },
  // ─── データダウンロードハブ 7種 (mall-csv-fetcher P1-R3、2026-07-10) ───
  {
    name: 'rakuten_store_device_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_store_device_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    selectSql: `SELECT * FROM fact_rakuten_store_device_daily WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst, device`,
    selectParams: (range) => [range.from, range.to],
    dropCols: ['source_file', 'import_id', 'updated_at'],
    hashRow: (r) => ({ date_jst: r.date_jst, device: r.device, sales: r.sales_yen, access: r.access_users, uu: r.unique_users }),
    sampleLog: (r) => `date=${r.date_jst} dev=${r.device} sales=¥${r.sales_yen} uu=${r.unique_users}`,
  },
  {
    name: 'rakuten_sku_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_sku_daily_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    // SKU名/連携キーは m_rakuten_skus の最新値を JOIN で付与
    selectSql: `
      SELECT
        f.date_jst, f.sku_key, f.raw_sku_mgmt_number, f.item_manage_number,
        f.sales_yen, f.orders, f.units, f.is_tax_included, f.imported_at,
        m.system_sku_number, m.item_number, m.catalog_id, m.item_name,
        m.sku_attr1, m.sku_attr2, m.sku_attr3
      FROM fact_rakuten_sku_daily f
      LEFT JOIN m_rakuten_skus m ON m.sku_key = f.sku_key
      WHERE f.date_jst >= ? AND f.date_jst <= ?
      ORDER BY f.date_jst, f.sku_key
    `,
    selectParams: (range) => [range.from, range.to],
    hashRow: (r) => ({ date_jst: r.date_jst, sku: r.sku_key, sales: r.sales_yen, units: r.units }),
    sampleLog: (r) => `date=${r.date_jst} sku=${r.sku_key} sales=¥${r.sales_yen}`,
  },
  {
    name: 'rakuten_category_daily',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_category_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    selectSql: `SELECT * FROM fact_rakuten_category_daily WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst, category_key, device`,
    selectParams: (range) => [range.from, range.to],
    dropCols: ['source_file', 'import_id', 'updated_at'],
    hashRow: (r) => ({ date_jst: r.date_jst, key: r.category_key, device: r.device, access: r.access_users }),
    sampleLog: (r) => `date=${r.date_jst} cat=${(r.category_name || '').slice(0, 20)} access=${r.access_users}`,
  },
  {
    name: 'rakuten_campaigns',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_campaign_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    // date_jst = キャンペーン開始日。scope より前に始まった開催中キャンペーンは clear されない
    // (一覧DLは常に「現在の開催一覧」なので、初回のみ --from を過去に広げて投入する)
    selectSql: `SELECT campaign_type, campaign_name, start_at, end_at, date_jst, imported_at FROM m_rakuten_campaigns WHERE date_jst >= ? AND date_jst <= ? ORDER BY start_at, campaign_name`,
    selectParams: (range) => [range.from, range.to],
    hashRow: (r) => ({ type: r.campaign_type, name: r.campaign_name, start: r.start_at }),
    sampleLog: (r) => `${r.campaign_type}: ${r.campaign_name.slice(0, 30)} (${r.start_at})`,
  },
  {
    name: 'rakuten_purchaser_monthly',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_purchaser_months',
    // 月次 grain: clear キーは scope 内の月初日 (RPP monthly と同方針)。
    // 過去2年が毎回全量入る → 初回のみ --from 2024-08-01 で全量 sync、以後 --days 70 で直近月を更新
    scopeDates: (range) => enumerateScopeMonthStarts(range.from, range.to),
    selectSql: `SELECT * FROM fact_rakuten_purchaser_monthly WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst`,
    selectParams: (range) => [`${range.from.slice(0, 7)}-01`, range.to],
    dropCols: ['source_file', 'import_id', 'updated_at'],
    hashRow: (r) => ({ date_jst: r.date_jst, nb: r.new_buyers, rb: r.repeat_buyers, ns: r.new_sales_yen }),
    sampleLog: (r) => `month=${r.date_jst.slice(0, 7)} new=${r.new_buyers} repeat=${r.repeat_buyers}`,
  },
  {
    name: 'rakuten_item_purchaser_snapshot',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_item_purchaser_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    selectSql: `SELECT * FROM fact_rakuten_item_purchaser_snapshot WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst, item_manage_number`,
    selectParams: (range) => [range.from, range.to],
    dropCols: ['source_file', 'import_id', 'updated_at'],
    hashRow: (r) => ({ date_jst: r.date_jst, item: r.item_manage_number, nb: r.new_buyers, rb: r.repeat_buyers }),
    sampleLog: (r) => `snap=${r.date_jst} item=${r.item_manage_number} repeat=${r.repeat_buyers}`,
  },
  {
    name: 'rakuten_genre_purchaser_snapshot',
    contractVersion: 1,
    clearMetaKey: 'clear_rakuten_genre_purchaser_dates',
    scopeDates: (range) => enumerateScopeDates(range.from, range.to),
    selectSql: `SELECT * FROM fact_rakuten_genre_purchaser_snapshot WHERE date_jst >= ? AND date_jst <= ? ORDER BY date_jst, genre_name`,
    selectParams: (range) => [range.from, range.to],
    dropCols: ['source_file', 'import_id', 'updated_at'],
    hashRow: (r) => ({ date_jst: r.date_jst, genre: r.genre_name, nb: r.new_buyers, rb: r.repeat_buyers }),
    sampleLog: (r) => `snap=${r.date_jst} genre=${(r.genre_name || '').slice(0, 25)}`,
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

    // fact 未作成は失敗扱い (daily-sync では import が先に走り必ず作成するため、欠落=異常。
    // ok 扱いにすると mirror 側の stale データが scope clear されず残る — Codex R1 medium)
    const tableName = contract.source_object;
    const exists = db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(tableName);
    if (!exists) {
      console.error(`  ✗ source table ${tableName} が未作成。先に import-rakuten-data.js を実行してください`);
      return { entity: entity.name, ok: false, error: 'source_table_missing' };
    }
    rows = db.prepare(entity.selectSql).all(...entity.selectParams(dateRange));
    // SELECT * の entity はローカル管理列を payload から落とす (mirror 側スキーマに無い列を送らない)
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
// fetch 直後の process.exit() は Windows libuv assertion crash で exit code が化ける (#464 と同根)
process.exitCode = failed.length > 0 ? 1 : 0;
