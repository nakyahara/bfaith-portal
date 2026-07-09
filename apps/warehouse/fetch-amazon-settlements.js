/**
 * fetch-amazon-settlements.js — Phase 3.1.1 ingest スクリプト
 *
 * SP-API Reports API で Settlement Report (旧 GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE) を取得し、
 * raw_amazon_settlement_headers / raw_amazon_settlement_lines に投入する。
 *
 * 機能:
 *   - SP-API getReports で過去 Settlement 一覧取得 (最大90日)
 *   - getReportDocument → TSV DL
 *   - TSV パース、micro INTEGER 化、UTC/JST 変換
 *   - physical_line_hash + business_line_key 計算
 *   - dim INSERT OR IGNORE (新 type 自動追加)
 *   - raw INSERT (冪等、physical_line_hash UNIQUE で重複防止)
 *   - settlement_refresh_queue に dirty month 追加
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-settlements.js              # 直近90日全件
 *   node apps/warehouse/fetch-amazon-settlements.js --report-id 1487945020577   # 特定 reportId のみ
 *   node apps/warehouse/fetch-amazon-settlements.js --dry-run    # DL だけして DB に書かない
 */

import 'dotenv/config';
import SellingPartner from 'amazon-sp-api';
import zlib from 'zlib';
import crypto from 'crypto';
import canonicalize from 'canonicalize';
import { initDB, getDB } from './db.js';

const REGION = 'fe';
const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
const REPORT_TYPE = 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE';
const PARSER_VERSION = 'v1.0.0';
const SOURCE_LAYER = 'sp_api_v1';

let spClient = null;
function getClient() {
  if (!spClient) {
    spClient = new SellingPartner({
      region: REGION,
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return spClient;
}

const nowIso = () => new Date().toISOString();
const nowSql = () => new Date().toISOString().replace('T', ' ').slice(0, 19);

function parseArgs() {
  const args = process.argv.slice(2);
  const r = { reportId: null, dryRun: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--report-id' && args[i + 1]) r.reportId = args[++i];
    else if (args[i] === '--dry-run') r.dryRun = true;
  }
  return r;
}

// ─── SP-API ───

async function listSettlementReports() {
  const sp = getClient();
  const all = [];
  let nextToken = null;
  let pageNum = 0;
  do {
    pageNum++;
    const params = nextToken ? { nextToken } : {
      reportTypes: [REPORT_TYPE],
      marketplaceIds: [MARKETPLACE_ID],
      pageSize: 100,
    };
    const resp = await sp.callAPI({ operation: 'getReports', endpoint: 'reports', query: params });
    if (resp.reports) all.push(...resp.reports);
    nextToken = resp.nextToken || null;
    console.log(`[list] page ${pageNum}: cumulative ${all.length}, nextToken=${nextToken ? 'yes' : 'no'}`);
    if (pageNum > 20) { console.log('(safety break at page 20)'); break; }
  } while (nextToken);
  return all;
}

async function downloadReportTsv(reportDocumentId) {
  const sp = getClient();
  const document = await sp.callAPI({
    operation: 'getReportDocument',
    endpoint: 'reports',
    path: { reportDocumentId },
  });
  const res = await fetch(document.url);
  if (document.compressionAlgorithm === 'GZIP') {
    const buf = Buffer.from(await res.arrayBuffer());
    return zlib.gunzipSync(buf).toString('utf-8');
  }
  return await res.text();
}

// ─── Parser / Normalizer ───

function parseAmount(s) {
  // Amazon の amount フィールドは "1234.56" or "" or "0.00"
  if (s == null || s === '') return null;
  const n = parseFloat(s);
  if (isNaN(n)) return null;
  // micro INTEGER (¥1 = 1,000,000)
  return Math.round(n * 1000000);
}

function parseQty(s) {
  if (s == null || s === '') return null;
  const n = parseInt(s, 10);
  return isNaN(n) ? null : n;
}

function normalizeStr(s) {
  if (s == null) return null;
  const t = String(s).trim();
  return t === '' ? null : t;
}

function normalizeSku(s) {
  if (s == null) return null;
  const t = String(s).trim().toLowerCase();
  return t === '' ? null : t;
}

function utcIsoToJstDate(utcIso) {
  if (!utcIso) return null;
  // '2026-04-20T01:11:37+00:00' → JST: 2026-04-20 10:11:37
  const d = new Date(utcIso);
  if (isNaN(d.getTime())) return null;
  // JST = UTC + 9h
  const jst = new Date(d.getTime() + 9 * 3600 * 1000);
  return jst.toISOString().replace('T', ' ').slice(0, 19);
}

function utcIsoToJstDateOnly(utcIso) {
  if (!utcIso) return null;
  const d = new Date(utcIso);
  if (isNaN(d.getTime())) return null;
  const jst = new Date(d.getTime() + 9 * 3600 * 1000);
  return jst.toISOString().slice(0, 10);
}

function dateToYearMonthInt(dateStr) {
  if (!dateStr) return null;
  const m = dateStr.match(/^(\d{4})-(\d{2})/);
  return m ? parseInt(m[1] + m[2], 10) : null;
}

function sha256(s) {
  return crypto.createHash('sha256').update(s, 'utf8').digest('hex');
}

// business_line_key: 業務的に同一な行を識別 (source 関係列を除外)
const BUSINESS_KEY_FIELDS = [
  'source_settlement_id', 'posted_date_utc', 'amazon_order_id', 'merchant_order_id',
  'shipment_id', 'order_item_code', 'adjustment_id', 'seller_sku_normalized',
  'transaction_type', 'marketplace_name', 'fulfillment_id', 'currency',
  'quantity_purchased', 'price_type', 'price_amount_micro',
  'item_related_fee_type', 'item_related_fee_amount_micro',
  'promotion_id', 'promotion_type', 'promotion_amount_micro',
  'shipment_fee_type', 'shipment_fee_amount_micro',
  'order_fee_type', 'order_fee_amount_micro',
  'misc_fee_amount_micro', 'other_fee_amount_micro', 'other_fee_reason_description',
  'direct_payment_type', 'direct_payment_amount_micro', 'other_amount_micro',
];

function makeBusinessKey(row) {
  const obj = {};
  for (const k of BUSINESS_KEY_FIELDS) obj[k] = row[k] ?? null;
  return sha256(canonicalize(obj));
}

// physical_line_hash から除外する volatile な ingest metadata。
// row には実行毎に変わる ingest_run_id(=`settlement-${Date.now()}`) / observed_at(=nowIso) /
// ingested_at(=nowSql) が含まれる。これらをハッシュに含めると同一物理行でも毎回
// ハッシュが変わり、INSERT OR IGNORE (physical_line_hash UNIQUE) の重複排除が無効化される。
// 2026-06-03 発覚: 同一 Settlement レポートを日次で再 fetch するたびに全行を再 INSERT し、
// raw_amazon_settlement_lines が 31.8M 行 (実数の 10-30 倍) に膨張、mart 再構築が timeout。
const PHYSICAL_HASH_EXCLUDE = new Set(['ingest_run_id', 'observed_at', 'ingested_at']);

function makePhysicalHash(row, sourceDocumentId, sourceLineNo) {
  // 物理行の安定 identity (= 同一レポートの同一行は再 fetch しても同一ハッシュ)。
  // volatile な ingest metadata を除いた全列 + source_document_id + source_line_no。
  const obj = { _source_document_id: sourceDocumentId, _source_line_no: sourceLineNo };
  for (const k of Object.keys(row)) {
    if (!PHYSICAL_HASH_EXCLUDE.has(k)) obj[k] = row[k];
  }
  return sha256(canonicalize(obj));
}

// ─── TSV Parser ───

function parseTsv(text) {
  const lines = text.split(/\r?\n/);
  const header = lines[0].split('\t');
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const cols = lines[i].split('\t');
    const obj = {};
    header.forEach((h, j) => obj[h] = cols[j]);
    obj._line_no = i;
    rows.push(obj);
  }
  return { header, rows };
}

// ─── Row classifier: header 行 vs line 行 ───

function isHeaderRow(rawRow) {
  // header 行: total-amount があって posted-date / order-id / sku 等が空
  const totalAmount = rawRow['total-amount'];
  const postedDate = rawRow['posted-date'];
  const orderId = rawRow['order-id'];
  const sku = rawRow['sku'];
  const txType = rawRow['transaction-type'];
  // total-amount があり、posted-date が空 → header
  return Boolean(totalAmount && totalAmount !== '' && (!postedDate || postedDate === '') && (!orderId || orderId === '') && (!sku || sku === '') && (!txType || txType === ''));
}

// ─── Normalize raw TSV row to DB row ───

function normalizeHeaderRow(rawRow, ctx) {
  const row = {
    source_document_id: ctx.sourceDocumentId,
    source_file_hash: ctx.sourceFileHash,
    source_path: ctx.sourcePath,
    source_line_no: rawRow._line_no,
    source_layer: SOURCE_LAYER,
    parser_version: PARSER_VERSION,
    source_settlement_id: normalizeStr(rawRow['settlement-id']),
    settlement_start_date: normalizeStr(rawRow['settlement-start-date']),
    settlement_end_date: normalizeStr(rawRow['settlement-end-date']),
    deposit_date: normalizeStr(rawRow['deposit-date']),
    total_amount_micro: parseAmount(rawRow['total-amount']),
    currency: normalizeStr(rawRow['currency']) || 'JPY',
    ingest_run_id: ctx.runId,
    observed_at: ctx.observedAt,
    ingested_at: nowSql(),
  };
  // business_line_key (header 用、簡易: settlement_id + start + end)
  row.business_line_key = sha256(canonicalize({
    type: 'header', settlement_id: row.source_settlement_id,
    start: row.settlement_start_date, end: row.settlement_end_date,
    total: row.total_amount_micro, currency: row.currency,
  }));
  row.physical_line_hash = makePhysicalHash(row, row.source_document_id, row.source_line_no);
  return row;
}

function normalizeLineRow(rawRow, ctx) {
  const postedDateUtc = normalizeStr(rawRow['posted-date']);
  const row = {
    source_document_id: ctx.sourceDocumentId,
    source_file_hash: ctx.sourceFileHash,
    source_path: ctx.sourcePath,
    source_line_no: rawRow._line_no,
    source_layer: SOURCE_LAYER,
    parser_version: PARSER_VERSION,
    source_settlement_id: normalizeStr(rawRow['settlement-id']),
    posted_date_utc: postedDateUtc,
    posted_datetime_jst: utcIsoToJstDate(postedDateUtc),
    economic_date: utcIsoToJstDateOnly(postedDateUtc),
    year_month_int: null,  // 後で
    amazon_order_id: normalizeStr(rawRow['order-id']),
    merchant_order_id: normalizeStr(rawRow['merchant-order-id']),
    shipment_id: normalizeStr(rawRow['shipment-id']),
    order_item_code: normalizeStr(rawRow['order-item-code']),
    adjustment_id: normalizeStr(rawRow['adjustment-id']),
    seller_sku: normalizeStr(rawRow['sku']),
    seller_sku_normalized: normalizeSku(rawRow['sku']),
    transaction_type: normalizeStr(rawRow['transaction-type']) || 'Order',
    marketplace_name: normalizeStr(rawRow['marketplace-name']),
    fulfillment_id: normalizeStr(rawRow['fulfillment-id']),
    quantity_purchased: parseQty(rawRow['quantity-purchased']),
    price_type: normalizeStr(rawRow['price-type']),
    price_amount_micro: parseAmount(rawRow['price-amount']),
    item_related_fee_type: normalizeStr(rawRow['item-related-fee-type']),
    item_related_fee_amount_micro: parseAmount(rawRow['item-related-fee-amount']),
    promotion_id: normalizeStr(rawRow['promotion-id']),
    promotion_type: normalizeStr(rawRow['promotion-type']),
    promotion_amount_micro: parseAmount(rawRow['promotion-amount']),
    shipment_fee_type: normalizeStr(rawRow['shipment-fee-type']),
    shipment_fee_amount_micro: parseAmount(rawRow['shipment-fee-amount']),
    order_fee_type: normalizeStr(rawRow['order-fee-type']),
    order_fee_amount_micro: parseAmount(rawRow['order-fee-amount']),
    misc_fee_amount_micro: parseAmount(rawRow['misc-fee-amount']),
    other_fee_amount_micro: parseAmount(rawRow['other-fee-amount']),
    other_fee_reason_description: normalizeStr(rawRow['other-fee-reason-description']),
    direct_payment_type: normalizeStr(rawRow['direct-payment-type']),
    direct_payment_amount_micro: parseAmount(rawRow['direct-payment-amount']),
    other_amount_micro: parseAmount(rawRow['other-amount']),
    currency: normalizeStr(rawRow['currency']) || 'JPY',
    ingest_run_id: ctx.runId,
    observed_at: ctx.observedAt,
    ingested_at: nowSql(),
  };
  row.year_month_int = dateToYearMonthInt(row.economic_date);
  // hash
  row.business_line_key = makeBusinessKey(row);
  row.physical_line_hash = makePhysicalHash(row, row.source_document_id, row.source_line_no);
  return row;
}

// ─── DB INSERT ───

const INSERT_HEADER_SQL = `
  INSERT OR IGNORE INTO raw_amazon_settlement_headers (
    physical_line_hash, business_line_key,
    source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, settlement_start_date, settlement_end_date, deposit_date,
    total_amount_micro, currency,
    ingest_run_id, observed_at, ingested_at
  ) VALUES (
    @physical_line_hash, @business_line_key,
    @source_document_id, @source_file_hash, @source_path, @source_line_no, @source_layer, @parser_version,
    @source_settlement_id, @settlement_start_date, @settlement_end_date, @deposit_date,
    @total_amount_micro, @currency,
    @ingest_run_id, @observed_at, @ingested_at
  )
`;

const INSERT_LINE_SQL = `
  INSERT OR IGNORE INTO raw_amazon_settlement_lines (
    physical_line_hash, business_line_key,
    source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, posted_date_utc, posted_datetime_jst, economic_date, year_month_int,
    amazon_order_id, merchant_order_id, shipment_id, order_item_code, adjustment_id,
    seller_sku, seller_sku_normalized,
    transaction_type, marketplace_name, fulfillment_id,
    quantity_purchased,
    price_type, price_amount_micro,
    item_related_fee_type, item_related_fee_amount_micro,
    promotion_id, promotion_type, promotion_amount_micro,
    shipment_fee_type, shipment_fee_amount_micro,
    order_fee_type, order_fee_amount_micro,
    misc_fee_amount_micro, other_fee_amount_micro, other_fee_reason_description,
    direct_payment_type, direct_payment_amount_micro, other_amount_micro,
    currency,
    ingest_run_id, observed_at, ingested_at
  ) VALUES (
    @physical_line_hash, @business_line_key,
    @source_document_id, @source_file_hash, @source_path, @source_line_no, @source_layer, @parser_version,
    @source_settlement_id, @posted_date_utc, @posted_datetime_jst, @economic_date, @year_month_int,
    @amazon_order_id, @merchant_order_id, @shipment_id, @order_item_code, @adjustment_id,
    @seller_sku, @seller_sku_normalized,
    @transaction_type, @marketplace_name, @fulfillment_id,
    @quantity_purchased,
    @price_type, @price_amount_micro,
    @item_related_fee_type, @item_related_fee_amount_micro,
    @promotion_id, @promotion_type, @promotion_amount_micro,
    @shipment_fee_type, @shipment_fee_amount_micro,
    @order_fee_type, @order_fee_amount_micro,
    @misc_fee_amount_micro, @other_fee_amount_micro, @other_fee_reason_description,
    @direct_payment_type, @direct_payment_amount_micro, @other_amount_micro,
    @currency,
    @ingest_run_id, @observed_at, @ingested_at
  )
`;

const UPSERT_DIM_TX_SQL = `
  INSERT INTO dim_amazon_transaction_type (transaction_type, observed_first_at, observed_last_at)
  VALUES (?, ?, ?)
  ON CONFLICT(transaction_type) DO UPDATE SET observed_last_at = excluded.observed_last_at
`;

const UPSERT_DIM_PRICE_SQL = `
  INSERT INTO dim_amazon_price_type (price_type, observed_first_at, observed_last_at)
  VALUES (?, ?, ?)
  ON CONFLICT(price_type) DO UPDATE SET observed_last_at = excluded.observed_last_at
`;

const UPSERT_DIM_FEE_SQL = `
  INSERT INTO dim_amazon_fee_type (fee_type, observed_first_at, observed_last_at)
  VALUES (?, ?, ?)
  ON CONFLICT(fee_type) DO UPDATE SET observed_last_at = excluded.observed_last_at
`;

const UPSERT_REFRESH_QUEUE_SQL = `
  INSERT INTO settlement_refresh_queue (year_month_int, reason, enqueued_at)
  VALUES (?, ?, ?)
  ON CONFLICT(year_month_int) DO UPDATE SET
    reason = excluded.reason,
    enqueued_at = excluded.enqueued_at,
    processed_at = NULL
`;

// ─── TSV → 正規化行 (監査PR-13: 冪等性テストが本番同一経路を叩けるよう関数化+export) ───
// main のレポート処理ループから抽出。純関数 (DB 非依存)。
export function prepareReportTsv(tsv, reportId, runId) {
  const sourceFileHash = sha256(tsv);
  const { rows } = parseTsv(tsv);
  const ctx = {
    sourceDocumentId: reportId,
    sourceFileHash,
    sourcePath: `sp_api://${REPORT_TYPE}/${reportId}`,
    runId,
    observedAt: nowIso(),
  };
  let headerRow = null;
  const lineRows = [];
  for (const raw of rows) {
    if (isHeaderRow(raw)) {
      headerRow = normalizeHeaderRow(raw, ctx);
    } else {
      lineRows.push(normalizeLineRow(raw, ctx));
    }
  }
  return { headerRow, lineRows, ctx, sourceFileHash, rowCount: rows.length };
}

export function ingestSettlement(db, headerRow, lineRows, ctx) {
  const insertHeader = db.prepare(INSERT_HEADER_SQL);
  const insertLine = db.prepare(INSERT_LINE_SQL);
  const upsertDimTx = db.prepare(UPSERT_DIM_TX_SQL);
  const upsertDimPrice = db.prepare(UPSERT_DIM_PRICE_SQL);
  const upsertDimFee = db.prepare(UPSERT_DIM_FEE_SQL);
  const upsertRefresh = db.prepare(UPSERT_REFRESH_QUEUE_SQL);

  const dirtyMonths = new Set();
  let headerInserted = 0;
  let lineInserted = 0;

  const txn = db.transaction(() => {
    if (headerRow) {
      const r = insertHeader.run(headerRow);
      if (r.changes > 0) headerInserted++;
    }
    for (const line of lineRows) {
      // dim 自動 INSERT
      if (line.transaction_type) upsertDimTx.run(line.transaction_type, ctx.observedAt, ctx.observedAt);
      if (line.price_type) upsertDimPrice.run(line.price_type, ctx.observedAt, ctx.observedAt);
      if (line.item_related_fee_type) upsertDimFee.run(line.item_related_fee_type, ctx.observedAt, ctx.observedAt);

      const r = insertLine.run(line);
      if (r.changes > 0) {
        lineInserted++;
        if (line.year_month_int) dirtyMonths.add(line.year_month_int);
      }
    }
    // refresh queue
    for (const ym of dirtyMonths) {
      upsertRefresh.run(ym, 'new_ingest', ctx.observedAt);
    }
  });
  txn();

  return { headerInserted, lineInserted, dirtyMonths: [...dirtyMonths] };
}

// ─── Main ───

async function main() {
  const args = parseArgs();
  const runId = `settlement-${Date.now()}`;
  console.log(`[settlements] run_id=${runId}, dry-run=${args.dryRun}`);

  await initDB();
  const db = getDB();

  // 1. Settlement 一覧取得
  let reports;
  if (args.reportId) {
    const sp = getClient();
    const r = await sp.callAPI({ operation: 'getReport', endpoint: 'reports', path: { reportId: args.reportId } });
    reports = [r];
  } else {
    reports = await listSettlementReports();
  }
  console.log(`[settlements] 対象 reports: ${reports.length}件`);

  let totalHeaders = 0, totalLines = 0;
  const allDirtyMonths = new Set();

  for (let i = 0; i < reports.length; i++) {
    const r = reports[i];
    if (r.processingStatus !== 'DONE' || !r.reportDocumentId) {
      console.log(`[skip ${i + 1}/${reports.length}] reportId=${r.reportId} status=${r.processingStatus}`);
      continue;
    }
    console.log(`\n[${i + 1}/${reports.length}] reportId=${r.reportId} (${r.dataStartTime?.slice(0, 10)} 〜 ${r.dataEndTime?.slice(0, 10)})`);

    const tsv = await downloadReportTsv(r.reportDocumentId);
    const { headerRow, lineRows, ctx, sourceFileHash, rowCount } = prepareReportTsv(tsv, r.reportId, runId);
    console.log(`  bytes: ${tsv.length}, file_hash: ${sourceFileHash.slice(0, 12)}...`);
    console.log(`  rows: ${rowCount}`);
    console.log(`  parsed: header=${headerRow ? 1 : 0}, lines=${lineRows.length}`);

    if (args.dryRun) {
      console.log('  [dry-run] DB 投入スキップ');
      // sample 表示
      if (lineRows.length > 0) {
        const sample = lineRows[0];
        console.log('  sample line:', JSON.stringify({
          settlement_id: sample.source_settlement_id,
          posted_jst: sample.posted_datetime_jst,
          economic_date: sample.economic_date,
          year_month: sample.year_month_int,
          tx: sample.transaction_type,
          sku: sample.seller_sku_normalized,
          qty: sample.quantity_purchased,
          price_type: sample.price_type,
          price_micro: sample.price_amount_micro,
        }));
      }
      continue;
    }

    const result = ingestSettlement(db, headerRow, lineRows, ctx);
    console.log(`  inserted: header=${result.headerInserted}, lines=${result.lineInserted}, dirty_months=${result.dirtyMonths.join(',')}`);
    totalHeaders += result.headerInserted;
    totalLines += result.lineInserted;
    result.dirtyMonths.forEach(m => allDirtyMonths.add(m));
  }

  console.log(`\n[settlements] 完了: headers=${totalHeaders}, lines=${totalLines}, dirty_months=${[...allDirtyMonths].join(',')}`);
  if (args.dryRun) console.log('[dry-run] 実 DB 変更なし');
}

// 監査PR-13: テストから import できるよう、直接実行時のみ main を起動
// (daily-sync は execFileSync で直接実行 = 従来通り動く)
import { pathToFileURL } from 'node:url';
const isDirectRun = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isDirectRun) {
  main().catch(e => { console.error('FATAL:', e?.message || e); if (e?.stack) console.error(e.stack); process.exit(1); });
}

// テスト用 export (本番経路と同一の関数群。監査PR-13 冪等性smokeテストが使用)
export { makePhysicalHash, makeBusinessKey, parseAmount, PHYSICAL_HASH_EXCLUDE };
