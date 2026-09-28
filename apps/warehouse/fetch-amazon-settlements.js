/**
 * fetch-amazon-settlements.js — Phase 3.1.1 ingest スクリプト
 *
 * SP-API Reports API で Settlement Report を取得し、raw_amazon_settlement_headers / raw_amazon_settlement_lines に投入する。
 *
 * 🚨 2026-09-28: 既定を V2 (GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2) に切り替えた (V1 = GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE は 2026-11-11 廃止)。
 *   V2 は形が違う (金額が amount-type / amount-description / amount の縦並び) → amazon-settlement-v2.js で V1 の形の TSV に並べ直してから
 *   今までと同じ正規化に通す (source_layer = 'sp_api_v2')。並べ直しは 6 期間の V1 / V2 で business_line_key が全部一致することを確かめた。
 *   V1 で取込済みの決済 (同じ settlement-id が sp_api_v1 にある) は V2 では入れない (中身は同じ = raw を倍にしない)。
 *   並べ直しの規則に無い組み合わせ・日時の空・品物の番号を補えない行が 1 つでもあるレポートは **取り込まない** + 終了コード 3
 *   (daily-sync で ❌ = 規則を足す合図。取り込んでから規則を直すと古い行と新しい行が二重になるため。V2 は約 90 日取り直せる = 落ちない。Codex #1508 R1)
 *   V1 が必要なら --source v1 (11/11 まで)
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
 *   node apps/warehouse/fetch-amazon-settlements.js --source v1  # 旧 V1 レポートで取る (2026-11-11 まで)
 */

import 'dotenv/config';
import SellingPartner from 'amazon-sp-api';
import zlib from 'zlib';
import crypto from 'crypto';
import canonicalize from 'canonicalize';
import { initDB, getDB } from './db.js';
import { convertV2TsvToV1Tsv, parseV2Tsv } from './amazon-settlement-v2.js';

const REGION = 'fe';
const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';
// 取得元ごとのレポートの種類・層・パーサの版 (V2 = V1 の形に並べ直してから同じ正規化)
export const SOURCES = {
  v1: { reportType: 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE', sourceLayer: 'sp_api_v1', parserVersion: 'v1.0.0' },
  v2: { reportType: 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2', sourceLayer: 'sp_api_v2', parserVersion: 'v2.0.0' },
};

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
  const r = { reportId: null, dryRun: false, source: 'v2' };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--report-id' && args[i + 1]) r.reportId = args[++i];
    else if (args[i] === '--dry-run') r.dryRun = true;
    else if (args[i] === '--source' && args[i + 1]) r.source = args[++i];
  }
  if (!Object.hasOwn(SOURCES, r.source)) throw new Error(`--source は v1 か v2: ${r.source}`);
  return r;
}

// ─── SP-API ───

async function listSettlementReports(reportType) {
  const sp = getClient();
  const all = [];
  let nextToken = null;
  let pageNum = 0;
  do {
    pageNum++;
    const params = nextToken ? { nextToken } : {
      reportTypes: [reportType],
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
    source_layer: ctx.sourceLayer,
    parser_version: ctx.parserVersion,
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
    source_layer: ctx.sourceLayer,
    parser_version: ctx.parserVersion,
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
// opts.source = 'v1' (既定。V1 の TSV) / 'v2' (V1 の形に並べ直した TSV)。opts.sourceFileHash = 元のファイルの hash (V2 は並べ直す前)
export function prepareReportTsv(tsv, reportId, runId, opts = {}) {
  const src = SOURCES[opts.source || 'v1'];
  if (!src) throw new Error(`source が違う: ${opts.source}`);
  const sourceFileHash = opts.sourceFileHash || sha256(tsv);
  const { rows } = parseTsv(tsv);
  const ctx = {
    sourceDocumentId: reportId,
    sourceFileHash,
    sourcePath: `sp_api://${src.reportType}/${reportId}`,
    sourceLayer: src.sourceLayer,
    parserVersion: src.parserVersion,
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

/** V2 の TSV → V1 の形に並べ直して prepareReportTsv。unknown = 並べ直しの規則に無い組み合わせ / itemCodeUnresolved = 品物の番号を補えなかったポイントの行 */
export function prepareV2ReportTsv(v2Tsv, reportId, runId) {
  const c = convertV2TsvToV1Tsv(v2Tsv);
  const p = prepareReportTsv(c.tsv, reportId, runId, { source: 'v2', sourceFileHash: sha256(v2Tsv) });
  return { ...p, v2RowCount: c.v2Rows, unknown: c.unknown, itemCodeUnresolved: c.itemCodeUnresolved };
}

/**
 * V2 のレポート 1 本を処理する (main のループの中身。試験から呼べるように関数に)。
 * 返り値 status: 'blocked' (規則に無いもの・形の違い = 取り込まない) / 'skipped_v1' (V1 で取込済み) / 'dry_run' / 'ingested'
 */
export function processV2Report(db, v2Tsv, reportId, runId, { dryRun = false } = {}) {
  // 🚨 先に決済の番号を読んで V1 取込済みか見る (V1 で代わりに入れた決済を、並べ直せないからと毎朝 ❌ にしない。Codex #1508 R2)
  let settlementId = null;
  try { settlementId = parseV2Tsv(v2Tsv).rows.map((r) => r['settlement-id']).find((x) => x) || null; }
  catch (e) { return { status: 'blocked', reason: `V2 として読めない: ${e.message}`, unknown: [], itemCodeUnresolved: 0 }; }
  if (!settlementId) return { status: 'blocked', reason: '決済の番号 (settlement-id) が無い', unknown: [], itemCodeUnresolved: 0 };
  if (settlementIngestedByV1(db, settlementId)) return { status: 'skipped_v1', settlementId, unknown: [], itemCodeUnresolved: 0 };
  let p;
  try { p = prepareV2ReportTsv(v2Tsv, reportId, runId); }
  catch (e) { return { status: 'blocked', reason: `並べ直せない: ${e.message}`, unknown: [], itemCodeUnresolved: 0 }; }
  const base = { prepared: p, unknown: p.unknown, itemCodeUnresolved: p.itemCodeUnresolved };
  if (p.unknown.length || p.itemCodeUnresolved) return { ...base, status: 'blocked', reason: `規則に無い組み合わせ ${JSON.stringify(p.unknown)} / 品物の番号を補えないポイントの行 ${p.itemCodeUnresolved}` };
  if (!p.headerRow || p.headerRow.source_settlement_id !== settlementId) return { ...base, status: 'blocked', reason: '決済の見出しの行が無い / 明細と決済の番号が違う' };
  if (dryRun) return { ...base, status: 'dry_run' };
  return { ...base, status: 'ingested', result: ingestSettlement(db, p.headerRow, p.lineRows, p.ctx) };
}

/** その決済が V1 (sp_api_v1) で取込済みか (V2 で同じ決済を入れ直さない = 中身は同じ・raw を倍にしない) */
export function settlementIngestedByV1(db, settlementId) {
  if (!settlementId) return false;
  return !!db.prepare(`SELECT 1 FROM raw_amazon_settlement_headers WHERE source_settlement_id = ? AND source_layer = 'sp_api_v1' LIMIT 1`).get(settlementId);
}

// 🚨 1 回の呼び出し = 1 決済の完全なレポート 1 本 (下流の重複除去は「同じ文書の中の出現順」で数える = db.js の v_amazon_settlement_unified の注記)。
//   1 つの決済を複数の文書に分けて入れないこと
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
  const src = SOURCES[args.source];
  console.log(`[settlements] run_id=${runId}, dry-run=${args.dryRun}, source=${args.source} (${src.reportType})`);

  await initDB();
  const db = getDB();

  // 1. Settlement 一覧取得
  let reports;
  if (args.reportId) {
    const sp = getClient();
    const r = await sp.callAPI({ operation: 'getReport', endpoint: 'reports', path: { reportId: args.reportId } });
    reports = [r];
  } else {
    reports = await listSettlementReports(src.reportType);
  }
  console.log(`[settlements] 対象 reports: ${reports.length}件`);

  let totalHeaders = 0, totalLines = 0;
  const allDirtyMonths = new Set();
  const blocked = [];

  for (let i = 0; i < reports.length; i++) {
    const r = reports[i];
    if (r.processingStatus !== 'DONE' || !r.reportDocumentId) {
      console.log(`[skip ${i + 1}/${reports.length}] reportId=${r.reportId} status=${r.processingStatus}`);
      continue;
    }
    console.log(`\n[${i + 1}/${reports.length}] reportId=${r.reportId} (${r.dataStartTime?.slice(0, 10)} 〜 ${r.dataEndTime?.slice(0, 10)})`);

    const tsv = await downloadReportTsv(r.reportDocumentId);
    if (args.source === 'v2') {
      const v = processV2Report(db, tsv, r.reportId, runId, { dryRun: args.dryRun });
      if (v.prepared) console.log(`  bytes: ${tsv.length}, rows: ${v.prepared.rowCount} (V2 の元の行 ${v.prepared.v2RowCount} → V1 の形), lines=${v.prepared.lineRows.length}`);
      if (v.status === 'blocked') { console.log(`  ❌ 取り込まない: ${v.reason}`); blocked.push({ reportId: r.reportId, reason: v.reason }); continue; }
      if (v.status === 'skipped_v1') { console.log(`  [skip] settlement-id ${v.settlementId} は V1 (sp_api_v1) で取込済み = V2 では入れない`); continue; }
      if (v.status === 'dry_run') { console.log('  [dry-run] DB 投入スキップ'); continue; }
      console.log(`  inserted: header=${v.result.headerInserted}, lines=${v.result.lineInserted}, dirty_months=${v.result.dirtyMonths.join(',')}`);
      totalHeaders += v.result.headerInserted; totalLines += v.result.lineInserted;
      v.result.dirtyMonths.forEach((m) => allDirtyMonths.add(m));
      continue;
    }
    const prepared = prepareReportTsv(tsv, r.reportId, runId);
    const { headerRow, lineRows, ctx, sourceFileHash, rowCount } = prepared;
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
  if (blocked.length) {
    console.error(`[settlements] ❌ 取り込まなかった V2 のレポート ${blocked.length} 本: ${JSON.stringify(blocked)}`);
    console.error('[settlements] → apps/warehouse/amazon-settlement-v2.js に規則を足す (V1 の書き方に合わせる。11/11 までは --source v1 でも取れる)。直るまで毎回 終了コード 3');
    process.exitCode = 3;
  }
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
