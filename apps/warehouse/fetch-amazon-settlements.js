/**
 * fetch-amazon-settlements.js — Phase 3.1.1 ingest スクリプト
 *
 * SP-API Reports API で Settlement Report を取得し、raw_amazon_settlement_headers / raw_amazon_settlement_lines に投入する。
 *
 * 🚨 2026-09-28: 既定を V2 (GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2) に切り替えた (V1 = GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE は 2026-11-11 廃止)。
 *   V2 は形が違う (金額が amount-type / amount-description / amount の縦並び) → amazon-settlement-v2.js で V1 の形の TSV に並べ直してから
 *   今までと同じ正規化に通す (source_layer = 'sp_api_v2')。並べ直しは 6 期間の V1 / V2 で business_line_key が全部一致することを確かめた。
 *   V1 で取込済みの決済 (同じ settlement-id が sp_api_v1 にある) は V2 では入れない (中身は同じ = raw を倍にしない)。
 *   逆に V2 で取込済みの決済は V1 (--source v1) でも入れない (skipped_v2)。古い決済は V1 と V2 で行の分け方が違い、両方入ると二重になる
 *   (2026-10-02・Codex #1582 R1 High)。確かめは ingestSettlement の書き込みと同じ取引の中
 *   並べ直しの規則に無い組み合わせ・日時の空・品物の番号を補えない行が 1 つでもあるレポートは **取り込まない** + 終了コード 3
 *   (daily-sync で ❌ = 規則を足す合図。取り込んでから規則を直すと古い行と新しい行が二重になるため。V2 は約 90 日取り直せる = 落ちない。Codex #1508 R1)
 *   V1 が必要なら --source v1 (11/11 まで)
 *
 * 🆕 2026-09-30 (D7b-1b の下ごしらえ・設計 = AI_reference CompanyDB構想/13 §3.1): 決済のレポートの一覧 (inventory) を記録する
 *   = amazon_settlement_report_inventory_runs (回) / amazon_settlement_report_inventory (report ごと・取込の結果)。部品 = amazon-settlement-inventory.js
 *   - 一覧の記録用の getReports は **取込の一覧とは別の要求** で、最初の要求に createdUntil = 回の開始の時刻・createdSince = その 85 日前 を明示して固定
 *     (取込の一覧は今までどおり日時の境なし = Amazon の既定の 90 日前〜今 → 取込む report は変わらない)
 *   - 🚨 呼ぶ順 = 取込の一覧の要求 (今と同じ位置・同じ形) → 取込のダウンロードのループ (結果は report ごとにメモリ) →
 *     **ループが全部終わった後** に一覧の記録の要求 (時間の上限 120 秒・ページの応答の形も確かめる) → 一覧・取込の結果・完了を 1 つの取引で書く
 *     (一覧の要求が取込の時間やレートの枠を食わない。Codex #1555 R1 High・R2 High)
 *   - 一覧の記録の要求は **専用の SP-API の接続** (getInventoryClient = 要求の時間の上限つき・429 / 通信の失敗で自動の再試行なし)。
 *     期限でライブラリが socket を破棄する = 取込が済んだら node が自分で終わる (Codex #1555 R3)。取込の接続 (getClient) の設定は変えない
 *     トークンの自動の取り直しも切り、最初に期限の中で 1 回だけ取る (403 expired = 一覧の失敗・再帰で呼ばない。Codex #1555 R4)
 *   - nextToken が残ったまま上限のページ (21) に来たら last_page_reached = 0 + ⚠️
 *   - 一覧の失敗・記録の失敗は ⚠️ だけ (取込の結果・終了コードは変わらない)。記録に失敗した回は completed_at を入れず record_error に理由
 *   - 取込が例外で止まった回も一覧を記録する (completed_at null・ingest_error)。kill された回は記録が無い = coverage に使えない (安全側)
 *   - --dry-run は表に書かない。--report-id の 1 本だけの回は一覧の回にしない
 *   - 今は記録だけ (読み手 = 後の coverage)。取込む行 (raw_amazon_settlement_*) の中身と数は変えない
 *   - 🚨 daily-sync が渡す --days 14 はこのスクリプトでは読んでいない (昔から。取込の一覧は既定の 90 日)
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
 *   node apps/warehouse/fetch-amazon-settlements.js              # 直近90日全件 (+ 決済の一覧を記録)
 *   node apps/warehouse/fetch-amazon-settlements.js --report-id 1487945020577   # 特定 reportId のみ (一覧の回にしない)
 *   node apps/warehouse/fetch-amazon-settlements.js --dry-run    # DL だけして DB に書かない (一覧も記録しない)
 *   node apps/warehouse/fetch-amazon-settlements.js --source v1  # 旧 V1 レポートで取る (2026-11-11 まで)
 */

import 'dotenv/config';
import SellingPartner from 'amazon-sp-api';
import zlib from 'zlib';
import crypto from 'crypto';
import canonicalize from 'canonicalize';
import { initDB, getDB } from './db.js';
import { convertV2TsvToV1Tsv, parseV2Tsv } from './amazon-settlement-v2.js';
import {
  MAX_LIST_PAGES, INVENTORY_TIMEOUT_MS, listReportPages, listInventoryReports, inventoryWindow, inventoryEntries, inventorySnapshotDigest,
  recordInventorySnapshot, inventoryClientOptions,
} from './amazon-settlement-inventory.js';

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

// 一覧の記録用の **専用の接続** (Codex #1555 R3・R4): 時間の上限つき・429 / 通信の失敗 / 403 expired で自動の再試行・トークンの取り直しをしない
// (取消されない再試行の timer や socket が残ると、取込が済んでも node が終わらず daily-sync の枠で kill される)。
// 取込の接続 (getClient) の設定は変えない
let inventorySpClient = null;
function getInventoryClient() {
  if (!inventorySpClient) {
    inventorySpClient = new SellingPartner({
      region: REGION,
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
      options: inventoryClientOptions(INVENTORY_TIMEOUT_MS),
    });
  }
  return inventorySpClient;
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

// 取込の一覧 = 今までと同じ要求 (日時の境なし = Amazon の既定の 90 日前〜今・21 ページで打ち切り)。
// 🚨 取込む report の選び方を変えないため、一覧の記録 (窓を固定) はこれとは別の要求 (amazon-settlement-inventory.js)
async function listSettlementReports(sp, reportType) {
  const r = await listReportPages(sp, {
    reportTypes: [reportType],
    marketplaceIds: [MARKETPLACE_ID],
    pageSize: 100,
  }, { maxPages: MAX_LIST_PAGES, label: 'list' });
  return r.reports;
}

async function downloadReportTsv(sp, reportDocumentId) {
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
  const result = ingestSettlement(db, p.headerRow, p.lineRows, p.ctx);
  // 上の確かめ (取引の外) の後に V1 が入った = 取引の中の確かめで 1 行も入れていない (Codex #1582 R1 High)
  if (result.skipped === 'skipped_v1') return { status: 'skipped_v1', settlementId, unknown: [], itemCodeUnresolved: 0 };
  return { ...base, status: 'ingested', result };
}

/** その決済の見出し・明細がその層 (source_layer) にあるか */
export function settlementInLayer(db, settlementId, layer) {
  if (!settlementId) return false;
  return !!(db.prepare(`SELECT 1 FROM raw_amazon_settlement_headers WHERE source_settlement_id = ? AND source_layer = ? LIMIT 1`).get(settlementId, layer)
    || db.prepare(`SELECT 1 FROM raw_amazon_settlement_lines WHERE source_settlement_id = ? AND source_layer = ? LIMIT 1`).get(settlementId, layer));
}

/** その決済が V1 (sp_api_v1) で取込済みか (V2 で同じ決済を入れ直さない = 中身は同じ・raw を倍にしない) */
export function settlementIngestedByV1(db, settlementId) {
  return settlementInLayer(db, settlementId, 'sp_api_v1');
}

// 🚨 1 つの決済は V1 (sp_api_v1) と V2 (sp_api_v2) の **どちらか一方だけ** 入れる (Codex #1582 R1 High)。
//   古い決済は V1 と V2 で行の分け方が違う (2026-01 の Easy Ship = V1 は MFNPostageFee + MFNPostageFeeTax の 2 行・V2 の並べ直しは 1 行)
//   = business_line_key が合わず、下流 (v_amazon_settlement_unified) の重複除去で 1 つにならない = 両方入ると金額が二重になる。
//   → もう片方の層に同じ決済の見出し・明細があれば入れない (V2 の取込 = skipped_v1・V1 の取込 = skipped_v2。どちらも取り込んだ側の中身で足りる)
export const OTHER_SP_API_LAYER = Object.freeze({
  sp_api_v1: Object.freeze({ layer: 'sp_api_v2', skipped: 'skipped_v2' }),
  sp_api_v2: Object.freeze({ layer: 'sp_api_v1', skipped: 'skipped_v1' }),
});

// 🚨 1 回の呼び出し = 1 決済の完全なレポート 1 本 (下流の重複除去は「同じ文書の中の出現順」で数える = db.js の v_amazon_settlement_unified の注記)。
//   1 つの決済を複数の文書に分けて入れないこと
//   返り値の skipped = 'skipped_v1' / 'skipped_v2' (もう片方の層で取込済み = 1 行も入れていない) / null
//   🚨 もう片方の層の確かめは書き込みと **同じ取引の中** (BEGIN IMMEDIATE = 書き込みの鍵を先に取る = 同時に走った取込が確かめと書き込みの間に入れない)
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
  const other = ctx && Object.hasOwn(OTHER_SP_API_LAYER, ctx.sourceLayer) ? OTHER_SP_API_LAYER[ctx.sourceLayer] : null;
  const settlementIds = [...new Set([headerRow?.source_settlement_id, ...lineRows.map((l) => l.source_settlement_id)].filter((x) => x != null && x !== ''))];
  let skipped = null;

  const txn = db.transaction(() => {
    if (other && settlementIds.some((id) => settlementInLayer(db, id, other.layer))) { skipped = other.skipped; return; }
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
  txn.immediate();

  return { headerInserted, lineInserted, dirtyMonths: [...dirtyMonths], skipped, settlementId: settlementIds[0] ?? null };
}

// ─── Main ───

/**
 * 決済のレポートの一覧 (inventory) を取って、取込の結果と合わせて 1 回で記録する (取込の一覧とは別の要求・窓を固定)。
 * 🚨 取込のループが **全部終わった後** に呼ぶ (Codex #1555 R2 High: ループの前に置くと、最大 120 秒の要求と取消されない裏の retry が
 *    daily-sync の 60 分の枠を食い、取込の後半が切られうる)。一覧の失敗・記録の失敗は ⚠️ だけ (取込の結果・終了コードに影響しない)。
 *    dry-run は表に書かない
 */
async function takeSettlementInventory(db, sp, { reportType, runId, dryRun, now, startedAt, timeoutMs, results, ingestError }) {
  const window = inventoryWindow(startedAt);
  const state = { id: null, window, listing: null, listError: null, recordError: null, count: 0, warn: null };
  try {
    state.listing = await listInventoryReports(sp, { reportType, marketplaceId: MARKETPLACE_ID, startedAt, timeoutMs });
  } catch (e) {
    state.listError = `一覧の要求の失敗: ${e?.message || e}`;
    console.log(`[inventory] ⚠️ ${state.listError} (取込は済んでいる・影響なし)`);
  }
  const listCompletedAt = state.listing ? now() : null;
  if (dryRun) {
    if (state.listing) {
      try {
        const { entries, missingId } = inventoryEntries(state.listing.reports, reportType);
        state.count = entries.length;
        console.log(`[inventory] 窓 ${window.createdSince} 〜 ${window.createdUntil}: report ${entries.length} 本 / ${state.listing.pages} ページ / 最後のページまで ${state.listing.lastPageReached ? '✅' : '⚠️ 取れていない'} / digest ${inventorySnapshotDigest(entries).slice(0, 12)}…${missingId ? ` / ⚠️ reportId の無いもの ${missingId} 件` : ''}`);
      } catch (e) { console.log(`[inventory] ⚠️ 一覧を読めない: ${e?.message || e}`); }
    }
    console.log('[inventory] [dry-run] 一覧は記録しない');
    return state;
  }
  const w = recordInventorySnapshot(db, {
    reportType, marketplaceId: MARKETPLACE_ID, window, startedAt, listCompletedAt,
    listing: state.listing, listError: state.listError, ingestRunId: runId, ingestError,
  }, results, ingestError ? null : now());
  state.id = w.id;
  state.recordError = w.recordError;
  if (w.recordError) console.log(`[inventory] ⚠️ ${w.recordError} (取込は済んでいる・影響なし)`);
  if (w.id != null) {
    try {
      const h = db.prepare(`SELECT report_count, page_count, last_page_reached, snapshot_digest, list_error, inventory_run_seq, completed_at FROM amazon_settlement_report_inventory_runs WHERE id = ?`).get(w.id);
      state.count = h.report_count;
      state.warn = h.list_error;
      console.log(`[inventory] 回 #${h.inventory_run_seq} 窓 ${window.createdSince} 〜 ${window.createdUntil}: report ${h.report_count} 本 / ${h.page_count} ページ / 最後のページまで ${h.last_page_reached ? '✅' : '⚠️ 取れていない'} / digest ${(h.snapshot_digest || '-').slice(0, 12)} / 完了 ${h.completed_at || '— (未完了)'}${h.list_error ? ` / ⚠️ ${h.list_error}` : ''}`);
    } catch (e) { console.log(`[inventory] ⚠️ 記録した回を読めない: ${e?.message || e}`); }
  }
  return state;
}

/** 完了の行の末尾 (daily-sync の朝の報告に出る) */
function inventorySummary(inv, args) {
  if (!inv) return '';
  if (inv.listError) return ' | ⚠️ 決済の一覧を取れなかった (取込は済んでいる)';
  if (args.dryRun) return ` | 決済の一覧 ${inv.count} 本 (dry-run = 記録しない)`;
  if (inv.recordError) return ' | ⚠️ 決済の一覧を記録できなかった';
  if (!inv.listing.lastPageReached) return ` | ⚠️ 決済の一覧が途中まで (${inv.listing.pages} ページで打ち切り・一覧 ${inv.count} 本)`;
  if (inv.warn) return ` | ⚠️ 決済の一覧 ${inv.count} 本 (${inv.warn})`;
  return ` | 決済の一覧 ${inv.count} 本を記録`;
}

/**
 * 取込の本体 (main から呼ぶ。試験から SP-API を差し替えて呼べるように関数にした)。
 * deps = { db, sp (callAPI を持つ), runId, downloadTsv(reportDocumentId) → TSV の文字 (既定 = SP-API), now() → Date,
 *          inventorySp (一覧の記録用の専用の接続・既定 = sp。本番は getInventoryClient = 時間の上限つき・自動の再試行なし),
 *          inventoryTimeoutMs (一覧の記録用の要求の時間の上限) }
 * 返り値 = { totalHeaders, totalLines, dirtyMonths, blocked, inventory }
 * 🚨 取込む report の選び方・取込む行は今までと同じ。足したのは一覧の記録 (別の要求) と、各分岐での取込の結果の記録だけ
 */
export async function runSettlementFetch(args, { db, sp, runId, downloadTsv = null, now = () => new Date(), inventorySp = null, inventoryTimeoutMs = INVENTORY_TIMEOUT_MS }) {
  const src = SOURCES[args.source];
  const download = downloadTsv || ((reportDocumentId) => downloadReportTsv(sp, reportDocumentId));
  const startedAt = now();   // 回の開始の時刻 = 一覧の窓の createdUntil

  // 1. Settlement 一覧取得 (🚨 今と同じ位置・同じ要求で **先に** 確定する。一覧の記録の要求を先に出すと、既定の 90 日の窓の「今」が
  //    後ろにずれて境の report が外れる / レートの枠を先に使う = 取込む report が変わりうる。Codex #1555 R1 High)
  let reports;
  if (args.reportId) {
    const r = await sp.callAPI({ operation: 'getReport', endpoint: 'reports', path: { reportId: args.reportId } });
    reports = [r];
  } else {
    reports = await listSettlementReports(sp, src.reportType);
  }
  console.log(`[settlements] 対象 reports: ${reports.length}件`);

  // 取込の結果は report ごとにメモリに持つ (一覧の記録はループが全部終わった後に 1 回で書く・Codex #1555 R2 High)
  const results = new Map();
  const rec = (reportId, result, extra = {}) => { if (reportId != null) results.set(String(reportId), { result, extra }); };

  let totalHeaders = 0, totalLines = 0;
  const allDirtyMonths = new Set();
  const blocked = [];
  let ingestError = null;

  try {
    for (let i = 0; i < reports.length; i++) {
      const r = reports[i];
      if (r.processingStatus !== 'DONE' || !r.reportDocumentId) {
        console.log(`[skip ${i + 1}/${reports.length}] reportId=${r.reportId} status=${r.processingStatus}`);
        rec(r.reportId, 'skipped_not_done', { note: `status=${r.processingStatus}${r.reportDocumentId ? '' : ' / reportDocumentId なし'}` });
        continue;
      }
      console.log(`\n[${i + 1}/${reports.length}] reportId=${r.reportId} (${r.dataStartTime?.slice(0, 10)} 〜 ${r.dataEndTime?.slice(0, 10)})`);

      // file hash は try の外に持つ = ダウンロードの後の例外 (parse・正規化・DB の投入) でも failed の行に残す (Codex #1555 R1 M2)
      const doc = { importedReportDocumentId: r.reportDocumentId, fileHash: null };
      try {
        const tsv = await download(r.reportDocumentId);
        doc.fileHash = sha256(tsv);   // raw の source_file_hash と同じ式 (V2 は並べ直す前の元のファイル)
        if (args.source === 'v2') {
          const v = processV2Report(db, tsv, r.reportId, runId, { dryRun: args.dryRun });
          if (v.prepared) console.log(`  bytes: ${tsv.length}, rows: ${v.prepared.rowCount} (V2 の元の行 ${v.prepared.v2RowCount} → V1 の形), lines=${v.prepared.lineRows.length}`);
          if (v.status === 'blocked') { console.log(`  ❌ 取り込まない: ${v.reason}`); blocked.push({ reportId: r.reportId, reason: v.reason }); rec(r.reportId, 'failed', { ...doc, note: `blocked: ${v.reason}` }); continue; }
          if (v.status === 'skipped_v1') { console.log(`  [skip] settlement-id ${v.settlementId} は V1 (sp_api_v1) で取込済み = V2 では入れない`); rec(r.reportId, 'skipped_v1', { ...doc, settlementId: v.settlementId }); continue; }
          if (v.status === 'dry_run') { console.log('  [dry-run] DB 投入スキップ'); continue; }
          console.log(`  inserted: header=${v.result.headerInserted}, lines=${v.result.lineInserted}, dirty_months=${v.result.dirtyMonths.join(',')}`);
          totalHeaders += v.result.headerInserted; totalLines += v.result.lineInserted;
          v.result.dirtyMonths.forEach((m) => allDirtyMonths.add(m));
          rec(r.reportId, 'imported', { ...doc, settlementId: v.prepared.headerRow?.source_settlement_id, headerInserted: v.result.headerInserted, linesInserted: v.result.lineInserted });
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
        if (result.skipped === 'skipped_v2') {
          // 🚨 V2 で取込済みの決済は V1 (--source v1) で入れない (古い決済は行の分け方が違い、両方入ると二重。Codex #1582 R1 High)。
          //   skipped_v1 (V2 の側) と同じく [skip] だけ = 終了コードは変えない (中身は V2 で入っている。❌ にすると、規則に無いもので止まった
          //   V2 の決済を --source v1 で入れる回が、90 日の中のほかの決済で毎回 ❌ になる)。
          //   一覧の行の結果は 'imported' (0 行) + 注記 skipped_v2 (結果の CHECK は固定の集合 = 足すには表を作り直す。中身が raw にある点は imported と同じ)
          console.log(`  [skip] settlement-id ${result.settlementId} は V2 (sp_api_v2) で取込済み = V1 では入れない (skipped_v2)`);
          rec(r.reportId, 'imported', { ...doc, settlementId: result.settlementId, headerInserted: 0, linesInserted: 0, note: 'skipped_v2: 同じ決済を V2 (sp_api_v2) で取込済み = V1 では入れない' });
          continue;
        }
        console.log(`  inserted: header=${result.headerInserted}, lines=${result.lineInserted}, dirty_months=${result.dirtyMonths.join(',')}`);
        totalHeaders += result.headerInserted;
        totalLines += result.lineInserted;
        result.dirtyMonths.forEach(m => allDirtyMonths.add(m));
        rec(r.reportId, 'imported', { ...doc, settlementId: headerRow?.source_settlement_id, headerInserted: result.headerInserted, linesInserted: result.lineInserted });
      } catch (e) {
        // 今までどおり例外で回ごと止める (FATAL・終了コード 1)。一覧の行に failed を残し、回の completed_at は null のまま
        rec(r.reportId, 'failed', { ...doc, note: `例外: ${e?.message || e}` });
        throw e;
      }
    }
  } catch (e) {
    ingestError = e;
  }

  // 2. 決済のレポートの一覧 (inventory) = 取込のループが全部終わった後に、時間の上限つきの別の要求で取り、取込の結果と合わせて 1 回で書く。
  //    失敗・時間切れ・記録の失敗でも取込の結果 (生の行・終了コード) は変わらない。--report-id の 1 本だけの回は一覧の回にしない (設計 §3.1)。
  //    取込が例外で止まった回も一覧を記録する (completed_at は null・ingest_error)。kill された回は記録が無い = coverage に使えない (安全側)
  const inv = args.reportId ? null : await takeSettlementInventory(db, inventorySp || sp, {
    reportType: src.reportType, runId, dryRun: args.dryRun, now, startedAt, timeoutMs: inventoryTimeoutMs,
    results, ingestError: ingestError ? `例外: ${ingestError?.message || ingestError}` : null,
  });
  if (ingestError) throw ingestError;

  console.log(`\n[settlements] 完了: headers=${totalHeaders}, lines=${totalLines}, dirty_months=${[...allDirtyMonths].join(',')}${inventorySummary(inv, args)}`);
  if (args.dryRun) console.log('[dry-run] 実 DB 変更なし');
  return { totalHeaders, totalLines, dirtyMonths: [...allDirtyMonths], blocked, inventory: inv };
}

async function main() {
  const args = parseArgs();
  const runId = `settlement-${Date.now()}`;
  const src = SOURCES[args.source];
  console.log(`[settlements] run_id=${runId}, dry-run=${args.dryRun}, source=${args.source} (${src.reportType})`);

  await initDB();
  const db = getDB();

  // --report-id の回は一覧を取らない = 専用の接続も作らない
  const { blocked } = await runSettlementFetch(args, { db, sp: getClient(), inventorySp: args.reportId ? null : getInventoryClient(), runId });
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
