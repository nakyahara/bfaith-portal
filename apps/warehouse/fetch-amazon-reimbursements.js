/**
 * fetch-amazon-reimbursements.js — Amazon FBA の補てん (Reimbursements) の明細を取る (AI の分析用・2026-10-22)
 *
 * レポート: GET_FBA_REIMBURSEMENTS_DATA (TSV)。ふだんは JST の昨日まで直近 60 日を毎朝取り直す
 *   列 (公式): approval-date, reimbursement-id, case-id, amazon-order-id, reason, sku, fnsku, asin, product-name, condition,
 *              currency-unit, amount-per-unit, amount-total, quantity-reimbursed-cash, quantity-reimbursed-inventory,
 *              quantity-reimbursed-total, original-reimbursement-id, original-reimbursement-type
 *   🚨 必須の列 (approval-date / reimbursement-id) が無ければ見出しを出して失敗
 *
 * 保存: amazon_fba_reimbursements (UPSERT・この取得が作る)
 *   鍵 = (reimbursement_id, seller_sku, fnsku, occurrence)。同じ補てん ID に SKU が複数ある / 鍵が全部同じ行が 2 回以上 (occurrence で分ける)
 *   金額・数量は取り消し (reversal) で負になりうる = 負を許す。空は null (0 にしない)
 *   行ごとに世代を持ち、古い取得では上書きしない。取得の記録 = sp_report_fetch_days
 *   レポートが CANCELLED = 期間に補てんが無いと読み、0 行として記録だけ残す (UPSERT = 何も消さない・最後の行に ⚠️)
 *
 * ここでは保存まで (紛失・破損で補てんされていない在庫の検出は次の段。在庫の台帳 fetch-amazon-ledger.js と突き合わせる)
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-reimbursements.js                # 直近 60 日
 *   node apps/warehouse/fetch-amazon-reimbursements.js --from 2025-10-01 --to 2026-09-30
 *   node apps/warehouse/fetch-amazon-reimbursements.js --dry-run      # 期間を出すだけ (API を呼ばない)
 *   node apps/warehouse/fetch-amazon-reimbursements.js --check        # 取って検査し、要約と見本を出す (DB に書かない)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import {
  fetchSpReportOrNoData, createSpClient, isDirectRun, parseCommonArgs, jstStart, jstEnd, headerResolver,
  numOrNull, strOrNull, looseDateToJst, assignOccurrence, saveUpsertRows,
} from './sp-api-report.js';

export const REPORT_TYPE = 'GET_FBA_REIMBURSEMENTS_DATA';
const DEFAULT_DAYS = 60;

export function ensureReimbursementsTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_fba_reimbursements (
    reimbursement_id              TEXT NOT NULL,
    seller_sku                    TEXT NOT NULL DEFAULT '',
    fnsku                         TEXT NOT NULL DEFAULT '',
    occurrence                    INTEGER NOT NULL,
    approval_date                 TEXT NOT NULL,   -- レポートの approval-date のまま
    approval_date_jst             TEXT NOT NULL,
    case_id                       TEXT,
    amazon_order_id               TEXT,
    reason                        TEXT,            -- Lost_Warehouse / Damaged_Warehouse / CustomerReturn など
    asin                          TEXT,
    product_name                  TEXT,
    condition                     TEXT,
    currency_unit                 TEXT,
    amount_per_unit               REAL,
    amount_total                  REAL,
    quantity_reimbursed_cash      INTEGER,
    quantity_reimbursed_inventory INTEGER,
    quantity_reimbursed_total     INTEGER,
    original_reimbursement_id     TEXT,
    original_reimbursement_type   TEXT,
    report_id                     TEXT NOT NULL,
    generation                    INTEGER NOT NULL,
    first_seen_at                 TEXT NOT NULL,
    fetched_at                    TEXT NOT NULL,
    PRIMARY KEY (reimbursement_id, seller_sku, fnsku, occurrence)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_afrb_date ON amazon_fba_reimbursements(approval_date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_afrb_sku ON amazon_fba_reimbursements(seller_sku)');
}

const KEY_COLS = ['reimbursement_id', 'seller_sku', 'fnsku', 'occurrence'];
const COLS = ['approval_date', 'approval_date_jst', 'case_id', 'amazon_order_id', 'reason', 'asin', 'product_name', 'condition', 'currency_unit',
  'amount_per_unit', 'amount_total', 'quantity_reimbursed_cash', 'quantity_reimbursed_inventory', 'quantity_reimbursed_total',
  'original_reimbursement_id', 'original_reimbursement_type'];

export function parseReimbursementRows({ headers, rows, noData = false }) {
  if (noData) return [];   // CANCELLED = データなし (sp-api-report.js の fetchSpReportOrNoData)
  const h = headerResolver(headers);
  const col = {
    approvalDate: h('approval-date'), id: h('reimbursement-id'), caseId: h('case-id'), orderId: h('amazon-order-id'), reason: h('reason'),
    sku: h('sku'), fnsku: h('fnsku'), asin: h('asin'), productName: h('product-name'), condition: h('condition'), currency: h('currency-unit'),
    perUnit: h('amount-per-unit'), total: h('amount-total'), qCash: h('quantity-reimbursed-cash'), qInv: h('quantity-reimbursed-inventory'),
    qTotal: h('quantity-reimbursed-total'), origId: h('original-reimbursement-id'), origType: h('original-reimbursement-type'),
  };
  const missing = ['approvalDate', 'id'].filter((k) => !col[k]);
  if (missing.length) throw new Error(`補てんのレポートに必須の列が無い (${missing.join(', ')})。見出し: ${JSON.stringify(headers).slice(0, 400)}`);
  const get = (r, k) => (col[k] ? strOrNull(r[col[k]]) : null);
  const signed = { min: -Infinity };
  const out = rows.map((r) => {
    const at = `行 ${r.__line}`;
    const id = get(r, 'id');
    if (!id) throw new Error(`${at} に reimbursement-id が無い`);
    const approvalDate = get(r, 'approvalDate');
    const jst = looseDateToJst(approvalDate);
    if (!jst) throw new Error(`${at} (${id}) の approval-date が日付として読めない: ${JSON.stringify(approvalDate)}`);
    return {
      reimbursement_id: id, seller_sku: get(r, 'sku') || '', fnsku: get(r, 'fnsku') || '',
      approval_date: approvalDate, approval_date_jst: jst, case_id: get(r, 'caseId'), amazon_order_id: get(r, 'orderId'), reason: get(r, 'reason'),
      asin: get(r, 'asin'), product_name: get(r, 'productName'), condition: get(r, 'condition'), currency_unit: get(r, 'currency'),
      amount_per_unit: numOrNull(get(r, 'perUnit'), `${at} の amount-per-unit`, signed),
      amount_total: numOrNull(get(r, 'total'), `${at} の amount-total`, signed),
      quantity_reimbursed_cash: numOrNull(get(r, 'qCash'), `${at} の quantity-reimbursed-cash`, { ...signed, integer: true }),
      quantity_reimbursed_inventory: numOrNull(get(r, 'qInv'), `${at} の quantity-reimbursed-inventory`, { ...signed, integer: true }),
      quantity_reimbursed_total: numOrNull(get(r, 'qTotal'), `${at} の quantity-reimbursed-total`, { ...signed, integer: true }),
      original_reimbursement_id: get(r, 'origId'), original_reimbursement_type: get(r, 'origType'),
    };
  });
  return assignOccurrence(out, KEY_COLS.filter((c) => c !== 'occurrence'));
}

export function saveReimbursements(db, report, { from, to, generation, reportId, now }) {
  ensureReimbursementsTable(db);
  const rows = parseReimbursementRows(report);   // 例外ならここで止まる = 何も書かない
  const r = saveUpsertRows(db, { reportType: REPORT_TYPE, table: 'amazon_fba_reimbursements', keyCols: KEY_COLS, cols: COLS, rows, dateCol: 'approval_date_jst', from, to, generation, reportId, now });
  return { ...r, rows: rows.length };
}

export function parseArgs(argv = process.argv.slice(2), nowMs = Date.now()) {
  return parseCommonArgs(argv, { defaultDays: DEFAULT_DAYS, maxDays: 730, nowMs });
}

async function main() {
  const args = parseArgs();
  console.log(`[Reimbursements] 期間: ${args.from}〜${args.to} (JST)`);
  if (args.dryRun) { console.log('[Reimbursements] --dry-run のため API を呼ばない'); return; }
  const sp = await createSpClient();
  const rep = await fetchSpReportOrNoData({ sp, reportType: REPORT_TYPE, format: 'tsv', dataStartTime: jstStart(args.from), dataEndTime: jstEnd(args.to), pollIntervalMs: 20000, maxPolls: 45 });
  if (args.check) {
    const rows = parseReimbursementRows(rep);
    console.log(`[Reimbursements] --check: 見出し ${JSON.stringify(rep.headers)}`);
    console.log(`[Reimbursements] --check: ${rows.length} 行 (金額の合計 ${rows.reduce((a, r) => a + (r.amount_total || 0), 0)})・見本 ${JSON.stringify(rows.slice(0, 2))}`);
    console.log(`[Reimbursements] ✅ 検査のみ (書かない) ${rows.length} 行`);
    return;
  }
  await initDB();
  const r = saveReimbursements(getDB(), rep, { from: args.from, to: args.to, generation: rep.generation, reportId: rep.reportId });
  console.log(`[Reimbursements] ${rep.noData ? '⚠️ レポートが CANCELLED = 期間に補てんが無いと読んだ (0 行)。' : '✅ '}${r.rows} 行を UPSERT (書いた行 ${r.upserted})・取得の記録 ${r.days} 日${r.olderDays ? ` / もっと新しい取得がある日 ${r.olderDays}` : ''} (${args.from}〜${args.to})`);
}

if (await isDirectRun(import.meta.url)) {
  main().then(() => { if (process.exitCode == null) process.exitCode = 0; }).catch((e) => { console.error('[Reimbursements] FATAL:', e?.message || e); process.exitCode = 1; });
}
