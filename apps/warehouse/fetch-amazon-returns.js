/**
 * fetch-amazon-returns.js — Amazon FBA の返品の明細と理由を取る (AI の分析とアラート用・2026-10-22)
 *
 * レポート: GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA (TSV)。ふだんは JST の昨日まで直近 60 日を毎朝取り直す (状態 status が後から変わるため)
 *   列 (公式): return-date, order-id, sku, asin, fnsku, product-name, quantity, fulfillment-center-id, detailed-disposition, reason, status,
 *              license-plate-number, customer-comments
 *   🚨 必須の列 (return-date / order-id / sku / quantity) が無ければ見出しを出して失敗 (JP の見出しが英語でない場合に気づくため)
 *
 * 保存: amazon_fba_returns (UPSERT・この取得が作る)
 *   鍵 = (order_id, seller_sku, fnsku, return_date, license_plate_number, occurrence)
 *     - 同じ注文 × SKU でも返品の時刻・LPN (返品 1 個ごとの番号) が違えば別の行
 *     - 鍵の列が全部同じ行が 1 つのレポートに 2 回以上 (LPN が空の同時刻の返品など) = occurrence (1, 2, …) で分ける (潰さない)
 *     - 状態 (status)・理由・処分 (detailed-disposition)・数量・コメントは鍵に入れない = 取り直しで新しい値に更新する
 *   行ごとに世代 (頼んだ時刻) を持ち、古い取得では上書きしない。取得の記録 = sp_report_fetch_days (頼んだ期間の日ごと)
 *   ⚠️ UPSERT なので、Amazon のレポートから消えた行は残る (返品の明細が消えることは想定していない)
 *
 * 自社発送 (FBM) の返品 GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE は作っていない (JP で取れるか・列の名前を確かめられていない)
 *
 * 🚨 読めない行 (日付・注文番号・SKU・数量) が 1 つでもあれば、そのレポートは何も書かずに失敗
 * レポートが CANCELLED = 期間に返品が無い (Amazon はデータが無いと取り消す) と読み、0 行として記録だけ残す (UPSERT = 何も消さない・最後の行に ⚠️)
 *
 * 使い方:
 *   node apps/warehouse/fetch-amazon-returns.js                 # 直近 60 日
 *   node apps/warehouse/fetch-amazon-returns.js --days 120
 *   node apps/warehouse/fetch-amazon-returns.js --from 2026-01-01 --to 2026-06-30
 *   node apps/warehouse/fetch-amazon-returns.js --dry-run       # 期間を出すだけ (API を呼ばない)
 *   node apps/warehouse/fetch-amazon-returns.js --check         # 取って検査し、要約と見本を出す (DB に書かない)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import {
  fetchSpReportOrNoData, createSpClient, isDirectRun, parseCommonArgs, jstStart, jstEnd, headerResolver,
  numOrNull, strOrNull, looseDateToJst, assignOccurrence, saveUpsertRows,
} from './sp-api-report.js';

export const REPORT_TYPE = 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA';
const DEFAULT_DAYS = 60;

export function ensureReturnsTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_fba_returns (
    order_id              TEXT NOT NULL,
    seller_sku            TEXT NOT NULL,
    fnsku                 TEXT NOT NULL DEFAULT '',
    return_date           TEXT NOT NULL,      -- レポートの return-date のまま (時差つきの ISO 8601)
    license_plate_number  TEXT NOT NULL DEFAULT '',
    occurrence            INTEGER NOT NULL,   -- 鍵の列が同じ行の通し番号 (ふつうは 1)
    return_date_jst       TEXT NOT NULL,      -- JST の日付
    asin                  TEXT,
    product_name          TEXT,
    quantity              INTEGER NOT NULL,
    fulfillment_center_id TEXT,
    detailed_disposition  TEXT,               -- SELLABLE / DEFECTIVE / CUSTOMER_DAMAGED など
    reason                TEXT,               -- 返品の理由のコード
    status                TEXT,
    customer_comments     TEXT,
    report_id             TEXT NOT NULL,
    generation            INTEGER NOT NULL,
    first_seen_at         TEXT NOT NULL,
    fetched_at            TEXT NOT NULL,
    PRIMARY KEY (order_id, seller_sku, fnsku, return_date, license_plate_number, occurrence)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_afr_date ON amazon_fba_returns(return_date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_afr_sku ON amazon_fba_returns(seller_sku, return_date_jst)');
}

const KEY_COLS = ['order_id', 'seller_sku', 'fnsku', 'return_date', 'license_plate_number', 'occurrence'];
const COLS = ['return_date_jst', 'asin', 'product_name', 'quantity', 'fulfillment_center_id', 'detailed_disposition', 'reason', 'status', 'customer_comments'];

/**
 * TSV の行を検査して保存の形にする。読めない行が 1 つでもあれば例外 (レポートごと書かない)
 */
export function parseReturnsRows({ headers, rows, noData = false }) {
  if (noData) return [];   // CANCELLED = データなし (sp-api-report.js の fetchSpReportOrNoData)
  const h = headerResolver(headers);
  const col = {
    returnDate: h('return-date'), orderId: h('order-id'), sku: h('sku'), asin: h('asin'), fnsku: h('fnsku'), productName: h('product-name'),
    quantity: h('quantity'), fc: h('fulfillment-center-id'), disposition: h('detailed-disposition'), reason: h('reason'), status: h('status'),
    lpn: h('license-plate-number'), comments: h('customer-comments'),
  };
  const missing = ['returnDate', 'orderId', 'sku', 'quantity'].filter((k) => !col[k]);
  if (missing.length) throw new Error(`返品のレポートに必須の列が無い (${missing.join(', ')})。見出し: ${JSON.stringify(headers).slice(0, 400)}`);
  const get = (r, k) => (col[k] ? strOrNull(r[col[k]]) : null);
  const out = rows.map((r) => {
    const at = `行 ${r.__line}`;
    const returnDate = get(r, 'returnDate');
    const jst = looseDateToJst(returnDate);
    if (!jst) throw new Error(`${at} の return-date が日付として読めない: ${JSON.stringify(returnDate)}`);
    const orderId = get(r, 'orderId');
    if (!orderId) throw new Error(`${at} に order-id が無い`);
    const sku = get(r, 'sku');
    if (!sku) throw new Error(`${at} (${orderId}) に sku が無い`);
    const quantity = numOrNull(get(r, 'quantity'), `${at} の quantity`, { integer: true });
    if (quantity === null) throw new Error(`${at} の quantity が無い`);
    return {
      order_id: orderId, seller_sku: sku, fnsku: get(r, 'fnsku') || '', return_date: returnDate, license_plate_number: get(r, 'lpn') || '',
      return_date_jst: jst, asin: get(r, 'asin'), product_name: get(r, 'productName'), quantity,
      fulfillment_center_id: get(r, 'fc'), detailed_disposition: get(r, 'disposition'), reason: get(r, 'reason'), status: get(r, 'status'),
      customer_comments: get(r, 'comments'),
    };
  });
  return assignOccurrence(out, KEY_COLS.filter((c) => c !== 'occurrence'));
}

export function saveReturns(db, report, { from, to, generation, reportId, now }) {
  ensureReturnsTable(db);
  const rows = parseReturnsRows(report);   // 例外ならここで止まる = 何も書かない
  const r = saveUpsertRows(db, { reportType: REPORT_TYPE, table: 'amazon_fba_returns', keyCols: KEY_COLS, cols: COLS, rows, dateCol: 'return_date_jst', from, to, generation, reportId, now });
  return { ...r, rows: rows.length };
}

export function parseArgs(argv = process.argv.slice(2), nowMs = Date.now()) {
  return parseCommonArgs(argv, { defaultDays: DEFAULT_DAYS, maxDays: 730, nowMs });
}

async function main() {
  const args = parseArgs();
  console.log(`[Returns] 期間: ${args.from}〜${args.to} (JST)`);
  if (args.dryRun) { console.log('[Returns] --dry-run のため API を呼ばない'); return; }
  const sp = await createSpClient();
  const rep = await fetchSpReportOrNoData({ sp, reportType: REPORT_TYPE, format: 'tsv', dataStartTime: jstStart(args.from), dataEndTime: jstEnd(args.to),
    pollIntervalMs: 20000, maxPolls: 45, joinExtraIntoLast: true });
  if (args.check) {
    const rows = parseReturnsRows(rep);
    console.log(`[Returns] --check: 見出し ${JSON.stringify(rep.headers)}`);
    console.log(`[Returns] --check: ${rows.length} 行 (数量の合計 ${rows.reduce((a, r) => a + r.quantity, 0)})・見本 ${JSON.stringify(rows.slice(0, 2))}`);
    console.log(`[Returns] ✅ 検査のみ (書かない) ${rows.length} 行`);
    return;
  }
  await initDB();
  const r = saveReturns(getDB(), rep, { from: args.from, to: args.to, generation: rep.generation, reportId: rep.reportId });
  console.log(`[Returns] ${rep.noData ? '⚠️ レポートが CANCELLED = 期間に返品が無いと読んだ (0 行)。' : '✅ '}${r.rows} 行を UPSERT (書いた行 ${r.upserted})・取得の記録 ${r.days} 日${r.olderDays ? ` / もっと新しい取得がある日 ${r.olderDays}` : ''} (${args.from}〜${args.to})`);
}

if (await isDirectRun(import.meta.url)) {
  main().then(() => { if (process.exitCode == null) process.exitCode = 0; }).catch((e) => { console.error('[Returns] FATAL:', e?.message || e); process.exitCode = 1; });
}
