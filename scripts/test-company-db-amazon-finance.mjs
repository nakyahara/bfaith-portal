#!/usr/bin/env node
/**
 * test-company-db-amazon-finance.mjs — Amazon 財務の送り手 (F2b-2: apps/company-db/push/amazon-finance.mjs) の試験
 *
 * 設計 = AI_reference『CompanyDB構想/12_Amazon財務のCompanyDB取込_設計_20260929.md』§4 / §6 / §7
 *   ① 二重の実装の一致 (§4.2 L1): 同じ決済の行を SQLite の build (日次の財務・月の手数料) と JS の集約に通し、PGlite の view (0043) が全列一致。
 *      場面 = 返品・カードの支払い取り消し・A-to-z・ポイント・値引きの税・送料の税・RestockingFee・補てん (注文番号なし・SKU あり)・
 *      保管料 (古い名前 / 新しい名前)・月額・長期保管料・返送料・納品不備・低在庫・手数料の調整・Easy Ship (古い月 = other_amount / 新しい月 = item_related_fee)・
 *      SKU なしで other_amount と item_related_fee の両方・unmapped (shipment_fee・知らない手数料の種類)・同じ鍵の 2 行 (出現順)・V1 と V2 の両方・
 *      BuyerRecharge (SKU あり)・預かり金・分けられない取引・Easy Ship の割り振りだけの日 × SKU・
 *      SAFE-T の補てん (取引の種類 Other + price_type)・補てんの取り消し (PAYMENT_RETRACTION_ITEMS)・SKU のある納品不備 (月の手数料へ。2026-09-30 D-63)
 *   ② 送り手 (本物の router を HTTP で): 期間は鍵を選ぶだけ・watermark・変換の版・--full (一部の行の削除・Render にだけある鍵に空の集合)・500 行超・
 *      鍵の分からない不正な行 (疑似注文を全部止める)・容量の見張り・受領記録 (2 回目に台帳を空にしない)・dry-run は台帳に書かない
 *   ③ 突き合わせ: 一致 / 差 → やり残しの月に登録・1 回目 ⚠️・2 回目 ❌ / 月の手数料の差 → 手数料のやり残し → 次の build (さかのぼる) で一致
 * 実行: node scripts/test-company-db-amazon-finance.mjs (本番には触れない。一時 DATA_DIR・PGlite)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';
import { openLedger } from '../apps/company-db/push/ledger.mjs';
import { aggregateOrderFinance, dedupSettlementRows, filterSelectedRows, feeKindOf, skuKindOf, AMAZON_FINANCE_TRANSFORM_VERSION } from '../apps/company-db/push/amazon-finance-transform.mjs';
import { backfillDocumentVersions, clearDirtyOrders, selectDocumentVersions } from '../apps/warehouse/amazon-settlement-versions.js';
import { pushAmazonFinance, reconcileAmazonFinance, readSqliteDaily, readSqliteFees, diffFinanceDaily, diffAccountFees, capacityGuard, parseArgs, sinceOf, META, FINANCE_KIND, financeKey, retryStore, SQL as PUSH_SQL }
  from '../apps/company-db/push/amazon-finance.mjs';
import { classifyAccountFee, classifySkuAccountFee } from '../apps/warehouse/amazon-account-fee-rules.js';
import { accountFeesMonthsBack, readPendingMonths, ACCOUNT_FEES_PENDING_FILE, PENDING_FILE } from '../apps/warehouse/amazon-finance-months.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// ─── 一時の warehouse.db (db.js の initDB = 本番と同じ表・view・索引) ───
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-amazon-finance-test-'));
process.env.DATA_DIR = tmpDir;
const { initDB, getDB } = await import('../apps/warehouse/db.js');
await initDB();
const wdb = getDB();
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const ymOffset = (k) => { const d = new Date(Date.UTC(nowJst.getUTCFullYear(), nowJst.getUTCMonth() + k, 1)); return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`; };
const MA = ymOffset(-2), MB = ymOffset(-1);   // 2 か月前・先月 (日付が今日より先にならない)
const OLD = ymOffset(-16);                    // 月の手数料の build の 14 か月より古い月 (③)
let seq = 0;
const OLD_INGEST = '2026-01-01 00:00:00';
/** 決済の行を 1 行入れる。金額は円 (× 100 万して micro)。doc / layer / lineNo / blk で V1・V2・同じ鍵の 2 行を作る */
function raw(o) {
  const n = ++seq;
  const date = o.date;
  const ymi = Number(date.slice(0, 7).replace('-', ''));
  const m = (v) => (v == null ? null : Math.round(v * 1e6));
  wdb.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku, seller_sku_normalized, transaction_type,
    quantity_purchased, price_type, price_amount_micro, item_related_fee_type, item_related_fee_amount_micro, promotion_type, promotion_amount_micro,
    shipment_fee_type, shipment_fee_amount_micro, misc_fee_amount_micro, other_fee_amount_micro, other_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
    VALUES (?, ?, ?, 'h', 'p', ?, ?, 'v', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'r', 'o', ?)`)
    .run(`ph-${n}`, o.blk ?? `k-${n}`, o.doc ?? 'D1', o.lineNo ?? n, o.layer ?? 'sp_api_v1', o.settlement ?? 'S1', `${date}T01:00:00+00:00`, `${date} 10:00:00`, date, ymi,
      o.order === undefined ? 'O1' : o.order, o.sku ?? null, o.sku === undefined ? null : o.sku, o.tt ?? 'Order',
      o.qty ?? null, o.pt ?? null, m(o.pa), o.ft ?? null, m(o.fa), o.prt ?? null, m(o.pra), o.sft ?? null, m(o.sfa), m(o.misc), m(o.ofa), m(o.oa), o.currency ?? 'JPY', o.ingested ?? OLD_INGEST);
  return n;
}
const d = (ym, day) => `${ym}-${String(day).padStart(2, '0')}`;

// ── 場面 ──
// O1 (MB 5 日): 売上・税・送料・送料の税・手数料・値引き (本体・送料・税の分)・unmapped (shipment_fee / 知らない手数料)・Easy Ship (新しい月 = item_related_fee)
const O1 = { order: 'O1', sku: 'sku-a', date: d(MB, 5) };
raw({ ...O1, qty: 1 }); raw({ ...O1, pt: 'Principal', pa: 1000 }); raw({ ...O1, pt: 'Tax', pa: 100 }); raw({ ...O1, pt: 'Shipping', pa: 300 }); raw({ ...O1, pt: 'ShippingTax', pa: 30 });
raw({ ...O1, ft: 'Commission', fa: -110 }); raw({ ...O1, ft: 'FBAPerUnitFulfillmentFee', fa: -330 });
raw({ ...O1, prt: 'Principal', pra: -200 }); raw({ ...O1, prt: 'TaxDiscount', pra: -20 }); raw({ ...O1, prt: 'Shipping', pra: -50 });
raw({ ...O1, sft: 'FBA transportation fee', sfa: -70 }); raw({ ...O1, ft: 'VariableClosingFee', fa: -10 });
raw({ order: 'O1', date: d(MB, 6), tt: 'Amazon Easy Ship Charges', ft: 'EasyShipFee', fa: -600 });
// O2 (sku-b): MB 5 日に 2 個・10 日に返品・12 日にカードの支払い取り消し・14 日に A-to-z
const O2 = { order: 'O2', sku: 'sku-b', date: d(MB, 5) };
raw({ ...O2, qty: 2 }); raw({ ...O2, pt: 'Principal', pa: 2000 }); raw({ ...O2, pt: 'Tax', pa: 200 }); raw({ ...O2, pt: 'Shipping', pa: 300 });
raw({ ...O2, ft: 'Commission', fa: -220 }); raw({ ...O2, ft: 'FBAPerUnitFulfillmentFee', fa: -660 }); raw({ ...O2, ft: 'ShippingChargeback', fa: -300 });
raw({ ...O2, prt: 'Shipping', pra: -300 }); raw({ ...O2, prt: 'TaxDiscount', pra: -30 });
const R2 = { ...O2, tt: 'Refund', date: d(MB, 10) };
raw({ ...R2, pt: 'Principal', pa: -1000 }); raw({ ...R2, pt: 'Tax', pa: -100 }); raw({ ...R2, pt: 'Shipping', pa: -300 }); raw({ ...R2, pt: 'RestockingFee', pa: 50 });
raw({ ...R2, ft: 'Commission', fa: 110 }); raw({ ...R2, ft: 'RefundCommission', fa: -22 }); raw({ ...R2, ft: 'ShippingChargeback', fa: 300 });
raw({ ...R2, prt: 'Shipping', pra: 300 }); raw({ ...R2, prt: 'TaxDiscount', pra: 30 });
const C2 = { ...O2, tt: 'Chargeback Refund', date: d(MB, 12) };
raw({ ...C2, pt: 'Principal', pa: -1000 }); raw({ ...C2, pt: 'Tax', pa: -100 }); raw({ ...C2, ft: 'Commission', fa: 110 }); raw({ ...C2, ft: 'RefundCommission', fa: -22 });
const Z2 = { ...O2, tt: 'A-to-z Guarantee Refund', date: d(MB, 14) };
raw({ ...Z2, pt: 'Principal', pa: -1000 }); raw({ ...Z2, pt: 'Shipping', pa: -300 });
// O3 (sku-c): ポイント (MB 5 日に付けて 10 日に返品で戻る)
const O3 = { order: 'O3', sku: 'sku-c', date: d(MB, 5) };
raw({ ...O3, qty: 1 }); raw({ ...O3, pt: 'Principal', pa: 1000 }); raw({ ...O3, ft: 'PointsGranted', fa: -30 });
raw({ ...O3, tt: 'Refund', date: d(MB, 10), ft: 'PointsReturned', fa: 10 });
// O4 (sku-e): MA 20 日に売上・MB 2 日に Easy Ship の料金 (古い月の形 = other_amount) → SQLite は MB 2 日 × sku-e に割り振りだけの行
raw({ order: 'O4', sku: 'sku-e', date: d(MA, 20), qty: 1 }); raw({ order: 'O4', sku: 'sku-e', date: d(MA, 20), pt: 'Principal', pa: 1500 });
raw({ order: 'O4', date: d(MB, 2), tt: 'Amazon Easy Ship Charges', oa: -500 });
// O5: SKU なしの行で other_amount と item_related_fee の両方に金額
raw({ order: 'O5', sku: 'sku-f', date: d(MB, 8), qty: 1 }); raw({ order: 'O5', sku: 'sku-f', date: d(MB, 8), pt: 'Principal', pa: 800 });
raw({ order: 'O5', date: d(MB, 8), tt: 'Amazon Easy Ship Charges', oa: -100, ft: 'EasyShipFee', fa: -50 });
// O6: 同じ鍵の 2 行 (同じ文書・行番号違い = 本物の 2 行) + V1 と V2 (別の文書・同じ鍵 = 1 行)
const O6 = { order: 'O6', sku: 'sku-g', date: d(MB, 9) };
raw({ ...O6, qty: 1, blk: 'dup-q', lineNo: 1001 }); raw({ ...O6, qty: 1, blk: 'dup-q', lineNo: 1002 });
raw({ ...O6, pt: 'Principal', pa: 700, blk: 'dup-p', lineNo: 1003 }); raw({ ...O6, pt: 'Principal', pa: 700, blk: 'dup-p', lineNo: 1004 });
// 🆕 2026-10-01 (D-66): 1 つの決済の文書 = その決済の全部の行 = V1 と V2 は別の決済 S-V12 の 2 つの文書 (決済ごとに採る版は 1 つ)
raw({ ...O6, pt: 'Principal', pa: 900, blk: 'v12', doc: 'D-V1', layer: 'sp_api_v1', lineNo: 5, settlement: 'S-V12' }); raw({ ...O6, pt: 'Principal', pa: 900, blk: 'v12', doc: 'D-V2', layer: 'sp_api_v2', lineNo: 7, ingested: '2026-02-01 00:00:00', settlement: 'S-V12' });
// O7: BuyerRecharge (SKU あり = 日次の財務は除く)
raw({ order: 'O7', sku: 'sku-h', date: d(MB, 11), tt: 'BuyerRecharge', pt: 'Principal', pa: -100 });
// 注文番号なし・SKU あり = 補てん (MB 7 日)
for (const [tt, oa] of [['WAREHOUSE_DAMAGE', 500], ['WAREHOUSE_LOST', 300], ['SAFE-T Reimbursement', 200], ['REVERSAL_REIMBURSEMENT', -100], ['Fee Adjustment', 40]]) raw({ order: null, sku: 'sku-d', date: d(MB, 7), tt, oa });
// 注文番号なし・SKU なし = 月の手数料 (MA と MB)・手数料に入れない・分けられない
const NS = (date, tt, x) => raw({ order: null, date, tt, ...x });
NS(d(MA, 7), 'Storage Fee', { oa: -5000 }); NS(d(MB, 7), 'FBA Inventory Storage Fee', { oa: -6000 }); NS(d(MB, 7), 'StorageRenewalBilling', { oa: -1000 });
NS(d(MB, 15), 'FBA Long Term Storage Fee', { oa: -1200 }); NS(d(MB, 1), 'Subscription Fee', { oa: -4900 }); NS(d(MB, 16), 'FBA Removal Order: Return Fee', { oa: -300 });
NS(d(MB, 16), 'Inbound Defect Fee - Unplanned Service', { oa: -150 }); NS(d(MB, 17), 'FBA Inventory Fee - LowInventoryLevel', { oa: -50 });
NS(d(MB, 18), 'Fee Adjustment', { oa: 80 }); NS(d(MB, 18), 'Overpaid Fees Adjustment', { oa: 20 });
NS(d(MB, 19), 'Current Reserve Amount', { oa: -10000 }); NS(d(MB, 19), 'Previous Reserve Amount Balance', { oa: 10000 }); NS(d(MB, 19), 'Mystery Fee', { oa: -77 });
NS(d(MA, 7), 'StorageRenewalBilling', { oa: -900, blk: 'v12s', doc: 'D-V1', lineNo: 3, settlement: 'S-V12' }); NS(d(MA, 7), 'StorageRenewalBilling', { oa: -900, blk: 'v12s', doc: 'D-V2', layer: 'sp_api_v2', lineNo: 4, settlement: 'S-V12' });
// 🆕 2026-09-30 (D-63): SKU のある行の「行き先の無い金額」を種類ごとに分ける
//   O13 (sku-v): MB 13 に売上・MB 14 に SAFE-T の補てん (取引の種類 Other・price_type SAFE-T Reimbursement) → safe_t と 補てんの取り消し (PAYMENT_RETRACTION_ITEMS) → reversal_reimbursement
//   SKU のある納品不備 → 月の手数料の inbound_defect (MB 16 = SKU の無い納品不備 −150 と同じ疑似注文の同じ行)・日次の財務には入らない
//   sku-w (MB 17) は納品不備だけ = 日 × SKU の行ができない (小文字の名前も = 前方一致は大文字小文字を問わない)
raw({ order: 'O13', sku: 'sku-v', date: d(MB, 13), qty: 1 }); raw({ order: 'O13', sku: 'sku-v', date: d(MB, 13), pt: 'Principal', pa: 1000 });
raw({ order: 'O13', sku: 'sku-v', date: d(MB, 14), tt: 'Other', pt: 'SAFE-T Reimbursement', oa: 634 });
raw({ order: 'O13', sku: 'sku-v', date: d(MB, 14), tt: 'PAYMENT_RETRACTION_ITEMS', oa: -120 });
raw({ order: null, sku: 'sku-v', date: d(MB, 16), tt: 'Inbound Defect Fee - Missing label', oa: -200 });
raw({ order: null, sku: 'sku-w', date: d(MB, 17), tt: 'Inbound Defect Fee - Barcode cannot be scanned', oa: -90 });
raw({ order: null, sku: 'sku-w', date: d(MB, 17), tt: 'inbound defect fee - x', oa: -10 });
// 🆕 2026-09-30 (D7b-1a・0047): 分けられない部品の数
//   O14 (sku-u14・MB 23): 売上 500・Other (price_type Something) の other_amount +100 と −100 (相殺して 0)・misc_fee +30・MFNPostageFee −40 (other_fee)
//     → SKU の行の分けられない部品 4 (+100 / −100 / +30 / −40)・符号つき −10・絶対値 270・other_amount は 0
//   月の手数料の行に手数料の材料でない部品: Subscription Fee (MB 23) の other_amount −100 (材料) + misc_fee +3 (分けられない 1)
//   手数料に入れない行の部品は数えない: Goodwill Concession (MB 23・SKU なし) の misc_fee +9 = not_account_fee (損益の外)
raw({ order: 'O14', sku: 'sku-u14', date: d(MB, 23), qty: 1 }); raw({ order: 'O14', sku: 'sku-u14', date: d(MB, 23), pt: 'Principal', pa: 500 });
raw({ order: 'O14', sku: 'sku-u14', date: d(MB, 23), tt: 'Other', pt: 'Something', oa: 100 });
raw({ order: 'O14', sku: 'sku-u14', date: d(MB, 23), tt: 'Other', pt: 'Something', oa: -100 });
raw({ order: 'O14', sku: 'sku-u14', date: d(MB, 23), misc: 30 });
raw({ order: 'O14', sku: 'sku-u14', date: d(MB, 23), ft: 'MFNPostageFee', fa: -40 });
NS(d(MB, 23), 'Subscription Fee', { oa: -100, misc: 3 });
NS(d(MB, 23), 'Goodwill Concession', { misc: 9 });
// 🆕 2026-10-01 (D-66): 決済 S-DIFF に文書が 2 つ・中身が違う (旧い V1 には相殺の +100 / −100 (Other / Something) がある・新しい V2 には無い)。
//   採る版 = 新しい V2 = build も変換も相殺の行を数えない (前は文書をまたいで行ごとに選び、旧い文書にだけある行が残った = 分けられない部品 2 が出た)
const O20 = { order: 'O20', sku: 'sku-o20', date: d(MB, 27), settlement: 'S-DIFF' };
raw({ ...O20, qty: 1, blk: 'o20q', doc: 'D-OLD', lineNo: 1 }); raw({ ...O20, pt: 'Principal', pa: 500, blk: 'o20p', doc: 'D-OLD', lineNo: 2 });
raw({ ...O20, tt: 'Other', pt: 'Something', oa: 100, blk: 'o20x', doc: 'D-OLD', lineNo: 3 }); raw({ ...O20, tt: 'Other', pt: 'Something', oa: -100, blk: 'o20y', doc: 'D-OLD', lineNo: 4 });
raw({ ...O20, qty: 1, blk: 'o20q', doc: 'D-NEW', layer: 'sp_api_v2', lineNo: 1, ingested: '2026-01-15 00:00:00' }); raw({ ...O20, pt: 'Principal', pa: 500, blk: 'o20p', doc: 'D-NEW', layer: 'sp_api_v2', lineNo: 2, ingested: '2026-01-15 00:00:00' });

// 🆕 #1567 Codex R2 High 1: 決済 S-H0 に「古い版 = 見出しつき (detail_valid 1・見出し 1 行)」と「新しい版 = 見出し無し (過去の backfill・detail_valid 1・見出し 0 行)」。
//   採る版 = 見出しのある古い版 (SQLite の view)。送り手の版の SQL に header_count が無いと新しい版 (中身が違う = 売上 650) を採って Render と SQLite がずれた
const O21 = { order: 'O21', sku: 'sku-o21', date: d(MB, 25), settlement: 'S-H0' };
raw({ ...O21, qty: 1, blk: 'o21q', doc: 'D-H0-OLD', lineNo: 1 }); raw({ ...O21, pt: 'Principal', pa: 600, blk: 'o21p', doc: 'D-H0-OLD', lineNo: 2 });
wdb.prepare(`INSERT INTO raw_amazon_settlement_headers (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
  source_settlement_id, settlement_start_date, settlement_end_date, deposit_date, total_amount_micro, currency, ingest_run_id, observed_at, ingested_at)
  VALUES ('ph-h0-head', 'h0-head', 'D-H0-OLD', 'h', 'p', 0, 'sp_api_v1', 'v', 'S-H0', ?, ?, ?, ?, 'JPY', 'r', 'o', ?)`)
  .run(`${d(MB, 20)} 00:00:00 UTC`, `${d(MB, 28)} 00:00:00 UTC`, `${d(MB, 28)} 00:00:00 UTC`, 600 * 1e6, OLD_INGEST);
raw({ ...O21, qty: 1, blk: 'o21q', doc: 'D-H0-NEW', layer: 'sp_api_v2', lineNo: 1, ingested: '2026-01-20 00:00:00' }); raw({ ...O21, pt: 'Principal', pa: 650, blk: 'o21p', doc: 'D-H0-NEW', layer: 'sp_api_v2', lineNo: 2, ingested: '2026-01-20 00:00:00' });

// 直接入れた行には文書の版が無い = 過去の行と同じ backfill で版を付けてから build・送る (どちらも版の無い行があれば止まる)
const build = (months = 14) => {
  backfillDocumentVersions(wdb);
  for (const m of [MA, MB]) execFileSync(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', m], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
  return execFileSync(process.execPath, ['apps/warehouse/rebuild-amazon-account-fees.js', '--data-dir', tmpDir, '--months', String(months)], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
};
const buildFees = (fromMonth) => execFileSync(process.execPath, ['apps/warehouse/rebuild-amazon-account-fees.js', '--data-dir', tmpDir, '--from-month', fromMonth], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
build();

// ─── PGlite + 本物の router (HTTP) ───
const pg = new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: quiet });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
__setPgClientFactory(async () => ({
  query: async (text, params) => {
    if (params && params.length) return pg.query(text, params);
    if (text.includes(';')) { await pg.exec(text); return { rows: [] }; }
    return pg.query(text);
  },
  end: async () => {},
}));
const app = express();
app.use('/apps/company-db/sync', requireSyncKey);
app.use('/apps/company-db/sync', companyDbRouter);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const BASE = `http://127.0.0.1:${server.address().port}/apps/company-db/sync`;
process.env.MIRROR_SYNC_KEY = 'k';
process.env.COMPANY_DB_URL = 'postgres://pglite';
const reader = () => new Database(path.join(tmpDir, 'warehouse.db'), { readonly: true });
const BIG = { limitBytes: 1e15, rowBytes: 1000, replaceFactor: 2, walAllowanceBytes: 0, marginBytes: 0, orderBytes: 300 };
// dirty = 読み直す注文の記録を消す口 (coordinator が渡すのと同じ = 読み取りの版 R 以下だけ)
const pushClose = async (ledger, x) => { backfillDocumentVersions(wdb); const w = reader(); try { return await pushAmazonFinance({ warehouse: w, ledger, base: BASE, syncKey: 'k', log: quiet, sleep: async () => {}, capacity: BIG,
  dirty: { clear: (nos, rev) => clearDirtyOrders(wdb, nos, rev) }, ...x }); } finally { w.close(); } };
const newLedger = () => { const l = openLedger(tmpDir, { memory: true, kind: FINANCE_KIND }); l.markInitialized(); return l; };
const renderDaily = async (from, to) => (await all(`select economic_date_jst::text as date_jst, seller_sku, units_ordered, units_refunded_customer, units_marketplace_guarantee, units_a_to_z_refund, units_net_sold,
  sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy, commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy, shipping_chargeback_jpy, giftwrap_chargeback_jpy,
  promotion_jpy, promotion_tax_jpy, points_jpy, warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy, refund_principal_jpy, reversal_reimbursement_jpy, misc_fee_jpy, other_fee_jpy, other_amount_jpy, profit_before_cogs_jpy
  from mart.v_finance_daily where economic_date_jst between $1::date and $2::date`, [from, to])).map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, k === 'date_jst' || k === 'seller_sku' ? v : Number(v)])));
const renderFees = async (months) => (await all(`select to_char(month_start_jst, 'YYYY-MM') as month, fee_type, amount_jpy, row_count from mart.v_finance_account_fees_monthly`))
  .filter((r) => months.includes(r.month)).map((r) => ({ ...r, amount_jpy: Number(r.amount_jpy), row_count: Number(r.row_count) }));
const monthEnd = (ym) => { const [y, m] = ym.split('-').map(Number); return new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10); };

console.log('① 二重の実装の一致 (SQLite の build ⇄ JS の集約 → PGlite の view)');
const L0 = newLedger();
const r0 = await pushClose(L0, { mode: 'range', from: `${MA}-01`, to: monthEnd(MB) });
await t('全部の注文・疑似注文を送れた (整形できない 0・failed 0)', async () => {
  assert.equal(r0.ok, true, JSON.stringify({ te: r0.transformErrors, f: r0.failed, e: r0.error }));
  assert.ok(r0.applied >= 10, `applied ${r0.applied}`);
});
await t('日 × SKU: mart.v_finance_daily = f_amazon_finance_sku_daily_v1 (Easy Ship の割り振りだけの行を除く・全列・鍵の和集合で差 0)', async () => {
  const w = reader();
  try {
    const local = readSqliteDaily(w, `${MA}-01`, monthEnd(MB));
    const remote = await renderDaily(`${MA}-01`, monthEnd(MB));
    const diff = diffFinanceDaily(local, remote);
    assert.deepEqual(diff, [], JSON.stringify(diff.slice(0, 5)));
    assert.ok(local.length >= 10, `SQLite の行 ${local.length}`);
    // 割り振りだけの行 (O4 の MB 2 日 × sku-e) は SQLite にあって比べる対象から外れる
    const alloc = w.prepare(`select source_layer_summary s from f_amazon_finance_sku_daily_v1 where date_jst = ? and seller_sku = 'sku-e'`).get(d(MB, 2));
    assert.equal(alloc && alloc.s, 'easy_ship_alloc');
    assert.ok(!local.some((r) => r.date_jst === d(MB, 2) && r.seller_sku === 'sku-e'));
  } finally { w.close(); }
});
await t('値の芯 (手で計算): 返品数 (単価で割る)・支払い取り消しは返品数に入れない・A-to-z・同じ鍵の 2 行・V1/V2 は 1 行・BuyerRecharge は日次に無い', async () => {
  const rows = await renderDaily(`${MA}-01`, monthEnd(MB));
  const at = (date, sku) => rows.find((r) => r.date_jst === date && r.seller_sku === sku);
  const b10 = at(d(MB, 10), 'sku-b'), b12 = at(d(MB, 12), 'sku-b'), b14 = at(d(MB, 14), 'sku-b');
  assert.equal(b10.units_refunded_customer, 1); assert.equal(b10.refund_principal_jpy, 1000 + 300 - 50);
  assert.equal(b12.units_refunded_customer, 0); assert.equal(b12.refund_principal_jpy, 1000);
  assert.equal(b14.units_a_to_z_refund, 1); assert.equal(b14.refund_principal_jpy, 1300);
  const g = at(d(MB, 9), 'sku-g');
  assert.equal(g.units_ordered, 2); assert.equal(g.sales_principal_jpy, 700 * 2 + 900);
  assert.equal(at(d(MB, 11), 'sku-h'), undefined);
  const dmg = at(d(MB, 7), 'sku-d');
  assert.deepEqual([dmg.warehouse_damage_jpy, dmg.warehouse_lost_jpy, dmg.safe_t_jpy, dmg.reversal_reimbursement_jpy], [500, 300, 200, -60]);
  assert.equal(at(d(MB, 5), 'sku-c').points_jpy, 30);
});
await t('行き先の無い金額を種類ごとに (2026-09-30 D-63): Other の SAFE-T → safe_t・取り消し → reversal・利益に入る / SKU のある納品不備は日次に無く月の手数料に', async () => {
  const rows = await renderDaily(`${MA}-01`, monthEnd(MB));
  const at = (date, sku) => rows.find((r) => r.date_jst === date && r.seller_sku === sku);
  const v14 = at(d(MB, 14), 'sku-v');
  assert.deepEqual([v14.safe_t_jpy, v14.reversal_reimbursement_jpy, v14.other_amount_jpy, v14.profit_before_cogs_jpy], [634, -120, 0, 514]);
  assert.equal(at(d(MB, 16), 'sku-v'), undefined);   // 納品不備だけの日 × SKU は無い
  assert.equal(at(d(MB, 17), 'sku-w'), undefined);
  // SKU のある行の other_amount = 0 (この試験の決済の行には、ほかに other_amount を持つ SKU のある行は無い)
  assert.equal(rows.reduce((s, r) => s + r.other_amount_jpy, 0), 0);
  const w = reader();
  try {
    // SQLite の日次の財務も同じ (利益 = 補てん 634 − 取り消し 120・原価の登録なし)
    const s = w.prepare(`select safe_t_jpy s, reversal_reimbursement_jpy r, other_amount_jpy o, profit_amount p from f_amazon_finance_sku_daily_v1 where date_jst = ? and seller_sku = 'sku-v'`).get(d(MB, 14));
    assert.deepEqual([s.s, s.r, s.o, s.p], [634, -120, 0, 514]);
    assert.equal(w.prepare(`select count(*) n from f_amazon_finance_sku_daily_v1 where seller_sku in ('sku-v', 'sku-w') and date_jst in (?, ?)`).get(d(MB, 16), d(MB, 17)).n, 0);
    const idf = w.prepare(`select amount_jpy a, row_count n from f_amazon_account_fees_monthly_v1 where month_start_jst = ? and fee_type = 'inbound_defect'`).get(`${MB}-01`);
    assert.deepEqual([idf.a, idf.n], [-150 - 200 - 90 - 10, 4]);
  } finally { w.close(); }
  // Company DB: SKU は '-'・line_kind = inbound_defect (SKU の無い納品不備と同じ行)
  const cdb = await all(`select economic_date_jst::text as d, seller_sku, line_kind, account_fee_amount_jpy::int as a, source_lines from core.order_finance_daily
    where line_kind = 'inbound_defect' order by 1`);
  assert.deepEqual(cdb.map((x) => [x.d, x.seller_sku, x.a, x.source_lines]), [[d(MB, 16), '-', -350, 2], [d(MB, 17), '-', -100, 2]]);
});
await t('🚨 D-66: 文書が 2 つで中身が違う決済 (S-DIFF) = 新しい版だけ (build も変換も相殺の行を数えない・分けられない部品 0)', async () => {
  const u = await one(`select sales_principal_jpy::int p, other_amount_jpy::int oa, unclassified_component_count c from core.order_finance_daily where mall_order_no = 'O20' and line_kind = 'sku'`);
  assert.deepEqual([u.p, u.oa, u.c], [500, 0, 0]);
  const w = reader();
  try {
    const s2 = w.prepare(`select sales_principal_jpy p, other_amount_jpy o, units_ordered q from f_amazon_finance_sku_daily_v1 where seller_sku = 'sku-o20' and date_jst = ?`).get(d(MB, 27));
    assert.deepEqual([s2.p, s2.o, s2.q], [500, 0, 1]);
  } finally { w.close(); }
});
await t('🚨 #1567 Codex R2 High 1: 古い版 = 見出しつき・新しい版 = 見出し無し (どちらも detail_valid 1) = 送り手の実際の版の SQL で採る版 = SQLite の view (古い版)・Render と SQLite の売上が同じ 600', async () => {
  const w = reader();
  try {
    const vs = w.prepare(`select settlement_id, seq, header_count, detail_valid, ingested_at from amazon_settlement_document_versions where settlement_id = 'S-H0' order by seq`).all();
    assert.deepEqual(vs.map((v) => [v.header_count, v.detail_valid]), [[1, 1], [0, 1]], '前提: 古い版 = 見出し 1 行・新しい版 = 見出し 0 行・どちらも detail_valid 1');
    const js = selectDocumentVersions(w.prepare(PUSH_SQL.versions).all());   // 送り手の実際の SQL
    const view = w.prepare(`select settlement_id, document_version_seq from v_amazon_settlement_selected_documents`).all();
    assert.equal(js.size, view.length);
    for (const r of view) assert.equal(js.get(r.settlement_id)?.seq, r.document_version_seq, `決済 ${r.settlement_id}: 送り手 #${js.get(r.settlement_id)?.seq} / view #${r.document_version_seq}`);
    assert.equal(js.get('S-H0').seq, vs[0].seq, 'S-H0 = 見出しのある古い版');
    const s = w.prepare(`select sales_principal_jpy p from f_amazon_finance_sku_daily_v1 where seller_sku = 'sku-o21' and date_jst = ?`).get(d(MB, 25));
    assert.equal(s.p, 600);
  } finally { w.close(); }
  const u = await one(`select sales_principal_jpy::int p from core.order_finance_daily where mall_order_no = 'O21' and line_kind = 'sku'`);
  assert.equal(u.p, 600, 'Render も古い版 (見出しつき) の 600');
});
await t('月 × 手数料: mart.v_finance_account_fees_monthly = f_amazon_account_fees_monthly_v1 (金額・行数)', async () => {
  const w = reader();
  try {
    const local = readSqliteFees(w, [MA, MB]);
    const remote = await renderFees([MA, MB]);
    assert.deepEqual(diffAccountFees(local, remote), []);
    const kinds = new Set(remote.map((r) => r.fee_type));
    for (const k of ['storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee']) assert.ok(kinds.has(k), `種類 ${k} が無い`);
    const es = remote.find((r) => r.month === MB && r.fee_type === 'easy_ship');
    assert.equal(es.amount_jpy, -600 - 500 - 150);   // 新しい月の形 (item_related_fee)・古い月の形 (other_amount)・両方
    const ltsMA = remote.find((r) => r.month === MA && r.fee_type === 'long_term_storage');
    assert.deepEqual([ltsMA.amount_jpy, ltsMA.row_count], [-900, 1]);   // V1 と V2 は 1 行
  } finally { w.close(); }
});
await t('net = 決済の行の金額の全部 (重複除去の後)・unmapped と手数料に入れない行も Company DB に残る', async () => {
  const net = Number((await one(`select sum(net_jpy) as n from core.order_finance_daily`)).n);
  const w = reader();
  try {
    const s = w.prepare(`select sum(coalesce(price_amount_micro,0) + coalesce(item_related_fee_amount_micro,0) + coalesce(promotion_amount_micro,0) + coalesce(shipment_fee_amount_micro,0)
      + coalesce(order_fee_amount_micro,0) + coalesce(misc_fee_amount_micro,0) + coalesce(other_fee_amount_micro,0) + coalesce(direct_payment_amount_micro,0) + coalesce(other_amount_micro,0)) / 1000000 as n
      from v_amazon_settlement_unified`).get().n;
    assert.equal(net, s);
  } finally { w.close(); }
  const u = await one(`select sum(unmapped_jpy) as u from core.order_finance_daily where mall_order_no = 'O1'`);
  assert.equal(Number(u.u), -80);
  const kinds = (await all(`select line_kind, seller_sku from core.order_finance_daily where mall_order_no = 'O7'`));
  assert.deepEqual(kinds.map((k) => [k.line_kind, k.seller_sku]), [['not_account_fee', '-']]);
  const unk = await one(`select count(*)::int as n from core.order_finance_daily where line_kind = 'unknown'`);
  assert.equal(unk.n, 1);
});
await t('拾われない金額を数える (shipment_fee と知らない手数料・打ち消して 0 でも行で数える)', async () => {
  assert.equal(r0.finance.unmapped.rows, 2);
  assert.deepEqual(Object.keys(r0.finance.unmapped.columns).sort(), ['item_related_fee:VariableClosingFee', 'shipment_fee']);
  const rows = [{ id: 1, source_settlement_id: 'S', business_line_key: 'a', source_document_id: 'D', source_line_no: 1, source_layer: 'sp_api_v1', ingested_at: OLD_INGEST, posted_date_utc: 'x',
    economic_date: '2026-03-01', amazon_order_id: 'X1', seller_sku_normalized: 's', transaction_type: 'Order', currency: 'JPY', shipment_fee_amount_micro: 5000000n },
  { id: 2, source_settlement_id: 'S', business_line_key: 'b', source_document_id: 'D', source_line_no: 2, source_layer: 'sp_api_v1', ingested_at: OLD_INGEST, posted_date_utc: 'x',
    economic_date: '2026-03-01', amazon_order_id: 'X1', seller_sku_normalized: 's', transaction_type: 'Order', currency: 'JPY', shipment_fee_amount_micro: -5000000n }];
  const a = aggregateOrderFinance('X1', rows);
  assert.equal(a.lines[0].unmapped_jpy, 0); assert.equal(a.stats.unmapped.rows, 2);
  assert.equal(a.lines[0].unmapped_component_count, 2);   // 0047: 打ち消して 0 でも部品の数は残る
});

console.log('分けられない部品の数 (0047・D7b-1a)');
await t('🚨 SKU の行: +100 と −100 (相殺して 0)・misc_fee・MFNPostageFee = 部品 4・符号つき −10・絶対値 270 (手で計算)。Company DB に入る', async () => {
  const u = await one(`select other_amount_jpy::int oa, misc_fee_jpy::int mf, other_fee_jpy::int ofe, unclassified_component_count c, unclassified_mapped_jpy::int m, unclassified_abs_jpy::int a,
    unmapped_component_count uc, transform_version v from core.order_finance_daily where mall_order_no = 'O14' and line_kind = 'sku'`);
  assert.deepEqual([u.oa, u.mf, u.ofe, u.c, u.m, u.a, u.uc], [0, 30, -40, 4, -10, 270, 0]);
  assert.equal(u.v, 'amazon_finance_v2');
  // SKU の行では 分けられない列の和 = 符号つきの合計 (受け口の形の確かめと同じ)
  const bad = await one(`select count(*)::int n from core.order_finance_daily where line_kind = 'sku' and unclassified_mapped_jpy <> misc_fee_jpy + other_fee_jpy + other_amount_jpy`);
  assert.equal(bad.n, 0);
  // O1 = unmapped (shipment_fee −70・知らない手数料 −10) の部品 2・分けられない部品 0
  const o1 = await one(`select unmapped_jpy::int u, unmapped_component_count uc, unclassified_component_count c from core.order_finance_daily where mall_order_no = 'O1' and line_kind = 'sku'`);
  assert.deepEqual([o1.u, o1.uc, o1.c], [-80, 2, 0]);
});
await t('月の手数料の行: 手数料の材料 (other_amount + item_related_fee) の外の部品だけ数える・手数料に入れない行 (not_account_fee) は数えない', async () => {
  const sub = await one(`select account_fee_amount_jpy::int af, net_jpy::int n, unclassified_component_count c, unclassified_mapped_jpy::int m, unclassified_abs_jpy::int a
    from core.order_finance_daily where mall_order_no = $1 and line_kind = 'subscription'`, [`-:${d(MB, 23)}`]);
  assert.deepEqual([sub.af, sub.n, sub.c, sub.m, sub.a], [-100, -97, 1, 3, 3]);
  const gw = await one(`select net_jpy::int n, unclassified_component_count c from core.order_finance_daily where mall_order_no = $1 and line_kind = 'not_account_fee'`, [`-:${d(MB, 23)}`]);
  assert.deepEqual([gw.n, gw.c], [9, 0]);
  // 保存則 (月の手数料の行): net = 手数料の材料 + 分けられない (符号つき) + unmapped。全部の月の手数料の行で
  const bad = await all(`select mall_order_no, line_kind from core.order_finance_daily
    where line_kind in ('storage','long_term_storage','removal','inbound_defect','low_inventory','subscription','easy_ship','other_account_fee')
      and net_jpy <> account_fee_amount_jpy + unclassified_mapped_jpy + unmapped_jpy`);
  assert.deepEqual(bad, []);
});
await t('🚨 mart.finance_daily_sku_range = mart.finance_daily_range と同じ期間の金額・数量の合計が一致 (粒度だけ違う)・分けられない部品は数で残る', async () => {
  const cols = ['units_ordered', 'units_refunded_customer', 'units_marketplace_guarantee', 'units_a_to_z_refund', 'units_net_sold', 'sales_principal_jpy', 'sales_shipping_jpy', 'sales_giftwrap_jpy', 'sales_tax_jpy',
    'commission_jpy', 'fba_fulfillment_jpy', 'fba_storage_jpy', 'closing_fee_jpy', 'shipping_chargeback_jpy', 'giftwrap_chargeback_jpy', 'promotion_jpy', 'promotion_tax_jpy', 'points_jpy',
    'warehouse_damage_jpy', 'warehouse_lost_jpy', 'safe_t_jpy', 'refund_principal_jpy', 'reversal_reimbursement_jpy', 'misc_fee_jpy', 'other_fee_jpy', 'other_amount_jpy', 'profit_before_cogs_jpy', 'source_lines', 'order_rows'];
  const sums = async (fn) => (await one(`select ${cols.map((c) => `sum(${c})::text as ${c}`).join(', ')}, count(*)::int as n
    from mart.${fn}(1::smallint, 'amazon', 'jp', $1::date, $2::date)`, [`${MA}-01`, monthEnd(MB)]));
  const a = await sums('finance_daily_range'), b = await sums('finance_daily_sku_range');
  for (const c of cols) assert.equal(b[c], a[c], c);
  assert.ok(a.n >= 10);
  const u = await one(`select unclassified_component_count c, unclassified_mapped_jpy::int m, unclassified_abs_jpy::int ab, other_amount_jpy::int oa, received_listing_unresolved_count r
    from mart.finance_daily_sku_range(1::smallint, 'amazon', 'jp', $1::date, $1::date) where seller_sku_norm = 'sku-u14'`, [d(MB, 23)]);
  assert.deepEqual([u.c, u.m, u.ab, u.oa, u.r], [4, -10, 270, 0, 1]);   // 出品が無い = 受け取りのとき未解決
});

console.log('集約の歯止め (純粋関数)');
const base = (x) => ({ id: 1, source_settlement_id: 'S', business_line_key: 'k', source_document_id: 'D', source_line_no: 1, source_layer: 'sp_api_v1', ingested_at: OLD_INGEST, posted_date_utc: 'x',
  economic_date: '2026-03-01', amazon_order_id: 'X1', seller_sku_normalized: 's', transaction_type: 'Order', currency: 'JPY', ...x });
await t('円未満の端数・読めない計上日・JPY 以外・空白だけの SKU・安全な整数の範囲を超える = 整形できない', async () => {
  assert.throws(() => aggregateOrderFinance('X1', [base({ price_type: 'Principal', price_amount_micro: 1500000n })]), /端数/);
  assert.throws(() => aggregateOrderFinance('X1', [base({ economic_date: '2026-02-30' })]), /計上日/);
  assert.throws(() => aggregateOrderFinance('X1', [base({ currency: 'USD' })]), /JPY/);
  assert.throws(() => aggregateOrderFinance('X1', [base({ seller_sku_normalized: '  ' })]), /空白/);
  const huge = BigInt(Number.MAX_SAFE_INTEGER) * 1000000n;
  assert.throws(() => aggregateOrderFinance('X1', [base({ price_type: 'Principal', price_amount_micro: huge }), base({ id: 2, business_line_key: 'k2', price_type: 'Principal', price_amount_micro: 1000000n })]), /安全な整数/);
});
await t('重複除去 = build と同じ (同じ版の同じ鍵の 2 行は残す・行番号が同じなら同じ出現 = 1 行) / 🆕 D-66: 決済ごとに採った版の行だけ・版が 2 つ混ざれば整形できない', async () => {
  const a = base({ id: 1, source_line_no: 1 }), b = base({ id: 2, source_line_no: 2 });
  assert.equal(dedupSettlementRows([a, b]).length, 2);
  const same = base({ id: 6, source_line_no: 1 });
  assert.equal(dedupSettlementRows([a, same]).length, 1);
  const lateSame = base({ id: 7, source_line_no: 1, ingested_at: '2026-05-01 00:00:00' });
  assert.deepEqual(dedupSettlementRows([a, lateSame]).map((r) => r.id), [7]);   // 残骸は ingested_at の新しい方 (build と同じ)
  const v1 = base({ id: 3, source_document_id: 'A', document_version_seq: 1, source_line_no: 9 }), v2 = base({ id: 4, source_document_id: 'B', document_version_seq: 2, source_line_no: 3 });
  assert.throws(() => dedupSettlementRows([v1, v2]), /版が 2 つ以上/);   // 選び忘れ = 黙って両方を足さない
  const man = base({ id: 5, source_document_id: 'M', document_version_seq: 3, source_layer: 'manual_csv' });
  assert.deepEqual(filterSelectedRows([v1, v2, man], new Map([['S', 2]])).map((r) => r.id), [4]);   // 採った版 (seq 2) だけ
  assert.deepEqual(filterSelectedRows([v1, v2], new Map([['OTHER', 1]])).map((r) => r.id), []);    // 採った版の無い決済の行は落とす
  assert.throws(() => filterSelectedRows([v1], null), /Map/);
});
await t('SKU のある行の行き先 (2026-09-30 D-63・純粋関数): Other の SAFE-T → safe_t / ほかの price_type の Other → other_amount / 取り消し → reversal / 納品不備 → SKU は - の inbound_defect', async () => {
  const M = 1000000n;
  const a = aggregateOrderFinance('X1', [
    base({ id: 1, business_line_key: 'a', transaction_type: 'Other', price_type: 'SAFE-T Reimbursement', other_amount_micro: 634n * M }),
    base({ id: 2, business_line_key: 'b', transaction_type: 'Other', price_type: 'Something', other_amount_micro: 7n * M }),
    base({ id: 3, business_line_key: 'c', transaction_type: 'PAYMENT_RETRACTION_ITEMS', other_amount_micro: -120n * M }),
    base({ id: 4, business_line_key: 'd', transaction_type: 'Inbound Defect Fee - Missing label', other_amount_micro: -200n * M }),
    base({ id: 5, business_line_key: 'e', transaction_type: 'INBOUND DEFECT FEE x', other_amount_micro: -5n * M }),
  ]);
  const sku = a.lines.find((l) => l.line_kind === 'sku'), idf = a.lines.find((l) => l.line_kind === 'inbound_defect');
  assert.deepEqual([sku.seller_sku, sku.safe_t_jpy, sku.other_amount_jpy, sku.reversal_reimbursement_jpy, sku.account_fee_amount_jpy], ['s', 634, 7, -120, 0]);
  assert.deepEqual([idf.seller_sku, idf.account_fee_amount_jpy, idf.other_amount_jpy, idf.source_lines], ['-', -205, -205, 2]);
  assert.equal(a.lines.length, 2);
  assert.equal(classifySkuAccountFee('Inbound Defect Fee - x'), 'inbound_defect');
  assert.equal(classifySkuAccountFee('FBA Inventory Storage Fee'), null);   // ほかの手数料の種類は SKU のある行なら日次の財務の側 (今まで通り)
  assert.equal(classifySkuAccountFee('Fee Adjustment'), null);
  assert.equal(classifySkuAccountFee('Order'), null);
});
await t('分けられない部品 (0047・純粋関数): 同じ SKU の行の other_amount +100 / −100 = 部品 2・符号つき 0・絶対値 200 / 0 円の部品は数えない / 月の手数料の行の税の price も分けられない (fail-closed)', async () => {
  const M = 1000000n;
  const a = aggregateOrderFinance('X1', [
    base({ id: 1, business_line_key: 'a', transaction_type: 'Other', price_type: 'x', other_amount_micro: 100n * M }),
    base({ id: 2, business_line_key: 'b', transaction_type: 'Other', price_type: 'x', other_amount_micro: -100n * M }),
    base({ id: 3, business_line_key: 'c', misc_fee_amount_micro: 0n }),
    base({ id: 4, business_line_key: 'd', price_type: 'Principal', price_amount_micro: 500n * M, promotion_type: 'Principal', promotion_amount_micro: -50n * M }),
  ]);
  const l = a.lines[0];
  assert.deepEqual([l.other_amount_jpy, l.unclassified_component_count, l.unclassified_mapped_jpy, l.unclassified_abs_jpy, l.unmapped_component_count], [0, 2, 0, 200, 0]);
  assert.equal(a.stats.unclassifiedComponents, 2);
  const f = aggregateOrderFinance('-:2026-03-01', [
    base({ id: 5, amazon_order_id: null, seller_sku_normalized: null, transaction_type: 'Storage Fee', other_amount_micro: -300n * M, price_type: 'Tax', price_amount_micro: -30n * M }),
  ]).lines[0];
  assert.deepEqual([f.line_kind, f.account_fee_amount_jpy, f.sales_tax_jpy, f.unclassified_component_count, f.unclassified_mapped_jpy], ['storage', -300, -30, 1, -30]);
});
await t('🚨 受け口で見分けられない「同じ箱の中の相殺」を変換が数える (#1554 Codex R4・期待値は変換と独立に手で書いた固定の値)', async () => {
  const M = 1000000n;
  const P = (x) => base({ amazon_order_id: null, seller_sku_normalized: null, transaction_type: 'Storage Fee', ...x });   // 疑似注文 -:2026-03-01 の保管料の行
  const pick = (lines, kind) => { const l = lines.find((x) => x.line_kind === kind); return [l.unclassified_component_count, l.unclassified_mapped_jpy, l.unclassified_abs_jpy, l.unmapped_component_count]; };
  // ① 月の手数料の行の同じ列の中の相殺: 材料 −100 (other_amount)・misc_fee +3 と −3 (列は 0) → 部品 2・符号つき 0・絶対値 6
  const a = aggregateOrderFinance('-:2026-03-01', [
    P({ id: 1, business_line_key: 'm1', other_amount_micro: -100n * M }),
    P({ id: 2, business_line_key: 'm2', misc_fee_amount_micro: 3n * M }),
    P({ id: 3, business_line_key: 'm3', misc_fee_amount_micro: -3n * M }),
  ]).lines;
  assert.deepEqual(pick(a, 'storage'), [2, 0, 6, 0]);
  assert.deepEqual([a[0].misc_fee_jpy, a[0].fba_storage_jpy, a[0].account_fee_amount_jpy], [0, -100, -100]);
  // ② 8 列の合計の残差の中の別の列の間の相殺: 材料 −100 (fba_storage)・分けられない other_fee +3 (other_fee の列)・分けられない未知の price −3 (other_amount の列)
  //    → 残差 = (−100 + 3 − 3) − (−100) = 0 = 受け口では見分けられない。変換は 部品 2・符号つき 0・絶対値 6
  const b = aggregateOrderFinance('-:2026-03-01', [
    P({ id: 4, business_line_key: 'r1', other_amount_micro: -100n * M }),
    P({ id: 5, business_line_key: 'r2', other_fee_amount_micro: 3n * M }),
    P({ id: 6, business_line_key: 'r3', price_type: 'Weird', price_amount_micro: -3n * M }),
  ]).lines;
  assert.deepEqual([b[0].fba_storage_jpy, b[0].other_fee_jpy, b[0].other_amount_jpy, b[0].account_fee_amount_jpy], [-100, 3, -3, -100]);
  assert.deepEqual(pick(b, 'storage'), [2, 0, 6, 0]);
  // ③ SKU の行の別の列の間の相殺: other_amount +5 (取引の種類 Other の分けられない補てん)・misc_fee −5 → 部品 2・符号つき 0・絶対値 10
  //    + 同じ列 (other_fee) の中の相殺: MFNPostageFee −7 と other_fee +7 → さらに部品 2・絶対値 14
  const c = aggregateOrderFinance('X1', [
    base({ id: 7, business_line_key: 's1', transaction_type: 'Other', price_type: 'x', other_amount_micro: 5n * M }),
    base({ id: 8, business_line_key: 's2', misc_fee_amount_micro: -5n * M }),
    base({ id: 9, business_line_key: 's3', item_related_fee_type: 'MFNPostageFee', item_related_fee_amount_micro: -7n * M }),
    base({ id: 10, business_line_key: 's4', other_fee_amount_micro: 7n * M }),
  ]).lines;
  assert.deepEqual([c[0].other_amount_jpy, c[0].misc_fee_jpy, c[0].other_fee_jpy], [5, -5, 0]);
  assert.deepEqual(pick(c, 'sku'), [4, 0, 24, 0]);
});
await t('SKU なしの行の種類 = 月の手数料の build と同じ (大文字小文字は LIKE だけ無視・最初に当たった種類)', async () => {
  assert.equal(feeKindOf('FBA INVENTORY STORAGE FEE'), 'storage');   // 前方一致 (LIKE) は大文字小文字を無視
  assert.equal(feeKindOf('storage fee'), 'unknown');                 // 完全一致 (IN) は区別する
  assert.equal(feeKindOf('X-lowinventory-y'), 'low_inventory');
  assert.equal(feeKindOf('Current Reserve Amount'), 'not_account_fee');
  assert.equal(feeKindOf('Goodwill Concession'), 'not_account_fee');
  assert.equal(classifyAccountFee('Fee Adjustment'), 'other_account_fee');
  assert.equal(skuKindOf(''), 'none'); assert.equal(skuKindOf(null), 'none'); assert.equal(skuKindOf(' a '), 'sku');
  // SQLite の CASE と JS の判定が同じ (本物の SQLite で名前ごとに)
  const w = new Database(':memory:');
  const names = ['Storage Fee', 'storage fee', 'FBA Inventory Storage Fee', 'fba inventory storage fee - x', 'StorageRenewalBilling', 'FBA Long Term Storage Fee', 'RemovalComplete', 'FBA Removal Order: Return Fee',
    'Inbound Defect Fee', 'inbound defect fee x', 'LowInventory', 'x Low-Inventory', 'Subscription Fee', 'Amazon Easy Ship Charges', 'Fee Adjustment', 'Overpaid Fees Adjustment', 'Order', 'Mystery', 'FBA_x%_y'];
  const src = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/rebuild-amazon-account-fees.js'), 'utf8');
  assert.ok(src.includes("from './amazon-account-fee-rules.js'"), '手数料の build が共通の部品を読んでいない');
  for (const n of names) {
    const r = w.prepare(`select (transaction_type LIKE 'FBA Inventory Storage Fee%' ESCAPE '\\') a, (transaction_type LIKE '%LowInventory%' OR transaction_type LIKE '%Low-Inventory%') l from (select ? as transaction_type)`).get(n);
    if (r.a) assert.equal(classifyAccountFee(n), 'storage', n);
    if (r.l && !r.a) assert.equal(classifyAccountFee(n), 'low_inventory', n);
  }
  w.close();
});

console.log('② 送り手');
await t('受領記録: 2 回目の回で Render の受領記録と台帳が一致する (台帳の指紋を空にしない)・変わらなければ送らない', async () => {
  const r = await pushClose(L0, { mode: 'range', from: `${MA}-01`, to: monthEnd(MB) });
  assert.equal(r.ledgerReset, null, r.ledgerReset);
  assert.equal(r.changed, 0); assert.equal(r.ok, true);
});
await t('期間は鍵を選ぶだけ: MB の 2 日だけを選んでも O4 は MA の売上の行ごと送る (部分の集合は送らない)', async () => {
  const L = newLedger();
  const before = Number((await one(`select received_batch_seq as s from core.order_finance_receipts where mall_order_no = 'O4'`)).s);
  const r = await pushClose(L, { mode: 'range', from: d(MB, 2), to: d(MB, 2), force: true });
  assert.equal(r.ok, true);
  assert.equal(r.finance.selectedOrders, 1); assert.equal(r.finance.selectedPseudo, 0);
  const rows = await all(`select economic_date_jst::text as d, line_kind from core.order_finance_daily where mall_order_no = 'O4' order by 1`);
  assert.deepEqual(rows.map((x) => x.d), [d(MA, 20), d(MB, 2)]);
  const after = Number((await one(`select received_batch_seq as s from core.order_finance_receipts where mall_order_no = 'O4'`)).s);
  assert.ok(after > before);
});
await t('dry-run は台帳にも Render にも書かない (未照合の月・watermark・送れない鍵)', async () => {
  const L = newLedger();
  const n0 = Number((await one(`select count(*) as n from ops.ingest_runs where entity = 'order_finance'`)).n);
  const r = await pushClose(L, { mode: 'full', dryRun: true });
  assert.equal(r.dryRun, true); assert.ok(r.changed > 0);
  assert.equal(L.getMeta(META.unreconciled), null); assert.equal(L.getMeta(META.watermark), null); assert.deepEqual(retryStore(L).list(), []);
  assert.equal(Number((await one(`select count(*) as n from ops.ingest_runs where entity = 'order_finance'`)).n), n0);
});
await t('watermark: incremental はそろって終わった回の ingested_at の 3 日前から後に入った行の注文だけ・範囲の回は watermark を動かさない', async () => {
  assert.equal(L0.getMeta(META.watermark), null);   // range の回は動かさない
  const r1 = await pushClose(L0, { mode: 'incremental' });   // watermark 無し = 全部 (内容を比べて変わらない)
  assert.equal(r1.ok, true); assert.equal(r1.finance.since, null); assert.equal(r1.changed, 0);
  assert.equal(L0.getMeta(META.watermark), '2026-02-01 00:00:00');
  assert.equal(sinceOf('2026-02-01 00:00:00'), '2026-01-29 00:00:00');
  const id = raw({ order: 'O9', sku: 'sku-z', date: d(MB, 20), qty: 1, ingested: '2026-02-02 00:00:00' });
  raw({ order: 'O9', sku: 'sku-z', date: d(MB, 20), pt: 'Principal', pa: 400, ingested: '2026-02-02 00:00:00' });
  const r2 = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r2.ok, true, JSON.stringify(r2.transformErrors));
  assert.equal(r2.finance.since, '2026-01-29 00:00:00');
  assert.equal(r2.finance.selectedOrders, 2);   // O9 + O6 (V2 の行が 2026-02-01 = 3 日の中)
  assert.equal(r2.changed, 1); assert.equal(r2.applied, 1);
  assert.ok(id > 0);
  assert.equal(L0.getMeta(META.watermark), '2026-02-02 00:00:00');
  // 止まった後 (watermark が古い) でも全部拾う = 窓は固定の日数ではない
  L0.setMeta({ [META.watermark]: '2025-01-01 00:00:00' });
  const r3 = await pushClose(L0, { mode: 'incremental' });
  assert.ok(r3.finance.selectedOrders >= 8, `selected ${r3.finance.selectedOrders}`);
});
await t('変換の版が変わった回は全部を選び、全部送り直す', async () => {
  // 版は今の形 (amazon_finance_v2 以上) の名前でないと送れない (版と行の形の結び付け・#1554 Codex R1 High)
  const r = await pushClose(L0, { mode: 'incremental', transformVersion: 'amazon_finance_v2_test' });
  assert.equal(r.ok, true, JSON.stringify({ te: r.transformErrors.slice(0, 2), f: r.failed.slice(0, 2), e: r.error }));
  assert.equal(r.finance.since, null);
  assert.ok(r.changed >= 10, `changed ${r.changed}`);
  const r2 = await pushClose(L0, { mode: 'incremental' });   // 元の版に戻す = また全部
  assert.ok(r2.changed >= 10);
  assert.equal(L0.getMeta(META.transformVersion), AMAZON_FINANCE_TRANSFORM_VERSION);
});
await t('🚨 旧いコードの送り手に戻しても 4 列は消えない (#1554 Codex R1 High): 旧い版の名前では変換の時点で整形できない / 受け口は v2 の注文への旧い形を 409 DOWNGRADE', async () => {
  const L = newLedger();
  const r = await pushClose(L, { mode: 'range', from: d(MB, 23), to: d(MB, 23), force: true, transformVersion: 'amazon_finance_v1' });
  assert.equal(r.ok, false);
  assert.ok(r.transformErrors.length >= 1 && r.transformErrors.every((x) => /old version/.test(x.error)), JSON.stringify(r.transformErrors.slice(0, 2)));
  // 旧いコードそのもの (4 列の鍵の無い行・旧い版) を HTTP で送る = 409 (送り手は ❌)
  const lines = (await all(`select * from core.order_finance_daily where mall_order_no = 'O14'`)).map((x) => ({ economic_date_jst: x.economic_date_jst.toISOString().slice(0, 10), seller_sku: x.seller_sku,
    line_kind: x.line_kind, source: x.source, source_lines: x.source_lines, sales_principal_jpy: Number(x.sales_principal_jpy), source_updated_at: '2026-01-01T00:00:00Z', content_hash: 'h' }));
  const { orderFinanceChecksum, validateFinanceRows } = await import('../apps/company-db/finance/order-finance-checksum.mjs');
  const body = { run_id: 'ship_202609301500000_eeeeee', batch_seq: 999999, chunk_index: 0, last: true, transform_version: 'amazon_finance_v1',
    rows: [{ mall: 'amazon', scope_key: 'jp', mall_order_no: 'O14', header: { transform_version: 'amazon_finance_v1', set_checksum: orderFinanceChecksum(validateFinanceRows('O14', lines), { legacy: true }) }, lines }] };
  const res = await fetch(`${BASE}/order-finance`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': 'k' }, body: JSON.stringify(body) });
  assert.equal(res.status, 409, await res.text());
  const u = await one(`select unclassified_component_count c, transform_version v from core.order_finance_daily where mall_order_no = 'O14' and line_kind = 'sku'`);
  assert.deepEqual([u.c, u.v], [4, AMAZON_FINANCE_TRANSFORM_VERSION]);
});
await t('--full: 注文の中の一部の行の削除 (ingested_at も鍵も変わらない) を拾う・Render にだけある注文に空の集合 / 🆕 incremental も「読み直す注文」で削除を拾う', async () => {
  // O3 の返品のポイントの行を消す (前は incremental では拾えなかった → 2026-10-01 D7b-1b-3 から生の表の trigger が「読み直す注文」に記録 = incremental も拾う)
  wdb.prepare(`delete from raw_amazon_settlement_lines where amazon_order_id = 'O3' and item_related_fee_type = 'PointsReturned'`).run();
  const ri = await pushClose(L0, { mode: 'incremental' });
  assert.equal(ri.changed, 1); assert.ok(ri.finance.dirtyOrders >= 1, `読み直す注文 ${ri.finance.dirtyOrders}`);
  assert.equal(wdb.prepare(`select count(*) n from amazon_settlement_dirty_orders where mall_order_no = 'O3'`).get().n, 0);   // 送れた = R 以下の記録を消した
  // O9 の行を全部消す = Render にだけある
  wdb.prepare(`delete from raw_amazon_settlement_lines where amazon_order_id = 'O9'`).run();
  // 「Render にだけある鍵」の道を通す = trigger の記録 (読み直す注文) を消してから (記録があれば読み直す注文として同じ墓石が送られる)
  wdb.prepare(`delete from amazon_settlement_dirty_orders where mall_order_no = 'O9'`).run();
  const rf = await pushClose(L0, { mode: 'full' });
  assert.equal(rf.ok, true);
  assert.equal(rf.finance.renderOnly, 1);
  assert.equal(Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O9'`)).n), 0);
  assert.equal(Number((await one(`select lines from core.order_finance_receipts where mall_order_no = 'O9'`)).lines), 0);
  assert.equal(Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O3' and economic_date_jst = $1::date`, [d(MB, 10)])).n), 0);
  const rf2 = await pushClose(L0, { mode: 'full' });   // もう空 = 送らない
  assert.equal(rf2.finance.renderOnly, 0); assert.equal(rf2.changed, 0);
});
await t('1 注文が 500 行を超えたら送らない・送れない鍵に残して次の回に必ず読み直す (直ったら外れる)', async () => {
  for (let i = 0; i < 501; i++) raw({ order: 'O-BIG', sku: `big-${i}`, date: d(MB, 21), qty: 1, ingested: '2026-02-03 00:00:00' });
  const wm0 = L0.getMeta(META.watermark);
  const r = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r.ok, false);
  assert.equal(r.transformErrors.length, 1); assert.match(r.transformErrors[0].error, /500/);
  const failed = retryStore(L0).list();
  assert.deepEqual(failed.map((f) => f.key), [financeKey('O-BIG')]);
  assert.equal(L0.getMeta(META.watermark), wm0);   // そろって終わらなかった = 進めない
  wdb.prepare(`delete from raw_amazon_settlement_lines where amazon_order_id = 'O-BIG' and seller_sku_normalized <> 'big-0'`).run();
  L0.setMeta({ [META.watermark]: '2026-06-01 00:00:00' });   // 窓の外でも送れない鍵は読み直す
  const r2 = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r2.ok, true, JSON.stringify(r2.transformErrors));
  assert.equal(r2.finance.selectedOrders, 1); assert.equal(r2.applied, 1);
  assert.deepEqual(retryStore(L0).list(), []);
});
await t('鍵の分からない不正な行 (注文番号なし・計上日が読めない) がある間は疑似注文を 1 つも送らない (空の集合も)・本物の注文は送る・❌', async () => {
  const before = await all(`select mall_order_no, economic_date_jst::text as d, net_jpy::text as n from core.order_finance_daily where mall_order_no like '-%' order by 1, 2, 3`);
  // 送った疑似注文 (MB 18 日) の 2 行のうち 1 行だけ日付を読めなくする
  const bad = wdb.prepare(`select id from raw_amazon_settlement_lines where amazon_order_id is null and economic_date = ? and transaction_type = 'Fee Adjustment'`).get(d(MB, 18)).id;
  wdb.prepare(`update raw_amazon_settlement_lines set economic_date = '2026-13-40' where id = ?`).run(bad);
  raw({ order: 'O10', sku: 'sku-y', date: d(MB, 22), qty: 1, ingested: '2026-06-02 00:00:00' });
  const pseudoFailed = financeKey(`-:${d(MB, 19)}`);   // 前の回に送れなかった疑似注文 = 止めた回も一覧に持ち越す
  retryStore(L0).replace([{ key: pseudoFailed, error: 'x' }]);
  const r = await pushClose(L0, { mode: 'full' });
  // 前の回の分も、今回の窓で選んで止めた疑似注文も全部残る (#1534 Codex R2 High)
  const keep = retryStore(L0).list().map((f) => f.key);
  assert.ok(keep.includes(pseudoFailed), JSON.stringify(keep));
  assert.equal(keep.length, r.finance.pseudoBlocked);
  assert.equal(r.ok, false);
  assert.equal(r.finance.unkeyed.length, 1); assert.equal(r.finance.unkeyed[0].economic_date, '2026-13-40');
  assert.ok(r.finance.pseudoBlocked > 0);
  const after = await all(`select mall_order_no, economic_date_jst::text as d, net_jpy::text as n from core.order_finance_daily where mall_order_no like '-%' order by 1, 2, 3`);
  assert.deepEqual(after, before);   // 疑似注文は古い集合のまま (18 日の 2 行とも残る)
  assert.equal(Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O10'`)).n), 1);
  assert.equal(JSON.parse(L0.getMeta(META.unkeyed)).length, 1);
  const wm = L0.getMeta(META.watermark);
  wdb.prepare(`update raw_amazon_settlement_lines set economic_date = ? where id = ?`).run(d(MB, 18), bad);   // 直す → 次の回で外れる
  const r2 = await pushClose(L0, { mode: 'full' });
  assert.equal(r2.ok, true, JSON.stringify({ te: r2.transformErrors, f: r2.failed, st: r2.stale, e: r2.error, u: r2.finance.unkeyed }));
  assert.deepEqual(JSON.parse(L0.getMeta(META.unkeyed)), []);
  assert.notEqual(L0.getMeta(META.watermark), wm);
});
await t('容量の見張り: 上限が無ければ送らない / いまは 80% 未満でも次の chunk を足すと超えるなら送らずに止める / 送った分を足して見込む', async () => {
  assert.throws(() => capacityGuard({ limitBytes: 0, rowBytes: 1, replaceFactor: 1, walAllowanceBytes: 0, marginBytes: 0, fetchStatus: async () => ({}) }), /CDB_DB_LIMIT_BYTES/);
  let calls = 0;
  const g = capacityGuard({ limitBytes: 1000, rowBytes: 10, replaceFactor: 2, walAllowanceBytes: 50, marginBytes: 0, orderBytes: 1, refreshEvery: 100, fetchStatus: async () => { calls++; return { size: { db_bytes: 600, wal_bytes: null } }; } });
  await g({ lines: 5, mustOwn: () => {} });            // DB 600 + WAL の見込み 50 + 次の 5 行 × 10 × 2 = 750 < 800
  await assert.rejects(g({ lines: 5, mustOwn: () => {} }), /超える/);   // 650 + 送った分 100 + 次の 100 = 850 > 800 (いまの 650 は 80% 未満)
  assert.equal(calls, 1);
  const g2 = capacityGuard({ limitBytes: 1000, rowBytes: 1, replaceFactor: 1, walAllowanceBytes: 0, marginBytes: 0, orderBytes: 1, fetchStatus: async () => ({ size: {} }) });
  await assert.rejects(g2({ lines: 1, mustOwn: () => {} }), /db_bytes/);
  // 送り手の通しでも: 小さな上限なら何も送らずに止まる
  const L = newLedger();
  await assert.rejects(pushClose(L, { mode: 'range', from: `${MA}-01`, to: monthEnd(MB), force: true, capacity: { ...BIG, limitBytes: 1000 } }), /超える/);
});
await t('Render の復元 (受領記録が無い) → 指紋を空にした鍵は watermark の窓の外でも全部読み直す (#1534 Codex R1 High)', async () => {
  assert.ok(L0.getLastReceipt());
  // 復元 = 台帳の最後の受領記録が Render に無い (ops.ingest_chunks は append-only で消せないので、無い run を指させる)
  L0.setMeta({ last_receipt: JSON.stringify({ run_id: 'ship_202601010000000_abcdef', chunk_index: 0, payload_checksum: 'x' }) });
  await pg.query(`delete from core.order_finance_daily where mall_order_no = 'O2'`);
  await pg.query(`delete from core.order_finance_receipts where mall_order_no = 'O2'`);
  L0.setMeta({ [META.watermark]: '2026-09-01 00:00:00' });   // 窓には何も入らない
  const r = await pushClose(L0, { mode: 'incremental' });
  assert.ok(r.ledgerReset, 'ledgerReset');
  assert.ok(r.finance.unconfirmed >= 10, `unconfirmed ${r.finance.unconfirmed}`);
  assert.equal(r.ok, true, JSON.stringify({ te: r.transformErrors, f: r.failed }));
  assert.ok(Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O2'`)).n) >= 4);
});
await t('前の回に outbox に残った鍵 (送らずに落ちた) は watermark の窓の外でも読み直す (#1534 Codex R1 High)', async () => {
  wdb.prepare(`update raw_amazon_settlement_lines set price_amount_micro = 900000000 where amazon_order_id = 'O5' and price_type = 'Principal'`).run();   // ingested_at は変えない
  L0.pushOutbox('ship_202601010000000_000000', [{ key: financeKey('O5'), fp: 'x', payload: '{}', n_lines: 0, n_bytes: 2 }]);
  L0.setMeta({ [META.watermark]: '2026-09-01 00:00:00' });
  const r = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r.ok, true); assert.equal(r.carriedOver, 0);   // もう追跡している鍵 (引き継ぎの数には入らない) でも選ぶ
  assert.equal(r.applied, 1);
  assert.equal(Number((await one(`select sales_principal_jpy as n from core.order_finance_daily where mall_order_no = 'O5' and line_kind = 'sku'`)).n), 900);
});
await t('送信の途中で落ちても、見つけた送れない鍵は台帳に残る (#1534 Codex R1 High)', async () => {
  raw({ order: 'O-FRAC', sku: 'sku-q', date: d(MB, 23), pt: 'Principal', pa: 10.5, ingested: '2026-06-04 00:00:00' });
  retryStore(L0).replace([]);
  await assert.rejects(pushClose(L0, { mode: 'range', from: d(MB, 1), to: monthEnd(MB), force: true, capacity: { ...BIG, limitBytes: 1000 } }), /超える/);
  assert.deepEqual(retryStore(L0).list().map((f) => f.key), [financeKey('O-FRAC')]);
  wdb.prepare(`delete from raw_amazon_settlement_lines where amazon_order_id = 'O-FRAC'`).run();
});
await t('chunk の応答で failed の鍵は、後の chunk で落ちても読み直す鍵に残る (#1534 Codex R2 High)', async () => {
  await pg.exec(`create function pg_temp_fail_o1() returns trigger language plpgsql as $$ begin if new.mall_order_no = 'O1' then raise exception '試験の失敗'; end if; return new; end $$;
    create trigger t_fail_o1 before insert on core.order_finance_daily for each row execute function pg_temp_fail_o1();`);
  wdb.prepare(`update raw_amazon_settlement_lines set price_amount_micro = 101000000 where amazon_order_id = 'O1' and price_type = 'Tax'`).run();   // 中身を変える (同じなら 'same' = 挿入しない)
  try {
    retryStore(L0).replace([]);
    let n = 0;
    await assert.rejects(pushClose(L0, { mode: 'range', from: d(MB, 5), to: d(MB, 6), force: true, chunkSize: 1,
      beforeChunk: async ({ rows }) => { if (n) throw new Error('O1 の次の chunk で止めた'); if (rows.some((x) => x.mall_order_no === 'O1')) n = 1; } }), /O1 の次の chunk/);
    const kept = retryStore(L0).list();   // 前の試験で outbox に残った鍵 (outbox_leftover) も入る
    assert.ok(kept.some((f) => f.key === financeKey('O1') && f.error === 'failed'), JSON.stringify(kept.slice(0, 3)));
  } finally { await pg.exec(`drop trigger t_fail_o1 on core.order_finance_daily; drop function pg_temp_fail_o1();`); }
  const r = await pushClose(L0, { mode: 'incremental' });   // 窓の外でも O1 を読み直す
  assert.equal(r.ok, true); assert.ok(r.applied >= 1);
  assert.deepEqual(retryStore(L0).list(), []);
});
await t('outbox を引き継いだ後、Render の鍵を取る前に落ちても、残った鍵は読み直す鍵に残る (#1534 Codex R2 High)', async () => {
  retryStore(L0).replace([]);
  L0.pushOutbox('ship_202601010000000_000001', [{ key: financeKey('O2'), fp: 'x', payload: '{}', n_lines: 0, n_bytes: 2 }]);
  const failKeys = (url, init) => (String(url).includes('/order-finance/keys') ? Promise.resolve(new Response('boom', { status: 500 })) : fetch(url, init));
  await assert.rejects(pushClose(L0, { mode: 'full', fetchImpl: failKeys }), /Render の鍵/);
  assert.deepEqual(L0.outboxKeys(), []);   // outbox は引き継ぎで消えた
  assert.deepEqual(retryStore(L0).list().map((f) => f.key), [financeKey('O2')]);
  const r = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r.ok, true); assert.deepEqual(retryStore(L0).list(), []);
});
await t('疑似注文を止めた回が送信の途中で落ちても、止めた疑似注文は読み直す鍵に残る (#1534 Codex R2 High)', async () => {
  retryStore(L0).replace([]);
  const bad = raw({ order: null, date: '2026-02-31', tt: 'Storage Fee', oa: -1, ingested: '2026-06-06 00:00:00' });
  try {
    await assert.rejects(pushClose(L0, { mode: 'range', from: d(MB, 1), to: d(MB, 20), force: true, capacity: { ...BIG, limitBytes: 1000 } }), /超える/);
    const keys = retryStore(L0).list().filter((f) => f.error === 'pseudo_blocked').map((f) => f.key);
    assert.ok(keys.includes(financeKey(`-:${d(MB, 7)}`)), JSON.stringify(keys));
  } finally { wdb.prepare(`delete from raw_amazon_settlement_lines where id = ?`).run(bad); }
  const r = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r.ok, true, JSON.stringify(r.transformErrors)); assert.deepEqual(retryStore(L0).list(), []);
});
await t('受け口が受け取らない注文番号は その注文だけ送れない鍵にする (同じ chunk の正常な注文は送る)・行が消えたら一覧から外れる (#1534 Codex R1 Medium)', async () => {
  raw({ order: 'BAD#ORDER', sku: 'sku-r', date: d(MB, 24), qty: 1, ingested: '2026-06-05 00:00:00' });
  raw({ order: 'O11', sku: 'sku-s', date: d(MB, 24), qty: 1, ingested: '2026-06-05 00:00:00' });
  const r = await pushClose(L0, { mode: 'range', from: d(MB, 24), to: d(MB, 24) });
  assert.deepEqual(r.transformErrors.map((x) => x.key), [financeKey('BAD#ORDER')]);
  assert.equal(r.failed.length, 0);
  assert.equal(Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O11'`)).n), 1);
  assert.ok(retryStore(L0).list().some((f) => f.key === financeKey('BAD#ORDER') || f.key === financeKey('O-FRAC')));
  wdb.prepare(`delete from raw_amazon_settlement_lines where amazon_order_id = 'BAD#ORDER'`).run();
  const r2 = await pushClose(L0, { mode: 'range', from: d(MB, 24), to: d(MB, 24) });
  assert.equal(r2.transformErrors.length, 0);
  assert.deepEqual(retryStore(L0).list(), []);
});
await t('容量の見張り: 空の集合で消す行も数える (max(前, 新))・注文ごとの受領の分・期限超過の予約は戻す・1 行の大きさと倍率は 0 を許さない (#1534 Codex R3)', async () => {
  assert.throws(() => capacityGuard({ limitBytes: 1000, rowBytes: 0, replaceFactor: 1, walAllowanceBytes: 0, marginBytes: 0, fetchStatus: async () => ({}) }), /0 より大きい/);
  assert.throws(() => capacityGuard({ limitBytes: 1000, rowBytes: 1, replaceFactor: 0, walAllowanceBytes: 0, marginBytes: 0, orderBytes: 1, fetchStatus: async () => ({}) }), /0 より大きい/);
  assert.throws(() => capacityGuard({ limitBytes: 1000, rowBytes: 1, replaceFactor: 1, walAllowanceBytes: 0, marginBytes: 0, orderBytes: 0, fetchStatus: async () => ({}) }), /0 より大きい/);
  const st = async () => ({ size: { db_bytes: 100, wal_bytes: 0 } });
  const g = capacityGuard({ limitBytes: 1000, rowBytes: 1, replaceFactor: 1, walAllowanceBytes: 0, marginBytes: 0, orderBytes: 10, refreshEvery: 100, fetchStatus: st,
    weightOf: (rows) => rows.reduce((s, x) => s + (x.mall_order_no === 'BIG' ? 600 : x.lines.length), 0) });
  // 空の集合 (lines 0) でも前の 600 行が消える = 600 + 受領 10 → 100 + 610 = 710 < 800
  const h = await g({ rows: [{ mall_order_no: 'BIG', lines: [] }], lines: 0, mustOwn: () => {} });
  await assert.rejects(g({ rows: [{ mall_order_no: 'BIG', lines: [] }], lines: 0, mustOwn: () => {} }), /超える/);   // 710 + 610 > 800
  h.release();   // 期限超過で送らなかった = 予約を戻す → 同じ chunk はまた通る
  await g({ rows: [{ mall_order_no: 'BIG', lines: [] }], lines: 0, mustOwn: () => {} });
});
await t('送り終えた後に lock を奪われていたら、読み直す鍵も watermark も確定しない (確定は lock の中・#1534 Codex R3 High)', async () => {
  retryStore(L0).replace([{ key: financeKey('KEEP-ME'), error: '別の送り手が残した' }]);
  const wm = L0.getMeta(META.watermark);
  raw({ order: 'O12', sku: 'sku-t', date: d(MB, 25), qty: 1, ingested: '2026-06-07 00:00:00' });
  const steal = async (url, init) => {
    const res = await fetch(url, init);
    if (init && init.method === 'POST') L0.putMeta('lock', JSON.stringify({ owner: 'other', pid: process.pid, started_at: new Date().toISOString(), heartbeat_at: new Date().toISOString() }));
    return res;
  };
  await assert.rejects(pushClose(L0, { mode: 'incremental', fetchImpl: steal }), /lock/);
  assert.ok(retryStore(L0).list().some((f) => f.key === financeKey('KEEP-ME')));
  assert.equal(L0.getMeta(META.watermark), wm);
  L0.putMeta('lock', '');   // 片付け (奪った体の lock を外す)
  retryStore(L0).replace([]);
});
await t('容量の見込みの「消える行」は Render のいまの行数 (incremental でも・台帳に頼らない。#1534 Codex R4 High)', async () => {
  const before = Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O2'`)).n);
  assert.ok(before >= 4);
  // O2 を 1 行 (5 日の売上) だけにする (Render は まだ before 行)。取込時刻は窓の中
  wdb.prepare(`delete from raw_amazon_settlement_lines where amazon_order_id = 'O2' and economic_date <> ?`).run(d(MB, 5));
  wdb.prepare(`update raw_amazon_settlement_lines set ingested_at = '2026-06-10 00:00:00' where amazon_order_id = 'O2' and quantity_purchased is not null`).run();
  const r = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r.ok, true); assert.ok(r.changed >= 1);
  assert.ok(r.finance.weightLines >= before, `見込み ${r.finance.weightLines} 行 < Render の ${before} 行`);
  assert.equal(Number((await one(`select count(*) as n from core.order_finance_daily where mall_order_no = 'O2'`)).n), 1);
});
await t('返送の注文番号 (+ / を含む) も送れる (本番の決済に 26 注文)', async () => {
  raw({ order: 'a+aKPxfQ/X', date: d(MB, 26), tt: 'FBA Removal Order: Return Fee', oa: -120, ingested: '2026-06-11 00:00:00' });
  raw({ order: '+3gubNop3S', date: d(MB, 26), tt: 'RemovalComplete', oa: -80, ingested: '2026-06-11 00:00:00' });
  const r = await pushClose(L0, { mode: 'incremental' });
  assert.equal(r.ok, true, JSON.stringify(r.transformErrors)); assert.equal(r.transformErrors.length, 0);
  const n = await one(`select count(*)::int as n, sum(account_fee_amount_jpy)::int as a from core.order_finance_daily where mall_order_no in ('a+aKPxfQ/X', '+3gubNop3S') and line_kind = 'removal'`);
  assert.deepEqual([n.n, n.a], [2, -200]);
  const keys = await (await fetch(`${BASE}/order-finance/keys?mall=amazon&scope=jp&after=${encodeURIComponent('+')}&limit=5`, { headers: { 'x-sync-key': 'k' } })).json();
  assert.ok(keys.keys.includes('+3gubNop3S'), JSON.stringify(keys));
});
await t("'-' で始まる不正な本物の注文番号は疑似注文と取り違えず、別の月の回でも送れない鍵に残る (#1534 Codex R5 Medium)", async () => {
  retryStore(L0).replace([]);
  raw({ order: '-BAD', sku: 'sku-u', date: d(MA, 3), pt: 'Principal', pa: 1000, ingested: '2026-06-12 00:00:00' });
  const r1 = await pushClose(L0, { mode: 'range', from: d(MA, 1), to: d(MA, 5) });
  assert.ok(r1.transformErrors.some((x) => x.key === financeKey('-BAD')), JSON.stringify(r1.transformErrors));
  const r2 = await pushClose(L0, { mode: 'range', from: d(MB, 27), to: d(MB, 27) });   // 別の月の回
  assert.ok(r2.transformErrors.some((x) => x.key === financeKey('-BAD')));
  assert.ok(retryStore(L0).list().some((f) => f.key === financeKey('-BAD')));
  wdb.prepare(`delete from raw_amazon_settlement_lines where amazon_order_id = '-BAD'`).run();
  const r3 = await pushClose(L0, { mode: 'range', from: d(MB, 27), to: d(MB, 27) });
  assert.equal(r3.transformErrors.length, 0); assert.deepEqual(retryStore(L0).list(), []);
});
await t('Render から読む (GET) は 408・429・5xx・通信の失敗を読み直し、ほかの 4xx はすぐ止める・回数と締め切りで止まる・読み直しをログに残す (共通部 pipeline.mjs・#1545 Codex R1)', async () => {
  const { getJson } = await import('../apps/company-db/push/pipeline.mjs');
  const seq = (list) => { let i = 0; const calls = []; const f = async (url) => { calls.push(url); const x = list[Math.min(i++, list.length - 1)]; if (x instanceof Error) throw x; return new Response(typeof x === 'number' ? '<html>  502\n bad </html>' : JSON.stringify(x), { status: typeof x === 'number' ? x : 200 }); }; f.calls = calls; return f; };
  let slept = []; const logs = [];
  const sleep = async (ms) => { slept.push(ms); };
  const f1 = seq([502, 500, 408, 429, { ok: 1 }]);
  assert.deepEqual(await getJson(f1, 'u', 'k', 'x', { sleep, log: (m) => logs.push(m) }), { ok: 1 });
  assert.equal(f1.calls.length, 5); assert.deepEqual(slept, [5000, 10000, 20000, 30000]);
  assert.equal(logs.length, 4); assert.match(logs[0], /HTTP 502 <html> 502 bad <\/html> → 5 秒後に読み直す \(1\/8\)/);
  slept = [];
  const f2 = seq([new TypeError('fetch failed'), { ok: 2 }]);
  assert.deepEqual(await getJson(f2, 'u', 'k', 'x', { sleep }), { ok: 2 });
  const f3 = seq([400]);
  await assert.rejects(getJson(f3, 'u', 'k', 'Render の鍵', { sleep }), /Render の鍵が取れない: HTTP 400/);
  assert.equal(f3.calls.length, 1);
  const f4 = seq([502]);
  await assert.rejects(getJson(f4, 'u', 'k', 'x', { sleep }), /HTTP 502 .*\(8 回読んだ\)/);
  assert.equal(f4.calls.length, 8);
  // 締め切り: 残りの時間で次の待ちが入らなければ止まる (工程の上限より前に自分で理由を出す)
  let t0 = 0; const now = () => t0;
  const f5 = seq([503]);
  await assert.rejects(getJson(f5, 'u', 'k', 'x', { sleep: async (ms) => { t0 += ms; }, now, deadline: 20000 }), /\(3 回読んだ\)/);   // 0 秒・5 秒・15 秒に読む → 次の 20 秒待ちは締め切り (20 秒) を越える = 3 回で止まる
  // 締め切りを過ぎていれば 1 度も読まない (#1545 Codex R2)
  const f6 = seq([{ ok: 6 }]);
  await assert.rejects(getJson(f6, 'u', 'k', 'x', { now: () => 1000, deadline: 100 }), /締め切りを過ぎた \(0 回読んだ\)/);
  assert.equal(f6.calls.length, 0);
  // 読み直した後の 4xx にも回数
  await assert.rejects(getJson(seq([502, 400]), 'u', 'k', 'x', { sleep }), /HTTP 400 .*\(2 回読んだ\)/);
  // 共通の鍵の一覧はページごとに心拍
  const { fetchAllKeys } = await import('../apps/company-db/push/pipeline.mjs');
  let pages = 0, beats = 0;
  const kf = async () => { pages++; return new Response(JSON.stringify({ keys: ['a'], next: pages < 3 ? 'x' : null }), { status: 200 }); };
  const keys = await fetchAllKeys(kf, { base: 'b', syncKey: 'k', path: '/p', keysOf: (j) => j.keys, onPage: () => { beats++; } });
  assert.deepEqual([keys.length, beats], [3, 3]);
});
await t('送り手の最初の読み取り (Render の状態) も 502 を読み直して進む (共通部 = 伝票・注文の送り手も同じ・#1545 Codex R1 High)', async () => {
  const L = newLedger();
  let first = true;
  const flaky = async (url, init) => { if (first && String(url).includes('/order-finance/status')) { first = false; return new Response('<html>502</html>', { status: 502 }); } return fetch(url, init); };
  const r = await pushClose(L, { mode: 'range', from: d(MB, 27), to: d(MB, 27), fetchImpl: flaky });
  assert.equal(first, false); assert.equal(r.ok, true);
});
await t('parseArgs: 操作は 1 つ・--from/--to は組・--all は --reconcile と', async () => {
  assert.throws(() => parseArgs([]), /どれか 1 つ/);
  assert.throws(() => parseArgs(['--incremental', '--full']), /どれか 1 つ/);
  assert.throws(() => parseArgs(['--from', '2026-01-01']), /組/);
  assert.throws(() => parseArgs(['--incremental', '--all']), /--all/);
  assert.equal(parseArgs(['--from', '2026-01-01', '--to', '2026-01-31', '--dry-run']).dryRun, true);
});

console.log('③ 突き合わせ');
build();   // 削除・追加の後の raw で SQLite を作り直す (daily-sync の順 = build → 送り手 → 突き合わせ)
await pushClose(L0, { mode: 'full' });
await t('一致: 直近 45 日 + 未照合の月 = 差 0・未照合の月が消える', async () => {
  const w = reader();
  try {
    assert.ok(JSON.parse(L0.getMeta(META.unreconciled)).length > 0);
    const rr = await reconcileAmazonFinance({ warehouse: w, ledger: L0, dataDir: tmpDir, base: BASE, syncKey: 'k', log: quiet });
    assert.equal(rr.ok, true, JSON.stringify({ d: rr.daily.slice(0, 3), f: rr.fees, u: rr.uncovered }));
    assert.equal(rr.level, 'ok');
    assert.deepEqual(JSON.parse(L0.getMeta(META.unreconciled)), []);
  } finally { w.close(); }
});
await t('差: 日次の財務の古い行 → 日次のやり残しに登録・1 回目 ⚠️・2 回目 ❌・直れば 0 に戻る', async () => {
  wdb.prepare(`update f_amazon_finance_sku_daily_v1 set commission_jpy = commission_jpy + 1 where date_jst = ? and seller_sku = 'sku-a'`).run(d(MB, 5));
  L0.setMeta({ [META.unreconciled]: JSON.stringify([MB]) });
  const w = reader();
  try {
    const r1 = await reconcileAmazonFinance({ warehouse: w, ledger: L0, dataDir: tmpDir, base: BASE, syncKey: 'k', log: quiet });
    assert.equal(r1.level, 'warn'); assert.deepEqual(r1.dailyDiffMonths, [MB]);
    assert.ok(readPendingMonths(tmpDir, { file: PENDING_FILE }).months.includes(MB));
    assert.deepEqual(JSON.parse(L0.getMeta(META.unreconciled)), [MB]);   // 差の月は残す
    const r2 = await reconcileAmazonFinance({ warehouse: w, ledger: L0, dataDir: tmpDir, base: BASE, syncKey: 'k', log: quiet });
    assert.equal(r2.level, 'error'); assert.equal(r2.streak, 2);
  } finally { w.close(); }
  build();
  const w2 = reader();
  try {
    const r3 = await reconcileAmazonFinance({ warehouse: w2, ledger: L0, dataDir: tmpDir, base: BASE, syncKey: 'k', log: quiet });
    assert.equal(r3.ok, true); assert.equal(L0.getMeta(META.diffStreak), '0');
  } finally { w2.close(); }
});
await t('月の手数料: 14 か月より古い月を訂正 → 差 → 手数料のやり残し → さかのぼる build で一致・build と sync の後に消す', async () => {
  await pg.query(`update core.finance_source_policy set period_from = $1::date where mall = 'amazon'`, [`${OLD}-01`]);   // 古い月も採用する (試験だけ)
  NS(d(OLD, 10), 'Storage Fee', { oa: -3000, ingested: '2026-06-09 00:00:00' });   // ほかの試験の行より後 (watermark の窓の中)
  const rp = await pushClose(L0, { mode: 'incremental' });
  assert.equal(rp.ok, true);
  assert.ok(JSON.parse(L0.getMeta(META.unreconciled)).includes(OLD));
  const w = reader();
  try {
    const r1 = await reconcileAmazonFinance({ warehouse: w, ledger: L0, dataDir: tmpDir, base: BASE, syncKey: 'k', log: quiet });
    assert.deepEqual(r1.feeDiffMonths, [OLD]);
    assert.deepEqual(readPendingMonths(tmpDir, { file: ACCOUNT_FEES_PENDING_FILE }).months, [OLD]);
  } finally { w.close(); }
  const plan = accountFeesMonthsBack(tmpDir, { currentMonth: ymOffset(0) });
  assert.equal(plan.months, 17); assert.deepEqual(plan.covered, [OLD]); assert.equal(plan.fromMonth, OLD);
  buildFees(plan.fromMonth);   // daily-sync と同じ = 始まりの月を明示で渡す
  // sync も同じ範囲 (dry-run で範囲だけ確かめる)
  const syncOut = execFileSync(process.execPath, ['apps/warehouse/sync-amazon-account-fees.js', '--data-dir', tmpDir, '--from-month', plan.fromMonth, '--dry-run'], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8' });
  assert.match(syncOut, new RegExp(`scope: ${OLD}-01 〜 ${ymOffset(0)}-01 \\(17 months\\)`));
  const w2 = reader();
  try {
    const r2 = await reconcileAmazonFinance({ warehouse: w2, ledger: L0, dataDir: tmpDir, base: BASE, syncKey: 'k', log: quiet });
    assert.equal(r2.ok, true, JSON.stringify(r2.fees));
    assert.ok(!JSON.parse(L0.getMeta(META.unreconciled)).includes(OLD));
  } finally { w2.close(); }
  // daily-sync は build と sync の両方が通った後に covered を消す (writePendingMonths の attempted)
  const { writePendingMonths } = await import('../apps/warehouse/amazon-finance-months.js');
  writePendingMonths(tmpDir, [], { attempted: plan.covered, file: ACCOUNT_FEES_PENDING_FILE });
  assert.deepEqual(readPendingMonths(tmpDir, { file: ACCOUNT_FEES_PENDING_FILE }).months, []);
});
await t('未照合の月が 800 日より前でも、月の手数料は 24 か月ずつ取る (受け口の上限で 400 にならない。#1534 Codex R1 Medium)', async () => {
  const far = ymOffset(-30);
  L0.setMeta({ [META.unreconciled]: JSON.stringify([far]) });
  const w = reader();
  try {
    const rr = await reconcileAmazonFinance({ warehouse: w, ledger: L0, dataDir: tmpDir, base: BASE, syncKey: 'k', log: quiet });
    assert.equal(rr.ok, true, JSON.stringify(rr.fees));
    assert.ok(rr.checkedMonths.includes(far));
    assert.deepEqual(JSON.parse(L0.getMeta(META.unreconciled)), []);
  } finally { w.close(); }
});
console.log('④ daily-sync の工程 (F2b-3)');
await t('日曜 (JST の業務日) は --full・ほかは --incremental・どちらも --require-backfilled', async () => {
  const { amazonFinanceDailyArgs } = await import('../apps/warehouse/amazon-finance-months.js');
  assert.deepEqual(amazonFinanceDailyArgs('2026-10-04'), ['--full', '--require-backfilled']);        // 日曜
  assert.deepEqual(amazonFinanceDailyArgs('2026-10-05'), ['--incremental', '--require-backfilled']); // 月曜
  assert.deepEqual(amazonFinanceDailyArgs('2026-10-03'), ['--incremental', '--require-backfilled']); // 土曜
  assert.throws(() => amazonFinanceDailyArgs('2026/10/04'), /YYYY-MM-DD/);
});
await t('🚨 daily-sync のスイッチ (#1567): env CDB_FINANCE_COORDINATOR が無い = master (PR #1567 の前) と同じ 2 工程・同じ引数 (Amazon Settlement → CompanyDB財務(Amazon)) / =1 のときだけ coordinator の 1 工程 (Amazon決済と財務)・retry も同じスイッチ', async () => {
  const { financeCoordinatorEnabled, settlementStep, financePushStep, FINANCE_COORDINATOR_ENV } = await import('../apps/warehouse/finance-coordinator-switch.js');
  assert.equal(FINANCE_COORDINATOR_ENV, 'CDB_FINANCE_COORDINATOR');
  const saved = process.env.CDB_FINANCE_COORDINATOR;
  delete process.env.CDB_FINANCE_COORDINATOR;
  try {
    // スイッチが無い (既定) = master の daily-sync と同じ: runScript('apps/warehouse/fetch-amazon-settlements.js --days 14', 'Amazon Settlement', 3600000) /
    //   runScript(`apps/company-db/push/amazon-finance.mjs ${financeArgs.join(' ')}`, `Company DB Amazon 財務 push (${financeArgs[0]})`, 1800000) (日曜 --full・ほか --incremental)
    assert.equal(financeCoordinatorEnabled(), false, '既定 (env が無い) は今までの 2 工程');
    assert.deepEqual(settlementStep(), { name: 'Amazon Settlement', cmd: 'apps/warehouse/fetch-amazon-settlements.js --days 14', label: 'Amazon Settlement', timeoutMs: 3600000 });
    assert.deepEqual(financePushStep('2026-10-05'), { name: 'CompanyDB財務(Amazon)', cmd: 'apps/company-db/push/amazon-finance.mjs --incremental --require-backfilled', label: 'Company DB Amazon 財務 push (--incremental)', timeoutMs: 1800000 });
    assert.deepEqual(financePushStep('2026-10-04'), { name: 'CompanyDB財務(Amazon)', cmd: 'apps/company-db/push/amazon-finance.mjs --full --require-backfilled', label: 'Company DB Amazon 財務 push (--full)', timeoutMs: 1800000 });   // 日曜
    for (const v of ['', '0', 'true', 'yes', ' 2 ']) assert.equal(financeCoordinatorEnabled({ CDB_FINANCE_COORDINATOR: v }), false, `「${v}」は off`);
    assert.equal(financeCoordinatorEnabled({ CDB_FINANCE_COORDINATOR: ' 1 ' }), false, '前後の空白も許さない (ちょうど 1・Codex R6 Low 1)');
    assert.equal(financeCoordinatorEnabled({ CDB_FINANCE_COORDINATOR: '1' }), true);
    // スイッチがある = coordinator の 1 工程・送り手の工程は無い
    process.env.CDB_FINANCE_COORDINATOR = '1';
    assert.deepEqual(settlementStep(), { name: 'Amazon決済と財務', cmd: 'apps/warehouse/amazon-finance-coverage-run.js --source v2', label: 'Amazon決済と財務', timeoutMs: 5400000 });
    assert.equal(financePushStep('2026-10-05'), null);
    // retry (同じスイッチ): 朝と retry の間にスイッチが変わっても今のスイッチの工程で走らせる
    const { renameRetryJobs, UPSTREAM_OF, JOB_DEFINITIONS, RETRY_ORDER } = await import('../apps/warehouse/retry-failed-jobs.js');
    assert.deepEqual(renameRetryJobs(['f_sales', 'Amazon Settlement', 'CompanyDB財務(Amazon)']), ['f_sales', 'Amazon決済と財務'], 'スイッチがある = 今までの 2 工程の名前 → coordinator');
    delete process.env.CDB_FINANCE_COORDINATOR;
    assert.deepEqual(renameRetryJobs(['f_sales', 'Amazon決済と財務'], { switched: false }), ['f_sales', 'Amazon Settlement', 'CompanyDB財務(Amazon)'], 'スイッチが無く一度も coordinator で回っていない = coordinator の名前 → 今までの 2 工程 (上流が先)');
    assert.deepEqual(renameRetryJobs(['f_sales', 'Amazon決済と財務']), ['f_sales', 'Amazon決済と財務'], '切り替え済みか分からない (既定) = 読み替えない (一方向・Codex R6 High)');
    assert.deepEqual(renameRetryJobs(['f_sales', 'Amazon決済と財務'], { switched: true }), ['f_sales', 'Amazon決済と財務'], '切り替え済み = 読み替えない');
    assert.deepEqual(renameRetryJobs(['CompanyDB財務(Amazon)', 'Render同期']), ['CompanyDB財務(Amazon)', 'Render同期'], 'スイッチが無い = 今までの名前はそのまま');
    // retry の定義 = master と同じ (今までの 2 工程) + coordinator
    assert.deepEqual(JOB_DEFINITIONS['Amazon Settlement'], { script: 'apps/warehouse/fetch-amazon-settlements.js', args: ['--days', '14'], timeoutMs: 3600000 });
    assert.deepEqual(JOB_DEFINITIONS['CompanyDB財務(Amazon)'], { script: 'apps/company-db/push/amazon-finance.mjs', args: ['--full', '--require-backfilled'], timeoutMs: 1800000 });
    assert.deepEqual(JOB_DEFINITIONS['Amazon決済と財務'], { script: 'apps/warehouse/amazon-finance-coverage-run.js', args: ['--source', 'v2'], timeoutMs: 5400000 });
    assert.equal(UPSTREAM_OF['CompanyDB財務(Amazon)'], 'Amazon Settlement', '今までどおり: 決済の取込が失敗した回は送らない');
    assert.ok(RETRY_ORDER.indexOf('Amazon Settlement') < RETRY_ORDER.indexOf('CompanyDB財務(Amazon)') && RETRY_ORDER.includes('Amazon決済と財務'));
  } finally { if (saved === undefined) delete process.env.CDB_FINANCE_COORDINATOR; else process.env.CDB_FINANCE_COORDINATOR = saved; }
  // daily-sync の配線 (import すると main が走るので本文で確かめる): 工程はスイッチの部品から取る・順 = 取込 → 手数料 → 送り手 → 突き合わせ
  const src = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/daily-sync.js'), 'utf8');
  const iSw = src.indexOf('const financeCoordinator = financeCoordinatorEnabled();'), iSettle = src.indexOf(': runScript(settleStep.cmd, settleStep.label, settleStep.timeoutMs);'),
    iFees = src.indexOf("'Amazonアカウントフィー sync', 300000"), iPush = src.indexOf('cdbFinanceResult = runScript(pushStep.cmd, pushStep.label, pushStep.timeoutMs);'),
    iRec = src.indexOf("'apps/company-db/push/amazon-finance.mjs --reconcile --require-backfilled'");
  assert.ok(iSw > 0 && iSettle > iSw && iFees > iSettle && iPush > iFees && iRec > iPush, `${iSw} ${iSettle} ${iFees} ${iPush} ${iRec}`);
  assert.ok(src.includes('const settleStep = settlementStep({ coordinator: financeCoordinator });') && src.includes('const pushStep = financePushStep(businessDate, { coordinator: financeCoordinator });'));
  // 一方向 (#1567 Codex R6 High): 切り替え済みでスイッチが無い朝は今までの 2 工程を起動しない (取込は ❌・送り手はその見送りで ⏭️)
  assert.match(src, /const legacyGate = financeCoordinator \? null : await legacyGateCheck\(\{ dataDir: process\.env\.DATA_DIR \|\| path\.join\(PROJECT_DIR, 'data'\) \}\);\s*const settlementResult = legacyGate && !legacyGate\.allowed\s*\? \{ success: false, summary: `❌ \$\{legacyGate\.message\}` \}/, '切り替え済み・判定できない朝は今までの 2 工程を起動しない (ローカル + Render・#1567 Codex R7)');
  // スイッチが無い朝 = master と同じ: 取込の結果の名前 (warn なし)・取込が失敗した朝は送らずに ⏭️ (retry に載せる)
  assert.match(src, /results\.push\(financeCoordinator \? \{ name: settleStep\.name, \.\.\.settlementResult, warn: settlementResult\.success && isWarnSummary\(settlementResult\.summary\) \} : \{ name: settleStep\.name, \.\.\.settlementResult \}\);/);
  assert.match(src, /if \(pushStep && settlementResult\.success\) \{[\s\S]{0,300}\} else if \(pushStep\) \{\s*cdbFinanceResult = \{ success: false, summary: '⏭️ skipped \(Amazon Settlement の取込が失敗。取込の再試行が成功したら送る\)' \};/);
  // 突き合わせの条件: スイッチがある = coordinator の記録の構造の値 (#1567 R1 L3・R2 L2・R3 L2) / 無い = 今までどおり送り手が成功して ⏭️ でない朝
  assert.match(src, /const financeSent = financeCoordinator\s*\? coordinatorPushedFinance\(process\.env\.DATA_DIR, process\.env\.DAILY_SYNC_RUN_ID\)\s*: !!\(cdbFinanceResult && cdbFinanceResult\.success && !String\(cdbFinanceResult\.summary \|\| ''\)\.trimStart\(\)\.startsWith\('⏭️'\)\);/);
  assert.match(src, /j\.finance_pushed === true && j\.finance_push_ok === true && \(runId == null \|\| j\.daily_sync_run_id === runId\)/);
  assert.match(src, /if \(financeSqliteFresh && financeSent\) \{\s*const cdbFinanceRecResult/);
  assert.match(src, /const financeSqliteFresh = financeBuildFailed\.length === 0 && accountFeesBuildResult\.success;/);
  assert.match(src, /financeFailed\.push\(month\);\s*financeBuildFailed\.push\(month\);/);   // build の失敗だけを数える (sync の失敗は SQLite に関係しない)
  const retryable = JSON.parse(`[${/const RETRYABLE_JOBS = \[([^\]]*)\]/.exec(src)[1].replace(/'/g, '"')}]`);
  assert.ok(['Amazon決済と財務', 'Amazon Settlement', 'CompanyDB財務(Amazon)'].every((x) => retryable.includes(x)) && !retryable.includes('CompanyDB財務突合(Amazon)'), '両方の形の名前が retry の対象・突き合わせは載せない');
  const reg = fs.readFileSync(path.join(repoRoot, 'config/jobs-registry.mjs'), 'utf8');
  assert.ok(reg.includes('Amazon決済と財務') && reg.includes('amazon-finance-coverage-run.js') && reg.includes('Company DB Amazon 財務 突き合わせ'));
  assert.ok(reg.includes("id: 'cdb-finance-coordinator-switch'") && reg.includes('CDB_FINANCE_COORDINATOR'), '台帳にスイッチの一時物がある');
  // D7b-1a (#1554 Codex R1 Medium): 変換の版 v2 の注意 (migrate の前に pull しない・旧い版に戻さない・手の --full の完了の条件) が台帳にある
  for (const x of [AMAZON_FINANCE_TRANSFORM_VERSION, 'migrate の前に miniPC 本体を pull しない', '旧い版 (amazon_finance_v1) の送り手に戻さない', '手の --full の完了の条件']) assert.ok(reg.includes(x), `台帳に「${x}」が無い`);
});
await t('要約の頭: Render の復元・台帳の取り戻しは ⚠️ (daily-sync が全部 OK に数えない)・拾われない金額も ⚠️・失敗は ❌', async () => {
  const { summarizeFinance } = await import('../apps/company-db/push/amazon-finance.mjs');
  const base = { ok: true, dryRun: false, lockedBy: null, changed: 1, applied: 1, same: 0, stale: 0, failed: [], transformErrors: [], batchSeq: 1, chunks: 1, finance: { unmapped: { rows: 0, columns: {} }, unkeyed: [] } };
  assert.match(summarizeFinance(base), /^✅/);
  assert.match(summarizeFinance({ ...base, ledgerReset: 'receipt_missing:x/0' }), /^⚠️/);
  assert.match(summarizeFinance({ ...base, ledgerRebuilt: 5 }), /^⚠️/);
  assert.match(summarizeFinance({ ...base, finance: { ...base.finance, unmapped: { rows: 1, columns: { price: 1 }, exampleIds: ['1'] } } }), /^⚠️/);
  assert.match(summarizeFinance({ ...base, ok: false, ledgerReset: 'x' }), /^❌/);
});
await t('CLI: バックフィルの完了印の前は送らずに「⏭️ バックフィル前」(exit 0・Render に触れない)・印の後でも容量の上限が無ければ送らない (exit 1)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-af-cli-'));
  fs.copyFileSync(path.join(tmpDir, 'warehouse.db'), path.join(dir, 'warehouse.db'));
  const env = { ...process.env, DATA_DIR: dir, RENDER_MIRROR_URL: 'https://127.0.0.1:9/none', RENDER_PORTAL_URL: '', MIRROR_SYNC_KEY: 'k', CDB_DB_LIMIT_BYTES: '' };
  const cli = (args) => { try { return { code: 0, out: execFileSync(process.execPath, ['apps/company-db/push/amazon-finance.mjs', ...args], { cwd: repoRoot, env, encoding: 'utf8' }) }; } catch (e) { return { code: e.status, out: String(e.stdout || '') + String(e.stderr || '') }; } };
  try {
    for (const args of [['--incremental', '--require-backfilled'], ['--full', '--require-backfilled'], ['--reconcile', '--require-backfilled']]) {
      const r = cli(args);
      assert.equal(r.code, 0, r.out); assert.match(r.out.trim().split('\n').pop(), /^⏭️ Company DB Amazon 財務: 初回のバックフィル前/);
    }
    const l = openLedger(dir, { kind: FINANCE_KIND }); l.putMeta(META.backfill, '1'); l.close();
    // 🆕 D7b-1b-3: スイッチ (CDB_FINANCE_COORDINATOR=1) があるとき、送る回は coordinator の中だけ (単独は dry-run)
    const envOn = { ...env, CDB_FINANCE_COORDINATOR: '1' };
    const cliOn = (args) => { try { return { code: 0, out: execFileSync(process.execPath, ['apps/company-db/push/amazon-finance.mjs', ...args], { cwd: repoRoot, env: envOn, encoding: 'utf8' }) }; } catch (e) { return { code: e.status, out: String(e.stdout || '') + String(e.stderr || '') }; } };
    const r = cliOn(['--incremental', '--require-backfilled']);
    assert.equal(r.code, 1); assert.match(r.out, /coordinator/);
    // スイッチが無いとき = 今までの送り手の道 (coordinator だけの拒みは出ない) → 送る前の一方向の門 (#1567 Codex R7) = Render (この試験は届かない https) を読めない = 判定できない = 送らずに ❌
    const r0 = cli(['--incremental', '--require-backfilled']);
    assert.equal(r0.code, 1); assert.match(r0.out, /判定できない[\s\S]*今までの送り手/); assert.doesNotMatch(r0.out, /coordinator \(node apps/);
    const rr = cli(['--from', '2026-01-01', '--to', '2026-01-31']);
    assert.equal(rr.code, 1); assert.match(rr.out, /判定できない[\s\S]*単独の --from\/--to の送信/);   // 🆕 #1567 Codex R7 High 2: range も送る前に同じ門 (容量の上限より先)
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
await t('月の手数料のやり残し: 60 か月より古い月は範囲の外 = 消さずに warn・読めないファイルは warn', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-af-pending-'));
  fs.writeFileSync(path.join(dir, ACCOUNT_FEES_PENDING_FILE), JSON.stringify({ months: ['2019-01', '2026-01'] }));
  const p = accountFeesMonthsBack(dir, { currentMonth: '2026-09' });
  assert.equal(p.months, 60); assert.equal(p.warn, true); assert.deepEqual(p.covered, ['2026-01']);
  fs.writeFileSync(path.join(dir, ACCOUNT_FEES_PENDING_FILE), '{');
  const q = accountFeesMonthsBack(dir, { currentMonth: '2026-09' });
  assert.equal(q.months, 14); assert.equal(q.warn, true);
  fs.rmSync(dir, { recursive: true, force: true });
});
await t('daily-sync: 手数料の build / sync に やり残しの月数を渡し、両方が通った後に covered を消す', async () => {
  const src = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/daily-sync.js'), 'utf8');
  assert.match(src, /rebuild-amazon-account-fees\.js --data-dir \$\{DATA_DIR_ARG\} \$\{feesRange\}/);
  assert.match(src, /sync-amazon-account-fees\.js --data-dir \$\{DATA_DIR_ARG\} \$\{feesRange\}/);
  assert.match(src, /const feesRange = feesPlan\.fromMonth \? `--from-month \$\{feesPlan\.fromMonth\}`/);
  assert.match(src, /accountFeesSyncResult\.success && feesPlan\.covered\.length[\s\S]{0,200}attempted: feesPlan\.covered, file: ACCOUNT_FEES_PENDING_FILE/);
});

await t('🚨 変換が作る payload は全部 JS と SQL の等式を通る (#1554 Codex R2 Medium 2): 乱数の決済の行 400 注文 (取引の種類・price / 手数料の種類・9 つの金額の列・SKU の有無・注文番号なし) → 集約 → 0047 の apply (CHECK)', async () => {
  const { orderFinanceChecksum } = await import('../apps/company-db/finance/order-finance-checksum.mjs');
  let seed = 20260930; const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
  const pick = (a) => a[Math.floor(rnd() * a.length)];
  const TX = ['Order', 'Order', 'Refund', 'Chargeback Refund', 'A-to-z Guarantee Refund', 'Other', 'SAFE-T Reimbursement', 'WAREHOUSE_DAMAGE', 'WAREHOUSE_LOST', 'REVERSAL_REIMBURSEMENT',
    'PAYMENT_RETRACTION_ITEMS', 'Storage Fee', 'FBA Inventory Storage Fee', 'StorageRenewalBilling', 'Subscription Fee', 'Amazon Easy Ship Charges', 'Fee Adjustment', 'Inbound Defect Fee - x',
    'FBA Removal Order: Return Fee', 'FBA Inventory Fee - LowInventoryLevel', 'Current Reserve Amount', 'Goodwill Concession', 'BuyerRecharge', 'Refund_Retrocharge', 'Mystery Fee'];
  const PT = [null, null, 'Principal', 'Shipping', 'GiftWrap', 'Tax', 'ShippingTax', 'RestockingFee', 'SAFE-T Reimbursement', 'Weird'];
  const FT = [null, null, 'Commission', 'RefundCommission', 'FBAPerUnitFulfillmentFee', 'ShippingChargeback', 'PointsGranted', 'MFNPostageFee', 'EasyShipFee', 'VariableClosingFee'];
  const PR = [null, null, 'Principal', 'Shipping', 'TaxDiscount'];
  const amt = () => (rnd() < 0.3 ? BigInt(Math.floor(rnd() * 1001) - 500) * 1000000n : null);
  let id = 900000, checked = 0, withClass = 0, feeRows = 0;
  for (let o = 0; o < 400; o++) {
    const pseudo = rnd() < 0.3;
    const date = `2031-0${1 + Math.floor(rnd() * 9)}-1${Math.floor(rnd() * 9)}`;
    const orderNo = pseudo ? `-:${date}` : `FZ-${o}`;
    const rows = [];
    for (let k = 0, n = 1 + Math.floor(rnd() * 8); k < n; k++) {
      const sku = rnd() < 0.5 ? null : pick(['fz-a', 'fz-b', 'FZ-A']);
      rows.push({ id: ++id, source_settlement_id: 'SFZ', business_line_key: `fz-${id}`, source_document_id: 'DFZ', source_line_no: id, source_layer: 'sp_api_v2', ingested_at: OLD_INGEST, posted_date_utc: 'x',
        economic_date: pseudo ? date : (rnd() < 0.8 ? date : `2031-0${1 + Math.floor(rnd() * 9)}-20`), amazon_order_id: pseudo ? null : orderNo, seller_sku_normalized: sku, transaction_type: pick(TX), currency: 'JPY',
        quantity_purchased: rnd() < 0.3 ? 1n : null, price_type: pick(PT), price_amount_micro: amt(), item_related_fee_type: pick(FT), item_related_fee_amount_micro: amt(),
        promotion_type: pick(PR), promotion_amount_micro: amt(), shipment_fee_amount_micro: rnd() < 0.1 ? amt() : null, order_fee_amount_micro: rnd() < 0.05 ? amt() : null,
        misc_fee_amount_micro: amt(), other_fee_amount_micro: amt(), direct_payment_amount_micro: rnd() < 0.05 ? amt() : null, other_amount_micro: amt() });
    }
    const { lines } = aggregateOrderFinance(orderNo, rows);   // JS の等式 (validateFinanceRows) はこの中で通る
    for (const l of lines) { if (l.unclassified_component_count) withClass++; if (l.line_kind !== 'sku' && l.line_kind !== 'not_account_fee' && l.line_kind !== 'unknown') feeRows++; }
    // 同じ疑似注文 (同じ日) が 2 回出てもよいように世代は注文ごとに進める
    const r = (await one(`select core.apply_order_finance_batch(1::smallint, 'amazon', 'jp', $1, $5::bigint, $2, $3, $4::jsonb) as r`,
      [orderNo, orderFinanceChecksum(lines), AMAZON_FINANCE_TRANSFORM_VERSION, JSON.stringify(lines), 900000 + 2 * o + 1])).r;
    assert.equal(r, 'applied', orderNo);
    checked++;
    await one(`select core.apply_order_finance_batch(1::smallint, 'amazon', 'jp', $1, $4::bigint, $2, $3, '[]'::jsonb) as r`, [orderNo, orderFinanceChecksum([]), AMAZON_FINANCE_TRANSFORM_VERSION, 900000 + 2 * o + 2]);
  }
  assert.equal(checked, 400);
  assert.ok(withClass > 50 && feeRows > 50, `分けられない部品のある行 ${withClass}・月の手数料の行 ${feeRows} (場面が薄い)`);
});

server.close();
try { wdb.close(); } catch { /* */ }
fs.rmSync(tmpDir, { recursive: true, force: true });
console.log(`\n${ng === 0 ? '✅' : '❌'} Amazon 財務の送り手: ok ${ok} / NG ${ng}`);
process.exitCode = ng === 0 ? 0 : 1;
