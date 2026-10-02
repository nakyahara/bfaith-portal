import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-settlement-dedup-occurrence.js — 決済の行の重複除去 (出現順つき) の試験
 *
 * 2026-09-28: 同じ決済の中で business_line_key が同じ行は本物の別々の行 (全部の行の合計が振込額と 1 円まで一致)。
 * (決済, 鍵) だけで 1 行にすると 2 週間ごとに 55〜65 万円・約 1,260 個を数え落としていた。
 * → 4 か所 (db.js の v_amazon_settlement_unified / rebuild-amazon-settlement-mart.js / rebuild-amazon-account-fees.js /
 *   sql/amazon/build_f_amazon_finance_sku_daily_v1.sql) で「同じ文書の中の出現順」を鍵に足した。ここで 4 か所とも:
 *   ① 同じ鍵の本物の 2 行を 2 行として数える
 *   ② 同じ決済のレポートが 2 本 (同じ期間の V1 が 2 本) / V1 と V2 の両方 / 過去の膨張の残骸 (同じ文書の同じ行が 2 行) は 1 回だけ数える
 *   = どの集計も振込額と一致
 *   + V2 だけで入った決済も 4 か所で振込額どおり / 返金の行 / 作り直しても原価の snapshot は残り、個数・原価の合計・利益だけ直る (Codex #1511 R1)
 *
 * 実行: node apps/warehouse/test-settlement-dedup-occurrence.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'settlement-occ-test-'));
process.env.DATA_DIR = tmpDir;
const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');

const { initDB, getDB } = await import('./db.js');
const { prepareReportTsv, prepareV2ReportTsv, ingestSettlement } = await import('./fetch-amazon-settlements.js');
const { V1_COLUMNS, V2_COLUMNS } = await import('./amazon-settlement-v2.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const tsvOf = (cols, rows) => [cols.join('\t'), ...rows.map((r) => cols.map((c) => r[c] ?? '').join('\t'))].join('\n') + '\n';

// 当月の日付 (手数料の集計は「今日から N か月」で絞るので、当月にする)
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const Y = nowJst.getUTCFullYear(), M = String(nowJst.getUTCMonth() + 1).padStart(2, '0');
const YM = `${Y}-${M}`, YMI = Number(`${Y}${M}`);
const P = `${YM}-02T01:00:00+00:00`, P2 = `${YM.replace('-', '/')}/02 01:00:00 UTC`;
const S = 'S-OCC-1';
// V1: 同じ注文・同じ品物・同じ時刻で「本体 1,000 円」と「個数 1」が 2 行ずつ (本物の別々の行) + 別の品物 500 円 + 保管料 (SKU なし) が同じ中身で 2 行
const line = (o) => ({ 'settlement-id': S, 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH', 'marketplace-name': 'Amazon.co.jp', 'fulfillment-id': 'AFN', 'posted-date': P, 'order-item-code': 'OI1', sku: 'SKU-A', ...o });
const V1_ROWS = [
  { 'settlement-id': S, 'settlement-start-date': `${YM}-01T00:00:00+00:00`, 'settlement-end-date': `${YM}-15T00:00:00+00:00`, 'deposit-date': `${YM}-17T00:00:00+00:00`, 'total-amount': '1800.00', currency: 'JPY' },
  line({ 'price-type': 'Principal', 'price-amount': '1000.00' }),
  line({ 'quantity-purchased': '1' }),
  line({ 'price-type': 'Principal', 'price-amount': '1000.00' }),
  line({ 'quantity-purchased': '1' }),
  line({ 'order-item-code': 'OI2', 'price-type': 'Principal', 'price-amount': '500.00' }),
  line({ 'order-item-code': 'OI2', 'quantity-purchased': '1' }),
  line({ 'transaction-type': 'Refund', 'adjustment-id': 'AD1', 'shipment-id': '', 'price-type': 'Principal', 'price-amount': '-300.00' }),
  { 'settlement-id': S, 'transaction-type': 'Storage Fee', 'posted-date': P, 'other-amount': '-200.00' },
  { 'settlement-id': S, 'transaction-type': 'Storage Fee', 'posted-date': P, 'other-amount': '-200.00' },
];
const TOTAL_MICRO = 1800 * 1e6;   // 1000 + 1000 + 500 - 300 (返金) - 200 - 200
const V1_TSV = tsvOf(V1_COLUMNS, V1_ROWS);
// 同じ中身の V2
const v2 = (o) => ({ 'settlement-id': S, 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH', 'marketplace-name': 'Amazon.co.jp', 'fulfillment-id': 'AFN', 'posted-date': P2.slice(0, 10), 'posted-date-time': P2, 'order-item-code': 'OI1', sku: 'SKU-A', 'quantity-purchased': '1', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', ...o });
const V2_TSV = tsvOf(V2_COLUMNS, [
  { 'settlement-id': S, 'settlement-start-date': `${YM.replace('-', '/')}/01 00:00:00 UTC`, 'settlement-end-date': `${YM.replace('-', '/')}/15 00:00:00 UTC`, 'deposit-date': `${YM.replace('-', '/')}/17 00:00:00 UTC`, 'total-amount': '1800.00', currency: 'JPY' },
  v2({ amount: '1000.00' }), v2({ amount: '1000.00' }), v2({ 'order-item-code': 'OI2', amount: '500.00' }),
  v2({ 'transaction-type': 'Refund', 'adjustment-id': 'AD1', 'shipment-id': '', 'quantity-purchased': '', amount: '-300.00' }),
  ...[1, 2].map(() => ({ 'settlement-id': S, 'transaction-type': 'other-transaction', 'marketplace-name': '', 'posted-date': P2.slice(0, 10), 'posted-date-time': P2, 'amount-type': 'other-transaction', 'amount-description': 'Storage Fee', amount: '-200.00' })),
]);

await initDB();
const db = getDB();
const ingest = (p) => ingestSettlement(db, p.headerRow, p.lineRows, p.ctx);
const AMT = `COALESCE(price_amount_micro,0) + COALESCE(item_related_fee_amount_micro,0) + COALESCE(promotion_amount_micro,0) + COALESCE(other_amount_micro,0)`;
const viewSum = () => db.prepare(`SELECT COUNT(*) n, SUM(${AMT}) a, SUM(COALESCE(quantity_purchased,0)) q FROM v_amazon_settlement_unified WHERE source_settlement_id = ?`).get(S);
const runNode = (args) => execFileSync(process.execPath, args, { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
const martCheck = () => {
  runNode(['apps/warehouse/rebuild-amazon-settlement-mart.js', '--ym', String(YMI)]);
  return db.prepare(`SELECT qty_ordered q, sales_principal_micro p, refund_principal_micro r FROM fact_amazon_settlement_monthly_wide WHERE year_month_int = ? AND seller_sku_normalized = 'sku-a'`).get(YMI);
};
const feesCheck = () => {
  runNode(['apps/warehouse/rebuild-amazon-account-fees.js', '--data-dir', tmpDir, '--months', '1']);
  return db.prepare(`SELECT amount_jpy a, row_count n FROM f_amazon_account_fees_monthly_v1 WHERE month_start_jst = ? AND fee_type = 'storage'`).get(`${YM}-01`);
};
const financeCheck = () => {
  runNode(['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM]);
  return db.prepare(`SELECT units_ordered q, sales_principal_jpy p, refund_principal_jpy r FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-a'`).get();
};
const { refreshStaleVersionDetails } = await import('./amazon-settlement-versions.js');
const expectAll = (label) => {
  refreshStaleVersionDetails(db);   // 行を直接足した (残骸) = 版の要約が古い = build は止まる → coordinator の回と同じく作り直してから (2026-10-01 #1567 R1 High 1)
  const v = viewSum();
  ok(v.n === 9 && v.a === TOTAL_MICRO && v.q === 3, `${label}: 表示用の集まり (v_amazon_settlement_unified) = 9 行・振込額 1,800 円・個数 3 (${v.n} 行 / ${v.a / 1e6} 円 / ${v.q} 個)`);
  const m = martCheck();
  ok(m && m.q === 3 && m.p === 2500 * 1e6 && m.r === -300 * 1e6, `${label}: 月の集計 = 個数 3・本体 2,500 円・返金 -300 円 (${m && m.q} 個 / ${m && m.p / 1e6} 円 / ${m && m.r / 1e6} 円)`);
  const f = feesCheck();
  ok(f && f.a === -400 && f.n === 2, `${label}: アカウント単位の手数料 = 保管料 -400 円 (2 行) (${f && f.a} 円 / ${f && f.n} 行)`);
  const d = financeCheck();
  ok(d && d.q === 3 && d.p === 2500 && Math.abs(d.r) === 300, `${label}: 日次の財務の集計 = 個数 3・本体 2,500 円・返金 300 円 (${d && d.q} 個 / ${d && d.p} 円 / ${d && d.r} 円)`);
};

// ⓪ V2 だけ (V1 に補われずに、V2 だけで 4 か所とも振込額どおり)
ingest(prepareV2ReportTsv(V2_TSV, 'R-V2', 'run0'));
expectAll('V2 だけ');
// 作り直しても原価の snapshot は残る: 前の数え方で作った行 (個数 1・原価 100 円の snapshot) を置いて、作り直す
db.prepare(`UPDATE f_amazon_finance_sku_daily_v1 SET unit_cost_snapshot = 100, cost_snapshot_date_jst = '2026-01-01', units_ordered = 1, cogs_amount = 100 WHERE seller_sku = 'sku-a'`).run();
financeCheck();
const snap = db.prepare(`SELECT unit_cost_snapshot u, cost_snapshot_date_jst d, units_ordered q, units_refunded_customer rq, units_a_to_z_refund aq, cogs_amount c, profit_amount pr, sales_principal_jpy sp, refund_principal_jpy rp FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-a'`).get();
ok(snap.u === 100 && snap.d === '2026-01-01' && snap.q === 3, `作り直しても原価の snapshot (100 円・2026-01-01) は残り、個数は 3 に直る (${JSON.stringify(snap)})`);
ok(snap.c === 100 * (snap.q - snap.rq - snap.aq), `原価の合計 = 残った snapshot 100 円 × 新しい個数 (注文 ${snap.q} − 返金の推定 ${snap.rq + snap.aq}) = ${snap.c} 円`);
ok(snap.pr === snap.sp - snap.rp - snap.c, `利益も新しい数で直る = 本体 ${snap.sp} − 返金 ${snap.rp} − 原価の合計 ${snap.c} = ${snap.pr} 円 (この試験ではほかの金額は 0)`);
// ① V1 を 1 本 (V2 の後に V1 も入った)
//   (#1582 の「V2 で入れた決済に V1 を入れない (skipped_v2)」は #1567 では採らない = 版として入り、下流は決済ごとに採った版 1 つ。test-settlement-v2.js の古い Easy Ship の試験)
ingest(prepareReportTsv(V1_TSV, 'R-V1-a', 'run1'));
expectAll('V2 + V1');
// ② 同じ決済のレポートがもう 1 本 (同じ期間の V1 が 2 本)
ingest(prepareReportTsv(V1_TSV, 'R-V1-b', 'run2'));
expectAll('V2 + 同じ決済の V1 が 2 本');

// ④ 過去の膨張の残骸 (同じ文書の同じ行が別の physical_line_hash で 2 行目)
const src = db.prepare(`SELECT * FROM raw_amazon_settlement_lines WHERE source_document_id = 'R-V1-a' ORDER BY source_line_no`).all();
const cols = Object.keys(src[0]).filter((c) => c !== 'id');
const ins = db.prepare(`INSERT INTO raw_amazon_settlement_lines (${cols.join(',')}) VALUES (${cols.map((c) => '@' + c).join(',')})`);
for (const r of src) { const { id, ...rest } = r; ins.run({ ...rest, physical_line_hash: rest.physical_line_hash + '-old', ingest_run_id: 'old-run', ingested_at: '2026-01-01 00:00:00' }); }
expectAll('過去の膨張の残骸 (同じ行が 2 回)');

// (過去の作り直しのスクリプト rebuild-amazon-settlement-history.js の試験は、スクリプトと一緒に 2026-09-29 に消した = 1 回きりの作り直しは済んだ・照合 17/17)

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== 出現順つき重複除去テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
