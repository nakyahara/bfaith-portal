/**
 * test-settlement-idempotency.js — Amazon settlement ingest の冪等性 smoke テスト
 * (設計監査 2026-07-06 PR-13 / INV-27。PR #229「31.8M行膨張」再発の恒久防御)
 *
 * 本番と同一の関数群 (fetch-amazon-settlements.js の prepareReportTsv / ingestSettlement /
 * makePhysicalHash / makeBusinessKey / parseAmount) を、使い捨ての一時 DATA_DIR に
 * initDB() した実スキーマ相手に叩く。**ハッシュ設計が1箇所でも崩れたら即検知**する。
 *
 * 8 パターン:
 *   1. 同一TSV 2回投入 (同run)      → 2回目は 0 行挿入
 *   2. キー順違いオブジェクト        → physical hash 同一 (canonicalize)
 *   3. 列欠落 vs 空文字             → business key 同一 (normalize で両方 null)
 *   4. 別 reportId で同一内容        → 別物理行として挿入 (文書単位dedupの仕様固定)
 *   5. header 行なし TSV            → lines は投入される (headers=0、エラーなし)
 *   6. 未知 transaction-type        → 行は落とされず dim に自動追加
 *   7. 金額表記ゆれ '123.45'/'123.450' → 同一 micro・同一 business key
 *   8. 別 run_id で同一TSV 再投入    → 0 行挿入 (PHYSICAL_HASH_EXCLUDE の回帰 = PR #229 の核心)
 *
 * 実行: node apps/warehouse/test-settlement-idempotency.js (daily-sync 冒頭でも実行)
 * 本番 DB には一切触れない (一時ディレクトリに専用 warehouse.db を作り、終了時に削除)。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// ★ db.js は import 時に DATA_DIR を読むため、動的 import より前に一時 DATA_DIR を設定
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'settlement-idem-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('./db.js');
const { prepareReportTsv, ingestSettlement, makePhysicalHash, makeBusinessKey, parseAmount } =
  await import('./fetch-amazon-settlements.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

// ─── TSV 合成ヘルパ (実レポートの列名) ───
const COLS = [
  'settlement-id', 'settlement-start-date', 'settlement-end-date', 'deposit-date',
  'total-amount', 'currency', 'transaction-type', 'order-id', 'merchant-order-id',
  'adjustment-id', 'shipment-id', 'marketplace-name', 'fulfillment-id', 'posted-date',
  'order-item-code', 'sku', 'quantity-purchased', 'price-type', 'price-amount',
  'promotion-id', 'promotion-type', 'promotion-amount',
];
function tsvOf(rows, cols = COLS) {
  const lines = [cols.join('\t')];
  for (const r of rows) lines.push(cols.map(c => r[c] ?? '').join('\t'));
  return lines.join('\n') + '\n';
}
const HEADER = { 'settlement-id': 'S001', 'settlement-start-date': '2099-01-01', 'settlement-end-date': '2099-01-14', 'deposit-date': '2099-01-16', 'total-amount': '1000.00', currency: 'JPY' };
const LINE = (over = {}) => ({
  'settlement-id': 'S001', 'transaction-type': 'Order', 'order-id': '123-4567890-0000001',
  'marketplace-name': 'Amazon.co.jp', 'fulfillment-id': 'AFN', 'posted-date': '2099-01-05T01:00:00+00:00',
  'order-item-code': 'OI1', sku: 'SKU-A', 'quantity-purchased': '1',
  'price-type': 'Principal', 'price-amount': '1000.00', currency: 'JPY', ...over,
});

await initDB();
const db = getDB();
const counts = () => ({
  h: db.prepare('SELECT COUNT(*) c FROM raw_amazon_settlement_headers').get().c,
  l: db.prepare('SELECT COUNT(*) c FROM raw_amazon_settlement_lines').get().c,
});

try {
  // ─── P1: 同一TSV 2回投入 (同run) ───
  const tsv1 = tsvOf([HEADER, LINE(), LINE({ 'order-item-code': 'OI2', sku: 'SKU-B' })]);
  const p1a = prepareReportTsv(tsv1, 'RPT-001', 'run-1');
  const r1a = ingestSettlement(db, p1a.headerRow, p1a.lineRows, p1a.ctx);
  ok(r1a.headerInserted === 1 && r1a.lineInserted === 2, `P1 初回投入: header=1, lines=2 (実際 ${r1a.headerInserted}/${r1a.lineInserted})`);
  const p1b = prepareReportTsv(tsv1, 'RPT-001', 'run-1');
  const r1b = ingestSettlement(db, p1b.headerRow, p1b.lineRows, p1b.ctx);
  const c1 = counts();
  ok(r1b.headerInserted === 0 && r1b.lineInserted === 0 && c1.h === 1 && c1.l === 2,
    `P1 同一TSV再投入: 0挿入・行数不変 (実際 +${r1b.headerInserted}/+${r1b.lineInserted}, 計${c1.h}/${c1.l})`);

  // ─── P8: 別 run_id (observed_at も変わる) で同一TSV → 0挿入 = PR #229 の核心 ───
  const p8 = prepareReportTsv(tsv1, 'RPT-001', `run-2-${Math.random().toString(36).slice(2)}`);
  const r8 = ingestSettlement(db, p8.headerRow, p8.lineRows, p8.ctx);
  const c8 = counts();
  ok(r8.headerInserted === 0 && r8.lineInserted === 0 && c8.l === 2,
    `P8 別run_id再投入: 0挿入 (PHYSICAL_HASH_EXCLUDE回帰=PR #229防御。実際 +${r8.headerInserted}/+${r8.lineInserted}, 計${c8.l}行)`);

  // ─── P2: キー順違いオブジェクト → physical hash 同一 ───
  const row = p1a.lineRows[0];
  const reversed = {};
  for (const k of Object.keys(row).reverse()) reversed[k] = row[k];
  ok(makePhysicalHash(row, 'RPT-001', 1) === makePhysicalHash(reversed, 'RPT-001', 1),
    'P2 キー順違い: physical hash 同一 (canonicalize)');

  // ─── P3: 列欠落 vs 空文字 → business key 同一 (normalize で両方 null) ───
  const withEmptyPromo = prepareReportTsv(tsvOf([LINE()]), 'RPT-P3', 'run-p3');
  const withoutPromoCol = prepareReportTsv(tsvOf([LINE()], COLS.filter(c => !c.startsWith('promotion'))), 'RPT-P3', 'run-p3');
  ok(withEmptyPromo.lineRows[0].business_line_key === withoutPromoCol.lineRows[0].business_line_key,
    'P3 promotion列欠落 vs 空文字: business key 同一 (normalizeで両方null)');

  // ─── P4: 別 reportId で同一内容 → 別物理行 (文書単位dedupの仕様固定) ───
  const before4 = counts().l;
  const p4 = prepareReportTsv(tsv1, 'RPT-002', 'run-4');
  const r4 = ingestSettlement(db, p4.headerRow, p4.lineRows, p4.ctx);
  ok(r4.lineInserted === 2 && p4.lineRows[0].business_line_key === p1a.lineRows[0].business_line_key,
    `P4 別reportId同一内容: 別物理行として+2挿入 (business keyは同一=mart側で論理dedup可能。実際+${r4.lineInserted})`);
  ok(counts().l === before4 + 2, `  行数 ${before4}→${counts().l}`);

  // ─── P5: header 行なし TSV → lines は投入される ───
  const p5 = prepareReportTsv(tsvOf([LINE({ 'settlement-id': 'S005', 'order-id': '123-4567890-0000005' })]), 'RPT-005', 'run-5');
  const r5 = ingestSettlement(db, p5.headerRow, p5.lineRows, p5.ctx);
  ok(p5.headerRow === null && r5.headerInserted === 0 && r5.lineInserted === 1,
    'P5 headerなしTSV: lines投入・エラーなし (headers/linesはFK無しの独立表)');

  // ─── P6: 未知 transaction-type → 行は落とされず dim に自動追加 ───
  const p6 = prepareReportTsv(tsvOf([LINE({ 'transaction-type': 'FutureNewType2099', 'order-id': '123-4567890-0000006' })]), 'RPT-006', 'run-6');
  const r6 = ingestSettlement(db, p6.headerRow, p6.lineRows, p6.ctx);
  const dimHit = db.prepare("SELECT COUNT(*) c FROM dim_amazon_transaction_type WHERE transaction_type = 'FutureNewType2099'").get().c;
  ok(r6.lineInserted === 1 && dimHit === 1, 'P6 未知transaction-type: 行保存+dim自動追加 (無音drop無し)');

  // ─── P7: 金額表記ゆれ → 同一 micro・同一 business key ───
  ok(parseAmount('123.45') === 123450000 && parseAmount('123.450') === 123450000,
    'P7 parseAmount: 123.45 と 123.450 → 同一micro (123,450,000)');
  const p7a = prepareReportTsv(tsvOf([LINE({ 'price-amount': '123.45' })]), 'RPT-007', 'run-7');
  const p7b = prepareReportTsv(tsvOf([LINE({ 'price-amount': '123.450' })]), 'RPT-007b', 'run-7');
  ok(p7a.lineRows[0].business_line_key === p7b.lineRows[0].business_line_key,
    '  表記ゆれTSV: business key 同一 (論理同一と判定)');
} finally {
  try { db.close(); } catch {}
  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (e) {
    console.warn(`一時ディレクトリ削除失敗 (無害): ${e.message}`);
  }
}

console.log(failed === 0 ? '\n=== 冪等性テスト ALL PASS (8パターン) ===' : `\n=== ${failed} 件 FAILED ===`);
process.exitCode = failed === 0 ? 0 : 1;
