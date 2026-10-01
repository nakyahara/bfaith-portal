import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-settlement-v2.js — 決済レポート V2 → V1 の形への並べ直し (amazon-settlement-v2.js) の試験
 *
 * 2026-09-28 に本番の V1 / V2 を 6 期間 突き合わせて決めた規則を、作り物の行で固定する:
 *   V2 を並べ直して正規化した business_line_key の集まりが、同じ中身の V1 と一致すること (= 下流の重複除去で同じ行になる)
 *   + 規則に無い組み合わせを落とさず数える / 日時・列の形が違えば止める / V1 で取込済みの決済は V2 で入れない / 両方入っても二重にならない
 *
 * 実行: node apps/warehouse/test-settlement-v2.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'settlement-v2-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('./db.js');
const { prepareReportTsv, prepareV2ReportTsv, ingestSettlement, settlementIngestedByV1, processV2Report } = await import('./fetch-amazon-settlements.js');
const { V1_COLUMNS, V2_COLUMNS, v2DateTimeToV1, convertV2TsvToV1Tsv } = await import('./amazon-settlement-v2.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const throws = (fn, re, label) => { try { fn(); ok(false, label + ' (止まらなかった)'); } catch (e) { ok(re.test(String(e.message)), label + ` (${e.message})`); } };
const tsvOf = (cols, rows) => [cols.join('\t'), ...rows.map((r) => cols.map((c) => r[c] ?? '').join('\t'))].join('\n') + '\n';

const S = 'S900';
const T1 = '2099/01/05 01:00:00 UTC', T1v1 = '2099-01-05T01:00:00+00:00';
const T2 = '2099/01/06 02:00:00 UTC', T2v1 = '2099-01-06T02:00:00+00:00';
const T3 = '2099/01/07 03:00:00 UTC', T3v1 = '2099-01-07T03:00:00+00:00';
const v2 = (over) => ({ 'settlement-id': S, currency: '', 'marketplace-name': 'Amazon.co.jp', 'posted-date': (over['posted-date-time'] || T1).slice(0, 10), 'posted-date-time': T1, ...over });
const v1 = (over) => ({ 'settlement-id': S, 'marketplace-name': 'Amazon.co.jp', 'posted-date': T1v1, ...over });

// ── V2 (本番で見た形を作り物で) ──
const V2_ROWS = [
  { 'settlement-id': S, 'settlement-start-date': '2099/01/01 00:20:09 UTC', 'settlement-end-date': '2099/01/15 00:20:09 UTC', 'deposit-date': '2099/01/17 00:20:09 UTC', 'total-amount': '12345.00', currency: 'JPY' },
  // 注文 1 品物: 本体・税・手数料 2 つ・ポイント (V2 では品物の番号が空)
  ...['Principal:1000.00', 'Tax:100.00'].map((x) => v2({ 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH1', 'fulfillment-id': 'AFN', 'order-item-code': 'OI1', sku: 'SKU-A', 'quantity-purchased': '2', 'amount-type': 'ItemPrice', 'amount-description': x.split(':')[0], amount: x.split(':')[1] })),
  ...['Commission:-100.00', 'FBAPerUnitFulfillmentFee:-300.00'].map((x) => v2({ 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH1', 'fulfillment-id': 'AFN', 'order-item-code': 'OI1', sku: 'SKU-A', 'quantity-purchased': '2', 'amount-type': 'ItemFees', 'amount-description': x.split(':')[0], amount: x.split(':')[1] })),
  v2({ 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH1', 'fulfillment-id': 'AFN', 'order-item-code': '', sku: 'SKU-A', 'quantity-purchased': '', 'amount-type': 'Points', 'amount-description': 'PointsGranted', amount: '-28.00' }),
  v2({ 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH1', 'fulfillment-id': 'AFN', 'order-item-code': 'OI1', sku: 'SKU-A', 'quantity-purchased': '2', 'amount-type': 'Promotion', 'amount-description': 'Shipping', amount: '-50.00', 'promotion-id': 'P1' }),
  // 返金のポイント
  v2({ 'transaction-type': 'Refund', 'order-id': 'O2', 'adjustment-id': 'AD2', 'fulfillment-id': 'MFN', 'order-item-code': 'OI2', sku: 'SKU-B', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '-500.00', 'posted-date-time': T2 }),
  v2({ 'transaction-type': 'Refund', 'order-id': 'O2', 'adjustment-id': 'AD2', 'fulfillment-id': 'MFN', 'order-item-code': '', sku: 'SKU-B', 'amount-type': 'Points', 'amount-description': 'PointsReturned', amount: '5.00', 'posted-date-time': T2 }),
  // Easy Ship (本体 + 税 → 1 行・手数料の列)
  v2({ 'transaction-type': 'AmazonFees', 'order-id': 'O3', 'amount-type': 'Amazon Easy Ship Charges', 'amount-description': 'Base fee', amount: '-400.00' }),
  v2({ 'transaction-type': 'AmazonFees', 'order-id': 'O3', 'amount-type': 'Amazon Easy Ship Charges', 'amount-description': 'Tax on fee', amount: '-40.00' }),
  // 返送手数料: 本体 -115 + 割引 +60 + 税 -5 = -60 の 1 行 / 続けて別の手数料 (本体 -55 + 税 -5)
  ...[['Base fee', '-115.00'], ['Discount on Fee', '60.00'], ['Tax on fee', '-5.00'], ['Base fee', '-55.00'], ['Tax on fee', '-5.00']].map(([d, a]) => v2({ 'transaction-type': 'FBAFees', 'order-id': 'R1', 'amount-type': 'FBA Removal Order: Return Fee', 'amount-description': d, amount: a, 'posted-date-time': T3 })),
  // 倉庫の破損: 同じ中身が 2 行 (まとめない・個数は残す)
  ...[1, 2].map(() => v2({ 'transaction-type': 'other-transaction', sku: 'SKU-C', 'quantity-purchased': '1', 'amount-type': 'FBA Inventory Reimbursement', 'amount-description': 'WAREHOUSE_DAMAGE', amount: '277.00', 'marketplace-name': '' })),
  // SAFE-T・紛失の補てん
  v2({ 'transaction-type': 'Other', 'order-id': 'O4', 'adjustment-id': 'AD4', 'fulfillment-id': 'MFN', 'order-item-code': 'OI4', sku: 'SKU-D', 'amount-type': 'Other transactions', 'amount-description': 'Reimbursement for Lost packages', amount: '634.00' }),
  v2({ 'transaction-type': 'SAFE-T Reimbursement', 'order-id': 'O5', 'adjustment-id': 'AD5', 'fulfillment-id': 'MFN', 'order-item-code': 'OI5', sku: 'SKU-E', 'amount-type': 'Other transactions', 'amount-description': 'SAFE-T reimbursement', amount: '1279.00' }),
  // 保管料の取り消し・手数料の調整・その他
  v2({ 'transaction-type': 'other-transaction - Reversal', 'amount-type': 'other-transaction', 'amount-description': 'Storage Fee', amount: '12.00', 'marketplace-name': '' }),
  v2({ 'transaction-type': 'Fee Adjustment', 'order-id': 'O6', 'amount-type': 'Item Fee Adjustment', 'amount-description': 'FBA Pick & Pack Fee', amount: '106.00' }),
  v2({ 'transaction-type': 'other-transaction', 'amount-type': 'other-transaction', 'amount-description': 'Subscription Fee', amount: '-4900.00', 'marketplace-name': '' }),
];
// ── 同じ中身の V1 (本番の V1 の書き方) ──
const V1_ROWS = [
  { 'settlement-id': S, 'settlement-start-date': '2099-01-01T00:20:09+00:00', 'settlement-end-date': '2099-01-15T00:20:09+00:00', 'deposit-date': '2099-01-17T00:20:09+00:00', 'total-amount': '12345.00', currency: 'JPY' },
  ...[['price', 'Principal', '1000.00'], ['price', 'Tax', '100.00'], ['fee', 'Commission', '-100.00'], ['fee', 'FBAPerUnitFulfillmentFee', '-300.00'], ['fee', 'PointsGranted', '-28.00']].map(([k, t, a]) => v1({ 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH1', 'fulfillment-id': 'AFN', 'order-item-code': 'OI1', sku: 'SKU-A', ...(k === 'price' ? { 'price-type': t, 'price-amount': a } : { 'item-related-fee-type': t, 'item-related-fee-amount': a }) })),
  v1({ 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH1', 'fulfillment-id': 'AFN', 'order-item-code': 'OI1', sku: 'SKU-A', 'quantity-purchased': '2' }),
  v1({ 'transaction-type': 'Order', 'order-id': 'O1', 'merchant-order-id': 'O1', 'shipment-id': 'SH1', 'fulfillment-id': 'AFN', 'order-item-code': 'OI1', sku: 'SKU-A', 'promotion-id': 'P1', 'promotion-type': 'Shipping', 'promotion-amount': '-50.00' }),
  v1({ 'transaction-type': 'Refund', 'order-id': 'O2', 'adjustment-id': 'AD2', 'fulfillment-id': 'MFN', 'order-item-code': 'OI2', sku: 'SKU-B', 'price-type': 'Principal', 'price-amount': '-500.00', 'posted-date': T2v1 }),
  v1({ 'transaction-type': 'Refund', 'order-id': 'O2', 'adjustment-id': 'AD2', 'fulfillment-id': 'MFN', 'order-item-code': 'OI2', sku: 'SKU-B', 'item-related-fee-type': 'PointsReturned', 'item-related-fee-amount': '5.00', 'posted-date': T2v1 }),
  v1({ 'transaction-type': 'Amazon Easy Ship Charges', 'order-id': 'O3', 'item-related-fee-type': 'Amazon Easy Ship Charges', 'item-related-fee-amount': '-440.00' }),
  v1({ 'transaction-type': 'FBA Removal Order: Return Fee', 'order-id': 'R1', 'other-amount': '-60.00', 'posted-date': T3v1 }),
  v1({ 'transaction-type': 'FBA Removal Order: Return Fee', 'order-id': 'R1', 'other-amount': '-60.00', 'posted-date': T3v1 }),
  v1({ 'transaction-type': 'WAREHOUSE_DAMAGE', sku: 'SKU-C', 'quantity-purchased': '1', 'other-amount': '277.00', 'marketplace-name': '' }),
  v1({ 'transaction-type': 'WAREHOUSE_DAMAGE', sku: 'SKU-C', 'quantity-purchased': '1', 'other-amount': '277.00', 'marketplace-name': '' }),
  v1({ 'transaction-type': 'Other', 'order-id': 'O4', 'adjustment-id': 'AD4', 'fulfillment-id': 'MFN', sku: 'SKU-D', 'price-type': 'SAFE-T Reimbursement', 'other-amount': '634.00' }),
  v1({ 'transaction-type': 'SAFE-T Reimbursement', 'order-id': 'O5', 'adjustment-id': 'AD5', 'fulfillment-id': 'MFN', sku: 'SKU-E', 'price-type': 'SAFE-T Reimbursement', 'other-amount': '1279.00' }),
  v1({ 'transaction-type': 'Storage Fee - Reversal', 'other-amount': '12.00', 'marketplace-name': '' }),
  v1({ 'transaction-type': 'Fee Adjustment', 'order-id': 'O6', 'item-related-fee-type': 'FBA Pick & Pack Fee', 'item-related-fee-amount': '106.00' }),
  v1({ 'transaction-type': 'Subscription Fee', 'other-amount': '-4900.00', 'marketplace-name': '' }),
];
const V2_TSV = tsvOf(V2_COLUMNS, V2_ROWS), V1_TSV = tsvOf(V1_COLUMNS, V1_ROWS);

const keys = (rows) => rows.map((r) => r.business_line_key).sort();
const p1 = prepareReportTsv(V1_TSV, 'R-V1', 'run1');
const p2 = prepareV2ReportTsv(V2_TSV, 'R-V2', 'run2');
ok(p1.lineRows.length === p2.lineRows.length, `行数が同じ (V1 ${p1.lineRows.length} / V2→V1 ${p2.lineRows.length})`);
ok(JSON.stringify(keys(p1.lineRows)) === JSON.stringify(keys(p2.lineRows)), 'business_line_key の集まりが V1 と一致 (下流の重複除去で同じ行になる)');
ok(p1.headerRow.business_line_key === p2.headerRow.business_line_key && p2.headerRow.settlement_start_date === '2099-01-01T00:20:09+00:00', '決済の見出しの行 (日時の書き方も V1 に)');
ok(p2.unknown.length === 0 && p2.itemCodeUnresolved === 0, '規則に無い組み合わせ 0・品物の番号を補えない行 0');
ok(p2.ctx.sourceLayer === 'sp_api_v2' && /FLAT_FILE_V2\//.test(p2.ctx.sourcePath) && p2.lineRows.every((r) => r.source_layer === 'sp_api_v2' && r.parser_version === 'v2.0.0'), 'source_layer = sp_api_v2・レポートの種類・パーサの版');
const byTx = (p, tx) => p.lineRows.filter((r) => r.transaction_type === tx);
ok(byTx(p2, 'FBA Removal Order: Return Fee').map((r) => r.other_amount_micro).join() === '-60000000,-60000000', '返送手数料 = 本体 + 割引 + 税 を 1 行 (-60)・続く別の手数料はまとめない');
ok(byTx(p2, 'WAREHOUSE_DAMAGE').length === 2 && byTx(p2, 'WAREHOUSE_DAMAGE').every((r) => r.quantity_purchased === 1 && r.other_amount_micro === 277000000), '倉庫の破損の同じ行 2 つはまとめない (個数 1 を残す)');
ok(p2.lineRows.filter((r) => r.transaction_type === 'Order' && r.quantity_purchased != null).length === 1 && p2.lineRows.find((r) => r.quantity_purchased === 2 && r.price_type == null), '注文の品物ごとに個数だけの行 1 つ (金額の行には個数を付けない)');
ok(p2.lineRows.find((r) => r.item_related_fee_type === 'PointsGranted')?.order_item_code === 'OI1', 'ポイントの行の品物の番号を補う');

// 規則に無い組み合わせは落とさず数える
const odd = convertV2TsvToV1Tsv(tsvOf(V2_COLUMNS, [V2_ROWS[0], v2({ 'transaction-type': 'NewThing', 'amount-type': 'Mystery', 'amount-description': 'x', amount: '-7.00' }), v2({ 'transaction-type': 'Order', 'order-id': 'Z', sku: 'Q', 'amount-type': 'Points', 'amount-description': 'PointsGranted', amount: '-1.00' })]));
const oddP = prepareReportTsv(odd.tsv, 'R-ODD', 'r');
ok(JSON.stringify(odd.unknown) === JSON.stringify([['NewThing | Mystery | x', 1]]) && oddP.lineRows.some((r) => r.transaction_type === 'NewThing' && r.other_amount_micro === -7000000), '規則に無い組み合わせ = 数える + 金額は other-amount に残す (落とさない)');
ok(odd.itemCodeUnresolved === 1, '品物の番号を補えないポイントの行を数える');
throws(() => v2DateTimeToV1('2099-01-05 01:00:00'), /日時の形/, '日時の形が違えば止める');
throws(() => v2DateTimeToV1('2099/02/30 01:00:00 UTC'), /暦に無い/, '暦に無い日時 (2/30) は止める');
// 🚨 組み合わせで判定する (Codex #1508 R1): 既知の取引 × 未知の金額の種類 / 未知の取引 × 既知の金額の種類 / 料金の部分が本体の前
const unk = (rows) => convertV2TsvToV1Tsv(tsvOf(V2_COLUMNS, [V2_ROWS[0], ...rows])).unknown.map((x) => x[0]);
ok(JSON.stringify(unk([v2({ 'transaction-type': 'Other', 'amount-type': 'Mystery', 'amount-description': 'Unknown category', amount: '1.00' })])) === JSON.stringify(['Other | Mystery | Unknown category']), '既知の取引 × 未知の金額の種類 = 規則に無い');
ok(JSON.stringify(unk([v2({ 'transaction-type': 'NewThing', 'order-id': 'N', sku: 'S', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '1.00' })])) === JSON.stringify(['NewThing | ItemPrice']), '未知の取引 × 既知の金額の種類 (品物の行) = 規則に無い');
ok(JSON.stringify(unk([v2({ 'transaction-type': 'FBAFees', 'order-id': 'R9', 'amount-type': 'FBA Removal Order: Return Fee', 'amount-description': 'Tax on fee', amount: '-5.00' }), v2({ 'transaction-type': 'FBAFees', 'order-id': 'R9', 'amount-type': 'FBA Removal Order: Return Fee', 'amount-description': 'Base fee', amount: '-55.00' })])) === JSON.stringify(['FBAFees | FBA Removal Order: Return Fee | Tax on fee']), '料金の税が本体より前 = 規則に無い (まとめ方が V1 と違いうる)');
ok(unk([v2({ 'transaction-type': 'other-transaction', 'amount-type': 'FBA Inventory Reimbursement', 'amount-description': 'CUSTOMER_RETURN', amount: '100.00' })]).length === 0, '説明をそのまま取引の種類にする型は新しい説明でも通す (補てんの新しい種類で止めない)');
throws(() => convertV2TsvToV1Tsv(tsvOf(V2_COLUMNS.filter((c) => c !== 'amount-type'), [V2_ROWS[0]])), /列が足りない/, 'V2 の列が足りなければ止める');
throws(() => convertV2TsvToV1Tsv(tsvOf(V2_COLUMNS, [V2_ROWS[0], v2({ 'transaction-type': 'Order', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: 'abc' })])), /数でない/, '金額が数でなければ止める');

// DB: V1 で取込済みか / 両方入っても下流 (v_amazon_settlement_unified) で二重にならない
await initDB();
const db = getDB();
ok(settlementIngestedByV1(db, S) === false, '取込前は V1 で取込済みでない');
ingestSettlement(db, p1.headerRow, p1.lineRows, p1.ctx);
ok(settlementIngestedByV1(db, S) === true, 'V1 で入れた決済 = 取込済み (V2 では入れない)');
const unified = () => db.prepare(`SELECT COUNT(*) n, SUM(COALESCE(price_amount_micro,0) + COALESCE(item_related_fee_amount_micro,0) + COALESCE(promotion_amount_micro,0) + COALESCE(other_amount_micro,0)) s FROM v_amazon_settlement_unified WHERE source_settlement_id = ?`).get(S);
const before = unified();
ingestSettlement(db, p2.headerRow, p2.lineRows, p2.ctx);   // skip を外して両方入れた場合
const after = unified();
ok(before.n === after.n && before.s === after.s, `V1 と V2 の両方が入っても下流は二重にならない (${before.n} 行 / ${after.n} 行)`);
ok(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_layer = 'sp_api_v2'`).get().n === p2.lineRows.length, 'V2 の行は raw に sp_api_v2 で入る');
const r2 = ingestSettlement(db, p2.headerRow, p2.lineRows, p2.ctx);
ok(r2.lineInserted === 0, 'V2 の同じレポートを入れ直しても 0 行 (冪等)');

// main の 1 本ずつの処理 (processV2Report): 規則に無いものがあるレポートは 1 行も入れない / V1 取込済み / dry-run / 取り込む
const S2 = 'S901', v2b = (o) => ({ ...v2(o), 'settlement-id': S2 });
const hdr2 = { ...V2_ROWS[0], 'settlement-id': S2 };
const good2 = tsvOf(V2_COLUMNS, [hdr2, v2b({ 'transaction-type': 'Order', 'order-id': 'X1', 'order-item-code': 'XI', sku: 'SKU-X', 'quantity-purchased': '1', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '300.00' })]);
const bad2 = tsvOf(V2_COLUMNS, [hdr2, v2b({ 'transaction-type': 'Order', 'order-id': 'X1', 'order-item-code': 'XI', sku: 'SKU-X', 'quantity-purchased': '1', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '300.00' }), v2b({ 'transaction-type': 'NewThing', 'amount-type': 'Mystery', 'amount-description': 'x', amount: '-7.00' })]);
const noDate = tsvOf(V2_COLUMNS, [hdr2, v2b({ 'transaction-type': 'Order', 'order-id': 'X1', sku: 'SKU-X', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '300.00', 'posted-date-time': '' })]);
const rawCount = () => db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_settlement_id = ?`).get(S2).n;
let pr = processV2Report(db, bad2, 'R-BAD', 'run-b');
ok(pr.status === 'blocked' && rawCount() === 0, `🚨 規則に無いものがあるレポートは 1 行も入れない (${pr.reason}) = 規則を直して入れ直しても二重にならない`);
pr = processV2Report(db, noDate, 'R-ND', 'run-b');
ok(pr.status === 'blocked' && /日時/.test(pr.reason) && rawCount() === 0, '明細の日時が空なら取り込まない (月の集計から落ちるのを防ぐ)');
pr = processV2Report(db, good2, 'R-G', 'run-b', { dryRun: true });
ok(pr.status === 'dry_run' && rawCount() === 0, 'dry-run は書かない');
pr = processV2Report(db, good2, 'R-G', 'run-b');
ok(pr.status === 'ingested' && rawCount() === 2, '規則どおりなら取り込む (本体の行 + 個数だけの行)');
// 🆕 2026-10-01 (D-66・R23 H1): V1 で取込済みの決済でも V2 を版として入れる (skipped_v1 をやめた)。採る版は 1 つ = 下流は二重にならない
{
  const beforeU = unified();
  const pv = processV2Report(db, V2_TSV, 'R-V2b', 'run-c', { reportDocumentId: 'DOC-V2b' });
  const afterU = unified();
  ok(pv.status === 'ingested' && pv.result.lineInserted === p2.lineRows.length && pv.coveredByOtherVersion === true, `V1 で取込済みの決済の V2 も版として入れる (${pv.status}・${pv.result && pv.result.lineInserted} 行)`);
  ok(beforeU.n === afterU.n && beforeU.s === afterU.s, `版が 3 つあっても下流は採った版 1 つだけ (${beforeU.n} 行 / ${afterU.n} 行)`);
}
// 🚨 規則に無いもので止まった決済を V1 で代わりに入れたら、V2 はもう ❌ にしない (並べ直しより先に V1 取込済みを見る。Codex #1508 R2)
const S3 = 'S902', hdr3 = { ...V2_ROWS[0], 'settlement-id': S3 }, v2c = (o) => ({ ...v2(o), 'settlement-id': S3 });
const bad3 = tsvOf(V2_COLUMNS, [hdr3, v2c({ 'transaction-type': 'NewThing', 'amount-type': 'Mystery', 'amount-description': 'x', amount: '-7.00' }), v2c({ 'transaction-type': 'Order', sku: 'Z', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '1.00', 'posted-date-time': '' })]);
ok(processV2Report(db, bad3, 'R-B3', 'run-d').status === 'blocked', '前提: V1 で入れる前は blocked');
const v1of3 = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...V1_ROWS[0], 'settlement-id': S3 }, v1({ 'settlement-id': S3, 'transaction-type': 'NewThing', 'other-amount': '-7.00' })]), 'R-V1-3', 'run-d');
ingestSettlement(db, v1of3.headerRow, v1of3.lineRows, v1of3.ctx);   // --source v1 で代わりに入れた
pr = processV2Report(db, bad3, 'R-B3', 'run-e');
ok(pr.status === 'blocked' && pr.settlementId === S3 && pr.coveredByOtherVersion === true, '規則に無いもの・日時の空がある V2 は入れない。V1 で取込済みなら coveredByOtherVersion (毎朝 exit 3 にはしない・coverage は満たせない)');

// 🆕 #1567 Codex R3 High 2: V2 で見出しが 2 行 (連結・壊れた文書) = parser は 2 行とも返す・取り込まない (blocked = 一覧に理由つきで残る)
{
  const S4 = 'S903', hdr4 = { ...V2_ROWS[0], 'settlement-id': S4 }, v2d = (o) => ({ ...v2(o), 'settlement-id': S4 });
  const line4 = v2d({ 'transaction-type': 'Order', 'order-id': 'Y1', 'order-item-code': 'YI', sku: 'SKU-Y', 'quantity-purchased': '1', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '300.00' });
  const twoH = tsvOf(V2_COLUMNS, [{ ...hdr4, 'total-amount': '300.00' }, { ...hdr4, 'total-amount': '999.00' }, line4]);
  const p4 = prepareV2ReportTsv(twoH, 'R-2H', 'run-2h');
  ok(p4.headerRowCount === 2 && p4.headerRows.length === 2, `🚨 V2: parser は見出しを 2 行とも返す (${p4.headerRowCount})`);
  const pr4 = processV2Report(db, twoH, 'R-2H', 'run-2h');
  const n4 = db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_settlement_id = ?`).get(S4).n;
  ok(pr4.status === 'blocked' && /見出しが 2 行/.test(pr4.reason) && n4 === 0 && !db.prepare(`SELECT 1 FROM amazon_settlement_document_versions WHERE settlement_id = ?`).get(S4),
    `🚨 V2: 見出しが 2 行の文書は取り込まない (blocked・行も版も作らない) (${pr4.status}: ${pr4.reason})`);
  const { inspectManualFile } = await import('./amazon-settlement-manual-file.js');
  ok(inspectManualFile(twoH, { format: 'v2' }).problems.some((p) => /見出しの行が 2 行/.test(p)), '手のファイル (V2) も見出しが 2 行なら積まない');
}

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== V2 並べ直しテスト ALL PASS ===');
process.exit(failed ? 1 : 0);
