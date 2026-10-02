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
// 🚨 取込は同じ決済を V1 と V2 の両方には入れない (Codex #1582 R1 High)。呼び手の確かめ (processV2Report) を通さずに直接渡しても、取引の中で止まる
const rSkip = ingestSettlement(db, p2.headerRow, p2.lineRows, p2.ctx);
ok(rSkip.skipped === 'skipped_v1' && rSkip.headerInserted === 0 && rSkip.lineInserted === 0 && unified().n === before.n,
  `🚨 V1 で入れた決済は ingestSettlement に V2 を直接渡しても入らない (取引の中で確かめる・skipped_v1) (${rSkip.skipped})`);
// 下流だけの確かめ: 取込の排他を通さずに両方を生の表へ直接入れる (前に両方入った決済の代わり)。鍵が同じ (= 今の形の決済) なら 1 つになる
const insertRaw = (p) => {
  const put = (table, rows) => { if (!rows.length) return; const cols = Object.keys(rows[0]); const st = db.prepare(`INSERT OR IGNORE INTO ${table} (${cols.join(', ')}) VALUES (${cols.map((c) => '@' + c).join(', ')})`); for (const r of rows) st.run(r); };
  db.transaction(() => { put('raw_amazon_settlement_headers', p.headerRow ? [p.headerRow] : []); put('raw_amazon_settlement_lines', p.lineRows); })();
};
insertRaw(p2);
const after = unified();
ok(before.n === after.n && before.s === after.s, `V1 と V2 の両方が生の表にあっても、鍵が同じなら下流は二重にならない (${before.n} 行 / ${after.n} 行)`);
ok(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_layer = 'sp_api_v2'`).get().n === p2.lineRows.length, '(前提) V2 の行が生の表に sp_api_v2 で入った');

// 🆕 2026-10-02: 税の取り直し (Order_Retrocharge / Refund_Retrocharge の ItemPrice)。本番の決済 1 つ (2025-12-29〜2026-01-12) が
//   「規則に無い組み合わせ Order_Retrocharge | ItemPrice ×2」で丸ごと止まった (本物の番号は PR #1582 の本文に)。同じ決済の V1 の 2 行の形を作り物の番号で写す:
//   取引・注文番号・marketplace・計上日・price-type Tax 53.00 / ShippingTax 0.00 だけ (SKU・品物の番号・個数・fulfillment は空・個数だけの行は無い)。
//   🚨 Refund_Retrocharge (取り消し) は実物を見ていない (手元の V1 にも無い) = 同じ形・符号を逆にしたものの **仮定**。ここは仮定どうしの比べ = 実データとの一致の証明ではない
{
  const RS = 'S920', RO = 'O-RETRO-1';
  const RETRO = [   // [取引, 説明 = price-type, 金額, V2 の日時, V1 の日時]
    ['Order_Retrocharge', 'Tax', '53.00', '2099/02/03 11:14:44 UTC', '2099-02-03T11:14:44+00:00'],
    ['Order_Retrocharge', 'ShippingTax', '0.00', '2099/02/03 11:14:44 UTC', '2099-02-03T11:14:44+00:00'],
    ['Refund_Retrocharge', 'Tax', '-53.00', '2099/02/07 02:00:00 UTC', '2099-02-07T02:00:00+00:00'],
    ['Refund_Retrocharge', 'ShippingTax', '0.00', '2099/02/07 02:00:00 UTC', '2099-02-07T02:00:00+00:00'],
  ];
  const rV2 = tsvOf(V2_COLUMNS, [
    { 'settlement-id': RS, 'settlement-start-date': '2099/02/01 00:20:08 UTC', 'settlement-end-date': '2099/02/15 00:20:08 UTC', 'deposit-date': '2099/02/17 00:20:08 UTC', 'total-amount': '12345.00', currency: 'JPY' },
    ...RETRO.map(([tx, d, a, t2]) => ({ 'settlement-id': RS, 'transaction-type': tx, 'order-id': RO, 'marketplace-name': 'Amazon.co.jp', 'amount-type': 'ItemPrice', 'amount-description': d, amount: a, 'posted-date': t2.slice(0, 10), 'posted-date-time': t2 })),
  ]);
  const rV1 = tsvOf(V1_COLUMNS, [
    { 'settlement-id': RS, 'settlement-start-date': '2099-02-01T00:20:08+00:00', 'settlement-end-date': '2099-02-15T00:20:08+00:00', 'deposit-date': '2099-02-17T00:20:08+00:00', 'total-amount': '12345.00', currency: 'JPY' },
    ...RETRO.map(([tx, d, a, , t1]) => ({ 'settlement-id': RS, 'transaction-type': tx, 'order-id': RO, 'marketplace-name': 'Amazon.co.jp', 'posted-date': t1, 'price-type': d, 'price-amount': a })),
  ]);
  const rc = convertV2TsvToV1Tsv(rV2);
  ok(rc.unknown.length === 0 && rc.itemCodeUnresolved === 0, `税の取り直し (Order_Retrocharge / Refund_Retrocharge の ItemPrice の Tax / ShippingTax) は規則にある (${JSON.stringify(rc.unknown)})`);
  ok(rc.tsv === rV1, '税の取り直しの V2 → V1 の TSV が V1 と 1 文字も違わない (個数だけの行を作らない・SKU・品物の番号・個数は空のまま)');
  const q1 = prepareReportTsv(rV1, 'R-RV1', 'run-r'), q2 = prepareV2ReportTsv(rV2, 'R-RV2', 'run-r');
  // detail_digest (#1567) の材料 = business_line_key・出現順・金額 9 つ・個数・取引・注文・SKU・計上日。ここでは中身の列の全部 + 行番号を比べる (= より強い)。
  //   出どころの列 (文書・層・版・取込の時刻・hash など) は V1 / V2 で違って当たり前 = 比べない (除く列を並べると、列が増えたとき落ちる → 比べる列を並べる)
  const CONTENT_COLS = ['business_line_key', 'source_line_no', 'source_settlement_id', 'posted_date_utc', 'posted_datetime_jst', 'economic_date', 'year_month_int',
    'amazon_order_id', 'merchant_order_id', 'shipment_id', 'order_item_code', 'adjustment_id', 'seller_sku', 'seller_sku_normalized', 'transaction_type', 'marketplace_name', 'fulfillment_id',
    'quantity_purchased', 'price_type', 'price_amount_micro', 'item_related_fee_type', 'item_related_fee_amount_micro', 'promotion_id', 'promotion_type', 'promotion_amount_micro',
    'shipment_fee_type', 'shipment_fee_amount_micro', 'order_fee_type', 'order_fee_amount_micro', 'misc_fee_amount_micro', 'other_fee_amount_micro', 'other_fee_reason_description',
    'direct_payment_type', 'direct_payment_amount_micro', 'other_amount_micro', 'currency'];
  const content = (p) => p.lineRows.map((r) => JSON.stringify(CONTENT_COLS.map((k) => [k, r[k]]))).sort();
  ok(q1.lineRows.length === 4 && q2.lineRows.length === 4 && [...q1.lineRows, ...q2.lineRows].every((r) => CONTENT_COLS.every((k) => Object.hasOwn(r, k)))   // 列の名前の書き違いで両方 undefined = 素通り、を防ぐ
    && JSON.stringify(content(q1)) === JSON.stringify(content(q2)) && q1.headerRow.business_line_key === q2.headerRow.business_line_key,
    `税の取り直しの行が V1 と中身の列で全部同じ (business_line_key・金額・計上日・行番号・行の数 = detail_digest も同じ) (V1 ${q1.lineRows.length} 行 / V2→V1 ${q2.lineRows.length} 行)`);
  ok(q2.lineRows.every((r) => r.amazon_order_id === RO && r.seller_sku == null && r.order_item_code == null && r.quantity_purchased == null && r.fulfillment_id == null && r.economic_date != null)
    && q2.lineRows.map((r) => `${r.transaction_type}:${r.price_type}:${r.price_amount_micro}`).join() === 'Order_Retrocharge:Tax:53000000,Order_Retrocharge:ShippingTax:0,Refund_Retrocharge:Tax:-53000000,Refund_Retrocharge:ShippingTax:0',
    '税の取り直し = price-type / price-amount に入り、SKU・品物の番号・個数は空 (V1 と同じ)');
  // 🚨 通すのは ItemPrice の Tax / ShippingTax だけ (Codex #1582 R1 Medium・Low)。公式の RetrochargeEvent は BaseTax / ShippingTax と 米国の源泉だけ = ほかは来たら止める
  const RETRO_UNKNOWN = [   // [取引, amount-type, 説明, unknown の名前]
    ['Order_Retrocharge', 'ItemPrice', 'Principal', 'Order_Retrocharge | ItemPrice | Principal'],
    ['Refund_Retrocharge', 'ItemPrice', 'Principal', 'Refund_Retrocharge | ItemPrice | Principal'],
    ['Order_Retrocharge', 'ItemPrice', 'Shipping', 'Order_Retrocharge | ItemPrice | Shipping'],
    ['Order_Retrocharge', 'ItemFees', 'Commission', 'Order_Retrocharge | ItemFees'],
    ['Refund_Retrocharge', 'ItemFees', 'Commission', 'Refund_Retrocharge | ItemFees'],
    ['Order_Retrocharge', 'Points', 'PointsGranted', 'Order_Retrocharge | Points'],
    ['Refund_Retrocharge', 'Points', 'PointsReturned', 'Refund_Retrocharge | Points'],
    ['Order_Retrocharge', 'Promotion', 'Shipping', 'Order_Retrocharge | Promotion'],
    ['Refund_Retrocharge', 'Promotion', 'Shipping', 'Refund_Retrocharge | Promotion'],
    ['Order_Retrocharge', 'ItemWithheldTax', 'MarketplaceFacilitatorTax-Principal', 'Order_Retrocharge | ItemWithheldTax | MarketplaceFacilitatorTax-Principal'],
    ['Refund_Retrocharge', 'ItemWithheldTax', 'MarketplaceFacilitatorTax-Shipping', 'Refund_Retrocharge | ItemWithheldTax | MarketplaceFacilitatorTax-Shipping'],
  ];
  const notStopped = RETRO_UNKNOWN.filter(([tx, at, d, want]) => JSON.stringify(unk([v2({ 'transaction-type': tx, 'order-id': 'O9', 'order-item-code': 'OI9', sku: 'SKU-9', 'amount-type': at, 'amount-description': d, amount: '-1.00' })])) !== JSON.stringify([want]));
  ok(notStopped.length === 0, `税の取り直しで規則に無いもの ${RETRO_UNKNOWN.length} 通り (Tax / ShippingTax 以外の説明・ItemFees・Points・Promotion・源泉 ItemWithheldTax) は止める${notStopped.length ? ' ' + JSON.stringify(notStopped) : ''}`);
  const rp = processV2Report(db, rV2, 'R-RETRO', 'run-r');
  ok(rp.status === 'ingested' && db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_settlement_id = ? AND transaction_type LIKE '%_Retrocharge'`).get(RS).n === 4,
    `税の取り直しのある決済を止めずに取り込む (${rp.status}${rp.reason ? ': ' + rp.reason : ''})`);
  const rp2 = processV2Report(db, rV2, 'R-RETRO', 'run-r2');
  ok(rp2.status === 'ingested' && rp2.result.lineInserted === 0 && rp2.result.headerInserted === 0, `V2 の同じレポートを入れ直しても 0 行 (冪等) (${rp2.result && rp2.result.lineInserted} 行)`);
}

// 🚨 古い決済は V1 と V2 で行の分け方が違う = 両方入れると二重 → 同じ決済は片方だけ (Codex #1582 R1 High)。
//   2026-01 の本物の V1 の Easy Ship = 1 注文 2 行 (item-related-fee-type MFNPostageFee / MFNPostageFeeTax・金額は other-amount)。
//   V2 の並べ直し = 今の V1 の形の 1 行 (Amazon Easy Ship Charges・item-related-fee-amount) = business_line_key が合わない
{
  const { runSettlementFetch } = await import('./fetch-amazon-settlements.js');
  const esV2 = (sid) => tsvOf(V2_COLUMNS, [{ ...V2_ROWS[0], 'settlement-id': sid, 'total-amount': '-165.00' },
    ...[['Base fee', '-150.00'], ['Tax on fee', '-15.00']].map(([d, a]) => ({ ...v2({ 'transaction-type': 'AmazonFees', 'order-id': 'O-ES', 'shipment-id': 'SH-ES', 'fulfillment-id': 'MFN', 'amount-type': 'Amazon Easy Ship Charges', 'amount-description': d, amount: a }), 'settlement-id': sid }))]);
  const esV1 = (sid) => tsvOf(V1_COLUMNS, [{ ...V1_ROWS[0], 'settlement-id': sid, 'total-amount': '-165.00' },
    ...[['MFNPostageFee', '-150.00'], ['MFNPostageFeeTax', '-15.00']].map(([t, a]) => v1({ 'settlement-id': sid, 'transaction-type': 'Amazon Easy Ship Charges', 'order-id': 'O-ES', 'shipment-id': 'SH-ES', 'fulfillment-id': 'MFN', 'item-related-fee-type': t, 'other-amount': a }))]);
  const sumOf = (sid) => db.prepare(`SELECT COUNT(*) n, SUM(COALESCE(price_amount_micro,0) + COALESCE(item_related_fee_amount_micro,0) + COALESCE(promotion_amount_micro,0) + COALESCE(other_amount_micro,0)) s FROM v_amazon_settlement_unified WHERE source_settlement_id = ?`).get(sid);
  const layersOf = (sid) => db.prepare(`SELECT GROUP_CONCAT(DISTINCT source_layer) g FROM raw_amazon_settlement_lines WHERE source_settlement_id = ?`).get(sid).g;
  const ES = -165000000;

  // 前提: 鍵が違う = 取込の排他を通さずに両方を生の表へ入れると 2 倍になる (この試験が意味を持つことの確かめ)
  insertRaw(prepareV2ReportTsv(esV2('S932'), 'R-ES-RAW2', 'run-es'));
  insertRaw(prepareReportTsv(esV1('S932'), 'R-ES-RAW1', 'run-es'));
  ok(sumOf('S932').s === 2 * ES && sumOf('S932').n === 3, `(前提) 古い Easy Ship の V1 (2 行) と V2 (1 行) は鍵が違う = 両方あると 2 倍 (${sumOf('S932').s / 1e6} 円・${sumOf('S932').n} 行)`);

  // ① V2 → V1 (毎朝の V2 で入った決済を、後から --source v1 で取った)
  const A = 'S930';
  ok(processV2Report(db, esV2(A), 'R-ES-V2', 'run-es').status === 'ingested' && sumOf(A).s === ES && sumOf(A).n === 1, '(前提) V2 で入れた (Easy Ship 1 行・-165 円)');
  const a1 = prepareReportTsv(esV1(A), 'R-ES-V1', 'run-es');
  const rA = ingestSettlement(db, a1.headerRow, a1.lineRows, a1.ctx);
  ok(rA.skipped === 'skipped_v2' && rA.headerInserted === 0 && rA.lineInserted === 0 && sumOf(A).s === ES && sumOf(A).n === 1 && layersOf(A) === 'sp_api_v2',
    `🚨 V2 → V1: V2 で入れた決済は V1 を入れない = 金額は増えない (${rA.skipped}・${sumOf(A).s / 1e6} 円・層 ${layersOf(A)})`);
  // 本番の V1 の道 (runSettlementFetch --source v1・一覧の回): 入れない・blocked にしない (終了コード 3 にしない)・一覧の行は imported (0 行) + 注記 skipped_v2
  const repA = { reportId: 'R-ES-V1', reportType: 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE', processingStatus: 'DONE', reportDocumentId: 'D-ES-V1',
    createdTime: '2099-01-16T00:00:00+00:00', dataStartTime: '2099-01-01T00:00:00+00:00', dataEndTime: '2099-01-15T00:00:00+00:00' };
  const fakeSp = { async callAPI(req) { if (req.operation === 'getReports') return { reports: [repA] }; throw new Error(`想定外の呼び出し ${req.operation}`); } };
  const runA = await runSettlementFetch({ reportId: null, dryRun: false, source: 'v1' }, { db, sp: fakeSp, runId: 'run-es-v1', downloadTsv: async () => esV1(A), now: () => new Date('2099-01-20T00:00:00Z') });
  const invA = db.prepare(`SELECT import_result r, import_note note, lines_inserted li, header_inserted hi, settlement_id sid FROM amazon_settlement_report_inventory WHERE report_id = 'R-ES-V1' ORDER BY id DESC LIMIT 1`).get();
  ok(runA.totalLines === 0 && runA.totalHeaders === 0 && runA.blocked.length === 0 && sumOf(A).s === ES && layersOf(A) === 'sp_api_v2'
    && invA && invA.r === 'imported' && /skipped_v2/.test(invA.note) && invA.li === 0 && invA.hi === 0 && invA.sid === A,
    `🚨 V1 の取込の回 (--source v1) でも入れない・❌ にしない・一覧の行 = imported (0 行) + skipped_v2 (${JSON.stringify(invA)})`);

  // ② V1 → V2 (前に V1 で入れた決済の V2 が来た)
  const B = 'S931';
  const b1 = prepareReportTsv(esV1(B), 'R-ES-V1b', 'run-es');
  ok(ingestSettlement(db, b1.headerRow, b1.lineRows, b1.ctx).lineInserted === 2 && sumOf(B).s === ES && sumOf(B).n === 2, '(前提) V1 で入れた (Easy Ship 2 行・-165 円)');
  const prB = processV2Report(db, esV2(B), 'R-ES-V2b', 'run-es');
  const b2 = prepareV2ReportTsv(esV2(B), 'R-ES-V2b', 'run-es');
  const rB = ingestSettlement(db, b2.headerRow, b2.lineRows, b2.ctx);   // 呼び手の確かめを通さずに直接
  ok(prB.status === 'skipped_v1' && rB.skipped === 'skipped_v1' && rB.lineInserted === 0 && sumOf(B).s === ES && sumOf(B).n === 2 && layersOf(B) === 'sp_api_v1',
    `🚨 V1 → V2: V1 で入れた決済は V2 を入れない (取込の道でも直接でも) = 金額は増えない (${prB.status} / ${rB.skipped}・${sumOf(B).s / 1e6} 円・層 ${layersOf(B)})`);
}

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
ok(processV2Report(db, V2_TSV, 'R-V2b', 'run-c').status === 'skipped_v1', 'V1 で取込済みの決済は skipped_v1');
// 🚨 規則に無いもので止まった決済を V1 で代わりに入れたら、V2 はもう ❌ にしない (並べ直しより先に V1 取込済みを見る。Codex #1508 R2)
const S3 = 'S902', hdr3 = { ...V2_ROWS[0], 'settlement-id': S3 }, v2c = (o) => ({ ...v2(o), 'settlement-id': S3 });
const bad3 = tsvOf(V2_COLUMNS, [hdr3, v2c({ 'transaction-type': 'NewThing', 'amount-type': 'Mystery', 'amount-description': 'x', amount: '-7.00' }), v2c({ 'transaction-type': 'Order', sku: 'Z', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: '1.00', 'posted-date-time': '' })]);
ok(processV2Report(db, bad3, 'R-B3', 'run-d').status === 'blocked', '前提: V1 で入れる前は blocked');
const v1of3 = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...V1_ROWS[0], 'settlement-id': S3 }, v1({ 'settlement-id': S3, 'transaction-type': 'NewThing', 'other-amount': '-7.00' })]), 'R-V1-3', 'run-d');
ingestSettlement(db, v1of3.headerRow, v1of3.lineRows, v1of3.ctx);   // --source v1 で代わりに入れた
pr = processV2Report(db, bad3, 'R-B3', 'run-e');
ok(pr.status === 'skipped_v1' && pr.settlementId === S3, '規則に無いもの・日時の空があっても、V1 で取込済みなら skipped_v1 (毎朝 ❌ にしない)');

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== V2 並べ直しテスト ALL PASS ===');
process.exit(failed ? 1 : 0);
