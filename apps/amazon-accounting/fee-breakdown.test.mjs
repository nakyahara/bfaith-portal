// 手数料内訳（説明別）の判定・集計テスト
//   node --test apps/amazon-accounting/fee-breakdown.test.mjs
// 実CSVでの受け入れ確認 (任意): 環境変数 AMAZON_PAYMENT_CSV_DIR にペイメントレポートCSVのあるフォルダを渡す
//   例: AMAZON_PAYMENT_CSV_DIR=C:/Users/info/Downloads node --test apps/amazon-accounting/fee-breakdown.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { aggregate, classifyFeeRow, normalizeFeeDesc, FEE_COLUMNS } from './router.js';

const EASY = 'Amazon Easy Ship料金';
const STORAGE = 'FBA在庫保管手数料';
const LONG = 'FBA長期在庫保管手数料';

const noSku = (desc, extra = {}) => ({ sku: '', 説明: desc, 解決方法: 'no_sku', 合計: 0, ...extra });

test('normalizeFeeDesc: 末尾コロン(半角/全角)と空白を除去し小文字化', () => {
  assert.equal(normalizeFeeDesc('FBA在庫保管手数料:'), 'fba在庫保管手数料');
  assert.equal(normalizeFeeDesc('FBA在庫保管手数料：'), 'fba在庫保管手数料');
  assert.equal(normalizeFeeDesc('  Amazon Easy Ship料金 '), 'amazon easy ship料金');
  assert.equal(normalizeFeeDesc(null), '');
});

test('classifyFeeRow: 月ごとの表記ゆれを全て拾う', () => {
  // 7月: 統合名
  assert.equal(classifyFeeRow(noSku('Amazon Easy Ship料金')), EASY);
  // 6月: 2種の別名
  assert.equal(classifyFeeRow(noSku('Amazon Easy Shipの発送重量手数料')), EASY);
  assert.equal(classifyFeeRow(noSku('Easy Ship発送重量手数料')), EASY);
  // 在庫保管 (通常 / 取り消し・訂正のコロン付き)
  assert.equal(classifyFeeRow(noSku('FBA在庫保管手数料')), STORAGE);
  assert.equal(classifyFeeRow(noSku('FBA在庫保管手数料:')), STORAGE);
  // 長期は在庫保管より先に排他判定
  assert.equal(classifyFeeRow(noSku('FBA長期在庫保管手数料')), LONG);
});

test('classifyFeeRow: 対象外の行は null', () => {
  assert.equal(classifyFeeRow(noSku('FBA在庫の返送手数料：')), null);
  assert.equal(classifyFeeRow(noSku('月額登録料')), null);
  assert.equal(classifyFeeRow(noSku('')), null);
  // SKUあり行は商品名なので判定しない (商品名に Easy Ship が入っても拾わない)
  assert.equal(classifyFeeRow({ sku: 'abc-easy-ship', 説明: 'Easy Ship 対応 ○○', 解決方法: 'direct' }), null);
  // 振込みは対象外
  assert.equal(classifyFeeRow({ sku: '', 説明: 'Easy Ship', 解決方法: 'skip' }), null);
  assert.equal(classifyFeeRow(null), null);
});

test('aggregate: 内訳列は other 行に入り、既存列・合計は変わらない', () => {
  const rows = [
    // 商品行 (自社商品)
    { sku: 'a1', 説明: '商品A', 解決方法: 'direct', 売上分類: 1, 税率: 10, 原価: 100, 数量: 2, 商品売上: 1000, 手数料: -100, 合計: 900 },
    // Easy Ship (7月形式: 手数料列に入る)
    noSku('Amazon Easy Ship料金', { トランザクション種類: 'Amazon手数料', 手数料: -150, 合計: -150 }),
    noSku('Amazon Easy Ship料金', { トランザクション種類: 'Amazon手数料', 手数料: -250, 合計: -250 }),
    // Easy Ship (6月形式: トランザクション他列に入る)
    noSku('Easy Ship発送重量手数料', { トランザクション種類: '配送サービス', トランザクション他: -175, 合計: -175 }),
    // 在庫保管 (通常 + 取り消し + 訂正)
    noSku('FBA在庫保管手数料', { トランザクション種類: 'FBA手数料', FBA手数料: -1000, 合計: -1000 }),
    noSku('FBA在庫保管手数料:', { トランザクション種類: 'FBA 在庫関連の手数料 - 取り消し', その他: 1000, 合計: 1000 }),
    noSku('FBA在庫保管手数料:', { トランザクション種類: 'FBA 在庫関連の手数料 - 訂正', その他: -900, 合計: -900 }),
    // 長期
    noSku('FBA長期在庫保管手数料', { トランザクション種類: 'FBA手数料', FBA手数料: -300, 合計: -300 }),
    // 対象外の SKUなし行
    noSku('月額登録料', { トランザクション種類: '注文外料金', その他: -50, 合計: -50 }),
    // 振込み (集計対象外)
    { sku: '', 説明: '末尾が029のアカウントへ', トランザクション種類: '振込み', 解決方法: 'skip', その他: -99999, 合計: -99999 },
  ];
  const r = aggregate(rows);

  assert.deepEqual(r.feeColumns, FEE_COLUMNS);
  const other = r.bySegment.other;
  assert.equal(other[EASY], -575);
  assert.equal(other[STORAGE], -900);   // -1000 + 1000 - 900
  assert.equal(other[LONG], -300);
  // 既存列は従来どおり (内訳は加算しない)
  assert.equal(other['手数料'], -400);
  assert.equal(other['トランザクション他'], -175);
  assert.equal(other['FBA手数料'], -1300);
  assert.equal(other['その他'], 50);     // 1000 - 900 - 50
  assert.equal(other['合計'], -1825);
  // 商品セグメントは 0
  const seg1 = r.bySegment['1'];
  assert.equal(seg1[EASY], 0); assert.equal(seg1[STORAGE], 0); assert.equal(seg1[LONG], 0);
  assert.equal(seg1['合計'], 900);
  // 税率別・除外の行にも列が生えている (0)
  assert.equal(r.byTax['10'][EASY], -575); // no_sku 行は 10% 扱いで税率別にも入る (既存仕様)
  assert.equal(r.excluded['4'][EASY], 0);

  // SKUなし行の説明別一覧: 種類×説明 で集約・判定付き・|合計| 降順
  const d = r.noSkuDetails;
  assert.equal(d.length, 7);
  assert.equal(d[0].説明, 'FBA在庫保管手数料'); assert.equal(d[0].判定, STORAGE); assert.equal(d[0].合計, -1000);
  const easy = d.find(x => x.説明 === 'Amazon Easy Ship料金');
  assert.equal(easy.行数, 2); assert.equal(easy.合計, -400); assert.equal(easy.判定, EASY);
  const cancel = d.find(x => x.トランザクション種類 === 'FBA 在庫関連の手数料 - 取り消し');
  assert.equal(cancel.説明, 'FBA在庫保管手数料:'); assert.equal(cancel.合計, 1000); assert.equal(cancel.判定, STORAGE);
  const monthly = d.find(x => x.説明 === '月額登録料');
  assert.equal(monthly.判定, '');
  assert.ok(!d.some(x => x.トランザクション種類 === '振込み'));
});

// ─── 実CSVでの受け入れ確認 (要件定義 §7 の期待値) ───
// /upload と同じ行パースを再現し、SKU解決は DB 不要のダミー (SKUあり=direct/分類1, SKUなし=no_sku, 振込み=skip)
function parsePaymentCsv(file) {
  const lines = fs.readFileSync(file, 'utf-8').split(/\r?\n/);
  let headerIdx = -1;
  for (let i = 0; i < Math.min(lines.length, 30); i++) {
    if (lines[i].includes('日付/時間') && lines[i].includes('SKU')) { headerIdx = i; break; }
  }
  assert.ok(headerIdx >= 0, 'header not found: ' + file);
  const num = v => { const n = parseFloat((v || '').replace(/"/g, '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };
  const clean = v => (v || '').replace(/^"|"$/g, '').trim();
  const rows = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    const cols = []; let cur = '', inQ = false;
    for (const ch of line) { if (ch === '"') inQ = !inQ; else if (ch === ',' && !inQ) { cols.push(cur); cur = ''; } else cur += ch; }
    cols.push(cur);
    if (!clean(cols[0])) continue;
    const tx = clean(cols[2]);
    const sku = clean(cols[4]).toLowerCase();
    rows.push({
      トランザクション種類: tx, sku, 説明: clean(cols[5]), 数量: parseInt(clean(cols[6])) || 0,
      商品売上: num(cols[13]), 手数料: num(cols[23]), FBA手数料: num(cols[24]), トランザクション他: num(cols[25]), その他: num(cols[26]), 合計: num(cols[27]),
      解決方法: tx === '振込み' ? 'skip' : (sku ? 'direct' : 'no_sku'),
      売上分類: (tx !== '振込み' && sku) ? 1 : null, 税率: (tx !== '振込み' && sku) ? 10 : null, 原価: 0,
    });
  }
  return rows;
}

const CSV_DIR = process.env.AMAZON_PAYMENT_CSV_DIR;
const REAL_CASES = [
  { file: '2026JulMonthlyTransaction.csv', easy: -2303252, easyRows: 12890, storage: -303905, long: -101496 },
  { file: '2026JunMonthlyTransaction.csv', easy: -1276326, easyRows: 7317, storage: -283754, long: -102696 },
];
for (const c of REAL_CASES) {
  const file = CSV_DIR ? path.join(CSV_DIR, c.file) : null;
  test('実CSV ' + c.file + ' の手数料内訳が要件定義 §7 の期待値と一致', { skip: !(file && fs.existsSync(file)) && 'AMAZON_PAYMENT_CSV_DIR 未指定またはCSVなし' }, () => {
    const r = aggregate(parsePaymentCsv(file));
    const other = r.bySegment.other;
    assert.equal(Math.round(other[EASY]), c.easy);
    assert.equal(Math.round(other[STORAGE]), c.storage);
    assert.equal(Math.round(other[LONG]), c.long);
    const easyRows = r.noSkuDetails.filter(d => d.判定 === EASY).reduce((s, d) => s + d.行数, 0);
    assert.equal(easyRows, c.easyRows);
    // 内訳は既存列に既に含まれている (抜き出し) こと: 3列の和が other 行の 手数料+FBA手数料+トランザクション他+その他 を超えない
    const feeSum = other[EASY] + other[STORAGE] + other[LONG];
    const colSum = other['手数料'] + other['FBA手数料'] + other['トランザクション他'] + other['その他'];
    assert.ok(Math.abs(feeSum) <= Math.abs(colSum) + 1, 'fee breakdown must be a subset of existing columns');
  });
}
