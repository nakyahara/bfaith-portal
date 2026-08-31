// 手数料内訳（説明別）の判定・CSV解析・集計テスト（router.js は import しない = express/multer/sqlite の副作用なし）
//   node --test apps/amazon-accounting/fee-breakdown.test.mjs
// 実CSVでの受け入れ確認 (任意): 環境変数 AMAZON_PAYMENT_CSV_DIR にペイメントレポートCSVのあるフォルダを渡す
//   例: AMAZON_PAYMENT_CSV_DIR=C:/Users/info/Downloads node --test apps/amazon-accounting/fee-breakdown.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { classifyFeeRow, normalizeFeeDesc, FEE_COLUMNS } from './fee-breakdown.js';
import { parsePaymentCsvText, aggregate } from './payment-csv.js';

const EASY = 'Amazon Easy Ship料金';
const STORAGE = 'FBA在庫保管手数料';
const LONG = 'FBA長期在庫保管手数料';

const noSku = (desc, extra = {}) => ({ sku: '', 説明: desc, 解決方法: 'no_sku', 合計: 0, ...extra });

test('normalizeFeeDesc: 末尾コロン(半角/全角)・空白・全角英数・NBSP を正規化し小文字化', () => {
  assert.equal(normalizeFeeDesc('FBA在庫保管手数料:'), 'fba在庫保管手数料');
  assert.equal(normalizeFeeDesc('FBA在庫保管手数料：'), 'fba在庫保管手数料');
  assert.equal(normalizeFeeDesc('  Amazon Easy Ship料金 '), 'amazon easy ship料金');
  assert.equal(normalizeFeeDesc('Ａｍａｚｏｎ　Ｅａｓｙ　Ｓｈｉｐ料金'), 'amazon easy ship料金'); // 全角英数・全角空白
  assert.equal(normalizeFeeDesc('Amazon Easy\u00a0Ship料金'), 'amazon easy ship料金');            // NBSP
  assert.equal(normalizeFeeDesc('Amazon  Easy   Ship料金'), 'amazon easy ship料金');               // 連続空白
  assert.equal(normalizeFeeDesc(null), '');
});

test('classifyFeeRow: 月ごとの表記ゆれを全て拾う', () => {
  // 7月: 統合名
  assert.equal(classifyFeeRow(noSku('Amazon Easy Ship料金')), EASY);
  // 6月: 2種の別名
  assert.equal(classifyFeeRow(noSku('Amazon Easy Shipの発送重量手数料')), EASY);
  assert.equal(classifyFeeRow(noSku('Easy Ship発送重量手数料')), EASY);
  // 全角・NBSP 表記
  assert.equal(classifyFeeRow(noSku('Ａｍａｚｏｎ Ｅａｓｙ Ｓｈｉｐ料金')), EASY);
  assert.equal(classifyFeeRow(noSku('Amazon Easy\u00a0Ship料金')), EASY);
  // 在庫保管 (通常 / 取り消し・訂正のコロン付き)
  assert.equal(classifyFeeRow(noSku('FBA在庫保管手数料')), STORAGE);
  assert.equal(classifyFeeRow(noSku('FBA在庫保管手数料:')), STORAGE);
  assert.equal(classifyFeeRow(noSku('FBA在庫保管手数料：')), STORAGE);
  // 長期は在庫保管より先に排他判定
  assert.equal(classifyFeeRow(noSku('FBA長期在庫保管手数料')), LONG);
});

test('classifyFeeRow: 対象外の行は null', () => {
  assert.equal(classifyFeeRow(noSku('FBA在庫の返送手数料：')), null);
  assert.equal(classifyFeeRow(noSku('月額登録料')), null);
  assert.equal(classifyFeeRow(noSku('購入者の再課金：')), null);
  assert.equal(classifyFeeRow(noSku('')), null);
  assert.equal(classifyFeeRow(noSku('   ')), null);
  // SKUあり行は商品名なので判定しない (商品名に Easy Ship / 在庫保管 が入っても拾わない)
  assert.equal(classifyFeeRow({ sku: 'abc-easy-ship', 説明: 'Easy Ship 対応 ○○', 解決方法: 'direct' }), null);
  assert.equal(classifyFeeRow({ sku: 'x1', 説明: 'FBA在庫保管手数料', 解決方法: 'direct' }), null);
  // 振込みは対象外
  assert.equal(classifyFeeRow({ sku: '', 説明: 'Easy Ship', 解決方法: 'skip' }), null);
  assert.equal(classifyFeeRow(null), null);
});

test('parsePaymentCsvText: メタ行の後のヘッダーを動的検出し、引用符内カンマ・桁区切り・時刻付き日付を処理', () => {
  const hdr = ['日付/時間','決済番号','トランザクションの種類','注文番号','SKU','説明','数量','Amazon 出品サービス','フルフィルメント','市町村','都道府県','郵便番号','税金徴収型',
    '商品売上','商品の売上税','配送料','配送料の税金','ギフト包装手数料','ギフト包装クレジットの税金','Amazonポイントの費用','プロモーション割引額','プロモーション割引の税金',
    '源泉徴収税を伴うマーケットプレイス','手数料','FBA 手数料','トランザクションに関するその他の手数料','その他','合計','トランザクションのステータス','トランザクション開始日'];
  const q = a => a.map(v => '"' + v + '"').join(',');
  const lines = [
    '"Amazon 出品サービス、フルフィルメント by Amazon (FBA)"', '"指定のない場合、単位は円"', '"定義："', '"日付/時刻：..."', '', // 空行も混ぜる
    q(hdr),
    q(['2026/07/03 12:34:56 JST','123','注文','249-1','SKU-A','商品, カンマ入り','2','','','','','','','1,200','120','0','0','0','0','0','-100','-10','','-150','-300','0','0','760','支払い実行済み','2026/07/01']),
    q(['2026/07/31 23:59:59 JST','123','Amazon手数料','','','Amazon Easy Ship料金','','','','','','','','0','0','0','0','0','0','0','0','0','','-180','0','0','0','-180','支払い実行済み','2026/07/31']),
    '',
  ];
  const { headerIdx, rows } = parsePaymentCsvText(lines.join('\r\n'));
  assert.equal(headerIdx, 5);
  assert.equal(rows.length, 2);
  const r0 = rows[0];
  assert.equal(r0.日付, '2026/07/03');
  assert.equal(r0.トランザクション種類, '注文');
  assert.equal(r0.sku, 'sku-a'); // 小文字化
  assert.equal(r0.説明, '商品, カンマ入り');
  assert.equal(r0.数量, 2);
  assert.equal(r0.商品売上, 1200); // 桁区切り除去
  assert.equal(r0.プロモーション割引額, -100);
  assert.equal(r0.手数料, -150);
  assert.equal(r0.FBA手数料, -300);
  assert.equal(r0.合計, 760);
  const r1 = rows[1];
  assert.equal(r1.sku, '');
  assert.equal(r1.説明, 'Amazon Easy Ship料金');
  assert.equal(r1.数量, 0);
  assert.equal(r1.手数料, -180);
  assert.equal(r1.合計, -180);
  // ヘッダー未検出
  assert.deepEqual(parsePaymentCsvText('a,b,c\n1,2,3\n'), { headerIdx: -1, rows: [] });
});

test('aggregate: 内訳列は bySegment だけに入り、既存列・合計・税率別・除外は変わらない', () => {
  const rows = [
    // 商品行 (自社商品)
    { sku: 'a1', 説明: '商品A', 解決方法: 'direct', 売上分類: 1, 税率: 10, 原価: 100, 数量: 2, 商品売上: 1000, 手数料: -100, 合計: 900 },
    // 輸出 (除外セグメント)
    { sku: 'e1', 説明: '商品E', 解決方法: 'direct', 売上分類: 4, 税率: 10, 原価: 50, 数量: 1, 商品売上: 500, 合計: 500 },
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
  assert.equal(other.行数, 8);
  // 商品セグメントは内訳 0・既存値そのまま
  const seg1 = r.bySegment['1'];
  assert.equal(seg1[EASY], 0); assert.equal(seg1[STORAGE], 0); assert.equal(seg1[LONG], 0);
  assert.equal(seg1['合計'], 900); assert.equal(seg1.原価合計, 200);
  // 税率別・除外の行には内訳キーを生やさない (DB の by_tax / excluded JSON を汚さない)
  for (const row of [r.byTax['10'], r.byTax['8'], r.excluded['4']]) {
    for (const c of FEE_COLUMNS) assert.equal(Object.prototype.hasOwnProperty.call(row, c), false, c + ' must not exist');
  }
  assert.equal(r.byTax['10']['合計'], 900 + 500 - 1825); // no_sku 行は 10% 扱い (既存仕様)
  assert.equal(r.excluded['4']['合計'], 500);
  assert.equal(r.excluded['4'].行数, 1);
  // MF 税込行は従来キーのみ・値も従来どおり
  assert.deepEqual(Object.keys(r.mfRow), r.mfColumns);
  assert.equal(r.mfRow['手数料'], -500);
  assert.equal(r.mfRow['FBA手数料'], -1300);
  assert.equal(r.mfRow['合計'], 900 + 500 - 1825);

  // SKUなし行の説明別一覧: 種類×説明 で集約・判定付き・|合計| 降順・振込みは含まない
  const d = r.noSkuDetails;
  assert.equal(d.length, 7);
  assert.equal(d[0].説明, 'FBA在庫保管手数料'); assert.equal(d[0].判定, STORAGE); assert.equal(d[0].合計, -1000);
  const easy = d.find(x => x.説明 === 'Amazon Easy Ship料金');
  assert.equal(easy.行数, 2); assert.equal(easy.合計, -400); assert.equal(easy.判定, EASY);
  const cancel = d.find(x => x.トランザクション種類 === 'FBA 在庫関連の手数料 - 取り消し');
  assert.equal(cancel.説明, 'FBA在庫保管手数料:'); assert.equal(cancel.合計, 1000); assert.equal(cancel.判定, STORAGE);
  assert.equal(d.find(x => x.説明 === '月額登録料').判定, '');
  assert.ok(!d.some(x => x.トランザクション種類 === '振込み'));
  // 内訳3列の和 = 説明別一覧で判定が付いた行の合計 (2つの経路の整合)
  const feeSum = FEE_COLUMNS.reduce((s, c) => s + other[c], 0);
  const judged = d.filter(x => x.判定).reduce((s, x) => s + x.合計, 0);
  assert.equal(feeSum, judged);
});

// ─── 実CSVでの受け入れ確認 (要件定義 §7 の期待値) ───
// 本番と同じ parsePaymentCsvText() を使う。SKU解決 (router.js resolveSkus) は DB が必要なので、ここでは
//   ・マスタ非依存の経路 (振込み → skip / 注文外料金 → no_sku (SKUがあってもマスタ照合しない) / SKU空欄 → no_sku) を本番どおり再現し、
//   ・マスタ依存の経路 (Stage 1/2・調整・未解決) は「全て解決済み・売上分類1」に固定する。
// この前提では other 行 = SKUなし行 (+注文外料金) だけになり、その既存列 (手数料 / FBA手数料 / トランザクション他 / その他 / 合計) が
// 本番画面 (2026-07 スクリーンショット) の other 行と一致することを確認済み。商品行の分類は本テストの検証対象外。
function loadResolved(file) {
  const { headerIdx, rows } = parsePaymentCsvText(fs.readFileSync(file, 'utf-8'));
  assert.ok(headerIdx >= 0, 'header not found: ' + file);
  const noMaster = { 商品コード: null, 売上分類: null, 税率: null, 原価: 0 };
  return rows.map(r => {
    const tx = r.トランザクション種類;
    if (tx === '振込み') return { ...r, ...noMaster, 解決方法: 'skip' };
    if (tx === '注文外料金') return { ...r, ...noMaster, 解決方法: 'no_sku' };
    if (!r.sku) return { ...r, ...noMaster, 解決方法: 'no_sku' };
    return { ...r, 商品コード: r.sku, 解決方法: 'direct', 売上分類: 1, 税率: 10, 原価: 0 };
  });
}

const CSV_DIR = process.env.AMAZON_PAYMENT_CSV_DIR;
const REAL_CASES = [
  // other 行の既存列 (手数料 / FBA手数料 / その他 / 合計) は本番画面 (2026-07 スクリーンショット) の other 行と一致する値
  { file: '2026JulMonthlyTransaction.csv', easy: -2303252, easyRows: 12890, storage: -303905, long: -101496,
    other: { 手数料: -2303252, FBA手数料: -480710, トランザクション他: 0, その他: 25019, 合計: -2758943 } },
  // 6月は本番画面と未照合の回帰アンカー (同じパーサ+ダミー解決で採取)。FBA手数料 −319 には SKU付き注文外料金 (納品不備手数料 −51) を含む = 本番と同じ経路
  { file: '2026JunMonthlyTransaction.csv', easy: -1276326, easyRows: 7317, storage: -283754, long: -102696,
    other: { 手数料: -203194, FBA手数料: -319, トランザクション他: -1073132, その他: -418237, 合計: -1694882 } },
];
for (const c of REAL_CASES) {
  // env 未指定のときだけ skip。指定したのにファイルが無い場合は fail (回帰検査が走らないまま green になるのを防ぐ)
  test('実CSV ' + c.file + ' の手数料内訳が要件定義 §7 の期待値と一致', { skip: !CSV_DIR && 'AMAZON_PAYMENT_CSV_DIR 未指定' }, () => {
    const file = path.join(CSV_DIR, c.file);
    assert.ok(fs.existsSync(file), 'AMAZON_PAYMENT_CSV_DIR は指定されたがCSVが無い: ' + file);
    const r = aggregate(loadResolved(file));
    const other = r.bySegment.other;
    assert.equal(Math.round(other[EASY]), c.easy);
    assert.equal(Math.round(other[STORAGE]), c.storage);
    assert.equal(Math.round(other[LONG]), c.long);
    const easyRows = r.noSkuDetails.filter(d => d.判定 === EASY).reduce((s, d) => s + d.行数, 0);
    assert.equal(easyRows, c.easyRows);
    // 既存列の回帰 (内訳を足しても変わらない)
    for (const [k, v] of Object.entries(c.other)) assert.equal(Math.round(other[k]), v, 'other.' + k);
    // 内訳3列の和 = 説明別一覧で判定が付いた行の合計
    const feeSum = FEE_COLUMNS.reduce((s, col) => s + other[col], 0);
    const judged = r.noSkuDetails.filter(d => d.判定).reduce((s, d) => s + d.合計, 0);
    assert.equal(Math.round(feeSum), Math.round(judged));
    // 商品セグメントには内訳が入らない
    for (const col of FEE_COLUMNS) assert.equal(r.bySegment['1'][col], 0);
  });
}
