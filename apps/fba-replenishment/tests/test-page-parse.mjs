/**
 * 拡張の画面パーサのテスト。
 * ⭐実物のSeller Central「ステップ3 – 印刷された輸送箱ラベル」の表示テキストを写したものを使う
 *   (2026-08-07 の納品。中原さんのスクリーンショットから起こした)。
 *   node apps/fba-replenishment/tests/test-page-parse.mjs
 */
import assert from 'node:assert/strict';
await import('../../../tools/fba-fukutsu-helper/page-parse.js');
await import('../../../tools/fba-fukutsu-helper/fukutsu-csv.js');
const P = globalThis.BF_PAGE;
const B = globalThis.BF_FUKUTSU;

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('page-parse');

// 実画面の表示テキスト (innerText 相当)
const REAL = [
  'ステップ2 – 出荷確認済み  発送先：2  方法：個口配送（SPD）  配送業者：福山通運',
  'ステップ3 – 印刷された輸送箱ラベル',
  '発送元：B-Faith株式会社, 41-36、吹田市南清和園町、Osaka、564-0038、JP',
  '出荷日：2026年8月7日金曜日',
  '2：確認済みの出荷数',
  '輸送箱ラベルを印刷すると、納品のステータスが「納品準備完了」に変わります。',
  '納品＃1',
  '納品名：FBA STA (2026/08/07 05:46)-HND2',
  '納品番号：FBA15GGL5J2X',
  'Amazon参照ID（PO）：3OQ4S42I',
  '発送元：B-Faith株式会社, 41-36、吹田市南清和園町、Osaka、564-0038、JP',
  '納品先：HND2 – 350-1301 Japan 埼玉県狭山市 青柳 915',
  '出荷能力：標準',
  '出荷商品: 輸送箱：5, SKU：9, ユニット：387',
  '輸送箱ラベルを印刷',
  '納品＃2',
  '納品名：FBA STA (2026/08/07 05:46)-XHD4',
  '納品番号：FBA15GGLDVMG',
  'Amazon参照ID（PO）：8PJNXQ1D',
  '納品先：XHD4 – 270-0193 Japan 千葉県 流山市 森のロジスティクスパーク一丁目383番地の11 DPL 流山IV 1F・2F（南棟）',
  '出荷商品: 輸送箱：14, SKU：26, ユニット：1687',
  '最終ステップ: 追跡情報',
].join('\n');

t('⭐実画面から納品2件を正しく読む', () => {
  const r = P.parseShipments(REAL);
  assert.equal(r.ok, true, r.problems.join(' / '));
  assert.equal(r.ymd, '20260807');
  assert.equal(r.shipments.length, 2);
  assert.deepEqual(
    r.shipments.map((s) => [s.shipmentConfirmationId, s.fcCode, s.boxCount, s.address.postalCode]),
    [['FBA15GGL5J2X', 'HND2', 5, '350-1301'], ['FBA15GGLDVMG', 'XHD4', 14, '270-0193']],
  );
  assert.equal(r.shipments[0].address.addressLine1, '埼玉県狭山市 青柳 915');
});

t('納品名の "-HND2" に釣られず、納品先のFCコードを使う', () => {
  const r = P.parseShipments(REAL);
  assert.equal(r.shipments[1].fcCode, 'XHD4'); // 納品名も XHD4 だが、納品先から取る
});

t('⭐読んだ内容がそのままCSVになる (伝票19枚)', () => {
  const r = P.parseShipments(REAL);
  const built = B.buildFukutsuCsv(r.shipments, r.ymd);
  assert.equal(built.summary.伝票枚数, 19); // 5 + 14
  const lines = built.csv.trimEnd().split('\n');
  const first = lines[2].split(',');
  assert.equal(first[0], 'HND2');
  assert.equal(first[20], 'FBA15GGL5J2X'); // お客様管理番号
  assert.equal(first[24], '20260807');
});

t('🚨ステップ3以外の画面では「押す場所が違う」と言う', () => {
  const r = P.parseShipments('ステップ1 – 商品を確認\nSKUを追加してください');
  assert.equal(r.ok, false);
  assert.match(r.problems[0], /ステップ3/);
});

t('🚨箱数が読めない納品はエラーにする (黙って落とさない)', () => {
  const broken = REAL.replace('出荷商品: 輸送箱：5, SKU：9, ユニット：387', '出荷商品: SKU：9');
  const r = P.parseShipments(broken);
  assert.equal(r.ok, false);
  assert.ok(r.problems.some((p) => /FBA15GGL5J2X.*輸送箱の数/.test(p)), r.problems.join(' / '));
});

t('🚨納品先が読めない納品はエラーにする', () => {
  const broken = REAL.replace('納品先：HND2 – 350-1301 Japan 埼玉県狭山市 青柳 915', '納品先：（表示エラー）');
  const r = P.parseShipments(broken);
  assert.equal(r.ok, false);
  assert.ok(r.problems.some((p) => /FBA15GGL5J2X.*納品先/.test(p)), r.problems.join(' / '));
});

t('出荷日が無ければエラー (当日で勝手に代用しない)', () => {
  const r = P.parseShipments(REAL.replace('出荷日：2026年8月7日金曜日', ''));
  assert.equal(r.ok, false);
  assert.ok(r.problems.some((p) => /出荷日/.test(p)));
});

t('半角コロン・全角数字・各種ダッシュを吸収する', () => {
  const alt = [
    '出荷日: 2026年8月10日月曜日',
    '納品番号: FBA15ALT0001',
    '納品先: ｈｎｄ２ — 350-1301 Japan 埼玉県狭山市 青柳 915',
    '出荷商品: 輸送箱: ３',
  ].join('\n');
  const r = P.parseShipments(alt);
  assert.equal(r.ok, true, r.problems.join(' / '));
  assert.equal(r.ymd, '20260810');
  assert.equal(r.shipments[0].fcCode, 'HND2');
  assert.equal(r.shipments[0].boxCount, 3);
});

t('郵便番号のハイフン無しも通る', () => {
  const alt = REAL.replace('350-1301 Japan', '3501301 Japan');
  const r = P.parseShipments(alt);
  assert.equal(r.ok, true, r.problems.join(' / '));
  assert.equal(r.shipments[0].address.postalCode, '3501301');
});

console.log(`\n${pass} 件すべて通過`);
