/**
 * fukutsu-csv.js のテスト。
 * ⭐実運用で取込に通っている fukutu.csv と同じ形になることを、実ファイルと突き合わせて確認する。
 *   node apps/fba-replenishment/tests/test-fukutsu-csv.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { buildFukutsuCsv, buildRowsForShipment, splitAddress, normPostal, HEADER, COL } from '../fukutsu-csv.js';
import { isRegisteredFc } from '../fukutsu-master.js';

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('fukutsu-csv');

const HND2 = { shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 5 };
const XHD4 = { shipmentConfirmationId: 'FBA15GGLDVMG', fcCode: 'XHD4', boxCount: 14 };

t('列数は26、ヘッダーの並びは仕様どおり', () => {
  assert.equal(HEADER.length, 26);
  assert.equal(HEADER[COL.お客様管理番号], 'お客様管理番号');
  assert.equal(HEADER[COL.荷受人コード], '荷受人ｺｰﾄﾞ（お届け先ｺｰﾄﾞ）');
});

t('⭐実物の fukutu.csv とヘッダーが1文字も違わない', () => {
  const p = 'C:/Users/中原　大輔/OneDrive/デスクトップ/Downloads/fukutu.csv';
  if (!fs.existsSync(p)) { console.log('     (実ファイルが無いのでスキップ)'); return; }
  const real = fs.readFileSync(p, 'utf-8').split(/\r?\n/)[0].split(',');
  assert.deepEqual(HEADER, real);
});

t('箱数ぶんの行が出る (1箱1伝票)', () => {
  const { csv, summary } = buildFukutsuCsv([HND2, XHD4], '20260807');
  assert.equal(summary.伝票枚数, 19);
  const lines = csv.trimEnd().split('\n');
  assert.equal(lines.length, 2 + 19); // ヘッダー + 空行 + データ
  assert.equal(lines[1], new Array(26).fill('').join(','), '2行目は空行 (現行GASと同じ形)');
});

t('⭐お客様管理番号(21列目)に納品番号が入る', () => {
  const { csv } = buildFukutsuCsv([HND2], '20260807');
  const first = csv.trimEnd().split('\n')[2].split(',');
  assert.equal(first[COL.お客様管理番号], 'FBA15GGL5J2X');
  assert.equal(first[COL.荷受人コード], 'HND2');
  assert.equal(first[COL.荷送人コード], '0648607868');
  assert.equal(first[COL.個数], '1');
  assert.equal(first[COL.元払区分], '1');
  assert.equal(first[COL.出荷日付], '20260807');
});

t('⭐実物の fukutu.csv と「埋めている列」が一致する (管理番号を除く)', () => {
  const p = 'C:/Users/中原　大輔/OneDrive/デスクトップ/Downloads/fukutu.csv';
  if (!fs.existsSync(p)) { console.log('     (実ファイルが無いのでスキップ)'); return; }
  const realRow = fs.readFileSync(p, 'utf-8').split(/\r?\n/)[2].split(',');
  const realFilled = realRow.map((v, i) => (v !== '' ? i : -1)).filter((i) => i >= 0);
  const mine = buildFukutsuCsv([{ ...HND2, fcCode: 'HND2' }], '20260806').csv.trimEnd().split('\n')[2].split(',');
  const mineFilled = mine.map((v, i) => (v !== '' ? i : -1)).filter((i) => i >= 0 && i !== COL.お客様管理番号);
  assert.deepEqual(mineFilled, realFilled, `自分=${mineFilled} 実物=${realFilled}`);
});

t('マスタ登録済みのFCは住所を書かない (コードだけ)', () => {
  assert.ok(isRegisteredFc('HND2'));
  const { rows } = buildRowsForShipment(HND2, '20260807');
  assert.equal(rows[0][COL.住所1], '');
  assert.equal(rows[0][COL.名前1], '');
  assert.equal(rows[0][COL.郵便番号], '');
});

t('🚨マスタ未登録のFCは住所を書いて伝票を出せるようにする (出荷を止めない)', () => {
  const neo = {
    shipmentConfirmationId: 'FBA15NEW0001', fcCode: 'ZZZ9', boxCount: 2,
    address: { postalCode: '350-1301', stateOrProvinceCode: '埼玉県', city: '狭山市', addressLine1: '青柳 915' },
  };
  assert.equal(isRegisteredFc('ZZZ9'), false);
  const { rows, registered } = buildRowsForShipment(neo, '20260807');
  assert.equal(registered, false);
  assert.equal(rows[0][COL.住所1], '埼玉県狭山市青柳 915');
  assert.equal(rows[0][COL.名前1], 'Amazon.co.jp ZZZ9');
  assert.equal(rows[0][COL.郵便番号], '350-1301');
  assert.equal(rows[0][COL.荷受人コード], 'ZZZ9');
  assert.equal(rows[0][COL.電話番号], '(06)6333-4858', 'マニュアルの「電話番号・住所・名前・郵便番号」を揃える');
});

t('登録済みFCには電話番号を入れない (マスタが正)', () => {
  const { rows } = buildRowsForShipment(HND2, '20260807');
  assert.equal(rows[0][COL.電話番号], '');
});

t('🚨TPFB は「無いもの」として扱う (マスタの登録内容がXHD1と食い違うため)', () => {
  assert.equal(isRegisteredFc('TPFB'), false);
  const { registered } = buildRowsForShipment({
    shipmentConfirmationId: 'FBA15TPFB001', fcCode: 'TPFB', boxCount: 1,
    address: { postalCode: '243-0213', stateOrProvinceCode: '神奈川県', city: '伊勢原市', addressLine1: '石田 100' },
  }, '20260807');
  assert.equal(registered, false, '住所を出力する側に倒れる');
});

t('住所は20文字ずつ3つに割る', () => {
  assert.deepEqual(splitAddress('あ'.repeat(45)), ['あ'.repeat(20), 'あ'.repeat(20), 'あ'.repeat(5)]);
  assert.deepEqual(splitAddress('埼玉県 狭山市  青柳 915'), ['埼玉県 狭山市 青柳 915', '', '']);
});

t('郵便番号は書式が合わなければ空にする (黙って壊れた値を送らない)', () => {
  assert.equal(normPostal('350-1301'), '350-1301');
  assert.equal(normPostal('3501301'), '3501301');
  assert.equal(normPostal('35-1301'), '');
  assert.equal(normPostal(null), '');
});

t('不正な入力は投げる (箱数0・納品番号なし・FCなし)', () => {
  assert.throws(() => buildRowsForShipment({ ...HND2, boxCount: 0 }, '20260807'), /輸送箱の数/);
  assert.throws(() => buildRowsForShipment({ ...HND2, shipmentConfirmationId: '' }, '20260807'), /納品番号が不正/);
  assert.throws(() => buildRowsForShipment({ ...HND2, fcCode: '' }, '20260807'), /宛先FCコード/);
  assert.throws(() => buildFukutsuCsv([HND2], '2026-08-07'), /出荷日が不正です/);
});

t('🚨マスタ未登録なのに住所が欠けていれば作らない (届かない伝票を出さない)', () => {
  const base = { shipmentConfirmationId: 'FBA15NEW0002', fcCode: 'ZZZ7', boxCount: 1 };
  assert.throws(() => buildRowsForShipment({ ...base, address: {} }, '20260807'), /郵便番号が読み取れません/);
  assert.throws(() => buildRowsForShipment({ ...base, address: { postalCode: '350-1301' } }, '20260807'), /住所が読み取れません/);
  assert.throws(
    () => buildRowsForShipment({ ...base, address: { postalCode: '350-1301', addressLine1: 'あ'.repeat(70) } }, '20260807'),
    /60文字を超えています/,
  );
});

t('🚨存在しない日付は弾く (書式だけ見ない)', () => {
  const ships = [{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 1 }];
  assert.throws(() => buildFukutsuCsv(ships, '20261340'), /出荷日が不正です/);
  assert.throws(() => buildFukutsuCsv(ships, '20260230'), /出荷日が不正です/);
  assert.throws(() => buildFukutsuCsv(ships, '20260229'), /出荷日が不正です/); // 2026は閏年ではない
  assert.equal(buildFukutsuCsv(ships, '20240229').summary.出荷日, '20240229'); // 2024は閏年
});

console.log(`\n${pass} 件すべて通過`);
