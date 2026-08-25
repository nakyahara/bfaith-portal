/**
 * fukutsu-csv.js のテスト。
 * ⭐実運用で取込に通っている fukutu.csv と同じ形になることを、実ファイルと突き合わせて確認する。
 * ⭐宛先は常に画面の住所を書く (iS-2 マスタに頼らない・2026-08-25)。
 *   node apps/fba-replenishment/tests/test-fukutsu-csv.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { buildFukutsuCsv, buildRowsForShipment, splitAddress, normPostal, HEADER, COL } from '../fukutsu-csv.js';

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('fukutsu-csv');

const HND2 = {
  shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 5,
  address: { postalCode: '350-1301', stateOrProvinceCode: '埼玉県', city: '狭山市', addressLine1: '青柳 915' },
};
const XHD4 = {
  shipmentConfirmationId: 'FBA15GGLDVMG', fcCode: 'XHD4', boxCount: 14,
  address: { postalCode: '243-0213', addressLine1: '神奈川県 伊勢原市 石田 100' },
};
const XJE1 = {
  shipmentConfirmationId: 'FBA15GH9C0L0', fcCode: 'XJE1', boxCount: 35,
  address: { postalCode: '243-0488', addressLine1: '神奈川県 海老名市 中新田3290 MFLP海老名I 2階' },
};

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
  assert.equal(lines.length, 1 + 19); // ヘッダー + データ
  assert.match(lines[1], /^HND2,/, '🚨2行目からデータ (空行を挟むと iS-2 でエラー1件になる・2026-08-25 実機)');
});

t('⭐お客様管理番号(21列目)に納品番号が入る', () => {
  const { csv } = buildFukutsuCsv([HND2], '20260807');
  const first = csv.trimEnd().split('
')[1].split(',');
  assert.equal(first[COL.お客様管理番号], 'FBA15GGL5J2X');
  assert.equal(first[COL.荷受人コード], 'HND2');
  assert.equal(first[COL.荷送人コード], '0648607868');
  assert.equal(first[COL.個数], '1');
  assert.equal(first[COL.元払区分], '1');
  assert.equal(first[COL.出荷日付], '20260807');
});

t('⭐実物の fukutu.csv (コードのみ時代) が埋めていた列は、今も全部埋まっている', () => {
  const p = 'C:/Users/中原　大輔/OneDrive/デスクトップ/Downloads/fukutu.csv';
  if (!fs.existsSync(p)) { console.log('     (実ファイルが無いのでスキップ)'); return; }
  const realRow = fs.readFileSync(p, 'utf-8').split(/\r?\n/)[2].split(',');
  const realFilled = realRow.map((v, i) => (v !== '' ? i : -1)).filter((i) => i >= 0);
  const mine = buildFukutsuCsv([HND2], '20260806').csv.trimEnd().split('
')[1].split(',');
  for (const i of realFilled) {
    if (i === COL.お客様管理番号) continue;
    assert.notEqual(mine[i], '', `実物が埋めていた ${i} 列目が空`);
  }
});

t('⭐宛先は常に画面の住所を書く (荷受人コード + 電話・住所・名前・郵便番号)', () => {
  const { rows, address, postalCode } = buildRowsForShipment(HND2, '20260807');
  assert.equal(rows.length, 5);
  assert.equal(rows[0][COL.荷受人コード], 'HND2');
  assert.equal(rows[0][COL.住所1], '埼玉県狭山市青柳 915');
  assert.equal(rows[0][COL.名前1], 'Amazon.co.jp HND2');
  assert.equal(rows[0][COL.郵便番号], '350-1301');
  assert.equal(rows[0][COL.電話番号], '(06)6333-4858', 'マニュアルの「電話番号・住所・名前・郵便番号」を揃える');
  assert.equal(address, '埼玉県狭山市青柳 915');
  assert.equal(postalCode, '350-1301');
  assert.deepEqual(rows[0], rows[4], '同じ納品の行は全部同じ');
});

t('画面の住所が1本の文字列でも (page-parse の形) そのまま書ける', () => {
  const { rows } = buildRowsForShipment(XJE1, '20260825');
  assert.equal(rows[0][COL.住所1], '神奈川県 海老名市 中新田3290 MF'); // 20文字 (空白込み)
  assert.equal(rows[0][COL.住所2], 'LP海老名I 2階');
  assert.equal(rows[0][COL.住所3], '');
  assert.equal(rows[0][COL.郵便番号], '243-0488');
});

t('確認パネル向けの summary に住所が入る', () => {
  const { summary } = buildFukutsuCsv([XJE1], '20260825');
  assert.equal(summary.detail[0].宛先, '〒243-0488 神奈川県 海老名市 中新田3290 MFLP海老名I 2階');
  assert.equal(summary.detail[0].箱数, 35);
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

t('🚨住所が欠けていれば作らない (届かない伝票を出さない)', () => {
  const base = { shipmentConfirmationId: 'FBA15NEW0002', fcCode: 'ZZZ7', boxCount: 1 };
  assert.throws(() => buildRowsForShipment(base, '20260807'), /郵便番号が読み取れません/);
  assert.throws(() => buildRowsForShipment({ ...base, address: {} }, '20260807'), /郵便番号が読み取れません/);
  assert.throws(() => buildRowsForShipment({ ...base, address: { postalCode: '350-1301' } }, '20260807'), /住所が読み取れません/);
  assert.throws(
    () => buildRowsForShipment({ ...base, address: { postalCode: '350-1301', addressLine1: 'あ'.repeat(70) } }, '20260807'),
    /60文字を超えています/,
  );
});

t('🚨存在しない日付は弾く (書式だけ見ない)', () => {
  const ships = [HND2];
  assert.throws(() => buildFukutsuCsv(ships, '20261340'), /出荷日が不正です/);
  assert.throws(() => buildFukutsuCsv(ships, '20260230'), /出荷日が不正です/);
  assert.throws(() => buildFukutsuCsv(ships, '20260229'), /出荷日が不正です/); // 2026は閏年ではない
  assert.equal(buildFukutsuCsv(ships, '20240229').summary.出荷日, '20240229'); // 2024は閏年
});

console.log(`\n${pass} 件すべて通過`);
