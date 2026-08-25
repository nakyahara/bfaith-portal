/**
 * 拡張が持つCSVビルダーと、サーバ側のCSVビルダーが**同じ結果を出す**ことを保証する。
 * 拡張は画面から読んだ情報だけでCSVを作る (サーバ不要) ため実装が2本ある。
 * 片方だけ直すとここが落ちる。
 *   node apps/fba-replenishment/tests/test-fukutsu-csv-parity.mjs
 */
import assert from 'node:assert/strict';
import * as server from '../fukutsu-csv.js';
await import('../../../tools/fba-fukutsu-helper/fukutsu-csv.js'); // globalThis.BF_FUKUTSU を立てる
const ext = globalThis.BF_FUKUTSU;

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('fukutsu-csv parity (拡張 ⇔ サーバ)');

t('ヘッダーが完全一致', () => assert.deepEqual(ext.HEADER, server.HEADER));
t('列位置が完全一致', () => assert.deepEqual(ext.COL, server.COL));

const ADDR_SAYAMA = { postalCode: '350-1301', stateOrProvinceCode: '埼玉県', city: '狭山市', addressLine1: '青柳 915' };
const CASES = [
  { name: '複数箱', ships: [{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 5, address: ADDR_SAYAMA }], ymd: '20260807' },
  { name: '複数納品', ships: [
      { shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 5, address: ADDR_SAYAMA },
      { shipmentConfirmationId: 'FBA15GGLDVMG', fcCode: 'XHD4', boxCount: 14, address: { postalCode: '2430213', addressLine1: '神奈川県 伊勢原市 石田 100' } },
    ], ymd: '20260808' },
  { name: '画面パーサの形 (住所が1本の文字列)', ships: [{
      shipmentConfirmationId: 'FBA15GH9C0L0', fcCode: 'XJE1', boxCount: 35,
      address: { postalCode: '243-0488', addressLine1: '神奈川県 海老名市 中新田3290 MFLP海老名I 2階' },
    }], ymd: '20260825' },
  { name: '長い住所 (20文字ずつ分割)', ships: [{
      shipmentConfirmationId: 'FBA15LONG001', fcCode: 'ZZZ8', boxCount: 1,
      address: { postalCode: '270-0193', stateOrProvinceCode: '千葉県', city: '流山市', addressLine1: '森のロジスティクスパーク一丁目383番地の11 DPL 流山IV 1F・2F（南棟）' },
    }], ymd: '20260810' },
];

for (const c of CASES) {
  t(`CSVが1バイトも違わない: ${c.name}`, () => {
    const a = ext.buildFukutsuCsv(c.ships, c.ymd);
    const b = server.buildFukutsuCsv(c.ships, c.ymd);
    assert.equal(a.csv, b.csv);
    assert.deepEqual(a.summary, b.summary);
  });
}

t('不正入力の弾き方も同じ', () => {
  for (const bad of [
    [[{ shipmentConfirmationId: 'FBA-X', fcCode: 'HND2', boxCount: 1, address: ADDR_SAYAMA }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: '', boxCount: 1, address: ADDR_SAYAMA }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 0, address: ADDR_SAYAMA }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 1, address: ADDR_SAYAMA }], '2026-08-07'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 1 }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 1, address: { postalCode: '350-1301' } }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 1, address: { postalCode: '350-1301', addressLine1: 'あ'.repeat(70) } }], '20260807'],
  ]) {
    let ea = null, eb = null;
    try { ext.buildFukutsuCsv(...bad); } catch (e) { ea = e.message; }
    try { server.buildFukutsuCsv(...bad); } catch (e) { eb = e.message; }
    assert.ok(ea, `拡張が弾いていない: ${JSON.stringify(bad)}`);
    assert.equal(ea, eb, `エラー文言が違う`);
  }
});

console.log(`\n${pass} 件すべて通過`);
