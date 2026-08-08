/**
 * 拡張が持つCSVビルダーと、サーバ側のCSVビルダーが**同じ結果を出す**ことを保証する。
 * 拡張は画面から読んだ情報だけでCSVを作る (サーバ不要) ため実装が2本ある。
 * 片方だけ直すとここが落ちる。
 *   node apps/fba-replenishment/tests/test-fukutsu-csv-parity.mjs
 */
import assert from 'node:assert/strict';
import * as server from '../fukutsu-csv.js';
import { REGISTERED_FC_CODES, isRegisteredFc as serverIsRegisteredFc } from '../fukutsu-master.js';
await import('../../../tools/fba-fukutsu-helper/fukutsu-csv.js'); // globalThis.BF_FUKUTSU を立てる
const ext = globalThis.BF_FUKUTSU;

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('fukutsu-csv parity (拡張 ⇔ サーバ)');

t('ヘッダーが完全一致', () => assert.deepEqual(ext.HEADER, server.HEADER));
t('列位置が完全一致', () => assert.deepEqual(ext.COL, server.COL));

t('登録済みFCコードの集合が完全一致', () => {
  const s = [...REGISTERED_FC_CODES].sort();
  const e = s.filter((c) => ext.isRegisteredFc(c));
  assert.deepEqual(e, s, '拡張側に足りないコードがある');
  // 逆向き: 拡張だけが登録済みと言うコードが無いか (代表例で確認)
  for (const c of ['TPFB', 'ZZZ9', 'QQQ1', '']) {
    assert.equal(ext.isRegisteredFc(c), serverIsRegisteredFc(c), `不一致: ${c}`);
  }
});

const CASES = [
  { name: '登録済みFC・複数箱', ships: [{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 5 }], ymd: '20260807' },
  { name: '複数納品', ships: [
      { shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 5 },
      { shipmentConfirmationId: 'FBA15GGLDVMG', fcCode: 'XHD4', boxCount: 14 },
    ], ymd: '20260808' },
  { name: '未登録FC (住所を出す)', ships: [{
      shipmentConfirmationId: 'FBA15NEW0001', fcCode: 'ZZZ9', boxCount: 2,
      address: { postalCode: '350-1301', stateOrProvinceCode: '埼玉県', city: '狭山市', addressLine1: '青柳 915' },
    }], ymd: '20260810' },
  { name: 'TPFB (無いもの扱い→住所)', ships: [{
      shipmentConfirmationId: 'FBA15TPFB001', fcCode: 'TPFB', boxCount: 1,
      address: { postalCode: '2430213', stateOrProvinceCode: '神奈川県', city: '伊勢原市', addressLine1: '石田 100' },
    }], ymd: '20260810' },
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
    [[{ shipmentConfirmationId: 'FBA-X', fcCode: 'HND2', boxCount: 1 }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: '', boxCount: 1 }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 0 }], '20260807'],
    [[{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', boxCount: 1 }], '2026-08-07'],
  ]) {
    let ea = null, eb = null;
    try { ext.buildFukutsuCsv(...bad); } catch (e) { ea = e.message; }
    try { server.buildFukutsuCsv(...bad); } catch (e) { eb = e.message; }
    assert.ok(ea, `拡張が弾いていない: ${JSON.stringify(bad)}`);
    assert.equal(ea, eb, `エラー文言が違う`);
  }
});

console.log(`\n${pass} 件すべて通過`);
