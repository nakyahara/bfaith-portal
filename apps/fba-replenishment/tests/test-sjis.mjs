/**
 * 拡張同梱の Shift_JIS エンコーダ (tools/fba-fukutsu-helper/sjis.js) が iconv-lite と同じバイト列を出すこと。
 * 🚨iS-2 は Shift_JIS で読む。UTF-8 で出すと住所が化ける (2026-08-25 に実害)。
 *   node apps/fba-replenishment/tests/test-sjis.mjs
 */
import assert from 'node:assert/strict';
import iconv from 'iconv-lite';
import * as server from '../fukutsu-csv.js';
await import('../../../tools/fba-fukutsu-helper/sjis.js');
await import('../../../tools/fba-fukutsu-helper/fukutsu-csv.js');
const { encodeSjis } = globalThis.BF_SJIS;
const ext = globalThis.BF_FUKUTSU;

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('sjis (拡張同梱エンコーダ ⇔ iconv-lite cp932)');

const SAMPLES = [
  'HND2,0648607868,1',
  '神奈川県 海老名市 中新田3290 MFLP海老名I 2階',
  '埼玉県狭山市青柳 915',
  '千葉県流山市森のロジスティクスパーク一丁目383番地の11 DPL 流山IV 1F・2F（南棟）',
  '荷受人ｺｰﾄﾞ（お届け先ｺｰﾄﾞ）,電話番号,住所,住所２ ,住所３ ,名前 ,名前２ ,郵便番号 ',
  'Amazon.co.jp XJE1 (06)6333-4858 〒243-0488',
  '①②③ ㈱ ～ ‐ ￣ ￥ 髙 﨑 ',  // NEC特殊文字・IBM拡張・波ダッシュなど、化けやすい代表
];
for (const s of SAMPLES) {
  t(`同じバイト列: ${s.slice(0, 24)}`, () => {
    assert.deepEqual(Buffer.from(encodeSjis(s)), iconv.encode(s, 'cp932'));
  });
}

t('⭐CSV全体 (ヘッダー + 住所行) が iconv-lite と一致する', () => {
  const ships = [{
    shipmentConfirmationId: 'FBA15GH9C0L0', fcCode: 'XJE1', boxCount: 3,
    address: { postalCode: '243-0488', addressLine1: '神奈川県 海老名市 中新田3290 MFLP海老名I 2階' },
  }];
  const csv = ext.buildFukutsuCsv(ships, '20260825').csv;
  assert.equal(csv, server.buildFukutsuCsv(ships, '20260825').csv);
  const bytes = Buffer.from(encodeSjis(csv));
  assert.deepEqual(bytes, iconv.encode(csv, 'cp932'));
  assert.equal(iconv.decode(bytes, 'cp932'), csv, '往復して元に戻る');
  assert.ok(bytes.indexOf(Buffer.from([0x90, 0x5f, 0x93, 0xde])) > 0, '「神奈」が Shift_JIS のバイトで入っている');
});

t('🚨Shift_JISにできない文字は投げる (黙って ? に化けさせない)', () => {
  assert.throws(() => encodeSjis('神奈川県 𠮷野家'), /Shift_JISにできない文字があります: /);
  assert.throws(() => encodeSjis('aéb'), /Shift_JISにできない文字があります: é/);
});

t('ASCII だけなら素通し', () => {
  assert.deepEqual(Buffer.from(encodeSjis('ABC,123\n')), Buffer.from('ABC,123\n', 'latin1'));
});

console.log(`\n${pass} 件すべて通過`);
