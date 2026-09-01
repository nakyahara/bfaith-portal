/**
 * test-yahoo-edit-item-probe.mjs — editItem 検証スクリプトの部品の検証
 *
 * Yahoo にも VPS にも接続しない。XML の潰し方と差分の取り方だけを試す。
 * ここが間違っていると「消えた項目を見落とす」= 検証そのものが嘘になる。
 *
 * 実行: node apps/warehouse/test-yahoo-edit-item-probe.mjs
 */
import { flattenXml, diff, isPricePath, guardTestCode } from './yahoo-edit-item-probe.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

console.log('\n── XML を「道すじ → 値」に潰す ──');
{
  const xml = '<Result><ItemCode>zz-1</ItemCode><Name>テスト商品</Name><Price>1000</Price></Result>';
  const m = flattenXml(xml);
  eq(m.get('Result[0]/ItemCode[0]'), 'zz-1', '入れ子の道すじで引ける');
  eq(m.get('Result[0]/Name[0]'), 'テスト商品', '日本語も読める');
  eq(m.get('Result[0]/Price[0]'), '1000', '価格');
  eq(m.size, 3, '項目数');
}

console.log('\n── 同じタグが並ぶときは連番を振る (取り違え防止) ──');
{
  const xml = '<R><SubCodes><SubCode code="a"><Price>100</Price></SubCode><SubCode code="b"><Price>200</Price></SubCode></SubCodes></R>';
  const m = flattenXml(xml);
  eq(m.get('R[0]/SubCodes[0]/SubCode[0]/Price[0]'), '100', '1つ目');
  eq(m.get('R[0]/SubCodes[0]/SubCode[1]/Price[0]'), '200', '2つ目');
  eq(m.get('R[0]/SubCodes[0]/SubCode[0]@code'), 'a', '属性も拾う (Yahoo の SubCode は属性にコードがある)');
  eq(m.get('R[0]/SubCodes[0]/SubCode[1]@code'), 'b', '2つ目の属性');
}

console.log('\n── 空タグ・自己終了タグでも階層が崩れない ──');
{
  const m = flattenXml('<R><A/><B>1</B><C></C><D>2</D></R>');
  eq(m.get('R[0]/B[0]'), '1', '自己終了タグの次が正しい階層');
  eq(m.get('R[0]/D[0]'), '2', '空タグの次も正しい階層');
  ok(!m.has('R[0]/A[0]'), '中身の無いタグは値として持たない');
}

console.log('\n── 差分: 変わった / 消えた / 増えた を分ける ──');
{
  const before = flattenXml('<R><Name>元の名前</Name><Price>1000</Price><Caption>説明</Caption></R>');
  const after = flattenXml('<R><Name>元の名前</Name><Price>1001</Price><Extra>x</Extra></R>');
  const d = diff(before, after);
  eq(d.changed.map((x) => x.path), ['R[0]/Price[0]'], '変わった項目');
  eq(d.changed[0], { path: 'R[0]/Price[0]', before: '1000', after: '1001' }, '前後の値を持つ');
  eq(d.removed.map((x) => x.path), ['R[0]/Caption[0]'], '★消えた項目 (全項目上書きならここに説明文が並ぶ)');
  eq(d.added.map((x) => x.path), ['R[0]/Extra[0]'], '増えた項目');
}

console.log('\n── 価格の道すじかどうか ──');
{
  ok(isPricePath('Result[0]/Price[0]'), '商品の価格');
  ok(isPricePath('Result[0]/SubCodes[0]/SubCode[0]/Price[0]'), 'SKU の価格');
  ok(!isPricePath('Result[0]/Name[0]'), '商品名は価格ではない');
  ok(!isPricePath('Result[0]/Caption[0]'), '説明文は価格ではない');
}

console.log('\n── 本番商品では動かさない ──');
{
  eq(guardTestCode('zz-yahoo-m0-0901'), null, '検証用コードは通る');
  ok(guardTestCode('0726-001802'), '★本番の商品コードは拒否する');
  ok(/zz-/.test(guardTestCode('0726-001802')), '理由に必要な接頭辞を書く');
  ok(guardTestCode(''), 'コード未指定も拒否');
  eq(guardTestCode('ZZ-Yahoo-Test'), null, '大文字でも通る');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
