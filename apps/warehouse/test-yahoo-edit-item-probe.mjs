/**
 * test-yahoo-edit-item-probe.mjs — editItem 検証スクリプトの部品の検証
 *
 * Yahoo にも VPS にも接続しない。XML の潰し方・差分の取り方・門番だけを試す。
 * ここが間違っていると「消えた項目を見落とす」= 検証そのものが嘘になる。
 *
 * 実行: node apps/warehouse/test-yahoo-edit-item-probe.mjs
 */
import {
  flattenXml, diff, isPricePath, collateralOf, guardTestCode, guardTestItem, itemPriceOf, editItemFailure,
} from './yahoo-edit-item-probe.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

console.log('\n── XML を「道すじ → 値」に潰す ──');
{
  const m = await flattenXml('<Result><ItemCode>zz-1</ItemCode><Name>テスト商品</Name><Price>1000</Price></Result>');
  eq(m.get('Result[0]/ItemCode[0]'), 'zz-1', '入れ子の道すじで引ける');
  eq(m.get('Result[0]/Name[0]'), 'テスト商品', '日本語も読める');
  eq(m.get('Result[0]/Price[0]'), '1000', '価格');
  eq(m.size, 3, '項目数');
  eq((await flattenXml('')).size, 0, '空文字でも落ちない');
  eq((await flattenXml(null)).size, 0, 'null でも落ちない');
}

console.log('\n── 同じタグが並ぶときは連番を振る (取り違え防止) ──');
{
  const m = await flattenXml('<R><SubCodes><SubCode code="a"><Price>100</Price></SubCode><SubCode code="b"><Price>200</Price></SubCode></SubCodes></R>');
  eq(m.get('R[0]/SubCodes[0]/SubCode[0]/Price[0]'), '100', '1つ目');
  eq(m.get('R[0]/SubCodes[0]/SubCode[1]/Price[0]'), '200', '2つ目');
  eq(m.get('R[0]/SubCodes[0]/SubCode[0]@code'), 'a', '属性も拾う (Yahoo の SubCode は属性にコードがある)');
  eq(m.get('R[0]/SubCodes[0]/SubCode[1]@code'), 'b', '2つ目の属性');
}

console.log('\n── ★自前の正規表現では読めない形 (Codex R1) ──');
{
  // CDATA の中に "<" が入る。商品説明は HTML なので実際に起きる
  const cdata = await flattenXml('<R><Caption><![CDATA[<b>太字</b>と<img src="x">]]></Caption><Price>100</Price></R>');
  eq(cdata.get('R[0]/Caption[0]'), '<b>太字</b>と<img src="x">', '★CDATA の中身をそのまま読む');
  eq(cdata.get('R[0]/Price[0]'), '100', 'CDATA の後ろの項目も読める');

  // シングルクォートの属性
  const sq = await flattenXml("<R><SubCode code='x'><Price>1</Price></SubCode></R>");
  eq(sq.get('R[0]/SubCode[0]@code'), 'x', "★シングルクォートの属性も拾う");

  // 実体参照
  const ent = await flattenXml('<R><Name>A&amp;B &lt;C&gt;</Name></R>');
  eq(ent.get('R[0]/Name[0]'), 'A&B <C>', '★実体参照をほどく (ほどけないと差分が嘘になる)');

  // テキストが CDATA と地の文に分かれる
  const mixed = await flattenXml('<R><Name>前<![CDATA[中]]>後</Name></R>');
  eq(mixed.get('R[0]/Name[0]'), '前中後', '★分かれたテキストを連結する');

  // 名前空間つきタグ
  const ns = await flattenXml('<R xmlns:y="urn:y"><y:Price>500</y:Price></R>');
  eq(ns.get('R[0]/y:Price[0]'), '500', '名前空間つきでも読める');
}

console.log('\n── 空タグ・自己終了タグでも階層が崩れない ──');
{
  const m = await flattenXml('<R><A/><B>1</B><C></C><D>2</D></R>');
  eq(m.get('R[0]/B[0]'), '1', '自己終了タグの次が正しい階層');
  eq(m.get('R[0]/D[0]'), '2', '空タグの次も正しい階層');
  ok(!m.has('R[0]/A[0]'), '中身の無いタグは値として持たない');
}

console.log('\n── 差分: 変わった / 消えた / 増えた を分ける ──');
{
  const before = await flattenXml('<R><Name>元の名前</Name><Price>1000</Price><Caption>説明</Caption></R>');
  const after = await flattenXml('<R><Name>元の名前</Name><Price>1001</Price><Extra>x</Extra></R>');
  const d = diff(before, after);
  eq(d.changed.map((x) => x.path), ['R[0]/Price[0]'], '変わった項目');
  eq(d.changed[0], { path: 'R[0]/Price[0]', before: '1000', after: '1001' }, '前後の値を持つ');
  eq(d.removed.map((x) => x.path), ['R[0]/Caption[0]'], '★消えた項目 (全項目上書きならここに説明文が並ぶ)');
  eq(d.added.map((x) => x.path), ['R[0]/Extra[0]'], '増えた項目');
}

console.log('\n── 価格の読み方 ──');
{
  ok(isPricePath('Result[0]/Price[0]'), '商品の価格');
  ok(isPricePath('Result[0]/SubCodes[0]/SubCode[0]/Price[0]'), 'SKU の価格');
  ok(!isPricePath('Result[0]/Name[0]'), '商品名は価格ではない');

  const flat = await flattenXml('<R><Price>1080</Price><SubCodes><SubCode code="a"><Price>1200</Price></SubCode></SubCodes></R>');
  eq(itemPriceOf(flat), 1080, '★商品本体の価格を取る (SKU の価格と取り違えない)');
  eq(itemPriceOf(await flattenXml('<R><Price>お問い合わせ</Price></R>')), null, '整数で読めなければ null');
  eq(itemPriceOf(await flattenXml('<R><Name>x</Name></R>')), null, '価格が無ければ null');
}

console.log('\n── 送信が成功したかの判定 (HTTP 200 でも失敗はある) ──');
{
  eq(editItemFailure({ status: 200, body: '<Result><Status>OK</Status></Result>' }), null, '成功');
  ok(editItemFailure({ status: 400, body: 'bad' }), 'HTTP エラー');
  ok(editItemFailure({ status: 200, body: '<Error><Message>価格が不正です</Message></Error>' }),
    '★HTTP 200 でも本文にエラーがあれば失敗扱い');
  ok(/価格が不正/.test(editItemFailure({ status: 200, body: '<Error><Message>価格が不正です</Message></Error>' })),
    '理由に本文を含める');
  ok(editItemFailure({ status: 200, body: '<Result><Code>2</Code></Result>' }), 'エラーコードつきも失敗扱い');
  eq(editItemFailure({ status: 200, body: '<Result><Code>0</Code></Result>' }), null, 'Code 0 は成功');
}

console.log('\n── 「価格以外の変化」の数え方 ──');
{
  const before = await flattenXml('<R><Name>名前</Name><Price>100</Price><Caption>説明</Caption></R>');
  eq(collateralOf(diff(before, await flattenXml('<R><Name>名前</Name><Price>101</Price><Caption>説明</Caption></R>'))).length,
    0, '価格だけ変わったなら 0 (= 部分更新)');
  eq(collateralOf(diff(before, await flattenXml('<R><Name>名前</Name><Price>101</Price></R>'))).map((x) => x.path),
    ['R[0]/Caption[0]'], '★消えた項目を数える');
  eq(collateralOf(diff(before, await flattenXml('<R><Name>別の名前</Name><Price>101</Price><Caption>説明</Caption></R>'))).map((x) => x.path),
    ['R[0]/Name[0]'], '価格以外が変わったら数える');
  eq(collateralOf(diff(before, await flattenXml('<R><Name>名前</Name><Price>101</Price><Caption>説明</Caption><New>x</New></R>'))).map((x) => x.path),
    ['R[0]/New[0]'], '★増えた項目も数える (無視すると誤って部分更新と結論する)');
  eq(collateralOf(diff(before, await flattenXml('<R><Name>名前</Name><Price>101</Price><Caption>説明</Caption><SubCodes><SubCode code="a"><Price>50</Price></SubCode></SubCodes></R>'))).length,
    2, 'SKU が増えたら SubCodes と SubCode の属性が増分として出る');
}

console.log('\n── 本番商品では動かさない ──');
{
  eq(guardTestCode('zz-yahoo-m0-0901'), null, '検証用コードは通る');
  ok(guardTestCode('0726-001802'), '★本番の商品コードは拒否する');
  ok(/zz-/.test(guardTestCode('0726-001802')), '理由に必要な接頭辞を書く');
  ok(guardTestCode(''), 'コード未指定も拒否');
  eq(guardTestCode('ZZ-Yahoo-Test'), null, '大文字でも通る');

  // ★接頭辞だけでは足りない。商品そのものに目印があることを実物で確かめる
  const marked = await flattenXml('<R><Name>zz検証用 editItem のテスト</Name><Price>100</Price></R>');
  eq(guardTestItem(marked), null, '目印つきの商品は通る');
  const real = await flattenXml('<R><Name>合皮補修シート ベージュ</Name><Price>577</Price></R>');
  ok(guardTestItem(real), '★目印が無い商品は拒否する (コードを取り違えても本番を触らない)');
  ok(/zz検証用/.test(guardTestItem(real)) && /合皮補修シート/.test(guardTestItem(real)), '理由に目印と実際の商品名を書く');
  ok(guardTestItem(await flattenXml('<R><Price>100</Price></R>')), '商品名を読めなければ拒否');
  // ★入れ子のどこかに目印があっても通さない (商品本体の名前だけを見る)
  const nested = await flattenXml('<R><Name>合皮補修シート ベージュ</Name><Options><Option><Name>zz検証用</Name></Option></Options></R>');
  ok(guardTestItem(nested), '★入れ子の Name に目印があっても拒否する');
  ok(/合皮補修シート/.test(guardTestItem(nested)), '理由には商品本体の名前を出す');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
