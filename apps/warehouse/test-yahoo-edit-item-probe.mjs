/**
 * test-yahoo-edit-item-probe.mjs — editItem 検証スクリプトの部品の検証
 *
 * Yahoo にも VPS にも接続しない。XML の潰し方・差分の取り方・門番だけを試す。
 * ここが間違っていると「消えた項目を見落とす」= 検証そのものが嘘になる。
 *
 * 実行: node apps/warehouse/test-yahoo-edit-item-probe.mjs
 */
import {
  flattenXml, diff, isPricePath, isItemPricePath, collateralOf, guardTestCode, guardTestItem, itemPriceOf, editItemFailure,
  itemBaseOf, isDirectChild,
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

  // ★実測の構造: ResultSet > Result の二段。商品本体はルート直下ではない (2026-09-01)
  const real = await flattenXml('<ResultSet totalResultsReturned="1"><Result><ItemCode>zz-1</ItemCode>'
    + '<Name>zz検証用 テスト</Name><Price>1080</Price>'
    + '<SubCodes><SubCode code="a"><Price>1200</Price></SubCode></SubCodes></Result></ResultSet>');
  const base = itemBaseOf(real, 'zz-1');
  eq(base, 'ResultSet[0]/Result[0]', '★商品本体の場所は ItemCode から見つける (ルート直下と決め打ちしない)');
  ok(isItemPricePath(base + '/Price[0]', base), '商品本体の価格');
  ok(!isItemPricePath(base + '/SubCodes[0]/SubCode[0]/Price[0]', base), '★SKU の価格は「商品本体の価格」ではない');
  ok(!isItemPricePath(base + '/Price[0]', null), '商品の場所が分からなければ本体扱いしない (fail-closed)');
  eq(itemPriceOf(real, 'zz-1'), 1080, '★商品本体の価格を取る (SKU の価格と取り違えない)');
  eq(itemBaseOf(await flattenXml('<R><Price>100</Price></R>'), 'zz-1'), null, 'ItemCode が無ければ場所を決めない');
  eq(itemPriceOf(await flattenXml('<R><Price>100</Price></R>'), 'zz-1'), null, '場所が分からなければ価格も読まない');
  eq(itemPriceOf(await flattenXml('<ResultSet><Result><ItemCode>x</ItemCode><Price>お問い合わせ</Price></Result></ResultSet>'), 'x'),
    null, '整数で読めなければ null');

  // ★応答に商品が複数あっても、指定したコードの商品だけを本体とする (Codex R1)
  const two = await flattenXml('<ResultSet>'
    + '<Result><ItemCode>other-1</ItemCode><Name>zz検証用 まぎらわしい商品</Name><Price>1</Price></Result>'
    + '<Result><ItemCode>zz-1</ItemCode><Name>本物</Name><Price>2</Price></Result></ResultSet>');
  eq(itemBaseOf(two, 'zz-1'), 'ResultSet[0]/Result[1]', '★指定したコードの商品を選ぶ (最初の1つではない)');
  eq(itemPriceOf(two, 'zz-1'), 2, '価格も指定した商品のもの');
  eq(itemBaseOf(two, 'ZZ-1'), 'ResultSet[0]/Result[1]', '大小文字は無視して照合');
  eq(itemBaseOf(two, 'not-there'), null, '一致が無ければ決めない');
  eq(itemBaseOf(two, ''), null, 'コード未指定なら決めない (fail-closed)');
  const dup = await flattenXml('<ResultSet>'
    + '<Result><ItemCode>zz-1</ItemCode><Price>1</Price></Result>'
    + '<Result><ItemCode>zz-1</ItemCode><Price>2</Price></Result></ResultSet>');
  eq(itemBaseOf(dup, 'zz-1'), null, '★同じコードが2つあれば決めない (取り違えるくらいなら動かさない)');
  // ★商品直下の Price が複数 = 想定外の構造。最初の1つを採ると、その値を基準に値付けしてしまう (Codex R3)
  const twoPrices = await flattenXml('<ResultSet><Result><ItemCode>zz-1</ItemCode><Price>100</Price><Price>200</Price></Result></ResultSet>');
  eq(itemPriceOf(twoPrices, 'zz-1'), null, '★価格が2つあれば読めない扱い (書き込みに進ませない)');
  // ★安全整数の外だと +1 しても同じ値になり、変わっていないのに変わったと読める (Codex R4)
  const huge = await flattenXml('<ResultSet><Result><ItemCode>zz-1</ItemCode><Price>9007199254740993</Price></Result></ResultSet>');
  eq(itemPriceOf(huge, 'zz-1'), null, '★安全整数の外は読めない扱い');

  // 道すじの直下判定 ([ ] を含む道すじでも壊れない)
  ok(isDirectChild('A[0]/B[0]/Name[0]', 'A[0]/B[0]', 'Name'), '直下なら true');
  ok(!isDirectChild('A[0]/B[0]/C[0]/Name[0]', 'A[0]/B[0]', 'Name'), '孫は false');
  ok(!isDirectChild('A[0]/B[0]/NameX[0]', 'A[0]/B[0]', 'Name'), '別のタグは false');
  ok(!isDirectChild('A[0]/B[0]/Name[0]', null, 'Name'), 'base が無ければ false');
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
  const item = (inner) => '<ResultSet><Result><ItemCode>zz-1</ItemCode>' + inner + '</Result></ResultSet>';
  const B = 'ResultSet[0]/Result[0]';
  const before = await flattenXml(item('<Name>名前</Name><Price>100</Price><Caption>説明</Caption>'));
  const bb = itemBaseOf(before, 'zz-1');
  eq(collateralOf(diff(before, await flattenXml(item('<Name>名前</Name><Price>101</Price><Caption>説明</Caption>'))), bb).length,
    0, '価格だけ変わったなら 0 (= 部分更新)');
  eq(collateralOf(diff(before, await flattenXml(item('<Name>名前</Name><Price>101</Price>'))), bb).map((x) => x.path),
    [B + '/Caption[0]'], '★消えた項目を数える');
  eq(collateralOf(diff(before, await flattenXml(item('<Name>別の名前</Name><Price>101</Price><Caption>説明</Caption>'))), bb).map((x) => x.path),
    [B + '/Name[0]'], '価格以外が変わったら数える');
  eq(collateralOf(diff(before, await flattenXml(item('<Name>名前</Name><Price>101</Price><Caption>説明</Caption><New>x</New>'))), bb).map((x) => x.path),
    [B + '/New[0]'], '★増えた項目も数える (無視すると誤って部分更新と結論する)');

  // ★SKU の価格が動いたのは「価格以外の変化」。見逃すと部分更新だと誤って結論する
  const withSku = await flattenXml(item('<Name>名前</Name><Price>100</Price><SubCodes><SubCode code="a"><Price>50</Price></SubCode></SubCodes>'));
  const skuMoved = await flattenXml(item('<Name>名前</Name><Price>101</Price><SubCodes><SubCode code="a"><Price>60</Price></SubCode></SubCodes>'));
  eq(collateralOf(diff(withSku, skuMoved), itemBaseOf(withSku, 'zz-1')).map((x) => x.path),
    [B + '/SubCodes[0]/SubCode[0]/Price[0]'], '★SKU の価格が動いたら数える (商品本体の価格だけを除外する)');
}

console.log('\n── 本番商品では動かさない ──');
{
  eq(guardTestCode('zz-yahoo-m0-0901'), null, '検証用コードは通る');
  ok(guardTestCode('0726-001802'), '★本番の商品コードは拒否する');
  ok(/zz-/.test(guardTestCode('0726-001802')), '理由に必要な接頭辞を書く');
  ok(guardTestCode(''), 'コード未指定も拒否');
  eq(guardTestCode('ZZ-Yahoo-Test'), null, '大文字でも通る');

  // ★接頭辞だけでは足りない。商品そのものに目印があることを実物で確かめる
  const wrap = (inner) => '<ResultSet><Result><ItemCode>zz-1</ItemCode>' + inner + '</Result></ResultSet>';
  const marked = await flattenXml(wrap('<Name>zz検証用 editItem のテスト</Name><Price>100</Price>'));
  eq(guardTestItem(marked, 'zz-1'), null, '★実測どおりの二段構造 (ResultSet > Result) でも通る');
  const prod = await flattenXml(wrap('<Name>合皮補修シート ベージュ</Name><Price>577</Price>'));
  ok(guardTestItem(prod, 'zz-1'), '★目印が無い商品は拒否する (コードを取り違えても本番を触らない)');
  ok(/zz検証用/.test(guardTestItem(prod, 'zz-1')) && /合皮補修シート/.test(guardTestItem(prod, 'zz-1')),
    '理由に目印と実際の商品名を書く');
  ok(guardTestItem(await flattenXml(wrap('<Price>100</Price>')), 'zz-1'), '商品名を読めなければ拒否');
  ok(/特定できません/.test(guardTestItem(await flattenXml('<R><Name>zz検証用</Name></R>'), 'zz-1')),
    '★ItemCode が無ければ「特定できない」として拒否 (目印があっても通さない)');
  // ★応答に別商品が混ざっていて、そちらに目印があっても通さない (Codex R1)
  const mixed = await flattenXml('<ResultSet>'
    + '<Result><ItemCode>other-1</ItemCode><Name>zz検証用 まぎらわしい商品</Name><Price>1</Price></Result>'
    + '<Result><ItemCode>zz-1</ItemCode><Name>合皮補修シート ベージュ</Name><Price>577</Price></Result></ResultSet>');
  ok(guardTestItem(mixed, 'zz-1'), '★別商品に目印があっても、指定した商品に無ければ拒否する');
  ok(/合皮補修シート/.test(guardTestItem(mixed, 'zz-1')), '理由は指定した商品の名前');
  // ★入れ子のどこかに目印があっても通さない (商品本体の名前だけを見る)
  const nested = await flattenXml(wrap('<Name>合皮補修シート ベージュ</Name><Options><Option><Name>zz検証用</Name></Option></Options>'));
  ok(guardTestItem(nested, 'zz-1'), '★入れ子の Name に目印があっても拒否する');
  ok(/合皮補修シート/.test(guardTestItem(nested, 'zz-1')), '理由には商品本体の名前を出す');
  // ★商品名が複数あるのは想定外。目印つきが混ざっていても通さない (Codex R2)
  const twoNames = await flattenXml(wrap('<Name>合皮補修シート ベージュ</Name><Name>zz検証用</Name><Price>577</Price>'));
  ok(guardTestItem(twoNames, 'zz-1'), '★商品名が2つあれば拒否する (目印つきが混ざっていても通さない)');
  ok(/2 個/.test(guardTestItem(twoNames, 'zz-1')), '理由に個数を書く: ' + guardTestItem(twoNames, 'zz-1'));
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
