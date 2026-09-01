/**
 * test-yahoo-edit-item-fields.mjs — 必須項目の割り出しに使う部品の検証
 *
 * Yahoo にも VPS にも接続しない。応答の読み方と「前」の値の取り方だけを試す。
 * ここが間違うと「当てずっぽうの値で商品を書き換える」ことになる。
 *
 * 実行: node apps/warehouse/test-yahoo-edit-item-fields.mjs
 */
import { editItemError, isDefiniteRejection, fieldValueFrom, FIELD_SOURCES } from './yahoo-edit-item-fields.js';
import { flattenXml, itemBaseOf } from './yahoo-edit-item-probe.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// 実測の応答 (2026-09-01)
const NG_PATH = `<?xml version="1.0" encoding="UTF-8"?>
<ResultSet>
  <Result>
    <Status>NG</Status>
    <Error>
      <Target>path</Target>
      <Code>it-01011</Code>
      <Message><![CDATA[パスは必須です。]]></Message>
    </Error>
  </Result>
</ResultSet>`;

console.log('\n── 応答から「どの項目が足りないか」を読む ──');
{
  const e = editItemError({ status: 400, body: NG_PATH });
  eq([e.status, e.target, e.code], [400, 'path', 'it-01011'], '★実測の応答から Target と Code を読む');
  eq(e.message, 'パスは必須です。', 'CDATA のメッセージも読む');

  eq(editItemError({ status: 200, body: '<ResultSet><Result><Status>OK</Status></Result></ResultSet>' }), null, '成功なら null');
  // ★2xx でも Status OK が無ければ成功にしない (Codex R3)
  ok(editItemError({ status: 200, body: '' }), '★本文が空の 200 は成功にしない');
  ok(editItemError({ status: 200, body: '<html>maintenance</html>' }), '★見たことのない 200 は成功にしない');
  ok(editItemError({ status: 200, body: '' }).unrecognized, '「形が違う」と分かる印が付く');
  ok(!isDefiniteRejection(editItemError({ status: 200, body: '' })), '★形が違う 200 は弾かれたとも言い切らない (戻しに行く)');
  ok(editItemError({ status: 200, body: '<ResultSet><Result><Status>NG</Status></Result></ResultSet>' }),
    '★HTTP 200 でも Status NG は失敗');
  ok(editItemError({ status: 500, body: 'boom' }), 'HTTP エラーは失敗');
  eq(editItemError({ status: 200, body: '<ResultSet><Result><Status>OK</Status></Result></ResultSet>' }), null, '成功は成功');
}

console.log('\n── 「送る前に弾かれた」と言い切れるか ──');
{
  ok(isDefiniteRejection(editItemError({ status: 400, body: NG_PATH })),
    '★4xx + 項目名つき = 受け付けられていない (戻しに行かなくてよい)');
  ok(!isDefiniteRejection(editItemError({ status: 500, body: 'boom' })),
    '★500 は言い切れない (書き込まれたかもしれないので戻しに行く)');
  ok(!isDefiniteRejection(editItemError({ status: 200, body: '<Result><Status>NG</Status></Result>' })),
    '200 で NG も言い切れない');
  ok(!isDefiniteRejection(null), '成功に対しては false');
  ok(!isDefiniteRejection(editItemError({ status: 400, body: 'ただの文字列' })),
    '4xx でも中身が読めなければ言い切らない');
  // ★Code だけの見たことがない 400 は言い切らない (Codex R1)
  ok(!isDefiniteRejection(editItemError({ status: 400, body: '<Result><Code>xx-99</Code></Result>' })),
    '★項目を名指ししていない 400 は言い切らない');
  ok(!isDefiniteRejection(editItemError({ status: 400, body: '<Result><Target>path</Target></Result>' })),
    '★Target だけで Code も NG も無い 400 も言い切らない');
}

console.log('\n── 「前」の応答から値を取る ──');
{
  const xml = '<ResultSet><Result><ItemCode>zz-1</ItemCode>'
    + '<PathList><Path>その他</Path><Path origFlag="1"><![CDATA[精油・アロマ・ハーブ:ハーブ]]></Path></PathList>'
    + '<Name><![CDATA[zz検証用 テスト]]></Name><ProductCategory>13587</ProductCategory>'
    + '<Price>1000</Price><Caption><![CDATA[説明]]></Caption><Abstract><![CDATA[]]></Abstract>'
    + '</Result></ResultSet>';
  const flat = await flattenXml(xml);
  const base = itemBaseOf(flat, 'zz-1');

  eq(fieldValueFrom(flat, base, 'path'), '精油・アロマ・ハーブ:ハーブ',
    '★PathList に複数あっても origFlag="1" を選ぶ');
  eq(fieldValueFrom(flat, base, 'name'), 'zz検証用 テスト', '商品名');
  eq(fieldValueFrom(flat, base, 'product_category'), '13587', 'カテゴリ番号');
  eq(fieldValueFrom(flat, base, 'caption'), '説明', '説明文');
  eq(fieldValueFrom(flat, base, 'abstract'), null, '中身が空の項目は取れない (空文字を送らない)');
  // ★空文字が値として入っていても送らない
  const emptyName = await flattenXml('<ResultSet><Result><ItemCode>zz-1</ItemCode><Name> </Name></Result></ResultSet>');
  eq(fieldValueFrom(emptyName, itemBaseOf(emptyName, 'zz-1'), 'name'), null, '★空白だけの値は送らない');
  eq(fieldValueFrom(flat, base, 'headline'), null, '無い項目は null');
  eq(fieldValueFrom(flat, base, 'shipping_bogus'), null, '知らない項目は null');
  eq(fieldValueFrom(flat, null, 'name'), null, '商品の場所が分からなければ null');

  // ★どれか決められない時は送らない
  const two = await flattenXml('<ResultSet><Result><ItemCode>zz-1</ItemCode>'
    + '<PathList><Path>A</Path><Path>B</Path></PathList></Result></ResultSet>');
  eq(fieldValueFrom(two, itemBaseOf(two, 'zz-1'), 'path'), null,
    '★印が無くて複数あるなら選ばない (当てずっぽうで送らない)');
  const twoMarked = await flattenXml('<ResultSet><Result><ItemCode>zz-1</ItemCode>'
    + '<PathList><Path origFlag="1">A</Path><Path origFlag="1">B</Path></PathList></Result></ResultSet>');
  eq(fieldValueFrom(twoMarked, itemBaseOf(twoMarked, 'zz-1'), 'path'), null, '印が2つあっても選ばない');
}

console.log('\n── 送ってよい項目の一覧 ──');
{
  ok(FIELD_SOURCES.path && FIELD_SOURCES.name && FIELD_SOURCES.product_category, '実測で要求された path を含む');
  ok(!FIELD_SOURCES.quantity_bogus, '知らない項目は入っていない');
  // ★一覧はすべて「getItem のどのタグから取るか」を持つ (作れない値を送らないため)
  ok(Object.values(FIELD_SOURCES).every((s) => s && typeof s.tag === 'string' && s.tag.length > 0),
    '★どの項目も getItem のタグに対応づいている');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
