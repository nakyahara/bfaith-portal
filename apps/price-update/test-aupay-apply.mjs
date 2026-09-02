/**
 * test-aupay-apply.mjs — au PAY の商品取得クライアントの検証
 *
 * au PAY へは接続せず、応答 XML を差し替えて「読み違えないか」を確かめる。
 * ここが緩むと、別商品の価格を掴む / エラーを価格として読む、が起きる。
 *
 * 実行: node apps/price-update/test-aupay-apply.mjs
 */
import { parseSearchItemInfoXml, toIntPrice, fetchAupayItemDetail,
  planAupayUpdate, makeAupayClient } from './aupay-apply.js';
import { fetchAupayPrices } from './live-price.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

/**
 * 実物の応答をなぞった XML (2026-09-01 に 0726-001802 で確認した形)。
 *
 * ★カラバリは「選択肢の定義 (choicesStockVerticals)」「在庫行 (choicesStocks)」
 *   「画像 (choicesStocksImage)」の3か所に同じコードが出てくる。
 *   実際の組み合わせ数は **在庫行の数**。ここを取り違えると、画面の
 *   「N通りすべてが同じ価格になります」が実物と合わない
 *   (実測で 6通りの商品が 25 と出ていた)。テストもその形をなぞる
 */
function xmlOf({ status = '0', itemCode = '0726-001802', itemPrice = '577',
  itemName = '合皮補修シート', choices = 12, extra = '' } = {}) {
  const vertical = Array.from({ length: choices }, (_, i) =>
    `<choicesStockVerticals><choicesStockVerticalCode>c${i}</choicesStockVerticalCode>`
    + `<choicesStockVerticalName>色${i}</choicesStockVerticalName></choicesStockVerticals>`).join('')
    // 在庫行 = 実際の組み合わせ。定義と同じ数だけ並ぶ
    + Array.from({ length: choices }, (_, i) =>
      `<choicesStocks><choicesStockHorizontalCode>-</choicesStockHorizontalCode>`
      + `<choicesStockVerticalCode>c${i}</choicesStockVerticalCode>`
      + `<choicesStockCount>${100 + i}</choicesStockCount></choicesStocks>`).join('')
    // 画像にも同じコードが出てくる (これを数えると倍になる)
    + Array.from({ length: choices }, (_, i) =>
      `<choicesStocksImage><choicesStockVerticalCode>c${i}</choicesStockVerticalCode>`
      + `<choicesStockImageUrl>https://example.com/${i}.jpg</choicesStockImageUrl></choicesStocksImage>`).join('');
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<response><result><status>${status}</status></result><searchResult><itemInfo>
<lotNumber>784383638</lotNumber><itemName>${itemName}</itemName>
<itemCode>${itemCode}</itemCode><itemPrice>${itemPrice}</itemPrice>
<makerRetailPrice>9999</makerRetailPrice>
<taxSegment>1</taxSegment><postageSegment>2</postageSegment><postage></postage>
<deliveryMethod><deliveryMethodId>tuiseki-ok</deliveryMethodId>
<deliveryMethodName>追跡可能メール便</deliveryMethodName></deliveryMethod>
<description>とても長い商品説明がここに入る。itemPrice のような文字列を含むこともある</description>
<saleStatus>1</saleStatus>${extra}
</itemInfo><registerStock>${vertical}</registerStock></searchResult></response>`;
}

console.log('── 応答の読み取り ──');
{
  const d = parseSearchItemInfoXml(xmlOf());
  eq([d.ok, d.itemCode, d.itemPrice], [true, '0726-001802', 577], '商品コードと価格を読める');
  eq(d.itemName, '合皮補修シート', '商品名も読める');
  eq(d.choiceCount, 12,
    '★カラバリは在庫行の数で数える (定義・在庫・画像の3か所に出るコードを足し上げない)');
  eq(d.itemPriceReadable, true, '価格が返ってきたことを記録する');
  eq(d.deliveryMethodName, '追跡可能メール便', '配送方法の名前');

  // ★メーカー希望小売価格を設定価格と取り違えない
  ok(d.itemPrice !== 9999, '★makerRetailPrice を価格として拾わない');
}

console.log('\n── 読めない・おかしい応答は価格を出さない ──');
{
  const err = parseSearchItemInfoXml(`<response><result><status>1</status>
<error><code>CME0021</code><message>店舗IDが設定されていません。</message></error></result></response>`);
  eq([err.ok, err.error], [false, 'AUPAY_ERROR'], '★status=1 は失敗 (HTTP 200 でも成功にしない)');
  ok(/CME0021/.test(err.message), 'エラーコードを理由に残す');
  eq(err.itemPrice, null, '価格は出さない');

  eq(parseSearchItemInfoXml('').ok, false, '空の応答は失敗');
  eq(parseSearchItemInfoXml(`<response><result><status>0</status></result>
<searchResult/></response>`).error, 'ITEM_NOT_FOUND', '★商品が入っていなければ見つからない扱い');

  const noPrice = parseSearchItemInfoXml(xmlOf({ itemPrice: '' }));
  eq([noPrice.itemPrice, noPrice.itemPriceReadable], [null, false], '価格が空なら null');

  const notInt = parseSearchItemInfoXml(xmlOf({ itemPrice: '577.5' }));
  eq([notInt.itemPrice, notInt.itemPriceReadable], [null, true],
    '★整数円で読めない値は null。ただし「返ってきた」ことは残す');

  eq(toIntPrice('1,280'), 1280, 'カンマ入りは読める');
  eq(toIntPrice('0'), 0, '★0円は「読めない」ではない (区別する)');
  eq(toIntPrice('abc'), null, '数字でなければ null');
}

console.log('\n── registerStock 側の同名タグを拾わない ──');
{
  // itemInfo の外に itemCode があっても、中の値を使う
  const xml = xmlOf().replace('<registerStock>', '<registerStock><itemCode>WRONG</itemCode>');
  eq(parseSearchItemInfoXml(xml).itemCode, '0726-001802',
    '★itemInfo の中だけを見る (registerStock の値を拾わない)');

  // registerStock ごと無い商品は「カラバリ無し」ではなく「分からない」
  const noStock = parseSearchItemInfoXml(xmlOf().replace(/<registerStock>[\s\S]*?<\/registerStock>/, ''));
  eq(noStock.choiceCount, null, '★在庫情報が無い時は null (0 = カラバリ無し と混ぜない)');
  eq(parseSearchItemInfoXml(xmlOf({ choices: 0 })).choiceCount, 0, 'カラバリが無い商品は 0');
}

console.log('\n── 商品説明の中の偽タグに騙されない (Codex R1 高) ──');
{
  // ★au PAY は商品説明をそのまま返す。CDATA の中にタグらしき文字列が入っていても
  //   それは説明文であって値ではない。正規表現で拾うとここで誤読する
  const trap = xmlOf().replace(
    '<description>とても長い商品説明がここに入る。itemPrice のような文字列を含むこともある</description>',
    '<description><![CDATA[お得! <itemPrice>9999</itemPrice> <itemCode>WRONG-999</itemCode>'
    + '</itemInfo></searchResult> ここで切れたら誤読]]></description>');
  const d = parseSearchItemInfoXml(trap);
  eq(d.ok, true, '説明に偽タグがあっても読める');
  eq(d.itemPrice, 577, '★説明文の中の数字を価格として読まない');
  eq(d.itemCode, '0726-001802', '★説明文の中のコードを商品コードとして読まない');
  eq(d.choiceCount, 12, '★説明文の偽タグで範囲が切れない');

  // 説明が価格より前にあっても同じ (出現順に依存しない)
  const before = xmlOf().replace('<itemCode>0726-001802</itemCode>',
    '<description><![CDATA[<itemPrice>1</itemPrice>]]></description><itemCode>0726-001802</itemCode>');
  eq(parseSearchItemInfoXml(before).itemPrice, 577, '★偽タグが先に出てきても本物を読む');
}

console.log('\n── 属性つきタグ・壊れた XML ──');
{
  const attr = parseSearchItemInfoXml(xmlOf().replace(/<choicesStocks>/g, '<choicesStocks type="a">'));
  eq(attr.choiceCount, 12, '属性がついていても数えられる');

  eq(parseSearchItemInfoXml('<response><result>').error, 'UNREADABLE_RESPONSE',
    '★XML として読めなければ、価格を出さずに理由を残す');
  eq(parseSearchItemInfoXml('ただの文字列').error, 'UNREADABLE_RESPONSE', 'XML でないものも同じ');
}

console.log('\n── 引き当て (fetchAupayPrices) ──');
{
  const deps = {
    gapMs: 0,
    fetchAupayItemDetail: async (code) => parseSearchItemInfoXml(
      String(code).toLowerCase() === '0726-001802' ? xmlOf() : xmlOf({ status: '1' })),
  };
  const r = await fetchAupayPrices([{ key: '0726-001802', candidates: ['0726-001802'] }], deps);
  const got = r.get('0726-001802');
  eq([got.found, got.price, got.skuCode], [true, 577, '0726-001802'],
    '★送り先は商品コード (au PAY は商品に1つの価格)');
  eq(got.choiceCount, 12, '影響する色の数を持ち回る');

  // ★取り違え防止: 応答の商品コードが違えば確定させない
  const wrong = await fetchAupayPrices([{ key: 'abc-001', candidates: ['abc-001'] }], {
    gapMs: 0,
    fetchAupayItemDetail: async () => parseSearchItemInfoXml(xmlOf({ itemCode: 'zzz-999' })),
  });
  eq(wrong.get('abc-001').found, false, '★別の商品が返ったら確定させない');
  ok(/別の商品/.test(wrong.get('abc-001').reason), '理由に取り違えと書く');

  // ★候補コードがキーと違う書き方でも、その候補で当たれば確定させる。
  //   キーと比べていると、正しい商品を「別の商品」として捨てる (Codex R1 中)。
  //   ※探す順は「キー → 候補」なので、キーでは見つからない状況にして候補を試させる
  const viaCandidate = await fetchAupayPrices(
    [{ key: 'ne-internal', candidates: ['aupay-real-001'] }], {
      gapMs: 0,
      fetchAupayItemDetail: async (c) => (String(c) === 'aupay-real-001'
        ? parseSearchItemInfoXml(xmlOf({ itemCode: 'aupay-real-001' }))
        : parseSearchItemInfoXml(xmlOf({ status: '1' }))),
    });
  const vc = viaCandidate.get('ne-internal');
  eq([vc.found, vc.skuCode], [true, 'aupay-real-001'],
    '★候補で当たれば確定する (送り先は応答の商品コード)');

  // 取得できなかった時
  const dead = await fetchAupayPrices([{ key: 'abc-002', candidates: ['abc-002'] }], {
    gapMs: 0,
    fetchAupayItemDetail: async () => { throw new Error('HTTP 500'); },
  });
  eq(dead.get('abc-002').found, false, '取得できなければ確定させない');
  ok(/HTTP 500/.test(dead.get('abc-002').reason), '理由に元のエラーを残す');
}

console.log('\n── env が無ければ送らない (fail-closed) ──');
{
  const save = { ...process.env };
  delete process.env.AUPAY_PROXY_BASE_URL;
  delete process.env.AUPAY_PROXY_URL;
  let caught = null;
  try { await fetchAupayItemDetail('abc-001'); } catch (e) { caught = e; }
  ok(caught !== null && /未設定/.test(caught.message),
    '★プロキシの設定が無ければ取りにいかない: ' + (caught?.message || 'エラーが出なかった'));
  Object.assign(process.env, save);
}

console.log('\n── 送る前の判定 ──');
{
  const d = parseSearchItemInfoXml(xmlOf({ itemCode: 'zz-1', itemPrice: '980', choices: 0 }));

  const okPlan = planAupayUpdate(d, 'zz-1', { 'zz-1': 980 }, { 'zz-1': 981 });
  eq([okPlan.ok, okPlan.price, okPlan.currentPrice], [true, 981, 980], 'まっとうな更新は通る');

  const same = planAupayUpdate(d, 'zz-1', { 'zz-1': 980 }, { 'zz-1': 980 });
  eq([same.ok, same.noop], [true, true], '同じ値なら noop');

  // ★色のコードを送り先にしたら止める (au PAY は商品に1つの価格)
  const wrongKey = planAupayUpdate(d, 'zz-1', { 'zz-1-BK': 980 }, { 'zz-1-BK': 981 });
  eq([wrongKey.ok, wrongKey.body.error], [false, 'SKU_KEY_MISMATCH'],
    '★色のコードをキーに渡されたら送らない');

  // 楽観ロック
  const conflict = planAupayUpdate(d, 'zz-1', { 'zz-1': 900 }, { 'zz-1': 981 });
  eq([conflict.ok, conflict.status, conflict.body.state], [false, 409, 'conflict'],
    '★記録時と今の価格が違えば送らない');

  eq(planAupayUpdate(d, 'zz-1', {}, { 'zz-1': 981 }).body.error, 'EXPECTED_REQUIRED',
    '★記録時の価格が無ければ送らない');
  eq(planAupayUpdate(d, 'zz-1', { 'zz-1': 980 }, { a: 1, b: 2 }).body.error, 'MULTIPLE_PRICES',
    '★1商品に2つの価格は受けない');
  eq(planAupayUpdate(d, 'zz-1', { 'zz-1': 980 }, { 'zz-1': 0 }).body.error, 'INVALID_PRICE',
    '★0円は送らない');
  eq(planAupayUpdate(d, 'other', { other: 980 }, { other: 981 }).body.error, 'ITEM_NOT_FOUND',
    '★別の商品が返ってきたら送らない');
  eq(planAupayUpdate({ ok: false, message: 'だめ' }, 'zz-1', {}, {}).body.error, 'ITEM_NOT_FOUND',
    '取得できていなければ送らない');
  // 大文字小文字の違いは同じ商品
  const up = parseSearchItemInfoXml(xmlOf({ itemCode: 'ZZ-1', itemPrice: '980', choices: 0 }));
  eq(planAupayUpdate(up, 'ZZ-1', { 'zz-1': 980 }, { 'zz-1': 981 }).ok, true,
    '大文字小文字の違いは同じ商品として扱う');
}

console.log('\n── 送信クライアント ──');
{
  const detail = parseSearchItemInfoXml(xmlOf({ itemCode: 'zz-1', itemPrice: '980', choices: 0 }));
  const mk = (postUpdate) => makeAupayClient({ getDetail: async () => detail, postUpdate });

  const okc = mk(async () => ({ status: 200, json: { ok: true, applied: 981 } }));
  const r1 = await okc.patchItemPrices('zz-1', { expected: { 'zz-1': 980 }, prices: { 'zz-1': 981 } });
  eq([r1.status, r1.body.state, r1.body.applied['zz-1']], [200, 'applied', 981], '成功は applied');

  // ★VPS が ok:false を返したら成功にしない
  const ng = mk(async () => ({ status: 200, json: { ok: false, updateBody: '<status>1</status>' } }));
  const r2 = await ng.patchItemPrices('zz-1', { expected: { 'zz-1': 980 }, prices: { 'zz-1': 981 } });
  ok(r2.body.state !== 'applied', '★ok:false を成功にしない');

  // 409 は conflict のまま返す
  const cf = mk(async () => ({ status: 409, json: { ok: false, error: 'CONFLICT', conflict: { reason: '違う', live: 999 } } }));
  const r3 = await cf.patchItemPrices('zz-1', { expected: { 'zz-1': 980 }, prices: { 'zz-1': 981 } });
  eq([r3.status, r3.body.state], [409, 'conflict'], '★VPS の 409 は conflict のまま返す');

  // 5xx はそのまま (execute 側が「結果不明」に倒す)
  const boom = mk(async () => ({ status: 502, body: 'bad gateway', json: null }));
  const r4 = await boom.patchItemPrices('zz-1', { expected: { 'zz-1': 980 }, prices: { 'zz-1': 981 } });
  eq(r4.status, 502, '★5xx はそのまま返す');

  // 照合のための再取得は楽天と同じ形
  const got = await okc.fetchItemDetail('zz-1');
  eq(got.item.variants['zz-1'].standardPrice, '980', '★照合の形は楽天とそろえる');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
