/**
 * test-yahoo-apply.mjs — Yahoo の価格更新の判断を検証 (M3-1)
 *
 * Yahoo にも VPS にも接続しない。get-item-detail の応答を模したオブジェクトで判断だけを試す。
 * ここが緩むと「別商品に値付け」「セール価格を消す」「誰かの変更を踏み潰す」が起きる。
 *
 * 実行: node apps/price-update/test-yahoo-apply.mjs
 */
import { planYahooUpdate, makeYahooClient, toIntPrice } from './yahoo-apply.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

/** 実測どおりの応答 (セール価格は空 = 使っていない) */
const detail = (over = {}) => ({
  ok: true, ItemCode: 'zz-1', Price: 1000,
  SalePrice: null, SalePriceReadable: true, SubCodes: [], ...over,
});

console.log('\n── 価格の読み取り ──');
{
  eq(toIntPrice('1000'), 1000, '文字列の価格');
  eq(toIntPrice(1000), 1000, '数値の価格');
  eq(toIntPrice('1000.0'), null, '小数表記は読まない');
  eq(toIntPrice(''), null, '空は読まない');
  eq(toIntPrice('お問い合わせ'), null, '文字は読まない');
}

console.log('\n── 正常系 ──');
{
  const p = planYahooUpdate(detail(), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq([p.ok, p.currentPrice, p.price, p.noop], [true, 1000, 1001, false], '送ってよい');
  const same = planYahooUpdate(detail(), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1000 });
  eq([same.ok, same.noop], [true, true], '同じ価格なら noop (Yahoo を無駄に叩かない)');
}

console.log('\n── 楽観ロック (記録時と今の価格が違えば送らない) ──');
{
  const c = planYahooUpdate(detail({ Price: 1200 }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq([c.ok, c.status, c.body.error], [false, 409, 'CONFLICT'], '★現在価格が想定と違えば送らない');
  eq(c.body.detail.conflicts[0], { sku: 'zz-1', expected: 1000, live: 1200, reason: '現在価格が想定と違います' },
    '食い違いの中身を返す');
  const noExp = planYahooUpdate(detail(), 'zz-1', {}, { 'zz-1': 1001 });
  eq([noExp.ok, noExp.body.error], [false, 'EXPECTED_REQUIRED'], '★記録時の価格が無ければ送らない');
  const unreadable = planYahooUpdate(detail({ Price: 'お問い合わせ' }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq(unreadable.body.error, 'CURRENT_PRICE_UNREADABLE', '今の価格が読めなければ送らない');
}

console.log('\n── ★セール価格 (空文字を送ると消えるので、入っている商品は触らない) ──');
{
  const has = planYahooUpdate(detail({ SalePrice: 900 }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq([has.ok, has.status, has.body.error], [false, 400, 'SALE_PRICE_PRESENT'], '★セール価格が入っていれば送らない');
  ok(/900/.test(has.body.message), '理由に実際のセール価格を書く');
  const unreadable = planYahooUpdate(detail({ SalePriceReadable: false }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq(unreadable.body.error, 'SALE_PRICE_UNREADABLE',
    '★入っているか確かめられない時も送らない (「無い」と決めつけない)');
}

console.log('\n── ★別の商品に値付けしない ──');
{
  const other = planYahooUpdate(detail({ ItemCode: 'zz-2' }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq([other.ok, other.body.error], [false, 'ITEM_NOT_FOUND'], '★応答の商品コードが違えば送らない');
  ok(/zz-1/.test(other.body.message) && /zz-2/.test(other.body.message), '要求と応答の両方を書く');
  const gone = planYahooUpdate({ ok: false }, 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq(gone.body.error, 'ITEM_NOT_FOUND', '取得できなければ送らない');
  eq(planYahooUpdate(null, 'zz-1', {}, {}).body.error, 'ITEM_NOT_FOUND', '応答が無くても落ちない');
  // ★大小文字の違いは同じ商品として扱う
  eq(planYahooUpdate(detail({ ItemCode: 'ZZ-1' }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 }).ok, true,
    '大小文字の違いは同じ商品');
}

console.log('\n── ★SKU別価格がある商品は、いまの版では触らない ──');
{
  const sub = planYahooUpdate(detail({ SubCodes: [{ SubCode: 'a', Price: 900 }] }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq([sub.ok, sub.body.error], [false, 'SUBCODE_PRICE_UNSUPPORTED'], '★個別価格つきは送らない');
  // 価格が空 (商品価格を継承) の SKU は普通の商品として扱ってよい
  const inherit = planYahooUpdate(detail({ SubCodes: [{ SubCode: 'a', Price: null }] }), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1001 });
  eq(inherit.ok, true, '個別価格が無い (商品価格を継承する) SKU は対象');
}

console.log('\n── 送る値の検査 ──');
{
  eq(planYahooUpdate(detail(), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 0 }).body.error, 'INVALID_PRICE', '★0円は送らない');
  eq(planYahooUpdate(detail(), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': -1 }).body.error, 'INVALID_PRICE', '負数は送らない');
  eq(planYahooUpdate(detail(), 'zz-1', { 'zz-1': 1000 }, { 'zz-1': 1000.5 }).body.error, 'INVALID_PRICE', '小数は送らない');
  eq(planYahooUpdate(detail(), 'zz-1', { 'zz-1': 1000 }, {}).body.error, 'MULTIPLE_PRICES', '空は送らない');
  eq(planYahooUpdate(detail(), 'zz-1', { a: 1, b: 2 }, { a: 10, b: 20 }).body.error, 'MULTIPLE_PRICES',
    '★Yahoo は商品ごとに1つの価格。複数渡されたら送らない');
}

console.log('\n── クライアント: 楽天と同じ形の応答にそろえる ──');
{
  const calls = [];
  const client = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async (code, price) => {
      calls.push({ code, price });
      return { status: 200, json: { ok: true, updateOk: true, submitted: true, submits: [{ item_code: code, ok: true }] } };
    },
  });

  const got = await client.fetchItemDetail('zz-1');
  eq(got.item.variants['zz-1'].standardPrice, '1000', '★照合の再取得は楽天と同じ形 (variants[sku].standardPrice)');
  eq(got.item.manageNumber, 'zz-1', '商品コードも返す');

  const r = await client.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq([r.status, r.body.state], [200, 'applied'], '★成功は楽天と同じ形 (200 + state applied)');
  eq(r.body.applied, { 'zz-1': 1001 }, '適用内容を返す');
  eq(r.body.publish, { requested: true, ok: true }, '★反映を依頼できたかも返す');
  eq(calls, [{ code: 'zz-1', price: 1001 }], '送ったのは1回だけ');

  // 同じ価格なら送らない
  const noopClient = makeYahooClient({ getDetail: async () => detail(), postUpdate: async () => { throw new Error('送ってはいけない'); } });
  const n = await noopClient.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1000 } });
  eq([n.status, n.body.state], [200, 'noop'], '★同じ価格なら Yahoo を叩かない');

  // 反映が通らなかった時も成功扱いにしない印を残す
  // ★VPS は反映が失敗すると ok:false を返す (updateOk:true で「更新は通った」と分かる)
  const noPub = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async () => ({ status: 200, json: { ok: false, updateOk: true, submitted: true, submits: [{ item_code: 'zz-1', ok: false }] } }),
  });
  const np = await noPub.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq(np.body.publish, { requested: true, ok: false }, '★反映が通らなかったことが記録に残る');

  // 取得できない商品
  const gone = makeYahooClient({ getDetail: async () => ({ ok: false }) });
  eq((await gone.fetchItemDetail('zz-1')).item, null, '取得できなければ item は null');
}

console.log('\n── ★反映を依頼できなければ「終わった」と言わない ──');
{
  const mk = (json) => makeYahooClient({ getDetail: async () => detail(), postUpdate: async () => ({ status: 200, json }) });

  // 反映の依頼そのものをしていない
  const notSubmitted = await mk({ ok: false, updateOk: true, submitted: false, submits: [] })
    .patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  ok(notSubmitted.body.state !== 'applied', '★反映を依頼していなければ applied にしない');
  eq(notSubmitted.body.error, 'PUBLISH_FAILED', '理由が分かる');
  eq(notSubmitted.body.applied, { 'zz-1': 1001 }, '★価格が変わったことは記録に残す (戻せるように)');
  ok(/管理画面/.test(notSubmitted.body.message), '人に何をすればよいか書く');

  // 依頼はしたが失敗した (★VPS が実際に返す形: ok:false + updateOk:true)
  const failed = await mk({ ok: false, updateOk: true, submitted: true, submits: [{ item_code: 'zz-1', ok: false }] })
    .patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  ok(failed.body.state !== 'applied', '★反映の依頼が失敗しても applied にしない');
  eq(failed.body.publish, { requested: true, ok: false }, '依頼はしたが通らなかったと分かる');

  // submits が空 (何も反映していない) も成功にしない
  const empty = await mk({ ok: false, updateOk: true, submitted: true, submits: [] })
    .patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  ok(empty.body.state !== 'applied', '★反映の結果が空でも applied にしない');

  // ★更新そのものが通らなかった回は「価格が変わった」と言わない (戻す対象にしない)
  const updateNg = await mk({ ok: false, updateOk: false, updateBody: '<ResultSet><Status>NG</Status></ResultSet>', submits: [] })
    .patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  ok(updateNg.body.state !== 'applied', '更新が通らなければ applied にしない');
  eq(updateNg.body.applied, undefined, '★更新が通っていないのに applied を残さない');

  // 成功した回 (VPS は ok:true + updateOk:true を返す)
  const good = await mk({ ok: true, updateOk: true, submitted: true, submits: [{ item_code: 'zz-1', ok: true }] })
    .patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq([good.body.state, good.body.publish.ok], ['applied', true], '更新も反映も通れば applied');
}

console.log('\n── ★VPS へ「今いくらのはず」を渡す (送る直前にあちらでも照合してもらう) ──');
{
  const sent = [];
  const client = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async (code, price, expectedPrice) => {
      sent.push({ code, price, expectedPrice });
      return { status: 200, json: { ok: true, updateOk: true, submitted: true, submits: [{ item_code: code, ok: true }] } };
    },
  });
  await client.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq(sent, [{ code: 'zz-1', price: 1001, expectedPrice: 1000 }],
    '★「今 1000 円のはず」を渡す (VPS が送る直前に読み直して照合できるように)');
}

console.log('\n── ★VPS が「今の価格が違う」と言ってきたら conflict として扱う (Codex R4) ──');
{
  const client = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async () => ({
      status: 409,
      json: { ok: false, error: 'CONFLICT', conflict: { item_code: 'zz-1', reason: '現在価格が想定と違います', expected: 1000, live: 1200 } },
    }),
  });
  const r = await client.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq([r.status, r.body.state, r.body.error], [409, 'conflict', 'CONFLICT'],
    '★楽天と同じ conflict の形にそろえる (ひとまとめに失敗へ潰さない)');
  eq(r.body.detail.conflicts[0].live, 1200, '実際の価格も残す');

  // セール価格が送信直前に足された場合も、VPS が 409 で止めてくれる
  const sale = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async () => ({
      status: 409,
      json: { ok: false, error: 'CONFLICT', conflict: { item_code: 'zz-1', reason: 'セール価格 (900 円) が入っています', salePrice: 900 } },
    }),
  });
  const sr = await sale.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq(sr.body.state, 'conflict', '★送信直前にセール価格が足された時も止まる');
  ok(/セール価格/.test(sr.body.message), '理由が分かる: ' + sr.body.message);
}

console.log('\n── クライアント: 送信が通らなかった時 ──');
{
  const reject = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async () => ({ status: 400, body: '<ResultSet><Error><Message>だめ</Message></Error></ResultSet>', json: null }),
  });
  const r = await reject.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq([r.status, r.body.error], [400, 'YAHOO_REJECTED'], '★4xx は「意味の分かる失敗」(続行してよい)');
  ok(/だめ/.test(r.body.message), '応答の中身を理由に残す');

  const boom = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async () => ({ status: 502, body: 'bad gateway', json: null }),
  });
  const b = await boom.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  eq(b.status, 502, '★5xx はそのまま返す (execute 側が「結果不明」に倒す)');

  // ok:false の JSON (更新は通ったが反映で失敗、など) も成功にしない
  const notOk = makeYahooClient({
    getDetail: async () => detail(),
    postUpdate: async () => ({ status: 200, json: { ok: false, updateStatus: 200, updateBody: '<Status>NG</Status>' } }),
  });
  const no = await notOk.patchItemPrices('zz-1', { expected: { 'zz-1': 1000 }, prices: { 'zz-1': 1001 } });
  ok(no.body.state !== 'applied', '★ok:false を成功にしない');
}

console.log('\n── 送り先は商品コードでなければならない (カラバリの取り違え防止) ──');
{
  // Yahoo は商品に1つの価格しか持たない。色 (個別商品コード) はそれを継承する。
  // 呼び出し側が色のコードをキーにして渡してきたら、送る前に止める。
  // ここを通すと「商品の全色が書き換わったのに、照合は色のコードで探して失敗になる」
  const item = {
    ok: true, ItemCode: 'kara-1', Price: 577, SalePrice: null, SalePriceReadable: true,
    SubCodes: [{ SubCode: 'kara-1-BK', Price: null }, { SubCode: 'kara-1-CL', Price: null }],
  };
  const wrongKey = planYahooUpdate(item, 'kara-1', { 'kara-1-BK': 577 }, { 'kara-1-BK': 600 });
  eq(wrongKey.ok, false, '★色のコードをキーに渡されたら送らない');
  eq(wrongKey.body.error, 'SKU_KEY_MISMATCH', 'SKU_KEY_MISMATCH で止まる');
  ok(/kara-1/.test(wrongKey.body.message), '正しい送り先 (商品コード) を理由に書く');

  const rightKey = planYahooUpdate(item, 'kara-1', { 'kara-1': 577 }, { 'kara-1': 600 });
  eq([rightKey.ok, rightKey.price], [true, 600], '商品コードなら通る (この商品の全色が 600 円になる)');

  // 大文字小文字の違いは同じものとして扱う (どこかで正規化がずれても止まらないように)
  const caseDiff = planYahooUpdate({ ...item, ItemCode: 'KARA-1' }, 'KARA-1', { 'kara-1': 577 }, { 'kara-1': 600 });
  eq(caseDiff.ok, true, '大文字小文字の違いは同じ商品として扱う');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
