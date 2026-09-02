/**
 * test-qoo10-apply.mjs — Qoo10 の価格更新まわりの検証 (M5)
 *
 * Qoo10 へは接続しない。miniPC 側の判定 (planQoo10Patch) と、
 * Render 側のクライアント・引き当てを、応答を差し替えて確かめる。
 *
 * 実行: node apps/price-update/test-qoo10-apply.mjs
 */
import { qooPriceToInt, shapeDetail, planQoo10Patch } from '../warehouse/qoo10-price-service.js';
import { makeQoo10Client, fetchQoo10ItemDetail } from './qoo10-apply.js';
import { fetchQoo10Prices } from './live-price.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

/** 実物の応答をなぞった形 (2026-09-02 に tp-react で確認) */
const RAW = {
  ItemNo: '1060223025', ItemStatus: 'S2', ItemTitle: '東京粉末 リアクト',
  SellerCode: 'tp-react', SellPrice: '2574.0000', SettlePrice: '2317.0000',
  ItemQty: '0', ExpireDate: '2073-08-10',
};

console.log('── 価格の読み方 (Qoo10 は "2574.0000" の形で返す) ──');
{
  eq(qooPriceToInt('2574.0000'), 2574, '小数部が全部0なら整数円');
  eq(qooPriceToInt('2574'), 2574, '整数そのもの');
  eq(qooPriceToInt('0'), 0, '★0 は「読めない」ではない (区別する)');
  eq(qooPriceToInt('2574.5000'), null, '★端数のある価格は読めない扱い (勝手に丸めない)');
  eq(qooPriceToInt('abc'), null, '数字でなければ null');
  eq(qooPriceToInt(''), null, '空は null');
  eq(qooPriceToInt(null), null, 'null は null');
}

console.log('\n── 応答の整形 ──');
{
  const d = shapeDetail(RAW);
  eq([d.itemNo, d.sellPrice, d.itemQty, d.expireDate], ['1060223025', 2574, 0, '2073-08-10'],
    '商品番号・価格・数量・販売終了日を読める');
  eq(d.itemStatus, 'S2', '販売状態');
  eq(shapeDetail({ ...RAW, ExpireDate: '2073/08/10' }).expireDate, null,
    '★日付の形が違えば null (そのまま送り返すと壊れるため)');
  eq(shapeDetail(null), null, '商品が無ければ null');
}

console.log('\n── 送る前の判定 (planQoo10Patch) ──');
{
  const d = shapeDetail(RAW);
  const go = planQoo10Patch(d, '1060223025', 2574, 2600);
  eq(go.proceed ? [go.proceed.next, go.proceed.qty, go.proceed.expireDate, go.proceed.sellerCode] : go,
    [2600, 0, '2073-08-10', 'tp-react'],
    '★通る時は Qty と ExpireDate の現在値を必ず持ち回る (省略の罠よけ)');

  const noop = planQoo10Patch(d, '1060223025', 2574, 2574);
  eq([noop.done.status, noop.done.body.state], [200, 'noop'], '同じ値なら noop');

  const conflict = planQoo10Patch(d, '1060223025', 2500, 2600);
  eq([conflict.done.status, conflict.done.body.state], [409, 'conflict'],
    '★記録時と今の価格が違えば送らない (楽観ロック)');
  eq(conflict.done.body.detail.conflicts[0].live, 2574, 'いまの価格を理由に残す');

  // ★Qty が読めない商品に送ると、省略扱いで在庫が 9999 になる (公式仕様)。送らない
  const noQty = planQoo10Patch(shapeDetail({ ...RAW, ItemQty: 'abc' }), '1060223025', 2574, 2600);
  eq([noQty.done.status, noQty.done.body.error], [400, 'PRESERVE_FIELDS_UNREADABLE'],
    '★数量が読めなければ送らない (在庫9999化の罠)');
  const noExp = planQoo10Patch(shapeDetail({ ...RAW, ExpireDate: '' }), '1060223025', 2574, 2600);
  eq(noExp.done.body.error, 'PRESERVE_FIELDS_UNREADABLE',
    '★販売終了日が読めなければ送らない (1年後化の罠)');

  // ★実行時にも販売状態を見る (記録した後に停止した商品へ送らない)
  const stopped = planQoo10Patch(shapeDetail({ ...RAW, ItemStatus: 'S1' }), '1060223025', 2574, 2600);
  eq([stopped.done.status, stopped.done.body.error], [400, 'NOT_ON_SALE'],
    '★販売中でない商品には送らない (実行時にも確かめる)');

  const noPrice = planQoo10Patch(shapeDetail({ ...RAW, SellPrice: '2574.5' }), '1060223025', 2574, 2600);
  eq(noPrice.done.body.error, 'CURRENT_PRICE_UNREADABLE', '価格が読めなければ送らない');
}

console.log('\n── 引き当て (fetchQoo10Prices) ──');
{
  const mk = (item) => async () => ({ ok: true, item });
  const base = shapeDetail(RAW);

  const r = await fetchQoo10Prices([{ key: 'row1', itemNo: '1060223025' }],
    { gapMs: 0, fetchQoo10ItemDetail: mk(base) });
  const got = r.get('row1');
  eq([got.found, got.price, got.skuCode], [true, 2574, '1060223025'],
    '★送り先は商品番号 (Qoo10 は商品に1つの価格)');
  eq(got.sellerCode, 'tp-react', '販売者商品コードも持ち回る (送信時に使う)');

  // ★販売中でない商品は確定させない (値付けしても客に見えない)
  const stopped = await fetchQoo10Prices([{ key: 'row2', itemNo: '1060223025' }],
    { gapMs: 0, fetchQoo10ItemDetail: mk({ ...base, itemStatus: 'S1' }) });
  eq(stopped.get('row2').found, false, '★販売中 (S2) 以外は対象外');
  ok(/販売中の商品ではありません/.test(stopped.get('row2').reason), '理由に状態を書く');

  // ★別の商品が返ったら確定させない
  const wrong = await fetchQoo10Prices([{ key: 'row3', itemNo: '9999999999' }],
    { gapMs: 0, fetchQoo10ItemDetail: mk(base) });
  eq(wrong.get('row3').found, false, '★別の商品が返ったら確定させない');

  // 取得できない
  const dead = await fetchQoo10Prices([{ key: 'row4', itemNo: '1060223025' }],
    { gapMs: 0, fetchQoo10ItemDetail: async () => { throw new Error('HTTP 500'); } });
  eq(dead.get('row4').found, false, '取得できなければ確定させない');
  ok(/HTTP 500/.test(dead.get('row4').reason), '理由に元のエラーを残す');
}

console.log('\n── 送信クライアント (makeQoo10Client) ──');
{
  const detail = { ok: true, item: shapeDetail(RAW) };
  const mkC = (patch) => makeQoo10Client({ getDetail: async () => detail, patch });

  // miniPC の応答 (楽天と同じ形) をそのまま返す = classify() がそのまま効く
  const okc = mkC(async () => ({ status: 200, body: { ok: true, state: 'applied', applied: { '1060223025': 2600 } } }));
  const r1 = await okc.patchItemPrices('1060223025', { operationId: 'op-12345678', expected: { 1060223025: 2574 }, prices: { 1060223025: 2600 } });
  eq([r1.status, r1.body.state], [200, 'applied'], '成功は applied のまま返す');

  const cf = mkC(async () => ({ status: 409, body: { ok: false, state: 'conflict', error: 'CONFLICT' } }));
  const r2 = await cf.patchItemPrices('1060223025', { operationId: 'op-12345678', expected: {}, prices: {} });
  eq([r2.status, r2.body.state], [409, 'conflict'], 'conflict もそのまま');

  const boom = mkC(async () => ({ status: 502, body: null }));
  const r3 = await boom.patchItemPrices('1060223025', { operationId: 'op-12345678', expected: {}, prices: {} });
  eq(r3.status, 502, '★5xx はそのまま (execute 側が「結果不明」に倒す)');

  // 照合の形は楽天とそろえる
  const got = await okc.fetchItemDetail('1060223025');
  eq(got.item.variants['1060223025'].standardPrice, '2574', '★照合の形は楽天とそろえる');

  const nf = makeQoo10Client({ getDetail: async () => ({ ok: false, message: '無い' }), patch: async () => ({}) });
  eq((await nf.fetchItemDetail('1060223025')).status, 'not_found', '取れなければ not_found');
}

console.log('\n── env が無ければ取りにいかない (fail-closed) ──');
{
  const save = { ...process.env };
  for (const k of ['WAREHOUSE_URL', 'CF_ACCESS_CLIENT_ID', 'CF_ACCESS_CLIENT_SECRET', 'WAREHOUSE_SERVICE_TOKEN']) delete process.env[k];
  let caught = null;
  try { await fetchQoo10ItemDetail('1060223025'); } catch (e) { caught = e; }
  ok(caught !== null && /未設定/.test(caught.message),
    '★miniPC の設定が無ければ取りにいかない: ' + (caught?.message || 'エラーが出なかった'));
  for (const k of Object.keys(process.env)) if (!(k in save)) delete process.env[k];
  Object.assign(process.env, save);
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
