/**
 * test-linegift-read.mjs — LINEギフトの「読むだけ」経路のテスト
 *
 * 実行: node apps/price-update/test-linegift-read.mjs
 *
 * ここで固定したいこと:
 *   ①**書き込みの口が無い**こと (増やされたら落とす)
 *   ②🚨**「引き当てできない」を「出品していない」と言わない**こと
 *     ([[feedback_existence_check_needs_authoritative_source]] — 楽天で 407件中135件を誤判定した)
 *   ③確定 (confirmed) させない条件が効くこと (販売中でない / セール中 / 価格が読めない / 別商品)
 */
import { fetchLinegiftPrices, reasonOf } from './linegift-read.js';
import { MALL_CAPABILITIES, UPDATABLE_MALLS, EXECUTABLE_MALLS, ITEM_PRICE_MALLS, findCapabilityProblems } from './mall-capabilities.js';
import { lookupItemIdHint, itemMatchesCode, toPriceView } from '../warehouse/linegift-price-service.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label}${JSON.stringify(a) === JSON.stringify(b) ? '' : ` (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`}`);

/** miniPC の応答を差し替えるための道具 */
function stub(map) {
  const calls = [];
  return {
    calls,
    fn: async (code) => { calls.push(code); return map[code.toLowerCase()] ?? { ok: false, error: 'ITEM_ID_UNKNOWN', message: 'なし' }; },
  };
}
const SALE_OK = { ok: true, itemId: 7842523, code: 'nikukyu15', itemName: '肉球クリーム 15g', status: 'sale', price: 848, salePrice: null, hasSale: false, webUrl: 'https://mall.line.me/xxx' };

console.log('── 能力の宣言 ──');
{
  const c = MALL_CAPABILITIES.linegift;
  ok(!!c, 'linegift が mall-capabilities に登録されている');
  eq(c.updatable, false, '★画面で新売価を入れられない (読むだけ)');
  eq(c.executable, false, '★API で書き込めない (送信経路を持たない)');
  eq(c.priceScope, 'item', '1商品1価格 (variations に price が無いことを実データで確認済)');
  ok(!!c.blockReason, '更新できない理由が書いてある');
  ok(!UPDATABLE_MALLS.includes('linegift'), 'UPDATABLE_MALLS に入っていない');
  ok(!EXECUTABLE_MALLS.includes('linegift'), 'EXECUTABLE_MALLS に入っていない');
  ok(ITEM_PRICE_MALLS.has('linegift'), 'ITEM_PRICE_MALLS には入っている');
  eq(findCapabilityProblems(), [], '能力の宣言に食い違いが無い');
}

console.log('\n── 🚨書き込みの口を作っていない ──');
{
  // ★LINEギフトの更新は「商品まるごと PATCH」しか無く、部分更新か未検証。
  //   将来ここに apply/patch を足す時は、必ず実測してからにする
  const mod = await import('./linegift-read.js');
  const writeish = Object.keys(mod).filter((k) => /apply|patch|update|write|set/i.test(k));
  eq(writeish, [], '★linegift-read.js に書き込みらしき関数が無い');
  const svc = await import('../warehouse/linegift-price-service.js');
  const svcWrite = Object.keys(svc).filter((k) => /apply|patch|update|write/i.test(k));
  eq(svcWrite, [], '★miniPC 側にも書き込みらしき export が無い');
}

console.log('\n── 読めた時 ──');
{
  const s = stub({ nikukyu15: SALE_OK });
  const out = await fetchLinegiftPrices([{ key: 'r1', code: 'nikukyu15' }], { fetchLinegiftItemByCode: s.fn, gapMs: 0 });
  const r = out.get('r1');
  eq(r.found, true, '確定する');
  eq(r.price, 848, '価格が整数円で返る');
  eq(r.itemCode, 'nikukyu15', '出品コードが返る');
  eq(r.webUrl, 'https://mall.line.me/xxx', '商品ページのURLが返る (目視確認用)');
  eq(r.reason, null, '理由は空');
}

console.log('\n── 確定させない条件 ──');
{
  const cases = [
    ['販売中でない', { ...SALE_OK, status: 'stop' }, '販売中の商品ではありません'],
    ['セール中 (価格あり)', { ...SALE_OK, salePrice: 700, hasSale: true }, 'セール価格が設定されています'],
    ['セール中 (idだけ)', { ...SALE_OK, salePrice: null, hasSale: true }, 'セール価格が設定されています'],
    ['価格が整数でない', { ...SALE_OK, price: 848.5 }, '整数円として読めません'],
    ['価格が0', { ...SALE_OK, price: 0 }, '整数円として読めません'],
    ['価格が無い', { ...SALE_OK, price: null }, '整数円として読めません'],
  ];
  for (const [label, body, expect] of cases) {
    const s = stub({ nikukyu15: body });
    const out = await fetchLinegiftPrices([{ key: 'r1', code: 'nikukyu15' }], { fetchLinegiftItemByCode: s.fn, gapMs: 0 });
    const r = out.get('r1');
    ok(r.found === false && r.price === null && String(r.reason).includes(expect), `${label} → 確定しない (${r.reason})`);
  }
}

console.log('\n── 🚨「出品していない」と言い切らない ──');
{
  // 引き当てできない = 受注実績が無いだけ。出品はされているかもしれない
  for (const err of ['ITEM_ID_UNKNOWN', 'ITEM_NOT_FOUND', 'CODE_MISMATCH']) {
    const msg = reasonOf({ error: err });
    ok(!/未出品|出品していない|出品がありません|存在しません/.test(msg), `${err}: 「未出品」と書いていない (${msg})`);
  }
  const msg = reasonOf({ error: 'ITEM_ID_UNKNOWN' });
  ok(/判定していません|分かりません/.test(msg), '★引き当てできないことと、出品の有無を区別して書いている');

  const s = stub({});   // 何も返さない = ITEM_ID_UNKNOWN
  const out = await fetchLinegiftPrices([{ key: 'r1', code: 'unsold-item' }], { fetchLinegiftItemByCode: s.fn, gapMs: 0 });
  ok(out.get('r1').found === false, '確定しない');
  ok(!/未出品/.test(out.get('r1').reason), '画面に出す文言も「未出品」ではない');
}

console.log('\n── 同じコードは1回だけ聞く / 通信失敗は落とさない ──');
{
  const s = stub({ nikukyu15: SALE_OK });
  await fetchLinegiftPrices(
    [{ key: 'a', code: 'nikukyu15' }, { key: 'b', code: 'NIKUKYU15' }, { key: 'c', code: 'nikukyu15' }],
    { fetchLinegiftItemByCode: s.fn, gapMs: 0 });
  eq(s.calls.length, 1, '大小違いも含めて1回だけ問い合わせる');

  const boom = { fn: async () => { throw new Error('接続できません'); } };
  const out = await fetchLinegiftPrices([{ key: 'r1', code: 'x' }], { fetchLinegiftItemByCode: boom.fn, gapMs: 0 });
  ok(out.get('r1').found === false && /接続できません/.test(out.get('r1').reason), '例外でも落ちずに理由を残す');
}

console.log('\n── miniPC 側: 手がかりの引き方 ──');
{
  const fakeDb = (rows) => ({ prepare: () => ({ all: () => rows }) });
  eq(lookupItemIdHint(fakeDb([{ item_id: 7842523 }]), 'nikukyu15').itemId, 7842523, '1件なら特定する');
  const none = lookupItemIdHint(fakeDb([]), 'nikukyu15');
  eq(none.itemId, null, '0件なら特定しない');
  ok(/判定していません/.test(none.reason), '★0件の理由に「出品の有無は判定していません」と書く');
  const many = lookupItemIdHint(fakeDb([{ item_id: 1 }, { item_id: 2 }]), 'x');
  eq(many.itemId, null, '★複数該当なら決めない (取り違え防止)');
  eq(lookupItemIdHint(fakeDb([{ item_id: 1 }]), '   ').itemId, null, '空コードは弾く');
}

console.log('\n── miniPC 側: 取り違え防止の照合 ──');
{
  const item = { code: 'roll-cake', variations: [{ code: 'roll-cake-001' }, { code: 'roll-cake-002' }] };
  ok(itemMatchesCode(item, 'roll-cake'), '商品コードで一致');
  ok(itemMatchesCode(item, 'ROLL-CAKE-002'), 'バリエーションコードで一致 (大小無視)');
  ok(!itemMatchesCode(item, 'other-item'), '★関係ないコードは一致させない');
  ok(!itemMatchesCode(item, ''), '空は一致させない');
  ok(!itemMatchesCode(null, 'roll-cake'), '商品が無ければ一致させない');
}

console.log('\n── miniPC 側: 価格の取り出し ──');
{
  const v = toPriceView({ id: 1, code: 'a', name: 'n', status: 'sale', price: 848, web_url: 'https://x', variations: [{ code: 'a' }] });
  eq(v.price, 848, '価格');
  eq(v.hasSale, false, '★sale_price / sale_id が無ければ「セール中でない」');
  eq(v.webUrl, 'https://x', '商品ページURL');
  eq(toPriceView({ price: 848, sale_id: 333 }).hasSale, true, '★sale_id だけでもセール中と見る');
  eq(toPriceView({ price: 848, sale_price: 700 }).hasSale, true, 'sale_price があればセール中');
  eq(toPriceView({ price: '848' }).price, null, '★文字列の価格は読めない扱い (整数円だけ受ける)');
  eq(toPriceView({ price: 848.5 }).price, null, '小数は読めない扱い');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
