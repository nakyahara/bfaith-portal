/**
 * test-yahoo-detail-proxy.mjs — VPS の応答を呼び出し側へどう渡すかの検証
 *
 * ★ここで項目を落とすと、価格更新側が「確かめられない」と判断して1件も送れなくなる
 *   (2026-09-01 実機で踏んだ: SalePrice を渡していなかったため Yahoo の更新が全部止まった)。
 * VPS には接続せず、fetch を差し替えて応答の受け渡しだけを見る。
 *
 * 実行: node apps/rakuten-yahoo-sync/test-yahoo-detail-proxy.mjs
 */
import { fetchYahooItemDetail } from './lib/yahoo-detail-proxy.js';

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

process.env.YAHOO_PROXY_BASE_URL = process.env.YAHOO_PROXY_BASE_URL || 'http://proxy.test';
process.env.YAHOO_PROXY_SECRET = process.env.YAHOO_PROXY_SECRET || 'test-secret';

const realFetch = globalThis.fetch;
const withResponse = async (payload, fn) => {
  globalThis.fetch = async () => ({ ok: true, status: 200, text: async () => JSON.stringify(payload) });
  try { return await fn(); } finally { globalThis.fetch = realFetch; }
};

console.log('\n── ★セール価格を落とさずに渡す (落とすと1件も送れなくなる) ──');
{
  // 実測の形: セール価格を使っていない商品
  const noSale = await withResponse(
    { ok: true, ItemCode: 'zz-1', Name: 'テスト', Price: 1000, SubCodes: [], SalePrice: null, SalePriceReadable: true },
    () => fetchYahooItemDetail('zz-1'));
  eq([noSale.SalePrice, noSale.SalePriceReadable], [null, true],
    '★「セール価格は使っていない」と分かる形で渡る');

  const hasSale = await withResponse(
    { ok: true, ItemCode: 'zz-1', Price: 1000, SalePrice: 900, SalePriceReadable: true },
    () => fetchYahooItemDetail('zz-1'));
  eq([hasSale.SalePrice, hasSale.SalePriceReadable], [900, true], 'セール価格が入っていれば値も渡る');

  // VPS が古い (SalePrice を返さない) 場合は「読めなかった」に倒れる
  const old = await withResponse(
    { ok: true, ItemCode: 'zz-1', Price: 1000 },
    () => fetchYahooItemDetail('zz-1'));
  eq(old.SalePriceReadable, false,
    '★VPS が古くて返してこない時は「読めなかった」= 送らない側に倒れる');

  const unreadable = await withResponse(
    { ok: true, ItemCode: 'zz-1', Price: 1000, SalePrice: null, SalePriceReadable: false },
    () => fetchYahooItemDetail('zz-1'));
  eq(unreadable.SalePriceReadable, false, '読めなかったことがそのまま伝わる');
}

console.log('\n── 既存の項目も落ちていない ──');
{
  const d = await withResponse({
    ok: true, ItemCode: 'zz-1', ProductCategory: 13587, Path: '精油:ハーブ', Name: 'テスト商品',
    Price: 1000, SubCodes: [{ SubCode: 'a', Price: null }],
    Delivery: '0', PostageSet: '1', ShipWeight: null,
    SalePrice: null, SalePriceReadable: true,
  }, () => fetchYahooItemDetail('zz-1'));
  eq([d.ItemCode, d.ProductCategory, d.Path, d.Name], ['zz-1', 13587, '精油:ハーブ', 'テスト商品'], '商品の基本');
  eq(d.Price, 1000, '価格');
  eq(d.SubCodes, [{ SubCode: 'a', Price: null }], '個別商品コード');
  eq([d.Delivery, d.PostageSet, d.ShipWeight], ['0', '1', null], '発送まわり');
}

console.log(`\n${failed === 0 ? '✅ 全テスト通過' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
