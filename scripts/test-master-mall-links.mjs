/**
 * test-master-mall-links.mjs — 商品の画面の「モールの商品ページへのリンク」(apps/master-edit/mall-links.mjs・10/20 中原さん「商品マスタからそれぞれのモールに飛ぶリンク」)
 *
 * 確かめること:
 *   1 URL の組み立て (純粋な関数): 楽天 = 商品管理番号 (小文字・encode)・Yahoo = 商品コード.html・Qoo10 = 数字の商品番号だけ・Amazon = 10 けたの ASIN だけ (jp / us)・
 *     店のコードは env (形が違う = 既定)・空 / 形の違うコード / ページの番号を持たないモール (au PAY・LINE ギフト・メルカリ) = null・危ない文字 (javascript: ・ / ? # 空白) は URL にしない
 *   2 まとめ方 (純粋な関数): 同じ URL は 1 つ (FBA と自社発送が同じ ASIN)・まとめ売りは ×N・同じモールに 2 つ以上 = 一言で見分ける・上限を超えた分は「ほか N」・
 *     出品の無いモールは出さない・モールの順・楽天の 2 店目 / 別の国の ASIN は出さない・構成で当たった Yahoo があれば楽天の番号で探した方は出さない
 *   3 DB (PGlite・画面のロール master_edit で読む): この商品だけでできた出品だけ (ほかの商品と組んだ出品は出さない)・セットはセットの出品・消した出品は出さない・
 *     色違いの Yahoo (構成なし) は楽天の商品管理番号で当てる・ASIN が出品に無い = 前の夜の Amazon の出品一覧で埋める・出品が無い = 空・読めない = ok: false
 * 使い方: node scripts/test-master-mall-links.mjs
 */
import assert from 'node:assert/strict';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const SCRATCH = await temporaryTestDataDir(import.meta.url, 'master-mall-links-');
process.env.DATA_DIR = SCRATCH;
delete process.env.RAKUTEN_SHOP_SLUG;
delete process.env.YAHOO_SELLER_ID;

const M = await import('../apps/master-edit/mall-links.mjs');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 8).join('\n      ')); } };
const quiet = () => {};

console.log('1 URL の組み立て');
await t('楽天 = 商品管理番号 (小文字・encode)・Yahoo = 商品コード.html・Qoo10 = 商品番号・Amazon = ASIN (jp / us)', () => {
  assert.equal(M.mallItemUrl('rakuten', { itemId: 'Waterbowl-S', code: 'waterbowl-s-wh' }), 'https://item.rakuten.co.jp/b-faith/waterbowl-s/');
  assert.equal(M.mallItemUrl('rakuten', { itemId: 'ab_c-1' }), 'https://item.rakuten.co.jp/b-faith/ab_c-1/');
  assert.equal(M.mallItemUrl('rakuten', { itemId: '日本茶' }), 'https://item.rakuten.co.jp/b-faith/%E6%97%A5%E6%9C%AC%E8%8C%B6/', '日本語は encode');
  assert.equal(M.mallItemUrl('yahoo', { code: '0726-001802' }), 'https://store.shopping.yahoo.co.jp/b-faith01/0726-001802.html');
  assert.equal(M.mallItemUrl('yahoo', { code: 'ABC-01' }), 'https://store.shopping.yahoo.co.jp/b-faith01/abc-01.html', '小文字');
  assert.equal(M.mallItemUrl('qoo10', { code: '810186502', itemId: '810186502' }), 'https://www.qoo10.jp/g/810186502');
  assert.equal(M.mallItemUrl('amazon', { asin: 'b0abc12345' }), 'https://www.amazon.co.jp/dp/B0ABC12345');
  assert.equal(M.mallItemUrl('amazon_us', { asin: 'B0ABC12345' }), 'https://www.amazon.com/dp/B0ABC12345');
});
await t('作れない = null: 空・形の違う ASIN / 商品番号・楽天の商品管理番号が無い・ページの番号を持たないモール (au PAY・LINE ギフト・メルカリ)', () => {
  for (const [mall, ids] of [['rakuten', {}], ['rakuten', { code: 'abc' }], ['rakuten', { itemId: '  ' }], ['yahoo', { code: '' }], ['qoo10', { code: 'abc' }], ['qoo10', { code: '12a' }],
    ['amazon', { asin: 'B0ABC1234' }], ['amazon', { asin: 'B0ABC123456' }], ['amazon', { asin: 'B0ABC-1234' }], ['amazon', {}],
    ['aupay', { code: 'x', itemId: '1' }], ['linegift', { code: 'x' }], ['mercari', { code: 'x' }], ['unknown', { code: 'x' }]]) {
    assert.equal(M.mallItemUrl(mall, ids), null, `${mall} ${JSON.stringify(ids)}`);
  }
});
await t('危ない文字は URL にしない (javascript:・/ ? # % \\ 空白・改行・長すぎ)・URL はいつも https の決まったホスト', () => {
  for (const bad of ['a/b', 'a?b=1', 'a#b', 'a b', 'a\nb', 'a\\b', '%2e%2e', 'x'.repeat(101)]) {
    assert.equal(M.mallItemUrl('rakuten', { itemId: bad }), null, `楽天 ${JSON.stringify(bad)}`);
    assert.equal(M.mallItemUrl('yahoo', { code: bad }), null, `Yahoo ${JSON.stringify(bad)}`);
  }
  const u = M.mallItemUrl('yahoo', { code: 'javascript:alert(1)' });
  assert.ok(u.startsWith('https://store.shopping.yahoo.co.jp/b-faith01/'), u);
  assert.ok(!/[<>"' ]/.test(M.mallItemUrl('rakuten', { itemId: `x"><img src=x onerror=alert(1)>` }) || ''), '引用符・山かっこは encode される');
  assert.equal(M.mallItemUrl('qoo10', { code: 'javascript:1' }), null);
});
await t('店のコード = env (RAKUTEN_SHOP_SLUG・YAHOO_SELLER_ID)。形が違う = 既定', () => {
  try {
    process.env.RAKUTEN_SHOP_SLUG = 'other-shop'; process.env.YAHOO_SELLER_ID = 'store02';
    assert.equal(M.mallItemUrl('rakuten', { itemId: 'a' }), 'https://item.rakuten.co.jp/other-shop/a/');
    assert.equal(M.mallItemUrl('yahoo', { code: 'a' }), 'https://store.shopping.yahoo.co.jp/store02/a.html');
    process.env.RAKUTEN_SHOP_SLUG = 'evil.com/x'; process.env.YAHOO_SELLER_ID = 'a b';
    assert.equal(M.mallItemUrl('rakuten', { itemId: 'a' }), 'https://item.rakuten.co.jp/b-faith/a/');
    assert.equal(M.mallItemUrl('yahoo', { code: 'a' }), 'https://store.shopping.yahoo.co.jp/b-faith01/a.html');
  } finally { delete process.env.RAKUTEN_SHOP_SLUG; delete process.env.YAHOO_SELLER_ID; }
});

console.log('2 まとめ方');
const R = (mall, listing_code, more = {}) => ({ mall, shop_code: mall === 'amazon' ? 'main@A1VC38T7YXB528' : 'main', listing_code, mall_item_id: null, asin: null, marketplace_id: null, qty: 1, via: 'component', ...more });
await t('同じ URL は 1 つ・まとめ売りは ×N・モールの順 (楽天 → Amazon → Yahoo! → Qoo10)・1 つだけなら一言なし', () => {
  const out = M.buildMallLinks([
    R('qoo10', '810186502', { mall_item_id: '810186502' }),
    R('amazon', 'solo-fbm', { asin: 'B0SOLO0001', marketplace_id: 'A1VC38T7YXB528' }), R('amazon', 'solo-fba', { asin: 'B0SOLO0001', marketplace_id: 'A1VC38T7YXB528' }),
    R('amazon', 'solo-3p', { asin: 'B0SOLO0003', qty: 3 }),
    R('amazon', 'solo-noasin'),   // ASIN が無い = 出さない
    R('rakuten', 'solo', { mall_item_id: 'solo' }), R('rakuten', '360', { mall_item_id: 'solo' }),   // 別名 (AM / AL) = 同じページ
    R('yahoo', 'solo'),
    R('aupay', 'solo', { mall_item_id: '123' }),   // ページの番号を作れない = 出さない
  ]);
  assert.deepEqual(out.map((m) => m.mall), ['rakuten', 'amazon', 'yahoo', 'qoo10']);
  assert.deepEqual(out[0].links.map((l) => [l.url, l.text]), [['https://item.rakuten.co.jp/b-faith/solo/', '']]);
  assert.match(out[0].links[0].title, /出品コード solo・360/);
  assert.deepEqual(out[1].links.map((l) => [l.url, l.text]), [['https://www.amazon.co.jp/dp/B0SOLO0001', 'B0SOLO0001'], ['https://www.amazon.co.jp/dp/B0SOLO0003', '×3']]);
  assert.match(out[1].links[0].title, /seller SKU solo-fbm・solo-fba/);
  assert.match(out[1].links[1].title, /3 個のまとめ売り/);
  assert.equal(out[1].more, 0);
  assert.deepEqual(out[3].links.map((l) => l.url), ['https://www.qoo10.jp/g/810186502']);
});
await t('上限 (4) を超えた分は「ほか N」・1 個の出品と ×N が同じページ = 1 個として見せる', () => {
  const rows = Array.from({ length: 7 }, (_, i) => R('qoo10', String(100 + i), { mall_item_id: String(100 + i) }));
  rows.push(R('qoo10', '100', { mall_item_id: '100', qty: 2 }));
  const [q] = M.buildMallLinks(rows);
  assert.equal(q.links.length, M.MAX_PER_MALL);
  assert.equal(q.more, 3);
  assert.deepEqual(q.links.map((l) => l.text), ['100', '101', '102', '103']);
});
await t('出さない: 楽天 / Yahoo の 2 店目 (URL の店と違う)・別の国の ASIN・行が無い / null', () => {
  assert.deepEqual(M.buildMallLinks([R('rakuten', 'a', { mall_item_id: 'a', shop_code: 'second' }), R('yahoo', 'a', { shop_code: 'second' })]), []);
  assert.deepEqual(M.buildMallLinks([R('amazon', 'a', { asin: 'B0SOLO0001', marketplace_id: 'ATVPDKIKX0DER' })]), [], 'US の ASIN を日本のページにしない');
  assert.deepEqual(M.buildMallLinks([]), []);
  assert.deepEqual(M.buildMallLinks(null), []);
  assert.deepEqual(M.buildMallLinks([null, { mall: 'other' }]), []);
});
await t('Yahoo: 構成で当たった出品がある = 楽天の商品管理番号で探した方は出さない / 無い = それを出す (一言で分かる)', () => {
  const both = M.buildMallLinks([R('yahoo', 'solo'), R('yahoo', 'solo-parent', { via: 'rakuten_page' })]);
  assert.deepEqual(both[0].links.map((l) => l.url), ['https://store.shopping.yahoo.co.jp/b-faith01/solo.html']);
  const page = M.buildMallLinks([R('yahoo', 'w-parent', { via: 'rakuten_page' })]);
  assert.deepEqual(page[0].links.map((l) => l.url), ['https://store.shopping.yahoo.co.jp/b-faith01/w-parent.html']);
  assert.match(page[0].links[0].title, /楽天の商品管理番号と同じコード/);
});

console.log('3 DB (PGlite・画面のロール master_edit)');
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const IL = await import('../apps/company-db/ingest/amazon-listings.mjs');
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const sku = (code, kind = 'single') => ({ code, name: code, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000 });
const lr = await runInitialLoad(db, {
  skus: [sku('solo'), sku('w-bk'), sku('other'), sku('lonely'), sku('set1', 'set')], variationGroups: [],
  setComponents: [{ parentCode: 'set1', childCode: 'solo', qty: 1, source: 'ne' }, { parentCode: 'set1', childCode: 'other', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [], suppliers: [], supplierSkus: [], primarySuppliers: [],
}, { log: quiet, runId: 'load_mall_links', now: new Date(Date.now() - 86400e3) });
assert.equal(lr.ok, true, lr.error);
const sid = async (code) => (await pg.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0].id;
const S = { solo: await sid('solo'), wbk: await sid('w-bk'), other: await sid('other'), lonely: await sid('lonely'), set1: await sid('set1') };
async function listing(mall, code, comps, { shop = mall === 'amazon' ? 'main@A1VC38T7YXB528' : 'main', itemId = null, asin = null, status = 'active' } = {}) {
  const id = (await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, mall_item_id, status) values (1, $1, $2, $3, $4, $5) returning listing_id`, [mall, shop, code, itemId, status])).rows[0].listing_id;
  let i = 0;
  for (const [skuId, qty] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, $3, $4, 'exact', 'system')`, [id, skuId, qty, i++]);
  if (asin) {
    const ci = (await pg.query(`insert into core.catalog_items (marketplace_id, asin) values ($1, $2) on conflict (marketplace_id, asin) do update set asin = excluded.asin returning catalog_item_id`, [mall === 'amazon_us' ? 'ATVPDKIKX0DER' : 'A1VC38T7YXB528', asin])).rows[0].catalog_item_id;
    await pg.query('update core.listings set catalog_item_id = $1 where listing_id = $2', [ci, id]);
  }
  return id;
}
await listing('rakuten', 'solo', [[S.solo, 1]], { itemId: 'solo' });
await listing('yahoo', 'solo', [[S.solo, 1]]);
await listing('qoo10', '810186502', [[S.solo, 1]], { itemId: '810186502' });
await listing('amazon', 'solo-fba', [[S.solo, 1]], { asin: 'B0SOLO0001' });
await listing('amazon', 'solo-fbm', [[S.solo, 1]], { asin: 'B0SOLO0001' });
await listing('amazon', 'solo-3p', [[S.solo, 3]], { asin: 'B0SOLO0003' });
await listing('amazon', 'Solo-Report', [[S.solo, 1]]);                       // ASIN は出品一覧から
await listing('amazon', 'solo-none', [[S.solo, 1]]);                         // どこにも ASIN が無い = 出さない
await listing('amazon', 'solo-other', [[S.solo, 1], [S.other, 1]], { asin: 'B0MIXED001' });   // ほかの商品と組んだ出品 = 出さない
await listing('qoo10', '999999999', [[S.solo, 1]], { itemId: '999999999', status: 'deleted' });  // 消した出品 = 出さない
await listing('rakuten', 'solo2', [[S.solo, 1]], { itemId: 'solo2', shop: 'second' });   // 2 店目 = 出さない
// 色違い: 楽天は SKU ごとの出品 (商品管理番号 = 親)・Yahoo は親のコードの出品だけ (構成なし)
await listing('rakuten', 'w-bk', [[S.wbk, 1]], { itemId: 'W-Parent' });
await listing('yahoo', 'w-parent', []);
await listing('yahoo', 'unrelated', []);
// 楽天の商品管理番号と同じコードでも、構成が付いた Yahoo の出品 (別の商品・組んだ出品) は使わない (Codex R1 Medium)
await listing('rakuten', 'solo-rk2', [[S.solo, 1]], { itemId: 'other' });       // solo の楽天の別ページ (商品管理番号 = other)
await listing('yahoo', 'other', [[S.other, 1]]);                                // other の Yahoo の出品 = solo の画面に出さない
await listing('rakuten', 'solo-rk3', [[S.solo, 1]], { itemId: 'mixed-page' });
await listing('yahoo', 'mixed-page', [[S.solo, 1], [S.other, 1]]);              // 組んだ出品 = 出さない
// セット: セットの商品コードに当たる出品
await listing('rakuten', 'set1', [[S.set1, 1]], { itemId: 'set1' });
await listing('amazon', 'set1-amz', [[S.set1, 1]], { asin: 'B0SET00001' });
{
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  const row = (sku, asin) => ({ seller_sku: sku, item_name: sku, asin, open_date: today, open_date_raw: `${today} 10:00:00 JST`, fulfillment: 'FBA', channel_raw: 'AMAZON_NA', status: 'active', status_raw: 'Active', quantity: 1, price: '1000.00' });
  const rows = [row('solo-report', 'B0REPORT01'), row('solo-fba', 'B0SOLO0001')];
  const gen = Date.now() - 3600e3;
  const got = await IL.ingestAmazonListings(db, { shop_code: IL.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: 'r_mall_links', checksum: IL.listingChecksum(rows.map((r) => IL.listingRowOf(r))), row_count: rows.length, skipped_count: 0, rows }, { nowMs: Date.now() });
  assert.equal(got.status, 'applied');
}
const asEdit = async (fn) => { await pg.query('set role master_edit'); try { return await fn(); } finally { await pg.query('set role deploy'); } };
const urls = (r) => Object.fromEntries(r.malls.map((m) => [m.mall, m.links.map((l) => l.url + (l.text ? ` [${l.text}]` : ''))]));

await t('単品: この商品だけでできた出品 (×N も)・同じ ASIN は 1 つ・出品一覧で ASIN を埋める・組んだ出品 / 消した出品 / 2 店目 / ASIN の無い出品は出さない', async () => {
  const r = await asEdit(() => M.readMallLinks(db, S.solo));
  assert.equal(r.ok, true);
  assert.deepEqual(urls(r), {
    rakuten: ['https://item.rakuten.co.jp/b-faith/mixed-page/ [mixed-page]', 'https://item.rakuten.co.jp/b-faith/other/ [other]', 'https://item.rakuten.co.jp/b-faith/solo/ [solo]'],
    amazon: ['https://www.amazon.co.jp/dp/B0REPORT01 [B0REPORT01]', 'https://www.amazon.co.jp/dp/B0SOLO0001 [B0SOLO0001]', 'https://www.amazon.co.jp/dp/B0SOLO0003 [×3]'],
    yahoo: ['https://store.shopping.yahoo.co.jp/b-faith01/solo.html'],   // 構成の付いた other・mixed-page は出さない
    qoo10: ['https://www.qoo10.jp/g/810186502'],
  });
  // 構成で当たった Yahoo が無くても、構成の付いた (別の商品の) Yahoo の出品は楽天の番号で拾わない
  await pg.query(`update core.listings set status = 'deleted' where mall = 'yahoo' and listing_code = 'solo'`);
  try {
    assert.equal(urls(await asEdit(() => M.readMallLinks(db, S.solo))).yahoo, undefined, '別の商品・組んだ出品の Yahoo のページへ飛ばさない');
  } finally { await pg.query(`update core.listings set status = 'active' where mall = 'yahoo' and listing_code = 'solo'`); }
});
await t('色違い: Yahoo は楽天の商品管理番号と同じコードの出品 (構成なし) で当てる・楽天のページは商品管理番号 (小文字)', async () => {
  const r = await asEdit(() => M.readMallLinks(db, S.wbk));
  assert.deepEqual(urls(r), { rakuten: ['https://item.rakuten.co.jp/b-faith/w-parent/'], yahoo: ['https://store.shopping.yahoo.co.jp/b-faith01/w-parent.html'] });
});
await t('セット: セットの商品コードに当たる出品だけ (構成品の出品は出さない)', async () => {
  const r = await asEdit(() => M.readMallLinks(db, S.set1));
  assert.deepEqual(urls(r), { rakuten: ['https://item.rakuten.co.jp/b-faith/set1/'], amazon: ['https://www.amazon.co.jp/dp/B0SET00001'] });
});
await t('出品が無い = ok で空・出品一覧が読めない (権限なし) = ASIN の無い出品だけ出さない・出品が読めない = ok: false (画面は止めない)', async () => {
  const none = await asEdit(() => M.readMallLinks(db, S.lonely));
  assert.deepEqual(none, { ok: true, malls: [] });
  assert.deepEqual(await M.readMallLinks(null, S.solo), { ok: false });
  assert.deepEqual(await M.readMallLinks(db, 'x; drop'), { ok: false });
  const errs = []; const orig = console.error; console.error = (m) => errs.push(String(m));
  try {
    await pg.query('revoke select on ops.amazon_listing_report from master_edit');
    const r = await asEdit(() => M.readMallLinks(db, S.solo));
    assert.equal(r.ok, true);
    assert.deepEqual(urls(r).amazon, ['https://www.amazon.co.jp/dp/B0SOLO0001 [B0SOLO0001]', 'https://www.amazon.co.jp/dp/B0SOLO0003 [×3]']);
    await pg.query('revoke select on core.listings from master_edit');
    assert.deepEqual(await asEdit(() => M.readMallLinks(db, S.solo)), { ok: false });
  } finally {
    console.error = orig;
    await pg.query('grant select on ops.amazon_listing_report to master_edit');
    await pg.query('grant select on core.listings to master_edit');
  }
  assert.equal(errs.length, 2, errs.join('\n'));
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
if (ng) process.exitCode = 1;
