#!/usr/bin/env node
/**
 * test-company-db-sales-daily.mjs — 0021 = 売上の日次 mart.sales_daily (D7a) の試験 (PGlite)。
 *   按分 (最大剰余法・重みの 3 段・取消) / 粒度 (shop_code・出品・SKU) / run_id publish (指し先の差し替え・古い行は残る → purge) /
 *   作り直す日を DB が自分で見つける (watermark・session・上限つきの呼び直し・15 分のさかのぼり) / 検算の view /
 *   Render の受け口 (POST /orders/sales-daily/refresh・GET …/check) と送り手 refreshSalesDaily (本物の router を HTTP で)
 * 🚨 2 接続の並行 (advisory lock) は PGlite では書けない (scripts/test-company-db-concurrency.mjs の系統で本番に対して確かめる)
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { refreshSalesDaily, salesNote } from '../apps/company-db/push/mall-orders.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const pg = new PGlite();
const applied = await applyMigrations(pgliteAdapter(pg), { log: quiet });
assert.ok(applied.applied.includes('0021'), '0021 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
const num = async (sql, p = []) => Number((await one(sql, p)).n);

const sku = async (code) => {   // 単品の SKU は商品 1 つにつき 1 つ (ux_skus_single_product)
  const prod = (await one(`insert into core.products (company_id, name) values (1, $1) returning product_id`, [`見本 ${code}`])).product_id;
  return Number((await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', $2, $2) returning sku_id`, [prod, code])).sku_id);
};
const skuA = await sku('sku-a'), skuB = await sku('sku-b'), skuC = await sku('sku-c');
const lstA = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', 'main', 'W-A', 'active') returning listing_id`)).listing_id);
let seq = 0;
const H = (x = {}) => ({ source_system: 'mall_api', shop_code: '1', ordered_at: '2026-03-01T10:00:00+09:00', status: 'shipped', amount_source: 'mall_api', source_updated_at: '2026-09-19T00:00:00Z', transform_version: 't-1',
  content_hash: 'h' + (++seq), total_amount_jpy: null, items_amount_jpy: null, shipping_fee_jpy: null, shop_coupon_jpy: null, mall_coupon_jpy: null, points_used_jpy: null, ...x });
const L = (key, x = {}) => ({ line_key: key, listing_code: null, sku_code: 'sku-a', qty: 1, cancelled_qty: 0, unit_price_jpy: null, line_amount_jpy: 1000, tax_rate: null, amount_source: 'mall_api', ...x });
const apply = async (mall, scope, no, header, lines, batch = ++seq) =>
  (await one(`select core.apply_order_batch(1::smallint, $1, $2, $3, $4::bigint, $5::jsonb, $6::jsonb) as r`, [mall, scope, no, batch, JSON.stringify(header), JSON.stringify(lines)])).r;
const refresh = async (mall, scope, x = {}) => one(`select * from mart.refresh_sales_daily(1::smallint, $1, $2, $3::timestamptz, $4::int, $5::boolean, 'test')`, [mall, scope, x.session ?? null, x.limit ?? 31, x.reset ?? false]);
const refreshAll = async (mall, scope, x = {}) => { let r = await refresh(mall, scope, x); let calls = 1; while (Number(r.remaining) > 0) { r = await refresh(mall, scope, { ...x, reset: false, session: r.session_at }); calls++; } return { ...r, calls }; };
const daily = async (mall, where = 'true') => (await all(`select date_jst::text as d, shop_code, listing_id, sku_id, orders, orders_cancelled, lines, units_ordered, units_cancelled, items_amount_jpy, cancelled_items_amount_jpy,
    shipping_alloc_jpy, shop_coupon_alloc_jpy, mall_coupon_alloc_jpy, points_alloc_jpy, sales_jpy, customer_paid_jpy, lines_amount_unknown, lines_unresolved from mart.v_sales_daily where mall = $1 and ${where} order by date_jst, grain_key`, [mall]))
  .map((r) => Object.fromEntries(Object.entries(r).map(([k, v]) => [k, typeof v === 'bigint' || (typeof v === 'string' && /^-?\d+$/.test(v) && k !== 'shop_code') ? Number(v) : v])));
const checkRows = (mall) => all(`select * from mart.v_sales_daily_check where mall = $1`, [mall]);
/** 試験は一瞬で進む = 直前に入れた注文は「15 分のさかのぼり」の中にいて次の回でも作り直される (設計どおり・無害)。「変わっていない」を試すときは注文の更新時刻だけ過去へずらす */
// 🚨 core.orders の updated_at は trigger (touch_updated_at_unless_seq_only) が守っている = 素の UPDATE では動かない → 試験の間だけ trigger を外して書く
const setUpdatedAt = async (setExpr, where, params = []) => { await pg.exec('alter table core.orders disable trigger trg_orders_touch'); try { await pg.query(`update core.orders set updated_at = ${setExpr} where ${where}`, params); } finally { await pg.exec('alter table core.orders enable trigger trg_orders_touch'); } };
const ageOrders = (mall) => setUpdatedAt(`updated_at - interval '1 hour'`, 'mall = $1', [mall]);

console.log('D7a: 按分と粒度');
await t('🚨 最大剰余法: 送料 1,000 円を 商品代 1:1:1 の 3 明細に配ると 334 / 333 / 333 (合計がヘッダと 1 円も違わない)。値引・ポイントも同じ。売上 = 商品代 + 送料 − 店負担 / 払った額 = 売上 − モール負担 − ポイント', async () => {
  await apply('rakuten', 'main', 'R-1', H({ shipping_fee_jpy: 1000, shop_coupon_jpy: 100, mall_coupon_jpy: 50, points_used_jpy: 200 }), [L('1', { sku_code: 'sku-a' }), L('2', { sku_code: 'sku-b' }), L('3', { sku_code: 'sku-c' })]);
  const r = await refreshAll('rakuten', 'main');
  assert.deepEqual([Number(r.dates_built), Number(r.remaining)], [1, 0]);
  const rows = await daily('rakuten');
  assert.deepEqual(rows.map((x) => [x.sku_id, x.shipping_alloc_jpy, x.shop_coupon_alloc_jpy, x.mall_coupon_alloc_jpy, x.points_alloc_jpy, x.sales_jpy, x.customer_paid_jpy]),
    [[skuA, 334, 34, 17, 67, 1300, 1216], [skuB, 333, 33, 17, 67, 1300, 1216], [skuC, 333, 33, 16, 66, 1300, 1218]]);
  assert.deepEqual([rows.reduce((a, x) => a + x.shipping_alloc_jpy, 0), rows.reduce((a, x) => a + x.shop_coupon_alloc_jpy, 0), rows.reduce((a, x) => a + x.mall_coupon_alloc_jpy, 0), rows.reduce((a, x) => a + x.points_alloc_jpy, 0)], [1000, 100, 50, 200]);
  assert.deepEqual(await checkRows('rakuten'), []);
});
await t('重みの 3 段: 商品代の比 (7:3) → 商品代が全部 0 / null なら数量の比 (2:1) → 数量も 0 なら等分。金額が null の明細は 0 として足して数える', async () => {
  await apply('aupay', 'main', 'A-1', H({ shop_code: '5', ordered_at: '2026-03-02T10:00:00+09:00', shipping_fee_jpy: 500 }), [L('1', { sku_code: 'sku-a', line_amount_jpy: 700 }), L('2', { sku_code: 'sku-b', line_amount_jpy: 300 })]);
  await apply('aupay', 'main', 'A-2', H({ shop_code: '5', ordered_at: '2026-03-03T10:00:00+09:00', shipping_fee_jpy: 100 }), [L('1', { sku_code: 'sku-a', qty: 2, line_amount_jpy: null }), L('2', { sku_code: 'sku-b', qty: 1, line_amount_jpy: null })]);
  await apply('aupay', 'main', 'A-3', H({ shop_code: '5', ordered_at: '2026-03-04T10:00:00+09:00', shipping_fee_jpy: 101 }), [L('1', { sku_code: 'sku-a', qty: 0, line_amount_jpy: null }), L('2', { sku_code: 'sku-b', qty: 0, line_amount_jpy: null })]);
  await refreshAll('aupay', 'main');
  const rows = await daily('aupay');
  assert.deepEqual(rows.map((x) => [x.d, x.sku_id, x.items_amount_jpy, x.shipping_alloc_jpy, x.lines_amount_unknown]), [
    ['2026-03-02', skuA, 700, 350, 0], ['2026-03-02', skuB, 300, 150, 0],
    ['2026-03-03', skuA, 0, 67, 1], ['2026-03-03', skuB, 0, 33, 1],
    ['2026-03-04', skuA, 0, 51, 1], ['2026-03-04', skuB, 0, 50, 1]]);
  assert.deepEqual(await checkRows('aupay'), []);
});
await t('取消: 取り消された注文は商品代と取消だけ数え、送料・値引は配らない (売上 0)。明細の一部取消は数量の比で取消額を出し、按分の重みは取消を引いた商品代', async () => {
  await apply('linegift', 'main', 'L-1', H({ shop_code: '14', ordered_at: '2026-03-05T10:00:00+09:00', status: undefined, is_cancelled: true, shipping_fee_jpy: 300 }), [L('1', { sku_code: 'sku-a', qty: 2, cancelled_qty: 2, line_amount_jpy: 3000 })]);
  await apply('linegift', 'main', 'L-2', H({ shop_code: '14', ordered_at: '2026-03-05T11:00:00+09:00', shipping_fee_jpy: 300 }), [L('1', { sku_code: 'sku-a', qty: 4, cancelled_qty: 1, line_amount_jpy: 4000 }), L('2', { sku_code: 'sku-b', qty: 1, line_amount_jpy: 1000 })]);
  await refreshAll('linegift', 'main');
  const rows = await daily('linegift');
  assert.deepEqual(rows.map((x) => [x.sku_id, x.orders, x.orders_cancelled, x.units_ordered, x.units_cancelled, x.items_amount_jpy, x.cancelled_items_amount_jpy, x.shipping_alloc_jpy, x.sales_jpy]),
    [[skuA, 2, 1, 6, 3, 7000, 4000, 225, 3225], [skuB, 1, 0, 1, 0, 1000, 0, 75, 1075]]);   // 送料 300 を 取消を引いた 3000 : 1000 で
  assert.deepEqual(await checkRows('linegift'), []);
});
await t('粒度: shop_code (Amazon の 自社発送 4 / FBA null) と 出品 / SKU で分かれる。どちらにも当たらない明細は lines_unresolved に数える', async () => {
  const lstAz = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main', 'pr_sku_1', 'active') returning listing_id`)).listing_id);
  const az = (no, shop, lines) => apply('amazon', 'jp', no, H({ shop_code: shop, ordered_at: '2026-03-06T10:00:00+09:00' }), lines);
  await az('250-1', null, [L('a#1', { listing_code: 'pr_sku_1', sku_code: null, line_amount_jpy: 1100 })]);
  await az('250-2', '4', [L('a#1', { listing_code: 'pr_sku_1', sku_code: null, line_amount_jpy: 1100 })]);
  await az('250-3', null, [L('a#1', { listing_code: 'no-such', sku_code: null, line_amount_jpy: 500 }), L('b#1', { listing_code: 'pr_sku_1', sku_code: null, line_amount_jpy: 1100 })]);
  await refreshAll('amazon', 'jp');
  const rows = await daily('amazon');
  assert.deepEqual(rows.map((x) => [x.shop_code, x.listing_id, x.orders, x.items_amount_jpy, x.lines_unresolved]).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))),
    [['4', lstAz, 1, 1100, 0], [null, lstAz, 2, 2200, 0], [null, null, 1, 500, 1]].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))));
  assert.ok(lstA > 0);
});

console.log('D7a: run_id publish と、作り直す日の見つけ方');
await t('🚨 注文が変わった日だけ作り直す: 何も変わっていなければ 0 日。取消に変わると、その日の指し先が新しい run に差し替わる (古い run の行は残る = 上書きしない)。ほかの日は前の run のまま', async () => {
  await ageOrders('rakuten');
  assert.equal(Number((await refreshAll('rakuten', 'main')).dates_built), 0, '変わっていないのに作り直した');
  await apply('rakuten', 'main', 'R-2', H({ ordered_at: '2026-03-10T10:00:00+09:00' }), [L('1')]);
  const r1 = await refreshAll('rakuten', 'main');
  assert.equal(Number(r1.dates_built), 1);
  const before = await all(`select date_jst::text as d, run_id from mart.sales_daily_published where mall = 'rakuten' order by 1`);
  await ageOrders('rakuten');
  assert.equal(Number((await refreshAll('rakuten', 'main')).dates_built), 0, '変わっていないのに作り直した');
  assert.equal(await apply('rakuten', 'main', 'R-2', H({ ordered_at: '2026-03-10T10:00:00+09:00', status: undefined, is_cancelled: true }), [L('1')]), 'applied');
  const r2 = await refreshAll('rakuten', 'main');
  assert.equal(Number(r2.dates_built), 1);
  const after = await all(`select date_jst::text as d, run_id from mart.sales_daily_published where mall = 'rakuten' order by 1`);
  assert.equal(after[0].run_id, before[0].run_id, '変わっていない日 (3/1) の指し先が動いた');
  assert.notEqual(after[1].run_id, before[1].run_id);
  assert.deepEqual((await daily('rakuten', `date_jst = '2026-03-10'`)).map((x) => [x.orders_cancelled, x.sales_jpy]), [[1, 0]]);
  assert.equal(await num(`select count(*) as n from mart.sales_daily where mall = 'rakuten' and date_jst = '2026-03-10'`), 2, '古い run の行が消えている (上書きした)');
  assert.deepEqual(await checkRows('rakuten'), []);
});
await t('🚨 15 分のさかのぼり: 集計を始める前に取込の取引が始まっていて、後から commit された注文 (updated_at が watermark より少し前) も次の回で拾う', async () => {
  await ageOrders('rakuten');
  await apply('rakuten', 'main', 'R-3', H({ ordered_at: '2026-03-11T10:00:00+09:00' }), [L('1')]);
  await setUpdatedAt(`(select watermark from mart.sales_daily_state where mall = 'rakuten') - interval '5 minutes'`, `mall_order_no = 'R-3'`);
  assert.ok(await num(`select count(*) as n from core.orders o, mart.sales_daily_state s where o.mall_order_no = 'R-3' and s.mall = 'rakuten' and o.updated_at < s.watermark`) === 1, '前提: R-3 の更新時刻が watermark より前になっていない');
  assert.equal(Number((await refreshAll('rakuten', 'main')).dates_built), 1);
  assert.equal((await daily('rakuten', `date_jst = '2026-03-11'`)).length, 1);
});
await t('上限つきの呼び直し: 5 日ぶんを 2 日ずつ → 3 回で終わる。同じ session で作った日は作り直さない。watermark は全部終わったときだけ進む', async () => {
  for (let d = 1; d <= 5; d++) await apply('qoo10', 'main', `Q-${d}`, H({ shop_code: '6', ordered_at: `2026-04-0${d}T10:00:00+09:00` }), [L('1')]);
  const r1 = await refresh('qoo10', 'main', { limit: 2 });
  assert.deepEqual([Number(r1.dates_built), Number(r1.remaining)], [2, 3]);
  assert.equal((await one(`select watermark from mart.sales_daily_state where mall = 'qoo10'`)).watermark, null, '途中なのに watermark が進んだ');
  const r2 = await refresh('qoo10', 'main', { limit: 2, session: r1.session_at });
  const r3 = await refresh('qoo10', 'main', { limit: 2, session: r1.session_at });
  assert.deepEqual([Number(r2.dates_built), Number(r2.remaining), Number(r3.dates_built), Number(r3.remaining)], [2, 1, 1, 0]);
  assert.notEqual((await one(`select watermark from mart.sales_daily_state where mall = 'qoo10'`)).watermark, null);
  assert.equal(await num(`select count(*) as n from mart.sales_daily_published where mall = 'qoo10'`), 5);
  // 途中で止まった回 (watermark が進んでいない) の続きは、次の回が全部拾う
  for (let d = 6; d <= 8; d++) await apply('qoo10', 'main', `Q-${d}`, H({ shop_code: '6', ordered_at: `2026-04-0${d}T10:00:00+09:00` }), [L('1')]);
  await refresh('qoo10', 'main', { limit: 1 });   // 1 日だけ作って放置 (落ちた run の見立て)
  const again = await refreshAll('qoo10', 'main', { limit: 2 });
  assert.equal(await num(`select count(*) as n from mart.sales_daily_published where mall = 'qoo10'`), 8);
  assert.equal(Number(again.remaining), 0);
  assert.deepEqual(await checkRows('qoo10'), []);
});
await t('p_reset (--all): watermark を忘れて全部の日を作り直す。purge は指されなくなった古い行だけを猶予のあとに消す (公開中の行は消さない)', async () => {
  const nPub = await num(`select count(*) as n from mart.v_sales_daily where mall = 'qoo10'`);
  const r = await refreshAll('qoo10', 'main', { reset: true, limit: 3 });
  assert.equal(r.calls, 3);   // 8 日を 3 日ずつ
  assert.equal(await num(`select count(*) as n from mart.v_sales_daily where mall = 'qoo10'`), nPub);
  assert.ok(await num(`select count(*) as n from mart.sales_daily where mall = 'qoo10'`) > nPub);
  assert.equal(Number((await one(`select mart.purge_sales_daily(1::smallint, 3) as n`)).n), 0, '猶予の中の行を消した');
  await pg.query(`update mart.sales_daily set built_at = built_at - interval '10 days'`);
  assert.ok(Number((await one(`select mart.purge_sales_daily(1::smallint, 3) as n`)).n) > 0);
  assert.equal(await num(`select count(*) as n from mart.v_sales_daily where mall = 'qoo10'`), nPub, '公開中の行を消した');
  assert.equal(await num(`select count(*) as n from mart.sales_daily where mall = 'qoo10'`), nPub);
});
await t('検算の view: 公開の後に注文が動くと食い違いとして出る (= refresh がまだ) → refresh で消える。引数の範囲は検査する', async () => {
  await ageOrders('aupay');
  await apply('aupay', 'main', 'A-1', H({ shop_code: '5', ordered_at: '2026-03-02T10:00:00+09:00', shipping_fee_jpy: 800 }), [L('1', { sku_code: 'sku-a', line_amount_jpy: 700 }), L('2', { sku_code: 'sku-b', line_amount_jpy: 300 })]);
  const c = await checkRows('aupay');
  assert.deepEqual(c.map((x) => [x.date_jst instanceof Date ? x.date_jst.toISOString().slice(0, 10) : String(x.date_jst).slice(0, 10), Number(x.src_shipping_jpy), Number(x.pub_shipping_jpy)]).map((x) => x.slice(1)), [[800, 500]]);
  await refreshAll('aupay', 'main');
  assert.deepEqual(await checkRows('aupay'), []);
  let e = null; try { await refresh('aupay', 'main', { limit: 0 }); } catch (x) { e = x; }
  assert.match(String(e && e.message), /p_limit must be 1\.\.400/);
});

console.log('D7a: Render の受け口と送り手 (本物の router を HTTP で)');
process.env.MIRROR_SYNC_KEY = 'k';
process.env.COMPANY_DB_URL = 'pglite://test';
__setPgClientFactory(async () => ({
  query: async (text, params) => {
    if (params && params.length) return pg.query(text, params);
    if (text.includes(';')) { await pg.exec(text); return { rows: [] }; }
    return pg.query(text);
  },
  end: async () => {},
}));
const app = express();
app.use('/apps/company-db/sync', requireSyncKey);
app.use('/apps/company-db/sync', companyDbRouter);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const BASE_URL = `http://127.0.0.1:${server.address().port}/apps/company-db/sync`;
const http = async (method, p, { body, key = 'k' } = {}) => {
  const res = await fetch(`${BASE_URL}${p}`, { method, headers: { ...(key == null ? {} : { 'x-sync-key': key }), ...(body !== undefined ? { 'content-type': 'application/json' } : {}) }, body: body !== undefined ? JSON.stringify(body) : undefined });
  const text = await res.text(); let json = null; try { json = JSON.parse(text); } catch { /* JSON でない */ }
  return { status: res.status, json };
};
await t('受け口: 鍵が無ければ 401・引数の検査 (mall / scope / limit / session / reset / 期間)。refresh は DB の関数の戻り値をそのまま数で返す', async () => {
  assert.equal((await http('POST', '/orders/sales-daily/refresh', { body: { mall: 'mercari', scope: 'main' }, key: null })).status, 401);
  for (const body of [{}, { mall: 'nowhere', scope: 'main' }, { mall: 'mercari' }, { mall: 'mercari', scope: 'main', limit: 0 }, { mall: 'mercari', scope: 'main', limit: 401 }, { mall: 'mercari', scope: 'main', session: 'きのう' }, { mall: 'mercari', scope: 'main', reset: 'yes' }])
    assert.equal((await http('POST', '/orders/sales-daily/refresh', { body })).status, 400, JSON.stringify(body));
  for (const q of ['?mall=mercari', '?mall=mercari&scope=main', '?mall=mercari&scope=main&from=2026-03-31&to=2026-03-01', '?mall=mercari&scope=main&from=2024-01-01&to=2026-03-01'])
    assert.equal((await http('GET', `/orders/sales-daily/check${q}`)).status, 400, q);
  await apply('mercari', 'main', 'M-1', H({ shop_code: '8', ordered_at: '2026-05-01T10:00:00+09:00', shipping_fee_jpy: 10 }), [L('1', { sku_code: 'sku-a' }), L('2', { sku_code: 'sku-b' }), L('3', { sku_code: 'sku-c' })]);
  const r = await http('POST', '/orders/sales-daily/refresh', { body: { mall: 'mercari', scope: 'main' } });
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.deepEqual([r.json.dates_built, r.json.remaining, r.json.n_rows, r.json.n_orders, typeof r.json.session_at, typeof r.json.run_id, typeof r.json.purged], [1, 0, 3, 1, 'string', 'string', 'number']);   // purged = 全部終わった回のついでの整理 (件数は前の試験の残り次第)
  const c = await http('GET', '/orders/sales-daily/check?mall=mercari&scope=main&from=2026-05-01&to=2026-05-31');
  assert.deepEqual([c.status, c.json.diffs, c.json.published.dates, c.json.published.lines, c.json.published.items_amount_jpy, c.json.published.sales_jpy], [200, [], 1, 3, 3000, 3010]);
});
await t('送り手 refreshSalesDaily: 残りがある間は同じ session で呼び直して全部作る。検算は「公開の後に動いた注文」を食い違いとして返し、作り直すと消える', async () => {
  for (let d = 2; d <= 6; d++) await apply('qoo10', 'main', `QM-${d}`, H({ shop_code: '6', ordered_at: `2026-05-0${d}T10:00:00+09:00` }), [L('1')]);
  const calls = []; const f = async (url, init) => { calls.push(JSON.parse(init.body)); return fetch(url, init); };
  const s = await refreshSalesDaily({ mall: 'qoo10', fetchImpl: f, base: BASE_URL, syncKey: 'k', limit: 2, log: quiet });
  assert.deepEqual([s.ok, s.complete, s.remaining, s.calls >= 3, s.dates >= 5], [true, true, 0, true, true]);
  assert.equal(calls[0].session, null);
  assert.ok(calls.slice(1).every((c) => c.session === calls[1].session && typeof c.session === 'string'), '同じ session を渡していない');
  assert.equal(salesNote(s), ` / 売上日次 ${s.dates} 日`);
  await apply('qoo10', 'main', 'QM-2', H({ shop_code: '6', ordered_at: '2026-05-02T10:00:00+09:00', status: undefined, is_cancelled: true }), [L('1')]);
  await apply('qoo10', 'main', 'QM-7', H({ shop_code: '6', ordered_at: '2026-05-07T10:00:00+09:00' }), [L('1')]);
  const c = await http('GET', '/orders/sales-daily/check?mall=qoo10&scope=main&from=2026-05-01&to=2026-05-31');
  assert.deepEqual(c.json.diffs.map((x) => [x.date_jst, x.is_published]), [['2026-05-07', false]], '取消は明細数・商品代を変えない = 5/2 は食い違いに出ない。まだ公開の無い 5/7 だけ出る');
  await refreshSalesDaily({ mall: 'qoo10', fetchImpl: fetch, base: BASE_URL, syncKey: 'k', log: quiet });
  assert.deepEqual((await http('GET', '/orders/sales-daily/check?mall=qoo10&scope=main&from=2026-05-01&to=2026-05-31')).json.diffs, []);
  assert.deepEqual((await daily('qoo10', `date_jst = '2026-05-02'`)).map((x) => [x.orders_cancelled, x.sales_jpy]), [[1, 0]]);
});
await t('🚨 送り手は黙って緑にしない: 0021 が未適用 (409 not_migrated) は「未適用」と最後の行に出す (push は失敗にしない) / 時間切れ・回数の上限は complete = false / 進まない応答・HTTP エラーは例外', async () => {
  const stub = (seq) => { let i = 0; return async () => { const x = seq[Math.min(i++, seq.length - 1)]; return { ok: x.status === 200, status: x.status, json: async () => x.body, text: async () => JSON.stringify(x.body) }; }; };
  const nm = await refreshSalesDaily({ mall: 'rakuten', fetchImpl: stub([{ status: 409, body: { error: 'not_migrated' } }]), base: BASE_URL, syncKey: 'k', log: quiet });
  assert.deepEqual([nm.ok, nm.skipped, salesNote(nm)], [true, 'not_migrated', ' / ⏭️ 売上日次は未適用 (migration 0021 を当てる)']);
  const more = { status: 200, body: { session_at: '2026-09-19T00:00:00.000Z', run_id: 'sd_x', dates_built: 1, remaining: 5, n_rows: 1, n_orders: 1, purged: null } };
  const capped = await refreshSalesDaily({ mall: 'rakuten', fetchImpl: stub([more]), base: BASE_URL, syncKey: 'k', maxCalls: 3, log: quiet });
  assert.deepEqual([capped.ok, capped.complete, capped.calls, capped.reason, salesNote(capped)], [false, false, 3, 'calls', ' / ⚠️ 売上日次 3 日 (打ち切り・残り 5 日は次の run)']);
  let clock = 0;
  const timed = await refreshSalesDaily({ mall: 'rakuten', fetchImpl: stub([more]), base: BASE_URL, syncKey: 'k', budgetMs: 1000, now: () => (clock += 600), log: quiet });
  assert.deepEqual([timed.ok, timed.reason], [false, 'budget']);
  const rejects = async (fn, re) => { let e = null; try { await fn(); } catch (x) { e = x; } assert.ok(e, 'did not throw'); assert.match(e.message, re); };
  await rejects(() => refreshSalesDaily({ mall: 'rakuten', fetchImpl: stub([{ status: 200, body: { ...more.body, dates_built: 0 } }]), base: BASE_URL, syncKey: 'k', log: quiet }), /進まない/);
  await rejects(() => refreshSalesDaily({ mall: 'rakuten', fetchImpl: stub([{ status: 500, body: { error: 'x' } }]), base: BASE_URL, syncKey: 'k', log: quiet }), /HTTP 500/);
  await rejects(() => refreshSalesDaily({ mall: 'rakuten', fetchImpl: stub([{ status: 409, body: { error: 'other' } }]), base: BASE_URL, syncKey: 'k', log: quiet }), /HTTP 409/);
  await rejects(() => refreshSalesDaily({ mall: 'toString', fetchImpl: stub([more]), base: BASE_URL, syncKey: 'k', log: quiet }), /知らないモール/);
  assert.match(salesNote({ error: 'boom' }), /❌ 売上日次の作り直しに失敗/);
});
server.close();

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
