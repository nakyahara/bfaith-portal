#!/usr/bin/env node
/**
 * test-company-db-orders-push-amazon.mjs — D5b-2 = Amazon の注文の push (raw_sp_orders → core.orders) の試験。
 *   整形 (純粋関数) / 0018 の状態対応表と受け皿 (PGlite) / 通し (送り手 ⇄ 本物の router を HTTP で): 範囲・マルチチャネル発送を送らない・差分・突合・伝票 (NE 店舗 4) との結び直し
 * 楽天と共通の部分 (台帳・chunk・再送・lock・relink の持ち越し) は test-company-db-orders-push.mjs が見ている。ここは Amazon に固有の所だけ。
 * 🚨 HTTP の先 (Render) と本番の raw は試験に無い → 初回は --dry-run → 1 か月だけ送る → --reconcile
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildAmazonOrder, amazonTaxRateOf, AMAZON_TRANSFORM_VERSION } from '../apps/company-db/push/mall-orders-transform.mjs';
import { pushOrders, reconcileOrdersDaily, MALL_SPECS, BACKFILL_DONE_KEY, isBackfillDone } from '../apps/company-db/push/mall-orders.mjs';
import { openLedger } from '../apps/company-db/push/ledger.mjs';
import { fingerprintOf } from '../apps/company-db/push/pipeline.mjs';
import { buildShipment } from '../apps/company-db/push/ne-shipments-transform.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };
const quiet = () => {};

// ─── SQLite の見本 (warehouse.db の raw_sp_orders と同じ列 = apps/warehouse/db.js / sp-api-orders.js) ───
function openWarehouse() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_sp_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, amazon_order_id TEXT, merchant_order_id TEXT, purchase_date TEXT, last_updated_date TEXT, order_status TEXT, fulfillment_channel TEXT,
    sales_channel TEXT, asin TEXT, seller_sku TEXT, title TEXT, quantity INTEGER, item_price REAL, item_tax REAL, shipping_price REAL, shipping_tax REAL, promotion_discount REAL, currency TEXT, item_status TEXT, synced_at TEXT)`);
  db.exec(`CREATE TABLE raw_ne_order_base (伝票番号 TEXT PRIMARY KEY, 受注番号 TEXT, 店舗コード TEXT, 受注日 TEXT, 出荷確定日 TEXT)`);
  return db;
}
const pick = (v, d) => (v === undefined ? d : v);
const az = (x) => ({ amazon_order_id: x.no, merchant_order_id: '', purchase_date: pick(x.date, '2025-03-01T10:00:00+09:00'), last_updated_date: pick(x.updated, '2025-03-02T08:00:00+09:00'), order_status: pick(x.status, 'Shipped'),
  fulfillment_channel: pick(x.channel, 'Amazon'), sales_channel: pick(x.sales, 'Amazon.co.jp'), asin: pick(x.asin, 'B000000001'), seller_sku: pick(x.sku, 'pr_sku_1'), title: 'お客様には関係のない商品名', quantity: pick(x.qty, 1),
  item_price: pick(x.price, 1100), item_tax: pick(x.tax, 100), shipping_price: pick(x.ship, 0), shipping_tax: 0, promotion_discount: pick(x.promo, 0), currency: pick(x.currency, 'JPY'), item_status: pick(x.itemStatus, 'Shipped'), synced_at: pick(x.synced, '2026-09-17 22:04:48') });
const insertAz = (db, r) => db.prepare(`insert into raw_sp_orders (amazon_order_id, merchant_order_id, purchase_date, last_updated_date, order_status, fulfillment_channel, sales_channel, asin, seller_sku, title, quantity,
  item_price, item_tax, shipping_price, shipping_tax, promotion_discount, currency, item_status, synced_at) values (@amazon_order_id, @merchant_order_id, @purchase_date, @last_updated_date, @order_status, @fulfillment_channel, @sales_channel,
  @asin, @seller_sku, @title, @quantity, @item_price, @item_tax, @shipping_price, @shipping_tax, @promotion_discount, @currency, @item_status, @synced_at)`).run(r);

console.log('D5b-2: 整形 (buildAmazonOrder)');
await t('FBA の 1 明細: 鍵・scope jp・shop_code null・金額 (item_price は行の合計で税込)・税率の逆算・更新時刻はモール側の last_updated_date', async () => {
  const b = buildAmazonOrder([az({ no: '250-1111111-1111111', qty: 2, price: 2200, tax: 200, ship: 0, promo: 100 })]);
  assert.equal(b.key, 'amazon|jp|250-1111111-1111111');
  const { header: h, lines: [l] } = b.payload;
  assert.deepEqual([b.payload.mall, b.payload.scope_key, h.shop_code, h.status_source, h.is_cancelled, h.ordered_at, h.source_updated_at, h.transform_version],
    ['amazon', 'jp', null, 'Shipped', false, '2025-03-01T10:00:00+09:00', '2025-03-02T08:00:00+09:00', AMAZON_TRANSFORM_VERSION]);
  assert.deepEqual([h.total_amount_jpy, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, h.mall_coupon_jpy, h.points_used_jpy], [null, 2200, 0, 100, null, null]);
  assert.deepEqual(l, { line_key: 'pr_sku_1|B000000001#1', listing_code: 'pr_sku_1', sku_code: null, qty: 2, cancelled_qty: 0, unit_price_jpy: 1100, line_amount_jpy: 2200, tax_rate: 0.1, amount_source: 'mall_api', source_line_ref: 'asin:B000000001|item_status:Shipped' });
  assert.equal(JSON.stringify(b.payload).includes('商品名'), false, 'title を運んでいる');
});
await t('自社発送 (Merchant) は shop_code 4 (NE の店舗 4)。送料は行の合計', async () => {
  const b = buildAmazonOrder([az({ no: 'M-1', channel: 'Merchant', sku: 'a', ship: 300 }), az({ no: 'M-1', channel: 'Merchant', sku: 'b', price: 540, tax: 40, ship: 200 })]);
  assert.deepEqual([b.payload.header.shop_code, b.payload.header.items_amount_jpy, b.payload.header.shipping_fee_jpy, b.payload.lines.map((l) => l.tax_rate)], ['4', 1640, 500, [0.1, 0.08]]);
});
await t('取消: 数量 0・金額 0 (Amazon が空にする) → qty 0・金額は null (0 円の売上にしない) と数える。ヘッダの金額も null', async () => {
  const stats = {};
  const b = buildAmazonOrder([az({ no: 'C-1', status: 'Cancelled', itemStatus: 'Cancelled', qty: 0, price: 0, tax: 0 })], { stats });
  const h = b.payload.header, l = b.payload.lines[0];
  assert.deepEqual([h.is_cancelled, h.status_source, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, l.qty, l.cancelled_qty, l.unit_price_jpy, l.line_amount_jpy, l.tax_rate], [true, 'Cancelled', null, null, null, 0, 0, null, null, null]);
  assert.deepEqual([stats.zeroPrice, stats.zeroQtyLive || 0], [1, 0]);
});
await t('🚨 取消でない明細の金額が分からない注文は、分かる行だけの部分和を合計にしない (商品代・送料・値引とも null) と数える。取消でないのに数量 0 の明細も数える (Codex R1 #3)', async () => {
  const stats = {};
  const b = buildAmazonOrder([az({ no: 'Z-1', sku: 'a', qty: 0, price: 0, tax: 0, ship: 300 }), az({ no: 'Z-1', sku: 'b', qty: 3, price: 1000, tax: 91, ship: 200, promo: 50 })], { stats });
  const h = b.payload.header;
  assert.deepEqual([stats.zeroPrice, stats.zeroQtyLive, stats.partialAmountOrders, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy], [1, 1, 1, null, null, null]);
  assert.deepEqual(b.payload.lines.map((l) => [l.line_amount_jpy, l.unit_price_jpy]), [[null, null], [1000, null]]);   // 明細は分かる行だけ金額を持つ。1000 / 3 は割り切れない → 単価は null
});
await t('一部の明細だけ取消 (行の状態が Cancelled で金額が空) の注文は、残りの行の合計が注文の合計。取消の行の送料・値引は足さない', async () => {
  const stats = {};
  const b = buildAmazonOrder([az({ no: 'Z-2', sku: 'a', itemStatus: 'Cancelled', qty: 0, price: 0, tax: 0, ship: 999 }), az({ no: 'Z-2', sku: 'b', qty: 1, price: 1100, ship: 200, promo: 50 })], { stats });
  const h = b.payload.header;
  assert.deepEqual([stats.partialAmountOrders || 0, stats.zeroQtyLive || 0, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy], [0, 0, 1100, 200, 50]);
});
await t('明細 ID が無い: 同じ SKU・ASIN が 2 行 (全列が同じでも) → #1 / #2。行の順が変わっても同じ payload (= 同じ指紋)', async () => {
  const r1 = az({ no: 'D-1', qty: 1, price: 1100 }), r2 = az({ no: 'D-1', qty: 2, price: 2200, tax: 200 }), r3 = az({ no: 'D-1', qty: 1, price: 1100 });
  const a = buildAmazonOrder([r1, r2, r3]), b = buildAmazonOrder([r3, r2, r1]);
  assert.deepEqual(a.payload.lines.map((l) => [l.line_key, l.qty]), [['pr_sku_1|B000000001#1', 1], ['pr_sku_1|B000000001#2', 1], ['pr_sku_1|B000000001#3', 2]]);
  assert.equal(fingerprintOf(AMAZON_TRANSFORM_VERSION, a.payload), fingerprintOf(AMAZON_TRANSFORM_VERSION, b.payload));
});
await t('last_updated_date だけ変わっても指紋は同じ (内容で判定)。状態が変われば変わる', async () => {
  const a = buildAmazonOrder([az({ no: 'U-1' })]), b = buildAmazonOrder([az({ no: 'U-1', updated: '2025-03-09T00:00:00+09:00', synced: '2026-09-18 22:00:00' })]), c = buildAmazonOrder([az({ no: 'U-1', status: 'Shipped - Delivered to Buyer' })]);
  assert.equal(fingerprintOf(AMAZON_TRANSFORM_VERSION, a.payload), fingerprintOf(AMAZON_TRANSFORM_VERSION, b.payload));
  assert.notEqual(fingerprintOf(AMAZON_TRANSFORM_VERSION, a.payload), fingerprintOf(AMAZON_TRANSFORM_VERSION, c.payload));
});
await t('更新時刻: last_updated_date が無ければ synced_at (UTC)、それも無ければ fallback (数える)', async () => {
  assert.equal(buildAmazonOrder([az({ no: 'T-1', updated: '' })]).payload.header.source_updated_at, '2026-09-17T22:04:48Z');
  const b = buildAmazonOrder([az({ no: 'T-2', updated: '', synced: '' })], { fallbackSourceUpdatedAt: '2026-09-18T00:00:00.000Z' });
  assert.deepEqual([b.payload.header.source_updated_at, b.no_synced_at], ['2026-09-18T00:00:00.000Z', true]);
  throws(() => buildAmazonOrder([az({ no: 'T-3', updated: '', synced: '' })]), /last_updated_date も synced_at も無い/);
});
await t('整形できないものは例外: マルチチャネル発送・知らない fulfillment_channel・JPY 以外・負の金額・小数の数量・SKU 無し・注文の列の食い違い・注文日なし', async () => {
  throws(() => buildAmazonOrder([az({ no: 'X-1', sales: 'Non-Amazon JP' })]), /Amazon\.co\.jp の注文でない/);
  throws(() => buildAmazonOrder([az({ no: 'X-2', channel: 'Other' })]), /fulfillment_channel/);
  throws(() => buildAmazonOrder([az({ no: 'X-3', currency: 'USD' })]), /JPY でない/);
  throws(() => buildAmazonOrder([az({ no: 'X-4', price: -1 })]), /負の金額/);
  throws(() => buildAmazonOrder([az({ no: 'X-5', qty: 1.5 })]), /quantity/);
  throws(() => buildAmazonOrder([az({ no: 'X-6', sku: '' })]), /seller_sku/);
  throws(() => buildAmazonOrder([az({ no: 'X-7' }), az({ no: 'X-7', status: 'Cancelled' })]), /order_status が行によって違う/);
  throws(() => buildAmazonOrder([az({ no: 'X-8', date: '' })]), /purchase_date/);
  throws(() => buildAmazonOrder([az({ no: 'X-9' }), az({ no: 'X-0' })]), /別の注文/);
});
await t('税率の逆算: 10% / 8% のどちらか一方にだけ合うとき。両方に合う小さな金額・合わない・税 0 は null', async () => {
  assert.deepEqual([amazonTaxRateOf(1100, 100), amazonTaxRateOf(1080, 80), amazonTaxRateOf(1980, 180), amazonTaxRateOf(1100, 50), amazonTaxRateOf(1100, 0), amazonTaxRateOf(0, 0)], [0.1, 0.08, 0.1, null, null, null]);
  assert.equal(amazonTaxRateOf(10, 1), null);   // 10 円: 10/110 = 0.9 → 1、8/108 = 0.7 → 1 = どちらにも合う
});

console.log('D5b-2: 受け皿 (0018 の状態対応表・出品の解決・結び)');
const pg = new PGlite();
const pdb = pgliteAdapter(pg);
const applied0 = await applyMigrations(pdb, { log: quiet });
assert.ok(applied0.applied.includes('0018'), '0018 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
await pg.query(`insert into core.products (company_id, name) values (1, '見本')`);
const lstFba = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', '', 'pr_sku_1', 'active') returning listing_id`)).listing_id;
await t('0018: 実測で出ていた 13 の状態がどれも unknown にならない。delivered は根拠のある 2 つだけ', async () => {
  const seen = ['Shipped', 'Shipped - Delivered to Buyer', 'Cancelled', 'Pending', 'Shipped - Picked Up', 'Shipped - Lost in Transit', 'Shipped - Returned to Seller', 'Shipped - Returning to Seller', 'Shipped - Undeliverable',
    'Shipped - Rejected by Buyer', 'Shipped - Out for Delivery', 'Unfulfillable', 'Pending - Waiting for Pick Up'];
  const got = {};
  for (const s of seen) got[s] = (await one(`select core.map_order_status('amazon', $1) as s`, [s])).s;
  assert.equal(Object.values(got).includes('unknown'), false, JSON.stringify(got));
  assert.deepEqual(Object.entries(got).filter(([, v]) => v === 'delivered').map(([k]) => k).sort(), ['Shipped - Delivered to Buyer', 'Shipped - Picked Up']);
  assert.deepEqual([got.Pending, got.Shipped, got.Cancelled, got['Shipped - Returned to Seller'], got.Unfulfillable], ['new', 'shipped', 'cancelled', 'returned', 'on_hold']);
  assert.equal((await one(`select core.map_order_status('amazon', 'まだ知らない状態') as s`)).s, 'unknown');
});

// ─── 本物の router を HTTP で (Postgres の接続だけ PGlite に差し替える) ───
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
const calls = [];
const f = async (url, init = {}) => { calls.push({ url, init }); return fetch(url, init); };
const posts = () => calls.filter((c) => c.init.method === 'POST' && c.url.endsWith('/orders')).map((c) => JSON.parse(c.init.body));
const push = (w, l, x = {}) => pushOrders({ mall: 'amazon', warehouse: w, ledger: l, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });
const shipBase = (x) => ({ 伝票番号: x.slip, 受注番号: x.orderNo, 店舗コード: x.shop ?? '4', 受注日: '2025-03-01 10:00:00', 出荷確定日: '2025-03-01 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済', キャンセル区分: '有効', 受注キャンセル日: '', 配送方法ID: '28', 配送方法名: 'ネコポス', 送り状番号: '', synced_at: '2026-09-10 00:00:00' });
const applyShipment = async (x, seq = 1) => { const s = buildShipment(shipBase(x), []); return (await one(`select core.apply_shipment_batch(1::smallint, $1, $2::bigint, $3::jsonb, $4::jsonb) as r`, [s.ne_slip_no, seq, JSON.stringify(s.header), JSON.stringify(s.lines)])).r; };

console.log('D5b-2: 通し (送り手 ⇄ 本物の受け口を HTTP で)');
const W = openWarehouse();
const L = openLedger(null, { memory: true, kind: 'order:amazon' }); L.markInitialized();
await t('初回: 範囲 (注文日 2025-01-01 以降 + 範囲内の出荷が参照する古い注文) だけ送る。マルチチャネル発送は送らずに数える。出品は seller SKU で解決、無ければ原文を残す', async () => {
  insertAz(W, az({ no: '250-0000001-0000001', sku: 'pr_sku_1', qty: 2, price: 2200, tax: 200 }));                                  // FBA
  insertAz(W, az({ no: '250-0000002-0000002', channel: 'Merchant', sku: 'unknown_sku', ship: 500 }));                              // 自社発送・出品が無い SKU
  insertAz(W, az({ no: '250-0000003-0000003', status: 'Cancelled', itemStatus: 'Cancelled', qty: 0, price: 0, tax: 0 }));          // 取消
  insertAz(W, az({ no: '250-0000004-0000004', sales: 'Non-Amazon JP', price: 0, tax: 0 }));                                        // マルチチャネル発送
  insertAz(W, az({ no: '249-0000005-0000005', date: '2024-12-30T10:00:00+09:00', channel: 'Merchant' }));                          // 古い。範囲内の出荷が参照する
  insertAz(W, az({ no: '249-0000006-0000006', date: '2024-12-30T10:00:00+09:00' }));                                                // 古い。誰も参照しない
  W.prepare(`insert into raw_ne_order_base values ('S-OLD', '249-0000005-0000005', '4', '2024-12-30 10:00:00', '2025-01-05 15:00:00')`).run();
  const r = await push(W, L);
  assert.deepEqual([r.ok, r.scanned, r.inScope, r.changed, r.applied, r.failed.length, r.transformErrors.length, r.stats.skippedNonAmazon, r.stats.zeroPrice, r.stats.referencedCount], [true, 5, 4, 4, 4, 0, 0, 1, 1, 1]);
  const sent = posts().flatMap((b) => b.rows.map((x) => x.mall_order_no)).sort();
  assert.deepEqual(sent, ['249-0000005-0000005', '250-0000001-0000001', '250-0000002-0000002', '250-0000003-0000003']);
  const rows = (await pg.query(`select o.mall_order_no, o.scope_key, o.shop_code, o.status, o.is_cancelled, o.items_amount_jpy, o.shipping_fee_jpy, o.order_date_jst::text as d, l.listing_id, l.unresolved_code, l.qty, l.line_amount_jpy
    from core.orders o join core.order_lines l using (order_id) where o.mall = 'amazon' order by o.mall_order_no`)).rows;
  assert.deepEqual(rows.map((x) => [x.mall_order_no, x.scope_key, x.shop_code, x.status, x.is_cancelled, x.items_amount_jpy == null ? null : Number(x.items_amount_jpy), x.d, x.listing_id == null ? null : Number(x.listing_id), x.unresolved_code, x.qty]), [
    ['249-0000005-0000005', 'jp', '4', 'shipped', false, 1100, '2024-12-30', Number(lstFba), null, 1],
    ['250-0000001-0000001', 'jp', null, 'shipped', false, 2200, '2025-03-01', Number(lstFba), null, 2],
    ['250-0000002-0000002', 'jp', '4', 'shipped', false, 1100, '2025-03-01', null, 'unknown_sku', 1],
    ['250-0000003-0000003', 'jp', null, 'cancelled', true, null, '2025-03-01', Number(lstFba), null, 0],
  ]);
});
await t('2 回目は変化なし。状態と明細が変わった注文だけ次の世代で送る (取込が注文を DELETE → INSERT して id が変わっても内容が同じなら送らない)', async () => {
  const before = posts().length;
  W.prepare(`delete from raw_sp_orders where amazon_order_id = '250-0000002-0000002'`).run();
  insertAz(W, az({ no: '250-0000002-0000002', channel: 'Merchant', sku: 'unknown_sku', ship: 500, synced: '2026-09-18 22:00:00' }));   // 同じ内容で入れ直し
  const r1 = await push(W, L);
  assert.deepEqual([r1.ok, r1.changed, posts().length - before], [true, 0, 0]);
  W.prepare(`update raw_sp_orders set order_status = 'Shipped - Delivered to Buyer' where amazon_order_id = '250-0000001-0000001'`).run();
  const r2 = await push(W, L);
  assert.deepEqual([r2.ok, r2.changed, r2.applied], [true, 1, 1]);
  assert.equal((await one(`select status from core.orders where mall = 'amazon' and mall_order_no = '250-0000001-0000001'`)).status, 'delivered');
});
await t('突合: 注文日ごとの 注文数 / 明細数 / 商品代 / 取消 が raw と一致 (マルチチャネル発送は両側で数えない)。raw が変わって未送信なら差が出る', async () => {
  const rr = await reconcileOrdersDaily({ mall: 'amazon', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-03-01', to: '2025-03-31', log: quiet });
  assert.equal(rr.ok, true, JSON.stringify(rr));
  const local = W.prepare(MALL_SPECS.amazon.dailySql).all('2025-03-01', '2025-03-31');
  assert.deepEqual(local, [{ order_date: '2025-03-01', orders: 3, lines: 3, items_amount_jpy: 3300, cancelled: 1 }]);
  // 金額の分からない取消でない明細が残る注文は、両側とも商品代を 0 として足す (整形と同じ規則)。sales_channel が NULL の行を含む注文は両側とも数えない (Codex R1)
  insertAz(W, az({ no: '250-0000010-0000010', date: '2025-03-02T09:00:00+09:00', sku: 'a', price: 0, tax: 0 })); insertAz(W, az({ no: '250-0000010-0000010', date: '2025-03-02T09:00:00+09:00', sku: 'b', price: 700, tax: 63 }));
  insertAz(W, az({ no: '250-0000011-0000011', date: '2025-03-02T09:00:00+09:00' })); insertAz(W, az({ no: '250-0000011-0000011', date: '2025-03-02T09:00:00+09:00', sku: 'z', sales: null }));
  assert.deepEqual(W.prepare(MALL_SPECS.amazon.dailySql).all('2025-03-02', '2025-03-02'), [{ order_date: '2025-03-02', orders: 1, lines: 2, items_amount_jpy: 0, cancelled: 0 }]);
  const rp = await push(W, L);
  assert.deepEqual([rp.ok, rp.stats.partialAmountOrders, rp.stats.skippedNonAmazon], [true, 1, 2]);
  assert.equal((await one(`select items_amount_jpy from core.orders where mall = 'amazon' and mall_order_no = '250-0000010-0000010'`)).items_amount_jpy, null);
  assert.equal(await num(`select count(*) as n from core.orders where mall = 'amazon' and mall_order_no = '250-0000011-0000011'`), 0);
  assert.equal((await reconcileOrdersDaily({ mall: 'amazon', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-03-02', to: '2025-03-02', log: quiet })).ok, true);
  insertAz(W, az({ no: '250-0000007-0000007', price: 500, tax: 45 }));
  const bad = await reconcileOrdersDaily({ mall: 'amazon', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-03-01', to: '2025-03-31', log: quiet });
  assert.equal(bad.ok, false);
  await push(W, L);
  assert.equal((await reconcileOrdersDaily({ mall: 'amazon', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-03-01', to: '2025-03-31', log: quiet })).ok, true);
});
await t('伝票との結び: NE の店舗 4 (自社発送) の伝票は注文番号そのままで Amazon の注文に結ばれる (先に届いていた伝票は push の後の結び直しで)', async () => {
  await applyShipment({ slip: 'S-AZ-LATE', orderNo: '250-0000008-0000008' });   // 注文より先に伝票が届く
  assert.equal((await one(`select order_id from core.shipments where ne_slip_no = 'S-AZ-LATE'`)).order_id, null);
  insertAz(W, az({ no: '250-0000008-0000008', channel: 'Merchant' }));
  const r = await push(W, L);
  assert.equal(r.ok, true);
  assert.ok(r.afterSend && r.afterSend.ran && !r.afterSend.error, JSON.stringify(r.afterSend));
  assert.equal(await num(`select count(*) as n from core.shipments s join core.orders o using (order_id) where s.ne_slip_no = 'S-AZ-LATE' and o.mall = 'amazon' and o.mall_order_no = '250-0000008-0000008'`), 1);
  await applyShipment({ slip: 'S-AZ-NOW', orderNo: '250-0000002-0000002' });     // 注文が先にある伝票は適用の時点で結ばれる
  assert.equal(await num(`select count(*) as n from core.shipments where ne_slip_no = 'S-AZ-NOW' and order_id is not null`), 1);
});
await t('--from/--to は注文日の期間だけ (バックフィルの窓)。古い注文も期間に入れば送る', async () => {
  const before = posts().length;
  const r = await push(W, L, { from: '2024-12-01', to: '2024-12-31', relink: false });
  assert.deepEqual([r.ok, r.inScope, r.changed], [true, 2, 1]);   // 249-…05 は送付済み = 変化なし、249-…06 が初めて入る
  assert.deepEqual(posts().slice(before).flatMap((b) => b.rows.map((x) => x.mall_order_no)), ['249-0000006-0000006']);
});
await t('🚨 バックフィルの完了印は指紋の件数と別: 途中まで送っただけでは付かない (Codex R1 #1)。付けた後は指紋を空にしても残る (R1 #2)', async () => {
  assert.ok(L.countConfirmed() > 0, '前提: この台帳はもう何件か送っている');
  assert.equal(isBackfillDone(L), false);
  L.putMeta(BACKFILL_DONE_KEY, '1');
  assert.equal(isBackfillDone(L), true);
  L.resetFingerprints();
  assert.deepEqual([L.countConfirmed(), isBackfillDone(L)], [0, true]);
});
L.close(); W.close();
await t('--require-backfilled (daily-sync 用) の CLI: 完了印が無ければ (途中まで送ってあっても) 送らずに最後の行へ「バックフィル前」exit 0 → --mark-backfilled → 以後は Render へ取りに行く (--reset-ledger の後も)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-az-'));
  try {
    const w = new Database(path.join(dir, 'warehouse.db'));
    w.exec('CREATE TABLE raw_sp_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, amazon_order_id TEXT, merchant_order_id TEXT, purchase_date TEXT, last_updated_date TEXT, order_status TEXT, fulfillment_channel TEXT, '
      + 'sales_channel TEXT, asin TEXT, seller_sku TEXT, title TEXT, quantity INTEGER, item_price REAL, item_tax REAL, shipping_price REAL, shipping_tax REAL, promotion_discount REAL, currency TEXT, item_status TEXT, synced_at TEXT)');
    insertAz(w, az({ no: '250-0000009-0000009' })); w.close();
    // 途中まで流したバックフィルの跡 (初期化済みで追跡中の鍵がある台帳。完了印は無い)
    const led = openLedger(dir, { kind: 'order:amazon' }); led.markInitialized(); led.trackKeys(['amazon|jp|250-0000001-0000001'], new Date());
    led.close();
    const cli = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'apps', 'company-db', 'push', 'mall-orders.mjs');
    const env = { ...process.env, DATA_DIR: dir, RENDER_MIRROR_URL: 'https://127.0.0.1:9/apps/mirror', RENDER_PORTAL_URL: '', MIRROR_SYNC_KEY: 'k' };   // https (設定としては正しい) だが誰も聞いていない宛先 = 取りに行けば必ず通信で失敗する
    const run = (args) => spawnSync(process.execPath, [cli, ...args], { env, encoding: 'utf8', timeout: 120000 });
    const lastLine = (r) => r.stdout.trim().split(/\r?\n/).pop();
    const wentToRender = (r) => r.status === 1 && /Render の状態|fetch failed|ECONNREFUSED/.test(r.stdout + r.stderr);   // 設定の誤り (DATA_DIR・引数) ではなく通信まで進んだ
    const a = run(['--mall', 'amazon', '--incremental', '--require-backfilled']);
    assert.equal(a.status, 0, a.stdout + a.stderr);
    assert.match(lastLine(a), /バックフィル前/);   // daily-sync は最後の行を朝の通知に出す = 黙った緑にしない
    assert.equal(wentToRender(run(['--mall', 'amazon', '--incremental'])), true, '歯止め無しなら Render へ取りに行く');
    const m = run(['--mall', 'amazon', '--mark-backfilled']);
    assert.equal(m.status, 0, m.stdout + m.stderr);
    const b = run(['--mall', 'amazon', '--incremental', '--require-backfilled']);
    assert.equal(wentToRender(b), true, b.stdout + b.stderr);
    assert.equal(run(['--mall', 'amazon', '--reset-ledger']).status, 0);
    const c = run(['--mall', 'amazon', '--incremental', '--require-backfilled']);
    assert.equal(wentToRender(c), true, '指紋を空にしても完了印は残る = daily-sync が送り直しに行く: ' + c.stdout + c.stderr);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
server.close();

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
