#!/usr/bin/env node
/**
 * test-company-db-orders-push-yahoo.mjs — D5b-5 = Yahoo!ショッピングの注文の push (raw_yahoo_orders → core.orders) の試験。
 *   整形 (純粋関数) / 0031 (D-32 を「入れる」に + 状態対応表) / 通し (送り手 ⇄ 本物の router を HTTP で): 出品と SKU の解決・差分・突合・NE 店舗 2 の伝票との結び・読めない値
 * ほかのモールと共通の部分 (台帳・chunk・再送・lock・結び直しの持ち越し・完了印の CLI) は test-company-db-orders-push.mjs / -amazon.mjs が見ている。
 * 🚨 HTTP の先 (Render) と本番の raw は試験に無い → 初回は --dry-run → 送る → --reconcile
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildYahooOrder, isYahooJst, isYahooOrderNo, YAHOO_COLUMNS, YAHOO_TRANSFORM_VERSION } from '../apps/company-db/push/mall-orders-transform.mjs';
import { pushOrders, reconcileOrdersDaily, MALL_SPECS } from '../apps/company-db/push/mall-orders.mjs';
import { openLedger } from '../apps/company-db/push/ledger.mjs';
import { fingerprintOf } from '../apps/company-db/push/pipeline.mjs';
import { buildShipment } from '../apps/company-db/push/ne-shipments-transform.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';
import { insertOrders } from '../apps/warehouse/yahoo-orders.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };
const quiet = () => {};
const pick = (v, d) => (v === undefined ? d : v);

// ─── SQLite の見本 (warehouse.db の raw_yahoo_orders と同じ列 = apps/warehouse/db.js) ───
function openWarehouse() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_yahoo_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL, order_time TEXT, last_update_time TEXT, order_status TEXT, pay_status TEXT, ship_status TEXT,
    total_price REAL, pay_charge REAL, ship_charge REAL, discount REAL, use_point REAL, line_id INTEGER, item_id TEXT, title TEXT, sub_code TEXT, unit_price REAL, original_price REAL, quantity INTEGER,
    item_tax_ratio REAL, coupon_discount REAL, synced_at TEXT, ship_date TEXT, social_gift_type TEXT)`);
  db.exec(`CREATE TABLE raw_ne_order_base (伝票番号 TEXT PRIMARY KEY, 受注番号 TEXT, 店舗コード TEXT, 受注日 TEXT, 出荷確定日 TEXT)`);
  // 取込 (apps/warehouse/yahoo-orders.js insertOrders) が書く履歴の表 (apps/warehouse/db.js と同じ列)
  db.exec(`CREATE TABLE raw_yahoo_orders_log (id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL, source_window_start TEXT, source_window_end TEXT, order_id TEXT NOT NULL, order_time TEXT, last_update_time TEXT,
    order_status TEXT, pay_status TEXT, ship_status TEXT, total_price REAL, pay_charge REAL, ship_charge REAL, discount REAL, use_point REAL, line_id INTEGER, item_id TEXT, title TEXT, sub_code TEXT,
    unit_price REAL, original_price REAL, quantity INTEGER, item_tax_ratio REAL, coupon_discount REAL, ingested_at TEXT, ship_date TEXT, social_gift_type TEXT)`);
  return db;
}
const yo = (x) => ({ order_id: pick(x.no, 'b-faith01-10000001'), order_time: pick(x.time, '2026-03-01T10:00:00+09:00'), last_update_time: pick(x.upd, '2026-03-02T09:00:00+09:00'),
  order_status: pick(x.os, '5'), pay_status: pick(x.ps, '1'), ship_status: pick(x.ss, '3'), total_price: pick(x.total, 3816), pay_charge: pick(x.payCharge, 0), ship_charge: pick(x.ship, 0),
  discount: pick(x.discount, 0), use_point: pick(x.point, 294), line_id: pick(x.line, 1), item_id: pick(x.item, 'yitem-a'), title: '商品名', sub_code: pick(x.sub, null), unit_price: pick(x.price, 2055),
  original_price: 0, quantity: pick(x.qty, 2), item_tax_ratio: pick(x.tax, 10), coupon_discount: pick(x.coupon, 25), synced_at: pick(x.synced, '2026-04-12 01:30:51'), ship_date: pick(x.shipDate, null), social_gift_type: null });
const insertRow = (db, r) => { const cols = Object.keys(r); db.prepare(`insert into raw_yahoo_orders (${cols.join(', ')}) values (${cols.map((c) => '@' + c).join(', ')})`).run(r); };
const only = (r) => Object.fromEntries(YAHOO_COLUMNS.map((c) => [c, r[c]]));

console.log('D5b-5: 整形 (Yahoo)');
await t('注文 × 明細: 鍵は注文番号・shop_code 2・状態は「注文-入金-出荷」の 3 桁・商品代 = 単価 × 数量 (単価は店のクーポン値引き後 = クーポンをもう一度引かない)・払った額 = total_price・ポイント = use_point・モール負担は null', async () => {
  const b = buildYahooOrder([only(yo({}))]);
  const h = b.payload.header;
  assert.deepEqual([b.key, b.payload.mall, b.payload.scope_key, h.shop_code, h.ordered_at, h.status_source, h.is_cancelled, h.shipped_at_source, h.source_updated_at, h.transform_version],
    ['yahoo|main|b-faith01-10000001', 'yahoo', 'main', '2', '2026-03-01T10:00:00+09:00', '5-1-3', false, null, '2026-04-12T01:30:51Z', YAHOO_TRANSFORM_VERSION]);
  assert.deepEqual([h.total_amount_jpy, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, h.mall_coupon_jpy, h.points_used_jpy], [3816, 4110, 0, 0, null, 294]);
  assert.deepEqual(b.payload.lines, [{ line_key: '1', listing_code: 'yitem-a', sku_code: 'yitem-a', qty: 2, cancelled_qty: 0, unit_price_jpy: 2055, line_amount_jpy: 4110, tax_rate: 0.1, amount_source: 'mall_api', source_line_ref: 'line_id:1' }]);
});
await t('明細が複数: line_id の順に並べる・サブコードがあれば SKU はサブコード・税率 8% → 0.08・値引き (注文後の手入力) = 店負担', async () => {
  const b = buildYahooOrder([only(yo({ line: 2, item: 'yitem-b', sub: 'yitem-b-red', price: 1000, qty: 1, tax: 8, discount: 100, total: 3010 })), only(yo({ line: 1, discount: 100, total: 3010 }))]);
  assert.deepEqual(b.payload.lines.map((l) => [l.line_key, l.listing_code, l.sku_code, l.tax_rate, l.line_amount_jpy]), [['1', 'yitem-a', 'yitem-a', 0.1, 4110], ['2', 'yitem-b', 'yitem-b-red', 0.08, 1000]]);
  assert.deepEqual([b.payload.header.items_amount_jpy, b.payload.header.shop_coupon_jpy], [5110, 100]);
});
await t('取消 (注文の状態 4): is_cancelled・取消の数量 = 数量 (数量 0 で来ることが多い)', async () => {
  const b = buildYahooOrder([only(yo({ os: '4', ps: '0', ss: '0', qty: 0 }))]);
  assert.deepEqual([b.payload.header.status_source, b.payload.header.is_cancelled, b.payload.header.items_amount_jpy, b.payload.lines[0].qty, b.payload.lines[0].cancelled_qty], ['4-0-0', true, 0, 0, 0]);
  const c = buildYahooOrder([only(yo({ os: '4', ps: '1', ss: '1', qty: 3 }))]);
  assert.deepEqual([c.payload.header.is_cancelled, c.payload.lines[0].cancelled_qty], [true, 3]);
});
await t('🚨 読めない値は例外 (黙って 0 や別の値にしない): 注文の列が行によって違う・line_id の重複や欠落・数量や単価の欠落・負の金額・状態が 1 桁の数字でない・日時の形 (+09:00 無し・13 月・前後の空白・文字列でない)・注文番号の空白', async () => {
  throws(() => buildYahooOrder([only(yo({ line: 1 })), only(yo({ line: 2, total: 9999 }))]), /total_price が行によって違う/);
  throws(() => buildYahooOrder([only(yo({ line: 1 })), only(yo({ line: 1 }))]), /line_id 1 が重複/);
  throws(() => buildYahooOrder([only(yo({ line: null }))]), /line_id の無い行/);
  throws(() => buildYahooOrder([only(yo({ qty: null }))]), /quantity が無い/);
  throws(() => buildYahooOrder([only(yo({ price: null }))]), /unit_price が無い/);
  throws(() => buildYahooOrder([only(yo({ point: -1 }))]), /0 以上の数でない/);
  throws(() => buildYahooOrder([only(yo({ os: 'x' }))]), /1 桁の数字でない/);
  throws(() => buildYahooOrder([only(yo({ item: '' }))]), /item_id が無い/);
  for (const bad of ['2026-03-01T10:00:00', '2026-13-01T10:00:00+09:00', ' 2026-03-01T10:00:00+09:00', '2026-03-01T10:00:00Z']) throws(() => buildYahooOrder([only(yo({ time: bad }))]), /order_time/);
  throws(() => buildYahooOrder([only(yo({ no: ' b-faith01-10000001' }))]), /注文番号の形が違う/);
  assert.deepEqual([isYahooJst('2026-02-29T10:00:00+09:00'), isYahooJst(Buffer.from('2026-03-01T10:00:00+09:00')), isYahooOrderNo('b-faith01-1'), isYahooOrderNo('b-faith01-1 ')], [false, false, true, false]);
});
await t('取込時刻 (synced_at) だけ変わっても指紋は同じ・状態が変われば変わる', async () => {
  const a = buildYahooOrder([only(yo({}))]), b = buildYahooOrder([only(yo({ synced: '2026-09-26 01:00:00' }))]), c = buildYahooOrder([only(yo({ ss: '4' }))]);
  assert.equal(fingerprintOf(YAHOO_TRANSFORM_VERSION, a.payload), fingerprintOf(YAHOO_TRANSFORM_VERSION, b.payload));
  assert.notEqual(fingerprintOf(YAHOO_TRANSFORM_VERSION, a.payload), fingerprintOf(YAHOO_TRANSFORM_VERSION, c.payload));
});

console.log('D5b-5: 受け皿 (0031 = D-32 を「入れる」に + 状態対応表)');
const pg = new PGlite();
const pdb = pgliteAdapter(pg);
const applied0 = await applyMigrations(pdb, { log: quiet });
assert.ok(applied0.applied.includes('0031'), '0031 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
await t('0031: Yahoo の orders_enabled が true。実測で出ていた状態の組み合わせがどれも unknown にならない。意味の分からない 5-0-0 と出ていない組み合わせは unknown のまま (DQ に出る)', async () => {
  assert.equal((await one(`select orders_enabled from core.mall_order_policy where company_id = 1 and mall = 'yahoo' and scope_key = 'main'`)).orders_enabled, true);
  const m = async (v) => (await one(`select core.map_order_status('yahoo', $1) as s`, [v])).s;
  const got = [];
  for (const v of ['5-1-3', '5-1-4', '2-0-0', '2-1-1', '2-1-3', '2-0-3', '4-0-0', '4-1-1', '4-1-3', '5-0-0', '3-1-1']) got.push(await m(v));
  assert.deepEqual(got, ['shipped', 'delivered', 'new', 'confirmed', 'shipped', 'shipped', 'cancelled', 'cancelled', 'cancelled', 'unknown', 'unknown']);
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
const push = (w, l, x = {}) => pushOrders({ mall: 'yahoo', warehouse: w, ledger: l, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });
const shipBase = (x) => ({ 伝票番号: x.slip, 受注番号: x.orderNo, 店舗コード: '2', 受注日: '2026-03-01 10:00:00', 出荷確定日: '2026-03-01 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済', キャンセル区分: '有効', 受注キャンセル日: '', 配送方法ID: '28', 配送方法名: 'ネコポス', 送り状番号: '', synced_at: '2026-09-10 00:00:00' });
const applyShipment = async (x, seq = 1) => { const s = buildShipment(shipBase(x), []); return (await one(`select core.apply_shipment_batch(1::smallint, $1, $2::bigint, $3::jsonb, $4::jsonb) as r`, [s.ne_slip_no, seq, JSON.stringify(s.header), JSON.stringify(s.lines)])).r; };
const prod = (await one(`insert into core.products (company_id, name) values (1, '見本') returning product_id`)).product_id;
const skuA = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'yitem-a', '見本 SKU') returning sku_id`, [prod])).sku_id;
const prod2 = (await one(`insert into core.products (company_id, name) values (1, '見本 赤') returning product_id`)).product_id;
const skuRed = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'yitem-b-red', '見本 赤') returning sku_id`, [prod2])).sku_id;
const lstA = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'yahoo', 'main', 'yitem-a', 'active') returning listing_id`)).listing_id;

console.log('D5b-5: 通し (送り手 ⇄ 本物の受け口を HTTP で)');
const W = openWarehouse();
const L = openLedger(null, { memory: true, kind: 'order:yahoo' }); L.markInitialized();
await t('送る: 出品は Yahoo の商品コードで・SKU はサブコード (無ければ商品コード) で解決。サブコードが大文字でも当たる。状態は 0031 で正規化', async () => {
  insertRow(W, yo({ no: 'b-faith01-10000001' }));
  insertRow(W, yo({ no: 'b-faith01-10000002', line: 1, item: 'yitem-b', sub: 'YITEM-B-RED', price: 1000, qty: 1, total: 1000, point: 0, os: '2', ps: '1', ss: '1' }));
  insertRow(W, yo({ no: 'b-faith01-10000002', line: 2, item: 'no-such-item', sub: null, price: 500, qty: 1, total: 1000, point: 0, os: '2', ps: '1', ss: '1' }));
  const r = await push(W, L);
  assert.deepEqual([r.ok, r.scanned, r.inScope, r.applied, r.failed.length, r.transformErrors.length], [true, 2, 2, 2, 0, 0]);
  assert.deepEqual(posts().flatMap((b) => b.rows.map((x) => x.mall_order_no)).sort(), ['b-faith01-10000001', 'b-faith01-10000002']);
  const rows = (await pg.query(`select o.mall_order_no, o.shop_code, o.status, o.items_amount_jpy, o.total_amount_jpy, o.points_used_jpy, o.order_date_jst::text as d, l.line_key, l.listing_id, l.sku_id, l.unresolved_code
    from core.orders o join core.order_lines l using (order_id) where o.mall = 'yahoo' order by o.mall_order_no, l.line_key`)).rows;
  assert.deepEqual(rows.map((x) => [x.mall_order_no, x.shop_code, x.status, Number(x.items_amount_jpy), Number(x.total_amount_jpy), Number(x.points_used_jpy), x.d, x.line_key, x.listing_id == null ? null : Number(x.listing_id), x.sku_id == null ? null : Number(x.sku_id), x.unresolved_code]), [
    ['b-faith01-10000001', '2', 'shipped', 4110, 3816, 294, '2026-03-01', '1', Number(lstA), Number(skuA), null],
    ['b-faith01-10000002', '2', 'confirmed', 1500, 1000, 0, '2026-03-01', '1', null, Number(skuRed), null],
    ['b-faith01-10000002', '2', 'confirmed', 1500, 1000, 0, '2026-03-01', '2', null, null, 'no-such-item']]);
});
await t('2 回目は変化なし (取込時刻だけ毎朝変わる) → 状態が進んだ注文だけ送る → 突合が一致 → 取消になった注文は cancelled', async () => {
  W.prepare(`update raw_yahoo_orders set synced_at = '2026-09-26 00:10:00'`).run();
  assert.equal((await push(W, L)).changed, 0);
  W.prepare(`update raw_yahoo_orders set ship_status = '3' where order_id = 'b-faith01-10000002'`).run();
  const r = await push(W, L);
  assert.deepEqual([r.changed, r.applied], [1, 1]);
  assert.equal((await one(`select status from core.orders where mall = 'yahoo' and mall_order_no = 'b-faith01-10000002'`)).status, 'shipped');
  assert.deepEqual(W.prepare(MALL_SPECS.yahoo.dailySql).all('2026-03-01', '2026-03-01'), [{ order_date: '2026-03-01', orders: 2, lines: 3, items_amount_jpy: 5610, cancelled: 0 }]);
  assert.equal((await reconcileOrdersDaily({ mall: 'yahoo', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-01-01', to: '2026-03-31', log: quiet })).ok, true);
  W.prepare(`update raw_yahoo_orders set order_status = '4', pay_status = '1', ship_status = '1' where order_id = 'b-faith01-10000001'`).run();
  assert.equal((await reconcileOrdersDaily({ mall: 'yahoo', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2026-03-01', to: '2026-03-31', log: quiet })).ok, false);
  await push(W, L);
  const o = await one(`select status, is_cancelled from core.orders where mall = 'yahoo' and mall_order_no = 'b-faith01-10000001'`);
  assert.deepEqual([o.status, o.is_cancelled], ['cancelled', true]);
  assert.equal((await reconcileOrdersDaily({ mall: 'yahoo', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2026-03-01', to: '2026-03-31', log: quiet })).ok, true);
});
await t('NE 店舗 2 の伝票は受注番号 (8 桁) で「b-faith01-」つきの注文に結ばれる', async () => {
  await applyShipment({ slip: 'S-Y-1', orderNo: '10000003' });
  insertRow(W, yo({ no: 'b-faith01-10000003' }));
  const r = await push(W, L);
  assert.ok(r.ok && r.afterSend && r.afterSend.ran && !r.afterSend.error, JSON.stringify(r.afterSend));
  assert.equal(await num(`select count(*) as n from core.shipments s join core.orders o using (order_id) where s.ne_slip_no = 'S-Y-1' and o.mall = 'yahoo' and o.mall_order_no = 'b-faith01-10000003'`), 1);
});
await t('🚨 注文日時・注文番号が読めない注文は、どの mode でも黙って範囲の外に落ちず「整形できない」❌。後ろの明細だけ日時が違う注文も同じ', async () => {
  const w = openWarehouse();
  const bad = ['b-faith01-20000001', 'b-faith01-20000002', 'b-faith01-20000003', ' b-faith01-20000004', 'b-faith01-20000005'];
  const times = ['2026-03-01 10:00:00', '2026-13-01T10:00:00+09:00', Buffer.from('2024-09-18T10:05:00+09:00'), '2026-03-05T10:00:00+09:00', '2026-03-05T10:00:00+09:00'];
  bad.forEach((no, i) => insertRow(w, yo({ no, time: times[i] })));
  insertRow(w, yo({ no: 'b-faith01-20000005', line: 2, time: '2024-01-01T10:00:00+09:00' }));   // 後ろの明細だけ日時が違う (先頭は範囲の中)
  insertRow(w, yo({ no: 'b-faith01-20000009', time: '2026-03-05T10:00:00+09:00' }));
  for (const x of [{}, { from: '2026-03-01', to: '2026-03-31' }, { from: '2026-04-01', to: '2026-04-30' }]) {
    const l = openLedger(null, { memory: true, kind: 'order:yahoo' }); l.markInitialized();
    const r = await push(w, l, { dryRun: true, ...x });
    assert.equal(r.transformErrors.length, bad.length, `${JSON.stringify(x)}: ${JSON.stringify(r.transformErrors)}`);
    for (const no of bad) assert.match(JSON.stringify(r.transformErrors), new RegExp(no.trim()));
    l.close();
  }
  w.close();
});
await t('🚨 取消の通し (#1465 Codex R1 P1): API の応答 → 取込 (insertOrders) → raw → 送り手 → Company DB。後から取り消された注文 (OrderStatus 4・数量 0) が raw に届いて cancelled になる。取消でない数量 0・数量が空は今まで通り skip (欠落を 0 にしない)', async () => {
  const w = openWarehouse();
  const api = (no, os, items) => ({ orderId: no, data: { ResultSet: { Result: { Status: 'OK', OrderInfo: { OrderId: no, OrderTime: '2026-03-10T10:00:00+09:00', LastUpdateTime: '2026-03-11T09:00:00+09:00', OrderStatus: os,
    Pay: { PayStatus: '1' }, Ship: { ShipStatus: os === '4' ? '1' : '1' }, Detail: { TotalPrice: '2000', PayCharge: '0', ShipCharge: '0', Discount: '0', UsePoint: '0' },
    Item: items.map((x, i) => ({ LineId: String(i + 1), ItemId: 'yitem-a', Title: '商品', SubCode: '', UnitPrice: '1000', OriginalPrice: '0', Quantity: x, ItemTaxRatio: '10', CouponDiscount: '0' })) } } } } });
  // 1 回目: ふつうの注文 (処理中・数量 2)
  let r = insertOrders(w, [api('b-faith01-30000001', '2', ['2'])], 'b1', 'x', 'y');
  assert.deepEqual([r.currentCount, r.skippedInvalid], [1, 0]);
  // 2 回目: 取り消された (数量 0 で返る) → raw が取消になる。取消でない数量 0 と、取消でも数量が空の注文は skip
  r = insertOrders(w, [api('b-faith01-30000001', '4', ['0']), api('b-faith01-30000002', '2', ['0']), api('b-faith01-30000003', '4', [''])], 'b2', 'x', 'y');
  assert.deepEqual([r.currentCount, r.skippedInvalid], [1, 2]);
  assert.deepEqual(w.prepare(`select order_status, quantity from raw_yahoo_orders where order_id = 'b-faith01-30000001'`).all(), [{ order_status: '4', quantity: 0 }]);
  assert.equal(w.prepare(`select count(*) as n from raw_yahoo_orders where order_id in ('b-faith01-30000002', 'b-faith01-30000003')`).get().n, 0);
  // 送り手 → Company DB: 取消として入る。売上日次は回さない (MALL_SPECS.yahoo.salesDaily = false)
  const l = openLedger(null, { memory: true, kind: 'order:yahoo' }); l.markInitialized();
  const p = await push(w, l);
  assert.deepEqual([p.ok, p.applied, p.transformErrors.length], [true, 1, 0]);
  const o = await one(`select status, is_cancelled, items_amount_jpy from core.orders where mall = 'yahoo' and mall_order_no = 'b-faith01-30000001'`);
  assert.deepEqual([o.status, o.is_cancelled, Number(o.items_amount_jpy)], ['cancelled', true, 0]);
  assert.equal(MALL_SPECS.yahoo.salesDaily, false);
  l.close(); w.close();
});
L.close(); W.close(); server.close();

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
