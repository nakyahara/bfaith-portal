#!/usr/bin/env node
/**
 * test-company-db-orders-push-qoo10.mjs — D5b-4 = Qoo10 の注文の push (raw_qoo10_orders の API の行 → core.orders) の試験。
 *   整形 (純粋関数) / 0020 の状態対応表 / 通し (送り手 ⇄ 本物の router を HTTP で): 旧データの行を送らない・出品と SKU の解決・差分・突合・NE 店舗 6 の伝票との結び・読めない日時
 * ほかのモールと共通の部分 (台帳・chunk・再送・lock・結び直しの持ち越し・完了印の CLI) は test-company-db-orders-push.mjs / -amazon.mjs が見ている。
 * 🚨 HTTP の先 (Render) と本番の raw は試験に無い → 初回は --dry-run → 送る → --reconcile
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildQoo10Order, qoo10DatetimeToIso, isQoo10Jst, QOO10_COLUMNS, QOO10_TRANSFORM_VERSION } from '../apps/company-db/push/mall-orders-transform.mjs';
import { pushOrders, reconcileOrdersDaily, MALL_SPECS } from '../apps/company-db/push/mall-orders.mjs';
import { openLedger } from '../apps/company-db/push/ledger.mjs';
import { fingerprintOf } from '../apps/company-db/push/pipeline.mjs';
import { buildShipment } from '../apps/company-db/push/ne-shipments-transform.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };
const quiet = () => {};
const pick = (v, d) => (v === undefined ? d : v);

// ─── SQLite の見本 (warehouse.db の raw_qoo10_orders と同じ列名・NOT NULL DEFAULT 0 = sql/qoo10/raw_qoo10_orders.sql) ───
function openWarehouse() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_qoo10_orders (order_id TEXT NOT NULL PRIMARY KEY, source_type TEXT NOT NULL, source_order_key TEXT NOT NULL, pack_no INTEGER NOT NULL, shipping_status TEXT NOT NULL, item_code TEXT NOT NULL, seller_item_code TEXT NOT NULL,
    item_title TEXT NOT NULL DEFAULT '', option_code TEXT NOT NULL DEFAULT '', order_price REAL NOT NULL DEFAULT 0, order_qty INTEGER NOT NULL DEFAULT 0, discount REAL NOT NULL DEFAULT 0, total REAL NOT NULL DEFAULT 0, settle_price REAL NOT NULL DEFAULT 0,
    seller_discount REAL NOT NULL DEFAULT 0, cart_discount_seller REAL NOT NULL DEFAULT 0, cart_discount_qoo10 REAL NOT NULL DEFAULT 0, shipping_rate REAL NOT NULL DEFAULT 0, order_date TEXT NOT NULL, payment_date TEXT, shipping_date TEXT, delivered_date TEXT,
    last_api_snapshot_at TEXT, is_frozen_after_horizon INTEGER, legacy_fields_missing INTEGER, synced_at TEXT)`);
  db.exec(`CREATE TABLE raw_ne_order_base (伝票番号 TEXT PRIMARY KEY, 受注番号 TEXT, 店舗コード TEXT, 受注日 TEXT, 出荷確定日 TEXT)`);
  return db;
}
const qo = (x) => ({ order_id: pick(x.orderId, `api:${x.no}`), source_type: pick(x.type, 'api_v2'), source_order_key: x.no, pack_no: pick(x.pack, 900000001), shipping_status: pick(x.status, 'Delivered(5)'), item_code: pick(x.item, '1000000001'),
  seller_item_code: pick(x.sku, 'sku-a'), item_title: '商品名', option_code: '', order_price: pick(x.price, 1500), order_qty: pick(x.qty, 2), discount: pick(x.discount, 600), total: pick(x.total, 2400), settle_price: 2700, seller_discount: pick(x.sellerDisc, 0),
  cart_discount_seller: pick(x.cartSeller, 0), cart_discount_qoo10: pick(x.cartQoo10, 100), shipping_rate: pick(x.ship, 0), order_date: pick(x.date, '2026-03-01 10:00:00'), payment_date: null, shipping_date: pick(x.shipped, null), delivered_date: null,
  last_api_snapshot_at: pick(x.snap, '2026-09-18T23:40:00.123+09:00'), is_frozen_after_horizon: 0, legacy_fields_missing: pick(x.legacy, 0), synced_at: pick(x.synced, '2026-09-18T23:40:00.123+09:00') });
const insertRow = (db, r) => { const cols = Object.keys(r); db.prepare(`insert into raw_qoo10_orders (${cols.join(', ')}) values (${cols.map((c) => '@' + c).join(', ')})`).run(r); };
const only = (r) => Object.fromEntries(QOO10_COLUMNS.map((c) => [c, r[c]]));

console.log('D5b-4: 整形 (Qoo10)');
await t('API の 1 行 = 1 注文 = 1 明細: 鍵は注文番号・shop_code 6・商品代は値引前 (単価 × 数量)・モール負担 = discount (メガ割) + カートの Qoo10 負担・店負担 = seller 系・顧客が払った額は null・カート番号は明細に残す', async () => {
  const b = buildQoo10Order([only(qo({ no: '1234567890', sellerDisc: 30, cartSeller: 20 }))]);
  const h = b.payload.header;
  assert.deepEqual([b.key, b.payload.mall, b.payload.scope_key, h.shop_code, h.ordered_at, h.status_source, h.is_cancelled, h.shipped_at_source, h.source_updated_at, h.transform_version],
    ['qoo10|main|1234567890', 'qoo10', 'main', '6', '2026-03-01T10:00:00+09:00', 'Delivered(5)', false, null, '2026-09-18T23:40:00+09:00', QOO10_TRANSFORM_VERSION]);
  assert.deepEqual([h.total_amount_jpy, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, h.mall_coupon_jpy, h.points_used_jpy], [null, 3000, 0, 50, 700, null]);
  assert.deepEqual(b.payload.lines, [{ line_key: '1', listing_code: '1000000001', sku_code: 'sku-a', qty: 2, cancelled_qty: 0, unit_price_jpy: 1500, line_amount_jpy: 3000, tax_rate: null, amount_source: 'mall_api', source_line_ref: 'pack_no:900000001' }]);
});
await t('🚨 旧データの行・order_id の形が違う行・2 行は例外 (送らない)。数量の欠落・負の金額・コード無しも例外。単価 0 は「分からない」= 金額を null にして数える', async () => {
  throws(() => buildQoo10Order([only(qo({ no: '900000001', type: 'legacy_migration', orderId: 'legacy:900000001:1' }))]), /API の行でない/);
  throws(() => buildQoo10Order([only(qo({ no: '1234567891', orderId: 'api:9999999999' }))]), /order_id が 'api:<注文番号>' の形でない/);
  throws(() => buildQoo10Order([only(qo({ no: '1234567892' })), only(qo({ no: '1234567892' }))]), /1 行のはず/);
  throws(() => buildQoo10Order([only(qo({ no: '1234567893', qty: null }))]), /order_qty が無い/);
  throws(() => buildQoo10Order([only(qo({ no: '1234567894', discount: -1 }))]), /0 以上の数でない/);
  throws(() => buildQoo10Order([only(qo({ no: '1234567895', item: '', sku: '' }))]), /item_code も seller_item_code も無い/);
  const stats = {};
  const z = buildQoo10Order([only(qo({ no: '1234567896', price: 0 }))], { stats });
  assert.deepEqual([stats.zeroPrice, z.payload.header.items_amount_jpy, z.payload.header.shipping_fee_jpy, z.payload.header.mall_coupon_jpy, z.payload.lines[0].unit_price_jpy, z.payload.lines[0].line_amount_jpy, z.payload.lines[0].qty], [1, null, null, null, null, null, 2]);
});
await t('日時は原値のまま検証する (形・実在する日時・前後の空白・文字列でない値)。取込時刻 (last_api_snapshot_at) だけ変わっても指紋は同じ・状態が変われば変わる', async () => {
  assert.deepEqual([isQoo10Jst('2026-03-01 10:00:00'), isQoo10Jst('2026-02-29 10:00:00'), isQoo10Jst('2026-13-01 10:00:00'), isQoo10Jst('2026-03-01 24:00:00'), isQoo10Jst(' 2026-03-01 10:00:00'), isQoo10Jst('2026-03-01T10:00:00'),
    isQoo10Jst(Buffer.from('2026-03-01 10:00:00')), isQoo10Jst(20260301), isQoo10Jst(null)], [true, false, false, false, false, false, false, false, false]);
  assert.deepEqual([qoo10DatetimeToIso('2026-03-01 10:00:00'), qoo10DatetimeToIso(null), qoo10DatetimeToIso('')], ['2026-03-01T10:00:00+09:00', null, null]);
  throws(() => buildQoo10Order([only(qo({ no: '1234567897', date: '2026/03/01 10:00' }))]), /実在する日時でない/);
  throws(() => buildQoo10Order([only(qo({ no: '1234567898', shipped: '2026-03-02' }))]), /shipping_date/);
  assert.equal(buildQoo10Order([only(qo({ no: '1234567899', shipped: '2026-03-02 15:00:00' }))]).payload.header.shipped_at_source, '2026-03-02T15:00:00+09:00');
  const a = buildQoo10Order([only(qo({ no: '1234567800' }))]), b = buildQoo10Order([only(qo({ no: '1234567800', snap: '2026-09-19T07:10:00.000+09:00', synced: '2026-09-19T07:10:00.000+09:00' }))]), c = buildQoo10Order([only(qo({ no: '1234567800', status: 'On delivery(4)' }))]);
  assert.equal(fingerprintOf(QOO10_TRANSFORM_VERSION, a.payload), fingerprintOf(QOO10_TRANSFORM_VERSION, b.payload));
  assert.notEqual(fingerprintOf(QOO10_TRANSFORM_VERSION, a.payload), fingerprintOf(QOO10_TRANSFORM_VERSION, c.payload));
});

console.log('D5b-4: 受け皿 (0020 の状態対応表)');
const pg = new PGlite();
const pdb = pgliteAdapter(pg);
const applied0 = await applyMigrations(pdb, { log: quiet });
assert.ok(applied0.applied.includes('0020'), '0020 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
await t('0020: 実測で出ていた 4 つの状態がどれも unknown にならない。出ていない状態 (2) は unknown のまま (DQ に出る)', async () => {
  const m = async (v) => (await one(`select core.map_order_status('qoo10', $1) as s`, [v])).s;
  assert.deepEqual([await m('Awaiting shipping(1)'), await m('Seller confirm(3)'), await m('On delivery(4)'), await m('Delivered(5)'), await m('Request shipping(2)')], ['new', 'confirmed', 'shipped', 'delivered', 'unknown']);
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
const push = (w, l, x = {}) => pushOrders({ mall: 'qoo10', warehouse: w, ledger: l, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });
const shipBase = (x) => ({ 伝票番号: x.slip, 受注番号: x.orderNo, 店舗コード: '6', 受注日: '2026-03-01 10:00:00', 出荷確定日: '2026-03-01 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済', キャンセル区分: '有効', 受注キャンセル日: '', 配送方法ID: '28', 配送方法名: 'ネコポス', 送り状番号: '', synced_at: '2026-09-10 00:00:00' });
const applyShipment = async (x, seq = 1) => { const s = buildShipment(shipBase(x), []); return (await one(`select core.apply_shipment_batch(1::smallint, $1, $2::bigint, $3::jsonb, $4::jsonb) as r`, [s.ne_slip_no, seq, JSON.stringify(s.header), JSON.stringify(s.lines)])).r; };
const prod = (await one(`insert into core.products (company_id, name) values (1, '見本') returning product_id`)).product_id;
const skuA = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'sku-a', '見本 SKU') returning sku_id`, [prod])).sku_id;
const lstQ = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'qoo10', 'main', '1000000001', 'active') returning listing_id`)).listing_id;

console.log('D5b-4: 通し (送り手 ⇄ 本物の受け口を HTTP で)');
const W = openWarehouse();
const L = openLedger(null, { memory: true, kind: 'order:qoo10' }); L.markInitialized();
await t('🚨 API の行だけ送る: 旧データの行 (同じ注文がカート番号の鍵で二重にある) は送らずに数える。出品は Qoo10 の商品番号で・SKU は販売者商品コードで解決。無ければ原文を残す', async () => {
  insertRow(W, qo({ no: '1000000001' }));
  insertRow(W, qo({ no: '1000000002', item: '1000000099', sku: 'no-such-sku', status: 'Seller confirm(3)', discount: 0, total: 3000, cartQoo10: 0 }));
  insertRow(W, qo({ no: '900000001', orderId: 'legacy:900000001:1', type: 'legacy_migration', legacy: 1, date: '2026-03-01 10:00:00' }));   // 上の 1000000001 と同じ注文の旧データ (鍵 = カート番号)
  insertRow(W, qo({ no: '900000777', orderId: 'legacy:900000777:1', type: 'legacy_migration', legacy: 1, date: '2025-06-01 10:00:00' }));
  const r = await push(W, L);
  assert.deepEqual([r.ok, r.scanned, r.inScope, r.applied, r.failed.length, r.transformErrors.length, r.stats.skippedLegacy], [true, 2, 2, 2, 0, 0, 2]);
  assert.deepEqual(posts().flatMap((b) => b.rows.map((x) => x.mall_order_no)).sort(), ['1000000001', '1000000002']);
  const rows = (await pg.query(`select o.mall_order_no, o.shop_code, o.status, o.is_cancelled, o.items_amount_jpy, o.mall_coupon_jpy, o.total_amount_jpy, o.order_date_jst::text as d, l.listing_id, l.sku_id, l.unresolved_code, l.qty, l.source_line_ref
    from core.orders o join core.order_lines l using (order_id) where o.mall = 'qoo10' order by o.mall_order_no`)).rows;
  assert.deepEqual(rows.map((x) => [x.mall_order_no, x.shop_code, x.status, x.is_cancelled, Number(x.items_amount_jpy), Number(x.mall_coupon_jpy), x.total_amount_jpy, x.d, x.listing_id == null ? null : Number(x.listing_id), x.sku_id == null ? null : Number(x.sku_id), x.unresolved_code, x.qty, x.source_line_ref]), [
    ['1000000001', '6', 'delivered', false, 3000, 700, null, '2026-03-01', Number(lstQ), Number(skuA), null, 2, 'pack_no:900000001'],
    ['1000000002', '6', 'confirmed', false, 3000, 0, null, '2026-03-01', null, null, 'no-such-sku', 2, 'pack_no:900000001']]);
});
await t('2 回目は変化なし (取込時刻だけ毎朝変わる) → 状態が進んだ注文だけ送る → 突合は API の行だけで一致 (旧データの行は両側で数えない)', async () => {
  W.prepare(`update raw_qoo10_orders set last_api_snapshot_at = '2026-09-19T07:10:00.000+09:00', synced_at = '2026-09-19T07:10:00.000+09:00'`).run();
  assert.equal((await push(W, L)).changed, 0);
  W.prepare(`update raw_qoo10_orders set shipping_status = 'On delivery(4)' where order_id = 'api:1000000002'`).run();
  const r = await push(W, L);
  assert.deepEqual([r.changed, r.applied], [1, 1]);
  assert.equal((await one(`select status from core.orders where mall = 'qoo10' and mall_order_no = '1000000002'`)).status, 'shipped');
  assert.deepEqual(W.prepare(MALL_SPECS.qoo10.dailySql).all('2026-03-01', '2026-03-01'), [{ order_date: '2026-03-01', orders: 2, lines: 2, items_amount_jpy: 6000, cancelled: 0 }]);
  assert.equal((await reconcileOrdersDaily({ mall: 'qoo10', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-01-01', to: '2026-03-31', log: quiet })).ok, true);
  insertRow(W, qo({ no: '1000000003', price: 0 }));   // 単価 0 = 分からない → 両側とも商品代 0 として足す
  assert.equal((await reconcileOrdersDaily({ mall: 'qoo10', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2026-03-01', to: '2026-03-31', log: quiet })).ok, false);
  const r3 = await push(W, L);
  assert.deepEqual([r3.ok, r3.applied, r3.stats.zeroPrice], [true, 1, 1]);
  assert.equal((await reconcileOrdersDaily({ mall: 'qoo10', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2026-03-01', to: '2026-03-31', log: quiet })).ok, true);
});
await t('NE 店舗 6 の伝票は注文番号 (10 桁) で結ばれる。NE がカート番号 (9 桁) で起票した伝票は結べない (宿題 = 未結合の一覧に残る)', async () => {
  await applyShipment({ slip: 'S-Q-LATE', orderNo: '1000000004' }); await applyShipment({ slip: 'S-Q-PACK', orderNo: '900000001' });
  insertRow(W, qo({ no: '1000000004' }));
  const r = await push(W, L);
  assert.ok(r.ok && r.afterSend && r.afterSend.ran && !r.afterSend.error, JSON.stringify(r.afterSend));
  assert.equal(await num(`select count(*) as n from core.shipments s join core.orders o using (order_id) where s.ne_slip_no = 'S-Q-LATE' and o.mall = 'qoo10' and o.mall_order_no = '1000000004'`), 1);
  assert.equal((await one(`select order_id from core.shipments where ne_slip_no = 'S-Q-PACK'`)).order_id, null);
});
await t('🚨 注文日時が読めない API の行は、どの mode でも黙って範囲の外に落ちず「整形できない」❌ (形・13 月・24 時・前後の空白・BLOB)。旧データの行の日時は見ない', async () => {
  const w = openWarehouse();
  const bad = ['2000000001', '2000000002', '2000000003', '2000000004', '2000000005'];
  const dates = ['2026/03/01 10:00:00', '2026-13-01 10:00:00', '2026-03-01 24:00:00', ' 2026-03-05 10:00:00', Buffer.from('2024-09-18 10:05:00')];
  bad.forEach((no, i) => insertRow(w, qo({ no, date: dates[i] })));
  insertRow(w, qo({ no: '2000000009', date: '2026-03-05 10:00:00' }));
  insertRow(w, qo({ no: '900000002', orderId: 'legacy:900000002:1', type: 'legacy_migration', legacy: 1, date: 'こわれた日時' }));
  for (const x of [{}, { from: '2026-03-01', to: '2026-03-31' }]) {
    const l = openLedger(null, { memory: true, kind: 'order:qoo10' }); l.markInitialized();
    const r = await push(w, l, { dryRun: true, ...x });
    assert.deepEqual([r.transformErrors.length, r.inScope], [bad.length, bad.length + 1], `${JSON.stringify(x)}: ${JSON.stringify(r.transformErrors)}`);
    for (const no of bad) assert.match(JSON.stringify(r.transformErrors), new RegExp(no));
    l.close();
  }
  w.close();
});
L.close(); W.close(); server.close();

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
