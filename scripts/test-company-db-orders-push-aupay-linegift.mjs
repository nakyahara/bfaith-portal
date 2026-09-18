#!/usr/bin/env node
/**
 * test-company-db-orders-push-aupay-linegift.mjs — D5b-3 = au PAY マーケット / LINE ギフトの注文の push (raw_aupay_orders / raw_linegift_orders → core.orders) の試験。
 *   整形 (純粋関数) / 0019 の状態対応表 / 通し (送り手 ⇄ 本物の router を HTTP で): 個人情報の列を読まない・運ばない、範囲、SKU の解決、差分、突合、NE 店舗 5 / 14 の伝票との結び
 * 楽天・Amazon と共通の部分 (台帳・chunk・再送・lock・結び直しの持ち越し・完了印の CLI) は test-company-db-orders-push.mjs / -amazon.mjs が見ている。
 * 🚨 HTTP の先 (Render) と本番の raw は試験に無い → 初回は --dry-run → 送る → --reconcile
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildAupayOrder, aupayDatetimeToIso, AUPAY_COLUMNS, AUPAY_TRANSFORM_VERSION, buildLinegiftOrder, LINEGIFT_COLUMNS, LINEGIFT_TRANSFORM_VERSION } from '../apps/company-db/push/mall-orders-transform.mjs';
import { pushOrders, reconcileOrdersDaily, MALL_SPECS, specOf } from '../apps/company-db/push/mall-orders.mjs';
import { openLedger } from '../apps/company-db/push/ledger.mjs';
import { fingerprintOf } from '../apps/company-db/push/pipeline.mjs';
import { buildShipment } from '../apps/company-db/push/ne-shipments-transform.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };
const quiet = () => {};
const pick = (v, d) => (v === undefined ? d : v);
const SECRET = '個人情報のつもりの値';   // 送り手がこの値をどこにも運ばないことを見る

// ─── SQLite の見本 (warehouse.db と同じ列名。個人情報の列も置く = 読まないことを確かめる) ───
function openWarehouse() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_aupay_orders (order_id TEXT NOT NULL, order_detail_id TEXT NOT NULL, order_date TEXT, mail_address TEXT, orderer_name TEXT, orderer_address TEXT, orderer_phone_number1 TEXT, sender_name TEXT, sender_address TEXT,
    user_comment TEXT, memo TEXT, order_status TEXT, ship_status TEXT, cancel_status TEXT, total_sale_price REAL, postage_price REAL, charge_price REAL, total_price REAL, coupon_total_price REAL, use_point REAL, use_au_point_price REAL, request_price REAL,
    item_management_id TEXT, item_code TEXT, lotnumber TEXT, item_name TEXT, item_cancel_status TEXT, item_price REAL, unit INTEGER, total_item_price REAL, tax_rate REAL, synced_at TEXT, PRIMARY KEY (order_id, order_detail_id))`);
  db.exec(`CREATE TABLE raw_linegift_orders (order_id TEXT NOT NULL PRIMARY KEY, status TEXT, user_name TEXT, selling_price REAL, fee REAL, parent_item_code TEXT, sku_code TEXT, sku_name TEXT, stock_count INTEGER, shipping_fee REAL,
    bought_at_jst TEXT, bought_date_jst TEXT, delivered_at_jst TEXT, received_at_jst TEXT, delivery_code TEXT, address_first_name TEXT, address_last_name TEXT, address_zip TEXT, address_tel TEXT, address_address TEXT, synced_at TEXT)`);
  db.exec(`CREATE TABLE raw_ne_order_base (伝票番号 TEXT PRIMARY KEY, 受注番号 TEXT, 店舗コード TEXT, 受注日 TEXT, 出荷確定日 TEXT)`);
  return db;
}
const au = (x) => ({ order_id: x.no, order_detail_id: pick(x.detail, '1'), order_date: pick(x.date, '2025/03/01 10:05'), mail_address: SECRET, orderer_name: SECRET, orderer_address: SECRET, orderer_phone_number1: SECRET, sender_name: SECRET, sender_address: SECRET,
  user_comment: SECRET, memo: SECRET, order_status: pick(x.status, '完了'), ship_status: 'Y', cancel_status: pick(x.cancel, 'N'), total_sale_price: pick(x.sale, 2000), postage_price: pick(x.post, 500), charge_price: 0, total_price: 2500,
  coupon_total_price: pick(x.coupon, 100), use_point: pick(x.point, 0), use_au_point_price: pick(x.auPoint, 50), request_price: pick(x.request, 2350), item_management_id: '', item_code: pick(x.item, 'sku-a'), lotnumber: 'L1', item_name: '商品名',
  item_cancel_status: pick(x.itemCancel, 'N'), item_price: pick(x.price, 1000), unit: pick(x.unit, 2), total_item_price: pick(x.lineTotal, 2000), tax_rate: pick(x.tax, 0.1), synced_at: pick(x.synced, '2026-09-17 23:01:44') });
const lg = (x) => ({ order_id: x.no, status: pick(x.status, 'received'), user_name: SECRET, selling_price: pick(x.price, 3000), fee: 389, parent_item_code: 'p-1', sku_code: pick(x.sku, 'sku-a'), sku_name: '商品名', stock_count: pick(x.qty, 1), shipping_fee: null,
  bought_at_jst: pick(x.date, '2026-03-01T10:00:00.000+09:00'), bought_date_jst: '2026-03-01', delivered_at_jst: pick(x.delivered, '2026-03-02T15:00:00.000+09:00'), received_at_jst: '2026-03-02T15:00:01.000+09:00', delivery_code: '1234', address_first_name: SECRET,
  address_last_name: SECRET, address_zip: SECRET, address_tel: SECRET, address_address: SECRET, synced_at: pick(x.synced, '2026-09-18T07:30:00.000+09:00') });
const insertRow = (db, table, r) => { const cols = Object.keys(r); db.prepare(`insert into ${table} (${cols.join(', ')}) values (${cols.map((c) => '@' + c).join(', ')})`).run(r); };
/** 送り手が実際に読む列だけに絞った行 (iterate が select する形) */
const only = (r, cols) => Object.fromEntries(cols.map((c) => [c, r[c]]));

console.log('D5b-3: 整形 (au PAY)');
await t('au PAY の 2 明細: 鍵・scope main・shop_code 5・注文日時 (秒なしの JST)・金額 (顧客が払った額 / 商品代 / 送料 / 店負担の値引 / ポイント = 通常 + au)・明細は item_code を SKU として', async () => {
  const b = buildAupayOrder([only(au({ no: 'A-1', detail: '2', item: 'sku-b', unit: 1, price: 540, lineTotal: 540, tax: 0.08 }), AUPAY_COLUMNS), only(au({ no: 'A-1', detail: '1' }), AUPAY_COLUMNS)]);
  const h = b.payload.header;
  assert.deepEqual([b.key, b.payload.mall, b.payload.scope_key, h.shop_code, h.ordered_at, h.status_source, h.is_cancelled, h.source_updated_at, h.transform_version],
    ['aupay|main|A-1', 'aupay', 'main', '5', '2025-03-01T10:05:00+09:00', '完了', false, '2026-09-17T23:01:44Z', AUPAY_TRANSFORM_VERSION]);
  assert.deepEqual([h.total_amount_jpy, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, h.mall_coupon_jpy, h.points_used_jpy], [2350, 2000, 500, 100, null, 50]);
  assert.deepEqual(b.payload.lines, [
    { line_key: '1', listing_code: null, sku_code: 'sku-a', qty: 2, cancelled_qty: 0, unit_price_jpy: 1000, line_amount_jpy: 2000, tax_rate: 0.1, amount_source: 'mall_api', source_line_ref: 'order_detail_id:1' },
    { line_key: '2', listing_code: null, sku_code: 'sku-b', qty: 1, cancelled_qty: 0, unit_price_jpy: 540, line_amount_jpy: 540, tax_rate: 0.08, amount_source: 'mall_api', source_line_ref: 'order_detail_id:2' }]);
});
await t('au PAY の取消: cancel_status C か order_status キャンセル → is_cancelled。明細の取消 C は cancelled_qty = 数量。知らない取消区分は例外', async () => {
  assert.equal(buildAupayOrder([only(au({ no: 'A-2', status: 'キャンセル', cancel: 'C' }), AUPAY_COLUMNS)]).payload.header.is_cancelled, true);
  assert.equal(buildAupayOrder([only(au({ no: 'A-3', status: 'キャンセル', cancel: 'N' }), AUPAY_COLUMNS)]).payload.header.is_cancelled, true);
  assert.equal(buildAupayOrder([only(au({ no: 'A-4', itemCancel: 'C' }), AUPAY_COLUMNS)]).payload.lines[0].cancelled_qty, 2);
  throws(() => buildAupayOrder([only(au({ no: 'A-5', itemCancel: 'X' }), AUPAY_COLUMNS)]), /item_cancel_status が知らない値/);
});
await t('au PAY: 欠落を 0 にしない・負を通さない・注文の列の食い違い・明細の重複・日時の形は例外。金額の null は null のまま。取込時刻だけ変わっても指紋は同じ', async () => {
  throws(() => buildAupayOrder([only(au({ no: 'A-6', unit: null }), AUPAY_COLUMNS)]), /unit が無い/);
  throws(() => buildAupayOrder([only(au({ no: 'A-7', sale: -1 }), AUPAY_COLUMNS)]), /0 以上の数でない/);
  throws(() => buildAupayOrder([only(au({ no: 'A-8' }), AUPAY_COLUMNS), only(au({ no: 'A-8', detail: '2', request: 9 }), AUPAY_COLUMNS)]), /request_price が行によって違う/);
  throws(() => buildAupayOrder([only(au({ no: 'A-9' }), AUPAY_COLUMNS), only(au({ no: 'A-9' }), AUPAY_COLUMNS)]), /order_detail_id 1 が重複/);
  throws(() => buildAupayOrder([only(au({ no: 'A-10', date: '2025-13-01 10:00' }), AUPAY_COLUMNS)]), /日時として不正|形が違う/);
  throws(() => buildAupayOrder([only(au({ no: 'A-11', item: '' }), AUPAY_COLUMNS)]), /item_code が無い/);
  const n = buildAupayOrder([only(au({ no: 'A-12', coupon: null, point: null, auPoint: null }), AUPAY_COLUMNS)]).payload.header;
  assert.deepEqual([n.shop_coupon_jpy, n.points_used_jpy], [null, null]);
  const a = buildAupayOrder([only(au({ no: 'A-13' }), AUPAY_COLUMNS)]), b = buildAupayOrder([only(au({ no: 'A-13', synced: '2026-09-18 23:00:00' }), AUPAY_COLUMNS)]);
  assert.equal(fingerprintOf(AUPAY_TRANSFORM_VERSION, a.payload), fingerprintOf(AUPAY_TRANSFORM_VERSION, b.payload));
  assert.deepEqual([aupayDatetimeToIso('2025/03/01 10:05'), aupayDatetimeToIso('2025/03/01 10:05:09'), aupayDatetimeToIso('')], ['2025-03-01T10:05:00+09:00', '2025-03-01T10:05:09+09:00', null]);
});

console.log('D5b-3: 整形 (LINE ギフト)');
await t('LINE ギフト: 1 行 = 1 注文 = 1 明細。shop_code 14・商品代 = 売価・送料や値引は null・発送時刻 = delivered_at・取込時刻は ISO8601 +09:00', async () => {
  const b = buildLinegiftOrder([only(lg({ no: '123456789' }), LINEGIFT_COLUMNS)]);
  const h = b.payload.header;
  assert.deepEqual([b.key, h.shop_code, h.ordered_at, h.status_source, h.is_cancelled, h.shipped_at_source, h.source_updated_at, h.transform_version],
    ['linegift|main|123456789', '14', '2026-03-01T10:00:00+09:00', 'received', false, '2026-03-02T15:00:00+09:00', '2026-09-18T07:30:00+09:00', LINEGIFT_TRANSFORM_VERSION]);
  assert.deepEqual([h.total_amount_jpy, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, h.mall_coupon_jpy, h.points_used_jpy], [null, 3000, null, null, null, null]);
  assert.deepEqual(b.payload.lines, [{ line_key: '1', listing_code: null, sku_code: 'sku-a', qty: 1, cancelled_qty: 0, unit_price_jpy: 3000, line_amount_jpy: 3000, tax_rate: null, amount_source: 'mall_api', source_line_ref: 'order' }]);
});
await t('LINE ギフト: 取消は is_cancelled + cancelled_qty = 数量 (発送時刻は無い)。2 行・数量なし・SKU なし・注文日なしは例外', async () => {
  const c = buildLinegiftOrder([only(lg({ no: 'L-2', status: 'cancel', delivered: null }), LINEGIFT_COLUMNS)]);
  assert.deepEqual([c.payload.header.is_cancelled, c.payload.header.shipped_at_source, c.payload.lines[0].cancelled_qty], [true, null, 1]);
  throws(() => buildLinegiftOrder([only(lg({ no: 'L-3' }), LINEGIFT_COLUMNS), only(lg({ no: 'L-3' }), LINEGIFT_COLUMNS)]), /1 行のはず/);
  throws(() => buildLinegiftOrder([only(lg({ no: 'L-4', qty: null }), LINEGIFT_COLUMNS)]), /数量/);
  throws(() => buildLinegiftOrder([only(lg({ no: 'L-5', sku: '' }), LINEGIFT_COLUMNS)]), /sku_code が無い/);
  throws(() => buildLinegiftOrder([only(lg({ no: 'L-6', date: null }), LINEGIFT_COLUMNS)]), /bought_at_jst が無い/);
});

console.log('D5b-3: 受け皿 (0019 の状態対応表)');
const pg = new PGlite();
const pdb = pgliteAdapter(pg);
const applied0 = await applyMigrations(pdb, { log: quiet });
assert.ok(applied0.applied.includes('0019'), '0019 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
await t('0019: 実測で出ていた状態がどれも unknown にならない。au PAY の 完了・LINE ギフトの received は shipped (根拠の無い delivered を作らない)', async () => {
  const m = async (sys, v) => (await one(`select core.map_order_status($1, $2) as s`, [sys, v])).s;
  assert.deepEqual([await m('aupay', '完了'), await m('aupay', 'キャンセル'), await m('aupay', '発送前入金待ち'), await m('aupay', '発送待ち')], ['shipped', 'cancelled', 'new', 'confirmed']);
  assert.deepEqual([await m('linegift', 'received'), await m('linegift', 'cancel'), await m('linegift', 'payment'), await m('linegift', 'gift_message_send'), await m('linegift', 'gift_message_wait'), await m('linegift', 'cvs')],
    ['shipped', 'cancelled', 'confirmed', 'new', 'new', 'new']);
  assert.equal(await m('aupay', 'まだ知らない状態'), 'unknown');
  assert.equal(await num(`select count(*) as n from core.order_status_map where source_system in ('aupay', 'linegift') and status = 'delivered'`), 0);
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
const push = (mall, w, l, x = {}) => pushOrders({ mall, warehouse: w, ledger: l, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });
const shipBase = (x) => ({ 伝票番号: x.slip, 受注番号: x.orderNo, 店舗コード: x.shop, 受注日: '2025-03-01 10:00:00', 出荷確定日: '2025-03-01 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済', キャンセル区分: '有効', 受注キャンセル日: '', 配送方法ID: '28', 配送方法名: 'ネコポス', 送り状番号: '', synced_at: '2026-09-10 00:00:00' });
const applyShipment = async (x, seq = 1) => { const s = buildShipment(shipBase(x), []); return (await one(`select core.apply_shipment_batch(1::smallint, $1, $2::bigint, $3::jsonb, $4::jsonb) as r`, [s.ne_slip_no, seq, JSON.stringify(s.header), JSON.stringify(s.lines)])).r; };
// SKU の見本 (sku-a だけ Company DB にある)
const prod = (await one(`insert into core.products (company_id, name) values (1, '見本') returning product_id`)).product_id;
const skuA = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'sku-a', '見本 SKU') returning sku_id`, [prod])).sku_id;

console.log('D5b-3: 通し (送り手 ⇄ 本物の受け口を HTTP で)');
const W = openWarehouse();
await t('🚨 送り手は個人情報・自由記述の列を読まない・運ばない (au PAY / LINE ギフトとも。select する列は固定の一覧)', async () => {
  for (const c of AUPAY_COLUMNS.concat(LINEGIFT_COLUMNS)) assert.equal(/name|address|phone|mail|zip|tel|comment|memo|kana|nickname|message|remarks/i.test(c), false, `個人情報らしい列を読んでいる: ${c}`);
  insertRow(W, 'raw_aupay_orders', au({ no: 'P-1' })); insertRow(W, 'raw_linegift_orders', lg({ no: '900000001' }));
  for (const mall of ['aupay', 'linegift']) for (const g of specOf(mall).iterate(W, {})) assert.equal(JSON.stringify(g).includes(SECRET), false, `${mall} の iterate が個人情報の値を読んでいる`);
  W.exec(`delete from raw_aupay_orders; delete from raw_linegift_orders`);
});
const LA = openLedger(null, { memory: true, kind: 'order:aupay' }); LA.markInitialized();
await t('au PAY の通し: 範囲 (注文日 2025-01-01 以降 + 範囲内の出荷が参照する古い注文) → SKU は item_code で解決・無ければ原文 → 2 回目は変化なし → 状態の変化だけ送る → 突合が一致。payload に個人情報の値が無い', async () => {
  insertRow(W, 'raw_aupay_orders', au({ no: 'AU-1', detail: '1' })); insertRow(W, 'raw_aupay_orders', au({ no: 'AU-1', detail: '2', item: 'no-such-sku', unit: 1, price: 0, lineTotal: 0 }));
  insertRow(W, 'raw_aupay_orders', au({ no: 'AU-2', status: 'キャンセル', cancel: 'C' }));
  insertRow(W, 'raw_aupay_orders', au({ no: 'AU-OLD', date: '2024/12/30 09:00' }));      // 範囲内の出荷が参照する
  insertRow(W, 'raw_aupay_orders', au({ no: 'AU-OLD2', date: '2024/12/30 09:00' }));     // 誰も参照しない
  W.prepare(`insert into raw_ne_order_base values ('S-AU-OLD', 'AU-OLD', '5', '2024-12-30 10:00:00', '2025-01-05 15:00:00')`).run();
  const before = posts().length;
  const r = await push('aupay', W, LA);
  assert.deepEqual([r.ok, r.scanned, r.inScope, r.applied, r.failed.length, r.transformErrors.length, r.stats.referencedCount], [true, 4, 3, 3, 0, 0, 1]);
  const bodies = posts().slice(before);
  assert.equal(JSON.stringify(bodies).includes(SECRET), false, '個人情報の値を送っている');
  const rows = (await pg.query(`select o.mall_order_no, o.shop_code, o.status, o.is_cancelled, o.total_amount_jpy, o.points_used_jpy, o.order_date_jst::text as d, l.line_key, l.sku_id, l.unresolved_code, l.line_amount_jpy
    from core.orders o join core.order_lines l using (order_id) where o.mall = 'aupay' order by o.mall_order_no, l.line_key`)).rows;
  assert.deepEqual(rows.map((x) => [x.mall_order_no, x.shop_code, x.status, x.is_cancelled, Number(x.total_amount_jpy), Number(x.points_used_jpy), x.d, x.line_key, x.sku_id == null ? null : Number(x.sku_id), x.unresolved_code]), [
    ['AU-1', '5', 'shipped', false, 2350, 50, '2025-03-01', '1', Number(skuA), null],
    ['AU-1', '5', 'shipped', false, 2350, 50, '2025-03-01', '2', null, 'no-such-sku'],
    ['AU-2', '5', 'cancelled', true, 2350, 50, '2025-03-01', '1', Number(skuA), null],
    ['AU-OLD', '5', 'shipped', false, 2350, 50, '2024-12-30', '1', Number(skuA), null]]);
  assert.equal((await push('aupay', W, LA)).changed, 0);
  W.prepare(`update raw_aupay_orders set order_status = '発送待ち', synced_at = '2026-09-19 07:00:00' where order_id = 'AU-1'`).run();
  const r2 = await push('aupay', W, LA);
  assert.deepEqual([r2.changed, r2.applied], [1, 1]);
  assert.equal((await one(`select status from core.orders where mall = 'aupay' and mall_order_no = 'AU-1'`)).status, 'confirmed');
  assert.deepEqual(W.prepare(MALL_SPECS.aupay.dailySql).all('2025-03-01', '2025-03-01'), [{ order_date: '2025-03-01', orders: 2, lines: 3, items_amount_jpy: 4000, cancelled: 1 }]);
  assert.equal((await reconcileOrdersDaily({ mall: 'aupay', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2024-12-01', to: '2025-03-31', log: quiet })).ok, false, 'AU-OLD2 は送っていない = 2024-12-30 は食い違うはず');
  assert.equal((await reconcileOrdersDaily({ mall: 'aupay', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-01-01', to: '2025-03-31', log: quiet })).ok, true);
});
await t('au PAY: NE 店舗 5 の伝票は注文番号そのままで結ばれる (先に届いた伝票は push の後の結び直しで)。--from/--to は JST の注文日で切る', async () => {
  await applyShipment({ slip: 'S-AU-LATE', orderNo: 'AU-3', shop: '5' });
  insertRow(W, 'raw_aupay_orders', au({ no: 'AU-3', date: '2025/04/30 23:59' })); insertRow(W, 'raw_aupay_orders', au({ no: 'AU-4', date: '2025/05/01 00:00' }));
  const r = await push('aupay', W, LA, { from: '2025-04-01', to: '2025-04-30' });
  assert.deepEqual([r.ok, r.inScope, r.applied], [true, 1, 1]);
  assert.ok(r.afterSend && r.afterSend.ran && !r.afterSend.error, JSON.stringify(r.afterSend));
  assert.equal(await num(`select count(*) as n from core.shipments s join core.orders o using (order_id) where s.ne_slip_no = 'S-AU-LATE' and o.mall = 'aupay' and o.mall_order_no = 'AU-3'`), 1);
  assert.equal(await num(`select count(*) as n from core.orders where mall = 'aupay' and mall_order_no = 'AU-4'`), 0);
});
const LL = openLedger(null, { memory: true, kind: 'order:linegift' }); LL.markInitialized();
await t('LINE ギフトの通し: 送る → 状態・発送時刻・SKU の解決 → 変化なし → 取消に変わった注文だけ送る → 突合が一致 → NE 店舗 14 の伝票と結ばれる', async () => {
  insertRow(W, 'raw_linegift_orders', lg({ no: '900000001' })); insertRow(W, 'raw_linegift_orders', lg({ no: '900000002', status: 'payment', delivered: null, sku: 'no-such-sku' }));
  await applyShipment({ slip: 'S-LG-1', orderNo: '900000001', shop: '14' });
  const before = posts().length;
  const r = await push('linegift', W, LL);
  assert.deepEqual([r.ok, r.inScope, r.applied, r.transformErrors.length], [true, 2, 2, 0]);
  assert.equal(JSON.stringify(posts().slice(before)).includes(SECRET), false, '個人情報の値を送っている');
  const rows = (await pg.query(`select o.mall_order_no, o.shop_code, o.status, o.items_amount_jpy, o.total_amount_jpy, (o.shipped_at_source is not null) as shipped, l.sku_id, l.unresolved_code, l.qty
    from core.orders o join core.order_lines l using (order_id) where o.mall = 'linegift' order by o.mall_order_no`)).rows;
  assert.deepEqual(rows.map((x) => [x.mall_order_no, x.shop_code, x.status, Number(x.items_amount_jpy), x.total_amount_jpy, x.shipped, x.sku_id == null ? null : Number(x.sku_id), x.unresolved_code, x.qty]), [
    ['900000001', '14', 'shipped', 3000, null, true, Number(skuA), null, 1],
    ['900000002', '14', 'confirmed', 3000, null, false, null, 'no-such-sku', 1]]);
  assert.equal(await num(`select count(*) as n from core.shipments s join core.orders o using (order_id) where s.ne_slip_no = 'S-LG-1' and o.mall = 'linegift'`), 1);
  assert.equal((await push('linegift', W, LL)).changed, 0);
  W.prepare(`update raw_linegift_orders set status = 'cancel' where order_id = '900000002'`).run();
  const r2 = await push('linegift', W, LL);
  assert.deepEqual([r2.changed, r2.applied], [1, 1]);
  assert.deepEqual(W.prepare(MALL_SPECS.linegift.dailySql).all('2026-03-01', '2026-03-01'), [{ order_date: '2026-03-01', orders: 2, lines: 2, items_amount_jpy: 6000, cancelled: 1 }]);
  assert.equal((await reconcileOrdersDaily({ mall: 'linegift', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2026-03-01', to: '2026-03-31', log: quiet })).ok, true);
});
await t('整形できない注文は送らずに ❌ (ほかの注文は送る): au PAY の日時の形が違う注文は incremental で見つかる', async () => {
  insertRow(W, 'raw_aupay_orders', au({ no: 'AU-BAD', date: '2025年3月1日' }));
  const r = await push('aupay', W, LA);
  assert.deepEqual([r.ok, r.transformErrors.length], [false, 1]);
  assert.match(JSON.stringify(r.transformErrors), /AU-BAD/);
  assert.equal(await num(`select count(*) as n from core.orders where mall = 'aupay' and mall_order_no = 'AU-4'`), 1, '同じ run のほかの注文 (AU-4) は届いている');
});
await t('🚨 注文日時が読めない注文は、どの mode でも黙って範囲の外に落ちず「整形できない」❌ になる (au PAY / LINE ギフト × incremental / --from/--to。Codex R1 #1)。LINE ギフトは JST (+09:00) 以外の形も拒む (R1 #2)', async () => {
  const w = openWarehouse();
  insertRow(w, 'raw_aupay_orders', au({ no: 'AU-X1', date: '2025年3月1日' })); insertRow(w, 'raw_aupay_orders', au({ no: 'AU-X2', date: null })); insertRow(w, 'raw_aupay_orders', au({ no: 'AU-OK', date: '2025/03/05 10:00' }));
  insertRow(w, 'raw_linegift_orders', lg({ no: '910000001', date: '   ' })); insertRow(w, 'raw_linegift_orders', lg({ no: '910000002', date: '0000-invalid' }));
  insertRow(w, 'raw_linegift_orders', lg({ no: '910000003', date: '2026-03-01T23:30:00Z' })); insertRow(w, 'raw_linegift_orders', lg({ no: '910000004', date: '2026-03-05T10:00:00.000+09:00' }));
  // 形は合っているが日時として実在しない (Codex R2): 13 月 (範囲の中の年・範囲より前の年)・24 時・2 月 30 日。Date.parse は 24 時と 2/30 を翌日・3 月に繰り上げて受ける = 範囲・突合の日付と保存の日付がずれる
  insertRow(w, 'raw_aupay_orders', au({ no: 'AU-X3', date: '2025/13/01 10:00' })); insertRow(w, 'raw_aupay_orders', au({ no: 'AU-X4', date: '2025/03/01 24:00' })); insertRow(w, 'raw_aupay_orders', au({ no: 'AU-X5', date: '2025/02/30 10:00' }));
  insertRow(w, 'raw_linegift_orders', lg({ no: '910000005', date: '2026-13-01T10:00:00+09:00' })); insertRow(w, 'raw_linegift_orders', lg({ no: '910000006', date: '2024-13-01T10:00:00+09:00' }));
  insertRow(w, 'raw_linegift_orders', lg({ no: '910000007', date: '2026-03-01T24:00:00+09:00' })); insertRow(w, 'raw_linegift_orders', lg({ no: '910000008', date: '2026-02-30T10:00:00.000+09:00' }));
  for (const [mall, bad, good, range] of [['aupay', ['AU-X1', 'AU-X2', 'AU-X3', 'AU-X4', 'AU-X5'], 'AU-OK', { from: '2025-03-01', to: '2025-03-31' }],
    ['linegift', ['910000001', '910000002', '910000003', '910000005', '910000006', '910000007', '910000008'], '910000004', { from: '2026-03-01', to: '2026-03-31' }]]) {
    for (const x of [{}, range]) {
      const l = openLedger(null, { memory: true, kind: `order:${mall}` }); l.markInitialized();
      const r = await push(mall, w, l, { dryRun: true, ...x });
      assert.deepEqual([r.transformErrors.length, r.inScope], [bad.length, bad.length + 1], `${mall} ${JSON.stringify(x)}: ${JSON.stringify(r.transformErrors)}`);
      for (const no of bad) assert.match(JSON.stringify(r.transformErrors), new RegExp(no));
      assert.equal(JSON.stringify(r.transformErrors).includes(good), false);
      l.close();
    }
  }
  throws(() => buildLinegiftOrder([only(lg({ no: 'L-Z', date: '2026-03-01T23:30:00Z' }), LINEGIFT_COLUMNS)]), /JST \(\+09:00\) の ISO8601 でない/);
  throws(() => buildLinegiftOrder([only(lg({ no: 'L-Z2', delivered: '2026-03-02 15:00:00' }), LINEGIFT_COLUMNS)]), /JST \(\+09:00\) の ISO8601 でない/);
  w.close();
});
await t('🚨 ログ・dry-run の「例」・整形できない注文の記録にも個人情報の値が出ない。LINE ギフトは取込時刻 (synced_at) だけ変わっても送り直さない (毎朝の挙動。R1 #3)', async () => {
  const w = openWarehouse();
  insertRow(w, 'raw_aupay_orders', au({ no: 'AU-L1' })); insertRow(w, 'raw_aupay_orders', au({ no: 'AU-L2', unit: null })); insertRow(w, 'raw_aupay_orders', au({ no: 'AU-L3', date: 'こわれた日時' }));
  insertRow(w, 'raw_linegift_orders', lg({ no: '920000001' })); insertRow(w, 'raw_linegift_orders', lg({ no: '920000002', qty: null }));
  for (const mall of ['aupay', 'linegift']) {
    const logs = []; const l = openLedger(null, { memory: true, kind: `order:${mall}` }); l.markInitialized();
    const dry = await push(mall, w, l, { dryRun: true, log: (m) => logs.push(String(m)) });
    const real = await push(mall, w, l, { log: (m) => logs.push(String(m)) });
    assert.ok(logs.length > 0, 'ログが 1 行も取れていない = 試験になっていない');
    assert.ok(dry.transformErrors.length >= 1 && real.transformErrors.length >= 1);
    assert.equal((logs.join('\n') + JSON.stringify(dry) + JSON.stringify(real)).includes(SECRET), false, `${mall}: ログか戻り値に個人情報の値が出ている`);
    l.close();
  }
  const l2 = openLedger(null, { memory: true, kind: 'order:linegift' }); l2.markInitialized();
  w.prepare(`delete from raw_linegift_orders where order_id = '920000002'`).run();
  assert.equal((await push('linegift', w, l2)).ok, true);
  w.prepare(`update raw_linegift_orders set synced_at = '2026-09-19T07:30:00.000+09:00'`).run();
  const again = await push('linegift', w, l2);
  assert.deepEqual([again.ok, again.inScope, again.changed], [true, 1, 0]);
  l2.close(); w.close();
});
LA.close(); LL.close(); W.close(); server.close();

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
