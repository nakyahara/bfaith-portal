#!/usr/bin/env node
/**
 * test-company-db-orders-push.mjs — D5b-1 (楽天の注文 → Company DB の push) の受入試験 (08 §4.1 / §4.7 / §9 D5)
 *
 *   0016: 楽天の状態の対応表 / resolve_listing_id が別名 (external_ids) でも当たる (1 件に決まるときだけ) / relink_shipments_bulk (集合で結び直し・続きの取り方)
 *   整形 (純粋関数): 列の対応 / '+0900' → '+09:00' / キャンセル系 / 番兵 -9999 と負の金額は null / 明細の並び / 指紋は行順と synced_at に依らない / 欠落は例外
 *   受け口 (PGlite): validateChunk (1 chunk = 1 モール × 1 scope) / applied → same → stale / 失敗の切り分け / 伝票の run と混ざらない (RUN_MISMATCH)
 *   台帳: 種類 (kind) ごとに独立 (鍵・世代・lock・outbox) / D5a の台帳 (outbox が ne_slip_no の形) をそのまま引き継ぐ
 *   通し (送り手 ⇄ 受け口。fetch を差し替え): 範囲 (注文日 2025-01-01 以降 = D-28) / 変更は次の世代 / --force / 範囲指定 / 台帳を失くした / 突合 / 伝票との結び直し
 * 実行: node scripts/test-company-db-orders-push.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildRakutenOrder, yen, rakutenDatetimeToIso, taxRateOf, RAKUTEN_TRANSFORM_VERSION } from '../apps/company-db/push/mall-orders-transform.mjs';
import { pushOrders, reconcileOrdersDaily, diffDailyOrders, relinkShipments, MALL_SPECS } from '../apps/company-db/push/mall-orders.mjs';
import { openLedger, LOCK_KEY } from '../apps/company-db/push/ledger.mjs';
import { summarizePush, fingerprintOf } from '../apps/company-db/push/pipeline.mjs';
import { ingestOrderChunk, validateChunk as validateOrderChunk } from '../apps/company-db/ingest/orders.mjs';
import { ingestShipmentChunk } from '../apps/company-db/ingest/shipments.mjs';
import { buildShipment, TRANSFORM_VERSION as SHIP_TV } from '../apps/company-db/push/ne-shipments-transform.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`); return threw; };
const quiet = () => {};

// ─── SQLite の見本 (warehouse.db の raw_rakuten_orders と同じ列 = apps/warehouse/rakuten-orders.js) ───
function openWarehouse() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_rakuten_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, order_number TEXT NOT NULL, order_date TEXT, order_status INTEGER, goods_price REAL, goods_tax REAL, total_price REAL, request_price REAL,
    postage_price REAL, coupon_shop_price REAL, coupon_all_total_price REAL, item_detail_id INTEGER, item_number TEXT, item_name TEXT, price REAL, price_tax_incl REAL, units INTEGER, tax_rate REAL, selected_choice TEXT, delete_item_flag INTEGER, synced_at TEXT)`);
  return db;
}
const pick = (v, d) => (v === undefined ? d : v);   // null は null のまま (欠落の試験)
const rk = (x) => ({ order_number: x.no, order_date: pick(x.date, '2025-03-01T10:00:00+0900'), order_status: pick(x.status, 600), goods_price: pick(x.goods, 2000), goods_tax: 0, total_price: 2500, request_price: pick(x.request, 2300),
  postage_price: pick(x.postage, 500), coupon_shop_price: pick(x.shopCoupon, 100), coupon_all_total_price: pick(x.allCoupon, 300), item_detail_id: pick(x.detail, 1), item_number: pick(x.item, 'W-001'), item_name: '見本', price: 1000,
  price_tax_incl: pick(x.priceIncl, 1100), units: pick(x.units, 2), tax_rate: pick(x.tax, 0.1), selected_choice: null, delete_item_flag: x.deleted ? 1 : 0, synced_at: pick(x.synced, '2026-09-14 00:00:00') });
const insertRk = (db, r) => db.prepare(`insert into raw_rakuten_orders (order_number, order_date, order_status, goods_price, goods_tax, total_price, request_price, postage_price, coupon_shop_price, coupon_all_total_price, item_detail_id, item_number, item_name, price, price_tax_incl, units, tax_rate, selected_choice, delete_item_flag, synced_at)
  values (@order_number, @order_date, @order_status, @goods_price, @goods_tax, @total_price, @request_price, @postage_price, @coupon_shop_price, @coupon_all_total_price, @item_detail_id, @item_number, @item_name, @price, @price_tax_incl, @units, @tax_rate, @selected_choice, @delete_item_flag, @synced_at)`).run(r);

// ─── PGlite (受け口) ───
const pg = new PGlite();
const pdb = pgliteAdapter(pg);
const applied0 = await applyMigrations(pdb, { log: quiet });
assert.ok(applied0.applied.includes('0016'), '0016 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
await pg.query(`insert into core.products (company_id, name) values (1, '見本')`);
const lst = async (code) => (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', $1, 'active') returning listing_id`, [code])).listing_id;
const alias = (id, value, x = {}) => pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, valid_to) values (1, 'listing', $1, $2, $3, $4, 'map', 'system', $5)`,
  [id, x.system || 'rakuten', x.kind || 'manage_number', value, x.expired ? new Date().toISOString() : null]);
const lstAM = await lst('AM-001'); await alias(lstAM, 'W-001');                       // AM (システム連携用 SKU 番号) が listing_code、W (商品番号) は別名
const lstDirect = await lst('W-003');                                                   // W がそのまま listing_code
const lstDup1 = await lst('W-004'); const lstDup2 = await lst('AM-004'); await alias(lstDup2, 'W-004');   // 直接 + 別名で 2 件に当たる → 決まらない
const lstOld = await lst('AM-005'); await alias(lstOld, 'W-005', { expired: true });   // 失効した別名
const lstY = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'yahoo', 'b-faith01', 'Y-006', 'active') returning listing_id`)).listing_id; await alias(lstY, 'W-006', { system: 'yahoo' });   // 別のモールの出品と別名
const RUN = (n) => `ship_202609140000000_${String(n).padStart(6, '0')}`;
const BASE_URL = 'https://portal.example/apps/company-db/sync';
const H = (x = {}) => ({ source_system: 'mall_api', shop_code: '1', ordered_at: '2025-03-01T01:00:00Z', status_source: '600', total_amount_jpy: 3000, amount_source: 'mall_api', source_updated_at: '2026-09-13T17:00:00Z', transform_version: 'v1', content_hash: 'h1', ...x });
const applyOrder = (mall, scope, no, seq, header, lines = []) => one(`select core.apply_order_batch(1::smallint, $1, $2, $3, $4::bigint, $5::jsonb, $6::jsonb) as r`, [mall, scope, no, seq, JSON.stringify(header), JSON.stringify(lines)]).then((r) => r.r);
const shipBase = (x) => ({ 伝票番号: x.slip, 受注番号: x.orderNo, 店舗コード: x.shop ?? '1', 受注日: '2025-03-01 10:00:00', 出荷確定日: '2025-03-01 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済', キャンセル区分: '有効', 受注キャンセル日: '', 配送方法ID: '28', 配送方法名: 'ネコポス', 送り状番号: '', synced_at: '2026-09-10 00:00:00' });
const applyShipment = async (x, seq = 1) => { const s = buildShipment(shipBase(x), []); return (await one(`select core.apply_shipment_batch(1::smallint, $1, $2::bigint, $3::jsonb, $4::jsonb) as r`, [s.ne_slip_no, seq, JSON.stringify(s.header), JSON.stringify(s.lines)])).r; };
const orderIdOf = async (slip) => (await one(`select order_id from core.shipments where company_id = 1 and ne_slip_no = $1`, [slip])).order_id;

console.log('D5b-1: 0016');
await t('楽天の注文状態 (orderProgress 100〜900) が core.order_status_map にある。知らない値は unknown', async () => {
  const m = async (v) => (await one(`select core.map_order_status('rakuten', $1) as s`, [v])).s;
  assert.deepEqual(await Promise.all(['100', '200', '300', '400', '500', '600', '700', '800', '900', '999'].map(m)), ['new', 'new', 'confirmed', 'on_hold', 'ready', 'shipped', 'delivered', 'cancelled', 'cancelled', 'unknown']);
});
await t('resolve_listing_id: listing_code に当たる / 別名 (external_ids の listing・同じモール・失効していない) でも当たる / 2 件に当たれば null / 失効・別モール・null は null', async () => {
  const rs = async (code) => (await one(`select core.resolve_listing_id(1::smallint, 'rakuten', $1) as id`, [code])).id;
  assert.equal(await rs('AM-001'), lstAM);
  assert.equal(await rs('W-001'), lstAM);
  assert.equal(await rs('W-003'), lstDirect);
  assert.equal(await rs('W-004'), null);
  assert.equal(await rs('W-005'), null);
  assert.equal(await rs('W-006'), null);
  assert.equal(await rs(null), null);
  assert.equal(await rs('nope'), null);
  assert.equal((await one(`select core.resolve_listing_id(1::smallint, 'yahoo', 'W-006') as id`)).id, lstY);   // Yahoo としてなら当たる
});
await t('relink_shipments_bulk: 注文が後から入った伝票を shipment_id の順に集合で結ぶ (楽天はそのまま・Yahoo は接頭辞)。last_id で続きを取る。結べない伝票は残る。p_limit の範囲', async () => {
  assert.equal(await applyShipment({ slip: 'S-RK', orderNo: 'RK-100' }), 'applied');
  assert.equal(await applyShipment({ slip: 'S-YH', orderNo: '12345678', shop: '2' }), 'applied');
  assert.equal(await applyShipment({ slip: 'S-NONE', orderNo: 'RK-NONE' }), 'applied');
  assert.deepEqual([await orderIdOf('S-RK'), await orderIdOf('S-YH'), await orderIdOf('S-NONE')], [null, null, null]);
  assert.equal(await applyOrder('rakuten', 'main', 'RK-100', 1, H()), 'applied');
  await pg.query(`update core.mall_order_policy set orders_enabled = true where company_id = 1 and mall = 'yahoo' and scope_key = 'main'`);   // 接頭辞の試験のためだけ (本番は D-32 で false のまま)
  assert.equal(await applyOrder('yahoo', 'main', 'b-faith01-12345678', 1, H({ shop_code: '2' })), 'applied');
  await pg.query(`update core.mall_order_policy set orders_enabled = false where company_id = 1 and mall = 'yahoo' and scope_key = 'main'`);
  const r1 = await one(`select * from core.relink_shipments_bulk(1::smallint, 0, 20000)`);
  assert.deepEqual([Number(r1.linked), Number(r1.examined)], [2, 3]);
  assert.equal(Number(r1.last_id), Number((await one(`select max(shipment_id) as m from core.shipments`)).m));
  const oRk = (await one(`select order_id from core.orders where mall = 'rakuten' and mall_order_no = 'RK-100'`)).order_id;
  const oYh = (await one(`select order_id from core.orders where mall = 'yahoo' and mall_order_no = 'b-faith01-12345678'`)).order_id;
  assert.deepEqual([await orderIdOf('S-RK'), await orderIdOf('S-YH'), await orderIdOf('S-NONE')], [oRk, oYh, null]);
  const r2 = await one(`select * from core.relink_shipments_bulk(1::smallint, $1, 20000)`, [r1.last_id]);
  assert.deepEqual([Number(r2.linked), Number(r2.examined), r2.last_id], [0, 0, null]);
  const r3 = await one(`select * from core.relink_shipments_bulk(1::smallint, 0, 20000)`);                  // 残り (結べない S-NONE) はまた見る
  assert.deepEqual([Number(r3.linked), Number(r3.examined)], [0, 1]);
  // 1 件ずつ (limit 1) でも続きが取れる
  await pg.query(`update core.shipments set order_id = null where ne_slip_no in ('S-RK', 'S-YH')`);
  let after = 0, linked = 0, calls = 0;
  while (calls < 10) { const r = await one(`select * from core.relink_shipments_bulk(1::smallint, $1, 1)`, [after]); calls++; linked += Number(r.linked); if (!Number(r.examined)) break; after = Number(r.last_id); }
  assert.deepEqual([linked, calls], [2, 4]);
  await rejects(() => pg.query(`select * from core.relink_shipments_bulk(1::smallint, 0, 0)`), /p_limit/);
});

console.log('D5b-1: 整形 (楽天)');
await t('1 注文の整形: 列の対応 / 明細は item_detail_id 順 / 取消明細は cancelled_qty / 税率 / source_updated_at は最大の synced_at', async () => {
  const rows = [rk({ no: 'R1', detail: 2, item: 'W-002', units: 1, priceIncl: 500, tax: 0.08, deleted: true, synced: '2026-09-14 01:00:00' }), rk({ no: 'R1', detail: 1 })];
  const stats = {};
  const r = buildRakutenOrder(rows, { stats });
  assert.equal(r.key, 'rakuten|main|R1');
  assert.deepEqual([r.payload.mall, r.payload.scope_key, r.payload.mall_order_no, r.n_lines, r.no_synced_at, r.source_updated_at], ['rakuten', 'main', 'R1', 2, false, '2026-09-14 01:00:00']);
  const h = r.payload.header;
  assert.deepEqual([h.source_system, h.shop_code, h.ordered_at, h.status_source, h.is_cancelled, h.cancelled_at, h.shipped_at_source], ['mall_api', '1', '2025-03-01T10:00:00+09:00', '600', false, null, null]);
  assert.deepEqual([h.total_amount_jpy, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, h.mall_coupon_jpy, h.points_used_jpy, h.amount_source, h.currency], [2300, 2000, 500, 100, 200, null, 'mall_api', 'JPY']);
  assert.deepEqual([h.source_updated_at, h.transform_version], ['2026-09-14T01:00:00Z', RAKUTEN_TRANSFORM_VERSION]); assert.match(h.content_hash, /^[0-9a-f]{16,}$/);
  assert.deepEqual(r.payload.lines, [
    { line_key: '1', listing_code: 'W-001', sku_code: null, qty: 2, cancelled_qty: 0, unit_price_jpy: 1100, line_amount_jpy: 2200, tax_rate: 0.1, amount_source: 'mall_api', source_line_ref: 'item_detail_id:1' },
    { line_key: '2', listing_code: 'W-002', sku_code: null, qty: 1, cancelled_qty: 1, unit_price_jpy: 500, line_amount_jpy: 500, tax_rate: 0.08, amount_source: 'mall_api', source_line_ref: 'item_detail_id:2' },
  ]);
  assert.deepEqual(stats, {});
});
await t('指紋 (content_hash + 明細) は行の順と synced_at に依らない。金額・状態・明細が変われば変わる', async () => {
  const a = buildRakutenOrder([rk({ no: 'R2', detail: 1 }), rk({ no: 'R2', detail: 2, item: 'W-002' })]);
  const b = buildRakutenOrder([rk({ no: 'R2', detail: 2, item: 'W-002', synced: '2026-09-15 00:00:00' }), rk({ no: 'R2', detail: 1, synced: '2026-09-15 00:00:00' })]);
  assert.equal(a.payload.header.content_hash, b.payload.header.content_hash);
  assert.equal(fingerprintOf(RAKUTEN_TRANSFORM_VERSION, a.payload), fingerprintOf(RAKUTEN_TRANSFORM_VERSION, b.payload));
  assert.notEqual(a.payload.header.source_updated_at, b.payload.header.source_updated_at);
  const c = buildRakutenOrder([rk({ no: 'R2', detail: 1, request: 2400 }), rk({ no: 'R2', detail: 2, item: 'W-002' })]);
  assert.notEqual(fingerprintOf(RAKUTEN_TRANSFORM_VERSION, a.payload), fingerprintOf(RAKUTEN_TRANSFORM_VERSION, c.payload));
  const d = buildRakutenOrder([rk({ no: 'R2', detail: 1 }), rk({ no: 'R2', detail: 2, item: 'W-002', units: 3 })]);
  assert.equal(a.payload.header.content_hash, d.payload.header.content_hash);
  assert.notEqual(fingerprintOf(RAKUTEN_TRANSFORM_VERSION, a.payload), fingerprintOf(RAKUTEN_TRANSFORM_VERSION, d.payload));
});
await t('キャンセル系 (800 / 900) は is_cancelled。状態が無ければ status_source null (Render が unknown にする)', async () => {
  assert.deepEqual([buildRakutenOrder([rk({ no: 'C1', status: 900 })]).payload.header.is_cancelled, buildRakutenOrder([rk({ no: 'C2', status: 800 })]).payload.header.is_cancelled, buildRakutenOrder([rk({ no: 'C3', status: 300 })]).payload.header.is_cancelled], [true, true, false]);
  const h = buildRakutenOrder([rk({ no: 'C4', status: null })]).payload.header;
  assert.deepEqual([h.status_source, h.is_cancelled], [null, false]);
});
await t('金額: 番兵 -9999 と負の値は null (stats に数える) / モール負担クーポン = 全体 − 店負担 (負なら 0・どちらかが無ければ null) / 小数は四捨五入', async () => {
  const stats = {};
  const h = buildRakutenOrder([rk({ no: 'M1', request: -9999, goods: -5, allCoupon: -9999, priceIncl: 1234.6 })], { stats }).payload;
  assert.deepEqual([h.header.total_amount_jpy, h.header.items_amount_jpy, h.header.mall_coupon_jpy, h.header.shop_coupon_jpy, h.lines[0].unit_price_jpy, h.lines[0].line_amount_jpy], [null, null, null, 100, 1235, 2470]);
  assert.deepEqual(stats, { sentinel: 2, negative: 1 });
  assert.equal(buildRakutenOrder([rk({ no: 'M2', shopCoupon: 400, allCoupon: 300 })]).payload.header.mall_coupon_jpy, 0);
  assert.equal(buildRakutenOrder([rk({ no: 'M3', priceIncl: null })]).payload.lines[0].line_amount_jpy, null);
  assert.deepEqual([yen(null), yen(''), yen('12'), yen(-9999), yen(-1), yen(0)], [null, null, 12, null, null, 0]);
  await rejects(async () => yen('abc', null, 'x'), /数でない/);
});
await t('日時と税率: +0900 → +09:00 / Z はそのまま / 時刻帯が無ければ JST / 形が違えば例外。税率は 0.08 か 0.10 だけ', async () => {
  assert.equal(rakutenDatetimeToIso('2025-03-01T10:00:00+0900'), '2025-03-01T10:00:00+09:00');
  assert.equal(rakutenDatetimeToIso('2025-03-01T01:00:00Z'), '2025-03-01T01:00:00Z');
  assert.equal(rakutenDatetimeToIso('2025-03-01 10:00:00'), '2025-03-01T10:00:00+09:00');
  assert.equal(rakutenDatetimeToIso(''), null);
  await rejects(async () => rakutenDatetimeToIso('2025/03/01 10:00'), /形が違う/);
  await rejects(async () => rakutenDatetimeToIso('2025-13-01T10:00:00+0900'), /不正/);
  assert.deepEqual([taxRateOf(0.1), taxRateOf('0.08'), taxRateOf(0.05), taxRateOf(null)], [0.1, 0.08, null, null]);
});
await t('欠落は例外 (0 にしない): 別の注文の行が混ざる / units 無し / item_detail_id 重複 / order_date 無し / synced_at が無ければ fallback (無ければ例外)', async () => {
  await rejects(async () => buildRakutenOrder([rk({ no: 'E1' }), rk({ no: 'E2' })]), /別の注文/);
  await rejects(async () => buildRakutenOrder([rk({ no: 'E1', units: null })]), /units が無い/);
  await rejects(async () => buildRakutenOrder([rk({ no: 'E1', detail: 1 }), rk({ no: 'E1', detail: 1 })]), /重複/);
  await rejects(async () => buildRakutenOrder([rk({ no: 'E1', date: null })]), /order_date が無い/);
  await rejects(async () => buildRakutenOrder([rk({ no: 'E1', synced: null })]), /synced_at が 1 つも無い/);
  const r = buildRakutenOrder([rk({ no: 'E1', synced: null })], { fallbackSourceUpdatedAt: '2026-09-14T09:00:00.000Z' });
  assert.deepEqual([r.no_synced_at, r.payload.header.source_updated_at], [true, '2026-09-14T09:00:00.000Z']);
  await rejects(async () => buildRakutenOrder([]), /行が無い/);
});

console.log('D5b-1: 受け口 (PGlite)');
const payloadOf = (x) => buildRakutenOrder(Array.isArray(x) ? x : [rk(x)]).payload;
const chunkOf = (n, seq, index, last, rows, x = {}) => ({ runId: RUN(n), batchSeq: seq, chunkIndex: index, last, transformVersion: RAKUTEN_TRANSFORM_VERSION, rows, log: quiet, ...x });
const bodyOf = (n, seq, index, last, rows, x = {}) => ({ run_id: RUN(n), batch_seq: seq, chunk_index: index, last, transform_version: RAKUTEN_TRANSFORM_VERSION, rows, ...x });
await t('validateChunk: 1 chunk は 1 モール × 1 scope / 知らないモール / 注文番号の形 / 鍵 = mall|scope|注文番号', async () => {
  const v = validateOrderChunk(bodyOf(1, 1, 0, true, [payloadOf({ no: 'V1' }), payloadOf({ no: 'V2' })]));
  assert.deepEqual([v.mall, v.scope, v.rows.map((r) => r.key), v.rows[0].mall_order_no], ['rakuten', 'main', ['rakuten|main|V1', 'rakuten|main|V2'], 'V1']);
  await rejects(async () => validateOrderChunk(bodyOf(1, 1, 0, true, [{ ...payloadOf({ no: 'V1' }), mall: 'ebay' }])), /not a known mall/);
  await rejects(async () => validateOrderChunk(bodyOf(1, 1, 0, true, [payloadOf({ no: 'V1' }), { ...payloadOf({ no: 'V2' }), mall: 'yahoo' }])), /one chunk must hold one mall/);
  await rejects(async () => validateOrderChunk(bodyOf(1, 1, 0, true, [payloadOf({ no: 'V1' }), { ...payloadOf({ no: 'V2' }), scope_key: 'sub' }])), /one chunk must hold one mall/);
  await rejects(async () => validateOrderChunk(bodyOf(1, 1, 0, true, [{ ...payloadOf({ no: 'V1' }), mall_order_no: 'a b' }])), /mall_order_no has a bad form/);
  await rejects(async () => validateOrderChunk(bodyOf(1, 1, 0, true, [payloadOf({ no: 'V1' }), payloadOf({ no: 'V1' })])), /duplicate key/);
  await rejects(async () => validateOrderChunk(bodyOf(1, 1, 0, true, [payloadOf({ no: 'V1' })], { transform_version: 'other' })), /transform_version differs/);
  const e = validateOrderChunk(bodyOf(1, 1, 0, true, []));
  assert.deepEqual([e.mall, e.scope, e.rows.length], [null, null, 0]);
});
await t('1 chunk = 1 取引: applied (状態は対応表・出品は別名でも解決・当たらなければ unresolved_code) → same → stale。run は source_system = モール / entity = orders', async () => {
  const rows = [payloadOf([rk({ no: 'P1', detail: 1, item: 'W-001' }), rk({ no: 'P1', detail: 2, item: 'W-004' })]), payloadOf({ no: 'P2', status: 900 })];
  const chunk = validateOrderChunk(bodyOf(2, 5, 0, true, rows));
  const r1 = await ingestOrderChunk(pdb, { ...chunk, log: quiet });
  assert.deepEqual([r1.applied, r1.same, r1.stale, r1.failed, r1.stale_keys, r1.finished, r1.replay], [2, 0, 0, [], [], true, false]);
  const o = await one(`select order_id, status, status_source, is_cancelled, order_date_jst::text as d, received_batch_seq, total_amount_jpy from core.orders where mall = 'rakuten' and scope_key = 'main' and mall_order_no = 'P1'`);
  assert.deepEqual([o.status, o.status_source, o.is_cancelled, o.d, Number(o.received_batch_seq), Number(o.total_amount_jpy)], ['shipped', '600', false, '2025-03-01', 5, 2300]);
  const ls = (await pg.query(`select line_key, listing_id, sku_id, unresolved_code, qty from core.order_lines where order_id = $1 order by line_key`, [o.order_id])).rows;
  assert.deepEqual(ls.map((l) => [l.line_key, l.listing_id, l.sku_id, l.unresolved_code, l.qty]), [['1', lstAM, null, null, 2], ['2', null, null, 'W-004', 2]]);
  assert.deepEqual(await one(`select status, is_cancelled from core.orders where mall_order_no = 'P2'`), { status: 'cancelled', is_cancelled: true });
  assert.deepEqual(await one(`select source_system, entity, scope_key, status, pages, checksum, format_version from ops.ingest_runs where ingest_run_id = $1`, [RUN(2)]), { source_system: 'rakuten', entity: 'orders', scope_key: 'main', status: 'success', pages: 1, checksum: '5', format_version: RAKUTEN_TRANSFORM_VERSION });
  const r2 = await ingestOrderChunk(pdb, { ...validateOrderChunk(bodyOf(3, 6, 0, true, rows)), log: quiet });
  assert.deepEqual([r2.applied, r2.same, r2.stale], [0, 2, 0]);
  const r3 = await ingestOrderChunk(pdb, { ...validateOrderChunk(bodyOf(4, 4, 0, true, rows)), log: quiet });
  assert.deepEqual([r3.applied, r3.same, r3.stale, r3.stale_keys], [0, 0, 2, ['rakuten|main|P1', 'rakuten|main|P2']]);
});
await t('失敗した注文だけ切り分ける (savepoint)。failed には mall_order_no。run は partial で "orders failed"。伝票の run と同じ run_id は RUN_MISMATCH', async () => {
  const bad = payloadOf({ no: 'Q2' }); bad.header = { ...bad.header, ordered_at: null };
  const rows = [payloadOf({ no: 'Q1' }), bad, payloadOf({ no: 'Q3' })];
  const r = await ingestOrderChunk(pdb, { ...validateOrderChunk(bodyOf(5, 7, 0, true, rows)), log: quiet });
  assert.deepEqual([r.applied, r.failed.length, r.failed[0].key, r.failed[0].mall_order_no, r.finished], [2, 1, 'rakuten|main|Q2', 'Q2', true]);
  const run = await one(`select status, error, failed_ranges from ops.ingest_runs where ingest_run_id = $1`, [RUN(5)]);
  assert.equal(run.status, 'partial'); assert.match(run.error, /1 orders failed/); assert.deepEqual(run.failed_ranges.map((f) => f.mall_order_no), ['Q2']);
  const s = buildShipment(shipBase({ slip: 'S-MIX', orderNo: 'MIX' }), []);
  await ingestShipmentChunk(pdb, { runId: RUN(6), batchSeq: 7, chunkIndex: 0, last: false, transformVersion: SHIP_TV, rows: [{ ne_slip_no: s.ne_slip_no, header: s.header, lines: s.lines }], log: quiet });
  const e = await rejects(() => ingestOrderChunk(pdb, { ...validateOrderChunk(bodyOf(6, 7, 1, true, [payloadOf({ no: 'Q4' })])), log: quiet }), /\(ne\/shipments\), not 7 \/ rakuten-orders-1 \(rakuten\/orders\)/);
  assert.equal(e.code, 'RUN_MISMATCH');
});

console.log('D5b-1: 台帳 (種類ごと)');
await t('同じ台帳ファイルでも種類 (shipment / order:rakuten) ごとに 鍵・世代・lock・outbox・run が独立', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-ledger-'));
  const ls = openLedger(dir, { kind: 'shipment' }), lo = openLedger(dir, { kind: 'order:rakuten' });
  try {
    assert.equal(lo.trackKeys(['rakuten|main|X', 'rakuten|main|Y']), 2);
    assert.deepEqual([lo.countTracked(), ls.countTracked(), lo.countConfirmed()], [2, 0, 0]);
    lo.markSent([{ key: 'rakuten|main|X', fp: 'f1' }], 3);
    assert.deepEqual([lo.countConfirmed(), [...lo.loadFingerprints().entries()], [...ls.loadFingerprints().entries()]], [1, [['rakuten|main|X', 'f1'], ['rakuten|main|Y', '']], []]);
    assert.deepEqual([lo.nextBatchSeq(), lo.nextBatchSeq(), ls.currentBatchSeq(), ls.nextBatchSeq()], [1, 2, 0, 1]);
    assert.equal(lo.acquireLock({ owner: 'o1', pid: process.pid }).ok, true);
    assert.equal(ls.acquireLock({ owner: 's1', pid: process.pid }).ok, true);
    assert.equal(lo.acquireLock({ owner: 'o2', pid: process.pid }).ok, false);
    assert.equal(JSON.parse(lo.getMeta(LOCK_KEY)).owner, 'o1'); assert.equal(JSON.parse(ls.getMeta(LOCK_KEY)).owner, 's1');
    lo.pushOutbox('r1', [{ key: 'rakuten|main|X', fp: 'f2', payload: '{}', n_lines: 0, n_bytes: 2 }]);
    assert.deepEqual([lo.countOutbox('r1').n, ls.countOutbox('r1').n, lo.outboxKeys(), ls.outboxKeys()], [1, 0, ['rakuten|main|X'], []]);
    assert.deepEqual(lo.takeOutbox('r1', 0, { maxRows: 10, maxLines: 10, maxBytes: 100 }).map((x) => [x.key, x.ne_slip_no]), [['rakuten|main|X', 'rakuten|main|X']]);
    lo.markInitialized(); assert.deepEqual([lo.isInitialized(), ls.isInitialized()], [true, false]);
    lo.recordRun({ run_id: 'r1', mode: 'incremental', started_at: '2026-09-14 00:00:00' });
    assert.deepEqual([lo.lastRuns(5).map((r) => r.kind), ls.lastRuns(5)], [['order:rakuten'], []]);
    assert.deepEqual(lo.carryOverOutbox('o1'), { leftover: 1, carried: 0, cleared: 1 });
    assert.equal(lo.resetFingerprints(), 2); assert.deepEqual([lo.countConfirmed(), lo.countTracked()], [0, 2]);
    assert.deepEqual([lo.releaseLock('o1'), ls.releaseLock('s1'), lo.getMeta(LOCK_KEY), ls.getMeta(LOCK_KEY)], [true, true, null, null]);
  } finally { ls.close(); lo.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});
await t('D5a の台帳 (outbox が ne_slip_no の形・runs に kind が無い) をそのまま引き継ぐ (送付済み・世代・残った outbox・lock は伝票の種類のもの)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-ledger-'));
  const raw = new Database(path.join(dir, 'company-db-push.db'));
  raw.exec(`create table shipments_sent (ne_slip_no text primary key, fp text not null, batch_seq integer not null, sent_at text not null);
    create table outbox (seq integer primary key autoincrement, run_id text not null, ne_slip_no text not null unique, fp text not null, payload text not null, n_lines integer not null, n_bytes integer not null);
    create table meta (key text primary key, value text, updated_at text);
    create table runs (run_id text primary key, mode text not null, started_at text not null, finished_at text, batch_seq integer, scanned integer, in_scope integer, changed integer, sent integer, applied integer, same integer, stale integer, failed integer, transform_errors integer, ok integer, note text);
    insert into shipments_sent values ('OLD1', 'fp1', 40, '2026-09-14 00:00:00'), ('OLD2', '', 0, '2026-09-14 00:00:00');
    insert into outbox (run_id, ne_slip_no, fp, payload, n_lines, n_bytes) values ('r-old', 'LEFT', 'fp9', '{"ne_slip_no":"LEFT"}', 1, 21);
    insert into meta values ('batch_seq', '40', '2026-09-14 00:00:00'), ('initialized', '1', '2026-09-14 00:00:00'), ('last_receipt', '{"run_id":"r-old","chunk_index":3,"payload_checksum":"abc"}', '2026-09-14 00:00:00');
    insert into runs (run_id, mode, started_at, ok) values ('r-old', 'incremental', '2026-09-14 00:00:00', 1);`);
  raw.close();
  const ls = openLedger(dir, { kind: 'shipment' }), lo = openLedger(dir, { kind: 'order:rakuten' });
  try {
    assert.deepEqual([ls.countTracked(), ls.countConfirmed(), ls.currentBatchSeq(), ls.isInitialized(), ls.getLastReceipt()], [2, 1, 40, true, { run_id: 'r-old', chunk_index: 3, payload_checksum: 'abc' }]);
    assert.deepEqual([ls.outboxKeys(), ls.outboxSlips(), ls.countOutbox('r-old').n], [['LEFT'], ['LEFT'], 1]);
    assert.deepEqual(ls.takeOutbox('r-old', 0, { maxRows: 10, maxLines: 10, maxBytes: 100 }).map((x) => [x.key, x.ne_slip_no, x.fp]), [['LEFT', 'LEFT', 'fp9']]);
    assert.deepEqual(ls.lastRuns(5).map((r) => [r.run_id, r.kind, r.ok]), [['r-old', 'shipment', 1]]);
    assert.deepEqual([lo.countTracked(), lo.currentBatchSeq(), lo.isInitialized(), lo.getLastReceipt(), lo.outboxKeys(), lo.lastRuns(5)], [0, 0, false, null, [], []]);
    assert.equal(ls.trackSlips(['OLD3']), 1); assert.equal(ls.countTracked(), 3);
  } finally { ls.close(); lo.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

console.log('D5b-1: 通し (送り手 ⇄ 受け口)');
/** 送り手が叩く fetch の代わり: 受け口の関数を直接呼ぶ (router.mjs と同じ SQL) */
function fakeFetch(opts = {}) {
  const calls = [];
  const f = async (url, init = {}) => {
    calls.push({ url, init });
    const u = new URL(url);
    if (opts.before) { const r = await opts.before(calls.length, url, init); if (r) return r; }
    assert.equal(init.headers['x-sync-key'], 'k');
    if (init.method === 'POST' && u.pathname.endsWith('/orders')) {
      let chunk; try { chunk = validateOrderChunk(JSON.parse(init.body)); } catch (e) { return new Response(JSON.stringify({ error: e.message }), { status: 400 }); }
      try {
        const r = await ingestOrderChunk(opts.db || pdb, { ...chunk, log: quiet });
        return new Response(JSON.stringify(r), { status: 200 });
      } catch (e) {
        const status = e.code === 'CHUNK_DEADLINE' ? 503 : (e.code === 'RUN_MISMATCH' || e.code === 'CHUNK_MISMATCH' || e.code === 'RUN_CLOSED') ? 409 : 500;
        return new Response(JSON.stringify({ error: e.message, code: e.code || null }), { status });
      }
    }
    const mall = u.searchParams.get('mall'), scope = u.searchParams.get('scope');
    if (u.pathname.endsWith('/orders/status')) {
      const c = await one(`select count(*)::int as orders, max(received_batch_seq) as max_batch_seq from core.orders where company_id = 1 and mall = $1 and scope_key = $2`, [mall, scope]);
      return new Response(JSON.stringify({ mall, scope, counts: { orders: c.orders, max_batch_seq: c.max_batch_seq == null ? null : Number(c.max_batch_seq), ...(opts.status || {}) } }), { status: 200 });
    }
    if (u.pathname.endsWith('/orders/receipt')) {
      if (opts.receiptFound === false) return new Response(JSON.stringify({ found: false, payload_checksum: null }), { status: 200 });
      const r = await one(`select payload_checksum from ops.ingest_chunks where ingest_run_id = $1 and chunk_index = $2`, [u.searchParams.get('run_id'), Number(u.searchParams.get('chunk_index'))]);
      return new Response(JSON.stringify({ found: !!r, payload_checksum: r ? r.payload_checksum : null }), { status: 200 });
    }
    if (u.pathname.endsWith('/orders/keys')) {
      const limit = Math.min(Number(u.searchParams.get('limit')) || 20000, opts.keysLimit || 50000);
      const rows = (await pg.query(`select mall_order_no from core.orders where company_id = 1 and mall = $1 and scope_key = $2 and mall_order_no > $3 order by mall_order_no limit $4`, [mall, scope, u.searchParams.get('after') || '', limit])).rows.map((x) => x.mall_order_no);
      return new Response(JSON.stringify({ keys: rows, next: rows.length === limit ? rows[rows.length - 1] : null }), { status: 200 });
    }
    if (u.pathname.endsWith('/orders/daily')) {
      const rows = (await pg.query(`select o.order_date_jst::text as order_date, count(*)::int as orders,
          coalesce(sum((select count(*) from core.order_lines l where l.order_id = o.order_id and l.removed_at is null)), 0)::int as lines,
          coalesce(sum(coalesce(o.items_amount_jpy, 0)), 0)::bigint as items_amount_jpy, (count(*) filter (where o.is_cancelled))::int as cancelled
         from core.orders o where o.company_id = 1 and o.mall = $1 and o.scope_key = $2 and o.order_date_jst between $3::date and $4::date group by o.order_date_jst order by o.order_date_jst`, [mall, scope, u.searchParams.get('from'), u.searchParams.get('to')])).rows;
      return new Response(JSON.stringify({ rows: rows.map((r) => ({ ...r, items_amount_jpy: Number(r.items_amount_jpy) })) }), { status: 200 });
    }
    if (init.method === 'POST' && u.pathname.endsWith('/shipments/relink')) {
      const b = JSON.parse(init.body);
      const r = await one(`select linked, examined, last_id from core.relink_shipments_bulk(1::smallint, $1::bigint, $2::int)`, [Number(b.after) || 0, Number(b.limit) || 20000]);
      return new Response(JSON.stringify({ linked: Number(r.linked), examined: Number(r.examined), last_id: r.last_id == null ? null : Number(r.last_id) }), { status: 200 });
    }
    return new Response('not found', { status: 404 });
  };
  f.calls = calls;
  f.posts = () => calls.filter((c) => c.init.method === 'POST' && c.url.endsWith('/orders')).map((c) => JSON.parse(c.init.body));
  return f;
}
const push = (w, l, f, x = {}) => pushOrders({ mall: 'rakuten', warehouse: w, ledger: l, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });
const newLedger = ({ initialized = true } = {}) => { const l = openLedger(null, { memory: true, kind: 'order:rakuten' }); if (initialized) l.markInitialized(); return l; };
const remoteMax = () => num(`select coalesce(max(received_batch_seq), 0) as n from core.orders where company_id = 1 and mall = 'rakuten' and scope_key = 'main'`);
const W = openWarehouse();   // 通しの試験で共有 (台帳を失くした・突合・結び直しが同じ raw を見る)
await t('初回は範囲 (注文日 2025-01-01 以降) を chunk に分けて送る → 台帳 (order:rakuten) に指紋 → 2 回目は変化なし → 明細の変更は次の世代 (synced_at が古くても) → --force。番兵は数える', async () => {
  const l = newLedger(), f = fakeFetch();
  insertRk(W, rk({ no: 'R-A', detail: 1, item: 'W-001', date: '2025-04-01T10:00:00+0900' })); insertRk(W, rk({ no: 'R-A', detail: 2, item: 'W-004', units: 1, date: '2025-04-01T10:00:00+0900' }));
  insertRk(W, rk({ no: 'R-B', date: '2025-04-02T09:00:00+0900' }));
  insertRk(W, rk({ no: 'R-C', date: '2025-04-02T23:59:59+0900', status: 900 }));
  insertRk(W, rk({ no: 'R-EDGE', date: '2025-01-01T00:00:00+0900' }));                     // 境目 (入る)
  insertRk(W, rk({ no: 'R-OLD', date: '2024-12-31T23:59:59+0900' }));                      // 範囲外
  insertRk(W, rk({ no: 'R-S', date: '2025-04-03T10:00:00+0900', request: -9999 }));        // 番兵 → total null
  const dry = await push(W, l, f, { dryRun: true });
  assert.deepEqual([dry.scanned, dry.inScope, dry.changed, dry.sent, dry.example.mall_order_no, dry.stats, f.calls.length, l.countTracked()], [6, 5, 5, 0, 'R-A', { sentinel: 1, negative: 0 }, 0, 0]);
  assert.match(summarizePush(dry, '楽天の注文'), /^dry-run: 読んだ 6 \/ 範囲 5 \/ 変わった 5/);
  const before = await remoteMax();
  const r1 = await push(W, l, f, { chunkSize: 2 });
  assert.deepEqual([r1.ok, r1.kind, r1.inScope, r1.changed, r1.sent, r1.applied, r1.same, r1.chunks, r1.batchSeq, l.countTracked(), l.countConfirmed(), l.countOutbox(r1.runId).n], [true, 'order:rakuten', 5, 5, 5, 5, 0, 3, before + 1, 5, 5, 0]);
  assert.deepEqual(f.posts().map((p) => [p.chunk_index, p.last, p.rows.length, p.rows[0].mall, p.rows[0].scope_key]), [[0, false, 2, 'rakuten', 'main'], [1, false, 2, 'rakuten', 'main'], [2, true, 1, 'rakuten', 'main']]);
  assert.deepEqual(l.getLastReceipt(), { run_id: r1.runId, chunk_index: 2, payload_checksum: (await one(`select payload_checksum from ops.ingest_chunks where ingest_run_id = $1 and chunk_index = 2`, [r1.runId])).payload_checksum });
  assert.deepEqual(await one(`select source_system, entity, scope_key, status, pages from ops.ingest_runs where ingest_run_id = $1`, [r1.runId]), { source_system: 'rakuten', entity: 'orders', scope_key: 'main', status: 'success', pages: 3 });
  assert.equal(await num(`select count(*) as n from core.orders where mall = 'rakuten' and mall_order_no in ('R-A','R-B','R-C','R-EDGE','R-S')`), 5);
  assert.equal(await num(`select count(*) as n from core.orders where mall_order_no = 'R-OLD'`), 0);
  assert.deepEqual(await one(`select status, is_cancelled, total_amount_jpy from core.orders where mall_order_no = 'R-S'`), { status: 'shipped', is_cancelled: false, total_amount_jpy: null });
  assert.deepEqual((await pg.query(`select l.line_key, l.listing_id, l.unresolved_code from core.order_lines l join core.orders o on o.order_id = l.order_id where o.mall_order_no = 'R-A' order by l.line_key`)).rows.map((x) => [x.line_key, x.listing_id, x.unresolved_code]), [['1', lstAM, null], ['2', null, 'W-004']]);
  assert.match(summarizePush(r1, '楽天の注文'), /^✅ Company DB 楽天の注文 push: 変わった 5 件を送った \(applied 5 \/ same 0 \/ stale 0 \/ failed 0 \/ 整形できない 0\) 世代 \d+ chunk 3 \/ 範囲 5 件のうち変化なし 0$/);
  assert.deepEqual(l.lastRuns(1).map((x) => [x.run_id, x.kind, x.sent, x.applied, x.ok]), [[r1.runId, 'order:rakuten', 5, 5, 1]]);
  const r2 = await push(W, l, f, {});
  assert.deepEqual([r2.ok, r2.changed, r2.unchanged, r2.sent, r2.chunks, r2.batchSeq, l.currentBatchSeq(), l.getMeta(LOCK_KEY)], [true, 0, 5, 0, 0, null, before + 1, null]);
  W.prepare(`update raw_rakuten_orders set units = 5, synced_at = '2020-01-01 00:00:00' where order_number = 'R-B'`).run();
  const r3 = await push(W, l, f, {});
  assert.deepEqual([r3.ok, r3.changed, r3.applied, r3.batchSeq], [true, 1, 1, before + 2]);
  assert.equal((await one(`select l.qty from core.order_lines l join core.orders o on o.order_id = l.order_id where o.mall_order_no = 'R-B' and l.line_key = '1'`)).qty, 5);
  const r4 = await push(W, l, f, { force: true });
  assert.deepEqual([r4.ok, r4.changed, r4.same, r4.applied], [true, 5, 5, 0]);
  l.close();
});
await t('注文日の範囲 (--from/--to) は台帳に書くので、その後の incremental は範囲外でも追跡する (取消の訂正が届く)', async () => {
  const l = newLedger(), f = fakeFetch();
  const r1 = await push(W, l, f, { from: '2024-12-01', to: '2024-12-31' });
  assert.deepEqual([r1.ok, r1.mode, r1.inScope, r1.changed, r1.applied], [true, 'range', 1, 1, 1]);
  assert.equal(await num(`select count(*) as n from core.orders where mall_order_no = 'R-OLD'`), 1);
  W.prepare(`update raw_rakuten_orders set order_status = 900 where order_number = 'R-OLD'`).run();
  const r2 = await push(W, l, f, {});
  assert.deepEqual([r2.ok, r2.inScope, r2.changed, r2.applied, r2.same], [true, 6, 6, 1, 5]);   // 台帳が新しいので 2025 の 5 件も送る (内容が同じ = same)。R-OLD は範囲外だが追跡中 → 取消が届く
  assert.equal((await one(`select is_cancelled from core.orders where mall_order_no = 'R-OLD'`)).is_cancelled, true);
  l.close();
});
await t('台帳を失くした: 空の台帳 + Render に注文 → Render から注文番号を取り戻して追跡し、全部送り直す (same)', async () => {
  const l = newLedger({ initialized: false }), f = fakeFetch({ keysLimit: 4 });
  const n = await num(`select count(*) as n from core.orders where mall = 'rakuten' and scope_key = 'main'`);
  const r = await push(W, l, f, {});
  assert.deepEqual([r.ok, r.ledgerRebuilt, l.countTracked() >= n, r.applied, r.same], [true, n, true, 0, 6]);
  assert.equal(f.calls.filter((c) => c.url.includes('/orders/keys')).length, Math.ceil(n / 4) + (n % 4 === 0 ? 1 : 0));
  assert.match(summarizePush(r, '楽天の注文'), new RegExp(`台帳が空だったので Render から ${n} 件を取り戻した`));
  l.close();
});
await t('突合: 注文日ごとの 注文数 / 明細数 / 商品代 / 取消 が raw と Render で一致する。raw で取消にすると差が出て、送れば戻る', async () => {
  const l = newLedger(), f = fakeFetch();
  await push(W, l, f, {});
  const r0 = await reconcileOrdersDaily({ mall: 'rakuten', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2024-12-01', to: '2025-01-31', log: quiet });
  assert.deepEqual([r0.ok, r0.localOrders, r0.remoteOrders, r0.matched, r0.windows.length], [true, 2, 2, 2, 1]);
  const rr = await reconcileOrdersDaily({ mall: 'rakuten', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-04-01', to: '2025-04-30', log: quiet });
  assert.deepEqual([rr.ok, rr.localOrders, rr.remoteOrders, rr.matched, rr.windows.length], [true, 4, 4, 3, 1]);
  W.prepare(`update raw_rakuten_orders set order_status = 900 where order_number = 'R-B'`).run();
  const bad = await reconcileOrdersDaily({ mall: 'rakuten', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-04-01', to: '2025-04-30', log: quiet });
  assert.deepEqual([bad.ok, bad.mismatched.map((m) => [m.key, m.diffs])], [false, [['2025-04-02', ['cancelled 2≠1']]]]);
  await push(W, l, f, {});
  assert.equal((await reconcileOrdersDaily({ mall: 'rakuten', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-04-01', to: '2025-04-30', log: quiet })).ok, true);
  assert.deepEqual(diffDailyOrders([{ order_date: '2025-01-01', orders: 1, lines: 2, items_amount_jpy: 100, cancelled: 0 }, { order_date: '2025-01-02', orders: 1, lines: 1, items_amount_jpy: 5, cancelled: 0 }],
    [{ order_date: '2025-01-01', orders: 1, lines: 2, items_amount_jpy: '100', cancelled: 0 }, { order_date: '2025-01-03', orders: 2, lines: 2, items_amount_jpy: 9, cancelled: 1 }]),
    { compared: 3, matched: 1, mismatched: [], onlyLocal: [{ order_date: '2025-01-02', orders: 1, lines: 1, items_amount_jpy: 5, cancelled: 0 }], onlyRemote: [{ order_date: '2025-01-03', orders: 2, lines: 2, items_amount_jpy: 9, cancelled: 1 }] });
  l.close();
});
await t('伝票との結び直し: 注文より先に届いた伝票 (order_id null) が、注文を送った後の relink で結ばれる', async () => {
  assert.equal(await applyShipment({ slip: 'S-LATE', orderNo: 'R-LATE' }, 2), 'applied');
  assert.equal(await orderIdOf('S-LATE'), null);
  const l = newLedger(), f = fakeFetch();
  insertRk(W, rk({ no: 'R-LATE', date: '2025-04-05T10:00:00+0900' }));
  const r = await push(W, l, f, {});
  assert.deepEqual([r.ok, r.applied], [true, 1]);
  const rl = await relinkShipments({ fetchImpl: f, base: BASE_URL, syncKey: 'k', limit: 2, log: quiet });
  assert.ok(rl.linked >= 1 && rl.examined >= 2 && rl.calls >= 2, JSON.stringify(rl));
  assert.equal(await orderIdOf('S-LATE'), (await one(`select order_id from core.orders where mall = 'rakuten' and mall_order_no = 'R-LATE'`)).order_id);
  assert.equal(await orderIdOf('S-NONE'), null);
  l.close();
});
await t('MALL_SPECS.rakuten: 注文番号順の流し読みで明細がそろう / dateOf は注文日 / 知らないモールは例外', async () => {
  const groups = [...MALL_SPECS.rakuten.iterate(W)];
  const a = groups.find((g) => g.no === 'R-A');
  assert.deepEqual([a.key, a.rows.length, MALL_SPECS.rakuten.dateOf(a), groups.map((g) => g.no)], ['rakuten|main|R-A', 2, '2025-04-01', ['R-A', 'R-B', 'R-C', 'R-EDGE', 'R-LATE', 'R-OLD', 'R-S']]);
  await rejects(() => pushOrders({ mall: 'ebay', warehouse: W, ledger: null }), /知らないモール/);
});
W.close();

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
