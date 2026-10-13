#!/usr/bin/env node
/**
 * test-company-db-orders-push-mercari.mjs — メルカリ shops の注文の push (NE の受注伝票 = raw_ne_order_base + raw_ne_orders の店舗 8 → core.orders) の試験。2026-10-13
 *   整形 (純粋関数): 注文 = 明細の受注番号でまとめる (同梱) / 分割 / 取消 (伝票・明細・全部) / 数量 0 / 金額は null / 読めない値は例外 / 指紋
 *   通し (送り手 ⇄ 本物の router を HTTP で・PGlite): SKU の解決・NE 店舗 8 の伝票との結び・差分・突合 (dailyLocal)・取消・年またぎ (D-28)・範囲指定・読めない値は 3 mode で ❌・売上日次は作らない
 *   daily-sync / retry: NE の出荷 push の直後の 1 段・retry の 3 か所
 * ほかのモールと共通の部分 (台帳・chunk・再送・lock・結び直しの持ち越し・完了印の CLI) は test-company-db-orders-push.mjs / -amazon.mjs が見ている。
 * 🚨 HTTP の先 (Render) と本番の raw は試験に無い → 初回は --dry-run → 送る → --reconcile
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { buildMercariOrder, groupMercariOrders, isNeJst, isMercariOrderNo, MERCARI_TRANSFORM_VERSION, MERCARI_BASE_COLUMNS, MERCARI_LINE_COLUMNS } from '../apps/company-db/push/mall-orders-transform.mjs';
import { pushOrders, reconcileOrdersDaily, refreshSalesDaily, MALL_SPECS } from '../apps/company-db/push/mall-orders.mjs';
import { openLedger } from '../apps/company-db/push/ledger.mjs';
import { fingerprintOf } from '../apps/company-db/push/pipeline.mjs';
import { buildShipment } from '../apps/company-db/push/ne-shipments-transform.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';
import { JOB_DEFINITIONS, RETRY_ORDER, UPSTREAM_OF } from '../apps/warehouse/retry-failed-jobs.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };
const quiet = () => {};
const pick = (v, d) => (v === undefined ? d : v);

// ─── SQLite の見本 (warehouse.db の raw_ne_order_base / raw_ne_orders と同じ列 = apps/warehouse/db.js) ───
function openWarehouse() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE raw_ne_orders (伝票番号 TEXT, 受注番号 TEXT, 受注状態区分 TEXT, 受注状態 TEXT, 受注キャンセル TEXT, 受注キャンセル日 TEXT, 受注日 TEXT, 店舗コード TEXT, 出荷確定日 TEXT,
    明細行番号 INTEGER, レコードナンバー TEXT, キャンセル区分 TEXT, 商品コード TEXT, 商品名 TEXT, 商品OP TEXT, 受注数 INTEGER, 引当数 INTEGER, 小計金額 REAL, synced_at TEXT, PRIMARY KEY (伝票番号, 明細行番号))`);
  db.exec(`CREATE TABLE raw_ne_order_base (伝票番号 TEXT PRIMARY KEY, 受注番号 TEXT, 店舗コード TEXT, 受注日 TEXT, 出荷確定日 TEXT, 受注状態区分 TEXT, 受注状態 TEXT, キャンセル区分 TEXT, 受注キャンセル日 TEXT,
    配送方法ID TEXT, 配送方法名 TEXT, 送り状番号 TEXT, synced_at TEXT)`);
  return db;
}
// 実データの形 (2026-10-13 に miniPC で見た行): 注文番号 22 文字・日時 'YYYY-MM-DD HH:MM:SS' (JST)・状態 50 = 出荷確定済（完了）・キャンセル区分 '有効' / 'キャンセル'・synced_at は UTC
const nb = (x) => ({ 伝票番号: pick(x.slip, '1586057'), 受注番号: pick(x.no, '2JY4WJifUnmFpJm5GiXSSX'), 店舗コード: pick(x.shop, '8'), 受注日: pick(x.date, '2026-10-12 23:59:48'),
  出荷確定日: pick(x.ship, '2026-10-13 15:00:00'), 受注状態区分: pick(x.st, '50'), 受注状態: '出荷確定済（完了）', キャンセル区分: pick(x.c, '有効'), 受注キャンセル日: pick(x.cdate, ''),
  配送方法ID: '28', 配送方法名: 'ネコポス', 送り状番号: '123456789012', synced_at: pick(x.synced, '2026-10-12 22:01:06') });
const nl = (x) => ({ 伝票番号: pick(x.slip, '1586057'), 受注番号: pick(x.no, '2JY4WJifUnmFpJm5GiXSSX'), 受注状態区分: pick(x.st, '50'), 受注状態: '出荷確定済（完了）', 受注キャンセル: '有効な受注です。',
  受注キャンセル日: '', 受注日: pick(x.date, '2026-10-12 23:59:48'), 店舗コード: pick(x.shop, '8'), 出荷確定日: '', 明細行番号: pick(x.ln, 1), レコードナンバー: '', キャンセル区分: pick(x.c, '有効'),
  商品コード: pick(x.code, 'as-ki5'), 商品名: '商品名', 商品OP: '', 受注数: pick(x.qty, 1), 引当数: 1, 小計金額: pick(x.amt, 548), synced_at: pick(x.synced, '2026-10-12 22:01:06') });
const only = (cols, r) => Object.fromEntries(cols.map((c) => [c, r[c]]));
const B = (x) => only(MERCARI_BASE_COLUMNS, nb(x)), Ln = (x) => only(MERCARI_LINE_COLUMNS, nl(x));
const groupsOf = (bases, lines) => groupMercariOrders(bases.map(B), lines.map(Ln));
const one1 = (bases, lines, no) => { const g = groupsOf(bases, lines).find((x) => x.no === no); assert.ok(g, `注文 ${no} が無い`); return g; };

const NO_A = '2JP9LufDpeoQRxLK4KVadP', NO_B = '2JP9J3BBNTLsuTgp7iMuVS', NO_C = '2JPDd4AdPhnkodNuKuQbpa';

console.log('メルカリ: 整形');
await t('1 伝票 1 明細: 鍵 = NE の受注番号 (メルカリの注文番号)・source_system ne・店舗 8・状態 = 受注状態区分・金額は全部 null (amount_source unknown)・明細の鍵 = 伝票番号-明細行番号', async () => {
  const b = buildMercariOrder(one1([{}], [{}], '2JY4WJifUnmFpJm5GiXSSX'));
  const h = b.payload.header;
  assert.deepEqual([b.key, b.payload.mall, b.payload.scope_key, b.payload.mall_order_no, h.source_system, h.shop_code, h.ordered_at, h.status_source, h.is_cancelled, h.cancelled_at, h.shipped_at_source, h.source_updated_at, h.transform_version],
    ['mercari|main|2JY4WJifUnmFpJm5GiXSSX', 'mercari', 'main', '2JY4WJifUnmFpJm5GiXSSX', 'ne', '8', '2026-10-12T23:59:48+09:00', '50', false, null, '2026-10-13T15:00:00+09:00', '2026-10-12T22:01:06Z', MERCARI_TRANSFORM_VERSION]);
  assert.deepEqual([h.total_amount_jpy, h.items_amount_jpy, h.shipping_fee_jpy, h.shop_coupon_jpy, h.mall_coupon_jpy, h.points_used_jpy, h.amount_source], [null, null, null, null, null, null, 'unknown']);
  assert.deepEqual(b.payload.lines, [{ line_key: '1586057-1', listing_code: null, sku_code: 'as-ki5', qty: 1, cancelled_qty: 0, unit_price_jpy: null, line_amount_jpy: null, tax_rate: null, amount_source: 'unknown', source_line_ref: 'ne_slip:1586057|row:1' }]);
  assert.ok(!JSON.stringify(b.payload).includes('548'), 'NE の小計金額を運んでいる');
});
await t('🚨 同梱 (実データの形): 同梱先の伝票に別の注文の明細が載る → 明細の受注番号でまとめる。同梱元の取消の伝票の明細は取消の数・同梱先の明細は有効 = 数量は 1 回だけ・注文は取消にならない・注文日は自分の伝票', async () => {
  const bases = [{ slip: '1394864', no: NO_A, date: '2026-03-30 17:41:03', ship: '2026-03-31 11:40:43' }, { slip: '1394860', no: NO_B, date: '2026-03-30 17:30:00', ship: '', st: '2', c: 'キャンセル', cdate: '2026-03-31 09:00:00' }];
  const lines = [{ slip: '1394864', ln: 1, no: NO_A, code: 'macchakuzuyu', qty: 3 }, { slip: '1394864', ln: 2, no: NO_B, code: 'macchakuzuyu', qty: 3 }, { slip: '1394860', ln: 1, no: NO_B, code: 'macchakuzuyu', qty: 3, c: 'キャンセル' }];
  const gs = groupsOf(bases, lines);
  assert.deepEqual(gs.map((g) => g.no), [NO_B, NO_A]);
  const a = buildMercariOrder(gs[1]), b = buildMercariOrder(gs[0]);
  assert.deepEqual(a.payload.lines.map((l) => [l.line_key, l.qty, l.cancelled_qty]), [['1394864-1', 3, 0]], '同梱先の注文に同梱元の明細が混ざった');
  assert.deepEqual([b.payload.header.is_cancelled, b.payload.header.ordered_at, b.payload.header.status_source, b.payload.header.shipped_at_source, b.payload.header.cancelled_at],
    [false, '2026-03-30T17:30:00+09:00', '50', '2026-03-31T11:40:43+09:00', null]);
  assert.deepEqual(b.payload.lines.map((l) => [l.line_key, l.qty, l.cancelled_qty]), [['1394860-1', 3, 3], ['1394864-2', 3, 0]]);
  // 同梱元の伝票がもう無い (実データ 63 行ぶん) → 注文日は同梱先の伝票の受注日
  const g2 = groupsOf([bases[0]], lines.slice(0, 2)).find((g) => g.no === NO_B);
  const c = buildMercariOrder(g2);
  assert.deepEqual([c.payload.header.ordered_at, c.payload.header.is_cancelled, c.payload.lines.length], ['2026-03-30T17:41:03+09:00', false, 1]);
  const st = {}; buildMercariOrder(g2, { stats: st }); assert.equal(st.mergedSlipRefs, 1);
});
await t('取消: 伝票がキャンセル → 注文の取消・取消の時刻 / 一部の明細だけキャンセル → 注文は有効でその明細だけ取消の数 / 明細が全部キャンセル (伝票は有効) → 注文の取消 / 取消でない数量 0 の明細があれば取消にしない', async () => {
  const c1 = buildMercariOrder(one1([{ c: 'キャンセル', st: '2', ship: '', cdate: '2026-10-12 09:13:02' }], [{ c: 'キャンセル' }], '2JY4WJifUnmFpJm5GiXSSX'));
  assert.deepEqual([c1.payload.header.is_cancelled, c1.payload.header.cancelled_at, c1.payload.header.shipped_at_source, c1.payload.header.status_source, c1.payload.lines[0].cancelled_qty], [true, '2026-10-12T09:13:02+09:00', null, '2', 1]);
  const c1b = buildMercariOrder(one1([{ c: 'キャンセル', st: '2', ship: '' }], [{ c: '有効' }], '2JY4WJifUnmFpJm5GiXSSX'));   // 伝票がキャンセルなら明細が有効のままでも取消 (実データ 24 行)
  assert.deepEqual([c1b.payload.header.is_cancelled, c1b.payload.lines[0].cancelled_qty], [true, 1]);
  const c2 = buildMercariOrder(one1([{}], [{ ln: 1 }, { ln: 2, code: 'matatabi5', qty: 2, c: 'キャンセル' }], '2JY4WJifUnmFpJm5GiXSSX'));
  assert.deepEqual([c2.payload.header.is_cancelled, c2.payload.lines.map((l) => l.cancelled_qty)], [false, [0, 2]]);
  const c3 = buildMercariOrder(one1([{}], [{ ln: 1, c: 'キャンセル' }, { ln: 2, c: 'キャンセル' }], '2JY4WJifUnmFpJm5GiXSSX'));
  assert.deepEqual([c3.payload.header.is_cancelled, c3.payload.header.shipped_at_source], [true, null]);
  const c4 = buildMercariOrder(one1([{}], [{ ln: 1, c: 'キャンセル' }, { ln: 2, qty: 0 }], '2JY4WJifUnmFpJm5GiXSSX'));
  assert.equal(c4.payload.header.is_cancelled, false, '取消でない数量 0 の明細を取消と数えた');
});
await t('分割 (1 注文が 2 伝票): 状態は一番手前 (20 と 50 → 20)・出荷の時刻は全部の伝票が出荷確定するまで null・明細は伝票番号の数の順', async () => {
  const bases = [{ slip: '1000', no: NO_C, st: '50' }, { slip: '999', no: NO_C, st: '20', ship: '' }];
  const lines = [{ slip: '1000', no: NO_C, ln: 1 }, { slip: '999', no: NO_C, ln: 1, code: 'wae10-wm' }];
  const b = buildMercariOrder(one1(bases, lines, NO_C));
  assert.deepEqual([b.payload.header.status_source, b.payload.header.shipped_at_source, b.payload.lines.map((l) => l.line_key)], ['20', null, ['999-1', '1000-1']]);
  const b2 = buildMercariOrder(one1([bases[0], { ...bases[1], st: '50', ship: '2026-10-14 10:00:00' }], lines, NO_C));
  assert.deepEqual([b2.payload.header.status_source, b2.payload.header.shipped_at_source], ['50', '2026-10-14T10:00:00+09:00']);
});
await t('商品コードが空の明細は sku_code null で送る (注文ごと捨てない・数える)・明細の受注番号が空なら伝票の受注番号・明細の無い伝票も注文になる (明細 0)', async () => {
  const st = {};
  const b = buildMercariOrder(one1([{}], [{ code: '' }], '2JY4WJifUnmFpJm5GiXSSX'), { stats: st });
  assert.deepEqual([b.payload.lines[0].sku_code, st.noCode], [null, 1]);
  const g = groupsOf([{}], [{ no: '' }]);
  assert.deepEqual([g.length, g[0].no, buildMercariOrder(g[0]).payload.lines.length], [1, '2JY4WJifUnmFpJm5GiXSSX', 1]);
  const e = buildMercariOrder(one1([{}], [], '2JY4WJifUnmFpJm5GiXSSX'), { stats: st });
  assert.deepEqual([e.payload.lines.length, e.payload.header.is_cancelled, st.noLines], [0, false, 1]);
});
await t('🚨 読めない値は例外 (黙って 0 や別の値にしない): 受注日の形 (秒なし・13 月・前後の空白)・受注数の欠落・明細行番号の欠落・受注状態区分が数字でない・注文番号の空白・別の店舗の伝票', async () => {
  for (const bad of ['2026-10-12 23:59', '2026-13-12 23:59:48', ' 2026-10-12 23:59:48', '2026-10-12T23:59:48']) throws(() => buildMercariOrder(one1([{ date: bad }], [{}], '2JY4WJifUnmFpJm5GiXSSX')), /受注日/);
  throws(() => buildMercariOrder(one1([{}], [{ qty: null }], '2JY4WJifUnmFpJm5GiXSSX')), /受注数が無い/);
  throws(() => buildMercariOrder(one1([{}], [{ ln: null }], '2JY4WJifUnmFpJm5GiXSSX')), /明細行番号の無い/);
  throws(() => buildMercariOrder(one1([{ st: '出荷確定済' }], [{}], '2JY4WJifUnmFpJm5GiXSSX')), /受注状態区分が数字でない/);
  throws(() => buildMercariOrder(one1([{ ship: '2026/10/13' }], [{}], '2JY4WJifUnmFpJm5GiXSSX')), /出荷確定日/);
  throws(() => buildMercariOrder(one1([{ no: '2JY4WJifUnmFpJm5GiXSSX ' }], [{ no: '2JY4WJifUnmFpJm5GiXSSX ' }], '2JY4WJifUnmFpJm5GiXSSX ')), /注文番号の形が違う/);
  throws(() => buildMercariOrder(one1([{ shop: '1' }], [{}], '2JY4WJifUnmFpJm5GiXSSX')), /店舗 1/);
  assert.deepEqual([isNeJst('2026-02-29 10:00:00'), isNeJst('2026-10-12 24:00:00'), isNeJst(Buffer.from('2026-10-12 10:00:00')), isMercariOrderNo('2JY4WJifUnmFpJm5GiXSSX'), isMercariOrderNo('ab c')], [false, false, false, true, false]);
  // 範囲の判定に使う値が読めない注文は groupMercariOrders が印を付ける (どの mode でも build に渡る)
  assert.deepEqual(groupsOf([{ date: '2026-10-12' }], [{}]).map((g) => g.invalidDate), [true]);
  assert.deepEqual(groupsOf([{ no: 'a b' }], [{ no: 'a b' }]).map((g) => g.invalidDate), [true]);
});
await t('取込時刻 (synced_at) だけ変わっても指紋は同じ・状態や数量が変われば変わる', async () => {
  const fp = (bases, lines) => fingerprintOf(MERCARI_TRANSFORM_VERSION, buildMercariOrder(one1(bases, lines, '2JY4WJifUnmFpJm5GiXSSX')).payload);
  const a = fp([{}], [{}]);
  assert.equal(fp([{ synced: '2026-10-13 22:00:00' }], [{ synced: '2026-10-13 22:00:00' }]), a);
  assert.notEqual(fp([{ st: '40', ship: '' }], [{}]), a);
  assert.notEqual(fp([{}], [{ qty: 2 }]), a);
});

console.log('メルカリ: 通し (送り手 ⇄ 本物の受け口を HTTP で)');
const pg = new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: quiet });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
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
const push = (w, l, x = {}) => pushOrders({ mall: 'mercari', warehouse: w, ledger: l, fetchImpl: f, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });
const ins = (db, table, r) => { const cols = Object.keys(r); db.prepare(`insert into ${table} (${cols.join(', ')}) values (${cols.map((c) => '@' + c).join(', ')})`).run(r); };
const addSlip = (db, x, lines = [{}]) => { ins(db, 'raw_ne_order_base', nb(x)); for (const l of lines) ins(db, 'raw_ne_orders', nl({ slip: x.slip, no: x.no, date: x.date, ...l })); };
const applyShipment = async (w, slip, seq = 1) => {
  const base = w.prepare('select * from raw_ne_order_base where 伝票番号 = ?').get(slip);
  const s = buildShipment(base, w.prepare('select * from raw_ne_orders where 伝票番号 = ? order by 明細行番号').all(slip));
  return (await one(`select core.apply_shipment_batch(1::smallint, $1, $2::bigint, $3::jsonb, $4::jsonb) as r`, [s.ne_slip_no, seq, JSON.stringify(s.header), JSON.stringify(s.lines)])).r;
};
const prod = (await one(`insert into core.products (company_id, name) values (1, '見本') returning product_id`)).product_id;
const skuA = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'as-ki5', '見本 SKU') returning sku_id`, [prod])).sku_id;
const W = openWarehouse();
const L = openLedger(null, { memory: true, kind: 'order:mercari' }); L.markInitialized();
await t('0013 の前提: メルカリ main は orders_enabled・NE 店舗 8 = mercari / main / 接頭辞なし', async () => {
  assert.equal((await one(`select orders_enabled from core.mall_order_policy where company_id = 1 and mall = 'mercari' and scope_key = 'main'`)).orders_enabled, true);
  const s = await one(`select mall, scope_key, order_no_prefix from core.ne_shops where company_id = 1 and shop_code = '8'`);
  assert.deepEqual([s.mall, s.scope_key, s.order_no_prefix], ['mercari', 'main', '']);
});
await t('送る: 店舗 8 の伝票だけ (ほかの店舗は読まない)・SKU は NE の商品コードで解決 (当たらなければ unresolved_code)・状態は NE の対応表 (50 = shipped / 20 = ready)・金額は null・同梱は明細の受注番号でまとまる', async () => {
  addSlip(W, { slip: '1394864', no: NO_A, date: '2026-03-30 17:41:03', ship: '2026-03-31 11:40:43' }, [{ ln: 1, code: 'as-ki5', qty: 3 }, { ln: 2, no: NO_B, code: 'wakearihin', qty: 1 }]);
  addSlip(W, { slip: '1394860', no: NO_B, date: '2026-03-30 17:30:00', ship: '', st: '2', c: 'キャンセル', cdate: '2026-03-31 09:00:00' }, [{ ln: 1, code: 'wakearihin', qty: 1, c: 'キャンセル' }]);
  addSlip(W, { slip: '1400001', no: NO_C, date: '2026-04-01 09:22:07', ship: '', st: '20' }, [{ ln: 1, code: 'as-ki5', qty: 2 }]);
  addSlip(W, { slip: '1400002', no: '411111111111', shop: '1', date: '2026-04-01 10:00:00' }, [{ ln: 1, shop: '1' }]);   // 楽天の伝票 (読まない)
  const r = await push(W, L);
  assert.deepEqual([r.ok, r.scanned, r.inScope, r.applied, r.failed.length, r.transformErrors.length], [true, 3, 3, 3, 0, 0], JSON.stringify(r.transformErrors));
  assert.deepEqual(posts().flatMap((b) => b.rows.map((x) => x.mall_order_no)).sort(), [NO_B, NO_A, NO_C].sort());
  const rows = (await pg.query(`select o.mall_order_no, o.source_system, o.shop_code, o.status, o.is_cancelled, o.items_amount_jpy, o.amount_source, o.order_date_jst::text as d, l.line_key, l.qty, l.cancelled_qty, l.sku_id, l.unresolved_code, l.line_amount_jpy
    from core.orders o join core.order_lines l using (order_id) where o.mall = 'mercari' and l.removed_at is null order by o.mall_order_no, l.line_key`)).rows;
  assert.deepEqual(rows.map((x) => [x.mall_order_no, x.source_system, x.shop_code, x.status, x.is_cancelled, x.items_amount_jpy, x.amount_source, x.d, x.line_key, x.qty, x.cancelled_qty, x.sku_id == null ? null : Number(x.sku_id), x.unresolved_code, x.line_amount_jpy]), [
    [NO_B, 'ne', '8', 'shipped', false, null, 'unknown', '2026-03-30', '1394860-1', 1, 1, null, 'wakearihin', null],
    [NO_B, 'ne', '8', 'shipped', false, null, 'unknown', '2026-03-30', '1394864-2', 1, 0, null, 'wakearihin', null],
    [NO_C, 'ne', '8', 'ready', false, null, 'unknown', '2026-04-01', '1400001-1', 2, 0, Number(skuA), null, null],
    [NO_A, 'ne', '8', 'shipped', false, null, 'unknown', '2026-03-30', '1394864-1', 3, 0, Number(skuA), null, null]].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[8] < b[8] ? -1 : 1)));
});
await t('NE 店舗 8 の伝票は受注番号そのまま (接頭辞なし) で注文に結ばれる (送った後の結び直し)。同梱先の伝票は同梱先の注文に結ばれる', async () => {
  for (const s of ['1394864', '1394860', '1400001']) assert.equal(await applyShipment(W, s), 'applied');
  addSlip(W, { slip: '1400010', no: '2JQ1111111111111111111', date: '2026-04-02 10:00:00' }, [{ ln: 1 }]);
  const r = await push(W, L);
  assert.ok(r.ok && r.afterSend && r.afterSend.ran && !r.afterSend.error, JSON.stringify(r.afterSend));
  const links = (await pg.query(`select s.ne_slip_no, o.mall_order_no from core.shipments s left join core.orders o using (order_id) where s.shop_code = '8' order by s.ne_slip_no`)).rows;
  assert.deepEqual(links.map((x) => [x.ne_slip_no, x.mall_order_no]), [['1394860', NO_B], ['1394864', NO_A], ['1400001', NO_C]]);
});
await t('2 回目は変化なし (取込時刻だけ変わる) → 出荷確定した注文だけ送る → 突合 (dailyLocal) が一致 → 取消になった注文は cancelled → 突合も追いつく', async () => {
  W.prepare(`update raw_ne_order_base set synced_at = '2026-10-13 22:00:00'`).run();
  W.prepare(`update raw_ne_orders set synced_at = '2026-10-13 22:00:00'`).run();
  assert.equal((await push(W, L)).changed, 0);
  W.prepare(`update raw_ne_order_base set 受注状態区分 = '50', 出荷確定日 = '2026-04-02 13:36:00' where 伝票番号 = '1400001'`).run();
  const r = await push(W, L);
  assert.deepEqual([r.changed, r.applied], [1, 1]);
  assert.equal((await one(`select status from core.orders where mall = 'mercari' and mall_order_no = $1`, [NO_C])).status, 'shipped');
  assert.deepEqual(MALL_SPECS.mercari.dailyLocal(W, '2026-03-30', '2026-04-02'), { rows: [
    { order_date: '2026-03-30', orders: 2, lines: 3, items_amount_jpy: 0, cancelled: 0 }, { order_date: '2026-04-01', orders: 1, lines: 1, items_amount_jpy: 0, cancelled: 0 },
    { order_date: '2026-04-02', orders: 1, lines: 1, items_amount_jpy: 0, cancelled: 0 }], errors: 0 });
  assert.equal((await reconcileOrdersDaily({ mall: 'mercari', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2025-01-01', to: '2026-04-30', log: quiet })).ok, true);
  W.prepare(`update raw_ne_order_base set キャンセル区分 = 'キャンセル', 受注キャンセル日 = '2026-04-03 09:00:00' where 伝票番号 = '1400010'`).run();
  assert.equal((await reconcileOrdersDaily({ mall: 'mercari', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2026-04-01', to: '2026-04-30', log: quiet })).ok, false);
  await push(W, L);
  const o = await one(`select status, is_cancelled, cancelled_at is not null as has_c from core.orders where mall = 'mercari' and mall_order_no = '2JQ1111111111111111111'`);
  assert.deepEqual([o.status, o.is_cancelled, o.has_c], ['cancelled', true, true]);
  assert.equal((await reconcileOrdersDaily({ mall: 'mercari', warehouse: W, fetchImpl: f, base: BASE_URL, syncKey: 'k', from: '2026-04-01', to: '2026-04-30', log: quiet })).ok, true);
});
await t('範囲 (D-28): 2025-01-01 より前の受注は、2025 年以降に出荷確定した伝票に載っていれば入る (実データ 19 注文)・載っていなければ入らない。--from/--to は注文日だけ', async () => {
  const w = openWarehouse();
  addSlip(w, { slip: '961983', no: 'jf8gkphrjwPYde8tABecFi', date: '2024-09-22 19:17:05', ship: '2025-01-08 13:25:03' });
  addSlip(w, { slip: '961000', no: 'oldoldoldoldoldoldoldo', date: '2024-09-01 10:00:00', ship: '2024-09-02 10:00:00' });
  addSlip(w, { slip: '1100000', no: '2JA0000000000000000000', date: '2025-02-01 10:00:00' });
  const l = openLedger(null, { memory: true, kind: 'order:mercari' }); l.markInitialized();
  const r = await push(w, l, { dryRun: true });
  assert.deepEqual([r.scanned, r.inScope, r.stats.referencedCount], [3, 2, 2]);
  const r2 = await push(w, l, { dryRun: true, from: '2025-02-01', to: '2025-02-28' });
  assert.equal(r2.inScope, 1);
  l.close(); w.close();
});
await t('🚨 受注日・注文番号が読めない注文は、どの mode でも黙って範囲の外に落ちず「整形できない」❌ (ほかの注文は届く)', async () => {
  const w = openWarehouse();
  addSlip(w, { slip: '2000001', no: '2JZ0000000000000000001', date: '2026-03-05 10:00' });
  addSlip(w, { slip: '2000002', no: '2JZ0000000000000000002', date: '2026-02-30 10:00:00' });
  addSlip(w, { slip: '2000003', no: '2JZ 000000000000000003', date: '2026-03-05 10:00:00' });
  addSlip(w, { slip: '2000009', no: '2JZ0000000000000000009', date: '2026-03-05 10:00:00' });
  for (const x of [{}, { from: '2026-03-01', to: '2026-03-31' }, { from: '2026-04-01', to: '2026-04-30' }]) {
    const l = openLedger(null, { memory: true, kind: 'order:mercari' }); l.markInitialized();
    const r = await push(w, l, { dryRun: true, ...x });
    assert.equal(r.transformErrors.length, 3, `${JSON.stringify(x)}: ${JSON.stringify(r.transformErrors)}`);
    l.close();
  }
  assert.equal(MALL_SPECS.mercari.dailyLocal(w, '2026-03-01', '2026-03-31').errors, 3, '突合が整形できない注文を数えていない');
  w.close();
});
await t('売上日次は作らない (salesDaily: false = 金額が null)・送り手は送り状番号・配送方法・商品名・小計金額を読まない', async () => {
  assert.equal(MALL_SPECS.mercari.salesDaily, false);
  await assert.rejects(() => refreshSalesDaily({ mall: 'mercari', base: BASE_URL, syncKey: 'k', fetchImpl: f, log: quiet }), /売上日次は止めている.*メルカリ/);
  for (const c of ['送り状番号', '配送方法名', '商品名', '小計金額']) assert.ok(![...MERCARI_BASE_COLUMNS, ...MERCARI_LINE_COLUMNS].includes(c), c);
  const body = JSON.stringify(posts());
  assert.ok(!body.includes('123456789012') && !body.includes('ネコポス'), '送り状番号・配送方法を運んでいる');
});
await t('daily-sync: 「Company DB 出荷 push」の直後 (NE が成功した朝だけ) に --mall mercari --incremental --require-backfilled の 1 段・retry の 3 か所 (UPSTREAM_OF には入れない)', async () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const iShip = ds.indexOf("results.push({ name: 'CompanyDB出荷', ...cdbShipResult });");
  const iMc = ds.indexOf("runScript('apps/company-db/push/mall-orders.mjs --mall mercari --incremental --require-backfilled'");
  const iElse = ds.indexOf("console.log('[DailySync] NE API 失敗のため出荷サマリ再構築と Company DB 出荷 push");
  assert.ok(iShip > 0 && iMc > iShip && iElse > iMc, [iShip, iMc, iElse].join(','));
  assert.ok(!/runScript\(|results\.push\(/.test(ds.slice(iShip + 60, iMc)), '出荷 push とメルカリの間に別の段がある');
  assert.ok(/const RETRYABLE_JOBS = \[[^\]]*'CompanyDB注文\(メルカリ\)'/.test(ds), 'RETRYABLE_JOBS に無い');
  assert.deepEqual(JOB_DEFINITIONS['CompanyDB注文(メルカリ)'].args, ['--mall', 'mercari', '--incremental', '--require-backfilled']);
  assert.ok(RETRY_ORDER.includes('CompanyDB注文(メルカリ)'));
  assert.ok(!Object.hasOwn(UPSTREAM_OF, 'CompanyDB注文(メルカリ)'), 'NE の取込は retry されないので上流に組まない');
});
L.close(); W.close(); server.close();

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
