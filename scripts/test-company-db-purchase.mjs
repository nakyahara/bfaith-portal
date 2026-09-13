#!/usr/bin/env node
/**
 * test-company-db-purchase.mjs — 0014 (発注) の受入試験 (Company DB 構想 08 §5。D6)
 *
 * PGlite で 0001〜0014 を流し、元の台帳 (apps/purchase-orders/db.js の po_orders / po_order_items / po_item_events) と同じ規則を確かめる:
 *   ヘッダ: issued ⇔ issued_at / 閉鎖は issued だけ / tracked は issued だけ / 移行 PO の規則 (ne_slip_no + send_blocked) / parent は supplement だけ / po_number・ne_slip_no の一意 /
 *   明細: qty > 0 / 同じ PO に同じ product_key は 1 行 / 分納の次回予定の組 (awaiting_delivery / awaiting_confirmation / null) / SKU か unresolved /
 *   イベント: 条件付き必須列 (元の CHECK) / 残数超過の拒否 / 逆仕訳の一致と 1 回 / legacy・draft には入れない / append-only /
 *   発注数を使った分より減らせない / 残数 view = 元の v_po_item_balance の式 (cutoff を含む) / 商品別の注残 = 元の条件 (issued・tracked・open・残 > 0)
 * 🚨 明細の for update の 2 接続の並行は PGlite では書けない → 本番で手で確かめる (08 §7.6)
 * 実行: node scripts/test-company-db-purchase.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`); return threw; };
const quiet = () => {};

const pg = new PGlite();
const db = pgliteAdapter(pg);
const applied = await applyMigrations(db, { log: quiet });
assert.ok(applied.applied.includes('0014'), '0014 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
const co = 1;
await pg.query(`insert into core.products (company_id, name) values ($1, '見本A'), ($1, '見本B')`, [co]);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', case name when '見本A' then 'sku-a' else 'sku-b' end, name from core.products`);
const skuA = (await one(`select sku_id from core.skus where code = 'sku-a'`)).sku_id;
const skuB = (await one(`select sku_id from core.skus where code = 'sku-b'`)).sku_id;
const sup = (await one(`insert into core.suppliers (company_id, code, name) values ($1, 'SUP1', '見本仕入先') returning supplier_id`, [co])).supplier_id;
const other = (await one(`select max(company_id)::smallint + 1 as c from core.companies`)).c;
await pg.query(`insert into core.companies (company_id, name, kind) values ($1, 'other', 'subsidiary')`, [other]);
const supOther = (await one(`insert into core.suppliers (company_id, code, name) values ($1, 'SUPX', '他社の仕入先') returning supplier_id`, [other])).supplier_id;

let seq = 0;
const po = (x = {}) => one(`insert into core.purchase_orders (company_id, source_ref, po_number, supplier_id, supplier_code, status, issued_at, closed_at, is_tracked, origin, send_blocked, ne_slip_no, parent_purchase_order_id, source_updated_at)
  values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now()) returning purchase_order_id`,
  [x.company ?? co, x.source_ref ?? `po_orders:${++seq}`, x.po_number ?? null, x.supplier_id === undefined ? sup : x.supplier_id, x.supplier_code ?? 'SUP1', x.status ?? 'issued',
   x.issued_at === undefined ? (x.status === 'draft' ? null : '2026-09-01T00:00:00Z') : x.issued_at, x.closed_at ?? null, x.is_tracked ?? (x.status !== 'draft'), x.origin ?? null, x.send_blocked ?? false, x.ne_slip_no ?? null, x.parent ?? null]).then((r) => r.purchase_order_id);
const line = (poId, x = {}) => one(`insert into core.purchase_order_lines (company_id, purchase_order_id, source_ref, product_key, product_code, sku_id, unresolved_code, qty, unit_cost_jpy, next_expected_date, next_expected_qty, next_action_date, remainder_disposition, source_updated_at)
  values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now()) returning purchase_order_line_id`,
  [x.company ?? co, poId, x.source_ref ?? `po_order_items:${++seq}`, x.product_key ?? `pk-${seq}`, x.product_code ?? 'sku-a', x.sku_id === undefined ? skuA : x.sku_id, x.unresolved ?? null, x.qty ?? 10, x.cost ?? 500,
   x.ned ?? null, x.neq ?? null, x.nad ?? null, x.disp ?? null]).then((r) => r.purchase_order_line_id);
const ev = (poId, lineId, type, qty, date, key, x = {}) => one(`insert into events.purchase_order_events (company_id, occurred_at, actor_type, source_actor_type, source_system, idempotency_key, purchase_order_id, purchase_order_line_id, event_type, qty, effective_date, receipt_source, inbound_ref, reason_code, reason_text, reverses_event_id)
  values ($1, now(), $2, $3, 'purchase_orders', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) returning event_id`,
  [x.company ?? co, x.actor ?? 'system', x.srcActor ?? 'system', key, poId, lineId, type, qty, date, x.src ?? null, x.inbound ?? null, x.reason ?? null, x.text ?? null, x.rev ?? null]).then((r) => r.event_id);

console.log('0014: 表・view');
await t('表・view・関数・trigger がある。events.purchase_order_events は append-only', async () => {
  const tables = (await pg.query(`select table_schema || '.' || table_name as t from information_schema.tables where table_type = 'BASE TABLE' and table_name in ('purchase_orders','purchase_order_lines','purchase_order_events') order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(tables, ['core.purchase_order_lines', 'core.purchase_orders', 'events.purchase_order_events']);
  const views = (await pg.query(`select table_name as t from information_schema.views where table_schema = 'mart' and table_name like 'v_purchase%' order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(views, ['v_purchase_backorder_by_sku', 'v_purchase_order_open']);
  assert.equal(await num(`select count(*) as n from pg_trigger where tgrelid = 'events.purchase_order_events'::regclass and tgfoid = 'core.reject_mutation'::regproc`), 2);
});

console.log('ヘッダ (po_orders と同じ規則)');
await t('issued ⇔ issued_at / 閉鎖・tracked は issued だけ / 移行 PO は ne_slip_no + send_blocked が必須 / parent は supplement だけ・自分は不可 / po_number・ne_slip_no は会社で一意 / 他社の仕入先は付かない', async () => {
  await rejects(() => po({ status: 'draft', issued_at: '2026-09-01T00:00:00Z', is_tracked: false }), /ck_po_issued_at/);
  await rejects(() => po({ status: 'issued', issued_at: null }), /ck_po_issued_at/);
  await rejects(() => po({ status: 'draft', closed_at: '2026-09-02T00:00:00Z', is_tracked: false }), /ck_po_closed_only_issued/);
  await rejects(() => po({ status: 'draft', is_tracked: true }), /ck_po_tracked_only_issued/);
  await rejects(() => po({ origin: 'migration' }), /ck_po_migration_attrs/);
  await rejects(() => po({ origin: 'migration', ne_slip_no: 'NE-1', send_blocked: false }), /ck_po_migration_attrs/);
  const mig = await po({ origin: 'migration', ne_slip_no: 'NE-1', send_blocked: true, po_number: 'PO-2026-0001' });
  await rejects(() => po({ ne_slip_no: 'NE-1', origin: 'migration', send_blocked: true }), /duplicate|unique/i);
  await rejects(() => po({ po_number: 'PO-2026-0001' }), /duplicate|unique/i);
  await rejects(() => po({ parent: mig }), /ck_po_parent_is_supplement/);
  const supp = await po({ origin: 'supplement', parent: mig, po_number: 'PO-2026-0002' });
  assert.ok(supp);
  await rejects(() => pg.query(`update core.purchase_orders set parent_purchase_order_id = purchase_order_id where purchase_order_id = $1`, [supp]), /ck_po_not_own_parent/);
  await rejects(() => po({ supplier_id: supOther }), /foreign key|violates/i);
  const unres = await po({ supplier_id: null, supplier_code: 'ZZZ' });   // 解決できない仕入先はコードだけ残る
  assert.equal((await one(`select supplier_id, supplier_code from core.purchase_orders where purchase_order_id = $1`, [unres])).supplier_code, 'ZZZ');
});

console.log('明細 (po_order_items と同じ規則)');
let poMain, lnA, lnB;
await t('qty > 0 / 同じ PO に同じ product_key は 1 行 / SKU か unresolved / 分納の次回予定は組で決まる (awaiting_delivery ⇔ 日付 + 数量、awaiting_confirmation ⇔ 期限、null ⇔ 全部 null)', async () => {
  poMain = await po({ po_number: 'PO-2026-0010' });
  lnA = await line(poMain, { product_key: 'A', qty: 10 });
  await rejects(() => line(poMain, { product_key: 'A', qty: 1 }), /duplicate|unique/i);
  await rejects(() => line(poMain, { product_key: 'Z', qty: 0 }), /qty|violates check/);
  await rejects(() => line(poMain, { product_key: 'Z', sku_id: null }), /ck_po_lines_resolved/);
  lnB = await line(poMain, { product_key: 'B', sku_id: null, unresolved: 'unknown-code', product_code: 'unknown-code', qty: 5 });
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_delivery' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_delivery', ned: '2026-10-01', neq: 3, nad: '2026-10-05' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_confirmation', nad: '2026-10-05', neq: 3 }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', ned: '2026-10-01' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', ned: '2026-10-01', neq: 3 }), /ck_po_lines_disposition/);   // 🚨 disposition が null なのに次回予定だけある (= の CHECK だと null で通る罠)
  await rejects(() => line(poMain, { product_key: 'C', nad: '2026-10-05' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_delivery', ned: '2026-10-01', neq: 0 }), /next_expected_qty|violates check/);
  const c1 = await line(poMain, { product_key: 'C', disp: 'awaiting_delivery', ned: '2026-10-01', neq: 3, sku_id: skuB, product_code: 'sku-b', qty: 4 });
  const c2 = await line(poMain, { product_key: 'D', disp: 'awaiting_confirmation', nad: '2026-10-05', qty: 2 });
  assert.ok(c1 && c2);
  await rejects(() => line(poMain, { product_key: 'E', sku_id: skuA, company: other }), /foreign key|violates/i);
});

console.log('イベント (po_item_events と同じ規則)');
await t('🚨 条件付き必須列 (元の CHECK) / 残数超過の拒否 / 逆仕訳の一致と 1 回 / 発注数を使った分より減らせない / 残数 view (cutoff を含む)', async () => {
  await rejects(() => ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'x1'), /ck_po_events_receipt_source/);
  await rejects(() => ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'x2', { src: 'logizard' }), /ck_po_events_receipt_inbound/);
  await rejects(() => ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'x2b', { src: 'manual', reason: 'x' }), /ck_po_events_receipt_cancel_reason/);
  await rejects(() => ev(poMain, lnA, 'shortage', 4, '2026-09-11', 'x3'), /ck_po_events_shortage_reason/);
  await rejects(() => ev(poMain, lnA, 'shortage', 4, '2026-09-11', 'x4', { reason: 'other' }), /ck_po_events_shortage_other/);
  await rejects(() => ev(poMain, lnA, 'cancel', 1, '2026-09-11', 'x4b', { reason: 'x' }), /ck_po_events_receipt_cancel_reason/);
  await rejects(() => ev(poMain, lnA, 'cancel', 1, '2026-09-11', 'x4c', { inbound: 'po_inbound_items:1' }), /ck_po_events_non_receipt_inbound/);
  await rejects(() => ev(poMain, lnA, 'receipt', 11, '2026-09-10', 'x5', { src: 'manual' }), /exceeds open qty/);
  await rejects(() => ev(poMain, lnA, 'receipt', 1, '2026-09-10', 'x5b', { src: 'manual', srcActor: 'bot' }), /source_actor_type|violates check/);
  const r1 = await ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'e1', { src: 'logizard', inbound: 'po_inbound_items:77', actor: 'external', srcActor: 'migration' });
  await ev(poMain, lnA, 'shortage', 3, '2026-09-11', 'e2', { reason: 'supplier_shortage' });
  await ev(poMain, lnA, 'shortage', 1, '2026-09-11', 'e2b', { reason: 'cutoff' });
  await rejects(() => ev(poMain, lnA, 'cancel', 1, '2026-09-11', 'x6'), /exceeds open qty/);
  await rejects(() => pg.query(`update core.purchase_order_lines set qty = 9 where purchase_order_line_id = $1`, [lnA]), /below used qty/);   // 使った 10 より下げられない
  await pg.query(`update core.purchase_order_lines set qty = 12 where purchase_order_line_id = $1`, [lnA]);   // 増やすのは可
  let b = await one(`select ordered_qty, received_qty, shortage_qty, cancelled_qty, cutoff_qty, remaining_qty from mart.v_purchase_order_open where purchase_order_line_id = $1`, [lnA]);
  assert.deepEqual([b.ordered_qty, b.received_qty, b.shortage_qty, b.cancelled_qty, b.cutoff_qty, b.remaining_qty], [12, 6, 4, 0, 1, 2]);
  await rejects(() => ev(poMain, lnA, 'reversal', 5, '2026-09-10', 'x7', { reason: 'correction', text: 'x', rev: r1 }), /must match/);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x8', { text: 'x', rev: r1 }), /ck_po_events_reversal_reason/);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x8b', { reason: 'correction', text: '', rev: r1 }), /ck_po_events_reversal_reason/);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x8c', { reason: 'correction', text: 'x' }), /ck_po_events_reversal_ref/);
  const rv = await ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'e5', { reason: 'correction', text: '誤入力', rev: r1 });
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x9', { reason: 'correction', text: 'again', rev: r1 }), /duplicate|unique/i);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x9b', { reason: 'correction', text: 'again', rev: rv }), /cannot reverse a reversal/);
  b = await one(`select received_qty, remaining_qty from mart.v_purchase_order_open where purchase_order_line_id = $1`, [lnA]);
  assert.deepEqual([b.received_qty, b.remaining_qty], [0, 8]);
  await rejects(() => pg.query(`update events.purchase_order_events set reason_text = 'x'`), /append-only|reject|変更|禁止/i);
  await rejects(() => pg.query(`delete from events.purchase_order_events`), /append-only|reject|変更|禁止/i);
});
await t('🚨 legacy (tracked でない) の PO と draft の PO にはイベントを入れない (元も拒む)。他社のイベントは付かない', async () => {
  const legacy = await po({ is_tracked: false, issued_at: '2026-06-01T00:00:00Z' });
  const ll = await line(legacy, { product_key: 'L' });
  await rejects(() => ev(legacy, ll, 'receipt', 1, '2026-06-02', 'lx1', { src: 'manual' }), /only for issued & tracked/);
  const draft = await po({ status: 'draft' });
  const dl = await line(draft, { product_key: 'D' });
  await rejects(() => ev(draft, dl, 'receipt', 1, '2026-09-02', 'dx1', { src: 'manual' }), /only for issued & tracked/);
  await rejects(() => ev(poMain, lnA, 'receipt', 1, '2026-09-12', 'ox1', { src: 'manual', company: other }), /foreign key|violates/i);
  await rejects(() => ev(poMain, dl, 'receipt', 1, '2026-09-12', 'ox2', { src: 'manual' }), /foreign key|violates/i);   // 別の PO の明細
});

console.log('商品別の注残 (元の v_ledger_backorder_by_product と同じ条件)');
await t('🚨 issued・tracked・open・残 > 0 だけを足す。閉じた PO / legacy / draft / 残 0 は入らない。同じ product_key は足し合わせる', async () => {
  const po2 = await po({ po_number: 'PO-2026-0011' });
  await line(po2, { product_key: 'A', qty: 5 });                                   // A: 8 (poMain) + 5 = 13
  const closed = await po({ closed_at: '2026-09-05T00:00:00Z', po_number: 'PO-2026-0012' });
  await line(closed, { product_key: 'A', qty: 100 });                              // 閉じた PO は入らない
  const done = await po({ po_number: 'PO-2026-0013' });
  const dl = await line(done, { product_key: 'F', qty: 2 });
  await ev(done, dl, 'receipt', 2, '2026-09-12', 'f1', { src: 'manual' });          // 残 0 は入らない
  const rows = Object.fromEntries((await pg.query(`select product_key, backorder_qty, open_lines, sku_id from mart.v_purchase_backorder_by_sku where company_id = $1 order by product_key`, [co])).rows.map((r) => [r.product_key, r]));
  assert.equal(rows.A.backorder_qty, 13); assert.equal(rows.A.open_lines, 2); assert.equal(rows.A.sku_id, skuA);
  assert.equal(rows.B.backorder_qty, 5); assert.equal(rows.B.sku_id, null);        // 未解決の明細も注残には入る (元も product_key で数える)
  assert.equal(rows.C.backorder_qty, 4); assert.equal(rows.D.backorder_qty, 2);
  assert.equal(rows.F, undefined); assert.equal(rows.L, undefined);
  assert.equal(await num(`select count(*) as n from mart.v_purchase_backorder_by_sku where product_key = 'A' and company_id = $1`, [other]), 0);
  // 再オープン (closed_at = null) で入る
  await pg.query(`update core.purchase_orders set closed_at = null where purchase_order_id = $1`, [closed]);
  assert.equal((await one(`select backorder_qty from mart.v_purchase_backorder_by_sku where product_key = 'A'`)).backorder_qty, 113);
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
