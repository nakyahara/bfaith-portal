#!/usr/bin/env node
/**
 * test-company-db-purchase.mjs — 0014 (発注) の受入試験 (Company DB 構想 08 §5。D6)
 *
 * PGlite で 0001〜0014 を流し、元の台帳 (apps/purchase-orders/db.js の po_orders / po_order_items / po_item_events / po_settings) と同じ規則を確かめる:
 *   追跡の境界 (未設定なら入らない) / ヘッダ: issued ⇔ issued_at・閉鎖は issued だけ・origin の規則 (両方向 + 親は issued)・仕入先ごとに draft は 1 件・一意 /
 *   発行済みは変えられない・消せない・足せない (保守経路は通る) / 明細: qty > 0・小数の単価・product_key・分納の次回予定の組 /
 *   イベント: 元の CHECK / 対象 = issued かつ境界以後 (tracking_mode は関係ない) / 閉鎖済みに通常イベントは不可 / 残数超過 / 逆仕訳 / append-only /
 *   閉鎖はイベントから導出 (全消込で閉じる・逆仕訳で開く・直接更新の guard) / 整合性検査 / 残数 view / 商品別の注残 (tracked かつ境界以後だけ) / 会社の分離
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
await pg.query(`insert into core.products (company_id, name) values ($1, '他社品')`, [other]);
const skuOther = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'other-1', name from core.products where company_id = $1 returning sku_id`, [other])).sku_id;
const supOther = (await one(`insert into core.suppliers (company_id, code, name) values ($1, 'SUPX', '他社の仕入先') returning supplier_id`, [other])).supplier_id;
const BOUNDARY = '2026-07-13T00:00:00Z';

let seq = 0;
/** ヘッダを直接入れる (loader と同じ保守経路 = 発行ゲートと不変を外す。CHECK・unique・親の検査は外れない)。maintenance: false で通常経路 */
const po = async (x = {}) => {
  const params = [x.company ?? co, x.source_ref ?? `po_orders:${++seq}`, x.po_number ?? null, x.supplier_id === undefined ? (x.company && x.company !== co ? supOther : sup) : x.supplier_id, x.supplier_code ?? `SUP-${seq}`, x.supplier_name === undefined ? '見本仕入先' : x.supplier_name,
    x.status ?? 'issued', x.issued_at === undefined ? (x.status === 'draft' ? null : '2026-09-01T00:00:00Z') : x.issued_at, x.closed_at ?? null, x.tracking_mode === undefined ? (x.status === 'draft' ? null : 'tracked') : x.tracking_mode,
    x.origin ?? null, x.send_blocked ?? false, x.ne_slip_no ?? null, x.parent ?? null];
  const sql = `insert into core.purchase_orders (company_id, source_ref, po_number, supplier_id, supplier_code, supplier_name, status, issued_at, closed_at, tracking_mode, origin, send_blocked, ne_slip_no, parent_purchase_order_id, source_updated_at)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now()) returning purchase_order_id`;
  if (x.maintenance === false) return (await one(sql, params)).purchase_order_id;
  await pg.exec(`begin; set local core.po_maintenance = 'on';`);
  try { const r = await one(sql, params); await pg.exec('commit'); return r.purchase_order_id; }
  catch (e) { await pg.exec('rollback'); throw e; }
};
const line = (poId, x = {}) => one(`insert into core.purchase_order_lines (company_id, purchase_order_id, source_ref, product_key, product_code, sku_id, unresolved_code, qty, unit_cost, next_expected_date, next_expected_qty, next_action_date, remainder_disposition, source_updated_at)
  values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now()) returning purchase_order_line_id`,
  [x.company ?? co, poId, x.source_ref ?? `po_order_items:${++seq}`, x.product_key ?? `pk-${seq}`, x.product_code ?? 'sku-a', x.sku_id === undefined ? skuA : x.sku_id, x.unresolved ?? null, x.qty ?? 10, x.cost ?? 500,
   x.ned ?? null, x.neq ?? null, x.nad ?? null, x.disp ?? null]).then((r) => r.purchase_order_line_id);
/** draft で作って明細を入れてから issued に上げる (元のアプリと同じ順。発行済みには明細を足せない) */
const issuedPo = async (x = {}, lines = [{ product_key: 'A', qty: 10 }]) => {
  const id = await po({ ...x, status: 'draft', issued_at: null, tracking_mode: null, closed_at: null, po_number: null });
  const ids = [];
  for (const l of lines) ids.push(await line(id, l));
  const tm = x.tracking_mode === undefined ? 'tracked' : x.tracking_mode;
  const sql = `update core.purchase_orders set status = 'issued', issued_at = $2, po_number = $3, tracking_mode = $4 where purchase_order_id = $1`;
  const params = [id, x.issued_at ?? '2026-09-01T00:00:00Z', x.po_number ?? `PO-AUTO-${++seq}`, tm];
  if (tm === 'tracked') await pg.query(sql, params);   // 正規経路 (発行ゲートを通る)
  else {                                                 // tracking_mode の無い issued (境界前の legacy 等) は loader と同じ保守経路でしか作れない
    await pg.exec(`begin; set local core.po_maintenance = 'on';`);
    try { await pg.query(sql, params); await pg.exec('commit'); } catch (e) { await pg.exec('rollback'); throw e; }
  }
  return { id, lines: ids };
};
const ev = (poId, lineId, type, qty, date, key, x = {}) => one(`insert into events.purchase_order_events (company_id, occurred_at, actor_type, source_actor_type, source_system, idempotency_key, purchase_order_id, purchase_order_line_id, event_type, qty, effective_date, receipt_source, inbound_ref, reason_code, reason_text, reverses_event_id)
  values ($1, now(), $2, $3, 'purchase_orders', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) returning event_id`,
  [x.company ?? co, x.actor ?? 'system', x.srcActor ?? 'system', key, poId, lineId, type, qty, date, x.src ?? null, x.inbound ?? null, x.reason ?? null, x.text ?? null, x.rev ?? null]).then((r) => r.event_id);
const bal = (lineId) => one(`select ordered_qty, received_qty, shortage_qty, cancelled_qty, cutoff_qty, remaining_qty from mart.v_purchase_order_open where purchase_order_line_id = $1`, [lineId]);
const closedAt = (poId) => one(`select closed_at from core.purchase_orders where purchase_order_id = $1`, [poId]).then((r) => r.closed_at);

console.log('0014: 表・view・境界');
await t('表・view・関数がある。events.purchase_order_events は append-only。追跡の境界が無ければイベントは入らない (未設定を黙って通さない)', async () => {
  const tables = (await pg.query(`select table_schema || '.' || table_name as t from information_schema.tables where table_type = 'BASE TABLE' and table_name in ('purchase_order_settings','purchase_orders','purchase_order_lines','purchase_order_events') order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(tables, ['core.purchase_order_lines', 'core.purchase_order_settings', 'core.purchase_orders', 'events.purchase_order_events']);
  const views = (await pg.query(`select table_name as t from information_schema.views where table_schema = 'mart' and table_name like 'v_purchase%' order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(views, ['v_purchase_backorder_by_sku', 'v_purchase_order_open']);
  assert.equal(await num(`select count(*) as n from pg_trigger where tgrelid = 'events.purchase_order_events'::regclass and tgfoid = 'core.reject_mutation'::regproc`), 2);
  const p = await issuedPo({ po_number: 'PO-0000' });
  await rejects(() => ev(p.id, p.lines[0], 'receipt', 1, '2026-09-02', 'nb1', { src: 'manual' }), /tracking_started_at is not set/);
  await rejects(() => pg.query(`select core.assert_purchase_orders_consistent($1::smallint)`, [co]), /has no tracking_started_at/);
  await pg.query(`insert into core.purchase_order_settings (company_id, tracking_started_at) values ($1, $2)`, [co, BOUNDARY]);
  await ev(p.id, p.lines[0], 'receipt', 1, '2026-09-02', 'nb2', { src: 'manual' });
  // 境界は変えられない・消せない (通常経路)。保守経路だけ通る
  await rejects(() => pg.query(`update core.purchase_order_settings set tracking_started_at = '2027-01-01' where company_id = $1`, [co]), /tracking_started_at is immutable/);
  await rejects(() => pg.query(`delete from core.purchase_order_settings where company_id = $1`, [co]), /tracking_started_at is immutable/);
  await pg.exec(`begin; set local core.po_maintenance = 'on'; update core.purchase_order_settings set note = 'x' where company_id = ${co}; commit;`);
  assert.equal((await one(`select tracking_started_at::text as t from core.purchase_order_settings where company_id = $1`, [co])).t.slice(0, 10), '2026-07-13');
});
await t('🚨 発行のゲート (通常経路): 境界がある会社では issued の直接 INSERT は不可 / draft → issued は po_number・issued_at・tracking_mode=tracked・明細 1 つ以上が必要 / 揃えば通る', async () => {
  await rejects(() => po({ maintenance: false, supplier_code: 'SUP-G1', po_number: 'PO-G1' }), /issue gate: .*直接 INSERT/);
  const d = await po({ maintenance: false, status: 'draft', supplier_code: 'SUP-G2' });
  await rejects(() => pg.query(`update core.purchase_orders set status = 'issued', issued_at = now(), po_number = 'PO-G2', tracking_mode = 'tracked' where purchase_order_id = $1`, [d]), /issue gate: .*明細/);   // 明細なし
  await line(d, { product_key: 'G', qty: 1 });
  await rejects(() => pg.query(`update core.purchase_orders set status = 'issued', issued_at = now() where purchase_order_id = $1`, [d]), /issue gate/);                                       // po_number / tracking_mode なし
  await rejects(() => pg.query(`update core.purchase_orders set status = 'issued', issued_at = now(), po_number = 'PO-G2' where purchase_order_id = $1`, [d]), /issue gate/);               // tracking_mode なし
  await pg.query(`update core.purchase_orders set status = 'issued', issued_at = now(), po_number = 'PO-G2', tracking_mode = 'tracked' where purchase_order_id = $1`, [d]);
  assert.equal((await one(`select status from core.purchase_orders where purchase_order_id = $1`, [d])).status, 'issued');
});

console.log('ヘッダ (po_orders と同じ規則)');
await t('issued ⇔ issued_at / 閉鎖は issued だけ / origin の規則 (移行 PO は ne_slip_no + send_blocked、ne_slip_no は移行 PO だけ、追加発注は parent 必須、parent は追加発注だけ・自分は不可・親は issued) / supplier_name 必須', async () => {
  await rejects(() => po({ status: 'draft', issued_at: '2026-09-01T00:00:00Z', tracking_mode: null }), /ck_po_issued_at/);
  await rejects(() => po({ status: 'issued', issued_at: null }), /ck_po_issued_at/);
  await rejects(() => po({ status: 'draft', closed_at: '2026-09-02T00:00:00Z', tracking_mode: null }), /ck_po_closed_only_issued/);
  await rejects(() => po({ origin: 'migration' }), /ck_po_migration_attrs/);
  await rejects(() => po({ origin: 'migration', ne_slip_no: 'NE-1', send_blocked: false }), /ck_po_migration_attrs/);
  await rejects(() => po({ ne_slip_no: 'NE-9' }), /ck_po_ne_slip_only_migration/);
  await rejects(() => po({ origin: 'supplement' }), /ck_po_supplement_parent/);
  await rejects(() => po({ supplier_name: null }), /supplier_name|null value/);
  const mig = await po({ origin: 'migration', ne_slip_no: 'NE-1', send_blocked: true, po_number: 'PO-2026-0001' });
  await rejects(() => po({ parent: mig }), /ck_po_supplement_parent/);
  const draft = await po({ status: 'draft', supplier_code: 'SUP-D' });
  await rejects(() => po({ origin: 'supplement', parent: draft }), /origin rules: parent .* not issued/);
  await rejects(() => po({ origin: 'supplement', parent: 999999 }), /origin rules|foreign key|violates/i);
  const supp = await po({ origin: 'supplement', parent: mig, po_number: 'PO-2026-0002' });
  assert.ok(supp);
  await rejects(() => pg.query(`update core.purchase_orders set parent_purchase_order_id = purchase_order_id, origin = 'supplement' where purchase_order_id = $1 and status = 'draft'`, [draft]), /own parent|ck_po_not_own_parent/);
});
await t('po_number・ne_slip_no は会社で一意 / 仕入先ごとに draft は同時に 1 件 (issued は何件でも) / 他社の仕入先は付かない', async () => {
  await rejects(() => po({ ne_slip_no: 'NE-1', origin: 'migration', send_blocked: true }), /duplicate|unique/i);
  await rejects(() => po({ po_number: 'PO-2026-0001' }), /duplicate|unique/i);
  await po({ status: 'draft', supplier_code: 'SUP-ONE' });
  await rejects(() => po({ status: 'draft', supplier_code: 'SUP-ONE' }), /ux_purchase_orders_one_draft|duplicate|unique/i);
  await po({ supplier_code: 'SUP-ONE' }); await po({ supplier_code: 'SUP-ONE' });   // issued は何件でも
  await rejects(() => po({ supplier_id: supOther }), /foreign key|violates/i);
  const unres = await po({ supplier_id: null, supplier_code: 'ZZZ' });   // 解決できない仕入先はコードだけ残る
  assert.equal((await one(`select supplier_id, supplier_code from core.purchase_orders where purchase_order_id = $1`, [unres])).supplier_code, 'ZZZ');
});
await t('🚨 発行済みの PO は 発行属性を変えられない・消せない・明細を足せない・明細を変えられない・消せない (元の immutable trigger)。draft → issued は可。保守経路 (set local core.po_maintenance) は通る', async () => {
  const p = await issuedPo({ po_number: 'PO-2026-0020', supplier_code: 'SUP-IM' }, [{ product_key: 'A', qty: 3 }]);
  for (const set of [`status = 'draft', issued_at = null, tracking_mode = null`, `po_number = 'PO-X'`, `issued_at = now()`, `tracking_mode = null`, `supplier_code = 'other'`, `origin = 'migration', ne_slip_no = 'NE-Z', send_blocked = true`]) {
    await rejects(() => pg.query(`update core.purchase_orders set ${set} where purchase_order_id = $1`, [p.id]), /issued purchase order .* immutable/);
  }
  await pg.query(`update core.purchase_orders set note = 'メモは変えてよい', pml_as_of_date = current_date where purchase_order_id = $1`, [p.id]);
  await rejects(() => pg.query(`delete from core.purchase_orders where purchase_order_id = $1`, [p.id]), /immutable/);
  await rejects(() => line(p.id, { product_key: 'B' }), /明細追加は不可|immutable/);
  for (const set of [`qty = 2`, `qty = 4`, `product_key = 'Z'`, `product_code = 'zzz'`, `unit_cost = 1`, `condition_id = 'c'`]) {
    await rejects(() => pg.query(`update core.purchase_order_lines set ${set} where purchase_order_line_id = $1`, [p.lines[0]]), /immutable/);
  }
  await pg.query(`update core.purchase_order_lines set sku_id = $2, promised_date = current_date, remainder_disposition = 'awaiting_confirmation', next_action_date = current_date where purchase_order_line_id = $1`, [p.lines[0], skuB]);   // 解決と次回予定は変えてよい
  await rejects(() => pg.query(`delete from core.purchase_order_lines where purchase_order_line_id = $1`, [p.lines[0]]), /immutable/);
  // 🚨 発行済み明細を draft の PO へ移して数量を変える抜け道は無い (移動元を見る)。draft の明細を発行済み PO へ移す (移入) も不可 (移動先を見る)
  const d = await po({ status: 'draft', supplier_code: 'SUP-IM2' });
  await rejects(() => pg.query(`update core.purchase_order_lines set purchase_order_id = $2, qty = 1 where purchase_order_line_id = $1`, [p.lines[0], d]), /immutable/);
  const dl = await line(d, { product_key: 'M', qty: 1 });
  await rejects(() => pg.query(`update core.purchase_order_lines set purchase_order_id = $2 where purchase_order_line_id = $1`, [dl, p.id]), /移入は不可|immutable/);
  // 保守経路 (loader) は通る
  await pg.exec(`begin; set local core.po_maintenance = 'on'; update core.purchase_orders set po_number = 'PO-2026-0020b' where po_number = 'PO-2026-0020'; commit;`);
  assert.equal((await one(`select po_number from core.purchase_orders where purchase_order_id = $1`, [p.id])).po_number, 'PO-2026-0020b');
  await rejects(() => pg.query(`update core.purchase_orders set po_number = 'PO-2026-0020c' where purchase_order_id = $1`, [p.id]), /immutable/);   // 取引の外では元どおり
});

console.log('明細 (po_order_items と同じ規則)');
let poMain, lnA, lnB;
await t('qty > 0 / 小数の単価をそのまま持つ / 同じ PO に同じ product_key は 1 行 / SKU か unresolved / 分納の次回予定は組で決まる (null の罠を含む)', async () => {
  poMain = await po({ status: 'draft', supplier_code: 'SUP-M', po_number: 'PO-2026-0010' });
  lnA = await line(poMain, { product_key: 'A', qty: 10, cost: 12.5 });
  assert.equal(Number((await one(`select unit_cost from core.purchase_order_lines where purchase_order_line_id = $1`, [lnA])).unit_cost), 12.5);
  await rejects(() => line(poMain, { product_key: 'A', qty: 1 }), /duplicate|unique/i);
  await rejects(() => line(poMain, { product_key: 'Z', qty: 0 }), /qty|violates check/);
  await rejects(() => line(poMain, { product_key: 'Z', sku_id: null }), /ck_po_lines_resolved/);
  lnB = await line(poMain, { product_key: 'B', sku_id: null, unresolved: 'unknown-code', product_code: 'unknown-code', qty: 5 });
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_delivery' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_delivery', ned: '2026-10-01', neq: 3, nad: '2026-10-05' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_confirmation', nad: '2026-10-05', neq: 3 }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', ned: '2026-10-01' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', ned: '2026-10-01', neq: 3 }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', nad: '2026-10-05' }), /ck_po_lines_disposition/);
  await rejects(() => line(poMain, { product_key: 'C', disp: 'awaiting_delivery', ned: '2026-10-01', neq: 0 }), /next_expected_qty|violates check/);
  const c1 = await line(poMain, { product_key: 'C', disp: 'awaiting_delivery', ned: '2026-10-01', neq: 3, sku_id: skuB, product_code: 'sku-b', qty: 4 });
  const c2 = await line(poMain, { product_key: 'D', disp: 'awaiting_confirmation', nad: '2026-10-05', qty: 2 });
  assert.ok(c1 && c2);
  await pg.query(`update core.purchase_orders set status = 'issued', issued_at = '2026-09-01T00:00:00Z', tracking_mode = 'tracked' where purchase_order_id = $1`, [poMain]);
});
await t('会社の分離: 同じ会社の PO に他社の SKU は付かない (fk sku) / 他社の PO に自社の SKU は付かない (fk sku) / 他社の PO に他社の SKU は付く', async () => {
  const mine = await po({ status: 'draft', supplier_code: 'SUP-FK' });
  const theirs = await po({ status: 'draft', supplier_code: 'SUP-FK', company: other });
  const e1 = await rejects(() => line(mine, { product_key: 'X', sku_id: skuOther }), /foreign key|violates/i);
  assert.match(e1.message, /sku/i);
  const e2 = await rejects(() => line(theirs, { product_key: 'X', sku_id: skuA, company: other }), /foreign key|violates/i);
  assert.match(e2.message, /sku/i);
  const e3 = await rejects(() => line(mine, { product_key: 'X', sku_id: skuOther, company: other }), /foreign key|violates/i);   // 明細の会社 (other) と PO の会社 (co) が違う。SKU は other で正しい
  assert.match(e3.message, /purchase_order_lines_company_id_purchase_order_id_fkey/);
  assert.ok(await line(theirs, { product_key: 'X', sku_id: skuOther, company: other }));
});

console.log('イベント (po_item_events と同じ規則)');
let r1;
await t('🚨 条件付き必須列 (元の CHECK) / 残数超過の拒否 / 逆仕訳の一致と 1 回 / 残数 view (cutoff を含む) / append-only', async () => {
  await rejects(() => ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'x1'), /ck_po_events_receipt_source/);
  await rejects(() => ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'x2', { src: 'logizard' }), /ck_po_events_receipt_inbound/);
  await rejects(() => ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'x2b', { src: 'manual', reason: 'x' }), /ck_po_events_receipt_cancel_reason/);
  await rejects(() => ev(poMain, lnA, 'shortage', 4, '2026-09-11', 'x3'), /ck_po_events_shortage_reason/);
  await rejects(() => ev(poMain, lnA, 'shortage', 4, '2026-09-11', 'x4', { reason: 'other' }), /ck_po_events_shortage_other/);
  await rejects(() => ev(poMain, lnA, 'cancel', 1, '2026-09-11', 'x4b', { reason: 'x' }), /ck_po_events_receipt_cancel_reason/);
  await rejects(() => ev(poMain, lnA, 'cancel', 1, '2026-09-11', 'x4c', { inbound: 'po_inbound_items:1' }), /ck_po_events_non_receipt_inbound/);
  await rejects(() => ev(poMain, lnA, 'receipt', 11, '2026-09-10', 'x5', { src: 'manual' }), /exceeds open qty/);
  await rejects(() => ev(poMain, lnA, 'receipt', 1, '2026-09-10', 'x5b', { src: 'manual', srcActor: 'bot' }), /source_actor_type|violates check/);
  r1 = await ev(poMain, lnA, 'receipt', 6, '2026-09-10', 'e1', { src: 'logizard', inbound: 'po_inbound_items:77', actor: 'external', srcActor: 'migration' });
  await ev(poMain, lnA, 'shortage', 3, '2026-09-11', 'e2', { reason: 'supplier_shortage' });
  await ev(poMain, lnA, 'shortage', 1, '2026-09-11', 'e2b', { reason: 'cutoff' });
  await rejects(() => ev(poMain, lnA, 'cancel', 1, '2026-09-11', 'x6'), /exceeds open qty/);
  let b = await bal(lnA);
  assert.deepEqual([b.ordered_qty, b.received_qty, b.shortage_qty, b.cancelled_qty, b.cutoff_qty, b.remaining_qty], [10, 6, 4, 0, 1, 0]);
  await rejects(() => ev(poMain, lnA, 'reversal', 5, '2026-09-10', 'x7', { reason: 'correction', text: 'x', rev: r1 }), /must match/);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x8', { text: 'x', rev: r1 }), /ck_po_events_reversal_reason/);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x8b', { reason: 'correction', text: '', rev: r1 }), /ck_po_events_reversal_reason/);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x8c', { reason: 'correction', text: 'x' }), /ck_po_events_reversal_ref/);
  const rv = await ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'e5', { reason: 'correction', text: '誤入力', rev: r1 });
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x9', { reason: 'correction', text: 'again', rev: r1 }), /duplicate|unique/i);
  await rejects(() => ev(poMain, lnA, 'reversal', 6, '2026-09-10', 'x9b', { reason: 'correction', text: 'again', rev: rv }), /cannot reverse a reversal/);
  b = await bal(lnA);
  assert.deepEqual([b.received_qty, b.remaining_qty], [0, 6]);
  await rejects(() => pg.query(`update events.purchase_order_events set reason_text = 'x'`), /append-only|reject|変更|禁止/i);
  await rejects(() => pg.query(`delete from events.purchase_order_events`), /append-only|reject|変更|禁止/i);
});
await t('🚨 対象 = issued かつ 境界以後 (tracking_mode は関係ない): 境界より前の PO・draft の PO には入らない。他社のイベント・別の PO の明細は付かない', async () => {
  const legacy = await issuedPo({ supplier_code: 'SUP-L', issued_at: '2026-06-01T00:00:00Z', tracking_mode: 'tracked' }, [{ product_key: 'L' }]);
  await rejects(() => ev(legacy.id, legacy.lines[0], 'receipt', 1, '2026-06-02', 'lx1', { src: 'manual' }), /event scope: only issued .* tracking_started_at/);
  const untracked = await issuedPo({ supplier_code: 'SUP-U', tracking_mode: null }, [{ product_key: 'U' }]);   // 境界以後だが tracking_mode が null → イベントは入る (元も入る)、注残の集計には入らない
  await ev(untracked.id, untracked.lines[0], 'receipt', 1, '2026-09-02', 'ux1', { src: 'manual' });
  const draft = await po({ status: 'draft', supplier_code: 'SUP-DR' });
  const dl = await line(draft, { product_key: 'D' });
  await rejects(() => ev(draft, dl, 'receipt', 1, '2026-09-02', 'dx1', { src: 'manual' }), /event scope/);
  await rejects(() => ev(poMain, lnA, 'receipt', 1, '2026-09-12', 'ox1', { src: 'manual', company: other }), /foreign key|violates/i);
  await rejects(() => ev(poMain, dl, 'receipt', 1, '2026-09-12', 'ox2', { src: 'manual' }), /not found in po|foreign key|violates/i);
});
await t('🚨 閉鎖はイベントから導出: 全消込で閉じる (時刻は最初のまま) → 閉鎖済みに通常イベントは不可 → 逆仕訳で残数が戻れば開く。直接更新の guard (改変不可 / 残数があれば閉じられない / 全消込は開けない / draft・明細なしは閉じられない)', async () => {
  const p = await issuedPo({ supplier_code: 'SUP-C', po_number: 'PO-2026-0030' }, [{ product_key: 'A', qty: 2 }, { product_key: 'B', qty: 1, product_code: 'sku-b', sku_id: skuB }]);
  assert.equal(await closedAt(p.id), null);
  await rejects(() => pg.query(`update core.purchase_orders set closed_at = now() where purchase_order_id = $1`, [p.id]), /still has remaining qty/);
  await ev(p.id, p.lines[0], 'receipt', 2, '2026-09-10', 'c1', { src: 'manual' });
  assert.equal(await closedAt(p.id), null);                                                          // B が残っている
  const rc = await ev(p.id, p.lines[1], 'shortage', 1, '2026-09-11', 'c2', { reason: 'cutoff' });
  const closed1 = await closedAt(p.id);
  assert.ok(closed1);                                                                                 // 全消込で閉じた
  await rejects(() => ev(p.id, p.lines[0], 'receipt', 1, '2026-09-12', 'c3', { src: 'manual' }), /is closed/);
  await rejects(() => pg.query(`update core.purchase_orders set closed_at = now() where purchase_order_id = $1`, [p.id]), /閉鎖時刻の改変は不可/);
  await rejects(() => pg.query(`update core.purchase_orders set closed_at = null where purchase_order_id = $1`, [p.id]), /cannot be reopened/);
  await ev(p.id, p.lines[1], 'reversal', 1, '2026-09-11', 'c4', { reason: 'correction', text: '取り消し', rev: rc });
  assert.equal(await closedAt(p.id), null);                                                          // 残数が戻ったので開いた
  await ev(p.id, p.lines[1], 'receipt', 1, '2026-09-13', 'c5', { src: 'manual' });
  const closed2 = await closedAt(p.id);
  assert.ok(closed2 && String(closed2) !== String(closed1) || closed2);                               // 閉じ直し (新しい時刻)
  const empty = await po({ supplier_code: 'SUP-E' });
  await rejects(() => pg.query(`update core.purchase_orders set closed_at = now() where purchase_order_id = $1`, [empty]), /empty purchase order/);
  assert.ok(Number((await one(`select core.assert_purchase_orders_consistent($1::smallint) as n`, [co])).n) > 0);
});
await t('整合性検査: 保守経路で矛盾させると例外、直せば通る', async () => {
  const p = await issuedPo({ supplier_code: 'SUP-AS' }, [{ product_key: 'A', qty: 1 }]);
  await pg.exec(`begin; set local core.po_maintenance = 'on'; update core.purchase_orders set closed_at = now() where purchase_order_id = ${p.id}; commit;`);
  await rejects(() => pg.query(`select core.assert_purchase_orders_consistent($1::smallint)`, [co]), /1 purchase orders have closed_at inconsistent/);
  await pg.exec(`begin; set local core.po_maintenance = 'on'; update core.purchase_orders set closed_at = null where purchase_order_id = ${p.id}; commit;`);
  await pg.query(`select core.assert_purchase_orders_consistent($1::smallint)`, [co]);
});

console.log('商品別の注残 (元の v_ledger_backorder_by_product と同じ条件)');
await t('🚨 issued・tracked・open・残 > 0・境界以後 だけを足す。閉じた PO / 境界前 / tracking_mode null / draft / 残 0 は入らない。同じ product_key は足し合わせる。他社は別', async () => {
  const p2 = await issuedPo({ supplier_code: 'SUP-B2', po_number: 'PO-2026-0011' }, [{ product_key: 'Q', qty: 5 }]);
  const p3 = await issuedPo({ supplier_code: 'SUP-B2b', po_number: 'PO-2026-0014' }, [{ product_key: 'Q', qty: 6 }]);                 // Q: 5 + 6 = 11 (2 明細)
  const closed = await issuedPo({ supplier_code: 'SUP-B3', po_number: 'PO-2026-0012' }, [{ product_key: 'Q', qty: 100 }]);
  await ev(closed.id, closed.lines[0], 'shortage', 100, '2026-09-12', 'b1', { reason: 'cutoff' });                      // 閉じた (残 0) → 入らない
  const theirs = await issuedPo({ supplier_code: 'SUP-B4', company: other, po_number: 'PO-2026-0013' }, [{ product_key: 'Q', qty: 7, sku_id: skuOther, company: other }]);
  await pg.query(`insert into core.purchase_order_settings (company_id, tracking_started_at) values ($1, $2)`, [other, BOUNDARY]);
  const rows = Object.fromEntries((await pg.query(`select product_key, backorder_qty, open_lines, sku_id from mart.v_purchase_backorder_by_sku where company_id = $1 order by product_key`, [co])).rows.map((r) => [r.product_key, r]));
  assert.equal(rows.Q.backorder_qty, 11); assert.equal(rows.Q.open_lines, 2); assert.equal(rows.Q.sku_id, skuA);
  assert.equal(rows.B.backorder_qty, 5); assert.equal(rows.B.sku_id, null);        // 未解決の明細も注残には入る (元も product_key で数える)
  assert.equal(rows.C.backorder_qty, 4); assert.equal(rows.D.backorder_qty, 2);
  assert.equal(rows.L, undefined); assert.equal(rows.U, undefined);                  // 境界前 / tracking_mode null
  // A = 各試験で作った issued・tracked・open の明細の残数の合計 (view の式で数え直して一致)
  const aRecount = await num(`select coalesce(sum(remaining_qty), 0) as n from mart.v_purchase_order_open where company_id = $1 and product_key = 'A' and status = 'issued' and tracking_mode = 'tracked' and closed_at is null and remaining_qty > 0 and in_tracking_window`, [co]);
  assert.equal(rows.A.backorder_qty, aRecount);
  const theirRows = (await pg.query(`select product_key, backorder_qty from mart.v_purchase_backorder_by_sku where company_id = $1`, [other])).rows;
  assert.deepEqual(theirRows, [{ product_key: 'Q', backorder_qty: 7 }]);
  assert.ok(p2 && p3 && theirs);
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
