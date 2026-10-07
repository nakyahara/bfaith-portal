#!/usr/bin/env node
/**
 * test-company-db-orders.mjs — 0013 (受注・出荷) の受入試験 (Company DB 構想 08 §4.1〜4.3 / §4.7。D4)
 *
 * PGlite で 0001〜0013 を流し、DDL と関数の「歯止め」を実際の操作で確かめる:
 *   状態の対応 (データ) / NE 店舗の対応 / 注文の可否 (Yahoo は 0013 で不可 → 0031 で可 = D-32 を 2026-09-26 に a へ) /
 *   apply_order_batch = ヘッダ + 明細集合の適用 (applied・same (世代だけ進み updated_at は動かない)・stale・同じ世代で内容違いは例外・明細の丸ごと置換・内部 ID の解決・JPY・取消) /
 *   apply_shipment_batch = 伝票の適用 + 注文との結び (同じ番号 / Yahoo は接頭辞 / 未着なら null → relink_shipments) /
 *   在庫イベント → 出荷明細 (会社一致) / v_shipments_daily = 旧 f_shipments_daily と同じ式 / v_shipments_unlinked の理由
 * 🚨 ヘッダの for update の 2 接続の並行は PGlite では書けない → 本番で手で確かめる (08 §7.6)
 * 実行: node scripts/test-company-db-orders.mjs
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
assert.ok(applied.applied.includes('0013'), '0013 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
const co = 1;
await pg.query(`insert into core.products (company_id, name) values ($1, '見本A'), ($1, '見本B')`, [co]);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', case name when '見本A' then 'sku-a' else 'bbb-2' end, name from core.products`);
const skuA = (await one(`select sku_id from core.skus where code = 'sku-a'`)).sku_id;
const skuB = (await one(`select sku_id from core.skus where code = 'bbb-2'`)).sku_id;
const lstAmz = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values ($1, 'amazon', 'main@A1VC38T7YXB528', 'pr_SKU-A', 'active') returning listing_id`, [co])).listing_id;
const lstRk = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values ($1, 'rakuten', 'main', 'rk-item-1', 'active') returning listing_id`, [co])).listing_id;
const other = (await one(`select max(company_id)::smallint + 1 as c from core.companies`)).c;
await pg.query(`insert into core.companies (company_id, name, kind) values ($1, 'other', 'subsidiary')`, [other]);
await pg.query(`insert into core.products (company_id, name) values ($1, '他社品')`, [other]);
const skuOther = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'other-1', name from core.products where company_id = $1 returning sku_id`, [other])).sku_id;

const applyOrder = (mall, scope, no, seq, header, lines, company = co) =>
  one(`select core.apply_order_batch($1::smallint, $2, $3, $4, $5::bigint, $6::jsonb, $7::jsonb) as r`, [company, mall, scope, no, seq, JSON.stringify(header), JSON.stringify(lines)]).then((r) => r.r);
const applyShip = (slip, seq, header, lines, company = co) =>
  one(`select core.apply_shipment_batch($1::smallint, $2, $3::bigint, $4::jsonb, $5::jsonb) as r`, [company, slip, seq, JSON.stringify(header), JSON.stringify(lines)]).then((r) => r.r);
const H = (x = {}) => ({ source_system: 'mall_api', ordered_at: '2026-09-13T16:30:00Z', status: 'confirmed', total_amount_jpy: 3000, items_amount_jpy: 2500, shipping_fee_jpy: 500, amount_source: 'mall_api', source_updated_at: '2026-09-13T17:00:00Z', transform_version: 'v1', content_hash: 'h1', ...x });   // lines_checksum は送らない (DB が計算する)
const SH = (x = {}) => ({ ne_order_no: '123456-20260914-0001', shop_code: '1', ne_status_code: '50', shipped_at: '2026-09-13T16:00:00Z', order_date_jst: '2026-09-13', delivery_method_code: '28', delivery_method_name: 'ネコポス', tracking_no: 'T1', source_updated_at: '2026-09-14T01:00:00Z', transform_version: 'v1', content_hash: 's1', ...x });   // lines_checksum は送らない

console.log('0013: 表・関数・seed');
await t('表・view・関数がある。状態の対応 (NE 5 件) / NE 店舗 15 件 / 注文の可否 8 件 (Yahoo は 0031 で true)', async () => {
  const tables = (await pg.query(`select table_name as t from information_schema.tables where table_schema = 'core' and table_type = 'BASE TABLE' and table_name in ('order_status_map','ne_shops','mall_order_policy','orders','order_lines','shipments','shipment_lines') order by 1`)).rows.map((r) => r.t);
  assert.equal(tables.length, 7);
  const views = (await pg.query(`select table_name as t from information_schema.views where table_schema = 'mart' and table_name like 'v_shipments%' order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(views, ['v_shipments_daily', 'v_shipments_unlinked']);
  assert.equal(await num(`select count(*) as n from pg_proc where proname in ('apply_order_batch','apply_shipment_batch','link_shipment_order','relink_shipments','map_order_status','resolve_listing_id','resolve_sku_id','touch_updated_at_unless_seq_only')`), 8);
  assert.equal(await num(`select count(*) as n from core.order_status_map where source_system = 'ne'`), 5);
  assert.equal((await one(`select core.map_order_status('ne', '50') as s`)).s, 'shipped');
  assert.equal((await one(`select core.map_order_status('ne', '99') as s`)).s, 'unknown');
  assert.equal(await num(`select count(*) as n from core.ne_shops`), 15);
  const y = await one(`select mall, scope_key, order_no_prefix from core.ne_shops where shop_code = '2'`);
  assert.equal(y.mall, 'yahoo'); assert.equal(y.order_no_prefix, 'b-faith01-');
  assert.equal((await one(`select mall from core.ne_shops where shop_code = '15'`)).mall, null);
  assert.equal((await one(`select orders_enabled from core.mall_order_policy where mall = 'yahoo'`)).orders_enabled, true);   // 0031 (D-32 = a)
  assert.equal((await one(`select orders_enabled from core.mall_order_policy where mall = 'rakuten'`)).orders_enabled, true);
  assert.equal(await num(`select count(*) as n from information_schema.columns where table_schema = 'events' and table_name = 'inventory_events' and column_name = 'shipment_line_id'`), 1);
});

console.log('apply_order_batch (注文 = モールの注文)');
await t('🚨 適用: ヘッダ + 明細 2 行 (出品コードと SKU コードで内部 ID を解決、当たらなければ unresolved_code)。order_date_jst は ordered_at の JST 日付', async () => {
  const lines = [{ line_key: '1', listing_code: 'RK-ITEM-1', sku_code: 'SKU-A', qty: 2, unit_price_jpy: 1000, line_amount_jpy: 2000, tax_rate: 0.10, amount_source: 'mall_api' }, { line_key: '2', listing_code: 'nope', sku_code: 'nope', qty: 1, line_amount_jpy: 500 }];
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 10, H(), lines), 'applied');
  const o = await one(`select order_id, order_date_jst::text d, status, received_batch_seq, total_amount_jpy, lines_checksum, first_ingest_run_id from core.orders where mall_order_no = 'RK-1'`);
  assert.equal(o.d, '2026-09-14'); assert.equal(o.status, 'confirmed'); assert.equal(Number(o.received_batch_seq), 10); assert.equal(Number(o.total_amount_jpy), 3000);
  assert.match(o.lines_checksum, /^[0-9a-f]{32}$/);   // 明細の checksum は DB が計算 (送り側は送っていない)
  assert.equal(await applyOrder('rakuten', 'main', 'RK-X', 1, H({ lines_checksum: 'sender-says-x' }), lines), 'applied');   // 送り側が入れても使わない
  assert.match((await one(`select lines_checksum from core.orders where mall_order_no = 'RK-X'`)).lines_checksum, /^[0-9a-f]{32}$/);
  assert.equal((await one(`select lines_checksum from core.orders where mall_order_no = 'RK-X'`)).lines_checksum, o.lines_checksum);
  assert.equal((await one(`select core.lines_checksum('[]'::jsonb) as c`)).c, (await one(`select core.lines_checksum('[]'::jsonb) as c`)).c);
  assert.notEqual((await one(`select core.lines_checksum('[]'::jsonb) as c`)).c, (await one(`select core.lines_checksum('[{"a":1}]'::jsonb) as c`)).c);
  assert.equal((await one(`select core.lines_checksum('[{"a":1},{"b":2}]'::jsonb) as c`)).c, (await one(`select core.lines_checksum('[{"b": 2}, {"a": 1}]'::jsonb) as c`)).c);   // 順序・空白に依らない
  const ls = (await pg.query(`select line_key, listing_id, sku_id, unresolved_code, qty, received_batch_seq from core.order_lines where order_id = $1 order by line_key`, [o.order_id])).rows;
  assert.equal(ls.length, 2);
  assert.equal(ls[0].listing_id, lstRk); assert.equal(ls[0].sku_id, skuA); assert.equal(ls[0].unresolved_code, null); assert.equal(Number(ls[0].received_batch_seq), 10);
  assert.equal(ls[1].listing_id, null); assert.equal(ls[1].sku_id, null); assert.equal(ls[1].unresolved_code, 'nope');
});
await t('🚨 same: 内容が同じ新しい世代は世代だけ進み (ヘッダ・明細とも)、updated_at は動かない。stale: 古い世代は何も変えない。同じ世代で内容が違えば例外', async () => {
  const before = await one(`select updated_at::text as u, received_batch_seq from core.orders where mall_order_no = 'RK-1'`);
  await new Promise((r) => setTimeout(r, 20));
  // 「同じ内容」= 明細の JSON が同じ (鍵の増減も違いになる = 送り側は毎回同じ形で送る。D5 の契約)
  const lines = [{ line_key: '1', listing_code: 'RK-ITEM-1', sku_code: 'SKU-A', qty: 2, unit_price_jpy: 1000, line_amount_jpy: 2000, tax_rate: 0.10, amount_source: 'mall_api' }, { line_key: '2', listing_code: 'nope', sku_code: 'nope', qty: 1, line_amount_jpy: 500 }];
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 11, H(), lines), 'same');
  const after = await one(`select updated_at::text as u, received_batch_seq from core.orders where mall_order_no = 'RK-1'`);
  assert.equal(Number(after.received_batch_seq), 11); assert.equal(after.u, before.u);
  assert.equal(await num(`select count(*) as n from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' and o.received_batch_seq = 11`), 2);
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 5, H({ content_hash: 'h0', total_amount_jpy: 1 }), lines), 'stale');
  assert.equal(Number((await one(`select total_amount_jpy from core.orders where mall_order_no = 'RK-1'`)).total_amount_jpy), 3000);
  await rejects(() => applyOrder('rakuten', 'main', 'RK-1', 11, H({ content_hash: 'h9' }), lines), /already applied with different content/);
  // 🚨 明細の checksum は DB が計算する: 送り側が lines_checksum を省略しても (H には無い)、明細の数量が変われば same ではなく applied (R1 #1)。同じ世代で明細だけ違えば例外
  const lines9 = [{ ...lines[0], qty: 9 }, lines[1]];
  await rejects(() => applyOrder('rakuten', 'main', 'RK-1', 11, H(), lines9), /already applied with different content/);
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 12, H(), [lines[1], lines[0]]), 'same');   // 順序が違うだけ = 同じ集合
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 13, H(), lines9), 'applied');   // ヘッダ固定・明細の数量だけ変更 → applied (same にならない)
  assert.equal((await one(`select qty from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' and o.line_key = '1'`)).qty, 9);
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 14, H(), []), 'applied');       // ヘッダ固定・空集合 → 全部 removed_at
  assert.equal(await num(`select count(*) as n from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' and o.removed_at is null`), 0);
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 15, H(), lines), 'applied');    // 元に戻す (qty 2、2 行とも現行に戻る)
  assert.equal(await num(`select count(*) as n from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' and o.removed_at is null`), 2);
});
await t('🚨 新しい世代で内容が変わったら、ヘッダを更新し明細を現行の集合に合わせる (外れた明細は消さず removed_at、id は保たれる。updated_at は動く)。取消は status=cancelled', async () => {
  const before = await one(`select updated_at::text as u from core.orders where mall_order_no = 'RK-1'`);
  const idBefore = (await one(`select order_line_id from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' and o.line_key = '1'`)).order_line_id;
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 16, H({ content_hash: 'h2', status: 'shipped', shipped_at_source: '2026-09-14T02:00:00Z', total_amount_jpy: 2500 }), [{ line_key: '1', sku_code: 'sku-a', qty: 2, cancelled_qty: 1 }]), 'applied');
  const o = await one(`select status, total_amount_jpy, shipped_at_source, updated_at::text as u, received_batch_seq from core.orders where mall_order_no = 'RK-1'`);
  assert.equal(o.status, 'shipped'); assert.equal(Number(o.total_amount_jpy), 2500); assert.ok(o.shipped_at_source); assert.notEqual(o.u, before.u); assert.equal(Number(o.received_batch_seq), 16);
  const ls = (await pg.query(`select o.order_line_id, o.line_key, o.sku_id, o.cancelled_qty, o.removed_at, o.received_batch_seq from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' order by o.line_key`)).rows;
  assert.equal(ls.length, 2);
  assert.equal(ls[0].order_line_id, idBefore); assert.equal(ls[0].sku_id, skuA); assert.equal(ls[0].cancelled_qty, 1); assert.equal(ls[0].removed_at, null);   // 同じ行を更新 (id 不変)
  assert.ok(ls[1].removed_at); assert.equal(Number(ls[1].received_batch_seq), 16);                                                                                // 集合から外れた行は removed_at
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 17, H({ content_hash: 'h2b', status: 'shipped', total_amount_jpy: 2500 }), [{ line_key: '1', sku_code: 'sku-a', qty: 2, cancelled_qty: 1 }, { line_key: '2', sku_code: 'nope', qty: 1 }]), 'applied');
  assert.equal((await one(`select removed_at from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' and o.line_key = '2'`)).removed_at, null);   // また現れたら戻る
  assert.equal(await applyOrder('rakuten', 'main', 'RK-1', 18, H({ content_hash: 'h3', is_cancelled: true, cancelled_at: '2026-09-14T03:00:00Z', status: 'shipped' }), []), 'applied');
  const c = await one(`select status, is_cancelled from core.orders where mall_order_no = 'RK-1'`);
  assert.equal(c.status, 'cancelled'); assert.equal(c.is_cancelled, true);
  assert.equal(await num(`select count(*) as n from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1' and o.removed_at is null`), 0);
  assert.equal(await num(`select count(*) as n from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'RK-1'`), 2);
  await rejects(() => pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash) values ($1, 'rakuten', 'main', 'bad', 'mall_api', now(), current_date, 'shipped', true, 1, now(), 'v1', 'h')`, [co]), /ck_orders_cancelled/);
});
await t('🚨 入れてよいモールだけ: orders_enabled が false のモール (0013 の Yahoo と同じ形。試験の中だけ止める) は不可、policy の無い scope も不可 (黙って入れない)。JPY 以外・content_hash 無し・ordered_at 無し・世代 0 は例外', async () => {
  await pg.query(`update core.mall_order_policy set orders_enabled = false where mall = 'yahoo'`);
  await rejects(() => applyOrder('yahoo', 'main', 'b-faith01-12345678', 1, H(), []), /not enabled/);
  await pg.query(`update core.mall_order_policy set orders_enabled = true where mall = 'yahoo'`);
  await rejects(() => applyOrder('amazon', 'us', 'AMZ-US-1', 1, H(), []), /no row/);
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 1, H({ currency: 'USD' }), []), /non-JPY/);
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 1, H({ content_hash: '' }), []), /content_hash/);
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 1, H({ ordered_at: null }), []), /ordered_at/);
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 0, H(), []), /positive/);
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 1, H(), [{ line_key: '1', sku_code: 'sku-a' }]), /qty/);            // 数量の欠落は 0 にしない (R1 #5)
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 1, H(), [{ line_key: '1', sku_code: 'sku-a', qty: null }]), /qty/);
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 1, H(), [{ sku_code: 'sku-a', qty: 1 }]), /line_key/);
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-1', 1, H(), [{ line_key: '1', sku_code: 'sku-a', qty: 1 }, { line_key: '1', sku_code: 'sku-a', qty: 2 }]), /duplicate line_key|second time/);
  assert.equal(await num(`select count(*) as n from core.orders where mall in ('yahoo') or mall_order_no = 'AMZ-1'`), 0);
});
await t('状態は status_source から対応表で決まる (NE 50 → shipped、未登録 → unknown)。会社違いの SKU / 出品は付かない (unresolved に落ちる)。金額に負は入らない', async () => {
  assert.equal(await applyOrder('amazon', 'jp', 'AMZ-1', 1, H({ status: undefined, source_system: 'ne', status_source: '50' }), [{ line_key: '1', listing_code: 'pr_SKU-A', qty: 1 }]), 'applied');
  const o = await one(`select status, status_source from core.orders where mall_order_no = 'AMZ-1'`);
  assert.equal(o.status, 'shipped'); assert.equal(o.status_source, '50');
  assert.equal((await one(`select listing_id from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'AMZ-1'`)).listing_id, lstAmz);
  assert.equal(await applyOrder('amazon', 'jp', 'AMZ-2', 1, H({ status: undefined, source_system: 'ne', status_source: '99' }), [{ line_key: '1', sku_code: 'other-1', qty: 1 }]), 'applied');
  const l = await one(`select sku_id, unresolved_code from core.order_lines o join core.orders x on x.order_id = o.order_id where x.mall_order_no = 'AMZ-2'`);
  assert.equal(l.sku_id, null); assert.equal(l.unresolved_code, 'other-1');   // 他社の SKU コードは当たらない (会社で絞る)
  assert.equal((await one(`select status from core.orders where mall_order_no = 'AMZ-2'`)).status, 'unknown');
  // モール API の状態はモール名で対応表を引く (R1 #6)
  // ('amazon', 'Shipped') は 0018 (D5b-2) が入れている。ここで足すと重複になる
  assert.equal(await applyOrder('amazon', 'jp', 'AMZ-4', 1, H({ status: undefined, source_system: 'mall_api', status_source: 'Shipped' }), []), 'applied');
  assert.equal((await one(`select status from core.orders where mall_order_no = 'AMZ-4'`)).status, 'shipped');
  await rejects(() => applyOrder('amazon', 'jp', 'AMZ-3', 1, H({ total_amount_jpy: -1 }), []), /total_amount_jpy|violates check/);
  await rejects(() => pg.query(`insert into core.order_lines (company_id, order_id, line_key, sku_id, qty, received_batch_seq) select $1, order_id, 'x', $2, 1, 1 from core.orders where mall_order_no = 'AMZ-1'`, [co, skuOther]), /foreign key|violates/i);
});

console.log('apply_shipment_batch (出荷 = NE の伝票) と注文との結び');
await t('🚨 伝票の適用: NE 50 → shipped、出荷確定日は JST、明細の SKU 解決、同じ受注番号の楽天注文に結ばれる', async () => {
  assert.equal(await applyOrder('rakuten', 'main', '123456-20260914-0001', 1, H({ content_hash: 'rk2' }), [{ line_key: '1', sku_code: 'sku-a', qty: 1 }]), 'applied');
  assert.equal(await applyShip('S-1', 1, SH(), [{ line_no: '1', sku_code: 'SKU-A', qty: 1, allocated_qty: 1 }, { line_no: '2', sku_code: 'zzz', qty: 2 }]), 'applied');
  const s = await one(`select s.shipment_id, s.status, s.ship_date_jst::text d, s.order_id, o.mall_order_no from core.shipments s left join core.orders o on o.order_id = s.order_id where s.ne_slip_no = 'S-1'`);
  assert.equal(s.status, 'shipped'); assert.equal(s.d, '2026-09-14'); assert.equal(s.mall_order_no, '123456-20260914-0001');
  const ls = (await pg.query(`select line_no, sku_id, unresolved_code, allocated_qty from core.shipment_lines where shipment_id = $1 order by line_no`, [s.shipment_id])).rows;
  assert.equal(ls[0].sku_id, skuA); assert.equal(ls[0].allocated_qty, 1); assert.equal(ls[1].sku_id, null); assert.equal(ls[1].unresolved_code, 'zzz');
});
await t('🚨 注文が未着の伝票は order_id が null のまま (v_shipments_unlinked に理由つきで出る) → 注文が届いたら relink_shipments で結ばれる。Yahoo は接頭辞 b-faith01- で結ぶ', async () => {
  assert.equal(await applyShip('S-2', 1, SH({ shop_code: '4', ne_order_no: '503-0000001-0000001', content_hash: 's2' }), [{ line_no: '1', sku_code: 'sku-a', qty: 1 }]), 'applied');
  assert.equal(await applyShip('S-3', 1, SH({ shop_code: '2', ne_order_no: '12345678', content_hash: 's3' }), [{ line_no: '1', sku_code: 'sku-a', qty: 1 }]), 'applied');
  assert.equal(await applyShip('S-4', 1, SH({ shop_code: '15', ne_order_no: 'FBA-1', content_hash: 's4', delivery_method_code: null, delivery_method_name: null }), []), 'applied');
  assert.equal(await applyShip('S-5', 1, SH({ shop_code: null, ne_order_no: null, content_hash: 's5', shipped_at: null }), []), 'applied');
  const u = Object.fromEntries((await pg.query(`select ne_slip_no, reason from mart.v_shipments_unlinked order by ne_slip_no`)).rows.map((r) => [r.ne_slip_no, r.reason]));
  assert.deepEqual(u, { 'S-2': 'order_missing', 'S-3': 'order_missing', 'S-4': 'shop_not_linked', 'S-5': 'no_shop' });
  assert.equal(await applyOrder('amazon', 'jp', '503-0000001-0000001', 1, H({ content_hash: 'amz3' }), []), 'applied');
  assert.equal(Number((await one(`select core.relink_shipments($1::smallint) as n`, [co])).n), 1);
  assert.equal((await one(`select o.mall_order_no from core.shipments s join core.orders o on o.order_id = s.order_id where s.ne_slip_no = 'S-2'`)).mall_order_no, '503-0000001-0000001');
  // Yahoo (0031 で有効): 注文が届いた後の「same」の再送では結ばない (updated_at も order_id も動かない) → relink で接頭辞つきで結ばれる
  assert.equal(await applyOrder('yahoo', 'main', 'b-faith01-12345678', 1, H({ content_hash: 'y1' }), []), 'applied');
  const s3 = await one(`select updated_at::text as u, order_id from core.shipments where ne_slip_no = 'S-3'`);
  assert.equal(await applyShip('S-3', 2, SH({ shop_code: '2', ne_order_no: '12345678', content_hash: 's3' }), [{ line_no: '1', sku_code: 'sku-a', qty: 1 }]), 'same');
  const s3b = await one(`select updated_at::text as u, order_id, received_batch_seq from core.shipments where ne_slip_no = 'S-3'`);
  assert.equal(s3b.u, s3.u); assert.equal(s3b.order_id, null); assert.equal(Number(s3b.received_batch_seq), 2);   // R1 #7
  assert.equal(Number((await one(`select core.relink_shipments($1::smallint) as n`, [co])).n), 1);
  assert.equal((await one(`select o.mall_order_no from core.shipments s join core.orders o on o.order_id = s.order_id where s.ne_slip_no = 'S-3'`)).mall_order_no, 'b-faith01-12345678');
  assert.equal(Number((await one(`select core.relink_shipments($1::smallint) as n`, [co])).n), 0);
  assert.equal(await num(`select count(*) as n from mart.v_shipments_unlinked`), 2);   // S-4 (対象外の店舗) と S-5 (店舗なし)
  // 🚨 受注番号を未着の番号に訂正したら、古い結びは残さない (order_id = null → unlinked に出る)。届いたら relink で結ばれる (R1 #4)
  assert.equal(await applyShip('S-2', 2, SH({ shop_code: '4', ne_order_no: '503-9999999-9999999', content_hash: 's2b' }), [{ line_no: '1', sku_code: 'sku-a', qty: 1 }]), 'applied');
  assert.equal((await one(`select order_id from core.shipments where ne_slip_no = 'S-2'`)).order_id, null);
  assert.equal((await one(`select reason from mart.v_shipments_unlinked where ne_slip_no = 'S-2'`)).reason, 'order_missing');
  assert.equal(await applyOrder('amazon', 'jp', '503-9999999-9999999', 1, H({ content_hash: 'amz9' }), []), 'applied');
  assert.equal(Number((await one(`select core.relink_shipments($1::smallint) as n`, [co])).n), 1);
  assert.equal((await one(`select o.mall_order_no from core.shipments s join core.orders o on o.order_id = s.order_id where s.ne_slip_no = 'S-2'`)).mall_order_no, '503-9999999-9999999');
});
await t('伝票の same / stale / 内容違いの置換 (明細が入れ替わる)。取消の伝票は status=cancelled。出荷確定日と shipped_at はどちらも有るか無いか', async () => {
  assert.equal(await applyShip('S-1', 2, SH(), [{ line_no: '1', sku_code: 'SKU-A', qty: 1, allocated_qty: 1 }, { line_no: '2', sku_code: 'zzz', qty: 2 }]), 'same');
  assert.equal(Number((await one(`select received_batch_seq from core.shipments where ne_slip_no = 'S-1'`)).received_batch_seq), 2);
  assert.equal(await applyShip('S-1', 1, SH({ content_hash: 'old' }), []), 'stale');
  await rejects(() => applyShip('S-1', 2, SH({ content_hash: 'diff' }), []), /already applied with different content/);
  // 🚨 ヘッダ固定 (SH() のまま)・明細だけ変更: 同じ世代なら例外、次の世代なら applied で数量が変わる、空集合なら全部 removed_at (R2)
  await rejects(() => applyShip('S-1', 2, SH(), [{ line_no: '1', sku_code: 'SKU-A', qty: 5, allocated_qty: 1 }, { line_no: '2', sku_code: 'zzz', qty: 2 }]), /already applied with different content/);
  assert.equal(await applyShip('S-1', 3, SH(), [{ line_no: '1', sku_code: 'SKU-A', qty: 5, allocated_qty: 1 }, { line_no: '2', sku_code: 'zzz', qty: 2 }]), 'applied');
  assert.equal((await one(`select l.qty from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'S-1' and l.line_no = '1'`)).qty, 5);
  assert.equal(await applyShip('S-1', 4, SH(), []), 'applied');
  assert.equal(await num(`select count(*) as n from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'S-1' and l.removed_at is null`), 0);
  assert.equal(await applyShip('S-1', 5, SH({ content_hash: 's1b', is_cancelled: true, cancelled_at: '2026-09-14T04:00:00Z' }), [{ line_no: '1', sku_code: 'sku-a', qty: 1, is_cancelled: true }]), 'applied');
  const s = await one(`select status, is_cancelled, (select count(*) from core.shipment_lines l where l.shipment_id = s.shipment_id and l.removed_at is null) as n, (select count(*) from core.shipment_lines l where l.shipment_id = s.shipment_id) as total from core.shipments s where ne_slip_no = 'S-1'`);
  assert.equal(s.status, 'cancelled'); assert.equal(s.is_cancelled, true); assert.equal(Number(s.n), 1); assert.equal(Number(s.total), 2);   // 行 '2' は removed_at で残る
  await rejects(() => applyShip('S-1', 6, SH({ content_hash: 's1c' }), [{ line_no: '1', sku_code: 'sku-a' }]), /qty/);
  await rejects(() => pg.query(`insert into core.shipments (company_id, ne_slip_no, status, ship_date_jst, received_batch_seq, source_updated_at, transform_version, content_hash) values ($1, 'bad', 'shipped', current_date, 1, now(), 'v1', 'h')`, [co]), /ck_shipments_ship_date/);
  await rejects(() => pg.query(`insert into core.shipments (company_id, ne_slip_no, shop_code, status, received_batch_seq, source_updated_at, transform_version, content_hash) values ($1, 'bad2', '99', 'new', 1, now(), 'v1', 'h')`, [co]), /foreign key|violates/i);   // 未知の店舗コード
});
await t('🚨 在庫イベント (exact) を出荷明細に結べる。会社違いは付かない。イベントが付いた後もヘッダの更新・明細の入れ替えができる (行は消さないので参照が壊れない)', async () => {
  const sl = await one(`select l.shipment_line_id from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'S-2' and l.removed_at is null`);
  await pg.query(`insert into events.inventory_events (company_id, occurred_at, actor_type, source_system, idempotency_key, sku_id, qty_delta, shipment_line_id) values ($1, now(), 'system', 'packing', 'packing:pk_pack_events:1', $2, -1, $3)`, [co, skuA, sl.shipment_line_id]);
  await rejects(() => pg.query(`insert into events.inventory_events (company_id, occurred_at, actor_type, source_system, idempotency_key, sku_id, qty_delta, shipment_line_id) values ($1, now(), 'system', 'packing', 'packing:pk_pack_events:2', $2, -1, $3)`, [other, skuOther, sl.shipment_line_id]), /foreign key|violates/i);
  // 送り状番号だけ変わった次の世代 (明細は同じ) → applied、明細の id は同じまま (R1 #2)
  assert.equal(await applyShip('S-2', 3, SH({ shop_code: '4', ne_order_no: '503-9999999-9999999', tracking_no: 'T2', content_hash: 's2c' }), [{ line_no: '1', sku_code: 'sku-a', qty: 1 }]), 'applied');
  assert.equal((await one(`select l.shipment_line_id from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id where s.ne_slip_no = 'S-2' and l.removed_at is null`)).shipment_line_id, sl.shipment_line_id);
  // 明細が集合から外れても行は残り (removed_at)、イベントの参照は生きている。また現れたら同じ id に戻る
  assert.equal(await applyShip('S-2', 4, SH({ shop_code: '4', ne_order_no: '503-9999999-9999999', tracking_no: 'T2', content_hash: 's2d' }), [{ line_no: '9', sku_code: 'bbb-2', qty: 1 }]), 'applied');
  const gone = await one(`select removed_at from core.shipment_lines where shipment_line_id = $1`, [sl.shipment_line_id]);
  assert.ok(gone.removed_at);
  assert.equal((await one(`select shipment_line_id from events.inventory_events where idempotency_key = 'packing:pk_pack_events:1'`)).shipment_line_id, sl.shipment_line_id);
  assert.equal(await applyShip('S-2', 5, SH({ shop_code: '4', ne_order_no: '503-9999999-9999999', tracking_no: 'T2', content_hash: 's2e' }), [{ line_no: '1', sku_code: 'sku-a', qty: 1 }, { line_no: '9', sku_code: 'bbb-2', qty: 1 }]), 'applied');
  assert.equal((await one(`select removed_at from core.shipment_lines where shipment_line_id = $1`, [sl.shipment_line_id])).removed_at, null);
});

console.log('mart.v_shipments_daily (旧 f_shipments_daily と同じ式)');
await t('🚨 slips は出荷確定日のある伝票の数 (取消を含む)、cancelled_slips は内数。delivery_name は出荷確定日が一番新しい伝票の名称 (同着は伝票番号)。名前が無ければ (未設定)。確定日の無い伝票は数えない', async () => {
  // 9/20 に店舗 1・配送 28 で 3 通 (うち 1 通 取消)。名前: D-1 (古い) 'Z-old' / D-2 と D-3 は同時刻で 'Z-name' と 'A-name' → 伝票番号の大きい D-3 の 'A-name' が勝つ (MAX や辞書順なら 'Z-name' になる)
  await applyShip('D-1', 1, SH({ ne_order_no: 'x1', shipped_at: '2026-09-19T20:00:00Z', delivery_method_name: 'Z-old', content_hash: 'd1' }), []);
  await applyShip('D-2', 1, SH({ ne_order_no: 'x2', shipped_at: '2026-09-20T01:00:00Z', delivery_method_name: 'Z-name', content_hash: 'd2' }), []);
  await applyShip('D-3', 1, SH({ ne_order_no: 'x3', shipped_at: '2026-09-20T01:00:00Z', delivery_method_name: 'A-name', is_cancelled: true, content_hash: 'd3' }), []);
  await applyShip('D-4', 1, SH({ ne_order_no: 'x4', shipped_at: null, delivery_method_name: 'Z-unshipped', content_hash: 'd4' }), []);   // 未出荷 = 数えない・名前にも使わない
  await applyShip('D-5', 1, SH({ ne_order_no: 'x5', shipped_at: '2026-09-20T02:00:00Z', delivery_method_code: null, delivery_method_name: null, content_hash: 'd5' }), []);
  const r = await one(`select slips, cancelled_slips, delivery_name from mart.v_shipments_daily where ship_date = date '2026-09-20' and shop_code = '1' and delivery_id = '28'`);
  assert.equal(r.slips, 3); assert.equal(r.cancelled_slips, 1); assert.equal(r.delivery_name, 'A-name');
  const r2 = await one(`select slips, delivery_name, delivery_id from mart.v_shipments_daily where ship_date = date '2026-09-20' and shop_code = '1' and delivery_id = ''`);
  assert.equal(r2.slips, 1); assert.equal(r2.delivery_name, '(未設定)');
  assert.equal(await num(`select count(*) as n from mart.v_shipments_daily where ship_date = date '2026-09-19'`), 0);   // 9/19 20:00Z = 9/20 JST
  // 名前は「全期間・店舗横断」で一番新しい伝票のもの: 別の店舗・別の日に同じ配送 28 で 'B-newest' が出荷されたら、9/20 の行の名前も 'B-newest' になる (旧 build と同じ)
  await applyShip('D-6', 1, SH({ shop_code: '4', ne_order_no: 'x6', shipped_at: '2026-09-21T05:00:00Z', delivery_method_name: 'B-newest', content_hash: 'd6' }), []);
  assert.equal((await one(`select delivery_name from mart.v_shipments_daily where ship_date = date '2026-09-20' and shop_code = '1' and delivery_id = '28'`)).delivery_name, 'B-newest');
  assert.equal((await one(`select delivery_name, slips from mart.v_shipments_daily where ship_date = date '2026-09-21' and shop_code = '4' and delivery_id = '28'`)).delivery_name, 'B-newest');
  assert.equal(await num(`select coalesce(sum(slips), 0) as n from mart.v_shipments_daily`), await num(`select count(*) as n from core.shipments where ship_date_jst is not null`));
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
