#!/usr/bin/env node
/**
 * test-company-db-domains.mjs — 0010 (土台) + 0011 (在庫) の受入試験 (Company DB 構想 08)
 *
 * PGlite で 0001〜0011 を流し、DDL の「歯止め」を実際の操作で確かめる:
 *   許可リスト (未知の列・入れ子・配列・volatile 列を拒む) / 完走した取得だけを読む現在庫 / 整理で鍵ごとの最新が残る /
 *   日次の building → complete (途中の日は見えない・complete 後は変えられない) / 会社 × scope の完走日 / ロケーションの会社一致 /
 *   0012〜0015 (受注・財務・発注・売上) の試験はそれぞれの PR で足す。
 * 🚨 2 接続の並行試験 (完了処理と INSERT の競合、policy の重複) は PGlite では書けない → 本番 Postgres で手で確かめる (08 §7.6)
 * 実行: node scripts/test-company-db-domains.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';

const pg = new PGlite();
const db = pgliteAdapter(pg);
const applied = await applyMigrations(db, { log: () => {} });
console.log(`migrations applied: ${JSON.stringify(applied).slice(0, 80)}`);

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.message || e)); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error('wrong error: ' + threw.message); };
const one = async (sql, params = []) => (await pg.query(sql, params)).rows[0];
const co = (await one(`select company_id from core.companies order by company_id limit 1`)).company_id;
const wh = await one(`select warehouse_id from core.warehouses order by warehouse_id limit 1`);
await pg.query(`insert into core.products (company_id, name) values ($1, '見本A'), ($1, '見本B')`, [co]);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'sku-' || product_id, name from core.products`);
await pg.query(`insert into core.suppliers (company_id, code, name) values ($1, 'SUP1', '見本仕入先')`, [co]);
const sku = await one(`select sku_id from core.skus order by sku_id limit 1`);
const sku2 = await one(`select sku_id from core.skus order by sku_id offset 1 limit 1`);
const other = (await one(`select max(company_id)::smallint + 1 as c from core.companies`)).c;
await pg.query(`insert into core.companies (company_id, name, kind) values ($1, 'other', 'subsidiary')`, [other]);
await pg.query(`insert into core.products (company_id, name) values ($1, '他社品')`, [other]);
const skuOther = await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'other-1', name from core.products where company_id = $1 returning sku_id`, [other]);
const H = 3600e3;
const at = (hoursAgo) => new Date(Date.now() - hoursAgo * H).toISOString();
const run = (id, scope, hoursAgo, status, complete) => pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, finished_at, status, complete) values ($1, 'logizard', 'inventory', $2, $3, $3, $4, $5)`, [id, scope, at(hoursAgo), status, complete]);
await run('run-1', 'main', 3, 'success', true); await run('run-2', 'main', 2, 'failed', false); await run('run-3', 'main', 1, 'success', true); await run('run-o1', 'other', 3, 'success', true);

await t('新しい raw 表は logizard だけ (受注・財務の raw は持ち込まない)', async () => {
  const r = await one(`select count(*)::int n from information_schema.tables where table_schema = 'raw' and not (table_name like any (array['ne_products%','ne_set_products%','amazon_listing%','amazon_listings%','amazon_catalog%','rakuten_items%','rakuten_inventory%','yahoo_item%','aupay_items%','qoo10_items%','linegift_products%','logizard_products%']))`);
  if (r.n !== 2) throw new Error('unexpected raw tables: ' + r.n);
});

await t('🚨 許可リスト (logizard): 未知のキー / 入れ子 / 配列 / 大文字小文字違い / volatile 列は入らない', async () => {
  for (const [h, p] of [['h-unk', '{"商品ID":"1","mail_address":"x@example.com"}'], ['h-nest', '{"商品ID":"1","ロケ":{"x":"y"}}'], ['h-arr', '[{"商品ID":"1"}]'], ['h-arr2', '{"商品ID":"1","在庫数":[1]}'], ['h-vol', '{"商品ID":"1","在庫日":"20260913"}'], ['h-sync', '{"商品ID":"1","synced_at":"x"}']]) {
    await rejects(() => pg.query(`insert into raw.logizard_inventory_contents (content_hash, payload) values ($1, $2)`, [h, p]), /ck_logizard_inventory_payload/);
  }
  await pg.query(`insert into raw.logizard_inventory_contents (content_hash, payload) values ('h-ok', '{"商品ID":"1","ブロック略称":"P3FA","ロケ":"001-001-01","品質区分名":"良品","在庫数":3,"引当数":0,"有効期限":null}')`);
});

await t('配列の鍵 (array_keys): 許可された鍵だけの object の配列なら入る (見本ソース)', async () => {
  await pg.query(`select raw.ensure_source('zz_test_orders', array['order_no','seller_sku','qty','status'], array['items'])`);
  await pg.query(`insert into raw.zz_test_orders_contents (content_hash, payload) values ('a1', '{"order_no":"503-1","status":"Shipped","items":[{"seller_sku":"x","qty":1,"status":"Shipped"},{"seller_sku":"x","qty":1,"status":"Cancelled"}]}')`);
  await rejects(() => pg.query(`insert into raw.zz_test_orders_contents (content_hash, payload) values ('a2', '{"order_no":"503-1","items":[{"seller_sku":"x","buyer_name":"T"}]}')`), /ck_zz_test_orders_payload/);
  await rejects(() => pg.query(`insert into raw.zz_test_orders_contents (content_hash, payload) values ('a3', '{"order_no":"503-1","items":[[1,2]]}')`), /ck_zz_test_orders_payload/);
  await rejects(() => pg.query(`insert into raw.zz_test_orders_contents (content_hash, payload) values ('a5', '{"Order_No":"503-1"}')`), /ck_zz_test_orders_payload/);
});

await t('raw の新表は append-only / ingest_runs に skipped が入る / 観測の scope は run の scope と一致 (複合 FK)', async () => {
  await rejects(() => pg.query(`update raw.logizard_inventory_contents set payload = '{}' where content_hash = 'h-ok'`), /append-only|reject|変更|禁止/i);
  await run('run-skip', 'main', 0, 'skipped', false);
  await rejects(() => pg.query(`insert into raw.logizard_inventory_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('run-1', 'other', 'k', 'h-ok', 'ok', now())`), /foreign key|violates/i);
});

const ins = async (runId, scope, key, hash, status, when) => {
  if (hash) await pg.query(`insert into raw.logizard_inventory_contents (content_hash, payload) values ($1, $2) on conflict do nothing`, [hash, JSON.stringify({ '商品ID': key.split('|')[0], 'ブロック略称': key.split('|')[1], 'ロケ': key.split('|')[2], '品質区分名': '良品', '在庫数': Number(hash.slice(-1)), '引当数': 0 })]);
  await pg.query(`insert into raw.logizard_inventory_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ($1, $2, $3, $4, $5, $6)`, [runId, scope, key, hash, status, when]);
};
const KA = 'A|P3FA|001-001-01|良品|-|-', KB = 'B|R1FA|002-001-01|良品|-|-', KC = 'C|P3FA|003-001-01|良品|-|-';

await t('🚨 いまの在庫: 完走した取得の観測だけを observed_at の順で読む。失敗した取得は無視、消えた行は出ない、scope は別、古い再ロードは最新にならない', async () => {
  await ins('run-1', 'main', KA, 'hA5', 'ok', at(3));
  await ins('run-1', 'main', KB, 'hB3', 'ok', at(3));
  await ins('run-o1', 'other', KA, 'hA7', 'ok', at(3));
  await ins('run-2', 'main', KA, 'hA1', 'ok', at(2));
  await ins('run-2', 'main', KC, 'hC2', 'ok', at(2));
  await ins('run-3', 'main', KA, 'hA8', 'ok', at(1));
  await ins('run-3', 'main', KB, null, 'not_found', at(1));
  const rows = (await pg.query(`select scope_key, line_key, qty from mart.v_warehouse_stock_current order by scope_key, line_key`)).rows;
  if (rows.length !== 2 || rows[0].scope_key !== 'main' || rows[0].qty !== 8 || rows[1].scope_key !== 'other' || rows[1].qty !== 7) throw new Error(JSON.stringify(rows));
  await run('run-old', 'main', 0, 'success', true);
  await ins('run-old', 'main', KA, 'hA2', 'ok', at(10));
  const r2 = await one(`select qty from mart.v_warehouse_stock_current where scope_key = 'main' and line_key = $1`, [KA]);
  if (r2.qty !== 8) throw new Error('reloaded old data became latest: ' + JSON.stringify(r2));
});

await t('🚨 保持期間の整理: 置き換えの根拠は完走した取得の状態観測だけ (失敗した run の新しい観測では消えない)。鍵ごとの最新は残る。trigger は元に戻る', async () => {
  await run('run-base', 'main', 24 * 60, 'success', true);
  await run('run-fail', 'main', 2, 'failed', false);
  const KD = 'D|P3FA|009-001-01|良品|-|-', KE = 'E|P3FA|009-002-01|良品|-|-', KF = 'F|P3FA|009-003-01|良品|-|-';
  await ins('run-base', 'main', KD, 'hD4', 'ok', at(24 * 60));   // 60 日前から変わらない
  await ins('run-base', 'main', KE, 'hE6', 'ok', at(24 * 60));   // 60 日前の値 → 3 時間前に変わった (完走)
  await ins('run-1', 'main', KE, 'hE9', 'ok', at(3));
  await ins('run-base', 'main', KF, 'hF5', 'ok', at(24 * 60));   // 60 日前の値 → 2 時間前に失敗した run で 9 (R3 #1 の筋書き)
  await ins('run-fail', 'main', KF, 'hF9', 'ok', at(2));
  const before = await one(`select tgenabled from pg_trigger where tgrelid = 'raw.logizard_inventory_observations'::regclass and tgfoid = 'core.reject_mutation'::regproc limit 1`);
  const purged = await one(`select raw.purge_superseded_observations('logizard_inventory', 30) as n`);
  if (purged.n !== 1) throw new Error('purged=' + purged.n);
  const d = await one(`select qty from mart.v_warehouse_stock_current where line_key = $1`, [KD]);
  if (!d || d.qty !== 4) throw new Error('unchanged stock vanished: ' + JSON.stringify(d));
  const f = await one(`select qty from mart.v_warehouse_stock_current where line_key = $1`, [KF]);
  if (!f || f.qty !== 5) throw new Error('stock superseded only by a failed run vanished: ' + JSON.stringify(f));
  const e = await one(`select qty from mart.v_warehouse_stock_current where line_key = $1`, [KE]);
  if (e.qty !== 9) throw new Error(JSON.stringify(e));
  if ((await one(`select count(*)::int n from raw.logizard_inventory_contents where content_hash = 'hE6'`)).n !== 0) throw new Error('orphan content not removed');
  const after = await one(`select tgenabled from pg_trigger where tgrelid = 'raw.logizard_inventory_observations'::regclass and tgfoid = 'core.reject_mutation'::regproc limit 1`);
  if (after.tgenabled !== before.tgenabled) throw new Error(`trigger state changed ${before.tgenabled} -> ${after.tgenabled}`);
  await rejects(() => pg.query(`delete from raw.logizard_inventory_observations where observation_id = (select min(observation_id) from raw.logizard_inventory_observations)`), /append-only|reject|変更|禁止/i);   // 失敗した INSERT が採番を消費するので id=1 とは限らない
});

await t('月パーティション関数が新表に効く + 既存関数も同じ結果', async () => {
  const r = await one(`select snapshots.ensure_month_partitions_for(array['warehouse_stock_daily','sku_stock_daily'], date '2026-09-01', date '2026-10-01') as n`);
  if (r.n !== 4) throw new Error('created=' + r.n);
  const r2 = await one(`select snapshots.ensure_month_partitions(date '2026-09-01', date '2026-09-01') as n`);
  if (r2.n !== 3) throw new Error('existing fn created=' + r2.n);
});

await t('core.ensure_location: code = ブロック-ロケ、同じロケは 1 行、R* は iroha、別会社が同じ code を取ろうとすると例外', async () => {
  const a = await one(`select core.ensure_location($1::smallint, $2::smallint, 'R1FA', '001-001-01') id`, [co, wh.warehouse_id]);
  const b = await one(`select core.ensure_location($1::smallint, $2::smallint, 'R1FA', '001-001-01') id`, [co, wh.warehouse_id]);
  const c2 = await one(`select core.ensure_location($1::smallint, $2::smallint, 'P3FA', '001-001-01') id`, [co, wh.warehouse_id]);
  if (a.id !== b.id || a.id === c2.id) throw new Error('dedupe wrong');
  const c = await one(`select code, building from core.locations where location_id = $1`, [a.id]);
  if (c.code !== 'R1FA-001-001-01' || c.building !== 'iroha') throw new Error(JSON.stringify(c));
  await rejects(() => pg.query(`select core.ensure_location($1::smallint, $2::smallint, 'R1FA', '001-001-01')`, [other, wh.warehouse_id]), /belongs to company/);
});

const capDay = (d, source, scope, company, status, runId) => pg.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id) values ($1, $2, $3, $4, $5, $6)`, [d, source, scope, company, status, runId]);
const complete = (d, source, scope) => pg.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1 and source = $2 and scope_key = $3`, [d, source, scope]);
const row = (d, scope, runId, code, company, s, q) => pg.query(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, allocated_qty, captured_at, ingest_run_id) values ($1, 'logizard', $2, $3, $4, $5, $6, 0, now(), $7)`, [d, scope, code, company, s, q, runId]);

await t('🚨 日次: building の間だけ INSERT/UPDATE/DELETE できる。complete 後は変えられない (保守は set local で通す)。partial・記録なし・別 run は書けない', async () => {
  await capDay('2026-09-11', 'logizard', 'main', co, 'building', 'run-1');
  await capDay('2026-09-13', 'logizard', 'main', co, 'partial', 'run-2');
  await row('2026-09-11', 'main', 'run-1', 'A', co, sku.sku_id, 10);
  await row('2026-09-11', 'main', 'run-1', 'B', co, sku.sku_id, 5);
  await row('2026-09-11', 'main', 'run-1', 'C', co, sku2.sku_id, 7);
  await rejects(() => row('2026-09-13', 'main', 'run-2', 'A', co, sku.sku_id, 1), /partial|building/);
  await rejects(() => row('2026-09-14', 'main', 'run-3', 'A', co, sku.sku_id, 1), /foreign key|violates|absent|building/i);
  await rejects(() => row('2026-09-11', 'main', 'run-3', 'A', co, sku.sku_id, 1), /foreign key|violates|absent|building/i);
  await rejects(() => row('2026-09-11', 'main', 'run-1', 'X', other, skuOther.sku_id, 1), /foreign key|violates/i);   // 会社違い (capture 行は co)
  let v = await one(`select warehouse_as_of, warehouse_qty from mart.v_sku_stock where sku_id = $1`, [sku.sku_id]);
  if (v.warehouse_as_of !== null || v.warehouse_qty !== null) throw new Error('building day visible: ' + JSON.stringify(v));
  await complete('2026-09-11', 'logizard', 'main');
  await rejects(() => row('2026-09-11', 'main', 'run-1', 'Z', co, sku.sku_id, 1), /complete|building/);
  await rejects(() => pg.query(`update snapshots.sku_stock_daily set qty = 999 where snapshot_date = date '2026-09-11' and source_code = 'A'`), /complete|building/);   // R3 #2
  await rejects(() => pg.query(`delete from snapshots.sku_stock_daily where snapshot_date = date '2026-09-11'`), /complete|building/);
  v = await one(`select warehouse_as_of::text d, warehouse_qty, ne_qty from mart.v_sku_stock where sku_id = $1`, [sku.sku_id]);
  if (v.d !== '2026-09-11' || v.warehouse_qty !== 15 || v.ne_qty !== null) throw new Error(JSON.stringify(v));
  // 保守の経路 (set local) では消せる。トランザクションの外では消せない
  await pg.exec(`begin; set local snapshots.maintenance = 'on'; delete from snapshots.sku_stock_daily where snapshot_date = date '2026-09-11' and source_code = 'C'; commit;`);
  await rejects(() => pg.query(`delete from snapshots.sku_stock_daily where snapshot_date = date '2026-09-11' and source_code = 'B'`), /complete|building/);
  await rejects(() => pg.query(`update snapshots.stock_capture_days set status = 'complete' where snapshot_date = date '2026-09-13'`), /ck_stock_capture_days_completed/);
});

await t('🚨 v_sku_stock: 会社 × source × scope 単位で「最新の完走日」。取得していない会社の SKU は null (0 にも他社の日付にもならない)。次の日は消えた SKU が 0', async () => {
  const vo = await one(`select warehouse_as_of, warehouse_qty from mart.v_sku_stock where sku_id = $1`, [skuOther.sku_id]);
  if (vo.warehouse_as_of !== null || vo.warehouse_qty !== null) throw new Error('company without captures got a value: ' + JSON.stringify(vo));   // R3 #3
  await capDay('2026-09-12', 'logizard', 'main', co, 'building', 'run-3');
  await row('2026-09-12', 'main', 'run-3', 'A', co, sku.sku_id, 8);
  await complete('2026-09-12', 'logizard', 'main');
  const v = await one(`select warehouse_as_of::text d, warehouse_qty from mart.v_sku_stock where sku_id = $1`, [sku.sku_id]);
  const v2 = await one(`select warehouse_qty from mart.v_sku_stock where sku_id = $1`, [sku2.sku_id]);
  if (v.d !== '2026-09-12' || v.warehouse_qty !== 8 || v2.warehouse_qty !== 0) throw new Error(JSON.stringify([v, v2]));
});

await t('external_ids に order / shipment / purchase_order が登録できる', async () => {
  await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values ($1, 'order', 1, 'rakuten', 'order_no', '123456-20260913-0001', 'exact', 'system')`, [co]);
});

// 0012〜0015 (受注・財務・発注・売上) の試験は各 PR で足す。草案版は AI_reference _raw/08_DDL草案_PGlite試験_20260913.mjs

