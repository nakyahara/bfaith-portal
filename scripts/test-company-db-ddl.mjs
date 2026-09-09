/**
 * test-company-db-ddl.mjs — Company DB (Postgres) の DDL とマイグレーション実行器の試験
 *
 * PGlite (WASM の Postgres、devDependency) で db/company/migrations を全部流し、
 * 03 §10 の DDL セルフチェックと 06 §11 の決めごとを機械で固定する。本物の Postgres は要らない。
 * 使い方: node scripts/test-company-db-ddl.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, migrationStatus, listMigrationFiles, pgliteAdapter, checksumOf, DEFAULT_DIR } from './company-db/migrate.mjs';
import { normSku } from '../lib/sku-norm.js';

let passed = 0;
async function ta(name, fn) {
  try { await fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}
const quiet = () => {};
const rejects = (fn, re) => assert.rejects(fn, re);

const pglite = new PGlite();
const db = pgliteAdapter(pglite);
const q = async (sql, params) => (await db.query(sql, params)).rows;

console.log('マイグレーション実行器');

await ta('[!] 全部流れて記録される。もう一度流しても何も起きない (冪等)', async () => {
  const files = listMigrationFiles(DEFAULT_DIR);
  const r1 = await applyMigrations(db, { log: quiet });
  assert.equal(r1.applied.length, files.length);
  const r2 = await applyMigrations(db, { log: quiet });
  assert.equal(r2.applied.length, 0);
  assert.equal(r2.skipped.length, files.length);
  const st = await migrationStatus(db);
  assert.ok(st.every((s) => s.state === 'applied'), JSON.stringify(st));
});

await ta('[!] 適用済みファイルを書き換えると止まる (checksum)。適用済みは書き換えず次の番号で直す', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-mig-'));
  for (const f of fs.readdirSync(DEFAULT_DIR)) fs.copyFileSync(path.join(DEFAULT_DIR, f), path.join(tmp, f));
  const first = fs.readdirSync(tmp).sort()[0];
  fs.appendFileSync(path.join(tmp, first), '\n-- tampered\n');
  await rejects(() => applyMigrations(db, { dir: tmp, log: quiet }), /checksum 不一致/);
  const st = await migrationStatus(db, { dir: tmp });
  assert.equal(st[0].state, 'CHANGED');
  fs.rmSync(tmp, { recursive: true, force: true });
});

await ta('[!] DB に記録があるのにファイルが無ければ止まる (古い checkout で流さない)。番号の欠番も止まる', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-mig3-'));
  const files = fs.readdirSync(DEFAULT_DIR).sort();
  for (const f of files.slice(0, -1)) fs.copyFileSync(path.join(DEFAULT_DIR, f), path.join(tmp, f));   // 最後の 1 本が無い
  await rejects(() => applyMigrations(db, { dir: tmp, log: quiet }), /適用記録があるのにファイルが無い/);
  fs.rmSync(tmp, { recursive: true, force: true });
  const gap = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-mig4-'));
  fs.writeFileSync(path.join(gap, '0001_a.sql'), 'select 1;\n');
  fs.writeFileSync(path.join(gap, '0003_c.sql'), 'select 1;\n');
  assert.throws(() => listMigrationFiles(gap), /欠番/);
  fs.rmSync(gap, { recursive: true, force: true });
});

await ta('checksum は改行コードの違いを吸収する (CRLF で checkout されても同じ)', () => {
  assert.equal(checksumOf('a\r\nb\r\n'), checksumOf('a\nb\n'));
});

await ta('[!] 途中で失敗したファイルは巻き戻され、前のファイルまでは記録が残る', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-mig2-'));
  fs.writeFileSync(path.join(tmp, '0001_ok.sql'), 'create table public.t_ok (id int primary key);\n');
  fs.writeFileSync(path.join(tmp, '0002_bad.sql'), 'create table public.t_bad (id int primary key);\nselect * from public.no_such_table;\n');
  const p2 = new PGlite(); const d2 = pgliteAdapter(p2);
  await rejects(() => applyMigrations(d2, { dir: tmp, log: quiet }), /0002_bad\.sql で失敗/);
  const rows = (await d2.query("select version from ops.schema_migrations order by version")).rows;
  assert.deepEqual(rows.map((r) => r.version), ['0001']);
  assert.equal((await d2.query("select to_regclass('public.t_bad') as r")).rows[0].r, null);   // 巻き戻っている
  await p2.close();
  fs.rmSync(tmp, { recursive: true, force: true });
});

console.log('\n表とスキーマ (03 §9 + 06 §5.4)');

await ta('[!] 期待する表がすべてある', async () => {
  const rows = await q("select table_schema || '.' || table_name as t from information_schema.tables where table_schema in ('core','raw','snapshots','events','ai','docs','ops') and table_type = 'BASE TABLE'");
  const have = new Set(rows.map((r) => r.t));
  const expect = [
    'core.companies', 'core.workers', 'core.products', 'core.skus', 'core.sku_components', 'core.sku_costs', 'core.suppliers', 'core.supplier_skus',
    'core.listings', 'core.listing_components', 'core.external_ids', 'core.warehouses', 'core.locations', 'core.rule_versions',
    'core.product_physicals', 'core.product_compliance', 'core.product_attribute_observations', 'core.attribute_resolution_rules', 'core.attribute_resolutions',
    'core.catalog_items', 'core.catalog_item_products', 'core.listing_states', 'core.listing_texts', 'core.listing_images',
    'raw.ne_products_contents', 'raw.ne_products_observations', 'raw.amazon_listing_report_contents', 'raw.amazon_listing_report_observations',
    'raw.amazon_catalog_items_contents', 'raw.rakuten_items_contents', 'raw.rakuten_items_observations', 'raw.yahoo_items_contents', 'raw.logizard_products_observations',
    'snapshots.listing_daily', 'snapshots.catalog_asin_daily', 'snapshots.catalog_asin_rank_daily', 'snapshots.listing_weekly',
    'events.price_change_events', 'events.listing_change_events', 'events.sku_attribute_events', 'events.inventory_events', 'events.work_events',
    'ai.decisions', 'ai.decision_reviews', 'ai.autonomy_policies', 'ai.actions', 'ai.action_results', 'ai.decision_outcomes', 'ai.watch_rules', 'ai.catalog_watchlist',
    'docs.documents', 'docs.document_links', 'ops.ingest_runs', 'ops.job_runs', 'ops.schema_migrations',
  ];
  const missing = expect.filter((t) => !have.has(t));
  assert.deepEqual(missing, [], `無い表: ${missing.join(', ')}`);
  const views = await q("select table_schema || '.' || table_name as t from information_schema.views where table_schema = 'mart'");
  assert.deepEqual(views.map((v) => v.t).sort(), ['mart.v_cross_mall_diff', 'mart.v_listing_360', 'mart.v_product_360', 'mart.v_product_dq']);
});

await ta('[!] 03 §10: 円の金額列 (*_jpy) はすべて bigint', async () => {
  const rows = await q("select table_schema, table_name, column_name, data_type from information_schema.columns where column_name like '%\\_jpy%' escape '\\' and table_schema in ('core','snapshots','events','ai','mart') and data_type <> 'bigint'");
  assert.deepEqual(rows, [], JSON.stringify(rows));
  const n = await q("select count(*)::int as n from information_schema.columns where column_name like '%\\_jpy%' escape '\\' and table_schema in ('core','snapshots','events')");
  assert.ok(n[0].n >= 10, String(n[0].n));
});

await ta('[!] 03 §10: append-only 表 (raw / events / 観測 / AI の記録) に updated_at や status が無い', async () => {
  const rows = await q(`select table_schema || '.' || table_name as t, column_name from information_schema.columns
    where ((table_schema = 'raw') or table_schema = 'events' or (table_schema = 'core' and table_name = 'product_attribute_observations')
           or (table_schema = 'ai' and table_name in ('decision_reviews','action_results','decision_outcomes')) or (table_schema = 'ops' and table_name = 'job_runs'))
      and column_name in ('updated_at')`);
  assert.deepEqual(rows, [], JSON.stringify(rows));
});

await ta('[!] 03 §10: Canonical 表 (core の 15 表) に company_id と監査列 (created_at / 誰が) がある', async () => {
  // external_ids と listing_components は「誰が結び付けたか」= resolved_by_* が監査列の役。それ以外は created_by_*
  const canonical = {
    products: 'created_by', skus: 'created_by', sku_components: 'created_by', sku_costs: 'created_by', suppliers: 'created_by', supplier_skus: 'created_by',
    listings: 'created_by', listing_components: 'resolved_by', external_ids: 'resolved_by', workers: 'created_by', locations: 'created_by', warehouses: 'created_by',
    product_physicals: 'created_by', product_compliance: 'created_by',
  };
  for (const [t, by] of Object.entries(canonical)) {
    const cols = new Set((await q("select column_name from information_schema.columns where table_schema = 'core' and table_name = $1", [t])).map((r) => r.column_name));
    for (const c of ['company_id', 'created_at', `${by}_type`, `${by}_id`]) assert.ok(cols.has(c), `core.${t} に ${c} が無い`);
  }
  for (const t of ['products', 'skus', 'listings', 'suppliers', 'supplier_skus', 'workers', 'locations', 'product_physicals', 'product_compliance']) {
    const cols = new Set((await q("select column_name from information_schema.columns where table_schema = 'core' and table_name = $1", [t])).map((r) => r.column_name));
    assert.ok(cols.has('updated_at'), `core.${t} に updated_at が無い`);
  }
});

console.log('\n正規化関数 core.norm_code = lib/sku-norm.js normSku (03 §10)');

await ta('[!] JS と SQL が同じ結果を返す (全角・ダッシュ・空白・半角カナ・NBSP・U+1680・U+0085・U+2028)', async () => {
  const fixtures = [
    'ＡＢＣ－001 ', 'abc−001', ' ab c 002', 'ＡＢＣ　003', 'x y-Z', 'ｶﾀｶﾅ-1', 'ABC—5', 'ABC‐6', 'ABC﹣7', 'ABC－8', 'pr_ABC001-3',
    'ＰＲ＿ａｂｃ００１', 'abc　 def', '﻿abc', 'ABC/DEF+GHI', 'Ａbc.ｄef', '　', '',
    'A B', 'AB', 'A B', 'A B', 'A B', 'A B', 'A B', 'A\tB\nC', 'Ärger-1', 'ＡＢＣ　　x',
  ];
  for (const f of fixtures) {
    const sql = (await q('select core.norm_code($1) as v', [f]))[0].v;
    assert.equal(sql, normSku(f), `${JSON.stringify(f)}: SQL=${JSON.stringify(sql)} JS=${JSON.stringify(normSku(f))}`);
  }
  assert.equal((await q('select core.norm_code(null) as v'))[0].v, null);   // null は null (JS は '')
});

await ta('[!] 正規化列は生成列: 手で入れられず、原文から必ず作られる', async () => {
  await rejects(() => q("insert into core.skus (company_id, sku_kind, code, code_norm, name) values (1, 'exception', 'GEN-1', 'gen-1', 'x')"), /generated|non-DEFAULT/i);
  const r = await q("insert into core.skus (company_id, sku_kind, code, name) values (1, 'exception', 'ＧＥＮ－1 ', 'x') returning code_norm");
  assert.equal(r[0].code_norm, 'gen-1');
});

await ta('core.jst_date は日本時間の日付 (UTC の 9/9 15:00 = JST 9/10)', async () => {
  assert.equal((await q("select core.jst_date('2026-09-09T15:00:00Z'::timestamptz)::text as d"))[0].d, '2026-09-10');
  assert.equal((await q("select core.jst_date('2026-09-09T14:59:59Z'::timestamptz)::text as d"))[0].d, '2026-09-09');
});

console.log('\n商品 → SKU → 販路商品 → 外部 ID の鎖 (06 §5.2)');

let productId, skuId, setSkuId, listingId, listingFbmId, catalogItemId;
await ta('[!] 単品 SKU は product と 1:1 (2 件目は拒む)。セットは構成表。code_norm は unique', async () => {
  productId = (await q("insert into core.products (company_id, display_code, name, brand, unit_count, unit_count_uom) values (1, 'abc001', 'テスト商品', 'テストブランド', 50, '個') returning product_id"))[0].product_id;
  skuId = (await q("insert into core.skus (company_id, product_id, sku_kind, code, name, tax_rate, tax_class) values (1, $1, 'single', 'ABC001', 'テスト商品', 0.10, 'STANDARD_10') returning sku_id", [productId]))[0].sku_id;
  setSkuId = (await q("insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'abc001set3', 'テスト商品 3個セット') returning sku_id"))[0].sku_id;
  await q("insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 3, 'ne')", [setSkuId, skuId]);
  await rejects(() => q("insert into core.skus (company_id, sku_kind, code, name) values (1, 'single', 'nop', 'x')"), /ck_skus_single_has_product/);
  await rejects(() => q("insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'abc001', 'x')", [productId]), /duplicate key/);
  await rejects(() => q("insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'abc001-v2', 'x')", [productId]), /ux_skus_single_product|duplicate key/);
  await rejects(() => q("insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $1, 1, 'ne')", [setSkuId]), /ck_sku_components_not_self/);
});

await ta('[!] 原価は有効期間つき。有効 (valid_to null) は SKU ごとに 1 行だけ', async () => {
  await q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (1, $1, 380, 'ne', 'COMPLETE', '2026-01-01')", [skuId]);
  await rejects(() => q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (1, $1, 400, 'manual', 'OVERRIDDEN', '2026-09-01')", [skuId]), /ux_sku_costs_active|duplicate key/);
  await q("update core.sku_costs set valid_to = '2026-08-31' where sku_id = $1 and valid_to is null", [skuId]);
  await q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (1, $1, 400, 'manual', 'OVERRIDDEN', '2026-09-01')", [skuId]);
  const active = await q('select cost_jpy from core.sku_costs where sku_id = $1 and valid_to is null', [skuId]);
  assert.deepEqual(active.map((r) => Number(r.cost_jpy)), [400]);
  await rejects(() => q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (1, $1, -1, 'ne', 'COMPLETE', '2026-01-01')", [skuId]), /cost_jpy/);
});

await ta('[!] ASIN は catalog_items に置き、listing と 1:N (FBA/FBM 併売)。product / sku への直接 ASIN は拒む', async () => {
  catalogItemId = (await q("insert into core.catalog_items (marketplace_id, asin, package_scope, pack_count, brand) values ('A1VC38T7YXB528', 'B000TEST01', 'multipack', 3, 'テストブランド') returning catalog_item_id"))[0].catalog_item_id;
  await rejects(() => q("insert into core.catalog_items (marketplace_id, asin, package_scope) values ('A1VC38T7YXB528', 'B000NOPACK', 'multipack')"), /ck_catalog_items_pack_count/);
  listingId = (await q("insert into core.listings (company_id, mall, shop_code, listing_code, status, catalog_item_id) values (1, 'amazon', 'S1@A1VC38T7YXB528', 'pr_ABC001-3', 'active', $1) returning listing_id", [catalogItemId]))[0].listing_id;
  listingFbmId = (await q("insert into core.listings (company_id, mall, shop_code, listing_code, status, catalog_item_id) values (1, 'amazon', 'S1@A1VC38T7YXB528', 'pr_ABC001-3-fbm', 'active', $1) returning listing_id", [catalogItemId]))[0].listing_id;
  await q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'imported', 'system')", [listingId, setSkuId]);
  await q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'imported', 'system')", [listingFbmId, setSkuId]);
  await q("insert into core.catalog_item_products (catalog_item_id, product_id, qty, resolution, resolved_by_type) values ($1, $2, 3, 'manual', 'human')", [catalogItemId, productId]);
  await rejects(() => q("insert into core.listings (company_id, mall, shop_code, listing_code) values (1, 'amazon', 'S1@A1VC38T7YXB528', 'PR_abc001-3')"), /duplicate key/);
  await rejects(() => q("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'product', $1, 'amazon', 'asin', 'B000TEST01', 'imported', 'system')", [productId]), /ck_external_ids_no_direct_asin/);
  await rejects(() => q("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'sku', $1, 'amazon', 'asin', 'B000TEST01', 'imported', 'system')", [skuId]), /ck_external_ids_no_direct_asin/);
});

await ta('[!] 外部 ID: 同じ system/kind の値は有効期間内で 1 エンティティだけ。付け替えは valid_to を埋めてから。norm は生成列', async () => {
  const jan = "insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', $2, $3, $4)";
  await q(jan, [productId, '4900000000011', 'imported', 'system']);
  const other = (await q("insert into core.products (company_id, name) values (1, '別の商品') returning product_id"))[0].product_id;
  await rejects(() => q(jan, [other, '4900000000011 ', 'imported', 'system']), /ux_external_ids_active|duplicate key/);   // 末尾空白でも同じ値
  await q("update core.external_ids set valid_to = now() where system = 'jan' and external_norm = '4900000000011' and valid_to is null");
  await q(jan, [other, '4900000000011', 'manual', 'human']);
  const active = await q("select entity_id from core.external_ids where system = 'jan' and external_norm = '4900000000011' and valid_to is null");
  assert.deepEqual(active.map((r) => Number(r.entity_id)), [Number(other)]);
  await q("update core.external_ids set valid_to = now() where system = 'jan' and external_norm = '4900000000011' and valid_to is null");
  await q(jan, [productId, '4900000000011', 'manual', 'human']);
});

await ta('updated_at は更新のたびに進む (trigger)', async () => {
  const before = (await q('select updated_at from core.products where product_id = $1', [productId]))[0].updated_at;
  await new Promise((r) => setTimeout(r, 5));
  await q("update core.products set brand = 'ブランド改' where product_id = $1", [productId]);
  const after = (await q('select updated_at from core.products where product_id = $1', [productId]))[0].updated_at;
  assert.ok(new Date(after) > new Date(before));
});

console.log('\n属性の観測と解決 (06 §5.3 / §11-3)');

const obs = (key, src, v, hash, at = 'now()') => q(`insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_text, raw_text, source_system, observed_at, content_hash) values ($1, 'product', $2, 'jan', 'item', $3, $3, $4, ${at}, $5) returning observation_id`, [key, productId, v, src, hash]);

await ta('[!] 観測は「観測の回」で一意 (A→B→A の 3 回目も、同じ値の再観測も残る)。値の無い観測は拒む', async () => {
  await obs('run1:jan', 'ne', '4900000000011', 'hA', "'2026-09-01T00:00:00Z'");
  await obs('run2:jan', 'ne', '4900000000099', 'hB', "'2026-09-02T00:00:00Z'");
  await obs('run3:jan', 'ne', '4900000000011', 'hA', "'2026-09-03T00:00:00Z'");     // A→B→A の 3 回目
  await obs('run4:jan', 'ne', '4900000000011', 'hA', "'2026-09-04T00:00:00Z'");     // 同じ値の再観測
  await rejects(() => obs('run4:jan', 'ne', '4900000000011', 'hA'), /duplicate key/);   // 同じ回の再実行は冪等 (拒む)
  await obs('run4:lz', 'logizard', '4900000000011', 'hA');
  await obs('run4:amz', 'amazon_catalog', '4900000000099', 'hC');                   // 不一致 = 所見の種
  const n = (await q("select count(*)::int as n from core.product_attribute_observations where entity_type = 'product' and entity_id = $1 and attribute = 'jan'", [productId]))[0].n;
  assert.equal(n, 6);
  await rejects(() => q("insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, source_system, observed_at, content_hash) values ('run9', 'product', $1, 'jan', 'ne', now(), 'h9')", [productId]), /ck_pao_has_value/);
});

await ta('[!] 観測・raw・events・AI の記録は append-only (UPDATE / DELETE / TRUNCATE を trigger が拒む)', async () => {
  await rejects(() => q("update core.product_attribute_observations set value_text = 'x' where entity_id = $1", [productId]), /append-only/);
  await rejects(() => q("delete from core.product_attribute_observations where entity_id = $1", [productId]), /append-only/);
  // truncate は FK で参照される表だと PG 自身が先に拒む。参照されない append-only 表で trigger を確かめる
  await rejects(() => q("truncate ops.job_runs"), /append-only/);
  await rejects(() => q("truncate raw.rakuten_items_observations"), /append-only/);
  await q("insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, complete) values ('ao1', 'rakuten', 'items', 'shop1', now(), 'success', true)");
  await q("insert into raw.rakuten_items_contents (content_hash, payload) values ('hZ', '{}'::jsonb)");
  await rejects(() => q("update raw.rakuten_items_contents set payload = '{\"x\":1}'::jsonb where content_hash = 'hZ'"), /append-only/);
  await rejects(() => q("delete from raw.rakuten_items_contents where content_hash = 'hZ'"), /append-only/);
  await q("insert into ops.job_runs (job_id, host, started_at, status) values ('j', 'h', now(), 'ok')");
  await rejects(() => q("delete from ops.job_runs"), /append-only/);
});

await ta('[!] 解決結果は、観測の対象・属性・包装範囲と一致しないと入らない (複合 FK)。規則版はその属性の規則がある版だけ', async () => {
  const janObs = (await q("select observation_id from core.product_attribute_observations where observation_key = 'run4:jan'"))[0].observation_id;
  // 正しい組
  await q("insert into core.attribute_resolutions (entity_type, entity_id, attribute, packaging_scope, resolved_observation_id, rule_version) values ('product', $1, 'jan', 'item', $2, 'v1')", [productId, janObs]);
  // 別の属性 (package の重量) の根拠に JAN の観測を使う → 複合 FK で拒む
  //   (source=amazon_catalog は package_weight_g の規則に載っているので trigger は通り、FK が止める)
  const amzJanObs = (await q("select observation_id from core.product_attribute_observations where observation_key = 'run4:amz'"))[0].observation_id;
  await rejects(() => q("insert into core.attribute_resolutions (entity_type, entity_id, attribute, packaging_scope, resolved_observation_id, rule_version) values ('product', $1, 'package_weight_g', 'package', $2, 'v1')", [productId, amzJanObs]), /foreign key|violates/i);
  //   (source=ne は package_weight_g の規則に無いので trigger が止める)
  await rejects(() => q("insert into core.attribute_resolutions (entity_type, entity_id, attribute, packaging_scope, resolved_observation_id, rule_version) values ('product', $1, 'package_weight_g', 'package', $2, 'v1')", [productId, janObs]), /source ne の規則が無い/);
  // 存在しない規則版
  await rejects(() => q("update core.attribute_resolutions set rule_version = 'v9' where entity_id = $1 and attribute = 'jan'", [productId]), /規則が無い|foreign key|violates/i);
  await q("insert into core.rule_versions (rule_version) values ('v9')");   // 版はあるが jan の規則が無い
  await rejects(() => q("update core.attribute_resolutions set rule_version = 'v9' where entity_id = $1 and attribute = 'jan'", [productId]), /規則が無い/);
  // 版は存在するが、その属性の規則が無い版 (v1 に 'color' の規則は無い)
  const colorObs = (await q("insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_text, source_system, observed_at, content_hash) values ('run4:color', 'product', $1, 'color', 'item', '赤', 'product_hub', now(), 'hc') returning observation_id", [productId]))[0].observation_id;
  await rejects(() => q("insert into core.attribute_resolutions (entity_type, entity_id, attribute, packaging_scope, resolved_observation_id, rule_version) values ('product', $1, 'color', 'item', $2, 'v1')", [productId, colorObs]), /規則が無い/);
});

await ta('[!] 解決規則 v1: 実測が API より優先。同じ属性・同じ包装範囲・同じ版で priority は重複しない', async () => {
  const rows = await q("select source_system, priority from core.attribute_resolution_rules where attribute = 'package_weight_g' and packaging_scope = 'package' and rule_version = 'v1' order by priority");
  assert.equal(rows[0].source_system, 'measured');
  assert.equal(rows[1].source_system, 'amazon_catalog');
  // 同じ属性・包装範囲・版で priority は重複しない (まだ採用されていない版で確かめる)
  await q("insert into core.rule_versions (rule_version, note) values ('vdup', 'test')");
  await q("insert into core.attribute_resolution_rules (attribute, packaging_scope, source_system, priority, rule_version) values ('package_weight_g', 'package', 'measured', 1, 'vdup')");
  await rejects(() => q("insert into core.attribute_resolution_rules (attribute, packaging_scope, source_system, priority, rule_version) values ('package_weight_g', 'package', 'ne', 1, 'vdup')"), /duplicate key/);
  // 規則と版は書き換えない (採用済みの根拠が消える)。直すときは新しい版
  await rejects(() => q("delete from core.attribute_resolution_rules where rule_version = 'v1'"), /append-only/);
  await rejects(() => q("update core.attribute_resolution_rules set priority = 9 where rule_version = 'v1' and attribute = 'jan' and source_system = 'ne'"), /append-only/);
  await rejects(() => q("delete from core.rule_versions where rule_version = 'v1'"), /append-only/);
});

await ta('[!] 採用した観測の source が、その版の規則に無ければ解決結果にできない', async () => {
  const unknownObs = (await q("insert into core.product_attribute_observations (observation_key, entity_type, entity_id, attribute, packaging_scope, value_text, source_system, observed_at, content_hash) values ('run6:jan:mystery', 'product', $1, 'jan', 'item', '4900000000011', 'mystery_source', now(), 'hm') returning observation_id", [productId]))[0].observation_id;
  await rejects(() => q("update core.attribute_resolutions set resolved_observation_id = $2 where entity_id = $1 and attribute = 'jan'", [productId, unknownObs]), /source mystery_source の規則が無い/);
});

await ta('[!] 親子の会社は一致する (会社 1 の SKU に会社 2 の構成行・原価・出品対応は入らない)', async () => {
  await rejects(() => q("insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (2, $1, $2, 1, 'manual')", [setSkuId, skuId]), /foreign key|violates/i);
  await rejects(() => q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (2, $1, 1, 'manual', 'OVERRIDDEN', '2026-01-01')", [setSkuId]), /foreign key|violates/i);
  await rejects(() => q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (2, $1, $2, 1, 'manual', 'human')", [listingFbmId, skuId]), /foreign key|violates/i);
  await rejects(() => q("insert into core.skus (company_id, product_id, sku_kind, code, name) values (2, $1, 'exception', 'other-co', 'x')", [productId]), /foreign key|violates/i);
  await rejects(() => q("insert into core.product_compliance (company_id, product_id, source_system) values (2, $1, 'product_hub')", [productId]), /foreign key|violates/i);
  // 親 (バリエーション親・出品の親)・ケースの中身も同じ会社
  const p2 = (await q("insert into core.products (company_id, name) values (2, 'いろはの商品') returning product_id"))[0].product_id;
  await rejects(() => q("update core.products set parent_product_id = $2 where product_id = $1", [productId, p2]), /foreign key|violates/i);
  const l2 = (await q("insert into core.listings (company_id, mall, listing_code) values (2, 'rakuten', 'iroha-1') returning listing_id"))[0].listing_id;
  await rejects(() => q("update core.listings set parent_listing_id = $2 where listing_id = $1", [listingId, l2]), /foreign key|violates/i);
  const s2 = (await q("insert into core.skus (company_id, sku_kind, code, name) values (2, 'exception', 'iroha-sku', 'x') returning sku_id"))[0].sku_id;
  await rejects(() => q("insert into core.product_physicals (company_id, product_id, scope, units_per_case, case_content_sku_id, source_system, observed_at) values (1, $1, 'case', 12, $2, 'supplier', now())", [productId, s2]), /foreign key|violates/i);
});

await ta('[!] 採用済みの規則版には規則を足せない (版の規則集合は使い始めたら固定)。新しい版なら足せる', async () => {
  await rejects(() => q("insert into core.attribute_resolution_rules (attribute, packaging_scope, source_system, priority, rule_version) values ('jan', 'item', 'mystery_source', 99, 'v1')"), /採用済み/);
  await q("insert into core.rule_versions (rule_version, note) values ('v2', 'test')");
  await q("insert into core.attribute_resolution_rules (attribute, packaging_scope, source_system, priority, rule_version) values ('jan', 'item', 'mystery_source', 1, 'v2')");
});

await ta('[!] 物理属性は包装範囲ごとに有効 1 行。実測を後から入れて切り替えられる', async () => {
  await q("insert into core.product_physicals (company_id, product_id, scope, length_mm, width_mm, height_mm, weight_g, source_system, observed_at, is_effective) values (1, $1, 'package', 200, 150, 50, 300, 'amazon_catalog', now(), true)", [productId]);
  await rejects(() => q("insert into core.product_physicals (company_id, product_id, scope, weight_g, source_system, observed_at, is_effective, is_measured) values (1, $1, 'package', 320, 'measured', now(), true, true)", [productId]), /ux_product_physicals_effective|duplicate key/);
  await q("update core.product_physicals set is_effective = false where product_id = $1 and scope = 'package'", [productId]);
  await q("insert into core.product_physicals (company_id, product_id, scope, weight_g, source_system, observed_at, is_effective, is_measured) values (1, $1, 'package', 320, 'measured', now(), true, true)", [productId]);
  const eff = await q("select weight_g, source_system from core.product_physicals where product_id = $1 and scope = 'package' and is_effective", [productId]);
  assert.deepEqual(eff, [{ weight_g: 320, source_system: 'measured' }]);
});

await ta('[!] 文言の履歴: A→B→A が 3 行残り、current は 1 行だけ', async () => {
  const ins = (body, at) => q("insert into core.listing_texts (listing_id, field, body, content_hash, observed_at, source_system, is_current) values ($1, 'title', $2, md5($2), $3, 'amazon_listing', false)", [listingId, body, at]);
  await ins('タイトルA', '2026-09-01T00:00:00Z');
  await ins('タイトルB', '2026-09-02T00:00:00Z');
  await ins('タイトルA', '2026-09-03T00:00:00Z');
  await rejects(() => ins('タイトルA', '2026-09-03T00:00:00Z'), /duplicate key/);          // 同じ回は冪等
  await q("update core.listing_texts set is_current = true where listing_id = $1 and field = 'title' and observed_at = '2026-09-03T00:00:00Z'", [listingId]);
  await rejects(() => q("update core.listing_texts set is_current = true where listing_id = $1 and field = 'title' and observed_at = '2026-09-01T00:00:00Z'", [listingId]), /ux_listing_texts_current|duplicate key/);
  const n = (await q("select count(*)::int as n from core.listing_texts where listing_id = $1 and field = 'title'", [listingId]))[0].n;
  assert.equal(n, 3);
  // 履歴の本文は書き換えられない・消せない (is_current の付け替えだけ)
  await rejects(() => q("update core.listing_texts set body = '改ざん' where listing_id = $1 and field = 'title' and observed_at = '2026-09-01T00:00:00Z'", [listingId]), /is_current 以外/);
  await rejects(() => q("delete from core.listing_texts where listing_id = $1", [listingId]), /履歴/);
  await rejects(() => q("update core.listing_texts set listing_text_id = default where listing_id = $1 and field = 'title' and observed_at = '2026-09-01T00:00:00Z'", [listingId]), /is_current 以外|generated|identity/i);
  await q("update core.listing_texts set is_current = false where listing_id = $1 and field = 'title' and is_current", [listingId]);
  await q("update core.listing_texts set is_current = true where listing_id = $1 and field = 'title' and observed_at = '2026-09-02T00:00:00Z'", [listingId]);
});

console.log('\nraw 2 表 (06 §11-1: A→B→A も「変化なし」も「取れなかった」も残る)');

await ta('[!] contents は中身の重複排除、observations は毎回。A→B→A で観測 3・中身 2。取れなかった回も残る', async () => {
  for (const [run, hash] of [['run1', 'hA'], ['run2', 'hB'], ['run3', 'hA']]) {
    await q("insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, complete) values ($1, 'rakuten', 'items', 'shop1', now(), 'success', true)", [run]);
    await q("insert into raw.rakuten_items_contents (content_hash, payload) values ($1, $2::jsonb) on conflict (content_hash) do nothing", [hash, JSON.stringify({ manageNumber: 'abc001', v: hash })]);
    await q("insert into raw.rakuten_items_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ($1, 'shop1', 'abc001', $2, 'ok', now())", [run, hash]);
  }
  await q("insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, complete) values ('run4', 'rakuten', 'items', 'shop1', now(), 'partial', false)");
  await q("insert into raw.rakuten_items_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('run4', 'shop1', 'abc001', null, 'error', now())");
  assert.equal((await q("select count(*)::int as n from raw.rakuten_items_contents where content_hash in ('hA','hB')"))[0].n, 2);
  assert.equal((await q("select count(*)::int as n from raw.rakuten_items_observations where business_key = 'abc001'"))[0].n, 4);
  await rejects(() => q("insert into raw.rakuten_items_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('run4', 'shop1', 'zzz', null, 'ok', now())"), /ck_rakuten_items_obs_content/);
  await rejects(() => q("insert into raw.rakuten_items_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('run3', 'shop1', 'abc001', 'hA', 'ok', now())"), /duplicate key/);
  await rejects(() => q("delete from raw.rakuten_items_observations where business_key = 'abc001'"), /append-only/);
});

console.log('\nsnapshots と events');

await ta('[!] 月パーティション: 作っていない月は default に入る (落ちない)。後から作ると default から移る', async () => {
  const created = (await q("select snapshots.ensure_month_partitions('2026-09-01', '2026-10-31') as n"))[0].n;
  assert.equal(created, 6);                                  // 3 表 × 2 か月
  assert.equal((await q("select snapshots.ensure_month_partitions('2026-09-01', '2026-10-31') as n"))[0].n, 0);
  await q("insert into snapshots.listing_daily (snapshot_date, listing_id, status, price_jpy, complete, source_run_id, observed_at) values ('2026-09-09', $1, 'active', 1980, true, 'run1', now())", [listingId]);
  await q("insert into snapshots.listing_daily (snapshot_date, listing_id, status, price_jpy, complete, source_run_id, observed_at) values ('2030-01-15', $1, 'active', 1980, true, 'runX', now())", [listingId]);
  let parts = await q("select tableoid::regclass::text as part from snapshots.listing_daily where listing_id = $1 order by snapshot_date", [listingId]);
  assert.deepEqual(parts.map((p) => p.part), ['snapshots.listing_daily_202609', 'snapshots.listing_daily_default']);
  // default に行がある月を後から作る → 行が移り、default は空になる
  assert.equal((await q("select snapshots.ensure_month_partitions('2030-01-01', '2030-01-31') as n"))[0].n, 3);
  parts = await q("select tableoid::regclass::text as part from snapshots.listing_daily where listing_id = $1 order by snapshot_date", [listingId]);
  assert.deepEqual(parts.map((p) => p.part), ['snapshots.listing_daily_202609', 'snapshots.listing_daily_203001']);
  assert.equal((await q("select count(*)::int as n from snapshots.listing_daily_default"))[0].n, 0);
  await rejects(() => q("insert into snapshots.listing_daily (snapshot_date, listing_id, status, price_jpy, complete, source_run_id, observed_at) values ('2030-01-15', $1, 'active', 1, true, 'runX', now())", [listingId]), /duplicate key/);
});

await ta('[!] events は idempotency_key unique・append-only。差分由来は actor_type=external で actor_id 無し', async () => {
  const ins = () => q("insert into events.price_change_events (company_id, occurred_at, actor_type, source_system, idempotency_key, listing_id, old_price_jpy, new_price_jpy) values (1, now(), 'external', 'snapshot_diff', $1, $2, 1980, 1780)", [`diff:2026-09-09:${listingId}`, listingId]);
  await ins();
  await rejects(ins, /duplicate key/);
  await rejects(() => q("insert into events.price_change_events (company_id, occurred_at, actor_type, source_system, idempotency_key, listing_id, new_price_jpy) values (1, now(), 'robot', 'x', 'k2', $1, 1)", [listingId]), /actor_type/);
  await rejects(() => q("delete from events.price_change_events"), /append-only/);
  await rejects(() => q("update events.price_change_events set new_price_jpy = 1"), /append-only/);
});

console.log('\nmart (AI Read Model v1)');

await ta('[!] v_product_360: SKU 1 行に 商品属性・JAN・ASIN (listing 経由)・原価・実測重量・出品の配列 (FBA/FBM 両方)・欠落フラグ', async () => {
  await q("insert into core.listing_states (listing_id, status, price_jpy, stock_qty, fulfillment, observed_at, snapshot_run_id) values ($1, 'active', 1980, 12, 'fba', now(), 'run1')", [listingId]);
  await q("insert into core.listing_states (listing_id, status, price_jpy, stock_qty, fulfillment, observed_at, snapshot_run_id) values ($1, 'active', 2080, 3, 'fbm', now(), 'run1')", [listingFbmId]);
  const r = (await q('select * from mart.v_product_360 where sku_id = $1', [setSkuId]))[0];
  assert.equal(r.sku_kind, 'set');
  assert.equal(r.asin, 'B000TEST01');
  assert.equal(Number(r.listing_count), 2);
  assert.equal(r.listings_json.length, 2);                   // 同じ店舗の 2 出品が両方見える
  assert.deepEqual(r.listings_json.map((x) => x.fulfillment), ['fba', 'fbm']);
  assert.equal(Number(r.min_price_jpy), 1980);
  assert.equal(Number(r.max_price_jpy), 2080);
  assert.ok(r.dq_flags.includes('cost'));
  const single = (await q('select * from mart.v_product_360 where sku_id = $1', [skuId]))[0];
  assert.equal(single.jan, '4900000000011');
  assert.equal(single.asin, null);                           // 単品は listing が無いので ASIN も無い (product 直付けは無い)
  assert.equal(Number(single.cost_jpy), 400);
  assert.equal(single.package_weight_g, 320);
  assert.equal(single.package_is_measured, true);
  assert.ok(single.dq_flags.includes('package_dims'));       // 実測は重量だけなので寸法は欠落
  assert.ok(single.dq_flags.includes('genre'));
  assert.ok(single.dq_flags.includes('no_active_listing'));
  assert.ok(!single.dq_flags.includes('jan'));
});

await ta('[!] v_cross_mall_diff: JAN はソースごとの最新だけを比べる (訂正後は消える)。価格は単品出品どうしだけ', async () => {
  // ne は 11→99→11→11 と観測 (最新 11)、amazon_catalog は 99 → 不一致
  let diff = await q("select diff_kind, values_by_source from mart.v_cross_mall_diff where sku_id = $1", [skuId]);
  assert.equal(diff.length, 1);
  assert.equal(diff[0].values_by_source.ne, '4900000000011');
  assert.equal(diff[0].values_by_source.amazon_catalog, '4900000000099');
  // amazon_catalog が訂正されたら (最新が 11) 不一致は消える
  await obs('run5:amz', 'amazon_catalog', '4900000000011', 'hA', "now() + interval '1 day'");
  diff = await q("select diff_kind from mart.v_cross_mall_diff where sku_id = $1", [skuId]);
  assert.equal(diff.length, 0);
  // 価格: 単品 sku に 楽天 1,000 円 と、A×1 + B×1 の組合せ出品 3,000 円 → 組合せは比較に入らない
  const rk = (await q("insert into core.listings (company_id, mall, listing_code, status) values (1, 'rakuten', 'abc001', 'active') returning listing_id"))[0].listing_id;
  await q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'exact', 'system')", [rk, skuId]);
  await q("insert into core.listing_states (listing_id, status, price_jpy, observed_at, snapshot_run_id) values ($1, 'active', 1000, now(), 'run1')", [rk]);
  const combo = (await q("insert into core.listings (company_id, mall, listing_code, status) values (1, 'rakuten', 'abc001-plus-set', 'active') returning listing_id"))[0].listing_id;
  await q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'manual', 'human'), (1, $1, $3, 1, 'manual', 'human')", [combo, skuId, setSkuId]);
  await q("insert into core.listing_states (listing_id, status, price_jpy, observed_at, snapshot_run_id) values ($1, 'active', 3000, now(), 'run1')", [combo]);
  assert.equal((await q("select count(*)::int as n from mart.v_cross_mall_diff where sku_id = $1 and diff_kind = 'price'", [skuId]))[0].n, 0);
  // v_product_360 の最小・最大価格にも組合せ出品は混ざらない
  const p360 = (await q("select min_price_jpy, max_price_jpy, listing_count from mart.v_product_360 where sku_id = $1", [skuId]))[0];
  assert.equal(Number(p360.min_price_jpy), 1000);
  assert.equal(Number(p360.max_price_jpy), 1000);
  assert.equal(Number(p360.listing_count), 2);               // 組合せ出品も出品としては数える
  // 同じ単品に Yahoo 1,200 円 → 価格差が出る
  const yh = (await q("insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'yahoo', 'store1', 'abc001', 'active') returning listing_id"))[0].listing_id;
  await q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'exact', 'system')", [yh, skuId]);
  await q("insert into core.listing_states (listing_id, status, price_jpy, observed_at, snapshot_run_id) values ($1, 'active', 1200, now(), 'run1')", [yh]);
  const pd = (await q("select spread_jpy, spread_ratio from mart.v_cross_mall_diff where sku_id = $1 and diff_kind = 'price'", [skuId]))[0];
  assert.equal(Number(pd.spread_jpy), 200);
  assert.equal(Number(pd.spread_ratio), 0.2);
});

await ta('v_product_dq は 1 行 1 欠落', async () => {
  const dq = await q('select missing from mart.v_product_dq where sku_id = $1 order by missing', [skuId]);
  assert.ok(dq.some((d) => d.missing === 'genre'));
  assert.ok(!dq.some((d) => d.missing === 'no_active_listing'));   // 楽天・Yahoo に出品ができた
});

await ta('v_listing_360 は販路商品 1 行に状態・最新スナップショット・代表 SKU・構成の配列', async () => {
  const r = (await q('select v.*, v.last_snapshot_date::text as last_snapshot_date_text from mart.v_listing_360 v where listing_id = $1', [listingId]))[0];
  assert.equal(r.current_status, 'active');
  assert.equal(r.asin, 'B000TEST01');
  assert.equal(r.asin_package_scope, 'multipack');
  assert.equal(Number(r.representative_sku_id), Number(setSkuId));
  assert.equal(r.component_count, 1);
  assert.equal(r.last_snapshot_date_text, '2030-01-15');
  const combo = (await q("select component_count, components_json from mart.v_listing_360 where listing_code = 'abc001-plus-set'"))[0];
  assert.equal(combo.component_count, 2);
  assert.equal(combo.components_json.length, 2);
});

await pglite.close();
console.log(`\n${passed} 件 PASS`);
