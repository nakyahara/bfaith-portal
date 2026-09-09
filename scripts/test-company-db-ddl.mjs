/**
 * test-company-db-ddl.mjs — Company DB (Postgres) の DDL とマイグレーション実行器の試験
 *
 * PGlite (WASM の Postgres、devDependency) で db/company/migrations を全部流し、
 * 03 §10 の DDL セルフチェックを機械で固定する。本物の Postgres は要らない。
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
  await assert.rejects(() => applyMigrations(db, { dir: tmp, log: quiet }), /checksum 不一致/);
  const st = await migrationStatus(db, { dir: tmp });
  assert.equal(st[0].state, 'CHANGED');
  fs.rmSync(tmp, { recursive: true, force: true });
});

await ta('checksum は改行コードの違いを吸収する (CRLF で checkout されても同じ)', () => {
  assert.equal(checksumOf('a\r\nb\r\n'), checksumOf('a\nb\n'));
});

await ta('[!] 途中で失敗したファイルは巻き戻され、前のファイルまでは記録が残る', async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-mig2-'));
  fs.writeFileSync(path.join(tmp, '0001_ok.sql'), 'create table public.t_ok (id int primary key);\n');
  fs.writeFileSync(path.join(tmp, '0002_bad.sql'), 'create table public.t_bad (id int primary key);\nselect * from public.no_such_table;\n');
  const p2 = new PGlite(); const d2 = pgliteAdapter(p2);
  await assert.rejects(() => applyMigrations(d2, { dir: tmp, log: quiet }), /0002_bad\.sql で失敗/);
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
    'core.listings', 'core.listing_components', 'core.external_ids', 'core.warehouses', 'core.locations',
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

await ta('[!] 03 §10: append-only 表 (raw observations / events / 観測) に updated_at や status が無い', async () => {
  const rows = await q(`select table_schema || '.' || table_name as t, column_name from information_schema.columns
    where ((table_schema = 'raw' and table_name like '%\\_observations' escape '\\') or table_schema = 'events' or (table_schema = 'core' and table_name = 'product_attribute_observations'))
      and column_name in ('updated_at', 'status')`);
  assert.deepEqual(rows, [], JSON.stringify(rows));
});

await ta('[!] 03 §10: Canonical 表に company_id と監査列がある', async () => {
  // external_ids は「誰が結び付けたか」= resolved_by_type が監査列の役 (created_by_type の代わり)
  const canonical = { products: 'created_by_type', skus: 'created_by_type', listings: 'created_by_type', suppliers: 'created_by_type', workers: 'created_by_type', external_ids: 'resolved_by_type', sku_costs: 'created_by_type' };
  for (const [t, byCol] of Object.entries(canonical)) {
    const cols = new Set((await q("select column_name from information_schema.columns where table_schema = 'core' and table_name = $1", [t])).map((r) => r.column_name));
    for (const c of ['company_id', 'created_at', byCol]) assert.ok(cols.has(c), `core.${t} に ${c} が無い`);
  }
  for (const t of ['products', 'skus', 'listings', 'suppliers', 'workers']) {
    const cols = new Set((await q("select column_name from information_schema.columns where table_schema = 'core' and table_name = $1", [t])).map((r) => r.column_name));
    assert.ok(cols.has('updated_at'), `core.${t} に updated_at が無い`);
  }
});

console.log('\n正規化関数 core.norm_code = lib/sku-norm.js normSku (03 §10)');

await ta('[!] JS と SQL が同じ結果を返す (全角・ダッシュ・空白・半角カナ・NBSP)', async () => {
  const fixtures = [
    'ＡＢＣ－001 ', 'abc−001', ' ab c 002', 'ＡＢＣ　003', 'x y-Z', 'ｶﾀｶﾅ-1', 'ABC—5', 'ABC‐6', 'ABC﹣7', 'ABC－8', 'pr_ABC001-3',
    'ＰＲ＿ａｂｃ００１', 'abc　 def', '﻿abc', 'ABC/DEF+GHI', 'Ａbc.ｄef', '　', '',
  ];
  for (const f of fixtures) {
    const sql = (await q('select core.norm_code($1) as v', [f]))[0].v;
    assert.equal(sql, normSku(f), `"${f}": SQL=${JSON.stringify(sql)} JS=${JSON.stringify(normSku(f))}`);
  }
  assert.equal((await q('select core.norm_code(null) as v'))[0].v, null);   // null は null (JS は '')
});

await ta('core.jst_date は日本時間の日付 (UTC の 9/9 15:00 = JST 9/10)', async () => {
  const d = (await q("select core.jst_date('2026-09-09T15:00:00Z'::timestamptz)::text as d"))[0].d;
  assert.equal(d, '2026-09-10');
  assert.equal((await q("select core.jst_date('2026-09-09T14:59:59Z'::timestamptz)::text as d"))[0].d, '2026-09-09');
});

console.log('\n商品 → SKU → 販路商品 → 外部 ID の鎖 (06 §5.2)');

let productId, skuId, setSkuId, listingId, catalogItemId;
await ta('[!] 単品 SKU は product と 1:1、セットは構成表。code_norm は unique', async () => {
  productId = (await q("insert into core.products (company_id, display_code, name, brand, unit_count, unit_count_uom, created_by_type) values (1, 'abc001', 'テスト商品', 'テストブランド', 50, '個', 'system') returning product_id"))[0].product_id;
  skuId = (await q("insert into core.skus (company_id, product_id, sku_kind, code, code_norm, name, tax_rate, tax_class, created_by_type) values (1, $1, 'single', 'ABC001', core.norm_code('ABC001'), 'テスト商品', 0.10, 'STANDARD_10', 'system') returning sku_id", [productId]))[0].sku_id;
  setSkuId = (await q("insert into core.skus (company_id, sku_kind, code, code_norm, name, created_by_type) values (1, 'set', 'abc001set3', core.norm_code('abc001set3'), 'テスト商品 3個セット', 'system') returning sku_id"))[0].sku_id;
  await q("insert into core.sku_components (parent_sku_id, child_sku_id, qty, source) values ($1, $2, 3, 'ne')", [setSkuId, skuId]);
  // 単品なのに product 無し → CHECK で止まる
  await assert.rejects(() => q("insert into core.skus (company_id, sku_kind, code, code_norm, name, created_by_type) values (1, 'single', 'nop', 'nop', 'x', 'system')"), /ck_skus_single_has_product/);
  // 大文字違いの同じコードは unique で止まる
  await assert.rejects(() => q("insert into core.skus (company_id, product_id, sku_kind, code, code_norm, name, created_by_type) values (1, $1, 'single', 'abc001', core.norm_code('abc001'), 'x', 'system')", [productId]), /skus_company_id_code_norm_key|duplicate key/);
  // 自分を構成品にはできない
  await assert.rejects(() => q("insert into core.sku_components (parent_sku_id, child_sku_id, qty, source) values ($1, $1, 1, 'ne')", [setSkuId]), /ck_sku_components_not_self/);
});

await ta('[!] 原価は有効期間つき。有効 (valid_to null) は SKU ごとに 1 行だけ', async () => {
  await q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, created_by_type) values (1, $1, 380, 'ne', 'COMPLETE', '2026-01-01', 'system')", [skuId]);
  await assert.rejects(() => q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, created_by_type) values (1, $1, 400, 'manual', 'OVERRIDDEN', '2026-09-01', 'human')", [skuId]), /ux_sku_costs_active|duplicate key/);
  await q("update core.sku_costs set valid_to = '2026-08-31' where sku_id = $1 and valid_to is null", [skuId]);
  await q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, created_by_type) values (1, $1, 400, 'manual', 'OVERRIDDEN', '2026-09-01', 'human')", [skuId]);
  const active = await q('select cost_jpy from core.sku_costs where sku_id = $1 and valid_to is null', [skuId]);
  assert.deepEqual(active.map((r) => Number(r.cost_jpy)), [400]);
  await assert.rejects(() => q("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, created_by_type) values (1, $1, -1, 'ne', 'COMPLETE', '2026-01-01', 'system')", [skuId]), /cost_jpy/);
});

await ta('[!] ASIN は catalog_items に置き、listing と 1:N (product の外部 ID にしない)', async () => {
  catalogItemId = (await q("insert into core.catalog_items (marketplace_id, asin, package_scope, pack_count, brand) values ('A1VC38T7YXB528', 'B000TEST01', 'multipack', 3, 'テストブランド') returning catalog_item_id"))[0].catalog_item_id;
  listingId = (await q("insert into core.listings (company_id, mall, shop_code, listing_code, listing_norm, status, catalog_item_id, created_by_type) values (1, 'amazon', 'S1@A1VC38T7YXB528', 'pr_ABC001-3', core.norm_code('pr_ABC001-3'), 'active', $1, 'system') returning listing_id", [catalogItemId]))[0].listing_id;
  const fbm = (await q("insert into core.listings (company_id, mall, shop_code, listing_code, listing_norm, status, catalog_item_id, created_by_type) values (1, 'amazon', 'S1@A1VC38T7YXB528', 'pr_ABC001-3-fbm', core.norm_code('pr_ABC001-3-fbm'), 'active', $1, 'system') returning listing_id", [catalogItemId]))[0].listing_id;
  assert.ok(fbm > listingId);                             // 同じ ASIN に 2 出品
  await q("insert into core.listing_components (listing_id, sku_id, qty, resolution, resolved_by_type) values ($1, $2, 1, 'imported', 'system')", [listingId, setSkuId]);
  await q("insert into core.catalog_item_products (catalog_item_id, product_id, qty, resolution, resolved_by_type) values ($1, $2, 3, 'manual', 'human')", [catalogItemId, productId]);
  // 同じモール・店舗・コード (大文字違い) は unique
  await assert.rejects(() => q("insert into core.listings (company_id, mall, shop_code, listing_code, listing_norm, created_by_type) values (1, 'amazon', 'S1@A1VC38T7YXB528', 'PR_abc001-3', core.norm_code('PR_abc001-3'), 'system')"), /duplicate key/);
});

await ta('[!] 外部 ID: 同じ system/kind の値は有効期間内で 1 エンティティだけ。付け替えは valid_to を埋めてから', async () => {
  await q("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, external_norm, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4900000000011', '4900000000011', 'imported', 'system')", [productId]);
  const other = (await q("insert into core.products (company_id, name, created_by_type) values (1, '別の商品', 'system') returning product_id"))[0].product_id;
  await assert.rejects(() => q("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, external_norm, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4900000000011', '4900000000011', 'imported', 'system')", [other]), /ux_external_ids_active|duplicate key/);
  await q("update core.external_ids set valid_to = now() where system = 'jan' and external_norm = '4900000000011' and valid_to is null");
  await q("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, external_norm, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4900000000011', '4900000000011', 'manual', 'human')", [other]);
  const active = await q("select entity_id from core.external_ids where system = 'jan' and external_norm = '4900000000011' and valid_to is null");
  assert.deepEqual(active.map((r) => Number(r.entity_id)), [Number(other)]);
  // 元に戻す (以降の試験で productId の JAN を使う)
  await q("update core.external_ids set valid_to = now() where system = 'jan' and external_norm = '4900000000011' and valid_to is null");
  await q("insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, external_norm, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4900000000011', '4900000000011', 'manual', 'human')", [productId]);
});

await ta('updated_at は更新のたびに進む (trigger)', async () => {
  const before = (await q('select updated_at from core.products where product_id = $1', [productId]))[0].updated_at;
  await new Promise((r) => setTimeout(r, 5));
  await q("update core.products set brand = 'ブランド改' where product_id = $1", [productId]);
  const after = (await q('select updated_at from core.products where product_id = $1', [productId]))[0].updated_at;
  assert.ok(new Date(after) > new Date(before));
});

console.log('\n属性の観測と解決 (06 §5.3 / §11-3)');

await ta('[!] 観測は append-only で出どころごとに残り、同じ内容の再観測は重複しない。値の無い観測は拒む', async () => {
  const ins = (src, v, hash) => q("insert into core.product_attribute_observations (entity_type, entity_id, attribute, packaging_scope, value_text, raw_text, source_system, observed_at, content_hash) values ('product', $1, 'jan', 'item', $2, $2, $3, now(), $4) on conflict do nothing", [productId, v, src, hash]);
  await ins('ne', '4900000000011', 'h1');
  await ins('logizard', '4900000000011', 'h2');
  await ins('amazon_catalog', '4900000000099', 'h3');     // 不一致 = 所見の種
  await ins('ne', '4900000000011', 'h1');                 // 同じ内容の再観測
  const n = (await q("select count(*)::int as n from core.product_attribute_observations where entity_type = 'product' and entity_id = $1 and attribute = 'jan'", [productId]))[0].n;
  assert.equal(n, 3);
  await assert.rejects(() => q("insert into core.product_attribute_observations (entity_type, entity_id, attribute, source_system, observed_at, content_hash) values ('product', $1, 'jan', 'ne', now(), 'h9')", [productId]), /ck_pao_has_value/);
});

await ta('[!] 解決規則 v1 が入っていて、同じ属性・同じ包装範囲の中で優先順位が引ける', async () => {
  const rows = await q("select source_system, priority from core.attribute_resolution_rules where attribute = 'package_weight_g' and packaging_scope = 'package' and rule_version = 'v1' order by priority");
  assert.equal(rows[0].source_system, 'measured');           // 実測が API より優先
  assert.equal(rows[1].source_system, 'amazon_catalog');
  const jan = await q("select source_system from core.attribute_resolution_rules where attribute = 'jan' and rule_version = 'v1' order by priority limit 2");
  assert.deepEqual(jan.map((r) => r.source_system), ['product_hub', 'ne']);
});

await ta('[!] 物理属性は包装範囲ごとに有効 1 行。実測を後から入れて切り替えられる', async () => {
  await q("insert into core.product_physicals (product_id, scope, length_mm, width_mm, height_mm, weight_g, source_system, observed_at, is_effective, created_by_type) values ($1, 'package', 200, 150, 50, 300, 'amazon_catalog', now(), true, 'system')", [productId]);
  await assert.rejects(() => q("insert into core.product_physicals (product_id, scope, weight_g, source_system, observed_at, is_effective, is_measured, created_by_type) values ($1, 'package', 320, 'measured', now(), true, true, 'human')", [productId]), /ux_product_physicals_effective|duplicate key/);
  await q("update core.product_physicals set is_effective = false where product_id = $1 and scope = 'package'", [productId]);
  await q("insert into core.product_physicals (product_id, scope, weight_g, source_system, observed_at, is_effective, is_measured, created_by_type) values ($1, 'package', 320, 'measured', now(), true, true, 'human')", [productId]);
  const eff = await q("select weight_g, source_system from core.product_physicals where product_id = $1 and scope = 'package' and is_effective", [productId]);
  assert.deepEqual(eff, [{ weight_g: 320, source_system: 'measured' }]);
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
  assert.equal((await q("select count(*)::int as n from raw.rakuten_items_contents"))[0].n, 2);
  assert.equal((await q("select count(*)::int as n from raw.rakuten_items_observations where business_key = 'abc001'"))[0].n, 4);
  // ok なのに中身が無い / error なのに中身がある は CHECK で止まる
  await assert.rejects(() => q("insert into raw.rakuten_items_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('run4', 'shop1', 'zzz', null, 'ok', now())"), /ck_rakuten_items_obs_content/);
  // 同じ run で同じ key を 2 回は unique
  await assert.rejects(() => q("insert into raw.rakuten_items_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at) values ('run3', 'shop1', 'abc001', 'hA', 'ok', now())"), /duplicate key/);
});

console.log('\nsnapshots と events');

await ta('[!] 月パーティションを作ってから入れる。作っていない月は default に入る (落ちない)', async () => {
  const created = (await q("select snapshots.ensure_month_partitions('2026-09-01', '2026-10-31') as n"))[0].n;
  assert.equal(created, 6);                                  // 3 表 × 2 か月
  assert.equal((await q("select snapshots.ensure_month_partitions('2026-09-01', '2026-10-31') as n"))[0].n, 0);
  await q("insert into snapshots.listing_daily (snapshot_date, listing_id, status, price_jpy, complete, source_run_id, observed_at) values ('2026-09-09', $1, 'active', 1980, true, 'run1', now())", [listingId]);
  await q("insert into snapshots.listing_daily (snapshot_date, listing_id, status, price_jpy, complete, source_run_id, observed_at) values ('2030-01-01', $1, 'active', 1980, true, 'runX', now())", [listingId]);
  const parts = await q("select tableoid::regclass::text as part from snapshots.listing_daily where listing_id = $1 order by snapshot_date", [listingId]);
  assert.deepEqual(parts.map((p) => p.part), ['snapshots.listing_daily_202609', 'snapshots.listing_daily_default']);
  await assert.rejects(() => q("insert into snapshots.listing_daily (snapshot_date, listing_id, status, price_jpy, complete, source_run_id, observed_at) values ('2026-09-09', $1, 'active', 1980, true, 'run1', now())", [listingId]), /duplicate key/);
});

await ta('[!] events は idempotency_key unique。差分由来は actor_type=external で actor_id 無し (人が変えたと断定しない)', async () => {
  const ins = () => q("insert into events.price_change_events (company_id, occurred_at, actor_type, source_system, idempotency_key, listing_id, old_price_jpy, new_price_jpy) values (1, now(), 'external', 'snapshot_diff', $1, $2, 1980, 1780)", [`diff:2026-09-09:${listingId}`, listingId]);
  await ins();
  await assert.rejects(ins, /duplicate key/);
  await assert.rejects(() => q("insert into events.price_change_events (company_id, occurred_at, actor_type, source_system, idempotency_key, listing_id, new_price_jpy) values (1, now(), 'robot', 'x', 'k2', $1, 1)", [listingId]), /actor_type/);
});

console.log('\nmart (AI Read Model v1)');

await ta('[!] v_product_360: SKU 1 行に 商品属性・JAN・ASIN・原価・実測重量・モール別の現在値・欠落フラグ', async () => {
  await q("insert into core.listing_states (listing_id, status, price_jpy, stock_qty, fulfillment, observed_at, snapshot_run_id) values ($1, 'active', 1980, 12, 'fba', now(), 'run1')", [listingId]);
  const rows = await q('select * from mart.v_product_360 where sku_id = $1', [setSkuId]);
  assert.equal(rows.length, 1);
  const r = rows[0];
  assert.equal(r.sku_kind, 'set');
  assert.equal(r.asin, 'B000TEST01');                        // listing → catalog_items 経由
  assert.equal(Number(r.listing_count), 1);
  assert.equal(Number(r.by_mall['amazon:S1@A1VC38T7YXB528'].price_jpy), 1980);
  assert.ok(r.dq_flags.includes('cost'));                    // セットには原価が無い
  const single = (await q('select * from mart.v_product_360 where sku_id = $1', [skuId]))[0];
  assert.equal(single.jan, '4900000000011');
  assert.equal(Number(single.cost_jpy), 400);
  assert.equal(single.package_weight_g, 320);
  assert.equal(single.package_is_measured, true);
  assert.equal(single.unit_count, 50);
  assert.ok(single.dq_flags.includes('genre'));               // ジャンル未設定
  assert.ok(single.dq_flags.includes('no_active_listing'));   // 単品には出品が無い
  assert.ok(!single.dq_flags.includes('jan'));
});

await ta('v_product_dq は 1 行 1 欠落、v_cross_mall_diff は JAN の不一致を出す', async () => {
  const dq = await q('select missing from mart.v_product_dq where sku_id = $1 order by missing', [skuId]);
  assert.ok(dq.some((d) => d.missing === 'genre'));
  const diff = await q("select diff_kind, values_by_source from mart.v_cross_mall_diff where sku_id = $1", [skuId]);
  assert.equal(diff.length, 1);
  assert.equal(diff[0].diff_kind, 'jan');
  assert.equal(diff[0].values_by_source.amazon_catalog, '4900000000099');
  assert.equal(diff[0].values_by_source.ne, '4900000000011');
});

await ta('v_listing_360 は販路商品 1 行に状態・最新スナップショット・SKU', async () => {
  const r = (await q('select v.*, v.last_snapshot_date::text as last_snapshot_date_text from mart.v_listing_360 v where listing_id = $1', [listingId]))[0];
  assert.equal(r.current_status, 'active');
  assert.equal(r.asin, 'B000TEST01');
  assert.equal(Number(r.sku_id), Number(setSkuId));
  assert.equal(r.last_snapshot_date_text, '2030-01-01');
});

await pglite.close();
console.log(`\n${passed} 件 PASS`);
