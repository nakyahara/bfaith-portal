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
    // 0011 在庫 (08 §3)
    'raw.logizard_inventory_contents', 'raw.logizard_inventory_observations',
    'snapshots.stock_capture_days', 'snapshots.warehouse_stock_daily', 'snapshots.sku_stock_daily', 'snapshots.sku_stock_weekly', 'snapshots.stock_diff_days',
    // 0012 Amazon 財務 (08 §4.4)
    'core.finance_source_policy', 'core.order_finance_receipts', 'core.order_finance_daily',
    // 0013 受注・出荷 (08 §4.1〜4.3)
    'core.order_status_map', 'core.ne_shops', 'core.mall_order_policy', 'core.orders', 'core.order_lines', 'core.shipments', 'core.shipment_lines',
    // 0014 発注 (08 §5)
    'core.purchase_order_settings', 'core.purchase_orders', 'core.purchase_order_lines', 'events.purchase_order_events',
    // 0015 取込 chunk の受領記録 (D5a)
    'ops.ingest_chunks',
    // 0023 見張りの結果 (09 §7)
    'ops.watch_runs', 'ops.watch_results', 'ops.watch_issues', 'ops.watch_result_items',
    // 0026 マスタの変更の記録 (10 §5.2)
    'events.master_change_events',
    // 0028 夜間ロードが読んだ材料の世代 (10 §6 / ③a-1)
    'ops.load_materials',
    'ops.load_decisions',
    // 0046 観測の原価 (13 §3.4・D7b-2)
    'core.sku_cost_observed_loads', 'core.sku_cost_observed',
    // 0051 マスタ入力画面の土台 (14 §6 ⑤-1)
    'ops.master_cutover_state', 'ops.master_cutover_events', 'ops.master_legacy_manifests', 'ops.master_legacy_gate_acks', 'ops.master_edit_requests', 'ops.sku_component_requests',
    'ops.ne_set_observation_runs', 'ops.ne_set_observations', 'ops.sku_component_breaches',
    // 0052 新商品の登録と登録の状態・product-hub のカードの outbox (14 ⑤-2a)
    'ops.master_registrations', 'ops.master_registration_events', 'ops.master_registration_backfill', 'ops.product_hub_outbox',
    // 0053 新商品の NE 登録の CSV・仕入先の登録の状態 (14 ⑤-2b)
    'ops.ne_reg_exports', 'ops.ne_reg_attempts', 'ops.ne_reg_export_items', 'ops.ne_reg_export_rows', 'ops.ne_reg_checks', 'ops.supplier_registrations', 'ops.supplier_registration_events',
  ];
  const missing = expect.filter((t) => !have.has(t));
  assert.deepEqual(missing, [], `無い表: ${missing.join(', ')}`);
  const martTables = await q("select table_name as t from information_schema.tables where table_schema = 'mart' and table_type = 'BASE TABLE'");
  assert.deepEqual(martTables.map((v) => v.t).sort(), ['finance_daily', 'sales_daily', 'sales_daily_published', 'sales_daily_runs', 'sales_daily_session_dates', 'sales_daily_state']);   // mart は view が基本。表は run_id publish の日次集計とその公開の管理 (0021) だけ
  const views = await q("select table_schema || '.' || table_name as t from information_schema.views where table_schema = 'mart'");
  assert.deepEqual(views.map((v) => v.t).sort(), ['mart.v_ad_spend_daily', 'mart.v_cross_mall_diff', 'mart.v_finance_account_fees_monthly', 'mart.v_finance_daily', 'mart.v_listing_360', 'mart.v_order_finance_summary', 'mart.v_order_finance_uncovered', 'mart.v_product_360', 'mart.v_product_dq', 'mart.v_purchase_backorder_by_sku', 'mart.v_purchase_order_open', 'mart.v_sales_daily', 'mart.v_shipments_daily', 'mart.v_shipments_unlinked', 'mart.v_sku_cost_observed_effective', 'mart.v_sku_stock', 'mart.v_warehouse_stock_current']);
});

await ta('[!] 0051 (14 §6 ⑤-1・#1563 R1・R2): 切替の段階は legacy_open から (門の記録が無い = 進めない)・セットの 2 列は null で足す・記録と観測は追記だけ・原価の期間の重なりの守りは夜間ロードを見ない・段階を進める関数と観測を書く関数は public に実行させない', async () => {
  assert.deepEqual((await q('select phase from ops.master_cutover_state'))[0], { phase: 'legacy_open' });
  const cols = await q("select column_name as c, data_type as t, is_nullable as n from information_schema.columns where table_schema = 'core' and table_name = 'skus' and column_name in ('set_sales_class_override', 'handling_own') order by 1");
  assert.deepEqual(cols.map((r) => [r.c, r.t, r.n]), [['handling_own', 'text', 'YES'], ['set_sales_class_override', 'smallint', 'YES']]);
  assert.equal((await q("select count(*)::int as n from core.skus where set_sales_class_override is not null or handling_own is not null"))[0].n, 0);
  const trg = await q("select tgname as t from pg_trigger where tgrelid in ('ops.master_edit_requests'::regclass, 'core.sku_costs'::regclass, 'ops.sku_component_requests'::regclass, 'ops.master_cutover_state'::regclass) and not tgisinternal order by 1");
  for (const t of ['trg_append_only_row', 'trg_sku_costs_no_overlap', 'trg_sku_component_requests_guard', 'trg_master_cutover_state_guard']) assert.ok(trg.some((r) => r.t === t), `trigger ${t} が無い`);
  const fn = await q("select pg_get_functiondef('core.guard_sku_cost_overlap()'::regprocedure) as d");
  assert.match(fn[0].d, /portal_master_edit/);
  for (const t of ['master_cutover_events', 'master_legacy_manifests', 'master_legacy_gate_acks', 'master_edit_requests', 'ne_set_observation_runs', 'ne_set_observations']) {
    assert.ok((await q("select 1 from pg_trigger where tgrelid = ('ops.' || $1)::regclass and tgname = 'trg_append_only_row'", [t])).length === 1, `${t} が追記だけでない`);
  }
  const acl = await q(`select p.proname, p.prosecdef, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p join pg_namespace n on n.oid = p.pronamespace
    where n.nspname in ('ops', 'core') and p.proname in ('set_master_cutover_phase', 'record_ne_set_observations', 'record_legacy_gate_ack', 'master_cutover_prereq_problems', 'audit_master_change', 'bump_parent_version', 'lock_suppliers_for_share') order by 1`);
  assert.deepEqual(acl.map((r) => [r.proname, r.prosecdef, r.pub]), [['audit_master_change', true, false], ['bump_parent_version', true, false], ['lock_suppliers_for_share', true, false], ['master_cutover_prereq_problems', true, false],
    ['record_legacy_gate_ack', true, false], ['record_ne_set_observations', true, false], ['set_master_cutover_phase', true, false]]);
  assert.deepEqual((await q("select ops.master_cutover_required_hosts() as h, ops.master_cutover_ack_fresh_minutes() as m, ops.master_cutover_prereq_problems('legacy_open', 'frozen') as p"))[0], { h: ['minipc', 'render'], m: 15, p: [] });
  // #1563 仮レビュー: 門の記録は場所ごとのログイン・止まった記録 (引数 10 個・後ろ 2 つは既定あり)・黙っているプロセスを見る時間・マスタの書き込みの鍵・原価の縮めるだけの UPDATE は見ない
  assert.deepEqual((await q("select pg_get_function_identity_arguments('ops.record_legacy_gate_ack(text, text, text, jsonb, text, text, integer, timestamptz, boolean, text)'::regprocedure) as a"))[0].a.split(', ').map((x) => x.split(' ')[0]),
    ['p_host', 'p_instance_id', 'p_build_id', 'p_manifest', 'p_owner_hash', 'p_phase_seen', 'p_inflight_count', 'p_oldest_inflight_at', 'p_stopped', 'p_stopped_reason']);
  assert.deepEqual((await q('select core.master_write_lock_key()::text as k'))[0], { k: '4705310051' });
  // #1563 R3: 黙っているプロセスは年齢で外さない (時間の窓の関数は無い)・前提の差し込み口の表 (空)・画面のロールの書き込みの約束 (begin + 7 つの表の guard)
  assert.equal((await q("select to_regprocedure('ops.master_cutover_ack_silent_hours()') is null as gone"))[0].gone, true);
  assert.deepEqual((await q('select name from ops.master_cutover_prereq_checks order by 1')).map((r) => r.name), ['0052_registrations', '0054_amazon_map']);   // 0051 は 0 行・0052 (⑤-2a)・0054 (⑦-1 消えた対応) が 1 行ずつ足す
  const guards = await q("select c.relnamespace::regnamespace::text || '.' || c.relname as t from pg_trigger g join pg_class c on c.oid = g.tgrelid where g.tgname = 'trg_master_edit_guard' order by 1");
  assert.deepEqual(guards.map((r) => r.t), ['core.products', 'core.sku_costs', 'core.skus', 'core.supplier_skus', 'ops.master_edit_requests', 'ops.sku_component_breaches', 'ops.sku_component_requests']);
  const be = (await q("select p.prosecdef, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p where p.oid = 'ops.begin_master_write(uuid, text, text, jsonb, text, bigint, text, text, jsonb)'::regprocedure"))[0];
  assert.deepEqual([be.prosecdef, be.pub], [true, false]);
  // #1563 R4: 止まった記録は書きかけ 0 (CHECK)・前提の表は追記だけ・約束の表に相手と操作
  assert.equal((await q("select count(*)::int as n from pg_constraint where conname = 'ck_mlga_stopped_drained'"))[0].n, 1);
  assert.deepEqual((await q("select tgname as t from pg_trigger where tgrelid = 'ops.master_cutover_prereq_checks'::regclass and not tgisinternal order by 1")).map((r) => r.t), ['trg_append_only_row', 'trg_append_only_stmt', 'trg_master_cutover_prereq_checks_guard']);
  assert.deepEqual((await q("select column_name as c from information_schema.columns where table_schema = 'ops' and table_name = 'master_write_sessions' and column_name in ('session_id', 'txid', 'operation', 'sku_id', 'derived_sku_ids', 'target_sku_ids', 'target_product_ids', 'edit_token', 'payload_hash', 'versions') order by 1")).map((r) => r.c),
    ['derived_sku_ids', 'edit_token', 'operation', 'payload_hash', 'session_id', 'sku_id', 'target_product_ids', 'txid', 'versions']);
  // #1563 R5: 約束の鍵 = session_id (乱数)・done の無い commit を拒む deferred の trigger・構成の依頼の形の CHECK
  assert.deepEqual((await q("select a.attname as c from pg_index i join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey) where i.indrelid = 'ops.master_write_sessions'::regclass and i.indisprimary")).map((r) => r.c), ['session_id']);
  assert.deepEqual((await q("select tgdeferrable as d, tginitdeferred as i from pg_trigger where tgname = 'trg_master_write_session_done'"))[0], { d: true, i: true });
  assert.equal((await q("select count(*)::int as n from pg_constraint where conrelid = 'ops.sku_component_requests'::regclass and contype = 'c' and pg_get_constraintdef(oid) like '%component_request_rows_ok%'"))[0].n, 2);
  const ackCols = await q("select column_name as c from information_schema.columns where table_schema = 'ops' and table_name = 'master_legacy_gate_acks' and column_name in ('session_role', 'stopped', 'stopped_reason') order by 1");
  assert.deepEqual(ackCols.map((r) => r.c), ['session_role', 'stopped', 'stopped_reason']);
  assert.match(fn[0].d, /tg_op = 'UPDATE' and new\.sku_id = old\.sku_id/);
  await rejects(() => q(`select ops.set_master_cutover_phase('frozen', 'x', '{}'::jsonb)`), /manifest_hash/);   // 門の記録も manifest も無い = 進めない
  // security definer の関数は search_path を固定して最後に pg_temp
  const cfg = await q("select p.proname, p.proconfig from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.prosecdef and n.nspname in ('ops', 'core') and p.proname in ('set_master_cutover_phase', 'record_ne_set_observations', 'record_legacy_gate_ack', 'master_cutover_prereq_problems', 'audit_master_change', 'bump_parent_version', 'lock_suppliers_for_share')");
  for (const r of cfg) assert.match(String(r.proconfig), /search_path=.*pg_temp/, r.proname);
});

await ta('[!] 0052 (14 ⑤-2a): 登録の状態は 1 行も作らない (backfill は切替の日に人が)・読む口 2 つ・守りの trigger・保存の記録に sku_create・登録の関数と 0051 の約束の sku_create', async () => {
  assert.equal((await q('select count(*)::int as n from ops.master_registrations'))[0].n, 0);
  assert.equal((await q('select count(*)::int as n from ops.master_registration_backfill'))[0].n, 0);
  const views = await q("select table_name as t from information_schema.views where table_schema = 'ops' and table_name in ('v_sku_available', 'v_sku_distributable') order by 1");
  assert.deepEqual(views.map((v) => v.t), ['v_sku_available', 'v_sku_distributable']);
  const trg = await q("select tgname as t from pg_trigger where tgrelid in ('ops.master_registrations'::regclass, 'ops.product_hub_outbox'::regclass, 'core.skus'::regclass, 'ops.master_registration_events'::regclass, 'ops.master_cutover_state'::regclass) and not tgisinternal order by 1");
  // 登録の状態を書く関数は security definer・search_path 固定・public の実行権なし (PR #1566 R1 H2)
  const fns = await q("select p.proname as n, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname in ('create_sku_registration', 'transition_sku_registration', 'backfill_sku_registrations', 'quarantine_unregistered_skus', 'registration_backfill_plan', 'sku_registration_problem', 'guard_product_hub_outbox_session', 'claim_card_events', 'finish_card_event', 'register_new_sku', 'new_sku_code_problem') order by 1");
  assert.equal(fns.length, 11);
  for (const f of fns) { assert.equal(f.d, true, f.n); assert.match(f.c, /search_path=pg_catalog, pg_temp/, f.n); assert.equal(f.pub, false, f.n); }
  for (const t of ['trg_master_registrations_guard', 'trg_master_registrations_no_truncate', 'trg_product_hub_outbox_guard', 'trg_skus_registered', 'trg_append_only_row', 'trg_master_cutover_state_prereq',
    'trg_product_hub_outbox_session']) assert.ok(trg.some((r) => r.t === t), `trigger ${t} が無い`);
  // 知らせの insert = 登録の約束の中だけ (0051 の guard は付けない = 知らせの表の相手を知らない。画面のロールには insert も渡さない)
  assert.equal((await q("select count(*)::int as n from pg_trigger where tgrelid = 'ops.product_hub_outbox'::regclass and tgname = 'trg_master_edit_guard'"))[0].n, 0);
  // 0051 の約束に「登録の約束」(sku_create・書くのは登録の関数だけ): 操作の CHECK・SKU の外部キーは遅らせられる (ふだんはすぐ・登録の関数だけ commit のとき)・書いてよい (表・書き方)
  assert.match((await q("select pg_get_constraintdef(oid) as d from pg_constraint where conname = 'ck_mws_operation'"))[0].d, /sku_edit.*sku_create/);
  assert.deepEqual((await q("select condeferrable as d, condeferred as i from pg_constraint where conrelid = 'ops.master_write_sessions'::regclass and contype = 'f' and confrelid = 'core.skus'::regclass and conname = 'fk_mws_sku'"))[0], { d: true, i: false });
  assert.deepEqual((await q(`select ops.master_write_allowed('sku_create', 'core.skus', 'INSERT') as a, ops.master_write_allowed('sku_create', 'core.products', 'INSERT') as b,
      ops.master_write_allowed('sku_create', 'core.sku_costs', 'DELETE') as c, ops.master_write_allowed('sku_create', 'ops.sku_component_breaches', 'UPDATE') as d,
      ops.master_write_allowed('sku_edit', 'core.skus', 'INSERT') as e, ops.master_write_allowed('sku_edit', 'core.sku_costs', 'DELETE') as f`))[0],
    { a: true, b: true, c: false, d: false, e: false, f: true });
  // 登録の関数の中だけで使う関数 (security definer にしない・public の実行権なし・search_path 固定)。JS の stable と同じ形 (Codex R3 Medium 1)
  const helpers = await q("select p.proname as n, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname in ('js_stable', 'js_stable_sha256', 'new_set_derivation') order by 1");
  assert.deepEqual(helpers.map((h) => [h.n, h.d, h.pub]), [['js_stable', false, false], ['js_stable_sha256', false, false], ['new_set_derivation', false, false]]);
  for (const h of helpers) assert.match(h.c, /search_path=pg_catalog, pg_temp/, h.n);
  assert.deepEqual((await q(`select ops.js_stable('{"b":[1,{"z":null,"a":"x\\n"}],"a":true}'::jsonb) as s`))[0].s, JSON.stringify({ a: true, b: [1, { a: 'x\n', z: null }] }));
  // new_open の前提 = ⑤-1 の差し込み口の表に 1 行 (集める関数は上書きしない・#1563 R3)。前提の関数は security definer にしない (集める関数が持ち主で呼ぶ)
  assert.deepEqual(await q("select name, fn::text as fn from ops.master_cutover_prereq_checks where name = '0052_registrations'"), [{ name: '0052_registrations', fn: 'ops.master_registrations_prereq(text,text)' }]);
  const pre = await q("select p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p where p.oid = 'ops.master_registrations_prereq(text, text)'::regprocedure");
  assert.deepEqual([pre[0].d, pre[0].pub], [false, false]); assert.match(pre[0].c, /search_path=pg_catalog, pg_temp/);
  assert.deepEqual(await q("select ops.master_cutover_prereq_problems('legacy_open', 'frozen') as p"), [{ p: [] }]);
  assert.match((await q("select ops.master_cutover_prereq_problems('company_owner', 'new_open') as p"))[0].p.join(' '), /^0052_registrations: backfill_missing/);
  const ck = await q("select pg_get_constraintdef(oid) as d from pg_constraint where conname = 'ck_mer_operation'");
  assert.match(ck[0].d, /sku_create/);
});

await ta('[!] 0053 (14 ⑤-2b): 新規登録の CSV の表 (追記だけ / 守り)・書く関数は security definer で public の実行権なし・0051 の約束に ⑤-2b の操作 (#1571 R1 High 3)・JAN の記録と専用の守り・仕入先の登録の状態と守り', async () => {
  for (const t of ['ne_reg_exports', 'ne_reg_attempts', 'ne_reg_export_items', 'ne_reg_export_rows', 'ne_reg_checks', 'supplier_registrations', 'supplier_registration_events',
    'ne_reg_compare_targets', 'ne_reg_compare_runs', 'ne_reg_compare_observations', 'ne_reg_compare_receipts']) {
    assert.ok((await q("select to_regclass('ops.' || $1) is not null as ok", [t]))[0].ok, `表 ops.${t} が無い`);
  }
  for (const t of ['ne_reg_attempts', 'ne_reg_export_rows', 'ne_reg_checks', 'supplier_registration_events', 'ne_reg_compare_targets', 'ne_reg_compare_runs', 'ne_reg_compare_observations', 'ne_reg_compare_receipts']) {
    assert.ok((await q("select 1 from pg_trigger where tgrelid = ('ops.' || $1)::regclass and tgname = 'trg_append_only_row'", [t])).length === 1, `${t} が追記だけでない`);
  }
  // 書く関数・守りは全部 security definer・search_path = pg_catalog, pg_temp・public の実行権なし (0052 の状態の関数も置き換えた)
  const want = [
    ['core', 'bump_jan_owner_version'], ['core', 'guard_master_edit_jan'],
    ['ops', 'close_reg_write'], ['ops', 'create_supplier'], ['ops', 'deactivate_supplier'], ['ops', 'declare_supplier_in_ne'], ['ops', 'edit_sku_jan'],
    ['ops', 'guard_reg_csv_live'], ['ops', 'guard_reg_csv_write'], ['ops', 'ne_reg_build'], ['ops', 'ne_reg_canonical'], ['ops', 'ne_reg_declare'], ['ops', 'ne_reg_guard_on_save'],
    ['ops', 'ne_reg_issue'], ['ops', 'ne_reg_lock_export'], ['ops', 'ne_reg_lock_skus'], ['ops', 'ne_reg_ne_codes'], ['ops', 'ne_reg_record_verified'], ['ops', 'ne_reg_supersede'],
    ['ops', 'ne_reg_supersede_built'], ['ops', 'open_reg_write'], ['ops', 'record_ne_registration_check'], ['ops', 'record_ne_registration_observations'], ['ops', 'reg_write_gate'],
    ['ops', 'seal_ne_registration_run'], ['ops', 'snapshot_ne_reg_targets'], ['ops', 'transition_sku_registration']];
  const acl = await q(`select n.nspname as s, p.proname as n, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub
      from pg_proc p join pg_namespace n on n.oid = p.pronamespace
     where (n.nspname::text, p.proname::text) in (select x ->> 0, x ->> 1 from jsonb_array_elements($1::jsonb) x) order by 1, 2`, [JSON.stringify(want)]);
  assert.deepEqual(acl.map((r) => [r.s, r.n]), want);
  for (const r of acl) assert.deepEqual([r.n, r.d, r.c, r.pub], [r.n, true, 'search_path=pg_catalog, pg_temp', false]);
  // 前の形の関数は無い (呼び手の約束を見る関数・lib が begin する形・仕入先の状態だけの関数)
  for (const f of ['ops.reg_write_session(text, uuid)', 'ops.create_supplier_registration(bigint, text)', 'core.guard_master_edit_jan_owner()']) {
    assert.equal((await q('select to_regprocedure($1) is null as gone', [f]))[0].gone, true, f);
  }
  // 確かめの関数は回の番号だけを受ける (#1571 R1 High 2・呼び手の観測の JSON の形は無い)
  assert.deepEqual((await q("select pg_get_function_identity_arguments(p.oid) as a from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname = 'record_ne_registration_check'")).map((r) => r.a), ['p_run text']);
  // 0051 の約束: 操作の CHECK に ⑤-2b の操作 (sku_edit・sku_create はそのまま)・SKU は sku_edit / sku_create だけ要る・書いてよい (表・書き方)・0051 の guard の表は変えない
  for (const c of ['ck_mws_operation', 'ck_mer_operation']) {
    const d = (await q('select pg_get_constraintdef(oid) as d from pg_constraint where conname = $1', [c]))[0].d;
    for (const op of ['sku_edit', 'sku_create', 'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate']) {
      assert.ok(d.includes(`'${op}'`), `${c}: ${op}`);
    }
  }
  assert.match((await q("select pg_get_constraintdef(oid) as d from pg_constraint where conname = 'ck_mws_sku_needed'"))[0].d, /sku_edit.*sku_create.*sku_id IS NOT NULL/);
  assert.deepEqual((await q(`select ops.master_write_allowed('reg_csv_build', 'ops.ne_reg_exports', 'INSERT') as a, ops.master_write_allowed('reg_csv_issue', 'ops.ne_reg_exports', 'INSERT') as b,
      ops.master_write_allowed('jan_edit', 'core.external_ids', 'INSERT') as c, ops.master_write_allowed('jan_edit', 'core.external_ids', 'DELETE') as d,
      ops.master_write_allowed('sku_edit', 'core.external_ids', 'INSERT') as e, ops.master_write_allowed('supplier_declare', 'core.suppliers', 'UPDATE') as f,
      ops.master_write_allowed('sku_edit', 'ops.ne_reg_export_items', 'UPDATE') as g, ops.master_write_allowed('sku_create', 'core.skus', 'INSERT') as h`))[0],
  { a: true, b: false, c: true, d: false, e: false, f: false, g: true, h: true });
  const trg = await q(`select c.relnamespace::regnamespace::text || '.' || c.relname || ' ' || g.tgname as t from pg_trigger g join pg_class c on c.oid = g.tgrelid
     where not g.tgisinternal and c.oid in ('core.external_ids'::regclass, 'core.suppliers'::regclass, 'core.supplier_skus'::regclass, 'core.skus'::regclass, 'core.products'::regclass,
       'core.sku_costs'::regclass, 'ops.sku_component_requests'::regclass, 'ops.ne_reg_exports'::regclass, 'ops.ne_reg_export_items'::regclass, 'ops.ne_reg_export_rows'::regclass,
       'ops.ne_reg_attempts'::regclass, 'ops.ne_csv_verified'::regclass) order by 1`);
  const has = new Set(trg.map((r) => r.t));
  for (const t of ['core.external_ids trg_external_ids_jan_guard', 'core.external_ids trg_external_ids_jan_no_delete', 'core.external_ids trg_external_ids_jan_audit',
    'core.external_ids trg_external_ids_jan_bump', 'core.external_ids trg_external_ids_writer', 'core.external_ids trg_master_edit_jan', 'core.suppliers trg_suppliers_lifecycle',
    'core.supplier_skus trg_supplier_skus_primary_registered', 'core.skus trg_reg_csv_live', 'core.products trg_reg_csv_live', 'core.supplier_skus trg_reg_csv_live',
    'core.sku_costs trg_reg_csv_live', 'ops.sku_component_requests trg_reg_csv_live', 'ops.ne_reg_exports trg_reg_csv_write', 'ops.ne_reg_export_items trg_reg_csv_write',
    'ops.ne_reg_export_rows trg_reg_csv_write', 'ops.ne_reg_attempts trg_reg_csv_write', 'ops.ne_csv_verified trg_reg_csv_write']) {
    assert.ok(has.has(t), `trigger ${t} が無い`);
  }
  // JAN の行の守りは専用 (0051 の guard は core.external_ids に付けない)
  assert.ok(!has.has('core.external_ids trg_master_edit_guard'));
  const ck = await q("select pg_get_constraintdef(oid) as d from pg_constraint where conname = 'master_change_events_entity_type_check'");
  assert.match(ck[0].d, /external_id/);
  // NOT VALID で足した (migration の中で前からの行を読まない・Low)。後の手順の VALIDATE が通る
  assert.equal((await q("select convalidated as v from pg_constraint where conname = 'master_change_events_entity_type_check'"))[0].v, false);
  await q('alter table events.master_change_events validate constraint master_change_events_entity_type_check');
  assert.equal((await q("select convalidated as v from pg_constraint where conname = 'master_change_events_entity_type_check'"))[0].v, true);
  const col = await q("select 1 from information_schema.columns where table_schema = 'ops' and table_name = 'ne_csv_verified' and column_name = 'reg_export_id'");
  assert.equal(col.length, 1);
});

await ta('[!] 0047 (13 §3.2・§3.7・D7b-1a): 財務の行に分けられない部品の 4 列 (既定 0)・日 × 正規化 SKU の関数 mart.finance_daily_sku_range', async () => {
  const cols = await q("select column_name as c, data_type as t, column_default as d, is_nullable as n from information_schema.columns where table_schema = 'core' and table_name = 'order_finance_daily' and column_name in ('unclassified_component_count', 'unclassified_mapped_jpy', 'unclassified_abs_jpy', 'unmapped_component_count') order by 1");
  assert.deepEqual(cols.map((r) => [r.c, r.t, r.d, r.n]), [
    ['unclassified_abs_jpy', 'bigint', '0', 'NO'], ['unclassified_component_count', 'integer', '0', 'NO'], ['unclassified_mapped_jpy', 'bigint', '0', 'NO'], ['unmapped_component_count', 'integer', '0', 'NO']]);
  const fns = await q("select p.proname as f, pg_get_function_identity_arguments(p.oid) as a from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'mart' and p.proname in ('finance_daily_range', 'finance_daily_sku_range') order by 1");
  assert.deepEqual(fns.map((r) => r.f), ['finance_daily_range', 'finance_daily_sku_range']);   // 今の関数は残す (R20 M4)
  assert.ok(fns.every((r) => /p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date/.test(r.a)), JSON.stringify(fns));
  const ck = await q("select conname as n, convalidated as v from pg_constraint where conname in ('ck_order_finance_daily_unclassified', 'ck_order_finance_daily_unmapped_count', 'ck_order_finance_daily_class_form') order by 1");
  assert.deepEqual(ck.map((r) => [r.n, r.v]), [['ck_order_finance_daily_class_form', true], ['ck_order_finance_daily_unclassified', true], ['ck_order_finance_daily_unmapped_count', true]]);   // 0048 で確かめ済み
});

await ta('[!] 0047 の CHECK は NOT VALID で足し (59 万行の検査を ACCESS EXCLUSIVE の中でしない)、0048 が既存の行で確かめる (#1554 Codex R1 Medium)', async () => {
  const p2 = new PGlite(); const d2 = pgliteAdapter(p2);
  await applyMigrations(d2, { log: quiet, to: '0047' });
  const st = async () => (await d2.query("select conname as n, convalidated as v from pg_constraint where conname in ('ck_order_finance_daily_unclassified', 'ck_order_finance_daily_unmapped_count', 'ck_order_finance_daily_class_form') order by 1")).rows.map((r) => [r.n, r.v]);
  assert.deepEqual(await st(), [['ck_order_finance_daily_class_form', false], ['ck_order_finance_daily_unclassified', false], ['ck_order_finance_daily_unmapped_count', false]]);
  const src47 = fs.readFileSync(path.join(DEFAULT_DIR, '0047_finance_sku_range.sql'), 'utf8');
  const code47 = src47.split(/\r?\n/).map((l) => l.replace(/--.*$/, '')).join('\n');   // 注釈を除いた SQL
  assert.ok(!/validate\s+constraint/i.test(code47), '0047 で既存の行を検査している');
  assert.equal((code47.match(/\bnot valid\b/gi) || []).length, 3, '0047 の 3 つの CHECK が NOT VALID でない');
  await applyMigrations(d2, { log: quiet });
  assert.deepEqual(await st(), [['ck_order_finance_daily_class_form', true], ['ck_order_finance_daily_unclassified', true], ['ck_order_finance_daily_unmapped_count', true]]);
  await p2.close();
});

await ta('[!] 0049 (13 §3.5・§3.6・D7b-3): Amazon の利益の mart = 関数だけ (表は作らない)・決済のそろいの差し込み口は今は null・監査の始まり = 0049 の適用の時刻', async () => {
  const fns = await q(`select n.nspname || '.' || p.proname as f, l.lanname as lang, pg_get_function_identity_arguments(p.oid) as a
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace join pg_language l on l.oid = p.prolang
   where (n.nspname = 'mart' and p.proname like any (array['amazon_profit%', '\\_amazon%', 'amazon_account_fee_tax_rate'])) or (n.nspname = 'core' and p.proname = 'finance_coverage_state') order by 1`);
  assert.deepEqual(fns.map((r) => [r.f, r.lang]), [
    ['core.finance_coverage_state', 'plpgsql'],
    ['mart._amazon_easy_ship_alloc', 'sql'], ['mart._amazon_profit_ad_children', 'sql'], ['mart._amazon_profit_ad_days', 'sql'],
    ['mart._amazon_profit_finance_days', 'sql'], ['mart._amazon_profit_rows', 'sql'], ['mart._amazon_profit_totals', 'sql'],
    ['mart.amazon_account_fee_tax_rate', 'sql'], ['mart.amazon_profit_assert_args', 'plpgsql'], ['mart.amazon_profit_composition_audit_since', 'sql'],
    ['mart.amazon_profit_daily_range', 'plpgsql'], ['mart.amazon_profit_day_totals_range', 'plpgsql']]);
  for (const f of ['mart.amazon_profit_daily_range', 'mart.amazon_profit_day_totals_range']) {
    assert.equal(fns.find((r) => r.f === f).a, 'p_company_id smallint, p_mall text, p_scope_key text, p_from date, p_to date', f);
  }
  // D7b-1b が差し替えるまで 1 行・全部 null (D7b-1b はこの関数だけ差し替える = 引数と戻りの形を固定)
  assert.deepEqual(await q(`select * from core.finance_coverage_state(1::smallint, 'amazon', 'jp', 'amazon_settlement_unified')`), [{ complete_to: null, generation: null, source_revision: null }]);
  assert.equal((await q(`select pg_get_function_result('core.finance_coverage_state(smallint,text,text,text)'::regprocedure) as r`))[0].r, 'TABLE(complete_to date, generation bigint, source_revision bigint)');
  const types = await q(`select t.typname as n from pg_type t join pg_namespace n on n.oid = t.typnamespace where n.nspname = 'mart' and t.typtype = 'c' and t.typname like 'amazon%' order by 1`);
  assert.deepEqual(types.map((r) => r.n), ['amazon_easy_ship_alloc_row', 'amazon_profit_ad_child', 'amazon_profit_ad_day', 'amazon_profit_finance_day']);   // 内部の材料の型 (1 回だけ計算して渡す)
  const [s] = await q(`select mart.amazon_profit_composition_audit_since() as since, (select applied_at from ops.schema_migrations where version = '0049') as applied`);
  assert.ok(Math.abs(new Date(s.since).getTime() - new Date(s.applied).getTime()) < 600e3, JSON.stringify(s));
  const tables = await q(`select count(*)::int as n from information_schema.tables where table_name like '%amazon_profit%'`);
  assert.equal(tables[0].n, 0);
});

await ta('[!] 0050 (13 §3.1・D7b-1b-2): 決済のそろい core.finance_coverage = 会社 × モール × scope × source の 1 行・core.finance_coverage_state は同じ形のまま差し替え (行なし = 全部 null)', async () => {
  const pk = await q(`select a.attname as c from pg_index i join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
    where i.indrelid = 'core.finance_coverage'::regclass and i.indisprimary order by array_position(i.indkey, a.attnum)`);
  assert.deepEqual(pk.map((r) => r.c), ['company_id', 'mall', 'scope_key', 'source']);
  const ck = await q(`select conname as n from pg_constraint where conrelid = 'core.finance_coverage'::regclass and contype = 'c' and conname like 'ck\\_finance\\_coverage\\_%' order by 1`);
  assert.deepEqual(ck.map((r) => r.n), ['ck_finance_coverage_complete', 'ck_finance_coverage_complete_to', 'ck_finance_coverage_evidence', 'ck_finance_coverage_invalidated', 'ck_finance_coverage_receipts']);
  assert.equal((await q(`select pg_get_function_result('core.finance_coverage_state(smallint,text,text,text)'::regprocedure) as r`))[0].r, 'TABLE(complete_to date, generation bigint, source_revision bigint)');
  assert.deepEqual(await q(`select * from core.finance_coverage_state(1::smallint, 'amazon', 'jp', 'amazon_settlement_unified')`), [{ complete_to: null, generation: null, source_revision: null }]);
  assert.equal((await q(`select count(*)::int as n from core.finance_coverage`))[0].n, 0);   // 作るだけ (値は coordinator = D7b-1b-3 が送る)
  // policy の指紋 (#1561 Codex R2 High) と、0047 の関数の差し替え (partial = coverage 基準・同じ引数と戻り・R2 Medium)
  assert.match((await q(`select core.finance_policy_fingerprint(1::smallint, 'amazon', 'jp') as f`))[0].f, /^[0-9a-f]{64}$/);
  const src = (await q(`select prosrc as s from pg_proc where oid = 'mart.finance_daily_sku_range(smallint,text,text,date,date)'::regprocedure`))[0].s;
  assert.ok(src.includes('core.finance_month_settled') && !src.includes('statement_timestamp'), '0047 の partial が今日基準のまま');
  // 0049 の行の関数の再判定も「月の全部の日」(#1561 Codex R3 High 1)。返品の日の source の complete_to だけの旧い式は残っていない
  const rows = (await q(`select prosrc as s from pg_proc where proname = '_amazon_profit_rows'`))[0].s;
  assert.ok(rows.includes('core.finance_month_settled') && !rows.includes('d.complete_to is null or'), '0049 の再判定が返品の日の source だけのまま');
  assert.deepEqual(await q(`select source_policy_count, origin_from::text as o from core.finance_policy_snapshot(1::smallint, 'amazon', 'jp', 'amazon_settlement_unified')`), [{ source_policy_count: 1, o: '2026-01-01' }]);
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
  // 親子の書き換えは 0036 の守り (約束の印と親子の鍵) を付けたうえで、会社の違う親を FK が拒む
  await q('begin');
  try {
    await q("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
    await rejects(() => q("update core.products set parent_product_id = $2 where product_id = $1", [productId, p2]), /foreign key|violates/i);
  } finally { await q('rollback'); }
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

await ta('[!] v_product_360 の ASIN は単品出品のものだけ (セットの ASIN を中身の SKU の ASIN にしない。0009)', async () => {
  // 「単品 A + セット B」の 2 点セット出品を Amazon に作る。ASIN は既存より後ろに並ぶ値にして、
  // 絞っていなければ max() がこちらを拾ってしまうようにする (直す前なら落ちる形にする)
  const comboAsin = (await q("insert into core.catalog_items (marketplace_id, asin, package_scope, pack_count) values ('A1VC38T7YXB528', 'B000ZZCOMBO', 'multipack', 2) returning catalog_item_id"))[0].catalog_item_id;
  const comboListing = (await q("insert into core.listings (company_id, mall, shop_code, listing_code, catalog_item_id, status) values (1, 'amazon', 'main@A1VC38T7YXB528', 'combo-a-plus-set', $1, 'active') returning listing_id", [comboAsin]))[0].listing_id;
  await q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'manual', 'human'), (1, $1, $3, 1, 'manual', 'human')", [comboListing, skuId, setSkuId]);
  await q("insert into core.listing_states (listing_id, status, price_jpy, observed_at, snapshot_run_id) values ($1, 'active', 4000, now(), 'run1')", [comboListing]);

  const single = (await q('select asin, listing_count from mart.v_product_360 where sku_id = $1', [skuId]))[0];
  assert.equal(single.asin, null, 'セット出品の ASIN は、中に入っている単品の ASIN にならない');
  assert.ok(Number(single.listing_count) >= 3, '出品の数としては数える (見えなくするわけではない)');
  const set = (await q('select asin from mart.v_product_360 where sku_id = $1', [setSkuId]))[0];
  assert.equal(set.asin, 'B000TEST01', 'セット SKU 自身の単品出品の ASIN は残る');
  // 出品の配列には両方出る (どの出品にどの ASIN が付いているかは見える)
  const listings = (await q('select listings_json from mart.v_product_360 where sku_id = $1', [skuId]))[0].listings_json;
  assert.ok(listings.some((x) => x.asin === 'B000ZZCOMBO'), JSON.stringify(listings.map((x) => x.asin)));

  // 「同じ商品を 2 個入り」で売る出品 (構成は 1 行だが qty=2) も単品ではない。
  // 2 個入りには 2 個入りの ASIN が付くので、1 個の SKU の ASIN にしない
  const packAsin = (await q("insert into core.catalog_items (marketplace_id, asin, package_scope, pack_count) values ('A1VC38T7YXB528', 'B000ZZPACK2', 'multipack', 2) returning catalog_item_id"))[0].catalog_item_id;
  const packListing = (await q("insert into core.listings (company_id, mall, shop_code, listing_code, catalog_item_id, status) values (1, 'amazon', 'main@A1VC38T7YXB528', 'abc001-x2', $1, 'active') returning listing_id", [packAsin]))[0].listing_id;
  await q("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 2, 'manual', 'human')", [packListing, skuId]);
  await q("insert into core.listing_states (listing_id, status, price_jpy, observed_at, snapshot_run_id) values ($1, 'active', 3500, now(), 'run1')", [packListing]);
  const afterPack = (await q('select asin, listings_json from mart.v_product_360 where sku_id = $1', [skuId]))[0];
  assert.equal(afterPack.asin, null, '2 個入り出品の ASIN も、1 個の SKU の ASIN にしない');
  assert.ok(afterPack.listings_json.some((x) => x.asin === 'B000ZZPACK2'), '出品の配列には出る');
});

await pglite.close();
console.log(`\n${passed} 件 PASS`);
