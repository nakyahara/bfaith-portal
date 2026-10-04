#!/usr/bin/env node
/**
 * test-company-db-d60-load-count-indexes-pg.mjs — D-60 PR 3a-i の index 6 つの migration (db/company/migrations/<番号>_d60_load_count_indexes.sql) を確かめる
 *   設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』(v40・D-60 v3.15) の付録 B (B.2 段のパイプライン・B.3 許す plan の木) と
 *   §3.10「migrate の runner の契約 (3a-i)」。runner = scripts/company-db/migrate.mjs (#1606)
 *
 * 固定する契約:
 *   S1 file の中身 = 付録 B の 6 つ (名前・表・列の並び・CASE の式・部分 index の条件・INCLUDE なし・unique でない・btree・昇順) / 横の expect.json も同じ 6 つ (PG 18)・
 *      本文と expect.json は番号を書かない (付け替え = git mv だけ) / ⑨a・⑨b の index の式 = 付録 B の本文の式 (別名 e. を外すと同じ文字)
 *   R1 本物の PG 18 の runner の legacy の道 (migrateWithLock・取引の外の CREATE INDEX CONCURRENTLY):
 *      a 一度も ANALYZE していない表 (新しい DB) = 見積もれない = 何も作らずに DISK_CHECK_FAILED (fail-closed・本番の手順の「先に reltuples を確かめる」)
 *      b 本番の CLI の容量の読み手は RENDER_PG_HOST_MAPPING.confirmed = false の間 HOST_MAPPING_UNCONFIRMED = 何も作らない (Render の回答の前にマージしない理由)
 *      c --dry-run 相当 = 作らずに pending に出る
 *      d データと統計のある DB = 6 つを作り、indisvalid・indisready・indislive と expect.json の属性が一致して記録・applied_by = migrate-v2・lock が残らない・
 *        もう一度流すと 0 本
 *   R2 門の GUC (付録 B.2) の下で 14 段 (①〜⑨b の 12 段 + (b)-1・(b)-2) の EXPLAIN (FORMAT JSON) が許す木 (付録 B.3) だけ:
 *      ①〜⑤ = Limit → (Index Scan | Index Only Scan) / ⑥〜⑨b・(b) = Aggregate (Plain) → [Subquery Scan] → Limit → scan・
 *      scan の Index Name が付録 B の表どおり・Filter なし・Index Cond に段の列・Disabled なし・ほかの node なし
 *   R3 EXPLAIN ANALYZE で、上限 100 のとき ①〜⑨b の scan が 101 行で止まる (データは各段 101 行より多い)
 *   R4 新しい index を 1 つずつ外すと (取引の中で drop → rollback)、その段の木が不合格になる (= 6 つとも段の上限に要る)
 *   I1 (PR 3a への事実) 段の文の多くは $1〜$7 の一部しか使わない = pg の driver に 7 つの値をそのまま渡すと 42P18 (型が決まらない)
 *   P1 PGlite の道 (applyMigrations・concurrently を外して取引の中) で同じ file が通り、属性が同じ expect.json に一致し、14 段の木も同じ形
 * 段の SQL の文字は付録 B の正本の写し (PR 3a で apps/company-db/profit/load-count.mjs に同じ文を置き、文字の一致を試験にする)。
 * 使い方: node scripts/test-company-db-d60-load-count-indexes-pg.mjs   (npm run test:company-db にも入っている)
 *   使い捨てのクラスタを embedded-postgres で起動し、最後に止めて消す (test-company-db-migrate-lock-pg.mjs と同じ作り)。
 *   見つからない・版が違う・起動できない・フォルダが消えない = 失敗 (exit 1)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import {
  openPgClient, pgAdapter, pgliteAdapter, applyMigrations, migrateWithLock, listMigrationFiles, readIndexAttrs, attrDiff, expectPathOf,
  splitSqlStatements, parseConcurrentIndexStatement, planConcurrentIndexFile, renderDiskMetricsReader, readOwnerMode, MIGRATE_LOCK_NAME, DEFAULT_DIR,
} from './company-db/migrate.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// ─── 番号を持たない = 付け替え (#1605・#1607 の後の次の空き) は 2 つの file を git mv するだけ ───
export const D60_IDX_NAME = 'd60_load_count_indexes';
const FILE = listMigrationFiles().find((f) => f.name === D60_IDX_NAME);
if (!FILE) { console.error(`❌ db/company/migrations/NNNN_${D60_IDX_NAME}.sql が無い`); process.exit(1); }
const VER = FILE.version;
const PREV = String(Number(VER) - 1).padStart(4, '0');

/** 付録 B の index 6 つ (正本の定義) */
const NEW_INDEXES = {
  'core.ix_listings_company_mall_norm': { stage: '④a', table: 'core.listings', columns: ['company_id', 'mall', 'listing_norm'], predicate: null },
  'core.ix_external_ids_listing_alias': { stage: '④b', table: 'core.external_ids', columns: ['company_id', 'system', 'external_norm'], predicate: "((entity_type = 'listing'::text) AND (valid_to IS NULL))" },
  'core.ix_sku_costs_company_sku_from': { stage: '⑥・⑥b', table: 'core.sku_costs', columns: ['company_id', 'sku_id', 'valid_from'], predicate: null },
  'events.ix_master_change_events_component_listing': { stage: '⑨a', table: 'events.master_change_events', columns: [null], expr: "case when entity_type = 'listing_component' then entity_key ->> 'listing_id' end" },
  'events.ix_master_change_events_component_old_listing': { stage: '⑨b', table: 'events.master_change_events', columns: [null], expr: "case when entity_type = 'listing_component' and operation = 'UPDATE' and attribute = 'listing_id' then old_value #>> '{}' end" },
  'raw.ix_logizard_inventory_obs_observed_at': { stage: '(b)-2', table: 'raw.logizard_inventory_observations', columns: ['observed_at'], predicate: null },
};

/**
 * 付録 B.2 の段 (SQL の文字は正本の写し)。引数 = $1 会社・$2 モール・$3 scope・$4 月の初日・$5 翌月の初日・$6 上限・$7 鍵の配列。
 * 🚨 段の多くは $1〜$7 の一部しか使わない (例 ⑤ = $6・$7) = 型の分からない引数がある = PREPARE で 7 つの型を明示する (PR 3a の driver も同じ扱いが要る = PR 本文の迷った所)
 * (b)-1・(b)-2 は書く関数の中の文 (p_company_id・p_keep_days を $1 に置いた・上限は文の中の定数)
 */
const STAGES = [
  { id: '①', kind: 'rows', index: 'ix_order_finance_daily_date', cond: ['company_id', 'mall', 'scope_key', 'economic_date_jst'], keys: null,
    sql: "select f.economic_date_jst as d, f.line_kind as lk, core.norm_code(f.seller_sku) as k, case when f.line_kind = 'easy_ship' then f.mall_order_no end as es_order from core.order_finance_daily f where f.company_id = $1 and f.mall = $2 and f.scope_key = $3 and f.economic_date_jst >= $4 and f.economic_date_jst < $5 limit $6 + 1" },
  { id: '②', kind: 'rows', index: 'ad_spend_daily_pkey', cond: ['company_id', 'mall', 'scope_key', 'ad_type', 'date_jst'], keys: null,
    sql: "select a.date_jst as d, a.target_granularity as g, core.norm_code(a.target_code) as k from core.ad_spend_daily a where a.company_id = $1 and a.mall = $2 and a.scope_key = $3 and a.ad_type = 'SP' and a.date_jst >= $4 and a.date_jst < $5 limit $6 + 1" },
  { id: '③', kind: 'rows', index: 'order_finance_daily_pkey', cond: ['company_id', 'mall', 'scope_key', 'mall_order_no'], keys: 'E',
    sql: 'select f.mall_order_no as o, f.line_kind as lk, core.norm_code(f.seller_sku) as k from core.order_finance_daily f where f.company_id = $1 and f.mall = $2 and f.scope_key = $3 and f.mall_order_no = any($7::text[]) limit $6 + 1' },
  { id: '④a', kind: 'rows', index: 'ix_listings_company_mall_norm', cond: ['company_id', 'mall', 'listing_norm'], keys: 'NT',
    sql: 'select l.listing_id from core.listings l where l.company_id = $1 and l.mall = $2 and l.listing_norm = any($7::text[]) limit $6 + 1' },
  { id: '④b', kind: 'rows', index: 'ix_external_ids_listing_alias', cond: ['company_id', 'system', 'external_norm'], keys: 'T',
    sql: "select x.entity_id as listing_id from core.external_ids x where x.company_id = $1 and x.entity_type = 'listing' and x.system = $2 and x.valid_to is null and x.external_norm = any($7::text[]) limit $6 + 1" },
  { id: '⑤', kind: 'rows', index: 'listing_components_pkey', cond: ['listing_id'], keys: 'L',
    sql: 'select c.listing_id, c.sku_id from core.listing_components c where c.listing_id = any($7::bigint[]) limit $6 + 1' },
  { id: '⑥', kind: 'count', index: 'ix_sku_costs_company_sku_from', cond: ['company_id', 'sku_id'], keys: 'S',
    sql: 'select count(*) from (select 1 from core.sku_costs c where c.company_id = $1 and c.sku_id = any($7::bigint[]) limit $6 + 1) s' },
  { id: '⑥b', kind: 'count', index: 'ix_sku_costs_company_sku_from', cond: ['company_id'], keys: null,
    sql: 'select count(*) from (select 1 from core.sku_costs c where c.company_id = $1 limit $6 + 1) s' },
  { id: '⑦', kind: 'count', index: 'ix_sku_cost_observed_sku', cond: ['company_id', 'sku_id'], keys: 'S',
    sql: 'select count(*) from (select 1 from core.sku_cost_observed o where o.company_id = $1 and o.sku_id = any($7::bigint[]) limit $6 + 1) s' },
  { id: '⑧', kind: 'count', index: 'ix_master_change_events_entity', cond: ['entity_type', 'entity_id'], keys: 'L',
    sql: "select count(*) from (select 1 from events.master_change_events e where e.entity_type = 'listing' and e.entity_id = any($7::bigint[]) limit $6 + 1) s" },
  { id: '⑨a', kind: 'count', index: 'ix_master_change_events_component_listing', cond: ['CASE', 'entity_key'], keys: 'Ltext',
    sql: "select count(*) from (select 1 from events.master_change_events e where (case when e.entity_type = 'listing_component' then e.entity_key ->> 'listing_id' end) = any($7::text[]) limit $6 + 1) s" },
  { id: '⑨b', kind: 'count', index: 'ix_master_change_events_component_old_listing', cond: ['CASE', 'old_value'], keys: 'Ltext',
    sql: "select count(*) from (select 1 from events.master_change_events e where (case when e.entity_type = 'listing_component' and e.operation = 'UPDATE' and e.attribute = 'listing_id' then e.old_value #>> '{}' end) = any($7::text[]) limit $6 + 1) s" },
  { id: '(b)-1', kind: 'count', index: 'ix_ad_spend_daily_unlinked', cond: ['company_id'], fn: true, types: ['smallint'],
    sql: "select count(*) from (select 1 from core.ad_spend_daily a where a.company_id = $1 and a.listing_id is null and a.target_granularity = 'sku' limit 50001) s" },
  { id: '(b)-2', kind: 'count', index: 'ix_logizard_inventory_obs_observed_at', cond: ['observed_at'], fn: true, types: ['integer'],
    sql: 'select count(*) from (select 1 from raw.logizard_inventory_observations o where o.observed_at < now() - make_interval(days => $1) limit 500001) s' },
];
const KEY_TYPE = { E: 'text[]', NT: 'text[]', T: 'text[]', L: 'bigint[]', S: 'bigint[]', Ltext: 'text[]' };

/** 門の GUC (付録 B.2・v3.9) + 1 回の要求の取引の 2. の並列 0 */
const GATE_GUCS = ['enable_seqscan = off', 'enable_bitmapscan = off', 'enable_tidscan = off', 'enable_sort = off', 'enable_incremental_sort = off',
  'enable_hashagg = off', 'enable_hashjoin = off', 'enable_mergejoin = off', 'enable_nestloop = off', 'enable_material = off', 'enable_memoize = off',
  'plan_cache_mode = force_custom_plan', 'cursor_tuple_fraction = 1.0', 'max_parallel_workers_per_gather = 0'];
const ALLOWED_SCANS = new Set(['Index Scan', 'Index Only Scan']);
const cut = (s) => (String(s).length > 120 ? String(s).slice(0, 120) + '…' : String(s));   // 理由の文に鍵の配列の全部を出さない

/** 付録 B.3 の木の検査 (違反の理由の配列・合格なら []) */
export function checkPlan(plan, st) {
  const errs = [];
  const nodes = [];
  (function walk(n) { nodes.push(n); for (const k of n.Plans || []) walk(k); })(plan);
  for (const n of nodes) if (n.Disabled === true) errs.push(`Disabled の node (${n['Node Type']})`);
  const only = (n) => { if (!n.Plans || n.Plans.length !== 1) { errs.push(`${n['Node Type']} の子が 1 つでない`); return null; } return n.Plans[0]; };
  let n = plan;
  const path = [];
  if (st.kind === 'count') {
    if (n['Node Type'] !== 'Aggregate' || n.Strategy !== 'Plain') errs.push(`一番上が Aggregate (Plain) でない (${n['Node Type']}${n.Strategy ? ' ' + n.Strategy : ''})`);
    path.push(n); n = only(n);
    if (n && n['Node Type'] === 'Subquery Scan') { path.push(n); n = only(n); }
  }
  if (n && n['Node Type'] !== 'Limit') errs.push(`Limit が要る位置に ${n['Node Type']}`);
  if (n) { path.push(n); n = only(n); }
  if (n) {
    path.push(n);
    if (!ALLOWED_SCANS.has(n['Node Type'])) errs.push(`scan が ${n['Node Type']} (許すのは Index Scan / Index Only Scan だけ)`);
    if (n.Plans && n.Plans.length) errs.push(`scan の下に node がある (${n.Plans.map((x) => x['Node Type']).join(', ')})`);
    if (n['Index Name'] !== st.index) errs.push(`Index Name が ${n['Index Name']} (期待 ${st.index})`);
    if (Object.hasOwn(n, 'Filter')) errs.push(`Filter がある (${cut(n.Filter)})`);
    const cond = n['Index Cond'];
    if (!cond) errs.push('Index Cond が無い');
    else for (const c of st.cond) if (!cond.includes(c)) errs.push(`Index Cond に ${c} が無い (${cut(cond)})`);
  }
  if (nodes.length !== path.length) errs.push(`許す木の外の node がある (${nodes.map((x) => x['Node Type']).join(' → ')})`);
  return { errs, scan: path[path.length - 1] || null, shape: nodes.map((x) => x['Node Type']).join(' → ') };
}

// ─── データ (本番の形に寄せた小さめの量・FK と trigger は試験の準備だけ session_replication_role = replica で外す) ───
const MONTH = { from: '2026-09-01', to: '2026-10-01' };
function dataSql(k = 1) {
  const [nL, nSku, nFin, nEs, nAd, nAlias, nObs] = [1000, 400, 6000, 300, 3000, 800, 5000].map((n) => Math.round(n * k));
  return `
set session_replication_role = replica;
insert into core.listings (company_id, mall, listing_code) select 1, 'amazon', 'SKU' || g from generate_series(1, ${nL}) g;
insert into core.listings (company_id, mall, listing_code) select 1, 'rakuten', 'SKU' || g from generate_series(1, ${nL / 2}) g;
insert into core.listings (company_id, mall, shop_code, listing_code) select 2, 'amazon', 'other', 'SKU' || g from generate_series(1, ${nL / 2}) g;
insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, line_kind, source, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash)
  select 1, 'amazon', 'jp', 'O' || (g / 6), date '${MONTH.from}' + (g % 30), 'SKU' || (g % ${nSku} + 1), 'sku', 'amazon_settlement_flat_v2', 1, 1, now(), 't', md5(g::text) from generate_series(1, ${nFin}) g
  union all
  select 1, 'amazon', 'jp', 'O' || g, date '${MONTH.from}' + (g % 30), '-', 'easy_ship', 'amazon_settlement_flat_v2', 1, 1, now(), 't', md5(g::text) from generate_series(1, ${nEs}) g
  union all
  select 1, 'amazon', 'jp', 'A' || g, date '2026-08-01' + (g % 31), 'SKU' || (g % ${nSku} + 1), 'sku', 'amazon_settlement_flat_v2', 1, 1, now(), 't', md5(g::text) from generate_series(1, ${nFin}) g
  union all
  select 1, 'amazon', 'other', 'B' || g, date '${MONTH.from}' + (g % 30), 'SKU' || (g % ${nSku} + 1), 'sku', 'amazon_settlement_flat_v2', 1, 1, now(), 't', md5(g::text) from generate_series(1, ${nFin / 2}) g
  union all
  select 1, 'amazon', 'jp', '-:' || (date '${MONTH.from}' + g)::text, date '${MONTH.from}' + g, '-', 'storage', 'amazon_settlement_flat_v2', 1, 1, now(), 't', md5(g::text) from generate_series(0, 29) g;
insert into core.ad_spend_daily (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code, clicks, impressions, ad_cost, ingest_run_id)
  select 1, 'amazon', 'jp', 'SP', date '${MONTH.from}' + (g % 30), 'C' || (g % 7), 'sku',
         case when g % 2 = 0 then 'ALIAS' || (g % ${nAlias} + 1) else 'SKU' || (g % ${nL} + 1) end, 1, 10, 1.00, 'run' from generate_series(1, ${nAd}) g
  union all
  select 1, 'amazon', 'jp', 'SP', date '${MONTH.from}' + (g % 30), 'X' || g, 'asin', 'B0' || g, 1, 10, 1.00, 'run' from generate_series(1, ${nAd / 10}) g
  union all
  select 1, 'amazon', 'jp', 'SP', date '${MONTH.from}' + (g % 30), 'Y' || g, 'none', '', 1, 10, 1.00, 'run' from generate_series(1, ${nAd / 10}) g
  union all
  select 1, 'amazon', 'jp', 'SP', date '2026-08-01' + (g % 31), 'Z' || g, 'sku', 'SKU' || g, 1, 10, 1.00, 'run' from generate_series(1, ${nAd}) g;
insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type)
  select 1, 'listing', (g % ${nL}) + 1, 'amazon', 'seller_sku', 'ALIAS' || g, 'manual', 'human' from generate_series(1, ${nAlias}) g
  union all
  select 1, 'sku', g, 'ne', 'product_code', 'P' || g, 'imported', 'system' from generate_series(1, ${nSku}) g
  union all
  select 1, 'listing', g, 'rakuten', 'manage_number', 'ALIAS' || g, 'imported', 'system' from generate_series(1, ${nAlias / 4}) g;
insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, valid_from, valid_to)
  select 1, 'listing', g, 'amazon', 'seller_sku', 'ALIAS' || g, 'manual', 'human', now() - interval '2 days', now() - interval '1 day' from generate_series(1, ${nAlias / 4}) g;
insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type)
  select 1, l, (l % ${nSku}) + 1, 1, 'exact', 'system' from generate_series(1, ${nL}) l
  union all
  select 1, l, ((l + 7) % ${nSku}) + 1, 2, 'exact', 'system' from generate_series(1, ${nL}) l;
insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to)
  select 1, s, 100, 'ne', 'COMPLETE', d.vf, d.vt from generate_series(1, ${nSku}) s,
         (values (date '2026-01-01', date '2026-03-31'), (date '2026-04-01', date '2026-06-30'), (date '2026-07-01', null::date)) d(vf, vt)
  union all
  select 2, ${nSku} + s, 100, 'ne', 'COMPLETE', date '2026-01-01', null from generate_series(1, ${nSku}) s;
insert into core.sku_cost_observed (observed_load_id, company_id, generation, sku_id, product_code, cost_jpy, cost_status, valid_from, valid_to, backfill_method, first_observed_at, source_history_id)
  select 1, 1, 1, s, 'P' || s, 100, 'COMPLETE', d.vf, d.vt, 'observed_daily_diff', now(), s from generate_series(1, ${nSku}) s,
         (values (date '2026-01-01', date '2026-06-30'), (date '2026-07-01', null::date)) d(vf, vt);
insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_id, entity_key, attribute, old_value, new_value, actor_type, source_system)
  select 1, gen_random_uuid(), 'INSERT', 'listing', l::bigint, jsonb_build_object('listing_id', l), null::text, null::jsonb, jsonb_build_object('x', 1), 'system', 'test' from generate_series(1, ${nL}) l
  union all
  select 1, gen_random_uuid(), 'INSERT', 'listing_component', null::bigint, jsonb_build_object('listing_id', l, 'sku_id', (l % ${nSku}) + 1), null::text, null::jsonb, jsonb_build_object('qty', 1), 'system', 'test' from generate_series(1, ${nL}) l
  union all
  select 1, gen_random_uuid(), 'UPDATE', 'listing_component', null, jsonb_build_object('listing_id', l + 1, 'sku_id', 1), 'listing_id', to_jsonb(l), to_jsonb(l + 1), 'system', 'test' from generate_series(1, ${nL / 2}) l
  union all
  select 1, gen_random_uuid(), 'UPDATE', 'listing_component', null, jsonb_build_object('listing_id', l, 'sku_id', 1), 'qty', to_jsonb(1), to_jsonb(2), 'system', 'test' from generate_series(1, ${nL / 2}) l
  union all
  select 1, gen_random_uuid(), 'INSERT', 'sku', s::bigint, jsonb_build_object('sku_id', s), null::text, null::jsonb, jsonb_build_object('x', 1), 'system', 'test' from generate_series(1, ${nSku * 3}) s;
insert into raw.logizard_inventory_observations (ingest_run_id, scope_key, business_key, content_hash, fetch_status, observed_at)
  select 'run' || (g % 50), 'lz', 'k' || g, null, 'not_found', now() - make_interval(hours => g) from generate_series(1, ${nObs}) g;
reset session_replication_role;
analyze;
`;
}

/** アプリが段ごとに作る鍵の配列 (付録 B.2 の「アプリが作る次の材料」を SQL で写した = 試験の準備) */
async function buildKeys(q) {
  const one = async (sql, p = []) => (await q(sql, p)).rows[0].a || [];
  const P = [1, 'amazon', 'jp', MONTH.from, MONTH.to];
  const E = await one("select array_agg(distinct f.mall_order_no order by f.mall_order_no) as a from core.order_finance_daily f where f.company_id = $1 and f.mall = $2 and f.scope_key = $3 and f.economic_date_jst >= $4 and f.economic_date_jst < $5 and f.line_kind = 'easy_ship' and f.mall_order_no not like '-%'", P);
  const N = await one(`select array_agg(distinct k order by k) as a from (
      select core.norm_code(f.seller_sku) as k from core.order_finance_daily f where f.company_id = $1 and f.mall = $2 and f.scope_key = $3 and f.economic_date_jst >= $4 and f.economic_date_jst < $5 and f.line_kind = 'sku'
      union all select core.norm_code(f.seller_sku) from core.order_finance_daily f where f.company_id = $1 and f.mall = $2 and f.scope_key = $3 and f.mall_order_no = any($6::text[]) and f.line_kind = 'sku') x`, [...P, E]);
  const T = await one("select array_agg(distinct core.norm_code(a.target_code) order by core.norm_code(a.target_code)) as a from core.ad_spend_daily a where a.company_id = $1 and a.mall = $2 and a.scope_key = $3 and a.date_jst >= $4 and a.date_jst < $5 and a.target_granularity = 'sku'", P);
  const NT = [...new Set([...N, ...T])];
  const L = (await one(`select array_agg(distinct x order by x) as a from (
      select l.listing_id as x from core.listings l where l.company_id = 1 and l.mall = 'amazon' and l.listing_norm = any($1::text[])
      union all select e.entity_id from core.external_ids e where e.company_id = 1 and e.entity_type = 'listing' and e.system = 'amazon' and e.valid_to is null and e.external_norm = any($2::text[])) y`, [NT, T])).map(Number);
  const S = (await one('select array_agg(distinct c.sku_id order by c.sku_id) as a from core.listing_components c where c.listing_id = any($1::bigint[])', [L])).map(Number);
  return { E, NT, T, L, S, Ltext: L.map(String) };
}

const lit = (v) => `'${String(v).replace(/'/g, "''")}'`;
const arrLit = (arr, type) => `array[${arr.map((x) => (type === 'bigint[]' ? String(Number(x)) : lit(x))).join(',')}]::${type}`;
/** 段の PREPARE の型と EXECUTE の引数 (limit = 上限) */
function prepared(st, keys, limit) {
  if (st.fn) return { types: st.types, args: [st.id === '(b)-1' ? '1::smallint' : '30'] };
  const kt = st.keys ? KEY_TYPE[st.keys] : 'text[]';
  return {
    types: ['smallint', 'text', 'text', 'date', 'date', 'integer', kt],
    args: ['1::smallint', lit('amazon'), lit('jp'), `${lit(MONTH.from)}::date`, `${lit(MONTH.to)}::date`, String(limit), st.keys ? arrLit(keys[st.keys], kt) : `null::${kt}`],
  };
}
const planOf = (r) => { const v = r.rows[0]['QUERY PLAN']; return (typeof v === 'string' ? JSON.parse(v) : v)[0].Plan; };

/**
 * 門の GUC の取引の中で、全部の段を PREPARE → EXPLAIN (FORMAT JSON) EXECUTE (→ analyze なら EXPLAIN ANALYZE)。
 * 戻り = 段ごとの { id, errs, shape, actualRows }。before(q) = 取引の中で先に流す (index を外す試験)
 */
async function explainStages(q, keys, { analyze = false, limit = 100, stages = STAGES, before = null } = {}) {
  const out = [];
  await q('begin');
  try {
    for (const g of GATE_GUCS) await q(`set local ${g}`);
    if (before) await before(q);
    for (const [i, st] of stages.entries()) {
      const name = `d60_idx_st_${i}`;
      const { types, args } = prepared(st, keys, limit);
      await q(`prepare ${name}(${types.join(', ')}) as ${st.sql}`);
      try {
        const plan = planOf(await q(`explain (format json) execute ${name}(${args.join(', ')})`));
        const res = { id: st.id, ...checkPlan(plan, st) };
        if (analyze && !st.fn) {
          const ap = planOf(await q(`explain (analyze, format json, timing off, summary off) execute ${name}(${args.join(', ')})`));
          const chk = checkPlan(ap, st);
          res.actualRows = chk.scan ? Number(chk.scan['Actual Rows']) : null;
          res.actualLoops = chk.scan ? Number(chk.scan['Actual Loops']) : null;
        }
        out.push(res);
      } finally { await q(`deallocate ${name}`); }
    }
  } finally { await q('rollback'); }
  return out;
}

// ─── 使い捨てのクラスタ (test-company-db-migrate-lock-pg.mjs と同じ作り) ───
const PINNED_EMBEDDED_PG = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).devDependencies['embedded-postgres'];
async function loadEmbeddedPostgres() {
  const bases = [path.join(ROOT, 'package.json'), ...(process.env.EMBEDDED_PG_DIR ? [path.join(process.env.EMBEDDED_PG_DIR, 'package.json')] : []), 'C:/tmp/pg-embed/package.json'];
  const seen = [];
  for (const b of bases) {
    let main, ver;
    try { const req = createRequire(b); main = req.resolve('embedded-postgres'); let d = path.dirname(main); while (path.basename(d) !== 'embedded-postgres' && path.dirname(d) !== d) d = path.dirname(d); ver = JSON.parse(fs.readFileSync(path.join(d, 'package.json'), 'utf8')).version; } catch { continue; }
    if (ver !== PINNED_EMBEDDED_PG) { seen.push(path.dirname(b) + ' = ' + ver); continue; }
    return { EmbeddedPostgres: (await import(pathToFileURL(main).href)).default, from: path.dirname(b) };
  }
  return { why: seen.length ? '版が ' + PINNED_EMBEDDED_PG + ' でない (' + seen.join(' / ') + ')' : '見つからない' };
}

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const GB = 1024 ** 3;
const BIG_DISK = async () => ({ ok: true, capacityBytes: 100 * GB, usedBytes: 1 * GB });
const EXPECT = JSON.parse(fs.readFileSync(expectPathOf(DEFAULT_DIR, FILE), 'utf8'));
const stripAlias = (s) => s.replace(/\b[a-z]\.(?=[a-z_]+\b)/g, '');

console.log(`対象 = ${FILE.file} (番号 ${VER}・前 ${PREV})`);

// ─── S1 file の中身 (DB なし) ───
await t('S1 file = 付録 B の 6 つ (名前・表・列の並び・式・部分 index の条件・INCLUDE なし)・expect.json も同じ 6 つ (PG 18)・番号を書かない・⑨a/⑨b の式 = 段の本文の式', async () => {
  assert.equal(FILE.concurrentIndex, true, '1 行目が -- migrate:concurrent-index でない');
  const stmts = splitSqlStatements(FILE.text).map((s) => parseConcurrentIndexStatement(s, FILE.file));
  assert.deepEqual(stmts.map((s) => `${s.schema}.${s.name}`), Object.keys(NEW_INDEXES));
  for (const s of stmts) {
    const want = NEW_INDEXES[`${s.schema}.${s.name}`];
    assert.equal(s.kind, 'create'); assert.equal(s.unique, false); assert.equal(`${s.schema}.${s.table}`, want.table);
    assert.deepEqual(s.elements.map((e) => e.column), want.columns, `${s.name} の列`);
    assert.match(s.sql, /^create index concurrently if not exists /);
    assert.doesNotMatch(s.sql, /\binclude\b/i, `${s.name} に INCLUDE (付録 B に無い)`);
    if (want.expr) {
      // 式の index = key は ( <式> )・条件は ( <式> ) is not null (同じ文字)
      const flat = s.sql.replace(/\s+/g, ' ');
      assert.ok(flat.includes(`((${want.expr}))`), `${s.name} の key の式が付録 B と違う`);
      assert.ok(flat.endsWith(`where (${want.expr}) is not null`), `${s.name} の部分 index の条件が (式) is not null でない`);
    }
  }
  // 付録 B の段の本文の式 (別名 e. を外す) = index の式
  for (const id of ['⑨a', '⑨b']) {
    const st = STAGES.find((x) => x.id === id);
    const want = Object.values(NEW_INDEXES).find((x) => x.stage === id).expr;
    assert.ok(stripAlias(st.sql).includes(`(${want}) = any($7::text[])`), `${id} の段の式が index の式と違う`);
  }
  // expect.json
  assert.equal(EXPECT.pg_major, 18);
  assert.deepEqual(Object.keys(EXPECT.indexes).sort(), Object.keys(NEW_INDEXES).sort());
  for (const [key, want] of Object.entries(NEW_INDEXES)) {
    const a = EXPECT.indexes[key];
    assert.equal(a.table, want.table); assert.equal(a.access_method, 'btree'); assert.equal(a.unique, false); assert.equal(a.nulls_not_distinct, false);
    assert.equal(a.key_columns, want.columns.length); assert.equal(a.all_columns, want.columns.length, `${key} に INCLUDE の列がある`);
    assert.deepEqual(a.columns.map((c) => c.column), want.columns);
    assert.ok(a.columns.every((c) => c.key === true && c.option === 0), `${key} の列が昇順の key でない`);
    assert.equal(a.reltablespace, 0); assert.deepEqual(a.reloptions, []);
    if (want.expr) {
      assert.match(a.expressions, /^\s*CASE\s+WHEN /); assert.match(a.predicate, /IS NOT NULL\)$/);
      assert.ok(a.predicate.includes(a.expressions.trim()), `${key} の条件が key の式の is not null でない`);
    } else {
      assert.equal(a.expressions, null); assert.equal(a.predicate, want.predicate);
    }
  }
  // 付け替え = git mv だけ (本文・expect.json に番号を書かない)
  //   (本文の 0057 / 0058 は #1605・#1607 の番号の説明 = 自分の番号ではない)
  assert.doesNotMatch(FILE.text, /\d{4}_d60_load_count_indexes/, '本文に自分の番号つきの名前がある (付け替えで本文を直すことになる)');
  assert.doesNotMatch(fs.readFileSync(expectPathOf(DEFAULT_DIR, FILE), 'utf8'), /\d{4}_/, 'expect.json に番号がある');
  planConcurrentIndexFile(FILE);   // runner の流す前の検査 (許す文・expect.json の形) が通る
});

await t('S2 試験の木の検査そのもの = 合格の木は通り、Bitmap・Seq Scan + Disabled・Filter・別の index・Index Cond の列の欠け・余分な node は落ちる', async () => {
  const st = STAGES.find((x) => x.id === '⑥');
  const scan = (o = {}) => ({ 'Node Type': 'Index Only Scan', 'Index Name': 'ix_sku_costs_company_sku_from', 'Index Cond': '((company_id = 1) AND (sku_id = ANY (...)))', ...o });
  const tree = (s) => ({ 'Node Type': 'Aggregate', Strategy: 'Plain', Plans: [{ 'Node Type': 'Limit', Plans: [s] }] });
  assert.deepEqual(checkPlan(tree(scan()), st).errs, []);
  assert.deepEqual(checkPlan({ 'Node Type': 'Aggregate', Strategy: 'Plain', Plans: [{ 'Node Type': 'Subquery Scan', Plans: [{ 'Node Type': 'Limit', Plans: [scan()] }] }] }, st).errs, []);
  const bad = [
    tree({ 'Node Type': 'Bitmap Heap Scan', Plans: [{ 'Node Type': 'Bitmap Index Scan', 'Index Name': 'ix_sku_costs_company_sku_from' }] }),
    tree({ 'Node Type': 'Seq Scan', Disabled: true, Filter: '(company_id = 1)' }),
    tree(scan({ Filter: '(valid_to IS NULL)' })),
    tree(scan({ 'Index Name': 'ux_sku_costs_active' })),
    tree(scan({ 'Index Cond': '(company_id = 1)' })),
    tree(scan({ 'Index Cond': undefined })),
    { 'Node Type': 'Aggregate', Strategy: 'Plain', Plans: [{ 'Node Type': 'Limit', Plans: [{ 'Node Type': 'Memoize', Plans: [scan()] }] }] },
    { 'Node Type': 'Aggregate', Strategy: 'Hashed', Plans: [{ 'Node Type': 'Limit', Plans: [scan()] }] },
    tree(scan({ Disabled: true })),
  ];
  for (const b of bad) { const s = JSON.parse(JSON.stringify(b)); assert.ok(checkPlan(s, st).errs.length > 0, `落ちない: ${JSON.stringify(b).slice(0, 120)}`); }
  assert.ok(checkPlan({ 'Node Type': 'Limit', Plans: [scan({ 'Node Type': 'Index Scan', 'Index Name': 'ix_order_finance_daily_date' })] }, { ...st, kind: 'rows', index: 'ix_order_finance_daily_date', cond: ['company_id', 'economic_date_jst'] }).errs.length === 1, 'rows の形の Index Cond の欠け');
});

// ─── 本物の PG ───
const loaded = await loadEmbeddedPostgres();
if (!loaded.EmbeddedPostgres) {
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の試験を流せない (飛ばさない)。リポジトリで npm ci');
  process.exit(1);
}
const clusterDir = path.join(os.tmpdir(), `cdb-d60-idx-pg-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
const cluster = new loaded.EmbeddedPostgres({ databaseDir: clusterDir, user: 'postgres', password: SU_PW, port, persistent: false, onLog: () => {}, onError: () => {} });
let cleanupFailed = false;
const stopCluster = async () => {
  try { await cluster.stop(); } catch (e) { console.error('使い捨てのクラスタを止めるときの誤り: ' + e.message); }
  for (let i = 0; i < 10 && fs.existsSync(clusterDir); i++) { try { fs.rmSync(clusterDir, { recursive: true, force: true }); } catch { await new Promise((r) => setTimeout(r, 500)); } }
  if (fs.existsSync(clusterDir)) { cleanupFailed = true; console.error('❌ 使い捨てのクラスタのフォルダが消えない: ' + clusterDir); }
};
const urlOf = (db) => `postgres://postgres:${SU_PW}@127.0.0.1:${port}/${db}`;
console.log('使い捨てのクラスタ: embedded-postgres ' + PINNED_EMBEDDED_PG + ' (' + loaded.from + ')');

const clients = [];
let setupError = null;
try {
  await cluster.initialise();
  await cluster.start();
} catch (e) { setupError = e; }

if (!setupError) {
  const su = await openPgClient(urlOf('postgres')); clients.push(su);
  const newDb = async (name) => { await su.query(`create database ${name}`); const c = await openPgClient(urlOf(name)); c.on('error', () => {}); clients.push(c); return c; };
  const indexState = async (c) => (await c.query(`select n.nspname || '.' || ci.relname as key, i.indisvalid and i.indisready and i.indislive as ok
      from pg_index i join pg_class ci on ci.oid = i.indexrelid join pg_namespace n on n.oid = ci.relnamespace
     where n.nspname || '.' || ci.relname = any($1::text[]) order by 1`, [Object.keys(NEW_INDEXES)])).rows;
  const lockHolders = async (c) => (await c.query(`select pid from pg_locks where locktype = 'advisory' and objid = (hashtextextended($1, 0) & 4294967295)::oid`, [MIGRATE_LOCK_NAME])).rows;

  await t('R1a 一度も ANALYZE していない表 (新しい DB) = 見積もれない = 何も作らず記録もしない (DISK_CHECK_FAILED・fail-closed)', async () => {
    const c = await newDb('d60_fresh');
    await migrateWithLock(pgAdapter(c), { to: PREV, log: quiet, readDiskMetrics: BIG_DISK });
    await assert.rejects(migrateWithLock(pgAdapter(c), { to: VER, log: quiet, readDiskMetrics: BIG_DISK }),
      (e) => e.code === 'DISK_CHECK_FAILED' && /ANALYZE/.test(e.message));
    assert.deepEqual(await indexState(c), []);
    assert.equal((await c.query('select count(*)::int as n from ops.schema_migrations where version = $1', [VER])).rows[0].n, 0);
    assert.deepEqual(await lockHolders(c), []);
  });

  const c = await newDb('d60_legacy');
  await migrateWithLock(pgAdapter(c), { to: PREV, log: quiet, readDiskMetrics: BIG_DISK });
  await c.query(dataSql(1));
  const keys = await buildKeys((s, p) => c.query(s, p));
  console.log(`  (データ: 鍵の数 E ${keys.E.length}・N∪T ${keys.NT.length}・T ${keys.T.length}・L ${keys.L.length}・S ${keys.S.length})`);

  await t('R1b 本番の CLI の容量の読み手は host の対応が未確認の間 HOST_MAPPING_UNCONFIRMED = 何も作らない (Render の回答の前にマージしない理由)', async () => {
    const reader = await renderDiskMetricsReader({ RENDER_API_KEY: 'rnd_d60idxtest', CDB_RENDER_PG_RESOURCE_ID: 'dpg-d60idxtest-a' }, 'postgres://u:p@dpg-d60idxtest-a/cdb', { currentDatabase: 'cdb' });
    const m = await reader();
    assert.equal(m.ok, false); assert.equal(m.detail, 'HOST_MAPPING_UNCONFIRMED', JSON.stringify(m));
    await assert.rejects(migrateWithLock(pgAdapter(c), { to: VER, log: quiet, readDiskMetrics: reader }),
      (e) => e.code === 'DISK_CHECK_FAILED' && /HOST_MAPPING_UNCONFIRMED/.test(e.message));
    assert.deepEqual(await indexState(c), []);
  });

  await t('R1c dry-run = 作らずに pending に出る (許す文と expect.json の検査は通る)', async () => {
    const r = await migrateWithLock(pgAdapter(c), { to: VER, dryRun: true, log: quiet, readDiskMetrics: BIG_DISK });
    assert.deepEqual([r.applied, r.pending], [[], [VER]], JSON.stringify(r));
    assert.deepEqual(await indexState(c), []);
  });

  await t('R1d runner の legacy の道 (取引の外の CIC) = 6 つが valid・ready・live で expect.json と一致して記録 / applied_by = migrate-v2 / lock が残らない / もう一度流すと 0 本', async () => {
    assert.equal((await readOwnerMode(pgAdapter(c))).mode, 'legacy');
    const logs = [];
    const r = await migrateWithLock(pgAdapter(c), { to: VER, log: (m) => logs.push(m), readDiskMetrics: BIG_DISK });
    assert.deepEqual(r.applied, [VER]);
    assert.ok(logs.some((m) => /concurrent-index・取引の外で 6 文/.test(m)), logs.join('\n'));
    assert.equal(logs.filter((m) => /^容量: .+ の予想 /.test(m)).length, 6, '各文の前の容量の判定が 6 回でない');
    const st = await indexState(c);
    assert.deepEqual(st.map((x) => x.key), Object.keys(NEW_INDEXES).sort());
    assert.ok(st.every((x) => x.ok), JSON.stringify(st));
    for (const key of Object.keys(NEW_INDEXES)) {
      const [sc, nm] = key.split('.');
      const cur = await readIndexAttrs(pgAdapter(c), sc, nm);
      assert.deepEqual(attrDiff(cur.attrs, EXPECT.indexes[key]), [], key);
    }
    const rec = (await c.query('select applied_by from ops.schema_migrations where version = $1', [VER])).rows;
    assert.equal(rec.length, 1); assert.match(rec[0].applied_by, / migrate-v2$/);
    assert.deepEqual(await lockHolders(c), []);
    const r2 = await migrateWithLock(pgAdapter(c), { to: VER, log: quiet, readDiskMetrics: BIG_DISK });
    assert.deepEqual(r2.applied, []); assert.ok(r2.skipped.includes(VER));
  });

  let plans = null;
  await t('R2 門の GUC の下で 14 段の EXPLAIN (FORMAT JSON) が許す木だけ (Limit → Index Scan / Index Only Scan・count = Aggregate (Plain) → Limit → scan・Index Name・Filter なし・Index Cond・Disabled なし)', async () => {
    plans = await explainStages((s, p) => c.query(s, p), keys, { analyze: true, limit: 100 });
    for (const p of plans) console.log(`      ${p.id.padEnd(5)} ${p.shape}${p.actualRows != null ? `  (scan の実際の行 ${p.actualRows})` : ''}`);
    const bad = plans.filter((p) => p.errs.length);
    assert.deepEqual(bad.map((p) => `${p.id}: ${p.errs.join(' / ')}`), []);
    assert.equal(plans.length, 14);
  });

  await t('R3 上限 100 のとき ①〜⑨b の 12 段の scan が 101 行で止まる (EXPLAIN ANALYZE の Actual Rows・データは各段 101 行より多い)', async () => {
    assert.ok(plans, 'R2 が流れていない');
    const rows = plans.filter((p) => !STAGES.find((s) => s.id === p.id).fn);
    assert.equal(rows.length, 12);
    assert.deepEqual(rows.filter((p) => p.actualRows !== 101 || p.actualLoops !== 1).map((p) => `${p.id}: ${p.actualRows} 行 × ${p.actualLoops}`), []);
  });

  await t('R4 新しい index を 1 つずつ外すと (取引の中で drop → rollback) その段の木が不合格 = 6 つとも段の上限に要る', async () => {
    for (const [key, want] of Object.entries(NEW_INDEXES)) {
      const ids = want.stage.split('・');
      const stages = STAGES.filter((s) => ids.includes(s.id));
      assert.equal(stages.length, ids.length, key);
      const res = await explainStages((s, p) => c.query(s, p), keys, { stages, before: async (q) => q(`drop index ${key}`) });
      for (const r of res) console.log(`      ${key} を外す → ${r.id}: ${r.shape} = ${r.errs.join(' / ')}`);
      for (const r of res) assert.ok(r.errs.length > 0, `${key} を外しても ${r.id} が合格した (${r.shape})`);
    }
    assert.ok((await indexState(c)).every((x) => x.ok), 'rollback の後に index が戻っていない');
    assert.equal((await indexState(c)).length, 6);
  });

  await t('I1 (PR 3a への事実) 付録 B の段の文を pg の driver に $1〜$7 の 7 つの値でそのまま渡すと、文が使わない引数の型が決まらず 42P18 = 型を明示する (PREPARE) か使う引数だけにする', async () => {
    const st = STAGES.find((x) => x.id === '⑤');   // $6・$7 だけを使う
    await assert.rejects(c.query(st.sql, [1, 'amazon', 'jp', MONTH.from, MONTH.to, 100, keys.L]), (e) => e.code === '42P18');
    const all = STAGES.find((x) => x.id === '①');  // $1〜$6 を全部使う = そのまま通る
    assert.equal((await c.query(all.sql, [1, 'amazon', 'jp', MONTH.from, MONTH.to, 100])).rows.length, 101);
  });
}

// ─── PGlite の道 ───
await t('P1 PGlite の道 (applyMigrations・concurrently を外して取引の中) = 同じ file が通り、属性が同じ expect.json に一致・14 段の木も同じ形', async () => {
  const pg = new PGlite();
  try {
    const db = pgliteAdapter(pg);
    const r = await applyMigrations(db, { to: VER, log: quiet });
    assert.ok(r.applied.includes(VER), JSON.stringify(r.applied.slice(-3)));
    for (const key of Object.keys(NEW_INDEXES)) {
      const [sc, nm] = key.split('.');
      const cur = await readIndexAttrs(db, sc, nm);
      assert.ok(cur && cur.valid && cur.ready && cur.live, key);
      assert.deepEqual(attrDiff(cur.attrs, EXPECT.indexes[key]), [], key);
    }
    await pg.exec(dataSql(0.2));
    const q = (s, p) => pg.query(s, p);
    const keys = await buildKeys(q);
    const res = await explainStages(q, keys, { limit: 20 });
    assert.deepEqual(res.filter((p) => p.errs.length).map((p) => `${p.id}: ${p.errs.join(' / ')} (${p.shape})`), []);
  } finally { await pg.close(); }
});

for (const c of clients) { try { await c.end(); } catch { /* */ } }
await stopCluster();
if (setupError) { console.error('❌ 使い捨てのクラスタを起動できない: ' + (setupError.stack || setupError.message)); process.exit(1); }
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng || cleanupFailed ? 1 : 0);
