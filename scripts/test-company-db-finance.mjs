#!/usr/bin/env node
/**
 * test-company-db-finance.mjs — 0012 (Amazon 財務の受け皿) の受入試験 (Company DB 構想 08 §4.4。F2 の DDL 部分)
 *
 * PGlite で 0001〜0012 を流し、DDL の「歯止め」を実際の操作で確かめる:
 *   policy の期間は重ならない (同じ 会社 × モール × scope) / 版の境目 (11/1) は隣接なら可 / scope が違えば可 / update でも検査 / period_to > from /
 *   集約は JPY だけ・net は各列の合計・source_lines > 0・会社違いの SKU / 出品は付かない / 再構築 (upsert) で置き換わる /
 *   注文の累計 view は policy が指す source の行だけ (旧と V2 の両方が入っていても二重にならない)・'-' の費用行は入らない /
 *   finance_daily は listing / sku / seller_sku のどれが null でも主キーが組める・JPY だけ
 * 🚨 policy の重複検査の 2 接続の並行 (advisory lock) は PGlite では書けない → 本番で手で確かめる (08 §7.6)
 * 実行: node scripts/test-company-db-finance.mjs
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
assert.ok(applied.applied.includes('0012'), '0012 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const num = async (sql, p = []) => Number((await one(sql, p)).n);
const co = (await one(`select company_id from core.companies order by company_id limit 1`)).company_id;
await pg.query(`insert into core.products (company_id, name) values ($1, '見本A')`, [co]);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'sku-a', name from core.products`);
const sku = (await one(`select sku_id from core.skus where code = 'sku-a'`)).sku_id;
const other = (await one(`select max(company_id)::smallint + 1 as c from core.companies`)).c;
await pg.query(`insert into core.companies (company_id, name, kind) values ($1, 'other', 'subsidiary')`, [other]);
await pg.query(`insert into core.products (company_id, name) values ($1, '他社品')`, [other]);
const skuOther = (await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) select company_id, product_id, 'single', 'other-1', name from core.products where company_id = $1 returning sku_id`, [other])).sku_id;

const policy = (mall, scope, from, to, source) => pg.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, period_to, source) values ($1, $2, $3, $4, $5, $6)`, [co, mall, scope, from, to, source]);
const FIN_COLS = 'company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source, sku_id, qty_net, principal_jpy, refund_jpy, commission_jpy, net_jpy, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash';
const fin = ({ order = '503-1', date, sku: sellerSku = 'SKU-A', source, principal = 0, refund = 0, commission = 0, skuId = null, qty = 0, seq = 1, hash = 'h', scope = 'jp', company = co }) =>
  pg.query(`insert into core.order_finance_daily (${FIN_COLS}) values ($1, 'amazon', $2, $3, $4, $5, $6, $7, $8, $9::bigint, $10::bigint, $11::bigint, ($9::bigint + $10::bigint + $11::bigint), 1, $12, now(), 'v1', $13)`,
    [company, scope, order, date, sellerSku, source, skuId, qty, principal, refund, commission, seq, hash]);

console.log('0012: 表と view');
await t('表・view・trigger がある。既存の表は増減しない (0011 まで + 0012 の 3 表)', async () => {
  const tables = (await pg.query(`select table_schema || '.' || table_name as t from information_schema.tables where table_schema in ('core','mart') and table_type = 'BASE TABLE' and table_name in ('finance_source_policy','order_finance_daily','finance_daily') order by 1`)).rows.map((r) => r.t);
  assert.deepEqual(tables, ['core.finance_source_policy', 'core.order_finance_daily', 'mart.finance_daily']);
  assert.equal(await num(`select count(*) as n from information_schema.views where table_schema = 'mart' and table_name = 'v_order_finance_summary'`), 1);
  assert.equal(await num(`select count(*) as n from pg_trigger where tgname = 'trg_finance_source_policy_overlap'`), 1);
});

console.log('policy (採用する取得元)');
await t('🚨 期間 [from, to) は同じ 会社 × モール × scope で重ならない。版の境目 (10/31 まで v1 → 11/1 から V2) は隣接なので可、scope が違えば可', async () => {
  await policy('amazon', 'jp', '2025-01-01', '2026-11-01', 'amazon_settlement_flat_v1');
  await policy('amazon', 'jp', '2026-11-01', null, 'amazon_settlement_flat_v2');
  await rejects(() => policy('amazon', 'jp', '2026-10-01', '2026-12-01', 'amazon_finances_api'), /overlaps/);
  await rejects(() => policy('amazon', 'jp', '2024-01-01', '2025-01-02', 'amazon_finances_api'), /overlaps/);   // 端が 1 日でも重なる
  await rejects(() => policy('amazon', 'jp', '2027-01-01', null, 'amazon_finances_api'), /overlaps/);           // 開いた期間 (to = null) とも重なる
  await policy('amazon', 'us', '2026-10-01', null, 'amazon_finances_api');
  await policy('rakuten', 'jp', '2025-01-01', null, 'mall_finance_daily_v1');
  assert.equal(await num(`select count(*) as n from core.finance_source_policy`), 4);
});
await t('update でも重複を検査する (V2 の開始を 10/1 に前倒しすると v1 と重なる → 拒否。v1 の終わりも 10/1 にすれば通る)。period_to > from。source は決まった値だけ', async () => {
  await rejects(() => pg.query(`update core.finance_source_policy set period_from = date '2026-10-01' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`), /overlaps/);
  await pg.query(`update core.finance_source_policy set period_to = date '2026-10-01' where source = 'amazon_settlement_flat_v1' and scope_key = 'jp'`);
  await pg.query(`update core.finance_source_policy set period_from = date '2026-10-01' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`);
  // 元に戻す (以降の試験は 11/1 が境目)
  await pg.query(`update core.finance_source_policy set period_from = date '2026-11-01' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`);
  await pg.query(`update core.finance_source_policy set period_to = date '2026-11-01' where source = 'amazon_settlement_flat_v1' and scope_key = 'jp'`);
  await rejects(() => policy('amazon', 'jp2', '2026-01-01', '2026-01-01', 'amazon_settlement_flat_v1'), /ck_finance_source_policy_period/);
  await rejects(() => policy('amazon', 'jp2', '2026-01-01', null, 'settlement'), /finance_source_policy_source_check|violates check/);
});

console.log('order_finance_daily (注文 × 計上日 × SKU × 取得元)');
await t('JPY だけ。net は各列の合計 (CHECK)。source_lines > 0。source は policy と同じ値の集合', async () => {
  await rejects(() => pg.query(`insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source, currency, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash) values ($1, 'amazon', 'us', '111-1', current_date, '-', 'amazon_finances_api', 'USD', 1, 1, now(), 'v1', 'h')`, [co]), /currency/);
  await rejects(() => pg.query(`insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source, principal_jpy, net_jpy, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash) values ($1, 'amazon', 'jp', '111-2', current_date, 'X', 'amazon_settlement_flat_v1', 1000, 999, 1, 1, now(), 'v1', 'h')`, [co]), /ck_order_finance_daily_net/);
  await rejects(() => pg.query(`insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash) values ($1, 'amazon', 'jp', '111-3', current_date, 'X', 'amazon_settlement_flat_v1', 0, 1, now(), 'v1', 'h')`, [co]), /ck_order_finance_daily_lines/);
  await rejects(() => pg.query(`insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash) values ($1, 'amazon', 'jp', '111-4', current_date, 'X', 'settlement', 1, 1, now(), 'v1', 'h')`, [co]), /ck_order_finance_daily_source/);
});
await t('会社違いの SKU / 出品は付かない (複合 FK)。同じ会社の SKU は付く', async () => {
  await rejects(() => fin({ order: '900-1', date: '2026-10-01', source: 'amazon_settlement_flat_v1', principal: 100, skuId: skuOther }), /foreign key|violates/i);
  await fin({ order: '900-1', date: '2026-10-01', source: 'amazon_settlement_flat_v1', principal: 100, skuId: sku, qty: 1 });
  assert.equal((await one(`select sku_id from core.order_finance_daily where mall_order_no = '900-1'`)).sku_id, sku);
});
await t('再構築で置き換わる (同じ主キーへの upsert。received_batch_seq は進む)', async () => {
  await pg.query(`insert into core.order_finance_daily (${FIN_COLS}) values ($1, 'amazon', 'jp', '900-1', date '2026-10-01', 'SKU-A', 'amazon_settlement_flat_v1', $2, 2, 250, 0, -30, 220, 2, 2, now(), 'v1', 'h2')
                  on conflict (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, source) do update set qty_net = excluded.qty_net, principal_jpy = excluded.principal_jpy, commission_jpy = excluded.commission_jpy, net_jpy = excluded.net_jpy, source_lines = excluded.source_lines, received_batch_seq = excluded.received_batch_seq, content_hash = excluded.content_hash, built_at = now()`, [co, sku]);
  const r = await one(`select qty_net, principal_jpy, commission_jpy, net_jpy, received_batch_seq, content_hash from core.order_finance_daily where mall_order_no = '900-1'`);
  assert.equal(Number(r.principal_jpy), 250); assert.equal(Number(r.commission_jpy), -30); assert.equal(Number(r.net_jpy), 220); assert.equal(Number(r.received_batch_seq), 2); assert.equal(r.content_hash, 'h2');
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '900-1'`), 1);
});

console.log('mart.v_order_finance_summary (注文の累計)');
await t('🚨 policy が指す source の行だけを足す: 10/31 は v1、11/2 は V2 (v1 の 11/2 の行は入らない = 旧と V2 が両方あっても二重にならない)。first / last は採用した行の範囲', async () => {
  await fin({ order: '503-1', date: '2026-10-31', source: 'amazon_settlement_flat_v1', principal: 1000, qty: 1 });
  await fin({ order: '503-1', date: '2026-11-02', source: 'amazon_settlement_flat_v2', refund: -200 });
  await fin({ order: '503-1', date: '2026-11-02', source: 'amazon_settlement_flat_v1', refund: -999 });   // 11 月は V2 が採用 → 累計に入らない (R3 #6 の筋書き)
  await fin({ order: '503-1', date: '2026-11-03', source: 'amazon_finances_api', refund: -500 });         // jp では採用されていない source → 入らない
  const s = await one(`select qty_net, principal_jpy, refund_jpy, net_jpy, first_economic_date_jst::text f, last_economic_date_jst::text l from mart.v_order_finance_summary where mall_order_no = '503-1'`);
  assert.equal(Number(s.qty_net), 1); assert.equal(Number(s.principal_jpy), 1000); assert.equal(Number(s.refund_jpy), -200); assert.equal(Number(s.net_jpy), 800); assert.equal(s.f, '2026-10-31'); assert.equal(s.l, '2026-11-02');
});
await t('policy を動かすと累計も変わる (V2 の開始を 11/3 にすると 11/2 は v1 の -999 が採用される)。境目を動かす順は「縮める → 伸ばす」(逆だと重複で拒まれる)。元に戻す', async () => {
  await rejects(() => pg.query(`update core.finance_source_policy set period_to = date '2026-11-03' where source = 'amazon_settlement_flat_v1' and scope_key = 'jp'`), /overlaps/);   // 先に伸ばすと重なる
  await pg.query(`update core.finance_source_policy set period_from = date '2026-11-03' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`);
  await pg.query(`update core.finance_source_policy set period_to = date '2026-11-03' where source = 'amazon_settlement_flat_v1' and scope_key = 'jp'`);
  assert.equal(Number((await one(`select refund_jpy from mart.v_order_finance_summary where mall_order_no = '503-1'`)).refund_jpy), -999);
  await pg.query(`update core.finance_source_policy set period_to = date '2026-11-01' where source = 'amazon_settlement_flat_v1' and scope_key = 'jp'`);
  await pg.query(`update core.finance_source_policy set period_from = date '2026-11-01' where source = 'amazon_settlement_flat_v2' and scope_key = 'jp'`);
  assert.equal(Number((await one(`select refund_jpy from mart.v_order_finance_summary where mall_order_no = '503-1'`)).refund_jpy), -200);
  // 累計 view の円の列は bigint (03 §10)
  const types = (await pg.query(`select column_name, data_type from information_schema.columns where table_schema = 'mart' and table_name = 'v_order_finance_summary' and column_name like '%_jpy'`)).rows;
  assert.ok(types.length >= 9 && types.every((c) => c.data_type === 'bigint'), JSON.stringify(types));
});
await t("注文に紐付かない費用 (mall_order_no = '-') は累計 view に出ない。policy の無い scope の行も出ない", async () => {
  await fin({ order: '-', date: '2026-10-15', sku: '-', source: 'amazon_settlement_flat_v1', commission: -5000 });
  await fin({ order: '777-1', date: '2026-10-15', source: 'amazon_settlement_flat_v1', principal: 10, scope: 'nopolicy' });
  assert.equal(await num(`select count(*) as n from mart.v_order_finance_summary where mall_order_no in ('-', '777-1')`), 0);
  assert.equal(await num(`select count(*) as n from core.order_finance_daily where mall_order_no = '-'`), 1);
});

console.log('mart.finance_daily (日次集計)');
await t('listing / sku / seller_sku のどれが null でも主キーが組める (grain_key)。同じ run に同じ粒度は 2 行入らない。JPY だけ。会社違いの SKU は付かない', async () => {
  await pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1')`, [co]);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1')`, [co]), /duplicate key/);
  await pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, sku_id, seller_sku) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', $2, 'SKU-A')`, [co, sku]);
  await pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, seller_sku) values ('r1', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', 'SKU-A')`, [co]);
  const g = (await pg.query(`select grain_key from mart.finance_daily where run_id = 'r1' order by grain_key`)).rows.map((r) => r.grain_key);
  assert.deepEqual(g, ['-|-|-', '-|-|SKU-A', `-|${sku}|SKU-A`]);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, currency) values ('r2', $1, current_date, 'amazon', 'us', 'amazon_finances_api', 'USD')`, [co]), /currency/);
  await rejects(() => pg.query(`insert into mart.finance_daily (run_id, company_id, economic_date_jst, mall, scope_key, source, sku_id) values ('r3', $1, current_date, 'amazon', 'jp', 'amazon_settlement_flat_v1', $2)`, [co, skuOther]), /foreign key|violates/i);
});

await pg.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
