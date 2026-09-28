#!/usr/bin/env node
/**
 * test-company-db-sku-activity.mjs — SKU ごとの動き (0042: mart.sku_activity / sku_activity_gaps / sales_expanded_to_skus / listings_to_skus) の試験。PGlite。
 *   セットは構成品に展開して数量を数える・売上と広告費は 1 つの SKU だけの品物にだけ付ける (まとめ売りは付ける・複数 SKU のセットは付けない)・
 *   割り振れなかった分は gaps に出る (合計が材料と一致)・在庫と何日もつか
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const pg = new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: () => {} });
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
const num = (x) => (x == null ? null : Number(x));

const sku = async (code, kind = 'single') => {
  const p = (await one(`insert into core.products (company_id, name) values (1, $1) returning product_id`, [`商品 ${code}`])).product_id;
  return Number((await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, $2, $3, $3) returning sku_id`, [p, kind, code])).sku_id);
};
const A = await sku('A'), B = await sku('B'), S = await sku('SET-AB', 'set');
await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'ne'), (1, $1, $3, 2, 'ne')`, [S, A, B]);   // NE のセット = A×1 + B×2
const listing = async (mall, code, comps) => {
  const id = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, $1, '', $2, 'active') returning listing_id`, [mall, code])).listing_id);
  for (const [s, q] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, $3, 'imported', 'system')`, [id, s, q]);
  return id;
};
const LA = await listing('amazon', 'la', [[A, 1]]), LB2 = await listing('amazon', 'lb2', [[B, 2]]), LAB = await listing('amazon', 'lab', [[A, 1], [B, 1]]), LNONE = await listing('amazon', 'lnone', []);
const [D1, D2] = ['2026-03-01', '2026-03-02'];
let seq = 0;
async function salesDay(day, rows) {
  const run = `sd_${++seq}`;
  await pg.query(`insert into mart.sales_daily_runs (run_id, company_id, mall, scope_key, session_id, started_at, finished_at, n_dates, n_rows, n_orders) values ($1, 1, 'amazon', 'jp', 's', now(), now(), 1, 0, 0)`, [run]);
  for (const r of rows) {
    await pg.query(`insert into mart.sales_daily (run_id, company_id, date_jst, mall, scope_key, shop_code, listing_id, sku_id, orders, orders_cancelled, lines, units_ordered, units_cancelled, items_amount_jpy, cancelled_items_amount_jpy, sales_jpy, customer_paid_jpy)
      values ($1, 1, $2::date, $3, 'jp', null, $4, $5, 1, 0, 1, $6, $7, $8, 0, $8, $8)`, [run, day, r.mall || 'amazon', r.lid ?? null, r.sku ?? null, r.units, r.cxl || 0, r.sales]);
  }
  await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) values (1, 'amazon', 'jp', $1::date, $2)`, [day, run]);
}
// D1: LA 3 個 3,000 円 / LB2 (B×2 のまとめ売り) 1 個 2,000 円 / LAB (A+B のセット出品) 1 個 1,500 円 (1 個取消で正味 1) / NE のセット SKU を直接 1 個 800 円 / 出品に当たらない 1 個 100 円 / 構成の無い出品 1 個 50 円
await salesDay(D1, [{ lid: LA, units: 3, sales: 3000 }, { lid: LB2, units: 1, sales: 2000 }, { lid: LAB, units: 2, cxl: 1, sales: 1500 }, { sku: S, units: 1, sales: 800 }, { units: 1, sales: 100 }, { lid: LNONE, units: 1, sales: 50 }]);
// D2: LA 1 個 1,000 円
await salesDay(D2, [{ lid: LA, units: 1, sales: 1000 }]);
// 広告費 (Amazon): LA 100 円・LAB 50 円・出品の分からない SKU 7 円
await pg.query(`insert into core.ad_spend_days (company_id, mall, scope_key, ad_type, date_jst, source_generation, source_report_id, checksum, row_count, cost_total, ingest_run_id) values (1, 'amazon', 'jp', 'SP', $1::date, 5, 'R', $2, 3, 157, 'r')`, [D1, 'a'.repeat(64)]);
await pg.query(`insert into core.ad_spend_daily (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code, listing_id, clicks, impressions, ad_cost, ad_sales_1d, units_1d, ingest_run_id)
  values (1, 'amazon', 'jp', 'SP', $1::date, '1', 'sku', 'la', $2, 1, 1, 100, 900, 1, 'r'), (1, 'amazon', 'jp', 'SP', $1::date, '1', 'sku', 'lab', $3, 1, 1, 50, 0, 0, 'r'), (1, 'amazon', 'jp', 'SP', $1::date, '1', 'sku', 'x', null, 1, 1, 7, 0, 0, 'r')`, [D1, LA, LAB]);
// 在庫 (ロジザードの完走した日): A 20 個
await pg.query(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, host, started_at, finished_at, status, complete, rows_seen, source_tz) values ('st', 'logizard', 'inventory', 'main', 'test', now(), now(), 'success', true, 1, 'UTC')`);
await pg.query(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, built_at) values ($1::date, 'logizard', 'main', 1, 'building', 'st', now())`, [D2]);
await pg.query(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, captured_at, ingest_run_id) values ($1::date, 'logizard', 'main', 'A', 1, $2, 20, now(), 'st')`, [D2, A]);
await pg.query(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1::date and source = 'logizard'`, [D2]);

const act = async (from = D1, to = D2) => all(`select * from mart.sku_activity(1::smallint, $1::date, $2::date) order by sku_id`, [from, to]);
const gaps = async (from = D1, to = D2) => one(`select * from mart.sku_activity_gaps(1::smallint, $1::date, $2::date)`, [from, to]);

await t('数量: セットは構成品に展開 (出品の構成・NE のセット・取消を引く)・モール別 / セット経由の数量', async () => {
  const r = await act();
  const a = r.find((x) => Number(x.sku_id) === A), b = r.find((x) => Number(x.sku_id) === B);
  // A = LA 4 + LAB 1 + セット 1 = 6 (セット経由 2) / B = LB2 2 + LAB 1 + セット 2 = 5 (セット経由 3)
  assert.deepEqual([num(a.units_net), num(a.units_via_sets), a.units_by_mall], [6, 2, { amazon: 6 }]);
  assert.deepEqual([num(b.units_net), num(b.units_via_sets)], [5, 3]);
  assert.equal(r.some((x) => Number(x.sku_id) === S), false, 'セット SKU 自体は数えない (在庫は構成品側)');
});
await t('🚨 売上と広告費は 1 つの SKU だけの品物にだけ付ける (まとめ売り B×2 は付ける・A+B のセットは付けない)', async () => {
  const r = await act();
  const a = r.find((x) => Number(x.sku_id) === A), b = r.find((x) => Number(x.sku_id) === B);
  assert.deepEqual([num(a.sales_jpy), a.sales_by_mall, num(a.amazon_sales_jpy), num(a.amazon_ad_cost), num(a.amazon_ad_sales_1d)], [4000, { amazon: 4000 }, 4000, 100, 900]);
  assert.deepEqual([num(b.sales_jpy), num(b.amazon_ad_cost), b.amazon_ad_sales_1d], [2000, 0, null]);
});
await t('在庫と何日もつか: 在庫 20 ÷ (6 個 ÷ 2 日) = 6.7 日 / 在庫の無い SKU は 0 日', async () => {
  const r = await act();
  const a = r.find((x) => Number(x.sku_id) === A), b = r.find((x) => Number(x.sku_id) === B);
  assert.deepEqual([num(a.stock_qty), num(a.daily_units), num(a.cover_days)], [20, 3, 6.7]);
  assert.deepEqual([num(b.stock_qty), num(b.cover_days)], [0, 0]);
});
await t('🚨 割り振れなかった分 (gaps): 展開できない販売・セットの売上・広告費 (セット / 出品が分からない) と、合計が材料と一致', async () => {
  const g = await gaps();
  assert.deepEqual([num(g.units_total), num(g.units_unexpanded)], [3 + 1 + 1 + 1 + 1 + 1 + 1, 2]);
  assert.deepEqual([num(g.sales_total), num(g.sales_attributed), num(g.sales_on_sets), num(g.sales_unexpanded)], [8450, 6000, 2300, 150]);
  assert.equal(num(g.sales_attributed) + num(g.sales_on_sets) + num(g.sales_unexpanded), num(g.sales_total));
  assert.deepEqual([num(g.ad_total), num(g.ad_attributed), num(g.ad_on_sets), num(g.ad_unlinked)], [157, 100, 50, 7]);
  const src = await one(`select sum(sales_jpy)::int s from mart.v_sales_daily where date_jst between $1::date and $2::date`, [D1, D2]);
  assert.equal(num(g.sales_total), src.s);
});
await t('期間の外は読まない (D2 だけ = LA 1 個・広告費なし)', async () => {
  const r = await act(D2, D2);
  assert.deepEqual(r.map((x) => [Number(x.sku_id), num(x.units_net), num(x.sales_jpy), num(x.amazon_ad_cost), num(x.cover_days)]), [[A, 1, 1000, 0, 20]]);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
