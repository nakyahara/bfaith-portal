#!/usr/bin/env node
/**
 * test-company-db-ad-efficiency.mjs — 出品ごとの広告の効き目 (0038: mart.ad_efficiency / mart.ad_efficiency_coverage) の試験。PGlite。
 *   広告費 (core.ad_spend_daily) と売上日次 (mart.v_sales_daily) が同じ出品に集まる・出品に当たらない行を捨てない・比率は分母が 0 / 不明 / 一部だけ分かるなら null・
 *   日ごと・材料がそろっているか (公開の後に注文が動いた日も)
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

const listing = async (code, title) => Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status) values (1, 'amazon', '', $1, $2, 'active') returning listing_id`, [code, title])).listing_id);
const A = await listing('SKU-A', '商品 A'), B = await listing('SKU-B', '商品 B'), C = await listing('SKU-C', '商品 C'), E = await listing('SKU-E', '商品 E'), F = await listing('SKU-F', '商品 F');
const [D1, D2, D3] = ['2026-03-01', '2026-03-02', '2026-03-03'];
const skuOf = async (code) => { const p = (await one(`insert into core.products (company_id, name) values (1, $1) returning product_id`, [code])).product_id; return Number((await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', $2, $2) returning sku_id`, [p, code])).sku_id); };
const S1 = await skuOf('f-red'), S2 = await skuOf('f-blue');

async function adDay(day, rows, { legacy = false } = {}) {
  await pg.query(`insert into core.ad_spend_days (company_id, mall, scope_key, ad_type, date_jst, source_generation, source_report_id, checksum, row_count, cost_total, ingest_run_id)
    values (1, 'amazon', 'jp', 'SP', $1::date, $2, $3, $4, $5, 0, 'ads_t')`, [day, legacy ? 1 : 5000, legacy ? 'legacy:upsert-v1' : 'R1', 'a'.repeat(64), rows.length]);
  for (const r of rows) {
    await pg.query(`insert into core.ad_spend_daily (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code, listing_id, clicks, impressions, ad_cost, ad_sales_1d, units_1d, ingest_run_id)
      values (1, 'amazon', 'jp', 'SP', $1::date, $2, $3, $4, $5, $6, $7, $8::numeric, $9::numeric, $10, 'ads_t')`,
      [day, r.c || '1', r.g || 'sku', r.code, r.g === 'asin' || r.g === 'none' ? null : r.lid ?? null, r.clicks || 0, r.imp || 0, r.cost, r.s1, r.u1 ?? null]);
  }
}
let seq = 0;
async function salesDay(day, rows) {
  const run = `sd_${++seq}`;
  await pg.query(`insert into mart.sales_daily_runs (run_id, company_id, mall, scope_key, session_id, started_at, finished_at, n_dates, n_rows, n_orders) values ($1, 1, 'amazon', 'jp', 's', now(), now(), 1, 0, 0)`, [run]);
  for (const r of rows) {
    await pg.query(`insert into mart.sales_daily (run_id, company_id, date_jst, mall, scope_key, shop_code, listing_id, sku_id, orders, orders_cancelled, lines, units_ordered, units_cancelled, items_amount_jpy, cancelled_items_amount_jpy, sales_jpy, customer_paid_jpy, lines_amount_unknown, lines_unresolved)
      values ($1, 1, $2::date, 'amazon', 'jp', $3, $4, $5, $6, 0, $6, $7, $8, $9, 0, $9, $9, $10, $11)`,
      [run, day, r.shop ?? null, r.lid ?? null, r.sku ?? null, r.orders || 1, r.units || 1, r.cancelled || 0, r.sales, r.unknown || 0, r.lid == null ? 1 : 0]);
  }
  await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) values (1, 'amazon', 'jp', $1::date, $2)`, [day, run]);
}
async function order(day, no) {
  await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
    values (1, 'amazon', 'jp', $1, 'mall_api', $2::timestamptz, $2::date, 'new', 1, $2::timestamptz, 'v1', 'h')`, [no, day]);
}

// D1: A (広告 100・経由 1000)・B (広告 50・経由は分からない)・E (広告 30・経由 100)・F (広告 10・経由 20)・ASIN の行 10・出品の分からない SKU 5 /
//     売上 A 2000 (自社 1500 + FBA 500)・C 500 (金額の分からない明細 1)・E 400・F 200 (同じ注文が SKU で 2 粒度に分かれる = 延べ 2)・出品の分からない明細 300
await adDay(D1, [{ code: 'sku-a', lid: A, cost: '100', s1: '1000', u1: 2, clicks: 10, imp: 100 }, { code: 'sku-b', lid: B, cost: '50', s1: null }, { code: 'sku-e', lid: E, cost: '30', s1: '100' },
  { code: 'sku-f', lid: F, cost: '10', s1: '20' }, { g: 'asin', code: 'b0x', cost: '10', s1: '0' }, { code: 'sku-x', lid: null, cost: '5', s1: '0' }], { legacy: true });
await salesDay(D1, [{ lid: A, sales: 1500, units: 2, shop: '4' }, { lid: A, sales: 500, units: 1 }, { lid: C, sales: 500, unknown: 1 }, { lid: E, sales: 400 },
  { lid: F, sales: 100, sku: S1 }, { lid: F, sales: 100, sku: S2 }, { lid: null, sales: 300 }]);
// D2: A (広告 60・経由 0) / E (広告 20・経由は分からない = 期間まとめでは 一部だけ分かる) / 売上 A 1000 (1 個取消)
await adDay(D2, [{ code: 'sku-a', lid: A, cost: '60', s1: '0', u1: 0 }, { code: 'sku-e', lid: E, cost: '20', s1: null }]);
await salesDay(D2, [{ lid: A, sales: 1000, units: 3, cancelled: 1 }]);
// D3: 注文はあるが売上日次は未公開・広告費の日が無い
for (const d of [D1, D2, D3]) await order(d, `o-${d}`);

const eff = (from, to, byDay = false) => all(`select * from mart.ad_efficiency(1::smallint, 'amazon', 'jp', $1::date, $2::date, $3) order by date_jst nulls first, listing_id nulls last, unresolved_key`, [from, to, byDay]);
const num = (x) => (x == null ? null : Number(x));
const row = (rows, lid) => rows.find((r) => Number(r.listing_id) === lid);

await t('期間まとめ: 広告費と売上日次が同じ出品に集まる (自社発送 + FBA をまとめる)・TACoS / ACoS / 広告経由の割合', async () => {
  const a = row(await eff(D1, D2), A);
  assert.deepEqual([a.listing_code, a.title, num(a.ad_cost), num(a.clicks), num(a.ad_sales_1d), num(a.ad_units_1d), num(a.sales_jpy), num(a.units_net), num(a.order_grains), num(a.tacos), num(a.acos_1d), num(a.ad_sales_share), a.date_jst],
    ['SKU-A', '商品 A', 160, 10, 1000, 2, 3000, 5, 3, 0.0533, 0.16, 0.3333, null]);
});
await t('🚨 比率は分母が 0 か分からないとき null (0 で割らない・0 と読ませない) / 広告の無い出品は広告費 0・TACoS 0', async () => {
  const rows = await eff(D1, D2);
  const b = row(rows, B);
  assert.deepEqual([num(b.ad_cost), b.ad_sales_1d, num(b.ad_unknown_rows), num(b.sales_jpy), b.tacos, b.acos_1d, b.ad_sales_share], [50, null, 1, 0, null, null, null]);
});
await t('🚨 一部だけ分かっている和で比率を作らない (#1492 Codex R1): 広告経由の売上が 1 日分からない → ACoS・広告経由の割合は null (TACoS は出す) / 売上に金額の分からない明細 → TACoS・広告経由の割合は null', async () => {
  const rows = await eff(D1, D2);
  const e = row(rows, E), c = row(rows, C);
  assert.deepEqual([num(e.ad_cost), num(e.ad_sales_1d), num(e.ad_unknown_rows), num(e.sales_jpy), num(e.tacos), e.acos_1d, e.ad_sales_share], [50, 100, 1, 400, 0.125, null, null]);
  assert.deepEqual([num(c.sales_jpy), num(c.sales_amount_unknown_lines), c.tacos, c.ad_sales_share], [500, 1, null, null]);
  // 日ごとなら D1 の E は分かっている = 比率を出す
  const e1 = (await eff(D1, D1, true)).find((r) => Number(r.listing_id) === E);
  assert.deepEqual([num(e1.acos_1d), num(e1.ad_sales_share)], [0.3, 0.25]);
});
await t('order_grains は粒度ごとの注文数の延べ (同じ注文が SKU で分かれると 2)', async () => {
  const f = row(await eff(D1, D1), F);
  assert.deepEqual([num(f.order_grains), num(f.sales_jpy), num(f.acos_1d)], [2, 200, 0.5]);
});
await t('🚨 出品に当たらない行を捨てない: 広告の ASIN の行・出品の分からない SKU の行・売上の出品の分からない明細は unresolved_key でまとめる (合計が材料と一致)', async () => {
  const rows = await eff(D1, D2);
  const u = rows.filter((r) => r.listing_id == null).map((r) => [r.unresolved_key, num(r.ad_cost), num(r.sales_jpy), num(r.sales_unresolved_lines)]);
  assert.deepEqual(u, [['ad:asin:b0x', 10, 0, 0], ['ad:sku:sku-x', 5, 0, 0], ['sales:unresolved', 0, 300, 1]]);
  const sum = (k) => rows.reduce((s, r) => s + num(r[k]), 0);
  const src = await one(`select (select sum(ad_cost) from core.ad_spend_daily where date_jst between $1::date and $2::date)::text as ad, (select sum(sales_jpy) from mart.v_sales_daily where date_jst between $1::date and $2::date)::text as sales`, [D1, D2]);
  assert.deepEqual([sum('ad_cost'), sum('sales_jpy')], [Number(src.ad), Number(src.sales)]);
  assert.deepEqual([sum('ad_cost'), sum('sales_jpy')], [285, 4400]);
});
await t('日ごと (p_by_day): 同じ出品が日ごとの行に分かれる・期間の外の日は読まない', async () => {
  const rows = (await eff(D1, D2, true)).filter((r) => Number(r.listing_id) === A).map((r) => [String(r.date_jst).length > 0, num(r.ad_cost), num(r.sales_jpy), num(r.acos_1d)]);
  assert.deepEqual(rows, [[true, 100, 2000, 0.1], [true, 60, 1000, null]]);
  const d2 = await eff(D2, D2);
  assert.deepEqual(d2.map((r) => [Number(r.listing_id), num(r.ad_cost), num(r.sales_jpy)]), [[A, 60, 1000], [E, 20, 0]], '期間の外の日を読んでいる');
});
await t('材料がそろっているか (coverage): 広告費の日・古い取込の行の日・広告費の無い日・注文のある日・売上日次が未公開の日・🚨 公開の後に注文が動いた日 (作り直し待ち。#1492 Codex R1)・開いた回', async () => {
  const cov = async (to = D3) => one(`select * from mart.ad_efficiency_coverage(1::smallint, 'amazon', 'jp', $1::date, $2::date)`, [D1, to]);
  const ds = (a) => (a || []).map((x) => String(x instanceof Date ? x.toISOString() : x).slice(0, 10));
  let c = await cov();
  // 売上日次の回がまだ一度も終わっていない (watermark が無い) = 公開済みの注文のある日は全部 作り直し待ち
  assert.deepEqual([c.days, c.ad_days, c.ad_legacy_days, ds(c.ad_missing_days), c.order_days, ds(c.sales_unpublished_days), ds(c.sales_pending_days), c.sales_session_open], [3, 2, 1, [D3], 3, [D3], [D1, D2], false]);
  await pg.query(`insert into mart.sales_daily_state (company_id, mall, scope_key, watermark) values (1, 'amazon', 'jp', now() + interval '30 seconds')`);
  c = await cov();
  assert.deepEqual(ds(c.sales_pending_days), [D1, D2], 'watermark の 30 秒前の更新 (集計の前に始まり後で commit した取込かもしれない) を見落とした (#1492 Codex R2)');
  await pg.query(`update mart.sales_daily_state set watermark = now() + interval '16 minutes'`);
  c = await cov();
  assert.deepEqual(ds(c.sales_pending_days), [], 'watermark − 15 分より前の更新を作り直し待ちにした');
  // 公開の後に注文が動いた (更新時刻を 1 時間後に固定 = watermark − 15 分より後。trigger は取引の中だけ外す)
  await pg.exec(`begin; alter table core.orders disable trigger trg_orders_touch; update core.orders set status = 'cancelled', is_cancelled = true, content_hash = 'h2', updated_at = now() + interval '1 hour' where mall_order_no = 'o-${D1}'; alter table core.orders enable trigger trg_orders_touch; commit;`);
  c = await cov();
  assert.deepEqual([ds(c.sales_pending_days), ds(c.sales_unpublished_days)], [[D1], [D3]]);
  await pg.query(`update mart.sales_daily_state set session_id = 'open', session_started_at = now()`);
  assert.equal((await cov(D1)).sales_session_open, true);
});
await t('別のモール・scope の行は混ざらない', async () => {
  assert.equal((await eff(D1, D2)).length, 8);
  assert.equal((await all(`select * from mart.ad_efficiency(1::smallint, 'amazon', 'us', $1::date, $2::date, false)`, [D1, D2])).length, 0);
  assert.equal((await all(`select * from mart.ad_efficiency(1::smallint, 'rakuten', 'main', $1::date, $2::date, false)`, [D1, D2])).length, 0);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
