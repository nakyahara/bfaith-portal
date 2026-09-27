#!/usr/bin/env node
/**
 * test-company-db-ad-efficiency.mjs — 出品ごとの広告の効き目 (0038・0039: mart.ad_efficiency / mart.ad_efficiency_coverage) の試験。PGlite。
 *   広告費 (core.ad_spend_daily) と売上日次 (mart.v_sales_daily) が同じ出品に集まる・出品に当たらない行を捨てない・比率は分母が 0 / 不明 / 一部だけ分かるなら null・
 *   日ごと・材料がそろっているか (公開の値が材料と食い違う日 = 0039)。売上に効く金額不明の明細は core から数える (取消の明細は数えない = 0039)
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
async function order(day, no, { mall = 'amazon', scope = 'jp', cancelled = false, lines = [] } = {}) {
  const id = (await one(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
    values (1, $3, $4, $1, 'mall_api', $2::timestamptz, $2::date, $5, $6, 1, $2::timestamptz, 'v1', 'h') returning order_id`, [no, day, mall, scope, cancelled ? 'cancelled' : 'new', cancelled])).order_id;
  for (const [i, l] of lines.entries()) {
    await pg.query(`insert into core.order_lines (company_id, order_id, line_key, listing_id, qty, cancelled_qty, line_amount_jpy, amount_source, received_batch_seq) values (1, $1, $2, $3, $4, $5, $6, 'mall_api', 1)`,
      [id, `k${i}`, l.lid, l.qty ?? 1, l.cxl ?? 0, l.amount ?? null]);
  }
  return id;
}

// D1: A (広告 100・経由 1000)・B (広告 50・経由は分からない)・E (広告 30・経由 100)・F (広告 10・経由 20)・ASIN の行 10・出品の分からない SKU 5 /
//     売上 A 2000 (自社 1500 + FBA 500)・C 500 (金額の分からない明細 1)・E 400・F 200 (同じ注文が SKU で 2 粒度に分かれる = 延べ 2)・出品の分からない明細 300
await adDay(D1, [{ code: 'sku-a', lid: A, cost: '100', s1: '1000', u1: 2, clicks: 10, imp: 100 }, { code: 'sku-b', lid: B, cost: '50', s1: null }, { code: 'sku-e', lid: E, cost: '30', s1: '100' },
  { code: 'sku-f', lid: F, cost: '10', s1: '20' }, { g: 'asin', code: 'b0x', cost: '10', s1: '0' }, { code: 'sku-x', lid: null, cost: '5', s1: '0' }], { legacy: true });
await salesDay(D1, [{ lid: A, sales: 1500, units: 2, shop: '4' }, { lid: A, sales: 500, units: 1 }, { lid: C, sales: 500, unknown: 1 }, { lid: E, sales: 400 },
  { lid: F, sales: 100, sku: S1 }, { lid: F, sales: 100, sku: S2 }, { lid: null, sales: 300 }]);
// D2: A (広告 60・経由 0) / E (広告 20・経由は分からない = 期間まとめでは 一部だけ分かる) / 売上 A 1000 (1 個取消)
await adDay(D2, [{ code: 'sku-a', lid: A, cost: '60', s1: '0', u1: 0 }, { code: 'sku-e', lid: E, cost: '20', s1: null }]);
await salesDay(D2, [{ lid: A, sales: 1000, units: 3, cancelled: 1 }, { lid: B, sales: 0, units: 2 }]);   // B = 金額の分からない明細だけ (売上 0)
// D3: 注文はあるが売上日次は未公開・広告費の日が無い
for (const d of [D1, D2, D3]) await order(d, `o-${d}`);
// 売上に効く金額不明: C の注文 (取り消されていない・金額 null) / 効かない: A の取消の注文 (数量 0・金額 null)・E の明細の全部取消 (金額 null)
await order(D1, 'o-c-unknown', { lines: [{ lid: C, amount: null }] });
await order(D1, 'o-a-cancelled', { cancelled: true, lines: [{ lid: A, qty: 0, amount: null }] });
await order(D1, 'o-e-line-cancelled', { lines: [{ lid: E, qty: 1, cxl: 1, amount: null }] });
// 効く (0021 の式どおり): 取り消されていない注文の数量 0 の明細 (0021 は商品代を足す)・一部だけ取り消された明細 (#1493 Codex R1)
await order(D2, 'o-b-qty0', { lines: [{ lid: B, qty: 0, amount: null }, { lid: B, qty: 2, cxl: 1, amount: null }] });

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
await t('🚨 一部だけ分かっている和で比率を作らない (#1492 Codex R1): 広告経由の売上が 1 日分からない → ACoS・広告経由の割合は null (TACoS は出す) / 売上に効く金額不明の明細 → TACoS・広告経由の割合は null。🚨 取消の明細 (数量 0・全部取消) の金額不明は売上に効かない = 数えない (0039)', async () => {
  const rows = await eff(D1, D2);
  const e = row(rows, E), c = row(rows, C);
  assert.deepEqual([num(e.ad_cost), num(e.ad_sales_1d), num(e.ad_unknown_rows), num(e.sales_jpy), num(e.tacos), e.acos_1d, e.ad_sales_share], [50, 100, 1, 400, 0.125, null, null]);
  assert.deepEqual([num(c.sales_jpy), num(c.sales_amount_unknown_lines), c.tacos, c.ad_sales_share], [500, 1, null, null]);
  const a = row(rows, A);
  assert.deepEqual([num(a.sales_amount_unknown_lines), num(a.tacos)], [0, 0.0533], '取消の注文の金額不明で A の TACoS を消した');
  assert.equal(num(e.sales_amount_unknown_lines), 0, '全部取り消された明細の金額不明を数えた');
  assert.equal(num(row(rows, B).sales_amount_unknown_lines), 2, '取り消されていない注文の数量 0 の明細・一部取消の明細の金額不明を数えなかった (0021 の式では売上に効く)');
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
  assert.deepEqual(d2.map((r) => [Number(r.listing_id), num(r.ad_cost), num(r.sales_jpy)]), [[A, 60, 1000], [B, 0, 0], [E, 20, 0]], '期間の外の日を読んでいる');
});
await t('材料がそろっているか (coverage): 広告費の日・古い取込の行の日・広告費の無い日・注文のある日・売上日次が未公開の日・開いた回', async () => {
  const cov = async (to = D3) => one(`select * from mart.ad_efficiency_coverage(1::smallint, 'amazon', 'jp', $1::date, $2::date)`, [D1, to]);
  const ds = (a) => (a || []).map((x) => String(x instanceof Date ? x.toISOString() : x).slice(0, 10));
  const c = await cov();
  assert.deepEqual([c.days, c.ad_days, c.ad_legacy_days, ds(c.ad_missing_days), c.order_days, ds(c.sales_unpublished_days), c.sales_session_open], [3, 2, 1, [D3], 3, [D3], false]);
  await pg.query(`insert into mart.sales_daily_state (company_id, mall, scope_key, watermark, session_id, session_started_at) values (1, 'amazon', 'jp', now(), 'open', now())`);
  assert.equal((await cov(D1)).sales_session_open, true);
});
await t('🚨 公開の値が古い日 (sales_stale_days。0039 = 検算の食い違い ∪ 公開した回の後に注文が動いた日): 本物の作り直しの直後は出ない / 日の合計が変わらない出品の付け替えも・金額の変更も出る / 作り直すと消える', async () => {
  // 別のモール (qoo10) で本物の注文 → 本物の作り直し (refresh_sales_daily) → 公開
  const Q = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'qoo10', '', 'Q-1', 'active') returning listing_id`)).listing_id);
  const o1 = await order(D1, 'q-1', { mall: 'qoo10', scope: 'main', lines: [{ lid: Q, amount: 1000 }] });
  await order(D2, 'q-2', { mall: 'qoo10', scope: 'main', lines: [{ lid: Q, amount: 700 }] });
  let left = 1, reset = true;
  while (left > 0) { left = (await one(`select remaining from mart.refresh_sales_daily(1::smallint, 'qoo10', 'main', 100, $1, 'test')`, [reset])).remaining; reset = false; }
  const cq = async () => one(`select * from mart.ad_efficiency_coverage(1::smallint, 'qoo10', 'main', $1::date, $2::date)`, [D1, D2]);
  const ds = (a) => (a || []).map((x) => String(x instanceof Date ? x.toISOString() : x).slice(0, 10));
  let c = await cq();
  assert.deepEqual([ds(c.sales_unpublished_days), ds(c.sales_stale_days), c.sales_session_open], [[], [], false], '本物の作り直しの直後に食い違いを出した');
  const ef = await all(`select listing_id, sales_jpy, sales_amount_unknown_lines from mart.ad_efficiency(1::smallint, 'qoo10', 'main', $1::date, $2::date, false)`, [D1, D2]);
  assert.deepEqual(ef.map((r) => [Number(r.listing_id), num(r.sales_jpy), num(r.sales_amount_unknown_lines)]), [[Q, 1700, 0]]);
  // 🚨 日の合計が変わらない変更 (明細の出品の付け替え。本物の経路 = 0024 の結び直しは注文の updated_at も動かす) も出る (#1493 Codex R1 High)
  const Q2 = Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'qoo10', '', 'Q-2', 'active') returning listing_id`)).listing_id);
  await pg.query(`update core.order_lines set listing_id = $2 where order_id = $1`, [o1, Q2]);
  await pg.query(`update core.orders set content_hash = 'h-relinked' where order_id = $1`, [o1]);
  assert.equal((await one(`select count(*)::int n from mart.sales_daily_check(1::smallint, 'qoo10', 'main', $1::date, $2::date)`, [D1, D2])).n, 0, '前提: 日の合計は変わらない');
  c = await cq();
  assert.deepEqual(ds(c.sales_stale_days), [D1]);
  // 作り直すと消える → 公開の後に明細の金額が変わった (日の合計が変わる) も出る
  left = 1; while (left > 0) left = (await one(`select remaining from mart.refresh_sales_daily(1::smallint, 'qoo10', 'main', 100, false, 'test')`)).remaining;
  assert.deepEqual(ds((await cq()).sales_stale_days), []);
  await pg.query(`update core.order_lines set line_amount_jpy = 1500 where order_id = $1`, [o1]);
  c = await cq();
  assert.deepEqual(ds(c.sales_stale_days), [D1]);
});
await t('別のモール・scope の行は混ざらない', async () => {
  assert.equal((await eff(D1, D2)).length, 8);
  assert.equal((await all(`select * from mart.ad_efficiency(1::smallint, 'amazon', 'jp', $1::date, $2::date, false)`, [D1, D2])).every((r) => r.listing_id == null || [A, B, C, E, F].includes(Number(r.listing_id))), true);
  assert.equal((await all(`select * from mart.ad_efficiency(1::smallint, 'amazon', 'us', $1::date, $2::date, false)`, [D1, D2])).length, 0);
  assert.equal((await all(`select * from mart.ad_efficiency(1::smallint, 'rakuten', 'main', $1::date, $2::date, false)`, [D1, D2])).length, 0);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
