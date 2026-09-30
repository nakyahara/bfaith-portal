#!/usr/bin/env node
/**
 * bench-company-db-amazon-profit.mjs — Amazon の利益の mart (0049 → 0050) の時間を合成のデータで比べる (PGlite・本番には触れない)
 *
 * 合成のデータ (既定 = 出品 1,000 × 30 日 = 2026-09):
 *   出品ごとに 1 日 1 注文 (1〜3 個)・10 出品に 1 つは返品の行・10 出品に 1 つは Easy Ship の料金・毎日の保管料の行 /
 *   SKU = 出品ごとに 1 つ (5 出品に 1 つはセット = 2 SKU)・原価 = 2026-01-01 から (20 SKU に 1 つは月の途中で変わる) /
 *   広告 = 出品ごとに毎日 1 行 (sku) + 50 行に 1 行は ASIN / 監査の記録 = 出品と構成を作ったときの INSERT
 * 手順: 0049 まで流す → データを入れる → 0049 の関数を測る (と結果を控える) → 0050 を流す → 測る → 結果が完全に同じか確かめる (calculated_at を除く全部の列)
 * 使い方: node scripts/bench-company-db-amazon-profit.mjs [--listings 1000] [--days 30] [--runs 3]
 *   --profile [--latest] [--full] [--generic] = 行の本体の SQL を EXPLAIN ANALYZE (--latest = 最新の migration の本体・--generic = 関数の中と同じ generic plan)
 *   🚨 本番の件数での時間は README の「本番の所要時間を読むだけで測る」(本適用の後に読むだけで)
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';

const arg = (name, def) => { const i = process.argv.indexOf(`--${name}`); return i >= 0 ? Number(process.argv[i + 1]) : def; };
const L = arg('listings', 1000), D = arg('days', 30), RUNS = arg('runs', 3);
const FROM = '2026-09-01', TO = new Date(Date.UTC(2026, 8, D)).toISOString().slice(0, 10);

const pg = new PGlite();
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: () => {}, to: '0049' });
const q = (sql, p) => pg.query(sql, p);

console.log(`合成のデータ: 出品 ${L} × ${D} 日 (${FROM}〜${TO})`);
const t0 = Date.now();
await pg.exec(`
  insert into core.products (company_id, name) select 1, 'p' || g from generate_series(0, ${L} + ${Math.ceil(L / 5)} - 1) g;
  insert into core.skus (company_id, product_id, sku_kind, code, name)
    select 1, product_id, 'single', 'sku-' || lpad((row_number() over (order by product_id) - 1)::text, 5, '0'), name from core.products;
  -- 原価: 全部 2026-01-01 から / 20 SKU に 1 つは 9/15 に変わる
  insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to, created_at)
    select 1, sku_id, 100 + sku_id % 500, 'ne', 'COMPLETE', '2026-01-01', case when sku_id % 20 = 0 then date '2026-09-14' end, '2026-01-01' from core.skus;
  insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to, created_at)
    select 1, sku_id, 120 + sku_id % 500, 'ne', 'COMPLETE', '2026-09-15', null, '2026-09-15' from core.skus where sku_id % 20 = 0;
  insert into core.listings (company_id, mall, shop_code, listing_code, status)
    select 1, 'amazon', 'main@A1VC38T7YXB528', 'L' || lpad(g::text, 5, '0'), 'active' from generate_series(0, ${L} - 1) g;
`);
// 構成: 出品 i → SKU i・5 出品に 1 つはセット (もう 1 つの SKU)
await pg.exec(`
  insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type)
    select 1, l.listing_id, s.sku_id, 1, 'imported', 'system'
      from (select listing_id, row_number() over (order by listing_id) - 1 as i from core.listings) l
      join (select sku_id, row_number() over (order by sku_id) - 1 as i from core.skus) s on s.i = l.i;
  insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type)
    select 1, l.listing_id, s.sku_id, 2, 'imported', 'system'
      from (select listing_id, row_number() over (order by listing_id) - 1 as i from core.listings) l
      join (select sku_id, row_number() over (order by sku_id) - 1 - ${L} as j from core.skus) s on l.i % 5 = 0 and s.j = l.i / 5;
`);
// 財務 (今の形の版の行を直に入れる = 受け口の検証は別の試験)。受領記録 → 行
const days = `(select g::date as d from generate_series(date '${FROM}', date '${TO}', interval '1 day') g) dd`;
await pg.exec(`
  insert into core.order_finance_receipts (company_id, mall, scope_key, mall_order_no, received_batch_seq, set_checksum, lines, transform_version)
    select 1, 'amazon', 'jp', 'B-' || l.listing_code || '-' || d, 1, repeat('a', 64), 1, 'amazon_finance_v2' from core.listings l cross join ${days};
  insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, line_kind, source, listing_id,
      units_ordered, sales_principal_jpy, commission_jpy, fba_fulfillment_jpy, net_jpy, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash)
    select 1, 'amazon', 'jp', 'B-' || l.listing_code || '-' || d, d, l.listing_code, 'sku', 'amazon_settlement_unified', l.listing_id,
           u, 1000 * u, -150 * u, -300 * u, 550 * u, 3, 1, now(), 'amazon_finance_v2', 'h'
      from core.listings l cross join ${days} cross join lateral (select 1 + ((l.listing_id + extract(day from d)::int) % 3) as u) x;
  -- 返品 (10 出品に 1 つ・同じ日)
  insert into core.order_finance_receipts (company_id, mall, scope_key, mall_order_no, received_batch_seq, set_checksum, lines, transform_version)
    select 1, 'amazon', 'jp', 'R-' || l.listing_code || '-' || d, 1, repeat('a', 64), 1, 'amazon_finance_v2' from core.listings l cross join ${days} where l.listing_id % 10 = 0;
  insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, line_kind, source, listing_id,
      refund_principal_jpy, refund_principal_customer_jpy, commission_jpy, net_jpy, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash)
    select 1, 'amazon', 'jp', 'R-' || l.listing_code || '-' || d, d, l.listing_code, 'sku', 'amazon_settlement_unified', l.listing_id,
           -1000, -1000, 150, -850, 2, 1, now(), 'amazon_finance_v2', 'h'
      from core.listings l cross join ${days} where l.listing_id % 10 = 0;
  -- Easy Ship (10 出品に 1 つ・注文の行に)
  insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, line_kind, source,
      other_fee_jpy, account_fee_amount_jpy, net_jpy, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash)
    select 1, 'amazon', 'jp', 'B-' || l.listing_code || '-' || d, d, '-', 'easy_ship', 'amazon_settlement_unified', -330, -330, -330, 1, 1, now(), 'amazon_finance_v2', 'h'
      from core.listings l cross join ${days} where l.listing_id % 10 = 1;
  -- 保管料 (毎日の疑似注文)
  insert into core.order_finance_receipts (company_id, mall, scope_key, mall_order_no, received_batch_seq, set_checksum, lines, transform_version)
    select 1, 'amazon', 'jp', '-:' || d, 1, repeat('a', 64), 1, 'amazon_finance_v2' from ${days};
  insert into core.order_finance_daily (company_id, mall, scope_key, mall_order_no, economic_date_jst, seller_sku, line_kind, source,
      fba_storage_jpy, account_fee_amount_jpy, net_jpy, source_lines, received_batch_seq, source_updated_at, transform_version, content_hash)
    select 1, 'amazon', 'jp', '-:' || d, d, '-', 'storage', 'amazon_settlement_unified', -500, -500, -500, 1, 1, now(), 'amazon_finance_v2', 'h' from ${days};
  -- 広告: 日の記録 + 出品ごとに 1 行 (sku) + 50 出品に 1 つは ASIN の行 (未解決)
  insert into core.ad_spend_days (company_id, mall, scope_key, ad_type, date_jst, source_generation, source_report_id, checksum, row_count, cost_total, ingest_run_id)
    select 1, 'amazon', 'jp', 'SP', d, 1, 'r-' || d, repeat('b', 64), 0, 0, 'bench' from ${days};
  insert into core.ad_spend_daily (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code, listing_id, clicks, impressions, ad_cost, ingest_run_id)
    select 1, 'amazon', 'jp', 'SP', d, 'c1', 'sku', l.listing_code, l.listing_id, 1, 10, 12.34, 'bench' from core.listings l cross join ${days};
  insert into core.ad_spend_daily (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code, listing_id, clicks, impressions, ad_cost, ingest_run_id)
    select 1, 'amazon', 'jp', 'SP', d, 'c2', 'asin', 'B0' || l.listing_code, null, 1, 10, 1.00, 'bench' from core.listings l cross join ${days} where l.listing_id % 50 = 0;
  analyze;
`);
const counts = (await q(`select (select count(*) from core.order_finance_daily) f, (select count(*) from core.ad_spend_daily) a, (select count(*) from events.master_change_events) e`)).rows[0];
console.log(`  財務の行 ${counts.f}・広告の行 ${counts.a}・監査の記録 ${counts.e} (入れるのに ${((Date.now() - t0) / 1000).toFixed(1)} 秒)`);
// coverage を与える (D7b-1b の後の形 = 正式な値も計算される)
await pg.exec(`create or replace function core.finance_coverage_state(p_company_id smallint, p_mall text, p_scope_key text, p_source text)
  returns table (complete_to date, generation bigint, source_revision bigint) language plpgsql stable as $$ begin return query select date '${TO}', 1::bigint, 1::bigint; end $$;`);

// --profile: 行の本体 (mart._amazon_profit_rows) の SQL を引数を埋めて EXPLAIN ANALYZE (どこが重いか・関数の中は EXPLAIN に出ないので本体を取り出す)
if (process.argv.includes('--profile')) {
  if (process.argv.includes('--latest')) await applyMigrations(db, { log: () => {} });   // 最新 (0050) の本体を見る
  const src = (await q(`select p.prosrc from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'mart' and p.proname = '_amazon_profit_rows'`)).rows[0].prosrc;
  const args = { p_company_id: '1::smallint', p_mall: `'amazon'`, p_scope_key: `'jp'`, p_from: `date '${FROM}'`, p_to: `date '${TO}'` };
  const arr = { p_days: '_amazon_profit_finance_days', p_ad_days: '_amazon_profit_ad_days', p_adc: '_amazon_profit_ad_children', p_es: '_amazon_easy_ship_alloc' };
  const call = `(1::smallint, 'amazon', 'jp', date '${FROM}', date '${TO}')`;
  let body = src;
  if (process.argv.includes('--generic')) {
    // 関数の中と同じ = 引数を値でなく parameter のまま計画する (generic plan)
    const names = ['p_company_id', 'p_mall', 'p_scope_key', 'p_from', 'p_to', 'p_days', 'p_ad_days', 'p_adc', 'p_es'];
    names.forEach((k, i) => { body = body.replace(new RegExp(`\\b${k}\\b`, 'g'), `$${i + 1}`); });
    await pg.exec(`set plan_cache_mode = force_generic_plan`);
    await q(`prepare gp(smallint, text, text, date, date, mart.amazon_profit_finance_day[], mart.amazon_profit_ad_day[], mart.amazon_profit_ad_child[], mart.amazon_easy_ship_alloc_row[]) as ${body}`);
    const lits = [];
    for (const fn of Object.values(arr)) lits.push((await q(`select array(select x from mart.${fn}${call} x)::text as t`)).rows[0].t);   // EXECUTE の引数に副問い合わせは書けない = 値の文字で渡す
    const types = ['mart.amazon_profit_finance_day[]', 'mart.amazon_profit_ad_day[]', 'mart.amazon_profit_ad_child[]', 'mart.amazon_easy_ship_alloc_row[]'];
    const ex = `execute gp(1::smallint, 'amazon', 'jp', date '${FROM}', date '${TO}', ${lits.map((l, i) => `'${l.replace(/'/g, "''")}'::${types[i]}`).join(', ')})`;
    const plan = (await q(`explain (analyze, costs on, buffers off) ${ex}`)).rows.map((r) => r['QUERY PLAN']);
    console.log(plan.join('\n'));
    process.exit(0);
  }
  for (const [k, fn] of Object.entries(arr)) body = body.replace(new RegExp(`\\b${k}\\b`, 'g'), `array(select x from mart.${fn}${call} x)`);
  for (const [k, v] of Object.entries(args)) body = body.replace(new RegExp(`\\b${k}\\b`, 'g'), v);
  const time = async (label, sql) => { const s = performance.now(); const n = (await q(sql)).rows.length; console.log(`  ${label}: ${((performance.now() - s) / 1000).toFixed(2)} 秒 (${n} 行)`); };
  await time('公開の関数 (plpgsql の包み + 本体)', `select * from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '${FROM}', '${TO}')`);
  await time('本体の関数を直に (材料の配列を渡す)', `select * from mart._amazon_profit_rows(1::smallint, 'amazon', 'jp', date '${FROM}', date '${TO}', ${Object.values(arr).map((fn) => `array(select x from mart.${fn}${call} x)`).join(', ')})`);
  await time('本体の SQL を引数を埋めて', body);
  const plan = (await q(`explain (analyze, costs off, buffers off) ${body}`)).rows.map((r) => r['QUERY PLAN']);
  console.log(process.argv.includes('--full') ? plan.join('\n') : plan.filter((l) => /actual time=\d+\.\d+\.\.(\d{3,})/.test(l) || /CTE|Execution/.test(l)).join('\n'));
  process.exit(0);
}

const ROWS = `select * from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '${FROM}', '${TO}')`;
const TOTALS = `select * from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', '${FROM}', '${TO}')`;
const strip = (rows) => rows.map((r) => { const { calculated_at, ...rest } = r; return JSON.stringify(rest, (k, v) => (typeof v === 'bigint' ? String(v) : v)); });
const measure = async (label) => {
  const out = {};
  for (const [name, sql] of [['daily', ROWS], ['totals', TOTALS]]) {
    // 時間 = DB の中の計算だけ (count(*) で包む = PGlite が約 100 列の行を JS に渡す時間を入れない)。結果の突き合わせは別に 1 回全部を読む
    const ms = [];
    for (let i = 0; i < RUNS; i++) { const s = performance.now(); await q(`select count(*) from (${sql}) z`); ms.push(performance.now() - s); }
    ms.sort((a, b) => a - b);
    const rows = (await q(`${sql} order by 1, 2, 3, 4, 5, 6`)).rows;
    out[name] = { ms: ms[Math.floor(ms.length / 2)], rows };
    console.log(`  ${label} ${name}: ${(out[name].ms / 1000).toFixed(2)} 秒 (DB の中・中央値・${RUNS} 回・${rows.length} 行)`);
  }
  return out;
};
const before = await measure('0049');
const r = await applyMigrations(db, { log: () => {} });
if (!r.applied.includes('0050')) { console.log('0050 が無い = 比べない'); process.exit(0); }
const after = await measure('0050');
for (const name of ['daily', 'totals']) {
  const a = strip(before[name].rows), b = strip(after[name].rows);
  assert.equal(b.length, a.length, `${name} の行の数が違う`);
  for (let i = 0; i < a.length; i++) assert.equal(b[i], a[i], `${name} の ${i} 行目が違う`);
  console.log(`  ${name}: 結果は完全に同じ (${a.length} 行・calculated_at を除く全部の列)・時間の比 0050 / 0049 = ${(after[name].ms / before[name].ms).toFixed(2)}`);
}
await pg.close();
