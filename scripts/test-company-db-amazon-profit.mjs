#!/usr/bin/env node
/**
 * test-company-db-amazon-profit.mjs — Amazon の利益の mart (0049・D7b-3) の受入試験
 *
 * 設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 (§3.3・§3.5・§3.5b・§3.6・§3.7・D-59・D-61・D-64)。PGlite で 0001〜0049 を流して確かめる:
 *   手で計算した値 (税込 / 税抜 / 返品 / 負の手数料 / 値引きの税 / override_zero と原価不明 / 広告 × 1.1) / 各ゲートで null・理由のコードの固定の順・0 と仮定の値と理由 /
 *   広告 (ASIN は未解決・別名の SKU は結ぶ・未解決は出品の行だけ止める・legacy・missing・not_collected) / 構成 0 件・候補 2 件 / master_notes は値を止めない /
 *   原価の選び方 (同じ日に 2 回変わった・観測・推定) / hash = JS の canonicalSha256 と同じ / Easy Ship の割り振り (割合・等分・端数・返金・期間に依らない・配れない額) /
 *   日の合計 (row_kind が重ならない・取引の無い日も日の行・列の組ごとの条件・月の手数料と税の表・保存則) / coverage の関数が null なら正式な値は全部 null・差し替えれば出る /
 *   受け口 (本物の router を HTTP で: 鍵・契約・409 not_migrated・ID は文字列)
 * 🚨 試験に無いもの: 本番の件数 (1 日 数百の出品 × 400 日) での所要時間 (statement_timeout 120s の中に入るか = 本適用の後に読むだけで測る)
 * 実行: node scripts/test-company-db-amazon-profit.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import { validateFinanceRows, orderFinanceChecksum, financeRowsFormat, CLASS_COLUMNS, pseudoOrderNo } from '../apps/company-db/finance/order-finance-checksum.mjs';
import { canonicalSha256 } from '../apps/company-db/canonical-hash.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const rejects = async (fn, re) => { let threw = null; try { await fn(); } catch (e) { threw = e; } if (!threw) throw new Error('did not throw'); if (re && !re.test(threw.message)) throw new Error(`wrong error: ${threw.message}`); return threw; };
const quiet = () => {};

const pg = new PGlite();
// 0050 (速くした本体) の結果が 0049 と完全に同じかを確かめるため、まず 0049 まで流してデータを入れ、控えを取ってから 0050 を流す (下の「0050 = 0049」の試験)
const applied = await applyMigrations(pgliteAdapter(pg), { log: quiet, to: '0049' });
assert.ok(applied.applied.includes('0049'), '0049 が流れていない');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const co = 1;
const N = (x) => (x == null ? null : Number(x));

// ─── 財務の行を入れる (受け口と同じ手順: 形を確かめ → 指紋 → 関数)。4 列の鍵があれば今の形 (amazon_finance_v2)・無ければ旧い形 (t1) ───
const U = 'amazon_settlement_unified';
const C4 = (c = 0, m = 0, a = 0, u = 0) => ({ unclassified_component_count: c, unclassified_mapped_jpy: m, unclassified_abs_jpy: a, unmapped_component_count: u });
const row = (date, sku, x = {}) => ({ economic_date_jst: date, seller_sku: sku, line_kind: sku === '-' ? 'unknown' : 'sku', source: U, source_lines: 1, source_updated_at: `${date}T00:00:00Z`, content_hash: 'h', ...x });
const v2 = (date, sku, x = {}) => row(date, sku, { ...C4(), ...x });
let seq = 0;
const apply = async (order, rows, version = null) => {
  const legacy = rows.length > 0 && financeRowsFormat(rows) !== 'v2';   // 墓石 (空の集合) は今の形の版で送る (旧い版への戻しは拒まれる)
  const content = validateFinanceRows(order, rows);
  const checksum = orderFinanceChecksum(content, { legacy });
  const lines = content.map((c, i) => { const o = { ...c, source_updated_at: rows[i].source_updated_at, content_hash: rows[i].content_hash }; if (legacy) for (const k of CLASS_COLUMNS) delete o[k]; return o; });
  const r = (await one(`select core.apply_order_finance_batch($1::smallint, 'amazon', 'jp', $2, $3::bigint, $4, $5, $6::jsonb) as r`, [co, order, ++seq, checksum, version ?? (legacy ? 't1' : 'amazon_finance_v2'), JSON.stringify(lines)])).r;
  assert.equal(r, 'applied', `${order}: ${r}`);
};

// ─── マスタ ───
const SHOP = 'main@A1VC38T7YXB528';
const sku = async (code) => Number((await one(`with p as (insert into core.products (company_id, name) values (1, $1) returning product_id)
  insert into core.skus (company_id, product_id, sku_kind, code, name) select 1, product_id, 'single', $1, $1 from p returning sku_id`, [code])).sku_id);
const listing = async (code, shop = SHOP) => Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', $1, $2, 'active') returning listing_id`, [shop, code])).listing_id);
const comp = (lid, sid, qty) => pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, $3, 'manual', 'human')`, [lid, sid, qty]);
const cost = async (sid, c, status, from, to = null, created = '2026-01-01T00:00:00Z', source = 'ne') => Number((await one(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to, created_at)
  values (1, $1, $2, $3, $4, $5, $6, $7) returning sku_cost_id`, [sid, c, source, status, from, to, created])).sku_cost_id);

const S1 = await sku('sku-1'), S2 = await sku('sku-2'), S3 = await sku('sku-3'), S4 = await sku('sku-4'), S5 = await sku('sku-5');
const C_S1 = await cost(S1, 300, 'COMPLETE', '2026-01-01');
const C_S2 = await cost(S2, 0, 'OVERRIDDEN', '2026-01-01', null, '2026-01-01T00:00:00Z', 'override_zero');
await cost(S3, 500, 'PARTIAL', '2026-01-01');
// 同じ日 (6/15) に 2 回変わった取込の行 (Y = 6/15〜6/15・Z = 6/15〜今も。created_at は Z が後) = 6/15 は Z だけ・6/14 は X
const C_X = await cost(S5, 100, 'COMPLETE', '2026-01-01', '2026-06-14', '2026-06-01T00:00:00Z');
const C_Y = await cost(S5, 110, 'COMPLETE', '2026-06-15', '2026-06-15', '2026-06-15T01:00:00Z');
const C_Z = await cost(S5, 120, 'COMPLETE', '2026-06-15', null, '2026-06-15T02:00:00Z');
// 観測の原価 (S4 = sku_costs が無い): 推定 2026-01-01〜05-04 / 観測 05-05〜今も
const load = Number((await one(`insert into core.sku_cost_observed_loads (company_id, source, generation, checksum, row_count, unresolved_code_count, ambiguous_code_count, ingest_run_id)
  values (1, 'warehouse_sqlite', 7, $1, 2, 0, 0, 'run-1') returning observed_load_id`, ['a'.repeat(64)])).observed_load_id);
const obs = async (from, to, method) => Number((await one(`insert into core.sku_cost_observed (observed_load_id, company_id, generation, sku_id, product_code, cost_jpy, cost_status, valid_from, valid_to, backfill_method, first_observed_at, source_history_id)
  values ($1, 1, 7, $2, 'sku-4', 200, 'COMPLETE', $3, $4, $5, '2026-05-04T20:00:00Z', 1) returning sku_cost_observed_id`, [load, S4, from, to, method])).sku_cost_observed_id);
const O_EST = await obs('2026-01-01', '2026-05-04', 'estimated_before_first_snapshot');
const O_OBS = await obs('2026-05-05', null, 'observed_daily_diff');

const LA = await listing('LA'), LB = await listing('LB'), LC = await listing('LC'), LD = await listing('LD');
const LE1 = await listing('LE'), LE2 = await listing('LE', 'other@X');   // 同じ正規化 SKU が 2 つ (shop が違う) = 未解決
const LF = await listing('LF'), LG = await listing('LG'), LH = await listing('LH'), LK = await listing('LK');
const LP = await listing('LP'), LQ = await listing('LQ'), LR = await listing('LR');
await comp(LA, S1, 2); await comp(LB, S2, 1); await comp(LC, S3, 1);   // LD = 構成なし
await comp(LE1, S1, 1); await comp(LE2, S1, 1);
await comp(LF, S1, 1); await comp(LF, S4, 1); await comp(LG, S1, 1); await comp(LH, S1, 1); await comp(LK, S5, 1);
await comp(LP, S2, 1); await comp(LQ, S2, 1); await comp(LR, S2, 1);
// 広告の SKU の別名 (external_ids の listing の別名 = resolve_listing_id が拾う。財務の直接の一致は拾わない)
await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'listing', $1, 'amazon', 'seller_sku', 'LA-ALIAS', 'manual', 'human')`, [LA]);
// LN1 だけがある間に財務を受け取り、後で LN2 (別の shop) を作る = 今は未解決・受け取ったときは LN1
const LN1 = await listing('LN');
await comp(LN1, S1, 1);

// ─── 財務 (2026-06 と 1 月) ───
await apply('O1', [
  v2('2026-06-05', 'LA', { units_ordered: 2, sales_principal_jpy: 2000, commission_jpy: -300, fba_fulfillment_jpy: -200, promotion_jpy: -110, promotion_tax_jpy: -10, points_jpy: -20, sales_tax_jpy: 200 }),
  v2('2026-06-05', '-', { line_kind: 'easy_ship', other_fee_jpy: -330, account_fee_amount_jpy: -330 }),
  v2('2026-06-06', 'LA', { refund_principal_jpy: -1000, refund_principal_customer_jpy: -1000, commission_jpy: 150 }),   // 返品 1 個 (単価 1,000)・手数料の戻り (負の手数料)
]);
await apply('O2', [v2('2026-06-05', 'LB', { units_ordered: 1, sales_principal_jpy: 1000, commission_jpy: -150 })]);
await apply('O3', [v2('2026-06-05', 'LC', { units_ordered: 1, sales_principal_jpy: 500 })]);
await apply('O4', [v2('2026-06-05', 'LD', { units_ordered: 1, sales_principal_jpy: 700 })]);
await apply('O5', [v2('2026-06-05', 'LE', { units_ordered: 1, sales_principal_jpy: 800 })]);
await apply('O6', [v2('2026-06-05', 'ZZ-NONE', { units_ordered: 1, sales_principal_jpy: 600 })]);
await apply('O12', [v2('2026-06-08', 'LA', { units_ordered: 1, sales_principal_jpy: 1000 })]);
await apply('O7', [v2('2026-06-13', 'LA', { units_ordered: 1, sales_principal_jpy: 1000 }), v2('2026-06-13', 'LF', { units_ordered: 1, sales_principal_jpy: 400 }),
  v2('2026-06-13', '-', { line_kind: 'easy_ship', other_fee_jpy: -100, account_fee_amount_jpy: -100 })]);
await apply('O9', [v2('2026-06-13', 'LP', { units_ordered: 1 }), v2('2026-06-13', 'LQ', { units_ordered: 1 }), v2('2026-06-13', 'LR', { units_ordered: 1 }),
  v2('2026-06-13', '-', { line_kind: 'easy_ship', other_fee_jpy: -100, account_fee_amount_jpy: -100 })]);
await apply('O8', [v2('2026-06-13', '-', { line_kind: 'easy_ship', other_fee_jpy: -50, account_fee_amount_jpy: -50 })]);   // 売上の行が無い注文 = 配らない
await apply('O10', [v2('2026-06-12', 'LF', { units_ordered: 1, sales_principal_jpy: 400 }), v2('2026-06-14', '-', { line_kind: 'easy_ship', other_fee_jpy: -80, account_fee_amount_jpy: -80 })]);
await apply('O13', [v2('2026-06-20', 'LA', { units_ordered: 1, sales_principal_jpy: 1000 }), v2('2026-06-21', '-', { line_kind: 'easy_ship', other_fee_jpy: 30, account_fee_amount_jpy: 30 })]);   // 返金が多い = 正味が正
// 本体売上が負の SKU を含む注文 (#1559 Codex R3): −18 / −18 / 1,036 に料金 100 → 重み = max(本体, 0) = 0 / 0 / 100 (前の規則は −1 / −1 / 103 = 101)
await apply('O16', [v2('2026-06-22', 'LP', { units_ordered: 1, sales_principal_jpy: -18 }), v2('2026-06-22', 'LQ', { units_ordered: 1, sales_principal_jpy: -18 }),
  v2('2026-06-22', 'LR', { units_ordered: 1, sales_principal_jpy: 1036 }), v2('2026-06-22', '-', { line_kind: 'easy_ship', other_fee_jpy: -100, account_fee_amount_jpy: -100 })]);
// 全部が負 (正の重みの合計 0) = 売上の行のある SKU で等分: 料金 11 → 6 / 5
await apply('O17', [v2('2026-06-23', 'LP', { units_ordered: 1, sales_principal_jpy: -10 }), v2('2026-06-23', 'LQ', { units_ordered: 1, sales_principal_jpy: -20 }),
  v2('2026-06-23', '-', { line_kind: 'easy_ship', other_fee_jpy: -11, account_fee_amount_jpy: -11 })]);
await apply('OK1', [v2('2026-06-14', 'LK', { units_ordered: 1, sales_principal_jpy: 900 })]);
await apply('OK2', [v2('2026-06-15', 'LK', { units_ordered: 1, sales_principal_jpy: 900 })]);
await apply('O11', [v2('2026-06-10', 'LB', { units_ordered: 1, sales_principal_jpy: 1000, unmapped_jpy: -3, ...C4(2, 0, 200, 1) })]);   // 分けられない部品 (+100 / −100 = 金額 0 でも 2 つ) と unmapped
await apply('OL', [row('2026-06-11', 'LG', { units_ordered: 1, sales_principal_jpy: 400 })]);   // 旧い形 (4 列なし・t1)
await apply('OV3', [v2('2026-06-25', 'LK', { units_ordered: 1, sales_principal_jpy: 400 })], 'amazon_finance_v3');   // 今の形のより新しい版 (4 列あり) = 旧い形ではない (0050 の文字の比較の前置きの確かめ)
await apply('OH', [v2('2026-06-12', 'LH', { refund_principal_jpy: -500, refund_principal_customer_jpy: -500 })]);   // 6 月に売上が無い = 単価が無い
await apply('OM', [v2('2026-06-16', 'LM', { units_ordered: 1, sales_principal_jpy: 500 })]);   // 出品を作る前 = 受け取ったときは未解決
await apply('ON', [v2('2026-06-16', 'LN', { units_ordered: 1, sales_principal_jpy: 500 })]);   // 受け取ったときは LN1
await apply(pseudoOrderNo('2026-06-05'), [
  v2('2026-06-05', '-', { line_kind: 'storage', fba_storage_jpy: -300, account_fee_amount_jpy: -300 }),
  v2('2026-06-05', '-', { line_kind: 'subscription', other_amount_jpy: -4900, account_fee_amount_jpy: -4900 }),
  v2('2026-06-05', '-', { line_kind: 'not_account_fee', other_amount_jpy: -1000, account_fee_amount_jpy: -1000 }),
  v2('2026-06-05', '-', { line_kind: 'unknown', other_amount_jpy: -7 }),
]);
await apply(pseudoOrderNo('2026-06-14'), [v2('2026-06-14', '-', { line_kind: 'storage', fba_storage_jpy: -110, account_fee_amount_jpy: -110 })]);
await apply(pseudoOrderNo('2026-06-15'), [v2('2026-06-15', '-', { line_kind: 'storage', fba_storage_jpy: -100, account_fee_amount_jpy: -100, misc_fee_jpy: 3, ...C4(1, 3, 3) })]);   // 月の手数料の側の分けられない部品
await apply('OJ', [v2('2026-01-10', 'LA', { units_ordered: 1, sales_principal_jpy: 1000 }), v2('2026-01-10', 'LF', { units_ordered: 1, sales_principal_jpy: 400 })]);
const LM = await listing('LM'); await comp(LM, S1, 1);   // 受け取った後に作る = 今は解決
const LN2 = await listing('LN', 'other@X'); await comp(LN2, S1, 1);   // 受け取った後に 2 つめ = 今は未解決

// ─── 広告 (SP) ───
const adDay = (d, total, report = null) => pg.query(`insert into core.ad_spend_days (company_id, mall, scope_key, ad_type, date_jst, source_generation, source_report_id, checksum, row_count, cost_total, ingest_run_id)
  values (1, 'amazon', 'jp', 'SP', $1, 1, $2, $3, 0, $4, 'ad-run')`, [d, report || `r-${d}`, 'b'.repeat(64), total]);
// 受け口 (0035) と同じく、受け取ったときのマスタで sku の行だけ listing_id を入れる (保存済みの値 = 診断の「受け取り時の出品」)
const adRow = (d, camp, gran, code, c) => pg.query(`insert into core.ad_spend_daily (company_id, mall, scope_key, ad_type, date_jst, campaign_id, target_granularity, target_code, listing_id, clicks, impressions, ad_cost, ingest_run_id)
  values (1, 'amazon', 'jp', 'SP', $1, $2, $3, $4, case when $3 = 'sku' then core.resolve_listing_id(1::smallint, 'amazon', $4) end, 0, 0, $5, 'ad-run')`, [d, camp, gran, code, c]);
const AD_TOTAL = { '2026-06-05': '120.50', '2026-06-06': '15.00', '2026-06-07': '50.00', '2026-06-13': '39.00', '2026-06-16': '1.00' };
for (let dd = 1; dd <= 30; dd++) {
  const d = `2026-06-${String(dd).padStart(2, '0')}`;
  if (d === '2026-06-08' || d === '2026-06-09') continue;   // 親が無い = missing
  await adDay(d, AD_TOTAL[d] || '0', d === '2026-06-07' ? 'legacy:2026-06-07' : null);
}
await adRow('2026-06-05', 'c1', 'sku', 'LA', '100.00'); await adRow('2026-06-05', 'c2', 'sku', 'LA-ALIAS', '20.50');   // 別名も LA に
await adRow('2026-06-06', 'c3', 'asin', 'LA', '5.00'); await adRow('2026-06-06', 'c1', 'sku', 'LA', '10.00');           // 🚨 ASIN が seller SKU と同じ文字でも未解決
await adRow('2026-06-07', 'c1', 'sku', 'LA', '50.00');                                                                   // legacy の日
await adRow('2026-06-13', 'c1', 'sku', 'LF', '30.00'); await adRow('2026-06-13', 'c4', 'sku', 'UNKNOWN-SKU', '2.00'); await adRow('2026-06-13', 'c5', 'sku', 'LE', '7.00');
await adRow('2026-06-16', 'c1', 'sku', 'LC', '1.00');                                                                    // 売上の無い日の原価不明の出品 = 数 0 でも原価 null

// ─── 読み方 ───
// D7b-1b が差し替える形 (core.finance_coverage_state だけ) で coverage を与える
const setCoverage = (d, gen = null, rev = null) => pg.exec(`create or replace function core.finance_coverage_state(p_company_id smallint, p_mall text, p_scope_key text, p_source text)
  returns table (complete_to date, generation bigint, source_revision bigint) language plpgsql stable as $$
  begin return query select ${d ? `date '${d}'` : 'null::date'}, ${gen == null ? 'null::bigint' : `${gen}::bigint`}, ${rev == null ? 'null::bigint' : `${rev}::bigint`}; end $$;`);
const setAuditSince = (ts) => pg.exec(`create or replace function mart.amazon_profit_composition_audit_since() returns timestamptz language sql immutable as $$ select '${ts}'::timestamptz $$;`);
const rowsOf = async (from, to) => (await pg.query(`select r.*, r.economic_date_jst::text as d from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', $1::date, $2::date) r`, [from, to])).rows;
const totalsOf = async (from, to) => (await pg.query(`select t.*, t.period_from::text as pf, t.period_to::text as pt, t.economic_date_jst::text as ed, t.month_start::text as ms,
    t.before_ad_incomplete_days::text[] as bad, t.after_ad_incomplete_days::text[] as aad, t.after_account_fees_incomplete_days::text[] as fad
    from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', $1::date, $2::date) t`, [from, to])).rows;
const AUDIT_SINCE0 = new Date((await one(`select mart.amazon_profit_composition_audit_since() as s`)).s).toISOString();
const at = (rows, d, key) => rows.find((r) => r.d === d && (typeof key === 'number' ? N(r.listing_id) === key : r.seller_sku_norm === key));
const dayOf = (tot, d) => tot.find((x) => x.row_kind === 'day' && x.ed === d);
const ORDER = ['finance_incomplete', 'finance_unclassified', 'refund_units_unknown', 'refund_units_partial_month', 'listing_unresolved', 'composition_missing', 'cost_missing',
  'ad_not_collected', 'ad_missing', 'ad_legacy_unverified', 'ad_unresolved'];
const BEFORE_CODES = ORDER.slice(0, 7);

// ─── 0049 のままで控え (coverage の 3 つの状態 × 4 つの期間 × 行と日の合計) → 0050 を流す ───
const SNAP_COVERAGE = [[null], ['2026-06-30', 5, 42], ['2026-06-10']];
const SNAP_PERIODS = [['2026-06-01', '2026-06-30'], ['2026-01-01', '2026-01-31'], ['2026-06-13', '2026-06-14'], ['2026-05-20', '2026-07-10']];
const snapshot = async () => {
  const out = [];
  for (const cov of SNAP_COVERAGE) {
    await setCoverage(...cov);
    for (const [f, to] of SNAP_PERIODS) {
      for (const [kind, sql] of [['daily', `select * from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', $1::date, $2::date) order by 1, 2, 3, 4, 5, 6`],
        ['totals', `select * from mart.amazon_profit_day_totals_range(1::smallint, 'amazon', 'jp', $1::date, $2::date) order by 1, 2, 3, 4, 5, 6`]]) {
        const rows = (await pg.query(sql, [f, to])).rows.map(({ calculated_at, ...rest }) => JSON.stringify(rest));
        out.push({ label: `${kind} ${f}〜${to} coverage=${cov[0]}`, rows });
      }
    }
  }
  await setCoverage(null);
  return out;
};
const SNAP_0049 = await snapshot();
const applied50 = await applyMigrations(pgliteAdapter(pg), { log: quiet });
assert.ok(applied50.applied.includes('0050'), '0050 が流れていない');

console.log('0050 (速くした本体) = 0049');
await t('🚨 0050 の行の本体・日の合計は 0049 と結果が完全に同じ (coverage の 3 つの状態 × 4 つの期間 × 行と日の合計・calculated_at を除く全部の列・行の順も)', async () => {
  const now = await snapshot();
  assert.equal(now.length, SNAP_0049.length);
  let rows = 0;
  for (let i = 0; i < now.length; i++) {
    assert.equal(now[i].rows.length, SNAP_0049[i].rows.length, `${now[i].label} の行の数`);
    for (let j = 0; j < now[i].rows.length; j++) assert.equal(now[i].rows[j], SNAP_0049[i].rows[j], `${now[i].label} の ${j} 行目`);
    rows += now[i].rows.length;
  }
  assert.ok(rows > 500, String(rows));
  // 公開の行の関数の順は、並べ替えを書かなくても 0049 と同じ (0050: 行の本体は並べ替えず、公開の関数だけが order by・#1562 Codex R1 Medium 2)
  await setCoverage('2026-06-30', 5, 42);
  const plain = (await pg.query(`select * from mart.amazon_profit_daily_range(1::smallint, 'amazon', 'jp', '2026-06-01', '2026-06-30')`)).rows.map(({ calculated_at, ...rest }) => JSON.stringify(rest));
  const snap = SNAP_0049.find((x) => x.label === 'daily 2026-06-01〜2026-06-30 coverage=2026-06-30').rows;
  assert.deepEqual(plain, snap);
  await setCoverage(null);
  // 0050 の本体はこの関数の中だけ work_mem と nested loop の設定を持つ (幅の広い行がディスクに溢れない・見込み違いの nested loop を選ばない)
  const cfg = (await one(`select array_to_string(p.proconfig, ',') as c from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'mart' and p.proname = '_amazon_profit_rows'`)).c;
  // 32MB (#1562 Codex R1 Medium 1: 64MB は節ごと = 2 本重なると Render の 1GB に危ない)・generic plan に固定 (PostgreSQL 18 の sql の関数の custom plan は 3 倍遅い)
  assert.equal(String(cfg), 'work_mem=32MB,enable_nestloop=off,plan_cache_mode=force_generic_plan');
});

console.log('coverage (決済のそろい) の差し込み口');
await t('🚨 core.finance_coverage_state は今は 1 行・全部 null = 全部の日が provisional / missing・正式な値と世代・版は全部 null・0 と仮定の値は出る', async () => {
  const st = (await pg.query(`select complete_to, generation, source_revision from core.finance_coverage_state(1::smallint, 'amazon', 'jp', $1)`, [U])).rows;
  assert.deepEqual(st, [{ complete_to: null, generation: null, source_revision: null }]);
  const rows = await rowsOf('2026-06-01', '2026-06-30');
  assert.ok(rows.every((r) => r.finance_coverage_generation == null && r.finance_source_revision == null));
  assert.ok(rows.length >= 20, String(rows.length));
  assert.deepEqual([at(rows, '2026-06-05', LA).day_finance_status, at(rows, '2026-06-07', LA).day_finance_status], ['provisional', 'missing']);   // 6/7 = 広告だけの日 (財務の行なし)
  for (const r of rows) {
    assert.ok(['provisional', 'missing'].includes(r.day_finance_status), `${r.d} ${r.listing_id} ${r.day_finance_status}`);
    assert.equal(r.profit_incomplete_reasons[0], 'finance_incomplete');
    for (const c of ['contribution_before_ad_incl_jpy', 'contribution_before_ad_excl', 'contribution_after_ad_incl', 'contribution_after_ad_excl']) assert.equal(r[c], null, `${r.d} ${c}`);
    assert.notEqual(r.contribution_after_ad_assuming_incomplete_zero_incl, null);
    assert.equal(r.assumed_zero_reasons[0], 'finance_incomplete');
  }
  const la5 = at(rows, '2026-06-05', LA);
  assert.deepEqual([la5.contribution_after_ad_assuming_incomplete_zero_incl, la5.contribution_after_ad_assuming_incomplete_zero_excl], ['37.45', '104.95']);   // 0 と仮定の値は同じ式
  const tot = await totalsOf('2026-06-01', '2026-06-30');
  assert.equal(dayOf(tot, '2026-06-09').day_finance_status, 'missing');   // 行の無い日
  for (const x of tot) for (const c of ['contribution_before_ad_incl_jpy', 'contribution_before_ad_excl', 'contribution_after_ad_incl', 'contribution_after_ad_excl', 'profit_after_account_fees_incl', 'profit_after_account_fees_excl']) assert.equal(x[c], null, `${x.row_kind} ${x.pf} ${c}`);
  const all = tot.find((x) => x.row_kind === 'calendar_month');
  assert.equal(all.complete_days, 0); assert.equal(all.before_ad_incomplete_day_count, 30);
  assert.equal(all.profit_incomplete_reasons[0], 'finance_incomplete');
});

await setCoverage('2026-06-30', 5, 42);
let rows = await rowsOf('2026-06-01', '2026-06-30');

console.log('手で計算した行 (coverage を 2026-06-30 に差し替えた = D7b-1b の後の形)');
await t('🚨 税込 / 税抜 / 値引きの税 / 広告 × 1.1: 6/5 LA = 本体 2,000・手数料 300・FBA 200・値引き 110 (税 10)・ポイント 20・原価 300 × 2 × 2 個・広告 120.50 (別名の SKU 20.50 を含む)', async () => {
  const r = at(rows, '2026-06-05', LA);
  assert.equal(r.day_finance_status, 'complete');
  assert.deepEqual([r.listing_resolution, r.seller_sku_norm, r.listing_code, r.master_basis], ['resolved', null, 'LA', 'current']);
  assert.deepEqual([r.units_ordered, r.units_net_sold, N(r.profit_before_cogs_jpy), N(r.taxable_sku_fee_cost_jpy), N(r.promotion_tax_jpy), N(r.sales_tax_jpy)], [2, 2, 2000 - 300 - 200 - 110 - 20, 500, 10, 200]);
  assert.deepEqual([N(r.component_unit_cost_jpy), N(r.cogs_jpy), r.cost_basis], [600, 1200, 'sku_costs']);
  assert.equal(N(r.contribution_before_ad_incl_jpy), 170);
  assert.equal(r.contribution_before_ad_excl, '225.45');   // 170 + 500 / 11 + 10 = 225.4545…
  assert.deepEqual([r.ad_status, r.ad_cost, r.ad_rows], ['complete', '120.50', 2]);
  assert.equal(r.contribution_after_ad_incl, '37.45');     // 170 − 120.50 × 1.1
  assert.equal(r.contribution_after_ad_excl, '104.95');    // 225.4545… − 120.50 (途中で丸めない)
  assert.deepEqual([r.profit_incomplete_reasons, r.assumed_zero_reasons], [[], []]);
  assert.equal(N(r.easy_ship_alloc_jpy), 330);   // 内訳 (寄与から引かない)
  assert.deepEqual(r.cost_sku_cost_ids.map(Number), [C_S1]);
  assert.deepEqual([N(r.observed_generation), N(r.finance_coverage_generation), N(r.finance_source_revision), r.calculation_version], [7, 5, 42, 'amazon_profit_v1']);   // 世代と版は coverage の関数から
  assert.deepEqual([r.units_marketplace_guarantee, String(r.units_refunded_customer_unrounded), String(r.units_a_to_z_refund_unrounded)], [0, '0', '0']);   // 子の列もまとめて出す (Codex R1 Low)
  assert.deepEqual([r.received_listing_ids.map(Number), r.ad_received_listing_ids.map(Number), r.ad_received_unresolved_rows, r.master_notes.includes('listing_changed_since_received')], [[LA], [LA], 0, false]);
});
await t('🚨 返品 (推定の数) と負の手数料: 6/6 LA = 返金 1,000 (単価 1,000 → 1 個)・手数料の戻り 150 → 原価も −1 個分・税抜は −150 / 11', async () => {
  const r = at(rows, '2026-06-06', LA);
  assert.deepEqual([r.units_ordered, r.units_refunded_customer, r.units_net_sold, N(r.commission_jpy), N(r.refund_principal_jpy), r.refund_units_status], [0, 1, -1, -150, 1000, 'estimated_monthly_unit_price']);
  assert.deepEqual([N(r.profit_before_cogs_jpy), N(r.cogs_jpy), N(r.contribution_before_ad_incl_jpy), r.contribution_before_ad_excl], [-850, -600, -250, '-263.64']);
  // 6/6 は ASIN の広告 (5 円) = 未解決 → 出品の広告の後は null。結びついた広告 (10 円) だけ引いた値は 0 と仮定の列
  assert.deepEqual([r.contribution_after_ad_incl, r.contribution_after_ad_excl, r.ad_cost], [null, null, '10.00']);
  assert.deepEqual([r.profit_incomplete_reasons, r.assumed_zero_reasons], [['ad_unresolved'], ['ad_unresolved']]);
  assert.deepEqual([r.contribution_after_ad_assuming_incomplete_zero_incl, r.contribution_after_ad_assuming_incomplete_zero_excl], ['-261.00', '-273.64']);
  assert.deepEqual([String(r.units_refunded_customer_unrounded), String(r.units_a_to_z_refund_unrounded)], ['1.000000', '0']);   // 丸める前の返品数 (子の列)
});
await t('🚨 原価 0 円 (override_zero) は正しい 0 / 原価不明 (PARTIAL) は null + cost_missing + missing_cost_sku_ids・0 と仮定では 0', async () => {
  const b = at(rows, '2026-06-05', LB);
  assert.deepEqual([N(b.cogs_jpy), N(b.component_unit_cost_jpy), N(b.contribution_before_ad_incl_jpy), b.contribution_before_ad_excl, b.contribution_after_ad_incl, b.cost_basis], [0, 0, 850, '863.64', '850.00', 'sku_costs']);
  assert.deepEqual(b.cost_sku_cost_ids.map(Number), [C_S2]);
  const c = at(rows, '2026-06-05', LC);
  assert.deepEqual([c.cogs_jpy, c.component_unit_cost_jpy, c.contribution_before_ad_incl_jpy, c.cost_basis, c.cost_input_hash], [null, null, null, 'missing', null]);
  assert.deepEqual(c.missing_cost_sku_ids.map(Number), [S3]);
  assert.deepEqual([c.profit_incomplete_reasons, c.assumed_zero_reasons], [['cost_missing'], ['cost_missing']]);
  assert.deepEqual([N(c.contribution_before_ad_assuming_incomplete_zero_incl_jpy), c.contribution_after_ad_assuming_incomplete_zero_incl], [500, '500.00']);
  assert.notEqual(c.composition_hash, null);   // 構成はある
  // 売上の無い日 (6/16 の広告だけ) でも原価不明なら 0 × 不明 = null (fail-closed)
  const c16 = at(rows, '2026-06-16', LC);
  assert.deepEqual([c16.units_net_sold, c16.cogs_jpy, c16.contribution_before_ad_incl_jpy, c16.profit_incomplete_reasons], [0, null, null, ['cost_missing']]);
});
await t('🚨 構成 0 件 = composition_missing (原価 0 にしない)・候補 2 件 (shop 違い) と出品なし = 未解決の行 (listing_id null・正規化 SKU)', async () => {
  const d = at(rows, '2026-06-05', LD);
  assert.deepEqual([d.composition_basis, d.cost_basis, d.cogs_jpy, d.composition_hash, d.contribution_before_ad_incl_jpy, d.profit_incomplete_reasons], ['missing', 'missing', null, null, null, ['composition_missing']]);
  assert.equal(N(d.contribution_before_ad_assuming_incomplete_zero_incl_jpy), 700);
  const e = at(rows, '2026-06-05', 'le');
  assert.deepEqual([e.listing_id, e.listing_resolution, e.composition_basis, e.cost_basis, e.profit_incomplete_reasons, e.master_notes], [null, 'unresolved', 'listing_unresolved', 'missing', ['listing_unresolved'], []]);
  assert.ok(!rows.some((r) => [LE1, LE2].includes(N(r.listing_id))), '候補 2 件なのにどちらかに結んだ');
  const z = at(rows, '2026-06-05', 'zz-none');
  assert.deepEqual([z.listing_id, z.profit_incomplete_reasons, N(z.contribution_before_ad_assuming_incomplete_zero_incl_jpy)], [null, ['listing_unresolved'], 600]);
});
await t('🚨 広告の状態: legacy の親 (子の行だけ・広告の後 null・0 と仮定は記録済みの額) / 親が無い = missing (費用 null) / 2026-02-05 より前 = not_collected', async () => {
  const l7 = at(rows, '2026-06-07', LA);   // 財務の無い日の広告だけの行
  assert.deepEqual([l7.ad_status, l7.ad_cost, N(l7.contribution_before_ad_incl_jpy), l7.contribution_after_ad_incl, l7.profit_incomplete_reasons, l7.contribution_after_ad_assuming_incomplete_zero_incl],
    ['legacy_incomplete', '50.00', 0, null, ['ad_legacy_unverified'], '-55.00']);
  const l8 = at(rows, '2026-06-08', LA);
  assert.deepEqual([l8.ad_status, l8.ad_cost, N(l8.contribution_before_ad_incl_jpy), l8.contribution_after_ad_incl, l8.profit_incomplete_reasons, l8.contribution_after_ad_assuming_incomplete_zero_incl],
    ['missing', null, 400, null, ['ad_missing'], '400.00']);
  const jan = await rowsOf('2026-01-01', '2026-01-31');
  const j = at(jan, '2026-01-10', LA);
  assert.deepEqual([j.ad_status, j.ad_cost, N(j.contribution_before_ad_incl_jpy), j.contribution_after_ad_incl, j.profit_incomplete_reasons], ['not_collected', null, 400, null, ['ad_not_collected']]);
  const jf = at(jan, '2026-01-10', LF);   // 観測の原価の推定 (5/5 より前)
  assert.deepEqual([jf.cost_basis, N(jf.cogs_jpy), jf.cost_observed_ids.map(Number), jf.cost_sku_cost_ids.map(Number)], ['estimated', 500, [O_EST], [C_S1]]);
});
await t('🚨 分けられない部品 (+100 / −100 = 金額 0 でも数) と unmapped = finance_unclassified / 旧い形の版の行 (数を持たない) も fail-closed / 単価の無い返品 = refund_units_unknown', async () => {
  const b10 = at(rows, '2026-06-10', LB);
  assert.deepEqual([b10.unclassified_component_count, N(b10.unclassified_mapped_jpy), N(b10.unclassified_abs_jpy), b10.unmapped_component_count, N(b10.unmapped_jpy)], [2, 0, 200, 1, -3]);
  assert.deepEqual([b10.contribution_before_ad_incl_jpy, b10.profit_incomplete_reasons, N(b10.contribution_before_ad_assuming_incomplete_zero_incl_jpy)], [null, ['finance_unclassified'], 1000]);
  const g = at(rows, '2026-06-11', LG);
  assert.deepEqual([g.finance_legacy_rows, g.unclassified_component_count, g.contribution_before_ad_incl_jpy, g.profit_incomplete_reasons], [1, 0, null, ['finance_unclassified']]);
  const v3 = at(rows, '2026-06-25', LK);   // 今の形のより新しい版 (amazon_finance_v3) は旧い形ではない
  assert.deepEqual([v3.finance_legacy_rows, v3.profit_incomplete_reasons, N(v3.contribution_before_ad_incl_jpy)], [0, [], 400 - 120]);
  const h = at(rows, '2026-06-12', LH);
  assert.deepEqual([h.refund_units_status, h.refund_incomplete_child_count, N(h.refund_unestimated_jpy), h.units_refunded_customer, h.contribution_before_ad_incl_jpy, h.profit_incomplete_reasons, h.assumed_zero_reasons],
    ['unit_price_missing', 1, 500, 0, null, ['refund_units_unknown'], ['refund_units_unknown']]);
  assert.equal(h.units_refunded_customer_unrounded, null);   // 単価が無い = 丸める前の数も分からない
});
await t('原価の選び方: 同じ日 (6/15) に 2 回変わった取込の行 = valid_from → created_at の遅い方 1 行だけ (二重に数えない)・前の日は前の行 / 観測の原価 (sku_costs の無い SKU) は observed', async () => {
  const k14 = at(rows, '2026-06-14', LK), k15 = at(rows, '2026-06-15', LK);
  assert.deepEqual([N(k14.cogs_jpy), k14.cost_sku_cost_ids.map(Number), N(k14.contribution_before_ad_incl_jpy)], [100, [C_X], 800]);
  assert.deepEqual([N(k15.cogs_jpy), k15.cost_sku_cost_ids.map(Number), N(k15.contribution_before_ad_incl_jpy)], [120, [C_Z], 780]);
  assert.ok(!k15.cost_sku_cost_ids.map(Number).includes(C_Y));
  const f12 = at(rows, '2026-06-12', LF);
  assert.deepEqual([f12.cost_basis, N(f12.component_unit_cost_jpy), N(f12.contribution_before_ad_incl_jpy), f12.cost_observed_ids.map(Number)], ['observed', 500, -100, [O_OBS]]);
});
await t('🚨 hash = 正規の JSON の SHA-256 (JS の canonicalSha256 と同じ・ID は 10 進の文字列)', async () => {
  const la = at(rows, '2026-06-05', LA);
  assert.equal(la.composition_hash, canonicalSha256({ listing_id: String(LA), components: [{ sku_id: String(S1), qty: 2 }] }));
  assert.equal(la.cost_input_hash, canonicalSha256([{ sku_id: String(S1), source: 'sku_costs', row_id: String(C_S1), cost_jpy: 300, cost_status: 'COMPLETE' }]));
  const f = at(rows, '2026-06-12', LF);
  assert.equal(f.composition_hash, canonicalSha256({ listing_id: String(LF), components: [{ sku_id: String(S1), qty: 1 }, { sku_id: String(S4), qty: 1 }] }));
  assert.equal(f.cost_input_hash, canonicalSha256([{ sku_id: String(S1), source: 'sku_costs', row_id: String(C_S1), cost_jpy: 300, cost_status: 'COMPLETE' },
    { sku_id: String(S4), source: 'observed', row_id: String(O_OBS), cost_jpy: 200, cost_status: 'COMPLETE' }]));
  const b = at(rows, '2026-06-05', LB);
  assert.equal(b.cost_input_hash, canonicalSha256([{ sku_id: String(S2), source: 'sku_costs', row_id: String(C_S2), cost_jpy: 0, cost_status: 'OVERRIDDEN' }]));
  assert.equal(at(rows, '2026-06-06', LA).cost_input_hash, la.cost_input_hash);   // 同じ原価の行なら同じ hash
  assert.notEqual(at(rows, '2026-06-15', LK).cost_input_hash, at(rows, '2026-06-14', LK).cost_input_hash);   // 採った原価の行が変われば変わる
});

console.log('広告の結び直し (今のマスタ)');
await t('🚨 ASIN の広告は出品のコードと同じ文字でも未解決 / SKU の別名 (external_ids) は結ぶ / 未解決の広告の行は行を作らない / 未解決が 1 つでもある日は全部の出品の広告の後が null', async () => {
  assert.ok(!rows.some((r) => r.seller_sku_norm === 'unknown-sku'), '未解決の広告の行が行を作った');
  for (const r of rows.filter((x) => x.d === '2026-06-13')) {
    assert.equal(r.contribution_after_ad_incl, null, `${r.listing_code}`);
    assert.ok(r.profit_incomplete_reasons.includes('ad_unresolved'));
  }
  const f13 = at(rows, '2026-06-13', LF);
  assert.deepEqual([f13.ad_cost, f13.contribution_after_ad_assuming_incomplete_zero_incl], ['30.00', String((-100 - 33).toFixed(2))]);   // 結びついた 30 円だけ引く
  assert.ok(!rows.some((r) => r.d === '2026-06-13' && r.seller_sku_norm === 'le'), 'LE の広告 (候補 2 件) が行を作った');
  // 🚨 保存済みの listing_id は結び直しに使わない = 診断だけ (#1559 Codex R1 Medium 2): 受け取ったときは LB・今は LA → 広告費は LA・印が付く
  await pg.query(`update core.ad_spend_daily set listing_id = $1 where target_code = 'LA-ALIAS'`, [LB]);
  let again = at(await rowsOf('2026-06-05', '2026-06-05'), '2026-06-05', LA);
  assert.deepEqual([again.ad_cost, again.ad_received_listing_ids.map(Number), again.received_listing_ids.map(Number), again.master_notes.includes('listing_changed_since_received'), N(again.contribution_before_ad_incl_jpy)],
    ['120.50', [LA, LB], [LA], true, 170]);   // 印は値を止めない
  // 受け取ったとき未解決 (保存済みが null) の sku の広告も印
  await pg.query(`update core.ad_spend_daily set listing_id = null where target_code = 'LA-ALIAS'`);
  again = at(await rowsOf('2026-06-05', '2026-06-05'), '2026-06-05', LA);
  assert.deepEqual([again.ad_received_listing_ids.map(Number), again.ad_received_unresolved_rows, again.master_notes.includes('listing_changed_since_received')], [[LA], 1, true]);
  await pg.query(`update core.ad_spend_daily set listing_id = $1 where target_code = 'LA-ALIAS'`, [LA]);   // 元に戻す (受け取りも LA)
  assert.equal(at(await rowsOf('2026-06-05', '2026-06-05'), '2026-06-05', LA).master_notes.includes('listing_changed_since_received'), false);
});
await t('🚨 財務の受け取り時の出品も集合で比べる: 同じ日・同じ SKU に「今の出品」と「別の出品」で受け取った行がある = 印 (今の ID を含んでいても)', async () => {
  await apply('O15', [v2('2026-06-08', 'LA', { units_ordered: 0 })]);   // 金額 0 の行 (値は変わらない)
  await pg.query(`update core.order_finance_daily set listing_id = $1 where mall_order_no = 'O15'`, [LB]);   // 受け取ったときは LB だった
  const r = at(await rowsOf('2026-06-08', '2026-06-08'), '2026-06-08', LA);
  assert.deepEqual([r.received_listing_ids.map(Number), r.master_notes.includes('listing_changed_since_received'), N(r.profit_before_cogs_jpy)], [[LA, LB], true, 1000]);
  await apply('O15', []);   // 墓石 (行を消す)
  assert.equal(at(await rowsOf('2026-06-08', '2026-06-08'), '2026-06-08', LA).master_notes.includes('listing_changed_since_received'), false);
});

console.log('Easy Ship の割り振り (D-59)');
await t('🚨 本体売上の割合 (1,000 : 400 = 71 / 29・端数は小数部の大きい方)・合計 0 は等分 (34 / 33 / 33・同じなら正規化 SKU の順)・返金が多い = 負の額・料金の日に売上が無くても行ができる', async () => {
  const es = (d, key) => N((at(rows, d, key) || {}).easy_ship_alloc_jpy);
  assert.deepEqual([es('2026-06-13', LA), es('2026-06-13', LF)], [71, 29]);
  assert.deepEqual([es('2026-06-13', LP), es('2026-06-13', LQ), es('2026-06-13', LR)], [34, 33, 33]);
  assert.equal(es('2026-06-14', LF), 80);   // 売上は 6/12・料金は 6/14
  assert.equal(es('2026-06-21', LA), -30);
  const f14 = at(rows, '2026-06-14', LF);
  assert.deepEqual([f14.units_ordered, N(f14.contribution_before_ad_incl_jpy), f14.profit_incomplete_reasons], [0, 0, []]);   // 寄与から引かない
});
await t('🚨 本体売上が負の SKU (#1559 Codex R3): 重み = max(本体, 0) = −18 / −18 / 1,036 に 100 → 0 / 0 / 100・全部が負は等分 (11 → 6 / 5)・どの注文も配った合計 = 元の額 (保存則)', async () => {
  const es = (d, key) => N((at(rows, d, key) || {}).easy_ship_alloc_jpy);
  assert.deepEqual([es('2026-06-22', LP), es('2026-06-22', LQ), es('2026-06-22', LR)], [0, 0, 100]);
  assert.deepEqual([es('2026-06-23', LP), es('2026-06-23', LQ)], [6, 5]);   // 等分 5.5 / 5.5 → 端数 1 円は同じ小数部 = 正規化 SKU の順 (lp)
  const t2 = await totalsOf('2026-06-22', '2026-06-23');
  for (const x of t2) assert.equal(N(x.easy_ship_alloc_jpy) + N(x.easy_ship_unallocated_jpy), N(x.account_fee_easy_ship_cost_jpy), `${x.row_kind} ${x.pf}`);
  assert.deepEqual([N(dayOf(t2, '2026-06-22').easy_ship_alloc_jpy), N(dayOf(t2, '2026-06-23').easy_ship_alloc_jpy)], [100, 11]);
  // 1 日ずつの合計も元の額 (日の合計の配った額 = 月の手数料の Easy Ship・配れない額なし)
  for (const x of (await totalsOf('2026-06-01', '2026-06-30')).filter((y) => y.row_kind === 'day')) assert.equal(N(x.easy_ship_alloc_jpy) + N(x.easy_ship_unallocated_jpy), N(x.account_fee_easy_ship_cost_jpy), x.ed);
});
await t('🚨 割り振りは期間に依らない (1 日・1 か月・1 年で同じ)・期間の外の日の売上で割り振る', async () => {
  const pick = (rs) => rs.filter((r) => ['2026-06-13', '2026-06-14', '2026-06-21'].includes(r.d)).map((r) => [r.d, N(r.listing_id), N(r.easy_ship_alloc_jpy)]).sort((a, b) => (a[0] + a[1]).localeCompare(b[0] + b[1]));
  const month = pick(rows);
  const year = pick(await rowsOf('2026-01-01', '2026-12-31'));
  const days = pick([...(await rowsOf('2026-06-13', '2026-06-13')), ...(await rowsOf('2026-06-14', '2026-06-14')), ...(await rowsOf('2026-06-21', '2026-06-21'))]);
  assert.deepEqual(year, month); assert.deepEqual(days, month);
  assert.ok(month.length >= 7);
});

console.log('master_notes (情報の印・値を止めない)');
await t('既定 (composition_audit_since = 0049 の適用の時刻): 過去の日は pre_audit_unverifiable と current_after_recorded_change の両方 (別々に判定)・値は出る', async () => {
  const since = new Date((await one(`select mart.amazon_profit_composition_audit_since() as s`)).s);
  assert.ok(Math.abs(Date.now() - since.getTime()) < 3600e3, String(since));
  const r = at(rows, '2026-06-05', LA);
  assert.deepEqual([r.composition_basis, r.master_notes, N(r.contribution_before_ad_incl_jpy)], ['pre_audit_unverifiable', ['pre_audit_unverifiable', 'current_after_recorded_change'], 170]);
  assert.notEqual(r.composition_audit_through, null);
});
await t('🚨 listing_changed_since_received: 受け取った後に出品ができた (今は解決) / 受け取った後に候補が 2 つになった (今は未解決・受け取ったのは LN1)', async () => {
  const m = at(rows, '2026-06-16', LM);
  assert.deepEqual([m.received_listing_ids.map(Number), m.received_listing_unresolved_count, m.master_notes.includes('listing_changed_since_received'), N(m.contribution_before_ad_incl_jpy)], [[], 1, true, 200]);
  const n = at(rows, '2026-06-16', 'ln');
  assert.deepEqual([n.listing_id, n.received_listing_ids.map(Number), n.master_notes, n.profit_incomplete_reasons], [null, [LN1], ['listing_changed_since_received'], ['listing_unresolved']]);
  assert.deepEqual(at(rows, '2026-06-05', LA).received_listing_ids.map(Number), [LA]);
});
await t('監査の始まりを前にずらす → current_after_recorded_change / 監査の記録を前日より前に → current_no_recorded_change (タイトルの変更は数えない・構成の変更は数える)', async () => {
  await setAuditSince('2026-01-01T00:00:00Z');
  let r = at(await rowsOf('2026-06-05', '2026-06-05'), '2026-06-05', LB);
  assert.deepEqual([r.composition_basis, r.master_notes], ['current_after_recorded_change', ['current_after_recorded_change']]);
  // 監査の記録を 2026-01-02 に動かす (保守の経路 = append-only の trigger を一時的に外す)
  await pg.exec(`alter table events.master_change_events disable trigger trg_append_only_row;
    update events.master_change_events set recorded_at = '2026-01-02T00:00:00Z' where entity_type in ('listing', 'listing_component');
    alter table events.master_change_events enable trigger trg_append_only_row;`);
  r = at(await rowsOf('2026-06-05', '2026-06-05'), '2026-06-05', LB);
  assert.deepEqual([r.composition_basis, r.master_notes, new Date(r.composition_audit_through).toISOString()], ['current_no_recorded_change', [], '2026-01-02T00:00:00.000Z']);
  await pg.query(`update core.listings set title = 'タイトルだけ' where listing_id = $1`, [LB]);   // 識別ではない列 = 数えない
  assert.equal(at(await rowsOf('2026-06-05', '2026-06-05'), '2026-06-05', LB).composition_basis, 'current_no_recorded_change');
  await pg.query(`update core.listing_components set qty = 2 where listing_id = $1`, [LB]);   // 構成の変更 (原価 0 なので値は同じ)
  r = at(await rowsOf('2026-06-05', '2026-06-05'), '2026-06-05', LB);
  assert.deepEqual([r.composition_basis, r.master_notes, N(r.contribution_before_ad_incl_jpy)], ['current_after_recorded_change', ['current_after_recorded_change'], 850]);
  await pg.query(`update core.listing_components set qty = 1 where listing_id = $1`, [LB]);
  await setAuditSince(AUDIT_SINCE0);   // 元の値 (0049 の適用の時刻) に戻す
});

console.log('列ごとの null と理由のコード (§3.7 の表)');
await t('🚨 どの行も: 理由は固定の順・寄与は前の 7 つのどれかで null・広告の後は 11 のどれかで null・0 と仮定の値は必ずある・assumed_zero_reasons = 理由 − partial', async () => {
  const check = (rs, label) => {
    for (const r of rs) {
      const reasons = r.profit_incomplete_reasons;
      const idx = reasons.map((c) => ORDER.indexOf(c));
      assert.ok(idx.every((i) => i >= 0) && idx.every((v, i) => i === 0 || idx[i - 1] < v), `${label} ${r.d} 順: ${reasons}`);
      const beforeNull = reasons.some((c) => BEFORE_CODES.includes(c));
      assert.equal(r.contribution_before_ad_incl_jpy === null, beforeNull, `${label} ${r.d} ${r.listing_code} before`);
      assert.equal(r.contribution_before_ad_excl === null, beforeNull);
      assert.equal(r.contribution_after_ad_incl === null, reasons.length > 0, `${label} ${r.d} ${r.listing_code} after`);
      assert.equal(r.contribution_after_ad_excl === null, reasons.length > 0);
      assert.notEqual(r.contribution_after_ad_assuming_incomplete_zero_incl, null);
      assert.deepEqual(r.assumed_zero_reasons, reasons.filter((c) => c !== 'refund_units_partial_month'));
      assert.equal(r.master_basis, 'current');
      assert.equal((r.listing_id == null) === (r.listing_resolution === 'unresolved'), true);
      assert.equal((r.seller_sku_norm == null) === (r.listing_resolution === 'resolved'), true);
    }
  };
  check(rows, 'coverage 6/30');
  await setCoverage('2026-06-10');   // 6/10 まで complete・その後は provisional。6 月の返品の推定は月末までそろっていない = partial
  const r10 = await rowsOf('2026-06-01', '2026-06-30');
  check(r10, 'coverage 6/10');
  const la6 = at(r10, '2026-06-06', LA);
  assert.deepEqual([la6.day_finance_status, la6.refund_units_status, la6.profit_incomplete_reasons, la6.assumed_zero_reasons], ['complete', 'estimated_partial_month_unit_price', ['refund_units_partial_month', 'ad_unresolved'], ['ad_unresolved']]);
  assert.equal(N(at(r10, '2026-06-05', LA).contribution_before_ad_incl_jpy), 170);   // 返品の無い行は出る
  assert.deepEqual(at(r10, '2026-06-13', LA).profit_incomplete_reasons, ['finance_incomplete', 'ad_unresolved']);
  await setCoverage(null);
  check(await rowsOf('2026-06-01', '2026-06-30'), 'coverage null');
  await setCoverage('2026-06-30', 5, 42);
});

console.log('日の合計 (§3.6)');
let tot = await totalsOf('2026-06-01', '2026-06-30');
await t('🚨 きれいな日 6/14 = 寄与 800・広告 0・月の手数料 (保管料 110 + Easy Ship 80) → 税込 610 / 税抜 800 − (100 + 72.73) = 627.27', async () => {
  const d = dayOf(tot, '2026-06-14');
  assert.deepEqual([d.day_finance_status, d.ad_status, d.ad_cost_total, N(d.contribution_before_ad_incl_jpy), d.contribution_before_ad_excl, d.contribution_after_ad_incl, d.contribution_after_ad_excl],
    ['complete', 'complete', '0.00', 800, '800.00', '800.00', '800.00']);
  assert.deepEqual([N(d.account_fee_cost_jpy), d.account_fee_cost_excl, N(d.account_fee_storage_cost_jpy), N(d.account_fee_easy_ship_cost_jpy), d.profit_after_account_fees_incl, d.profit_after_account_fees_excl],
    [190, '172.73', 110, 80, '610.00', '627.27']);
  assert.deepEqual([N(d.easy_ship_alloc_jpy), N(d.cogs_jpy), N(d.profit_before_cogs_jpy), d.resolved_rows, d.unresolved_rows], [80, 100, 900, 2, 0]);   // Easy Ship の割り振りは内訳 = 二重に引かない
  assert.deepEqual([d.bad, d.aad, d.fad, d.profit_incomplete_reasons], [[], [], [], []]);
});
await t('🚨 未解決の広告は合計を止めない (6/13): 出品の行は広告の後 null だが、日の合計は ad_spend_days の全額 39 円 × 1.1 を引いて正式・配れない額と行の数は内訳', async () => {
  const d = dayOf(tot, '2026-06-13');
  assert.deepEqual([N(d.contribution_before_ad_incl_jpy), d.contribution_after_ad_incl, d.contribution_after_ad_excl, d.ad_cost_total, d.ad_cost_allocated, d.ad_cost_unresolved, d.ad_unresolved_rows],
    [300, '257.10', '261.00', '39.00', '30.00', '9.00', 2]);
  assert.deepEqual([N(d.account_fee_cost_jpy), N(d.account_fee_easy_ship_cost_jpy), d.profit_after_account_fees_incl, d.profit_after_account_fees_excl], [250, 250, '7.10', '33.73']);
  assert.deepEqual([N(d.easy_ship_alloc_jpy), N(d.easy_ship_unallocated_jpy), d.easy_ship_unallocated_count], [200, 50, 1]);
  assert.ok(!d.profit_incomplete_reasons.includes('ad_unresolved'));
  assert.ok(!tot.find((x) => x.row_kind === 'calendar_month').aad.includes('2026-06-13'));
});
await t('🚨 列の組ごとの条件: 6/15 = 寄与と広告の後は正式・月の手数料の側に分けられない部品 → 手数料の後だけ null / 6/9 (取引なし) = 寄与 0・広告 missing → 広告の後 null / 6/5 = 行の不完全で寄与から null', async () => {
  const d15 = dayOf(tot, '2026-06-15');
  assert.deepEqual([N(d15.contribution_before_ad_incl_jpy), d15.contribution_after_ad_incl, d15.profit_after_account_fees_incl, d15.unclassified_component_count, d15.sku_unclassified_component_count, d15.profit_incomplete_reasons],
    [780, '780.00', null, 1, 0, ['finance_unclassified']]);
  assert.deepEqual([d15.bad, d15.aad, d15.fad], [[], [], ['2026-06-15']]);
  const d9 = dayOf(tot, '2026-06-09');
  assert.deepEqual([d9.day_finance_status, d9.resolved_rows, N(d9.contribution_before_ad_incl_jpy), d9.contribution_before_ad_excl, d9.contribution_after_ad_incl, d9.ad_status, d9.ad_cost_total, d9.profit_incomplete_reasons],
    ['complete', 0, 0, '0.00', null, 'missing', null, ['ad_missing']]);
  const d5 = dayOf(tot, '2026-06-05');
  assert.equal(d5.contribution_before_ad_incl_jpy, null);
  assert.deepEqual(d5.profit_incomplete_reasons, ['finance_unclassified', 'listing_unresolved', 'composition_missing', 'cost_missing']);   // unknown の行 = 手数料の側の finance_unclassified
  assert.deepEqual([d5.unknown_line_rows, N(d5.unknown_line_mapped_jpy), N(d5.not_account_fee_mapped_jpy), N(d5.account_fee_subscription_cost_jpy)], [1, -7, -1000, 4900]);
  const m = tot.find((x) => x.row_kind === 'calendar_month');
  assert.equal(m.contribution_before_ad_incl_jpy, null);
  assert.ok(m.bad.includes('2026-06-05') && !m.bad.includes('2026-06-14') && !m.bad.includes('2026-06-09'));
  assert.ok(m.aad.includes('2026-06-09') && m.aad.includes('2026-06-08') && m.aad.includes('2026-06-07'));
  assert.ok(m.fad.includes('2026-06-15'));
  assert.deepEqual([m.before_ad_incomplete_day_count, m.after_ad_incomplete_day_count, m.after_account_fees_incomplete_day_count], [m.bad.length, m.aad.length, m.fad.length]);
});
await t('期間の中の月の小計と合計 (6/13〜6/14) = 日の値の和 (途中で丸めない: 税抜の手数料の後 33.727… + 627.272… = 661.00)', async () => {
  const s = await totalsOf('2026-06-13', '2026-06-14');
  assert.deepEqual(s.map((x) => x.row_kind), ['day', 'day', 'range_month_subtotal', 'range_total']);
  for (const x of s.filter((y) => y.row_kind !== 'day')) {
    assert.deepEqual([N(x.contribution_before_ad_incl_jpy), x.contribution_after_ad_incl, x.contribution_after_ad_excl, x.profit_after_account_fees_incl, x.profit_after_account_fees_excl, x.day_count],
      [1100, '1057.10', '1061.00', '617.10', '661.00', 2]);
    assert.equal(x.day_finance_status, null);
  }
});
await t('🚨 row_kind は重ならない: 5/20〜7/10 = 日 52 行・5 月と 7 月は期間の中の小計・6 月は暦の月・合計 1 行 / 取引の無い日も日の行 / 行の関数は何も無い期間で 0 行', async () => {
  const s = await totalsOf('2026-05-20', '2026-07-10');
  const kinds = s.map((x) => `${x.row_kind}:${x.pf}:${x.pt}`).filter((k) => !k.startsWith('day'));
  assert.deepEqual(kinds, ['range_month_subtotal:2026-05-20:2026-05-31', 'calendar_month:2026-06-01:2026-06-30', 'range_month_subtotal:2026-07-01:2026-07-10', 'range_total:2026-05-20:2026-07-10']);
  const days = s.filter((x) => x.row_kind === 'day');
  assert.equal(days.length, 52);
  assert.ok(days.every((x) => x.pf === x.pt && x.pf === x.ed && x.day_count === 1));
  const months = s.filter((x) => x.row_kind === 'calendar_month' || x.row_kind === 'range_month_subtotal');
  assert.equal(months.reduce((a, x) => a + x.day_count, 0), 52);
  assert.deepEqual(months.map((x) => x.ms), ['2026-05-01', '2026-06-01', '2026-07-01']);
  assert.equal(s.find((x) => x.row_kind === 'range_total').day_count, 52);
  const may = dayOf(s, '2026-05-25');   // 取引の無い日 = complete_to (6/30) 以下なので complete (確定の 0)
  assert.deepEqual([may.day_finance_status, may.resolved_rows, N(may.contribution_before_ad_incl_jpy)], ['complete', 0, 0]);
  assert.equal(dayOf(s, '2026-07-05').day_finance_status, 'missing');   // complete_to より後・行なし
  assert.equal((await rowsOf('2026-03-01', '2026-03-31')).length, 0);
  assert.equal((await totalsOf('2026-03-01', '2026-03-31')).filter((x) => x.row_kind === 'day').length, 31);
});
await t('🚨 保存則 (税込・符号つき): net = 寄与の部品 (profit_before_cogs) + 消費税の預かり − 月の手数料 + (unknown + 分けられない + unmapped) + 損益の外 (日・月・合計)・net は決済の行の全部と一致', async () => {
  for (const x of tot) {
    const rhs = N(x.profit_before_cogs_jpy) + N(x.sales_tax_jpy) - N(x.account_fee_cost_jpy) + N(x.unknown_line_mapped_jpy) + N(x.unclassified_mapped_jpy) + N(x.unmapped_jpy) + N(x.not_account_fee_mapped_jpy);
    assert.equal(N(x.net_jpy), rhs, `${x.row_kind} ${x.pf}`);
  }
  const direct = N((await one(`select sum(net_jpy) as n from core.order_finance_daily where company_id = 1 and mall = 'amazon' and scope_key = 'jp' and economic_date_jst between '2026-06-01' and '2026-06-30'`)).n);
  assert.equal(N(tot.find((x) => x.row_kind === 'range_total').net_jpy), direct);
  const d5 = dayOf(tot, '2026-06-05');
  assert.equal(N(d5.unmapped_jpy) + N(d5.unclassified_mapped_jpy), 0);
  assert.equal(N(dayOf(tot, '2026-06-15').unclassified_mapped_jpy), 3);
});
await t('月の手数料の税の表 (試験で固定): 8 種類とも 10% (Amazon の決済の手数料は全部税込)・ほかの種類は表の外 (null)', async () => {
  const kinds = ['storage', 'long_term_storage', 'removal', 'inbound_defect', 'low_inventory', 'subscription', 'easy_ship', 'other_account_fee'];
  for (const k of kinds) assert.equal(String((await one(`select mart.amazon_account_fee_tax_rate($1) as r`, [k])).r), '0.10', k);
  for (const k of ['sku', 'not_account_fee', 'unknown', 'x']) assert.equal((await one(`select mart.amazon_account_fee_tax_rate($1) as r`, [k])).r, null, k);
  const src = fs.readFileSync(new URL('../db/company/migrations/0043_amazon_finance_v2.sql', import.meta.url), 'utf8');
  for (const k of kinds) assert.ok(src.includes(`'${k}'`), `0043 の line_kind に ${k} が無い`);
});
await t('master_note_counts (鍵は 3 つに固定・値は印を持つ行の数)・master_basis = current・calculated_at は 1 回の呼び出しで同じ', async () => {
  const m = tot.find((x) => x.row_kind === 'range_total');
  assert.deepEqual(Object.keys(m.master_note_counts).sort(), ['current_after_recorded_change', 'listing_changed_since_received', 'pre_audit_unverifiable']);
  assert.equal(m.master_note_counts.listing_changed_since_received, 2);   // LM・LN
  assert.equal(m.master_note_counts.pre_audit_unverifiable, rows.filter((r) => r.master_notes.includes('pre_audit_unverifiable')).length);
  assert.ok(tot.every((x) => x.master_basis === 'current'));
  assert.ok(tot.every((x) => N(x.finance_coverage_generation) === 5 && N(x.finance_source_revision) === 42), '合計の世代と版 (coverage の関数から)');
  assert.equal(new Set(tot.map((x) => new Date(x.calculated_at).getTime())).size, 1);
  assert.equal(new Set((await rowsOf('2026-06-01', '2026-06-30')).map((x) => new Date(x.calculated_at).getTime())).size, 1);
});

await t('🚨 わかる範囲の印の限界 (仕様・#1559 Codex R2 Medium 1): 受け取り時は未解決の広告 → 別名を足す → relink = 保存値が埋まる = 印は付かない・どの段でも正式な利益は止めない', async () => {
  await adRow('2026-06-20', 'c9', 'sku', 'LA-LATE', '1.00');   // 受け取り時は結びつかない (保存値 null)
  const r20 = async () => at(await rowsOf('2026-06-20', '2026-06-20'), '2026-06-20', LA);
  let r = await r20();   // 別名の前 = 今も未解決 = 行を作らない・その日の出品の広告の後は ad_unresolved
  assert.deepEqual([r.ad_cost, r.profit_incomplete_reasons, r.master_notes.includes('listing_changed_since_received')], ['0.00', ['ad_unresolved'], false]);
  const alias = N((await one(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'listing', $1, 'amazon', 'seller_sku', 'LA-LATE', 'manual', 'human') returning external_id_row`, [LA])).external_id_row);
  try {
    r = await r20();   // relink の前 = 保存値が null のまま = 受け取り時の未解決が見える
    assert.deepEqual([r.ad_cost, r.ad_received_unresolved_rows, r.master_notes.includes('listing_changed_since_received'), r.contribution_after_ad_incl], ['1.00', 1, true, '398.90']);
    assert.equal(N((await one(`select core.relink_ad_spend_listings(1::smallint) as n`)).n), 1);
    r = await r20();   // relink の後 = 保存値が LA に埋まる = 受け取り時の未解決は区別できない = 印は付かない (仕様)
    assert.deepEqual([r.ad_cost, r.ad_received_listing_ids.map(Number), r.ad_received_unresolved_rows, r.master_notes.includes('listing_changed_since_received'), r.contribution_after_ad_incl],
      ['1.00', [LA], 0, false, '398.90']);
  } finally {
    await pg.query(`delete from core.ad_spend_daily where campaign_id = 'c9'`);
    await pg.query(`delete from core.external_ids where external_id_row = $1`, [alias]);
  }
});
await t('合計の coverage の世代と版は (source・generation・source_revision) の組が 1 つのときだけ = source が違えば世代と版が同じ数でも null (#1559 Codex R2 Low 1)', async () => {
  const total = (s) => s.find((x) => x.row_kind === 'range_total');
  assert.deepEqual([N(total(await totalsOf('2026-06-10', '2026-06-20')).finance_coverage_generation)], [5]);
  await pg.query(`update core.finance_source_policy set period_to = '2026-06-16' where company_id = 1 and mall = 'amazon' and scope_key = 'jp'`);
  await pg.query(`insert into core.finance_source_policy (company_id, mall, scope_key, period_from, source) values (1, 'amazon', 'jp', '2026-06-16', 'amazon_settlement_flat_v2')`);
  try {
    const s = await totalsOf('2026-06-10', '2026-06-20');   // 6/16〜 は別の source (coverage の関数は同じ世代 5・版 42 を返す)
    assert.deepEqual([total(s).finance_coverage_generation, total(s).finance_source_revision], [null, null]);
    assert.deepEqual([N(dayOf(s, '2026-06-12').finance_coverage_generation), N(dayOf(s, '2026-06-18').finance_coverage_generation)], [5, 5]);   // 日の行はその日の source の値
    const one15 = total(await totalsOf('2026-06-10', '2026-06-15'));   // 1 つの source だけの期間
    assert.deepEqual([N(one15.finance_coverage_generation), N(one15.finance_source_revision)], [5, 42]);
  } finally {
    await pg.query(`delete from core.finance_source_policy where source = 'amazon_settlement_flat_v2' and company_id = 1 and mall = 'amazon' and scope_key = 'jp'`);
    await pg.query(`update core.finance_source_policy set period_to = null where company_id = 1 and mall = 'amazon' and scope_key = 'jp'`);
  }
  assert.equal(N(total(await totalsOf('2026-06-10', '2026-06-20')).finance_coverage_generation), 5);
});
await t('🚨 材料は 1 回だけ計算する (#1559 Codex R1 Medium 1): 日の合計の本体は Easy Ship の割り振り・広告・日の状態を 1 回ずつ呼んで行の本体に配列で渡す・行の本体は自分で呼ばない', async () => {
  const src = async (f) => (await one(`select p.prosrc as s from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'mart' and p.proname = $1`, [f])).s;
  const count = (s, name) => (s.match(new RegExp(`mart\\.${name}\\(`, 'g')) || []).length;
  const totalsSrc = await src('_amazon_profit_totals'), rowsSrc = await src('_amazon_profit_rows');
  for (const f of ['_amazon_easy_ship_alloc', '_amazon_profit_ad_children', '_amazon_profit_ad_days', '_amazon_profit_finance_days', '_amazon_profit_rows']) {
    assert.equal(count(totalsSrc, f), 1, `合計の本体で ${f} が 1 回でない`);
    assert.equal(count(rowsSrc, f), 0, `行の本体が ${f} を自分で呼んでいる`);
  }
  // 日の合計の Easy Ship の内訳は行と同じ材料 = 配った額 + 配れない額 = 月の手数料の Easy Ship
  const all = tot.find((x) => x.row_kind === 'range_total');
  assert.equal(N(all.easy_ship_alloc_jpy) + N(all.easy_ship_unallocated_jpy), N(all.account_fee_easy_ship_cost_jpy));
});

console.log('契約');
await t('from <= to・行は最大 400 日・日の合計は最大 93 日 (0050・両端を含む)・amazon / jp だけ・null は拒む (22023)', async () => {
  for (const fn of ['amazon_profit_daily_range', 'amazon_profit_day_totals_range']) {
    const q = (m, s, f, to) => pg.query(`select count(*) from mart.${fn}(1::smallint, $1, $2, $3::date, $4::date)`, [m, s, f, to]);
    await rejects(() => q('amazon', 'jp', '2026-06-02', '2026-06-01'), /invalid_input: from/);
    // 401 日: 行の関数 = 400 日の文 / 日の合計 = 93 日の文 (93 日の確かめを先に・#1562 Codex R1 Low 2)
    await rejects(() => q('amazon', 'jp', '2026-01-01', '2027-02-05'), fn === 'amazon_profit_daily_range' ? /400 日/ : /93 日まで.*長い期間は月ごとに呼ぶ/);
    if (fn === 'amazon_profit_daily_range') await q('amazon', 'jp', '2026-01-01', '2027-02-04');
    else {
      await q('amazon', 'jp', '2026-06-01', '2026-09-01');   // 93 日
      const e = await rejects(() => q('amazon', 'jp', '2026-06-01', '2026-09-02'), /93 日まで/);   // 94 日 (0049 は 400 日まで = 本番で 1〜9 月が 120 秒で打ち切り)
      assert.equal(e.code, '22023');
    }
    await rejects(() => q('rakuten', 'jp', '2026-06-01', '2026-06-01'), /amazon \/ jp だけ/);
    await rejects(() => q('amazon', 'us', '2026-06-01', '2026-06-01'), /amazon \/ jp だけ/);
    await rejects(() => q('amazon', 'jp', null, '2026-06-01'), /invalid_input/);
  }
});

// ─── 受け口 (本物の router を HTTP で) ───
console.log('受け口 (GET /amazon-profit/daily・/totals)');
process.env.MIRROR_SYNC_KEY = 'k';
process.env.COMPANY_DB_URL = 'pglite://test';
const factoryOf = (db) => async () => ({
  query: async (text, params) => {
    if (params && params.length) return db.query(text, params);
    if (text.includes(';')) { await db.exec(text); return { rows: [] }; }
    return db.query(text);
  },
  end: async () => {},
});
__setPgClientFactory(factoryOf(pg));
const app = express();
app.use('/apps/company-db/sync', requireSyncKey);
app.use('/apps/company-db/sync', companyDbRouter);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const BASE_URL = `http://127.0.0.1:${server.address().port}/apps/company-db/sync`;
const http = async (p, key = 'k') => {
  const res = await fetch(`${BASE_URL}${p}`, { headers: key == null ? {} : { 'x-sync-key': key } });
  const text = await res.text(); let json = null; try { json = JSON.parse(text); } catch { /* */ }
  return { status: res.status, json };
};
await t('鍵が無ければ 401 / 契約の外は 400 (from > to・401 日・実在しない日・amazon / jp 以外)', async () => {
  assert.equal((await http('/amazon-profit/daily?mall=amazon&scope=jp&from=2026-06-01&to=2026-06-01', null)).status, 401);
  for (const q of ['from=2026-06-02&to=2026-06-01', 'from=2026-01-01&to=2027-02-05', 'from=2026-02-30&to=2026-03-01', 'from=2026-06-01']) {
    assert.equal((await http(`/amazon-profit/daily?mall=amazon&scope=jp&${q}`)).status, 400, q);
    assert.equal((await http(`/amazon-profit/totals?mall=amazon&scope=jp&${q}`)).status, 400, q);
  }
  assert.equal((await http('/amazon-profit/daily?mall=rakuten&scope=jp&from=2026-06-01&to=2026-06-01')).status, 400);
  assert.equal((await http('/amazon-profit/daily?mall=amazon&scope=us&from=2026-06-01&to=2026-06-01')).status, 400);
  // /daily も /totals も 1 回 93 日まで (#1559 Codex R1 Medium 1・0050 で /totals も 400 → 93 日 = 本番で 1〜9 月が 120 秒で打ち切り)
  for (const kind of ['daily', 'totals']) {
    assert.equal((await http(`/amazon-profit/${kind}?mall=amazon&scope=jp&from=2026-06-01&to=2026-09-01`)).status, 200, `${kind} 93 日`);
    const d94 = await http(`/amazon-profit/${kind}?mall=amazon&scope=jp&from=2026-06-01&to=2026-09-02`);
    assert.deepEqual([d94.status, /93 days/.test(d94.json.error)], [400, true], `${kind} 94 日`);
  }
});
await t('🚨 /daily = 関数の行 (ID と ID の配列は 10 進の文字列・円は数・税抜などは小数 2 桁の文字列・日付は YYYY-MM-DD) / /totals = row_kind つき', async () => {
  const r = await http('/amazon-profit/daily?mall=amazon&scope=jp&from=2026-06-05&to=2026-06-06');
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.deepEqual([r.json.mall, r.json.scope, r.json.from, r.json.to, r.json.master_basis], ['amazon', 'jp', '2026-06-05', '2026-06-06', 'current']);
  const la = r.json.rows.find((x) => x.economic_date_jst === '2026-06-05' && x.listing_id === String(LA));
  assert.ok(la, JSON.stringify(r.json.rows.slice(0, 2)));
  assert.deepEqual([la.contribution_before_ad_incl_jpy, la.contribution_before_ad_excl, la.contribution_after_ad_incl, la.ad_cost, la.cogs_jpy, la.easy_ship_alloc_jpy],
    [170, '225.45', '37.45', '120.50', 1200, 330]);
  assert.deepEqual([la.received_listing_ids, la.ad_received_listing_ids, la.cost_sku_cost_ids, la.missing_cost_sku_ids, la.observed_generation, la.finance_coverage_generation, la.finance_source_revision, la.profit_incomplete_reasons, la.master_basis],
    [[String(LA)], [String(LA)], [String(C_S1)], [], '7', '5', '42', [], 'current']);
  assert.deepEqual([la.units_marketplace_guarantee, la.units_refunded_customer_unrounded], [0, '0']);
  // 形の説明 (README・router の注釈) = 実際の出力 (#1559 Codex R2 Low 2): 金額の numeric は小数 2 桁の文字列・units_*_unrounded は最大 6 桁の文字列
  const money = ['ad_cost', 'contribution_before_ad_excl', 'contribution_after_ad_incl', 'contribution_after_ad_excl', 'contribution_before_ad_assuming_incomplete_zero_excl',
    'contribution_after_ad_assuming_incomplete_zero_incl', 'contribution_after_ad_assuming_incomplete_zero_excl'];
  for (const x of r.json.rows) {
    for (const c of money) assert.ok(x[c] === null || /^-?\d+\.\d{2}$/.test(x[c]), `${c} = ${JSON.stringify(x[c])}`);
    for (const c of ['units_refunded_customer_unrounded', 'units_a_to_z_refund_unrounded']) assert.ok(x[c] === null || /^-?\d+(\.\d{1,6})?$/.test(x[c]), `${c} = ${JSON.stringify(x[c])}`);
  }
  assert.ok(r.json.rows.some((x) => x.units_refunded_customer_unrounded === '1.000000'));
  assert.match(la.calculated_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  const le = r.json.rows.find((x) => x.seller_sku_norm === 'le');
  assert.deepEqual([le.listing_id, le.listing_resolution], [null, 'unresolved']);
  const tt = await http('/amazon-profit/totals?mall=amazon&scope=jp&from=2026-06-13&to=2026-06-14');
  assert.equal(tt.status, 200, JSON.stringify(tt.json));
  assert.deepEqual(tt.json.rows.map((x) => x.row_kind), ['day', 'day', 'range_month_subtotal', 'range_total']);
  const tot2 = tt.json.rows.find((x) => x.row_kind === 'range_total');
  assert.deepEqual([tot2.period_from, tot2.period_to, tot2.contribution_before_ad_incl_jpy, tot2.profit_after_account_fees_excl, tot2.before_ad_incomplete_days, tot2.master_note_counts.listing_changed_since_received],
    ['2026-06-13', '2026-06-14', 1100, '661.00', [], 0]);
  assert.equal(tt.json.rows[0].economic_date_jst, '2026-06-13');
  const src = fs.readFileSync(new URL('../apps/company-db/router.mjs', import.meta.url), 'utf8');
  assert.match(src, /daily: \{ fn: 'mart\.amazon_profit_daily_range'/);
  assert.match(src, /totals: \{ fn: 'mart\.amazon_profit_day_totals_range'/);
  // 取引の中で advisory lock → 時間の上限 → 関数 (Company DB の読む口の流儀 + 同時に 1 本だけ)
  assert.match(src, /begin read only`\);[\s\S]{0,200}pg_try_advisory_xact_lock\(\$1::bigint\)[\s\S]{0,400}set local statement_timeout = '120s'`\);\s+rows = \(await client\.query\(`select \$\{sel\.list\} from \$\{spec\.fn\}\(1::smallint/);
  assert.match(src, /daily: \{ fn: 'mart\.amazon_profit_daily_range', maxDays: 93, order: '' \}/);   // /daily は関数の順のまま (幅の広い行をもう一度並べ替えない)
});
await t('🚨 読む口は同時に 1 本だけ (#1562 Codex R1 Medium 1): 1 本目が lock を持っている間の 2 本目 = 503 BUSY (retryable)・1 本目は最後まで返る・終われば次は通る', async () => {
  // PGlite は 1 つの接続 = 2 つの session の advisory lock を作れない → 接続の包みで lock を持つ / 持たないを作る (lock の SQL・取引の範囲は本物の router のまま)
  let held = null, release = null, sawLockSql = 0;
  const gate = new Promise((r) => { release = r; });
  let n = 0;
  __setPgClientFactory(async () => {
    const id = ++n;
    const base = await factoryOf(pg)();
    return {
      query: async (text, params) => {
        if (/pg_try_advisory_xact_lock/.test(text)) { sawLockSql++; if (held && held !== id) return { rows: [{ ok: false }] }; held = id; return { rows: [{ ok: true }] }; }
        if (/^(begin read only|commit|rollback)$/.test(text)) { if (text !== 'begin read only' && held === id) held = null; return { rows: [] }; }   // 共有の PGlite に取引を開かない
        if (/set local statement_timeout/.test(text)) return { rows: [] };
        if (id === 1 && /from mart\.amazon_profit_daily_range/.test(text)) await gate;   // 1 本目は lock を持ったまま待つ
        return base.query(text, params);
      },
      end: async () => {},
    };
  });
  try {
    const first = http('/amazon-profit/daily?mall=amazon&scope=jp&from=2026-06-05&to=2026-06-05');
    for (let i = 0; i < 200 && held !== 1; i++) await new Promise((r) => setTimeout(r, 10));
    assert.equal(held, 1, '1 本目が lock を持っていない');
    const second = await http('/amazon-profit/totals?mall=amazon&scope=jp&from=2026-06-05&to=2026-06-05');
    assert.deepEqual([second.status, second.json.code, second.json.retryable], [503, 'BUSY', true]);
    release();
    const r1 = await first;
    assert.equal(r1.status, 200); assert.ok(r1.json.rows.length > 0);
    assert.equal(held, null, 'commit で lock を放していない');
    assert.equal((await http('/amazon-profit/totals?mall=amazon&scope=jp&from=2026-06-05&to=2026-06-05')).status, 200);   // 終われば次は通る
    assert.equal(sawLockSql, 3);
  } finally { release(); __setPgClientFactory(factoryOf(pg)); }
});
await t('0049 の適用前は 409 not_migrated (関数が無い = 500 にしない)', async () => {
  const pg2 = new PGlite();
  await applyMigrations(pgliteAdapter(pg2), { log: quiet, to: '0048' });
  __setPgClientFactory(factoryOf(pg2));
  try {
    const r = await http('/amazon-profit/daily?mall=amazon&scope=jp&from=2026-06-01&to=2026-06-01');
    assert.deepEqual([r.status, r.json.error], [409, 'not_migrated']);
    assert.equal((await http('/amazon-profit/totals?mall=amazon&scope=jp&from=2026-06-01&to=2026-06-01')).status, 409);
  } finally { __setPgClientFactory(factoryOf(pg)); await pg2.close(); }
});
await new Promise((resolve) => server.close(resolve));

console.log(`\n${ok} 件 PASS${ng ? ` / ${ng} 件 NG` : ''}`);
process.exit(ng ? 1 : 0);
