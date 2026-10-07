#!/usr/bin/env node
/**
 * test-company-db-profit-response-contract.mjs — Amazon の利益の受け口の **最終の JSON の契約** (D-60 v3.4 の PR 2b) の試験
 *
 * 設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10「応答の契約」「長い期間の応答の列ごとの規則」(Codex R-D60-v3-4 M5・v3-5 Low)
 * 契約 = apps/company-db/profit/response-contract.mjs / 文書 = docs/contracts/company_db_amazon_profit_response.contract.md / fixture = scripts/fixtures/amazon-profit-response/
 *   - 列の分類が DB の関数の戻りの全部の列と一致 (PGlite で 0001〜 を流して pg_proc から読む = 列が増えたら落ちる)
 *   - 理由の決まった順が 0049 / 0050 の関数の本文の順と一致
 *   - Decimal の足し算と丸め = PostgreSQL の round(sum(numeric), 2) と一致 (PGlite で突き合わせる)
 *   - fixture (月ごとの入力 → 最終の応答) が列ごとの規則どおり・最終の応答が契約を満たす (BigInt / Decimal は文字列・null の伝わり方・理由の順・raw の列なし・months[]・calculated_at は 1 つの値)
 *   - 契約を破った応答 (raw の列・数の bigint・3 桁の小数・-0.00・理由の順・calculated_at の違い・months の欠け ほか) を全部拒む
 *   - 503 の code の一覧・今の router の 503 (PROFIT_ROUTE_DISABLED) も同じ形 (DB に接続しない)
 *   🆕 #1602 Codex R1: 503 は code ごとの固定の文・reason の列挙 (秘密の sentinel を必ず拒む)・全部の失敗の経路の対応表 /
 *     /daily の全部の列の null の規則・下限・列挙・行の不変条件 (全部の列 × 行で規則を逆にすると拒む) / validator は例外を投げない / 13 か月の上限
 * 🚨 DB の実装はしない。利益の受け口は 503 のまま
 * 実行: node scripts/test-company-db-profit-response-contract.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import {
  CONTRACT_VERSION, PROFIT_503_CODES, REQUIRED_HEADERS, REASON_ORDER, TOTALS_REASONS, ASSUMED_ZERO_REASONS, MASTER_NOTE_KEYS, TOTALS_COLUMNS, TOTALS_RULES, DAILY_COLUMNS,
  MONTH_KEYS, validateTotalsResponse, validateDailyResponse, validate503Body, monthsOf, PROFIT_503_ERRORS, PROFIT_503_REASONS, METRICS_REASONS, FAILURE_PATHS,
  build503Body, SECRET_PATTERNS, DAILY_NUL_RULES, MAX_MONTHS, REQUEST_LEVEL_FIELDS,
  // 🆕 v2 (PR 2c)
  CONTRACT_HISTORY, DAILY_DB_COLUMNS, MAX_MEMBER_SELLER_SKUS, MAX_MEMBER_SKU_CHARS, memberSkuErrors, FAILED_MONTHS_CODE, PROFIT_503_BODY_KEYS,
  CANCELLATION_SOURCES, CANCEL_STAGES, CANCEL_MAP, CANCEL_UNCLASSIFIED, CANCEL_SQLSTATES, classifyCancellation,
  // 🆕 #1615 Codex R1
  NO_ROLLBACK_SQLSTATES, REASON_REQUIRED_CODES,
} from '../apps/company-db/profit/response-contract.mjs';
import crypto from 'node:crypto';
import { combineTotals, combineDaily, sumDecimals, round2 } from './fixtures/amazon-profit-response/reference-combine.mjs';
import companyDbRouter, { __setPgClientFactory } from '../apps/company-db/router.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const FIX = new URL('./fixtures/amazon-profit-response/', import.meta.url);
const readFix = (f) => JSON.parse(fs.readFileSync(new URL(f, FIX), 'utf8'));
const clone = (x) => JSON.parse(JSON.stringify(x));
const totalsIn = readFix('totals-3months.input.json'), totalsExp = readFix('totals-3months.expected.json');
const dailyIn = readFix('daily-2months.input.json'), dailyExp = readFix('daily-2months.expected.json');
const vectors = readFix('rounding-vectors.json').cases, errors503 = readFix('errors-503.json').bodies;

const pg = new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: () => {} });
const fnCols = async (sig) => (await pg.query(`select a.name, format_type(a.typ, null) as type
    from pg_proc p cross join lateral unnest(p.proargnames, p.proallargtypes, p.proargmodes::text[]) with ordinality as a(name, typ, mode, ord)
   where p.oid = $1::regprocedure and a.mode = 't' order by a.ord`, [sig])).rows;

console.log('列の分類 = DB の関数の戻りの全部の列');
await t('/totals の分類の表 = mart.amazon_profit_day_totals_range の戻りの列 (名前・型・並び) と同じ・規則は型に合う', async () => {
  const cols = await fnCols('mart.amazon_profit_day_totals_range(smallint,text,text,date,date)');
  assert.ok(cols.length > 60, `${cols.length}`);
  assert.deepEqual(TOTALS_COLUMNS.map((c) => [c.name, c.type]), cols.map((c) => [c.name, c.type]));
  for (const c of TOTALS_COLUMNS) {
    assert.ok(TOTALS_RULES.includes(c.rule), c.name);
    if (/^decimal_sum/.test(c.rule)) assert.equal(c.type, 'numeric', c.name);
    if (/^bigint_sum/.test(c.rule)) assert.equal(c.type, 'bigint', c.name);
    if (c.rule === 'int_sum') assert.equal(c.type, 'integer', c.name);
    if (c.type === 'numeric') assert.match(c.rule, /^decimal_sum/, `numeric の列は Decimal で足す: ${c.name}`);
    if (c.type === 'bigint') assert.match(c.rule, /^(bigint_sum|null_in_period)/, `bigint の列は BigInt で足すか期間で null: ${c.name}`);
  }
  // 正式な値の列 = 1 か月でも null なら null
  for (const n of ['cogs_jpy', 'contribution_before_ad_incl_jpy', 'contribution_before_ad_excl', 'contribution_after_ad_incl', 'contribution_after_ad_excl',
    'profit_after_account_fees_incl', 'profit_after_account_fees_excl', 'ad_cost_total']) assert.match(TOTALS_COLUMNS.find((c) => c.name === n).rule, /_null$/, n);
  for (const n of ['day_finance_status', 'finance_coverage_generation', 'finance_source_revision']) assert.equal(TOTALS_COLUMNS.find((c) => c.name === n).rule, 'null_in_period', n);
});
await t('/daily の列の表 (v2 で足した列を除く) = mart.amazon_profit_daily_range の戻りの列 (名前・型・並び) と同じ・v2 で足したのは member_seller_skus だけ', async () => {
  const cols = await fnCols('mart.amazon_profit_daily_range(smallint,text,text,date,date)');
  assert.ok(cols.length > 80, `${cols.length}`);
  assert.deepEqual(DAILY_DB_COLUMNS.map((c) => [c.name, c.type]), cols.map((c) => [c.name, c.type]));
  // 🆕 v2: 応答だけの列 (3a の包む関数が返す) = 今の DB の関数の戻りに無い
  assert.deepEqual(DAILY_COLUMNS.filter((c) => c.addedIn).map((c) => [c.name, c.type, c.nul, c.addedIn]), [['member_seller_skus', 'text[]', 'never', 'v2']]);
  assert.ok(!cols.some((c) => c.name === 'member_seller_skus'), '0050 の関数が member_seller_skus を返すようになった = DAILY_COLUMNS の addedIn を外す');
});
await t('理由・印の決まった順 = 0049 / 0050 の関数の本文の array_remove(array[…]) の順', async () => {
  const def = async (sig) => (await pg.query(`select pg_get_functiondef($1::regprocedure) as d`, [sig])).rows[0].d;
  const seqs = (d) => [...d.matchAll(/array_remove\(array\[([\s\S]*?)\]::text\[\], null\)/g)].map((m) => [...m[1].matchAll(/then '([a-z_]+)' end/g)].map((x) => x[1]));
  const rows = seqs(await def('mart._amazon_profit_rows(smallint,text,text,date,date,mart.amazon_profit_finance_day[],mart.amazon_profit_ad_day[],mart.amazon_profit_ad_child[],mart.amazon_easy_ship_alloc_row[])'));
  assert.deepEqual(rows, [[...REASON_ORDER], [...ASSUMED_ZERO_REASONS], [...MASTER_NOTE_KEYS]]);
  const totals = seqs(await def('mart._amazon_profit_totals(smallint,text,text,date,date)'));
  assert.deepEqual(totals, [[...TOTALS_REASONS]]);
});

console.log('Decimal の足し算と丸め = PostgreSQL の round(sum(numeric), 2)');
await t(`丸めの例 ${vectors.length} 件: 参照の組み立て = fixture の期待 = PGlite の round`, async () => {
  for (const v of vectors) {
    assert.equal(round2(sumDecimals(v.raws)), v.expected, JSON.stringify(v.raws));
    const r = (await pg.query(`select round(sum(x::numeric), 2)::text as s from unnest($1::text[]) x`, [v.raws])).rows[0].s;
    assert.equal(r, v.expected, `PG: ${JSON.stringify(v.raws)} → ${r}`);
  }
  // 月ごとに丸めてから足すと違う例がある (= 丸めは最後に 1 回)
  const v = vectors.find((x) => x.expected === '300.01');
  assert.equal(round2(sumDecimals(v.raws.map((x) => round2(sumDecimals([x]))))), '300.00');
});

console.log('fixture (月ごとの計算 → 最終の応答)');
await t('/totals: 列ごとの規則で作った応答 = totals-3months.expected.json・契約を満たす', async () => {
  const r = combineTotals(clone(totalsIn));
  assert.equal(r.status, 200);
  assert.deepEqual(r.body, totalsExp);
  const v = validateTotalsResponse(totalsExp);
  assert.deepEqual(v.errors, []);
});
await t('/totals の期待の値 (手で確かめた値): 最後に 1 回丸める・負の半分は 0 から遠い方へ・2^53 を超える BigInt・1 か月でも null なら null・状態は一番弱い・理由は和集合を決まった順に', async () => {
  const x = totalsExp.total;
  assert.equal(x.ad_cost_allocated, '300.01');                         // 月ごとに丸めると 300.00
  assert.equal(x.profit_after_account_fees_assuming_incomplete_zero_incl, '-1500.01');   // -1500.005
  assert.equal(x.net_jpy, '9007199254740992');                         // Number で足すと …991
  assert.notEqual(String(Number('9007199254740993') + Number('-1')), x.net_jpy);
  assert.equal(x.cogs_jpy, null);
  for (const n of ['contribution_before_ad_incl_jpy', 'contribution_before_ad_excl', 'contribution_after_ad_incl', 'contribution_after_ad_excl', 'profit_after_account_fees_incl', 'profit_after_account_fees_excl', 'ad_cost_total']) assert.equal(x[n], null, n);
  assert.equal(x.ad_status, 'missing');
  assert.deepEqual(x.profit_incomplete_reasons, ['finance_incomplete', 'finance_unclassified', 'cost_missing', 'ad_missing']);
  assert.deepEqual(x.master_note_counts, { pre_audit_unverifiable: 7, current_after_recorded_change: 1, listing_changed_since_received: 7 });
  assert.equal(x.day_count, 57);
  assert.equal(x.before_ad_incomplete_day_count, x.before_ad_incomplete_days.length);
  assert.deepEqual([x.day_finance_status, x.finance_coverage_generation, x.finance_source_revision], [null, null, null]);
  assert.deepEqual([x.period_from, x.period_to], ['2026-06-15', '2026-08-10']);
});
await t('/totals の Decimal の列の全部 = PGlite の round(sum(月の raw), 2) (参照の組み立てと別の計算で)', async () => {
  for (const c of TOTALS_COLUMNS.filter((c) => /^decimal_sum/.test(c.rule))) {
    const raws = totalsIn.months.map((m) => m.totals[`${c.name}_raw`]);
    const want = raws.some((v) => v === null) ? null : (await pg.query(`select round(sum(x::numeric), 2)::text as s from unnest($1::text[]) x`, [raws])).rows[0].s;
    assert.equal(totalsExp.total[c.name], want, c.name);
  }
});
await t('/totals の months[]: 触れる暦月が全部 (財務の行が 0 の 8 月も)・世代と版は月ごと・calculated_at は全部同じ 1 つの値 (月の計算の値は捨てる)', async () => {
  assert.deepEqual(totalsExp.months.map((m) => [m.month_start, m.period_from, m.period_to]),
    [['2026-06-01', '2026-06-15', '2026-06-30'], ['2026-07-01', '2026-07-01', '2026-07-31'], ['2026-08-01', '2026-08-01', '2026-08-10']]);
  assert.deepEqual(totalsExp.months.map((m) => [m.finance_status, m.has_finance_rows, m.finance_month_settled, m.finance_coverage_generation, m.finance_source_revision]),
    [['complete', true, true, '12', '345'], ['provisional', true, false, '12', null], ['missing', false, false, null, null]]);
  const all = [totalsExp.calculated_at, totalsExp.master_as_of, totalsExp.total.calculated_at, ...totalsExp.months.map((m) => m.calculated_at)];
  assert.deepEqual([...new Set(all)], [totalsIn.request.calculated_at]);
  assert.ok(new Set(totalsIn.months.map((m) => m.totals.calculated_at)).size === 3, '入力は月ごとに違う時刻 (置き換えを試す)');
  for (const m of totalsExp.months) assert.deepEqual(Object.keys(m), [...MONTH_KEYS]);
});
await t('/totals: raw の列・旧 totals の行の種類の列が応答に無い (入力にはある)', async () => {
  assert.match(JSON.stringify(totalsIn), /_raw"/);
  assert.match(JSON.stringify(totalsIn), /"row_kind"/);
  const text = JSON.stringify(totalsExp);
  assert.doesNotMatch(text, /_raw"|"row_kind"|"month_start":"2026-06-01","economic|range_total|"economic_date_jst"/);
  assert.ok(!('month_start' in totalsExp.total) && !('economic_date_jst' in totalsExp.total));
});
await t('/daily: 月の行をつなぐだけ (足さない) = daily-2months.expected.json・契約を満たす・0 行の月も months[] にある・calculated_at は 1 つ', async () => {
  const r = combineDaily(clone(dailyIn));
  assert.equal(r.status, 200);
  assert.deepEqual(r.body, dailyExp);
  assert.deepEqual(validateDailyResponse(dailyExp).errors, []);
  assert.equal(dailyExp.rows.length, 5);
  assert.deepEqual([dailyExp.rows[4].cost_basis, dailyExp.rows[4].composition_basis, dailyExp.rows[4].profit_incomplete_reasons], ['missing', 'current_no_recorded_change', ['cost_missing']]);   // 本当の原価不足の行 (R3 M1)
  assert.deepEqual([dailyExp.rows[3].unclassified_component_count, dailyExp.rows[3].unmapped_component_count, dailyExp.rows[3].finance_legacy_rows, dailyExp.rows[3].profit_incomplete_reasons], [1, 1, 2, ['finance_unclassified']]);   // 件数が 0 でない行 (R2 M1)
  assert.deepEqual(dailyExp.months.map((m) => [m.month_start, m.has_finance_rows]), [['2026-07-01', true], ['2026-08-01', false]]);
  assert.deepEqual([...new Set([dailyExp.calculated_at, dailyExp.master_as_of, ...dailyExp.rows.map((x) => x.calculated_at), ...dailyExp.months.map((m) => m.calculated_at)])], [dailyIn.request.calculated_at]);
  assert.equal(dailyExp.rows[0].listing_id, '9007199254740993');   // bigint の ID は文字列のまま (Number なら …992)
  assert.equal(dailyExp.rows[1].listing_id, null);                 // 出品の無い行は日の中で後ろ
});
await t('calculation_version / master_basis が月で違えば 503 PROFIT_VERSION_MISMATCH (部分の値を返さない)', async () => {
  const a = clone(totalsIn); a.months[1].totals.calculation_version = 'amazon_profit_v2';
  const b = clone(totalsIn); b.months[2].meta.calculation_version = 'amazon_profit_v2';
  const c = clone(totalsIn); c.months[0].totals.master_basis = 'received';
  const d = clone(dailyIn); d.months[0].rows[2].calculation_version = 'amazon_profit_v2';
  for (const r of [combineTotals(a), combineTotals(b), combineTotals(c), combineDaily(d)]) {
    assert.equal(r.status, 503);
    assert.equal(r.body.code, 'PROFIT_VERSION_MISMATCH');
    assert.deepEqual(validate503Body(r.body).errors, []);
  }
});

console.log('契約を破った応答を拒む');
const breakTotals = [
  ['total に raw の列', (x) => { x.total.ad_cost_allocated_raw = '300.012'; }],
  ['months に raw の列', (x) => { x.months[0].range_total_raw = '1'; }],
  ['上に raw', (x) => { x.raw = {}; }],
  ['旧 totals の row_kind', (x) => { x.total.row_kind = 'range_total'; }],
  ['旧 totals の month_start', (x) => { x.total.month_start = '2026-06-01'; }],
  ['bigint が数', (x) => { x.total.net_jpy = 9007199254740992; }],
  ['bigint が -0', (x) => { x.total.unmapped_jpy = '-0'; }],
  ['bigint に小数', (x) => { x.total.units_ordered = '1523.0'; }],
  ['Decimal が 3 桁', (x) => { x.total.ad_cost_allocated = '300.012'; }],
  ['Decimal が 1 桁', (x) => { x.total.ad_cost_allocated = '300.0'; }],
  ['Decimal が -0.00', (x) => { x.total.account_fee_cost_excl = '-0.00'; }],
  ['Decimal が数', (x) => { x.total.ad_cost_allocated = 300.01; }],
  ['null にならない Decimal が null', (x) => { x.total.contribution_after_ad_assuming_incomplete_zero_incl = null; }],
  ['null にならない bigint が null', (x) => { x.total.net_jpy = null; }],
  ['integer が文字', (x) => { x.total.day_count = '57'; }],
  ['理由の順が違う', (x) => { x.total.profit_incomplete_reasons = ['finance_unclassified', 'finance_incomplete']; }],
  ['理由の重複', (x) => { x.total.profit_incomplete_reasons = ['finance_incomplete', 'finance_incomplete']; }],
  ['totals に ad_unresolved', (x) => { x.total.profit_incomplete_reasons = ['ad_unresolved']; }],
  ['知らない理由', (x) => { x.total.profit_incomplete_reasons = ['something']; }],
  ['知らない状態', (x) => { x.total.ad_status = 'partial'; }],
  ['期間の行で day_finance_status', (x) => { x.total.day_finance_status = 'complete'; }],
  ['期間の行で世代', (x) => { x.total.finance_coverage_generation = '12'; }],
  ['total.calculated_at が違う', (x) => { x.total.calculated_at = '2026-10-03T03:00:12.004Z'; }],
  ['months の calculated_at が違う', (x) => { x.months[2].calculated_at = '2026-10-03T03:00:20.777Z'; }],
  ['master_as_of が違う', (x) => { x.master_as_of = '2026-10-03T03:00:00.000Z'; }],
  ['calculated_at がミリ秒なし', (x) => { x.calculated_at = '2026-10-03T03:00:00Z'; x.master_as_of = x.calculated_at; x.total.calculated_at = x.calculated_at; for (const m of x.months) m.calculated_at = x.calculated_at; }],
  ['months の月が欠ける (0 行の月を落とした)', (x) => { x.months.pop(); }],
  ['months の順が違う', (x) => { x.months.reverse(); }],
  ['months の calculation_version が違う', (x) => { x.months[1].calculation_version = 'amazon_profit_v2'; }],
  ['months の世代が数', (x) => { x.months[0].finance_coverage_generation = 12; }],
  ['months の知らない状態', (x) => { x.months[0].finance_status = 'settled'; }],
  ['months に知らない列', (x) => { x.months[0].ad_cost_total = '1.00'; }],
  ['total に知らない列', (x) => { x.total.extra = 1; }],
  ['total の列が欠ける', (x) => { delete x.total.cogs_jpy; }],
  ['不完全な日の数が違う', (x) => { x.total.before_ad_incomplete_day_count = 1; }],
  ['不完全な日の順が違う', (x) => { x.total.before_ad_incomplete_days.reverse(); }],
  ['不完全な日が期間の外', (x) => { x.total.after_account_fees_incomplete_days[0] = '2026-06-01'; }],
  ['master_note_counts のキーが違う', (x) => { x.total.master_note_counts = { pre_audit_unverifiable: 1 }; }],
  ['period_from が要求と違う', (x) => { x.total.period_from = '2026-06-01'; }],
  ['contract の版が違う', (x) => { x.contract = 'amazon_profit_response_v0'; }],
  ['ok が false', (x) => { x.ok = false; }],
  ['kind が違う', (x) => { x.kind = 'daily'; }],
  // 🆕 #1602 Codex R2 Low: int_sum の件数の列は 0 以上
  ['day_count が負', (x) => { x.total.day_count = -1; }],
  ['resolved_rows が負', (x) => { x.total.resolved_rows = -1; }],
  ['unknown_line_rows が負', (x) => { x.total.unknown_line_rows = -1; }],
  ['easy_ship_unallocated_count が負', (x) => { x.total.easy_ship_unallocated_count = -3; }],
];
await t(`/totals: 契約を破った ${breakTotals.length} 通りを全部拒む`, async () => {
  for (const [name, f] of breakTotals) {
    const x = clone(totalsExp); f(x);
    assert.equal(validateTotalsResponse(x).ok, false, name);
  }
});
await t('/totals: int_sum の全部の列 (日数・行数・件数) を -1 にすると拒む・0 は通る (R2 Low)', async () => {
  const cols = TOTALS_COLUMNS.filter((c) => c.rule === 'int_sum');
  assert.ok(cols.length >= 15, `${cols.length}`);
  for (const c of cols) {
    const x = clone(totalsExp); x.total[c.name] = -1;
    assert.ok(validateTotalsResponse(x).errors.some((e) => e.includes(`$.total.${c.name}: 件数が負`)), c.name);
  }
  const z = clone(totalsExp); z.total.unknown_line_rows = 0;
  assert.deepEqual(validateTotalsResponse(z).errors, []);
});
const breakDaily = [
  ['行に raw の列', (x) => { x.rows[0].ad_cost_raw = '120.5'; }],
  ['行の calculated_at が違う', (x) => { x.rows[2].calculated_at = '2026-10-03T03:00:00.130Z'; }],
  ['ID が数', (x) => { x.rows[0].listing_id = 9007199254740993; }],
  ['ID の配列に数', (x) => { x.rows[0].received_listing_ids = [1]; }],
  ['返品数が 2 桁', (x) => { x.rows[2].units_refunded_customer_unrounded = '0.50'; }],
  ['金額の numeric が 6 桁', (x) => { x.rows[0].ad_cost = '120.500000'; }],
  ['null にならない列が null', (x) => { x.rows[0].profit_incomplete_reasons = null; }],
  ['理由の順', (x) => { x.rows[2].profit_incomplete_reasons = ['refund_units_partial_month', 'finance_incomplete']; }],
  ['0 と仮定の理由に refund_units_partial_month', (x) => { x.rows[2].assumed_zero_reasons = ['refund_units_partial_month']; }],
  ['master_notes の順', (x) => { x.rows[0].master_notes = ['listing_changed_since_received', 'pre_audit_unverifiable']; }],
  ['並び (出品の無い行が前)', (x) => { [x.rows[0], x.rows[1]] = [x.rows[1], x.rows[0]]; }],
  ['同じ行が 2 つ (行の鍵が重なる)', (x) => { x.rows.push(clone(x.rows[2])); }],
  ['期間の外の日', (x) => { x.rows[2].economic_date_jst = '2026-08-03'; }],
  ['日付の形', (x) => { x.rows[0].economic_date_jst = '2026-07-30T00:00:00Z'; }],
  ['列が欠ける', (x) => { delete x.rows[0].cogs_jpy; }],
  ['知らない列', (x) => { x.rows[0].row_total = '1'; }],
  ['0 行の月を落とした', (x) => { x.months.pop(); }],
  ['旧 totals の行の種類', (x) => { x.rows[0].row_kind = 'day'; }],
  // 🆕 #1602 Codex R1 M1 (Codex が通ってしまうと示した 4 つ + 値域・列挙・行の鍵)
  ['units_ordered が null', (x) => { x.rows[0].units_ordered = null; }],
  ['0 と仮定の利益が null', (x) => { x.rows[1].contribution_after_ad_assuming_incomplete_zero_incl = null; }],
  ['refund_units_status が知らない値', (x) => { x.rows[0].refund_units_status = 'invented'; }],
  ['source_lines が負', (x) => { x.rows[0].source_lines = -1; }],
  ['unclassified_abs_jpy が負', (x) => { x.rows[0].unclassified_abs_jpy = '-1'; }],
  ['refund_incomplete_child_count が 2', (x) => { x.rows[2].refund_incomplete_child_count = 2; }],
  ['refund_incomplete_child_count が状態と合わない', (x) => { x.rows[0].refund_incomplete_child_count = 1; }],
  ['cost_basis が知らない値', (x) => { x.rows[0].cost_basis = 'sku_cost'; }],
  ['composition_basis が知らない値', (x) => { x.rows[0].composition_basis = 'current'; }],
  ['master_basis が current でない', (x) => { x.rows[0].master_basis = 'received'; }],
  ['hash が 64 桁の 16 進でない', (x) => { x.rows[0].composition_hash = 'c0ffee'; }],
  ['hash が大文字', (x) => { x.rows[0].cost_input_hash = 'B'.repeat(64); }],
  ['resolved なのに listing_id が null', (x) => { x.rows[0].listing_id = null; }],
  ['resolved なのに seller_sku_norm がある', (x) => { x.rows[0].seller_sku_norm = 'AB-001'; }],
  ['resolved なのに listing_code が null', (x) => { x.rows[2].listing_code = null; }],
  ['unresolved なのに listing_id がある', (x) => { x.rows[1].listing_id = '99'; }],
  ['unresolved なのに seller_sku_norm が null', (x) => { x.rows[1].seller_sku_norm = null; }],
  ['unresolved なのに listing_code がある', (x) => { x.rows[1].listing_code = 'ZZ'; }],
  ['unresolved なのに原価がある', (x) => { x.rows[1].component_unit_cost_jpy = '1'; x.rows[1].cogs_jpy = '1'; }],
  ['unresolved なのに cost_basis が sku_costs', (x) => { x.rows[1].cost_basis = 'sku_costs'; }],
  ['unresolved なのに composition_basis が missing', (x) => { x.rows[1].composition_basis = 'missing'; }],
  ['unresolved なのに理由に listing_unresolved が無い', (x) => { x.rows[1].profit_incomplete_reasons = []; x.rows[1].assumed_zero_reasons = []; }],
  ['unresolved に pre_audit_unverifiable の印', (x) => { x.rows[1].master_notes = ['pre_audit_unverifiable']; }],
  ['resolved に理由 listing_unresolved', (x) => { x.rows[0].profit_incomplete_reasons = ['listing_unresolved']; x.rows[0].assumed_zero_reasons = ['listing_unresolved']; }],
  ['原価が分かるのに cogs が null', (x) => { x.rows[0].cogs_jpy = null; }],
  ['原価が分かるのに cost_input_hash が null', (x) => { x.rows[0].cost_input_hash = null; }],
  ['構成が分かるのに composition_hash が null', (x) => { x.rows[2].composition_hash = null; }],
  ['理由が無いのに正式な値が null', (x) => { x.rows[0].contribution_after_ad_excl = null; }],
  ['広告の前の理由があるのに正式な値がある', (x) => { x.rows[2].contribution_before_ad_incl_jpy = '-150'; }],
  ['理由があるのに広告の後の正式な値がある', (x) => { x.rows[2].contribution_after_ad_incl = '-150.00'; }],
  ['広告が complete なのに ad_cost が null', (x) => { x.rows[0].ad_cost = null; }],
  ['広告が missing なのに ad_cost がある・理由が無い', (x) => { x.rows[0].ad_status = 'missing'; }],
  ['返品の単価が無いのに丸める前の返品数がある', (x) => { x.rows[0].refund_units_status = 'unit_price_missing'; x.rows[0].refund_incomplete_child_count = 1; }],
  ['返品の単価があるのに丸める前の返品数が null', (x) => { x.rows[0].units_a_to_z_refund_unrounded = null; }],
  ['月の途中の単価なのに理由が無い', (x) => { x.rows[0].refund_units_status = 'estimated_partial_month_unit_price'; x.rows[0].refund_incomplete_child_count = 1; }],
  ['日の財務が provisional なのに理由が無い', (x) => { x.rows[0].day_finance_status = 'provisional'; }],
  ['0 と仮定の理由が理由と合わない', (x) => { x.rows[1].assumed_zero_reasons = []; }],
  ['composition_basis が印と合わない', (x) => { x.rows[0].composition_basis = 'current_no_recorded_change'; }],
  ['composition_audit_since が null', (x) => { x.rows[0].composition_audit_since = null; }],
  // 🆕 #1602 Codex R2 M1: 件数の合計 > 0 ⇔ 理由 finance_unclassified (正式な値も止まる)
  ['件数があるのに理由 finance_unclassified を落とした (正式な値は null のまま)', (x) => { x.rows[3].profit_incomplete_reasons = []; x.rows[3].assumed_zero_reasons = []; }],
  ['件数があるのに理由を落とし正式な値を出した', (x) => { const r = x.rows[3]; r.profit_incomplete_reasons = []; r.assumed_zero_reasons = []; r.contribution_before_ad_incl_jpy = '1500'; r.contribution_before_ad_excl = '1363.64'; r.contribution_after_ad_incl = '1489.00'; r.contribution_after_ad_excl = '1353.64'; }],
  ['件数が 0 なのに理由 finance_unclassified', (x) => { const r = x.rows[3]; r.unclassified_component_count = 0; r.unmapped_component_count = 0; r.finance_legacy_rows = 0; }],
  ['unclassified_component_count だけ 1・理由なし', (x) => { x.rows[0].unclassified_component_count = 1; }],
  ['unmapped_component_count だけ 1・理由なし', (x) => { x.rows[0].unmapped_component_count = 1; }],
  ['finance_legacy_rows だけ 1・理由なし', (x) => { x.rows[0].finance_legacy_rows = 1; }],
  // 🆕 #1602 Codex R3 M1: cost_missing は listing_unresolved・composition_missing と排他 (0050 の g_unres・g_comp・g_cost)
  ['未解決の行に cost_missing', (x) => { x.rows[1].profit_incomplete_reasons = ['listing_unresolved', 'cost_missing']; x.rows[1].assumed_zero_reasons = ['listing_unresolved', 'cost_missing']; }],
  ['構成なしの行に cost_missing', (x) => { const r = x.rows[4]; r.composition_basis = 'missing'; r.composition_hash = null; r.profit_incomplete_reasons = ['composition_missing', 'cost_missing']; r.assumed_zero_reasons = ['composition_missing', 'cost_missing']; }],
  ['本当の原価不足の行から cost_missing を落とす', (x) => { x.rows[4].profit_incomplete_reasons = []; x.rows[4].assumed_zero_reasons = []; }],
  ['原価が分かる行に cost_missing (正式な値も null に)', (x) => { const r = x.rows[0]; r.profit_incomplete_reasons = ['cost_missing']; r.assumed_zero_reasons = ['cost_missing']; for (const k of ['contribution_before_ad_incl_jpy', 'contribution_before_ad_excl', 'contribution_after_ad_incl', 'contribution_after_ad_excl']) r[k] = null; }],
  // 🆕 R3 の突き合わせ: 日で決まる値 (day_finance_status・ad_status・coverage の世代と版・理由 ad_unresolved) は同じ日の行で同じ
  ['同じ日の行で day_finance_status が違う (行の中は整合)', (x) => { const r = x.rows[3]; r.day_finance_status = 'provisional'; r.profit_incomplete_reasons = ['finance_incomplete', 'finance_unclassified']; r.assumed_zero_reasons = ['finance_incomplete', 'finance_unclassified']; }],
  ['同じ日の行で ad_status が違う (行の中は整合)', (x) => { x.rows[4].ad_status = 'complete'; }],
  ['ad_unresolved が同じ日の 1 行だけ (行の中は整合)', (x) => { const r = x.rows[0]; r.profit_incomplete_reasons = ['ad_unresolved']; r.assumed_zero_reasons = ['ad_unresolved']; r.contribution_after_ad_incl = null; r.contribution_after_ad_excl = null; }],
  ['同じ日の行で coverage の世代が違う', (x) => { x.rows[1].finance_coverage_generation = '13'; }],
  // 🆕 #1602 Codex R4 M1: 要求全体で固定の値 (company_id・observed_generation・composition_audit_since) は全部の行で同じ
  ['2 行目だけ company_id が違う (別の会社の行が混ざる)', (x) => { x.rows[1].company_id = 2; }],
  ['最後の行だけ company_id が違う (別の月・別の日)', (x) => { x.rows[4].company_id = 2; }],
  ['2 行目だけ observed_generation が違う', (x) => { x.rows[1].observed_generation = '999'; }],
  ['別の日の行だけ observed_generation が null', (x) => { x.rows[3].observed_generation = null; }],
  ['2 行目だけ composition_audit_since が違う', (x) => { x.rows[1].composition_audit_since = '2026-10-01T15:00:00.000Z'; }],
  ['company_id が 0', (x) => { for (const r of x.rows) r.company_id = 0; }],
];
await t(`/daily: 契約を破った ${breakDaily.length} 通りを全部拒む`, async () => {
  for (const [name, f] of breakDaily) {
    const x = clone(dailyExp); f(x);
    assert.equal(validateDailyResponse(x).ok, false, name);
  }
});
await t('/daily: 全部の列 × 全部の行で、null の規則 (nul) を逆にすると拒む (never は null に・iff_* は null ⇔ 値を入れ替える)', async () => {
  assert.deepEqual([...new Set(DAILY_COLUMNS.map((c) => c.nul))].sort(), [...DAILY_NUL_RULES].sort());
  const sample = (c) => ({ smallint: 1, integer: 0, bigint: '1', 'bigint[]': [], numeric: c.sixDp ? '0' : '1.00', date: '2026-07-30', text: c.hex64 ? 'e'.repeat(64) : 'x',
    'text[]': [], 'timestamp with time zone': '2026-07-30T00:00:00.000Z' }[c.type]);
  let n = 0;
  for (const c of DAILY_COLUMNS.filter((c) => c.nul !== 'maybe')) {
    for (let i = 0; i < dailyExp.rows.length; i++) {
      const x = clone(dailyExp);
      x.rows[i][c.name] = x.rows[i][c.name] === null ? sample(c) : null;
      assert.equal(validateDailyResponse(x).ok, false, `${c.name} (${c.nul}) の行 ${i}`);
      n++;
    }
  }
  assert.ok(n > 200, `${n}`);
});
await t('/daily: 受け取るべき形 (R3): 構成なしの行は composition_missing だけ・ad_unresolved は同じ日の全部の行に付く', async () => {
  const a = clone(dailyExp); const r = a.rows[4];
  r.composition_basis = 'missing'; r.composition_hash = null; r.profit_incomplete_reasons = ['composition_missing']; r.assumed_zero_reasons = ['composition_missing'];
  r.missing_cost_sku_ids = [];   // 構成なしの行は 0050 の uc の行が無い = missing_ids は空 (R5)
  assert.deepEqual(validateDailyResponse(a).errors, []);
  const b = clone(dailyExp);
  for (const r of b.rows.filter((r) => r.economic_date_jst === '2026-07-30')) {
    r.profit_incomplete_reasons = [...r.profit_incomplete_reasons, 'ad_unresolved']; r.assumed_zero_reasons = [...r.assumed_zero_reasons, 'ad_unresolved'];
    r.contribution_after_ad_incl = null; r.contribution_after_ad_excl = null;
  }
  assert.deepEqual(validateDailyResponse(b).errors, []);
});
await t('/daily: 要求全体で固定の値 (R4): 全部の行でそろえて変えるのは受け取る (company_id・observed_generation・composition_audit_since)', async () => {
  for (const [k, v] of [['company_id', 2], ['observed_generation', '999'], ['observed_generation', null], ['composition_audit_since', '2026-10-01T15:00:00.000Z']]) {
    const x = clone(dailyExp); for (const r of x.rows) r[k] = v;
    assert.deepEqual(validateDailyResponse(x).errors, [], `${k}=${v}`);
  }
  assert.deepEqual([...REQUEST_LEVEL_FIELDS], ['company_id', 'observed_generation', 'composition_audit_since']);
});
const ID_ARRAYS = ['received_listing_ids', 'ad_received_listing_ids', 'missing_cost_sku_ids', 'cost_sku_cost_ids', 'cost_observed_ids'];
await t('/daily: ID の配列 5 つは ID (BigInt) の厳密な昇順・重複なし・1 以上 (R5 Low)・fixture は 2 要素以上', async () => {
  assert.deepEqual(DAILY_COLUMNS.filter((c) => c.type === 'bigint[]').map((c) => c.name), ID_ARRAYS);
  for (const k of ID_ARRAYS) {
    assert.ok(DAILY_COLUMNS.find((c) => c.name === k).idsAscending, k);
    const i = dailyExp.rows.findIndex((r) => r[k].length >= 2);
    assert.ok(i >= 0, `${k}: fixture に 2 要素以上の行が無い`);
    const bad = [[...dailyExp.rows[i][k]].reverse(), [dailyExp.rows[i][k][0], dailyExp.rows[i][k][0]], ['10', '9'], ['0', '5'], ['9007199254740993', '9007199254740992'], ['-1']];
    for (const v of bad) {
      const x = clone(dailyExp); x.rows[i][k] = v;
      assert.ok(validateDailyResponse(x).errors.some((e) => e.includes(`.${k}: ID の厳密な昇順`)), `${k} = ${JSON.stringify(v)}`);
    }
    // 数の順 (文字の順でない)・2^53 の近くも BigInt で比べる (Number なら 2 つが同じになる)
    for (const v of [['9', '10'], ['9007199254740992', '9007199254740993']]) {
      const x = clone(dailyExp); x.rows[i][k] = v;
      assert.ok(!validateDailyResponse(x).errors.some((e) => e.includes(`.${k}: ID の厳密な昇順`)), `${k} = ${JSON.stringify(v)}`);
    }
  }
});
await t('/daily: 原価の無い SKU (missing_cost_sku_ids) がある ⇔ 理由 cost_missing (R5 の突き合わせ)', async () => {
  const a = clone(dailyExp); a.rows[0].missing_cost_sku_ids = ['99'];
  assert.ok(validateDailyResponse(a).errors.some((e) => e.includes('missing_cost_sku_ids) がある ⇔ 理由 cost_missing')));
  const b = clone(dailyExp); b.rows[4].missing_cost_sku_ids = [];
  assert.ok(validateDailyResponse(b).errors.some((e) => e.includes('missing_cost_sku_ids) がある ⇔ 理由 cost_missing')));
});
await t('0050 の ID の配列の作り方 = 昇順 (distinct つき・または主キー (listing_id, sku_id) で重複が出ない)', async () => {
  const def = async (sig) => (await pg.query('select pg_get_functiondef($1::regprocedure) as d', [sig])).rows[0].d;
  const sku = await def('mart.finance_daily_sku_range(smallint,text,text,date,date)');
  const lineWith = (text, needle) => text.split('\n').find((l) => l.includes(needle)) || '';
  assert.ok(lineWith(sku, 'as received_listing_ids').includes('array_agg(distinct listing_id order by listing_id)'), 'received_listing_ids');
  const rows = await def('mart._amazon_profit_rows(smallint,text,text,date,date,mart.amazon_profit_finance_day[],mart.amazon_profit_ad_day[],mart.amazon_profit_ad_child[],mart.amazon_easy_ship_alloc_row[])');
  assert.ok(lineWith(rows, 'ad_rcv as (') !== '' && rows.includes('array_agg(distinct x.rid order by x.rid) as ids'), 'ad_received_listing_ids');
  assert.ok(lineWith(rows, 'as missing_ids').includes('array_agg(cc.sku_id order by cc.sku_id) filter (where not cc.known)'), 'missing_cost_sku_ids');
  assert.ok(lineWith(rows, 'as sc_ids').includes('array_agg(cc.row_id order by cc.row_id)'), 'cost_sku_cost_ids');
  assert.ok(lineWith(rows, 'as ob_ids').includes('array_agg(cc.row_id order by cc.row_id)'), 'cost_observed_ids');
  const pk = (await pg.query(`select pg_get_constraintdef(c.oid) as d from pg_constraint c where c.conrelid = 'core.listing_components'::regclass and c.contype = 'p'`)).rows[0].d;
  assert.equal(pk, 'PRIMARY KEY (listing_id, sku_id)');   // 1 つの出品の構成の SKU は重ならない = sku_id・原価の行の ID も重ならない
});
await t('/daily: maybe の列は null でも値でもよい (世代・監査の時刻・coverage)', async () => {
  for (const c of DAILY_COLUMNS.filter((c) => c.nul === 'maybe')) {
    const x = clone(dailyExp); for (const r of x.rows) r[c.name] = null;   // 日で決まる列 (coverage) は同じ日の全部の行で同じ
    assert.deepEqual(validateDailyResponse(x).errors, [], c.name);
  }
});

console.log('503 (固定の文・列挙の reason・全部の失敗の経路)');
await t('503 の code の一覧 = fixture の本文 (code ごとに 1 つ・固定の文)・全部が契約を満たす', async () => {
  assert.deepEqual(errors503.map((b) => b.code), [...PROFIT_503_CODES]);
  for (const b of errors503) {
    assert.deepEqual(validate503Body(b).errors, [], b.code);
    assert.equal(b.error, PROFIT_503_ERRORS[b.code]);
    assert.deepEqual(b, build503Body(b.code, b.reason, b.failed_months));
    assert.equal(Object.hasOwn(b, 'failed_months'), b.code === FAILED_MONTHS_CODE, b.code);   // 🆕 v2
  }
  for (const c of PROFIT_503_CODES) for (const re of SECRET_PATTERNS) assert.doesNotMatch(PROFIT_503_ERRORS[c], re, c);
});
await t('🚨 秘密の sentinel を error・reason・上流の例外・余計な列に入れた 503 は必ず拒む (H1)', async () => {
  const SECRETS = ['Bearer rnd_EXPOSEDSECRET123456', 'rnd_EXPOSEDSECRET123456', 'postgres://cdb:p4ss@dpg-x.oregon-postgres.render.com/cdb',
    'postgresql://u@h/db', 'RENDER_API_KEY=abc', 'password=hunter2'];
  for (const s of SECRETS) {
    const upstream = new Error(`connect failed: ${s}`);
    const bad = [
      { ok: false, code: 'PROFIT_METRICS_UNAVAILABLE', error: s, reason: 'METRICS_AUTH' },
      { ok: false, code: 'PROFIT_METRICS_UNAVAILABLE', error: `${PROFIT_503_ERRORS.PROFIT_METRICS_UNAVAILABLE} ${s}` },
      { ok: false, code: 'PROFIT_DB_UNAVAILABLE', error: upstream.message, reason: 'DB_CONNECT' },
      { ok: false, code: 'PROFIT_DB_UNAVAILABLE', error: PROFIT_503_ERRORS.PROFIT_DB_UNAVAILABLE, reason: s },
      { ok: false, code: 'PROFIT_DB_UNAVAILABLE', error: PROFIT_503_ERRORS.PROFIT_DB_UNAVAILABLE, reason: 'DB_CONNECT', detail: upstream.message },
      { ok: false, code: 'PROFIT_INTERNAL', error: PROFIT_503_ERRORS.PROFIT_INTERNAL, reason: String(upstream.stack).slice(0, 60) },
    ];
    for (const b of bad) assert.equal(validate503Body(b).ok, false, JSON.stringify(b));
    // build503Body は上流の例外の文を reason に渡されても付けない (列挙に無い) = 固定の本文だけ
    const built = build503Body('PROFIT_DB_UNAVAILABLE', upstream.message);
    assert.deepEqual(built, { ok: false, code: 'PROFIT_DB_UNAVAILABLE', error: PROFIT_503_ERRORS.PROFIT_DB_UNAVAILABLE });
    assert.ok(!JSON.stringify(built).includes(s));
    assert.deepEqual(validate503Body(built).errors, []);
  }
  assert.deepEqual(build503Body('NOT_A_CODE', 'x'), { ok: false, code: 'PROFIT_INTERNAL', error: PROFIT_503_ERRORS.PROFIT_INTERNAL });
});
await t('503 の契約を破った本文を拒む (知らない code・rows・Render の本文・列挙に無い reason・別の code の reason・ok が true・文の違い)', async () => {
  const E = PROFIT_503_ERRORS;
  const bad = [{ ok: false, code: 'PROFIT_UNKNOWN', error: 'x' }, { ok: false, code: 'PROFIT_BUSY', error: E.PROFIT_BUSY, rows: [] },
    { ok: false, code: 'PROFIT_METRICS_UNAVAILABLE', error: E.PROFIT_METRICS_UNAVAILABLE, upstream: { message: 'invalid key' } },
    { ok: false, code: 'PROFIT_BUSY', error: E.PROFIT_BUSY, reason: 'busy' }, { ok: false, code: 'PROFIT_BUSY', error: E.PROFIT_BUSY, reason: 'METRICS_AUTH' },
    { ok: false, code: 'PROFIT_ROUTE_DISABLED', error: E.PROFIT_ROUTE_DISABLED, reason: 'LOCK_NOT_AVAILABLE' },
    { ok: true, code: 'PROFIT_BUSY', error: E.PROFIT_BUSY }, { ok: false, code: 'PROFIT_BUSY' }, { ok: false, code: 'PROFIT_BUSY', error: 'x' },
    { ok: false, code: 'PROFIT_BUSY', error: `${E.PROFIT_BUSY} ` }, { ok: false, code: 'PROFIT_BUSY', error: E.PROFIT_BUSY, total: {} }, null, 'x', [], { ok: false, code: 'toString' }];
  for (const b of bad) assert.equal(validate503Body(b).ok, false, JSON.stringify(b));
});
await t('全部の失敗の経路 (M2) が 503 の code と reason の列挙に対応している・どの code も少なくとも 1 つの経路がある', async () => {
  for (const f of FAILURE_PATHS) {
    assert.ok(PROFIT_503_CODES.includes(f.code), f.path);
    if (f.reason == null) { assert.deepEqual([...PROFIT_503_REASONS[f.code]], [], f.path); continue; }
    for (const r of f.reason.split(' / ')) {
      const allowed = PROFIT_503_REASONS[f.code];
      if (r.endsWith('*')) assert.ok(allowed.some((a) => a.startsWith(r.slice(0, -1))), `${f.path}: ${r}`);
      else assert.ok(allowed.includes(r), `${f.path}: ${r}`);
    }
  }
  for (const c of PROFIT_503_CODES) assert.ok(FAILURE_PATHS.some((f) => f.code === c), `経路の無い code: ${c}`);
  // 名指しの経路 (Codex R1 M2)
  const need = ['DB の接続の失敗', 'BEGIN', 'SET LOCAL', '設定の読み返し', 'COMMIT の失敗', '結果が分からない', '予期しない DB の例外', '400'];
  for (const w of need) assert.ok(FAILURE_PATHS.some((f) => f.path.includes(w)), w);
  assert.ok(METRICS_REASONS.includes('METRICS_HTTP'));   // Render の 400 → PROFIT_METRICS_UNAVAILABLE + METRICS_HTTP
  assert.equal(MAX_MONTHS, 13);
});
await t('metrics の理由の一覧 = PR 2 (#1600) の render-metrics.mjs の REASONS (両方の PR がそろったときだけ比べる)', async () => {
  const p = new URL('../apps/company-db/profit/render-metrics.mjs', import.meta.url);
  if (!fs.existsSync(p)) { console.log('      (render-metrics.mjs がまだ無い = PR 2 のマージの後に比べる)'); return; }
  const { REASONS } = await import(p.href);
  assert.deepEqual([...METRICS_REASONS], [...REASONS]);
});
await t('今の router の 503 (PROFIT_ROUTE_DISABLED) も同じ形 (固定の文と完全に一致)・DB に接続しない (封じ込めのまま)', async () => {
  process.env.MIRROR_SYNC_KEY = 'k';
  process.env.COMPANY_DB_URL = 'pglite://test';
  let created = 0;
  __setPgClientFactory(async () => { created++; throw new Error('DB に接続した'); });
  const app = express();
  app.use('/apps/company-db/sync', companyDbRouter);
  const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  try {
    for (const kind of ['daily', 'totals']) {
      const res = await fetch(`http://127.0.0.1:${server.address().port}/apps/company-db/sync/amazon-profit/${kind}?mall=amazon&scope=jp&from=2026-06-01&to=2026-06-30`, { headers: { 'x-sync-key': 'k' } });
      const body = await res.json();
      assert.equal(res.status, 503);
      assert.equal(body.code, 'PROFIT_ROUTE_DISABLED');
      assert.deepEqual(validate503Body(body).errors, []);
      // 🚨 no-store は今の router に付いていない = この契約の「router に触れる PR (遅くとも PR 6) の必須の条件」。ここでは今の事実だけを記録する
      assert.notEqual(res.headers.get('cache-control'), REQUIRED_HEADERS['cache-control'], 'router に no-store が付いた = 文書の「PR 6 で付ける」を直す');
    }
  } finally { await new Promise((resolve) => server.close(resolve)); }
  assert.equal(created, 0);
  const src = fs.readFileSync(new URL('../apps/company-db/router.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(src, /response-contract/);   // 使う所はまだ無い (PR 6 で)
});

console.log('そのほか');
await t('monthsOf: 月の境・年の境・1 日・うるう年', async () => {
  assert.deepEqual(monthsOf('2026-12-31', '2027-01-01'), [{ month_start: '2026-12-01', period_from: '2026-12-31', period_to: '2026-12-31' }, { month_start: '2027-01-01', period_from: '2027-01-01', period_to: '2027-01-01' }]);
  assert.deepEqual(monthsOf('2028-02-10', '2028-02-29'), [{ month_start: '2028-02-01', period_from: '2028-02-10', period_to: '2028-02-29' }]);
  assert.equal(monthsOf('2026-01-01', '2026-12-31').length, 12);
});
await t('形の合う実在しない時刻・日付・壊れた入力でも validator は例外を投げず {ok:false} (Low)', async () => {
  const cases = [
    (x) => { x.calculated_at = '2026-99-99T99:99:99.999Z'; x.master_as_of = x.calculated_at; },
    (x) => { x.from = '2026-02-30'; },
    (x) => { x.months[0].calculated_at = '2026-13-40T25:61:61.000Z'; },
    (x) => { x.total = null; }, (x) => { x.months = [null, 1, 'x']; }, (x) => { x.total.master_note_counts = null; },
  ];
  for (const f of cases) { const x = clone(totalsExp); f(x); let r; assert.doesNotThrow(() => { r = validateTotalsResponse(x); }); assert.equal(r.ok, false); }
  for (const f of [(x) => { x.rows[0].calculated_at = '2026-99-99T99:99:99.999Z'; }, (x) => { x.rows = [null, 7]; }, (x) => { x.rows[0].listing_id = '1x'; }, (x) => { x.rows[0].composition_audit_since = '9999-99-99T99:99:99.999Z'; }]) {
    const x = clone(dailyExp); f(x); let r; assert.doesNotThrow(() => { r = validateDailyResponse(x); }); assert.equal(r.ok, false);
  }
  for (const b of [undefined, null, 1, 'x', [], { ok: false, code: 'PROFIT_BUSY', error: { toString: () => { throw new Error('x'); } } }]) {
    let r; assert.doesNotThrow(() => { r = validateTotalsResponse(b); }); assert.equal(r.ok, false);
    assert.doesNotThrow(() => { r = validateDailyResponse(b); }); assert.equal(r.ok, false);
    assert.doesNotThrow(() => { r = validate503Body(b); }); assert.equal(r.ok, false);
  }
});
await t(`触れる暦月は ${MAX_MONTHS} か月まで: 14 か月の応答は validator が拒む (受け口の 400 と 2 重)・13 か月はその理由では拒まない`, async () => {
  const shift = (from, to) => {
    const x = clone(dailyExp); x.from = from; x.to = to; x.rows = [];
    x.months = monthsOf(from, to).map((m) => ({ ...m, finance_status: 'missing', has_finance_rows: false, finance_month_settled: false,
      finance_coverage_generation: null, finance_source_revision: null, finance_coverage_token: crypto.createHash('sha256').update(m.month_start).digest('hex'),
      calculation_version: x.calculation_version, calculated_at: x.calculated_at }));
    return x;
  };
  assert.deepEqual(validateDailyResponse(shift('2025-08-31', '2026-08-01')).errors, []);   // 13 か月 (8 月の端から端)
  const r = validateDailyResponse(shift('2025-08-31', '2026-09-01'));
  assert.equal(r.ok, false);
  assert.ok(r.errors.some((e) => e.includes('上限 13 か月')), r.errors.join(' / '));
});
await t('文書 (docs/contracts) の付録の表 = 契約の部品の分類 (列・型・規則 / null) と同じ・503 の code の表も同じ', async () => {
  const doc = fs.readFileSync(new URL('../docs/contracts/company_db_amazon_profit_response.contract.md', import.meta.url), 'utf8');
  const section = (h) => { const s = doc.indexOf(h); assert.ok(s >= 0, h); const e = doc.indexOf('\n## ', s + 1); return doc.slice(s, e < 0 ? undefined : e); };
  const rows = (text) => [...text.matchAll(/^\| `([a-z_]+)` \| ([^|]+) \| ([^|]+) \|/gm)].map((m) => [m[1], m[2].trim(), m[3].trim()]);
  assert.deepEqual(rows(section('## 5. 付録 A')), TOTALS_COLUMNS.map((c) => [c.name, c.type, `\`${c.rule}\``]));
  assert.deepEqual(rows(section('## 6. 付録 B')).map((r) => [r[0], r[1], r[2]]), DAILY_COLUMNS.map((c) => [c.name, c.type, `\`${c.nul}\``]));
  const s503 = section('## 4. 503');
  assert.deepEqual([...s503.matchAll(/^\| `(PROFIT_[A-Z_]+)` \| (.+?) \|/gm)].map((m) => [m[1], m[2]]), PROFIT_503_CODES.map((c) => [c, PROFIT_503_ERRORS[c]]));
  for (const f of FAILURE_PATHS) assert.ok(section('## 4b.').includes(f.path), f.path);
  // no-store は「PR 6 で付ける」(今の 503 には付いていない = 事実と違うことを書かない・Low)
  assert.match(doc, /no-store[^\n]*PR 6/);
  assert.doesNotMatch(doc, /全部の応答 \(200 も 503 も\) に `Cache-Control: no-store`。/);
});
await t('fixture に秘密らしい文字が無い (鍵・接続の文字列・Bearer)', async () => {
  for (const f of fs.readdirSync(FIX)) {
    const text = fs.readFileSync(new URL(f, FIX), 'utf8');
    assert.doesNotMatch(text, /rnd_[A-Za-z0-9]{8,}|RENDER_API_KEY|postgres(ql)?:\/\/|Bearer |x-sync-key/i, f);
  }
  // 🆕 v2 (PR 2c): 版を上げた・版の履歴の最後 = 今の版・fixture の応答も同じ版
  assert.equal(CONTRACT_VERSION, 'amazon_profit_response_v2');
  assert.deepEqual(CONTRACT_HISTORY.map((h) => h.version), ['amazon_profit_response_v1', 'amazon_profit_response_v2']);
  assert.equal(CONTRACT_HISTORY.at(-1).version, CONTRACT_VERSION);
  for (const x of [totalsExp, dailyExp]) assert.equal(x.contract, CONTRACT_VERSION);
});

console.log('🆕 v2 (PR 2c): months[].finance_coverage_token');
await t('fixture の全部の月に finance_coverage_token (64 桁の小文字の 16 進・財務の行が 0 の月にも・月ごとに違う)・月の metadata の値をそのまま', async () => {
  for (const [exp, inp] of [[totalsExp, totalsIn], [dailyExp, dailyIn]]) {
    const toks = exp.months.map((m) => m.finance_coverage_token);
    assert.ok(toks.every((x) => /^[0-9a-f]{64}$/.test(x)), JSON.stringify(toks));
    assert.equal(new Set(toks).size, toks.length);
    assert.deepEqual(toks, inp.months.map((m) => m.meta.finance_coverage_token));
    assert.ok(exp.months.some((m) => m.has_finance_rows === false && m.finance_coverage_token), '財務の行が 0 の月にも token');
  }
  assert.equal(MONTH_KEYS.indexOf('finance_coverage_token'), MONTH_KEYS.indexOf('finance_source_revision') + 1);
  // 世代と版が null の月 (月の途中で source が 2 つ) でも token はある = 無効化の判定は token で
  const x = clone(totalsExp); x.months[0].finance_coverage_generation = null; x.months[0].finance_source_revision = null;
  assert.deepEqual(validateTotalsResponse(x).errors, []);
});
const breakToken = [
  ['token が無い', (m) => { delete m[0].finance_coverage_token; }],
  ['token が null', (m) => { m[0].finance_coverage_token = null; }],
  ['token が大文字', (m) => { m[0].finance_coverage_token = m[0].finance_coverage_token.toUpperCase(); }],
  ['token が 63 桁', (m) => { m[0].finance_coverage_token = m[0].finance_coverage_token.slice(1); }],
  ['token が 65 桁', (m) => { m[0].finance_coverage_token += '0'; }],
  ['token が 16 進でない', (m) => { m[0].finance_coverage_token = 'g'.repeat(64); }],
  ['token が数', (m) => { m[0].finance_coverage_token = 12345; }],
  ['token が空', (m) => { m[0].finance_coverage_token = ''; }],
  ['token が object (部品の配列をそのまま出した)', (m) => { m[0].finance_coverage_token = { components: [] }; }],
  ['2 つの月で同じ token (写し間違い)', (m) => { m[1].finance_coverage_token = m[0].finance_coverage_token; }],
  ['財務の行が 0 の月の token が null', (m) => { m[m.length - 1].finance_coverage_token = null; }],
];
await t(`finance_coverage_token の違反 ${breakToken.length} 通りを /totals・/daily の両方で拒む`, async () => {
  for (const [name, f] of breakToken) {
    const a = clone(totalsExp); f(a.months);
    assert.ok(validateTotalsResponse(a).errors.some((e) => e.includes('finance_coverage_token')), `totals: ${name}`);
    const b = clone(dailyExp); f(b.months);
    assert.ok(validateDailyResponse(b).errors.some((e) => e.includes('finance_coverage_token')), `daily: ${name}`);
  }
});

console.log('🆕 v2 (PR 2c): 日の行の member_seller_skus');
const FW_SPACE = String.fromCodePoint(0x3000), NBSP = String.fromCodePoint(0xa0), BOM = String.fromCodePoint(0xfeff), TAB = String.fromCodePoint(9);
await t('fixture の全部の行に member_seller_skus・財務の行がある (order_rows > 0) 行は空でない・全角の SKU も bytes の順で同じ粒度に', async () => {
  for (const r of dailyExp.rows) {
    assert.ok(Array.isArray(r.member_seller_skus) && r.member_seller_skus.length > 0 && r.order_rows > 0, r.listing_code);
    assert.deepEqual(memberSkuErrors(r.member_seller_skus), []);
  }
  assert.deepEqual(dailyExp.rows[0].member_seller_skus, ['ab-001', 'ａｂ-００１']);   // locale の比べでも同じだが bytes で確かめる (半角 0x61 < 全角 0xEF)
  assert.deepEqual(dailyExp.rows[1].member_seller_skus, ['zz-unknown']);              // 未解決の行にも (seller_sku_norm の粒度)
  assert.deepEqual(DAILY_COLUMNS.map((c) => c.name).slice(DAILY_COLUMNS.findIndex((c) => c.name === 'listing_code'), DAILY_COLUMNS.findIndex((c) => c.name === 'listing_code') + 2), ['listing_code', 'member_seller_skus']);
  assert.equal(MAX_MEMBER_SELLER_SKUS, 100);
  assert.equal(MAX_MEMBER_SKU_CHARS, 255);
});
await t('member_seller_skus の受け取るべき形: 財務の行が無い行 (広告だけ・Easy Ship だけ) は [] ・上限ちょうど・255 文字・bytes の順 (locale の順でない)・別の日なら同じ SKU', async () => {
  const ok = [
    ['財務の行が無い行は []', (x) => { x.rows[3].order_rows = 0; x.rows[3].source_lines = 0; x.rows[3].member_seller_skus = []; }],
    ['100 個ちょうど', (x) => { x.rows[0].member_seller_skus = Array.from({ length: 100 }, (_, i) => `ab-001-${String(i).padStart(3, '0')}`); }],
    ['255 文字ちょうど (全角も 1 文字)', (x) => { x.rows[0].member_seller_skus = ['a'.repeat(254) + 'ａ']; }],
    ['bytes の順 = f (0x66) < é (0xC3) (locale の比べなら é が先)', (x) => { x.rows[0].member_seller_skus = ['f', 'é']; }],
    ['中の空白はそのまま (trim は前後だけ)', (x) => { x.rows[0].member_seller_skus = ['ab 001']; }],
    ['別の日の行なら同じ SKU', (x) => { x.rows[2].member_seller_skus = ['ab-001']; }],
    // 🆕 #1615 R1 Low: 「小文字」の保証は ASCII の英字 (A-Z) だけ = 非 ASCII の大文字 (全角の Ａ・É・Σ) は縛らない
    ['非 ASCII の大文字 (全角の ＡＢ) は縛らない', (x) => { x.rows[0].member_seller_skus = ['ab-001', 'ＡＢ-００１']; }],
    ['非 ASCII の大文字 (É・Σ) は縛らない', (x) => { x.rows[0].member_seller_skus = ['ab-É', 'ab-Σ']; }],
  ];
  assert.ok('é'.localeCompare('f') < 0, 'locale の比べでは é が先 (= bytes の順と違う例になっている)');
  for (const [name, f] of ok) { const x = clone(dailyExp); f(x); assert.deepEqual(validateDailyResponse(x).errors, [], name); }
});
const breakMember = [
  ['列が無い', (x) => { delete x.rows[0].member_seller_skus; }],
  ['null', (x) => { x.rows[0].member_seller_skus = null; }],
  ['配列でない (文字)', (x) => { x.rows[0].member_seller_skus = 'ab-001'; }],
  ['要素が数', (x) => { x.rows[0].member_seller_skus = [1]; }],
  ['要素が null', (x) => { x.rows[0].member_seller_skus = [null]; }],
  ['要素が空の文字', (x) => { x.rows[0].member_seller_skus = ['']; }],
  ['大文字 (小文字にしていない)', (x) => { x.rows[0].member_seller_skus = ['AB-001']; }],
  ['ASCII の大文字が 1 つだけ混ざる (全角の英字の中)', (x) => { x.rows[0].member_seller_skus = ['ab-001', 'ａｂ-００１X']; }],
  ['前に半角の空白', (x) => { x.rows[0].member_seller_skus = [' ab-001']; }],
  ['後ろに半角の空白', (x) => { x.rows[0].member_seller_skus = ['ab-001 ']; }],
  ['後ろに全角の空白', (x) => { x.rows[0].member_seller_skus = ['ab-001' + FW_SPACE]; }],
  ['前に NBSP', (x) => { x.rows[0].member_seller_skus = [NBSP + 'ab-001']; }],
  ['前に BOM', (x) => { x.rows[0].member_seller_skus = [BOM + 'ab-001']; }],
  ['後ろに tab', (x) => { x.rows[0].member_seller_skus = ['ab-001' + TAB]; }],
  ['順が違う', (x) => { x.rows[0].member_seller_skus = ['ａｂ-００１', 'ab-001']; }],
  ['locale の順 (é が f より前)', (x) => { x.rows[0].member_seller_skus = ['é', 'f']; }],
  ['重複', (x) => { x.rows[0].member_seller_skus = ['ab-001', 'ab-001']; }],
  ['101 個 (上限超え)', (x) => { x.rows[0].member_seller_skus = Array.from({ length: 101 }, (_, i) => `ab-001-${String(i).padStart(3, '0')}`); }],
  ['256 文字', (x) => { x.rows[0].member_seller_skus = ['a'.repeat(256)]; }],
  ['財務の行があるのに []', (x) => { x.rows[0].member_seller_skus = []; }],
  ['財務の行が無いのに空でない', (x) => { x.rows[3].order_rows = 0; x.rows[3].source_lines = 0; }],
  ['同じ日の 2 つの行に同じ SKU (1 つの SKU が 2 つの粒度に)', (x) => { x.rows[1].member_seller_skus = ['ab-001']; }],
  ['同じ日の別の行の SKU を写した', (x) => { x.rows[4].member_seller_skus = ['ab-003', 'ab-004']; }],
  ['要素が object (財務の値を混ぜた)', (x) => { x.rows[0].member_seller_skus = [{ seller_sku: 'ab-001', net_jpy: '2625' }]; }],
];
await t(`member_seller_skus の違反 ${breakMember.length} 通りを全部拒む`, async () => {
  for (const [name, f] of breakMember) {
    const x = clone(dailyExp); f(x);
    const v = validateDailyResponse(x);
    assert.equal(v.ok, false, name);
    assert.ok(v.errors.some((e) => e.includes('member_seller_skus')), `${name}: ${v.errors.join(' / ')}`);
  }
});

console.log('🆕 v2 (PR 2c): 503 の failed_months');
await t('PROFIT_PARTIAL_FAILED の 3 つの reason で failed_months つきの本文が通る・build503Body は並べ替えと重複の除き・要求の暦月の中', async () => {
  for (const reason of PROFIT_503_REASONS.PROFIT_PARTIAL_FAILED) {
    const b = build503Body('PROFIT_PARTIAL_FAILED', reason, ['2026-08', '2026-07', '2026-08']);
    assert.deepEqual(b, { ok: false, code: 'PROFIT_PARTIAL_FAILED', error: PROFIT_503_ERRORS.PROFIT_PARTIAL_FAILED, reason, failed_months: ['2026-07', '2026-08'] });
    assert.deepEqual(validate503Body(b).errors, [], reason);
    assert.deepEqual(validate503Body(b, { from: '2026-07-30', to: '2026-08-02' }).errors, [], reason);
    assert.ok(validate503Body(b, { from: '2026-07-01', to: '2026-07-31' }).errors.some((e) => e.includes('2026-08 は要求の触れる暦月の外')));
  }
  const thirteen = monthsOf('2025-08-31', '2026-08-01').map((m) => m.month_start.slice(0, 7));
  assert.equal(thirteen.length, MAX_MONTHS);
  assert.deepEqual(validate503Body(build503Body('PROFIT_PARTIAL_FAILED', 'MONTH_CALC_FAILED', thirteen)).errors, []);
  assert.deepEqual([...PROFIT_503_BODY_KEYS], ['ok', 'code', 'error', 'reason', 'failed_months']);
  assert.equal(FAILED_MONTHS_CODE, 'PROFIT_PARTIAL_FAILED');
});
await t('🚨 build503Body: failed_months に値・例外の文・秘密が混ざる / 空 / 14 個 = PROFIT_INTERNAL に落とし、月も値も出さない・ほかの code には付けない', async () => {
  const INTERNAL = { ok: false, code: 'PROFIT_INTERNAL', error: PROFIT_503_ERRORS.PROFIT_INTERNAL, reason: 'APP_UNEXPECTED' };
  const fourteen = monthsOf('2025-07-31', '2026-08-01').map((m) => m.month_start.slice(0, 7));
  assert.equal(fourteen.length, 14);
  for (const fm of [undefined, null, [], '2026-07', [202607], ['2026-07', 'net_jpy=2625'], ['2026-07: -1500.01'], ['2026-07-01'], ['2026-7'], ['2026-13'],
    [{ month: '2026-07' }], ['postgres://u:p@h/db'], fourteen]) {
    const b = build503Body('PROFIT_PARTIAL_FAILED', 'MONTH_CALC_FAILED', fm);
    assert.deepEqual(b, INTERNAL, JSON.stringify(fm));
    assert.deepEqual(validate503Body(b).errors, []);
  }
  for (const c of PROFIT_503_CODES.filter((x) => x !== FAILED_MONTHS_CODE)) assert.ok(!('failed_months' in build503Body(c, PROFIT_503_REASONS[c][0], ['2026-07'])), c);
});
const PF = (fm) => ({ ok: false, code: 'PROFIT_PARTIAL_FAILED', error: PROFIT_503_ERRORS.PROFIT_PARTIAL_FAILED, reason: 'MONTH_CALC_FAILED', ...(fm === undefined ? {} : { failed_months: fm }) });
const breakFailed = [
  ['PROFIT_PARTIAL_FAILED に failed_months が無い', PF(undefined)],
  ['failed_months が空', PF([])],
  ['failed_months が null', PF(null)],
  ['failed_months が文字', PF('2026-07')],
  ['要素が数', PF([202607])],
  ['要素が日付', PF(['2026-07-01'])],
  ['要素の月が 1 桁', PF(['2026-7'])],
  ['13 月', PF(['2026-13'])],
  ['値が混ざる (金額)', PF(['2026-07', '2625'])],
  ['値が混ざる (月 + 金額の文字)', PF(['2026-07: net_jpy=2625'])],
  ['値が混ざる (object)', PF([{ month: '2026-07', net_jpy: '2625' }])],
  ['値が混ざる (例外の文)', PF(['canceling statement due to statement timeout'])],
  ['重複', PF(['2026-07', '2026-07'])],
  ['降順', PF(['2026-08', '2026-07'])],
  ['14 個', PF(monthsOf('2025-07-31', '2026-08-01').map((m) => m.month_start.slice(0, 7)))],
  ['秘密 (接続の文字列)', PF(['postgres://cdb:p4ss@dpg-x/cdb'])],
  ['ほかの code に failed_months', { ok: false, code: 'PROFIT_RESOURCE', error: PROFIT_503_ERRORS.PROFIT_RESOURCE, reason: 'RESOURCE_LOAD_COUNT_TIME', failed_months: ['2026-07'] }],
  ['封じ込めの 503 に failed_months', { ok: false, code: 'PROFIT_ROUTE_DISABLED', error: PROFIT_503_ERRORS.PROFIT_ROUTE_DISABLED, failed_months: ['2026-07'] }],
];
await t(`503 の failed_months の違反 ${breakFailed.length} 通りを全部拒む`, async () => {
  for (const [name, b] of breakFailed) assert.equal(validate503Body(b).ok, false, name);
});
await t('🆕 #1615 R1 M2: PROFIT_PARTIAL_FAILED は reason が必須 = 無い・不正の reason の本文を拒み、build503Body は PROFIT_INTERNAL / APP_UNEXPECTED に落とす', async () => {
  assert.deepEqual([...REASON_REQUIRED_CODES], ['PROFIT_PARTIAL_FAILED']);
  const INTERNAL = { ok: false, code: 'PROFIT_INTERNAL', error: PROFIT_503_ERRORS.PROFIT_INTERNAL, reason: 'APP_UNEXPECTED' };
  const BAD_REASONS = [undefined, null, '', 'month_calc_failed', 'MONTH_FAILED', 'DB_CONNECT', 'APP_UNEXPECTED', 'RESOURCE_TRANSACTION_TIMEOUT', 1, ['MONTH_CALC_FAILED'],
    { reason: 'MONTH_CALC_FAILED' }, 'canceling statement due to statement timeout', 'toString', '__proto__'];
  for (const r of BAD_REASONS) {
    const b = build503Body('PROFIT_PARTIAL_FAILED', r, ['2026-07']);
    assert.deepEqual(b, INTERNAL, String(r));
    assert.ok(!('failed_months' in b), String(r));
    assert.deepEqual(validate503Body(b).errors, [], String(r));
  }
  // 本文の側: reason が無い (Codex R1 の例そのもの)・null・不正・別の code の reason・小文字 = 拒む
  const body = (extra) => ({ ok: false, code: 'PROFIT_PARTIAL_FAILED', error: PROFIT_503_ERRORS.PROFIT_PARTIAL_FAILED, ...extra, failed_months: ['2026-07'] });
  const noReason = body({});
  assert.deepEqual(Object.keys(noReason), ['ok', 'code', 'error', 'failed_months']);
  assert.ok(validate503Body(noReason).errors.some((e) => e.startsWith('$.reason: PROFIT_PARTIAL_FAILED では必須')), JSON.stringify(validate503Body(noReason).errors));
  for (const r of BAD_REASONS.filter((x) => x !== undefined)) assert.equal(validate503Body(body({ reason: r })).ok, false, String(r));
  // 3 つの正しい reason は通る (requestつきでも)
  for (const r of PROFIT_503_REASONS.PROFIT_PARTIAL_FAILED) assert.deepEqual(validate503Body(body({ reason: r }), { from: '2026-07-01', to: '2026-07-31' }).errors, [], r);
  // ほかの code は今までどおり reason なしでも通る (必須にしたのは PROFIT_PARTIAL_FAILED だけ)
  for (const c of PROFIT_503_CODES.filter((x) => x !== FAILED_MONTHS_CODE)) assert.deepEqual(validate503Body(build503Body(c)).errors, [], c);
});

console.log('🆕 v2 (PR 2c): 57014 / 25P04 の分け方 (CANCEL_MAP)');
const cancelCases = readFix('cancel-cases.json').cases;
const outcome = (r) => r && { code: r.code, reason: r.reason, respond: r.respond, send_rollback: r.send_rollback, failed_months: r.failed_months, log: r.log };
await t(`取り消しの golden ${cancelCases.length} 件 = classifyCancellation の結果 (表の全部の行・印の優先・表に無い組・取り消しでない)`, async () => {
  for (const c of cancelCases) assert.deepEqual(outcome(classifyCancellation(c.input)), c.expect, c.name);
});
await t('CANCEL_MAP の全部の行と「表に無い組」が golden で 1 回以上当たる・code / reason は 503 の列挙と FAILURE_PATHS にある', async () => {
  const hit = new Set(cancelCases.map((c) => classifyCancellation(c.input)).filter(Boolean));
  for (const e of [...CANCEL_MAP, CANCEL_UNCLASSIFIED]) assert.ok(hit.has(e), `golden に当たらない行: ${JSON.stringify(e)}`);
  for (const e of [...CANCEL_MAP, CANCEL_UNCLASSIFIED]) {
    assert.ok(e.stage === '*' || CANCEL_STAGES.includes(e.stage), e.stage);
    assert.ok(e.source === '*' || CANCELLATION_SOURCES.includes(e.source), e.source);
    assert.ok(e.sqlstate === '*' || e.sqlstate === null || CANCEL_SQLSTATES.includes(e.sqlstate), String(e.sqlstate));
    if (!e.respond) { assert.equal(e.code, null); assert.equal(e.source, 'client_disconnect'); continue; }
    assert.ok(PROFIT_503_REASONS[e.code].includes(e.reason), `${e.code} / ${e.reason}`);
    assert.ok(FAILURE_PATHS.some((f) => f.code === e.code && f.reason === e.reason), `FAILURE_PATHS に無い: ${e.code} / ${e.reason}`);
    assert.equal(e.failed_months, e.code === FAILED_MONTHS_CODE, e.reason);
  }
  // 新しい reason 4 つは全部 CANCEL_MAP か「表に無い組」から出る
  for (const r of ['RESOURCE_LOAD_COUNT_TIME', 'RESOURCE_TRANSACTION_TIMEOUT', 'INTERNAL_EXTERNAL_CANCEL', 'INTERNAL_UNCLASSIFIED_CANCEL']) {
    assert.ok([...CANCEL_MAP, CANCEL_UNCLASSIFIED].some((e) => e.reason === r), r);
  }
  // 25P04 だけ ROLLBACK を送らない・client_disconnect だけ応答を作らない
  assert.deepEqual(CANCEL_MAP.filter((e) => !e.send_rollback).map((e) => e.sqlstate), ['25P04']);
  assert.deepEqual(CANCEL_MAP.filter((e) => !e.respond).map((e) => e.source), ['client_disconnect']);
  assert.deepEqual([...CANCELLATION_SOURCES], ['app_statement_budget', 'app_deadline', 'client_disconnect', 'unmarked']);
});
await t('印の優先: 経過時間は分け方に使わない・印があれば例外が無くても 503・印が無ければ期限の直前でも unmarked', async () => {
  // classifyCancellation は経過時間を受け取らない (渡しても同じ結果)
  for (const c of cancelCases) {
    const a = classifyCancellation(c.input), b = classifyCancellation({ ...c.input, elapsed_ms: 0 }), d = classifyCancellation({ ...c.input, elapsed_ms: 1e9 });
    assert.equal(a, b, c.name); assert.equal(a, d, c.name);
  }
  for (const st of CANCEL_STAGES) {
    for (const src of ['app_statement_budget', 'app_deadline']) {
      const r = classifyCancellation({ stage: st, sqlstate: null, mark: { source: src, stage: st } });
      assert.ok(r && r.respond && r.code, `${st} / ${src}: 印を書いた要求は 503`);
    }
    assert.equal(classifyCancellation({ stage: st, sqlstate: null, mark: null }), null, `${st}: 印も例外も無ければ取り消しでない`);
  }
  assert.equal(classifyCancellation({ stage: 'month_body', sqlstate: '57014', mark: null }).source, 'unmarked');
  // 応答を作る結果は全部、安全な固定の 503 の本文になる (failed_months つきは止まった月で)
  for (const c of cancelCases) {
    const r = classifyCancellation(c.input);
    if (!r || !r.respond) continue;
    const b = build503Body(r.code, r.reason, r.failed_months ? ['2026-07'] : undefined);
    assert.deepEqual([b.code, b.reason], [r.code, r.reason], c.name);
    assert.deepEqual(validate503Body(b, { from: '2026-07-30', to: '2026-08-02' }).errors, [], c.name);
  }
  // 壊れた入力でも例外を投げない (表に無い組 か 取り消しでない)
  for (const x of [undefined, {}, { stage: null, sqlstate: '57014' }, { stage: 'load_count', sqlstate: '57014', mark: {} }, { stage: 'load_count', sqlstate: '57014', mark: { source: 1 } }]) {
    let r; assert.doesNotThrow(() => { r = classifyCancellation(x); });
    assert.ok(r === null || r === CANCEL_UNCLASSIFIED, JSON.stringify(x));
  }
});
await t('🆕 #1615 R1 M1: respond と send_rollback は別々 = 25P04 は段・印 (無い・正しい・知らない・壊れた) に関係なく ROLLBACK なし / 印 client_disconnect は sqlstate に関係なく応答なし', async () => {
  assert.deepEqual([...NO_ROLLBACK_SQLSTATES], ['25P04']);
  const MARKS = [null, ...CANCELLATION_SOURCES.map((source) => ({ source })), { source: 'timer' }, { source: 1 }, {}, { source: 'app_statement_budget', stage: 'somewhere' }];
  const STAGES = [...CANCEL_STAGES, 'somewhere', undefined];
  let n = 0;
  for (const stage of STAGES) for (const m of MARKS) for (const sqlstate of [...CANCEL_SQLSTATES, null, 'D6L01', '40001']) {
    const mark = m && m.source !== undefined && !m.stage && stage ? { ...m, stage } : m;
    const x = { stage, sqlstate, mark }, r = classifyCancellation(x), label = JSON.stringify(x);
    if (sqlstate === '25P04') { assert.ok(r, `${label}: 25P04 は必ず取り消しの結果`); assert.equal(r.send_rollback, false, label); n++; }
    else if (r) assert.equal(r.send_rollback, true, label);   // 25P04 でなければ ROLLBACK を送る (表の行の値)
    if (r) assert.equal(r.respond, mark?.source !== 'client_disconnect', label);
    if (mark?.source === 'client_disconnect') assert.deepEqual([r.respond, r.code, r.reason, r.log], [false, null, null, 'client_disconnect'], label);
    if (r) { assert.ok(Object.isFrozen(r), label); assert.equal(classifyCancellation({ ...x }), r, `${label}: 同じ入力には同じ参照`); }
    // 「送らない」版は元の行と send_rollback だけが違う
    if (r && !CANCEL_MAP.includes(r) && r !== CANCEL_UNCLASSIFIED) {
      const base = [...CANCEL_MAP, CANCEL_UNCLASSIFIED].find((e) => e.log === r.log && e.code === r.code && e.reason === r.reason && e.respond === r.respond);
      assert.ok(base, label); assert.deepEqual({ ...r, send_rollback: true }, { ...base, send_rollback: true }, label);
    }
  }
  assert.equal(n, STAGES.length * MARKS.length, `25P04 の組 ${n}`);   // 段 7 × 印 9 の全部の組
  // Codex R1 の 2 つの組そのもの
  assert.deepEqual(outcome(classifyCancellation({ stage: 'month_body', sqlstate: '25P04', mark: { source: 'client_disconnect', stage: 'month_body' } })),
    { code: null, reason: null, respond: false, send_rollback: false, failed_months: false, log: 'client_disconnect' });
  assert.deepEqual(outcome(classifyCancellation({ stage: 'month_body', sqlstate: '25P04', mark: { source: 'timer', stage: 'month_body' } })),
    { code: 'PROFIT_INTERNAL', reason: 'INTERNAL_UNCLASSIFIED_CANCEL', respond: true, send_rollback: false, failed_months: false, log: 'unclassified_cancel' });
  // 表の行そのものは変えない (送らない版は別の object)
  assert.equal(CANCEL_UNCLASSIFIED.send_rollback, true);
  assert.equal(CANCEL_MAP.find((e) => e.source === 'client_disconnect').send_rollback, true);
});
console.log('🆕 #1615 Codex R2 Low: 疎な配列の穴 / object でない取り消しの入力');
// 疎な配列 (穴 = hole) を作る。every / forEach は穴を飛ばすが、JSON にすると null になる
const hasHole = (a) => { for (let i = 0; i < Math.min(a.length, 16); i++) if (!(i in a)) return true; return false; };
const sparse = (len, at) => { const a = []; a.length = len; for (const [i, v] of Object.entries(at)) a[Number(i)] = v; return a; };
await t('🆕 R2 Low 1: build503Body は疎な配列の穴も 1 つの要素として見る = 穴が 1 つでもあれば PROFIT_INTERNAL / APP_UNEXPECTED (月も値も出さない)', async () => {
  const INTERNAL = { ok: false, code: 'PROFIT_INTERNAL', error: PROFIT_503_ERRORS.PROFIT_INTERNAL, reason: 'APP_UNEXPECTED' };
  const holes = [
    ['後ろに穴 (Codex R2 の例そのもの)', sparse(2, { 0: '2026-07' })],
    ['前に穴', sparse(2, { 1: '2026-07' })],
    ['間に穴', sparse(3, { 0: '2026-07', 2: '2026-08' })],
    ['全部穴', sparse(3, {})],
    ['length だけ大きい (2^32-1)', sparse(2 ** 32 - 1, { 0: '2026-07' })],
  ];
  for (const [name, fm] of holes) {
    assert.ok(Array.isArray(fm) && hasHole(fm), `${name}: 試験の配列に穴がある`);
    for (const reason of PROFIT_503_REASONS.PROFIT_PARTIAL_FAILED) {
      const b = build503Body('PROFIT_PARTIAL_FAILED', reason, fm);
      assert.deepEqual(b, INTERNAL, `${name} / ${reason}`);
      assert.ok(!('failed_months' in b) && !JSON.stringify(b).includes('2026-07'), name);
      assert.deepEqual(validate503Body(b).errors, [], name);
      assert.deepEqual(validate503Body(JSON.parse(JSON.stringify(b))).errors, [], `${name}: JSON を通しても`);
    }
  }
  // 直す前に作れた本文 (JSON で ["2026-07", null]) は validate503Body が拒む本文そのもの = 作る側と確かめる側をそろえた
  assert.equal(validate503Body(PF(['2026-07', null])).ok, false);
  // 穴の無い同じ月の配列は今までどおり通る (並べ替えと重複の除き)
  assert.deepEqual(build503Body('PROFIT_PARTIAL_FAILED', 'MONTH_CALC_FAILED', ['2026-08', '2026-07', '2026-07']).failed_months, ['2026-07', '2026-08']);
});
await t('🆕 R2 Low 1: validate503Body も疎な配列の穴を飛ばさない (メモリの上の本文でも JSON を通した本文でも拒む)', async () => {
  for (const [name, fm] of [['後ろに穴', sparse(2, { 0: '2026-07' })], ['前に穴', sparse(2, { 1: '2026-07' })], ['間に穴', sparse(3, { 0: '2026-07', 2: '2026-08' })],
    ['1 つで穴', sparse(1, {})], ['14 個の疎な配列', sparse(14, { 0: '2026-07' })]]) {
    const body = PF(fm);
    const v = validate503Body(body);
    assert.equal(v.ok, false, name);
    if (fm.length <= MAX_MONTHS) assert.ok(v.errors.some((e) => /^\$\.failed_months\[\d+\]: 'YYYY-MM' の形でない/.test(e)), `${name}: ${v.errors.join(' / ')}`);
    assert.equal(validate503Body(JSON.parse(JSON.stringify(body))).ok, false, `${name}: JSON を通しても`);
    assert.equal(validate503Body(body, { from: '2026-07-01', to: '2026-08-31' }).ok, false, `${name}: 要求つきでも`);
  }
});
await t('🆕 R2 Low 2: classifyCancellation は object でない入力 (null・undefined・配列・文字・数・関数) で例外を投げず「表に無い組」= 知らない印と同じ', async () => {
  const unknownMark = classifyCancellation({ stage: 'month_body', sqlstate: '57014', mark: { source: 'timer', stage: 'month_body' } });
  assert.equal(unknownMark, CANCEL_UNCLASSIFIED);
  for (const x of [null, undefined, [], [{ stage: 'month_body', sqlstate: '57014' }], '57014', '', 0, 1, true, false, () => ({}), 10n, Symbol('x')]) {
    const label = typeof x === 'symbol' ? 'symbol' : typeof x === 'bigint' ? `${x}n` : JSON.stringify(x) ?? String(x);
    let r; assert.doesNotThrow(() => { r = classifyCancellation(x); }, label);
    assert.equal(r, CANCEL_UNCLASSIFIED, label);   // 同じ参照 (凍結した 1 つの行)
    assert.deepEqual(outcome(r), outcome(unknownMark), label);
    assert.deepEqual([r.respond, r.send_rollback, r.code, r.reason], [true, true, 'PROFIT_INTERNAL', 'INTERNAL_UNCLASSIFIED_CANCEL'], label);
    assert.deepEqual(validate503Body(build503Body(r.code, r.reason)).errors, [], label);
  }
  // 引数なしも同じ (既定の {} で「取り消しでない」の null にしない)
  let r0; assert.doesNotThrow(() => { r0 = classifyCancellation(); });
  assert.equal(r0, CANCEL_UNCLASSIFIED);
  // object の入力は今までどおり (取り消しでない = null・表の行)
  assert.equal(classifyCancellation({}), null);
  assert.equal(classifyCancellation({ stage: 'month_body', sqlstate: 'D6L01', mark: null }), null);
  assert.equal(classifyCancellation({ stage: 'month_body', sqlstate: '57014', mark: null }).log, 'external_cancel');
});

await t('文書 (docs/contracts) に v2 の項目: months の token・§4c の取り消しの表の全部のログの理由・§8 の版の履歴', async () => {
  const doc = fs.readFileSync(new URL('../docs/contracts/company_db_amazon_profit_response.contract.md', import.meta.url), 'utf8');
  const section = (h) => { const s = doc.indexOf(h); assert.ok(s >= 0, h); const e = doc.indexOf('\n## ', s + 1); return doc.slice(s, e < 0 ? undefined : e); };
  for (const k of MONTH_KEYS) assert.ok(section('## 3. months[]').includes(`\`${k}\``), k);
  const s4c = section('## 4c.');
  for (const e of [...CANCEL_MAP, CANCEL_UNCLASSIFIED]) assert.ok(s4c.includes(`\`${e.log}\``), e.log);
  for (const s of [...CANCEL_STAGES, ...CANCELLATION_SOURCES]) assert.ok(s4c.includes(`\`${s}\``), s);
  assert.deepEqual([...section('## 8. 版の履歴').matchAll(/^\| `(amazon_profit_response_v\d+)` \|/gm)].map((m) => m[1]), CONTRACT_HISTORY.map((h) => h.version));
  assert.ok(doc.includes(`\`contract\` = \`${CONTRACT_VERSION}\``));
  assert.ok(section('## 4. 503').includes('failed_months'));
  // 🆕 #1615 Codex R1: 文書と実装の意味を 1 つに (M1 = 25P04 は表のどの行でも ROLLBACK を送らない / M2 = reason が必須 / Low = ASCII の英字だけ)
  assert.ok(s4c.includes('`NO_ROLLBACK_SQLSTATES`') && s4c.includes('respond と send_rollback は別々に決める'), '§4c の M1');
  assert.ok(!s4c.includes('送るか接続を捨てる'), '§4c: client_disconnect の ROLLBACK の曖昧な書き方を残さない');
  assert.ok(section('## 4. 503').includes('`reason` も **必須**'), '§4 の M2');
  for (const h of ['## 6.', '## 7.']) assert.ok(section(h).includes('ASCII の英字だけ'), `${h} の Low`);
  assert.ok(!/trim \+ 小文字・UTF-8/.test(doc), '「小文字」をただし書きなしで書かない');
  // 🆕 #1615 Codex R2 Low: 疎な配列の穴 (§4) と object でない取り消しの入力 (§4c)
  assert.ok(section('## 4. 503').includes('穴 (hole) も 1 つの要素'), '§4 の R2 Low 1');
  assert.ok(s4c.includes('入力そのものが object でない'), '§4c の R2 Low 2');
});

console.log(`\n${ok} 件 PASS${ng ? ` / ${ng} 件 NG` : ''}`);
// 🚨 fetch の直後に process.exit() すると Windows の Node で libuv の assertion が出て終了コードが 127 になる = exitCode を置いて自然に終わらせる (保険に unref つきの setTimeout)
process.exitCode = ng ? 1 : 0;
setTimeout(() => process.exit(ng ? 1 : 0), 10000).unref();
