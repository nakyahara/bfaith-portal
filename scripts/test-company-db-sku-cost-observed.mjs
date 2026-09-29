#!/usr/bin/env node
/**
 * test-company-db-sku-cost-observed.mjs — 観測の原価 (D7b-2。設計 = AI_reference CompanyDB構想/13 §3.3・§3.4・D-57) の試験。
 *   ① 期間の作り方 (buildObservedPeriods = 純粋な関数) ② 商品コードの結びつけ (衝突の隔離・結びつかない) と checksum の共通の部品
 *   ③ 受け口 (PGlite の 0046 に直に: 世代 stale / same / 409 / 入れ替え・見出しは追記だけ・読む口の境目) ④ 本物の router を HTTP で + 送り手 (メモリの SQLite と台帳)
 *   ⑤ CLI (送信の失敗で exit 1 = daily-sync の工程が失敗) と daily-sync / retry / 台帳の配線
 * 🚨 試験に無いもの: 2 接続の並行 (advisory lock。PGlite は 1 接続)・本番の件数 (約 2 万行) での所要時間
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import Database from 'better-sqlite3';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';
import { canonicalJsonStrict, canonicalSha256 } from '../apps/company-db/canonical-hash.mjs';
import { ingestSkuCostObserved, validateObservedBody, observedChecksum, observedRowOf, skuCostObservedStatus } from '../apps/company-db/ingest/sku-cost-observed.mjs';
import { buildObservedPeriods, planPayload, pushSkuCostObserved, parseArgs, changedAtIso, KIND, META_PENDING } from '../apps/company-db/push/sku-cost-observed.mjs';
import { openLedger, LEDGER_FILE } from '../apps/company-db/push/ledger.mjs';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// ─── ① 期間の作り方 ───
const BL = '2026-05-04 20:00:00';   // 最初の写し = UTC 5/4 20:00 = JST 5/5 05:00 → 写しの日 2026-05-05
let hidSeq = 0;
const H = (code, at, op, cost, status = 'COMPLETE', hid = null) => ({ history_id: hid ?? ++hidSeq, 商品コード: code, 原価: cost, 原価ソース: 'NE', 原価状態: status, changed_at: at, operation: op });
const tup = (rows) => rows.map((r) => [r.product_code, r.cost_jpy, r.cost_status, r.valid_from, r.valid_to, r.backfill_method === 'estimated_before_first_snapshot' ? 'est' : 'obs', r.source_history_id]);
const periodsOf = (rows, code) => tup(buildObservedPeriods(rows).periods.filter((p) => p.product_code === code));

console.log('① 期間の作り方 (buildObservedPeriods)');
await t('最初の写しはその日 (JST) から observed・それより前は同じ値を 2026-01-01 から推定 (終わり = 写しの日の前日)・first_observed_at = 写しの時刻 (UTC)', () => {
  const b = buildObservedPeriods([H('A', BL, 'BASELINE_RESET', 100, 'COMPLETE', 1)]);
  assert.deepEqual([b.baselineAt, b.baselineDay, b.estimatedRows, b.ignoredBeforeBaseline], ['2026-05-04T20:00:00Z', '2026-05-05', 1, 0]);
  assert.deepEqual(tup(b.periods), [['A', 100, 'COMPLETE', '2026-01-01', '2026-05-04', 'est', 1], ['A', 100, 'COMPLETE', '2026-05-05', null, 'obs', 1]]);
  assert.deepEqual(b.periods.map((p) => p.first_observed_at), ['2026-05-04T20:00:00Z', '2026-05-04T20:00:00Z']);
});
await t('🚨 変化は changed_at の JST の日の翌日から (UTC 14:59:59 = JST 同じ日 23:59:59 → 翌日 / UTC 15:00:00 = JST 翌日 → その翌日)', () => {
  const rows = [H('B', BL, 'BASELINE_RESET', 50, 'COMPLETE', 2), H('B', '2026-06-10 14:59:59', 'UPDATE', 60, 'COMPLETE', 3), H('B', '2026-06-10 15:00:00', 'UPDATE', 70, 'COMPLETE', 4)];
  assert.deepEqual(periodsOf(rows, 'B'), [['B', 50, 'COMPLETE', '2026-01-01', '2026-05-04', 'est', 2], ['B', 50, 'COMPLETE', '2026-05-05', '2026-06-10', 'obs', 2],
    ['B', 60, 'COMPLETE', '2026-06-11', '2026-06-11', 'obs', 3], ['B', 70, 'COMPLETE', '2026-06-12', null, 'obs', 4]]);
});
await t('同じ changed_at は history_id の大きい方 (並びに依らない)・同じ JST の日の複数の変化は最後の値', () => {
  const c = [H('C', BL, 'BASELINE_RESET', 10, 'COMPLETE', 5), H('C', '2026-07-01 01:00:00', 'UPDATE', 12, 'COMPLETE', 99), H('C', '2026-07-01 01:00:00', 'UPDATE', 11, 'COMPLETE', 100)];
  assert.deepEqual(periodsOf([...c].reverse(), 'C').slice(2), [['C', 11, 'COMPLETE', '2026-07-02', null, 'obs', 100]]);
  const c2 = [H('C2', BL, 'BASELINE_RESET', 20, 'COMPLETE', 6), H('C2', '2026-07-01 01:00:00', 'UPDATE', 21, 'COMPLETE', 7), H('C2', '2026-07-01 09:00:00', 'UPDATE', 22, 'COMPLETE', 8)];   // JST 7/1 10:00 と 18:00
  const p = buildObservedPeriods(c2).periods.filter((x) => x.valid_from === '2026-07-02');
  assert.deepEqual([tup(p), p[0].first_observed_at], [[['C2', 22, 'COMPLETE', '2026-07-02', null, 'obs', 8]], '2026-07-01T09:00:00Z']);
});
await t('DELETE = 原価不明の始まり (行を作らない)・再 INSERT はその日の翌日から', () => {
  const rows = [H('D', BL, 'BASELINE_RESET', 5, 'COMPLETE', 9), H('D', '2026-06-01 00:00:00', 'DELETE', 5, 'COMPLETE', 10), H('D', '2026-06-05 00:00:00', 'INSERT', 5, 'COMPLETE', 11)];
  assert.deepEqual(periodsOf(rows, 'D').slice(1), [['D', 5, 'COMPLETE', '2026-05-05', '2026-06-01', 'obs', 9], ['D', 5, 'COMPLETE', '2026-06-06', null, 'obs', 11]]);
});
await t('採る状態は COMPLETE / OVERRIDDEN だけ (PARTIAL / MISSING = 不明)・override の 0 円は正しい 0・写しが不明なら推定しない', () => {
  const rows = [H('E', BL, 'BASELINE_RESET', 100, 'PARTIAL', 12), H('E', '2026-06-01 00:00:00', 'UPDATE', 100, 'COMPLETE', 13), H('E', '2026-06-10 00:00:00', 'UPDATE', 100, 'MISSING', 14),
    H('E', '2026-06-20 00:00:00', 'UPDATE', 0, 'OVERRIDDEN', 15)];
  assert.deepEqual(periodsOf(rows, 'E'), [['E', 100, 'COMPLETE', '2026-06-02', '2026-06-10', 'obs', 13], ['E', 0, 'OVERRIDDEN', '2026-06-21', null, 'obs', 15]]);
});
await t('値は夜間ロードと同じ costForLoad (Math.round・負・数でないは不明)・不明が続いても 1 つの空白', () => {
  const rows = [H('F', BL, 'BASELINE_RESET', 100.5, 'COMPLETE', 16), H('F', '2026-06-01 00:00:00', 'UPDATE', 100.4, 'COMPLETE', 17), H('F', '2026-06-10 00:00:00', 'UPDATE', -1, 'COMPLETE', 18),
    H('F', '2026-06-20 00:00:00', 'UPDATE', null, 'COMPLETE', 19), H('F', '2026-06-25 00:00:00', 'UPDATE', 'abc', 'COMPLETE', 20)];
  assert.deepEqual(periodsOf(rows, 'F'), [['F', 101, 'COMPLETE', '2026-01-01', '2026-05-04', 'est', 16], ['F', 101, 'COMPLETE', '2026-05-05', '2026-06-01', 'obs', 16], ['F', 100, 'COMPLETE', '2026-06-02', '2026-06-10', 'obs', 17]]);
});
await t('原価と状態が変わらない履歴の行 (商品名だけの変化など) は区切りにしない・状態だけ変われば区切る', () => {
  const rows = [H('G', BL, 'BASELINE_RESET', 30, 'COMPLETE', 21), H('G', '2026-06-01 00:00:00', 'UPDATE', 30, 'COMPLETE', 22), H('G', '2026-06-10 00:00:00', 'UPDATE', 30, 'OVERRIDDEN', 23)];
  assert.deepEqual(periodsOf(rows, 'G'), [['G', 30, 'COMPLETE', '2026-01-01', '2026-05-04', 'est', 21], ['G', 30, 'COMPLETE', '2026-05-05', '2026-06-10', 'obs', 21], ['G', 30, 'OVERRIDDEN', '2026-06-11', null, 'obs', 23]]);
});
await t('🚨 最初の写しに無く後で初めて出たコードは、初めて出た日より前を推定しない / 2 回目の BASELINE_RESET はふつうの観測 (翌日から)', () => {
  const rows = [H('A', BL, 'BASELINE_RESET', 1, 'COMPLETE', 24), H('N', '2026-06-01 00:00:00', 'INSERT', 40, 'COMPLETE', 25), H('N', '2026-08-01 00:00:00', 'BASELINE_RESET', 45, 'COMPLETE', 26)];
  assert.deepEqual(periodsOf(rows, 'N'), [['N', 40, 'COMPLETE', '2026-06-02', '2026-08-01', 'obs', 25], ['N', 45, 'COMPLETE', '2026-08-02', null, 'obs', 26]]);
  assert.equal(buildObservedPeriods(rows).estimatedRows, 1);
});
await t('最初の写しより前の履歴の行は使わずに数える / 写しが無い・changed_at が読めない・知らない operation は例外 (推測しない)', () => {
  const b = buildObservedPeriods([H('A', '2026-04-01 00:00:00', 'UPDATE', 7), H('OLD', '2026-04-02 00:00:00', 'INSERT', 7), H('A', BL, 'BASELINE_RESET', 1)]);
  assert.deepEqual([b.ignoredBeforeBaseline, b.codes], [2, ['A']]);
  assert.throws(() => buildObservedPeriods([H('A', '2026-06-01 00:00:00', 'INSERT', 1)]), /BASELINE_RESET/);
  assert.throws(() => buildObservedPeriods([H('A', BL, 'BASELINE_RESET', 1), H('A', '2026/06/01 00:00:00', 'UPDATE', 1)]), /changed_at が読めない/);
  assert.throws(() => buildObservedPeriods([H('A', BL, 'BASELINE_RESET', 1), H('A', '2026-02-30 00:00:00', 'UPDATE', 1)]), /changed_at が読めない/);
  assert.throws(() => buildObservedPeriods([H('A', BL, 'BASELINE_RESET', 1), H('A', '2026-06-01 00:00:00', 'MERGE', 1)]), /知らない operation/);
  assert.equal(changedAtIso('2026-06-01 00:00:00'), '2026-06-01T00:00:00Z');
});
await t('履歴の記録 (record-m-products-history.js) の形と同じ前提: 読む列が表にある・changed_at は UTC (toISOString) の秒まで', () => {
  const src = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/record-m-products-history.js'), 'utf8');
  const ddl = /CREATE TABLE IF NOT EXISTS m_products_history \(([\s\S]*?)\)`\);/.exec(src)[1];
  for (const c of ['history_id', '商品コード', '原価 REAL', '原価ソース', '原価状態', 'changed_at TEXT NOT NULL', 'operation TEXT NOT NULL']) assert.ok(ddl.includes(c), c);
  assert.match(src, /return new Date\(\)\.toISOString\(\)\.replace\('T', ' '\)\.slice\(0, 19\);/);
  for (const op of ["'BASELINE_RESET'", "'INSERT'", "'UPDATE'", "'DELETE'"]) assert.ok(src.includes(op), op);
});

console.log('② 結びつけ (衝突の隔離) と checksum の共通の部品');
await t('正規の JSON: 鍵の順は作った順に依らない・null は null・数は安全な整数だけ (小数・NaN・undefined・Date・bigint は例外)', () => {
  assert.equal(canonicalJsonStrict({ b: 1, a: [null, 'x', true] }), '{"a":[null,"x",true],"b":1}');
  assert.equal(canonicalSha256({ b: 1, a: [null, 'x'] }), canonicalSha256({ a: [null, 'x'], b: 1 }));
  assert.notEqual(canonicalSha256({ a: null }), canonicalSha256({ a: 0 }));
  for (const v of [{ a: 1.5 }, { a: NaN }, { a: undefined }, { a: new Date() }, { a: 1n }, [Infinity]]) assert.throws(() => canonicalJsonStrict(v), /整数|素の object|JSON にできない/);
});
await t('🚨 正規化で同じになる履歴のコードは曖昧 = どれも送らない (夜間ロードと同じ normSku)・Render に無い / 形が不正なコードは結びつかない・数は履歴に出るコード (行の無いコードも)', () => {
  const rows = [H('ABC-1', BL, 'BASELINE_RESET', 10), H('abc－1', '2026-06-01 00:00:00', 'INSERT', 20), H('ZZZ', BL, 'BASELINE_RESET', 1), H('Q', BL, 'BASELINE_RESET', 1, 'PARTIAL'),
    H(' A2', BL, 'BASELINE_RESET', 3), H('A', BL, 'BASELINE_RESET', 5)];
  const p = planPayload(buildObservedPeriods(rows), new Set(['abc-1', 'a', 'a2']));
  assert.deepEqual([p.ambiguousCodes, p.unresolvedCodes, p.manifest.ambiguous_code_count, p.manifest.unresolved_code_count, p.skus], [['ABC-1', 'abc－1'], [' A2', 'Q', 'ZZZ'], 2, 3, 1]);
  assert.deepEqual(p.rows.map((r) => r.product_code), ['A', 'A']);
  assert.equal(p.manifest.row_count, 2);
});
await t('checksum: 履歴の並びに依らない・行のどの列が違っても変わる (first_observed_at も)・送る行から計算し直せる', () => {
  const rows = [H('A', BL, 'BASELINE_RESET', 5, 'COMPLETE', 1), H('B', BL, 'BASELINE_RESET', 6, 'COMPLETE', 2), H('A', '2026-06-01 00:00:00', 'UPDATE', 7, 'COMPLETE', 3)];
  const norms = new Set(['a', 'b']);
  const x = planPayload(buildObservedPeriods(rows), norms), y = planPayload(buildObservedPeriods([...rows].reverse()), norms);
  assert.equal(x.manifest.checksum, y.manifest.checksum);
  assert.equal(observedChecksum([...x.rows].reverse()), x.manifest.checksum);
  for (const [k, v] of [['cost_jpy', 8], ['cost_status', 'OVERRIDDEN'], ['valid_to', '2026-12-31'], ['source_history_id', 99], ['first_observed_at', '2026-05-04T20:00:01Z']]) {
    const r2 = x.rows.map((r, i) => (i === x.rows.length - 1 ? { ...r, [k]: v } : r));
    assert.notEqual(observedChecksum(r2), x.manifest.checksum, k);
  }
});

// ─── ③ 受け口 (PGlite) ───
const pg = new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: quiet });
const db = pgliteAdapter(pg);
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
const sku = async (code) => Number((await one(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'exception', $1, $1) returning sku_id`, [code])).sku_id);
const skuA = await sku('SKU-A'), skuB = await sku('SKU-B'), skuC = await sku('SKU-C');
const TODAY = '2026-09-30';
const R = (x = {}) => ({ product_code: 'sku-a', cost_jpy: 100, cost_status: 'COMPLETE', valid_from: '2026-05-05', valid_to: null, backfill_method: 'observed_daily_diff', source_history_id: 1, first_observed_at: '2026-05-04T20:00:00Z', ...x });
const EST = (x = {}) => R({ valid_from: '2026-01-01', valid_to: '2026-05-04', backfill_method: 'estimated_before_first_snapshot', ...x });
const sumOf = (rows) => { try { return observedChecksum(rows); } catch { return '0'.repeat(64); } };   // 形の外れた行 (検証の試験) は指紋を作れない = 検証が先に止める
const OB = (rows, x = {}) => ({ source: 'warehouse_sqlite', generation: 1, checksum: sumOf(rows), row_count: rows.length, unresolved_code_count: 0, ambiguous_code_count: 0, rows, ...x });
const rowsNow = () => all(`select sku_id::int as sku, product_code, cost_jpy::int as cost, cost_status, valid_from::text as vf, valid_to::text as vt, backfill_method, generation::int as g, source_history_id::int as h,
  to_char(first_observed_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as fo from core.sku_cost_observed order by product_code, valid_from`);
const loads = () => all(`select generation::int as g, checksum, row_count, unresolved_code_count as u, ambiguous_code_count as a from core.sku_cost_observed_loads order by generation`);
const runs = async () => Number((await one(`select count(*)::int as n from ops.ingest_runs where entity = 'sku_cost_observed'`)).n);

console.log('③ 受け口 (ingestSkuCostObserved・0046)');
await t('検証 (400): checksum の食い違い・同じコードの期間の重なり・終わりの無い推定・正規化で同じになる 2 コード・row_count の食い違い・未来すぎる valid_from・状態 / 金額 / 日時の形・source・世代', () => {
  const bad = (b, re) => assert.throws(() => validateObservedBody(b, { todayJst: TODAY }), (e) => e.code === 'BAD_REQUEST' && re.test(e.message), re.source);
  bad({ ...OB([R()]), checksum: 'a'.repeat(64) }, /checksum が届いた行から計算した値と合わない/);
  bad(OB([R(), R({ valid_from: '2026-06-01' })]), /期間が重なる/);
  bad(OB([R({ valid_to: '2026-06-01' }), R({ valid_from: '2026-06-01' })]), /期間が重なる/);
  bad(OB([EST({ valid_to: null })]), /終わり/);
  bad(OB([R(), R({ product_code: 'SKU-A', valid_from: '2026-07-01' })]), /正規化すると同じ SKU/);
  bad({ ...OB([R()]), row_count: 2 }, /row_count/);
  bad(OB([R({ valid_from: '2026-10-03' })]), /未来すぎる/);
  assert.equal(validateObservedBody(OB([R({ valid_from: '2026-10-02' })]), { todayJst: TODAY }).rowCount, 1);   // 今日の変化 → 明日から (+ 時計のずれ 1 日)
  bad(OB([R({ cost_status: 'PARTIAL' })]), /cost_status/);
  bad(OB([R({ cost_jpy: 1.5 })]), /cost_jpy/);
  bad(OB([R({ cost_jpy: -1 })]), /cost_jpy/);
  bad(OB([R({ first_observed_at: '2026-05-04 20:00:00' })]), /first_observed_at/);
  bad(OB([R({ valid_to: '2026-05-01' })]), /valid_to/);
  bad(OB([R()], { source: 'x' }), /source/);
  bad(OB([R()], { generation: 0 }), /generation/);
  bad(OB([R()], { unresolved_code_count: -1 }), /unresolved_code_count/);
});
await t('applied: 行を入れ、商品コードは core.norm_code で SKU に結ぶ (sku-a → SKU-A)・見出し (manifest)・取込の記録', async () => {
  const r = await ingestSkuCostObserved(db, OB([EST(), R(), R({ product_code: 'sku-b', cost_jpy: 50, source_history_id: 2 })], { unresolved_code_count: 3, ambiguous_code_count: 2 }), { todayJst: TODAY });
  assert.deepEqual([r.status, r.generation, r.rows, r.skus, r.replaced], ['applied', 1, 3, 2, 0]);
  assert.deepEqual((await rowsNow()).map((x) => [x.sku, x.product_code, x.cost, x.vf, x.vt, x.backfill_method, x.g, x.h, x.fo]),
    [[skuA, 'sku-a', 100, '2026-01-01', '2026-05-04', 'estimated_before_first_snapshot', 1, 1, '2026-05-04T20:00:00Z'], [skuA, 'sku-a', 100, '2026-05-05', null, 'observed_daily_diff', 1, 1, '2026-05-04T20:00:00Z'],
      [skuB, 'sku-b', 50, '2026-05-05', null, 'observed_daily_diff', 1, 2, '2026-05-04T20:00:00Z']]);
  assert.deepEqual(await loads(), [{ g: 1, checksum: r.checksum, row_count: 3, u: 3, a: 2 }]);
  const run = await one(`select source_system, entity, scope_key, status, complete, rows_inserted from ops.ingest_runs where ingest_run_id = $1`, [r.run_id]);
  assert.deepEqual([run.source_system, run.entity, run.scope_key, run.status, run.complete, run.rows_inserted], ['warehouse', 'sku_cost_observed', 'warehouse_sqlite', 'success', true, 3]);
  const st = await skuCostObservedStatus(db);
  assert.deepEqual([st.load.generation, st.load.checksum, st.load.unresolved_code_count, st.rows, st.skus], [1, r.checksum, 3, 3, 2]);
});
await t('🚨 世代: 同じ世代・manifest 全部同じ = same (並びが違っても・何も書かない) / 同じ世代で結びつかない数・曖昧な数・中身のどれかが違う = 409', async () => {
  const before = [await rowsNow(), await loads(), await runs()];
  const body = OB([R({ product_code: 'sku-b', cost_jpy: 50, source_history_id: 2 }), R(), EST()], { unresolved_code_count: 3, ambiguous_code_count: 2 });
  assert.equal((await ingestSkuCostObserved(db, body, { todayJst: TODAY })).status, 'same');
  await assert.rejects(ingestSkuCostObserved(db, { ...body, unresolved_code_count: 4 }, { todayJst: TODAY }), (e) => e.code === 'CONFLICT' && /manifest が違う/.test(e.message));
  await assert.rejects(ingestSkuCostObserved(db, { ...body, ambiguous_code_count: 0 }, { todayJst: TODAY }), (e) => e.code === 'CONFLICT');
  await assert.rejects(ingestSkuCostObserved(db, OB([R({ cost_jpy: 1 })], { unresolved_code_count: 3, ambiguous_code_count: 2 }), { todayJst: TODAY }), (e) => e.code === 'CONFLICT');
  assert.deepEqual([await rowsNow(), await loads(), await runs()], before);
});
await t('新しい世代 = その会社 × 送り元の行を全部入れ替える (消えた行は消える)・古い見出しは監査の履歴として残る (追記だけ = UPDATE / DELETE は拒む) / 古い世代 = stale (何も変わらない)', async () => {
  const r = await ingestSkuCostObserved(db, OB([R({ cost_jpy: 120 }), R({ product_code: 'sku-c', cost_jpy: 7, source_history_id: 3 })], { generation: 3 }), { todayJst: TODAY });
  assert.deepEqual([r.status, r.replaced, r.rows], ['applied', 3, 2]);
  assert.deepEqual((await rowsNow()).map((x) => [x.sku, x.cost, x.g]), [[skuA, 120, 3], [skuC, 7, 3]]);
  assert.deepEqual((await loads()).map((x) => x.g), [1, 3]);
  await assert.rejects(pg.query(`update core.sku_cost_observed_loads set row_count = 0 where generation = 1`), /append-only/);
  await assert.rejects(pg.query(`delete from core.sku_cost_observed_loads where generation = 1`), /append-only/);
  const before = [await rowsNow(), await loads(), await runs()];
  const st = await ingestSkuCostObserved(db, OB([R({ cost_jpy: 999 })], { generation: 2 }), { todayJst: TODAY });
  assert.deepEqual([st.status, st.remote_generation], ['stale', 3]);
  assert.deepEqual([await rowsNow(), await loads(), await runs()], before);
});
await t('Render に無い SKU の商品コードが 1 つでもあれば全部を拒む (409 SKU_UNRESOLVED・何も書かない) / 途中で落ちたら全部巻き戻る', async () => {
  const before = [await rowsNow(), await loads(), await runs()];
  await assert.rejects(ingestSkuCostObserved(db, OB([R({ cost_jpy: 5 }), R({ product_code: 'nope-1' })], { generation: 4 }), { todayJst: TODAY }), (e) => e.code === 'SKU_UNRESOLVED' && /nope-1/.test(e.message));
  await assert.rejects(ingestSkuCostObserved(db, OB([R({ cost_jpy: 5 })], { generation: 4 }), { todayJst: TODAY, afterWrite: async () => { throw new Error('commit の直前で落ちた'); } }), /commit の直前で落ちた/);
  assert.deepEqual([await rowsNow(), await loads(), await runs()], before);
});
await t('読む口 mart.v_sku_cost_observed_effective: SKU ごとに core.sku_costs の最初の valid_from より前だけ (終わりを前日で切る・後に始まる行は出さない・sku_costs の無い SKU はそのまま)', async () => {
  await ingestSkuCostObserved(db, OB([EST(), R({ valid_to: '2026-06-30' }), R({ valid_from: '2026-07-01', valid_to: '2026-09-19', cost_jpy: 110, source_history_id: 5 }), R({ valid_from: '2026-09-20', cost_jpy: 130, source_history_id: 6 }),
    R({ product_code: 'sku-b', cost_jpy: 50, source_history_id: 2 }), R({ product_code: 'sku-c', cost_jpy: 7, source_history_id: 3 })], { generation: 5 }), { todayJst: TODAY });
  // SKU-A: sku_costs の最初の行は 9/10 (PARTIAL でも境目) / SKU-B: 9/15 から (観測の終わりの無い行を 9/14 で切る) / SKU-C: sku_costs が無い = 観測のまま
  await pg.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, valid_to) values (1, $1, 130, 'ne', 'PARTIAL', '2026-09-10', '2026-09-10'), (1, $1, 130, 'ne', 'COMPLETE', '2026-09-11', null)`, [skuA]);
  await pg.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (1, $1, 52, 'ne', 'COMPLETE', '2026-09-15')`, [skuB]);
  const v = await all(`select sku_id::int as sku, cost_jpy::int as cost, valid_from::text as vf, valid_to::text as vt, observed_valid_to::text as ovt, sku_costs_first_from::text as ff, cost_basis from mart.v_sku_cost_observed_effective order by sku_id, valid_from`);
  assert.deepEqual(v.map((x) => [x.sku, x.cost, x.vf, x.vt, x.ovt, x.ff, x.cost_basis]), [
    [skuA, 100, '2026-01-01', '2026-05-04', '2026-05-04', '2026-09-10', 'estimated'], [skuA, 100, '2026-05-05', '2026-06-30', '2026-06-30', '2026-09-10', 'observed'],
    [skuA, 110, '2026-07-01', '2026-09-09', '2026-09-19', '2026-09-10', 'observed'],   // 9/20 からの行は sku_costs の後 = 出さない
    [skuB, 50, '2026-05-05', '2026-09-14', null, '2026-09-15', 'observed'], [skuC, 7, '2026-05-05', null, null, null, 'observed']]);
  await pg.query(`delete from core.sku_costs where sku_id = $1`, [skuB]);   // 送り手の試験 (④) は SKU-B の境目なしで読む
});

// ─── ④ 本物の router を HTTP で + 送り手 ───
console.log('④ Render の受け口 (本物の router を HTTP で) と送り手');
process.env.MIRROR_SYNC_KEY = 'k';
process.env.COMPANY_DB_URL = 'pglite://test';
__setPgClientFactory(async () => ({
  query: async (text, params) => {
    if (params && params.length) return pg.query(text, params);
    if (text.includes(';')) { await pg.exec(text); return { rows: [] }; }
    return pg.query(text);
  },
  end: async () => {},
}));
const app = express();
app.use('/apps/company-db/sync', requireSyncKey);
app.use('/apps/company-db/sync', companyDbRouter);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const BASE_URL = `http://127.0.0.1:${server.address().port}/apps/company-db/sync`;
const http = async (method, p, { body: b, key = 'k' } = {}) => {
  const res = await fetch(`${BASE_URL}${p}`, { method, headers: { ...(key == null ? {} : { 'x-sync-key': key }), ...(b !== undefined ? { 'content-type': 'application/json' } : {}) }, body: b !== undefined ? JSON.stringify(b) : undefined });
  const text = await res.text(); let json = null; try { json = JSON.parse(text); } catch { /* JSON でない */ }
  return { status: res.status, json };
};
const realToday = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);

await t('受け口: 鍵が無ければ 401 / 検証に通らなければ 400 / 同じ世代で違う manifest は 409 / Render に無い SKU は 409 / status・sku-codes / server.js は鍵の検査と共通 parser の素通りに入れている', async () => {
  const cur = (await skuCostObservedStatus(db)).load;
  assert.equal((await http('POST', '/sku-cost-observed', { body: OB([R()]), key: null })).status, 401);
  assert.equal((await http('POST', '/sku-cost-observed', { body: { source: 'warehouse_sqlite' } })).status, 400);
  const c = await http('POST', '/sku-cost-observed', { body: OB([R({ cost_jpy: 3 })], { generation: cur.generation }) });
  assert.deepEqual([c.status, c.json.code], [409, 'CONFLICT']);
  const u = await http('POST', '/sku-cost-observed', { body: OB([R({ product_code: 'nope-2' })], { generation: cur.generation + 1 }) });
  assert.deepEqual([u.status, u.json.code], [409, 'SKU_UNRESOLVED']);
  const st = await http('GET', '/sku-cost-observed/status');
  assert.deepEqual([st.status, st.json.load.generation, st.json.load.checksum], [200, cur.generation, cur.checksum]);
  const k1 = await http('GET', '/sku-cost-observed/sku-codes?limit=2');
  assert.deepEqual([k1.json.keys, k1.json.next], [['sku-a', 'sku-b'], 'sku-b']);
  const k2 = await http('GET', `/sku-cost-observed/sku-codes?limit=2&after=${k1.json.next}`);
  assert.deepEqual([k2.json.keys, k2.json.next], [['sku-c'], null]);
  assert.equal((await http('GET', '/sku-cost-observed/sku-codes?limit=x')).status, 400);
  const srv = fs.readFileSync(path.join(repoRoot, 'server.js'), 'utf8');
  assert.match(srv, /app\.use\(\[[^\]]*'\/apps\/company-db\/sync\/sku-cost-observed'[^\]]*\], companyDbRequireSyncKey\);/);
  assert.match(srv, /normalizedPath\.toLowerCase\(\)\.startsWith\('\/apps\/company-db\/sync\/sku-cost-observed'\)\) return next\(\);/);
});

// 送り手: メモリ上の warehouse.db (m_products_history は record-m-products-history.js と同じ形)
const HISTORY_DDL = `CREATE TABLE m_products_history (history_id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER NOT NULL, 商品コード TEXT NOT NULL, 商品名 TEXT, 商品区分 TEXT, 取扱区分 TEXT,
  標準売価 REAL, 原価 REAL, 原価ソース TEXT, 原価状態 TEXT, 消費税率 REAL, 税区分 TEXT, 売上分類 INTEGER, changed_at TEXT NOT NULL, operation TEXT NOT NULL, changed_by TEXT,
  原価_before REAL, 標準売価_before REAL, 消費税率_before REAL, changed_columns TEXT)`;
const addH = (w, code, at, op, cost, status = 'COMPLETE') => w.prepare(`insert into m_products_history (product_id, 商品コード, 原価, 原価ソース, 原価状態, changed_at, operation) values (1, ?, ?, 'NE', ?, ?, ?)`).run(code, cost, status, at, op);
function openWh(file = ':memory:') {
  const w = new Database(file);
  w.exec(HISTORY_DDL);
  addH(w, 'SKU-A', BL, 'BASELINE_RESET', 100); addH(w, 'SKU-A', '2026-06-10 00:00:00', 'UPDATE', 120);
  addH(w, 'SKU-B', BL, 'BASELINE_RESET', 50);
  addH(w, 'SKU-X', BL, 'BASELINE_RESET', 10);                                          // Render に無い = 結びつかない
  addH(w, 'SKU-C', BL, 'BASELINE_RESET', 70); addH(w, 'sku-c', '2026-06-01 00:00:00', 'INSERT', 71);   // 正規化で同じ = 曖昧
  return w;
}
const spyFetch = (hook = null) => {
  const posts = [];
  const f = async (url, init) => { if (init && init.method === 'POST') { posts.push(String(url).replace(BASE_URL, '')); if (hook) return hook(url, init, posts.length); } return fetch(url, init); };
  f.posts = posts; return f;
};
const L = openLedger(null, { memory: true, kind: KIND });
const push = (w, x = {}) => pushSkuCostObserved({ warehouse: w, ledger: L, base: BASE_URL, syncKey: 'k', log: quiet, sleep: async () => {}, ...x });
const wh = openWh();
const remoteGen = async () => (await skuCostObservedStatus(db)).load.generation;

await t('送り手: 全部を作って 1 要求で入れ替える・台帳が空でも Render の世代まで進めてから次の世代 (HTTP の前に台帳に書く)・Render の行 = 作った期間 (結べる・曖昧でないコードだけ)', async () => {
  const g0 = await remoteGen();
  assert.equal(L.currentBatchSeq(), 0);
  const f = spyFetch();
  const r = await push(wh, { fetchImpl: f });
  assert.deepEqual([r.ok, r.status, r.generation, r.rows, r.skus, r.manifest.unresolved_code_count, r.manifest.ambiguous_code_count, f.posts], [true, 'applied', g0 + 1, 5, 2, 1, 2, ['/sku-cost-observed']]);
  assert.match(r.lastLine, /^✅ Company DB 観測の原価: 入れ替えた 世代 \d+ 行 5 \/ SKU 2 \(推定の行 \d\) \/ 結びつかない商品コード 1 \/ 曖昧 2/);
  assert.deepEqual((await rowsNow()).map((x) => [x.sku, x.product_code, x.cost, x.vf, x.vt, x.backfill_method === 'observed_daily_diff' ? 'obs' : 'est', x.g]), [
    [skuA, 'SKU-A', 100, '2026-01-01', '2026-05-04', 'est', g0 + 1], [skuA, 'SKU-A', 100, '2026-05-05', '2026-06-10', 'obs', g0 + 1], [skuA, 'SKU-A', 120, '2026-06-11', null, 'obs', g0 + 1],
    [skuB, 'SKU-B', 50, '2026-01-01', '2026-05-04', 'est', g0 + 1], [skuB, 'SKU-B', 50, '2026-05-05', null, 'obs', g0 + 1]]);
  assert.deepEqual([L.currentBatchSeq(), JSON.parse(L.getMeta(META_PENDING)).generation], [g0 + 1, g0 + 1]);
  assert.equal(L.lastRuns(1)[0].ok, 1);
});
await t('送り手: 2 回目 (変わりなし) は POST しない・世代を進めない / 履歴が変われば新しい世代で入れ替え', async () => {
  const g = await remoteGen(), f = spyFetch();
  const r = await push(wh, { fetchImpl: f });
  assert.deepEqual([r.status, r.generation, f.posts, L.currentBatchSeq()], ['unchanged', g, [], g]);
  assert.match(r.lastLine, /^✅ .*変わりなし/);
  addH(wh, 'SKU-B', '2026-07-01 00:00:00', 'UPDATE', 55);
  const r2 = await push(wh);
  assert.deepEqual([r2.status, r2.generation, (await rowsNow()).filter((x) => x.sku === skuB).map((x) => [x.cost, x.vf, x.vt])], ['applied', g + 1, [[50, '2026-01-01', '2026-05-04'], [50, '2026-05-05', '2026-07-01'], [55, '2026-07-02', null]]]);
});
await t('🚨 応答が失われた (同じ回): Render は入れたが応答が届かない → 同じ世代・同じ body で再送 = same', async () => {
  addH(wh, 'SKU-B', '2026-07-10 00:00:00', 'UPDATE', 56);
  const g = await remoteGen();
  const f = spyFetch(async (url, init, n) => { const res = await fetch(url, init); if (n === 1) { await res.text(); throw new Error('socket hang up (応答が失われた)'); } return res; });
  const r = await push(wh, { fetchImpl: f });
  assert.deepEqual([r.ok, r.status, r.generation, f.posts.length, await remoteGen()], [true, 'same', g + 1, 2, g + 1]);
});
await t('🚨 応答が失われた (回をまたぐ): 届かなかった → 次の回は台帳の pending と同じ中身なら同じ世代で送る / 届いていた → 次の回は変わりなし', async () => {
  addH(wh, 'SKU-A', '2026-08-01 00:00:00', 'UPDATE', 130);
  const g = await remoteGen();
  const down = spyFetch(async () => { throw new Error('ECONNRESET'); });
  await assert.rejects(push(wh, { fetchImpl: down }), (e) => /ECONNRESET/.test(e.message) && /^❌ Company DB 観測の原価: ECONNRESET/.test(e.result.lastLine));
  assert.deepEqual([down.posts.length, await remoteGen(), JSON.parse(L.getMeta(META_PENDING)).generation], [6, g, g + 1]);
  const r = await push(wh);
  assert.deepEqual([r.status, r.generation, r.reusedGeneration, await remoteGen()], ['applied', g + 1, true, g + 1]);
  assert.match(r.lastLine, /応答が失われた前の回と同じ世代/);
  addH(wh, 'SKU-A', '2026-08-10 00:00:00', 'UPDATE', 131);
  const lost = spyFetch(async (url, init) => { const res = await fetch(url, init); await res.text(); throw new Error('ETIMEDOUT'); });
  await assert.rejects(push(wh, { fetchImpl: lost }), /ETIMEDOUT/);
  assert.equal(await remoteGen(), g + 2);   // 1 回目で Render は入れていた (2 回目以降は same)
  const f = spyFetch();
  const r2 = await push(wh, { fetchImpl: f });
  assert.deepEqual([r2.status, r2.generation, f.posts], ['unchanged', g + 2, []]);
});
await t('送り手: Render が同じ世代で違う中身を持つ = 409 で失敗 / Render の方が新しい世代 = stale で失敗 (どちらも ❌・Render は変わらない)', async () => {
  addH(wh, 'SKU-B', '2026-08-15 00:00:00', 'UPDATE', 57);
  const g = await remoteGen();
  // status が 1 つ古い世代を言う (台帳を失くし Render の読み違い) → 採る世代が Render の今の世代と同じ → 中身が違うので 409
  const lie = (d) => async (url, init) => {
    if (!init || init.method !== 'POST') {
      if (String(url).includes('/sku-cost-observed/status')) { const j = await (await fetch(url, init)).json(); return new Response(JSON.stringify({ ...j, load: { ...j.load, generation: j.load.generation - d, checksum: '0'.repeat(64) } }), { status: 200 }); }
    }
    return fetch(url, init);
  };
  await assert.rejects(push(wh, { ledger: openLedger(null, { memory: true, kind: KIND }), fetchImpl: lie(1) }), (e) => /HTTP 409/.test(e.message) && /^❌/.test(e.result.lastLine));
  await assert.rejects(push(wh, { ledger: openLedger(null, { memory: true, kind: KIND }), fetchImpl: lie(2) }), (e) => /Render の方が新しい世代/.test(e.message) && e.result.status === 'stale');
  assert.equal(await remoteGen(), g);
  assert.equal((await push(wh)).status, 'applied');   // ふだんの台帳ならそのまま通る
});
await t('🚨 --dry-run: POST しない・Render は変わらない・台帳を開かない (ファイルも作らない)・世代を採らない', async () => {
  addH(wh, 'SKU-B', '2026-09-01 00:00:00', 'UPDATE', 58);
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-sco-dry-'));
  try {
    const before = [await rowsNow(), await loads(), await runs()], seq = L.currentBatchSeq(), f = spyFetch();
    const r = await pushSkuCostObserved({ warehouse: wh, dataDir: dir, base: BASE_URL, syncKey: 'k', dryRun: true, fetchImpl: f, log: quiet });
    assert.deepEqual([r.ok, r.status, r.generation, f.posts, fs.existsSync(path.join(dir, LEDGER_FILE)), L.currentBatchSeq()], [true, 'dry-run', null, [], false, seq]);
    assert.match(r.lastLine, /^dry-run: 送る予定 行 \d+ .*\[dry-run = 送っていない\]/);
    assert.deepEqual([await rowsNow(), await loads(), await runs()], before);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
await t('0046 の適用前 (0045 までの DB): status は 409 not_migrated → 送り手は ⏭️ (ok・POST しない・世代を採らない = マージから migrate までの朝を ❌ にしない)', async () => {
  const old = new PGlite();
  await applyMigrations(pgliteAdapter(old), { log: quiet, to: '0045' });
  __setPgClientFactory(async () => ({ query: async (text, params) => (params && params.length ? old.query(text, params) : text.includes(';') ? (await old.exec(text), { rows: [] }) : old.query(text)), end: async () => {} }));
  try {
    const st = await http('GET', '/sku-cost-observed/status');
    assert.deepEqual([st.status, st.json.error], [409, 'not_migrated']);
    const seq = L.currentBatchSeq(), f = spyFetch();
    const r = await push(wh, { fetchImpl: f });
    assert.deepEqual([r.ok, r.status, r.generation, f.posts, L.currentBatchSeq()], [true, 'not_migrated', null, [], seq]);
    assert.match(r.lastLine, /^⏭️ Company DB 観測の原価: Render に migration 0046 がまだ無い/);
  } finally {
    __setPgClientFactory(async () => ({ query: async (text, params) => (params && params.length ? pg.query(text, params) : text.includes(';') ? (await pg.exec(text), { rows: [] }) : pg.query(text)), end: async () => {} }));
    await old.close();
  }
});
await t('送り手: 別の送り手が走っている (lock) = 見送り (POST しない・ok ではない)・引数の検査 (--send か --dry-run のどちらか 1 つ)', async () => {
  assert.equal(L.acquireLock({ owner: 'other', pid: process.pid }).ok, true);
  try {
    const f = spyFetch();
    const r = await push(wh, { fetchImpl: f });
    assert.deepEqual([r.ok, !!r.lockedBy, f.posts], [false, true, []]);
    assert.match(r.lastLine, /^⏸️ /);
  } finally { L.releaseLock('other'); }
  assert.deepEqual(parseArgs(['--send']), { send: true, dryRun: false, dataDir: null });
  assert.equal(parseArgs(['--dry-run', '--data-dir', 'x']).dataDir, 'x');
  assert.throws(() => parseArgs([]), /どちらか 1 つ/);
  assert.throws(() => parseArgs(['--send', '--dry-run']), /どちらか 1 つ/);
  assert.throws(() => parseArgs(['7']), /知らない引数/);   // daily-sync の runScript は引数が無いと '7' を足す = 必ず --send を付けて呼ぶ
});

// ─── ⑤ CLI と daily-sync の配線 ───
console.log('⑤ CLI (送信の失敗 = exit 1) と daily-sync / retry / 台帳');
await t('🚨 CLI: Render に届かない = ❌ で exit 1 (daily-sync の runScript は success: false = 成功の合図を出さない・retry に載る)・引数が無い = exit 1', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-sco-cli-'));
  try {
    openWh(path.join(dir, 'warehouse.db')).close();
    const env = { ...process.env, DATA_DIR: dir, RENDER_MIRROR_URL: 'https://127.0.0.1:9/none', RENDER_PORTAL_URL: '', MIRROR_SYNC_KEY: 'k' };
    const cli = (args) => { try { return { code: 0, out: execFileSync(process.execPath, ['apps/company-db/push/sku-cost-observed.mjs', ...args], { cwd: repoRoot, env, encoding: 'utf8' }) }; } catch (e) { return { code: e.status, out: String(e.stdout || '') + String(e.stderr || '') }; } };
    const r = cli(['--send']);
    assert.equal(r.code, 1, r.out);
    assert.match(r.out.trim().split('\n').pop(), /^❌ Company DB 観測の原価: /);
    const n = cli([]);
    assert.equal(n.code, 1); assert.match(n.out, /--send か --dry-run/);
    const d = cli(['7']);
    assert.equal(d.code, 1); assert.match(d.out, /知らない引数: 7/);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
await t('daily-sync: 「m_products 履歴記録」の直後に --send で 1 工程 (新しい定期実行は無い)・retry は CompanyDB観測原価 --send・台帳 (jobs-registry) の warehouse-daily-sync に書いてある', async () => {
  const src = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/daily-sync.js'), 'utf8');
  const iHist = src.indexOf("runScript('apps/warehouse/record-m-products-history.js', 'm_products 履歴記録')"), iPush = src.indexOf("runScript('apps/company-db/push/sku-cost-observed.mjs --send', 'Company DB 観測の原価', 600000)");
  const iSales = src.indexOf("runScript('apps/warehouse/rebuild-f-sales.js'");
  assert.ok(iHist > 0 && iPush > iHist && iSales > iPush, `${iHist} ${iPush} ${iSales}`);
  assert.match(src, /results\.push\(\{ name: 'CompanyDB観測原価', \.\.\.cdbObservedResult, warn: cdbObservedResult\.success && isWarnSummary\(cdbObservedResult\.summary\) \}\);/);
  const retryable = JSON.parse(`[${/const RETRYABLE_JOBS = \[([^\]]*)\]/.exec(src)[1].replace(/'/g, '"')}]`);
  assert.ok(retryable.includes('CompanyDB観測原価'));
  const { JOB_DEFINITIONS, RETRY_ORDER, UPSTREAM_OF } = await import('../apps/warehouse/retry-failed-jobs.js');
  assert.deepEqual([JOB_DEFINITIONS['CompanyDB観測原価'].script, JOB_DEFINITIONS['CompanyDB観測原価'].args], ['apps/company-db/push/sku-cost-observed.mjs', ['--send']]);
  assert.ok(RETRY_ORDER.includes('CompanyDB観測原価') && RETRY_ORDER.indexOf('CompanyDB観測原価') < RETRY_ORDER.indexOf('CompanyDB見張り'));
  assert.equal(Object.hasOwn(UPSTREAM_OF, 'CompanyDB観測原価'), false);
  const reg = fs.readFileSync(path.join(repoRoot, 'config/jobs-registry.mjs'), 'utf8');
  const entry = reg.slice(reg.indexOf("id: 'warehouse-daily-sync'"), reg.indexOf("where: 'miniPC TaskScheduler [WarehouseDailySync"));
  assert.ok(entry.includes('Company DB 観測の原価') && entry.includes('sku-cost-observed.mjs --send') && entry.includes('新しい定期実行は無い'), 'warehouse-daily-sync の説明に無い');
});

server.close();
try { wh.close(); } catch { /* */ }
console.log(`\n${ng === 0 ? '✅' : '❌'} 観測の原価: ok ${ok} / NG ${ng}`);
process.exitCode = ng === 0 ? 0 : 1;
