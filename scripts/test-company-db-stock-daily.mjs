#!/usr/bin/env node
/**
 * test-company-db-stock-daily.mjs — 在庫の日次 (SKU 単位) を miniPC から Company DB へ送る (08 §3.3 ③ NE = D2b-1 / ④ FBA = D2b-2) の試験。
 *   PGlite + 本物の router を HTTP で + 送り手 (pushStockDaily) を メモリ上の SQLite (本番と同じ列の ne_stock_daily_snapshot) で回す。
 *   1 日 = 1 取引 (building → 行 → complete)・先に確定した日は書き換えない・取れなかった日は missing (0 と読ませない)・台帳を持たず Render に聞く
 * 🚨 試験に無いもの: 2 接続の並行 (advisory lock。PGlite は 1 接続)・本番の件数 (1 日 5,000 行 × 140 日) での所要時間・main() の終了コード
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import Database from 'better-sqlite3';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ingestStockDay, validateStockDayBody, stockChecksum, strictInstant, STOCK_SOURCES } from '../apps/company-db/ingest/stock-daily.mjs';
import os from 'node:os';
import { pushStockDaily, parseArgs, rowsOfDay, datesBetween, readWindow, openSource, SOURCES, WINDOW_DAYS } from '../apps/company-db/push/stock-daily.mjs';
import { lockDbFileOf } from '../apps/fba-replenishment/file-lock.js';
import { MAX_ROWS, fbaChecksum, fbaRowOf, normCodeKey, looseCodeKey } from '../apps/company-db/ingest/stock-daily.mjs';
import { runFbaReportSnapshot } from '../apps/warehouse/fba-report-snapshot.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const pg = new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: quiet });
const db = pgliteAdapter(pg);
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
const sku = async (code) => {
  const prod = (await one(`insert into core.products (company_id, name) values (1, $1) returning product_id`, [`見本 ${code}`])).product_id;
  return Number((await one(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', $2, $2) returning sku_id`, [prod, code])).sku_id);
};
const skuA = await sku('ne-aaa'), skuB = await sku('ne-bbb');
const TODAY = '2026-03-20';   // 本体の試験は「今日」を渡せる → 実際の今日 (HTTP の試験が使う) とぶつからない昔の日付で回す
/** 取得時刻 = その業務日の朝 07:01 JST (= 前の日の 22:01 UTC)。本番の ne_stock_daily_snapshot と同じ関係 (今日の行でも未来にならない) */
const capOf = (date) => { const t = Date.parse(`${date}T00:00:00Z`); return Number.isNaN(t) ? '2026-01-01T00:00:00.000Z' : new Date(Math.min(t - 7117 * 1000, Date.now() - 60000)).toISOString(); };   // 🚨 今日の行でも必ず過去 (早朝に試験を回しても「未来の取得時刻」にならない。Codex R2 #3)   // 日付そのものが不正な試験でも、取得時刻は正しい形にしておく (日付の検証に届かせる)
const body = (date, rows, x = {}) => ({ source: 'ne', scope: 'main', snapshot_date: date, captured_at: capOf(date), rows, ...x });
const dayOf = (date) => one(`select status, ingest_run_id, rows, completed_at is not null as done from snapshots.stock_capture_days where snapshot_date = $1::date and source = 'ne' and scope_key = 'main'`, [date]);
const rowsOf = (date) => all(`select source_code, sku_id, qty, allocated_qty, fba_available from snapshots.sku_stock_daily where snapshot_date = $1::date and source = 'ne' order by source_code`, [date]);

console.log('受け口の本体 (ingestStockDay)');
await t('1 日 = 1 取引: building → 行 → complete。SKU は code_norm で解決 (分からない行も入れて数える)。取込の記録に内容の指紋。mart.v_sku_stock の ne_qty に出る', async () => {
  const r = await ingestStockDay(db, body('2026-03-18', [{ code: 'ne-aaa', qty: 5 }, { code: 'ne-bbb', qty: 0 }, { code: 'ne-unknown', qty: 7 }]), { todayJst: TODAY });
  assert.deepEqual([r.status, r.rows, r.resolved, r.unresolved], ['applied', 3, 2, 1]);
  const d = await dayOf('2026-03-18');
  assert.deepEqual([d.status, d.done, d.rows, d.ingest_run_id === r.run_id], ['complete', true, 3, true]);
  assert.deepEqual((await rowsOf('2026-03-18')).map((x) => [x.source_code, x.sku_id == null ? null : Number(x.sku_id), x.qty, x.allocated_qty, x.fba_available]),
    [['ne-aaa', skuA, 5, null, null], ['ne-bbb', skuB, 0, null, null], ['ne-unknown', null, 7, null, null]]);
  const run = await one(`select source_system, entity, scope_key, status, complete, rows_seen, rows_inserted, checksum from ops.ingest_runs where ingest_run_id = $1`, [r.run_id]);
  assert.deepEqual([run.source_system, run.entity, run.scope_key, run.status, run.complete, run.rows_seen, run.rows_inserted, run.checksum], ['ne', 'stock_daily', 'main', 'success', true, 3, 3, r.checksum]);
  const v = await one(`select ne_qty, ne_as_of::text as as_of from mart.v_sku_stock where sku_id = $1`, [skuA]);
  assert.deepEqual([Number(v.ne_qty), v.as_of], [5, '2026-03-18']);
});
await t('🚨 先に確定した日は書き換えない: 同じ内容の再送 = same (行の順が違っても同じ指紋) / 違う内容 = CONFLICT で何も変わらない / 確定済みの日を missing にもできない', async () => {
  const same = await ingestStockDay(db, body('2026-03-18', [{ code: 'ne-unknown', qty: 7 }, { code: 'ne-bbb', qty: 0 }, { code: 'ne-aaa', qty: 5 }]), { todayJst: TODAY });
  assert.equal(same.status, 'same');
  await assert.rejects(ingestStockDay(db, body('2026-03-18', [{ code: 'ne-aaa', qty: 6 }, { code: 'ne-bbb', qty: 0 }, { code: 'ne-unknown', qty: 7 }]), { todayJst: TODAY }), (e) => e.code === 'CONFLICT');
  await assert.rejects(ingestStockDay(db, { source: 'ne', snapshot_date: '2026-03-18', missing: true }, { todayJst: TODAY }), (e) => e.code === 'CONFLICT');
  assert.deepEqual((await rowsOf('2026-03-18')).map((x) => x.qty), [5, 0, 7]);
  assert.equal(Number((await one(`select count(*)::int as n from ops.ingest_runs where entity = 'stock_daily'`)).n), 1, '再送・衝突で取込の記録が増えている');
});
await t('取れなかった日は missing (過去の日だけ)。missing の日は view に出ない。後から行が届いたら complete に上がる', async () => {
  const m = await ingestStockDay(db, { source: 'ne', snapshot_date: '2026-03-17', missing: true }, { todayJst: TODAY });
  assert.equal(m.status, 'missing');
  assert.deepEqual([(await dayOf('2026-03-17')).status, (await rowsOf('2026-03-17')).length], ['missing', 0]);
  assert.equal((await ingestStockDay(db, { source: 'ne', snapshot_date: '2026-03-17', missing: true }, { todayJst: TODAY })).status, 'missing_same');
  await assert.rejects(ingestStockDay(db, { source: 'ne', snapshot_date: TODAY, missing: true }, { todayJst: TODAY }), /今日 .* 以降を missing にはできない/);
  const late = await ingestStockDay(db, body('2026-03-17', [{ code: 'ne-aaa', qty: 9 }]), { todayJst: TODAY });
  assert.deepEqual([late.status, (await dayOf('2026-03-17')).status, (await rowsOf('2026-03-17')).map((x) => x.qty)], ['applied', 'complete', [9]]);
});
await t('途中で落ちたら全部巻き戻る: building の日も、行も、取込の記録も残らない (= 次の送信がそのまま通る)', async () => {
  const runs = async () => Number((await one(`select count(*)::int as n from ops.ingest_runs where entity = 'stock_daily'`)).n);
  const before = await runs();
  await assert.rejects(ingestStockDay(db, body('2026-03-19', [{ code: 'ne-aaa', qty: 1 }]), { todayJst: TODAY, afterWrite: async () => { throw new Error('commit の直前で落ちた'); } }), /commit の直前で落ちた/);
  // afterWrite の時点で取込の記録は success になっている → 「success の記録だけ残る」を見逃さないよう、件数そのものを比べる (Codex R1 #5)
  assert.deepEqual([await dayOf('2026-03-19'), (await rowsOf('2026-03-19')).length, await runs()], [undefined, 0, before]);
  // missing → complete の途中で落ちたら、元の missing に戻る (building のまま・run つきの missing にならない)
  await ingestStockDay(db, { source: 'ne', snapshot_date: '2026-03-15', missing: true }, { todayJst: TODAY });
  await assert.rejects(ingestStockDay(db, body('2026-03-15', [{ code: 'ne-aaa', qty: 1 }]), { todayJst: TODAY, afterWrite: async () => { throw new Error('途中で落ちた'); } }), /途中で落ちた/);
  const back = await dayOf('2026-03-15');
  assert.deepEqual([back.status, back.ingest_run_id, back.done, (await rowsOf('2026-03-15')).length, await runs()], ['missing', null, false, 0, before]);
  assert.equal((await ingestStockDay(db, body('2026-03-19', [{ code: 'ne-aaa', qty: 1 }]), { todayJst: TODAY })).status, 'applied');
});
await t('検証: 知らない source / scope・実在しない日付・未来・行 0 件 (= missing で言う)・重複・前後の空白 (trim しない)・負・小数・int32 超・captured_at なし は BAD_REQUEST。継承プロパティの名前を source にできない', async () => {
  const ok1 = { code: 'x', qty: 1 };
  const cases = [
    { ...body('2026-03-16', [ok1]), source: 'logizard' }, { ...body('2026-03-16', [ok1]), source: 'toString' }, { ...body('2026-03-16', [ok1]), scope: 'other' },
    body('2026-02-30', [ok1]), body('2026-3-1', [ok1]), body('2026-03-21', [ok1]), body('2026-03-16', []), body('2026-03-16', [ok1, ok1]),
    body('2026-03-16', [{ code: ' x', qty: 1 }]), body('2026-03-16', [{ code: '', qty: 1 }]), body('2026-03-16', [{ code: 'x', qty: -1 }]), body('2026-03-16', [{ code: 'x', qty: 1.5 }]),
    body('2026-03-16', [{ code: 'x', qty: 2147483648 }]), body('2026-03-16', [{ code: 'x', qty: '3' }]), { ...body('2026-03-16', [ok1]), captured_at: undefined }, { ...body('2026-03-16', [ok1]), captured_at: '9/16 07:00' },
    { ...body('2026-03-16', [ok1]), missing: 'yes' }, { source: 'ne', snapshot_date: '2026-03-16', missing: true, rows: [ok1] }, null, [],
  ];
  for (const c of cases) assert.throws(() => validateStockDayBody(c, { todayJst: TODAY }), (e) => e.code === 'BAD_REQUEST', JSON.stringify(c).slice(0, 120));
  assert.equal(await dayOf('2026-03-16'), undefined);
  assert.deepEqual(Object.keys(STOCK_SOURCES), ['ne', 'fba_jp', 'fba_us']);
  // 🚨 取得時刻 (Codex R1 #4): タイムゾーンの無い日時・実在しない日時 (Date が黙って繰り上げる)・未来 は受けない。内容の指紋は取得時刻を含まない = 間違った時刻で確定すると、後から直せない
  const NOW = Date.parse('2026-03-20T05:00:00Z');
  assert.deepEqual([strictInstant('2026-03-19T22:01:23.005Z', { now: NOW }), strictInstant('2026-03-20T07:01:23+09:00', { now: NOW }), strictInstant('2026-03-20T05:09:59Z', { now: NOW })],
    ['2026-03-19T22:01:23.005Z', '2026-03-19T22:01:23.000Z', '2026-03-20T05:09:59.000Z']);
  for (const v of ['2026-02-30T01:00:00Z', '2026-03-19T22:01:23', '2026-03-19 22:01:23Z', '2026-03-19T24:00:00Z', '2026-03-19T23:60:00Z', '2026-03-19T23:00:61Z', '2026-03-19T22:01:23+15:00', '2026-03-20T05:11:00Z', '2026-03-19', '', null, 1742421683])
    assert.equal(strictInstant(v, { now: NOW }), null, String(v));
  for (const c of ['2026-02-30T01:00:00Z', '2026-03-16T07:00:00', '2026-03-20T05:11:00Z'])
    assert.throws(() => validateStockDayBody({ ...body('2026-03-16', [ok1]), captured_at: c }, { todayJst: TODAY, now: NOW }), (e) => e.code === 'BAD_REQUEST', c);
  assert.equal(stockChecksum([{ code: 'a', qty: 1 }, { code: 'b', qty: 2 }]), stockChecksum([{ code: 'b', qty: 2 }, { code: 'a', qty: 1 }]));
  assert.notEqual(stockChecksum([{ code: 'a', qty: 12 }]), stockChecksum([{ code: 'a1', qty: 2 }]), '区切りが無いと (a,12) と (a1,2) がぶつかる');
});

console.log('受け口の本体: FBA (D2b-2)');
// 出品と構成: 単品 × 1 個 / まとめ売り (1 SKU × 3 個) / セット (2 SKU) / 出品が無い SKU
const listing = async (mall, code, comps) => {
  const id = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, $1, '', $2, 'active') returning listing_id`, [mall, code])).listing_id;
  for (const [skuId, qty] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, $3, 'exact', 'system')`, [id, skuId, qty]);
  return id;
};
await listing('amazon', 'pr_single_001', [[skuA, 1]]);
await listing('amazon', 'pr_pack3_001', [[skuB, 3]]);
await listing('amazon', 'pr_set_001', [[skuA, 1], [skuB, 1]]);
const F = (code, a, x, p2, c, w = 0, s2 = 0, r = 0) => ({ code, fba_available: a, fba_fc_transfer: x, fba_fc_processing: p2, fba_customer_order: c, fba_inbound_working: w, fba_inbound_shipped: s2, fba_inbound_received: r });
const fbody = (date, rows, x = {}) => ({ source: 'fba_jp', snapshot_date: date, captured_at: capOf(date), rows, ...x });
const fdayOf = (date, source = 'fba_jp') => one(`select status, ingest_run_id, completed_at is not null as done from snapshots.stock_capture_days where snapshot_date = $1::date and source = $2`, [date, source]);
const frowsOf = (date, source = 'fba_jp') => all(`select source_code, sku_id, qty, fba_available, fba_fc_transfer, fba_fc_processing, fba_customer_order, fba_inbound_working, fba_inbound_shipped, fba_inbound_received, scope_key from snapshots.sku_stock_daily where snapshot_date = $1::date and source = $2 order by source_code`, [date, source]);
await t('FBA の 1 日: qty = 倉庫の中の在庫 (available + FC 移管中 + 処理中 + 出荷待ち)。🚨 SKU を入れるのは、出品の構成が 1 SKU × 1 個のときだけ (まとめ売り・セット・出品なしは sku_id = null。FBA の 1 個が SKU の 1 個ではない)。出品 SKU の大文字小文字は出品の側の正規化で当たる。mart.v_sku_stock の fba_jp に出る', async () => {
  const r = await ingestStockDay(db, fbody('2026-03-10', [F('PR_SINGLE_001', 10, 1, 2, 3, 4, 5, 6), F('pr_pack3_001', 7, 0, 0, 1), F('pr_set_001', 2, 0, 0, 0), F('pr_nolisting', 9, 0, 0, 0)]), { todayJst: TODAY });
  assert.deepEqual([r.status, r.day_status, r.upgraded, r.rows, r.resolved, r.unresolved, r.scope], ['applied', 'complete', false, 4, 1, 3, 'jp']);
  assert.deepEqual((await frowsOf('2026-03-10')).map((x) => [x.source_code, x.sku_id == null ? null : Number(x.sku_id), x.qty, x.fba_available, x.fba_fc_transfer, x.fba_fc_processing, x.fba_customer_order, x.fba_inbound_working, x.fba_inbound_shipped, x.fba_inbound_received, x.scope_key]), [
    ['PR_SINGLE_001', skuA, 16, 10, 1, 2, 3, 4, 5, 6, 'jp'], ['pr_nolisting', null, 9, 9, 0, 0, 0, 0, 0, 0, 'jp'], ['pr_pack3_001', null, 8, 7, 0, 0, 1, 0, 0, 0, 'jp'], ['pr_set_001', null, 2, 2, 0, 0, 0, 0, 0, 0, 'jp']]);
  const run = await one(`select source_system, entity, scope_key, status, complete, format_version from ops.ingest_runs where ingest_run_id = $1`, [r.run_id]);
  assert.deepEqual([run.source_system, run.entity, run.scope_key, run.status, run.complete, run.format_version], ['amazon', 'stock_daily', 'jp', 'success', true, 'v1']);
  const v = await one(`select fba_jp_available, fba_jp_inbound, fba_jp_as_of::text as as_of from mart.v_sku_stock where sku_id = $1`, [skuA]);
  assert.deepEqual([Number(v.fba_jp_available), Number(v.fba_jp_inbound), v.as_of], [10, 15, '2026-03-10']);
  const vb = await one(`select fba_jp_available from mart.v_sku_stock where sku_id = $1`, [skuB]);
  assert.equal(Number(vb.fba_jp_available), 0, 'まとめ売りの 7 個を、SKU の 7 個として数えている');
});
await t('🚨 partial (RESTOCK が取れなかった日): FC 移管中・処理中・出荷待ち は null で受ける (0 ではなく不明)。qty は分かっている available だけ。日の状態は partial = view は読まない (as_of は前の complete の日のまま)。取込の記録も partial', async () => {
  const P = (code, a, w = 0) => ({ ...F(code, a, null, null, null, w), });
  const r = await ingestStockDay(db, fbody('2026-03-11', [P('PR_SINGLE_001', 99, 3), P('pr_pack3_001', 5)], { partial: true, captured_at_nominal: true }), { todayJst: TODAY });
  assert.deepEqual([r.status, r.day_status, r.rows], ['applied', 'partial', 2]);
  const d = await fdayOf('2026-03-11');
  assert.deepEqual([d.status, d.done], ['partial', false]);
  assert.deepEqual((await frowsOf('2026-03-11')).map((x) => [x.source_code, x.qty, x.fba_available, x.fba_fc_transfer, x.fba_fc_processing, x.fba_customer_order, x.fba_inbound_working]), [['PR_SINGLE_001', 99, 99, null, null, null, 3], ['pr_pack3_001', 5, 5, null, null, null, 0]]);
  const run = await one(`select status, complete, format_version from ops.ingest_runs where ingest_run_id = $1`, [r.run_id]);
  assert.deepEqual([run.status, run.complete, run.format_version], ['partial', false, 'v1-nominal-time']);
  const v = await one(`select fba_jp_available, fba_jp_as_of::text as as_of from mart.v_sku_stock where sku_id = $1`, [skuA]);
  assert.deepEqual([Number(v.fba_jp_available), v.as_of], [10, '2026-03-10'], 'partial の日を view が読んでいる');
  // 検証: partial なのに 3 区分に数字 / partial でないのに null / NE に partial / missing と partial / 7 区分の欠け・負・小数
  for (const bad of [fbody('2026-03-12', [F('x', 1, 0, 0, 0)], { partial: true }), fbody('2026-03-12', [F('x', 1, null, 0, 0)]), { ...body('2026-03-12', [{ code: 'x', qty: 1 }]), partial: true },
    { source: 'fba_jp', snapshot_date: '2026-03-12', missing: true, partial: true }, fbody('2026-03-12', [{ code: 'x', fba_available: 1 }]), fbody('2026-03-12', [F('x', -1, 0, 0, 0)]), fbody('2026-03-12', [F('x', 1.5, 0, 0, 0)]),
    fbody('2026-03-12', [F('x', 1, 0, 0, 0), F('x', 2, 0, 0, 0)]), fbody('2026-03-12', [F(' x', 1, 0, 0, 0)]), fbody('2026-03-12', [F('x', 2147483647, 1, 0, 0)]), { ...fbody('2026-03-12', [F('x', 1, 0, 0, 0)]), scope: 'main' }, { ...fbody('2026-03-12', [F('x', 1, 0, 0, 0)]), captured_at_nominal: 'yes' }])
    assert.throws(() => validateStockDayBody(bad, { todayJst: TODAY }), (e) => e.code === 'BAD_REQUEST', JSON.stringify(bad).slice(0, 140));
  assert.throws(() => fbaRowOf(F('x', 1, 2, 3, 4), true), /partial の日の fba_fc_transfer は null/);
  // 🚨 partial でない日でも、RESTOCK に載っていない SKU (PLANNING にしか無い) の 3 区分は null = 行ごとに「3 つとも数字」か「3 つとも null」(Codex #1388 R1 #1)
  assert.deepEqual([fbaRowOf(F('x', 5, null, null, null), false).qty, fbaRowOf(F('x', 5, 0, 0, 0), false).qty, fbaRowOf(F('x', 5, 1, 2, 3), false).qty], [5, 5, 11]);
  assert.throws(() => fbaRowOf(F('x', 1, null, 0, 0), false), /3 つとも数字か、3 つとも null/);
  // 🚨 表記だけ違う同じ SKU を 2 行で受けない (Company DB では同じ出品・同じ SKU に当たり、view が二重に数える。Codex #1388 R2 #1)。NE も同じ
  assert.deepEqual(['SKU-A', 'sku-a', 'ＳＫＵ－Ａ', 'sk u-a'].map(normCodeKey), ['sku-a', 'sku-a', 'sku-a', 'sku-a']);
  // 🚨 「同じ SKU か」を決めるのは DB (core.norm_code)。JS の鍵は DB より広くも狭くもしない (Codex #1388 R3): 鍵 = DB の式と同じ値・DB が NFKC しないものを JS が同じにしない
  const HK = String.fromCharCode(0xFF76), ZK = String.fromCharCode(0x30AB), C1 = String.fromCharCode(0x2460), IDOT = String.fromCharCode(0x130);
  const CODES = ['SKU-A', 'ＳＫＵ－Ａ', ' s k u' + String.fromCharCode(0x3000) + '-a', 'sku' + String.fromCharCode(0x2212) + 'a', 'sku-' + HK, 'sku-' + ZK, 'sku-' + C1, 'sku-1', 'PR_単品_001'];
  assert.deepEqual(CODES.map(normCodeKey), (await all(`select core.norm_code(c) as k from unnest($1::text[]) with ordinality as t(c, n) order by n`, [CODES])).map((x) => x.k), 'JS の鍵が core.norm_code と違う値を返す');
  assert.deepEqual([normCodeKey('sku-' + HK) === normCodeKey('sku-' + ZK), normCodeKey('sku-' + C1) === normCodeKey('sku-1'), looseCodeKey('sku-' + HK) === looseCodeKey('sku-' + ZK), looseCodeKey('sku-' + IDOT) === looseCodeKey('sku-i')], [false, false, true, true]);
  assert.throws(() => validateStockDayBody(fbody('2026-03-12', [F('PR_SINGLE_001', 10, 1, 1, 1), F('pr_single_001', 10, 1, 1, 1)]), { todayJst: TODAY }), /表記違いで別の行と同じものを指している/);
  assert.throws(() => validateStockDayBody(body('2026-03-12', [{ code: 'ne-aaa', qty: 1 }, { code: 'NE-AAA', qty: 1 }]), { todayJst: TODAY }), /表記違いで別の行と同じものを指している/);
  assert.throws(() => validateStockDayBody(fbody('2026-03-12', [F('x', 1, null, null, null)]), { todayJst: TODAY }), /RESTOCK の 3 区分の入った行が 1 つも無い/);
  const mixed = await ingestStockDay(db, fbody('2026-03-09', [F('PR_SINGLE_001', 3, 1, 1, 1), F('pr_planning_only', 8, null, null, null)]), { todayJst: TODAY });
  assert.deepEqual([mixed.day_status, (await frowsOf('2026-03-09')).map((x) => [x.source_code, x.qty, x.fba_customer_order])], ['complete', [['PR_SINGLE_001', 6, 1], ['pr_planning_only', 8, null]]]);
  assert.notEqual(fbaChecksum([F('x', 1, null, null, null)], true), fbaChecksum([F('x', 1, 0, 0, 0)], false), '「不明」と「0」が同じ指紋になっている');
});
await t('🚨 partial → complete だけは上げてよい (後から RESTOCK が取れた): 行を入れ替えて complete に。同じ内容の partial の再送 = same・違う内容の partial = CONFLICT・complete の日を partial で送る = CONFLICT (先に確定した日は書き換えない)。途中で落ちたら partial のまま残る', async () => {
  const P = (code, a) => F(code, a, null, null, null);
  assert.equal((await ingestStockDay(db, fbody('2026-03-11', [P('pr_pack3_001', 5), { ...P('PR_SINGLE_001', 99), fba_inbound_working: 3 }], { partial: true, captured_at_nominal: true }), { todayJst: TODAY })).status, 'same');
  await assert.rejects(ingestStockDay(db, fbody('2026-03-11', [P('PR_SINGLE_001', 98)], { partial: true }), { todayJst: TODAY }), (e) => e.code === 'CONFLICT');
  await assert.rejects(ingestStockDay(db, fbody('2026-03-10', [P('PR_SINGLE_001', 10)], { partial: true }), { todayJst: TODAY }), (e) => e.code === 'CONFLICT');
  const runsBefore = Number((await one(`select count(*)::int as n from ops.ingest_runs where entity = 'stock_daily'`)).n);
  // 🚨 上げる版に、前の版にあった SKU (pr_pack3_001) が無ければ上げない: 消えた SKU は view で在庫 0 に見える (PLANNING が取れなかった回の RESTOCK だけで上げる形。Codex #1388 R2 #2)。
  //    エラーにはしない (本当に出品が消えた日に、送り手が毎朝 ❌ になるだけ) = kept_partial・何も書き換えない・取込の記録も残さない
  const runsBeforeKp = (await one(`select count(*)::int as n from ops.ingest_runs`)).n;
  const kp = await ingestStockDay(db, fbody('2026-03-11', [F('PR_SINGLE_001', 20, 1, 1, 1)]), { todayJst: TODAY });
  assert.deepEqual([kp.status, kp.day_status, kp.upgraded, kp.gone_count, kp.gone], ['kept_partial', 'partial', false, 1, ['pr_pack3_001']]);
  assert.deepEqual([(await fdayOf('2026-03-11')).status, (await frowsOf('2026-03-11')).map((x) => x.qty), (await one(`select count(*)::int as n from ops.ingest_runs`)).n], ['partial', [99, 5], runsBeforeKp]);
  // 前の版は小文字 (PLANNING の表記)・上げる版は大文字 (RESTOCK の表記) は、DB でも同じ SKU = 「ある」
  const UP = [F('PR_SINGLE_001', 20, 1, 1, 1), F('PR_PACK3_001', 5, null, null, null)];
  await assert.rejects(ingestStockDay(db, fbody('2026-03-11', UP), { todayJst: TODAY, afterWrite: async () => { throw new Error('上げる途中で落ちた'); } }), /上げる途中で落ちた/);
  assert.deepEqual([(await fdayOf('2026-03-11')).status, (await frowsOf('2026-03-11')).map((x) => x.qty), Number((await one(`select count(*)::int as n from ops.ingest_runs where entity = 'stock_daily'`)).n)], ['partial', [99, 5], runsBefore]);
  const up = await ingestStockDay(db, fbody('2026-03-11', UP), { todayJst: TODAY });
  assert.deepEqual([up.status, up.day_status, up.upgraded, (await fdayOf('2026-03-11')).status, (await frowsOf('2026-03-11')).map((x) => [x.source_code, x.qty, x.fba_fc_transfer])], ['applied', 'complete', true, 'complete', [['PR_PACK3_001', 5, null], ['PR_SINGLE_001', 23, 1]]]);
  // 🚨 比べるのは DB の同一性 (core.norm_code)。JS の広い鍵 (NFKC) で比べると、DB では別の SKU (半角カナと全角カナ) を「ある」と読んで、前の版の SKU が黙って消える (Codex #1388 R3 が 7 → 0 を再現)
  const HKc = 'sku-' + String.fromCharCode(0xFF76), ZKc = 'sku-' + String.fromCharCode(0x30AB);
  await ingestStockDay(db, fbody('2026-03-13', [P(HKc, 7)], { partial: true }), { todayJst: TODAY });
  const kp2 = await ingestStockDay(db, fbody('2026-03-13', [F(ZKc, 7, 0, 0, 0)]), { todayJst: TODAY });
  assert.deepEqual([kp2.status, kp2.gone, (await frowsOf('2026-03-13')).map((x) => [x.source_code, x.qty])], ['kept_partial', [HKc], [[HKc, 7]]]);
  // 🚨 逆向き: JS の鍵では別 (İ と i) でも、DB の lower() が同じにするなら 2 行で受けない (同じ出品に当たれば二重に数える)。判定は DB に聞く = 照合環境が変わっても DB の答えに従う
  const IDc = 'sku-' + String.fromCharCode(0x130);
  const dbSame = (await one(`select core.norm_code($1) = core.norm_code($2) as same`, [IDc, 'sku-i'])).same;
  const twoRows = ingestStockDay(db, fbody('2026-03-14', [F(IDc, 10, 0, 0, 0), F('sku-i', 10, 0, 0, 0)]), { todayJst: TODAY });
  if (dbSame) { await assert.rejects(twoRows, (e) => e.code === 'BAD_REQUEST' && /表記違いで別の行と同じものを指している/.test(e.message)); assert.equal(await fdayOf('2026-03-14'), undefined); }
  else assert.equal((await twoRows).rows, 2);
  const v = await one(`select fba_jp_available, fba_jp_as_of::text as as_of from mart.v_sku_stock where sku_id = $1`, [skuA]);
  assert.deepEqual([Number(v.fba_jp_available), v.as_of], [20, '2026-03-11']);
  // US: scope は us・出品は amazon_us で探す (無ければ sku_id = null)。JP の同じ日とぶつからない
  const us = await ingestStockDay(db, { source: 'fba_us', snapshot_date: '2026-03-11', captured_at: capOf('2026-03-11'), rows: [F('PR_SINGLE_001', 4, 0, 0, 0)] }, { todayJst: TODAY });
  assert.deepEqual([us.scope, us.resolved, (await frowsOf('2026-03-11', 'fba_us')).map((x) => [x.scope_key, x.sku_id, x.qty])], ['us', 0, [['us', null, 4]]]);
});

console.log('Render の受け口 (本物の router を HTTP で) と送り手');
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
// 🚨 受け口は「JST の今日」を自分で決める (外から渡せない) → HTTP の試験は実際の今日を基準に日付を作る
const realToday = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
const ago = (n) => new Date(Date.parse(`${realToday}T00:00:00Z`) - n * 86400000).toISOString().slice(0, 10);
await t('受け口: 鍵が無ければ 401 / 検証に通らなければ 400 / applied → same (200) / 違う内容は 409 / 未来の日付は 400 (今日は受け口が決める) / status は期間の日ごとの状態と指紋', async () => {
  assert.equal((await http('POST', '/stock-daily', { body: body(ago(3), [{ code: 'ne-aaa', qty: 1 }]), key: null })).status, 401);
  assert.equal((await http('POST', '/stock-daily', { body: { source: 'ne' } })).status, 400);
  assert.equal((await http('POST', '/stock-daily', { body: body(ago(-1), [{ code: 'ne-aaa', qty: 1 }]) })).status, 400);
  const a = await http('POST', '/stock-daily', { body: body(ago(3), [{ code: 'ne-aaa', qty: 1 }, { code: 'zzz', qty: 2 }]) });
  assert.deepEqual([a.status, a.json.status, a.json.rows, a.json.resolved, a.json.unresolved], [200, 'applied', 2, 1, 1]);
  assert.equal((await http('POST', '/stock-daily', { body: body(ago(3), [{ code: 'zzz', qty: 2 }, { code: 'ne-aaa', qty: 1 }]) })).json.status, 'same');
  const c = await http('POST', '/stock-daily', { body: body(ago(3), [{ code: 'ne-aaa', qty: 2 }]) });
  assert.deepEqual([c.status, c.json.code], [409, 'CONFLICT']);
  const st = await http('GET', `/stock-daily/status?source=ne&scope=main&from=${ago(5)}&to=${realToday}`);
  assert.deepEqual([st.status, st.json.days.map((x) => [x.snapshot_date, x.status, x.rows])], [200, [[ago(3), 'complete', 2]]]);
  assert.equal(st.json.days[0].checksum, a.json.checksum);
  for (const q of ['source=nowhere&from=2026-01-01&to=2026-01-02', `source=ne&from=${realToday}&to=${ago(1)}`, 'source=ne&from=2020-01-01&to=2026-01-01', 'source=ne&scope=x&from=2026-01-01&to=2026-01-02'])
    assert.equal((await http('GET', `/stock-daily/status?${q}`)).status, 400, q);
});

await t('🚨 本番の middleware の順でも 4MB の上限と「鍵の検査が先」が効く (Codex R1 #2): server.js は stock-daily を、事前の鍵の検査と、共通の 10MB parser の素通りの両方に入れている。共通 parser を前に置いた app でも、5MB の本文は 413 (DB まで行かない)・鍵なしは本文を読む前に 401', async () => {
  const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
  const srv = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  assert.match(srv, /app\.use\(\[[^\]]*'\/apps\/company-db\/sync\/stock-daily'[^\]]*\], companyDbRequireSyncKey\);/);
  assert.match(srv, /normalizedPath\.toLowerCase\(\)\.startsWith\('\/apps\/company-db\/sync\/stock-daily'\)\) return next\(\);/);
  // server.js と同じ順を組む: 事前の鍵の検査 → 共通 parser (stock-daily は素通り) → router
  const app2 = express();
  app2.use(['/apps/company-db/sync/stock-daily'], requireSyncKey);
  const common = express.json({ limit: '10mb' });
  app2.use((req, res, next) => (req.method === 'POST' && req.path.toLowerCase().startsWith('/apps/company-db/sync/stock-daily') ? next() : common(req, res, next)));
  app2.use('/apps/company-db/sync', companyDbRouter);
  const srv2 = await new Promise((resolve) => { const x = app2.listen(0, '127.0.0.1', () => resolve(x)); });
  try {
    const url = `http://127.0.0.1:${srv2.address().port}/apps/company-db/sync/stock-daily`;
    const big = JSON.stringify({ ...body(ago(30), [{ code: 'ne-aaa', qty: 1 }]), pad: 'x'.repeat(5 * 1024 * 1024) });
    const r1 = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': 'k' }, body: big });
    assert.equal(r1.status, 413);
    const r2 = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: big });
    assert.equal(r2.status, 401);
    assert.equal((await all(`select 1 from snapshots.stock_capture_days where source = 'ne' and snapshot_date = $1::date`, [ago(30)])).length, 0);
  } finally { srv2.close(); }
});

// 送り手: メモリ上の warehouse.db (本番と同じ列)
const wh = new Database(':memory:');
wh.exec(`CREATE TABLE ne_stock_daily_snapshot (business_date TEXT NOT NULL, 商品コード TEXT NOT NULL, 在庫数 INTEGER NOT NULL, captured_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), PRIMARY KEY (business_date, 商品コード))`);
const put = (date, code, qty) => wh.prepare(`insert or replace into ne_stock_daily_snapshot (business_date, 商品コード, 在庫数, captured_at) values (?, ?, ?, ?)`).run(date, code, qty, capOf(date));
const push = (x = {}) => pushStockDaily({ source: 'ne', warehouse: wh, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, ...x });
await t('送り手: 台帳を持たず Render に聞く。まだ無い日だけ送る・元データに無い過去の日は missing と申告・2 回目は何も送らない (確定済み)', async () => {
  for (const d of [ago(9), ago(8), ago(6)]) { put(d, 'ne-aaa', 3); put(d, 'ne-bbb', 4); put(d, 'ne-ccc', 0); }
  put(realToday, 'ne-aaa', 1);
  const r = await push({ from: ago(9), to: ago(6) });
  assert.deepEqual([r.ok, r.sent.map((s) => [s.date, s.rows, s.unresolved]), r.missingDeclared, r.done], [true, [[ago(9), 3, 1], [ago(8), 3, 1], [ago(6), 3, 1]], [ago(7)], 0]);
  assert.match(r.lastLine, /^✅ Company DB 在庫日次 \(NE\) .*: 送った 3 日 \(9 行 \/ SKU が分からない 3\) \/ 確定済み 0 日 \/ 取れていない日を申告 1 日/);
  assert.deepEqual((await all(`select snapshot_date::text as d, status from snapshots.stock_capture_days where source = 'ne' and snapshot_date between $1::date and $2::date order by 1`, [ago(9), ago(6)])).map((x) => [x.d, x.status]),
    [[ago(9), 'complete'], [ago(8), 'complete'], [ago(7), 'missing'], [ago(6), 'complete']]);
  const again = await push({ from: ago(9), to: ago(6) });
  assert.deepEqual([again.ok, again.sent.length, again.done, again.missingKept, again.missingDeclared.length], [true, 0, 3, 1, 0]);
});
await t('送り手: 確定済みの日と元データの内容が違えば ⚠️ に出すだけ (書き換えない) / missing と申告した日に後から元データが入れば送る', async () => {
  put(ago(8), 'ne-aaa', 99);
  put(ago(7), 'ne-aaa', 5);
  const r = await push({ from: ago(9), to: ago(6) });
  assert.deepEqual([r.ok, r.mismatched, r.sent.map((s) => s.date)], [true, [ago(8)], [ago(7)]]);
  assert.match(r.lastLine, /^⚠️ .*確定済みと内容が違う日 1/);
  assert.equal(Number((await one(`select qty from snapshots.sku_stock_daily where snapshot_date = $1::date and source = 'ne' and source_code = 'ne-aaa'`, [ago(8)])).qty), 3);
  assert.equal((await one(`select status from snapshots.stock_capture_days where snapshot_date = $1::date and source = 'ne'`, [ago(7)])).status, 'complete');
});
await t('🚨 送り手: 今日の元データが無ければ失敗 (missing にしない) / 検証に通らない行が 1 つでもある日は送らない (部分的な日を作らない) / --days の既定は直近 14 日 / dry-run は送らない', async () => {
  wh.prepare(`delete from ne_stock_daily_snapshot where business_date = ?`).run(realToday);
  const noToday = await push({ days: 2 });
  assert.deepEqual([noToday.ok, noToday.failed.map((f) => f.date), /^❌ /.test(noToday.lastLine), /朝の在庫スナップショットが先に要る/.test(noToday.lastLine)], [false, [realToday], true, true]);
  assert.equal((await all(`select 1 from snapshots.stock_capture_days where source = 'ne' and snapshot_date = $1::date`, [realToday])).length, 0);
  put(ago(2), 'ne-aaa', 1); put(ago(2), ' ne-bad', 2);
  const bad = await push({ from: ago(2), to: ago(2) });
  assert.deepEqual([bad.ok, bad.failed.length, /検証に通らない行が 1 件/.test(bad.failed[0].error)], [false, 1, true]);
  assert.equal((await all(`select 1 from snapshots.stock_capture_days where source = 'ne' and snapshot_date = $1::date`, [ago(2)])).length, 0);
  wh.prepare(`delete from ne_stock_daily_snapshot where 商品コード = ' ne-bad'`).run();
  put(realToday, 'ne-aaa', 2);
  const dry = await push({ dryRun: true });
  assert.deepEqual([dry.from, dry.to, dry.dryRun, / \[dry-run = 送っていない\]$/.test(dry.lastLine)], [ago(13), realToday, true, true]);
  assert.equal((await all(`select 1 from snapshots.stock_capture_days where source = 'ne' and snapshot_date = $1::date`, [realToday])).length, 0, 'dry-run が送っている');
  const real = await push({});
  assert.ok(real.ok && real.sent.some((s) => s.date === realToday), JSON.stringify(real.failed));
});
await t('送り手: --all は元データの最初の日から / 引数の検査 / 行の検証は受け口と同じ規則 / 受け口が落ちていれば失敗 (exit 1 の材料)', async () => {
  const allRun = await push({ all: true });
  assert.deepEqual([allRun.from, allRun.ok], [ago(9), true]);
  for (const bad of [[], ['--source', 'fba'], ['--source', 'ne', '--days', '0'], ['--source', 'ne', '--days', '14', '--all'], ['--source', 'ne', '--from', '2026-09-01'], ['--source', 'ne', '--from', '2026-09-31', '--to', '2026-10-01'],
    ['--source', 'ne', '--from', '2026-09-02', '--to', '2026-09-01'], ['--source'], ['--source', 'ne', '--nope'], ['--source', 'toString'], ['--source', 'fba_eu']])
    assert.throws(() => parseArgs(bad), Error, JSON.stringify(bad));
  assert.deepEqual(parseArgs(['--source', 'ne', '--days', '14']), { source: 'ne', days: 14, from: null, to: null, all: false, dryRun: false, dataDir: null });
  assert.deepEqual(rowsOfDay([{ code: 'a', qty: 1 }, { code: 'a', qty: 2 }, { code: 'b ', qty: 1 }, { code: 'c', qty: -1 }, { code: 'd', qty: 1.5 }, { code: 'e', qty: 0 }]).rows, [{ code: 'a', qty: 1 }, { code: 'e', qty: 0 }]);
  assert.deepEqual(datesBetween('2026-02-27', '2026-03-01'), ['2026-02-27', '2026-02-28', '2026-03-01']);
  assert.deepEqual(Object.keys(SOURCES), Object.keys(STOCK_SOURCES), '送り手と受け口の source の一覧がずれている');
  const down = await pushStockDaily({ source: 'ne', warehouse: wh, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(20), to: ago(20),
    fetchImpl: async (url, init) => ((init && init.method === 'POST') ? { ok: false, status: 502, text: async () => 'Bad Gateway' } : fetch(url, init)) });
  assert.deepEqual([down.ok, down.failed.length, /HTTP 502/.test(down.failed[0].error)], [false, 1, true]);
  await assert.rejects(pushStockDaily({ source: 'ne', warehouse: wh, base: BASE_URL, syncKey: 'wrong', today: realToday, log: quiet, days: 1 }), /Render の状態が取れない: HTTP 401/);
});

await t('🚨 元データの日付の形が違う行を、黙って範囲の外に落とさない (Codex R1 #1 の再現): 2026-..T00:00:00 の形の日付が 1 行でもあれば、どの日も送らない (その日を missing と申告したり、形の合う行だけで complete にしない)', async () => {
  const wh2 = new Database(':memory:');
  wh2.exec(`CREATE TABLE ne_stock_daily_snapshot (business_date TEXT NOT NULL, 商品コード TEXT NOT NULL, 在庫数 INTEGER NOT NULL, captured_at TEXT NOT NULL, PRIMARY KEY (business_date, 商品コード))`);
  const ins = wh2.prepare(`insert into ne_stock_daily_snapshot values (?, ?, ?, ?)`);
  ins.run(`${ago(40)}T00:00:00`, 'ne-aaa', 5, `${ago(40)}T22:00:00.000Z`);   // 形の違う日付だけの日 → 以前は missing と申告して ok だった
  ins.run(ago(41), 'ne-aaa', 5, `${ago(41)}T22:00:00.000Z`);
  const calls = [];
  const f = async (url, init) => { calls.push((init && init.method) || 'GET'); return fetch(url, init); };
  await assert.rejects(pushStockDaily({ source: 'ne', warehouse: wh2, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(41), to: ago(40), fetchImpl: f }), /読めない日付が 1 種類ある .*どの日も送らない/);
  assert.deepEqual(calls, [], '読めない日付があるのに Render へ要求している');
  assert.equal((await all(`select 1 from snapshots.stock_capture_days where source = 'ne' and snapshot_date between $1::date and $2::date`, [ago(41), ago(40)])).length, 0);
  wh2.close();
});
await t('🚨 Render の状態の応答は 1 件ずつ確かめる (Codex R1 #3): 状態の無い行・知らない状態・building・範囲の外・同じ日が 2 つ・complete なのに指紋なし は例外 (「確定済み」と読んで成功にしない)。missing の申告への応答も確かめる / 取得時刻にタイムゾーンが無い日は送らない', async () => {
  const statusOnly = (days) => async (url, init) => ((init && init.method === 'POST') ? fetch(url, init) : { ok: true, status: 200, json: async () => ({ days }), text: async () => '' });
  const run = (days, x = {}) => pushStockDaily({ source: 'ne', warehouse: wh, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(9), to: ago(6), fetchImpl: statusOnly(days), ...x });
  const sum = 'a'.repeat(64);
  for (const [days, re] of [[[{ snapshot_date: ago(9) }], /分からない状態/], [[{ snapshot_date: ago(9), status: 'done' }], /分からない状態/], [[{ snapshot_date: ago(9), status: 'building' }], /分からない状態/],
    [[{ snapshot_date: ago(3), status: 'complete', checksum: sum }], /範囲の外の日付/], [[{ snapshot_date: '2026-9-1', status: 'missing' }], /読めない・範囲の外/],
    [[{ snapshot_date: ago(9), status: 'missing' }, { snapshot_date: ago(9), status: 'missing' }], /同じ日が 2 つ/], [[{ snapshot_date: ago(9), status: 'complete' }], /内容の指紋が無い/], [[{ snapshot_date: ago(9), status: 'complete', checksum: 'xyz' }], /内容の指紋が無い/], [[null], /読めない/]])
    await assert.rejects(run(days), re, JSON.stringify(days));
  const badMissing = async (url, init) => ((init && init.method === 'POST') ? { ok: true, status: 200, text: async () => JSON.stringify({ status: 'applied' }) } : { ok: true, status: 200, json: async () => ({ days: [] }), text: async () => '' });
  const wh3 = new Database(':memory:');
  wh3.exec(`CREATE TABLE ne_stock_daily_snapshot (business_date TEXT NOT NULL, 商品コード TEXT NOT NULL, 在庫数 INTEGER NOT NULL, captured_at TEXT NOT NULL, PRIMARY KEY (business_date, 商品コード))`);
  wh3.prepare(`insert into ne_stock_daily_snapshot values (?, ?, ?, ?)`).run(ago(50), 'ne-aaa', 1, `${ago(50)} 22:00:00`);   // タイムゾーンの無い取得時刻
  const m = await pushStockDaily({ source: 'ne', warehouse: wh3, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(51), to: ago(50), fetchImpl: badMissing });
  assert.deepEqual([m.ok, m.failed.map((x) => x.date), /missing の申告への応答が分からない/.test(m.failed[0].error), /captured_at が読めない/.test(m.failed[1].error)], [false, [ago(51), ago(50)], true, true]);
  wh3.close();
});

await t('🚨 取得時刻は行ごとに確かめ、1 日に 1 つだけ (Codex R2 #1 の再現): 正しい時刻の行に 2/30 の行が混ざっていても max() で隠れない・別の時刻が 2 つある日 (取り直しの途中) も送らない・offset 違いの同じ瞬間は 1 つと数える', async () => {
  const mk = () => { const w = new Database(':memory:'); w.exec(`CREATE TABLE ne_stock_daily_snapshot (business_date TEXT NOT NULL, 商品コード TEXT NOT NULL, 在庫数 INTEGER NOT NULL, captured_at TEXT NOT NULL, PRIMARY KEY (business_date, 商品コード))`); return w; };
  const w = mk(); const ins = w.prepare(`insert into ne_stock_daily_snapshot values (?, ?, ?, ?)`);
  ins.run(ago(60), 'ne-aaa', 1, `${ago(61)}T22:01:23.005Z`); ins.run(ago(60), 'ne-bbb', 2, '2026-02-30T01:00:00Z');           // 不正な時刻が混ざる (文字列の最大は正しいほう)
  ins.run(ago(59), 'ne-aaa', 1, `${ago(60)}T22:01:23.005Z`); ins.run(ago(59), 'ne-bbb', 2, `${ago(60)}T23:01:23.005Z`);     // 時刻が 2 つ
  ins.run(ago(58), 'ne-aaa', 1, `${ago(59)}T22:01:23.005Z`); ins.run(ago(58), 'ne-bbb', 2, `${ago(58)}T07:01:23.005+09:00`); // 同じ瞬間の別の書き方
  const r = await pushStockDaily({ source: 'ne', warehouse: w, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(60), to: ago(58) });
  assert.deepEqual([r.ok, r.failed.map((f) => f.date), r.sent.map((x) => x.date)], [false, [ago(60), ago(59)], [ago(58)]]);
  assert.match(r.failed[0].error, /captured_at が読めない行がある/);
  assert.match(r.failed[1].error, /取得時刻が 2 つある/);
  assert.equal((await all(`select 1 from snapshots.stock_capture_days where source = 'ne' and snapshot_date in ($1::date, $2::date)`, [ago(60), ago(59)])).length, 0);
  w.close();
});
await t('🚨 送る内容は 1 つの読み取り取引で確定する (Codex R2 #2 の再現): Render の応答を待つ間にスナップショットが取り直されても、在庫数と取得時刻は同じ世代のまま送る (08:01 の在庫数を 07:01 の時刻で送らない)', async () => {
  const w = new Database(':memory:');
  w.exec(`CREATE TABLE ne_stock_daily_snapshot (business_date TEXT NOT NULL, 商品コード TEXT NOT NULL, 在庫数 INTEGER NOT NULL, captured_at TEXT NOT NULL, PRIMARY KEY (business_date, 商品コード))`);
  const ins = w.prepare(`insert or replace into ne_stock_daily_snapshot values (?, ?, ?, ?)`);
  const oldT = `${ago(71)}T22:01:23.005Z`, newT = `${ago(71)}T23:01:23.005Z`;
  ins.run(ago(71), 'ne-aaa', 1, `${ago(72)}T22:01:23.005Z`); ins.run(ago(70), 'ne-aaa', 10, oldT);
  const posted = [];
  const f = async (url, init) => {
    if (init && init.method === 'POST') {
      const b = JSON.parse(init.body); posted.push([b.snapshot_date, b.captured_at, b.rows.map((x) => x.qty)]);
      if (b.snapshot_date === ago(71)) ins.run(ago(70), 'ne-aaa', 99, newT);   // 1 日目の応答を待つ間に、2 日目が取り直された
    }
    return fetch(url, init);
  };
  const r = await pushStockDaily({ source: 'ne', warehouse: w, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(71), to: ago(70), fetchImpl: f });
  assert.ok(r.ok, JSON.stringify(r.failed));
  assert.deepEqual(posted[1], [ago(70), oldT, [10]], '在庫数と取得時刻が別の世代になっている');
  assert.ok(WINDOW_DAYS >= 14, 'ふだんの 14 日は 1 回の読み取りに収まる');
  // readWindow は 1 つの取引の中で読む (better-sqlite3 の transaction)。日付の形の検査も同じ取引の中
  let inTx = null; const spy = { prepare: (q) => { inTx = w.inTransaction; return w.prepare(q); }, transaction: (fn) => w.transaction(fn) };
  readWindow(spy, SOURCES.ne, ago(71), ago(70));
  assert.equal(inTx, true);
  w.close();
});
await t('受け口の上限は送る前に確かめる (dry-run でも分かる): 50,001 行の日は POST せずに失敗', async () => {
  const w = new Database(':memory:');
  w.exec(`CREATE TABLE ne_stock_daily_snapshot (business_date TEXT NOT NULL, 商品コード TEXT NOT NULL, 在庫数 INTEGER NOT NULL, captured_at TEXT NOT NULL, PRIMARY KEY (business_date, 商品コード))`);
  const ins = w.prepare(`insert into ne_stock_daily_snapshot values (?, ?, ?, ?)`);
  w.transaction(() => { for (let i = 0; i <= MAX_ROWS; i++) ins.run(ago(80), `c${i}`, 1, `${ago(81)}T22:01:23.005Z`); })();
  let posts = 0;
  const f = async (url, init) => { if (init && init.method === 'POST') posts++; return fetch(url, init); };
  const r = await pushStockDaily({ source: 'ne', warehouse: w, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(80), to: ago(80), dryRun: true, fetchImpl: f });
  assert.deepEqual([r.ok, posts, /行が多すぎる \(50001 > 受け口の上限 50000\)/.test(r.failed[0].error)], [false, 0, true]);
  w.close();
});

console.log('送り手: FBA (fba.db)');
const mkFba = (file = ':memory:') => {
  const f = new Database(file);
  const cols = 'id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_date TEXT NOT NULL, amazon_sku TEXT NOT NULL, product_name TEXT, fba_available INTEGER DEFAULT 0, fba_inbound_working INTEGER DEFAULT 0, fba_inbound_shipped INTEGER DEFAULT 0, fba_inbound_received INTEGER DEFAULT 0, fba_fc_transfer INTEGER DEFAULT 0, fba_fc_processing INTEGER DEFAULT 0, fba_customer_order INTEGER DEFAULT 0, fba_unfulfillable INTEGER DEFAULT 0, UNIQUE(snapshot_date, amazon_sku)';
  f.exec(`CREATE TABLE daily_snapshots (${cols}); CREATE TABLE daily_snapshots_us (${cols});
    CREATE TABLE cdb_stock_export_days (snapshot_date TEXT NOT NULL, market TEXT NOT NULL, captured_at TEXT NOT NULL, restock_rows INTEGER NOT NULL, planning_rows INTEGER NOT NULL, PRIMARY KEY (snapshot_date, market));
    CREATE TABLE cdb_stock_export (snapshot_date TEXT NOT NULL, market TEXT NOT NULL, amazon_sku TEXT NOT NULL, fba_available INTEGER NOT NULL, fba_inbound_working INTEGER NOT NULL, fba_inbound_shipped INTEGER NOT NULL, fba_inbound_received INTEGER NOT NULL, fba_fc_transfer INTEGER, fba_fc_processing INTEGER, fba_customer_order INTEGER, PRIMARY KEY (snapshot_date, market, amazon_sku))`);
  return f;
};
const fput = (f, table, date, sku, a, x = 0, p2 = 0, c = 0, w = 0) => f.prepare(`insert or replace into ${table} (snapshot_date, amazon_sku, product_name, fba_available, fba_fc_transfer, fba_fc_processing, fba_customer_order, fba_inbound_working) values (?, ?, '商品名は送らない', ?, ?, ?, ?, ?)`).run(date, sku, a, x, p2, c, w);
/** 送る版 (朝のスナップショットが作るもの) を入れる。rows = [[sku, available, x, p, c, working]] (x/p/c が null = RESTOCK に載っていない SKU) */
const fexp = (f, date, market, capturedAt, rows) => {
  f.prepare(`insert or replace into cdb_stock_export_days values (?, ?, ?, ?, ?)`).run(date, market, capturedAt, rows.filter((r) => r[2] != null).length, rows.length);
  f.prepare(`delete from cdb_stock_export where snapshot_date = ? and market = ?`).run(date, market);
  for (const [sku, a, x = null, p2 = null, c = null, w = 0] of rows) f.prepare(`insert into cdb_stock_export values (?, ?, ?, ?, ?, 0, 0, ?, ?, ?)`).run(date, market, sku, a, w, x, p2, c);
};
const fpush = (f, source, x = {}) => pushStockDaily({ source, warehouse: f, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, ...x });
await t('🚨 送るのは daily_snapshots ではなく、朝のスナップショットが作った「送る版」(Codex #1388 R1): RESTOCK に載っていない SKU の 3 区分は null のまま complete で送る・RESTOCK が丸ごと無い版は partial・取得時刻は版のもの。版の無い過去の日は **推定しない** = daily_snapshots に数字があっても 3 区分は null・partial・定刻 (nominal)。商品名は送らない', async () => {
  const f = mkFba();
  fput(f, 'daily_snapshots', ago(30), 'PR_SINGLE_001', 777, 7, 7, 7, 7);   // daily_snapshots の値は、版がある日は使わない (取り直しで変わり得る)
  fexp(f, ago(30), 'jp', capOf(ago(30)), [['PR_SINGLE_001', 10, 1, 2, 3, 4], ['pr_planning_only', 8]]);   // 版あり・RESTOCK あり → complete (PLANNING にしか無い SKU は null)
  fexp(f, ago(29), 'jp', capOf(ago(29)), [['PR_SINGLE_001', 11, null, null, null, 5]]);                    // 版あり・RESTOCK なし → partial
  for (let i = 0; i < 120; i++) fput(f, 'daily_snapshots', ago(28), `sku-${String(i).padStart(3, '0')}`, i, 0, 0, i === 7 ? 2 : 0);   // 版なし (過去の日)。どこかに数字があっても complete と推定しない
  const posted = [];
  const spy = async (url, init) => { if (init && init.method === 'POST') posted.push(JSON.parse(init.body)); return fetch(url, init); };
  const r = await fpush(f, 'fba_jp', { from: ago(30), to: ago(28), fetchImpl: spy });
  assert.ok(r.ok, JSON.stringify(r.failed));
  assert.deepEqual(posted.map((b) => [b.snapshot_date, b.partial === true, b.captured_at_nominal === true, b.rows.length, b.scope]), [[ago(30), false, false, 2, 'jp'], [ago(29), true, false, 1, 'jp'], [ago(28), true, true, 120, 'jp']]);
  assert.deepEqual([posted[0].rows, posted[0].captured_at], [[{ code: 'PR_SINGLE_001', fba_available: 10, fba_fc_transfer: 1, fba_fc_processing: 2, fba_customer_order: 3, fba_inbound_working: 4, fba_inbound_shipped: 0, fba_inbound_received: 0 },
    { code: 'pr_planning_only', fba_available: 8, fba_fc_transfer: null, fba_fc_processing: null, fba_customer_order: null, fba_inbound_working: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 }], capOf(ago(30))]);
  assert.deepEqual([posted[2].rows[7].fba_available, posted[2].rows[7].fba_customer_order], [7, null], '版の無い日の 3 区分を送っている (推定している)');
  assert.equal(JSON.stringify(posted).includes('商品名'), false);
  assert.match(r.lastLine, /^✅ Company DB 在庫日次 \(FBA\) .*送った 3 日 .*うち一部だけ取れた日 \(partial\) 2 \/ 取得時刻の記録が無く定刻を入れた 1 日/);
  assert.deepEqual((await all(`select status from snapshots.stock_capture_days where source = 'fba_jp' and snapshot_date between $1::date and $2::date order by snapshot_date`, [ago(30), ago(28)])).map((x) => x.status), ['complete', 'partial', 'partial']);
  // 2 回目は何も送らない / 後から RESTOCK のある版になった日は送って上げる (版は「RESTOCK なし → あり」のときだけ入れ替わる)
  const again = await fpush(f, 'fba_jp', { from: ago(30), to: ago(28) });
  assert.deepEqual([again.ok, again.sent.length, again.done, again.mismatched], [true, 0, 3, []]);
  fexp(f, ago(29), 'jp', capOf(ago(29)), [['PR_SINGLE_001', 11, 1, 1, 1, 5]]);
  const up = await fpush(f, 'fba_jp', { from: ago(30), to: ago(28) });
  assert.deepEqual([up.ok, up.sent.map((x) => x.date), up.upgraded, /partial から上げた 1 日/.test(up.lastLine)], [true, [ago(29)], 1, true]);
  assert.equal((await one(`select status from snapshots.stock_capture_days where source = 'fba_jp' and snapshot_date = $1::date`, [ago(29)])).status, 'complete');
  // 🚨 上げる版に、前の版 (partial・120 SKU) にあった SKU が無い → 受け口は上げない (kept_partial)。送り手は失敗にせず ⚠️ で知らせる (本当に出品が消えた日に、範囲を抜けるまで毎朝 ❌ にしない)
  fexp(f, ago(28), 'jp', capOf(ago(28)), [['sku-007', 7, 0, 0, 2, 0]]);
  const kept = await fpush(f, 'fba_jp', { from: ago(30), to: ago(28) });
  assert.deepEqual([kept.ok, kept.sent.length, kept.upgraded, kept.keptPartial.map((k) => [k.date, k.goneCount, k.gone[0]])], [true, 0, 0, [[ago(28), 119, 'sku-000']]]);
  assert.match(kept.lastLine, /^⚠️ Company DB 在庫日次 \(FBA\) .*complete に上げなかった日 1 \(.*前の版にあった SKU が 119 件無い 例 sku-000/);
  assert.deepEqual([(await fdayOf(ago(28))).status, (await frowsOf(ago(28))).length], ['partial', 120]);
  // 版の表がまだ無い fba.db (常駐サーバが古い版のまま) でも動く = 全部「版の無い日」
  const old = new Database(':memory:');
  old.exec('CREATE TABLE daily_snapshots (snapshot_date TEXT NOT NULL, amazon_sku TEXT NOT NULL, fba_available INTEGER DEFAULT 0, fba_inbound_working INTEGER DEFAULT 0, fba_inbound_shipped INTEGER DEFAULT 0, fba_inbound_received INTEGER DEFAULT 0, fba_fc_transfer INTEGER DEFAULT 0, fba_fc_processing INTEGER DEFAULT 0, fba_customer_order INTEGER DEFAULT 0)');
  old.prepare('insert into daily_snapshots (snapshot_date, amazon_sku, fba_available, fba_customer_order) values (?, ?, 3, 9)').run(ago(25), 'PR_SINGLE_001');
  const o = await fpush(old, 'fba_jp', { from: ago(25), to: ago(25), dryRun: true });
  assert.deepEqual([o.ok, o.sent.map((x) => [x.date, x.partial]), o.nominalDays], [true, [[ago(25), true]], 1]);
  old.close(); f.close();
});
await t('US: 版の無い日は partial・版のある日は complete。今日の行が無くても失敗にしない (US の取得は失敗しても朝のステップは成功)。JP は今日の行が無ければ失敗', async () => {
  const f = mkFba();
  fput(f, 'daily_snapshots_us', ago(12), 'US-SKU-1', 5, 0, 0, 0);
  fexp(f, ago(11), 'us', capOf(ago(11)), [['US-SKU-1', 6, 0, 1, 0]]);
  const us = await fpush(f, 'fba_us', { from: ago(12), to: realToday });
  assert.deepEqual([us.ok, us.todayAbsent, us.sent.map((x) => [x.date, x.partial]), us.missingDeclared.length], [true, true, [[ago(12), true], [ago(11), false]], 10]);
  assert.match(us.lastLine, /^✅ Company DB 在庫日次 \(FBA US\) .*今日の行はまだ無い \(失敗にしない\)/);
  fexp(f, ago(1), 'jp', capOf(ago(1)), [['PR_SINGLE_001', 1, 0, 0, 1]]);
  const jp = await fpush(f, 'fba_jp', { from: ago(1), to: realToday });
  assert.deepEqual([jp.ok, jp.failed.map((x) => x.date), /朝の在庫スナップショットが先に要る/.test(jp.failed[0].error)], [false, [realToday], true]);
  f.close();
});
await t('🚨 通し (取得結果 → 実物の正規化 → 実物の db.js の「送る版」→ 送り手が lock つきで fba.db を読む → 受け口 → view。Codex #1388 R2): 表記違いの同じ SKU を二重に数えない / レポートの「--」を 0 と確定しない / PLANNING が取れなかった回を complete にしない / US の版の失敗も最後の行に出る', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-fba-e2e-'));
  const prevDataDir = process.env.DATA_DIR;
  process.env.DATA_DIR = dir;   // db.js は import の時点で DATA_DIR を読む
  try {
    const fdb = await import(new URL('../apps/fba-replenishment/db.js', import.meta.url).href + '?e2e=' + Date.now());
    await fdb.initDb();
    const RS = (sku, a, x, p2, c, w = '0') => ({ 'Merchant SKU': sku, FNSKU: 'X0' + sku, ASIN: 'B0TEST', Available: String(a), 'FC Transfer': String(x), 'FC Processing': String(p2), 'Customer Order': String(c), Working: w, Shipped: '0', Receiving: '0' });
    const PL = (sku, a) => ({ sku, fnsku: 'X0' + sku, asin: 'B0TEST', available: String(a), 'inbound-working': '0', 'inbound-shipped': '0', 'inbound-received': '0' });
    const usCtx = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
    const snap = (date, jp, us = { restock: [], planning: [], errors: [] }) => runFbaReportSnapshot({ db: fdb, businessDate: date, fetchReports: async (ctx) => (ctx ? us : jp), usContext: usCtx, log: quiet, warn: quiet });
    // ① ふつうの朝: RESTOCK は大文字・PLANNING は小文字の同じ SKU + PLANNING にしか無い SKU。US は RESTOCK の値が「--」
    const s1 = await snap(ago(15), { restock: [RS('PR_SINGLE_001', 10, 1, 2, 3, '4')], planning: [PL('pr_single_001', 999), PL('pr_planning_only', 8)], errors: [] },
      { restock: [RS('US-SKU-1', 5, 0, '--', 0)], planning: [PL('US-SKU-1', 5)], errors: [] });
    assert.deepEqual([s1.ok, s1.jp.exportSaved, s1.us.exportSaved], [true, true, false]);
    assert.match(s1.lastLine, /^⚠️ .*Company DB へ送る版を作れなかった \(US: US-SKU-1 の fba_fc_processing が 0 以上の整数でない: NaN\)/);
    // ② レポートの「--」: JP の RESTOCK の値が数字でない朝 → 版を作らない (0 と確定しない)
    const s2 = await snap(ago(16), { restock: [RS('PR_SINGLE_001', 10, 1, 2, '--')], planning: [PL('PR_SINGLE_001', 10)], errors: [] });
    assert.deepEqual([s2.ok, s2.jp.exportSaved, /JP: PR_SINGLE_001 の fba_customer_order が 0 以上の整数でない: NaN/.test(s2.lastLine)], [true, false, true]);
    // ③ PLANNING が取れなかった朝: RESTOCK だけでは版を作らない
    const s3 = await snap(ago(17), { restock: [RS('PR_SINGLE_001', 10, 1, 2, 3)], planning: null, errors: [{ report: 'planning', error: 'timeout' }] });
    assert.deepEqual([s3.ok, s3.jp.exportSaved, s3.jp.exportError, /JP: no_planning/.test(s3.lastLine)], [true, false, 'no_planning', true]);
    // 送り手: lock つきで実ファイルを読む → 受け口
    const src = openSource(SOURCES.fba_jp, path.join(dir, 'fba.db'));
    const posted = [];
    const spy = async (url, init) => { if (init && init.method === 'POST') posted.push(JSON.parse(init.body)); return fetch(url, init); };
    const r = await pushStockDaily({ source: 'fba_jp', warehouse: src.handle, guard: src.guard, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(17), to: ago(15), fetchImpl: spy });
    assert.ok(r.ok, JSON.stringify(r.failed));
    assert.deepEqual(posted.map((b) => [b.snapshot_date, b.partial === true, b.captured_at_nominal === true, b.rows.map((x) => [x.code, x.fba_available, x.fba_customer_order])]), [
      [ago(17), true, true, [['PR_SINGLE_001', 10, null]]],                                        // PLANNING なし → 版なし → 推定せず partial
      [ago(16), true, true, [['PR_SINGLE_001', 10, null]]],                                        // 「--」→ 版なし → partial (出荷待ち 0 と確定しない)
      [ago(15), false, false, [['PR_SINGLE_001', 10, 3], ['pr_planning_only', 8, null]]]]);        // 版あり: 表記違いは 1 行・PLANNING にしか無い SKU は null
    // その日の行そのもの: 表記違いの同じ SKU は 1 行だけ (二重に数えない)・PLANNING にしか無い SKU は 3 区分が null。版を作れなかった日は partial = view は読まない
    assert.deepEqual((await frowsOf(ago(15))).map((x) => [x.source_code, x.sku_id, x.qty, x.fba_available, x.fba_customer_order]), [['PR_SINGLE_001', skuA, 16, 10, 3], ['pr_planning_only', null, 8, 8, null]]);
    assert.deepEqual([(await fdayOf(ago(15))).status, (await fdayOf(ago(16))).status, (await fdayOf(ago(17))).status], ['complete', 'partial', 'partial']);
    assert.deepEqual((await frowsOf(ago(16))).map((x) => [x.source_code, x.qty, x.fba_customer_order]), [['PR_SINGLE_001', 10, null]], 'レポートの「--」を出荷待ち 0 と確定している');
  } finally { if (prevDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prevDataDir; try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* */ } }
});
await t('🚨 fba.db は lock の中でだけ読む (常駐サーバが保存している最中のファイルを読まない): openSource は読むあいだだけ db.js と同じ lock (fba.db.lockdb) を取り、接続もそのあいだだけ開く。相手が lock を持っていれば待って FBA_DB_LOCK_TIMEOUT。NE (warehouse.db) は lock なし', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-fba-src-'));
  const file = path.join(dir, 'fba.db');
  const f = mkFba(file); fput(f, 'daily_snapshots', ago(3), 'PR_SINGLE_001', 1, 0, 0, 1); f.close();
  const src = openSource(SOURCES.fba_jp, file, { lockWaitMs: 300 });
  assert.throws(() => src.handle.prepare('select 1'), /lock の中でだけ読む/);
  assert.deepEqual(src.guard(() => SOURCES.fba_jp.allDates(src.handle)), [ago(3)]);
  assert.throws(() => src.handle.prepare('select 1'), /lock の中でだけ読む/, 'guard の外でも接続が開いたまま');
  const holder = new Database(lockDbFileOf(file)); holder.exec('BEGIN EXCLUSIVE');   // 常駐サーバが保存中、の役
  let code = null; try { src.guard(() => 1); } catch (e) { code = e.code; }
  holder.exec('ROLLBACK'); holder.close();
  assert.equal(code, 'FBA_DB_LOCK_TIMEOUT');
  assert.equal(src.guard(() => 2), 2);
  // pushStockDaily が guard を通して読む (readWindow・日付の事前検査・--all の最初の日)
  let guarded = 0; const g = (fn) => { guarded++; return src.guard(fn); };
  const r = await pushStockDaily({ source: 'fba_jp', warehouse: src.handle, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, from: ago(3), to: ago(3), dryRun: true, guard: g });
  assert.deepEqual([r.ok, r.sent.length, guarded >= 2], [true, 1, true]);
  const neFile = path.join(dir, 'warehouse.db'); new Database(neFile).close();   // readonly で開くので実ファイルが要る
  const ne = openSource(SOURCES.ne, neFile); assert.equal(ne.guard, undefined); ne.close();
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* */ }
});

server.close();
wh.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
