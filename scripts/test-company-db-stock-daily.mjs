#!/usr/bin/env node
/**
 * test-company-db-stock-daily.mjs — 在庫の日次 (SKU 単位) を miniPC から Company DB へ送る (08 §3.3 ③ NE = D2b-1) の試験。
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
import { pushStockDaily, parseArgs, rowsOfDay, datesBetween, SOURCES } from '../apps/company-db/push/stock-daily.mjs';

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
const capOf = (date) => { const t = Date.parse(`${date}T00:00:00Z`); return Number.isNaN(t) ? '2026-01-01T00:00:00.000Z' : new Date(t - 7117 * 1000).toISOString(); };   // 日付そのものが不正な試験でも、取得時刻は正しい形にしておく (日付の検証に届かせる)
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
  assert.deepEqual(Object.keys(STOCK_SOURCES), ['ne'], 'D2b-1 は NE だけ (FBA は D2b-2)');
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
    ['--source', 'ne', '--from', '2026-09-02', '--to', '2026-09-01'], ['--source'], ['--source', 'ne', '--nope'], ['--source', 'toString']])
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

server.close();
wh.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
