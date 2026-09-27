#!/usr/bin/env node
/**
 * test-company-db-ad-spend.mjs — 広告費の日次を miniPC から Company DB へ送る (Company DB構想 11 の ②) の試験。
 *   PGlite + 本物の router を HTTP で + 送り手 (pushAdSpend) を メモリ上の SQLite で回す。元データは本物の取込の保存 (fetch-amazon-ads.js saveAdProduct) で作る。
 *   世代で古い要求を拒む・同じ世代で違う内容は 409・中身が同じ新しい世代は世代だけ進める・0 行の日も置き換える・記録の無い日は送らない・金額は十進の文字列
 * 🚨 試験に無いもの: 2 接続の並行 (advisory lock。PGlite は 1 接続)・本番の件数 (1 日 約 2,000 行 × 235 日) での所要時間・main() の終了コード
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import express from 'express';
import Database from 'better-sqlite3';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';
import companyDbRouter, { requireSyncKey, __setPgClientFactory } from '../apps/company-db/router.mjs';
import { ingestAdSpendDay, validateAdSpendBody, adSpendChecksum, adRowOf, canonMoney, relinkAdSpend } from '../apps/company-db/ingest/ad-spend.mjs';
import { pushAdSpend, parseArgs, realToCents, REPORT_TYPE as PUSH_REPORT_TYPE } from '../apps/company-db/push/ad-spend.mjs';
import { saveAdProduct, REPORT_TYPE } from '../apps/warehouse/fetch-amazon-ads.js';

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const pg = new PGlite();
await applyMigrations(pgliteAdapter(pg), { log: quiet });
const db = pgliteAdapter(pg);
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
const listing = async (code) => Number((await one(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', '', $1, 'active') returning listing_id`, [code])).listing_id);
const lidA = await listing('SKU-A');
const TODAY = '2026-03-20';

const R = (x = {}) => ({ campaign_id: '111', target_granularity: 'sku', target_code: 'sku-a', clicks: 10, impressions: 100, cost: '123.45', sales_1d: '2000', units_1d: 2, ...x });
const B = (date, rows, x = {}) => {
  const norm = rows.map((r) => adRowOf(r));
  return { mall: 'amazon', scope: 'jp', ad_type: 'SP', date_jst: date, generation: 1000, report_id: 'RA', rows, checksum: adSpendChecksum(date, norm), ...x };
};
const dayOf = (date) => one(`select source_generation::text as g, source_report_id as r, checksum, row_count, cost_total::text as cost, ingest_run_id from core.ad_spend_days where date_jst = $1::date and mall = 'amazon'`, [date]);
const rowsOf = (date) => all(`select campaign_id c, target_granularity g, target_code t, listing_id::int as lid, clicks, ad_cost::text as cost, ad_sales_1d::text as s, units_1d u, ingest_run_id from core.ad_spend_daily where date_jst = $1::date order by 1, 2, 3`, [date]);
const runs = async () => Number((await one(`select count(*)::int as n from ops.ingest_runs where entity = 'ad_spend_daily'`)).n);

console.log('検証 (validateAdSpendBody)');
await t('金額は十進の文字列 (小数 2 桁まで・12 と 12.00 は同じ指紋) / 数 (number)・3 桁・負・空は 400 / 重複・none に対象・sku に空・指紋の食い違い・未来の日付は 400', async () => {
  assert.deepEqual([canonMoney('12'), canonMoney('12.5'), canonMoney('0.05'), canonMoney('012'), canonMoney('1.234'), canonMoney(12)], ['12.00', '12.50', '0.05', null, null, null]);
  assert.equal(adSpendChecksum('2026-03-01', [adRowOf(R({ cost: '12' }))]), adSpendChecksum('2026-03-01', [adRowOf(R({ cost: '12.00' }))]));
  assert.notEqual(adSpendChecksum('2026-03-01', [adRowOf(R({ sales_1d: null }))]), adSpendChecksum('2026-03-01', [adRowOf(R({ sales_1d: '0' }))]), 'null と 0 が同じ指紋');
  assert.notEqual(adSpendChecksum('2026-03-01', [adRowOf(R())]), adSpendChecksum('2026-03-02', [adRowOf(R())]), '日付が指紋に入っていない');
  const bad = (b, re) => assert.throws(() => validateAdSpendBody(b, { todayJst: TODAY }), (e) => e.code === 'BAD_REQUEST' && re.test(e.message));
  bad({ ...B('2026-03-01', [R()]), rows: [{ ...R(), cost: 123.45 }] }, /cost は 0 以上/);
  bad({ ...B('2026-03-01', [R()]), rows: [{ ...R(), cost: '1.234' }] }, /cost は 0 以上/);
  bad({ ...B('2026-03-01', [R()]), rows: [{ ...R(), cost: '-1' }] }, /cost は 0 以上/);
  bad({ ...B('2026-03-01', [R()]), rows: [{ ...R(), units_1d: 1.5 }] }, /units_1d/);
  bad(B('2026-03-01', [R(), R({ cost: '1' })]), /重複/);
  bad({ ...B('2026-03-01', [R()]), rows: [R({ target_granularity: 'none', target_code: 'x' })] }, /none の target_code は空/);
  bad({ ...B('2026-03-01', [R()]), rows: [R({ target_code: '' })] }, /target_code が不正/);
  bad({ ...B('2026-03-01', [R()]), rows: [R({ target_code: ' sku-a' })] }, /target_code が不正/);
  bad({ ...B('2026-03-01', [R()]), checksum: 'a'.repeat(64) }, /checksum が届いた行から計算した値と合わない/);
  bad(B('2026-03-21', [R()]), /未来/);
  bad(B('2026-03-01', [R()], { generation: 0 }), /generation/);
  bad(B('2026-03-01', [R()], { ad_type: 'SB' }), /ad_type/);
  bad(B('2026-03-01', [R()], { mall: 'rakuten' }), /mall/);
  bad({ ...B('2026-03-01', []), rows: undefined }, /rows は配列/);
  const v = validateAdSpendBody(B('2026-03-01', [R({ cost: '0.1' }), R({ target_code: 'sku-b', cost: '0.2' })]), { todayJst: TODAY });
  assert.equal(v.costTotal, '0.30', '合計を浮動小数で足している');
});

console.log('受け口の本体 (ingestAdSpendDay)');
await t('applied: 日の行を入れる。出品は粒度 sku だけ結ぶ (asin・none は null)。日の状態に世代・レポート・指紋・行数・費用の合計。mart は売上の分からない行を数える', async () => {
  const rows = [R(), R({ target_granularity: 'asin', target_code: 'b0asin', cost: '10', sales_1d: null, units_1d: null }), R({ target_granularity: 'none', target_code: '', cost: '5.5' }), R({ campaign_id: '222', target_code: 'sku-unknown', cost: '1' })];
  const r = await ingestAdSpendDay(db, B('2026-03-01', rows), { todayJst: TODAY });
  assert.deepEqual([r.status, r.rows, r.resolved, r.unresolved_sku], ['applied', 4, 1, 1]);
  assert.deepEqual((await rowsOf('2026-03-01')).map((x) => [x.c, x.g, x.t, x.lid, x.cost, x.s, x.u]),
    [['111', 'asin', 'b0asin', null, '10.00', null, null], ['111', 'none', '', null, '5.50', '2000.00', 2], ['111', 'sku', 'sku-a', lidA, '123.45', '2000.00', 2], ['222', 'sku', 'sku-unknown', null, '1.00', '2000.00', 2]]);
  const d = await dayOf('2026-03-01');
  assert.deepEqual([d.g, d.r, d.checksum, d.row_count, d.cost, d.ingest_run_id], ['1000', 'RA', r.checksum, 4, '139.95', r.run_id]);
  const run = await one(`select source_system, entity, scope_key, status, complete, rows_inserted, checksum from ops.ingest_runs where ingest_run_id = $1`, [r.run_id]);
  assert.deepEqual([run.source_system, run.entity, run.scope_key, run.status, run.complete, run.rows_inserted, run.checksum], ['amazon', 'ad_spend_daily', 'jp:SP', 'success', true, 4, r.checksum]);
  const m = await all(`select listing_id::int as lid, unresolved_granularity g, unresolved_code c, ad_cost::text cost, ad_sales_1d::text s, sales_unknown_rows su from mart.v_ad_spend_daily where date_jst = '2026-03-01' order by ad_cost desc`);
  assert.deepEqual(m.map((x) => [x.lid, x.g, x.c, x.cost, x.s, x.su]), [[lidA, null, null, '123.45', '2000.00', 0], [null, 'asin', 'b0asin', '10.00', null, 1], [null, 'none', '', '5.50', '2000.00', 0], [null, 'sku', 'sku-unknown', '1.00', '2000.00', 0]]);
});
await t('🚨 世代: 同じ世代・レポート・指紋 = same (何も書かない) / 同じ世代で別のレポート・違う内容 = CONFLICT / 古い世代 = stale (何も変わらない)', async () => {
  const rows = (await rowsOf('2026-03-01')).map((x) => x.ingest_run_id);
  const before = await runs();
  const base = [R(), R({ target_granularity: 'asin', target_code: 'b0asin', cost: '10', sales_1d: null, units_1d: null }), R({ target_granularity: 'none', target_code: '', cost: '5.5' }), R({ campaign_id: '222', target_code: 'sku-unknown', cost: '1' })];
  assert.equal((await ingestAdSpendDay(db, B('2026-03-01', [...base].reverse()), { todayJst: TODAY })).status, 'same', '並びが違うだけで same にならない');
  await assert.rejects(ingestAdSpendDay(db, B('2026-03-01', base, { report_id: 'RB' }), { todayJst: TODAY }), (e) => e.code === 'CONFLICT' && /別のレポート/.test(e.message));
  await assert.rejects(ingestAdSpendDay(db, B('2026-03-01', [R()]), { todayJst: TODAY }), (e) => e.code === 'CONFLICT' && /内容が違う/.test(e.message));
  const st = await ingestAdSpendDay(db, B('2026-03-01', [R({ cost: '999' })], { generation: 999, report_id: 'R0' }), { todayJst: TODAY });
  assert.deepEqual([st.status, st.remote_generation], ['stale', 1000]);
  assert.deepEqual([(await rowsOf('2026-03-01')).map((x) => x.ingest_run_id), (await dayOf('2026-03-01')).g, await runs()], [rows, '1000', before]);
});
await t('新しい世代で中身が同じ = refreshed (行は触らず世代とレポートだけ進める) / 新しい世代で中身が違う = 置き換え (消えた行は消える)', async () => {
  const base = [R(), R({ target_granularity: 'asin', target_code: 'b0asin', cost: '10', sales_1d: null, units_1d: null }), R({ target_granularity: 'none', target_code: '', cost: '5.5' }), R({ campaign_id: '222', target_code: 'sku-unknown', cost: '1' })];
  const runIds = (await rowsOf('2026-03-01')).map((x) => x.ingest_run_id);
  const before = await runs();
  const rf = await ingestAdSpendDay(db, B('2026-03-01', base, { generation: 2000, report_id: 'RC' }), { todayJst: TODAY });
  assert.equal(rf.status, 'refreshed');
  const d = await dayOf('2026-03-01');
  assert.deepEqual([d.g, d.r, (await rowsOf('2026-03-01')).map((x) => x.ingest_run_id), await runs()], ['2000', 'RC', runIds, before]);
  const ap = await ingestAdSpendDay(db, B('2026-03-01', [R({ cost: '50', sales_1d: '3000' })], { generation: 3000, report_id: 'RD' }), { todayJst: TODAY });
  assert.deepEqual([ap.status, (await rowsOf('2026-03-01')).map((x) => [x.t, x.cost, x.s]), (await dayOf('2026-03-01')).cost], ['applied', [['sku-a', '50.00', '3000.00']], '50.00']);
});
await t('🚨 0 行の日 = 空の集合で置き換える (最後まで取れて 0 行。日の状態は row_count 0・費用 0)', async () => {
  const r = await ingestAdSpendDay(db, B('2026-03-01', [], { generation: 4000, report_id: 'RE' }), { todayJst: TODAY });
  assert.deepEqual([r.status, r.rows, (await rowsOf('2026-03-01')).length, (await dayOf('2026-03-01')).row_count, (await dayOf('2026-03-01')).cost], ['applied', 0, 0, 0, '0.00']);
});
await t('途中で落ちたら全部巻き戻る: 行・日の状態・取込の記録が元のまま', async () => {
  await ingestAdSpendDay(db, B('2026-03-02', [R({ cost: '7' })]), { todayJst: TODAY });
  const before = [await rowsOf('2026-03-02'), await dayOf('2026-03-02'), await runs()];
  await assert.rejects(ingestAdSpendDay(db, B('2026-03-02', [R({ cost: '8' })], { generation: 5000, report_id: 'RF' }), { todayJst: TODAY, afterWrite: async () => { throw new Error('commit の直前で落ちた'); } }), /commit の直前で落ちた/);
  assert.deepEqual([await rowsOf('2026-03-02'), await dayOf('2026-03-02'), await runs()], before);
  await assert.rejects(ingestAdSpendDay(db, B('2026-03-03', [R()]), { todayJst: TODAY, afterWrite: async () => { throw new Error('x'); } }), /x/);
  assert.deepEqual([await dayOf('2026-03-03'), (await rowsOf('2026-03-03')).length], [undefined, 0], '新しい日で落ちて日の状態が残った');
});
await t('結び直し: 出品が後から増えたら relink で sku の行だけ結ぶ (asin の行・別のモールの出品には結ばない)', async () => {
  await ingestAdSpendDay(db, B('2026-03-04', [R({ target_code: 'sku-late' }), R({ target_granularity: 'asin', target_code: 'sku-late' })]), { todayJst: TODAY });
  await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', 'sku-late', 'active')`);
  assert.equal((await relinkAdSpend(db)).relinked, 0, '別のモールの出品に結んだ');
  const lid = await listing('sku-late');
  const r = await relinkAdSpend(db);
  assert.equal(r.relinked, 1);
  assert.deepEqual((await rowsOf('2026-03-04')).map((x) => [x.g, x.lid]), [['asin', null], ['sku', lid]]);
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
const realToday = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
const ago = (n) => new Date(Date.parse(`${realToday}T00:00:00Z`) - n * 86400000).toISOString().slice(0, 10);

await t('受け口: 鍵が無ければ 401 / 検証に通らなければ 400 / applied → same / 同じ世代で違う内容は 409 / status は日ごとの世代・指紋・合計 / relink / server.js は鍵の検査と共通 parser の素通りに ad-spend を入れている', async () => {
  assert.equal((await http('POST', '/ad-spend/day', { body: B(ago(100), [R()]), key: null })).status, 401);
  assert.equal((await http('POST', '/ad-spend/day', { body: { mall: 'amazon' } })).status, 400);
  const a = await http('POST', '/ad-spend/day', { body: B(ago(100), [R()]) });
  assert.deepEqual([a.status, a.json.status, a.json.rows, a.json.resolved], [200, 'applied', 1, 1]);
  assert.equal((await http('POST', '/ad-spend/day', { body: B(ago(100), [R()]) })).json.status, 'same');
  const c = await http('POST', '/ad-spend/day', { body: B(ago(100), [R({ cost: '1' })]) });
  assert.deepEqual([c.status, c.json.code], [409, 'CONFLICT']);
  const st = await http('GET', `/ad-spend/status?mall=amazon&scope=jp&ad_type=SP&from=${ago(101)}&to=${ago(99)}`);
  assert.deepEqual([st.status, st.json.days.map((x) => [x.date_jst, x.generation, x.report_id, x.checksum === a.json.checksum, x.row_count, x.cost_total])], [200, [[ago(100), 1000, 'RA', true, 1, '123.45']]]);
  for (const q of ['mall=x&scope=jp&ad_type=SP&from=2026-01-01&to=2026-01-02', 'mall=amazon&scope=us&ad_type=SP&from=2026-01-01&to=2026-01-02', 'mall=amazon&scope=jp&ad_type=SP&from=2026-01-02&to=2026-01-01', 'mall=amazon&scope=jp&ad_type=SP&from=2020-01-01&to=2026-01-01'])
    assert.equal((await http('GET', `/ad-spend/status?${q}`)).status, 400, q);
  const rl = await http('POST', '/ad-spend/relink', { body: {} });
  assert.deepEqual([rl.status, typeof rl.json.relinked, typeof rl.json.unresolved_sku], [200, 'number', 'number']);
  const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
  const srv = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  assert.match(srv, /app\.use\(\[[^\]]*'\/apps\/company-db\/sync\/ad-spend'[^\]]*\], companyDbRequireSyncKey\);/);
  assert.match(srv, /normalizedPath\.toLowerCase\(\)\.startsWith\('\/apps\/company-db\/sync\/ad-spend'\)\) return next\(\);/);
});

// 送り手: メモリ上の warehouse.db。行と取得の記録は本物の取込 (saveAdProduct) で作る
function openWh() {
  const w = new Database(':memory:');
  w.exec(`CREATE TABLE fact_ad_spend (日付 TEXT NOT NULL, モール TEXT NOT NULL, キャンペーンID TEXT NOT NULL, 広告タイプ TEXT NOT NULL, ターゲット TEXT NOT NULL, ターゲット粒度 TEXT NOT NULL,
    クリック数 INTEGER DEFAULT 0, インプレッション INTEGER DEFAULT 0, 広告費 REAL DEFAULT 0, 広告経由売上 REAL DEFAULT 0, 広告経由数量 INTEGER DEFAULT 0, ingested_at TEXT NOT NULL,
    PRIMARY KEY (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度))`);
  return w;
}
const api = (date, x = {}) => ({ date, campaignId: 111, advertisedSku: 'SKU-A', advertisedAsin: 'B000000001', impressions: 100, clicks: 10, cost: 123.45, sales1d: 2000, unitsSoldClicks1d: 2, ...x });
const save = (w, rows, from, to, generation, reportId = `R${generation}`) => saveAdProduct(w, rows, { from, to, generation, reportId, profileId: 'P1' });
/** POST を数える fetch */
const spyFetch = () => { const posts = []; const f = async (url, init) => { if (init && init.method === 'POST') posts.push(String(url).replace(BASE_URL, '')); return fetch(url, init); }; f.posts = posts; return f; };
const push = (w, x = {}) => pushAdSpend({ mall: 'amazon', warehouse: w, base: BASE_URL, syncKey: 'k', today: realToday, log: quiet, sleep: async () => {}, ...x });

await t('取込と送り手の REPORT_TYPE が同じ', async () => { assert.equal(PUSH_REPORT_TYPE, REPORT_TYPE); });
await t('送り手: 記録のある日だけ送る (0 行の日も)・記録の無い日は送らず ⚠️・2 回目は何も送らない・送った後に relink・Render の行は取込の行と同じ', async () => {
  const w = openWh();
  save(w, [api(ago(12)), api(ago(12), { advertisedSku: '', advertisedAsin: 'B0X', cost: 0.1, sales1d: null }), api(ago(11), { cost: 7 })], ago(13), ago(11), 1000);   // ago(13) = 0 行の日
  const f = spyFetch();
  const r = await push(w, { from: ago(14), to: ago(11), fetchImpl: f });
  assert.equal(r.ok, true, JSON.stringify(r.failed));
  assert.deepEqual([r.sent.map((s) => [s.date, s.rows]), r.noRecord, f.posts.filter((p) => p === '/ad-spend/day').length, f.posts.at(-1)], [[[ago(13), 0], [ago(12), 2], [ago(11), 1]], [ago(14)], 3, '/ad-spend/relink']);
  assert.match(r.lastLine, /^⚠️ .*取得の記録が無い日 1/);
  assert.deepEqual((await rowsOf(ago(12))).map((x) => [x.g, x.t, x.cost, x.s]), [['asin', 'b0x', '0.10', null], ['sku', 'sku-a', '123.45', '2000.00']]);
  assert.deepEqual([(await dayOf(ago(13))).row_count, (await dayOf(ago(12))).cost], [0, '123.55']);
  const f2 = spyFetch();
  const r2 = await push(w, { from: ago(13), to: ago(11), fetchImpl: f2 });
  assert.deepEqual([r2.sent.length, r2.done, f2.posts], [0, 3, ['/ad-spend/relink']]);
  assert.match(r2.lastLine, /^✅ /);
});
await t('送り手: 取込が取り直した日 (新しい世代) だけ送る。中身が同じなら refreshed・違えば置き換え・レポートから消えた行は Render でも消える', async () => {
  const w = openWh();
  save(w, [api(ago(22)), api(ago(21)), api(ago(21), { campaignId: 222 })], ago(22), ago(21), 1000);
  await push(w, { from: ago(22), to: ago(21) });
  save(w, [api(ago(22)), api(ago(21))], ago(22), ago(21), 2000);   // ago(22) は同じ中身・ago(21) は 222 が消えた
  const r = await push(w, { from: ago(22), to: ago(21) });
  assert.deepEqual([r.refreshed, r.sent.map((s) => s.date), (await rowsOf(ago(21))).map((x) => x.c), (await dayOf(ago(22))).g, (await dayOf(ago(21))).g], [1, [ago(21)], ['111'], '2000', '2000']);
});
await t('🚨 送り手: 取込の後に行が書き換わった日 (記録の行数・合計と合わない)・小数 2 桁より細かい金額 は送らず ❌ (日ごとに判定・Render には何も入らない)', async () => {
  const w = openWh();
  save(w, [api(ago(32)), api(ago(31)), api(ago(30))], ago(32), ago(30), 1000);
  w.prepare(`update fact_ad_spend set 広告費 = 1 where 日付 = ?`).run(ago(32));
  w.prepare(`insert into fact_ad_spend (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度, 広告費, ingested_at) values (?, 'amazon', '9', 'SP', 'x', 'sku', 0, 'T')`).run(ago(31));
  w.prepare(`update fact_ad_spend set 広告費 = 1.234 where 日付 = ?`).run(ago(30));
  w.prepare(`update ads_fetch_days set cost_total = 1.234 where date_jst = ?`).run(ago(30));
  const r = await push(w, { from: ago(32), to: ago(30) });
  assert.equal(r.ok, false);
  assert.deepEqual(r.failed.map((x) => x.date), [ago(32), ago(31), ago(30)]);
  assert.match(r.failed[0].error, /費用の合計 .* が取得の記録/);
  assert.match(r.failed[1].error, /行の数 2 が取得の記録 \(1\)/);
  assert.match(r.failed[2].error, /小数 2 桁より細かい/);
  assert.equal(await dayOf(ago(32)), undefined);
  assert.match(r.lastLine, /^❌ /);
});
await t('送り手: Render の方が新しい世代の日は送らず ⚠️ / dry-run は POST しない / 別のプロファイルの記録があれば何も送らない / 引数の検査', async () => {
  const w = openWh();
  save(w, [api(ago(42))], ago(42), ago(42), 1000);
  await push(w, { from: ago(42), to: ago(42) });
  await pg.query(`update core.ad_spend_days set source_generation = 9999 where date_jst = $1::date`, [ago(42)]);
  save(w, [api(ago(42), { cost: 1 })], ago(42), ago(42), 2000);
  const r = await push(w, { from: ago(42), to: ago(42) });
  assert.deepEqual([r.ok, r.remoteNewer, (await rowsOf(ago(42)))[0].cost], [true, [ago(42)], '123.45']);
  assert.match(r.lastLine, /^⚠️ .*Render の方が新しい取得の日 1/);
  save(w, [api(ago(43))], ago(43), ago(43), 3000);
  const f = spyFetch();
  const d = await push(w, { from: ago(43), to: ago(43), dryRun: true, fetchImpl: f });
  assert.deepEqual([d.sent.map((s) => s.status), f.posts, await dayOf(ago(43))], [['dry-run'], [], undefined]);
  w.prepare(`insert into ads_fetch_days (report_type, profile_id, date_jst, generation, report_id, window_from, window_to, row_count, cost_total, fetched_at) values (?, 'P2', ?, 1, 'x', ?, ?, 0, 0, 'T')`).run(REPORT_TYPE, ago(50), ago(50), ago(50));
  await assert.rejects(push(w, { from: ago(43), to: ago(43) }), /広告プロファイルが 2 つ/);
  await assert.rejects(push(openWh(), { all: true }), /取得の記録の表 \(ads_fetch_days\) が無い/);
  await assert.rejects(push(w, { from: ago(1), to: realToday }), /--to は昨日/);
  assert.throws(() => parseArgs(['--mall', 'rakuten']), /--mall は amazon/);
  assert.throws(() => parseArgs(['--mall', 'amazon', '--days', '3', '--all']), /どれか 1 つ/);
  assert.deepEqual(parseArgs(['--mall', 'amazon', '--days', '35']).days, 35);
  assert.equal(realToCents(0.1 + 0.2, 'x'), 30);
  assert.throws(() => realToCents(1.005, 'x'), /小数 2 桁より細かい/);
  assert.throws(() => realToCents(-1, 'x'), /0 以上/);
});

console.log('古い取込の行 (--legacy。中原さん 2026-09-27)');
await t('受け口: 印は「世代 1 + legacy:upsert-v1」の組だけ (片方だけは 400)。古い行の日に本物の取得が来れば置き換わり、本物の後の古い行は stale', async () => {
  const bad = (b) => assert.throws(() => validateAdSpendBody(b, { todayJst: TODAY }), (e) => e.code === 'BAD_REQUEST' && /古い取込の行は generation 1/.test(e.message));
  bad(B('2026-03-10', [R()], { generation: 1, report_id: 'RA' }));
  bad(B('2026-03-10', [R()], { generation: 5, report_id: 'legacy:upsert-v1' }));
  bad(B('2026-03-10', [R()], { generation: 1, report_id: 'legacy:other' }));
  const L = { generation: 1, report_id: 'legacy:upsert-v1' };
  assert.equal((await ingestAdSpendDay(db, B('2026-03-10', [R({ cost: '9' })], L), { todayJst: TODAY })).status, 'applied');
  assert.deepEqual([(await dayOf('2026-03-10')).g, (await dayOf('2026-03-10')).r], ['1', 'legacy:upsert-v1']);
  assert.equal((await ingestAdSpendDay(db, B('2026-03-10', [R({ cost: '10' })], { generation: 7000, report_id: 'RR' }), { todayJst: TODAY })).status, 'applied');
  assert.equal((await ingestAdSpendDay(db, B('2026-03-10', [R({ cost: '9' })], L), { todayJst: TODAY })).status, 'stale');
  assert.deepEqual([(await dayOf('2026-03-10')).r, (await rowsOf('2026-03-10'))[0].cost], ['RR', '10.00']);
});
function openLegacyWh() {
  const w = openWh();
  w.exec(`CREATE TABLE fact_ad_spend_campaign (日付 TEXT NOT NULL, モール TEXT NOT NULL, キャンペーンID TEXT NOT NULL, キャンペーン名 TEXT, 広告タイプ TEXT NOT NULL DEFAULT 'SP', キャンペーンステータス TEXT,
    クリック数 INTEGER DEFAULT 0, インプレッション INTEGER DEFAULT 0, 広告費 REAL DEFAULT 0, 広告経由売上_1d REAL DEFAULT 0, 広告経由売上_7d REAL DEFAULT 0, 広告経由売上_14d REAL DEFAULT 0, 広告経由売上_30d REAL DEFAULT 0,
    広告経由数量_1d INTEGER DEFAULT 0, ingested_at TEXT NOT NULL, PRIMARY KEY (日付, モール, キャンペーンID, 広告タイプ))`);
  return w;
}
const oldRow = (w, d, target, cost, x = {}) => w.prepare(`insert into fact_ad_spend (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度, クリック数, インプレッション, 広告費, 広告経由売上, 広告経由数量, ingested_at)
  values (?, 'amazon', ?, 'SP', ?, 'sku', 3, 30, ?, 500, 1, 'T')`).run(d, x.c || '111', target, cost);
const campRow = (w, d, cost, c = '111') => w.prepare(`insert into fact_ad_spend_campaign (日付, モール, キャンペーンID, 広告タイプ, 広告費, ingested_at) values (?, 'amazon', ?, 'SP', ?, 'T')`).run(d, c, cost);
await t('🚨 送り手 --legacy: 取得の記録より前の日だけ・大文字の重複行は外す・キャンペーンの合計と 1 円以内の日だけ送る (合わない・行が無い・合計が無い日は ⚠️ で送らない)・印つき・2 回目は送らない', async () => {
  const w = openLegacyWh();
  const [L1, L2, L3, L4, L5] = [ago(66), ago(65), ago(64), ago(63), ago(62)];
  oldRow(w, L1, 'sku-a', 100.5); oldRow(w, L1, 'SKU-A', 100.5); oldRow(w, L1, 'sku-z', 20, { c: '222' }); campRow(w, L1, 100.5); campRow(w, L1, 20.4, '222');   // 大文字の重複・合計の差 0.4 円 = 送る
  oldRow(w, L2, 'sku-a', 50); campRow(w, L2, 80);        // キャンペーンの合計と 30 円違う = 送らない
  /* L3 = 行が無い日 */ campRow(w, L3, 0);
  oldRow(w, L4, 'sku-a', 5);                              // キャンペーンの合計が無い = 送らない
  oldRow(w, L5, 'sku-a', 7); campRow(w, L5, 7);
  save(w, [api(ago(61))], ago(61), ago(61), 1000);       // 取得の記録の最初の日 = ago(61) → 古い行は ago(62) まで
  const r = await push(w, { legacy: true });
  assert.equal(r.ok, true, JSON.stringify(r.failed));
  assert.deepEqual([r.from, r.to, r.sent.map((s) => [s.date, s.rows]), r.legacySkipped.map((s) => s.date), r.droppedUpper], [L1, L5, [[L1, 2], [L5, 1]], [L2, L3, L4], 1]);
  assert.match(r.legacySkipped[0].reason, /キャンペーンの合計 80 と 1 円より違う/);
  assert.match(r.legacySkipped[1].reason, /行が無い/);
  assert.match(r.legacySkipped[2].reason, /キャンペーンの合計が無い/);
  assert.match(r.lastLine, /^⚠️ .*古い取込の行 .*大文字の重複行を外した 1 行.*送らなかった日 3/);
  assert.deepEqual((await rowsOf(L1)).map((x) => [x.t, x.cost]), [['sku-a', '100.50'], ['sku-z', '20.00']]);
  assert.deepEqual([(await dayOf(L1)).g, (await dayOf(L1)).r, await dayOf(L2)], ['1', 'legacy:upsert-v1', undefined]);
  const f = spyFetch();
  const r2 = await push(w, { legacy: true, fetchImpl: f });
  assert.deepEqual([r2.sent.length, r2.done, f.posts], [0, 2, ['/ad-spend/relink']]);
  // Render に本物の取得がある日は古い行で戻さない (送り手は送らずに数えるだけ)
  await ingestAdSpendDay(db, B(L5, [R({ cost: '8' })], { generation: 9000, report_id: 'RZ' }));
  const r3 = await push(w, { legacy: true, from: L5, to: L5 });
  assert.deepEqual([r3.sent.length, r3.done, (await dayOf(L5)).r], [0, 1, 'RZ']);
  // 範囲の守り: 記録のある日を含む --to は例外 / 記録が 1 つも無ければ例外 / --legacy と --days・--all は一緒に使えない
  await assert.rejects(push(w, { legacy: true, from: L5, to: ago(61) }), /--to は取得の記録の最初の日の前日/);
  await assert.rejects(push(openLegacyWh(), { legacy: true }), /取得の記録の表 \(ads_fetch_days\) が無い/);
  { const w2 = openLegacyWh(); w2.exec("CREATE TABLE ads_fetch_days (report_type TEXT, profile_id TEXT, date_jst TEXT, generation INTEGER, report_id TEXT, window_from TEXT, window_to TEXT, row_count INTEGER, cost_total REAL, fetched_at TEXT)"); await assert.rejects(push(w2, { legacy: true }), /取得の記録 \(ads_fetch_days\) が無い/); }
  assert.throws(() => parseArgs(['--mall', 'amazon', '--legacy', '--days', '3']), /--legacy は/);
  assert.equal(parseArgs(['--mall', 'amazon', '--legacy']).legacy, true);
});

server.close();
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
