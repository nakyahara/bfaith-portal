#!/usr/bin/env node
/**
 * test-fba-sheetless-mode.mjs — FBA 補充の「Sheet なし (fail-closed)」モード (FBA_SHEETLESS_MODE・マスタ正本切替 ⑦-F) の試験
 *   node scripts/test-fba-sheetless-mode.mjs
 *
 * 契約 = AI_reference システム設計/CompanyDB構想/16 §7 v2 H3・§8 契約 v3 High 3 (Codex 設計 R1)。確かめること:
 *   ① モードを使わない (env なし / '0' / '') = 今までどおり (Sheet の値に戻る・二重書き・起動時の backfill・Sheet の同期)
 *   ② モードを使うと、sku_mapping が古い / 空 / 無い のどれでも計算の結果が変わらない
 *   ③ 商品管理リストが欠けたら計算しない (Sheet に戻らない)。前の結果 (健全性の記録・画面の一覧) はそのまま・9:40 の自動決定は fail の ping
 *   ④ 06:00 の定期同期は Sheet の段だけ外し、土台・納品実績は続ける。ok の基準 = Sheet なしの材料
 *   ⑤ 手の Sheet 同期の口 (Render・miniPC) は 410
 *   ⑥ FNSKU の更新は fba_sku_attrs だけ (Render の引き取りも miniPC の fba_sku_attrs からだけ)
 *   ⑦ 起動時の backfill はモードを使うとき・印があるときは流さない。一回限りの移行のスクリプトは印を残し、二度目・モードありは断る
 *   ⑧ 設定がそろわない (値が 1 でない・mirror でない・pml でない) = 計算しない
 * DATA_DIR は一時フォルダ (本番・開発の fba.db には触れない)。miniPC・Google・SP-API には行かない (fetch は差し替え)。
 */
import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import express from 'express';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-sheetless-'));
process.env.DATA_DIR = dataDir;
process.env.FBA_SKU_MAPPING_SOURCE = 'mirror';
process.env.FBA_NONFBA_SOURCE = 'pml';
process.env.WAREHOUSE_URL = 'http://minipc.test';
for (const k of ['FBA_SHEETLESS_MODE', 'RENDER', 'GOOGLE_SERVICE_ACCOUNT_KEY', 'JOBS_MONITOR_ENABLED']) delete process.env[k];
const dbFile = path.join(dataDir, 'fba.db');
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ok  ${name}`); }
  catch (e) { fail++; console.log(`  NG  ${name}\n      ${String(e.stack || e).split('\n').slice(0, 6).join('\n      ')}`); }
}
const modeOn = (v = '1') => { process.env.FBA_SHEETLESS_MODE = v; };
const modeOff = () => { delete process.env.FBA_SHEETLESS_MODE; };
const tick = () => new Promise((r) => setTimeout(r, 30));
const quiet = () => {};

// ── miniPC への通信 (callMiniPC は global の fetch を使う) を差し替える ──
const realFetch = globalThis.fetch;
const miniCalls = [];
let miniHandler = () => ({ ok: false });
globalThis.fetch = async (url, opts) => {
  const u = String(url);
  if (!u.startsWith('http://minipc.test')) return realFetch(url, opts);
  miniCalls.push(u);
  return new Response(JSON.stringify(miniHandler(u)), { status: 200, headers: { 'content-type': 'application/json' } });
};

// ── 画面の口 (Render の router) と miniPC の口 (fba-service) を 1 つの express に載せる ──
const db = await imp('apps/fba-replenishment/db.js');
const mirror = await imp('apps/warehouse-mirror/db.js');
mirror.initMirrorDB();
const mdb = mirror.getMirrorDB();
const routerMod = await imp('apps/fba-replenishment/router.js');
const fbaService = (await imp('apps/warehouse/fba-service.js')).default;
const { generateRecommendations } = await imp('apps/fba-replenishment/calculation-engine.js');
const { runDecisionAttempt } = await imp('apps/fba-replenishment/decision-job.js');
const sheetless = await imp('apps/fba-replenishment/sheetless-mode.js');
const app = express();
app.use('/fba-service', express.json(), fbaService);
app.use('/', routerMod.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;
const call = async (method, p) => { const r = await realFetch(base + p, { method }); let body = null; try { body = await r.json(); } catch { /* */ } return { status: r.status, body }; };
for (let i = 0; i < 100; i++) { if ((await call('GET', '/api/settings')).status === 200) break; await tick(); }
assert.equal((await call('GET', '/api/settings')).status, 200, 'router の DB 初期化が終わらない');

// ── 材料 ──
const todayJst = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
// Sheet の写し (sku_mapping): 他 CH 販売・ASIN・名前はわざと「古い Sheet の値」にしておく
db.upsertSkuMappings([
  { amazon_sku: 'Alpha-1', asin: 'B0SHEETA', product_name: 'シートの名前A', ne_code: 'alpha', logizard_code: 'alpha', non_fba_sales_7d: 70, non_fba_sales_30d: 300 },
  { amazon_sku: 'Beta-2', asin: 'B0SHEETB', product_name: 'シートの名前B', ne_code: 'beta', logizard_code: 'beta', non_fba_sales_7d: 9, non_fba_sales_30d: 90 },
  { amazon_sku: 'Sheetonly-9', asin: 'B0SHEET9', product_name: 'シートだけ', ne_code: 'zzz', logizard_code: 'zzz', non_fba_sales_30d: 5 },
]);
db.saveNonFbaSalesSnapshot([{ amazon_sku: 'Unmapped-X', non_fba_sales_7d: 5, non_fba_sales_30d: 25 }]);   // Sheet 由来の他 CH のスナップショット
db.updateFnskuBatch([{ sku: 'Alpha-1', fnsku: 'X0ALPHA' }]);
db.saveRestockLatest([
  { amazon_sku: 'Alpha-1', product_name: 'A', fba_available: 2, units_sold_30d: 60, amazon_recommended_qty: null },
  { amazon_sku: 'Beta-2', product_name: 'B', fba_available: 0, units_sold_30d: 30, amazon_recommended_qty: null },
  { amazon_sku: 'Gamma-3', product_name: 'G', fba_available: 1, units_sold_30d: 45, amazon_recommended_qty: null },
  { amazon_sku: 'Unmapped-X', product_name: 'X', fba_available: 0, units_sold_30d: 0, amazon_recommended_qty: null },
]);
db.replaceWarehouseInventory([
  { logizard_code: 'alpha', product_name: 'A', location: 'P-01', quantity: 200, reserved: 0, available_qty: 200, expiry_date: '', block_alloc_order: 1 },
  { logizard_code: 'beta', product_name: 'B', location: 'P-02', quantity: 80, reserved: 0, available_qty: 80, expiry_date: '', block_alloc_order: 1 },
  { logizard_code: 'gamma', product_name: 'G', location: 'P-03', quantity: 150, reserved: 0, available_qty: 150, expiry_date: '', block_alloc_order: 1 },
]);
db.excludeReplenishmentSku('Beta-2', '試験');
// マスタの写し (mirror): gamma-3 はマスタにだけある新しい SKU
const now = new Date().toISOString();
for (const [sku, ne, qty, name, so] of [['alpha-1', 'alpha', 1, 'マスタA', 0], ['beta-2', 'beta', 1, 'マスタB', 0], ['gamma-3', 'gamma', 2, 'マスタG', 0]]) {
  mdb.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, source_updated_at, sort_order, synced_at) VALUES (?, ?, ?, 'master', ?, ?, ?, ?)`).run(sku, ne, qty, name, now, so, now);
  mdb.prepare(`INSERT INTO mirror_sku_master (seller_sku, 商品名, source_created_at, source_updated_at, synced_at) VALUES (?, ?, ?, ?, ?)`).run(sku, name, now, now, now);
}
// 商品管理リスト (他 CH 販売の正)
const PML = [['alpha', 4, 12], ['beta', 0, 0], ['gamma', 1, 3]];
function publishPml(rows = PML) {
  mdb.prepare('DELETE FROM mirror_pml_snapshot_rows').run();
  mdb.prepare('DELETE FROM mirror_pml_published').run();
  const ins = mdb.prepare('INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 販売数7日_FBA以外, 販売数30日_FBA以外) VALUES (?, ?, ?, ?)');
  for (const [c, n7, n30] of rows) ins.run('run1', c, n7, n30);
  mdb.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, as_of_date, src_velocity_as_of, row_count, synced_at) VALUES (1, 'run1', 'ok', ?, ?, ?, datetime('now'))`).run(todayJst, todayJst, rows.length);
  db._clearNonFbaCache();
}
function removePml() {
  mdb.prepare('DELETE FROM mirror_pml_published').run();
  db._clearNonFbaCache();
}
publishPml();

const ENGINE_OPTS = { pendingSlips: { status: 'ok', slips: [], byCode: new Map() } };
/** 計算の結果 (時刻だけ落とす) */
const engine = () => { const r = generateRecommendations(false, {}, ENGINE_OPTS); const { generated_at, ...rest } = r; return JSON.parse(JSON.stringify(rest)); };
const itemOf = (r, sku) => r.items.find((i) => i.amazon_sku === sku);
const sheetRows = () => db.getSkuMappingsFromSheet();
const attrsOf = (sku) => db.getFbaSkuAttrs().find((a) => a.amazon_sku === sku) || null;
/** fba.db のファイルを外から書き換え、このプロセスのメモリを読み直す (保存の歯止め = 外から書かれたら読み直して例外) */
async function editFileAndReload(sqls) {
  await tick();
  const f = new Database(dbFile);
  try { for (const s of sqls) f.exec(s); } finally { f.close(); }
  await tick();
  let code = null;
  try { db.updateSetting('_reload_probe', String(Date.now())); } catch (e) { code = e.code; }
  assert.equal(code, 'FBA_DB_EXTERNAL_WRITE', '外からの書き換えを読み直していない');
}
const fileMark = () => { const f = new Database(dbFile, { readonly: true }); try { return f.prepare(`SELECT 1 FROM sqlite_master WHERE name = 'fba_migration_marks'`).get() ? f.prepare('SELECT * FROM fba_migration_marks').all() : []; } finally { f.close(); } };

// =====================================================================================
console.log('① モードを使わない = 今までどおり');
let offResult;
await t('env なし / "0" / "" は同じ (計算・対応の一覧)', async () => {
  modeOff(); offResult = engine(); const offMap = db.getSkuMappings();
  modeOn('0'); assert.deepEqual(engine(), offResult); assert.deepEqual(db.getSkuMappings(), offMap);
  modeOn(''); assert.deepEqual(engine(), offResult); assert.deepEqual(db.getSkuMappings(), offMap);
  modeOff();
  assert.equal(sheetless.isSheetlessRequested({}), false);
  assert.deepEqual(sheetless.sheetlessProblems({ FBA_SHEETLESS_MODE: '0' }), []);
});
await t('モードなし: 他 CH 販売は商品管理リストから / 無ければ今までどおり Sheet の値に戻る (fail-safe のまま)', async () => {
  modeOff();
  assert.equal(itemOf(offResult, 'Alpha-1').non_fba_sales_30d, 12);
  removePml();
  const r = engine();
  assert.equal(r.errors?.length ?? 0, 0, JSON.stringify(r.errors));
  assert.equal(itemOf(r, 'Alpha-1').non_fba_sales_30d, 300, 'Sheet に戻っていない = 今までの動きが変わった');
  publishPml();
});
await t('モードなし: Sheet 由来の他 CH スナップショットで未対応の SKU を「動いている」に数える (今までどおり)', async () => {
  modeOff();
  assert.ok(offResult.data_quality.unmapped_active.some((u) => u.sku === 'Unmapped-X'));
});
await t('モードなし: FNSKU は sku_mapping と fba_sku_attrs の両方に書く (二重書き)', async () => {
  modeOff();
  db.updateFnskuBatch([{ sku: 'Alpha-1', fnsku: 'X0OFF' }]);
  assert.equal(sheetRows().find((r) => r.amazon_sku === 'Alpha-1').fnsku, 'X0OFF');
  assert.equal(attrsOf('Alpha-1').fnsku, 'X0OFF');
  db.syncFnskuBatch([{ sku: 'Beta-2', fnsku: 'X0BETA' }]);
  assert.equal(sheetRows().find((r) => r.amazon_sku === 'Beta-2').fnsku, 'X0BETA');
  assert.equal(attrsOf('Beta-2').fnsku, 'X0BETA');
});
await t('モードなし・印なし: 起動のたびに sku_mapping → fba_sku_attrs の backfill を流す (今までどおり)', async () => {
  modeOff();
  db.upsertSkuMappings([{ amazon_sku: 'Delta-4', asin: 'B0DELTA', ne_code: 'delta' }]);
  assert.equal(attrsOf('Delta-4'), null);
  await db.initDb();
  assert.deepEqual(attrsOf('Delta-4'), { amazon_sku: 'Delta-4', asin: 'B0DELTA', fnsku: null });
  assert.equal(db.getBackfillMark(), null);
  assert.deepEqual(fileMark(), [], '起動で印の表を作っている (モードを使わない間は fba.db の形も今のまま)');
});
await t('モードなし: 手の Sheet 同期の口は Sheet を読みに行く (410 ではない。鍵が無いので 500)', async () => {
  modeOff();
  const r = await call('POST', '/api/sync-sku-mappings');
  assert.equal(r.status, 500);
  assert.match(r.body.error, /GOOGLE_SERVICE_ACCOUNT_KEY/);
});
await t('モードなし: 06:00 の定期同期は Sheet を同期し、成否で ok / fail (今までどおり)', async () => {
  modeOff();
  const pings = [], called = [];
  const stubs = {
    syncSkuMappings: async () => { called.push('sheet'); return { total: 3, snapshots: 3 }; },
    syncDodaiMaster: async () => { called.push('dodai'); return { count: 5 }; },
    runInboundHistoryDailySync: async () => { called.push('inbound'); return { shipments: 1, items: 2, items_failed: 0, items_failed_sample: [] }; },
    pingJob: (...a) => pings.push(a),
  };
  await routerMod.runFbaDailySync(stubs);
  assert.deepEqual(called, ['sheet', 'dodai', 'inbound']);
  assert.deepEqual(pings, [['fba-daily-sync', 'ok', 'sku=3 土台=5 納品=1/2']]);
  pings.length = 0;
  await routerMod.runFbaDailySync({ ...stubs, syncSkuMappings: async () => { throw new Error('共有が外れた'); } });
  assert.deepEqual(pings, [['fba-daily-sync', 'fail', 'sku失敗: 共有が外れた 土台=5 納品=1/2']]);
});
await t('モードなし: 画面の同期は miniPC に今までの URL で頼み、FNSKU を両方の表に入れる', async () => {
  modeOff();
  miniCalls.length = 0;
  miniHandler = (u) => (u.includes('/sync/latest-planning') ? {
    ok: true, snapshot_date: '2026-09-30', rows: [{ amazon_sku: 'Alpha-1', product_name: 'A', units_sold_30d: 60 }],
    fnskus: [{ sku: 'Alpha-1', fnsku: 'X0PULLOFF' }], restock_rows: [], planning_latest_rows: [],
  } : { ok: false });
  const r = await routerMod.syncLatestPlanningFromMiniPC();
  assert.equal(r.ok, true);
  assert.deepEqual(miniCalls, ['http://minipc.test/service-api/fba/sync/latest-planning']);
  assert.equal(r.fnskus, 1);
  assert.equal('fnsku_skip_reason' in r, false);
  assert.equal(sheetRows().find((x) => x.amazon_sku === 'Alpha-1').fnsku, 'X0PULLOFF');
  assert.equal(attrsOf('Alpha-1').fnsku, 'X0PULLOFF');
});
await t('モードなし: miniPC の口は FNSKU を今までどおり getSkuMappings から返す (fnsku_source は付けない)', async () => {
  modeOff();
  const r = await call('GET', '/fba-service/sync/latest-planning');
  assert.equal(r.status, 200);
  assert.equal('fnsku_source' in r.body, false);
  assert.deepEqual(r.body.fnskus.map((f) => f.sku).sort(), db.getSkuMappings().map((m) => m.amazon_sku).sort());
});

// =====================================================================================
console.log('② モードを使う: sku_mapping が古い / 空 / 無い でも結果が変わらない');
let onResult;
await t('モードあり: 他 CH 販売は商品管理リストだけ・Sheet 由来のスナップショットも使わない', async () => {
  modeOn();
  onResult = engine();
  assert.equal(onResult.errors?.length ?? 0, 0, JSON.stringify(onResult.errors));
  assert.equal(itemOf(onResult, 'Alpha-1').non_fba_sales_30d, 12);
  assert.ok(itemOf(onResult, 'Gamma-3'), 'マスタにだけある SKU が出ない');
  assert.equal(onResult.data_quality.unmapped_active.some((u) => u.sku === 'Unmapped-X'), false, 'Sheet 由来の他 CH スナップショットを使っている');
  assert.deepEqual(db.getAllNonFbaMax60d(), []);
  assert.deepEqual(db.getNonFbaMax60d('Unmapped-X'), { max_30d: 0, max_7d: 0 });
  const m = db.getSkuMapping('Alpha-1');
  assert.equal(m.non_fba_sales_30d, 12);
  assert.equal(m.product_name, 'マスタA');
});
await t('モードあり: sku_mapping の値を変えても (古い Sheet) 結果は同じ', async () => {
  modeOn();
  await editFileAndReload([`UPDATE sku_mapping SET non_fba_sales_30d = 9999, non_fba_sales_7d = 999, ne_code = 'wrong', asin = 'B0STALE', fnsku = 'X0STALE', product_name = '古い'`]);
  assert.deepEqual(engine(), onResult);
  assert.equal(db.getSkuMapping('Alpha-1').non_fba_sales_30d, 12);
});
await t('モードあり: sku_mapping が空でも結果は同じ', async () => {
  modeOn();
  await editFileAndReload(['DELETE FROM sku_mapping']);
  assert.deepEqual(engine(), onResult);
});
await t('モードあり: sku_mapping の表が無くても結果は同じ・除外一覧・FNSKU の更新も動く (= どこも読まない / 書かない)', async () => {
  modeOn();
  await editFileAndReload(['DROP TABLE sku_mapping']);
  assert.deepEqual(engine(), onResult);
  const ex = db.getReplenishmentExcluded();
  assert.deepEqual(ex.map((e) => [e.amazon_sku, e.product_name, e.asin]), [['Beta-2', 'マスタB', null]], '除外一覧の名前・ASIN を Sheet から引いている');
  db.updateFnskuBatch([{ sku: 'Gamma-3', fnsku: 'X0GAMMA' }]);
  db.syncFnskuBatch([{ sku: 'Gamma-3', fnsku: 'X0GAMMA2' }]);
  assert.equal(attrsOf('Gamma-3').fnsku, 'X0GAMMA2');
  const r = await call('GET', '/api/recommendations?persist=0');
  assert.equal(r.status, 200, JSON.stringify(r.body).slice(0, 300));
  assert.equal(r.body.items.find((i) => i.amazon_sku === 'Gamma-3').fnsku, 'X0GAMMA2');
  // 対照: モードなしなら表が無いと読みに行って落ちる (= 上の「同じ」は読んでいないから)
  modeOff();
  assert.throws(() => db.getSkuMappings(), /no such table: sku_mapping/);
  modeOn();
});
await t('表を戻す (モードありの起動 = backfill を流さない)', async () => {
  modeOn();
  await db.initDb();
  assert.deepEqual(sheetRows(), []);
  assert.deepEqual(engine(), onResult);
  modeOff();
  db.upsertSkuMappings([
    { amazon_sku: 'Alpha-1', asin: 'B0SHEETA', product_name: 'シートの名前A', ne_code: 'alpha', logizard_code: 'alpha', non_fba_sales_7d: 70, non_fba_sales_30d: 300 },
    { amazon_sku: 'Beta-2', asin: 'B0SHEETB', product_name: 'シートの名前B', ne_code: 'beta', logizard_code: 'beta', non_fba_sales_7d: 9, non_fba_sales_30d: 90 },
  ]);
});

// =====================================================================================
console.log('③ モードあり・商品管理リストが欠けた = 計算しない (前の結果はそのまま・fail の ping)');
await t('計算: エラーを返して何も作らない (Sheet の値に戻らない)', async () => {
  modeOn();
  removePml();
  const r = generateRecommendations(false, {}, ENGINE_OPTS);
  assert.deepEqual(r.items, []);
  assert.equal(r.sheetless_blocked, true);
  assert.match(r.errors.join(), /商品管理リスト/);
  assert.match(r.errors.join(), /Sheet には戻らない/);
  // v3 (9:40 の自動決定の決まり) も同じ
  assert.equal(generateRecommendations(false, {}, { ...ENGINE_OPTS, rules: 'v3' }).sheetless_blocked, true);
  publishPml();
});
await t('画面: 健全性の記録 (前回) を書かずに 503 = 画面は今の一覧のまま。SKU の詳細も 503', async () => {
  modeOn();
  miniHandler = () => ({ ok: true, count: 0, data: {} });
  const ok = await call('GET', '/api/recommendations?persist=1');
  assert.equal(ok.status, 200);
  const runs = db.getRecentRecommendationRuns(30);
  assert.equal(runs.length, 1);
  removePml();
  const r = await call('GET', '/api/recommendations?persist=1');
  assert.equal(r.status, 503);
  assert.equal(r.body.sheetless_blocked, true);
  assert.match(r.body.error, /商品管理リスト/);
  assert.deepEqual(db.getRecentRecommendationRuns(30), runs, '前の結果 (健全性の記録) を書き換えた');
  const one = await call('GET', '/api/recommendations/Alpha-1');
  assert.equal(one.status, 503);
  publishPml();
});
await t('9:40 の自動決定: 計算の失敗 = fail の ping。提案は 1 行も書かない', async () => {
  modeOn();
  removePml();
  const CAP = '2026-10-05T00:20:00.000Z';
  const rows = [{ 商品ID: 'alpha', 商品名: 'A', ブロック略称: 'A', ロケ: 'P-01', 有効期限: '', 在庫数: 200, 引当数: 0, ロケ業務区分: '通販', 最終入荷日: '20260901', ブロック引当順: 1, captured_at: CAP }];
  const meta = { captured_at: CAP, source_at: '2026-10-05T00:05:00.000Z', rows_read: 1, skipped_rows: 0, row_count: 1 };
  const queries = [], pings = [];
  const deps = {
    openClient: async () => ({ db: { query: async (sql) => { queries.push(sql); return { rows: /pg_try_advisory_lock/.test(sql) ? [{ ok: true }] : [] }; } }, close: async () => {} }),
    syncReports: async () => ({ ok: true, snapshot_date: '2026-10-05' }),
    fetchInbound: async () => ({ data: {}, state: { source: 'fresh', count: 0, at: 'x' } }),
    readMirror: () => ({ rows, meta }),
    readInputFreshness: () => ({}),
    readManualWarehouseSummary: () => [],
    generate: (inbound, opts) => generateRecommendations(false, inbound, { ...opts, ...ENGINE_OPTS }),
    readSettings: () => ({}),
    ping: (s, n) => pings.push([s, n]),
  };
  const r = await runDecisionAttempt(deps, { nowMs: () => Date.parse('2026-10-05T00:40:00Z'), log: quiet });
  assert.equal(r.outcome, 'engine_failed');
  assert.deepEqual(pings.map((p) => p[0]), ['fail']);
  assert.match(pings[0][1], /Sheet なしのモード/);
  assert.equal(queries.some((q) => /insert into ai\.decisions/i.test(q)), false, '提案を書いた');
  assert.ok(queries.some((q) => /insert into ops\.job_runs/i.test(q)), '失敗を記録していない');
  publishPml();
});
await t('SKU の対応 (mirror) が 0 行でも計算しない', async () => {
  modeOn();
  const saved = mdb.prepare('SELECT * FROM mirror_sku_resolved').all();
  mdb.prepare('DELETE FROM mirror_sku_resolved').run();
  try {
    const r = generateRecommendations(false, {}, ENGINE_OPTS);
    assert.equal(r.sheetless_blocked, true);
    assert.match(r.errors.join(), /mirror_sku_resolved\) が 0 行/);
  } finally {
    const ins = mdb.prepare('INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, source_updated_at, sort_order, synced_at) VALUES (@seller_sku, @ne_code, @quantity, @source, @商品名, @source_updated_at, @sort_order, @synced_at)');
    for (const s of saved) ins.run(s);
  }
  assert.deepEqual(engine(), onResult);
});

// =====================================================================================
console.log('④ 06:00 の定期同期 (モードあり)');
const dailyStubs = (called, pings, over = {}) => ({
  syncSkuMappings: async () => { called.push('sheet'); return { total: 3, snapshots: 3 }; },
  syncDodaiMaster: async () => { called.push('dodai'); return { count: 5 }; },
  runInboundHistoryDailySync: async () => { called.push('inbound'); return { shipments: 1, items: 2, items_failed: 0, items_failed_sample: [] }; },
  pingJob: (...a) => pings.push(a),
  ...over,
});
await t('Sheet の段だけ外す。土台・納品実績は続け、材料がそろえば ok', async () => {
  modeOn();
  const called = [], pings = [];
  await routerMod.runFbaDailySync(dailyStubs(called, pings));
  assert.deepEqual(called, ['dodai', 'inbound']);
  assert.equal(pings.length, 1);
  assert.equal(pings[0][1], 'ok');
  assert.match(pings[0][2], /^Sheetなし 対応=3 他CH=3 土台=5 納品=1\/2$/);
});
await t('商品管理リストが欠けたら fail (土台・納品実績は流す)', async () => {
  modeOn();
  removePml();
  const called = [], pings = [];
  await routerMod.runFbaDailySync(dailyStubs(called, pings));
  assert.deepEqual(called, ['dodai', 'inbound']);
  assert.equal(pings[0][1], 'fail');
  assert.match(pings[0][2], /Sheetなし 材料が欠けている: 商品管理リスト/);
  publishPml();
});
await t('材料がそろい納品実績が失敗なら partial (今までと同じ決まり)', async () => {
  modeOn();
  const called = [], pings = [];
  await routerMod.runFbaDailySync(dailyStubs(called, pings, { runInboundHistoryDailySync: async () => { throw new Error('miniPC 応答なし'); } }));
  assert.equal(pings[0][1], 'partial');
});
await t('台帳 (fba-daily-sync・fba-decision-draft) に Sheet なしのモードの成功の基準が書いてある', async () => {
  const { JOBS_REGISTRY: jobs } = await imp('config/jobs-registry.mjs');
  const daily = jobs.find((j) => j.id === 'fba-daily-sync');
  assert.match(daily.runbook, /FBA_SHEETLESS_MODE=1/);
  assert.match(daily.runbook, /ok の基準 = Sheet なしの材料/);
  assert.match(daily.runbook, /fba-sheetless-backfill-once\.mjs/);
  assert.match(jobs.find((j) => j.id === 'fba-decision-draft').runbook, /Sheet なしのモード/);
});

// =====================================================================================
console.log('⑤ 手の Sheet 同期の口は 410');
await t('Render の口: 410 + 日本語の理由 / Sheet の関数と sku_mapping への書き込みも断る', async () => {
  modeOn();
  const r = await call('POST', '/api/sync-sku-mappings');
  assert.equal(r.status, 410);
  assert.equal(r.body.sheetless, true);
  assert.match(r.body.error, /Sheet なしのモード/);
  const { syncSkuMappings } = await imp('apps/fba-replenishment/sheets-sync.js');
  await assert.rejects(syncSkuMappings(), (e) => e.code === 'FBA_SHEETLESS_GONE');
  assert.throws(() => db.upsertSkuMappings([{ amazon_sku: 'Hotel-8' }]), (e) => e.code === 'FBA_SHEETLESS_GONE');
});
await t('miniPC の口: 410', async () => {
  modeOn();
  const r = await call('POST', '/fba-service/sync-sku-mappings');
  assert.equal(r.status, 410);
  assert.equal(r.body.error, 'SHEETLESS_MODE');
  assert.match(r.body.message, /Sheet なしのモード/);
});

// =====================================================================================
console.log('⑥ FNSKU は fba_sku_attrs だけ');
await t('更新の 2 つの口とも sku_mapping に書かない (古い値が残る = 書いていない)', async () => {
  modeOff();
  db.updateFnskuBatch([{ sku: 'Alpha-1', fnsku: 'X0BEFORE' }]);
  modeOn();
  db.updateFnskuBatch([{ sku: 'Alpha-1', fnsku: 'X0NEW' }]);
  assert.equal(attrsOf('Alpha-1').fnsku, 'X0NEW');
  assert.equal(sheetRows().find((r) => r.amazon_sku === 'Alpha-1').fnsku, 'X0BEFORE');
  db.syncFnskuBatch([{ sku: 'Alpha-1', fnsku: null }]);
  assert.equal(attrsOf('Alpha-1').fnsku, null);
  assert.equal(sheetRows().find((r) => r.amazon_sku === 'Alpha-1').fnsku, 'X0BEFORE');
  assert.equal(db.getSkuMapping('Alpha-1').fnsku, null, 'FNSKU を sku_mapping から取っている');
});
await t('Render の引き取り: ?fnsku_source=attrs で頼み、miniPC が attrs からと答えたときだけ入れる (attrs だけ)', async () => {
  modeOn();
  miniCalls.length = 0;
  miniHandler = (u) => (u.includes('/sync/latest-planning') ? {
    ok: true, snapshot_date: '2026-09-30', rows: [{ amazon_sku: 'Alpha-1', product_name: 'A', units_sold_30d: 60 }],
    fnskus: [{ sku: 'Alpha-1', fnsku: 'X0PULLON' }], restock_rows: [], planning_latest_rows: [], fnsku_source: 'fba_sku_attrs',
  } : { ok: false });
  const r = await routerMod.syncLatestPlanningFromMiniPC();
  assert.deepEqual(miniCalls, ['http://minipc.test/service-api/fba/sync/latest-planning?fnsku_source=attrs']);
  assert.equal(r.fnskus, 1);
  assert.equal(attrsOf('Alpha-1').fnsku, 'X0PULLON');
  assert.equal(sheetRows().find((x) => x.amazon_sku === 'Alpha-1').fnsku, 'X0BEFORE');
});
await t('Render の引き取り: miniPC が古い (fnsku_source なし) なら FNSKU を反映しない (前の値のまま)・理由を返す', async () => {
  modeOn();
  miniHandler = (u) => (u.includes('/sync/latest-planning') ? {
    ok: true, snapshot_date: '2026-09-30', rows: [{ amazon_sku: 'Alpha-1', product_name: 'A', units_sold_30d: 60 }],
    fnskus: [{ sku: 'Alpha-1', fnsku: 'X0FROMSHEET' }], restock_rows: [], planning_latest_rows: [],
  } : { ok: false });
  const r = await routerMod.syncLatestPlanningFromMiniPC();
  assert.equal(r.ok, true);
  assert.equal(r.fnskus, 0);
  assert.match(r.fnsku_skip_reason, /fba_sku_attrs からではない/);
  assert.equal(attrsOf('Alpha-1').fnsku, 'X0PULLON');
});
await t('miniPC の口: ?fnsku_source=attrs なら fba_sku_attrs の全行を返し、fnsku_source を付ける', async () => {
  modeOff();   // miniPC 側のモードとは別 (Render が頼み方で決める)
  const r = await call('GET', '/fba-service/sync/latest-planning?fnsku_source=attrs');
  assert.equal(r.status, 200);
  assert.equal(r.body.fnsku_source, 'fba_sku_attrs');
  const attrs = db.getFbaSkuAttrs();
  assert.deepEqual(r.body.fnskus, attrs.map((a) => ({ sku: a.amazon_sku, fnsku: a.fnsku || null })));
  assert.ok(r.body.fnskus.some((f) => f.sku === 'Delta-4'), 'mirror に無い SKU も attrs にあれば返す');
});

// =====================================================================================
console.log('⑦ 起動時の backfill と一回限りの移行');
const cli = (args = [], mode = null) => {
  const env = { ...process.env, DATA_DIR: dataDir };
  delete env.FBA_SHEETLESS_MODE;
  if (mode !== null) env.FBA_SHEETLESS_MODE = mode;
  const r = spawnSync(process.execPath, [path.join(root, 'scripts', 'fba-sheetless-backfill-once.mjs'), ...args], { cwd: dataDir, env, encoding: 'utf8', windowsHide: true, timeout: 120000 });
  return { code: r.status, out: `${r.stdout}${r.stderr}` };
};
await t('モードあり: 起動時の backfill を流さない (Sheet の古い値が attrs に入らない)', async () => {
  modeOff();
  db.upsertSkuMappings([{ amazon_sku: 'Echo-5', asin: 'B0ECHO', ne_code: 'echo' }]);
  modeOn();
  await db.initDb();
  assert.equal(attrsOf('Echo-5'), null);
});
await t('移行のスクリプト: --check は書かない', async () => {
  const st = fs.statSync(dbFile);
  const r = cli(['--check']);
  assert.equal(r.code, 0, r.out);
  assert.match(r.out, /印=なし/);
  assert.match(r.out, /入れる予定 1 行/);
  const st2 = fs.statSync(dbFile);
  assert.deepEqual([st2.mtimeMs, st2.size], [st.mtimeMs, st.size]);
});
await t('移行のスクリプト: モードが入っている間は断る (印を書かない)', async () => {
  const r = cli([], '1');
  assert.equal(r.code, 1, r.out);
  assert.match(r.out, /断った/);
  assert.deepEqual(fileMark(), []);
  const r2 = cli([], 'yes');
  assert.equal(r2.code, 1, r2.out);
});
await t('移行のスクリプト: モードなしで流すと backfill して印を残す (時刻と件数)', async () => {
  const r = cli([]);
  assert.equal(r.code, 0, r.out);
  assert.match(r.out, /済んだ/);
  const marks = fileMark();
  assert.equal(marks.length, 1);
  assert.equal(marks[0].key, sheetless.BACKFILL_MARK_KEY);
  const detail = JSON.parse(marks[0].detail);
  assert.equal(detail.before_init.would_insert, 1);
  assert.ok(Number.isFinite(detail.attrs_after));
  assert.ok(!Number.isNaN(Date.parse(marks[0].done_at)));
});
await t('移行のスクリプト: 二度目は断る', async () => {
  const r = cli([]);
  assert.equal(r.code, 1, r.out);
  assert.match(r.out, /二度は流さない/);
  assert.equal(fileMark().length, 1);
});
await t('印があれば、モードを外しても起動時の backfill を流さない', async () => {
  modeOff();
  await db.initDb();   // スクリプトが書いたファイルを読み直す
  assert.deepEqual(attrsOf('Echo-5'), { amazon_sku: 'Echo-5', asin: 'B0ECHO', fnsku: null }, '移行のスクリプトが入れた');
  assert.equal(db.getBackfillMark().key, sheetless.BACKFILL_MARK_KEY);
  db.upsertSkuMappings([{ amazon_sku: 'Golf-7', asin: 'B0GOLF', ne_code: 'golf' }]);
  await db.initDb();
  assert.equal(attrsOf('Golf-7'), null);
  assert.throws(() => db.runSkuMappingBackfillOnce(), (e) => e.code === 'FBA_BACKFILL_ALREADY_DONE');
});

// =====================================================================================
console.log('⑧ 設定がそろわない = 計算しない (黙ってモードを外さない)');
for (const [name, env, re] of [
  ['FBA_SKU_MAPPING_SOURCE が mirror でない', { FBA_SKU_MAPPING_SOURCE: 'sheet' }, /FBA_SKU_MAPPING_SOURCE が mirror ではない/],
  ['FBA_SKU_MAPPING_SOURCE が Mirror (大小文字違い = 実際は sheet で読まれる)', { FBA_SKU_MAPPING_SOURCE: 'Mirror' }, /mirror ではない/],
  ['FBA_NONFBA_SOURCE が pml でない', { FBA_NONFBA_SOURCE: 'shadow' }, /FBA_NONFBA_SOURCE が pml ではない/],
  ['FBA_SHEETLESS_MODE が 1 以外 (true)', { FBA_SHEETLESS_MODE: 'true' }, /FBA_SHEETLESS_MODE の値が 1 ではない/],
]) {
  await t(name, async () => {
    const saved = { FBA_SKU_MAPPING_SOURCE: process.env.FBA_SKU_MAPPING_SOURCE, FBA_NONFBA_SOURCE: process.env.FBA_NONFBA_SOURCE, FBA_SHEETLESS_MODE: '1' };
    Object.assign(process.env, saved, env);
    try {
      const r = generateRecommendations(false, {}, ENGINE_OPTS);
      assert.equal(r.sheetless_blocked, true);
      assert.match(r.errors.join(), re);
      assert.throws(() => db.getSkuMappings(), (e) => e.code === 'FBA_SHEETLESS_MISCONFIG');
      assert.throws(() => db.getSkuMapping('Alpha-1'), (e) => e.code === 'FBA_SHEETLESS_MISCONFIG');
      const pings = [], called = [];
      await routerMod.runFbaDailySync(dailyStubs(called, pings));
      assert.deepEqual(called, ['dodai', 'inbound']);
      assert.equal(pings[0][1], 'fail');
      assert.equal((await call('POST', '/api/sync-sku-mappings')).status, 410);
    } finally {
      process.env.FBA_SKU_MAPPING_SOURCE = saved.FBA_SKU_MAPPING_SOURCE;
      process.env.FBA_NONFBA_SOURCE = saved.FBA_NONFBA_SOURCE;
      modeOff();
    }
  });
}

server.close();
globalThis.fetch = realFetch;
console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
