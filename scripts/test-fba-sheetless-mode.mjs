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
import vm from 'node:vm';
import Database from 'better-sqlite3';
import express from 'express';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-sheetless-'));
process.env.DATA_DIR = dataDir;
process.env.FBA_SKU_MAPPING_SOURCE = 'mirror';
process.env.FBA_NONFBA_SOURCE = 'pml';
process.env.WAREHOUSE_URL = 'http://minipc.test';
for (const k of ['FBA_SHEETLESS_MODE', 'FBA_SHEETLESS_IO', 'RENDER', 'GOOGLE_SERVICE_ACCOUNT_KEY', 'JOBS_MONITOR_ENABLED']) delete process.env[k];
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
const usRouter = (await imp('apps/fba-replenishment-us/router.js')).default;
const app = express();
app.set('views', path.join(root, 'views'));
app.set('view engine', 'ejs');
app.use('/fba-service', express.json(), fbaService);
app.use('/us', usRouter);
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
await t('モードありでも、この fba.db に一回限りの移行の印が無ければ計算しない (Codex PR R2 Medium 1) → 印を書くと計算する', async () => {
  modeOn();
  const r = generateRecommendations(false, {}, ENGINE_OPTS);
  assert.equal(r.sheetless_blocked, true);
  assert.match(r.errors.join(), /一回限りの移行の印が無い/);
  modeOff();
  const done = db.runSkuMappingBackfillOnce();
  assert.equal(done.missing_after, 0);
  modeOn();
  assert.equal(generateRecommendations(false, {}, ENGINE_OPTS).sheetless_blocked, undefined);
  modeOff();
});
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
  const queries = [], pings = [], params = [];
  const deps = {
    openClient: async () => ({ db: { query: async (sql, p) => { queries.push(sql); params.push(p); return { rows: /pg_try_advisory_lock/.test(sql) ? [{ ok: true }] : [] }; } }, close: async () => {} }),
    syncReports: async () => ({ ok: true, snapshot_date: '2026-10-05' }),
    fetchInbound: async () => ({ data: {}, state: { source: 'fresh', count: 0, at: 'x' } }),
    readMirror: () => ({ rows, meta }),
    readInputFreshness: () => ({}),
    readManualWarehouseSummary: () => [],
    generate: (inbound, opts) => generateRecommendations(false, inbound, { ...opts, ...ENGINE_OPTS }),
    readSettings: () => ({}),
    ping: (s, n) => pings.push([s, n]),
  };
  try {
    const r = await runDecisionAttempt(deps, { nowMs: () => Date.parse('2026-10-05T00:40:00Z'), log: quiet });
    assert.equal(r.outcome, 'engine_failed');
    assert.deepEqual(pings.map((p) => p[0]), ['fail']);
    assert.match(pings[0][1], /Sheet なしのモード/);
    assert.equal(queries.some((q) => /set status = 'superseded'/i.test(q)), false, '前の提案を superseded にした (Codex PR R1 High 2)');
    const marks = queries.map((q, i) => [q, params[i]]).filter(([q]) => /insert into ai\.decisions/i.test(q));
    assert.equal(marks.length, 1, '止めた印 (run 要約行) だけを書く');
    const ref = marks[0][1].find((x) => x && typeof x === 'object' && 'send_blocked' in x);
    assert.deepEqual([ref.send_blocked, ref.sheetless_blocked, ref.run_summary, ref.decision_final], [true, true, true, false]);
    assert.ok(queries.some((q) => /insert into ops\.job_runs/i.test(q)), '失敗を記録していない');
  } finally {
    publishPml();
  }
});
await t('9:40 の自動決定: 倉庫の写しが読めない・古い日でも、Sheet なしの材料が欠けていれば止めた印の道 (最後の回でも前の提案を superseded にしない。Codex PR R2 High 1)', async () => {
  const src = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'router.js'), 'utf8');
  assert.match(src, /checkSheetless: \(\) => getSheetlessCalcBlock\(\),/, '本番の自動決定に Sheet なしの確かめを渡していない');
  const CAP = '2026-10-05T00:20:00.000Z';
  const staleRows = [{ 商品ID: 'alpha', 商品名: 'A', ブロック略称: 'A', ロケ: 'P-01', 有効期限: '', 在庫数: 200, 引当数: 0, ロケ業務区分: '通販', 最終入荷日: '20260901', ブロック引当順: 1, captured_at: CAP }];
  const staleMeta = { captured_at: CAP, source_at: '2026-10-04T20:00:00.000Z', rows_read: 1, skipped_rows: 0, row_count: 1 };
  for (const [name, readMirror] of [
    ['写しの DB ごと読めない', () => { throw new Error('warehouse-mirror.db を開けない'); }],
    ['写しが古い', () => ({ rows: staleRows, meta: staleMeta })],
  ]) {
    modeOn();
    removePml();
    try {
      const queries = [], pings = [], generated = [];
      const deps = {
        openClient: async () => ({ db: { query: async (sql) => { queries.push(sql); return { rows: /pg_try_advisory_lock/.test(sql) ? [{ ok: true }] : [] }; } }, close: async () => {} }),
        syncReports: async () => ({ ok: true, snapshot_date: '2026-10-05' }),
        fetchInbound: async () => ({ data: {}, state: { source: 'fresh', count: 0, at: 'x' } }),
        readMirror, readInputFreshness: () => ({}), readManualWarehouseSummary: () => [],
        generate: (inbound, opts) => { generated.push(opts.rules); return generateRecommendations(false, inbound, { ...opts, ...ENGINE_OPTS }); },
        readSettings: () => ({}),
        ping: (st, n) => pings.push([st, n]),
        checkSheetless: () => db.getSheetlessCalcBlock(),
      };
      const r = await runDecisionAttempt(deps, { nowMs: () => Date.parse('2026-10-05T02:40:00Z'), log: quiet });   // 11:40 = 最後の回
      assert.equal(r.outcome, 'engine_failed', `${name}: ${r.outcome}`);
      assert.deepEqual(pings.map((p) => p[0]), ['fail'], name);
      assert.match(pings[0][1], /Sheet なしのモード/, name);
      assert.deepEqual(generated, [], `${name}: 止めたのに計算した`);
      assert.equal(queries.some((q) => /set status = 'superseded'/i.test(q)), false, `${name}: 前の提案を superseded にした`);
      assert.equal(queries.filter((q) => /insert into ai\.decisions/i.test(q)).length, 1, `${name}: 止めた印を書いていない`);
    } finally { publishPml(); }
  }
  modeOff();
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
  // 一回限りの移行・比べる道具・モードなしの Sheet の経路は一時物 (撤去期限つき)
  const tmp = jobs.find((j) => j.id === 'fba-sheetless-transition');
  assert.equal(tmp?.type, 'temporary_asset');
  assert.match(tmp.remove_by, /^\d{4}-\d{2}-\d{2}$/);
  assert.match(tmp.where, /fba-sheetless-backfill-once\.mjs/);
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
    fnskus: [{ sku: 'Alpha-1', fnsku: 'X0PULLON' }], restock_rows: [], planning_latest_rows: [], fnsku_source: 'fba_sku_attrs', fnsku_ready: true,
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
  assert.equal(r.body.fnsku_ready, true, 'この fba.db には ② で移行の印を書いた');
  const attrs = db.getFbaSkuAttrs();
  assert.deepEqual(r.body.fnskus, attrs.map((a) => ({ sku: a.amazon_sku, fnsku: a.fnsku || null })));
  assert.ok(r.body.fnskus.some((f) => f.sku === 'Delta-4'), 'mirror に無い SKU も attrs にあれば返す');
});

// =====================================================================================
console.log('⑦ 起動時の backfill と一回限りの移行 (印の無い別の fba.db で)');
const cli = (args = [], mode = null, { io = null, dir = dataDir } = {}) => {
  const env = { ...process.env, DATA_DIR: dir };
  delete env.FBA_SHEETLESS_MODE;
  delete env.FBA_SHEETLESS_IO;
  if (mode !== null) env.FBA_SHEETLESS_MODE = mode;
  if (io !== null) env.FBA_SHEETLESS_IO = io;
  const r = spawnSync(process.execPath, [path.join(root, 'scripts', 'fba-sheetless-backfill-once.mjs'), ...args], { cwd: dir, env, encoding: 'utf8', windowsHide: true, timeout: 120000 });
  return { code: r.status, out: `${r.stdout}${r.stderr}` };
};
// 本体の fba.db は ② で印を書いたので、移行は別の fba.db で試す (module の実体を分ける = 別のプロセスの代わり)
modeOff();
const cliDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-sheetless-cli-'));
process.env.DATA_DIR = cliDir;
const C = await import(`${pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'db.js')).href}?proc=cli`);
process.env.DATA_DIR = dataDir;
const cliFile = path.join(cliDir, 'fba.db');
const cliMark = () => { const f = new Database(cliFile, { readonly: true }); try { return f.prepare(`SELECT 1 FROM sqlite_master WHERE name = 'fba_migration_marks'`).get() ? f.prepare('SELECT * FROM fba_migration_marks').all() : []; } finally { f.close(); } };
const cliState = () => { const f = new Database(cliFile, { readonly: true }); try { return f.prepare(`SELECT 1 FROM sqlite_master WHERE name = 'fba_sheetless_state'`).get() ? f.prepare(`SELECT value FROM fba_sheetless_state WHERE key = 'sheet_frozen'`).get()?.value ?? null : 'no-table'; } finally { f.close(); } };
const cAttrs = (sku) => C.getFbaSkuAttrs().find((a) => a.amazon_sku === sku) || null;
await C.initDb();
C.upsertSkuMappings([{ amazon_sku: 'Alpha-1', asin: 'B0SHEETA', ne_code: 'alpha' }, { amazon_sku: 'Beta-2', asin: 'B0SHEETB', ne_code: 'beta' }]);
await C.initDb();   // 今までどおりの起動 = backfill で Alpha-1・Beta-2 が attrs に入る
await t('モードなしの起動は「凍結」の印を書かない (表を作らない = fba.db の形は今のまま)', async () => {
  assert.equal(cliState(), 'no-table');
  assert.ok(cAttrs('Alpha-1'));
});
await t('モードあり (印なし): 起動時の backfill を流さない (Sheet の古い値が attrs に入らない)・凍結の印を書く', async () => {
  modeOff();
  C.upsertSkuMappings([{ amazon_sku: 'Echo-5', asin: 'B0ECHO', ne_code: 'echo' }]);
  modeOn();
  await C.initDb();
  assert.equal(cAttrs('Echo-5'), null);
  assert.equal(cliState(), '1');
  // 一回限りの移行も、モードが入っている間は流せない (db.js 側の歯止め)
  assert.throws(() => C.runSkuMappingBackfillOnce(), (e) => e.code === 'FBA_BACKFILL_MODE_ON');
  assert.equal(cAttrs('Echo-5'), null);
  assert.equal(C.getBackfillMark(), null);
  modeOff();
});
await t('移行のスクリプト: --check は書かない', async () => {
  const st = fs.statSync(cliFile);
  const r = cli(['--check'], null, { dir: cliDir });
  assert.equal(r.code, 0, r.out);
  assert.match(r.out, /印=なし/);
  assert.match(r.out, /入れる予定 1 行/);
  const st2 = fs.statSync(cliFile);
  assert.deepEqual([st2.mtimeMs, st2.size], [st.mtimeMs, st.size]);
});
await t('移行のスクリプト: モード / IO が入っている間は断る (印を書かない・fba.db に触らない)', async () => {
  const st = fs.statSync(cliFile);
  await tick();
  const r = cli([], '1', { dir: cliDir });
  assert.equal(r.code, 1, r.out);
  assert.match(r.out, /断った/);
  assert.deepEqual(cliMark(), []);
  const r2 = cli([], 'yes', { dir: cliDir });
  assert.equal(r2.code, 1, r2.out);
  const r3 = cli(['--min-rows', '1'], null, { io: '1', dir: cliDir });   // miniPC の入出力の止めが入っていても断る
  assert.equal(r3.code, 1, r3.out);
  assert.match(r3.out, /FBA_SHEETLESS_IO/);
  const st2 = fs.statSync(cliFile);
  assert.deepEqual([st2.mtimeMs, st2.size], [st.mtimeMs, st.size], '断ったのに fba.db を書いた');
});
await t('移行のスクリプト: 既定の下限 (100 行) より少ない sku_mapping は断る', async () => {
  const r = cli([], null, { dir: cliDir });
  assert.equal(r.code, 1, r.out);
  assert.match(r.out, /下限 100/);
  assert.deepEqual(cliMark(), []);
});
await t('移行のスクリプト: モードなしで流すと backfill して印を残す (時刻と件数)', async () => {
  const r = cli(['--min-rows', '1'], null, { dir: cliDir });
  assert.equal(r.code, 0, r.out);
  assert.match(r.out, /済んだ/);
  const marks = cliMark();
  assert.equal(marks.length, 1);
  assert.equal(marks[0].key, sheetless.BACKFILL_MARK_KEY);
  const detail = JSON.parse(marks[0].detail);
  assert.equal(detail.before_init.would_insert, 1);
  assert.equal(detail.missing_after, 0);
  assert.ok(Number.isFinite(detail.attrs_after));
  assert.ok(!Number.isNaN(Date.parse(marks[0].done_at)));
});
await t('移行のスクリプト: 二度目は断る (fba.db に触らない = 開いて保存もしない)', async () => {
  const st = fs.statSync(cliFile);
  await tick();
  const r = cli(['--min-rows', '1'], null, { dir: cliDir });
  assert.equal(r.code, 1, r.out);
  assert.match(r.out, /二度は流さない/);
  assert.equal(cliMark().length, 1);
  const st2 = fs.statSync(cliFile);
  assert.deepEqual([st2.mtimeMs, st2.size], [st.mtimeMs, st.size], '断ったのに fba.db を書いた');
});
await t('印があれば、モードを外しても起動時の backfill を流さない・凍結の印は 0 に戻る', async () => {
  modeOff();
  await C.initDb();   // スクリプトが書いたファイルを読み直す
  assert.deepEqual(cAttrs('Echo-5'), { amazon_sku: 'Echo-5', asin: 'B0ECHO', fnsku: null }, '移行のスクリプトが入れた');
  assert.equal(C.getBackfillMark().key, sheetless.BACKFILL_MARK_KEY);
  assert.equal(cliState(), '0');
  C.upsertSkuMappings([{ amazon_sku: 'Golf-7', asin: 'B0GOLF', ne_code: 'golf' }]);
  await C.initDb();
  assert.equal(cAttrs('Golf-7'), null);
  assert.throws(() => C.runSkuMappingBackfillOnce(), (e) => e.code === 'FBA_BACKFILL_ALREADY_DONE');
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
      // 画面の状態の口・米国の画面も、そのままの 500 ではなく理由 (日本語) を返す
      const st = await call('GET', '/api/status');
      assert.equal(st.status, 503);
      assert.equal(st.body.sheetless_misconfig, true);
      assert.match(st.body.error, /Sheet なしのモードの設定がそろっていない/);
      const us = await imp('apps/fba-replenishment-us/router.js');
      await assert.rejects(us.loadJpInputs(), (e) => e.code === 'FBA_SHEETLESS_BLOCKED' && /Sheet なしのモード/.test(e.message));
    } finally {
      process.env.FBA_SKU_MAPPING_SOURCE = saved.FBA_SKU_MAPPING_SOURCE;
      process.env.FBA_NONFBA_SOURCE = saved.FBA_NONFBA_SOURCE;
      modeOff();
    }
  });
}

// =====================================================================================
console.log('⑨ 大小文字・ASIN・移行のやり直し (独立レビューの直し)');
await t('モードあり: どのレポートにも無い大文字の SKU は、fba_sku_attrs の大小文字を保つ (除外の完全一致も合う) / モードなしは今までどおり', async () => {
  modeOn();
  mdb.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, source_updated_at, sort_order, synced_at) VALUES ('pr_new001', 'newc', 1, 'master', 'マスタN', ?, 0, ?)`).run(now, now);
  mdb.prepare('INSERT INTO mirror_sku_master (seller_sku, 商品名, source_created_at, source_updated_at, synced_at) VALUES (?, ?, ?, ?, ?)').run('pr_new001', 'マスタN', now, now, now);
  db.syncFnskuBatch([{ sku: 'pr_NEW001', fnsku: 'X0NEW1', asin: 'B0NEW00001' }]);
  const m = db.getSkuMappings().find((x) => x.amazon_sku.toLowerCase() === 'pr_new001');
  assert.deepEqual([m.amazon_sku, m.fnsku, m.asin], ['pr_NEW001', 'X0NEW1', 'B0NEW00001'], '小文字になった (納品プランの MSKU・除外・非表示の完全一致が外れる)');
  db.excludeReplenishmentSku('pr_NEW001', '試験');
  assert.ok(new Set(db.getReplenishmentExcluded().map((e) => e.amazon_sku)).has(m.amazon_sku));
  db.unexcludeReplenishmentSku('pr_NEW001');
  modeOff();
  assert.equal(db.getSkuMappings().find((x) => x.amazon_sku.toLowerCase() === 'pr_new001').amazon_sku, 'pr_new001', 'モードなしで fba_sku_attrs を大小文字の出どころにしている (今までの動きが変わった)');
});
await t('モードあり: FNSKU の更新で ASIN も fba_sku_attrs に書く (空なら前のまま) / モードなしは ASIN に触らない', async () => {
  modeOn();
  db.updateFnskuBatch([{ sku: 'pr_NEW001', fnsku: 'X0NEW2', asin: 'B0NEW00002' }]);
  assert.deepEqual([attrsOf('pr_NEW001').fnsku, attrsOf('pr_NEW001').asin], ['X0NEW2', 'B0NEW00002']);
  db.syncFnskuBatch([{ sku: 'pr_NEW001', fnsku: 'X0NEW3' }, { sku: 'pr_NEW001', fnsku: 'X0NEW3', asin: '  ' }]);
  assert.deepEqual([attrsOf('pr_NEW001').fnsku, attrsOf('pr_NEW001').asin], ['X0NEW3', 'B0NEW00002']);
  modeOff();
  db.syncFnskuBatch([{ sku: 'pr_NEW001', fnsku: 'X0NEW3', asin: 'B0OFF' }]);
  db.updateFnskuBatch([{ sku: 'Gamma-3', fnsku: 'X0GAMMA3', asin: 'B0OFFG' }]);
  assert.equal(attrsOf('pr_NEW001').asin, 'B0NEW00002');
  assert.equal(attrsOf('Gamma-3').asin, null);
});
await t('モードあり: Render の引き取りで、同じ回の RESTOCK の ASIN も入れる (SKU の大小文字は無視) / モードなしは入れない', async () => {
  const pull = (withSource, asin) => (u) => (u.includes('/sync/latest-planning') ? {
    ok: true, snapshot_date: '2026-09-30', rows: [{ amazon_sku: 'Alpha-1', product_name: 'A', units_sold_30d: 60 }],
    fnskus: [{ sku: 'pr_NEW001', fnsku: 'X0PULL' }], restock_rows: [{ amazon_sku: 'PR_new001', asin }], planning_latest_rows: [],
    ...(withSource ? { fnsku_source: 'fba_sku_attrs', fnsku_ready: true } : {}),
  } : { ok: false });
  modeOn();
  miniHandler = pull(true, 'B0FROMRESTOCK');
  await routerMod.syncLatestPlanningFromMiniPC();
  assert.deepEqual([attrsOf('pr_NEW001').fnsku, attrsOf('pr_NEW001').asin], ['X0PULL', 'B0FROMRESTOCK']);
  modeOff();
  miniHandler = pull(false, 'B0OFFPULL');
  await routerMod.syncLatestPlanningFromMiniPC();
  assert.equal(attrsOf('pr_NEW001').asin, 'B0FROMRESTOCK');
});
await t('移行のやり直し: 流している間に常駐のサーバが fba.db を書いた → 読み直して 1 回やり直し、相手の行も印も残る', async () => {
  modeOff();
  const dir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-sheetless-2-'));
  const dbUrl = pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'db.js')).href;
  process.env.DATA_DIR = dir2;   // db.js は import の時点で DATA_DIR を読む。module の実体を分ける = 別のプロセスの代わり
  const A = await import(dbUrl + '?proc=resident');
  const B = await import(dbUrl + '?proc=script');
  process.env.DATA_DIR = dataDir;
  await A.initDb();
  A.upsertSkuMappings([{ amazon_sku: 'Kilo-1', asin: 'B0KILO', ne_code: 'kilo' }]);
  await tick();
  await B.initDb();   // 移行のスクリプトの役 (起動時の backfill で Kilo-1 が入る)
  await tick();
  let code = null;
  try { A.updateSetting('resident_memo', 'a1'); } catch (e) { code = e.code; }
  assert.equal(code, 'FBA_DB_EXTERNAL_WRITE', '前提: 常駐の役もスクリプトの保存に気づく');
  A.updateSetting('resident_memo', 'a1');   // 常駐の役はやり直して書く (スクリプトが読んだ後)
  await tick();
  const retries = [];
  const r = B.runSkuMappingBackfillOnceRetrying({ onRetry: (n, e) => retries.push([n, e.code]) });
  assert.deepEqual(retries, [[1, 'FBA_DB_EXTERNAL_WRITE']]);
  assert.equal(r.attrs_after, 1);
  const f = new Database(path.join(dir2, 'fba.db'), { readonly: true });
  try {
    assert.equal(f.prepare(`SELECT value FROM settings WHERE key = 'resident_memo'`).get()?.value, 'a1', '常駐の役の行が消えた');
    assert.equal(f.prepare('SELECT key FROM fba_migration_marks').get()?.key, sheetless.BACKFILL_MARK_KEY);
  } finally { f.close(); }
  // もう一度流しても断る (印がある)
  assert.throws(() => B.runSkuMappingBackfillOnceRetrying(), (e) => e.code === 'FBA_BACKFILL_ALREADY_DONE');
});

// =====================================================================================
console.log('⑩ Codex PR R1 の直し (仮確定・miniPC の入出力・古い miniPC・空の DB・米国の画面)');
const dbUrl = pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'db.js')).href;
/** 別の fba.db (DATA_DIR) を持つ db.js の実体 (= 別のプロセスの代わり)。import の時点で DATA_DIR を読む */
async function otherDb(tag) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `fba-sheetless-${tag}-`));
  process.env.DATA_DIR = dir;
  try { return { dir, mod: await import(`${dbUrl}?proc=${tag}`) }; } finally { process.env.DATA_DIR = dataDir; }
}
const PROV = [{ amazon_sku: 'Alpha-1', product_name: 'A', fnsku: 'X0A', ship_qty: 12, fba_available: 0, units_sold_7d: 1, units_sold_30d: 4, warehouse_raw: 10, recommended_qty: 12, urgency_score: 1, set_components: null, asin: 'B0A', expiry_date: null }];

const RECALC = '/api/recommendations/recalculate?debug=1&persist=1';
const provRows = () => db.getProvisionalItems().items.map((p) => [p.amazon_sku, p.ship_qty]);
await t('Step4 (モードあり): POST の口。計算が止まった (503) ら Amazon 仮確定は消えない / 計算できたときだけ同じ操作で消える', async () => {
  modeOn();
  miniHandler = () => ({ ok: true, count: 0, data: {} });
  db.saveProvisionalItems(PROV);
  removePml();
  try {
    const r = await call('POST', RECALC);
    assert.equal(r.status, 503);
    assert.equal(r.body.provisional_cleared, false);
    assert.deepEqual(provRows(), [['Alpha-1', 12]], '止まった日に仮確定を消した');
  } finally { publishPml(); }
  const ok = await call('POST', RECALC);
  assert.equal(ok.status, 200);
  assert.equal(ok.body.provisional_cleared, true);
  assert.deepEqual(db.getProvisionalItems().items, []);
  modeOff();
});
await t('Step4 (モードあり): 計算している間に仮確定が変わったら消さずに 409 (Codex PR R2 Medium 2)', async () => {
  modeOn();
  db.saveProvisionalItems(PROV);
  routerMod._routerTestHooks.afterRecalcFingerprint = async () => { db.saveProvisionalItems([{ ...PROV[0], ship_qty: 30 }]); };
  try {
    const r = await call('POST', RECALC);
    assert.equal(r.status, 409);
    assert.equal(r.body.provisional_changed, true);
    assert.match(r.body.error, /消していない/);
    assert.deepEqual(provRows(), [['Alpha-1', 30]], '計算の間に入れた仮確定を消した');
  } finally { routerMod._routerTestHooks.afterRecalcFingerprint = null; }
  modeOff();
});
await t('Step4 (モードなし): POST の口は 400・GET に clear_provisional を付けても消さない (今までどおり GET は消さない)', async () => {
  modeOff();
  db.saveProvisionalItems(PROV);
  const r = await call('POST', RECALC);
  assert.equal(r.status, 400);
  const g = await call('GET', '/api/recommendations?debug=1&persist=0&clear_provisional=1');
  assert.equal(g.status, 200);
  assert.equal('provisional_cleared' in g.body, false);
  assert.deepEqual(provRows(), [['Alpha-1', 12]]);
});
await t('Step4 の画面: モードありは先に消さず POST で頼み、止まったら選んだ SKU・数量も残す / モードなしは今までどおり DELETE → GET (同じ URL)', async () => {
  const page = async () => { const r = await realFetch(base + '/'); assert.equal(r.status, 200); return r.text(); };
  /** 画面の calcRecommendations を取り出して、fetch を差し替えて 1 回押す (応答 = 計算が止まった 503) */
  const press = (html) => {
    const start = html.indexOf('async function calcRecommendations()');
    const end = html.indexOf('// 推奨健全性バナー', start);
    assert.ok(start > 0 && end > start, '画面に calcRecommendations が無い');
    const calls = [], logs = [];
    const selectedSkus = new Set(['Alpha-1']);
    const shipQtyMap = { 'Alpha-1': 3 };
    const logEl = { dataset: /id="log" data-sheetless="1"/.test(html) ? { sheetless: '1' } : {} };
    const el = () => ({ style: {}, disabled: false, innerHTML: '', textContent: '' });
    const ctx = vm.createContext({
      confirm: () => true, log: (m) => logs.push(m), BASE: '', selectedSkus, shipQtyMap,
      document: { getElementById: (id) => (id === 'log' ? logEl : el()) },
      fetch: async (url, opts) => { calls.push(`${(opts && opts.method) || 'GET'} ${url}`); return { json: async () => (String(url).includes('/api/recommendations') ? { error: '止まった', sheetless_blocked: true } : { success: true }) }; },
    });
    vm.runInContext(`${html.slice(start, end)}\nglobalThis.__press = calcRecommendations;`, ctx);
    return ctx.__press().then(() => ({ calls, logs, selected: [...selectedSkus], qty: { ...shipQtyMap } }));
  };
  modeOn();
  const on = await press(await page());
  assert.deepEqual(on.calls, ['POST /api/recommendations/recalculate?debug=1&persist=1']);
  assert.ok(on.logs.some((m) => /Amazon仮確定・選んだ SKU・数量は消していません/.test(m)));
  assert.deepEqual([on.selected, on.qty], [['Alpha-1'], { 'Alpha-1': 3 }], '計算が止まったのに選んだ SKU・数量を消した');
  modeOff();
  const htmlOff = await page();
  assert.equal(htmlOff.includes('data-sheetless'), false, 'モードなしの画面に印が出ている');
  const off = await press(htmlOff);
  assert.deepEqual(off.calls, ['DELETE /api/provisional', 'GET /api/recommendations?debug=1&persist=1']);
  assert.deepEqual([off.selected, off.qty], [[], {}], 'モードなしは今までどおり先に消す');
});

await t('miniPC は FBA_SHEETLESS_IO=1 で入出力だけ止める (別のプロセス・env は別々): Sheet 同期 410・FNSKU は attrs だけ・起動時の backfill なし・計算の読み方は今のまま', async () => {
  const seed = async (tag, { mark = false } = {}) => {
    modeOff();
    const { dir, mod } = await otherDb(tag);
    await mod.initDb();
    mod.upsertSkuMappings([{ amazon_sku: 'Io-1', asin: 'B0IO1', ne_code: 'io1' }, { amazon_sku: 'Io-2', asin: 'B0IO2', ne_code: 'io2' }]);
    mod.updateFnskuBatch([{ sku: 'Io-1', fnsku: 'X0IOOLD' }]);
    if (mark) mod.runSkuMappingBackfillOnce();
    return dir;
  };
  const child = (dir, extraEnv) => {
    const code = [
      "import { pathToFileURL } from 'node:url';",
      "import path from 'node:path';",
      "import http from 'node:http';",
      "const root = process.env.T_ROOT;",
      "const imp = (p) => import(pathToFileURL(path.join(root, p)).href);",
      "const db = await imp('apps/fba-replenishment/db.js');",
      "await db.initDb();",
      "const out = {};",
      "try { db.upsertSkuMappings([{ amazon_sku: 'Io-9' }]); out.upsert = 'written'; } catch (e) { out.upsert = e.code; }",
      "db.updateFnskuBatch([{ sku: 'Io-1', fnsku: 'X0IONEW', asin: 'B0IONEW' }]);",
      "out.sheet = db.getSkuMappingsFromSheet().map((r) => [r.amazon_sku, r.fnsku]);",
      "out.attrs = db.getFbaSkuAttrs().map((a) => [a.amazon_sku, a.fnsku, a.asin]).sort();",
      "out.source = db.getSkuMappingSourceMode();",
      "out.mappings = db.getSkuMappings().map((m) => m.amazon_sku).sort();",
      "const express = (await imp('node_modules/express/index.js')).default;",
      "const svc = (await imp('apps/warehouse/fba-service.js')).default;",
      "const app = express(); app.use('/fba', express.json(), svc);",
      "const server = http.createServer(app); await new Promise((r) => server.listen(0, '127.0.0.1', r));",
      "const res = await fetch(`http://127.0.0.1:${server.address().port}/fba/sync-sku-mappings`, { method: 'POST' });",
      "out.syncStatus = res.status; out.syncBody = await res.json();",
      "const pl = await (await fetch(`http://127.0.0.1:${server.address().port}/fba/sync/latest-planning?fnsku_source=attrs`)).json();",
      "out.ready = pl.fnsku_ready; out.notReady = pl.fnsku_not_ready_reason || null; out.fnskuSource = pl.fnsku_source;",
      "server.close();",
      "console.log('@@' + JSON.stringify(out));",
      "process.exit(0);",
    ].join('\n');
    const env = { ...process.env, DATA_DIR: dir, T_ROOT: root, FBA_SKU_MAPPING_SOURCE: 'sheet', ...extraEnv };
    for (const k of ['FBA_SHEETLESS_MODE', 'FBA_SHEETLESS_IO', 'FBA_NONFBA_SOURCE']) if (!(k in extraEnv)) delete env[k];
    const r = spawnSync(process.execPath, ['--input-type=module', '-e', code], { cwd: root, env, encoding: 'utf8', windowsHide: true, timeout: 120000, maxBuffer: 16 * 1024 * 1024 });
    const line = String(r.stdout).split('\n').find((l) => l.startsWith('@@'));
    assert.ok(line, `子のプロセスが答えない: ${String(r.stderr).slice(-800)}`);
    return JSON.parse(line.slice(2));
  };
  modeOn();   // 親 (Render の役) はモードあり。子 (miniPC の役) は自分の env だけを見る
  const io = child(await seed('io-on'), { FBA_SHEETLESS_IO: '1' });
  assert.equal(io.upsert, 'FBA_SHEETLESS_GONE');
  assert.deepEqual(io.sheet, [['Io-1', 'X0IOOLD'], ['Io-2', null]], 'sku_mapping に FNSKU を書いた');
  assert.deepEqual(io.attrs, [['Io-1', 'X0IONEW', 'B0IONEW']], 'attrs だけに書く・起動時の backfill (Io-2) を流さない');
  assert.deepEqual([io.source, io.mappings], ['sheet', ['Io-1', 'Io-2']], '計算の読み方は今のまま (miniPC は計算しない)');
  assert.deepEqual([io.syncStatus, io.syncBody.error], [410, 'SHEETLESS_MODE']);
  // 移行の印が無い miniPC は fba_sku_attrs をまだ正にできない (Codex PR R2 Medium 1)
  assert.deepEqual([io.fnskuSource, io.ready], ['fba_sku_attrs', false]);
  assert.match(io.notReady, /一回限りの移行の印が無い/);
  const ready = child(await seed('io-mark', { mark: true }), { FBA_SHEETLESS_IO: '1' });
  assert.deepEqual([ready.fnskuSource, ready.ready, ready.notReady], ['fba_sku_attrs', true, null]);
  const plain = child(await seed('io-off'), {});
  assert.equal(plain.upsert, 'written');
  assert.deepEqual(plain.sheet.find((r) => r[0] === 'Io-1'), ['Io-1', 'X0IONEW'], 'IO なしの miniPC は今までどおり二重書き');
  assert.ok(plain.attrs.some((a) => a[0] === 'Io-2'), 'IO なしの miniPC は今までどおり起動時の backfill');
  assert.equal(plain.syncStatus, 500);
  modeOff();
});

await t('古い miniPC: モードありで fnsku_source の印が無ければ、FNSKU が 0 件・欄が無いときも見送りの理由を返す', async () => {
  modeOn();
  const pull = (o) => (u) => (u.includes('/sync/latest-planning') ? { ok: true, snapshot_date: '2026-09-30', rows: [{ amazon_sku: 'Alpha-1', product_name: 'A', units_sold_30d: 60 }], restock_rows: [], planning_latest_rows: [], ...o } : { ok: false });
  miniHandler = pull({ fnskus: [] });
  assert.match((await routerMod.syncLatestPlanningFromMiniPC()).fnsku_skip_reason, /fba_sku_attrs からではない/);
  miniHandler = pull({});
  assert.match((await routerMod.syncLatestPlanningFromMiniPC()).fnsku_skip_reason, /fba_sku_attrs からではない/);
  miniHandler = pull({ fnskus: [], fnsku_source: 'fba_sku_attrs', fnsku_ready: true });
  assert.equal('fnsku_skip_reason' in (await routerMod.syncLatestPlanningFromMiniPC()), false);
  // 移行の印が無い miniPC (fnsku_ready: false) も見送る (Codex PR R2 Medium 1)
  miniHandler = pull({ fnskus: [{ sku: 'Alpha-1', fnsku: 'X0NOTREADY' }], fnsku_source: 'fba_sku_attrs', fnsku_ready: false, fnsku_not_ready_reason: 'miniPC の fba.db に一回限りの移行の印が無い' });
  const nr = await routerMod.syncLatestPlanningFromMiniPC();
  assert.match(nr.fnsku_skip_reason, /まだ正にできない.*一回限りの移行の印が無い/);
  assert.notEqual(attrsOf('Alpha-1')?.fnsku, 'X0NOTREADY');
  modeOff();
  miniHandler = pull({ fnskus: [] });
  assert.equal('fnsku_skip_reason' in (await routerMod.syncLatestPlanningFromMiniPC()), false, 'モードなしは今までどおり');
});

await t('移行のスクリプト: 空の sku_mapping・表が無い・違う DB には印を付けない (fba.db に触らない)', async () => {
  modeOff();
  const make = (tag, sqls) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), `fba-sheetless-${tag}-`));
    const f = new Database(path.join(dir, 'fba.db'));
    try { for (const s of sqls) f.exec(s); } finally { f.close(); }
    return dir;
  };
  const cases = [
    ['empty', ['CREATE TABLE sku_mapping (id INTEGER PRIMARY KEY, amazon_sku TEXT UNIQUE, asin TEXT, fnsku TEXT)', 'CREATE TABLE fba_sku_attrs (amazon_sku TEXT PRIMARY KEY, asin TEXT, fnsku TEXT, source TEXT, updated_at TEXT)'], /sku_mapping が 0 行しかない/],
    ['notable', ['CREATE TABLE fba_sku_attrs (amazon_sku TEXT PRIMARY KEY, asin TEXT, fnsku TEXT, source TEXT, updated_at TEXT)'], /表 sku_mapping が無い/],
    ['wrongdb', ['CREATE TABLE mirror_products (code TEXT)', "INSERT INTO mirror_products VALUES ('x')"], /FBA 補充の fba\.db ではない/],
  ];
  for (const [tag, sqls, re] of cases) {
    const dir = make(tag, sqls);
    const file = path.join(dir, 'fba.db');
    const st = fs.statSync(file);
    await tick();
    const r = cli(['--min-rows', '1'], null, { dir });
    assert.equal(r.code, 1, `${tag}: ${r.out}`);
    assert.match(r.out, re, tag);
    const st2 = fs.statSync(file);
    assert.deepEqual([st2.mtimeMs, st2.size], [st.mtimeMs, st.size], `${tag}: 断ったのに fba.db を書いた`);
  }
});
await t('一回限りの移行: 空の sku_mapping は断る・流した後に attrs に無い SKU が残れば巻き戻して印を付けない', async () => {
  modeOff();
  const { mod } = await otherDb('verify');
  await mod.initDb();
  assert.throws(() => mod.runSkuMappingBackfillOnce(), (e) => e.code === 'FBA_BACKFILL_SOURCE_EMPTY');
  mod.upsertSkuMappings([{ amazon_sku: 'Ver-1', asin: 'B0V1' }, { amazon_sku: 'Ver-2', asin: 'B0V2' }]);
  mod._testHooks.afterBackfillInsert = (run) => run("DELETE FROM fba_sku_attrs WHERE amazon_sku = 'Ver-1'");
  try {
    assert.throws(() => mod.runSkuMappingBackfillOnce(), (e) => e.code === 'FBA_BACKFILL_VERIFY_FAILED');
  } finally { mod._testHooks.afterBackfillInsert = null; }
  assert.equal(mod.getBackfillMark(), null);
  assert.deepEqual(mod.getFbaSkuAttrs().filter((a) => a.amazon_sku.startsWith('Ver-')), [], '巻き戻していない');
  assert.throws(() => mod.runSkuMappingBackfillOnce({ minSkuMappingRows: 3 }), (e) => e.code === 'FBA_BACKFILL_SOURCE_EMPTY');
  const ok = mod.runSkuMappingBackfillOnce();
  assert.deepEqual([ok.inserted, ok.missing_after], [2, 0]);
  assert.equal(mod.getBackfillMark().key, sheetless.BACKFILL_MARK_KEY);
});

await t('米国の画面 (モードあり): 商品管理リストが欠けたら配分を出さない (503・日本の画面と同じ理由)', async () => {
  modeOn();
  miniHandler = (u) => (u.includes('/us/reports/latest') ? { ok: true } : { ok: false });
  removePml();
  try {
    const r = await call('GET', '/us/api/allocation');
    assert.equal(r.status, 503);
    assert.equal(r.body.error, 'FBA_SHEETLESS_BLOCKED');
    assert.match(r.body.message, /商品管理リスト/);
  } finally { publishPml(); }
  const ok = await call('GET', '/us/api/allocation');
  assert.notEqual(ok.status, 503, JSON.stringify(ok.body).slice(0, 200));
  modeOff();
});

// =====================================================================================
console.log('⑪ Company DB の毎晩のロード (Codex PR R2 Medium 3)');
await t('凍結した fba.db (モードあり) では sku_mapping の値を使わない: fba_sheet_import の候補・JAN・Sheet だけの出品を作らない / 古い・空・無いでも同じ計画 / モードなしは今までどおり', async () => {
  const { buildPlanFromRender } = await imp('apps/company-db/load/sources.mjs');
  const NOW = new Date('2026-10-01T00:00:00Z');
  const planOf = () => JSON.parse(JSON.stringify(buildPlanFromRender({ dataDir, now: NOW })));
  const amazonOf = (p) => ({ listings: p.listings.filter((l) => l.mall === 'amazon'), obs: p.observations, frozen: p.sources.fba_sheet_frozen ?? null });
  // 今の Sheet の写しに、Sheet にだけある SKU と JAN を足す
  modeOff();
  db.upsertSkuMappings([{ amazon_sku: 'Sheetonly-8', asin: 'B0SHEET8', jan: '4901234567894', ne_code: 'alpha', logizard_code: 'alpha' }]);
  await db.initDb();   // 凍結の印 = 0
  const off = amazonOf(planOf());
  assert.equal(off.frozen, null);
  assert.ok(JSON.stringify(off).includes('fba_sheet_import'), 'モードなしは今までどおり Sheet の候補を作る');
  assert.ok(off.listings.some((l) => l.listingCode.toLowerCase() === 'sheetonly-8'), 'モードなしは今までどおり Sheet だけの出品を作る');
  modeOn();
  await db.initDb();   // 凍結の印 = 1
  const on0 = amazonOf(planOf());
  assert.equal(on0.frozen, true);
  assert.equal(JSON.stringify(on0).includes('fba_sheet_import'), false, 'モードありで Sheet の値を使った');
  assert.equal(on0.listings.some((l) => l.listingCode.toLowerCase() === 'sheetonly-8'), false, 'モードありで Sheet だけの出品を作った');
  for (const [name, sql] of [
    ['古い', "UPDATE sku_mapping SET asin = 'B0STALE', fnsku = 'X0STALE', jan = '4900000000000', ne_code = 'wrong'"],
    ['空', 'DELETE FROM sku_mapping'],
    ['無い', 'DROP TABLE sku_mapping'],
  ]) {
    await tick();
    const f = new Database(dbFile);
    try { f.exec(sql); } finally { f.close(); }
    assert.deepEqual(amazonOf(planOf()), on0, `sku_mapping が${name}ときに計画が変わった`);
  }
  await db.initDb();   // 外から書いたファイルを読み直す (表を戻す)
  modeOff();
});

server.close();
globalThis.fetch = realFetch;
console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
