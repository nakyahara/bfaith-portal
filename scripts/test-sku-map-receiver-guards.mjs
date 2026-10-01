/**
 * test-sku-map-receiver-guards.mjs — Amazon SKU の対の世代の受け口 (PR ⑦-0) の守り (仮レビューの直し)
 *
 * 固定する契約:
 *   [G1] 有効になる前: 今の送り手のマスタの部 (products・set_components・手数料・楽天 SKU・在庫の集計・材料の世代・SKU の対) は今までどおり
 *        (応答の形・log・材料の記録・各表の行・状態の行なし)
 *   [G2] 古い形の DB (構成の時刻の列・状態の表が無い・行はある) を開くと列と表を足す。行はそのまま・今までどおり動く
 *   [G3] 状態が読めない (表が無いのとは別の失敗) → 世代なしの対も 503 (有効かどうか分からないまま入れない)。対の無い部は今までどおり
 *   [G4] 初期化が途中で落ちた (同じ名前の古い trigger・列の足りない状態の表) → 確かめる口は 503 で capability を出さない・
 *        世代つきは 503 (何も書かない)・有効になる前の世代なしは今までどおり。直して再起動すれば戻る
 *   [G5] 世代つきに他の表・meta でないものが相乗り → 422 (何も書かない・有効にならない)
 *   [G6] 有効にする許し: env SKU_MAP_ACTIVATION_ALLOWED=1 と activate: true の両方がそろったときだけ有効にする (片方・'true' の文字などは 409・何も書かない)
 *   [G7] 有効になった後に今の送り手のマスタの部が届く = 部ごと 409 (products も入らない) = 間違えて有効にしたときに壊れるもの。対の無い部は入る
 *   [G8] 戻し (sku-map-state-reset.mjs): env が残っていれば断る・誰が / なぜ / 期待の世代とハッシュ が要る・見るだけは何も書かない・
 *        鍵を取った後に読み直す (見た後に送り手が世代を入れたら断る = その世代を消さない)・DB で落ちれば控えは aborted (Codex R1 Medium)・
 *        戻すと控え・記録 (消せない) が残り、有効でなくなり、今の送り手のマスタの部がまた入る
 *   [G9] SKU_MAP_REQUIRE_GENERATION=1: 状態の行が無くても (表が無くても) 世代なしの対を断る。世代つきは許しがそろえば有効にして記録する
 *   [G10] 時間 (20k 親 / 40k 構成): 確かめ + ハッシュ・入れる・replayed の時間を出す (上限はゆるく = 落ちにくい)
 *   [G11] バックアップから戻したとき: 状態の行が無い → 許しを一時的に置いて max より大きい世代で有効にし直す (手順書どおり)。
 *         古い行が残る → 大きい世代は許しなしで入る (Codex R1 Low)
 * 使い方: node scripts/test-sku-map-receiver-guards.mjs
 */
import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'skumap-guard-');

import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';

process.env.MIRROR_SYNC_KEY = 'test-key';
delete process.env.ALLOW_INSECURE_MIRROR_SYNC;
delete process.env.SKU_MAP_ACTIVATION_ALLOWED;
delete process.env.SKU_MAP_REQUIRE_GENERATION;

const { toMirrorWireRows, buildSkuMapGeneration } = await import('../lib/sku-map-canonical.js');
const { buildMaterialGeneration } = await import('../apps/warehouse/material-lineage.js');
const gen = await import('../apps/warehouse-mirror/sku-map-generation.js');
const { resetSkuMapState, SKU_MAP_RESET_AUDIT_DDL } = await import('../apps/warehouse-mirror/sku-map-state-reset.mjs');
const Database = (await import('better-sqlite3')).default;
const express = (await import('express')).default;
const mirrorRouter = (await import('../apps/warehouse-mirror/router.js')).default;
const mirrorDb = await import('../apps/warehouse-mirror/db.js');
const { getMirrorDB, initMirrorDB } = mirrorDb;

const app = express();
app.use(express.json({ limit: '20mb' }));
app.use('/', mirrorRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;
for (let i = 0; i < 200; i++) { try { getMirrorDB(); break; } catch { await new Promise((r) => setTimeout(r, 20)); } }
for (let i = 0; i < 200; i++) {
  const r = await fetch(`${base}/api/sync/sku-map/state`, { headers: { 'x-sync-key': 'test-key' } }).catch(() => null);
  if (r && r.status !== 503) break;
  await new Promise((res) => setTimeout(res, 20));
}
let db = getMirrorDB();
const DATA_DIR = process.env.DATA_DIR;

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const post = async (body) => {
  const res = await fetch(`${base}/api/sync`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': 'test-key' }, body: JSON.stringify(body) });
  return { status: res.status, json: await res.json().catch(() => null) };
};
const getState = async () => {
  const res = await fetch(`${base}/api/sync/sku-map/state`, { headers: { 'x-sync-key': 'test-key' } });
  return { status: res.status, json: await res.json().catch(() => null) };
};
const withEnv = async (vars, fn) => {
  const before = Object.fromEntries(Object.keys(vars).map((k) => [k, process.env[k]]));
  for (const [k, v] of Object.entries(vars)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  try { return await fn(); } finally { for (const [k, v] of Object.entries(before)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; } }
};
const reopen = () => { initMirrorDB(); db = getMirrorDB(); installWriteCounter(); };

// ── 書き込みの数 (「何も書かない」の確かめ) ──
const WATCHED = ['mirror_sku_master', 'mirror_sku_resolved', 'mirror_products', 'mirror_set_components', 'mirror_amazon_sku_fees',
  'mirror_rakuten_sku_map', 'mirror_inv_daily_summary', 'mirror_material_generations', 'mirror_sync_status'];
function installWriteCounter() {
  db.exec('CREATE TABLE IF NOT EXISTS test_writes (tbl TEXT NOT NULL)');
  for (const t of WATCHED) {
    for (const op of ['INSERT', 'UPDATE', 'DELETE']) {
      db.exec(`CREATE TRIGGER IF NOT EXISTS test_w_${t}_${op} AFTER ${op} ON ${t} BEGIN INSERT INTO test_writes (tbl) VALUES ('${t}'); END`);
    }
  }
}
installWriteCounter();
const writes = () => Object.fromEntries(db.prepare('SELECT tbl, count(*) AS n FROM test_writes GROUP BY tbl').all().map((r) => [r.tbl, r.n]));
const writesSince = (before) => { const now = writes(); const d = {}; for (const t of WATCHED) { const n = (now[t] || 0) - (before[t] || 0); if (n) d[t] = n; } return d; };
const stateRow = () => { try { return db.prepare('SELECT * FROM mirror_sku_map_state').safeIntegers(true).get(); } catch { return 'no table'; } };
const rows = (t, order) => db.prepare(`SELECT * FROM ${t} ORDER BY ${order}`).all();

// ── 材料: 今の送り手 (sync-to-render.js) のマスタの部と同じ形 ──
const T1 = '2026-05-01T00:00:00.000Z', T2 = '2026-09-30T12:34:56.789Z';
const CANON = {
  master: [
    { seller_sku: 'pr_a-001', name: 'セット A', created_at: T1, updated_at: T2 },
    { seller_sku: 'b-002', name: '単品 B', created_at: T1, updated_at: T1 },
    { seller_sku: 'hakkap100', name: 'FBM と同じ', created_at: T2, updated_at: T2 },
  ],
  components: [
    { seller_sku: 'pr_a-001', ne_code: 'ne-001', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 },
    { seller_sku: 'pr_a-001', ne_code: 'ne-002', quantity: 2, sort_order: 1, created_at: T1, updated_at: T2 },
    { seller_sku: 'b-002', ne_code: 'b-002', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 },
    { seller_sku: 'hakkap100', ne_code: 'hakkap100', quantity: 1, sort_order: 0, created_at: T2, updated_at: T2 },
  ],
};
const legacyPair = (canon) => {
  const w = toMirrorWireRows(canon);
  return { sku_master: w.sku_master, sku_resolved: w.sku_resolved.map(({ component_created_at, component_updated_at, ...r }) => r) };
};
const PRODUCTS = [
  { product_id: 1, 商品コード: 'ne-001', 商品名: 'NE 1', 商品区分: '単品', 取扱区分: '取扱中', 標準売価: 1980, 原価: 500, 原価ソース: 'ne', 原価状態: 'COMPLETE', 送料: 0, 送料コード: 'M', 配送方法: 'メール便', 消費税率: 10, 税区分: '課税', 在庫数: 10, 引当数: 1, 仕入先コード: 'S1', セット構成品数: 0, 売上分類: 'A', 代表商品コード: null },
  { product_id: 2, 商品コード: 'ne-002', 商品名: 'NE 2', 商品区分: '単品', 取扱区分: '取扱中', 標準売価: 980, 原価: 200, 原価ソース: 'ne', 原価状態: 'COMPLETE', 送料: 0, 送料コード: 'M', 配送方法: 'メール便', 消費税率: 8, 税区分: '課税', 在庫数: 0, 引当数: 0, 仕入先コード: 'S2', セット構成品数: 0, 売上分類: 'B', 代表商品コード: 'ne-001', seasonality_flag: 1, season_months: '6,7' },
  { product_id: 3, 商品コード: 'set-1', 商品名: 'セット', 商品区分: 'セット', 取扱区分: '取扱中', 標準売価: 2980, 原価: 900, 原価ソース: 'set', 原価状態: 'COMPLETE', 送料: 0, 送料コード: 'T', 配送方法: '宅急便', 消費税率: 10, 税区分: '課税', 在庫数: 0, 引当数: 0, 仕入先コード: null, セット構成品数: 2 },
];
const SET_COMPONENTS = [
  { セット商品コード: 'set-1', 構成商品コード: 'ne-001', 数量: 1, 構成商品名: 'NE 1', 構成商品原価: 500 },
  { セット商品コード: 'set-1', 構成商品コード: 'ne-002', 数量: 2, 構成商品名: 'NE 2', 構成商品原価: 200 },
];
const FEES = [
  { seller_sku: 'pr_a-001', asin: 'B000000001', fulfillment_channel: 'FBA', referral_fee: 150, referral_fee_rate: 0.1, fba_fee: 300, variable_closing_fee: 0, per_item_fee: 0, total_fee: 450, price_used: 1500, fetched_at: T2 },
  { seller_sku: 'hakkap100', asin: 'B000000002', fulfillment_channel: 'FBM', referral_fee: 98, referral_fee_rate: 0.1, fba_fee: 0, variable_closing_fee: 0, per_item_fee: 0, total_fee: 98, price_used: 980, fetched_at: T2 },
];
const RAKUTEN = [
  { rakuten_code: 'am-ne-001', ne_code: 'ne-001', source: 'auto', manage_number: 'm1', updated_at: T2 },
  { rakuten_code: 'w-ne-002', ne_code: 'ne-002', source: 'manual', updated_at: T2 },
];
const INV_SUMMARY = [
  { business_date: '2026-09-30', market: 'jp', category: 'own_warehouse', total_qty: 10, total_value: 5000, resolved_count: 2, unresolved_count: 0, cost_missing_count: 0, source_status: 'ok', source_row_count: 2, captured_at: T2 },
  { business_date: '2026-09-30', category: 'fba_warehouse', total_qty: 3, source_status: 'ok' },
];
/** 今の送り手のマスタの部 (SKU の対は世代なし) */
const masterPart = () => {
  const material_generation = buildMaterialGeneration({ products: PRODUCTS, set_components: SET_COMPONENTS, now: new Date(Date.UTC(2026, 8, 30, 22, 0, 0, 123)) });
  return { products: PRODUCTS, set_components: SET_COMPONENTS, amazon_sku_fees: FEES, rakuten_sku_map: RAKUTEN, inv_daily_summary: INV_SUMMARY, material_generation, ...legacyPair(CANON) };
};
const genBody = (generation, canon, { activate = false, extra = {} } = {}) => ({
  ...toMirrorWireRows(canon),
  sku_map_generation: { ...buildSkuMapGeneration({ generation, ...canon }), ...(activate ? { activate: true } : {}) },
  ...extra,
});

await ta('[G1] 有効になる前: 今の送り手のマスタの部は今までどおり (応答・log・材料の記録・各表の行)', async () => {
  const body = masterPart();
  const r = await post(body);
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.deepEqual(Object.keys(r.json).sort(), ['log', 'material_recorded', 'ok', 'synced_at']);
  assert.deepEqual(r.json.log, ['products: 3件', 'set_components: 2件', 'sku_resolved: 4件', 'sku_master: 3件', 'inv_daily_summary: 2件', 'rakuten_sku_map: 2件', 'amazon_sku_fees: 2件']);
  const mg = body.material_generation;
  assert.deepEqual(r.json.material_recorded, {
    products: { recorded: true, generation_id: mg.generation_id, content_hash: mg.products.content_hash, row_count: 3 },
    set_components: { recorded: true, generation_id: mg.generation_id, content_hash: mg.set_components.content_hash, row_count: 2 },
  });
  const now = r.json.synced_at;
  assert.deepEqual(rows('mirror_sku_master', 'seller_sku'), body.sku_master.map((m) => ({ ...m, synced_at: now })).sort((a, b) => (a.seller_sku < b.seller_sku ? -1 : 1)));
  const byKey = (a, b) => (a.seller_sku === b.seller_sku ? (a.ne_code < b.ne_code ? -1 : 1) : (a.seller_sku < b.seller_sku ? -1 : 1));
  assert.deepEqual(rows('mirror_sku_resolved', 'seller_sku, ne_code'),
    body.sku_resolved.map((c) => ({ ...c, synced_at: now, component_created_at: null, component_updated_at: null })).sort(byKey));
  assert.equal(rows('mirror_products', 'product_id').length, 3);
  assert.equal(rows('mirror_set_components', 'セット商品コード, 構成商品コード').length, 2);
  assert.equal(rows('mirror_amazon_sku_fees', 'seller_sku').length, 2);
  assert.equal(rows('mirror_rakuten_sku_map', 'rakuten_code').length, 2);
  assert.equal(rows('mirror_inv_daily_summary', 'category').length, 2);
  assert.equal(rows('mirror_material_generations', 'entity').length, 2);
  assert.equal(db.prepare("SELECT value FROM mirror_sync_status WHERE key = 'last_sync'").get().value, now);
  assert.equal(stateRow(), undefined);
});

await ta('[G2] 古い形の DB (列・状態の表が無い・行はある) を開くと足す。行はそのまま・今までどおり', async () => {
  const cols = () => db.prepare('PRAGMA table_info(mirror_sku_resolved)').all().map((c) => c.name);
  db.exec('DROP TABLE mirror_sku_map_state');
  db.exec('ALTER TABLE mirror_sku_resolved DROP COLUMN component_created_at');
  db.exec('ALTER TABLE mirror_sku_resolved DROP COLUMN component_updated_at');
  assert.ok(!cols().includes('component_created_at'));
  const before = { master: rows('mirror_sku_master', 'seller_sku'), resolved: rows('mirror_sku_resolved', 'seller_sku, ne_code') };
  assert.equal(before.resolved.length, 4);
  reopen();
  assert.equal(mirrorDb.skuMapGenerationInitError, null);
  assert.ok(cols().includes('component_created_at') && cols().includes('component_updated_at'));
  assert.deepEqual(rows('mirror_sku_master', 'seller_sku'), before.master);
  assert.deepEqual(rows('mirror_sku_resolved', 'seller_sku, ne_code'), before.resolved.map((x) => ({ ...x, component_created_at: null, component_updated_at: null })));
  assert.equal(stateRow(), undefined);   // 表はできた・行は無い
  const s = await getState();
  assert.equal(s.status, 200); assert.deepEqual(s.json.state, { activated: false });
  const r = await post(masterPart());
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.equal(r.json.sku_map, undefined);
});

await ta('[G3] 状態が読めない (表が無いのとは別) → 世代なしの対も 503・何も書かない。対の無い部は今までどおり', async () => {
  const w0 = writes();
  db.prepare = function prepare(sql) {
    if (/FROM mirror_sku_map_state/.test(sql)) throw Object.assign(new Error('disk I/O error (試験)'), { code: 'SQLITE_IOERR' });
    return Object.getPrototypeOf(this).prepare.call(this, sql);
  };
  try {
    let r = await post(masterPart());
    assert.equal(r.status, 503, JSON.stringify(r.json));
    assert.equal(r.json.error, 'sku_map_state_unavailable');
    assert.equal(r.json.sku_map.capability, undefined);
    assert.deepEqual(writesSince(w0), {});   // products も材料の記録も書いていない
    r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, () => post(genBody(5, CANON, { activate: true })));
    assert.equal(r.status, 503); assert.equal(r.json.error, 'sku_map_state_unavailable');
    const s = await getState();
    assert.equal(s.status, 503); assert.equal(s.json.capability, undefined);
    assert.deepEqual(writesSince(w0), {});
    // 対の無い部 (出荷サマリ・月次など) は状態を読まない = 今までどおり
    r = await post({ products: PRODUCTS });
    assert.equal(r.status, 200, JSON.stringify(r.json));
  } finally { delete db.prepare; }
  assert.equal((await getState()).status, 200);
});

await ta('[G4] 初期化が途中で落ちた → 確かめる口 503 (capability なし)・世代つき 503・世代なしは今までどおり。直せば戻る', async () => {
  // (a) 同じ名前で定義の違う trigger (IF NOT EXISTS では直らない)
  db.exec('DROP TRIGGER trg_sku_map_state_single_v1');
  db.exec('CREATE TRIGGER trg_sku_map_state_single_v1 BEFORE INSERT ON mirror_sku_map_state WHEN 0 BEGIN SELECT 1; END');
  reopen();
  assert.match(mirrorDb.skuMapGenerationInitError?.message || '', /trg_sku_map_state_single_v1 の定義が期待と違う/);
  let s = await getState();
  assert.equal(s.status, 503, JSON.stringify(s.json));
  assert.equal(s.json.error, 'sku_map_init_failed');
  assert.equal(s.json.capability, undefined);
  const w0 = writes();
  let r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, () => post(genBody(5, CANON, { activate: true })));
  assert.equal(r.status, 503, JSON.stringify(r.json));
  assert.equal(r.json.error, 'sku_map_init_failed');
  assert.equal(r.json.sku_map.capability, undefined);
  assert.deepEqual(writesSince(w0), {});
  assert.equal(stateRow(), undefined);
  r = await post(masterPart());   // 有効になる前の世代なしは今までどおり
  assert.equal(r.status, 200, JSON.stringify(r.json));
  // (b) 列の足りない状態の表 (CREATE TABLE IF NOT EXISTS では直らない)
  db.exec('DROP TABLE mirror_sku_map_state');
  db.exec('CREATE TABLE mirror_sku_map_state (id INTEGER PRIMARY KEY, activated INTEGER)');
  reopen();
  assert.match(mirrorDb.skuMapGenerationInitError?.message || '', /列が無い/);
  assert.equal((await getState()).status, 503);
  // 直して再起動すれば戻る
  db.exec('DROP TABLE mirror_sku_map_state');
  reopen();
  assert.equal(mirrorDb.skuMapGenerationInitError, null);
  s = await getState();
  assert.equal(s.status, 200); assert.deepEqual(s.json.capability, gen.SKU_MAP_RECEIVER_CAPABILITY);
});

await ta('[G5] 世代つきに他の表・meta でないものが相乗り → 422 (何も書かない・有効にならない)', async () => {
  const w0 = writes();
  const others = {
    products: PRODUCTS, set_components: SET_COMPONENTS, material_generation: masterPart().material_generation, amazon_sku_fees: FEES,
    rakuten_sku_map: RAKUTEN, inv_daily_summary: INV_SUMMARY, shipments_daily: [], sales_daily: [], unknown_key: 1,
  };
  await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, async () => {
    for (const [k, v] of Object.entries(others)) {
      const r = await post(genBody(5, CANON, { activate: true, extra: { [k]: v } }));
      assert.equal(r.status, 422, `${k}: ${JSON.stringify(r.json)}`);
      assert.equal(r.json.error, 'sku_map_body_not_standalone', k);
      assert.deepEqual(r.json.sku_map.extra_keys, [k]);
    }
    for (const meta of ['x', 1, ['a'], true]) {
      const r = await post(genBody(5, CANON, { activate: true, extra: { meta } }));
      assert.equal(r.status, 422, JSON.stringify(meta)); assert.equal(r.json.error, 'sku_map_body_not_standalone');
    }
    // logizard_stock は前からの守り (単独限定) が先に断る
    const r = await post(genBody(5, CANON, { activate: true, extra: { logizard_stock: { rows: [] } } }));
    assert.equal(r.status, 400);
  });
  assert.deepEqual(writesSince(w0), {});
  assert.equal(stateRow(), undefined);
});

await ta('[G6] 有効にする許し: env =1 と activate: true の両方がそろったときだけ有効にする', async () => {
  const w0 = writes();
  const cases = [
    [{}, false, ['env SKU_MAP_ACTIVATION_ALLOWED=1 (Render)', 'sku_map_generation.activate = true (送り手)']],
    [{}, true, ['env SKU_MAP_ACTIVATION_ALLOWED=1 (Render)']],
    [{ SKU_MAP_ACTIVATION_ALLOWED: '1' }, false, ['sku_map_generation.activate = true (送り手)']],
    [{ SKU_MAP_ACTIVATION_ALLOWED: 'true' }, true, ['env SKU_MAP_ACTIVATION_ALLOWED=1 (Render)']],
    [{ SKU_MAP_ACTIVATION_ALLOWED: '0' }, true, ['env SKU_MAP_ACTIVATION_ALLOWED=1 (Render)']],
  ];
  for (const [env, activate, missing] of cases) {
    const r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: undefined, ...env }, () => post(genBody(5, CANON, { activate })));
    assert.equal(r.status, 409, `${JSON.stringify(env)} ${activate}: ${JSON.stringify(r.json)}`);
    assert.equal(r.json.error, 'sku_map_activation_not_allowed');
    assert.equal(r.json.sku_map.validated, true);   // 形・ハッシュは確かめ済み (この 409 は「受けられる形だったが有効にしていない」)
    assert.deepEqual(r.json.sku_map.missing, missing);
  }
  // activate が true でない値 ('true' の文字・1) も許しにならない
  for (const bad of ['true', 1]) {
    const body = genBody(5, CANON); body.sku_map_generation.activate = bad;
    const r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, () => post(body));
    assert.equal(r.status, 409, JSON.stringify(bad)); assert.equal(r.json.error, 'sku_map_activation_not_allowed');
  }
  // 形がおかしければ許しより先に 422 (許しの有無で答えが変わらない)
  const badHash = genBody(5, CANON); badHash.sku_map_generation.content_hash = 'e'.repeat(64);
  assert.equal((await post(badHash)).json.error, 'sku_map_hash_mismatch');
  assert.deepEqual(writesSince(w0), {});
  assert.equal(stateRow(), undefined);
  assert.deepEqual((await getState()).json.state, { activated: false });
  // 両方そろう → 有効
  const r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, () => post(genBody(5, CANON, { activate: true })));
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.equal(r.json.sku_map.result, 'activated');
  assert.equal(stateRow().generation, 5n);
  // 有効になった後は許しを見ない
  const r2 = await post(genBody(6, CANON));
  assert.equal(r2.status, 200, JSON.stringify(r2.json)); assert.equal(r2.json.sku_map.result, 'applied');
});

await ta('[G7] 有効になった後に今の送り手のマスタの部が届く = 部ごと 409 (products も入らない)。対の無い部は入る', async () => {
  const w0 = writes();
  const r = await post({ ...masterPart(), products: [{ ...PRODUCTS[0], 原価: 999 }] });
  assert.equal(r.status, 409, JSON.stringify(r.json));
  assert.equal(r.json.error, 'sku_map_generation_required');
  assert.deepEqual(writesSince(w0), {});
  assert.equal(db.prepare('SELECT 原価 FROM mirror_products WHERE product_id = 1').get().原価, 500);
  // 対を外した部なら今までどおり入る (⑦-2 は対をマスタの部から外してから有効にする)
  const { sku_master, sku_resolved, ...noPair } = masterPart();
  const r2 = await post(noPair);
  assert.equal(r2.status, 200, JSON.stringify(r2.json));
  assert.equal(r2.json.material_recorded.products.recorded, true);
});

await ta('[G8] 戻し: env が残れば断る・誰が / なぜ / 期待の世代とハッシュが要る・見るだけは書かない・鍵の後に状態が変われば断る・DB で落ちれば控えは aborted・戻すと控えと記録が残り今までどおり', async () => {
  const backupDir = path.join(DATA_DIR, 'sku-map-state-resets');
  const st0 = stateRow();
  const code = (c) => (e) => e.code === c;
  const files = () => (fs.existsSync(backupDir) ? fs.readdirSync(backupDir).sort() : []);
  const readJson = (f) => JSON.parse(fs.readFileSync(path.join(backupDir, f), 'utf8'));
  const auditRows = () => { try { return db.prepare('SELECT * FROM mirror_sku_map_state_resets ORDER BY id').all(); } catch { return []; } };
  const h6 = st0.content_hash;
  const ok6 = { apply: true, by: 'x', reason: 'y', expectGeneration: '6', expectContentHash: h6, backupDir };
  assert.throws(() => resetSkuMapState(db, { ...ok6, env: { SKU_MAP_ACTIVATION_ALLOWED: '1' } }), code('RESET_ENV_STILL_SET'));
  assert.throws(() => resetSkuMapState(db, { ...ok6, env: { SKU_MAP_REQUIRE_GENERATION: '1' } }), code('RESET_ENV_STILL_SET'));
  assert.throws(() => resetSkuMapState(db, { ...ok6, by: ' ', env: {} }), code('RESET_NEEDS_BY_REASON'));
  assert.throws(() => resetSkuMapState(db, { ...ok6, reason: undefined, env: {} }), code('RESET_NEEDS_BY_REASON'));
  // 期待の世代とハッシュは必須 (見るだけの回に出た値)
  assert.throws(() => resetSkuMapState(db, { ...ok6, expectGeneration: undefined, env: {} }), code('RESET_NEEDS_EXPECT'));
  assert.throws(() => resetSkuMapState(db, { ...ok6, expectContentHash: undefined, env: {} }), code('RESET_NEEDS_EXPECT'));
  assert.throws(() => resetSkuMapState(db, { ...ok6, expectGeneration: '06', env: {} }), code('RESET_NEEDS_EXPECT'));
  assert.throws(() => resetSkuMapState(db, { ...ok6, expectContentHash: h6.toUpperCase(), env: {} }), code('RESET_NEEDS_EXPECT'));
  const dry = resetSkuMapState(db, { backupDir, env: {} });
  assert.equal(dry.action, 'dry_run');
  assert.deepEqual([dry.before.generation, dry.before.content_hash], ['6', h6]);
  assert.deepEqual(stateRow(), st0);
  assert.deepEqual(files(), []);
  // 見た後・鍵を取る前に送り手が世代 7 を入れた (Codex R1 Medium) → 鍵の中で読み直して断る。世代 7 は消さない・控えも記録も残さない
  const CANON7 = { ...CANON, master: CANON.master.map((m) => (m.seller_sku === 'b-002' ? { ...m, name: '単品 B (7)' } : m)) };
  assert.throws(() => resetSkuMapState(db, {
    ...ok6, env: {}, now: new Date(Date.UTC(2026, 9, 1, 2, 0, 0)),
    beforeLockForTest: () => {
      const plan = gen.planSkuMapPair(db, genBody(7, CANON7), { env: {} });
      assert.equal(plan.mode, 'generation', plan.code);
      assert.equal(gen.applySkuMapGeneration(db, plan, { syncedAt: 'race' }).result, 'applied');
    },
  }), code('RESET_STATE_CHANGED'));
  const st7 = stateRow();
  assert.equal(st7.generation, 7n);
  assert.deepEqual(files(), []);
  assert.deepEqual(auditRows(), []);
  // 世代は合うがハッシュが違う / 世代が違う → 断る (何も書かない)
  assert.throws(() => resetSkuMapState(db, { ...ok6, expectGeneration: '7', env: {} }), code('RESET_STATE_CHANGED'));
  assert.throws(() => resetSkuMapState(db, { ...ok6, expectContentHash: st7.content_hash, env: {} }), code('RESET_STATE_CHANGED'));
  assert.deepEqual(files(), []);
  const ok7 = { ...ok6, expectGeneration: '7', expectContentHash: st7.content_hash, env: {} };
  // DB で落ちる (記録の表に入れられない) → 何も戻していない。控えは aborted (戻したように読めない)・記録は無い・有効のまま
  for (const sql of SKU_MAP_RESET_AUDIT_DDL) db.exec(sql);
  db.exec("CREATE TRIGGER test_fail_audit BEFORE INSERT ON mirror_sku_map_state_resets BEGIN SELECT RAISE(ABORT, 'test: audit fails'); END");
  try {
    assert.throws(() => resetSkuMapState(db, { ...ok7, now: new Date(Date.UTC(2026, 9, 1, 2, 30, 0)) }), /test: audit fails/);
  } finally { db.exec('DROP TRIGGER test_fail_audit'); }
  assert.equal(files().length, 1);
  const aborted = readJson(files()[0]);
  assert.deepEqual([aborted.status, aborted.state.generation], ['aborted', '7']);
  assert.match(aborted.error, /test: audit fails/);
  assert.deepEqual(auditRows(), []);
  assert.deepEqual(stateRow(), st7);
  // 控えの名前が重なる (同じ時刻) → 書けずに断る。前の回の控え (aborted) は書き換えない・何も戻さない
  assert.throws(() => resetSkuMapState(db, { ...ok7, now: new Date(Date.UTC(2026, 9, 1, 2, 30, 0)) }), (e) => e.code === 'EEXIST');
  assert.deepEqual(readJson(files()[0]), aborted);
  assert.deepEqual(auditRows(), []);
  assert.deepEqual(stateRow(), st7);
  // 戻す
  const out = resetSkuMapState(db, { ...ok7, by: '中原', reason: '試験: 影運転が activate: true で送った', now: new Date(Date.UTC(2026, 9, 1, 3, 0, 0)) });
  assert.equal(out.action, 'reset');
  const saved = JSON.parse(fs.readFileSync(out.backup_file, 'utf8'));
  assert.deepEqual([saved.status, saved.audit_id, saved.reset_by, saved.state.generation, saved.state.activation_generation, saved.state.content_hash, saved.tables.mirror_sku_master],
    ['committed', out.audit_id, '中原', '7', '5', st7.content_hash, 3]);
  assert.ok(saved.schema.some((x) => x.name === 'trg_sku_map_state_no_delete_v1'));
  const audit = auditRows();
  assert.equal(audit.length, 1);
  assert.deepEqual([audit[0].id, audit[0].reset_by, audit[0].reason, audit[0].backup_file, JSON.parse(audit[0].previous_state).generation],
    [out.audit_id, '中原', '試験: 影運転が activate: true で送った', out.backup_file, '7']);
  assert.throws(() => db.exec('DELETE FROM mirror_sku_map_state_resets'), /消せない/);
  assert.throws(() => db.exec("UPDATE mirror_sku_map_state_resets SET reason = 'x'"), /直せない/);
  // 有効でなくなった・trigger は作り直した・今の送り手のマスタの部がまた入る
  assert.equal(stateRow(), undefined);
  gen.verifySkuMapGenerationSchema(db);
  assert.deepEqual((await getState()).json.state, { activated: false });
  const r = await post(masterPart());
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.equal(r.json.sku_map, undefined);
  // 有効でなければ断る (先に誰かが戻した) / 見るだけは none
  assert.throws(() => resetSkuMapState(db, { ...ok7, now: new Date(Date.UTC(2026, 9, 1, 4, 0, 0)) }), code('RESET_STATE_CHANGED'));
  assert.equal(resetSkuMapState(db, { backupDir, env: {} }).action, 'none');
  assert.equal(files().length, 2);
});

await ta('[G9] SKU_MAP_REQUIRE_GENERATION=1: 行が無くても (表が無くても) 世代なしの対を断る。世代つきは許しがそろえば有効にする', async () => {
  const w0 = writes();
  await withEnv({ SKU_MAP_REQUIRE_GENERATION: '1' }, async () => {
    let r = await post(masterPart());
    assert.equal(r.status, 409, JSON.stringify(r.json));
    assert.equal(r.json.error, 'sku_map_generation_required');
    assert.equal(r.json.sku_map.require_generation, true);
    assert.deepEqual(r.json.sku_map.current, { activated: false });
    r = await post({ sku_master: [], sku_resolved: [], meta: { clear_sku_master: true, clear_sku_resolved: true } });
    assert.equal(r.status, 422); assert.equal(r.json.error, 'sku_map_clear_forbidden');
    // 表ごと無い (古いバックアップから戻した) ときも断る
    db.exec('DROP TABLE mirror_sku_map_state');
    r = await post(masterPart());
    assert.equal(r.status, 409, JSON.stringify(r.json));
    assert.deepEqual(writesSince(w0), {});
    r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, () => post(genBody(7, CANON, { activate: true })));
    assert.equal(r.status, 503); assert.equal(r.json.error, 'sku_map_state_unavailable');
  });
  // env が無ければ、表の無い DB は今までどおり (= この env が無いと DB を戻したときに黙って世代なしを受ける)
  assert.equal((await post(masterPart())).status, 200);
  reopen();
  await withEnv({ SKU_MAP_REQUIRE_GENERATION: '1' }, async () => {
    const w1 = writes();
    let r = await post(masterPart());
    assert.equal(r.status, 409);
    r = await post(genBody(7, CANON));
    assert.equal(r.status, 409); assert.equal(r.json.error, 'sku_map_activation_not_allowed');
    assert.deepEqual(writesSince(w1), {});
    // 対の無い部はこの env でも今までどおり
    const { sku_master, sku_resolved, ...noPair } = masterPart();
    assert.equal((await post(noPair)).status, 200);
    r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, () => post(genBody(7, CANON, { activate: true })));
    assert.equal(r.status, 200, JSON.stringify(r.json));
    assert.equal(r.json.sku_map.result, 'activated');
    assert.equal(stateRow().generation, 7n);
    assert.deepEqual((await getState()).json.receiver, { activation_allowed: false, require_generation: true });
  });
});

await ta('[G10] 時間 (20k 親 / 40k 構成): 確かめ + ハッシュ・入れる・replayed (上限はゆるく)', async () => {
  const T = '2026-05-01T00:00:00.000Z';
  const master = [], components = [];
  for (let i = 0; i < 20000; i++) {
    const sku = `${i % 7 === 0 ? 'pr_' : ''}sku-${String(i).padStart(6, '0')}${i % 97 === 0 ? String.fromCodePoint(0xff5e) : ''}${i % 89 === 0 ? String.fromCodePoint(0x1f600) : ''}`;
    master.push({ seller_sku: sku, name: `商品 ${i} セット`, created_at: T, updated_at: T });
    components.push({ seller_sku: sku, ne_code: `ne-${i}`, quantity: 1, sort_order: 0, created_at: T, updated_at: T });
    components.push({ seller_sku: sku, ne_code: `ne-${i}-b`, quantity: 2, sort_order: 1, created_at: T, updated_at: T });
  }
  master.reverse(); components.reverse();
  const body = { ...toMirrorWireRows({ master, components }), sku_map_generation: { ...buildSkuMapGeneration({ generation: 1, master, components }), activate: true } };
  const bdb = new Database(path.join(DATA_DIR, 'bench.db'));
  try {
    bdb.pragma('journal_mode = WAL');
    bdb.exec('CREATE TABLE mirror_sku_master (seller_sku TEXT PRIMARY KEY, 商品名 TEXT, source_created_at TEXT, source_updated_at TEXT, synced_at TEXT)');
    bdb.exec('CREATE TABLE mirror_sku_resolved (seller_sku TEXT, ne_code TEXT, quantity INTEGER, source TEXT, 商品名 TEXT, source_updated_at TEXT, sort_order INTEGER, synced_at TEXT, PRIMARY KEY (seller_sku, ne_code))');
    gen.createSkuMapGenerationTables(bdb);
    const ms = (fn) => { const s = process.hrtime.bigint(); const out = fn(); return [out, Number(process.hrtime.bigint() - s) / 1e6]; };
    const [plan, tPlan] = ms(() => gen.planSkuMapPair(bdb, body, { env: { SKU_MAP_ACTIVATION_ALLOWED: '1' } }));
    assert.equal(plan.mode, 'generation', `${plan.code}: ${plan.message}`);
    const [first, tApply] = ms(() => gen.applySkuMapGeneration(bdb, plan, { syncedAt: 'x' }));
    const [again, tReplay] = ms(() => gen.applySkuMapGeneration(bdb, plan, { syncedAt: 'x' }));
    assert.deepEqual([first.result, again.result, first.master_rows, first.component_rows], ['activated', 'replayed', 20000, 40000]);
    const mb = (Buffer.byteLength(JSON.stringify(body)) / 1048576).toFixed(1);
    console.log(`      時間: 確かめ + ハッシュ ${tPlan.toFixed(0)}ms / 入れる ${tApply.toFixed(0)}ms / replayed ${tReplay.toFixed(0)}ms (body ${mb}MB。受け口の上限は 12MB)`);
    assert.ok(tPlan + tApply + tReplay < 60000, '1 分を超えた (ゆるい上限)');
  } finally { bdb.close(); }
});

await ta('[G11] バックアップから戻したとき: 状態の行が無い → 許しを一時的に置いて max より大きい世代で有効にし直す / 古い行が残る → 大きい世代はそのまま入る', async () => {
  await withEnv({ SKU_MAP_REQUIRE_GENERATION: '1' }, async () => {
    // [G9] の後 = 世代 7 で有効。Render のディスクを「有効にする前」のバックアップから戻した = 状態の表は空
    assert.equal(stateRow().generation, 7n);
    db.exec('DROP TABLE mirror_sku_map_state');
    reopen();
    assert.equal(stateRow(), undefined);
    let s = await getState();
    assert.deepEqual([s.status, s.json.state, s.json.receiver], [200, { activated: false }, { activation_allowed: false, require_generation: true }]);
    const w0 = writes();
    // 世代なしは REQUIRE_GENERATION が断る・世代つきも許しが無ければ断る (何も書かない)
    let r = await post(masterPart());
    assert.deepEqual([r.status, r.json.error], [409, 'sku_map_generation_required']);
    r = await post(genBody(8, CANON, { activate: true }));
    assert.deepEqual([r.status, r.json.error, r.json.sku_map.missing], [409, 'sku_map_activation_not_allowed', ['env SKU_MAP_ACTIVATION_ALLOWED=1 (Render)']]);
    assert.deepEqual(writesSince(w0), {});
    // 手順: 送り手を止める → max(PG, miniPC, Render) = 7 より大きい 8 を決める → 許しを一時的に置く → activate: true の単独の POST → 許しを消す
    r = await withEnv({ SKU_MAP_ACTIVATION_ALLOWED: '1' }, () => post(genBody(8, CANON, { activate: true })));
    assert.equal(r.status, 200, JSON.stringify(r.json));
    assert.deepEqual([r.json.sku_map.result, r.json.sku_map.generation], ['activated', '8']);
    assert.equal(stateRow().activation_generation, 8n);
    s = await getState();
    assert.deepEqual([s.json.state.generation, s.json.receiver], ['8', { activation_allowed: false, require_generation: true }]);
    // 許しを消した後: 次の世代は入る・同じ世代は replayed・古い世代は断る
    assert.equal((await post(genBody(9, CANON))).json.sku_map.result, 'applied');
    assert.equal((await post(genBody(9, CANON))).json.sku_map.result, 'replayed');
    r = await post(genBody(8, CANON));
    assert.deepEqual([r.status, r.json.error], [409, 'sku_map_generation_stale']);
    // 古い状態の行が残るバックアップ (世代 5) から戻した → 許しは要らず、大きい世代 (10) はそのまま入る。有効の時刻と最初の世代はバックアップのまま
    db.exec('DROP TABLE mirror_sku_map_state');
    reopen();
    const g5 = buildSkuMapGeneration({ generation: 5, ...CANON });
    db.prepare(`INSERT INTO mirror_sku_map_state (id, activated, activated_at, activation_generation, generation, format, content_hash, master_rows, component_rows, applied_at)
      VALUES (1, 1, '2026-09-01T00:00:00.000Z', 5, 5, ?, ?, ?, ?, '2026-09-01T00:00:00.000Z')`).run(g5.format, g5.content_hash, g5.master_rows, g5.component_rows);
    r = await post(genBody(10, CANON));
    assert.equal(r.status, 200, JSON.stringify(r.json));
    assert.deepEqual([r.json.sku_map.result, r.json.sku_map.activated_at], ['applied', '2026-09-01T00:00:00.000Z']);
    assert.deepEqual([stateRow().generation, stateRow().activation_generation], [10n, 5n]);
  });
});

server.close();
console.log(`\n${passed} 件 PASS`);
