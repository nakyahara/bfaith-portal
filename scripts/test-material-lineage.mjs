/**
 * test-material-lineage.mjs — 夜間ロードの「材料」の世代を残す (Company DB構想 10 §6 / PR ③a-1)
 *
 * 固定する契約:
 *   1 中身のハッシュは行の並び・オブジェクトの鍵の順番に依らず、値が変われば変わる
 *   2 控え (DATA_DIR/cdb-material/<世代>.json.gz) は新しい keep 個だけ残り、読むときにハッシュを確かめる (壊れていれば MATERIAL_HASH_MISMATCH)
 *   3 Render の受け手 (/api/sync) は products / set_components を入れ替えたのと同じ取引で世代を記録する。
 *     行数が世代と合わない・形がおかしい・世代が無い (古い送り手) ときは記録しないが、写しの入れ替えは今までどおり
 *   4 夜間ロードは mirror の世代を ops.load_materials に残す。世代が無い材料は generation_id = null の行。0028 が未適用でも失敗しない
 * 使い方: node scripts/test-material-lineage.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import http from 'node:http';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mat-lineage-'));
process.env.DATA_DIR = tmp;
process.env.MIRROR_SYNC_KEY = 'test-key';

const { contentHash, buildMaterialGeneration, saveMaterialSnapshot, readMaterialSnapshot, MATERIAL_DIR_NAME } = await import('../apps/warehouse/material-lineage.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

const P = [{ 商品コード: 'a001', 商品名: 'A', 原価: 100 }, { 商品コード: 'b002', 商品名: 'B', 原価: 200 }];
const S = [{ セット商品コード: 'set1', 構成商品コード: 'a001', 数量: 2 }];

await ta('[1] 中身のハッシュは並び・鍵の順番に依らず、値が変われば変わる', async () => {
  const h = contentHash(P);
  assert.equal(contentHash([P[1], P[0]]), h);
  assert.equal(contentHash([{ 原価: 100, 商品名: 'A', 商品コード: 'a001' }, P[1]]), h);
  assert.notEqual(contentHash([{ ...P[0], 原価: 101 }, P[1]]), h);
  assert.notEqual(contentHash([{ ...P[0], 原価: null }, P[1]]), h);   // 空にしても変わる
  assert.notEqual(contentHash([P[0]]), h);                            // 行が減っても変わる
  const g = buildMaterialGeneration({ products: P, set_components: S, neProductsCompleteAt: '2026-09-25 07:05:00', now: new Date('2026-09-25T00:10:11.123Z') });
  assert.match(g.generation_id, /^mat_20260925T001011Z_[0-9a-f]{8}$/);
  assert.equal(g.products.row_count, 2); assert.equal(g.set_components.row_count, 1);
  assert.equal(g.products.source_complete_at, '2026-09-25 07:05:00');
});

await ta('[2] 控えは新しい keep 個だけ残り、読むときにハッシュを確かめる', async () => {
  const gens = [];
  for (let i = 0; i < 5; i++) {
    const g = buildMaterialGeneration({ products: P, set_components: S, now: new Date(Date.UTC(2026, 8, 20 + i, 0, 0, 0)) });
    saveMaterialSnapshot({ dataDir: tmp, generation: g, products: P, set_components: S, keep: 3 });
    gens.push(g);
  }
  const files = fs.readdirSync(path.join(tmp, MATERIAL_DIR_NAME)).sort();
  assert.deepEqual(files, gens.slice(2).map((g) => `${g.generation_id}.json.gz`));
  const back = readMaterialSnapshot({ dataDir: tmp, generationId: gens[4].generation_id });
  assert.deepEqual(back.products, P);
  assert.equal(readMaterialSnapshot({ dataDir: tmp, generationId: gens[0].generation_id }), null);   // 消えた世代
  // 壊す (中身を書き換える) → 読むときに止まる
  const f = path.join(tmp, MATERIAL_DIR_NAME, `${gens[3].generation_id}.json.gz`);
  const obj = JSON.parse(zlib.gunzipSync(fs.readFileSync(f)).toString('utf8'));
  obj.products[0].原価 = 999;
  fs.writeFileSync(f, zlib.gzipSync(Buffer.from(JSON.stringify(obj), 'utf8')));
  assert.throws(() => readMaterialSnapshot({ dataDir: tmp, generationId: gens[3].generation_id }), (e) => e.code === 'MATERIAL_HASH_MISMATCH');
});

// ── Render の受け手 (warehouse-mirror の /api/sync) を同じプロセスで立てる ──
const express = (await import('express')).default;
const mirrorRouter = (await import('../apps/warehouse-mirror/router.js')).default;
const { getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const app = express();
app.use(express.json({ limit: '20mb' }));
app.use('/', mirrorRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, r));
const base = `http://127.0.0.1:${server.address().port}`;
for (let i = 0; i < 100; i++) { try { getMirrorDB(); break; } catch { await new Promise((r) => setTimeout(r, 20)); } }
const post = async (body) => {
  const res = await fetch(`${base}/api/sync`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': 'test-key' }, body: JSON.stringify(body) });
  return { status: res.status, json: await res.json().catch(() => null) };
};
const MP = [
  { product_id: 1, 商品コード: 'a001', 商品名: 'A', 商品区分: '単品', 取扱区分: '取扱中', 原価状態: 'COMPLETE', 原価: 100, 仕入先コード: '0001' },
  { product_id: 2, 商品コード: 'b002', 商品名: 'B', 商品区分: '単品', 取扱区分: '取扱中', 原価状態: 'MISSING', 仕入先コード: '0002' },
];
const MS = [{ セット商品コード: 'set1', 構成商品コード: 'a001', 数量: 2, 構成商品名: 'A', 構成商品原価: 100 }];
const gens = () => getMirrorDB().prepare('select * from mirror_material_generations order by entity').all();

await ta('[3] 受け手は写しを入れ替えたのと同じ取引で世代を記録する', async () => {
  const g = buildMaterialGeneration({ products: MP, set_components: MS, neProductsCompleteAt: '2026-09-25 07:05:00', now: new Date('2026-09-25T00:20:00Z') });
  const r = await post({ products: MP, set_components: MS, material_generation: g });
  assert.equal(r.status, 200, JSON.stringify(r.json));
  const rows = gens();
  assert.deepEqual(rows.map((x) => [x.entity, x.generation_id, x.row_count, x.content_hash]), [['products', g.generation_id, 2, g.products.content_hash], ['set_components', g.generation_id, 1, g.set_components.content_hash]]);
  assert.equal(rows[0].source_complete_at, '2026-09-25 07:05:00');
});

await ta('[3] 行数が合わない・形がおかしい・世代が無い (古い送り手) なら記録しない。写しの入れ替えは今までどおり', async () => {
  const before = gens();
  const g = buildMaterialGeneration({ products: MP, set_components: MS, now: new Date('2026-09-26T00:20:00Z') });
  g.products.row_count = 99;
  let r = await post({ products: MP, set_components: MS, material_generation: g });
  assert.equal(r.status, 200);
  assert.equal(gens().find((x) => x.entity === 'products').generation_id, before.find((x) => x.entity === 'products').generation_id);   // 行数違い = products は記録しない
  assert.equal(gens().find((x) => x.entity === 'set_components').generation_id, g.generation_id);   // set_components は合うので記録
  r = await post({ products: [MP[0]], set_components: MS, material_generation: { generation_id: 'bad', products: {}, set_components: {} } });
  assert.equal(r.status, 200);
  assert.equal(getMirrorDB().prepare('select count(*) as n from mirror_products').get().n, 1);   // 入れ替えは行われた
  r = await post({ products: MP, set_components: MS });
  assert.equal(r.status, 200);
  assert.equal(getMirrorDB().prepare('select count(*) as n from mirror_products').get().n, 2);
});

// ── 夜間ロードが ops.load_materials に残す ──
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { buildPlanFromRender } = await import('../apps/company-db/load/sources.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');

await ta('[4] 夜間ロードは読んだ世代を ops.load_materials に残す (世代の無い材料は null)。0028 が未適用でも失敗しない', async () => {
  const g = buildMaterialGeneration({ products: MP, set_components: MS, now: new Date('2026-09-27T00:20:00Z') });
  assert.equal((await post({ products: MP, set_components: MS, material_generation: g })).status, 200);
  getMirrorDB().prepare("delete from mirror_material_generations where entity = 'set_components'").run();   // set_components の世代は分からない状態
  const pg = new PGlite(); const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  const r = await runInitialLoad(db, buildPlanFromRender({ dataDir: tmp, log: quiet }), { log: quiet, runId: 'mat_load_1', host: 'test-host' });
  assert.equal(r.ok, true, r.error);
  const rows = (await db.query("select entity, generation_id, content_hash, row_count, rule_version, ownership_hash from ops.load_materials where ingest_run_id = 'mat_load_1' order by entity")).rows;
  assert.equal(rows.length, 2);
  assert.deepEqual([rows[0].entity, rows[0].generation_id, rows[0].content_hash, rows[0].row_count], ['products', g.generation_id, g.products.content_hash, 2]);
  assert.deepEqual([rows[1].entity, rows[1].generation_id, rows[1].content_hash], ['set_components', null, null]);
  assert.match(rows[0].ownership_hash, /^[0-9a-f]{64}$/); assert.equal(rows[0].rule_version, 'v1');
  assert.deepEqual(r.material, { products: g.generation_id, set_components: null });
  // dry-run は残さない
  await runInitialLoad(db, buildPlanFromRender({ dataDir: tmp, log: quiet }), { log: quiet, runId: 'mat_load_dry', host: 'test-host', dryRun: true });
  assert.equal((await db.query("select count(*)::int as n from ops.load_materials where ingest_run_id = 'mat_load_dry'")).rows[0].n, 0);
  await pg.close();
  // 0028 が未適用
  const pg0 = new PGlite(); const db0 = pgliteAdapter(pg0);
  await applyMigrations(db0, { log: quiet, to: '0027' });
  const r0 = await runInitialLoad(db0, buildPlanFromRender({ dataDir: tmp, log: quiet }), { log: quiet, runId: 'mat_load_0', host: 'test-host' });
  assert.equal(r0.ok, true, r0.error);
  assert.ok((r0.notes || []).some((x) => /0028 が未適用/.test(x)), JSON.stringify(r0.notes));
  await pg0.close();
});

server.close();
try { getMirrorDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
