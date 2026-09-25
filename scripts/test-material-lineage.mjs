/**
 * test-material-lineage.mjs — 夜間ロードの「材料」の世代を残す (Company DB構想 10 §6 / PR ③a-1)
 *
 * 固定する契約:
 *   1 中身のハッシュは Render の mirror が持つ形 (MATERIAL_COLUMNS・空の埋め方) にそろえてから出す。行の並び・鍵の順番に依らず、値が変われば変わる。
 *     世代 ID は同じミリ秒・同じ中身でも重ならない
 *   2 控え (DATA_DIR/cdb-material/<世代>.json.gz) は上書きしない・書きかけ (.tmp) を残さない・失敗しても古い世代と古い .tmp を片付ける。
 *     読むときは中の世代 ID・中身のハッシュ・期待のハッシュ (ops.load_materials) を確かめる (MATERIAL_HASH_MISMATCH)
 *   3 Render の受け手 (/api/sync) は入れ替えたのと同じ取引で、入れた中身からハッシュを出し直し、合うときだけ世代を記録する。
 *     記録できないとき (古い送り手・形がおかしい・中身が合わない) は前の世代の記録を消す。どの場合も写しの入れ替えは今までどおり (500 にしない)
 *   4 夜間ロードは自分が読んだ中身のハッシュを ops.load_materials に残し、世代と合うときだけ世代 ID を付ける (matched)。
 *     Render 側で mirror が書き換えられていれば mismatch。0028 が未適用でも失敗しない
 *   5 NE 取込の「最後まで取れた印」: 商品は途中で失敗すると印が無い / セット商品は入れ替えと同じ取引で書く / CSV で上書きしたら印が消える
 *   6 raw_ne_products / raw_ne_set_products を書き換えるファイルは、どれも印を消す関数 (clearNeCompleteMarks) を呼ぶ (書き込み口が増えたら落ちる)
 * 使い方: node scripts/test-material-lineage.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import http from 'node:http';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mat-lineage-'));
process.env.DATA_DIR = tmp;
process.env.MIRROR_SYNC_KEY = 'test-key';

const {
  contentHash, materialDigest, projectMaterialRows, buildMaterialGeneration, saveMaterialSnapshot, readMaterialSnapshot,
  MATERIAL_DIR_NAME, MATERIAL_COLUMNS, MATERIAL_ID_RE,
} = await import('../apps/warehouse/material-lineage.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

const P = [
  { product_id: 1, 商品コード: 'a001', 商品名: 'A', 商品区分: '単品', 原価状態: 'COMPLETE', 原価: 100 },
  { product_id: 2, 商品コード: 'b002', 商品名: 'B', 商品区分: '単品', 原価状態: 'MISSING', 原価: 200 },
];
const S = [{ セット商品コード: 'set1', 構成商品コード: 'a001', 数量: 2 }];

await ta('[1] ハッシュは mirror の形にそろえてから。並び・鍵の順番に依らず、値が変われば変わる。世代 ID は重ならない', async () => {
  const h = materialDigest('products', P).content_hash;
  assert.equal(materialDigest('products', [P[1], P[0]]).content_hash, h);
  assert.equal(materialDigest('products', [{ 原価: 100, 商品名: 'A', 商品コード: 'a001', product_id: 1, 原価状態: 'COMPLETE', 商品区分: '単品' }, P[1]]).content_hash, h);
  assert.notEqual(materialDigest('products', [{ ...P[0], 原価: 101 }, P[1]]).content_hash, h);
  assert.notEqual(materialDigest('products', [{ ...P[0], 原価: null }, P[1]]).content_hash, h);   // 空にしても変わる
  assert.notEqual(materialDigest('products', [P[0]]).content_hash, h);                             // 行が減っても変わる
  // mirror に無い列 (miniPC の SELECT * にだけある列) は数えない / 空のフラグは 0 (受け手の ?? 0) / Infinity は null (JSON で送ると null)
  assert.equal(materialDigest('products', [{ ...P[0], miniPC_only: 'x' }, P[1]]).content_hash, h);
  assert.equal(materialDigest('products', [{ ...P[0], seasonality_flag: 0, new_product_flag: 0 }, P[1]]).content_hash, h);
  const [pr] = projectMaterialRows('products', [{ ...P[0], 原価: Infinity, miniPC_only: 'x' }]);
  assert.deepEqual(Object.keys(pr), [...MATERIAL_COLUMNS.products]);
  assert.equal(pr.原価, null); assert.equal(pr.seasonality_flag, 0); assert.equal(pr.代表商品コード, null);
  // ハッシュそのものは Infinity と null を区別する
  assert.notEqual(contentHash([{ a: Infinity }]), contentHash([{ a: null }]));
  const at = new Date('2026-09-25T00:10:11.123Z');
  const g = buildMaterialGeneration({ products: P, set_components: S, neProductsCompleteAt: '2026-09-25 07:05:00', now: at });
  assert.match(g.generation_id, MATERIAL_ID_RE);
  assert.match(g.generation_id, /^mat_20260925T001011123Z_/);
  assert.equal(g.products.row_count, 2); assert.equal(g.set_components.row_count, 1);
  assert.equal(g.products.source_complete_at, '2026-09-25 07:05:00');
  assert.equal(g.set_components.source_complete_at, null);
  // 同じミリ秒・同じ中身でも別の ID / set_components だけ違えばハッシュの部分も違う
  const g2 = buildMaterialGeneration({ products: P, set_components: S, now: at });
  assert.notEqual(g2.generation_id, g.generation_id);
  const g3 = buildMaterialGeneration({ products: P, set_components: [{ ...S[0], 数量: 3 }], now: at });
  assert.notEqual(g3.generation_id.split('_')[2], g.generation_id.split('_')[2]);
});

await ta('[2] 控え: 新しい keep 個だけ残る・上書きしない・書きかけを残さない・読むときに確かめる', async () => {
  const dir = path.join(tmp, MATERIAL_DIR_NAME);
  const gens = [];
  for (let i = 0; i < 5; i++) {
    const g = buildMaterialGeneration({ products: P, set_components: S, now: new Date(Date.UTC(2026, 8, 20 + i, 0, 0, 0)) });
    saveMaterialSnapshot({ dataDir: tmp, generation: g, products: P, set_components: S, keep: 3 });
    gens.push(g);
  }
  assert.deepEqual(fs.readdirSync(dir).sort(), gens.slice(2).map((g) => `${g.generation_id}.json.gz`));
  const back = readMaterialSnapshot({ dataDir: tmp, generationId: gens[4].generation_id });
  assert.deepEqual(back.products, projectMaterialRows('products', P));   // 控えの中身 = mirror の形
  assert.equal(readMaterialSnapshot({ dataDir: tmp, generationId: gens[0].generation_id }), null);   // 消えた世代
  assert.ok(readMaterialSnapshot({ dataDir: tmp, generationId: gens[4].generation_id, expected: { products: gens[4].products.content_hash } }));
  const mismatch = (e) => e.code === 'MATERIAL_HASH_MISMATCH';
  // 期待のハッシュ (ops.load_materials) と違う
  assert.throws(() => readMaterialSnapshot({ dataDir: tmp, generationId: gens[4].generation_id, expected: { products: 'f'.repeat(64) } }), mismatch);
  // 別の正当な控えが別の世代の名前で置かれている
  fs.copyFileSync(path.join(dir, `${gens[4].generation_id}.json.gz`), path.join(dir, `${gens[3].generation_id}.json.gz`));
  assert.throws(() => readMaterialSnapshot({ dataDir: tmp, generationId: gens[3].generation_id }), mismatch);
  // 中身を書き換える
  const f = path.join(dir, `${gens[2].generation_id}.json.gz`);
  const obj = JSON.parse(zlib.gunzipSync(fs.readFileSync(f)).toString('utf8'));
  obj.products[0].原価 = 999;
  fs.writeFileSync(f, zlib.gzipSync(Buffer.from(JSON.stringify(obj), 'utf8')));
  assert.throws(() => readMaterialSnapshot({ dataDir: tmp, generationId: gens[2].generation_id }), mismatch);
  // 世代 ID の形がおかしい (場所を外に向けさせない)
  assert.throws(() => readMaterialSnapshot({ dataDir: tmp, generationId: '../warehouse' }), (e) => e.code === 'BAD_GENERATION_ID');
  // 同じ世代をもう一度 = 上書きしない。書きかけも残さない
  assert.throws(() => saveMaterialSnapshot({ dataDir: tmp, generation: gens[4], products: P, set_components: S, keep: 3 }), (e) => e.code === 'MATERIAL_SNAPSHOT_EXISTS');
  assert.equal(fs.readdirSync(dir).filter((x) => x.endsWith('.tmp')).length, 0);
  // 中身が世代と合わなければ書かない (その失敗でも古い .tmp は片付ける。Codex R2 Low)
  const g5 = buildMaterialGeneration({ products: P, set_components: S });
  const stale0 = path.join(dir, 'mat_20260101T000000000Z_00000000_000009.1.00000000.tmp');
  fs.writeFileSync(stale0, 'x');
  const longAgo = new Date(Date.now() - 3 * 60 * 60 * 1000);
  fs.utimesSync(stale0, longAgo, longAgo);
  assert.throws(() => saveMaterialSnapshot({ dataDir: tmp, generation: g5, products: [P[0]], set_components: S, keep: 3 }), mismatch);
  assert.ok(!fs.existsSync(path.join(dir, `${g5.generation_id}.json.gz`)));
  assert.ok(!fs.existsSync(stale0));
  // 失敗しても片付ける: 古い世代が keep を超えていれば消え、古い .tmp は消え、新しい .tmp (別の回が書いている最中) は残る
  for (let i = 0; i < 3; i++) fs.writeFileSync(path.join(dir, `mat_20260101T00000000${i}Z_00000000_00000${i}.json.gz`), 'old');
  const stale = path.join(dir, 'mat_20260101T000000000Z_00000000_000000.123.deadbeef.tmp');
  const fresh = path.join(dir, 'mat_20260101T000000000Z_00000000_000001.456.cafebabe.tmp');
  fs.writeFileSync(stale, 'x'); fs.writeFileSync(fresh, 'x');
  const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);
  fs.utimesSync(stale, twoHoursAgo, twoHoursAgo);
  assert.throws(() => saveMaterialSnapshot({ dataDir: tmp, generation: gens[4], products: P, set_components: S, keep: 3 }), (e) => e.code === 'MATERIAL_SNAPSHOT_EXISTS');
  const left = fs.readdirSync(dir);
  assert.equal(left.filter((x) => x.endsWith('.json.gz')).length, 3);
  assert.ok(!left.some((x) => x.endsWith('.json.gz') && x.startsWith('mat_20260101T')));   // 古い世代は消えた
  assert.ok(!fs.existsSync(stale)); assert.ok(fs.existsSync(fresh));
  fs.rmSync(fresh);
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
const realFetch = globalThis.fetch;
const post = async (body) => {
  const res = await realFetch(`${base}/api/sync`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-sync-key': 'test-key' }, body: JSON.stringify(body) });
  return { status: res.status, json: await res.json().catch(() => null) };
};
// miniPC の m_products の SELECT * の形 (mirror に無い列あり・フラグが空の行あり)
const MP = [
  { product_id: 1, 商品コード: 'a001', 商品名: 'A', 商品区分: '単品', 取扱区分: '取扱中', 原価状態: 'COMPLETE', 原価: 100, 消費税率: 0.1, 仕入先コード: '0001', seasonality_flag: 1, season_months: '12', updated_at: '2026-09-25 07:00:00' },
  { product_id: 2, 商品コード: 'b002', 商品名: 'B', 商品区分: '単品', 取扱区分: '取扱中', 原価状態: 'MISSING', 仕入先コード: '0002', updated_at: '2026-09-25 07:00:00' },
];
const MS = [{ セット商品コード: 'set1', 構成商品コード: 'a001', 数量: 2, 構成商品名: 'A', 構成商品原価: 100, updated_at: '2026-09-25 07:00:00' }];
const gensOf = () => Object.fromEntries(getMirrorDB().prepare('select * from mirror_material_generations').all().map((r) => [r.entity, r]));
const mirrorCount = (t) => getMirrorDB().prepare(`select count(*) as n from ${t}`).get().n;
let genNo = 0;
const newGen = (p, s) => buildMaterialGeneration({ products: p, set_components: s, now: new Date(Date.UTC(2026, 8, 25, 1, 0, genNo++)) });

await ta('[3] 受け手: 入れた中身からハッシュを出し直し、合えば同じ取引で世代を記録する (mirror の列 = MATERIAL_COLUMNS)', async () => {
  const cols = (t) => getMirrorDB().prepare(`pragma table_info(${t})`).all().map((c) => c.name).filter((c) => c !== 'updated_at').sort();
  assert.deepEqual(cols('mirror_products'), [...MATERIAL_COLUMNS.products].sort());
  assert.deepEqual(cols('mirror_set_components'), [...MATERIAL_COLUMNS.set_components].sort());
  const g = buildMaterialGeneration({ products: MP, set_components: MS, neProductsCompleteAt: '2026-09-25 07:05:00', now: new Date('2026-09-25T00:20:00Z') });
  const r = await post({ products: MP, set_components: MS, material_generation: g });
  assert.equal(r.status, 200, JSON.stringify(r.json));
  const rows = gensOf();
  assert.deepEqual([rows.products.generation_id, rows.products.row_count, rows.products.content_hash], [g.generation_id, 2, g.products.content_hash]);
  assert.deepEqual([rows.set_components.generation_id, rows.set_components.content_hash], [g.generation_id, g.set_components.content_hash]);
  assert.equal(rows.products.source_complete_at, '2026-09-25 07:05:00');
  // mirror に入った行 = 送り手がそろえた形 (空の埋め方が受け手と同じ)
  const stored = getMirrorDB().prepare(`select ${MATERIAL_COLUMNS.products.map((c) => `"${c}"`).join(', ')} from mirror_products order by product_id`).all();
  assert.deepEqual(stored.map((x) => ({ ...x })), projectMaterialRows('products', MP));
});

await ta('[3] 記録できない受信は前の世代の記録を消す (新→旧の送り手 / 壊れた世代 / 同じ件数で中身違い / 行数違い / セット側だけ不正)。入れ替えは続く', async () => {
  const MP2 = MP.map((p) => ({ ...p, 商品名: `${p.商品名}2` }));
  const MS2 = [{ ...MS[0], 数量: 5 }];
  // (a) 新しい送り手で記録 → 古い送り手 (世代なし) で中身が変わる → 両方の記録が消える
  assert.equal((await post({ products: MP, set_components: MS, material_generation: newGen(MP, MS) })).status, 200);
  assert.deepEqual(Object.keys(gensOf()).sort(), ['products', 'set_components']);
  assert.equal((await post({ products: MP2, set_components: MS2 })).status, 200);
  assert.deepEqual(gensOf(), {});
  assert.equal(getMirrorDB().prepare("select 商品名 from mirror_products where 商品コード = 'a001'").get().商品名, 'A2');   // 入れ替えは行われた
  // (b) 記録 → 時刻が文字列でない壊れた世代 → 500 にならず入れ替え、記録は消える
  assert.equal((await post({ products: MP, set_components: MS, material_generation: newGen(MP, MS) })).status, 200);
  const bad = { ...newGen(MP2, MS2), created_at: { bad: true } };
  let r = await post({ products: MP2, set_components: MS2, material_generation: bad });
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.deepEqual(gensOf(), {});
  assert.equal(getMirrorDB().prepare("select 商品名 from mirror_products where 商品コード = 'a001'").get().商品名, 'A2');
  const badTs = newGen(MP, MS); badTs.products.source_complete_at = 12345;   // products の部分だけ壊れている
  r = await post({ products: MP, set_components: MS, material_generation: badTs });
  assert.equal(r.status, 200);
  assert.deepEqual(Object.keys(gensOf()), ['set_components']);   // set_components の部分は記録
  // (b') NUL を含む時刻 (SQLite には入るが夜間ロードの PostgreSQL が拒む。Codex R2 M-1) → 記録しない
  const nulGen = { ...newGen(MP, MS), created_at: 'a\u0000b' };
  assert.equal((await post({ products: MP, set_components: MS, material_generation: nulGen })).status, 200);
  assert.deepEqual(gensOf(), {});
  const nulPart = newGen(MP, MS); nulPart.products.source_complete_at = '2026-09-25\u000007:00';
  assert.equal((await post({ products: MP, set_components: MS, material_generation: nulPart })).status, 200);
  assert.deepEqual(Object.keys(gensOf()), ['set_components']);
  // (c) 同じ件数で中身が違う (世代は別の中身のもの) → products の記録が消える
  assert.equal((await post({ products: MP, set_components: MS, material_generation: newGen(MP, MS) })).status, 200);
  const other = newGen(MP2, MS);
  assert.equal(other.products.row_count, MP.length);
  assert.equal((await post({ products: MP, set_components: MS, material_generation: other })).status, 200);
  assert.equal(gensOf().products, undefined);
  assert.equal(gensOf().set_components.generation_id, other.generation_id);
  // (d) 行数が合わない
  const wrongCount = newGen(MP, MS); wrongCount.products.row_count = 99;
  assert.equal((await post({ products: MP, set_components: MS, material_generation: wrongCount })).status, 200);
  assert.equal(gensOf().products, undefined);
  // (e) セット側だけ形がおかしい → products は記録、set_components は前の記録を消す
  const setBad = newGen(MP, MS2); setBad.set_components.content_hash = 'xyz';
  assert.equal((await post({ products: MP, set_components: MS2, material_generation: setBad })).status, 200);
  assert.equal(gensOf().products.generation_id, setBad.generation_id);
  assert.equal(gensOf().set_components, undefined);
  assert.equal(getMirrorDB().prepare('select 数量 from mirror_set_components').get().数量, 5);   // セットも入れ替わった
  // products を送らない回は products の記録に触らない (中身も変わらない)
  const onlySet = newGen(MP, MS);
  assert.equal((await post({ set_components: MS, material_generation: onlySet })).status, 200);
  assert.equal(gensOf().products.generation_id, setBad.generation_id);
  assert.equal(gensOf().set_components.generation_id, onlySet.generation_id);
  assert.equal(mirrorCount('mirror_products'), 2);
});

// ── 夜間ロードが ops.load_materials に残す ──
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { buildPlanFromRender } = await import('../apps/company-db/load/sources.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');

await ta('[4] 夜間ロード: 読んだ中身のハッシュを残し、世代と合うときだけ世代 ID (matched)。Render 側の書き換えは mismatch。0028 未適用でも失敗しない', async () => {
  const g = buildMaterialGeneration({ products: MP, set_components: MS, neProductsCompleteAt: '2026-09-27 07:05:00', now: new Date('2026-09-27T00:20:00Z') });
  assert.equal((await post({ products: MP, set_components: MS, material_generation: g })).status, 200);
  const pg = new PGlite(); const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  const load = async (runId, opts = {}) => {
    const r = await runInitialLoad(db, buildPlanFromRender({ dataDir: tmp, log: quiet }), { log: quiet, runId, host: 'test-host', ...opts });
    assert.equal(r.ok, true, r.error);
    return { r, rows: (await db.query('select * from ops.load_materials where ingest_run_id = $1 order by entity', [runId])).rows };
  };
  let { r, rows } = await load('mat_load_1');
  assert.deepEqual(rows.map((x) => [x.entity, x.status, x.generation_id, x.content_hash, x.row_count]),
    [['products', 'matched', g.generation_id, g.products.content_hash, 2], ['set_components', 'matched', g.generation_id, g.set_components.content_hash, 1]]);
  assert.equal(rows[0].source_complete_at, '2026-09-27 07:05:00');
  assert.match(rows[0].ownership_hash, /^[0-9a-f]{64}$/); assert.equal(rows[0].rule_version, 'v1');
  assert.deepEqual(r.material, { products: { status: 'matched', generation_id: g.generation_id }, set_components: { status: 'matched', generation_id: g.generation_id } });
  // Render 側のアプリが mirror_products を書き換えた (会計アプリの税率の登録と同じ UPDATE) → products は mismatch (世代 ID を付けない)
  getMirrorDB().prepare("UPDATE mirror_products SET 消費税率 = ? WHERE lower(商品コード) = lower(?)").run(0.08, 'a001');
  getMirrorDB().prepare("delete from mirror_material_generations where entity = 'set_components'").run();   // set_components の世代は分からない
  ({ r, rows } = await load('mat_load_2'));
  const readNow = materialDigest('products', getMirrorDB().prepare('select * from mirror_products').all());
  assert.deepEqual(rows.map((x) => [x.entity, x.status, x.generation_id, x.source_complete_at, x.mirror_generation_id]),
    [['products', 'mismatch', null, null, g.generation_id], ['set_components', 'no_generation', null, null, null]]);
  assert.equal(rows[0].content_hash, readNow.content_hash);   // 残るのは実際に読んだ中身のハッシュ
  assert.notEqual(rows[0].content_hash, g.products.content_hash);
  assert.ok((r.notes || []).some((x) => /products: mirror の中身が世代 .* と合わない/.test(x)), JSON.stringify(r.notes));
  // mirror の世代の行に NUL を含む時刻・形のおかしい世代 ID があっても夜間ロードは失敗しない (時刻は null・行は no_generation。Codex R2 M-1)
  const g4 = buildMaterialGeneration({ products: MP, set_components: MS, neProductsCompleteAt: '2026-09-28 07:05:00', now: new Date('2026-09-28T00:20:00Z') });
  assert.equal((await post({ products: MP, set_components: MS, material_generation: g4 })).status, 200);
  getMirrorDB().prepare("UPDATE mirror_material_generations SET received_at = 'x' || char(0) || 'y', source_complete_at = 'p' || char(0) WHERE entity = 'products'").run();
  getMirrorDB().prepare("UPDATE mirror_material_generations SET generation_id = 'bad' WHERE entity = 'set_components'").run();
  ({ r, rows } = await load('mat_load_3'));
  assert.deepEqual(rows.map((x) => [x.entity, x.status, x.generation_id, x.source_complete_at, x.mirror_received_at]),
    [['products', 'matched', g4.generation_id, null, null], ['set_components', 'no_generation', null, null, null]]);
  // matched なのに世代 ID が無い行は表が受け付けない
  await assert.rejects(db.query("insert into ops.load_materials (ingest_run_id, entity, status, content_hash, row_count, rule_version, ownership_hash) values ('x', 'products', 'matched', $1, 1, 'v1', 'h')", ['a'.repeat(64)]));
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

// ── NE 取込の「最後まで取れた印」 (ne-api.js。NE の API は fetch を差し替えて返す) ──
fs.writeFileSync(path.join(tmp, 'ne-tokens.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));
const ne = { goods: [], setgoods: [], failGoodsAtOffset: null, failSetgoods: false };
globalThis.fetch = async (url, opts) => {
  const u = String(url);
  if (!u.startsWith('https://api.next-engine.org')) return realFetch(url, opts);
  const q = new URLSearchParams(opts.body);
  const offset = Number(q.get('offset')), limit = Number(q.get('limit'));
  let body;
  if (u.endsWith('/api_v1_master_goods/search')) {
    body = ne.failGoodsAtOffset != null && offset >= ne.failGoodsAtOffset ? { result: 'error', message: 'テストの失敗' } : { result: 'success', data: ne.goods.slice(offset, offset + limit) };
  } else if (u.endsWith('/api_v1_master_setgoods/search')) {
    body = ne.failSetgoods ? { result: 'error', message: 'テストの失敗' } : { result: 'success', data: ne.setgoods.slice(offset, offset + limit) };
  } else body = { result: 'error', message: `知らない API ${u}` };
  return { ok: true, status: 200, json: async () => body };
};
const origLog = console.log; console.log = () => {};
const { fetchProducts, fetchSetProducts } = await import('../apps/warehouse/ne-api.js');
const { getDB } = await import('../apps/warehouse/db.js');
console.log = origLog;
const metaOf = (k) => getDB().prepare('select value from sync_meta where key = ?').get(k)?.value ?? null;
const quietly = async (fn) => { const l = console.log; console.log = () => {}; try { return await fn(); } finally { console.log = l; } };

await ta('[5] NE 取込の印: 商品は途中で失敗すると印が消える (前回の印で集合を取り出させない) / セット商品は入れ替えと同じ取引', async () => {
  ne.goods = Array.from({ length: 1003 }, (_, i) => ({ goods_id: `G${String(i).padStart(4, '0')}`, goods_name: `商品${i}` }));
  await quietly(fetchProducts);
  const at = metaOf('ne_api_products_complete_at');
  assert.ok(at);
  assert.equal(metaOf('ne_api_products_complete_count'), '1003');
  assert.equal(getDB().prepare('select count(*) as c from raw_ne_products where synced_at = ?').get(at).c, 1003);
  // 1 ページ目 (1000 件) は書けて 2 ページ目で失敗 → 印は無い
  ne.failGoodsAtOffset = 1000;
  await assert.rejects(quietly(fetchProducts), /テストの失敗/);
  assert.equal(metaOf('ne_api_products_complete_at'), null);
  assert.equal(metaOf('ne_api_products_complete_count'), null);
  // 次に最後まで取れれば印が戻る
  ne.failGoodsAtOffset = null;
  await quietly(fetchProducts);
  assert.ok(metaOf('ne_api_products_complete_at'));
  assert.equal(metaOf('ne_api_products_complete_count'), '1003');
  // セット商品: 成功で印と件数 (= synced_at が印の行)。失敗は入れ替えも印もしない (前の印のまま = 前の集合がそのまま取り出せる)
  ne.setgoods = [{ set_goods_id: 'SET1', set_goods_detail_goods_id: 'G0001', set_goods_detail_quantity: '2' }, { set_goods_id: 'SET1', set_goods_detail_goods_id: 'G0002', set_goods_detail_quantity: '1' }];
  await quietly(fetchSetProducts);
  const sat = metaOf('ne_api_setproducts_complete_at');
  assert.ok(sat);
  assert.equal(metaOf('ne_api_setproducts_complete_count'), '2');
  ne.failSetgoods = true;
  await assert.rejects(quietly(fetchSetProducts), /テストの失敗/);
  assert.equal(metaOf('ne_api_setproducts_complete_at'), sat);
  assert.equal(getDB().prepare('select count(*) as c from raw_ne_set_products where synced_at = ?').get(sat).c, 2);
  // CSV の取込 (csv-import.js の CLI。auto-import.js も同じ書き方) で上書きしたら、同じ取引で印が消える (Codex R2 M-2)
  const runCsv = (kind, lines) => {
    const p = path.join(tmp, `${kind}.csv`);
    fs.writeFileSync(p, lines.join('\r\n'), 'utf8');
    execFileSync(process.execPath, [path.join(repoRoot, 'apps', 'warehouse', 'csv-import.js'), kind, p], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmp }, encoding: 'utf8' });
  };
  assert.ok(metaOf('ne_api_products_complete_at'));
  runCsv('products', [Array.from({ length: 18 }, (_, i) => `c${i}`).join(','), 'G0001,商品1改,0001,100,200,取扱中,,,,0,,,,0,0,,0.1,0']);
  assert.equal(getDB().prepare("select 商品名 from raw_ne_products where 商品コード = 'g0001'").get().商品名, '商品1改');
  assert.equal(metaOf('ne_api_products_complete_at'), null);
  assert.equal(metaOf('ne_api_products_complete_count'), null);
  assert.ok(metaOf('ne_api_setproducts_complete_at'));   // 商品の CSV はセット商品の印に触らない
  runCsv('sets', [Array.from({ length: 7 }, (_, i) => `c${i}`).join(','), 'SET2,セット2,1000,G0003,1,0,']);
  assert.equal(metaOf('ne_api_setproducts_complete_at'), null);
});
globalThis.fetch = realFetch;

await ta('[6] raw_ne_products / raw_ne_set_products を書き換えるファイルは、どれも完了の印を消す (clearNeCompleteMarks) を呼ぶ', async () => {
  // 表ごと: raw_ne_products を書くなら clearNeCompleteMarks('products')、raw_ne_set_products を書くなら
  //   clearNeCompleteMarks('setproducts') か「同じ取引で印そのものを書く」(ne-api.js の fetchSetProducts)
  const writes = (table) => new RegExp(`(INSERT(\\s+OR\\s+\\w+)?\\s+INTO|UPDATE|DELETE\\s+FROM)\\s+${table}\\b`, 'i');
  const RULES = [
    { table: 'raw_ne_products', ok: (src) => src.includes("clearNeCompleteMarks('products')") },
    { table: 'raw_ne_set_products', ok: (src) => src.includes("clearNeCompleteMarks('setproducts')") || src.includes("updateSyncMeta('ne_api_setproducts_complete_at'") },
  ];
  const found = [];
  const walk = (dir) => {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      if (e.name === 'node_modules' || e.name.startsWith('.')) continue;
      const p = path.join(dir, e.name);
      if (e.isDirectory()) { if (e.name !== 'tests') walk(p); continue; }
      if (!/\.(m|c)?js$/.test(e.name) || /^test-|^smoke|\.test\./.test(e.name)) continue;
      const src = fs.readFileSync(p, 'utf8');
      for (const rule of RULES) if (writes(rule.table).test(src)) found.push([`${path.relative(repoRoot, p).replace(/\\/g, '/')} ${rule.table}`, rule.ok(src)]);
    }
  };
  walk(path.join(repoRoot, 'apps')); walk(path.join(repoRoot, 'scripts'));
  assert.deepEqual(found.map(([f]) => f).sort(), [
    'apps/warehouse/auto-import.js raw_ne_products', 'apps/warehouse/auto-import.js raw_ne_set_products',
    'apps/warehouse/csv-import.js raw_ne_products', 'apps/warehouse/csv-import.js raw_ne_set_products',
    'apps/warehouse/ne-api.js raw_ne_products', 'apps/warehouse/ne-api.js raw_ne_set_products',
  ]);
  assert.deepEqual(found.filter(([, ok]) => !ok), []);
});

server.close();
try { getMirrorDB().close(); } catch { /* */ }
try { getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
