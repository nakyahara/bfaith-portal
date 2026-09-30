/**
 * test-sku-map-receiver.mjs — Render の受け手: Amazon SKU の対 (sku_master / sku_resolved) の世代 (PR ⑦-0・16 §7 H1 / §8 契約 v3 High 1)
 *
 * 固定する契約:
 *   [1] 有効になる前: 世代なしは今までどおり (入れ替え・片方だけは保持・空は保持・clear は空に・sort_order の補い)。応答も今までどおり
 *   [2] 有効になる前でも、世代つきで形がおかしいものは断る (422・何も書かない・有効にならない)。状態の行を入れる所で落ちたら巻き戻る
 *   [3] 世代つきが届いたら確かめて有効にし、2 表と状態を入れる (応答 = part・世代・ハッシュ・行数)。同じ body の他の表も入る
 *   [4] 有効になった後: 世代なし・空にする・片方だけ・古い世代・同じ世代で違うハッシュ・ハッシュ / 行数が合わない・行の形・世代の形 を断る。
 *       どれも 409 / 422 で、同じ body の他の表 (products・同期の印) も含めて何も書かない。状態は変わらない
 *   [5] 同じ世代・同じハッシュ = replayed (何も書かない)。世代は数でも文字でも同じ
 *   [6] 表が手で書き換えられていたら、同じ世代の再送で入れ直す (repaired・状態は変えない)
 *   [7] 世代は数で比べる ('10' > '9'・2^53 を超えても桁が落ちない)
 *   [8] 途中で失敗 (行の途中・入れた後のハッシュ違い・状態の更新) → 2 表も状態も元のまま。直せば同じ世代が入る
 *   [9] 状態の行は消せない・世代は下げられない・同じ世代の中身は変えられない (DB の trigger)。再起動しても残る
 *   [10] 確かめる口 GET /api/sync/sku-map/state (x-sync-key が要る・世代は文字)
 *   [11] SKU の対を入れた後に同じ body の他の表で落ちた (500) → 応答に sku_map が載る・同じ世代の再送は replayed (状態は戻さない)
 * 使い方: node scripts/test-sku-map-receiver.mjs
 */
import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'skumap-gen-');

import assert from 'node:assert/strict';
import http from 'node:http';

process.env.MIRROR_SYNC_KEY = 'test-key';
delete process.env.ALLOW_INSECURE_MIRROR_SYNC;

const { toMirrorWireRows, buildSkuMapGeneration, skuMapDigest } = await import('../lib/sku-map-canonical.js');
const express = (await import('express')).default;
const mirrorRouter = (await import('../apps/warehouse-mirror/router.js')).default;
const { getMirrorDB, initMirrorDB } = await import('../apps/warehouse-mirror/db.js');

const app = express();
app.use(express.json({ limit: '20mb' }));
app.use('/', mirrorRouter);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;
for (let i = 0; i < 200; i++) { try { getMirrorDB(); break; } catch { await new Promise((r) => setTimeout(r, 20)); } }
// router の初期化 (dbReady) を待つ
for (let i = 0; i < 200; i++) {
  const r = await fetch(`${base}/api/sync/sku-map/state`, { headers: { 'x-sync-key': 'test-key' } }).catch(() => null);
  if (r && r.status !== 503) break;
  await new Promise((res) => setTimeout(res, 20));
}
let db = getMirrorDB();

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const post = async (body, { key = 'test-key' } = {}) => {
  const res = await fetch(`${base}/api/sync`, { method: 'POST', headers: { 'content-type': 'application/json', ...(key ? { 'x-sync-key': key } : {}) }, body: JSON.stringify(body) });
  return { status: res.status, json: await res.json().catch(() => null) };
};
const getState = async ({ key = 'test-key' } = {}) => {
  const res = await fetch(`${base}/api/sync/sku-map/state`, { headers: key ? { 'x-sync-key': key } : {} });
  return { status: res.status, json: await res.json().catch(() => null) };
};

// ── 書き込みの数を数える (「何も書かない」の確かめ) ──
const WATCHED = ['mirror_sku_master', 'mirror_sku_resolved', 'mirror_sku_map_state', 'mirror_products', 'mirror_sync_status'];
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

const bigJson = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? `${x}n` : x));
const snap = () => bigJson({
  master: db.prepare('SELECT * FROM mirror_sku_master ORDER BY seller_sku').all(),
  resolved: db.prepare('SELECT * FROM mirror_sku_resolved ORDER BY seller_sku, ne_code').all(),
  state: db.prepare('SELECT * FROM mirror_sku_map_state').safeIntegers(true).all(),
  products: db.prepare('SELECT * FROM mirror_products ORDER BY product_id').all(),
  sync: db.prepare('SELECT * FROM mirror_sync_status ORDER BY key').all(),
});
const stateRow = () => db.prepare('SELECT * FROM mirror_sku_map_state').safeIntegers(true).get();
const storedHash = () => skuMapDigest({
  master: db.prepare('SELECT seller_sku, 商品名 AS name, source_created_at AS created_at, source_updated_at AS updated_at FROM mirror_sku_master').all(),
  components: db.prepare('SELECT seller_sku, ne_code, quantity, sort_order, component_created_at AS created_at, component_updated_at AS updated_at FROM mirror_sku_resolved').all(),
}).content_hash;

// ── 材料 ──
const T1 = '2026-05-01T00:00:00.000Z', T2 = '2026-09-30T12:34:56.789Z', T3 = '2026-10-01T01:02:03.004Z';
const CANON_A = {
  master: [
    { seller_sku: 'pr_a-001', name: 'セット A', created_at: T1, updated_at: T2 },
    { seller_sku: 'b-002', name: '単品 B', created_at: T1, updated_at: T1 },
  ],
  components: [
    { seller_sku: 'pr_a-001', ne_code: 'ne-001', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 },
    { seller_sku: 'pr_a-001', ne_code: 'ne-002', quantity: 2, sort_order: 1, created_at: T1, updated_at: T2 },
    { seller_sku: 'b-002', ne_code: 'b-002', quantity: 1, sort_order: 0, created_at: T1, updated_at: T1 },
  ],
};
const CANON_B = {
  master: [...CANON_A.master.map((m) => (m.seller_sku === 'b-002' ? { ...m, name: '単品 B (改)', updated_at: T3 } : m)),
    { seller_sku: 'c-003', name: '新しい C', created_at: T3, updated_at: T3 }],
  components: [...CANON_A.components, { seller_sku: 'c-003', ne_code: 'ne-003', quantity: 3, sort_order: 0, created_at: T3, updated_at: T3 }],
};
const CANON_C = {
  master: [...CANON_B.master, { seller_sku: 'pr_c-004', name: 'D', created_at: T3, updated_at: T3 }],
  components: [...CANON_B.components, { seller_sku: 'pr_c-004', ne_code: 'boom', quantity: 1, sort_order: 0, created_at: T3, updated_at: T3 }],
};
const genPayload = (generation, canon, extra = {}) => ({ ...toMirrorWireRows(canon), sku_map_generation: buildSkuMapGeneration({ generation, ...canon }), ...extra });
/** 今の送り手 (sync-to-render.js) と同じ形 = 構成の時刻なし・世代なし */
const legacyPayload = (canon) => {
  const w = toMirrorWireRows(canon);
  return { sku_master: w.sku_master, sku_resolved: w.sku_resolved.map(({ component_created_at, component_updated_at, ...r }) => r) };
};
const PRODUCTS_1 = [{ product_id: 1, 商品コード: 'ne-001', 商品名: 'NE 1', 商品区分: '単品', 取扱区分: '取扱中', 原価: 100, 原価状態: 'COMPLETE' }];
const PRODUCTS_2 = [{ product_id: 2, 商品コード: 'ne-002', 商品名: 'NE 2', 商品区分: '単品', 取扱区分: '取扱中', 原価: 200, 原価状態: 'COMPLETE' }];
/** 入れると NOT NULL で落ちる products (原価状態が無い) */
const PRODUCTS_BAD = [{ product_id: 3, 商品コード: 'ne-003', 商品名: 'NE 3', 商品区分: '単品', 取扱区分: '取扱中', 原価: 300 }];
const masterKeys = () => db.prepare('SELECT seller_sku FROM mirror_sku_master ORDER BY seller_sku').all().map((r) => r.seller_sku);

await ta('[1] 有効になる前: 世代なしは今までどおり', async () => {
  let s = await getState();
  assert.equal(s.status, 200, JSON.stringify(s.json));
  assert.deepEqual(s.json.capability, { sku_map_generations: 1, format: 'sku-map-canon-v1', hash: 'sha256' });
  assert.deepEqual(s.json.state, { activated: false });
  // 入れ替え
  let r = await post(legacyPayload(CANON_A));
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.equal(r.json.sku_map, undefined);   // 応答は今までどおり (世代つきのときだけ sku_map)
  assert.deepEqual(masterKeys(), ['b-002', 'pr_a-001']);
  assert.equal(db.prepare('SELECT count(*) AS n FROM mirror_sku_resolved').get().n, 3);
  assert.equal(db.prepare('SELECT count(*) AS n FROM mirror_sku_resolved WHERE component_created_at IS NOT NULL').get().n, 0);
  assert.equal(stateRow(), undefined);
  // 片方だけ = 両方とも前の値を保持 (200)
  r = await post({ sku_master: legacyPayload(CANON_B).sku_master });
  assert.equal(r.status, 200);
  assert.deepEqual(masterKeys(), ['b-002', 'pr_a-001']);
  // 空 (flag なし) = 保持
  r = await post({ sku_master: [], sku_resolved: [] });
  assert.equal(r.status, 200);
  assert.deepEqual(masterKeys(), ['b-002', 'pr_a-001']);
  // sort_order の無い古い行は届いた順に 0,1,.. を補う
  const noOrder = legacyPayload(CANON_B); noOrder.sku_resolved.forEach((x) => { delete x.sort_order; });
  r = await post(noOrder);
  assert.equal(r.status, 200);
  assert.deepEqual(db.prepare("SELECT ne_code, sort_order FROM mirror_sku_resolved WHERE seller_sku = 'pr_a-001' ORDER BY sort_order").all().map((x) => [x.ne_code, x.sort_order]), [['ne-001', 0], ['ne-002', 1]]);
  // clear の印 + 空 = 両方を空に (手のオペ専用。今までどおり)
  r = await post({ sku_master: [], sku_resolved: [], meta: { clear_sku_master: true, clear_sku_resolved: true } });
  assert.equal(r.status, 200);
  assert.deepEqual(masterKeys(), []);
  // 次の試験のために今の送り方で入れ直す
  assert.equal((await post(legacyPayload(CANON_A))).status, 200);
  assert.equal(stateRow(), undefined);
});

await ta('[2] 有効になる前でも世代つきで形がおかしいものは断る (何も書かない・有効にならない)。状態の行を入れる所で落ちても巻き戻る', async () => {
  const before = snap(); const w0 = writes();
  const cases = [
    ['ハッシュ違い', { ...genPayload(5, CANON_A), sku_map_generation: { ...buildSkuMapGeneration({ generation: 5, ...CANON_A }), content_hash: 'f'.repeat(64) } }, 422, 'sku_map_hash_mismatch'],
    ['空', { sku_master: [], sku_resolved: [], sku_map_generation: buildSkuMapGeneration({ generation: 5, ...CANON_A }) }, 422, 'sku_map_clear_forbidden'],
    ['片方だけ', { sku_master: toMirrorWireRows(CANON_A).sku_master, sku_map_generation: buildSkuMapGeneration({ generation: 5, ...CANON_A }) }, 422, 'sku_map_pair_incomplete'],
    ['世代の形', { ...genPayload(5, CANON_A), sku_map_generation: { ...buildSkuMapGeneration({ generation: 5, ...CANON_A }), generation: '0' } }, 422, 'sku_map_generation_invalid'],
    ['行の形 (時刻が +09:00)', (() => { const p = genPayload(5, CANON_A); p.sku_master[0].source_created_at = '2026-05-01T09:00:00.000+09:00'; return p; })(), 422, 'sku_map_rows_invalid'],
  ];
  for (const [label, body, status, code] of cases) {
    const r = await post({ ...body, products: PRODUCTS_2 });
    assert.equal(r.status, status, `${label}: ${JSON.stringify(r.json)}`);
    assert.equal(r.json.error, code, label);
    assert.equal(r.json.sku_map.result, 'rejected', label);
    assert.deepEqual(r.json.sku_map.current, { activated: false }, label);
  }
  assert.equal(snap(), before);
  assert.deepEqual(writesSince(w0), {});
  // 状態の行を入れる所で落ちる → 2 表も元のまま・有効にならない
  db.exec(`CREATE TRIGGER test_fail_state_insert BEFORE INSERT ON mirror_sku_map_state BEGIN SELECT RAISE(ABORT, 'test: state insert fails'); END`);
  try {
    const r = await post(genPayload(5, CANON_A));
    assert.equal(r.status, 500, JSON.stringify(r.json));
    assert.equal(r.json.error, 'sku_map_apply_failed');
  } finally { db.exec('DROP TRIGGER test_fail_state_insert'); }
  assert.equal(snap(), before);
  assert.equal(stateRow(), undefined);
  assert.deepEqual((await getState()).json.state, { activated: false });
});

let activatedAt = null;
await ta('[3] 世代つきが届いたら有効にして 2 表と状態を入れる (同じ body の他の表も入る)', async () => {
  const g = buildSkuMapGeneration({ generation: 5, ...CANON_A });
  const r = await post({ ...genPayload(5, CANON_A), products: PRODUCTS_1 });
  assert.equal(r.status, 200, JSON.stringify(r.json));
  const sm = r.json.sku_map;
  assert.deepEqual([sm.part, sm.result, sm.generation, sm.content_hash, sm.master_rows, sm.component_rows, sm.format, sm.activated],
    ['sku_map', 'activated', '5', g.content_hash, 2, 3, 'sku-map-canon-v1', true]);
  assert.deepEqual(sm.capability, { sku_map_generations: 1, format: 'sku-map-canon-v1', hash: 'sha256' });
  const st = stateRow();
  assert.deepEqual([st.activated, st.activation_generation, st.generation, st.content_hash, st.master_rows, st.component_rows], [1n, 5n, 5n, g.content_hash, 2n, 3n]);
  assert.equal(typeof st.generation, 'bigint');
  assert.equal(db.prepare('SELECT typeof(generation) AS t FROM mirror_sku_map_state').get().t, 'integer');
  activatedAt = st.activated_at;
  assert.equal(sm.activated_at, activatedAt);
  // 表の中身 = 世代の中身 (読み直したハッシュが同じ)・親から写す列・構成の時刻
  assert.equal(storedHash(), g.content_hash);
  const row = db.prepare("SELECT * FROM mirror_sku_resolved WHERE seller_sku = 'pr_a-001' AND ne_code = 'ne-002'").get();
  assert.deepEqual([row.source, row.商品名, row.source_updated_at, row.component_created_at, row.component_updated_at, row.quantity, row.sort_order],
    ['master', 'セット A', T2, T1, T2, 2, 1]);
  assert.deepEqual(db.prepare('SELECT product_id FROM mirror_products').all().map((x) => x.product_id), [1]);   // 同じ body の products も入った
  const s = await getState();
  assert.deepEqual([s.json.state.activated, s.json.state.generation, s.json.state.activation_generation, s.json.state.content_hash], [true, '5', '5', g.content_hash]);
});

await ta('[4] 有効になった後は断る (409 / 422・同じ body の他の表も含めて何も書かない・状態は変わらない)', async () => {
  const before = snap(); const w0 = writes();
  const genA5 = buildSkuMapGeneration({ generation: 5, ...CANON_A });
  const cases = [
    ['世代なし (今の送り手)', legacyPayload(CANON_B), 409, 'sku_map_generation_required'],
    ['世代なしの片方だけ', { sku_master: legacyPayload(CANON_B).sku_master }, 409, 'sku_map_generation_required'],
    ['世代なしで clear', { sku_master: [], sku_resolved: [], meta: { clear_sku_master: true, clear_sku_resolved: true } }, 422, 'sku_map_clear_forbidden'],
    ['世代つきで空', { sku_master: [], sku_resolved: [], sku_map_generation: buildSkuMapGeneration({ generation: 6, ...CANON_B }) }, 422, 'sku_map_clear_forbidden'],
    ['世代つき + clear の印', { ...genPayload(6, CANON_B), meta: { clear_sku_master: true } }, 422, 'sku_map_clear_forbidden'],
    ['片方だけ (sku_master)', { sku_master: toMirrorWireRows(CANON_B).sku_master, sku_map_generation: buildSkuMapGeneration({ generation: 6, ...CANON_B }) }, 422, 'sku_map_pair_incomplete'],
    ['片方だけ (sku_resolved)', { sku_resolved: toMirrorWireRows(CANON_B).sku_resolved, sku_map_generation: buildSkuMapGeneration({ generation: 6, ...CANON_B }) }, 422, 'sku_map_pair_incomplete'],
    ['世代だけ', { sku_map_generation: buildSkuMapGeneration({ generation: 6, ...CANON_B }) }, 422, 'sku_map_pair_incomplete'],
    ['古い世代', genPayload(4, CANON_B), 409, 'sku_map_generation_stale'],
    ['同じ世代で違うハッシュ', genPayload(5, CANON_B), 409, 'sku_map_generation_conflict'],
    ['ハッシュが行と合わない', { ...toMirrorWireRows(CANON_B), sku_map_generation: { ...genA5, generation: '6', master_rows: 3, component_rows: 4 } }, 422, 'sku_map_hash_mismatch'],
    ['行数が印と合わない', { ...genPayload(6, CANON_B), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), master_rows: 2 } }, 422, 'sku_map_count_mismatch'],
    ['大文字の SKU', (() => { const p = genPayload(6, CANON_B); p.sku_master[0].seller_sku = 'PR_A-001'; return p; })(), 422, 'sku_map_rows_invalid'],
    ['数量が文字', (() => { const p = genPayload(6, CANON_B); p.sku_resolved[0].quantity = '1'; return p; })(), 422, 'sku_map_rows_invalid'],
    ['親の名前と違う 商品名', (() => { const p = genPayload(6, CANON_B); p.sku_resolved[0].商品名 = '別'; return p; })(), 422, 'sku_map_rows_invalid'],
    ['構成の無い SKU', (() => { const c = { master: CANON_B.master, components: CANON_B.components.filter((x) => x.seller_sku !== 'c-003') }; return { ...toMirrorWireRows(c), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), component_rows: 3 } }; })(), 422, 'sku_map_rows_invalid'],
    ['世代 0', { ...genPayload(6, CANON_B), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), generation: 0 } }, 422, 'sku_map_generation_invalid'],
    ['世代 "06"', { ...genPayload(6, CANON_B), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), generation: '06' } }, 422, 'sku_map_generation_invalid'],
    ['世代 6.5', { ...genPayload(6, CANON_B), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), generation: 6.5 } }, 422, 'sku_map_generation_invalid'],
    ['世代が 2^53 を超える数', { ...genPayload(6, CANON_B), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), generation: 2 ** 53 + 2 } }, 422, 'sku_map_generation_invalid'],
    ['世代が bigint を超える', { ...genPayload(6, CANON_B), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), generation: '9223372036854775808' } }, 422, 'sku_map_generation_invalid'],
    ['世代がオブジェクトでない', { ...genPayload(6, CANON_B), sku_map_generation: '6' }, 422, 'sku_map_generation_invalid'],
    ['並べ方の版が違う', { ...genPayload(6, CANON_B), sku_map_generation: { ...buildSkuMapGeneration({ generation: 6, ...CANON_B }), format: 'sku-map-canon-v2' } }, 422, 'sku_map_format_unsupported'],
  ];
  for (const [label, body, status, code] of cases) {
    const r = await post({ ...body, products: PRODUCTS_2, meta: { ...(body.meta || {}), test_marker: label } });
    assert.equal(r.status, status, `${label}: ${JSON.stringify(r.json)}`);
    assert.equal(r.json.error, code, `${label}: ${JSON.stringify(r.json)}`);
    assert.equal(r.json.sku_map.result, 'rejected');
    assert.equal(r.json.sku_map.current.generation, '5', label);   // 送り手が今の世代を知れる
  }
  assert.equal(snap(), before);
  assert.deepEqual(writesSince(w0), {});   // products も同期の印 (meta) も書いていない
});

await ta('[5] 同じ世代・同じハッシュ = replayed (何も書かない)。世代は数でも文字でも同じ', async () => {
  const st0 = bigJson(stateRow());
  for (const body of [genPayload(5, CANON_A), genPayload('5', CANON_A), { ...genPayload(5, CANON_A), sku_map_generation: { ...buildSkuMapGeneration({ generation: 5, ...CANON_A }), generation: 5 } }]) {
    const w0 = writes();
    const r = await post(body);
    assert.equal(r.status, 200, JSON.stringify(r.json));
    assert.equal(r.json.sku_map.result, 'replayed');
    assert.equal(r.json.sku_map.generation, '5');
    const d = writesSince(w0);
    assert.equal(d.mirror_sku_master, undefined); assert.equal(d.mirror_sku_resolved, undefined); assert.equal(d.mirror_sku_map_state, undefined);
  }
  assert.equal(bigJson(stateRow()), st0);
});

await ta('[6] 表が手で書き換えられていたら、同じ世代の再送で入れ直す (repaired・状態は変えない)', async () => {
  const st0 = bigJson(stateRow());
  const h = stateRow().content_hash;
  db.prepare("UPDATE mirror_sku_master SET 商品名 = '手で変えた' WHERE seller_sku = 'b-002'").run();
  db.prepare("DELETE FROM mirror_sku_resolved WHERE ne_code = 'ne-002'").run();
  assert.notEqual(storedHash(), h);
  const r = await post(genPayload(5, CANON_A));
  assert.equal(r.status, 200);
  assert.equal(r.json.sku_map.result, 'repaired');
  assert.equal(storedHash(), h);
  assert.equal(bigJson(stateRow()), st0);
});

await ta('[7] 世代は数で比べる (文字で比べると "10" < "5"・2^53 を超えても桁が落ちない)', async () => {
  let r = await post(genPayload('10', CANON_B));   // 文字で比べると '10' < '5' = 古いと誤る
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.deepEqual([r.json.sku_map.result, r.json.sku_map.generation], ['applied', '10']);
  assert.equal(storedHash(), buildSkuMapGeneration({ generation: 10, ...CANON_B }).content_hash);
  assert.deepEqual(masterKeys(), ['b-002', 'c-003', 'pr_a-001']);
  assert.equal(stateRow().activated_at, activatedAt);   // 有効になった時刻は変わらない
  assert.equal(stateRow().activation_generation, 5n);
  r = await post(genPayload('9', CANON_A));   // 文字で比べると '9' > '10' = 新しいと誤る
  assert.equal(r.status, 409); assert.equal(r.json.error, 'sku_map_generation_stale');
  // 2^53 + 1 (JSON の数だと 2^53 に丸まる) を文字で送る
  r = await post(genPayload('9007199254740993', CANON_A));
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.equal(r.json.sku_map.generation, '9007199254740993');
  assert.equal(stateRow().generation, 9007199254740993n);
  r = await post(genPayload('9007199254740992', CANON_B));   // 1 小さい = 古い (float だと同じ世代に見える)
  assert.equal(r.status, 409); assert.equal(r.json.error, 'sku_map_generation_stale');
  assert.equal((await getState()).json.state.generation, '9007199254740993');
});

await ta('[8] 途中で失敗 → 2 表も状態も元のまま。直せば同じ世代が入る', async () => {
  const before = snap();
  const G = '9007199254741000';
  const fail = async (label, triggerSql, code) => {
    db.exec(triggerSql);
    try {
      const r = await post({ ...genPayload(G, CANON_C), products: PRODUCTS_2 });
      assert.equal(r.status, 500, `${label}: ${JSON.stringify(r.json)}`);
      assert.equal(r.json.error, code, label);
    } finally { db.exec('DROP TRIGGER test_fail'); }
    assert.equal(snap(), before, label);
  };
  // 構成の行を入れている途中 (前の表は消し、親と他の構成は入れた後に 'boom' の行で落ちる)
  await fail('構成の途中', `CREATE TRIGGER test_fail BEFORE INSERT ON mirror_sku_resolved WHEN NEW.ne_code = 'boom' BEGIN SELECT RAISE(ABORT, 'test: boom'); END`, 'sku_map_apply_failed');
  // 入れた後に表の中身が変わる (読み直したハッシュが違う)
  await fail('入れた後のハッシュ違い', `CREATE TRIGGER test_fail AFTER INSERT ON mirror_sku_master WHEN NEW.seller_sku = 'c-003' BEGIN UPDATE mirror_sku_master SET 商品名 = '変わった' WHERE seller_sku = NEW.seller_sku; END`, 'sku_map_stored_hash_mismatch');
  // 状態を進める所
  await fail('状態の更新', `CREATE TRIGGER test_fail BEFORE UPDATE ON mirror_sku_map_state BEGIN SELECT RAISE(ABORT, 'test: state update fails'); END`, 'sku_map_apply_failed');
  // 直せば同じ世代が入る (失敗した回は何も残していない)
  const r = await post(genPayload(G, CANON_C));
  assert.equal(r.status, 200, JSON.stringify(r.json));
  assert.deepEqual([r.json.sku_map.result, r.json.sku_map.generation, r.json.sku_map.master_rows], ['applied', G, 4]);
});

await ta('[9] 状態の行は消せない・世代は下げられない・同じ世代の中身は変えられない (DB の trigger)。再起動しても残る', async () => {
  const st0 = bigJson(stateRow());
  const raises = (sql, re) => assert.throws(() => db.exec(sql), re, sql);
  raises('DELETE FROM mirror_sku_map_state', /消せない/);
  raises('UPDATE mirror_sku_map_state SET generation = generation - 1', /下げられない/);
  raises(`UPDATE mirror_sku_map_state SET content_hash = '${'0'.repeat(64)}'`, /変えられない/);
  raises('UPDATE mirror_sku_map_state SET master_rows = master_rows + 1', /変えられない/);
  raises("UPDATE mirror_sku_map_state SET activated_at = '2000-01-01T00:00:00.000Z'", /変えられない|下げられない/);
  raises('UPDATE mirror_sku_map_state SET activation_generation = 1', /変えられない|下げられない/);
  raises(`INSERT OR REPLACE INTO mirror_sku_map_state (id, activated, activated_at, activation_generation, generation, format, content_hash, master_rows, component_rows, applied_at)
    VALUES (1, 1, 'x', 1, 1, 'f', '${'0'.repeat(64)}', 1, 1, 'x')`, /1 つだけ/);
  raises(`INSERT INTO mirror_sku_map_state (id, activated, activated_at, activation_generation, generation, format, content_hash, master_rows, component_rows, applied_at)
    VALUES (2, 1, 'x', 1, 1, 'f', '${'0'.repeat(64)}', 1, 1, 'x')`, /1 つだけ|CHECK/);
  raises("UPDATE mirror_sku_map_state SET generation = 'abc'", /CHECK|下げられない/);
  assert.equal(bigJson(stateRow()), st0);
  // 再起動 (initMirrorDB をもう一度 = createTables) しても状態と trigger は残る
  initMirrorDB();
  db = getMirrorDB();
  assert.equal(bigJson(stateRow()), st0);
  raises('DELETE FROM mirror_sku_map_state', /消せない/);
  const r = await post(legacyPayload(CANON_A));
  assert.equal(r.status, 409); assert.equal(r.json.error, 'sku_map_generation_required');
});

await ta('[10] 確かめる口: x-sync-key が要る・世代は文字', async () => {
  assert.equal((await getState({ key: null })).status, 401);
  assert.equal((await getState({ key: 'wrong' })).status, 401);
  const s = await getState();
  assert.equal(s.status, 200);
  assert.deepEqual(Object.keys(s.json.state).sort(), ['activated', 'activated_at', 'activation_generation', 'applied_at', 'component_rows', 'content_hash', 'format', 'generation', 'master_rows'].sort());
  assert.equal(typeof s.json.state.generation, 'string');
  assert.equal(s.json.state.generation, '9007199254741000');
  assert.equal(s.json.state.content_hash, storedHash());
  // /api/sync も key が要る (今までどおり)
  assert.equal((await post(genPayload('9007199254741001', CANON_A), { key: 'wrong' })).status, 401);
});

await ta('[11] 対を入れた後に同じ body の他の表で落ちた (500) → 応答に sku_map が載る・同じ世代の再送は replayed', async () => {
  const G = '9007199254741002';
  const r = await post({ ...genPayload(G, CANON_B), products: PRODUCTS_BAD });
  assert.equal(r.status, 500, JSON.stringify(r.json));
  assert.match(r.json.error, /NOT NULL/);
  assert.deepEqual([r.json.sku_map.result, r.json.sku_map.generation], ['applied', G]);   // 送り手は「対は入った」と分かる
  assert.equal(stateRow().generation, BigInt(G));
  assert.equal(db.prepare('SELECT count(*) AS n FROM mirror_products WHERE product_id = 3').get().n, 0);
  const again = await post({ ...genPayload(G, CANON_B), products: PRODUCTS_1 });
  assert.equal(again.status, 200, JSON.stringify(again.json));
  assert.equal(again.json.sku_map.result, 'replayed');
});

server.close();
console.log(`\n${passed} 件 PASS`);
