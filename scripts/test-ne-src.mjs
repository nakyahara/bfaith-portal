/**
 * test-ne-src.mjs — NE の元の値と取込の整合 (Company DB構想 10 §6.1.1 C1。Codex ③a-2 C-R0 #3・C-R1 H3 と C1 の受入条件)
 *
 * 固定する契約:
 *   1 NE の API の取込は、原価・売価・消費税率 (単品) / セット販売価格・数量 (セット) の**元の値**を *_src に JSON の文字列で残す
 *     ('""' 空文字 / '"0"' 文字列のゼロ / '0' 数値 / 'null' = API が null / SQL の NULL = 欠落)。数値の列は今までどおり
 *   2 その回に取れなかった古い行の *_src は NULL のまま (数値から逆算しない)
 *   3 商品の取込の整合 = 取った行・コードが空で飛ばした行・同じコードが 2 度来た (ne_api_products_integrity)。完了の印と一緒
 *   4 セットの取込の整合 = 保存の前に、同じ親の名前・売価の食い違い・同じ親 × 子の重複 (数量)・キーの欠落を数える (ne_api_setproducts_integrity)。
 *     親の数 (ne_api_setproducts_complete_parents) は保存した同じ集合から、入れ替えと同じ取引で
 *   5 CSV の取込も元の値を残す (列が無ければ NULL)。完了の印・親の数・整合の証跡は消える (CSV は完全な NE 集合にしない)
 * 使い方: node scripts/test-ne-src.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ne-src-'));
process.env.DATA_DIR = tmp;
fs.writeFileSync(path.join(tmp, 'ne-tokens.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; } };

const ne = { goods: [], setgoods: [] };
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, opts) => {
  const u = String(url);
  if (!u.startsWith('https://api.next-engine.org')) return realFetch(url, opts);
  const q = new URLSearchParams(opts.body);
  const offset = Number(q.get('offset')), limit = Number(q.get('limit'));
  const list = u.endsWith('/api_v1_master_goods/search') ? ne.goods : ne.setgoods;
  return { ok: true, status: 200, json: async () => ({ result: 'success', data: list.slice(offset, offset + limit) }) };
};
const { fetchProducts, fetchSetProducts } = await quietly(() => import('../apps/warehouse/ne-api.js'));
const { getDB, neSrc } = await import('../apps/warehouse/db.js');
const db = () => getDB();
const meta = (k) => db().prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value ?? null;
const row = (code) => db().prepare('SELECT * FROM raw_ne_products WHERE 商品コード = ?').get(code);

await ta('[1] API の取込は元の値を JSON の文字列で残す (空文字・文字列のゼロ・数値・null・欠落を区別)。数値の列は今までどおり', async () => {
  assert.equal(neSrc(undefined), null); assert.equal(neSrc(''), '""'); assert.equal(neSrc('0'), '"0"'); assert.equal(neSrc(0), '0'); assert.equal(neSrc(null), 'null');
  ne.goods = [
    { goods_id: 'E1', goods_name: '空', goods_cost_price: '', goods_selling_price: '', goods_tax_rate: '' },
    { goods_id: 'Z1', goods_name: 'ゼロ', goods_cost_price: '0', goods_selling_price: 0, goods_tax_rate: '0' },
    { goods_id: 'V1', goods_name: '値', goods_cost_price: '12.5', goods_selling_price: '980', goods_tax_rate: '10' },
    { goods_id: 'N1', goods_name: 'null', goods_cost_price: null, goods_selling_price: null, goods_tax_rate: null },
    { goods_id: 'M1', goods_name: '欠落' },
  ];
  await quietly(fetchProducts);
  const src = (c) => { const r = row(c); return [r.原価_src, r.売価_src, r.消費税率_src]; };
  assert.deepEqual(src('e1'), ['""', '""', '""']);
  assert.deepEqual(src('z1'), ['"0"', '0', '"0"']);
  assert.deepEqual(src('v1'), ['"12.5"', '"980"', '"10"']);
  assert.deepEqual(src('n1'), ['null', 'null', 'null']);
  assert.deepEqual(src('m1'), [null, null, null]);
  // 数値の列は今までどおり (空・null・欠落 → 0)
  assert.deepEqual([row('e1').原価, row('n1').売価, row('m1').消費税率, row('v1').原価], [0, 0, 0, 12.5]);
});

await ta('[2] その回に取れなかった古い行の元の値は NULL のまま (数値から逆算しない)', async () => {
  db().prepare("INSERT OR REPLACE INTO raw_ne_products (商品コード, 商品名, 原価, 売価, 消費税率, synced_at) VALUES ('old1', '古い', 100, 200, 10, '2026-01-01 00:00:00')").run();
  await quietly(fetchProducts);   // old1 は NE の一覧に無い
  assert.deepEqual([row('old1').原価_src, row('old1').売価_src, row('old1').消費税率_src], [null, null, null]);
  assert.equal(row('old1').原価, 100);
});

await ta('[3] 商品の取込の整合 (取った行・コードが空・同じコードが 2 度) を完了の印と一緒に残す', async () => {
  ne.goods = [...Array.from({ length: 1000 }, (_, i) => ({ goods_id: `G${i}` })), { goods_id: 'G5' }, { goods_id: '' }, { goods_id: 'X9' }];   // 2 ページ目に G5 がもう一度・コードが空が 1 行
  await quietly(fetchProducts);
  const it = JSON.parse(meta('ne_api_products_integrity'));
  assert.deepEqual([it.fetched_rows, it.written_rows, it.dropped_no_code, it.distinct_codes, it.dup_code_count, it.dup_codes], [1003, 1002, 1, 1001, 1, ['g5']]);
  assert.ok(meta('ne_api_products_complete_at'));
});

await ta('[4] セットの取込の整合: 保存の前に 親の名前・売価の食い違い・親 × 子の重複・キーの欠落を数える / 親の数は保存した集合から', async () => {
  ne.setgoods = [
    { set_goods_id: 'S1', set_goods_name: 'セット1', set_goods_selling_price: '900', set_goods_detail_goods_id: 'G1', set_goods_detail_quantity: '2' },
    { set_goods_id: 'S1', set_goods_name: 'セット1 (別名)', set_goods_selling_price: '900', set_goods_detail_goods_id: 'G2', set_goods_detail_quantity: '1' },   // 名前の食い違い
    { set_goods_id: 'S2', set_goods_name: 'セット2', set_goods_selling_price: '500', set_goods_detail_goods_id: 'G3', set_goods_detail_quantity: '1' },
    { set_goods_id: 'S2', set_goods_name: 'セット2', set_goods_selling_price: '500', set_goods_detail_goods_id: 'G3', set_goods_detail_quantity: '3' },   // 同じ親 × 子で数量が違う
    { set_goods_id: 'S3', set_goods_name: 'セット3', set_goods_selling_price: '', set_goods_detail_goods_id: 'G4', set_goods_detail_quantity: '' },     // 元の値が空
    { set_goods_id: '', set_goods_detail_goods_id: 'G5' },   // キーの欠落
  ];
  await quietly(fetchSetProducts);
  const it = JSON.parse(meta('ne_api_setproducts_integrity'));
  assert.deepEqual([it.fetched_rows, it.valid_rows, it.dropped_missing_key, it.parent_conflict_count, it.parent_conflicts, it.pair_dup_count], [6, 5, 1, 1, ['s1'], 1]);
  assert.deepEqual(it.pair_dups, [{ parent: 's2', child: 'g3', qtys: ['1', '3'] }]);
  assert.equal(meta('ne_api_setproducts_complete_parents'), '3');   // 保存した集合 = s1・s2・s3
  assert.equal(meta('ne_api_setproducts_complete_count'), '4');     // s2 × g3 は後の行だけ残る
  const s3 = db().prepare("SELECT セット販売価格, 数量, セット販売価格_src, 数量_src FROM raw_ne_set_products WHERE セット商品コード = 's3'").get();
  assert.deepEqual([s3.セット販売価格, s3.数量, s3.セット販売価格_src, s3.数量_src], [0, 1, '""', '""']);   // 数量の空は 1 として保存 (今までどおり)・元の値は空
});

await ta('[5] CSV の取込も元の値を残す (列が無ければ NULL)。完了の印・親の数・整合の証跡は消える', async () => {
  const runCsv = (kind, lines) => {
    const p = path.join(tmp, `${kind}.csv`);
    fs.writeFileSync(p, lines.join('\r\n'), 'utf8');
    execFileSync(process.execPath, [path.join(repoRoot, 'apps', 'warehouse', 'csv-import.js'), kind, p], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmp }, encoding: 'utf8' });
  };
  runCsv('products', [Array.from({ length: 18 }, (_, i) => `c${i}`).join(','), 'CSV1,商品,0001,,0,取扱中,,,,0,,,,0,0,,10,0']);
  const r = db().prepare("SELECT 原価, 原価_src, 売価_src, 消費税率_src FROM raw_ne_products WHERE 商品コード = 'csv1'").get();
  assert.deepEqual([r.原価, r.原価_src, r.売価_src, r.消費税率_src], [0, '""', '"0"', '"10"']);
  assert.equal(meta('ne_api_products_complete_at'), null); assert.equal(meta('ne_api_products_integrity'), null);
  runCsv('sets', [Array.from({ length: 7 }, (_, i) => `c${i}`).join(','), 'CSVSET,セット,,G1,,0,']);
  const s = db().prepare("SELECT セット販売価格_src, 数量_src, 数量 FROM raw_ne_set_products WHERE セット商品コード = 'csvset'").get();
  assert.deepEqual([s.セット販売価格_src, s.数量_src, s.数量], ['""', '""', 1]);
  for (const k of ['ne_api_setproducts_complete_at', 'ne_api_setproducts_complete_parents', 'ne_api_setproducts_integrity']) assert.equal(meta(k), null, k);
  // auto-import も同じ書き方 (読み込むと監視が始まるので、書き込みの文を確かめる)
  const src = fs.readFileSync(path.join(repoRoot, 'apps', 'warehouse', 'auto-import.js'), 'utf8');
  assert.match(src, /synced_at, 原価_src, 売価_src, 消費税率_src/);
  assert.match(src, /neSrc\(row\[3\]\), neSrc\(row\[4\]\), neSrc\(row\[16\]\)/);
  assert.match(src, /synced_at, セット販売価格_src, 数量_src/);
  assert.match(src, /neSrc\(row\[2\]\), neSrc\(row\[4\]\)/);
});

globalThis.fetch = realFetch;
try { getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
