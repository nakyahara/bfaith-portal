/**
 * test-ne-src.mjs — NE の元の値と取込の整合 (Company DB構想 10 §6.1.1 C1。Codex ③a-2 C-R0 #3・C-R1 H3 と C1 の受入条件・C1-R1 M1/M2/L3)
 *
 * 固定する契約:
 *   1 NE の API の取込は、原価・売価・消費税率 (単品) / セット販売価格・数量 (セット) の**元の値**を *_src に JSON の文字列で残す
 *     ('""' 空文字 / '"0"' 文字列のゼロ / '0' 数値 / 'null' = API が null / SQL の NULL = 欠落)。数値の列は今までどおり
 *   2 古い形の DB (列が無い) は initDB が列を足し、前からの行・その回に取れなかった行の *_src は NULL のまま (数値から逆算しない)。
 *     作り直しの記録の前からの行も番号は NULL
 *   3 商品の取込の整合 = 取った行・コードが空で飛ばした行・同じコードが 2 度来た (ne_api_products_integrity)。対象のコードは全件 (切り詰めない)
 *   4 セットの取込の整合 = 保存の前に、同じ親の名前・売価の食い違い・同じ親 × 子の重複 (数量)・キーの欠落を数える (ne_api_setproducts_integrity)。
 *     比べる値は元の値の形 (null と欠落を区別)。対象の親・親 × 子は全件。親の数 (ne_api_setproducts_complete_parents) は保存した同じ集合から
 *   5 CSV の取込も元の値を残す (列が無ければ NULL)。完了の印・親の数・整合の証跡は消える (CSV は完全な NE 集合にしない)
 *   6 自動取込 (auto-import.js) も同じ (実際に動かして確かめる)
 *   7 証跡の書き込みで失敗したら、セットの入れ替え・完了の印・親の数・整合の証跡がそろって巻き戻る / 商品は印が付かない
 * 使い方: node scripts/test-ne-src.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ne-src-'));
process.env.DATA_DIR = tmp;
fs.writeFileSync(path.join(tmp, 'ne-tokens.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));

// 古い形の DB (C1 の列が無い) を先に作っておく = initDB が列を足す道を通す
{
  const old = new Database(path.join(tmp, 'warehouse.db'));
  old.exec(`CREATE TABLE raw_ne_products (商品コード TEXT PRIMARY KEY, 商品名 TEXT, 仕入先コード TEXT, 原価 REAL, 売価 REAL, 取扱区分 TEXT,
    代表商品コード TEXT, ロケーションコード TEXT, 配送業者 TEXT, 発注ロット単位 INTEGER, 最終仕入日 TEXT, 商品分類タグ TEXT, 作成日 TEXT,
    在庫数 INTEGER, 引当数 INTEGER, 最終更新日 TEXT, 消費税率 REAL, 発注残数 INTEGER, synced_at TEXT)`);
  old.exec(`CREATE TABLE raw_ne_set_products (セット商品コード TEXT, セット商品名 TEXT, セット販売価格 REAL, 商品コード TEXT, 数量 INTEGER,
    セット在庫数 INTEGER, 代表商品コード TEXT, synced_at TEXT, PRIMARY KEY (セット商品コード, 商品コード))`);
  old.exec(`CREATE TABLE m_products_builds (build_id TEXT PRIMARY KEY, daily_sync_run_id TEXT, started_at TEXT NOT NULL, published_at TEXT NOT NULL,
    ne_products_complete_at TEXT, ne_products_mark_note TEXT, ne_setproducts_complete_at TEXT, ne_setproducts_mark_note TEXT,
    products_rows INTEGER NOT NULL, products_hash TEXT NOT NULL, set_components_rows INTEGER NOT NULL, set_components_hash TEXT NOT NULL,
    rule_version TEXT NOT NULL, reason_counts TEXT NOT NULL, reasons TEXT NOT NULL)`);
  old.prepare("INSERT INTO raw_ne_products (商品コード, 商品名, 原価, 売価, 消費税率, synced_at) VALUES ('old1', '古い', 100, 200, 10, '2026-01-01 00:00:00')").run();
  old.prepare("INSERT INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES ('oldset', '古いセット', 500, 'old1', 2, '2026-01-01 00:00:00')").run();
  old.prepare("INSERT INTO m_products_builds VALUES ('mpb_old', NULL, '2026-01-01 00:00:00', '2026-01-01 00:00:01', '2026-01-01 00:00:00', NULL, NULL, NULL, 0, 'h', 0, 'h', 'v', '{}', '[]')").run();
  old.close();
}

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
const cols = (t) => db().prepare(`PRAGMA table_info(${t})`).all().map((c) => c.name);

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

await ta('[2] 古い形の DB は列が足され、前からの行・その回に取れなかった行の元の値は NULL のまま (逆算しない)。作り直しの記録の前の行も番号は NULL', async () => {
  for (const [t, c] of [['raw_ne_products', ['原価_src', '売価_src', '消費税率_src']], ['raw_ne_set_products', ['セット販売価格_src', '数量_src']],
    ['m_products_builds', ['ne_products_complete_rev', 'ne_setproducts_complete_rev']]]) {
    for (const x of c) assert.ok(cols(t).includes(x), `${t}.${x}`);
  }
  // old1 は古い DB の行で、NE の一覧に無い (上の取込でも次の取込でも取れない)
  await quietly(fetchProducts);
  assert.deepEqual([row('old1').原価_src, row('old1').売価_src, row('old1').消費税率_src], [null, null, null]);
  assert.equal(row('old1').原価, 100);
  const s = db().prepare("SELECT セット販売価格_src, 数量_src, 数量 FROM raw_ne_set_products WHERE セット商品コード = 'oldset'").get();
  assert.deepEqual([s.セット販売価格_src, s.数量_src, s.数量], [null, null, 2]);
  const b = db().prepare("SELECT ne_products_complete_rev, ne_setproducts_complete_rev FROM m_products_builds WHERE build_id = 'mpb_old'").get();
  assert.deepEqual([b.ne_products_complete_rev, b.ne_setproducts_complete_rev], [null, null]);
});

await ta('[3] 商品の取込の整合 (取った行・コードが空・同じコードが 2 度) を完了の印と一緒に残す。対象のコードは全件', async () => {
  // 2 ページ目に G0〜G59 がもう一度 (60 件 = 前の上限 50 を超える)・コードが空が 1 行
  ne.goods = [...Array.from({ length: 1000 }, (_, i) => ({ goods_id: `G${i}` })), ...Array.from({ length: 60 }, (_, i) => ({ goods_id: `G${i}` })), { goods_id: '' }, { goods_id: 'X9' }];
  await quietly(fetchProducts);
  const it = JSON.parse(meta('ne_api_products_integrity'));
  assert.deepEqual([it.fetched_rows, it.written_rows, it.dropped_no_code, it.distinct_codes, it.dup_code_count], [1062, 1061, 1, 1001, 60]);
  assert.equal(it.dup_codes.length, 60);
  assert.equal(it.dup_codes[59], 'g59');
  assert.ok(meta('ne_api_products_complete_at'));
});

await ta('[4] セットの取込の整合: 保存の前に 親の名前・売価の食い違い (null と欠落も区別)・親 × 子の重複・キーの欠落を数える / 対象は全件 / 親の数は保存した集合から', async () => {
  ne.setgoods = [
    { set_goods_id: 'S1', set_goods_name: 'セット1', set_goods_selling_price: '900', set_goods_detail_goods_id: 'G1', set_goods_detail_quantity: '2' },
    { set_goods_id: 'S1', set_goods_name: 'セット1 (別名)', set_goods_selling_price: '900', set_goods_detail_goods_id: 'G2', set_goods_detail_quantity: '1' },   // 名前の食い違い
    { set_goods_id: 'S2', set_goods_name: 'セット2', set_goods_selling_price: '500', set_goods_detail_goods_id: 'G3', set_goods_detail_quantity: '1' },
    { set_goods_id: 'S2', set_goods_name: 'セット2', set_goods_selling_price: '500', set_goods_detail_goods_id: 'G3', set_goods_detail_quantity: '3' },   // 同じ親 × 子で数量が違う
    { set_goods_id: 'S3', set_goods_name: 'セット3', set_goods_selling_price: '', set_goods_detail_goods_id: 'G4', set_goods_detail_quantity: '' },     // 元の値が空
    { set_goods_id: '', set_goods_detail_goods_id: 'G5' },   // キーの欠落
    { set_goods_id: 'S4', set_goods_name: 'セット4', set_goods_selling_price: null, set_goods_detail_goods_id: 'G7', set_goods_detail_quantity: '1' },   // 売価 = null
    { set_goods_id: 'S4', set_goods_name: 'セット4', set_goods_detail_goods_id: 'G8', set_goods_detail_quantity: '1' },                                  // 売価が欠落 (null とは別)
    { set_goods_id: 'S5', set_goods_name: 'セット5', set_goods_selling_price: '300', set_goods_detail_goods_id: 'G6', set_goods_detail_quantity: null },
    { set_goods_id: 'S5', set_goods_name: 'セット5', set_goods_selling_price: '300', set_goods_detail_goods_id: 'G6' },                                  // 数量 null と欠落の重複
    // 食い違う親が 120 (前の上限 100 を超える)
    ...Array.from({ length: 120 }, (_, i) => [
      { set_goods_id: `B${i}`, set_goods_name: `束${i}`, set_goods_selling_price: '100', set_goods_detail_goods_id: 'C1', set_goods_detail_quantity: '1' },
      { set_goods_id: `B${i}`, set_goods_name: `束${i}'`, set_goods_selling_price: '100', set_goods_detail_goods_id: 'C2', set_goods_detail_quantity: '1' },
    ]).flat(),
    // 同じ親 × 子の重複が 105 (前の上限 100 を超える。親の名前・売価は同じ = 食い違いには数えない)
    ...Array.from({ length: 105 }, (_, i) => [
      { set_goods_id: `D${i}`, set_goods_name: `重${i}`, set_goods_selling_price: '100', set_goods_detail_goods_id: 'E1', set_goods_detail_quantity: '1' },
      { set_goods_id: `D${i}`, set_goods_name: `重${i}`, set_goods_selling_price: '100', set_goods_detail_goods_id: 'E1', set_goods_detail_quantity: '2' },
    ]).flat(),
  ];
  await quietly(fetchSetProducts);
  const it = JSON.parse(meta('ne_api_setproducts_integrity'));
  assert.deepEqual([it.fetched_rows, it.valid_rows, it.dropped_missing_key, it.parent_conflict_count, it.pair_dup_count], [460, 459, 1, 122, 107]);
  assert.equal(it.parent_conflicts.length, 122);
  assert.deepEqual(it.parent_conflicts.slice(0, 2), ['s1', 's4']);
  assert.equal(it.parent_conflicts[121], 'b119');
  assert.deepEqual(it.pair_dups.slice(0, 2), [{ parent: 's2', child: 'g3', qtys: ['"1"', '"3"'] }, { parent: 's5', child: 'g6', qtys: ['null', null] }]);
  assert.equal(it.pair_dups.length, 107);
  assert.deepEqual(it.pair_dups[106], { parent: 'd104', child: 'e1', qtys: ['"1"', '"2"'] });
  assert.equal(meta('ne_api_setproducts_complete_parents'), '230');   // 保存した集合 = s1〜s5・b0〜b119・d0〜d104 (古い oldset は入れ替えで消える)
  assert.equal(meta('ne_api_setproducts_complete_count'), '352');     // s2 × g3・s5 × g6・d × e1 は後の行だけ残る
  const s3 = db().prepare("SELECT セット販売価格, 数量, セット販売価格_src, 数量_src FROM raw_ne_set_products WHERE セット商品コード = 's3'").get();
  assert.deepEqual([s3.セット販売価格, s3.数量, s3.セット販売価格_src, s3.数量_src], [0, 1, '""', '""']);   // 数量の空は 1 として保存 (今までどおり)・元の値は空
});

const runCsv = (kind, lines) => {
  const p = path.join(tmp, `${kind}.csv`);
  fs.writeFileSync(p, lines.join('\r\n'), 'utf8');
  execFileSync(process.execPath, [path.join(repoRoot, 'apps', 'warehouse', 'csv-import.js'), kind, p], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmp }, encoding: 'utf8' });
};
const header = (n) => Array.from({ length: n }, (_, i) => `c${i}`).join(',');

await ta('[5] CSV の取込も元の値を残す (列が無ければ NULL)。完了の印・親の数・整合の証跡は消える', async () => {
  runCsv('products', [header(18), 'CSV1,商品,0001,,0,取扱中,,,,0,,,,0,0,,10,0']);
  const r = db().prepare("SELECT 原価, 原価_src, 売価_src, 消費税率_src FROM raw_ne_products WHERE 商品コード = 'csv1'").get();
  assert.deepEqual([r.原価, r.原価_src, r.売価_src, r.消費税率_src], [0, '""', '"0"', '"10"']);
  assert.equal(meta('ne_api_products_complete_at'), null); assert.equal(meta('ne_api_products_integrity'), null);
  // 消費税率の列が無い CSV (16 列) = 元の値は NULL (空文字にしない)
  runCsv('products', [header(16), 'CSV2,商品2,0001,50,80,取扱中,,,,0,,,,0,0,']);
  const r2 = db().prepare("SELECT 原価_src, 売価_src, 消費税率_src, 消費税率 FROM raw_ne_products WHERE 商品コード = 'csv2'").get();
  assert.deepEqual([r2.原価_src, r2.売価_src, r2.消費税率_src, r2.消費税率], ['"50"', '"80"', null, 0]);
  runCsv('sets', [header(7), 'CSVSET,セット,,G1,,0,']);
  const s = db().prepare("SELECT セット販売価格_src, 数量_src, 数量 FROM raw_ne_set_products WHERE セット商品コード = 'csvset'").get();
  assert.deepEqual([s.セット販売価格_src, s.数量_src, s.数量], ['""', '""', 1]);
  for (const k of ['ne_api_setproducts_complete_at', 'ne_api_setproducts_complete_parents', 'ne_api_setproducts_integrity']) assert.equal(meta(k), null, k);
  // 数量の列が無い CSV (4 列) = 数量_src は NULL・数量は今までどおり 1
  runCsv('sets', [header(4), 'CSVSET2,セット2,700,G2']);
  const s2 = db().prepare("SELECT セット販売価格_src, 数量_src, 数量 FROM raw_ne_set_products WHERE セット商品コード = 'csvset2'").get();
  assert.deepEqual([s2.セット販売価格_src, s2.数量_src, s2.数量], ['"700"', null, 1]);
});

await ta('[6] 自動取込 (auto-import.js を実際に動かす) も元の値を残し、完了の印・親の数・整合の証跡を消す', async () => {
  // 先に API の取込で印を付けておく
  ne.goods = [{ goods_id: 'A1', goods_cost_price: '1' }];
  ne.setgoods = [{ set_goods_id: 'AS', set_goods_name: 'x', set_goods_selling_price: '1', set_goods_detail_goods_id: 'A1', set_goods_detail_quantity: '1' }];
  await quietly(fetchProducts); await quietly(fetchSetProducts);
  for (const k of ['ne_api_products_complete_at', 'ne_api_products_integrity', 'ne_api_setproducts_complete_at', 'ne_api_setproducts_complete_parents', 'ne_api_setproducts_integrity']) assert.ok(meta(k), k);
  const importDir = path.join(tmp, 'import');
  fs.mkdirSync(importDir, { recursive: true });
  const files = { 'products_c1.csv': [header(18), 'AUTO1,自動,0001,,0,取扱中,,,,0,,,,0,0,,8,0'], 'sets_c1.csv': [header(7), 'AUTOSET,自動セット,1200,A1,3,0,'] };
  const past = new Date(Date.now() - 60_000);
  for (const [f, lines] of Object.entries(files)) { const p = path.join(importDir, f); fs.writeFileSync(p, lines.join('\r\n'), 'utf8'); fs.utimesSync(p, past, past); }
  const child = spawn(process.execPath, [path.join(repoRoot, 'apps', 'warehouse', 'auto-import.js')], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmp }, stdio: 'ignore' });
  try {
    const deadline = Date.now() + 30_000;
    while (Object.keys(files).some((f) => fs.existsSync(path.join(importDir, f)))) {
      if (Date.now() > deadline) throw new Error('auto-import が 30 秒で取り込まなかった');
      await new Promise((r) => setTimeout(r, 200));
    }
  } finally { child.kill(); }
  const r = db().prepare("SELECT 原価_src, 売価_src, 消費税率_src FROM raw_ne_products WHERE 商品コード = 'auto1'").get();
  assert.deepEqual([r.原価_src, r.売価_src, r.消費税率_src], ['""', '"0"', '"8"']);
  const s = db().prepare("SELECT セット販売価格_src, 数量_src FROM raw_ne_set_products WHERE セット商品コード = 'autoset'").get();
  assert.deepEqual([s.セット販売価格_src, s.数量_src], ['"1200"', '"3"']);
  for (const k of ['ne_api_products_complete_at', 'ne_api_products_integrity', 'ne_api_setproducts_complete_at', 'ne_api_setproducts_complete_parents', 'ne_api_setproducts_integrity']) assert.equal(meta(k), null, k);
});

await ta('[7] 証跡の書き込みで失敗したら、セットは入れ替え・完了の印・親の数・整合の証跡がそろって巻き戻る / 商品は印が付かない', async () => {
  ne.setgoods = [{ set_goods_id: 'R1', set_goods_name: 'r', set_goods_selling_price: '1', set_goods_detail_goods_id: 'G1', set_goods_detail_quantity: '1' }];
  await quietly(fetchSetProducts);
  const snap = () => ({ rows: db().prepare('SELECT セット商品コード, 商品コード, 数量_src FROM raw_ne_set_products ORDER BY 1, 2').all(),
    marks: ['complete_at', 'complete_count', 'complete_rev', 'complete_parents', 'integrity'].map((k) => meta(`ne_api_setproducts_${k}`)) });
  const before = snap();
  // 接続は取込ごとに開き直すので、TEMP ではなく本体の trigger で失敗させる
  db().exec("CREATE TRIGGER t_c1_fail BEFORE INSERT ON sync_meta WHEN NEW.key IN ('ne_api_setproducts_integrity', 'ne_api_products_integrity') BEGIN SELECT RAISE(ABORT, 'forced'); END");
  try {
    ne.setgoods = [{ set_goods_id: 'R2', set_goods_name: 'r2', set_goods_selling_price: '2', set_goods_detail_goods_id: 'G2', set_goods_detail_quantity: '5' },
      { set_goods_id: 'R2', set_goods_name: 'r2 別', set_goods_selling_price: '2', set_goods_detail_goods_id: 'G3', set_goods_detail_quantity: '5' }];
    await assert.rejects(quietly(fetchSetProducts), /forced/);
    assert.deepEqual(snap(), before);
    ne.goods = [{ goods_id: 'P1', goods_cost_price: '5' }];
    await assert.rejects(quietly(fetchProducts), /forced/);
    for (const k of ['complete_at', 'complete_count', 'complete_rev', 'integrity']) assert.equal(meta(`ne_api_products_${k}`), null, k);
  } finally { db().exec('DROP TRIGGER IF EXISTS t_c1_fail'); }
});

globalThis.fetch = realFetch;
try { getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
