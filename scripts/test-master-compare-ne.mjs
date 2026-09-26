/**
 * test-master-compare-ne.mjs — 毎朝のマスタ照合 ②外との照合 (Company DB構想 10 §6.1.1 C2 v3〜v6。apps/company-db/master-compare/compare-ne.mjs・pending.mjs)
 *
 * 仮の日付 (2030-01-xx) で何日か回す。1 日 = 02:00 夜間ロード (その時の mirror を読む) → 07:00 NE の取込 → 08:07 作り直し → 08:30 Render へ送る (世代・到達) → 照合
 * 固定する契約:
 *   1 値の状態 (numState / textState / comparability) の組み合わせ = どの入力も 1 つに決まる
 *   2 NE = 材料 = Company DB なら ② pass・recoverable / W13:load は mc-v2 の全件 JSON を読める
 *   3 反映待ち: 1 日目 lag → 夜の再送で古い材料をロード → 2 日目 not_delivered_by_load (始まりは 1 日目のまま) → 3 日目に直れば match・台帳から消える
 *   4 新しい SKU = only_in_ne の lag → 翌朝 recoverable
 *   5 構成: 飛ばした行は記録した保持状態と今が一致したときだけ held_by_load / manual の数量違いは記録と一致したときだけ rule (manual) /
 *     ロードの後に変わったら unexplained / manual_same の行が変わったら ① と同じ load_mismatch
 *   6 値の状態: "0.0" = ne_no_value (判断の一覧) / 記録なし・知らない取扱区分・税率 0 = incomparable (保持・税率 0 は判断の一覧にも)
 *   7 説明: 税率の補い (ne_no_value + 理由) / セット売価を商品表から (rule) / 理由の無い材料の違い (unexplained)
 *   8 CDB にだけある: 材料に無い = spec_undecided / not_in_latest_fetch = rule / 整合で落ちた行がある朝は保持
 *   9 取込の整合: dup_codes の SKU は全部保持 / C1 の形の証跡で dropped_missing_key > 0 = 構成を保持
 *  10 台帳: HEAD が無いのに版がある = untrusted (反映待ちの判定は blocked・HEAD を作り直さない)
 *  11 前提の欠け: stale_ne (差の一覧は出す)・build_ne_mismatch・ne_written_after_mark・no_render_master・no_integrity / ② だけ落ちても ① は残る
 *  12 集合は重ならない (items / held / recoverable / out_of_scope)・承認の指紋は作り直しの ID で変わらず c で変わる
 * 使い方: node scripts/test-master-compare-ne.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mcne-test-'));
process.env.DATA_DIR = tmp;
process.env.DAILY_SYNC_RUN_ID = 'ds_test';

const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; } };
const { default: Database } = await import('better-sqlite3');
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { buildPlanFromRender } = await import('../apps/company-db/load/sources.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { buildMaterialGeneration, saveMaterialSnapshot, materialDigest, projectMaterialRows, MATERIAL_COLUMNS } = await import('../apps/warehouse/material-lineage.js');
const { MIRROR_PRODUCTS_DDL, MIRROR_SET_COMPONENTS_DDL } = await import('../apps/warehouse-mirror/material-tables.js');
const { numState, textState, comparability, KNOWN_DIFF } = await import('../apps/company-db/master-compare/compare-ne.mjs');
const { runCompare, RESULT_DIR } = await import('../apps/company-db/master-compare/run.mjs');
const { pendingDir } = await import('../apps/company-db/master-compare/pending.mjs');
const { writeEvidence } = await import('../apps/company-db/push/evidence.mjs');
const WH = await quietly(() => import('../apps/warehouse/db.js'));
await quietly(() => WH.initDB());
const wh = () => WH.getDB();
const W13CFG = await import('../config/watch-checks.mjs');
const { evalW13 } = await import('../apps/company-db/watch/checks.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

// ── 1 日の時刻 (JST の asOf) ──
const at = (asOf, hhmm, dayOffset = 0) => { const d = new Date(`${asOf}T${hhmm}:00+09:00`); d.setUTCDate(d.getUTCDate() + dayOffset); return d; };
const utcText = (d) => d.toISOString().replace('T', ' ').slice(0, 19);
const days = ['2030-01-10', '2030-01-11', '2030-01-12', '2030-01-13', '2030-01-14', '2030-01-15', '2030-01-16', '2030-01-17', '2030-01-18', '2030-01-19', '2030-01-20', '2030-01-21'];

// ── NE の状態 (元の値 = JSON の文字列) ──
const J = (v) => JSON.stringify(v);
function baseNe() {
  const p = (code, name, sup, cost, price, tax) => ({ code, name, supplier: sup, handling: '取扱中', cost_src: J(String(cost)), price_src: J(String(price)), tax_src: J(String(tax)) });
  return {
    products: [p('a001', '単品A', '0001', 100, 1000, 10), p('b002', '単品B', '0002', 200, 2000, 8), p('c003', '単品C', '0001', 300, 3000, 10),
      p('d004', '単品D', '0002', 400, 4000, 10), p('e005', '単品E', '0001', 500, 5000, 10), p('f006', '単品F', '0001', 600, 6000, 10)],
    sets: [{ parent: 's001', name: 'セット1', child: 'a001', price_src: J('5000'), qty_src: J('2') }, { parent: 's001', name: 'セット1', child: 'b002', price_src: J('5000'), qty_src: J('1') },
      { parent: 's002', name: 'セット2', child: 'a001', price_src: J('900'), qty_src: J('1') }],
  };
}
const clone = (x) => JSON.parse(JSON.stringify(x));
const num = (src) => { try { const v = JSON.parse(src); const n = Number(v); return v === '' || v == null || !Number.isFinite(n) ? null : n; } catch { return null; } };
/** NE → 材料 (作り直しがそのまま写した形)。patch で材料だけ変えられる */
function toMaterial(ne, patch = null) {
  const products = ne.products.map((r) => ({ 商品コード: r.code, 商品名: r.name || r.code, 商品区分: '単品', 取扱区分: r.handling, 標準売価: num(r.price_src), 原価: num(r.cost_src),
    原価ソース: 'NE', 原価状態: num(r.cost_src) ? 'COMPLETE' : 'MISSING', 消費税率: num(r.tax_src) ? num(r.tax_src) / 100 : null, 税区分: num(r.tax_src) === 8 ? 'REDUCED_8' : 'STANDARD_10', 仕入先コード: r.supplier }));
  const parents = new Map();
  for (const r of ne.sets) if (!parents.has(r.parent)) parents.set(r.parent, r);
  for (const [code, r] of parents) products.push({ 商品コード: code, 商品名: r.name || code, 商品区分: 'セット', 取扱区分: '取扱中', 標準売価: num(r.price_src), 原価: 1, 原価ソース: 'セット計算', 原価状態: 'COMPLETE', 消費税率: 0.1, 税区分: 'STANDARD_10' });
  const sets = ne.sets.map((r) => ({ セット商品コード: r.parent, 構成商品コード: r.child, 数量: num(r.qty_src) ?? 0 }));
  // product_id は mirror の主キー (材料の中身に入る)。コードごとに決まった番号にする (日をまたいで同じ SKU = 同じ番号)
  const PID = { a001: 1, b002: 2, c003: 3, d004: 4, e005: 5, f006: 6, s001: 7, s002: 8, h008: 9 };
  products.forEach((r) => { r.product_id = PID[r.商品コード] ?? 100 + products.indexOf(r); });
  const m = { products, sets };
  if (patch) patch(m);
  return m;
}
const setMat = (m, code, k, v) => { m.products.find((r) => r.商品コード === code)[k] = v; };

// ── Render の mirror と控え ──
const mirrorFile = path.join(tmp, 'warehouse-mirror.db');
function publishToMirror(mat, when) {
  const g = buildMaterialGeneration({ products: mat.products, set_components: mat.sets, now: when });
  saveMaterialSnapshot({ dataDir: tmp, generation: g, products: mat.products, set_components: mat.sets, nowMs: when.getTime() });
  const m = new Database(mirrorFile);
  try {
    m.exec(MIRROR_PRODUCTS_DDL); m.exec(MIRROR_SET_COMPONENTS_DDL);
    m.exec(`CREATE TABLE IF NOT EXISTS mirror_material_generations (entity TEXT PRIMARY KEY, generation_id TEXT NOT NULL, content_hash TEXT NOT NULL, row_count INTEGER NOT NULL,
      source_complete_at TEXT, created_at TEXT, received_at TEXT NOT NULL)`);
    m.transaction(() => {
      m.exec('DELETE FROM mirror_products; DELETE FROM mirror_set_components; DELETE FROM mirror_material_generations');
      const put = (table, cols, rows) => { const st = m.prepare(`INSERT INTO ${table} (${[...cols, 'updated_at'].map((c) => `"${c}"`).join(', ')}) VALUES (${[...cols, 'updated_at'].map(() => '?').join(', ')})`); for (const r of rows) st.run(...cols.map((c) => r[c] ?? null), 'x'); };
      put('mirror_products', MATERIAL_COLUMNS.products, projectMaterialRows('products', mat.products));
      put('mirror_set_components', MATERIAL_COLUMNS.set_components, projectMaterialRows('set_components', mat.sets));
      for (const e of ['products', 'set_components']) {
        const d = materialDigest(e, m.prepare(`SELECT * FROM mirror_${e}`).all());
        m.prepare('INSERT INTO mirror_material_generations VALUES (?,?,?,?,?,?,?)').run(e, g.generation_id, d.content_hash, d.row_count, null, g.created_at, 'x');
      }
    })();
  } finally { m.close(); }
  return g;
}

// ── warehouse.db の NE と作り直しの記録 ──
const INT_P = { fetched_rows: 0, written_rows: 0, dropped_no_code: 0, distinct_codes: 0, dup_code_count: 0, dup_codes: [] };
const INT_S = { fetched_rows: 0, valid_rows: 0, dropped_missing_key: 0, dropped_missing_parent: 0, missing_child_parents: [], parent_conflict_count: 0, parent_conflicts: [], pair_dup_count: 0, pair_dups: [] };
function setNe(ne, asOf, { intP = {}, intS = {}, dropIntKeys = [] } = {}) {
  const d = wh(); const ts = utcText(at(asOf, '07:00'));
  d.exec('DELETE FROM raw_ne_products; DELETE FROM raw_ne_set_products');
  const ip = d.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 取扱区分, 原価, 売価, 消費税率, synced_at, 原価_src, 売価_src, 消費税率_src) VALUES (?,?,?,?,?,?,?,?,?,?,?)');
  for (const r of ne.products) ip.run(r.code, r.name, r.supplier, r.handling, 0, 0, 0, ts, r.cost_src, r.price_src, r.tax_src);
  const is = d.prepare('INSERT INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at, セット販売価格_src, 数量_src) VALUES (?,?,?,?,?,?,?,?)');
  for (const r of ne.sets) is.run(r.parent, r.name, 0, r.child, 1, ts, r.price_src, r.qty_src);
  const meta = (k) => d.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value;
  const up = (k, v) => d.prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(k, v, ts);
  up('ne_api_products_complete_at', ts); up('ne_api_products_complete_count', String(ne.products.length)); up('ne_api_products_complete_rev', meta('ne_raw_products_rev'));
  up('ne_api_setproducts_complete_at', ts); up('ne_api_setproducts_complete_count', String(ne.sets.length)); up('ne_api_setproducts_complete_rev', meta('ne_raw_setproducts_rev'));
  up('ne_api_setproducts_complete_parents', String(new Set(ne.sets.map((r) => r.parent)).size));
  const s = { ...INT_S, ...intS }; for (const k of dropIntKeys) delete s[k];
  up('ne_api_products_integrity', JSON.stringify({ ...INT_P, ...intP })); up('ne_api_setproducts_integrity', JSON.stringify(s));
  return { at: ts, prev: meta('ne_raw_products_rev'), srev: meta('ne_raw_setproducts_rev') };
}
function setBuild(asOf, marks, reasons = []) {
  const d = wh(); const id = `mpb_${asOf.replace(/-/g, '')}_${Math.random().toString(16).slice(2, 8)}`;
  const pub = at(asOf, '08:07').toISOString();
  d.prepare(`INSERT INTO m_products_builds (build_id, daily_sync_run_id, started_at, published_at, ne_products_complete_at, ne_setproducts_complete_at, products_rows, products_hash,
    set_components_rows, set_components_hash, rule_version, reason_counts, reasons, ne_products_complete_rev, ne_setproducts_complete_rev) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(id, `ds_${asOf}`, pub, pub, marks.at, marks.at, 0, 'x', 0, 'x', 'v', '{}', JSON.stringify(reasons), Number(marks.prev), Number(marks.srev));
  return id;
}
function sendToRender(mat, asOf, buildId, status = 'recorded') {
  const when = at(asOf, '08:30');
  const g = publishToMirror(mat, when);
  const ent = (e) => ({ requested: { row_count: g[e].row_count, content_hash: g[e].content_hash }, status });
  writeEvidence(tmp, 'render-master', { generation_id: g.generation_id, build_id: buildId, entities: { products: ent('products'), set_components: ent('set_components') } }, { now: when, warn: quiet });
  return g;
}

// ── Company DB ──
const pg = new PGlite(); const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const nightly = async (asOf) => { const r = await runInitialLoad(db, buildPlanFromRender({ dataDir: tmp, log: quiet, now: at(asOf, '02:00') }), { log: quiet, runId: `load_${asOf}`, host: 'render-nightly', now: at(asOf, '02:00') }); assert.equal(r.ok, true, r.error); return r; };
const compare = (asOf, extra = {}) => runCompare({ db, dataDir: tmp, asOf, now: at(asOf, '08:40'), syncRunId: `ds_${asOf}`,
  write: (d, n, p) => writeEvidence(d, n, p, { now: at(asOf, '08:40'), warn: quiet }), ...extra });
/**
 * 1 日を回す。mirrorBeforeLoad = 夜の再送 (ロードの前に mirror を別の材料にする) / beforeLoad = ロードの前に Company DB を書き換える
 */
async function day(asOf, { ne, material = null, reasons = [], mirrorBeforeLoad = null, beforeLoad = null, status = 'recorded', integrity = {} } = {}) {
  if (mirrorBeforeLoad) publishToMirror(mirrorBeforeLoad, at(asOf, '01:00'));
  if (beforeLoad) await beforeLoad();
  await nightly(asOf);
  const marks = setNe(ne, asOf, integrity);
  const buildId = setBuild(asOf, marks, reasons);
  sendToRender(material || toMaterial(ne), asOf, buildId, status);
  const r = await compare(asOf);
  return { ...r, ne: r.result.ne, buildId };
}
const col = (ne, key, c) => { const it = ne.items.find((i) => i.subject_key === key); return it ? it.columns.filter((x) => (c ? x.col === c || x.child === c : true)) : []; };
const clsOf = (ne, key, c) => col(ne, key, c).map((x) => x.cls);
function disjoint(ne) {
  const sets = [new Set(ne.items.map((i) => i.subject_key)), new Set(Object.keys(ne.held)), new Set(ne.recoverable), new Set(Object.keys(ne.out_of_scope))];
  for (let i = 0; i < 4; i++) for (let j = i + 1; j < 4; j++) for (const k of sets[i]) assert.ok(!sets[j].has(k), `集合が重なる: ${k}`);
}
const sqlComp = (parent, child) => `parent_sku_id = (select sku_id from core.skus where code = '${parent}') and child_sku_id = (select sku_id from core.skus where code = '${child}')`;

await ta('[1] 値の状態の組み合わせ = どの入力も 1 つに決まる (比べやすさ・妥当性)', async () => {
  const cases = [
    [null, 'yen', 'unknown', 'incomparable'], ['"12.5"', 'yen', 'value', 'comparable'], ['"0.0"', 'yen', 'zero', 'no_value'], ['" 0 "', 'yen', 'zero', 'no_value'], ['0', 'yen', 'zero', 'no_value'],
    ['""', 'yen', 'empty', 'no_value'], ['"  "', 'yen', 'empty', 'no_value'], ['null', 'yen', 'null', 'no_value'], ['"-5"', 'yen', 'value', 'incomparable'], ['"abc"', 'yen', 'value', 'incomparable'],
    ['"1e999"', 'yen', 'value', 'incomparable'], ['"10"', 'tax', 'value', 'comparable'], ['"0"', 'tax', 'zero', 'incomparable'], ['"5"', 'tax', 'value', 'incomparable'], ['""', 'tax', 'empty', 'no_value'],
    ['"2"', 'qty', 'value', 'comparable'], ['"0"', 'qty', 'zero', 'incomparable'], ['"1.5"', 'qty', 'value', 'incomparable'], ['""', 'qty', 'empty', 'incomparable'], ['null', 'qty', 'null', 'incomparable'], ['{', 'yen', 'unknown', 'incomparable'],
  ];
  for (const [src, kind, raw, comp] of cases) { const s = numState(src, kind); assert.deepEqual([s.raw, comparability(s)], [raw, comp], `${src} ${kind}`); }
  assert.equal(comparability(textState('', 'name')), 'no_value'); assert.equal(comparability(textState(null, 'supplier')), 'no_value');
  assert.equal(comparability(textState('ﾒｰｶｰ取扱中止', 'handling')), 'comparable'); assert.equal(textState('ﾒｰｶｰ取扱中止', 'handling').value, 'discontinued');
  assert.equal(comparability(textState('廃番X', 'handling')), 'incomparable');   // 知らない語 (mapHandling は discontinued にしてしまう)
  assert.equal(textState(' 1 ', 'supplier').value, '0001');   // 仕入先コードはロードと同じくそろえる
  assert.ok(KNOWN_DIFF.includes('ne_no_value') && !KNOWN_DIFF.includes('incomparable') && !KNOWN_DIFF.includes('blocked'));
});

// 0 日目: 材料 M0 を送っておく (1 日目 02:00 のロードが読む)
publishToMirror(toMaterial(baseNe()), at('2030-01-09', '08:30'));
let NE = baseNe();
let D1;

await ta('[2] NE = 材料 = Company DB なら ② pass・recoverable・台帳の初めての版 / W13:load は mc-v2 を読める', async () => {
  D1 = await day(days[0], { ne: NE });
  assert.equal(D1.result.verdict, 'pass', JSON.stringify(D1.result.items).slice(0, 400) + D1.result.blocked_reason);
  const ne = D1.ne;
  assert.equal(ne.verdict, 'pass', JSON.stringify({ b: ne.blocked_reason, items: ne.items.slice(0, 3) }, null, 1));
  assert.ok(ne.recoverable.includes('value:a001') && ne.recoverable.includes('components:s001') && ne.recoverable.includes('only_in_ne:a001') && ne.recoverable.includes('kind:s002'));
  assert.equal(ne.pending.state, 'initial'); assert.ok(ne.pending.written);
  assert.ok(fs.existsSync(path.join(pendingDir(tmp, RESULT_DIR), 'HEAD.json')));
  disjoint(ne);
  assert.equal(D1.result.format, 'mc-v2'); assert.equal(D1.evidence.ne.verdict, 'pass');
  assert.match(D1.line, /^✅ マスタ照合 ①.* \/ ✅ ②: NE との差 0/);
  const check = W13CFG.CHECKS.find((c) => c.id === 'W13');
  const [r] = await evalW13({ config: W13CFG, asOf: days[0], evidence: null, syncRunId: 'ds_test', openIssues: [], dataDir: tmp }, check);
  assert.equal(r.verdict, 'pass', r.reason);
});

let lagFp = null;
await ta('[3] 反映待ち: lag → 夜の再送で古い材料 → not_delivered_by_load (始まりは 1 日目のまま) → 直れば match・台帳から消える', async () => {
  const OLD = clone(NE);
  NE.products[0].name = '新しい名前';
  const d2 = await day(days[1], { ne: NE });
  assert.deepEqual(clsOf(d2.ne, 'value:a001', 'name'), ['lag'], JSON.stringify(d2.ne.items, null, 1).slice(0, 600));
  const since = col(d2.ne, 'value:a001', 'name')[0].pending_since;
  assert.ok(since && since.startsWith('2030-01-10T23:30'));   // 2 日目 08:30 JST
  // 3 日目: 夜の再送で古い材料 (A) に戻ってからロード → Company DB は古いまま。今朝また新しい材料 (B) を送る
  const d3 = await day(days[2], { ne: NE, mirrorBeforeLoad: toMaterial(OLD) });
  assert.deepEqual(clsOf(d3.ne, 'value:a001', 'name'), ['not_delivered_by_load']);
  assert.equal(col(d3.ne, 'value:a001', 'name')[0].pending_since, since);   // 再送で延ばさない
  // 4 日目: mirror = 3 日目の新しい材料 → ロードが入れる → match・台帳から消える
  const d4 = await day(days[3], { ne: NE });
  assert.equal(d4.ne.verdict, 'pass', JSON.stringify(d4.ne.items).slice(0, 300));
  assert.ok(d4.ne.recoverable.includes('value:a001'));
  assert.equal(d4.ne.counts.pending, 0);
  disjoint(d2.ne); disjoint(d3.ne);
});

await ta('[4] 新しい SKU = only_in_ne の lag → 翌朝 recoverable', async () => {
  NE.products.push({ code: 'h008', name: '新商品', supplier: '0001', handling: '取扱中', cost_src: J('80'), price_src: J('800'), tax_src: J('10') });
  const d5 = await day(days[4], { ne: NE });
  assert.deepEqual(clsOf(d5.ne, 'only_in_ne:h008'), ['lag']);
  assert.equal(d5.ne.held['value:h008'], 'not_in_cdb');
  const d6 = await day(days[5], { ne: NE });
  assert.ok(d6.ne.recoverable.includes('only_in_ne:h008') && d6.ne.recoverable.includes('value:h008'), JSON.stringify(d6.ne.items).slice(0, 300));
});

await ta('[5] 構成: 飛ばした行の保持状態・manual の記録と一致したときだけ説明済み / 変わったら unexplained / manual_same の行が変わったら load_mismatch', async () => {
  // 7 日目: 夜の再送で s002 × a001 の数量 0 (不正) の材料 → ロードは飛ばす (invalid_qty・保持状態 = 数量 1・ne)。NE は数量 3
  NE.sets[2].qty_src = J('3');
  const bad = toMaterial(NE, (m) => { m.sets.find((r) => r.セット商品コード === 's002').数量 = 0; });
  // 5 日目までの s002 × a001 は 1。manual: s001 × b002 を manual 5 (材料 1) に・s001 × a001 を manual 2 (材料と同じ)
  const d7 = await day(days[6], { ne: NE, mirrorBeforeLoad: bad, beforeLoad: async () => {
    await db.query(`update core.sku_components set source = 'manual', qty = 5 where ${sqlComp('s001', 'b002')}`);
    await db.query(`update core.sku_components set source = 'manual' where ${sqlComp('s001', 'a001')}`);
  } });
  const dec = (await db.query(`select payload from ops.load_decisions where ingest_run_id = 'load_${days[6]}' and section = 'set_components'`)).rows[0].payload;
  assert.ok(dec.skipped.some(([p, c, why, x]) => p === 's002' && c === 'a001' && why === 'invalid_qty' && x.held && x.held.qty === 1 && x.held.source === 'ne'));
  assert.ok(dec.skipped.some(([p, c, why, x]) => p === 's001' && c === 'b002' && why === 'manual_qty_mismatch' && x.manual_qty === 5 && x.plan_qty === 1));
  assert.deepEqual(clsOf(d7.ne, 'components:s002', 'a001'), ['held_by_load'], JSON.stringify(d7.ne.items, null, 1).slice(0, 800));
  assert.deepEqual(clsOf(d7.ne, 'components:s001', 'b002'), ['rule']);
  assert.equal(col(d7.ne, 'components:s001', 'b002')[0].explained.reason, 'manual');
  assert.deepEqual(clsOf(d7.ne, 'components:s001', 'a001'), ['match']);   // 同じ親の他の子には効かない
  assert.ok(d7.ne.decisions.some((d) => d.subject_key === 'components:s001' && d.reason_kind === 'manual'));
  // ロードの後に変わった: 飛ばした行の数量 → unexplained / manual の数量 → unexplained / manual_same の行 → ① と同じ load_mismatch
  await db.query(`update core.sku_components set qty = 7 where ${sqlComp('s002', 'a001')}`);
  await db.query(`update core.sku_components set qty = 9 where ${sqlComp('s001', 'b002')}`);
  await db.query(`update core.sku_components set qty = 6 where ${sqlComp('s001', 'a001')}`);
  const r = (await compare(days[6])).result.ne;
  assert.deepEqual(clsOf(r, 'components:s002', 'a001'), ['unexplained']);
  assert.deepEqual(clsOf(r, 'components:s001', 'b002'), ['unexplained']);
  assert.deepEqual(clsOf(r, 'components:s001', 'a001'), ['load_mismatch']);
  disjoint(r);
  // 元に戻す (manual を消して、次のロードで材料どおりに)
  await db.query(`update core.sku_components set source = 'ne', qty = 1 where ${sqlComp('s001', 'b002')}`);
  await db.query(`update core.sku_components set source = 'ne', qty = 2 where ${sqlComp('s001', 'a001')}`);
});

await ta('[6] 値の状態: "0.0" = ne_no_value (判断の一覧) / 記録なし・知らない取扱区分・税率 0 = incomparable (保持。税率 0 は判断の一覧にも)', async () => {
  NE.products.find((r) => r.code === 'c003').price_src = J('0.0');
  NE.products.find((r) => r.code === 'd004').cost_src = null;
  NE.products.find((r) => r.code === 'e005').handling = '廃番X';
  NE.products.find((r) => r.code === 'f006').tax_src = J('0');
  // 材料と Company DB は前のまま (作り直しは NE の空・0 に補いをかける想定 = ここでは材料を前の値に固定する)
  const keep = (m) => { setMat(m, 'c003', '標準売価', 3000); setMat(m, 'd004', '原価', 400); setMat(m, 'd004', '原価状態', 'COMPLETE'); setMat(m, 'e005', '取扱区分', '取扱中'); setMat(m, 'f006', '消費税率', 0.1); };
  const d8 = await day(days[7], { ne: NE, material: toMaterial(NE, keep) });
  const ne = d8.ne;
  assert.deepEqual(clsOf(ne, 'value:c003', 'standard_price_jpy'), ['ne_no_value']);
  const dc = ne.decisions.find((d) => d.subject_key === 'value:c003' && d.col === 'standard_price_jpy');
  assert.deepEqual([dc.n_state, dc.proposal], ['zero', { op: 'set_ne_value', value: 3000 }]);
  assert.equal(ne.held['cost:d004'], 'incomparable');
  assert.equal(ne.held['value:e005'], 'incomparable');   // 名前・税率などは一致・取扱区分だけ比べられない = 保持 (回復にしない)
  assert.ok(!ne.recoverable.includes('value:e005'));
  assert.equal(ne.held['value:f006'], 'incomparable');
  assert.ok(ne.decisions.some((d) => d.subject_key === 'value:f006' && d.col === 'tax_rate' && d.cls === 'incomparable' && d.n_state === 'zero'));
  lagFp = dc.approval_fingerprint;
  disjoint(ne);
});

await ta('[7] 説明: 税率の補い (ne_no_value + 理由) / セット売価を商品表から (rule) / 理由の無い材料の違い (unexplained) / 承認の指紋', async () => {
  NE.products.find((r) => r.code === 'b002').tax_src = J('');
  NE.sets.filter((r) => r.parent === 's002').forEach((r) => { r.price_src = J('900'); });
  const keep = (m) => { setMat(m, 'b002', '消費税率', 0.08); setMat(m, 'b002', '税区分', 'REDUCED_8'); setMat(m, 's002', '標準売価', 1000); setMat(m, 'a001', '商品名', '材料だけの名前');
    setMat(m, 'c003', '標準売価', 3000); setMat(m, 'd004', '原価', 400); setMat(m, 'd004', '原価状態', 'COMPLETE'); setMat(m, 'e005', '取扱区分', '取扱中'); setMat(m, 'f006', '消費税率', 0.1); };
  const reasons = [{ code: 'b002', kind: '単品', col: 'tax_rate', reason: 'tax_fallback', value: 0.08, source: 'product_tax_rate', ne_value: 0 },
    { code: 's002', kind: 'セット', col: 'price', reason: 'set_price_from_goods', value: 1000, set_master_value: 0 }];
  // 1 日目 (材料を送る) → 2 日目 (ロードが入れた後) に比べる
  await day(days[8], { ne: NE, material: toMaterial(NE, keep), reasons });
  const d = await day(days[9], { ne: NE, material: toMaterial(NE, keep), reasons });
  const ne = d.ne;
  assert.deepEqual(clsOf(ne, 'value:b002', 'tax_rate'), ['ne_no_value']);
  assert.equal(col(ne, 'value:b002', 'tax_rate')[0].reasons[0].reason, 'tax_fallback');
  assert.deepEqual(clsOf(ne, 'value:s002', 'standard_price_jpy'), ['rule']);
  assert.deepEqual(ne.decisions.find((x) => x.subject_key === 'value:s002').proposal, { op: 'set_ne_value', value: 1000 });
  assert.deepEqual(clsOf(ne, 'value:a001', 'name'), ['unexplained']);
  assert.equal(col(ne, 'value:a001', 'name')[0].why, 'build_without_reason');
  // 承認の指紋: 作り直しの ID・日付が変わっても同じ (6 の c003 と同じ判断) / c が変わると変わる
  assert.equal(ne.decisions.find((x) => x.subject_key === 'value:c003' && x.col === 'standard_price_jpy').approval_fingerprint, lagFp);
  await db.query("update core.skus set standard_price_jpy = 3100 where code = 'c003'");
  const r2 = (await compare(days[9])).result.ne;
  assert.notEqual(r2.decisions.find((x) => x.subject_key === 'value:c003' && x.col === 'standard_price_jpy').approval_fingerprint, lagFp);
});

await ta('[8] CDB にだけある: 材料に無い = spec_undecided / not_in_latest_fetch = rule / 落ちた行がある朝は保持 / value:X は回復しない', async () => {
  const noD = clone(NE); noD.products = noD.products.filter((r) => r.code !== 'd004');
  const d = await day(days[10], { ne: noD, material: toMaterial(noD) });
  assert.deepEqual(clsOf(d.ne, 'only_in_cdb:d004'), ['spec_undecided']);
  assert.equal(d.ne.held['value:d004'], 'not_in_ne');
  assert.ok(!d.ne.recoverable.includes('value:d004'));
  // 材料には残っていて、作り直しが not_in_latest_fetch を付けた = rule
  const withD = toMaterial(NE);
  const marks = setNe(noD, days[10]);
  const bid = setBuild(days[10], marks, [{ code: 'd004', kind: '単品', col: '*', reason: 'not_in_latest_fetch', raw_synced_at: 'x' }]);
  sendToRender(withD, days[10], bid);
  let r = (await compare(days[10])).result.ne;
  assert.deepEqual(clsOf(r, 'only_in_cdb:d004'), ['rule']);
  // コードの無い行が落ちた朝 = 「NE の表に無い」を根拠にしない (保持)
  const m2 = setNe(noD, days[10], { intP: { dropped_no_code: 1 } });
  sendToRender(withD, days[10], setBuild(days[10], m2, []));
  r = (await compare(days[10])).result.ne;
  assert.equal(r.held['only_in_cdb:d004'], 'ne_dropped_rows');
  disjoint(r);
});

await ta('[9] 取込の整合: dup_codes の SKU は全部保持 / C1 の形の証跡で dropped_missing_key > 0 = 構成を保持', async () => {
  const m = setNe(NE, days[10], { intP: { dup_codes: ['a001'], dup_code_count: 1 } });
  sendToRender(toMaterial(NE), days[10], setBuild(days[10], m, []));
  let r = (await compare(days[10])).result.ne;
  for (const t of ['value', 'cost', 'primary_supplier', 'kind', 'only_in_ne']) assert.equal(r.held[`${t}:a001`], 'ne_integrity:dup_code', t);
  const m2 = setNe(NE, days[10], { intS: { dropped_missing_key: 1 }, dropIntKeys: ['dropped_missing_parent', 'missing_child_parents'] });
  sendToRender(toMaterial(NE), days[10], setBuild(days[10], m2, []));
  r = (await compare(days[10])).result.ne;
  assert.equal(r.prerequisites.integrity.form, 'c1');
  assert.equal(r.held['components:s001'], 'ne_dropped_rows');
  disjoint(r);
});

await ta('[10] 台帳: HEAD が無いのに版がある = untrusted (反映待ちの判定は blocked・何回走っても HEAD を作り直さない)', async () => {
  const dir = pendingDir(tmp, RESULT_DIR);
  fs.rmSync(path.join(dir, 'HEAD.json'));
  const ne2 = clone(NE); ne2.products.find((r) => r.code === 'b002').name = '台帳が壊れた日の変更';
  const d = await day(days[11], { ne: ne2 });
  assert.equal(d.ne.pending.state, 'untrusted');
  // 名前の変更は反映待ちの判定が要る = blocked (台帳が信用できない)。b002 は [7] の税率の空欄 (ne_no_value) で案件としては明細に出る
  const nameCol = col(d.ne, 'value:b002', 'name')[0];
  assert.deepEqual([nameCol.cls, nameCol.why], ['blocked', 'pending_untrusted']);
  await compare(days[11]);
  assert.ok(!fs.existsSync(path.join(dir, 'HEAD.json')));
});

await ta('[11] 前提の欠け: stale_ne (差の一覧は出す)・build_ne_mismatch・ne_written_after_mark・no_render_master・no_integrity / ② だけ落ちても ① は残る', async () => {
  const next = '2030-01-22';
  let r = (await compare(next)).result.ne;
  assert.equal(r.blocked_reason, 'stale_ne'); assert.ok(Array.isArray(r.raw_diffs));
  const m = setNe(NE, days[11]);
  const bid = setBuild(days[11], { ...m, prev: String(Number(m.prev) + 1) }, []);
  sendToRender(toMaterial(NE), days[11], bid);
  assert.equal((await compare(days[11])).result.ne.blocked_reason, 'build_ne_mismatch');
  sendToRender(toMaterial(NE), days[11], setBuild(days[11], m, []));
  wh().prepare("INSERT INTO raw_ne_products (商品コード, 商品名, synced_at) VALUES ('zz99', '後から', 'x')").run();
  assert.equal((await compare(days[11])).result.ne.blocked_reason, 'ne_written_after_mark');
  const m3 = setNe(NE, days[11]);
  sendToRender(toMaterial(NE), days[11], setBuild(days[11], m3, []));
  wh().prepare("DELETE FROM sync_meta WHERE key = 'ne_api_products_integrity'").run();
  assert.equal((await compare(days[11])).result.ne.blocked_reason, 'no_integrity');
  const m4 = setNe(NE, days[11]); setBuild(days[11], m4, []);   // 作り直しの後に送っていない = 証跡の build が違う
  assert.equal((await compare(days[11])).result.ne.blocked_reason, 'generation_build_mismatch');
  fs.rmSync(path.join(tmp, 'company-db-evidence', days[11], 'render-master.json'), { force: true });
  assert.equal((await compare(days[11])).result.ne.blocked_reason, 'no_render_master');
  // ② だけ落ちる
  const x = await compare(days[11], { neCompare: () => { throw new Error('② が落ちた'); } });
  assert.equal(x.result.ne.verdict, 'error'); assert.match(x.result.ne.error, /② が落ちた/);
  assert.ok(['pass', 'breach'].includes(x.evidence.verdict)); assert.equal(x.evidence.ne.verdict, 'error');
  assert.match(x.line, /^⚠️ ②: 照合が落ちた .* \/ .*マスタ照合 ①/);   // 先頭が ⚠️ = daily-sync の見出しが ⚠️ になる
});

await ta('[12] ① が判定できない朝を挟んでも、反映待ちの始まりを保つ (blocked・台帳に書き写す) → 直れば match', async () => {
  fs.rmSync(pendingDir(tmp, RESULT_DIR), { recursive: true, force: true });   // 台帳を初めからにする (10 で壊したので)
  const OLD = clone(NE);
  const d23 = '2030-01-23', d24 = '2030-01-24', d25 = '2030-01-25';
  NE.products.find((r) => r.code === 'c003').name = '期限の試験';
  const a = await day(d23, { ne: NE });
  assert.deepEqual(clsOf(a.ne, 'value:c003', 'name'), ['lag']);
  const since = col(a.ne, 'value:c003', 'name')[0].pending_since;
  // 24 日: 夜の再送で古い材料 + 受信のあとで mirror が書き換えられた → ロードの材料は mismatch = ① blocked (P4 が無い)
  const tamper = () => { const m = new Database(mirrorFile); m.prepare("UPDATE mirror_products SET 商品名 = '受信のあと' WHERE 商品コード = 'e005'").run(); m.close(); };
  publishToMirror(toMaterial(OLD, (m) => { setMat(m, 'c003', '標準売価', 3000); }), at(d24, '01:00')); tamper();
  const b = await day(d24, { ne: NE });
  assert.equal(b.result.verdict, 'blocked');
  const cb = col(b.ne, 'value:c003', 'name');
  assert.ok(cb.length === 0 || cb[0].cls === 'blocked', JSON.stringify(cb));
  assert.ok(b.ne.counts.pending >= 1);
  const head = JSON.parse(fs.readFileSync(path.join(pendingDir(tmp, RESULT_DIR), 'HEAD.json'), 'utf8'));
  const ver = JSON.parse(fs.readFileSync(path.join(pendingDir(tmp, RESULT_DIR), `pending_${head.compare_run_id}.json`), 'utf8'));
  assert.ok(ver.entries.some((e) => e.key === 'value:c003' && e.col === 'name' && e.start_at === since), JSON.stringify(ver.entries));
  // 25 日: mirror = 24 日の新しい材料 → ロードが入れる → match
  const c = await day(d25, { ne: NE });
  const cn = clsOf(c.ne, 'value:c003', 'name');   // c003 は [6] の売価 "0.0" (ne_no_value) で案件としては残る = 名前の列が match
  assert.ok(cn.length === 0 || (cn.length === 1 && cn[0] === 'match'), JSON.stringify(cn));
  assert.equal(c.ne.counts.pending, 0);
  disjoint(b.ne); disjoint(c.ne);
});

await pg.close();
try { WH.getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
