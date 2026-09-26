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
const { numState, textState, comparability, KNOWN_DIFF, ABSENT, compareNe } = await import('../apps/company-db/master-compare/compare-ne.mjs');
const { writeDecisions } = await import('../apps/company-db/master-compare/decisions.mjs');
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
// 判断の台帳を書く接続は既定で同じ DB (本番の miniPC = watch_writer)。無い回 (not_configured) は [23] で
const compare = (asOf, extra = {}) => runCompare({ db, dataDir: tmp, asOf, now: at(asOf, '08:40'), syncRunId: `ds_${asOf}`, writerDb: db, cdbReadAt: at(asOf, '08:40'),
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

/** 同じ日のうちに NE・作り直し・送信をやり直して照合だけ流す (ロードは走らせない) */
async function redo(asOf, ne, material = null, reasons = []) {
  const m = setNe(ne, asOf);
  sendToRender(material || toMaterial(ne), asOf, setBuild(asOf, m, reasons));
  return (await compare(asOf)).result.ne;
}
const costRow = (code) => `(select sku_id from core.skus where code = '${code}')`;

await ta('[13] 原価: ロードが触らなかった原価は記録した保持状態と照らす (後で変わった = unexplained・同じ = 反映待ちへ) / ① の差は金額の一致より先 (load_mismatch)', async () => {
  const d26 = '2030-01-26', d27 = '2030-01-27';
  // 夜の再送で a001 の原価が不正 (-5) の材料 → ロードは原価を飛ばす (保持状態 = 100・ne・COMPLETE)
  const bad = toMaterial(NE, (m) => { setMat(m, 'a001', '原価', -5); });
  await day(d26, { ne: NE, mirrorBeforeLoad: bad });
  const dec = (await db.query(`select payload from ops.load_decisions where ingest_run_id = 'load_${d26}' and section = 'sku_costs'`)).rows[0].payload;
  assert.deepEqual(dec.skipped.find(([c]) => c === 'a001'), ['a001', 'invalid_cost', { held: { cost_jpy: 100, cost_source: 'ne', cost_status: 'COMPLETE' } }]);
  // ロードの後に原価が 200 に変わった → 保持状態と違う = unexplained (lag にしない)
  await db.query(`update core.sku_costs set cost_jpy = 200 where valid_to is null and sku_id = ${costRow('a001')}`);
  let r = (await compare(d26)).result.ne;
  const cc = col(r, 'cost:a001', 'cost')[0];
  assert.deepEqual([cc.cls, cc.A, cc.why_a], ['unexplained', 'unexplained', 'preserve_unverified']);
  // 保持状態のまま (100) で、NE と今朝の材料が 150 = 反映待ち
  await db.query(`update core.sku_costs set cost_jpy = 100 where valid_to is null and sku_id = ${costRow('a001')}`);
  const ne150 = clone(NE); ne150.products.find((x) => x.code === 'a001').cost_src = J('150');
  r = await redo(d26, ne150);
  assert.deepEqual(clsOf(r, 'cost:a001', 'cost'), ['lag']);
  // 27 日: ロードが 150 を入れた後、原価の source だけ manual に (金額は 150 のまま)。NE・材料は 200 → ① が差を出す = load_mismatch (lag にしない)
  await day(d27, { ne: ne150 });
  await db.query(`update core.sku_costs set cost_source = 'manual' where valid_to is null and sku_id = ${costRow('a001')}`);
  const ne200 = clone(NE); ne200.products.find((x) => x.code === 'a001').cost_src = J('200');
  r = await redo(d27, ne200);
  assert.deepEqual(clsOf(r, 'cost:a001', 'cost'), ['load_mismatch']);
  await db.query(`update core.sku_costs set cost_source = 'ne' where valid_to is null and sku_id = ${costRow('a001')}`);
  Object.assign(NE.products.find((x) => x.code === 'a001'), { cost_src: J('150') });
});

await ta('[14] 構成の数量が比べられない朝も台帳の子の単位を書き写す (始まりを変えない) / 数量 0 の子は判断の一覧に載る', async () => {
  const d28 = '2030-01-28';
  await day(d28, { ne: NE });
  const ne3 = clone(NE); ne3.sets.find((x) => x.parent === 's001' && x.child === 'a001').qty_src = J('3');
  let r = await redo(d28, ne3);
  assert.deepEqual(clsOf(r, 'components:s001', 'a001'), ['lag']);
  const since = col(r, 'components:s001', 'a001')[0].pending_since;
  // 同じ日に NE の数量が 0 (不正) → 親ごと比べない。子の判断情報は残る・台帳は書き写す
  const ne0 = clone(NE); ne0.sets.find((x) => x.parent === 's001' && x.child === 'a001').qty_src = J('0');
  r = await redo(d28, ne0, toMaterial(ne3));
  assert.equal(r.held['components:s001'], 'incomparable');
  const dz = r.decisions.find((d) => d.subject_key === 'components:s001' && d.child === 'a001' && d.cls === 'incomparable' && d.n_state === 'zero');
  assert.ok(dz); assert.equal(dz.c, 2);   // 今の Company DB の数量も入る
  await db.query(`update core.sku_components set qty = 4 where ${sqlComp('s001', 'a001')}`);
  const r4 = await redo(d28, ne0, toMaterial(ne3));
  assert.notEqual(r4.decisions.find((d) => d.subject_key === 'components:s001' && d.child === 'a001').approval_fingerprint, dz.approval_fingerprint);   // CDB の数量が変われば指紋も変わる
  await db.query(`update core.sku_components set qty = 2 where ${sqlComp('s001', 'a001')}`);
  const head = JSON.parse(fs.readFileSync(path.join(pendingDir(tmp, RESULT_DIR), 'HEAD.json'), 'utf8'));
  const ver = JSON.parse(fs.readFileSync(path.join(pendingDir(tmp, RESULT_DIR), `pending_${head.compare_run_id}.json`), 'utf8'));
  assert.ok(ver.entries.some((e) => e.key === 'components:s001' && e.col === 'components:a001' && e.start_at === since));
  // 数量が戻る = 同じ目標値 = 始まりはそのまま
  r = await redo(d28, ne3);
  assert.equal(col(r, 'components:s001', 'a001')[0].pending_since, since);
  await redo(d28, NE);
});

await ta('[15] 代表の仕入先: 付けなかった SKU は記録した保持状態と今が一致したときだけ held_by_load / 後で変わった・古い記録 = unexplained', async () => {
  const d29 = '2030-01-29', d30 = '2030-01-30';
  const ne9 = clone(NE); ne9.products.find((x) => x.code === 'a001').supplier = '0009';
  await day(d29, { ne: ne9 });   // 今朝の材料 = 仕入先 0009 (30 日 02:00 のロードが読む)
  await day(d30, { ne: ne9 });   // ロードは 0009 を代表にする
  // ロードが「付けられなかった」ことにする: 代表を 0001 に戻し、判断の記録を unresolved (保持状態 = 0001) に書き換える
  await db.query(`update core.supplier_skus set is_primary = false where sku_id = ${costRow('a001')}`);
  await db.query(`update core.supplier_skus set is_primary = true where sku_id = ${costRow('a001')} and supplier_id = (select supplier_id from core.suppliers where code_norm = '0001')`);
  const setUnresolved = async (entry) => db.query(`update ops.load_decisions set payload = jsonb_set(jsonb_set(payload, '{targets}', (select coalesce(jsonb_agg(t), '[]'::jsonb) from jsonb_array_elements(payload->'targets') t where t->>0 <> 'a001')),
    '{unresolved}', (payload->'unresolved') || $1::jsonb) where ingest_run_id = 'load_${d30}' and section = 'primary_suppliers'`, [JSON.stringify([entry])]);
  await setUnresolved(['a001', '0009', 'no_supplier_sku_row', { held: ['0001'] }]);
  let r = (await compare(d30)).result.ne;
  assert.deepEqual(clsOf(r, 'primary_supplier:a001', 'primary_supplier'), ['held_by_load'], JSON.stringify(col(r, 'primary_supplier:a001')));
  // ロードの後に代表が外された = 保持状態 (0001) と違う = unexplained
  await db.query(`update core.supplier_skus set is_primary = false where sku_id = ${costRow('a001')}`);
  r = (await compare(d30)).result.ne;
  assert.deepEqual(clsOf(r, 'primary_supplier:a001', 'primary_supplier'), ['unexplained']);
  // 保持状態の無い古い記録 = 確かめられない = unexplained
  await db.query(`update core.supplier_skus set is_primary = true where sku_id = ${costRow('a001')} and supplier_id = (select supplier_id from core.suppliers where code_norm = '0001')`);
  await db.query(`update ops.load_decisions set payload = jsonb_set(payload, '{unresolved}', '[["a001", "0009", "no_supplier_sku_row"]]'::jsonb) where ingest_run_id = 'load_${d30}' and section = 'primary_suppliers'`);
  r = (await compare(d30)).result.ne;
  assert.deepEqual(clsOf(r, 'primary_supplier:a001', 'primary_supplier'), ['unexplained']);
});

await ta('[16] 排他 (中身の無い .lock を消さない・自分の印だけ解放・古い .lock も自動では消さない) / 台帳が使えない朝・比べられない案件だけの朝の要約 / 承認の指紋', async () => {
  const { acquireLock } = await import('../apps/company-db/master-compare/pending.mjs');
  const { neSummary } = await import('../apps/company-db/master-compare/run.mjs');
  const { approvalFingerprint, decisionPrint } = await import('../apps/company-db/master-compare/compare-ne.mjs');
  const dir = fs.mkdtempSync(path.join(tmp, 'lock-'));
  const relA = acquireLock(dir); assert.ok(relA);
  assert.equal(acquireLock(dir), null);   // 取られている
  fs.writeFileSync(path.join(dir, '.lock'), '');   // 中身の無い .lock (作った直後に見えた) = 新しい mtime = 消さない
  assert.equal(acquireLock(dir), null);
  assert.ok(fs.existsSync(path.join(dir, '.lock')));
  relA();   // 自分の印ではない (空) = 消さない
  assert.ok(fs.existsSync(path.join(dir, '.lock')));
  const old = new Date(Date.now() - 30 * 3600 * 1000); fs.utimesSync(path.join(dir, '.lock'), old, old);
  assert.equal(acquireLock(dir), null);   // 古くても自動では消さない (回収どうしの競合を作らない)
  fs.rmSync(path.join(dir, '.lock'));      // 人が確かめて消す
  const relB = acquireLock(dir); assert.ok(relB);
  relB(); assert.ok(!fs.existsSync(path.join(dir, '.lock')));
  assert.match(neSummary({ verdict: 'breach', counts: { items: 3, held: 1 }, pending: { state: 'locked', reason: 'pending/.lock がある (900 分前)' } }), /^⚠️ ②: 反映待ちの台帳が使えない \(locked/);
  assert.match(neSummary({ verdict: 'pass', counts: { held: 2, items: 0 } }), /^ℹ️ ②: 判明した差 0・比べられない/);
  assert.equal(neSummary({ verdict: 'pass', counts: { held: 0, items: 0 } }), '✅ ②: NE との差 0');
  const base = { norm: 'x1', kind: 'single', col: 'tax_rate', problem: 'value', owner: 'load', reasonKind: 'tax_fallback', reason: { reason: 'tax_fallback', source: 'product_tax_rate', value: 0.1, build_id: 'mpb_1', raw_synced_at: 't' }, n_state: 'empty', n: null, c: 0.1, proposal: { op: 'set_ne_value', value: 0.1 } };
  const fp = approvalFingerprint(decisionPrint(base));
  assert.equal(approvalFingerprint(decisionPrint({ ...base, reason: { ...base.reason, build_id: 'mpb_2', raw_synced_at: 'u' } })), fp);   // 作り直しの ID・時刻では変わらない
  for (const v of [{ owner: 'company' }, { proposal: { op: 'set_ne_value', value: 0.08 } }, { kind: 'set' }, { c: 0.08 }, { reason: { ...base.reason, value: 0.08 } }]) assert.notEqual(approvalFingerprint(decisionPrint({ ...base, ...v })), fp, JSON.stringify(v));
  assert.notEqual(approvalFingerprint(decisionPrint(base, { tax_fallback: 2 })), fp);   // 意味の版を上げると失効
});

await ta('[17] 正規化で同じになる別の表記 (x1 と ｘ1) は潰さず保持 (回復させない) / 本物のロードが負けた表記の代表の仕入先を unresolved (保持状態 unknown) で記録する', async () => {
  const d1 = '2030-02-01', d2 = '2030-02-02';
  const neX = clone(NE);
  neX.products.push({ code: 'x1', name: '半角', supplier: '0001', handling: '取扱中', cost_src: J('10'), price_src: J('100'), tax_src: J('10') },
    { code: 'ｘ1', name: '全角', supplier: '0001', handling: '取扱中', cost_src: J('20'), price_src: J('200'), tax_src: J('10') });
  const a = await day(d1, { ne: neX });
  for (const t of ['value', 'cost', 'only_in_ne', 'kind']) assert.equal(a.ne.held[`${t}:x1`], 'norm_collision', t);
  const b = await day(d2, { ne: neX });   // ロードは先に来た x1 を採用・ｘ1 は norm_collision で飛ばす
  const dec = (await db.query(`select payload from ops.load_decisions where ingest_run_id = 'load_${d2}' and section = 'primary_suppliers'`)).rows[0].payload;
  assert.ok(dec.unresolved.some(([s, p, why, x]) => s === 'ｘ1' && p === '0001' && why === 'no_sku' && x.held === 'unknown'), JSON.stringify(dec.unresolved));
  for (const t of ['value', 'cost', 'primary_supplier', 'only_in_cdb']) { assert.equal(b.ne.held[`${t}:x1`], 'norm_collision', t); assert.ok(!b.ne.recoverable.includes(`${t}:x1`)); }
  disjoint(b.ne);
  // NE にだけ衝突がある (材料は片方の表記だけ) = NE 側で後勝ちに潰さず保持
  const neY = clone(neX); neY.products.push({ ...neY.products.find((x) => x.code === 'a001'), code: 'ａ001', name: '全角の a001' });
  const r = await redo(d2, neY, toMaterial(neX));
  for (const t of ['value', 'cost', 'kind']) { assert.equal(r.held[`${t}:a001`], 'norm_collision', t); assert.ok(!r.recoverable.includes(`${t}:a001`)); }
  // セットの表の親と同じ正規化の商品が 2 表記 (y1 と ｙ1。親は y1) = 商品の行を捨てる前に衝突として保持
  const neS = clone(neX);
  neS.products.push({ ...neS.products.find((x) => x.code === 'x1'), code: 'y1', name: 'y1 の商品' }, { ...neS.products.find((x) => x.code === 'x1'), code: 'ｙ1', name: '全角 y1' });
  neS.sets.push({ parent: 'y1', name: 'セット y1', child: 'b002', price_src: J('700'), qty_src: J('1') });
  const rs = await redo(d2, neS, toMaterial(neX));
  for (const t of ['value', 'components', 'kind', 'only_in_ne']) assert.equal(rs.held[`${t}:y1`], 'norm_collision', t);
});

await ta('[18] セット表の行が落ちた朝に NE が単品・CDB がセット = 種別に依存する案件も保持 (値・原価・仕入先・構成を回復させない)', async () => {
  const d = '2030-02-03';
  const ne = clone(NE);
  ne.sets = ne.sets.filter((r) => r.parent !== 's002');
  ne.products.push({ code: 's002', name: 'セット2', supplier: '', handling: '取扱中', cost_src: J(''), price_src: J('900'), tax_src: J('10') });
  const r = await day(d, { ne, material: toMaterial(NE), integrity: { intS: { dropped_missing_key: 1, dropped_missing_parent: 1 } } });
  for (const t of ['kind', 'value', 'cost', 'primary_supplier', 'components']) {
    assert.equal(r.ne.held[`${t}:s002`], 'ne_dropped_rows', t);
    assert.ok(!r.ne.recoverable.includes(`${t}:s002`), t);
  }
  disjoint(r.ne);
});

await ta('[19] 構成の子の削除も反映待ち: lag → 夜の再送で古い材料 → not_delivered_by_load → ロードが消せば match', async () => {
  const d1 = '2030-02-05', d2 = '2030-02-06', d3 = '2030-02-07';
  await day('2030-02-04', { ne: NE });
  const neDel = clone(NE); neDel.sets = neDel.sets.filter((r) => !(r.parent === 's001' && r.child === 'b002'));
  const a = await day(d1, { ne: neDel });
  assert.deepEqual(clsOf(a.ne, 'components:s001', 'b002'), ['lag'], JSON.stringify(col(a.ne, 'components:s001')));
  const since = col(a.ne, 'components:s001', 'b002')[0].pending_since;
  const b = await day(d2, { ne: neDel, mirrorBeforeLoad: toMaterial(NE) });
  assert.deepEqual(clsOf(b.ne, 'components:s001', 'b002'), ['not_delivered_by_load']);
  assert.equal(col(b.ne, 'components:s001', 'b002')[0].pending_since, since);
  const c = await day(d3, { ne: neDel });
  assert.deepEqual(clsOf(c.ne, 'components:s001', 'b002'), []);
  assert.equal((await db.query(`select count(*)::int as n from core.sku_components where ${sqlComp('s001', 'b002')}`)).rows[0].n, 0);
});

await ta('[20] 台帳の保存に失敗 = 要約の先頭に ⚠️・失敗 / 書きかけの印で次の回は untrusted → 失敗した回の全件 JSON から作り直す (始まりを保つ)', async () => {
  const d = '2030-02-08';
  const neL = clone(NE); neL.products.find((x) => x.code === 'f006').name = '復旧の試験';
  const a = await day(d, { ne: neL });
  const since = col(a.ne, 'value:f006', 'name')[0].pending_since;
  assert.ok(since);
  const { makeCompareRunId } = await import('../apps/company-db/master-compare/run.mjs');
  const { restoreLedger } = await import('../apps/company-db/master-compare/pending.mjs');
  const id = makeCompareRunId(at(d, '09:00'));
  const dir = pendingDir(tmp, RESULT_DIR);
  fs.mkdirSync(path.join(dir, `pending_${id}.json`));   // 版のファイルの場所にフォルダ = 書けない
  const x = await compare(d, { compareRunId: id });
  assert.equal(x.result.ne.pending.state, 'write_failed');
  assert.match(x.line, /^⚠️ ②: 反映待ちの台帳が使えない \(write_failed/);
  assert.ok(fs.existsSync(path.join(dir, 'WRITE_FAILED.json')) && fs.existsSync(path.join(dir, 'WRITE_INTENT.json')));
  const y = await compare(d);
  assert.deepEqual([y.result.ne.pending.state, y.result.ne.pending.reason], ['untrusted', 'previous_write_failed']);
  // 失敗の印だけ消しても、書きかけの印が残る = まだ untrusted (印を手で消して数え直す道は無い)
  fs.rmSync(path.join(dir, 'WRITE_FAILED.json')); fs.rmSync(path.join(dir, `pending_${id}.json`), { recursive: true });
  assert.deepEqual([(await compare(d)).result.ne.pending.reason], ['write_interrupted']);
  // 復旧 = 失敗した回の全件 JSON (書こうとした台帳の中身) から作り直す → 印が消え、反映待ちの始まりは元のまま
  const failed = JSON.parse(fs.readFileSync(path.join(tmp, x.evidence.json_path), 'utf8'));
  assert.ok(failed.ne.pending_entries.some((e) => e.key === 'value:f006' && e.start_at === since));
  const rr = restoreLedger(tmp, RESULT_DIR, { result: failed, compareRunId: makeCompareRunId(at(d, '09:30')) });
  assert.equal(rr.entries, failed.ne.pending_entries.length);
  assert.ok(!fs.existsSync(path.join(dir, 'WRITE_FAILED.json')) && !fs.existsSync(path.join(dir, 'WRITE_INTENT.json')));
  const z = (await compare(d)).result.ne;
  assert.equal(z.pending.state, 'ok');
  assert.equal(col(z, 'value:f006', 'name')[0].pending_since, since);
  assert.throws(() => restoreLedger(tmp, RESULT_DIR, { result: { ne: { pending_entries: [{ unit: 'x' }] } }, compareRunId: makeCompareRunId(new Date()) }), /形が違う/);
  await redo(d, NE);
});

await ta('[21] 本物のロードの記録: manual_kept_on_prune (数量つき) と一致したときだけ rule (manual) / 飛ばした行の source だけが変わっても unexplained', async () => {
  const d1 = '2030-02-10', d2 = '2030-02-11';
  // s002 (削除まで行く親) に manual の余分な子 b002 (数量 4) → ロードは残して manual_kept_on_prune に記録
  const a = await day(d1, { ne: NE, beforeLoad: async () => {
    await db.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) select 1, p.sku_id, c.sku_id, 4, 'manual' from core.skus p, core.skus c where p.code = 's002' and c.code = 'b002'`);
  } });
  const dec = (await db.query(`select payload from ops.load_decisions where ingest_run_id = 'load_${d1}' and section = 'set_components'`)).rows[0].payload;
  assert.ok(dec.manual_kept_on_prune.some(([p, c, q]) => q === 4), JSON.stringify(dec.manual_kept_on_prune));
  assert.deepEqual(clsOf(a.ne, 'components:s002', 'b002'), ['rule']);
  await db.query(`update core.sku_components set qty = 6 where ${sqlComp('s002', 'b002')}`);
  assert.deepEqual(clsOf((await compare(d1)).result.ne, 'components:s002', 'b002'), ['unexplained']);
  await db.query(`delete from core.sku_components where ${sqlComp('s002', 'b002')}`);
  // 飛ばした行 (数量 0 の材料 = invalid_qty・保持状態 = 今の数量 3・ne。[5] から NE の s002 × a001 は 3) の source だけが変わった = 保持状態と違う = unexplained
  const ne3 = clone(NE); ne3.sets.find((r) => r.parent === 's002' && r.child === 'a001').qty_src = J('5');
  const bad = toMaterial(ne3, (m) => { m.sets.find((r) => r.セット商品コード === 's002').数量 = 0; });
  const b = await day(d2, { ne: ne3, mirrorBeforeLoad: bad });
  assert.deepEqual(clsOf(b.ne, 'components:s002', 'a001'), ['held_by_load']);
  await db.query(`update core.sku_components set source = 'imported' where ${sqlComp('s002', 'a001')}`);
  assert.deepEqual(clsOf((await compare(d2)).result.ne, 'components:s002', 'a001'), ['unexplained']);
  await db.query(`update core.sku_components set source = 'ne' where ${sqlComp('s002', 'a001')}`);
});

await ta('[22] ロードが削除まで行かない親 (Company DB の manual の行と数量が違う) の「子が材料に無い」は削除の反映待ちにしない', async () => {
  const d = '2030-02-12';
  await day(d, { ne: NE });
  await db.query(`update core.sku_components set source = 'manual', qty = 5 where ${sqlComp('s001', 'a001')}`);   // 材料は 2 = manual_qty_mismatch で削除まで行かない
  const neDel = clone(NE); neDel.sets = neDel.sets.filter((r) => !(r.parent === 's001' && r.child === 'b002'));
  const r = await redo(d, neDel);
  const b = col(r, 'components:s001', 'b002')[0];
  assert.deepEqual([b.cls, b.why], ['unexplained', 'delete_not_expected'], JSON.stringify(b));
  await db.query(`update core.sku_components set source = 'ne', qty = 2 where ${sqlComp('s001', 'a001')}`);
  assert.deepEqual(clsOf(await redo(d, neDel), 'components:s001', 'b002'), ['lag']);   // 削除まで行く親なら反映待ち
  await redo(d, NE);
});

await ta('[23] 判断の台帳 (D1): 候補を書く / 差を残す承認だけの案件は閉じる / 直す承認は目標値に届いたときだけ完了 (n = c だけでは完了にしない)', async () => {
  const d = '2030-02-13';
  const cmp = (extra = {}) => compare(d, { writerDb: db, ...extra });
  const approve = async (fp, resolution, target = null) => Number((await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', $2, $3::jsonb, 'user', 'test@example.com') returning event_id`,
    [fp, resolution, target ? JSON.stringify(target) : null])).rows[0].event_id);
  await day(d, { ne: NE });
  let x = await cmp();
  assert.equal(x.result.ne.decisions_write, 'ok', x.result.ne.decisions_write_error);
  const nCand = (await db.query('select count(*)::int as n from ops.master_decision_candidates')).rows[0].n;
  assert.ok(nCand > 0 && nCand >= x.result.ne.decisions.length - 0);
  assert.ok(x.result.ne.decisions.every((dd) => dd.print && Array.isArray(dd.resolutions) && dd.fingerprint === dd.approval_fingerprint));
  // 差を残す: 非一致の列が全部判断の候補になっている案件を 1 つ選び、全部 accept_difference で承認 → 閉じる
  const decByKey = new Map();
  for (const dd of x.result.ne.decisions) { if (!decByKey.has(dd.subject_key)) decByKey.set(dd.subject_key, []); decByKey.get(dd.subject_key).push(dd); }
  const target = x.result.ne.items.find((it) => { const non = it.columns.filter((c) => c.cls !== 'match'); const ds = decByKey.get(it.subject_key) || [];
    return non.length && !non.some((c) => c.cls === 'incomparable' || c.cls === 'blocked') && non.every((c) => ds.some((dd) => dd.col === c.col && (dd.child ?? null) === (c.child ?? null) && dd.resolutions.includes('accept_difference'))); });
  assert.ok(target, '閉じられる案件が無い');
  for (const dd of decByKey.get(target.subject_key)) if (dd.resolutions.includes('accept_difference')) await approve(dd.fingerprint, 'accept_difference');
  x = await cmp();
  assert.equal(x.result.ne.out_of_scope[target.subject_key], 'approved_exception');
  assert.ok(!x.result.ne.items.some((it) => it.subject_key === target.subject_key));
  // 直す: f006 の税率 (NE が 0 = 不正 → 直す提案だけ) を fix_ne (目標 10% = 0.1) で承認 → NE がまだ 0 = 完了しない → NE が 10 になった = 完了
  const f6 = x.result.ne.decisions.find((dd) => dd.subject_key === 'value:f006' && dd.col === 'tax_rate');
  assert.deepEqual(f6.resolutions, ['fix_ne']);
  const eF = await approve(f6.fingerprint, 'fix_ne', { subject_key: 'value:f006', col: 'tax_rate', value: 0.1 });
  // n = c でも目標値でなければ完了しない: b002 の税率 (NE は空欄・CDB は 8%) を fix_ne (目標 10% = 0.1) で承認 → NE が 8 (= CDB) になっても完了しない
  const nc = x.result.ne.decisions.find((dd) => dd.subject_key !== target.subject_key && dd.cls === 'ne_no_value' && typeof dd.c === 'number' && ['standard_price_jpy', 'cost'].includes(dd.col) && dd.resolutions.includes('fix_ne'));
  assert.ok(nc, 'NE に値が無く CDB に値がある候補が無い');
  const eD = await approve(nc.fingerprint, 'fix_ne', { subject_key: nc.subject_key, col: nc.col, value: nc.c + 1 });   // 目標 = CDB + 1 円 (n を CDB に合わせても届かない)
  x = await cmp();
  assert.ok(!x.result.ne.decisions_done.some((z) => z.approved_event_id === eF));
  assert.equal(x.result.ne.out_of_scope[nc.subject_key], undefined);   // 直す承認では閉じない (目標に届くまで対応待ち)
  assert.ok(x.result.ne.decisions.some((dd) => dd.fingerprint === nc.fingerprint && dd.decision_status === 'approved:fix_ne'));
  const neFix = clone(NE);
  neFix.products.find((r) => r.code === 'f006').tax_src = J('10');
  { const p = neFix.products.find((r) => r.code === nc.norm); p[nc.col === 'cost' ? 'cost_src' : 'price_src'] = J(String(nc.c)); }   // NE = CDB の値 (n = c)
  const rf = await redo(d, neFix);
  assert.ok(rf.decisions_done.some((z) => z.approved_event_id === eF), JSON.stringify(rf.decisions_done));
  x = await cmp();
  assert.ok(!x.result.ne.decisions_done.some((z) => z.approved_event_id === eF));   // 書いた完了は次の回に数え直さない
  const doneRows = (await db.query(`select approved_event_id from ops.master_decision_events where kind = 'action_done'`)).rows.map((r) => Number(r.approved_event_id));
  assert.ok(doneRows.includes(eF));
  assert.ok(!doneRows.includes(eD), 'n = c でも目標値 (CDB + 1) でなければ完了しない');
  // 台帳を書けない (writer が落ちる) = 要約の先頭に ⚠️・② の判定は残る
  const bad = { query: async () => { throw new Error('writer down'); } };
  x = await compare(d, { writerDb: bad });
  assert.equal(x.result.ne.decisions_write, 'failed');
  assert.match(x.line, /^⚠️ ②: 判断の台帳を書けない/);
  // 台帳はあるのに書く接続が無い (env の入れ忘れ) = 黙って ✅ にしない (Codex #1475 R1)
  x = await compare(d, { writerDb: null });
  assert.equal(x.result.ne.decisions_write, 'not_configured');
  assert.match(x.line, /^⚠️ ②: 判断の台帳を書けない \(書く接続が無い: COMPANY_DB_WATCH_WRITER_URL\)/);
  assert.equal(x.evidence.ne.decisions_write, 'not_configured');
  await redo(d, NE);
});

await ta('[24] 直す承認の完了は信頼できる観測だけで: 値が無い・比べられない・構成の行が落ちた・種類を保留した・NE の行が落ちた回は完了にしない / 有無の目標 / 比べられない列がある案件は差を残す承認でも閉じない', async () => {
  const d = '2030-02-13';
  let seq = 0;
  const mkCand = async (subject, col, child = null) => {   // 完了の確かめは目標で決まる = 候補は合成でよい
    const fp = (++seq).toString(16).padStart(2, '0').repeat(32);
    await writeDecisions(db, { compareRunId: `mc_20300213T0000000${String(seq).padStart(2, '0')}Z_abcdef`, observedAt: '2030-02-13T00:00:00Z',
      decisions: [{ fingerprint: fp, subject_key: subject, code_norm: subject.split(':')[1], col, child, cls: 'ne_no_value', reason_kind: 'none', semantic: 'test@1',
        print: { t: fp }, resolutions: ['accept_difference', 'fix_ne', 'fix_cdb'], proposal: { op: 'decide' } }] });
    return fp;
  };
  const approve = async (fp, resolution, target = null) => Number((await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', $2, $3::jsonb, 'user', 'test@example.com') returning event_id`,
    [fp, resolution, target ? JSON.stringify(target) : null])).rows[0].event_id);
  const fixNe = async (subject, col, value, child = null) => approve(await mkCand(subject, col, child), 'fix_ne', { subject_key: subject, col, child, value });
  const doneIds = async () => new Set((await db.query(`select approved_event_id from ops.master_decision_events where kind = 'action_done'`)).rows.map((r) => Number(r.approved_event_id)));
  const runNe = async (ne, integrity = {}) => { const m = setNe(ne, d, integrity); sendToRender(toMaterial(ne), d, setBuild(d, m, [])); const r = (await compare(d)).result.ne; assert.equal(r.decisions_write, 'ok', r.decisions_write_error); return r; };
  // 値が無い ("0.0") に「空にする」目標 / 記録なし (比べられない) に空の目標 = 完了にしない (空・0 を目標の null と読まない)
  const eNo = await fixNe('value:c003', 'standard_price_jpy', null);
  const eUnk = await fixNe('cost:d004', 'cost', null);
  // 構成の子を消す目標: 構成の行が落ちた回 (C1 の形・dropped_missing_key) は「子が無い」と言えない → 落ちていない回に完了
  const eComp = await fixNe('components:s001', 'components', ABSENT, 'b002');
  const noB = clone(NE); noB.sets = noB.sets.filter((r) => !(r.parent === 's001' && r.child === 'b002'));
  const C1DROP = { intS: { dropped_missing_key: 1 }, dropIntKeys: ['dropped_missing_parent', 'missing_child_parents'] };
  let r = await runNe(noB, C1DROP);
  assert.equal(r.prerequisites.integrity.components_untrusted, true);
  let dn = await doneIds();
  assert.ok(!dn.has(eNo) && !dn.has(eUnk) && !dn.has(eComp), JSON.stringify(r.decisions_done));
  //   C2 の形で親の行が落ちた回 (dropped_missing_parent・構成そのものは信頼できる) も「子が無い」とは言えない (Codex #1475 R2)
  r = await runNe(noB, { intS: { dropped_missing_parent: 1 } });
  assert.deepEqual([r.prerequisites.integrity.form, r.prerequisites.integrity.components_untrusted, r.prerequisites.integrity.absence_untrusted], ['c2', false, true]);
  assert.ok(!(await doneIds()).has(eComp), JSON.stringify(r.decisions_done));
  r = await runNe(noB);
  dn = await doneIds();
  assert.ok(dn.has(eComp) && !dn.has(eNo) && !dn.has(eUnk), JSON.stringify(r.decisions_done));
  // 有無: NE から消す目標 (exists = false)。NE の行が落ちた回 (dropped_no_code) は「NE に無い」と言えない → 落ちていない回に完了 / 有る目標は有れば完了
  const eGone = await fixNe('only_in_cdb:d004', 'exists', false);
  const eHas = await fixNe('only_in_cdb:a001', 'exists', true);
  const noD = clone(NE); noD.products = noD.products.filter((x) => x.code !== 'd004');
  r = await runNe(noD, { intP: { dropped_no_code: 1 } });
  dn = await doneIds();
  assert.ok(!dn.has(eGone) && dn.has(eHas), JSON.stringify(r.decisions_done));
  r = await runNe(noD);
  assert.ok((await doneIds()).has(eGone), JSON.stringify(r.decisions_done));
  // 種類: セット表の行が落ちた回に NE が単品・CDB がセット = 種類の判定を保留 → 完了にしない / 落ちていない回に完了
  const eKind = await fixNe('kind:s002', 'kind', 'single');
  const k2 = clone(NE); k2.sets = k2.sets.filter((x) => x.parent !== 's002');
  k2.products.push({ code: 's002', name: 'セット2', supplier: '', handling: '取扱中', cost_src: J(''), price_src: J('900'), tax_src: J('10') });
  r = await runNe(k2, { intS: { dropped_missing_key: 1, dropped_missing_parent: 1 } });
  assert.ok(!(await doneIds()).has(eKind), JSON.stringify(r.decisions_done));
  r = await runNe(k2);
  assert.ok((await doneIds()).has(eKind), JSON.stringify(r.decisions_done));
  // 比べられない列 (知らない取扱区分) がある案件は、ほかの非一致の列を全部「差を残す」で承認しても閉じない
  //   比べられない列の候補 (税率 0 = 不正) の選べる解決は fix_ne だけ = 「差を残す」の承認は DB が拒む (閉じる前提を DB で守る)
  const e5 = clone(NE); Object.assign(e5.products.find((x) => x.code === 'e005'), { price_src: J('0.0'), tax_src: J('0') });
  r = await runNe(e5);
  const t5 = r.decisions.find((x) => x.subject_key === 'value:e005' && x.col === 'tax_rate');
  assert.deepEqual([t5.cls, t5.resolutions], ['incomparable', ['fix_ne']]);
  await assert.rejects(approve(t5.fingerprint, 'accept_difference'), /選べる解決に無い/);
  for (const dd of r.decisions.filter((x) => x.subject_key === 'value:e005' && x.resolutions.includes('accept_difference'))) await approve(dd.fingerprint, 'accept_difference');
  r = await runNe(e5);
  const it5 = r.items.find((x) => x.subject_key === 'value:e005');
  assert.ok(it5, `value:e005 が案件に無い: held=${r.held['value:e005']}`);
  const non5 = it5.columns.filter((c) => c.cls !== 'match');
  assert.ok(non5.some((c) => c.cls === 'incomparable'), JSON.stringify(non5));
  assert.ok(non5.filter((c) => c.cls !== 'incomparable').every((c) => c.decision === 'approved:accept_difference'), JSON.stringify(non5));   // 閉じない理由は比べられない列だけ
  assert.equal(r.out_of_scope['value:e005'], undefined);
  disjoint(r);
  await runNe(NE);
});

await ta('[25] 最後に一致した値 (D2): D2 の意味の一致を書く (② が ne_no_value でも)・同じ値は書かない・方向 4 種・構成・片側は有無だけ・読めない・拒まれた・古い観測・4 時間 / ② は変わらない', async () => {
  const d = '2030-02-14';
  const nameA = NE.products.find((r) => r.code === 'a001').name;   // 前の試験で変えた今の NE の名前
  const bl = async (code, col) => (await db.query('select value, since_run from ops.master_ne_baseline where code_norm = $1 and col = $2', [code, col])).rows[0] || null;
  // ② が基準で変わらない = 同じ入力で基準なしの compareNe と、基準の節・列の direction を除いて同じ
  const strip = (ne) => JSON.stringify(ne, (k, v) => (k === 'direction' || k === 'baseline' ? undefined : v));
  let sameCount = 0, diffCount = 0;
  const neCompare = (args) => { const a = compareNe(args); const b = compareNe({ ...args, baseline: null }); if (strip(a.result) === strip(b.result)) sameCount++; else diffCount++; return a; };
  const run2 = async (ne, extra = {}) => { const m = setNe(ne, d); sendToRender(toMaterial(ne), d, setBuild(d, m, [])); return compare(d, { neCompare, ...extra }); };
  const dirOf = (x, code, col) => x.result.ne.baseline.diffs.find((z) => z.code_norm === code && z.col === col)?.direction ?? null;
  await day(d, { ne: NE });
  let x = await run2(NE);
  let b = x.result.ne.baseline;
  assert.deepEqual([b.state, b.write], ['ok', 'ok'], JSON.stringify(b).slice(0, 300));
  assert.deepEqual((await bl('a001', 'name')).value, nameA);
  assert.deepEqual((await bl('s001', 'components')).value, [['a001', 2], ['b002', 1]]);
  // 同じ値の再確認では書かない (初めて一致を見た回のまま)・札は進む
  const since = (await bl('a001', 'name')).since_run;
  x = await run2(NE);
  assert.deepEqual([x.result.ne.baseline.written.inserted, x.result.ne.baseline.written.updated, x.result.ne.baseline.counts.to_write], [0, 0, 0]);   // 送りもしない (毎朝 5 万単位を送らない)
  assert.equal((await bl('a001', 'name')).since_run, since);
  assert.equal((await db.query('select compare_run_id from ops.master_ne_baseline_mark')).rows[0].compare_run_id, x.result.compare_run_id);
  // ② は ne_no_value でも D2 の意味で一致 (NE の売価 "0" と CDB の null = どちらも値なし) = 基準は null
  await db.query(`update core.skus set standard_price_jpy = null where code = 'a001'`);
  const p0 = clone(NE); p0.products.find((r) => r.code === 'a001').price_src = J('0');
  x = await run2(p0);
  assert.deepEqual(clsOf(x.result.ne, 'value:a001', 'standard_price_jpy'), ['ne_no_value']);
  assert.equal((await bl('a001', 'standard_price_jpy')).value, null);
  await db.query(`update core.skus set standard_price_jpy = 1000 where code = 'a001'`);
  x = await run2(NE);
  assert.equal((await bl('a001', 'standard_price_jpy')).value, 1000);
  // 方向: CDB だけ変わった = to_ne / 両方 = conflict / NE だけ = ne_changed / 基準なし = unknown
  await db.query(`update core.skus set name = '新A' where code = 'a001'`);
  x = await run2(NE);
  assert.equal(dirOf(x, 'a001', 'name'), 'to_ne');
  assert.equal(col(x.result.ne, 'value:a001', 'name')[0].direction, 'to_ne');   // ② の列にも参照として
  const nA = clone(NE); nA.products.find((r) => r.code === 'a001').name = '別A';
  x = await run2(nA);
  assert.equal(dirOf(x, 'a001', 'name'), 'conflict');
  await db.query(`update core.skus set name = $1 where code = 'a001'`, [nameA]);
  x = await run2(nA);
  assert.equal(dirOf(x, 'a001', 'name'), 'ne_changed');
  await db.query(`delete from ops.master_ne_baseline where code_norm = 'a001' and col = 'name'`);
  x = await run2(nA);
  assert.equal(dirOf(x, 'a001', 'name'), 'unknown');
  x = await run2(NE);
  assert.equal((await bl('a001', 'name')).value, nameA);
  // 構成: 子の並びが違っても一致 (書かない) / CDB の数量だけ変わった = 親 1 件で to_ne・② の子の列にも
  const rev = clone(NE); rev.sets = [...rev.sets].reverse();
  x = await run2(rev);
  assert.deepEqual([x.result.ne.baseline.written.inserted, x.result.ne.baseline.written.updated], [0, 0]);
  await db.query(`update core.sku_components set qty = 3 where ${sqlComp('s001', 'a001')}`);
  x = await run2(NE);
  assert.equal(x.result.ne.baseline.diffs.filter((z) => z.code_norm === 's001' && z.col === 'components').length, 1);
  assert.equal(dirOf(x, 's001', 'components'), 'to_ne');
  assert.ok(col(x.result.ne, 'components:s001', 'a001').every((c) => c.direction === 'to_ne'));
  await db.query(`update core.sku_components set qty = 2 where ${sqlComp('s001', 'a001')}`);
  // 片側だけの SKU = 有無だけ (ほかの列を「値なし」で一致させない)
  const h9 = clone(NE); h9.products.push({ code: 'h009', name: '新H', supplier: '0001', handling: '取扱中', cost_src: J('90'), price_src: J('900'), tax_src: J('10') });
  x = await run2(h9);
  assert.deepEqual(x.result.ne.baseline.diffs.filter((z) => z.code_norm === 'h009').map((z) => [z.col, z.direction]), [['exists', 'unknown']]);
  assert.equal(await bl('h009', 'name'), null);
  // 確かめられない単位 = held・書かない: NE の行が落ちた回の「NE に無い」/ 構成の行が落ちた回の構成 / 種類が違う SKU の値の列
  const noD = clone(NE); noD.products = noD.products.filter((r) => r.code !== 'd004');
  { const m = setNe(noD, d, { intP: { dropped_no_code: 1 } }); sendToRender(toMaterial(NE), d, setBuild(d, m, [])); x = await compare(d, { neCompare }); }
  assert.deepEqual(x.result.ne.baseline.diffs.filter((z) => z.code_norm === 'd004').map((z) => [z.col, z.direction, z.held]), [['exists', 'held', 'ne_dropped_rows']]);
  assert.equal((await bl('d004', 'exists')).value, true);
  { const m = setNe(NE, d, { intS: { dropped_missing_key: 1 }, dropIntKeys: ['dropped_missing_parent', 'missing_child_parents'] }); sendToRender(toMaterial(NE), d, setBuild(d, m, []));
    await db.query(`update core.sku_components set qty = 5 where ${sqlComp('s001', 'a001')}`);
    x = await compare(d, { neCompare });
    await db.query(`update core.sku_components set qty = 2 where ${sqlComp('s001', 'a001')}`); }
  assert.equal(dirOf(x, 's001', 'components'), 'held');
  assert.deepEqual((await bl('s001', 'components')).value, [['a001', 2], ['b002', 1]]);
  await db.query(`update core.skus set sku_kind = 'set' where code = 'b002'`);
  x = await run2(NE);
  await db.query(`update core.skus set sku_kind = 'single' where code = 'b002'`);
  assert.deepEqual(x.result.ne.baseline.diffs.filter((z) => z.code_norm === 'b002').map((z) => [z.col, z.direction]).sort(), [['*', 'held'], ['kind', 'to_ne']]);
  // 読めない = 方向は全部 held・書かない・⚠️・① と ② は続く
  await db.query('alter table ops.master_ne_baseline rename column value to value_x');
  try {
    x = await run2(NE);
    assert.equal(x.result.ne.baseline.state, 'unreadable');
    assert.ok(['pass', 'breach'].includes(x.result.ne.verdict) && ['pass', 'breach'].includes(x.result.verdict));
    assert.match(x.line, /^⚠️ ②: 基準を読めない/);
  } finally { await db.query('alter table ops.master_ne_baseline rename column value_x to value'); }
  // 書く時に拒まれた (読んだ後に札が動いた) = 方向は全部 held・書いた件数 0・⚠️・札は動かない
  await db.query(`update core.skus set name = '新A' where code = 'a001'`);
  const markBefore = (await db.query('select compare_run_id from ops.master_ne_baseline_mark')).rows[0].compare_run_id;
  let moved = false;
  const racer = { query: async (sql, p) => {
    if (!moved && /record_ne_baseline/.test(sql)) { moved = true; await db.query(`update ops.master_ne_baseline_mark set compare_run_id = 'mc_20300214T000000000Z_ffffff'`); }
    return db.query(sql, p);
  } };
  x = await run2(NE, { writerDb: racer });
  b = x.result.ne.baseline;
  assert.deepEqual([b.write, b.write_code, b.written.inserted, b.written.updated], ['rejected', 'mark_moved', 0, 0]);
  assert.equal(dirOf(x, 'a001', 'name'), 'held');
  assert.equal(col(x.result.ne, 'value:a001', 'name')[0].direction, 'held');
  assert.equal(b.counts.to_ne, 0);
  assert.match(x.line, /^⚠️ ②: 基準を書けない \(拒まれた: mark_moved\)/);
  assert.equal((await db.query('select compare_run_id from ops.master_ne_baseline_mark')).rows[0].compare_run_id, markBefore);
  await db.query(`update core.skus set name = $1 where code = 'a001'`, [nameA]);
  // 書く接続が無い = 読めた基準からの方向は残す・⚠️ (判断の台帳と同じ env)
  x = await run2(NE, { writerDb: null });
  assert.equal(x.result.ne.baseline.write, 'not_configured');
  assert.match(x.line, /^⚠️ ②: 判断の台帳を書けない \(書く接続が無い/);
  // 4 時間: 超え = 全部 held・書かない (⚠️ にはしない) / ちょうど = 書く → その後に古い読みの回 = stale_observation・⚠️
  x = await run2(NE, { cdbReadAt: at(d, '11:00', 0).getTime() + 1000 });
  assert.deepEqual([x.result.ne.baseline.state, x.result.ne.baseline.held_reason, x.result.ne.baseline.write], ['held', 'gap', 'skipped_held']);
  assert.doesNotMatch(x.line, /^⚠️ ②: 基準/);
  x = await run2(NE, { cdbReadAt: at(d, '11:00') });
  assert.deepEqual([x.result.ne.baseline.state, x.result.ne.baseline.write], ['ok', 'ok']);
  x = await run2(NE);   // 08:40 の読み = 札 (11:00) より古い
  assert.match(x.result.ne.baseline.held_reason, /^stale_observation: cdb_read_at/);
  assert.match(x.line, /^⚠️ ②: 基準と照らせない \(stale_observation/);
  assert.equal(diffCount, 0, '基準があると ② の結果が変わった');
  assert.ok(sameCount >= 10);
});

await pg.close();
try { WH.getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
