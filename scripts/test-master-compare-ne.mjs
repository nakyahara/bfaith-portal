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
 *  26 代表 (親子。D3b): NE で代表が付く = lag → 翌朝一致 / 代表がセット = held_by_load (判断の候補) / 保持の後に親が変わった = unexplained /
 *     人が決めた親 = rule (parent_manual・候補) / 記録の無い空 = incomparable / 自分自身 = 一致 / 最後に一致した値にも代表 / 直す承認の完了は目標の親 (親なしを含む)
 *  29 新商品の NE 登録の CSV (⑤-2b・0053): ② が最後まで走った回だけ、確かめ待ちの商品の NE の完全な取得の値を送る → 全部の列が合えば verified + NE 確認済み /
 *     ② が判定できない回 (blocked) は送らない
 *  30 持ち主が C の列 (④a で古い表に写す列。Codex ④ 設計 R0 #2・R1 H4): C ≠ NE で写し = C なら rule (company_owned・判断の一覧で NE を C の値に・方向 to_ne) /
 *     写しの後に C が変わった = rule_lag。列ごとに (名前・取扱区分・税率・標準売価・原価・代表の仕入先) direction_unknown にしない
 *  31 持ち主が C の列は NE の値が空・0・null・不正でも company_owned (NE の状態は残す)
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
const { numState, textState, comparability, KNOWN_DIFF, ABSENT, compareNe, nameIsCode, readRegistrations } = await import('../apps/company-db/master-compare/compare-ne.mjs');
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
  // 代表 (D3b): 既定は NE が空文字を返した = 明示の親なし (rep_src = '""')
  const p = (code, name, sup, cost, price, tax) => ({ code, name, supplier: sup, handling: '取扱中', cost_src: J(String(cost)), price_src: J(String(price)), tax_src: J(String(tax)), rep: '', rep_src: J('') });
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
  // 代表商品コード = 送る形 (readMasterMaterial) と同じ: 値 = そのまま (小文字) / 空で元の値が "" = '' / それ以外 = NULL
  const repOf = (r) => (r.rep ? String(r.rep).toLowerCase() : r.rep_src === J('') ? '' : null);
  const products = ne.products.map((r) => ({ 商品コード: r.code, 商品名: r.name || r.code, 商品区分: '単品', 取扱区分: r.handling, 標準売価: num(r.price_src), 原価: num(r.cost_src),
    原価ソース: 'NE', 原価状態: num(r.cost_src) ? 'COMPLETE' : 'MISSING', 消費税率: num(r.tax_src) ? num(r.tax_src) / 100 : null, 税区分: num(r.tax_src) === 8 ? 'REDUCED_8' : 'STANDARD_10', 仕入先コード: r.supplier,
    代表商品コード: repOf(r) }));
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
  const g = buildMaterialGeneration({ products: mat.products, set_components: mat.sets, now: when, productsSemantics: mat.semantics === undefined ? { rep: 'src1' } : mat.semantics });
  saveMaterialSnapshot({ dataDir: tmp, generation: g, products: mat.products, set_components: mat.sets, nowMs: when.getTime() });
  const m = new Database(mirrorFile);
  try {
    m.exec(MIRROR_PRODUCTS_DDL); m.exec(MIRROR_SET_COMPONENTS_DDL);
    m.exec(`CREATE TABLE IF NOT EXISTS mirror_material_generations (entity TEXT PRIMARY KEY, generation_id TEXT NOT NULL, content_hash TEXT NOT NULL, row_count INTEGER NOT NULL,
      source_complete_at TEXT, created_at TEXT, received_at TEXT NOT NULL)`);
    try { m.exec('ALTER TABLE mirror_material_generations ADD COLUMN semantics TEXT'); } catch { /* もうある */ }
    m.transaction(() => {
      m.exec('DELETE FROM mirror_products; DELETE FROM mirror_set_components; DELETE FROM mirror_material_generations');
      const put = (table, cols, rows) => { const st = m.prepare(`INSERT INTO ${table} (${[...cols, 'updated_at'].map((c) => `"${c}"`).join(', ')}) VALUES (${[...cols, 'updated_at'].map(() => '?').join(', ')})`); for (const r of rows) st.run(...cols.map((c) => r[c] ?? null), 'x'); };
      put('mirror_products', MATERIAL_COLUMNS.products, projectMaterialRows('products', mat.products));
      put('mirror_set_components', MATERIAL_COLUMNS.set_components, projectMaterialRows('set_components', mat.sets));
      for (const e of ['products', 'set_components']) {
        const d = materialDigest(e, m.prepare(`SELECT * FROM mirror_${e}`).all());
        m.prepare('INSERT INTO mirror_material_generations (entity, generation_id, content_hash, row_count, source_complete_at, created_at, received_at, semantics) VALUES (?,?,?,?,?,?,?,?)')
          .run(e, g.generation_id, d.content_hash, d.row_count, null, g.created_at, 'x', e === 'products' && g.products.semantics ? JSON.stringify(g.products.semantics) : null);
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
  const ip = d.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 取扱区分, 原価, 売価, 消費税率, synced_at, 原価_src, 売価_src, 消費税率_src, 代表商品コード, 代表商品コード_src) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)');
  for (const r of ne.products) ip.run(r.code, r.name, r.supplier, r.handling, 0, 0, 0, ts, r.cost_src, r.price_src, r.tax_src, r.rep ? String(r.rep).toLowerCase() : '', r.rep_src ?? null);
  const is = d.prepare('INSERT INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at, セット販売価格_src, 数量_src) VALUES (?,?,?,?,?,?,?,?)');
  for (const r of ne.sets) is.run(r.parent, r.name, 0, r.child, 1, ts, r.price_src, r.qty_src);
  const meta = (k) => d.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value;
  const up = (k, v) => d.prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(k, v, ts);
  up('ne_api_products_complete_at', ts); up('ne_api_products_complete_count', String(ne.products.length)); up('ne_api_products_complete_rev', meta('ne_raw_products_rev'));
  up('ne_api_setproducts_complete_at', ts); up('ne_api_setproducts_complete_count', String(ne.sets.length)); up('ne_api_setproducts_complete_rev', meta('ne_raw_setproducts_rev'));
  up('ne_api_setproducts_complete_parents', String(new Set(ne.sets.map((r) => r.parent)).size));
  const s = { ...INT_S, ...intS }; for (const k of dropIntKeys) delete s[k];
  up('ne_api_products_integrity', JSON.stringify({ ...INT_P, ...intP })); up('ne_api_setproducts_integrity', JSON.stringify(s));
  // 取得の件数 (#1642 の ne-fetch-counts.js の形)。この試験の NE は落とした行・重なりの無いきれいな取得 (本物の取込を通す試験は [38])
  const fc = (kind, n, rev, detail, notes) => JSON.stringify({ version: 'fc1', kind, fetch_fingerprint: 'a'.repeat(64), complete_at: ts, complete_rev: Number(rev),
    started_at: `${ts.replace(' ', 'T')}.000Z`, finished_at: `${ts.replace(' ', 'T')}.000Z`, fetched_rows: n, write_attempts: n, stored_rows: n,
    dropped_no_code: 0, dropped_missing_fields: 0, dropped_missing_detail: detail, notes, page_limit: 1000, pages: 1, page_rows: [n], last_page_rows: n });
  up('ne_api_products_fetch_counts', fc('products', ne.products.length, meta('ne_raw_products_rev'), {}, {}));
  up('ne_api_setproducts_fetch_counts', fc('setproducts', ne.sets.length, meta('ne_raw_setproducts_rev'), { set_goods_detail_goods_id: 0 }, { quantity_defaulted_rows: 0 }));
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
const nightly = async (asOf, ownership = null) => { const r = await runInitialLoad(db, buildPlanFromRender({ dataDir: tmp, log: quiet, now: at(asOf, '02:00') }), { log: quiet, runId: `load_${asOf}`, host: 'render-nightly', now: at(asOf, '02:00'), ...(ownership ? { ownership } : {}) }); assert.equal(r.ok, true, r.error); return r; };
// 判断の台帳を書く接続は既定で同じ DB (本番の miniPC = watch_writer)。無い回 (not_configured) は [23] で
const compare = (asOf, extra = {}) => runCompare({ db, dataDir: tmp, asOf, now: at(asOf, '08:40'), syncRunId: `ds_${asOf}`, writerDb: db, cdbReadAt: at(asOf, '08:40'),
  write: (d, n, p) => writeEvidence(d, n, p, { now: at(asOf, '08:40'), warn: quiet }), ...extra });
/**
 * 1 日を回す。mirrorBeforeLoad = 夜の再送 (ロードの前に mirror を別の材料にする) / beforeLoad = ロードの前に Company DB を書き換える
 */
async function day(asOf, { ne, material = null, reasons = [], mirrorBeforeLoad = null, beforeLoad = null, status = 'recorded', integrity = {}, spellings = null, ownership = null, compareExtra = {} } = {}) {
  if (mirrorBeforeLoad) publishToMirror(mirrorBeforeLoad, at(asOf, '01:00'));
  if (beforeLoad) await beforeLoad();
  await nightly(asOf, ownership);
  const marks = setNe(ne, asOf, integrity);
  // NE のコードの元の書き方 (③b-1b): 取得の世代 (完了の印) に書き方と「集め終えた印」を付ける (ne-api.js と同じ関数)
  if (spellings) { if (spellings.products) WH.writeCodeSpellings('products', marks.at, spellings.products); if (spellings.sets) WH.writeCodeSpellings('sets', marks.at, spellings.sets); }
  const buildId = setBuild(asOf, marks, reasons);
  sendToRender(material || toMaterial(ne), asOf, buildId, status);
  const r = await compare(asOf, compareExtra);
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
  // c003 = NE の売価 0 (値が無い)。NE の 0 と社内の 0 は差にしない (2026-10-05) = 社内に値 (3000) がある形で試す (最後に 0 に戻す)
  await db.query(`update core.skus set standard_price_jpy = 3000 where code = 'c003'`);
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
  assert.match(x.line, /^⚠️ 新商品の入口を閉じられない \(前日の許可が残りうる\): 書けない \(writer down\) \/ ⚠️ ②: 判断の台帳を書けない/);   // 書く接続が全部落ちる = 関数の有無も確かめられない = 入口の ⚠️ も先頭に (#1641 Codex R3 High)
  // 台帳はあるのに書く接続が無い (env の入れ忘れ) = 黙って ✅ にしない (Codex #1475 R1)
  x = await compare(d, { writerDb: null });
  assert.equal(x.result.ne.decisions_write, 'not_configured');
  // 0058 がある DB (広げる道 PR-1 の後) = 入口を閉じる関数も同じ書く接続で呼ぶ = 入口の ⚠️ が先頭・その直後に判断の台帳の ⚠️
  assert.match(x.line, /^⚠️ 新商品の入口を閉じられない \(前日の許可が残りうる\): 書く接続が無い \/ ⚠️ ②: 判断の台帳を書けない \(書く接続が無い: COMPANY_DB_WATCH_WRITER_URL\)/);
  assert.equal(x.evidence.ne.decisions_write, 'not_configured');
  await db.query(`update core.skus set standard_price_jpy = 0 where code = 'c003'`);
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
  assert.ok(!x.result.ne.baseline.diffs.some((z) => z.code_norm === 'a001'), '商品の表の行だけが落ちた回に単品を保留した');   // 種類の判断には効かない
  // セットの表の行が落ちた回 (C2 の形) = 単品に見える SKU は本当はセットかもしれない = 種類と値の列を書かない (有無は書く)
  { const m = setNe(NE, d, { intS: { dropped_missing_parent: 1 } }); sendToRender(toMaterial(NE), d, setBuild(d, m, []));
    await db.query(`update core.skus set name = '新A' where code = 'a001'`);
    x = await compare(d, { neCompare });
    await db.query(`update core.skus set name = $1 where code = 'a001'`, [nameA]); }
  assert.deepEqual(x.result.ne.baseline.diffs.filter((z) => z.code_norm === 'a001').map((z) => [z.col, z.direction]), [['*', 'held']]);
  assert.equal(col(x.result.ne, 'value:a001', 'name')[0].direction, 'held');
  assert.equal((await bl('a001', 'kind')).value, 'single');
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
  assert.equal(col(x.result.ne, 'kind:b002', 'kind')[0].direction, 'to_ne');   // ② の kind の列は kind の方向 (SKU 全部の保持で上書きしない)
  assert.ok(x.result.ne.baseline.counts.held_skus >= 1);
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
  await db.query(`update core.skus set name = '新A' where code = 'a001'`);
  x = await run2(NE, { writerDb: null });
  await db.query(`update core.skus set name = $1 where code = 'a001'`, [nameA]);
  assert.equal(x.result.ne.baseline.write, 'not_configured');
  assert.equal(dirOf(x, 'a001', 'name'), 'to_ne');
  assert.match(x.line, /^⚠️ 新商品の入口を閉じられない \(前日の許可が残りうる\): 書く接続が無い \/ ⚠️ ②: 判断の台帳を書けない \(書く接続が無い/);   // 0058 がある DB = 入口の ⚠️ が先頭
  // 4 時間: 超え = 全部 held・書かない (⚠️ にはしない) / ちょうど = 書く → その後に古い読みの回 = stale_observation・⚠️
  x = await run2(NE, { cdbReadAt: at(d, '11:00', 0).getTime() + 1000 });
  assert.deepEqual([x.result.ne.baseline.state, x.result.ne.baseline.held_reason, x.result.ne.baseline.write], ['held', 'gap', 'skipped_held']);
  { const k = x.result.ne.baseline.counts; assert.deepEqual([k.match, k.held, k.to_write], [0, k.units, 0], JSON.stringify(k)); }   // 回全体の保留 = 一致も数えず全部 held (Codex #1479 マージ後 Low 1)
  assert.doesNotMatch(x.line, /^⚠️ ②: 基準/);
  assert.match(x.line, /基準は照らさず/);   // ⚠️ にはしないが、要約で見える
  x = await run2(NE, { cdbReadAt: at(d, '11:00') });
  assert.deepEqual([x.result.ne.baseline.state, x.result.ne.baseline.write], ['ok', 'ok']);
  x = await run2(NE);   // 08:40 の読み = 札 (11:00) より古い
  assert.match(x.result.ne.baseline.held_reason, /^stale_observation: cdb_read_at/);
  assert.match(x.line, /^⚠️ ②: 基準と照らせない \(stale_observation/);
  assert.equal(diffCount, 0, '基準があると ② の結果が変わった');
  assert.ok(sameCount >= 10);
});

await ta('[26] 代表 (親子。D3b): lag → 一致 / 代表がセット = held_by_load / 保持の後に変わった = unexplained / 人が決めた = rule (parent_manual) / 不明 = incomparable / 自分自身 = 一致 / 基準 / 完了', async () => {
  const NEp = clone(baseNe());
  const setRep = (code, rep, src = J(rep ?? '')) => { const r = NEp.products.find((x) => x.code === code); r.rep = rep; r.rep_src = src; };
  const parentCols = (ne, code) => col(ne, `parent:${code}`, 'parent');
  const asParentWriter = async (fn) => { await db.exec('begin'); try { await db.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())"); await fn(); await db.exec('commit'); } catch (e) { await db.exec('rollback'); throw e; } };
  const pidOfCode = async (code) => Number((await db.query('select product_id from core.skus where code = $1', [code])).rows[0].product_id);
  const d0 = '2030-02-19', d1 = '2030-02-20', d2 = '2030-02-21', d3 = '2030-02-22', d4 = '2030-02-23', d5 = '2030-02-24';
  let x = await day(d0, { ne: NEp });
  assert.ok(!['blocked', 'error'].includes(x.ne.verdict), x.ne.blocked_reason);   // (前の試験が残した名前・原価の反映待ちはここでは見ない)
  assert.ok(x.ne.recoverable.includes('parent:a001'), JSON.stringify(col(x.ne, 'parent:a001')));   // 親なし同士 = 一致
  // 1 日目: NE で b002 に代表 (名札 grp1) が付く・e005 は自分自身 = 親なし → 昨夜のロードは前の材料 = lag (反映待ち) / e005 は一致
  setRep('b002', 'GRP1'); setRep('e005', 'e005');
  x = await day(d1, { ne: NEp });
  assert.deepEqual(parentCols(x.ne, 'b002').map((c) => [c.cls, c.n, c.c, c.t_today]), [['lag', 'grp1', null, 'grp1']]);
  assert.ok(x.ne.recoverable.includes('parent:e005'));
  assert.equal(x.ne.out_of_scope['parent:s001'], 'set_not_compared');   // セット同士 = 代表は比べない (開いていた案件を閉じる)
  // 2 日目: ロードが付けた = 一致 (案件が閉じる)
  x = await day(d2, { ne: NEp });
  assert.ok(x.ne.recoverable.includes('parent:b002'), JSON.stringify(parentCols(x.ne, 'b002')));
  assert.equal((await db.query("select pp.display_code as d, p.parent_set_by as by from core.skus s join core.products p on p.product_id = s.product_id join core.products pp on pp.product_id = p.parent_product_id where s.code = 'b002'")).rows[0].by, 'load');
  // 3 日目: c003 の代表 = セットのコード (s001) → 今朝は lag / 4 日目: ロードは付けない (rep_not_single) = held_by_load = 判断の候補
  setRep('c003', 's001');
  x = await day(d3, { ne: NEp });
  assert.equal(parentCols(x.ne, 'c003')[0].cls, 'lag');
  x = await day(d4, { ne: NEp });
  const hc = parentCols(x.ne, 'c003')[0];
  assert.deepEqual([hc.cls, hc.explained?.reason_code], ['held_by_load', 'rep_not_single'], JSON.stringify(hc));
  const dc = x.ne.decisions.find((d) => d.subject_key === 'parent:c003');
  assert.deepEqual([dc.col, dc.cls, dc.reason_kind, dc.print.reason?.reason_code, dc.resolutions], ['parent', 'held_by_load', 'held_by_load', 'rep_not_single', ['fix_input', 'accept_difference']]);
  // 保持の後に人が親を変えた (記録した親・帰属と違う) = unexplained (同じ回をもう一度照らす)
  const g1 = Number((await db.query("select product_id from core.products where display_code = 'GRP1' or display_code = 'grp1' limit 1")).rows[0].product_id);
  await asParentWriter(async () => db.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [await pidOfCode('c003'), g1]));
  let y = await compare(d4);
  assert.deepEqual([parentCols(y.result.ne, 'c003')[0].cls, parentCols(y.result.ne, 'c003')[0].why_a], ['unexplained', 'held_state_changed']);
  // 5 日目: 人が決めた親 (manual) はロードが保持 = rule (parent_manual) = 判断の候補 (差を残す / NE を直す / CDB を直す)
  // d004 は NE の空に元の値の記録が無い = 不明 = incomparable (保持)
  setRep('d004', '', null);
  x = await day(d5, { ne: NEp });
  const mc = parentCols(x.ne, 'c003')[0];
  assert.deepEqual([mc.cls, mc.explained?.reason], ['rule', 'parent_manual'], JSON.stringify(mc));
  const dm = x.ne.decisions.find((d) => d.subject_key === 'parent:c003');
  assert.deepEqual([dm.reason_kind, dm.resolutions, dm.proposal.op], ['parent_manual', ['accept_difference', 'fix_ne', 'fix_cdb'], 'decide_manual_priority']);
  assert.equal(x.ne.held['parent:d004'], 'incomparable');
  // 最後に一致した値 (0037): 代表の単位が書かれる・差には方向
  const bl = (await db.query("select count(*)::int as n, count(*) filter (where value = 'null'::jsonb)::int as none from ops.master_ne_baseline where col = 'parent'")).rows[0];
  assert.ok(bl.n >= 4 && bl.none >= 3, JSON.stringify(bl));
  assert.equal((await db.query("select value #>> '{}' as v from ops.master_ne_baseline where col = 'parent' and code_norm = 'b002'")).rows[0].v, 'grp1');
  assert.ok(x.ne.baseline.diffs.some((d) => d.code_norm === 'c003' && d.col === 'parent'), JSON.stringify(x.ne.baseline.diffs.filter((d) => d.col === 'parent')));
  assert.ok(x.ne.baseline.diffs.some((d) => d.code_norm === 'd004' && d.col === 'parent' && d.direction === 'held'));
  disjoint(x.ne);
  // 直す承認の完了: NE を「親なし」に直す (目標 null)。NE が明示の空を返したら完了 / 記録の無い空 (不明) では完了にしない
  const ev = Number((await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'test@example.com') returning event_id`,
    [dm.fingerprint, JSON.stringify({ subject_key: 'parent:c003', col: 'parent', value: null })])).rows[0].event_id);
  setRep('c003', '', null);   // 空だが元の値の記録が無い = 不明
  x = await day('2030-02-25', { ne: NEp });
  assert.ok(!x.ne.decisions_done.some((z) => z.approved_event_id === ev), '不明な観測で親なしの目標を完了にした');
  setRep('c003', '');         // NE が空文字を返した = 明示の親なし
  x = await day('2030-02-26', { ne: NEp });
  assert.ok(x.ne.decisions_done.some((z) => z.approved_event_id === ev), JSON.stringify(x.ne.decisions_done));
  // NE で代表を外した (明示の空) = 材料の意味の版 (src1) で t_today も親なし = 反映待ち (lag)。版を渡さないと PRESERVE = 材料に値が無いと読み違える
  //   次の朝: この試験の世代は完了した NE の取得の印が無い (source_complete_at = null) = 外せる材料でない = ロードは保持 (material_untrusted) = held_by_load
  // 親はあるのにコードが読めない (親の display_code が空) = 親なしに潰さず保持 (② も基準も)
  const blank = Number((await db.query("insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, null, '名前だけ', 'active', 'human', 't') returning product_id")).rows[0].product_id);
  await asParentWriter(async () => db.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [await pidOfCode('f006'), blank]));
  // 読めない親の回は、NE を「親なし」に直す承認も完了にしない (NE は親なし = 目標と同じでも。Codex #1490 R1 Medium)
  const evU = Number((await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_ne', $2::jsonb, 'user', 'test@example.com') returning event_id`,
    [dm.fingerprint, JSON.stringify({ subject_key: 'parent:f006', col: 'parent', value: null })])).rows[0].event_id);
  y = await compare('2030-02-26');
  assert.equal(y.result.ne.held['parent:f006'], 'cdb_parent_unresolved');
  assert.ok(!y.result.ne.decisions_done.some((z) => z.approved_event_id === evU), '読めない親の回に完了を書いた');
  assert.ok(y.result.ne.baseline.diffs.some((d) => d.code_norm === 'f006' && d.col === 'parent' && d.direction === 'held' && d.held === 'cdb_parent_unresolved'));
  await asParentWriter(async () => db.query('update core.products set parent_product_id = null, parent_set_by = null where product_id = $1', [await pidOfCode('f006')]));
  setRep('b002', '');
  x = await day('2030-02-27', { ne: NEp });
  assert.deepEqual(parentCols(x.ne, 'b002').map((c) => [c.cls, c.n, c.c, c.t_today]), [['lag', null, 'grp1', null]], JSON.stringify(parentCols(x.ne, 'b002')));
  x = await day('2030-02-28', { ne: NEp });
  assert.deepEqual([parentCols(x.ne, 'b002')[0].cls, parentCols(x.ne, 'b002')[0].explained?.reason_code], ['held_by_load', 'material_untrusted']);
  // 保持の後に CDB の親が「昨夜の材料と同じ値」(親なし) に変わった = 記録と違う = unexplained (c == t_load の applied より先。Codex #1490 R1 Low)
  await asParentWriter(async () => db.query('update core.products set parent_product_id = null, parent_set_by = null where product_id = $1', [await pidOfCode('b002')]));
  setRep('b002', 'GRP1');   // NE は今朝また付いた (n ≠ c)
  { const mk = setNe(NEp, '2030-02-28'); sendToRender(toMaterial(NEp), '2030-02-28', setBuild('2030-02-28', mk, [])); }
  y = await compare('2030-02-28');
  assert.deepEqual([parentCols(y.result.ne, 'b002')[0].cls, parentCols(y.result.ne, 'b002')[0].why_a, parentCols(y.result.ne, 'b002')[0].t_load], ['unexplained', 'held_state_changed', null]);
});

await ta('[27] 代表の境界 (Codex #1490 R1 Low): PRESERVE × manual = rule / 不明 × PRESERVE = applied の後の分類 / lag の途中に blocked を挟んでも期限は 1 日目のまま / 開いた案件の単品がセットに変わる = 種類違いの間は保持 → セット同士で out_of_scope', async () => {
  const NEq = clone(baseNe());
  const setRep = (code, rep, src = J(rep ?? '')) => { const r = NEq.products.find((x) => x.code === code); r.rep = rep; r.rep_src = src; };
  const pc = (ne, code) => col(ne, `parent:${code}`, 'parent');
  const asParentWriter = async (fn) => { await db.exec('begin'); try { await db.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())"); await fn(); await db.exec('commit'); } catch (e) { await db.exec('rollback'); throw e; } };
  const pidOfCode = async (code) => Number((await db.query('select product_id from core.skus where code = $1', [code])).rows[0].product_id);
  const oldSender = (ne) => ({ ...toMaterial(ne), semantics: null });   // 意味の版の無い古い送り手 (空 = 不明)
  // X1: f006 に代表 → 反映待ち / X2: ロードが付ける・材料は古い送り手 → d004 を人が付ける
  setRep('f006', 'GRP1');
  let x = await day('2030-03-01', { ne: NEq });
  assert.equal(pc(x.ne, 'f006')[0].cls, 'lag');
  x = await day('2030-03-02', { ne: NEq, material: oldSender(NEq) });
  assert.ok(x.ne.recoverable.includes('parent:f006'), JSON.stringify(pc(x.ne, 'f006')));
  const g1 = Number((await db.query("select product_id from core.products where display_code = 'grp1'")).rows[0].product_id);
  await asParentWriter(async () => db.query("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [await pidOfCode('d004'), g1]));
  // X3: NE で f006 の代表を外す (明示の空)。昨夜の材料も今朝の材料も古い送り手 = 空は不明 (PRESERVE)
  //   d004 = manual (記録と同じ) × t_load PRESERVE = rule (parent_manual) / f006 = 昨夜は材料どおり (applied) → 今朝の材料に値が無い = unexplained
  setRep('f006', '');
  x = await day('2030-03-03', { ne: NEq, material: oldSender(NEq) });
  assert.deepEqual([pc(x.ne, 'd004')[0].cls, pc(x.ne, 'd004')[0].explained?.reason, pc(x.ne, 'd004')[0].t_load], ['rule', 'parent_manual', '(ロードは触らない)'], JSON.stringify(pc(x.ne, 'd004')));
  assert.deepEqual([pc(x.ne, 'f006')[0].cls, pc(x.ne, 'f006')[0].why], ['unexplained', 'material_has_no_value'], JSON.stringify(pc(x.ne, 'f006')));
  // X4: ロードは代表が不明 = 保持 (rep_unknown)。記録と今が同じ × t_load PRESERVE = A は applied → 今朝の材料に値が無い = unexplained
  x = await day('2030-03-04', { ne: NEq, material: oldSender(NEq) });
  assert.deepEqual([pc(x.ne, 'f006')[0].A, pc(x.ne, 'f006')[0].cls, pc(x.ne, 'f006')[0].why], ['applied', 'unexplained', 'material_has_no_value'], JSON.stringify(pc(x.ne, 'f006')));
  // lag の途中に blocked: Y1 = a001 に代表 (lag) → Y2 = 夜に古い材料を読み・② は blocked → Y3 = また古い材料 = not_delivered_by_load・始まりは Y1 のまま
  const before = clone(NEq);
  setRep('a001', 'GRP2');
  x = await day('2030-03-05', { ne: NEq });
  const start = pc(x.ne, 'a001')[0].pending_since;
  assert.equal(pc(x.ne, 'a001')[0].cls, 'lag'); assert.ok(start);
  x = await day('2030-03-06', { ne: NEq, mirrorBeforeLoad: toMaterial(before), integrity: { dropIntKeys: ['pair_dups'] } });
  assert.equal(x.ne.blocked_reason, 'no_integrity');
  x = await day('2030-03-07', { ne: NEq, mirrorBeforeLoad: toMaterial(before) });
  assert.deepEqual([pc(x.ne, 'a001')[0].cls, pc(x.ne, 'a001')[0].pending_since], ['not_delivered_by_load', start], JSON.stringify(pc(x.ne, 'a001')));
  // 単品 → セット: h008 (単品・代表 GRP3) → 開いた案件 (代表を GRP4 に) → NE でセットに (種類違い = 保持) → ロードがセットに = out_of_scope
  NEq.products.push({ code: 'h008', name: '単品H', supplier: '0001', handling: '取扱中', cost_src: J('800'), price_src: J('8000'), tax_src: J('10'), rep: 'GRP3', rep_src: J('GRP3') });
  await day('2030-03-08', { ne: NEq });
  x = await day('2030-03-09', { ne: NEq });
  assert.ok(x.ne.recoverable.includes('parent:h008'), JSON.stringify(pc(x.ne, 'h008')));
  NEq.products.find((r) => r.code === 'h008').rep = 'GRP4'; NEq.products.find((r) => r.code === 'h008').rep_src = J('GRP4');
  x = await day('2030-03-10', { ne: NEq });
  assert.equal(pc(x.ne, 'h008')[0].cls, 'lag');
  NEq.products = NEq.products.filter((r) => r.code !== 'h008');
  NEq.sets.push({ parent: 'h008', name: 'セットH', child: 'a001', price_src: J('8000'), qty_src: J('1') });
  x = await day('2030-03-11', { ne: NEq });
  assert.equal(x.ne.held['parent:h008'], 'kind_mismatch', JSON.stringify({ held: x.ne.held['parent:h008'], oos: x.ne.out_of_scope['parent:h008'] }));
  x = await day('2030-03-12', { ne: NEq });
  assert.equal(x.ne.out_of_scope['parent:h008'], 'set_not_compared', JSON.stringify(x.ne.items.filter((i) => i.norm === 'h008').map((i) => i.subject_key)));
  disjoint(x.ne);
  // 代表の直す承認は、案件を対象外・保持にする回には完了にしない (Codex #1490 R2 Medium): 今朝の材料で例外 (e005) / NE に無い (b002)
  const fps = (await db.query('select fingerprint from ops.master_decision_candidates order by fingerprint limit 2')).rows.map((r) => r.fingerprint);
  const apv = async (fp, target) => Number((await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'fix_cdb', $2::jsonb, 'user', 'test@example.com') returning event_id`,
    [fp, JSON.stringify(target)])).rows[0].event_id);
  const eE = await apv(fps[0], { subject_key: 'parent:e005', col: 'parent', value: null });   // 今の CDB の親 = なし (目標と同じ)
  const bNow = (await db.query("select core.norm_code(pp.display_code) as d from core.skus s join core.products p on p.product_id = s.product_id left join core.products pp on pp.product_id = p.parent_product_id where s.code = 'b002'")).rows[0].d ?? null;
  const eB = await apv(fps[1], { subject_key: 'parent:b002', col: 'parent', value: bNow });   // 目標 = 今の CDB の親 (壊した版なら完了になる)
  const NEr = clone(NEq); NEr.products = NEr.products.filter((r) => r.code !== 'b002');
  x = await day('2030-03-13', { ne: NEr, material: toMaterial(NEr, (m) => setMat(m, 'e005', '商品区分', '例外')) });
  assert.equal(x.ne.out_of_scope['parent:e005'], 'exception_item');
  assert.equal(x.ne.held['parent:b002'], 'not_in_ne');
  assert.ok(!x.ne.decisions_done.some((z) => z.approved_event_id === eE || z.approved_event_id === eB), JSON.stringify(x.ne.decisions_done));
});

await ta('[28] NE の元のコード (③b-1b): 書き方を集め終えた印が両方の側にある回だけ Company DB に書く / 片方だけ = 書かない (前の回のまま) / 別の種類 (単品とセットの子) の書き方違いも衝突 / 名札は別の名前空間', async () => {
  const NEc = clone(baseNe());
  const sp = (o) => new Map(Object.entries(o).map(([k, v]) => [k, new Set(v)]));
  const mark = async () => (await db.query('select compare_run_id from ops.master_ne_code_mark')).rows.map((r) => r.compare_run_id);
  const codes = async () => Object.fromEntries((await db.query('select code_norm, kind, state, ne_code from ops.master_ne_codes order by 1, 2')).rows.map((r) => [`${r.kind}|${r.code_norm}`, [r.state, r.ne_code]]));
  // 書き方をまだ集めていない回 = 書かない
  let x = await day('2030-04-01', { ne: NEc });
  assert.deepEqual([x.ne.ne_codes.state, x.ne.ne_codes.reason, x.ne.ne_codes.write], ['unavailable', 'not_collected', 'skipped_not_collected']);
  assert.deepEqual(await mark(), []);
  const full = {
    products: { single: sp({ a001: ['A001'], b002: ['b002'], c003: ['C003', 'c003'], d004: ['d004'], e005: ['e005'], f006: ['F006'] }), rep: sp({ grp1: ['GRP1'], grp2: ['Grp2', 'GRP2'] }) },
    sets: { set: sp({ s001: ['S001'], s002: ['s002'] }), child: sp({ a001: ['a001'], b002: ['b002'] }), set_rep: sp({ grp2: ['grp2'] }) },
  };
  x = await day('2030-04-02', { ne: NEc, spellings: full });
  assert.equal(x.ne.ne_codes.write, 'ok', JSON.stringify(x.ne.ne_codes));
  assert.deepEqual(await mark(), [x.result.compare_run_id]);
  const c = await codes();
  assert.deepEqual([c['product|a001'], c['product|b002'], c['product|c003'], c['product|f006'], c['product|s001'], c['rep|grp1'], c['rep|grp2']],
    [['collided', null], ['ok', 'b002'], ['collided', null], ['ok', 'F006'], ['ok', 'S001'], ['ok', 'GRP1'], ['collided', null]]);   // a001 = 単品 A001 と子 a001 の違い
  assert.equal(c['rep|a001'], undefined);   // 名札は別の名前空間
  assert.deepEqual(x.ne.ne_codes.counts, { ok: 7, collided: 3, invalid: 0 });
  // 次の日: 商品の側だけ集め終えた (セットの側の世代に印が無い) = 書かない = 前の回のまま
  const prev = x.result.compare_run_id;
  x = await day('2030-04-03', { ne: NEc, spellings: { products: full.products } });
  assert.deepEqual([x.ne.ne_codes.reason, x.ne.ne_codes.write], ['not_collected', 'skipped_not_collected']);
  assert.deepEqual(await mark(), [prev]);
  // 証跡にも残る
  assert.deepEqual(x.evidence.ne.ne_codes && [x.evidence.ne.ne_codes.state, x.evidence.ne.ne_codes.write], ['unavailable', 'skipped_not_collected']);
  // 両方を集め終えた日でも、判断の台帳が書けなかった回は元のコードを書かない (この回は照合の回の記録が無い)
  x = await day('2030-04-04', { ne: NEc, spellings: full });
  assert.equal(x.ne.ne_codes.write, 'ok');
  const okRun = x.result.compare_run_id;
  const failDecisions = { query: async (t, p) => { if (/record_decision_candidates/.test(t)) throw new Error('writer down'); return db.query(t, p); } };
  x = await compare('2030-04-04', { writerDb: failDecisions });
  assert.deepEqual([x.result.ne.decisions_write, x.result.ne.ne_codes.write], ['failed', 'skipped_decisions_failed']);
  assert.deepEqual(await mark(), [okRun]);
});

await ta('[29] 新商品の NE 登録の CSV (⑤-2b): ② が最後まで走った回に確かめ待ちの商品の NE の値を送る → 全部の列が合えば verified + NE 確認済み / blocked の回は送らない', async () => {
  // 下書きの新商品 h009 と、取り込んだと申告したファイル (画面の道 = 関数 ops.ne_reg_* の代わりに、持ち主のロールで直接。切替の段階を進めない試験の DB)
  await pg.query('begin');
  const pid = (await pg.query(`insert into core.products (company_id, display_code, name, status) values (1, 'h009', '単品H', 'active') returning product_id`)).rows[0].product_id;
  const sid = (await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'h009', '単品H') returning sku_id`, [pid])).rows[0].sku_id;
  await pg.query('select ops.create_sku_registration($1, $2)', [sid, 'naka@test']);
  await pg.query('commit');
  const sha = 'c'.repeat(64);
  const ex = (await pg.query(`insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
    values ('products', 'ne-reg-single-v1', 'syohin_code', 'utf8', true, 1, 1, repeat('a', 64), repeat('b', 64), $1, '\\x00', gen_random_uuid(), 'x', '2030-04-01', 't') returning export_id`, [sha])).rows[0].export_id;
  const expected = { kind: 'single', values: { name: '単品H', supplier: '0001', cost: 700, price: 7000, tax_rate: 0.1, handling: 'active', parent: null } };
  const it = (await pg.query(`insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by)
    values ($1, $2, 'h009', 'h009', 'single', repeat('d', 64), $3::jsonb, repeat('e', 64), 1, 1, 't') returning item_id`, [ex, sid, JSON.stringify(expected)])).rows[0].item_id;
  await pg.query(`update ops.ne_reg_export_items set state = 'issued' where item_id = $1`, [it]);
  await pg.query(`update ops.ne_reg_exports set state = 'issued', issued_at = now(), issued_by = 't' where export_id = $1`, [ex]);
  const att = (await pg.query(`insert into ops.ne_reg_attempts (export_id, sha256, declared_by, result) values ($1, $2, 't', 'ok') returning attempt_id`, [ex, sha])).rows[0].attempt_id;
  await pg.query(`update ops.ne_reg_export_items set state = 'import_declared', attempt_id = $2 where item_id = $1`, [it, att]);
  await pg.query(`update ops.ne_reg_exports set state = 'declared', declared_at = now(), declared_by = 't' where export_id = $1`, [ex]);
  // 根拠は関数が上の記録 (申告した品目・試み・sha256) から自分で読む (呼び手の根拠は渡さない = 渡せば caller_evidence)
  await assert.rejects(pg.query(`select ops.transition_sku_registration($1, 'ne_pending', 'human', 't', null, $2::jsonb)`, [sid, JSON.stringify({ export_id: String(ex), sha256: sha })]), /caller_evidence/);
  await pg.query(`select ops.transition_sku_registration($1, 'ne_pending', 'human', 't', null, '{}'::jsonb)`, [sid]);
  const NEh = clone(baseNe());
  NEh.products.push({ code: 'h009', name: '単品H', supplier: '0001', handling: '取扱中', cost_src: J('700'), price_src: J('7000'), tax_src: J('10'), rep: '', rep_src: J('') });
  // ② が判定できない回 (NE の取得が古い = 前の日のまま) = 送らない
  let x = await compare('2030-04-05');
  assert.equal(x.result.ne.verdict, 'blocked');
  assert.match(String(x.result.ne.registrations?.write), /^skipped_/);
  assert.equal((await pg.query('select state from ops.ne_reg_export_items where item_id = $1', [it])).rows[0].state, 'import_declared');
  // 観測を残した後、受け取りの前に落ちた回 = 確かめない (NE 確認済みにしない・#1571 Codex R1 High 2)。後から回の番号で確かめても受け取りが無い = not_sealed
  const failAt = (re) => ({ query: async (sql, p) => { if (re.test(sql)) throw new Error('writer down'); return db.query(sql, p); } });
  x = await day('2030-04-06', { ne: NEh, compareExtra: { writerDb: failAt(/seal_ne_registration_run/) } });
  assert.deepEqual([x.result.ne.registrations.write, x.result.ne.registrations.seal, x.evidence.state], ['observed', 'failed', 'complete'], JSON.stringify(x.result.ne.registrations));
  assert.deepEqual(x.evidence.ne.reg_after_check, { state: 'skipped', reason: 'seal_failed' });   // 確かめが走らなかった = 何も変えていない = 確かめの前の数のまま (#1635 Codex R4)
  assert.deepEqual([(await pg.query('select state from ops.ne_reg_export_items where item_id = $1', [it])).rows[0].state,
    (await pg.query('select state from ops.master_registrations where sku_id = $1', [sid])).rows[0].state], ['import_declared', 'ne_pending']);
  await assert.rejects(pg.query('select ops.record_ne_registration_check($1)', [x.result.compare_run_id]), /not_sealed/);
  if (x.result.ne.baseline?.write === 'ok') {
    // 基準の書き込みが落ちた回も受け取りを書かない
    const y = await day('2030-04-07', { ne: NEh, compareExtra: { writerDb: failAt(/record_ne_baseline/) } });
    assert.deepEqual([y.result.ne.registrations.write, y.result.ne.registrations.seal], ['observed', 'skipped_baseline_failed'], JSON.stringify(y.result.ne.registrations));
    assert.equal((await pg.query('select state from ops.ne_reg_export_items where item_id = $1', [it])).rows[0].state, 'import_declared');
  }
  // 完了の証跡を書けなかった回 (回は失敗) も受け取りを書かない
  const z = await day('2030-04-08', { ne: NEh, compareExtra: { write: (d, n, p) => (p.state === 'complete' ? false : writeEvidence(d, n, p, { now: at('2030-04-08', '08:40'), warn: quiet })) } }).catch((e) => e);
  assert.ok(String(z && z.message).includes('証跡 (完了) を書けない'), String(z && z.message));
  assert.equal(Number((await pg.query('select count(*)::int as n from ops.ne_reg_compare_receipts')).rows[0].n), 0);
  assert.equal((await pg.query('select state from ops.ne_reg_export_items where item_id = $1', [it])).rows[0].state, 'import_declared');
  x = await day('2030-04-09', { ne: NEh });
  assert.equal(x.result.ne.registrations.write, 'ok', JSON.stringify(x.result.ne.registrations));
  assert.equal(x.result.ne.registrations.seal, 'ok');
  assert.deepEqual(x.result.ne.registrations.written.counts, { verified: 1 });
  assert.equal(x.evidence.ne.registrations.write, 'ok');
  assert.equal((await pg.query('select state from ops.ne_reg_export_items where item_id = $1', [it])).rows[0].state, 'verified');
  const reg = (await pg.query(`select r.state, e.evidence from ops.master_registrations r join ops.master_registration_events e on e.sku_id = r.sku_id and e.to_state = 'ne_confirmed' where r.sku_id = $1`, [sid])).rows[0];
  assert.deepEqual([reg.state, reg.evidence.compare_run_id, reg.evidence.matched], ['ne_confirmed', x.result.compare_run_id, true]);
  // 回の始まりの写し (#1571 Codex R2 Medium 1)・取得の世代と原本のハッシュ = 観測を作った NE の取得 (warehouse.db の完了の印と raw の行。Render の材料でない・R2 Low)
  const cr = (await pg.query(`select r.fetch_generation, r.raw_hash, r.target_codes, t.target_codes as snap from ops.ne_reg_compare_runs r join ops.ne_reg_compare_targets t using (compare_run_id)
     where r.compare_run_id = $1`, [x.result.compare_run_id])).rows[0];
  const mk = x.result.ne.ne_marks;
  const digits = (t) => String(t).replace(/[^0-9]/g, '');
  assert.equal(cr.fetch_generation, `ne_${digits(mk.products.at)}_${mk.products.rev}_${digits(mk.sets.at)}_${mk.sets.rev}`);
  assert.ok(!cr.fetch_generation.startsWith('mat_'));
  assert.deepEqual([cr.target_codes, cr.snap], [['h009'], ['h009']]);
  assert.deepEqual(x.result.ne.registrations.snapshot, { state: 'taken', target_hash: x.result.ne.registrations.snapshot.target_hash, targets: 1 });
});

await ta('[30] 持ち主が C の列 (④a): C ≠ NE で写し = C なら rule (company_owned・NE を C の値に・方向 to_ne) / 写しの後に C が変わった = rule_lag / どの列も direction_unknown にしない', async () => {
  // 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
  const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
  const own = { ...MASTER_OWNERSHIP, ...Object.fromEntries(['skus.name', 'products.name', 'skus.handling', 'products.status', 'skus.tax_rate', 'skus.tax_class', 'skus.standard_price', 'sku_costs', 'supplier_skus.is_primary'].map((k) => [k, 'company'])) };
  const NE = baseNe();
  const d0 = '2030-05-01', d1 = '2030-05-02';
  await day(d0, { ne: NE });   // 持ち主が全部 load の日 = そろえる (最後に一致した値も)
  // C (Company DB) で a001 の 6 つの列を直す (人の入力画面と同じく、単品の名前・状態は商品側も)。ロードは持ち主が C の列を書かない
  const cEdit = async () => {
    await db.query(`update core.skus set name = 'C の名前', handling = 'discontinued', tax_rate = 0.08, tax_class = 'REDUCED_8', standard_price_jpy = 1234 where code = 'a001'`);
    await db.query(`update core.products set name = 'C の名前', status = 'discontinued' where product_id = (select product_id from core.skus where code = 'a001')`);
    await db.query(`update core.sku_costs set cost_jpy = 150, cost_source = 'manual', cost_status = 'OVERRIDDEN' where valid_to is null and sku_id = (select sku_id from core.skus where code = 'a001')`);
    await db.query(`update core.supplier_skus set is_primary = false where sku_id = (select sku_id from core.skus where code = 'a001')`);
    await db.query(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary) select 1, sup.supplier_id, s.sku_id, true from core.suppliers sup, core.skus s
      where sup.code = '0002' and s.code = 'a001' on conflict (supplier_id, sku_id) do update set is_primary = true`);
  };
  // 今朝の写し (m_products) = C の値 (作り直しが company_owned の理由つきで重ねた)
  const copyOfC = toMaterial(NE, (m) => { for (const [k, v] of [['商品名', 'C の名前'], ['取扱区分', '取扱中止'], ['消費税率', 0.08], ['税区分', 'REDUCED_8'], ['標準売価', 1234], ['原価', 150], ['原価ソース', '例外'], ['原価状態', 'OVERRIDDEN'], ['仕入先コード', '0002']]) setMat(m, 'a001', k, v); });
  const co = (col, value, ne_value) => ({ code: 'a001', kind: '単品', col, reason: 'company_owned', owner_key: 'x', cdb_value: null, value, ne_value, generation_no: 1 });
  const reasons = [co('name', 'C の名前', '単品A'), co('handling', '取扱中止', '取扱中'), co('tax_rate', { 消費税率: 0.08, 税区分: 'REDUCED_8' }, { 消費税率: 0.1, 税区分: 'STANDARD_10' }),
    co('price', 1234, 1000), { ...co('cost', { 原価: 150 }, { 原価: 100 }), cdb_cost_source: 'manual' }, co('primary_supplier', '0002', '0001')];
  const x = await day(d1, { ne: NE, material: copyOfC, reasons, beforeLoad: cEdit, ownership: own });
  assert.equal(x.result.verdict, 'pass', JSON.stringify(x.result.items));   // ① ロードの検証: 持ち主が C の列は比べない
  const want = [['value:a001', 'name'], ['value:a001', 'handling'], ['value:a001', 'tax_rate'], ['value:a001', 'standard_price_jpy'], ['cost:a001', 'cost'], ['primary_supplier:a001', 'primary_supplier']];
  for (const [key, c] of want) {
    const cc = col(x.ne, key, c);
    assert.equal(cc.length, 1, `${key} ${c} が見えない`);
    assert.deepEqual([cc[0].cls, cc[0].explained?.reason, cc[0].owner], ['rule', 'company_owned', 'company'], `${key} ${c}: ${JSON.stringify(cc[0])}`);
  }
  assert.ok(!x.ne.items.some((i) => i.columns.some((cc) => cc.cls === 'direction_unknown')), '持ち主が C の列が direction_unknown に落ちた');
  // 判断の一覧: NE を C の値に (持ち主 = company・理由 = company_owned)
  const dn = x.ne.decisions.find((d) => d.subject_key === 'value:a001' && d.col === 'name');
  assert.deepEqual([dn.reason_kind, dn.proposal, dn.print.owner], ['company_owned', { op: 'set_ne_value', value: 'C の名前' }, 'company']);
  // 最後に一致した値の方向 = C 側が変わった (to_ne)
  assert.equal(col(x.ne, 'value:a001', 'name')[0].direction, 'to_ne');
  // 写しの後に C が変わった (今朝の写しはまだ前の C の値) = 翌朝の写し待ち (rule_lag)
  await db.query(`update core.skus set name = 'C の新しい名前' where code = 'a001'`);
  await db.query(`update core.products set name = 'C の新しい名前' where product_id = (select product_id from core.skus where code = 'a001')`);
  const y = await compare(d1);
  const yn = col(y.result.ne, 'value:a001', 'name');
  assert.deepEqual([yn[0].cls, yn[0].explained?.reason], ['rule_lag', 'company_owned']);
  assert.ok(!y.result.ne.items.some((i) => i.columns.some((cc) => cc.cls === 'direction_unknown')));
});

await ta('[31] 持ち主が C の列は NE の値が空・0・null・不正でも company_owned で分ける (incomparable・ne_no_value にしない。NE の状態は残す)・NE も C も空 = 一致 (Codex #1564 R1 M6)', async () => {
  // 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
  const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
  const own = { ...MASTER_OWNERSHIP, ...Object.fromEntries(['skus.name', 'products.name', 'skus.handling', 'products.status', 'skus.tax_rate', 'skus.tax_class', 'skus.standard_price', 'sku_costs', 'supplier_skus.is_primary'].map((k) => [k, 'company'])) };
  const NE = baseNe();
  const d0 = '2030-06-01', d1 = '2030-06-02';
  await day(d0, { ne: NE });   // 持ち主が全部 load の日 = そろえる
  // NE の c003 の値を列ごとに空・0・null・不正にする (C は前の値のまま = 持ち主が C の列はロードが書かない)
  const ne1 = clone(NE);
  Object.assign(ne1.products.find((r) => r.code === 'c003'), { name: '', handling: '廃番X', tax_src: J('0'), price_src: J(null), cost_src: null, supplier: '' });
  Object.assign(ne1.products.find((r) => r.code === 'd004'), { supplier: '' });
  // d004 の代表の仕入先を C で外す (NE も空 = 一致)
  const cEdit = async () => { await db.query(`update core.supplier_skus set is_primary = false where sku_id = (select sku_id from core.skus where code = 'd004')`); };
  const copyOfC = toMaterial(NE, (m) => setMat(m, 'd004', '仕入先コード', null));   // 今朝の写し = C の値
  const x = await day(d1, { ne: ne1, material: copyOfC, beforeLoad: cEdit, ownership: own });
  const want = [['value:c003', 'name', 'empty', 'no_value', '単品C'], ['value:c003', 'handling', 'value', 'incomparable', 'active'], ['value:c003', 'tax_rate', 'zero', 'incomparable', 0.1],
    ['value:c003', 'standard_price_jpy', 'null', 'no_value', 3000], ['cost:c003', 'cost', 'unknown', 'incomparable', null], ['primary_supplier:c003', 'primary_supplier', 'empty', 'no_value', null]];
  for (const [key, c, nState, comp] of want) {
    const cc = col(x.ne, key, c);
    assert.equal(cc.length, 1, `${key} ${c} が見えない (held = ${x.ne.held[key]})`);
    assert.deepEqual([cc[0].cls, cc[0].explained?.reason, cc[0].owner, cc[0].n_state, cc[0].n_comparability], ['rule', 'company_owned', 'company', nState, comp], `${key} ${c}: ${JSON.stringify(cc[0])}`);
    assert.ok(!Object.hasOwn(x.ne.held, key), `${key} が保持 (incomparable) に落ちた`);
  }
  assert.equal(col(x.ne, 'value:c003', 'handling')[0].n_validity, 'invalid');   // NE の不正な状態は残す
  // 判断の一覧 = NE を C の値に (NE の値が無くても・不正でも)
  const dn = x.ne.decisions.find((d) => d.subject_key === 'value:c003' && d.col === 'name');
  assert.deepEqual([dn.cls, dn.reason_kind, dn.proposal], ['rule', 'company_owned', { op: 'set_ne_value', value: '単品C' }]);
  // NE も C も空 (d004 の代表の仕入先) = 一致 = 項目にも保持にも出ない
  assert.deepEqual([col(x.ne, 'primary_supplier:d004').length, Object.hasOwn(x.ne.held, 'primary_supplier:d004')], [0, false]);
  // 持ち主が load の列は今までどおり (NE の値が無い = ne_no_value) = 持ち主を全部 load に戻した日
  const ne2 = clone(NE); ne2.products.find((r) => r.code === 'e005').price_src = J('');
  const y = await day('2030-06-03', { ne: ne2 });
  assert.deepEqual(clsOf(y.ne, 'value:e005', 'standard_price_jpy'), ['ne_no_value']);
});

await ta('[32] 持ち主が C の名前・売価・原価 (2026-10-05・#1629 Codex R1): 社内 0 円は実値 (NE の空・0・非 0 と一致にしない = cdb_zero_yen・NE を直すは出さない) / 社内の名前 = NE のコードで NE が空 = cdb_name_is_code / 証拠の無いコードに見える名前・NE も社内もコード名 = name_like_code (中立) / 写し待ちは rule_lag を先に / W13 の案件', async () => {
  const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
  const own = { ...MASTER_OWNERSHIP, ...Object.fromEntries(['skus.name', 'products.name', 'skus.handling', 'products.status', 'skus.tax_rate', 'skus.tax_class', 'skus.standard_price', 'sku_costs', 'supplier_skus.is_primary'].map((k) => [k, 'company'])) };
  const NE = baseNe();
  await day('2030-07-01', { ne: NE });   // 持ち主が全部 load の日 = そろえる
  const ne1 = clone(NE);
  const P = (code, o) => Object.assign(ne1.products.find((r) => r.code === code), o);
  P('a001', { name: 'a001' });                                       // NE も社内もコード名
  P('b002', { cost_src: J('') });                                    // 名前: NE に名前・社内はコード (大文字) / 原価: NE が空・社内 override_zero の 0 円
  P('c003', { name: '', price_src: J('0.00'), cost_src: J('0') });   // 名前: NE が空・社内 = NE のコード (夜間ロードの名残) / 売価: NE 0.00・社内 0 / 原価: NE 0・社内 override_zero 0
  P('d004', { name: '', price_src: J('1980') });                     // 名前: NE が空・社内は全角のコード (証拠なし) / 売価: NE 1980・社内 0
  P('e005', { price_src: J('0') });                                  // NE 0・社内 5555 = NE を 5555 に
  P('f006', { tax_src: J(''), price_src: J('') });                   // 税率: NE が空・社内 0.1 / 売価: NE が空・社内 null = 一致
  ne1.sets.filter((r) => r.parent === 's002').forEach((r) => { r.name = ''; r.price_src = J('0.00'); });   // セット: 名前が空・売価 0.00
  const sku = (code, set) => db.query(`update core.skus set ${set} where code = '${code}'`);
  const zeroCost = (code) => db.query(`update core.sku_costs set cost_jpy = 0, cost_source = 'override_zero', cost_status = 'OVERRIDDEN' where valid_to is null and sku_id = (select sku_id from core.skus where code = '${code}')`);
  const cEdit = async () => {
    await sku('a001', `name = 'a001', standard_price_jpy = 0`);   // 売価 0 は写し待ち (材料は前の 1000) = rule_lag
    await sku('b002', `name = 'B002'`); await zeroCost('b002');
    await sku('c003', `name = 'c003', standard_price_jpy = 0`); await zeroCost('c003');
    await sku('d004', `name = 'Ｄ００４', standard_price_jpy = 0`);
    await sku('e005', 'standard_price_jpy = 5555');
    await sku('e005', `name = 'e005'`);                            // NE に名前あり・材料の名前が空・社内 = コード = 証拠なし (材料は由来の証明にしない。#1629 Codex R2)
    await sku('f006', `name = 'f006', standard_price_jpy = null`);  // 名前: 材料は前の名前 (単品F) = rule_lag
    await sku('s001', `name = 's001'`);                            // セット: NE に名前 (セット1)・材料の名前が空・社内 = コード = 証拠なし
    await sku('s002', `name = 's002', standard_price_jpy = 0`);
  };
  // 今朝の写し = C の値 (a001 の売価と s001 の名前だけ前の値 = 写し待ち)
  const copyOfC = toMaterial(ne1, (m) => {
    for (const [code, k, v] of [['a001', '商品名', 'a001'], ['b002', '商品名', 'B002'], ['b002', '原価', 0], ['b002', '原価ソース', '例外'], ['b002', '原価状態', 'OVERRIDDEN'],
      ['c003', '商品名', 'c003'], ['c003', '標準売価', 0], ['c003', '原価', 0], ['c003', '原価ソース', '例外'], ['c003', '原価状態', 'OVERRIDDEN'], ['d004', '商品名', 'Ｄ００４'], ['d004', '標準売価', 0],
      ['e005', '標準売価', 5555], ['f006', '標準売価', null], ['f006', '消費税率', 0.1], ['s002', '商品名', 's002'], ['s002', '標準売価', 0]]) setMat(m, code, k, v);
    setMat(m, 'a001', '標準売価', 1000); setMat(m, 'f006', '商品名', '単品F');   // 写し待ち
    setMat(m, 'e005', '商品名', ''); setMat(m, 's001', '商品名', '');            // 材料の名前が空 (NE には名前がある)
  });
  const x = await day('2030-07-02', { ne: ne1, material: copyOfC, beforeLoad: cEdit, ownership: own });
  const one = (key, c) => { const cc = col(x.ne, key, c); assert.equal(cc.length, 1, `${key} ${c}: ${JSON.stringify(cc)} held=${x.ne.held[key]}`); return cc[0]; };
  const dec = (key, c) => x.ne.decisions.find((d) => d.subject_key === key && d.col === c);
  const want = [
    // [案件, 列, 分類, 理由, 提案, 選べる決め方]
    ['value:c003', 'name', 'rule', 'cdb_name_is_code', { op: 'fill_cdb_name' }, ['fix_cdb', 'accept_difference']],
    ['value:s002', 'name', 'rule', 'cdb_name_is_code', { op: 'fill_cdb_name' }, ['fix_cdb', 'accept_difference']],
    ['value:b002', 'name', 'rule', 'name_like_code', { op: 'check_name' }, ['accept_difference', 'spec']],   // NE に名前 = 社内を NE に戻す既定は出さない
    ['value:d004', 'name', 'rule', 'name_like_code', { op: 'check_name' }, ['accept_difference', 'spec']],   // 全角のコード = 夜間ロードの名残の証拠なし
    ['value:a001', 'name', 'rule', 'name_like_code', { op: 'check_name' }, ['accept_difference', 'spec']],   // NE も社内もコード名 = 一致にしない
    ['value:e005', 'name', 'rule', 'name_like_code', { op: 'check_name' }, ['accept_difference', 'spec']],   // 材料の名前が空でも NE に名前 = 証拠なし (単品)
    ['value:s001', 'name', 'rule', 'name_like_code', { op: 'check_name' }, ['accept_difference', 'spec']],   // 同じ (セット)
    ['value:f006', 'name', 'rule_lag', 'company_owned', { op: 'decide' }, ['accept_difference', 'fix_ne']],  // 写し待ちを隠さない (コード名でも NE をコードにする提案は出さない)
    ['value:c003', 'standard_price_jpy', 'rule', 'cdb_zero_yen', { op: 'decide_zero' }, ['accept_difference', 'fix_cdb']],   // NE 0.00・社内 0
    ['value:s002', 'standard_price_jpy', 'rule', 'cdb_zero_yen', { op: 'decide_zero' }, ['accept_difference', 'fix_cdb']],   // セット
    ['value:d004', 'standard_price_jpy', 'rule', 'cdb_zero_yen', { op: 'decide_zero' }, ['accept_difference', 'fix_cdb']],   // NE 1980・社内 0
    ['value:a001', 'standard_price_jpy', 'rule_lag', 'company_owned', { op: 'decide' }, ['accept_difference', 'fix_ne']],    // 社内を 0 にした直後 = 写し待ち
    ['cost:b002', 'cost', 'rule', 'cdb_zero_yen', { op: 'decide_zero' }, ['accept_difference', 'fix_cdb']],                 // override_zero の 0 円・NE が空
    ['cost:c003', 'cost', 'rule', 'cdb_zero_yen', { op: 'decide_zero' }, ['accept_difference', 'fix_cdb']],                 // override_zero の 0 円・NE 0
    ['value:e005', 'standard_price_jpy', 'rule', 'company_owned', { op: 'set_ne_value', value: 5555 }, ['accept_difference', 'fix_ne']],
    ['value:f006', 'tax_rate', 'rule', 'company_owned', { op: 'set_ne_value', value: 0.1 }, ['accept_difference', 'fix_ne']],   // NE の税率が空 (CSV は 10 と書く = ne-csv の試験)
  ];
  for (const [key, c, cls, reason, proposal, res] of want) {
    const cc = one(key, c);
    assert.deepEqual([cc.cls, cc.explained?.reason], [cls, reason], `${key} ${c}: ${JSON.stringify(cc)}`);
    const d = dec(key, c);
    assert.deepEqual([d.reason_kind, d.proposal, d.resolutions], [reason, proposal, res], `${key} ${c}: ${JSON.stringify(d)}`);
  }
  // 社内 null・NE 空 = 一致 (値なし同士)
  assert.deepEqual(clsOf(x.ne, 'value:f006', 'standard_price_jpy'), ['match']);
  // NE をコードにする提案・NE を 0 にする提案は 1 つも無い
  assert.ok(!x.ne.decisions.some((d) => d.proposal?.op === 'set_ne_value' && ((d.col === 'name' && nameIsCode(d.proposal.value, d.norm)) || (['standard_price_jpy', 'cost'].includes(d.col) && Number(d.proposal.value) === 0))));
  // W13 の案件 (items) = 上の案件が全部入る・件数が items と合う
  const keys = new Set(x.ne.items.map((i) => i.subject_key));
  for (const [key] of want) assert.ok(keys.has(key), `${key} が案件に無い`);
  assert.equal(x.ne.counts.items, x.ne.items.length);
  // 持ち主が load の列: NE 0 と社内 0 = ne_no_value のまま (社内 0 は実値・0 は NE に提案しない = 決める)
  const ne3 = clone(NE); ne3.products.find((r) => r.code === 'a001').price_src = J('0');
  await day('2030-07-03', { ne: ne3 });
  const y = await day('2030-07-04', { ne: ne3 });
  assert.deepEqual(clsOf(y.ne, 'value:a001', 'standard_price_jpy'), ['ne_no_value']);
  const ya = y.ne.decisions.find((d) => d.subject_key === 'value:a001' && d.col === 'standard_price_jpy');
  assert.deepEqual([ya.c, ya.proposal, ya.resolutions], [0, { op: 'decide' }, ['accept_difference', 'spec']]);
  await day('2030-07-05', { ne: NE });
});

await ta('[33] ポータルで登録した新商品 (0052): 下書き・NE登録待ちで NE に無い = NE 登録待ち (差・判断・W13 に出さない・報告に段階と数) / 14 日たつと reg_stale / やめた = 対象外 / 区分違い = reg_kind_mismatch (⚠️) / partial・failed も報告 / 登録の無い NE 欠け・NE 確認済みは今までどおり / 読めない = 今までどおり', async () => {
  // 新商品 (画面の道 = ops.create_sku_registration と同じ取引で SKU を作る)。n904 はポータルで単品として登録したのに NE はセット
  const mk = async (code, kind = 'single') => {
    await pg.query('begin');
    const pid = (await pg.query(`insert into core.products (company_id, display_code, name, status) values (1, $1, $2, 'active') returning product_id`, [code, `新商品 ${code}`])).rows[0].product_id;
    const sid = (await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, $2, $3, $4) returning sku_id`, [pid, kind, code, `新商品 ${code}`])).rows[0].sku_id;
    await pg.query('select ops.create_sku_registration($1, $2)', [sid, 'naka@test']);
    await pg.query('commit');
    return sid;
  };
  const sid = {};
  for (const c of ['n901', 'n902', 'n903', 'n904', 'n905', 'n906', 'n907']) sid[c] = await mk(c);
  // NE 登録の CSV の段階 ([29] と同じく持ち主のロールで直接): n901 = 配った / n905 = 申告 → NE登録待ち → 取り込めていない (failed) / n906 = 申告 → NE登録待ち → 中身が違う (partial)
  let shaN = 0;
  const csvItem = async (code, upTo) => {
    const sha = (++shaN).toString(16).padStart(64, 'f');
    const ex = (await pg.query(`insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day, created_by)
      values ('products', 'ne-reg-single-v1', 'syohin_code', 'utf8', true, 1, 1, repeat('a', 64), repeat('b', 64), $1, '\\x00', gen_random_uuid(), 'x', '2030-08-01', 't') returning export_id`, [sha])).rows[0].export_id;
    const expected = { kind: 'single', values: { name: '登録の名前 (NE と違う)', supplier: '0001', cost: 1, price: 1, tax_rate: 0.1, handling: 'active', parent: null } };
    const it = (await pg.query(`insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by)
      values ($1, $2, $3, $3, 'single', repeat('d', 64), $4::jsonb, repeat('e', 64), 1, 1, 't') returning item_id`, [ex, sid[code], code, JSON.stringify(expected)])).rows[0].item_id;
    await pg.query(`update ops.ne_reg_export_items set state = 'issued' where item_id = $1`, [it]);
    await pg.query(`update ops.ne_reg_exports set state = 'issued', issued_at = now(), issued_by = 't' where export_id = $1`, [ex]);
    if (upTo === 'issued') return;
    // 全部拒まれたと申告 (0053 の ne_reg_declare と同じ = 配った品目を failed (rejected_all) に。登録は下書きのまま)
    if (upTo === 'rejected') { await pg.query(`update ops.ne_reg_export_items set state = 'failed', failed_reason = 'rejected_all' where item_id = $1`, [it]); return; }
    const att = (await pg.query(`insert into ops.ne_reg_attempts (export_id, sha256, declared_by, result) values ($1, $2, 't', 'ok') returning attempt_id`, [ex, sha])).rows[0].attempt_id;
    await pg.query(`update ops.ne_reg_export_items set state = 'import_declared', attempt_id = $2 where item_id = $1`, [it, att]);
    await pg.query(`update ops.ne_reg_exports set state = 'declared', declared_at = now(), declared_by = 't' where export_id = $1`, [ex]);
    await pg.query(`select ops.transition_sku_registration($1, 'ne_pending', 'human', 't', null, '{}'::jsonb)`, [sid[code]]);
    if (upTo === 'declared') return;   // 取り込んだと申告しただけ (翌朝の照合の確かめが failed / partial にする)
    await pg.query(upTo === 'failed' ? `update ops.ne_reg_export_items set state = 'failed', failed_reason = 'not_in_ne' where item_id = $1` : `update ops.ne_reg_export_items set state = 'partial' where item_id = $1`, [it]);
  };
  await csvItem('n901', 'issued'); await csvItem('n905', 'failed'); await csvItem('n906', 'partial'); await csvItem('n907', 'rejected');
  await pg.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 't', '売らないことにした', '{}'::jsonb)`, [sid.n902]);
  // 状態が最後に進んだ日 (本物の now() は 2030 年より前 = 試験の日に合わせる。関数の中の印を立てて持ち主のロールで)
  const since = async (code, at) => {
    await pg.query('begin'); await pg.query("select set_config('ops.registration_protocol', '1', true)");
    await pg.query('update ops.master_registrations set state_changed_at = $2::timestamptz where sku_id = $1', [sid[code], at]); await pg.query('commit');
  };
  for (const c of ['n901', 'n902', 'n904', 'n905', 'n906', 'n907']) await since(c, '2030-08-01T10:00:00+09:00');
  await since('n903', '2030-07-10T10:00:00+09:00');
  // NE: いつもの商品 (e005 は NE から消えた = 登録の行が無い NE 欠け) + n904 (セット) + n906 (単品)。材料 = NE のまま、ただし n904 は材料に入れない (ロードが区分を直さない)
  const NE4 = clone(NE);
  NE4.products = NE4.products.filter((r) => r.code !== 'e005');
  NE4.products.push({ code: 'n906', name: 'NE の名前', supplier: '0001', handling: '取扱中', cost_src: J('100'), price_src: J('1000'), tax_src: J('10'), rep: '', rep_src: J('') });
  NE4.sets.push({ parent: 'n904', name: 'セット N4', child: 'a001', price_src: J('3000'), qty_src: J('2') });
  const mat4 = () => toMaterial(NE4, (m) => { m.products = m.products.filter((r) => r.商品コード !== 'n904'); m.sets = m.sets.filter((r) => r.セット商品コード !== 'n904'); });
  const d = await day('2030-08-05', { ne: NE4, material: mat4() });
  const ne = d.ne;
  assert.equal(ne.verdict, 'breach', ne.blocked_reason);
  // NE 登録待ち = 差にしない (案件・判断・保持・回復のどれにも無く、対象外の理由つき)
  for (const c of ['n901', 'n905', 'n907']) {
    assert.equal(ne.out_of_scope[`only_in_cdb:${c}`], 'reg_pending', c);
    assert.ok(!ne.items.some((i) => i.norm === c) && !ne.decisions.some((x) => x.norm === c), `${c} が差に出た`);
  }
  assert.equal(ne.out_of_scope['only_in_cdb:n902'], 'reg_cancelled');
  assert.ok(!ne.items.some((i) => i.norm === 'n902'));
  // 登録から 14 日以上 (7/10 → 8/5 = 26 日) = reg_stale (判断の一覧に・提案は NE 登録の CSV)
  assert.deepEqual(clsOf(ne, 'only_in_cdb:n903'), ['reg_stale']);
  const st = ne.decisions.find((x) => x.subject_key === 'only_in_cdb:n903');
  assert.deepEqual([st.reason_kind, st.proposal, st.resolutions], ['reg_stale', { op: 'register_in_ne', state: 'draft', since: '2030-07-10' }, ['spec', 'accept_difference']]);
  // 区分違い = 重大な登録の不一致 (NE を登録の区分に・差を残すは出さない)
  assert.deepEqual(clsOf(ne, 'kind:n904'), ['reg_kind_mismatch']);
  const km = ne.decisions.find((x) => x.subject_key === 'kind:n904');
  assert.deepEqual([km.reason_kind, km.proposal, km.resolutions, km.n, km.c], ['reg_kind_mismatch', { op: 'set_ne_value', value: 'single' }, ['fix_ne', 'spec'], 'set', 'single']);
  // 登録の行が無い NE 欠け (e005)・NE で一度確かめた新商品 (h009 = [29] で NE 確認済み) は今までどおり差
  assert.deepEqual(clsOf(ne, 'only_in_cdb:e005'), ['spec_undecided']);
  assert.ok(ne.items.some((i) => i.subject_key === 'only_in_cdb:h009'));
  assert.ok(!['reg_stale', 'reg_pending'].includes(col(ne, 'only_in_cdb:h009')[0]?.cls));
  // 報告: 段階と数・一覧
  const rp = ne.reg_pending;
  assert.equal(rp.state, 'ok');
  // failed の理由で分ける: not_in_ne (申告したのに NE に無い) = failed / rejected_all (全部拒まれた・下書きのまま) = rejected (#1635 Codex R1 Low)
  assert.deepEqual(rp.waiting.map((e) => [e.code, e.state, e.stage, e.days]), [['n901', 'draft', 'issued', 4], ['n905', 'ne_pending', 'failed', 4], ['n907', 'draft', 'rejected', 4]]);
  assert.deepEqual(rp.stages, { issued: 1, failed: 1, rejected: 1 });
  assert.deepEqual([rp.stale.map((e) => [e.code, e.days]), rp.cancelled.map((e) => e.code), rp.kind_mismatch.map((e) => [e.code, e.ne_kind]), rp.partial.map((e) => [e.code, e.stage])],
    [[['n903', 26]], ['n902'], [['n904', 'set']], [['n906', 'partial']]]);
  assert.deepEqual([ne.counts.reg_pending, ne.counts.reg_stale, ne.counts.reg_cancelled, ne.counts.reg_kind_mismatch, ne.counts.reg_partial, ne.counts.reg_failed, ne.counts.reg_rejected], [3, 1, 1, 1, 1, 1, 1]);
  assert.equal(ne.counts.items, ne.items.length);
  disjoint(ne);
  // 朝の要約: 新商品の登録の不一致 (区分違い・取り込めていない・中身違い) = ② を先頭に ⚠️・NE 登録待ちの数と段階
  assert.match(d.line, /^⚠️ ②: 新商品の NE 登録の不一致 \(区分 \(単品・セット\) 違い 1 件・取り込んだと申告したのに NE に無い 1 件・NE が取り込みを全部拒んだ \(CSV を作り直す\) 1 件・NE の中身が登録と違う 1 件\)/);
  assert.match(d.line, /NE 登録待ち 3 件 \(差に入れない。配った 1・取り込めていない 1・NE が全部拒んだ 1\)・登録から 14 日以上 NE に無い 1 件 \(差\)/);
  // W13:ne の案件 = items だけ (NE 登録待ちは対象外 = 開かない)。証跡にも数が残る
  const ev = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', '2030-08-05', 'master-compare.json'), 'utf8'));
  assert.deepEqual([ev.ne.counts.reg_pending, ev.ne.counts.items], [3, ne.items.length]);

  // NE の行が落ちた朝 (同じ日の取り直し): 前の回に区分違い (n904)・partial (n906) だった新商品が NE から欠けても、NE 登録待ちの対象外にしない = 保持 (#1635 Codex R1 Medium)。
  //   partial は確かめの記録で数える = ⚠️ を消さない / 待ちの n901 も保持 (NE に無いと言えない)
  const NEd = clone(NE4); NEd.products = NEd.products.filter((r) => r.code !== 'n906'); NEd.sets = NEd.sets.filter((r) => r.parent !== 'n904');
  const md = setNe(NEd, '2030-08-05', { intP: { dropped_no_code: 1 } });
  sendToRender(mat4(), '2030-08-05', setBuild('2030-08-05', md, []));
  const dr = await compare('2030-08-05');
  const dn = dr.result.ne;
  for (const c of ['n901', 'n904', 'n906']) {
    assert.equal(dn.held[`only_in_cdb:${c}`], 'ne_dropped_rows', c);
    assert.ok(!Object.hasOwn(dn.out_of_scope, `only_in_cdb:${c}`) && !Object.hasOwn(dn.out_of_scope, `kind:${c}`) && !Object.hasOwn(dn.out_of_scope, `value:${c}`), `${c} を対象外にした`);
  }
  assert.equal(dn.held['kind:n904'], 'not_in_ne');
  assert.deepEqual([dn.counts.reg_pending, dn.counts.reg_partial, dn.reg_pending.partial.map((e) => e.code)], [0, 1, ['n906']]);
  // 取り込めていない (n905)・全部拒まれた (n907) も確かめの記録で数える = 行が落ちた朝も ⚠️ を消さない
  assert.match(dr.line, /^⚠️ ②: 新商品の NE 登録の不一致 \(取り込んだと申告したのに NE に無い 1 件・NE が取り込みを全部拒んだ \(CSV を作り直す\) 1 件・NE の中身が登録と違う 1 件\)/);
  disjoint(dn);

  // 13 日目 = まだ待ち / 14 日目 = reg_stale (n901: 8/1 から)
  let x = await day('2030-08-14', { ne: NE4, material: mat4() });
  assert.equal(x.ne.out_of_scope['only_in_cdb:n901'], 'reg_pending');
  x = await day('2030-08-15', { ne: NE4, material: mat4() });
  assert.deepEqual(clsOf(x.ne, 'only_in_cdb:n901'), ['reg_stale']);
  assert.equal(x.ne.decisions.find((y) => y.subject_key === 'only_in_cdb:n901').proposal.state, 'draft');
  assert.deepEqual(x.ne.reg_pending.stale.map((e) => e.code), ['n901', 'n903', 'n905', 'n907']);
  // 登録の状態を読めない朝 = NE 登録待ちを分けない (今までどおり差に出す)・要約に「読めない」
  const broken = { query: (sql, p) => (/from ops\.master_registrations r/.test(sql) ? Promise.reject(new Error('permission denied for table master_registrations')) : db.query(sql, p)) };
  const y = await compare('2030-08-15', { db: broken });
  assert.equal(y.result.ne.reg_pending.state, 'unreadable');
  assert.deepEqual(clsOf(y.result.ne, 'only_in_cdb:n901'), ['spec_undecided']);
  assert.ok(!Object.hasOwn(y.result.ne.out_of_scope, 'only_in_cdb:n902'));
  assert.match(y.line, /NE 登録待ちを読めない \(差に含む\)/);

  // 登録の状態は照合が使う状態 (NE 登録待ち・やめた) だけ読む = NE 確認済み (h009) の行は読まない (#1635 Codex R2 Low)
  await pg.query('begin');
  try {
    const rr = await readRegistrations(db);
    assert.deepEqual([rr.state, rr.byNorm.has('h009'), rr.byNorm.has('n902'), rr.byNorm.get('n907')?.stage, rr.byNorm.get('n905')?.stage], ['ok', false, true, 'rejected', 'failed']);
  } finally { await pg.query('rollback'); }

  // 本物の順番 (#1635 Codex R2 Medium): 取り込んだと申告しただけ (import_declared) の n908 (NE に無い)・n909 (NE にあるが中身が違う) を、その朝の照合の確かめが
  //   failed / partial にする → 全件 JSON (確かめの前) の数は前のまま・同じ朝の要約と証跡 (W13 が読む) は確かめの後の数で ⚠️
  for (const c of ['n908', 'n909']) { sid[c] = await mk(c); await csvItem(c, 'declared'); await since(c, '2030-08-19T10:00:00+09:00'); }
  const NE5 = clone(NE4);
  NE5.products.push({ code: 'n909', name: 'NE の名前 9', supplier: '0001', handling: '取扱中', cost_src: J('100'), price_src: J('1000'), tax_src: J('10'), rep: '', rep_src: J('') });
  const mat5 = () => toMaterial(NE5, (m) => { m.products = m.products.filter((r) => r.商品コード !== 'n904'); m.sets = m.sets.filter((r) => r.セット商品コード !== 'n904'); });
  const z = await day('2030-08-20', { ne: NE5, material: mat5() });
  assert.deepEqual(z.result.ne.registrations.written?.counts && [z.result.ne.registrations.written.counts.failed, z.result.ne.registrations.written.counts.partial], [1, 2], JSON.stringify(z.result.ne.registrations));
  const itemState = async (c) => (await pg.query('select state from ops.ne_reg_export_items where sku_id = $1 order by item_id desc limit 1', [sid[c]])).rows[0].state;
  assert.deepEqual([await itemState('n908'), await itemState('n909')], ['failed', 'partial']);
  assert.deepEqual([z.ne.counts.reg_failed, z.ne.counts.reg_partial], [1, 1]);   // 全件 JSON = 確かめの前 (n905・n906 だけ)
  assert.deepEqual([z.ne.reg_after_check.state, z.ne.reg_after_check.reg_failed, z.ne.reg_after_check.reg_partial, z.ne.reg_after_check.reg_rejected], ['ok', 2, 2, 1]);
  assert.match(z.line, /^⚠️ ②: 新商品の NE 登録の不一致 \(区分 \(単品・セット\) 違い 1 件・取り込んだと申告したのに NE に無い 2 件・NE が取り込みを全部拒んだ \(CSV を作り直す\) 1 件・NE の中身が登録と違う 2 件\)/);
  // 0063: 配っただけ (申告なし) の n901 は NE に無い = 待ち (failed にしない)・配ってから 3 日を過ぎた取得 = 要約に ℹ️ で知らせる (失敗にしない)
  assert.equal(await itemState('n901'), 'issued');
  assert.deepEqual((z.result.ne.registrations.written.not_imported || []).map((x) => x.code), ['n901'], JSON.stringify(z.result.ne.registrations.written));
  assert.match(z.line, /ℹ️ 配ってから 3 日たっても NE に無い \(取り込まれていないらしい\) 1 件 \(n901\)/);
  assert.equal(z.evidence.ne.registrations.not_imported, 1);
  // 区分違いがある朝 (登録の不一致なし) も 0063 の知らせを消さない (#1659 Codex R1 Medium)
  const { neSummary: neSum } = await import('../apps/company-db/master-compare/run.mjs');
  const syn = { verdict: 'breach', counts: { items: 1 }, sku_kind_raw_mismatch: { alert: true, count: 1, codes: ['k001'] },
    registrations: { written: { not_imported: [{ code: 'n901' }], not_imported_days: 3, needs_declaration: [{ code: 'j001', reason: 'jan_not_compared' }] } } };
  const sl = neSum(syn);
  assert.match(sl, /^⚠️ ②: 区分が NE と違う SKU 1 件/);
  assert.match(sl, /ℹ️ 配ってから 3 日たっても NE に無い \(取り込まれていないらしい\) 1 件 \(n901\)/);
  assert.match(sl, /ℹ️ NE にあるが自動では確かめない \(取り込んだと申告すると確かめる\) 1 件 \(j001 JAN\)/);
  const ev5 = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', '2030-08-20', 'master-compare.json'), 'utf8'));
  assert.deepEqual([ev5.ne.reg_after_check.reg_failed, ev5.ne.reg_after_check.reg_partial], [2, 2]);
  // W13:ne は証跡の確かめの後の数を理由と観測に使う
  const w13 = await evalW13({ config: W13CFG, asOf: '2030-08-20', evidence: null, syncRunId: null, openIssues: [], dataDir: tmp }, W13CFG.checkById('W13'));
  const wne = (Array.isArray(w13) ? w13 : [w13]).find((r) => r.scopeKey === 'ne');
  assert.ok(wne, JSON.stringify(w13).slice(0, 300));
  assert.deepEqual([wne.observed.reg.failed, wne.observed.reg.partial], [2, 2], JSON.stringify(wne.observed));
  assert.match(wne.reason, /新商品の取込失敗 2/);

  // 確かめの後の読み直しだけが読めない朝 (#1635 Codex R3): 確かめの前の数に戻さない = 要約の先頭に ⚠️ (確かめの内訳つき)・W13:ne は blocked (案件は保持)
  let regReads = 0;
  const failSecond = { query: (sql, p) => (/from ops\.master_registrations r/.test(sql) && ++regReads === 2 ? Promise.reject(new Error('reread down')) : db.query(sql, p)) };
  const u = await compare('2030-08-20', { db: failSecond });
  assert.equal(regReads, 2);
  assert.deepEqual([u.result.ne.registrations.write, u.result.ne.reg_after_check.state], ['ok', 'unreadable']);
  assert.match(u.line, /^⚠️ ②: 新商品の NE 登録の確かめの後の数を読めない \(reread down\) — 確かめ: .*partial 2/);
  const evU = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', '2030-08-20', 'master-compare.json'), 'utf8'));
  assert.equal(evU.ne.reg_after_check.state, 'unreadable');
  const wU = (await evalW13({ config: W13CFG, asOf: '2030-08-20', evidence: null, syncRunId: null, openIssues: [], dataDir: tmp }, W13CFG.checkById('W13'))).find((r) => r.scopeKey === 'ne');
  assert.deepEqual([wU.verdict], ['blocked']);
  assert.match(wU.reason, /確かめの後の数を読めない/);
  // 確かめの後の証跡を書けない (writeEvidence は失敗で null を返す) = 回を失敗にする (state = failed = W13 は blocked・daily-sync は再試行)
  const noAfter = (d0, n0, p0) => (p0.state === 'complete' && p0.ne && p0.ne.reg_after_check && p0.ne.reg_after_check.state !== 'pending' ? null : writeEvidence(d0, n0, p0, { now: at('2030-08-20', '08:40'), warn: quiet }));
  const wf = await compare('2030-08-20', { write: noAfter }).catch((e) => e);
  assert.match(String(wf && wf.message), /証跡 \(新商品の確かめの後\) を書けない/);
  const evF = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', '2030-08-20', 'master-compare.json'), 'utf8'));
  assert.equal(evF.state, 'failed');
  const wF = (await evalW13({ config: W13CFG, asOf: '2030-08-20', evidence: null, syncRunId: null, openIssues: [], dataDir: tmp }, W13CFG.checkById('W13'))).find((r) => r.scopeKey === 'ne');
  assert.equal(wF.verdict, 'blocked');
  // 確かめの後の書き直しも failed の書き込みも落ちた (同じファイルの障害が続く) = 確かめの前の完了の証跡に残した印 (pending) で W13:ne は blocked (#1635 Codex R4)
  const bothFail = (d0, n0, p0) => (p0.state === 'failed' || (p0.state === 'complete' && p0.ne && p0.ne.reg_after_check && p0.ne.reg_after_check.state !== 'pending') ? null
    : writeEvidence(d0, n0, p0, { now: at('2030-08-20', '08:40'), warn: quiet }));
  const bf = await compare('2030-08-20', { write: bothFail }).catch((e) => e);
  assert.match(String(bf && bf.message), /証跡 \(新商品の確かめの後\) を書けない/);
  const evB = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', '2030-08-20', 'master-compare.json'), 'utf8'));
  assert.deepEqual([evB.state, evB.ne.reg_after_check], ['complete', { state: 'pending' }]);
  const wB = (await evalW13({ config: W13CFG, asOf: '2030-08-20', evidence: null, syncRunId: null, openIssues: [], dataDir: tmp }, W13CFG.checkById('W13'))).find((r) => r.scopeKey === 'ne');
  assert.equal(wB.verdict, 'blocked');
  assert.match(wB.reason, /確かめの後の証跡が書けていない/);
  // ふつうの朝は最後の書き直しで ok (印が残らない)
  const ok2 = await compare('2030-08-20');
  assert.equal(ok2.evidence.ne.reg_after_check.state, 'ok');
  const evO = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', '2030-08-20', 'master-compare.json'), 'utf8'));
  assert.equal(evO.ne.reg_after_check.state, 'ok');
  // 確かめの関数が commit した後に応答だけ失われた (接続が切れた) = check_failed でも読み直す (skipped にしない。#1635 Codex R5)
  sid.n910 = await mk('n910'); await csvItem('n910', 'declared'); await since('n910', '2030-08-19T10:00:00+09:00');
  const lostReply = { query: async (sql, p) => { const r = await db.query(sql, p); if (/record_ne_registration_check/.test(sql)) throw new Error('connection lost after commit'); return r; } };
  const lr = await compare('2030-08-20', { writerDb: lostReply });
  assert.deepEqual([lr.result.ne.registrations.write, await itemState('n910')], ['check_failed', 'failed']);   // DB は確かめを commit 済み
  assert.deepEqual([lr.result.ne.reg_after_check.state, lr.result.ne.reg_after_check.reg_failed], ['ok', 3]);   // n905・n908・n910
  assert.match(lr.line, /^⚠️ ②: 新商品の NE 登録の不一致 \(.*取り込んだと申告したのに NE に無い 3 件/);
  const evL = JSON.parse(fs.readFileSync(path.join(tmp, 'company-db-evidence', '2030-08-20', 'master-compare.json'), 'utf8'));
  assert.deepEqual([evL.ne.reg_after_check.state, evL.ne.reg_after_check.reg_failed], ['ok', 3]);
});

await ta('[34] 区分 (skus.sku_kind) の持ち主が C: 夜間ロードは区分を NE に合わせない → ② は区分の差を判断の一覧に (company_owned・NE を社内の区分に・NE の画面で) / ロードの記録に無い差 = rule_lag / 持ち主が load の日は今までどおり NE に合わせる (差が消える)', async () => {
  const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
  const own = { ...MASTER_OWNERSHIP, 'skus.sku_kind': 'company' };
  const NE = baseNe();
  await day('2030-08-21', { ne: NE });   // 持ち主が全部 load の日 = そろえる
  // NE で c003 を単品 → セット (a001 × 2)、s002 をセット → 単品 にする (NE の画面で区分を変えた。社内はポータルの区分のまま)
  const ne1 = clone(NE);
  ne1.products = ne1.products.filter((r) => r.code !== 'c003');
  ne1.products.push({ code: 's002', name: 'セット2', supplier: '0001', handling: '取扱中', cost_src: J('900'), price_src: J('900'), tax_src: J('10'), rep: '', rep_src: J('') });
  ne1.sets = ne1.sets.filter((r) => r.parent !== 's002');
  ne1.sets.push({ parent: 'c003', name: '単品C', child: 'a001', price_src: J('3000'), qty_src: J('2') });
  const kindOf = async (code) => (await db.query('select sku_kind, product_id from core.skus where code = $1', [code])).rows[0];
  const before = { c003: await kindOf('c003'), s002: await kindOf('s002') };
  // 1 日目: 夜間ロードはまだ前の材料 (NE の変更は今朝の取込から) = ロードの記録に無い差 = rule_lag (黙って一致にも direction_unknown にもしない)
  const w = await day('2030-08-22', { ne: ne1, ownership: own });
  for (const code of ['c003', 's002']) assert.deepEqual([clsOf(w.ne, `kind:${code}`, 'kind'), col(w.ne, `kind:${code}`, 'kind')[0]?.why_a], [['rule_lag'], 'not_in_load_record'], code);
  // 2 日目: 夜間ロードが NE の新しい区分を読む = 区分は社内のまま・NE のセットの構成を社内の単品に入れない・判断の記録に残す
  const x = await day('2030-08-23', { ne: ne1, ownership: own });
  assert.deepEqual([await kindOf('c003'), await kindOf('s002')], [before.c003, before.s002]);
  assert.equal((await db.query(`select count(*)::int as n from core.sku_components where parent_sku_id = (select sku_id from core.skus where code = 'c003')`)).rows[0].n, 0);
  const D = (await db.query(`select payload from ops.load_decisions where ingest_run_id = 'load_2030-08-23' and section = 'skus'`)).rows[0].payload;
  assert.deepEqual([...D.kind_held].sort(), [['c003', 'set', 'single'], ['s002', 'single', 'set']]);
  assert.equal(x.result.verdict, 'pass', JSON.stringify(x.result.items));   // ① ロードの検証: 区分は比べない (持ち主が C)・記録の形は通る
  for (const [code, n, c] of [['c003', 'set', 'single'], ['s002', 'single', 'set']]) {
    const cc = col(x.ne, `kind:${code}`, 'kind');
    assert.equal(cc.length, 1, `kind:${code} が見えない (held = ${x.ne.held[`kind:${code}`]})`);
    assert.deepEqual([cc[0].cls, cc[0].explained?.reason, cc[0].explained?.owner_key, cc[0].owner, cc[0].n, cc[0].c], ['rule', 'company_owned', 'skus.sku_kind', 'company', n, c], JSON.stringify(cc[0]));
    const d = x.ne.decisions.find((z) => z.subject_key === `kind:${code}` && z.col === 'kind');
    assert.deepEqual([d.cls, d.reason_kind, d.proposal, d.resolutions, d.print.owner], ['rule', 'company_owned', { op: 'set_ne_value', value: c }, ['accept_difference', 'fix_ne'], 'company'], JSON.stringify(d));
    assert.equal(x.ne.held[`value:${code}`], 'kind_mismatch');   // 区分が違う間は値・構成などは比べない (今までどおり)
  }
  assert.ok(!x.ne.items.some((i) => i.columns.some((z) => z.col === 'kind' && z.cls === 'direction_unknown')), '区分の差が direction_unknown に落ちた');
  // 区分の差の生の数 (広げる道 v8・Codex R8): 結果・証跡に・朝の要約の先頭が ⚠️ (重大)・差を残す承認でも減らない
  assert.deepEqual(x.ne.sku_kind_raw_mismatch, { count: 2, codes: ['c003', 's002'], alert: true });
  assert.deepEqual([x.ne.sku_kind_raw_mismatch_count, x.ne.kind_gate], [2, { raw_mismatch: 2, raw_unverifiable_affected_existing_cdb: 0, norm_collision: 0, unknown_kind: 0, integrity_untrusted: 0 }]);   // 区分のゲートの 5 つの鍵 (v11 §3.6.4)
  assert.deepEqual(Object.keys(x.ne.kind_gate).sort(), ['integrity_untrusted', 'norm_collision', 'raw_mismatch', 'raw_unverifiable_affected_existing_cdb', 'unknown_kind']);
  assert.match(x.line, /^⚠️ ②: 区分が NE と違う SKU 2 件 \(NE の画面で直す: c003, s002\)/);
  const evK = (await import('../apps/company-db/push/evidence.mjs')).readEvidence(tmp, '2030-08-23')['master-compare']?.ne?.sku_kind_raw_mismatch;
  assert.deepEqual(evK, x.ne.sku_kind_raw_mismatch);
  const evN = (await import('../apps/company-db/push/evidence.mjs')).readEvidence(tmp, '2030-08-23')['master-compare']?.ne;
  assert.deepEqual([evN.sku_kind_raw_mismatch_count, evN.kind_gate], [x.ne.sku_kind_raw_mismatch_count, x.ne.kind_gate]);
  for (const dd of x.ne.decisions.filter((z) => z.subject_key === 'kind:c003')) {
    await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'approved', 'accept_difference', null, 'user', 'test@example.com')`, [dd.fingerprint]);
  }
  const xa = await compare('2030-08-23');
  assert.equal(xa.result.ne.out_of_scope['kind:c003'], 'approved_exception');   // 通常の案件は承認で閉じる
  assert.deepEqual([xa.result.ne.sku_kind_raw_mismatch.count, xa.result.ne.sku_kind_raw_mismatch.codes], [2, ['c003', 's002']]);   // 生の数は減らない
  assert.match(xa.line, /^⚠️ ②: 区分が NE と違う SKU 2 件/);
  for (const dd of x.ne.decisions.filter((z) => z.subject_key === 'kind:c003')) {   // 承認を取り消す (後の確かめのため)
    await db.query(`insert into ops.master_decision_events (fingerprint, kind, resolution, target, actor_type, actor) values ($1, 'revoked', null, null, 'user', 'test@example.com')`, [dd.fingerprint]);
  }
  // 記録と社内の今が違う (ロードの後に区分が変わった) = rule_lag
  await db.query(`update ops.load_decisions set payload = jsonb_set(payload, '{kind_held}', '[["c003", "set", "single"]]'::jsonb) where section = 'skus' and ingest_run_id = 'load_2030-08-23'`);
  const y = await compare('2030-08-23');
  assert.deepEqual(clsOf(y.result.ne, 'kind:c003', 'kind'), ['rule']);
  assert.deepEqual([clsOf(y.result.ne, 'kind:s002', 'kind'), col(y.result.ne, 'kind:s002', 'kind')[0].why_a], [['rule_lag'], 'not_in_load_record']);
  // 持ち主が load の日 = 区分を NE に合わせる (今までどおり) → 区分の差は消える
  const z = await day('2030-08-24', { ne: ne1 });
  assert.deepEqual([(await kindOf('c003')).sku_kind, (await kindOf('s002')).sku_kind], ['set', 'single']);
  assert.deepEqual([col(z.ne, 'kind:c003').filter((q) => q.cls !== 'match').length, col(z.ne, 'kind:s002').filter((q) => q.cls !== 'match').length], [0, 0]);
});

await ta('[35] 区分の持ち主が C: 例外が絡む区分の差 (C 例外・NE 単品 / C 例外・NE セット / C 単品・NE 例外 / C セット・NE 例外) も例外の対象外より先に判断の一覧に・値などは今までどおり対象外 (exception_item)', async () => {
  const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
  const own = { ...MASTER_OWNERSHIP, 'skus.sku_kind': 'company' };
  const NE = baseNe();
  await day('2030-09-01', { ne: NE });   // 持ち主が全部 load の日 = そろえる
  // C: e005 (単品) → 例外・s002 (セット) → 例外。NE: f006 を商品の表から外す・s001 をセットの表から外す (どちらも今朝の作り直しでは例外の行)
  await db.query("update core.skus set sku_kind = 'exception' where code in ('e005', 's002')");
  const ne1 = clone(NE);
  ne1.products = ne1.products.filter((r) => r.code !== 'f006');
  ne1.sets = ne1.sets.filter((r) => r.parent !== 's001');
  wh().prepare("INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES ('f006', 99, '例外 f006', 'x'), ('s001', 99, '例外 s001', 'x')").run();   // NE の例外 = 例外の表 (区分のゲートの E)
  const mat = () => toMaterial(ne1, (m) => {
    for (const code of ['f006', 's001']) m.products.push({ 商品コード: code, 商品名: `例外 ${code}`, 商品区分: '例外', 取扱区分: '取扱中', 標準売価: null, 原価: 99, 原価ソース: '例外', 原価状態: 'OVERRIDDEN', 消費税率: 0.1, 税区分: 'STANDARD_10', product_id: code === 'f006' ? 6 : 7 });
  });
  const want = [['e005', 'single', 'exception'], ['s002', 'set', 'exception'], ['f006', 'exception', 'single'], ['s001', 'exception', 'set']];
  const check = (ne, clsOfCode) => {
    for (const [code, n, c] of want) {
      const cc = col(ne, `kind:${code}`, 'kind');
      assert.equal(cc.length, 1, `kind:${code} が見えない (out_of_scope = ${ne.out_of_scope[`kind:${code}`]})`);
      assert.deepEqual([cc[0].cls, cc[0].explained?.reason, cc[0].owner, cc[0].n, cc[0].c], [clsOfCode(code), 'company_owned', 'company', n, c], `${code}: ${JSON.stringify(cc[0])}`);
      assert.equal(ne.out_of_scope[`value:${code}`], 'exception_item');   // 値などは今までどおり対象外
      assert.ok(!Object.hasOwn(ne.out_of_scope, `kind:${code}`));
      const d = ne.decisions.find((z) => z.subject_key === `kind:${code}` && z.col === 'kind');
      assert.deepEqual([d.reason_kind, d.proposal], ['company_owned', { op: 'set_ne_value', value: c }]);
    }
  };
  // 1 日目: 夜間ロードは前の材料 = 社内の例外 (e005・s002) は記録 (kind_held) あり = rule / NE 側の例外 (f006・s001) はロードの記録に無い = rule_lag
  const x = await day('2030-09-02', { ne: ne1, material: mat(), ownership: own });
  check(x.ne, (code) => (['e005', 's002'].includes(code) ? 'rule' : 'rule_lag'));
  // 2 日目: 夜間ロードも今の材料を読んだ = どれも記録あり = rule・区分は社内のまま
  const y = await day('2030-09-03', { ne: ne1, material: mat(), ownership: own });
  check(y.ne, () => 'rule');
  assert.deepEqual([y.ne.sku_kind_raw_mismatch.codes.filter((c) => c !== 'c003'), y.ne.sku_kind_raw_mismatch.alert], [['e005', 'f006', 's001', 's002'], true]);   // 例外が絡む差も生の数に (c003 = 前の試験の NE の区分替えの残り)
  wh().prepare("DELETE FROM exception_genka WHERE sku IN ('f006', 's001')").run();
  assert.deepEqual((await db.query("select code, sku_kind from core.skus where code in ('e005', 's002', 'f006', 's001') order by code")).rows.map((r) => [r.code, r.sku_kind]),
    [['e005', 'exception'], ['f006', 'single'], ['s001', 'set'], ['s002', 'exception']]);
});

await ta('[36] 区分のゲートの数は今朝の生の集合 (商品の表 P・セットの表 S・例外の表 E) から数える: 前夜のロードは 0 でも、今朝だけ空のコード・正規化の重なり・壊れたセットがある朝に数える / 例外原価の行は区分不明にしない (NE 単品 + 例外原価・NE セット + 例外原価) / P・S に無い例外商品は例外として比べる (Codex R10 High・R12 High・v11 §3.6.4)', async () => {
  const NE = baseNe();
  await day('2030-10-01', { ne: NE });   // 前夜も今朝も きれい
  // Company DB に例外の商品 x999 (例外) と x998 (単品) を足す (NE は例外の表にだけ持つ)
  await db.query("insert into core.skus (company_id, sku_kind, code, name) values (1, 'exception', 'x999', '例外 x999')");
  const xp = (await db.query("insert into core.products (company_id, display_code, name, status) values (1, 'x998', '単品 x998', 'active') returning product_id")).rows[0].product_id;
  await db.query("insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'x998', '単品 x998')", [xp]);
  const ne1 = clone(NE);
  ne1.products.push({ code: '', name: '空のコード', supplier: '', handling: '取扱中', cost_src: J('1'), price_src: J('1'), tax_src: J('10'), rep: '', rep_src: J('') });   // 空のコード = integrity_untrusted
  ne1.products.push({ code: 'Ｂ００２', name: '全角の b002', supplier: '', handling: '取扱中', cost_src: J('1'), price_src: J('1'), tax_src: J('10'), rep: '', rep_src: J('') });   // 正規化の重なり (b002)
  ne1.sets.push({ parent: 'f006', name: 'f006 のセット', child: 'a001', price_src: J('100'), qty_src: J('1') });   // 商品の表にもある = セット (正常) → 社内は単品 = 区分の差
  ne1.sets.push({ parent: 'u777', name: '壊れたセット', child: '', price_src: J('100'), qty_src: J('1') });          // 子のコードが空の行だけのセット = integrity_untrusted に 1 行 (unknown_kind は今の母集合では必ず 0)
  // 例外の表: d004 (NE 単品 + 例外原価)・s002 (NE セット + 例外原価) = 原価の補完 = 区分不明にしない / x999・x998 = P・S に無い例外商品
  wh().prepare("INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES ('d004', 9, 'x', 'x'), ('s002', 9, 'x', 'x'), ('x999', 9, 'x', 'x'), ('x998', 9, 'x', 'x')").run();
  const x = await day('2030-10-02', { ne: ne1, material: toMaterial(NE) });
  const D = (await db.query("select payload from ops.load_decisions where ingest_run_id = 'load_2030-10-02' and section = 'skus'")).rows[0].payload;
  assert.deepEqual(D.sku_kind.unverifiable, []);   // 前夜のロード (前の材料) は 0
  // raw_mismatch = f006 (NE セット・社内単品)・x998 (NE 例外・社内単品)。d004・s002 (例外原価つき) と x999 (例外同士) は差にしない
  // affected = b002 (重なり・社内にある) だけ (u777 は社内に無い・空のコードは code_norm が無い)。integrity = 空のコードの行 + 子が空の行 = 2
  assert.deepEqual(x.ne.kind_gate, { raw_mismatch: 2, raw_unverifiable_affected_existing_cdb: 1, norm_collision: 1, unknown_kind: 0, integrity_untrusted: 2 });
  assert.deepEqual(x.ne.sku_kind_raw_mismatch.codes, ['f006', 'x998']);
  wh().prepare("DELETE FROM exception_genka WHERE sku IN ('d004', 's002', 'x999', 'x998')").run();
  await db.query("delete from core.skus where code in ('x999', 'x998')");
  await day('2030-10-03', { ne: NE });
});

await ta('[37] 区分の持ち主が C でも、登録待ち (下書き・NE 登録待ち) の新商品の区分違いは reg_kind_mismatch (#1635) = 1 つの SKU に区分の列は 1 つ (company_owned と二重に数えない)・区分のゲートの生の数には入る', async () => {
  const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
  const own = { ...MASTER_OWNERSHIP, 'skus.sku_kind': 'company' };
  const NE = baseNe();
  await day('2030-11-01', { ne: NE });
  // ポータルで単品として登録した新商品 r901 (下書き)。NE はセットとして持つ
  await pg.query('begin');
  const pid = (await pg.query(`insert into core.products (company_id, display_code, name, status) values (1, 'r901', '新商品 r901', 'active') returning product_id`)).rows[0].product_id;
  const sid = (await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, 'single', 'r901', '新商品 r901') returning sku_id`, [pid])).rows[0].sku_id;
  await pg.query('select ops.create_sku_registration($1, $2)', [sid, 'naka@test']);
  await pg.query('commit');
  const ne1 = clone(NE);
  ne1.sets.push({ parent: 'r901', name: 'セット R', child: 'a001', price_src: J('3000'), qty_src: J('2') });
  const x = await day('2030-11-02', { ne: ne1, material: toMaterial(NE), ownership: own });
  const cc = col(x.ne, 'kind:r901', 'kind');
  assert.deepEqual([cc.length, cc[0]?.cls], [1, 'reg_kind_mismatch'], JSON.stringify(cc));   // company の分類 (company_owned) にはしない = 1 列
  assert.deepEqual(x.ne.decisions.filter((d) => d.subject_key === 'kind:r901').map((d) => d.reason_kind), ['reg_kind_mismatch']);
  assert.ok(x.ne.sku_kind_raw_mismatch.codes.includes('r901'));   // 生の数 (ゲート) には入る = 登録待ちでも区分の差は区分の差
  assert.equal(x.ne.kind_gate.raw_mismatch, x.ne.sku_kind_raw_mismatch.count);
  // 例外の分岐でも登録待ちが最優先 (#1641 Codex R1 Medium): 下書きの単品 r902・NE 登録待ちのセット r903 を、NE は例外 (例外の表だけ・今朝の材料の例外の行) で持つ
  const regSku = async (code, kind, state) => {
    await pg.query('begin');
    const pp = kind === 'single' ? (await pg.query(`insert into core.products (company_id, display_code, name, status) values (1, $1, $1, 'active') returning product_id`, [code])).rows[0].product_id : null;
    const s2 = (await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, $2, $3, $3) returning sku_id`, [pp, kind, code])).rows[0].sku_id;
    await pg.query('select ops.create_sku_registration($1, $2)', [s2, 'naka@test']);
    await pg.query('commit');
    if (state !== 'draft') {
      await pg.query('begin'); await pg.query("select set_config('ops.registration_protocol', '1', true)");
      await pg.query('update ops.master_registrations set state = $2 where sku_id = $1', [s2, state]); await pg.query('commit');
    }
  };
  await regSku('r902', 'single', 'draft'); await regSku('r903', 'set', 'ne_pending');
  assert.deepEqual((await db.query("select s.code, r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code in ('r902', 'r903') order by s.code")).rows.map((r) => [r.code, r.state]), [['r902', 'draft'], ['r903', 'ne_pending']]);
  wh().prepare("INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES ('r902', 9, 'x', 'x'), ('r903', 9, 'x', 'x')").run();
  const matEx = toMaterial(NE, (m) => { for (const [c, pid] of [['r902', 902], ['r903', 903]]) m.products.push({ 商品コード: c, 商品名: c, 商品区分: '例外', 取扱区分: '取扱中', 標準売価: null, 原価: 9, 原価ソース: '例外', 原価状態: 'OVERRIDDEN', 消費税率: 0.1, 税区分: 'STANDARD_10', product_id: pid }); });
  const y = await day('2030-11-03', { ne: NE, material: matEx, ownership: own });
  for (const [code, c] of [['r902', 'single'], ['r903', 'set']]) {
    const k = col(y.ne, `kind:${code}`, 'kind');
    assert.deepEqual(k.map((z) => [z.cls, z.n, z.c]), [['reg_kind_mismatch', 'exception', c]], `${code}: ${JSON.stringify(k)}`);
    const ds = y.ne.decisions.filter((d) => d.subject_key === `kind:${code}`);
    assert.deepEqual(ds.map((d) => d.reason_kind), ['reg_kind_mismatch']);   // company_owned は 0 件
    assert.ok(y.ne.reg_pending.kind_mismatch.some((e) => e.code === code && e.ne_kind === 'exception'));
  }
  wh().prepare("DELETE FROM exception_genka WHERE sku IN ('r902', 'r903')").run();
});

await ta('[38] 区分のゲートは本物の取込 (fetchProducts / fetchSetProducts) が raw 表に書く前に落とした行・重なりも数える: コードが空の商品・子が空の行だけのセット (親は intBlocked で affected に)・同じコードが 2 度 (#1641 Codex R1 High 2・#1642 の取得の件数)', async () => {
  fs.writeFileSync(path.join(tmp, 'ne-tokens.json'), JSON.stringify({ access_token: 'a', refresh_token: 'r' }));
  const api = { goods: [], setgoods: [] };
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, opts) => {
    const u = String(url);
    if (!u.startsWith('https://api.next-engine.org')) return realFetch(url, opts);
    const q = new URLSearchParams(opts.body); const offset = Number(q.get('offset')), limit = Number(q.get('limit'));
    const rows = (u.endsWith('/api_v1_master_goods/search') ? api.goods : api.setgoods).slice(offset, offset + limit);
    return { ok: true, status: 200, json: async () => ({ result: 'success', data: rows }) };
  };
  try {
    const { fetchProducts, fetchSetProducts } = await quietly(() => import('../apps/warehouse/ne-api.js'));
    const NE = baseNe();
    const goodsOf = (r) => ({ goods_id: r.code, goods_name: r.name, goods_supplier_id: r.supplier, goods_cost_price: JSON.parse(r.cost_src), goods_selling_price: JSON.parse(r.price_src),
      goods_merchandise_name: r.handling, goods_representation_id: '', goods_tax_rate: JSON.parse(r.tax_src) });
    const setOf = (r) => ({ set_goods_id: r.parent, set_goods_name: r.name, set_goods_selling_price: JSON.parse(r.price_src), set_goods_detail_goods_id: r.child, set_goods_detail_quantity: JSON.parse(r.qty_src) });
    api.goods = [...NE.products.map(goodsOf),
      { goods_id: '', goods_name: 'コードが空' },                                  // 書く前に落とす (dropped_no_code)
      { ...goodsOf(NE.products.find((r) => r.code === 'c003')), goods_id: 'C003' }];   // 同じコードが 2 度 (小文字にして重なる = 上書き)
    api.setgoods = [...NE.sets.map(setOf),
      { set_goods_id: 'e005', set_goods_name: '子が空', set_goods_selling_price: '1', set_goods_detail_goods_id: '', set_goods_detail_quantity: '1' },   // 子が空の行だけ = 書く前に落とす・親は missing_child_parents
      { set_goods_id: 'z801', set_goods_name: '子が空', set_goods_selling_price: '1', set_goods_detail_goods_id: '', set_goods_detail_quantity: '1' }];   // 社内のセット z801 = 商品の表にも無い (保存した行が 1 つも無い親)
    await db.query("insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'z801', 'セット z801') on conflict do nothing");
    const asOf = new Date(Date.now() + 9 * 3600000).toISOString().slice(0, 10);   // 本物の取込の時刻 = 今日 (JST)
    await nightly(asOf);
    await quietly(() => fetchProducts()); await quietly(() => fetchSetProducts());
    const m = (k) => wh().prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value;
    // 取込は落とした行を raw 表に書かない (保存した行を数えるだけでは見えない)
    assert.equal(wh().prepare("SELECT COUNT(*) AS n FROM raw_ne_products WHERE 商品コード = ''").get().n, 0);
    assert.equal(wh().prepare("SELECT COUNT(*) AS n FROM raw_ne_set_products WHERE セット商品コード = 'e005'").get().n, 0);
    // 作り直しの記録 (本物の印)・Render へ送る・照合
    const pa = m('ne_api_products_complete_at'), sa = m('ne_api_setproducts_complete_at');
    wh().prepare('DELETE FROM m_products_builds').run();   // 前の試験の 2030 年の作り直しの記録 (今日より新しい) を外す = 今朝の作り直しがこの回
    const id = `mpb_${asOf.replace(/-/g, '')}_real38`; const pub = at(asOf, '08:07').toISOString();
    wh().prepare(`INSERT INTO m_products_builds (build_id, daily_sync_run_id, started_at, published_at, ne_products_complete_at, ne_setproducts_complete_at, products_rows, products_hash,
      set_components_rows, set_components_hash, rule_version, reason_counts, reasons, ne_products_complete_rev, ne_setproducts_complete_rev) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
      .run(id, `ds_${asOf}`, pub, pub, pa, sa, 0, 'x', 0, 'x', 'v', '{}', '[]', Number(m('ne_api_products_complete_rev')), Number(m('ne_api_setproducts_complete_rev')));
    sendToRender(toMaterial(NE), asOf, id);
    const r = await compare(asOf);
    const ne = r.result.ne;
    assert.notEqual(ne.verdict, 'blocked', ne.blocked_reason);
    const noAt = ({ complete_at, ...x }) => x;   // 完了の時刻 (ゲートの記録に渡す) は別に見る
    assert.deepEqual([ne.fetch_counts.ok, noAt(ne.fetch_counts.products), noAt(ne.fetch_counts.setproducts)],
      [true, { ok: true, dropped_no_code: 1, dropped_missing_fields: 0, overwritten: 1 }, { ok: true, dropped_no_code: 0, dropped_missing_fields: 2, overwritten: 0 }]);
    // integrity = 書く前に落とした 3 行 + 重なり 1 行 + 保持した SKU (intBlocked の c003・e005) の保存した商品の行 2 行 = 6
    //   affected = intBlocked の c003 (重なり)・e005・z801 (子が空のセットの親。保存した行が 1 つも無い z801 も) = Company DB にある 3 件
    assert.deepEqual([ne.kind_gate.integrity_untrusted, ne.kind_gate.raw_unverifiable_affected_existing_cdb], [6, 3]);
    // 取得の件数が読めない朝 (記録が消えた = PR-9 の前の取得) = 種類ごとに 1 (fail-closed)
    wh().prepare("DELETE FROM sync_meta WHERE key IN ('ne_api_products_fetch_counts', 'ne_api_setproducts_fetch_counts')").run();
    const r2 = await compare(asOf);
    assert.deepEqual([r2.result.ne.fetch_counts.products, r2.result.ne.fetch_counts.setproducts, r2.result.ne.kind_gate.integrity_untrusted],
      [{ ok: false, reason: 'no_record' }, { ok: false, reason: 'no_record' }, 2 + 2]);   // 読めない 2 種類 + 保持した SKU の保存した行 2
  } finally { globalThis.fetch = realFetch; }
});

await ta('[39] 照合 ② の始め (何かを読む前) に新商品の入口を閉じ (ops.close_new_entry_for_compare)、最後に同じ回でゲートの記録 (ops.record_new_entry_gate) を 1 行書く: 関数がある DB = 閉じる → 1 行 (照合の回・取得の完了の時刻 RFC 3339・kind_gate の 5 つ) / 無い DB (0058 の前と確かめた) = 照合は今までどおり (ℹ️) / 0058 があるのに閉じられない (関数が落ちる・書く接続が無い・有無を確かめられない) = 記録を呼ばない・要約の先頭に ⚠️・照合そのものは止めない', async () => {
  const NE = baseNe();
  // 試験の DB には 0058 がある = 本物の 2 つの関数の名前を試験の間だけ変えて「0058 の前」を作る (最後に戻す)
  const REAL = [['ops.close_new_entry_for_compare(text)', 'close_new_entry_for_compare'], ['ops.record_new_entry_gate(text, text, text, jsonb)', 'record_new_entry_gate']];
  const hidden = [];
  for (const [sig, name] of REAL) {
    if ((await db.query('select to_regprocedure($1) is not null as ok', [sig])).rows[0].ok) { await db.exec(`alter function ${sig} rename to ${name}__0058_hidden`); hidden.push([sig, name]); }
  }
  try {
  // 関数が無い DB (0058 の前) = 照合は通る・状態は not_applied
  const a = await day('2030-12-01', { ne: NE });
  assert.notEqual(a.result.ne.verdict, 'blocked', a.result.ne.blocked_reason);
  assert.deepEqual([a.ne.gate_close, a.ne.gate_record], [{ state: 'not_applied' }, { state: 'not_applied' }]);
  assert.match(a.line, /ℹ️ 新商品のゲートの記録: 関数が無い \(0058 の前\)/);
  // 関数がある DB (PR-1 の関数の代わりの fixture = 呼ばれた順に 1 行ずつ残す)。閉じる関数は、その時点で照合の読む今朝の NE の取得の印がまだ読まれていないことを見るため、呼ばれた時刻だけ残す
  await db.exec(`create table ops.test_new_entry_log (seq serial primary key, fn text, compare_run_id text, read_started boolean, products_complete_at text, setproducts_complete_at text, kind_gate jsonb);
    create function ops.close_new_entry_for_compare(p_compare_run_id text) returns jsonb language sql as $$
      insert into ops.test_new_entry_log (fn, compare_run_id, read_started) values ('close', p_compare_run_id, exists (select 1 from ops.ne_reg_compare_targets t where t.compare_run_id = p_compare_run_id))
      returning jsonb_build_object('closed', true) $$;   -- read_started = 照合の回の最初の読み (登録の確かめ待ちの写し) が済んでいたか
    create function ops.record_new_entry_gate(p_compare_run_id text, p_products_complete_at text, p_setproducts_complete_at text, p_kind_gate jsonb) returns jsonb language sql as $$
      insert into ops.test_new_entry_log (fn, compare_run_id, products_complete_at, setproducts_complete_at, kind_gate) values ('record', p_compare_run_id, p_products_complete_at, p_setproducts_complete_at, p_kind_gate)
      returning jsonb_build_object('result_id', 1) $$;`);
  try {
    const b = await day('2030-12-02', { ne: NE });
    const rows = (await db.query('select * from ops.test_new_entry_log order by seq')).rows;
    assert.deepEqual(rows.map((r) => [r.fn, r.compare_run_id]), [['close', b.result.compare_run_id], ['record', b.result.compare_run_id]]);   // 始めに閉じる → 最後に 1 行・同じ回
    assert.equal(rows[0].read_started, false);   // 閉じたのは回の最初の読みより前
    assert.equal((await db.query('select count(*)::int as n from ops.ne_reg_compare_targets where compare_run_id = $1', [b.result.compare_run_id])).rows[0].n, 1);   // 最初の読みはその後に走った
    const ts = utcText(at('2030-12-02', '07:00')).replace(' ', 'T') + 'Z';
    assert.deepEqual([rows[1].products_complete_at, rows[1].setproducts_complete_at, rows[1].kind_gate], [ts, ts, b.ne.kind_gate]);
    assert.deepEqual(Object.keys(rows[1].kind_gate).sort(), ['integrity_untrusted', 'norm_collision', 'raw_mismatch', 'raw_unverifiable_affected_existing_cdb', 'unknown_kind']);
    assert.deepEqual([b.ne.gate_close.state, b.ne.gate_record.state], ['ok', 'ok']);
    assert.doesNotMatch(b.line, /新商品の(ゲートの記録|入口を閉じられない)/);
    // ② 関数があり閉じるのが落ちる (0058 あり) = 閉じていない = 記録 (record) を呼ばない・要約の先頭に ⚠️・照合そのものは止めない (#1641 Codex R3 High)
    await db.exec(`create or replace function ops.close_new_entry_for_compare(p_compare_run_id text) returns jsonb language plpgsql as $$ begin raise exception 'close_failed: 閉じられない'; end $$;`);
    const c = await day('2030-12-03', { ne: NE });
    assert.notEqual(c.result.ne.verdict, 'error');
    assert.equal(c.result.ne.kind_gate.raw_mismatch, b.ne.kind_gate.raw_mismatch);   // 照合の本体の結果は変えない
    assert.deepEqual([c.ne.gate_close.state, c.ne.gate_close.stage, c.ne.gate_record.state], ['failed', 'close', 'skipped_not_closed']);
    assert.match(c.line, /^⚠️ 新商品の入口を閉じられない \(前日の許可が残りうる\): 書けない \(.*close_failed/);
    assert.equal((await db.query("select count(*)::int as n from ops.test_new_entry_log where fn = 'record'")).rows[0].n, 1);   // 記録の関数は動くのに呼ばれない (前の 1 行のまま)
    // ③ 書く接続が無い・関数はある (0058 あり) = not_configured = 記録を呼ばない・⚠️
    await db.exec(`create or replace function ops.close_new_entry_for_compare(p_compare_run_id text) returns jsonb language sql as $$
      insert into ops.test_new_entry_log (fn, compare_run_id) values ('close', p_compare_run_id) returning jsonb_build_object('closed', true) $$;`);
    const d3 = await day('2030-12-04', { ne: NE, compareExtra: { writerDb: null } });
    assert.deepEqual([d3.ne.gate_close.state, d3.ne.gate_close.fn, d3.ne.gate_record.state], ['not_configured', 'present', 'skipped_not_closed']);
    assert.match(d3.line, /^⚠️ 新商品の入口を閉じられない \(前日の許可が残りうる\): 書く接続が無い/);
    assert.equal((await db.query("select count(*)::int as n from ops.test_new_entry_log where fn = 'record'")).rows[0].n, 1);
    // ④ 関数の有無を確かめられない (接続・権限の失敗) = 「0058 の前」と取り違えない = failed・記録を呼ばない・⚠️ (関数を消しておいても not_applied にしない)
    await db.exec('drop function ops.close_new_entry_for_compare(text);');
    const denied = { query: async (sql, p) => { if (/to_regprocedure\('ops\.close_new_entry_for_compare/.test(sql)) throw new Error('permission denied for schema ops'); return db.query(sql, p); } };
    const d4 = await day('2030-12-05', { ne: NE, compareExtra: { writerDb: denied } });
    assert.deepEqual([d4.ne.gate_close.state, d4.ne.gate_close.stage, d4.ne.gate_record.state], ['failed', 'presence', 'skipped_not_closed']);
    assert.match(d4.line, /^⚠️ 新商品の入口を閉じられない \(前日の許可が残りうる\): 書けない \(permission denied/);
    assert.equal((await db.query("select count(*)::int as n from ops.test_new_entry_log where fn = 'record'")).rows[0].n, 1);
  } finally {
    await db.exec('drop function if exists ops.record_new_entry_gate(text, text, text, jsonb); drop function if exists ops.close_new_entry_for_compare(text); drop table if exists ops.test_new_entry_log;');
  }
  // ① の続き: 関数が無いと確かめた朝 (今の本番) = 先頭は ⚠️ にしない (今までどおり)
  const e = await day('2030-12-06', { ne: NE });
  assert.deepEqual([e.ne.gate_close, e.ne.gate_record], [{ state: 'not_applied' }, { state: 'not_applied' }]);
  assert.doesNotMatch(e.line, /新商品の入口を閉じられない/);
  } finally {
    for (const [sig, name] of hidden) await db.exec(`alter function ${sig.replace(name, `${name}__0058_hidden`)} rename to ${name}`);
  }
  for (const [sig] of REAL) assert.equal((await db.query('select to_regprocedure($1) is not null as ok', [sig])).rows[0].ok, true, `${sig} を戻した`);
});

await pg.close();
try { WH.getDB().close(); } catch { /* */ }
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
