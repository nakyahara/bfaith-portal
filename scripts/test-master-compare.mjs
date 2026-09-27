/**
 * test-master-compare.mjs — 毎朝のマスタ照合 ①ロードの検証 (Company DB構想 10 §6.1.1 B。apps/company-db/master-compare)
 *
 * 固定する契約:
 *   1 夜間ロードの直後は差 0 (pass)。ロードの判断 (0030) を残す (飛ばした SKU・構成の書いた行 / manual で同じ行 / 削除まで行った親・代表の仕入先の対象全体)
 *   2 ロードの後の書き換えを種類ごとに拾う: 値 (名前) / 原価 / 代表の仕入先 / 構成の欠け・数量・削除まで行った親の余分な行 / SKU が無い
 *   3 ロードの時の判断を使う: manual で数量が同じ行・違う行は偽の異常にしない・削除まで行かなかった親の余分な行は拾わない・正規化の衝突で落とした表記は比べない
 *   4 ロードした回の持ち主・条件で比べる列を決める (company の列・0027 の無い回の列は比べない)
 *   5 判定できないときは blocked (夜間ロードが無い・今日でない・規則の指紋違い・0030 が無い・判断の欠け・材料が matched でない・控えが無い / 壊れている)
 *   6 実行口 (runCompare): 始めに「実行中」の証跡で前の結果を無効にし、全件 JSON (sha256) → 完了の証跡。失敗は failed の証跡を残して投げる
 *   7 代表 (親子。D3): 記録 (targets) と今の親・帰属を比べる / 記録の漏れ・材料の証跡の食い違い = blocked / 0035 の前のロードは比べない (blocked にしない) /
 *     送り手の意味の版 (src1) の材料だけ '' を明示の空と読んで外す (版の無い古い材料は不明 = 外さない)
 * 使い方: node scripts/test-master-compare.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mc-test-'));
process.env.DATA_DIR = tmp;
process.env.DAILY_SYNC_RUN_ID = 'ds_test_1';

const { default: Database } = await import('better-sqlite3');
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { buildPlanFromRender } = await import('../apps/company-db/load/sources.mjs');
const { runInitialLoad, LOAD_RULE_FINGERPRINT } = await import('../apps/company-db/load/engine.mjs');
const { buildMaterialGeneration, saveMaterialSnapshot, materialDigest, projectMaterialRows, MATERIAL_COLUMNS, MATERIAL_DIR_NAME } = await import('../apps/warehouse/material-lineage.js');
const { MIRROR_PRODUCTS_DDL, MIRROR_SET_COMPONENTS_DDL } = await import('../apps/warehouse-mirror/material-tables.js');
const { compareLoad, sameValue, subjectKey } = await import('../apps/company-db/master-compare/compare-load.mjs');
const { runCompare, EVIDENCE_NAME, sha256 } = await import('../apps/company-db/master-compare/run.mjs');
const { readEvidence } = await import('../apps/company-db/push/evidence.mjs');
const { jstDateStr } = await import('../lib/jst-date.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const asOf = jstDateStr(new Date());

// ── Render の mirror (受け手が確かめて世代を記録した状態) と miniPC の控え ──
const P = [
  { product_id: 1, 商品コード: 'a001', 商品名: '単品A', 商品区分: '単品', 取扱区分: '取扱中', 標準売価: 1000, 原価: 100, 原価ソース: 'NE', 原価状態: 'COMPLETE', 送料: 200, 送料コード: 'S1', 配送方法: 'ネコポス', 消費税率: 0.1, 税区分: 'STANDARD_10', 仕入先コード: '0001' },
  { product_id: 2, 商品コード: 'b002', 商品名: '単品B', 商品区分: '単品', 取扱区分: '取扱中', 標準売価: 2000, 原価: 555, 原価ソース: '例外', 原価状態: 'OVERRIDDEN', 消費税率: 0.1, 税区分: 'STANDARD_10', 仕入先コード: '0002' },
  { product_id: 3, 商品コード: 's001', 商品名: 'セット1', 商品区分: 'セット', 取扱区分: '取扱中', 標準売価: 3000, 原価: 755, 原価ソース: 'セット計算', 原価状態: 'COMPLETE', 消費税率: 0.1, 税区分: 'STANDARD_10' },
  { product_id: 4, 商品コード: 's002', 商品名: 'セット2', 商品区分: 'セット', 取扱区分: '取扱中', 標準売価: 900, 原価: 100, 原価ソース: 'セット計算', 原価状態: 'COMPLETE', 消費税率: 0.1, 税区分: 'STANDARD_10' },
  { product_id: 5, 商品コード: 'Dup1', 商品名: '衝突の勝ち', 商品区分: '単品', 取扱区分: '取扱中', 原価状態: 'MISSING', 消費税率: 0.1, 税区分: 'STANDARD_10' },
  { product_id: 6, 商品コード: 'dup1', 商品名: '衝突の負け', 商品区分: '単品', 取扱区分: '取扱中', 原価状態: 'MISSING', 消費税率: 0.1, 税区分: 'STANDARD_10' },
  { product_id: 7, 商品コード: 'c003', 商品名: '原価も仕入先も無い単品', 商品区分: '単品', 取扱区分: '取扱中', 原価状態: 'MISSING', 消費税率: 0.1, 税区分: 'STANDARD_10' },
];
const S = [
  { セット商品コード: 's001', 構成商品コード: 'a001', 数量: 2 }, { セット商品コード: 's001', 構成商品コード: 'b002', 数量: 1 },
  { セット商品コード: 's002', 構成商品コード: 'a001', 数量: 1 },
];
const mirrorFile = path.join(tmp, 'warehouse-mirror.db');
/** mirror を材料 P・S で入れ替え、受け手と同じく中身から出し直したハッシュで世代を記録し、控えを残す */
function publishMaterial(products = P, sets = S, { semantics = null, complete = null } = {}) {
  const m = new Database(mirrorFile);
  try {
    m.exec(MIRROR_PRODUCTS_DDL); m.exec(MIRROR_SET_COMPONENTS_DDL);
    m.exec(`CREATE TABLE IF NOT EXISTS mirror_material_generations (entity TEXT PRIMARY KEY, generation_id TEXT NOT NULL, content_hash TEXT NOT NULL, row_count INTEGER NOT NULL,
      source_complete_at TEXT, created_at TEXT, received_at TEXT NOT NULL)`);
    try { m.exec('ALTER TABLE mirror_material_generations ADD COLUMN semantics TEXT'); } catch { /* もうある */ }
    const g = buildMaterialGeneration({ products, set_components: sets, productsSemantics: semantics, neProductsCompleteAt: complete, neSetProductsCompleteAt: complete });
    saveMaterialSnapshot({ dataDir: tmp, generation: g, products, set_components: sets });
    m.transaction(() => {
      m.exec('DELETE FROM mirror_products; DELETE FROM mirror_set_components; DELETE FROM mirror_material_generations');
      const put = (table, cols, rows) => { const st = m.prepare(`INSERT INTO ${table} (${[...cols, 'updated_at'].map((c) => `"${c}"`).join(', ')}) VALUES (${[...cols, 'updated_at'].map(() => '?').join(', ')})`); for (const r of rows) st.run(...cols.map((c) => r[c]), 'x'); };
      put('mirror_products', MATERIAL_COLUMNS.products, projectMaterialRows('products', products));
      put('mirror_set_components', MATERIAL_COLUMNS.set_components, projectMaterialRows('set_components', sets));
      for (const e of ['products', 'set_components']) {
        const d = materialDigest(e, m.prepare(`SELECT * FROM mirror_${e}`).all());
        m.prepare('INSERT INTO mirror_material_generations (entity, generation_id, content_hash, row_count, source_complete_at, created_at, received_at, semantics) VALUES (?,?,?,?,?,?,?,?)')
          .run(e, g.generation_id, d.content_hash, d.row_count, g[e].source_complete_at ?? null, g.created_at, 'x', e === 'products' && g.products.semantics ? JSON.stringify(g.products.semantics) : null);
      }
    })();
    return g;
  } finally { m.close(); }
}
const nightly = (db, runId, opts = {}) => runInitialLoad(db, buildPlanFromRender({ dataDir: tmp, log: quiet }), { log: quiet, runId, host: 'render-nightly', ...opts });
async function compareIn(db, opts = {}) {
  await db.query('begin transaction isolation level repeatable read read only');
  try { return await compareLoad({ db, dataDir: tmp, asOfJst: asOf, ...opts }); } finally { await db.query('rollback'); }
}
const byType = (r, t) => r.items.filter((i) => i.type === t);

const pg = new PGlite(); const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
publishMaterial();

await ta('[1] 夜間ロードの直後は差 0 (pass)。ロードの判断 (0030) を残す', async () => {
  const r0 = await nightly(db, 'load_mc_1');
  assert.equal(r0.ok, true, r0.error);
  const dec = Object.fromEntries((await db.query("select section, format, payload from ops.load_decisions where ingest_run_id = 'load_mc_1'")).rows.map((x) => [x.section, x]));
  assert.deepEqual(Object.keys(dec).sort(), ['primary_suppliers', 'set_components', 'sku_costs', 'skus', 'variation_parents']);   // variation_parents = 0035 の後 (D3)
  assert.equal(dec.skus.format, 'ld-v1');
  assert.deepEqual(dec.skus.payload.skipped.map(([c, why]) => [c, why]), [['dup1', 'norm_collision']]);
  assert.deepEqual(dec.set_components.payload.prune_parents.map(([c]) => c).sort(), ['s001', 's002']);
  assert.equal(dec.set_components.payload.rows.length, 3);
  assert.equal(dec.primary_suppliers.payload.applied, true);
  assert.deepEqual(dec.primary_suppliers.payload.targets.map(([s, p]) => [s, p]).sort(), [['a001', '0001'], ['b002', '0002']]);
  const r = await compareIn(db);
  assert.equal(r.verdict, 'pass', JSON.stringify(r.items, null, 1) + r.blocked_reason);
  assert.equal(r.load.ingest_run_id, 'load_mc_1');
  assert.equal(r.counts.compared.value, 6);   // a001 b002 s001 s002 Dup1 c003 (dup1 は落とした表記)
  assert.ok(!r.compared.value.includes('dup1') || r.compared.value.filter((x) => x === 'dup1').length === 1);
  assert.equal(r.counts.compared.cost, 4); assert.equal(r.counts.compared.primary_supplier, 2); assert.equal(r.counts.compared.components, 2);
});

await ta('[2] ロードの後の書き換えを種類ごとに拾う (値・原価・代表の仕入先・構成の欠け / 余分)', async () => {
  await db.query("update core.skus set name = '人が直した名前' where code = 'a001'");
  await db.query("update core.sku_costs set cost_jpy = 999 where valid_to is null and sku_id = (select sku_id from core.skus where code = 'b002')");
  await db.query("update core.supplier_skus set is_primary = false where sku_id = (select sku_id from core.skus where code = 'a001')");
  await db.query("delete from core.sku_components where parent_sku_id = (select sku_id from core.skus where code = 's001') and child_sku_id = (select sku_id from core.skus where code = 'b002')");
  await db.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) select 1, p.sku_id, c.sku_id, 3, 'imported'
    from core.skus p, core.skus c where p.code = 's002' and c.code = 'b002'`);   // 削除まで行った親に余分な行
  const r = await compareIn(db);
  assert.equal(r.verdict, 'breach');
  const v = byType(r, 'value'); assert.equal(v.length, 1); assert.deepEqual(v[0].diffs, [{ col: 'name', expected: '単品A', actual: '人が直した名前' }]);
  assert.equal(v[0].subject_key, subjectKey('value', 'a001'));
  assert.ok(v[0].change_candidates && v[0].change_candidates.some((e) => e.attribute === 'name'));   // 変更の記録の候補
  const c = byType(r, 'cost'); assert.equal(c.length, 1); assert.equal(c[0].norm, 'b002'); assert.deepEqual(c[0].diffs, [{ col: 'cost_jpy', expected: 555, actual: 999 }]);
  const p = byType(r, 'primary_supplier'); assert.equal(p.length, 1); assert.equal(p[0].norm, 'a001'); assert.deepEqual(p[0].actual, []);
  const k = byType(r, 'components').sort((x, y) => x.norm.localeCompare(y.norm));
  assert.equal(k.length, 2);
  assert.deepEqual(k[0].missing, [{ child: 'b002', qty: 1 }]); assert.equal(k[0].norm, 's001');
  assert.deepEqual(k[1].extra.map((x) => x.child), ['b002']); assert.equal(k[1].norm, 's002');
  // SKU が無い (材料にあるのに Company DB に無い)
  await db.query("delete from core.skus where code = 'c003'");
  const rm = await compareIn(db);
  assert.deepEqual(byType(rm, 'missing').map((i) => [i.code, i.subject_key]), [['c003', 'missing:c003']]);
  // 次のロードで元に戻る (ロードは load の列を材料どおりに直す) → 差 0
  assert.equal((await nightly(db, 'load_mc_2')).ok, true);
  const r2 = await compareIn(db);
  assert.equal(r2.verdict, 'pass', JSON.stringify(r2.items, null, 1));
  assert.equal(r2.load.ingest_run_id, 'load_mc_2');
});

await ta('[3] ロードの時の判断を使う: manual の行 (数量が同じ / 違う) を偽の異常にしない・削除まで行かなかった親の余分は拾わない', async () => {
  // s001 に manual の行: (s001, a001) 数量 2 = 材料と同じ / (s001, b002) 数量 5 = 材料 (1) と違う → s001 は削除まで行かない親になる
  await db.query(`update core.sku_components set source = 'manual' where parent_sku_id = (select sku_id from core.skus where code = 's001') and child_sku_id = (select sku_id from core.skus where code = 'a001')`);
  await db.query(`update core.sku_components set source = 'manual', qty = 5 where parent_sku_id = (select sku_id from core.skus where code = 's001') and child_sku_id = (select sku_id from core.skus where code = 'b002')`);
  assert.equal((await nightly(db, 'load_mc_3')).ok, true);
  const dec = (await db.query("select payload from ops.load_decisions where ingest_run_id = 'load_mc_3' and section = 'set_components'")).rows[0].payload;
  assert.deepEqual(dec.prune_parents.map(([c]) => c), ['s002']);
  assert.ok(dec.rows.some(([p, c, q, kind]) => p === 's001' && c === 'a001' && q === 2 && kind === 'manual_same'));
  assert.ok(dec.skipped.some(([p, c, why]) => p === 's001' && c === 'b002' && why === 'manual_qty_mismatch'));
  // 削除まで行かなかった親 (s001) に余分な行を足しても拾わない (ロードもそのまま残す)
  await db.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) select 1, p.sku_id, c.sku_id, 1, 'imported'
    from core.skus p, core.skus c where p.code = 's001' and c.code = 's002'`);
  const r = await compareIn(db);
  assert.equal(r.verdict, 'pass', JSON.stringify(r.items, null, 1));
  // manual で数量が同じだった行を人が変えたら拾う (ロードが保証しようとした行)
  await db.query(`update core.sku_components set qty = 7 where parent_sku_id = (select sku_id from core.skus where code = 's001') and child_sku_id = (select sku_id from core.skus where code = 'a001')`);
  const r2 = await compareIn(db);
  assert.deepEqual(byType(r2, 'components').map((i) => [i.norm, i.qty]), [['s001', [{ child: 'a001', expected: 2, actual: 7 }]]]);
  await db.query(`update core.sku_components set qty = 2 where parent_sku_id = (select sku_id from core.skus where code = 's001') and child_sku_id = (select sku_id from core.skus where code = 'a001')`);
});

await ta('[4] ロードした回の持ち主・条件で比べる列を決める (company の列・0027 の無い回の列は比べない)', async () => {
  const own = { ...(await import('../config/master-ownership.mjs')).MASTER_OWNERSHIP, 'skus.name': 'company' };
  assert.equal((await nightly(db, 'load_mc_4', { ownership: own })).ok, true);
  await db.query("update core.skus set name = '持ち主が company の名前' where code = 'b002'");
  let r = await compareIn(db);
  assert.equal(r.verdict, 'pass', JSON.stringify(r.items, null, 1));
  // ロードの時に 0027 が無かった (load_conditions.has0027 = false) なら標準売価は比べない
  await db.query("update core.skus set standard_price_jpy = 1 where code = 'a001'");
  r = await compareIn(db);
  assert.deepEqual(byType(r, 'value').map((i) => i.diffs.map((d) => d.col)), [['standard_price_jpy']]);
  await db.query("update ops.load_materials set load_conditions = jsonb_set(load_conditions, '{has0027}', 'false') where ingest_run_id = 'load_mc_4'");
  // 条件と判断が食い違う (0027 が無いのに代表の仕入先を付けた記録) = 形がおかしい → blocked
  r = await compareIn(db);
  assert.deepEqual([r.verdict, r.blocked_reason, r.section], ['blocked', 'decisions_malformed', 'primary_suppliers_owner']);
  await db.query(`update ops.load_decisions set payload = '{"applied": false, "reason_code": "no_0027"}'::jsonb where ingest_run_id = 'load_mc_4' and section = 'primary_suppliers'`);
  r = await compareIn(db);
  assert.equal(r.verdict, 'pass', JSON.stringify(r.items, null, 1));
  assert.equal(sameValue(0.1, '0.10'), true); assert.equal(sameValue(null, undefined), true); assert.equal(sameValue(0, null), false);
});

await ta('[5] 判定できないときは blocked (規則の指紋違い・今日でない・判断の欠け・控えが無い / 壊れた・材料が matched でない・0030 が無い・ロードが無い)', async () => {
  assert.equal((await nightly(db, 'load_mc_5')).ok, true);
  assert.equal((await compareIn(db)).verdict, 'pass');
  assert.equal((await compareIn(db, { localFingerprint: 'f'.repeat(64) })).blocked_reason, 'rule_mismatch');
  assert.equal((await compareIn(db, { asOfJst: '2099-01-01' })).blocked_reason, 'stale_load');
  await db.query("delete from ops.load_decisions where ingest_run_id = 'load_mc_5' and section = 'primary_suppliers'");
  let r = await compareIn(db); assert.equal(r.blocked_reason, 'no_decisions'); assert.equal(r.missing_section, 'primary_suppliers');
  await db.query("update ops.load_decisions set format = 'ld-v0' where ingest_run_id = 'load_mc_5' and section = 'skus'");
  // 判断の中身が欠けている (形は ld-v1 のまま) = 比べるものが無い、と読まない (Codex #1456 R1 Medium)
  assert.equal((await nightly(db, 'load_mc_5b')).ok, true);
  for (const [section, payload, want] of [
    ['sku_costs', '{}', 'sku_costs'],
    ['set_components', '{"owned": true}', 'set_components'],
    ['sku_costs', '{"owned": false, "skipped": []}', 'sku_costs_owner'],
  ]) {
    const before = (await db.query("select payload from ops.load_decisions where ingest_run_id = 'load_mc_5b' and section = $1", [section])).rows[0].payload;
    await db.query("update ops.load_decisions set payload = $2::jsonb where ingest_run_id = 'load_mc_5b' and section = $1", [section, payload]);
    const x = await compareIn(db);
    assert.deepEqual([x.verdict, x.blocked_reason, x.section], ['blocked', 'decisions_malformed', want]);
    await db.query("update ops.load_decisions set payload = $2::jsonb where ingest_run_id = 'load_mc_5b' and section = $1", [section, JSON.stringify(before)]);
  }
  assert.equal((await compareIn(db)).verdict, 'pass');
  // 控えが無い / 壊れている
  assert.equal((await nightly(db, 'load_mc_6')).ok, true);
  const gen = (await db.query("select generation_id from ops.load_materials where ingest_run_id = 'load_mc_6' and entity = 'products'")).rows[0].generation_id;
  const snap = path.join(tmp, MATERIAL_DIR_NAME, `${gen}.json.gz`);
  const saved = fs.readFileSync(snap);
  fs.writeFileSync(snap, Buffer.from('broken'));
  assert.equal((await compareIn(db)).blocked_reason, 'snapshot_unreadable');
  fs.rmSync(snap);
  assert.equal((await compareIn(db)).blocked_reason, 'snapshot_missing');
  fs.writeFileSync(snap, saved);
  assert.equal((await compareIn(db)).verdict, 'pass');
  // 材料が matched でない (mirror が受信のあと書き換えられた)
  const m = new Database(mirrorFile); m.prepare("UPDATE mirror_products SET 商品名 = '受信のあと' WHERE 商品コード = 'a001'").run(); m.close();
  assert.equal((await nightly(db, 'load_mc_7')).ok, true);
  assert.equal((await compareIn(db)).blocked_reason, 'material_not_matched');
  publishMaterial();
  // 0030 が無い (0029 まで) / 夜間ロードが無い (手動のロードは使わない)
  const pg0 = new PGlite(); const db0 = pgliteAdapter(pg0);
  await applyMigrations(db0, { log: quiet, to: '0029' });
  assert.equal((await compareIn(db0)).blocked_reason, 'no_nightly_load');
  assert.equal((await runInitialLoad(db0, buildPlanFromRender({ dataDir: tmp, log: quiet }), { log: quiet, runId: 'load_manual', host: 'someone' })).ok, true);
  assert.equal((await compareIn(db0)).blocked_reason, 'no_nightly_load');
  assert.equal((await nightly(db0, 'load_mc_0029')).ok, true);
  assert.equal((await compareIn(db0)).blocked_reason, 'no_0030');
  await pg0.close();
  assert.equal(LOAD_RULE_FINGERPRINT.length, 64);
});

await ta('[6] 実行口: 始めに「実行中」の証跡で前の結果を無効にし、全件 JSON → 完了の証跡。失敗は failed を残して投げる', async () => {
  assert.equal((await nightly(db, 'load_mc_8')).ok, true);
  const r = await runCompare({ db, dataDir: tmp, asOf });
  assert.equal(r.evidence.state, 'complete'); assert.equal(r.evidence.verdict, 'pass');
  const ev = readEvidence(tmp, asOf)[EVIDENCE_NAME];
  assert.equal(ev.compare_run_id, r.evidence.compare_run_id); assert.equal(ev.sync_run_id, 'ds_test_1');
  const buf = fs.readFileSync(path.join(tmp, ev.json_path));
  assert.equal(sha256(buf), ev.sha256);
  assert.equal(JSON.parse(buf.toString('utf8')).compare_run_id, ev.compare_run_id);
  assert.match(r.line, /✅ マスタ照合 ①/);   // この試験には NE (warehouse.db) が無い = ② は判定できない → 要約は「⚠️ ② … / ✅ ①」(② の試験 = test-master-compare-ne.mjs)
  assert.equal(r.evidence.ne.verdict, 'blocked'); assert.equal(r.evidence.verdict, 'pass');
  // 同じ実行 ID の retry で照合が失敗 → 前の complete は残らない (failed に置き換わる)
  await assert.rejects(runCompare({ db, dataDir: tmp, asOf, compare: async () => { throw new Error('DB に届かない'); } }), /DB に届かない/);
  const ev2 = readEvidence(tmp, asOf)[EVIDENCE_NAME];
  assert.equal(ev2.state, 'failed'); assert.notEqual(ev2.compare_run_id, ev.compare_run_id);
  // 接続・初期設定で失敗しても、前の complete は残らない (「実行中」は接続より前。Codex #1456 R1 High-1)
  const ok2 = await runCompare({ db, dataDir: tmp, asOf });
  assert.equal(readEvidence(tmp, asOf)[EVIDENCE_NAME].state, 'complete');
  await assert.rejects(runCompare({ connect: async () => { throw new Error('ECONNREFUSED'); }, dataDir: tmp, asOf }), /ECONNREFUSED/);
  const ev3 = readEvidence(tmp, asOf)[EVIDENCE_NAME];
  assert.equal(ev3.state, 'failed'); assert.notEqual(ev3.compare_run_id, ok2.evidence.compare_run_id); assert.match(ev3.error, /ECONNREFUSED/);
  // 接続できたら閉じる (成功でも失敗でも)
  let closed = 0;
  await runCompare({ connect: async () => ({ db, close: async () => { closed++; } }), dataDir: tmp, asOf });
  await assert.rejects(runCompare({ connect: async () => ({ db, close: async () => { closed++; } }), dataDir: tmp, asOf, compare: async () => { throw new Error('x'); } }));
  assert.equal(closed, 2);
  // 始めの証跡が書けない = 前の結果を無効にできない → 照合しない
  let called = false;
  await assert.rejects(runCompare({ db, dataDir: tmp, asOf, write: () => null, compare: async () => { called = true; return {}; } }), /実行中/);
  assert.equal(called, false);
});

// 代表 (親子。D3): a001・b002 の代表 = grp (名札)・c003 = 明示の空 ('')・Dup1 = 代表なし (NULL)
const P3 = P.map((r) => ({ ...r, 代表商品コード: { a001: 'grp', b002: 'grp', c003: '' }[r.商品コード] ?? null }));
const asParentWriter = async (fn) => {
  await db.exec('begin');
  try { await db.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())"); await fn(); await db.exec('commit'); }
  catch (e) { await db.exec('rollback'); throw e; }
};
const parentPayload = async (runId) => (await db.query("select payload from ops.load_decisions where ingest_run_id = $1 and section = 'variation_parents'", [runId])).rows[0].payload;
const setParentPayload = async (runId, p) => db.query("update ops.load_decisions set payload = $2::jsonb where ingest_run_id = $1 and section = 'variation_parents'", [runId, JSON.stringify(p)]);

await ta('[7] 代表 (親子): 記録 (targets) の親と帰属を今と比べる。held は理由つきで対象外。人が変えたら差 (親の違い・帰属の違い)', async () => {
  publishMaterial(P3, S, { semantics: { rep: 'src1' }, complete: '2026-09-27 00:00:00' });
  assert.equal((await nightly(db, 'load_mc_p1')).ok, true);
  const pv = await parentPayload('load_mc_p1');
  assert.deepEqual(pv.trusted, { matched: true, source_complete_at: '2026-09-27 00:00:00', rep_semantics: 'src1' });
  assert.deepEqual(pv.targets.map((x) => [x[0], x[2], x[3]]).sort(), [['a001', 'grp', 'load'], ['b002', 'grp', 'load'], ['c003', null, null]]);
  assert.deepEqual(pv.held, [['Dup1', 'rep_unknown', null, null, null]]);   // 代表が NULL (NE の単品に無い・記録なし) = 不明 = 保持
  const r = await compareIn(db);
  assert.equal(r.verdict, 'pass', JSON.stringify(r.items, null, 1) + r.blocked_reason);
  assert.equal(r.counts.compared.parent, 3);
  assert.equal(r.exclusions['parent:dup1'], 'held:rep_unknown');
  assert.equal(r.parent_not_compared, undefined);
  // 人が帰属だけ変えた (親は同じ) = 差 / 親を外した = 差
  const pidOf = async (code) => Number((await db.query('select product_id from core.skus where code = $1', [code])).rows[0].product_id);
  await asParentWriter(async () => {
    await db.query("update core.products set parent_set_by = 'manual' where product_id = $1", [await pidOf('a001')]);
    await db.query('update core.products set parent_product_id = null, parent_set_by = null where product_id = $1', [await pidOf('b002')]);
  });
  const r2 = await compareIn(db);
  assert.equal(r2.verdict, 'breach');
  const it = Object.fromEntries(byType(r2, 'parent').map((i) => [i.code, [i.expected.set_by, i.actual.set_by, i.actual.parent_product_id === null]]));
  assert.deepEqual(it, { a001: ['load', 'manual', false], b002: ['load', null, true] });
  assert.equal(byType(r2, 'parent')[0].subject_key.startsWith('parent:'), true);
  // 次のロード: a001 は manual = 保持 (対象外)・b002 は付け直す → 差 0
  assert.equal((await nightly(db, 'load_mc_p2')).ok, true);
  const r3 = await compareIn(db);
  assert.equal(r3.verdict, 'pass', JSON.stringify(r3.items, null, 1) + r3.blocked_reason);
  assert.equal(r3.exclusions['parent:a001'], 'held:manual');
  assert.equal(r3.counts.compared.parent, 2);
  await asParentWriter(async () => db.query("update core.products set parent_set_by = 'load' where product_id = $1", [await pidOf('a001')]));
});

await ta('[8] 代表の記録の漏れ・余り・材料の証跡の食い違い・知らない理由 = blocked (decisions_malformed)。0035 の前のロード = 比べない (blocked にしない)', async () => {
  assert.equal((await nightly(db, 'load_mc_p3')).ok, true);
  const before = await parentPayload('load_mc_p3');
  for (const [mut, want] of [
    [(p) => ({ ...p, targets: p.targets.filter((x) => x[0] !== 'a001') }), 'variation_parents_coverage'],
    [(p) => ({ ...p, targets: [...p.targets, ['zzz', null, null, null]] }), 'variation_parents_coverage'],
    [(p) => ({ ...p, targets: [...p.targets, p.targets[0]] }), 'variation_parents_duplicate'],
    [(p) => ({ ...p, trusted: { ...p.trusted, source_complete_at: '2026-01-01 00:00:00' } }), 'variation_parents_trusted'],
    [(p) => ({ ...p, trusted: { ...p.trusted, matched: false } }), 'variation_parents_trusted'],
    [(p) => ({ ...p, trusted: { ...p.trusted, source_complete_at: null } }), 'variation_parents_trusted'],   // 記録に完了が無いのに材料の記録にはある
    [(p) => ({ ...p, trusted: { ...p.trusted, source_complete_at: 'きのう' } }), 'variation_parents'],   // 時刻の形でない (SQL に渡さない)
    [(p) => ({ ...p, held: [['a001', 'no_such_reason', null, null, null]], targets: p.targets.filter((x) => x[0] !== 'a001') }), 'variation_parents'],
    [(p) => ({ ...p, targets: p.targets.map((x) => (x[0] === 'a001' ? [x[0], x[1], x[2], null] : x)) }), 'variation_parents'],
    [(p) => ({ ...p, owned: false }), 'variation_parents_owner'],
  ]) {
    await setParentPayload('load_mc_p3', mut(before));
    const x = await compareIn(db);
    assert.deepEqual([x.verdict, x.blocked_reason, x.section], ['blocked', 'decisions_malformed', want], JSON.stringify(x.section));
  }
  await setParentPayload('load_mc_p3', before);
  assert.equal((await compareIn(db)).verdict, 'pass');
  // has0035 = true なのに section が無い = blocked
  await db.query("delete from ops.load_decisions where ingest_run_id = 'load_mc_p3' and section = 'variation_parents'");
  const nd = await compareIn(db);
  assert.deepEqual([nd.blocked_reason, nd.missing_section], ['no_decisions', 'variation_parents']);
  // 古いコードのロード (条件に has0035 が無い) = 代表は比べない。blocked にしない
  await db.query("update ops.load_materials set load_conditions = load_conditions - 'has0035' where ingest_run_id = 'load_mc_p3'");
  const old = await compareIn(db);
  assert.equal(old.verdict, 'pass', old.blocked_reason);
  assert.equal(old.parent_not_compared, 'no_0035');
  assert.equal(old.counts.compared.parent, 0);
});

await ta('[9] 送り手の意味の版: 版の無い古い材料の \'\' は不明 = 外さない (rep_unknown)。src1 の材料の \'\' は明示の空 = 帰属 load の親を外す', async () => {
  // c003 に親を付ける (材料の代表 = grp) → 版の無い材料で '' に戻しても外さない → src1 の材料で '' なら外す
  const P4 = P3.map((r) => (r.商品コード === 'c003' ? { ...r, 代表商品コード: 'grp' } : r));
  publishMaterial(P4, S, { semantics: { rep: 'src1' }, complete: '2026-09-27 00:00:00' });
  assert.equal((await nightly(db, 'load_mc_p4')).ok, true);
  const par = async () => (await db.query("select pp.display_code as d, p.parent_set_by as by from core.skus s join core.products p on p.product_id = s.product_id left join core.products pp on pp.product_id = p.parent_product_id where s.code = 'c003'")).rows[0];
  assert.deepEqual(await par(), { d: 'grp', by: 'load' });
  publishMaterial(P3, S, { semantics: null, complete: '2026-09-27 00:00:00' });   // 古い送り手 (版なし)
  assert.equal((await nightly(db, 'load_mc_p5')).ok, true);
  assert.deepEqual(await par(), { d: 'grp', by: 'load' });
  assert.deepEqual((await parentPayload('load_mc_p5')).held.find((x) => x[0] === 'c003').slice(0, 2), ['c003', 'rep_unknown']);
  assert.equal((await compareIn(db)).verdict, 'pass');
  publishMaterial(P3, S, { semantics: { rep: 'src1' }, complete: null });           // 版はあるが完了した取得でない
  assert.equal((await nightly(db, 'load_mc_p6')).ok, true);
  assert.deepEqual(await par(), { d: 'grp', by: 'load' });
  assert.deepEqual((await parentPayload('load_mc_p6')).held.find((x) => x[0] === 'c003').slice(0, 2), ['c003', 'material_untrusted']);
  publishMaterial(P3, S, { semantics: { rep: 'src1' }, complete: '2026-09-27 00:00:00' });
  assert.equal((await nightly(db, 'load_mc_p7')).ok, true);
  assert.deepEqual(await par(), { d: null, by: null });
  const r = await compareIn(db);
  assert.equal(r.verdict, 'pass', JSON.stringify(r.items, null, 1) + r.blocked_reason);
});

await pg.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
