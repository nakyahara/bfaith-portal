/**
 * test-master-sku-kind.mjs — 区分 (skus.sku_kind。単品 / セット / 例外) の持ち主を 'company' にしたときの夜間ロード (Company DB構想 10 §5.2・「広げる道」の準備)
 *
 * 固定する契約:
 *   1 持ち主が load (今) = 区分は NE (材料) に合わせる・report / 判断の記録に区分の食い違いの欄を足さない (今までと同じ形)
 *   2 company: ポータルで単品として登録した SKU を NE がセットとして持つ = 区分は単品のまま・product_id もそのまま・NE の構成を入れない (kind_held で飛ばす)・
 *     食い違いを report.conflicts (sku_kind_held)・判断の記録 (decisions.skus.kind_held) に残す・構成の観測は対象から外す (完全な回のまま = 構成の依頼を止めない)
 *   3 company: 前の区分の product_id が残ったセット (構成つき) を NE が単品とする = 区分はセットのまま・単品の商品を作らない・名札の親子に入れない (held = kind_held)・
 *     商品 (product) に属性を付けない・構成はそのまま (NE に構成が無い = 消さない)
 *   4 company: 新しく見つかった SKU = 材料 (NE) の区分で作る (最初の値)
 *   5 company で区分が同じ = 持ち主が load のときと同じ結果 (表の中身・帳尻)
 *   6 照合 ① の記録の形: company のロードは kind_held が要る・load のロードには無い・代表の保持の理由 kind_held を知っている
 *   7 company: 初めからセット (商品なし) を NE が単品とする = 商品を作らない / 正規化 (広げる道 v5): 名残の product_id があるセット・構成のある単品 = load でも company でも外す・消す /
 *     どのロードの後も「単品 ⇔ product_id あり」「セットでない親に構成なし」の不整合が 0 (load の補助で毎回確かめる)・整合している行は書かない
 *   8 configured (config/master-ownership.mjs) の skus.sku_kind = company を配っても、ロードは DB の active (本番 10/5 の 13 キー・区分は load) に従う = 区分は NE に合わせる・
 *     widen の後 (active = configured) は区分を社内のまま (kind_held)。照合 ① の記録の形もロードが記録した持ち主で合う
 * 使い方: node apps/company-db/test-master-sku-kind.mjs (PGlite)
 *         TEST_PG_URL=postgres://...@localhost:port/postgres node apps/company-db/test-master-sku-kind.mjs (本物の PostgreSQL。試験ごとに DB を作って消す。
 *         C:/tmp/pg-embed/run-conc.mjs で使い捨ての PostgreSQL を起動して流せる)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter, openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';
import { runInitialLoad } from './load/engine.mjs';
import { decisionsProblem, parentDecisionsProblem } from './master-compare/compare-load.mjs';
import { OWNED_COLUMNS, MASTER_OWNERSHIP, companyOwned } from '../../config/master-ownership.mjs';
import { seedActiveEpoch } from '../../scripts/fixtures/master-epoch.mjs';

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const H = (c) => c.repeat(64);
const ALL_LOAD = Object.freeze(Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load'])));
const KIND_C = Object.freeze({ ...ALL_LOAD, 'skus.sku_kind': 'company' });

let genN = 0;
/** 材料の証跡 (構成の観測が書ける = 完全な NE の取得・新しい時刻)。回ごとに別の世代 */
function material() {
  const at = new Date(Date.now() - 3600000).toISOString().replace('T', ' ').slice(0, 19);
  const n = String(++genN).padStart(3, '0');
  const gen = (e) => ({ generation_id: `mat_20261006T000${n}000Z_aaaaaaaa_${e === 'products' ? 'aaaaaa' : 'bbbbbb'}`, content_hash: H('a'), row_count: 1, source_complete_at: at, created_at: at });
  const part = (e) => ({ status: 'matched', content_hash: H(e === 'products' ? 'b' : 'c'), row_count: 1, generation: gen(e) });
  return { products: { ...part('products'), repSemantics: 'src1' }, set_components: part('set_components') };
}
const S = (code, kind, extra = {}) => ({ code, name: `名 ${code}`, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: kind === 'single' ? 3 : null,
  representativeCode: extra.rep ?? null, representativeState: extra.rep ? 'value' : 'empty', cost: null, ...extra });
/** skus = [[code, kind, extra?]] / comps = [[parent, child, qty]] / groups = [[rep, [children]]] */
function planOf(skus, comps = [], groups = [], extra = {}) {
  return {
    skus: skus.map(([code, kind, x]) => S(code, kind, x)),
    variationGroups: groups.map(([code, childCodes]) => ({ code, name: `まとまり ${code}`, childCodes, status: 'active' })),
    setComponents: comps.map(([parentCode, childCode, qty]) => ({ parentCode, childCode, qty, source: 'ne' })),
    listings: [], observations: [], physicals: [], compliance: [], suppliers: [], supplierSkus: [], primarySuppliers: [], workers: [],
    reorder: { available: false, runId: null, reason: '試験' }, sources: {}, material: material(), ...extra,
  };
}
const PG_URL = process.env.TEST_PG_URL || '';
if (PG_URL && !['localhost', '127.0.0.1', '::1', '[::1]'].includes(new URL(PG_URL).hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
/** 試験ごとの空の Company DB (TEST_PG_URL があれば本物の PostgreSQL に DB を作る・閉じると消す) */
async function freshDb() {
  if (PG_URL) {
    const name = `cdb_skukind_${crypto.randomBytes(4).toString('hex')}`;
    const admin = await openPgClient(PG_URL);
    await admin.query(`create database ${name}`);
    const u = new URL(PG_URL); u.pathname = `/${name}`;
    const client = await openPgClient(u.toString());
    const db = pgAdapter(client);
    await applyMigrations(db, { log: quiet });
    const pg = { close: async () => { await client.end(); try { await admin.query(`drop database ${name}`); } finally { await admin.end(); } } };
    return { pg, db, q: async (sql, p) => (await db.query(sql, p)).rows };
  }
  const pg = new PGlite(); const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  return { pg, db, q: async (sql, p) => (await db.query(sql, p)).rows };
}
const balanced = (r) => { for (const [k, v] of Object.entries(r.sections)) assert.equal(v.expected, v.applied + v.same + v.skipped.length, `${k} が釣り合わない`); };
let runN = 0;
/** 区分の最終形の不整合 (正規化 = 広げる道 v5): 単品 ⇔ product_id あり・セットでない親に構成なし。どのロードの後も 0 */
async function invariants(db) {
  const one = async (sql) => Number((await db.query(sql)).rows[0].n);
  return {
    product: await one("select count(*)::int as n from core.skus where (sku_kind = 'single') <> (product_id is not null)"),
    comps: await one("select count(*)::int as n from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id where p.sku_kind <> 'set'"),
  };
}
async function load(db, plan, ownership) {
  const r = await runInitialLoad(db, plan, { log: quiet, runId: `kind_${++runN}`, ownership, now: new Date('2026-10-06T01:00:00Z') });
  assert.equal(r.ok, true, r.error);
  balanced(r);
  assert.deepEqual(await invariants(db), { product: 0, comps: 0 }, `区分の最終形の不整合が残った (${r.run_id})`);
  return r;
}
/** 前のコードが残した形を作る (セットなのに前の単品の product_id・構成つきの単品)。DB の CHECK は通る形 */
async function legacy(q, { setWithProduct = null, singleWithComps = null } = {}) {
  if (setWithProduct) {
    const pid = (await q("insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, $1, $2, 'active', 'system', 'old') returning product_id", [setWithProduct, `名残 ${setWithProduct}`]))[0].product_id;
    await q('update core.skus set product_id = $2 where code = $1', [setWithProduct, pid]);
    return pid;
  }
  if (singleWithComps) await q(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source, created_by_type, created_by_id)
    select 1, p.sku_id, c.sku_id, 1, 'ne', 'system', 'old' from core.skus p, core.skus c where p.code = $1 and c.code = 'a2'`, [singleWithComps]);
  return null;
}
const skuRow = async (q, code) => (await q('select sku_kind, product_id::text as product_id from core.skus where code = $1', [code]))[0];
const compsOf = async (q, code) => (await q(`select ch.code, c.qty from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id join core.skus ch on ch.sku_id = c.child_sku_id
  where p.code = $1 order by ch.code`, [code])).map((r) => [r.code, Number(r.qty)]);
const decisionsOf = async (q, runId) => Object.fromEntries((await q('select section, payload from ops.load_decisions where ingest_run_id = $1', [runId])).map((r) => [r.section, r.payload]));
const BASE = [['a1', 'single'], ['a2', 'single'], ['b1', 'single'], ['set1', 'set']];
const BASE_COMPS = [['set1', 'a1', 2]];

await ta('[1] 持ち主が load (今): 区分は NE に合わせる・report と判断の記録に区分の食い違いの欄は無い (今までと同じ形)', async () => {
  const { pg, db, q } = await freshDb();
  try {
    await load(db, planOf([...BASE, ['k1', 'single']], BASE_COMPS), ALL_LOAD);
    const r = await load(db, planOf([...BASE, ['k1', 'set']], [...BASE_COMPS, ['k1', 'a1', 1]]), ALL_LOAD);
    assert.equal((await skuRow(q, 'k1')).sku_kind, 'set');   // NE に合わせた
    assert.deepEqual(await compsOf(q, 'k1'), [['a1', 1]]);
    assert.ok(!r.conflicts.some((c) => c.kind === 'sku_kind_held'));
    assert.ok(!r.sections.skus.notes.some((n) => /区分: Company DB/.test(n)));
    assert.deepEqual([r.normalization.product_unlinked.rows.map((x) => x.slice(0, 2)), (await skuRow(q, 'k1')).product_id], [[['k1', 'set']], null]);   // 単品 → セット = product_id を外す (正規化)
    const D = await decisionsOf(q, r.run_id);
    assert.ok(!Object.hasOwn(D.skus, 'kind_held'), JSON.stringify(D.skus));
    assert.equal(decisionsProblem(D, { ownership: ALL_LOAD, has0027: true }), null);
  } finally { await pg.close(); }
});

await ta('[2] company: ポータルで単品として登録した SKU を NE がセットとして持つ = 単品のまま・product_id そのまま・NE の構成を入れない・食い違いを記録・構成の観測は完全な回のまま', async () => {
  const { pg, db, q } = await freshDb();
  try {
    await load(db, planOf([...BASE, ['p1', 'single']], BASE_COMPS), ALL_LOAD);   // ポータルの単品の代わり (C に単品 p1 と商品)
    const before = await skuRow(q, 'p1');
    const prodN = (await q('select count(*)::int as n from core.products'))[0].n;
    const r = await load(db, planOf([...BASE, ['p1', 'set']], [...BASE_COMPS, ['p1', 'a1', 3], ['p1', 'a2', 1]]), KIND_C);
    assert.deepEqual(await skuRow(q, 'p1'), before);   // 区分も product_id も社内のまま
    assert.deepEqual(await compsOf(q, 'p1'), []);      // NE のセットの構成を社内の単品に入れない
    assert.equal((await q('select count(*)::int as n from core.products'))[0].n, prodN);
    assert.deepEqual(r.conflicts.filter((c) => c.kind === 'sku_kind_held'), [{ kind: 'sku_kind_held', code: 'p1', ne_kind: 'set', cdb_kind: 'single' }]);
    assert.ok(r.sections.skus.notes.some((n) => /区分: Company DB が正 \(NE と区分が違う 1 件/.test(n)), JSON.stringify(r.sections.skus.notes));
    assert.deepEqual(r.sections.set_components.skipped.filter((x) => x.reason_code === 'kind_held').map((x) => [x.parent, x.child]), [['p1', 'a1'], ['p1', 'a2']]);
    assert.deepEqual(await compsOf(q, 'set1'), [['a1', 2]]);   // ほかのセットは今までどおり
    const D = await decisionsOf(q, r.run_id);
    assert.deepEqual(D.skus.kind_held, [['p1', 'set', 'single']]);
    assert.deepEqual(D.skus.sku_kind, { format: 'sku-kind-v1', held: ['p1'], unverifiable: [] });   // 形 (広げる道 v11 §8-4)
    assert.equal(decisionsProblem(D, { ownership: KIND_C, has0027: true }), null);
    assert.deepEqual(D.set_components.skipped.filter((x) => x[2] === 'kind_held').map((x) => [x[0], x[1], x[3].held]), [['p1', 'a1', null], ['p1', 'a2', null]]);
    // 構成の観測: p1 は社内のセットでない = 対象から外す (kind_held)・ほかのセットは完全な回 (構成の依頼を止めない)
    assert.deepEqual([r.set_observations.state, r.set_observations.complete, r.set_observations.requested, r.set_observations.kind_held, r.set_observations.excluded],
      ['written', true, 1, 1, {}], JSON.stringify(r.set_observations));
    // 2 回目も同じ (冪等)
    const r2 = await load(db, planOf([...BASE, ['p1', 'set']], [...BASE_COMPS, ['p1', 'a1', 3], ['p1', 'a2', 1]]), KIND_C);
    assert.deepEqual([await skuRow(q, 'p1'), await compsOf(q, 'p1'), r2.sections.skus.applied], [before, [], 0]);
  } finally { await pg.close(); }
});

await ta('[3] company: 前の区分の product_id が残ったセット (構成つき) を NE が単品とする = セットのまま・product_id は外す (正規化)・単品の商品を作らない・親子に入れない・商品に属性を付けない・構成は消さない', async () => {
  const { pg, db, q } = await freshDb();
  try {
    // 持ち主が load の間に NE で単品 → セットにした。前のコードは product_id を coalesce で残した = その形 (Codex の指摘の形) を作る
    await load(db, planOf([...BASE, ['k2', 'single', { rep: 'vg' }], ['k3', 'single', { rep: 'vg' }]], BASE_COMPS, [['vg', ['k2', 'k3']]]), ALL_LOAD);
    await load(db, planOf([...BASE, ['k2', 'set'], ['k3', 'single', { rep: 'vg' }]], [...BASE_COMPS, ['k2', 'a2', 2]], [['vg', ['k3']]]), ALL_LOAD);
    assert.equal((await skuRow(q, 'k2')).product_id, null);   // 今のコードは単品 → セットで product_id を外す
    const oldPid = await legacy(q, { setWithProduct: 'k2' });
    const before = await skuRow(q, 'k2');
    assert.deepEqual([before.sku_kind, before.product_id, await compsOf(q, 'k2')], ['set', String(oldPid), [['a2', 2]]]);
    const prodN = (await q('select count(*)::int as n from core.products'))[0].n;
    // NE が k2 をまた単品にした (代表 vg・物理属性・観測つき)。社内はセットのまま
    const plan = planOf([...BASE, ['k2', 'single', { rep: 'vg' }], ['k3', 'single', { rep: 'vg' }]], BASE_COMPS, [['vg', ['k2', 'k3']]], {
      physicals: [{ skuCode: 'k2', scope: 'package', weightG: 100, source: 'fba_sku_attrs', sourceRef: 'k2', isMeasured: false, observedAt: null }],
      observations: [{ skuCode: 'k2', attribute: 'brand', scope: 'item', valueText: 'B', source: 'product_hub', sourceRef: 'x', observedAt: null }],
    });
    const r = await load(db, plan, KIND_C);
    assert.deepEqual(await skuRow(q, 'k2'), { sku_kind: 'set', product_id: null });   // セットのまま・名残の product_id は外す (商品の行は消さない)
    assert.equal((await q('select count(*)::int as n from core.products where product_id = $1', [oldPid]))[0].n, 1);
    assert.deepEqual(await compsOf(q, 'k2'), [['a2', 2]]);   // NE に構成が無い = 社内の構成は消さない
    assert.equal((await q('select count(*)::int as n from core.products'))[0].n, prodN);   // 単品の商品を作らない
    assert.deepEqual(r.sections.products.skipped.map((x) => [x.code, x.reason_code]), [['k2', 'kind_held']]);
    assert.deepEqual(r.sections.physicals.skipped.map((x) => x.code), ['k2']);   // セットの (前の区分の) 商品に物理属性を付けない
    assert.equal((await q("select count(*)::int as n from core.product_attribute_observations where entity_type = 'product'"))[0].n, 0);
    assert.deepEqual(r.conflicts.filter((c) => c.kind === 'sku_kind_held').map((c) => [c.code, c.ne_kind, c.cdb_kind]), [['k2', 'single', 'set']]);
    // 代表 (親子): k2 は子にしない = 判断の記録の held (kind_held)。照合 ① の網羅 (材料の単品 = targets ∪ held) は通る
    const D = await decisionsOf(q, r.run_id);
    const V = (await q("select payload from ops.load_decisions where ingest_run_id = $1 and section = 'skus'", [r.run_id]))[0].payload;
    assert.deepEqual(V.kind_held, [['k2', 'single', 'set']]);
    const PV = (await q("select payload from ops.load_decisions where ingest_run_id = $1 and section = 'variation_parents'", [r.run_id]))[0].payload;
    {
      assert.deepEqual(PV.held.filter((x) => x[1] === 'kind_held'), [['k2', 'kind_held', null, null, null]]);
      assert.equal(parentDecisionsProblem(PV, { ownership: KIND_C, material: { status: 'matched', source_complete_at: PV.trusted.source_complete_at } }), null);
      const singles = plan.skus.filter((s) => s.kind === 'single').map((s) => s.code).sort();
      assert.deepEqual([...PV.targets, ...PV.held].map((x) => x[0]).sort(), singles);
    }
    assert.equal(decisionsProblem(D, { ownership: KIND_C, has0027: true }), null);
  } finally { await pg.close(); }
});

await ta('[4] company: 新しく見つかった SKU は材料 (NE) の区分で作る (最初の値)・単品は商品つき', async () => {
  const { pg, db, q } = await freshDb();
  try {
    await load(db, planOf(BASE, BASE_COMPS), ALL_LOAD);
    const r = await load(db, planOf([...BASE, ['n1', 'single'], ['n2', 'set']], [...BASE_COMPS, ['n2', 'n1', 2]]), KIND_C);
    const n1 = await skuRow(q, 'n1'), n2 = await skuRow(q, 'n2');
    assert.deepEqual([n1.sku_kind, n1.product_id != null, n2.sku_kind, n2.product_id], ['single', true, 'set', null]);
    assert.deepEqual(await compsOf(q, 'n2'), [['n1', 2]]);
    assert.ok(!r.conflicts.some((c) => c.kind === 'sku_kind_held'));
  } finally { await pg.close(); }
});

await ta('[5] company で区分が同じ = 持ち主が load のときと同じ結果 (表の中身・帳尻)', async () => {
  const dump = async (q) => {
    const out = {};
    for (const t of ['core.products', 'core.skus', 'core.sku_components', 'core.sku_costs', 'core.product_physicals']) {
      out[t] = (await q(`select * from ${t}`)).map((r) => JSON.stringify(Object.fromEntries(Object.entries(r).filter(([k]) => !/(_at|updated|created|recorded|valid_from|version|_by_id)$/.test(k))))).sort();
    }
    return out;
  };
  const plans = [planOf([...BASE, ['k1', 'single', { rep: 'vg' }]], BASE_COMPS, [['vg', ['k1', 'a2']]]), planOf([...BASE, ['k1', 'single', { rep: 'vg' }], ['n1', 'set']], [...BASE_COMPS, ['n1', 'a1', 1]], [['vg', ['k1', 'a2']]])];
  const runAll = async (own) => {
    const { pg, db, q } = await freshDb();
    try {
      const sec = [];
      await load(db, plans[0], ALL_LOAD);
      const r = await load(db, plans[1], own);
      for (const [k, v] of Object.entries(r.sections)) sec.push([k, v.expected, v.applied, v.same, v.skipped.length]);
      return { d: await dump(q), sec, conflicts: r.conflicts };
    } finally { await pg.close(); }
  };
  const a = await runAll(ALL_LOAD), b = await runAll(KIND_C);
  assert.deepEqual(b.d, a.d);
  assert.deepEqual(b.sec, a.sec);
  assert.deepEqual(b.conflicts, a.conflicts);
});

await ta('[7] company: 初めからセット (商品なし) の SKU を NE が単品とする = 単品の商品を作らない / 名残の product_id があるセット・構成のある単品 = load でも company でも正規化 (外す・消す)・商品の属性を付けない / 整合している行は 2 回目に何も書かない', async () => {
  const { pg, db, q } = await freshDb();
  try {
    await load(db, planOf([...BASE, ['k4', 'set']], [...BASE_COMPS, ['k4', 'a1', 1]]), ALL_LOAD);
    const prodN = (await q('select count(*)::int as n from core.products'))[0].n;
    const r = await load(db, planOf([...BASE, ['k4', 'single']], BASE_COMPS), KIND_C);
    assert.deepEqual([await skuRow(q, 'k4'), (await q('select count(*)::int as n from core.products'))[0].n], [{ sku_kind: 'set', product_id: null }, prodN]);
    assert.deepEqual(r.sections.products.skipped.map((x) => [x.code, x.reason_code]), [['k4', 'kind_held']]);
    // 前のコードが残した形: 名残の product_id があるセット k5 (NE もセット)・構成のある単品 b1 (NE も単品)
    const plan5 = (extra = {}) => planOf([...BASE, ['k5', 'set']], [...BASE_COMPS, ['k5', 'a1', 1]], [], extra);
    await load(db, plan5(), ALL_LOAD);
    for (const own of [ALL_LOAD, KIND_C]) {
      await legacy(q, { setWithProduct: 'k5' }); await legacy(q, { singleWithComps: 'b1' });
      const phys = [{ skuCode: 'k5', scope: 'package', weightG: 120, source: 'fba_sku_attrs', sourceRef: 'k5', isMeasured: false, observedAt: null }];
      const rr = await load(db, plan5({ physicals: phys }), own);   // load も invariants (0) を確かめる
      assert.deepEqual([(await skuRow(q, 'k5')).product_id, await compsOf(q, 'b1'), await compsOf(q, 'k5')], [null, [], [['a1', 1]]]);
      assert.deepEqual(rr.sections.physicals.skipped.map((x) => x.code), ['k5']);   // 最終の区分 (セット) の SKU は商品に付けない
      assert.deepEqual(rr.conflicts.filter((c) => c.kind === 'components_on_non_set_removed').map((c) => c.count), [1]);
      assert.ok(rr.sections.set_components.notes.some((n) => /セットでない親の構成 1 行を消した/.test(n)));
      // 証拠 = 消した構成・外した product_id の全件・件数・hash (report と判断の記録の両方)
      const N = rr.normalization;
      assert.deepEqual([N.components_removed.count, N.components_removed.rows, N.product_unlinked.count, N.product_unlinked.rows.map((x) => x.slice(0, 2))],
        [1, [['b1', 'a2', 1, 'ne', 'single']], 1, [['k5', 'set']]]);
      assert.match(N.components_removed.sha256, /^[0-9a-f]{64}$/);
      const D = await decisionsOf(q, rr.run_id);
      assert.deepEqual([D.set_components.normalized_removed, D.skus.normalized_unlinked], [N.components_removed, N.product_unlinked]);
      assert.equal(decisionsProblem(D, { ownership: own, has0027: true }), null);
    }
    // 材料の形の誤り (単品 b1 に構成の行) = 最終の区分がセットでない親には構成を入れない (parent_not_set で飛ばす)
    const rb = await load(db, planOf([...BASE, ['k5', 'set']], [...BASE_COMPS, ['k5', 'a1', 1], ['b1', 'a2', 1]]), ALL_LOAD);
    assert.deepEqual([await compsOf(q, 'b1'), rb.sections.set_components.skipped.filter((x) => x.reason_code === 'parent_not_set').map((x) => [x.parent, x.child])], [[], [['b1', 'a2']]]);
    // 整合している = もう一度流しても何も書かない (正規化の書き込み・メモ・証拠の欄も出ない)
    const r3 = await load(db, plan5(), ALL_LOAD);
    assert.deepEqual([r3.sections.skus.applied, r3.conflicts.filter((c) => /components_on_non_set/.test(c.kind)).length, r3.sections.set_components.notes.some((n) => /正規化/.test(n)), Object.hasOwn(r3, 'normalization')], [0, 0, false, false]);
    const D3 = await decisionsOf(q, r3.run_id);
    assert.deepEqual([Object.hasOwn(D3.set_components, 'normalized_removed'), Object.hasOwn(D3.skus, 'normalized_unlinked')], [false, false]);
  } finally { await pg.close(); }
});

await ta('[8] configured の区分 = company を配っても、ロードは DB の active (10/5 の 13 キー・区分 load) に従う = 区分は NE に合わせる / widen の後 (active = configured) は社内の区分のまま', async () => {
  // 広げる道の手順の 1 (10/7): configured の skus.sku_kind = company。DB の active (本番 = 10/5 の 13 キー) には無い = prepare --widen の足すキーは区分だけ
  const PROD_ACTIVE_20261005 = { ...ALL_LOAD, ...Object.fromEntries(companyOwned(MASTER_OWNERSHIP).filter((k) => k !== 'skus.sku_kind').map((k) => [k, 'company'])) };
  assert.equal(MASTER_OWNERSHIP['skus.sku_kind'], 'company');
  assert.deepEqual(companyOwned(MASTER_OWNERSHIP).filter((k) => PROD_ACTIVE_20261005[k] !== 'company'), ['skus.sku_kind']);
  const { pg, db, q } = await freshDb();
  try {
    await load(db, planOf([...BASE, ['k1', 'single'], ['k8', 'single']], BASE_COMPS), ALL_LOAD);
    assert.equal(await seedActiveEpoch(db, PROD_ACTIVE_20261005), true);
    const recorded = async (runId) => (await q("select ownership from ops.load_materials where ingest_run_id = $1 and entity = 'products'", [runId]))[0]?.ownership ?? null;
    // (1) 配ってから widen まで: 持ち主を渡さない (= 本番の夜間ロードと同じく epoch を読む) = 区分は NE に合わせる・kind_held は無い
    const r = await load(db, planOf([...BASE, ['k1', 'set'], ['k8', 'single']], [...BASE_COMPS, ['k1', 'a1', 1]]), undefined);
    assert.equal(r.ownership_epoch.epoch, 'active');
    assert.ok(!r.company_owned.includes('skus.sku_kind'), JSON.stringify(r.company_owned));
    assert.equal((await skuRow(q, 'k1')).sku_kind, 'set');
    assert.deepEqual(await compsOf(q, 'k1'), [['a1', 1]]);
    assert.ok(!r.conflicts.some((c) => c.kind === 'sku_kind_held'));
    const own1 = await recorded(r.run_id);
    assert.equal(own1['skus.sku_kind'], 'load');   // 照合 ①・②・写しが使うロードの記録の持ち主 = active (configured ではない)
    const D = await decisionsOf(q, r.run_id);
    assert.ok(!Object.hasOwn(D.skus, 'kind_held'));
    assert.equal(decisionsProblem(D, { ownership: own1, has0027: true }), null);
    // (2) widen の後 (active = configured): 区分は社内のまま (NE の単品 → セットを入れない)・食い違いを記録
    assert.equal(await seedActiveEpoch(db, MASTER_OWNERSHIP), true);
    const r2 = await load(db, planOf([...BASE, ['k1', 'set'], ['k8', 'set']], [...BASE_COMPS, ['k1', 'a1', 1], ['k8', 'a2', 2]]), undefined);
    assert.equal(r2.ownership_epoch.epoch, 'active');
    assert.ok(r2.company_owned.includes('skus.sku_kind'));
    assert.deepEqual([(await skuRow(q, 'k8')).sku_kind, await compsOf(q, 'k8')], ['single', []]);
    assert.deepEqual(r2.conflicts.filter((c) => c.kind === 'sku_kind_held'), [{ kind: 'sku_kind_held', code: 'k8', ne_kind: 'set', cdb_kind: 'single' }]);
    const own2 = await recorded(r2.run_id);
    assert.equal(own2['skus.sku_kind'], 'company');
    assert.equal(decisionsProblem(await decisionsOf(q, r2.run_id), { ownership: own2, has0027: true }), null);
  } finally { await pg.close(); }
});

await ta('[6] 照合 ① の記録の形: company のロードは kind_held が要る (形も)・load のロードに kind_held があれば形の誤り', async () => {
  const D = (skus) => ({ skus, sku_costs: { owned: true, skipped: [] }, set_components: { owned: true, prune_parents: [], rows: [], manual_kept_on_prune: [], skipped: [] }, primary_suppliers: { applied: false, reason_code: 'no_0027' } });
  const base = { accepted: 1, skipped: [] };
  assert.equal(decisionsProblem(D(base), { ownership: ALL_LOAD, has0027: false }), null);
  assert.equal(decisionsProblem(D({ ...base, kind_held: [] }), { ownership: ALL_LOAD, has0027: false }), 'skus_kind_held');
  assert.equal(decisionsProblem(D(base), { ownership: KIND_C, has0027: false }), 'skus_kind_held');
  assert.equal(decisionsProblem(D({ ...base, kind_held: [] }), { ownership: KIND_C, has0027: false }), null);
  assert.equal(decisionsProblem(D({ ...base, kind_held: [['x', 'set', 'single']] }), { ownership: KIND_C, has0027: false }), null);
  assert.equal(decisionsProblem(D({ ...base, kind_held: [['x', 'set', 'set']] }), { ownership: KIND_C, has0027: false }), 'skus_kind_held');
  assert.equal(decisionsProblem(D({ ...base, kind_held: [['x', 'box', 'single']] }), { ownership: KIND_C, has0027: false }), 'skus_kind_held');
  // 区分の記録 (sku_kind) の形 (広げる道 v11 §8-4): 版・鍵 3 つ・held は文字の配列・unverifiable の要素 {reason, raw_code, code_norm}・load なら held は空 (無い = この版の前のロード = 見ない)
  const SK = (o = {}) => ({ format: 'sku-kind-v1', held: [], unverifiable: [{ reason: 'unknown_kind', raw_code: 'w', code_norm: 'w' }, { reason: 'empty_code', raw_code: '', code_norm: null }], ...o });
  assert.equal(decisionsProblem(D({ ...base, sku_kind: SK() }), { ownership: ALL_LOAD, has0027: false }), null);
  assert.equal(decisionsProblem(D({ ...base, kind_held: [['x', 'set', 'single']], sku_kind: SK({ held: ['x'] }) }), { ownership: KIND_C, has0027: false }), null);
  for (const bad of [SK({ format: 'v0' }), SK({ held: null }), SK({ held: ['x'] }), SK({ held: [{ code: 'x' }] }), SK({ extra: 1 }),
    SK({ unverifiable: [{ reason: 'unknown_kind', code: 'w', code_norm: 'w' }] }), SK({ unverifiable: [{ reason: 'odd', raw_code: 'w', code_norm: 'w' }] }),
    SK({ unverifiable: [{ reason: 'empty_code', raw_code: '', code_norm: '' }] }), SK({ unverifiable: [{ reason: 'unknown_kind', raw_code: 'w', code_norm: null }] })]) {
    assert.equal(decisionsProblem(D({ ...base, sku_kind: bad }), { ownership: ALL_LOAD, has0027: false }), 'skus_sku_kind', JSON.stringify(bad));
  }
});

console.log(`\n${passed} 件 PASS${PG_URL ? ' (本物の PostgreSQL)' : ''}`);
