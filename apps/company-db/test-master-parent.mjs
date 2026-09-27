/**
 * test-master-parent.mjs — 代表関係 (親子) の帰属・DB の守り・夜間ロードの付け外し (Company DB構想 10 §6.1.1 D3 の契約 v3。0036)
 *
 * 固定する契約:
 *   1 backfill = 変更の記録で「夜間ロードが今の親を付け、その後に誰も変えていない」と証明できる行だけ load。記録の無い行・後で他の口が変えた行は null
 *   2 DB の守り: 親子を変える取引は約束の印 (core.parent_protocol = '1') と親子の鍵 (排他・bigint の形・この接続) の両方が要る。
 *     共有の鍵・整数 2 つの形・別の鍵・印だけ・鍵だけは拒む。親子を変えない UPDATE・親の無い INSERT は見ない
 *   3 夜間ロードの表 (付ける・同じ・付け替え・外す・保持 = manual / 帰属不明 / 不明 / 代表が単品でない) と判断の記録 (targets / held を 1 回ずつ)
 *   4 外せる材料 (matched・完了した取得・意味の版 src1) でなければ外さない (material_untrusted)
 *   5 一度に外しすぎの守り (max(20, 2%) を超えたら 1 件も外さない)
 *   6 外す辺を明示の null 辺として循環を検算する (同時に外すと循環しない / manual で旧い辺が残ると循環になる)
 *   7 持ち主 company = 名札も親子も触らない / 0036 の前の DB = 今までどおり (付けるだけ・記録なし)
 *   8 送る形の代表の状態 (readMasterMaterial) と、材料の意味の版 (representativeStateOf・受け手の validMaterialSemantics)
 * 使い方: node apps/company-db/test-master-parent.mjs
 */
import assert from 'node:assert/strict';
import { PGlite } from '@electric-sql/pglite';
import Database from 'better-sqlite3';
import { applyMigrations, pgliteAdapter } from '../../scripts/company-db/migrate.mjs';
import { runInitialLoad, UNLINK_GUARD_MIN, unlinkGuardLimit } from './load/engine.mjs';
import { summaryLine } from './master-compare/run.mjs';
import { validTimestampText } from './master-compare/compare-load.mjs';
import { representativeStateOf } from './load/sources.mjs';
import { MASTER_OWNERSHIP } from '../../config/master-ownership.mjs';
import { readMasterMaterial, MATERIAL_REP_SEMANTICS } from '../warehouse/master-material.js';
import { validMaterialSemantics } from '../warehouse/material-lineage.js';

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const H = (c) => c.repeat(64);

/** 最小の plan。skus = [[code, rep, state, kind?]]、groups = [[rep, [children]]] */
function planOf(skus, groups, material = trusted()) {
  return {
    skus: skus.map(([code, rep, state, kind = 'single']) => ({ code, name: `商品 ${code}`, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null,
      representativeCode: rep ?? null, representativeState: state, cost: null })),
    variationGroups: groups.map(([code, childCodes]) => ({ code, name: `まとまり ${code}`, childCodes, status: 'active' })),
    setComponents: [], listings: [], observations: [], physicals: [], compliance: [], suppliers: [], supplierSkus: [], workers: [], primarySuppliers: [],
    reorder: { available: false, runId: null, reason: '試験' }, sources: {},
    ...(material ? { material } : {}),
  };
}
function trusted({ status = 'matched', complete = '2026-09-27 00:00:00', semantics = 'src1' } = {}) {
  const gen = (e) => ({ generation_id: `mat_20260927T000000000Z_aaaaaaaa_${e === 'products' ? 'aaaaaa' : 'bbbbbb'}`, content_hash: H('a'), row_count: 1, source_complete_at: complete, created_at: '2026-09-27 00:00:00', received_at: '2026-09-27 00:00:01' });
  const part = (e) => ({ status, content_hash: H(e === 'products' ? 'b' : 'c'), row_count: 1, generation: status === 'no_generation' ? null : gen(e) });
  return { products: { ...part('products'), repSemantics: status === 'matched' ? semantics : null }, set_components: part('set_components') };
}

async function freshDb(to = null) {
  const pg = new PGlite();
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet, ...(to ? { to } : {}) });
  return { pg, db, q: async (sql, p) => (await db.query(sql, p)).rows };
}
/** 親子を直接書き換える (人の操作)。約束の印と親子の鍵 (取引の鍵) を付ける */
async function asParentWriter(db, fn) {
  await db.exec('begin');
  try { await db.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())"); await fn(); await db.exec('commit'); }
  catch (e) { await db.exec('rollback'); throw e; }
}
const stateOf = async (q, code) => {
  const r = (await q(`select pp.display_code as parent, p.parent_set_by as by from core.skus s join core.products p on p.product_id = s.product_id
    left join core.products pp on pp.product_id = p.parent_product_id where s.code = $1`, [code]))[0];
  return [r.parent ?? null, r.by ?? null];
};
const decisionsOf = async (q, runId) => (await q("select payload from ops.load_decisions where ingest_run_id = $1 and section = 'variation_parents'", [runId]))[0]?.payload ?? null;
const load = (db, plan, runId, opts = {}) => runInitialLoad(db, plan, { log: quiet, runId, host: 'test', ...opts });

console.log('0036 の backfill と守り');
await ta('[1] backfill = 記録で証明できる行だけ load (記録の無い行・後で他の口が変えた行・最後の変更の値が今と違う行は null)', async () => {
  const { pg, db, q } = await freshDb('0035');
  try {
    const mk = async (code) => Number((await q(`insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, $1, $1, 'active', 'system', 'load_x') returning product_id`, [code]))[0].product_id);
    const g1 = await mk('g1'); const g2 = await mk('g2');
    const ids = {}; for (const c of ['a', 'b', 'c', 'd', 'e']) ids[c] = await mk(c);
    const setBy = async (source, id, pp) => {
      await db.exec('begin');
      await q("select set_config('core.source_system', $1, true)", [source]);
      await q('update core.products set parent_product_id = $2 where product_id = $1', [id, pp]);
      await db.exec('commit');
    };
    await setBy('company_db_load', ids.a, g1);                         // a: ロードが付けた (記録あり) → load
    await q('alter table core.products disable trigger trg_products_audit');
    await q('update core.products set parent_product_id = $2 where product_id = $1', [ids.b, g1]);   // b: 記録の前 (記録なし) → null
    await q('alter table core.products enable trigger trg_products_audit');
    await setBy('company_db_load', ids.c, g1); await setBy('portal', ids.c, g2);                   // c: ロードの後に人が変えた → null
    await setBy('portal', ids.d, g1); await setBy('company_db_load', ids.d, g2);                   // d: 人の後にロードが今の親を付けた (最後の変更 = ロード) → load
    await setBy('company_db_load', ids.e, g1);
    await q('alter table core.products disable trigger trg_products_audit');
    await q('update core.products set parent_product_id = $2 where product_id = $1', [ids.e, g2]);   // e: 最後の記録の値 (g1) と今 (g2) が違う → null
    await q('alter table core.products enable trigger trg_products_audit');
    await applyMigrations(db, { log: quiet });
    const by = Object.fromEntries((await q("select display_code, parent_set_by from core.products where display_code in ('a','b','c','d','e','g1','g2')")).map((r) => [r.display_code, r.parent_set_by]));
    assert.deepEqual(by, { a: 'load', b: null, c: null, d: 'load', e: null, g1: null, g2: null });
    // backfill の変更も記録に残る (source = migration_0036)
    const ev = await q("select entity_id::int as id, old_value, new_value, source_system from events.master_change_events where attribute = 'parent_set_by' order by entity_id");
    assert.deepEqual(ev.map((e) => [e.id, e.new_value, e.source_system]), [[ids.a, 'load', 'migration_0036'], [ids.d, 'load', 'migration_0036']]);
  } finally { await pg.close(); }
});

await ta('[2] 守り: 印と鍵 (排他・bigint の形・この接続) の両方が要る。共有の鍵・整数 2 つの形・別の鍵・印だけ・鍵だけは拒む。親子を変えない UPDATE・親の無い INSERT は見ない', async () => {
  const { pg, db, q } = await freshDb();
  try {
    const mk = async (code, pp = null) => Number((await q(`insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id, parent_product_id) values (1, $1, $1, 'active', 'system', 't', $2) returning product_id`, [code, pp]))[0].product_id);
    const g = await mk('g'); const c = await mk('c');
    const tryIn = async (setup) => {
      await db.exec('begin');
      try { for (const s of setup) await q(s); await q("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [c, g]); return 'ok'; }
      catch (e) { return /parent_protocol_required/.test(e.message) ? 'rejected' : `error: ${e.message}`; }
      finally { await db.exec('rollback'); }
    };
    const P = "select set_config('core.parent_protocol', '1', true)";
    const K = 'select pg_advisory_xact_lock(core.parent_lock_key())';
    assert.equal(await tryIn([]), 'rejected', '印も鍵も無い');
    assert.equal(await tryIn([P]), 'rejected', '印だけ');
    assert.equal(await tryIn([K]), 'rejected', '鍵だけ');
    assert.equal(await tryIn(["select set_config('core.parent_protocol', '0', true)", K]), 'rejected', '印の値が違う');
    assert.equal(await tryIn([P, 'select pg_advisory_xact_lock_shared(core.parent_lock_key())']), 'rejected', '共有の鍵');
    assert.equal(await tryIn([P, 'select pg_advisory_xact_lock(1, 410342739)']), 'rejected', '整数 2 つの形 (同じ数でも別の鍵)');
    assert.equal(await tryIn([P, 'select pg_advisory_xact_lock(core.parent_lock_key() + 1)']), 'rejected', '別の鍵');
    assert.equal(await tryIn([P, K]), 'ok', '印と鍵');
    assert.equal(await tryIn([P, 'select pg_advisory_lock(core.parent_lock_key())']), 'ok', '接続の鍵 (DB は見分けられない。決まりは取引の鍵)');
    await q('select pg_advisory_unlock_all()');
    // 親子を変えない UPDATE・親の無い INSERT は見ない
    await q("update core.products set name = 'x' where product_id = $1", [c]);
    await q("update core.products set parent_product_id = null where product_id = $1", [c]);   // 変わらない (null → null)
    await mk('n');
    await assert.rejects(q(`insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id, parent_product_id) values (1, 'p', 'p', 'active', 'system', 't', $1)`, [g]), /parent_protocol_required/);
    await assert.rejects(q(`insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id, parent_set_by) values (1, 'p2', 'p2', 'active', 'system', 't', 'manual')`), /parent_protocol_required/);
    // 古いコードの夜間ロード (印も鍵も付けずに親を書く) は取引ごと失敗する
    await assert.rejects(q(`update core.products p set parent_product_id = v.pp from (values ($1::bigint, $2::bigint)) as v(pid, pp) where p.product_id = v.pid and p.parent_product_id is distinct from v.pp`, [c, g]), /parent_protocol_required/);
  } finally { await pg.close(); }
});

console.log('夜間ロードの表');
// 1 回目: c1〜c8 を g1 に付ける (全部 load)。人の操作で状態を作ってから 2 回目の材料で表の各マスを見る
const T = await freshDb();
const SKUS1 = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10'].map((c) => [c, c <= 'c8' && c.length === 2 ? 'g1' : null, c <= 'c8' && c.length === 2 ? 'value' : 'empty']);
await ta('[3] 1 回目: 代表の値から名札に付ける (帰属 load)。判断の記録は採用した単品を targets に 1 回ずつ', async () => {
  const r = await load(T.db, planOf([...SKUS1, ['ex1', null, 'unknown', 'exception']], [['g1', ['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8']]]), 'load_p1');
  assert.equal(r.ok, true, r.error);
  for (const c of ['c1', 'c2', 'c8']) assert.deepEqual(await stateOf(T.q, c), ['g1', 'load']);
  assert.deepEqual(await stateOf(T.q, 'c9'), [null, null]);
  const d = await decisionsOf(T.q, 'load_p1');
  assert.equal(d.owned, true); assert.deepEqual(d.trusted, { matched: true, source_complete_at: '2026-09-27 00:00:00', rep_semantics: 'src1' });
  assert.equal(d.targets.length + d.held.length, 10);   // 例外 (ex1) は対象外
  assert.deepEqual(d.targets.find((x) => x[0] === 'c1').slice(2), ['g1', 'load']);
  assert.deepEqual(d.targets.find((x) => x[0] === 'c9').slice(1), [null, null, null]);
  const lm = (await T.q("select load_conditions from ops.load_materials where ingest_run_id = 'load_p1' and entity = 'products'"))[0].load_conditions;
  assert.equal(lm.has0036, true);
});

await ta('[4] 表の各マス: 付ける / 同じ / 付け替え / 外す (空・自分自身) / 保持 (帰属不明・manual・人が外した・不明・代表が例外の SKU)', async () => {
  const pid = async (code) => Number((await T.q('select product_id from core.skus where code = $1', [code]))[0].product_id);
  const g1 = Number((await T.q("select product_id from core.products where display_code = 'g1'"))[0].product_id);
  await asParentWriter(T.db, async () => {
    await T.q('update core.products set parent_set_by = null where product_id = $1', [await pid('c5')]);                           // c5: 帰属不明
    await T.q("update core.products set parent_set_by = 'manual' where product_id = $1", [await pid('c6')]);                        // c6: 人が付けた
    await T.q("update core.products set parent_product_id = null, parent_set_by = 'manual' where product_id = $1", [await pid('c10')]);   // c10: 人が外した
  });
  const skus = [['c1', 'g1', 'value'], ['c2', 'g2', 'value'], ['c3', null, 'empty'], ['c4', 'c4', 'value'], ['c5', 'g2', 'value'], ['c6', 'g2', 'value'],
    ['c7', null, 'unknown'], ['c8', 'ex1', 'value'], ['c9', 'g1', 'value'], ['c10', 'g1', 'value'], ['ex1', null, 'unknown', 'exception']];
  const r = await load(T.db, planOf(skus, [['g1', ['c1', 'c9', 'c10']], ['g2', ['c2', 'c5', 'c6']], ['ex1', ['c8']]]), 'load_p2');
  assert.equal(r.ok, true, r.error);
  const want = { c1: ['g1', 'load'], c2: ['g2', 'load'], c3: [null, null], c4: [null, null], c5: ['g1', null], c6: ['g1', 'manual'], c7: ['g1', 'load'], c8: ['g1', 'load'], c9: ['g1', 'load'], c10: [null, 'manual'] };
  for (const [c, w] of Object.entries(want)) assert.deepEqual(await stateOf(T.q, c), w, c);
  const d = await decisionsOf(T.q, 'load_p2');
  const g2 = Number((await T.q("select product_id from core.products where display_code = 'g2'"))[0].product_id);
  assert.deepEqual(Object.fromEntries(d.targets.map((x) => [x[0], x.slice(1)])), {
    c1: [g1, 'g1', 'load'], c2: [g2, 'g2', 'load'], c3: [null, null, null], c4: [null, null, null], c9: [g1, 'g1', 'load'] });
  assert.deepEqual(Object.fromEntries(d.held.map((x) => [x[0], x.slice(1)])), {
    c5: ['unknown_owner', g1, 'g1', null], c6: ['manual', g1, 'g1', 'manual'], c7: ['rep_unknown', g1, 'g1', 'load'], c8: ['rep_not_single', g1, 'g1', 'load'], c10: ['manual', null, null, 'manual'] });
  assert.deepEqual([r.summary.variation_unlinks.expected, r.summary.variation_unlinks.applied], [2, 2]);   // c3・c4
  assert.equal(r.summary.variation_parents.expected, 7);                  // まとまりの行 (c1 c9 c10 / c2 c5 c6 / c8)
  assert.equal(r.summary.variation_parents.applied, 2);                   // c2 (付け替え)・c9 (付ける)
  assert.equal(r.summary.variation_parents.same, 1);                      // c1
  // 同じ材料でもう一度 = 何も変わらない (冪等)
  const r2 = await load(T.db, planOf(skus, [['g1', ['c1', 'c9', 'c10']], ['g2', ['c2', 'c5', 'c6']], ['ex1', ['c8']]]), 'load_p3');
  assert.deepEqual([r2.summary.variation_parents.applied, r2.summary.variation_unlinks.expected, r2.summary.variation_unlinks.applied], [0, 0, 0]);
});

await ta('[5] 外せる材料でなければ外さない (matched でない・完了した取得でない・意味の版が src1 でない = material_untrusted)', async () => {
  for (const [m, label] of [[trusted({ status: 'mismatch' }), 'mismatch'], [trusted({ complete: null }), '完了なし'], [trusted({ semantics: null }), '版なし'], [null, '材料の情報なし']]) {
    const { pg, db, q } = await freshDb();
    try {
      await load(db, planOf([['k1', 'g', 'value']], [['g', ['k1']]]), 'load_u1');
      const r = await load(db, planOf([['k1', null, 'empty']], [], m), 'load_u2');
      assert.equal(r.ok, true, r.error);
      assert.deepEqual(await stateOf(q, 'k1'), ['g', 'load'], label);
      const d = await decisionsOf(q, 'load_u2');
      assert.deepEqual(d.held.map((x) => [x[0], x[1]]), [['k1', 'material_untrusted']], label);
      assert.deepEqual([r.summary.variation_unlinks.expected, r.summary.variation_unlinks.skipped], [1, 1], label);
    } finally { await pg.close(); }
  }
});

await ta(`[6] 一度に外しすぎの守り: 外す数が max(${UNLINK_GUARD_MIN}, 2%) を超えたら 1 件も外さない。上限までなら外す`, async () => {
  const { pg, db, q } = await freshDb();
  try {
    const codes = Array.from({ length: UNLINK_GUARD_MIN + 5 }, (_, i) => `m${String(i).padStart(2, '0')}`);
    await load(db, planOf(codes.map((c) => [c, 'g', 'value']), [['g', codes]]), 'load_m1');
    const r = await load(db, planOf(codes.map((c) => [c, null, 'empty']), []), 'load_m2');
    assert.equal(r.ok, true, r.error);
    for (const c of codes) assert.deepEqual(await stateOf(q, c), ['g', 'load']);
    assert.ok(r.conflicts.some((c) => c.kind === 'variation_mass_unlink_guard' && c.candidates === codes.length && c.limit === UNLINK_GUARD_MIN));
    assert.match(r.notes[0], /外す数 25 が上限 20 を超えた/);
    assert.ok((await decisionsOf(q, 'load_m2')).held.every((x) => x[1] === 'mass_unlink_guard'));
    // 上限ちょうど (20) なら外す
    const some = codes.slice(0, UNLINK_GUARD_MIN);
    const r2 = await load(db, planOf(codes.map((c) => [c, some.includes(c) ? null : 'g', some.includes(c) ? 'empty' : 'value']), [['g', codes.filter((c) => !some.includes(c))]]), 'load_m3');
    assert.equal(r2.summary.variation_unlinks.applied, UNLINK_GUARD_MIN);
    assert.deepEqual(await stateOf(q, some[0]), [null, null]);
  } finally { await pg.close(); }
});

await ta('[7] 循環は外す辺を明示の null 辺として検算する (同時に外せば循環しない / manual で旧い辺が残るなら循環 = 付けない)', async () => {
  const { pg, db, q } = await freshDb();
  try {
    // A の親 = B (実在の単品) を作る
    await load(db, planOf([['a', 'b', 'value'], ['b', null, 'empty']], [['b', ['a']]]), 'load_l1');
    assert.deepEqual(await stateOf(q, 'a'), ['b', 'load']);
    // 材料: B の親 = A・A は明示の空 → A を外すので循環しない (両方通る)
    const r = await load(db, planOf([['a', null, 'empty'], ['b', 'a', 'value']], [['a', ['b']]]), 'load_l2');
    assert.equal(r.ok, true, r.error);
    assert.deepEqual([await stateOf(q, 'a'), await stateOf(q, 'b')], [[null, null], ['a', 'load']]);
    assert.equal(r.conflicts.filter((c) => c.kind === 'variation_parent_loop').length, 0);
    // 逆に戻す: A の親 = B (A は manual で残す) → B は明示の空で外せる。次に「B の親 = A」を材料に出しても、A (manual) の旧い辺 A→B が残る = 循環 = 付けない
    const pidA = Number((await q("select product_id from core.skus where code = 'a'"))[0].product_id);
    const pidB = Number((await q("select product_id from core.skus where code = 'b'"))[0].product_id);
    await asParentWriter(db, async () => {
      await q('update core.products set parent_product_id = null, parent_set_by = null where product_id = $1', [pidB]);
      await q("update core.products set parent_product_id = $2, parent_set_by = 'manual' where product_id = $1", [pidA, pidB]);
    });
    const r2 = await load(db, planOf([['a', null, 'empty'], ['b', 'a', 'value']], [['a', ['b']]]), 'load_l3');
    assert.equal(r2.ok, true, r2.error);
    assert.deepEqual([await stateOf(q, 'a'), await stateOf(q, 'b')], [['b', 'manual'], [null, null]]);
    assert.equal(r2.conflicts.filter((c) => c.kind === 'variation_parent_loop').length, 1);
    assert.deepEqual((await decisionsOf(q, 'load_l3')).held.map((x) => [x[0], x[1]]).sort(), [['a', 'manual'], ['b', 'loop']]);
  } finally { await pg.close(); }
});

await ta('[8] 持ち主 company = 名札を作らず親子を触らない (記録は owned: false)', async () => {
  const { pg, db, q } = await freshDb();
  try {
    await load(db, planOf([['k1', 'g', 'value'], ['k2', null, 'empty']], [['g', ['k1']]]), 'load_o1');
    const r = await load(db, planOf([['k1', null, 'empty'], ['k2', 'h', 'value']], [['h', ['k2']]]), 'load_o2', { ownership: { ...MASTER_OWNERSHIP, 'products.parent': 'company' } });
    assert.equal(r.ok, true, r.error);
    assert.deepEqual([await stateOf(q, 'k1'), await stateOf(q, 'k2')], [['g', 'load'], [null, null]]);
    assert.equal((await q("select count(*)::int as n from core.products where display_code = 'h'"))[0].n, 0);
    assert.deepEqual(await decisionsOf(q, 'load_o2'), { owned: false });
    assert.equal(r.summary.variation_groups.expected, 0); assert.equal(r.summary.variation_parents.expected, 0);
  } finally { await pg.close(); }
});

await ta('[9] 0036 の前の DB = 今までどおり (付ける・付け替える・外さない・帰属も記録も書かない・has0036 = false)', async () => {
  const { pg, db, q } = await freshDb('0035');
  try {
    await load(db, planOf([['k1', 'g', 'value'], ['k2', 'g', 'value']], [['g', ['k1', 'k2']]]), 'load_z1');
    const r = await load(db, planOf([['k1', null, 'empty'], ['k2', 'h', 'value']], [['h', ['k2']]]), 'load_z2');
    assert.equal(r.ok, true, r.error);
    const par = async (c) => (await q('select pp.display_code as d from core.skus s join core.products p on p.product_id = s.product_id left join core.products pp on pp.product_id = p.parent_product_id where s.code = $1', [c]))[0].d;
    assert.deepEqual([await par('k1'), await par('k2')], ['g', 'h']);
    assert.ok(!('variation_unlinks' in r.summary));
    assert.equal((await q("select count(*)::int as n from ops.load_decisions where section = 'variation_parents'"))[0]?.n ?? 0, 0);
    assert.equal((await q("select load_conditions from ops.load_materials where ingest_run_id = 'load_z2' and entity = 'products'"))[0].load_conditions.has0036, false);
    assert.ok(r.notes.some((n) => /0036 が未適用/.test(n)));
  } finally { await pg.close(); }
});

console.log('送る形・材料の意味の版');
await ta('[10] 送る形の代表: JOIN 不成立 = NULL / 値 = そのまま / 空は元の値が "" のときだけ \'\' (null・記録なし = NULL)。_src の列が無い DB では空は全部 NULL', async () => {
  const m = new Database(':memory:');
  m.exec(`CREATE TABLE m_products (商品コード TEXT, 商品名 TEXT); CREATE TABLE m_set_components (セット商品コード TEXT);
    CREATE TABLE raw_ne_products (商品コード TEXT, 代表商品コード TEXT, 代表商品コード_src TEXT);`);
  const ins = m.prepare('INSERT INTO m_products VALUES (?, ?)');
  for (const c of ['v1', 'e1', 'n1', 'u1', 'set1', 's1']) ins.run(c, c);
  const raw = m.prepare('INSERT INTO raw_ne_products VALUES (?, ?, ?)');
  raw.run('v1', 'grp', '"grp"'); raw.run('e1', '', '""'); raw.run('n1', '', 'null'); raw.run('u1', '', null); raw.run('s1', 's1', '"s1"');
  const rep = (mm) => Object.fromEntries(readMasterMaterial(mm).products.map((r) => [r.商品コード, r.代表商品コード]));
  assert.deepEqual(rep(m), { v1: 'grp', e1: '', n1: null, u1: null, set1: null, s1: 's1' });
  assert.deepEqual(readMasterMaterial(m).semantics, { rep: MATERIAL_REP_SEMANTICS });
  const m2 = new Database(':memory:');
  m2.exec(`CREATE TABLE m_products (商品コード TEXT); CREATE TABLE m_set_components (x TEXT); CREATE TABLE raw_ne_products (商品コード TEXT, 代表商品コード TEXT);`);
  m2.prepare('INSERT INTO m_products VALUES (?)').run('e1'); m2.prepare('INSERT INTO m_products VALUES (?)').run('v1');
  m2.prepare('INSERT INTO raw_ne_products VALUES (?, ?)').run('e1', ''); m2.prepare('INSERT INTO raw_ne_products VALUES (?, ?)').run('v1', 'grp');
  assert.deepEqual(rep(m2), { e1: null, v1: 'grp' });
});

await ta('[11] 材料の意味の版: src1 の材料の \'\' だけ明示の空。版なし・知らない版は不明。受け手は短い英小文字だけ残す', () => {
  assert.equal(representativeStateOf('grp', null), 'value');
  assert.equal(representativeStateOf('', 'src1'), 'empty');
  assert.equal(representativeStateOf('  ', 'src1'), 'empty');
  assert.equal(representativeStateOf('', null), 'unknown');
  assert.equal(representativeStateOf('', 'src2'), 'unknown');
  assert.equal(representativeStateOf(null, 'src1'), 'unknown');
  assert.equal(validMaterialSemantics({ rep: 'src1' }), '{"rep":"src1"}');
  assert.equal(validMaterialSemantics({ rep: 'SRC1' }), null);
  assert.equal(validMaterialSemantics({ rep: 1 }), null);
  assert.equal(validMaterialSemantics(['src1']), null);
  assert.equal(validMaterialSemantics(null), null);
  assert.equal(validMaterialSemantics({ a: 'x', b: 'x', c: 'x', d: 'x', e: 'x' }), null);
});

await ta('[12] 外しすぎの上限の境界 (2% 側が効く件数) / ① の要約は 0036 の前のロードなら差があっても「比べていない」と書く / 日時の文字列の妥当性', () => {
  assert.deepEqual([0, 999, 1000, 1049, 1050, 2165, 5000].map(unlinkGuardLimit), [20, 20, 20, 20, 21, 43, 100]);
  const base = { load: { ingest_run_id: 'load_x' }, counts: { items: 1, by_type: { value: 1 }, compared: { value: 3, parent: 0 } } };
  assert.match(summaryLine({ ...base, verdict: 'breach', parent_not_compared: 'no_0036' }), /代表の親子は比べていない/);
  assert.match(summaryLine({ ...base, verdict: 'breach' }), /代表の親子 0\)/);
  assert.match(summaryLine({ ...base, verdict: 'pass', counts: { ...base.counts, items: 0 }, parent_not_compared: 'no_0036' }), /代表の親子は比べていない/);
  for (const ok of ['2026-09-27 00:00:00', '2026-09-27T00:00:00Z', '2026-09-27 00:00:00.123+09', '2024-02-29 23:59:59+09:00']) assert.equal(validTimestampText(ok), true, ok);
  for (const bad of ['2026-99-99 00:00:00', '2026-02-30 00:00:00', '2025-02-29 00:00:00', '2026-09-27 24:00:00', '2026-09-27 00:60:00', '2026-09-27 00:00:00+15', 'きのう', null]) assert.equal(validTimestampText(bad), false, String(bad));
});

await T.pg.close();
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
