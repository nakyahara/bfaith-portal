/**
 * test-sku-kind-to-set.mjs — scripts/company-db/sku-kind-to-set.mjs (単品の SKU を NE と同じセットにする道具) の試験 (PGlite・miniPC の warehouse.db の形)
 *
 * 切替の後の DB (fixtures/master-variation-db.mjs = new_open・backfill 済み・持ち主は本番と同じ config/master-ownership.mjs = 区分 company・構成 load)。
 * NE では nametag30 = nametag10 × 3・nametag50 = nametag10 × 5 のセット・Company DB では単品 (10/15 の本番と同じ形)。夜間ロードは sku_kind_held を記録するだけ。
 * 固定する契約:
 *   1 引数 (--code・--actor・--reason が要る・--apply は --expect-hash と --yes)
 *   2 dry-run = 何も書かない・計画のハッシュ・照合の区分の生の差の見込み (今 3 → 直した後 1)
 *   3 ハッシュが違う apply = 巻き戻す (何も変えない)・保守の印が無い直接の UPDATE は G18 が断る (道具の外の道は無い)
 *   4 apply = セット・product_id なし・NE の構成 (source ne)・原価は今日から set_calc・前の原価は昨日で閉じる・商品の行は残る・変更の記録 (人・理由・request_id)・最終形 0
 *   5 次の夜間ロード (同じ NE の材料) = 直した 2 つは sku_kind_held に出ない・構成は「同じ」で触らない (行も作った人もそのまま)
 *   6 断る: NE でセットでない・夜間ロードの記録が無い・ほかのセットの構成品・まとまりの子・NE の取得が古い・もうセット・NE の構成品が Company DB に無い
 *   7 (Codex R1) 今日以降を覆う閉じた原価の行 = 断る・commit の答えが分からない = SKU の鍵の後に読む・構成の判断の記録の形が違う = 断る
 *   8 (Codex R2) 夜間ロードが読んだ構成の材料の控え (構成品と数量) が NE の取得と違う・控えが無い = 断る・構成品に今回セットにするコードがある (入れ子) = 断る
 * 使い方: node scripts/test-sku-kind-to-set.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'kind-set-'));
process.env.DATA_DIR = tmp;
const quietly = async (fn) => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = () => {}; try { return await fn(); } finally { console.log = l; console.warn = w; } };
const WH = await quietly(() => import('../apps/warehouse/db.js'));
await quietly(() => WH.initDB());
const { setupVariationDb, basePlan, skuOf } = await import('./fixtures/master-variation-db.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
const X = await import('./company-db/sku-kind-to-set.mjs');
const ML = await import('../apps/warehouse/material-lineage.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

// ── Company DB (切替の後) ──
//   nametag10 (構成品)・nametag30 / nametag50 (直す相手)・nametag99 (NE はセット・社内は単品・ほかのセット giftbox の構成品)・giftbox (社内もセット)
const extra = [skuOf('nametag10', 'ネームタグ 10枚', { cost: { jpy: 300, source: 'ne', status: 'COMPLETE' } }), skuOf('nametag30', 'ネームタグ 3個セット', { cost: { jpy: 777, source: 'ne', status: 'COMPLETE' } }),
  skuOf('nametag50', 'ネームタグ 5個セット'), skuOf('nametag99', 'ネームタグ 2個'), skuOf('nametag70', 'ネームタグ 7個'), skuOf('giftbox', 'ギフトボックス', { kind: 'set' })];
const T = await setupVariationDb({ extraSkus: extra });
const { pg, db, q, one } = T;
await T.W2.setActiveOwnershipInDb(pg, MASTER_OWNERSHIP);   // 本番と同じ持ち主 (区分 company・構成 load)
const id = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;
// giftbox の構成 = nametag99 (社内で構成品にしている単品)
await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'ne')`, [await id('giftbox'), await id('nametag99')]);

// NE (今の本番の形): nametag30 / 50 / 99 は NE ではセット
const NE_SETS = [['nametag30', 'ボトルキープ ネームタグ 【10枚】3個セット', 'nametag10', 3, 980], ['nametag50', 'ボトルキープ ネームタグ 【10枚】5個セット', 'nametag10', 5, 1480],
  ['nametag99', 'ネームタグ 2個', 'nametag10', 2, 600], ['nametag70', 'ネームタグ 7個', 'nametag10', 7, 1980], ['giftbox', 'ギフトボックス', 'nametag99', 1, 2980]];
/** 夜間ロードの plan (材料の形)。sets = 材料でセットのコード (構成あり) */
function nePlan(sets) {
  const p = basePlan(extra);
  const setCodes = new Set(sets.map((x) => x[0]));
  p.skus = p.skus.map((s) => (setCodes.has(s.code) ? { ...s, kind: 'set', name: sets.find((x) => x[0] === s.code)[1] } : s));
  p.setComponents = sets.map(([parent, , child, qty]) => ({ parentCode: parent, childCode: child, qty, source: 'ne' }));
  return p;
}
/**
 * 材料 = 作り直し (apps/warehouse/rebuild-m-products.js) の形: 区分の持ち主が company = 社内の区分の写しを重ねる
 *   (NE はセット・社内は単品 = 材料は「単品・構成なし」= 10/17 の本番の dry-run で分かった形)。matOf(社内でセットのコード) = 材料のセットの行
 */
const matOf = (cdbSets) => NE_SETS.filter((x) => cdbSets.includes(x[0]));
const MAT_BEFORE = matOf(['giftbox']);
/** 夜間ロードが読んだ材料 (世代と miniPC の DATA_DIR の控え = 本番の sync-to-render と同じ形)。plan も同じ材料から */
function withMaterial(plan, matSets) {
  const setCodes = new Set(matSets.map((x) => x[0]));
  const set_components = matSets.map(([parent, , child, qty]) => ({ セット商品コード: parent, 構成商品コード: child, 数量: qty }));
  const products = plan.skus.map((x, i) => ({ product_id: i + 1, 商品コード: x.code, 商品区分: setCodes.has(x.code) ? 'セット' : '単品' }));
  const g = ML.buildMaterialGeneration({ products, set_components, now: new Date() });
  ML.saveMaterialSnapshot({ dataDir: tmp, generation: g, products, set_components });
  const m = (e) => ({ status: 'matched', content_hash: g[e].content_hash, row_count: g[e].row_count, generation: { generation_id: g.generation_id, created_at: g.created_at } });
  plan.material = { products: m('products'), set_components: m('set_components') };
  return plan;
}
const nightly = async (runId, matSets = MAT_BEFORE) => {
  const r = await runInitialLoad(db, withMaterial(nePlan(matSets), matSets), { log: quiet, runId, ownership: MASTER_OWNERSHIP }); assert.equal(r.ok, true, r.error); return r;
};

// ── miniPC の warehouse.db の NE の完全な取得 (照合 ② が読む形・test-master-compare-ne.mjs の setNe と同じ書き方) ──
const J = (v) => JSON.stringify(v);
const utcText = (d) => d.toISOString().replace('T', ' ').slice(0, 19);
function setNe({ sets = NE_SETS, at = new Date() } = {}) {
  const d = WH.getDB(); const ts = utcText(at);
  const setCodes = new Set(sets.map((x) => x[0]));
  const products = basePlan(extra).skus.map((s) => ({ code: s.code, name: s.name }));
  d.exec('DELETE FROM raw_ne_products; DELETE FROM raw_ne_set_products');
  const ip = d.prepare('INSERT INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 取扱区分, 原価, 売価, 消費税率, synced_at, 原価_src, 売価_src, 消費税率_src, 代表商品コード, 代表商品コード_src) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)');
  for (const r of products) ip.run(r.code, r.name, '0001', '取扱中', 0, 0, 0, ts, J(setCodes.has(r.code) ? '0' : '1100'), J('2980'), J('10'), '', J(''));
  const is = d.prepare('INSERT INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at, セット販売価格_src, 数量_src) VALUES (?,?,?,?,?,?,?,?)');
  for (const [parent, name, child, qty, price] of sets) is.run(parent, name, 0, child, 1, ts, J(String(price)), J(String(qty)));
  const meta = (k) => d.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value;
  const up = (k, v) => d.prepare('INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)').run(k, v, ts);
  up('ne_api_products_complete_at', ts); up('ne_api_products_complete_count', String(products.length)); up('ne_api_products_complete_rev', meta('ne_raw_products_rev'));
  up('ne_api_setproducts_complete_at', ts); up('ne_api_setproducts_complete_count', String(sets.length)); up('ne_api_setproducts_complete_rev', meta('ne_raw_setproducts_rev'));
  up('ne_api_setproducts_complete_parents', String(sets.length));
  up('ne_api_products_integrity', J({ fetched_rows: 0, written_rows: 0, dropped_no_code: 0, distinct_codes: 0, dup_code_count: 0, dup_codes: [] }));
  up('ne_api_setproducts_integrity', J({ fetched_rows: 0, valid_rows: 0, dropped_missing_key: 0, dropped_missing_parent: 0, missing_child_parents: [], parent_conflict_count: 0, parent_conflicts: [], pair_dup_count: 0, pair_dups: [] }));
  const fc = (kind, n, rev, detail, notes) => J({ version: 'fc1', kind, fetch_fingerprint: 'a'.repeat(64), complete_at: ts, complete_rev: Number(rev),
    started_at: `${ts.replace(' ', 'T')}.000Z`, finished_at: `${ts.replace(' ', 'T')}.000Z`, fetched_rows: n, write_attempts: n, stored_rows: n,
    dropped_no_code: 0, dropped_missing_fields: 0, dropped_missing_detail: detail, notes, page_limit: 1000, pages: 1, page_rows: [n], last_page_rows: n });
  up('ne_api_products_fetch_counts', fc('products', products.length, meta('ne_raw_products_rev'), {}, {}));
  up('ne_api_setproducts_fetch_counts', fc('setproducts', sets.length, meta('ne_raw_setproducts_rev'), { set_goods_detail_goods_id: 0 }, { quantity_defaulted_rows: 0 }));
}
const neNow = async (o) => { setNe(o); return X.readNeContext(tmp); };

const ACTOR = 'naka@test';
const REASON = 'NE で 10/15 にセットにした nametag30 / nametag50 を Company DB もセットにする (試験)';
const opts = (codes, x = {}) => X.parseKindArgs([...codes.flatMap((c) => ['--code', c]), '--actor', ACTOR, '--reason', REASON, ...(x.apply ? ['--apply', '--expect-hash', x.hash, '--yes'] : [])]);
const run = (o, ne, extraOpts = {}) => X.runKind(db, o, { ne, log: quiet, ...extraOpts });
const counts = async () => one(`select (select count(*)::int from core.skus where sku_kind = 'set') as sets, (select count(*)::int from core.sku_components) as comps,
  (select count(*)::int from core.sku_costs) as costs, (select count(*)::int from events.master_change_events) as events, (select count(*)::int from core.products) as products`);

// 夜間ロード (10/17 の本番の形 = NE はセット・社内は単品・材料は社内の区分の写し = 単品・構成なし)
const before = await nightly('load_kind_1');
assert.deepEqual(before.conflicts.filter((c) => c.kind === 'sku_kind_held').map((c) => c.code), []);   // 材料は社内の区分の写し = 区分のずれは記録されない (本番の 10/17 と同じ)

await ta('[1] 引数: --code・--actor・--reason が要る・--apply は --expect-hash と --yes・同じコード 2 回は断る', async () => {
  assert.throws(() => X.parseKindArgs(['--actor', ACTOR, '--reason', 'x']), /--code/);
  assert.throws(() => X.parseKindArgs(['--code', 'a', '--reason', 'x']), /--actor/);
  assert.throws(() => X.parseKindArgs(['--code', 'a', '--actor', ACTOR]), /--reason/);
  assert.throws(() => X.parseKindArgs(['--code', 'a', '--code', 'A', '--actor', ACTOR, '--reason', 'x']), /重なっている/);
  assert.throws(() => X.parseKindArgs(['--code', 'a', '--actor', ACTOR, '--reason', 'x', '--apply', '--yes']), /--expect-hash/);
  assert.throws(() => X.parseKindArgs(['--code', 'a', '--actor', ACTOR, '--reason', 'x', '--apply', '--expect-hash', 'f'.repeat(64)]), /--yes/);
  assert.throws(() => X.parseKindArgs(['--code', 'a b', '--actor', ACTOR, '--reason', 'x']), /英数字/);
});

let HASH = null;
await ta('[2] dry-run = 何も書かない・計画 (セット・構成・原価)・照合の区分の生の差の見込み (今 3 → 直した後 1)', async () => {
  const ne = await neNow();
  const c0 = await counts();
  const r = await run(opts(['nametag30', 'nametag50']), ne);
  assert.deepEqual(r.problems, [], r.problems.join('\n'));
  assert.equal(r.exitCode, 0);
  assert.deepEqual(await counts(), c0);
  assert.deepEqual(r.plan.items.map((it) => [it.code, it.components.map((c) => [c.code, c.qty])]), [['nametag30', [['nametag10', 3]]], ['nametag50', [['nametag10', 5]]]]);
  const cost30 = r.plan.items[0].derived.changes.find((c) => c.col === 'cost');
  assert.deepEqual([cost30.from, cost30.to, r.plan.items[0].derived.cost_plan.source], [777, 900, 'set_calc']);
  assert.deepEqual(r.estimate, { now: ['nametag30', 'nametag50', 'nametag70', 'nametag99'], after: ['nametag70', 'nametag99'], integrity_untrusted: 0 });
  // 同じ入力 = 同じハッシュ / 理由が違う = 違うハッシュ
  assert.equal((await run(opts(['nametag30', 'nametag50']), ne)).hash, r.hash);
  const o2 = opts(['nametag30', 'nametag50']); o2.reason = '別の理由';
  assert.notEqual((await run(o2, ne)).hash, r.hash);
  HASH = r.hash;
});

await ta('[3] ハッシュが違う apply = 巻き戻す (何も変えない)・保守の印の無い直接の UPDATE は G18 が断る', async () => {
  const ne = await neNow();
  const c0 = await counts();
  const r = await run(opts(['nametag30', 'nametag50'], { apply: true, hash: 'e'.repeat(64) }), ne);
  assert.deepEqual([r.ok, r.outcome, r.error.reason], [false, 'rolled_back', 'plan_mismatch']);
  assert.deepEqual(await counts(), c0);
  await assert.rejects(pg.query(`update core.skus set sku_kind = 'set', product_id = null where code = 'nametag30'`), /sku_kind_locked/);
});

await ta('[4] apply = セット・product_id なし・NE の構成 (source ne)・原価は今日から set_calc (前は昨日で閉じる)・商品の行は残る・変更の記録 (人・理由・request_id)・最終形 0', async () => {
  const ne = await neNow();
  const dry = await run(opts(['nametag30', 'nametag50']), ne);
  const c0 = await counts();
  const p30 = (await one(`select product_id::text as p from core.skus where code = 'nametag30'`)).p;
  const r = await run(opts(['nametag30', 'nametag50'], { apply: true, hash: dry.hash }), ne);
  assert.deepEqual([r.ok, r.outcome], [true, 'committed'], JSON.stringify(r.error));
  const k = await q(`select code, sku_kind, product_id from core.skus where code in ('nametag30', 'nametag50') order by code`);
  assert.deepEqual(k.map((x) => [x.code, x.sku_kind, x.product_id]), [['nametag30', 'set', null], ['nametag50', 'set', null]]);
  const comps = await q(`select p.code as parent, c.code as child, x.qty, x.source, x.sort_order, x.created_by_id from core.sku_components x join core.skus p on p.sku_id = x.parent_sku_id
    join core.skus c on c.sku_id = x.child_sku_id where p.code like 'nametag%' order by p.code`);
  assert.deepEqual(comps.map((x) => [x.parent, x.child, x.qty, x.source, x.sort_order, x.created_by_id]), [['nametag30', 'nametag10', 3, 'ne', 0, ACTOR], ['nametag50', 'nametag10', 5, 'ne', 0, ACTOR]]);
  const costs = await q(`select x.cost_jpy::int as jpy, x.cost_source, x.valid_from::text as f, x.valid_to::text as t from core.sku_costs x join core.skus s on s.sku_id = x.sku_id where s.code = 'nametag30' order by x.valid_from`);
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  assert.deepEqual(costs.slice(-1).map((x) => [x.jpy, x.cost_source, x.f, x.t]), [[900, 'set_calc', today, null]]);
  assert.equal(costs.filter((x) => x.t == null).length, 1);
  assert.equal(costs[0].cost_source, 'ne'); assert.ok(costs[0].t < today);
  assert.equal((await one('select count(*)::int as n from core.products where product_id = $1', [p30])).n, 1);   // 商品の行は消さない
  assert.equal((await counts()).products, c0.products);
  const ev = await q(`select distinct actor_type, actor_id, source_system, request_id, reason_text from events.master_change_events where request_id = $1`, [r.request_id]);
  assert.deepEqual(ev.map((e) => [e.actor_type, e.actor_id, e.source_system, e.reason_text]), [['human', ACTOR, 'sku_kind_to_set', REASON]]);
  assert.ok((await one(`select count(*)::int as n from events.master_change_events where request_id = $1 and attribute = 'sku_kind'`, [r.request_id])).n === 2);
  assert.deepEqual((await one('select ops.sku_kind_shape_counts(1) as c')).c, { single_product_mismatch: 0, non_set_parent_components: 0 });
  // 同じ計画をもう一度 = 断る (もうセット)
  const again = await run(opts(['nametag30']), ne);
  assert.match(again.problems.join(' '), /区分が set/);
  // 照合の区分の生の差の見込み = nametag99 だけ
  const r99 = await run(opts(['nametag99']), ne);
  assert.deepEqual(r99.estimate.now, ['nametag70', 'nametag99']);
});

await ta('[5] 次の夜間ロード: 材料がまだ単品 (写しの前) = sku_kind_held (材料 単品・社内 セット) を記録するだけで区分・構成・商品は変えない / 翌朝の写しの後の材料 (セット + 構成) = 何も記録しない・構成は「同じ」', async () => {
  const comps0 = await q(`select parent_sku_id::text, child_sku_id::text, qty, source, created_by_id from core.sku_components order by 1, 2`);
  const p0 = (await counts()).products;
  let r = await nightly('load_kind_2');   // 材料 = 直す前の写し (単品・構成なし)
  assert.deepEqual(r.conflicts.filter((c) => c.kind === 'sku_kind_held').map((c) => [c.code, c.ne_kind, c.cdb_kind]).sort(), [['nametag30', 'single', 'set'], ['nametag50', 'single', 'set']]);
  assert.deepEqual(await q(`select parent_sku_id::text, child_sku_id::text, qty, source, created_by_id from core.sku_components order by 1, 2`), comps0);
  const k = await q(`select code, sku_kind, product_id from core.skus where code in ('nametag30', 'nametag50') order by code`);
  assert.deepEqual(k.map((x) => [x.sku_kind, x.product_id]), [['set', null], ['set', null]]);
  assert.equal((await counts()).products, p0);   // 単品の商品を作り直さない
  assert.deepEqual((await one('select ops.sku_kind_shape_counts(1) as c')).c, { single_product_mismatch: 0, non_set_parent_components: 0 });
  r = await nightly('load_kind_2b', matOf(['giftbox', 'nametag30', 'nametag50']));   // 翌朝の写し → 作り直しの後の材料
  assert.deepEqual(r.conflicts.filter((c) => c.kind === 'sku_kind_held').map((c) => c.code), []);
  assert.deepEqual(await q(`select parent_sku_id::text, child_sku_id::text, qty, source, created_by_id from core.sku_components order by 1, 2`), comps0);
  const d = (await one(`select payload from ops.load_decisions where ingest_run_id = 'load_kind_2b' and section = 'skus'`)).payload;
  assert.deepEqual(d.kind_held, []);
  assert.deepEqual((await one('select ops.sku_kind_shape_counts(1) as c')).c, { single_product_mismatch: 0, non_set_parent_components: 0 });
});

await ta('[6] 断る: ほかのセットの構成品 (nametag99)・まとまりの子 (ws100-BR)・NE でセットでない・NE の取得が古い・NE の構成品が Company DB に無い・夜間ロードの記録が無い', async () => {
  const ne = await neNow();
  const c0 = await counts();
  let r = await run(opts(['nametag99']), ne);
  assert.match(r.problems.join(' '), /ほかのセットの構成品/); assert.equal(r.exitCode, 1);
  r = await run(opts(['ws100-BR']), ne);
  assert.match(r.problems.join(' '), /まとまり \(色違い\) の子/);
  assert.match(r.problems.join(' '), /NE の取得でセットでない/);
  // NE の取得が 40 時間前
  r = await run(opts(['nametag99']), await neNow({ at: new Date(Date.now() - 40 * 3600e3) }));
  assert.match(r.problems.join(' '), /36 時間より古い/);
  // NE の構成品が Company DB に無い
  r = await run(opts(['nametag99']), await neNow({ sets: [...NE_SETS.filter((x) => x[0] !== 'nametag99'), ['nametag99', 'ネームタグ 2個', 'nothere1', 2, 600]] }));
  assert.match(r.problems.join(' '), /構成品 nothere1 が Company DB に 0 件/);
  // NE の取得が読めない
  r = await run(opts(['nametag99']), { error: 'no_warehouse_db' });
  assert.match(r.problems.join(' '), /warehouse\.db\) を読めない/);
  // apply も同じ理由で断る (計画のハッシュを合わせても)
  const dry = await run(opts(['nametag99']), await neNow());
  r = await run(opts(['nametag99'], { apply: true, hash: dry.hash }), await neNow());
  assert.deepEqual([r.ok, r.error.reason], [false, 'not_fixable']);
  assert.deepEqual(await counts(), c0);
});

await ta('[7] (Codex R1 High) 今日以降を覆う閉じた原価の行がある = 断る (今日からの set_calc を足すと期間が重なる・DB の守りは持ち主の書き込みを見ない)', async () => {
  const ne = await neNow();
  assert.deepEqual((await run(opts(['nametag70']), ne)).problems, []);
  // 開いた原価の行を 3 日後で閉じる (DB の持ち主の直接の書き込み = 0051 の守りは見ない)
  await pg.query(`update core.sku_costs set valid_to = (now() at time zone 'Asia/Tokyo')::date + 3 where sku_id = $1 and valid_to is null`, [await id('nametag70')]);
  const c0 = await counts();
  const r = await run(opts(['nametag70']), ne);
  assert.match(r.problems.join(' '), /以降を覆う閉じた原価の行が 1 行/);
  const a = await run(opts(['nametag70'], { apply: true, hash: r.hash }), ne);
  assert.deepEqual([a.ok, a.error.reason], [false, 'not_fixable']);
  assert.deepEqual(await counts(), c0);
});

await ta('[8] (Codex R1 Medium) commit の答えが分からないとき = SKU の鍵を取ってから読む: 書けた request_id = written・知らない request_id = not_written', async () => {
  const ev = (await one(`select request_id from events.master_change_events where source_system = 'sku_kind_to_set' and attribute = 'sku_kind' limit 1`)).request_id;
  const reconnect = async () => ({ db, close: async () => {} });
  const ids = [await id('nametag30'), await id('nametag50')];
  assert.deepEqual(await X.outcomeOf(reconnect, ev, ids), { state: 'written' });
  assert.deepEqual(await X.outcomeOf(reconnect, '00000000-0000-4000-8000-000000000000', [await id('nametag70')]), { state: 'not_written' });
  assert.equal((await X.outcomeOf(null, ev, ids)).state, 'unknown');
});

await ta('[8b] (Codex R2) 夜間ロードの材料の構成の数量が NE の取得と違う = 断る・構成品に今回セットにするコードがある (入れ子) = 断る・材料の控えが無い = 断る', async () => {
  // 材料の nametag70 = nametag10 × 6 (NE の取得は × 7)
  await nightly('load_kind_qty', [...MAT_BEFORE, ['nametag70', 'ネームタグ 7個', 'nametag10', 6, 1980]]);   // 写しを重ねない材料の形 (セット) で数量だけ違う
  await pg.query(`update core.sku_costs set valid_to = null where sku_id = $1 and valid_to is not null and valid_to >= (now() at time zone 'Asia/Tokyo')::date`, [await id('nametag70')]);   // [7] の閉じた行を戻す
  let r = await run(opts(['nametag70']), await neNow());
  assert.match(r.problems.join(' '), /受け入れる形でない \(材料の商品区分 セット・材料の構成 nametag10 × 6・NE の取得の構成 nametag10 × 7・判断の記録 set \/ single\)/);
  assert.equal(r.problems.length, 1, r.problems.join('\n'));
  await nightly('load_kind_qty2', [...MAT_BEFORE, ['nametag70', 'ネームタグ 7個', 'nametag10', 7, 1980]]);   // 材料 = セット・NE と同じ (形 B)
  assert.deepEqual((await run(opts(['nametag70']), await neNow())).problems, []);
  // 入れ子: NE で nametag70 の構成品が nametag99 (どちらも今回セットにする)
  const nest = NE_SETS.map((x) => (x[0] === 'nametag70' ? [x[0], x[1], 'nametag99', 1, x[4]] : x));
  await nightly('load_kind_nest');
  r = await run(opts(['nametag70', 'nametag99']), await neNow({ sets: nest }));
  assert.match(r.problems.join(' '), /構成品 nametag99 も今回セットにする/);
  // 材料の控えが DATA_DIR に無い
  const lm = (await one(`select generation_id from ops.load_materials where ingest_run_id = 'load_kind_nest' and entity = 'set_components'`)).generation_id;
  const file = path.join(tmp, fs.readdirSync(tmp).find((d) => fs.existsSync(path.join(tmp, d, `${lm}.json.gz`))), `${lm}.json.gz`);
  fs.renameSync(file, `${file}.bak`);
  r = await run(opts(['nametag70']), await neNow({ sets: nest }));
  assert.match(r.problems.join(' '), /材料の控え .* が DATA_DIR に無い/);
  fs.renameSync(`${file}.bak`, file);
  await nightly('load_kind_back');   // 元の NE に戻す
});

await ta('[9] (Codex R1 Medium) 構成の持ち主が load なのに最新の夜間ロードの構成の判断の記録が持ち主 company の形 = 断る (夜間ロードの材料の構成と照らせない)', async () => {
  const r0 = await runInitialLoad(db, withMaterial(nePlan(MAT_BEFORE), MAT_BEFORE), { log: quiet, runId: 'load_kind_3', ownership: { ...MASTER_OWNERSHIP, sku_components: 'company' } });
  assert.equal(r0.ok, true, r0.error);
  const r = await run(opts(['nametag70']), await neNow());
  assert.match(r.problems.join(' '), /構成の判断の記録 \(set_components\) が無い \/ 形が違う/);
});

console.log(`\n${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
