/**
 * test-set-derived-load.mjs — 夜間ロードがセットの構成を変えた後の導く値 (税率・取扱・原価) の計算し直し (lib/master-write.mjs の recomputeSetsAfterLoad・engine.mjs の commit の後) と、
 * 前から残るずれの後始末の道具 (scripts/company-db/set-derived-catchup.mjs) の試験 (PGlite)
 *
 * 切替の後の DB (fixtures/master-variation-db.mjs = new_open・backfill 済み)・持ち主は本番と同じ config/master-ownership.mjs (導く値 company・構成 load)。
 * 固定する契約:
 *   L1 構成を入れただけで導く値が今と同じ = 何も書かない (計算し直しの変更の記録なし)
 *   L2 NE で構成が変わった (夜間ロードが core.sku_components を書き直した) = commit の後に、画面の保存と同じ規則で税率・取扱・原価を計算し直す
 *      (原価は今日から set_calc・前の行は昨日で閉じる)・変更の記録 = system・company_db_load・夜間ロードの run_id・理由「構成の変更に合わせて計算し直し」
 *   L3 人の値は残す: 例外原価 (manual) は構成が変わっても上書きしない・先の日付の原価があるセットは計算しない (set_cost_future = 次の晩にもう一度)
 *   L4 同じ材料でもう一度 (構成は変わらない・前の 7 日に変えたセットも見る) = 何も書かない (記録も増えない)
 *   L5 守り: 対象が多すぎ = 1 つも計算し直さない・dry-run のロード = 計算し直さない・構成の持ち主が company = 呼ばない (構成の昇格が計算し直す)
 *   C1 後始末の道具: dry-run = 何も書かない・直すセット (前から残ったずれ・原価が空のセット) と人の値で残すセット (例外原価)・計画のハッシュ
 *   C2 ハッシュが違う apply = 巻き戻す・同じ = 書く (人・理由・request_id・set_derived_catchup)・2 回目の dry-run = 直すもの 0・もう一度 apply = 何も書かない
 *   C3 --code で絞る・セットでないコード = 断る・引数
 * 使い方: node scripts/test-set-derived-load.mjs
 */
import assert from 'node:assert/strict';

const { setupVariationDb, basePlan, skuOf } = await import('./fixtures/master-variation-db.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
const W = await import('../lib/master-write.mjs');
const X = await import('./company-db/set-derived-catchup.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

const cost = (jpy, source = 'ne', status = 'COMPLETE') => ({ cost: { jpy, source, status } });
const setCost = (jpy) => ({ kind: 'set', ...cost(jpy, 'set_calc') });
const extra = [
  skuOf('pa1', '構成品 A', cost(300)), skuOf('pa2', '構成品 B (8%)', { taxRate: 0.08, taxClass: 'REDUCED_8', ...cost(500) }), skuOf('pa3', '構成品 C (止める)', cost(200)),
  skuOf('setx', 'セット X (中身が変わる)', setCost(600)), skuOf('sety', 'セット Y (例外原価)', setCost(300)), skuOf('setz', 'セット Z (変わらない)', setCost(300)),
  skuOf('setw', 'セット W (止まった構成品になる)', setCost(300)), skuOf('setv', 'セット V (先の日付の原価)', setCost(300)),
];
const T = await setupVariationDb({ extraSkus: extra });
const { pg, db, q, one } = T;
await T.W2.setActiveOwnershipInDb(pg, MASTER_OWNERSHIP);   // 本番と同じ持ち主 (導く値 company・構成 load)
assert.equal(MASTER_OWNERSHIP.sku_components, 'load');
for (const k of W.SET_DERIVED_KEYS) assert.equal(MASTER_OWNERSHIP[k], 'company', k);

const COMPS1 = { setx: [['pa1', 2]], sety: [['pa1', 1]], setz: [['pa1', 1]], setw: [['pa1', 1]], setv: [['pa1', 1]] };
const COMPS2 = { setx: [['pa1', 1], ['pa2', 1]], sety: [['pa1', 3]], setz: [['pa1', 1]], setw: [['pa3', 1]], setv: [['pa1', 2]] };
function planOf(comps) {
  const p = basePlan(extra);
  p.setComponents = Object.entries(comps).flatMap(([parent, rows]) => rows.map(([child, qty]) => ({ parentCode: parent, childCode: child, qty, source: 'ne' })));
  return p;
}
let seq = 0;
const nightly = async (comps, opts = {}) => {
  const runId = `load_sd_${++seq}`;
  const r = await runInitialLoad(db, planOf(comps), { log: quiet, runId, ownership: MASTER_OWNERSHIP, host: 'render-nightly', ...opts });
  assert.equal(r.ok, true, r.error);
  return r;
};
const today = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
const sid = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;
const skuRow = async (code) => one('select tax_rate::float8 as tax_rate, tax_class, handling from core.skus where code = $1', [code]);
const costsOf = async (code) => (await q(`select c.cost_jpy::int as jpy, c.cost_source as src, c.cost_status as st, c.valid_from::text as f, c.valid_to::text as t
  from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = $1 order by c.valid_from, c.sku_cost_id`, [code]));
const openCost = async (code) => (await costsOf(code)).find((c) => c.t == null) ?? null;
/** 計算し直しの変更の記録 (理由で見分ける) */
const derivedEvents = async (code, source = 'company_db_load') => q(`select e.entity_type, e.attribute, e.actor_type, e.actor_id, e.source_system, e.run_id, e.request_id, e.reason_text
  from events.master_change_events e left join core.sku_costs c on e.entity_type = 'sku_cost' and c.sku_cost_id = e.entity_id
  where e.source_system = $2 and e.reason_text is not null and e.entity_type in ('sku', 'sku_cost') and coalesce(case when e.entity_type = 'sku' then e.entity_id end, c.sku_id, (e.old_value ->> 'sku_id')::bigint) = $1::bigint
  order by e.event_id`, [await sid(code), source]);

// 例外原価 (人が決めた原価) のセット Y・先の日付の原価のセット V は、構成が入る前から (持ち主のロール = 画面の保存と同じ形の行)
async function replaceOpen(code, row) {
  const id = await sid(code);
  await pg.query(`update core.sku_costs set valid_to = $2::date - 1 where sku_id = $1 and valid_to is null and valid_from < $2::date`, [id, today]);
  await pg.query(`delete from core.sku_costs where sku_id = $1 and valid_to is null`, [id]);
  await pg.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id)
    values (1, $1, $2, $3, $4, $5::date, '試験', 'human', 'naka@test')`, [id, row.jpy, row.source, row.status, row.from]);
}
await replaceOpen('sety', { jpy: 999, source: 'manual', status: 'OVERRIDDEN', from: today });
const future = (await one(`select ($1::date + 3)::text as d`, [today])).d;
await replaceOpen('setv', { jpy: 300, source: 'set_calc', status: 'COMPLETE', from: future });

let r1;
await ta('[L1] 構成を入れただけで導く値が今と同じ = 何も書かない (計算し直しの変更の記録なし)', async () => {
  r1 = await nightly(COMPS1);
  assert.equal(r1.set_derived.changed_parents, 5, JSON.stringify(r1.set_derived));
  assert.equal(r1.set_derived.candidates, 5);
  assert.equal(r1.set_derived.changed, 0, JSON.stringify(r1.set_derived));
  assert.equal(r1.set_derived.retry, 1, JSON.stringify(r1.set_derived));          // setv = 先の日付の原価 = 計算しない
  assert.deepEqual(r1.set_derived.reasons, { set_cost_future: 1 });
  for (const c of ['setx', 'sety', 'setz', 'setw', 'setv']) assert.deepEqual(await derivedEvents(c), [], c);
  assert.equal((await openCost('setx')).jpy, 600);
});

let r2;
await ta('[L2] NE で構成が変わった = commit の後に税率・取扱・原価を計算し直す (原価は今日から set_calc・前の行は昨日で閉じる)・変更の記録 = 夜間ロード・理由つき', async () => {
  await pg.query(`update core.skus set handling = 'discontinued' where code = 'pa3'`);   // 構成品 C は Company DB で止めた (取扱の持ち主 company)
  const before = await costsOf('setx');
  r2 = await nightly(COMPS2);
  assert.equal(r2.set_derived.changed, 2, JSON.stringify(r2.set_derived));
  assert.deepEqual(r2.set_derived.results.filter((x) => x.reason === 'recomputed').map((x) => x.code).sort(), ['setw', 'setx']);
  // setx: pa1 × 1 (10%) + pa2 × 1 (8%) = 税率は低い方の 8% (MIXED)・原価 300 + 500 = 800
  assert.deepEqual(await skuRow('setx'), { tax_rate: 0.08, tax_class: 'MIXED', handling: 'active' });
  const cx = await costsOf('setx');
  assert.deepEqual(cx.at(-1), { jpy: 800, src: 'set_calc', st: 'COMPLETE', f: today, t: null });
  assert.equal(cx.length, before.length + 1);
  assert.equal(cx.at(-2).jpy, 600);
  assert.equal(cx.at(-2).t, (await one(`select ($1::date - 1)::text as d`, [today])).d, '前の原価は昨日で閉じる');
  // setw: 構成品が止まった = セットも止める (セット自身の取扱は決まっていない)・原価 200
  assert.deepEqual(await skuRow('setw'), { tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'discontinued' });
  assert.equal((await openCost('setw')).jpy, 200);
  // 変更の記録: system・company_db_load・host・この回の run_id・理由
  const ev = await derivedEvents('setx');
  assert.ok(ev.length >= 3, JSON.stringify(ev));
  for (const e of ev) assert.deepEqual([e.actor_type, e.actor_id, e.source_system, e.run_id, e.reason_text], ['system', 'render-nightly', 'company_db_load', r2.run_id, W.LOAD_DERIVED_WHY]);
  assert.ok(ev.some((e) => e.entity_type === 'sku' && e.attribute === 'tax_class'));
  const reason = (await one(`select reason from core.sku_costs where sku_id = $1 and valid_to is null`, [await sid('setx')])).reason;
  assert.equal(reason, `構成品から計算 (${W.LOAD_DERIVED_WHY})`);
});

await ta('[L3] 人の値は残す: 例外原価は構成が変わっても上書きしない・先の日付の原価は計算しない・変わらないセットは書かない', async () => {
  assert.deepEqual(await openCost('sety'), { jpy: 999, src: 'manual', st: 'OVERRIDDEN', f: today, t: null });
  assert.deepEqual(await derivedEvents('sety'), []);
  assert.equal((await openCost('setv')).f, future);
  assert.deepEqual(await derivedEvents('setv'), []);
  assert.ok(r2.set_derived.results.some((x) => x.code === 'setv' && x.reason === 'set_cost_future'), JSON.stringify(r2.set_derived.results));
  assert.deepEqual(await derivedEvents('setz'), []);
  assert.equal((await openCost('setz')).jpy, 300);
});

await ta('[L4] 同じ材料でもう一度 = 何も書かない (前の 7 日に変えたセットも見る・記録は増えない)', async () => {
  const n0 = (await one(`select count(*)::int as n from events.master_change_events where reason_text = $1`, [W.LOAD_DERIVED_WHY])).n;
  assert.ok(n0 > 0);
  const r3 = await nightly(COMPS2);
  assert.equal(r3.set_derived.changed_parents, 0);
  assert.equal(r3.set_derived.candidates, 5, '前の 7 日に夜間ロードが構成を変えたセット');
  assert.equal(r3.set_derived.changed, 0, JSON.stringify(r3.set_derived));
  assert.equal(r3.set_derived.same, 4);
  assert.equal(r3.set_derived.retry, 1, 'setv (先の日付の原価) は毎晩もう一度見るだけ');
  // 計算し直しの記録は増えない (同じ材料の夜間ロード本体も構成・導く値を書かない)
  assert.equal((await one(`select count(*)::int as n from events.master_change_events where reason_text = $1`, [W.LOAD_DERIVED_WHY])).n, n0);
  assert.equal((await q(`select 1 from events.master_change_events where run_id = $1 and entity_type in ('sku', 'sku_cost', 'sku_component') and entity_id in (select sku_id from core.skus where sku_kind = 'set')`, [r3.run_id])).length, 0);
});

await ta('[L5] 守り: 対象が多すぎ = 1 つも計算し直さない・dry-run のロード = 計算し直さない・構成の持ち主が company = 呼ばない', async () => {
  const many = Array.from({ length: W.LOAD_DERIVED_MAX_SETS + 1 }, (_, i) => String(900000 + i));
  const g = await W.recomputeSetsAfterLoad(db, { changedSetIds: many, ownership: MASTER_OWNERSHIP, runId: 'x' });
  assert.ok(g.guarded, JSON.stringify(g));
  assert.equal(g.changed + g.same + g.skipped, 0);
  // dry-run のロード (構成を戻す) = 巻き戻す = 計算し直さない
  const rd = await runInitialLoad(db, planOf(COMPS1), { log: quiet, runId: 'load_sd_dry', ownership: MASTER_OWNERSHIP, dryRun: true });
  assert.equal(rd.ok, true, rd.error);
  assert.deepEqual(Object.keys(rd.set_derived), ['changed_parents']);
  assert.equal((await openCost('setx')).jpy, 800);
  // 構成の持ち主が company = 夜間ロードは構成を書かない = 計算し直しも呼ばない
  const ALL = { ...MASTER_OWNERSHIP, sku_components: 'company' };
  await T.W2.setActiveOwnershipInDb(pg, ALL);
  try {
    const rc = await runInitialLoad(db, planOf(COMPS1), { log: quiet, runId: 'load_sd_c', ownership: ALL });
    assert.equal(rc.ok, true, rc.error);
    assert.deepEqual(Object.keys(rc.set_derived), ['changed_parents']);
  } finally { await T.W2.setActiveOwnershipInDb(pg, MASTER_OWNERSHIP); }
  // 1 つのセットの答え: セットでない・構成が空・持ち主が load
  assert.equal((await W.recomputeSetAfterLoad(db, await sid('pa1'), { ownership: MASTER_OWNERSHIP })).reason, 'not_a_set');
  assert.equal((await W.recomputeSetAfterLoad(db, await sid('setx'), { ownership: { ...MASTER_OWNERSHIP, sku_costs: 'load' } })).reason, 'before_cutover');
});

// ── 後始末の道具 ──
const OPTS = { actor: 'naka@test', reason: '10/21 の構成のずれの後始末', codes: [] };
const run = (opts) => { const lines = []; return X.runCatchup(db, { ...OPTS, ...opts }, { log: (l) => lines.push(l) }).then((r) => ({ ...r, lines })); };
await ta('[C1] dry-run = 何も書かない・前から残ったずれ (古い合計・原価が空) を直す計画・例外原価は残す・計画のハッシュ', async () => {
  // 前から残ったずれ: setx の原価を古い構成の合計のまま (1,562 円) にする・setz の原価を空にする (NE で直接作られたセットの形)
  await pg.query(`update core.sku_costs set cost_jpy = 1562 where sku_id = $1 and valid_to is null`, [await sid('setx')]);
  await pg.query(`delete from core.sku_costs where sku_id = $1`, [await sid('setz')]);
  const n0 = (await one('select count(*)::int as n from events.master_change_events')).n;
  const r = await run({});
  assert.equal(r.exitCode, 0, r.lines.join('\n'));
  assert.equal((await one('select count(*)::int as n from events.master_change_events')).n, n0, 'dry-run は書かない');
  assert.deepEqual(r.plan.items.map((x) => [x.code, x.changes.map((c) => [c.col, c.from, c.to])]), [['setx', [['cost', 1562, 800]]], ['setz', [['cost', null, 300]]]]);
  assert.deepEqual(r.kept.map((k) => [k.code, k.exception_jpy, k.components_jpy]), [['sety', 999, 900]]);
  assert.deepEqual(r.excluded.map((x) => [x.code, x.reason]), [['setv', 'set_cost_future']]);
  assert.match(r.hash, /^[0-9a-f]{64}$/);
  assert.ok(r.lines.some((l) => l.includes('setx') ), r.lines.join('\n'));
  assert.ok(r.lines.some((l) => l.includes('1,562 円 → 800 円')), r.lines.join('\n'));
});

await ta('[C2] ハッシュが違う apply = 巻き戻す・同じ = 書く (人・理由・request_id)・2 回目は直すもの 0・もう一度 apply = 何も書かない', async () => {
  const d = await run({});
  const bad = await run({ apply: true, expectHash: 'f'.repeat(64) });
  assert.deepEqual([bad.exitCode, bad.outcome, bad.error.reason], [1, 'rolled_back', 'plan_mismatch']);
  assert.equal((await openCost('setx')).jpy, 1562);
  const ok = await run({ apply: true, expectHash: d.hash });
  assert.deepEqual([ok.exitCode, ok.outcome], [0, 'committed'], ok.lines.join('\n'));
  assert.deepEqual(await openCost('setx'), { jpy: 800, src: 'set_calc', st: 'COMPLETE', f: today, t: null });
  assert.deepEqual(await openCost('setz'), { jpy: 300, src: 'set_calc', st: 'COMPLETE', f: today, t: null });
  assert.equal((await openCost('sety')).jpy, 999, '例外原価はそのまま');
  const ev = await derivedEvents('setz', X.SOURCE_SYSTEM);
  assert.ok(ev.length >= 1);
  for (const e of ev) assert.deepEqual([e.actor_type, e.actor_id, e.request_id, e.reason_text], ['human', 'naka@test', ok.request_id, OPTS.reason]);
  // もう一度: 直すもの 0・apply は何も書かない
  const d2 = await run({});
  assert.deepEqual([d2.exitCode, d2.plan.items.length], [0, 0], d2.lines.join('\n'));
  const n0 = (await one('select count(*)::int as n from events.master_change_events')).n;
  const again = await run({ apply: true, expectHash: d2.hash });
  assert.deepEqual([again.exitCode, again.outcome], [0, 'nothing']);
  assert.equal((await one('select count(*)::int as n from events.master_change_events')).n, n0);
  // 夜間ロードもその後は何も書かない (道具の答え = 夜間ロードの計算と同じ)
  const rn = await nightly(COMPS2);
  assert.equal(rn.set_derived.changed, 0, JSON.stringify(rn.set_derived));
});

await ta('[C3] --code で絞る・セットでないコード / 無いコード = 断る・引数', async () => {
  await pg.query(`update core.sku_costs set cost_jpy = 1 where sku_id = $1 and valid_to is null`, [await sid('setx')]);
  await pg.query(`update core.sku_costs set cost_jpy = 2 where sku_id = $1 and valid_to is null`, [await sid('setw')]);
  const r = await run({ codes: ['SETX'] });
  assert.deepEqual(r.plan.items.map((x) => x.code), ['setx']);
  assert.equal(r.counts.sets, 1);
  const all = await run({});
  assert.deepEqual(all.plan.items.map((x) => x.code), ['setw', 'setx']);
  assert.notEqual(all.hash, r.hash);
  const bad = await run({ codes: ['pa1', 'nosuch'] });
  assert.equal(bad.exitCode, 1);
  assert.deepEqual(bad.problems, ['pa1: セットでない (single)', 'nosuch: Company DB に無い']);
  assert.throws(() => X.parseCatchupArgs(['--actor', 'a@b']), /--reason/);
  assert.throws(() => X.parseCatchupArgs(['--actor', 'a@b', '--reason', 'r', '--apply']), /--expect-hash/);
  assert.throws(() => X.parseCatchupArgs(['--actor', 'a@b', '--reason', 'r', '--apply', '--expect-hash', 'a'.repeat(64)]), /--yes/);
  assert.equal(X.parseCatchupArgs(['--actor', 'a@b', '--reason', 'r']).apply, false, '既定は dry-run');
});

console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
