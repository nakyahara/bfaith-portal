/**
 * test-once-variation-group.mjs — 一回きり scripts/company-db/once-variation-group-from-ne.mjs (NE で直接作った色違いのまとまり fuwear を Company DB のまとまりの記録にする)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 0067 の後) を今の本番の形にする: products.parent の DB の active = load (ほかは company)・
 * 0067 の後の夜間ロードが NE の代表 fuwear から札を作って子 3 つに親を付けた (帰属 load・予約なし)・子は NE で直接作った商品 = quarantined・NE のコードの台帳 (0041) は fuwear-BK の書き方。
 * product-hub = 一時の DATA_DIR の SQLite (本物の取り込み cdb-group-intake.js)。
 * 固定する契約:
 *   D dry-run: watcher (読むだけ) で計画 (札・軸 1 = カラー・選択肢 = NE のコードの末尾 -BK + 商品名の最後の【】・子 3 つ)・何も書かない・--names で名前を変えるとハッシュも変わる
 *   R 直さない: 札が無い / 子が無い / 予約が別を指す / もう軸がある / 選択肢名が重なる / NE のコードが無い = 理由を出して exit 1・apply も何も書かない
 *   A apply: ハッシュが違う = 巻き戻す (何も書かない) / 同じ = 予約 (source load)・軸・選択肢・子の選択肢・revision 1・知らせ 1 件 (DB の trigger が今の値と照らす)・
 *     親 (帰属 load) は変えない・products.parent の持ち主は load のまま・照合 ② の親の数え (ops.parent_raw_gate) は前と同じ (0)・2 回目 = もう直してある (何もしない)
 *   P product-hub: ボードの sweep と同じ取り込みでまとまりで 1 枚のカード (子 3 つ・選択肢 -BK/-ND/-WH)
 *   わざと壊す確かめ = once-variation-group-from-ne.mjs の確かめを 1 つずつ外して流すと、この試験のどれかが NG になる (PR の本文に結果)
 * 使い方: node scripts/test-once-variation-group.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'once-vg-'));
delete process.env.MASTER_EDIT_OPEN;
(await import('../apps/warehouse-mirror/db.js')).initMirrorDB();   // product-hub の取り込みが NE の写し (mirror) を読む

const { setupVariationDb, basePlan, skuOf } = await import('./fixtures/master-variation-db.mjs');
const X = await import('./company-db/once-variation-group-from-ne.mjs');
const O = await import('../lib/product-hub-outbox.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

const T = await setupVariationDb();
const { pg, db, q, one, asRole } = T;
// 今の本番の形: products.parent だけ load (夜間ロードが NE の代表から親を付ける)
await T.W2.setActiveOwnershipInDb(pg, { ...T.ALL_COMPANY, 'products.parent': 'load' });
const KIDS = [['fuwear-bk', 'ブラック'], ['fuwear-nd', 'ヌード'], ['fuwear-wh', 'ホワイト']];
const extra = KIDS.map(([c, n]) => skuOf(c, `機能性肌着の補修シート 薄地【${n}】`, { representativeCode: 'fuwear', representativeState: 'value', standardPriceJpy: 598 }));
const lr = await runInitialLoad(db, basePlan(extra, [{ code: 'fuwear', name: '機能性肌着の補修シート 薄地', childCodes: KIDS.map(([c]) => c), status: 'active' }]),
  { log: quiet, runId: 'load_once_vg', now: new Date(Date.now() - 2 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
// 照合 ② の NE のコードの台帳 (0041): 子は NE の書き方 fuwear-BK・代表 fuwear
const RUN2 = 'mc_20300111T000000000Z_0ce001';
await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-11T00:00:00Z', 0)`, [RUN2]);
const prev = await q('select code_norm, kind, state, ne_code from ops.master_ne_codes');
await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN2, entries: [
  ...prev.map((x) => ({ code_norm: x.code_norm, kind: x.kind, state: x.state, ne_code: x.ne_code, spellings: [x.ne_code] })),
  ...KIDS.map(([c]) => { const ne = `fuwear-${c.slice(7).toUpperCase()}`; return { code_norm: c, kind: 'product', state: 'ok', ne_code: ne, spellings: [ne] }; }),
  { code_norm: 'fuwear', kind: 'rep', state: 'ok', ne_code: 'fuwear', spellings: ['fuwear'] }] })]);
const TAG = (await one(`select product_id::text as id from core.products where display_code = 'fuwear'`)).id;
const state = async () => ({
  parents: (await q(`select s.code, pp.display_code as parent, p.parent_set_by as by, r.state from core.skus s join core.products p on p.product_id = s.product_id
      left join core.products pp on pp.product_id = p.parent_product_id left join ops.master_registrations r on r.sku_id = s.sku_id where s.code like 'fuwear%' order by s.code`)),
  res: (await q(`select code, source from ops.variation_group_codes where group_product_id = $1`, [TAG])),
  axes: (await q('select axis, name from core.variation_axes where group_product_id = $1', [TAG])),
  options: (await q('select code, name, sort from core.variation_options where group_product_id = $1 order by sort', [TAG])),
  choices: (await one('select count(*)::int as n from core.sku_variation_choices where group_product_id = $1', [TAG])).n,
  rev: (await one('select revision from ops.variation_group_revisions where group_product_id = $1', [TAG]))?.revision ?? null,
  events: (await one('select count(*)::int as n from ops.product_hub_outbox where group_product_id = $1', [TAG])).n,
});
const OPTS = { group: 'fuwear', axisName: 'カラー', names: {} };
const dry = (opts = OPTS) => asRole('watcher', () => X.runOnce(db, opts, { log: quiet }));
const apply = (hash, opts = OPTS) => X.runOnce(db, { ...opts, apply: true, expectHash: hash, actor: 'naka@test', reason: '試験' }, { log: quiet });
/** 照合 ② の親の数え (NE の観測 = 子 3 つの代表 fuwear・ほかの単品は代表なし / 今の値どおり) */
const parentCounts = async () => {
  const rows = (await q(`select s.code_norm, pp.display_code as rep from core.skus s join core.products p on p.product_id = s.product_id left join core.products pp on pp.product_id = p.parent_product_id where s.sku_kind = 'single'`))
    .map((r) => [r.code_norm, 'single', 'ok', r.rep ? r.rep.toLowerCase() : null, r.rep ?? null]);
  return (await one('select ops.parent_raw_gate(1, $1::jsonb, false) as r', [JSON.stringify({ format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' }, rows })])).r.counts;
};

console.log('前提 (今の本番の形)');
await ta('[0] 夜間ロード (products.parent = load) が札 fuwear を作り、子 3 つ (quarantined) に親を付けた・予約なし・軸なし・知らせなし', async () => {
  const s = await state();
  assert.deepEqual(s.parents.map((r) => [r.code, r.parent, r.by, r.state]), KIDS.map(([c]) => [c, 'fuwear', 'load', 'quarantined']));
  assert.deepEqual([s.res, s.axes, s.options, s.choices, s.rev, s.events], [[], [], [], 0, null, 0]);
});

console.log('dry-run');
let HASH = null;
await ta('[D1] watcher で読むだけ: 札・軸 1 = カラー・選択肢 -BK ブラック / -ND ヌード / -WH ホワイト (NE の書き方の末尾 + 商品名の最後の【】)・何も書かない', async () => {
  const before = await state();
  const r = await dry();
  assert.equal(r.exitCode, 0, JSON.stringify(r.problems));
  assert.deepEqual(r.problems, []);
  assert.equal(r.plan.group.code, 'fuwear');
  assert.equal(r.plan.group.reserved, null);
  assert.deepEqual(r.plan.options, [{ code: '-BK', name: 'ブラック', sort: 0 }, { code: '-ND', name: 'ヌード', sort: 1 }, { code: '-WH', name: 'ホワイト', sort: 2 }]);
  assert.deepEqual(r.plan.children.map((c) => [c.code, c.ne_code, c.option, c.reg, c.parent_set_by]),
    [['fuwear-bk', 'fuwear-BK', '-BK', 'quarantined', 'load'], ['fuwear-nd', 'fuwear-ND', '-ND', 'quarantined', 'load'], ['fuwear-wh', 'fuwear-WH', '-WH', 'quarantined', 'load']]);
  assert.match(r.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(await state(), before);
  HASH = r.hash;
  const lines = X.planLines(r).join('\n');
  assert.match(lines, /子 fuwear-bk \(NE fuwear-BK.*→ 選択肢 -BK「ブラック」/);
});
await ta('[D2] --names で名前を変える = ハッシュも変わる・知らない番号 = 直さない', async () => {
  const r = await dry({ ...OPTS, names: { '-ND': 'ベージュ' } });
  assert.equal(r.exitCode, 0);
  assert.equal(r.plan.options[1].name, 'ベージュ');
  assert.notEqual(r.hash, HASH);
  const r2 = await dry({ ...OPTS, names: { '-XX': '何か' } });
  assert.equal(r2.exitCode, 1);
  assert.ok(r2.problems.some((p) => /-XX に当たる子が無い/.test(p)));
  const r3 = await dry({ ...OPTS, names: { '-ND': 'ブラック' } });
  assert.ok(r3.problems.some((p) => /選択肢名 ブラック が 2 つの子/.test(p)));
});
await ta('[D3] 引数: --group が要る・--apply は --expect-hash・--actor・--yes が要る・--names の形', async () => {
  assert.throws(() => X.parseOnceArgs([]), /--group/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear', '--apply', '--actor', 'a@b', '--yes']), /--expect-hash/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear', '--apply', '--expect-hash', 'a'.repeat(64), '--yes']), /--actor/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear', '--apply', '--expect-hash', 'a'.repeat(64), '--actor', 'a@b']), /--yes/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear', '--names', 'BK=黒']), /選択肢番号/);
  const o = X.parseOnceArgs(['--group', 'fuwear', '--names', '-BK=黒,-WH=白', '--axis-name', '色']);
  assert.deepEqual([o.group, o.axisName, o.names, o.apply], ['fuwear', '色', { '-BK': '黒', '-WH': '白' }, false]);
  assert.equal(X.lastBracket('A【赤】【100サイズ】'), '100サイズ');
  assert.equal(X.lastBracket('名前だけ'), null);
});
await ta('[R1] 直さない: 札が無いコード・SKU のコード (単品の代表)・NE のコードが無いまとまり = 理由つき exit 1', async () => {
  const r = await dry({ ...OPTS, group: 'nothere' });
  assert.equal(r.exitCode, 1);
  assert.ok(r.problems.some((p) => /札\) が 0 件/.test(p)));
  const r2 = await dry({ ...OPTS, group: 'towel-gift' });
  assert.equal(r2.exitCode, 1);
  assert.ok(r2.problems.some((p) => /SKU のコード/.test(p)));
  // ws100 (0067 の前の札) は予約済み・NE のコードの台帳の子の書き方はある = 札としては直せる形 (この道の対象を fuwear に絞るのは人) → 軸を付けた後は直さない は [A3] で
});

console.log('apply');
await ta('[A1] ハッシュが違う = 巻き戻す (何も書かない・exit 1)', async () => {
  const before = await state();
  const r = await apply('0'.repeat(64));
  assert.equal(r.exitCode, 1);
  assert.equal(r.error.reason, 'plan_mismatch');
  assert.deepEqual(await state(), before);
});
let EVENT = null;
await ta('[A2] 同じハッシュ = 予約 (load)・軸・選択肢・子の選択肢・revision 1・知らせ 1 件・親 (帰属 load) と持ち主 (load) は変えない・親の数えは前と同じ', async () => {
  const c0 = await parentCounts();
  const r = await apply(HASH);
  assert.equal(r.exitCode, 0, JSON.stringify(r.error));
  EVENT = r.event_id;
  const s = await state();
  assert.deepEqual(s.parents.map((x) => [x.code, x.parent, x.by, x.state]), KIDS.map(([c]) => [c, 'fuwear', 'load', 'quarantined']));
  assert.deepEqual(s.res, [{ code: 'fuwear', source: 'load' }]);
  assert.deepEqual(s.axes, [{ axis: 1, name: 'カラー' }]);
  assert.deepEqual(s.options.map((o) => [o.code, o.name, o.sort]), [['-BK', 'ブラック', 0], ['-ND', 'ヌード', 1], ['-WH', 'ホワイト', 2]]);
  assert.deepEqual([s.choices, s.rev, s.events], [3, 1, 1]);
  const ev = await one('select payload, request_id::text as rid, created_by, status from ops.product_hub_outbox where event_id = $1', [EVENT]);
  assert.deepEqual([ev.created_by, ev.status, ev.rid], ['naka@test', 'pending', r.request_id]);
  assert.deepEqual(ev.payload.group, { product_id: TAG, sku_id: null, code: 'fuwear', name: '機能性肌着の補修シート 薄地', kind: 'tag' });
  assert.deepEqual(ev.payload.axes, [{ axis: 1, name: 'カラー' }]);
  assert.deepEqual(ev.payload.children.map((c) => [c.code, c.choices]), KIDS.map(([c]) => [c, { 1: `-${c.slice(7).toUpperCase()}` }]));
  assert.equal((await one(`select active_map ->> 'products.parent' as o from ops.master_ownership_state where id = 1`)).o, 'load');
  assert.deepEqual(await parentCounts(), c0);
  // 変更の記録 (0026): 誰が・理由・どこから
  const au = await q(`select distinct actor_type, actor_id, source_system, reason_text from events.master_change_events where entity_type in ('variation_axis', 'variation_option', 'sku_variation_choice')
     and request_id = $1`, [r.request_id]);
  assert.deepEqual(au, [{ actor_type: 'human', actor_id: 'naka@test', source_system: 'once_variation_group', reason_text: '試験' }]);
});
await ta('[A3] 2 回目 = もう直してある (dry-run も apply も何も書かない・exit 1)', async () => {
  const before = await state();
  const r = await dry();
  assert.equal(r.exitCode, 1);
  assert.ok(r.problems.some((p) => /もう軸 1/.test(p)));
  const r2 = await apply(HASH);
  assert.equal(r2.exitCode, 1);
  assert.deepEqual(await state(), before);
});
await ta('[A4] 夜間ロード (products.parent = load) をもう一度流しても、親・予約・軸・選択肢・revision は変わらない', async () => {
  const before = await state();
  const r = await runInitialLoad(db, basePlan(extra, [{ code: 'fuwear', name: '機能性肌着の補修シート 薄地', childCodes: KIDS.map(([c]) => c), status: 'active' }]),
    { log: quiet, runId: 'load_once_vg2', now: new Date(Date.now() - 86400e3) });
  assert.equal(r.ok, true, r.error);
  assert.deepEqual(await state(), before);
});

await ta('[A5] 切替 (widen) の後 (products.parent = company) でも同じに動く: 0067 で予約した札 ws100 (予約はそのまま・source load)', async () => {
  await T.W2.setActiveOwnershipInDb(pg, T.ALL_COMPANY);
  try {
    const o = { group: 'ws100', axisName: 'カラー', names: {} };
    const d = await dry(o);
    assert.equal(d.exitCode, 0, JSON.stringify(d.problems));
    assert.equal(d.plan.group.reserved, 'load');
    assert.deepEqual(d.plan.options.map((x) => [x.code, x.name]), [['-BR', 'ブラウン'], ['-GY', 'グレー'], ['-NV', 'ネイビー']]);
    const r = await apply(d.hash, o);
    assert.equal(r.exitCode, 0, JSON.stringify(r.error));
    const gid = d.plan.group.product_id;
    assert.deepEqual(await q('select code, source from ops.variation_group_codes where group_product_id = $1', [gid]), [{ code: 'ws100', source: 'load' }]);
    assert.equal((await one('select revision from ops.variation_group_revisions where group_product_id = $1', [gid])).revision, 1);
  } finally { await T.W2.setActiveOwnershipInDb(pg, { ...T.ALL_COMPANY, 'products.parent': 'load' }); }
});

console.log('product-hub');
await ta('[P1] ボードの sweep と同じ取り込み = まとまりで 1 枚のカード (子 3 つ・選択肢)・知らせは done', async () => {
  const PG = await import('../apps/product-hub/services/cdb-group-intake.js');
  const PHDB = await import('../apps/product-hub/db.js');
  const res = await asRole('master_edit', () => O.runGroupOutbox(db, (ev) => PG.applyCdbGroupEvent(ev), { eventId: EVENT, limit: 1 }));
  assert.equal(res.length, 1);
  assert.equal(res[0].status, 'done', res[0].error);
  assert.equal(res[0].result.outcome, 'created');
  const ph = PHDB.getDB();
  const d = ph.prepare('SELECT id, ne_code, cdb_group_product_id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').all('fuwear');
  assert.equal(d.length, 1);
  assert.equal(String(d[0].cdb_group_product_id), TAG);
  const kids = ph.prepare('SELECT code, active, choice1 FROM ph_cdb_group_children WHERE draft_id = ? ORDER BY code_key').all(d[0].id);
  assert.deepEqual(kids.map((k) => [k.code, k.active, k.choice1]), KIDS.map(([c]) => [c, 1, `-${c.slice(7).toUpperCase()}`]));
});

console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
