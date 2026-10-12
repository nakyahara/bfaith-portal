/**
 * test-once-variation-group.mjs — 一回きり scripts/company-db/once-variation-group-from-ne.mjs (NE で直接作った色違いのまとまり fuwear を Company DB のまとまりの記録にする)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 0067 の後) を今の本番の形にする: products.parent の DB の active = load (ほかは company)・
 * 0067 の後の夜間ロードが NE の代表 fuwear / smhc から札を作って子に親を付けた (帰属 load・予約なし)・子は NE で直接作った商品 = quarantined・
 * NE のコードの台帳 (0041) は fuwear-BK の書き方。NE の代表の観測は **Company DB の親から作らず**、NE の材料 (この試験の neObs) から作って
 * 照合 ② の記録 (ops.record_parent_gate = 封をした回) に入れる。product-hub = 一時の DATA_DIR の SQLite (本物の取り込み cdb-group-intake.js)。
 * 固定する契約 (#1693 Codex R1 の High 2 / Medium 3 / Low 1 を含む):
 *   G 切替の前 (products.parent = load) = 「切替の後に流す」で止まる (表は出す・apply も何も書かない)
 *   N NE の代表: 最新の封をした照合の回の観測と同じ観測だけ使う (封の無い / 違う観測 = 止まる)・36 時間より古い = 止まる・子の NE の代表が違う = 止まる (親の数えのずれも)・
 *     信用できない観測 = 止まる
 *   D dry-run: 計画 (札・軸 1・選択肢 = NE のコードの末尾 + 商品名の最後の【】・子)・何も書かない・--names でハッシュが変わる・引数 (許可の一覧・人)
 *   R 直さない: 許可の一覧の札が無い・子が quarantined でない
 *   A apply: ハッシュが違う = 巻き戻す / 同じ = 予約 (load)・軸・選択肢・子の選択肢・revision 1・知らせ 1 件 (中身の全部が計画と同じ)・保存の記録 (request_id)・
 *     親 (帰属 load) は変えない・親の数えは前と同じ・2 回目 = 何もしない・夜間ロードをもう一度流しても同じ
 *   C commit の答えが分からない = 新しい接続で request_id を読んで「書けていない / 分からない / 書けた」を言う (巻き戻したと言わない)
 *   P product-hub: まとまりで 1 枚のカード (結ぶ = linked)・ほかに結ばれた同じコードのカード = conflict (二重にしない)
 *   わざと壊す確かめ = once-variation-group-from-ne.mjs の確かめを 1 つずつ外して流すと、この試験のどれかが NG になる (PR の本文に結果)
 * 使い方: node scripts/test-once-variation-group.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'once-vg-'));
delete process.env.MASTER_EDIT_OPEN;
(await import('../apps/warehouse-mirror/db.js')).initMirrorDB();   // product-hub の取り込みが NE の写し (mirror) を読む

const { setupVariationDb, basePlan, skuOf } = await import('./fixtures/master-variation-db.mjs');
const { seedParentGate } = await import('./fixtures/master-widen.mjs');
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
const SMHC = [['smhc-bkg', 'ブラックG'], ['smhc-bks', 'ブラックS']];
// サンリオのトレパン: NE の代表の書き方 = sanrio3souPK (大文字混じり)・夜間ロードの札 = sanrio3soupk (本番と同じ)。sku_id の順 = 100 → 90 → 95
const SAN = [['sanrio3soupk-100', '100サイズ'], ['sanrio3soupk-90', '90サイズ'], ['sanrio3soupk-95', '95サイズ']];
const SAN_REP_NE = 'sanrio3souPK';
const extra = [...KIDS.map(([c, n]) => skuOf(c, `機能性肌着の補修シート 薄地【${n}】`, { representativeCode: 'fuwear', representativeState: 'value', standardPriceJpy: 598 })),
  ...SMHC.map(([c, n]) => skuOf(c, `ショルダー【${n}】`, { representativeCode: 'smhc', representativeState: 'value' })),
  ...SAN.map(([c, n]) => skuOf(c, `トレパン 3層【マイメロ・キティ】【${n}】`, { representativeCode: 'sanrio3soupk', representativeState: 'value' }))];
const GROUPS = [{ code: 'fuwear', name: '機能性肌着の補修シート 薄地', childCodes: KIDS.map(([c]) => c), status: 'active' },
  { code: 'smhc', name: 'ショルダー', childCodes: SMHC.map(([c]) => c), status: 'active' },
  { code: 'sanrio3soupk', name: 'トレパン 3層', childCodes: SAN.map(([c]) => c), status: 'active' }];
const lr = await runInitialLoad(db, basePlan(extra, GROUPS), { log: quiet, runId: 'load_once_vg', now: new Date(Date.now() - 2 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
// 照合 ② の NE のコードの台帳 (0041): 子は NE の書き方 fuwear-BK・代表 fuwear / smhc
const RUN2 = 'mc_20300111T000000000Z_0ce001';
await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-11T00:00:00Z', 0)`, [RUN2]);
const prev = await q('select code_norm, kind, state, ne_code from ops.master_ne_codes');
const neSpell = (c) => (c.startsWith('fuwear-') ? `fuwear-${c.slice(7).toUpperCase()}` : c);
await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN2, entries: [
  ...prev.map((x) => ({ code_norm: x.code_norm, kind: x.kind, state: x.state, ne_code: x.ne_code, spellings: [x.ne_code] })),
  ...[...KIDS, ...SMHC, ...SAN].map(([c]) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: neSpell(c), spellings: [neSpell(c)] })),
  { code_norm: 'fuwear', kind: 'rep', state: 'ok', ne_code: 'fuwear', spellings: ['fuwear'] }, { code_norm: 'smhc', kind: 'rep', state: 'ok', ne_code: 'smhc', spellings: ['smhc'] },
  { code_norm: 'sanrio3soupk', kind: 'rep', state: 'ok', ne_code: SAN_REP_NE, spellings: [SAN_REP_NE] }] })]);
const SAN_TAG = (await one(`select product_id::text as id from core.products where display_code = 'sanrio3soupk'`)).id;
const TAG = (await one(`select product_id::text as id from core.products where display_code = 'fuwear'`)).id;
const SMHC_TAG = (await one(`select product_id::text as id from core.products where display_code = 'smhc'`)).id;

/** NE の観測 (Company DB の親を読まない = NE の材料 = 夜間ロードの材料の代表から)。over = { 子のコード: 代表 } で NE の値を変える */
function neObs(over = {}, { complete = true } = {}) {
  const plan = basePlan(extra, GROUPS);
  const rows = plan.skus.map((s) => {
    const rep0 = Object.hasOwn(over, s.code) ? over[s.code] : (s.representativeCode ?? null);
    const rep = rep0 === 'sanrio3soupk' ? SAN_REP_NE : rep0;   // NE の元の書き方
    return [s.code.toLowerCase(), 'single', 'ok', rep ? rep.toLowerCase() : null, rep];
  }).sort((a, b) => (a[0] < b[0] ? -1 : 1));
  return { format: 'parent-obs-v1', complete, ...(complete ? {} : { incomplete_reason: 'fetch_rows_dropped:products' }), untrusted: [], rep_collided: [], rep_spellings: { state: 'ok' },
    trust: { fetch_counts: complete ? 'ok' : 'fetch_rows_dropped:products', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' }, rows };
}
let sealN = 0;
/** 照合 ② が封をした回の代表の数えの記録 (ops.record_parent_gate) = この観測 */
const seal = (obs) => seedParentGate(db, { runId: `mc_once_${++sealN}`, obs });
const NE = { obs: neObs() };
const state = async (gid = TAG, like = 'fuwear%') => ({
  parents: (await q(`select s.code, pp.display_code as parent, p.parent_set_by as by, r.state from core.skus s join core.products p on p.product_id = s.product_id
      left join core.products pp on pp.product_id = p.parent_product_id left join ops.master_registrations r on r.sku_id = s.sku_id where s.code like $1 order by s.code`, [like])),
  res: (await q(`select code, source from ops.variation_group_codes where group_product_id = $1`, [gid])),
  axes: (await q('select axis, name from core.variation_axes where group_product_id = $1', [gid])),
  options: (await q('select code, name, sort from core.variation_options where group_product_id = $1 order by sort', [gid])),
  choices: (await one('select count(*)::int as n from core.sku_variation_choices where group_product_id = $1', [gid])).n,
  rev: (await one('select revision from ops.variation_group_revisions where group_product_id = $1', [gid]))?.revision ?? null,
  events: (await one('select count(*)::int as n from ops.product_hub_outbox where group_product_id = $1', [gid])).n,
});
const ACTOR = 'naka@test';
const OPTS = { group: 'fuwear', axisName: 'カラー', names: {}, actor: ACTOR, reason: '試験' };
const dry = (opts = OPTS, ne = NE, now = new Date()) => asRole('watcher', () => X.runOnce(db, opts, { ne, now, log: quiet }));
const apply = (hash, opts = OPTS, extraOpts = {}) => X.runOnce(extraOpts.db || db, { ...opts, apply: true, expectHash: hash }, { ne: NE, log: quiet, ...extraOpts });
/** 照合 ② の親の数え (NE の観測 = Company DB の親から作らない) */
const parentCounts = async () => (await one('select ops.parent_raw_gate(1, $1::jsonb, false) as r', [JSON.stringify(NE.obs)])).r.counts;

console.log('前提 (今の本番の形)');
await ta('[0] 夜間ロード (products.parent = load) が札 fuwear を作り、子 3 つ (quarantined) に親を付けた・予約なし・軸なし・知らせなし', async () => {
  const s = await state();
  assert.deepEqual(s.parents.map((r) => [r.code, r.parent, r.by, r.state]), KIDS.map(([c]) => [c, 'fuwear', 'load', 'quarantined']));
  assert.deepEqual([s.res, s.axes, s.options, s.choices, s.rev, s.events], [[], [], [], 0, null, 0]);
});

console.log('切替の前');
await ta('[G1] products.parent が load = 「切替の後に流す」で止まる (表は出す・exit 1)・apply も何も書かない', async () => {
  await seal(NE.obs);
  const before = await state();
  const r = await dry();
  assert.equal(r.exitCode, 1);
  assert.ok(r.problems.some((p) => /^parent_not_company: .*切替 \(widen\) の後に流す/.test(p)), JSON.stringify(r.problems));
  assert.equal(r.problems.length, 1, JSON.stringify(r.problems));
  assert.deepEqual(r.plan.options.map((o) => [o.code, o.name]), [['-BK', 'ブラック'], ['-ND', 'ヌード'], ['-WH', 'ホワイト']]);
  const lines = X.planLines(r).join('\n');
  assert.match(lines, /代表 \(products.parent\) の持ち主 load/);
  assert.match(lines, /⛔ parent_not_company/);
  const a = await apply(r.hash);
  assert.equal(a.exitCode, 1);
  assert.equal(a.outcome, 'rolled_back');
  assert.deepEqual(await state(), before);
});

// 切替 (widen) の後の形
await T.W2.setActiveOwnershipInDb(pg, T.ALL_COMPANY);

console.log('NE の代表の根拠');
await ta('[N1] 封をした照合の回の観測と違う NE の観測 (新しい取得・封の無い観測) = 止まる', async () => {
  const r = await dry(OPTS, { obs: neObs({ 's001': 'x1' }) });
  assert.equal(r.exitCode, 1);
  assert.ok(r.problems.some((p) => /最新の封をした照合の回 .* の観測と違う/.test(p)), JSON.stringify(r.problems));
});
await ta('[N2] 子の NE の代表が --group と違う (封をした回の観測で) = 止まる・親の数えのずれも出す', async () => {
  const bad = neObs({ 'fuwear-nd': 'other' });
  await seal(bad);
  const r = await dry(OPTS, { obs: bad });
  assert.equal(r.exitCode, 1);
  assert.ok(r.problems.some((p) => /子 fuwear-nd の NE の代表が other/.test(p)), JSON.stringify(r.problems));
  assert.ok(r.problems.some((p) => /子 fuwear-nd の代表のずれ: parent_mismatch/.test(p)), JSON.stringify(r.problems));
  await seal(NE.obs);   // 正しい観測の回を最新に
});
await ta('[N3] 封をした回が 36 時間より古い = 止まる / 信用できない観測 (complete でない) = 止まる', async () => {
  const r = await dry(OPTS, NE, new Date(Date.now() + 37 * 3600e3));
  assert.ok(r.problems.some((p) => /36 時間より古い/.test(p)), JSON.stringify(r.problems));
  const r2 = await dry(OPTS, { obs: neObs({}, { complete: false }) });
  assert.ok(r2.problems.some((p) => /NE の取得を確かめられない/.test(p)), JSON.stringify(r2.problems));
  const r3 = await dry(OPTS, { obs: null });
  assert.ok(r3.problems.some((p) => /NE の観測 .* を読めない/.test(p)));
});

console.log('dry-run');
let HASH = null;
await ta('[D1] watcher で読むだけ: 札・軸 1 = カラー・選択肢 -BK ブラック / -ND ヌード / -WH ホワイト・封をした回・何も書かない', async () => {
  const before = await state();
  const r = await dry();
  assert.equal(r.exitCode, 0, JSON.stringify(r.problems));
  assert.deepEqual(r.problems, []);
  assert.equal(r.plan.group.code, 'fuwear');
  assert.equal(r.plan.group.reserved, null);
  assert.deepEqual(r.plan.options, [{ code: '-BK', name: 'ブラック', sort: 0 }, { code: '-ND', name: 'ヌード', sort: 1 }, { code: '-WH', name: 'ホワイト', sort: 2 }]);
  assert.deepEqual(r.plan.children.map((c) => [c.code, c.ne_code, c.option, c.reg, c.parent_set_by]),
    [['fuwear-bk', 'fuwear-BK', '-BK', 'quarantined', 'load'], ['fuwear-nd', 'fuwear-ND', '-ND', 'quarantined', 'load'], ['fuwear-wh', 'fuwear-WH', '-WH', 'quarantined', 'load']]);
  assert.equal(r.ne.compare_run_id, `mc_once_${sealN}`);
  assert.deepEqual(r.expected.children.map((c) => [c.code, c.name, c.price, c.choices]), KIDS.map(([c, n]) => [c, `機能性肌着の補修シート 薄地【${n}】`, 598, { 1: `-${c.slice(7).toUpperCase()}` }]));
  assert.match(r.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(await state(), before);
  HASH = r.hash;
  const lines = X.planLines(r).join('\n');
  assert.match(lines, /子 fuwear-bk \(NE fuwear-BK.*→ 選択肢 -BK「ブラック」/);
  assert.match(lines, /封をした照合 ② の回 mc_once_/);
});
await ta('[D2] --names で名前を変える・人が違う = ハッシュも変わる・知らない番号 / 名前の重なり = 直さない', async () => {
  const r = await dry({ ...OPTS, names: { '-ND': 'ベージュ' } });
  assert.equal(r.exitCode, 0);
  assert.equal(r.plan.options[1].name, 'ベージュ');
  assert.notEqual(r.hash, HASH);
  assert.notEqual((await dry({ ...OPTS, actor: 'other@test' })).hash, HASH);
  const r2 = await dry({ ...OPTS, names: { '-XX': '何か' } });
  assert.equal(r2.exitCode, 1);
  assert.ok(r2.problems.some((p) => /-XX に当たる子が無い/.test(p)));
  const r3 = await dry({ ...OPTS, names: { '-ND': 'ブラック' } });
  assert.ok(r3.problems.some((p) => /選択肢名 ブラック が 2 つの子/.test(p)));
});
await ta('[D3] 引数: 許可の一覧 (fuwear・sanrio3soupk・smhc) だけ・--actor が要る・--apply は --expect-hash・--yes が要る・--names の形', async () => {
  assert.throws(() => X.parseOnceArgs(['--actor', 'a@b']), /許可の一覧/);
  assert.throws(() => X.parseOnceArgs(['--group', 'ws100', '--actor', 'a@b']), /許可の一覧/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear']), /--actor/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear', '--actor', 'a@b', '--apply', '--yes']), /--expect-hash/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear', '--actor', 'a@b', '--apply', '--expect-hash', 'a'.repeat(64)]), /--yes/);
  assert.throws(() => X.parseOnceArgs(['--group', 'fuwear', '--actor', 'a@b', '--names', 'BK=黒']), /選択肢番号/);
  const o = X.parseOnceArgs(['--group', 'fuwear', '--actor', 'a@b', '--names', '-BK=黒,-WH=白', '--axis-name', '色']);
  assert.deepEqual([o.group, o.axisName, o.names, o.apply, o.actor], ['fuwear', '色', { '-BK': '黒', '-WH': '白' }, false, 'a@b']);
  assert.equal(X.lastBracket('A【赤】【100サイズ】'), '100サイズ');
  assert.equal(X.lastBracket('名前だけ'), null);
  const r = await dry({ ...OPTS, group: 'ws100' });
  assert.ok(r.problems.some((p) => /は許可の一覧 (.*) に無い/.test(p)));
});
await ta('[R1] 直さない: 札が無い・子が quarantined でない', async () => {
  const noTag = { query: (sql, params) => (/from core\.products p where p\.company_id = 1 and core\.norm_code\(p\.display_code\) = \$1/.test(sql) ? Promise.resolve({ rows: [] }) : db.query(sql, params)) };
  const r = await X.readOncePlan(noTag, { ...OPTS, group: 'sanrio3soupk' }, { ne: NE });
  assert.ok(r.problems.some((p) => /札\) が 0 件/.test(p)));
  // 子の 1 つが quarantined でない (登録の状態だけ試験で変える = 取引の中で巻き戻す)
  await pg.query('begin');
  try {
    await pg.query(`alter table ops.master_registrations disable trigger user`);
    await pg.query(`update ops.master_registrations set state = 'available' where sku_id = (select sku_id from core.skus where code = 'fuwear-wh')`);
    const r2 = await X.readOncePlan(db, OPTS, { ne: NE });
    assert.ok(r2.problems.some((p) => /子 fuwear-wh の登録の状態が available/.test(p)), JSON.stringify(r2.problems));
  } finally { await pg.query('rollback'); }
});
await ta('[R2] 直さない: 子の親の帰属が load でない (人が付けた親)・封をした照合の回が 1 つも無い', async () => {
  await pg.query('begin');
  try {
    await pg.query(`alter table core.products disable trigger user`);
    await pg.query(`update core.products set parent_set_by = 'manual' where product_id = (select product_id from core.skus where code = 'fuwear-bk')`);
    const r = await X.readOncePlan(db, OPTS, { ne: NE });
    assert.ok(r.problems.some((p) => /子 fuwear-bk の親の帰属が manual/.test(p)), JSON.stringify(r.problems));
  } finally { await pg.query('rollback'); }
  // 代表の数えの記録 (封をした回) を読めない DB = 記録の表の答えを空にした接続
  const noSeal = { query: (sql, params) => (/from ops\.master_parent_gate_results/.test(sql) ? Promise.resolve({ rows: [] }) : db.query(sql, params)) };
  const r2 = await X.readOncePlan(noSeal, OPTS, { ne: NE });
  assert.ok(r2.problems.some((p) => /封をした照合 ② の回 .* が無い/.test(p)), JSON.stringify(r2.problems));
});

console.log('apply');
await ta('[A1] ハッシュが違う = 巻き戻す (何も書かない・exit 1)', async () => {
  const before = await state();
  const r = await apply('0'.repeat(64));
  assert.equal(r.exitCode, 1);
  assert.equal(r.outcome, 'rolled_back');
  assert.equal(r.error.reason, 'plan_mismatch');
  assert.deepEqual(await state(), before);
});
let EVENT = null;
await ta('[A2] 同じハッシュ = 予約 (load)・軸・選択肢・子の選択肢・revision 1・知らせ 1 件 (中身 = 計画)・保存の記録・親 (帰属 load) は変えない・親の数えは前と同じ', async () => {
  const c0 = await parentCounts();
  const d = await dry();
  const r = await apply(HASH);
  assert.equal(r.exitCode, 0, JSON.stringify(r.error));
  assert.equal(r.outcome, 'committed');
  EVENT = r.event_id;
  const s = await state();
  assert.deepEqual(s.parents.map((x) => [x.code, x.parent, x.by, x.state]), KIDS.map(([c]) => [c, 'fuwear', 'load', 'quarantined']));
  assert.deepEqual(s.res, [{ code: 'fuwear', source: 'load' }]);
  assert.deepEqual(s.axes, [{ axis: 1, name: 'カラー' }]);
  assert.deepEqual(s.options.map((o) => [o.code, o.name, o.sort]), [['-BK', 'ブラック', 0], ['-ND', 'ヌード', 1], ['-WH', 'ホワイト', 2]]);
  assert.deepEqual([s.choices, s.rev, s.events], [3, 1, 1]);
  const ev = await one('select payload, request_id::text as rid, created_by, status from ops.product_hub_outbox where event_id = $1', [EVENT]);
  assert.deepEqual([ev.created_by, ev.status, ev.rid], [ACTOR, 'pending', r.request_id]);
  assert.equal(X.stable(ev.payload), X.stable(d.expected));
  const m = await one('select operation, status, actor_id, payload_hash from ops.master_edit_requests where request_id = $1', [r.request_id]);
  assert.deepEqual(m, { operation: 'variation_label_edit', status: 'done', actor_id: ACTOR, payload_hash: HASH });
  assert.deepEqual(await parentCounts(), c0);
  const au = await q(`select distinct actor_type, actor_id, source_system, reason_text from events.master_change_events where entity_type in ('variation_axis', 'variation_option', 'sku_variation_choice')
     and request_id = $1`, [r.request_id]);
  assert.deepEqual(au, [{ actor_type: 'human', actor_id: ACTOR, source_system: 'once_variation_group', reason_text: '試験' }]);
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
await ta('[A4] 夜間ロードをもう一度流しても、親・予約・軸・選択肢・revision は変わらない', async () => {
  const before = await state();
  const r = await runInitialLoad(db, basePlan(extra, GROUPS), { log: quiet, runId: 'load_once_vg2', now: new Date(Date.now() - 86400e3) });
  assert.equal(r.ok, true, r.error);
  assert.deepEqual(await state(), before);
});

await ta('[S1] NE の代表の書き方 sanrio3souPK と札 sanrio3soupk = 大文字小文字をそろえて同じ = 通す・軸 サイズ・選択肢は -90 / -95 / -100 の順 (名前は最後の【】)・書ける', async () => {
  const o = { ...OPTS, group: 'sanrio3soupk', axisName: 'サイズ' };
  const d = await dry(o);
  assert.equal(d.exitCode, 0, JSON.stringify(d.problems));
  assert.deepEqual(d.plan.options, [{ code: '-90', name: '90サイズ', sort: 0 }, { code: '-95', name: '95サイズ', sort: 1 }, { code: '-100', name: '100サイズ', sort: 2 }]);
  assert.equal(d.plan.group.code, 'sanrio3soupk');
  const r = await apply(d.hash, o);
  assert.equal(r.exitCode, 0, JSON.stringify(r.error));
  const s = await state(SAN_TAG, 'sanrio3soupk%');
  assert.deepEqual(s.parents.map((x) => [x.parent, x.by]), SAN.map(() => ['sanrio3soupk', 'load']));
  assert.deepEqual([s.axes, s.options.map((x) => x.code), s.choices, s.rev, s.events], [[{ axis: 1, name: 'サイズ' }], ['-90', '-95', '-100'], 3, 1, 1]);
  assert.equal(X.stable((await one('select payload from ops.product_hub_outbox where group_product_id = $1', [SAN_TAG])).payload), X.stable(d.expected));
});
await ta('[S2] 大文字小文字をそろえても違う NE の代表 (sanrio3soupx) = 止まる', async () => {
  const bad = neObs(Object.fromEntries(SAN.map(([c]) => [c, 'sanrio3soupX'])));
  await seal(bad);
  try {
    const r = await dry({ ...OPTS, group: 'sanrio3soupk', axisName: 'サイズ' }, { obs: bad });
    assert.ok(r.problems.some((p) => /子 sanrio3soupk-90 の NE の代表が sanrio3soupX/.test(p)), JSON.stringify(r.problems));
  } finally { await seal(NE.obs); }
});

console.log('commit の答えが分からない');
const SOPTS = { ...OPTS, group: 'smhc' };
/** commit で落ちる接続 (mode = lost: 本当は commit した / failed: 巻き戻した) */
const flaky = (mode) => ({ query: async (sql, params) => {
  if (sql === 'commit') {
    await db.query(mode === 'lost' ? 'commit' : 'rollback');
    throw Object.assign(new Error('Connection terminated unexpectedly'), { code: 'ECONNRESET' });
  }
  return db.query(sql, params);
} });
const reconnectOk = async () => ({ db, close: async () => {} });
await ta('[C1] 答えが無いまま巻き戻っていた = 「書けていない」 / 読み直せない = 「分からない」 / 本当は通っていた = 「書けた」 (どれも「巻き戻した」と言わない)', async () => {
  const d = await dry(SOPTS);
  assert.equal(d.exitCode, 0, JSON.stringify(d.problems));
  const a = await apply(d.hash, SOPTS, { db: flaky('failed'), reconnect: reconnectOk });
  assert.deepEqual([a.exitCode, a.outcome, a.ok], [1, 'not_written', false]);
  assert.equal((await state(SMHC_TAG, 'smhc%')).rev, null);
  const b = await apply(d.hash, SOPTS, { db: flaky('failed'), reconnect: async () => { throw new Error('no route'); } });
  assert.deepEqual([b.exitCode, b.outcome], [1, 'unknown']);
  const c = await apply(d.hash, SOPTS, { db: flaky('lost'), reconnect: reconnectOk });
  assert.deepEqual([c.exitCode, c.outcome, c.ok], [0, 'written', true]);
  const s = await state(SMHC_TAG, 'smhc%');
  assert.deepEqual([s.rev, s.events, s.choices], [1, 1, 2]);
});

console.log('product-hub');
const PG = await import('../apps/product-hub/services/cdb-group-intake.js');
const PHDB = await import('../apps/product-hub/db.js');
const ph = PHDB.getDB();
const runGroup = (gid) => asRole('master_edit', () => O.runGroupOutbox(db, (ev) => PG.applyCdbGroupEvent(ev), { groupProductId: gid, limit: 1 }));
await ta('[P1] 同じコードの (どこにも結ばれていない) カードがある = そのカードに結ぶ (linked・done)・子 3 つと選択肢', async () => {
  const pre = ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('fuwear', '補修シート', 'someone')`).run().lastInsertRowid;
  const res = await runGroup(TAG);
  assert.equal(res.length, 1);
  assert.deepEqual([res[0].status, res[0].result.outcome, res[0].result.draft_id], ['done', 'linked', Number(pre)]);
  const d = ph.prepare('SELECT id, cdb_group_product_id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').all('fuwear');
  assert.equal(d.length, 1);
  assert.equal(String(d[0].cdb_group_product_id), TAG);
  const kids = ph.prepare('SELECT code, active, choice1 FROM ph_cdb_group_children WHERE draft_id = ? ORDER BY code_key').all(d[0].id);
  assert.deepEqual(kids.map((k) => [k.code, k.active, k.choice1]), KIDS.map(([c]) => [c, 1, `-${c.slice(7).toUpperCase()}`]));
});
await ta('[P2] 同じコードのカードがほかの商品に結ばれている = conflict (増やさない・結ばない)', async () => {
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, cdb_sku_id) VALUES ('smhc', 'ほかの', 'someone', 99999)`).run();
  const res = await runGroup(SMHC_TAG);
  assert.deepEqual([res[0].status, res[0].result.outcome], ['conflict', 'conflict']);
  assert.equal(ph.prepare('SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get('smhc').c, 1);
  assert.equal((await one('select status from ops.product_hub_outbox where group_product_id = $1', [SMHC_TAG])).status, 'conflict');
});

console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
