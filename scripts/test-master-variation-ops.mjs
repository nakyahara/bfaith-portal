/**
 * test-master-variation-ops.mjs — まとまりの名前を直す・子の廃止・quarantined の代表の採用の lib (lib/master-variation.mjs・CompanyDB構想/20 v7 §⑩ の PR-7b)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs・products.parent も company)。書き込みは画面のロール master_edit。
 * 固定する契約:
 *   L 名前を直す: まとまりの名前 (札)・軸の名前・選択肢名・revision が 1 上がる・知らせ (スナップショット) の名前・子の商品名は変えない・変わる所が無い = 何も書かない・
 *     見た revision が古い = 409 version_conflict・選択肢名の重なり = 409・単品が代表のまとまりの名前 = 409・空の名前 = 400・products.parent が load = 409 parent_not_company
 *   C 子の廃止: 下書きの子 = cancelled・revision + 1・スナップショットの廃止した子・理由が要る (400)・まとまりの子でない = 409・生きている NE 登録の CSV = 409 live_file・
 *     NE に一度でも現れた = 409 seen_in_ne・押し直し (同じ request_id) = 前の答え
 *   A 代表の採用: NE で直接作られた商品 (quarantined) = 照合の観測の代表 (今ある札) に 1 回だけ・2 回目 = 409 already_adopted・代表が無い = 409 ne_no_parent・
 *     quarantined でない = 409 not_quarantined
 * 使い方: node scripts/test-master-variation-ops.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vg7b-ops-'));
delete process.env.MASTER_EDIT_OPEN;

const { setupVariationDb, basePlan, skuOf, NOW_MS } = await import('./fixtures/master-variation-db.mjs');
const V = await import('../lib/master-variation.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const uuid = () => crypto.randomUUID();
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
async function rejects(p, status, reason) {
  const e = await errOf(p);
  assert.ok(e, `断らなかった (${reason})`);
  assert.equal(e.reason, reason, `${e.reason}: ${e.message}`);
  assert.equal(e.status, status, e.message);
  return e;
}

const T = await setupVariationDb();
const { pg, db, q, one, asEditor } = T;
const RATES = new Map([['A1', { method: 'ネコポス', cost: 280 }]]);
const VALUES = { standard_price: '3980', cost: { jpy: '1650' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0' };
const op = (fn, input, opts = {}) => asEditor(() => fn(db, { actor: 'naka@test', requestId: uuid(), ...input }, { open: true, ...opts }));
const reg = (input) => asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: uuid(), ...input }, { open: true, shippingRates: RATES }));
const revOf = async (gid) => (await one('select revision from ops.variation_group_revisions where group_product_id = $1', [gid]))?.revision ?? 0;
const lastSnap = async (gid) => (await one('select payload from ops.product_hub_outbox where group_product_id = $1 order by revision desc limit 1', [gid])).payload;
const BL = await reg({ group: { mode: 'new', code: 'blanket-fl', name: 'フランネル ブランケット' }, axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
  options: [{ axis: 1, code: '-BR', name: 'ブラウン' }, { axis: 1, code: '-BE', name: 'ベージュ' }, { axis: 2, code: '-S', name: 'S' }],
  children: [{ code: 'blanket-fl-BR-S', choices: { 1: '-BR', 2: '-S' }, name: 'フランネル ブランケット【ブラウン】【S】' }, { code: 'blanket-fl-BE-S', choices: { 1: '-BE', 2: '-S' }, name: 'フランネル ブランケット【ベージュ】【S】' }],
  values: VALUES });
const GID = BL.group_product_id;

console.log('名前を直す');
await ta('[L1] まとまりの名前・軸の名前・選択肢名を直す = revision + 1・知らせの名前・子の商品名は変えない・変わる所が無い = 何も書かない', async () => {
  const r0 = await revOf(GID);
  const r = await op(V.editVariationLabels, { groupProductId: GID, seenRevision: r0, reason: '呼び方をそろえる',
    changes: { name: 'フランネル ブランケット (新)', axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-BR', name: 'チョコ' }] } });
  assert.equal(r.revision, r0 + 1);
  const g = await V.readVariationGroup(db, GID);
  assert.deepEqual([g.name, g.axes.map((a) => a.name), g.options.map((o) => o.name)], ['フランネル ブランケット (新)', ['色', 'サイズ'], ['チョコ', 'ベージュ', 'S']]);
  const p = await lastSnap(GID);
  assert.deepEqual([p.revision, p.group.name, p.options[0].name], [r0 + 1, 'フランネル ブランケット (新)', 'チョコ']);
  assert.equal((await one(`select name from core.skus where code = 'blanket-fl-BR-S'`)).name, 'フランネル ブランケット【ブラウン】【S】', '子の商品名は自動では変えない');
  const n = await op(V.editVariationLabels, { groupProductId: GID, seenRevision: r0 + 1, changes: { name: 'フランネル ブランケット (新)' } });
  assert.deepEqual([n.no_change, n.revision], [true, r0 + 1]);
  assert.equal(await revOf(GID), r0 + 1);
});

await ta('[L2] 断る: 見た revision が古い (409 version_conflict)・選択肢名の重なり (409)・単品が代表のまとまりの名前 (409)・空の名前 (400)・products.parent が load (409)・知らないまとまり (404)', async () => {
  const rev = await revOf(GID);
  await rejects(op(V.editVariationLabels, { groupProductId: GID, seenRevision: rev - 1, changes: { name: 'x' } }), 409, 'version_conflict');
  await rejects(op(V.editVariationLabels, { groupProductId: GID, seenRevision: rev, changes: { options: [{ axis: 1, code: '-BE', name: 'チョコ' }] } }), 409, 'option_name_exists');
  await rejects(op(V.editVariationLabels, { groupProductId: GID, seenRevision: rev, changes: { options: [{ axis: 1, code: '-BR', name: 'A' }, { axis: 1, code: '-BE', name: 'Ａ' }] } }), 400, 'option_name_exists');
  const towel = (await one(`select product_id::text as id from core.skus where code = 'towel-gift'`)).id;
  await rejects(op(V.editVariationLabels, { groupProductId: towel, seenRevision: await revOf(towel), changes: { name: 'タオル' } }), 409, 'group_name_is_single');
  await rejects(op(V.editVariationLabels, { groupProductId: GID, seenRevision: rev, changes: { axes: [{ axis: 1, name: ' ' }] } }), 400, 'invalid_input');
  await rejects(op(V.editVariationLabels, { groupProductId: '999999999', seenRevision: 0, changes: { name: 'x' } }), 404, 'not_found');
  await T.W2.setActiveOwnershipInDb(pg, { ...T.ALL_COMPANY, 'products.parent': 'load' });
  try { await rejects(op(V.editVariationLabels, { groupProductId: GID, seenRevision: rev, changes: { name: 'x' } }), 409, 'parent_not_company'); }
  finally { await T.W2.setActiveOwnershipInDb(pg, T.ALL_COMPANY); }
  await rejects(op(V.editVariationLabels, { groupProductId: GID, seenRevision: rev, changes: { name: 'x' } }, { open: false }), 409, 'before_cutover');
  assert.equal(await revOf(GID), rev);
});

console.log('\n子の廃止');
await ta('[C1] 下書きの子を廃止 = cancelled・revision + 1・知らせの廃止した子・押し直し = 前の答え / 理由が要る (400)・まとまりの子でない (409)', async () => {
  const rev = await revOf(GID);
  const er = await rejects(op(V.cancelVariationChild, { code: 'blanket-fl-BE-S', reason: '' }), 400, 'invalid_input');
  assert.equal(er.extra.field, 'reason', '画面の理由の欄へ (DB に送る前に断る)');
  const rid = uuid();
  const r = await op(V.cancelVariationChild, { requestId: rid, code: 'blanket-fl-BE-S', reason: 'ベージュは作らない' });
  assert.deepEqual([r.state, r.revision], ['cancelled', rev + 1]);
  assert.equal((await one(`select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = 'blanket-fl-BE-S'`)).state, 'cancelled');
  const p = await lastSnap(GID);
  assert.deepEqual([p.children.map((c) => c.code), p.cancelled_children.map((c) => c.code)], [['blanket-fl-BR-S'], ['blanket-fl-BE-S']]);
  const again = await op(V.cancelVariationChild, { requestId: rid, code: 'blanket-fl-BE-S', reason: 'ベージュは作らない' });
  assert.equal(again.replayed, true);
  await rejects(op(V.cancelVariationChild, { code: 's001', reason: 'x' }), 409, 'not_variation_child');
  await rejects(op(V.cancelVariationChild, { code: 'blanket-fl-BE-S', reason: 'もう一度' }), 409, 'child_not_cancellable');
  await rejects(op(V.cancelVariationChild, { code: 'nope-1', reason: 'x' }), 404, 'not_found');
  // 今の画面: 廃止した組み合わせは「今ある」(作り直さない = コードは使い回さない)
  const g = await V.readVariationGroup(db, GID);
  assert.ok(g.children.some((k) => k.code === 'blanket-fl-BE-S' && k.cancelled));
});

await ta('[C2] 生きている NE 登録の CSV がある子 = 409 live_file / NE に一度でも現れた子 = 409 seen_in_ne (どちらも何もしない)', async () => {
  const s = await asEditor(() => G.regSummary(db, { nowMs: NOW_MS }));
  const codes = s.variation.find((g) => g.group_code === 'blanket-fl').rounds.flatMap((r) => r.codes);
  assert.deepEqual(codes, ['blanket-fl-BR-S']);
  const f = await asEditor(() => G.buildRegExport(db, { actor: 'naka@test', kind: 'products', variation: true, codes, requestId: uuid() }, { open: true, nowMs: NOW_MS }));
  const rev = await revOf(GID);
  await rejects(op(V.cancelVariationChild, { code: 'blanket-fl-BR-S', reason: 'x' }), 409, 'live_file');
  await asEditor(() => G.supersedeRegExport(db, { actor: 'naka@test', exportId: f.export.export_id, reason: '試験', correction: 'NE では何もしていない', confirm: true }, { open: true }));
  // NE に現れた (新しい照合の回の NE のコード)
  const run = 'mc_20300110T050000000Z_ccccc1';
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T05:00:00Z', 0)`, [run]);
  const prev = await q('select code_norm, kind, state, ne_code from ops.master_ne_codes');
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: [...prev.map((x) => ({ code_norm: x.code_norm, kind: x.kind, state: x.state, ne_code: x.ne_code, spellings: [x.ne_code] })),
    { code_norm: 'blanket-fl-br-s', kind: 'product', state: 'ok', ne_code: 'blanket-fl-BR-S', spellings: ['blanket-fl-BR-S'] }] })]);
  await rejects(op(V.cancelVariationChild, { code: 'blanket-fl-BR-S', reason: 'x' }), 409, 'seen_in_ne');
  assert.equal(await revOf(GID), rev);
});

console.log('\nquarantined の代表の採用');
await ta('[A1] NE で直接作られた商品 (quarantined): 照合の観測の代表 (今ある札 ws100) を 1 回だけ採用・2 回目 = already_adopted・代表が無い = ne_no_parent・quarantined でない = not_quarantined', async () => {
  const extra = [skuOf('q-ws-RD', 'ウールストール【レッド】', { representativeCode: 'ws100', representativeState: 'value' }), skuOf('q-none', '代表なし')];
  const lr = await runInitialLoad(db, basePlan(extra), { log: () => {}, runId: 'load_vg7b_q', now: new Date(Date.now() - 4 * 86400e3) });
  assert.equal(lr.ok, true, lr.error);
  assert.deepEqual((await q(`select s.code, r.state, p.parent_product_id from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id join core.products p on p.product_id = s.product_id
     where s.code like 'q-%' order by s.code`)).map((r) => [r.code, r.state, r.parent_product_id]), [['q-none', 'quarantined', null], ['q-ws-RD', 'quarantined', null]]);
  // 照合 ②: 回 → NE の元のコード → 確かめ待ちの写し → 観測 (代表) → 封
  const run = 'mc_20300111T000000000Z_ddddd1';
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-11T00:00:00Z', 0)`, [run]);
  const prev = await q('select code_norm, kind, state, ne_code from ops.master_ne_codes');
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: [...prev.map((x) => ({ code_norm: x.code_norm, kind: x.kind, state: x.state, ne_code: x.ne_code, spellings: [x.ne_code] })),
    ...['q-ws-RD', 'q-none'].map((c) => ({ code_norm: c.toLowerCase(), kind: 'product', state: 'ok', ne_code: c, spellings: [c] }))] })]);
  const snap = (await T.asRole('watch_writer', () => pg.query('select ops.snapshot_ne_reg_targets($1) as r', [run]))).rows[0].r;
  const ok = (v) => ({ st: 'ok', v });
  const obs = { 'q-ws-rd': 'ws100', 'q-none': null };
  const observations = snap.targets.map((t) => (Object.prototype.hasOwnProperty.call(obs, t.code_norm)
    ? { code_norm: t.code_norm, present: true, trusted: true, kind: 'single', cols: { name: ok('x'), parent: ok(obs[t.code_norm]) } }
    : { code_norm: t.code_norm, present: false, trusted: true, kind: null }));
  const w = (await T.asRole('watch_writer', () => pg.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run,
    fetch: { generation_id: 'gen_vg7b', products_rev: '1', sets_rev: '1', raw_hash: 'c'.repeat(64) }, products_at: new Date().toISOString(), sets_at: new Date().toISOString(), absence_trusted: true, observations })]))).rows[0].r;
  await T.asRole('watch_writer', () => pg.query('select ops.seal_ne_registration_run($1, $2, $3)', [run, w.observation_hash, 'e'.repeat(64)]));
  const ws = (await one(`select product_id::text as id from core.products where display_code = 'ws100'`)).id;
  const rev = await revOf(ws);
  const a = await op(V.adoptNeParent, { code: 'q-ws-RD', reason: 'NE で直接作ってしまった' });
  assert.deepEqual([a.group_product_id, a.group_code, a.group_created, a.revision], [ws, 'ws100', false, rev + 1]);
  assert.equal((await one(`select pp.display_code as p from core.skus s join core.products p on p.product_id = s.product_id join core.products pp on pp.product_id = p.parent_product_id where s.code = 'q-ws-RD'`)).p, 'ws100');
  await rejects(op(V.adoptNeParent, { code: 'q-ws-RD' }), 409, 'already_adopted');
  await rejects(op(V.adoptNeParent, { code: 'q-none' }), 409, 'ne_no_parent');
  await rejects(op(V.adoptNeParent, { code: 's001' }), 409, 'not_quarantined');
});

console.log(`\n${passed} ok`);
