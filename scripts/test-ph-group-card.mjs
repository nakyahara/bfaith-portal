/**
 * test-ph-group-card.mjs — product-hub のまとまり (色違い・サイズ違い) のカード (Company DB構想 20 v7 §⑤ / §⑩ の PR-4)
 *
 * Company DB = PGlite (最新の migration まで・Render と同じ持ち主のロール deploy)。product-hub = 一時の DATA_DIR の SQLite (本物の取り込み・本物の router)。
 * まとまりの知らせを出す側 (PR-5 の DB の関数) はまだ無い = 試験は Company DB の今の値から lib の形 (ph-group-v1) のスナップショットを作り、
 * 部品 ops.enqueue_group_snapshot (持ち主) で outbox に入れて流す (DB の trigger が形と今の値を確かめる = 本番と同じ決まりを通る)。
 * 固定する契約:
 *   S 今の単品のカード: 登録 → SKU の知らせ → runCardOutbox → カード (今までどおり)。SKU の取り込みはまとまりの知らせを借りない・まとまりの取り込みは SKU の知らせを借りない。
 *     ボードの sweep は両方を同じ接続で・まとまりの側が落ちても SKU の側は今までどおり done
 *   G まとまりの取り込み: 作る (まとまりで 1 枚・cdb_group_product_id は別の列・cdb_sku_id は使わない)・同じ知らせは 1 回だけ・revision が大きいときだけ全部置き換え・
 *     小さい / 同じ = 何もしない (順番が逆でも)・廃止した子は消さずに無効・要確認・hash / 形 / 番号が違えば取り込まない・同じコードのカード = 結ぶ / 衝突 (結べない・2 枚)
 *   N NE の写し待ち: 有効な子と mirror の子の集まり・代表がちょうど同じになるまで出品を止める (足りない・代表が違う・NE にだけある・同じコードが 2 行)・大文字のコード
 *   X 2 軸のまとまり = 保存するが出品は止める
 *   L サーバー側の共通の門: 楽天の出品 (詳細画面・ボード・確認済みの再実行)・payload (プレビュー)・今あるページの SKU (sku-images)・公開に切り替える が全部止まる
 *     (楽天に 1 回も書かない)・非公開にするのは止めない・まとまりでないカードは今までどおり・楽天に書く道を全部数えて門があるか (ソース)
 *   H 画面: ボードの札・詳細のまとまり (暫定の正本の子の一覧・無効の子・要確認) と「確かめた」・画面の JS が読める
 * 使い方: node scripts/test-ph-group-card.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';

const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vg4-group-card-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const C = await import('../lib/master-cutover.mjs');
const R = await import('../lib/master-register.mjs');
const O = await import('../lib/product-hub-outbox.mjs');
const LG = await import('../lib/master-legacy-gate.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const PH = await import('../apps/product-hub/services/cdb-card-intake.js');
const PG = await import('../apps/product-hub/services/cdb-group-intake.js');
const GATE = await import('../apps/product-hub/services/cdb-group-gate.js');
const RL = await import('../apps/product-hub/services/rakuten-listing.js');
const BL = await import('../apps/product-hub/services/board-listing.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const uuid = () => crypto.randomUUID();

const pg = new PGlite();
const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
assert.ok((await pg.query(`select count(*)::int as n from ops.schema_migrations where version = '0066'`)).rows[0].n === 1, '0066 (PR-3) が要る');
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg, { leases: ['single', 'set'], futureSetLease: true });
const q = async (sql, params) => (await db.query(sql, params)).rows;
const one = async (sql, params) => (await q(sql, params))[0];
async function tx(fn) {
  await pg.query('begin');
  try { const r = await fn(); await pg.query('commit'); return r; } catch (e) { try { await pg.query('rollback'); } catch { /* */ } throw e; }
}
async function asRole(role, fn) {
  await pg.query(`set role ${role}`);
  try { return await fn(); } finally { await pg.query('set role deploy'); }
}
const asEditor = (fn) => asRole('master_edit', fn);
async function asGate(host, fn) {
  await pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await pg.query(`set session authorization ${sessionUser}`); await pg.query('set role deploy'); }
}

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const LOAD_NOW = new Date(Date.now() - 5 * 86400e3);
const NOW = new Date();
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }] };
const BUILDS = { render: ['r1'], minipc: ['m1'] };
async function toPhase(to) {
  const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
  if (to !== 'frozen') await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(db, ALL_COMPANY);
  const own = to === 'frozen' ? MASTER_OWNERSHIP : ALL_COMPANY;
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) {
    await asGate(host, () => C.recordLegacyGateAck(db, { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
  }
  const mh = await C.manifestHashOf(db, MANIFEST);
  const evidence = to === 'frozen'
    ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(MASTER_OWNERSHIP), manual_entries_stopped: [{ id: 'ne.product_screen', by: 'naka@test', at: new Date().toISOString() }],
      drain: { done: true, checked_by: 'naka@test', checked_at: new Date().toISOString() } }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL_COMPANY) };
  const r = await asRole('master_ops', () => C.advanceCutoverPhase(db, { to, actor: 'naka@test', evidence }));
  if (to === 'new_open') await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(db, { withSet: true });
  return r;
}

// 夜間ロード: 札のまとまり grp1 (子 s001・s003)・単品の代表のまとまり s002 (子 s004)・大文字の札 Hakama (2 軸の子 2 つ)・札 tshirt (子 1 つ)
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
const rep = (code) => ({ representativeCode: code, representativeState: 'value' });
const plan = {
  skus: [sku('s001', '単品 1【赤】', { ...rep('grp1'), standardPriceJpy: 1980 }), sku('s002', '代表の単品'), sku('s003', '単品 3【青】', rep('grp1')), sku('s004', '代表の子', rep('s002')),
    sku('Hakama-WH-90', 'はかま【白】【90】', rep('Hakama')), sku('Hakama-BK-90', 'はかま【黒】【90】', rep('Hakama')), sku('tshirt-RD', 'Tシャツ【赤】', rep('tshirt')), sku('cap-1', '帽子【赤】', rep('cap')), sku('cap-2', '帽子【青】', rep('cap'))],
  variationGroups: [{ code: 'grp1', name: '札のまとまり', childCodes: ['s001', 's003'], status: 'active' }, { code: 's002', name: '単品の代表', childCodes: ['s004'], status: 'active' },
    { code: 'Hakama', name: 'はかま', childCodes: ['Hakama-WH-90', 'Hakama-BK-90'], status: 'active' }, { code: 'tshirt', name: 'Tシャツ', childCodes: ['tshirt-RD'], status: 'active' },
    { code: 'cap', name: '帽子', childCodes: ['cap-1', 'cap-2'], status: 'active' }],
  setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_test' },
};
const loadR = await runInitialLoad(db, plan, { log: quiet, runId: 'load_vg4', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
assert.equal(loadR.ok, true, loadR.error);
const skuId = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code]))?.id;
const tagOf = async (code) => (await one(`select product_id::text as id from core.products p where p.display_code = $1 and not exists (select 1 from core.skus k where k.product_id = p.product_id)`, [code]))?.id;
const grp1 = await tagOf('grp1');
const hakama = await tagOf('Hakama');
const tshirt = await tagOf('tshirt');
const rep2 = (await one(`select product_id::text as id from core.skus where code = 's002'`)).id;
assert.ok(grp1 && hakama && tshirt && rep2, '夜間ロードがまとまりを作っていない');

await toPhase('frozen');
await toPhase('company_owner');
const bp = await one('select * from ops.registration_backfill_plan()');
await asRole('master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3, $4)', [bp.sku_count, bp.snapshot_hash, 'naka@test', '試験']));
await toPhase('new_open');

const ph = PHDB.getDB();
ph.prepare(`INSERT INTO ph_shipping_method_map (ne_label, rakuten_group) VALUES ('ゆうパケット', '9')`).run();
const applyCard = (ev) => PH.applyCdbCardEvent(ev);
const applyGroup = (ev) => PG.applyCdbGroupEvent(ev);
const runCards = (opts) => asEditor(() => O.runCardOutbox(db, applyCard, opts));
const runGroups = (opts) => asEditor(() => O.runGroupOutbox(db, applyGroup, opts));
const draftOf = (code) => ph.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(String(code).toLowerCase());
const kidsOf = (draftId) => ph.prepare('SELECT code, active, inactive_reason, choice1, choice2 FROM ph_cdb_group_children WHERE draft_id = ? ORDER BY code_key').all(draftId);
const single = (over = {}) => ({ name: '新しい単品', standard_price: '1,980', shipping_code: 'S01', tax_rate: '10', primary_supplier: '1', sales_class: '3', expiry_managed: '0', reorder_months: '1', ...over });
const reg = (code, values = single(), card = {}) => asEditor(() => R.registerNewSku(db, { actor: 'Naka@Test', requestId: uuid(), kind: 'single', code, reason: null, values, card },
  { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES }));

/** 今の Company DB の値から作ったまとまりの完全なスナップショット (PR-5 の関数が作る形)。choices = { 子のコード: { 1, 2 } } */
async function snap(groupPid, revision, { axes = [], options = [], choices = {}, common = null } = {}) {
  const g = await one(`select p.product_id::text as pid, p.display_code, p.name, k.sku_id::text as rep_sku, k.code as rep_code
    from core.products p left join core.skus k on k.product_id = p.product_id and k.sku_kind = 'single' where p.product_id = $1`, [groupPid]);
  const kids = await q(`select s.sku_id::text as sku_id, s.code, s.name, s.standard_price_jpy::text as price, r.state,
      coalesce((select array_agg(e.external_value order by e.external_value) from core.external_ids e where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.valid_to is null), '{}') as jans
    from core.products c join core.skus s on s.product_id = c.product_id and s.sku_kind = 'single' left join ops.master_registrations r on r.sku_id = s.sku_id
    where c.parent_product_id = $1 order by s.sku_id`, [groupPid]);
  return {
    schema: 'ph-group-v1', revision, created_by: 'naka@test',
    group: { product_id: g.pid, sku_id: g.rep_sku ?? null, code: g.rep_sku ? g.rep_code : g.display_code, name: g.name, kind: g.rep_sku ? 'single' : 'tag' },
    axes, options,
    children: kids.filter((k) => k.state !== 'cancelled').map((k) => ({ sku_id: k.sku_id, code: k.code, name: k.name, price: k.price == null ? null : Number(k.price), choices: choices[k.code] || {}, jans: k.jans })),
    cancelled_children: kids.filter((k) => k.state === 'cancelled').map((k) => ({ sku_id: k.sku_id, code: k.code })),
    common: common || { shipping: { code: 'S01', method: 'ゆうパケット', cost_jpy: 210 }, amazon_url: null, asin: null, official_url: 'https://maker.example/grp', reference_urls: ['https://ref.example/1'], yahoo: null },
  };
}
const enqueue = (gid, rev, payload) => tx(async () => (await pg.query('select ops.enqueue_group_snapshot($1, $2, $3::jsonb, $4, $5)::text as id', [gid, rev, JSON.stringify(payload), uuid(), 'naka@test'])).rows[0].id);
const outboxEv = (eid) => one('select event_id::text as eid, status, attempts, result, last_error, revision, payload from ops.product_hub_outbox where event_id = $1', [eid]);
const cancelChild = (code) => tx(async () => {
  await pg.query(`select set_config('ops.registration_protocol', '1', true)`);
  await pg.query(`update ops.master_registrations set state = 'cancelled' where sku_id = (select sku_id from core.skus where code = $1)`, [code]);
});
let mirrorPid = 900000;
const mirror = (code, repCode) => ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', ?, '2026-10-09T00:00:00Z')`).run(++mirrorPid, code, repCode);
const unmirror = (code) => ph.prepare('DELETE FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ?').run(String(code).toLowerCase());

console.log('今の単品のカード (契約)');

let skuA;
await ta('[S1] 単品の登録 → SKU の知らせ → runCardOutbox → カード (今までどおり)。まとまりの取り込みは SKU の知らせを借りない', async () => {
  skuA = await reg('ph-single-1', single({ name: 'ふつうの単品' }), { official_url: 'https://maker.example/s1' });
  assert.deepEqual(await runGroups({}), []);
  assert.equal((await outboxEv(skuA.card.event_id)).status, 'pending');
  const res = await runCards({});
  assert.deepEqual(res.map((r) => [r.sku_id, r.status, r.result.outcome]), [[skuA.sku_id, 'done', 'created']]);
  const d = draftOf('ph-single-1');
  assert.deepEqual([String(d.cdb_sku_id), d.cdb_group_product_id, d.cdb_group_revision, d.official_url], [skuA.sku_id, null, null, 'https://maker.example/s1']);
  assert.equal(GATE.cdbGroupListingBlock(ph, d.id), null, 'まとまりでないカードに門が掛かった');
  assert.equal(GATE.cdbGroupCardView(ph, d.id), null);
});

console.log('\nまとまりの取り込み');

let g1ev1; let g1;
await ta('[G1] まとまりの知らせ (revision 1) → まとまりで 1 枚のカード (cdb_group_product_id・revision・子の表・共通の欄)。SKU の取り込みはまとまりの知らせを借りない', async () => {
  g1ev1 = await enqueue(grp1, 1, await snap(grp1, 1));
  assert.deepEqual(await runCards({ manual: true }), []);
  const res = await runGroups({});
  assert.deepEqual(res.map((r) => [r.event_id, r.group_product_id, r.revision, r.status, r.result?.outcome, r.recorded]), [[g1ev1, grp1, 1, 'done', 'created', true]]);
  g1 = draftOf('grp1');
  const tagName = (await one('select name from core.products where product_id = $1', [grp1])).name;
  assert.deepEqual([g1.ne_code, g1.name, String(g1.cdb_group_product_id), g1.cdb_group_revision, g1.cdb_sku_id, g1.has_variation, g1.price, g1.official_url],
    ['grp1', tagName, grp1, 1, null, 1, null, 'https://maker.example/grp']);
  assert.deepEqual(kidsOf(g1.id).map((k) => [k.code, k.active]), [['s001', 1], ['s003', 1]]);
  assert.equal(ph.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(g1.id).shipping_method_group, '9');
  assert.deepEqual(ph.prepare('SELECT url FROM draft_reference_urls WHERE draft_id = ?').all(g1.id).map((r) => r.url), ['https://ref.example/1']);
  const ob = await outboxEv(g1ev1);
  assert.deepEqual([ob.status, ob.result.outcome, ob.result.draft_id], ['done', 'created', g1.id]);
  assert.equal(ph.prepare(`SELECT COUNT(*) AS c FROM draft_events WHERE draft_id = ? AND event = 'created_from_cdb_group'`).get(g1.id).c, 1);
  assert.equal(GATE.cdbGroupCardView(ph, g1.id).attention, null, '作っただけで要確認を出した');
});

await ta('[G2] 同じ知らせをもう一度 (結果を書けなかった後の取り込みの形) = 記録の答え・カードも子も増えない', async () => {
  const ob = await outboxEv(g1ev1);
  const again = PG.applyCdbGroupEvent({ event_id: g1ev1, schema_version: 'ph-group-v1', kind: 'group_snapshot', entity_kind: 'variation_group', group_product_id: grp1, revision: 1, payload: ob.payload });
  assert.deepEqual([again.outcome, again.draft_id, again.replayed], ['created', g1.id, true]);
  assert.equal(ph.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'grp1'`).get().c, 1);
  assert.equal(kidsOf(g1.id).length, 2);
});

let g1ev3;
await ta('[G3] 子を廃止 → revision 3 (続いていなくてよい) = 全部置き換え・廃止した子は消さずに無効・要確認 (モールは自動で変えない)', async () => {
  await cancelChild('s003');
  g1ev3 = await enqueue(grp1, 3, await snap(grp1, 3));
  const res = await runGroups({});
  assert.deepEqual(res.map((r) => [r.revision, r.status, r.result.outcome]), [[3, 'done', 'replaced']]);
  assert.equal(draftOf('grp1').cdb_group_revision, 3);
  assert.deepEqual(kidsOf(g1.id).map((k) => [k.code, k.active, k.inactive_reason]), [['s001', 1, null], ['s003', 0, 'cancelled']]);
  const v = GATE.cdbGroupCardView(ph, g1.id);
  assert.match(v.attention, /revision 3: 子を 1 件廃止した/);
  assert.deepEqual(v.children.map((c) => c.code), ['s001']);
  assert.deepEqual(v.inactiveChildren.map((c) => c.code), ['s003']);
  assert.equal(draftOf('grp1').name, g1.name, 'カードの名前を置き換えた');
});

await ta('[G4] 古い / 同じ revision = 何もしない (stale = done)・順番が逆に届いても最後は大きい revision の姿', async () => {
  const p1 = (await outboxEv(g1ev1)).payload;
  // 古い revision 1 の中身を別の知らせとして (書けなかった・遅れて届いた知らせの形)
  const st = PG.applyCdbGroupEvent({ event_id: uuid(), schema_version: 'ph-group-v1', payload: p1 });
  assert.deepEqual([st.outcome, st.draft_id, st.revision], ['stale', g1.id, 3]);
  const p3 = (await outboxEv(g1ev3)).payload;
  assert.equal(PG.applyCdbGroupEvent({ event_id: uuid(), schema_version: 'ph-group-v1', payload: p3 }).outcome, 'stale', '同じ revision をもう一度効かせた');
  assert.deepEqual(kidsOf(g1.id).map((k) => [k.code, k.active]), [['s001', 1], ['s003', 0]], '古い revision で廃止した子を戻した');
  // 順番が逆: revision 5 を先に・4 を後に取り込む
  const e4 = await enqueue(grp1, 4, await snap(grp1, 4));
  const e5 = await enqueue(grp1, 5, await snap(grp1, 5));
  assert.deepEqual((await runGroups({ eventId: e5 })).map((r) => [r.revision, r.status, r.result.outcome]), [[5, 'done', 'replaced']]);
  assert.deepEqual((await runGroups({ eventId: e4 })).map((r) => [r.revision, r.status, r.result.outcome]), [[4, 'done', 'stale']]);
  assert.equal(draftOf('grp1').cdb_group_revision, 5);
});

await ta('[G5] 取り込まない知らせ: hash が違う・まとまりの番号 / revision が列と違う・形が違う = failed (カードは変えない)・product-hub の取り込みも形を確かめる', async () => {
  const e6 = await enqueue(grp1, 6, await snap(grp1, 6));
  const tamper = (fn) => ({ query: async (sql, p) => { const r = await db.query(sql, p); if (/claim_outbox_events/.test(sql)) r.rows = r.rows.map(fn); return r; } });
  for (const [label, fn, re] of [
    ['hash', (row) => ({ ...row, payload: { ...row.payload, group: { ...row.payload.group, name: '書き換えた' } } }), /payload_hash_mismatch/],
    ['番号', (row) => ({ ...row, group_product_id: rep2 }), /group_mismatch/],
    ['revision', (row) => ({ ...row, revision: 7 }), /revision_mismatch/],
    ['版', (row) => ({ ...row, schema_version: 'ph-group-v2' }), /kind_or_schema/],
  ]) {
    const res = await asEditor(() => O.runGroupOutbox(tamper(fn), applyGroup, { eventId: e6, manual: true }));
    assert.equal(res.length, 1, label);
    assert.deepEqual([res[0].status, res[0].recorded], ['failed', true], label);
    assert.match(res[0].error, re, label);
    assert.equal(draftOf('grp1').cdb_group_revision, 5, `${label}: 取り込んだ`);
  }
  assert.equal((await outboxEv(e6)).status, 'failed');
  // 直してもう一度 (人が押す = manual) = 取り込む
  assert.deepEqual((await runGroups({ eventId: e6, manual: true })).map((r) => [r.status, r.result.outcome]), [['done', 'replaced']]);
  // product-hub の取り込みだけでも形を見る (lib を通らない呼び手)
  const p = (await outboxEv(e6)).payload;
  assert.throws(() => PG.applyCdbGroupEvent({ event_id: uuid(), schema_version: 'ph-group-v1', payload: { ...p, axes: [{ axis: 2, name: '縦だけ' }] } }), /axes_order/);
  assert.throws(() => PG.applyCdbGroupEvent({ event_id: uuid(), schema_version: 'ph-card-v1', payload: p }), /知らないまとまりの知らせの版/);
  assert.throws(() => PG.applyCdbGroupEvent({ event_id: uuid(), schema_version: 'ph-group-v1', group_product_id: rep2, payload: p }), /番号が違う/);
  assert.throws(() => PG.applyCdbGroupEvent({ event_id: uuid(), schema_version: 'ph-group-v1', revision: 9, payload: p }), /revision が違う/);
  assert.equal(draftOf('grp1').cdb_group_revision, 6);
});

await ta('[G6] 同じコードのカード: 別の商品 (SKU) に結ばれている = 衝突 (増やさない・結ばない)・単品の代表のカード = 結ぶ (要確認)・2 枚 = どれか決められない', async () => {
  // 単品の代表のまとまり s002: 同じコードのカードが別の SKU に結ばれている = 衝突
  const other = ph.prepare(`INSERT INTO product_drafts (ne_code, name, price, created_by, cdb_sku_id) VALUES ('s002', '代表のページ', 2980, 'someone', 99999)`).run();
  const oid = Number(other.lastInsertRowid);
  const e1 = await enqueue(rep2, 1, await snap(rep2, 1));
  let res = await runGroups({ eventId: e1 });
  assert.deepEqual([res[0].status, res[0].result.outcome, res[0].result.conflict_draft_id], ['conflict', 'conflict', oid]);
  assert.match(res[0].error, /別の商品 \(SKU 99999\)/);
  assert.deepEqual([draftOf('s002').cdb_group_product_id, ph.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 's002'`).get().c], [null, 1]);
  assert.deepEqual(await runGroups({}), [], '衝突を自動でもう一度取り込んだ');
  // 人がカードを代表の SKU のカードに直してから「もう一度」(manual) = 結ぶ・カードの欄は変えない・要確認
  ph.prepare('UPDATE product_drafts SET cdb_sku_id = ? WHERE id = ?').run(Number(await skuId('s002')), oid);
  res = await runGroups({ eventId: e1, manual: true });
  assert.deepEqual([res[0].status, res[0].result.outcome, res[0].result.draft_id], ['done', 'linked', oid]);
  const d = draftOf('s002');
  assert.deepEqual([String(d.cdb_group_product_id), d.cdb_group_revision, String(d.cdb_sku_id), d.name, d.price], [rep2, 1, await skuId('s002'), '代表のページ', 2980]);
  assert.deepEqual(kidsOf(oid).map((k) => k.code), ['s004']);
  assert.match(GATE.cdbGroupCardView(ph, oid).attention, /今あるカードを Company DB のまとまり s002 に結んだ/);
  // 2 枚 (前からの重なりで一意の index を張れなかった DB) = どれか決められない・増やさない
  ph.exec('DROP INDEX IF EXISTS idx_product_drafts_ne_norm');
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('tshirt', 'A', 'x')`).run();
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('TSHIRT ', 'B', 'x')`).run();
  const e2 = await enqueue(tshirt, 1, await snap(tshirt, 1));
  res = await runGroups({ eventId: e2 });
  assert.deepEqual([res[0].status, res[0].result.ambiguous, res[0].result.conflict_draft_ids.length], ['conflict', true, 2]);
  assert.equal(ph.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'tshirt'`).get().c, 2);
  assert.equal(ph.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE cdb_group_product_id = ?`).get(Number(tshirt)).c, 0);
  // 片付けて「もう一度」= 結ぶ
  ph.prepare(`DELETE FROM product_drafts WHERE ne_code = 'TSHIRT '`).run();
  ph.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_product_drafts_ne_norm ON product_drafts(LOWER(TRIM(ne_code)))');
  res = await runGroups({ eventId: e2, manual: true });
  assert.deepEqual([res[0].status, res[0].result.outcome], ['done', 'linked']);
  // まとまりのカードに単品の知らせ (同じコード) = 今までどおり衝突 (cdb_sku_id は使い回さない)
  assert.equal(PH.applyCdbCardEvent({ event_id: uuid(), sku_id: '88888', schema_version: 'ph-card-v1',
    payload: { schema: 'ph-card-v1', code: 'grp1', kind: 'single', name: 'x', price: 1, shipping: { code: 'S01', method: null, cost_jpy: null } } }).outcome, 'conflict');
  assert.equal(draftOf('grp1').cdb_sku_id, null);
});

await ta('[G7] 初めてのスナップショットに廃止した子がある (product-hub が一度も見ていない子) = 無効の印で入れる・有効な子と NE の比べには入れない', async () => {
  await cancelChild('cap-2');
  const cap = await tagOf('cap');
  const e1 = await enqueue(cap, 1, await snap(cap, 1));
  assert.deepEqual((await runGroups({ eventId: e1 })).map((r) => r.result.outcome), ['created']);
  const d = draftOf('cap');
  assert.deepEqual(kidsOf(d.id).map((k) => [k.code, k.active, k.inactive_reason]), [['cap-1', 1, null], ['cap-2', 0, 'cancelled']]);
  mirror('cap-1', 'cap');
  assert.equal(GATE.cdbGroupCardView(ph, d.id).state, 'ready', '廃止した子を NE と比べた');
});

console.log('\nNE の写し待ち・2 軸・大文字');

await ta('[N1] NE に入る前 = NE の写し待ち (出品を止める)・有効な子と mirror の子の集まり・代表がちょうど同じ = ふつう・ずれれば戻る', async () => {
  let v = GATE.cdbGroupCardView(ph, g1.id);
  assert.deepEqual([v.state, v.sync.missing], ['ne_waiting', ['s001']]);
  assert.match(GATE.cdbGroupListingBlock(ph, g1.id).message, /NE の写し待ち.*NE にまだ無い子 1 件 \(s001\)/);
  mirror('s001', '');
  v = GATE.cdbGroupCardView(ph, g1.id);
  assert.deepEqual([v.state, v.sync.repDiffers], ['ne_waiting', [{ code: 's001', rep: '' }]]);
  ph.prepare(`UPDATE mirror_products SET 代表商品コード = 'GRP1' WHERE 商品コード = 's001'`).run();
  assert.equal(GATE.cdbGroupCardView(ph, g1.id).state, 'ready', '代表を小文字で比べていない');
  assert.equal(GATE.cdbGroupListingBlock(ph, g1.id), null);
  // 廃止した子が NE に代表つきで現れた = NE にだけある子 = 止める
  mirror('s003', 'grp1');
  v = GATE.cdbGroupCardView(ph, g1.id);
  assert.deepEqual([v.state, v.sync.extra], ['ne_waiting', ['s003']]);
  unmirror('s003');
  // NE に同じコードが 2 行
  mirror('S001', 'grp1');
  assert.deepEqual(GATE.cdbGroupCardView(ph, g1.id).sync.dup, ['s001']);
  ph.prepare(`DELETE FROM mirror_products WHERE 商品コード = 'S001'`).run();
  assert.equal(GATE.cdbGroupCardView(ph, g1.id).state, 'ready');
  // 単品の代表のまとまり: 代表自身 (代表 = 自分) は NE にだけある子に数えない
  const d2 = draftOf('s002');
  mirror('s002', 's002'); mirror('s004', 's002');
  assert.equal(GATE.cdbGroupCardView(ph, d2.id).state, 'ready');
});

let hk;
await ta('[X1] 2 軸 (横・縦) のまとまり = 子・軸・選択肢を保存・NE と一致しても出品は止める・大文字のコードは打ったとおり (鍵は小文字)', async () => {
  const axes = [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }];
  const options = [{ axis: 1, code: '-WH', name: 'ホワイト', sort: 0 }, { axis: 1, code: '-BK', name: 'ブラック', sort: 1 }, { axis: 2, code: '-90', name: '90cm', sort: 0 }];
  const choices = { 'Hakama-WH-90': { 1: '-WH', 2: '-90' }, 'Hakama-BK-90': { 1: '-BK', 2: '-90' } };
  const e1 = await enqueue(hakama, 1, await snap(hakama, 1, { axes, options, choices }));
  assert.deepEqual((await runGroups({ eventId: e1 })).map((r) => [r.status, r.result.outcome]), [['done', 'created']]);
  hk = draftOf('hakama');
  assert.equal(hk.ne_code, 'Hakama', 'まとまりのコードを打ったとおりに入れていない');
  assert.deepEqual(kidsOf(hk.id).map((k) => [k.code, k.choice1, k.choice2]), [['Hakama-BK-90', '-BK', '-90'], ['Hakama-WH-90', '-WH', '-90']]);
  // NE の写しは小文字 (ne-api.js) = 小文字で比べて一致
  mirror('hakama-wh-90', 'hakama'); mirror('hakama-bk-90', 'hakama');
  const v = GATE.cdbGroupCardView(ph, hk.id);
  assert.deepEqual([v.twoAxes, v.sync.synced, v.state], [true, true, 'two_axes']);
  assert.match(GATE.cdbGroupListingBlock(ph, hk.id).message, /2 軸 \(カラー × サイズ\) のまとまり/);
  // 選択肢名を社内で直した (revision 2) = 要確認「社内の選択肢名が変わった」・前の名前は残さない (有効な選択肢 = スナップショット)
  const e2 = await enqueue(hakama, 2, await snap(hakama, 2, { axes, options: options.map((o) => (o.code === '-WH' ? { ...o, name: 'オフホワイト' } : o)), choices }));
  assert.deepEqual((await runGroups({ eventId: e2 })).map((r) => r.result.outcome), ['replaced']);
  const v2 = GATE.cdbGroupCardView(ph, hk.id);
  assert.match(v2.attention, /社内の選択肢名が変わった -WH ホワイト → オフホワイト/);
  assert.equal(v2.options.find((o) => o.code === '-WH').name, 'オフホワイト');
});

console.log('\nサーバー側の共通の出品の門');

// 楽天に書く道は miniPC (WAREHOUSE_URL) を呼ぶ = fetch を数える (この試験の HTTP は通す)
process.env.WAREHOUSE_URL = 'http://warehouse.invalid';
process.env.CF_ACCESS_CLIENT_ID = 'x'; process.env.CF_ACCESS_CLIENT_SECRET = 'x'; process.env.WAREHOUSE_SERVICE_TOKEN = 'x';
const realFetch = globalThis.fetch;
const rmsCalls = [];
globalThis.fetch = async (url, opts = {}) => {
  if (String(url).startsWith('http://warehouse.invalid')) {
    rmsCalls.push(`${opts.method || 'GET'} ${String(url).replace('http://warehouse.invalid', '')}`);
    return new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } });
  }
  return realFetch(url, opts);
};
LG.__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open', changed_at: '2030-01-01', changed_by: 'x' }));
const app = express();
app.set('view engine', 'ejs');
app.use(express.json());
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'admin' ? { authenticated: true, email: 'admin@test', role: 'admin', allowedApps: ['product-hub'] }
    : { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['product-hub'] };
  next();
});
const PHR = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', PHR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'editor' } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session, Origin: ORIGIN };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
function checkScripts(html) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  assert.equal(all.length, [...html.matchAll(/<script\b/gi)].length, 'script の開きと閉じの数が合わない');
  for (const m of all) {
    if (/type="application\/json"/.test(m[1])) { JSON.parse(m[2]); continue; }
    if (/\bsrc=/.test(m[1])) continue;
    new vm.Script(m[2]);
    assert.ok(!/<%|%>/.test(m[2]), 'EJS のタグが JS に残っている');
  }
}
// 出品・展開の番に進めた形 (ボードからの出品の工程の確かめを越える = 門が止めるかを見る)
const toListingStep = (id) => {
  ph.prepare(`UPDATE draft_step_progress SET state = 'done' WHERE draft_id = ? AND step_code IN (SELECT code FROM ph_steps WHERE track = 'main' AND code <> 'listing')`).run(id);
};

await ta('[L1] 2 軸 / NE の写し待ちのまとまり: 詳細画面の「公開で登録」・ボードからの出品・確認済みの再実行・プレビュー・今あるページの SKU・公開 が全部止まる (楽天に 1 回も書かない)', async () => {
  // NE の写し待ちにする (grp1 の子 s001 を NE から外す)
  unmirror('s001');
  assert.equal(GATE.cdbGroupCardView(ph, g1.id).state, 'ne_waiting');
  await call('GET', `/apps/product-hub/detail/${hk.id}`);   // 工程の行を作る (表示の自己修復)
  await call('GET', `/apps/product-hub/detail/${g1.id}`);
  for (const [id, re] of [[hk.id, /2 軸/], [g1.id, /NE の写し待ち/]]) {
    toListingStep(id);
    ph.prepare(`INSERT INTO ph_cdb_group_events (event_id, cdb_group_product_id, revision, group_code, outcome) VALUES (?, 0, 0, 'x', 'stale')`).run(uuid());   // 記録の表は門に関係しない
    rmsCalls.length = 0;
    let r = await call('POST', `/apps/product-hub/api/drafts/${id}/rakuten/register`, { body: { confirm: true } });
    assert.equal(r.status, 400, r.text.slice(0, 200)); assert.match(r.j.error, re);
    r = await call('POST', `/apps/product-hub/api/drafts/${id}/rakuten/list-from-board`, { body: { confirm: true } });
    assert.equal(r.status, 400, r.text.slice(0, 200)); assert.match(r.j.error, re);
    // 結果不明の後の「確認済みで再実行」(管理者) も通さない
    ph.prepare(`INSERT INTO draft_rakuten (draft_id, listing_outcome) VALUES (?, 'unknown') ON CONFLICT(draft_id) DO UPDATE SET listing_outcome = 'unknown'`).run(id);
    r = await call('POST', `/apps/product-hub/api/drafts/${id}/rakuten/list-from-board`, { body: { confirm: true, force_unknown: true }, session: 'admin' });
    assert.equal(r.status, 400, r.text.slice(0, 200)); assert.match(r.j.error, re);
    r = await call('GET', `/apps/product-hub/api/drafts/${id}/rakuten/preview`);
    assert.equal(r.j.ok, false); assert.ok(r.j.reasons.some((x) => re.test(x)), JSON.stringify(r.j.reasons));
    // 今ある楽天のページの SKU に書く (sku-images の PATCH)
    ph.prepare(`INSERT OR IGNORE INTO draft_sku_images (draft_id, sku_code, drive_file_id, cabinet_location) VALUES (?, 'x-sku', 'f1', '/x.jpg')`).run(id);
    r = await call('POST', `/apps/product-hub/api/drafts/${id}/rakuten/sync-sku-images`, { body: {} });
    assert.equal(r.status, 400, r.text.slice(0, 200)); assert.match(r.j.error, re);
    // 公開に切り替える (登録済みのページ) = 止める・非公開にするのは止めない
    ph.prepare(`UPDATE draft_rakuten SET registered_at = '2026-10-01T00:00:00Z', listing_outcome = NULL WHERE draft_id = ?`).run(id);
    r = await call('POST', `/apps/product-hub/api/drafts/${id}/rakuten/visibility`, { body: { confirm: true, hide: false } });
    assert.equal(r.status, 400, r.text.slice(0, 200)); assert.match(r.j.error, re);
    assert.deepEqual(rmsCalls, [], `楽天 (miniPC) に書いた: ${rmsCalls.join(', ')}`);
    r = await call('POST', `/apps/product-hub/api/drafts/${id}/rakuten/visibility`, { body: { confirm: true, hide: true } });
    assert.equal(r.status, 200, r.text.slice(0, 200));
    assert.equal(rmsCalls.length, 1); assert.match(rmsCalls[0], /^POST .*\/visibility$/);
    ph.prepare('UPDATE draft_rakuten SET registered_at = NULL, published_at = NULL WHERE draft_id = ?').run(id);
    // 関数を直に呼んでも同じ (router を通らない呼び手)
    rmsCalls.length = 0;
    const reg2 = await RL.registerItem(id, { actor: 't' });
    assert.equal(reg2.ok, false); assert.match(reg2.reasons[0], re);
    assert.throws(() => BL.assertRakutenListable(ph, { id, ne_code: 'x' }, { forceUnknown: true }), re);
    assert.ok(RL.buildItemPayload(ph, id, { tax: { mode: 'legacy' } }).reasons.some((x) => re.test(x)));
    assert.deepEqual(rmsCalls, []);
  }
});

await ta('[L2] NE と一致した 1 軸のまとまり・まとまりでないカード = 門は何も言わない (ほかの門 = 画像・ジャンルなどはそのまま)', async () => {
  mirror('s001', 'grp1');
  assert.equal(GATE.cdbGroupListingBlock(ph, g1.id), null);
  const built = RL.buildItemPayload(ph, g1.id, { tax: { mode: 'legacy' } });
  assert.ok(!built.reasons.some((x) => /NE の写し待ち|2 軸/.test(x)), JSON.stringify(built.reasons));
  assert.ok(built.reasons.some((x) => /ジャンルID/.test(x)), 'ほかの門が効いていない');
  const single1 = draftOf('ph-single-1');
  assert.ok(!RL.buildItemPayload(ph, single1.id, { tax: { mode: 'legacy' } }).reasons.some((x) => /NE の写し待ち|2 軸/.test(x)));
  assert.doesNotThrow(() => BL.assertRakutenListable(ph, single1));
  // 有効な子が 0 (全部廃止) = 止める
  ph.prepare('UPDATE ph_cdb_group_children SET active = 0 WHERE draft_id = ?').run(g1.id);
  assert.equal(GATE.cdbGroupListingBlock(ph, g1.id).code, 'no_children');
  ph.prepare(`UPDATE ph_cdb_group_children SET active = 1 WHERE draft_id = ? AND inactive_reason IS NULL`).run(g1.id);
  assert.equal(GATE.cdbGroupListingBlock(ph, g1.id), null);
});

await ta('[L3] 楽天に書く道 (miniPC の items/manage-numbers に PUT / PATCH / POST) は全部、共通の門 cdbGroupListingBlock を通す (ソースで数える・店舗内カテゴリの付け替えだけ理由つきで外す)', async () => {
  const src = fs.readFileSync(new URL('../apps/product-hub/services/rakuten-listing.js', import.meta.url), 'utf8');
  const fns = [...src.matchAll(/^export (?:async )?function (\w+)\(/gm)].map((m, i, a) => ({ name: m[1], body: src.slice(m.index, a[i + 1]?.index ?? src.length) }));
  const writers = fns.filter((f) => f.body.includes('callWarehouse(') && f.body.includes('manage-numbers') && /method: '(PUT|PATCH|POST)'/.test(f.body));
  const EXEMPT = { syncShopCategoriesToRms: '店舗内カテゴリ (お店の棚) の付け替え = 登録済みのページの棚だけ (SKU も公開も変えない)' };
  assert.deepEqual(writers.map((f) => f.name).sort(), ['registerItem', 'setItemVisibility', 'syncShopCategoriesToRms', 'syncSkuImagesToRms'], '楽天に書く道が増えた / 減った = 門を見直す');
  for (const f of writers) {
    if (EXEMPT[f.name]) continue;
    const gate = f.body.indexOf('cdbGroupListingBlock(');
    const firstCall = f.body.search(/await (callWarehouse|fetchGenreAttributes|refreshDriveModifiedTimes)\(/);
    assert.ok(gate >= 0 && (firstCall < 0 || gate < firstCall), `${f.name}: 外への呼び出しの前に共通の門を通していない`);
  }
  const bp2 = fns.find((f) => f.name === 'buildItemPayload');
  assert.ok(bp2.body.includes('cdbGroupListingBlock('), 'buildItemPayload (プレビュー・登録) に門が無い');
  const bl = fs.readFileSync(new URL('../apps/product-hub/services/board-listing.js', import.meta.url), 'utf8');
  const assertFn = bl.slice(bl.indexOf('export function assertRakutenListable'), bl.indexOf('export function rememberUnknownOutcome'));
  assert.ok(assertFn.includes('cdbGroupListingBlock('), 'assertRakutenListable (詳細画面とボードの両方) に門が無い');
});

console.log('\n画面 (router)');

await ta('[H1] ボードの札 (2 軸: 出品停止・NE の写し待ち・要確認)・詳細のまとまり (暫定の正本の子・無効の子・要確認・確かめた)・画面の JS が読める', async () => {
  unmirror('s001');
  const b = await call('GET', '/apps/product-hub/board');
  assert.equal(b.status, 200, b.text.slice(0, 300));
  checkScripts(b.text);
  assert.match(b.text, /2 軸: 出品停止/);
  assert.match(b.text, /NE の写し待ち/);
  assert.match(b.text, /⚠ まとまり 要確認/);
  let d = await call('GET', `/apps/product-hub/detail/${g1.id}`);
  assert.equal(d.status, 200, d.text.slice(0, 300));
  checkScripts(d.text);
  assert.match(d.text, /id="cdb-group-card"[^>]*data-state="ne_waiting"/);
  assert.match(d.text, /NE にまだ無い子: s001/);
  assert.match(d.text, /<tr data-code="s001">/);
  assert.match(d.text, /無効にした子 \(出品・NE との比べには使いません\): s003 \(廃止\)/);
  const att = GATE.cdbGroupCardView(ph, g1.id).attention;
  assert.ok(att);
  // 見ていた文と違う = 消さない (その間に新しい知らせ)
  let r = await call('POST', `/apps/product-hub/api/drafts/${g1.id}/cdb-group/ack`, { body: { expected: '違う文' } });
  assert.equal(r.status, 409);
  assert.equal(GATE.cdbGroupCardView(ph, g1.id).attention, att);
  r = await call('POST', `/apps/product-hub/api/drafts/${g1.id}/cdb-group/ack`, { body: { expected: att } });
  assert.deepEqual([r.status, r.j.cleared], [200, true]);
  assert.equal(GATE.cdbGroupCardView(ph, g1.id).attention, null);
  d = await call('GET', `/apps/product-hub/detail/${g1.id}`);
  assert.ok(!d.text.includes('id="cdb-group-ack-btn"'));
  // 2 軸の詳細 = 選択肢の列 (名前)
  d = await call('GET', `/apps/product-hub/detail/${hk.id}`);
  assert.match(d.text, /data-state="two_axes"/);
  assert.match(d.text, /横 = カラー \/ 縦 = サイズ/);
  assert.match(d.text, /オフホワイト \/ 90cm/);
  // まとまりでないカード = まとまりの欄を出さない・「確かめた」はまとまりのカードだけ
  const s1 = draftOf('ph-single-1');
  d = await call('GET', `/apps/product-hub/detail/${s1.id}`);
  assert.ok(!d.text.includes('id="cdb-group-card"'));
  assert.equal((await call('POST', `/apps/product-hub/api/drafts/${s1.id}/cdb-group/ack`, { body: {} })).status, 400);
});

await ta('[H2] ボードを開いたときの取り込み (MASTER_EDIT_OPEN = 1・Render): SKU とまとまりの知らせを同じ接続で・まとまりの側が落ちても SKU の側は done', async () => {
  process.env.MASTER_EDIT_OPEN = '1';
  process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/test';
  process.env.PORTAL_VARIANT = 'render';
  let failGroup = false;
  O.__setCompanyDbClientFactory(async () => {
    await pg.query('set role master_edit');
    return { query: async (t, p) => { if (failGroup && /claim_outbox_events/.test(t)) throw Object.assign(new Error('permission denied for function claim_outbox_events'), { code: '42501' }); return pg.query(t, p); },
      end: async () => { await pg.query('set role deploy'); }, on: () => {} };
  });
  try {
    const a = await reg('ph-single-2', single({ name: 'ボードで作る単品' }));
    await cancelChild('Hakama-BK-90');
    const ge = await enqueue(hakama, 3, await snap(hakama, 3, { axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
      options: [{ axis: 1, code: '-WH', name: 'オフホワイト', sort: 0 }, { axis: 2, code: '-90', name: '90cm', sort: 0 }], choices: { 'Hakama-WH-90': { 1: '-WH', 2: '-90' } } }));
    let b = await call('GET', '/apps/product-hub/board');
    assert.equal(b.status, 200);
    assert.deepEqual([(await outboxEv(a.card.event_id)).status, (await outboxEv(ge)).status], ['done', 'done']);
    assert.ok(draftOf('ph-single-2'));
    assert.deepEqual(kidsOf(hk.id).map((k) => [k.code, k.active]), [['Hakama-BK-90', 0], ['Hakama-WH-90', 1]]);
    // 選択肢 -BK はスナップショットから消えた = 無効の印 (消さない)
    assert.deepEqual(ph.prepare('SELECT code, active FROM ph_cdb_group_options WHERE draft_id = ? ORDER BY axis, code_key').all(hk.id).map((o) => [o.code, o.active]), [['-BK', 0], ['-WH', 1], ['-90', 1]]);
    // NE の写しの子 hakama-bk-90 (廃止した子) が代表つきで残っている = NE にだけある = 止める側
    assert.deepEqual(GATE.cdbGroupCardView(ph, hk.id).sync.extra, ['hakama-bk-90']);
    // まとまりの側が落ちる (ロールの流し直しの前の形) = SKU の側は今までどおり
    failGroup = true;
    const c2 = await reg('ph-single-3', single({ name: 'まとまりの側が落ちても' }));
    const sw = await O.sweepCardOutbox(applyCard, { applyGroup });
    assert.equal(sw.ok, true);
    assert.deepEqual(sw.results.map((r) => [r.sku_id, r.status]), [[c2.sku_id, 'done']]);
    assert.match(sw.groupError, /permission denied/);
    b = await call('GET', '/apps/product-hub/board');
    assert.equal(b.status, 200);
    // applyGroup を渡さない呼び手 (今までの形) = SKU だけ
    failGroup = false;
    const c3 = await reg('ph-single-4', single({ name: '今までの呼び方' }));
    const sw2 = await O.sweepCardOutbox(applyCard);
    assert.deepEqual([sw2.results.map((r) => r.sku_id), sw2.groups], [[c3.sku_id], []]);
  } finally {
    delete process.env.MASTER_EDIT_OPEN;
    O.__setCompanyDbClientFactory(null);
  }
});

server.close();
globalThis.fetch = realFetch;
try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows で開いたままのことがある */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
