/**
 * test-master-outbox-ns.mjs — product-hub の知らせ (ops.product_hub_outbox) の名前空間 (0066・Company DB構想 20 v7 §⑤ / §⑩ の PR-3)
 *
 * Company DB = PGlite (Render と同じ持ち主のロール deploy で migration)。product-hub = 一時の DATA_DIR の SQLite (本物の取り込み cdb-card-intake.js)。
 * 🚨 移行の間の契約: 0065 まで流した DB で今の単品の登録 → カードの知らせ (pending / done / conflict) を作り、その後で 0066 を流して、
 *    同じ DB で今までどおり (今の登録の関数・今の借りる関数 ops.claim_card_events・lib の runCardOutbox / linkCardToExisting) 動くことを確かめる。
 * 固定する契約:
 *   M 移行: 前からの行は entity_kind = sku・entity_id = sku_id・revision = 1 (中身・状態・hash は同じ)・済んだ知らせは変えない守りのまま・
 *     0066 の後の今の登録の insert もそのまま通る・今の取り込みで pending → done・conflict → 人が結ぶ → done・SKU の知らせは revision 1 だけ・(SKU, card_create) は 1 つ
 *   B 関数の本文: 0066 の claim_card_events / guard_product_hub_outbox / guard_product_hub_outbox_session は 0052 (= 最新の版) の本文に決めた行を足しただけ
 *   G まとまりの知らせ (group_snapshot / ph-group-v1): 今の借りる関数・今の取り込みは借りない・名前空間つきの関数 (entity_kind 必須) で借りる・
 *     revision は増えるだけ (同じ / 小さい = 断る・続いていなくてよい)・hash と作った人は DB が確かめる・形と今の Company DB の値 (子の全部・廃止した子・コード・名前・売価・JAN・
 *     札か単品の代表か) が違えば断る・書き方によらず (部品を通らない持ち主の insert も) 同じ決まり
 *   J 形の決まり: lib の groupSnapshotShapeProblem と DB の ops.group_snapshot_shape_problem が同じ見本に同じ答え
 *   P 権限: 画面のロールは部品・確かめを実行できない・知らせを直接足せない・名前空間つきで借りる関数は実行できる・
 *     画面のロールの取引でまとまりの知らせは書けない (PR-5 まで)・security definer の関数は search_path 固定・public の実行権なし
 * 使い方: node scripts/test-master-outbox-ns.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vg3-outbox-ns-'));
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
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const PH = await import('../apps/product-hub/services/cdb-card-intake.js');

const MIG_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'db', 'company', 'migrations');
let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const uuid = () => crypto.randomUUID();
const pgCode = async (p) => { try { await p; } catch (e) { return e.code ?? String(e.message); } return null; };

const pg = new PGlite();
const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
// 🚨 0065 まで (= 0066 の前の本番の形) で始める
await applyMigrations(db, { log: quiet, to: '0065' });
assert.equal((await pg.query(`select count(*)::int as n from ops.schema_migrations where version = '0066'`)).rows[0].n, 0);
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});   // 0066 の前に流しても止まらない (関数が無い DB では付けない)
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
const manualStopped = () => [{ id: 'ne.product_screen', by: 'naka@test', at: new Date().toISOString() }];
const drain = () => ({ done: true, checked_by: 'naka@test', checked_at: new Date().toISOString() });
const BUILDS = { render: ['r1'], minipc: ['m1'] };
const LEGACY_HASH = C.ownershipHash(MASTER_OWNERSHIP);
async function toPhase(to) {
  const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
  if (to !== 'frozen') await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(db, ALL_COMPANY);
  const own = to === 'frozen' ? MASTER_OWNERSHIP : ALL_COMPANY;
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) {
    await asGate(host, () => C.recordLegacyGateAck(db, { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
  }
  const mh = await C.manifestHashOf(db, MANIFEST);
  const evidence = to === 'frozen' ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: manualStopped(), drain: drain() }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL_COMPANY) };
  const r = await asRole('master_ops', () => C.advanceCutoverPhase(db, { to, actor: 'naka@test', evidence }));
  if (to === 'new_open') await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(db, { withSet: true });
  return r;
}

// 夜間ロード: 札のまとまり grp1 (子 s001・s003) と 単品の代表のまとまり (代表 s002・子 s004)
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
const plan = {
  skus: [sku('s001', '単品 1【赤】', { representativeCode: 'grp1', representativeState: 'value', standardPriceJpy: 1980 }), sku('s002', '代表の単品'),
    sku('s003', '単品 3【青】', { representativeCode: 'grp1', representativeState: 'value' }), sku('s004', '代表の子', { representativeCode: 's002', representativeState: 'value' })],
  variationGroups: [{ code: 'grp1', name: '札のまとまり', childCodes: ['s001', 's003'], status: 'active' }, { code: 's002', name: '単品の代表', childCodes: ['s004'], status: 'active' }],
  setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_test' },
};
const loadR = await runInitialLoad(db, plan, { log: quiet, runId: 'load_vg3', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
assert.equal(loadR.ok, true, loadR.error);
const skuId = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code]))?.id;
const grp1 = (await one(`select product_id::text as id from core.products where display_code = 'grp1' and not exists (select 1 from core.skus k where k.product_id = core.products.product_id)`)).id;
const rep2 = (await one(`select product_id::text as id from core.skus where code = 's002'`)).id;
assert.equal((await one(`select p.parent_product_id::text as par from core.products p join core.skus k on k.product_id = p.product_id where k.code = 's001'`)).par, grp1);
assert.equal((await one(`select p.parent_product_id::text as par from core.products p join core.skus k on k.product_id = p.product_id where k.code = 's004'`)).par, rep2);

// 切替 (frozen → company_owner → backfill → new_open) = 今の本番と同じ「新商品の登録が開いている」形
await toPhase('frozen');
await toPhase('company_owner');
const bp = await one('select * from ops.registration_backfill_plan()');
await asRole('master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3, $4)', [bp.sku_count, bp.snapshot_hash, 'naka@test', '試験']));
await toPhase('new_open');

const single = (over = {}) => ({ name: '新しい単品', standard_price: '1,980', shipping_code: 'S01', tax_rate: '10', primary_supplier: '1', sales_class: '3', expiry_managed: '0', reorder_months: '1', ...over });
const reg = (code, values = single(), card = {}) => asEditor(() => R.registerNewSku(db, { actor: 'Naka@Test', requestId: uuid(), kind: 'single', code, reason: null, values, card },
  { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES }));
const ph = PHDB.getDB();
ph.prepare(`INSERT INTO ph_shipping_method_map (ne_label, rakuten_group) VALUES ('ゆうパケット', '9')`).run();
const apply = (ev) => PH.applyCdbCardEvent(ev);
const runCards = (opts) => asEditor(() => O.runCardOutbox(db, apply, opts));
const draftOf = (code) => ph.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code);
const outboxOf = async (code) => one(`select o.* , o.event_id::text as eid, o.sku_id::text as sid from ops.product_hub_outbox o join core.skus k on k.sku_id = o.sku_id where k.code = $1`, [code]);

// ── 0066 の前: 今の登録 → カードの知らせ (pending / done / conflict) ──
const preA1 = await reg('mig-a1', single({ name: 'まだ取り込まない' }), { official_url: 'https://maker.example/a1' });
const preA2 = await reg('mig-a2', single({ name: '取り込んだ' }));
assert.equal((await runCards({ eventId: preA2.card.event_id }))[0].status, 'done');
const preA3 = await reg('mig-a3', single({ name: '衝突する' }));
ph.prepare(`INSERT INTO product_drafts (ne_code, name, price, created_by) VALUES ('mig-a3', '前からのカード', 500, 'someone')`).run();
assert.equal((await runCards({ eventId: preA3.card.event_id }))[0].status, 'conflict');
const COLS_0052 = ['event_id', 'company_id', 'sku_id', 'kind', 'schema_version', 'payload', 'payload_hash', 'request_id', 'created_by', 'created_at', 'status', 'attempts',
  'last_error', 'lease_owner', 'leased_until', 'result', 'done_at', 'updated_at'];
const before = await q(`select ${COLS_0052.map((c) => `o.${c}::text as ${c}`).join(', ')} from ops.product_hub_outbox o order by o.event_id`);
assert.equal(before.length, 3);

// ── 0066 を流す (+ ロールの流し直し) ──
await applyMigrations(db, { log: quiet });
assert.equal((await one(`select count(*)::int as n from ops.schema_migrations where version = '0066'`)).n, 1);
await createMasterEditRoles(pg, {});

console.log('移行 (0066 の前の知らせ・今の登録と取り込み)');

await ta('[M1] 前からの行 = entity_kind sku・entity_id = sku_id・revision 1・group_product_id なし。0052 の列は全部そのまま (中身・状態・結果・借り)', async () => {
  const after = await q(`select ${COLS_0052.map((c) => `o.${c}::text as ${c}`).join(', ')} from ops.product_hub_outbox o order by o.event_id`);
  assert.deepEqual(after, before);
  const ns = await q('select entity_kind, entity_id::text as eid, sku_id::text as sid, revision, group_product_id from ops.product_hub_outbox order by event_id');
  assert.equal(ns.length, 3);
  for (const r of ns) assert.deepEqual([r.entity_kind, r.eid, r.revision, r.group_product_id], ['sku', r.sid, 1, null]);
  assert.deepEqual((await q('select status from ops.product_hub_outbox order by created_at')).map((r) => r.status), ['pending', 'done', 'conflict']);
});

await ta('[M2] 表の形: entity_kind / entity_id は生成の列・一意は (entity_kind, entity_id, kind, revision) だけ (前の (sku_id, kind) は無い)・SKU の知らせは revision 1 だけ・相手はどちらか一方', async () => {
  const cols = Object.fromEntries((await q(`select column_name as c, is_generated as g, is_nullable as n, column_default as d from information_schema.columns where table_schema = 'ops' and table_name = 'product_hub_outbox'`)).map((r) => [r.c, r]));
  assert.deepEqual([cols.entity_kind.g, cols.entity_id.g, cols.revision.g, cols.revision.d, cols.sku_id.n, cols.group_product_id.n], ['ALWAYS', 'ALWAYS', 'NEVER', '1', 'YES', 'YES']);
  const uq = (await q(`select conname, pg_get_constraintdef(oid) as d from pg_constraint where conrelid = 'ops.product_hub_outbox'::regclass and contype = 'u' order by 1`));
  assert.deepEqual(uq, [{ conname: 'ux_pho_entity', d: 'UNIQUE (entity_kind, entity_id, kind, revision)' }]);
  const a2 = await outboxOf('mig-a2');
  const ins = (cols2, vals) => tx(() => pg.query(`insert into ops.product_hub_outbox (company_id, ${cols2}, kind, schema_version, payload, payload_hash, request_id, created_by) values (1, ${vals}, '{}'::jsonb, repeat('a', 64), gen_random_uuid(), 'x')`));
  // 同じ SKU のカードの知らせを 2 つ = 一意 (今の (sku_id, kind) と同じ意味)
  assert.equal(await pgCode(ins('sku_id', `${a2.sid}, 'card_create', 'ph-card-v1'`)), '23505');
  // SKU の知らせの revision は 1 だけ
  assert.equal(await pgCode(ins('sku_id, revision', `${a2.sid}, 2, 'card_create', 'ph-card-v1'`)), '23514');
  // 相手はどちらか一方 / 種類と版は相手に合う
  assert.equal(await pgCode(ins('sku_id', `${a2.sid}, 'group_snapshot', 'ph-group-v1'`)), '23514');
  assert.equal(await pgCode(ins('sku_id, group_product_id', `${a2.sid}, ${grp1}, 'card_create', 'ph-card-v1'`)), '22023');
  // まとまりの知らせ = 先に insert の trigger (形・hash) が断る (22023)。trigger を止めても CHECK が断る (23514)
  assert.equal(await pgCode(ins('group_product_id', `${grp1}, 'card_create', 'ph-card-v1'`)), '22023');
  await pg.query('alter table ops.product_hub_outbox disable trigger trg_product_hub_outbox_group');
  try {
    assert.equal(await pgCode(ins('sku_id, group_product_id', `${a2.sid}, ${grp1}, 'card_create', 'ph-card-v1'`)), '23514');
    assert.equal(await pgCode(ins('group_product_id', `${grp1}, 'card_create', 'ph-card-v1'`)), '23514');
    assert.equal(await pgCode(ins('group_product_id', `${grp1}, 'group_snapshot', 'ph-card-v1'`)), '23514');
  } finally { await pg.query('alter table ops.product_hub_outbox enable trigger trg_product_hub_outbox_group'); }
  assert.equal(await pgCode(tx(() => pg.query(`insert into ops.product_hub_outbox (company_id, kind, schema_version, payload, payload_hash, request_id, created_by) values (1, 'card_create', 'ph-card-v1', '{}', repeat('a', 64), gen_random_uuid(), 'x')`))), '23514');
  // 生成の列は書けない
  assert.equal(await pgCode(tx(() => pg.query(`update ops.product_hub_outbox set entity_id = 1 where event_id = $1`, [a2.eid]))), '428C9');
  // 済んだ知らせ・中身 (revision・まとまりの欄も) は変えない
  await assert.rejects(() => pg.query(`update ops.product_hub_outbox set status = 'pending', done_at = null where event_id = $1`, [a2.eid]), /済んだ/);
  const a1 = await outboxOf('mig-a1');
  await assert.rejects(() => pg.query(`update ops.product_hub_outbox set revision = 1 + 0 * revision, payload = '{}' where event_id = $1`, [a1.eid]), /変えない/);
  await assert.rejects(() => tx(() => pg.query(`update ops.product_hub_outbox set sku_id = null, group_product_id = $2 where event_id = $1`, [a1.eid, grp1])), /変えない/);
});

await ta('[M3] 0066 の後も今の取り込みで pending (0066 の前の知らせ) → done・カード 1 枚 (cdb_sku_id)', async () => {
  const res = await runCards({});
  assert.deepEqual(res.map((r) => [r.sku_id, r.status]), [[preA1.sku_id, 'done']]);
  assert.equal(String(draftOf('mig-a1').cdb_sku_id), preA1.sku_id);
  assert.equal(draftOf('mig-a1').official_url, 'https://maker.example/a1');
  assert.equal((await outboxOf('mig-a1')).status, 'done');
});

await ta('[M4] 0066 の後の今の登録: 登録の関数の insert がそのまま通る (entity sku・revision 1)・保存の直後の取り込み → done・2 回目も 1 枚', async () => {
  const r = await reg('mig-a4', single({ name: '0066 の後の単品' }), { asin: 'b0abcdefgh' });
  assert.equal(r.card.status, 'pending');
  const ob = await outboxOf('mig-a4');
  assert.deepEqual([ob.entity_kind, String(ob.entity_id), ob.revision, ob.group_product_id, ob.kind, ob.schema_version], ['sku', r.sku_id, 1, null, 'card_create', 'ph-card-v1']);
  assert.equal(ob.payload_hash, O.cardPayloadHash(ob.payload));
  assert.equal((await runCards({ skuId: r.sku_id }))[0].status, 'done');
  assert.equal((await runCards({ skuId: r.sku_id, manual: true })).length, 0);
  assert.equal(ph.prepare('SELECT COUNT(*) AS n FROM product_drafts WHERE cdb_sku_id = ?').get(Number(r.sku_id)).n, 1);
  assert.equal(draftOf('mig-a4').asin, 'B0ABCDEFGH');
});

await ta('[M5] 0066 の前の衝突 (conflict) = 人が既存のカードに結ぶ (今の linkCardToExisting) → done linked', async () => {
  const old = draftOf('mig-a3');
  const out = await asEditor(() => O.linkCardToExisting(db, (ev, o) => PH.linkCdbCardToExisting(ev, o), { skuId: preA3.sku_id, actor: 'naka@test', expectedDraftId: old.id }));
  assert.deepEqual([out.ok, out.draft_id], [true, old.id]);
  const ob = await outboxOf('mig-a3');
  assert.deepEqual([ob.status, ob.result.outcome], ['done', 'linked']);
  assert.equal(String(draftOf('mig-a3').cdb_sku_id), preA3.sku_id);
});

console.log('\n関数の本文 (0052 = 最新の版に決めた行を足しただけ)');

const migText = (file) => fs.readFileSync(path.join(MIG_DIR, file), 'utf-8').replace(/\r\n/g, '\n');
const migFiles = fs.readdirSync(MIG_DIR).filter((f) => /^\d{4}_.*\.sql$/.test(f)).sort();
/** migration の文から関数の定義 (create [or replace] function <名前>( … から $$; まで) を取り出す */
function fnDef(file, name) {
  const t = migText(file);
  const re = new RegExp(`create (or replace )?function ${name.replace('.', '\\.')}\\(`, 'g');
  const m = [...t.matchAll(re)];
  assert.equal(m.length, 1, `${file} の ${name} が 1 つでない (${m.length})`);
  const end = t.indexOf('$$;', m[0].index);
  return t.slice(m[0].index, end + 3).replace(/^create or replace function/, 'create function');
}
/** 行の差 (最長の共通の並び = LCS。同じ行が何度あっても数える) */
const lineDiff = (a, b) => {
  const A = a.split('\n'); const B = b.split('\n');
  const L = Array.from({ length: A.length + 1 }, () => new Array(B.length + 1).fill(0));
  for (let i = A.length - 1; i >= 0; i--) for (let j = B.length - 1; j >= 0; j--) L[i][j] = A[i] === B[j] ? L[i + 1][j + 1] + 1 : Math.max(L[i + 1][j], L[i][j + 1]);
  const removed = []; const added = [];
  let i = 0; let j = 0;
  while (i < A.length && j < B.length) {
    if (A[i] === B[j]) { i++; j++; } else if (L[i + 1][j] >= L[i][j + 1]) removed.push(A[i++]); else added.push(B[j++]);
  }
  return { removed: [...removed, ...A.slice(i)], added: [...added, ...B.slice(j)] };
};
await ta('[B1] 0052 の後に claim_card_events / guard_product_hub_outbox / guard_product_hub_outbox_session を置き換えた migration は 0066 だけ (= 0052 が最新の版)', async () => {
  for (const name of ['ops.claim_card_events', 'ops.guard_product_hub_outbox', 'ops.guard_product_hub_outbox_session', 'ops.finish_card_event']) {
    const hits = migFiles.filter((f) => new RegExp(`create (or replace )?function ${name.replace('.', '\\.')}\\(`).test(migText(f)));
    // 🆕 0067 (PR-5) が guard_product_hub_outbox_session を 0066 の版から置き換えた (まとまりの約束で開く) = その中身は scripts/test-master-variation.mjs の [S2] が 0066 との差で確かめる
    const want = name === 'ops.finish_card_event' ? ['0052_master_registrations.sql']
      : name === 'ops.guard_product_hub_outbox_session' ? ['0052_master_registrations.sql', '0066_product_hub_outbox_namespace.sql', '0067_variation_groups.sql']
        : ['0052_master_registrations.sql', '0066_product_hub_outbox_namespace.sql'];
    assert.deepEqual(hits, want, name);
  }
});
await ta('[B2] claim_card_events = 0052 の本文に「and o.entity_kind = \'sku\'」の 1 行を足しただけ・DB の本文も 0066 のもの', async () => {
  const d = lineDiff(fnDef('0052_master_registrations.sql', 'ops.claim_card_events'), fnDef('0066_product_hub_outbox_namespace.sql', 'ops.claim_card_events'));
  assert.deepEqual(d, { removed: [], added: ["       and o.entity_kind = 'sku'"] });
  assert.match((await one(`select prosrc from pg_proc where oid = 'ops.claim_card_events(text, text, uuid, bigint, integer, integer, integer)'::regprocedure`)).prosrc, /o\.entity_kind = 'sku'/);
});
await ta('[B3] guard_product_hub_outbox = 0052 の本文の変えない列に group_product_id と revision を足しただけ', async () => {
  const a = fnDef('0052_master_registrations.sql', 'ops.guard_product_hub_outbox'); const b = fnDef('0066_product_hub_outbox_namespace.sql', 'ops.guard_product_hub_outbox');
  const d = lineDiff(a, b);
  assert.equal(d.removed.length, 2); assert.equal(d.added.length, 2);
  for (let i = 0; i < 2; i++) {
    const tail = i === 0 ? 'new' : 'old';
    assert.equal(d.added[i], d.removed[i].replace(`${tail}.created_at)`, `${tail}.created_at, ${tail}.group_product_id, ${tail}.revision)`));
  }
});
await ta('[B4] guard_product_hub_outbox_session = 0052 の本文に、まとまりの知らせを断る 3 行を足しただけ', async () => {
  const d = lineDiff(fnDef('0052_master_registrations.sql', 'ops.guard_product_hub_outbox_session'), fnDef('0066_product_hub_outbox_namespace.sql', 'ops.guard_product_hub_outbox_session'));
  assert.deepEqual(d.removed, []);
  assert.deepEqual(d.added, ['  if new.group_product_id is not null then',
    "    raise exception 'group_snapshot_session_required: まとまりの知らせは、まとまりの約束の関数 (PR-5) の中だけで書く' using errcode = '42501';", '  end if;']);
});
await ta('[B5] claim_outbox_events = claim_card_events (0066) と同じ状態の決まり (違うのは entity_kind の引数・絞り・戻りの列だけ)', async () => {
  const a = fnDef('0066_product_hub_outbox_namespace.sql', 'ops.claim_card_events'); const b = fnDef('0066_product_hub_outbox_namespace.sql', 'ops.claim_outbox_events');
  const d = lineDiff(a, b);
  // 借りる本体 (with c as ( … から from c where まで) は、相手の絞り (entity_kind / sku_id / entity_id) の行の外は同じ
  const body = (s) => { const L = s.split('\n'); return L.slice(L.findIndex((l) => l.includes('with c as (')), L.findIndex((l) => l.includes('from c where')) + 1)
    .filter((l) => !/entity_kind|p_sku_id|p_entity_id/.test(l)); };
  assert.ok(body(a).length >= 10, String(body(a).length));
  assert.deepEqual(body(b), body(a));
  assert.ok(d.added.some((l) => l.includes("o.entity_kind = p_entity_kind")));
  assert.ok(d.added.some((l) => l.includes("p_entity_kind not in ('sku', 'variation_group')")));
});

console.log('\nまとまりの知らせ (group_snapshot / ph-group-v1)');

/** 今の Company DB の値から作ったまとまりの完全なスナップショット (PR-5 の関数が作る形と同じ) */
async function snap(groupPid, revision, over = {}) {
  const g = await one(`select p.product_id::text as pid, p.display_code, p.name, k.sku_id::text as rep_sku, k.code as rep_code
    from core.products p left join core.skus k on k.product_id = p.product_id and k.sku_kind = 'single' where p.product_id = $1`, [groupPid]);
  const kids = await q(`select s.sku_id::text as sku_id, s.code, s.name, s.standard_price_jpy::text as price, r.state,
      coalesce((select array_agg(e.external_value order by e.external_value) from core.external_ids e where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.valid_to is null), '{}') as jans
    from core.products c join core.skus s on s.product_id = c.product_id and s.sku_kind = 'single' left join ops.master_registrations r on r.sku_id = s.sku_id
    where c.parent_product_id = $1 order by s.sku_id`, [groupPid]);
  return {
    schema: 'ph-group-v1', revision, created_by: 'naka@test',
    group: { product_id: g.pid, sku_id: g.rep_sku ?? null, code: g.rep_sku ? g.rep_code : g.display_code, name: g.name, kind: g.rep_sku ? 'single' : 'tag' },
    axes: [], options: [],
    children: kids.filter((k) => k.state !== 'cancelled').map((k) => ({ sku_id: k.sku_id, code: k.code, name: k.name, price: k.price == null ? null : Number(k.price), choices: {}, jans: k.jans })),
    cancelled_children: kids.filter((k) => k.state === 'cancelled').map((k) => ({ sku_id: k.sku_id, code: k.code })),
    common: { shipping: null, amazon_url: null, asin: null, official_url: null, reference_urls: [], yahoo: null },
    ...over,
  };
}
const enqueue = (gid, rev, payload, actor = 'naka@test') => tx(async () => (await pg.query('select ops.enqueue_group_snapshot($1, $2, $3::jsonb, $4, $5)::text as id', [gid, rev, JSON.stringify(payload), uuid(), actor])).rows[0].id);
const groupRows = (gid) => q('select event_id::text as eid, revision, status from ops.product_hub_outbox where group_product_id = $1 order by revision', [gid]);

let g1ev;
await ta('[G1] まとまりの知らせを書く (部品・持ち主): entity variation_group・entity_id = まとまり・sku_id なし・hash は DB が作る (= lib の hash)', async () => {
  const p = await snap(grp1, 1);
  assert.equal(p.children.length, 2);
  assert.equal(O.groupSnapshotShapeProblem(p), null);
  g1ev = await enqueue(grp1, 1, p);
  const r = await one('select entity_kind, entity_id::text as eid, sku_id, group_product_id::text as gid, kind, schema_version, revision, payload, payload_hash, status, created_by from ops.product_hub_outbox where event_id = $1', [g1ev]);
  assert.deepEqual([r.entity_kind, r.eid, r.sku_id, r.gid, r.kind, r.schema_version, r.revision, r.status, r.created_by],
    ['variation_group', grp1, null, grp1, 'group_snapshot', 'ph-group-v1', 1, 'pending', 'naka@test']);
  assert.equal(r.payload_hash, O.groupPayloadHash(p));
  assert.deepEqual(r.payload, p);
});

await ta('[G2] 今の借りる関数 (claim_card_events)・今の取り込み (runCardOutbox・manual も) はまとまりの知らせを借りない = 今の product-hub に渡らない', async () => {
  const c = await asEditor(() => q(`select event_id::text as id from ops.claim_card_events('t-1', 'manual', null, null, 200, 60, 5)`));
  assert.ok(!c.some((x) => x.id === g1ev));
  assert.deepEqual(await runCards({ manual: true, limit: 200 }), []);
  assert.equal((await asEditor(() => q(`select event_id from ops.claim_card_events('t-1', 'auto', $1::uuid, null, 1, 60, 5)`, [g1ev]))).length, 0);
  const r = await one('select status, attempts, lease_owner from ops.product_hub_outbox where event_id = $1', [g1ev]);
  assert.deepEqual([r.status, r.attempts, r.lease_owner], ['pending', 0, null]);
});

await ta('[G3] 名前空間つきで借りる (画面のロール・entity_kind 必須): まとまりの知らせだけ / SKU の知らせだけ・結果は今の finish_card_event で書ける', async () => {
  assert.equal(await asEditor(() => pgCode(pg.query(`select * from ops.claim_outbox_events('t-2', 'auto', null)`))), '22023');
  assert.equal(await asEditor(() => pgCode(pg.query(`select * from ops.claim_outbox_events('t-2', 'auto', 'product')`))), '22023');
  const s = await asEditor(() => q(`select entity_kind from ops.claim_outbox_events('t-2', 'manual', 'sku', null, null, 200, 1, 5)`));
  assert.ok(s.every((x) => x.entity_kind === 'sku'));
  const g = await asEditor(() => q(`select event_id::text as id, entity_kind, entity_id::text as eid, revision, kind, sku_id, group_product_id::text as gid, schema_version, payload_hash, attempts
      from ops.claim_outbox_events('t-2', 'auto', 'variation_group', null, $1::bigint, 20, 60, 5)`, [grp1]));
  assert.deepEqual(g.map((x) => [x.id, x.entity_kind, x.eid, x.revision, x.kind, x.sku_id, x.gid, x.schema_version, x.attempts]),
    [[g1ev, 'variation_group', grp1, 1, 'group_snapshot', null, grp1, 'ph-group-v1', 1]]);
  // 借りの間はほかが取らない
  assert.equal((await asEditor(() => q(`select 1 from ops.claim_outbox_events('t-3', 'manual', 'variation_group')`))).length, 0);
  assert.equal((await asEditor(() => one(`select ops.finish_card_event($1::uuid, 't-3', 'done', '{}'::jsonb, null) as ok`, [g1ev]))).ok, false);
  assert.equal((await asEditor(() => one(`select ops.finish_card_event($1::uuid, 't-2', 'done', '{"outcome":"created"}'::jsonb, null) as ok`, [g1ev]))).ok, true);
  assert.equal((await one('select status from ops.product_hub_outbox where event_id = $1', [g1ev])).status, 'done');
  await asEditor(() => q(`select ops.finish_card_event(event_id, 't-2', 'failed', null, 'x') from ops.product_hub_outbox where lease_owner = 't-2'`));
});

await ta('[G4] revision は増えるだけ: 同じ / 小さい = 断る (group_revision_not_newer・何も書かない)・続いていなくてよい (1 → 3)・その後の 2 も断る', async () => {
  await assert.rejects(async () => enqueue(grp1, 1, await snap(grp1, 1)), /group_revision_not_newer/);
  await enqueue(grp1, 3, await snap(grp1, 3));
  await assert.rejects(async () => enqueue(grp1, 2, await snap(grp1, 2)), /group_revision_not_newer/);
  await assert.rejects(async () => enqueue(grp1, 3, await snap(grp1, 3)), /group_revision_not_newer/);
  assert.deepEqual((await groupRows(grp1)).map((r) => r.revision), [1, 3]);
  await enqueue(grp1, 4, await snap(grp1, 4));
  assert.deepEqual((await groupRows(grp1)).map((r) => r.revision), [1, 3, 4]);
});

await ta('[G5] 書き方によらず同じ決まり: 持ち主が部品を通らずに insert しても revision・hash・作った人・形・値を DB が見る', async () => {
  const ins = (p, { rev = p.revision, hash = O.groupPayloadHash(p), by = 'naka@test' } = {}) => tx(() => pg.query(`insert into ops.product_hub_outbox (company_id, group_product_id, kind, schema_version, payload, payload_hash, revision, request_id, created_by)
      values (1, $1, 'group_snapshot', 'ph-group-v1', $2::jsonb, $3, $4, gen_random_uuid(), $5)`, [grp1, JSON.stringify(p), hash, rev, by]));
  await assert.rejects(async () => ins(await snap(grp1, 2)), /group_revision_not_newer/);
  await assert.rejects(async () => ins(await snap(grp1, 5), { hash: 'b'.repeat(64) }), /payload_hash/);
  await assert.rejects(async () => ins(await snap(grp1, 5), { by: 'someone@else' }), /作った人/);
  await assert.rejects(async () => ins(await snap(grp1, 5), { rev: 6 }), /revision_mismatch/);
  await assert.rejects(async () => ins({ ...(await snap(grp1, 5)), schema: 'ph-group-v2' }), /schema/);
  await ins(await snap(grp1, 5));
  assert.deepEqual((await groupRows(grp1)).map((r) => r.revision), [1, 3, 4, 5]);
  // 書いた後の revision・まとまりは変えない (まだ済んでいない知らせでも = 0052 の中身の守りに 0066 で足した列)
  const r5 = (await groupRows(grp1)).find((r) => r.revision === 5);
  assert.equal(r5.status, 'pending');
  await assert.rejects(() => tx(() => pg.query('update ops.product_hub_outbox set revision = 50 where event_id = $1', [r5.eid])), /変えない/);
  await assert.rejects(() => tx(() => pg.query('update ops.product_hub_outbox set group_product_id = $2 where event_id = $1', [r5.eid, rep2])), /変えない/);
  assert.deepEqual((await groupRows(grp1)).map((r) => r.revision), [1, 3, 4, 5]);
});

await ta('[G6] 今の Company DB の値と違うスナップショットは断る (子が足りない / 多い・コード・名前・売価・JAN・まとまりのコード・名前・種類・無いまとまり・親のある商品)', async () => {
  const base = await snap(grp1, 9);
  const s002 = await one(`select sku_id::text as id from core.skus where code = 's002'`);
  const bad = [
    [{ children: base.children.slice(1) }, /children_differ/],
    [{ children: [...base.children, { sku_id: s002.id, code: 's002', name: '代表の単品', price: 1000, choices: {}, jans: [] }] }, /children_differ/],
    [{ children: base.children.map((c, i) => (i === 0 ? { ...c, code: 'S001' } : c)) }, /child_differs: S001/],
    [{ children: base.children.map((c, i) => (i === 0 ? { ...c, name: '違う名前' } : c)) }, /child_differs/],
    [{ children: base.children.map((c, i) => (i === 0 ? { ...c, price: c.price + 1 } : c)) }, /child_differs/],
    [{ children: base.children.map((c, i) => (i === 0 ? { ...c, price: null } : c)) }, /child_differs/],
    [{ children: base.children.map((c, i) => (i === 0 ? { ...c, jans: ['4901234567894'] } : c)) }, /child_differs/],
    [{ cancelled_children: [{ sku_id: s002.id, code: 's002' }] }, /cancelled_children_differ/],
    [{ group: { ...base.group, code: 'GRP1' } }, /group_tag_differs/],
    [{ group: { ...base.group, name: '違うまとまり' } }, /group_name_differs/],
    [{ group: { ...base.group, kind: 'single', sku_id: s002.id } }, /group_tag_differs/],
  ];
  for (const [over, re] of bad) await assert.rejects(() => enqueue(grp1, 9, { ...base, ...over }), re, JSON.stringify(over));
  // まとまりの番号が payload と違う / 無いまとまり / 親のある商品 (子) をまとまりに
  await assert.rejects(() => enqueue(grp1, 9, { ...base, group: { ...base.group, product_id: rep2 } }), /group_mismatch/);
  await assert.rejects(() => enqueue('999999', 1, { ...base, revision: 1, group: { ...base.group, product_id: '999999' } }), /group_not_found|fk_pho_group|violates foreign key/);
  const childPid = (await one(`select product_id::text as id from core.skus where code = 's001'`)).id;
  await assert.rejects(() => enqueue(childPid, 1, { ...base, revision: 1, group: { ...base.group, product_id: childPid } }), /group_has_parent/);
  assert.deepEqual((await groupRows(grp1)).map((r) => r.revision), [1, 3, 4, 5]);
  // JAN を足すと、JAN の入ったスナップショットでないと断る
  const s001p = (await one(`select product_id::text as id from core.skus where code = 's001'`)).id;
  await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type) values (1, 'product', $1, 'jan', 'jan', '4901234567894', 'manual', 'human')`, [s001p]);
  await assert.rejects(() => enqueue(grp1, 9, base), /child_differs: s001/);
  const withJan = await snap(grp1, 9);
  assert.deepEqual(withJan.children[0].jans, ['4901234567894']);
  await enqueue(grp1, 9, withJan);
});

await ta('[G7] 子を廃止 (登録の状態 cancelled) した後は、廃止した子の一覧に入れたスナップショットでないと断る', async () => {
  const s003 = await skuId('s003');
  // 試験だけ: 持ち主が印 (GUC) を立てて状態を直接 cancelled にする (子の廃止の関数は PR-5)
  await tx(async () => {
    await pg.query(`select set_config('ops.registration_protocol', '1', true)`);
    await pg.query(`update ops.master_registrations set state = 'cancelled' where sku_id = $1`, [s003]);
  });
  const old = await snap(grp1, 10);
  assert.deepEqual(old.cancelled_children, [{ sku_id: s003, code: 's003' }]);
  assert.deepEqual(old.children.map((c) => c.code), ['s001']);
  await assert.rejects(() => enqueue(grp1, 10, { ...old, children: [...old.children, { sku_id: s003, code: 's003', name: '単品 3【青】', price: 1000, choices: {}, jans: [] }], cancelled_children: [] }), /children_differ/);
  await assert.rejects(() => enqueue(grp1, 10, { ...old, cancelled_children: [] }), /cancelled_children_differ/);
  await assert.rejects(() => enqueue(grp1, 10, { ...old, cancelled_children: [{ sku_id: s003, code: 'x003' }] }), /cancelled_child_differs/);
  await enqueue(grp1, 10, old);
});

await ta('[G8] 単品の代表のまとまり: kind single・sku_id = 代表の SKU・コード = 代表の SKU のコード (札の形では断る)', async () => {
  const p = await snap(rep2, 1);
  assert.deepEqual([p.group.kind, p.group.sku_id, p.group.code, p.children.map((c) => c.code)], ['single', await skuId('s002'), 's002', ['s004']]);
  await assert.rejects(() => enqueue(rep2, 1, { ...p, group: { ...p.group, kind: 'tag', sku_id: null } }), /group_rep_differs/);
  const s004 = await skuId('s004');
  await assert.rejects(() => enqueue(rep2, 1, { ...p, group: { ...p.group, sku_id: s004 } }), /group_rep_differs/);
  await enqueue(rep2, 1, p);
  assert.deepEqual((await groupRows(rep2)).map((r) => r.revision), [1]);
});

await ta('[G9] 軸と選択肢のある形 (2 軸・子のコード = まとまり + 横 + 縦) も DB が受ける (軸を入れる前からの子は choices が空)', async () => {
  // 試験だけ: 夜間ロードで 2 軸の子を足す (PR-5 の登録の関数の代わり)
  const r2 = await runInitialLoad(db, { ...plan, skus: [...plan.skus, sku('grp1-WH-90', 'はかま【白】【90】', { representativeCode: 'grp1', representativeState: 'value' })],
    variationGroups: [{ code: 'grp1', name: '札のまとまり', childCodes: ['s001', 's003', 'grp1-WH-90'], status: 'active' }, plan.variationGroups[1]] },
  { log: quiet, runId: 'load_vg3b', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
  assert.equal(r2.ok, true, r2.error);
  const p = await snap(grp1, 11);
  const wh = p.children.find((c) => c.code === 'grp1-WH-90');
  assert.ok(wh, JSON.stringify(p.children));
  const full = { ...p, axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
    options: [{ axis: 1, code: '-WH', name: 'ホワイト', sort: 0 }, { axis: 2, code: '-90', name: '90cm', sort: 0 }],
    children: p.children.map((c) => (c === wh ? { ...c, choices: { 1: '-WH', 2: '-90' } } : c)) };
  assert.equal(O.groupSnapshotShapeProblem(full), null);
  await enqueue(grp1, 11, full);
  await assert.rejects(() => enqueue(grp1, 12, { ...full, revision: 12, children: full.children.map((c) => (c.code === 'grp1-WH-90' ? { ...c, choices: { 1: '-WH', 2: '-91' } } : c)) }), /child_choice_unknown/);
});

console.log('\n形の決まり (lib と DB が同じ答え)');

await ta('[J1] 見本 (正しい形と 1 か所ずつ壊した形) に、lib の groupSnapshotShapeProblem と DB の ops.group_snapshot_shape_problem が同じ答え', async () => {
  const ok = {
    schema: 'ph-group-v1', revision: 2, created_by: 'naka@test',
    group: { product_id: '12', sku_id: null, code: 'hakama', name: 'はかま', kind: 'tag' },
    axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
    options: [{ axis: 1, code: '-WH', name: 'ホワイト', sort: 0 }, { axis: 1, code: '-BK', name: 'ブラック', sort: 1 }, { axis: 2, code: '-90', name: '90', sort: 0 }],
    children: [{ sku_id: '101', code: 'hakama-WH-90', name: 'はかま【ホワイト】【90】', price: 1980, choices: { 1: '-WH', 2: '-90' }, jans: ['4901234567894'] },
      { sku_id: '102', code: 'Hakama-BK-90', name: 'はかま【ブラック】【90】', price: null, choices: { 1: '-BK', 2: '-90' }, jans: [] },
      { sku_id: '103', code: 'old-child', name: '前からの子', price: 0, choices: {}, jans: [] }],
    cancelled_children: [{ sku_id: '104', code: 'hakama-RD-90' }],
    common: { shipping: { code: 'S01', method: 'ゆうパケット', cost_jpy: 210 }, amazon_url: null, asin: 'B0ABCDEFGH', official_url: null, reference_urls: ['https://a.example'], yahoo: { price: 2080 } },
  };
  const set = (o, pth, v) => { const c = structuredClone(o); let x = c; for (const k of pth.slice(0, -1)) x = x[k]; if (v === undefined) delete x[pth.at(-1)]; else x[pth.at(-1)] = v; return c; };
  const one1 = { ...ok, axes: [{ axis: 1, name: 'カラー' }], options: ok.options.filter((o) => o.axis === 1),
    children: [{ sku_id: '101', code: 'hakama-WH', name: 'x', price: 1, choices: { 1: '-WH' }, jans: [] }] };
  const samples = [
    ok, one1, { ...ok, axes: [], options: [], children: [ok.children[2]] },
    null, [], 'x', set(ok, ['extra'], 1), set(ok, ['common'], undefined), set(ok, ['schema'], 'ph-group-v2'), set(ok, ['revision'], 0), set(ok, ['revision'], 1.5), set(ok, ['revision'], '2'),
    set(ok, ['revision'], 2147483648), set(ok, ['created_by'], ''), set(ok, ['created_by'], 'a\u0001'),
    set(ok, ['group', 'extra'], 1), set(ok, ['group', 'product_id'], 12), set(ok, ['group', 'product_id'], '012'), set(ok, ['group', 'kind'], 'set'), set(ok, ['group', 'sku_id'], '5'),
    set(set(ok, ['group', 'kind'], 'single'), ['group', 'sku_id'], null), set(set(ok, ['group', 'kind'], 'single'), ['group', 'sku_id'], '05'),
    set(ok, ['group', 'code'], ' hakama'), set(ok, ['group', 'code'], ''), set(ok, ['group', 'name'], ''), set(ok, ['group', 'name'], 'a\nb'),
    set(ok, ['axes'], {}), set(ok, ['axes'], [{ axis: 1, name: 'a' }, { axis: 2, name: 'b' }, { axis: 2, name: 'c' }]), set(ok, ['axes', 0, 'axis'], 3), set(ok, ['axes', 0, 'axis'], '1'),
    set(ok, ['axes', 0, 'name'], '   '), set(ok, ['axes', 0, 'extra'], 1), set(ok, ['axes'], [{ axis: 2, name: 'b' }, { axis: 1, name: 'a' }]), set(ok, ['axes'], [{ axis: 2, name: 'b' }]),
    set(ok, ['options', 0, 'axis'], 3), set(ok, ['options', 0, 'code'], 'WH'), set(ok, ['options', 0, 'code'], '-W_H'), set(ok, ['options', 0, 'code'], '-ABCDEFGHIJK'), set(ok, ['options', 0, 'name'], ''),
    set(ok, ['options', 0, 'sort'], -1), set(ok, ['options', 0, 'sort'], 0.5), set(ok, ['options', 1, 'code'], '-wh'), set(ok, ['options', 1, 'name'], ' ホワイト '), set(ok, ['options', 1, 'name'], 'ﾎﾜｲﾄ'),
    { ...ok, axes: [], options: ok.options, children: [] },
    set(ok, ['children'], {}), set(ok, ['children', 0, 'extra'], 1), set(ok, ['children', 0, 'sku_id'], 101), set(ok, ['children', 1, 'sku_id'], '101'), set(ok, ['children', 1, 'code'], 'HAKAMA-WH-90'),
    set(ok, ['children', 0, 'code'], 'hakama-WH-90 '), set(ok, ['children', 0, 'name'], ''), set(ok, ['children', 0, 'price'], -1), set(ok, ['children', 0, 'price'], 1.5), set(ok, ['children', 0, 'price'], '1980'),
    set(ok, ['children', 0, 'jans'], ['123']), set(ok, ['children', 0, 'jans'], ['4901234567894', '4901234567894']), set(ok, ['children', 0, 'jans'], ['1', '2', '3', '4', '5', '6'].map(() => '12345678').map((x, i) => String(Number(x) + i))),
    set(ok, ['children', 0, 'jans'], 'x'), set(ok, ['children', 0, 'choices'], []), set(ok, ['children', 0, 'choices'], { 1: '-WH' }), set(ok, ['children', 0, 'choices'], { 1: '-WH', 3: '-90' }),
    set(ok, ['children', 0, 'choices'], { 1: '-WH', 2: null }), set(ok, ['children', 0, 'choices'], { 1: '-wh', 2: '-90' }), set(ok, ['children', 0, 'choices'], { 1: '-RD', 2: '-90' }),
    set(ok, ['children', 0, 'code'], 'hakama-WH-91'), set(ok, ['children', 1, 'choices'], { 1: '-WH', 2: '-90' }),
    { ...one1, children: [{ sku_id: '101', code: 'hakama-WH', name: 'x', price: 1, choices: { 1: '-WH', 2: '-90' }, jans: [] }] },
    set(ok, ['cancelled_children'], null), set(ok, ['cancelled_children', 0, 'sku_id'], '101'), set(ok, ['cancelled_children', 0, 'code'], 'HAKAMA-WH-90'), set(ok, ['cancelled_children', 0, 'extra'], 1),
    set(ok, ['cancelled_children', 0, 'code'], ''),
    set(ok, ['common', 'extra'], 1), set(ok, ['common', 'shipping'], { code: 'S01' }), set(ok, ['common', 'shipping'], []), set(ok, ['common', 'asin'], 1), set(ok, ['common', 'reference_urls'], [1]),
    set(ok, ['common', 'reference_urls'], null), set(ok, ['common', 'yahoo'], { price: 1, other: 2 }), set(ok, ['common', 'yahoo'], 'x'),
  ];
  const answers = [];
  for (const s of samples) {
    const js = O.groupSnapshotShapeProblem(s);
    const dbAns = (await one('select ops.group_snapshot_shape_problem($1::jsonb) as p', [JSON.stringify(s)])).p;
    assert.equal(js, dbAns, `見本 ${JSON.stringify(s)?.slice(0, 300)}`);
    answers.push(js);
  }
  assert.deepEqual(answers.slice(0, 3), [null, null, null]);
  assert.ok(answers.slice(3).every((a) => typeof a === 'string'), JSON.stringify(answers));
  // 答えの種類が十分に多い (どこかの確かめが黙って抜けていない)
  assert.ok(new Set(answers.slice(3)).size >= 35, `${new Set(answers.slice(3)).size}: ${[...new Set(answers.slice(3))].join(' ')}`);
});

console.log('\n権限');

await ta('[P1] 画面のロール: 部品・確かめは実行できない (42501)・知らせを直接足せない (42501)・名前空間つきで借りる関数は実行できる', async () => {
  const p = await snap(grp1, 20);
  assert.equal(await asEditor(() => pgCode(pg.query('select ops.enqueue_group_snapshot($1, 20, $2::jsonb, gen_random_uuid(), $3)', [grp1, JSON.stringify(p), 'naka@test']))), '42501');
  assert.equal(await asEditor(() => pgCode(pg.query('select ops.group_snapshot_problem(1::smallint, $1, 20, $2::jsonb)', [grp1, JSON.stringify(p)]))), '42501');
  assert.equal(await asEditor(() => pgCode(pg.query('select ops.group_snapshot_shape_problem($1::jsonb)', [JSON.stringify(p)]))), '42501');
  assert.equal(await asEditor(() => pgCode(tx(() => pg.query(`insert into ops.product_hub_outbox (company_id, group_product_id, kind, schema_version, payload, payload_hash, revision, request_id, created_by)
    values (1, $1, 'group_snapshot', 'ph-group-v1', $2::jsonb, $3, 20, gen_random_uuid(), 'naka@test')`, [grp1, JSON.stringify(p), O.groupPayloadHash(p)])))), '42501');
  const priv = await one(`select has_function_privilege('master_edit', 'ops.claim_outbox_events(text, text, text, uuid, bigint, integer, integer, integer)', 'execute') as a,
      has_function_privilege('master_edit', 'ops.enqueue_group_snapshot(bigint, integer, jsonb, uuid, text)', 'execute') as b,
      has_function_privilege('master_edit', 'ops.claim_card_events(text, text, uuid, bigint, integer, integer, integer)', 'execute') as c,
      has_function_privilege('master_edit', 'ops.finish_card_event(uuid, text, text, jsonb, text)', 'execute') as d`);
  assert.deepEqual(priv, { a: true, b: false, c: true, d: true });
  for (const r of ['master_ops', 'master_observer', 'master_gate', 'watcher']) {
    assert.equal((await one(`select has_function_privilege($1, 'ops.claim_outbox_events(text, text, text, uuid, bigint, integer, integer, integer)', 'execute') as x`, [r])).x, false, r);
  }
});

await ta('[P2] 画面のロールの取引では、security definer の関数を通してもまとまりの知らせは書けない (group_snapshot_session_required・PR-5 で開く)', async () => {
  await pg.query(`create function public.vg3_test_enqueue(p_gid bigint, p_rev integer, p jsonb) returns uuid language sql security definer set search_path = pg_catalog, pg_temp as
    $$ select ops.enqueue_group_snapshot(p_gid, p_rev, p, gen_random_uuid(), 'naka@test') $$`);
  try {
    await pg.query('grant execute on function public.vg3_test_enqueue(bigint, integer, jsonb) to master_edit');
    const p = await snap(grp1, 20);
    const e = await asEditor(async () => { try { await tx(() => pg.query('select public.vg3_test_enqueue($1, 20, $2::jsonb)', [grp1, JSON.stringify(p)])); return null; } catch (x) { return x; } });
    assert.equal(e?.code, '42501'); assert.match(e.message, /group_snapshot_session_required/);
    // 持ち主 (夜間ロード・migration・PR-5 の関数を持ち主が呼ぶ) は書ける
    await tx(() => pg.query('select public.vg3_test_enqueue($1, 20, $2::jsonb)', [grp1, JSON.stringify(p)]));
  } finally { await pg.query('drop function public.vg3_test_enqueue(bigint, integer, jsonb)'); }
});

await ta('[P3] 新しい関数は search_path = pg_catalog, pg_temp・public の実行権なし (security definer は部品・借りる・trigger)', async () => {
  const fns = await q(`select p.proname as n, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub
    from pg_proc p join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname in
    ('claim_outbox_events', 'enqueue_group_snapshot', 'guard_product_hub_outbox_group', 'group_snapshot_problem', 'group_snapshot_shape_problem', 'claim_card_events', 'guard_product_hub_outbox_session') order by 1`);
  assert.equal(fns.length, 7);
  for (const f of fns) { assert.match(f.c, /^search_path=pg_catalog, pg_temp$/, f.n); assert.equal(f.pub, false, f.n); }
  assert.deepEqual(fns.filter((f) => f.d).map((f) => f.n).sort(), ['claim_card_events', 'claim_outbox_events', 'enqueue_group_snapshot', 'guard_product_hub_outbox_group', 'guard_product_hub_outbox_session']);
  const trg = (await q(`select tgname from pg_trigger where tgrelid = 'ops.product_hub_outbox'::regclass and not tgisinternal order by 1`)).map((r) => r.tgname);
  assert.deepEqual(trg, ['trg_product_hub_outbox_group', 'trg_product_hub_outbox_guard', 'trg_product_hub_outbox_no_truncate', 'trg_product_hub_outbox_session']);
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* */ }
