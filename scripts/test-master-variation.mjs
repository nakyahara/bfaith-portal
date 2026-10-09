/**
 * test-master-variation.mjs — 色違い・サイズ違いのまとまりの DB (0067・AI_reference CompanyDB構想/20 v7 §③・§⑤・§⑩ の PR-5)
 *
 * Company DB = PGlite (持ち主のロール deploy で migration)。書き込みは画面のロール master_edit (本物の関数の権限)・照合の観測は watch_writer。
 * 🚨 移行の試験: 0066 まで流した DB で夜間ロードが札 (grp1) と単品の代表 (s002) を作ってから 0067 を流す = 今あるまとまりの予約・重なりの事前検査
 * 固定する契約:
 *   M 予約: 0067 が今ある札・単品の代表を予約 (load) に入れる・重なりがあれば migration を止める (何も入れない)・事前検査の読むだけの SQL (README / PR 本文と同じ文) が同じ数
 *   G 持ち主の門: products.parent の DB の active が load = まとまりの関数は全部 parent_not_company (何も書かない)・ふつうの単品の登録は今までどおり
 *   B まとめての登録 (1 取引): 札・予約 (portal)・軸・選択肢・子の選択肢・親 (manual)・revision 1・スナップショット (ph-group-v1 = lib の形の決まりと DB の値に合う)・約束と done・
 *     同じ request_id = 前の答え・コードの決まり (まとまり・選択肢番号・子 = まとまり + 選択肢・一意・(横, 縦) の一意・20 件まで)・閉じないと commit できない・
 *     閉じる時の子の確かめ (この取引で登録した下書き・子の request_id・カードなし)・一部が断られたら全部巻き戻す・今あるまとまり (札・単品の代表) に足す・
 *     同じまとまりを 1 つの取引で 2 回変える = revision_twice
 *   R 名前を直す: まとまり (札だけ)・軸・選択肢名・見た revision・一意・変わらない = 何も書かない・巻き戻した取引は revision を上げない
 *   C 子の廃止: 下書き / NE 登録待ち・生きているファイルなし・NE に一度も現れていない子だけ・スナップショットの廃止した子・コードは使い回さない
 *   P 親か子のどちらか一方 (company のときだけ・load は止めない)
 *   N NE 登録の CSV のまとまりの版: ポータルで作った札の子 = NE に無くても予約のコード (lib = DB)・全部同じまとまり・まとまりで 1 ファイル・単品の版には入れない・JAN = empty
 *   A quarantined の代表の採用: 照合の確かめ待ちに入る (company のときだけ)・最新の封のある回の観測・今ある札 / 札を作る / 子の無い単品・1 回だけ・無い / 古い / 読めない = 断る
 *   S 権限と関数の形・本文の差 (0065 / 0066 / 0054 の版から変えた行だけ)
 * 使い方: node scripts/test-master-variation.mjs
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

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vg5-variation-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles, VARIATION_EDIT_FUNCTIONS, VARIATION_OWNER_ONLY_FUNCTIONS, VARIATION_SELECT } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const C = await import('../lib/master-cutover.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const O = await import('../lib/product-hub-outbox.mjs');

const MIG_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'db', 'company', 'migrations');
const MIG_FILES = fs.readdirSync(MIG_DIR).filter((x) => /^\d{4}_.*\.sql$/.test(x)).sort();
const PRE_0067 = MIG_FILES.map((x) => x.slice(0, 4)).filter((v) => v < '0067').pop();
let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const uuid = () => crypto.randomUUID();
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
const jan13 = (b) => { const d = b.split('').map(Number).reverse(); const s = d.reduce((a, x, i) => a + x * (i % 2 === 0 ? 3 : 1), 0); return b + ((10 - (s % 10)) % 10); };

/** 事前検査の読むだけの SQL (0067 を流す前に watcher で数える = README / PR 本文の SQL と同じ文)。dup + tag_is_sku = 0 でなければ 0067 は止まる */
export const PRECHECK_SQL = `with g as (
  select p.product_id as gid, core.norm_code(p.display_code) as norm, 'tag' as kind from core.products p
   where p.company_id = 1 and coalesce(btrim(p.display_code), '') <> '' and not exists (select 1 from core.skus k where k.product_id = p.product_id)
  union all
  select p.product_id, k.code_norm, 'single' from core.products p join core.skus k on k.product_id = p.product_id and k.sku_kind = 'single'
   where p.company_id = 1 and exists (select 1 from core.products c where c.parent_product_id = p.product_id))
select (select count(*) from g)::int as candidates,
       (select count(*) from (select norm from g group by norm having count(*) > 1) d)::int as dup,
       (select count(*) from g join core.skus k on k.company_id = 1 and k.code_norm = g.norm and k.product_id is distinct from g.gid where g.kind = 'tag')::int as tag_is_sku`;

// ── DB (0066 まで → 夜間ロード → 切替 → 0067) ──
const pg = new PGlite();
const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet, to: PRE_0067 });
assert.equal(PRE_0067, '0066');
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});   // 0067 の前に流しても止まらない (関数・表が無い DB では付けない)
await W2.useReal0058(pg, { leases: ['single', 'set'], futureSetLease: true });
const q = async (sql, params) => (await db.query(sql, params)).rows;
const one = async (sql, params) => (await q(sql, params))[0];
async function asRole(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
const asEditor = (fn) => asRole('master_edit', fn);
async function asGate(host, fn) {
  await pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await pg.query(`set session authorization ${sessionUser}`); await pg.query('set role deploy'); }
}
const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const OWN = JSON.stringify(ALL_COMPANY);
const LOAD_NOW = new Date(Date.now() - 5 * 86400e3);
const NOW_MS = new Date('2030-01-10T03:00:00Z').getTime();
const RUN1 = 'mc_20300110T000000000Z_aaaaaa';
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
    ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(MASTER_OWNERSHIP), manual_entries_stopped: [{ id: 'ne.product_screen', by: 'naka@test', at: new Date().toISOString() }], drain: { done: true, checked_by: 'naka@test', checked_at: new Date().toISOString() } }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL_COMPANY) };
  return asRole('master_ops', () => C.advanceCutoverPhase(db, { to, actor: 'naka@test', evidence }));
}
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
const baseSkus = [sku('s001', '単品 1'), sku('s002', '代表の単品'), sku('s003', '単品 3'), sku('s004', '代表の子', { representativeCode: 's002', representativeState: 'value' }),
  sku('s005', '子の無い単品'), sku('s006', '札の子 赤【赤】', { representativeCode: 'grp1', representativeState: 'value' }), sku('s007', '札の子 青【青】', { representativeCode: 'grp1', representativeState: 'value' })];
const planOf = (extra = []) => ({
  skus: [...baseSkus, ...extra],
  variationGroups: [{ code: 'grp1', name: '札のまとまり', childCodes: ['s006', 's007'], status: 'active' }, { code: 's002', name: '単品の代表', childCodes: ['s004'], status: 'active' }],
  setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [...baseSkus, ...extra].map((s) => ({ supplierCode: '0001', skuCode: s.code })),
  primarySuppliers: [...baseSkus, ...extra].map((s) => ({ skuCode: s.code, supplierCode: '0001' })), reorder: { available: true, runId: 'pml_test' },
});
const loadR = await runInitialLoad(db, planOf(), { log: quiet, runId: 'load_vg5', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
assert.equal(loadR.ok, true, loadR.error);
const pidOf = async (code) => (await one('select product_id::text as id from core.skus where code = $1', [code]))?.id;
const skuIdOf = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code]))?.id;
const GRP1 = (await one(`select product_id::text as id from core.products p where display_code = 'grp1' and not exists (select 1 from core.skus k where k.product_id = p.product_id)`)).id;
const REP2 = await pidOf('s002');
const PRE = await one(PRECHECK_SQL);
const productsBefore = await q('select product_id::text as id, parent_product_id::text as par, name from core.products order by product_id');

await toPhase('frozen');
await toPhase('company_owner');
const bp = await one('select * from ops.registration_backfill_plan()');
await asRole('master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3, $4)', [bp.sku_count, bp.snapshot_hash, 'naka@test', '試験']));
await toPhase('new_open');
// 今日の照合の回 (画面の今日 2030-01-10) と NE の元のコード・許可
async function recordNeCodes(run, extra = [], reps = [{ code_norm: 'grp1', ne_code: 'grp1' }]) {
  const codes = ['s001', 's002', 's003', 's004', 's005', 's006', 's007', ...extra];
  const entries = [...codes.map((c) => ({ code_norm: c.toLowerCase(), kind: 'product', state: 'ok', ne_code: c, spellings: [c] })),
    ...reps.map((r) => ({ code_norm: r.code_norm, kind: 'rep', state: 'ok', ne_code: r.ne_code, spellings: [r.ne_code] }))];
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries })]);
  await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(db, { runId: run, withSet: true });
}
await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
await recordNeCodes(RUN1);
/** NE のコードを変える = 今日 (2030-01-10) の新しい照合の回 (同じ回の中身は変えられない = 0041) */
let todaySeq = 0;
async function newRunToday(extra, reps) {
  todaySeq++;
  const run = `mc_20300110T0${todaySeq}0000000Z_d0000${todaySeq}`;
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)`, [run, `2030-01-10T0${todaySeq}:00:00Z`]);
  await recordNeCodes(run, extra, reps);
  return run;
}

// ── 0067 を流す (+ ロールの流し直し) ──
await applyMigrations(db, { log: quiet });
assert.equal((await one(`select count(*)::int as n from ops.schema_migrations where version = '0067'`)).n, 1);
await createMasterEditRoles(pg, {});

const SUP1 = (await one(`select supplier_id::text as id from core.suppliers where code = '0001'`)).id;
const TODAY_DB = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
/** 子の登録の中身 (ops.register_new_sku・単品・カードなし) */
const entry = (code, name, over = {}) => ({
  kind: 'single', code, started_at: null,
  product: { name, sales_class: 3, expiry_managed: false, inbound_date_managed: null },
  sku: { name, tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', standard_price_jpy: 1500, shipping_code: null, shipping_method: null, shipping_cost_jpy: null,
    reorder_months: 1, set_sales_class_override: null, handling_own: null },
  supplier_id: SUP1, cost: { jpy: 300, source: 'manual', status: 'COMPLETE', valid_from: TODAY_DB, reason: '試験の原価' }, component_request: null, card: null, ...over,
});
const openSql = 'select ops.variation_batch_open($1::uuid, $2, $3, $4::jsonb, $5::jsonb) as r';
const closeSql = 'select ops.variation_batch_close($1::uuid, $2, $3::jsonb, $4::jsonb) as r';
const regSql = 'select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r';
/**
 * まとめての登録 (画面のロール・1 つの取引): 開く → 子ごとに登録 (子の request_id = DB が決めた番号)・JAN → 閉じる → commit。
 * hooks: names (子のコード → 名前)・jans (子のコード → JAN)・mutate (閉じる前に何かする)・skipClose・childEntry (子の中身を変える)
 */
async function batch(spec, { actor = 'naka@test', rid = uuid(), names = {}, jans = {}, common = null, mutate = null, skipClose = false, childEntry = null, regRid = null } = {}) {
  return asEditor(async () => {
    await pg.query('begin');
    try {
      const o = (await pg.query(openSql, [rid, actor, null, OWN, JSON.stringify(spec)])).rows[0].r;
      if (o.replayed) { await pg.query('commit'); return { open: o, replayed: true, rid }; }
      const kids = [];
      for (const c of o.children) {
        const e = childEntry ? childEntry(c.code, entry(c.code, names[c.code] ?? `子 ${c.code}`)) : entry(c.code, names[c.code] ?? `子 ${c.code}`);
        const r = (await pg.query(regSql, [regRid ? regRid(c) : c.request_id, actor, null, OWN, 'e'.repeat(64), JSON.stringify(e)])).rows[0].r;
        kids.push(r);
        if (jans[c.code]) await pg.query('select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb)', [uuid(), actor, null, OWN, r.sku_id, '[]', JSON.stringify([jans[c.code]])]);
      }
      if (mutate) await mutate(o, kids);
      const cl = skipClose ? null : (await pg.query(closeSql, [rid, actor, OWN, common ? JSON.stringify(common) : null])).rows[0].r;
      await pg.query('commit');
      return { open: o, close: cl, kids, rid };
    } catch (e) { try { await pg.query('rollback'); } catch { /* */ } throw e; }
  });
}
/** 画面のロールで 1 つの関数を 1 つの取引で */
const editorCall = (sql, params) => asEditor(async () => (await pg.query(sql, params)).rows[0].r);
const counts = async () => one(`select (select count(*) from core.products)::int as products, (select count(*) from core.skus)::int as skus,
  (select count(*) from ops.variation_group_codes)::int as codes, (select count(*) from core.variation_axes)::int as axes, (select count(*) from core.variation_options)::int as options,
  (select count(*) from core.sku_variation_choices)::int as choices, (select count(*) from ops.product_hub_outbox)::int as outbox, (select count(*) from ops.variation_batches)::int as batches,
  (select count(*) from ops.master_edit_requests)::int as requests, (select coalesce(sum(revision), 0)::int from ops.variation_group_revisions) as revs`);
const outboxOf = async (gid) => q(`select revision, payload, payload_hash, request_id::text as rid, created_by, entity_kind, entity_id::text as eid from ops.product_hub_outbox where group_product_id = $1 order by revision`, [gid]);
const revOf = async (gid) => (await one('select revision from ops.variation_group_revisions where group_product_id = $1', [gid]))?.revision ?? 0;
const failsWith = async (p, re, code) => {
  const e = await errOf(p);
  assert.ok(e, `断らなかった (${re})`);
  assert.match(String(e.message), re);
  if (code) assert.equal(e.code, code, e.message);
  return e;
};
const spec2 = (over = {}) => ({
  group: { code: 'Hakama', name: '袴' },
  axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
  options: [{ axis: 1, code: '-WH', name: '白' }, { axis: 1, code: '-BK', name: '黒' }, { axis: 2, code: '-90', name: '90cm' }],
  children: [{ code: 'Hakama-WH-90', choices: { 1: '-WH', 2: '-90' } }, { code: 'Hakama-BK-90', choices: { 1: '-BK', 2: '-90' } }],
  ...over,
});
const JAN_A = jan13('490000000101');

console.log('予約 (0067 の migration)');

await ta('[M1] 0067 = 今ある札 (grp1) と単品の代表 (s002) を予約 (load) に入れる・事前検査の SQL と DB の数え (ops.variation_reservation_check) が同じ・商品・親子は何も変えない', async () => {
  assert.deepEqual(PRE, { candidates: 2, dup: 0, tag_is_sku: 0 });
  const rows = await q('select code_norm, code, group_product_id::text as gid, source, reserved_by from ops.variation_group_codes order by code_norm');
  assert.deepEqual(rows, [{ code_norm: 'grp1', code: 'grp1', gid: GRP1, source: 'load', reserved_by: 'migration_0067' }, { code_norm: 's002', code: 's002', gid: REP2, source: 'load', reserved_by: 'migration_0067' }]);
  const chk = (await one('select ops.variation_reservation_check() as r')).r;
  assert.deepEqual([chk.candidates, chk.dup, chk.tag_is_sku, chk.reserved_other, chk.reserved], [PRE.candidates, PRE.dup, PRE.tag_is_sku, 0, 2]);
  // 何度流しても同じ (widen の前の流し直し = 持ち主だけ)
  const again = (await one(`select ops.reserve_existing_variation_groups('again') as r`)).r;
  assert.equal(again.inserted, 0);
  // 商品・親子は 0067 で変わらない (切替の backfill は登録の状態だけ)
  assert.deepEqual(await q('select product_id::text as id, parent_product_id::text as par, name from core.products order by product_id'), productsBefore);
  assert.equal((await one('select count(*)::int as n from ops.variation_group_revisions')).n, 0);
});

await ta('[M2] 重なりの事前検査: 同じ norm の札が 2 つ・札のコードが SKU = 0067 は止まる (何も入れない・migration の記録なし)・読むだけの SQL が同じ数を出す', async () => {
  const p2 = new PGlite();
  await p2.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await p2.query(`alter database ${(await p2.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await p2.query('set role deploy');
  const d2 = pgliteAdapter(p2);
  await applyMigrations(d2, { log: quiet, to: '0066' });
  const r2 = await runInitialLoad(d2, planOf(), { log: quiet, runId: 'load_vg5_b', ownership: MASTER_OWNERSHIP, now: LOAD_NOW });
  assert.equal(r2.ok, true, r2.error);
  await p2.query(`insert into core.products (company_id, display_code, name) values (1, 'GRP1', '同じ norm の札'), (1, 'ｓ００１', 'SKU と同じ札')`);
  assert.deepEqual((await p2.query(PRECHECK_SQL)).rows[0], { candidates: 4, dup: 1, tag_is_sku: 1 });
  const e = await errOf(applyMigrations(d2, { log: quiet }));
  assert.ok(e, '止まらなかった');
  assert.match(String(e.message), /reservation_collision/);
  assert.equal((await p2.query(`select count(*)::int as n from ops.schema_migrations where version = '0067'`)).rows[0].n, 0);
  assert.equal((await p2.query(`select to_regclass('ops.variation_group_codes') is null as gone`)).rows[0].gone, true);
  // 直す (同じ norm の札を別のコードに) と流せる
  await p2.query(`update core.products set display_code = 'grp1-old' where display_code = 'GRP1'`);
  await p2.query(`update core.products set display_code = 's001-tag' where display_code = 'ｓ００１'`);
  await applyMigrations(d2, { log: quiet });
  assert.equal((await p2.query(`select count(*)::int as n from ops.variation_group_codes`)).rows[0].n, 4);
  await p2.close();
});

console.log('\n持ち主の門');

await ta('[G1] products.parent の DB の active が load (今の本番と同じ = active と段階の記録の両方) = まとまりの関数は全部 parent_not_company (何も書かない)・ふつうの単品の登録は今までどおり', async () => {
  const LOAD_PARENT = { ...ALL_COMPANY, 'products.parent': 'load' };
  const OWN_L = JSON.stringify(LOAD_PARENT);
  await W2.setActiveOwnershipInDb(pg, LOAD_PARENT);
  try {
    const before = await counts();
    const e0 = await errOf(asEditor(async () => { await pg.query('begin'); try { await pg.query(openSql, [uuid(), 'naka@test', null, OWN_L, JSON.stringify(spec2({ group: { code: 'GateX', name: 'x' } }))]); } finally { await pg.query('rollback'); } }));
    assert.match(String(e0?.message), /^parent_not_company/);
    await failsWith(editorCall('select ops.variation_batch_close($1::uuid, $2, $3::jsonb, $4::jsonb) as r', [uuid(), 'naka@test', OWN_L, null]), /^no_open_batch/);
    await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', '理由', OWN_L, await skuIdOf('s006')]), /^parent_not_company/);
    await failsWith(editorCall('select ops.edit_variation_labels($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7::jsonb) as r', [uuid(), 'naka@test', null, OWN_L, GRP1, 0, '{"name":"x"}']), /^parent_not_company/);
    await failsWith(editorCall('select ops.adopt_ne_parent_for_quarantined($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', null, OWN_L, await skuIdOf('s001')]), /^parent_not_company/);
    // 呼び手が company と言っても DB の active で断る (段階の記録と違う持ち主表 = before_cutover)
    await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', '理由', OWN, await skuIdOf('s006')]), /^before_cutover/);
    assert.deepEqual(await counts(), before);
    assert.equal((await one('select ops.variation_parent_company() as c')).c, false);
    // ふつうの単品の登録 (親なし) は止まらない
    const r = await editorCall(regSql, [uuid(), 'naka@test', null, OWN_L, 'e'.repeat(64), JSON.stringify(entry('gate-single', 'ふつうの単品'))]);
    assert.equal(r.state, 'draft');
  } finally { await W2.setActiveOwnershipInDb(pg, ALL_COMPANY); }
  assert.equal((await one('select ops.variation_parent_company() as c')).c, true);
});

console.log('\nまとめての登録');

let HAKAMA = null;
await ta('[B1] 新しいまとまり (2 軸): 札・予約 (portal)・軸・選択肢・子の選択肢・子の親 (manual)・revision 1・スナップショット (形と DB の値に合う)・約束と done・カードは子ごとに作らない', async () => {
  const before = await counts();
  const names = { 'Hakama-WH-90': `袴【白】【90cm】【${JAN_A}】`, 'Hakama-BK-90': '袴【黒】【90cm】' };
  const r = await batch(spec2(), { names, jans: { 'Hakama-WH-90': JAN_A } });
  HAKAMA = r.close.group_product_id;
  assert.equal(r.open.group_code, 'Hakama'); assert.equal(r.open.group_created, true);
  assert.equal(r.close.revision, 1);
  // 子の request_id = まとめての request_id から (sha256(request_id:child:norm) の先頭 32 字 = janRequestId と同じ作り方)
  const sub = (tag) => { const h = crypto.createHash('sha256').update(`${r.rid}:${tag}`).digest('hex'); return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`; };
  assert.deepEqual(r.open.children.map((c) => c.request_id), [sub('child:hakama-wh-90'), sub('child:hakama-bk-90')]);
  assert.equal(r.open.close_request_id, sub('close'));
  // 札 = SKU の無い商品・予約 (portal・打ったとおり)
  assert.deepEqual(await one('select display_code, name, status, parent_product_id, created_by_type from core.products where product_id = $1', [HAKAMA]),
    { display_code: 'Hakama', name: '袴', status: 'active', parent_product_id: null, created_by_type: 'human' });
  assert.equal((await one('select count(*)::int as n from core.skus where product_id = $1', [HAKAMA])).n, 0);
  assert.deepEqual(await one('select code, code_norm, source from ops.variation_group_codes where group_product_id = $1', [HAKAMA]), { code: 'Hakama', code_norm: 'hakama', source: 'portal' });
  assert.deepEqual(await q('select axis, name from core.variation_axes where group_product_id = $1 order by axis', [HAKAMA]), [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }]);
  assert.deepEqual(await q('select axis, code, name, sort from core.variation_options where group_product_id = $1 order by axis, sort', [HAKAMA]),
    [{ axis: 1, code: '-WH', name: '白', sort: 0 }, { axis: 1, code: '-BK', name: '黒', sort: 1 }, { axis: 2, code: '-90', name: '90cm', sort: 0 }]);
  const kids = await q(`select s.code, p.parent_product_id::text as par, p.parent_set_by, r.state, o1.code as c1, o2.code as c2 from core.skus s join core.products p on p.product_id = s.product_id
      join ops.master_registrations r on r.sku_id = s.sku_id join core.sku_variation_choices ch on ch.sku_id = s.sku_id join core.variation_options o1 on o1.option_id = ch.option1_id
      left join core.variation_options o2 on o2.option_id = ch.option2_id where p.parent_product_id = $1 order by s.code`, [HAKAMA]);
  assert.deepEqual(kids, [{ code: 'Hakama-BK-90', par: HAKAMA, parent_set_by: 'manual', state: 'draft', c1: '-BK', c2: '-90' },
    { code: 'Hakama-WH-90', par: HAKAMA, parent_set_by: 'manual', state: 'draft', c1: '-WH', c2: '-90' }]);
  // 知らせ = まとまりの完全なスナップショット 1 件 (revision 1)・子ごとのカードの知らせは無い
  const ob = await outboxOf(HAKAMA);
  assert.equal(ob.length, 1);
  assert.deepEqual([ob[0].revision, ob[0].entity_kind, ob[0].eid, ob[0].rid, ob[0].created_by], [1, 'variation_group', HAKAMA, r.open.close_request_id, 'naka@test']);
  assert.equal(O.groupSnapshotShapeProblem(ob[0].payload), null);
  assert.equal(ob[0].payload_hash, O.groupPayloadHash(ob[0].payload));
  assert.equal((await one('select ops.group_snapshot_problem(1::smallint, $1::bigint, 1, $2::jsonb) as p', [HAKAMA, JSON.stringify(ob[0].payload)])).p, null);
  const p = ob[0].payload;
  assert.deepEqual(p.group, { product_id: HAKAMA, sku_id: null, code: 'Hakama', name: '袴', kind: 'tag' });
  assert.deepEqual(p.children.map((c) => [c.code, c.name, c.price, c.choices, c.jans]),
    [['Hakama-WH-90', names['Hakama-WH-90'], 1500, { 1: '-WH', 2: '-90' }, [JAN_A]], ['Hakama-BK-90', names['Hakama-BK-90'], 1500, { 1: '-BK', 2: '-90' }, []]]);
  assert.deepEqual(p.cancelled_children, []);
  assert.deepEqual(p.common, { shipping: null, amazon_url: null, asin: null, official_url: null, reference_urls: [], yahoo: null });
  assert.equal((await one(`select count(*)::int as n from ops.product_hub_outbox o join core.skus s on s.sku_id = o.sku_id where s.code like 'Hakama-%'`)).n, 0);
  // 約束と done: 開く・子の登録 2・JAN 1・閉じる = 5 (どれも同じ人・db_user は画面のロール)
  const after = await counts();
  assert.equal(after.requests - before.requests, 5);
  const ops2 = await q(`select operation, count(*)::int as n from ops.master_edit_requests where request_id = any($1::uuid[]) group by 1 order by 1`,
    [[r.rid, r.open.close_request_id, ...r.open.children.map((c) => c.request_id)]]);
  assert.deepEqual(ops2, [{ operation: 'sku_create', n: 2 }, { operation: 'variation_batch_close', n: 1 }, { operation: 'variation_batch_open', n: 1 }]);
  assert.equal((await one(`select count(*)::int as n from ops.master_write_sessions where operation like 'variation_%' and db_user <> 'master_edit'`)).n, 0);
  assert.deepEqual(await one('select status, group_created from ops.variation_batches where request_id = $1', [r.rid]), { status: 'closed', group_created: true });
  // 変更の記録 (軸・選択肢・子の選択肢・親) = 誰が・request_id は約束から
  const ev = await q(`select entity_type, count(*)::int as n from events.master_change_events where request_id = $1 group by 1 order by 1`, [r.open.close_request_id]);
  assert.deepEqual(ev, [{ entity_type: 'product', n: 4 }, { entity_type: 'sku_variation_choice', n: 2 }, { entity_type: 'variation_axis', n: 2 }, { entity_type: 'variation_option', n: 3 }]);
  assert.equal((await one(`select count(*)::int as n from events.master_change_events where request_id = $1 and (actor_id <> 'naka@test' or db_user <> 'master_edit')`, [r.open.close_request_id])).n, 0);
});

await ta('[B2] 同じ request_id = 前の答え (replayed・何も足さない)・違う中身 / 違う人 = request_id_reused', async () => {
  const rid = uuid();
  const spec = spec2({ group: { code: 'Replay1', name: '押し直し' }, children: [{ code: 'Replay1-WH-90', choices: { 1: '-WH', 2: '-90' } }] });
  const a = await batch(spec, { rid });
  const before = await counts();
  const b = await batch(spec, { rid });
  assert.equal(b.replayed, true);
  assert.equal(b.open.group_product_id, a.close.group_product_id);
  assert.equal(b.open.revision, 1);
  assert.deepEqual(await counts(), before);
  await failsWith(batch({ ...spec, group: { code: 'Replay1x', name: '違う' } }, { rid }), /^request_id_reused/);
  await failsWith(batch(spec, { rid, actor: 'other@test' }), /^request_id_reused/);
});

await ta('[B3] 断る (どれも全部巻き戻す = 札・予約・子・知らせ・done が残らない): まとまりのコード・選択肢番号・選択肢名・子のコード・選択肢・子の数・軸', async () => {
  const before = await counts();
  const g = (code) => ({ group: { code, name: 'x' } });
  const one1 = (code, extra = {}) => spec2({ ...g(code), children: [{ code: `${code}-WH-90`, choices: { 1: '-WH', 2: '-90' } }], ...extra });
  // まとまりのコード
  await failsWith(batch(one1('grp1')), /^group_exists/);
  await failsWith(batch(one1('GRP1')), /^group_exists/);
  await failsWith(batch(one1('Hakama')), /^group_exists/);
  await failsWith(batch(one1('s001')), /^code_taken/);
  await failsWith(batch(one1('set-abc')), /^code_shape/);
  await failsWith(batch(spec2({ group: { code: 'a b', name: 'x' } })), /^code_shape/);
  await newRunToday(['neonly1']);   // NE にだけあるコード (商品)
  await failsWith(batch(one1('NeOnly1')), /^code_in_ne/);
  // まとまりの名前
  await failsWith(batch(spec2({ group: { code: 'NmBad', name: ' 前に空白' } })), /^invalid_value/);
  // 選択肢番号の形・軸ごとの一意 (大文字小文字を問わず)・選択肢名の一意 (NFKC)
  for (const bad of ['WH', '-', '-W-H', '-W_H', '-ABCDEFGHIJK', '-白']) {
    await failsWith(batch(spec2({ group: { code: 'OptBad', name: 'x' }, options: [{ axis: 1, code: bad, name: 'x' }, { axis: 2, code: '-90', name: '90' }], children: [{ code: `OptBad${bad}-90`, choices: { 1: bad, 2: '-90' } }] })), /option_code_shape/);
  }
  await failsWith(batch(spec2({ group: { code: 'OptDup', name: 'x' }, options: [{ axis: 1, code: '-WH', name: '白' }, { axis: 1, code: '-wh', name: '白 2' }, { axis: 2, code: '-90', name: '90' }] })), /^option_exists/);
  await failsWith(batch(spec2({ group: { code: 'OptNm', name: 'x' }, options: [{ axis: 1, code: '-A', name: 'Ａ白' }, { axis: 1, code: '-B', name: 'A白' }, { axis: 2, code: '-90', name: '90' }],
    children: [{ code: 'OptNm-A-90', choices: { 1: '-A', 2: '-90' } }] })), /^option_name_exists/);
  // 子のコード = まとまり + 選択肢 (打ったとおり)・選択肢が無い / 足りない・同じ子・(横, 縦) の一意・20 件まで・長すぎ
  await failsWith(batch(spec2({ group: { code: 'ChCase', name: 'x' }, children: [{ code: 'chcase-WH-90', choices: { 1: '-WH', 2: '-90' } }] })), /^child_code_not_group_plus_choices/);
  await failsWith(batch(spec2({ group: { code: 'ChUnk', name: 'x' }, children: [{ code: 'ChUnk-wh-90', choices: { 1: '-wh', 2: '-90' } }] })), /^choice_unknown/);
  await failsWith(batch(spec2({ group: { code: 'ChOne', name: 'x' }, children: [{ code: 'ChOne-WH', choices: { 1: '-WH' } }] })), /^invalid_input/);
  await failsWith(batch(spec2({ group: { code: 'ChDup', name: 'x' }, children: [{ code: 'ChDup-WH-90', choices: { 1: '-WH', 2: '-90' } }, { code: 'ChDup-WH-90', choices: { 1: '-WH', 2: '-90' } }] })), /^child_dup/);
  const many = Array.from({ length: 21 }, (_, i) => ({ axis: 1, code: `-C${i}`, name: `色 ${i}` }));
  await failsWith(batch(spec2({ group: { code: 'Many', name: 'x' }, options: [...many, { axis: 2, code: '-90', name: '90' }], children: many.map((o) => ({ code: `Many${o.code}-90`, choices: { 1: o.code, 2: '-90' } })) })), /^too_many/);
  await failsWith(batch(spec2({ group: { code: 'L'.repeat(20), name: 'x' }, options: [{ axis: 1, code: '-ABCDEFGHIJ', name: 'x' }, { axis: 2, code: '-90', name: '90' }],
    children: [{ code: `${'L'.repeat(20)}-ABCDEFGHIJ-90`, choices: { 1: '-ABCDEFGHIJ', 2: '-90' } }] })), /^code_shape/);
  // 子のコードが前からある (Company DB の SKU) = 0064 の決まり (code_taken)
  await editorCall(regSql, [uuid(), 'naka@test', null, OWN, 'e'.repeat(64), JSON.stringify(entry('Zz-1', '前からある単品'))]);
  await failsWith(batch({ group: { code: 'Zz', name: 'x' }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-1', name: '1' }], children: [{ code: 'Zz-1', choices: { 1: '-1' } }] }), /^code_taken/);
  // 軸の番号・選択肢の鍵が 1 / 2 でない (1.5・'x') = invalid_input (数にしない)
  await failsWith(batch(spec2({ group: { code: 'Ax15', name: 'x' }, options: [{ axis: 1.5, code: '-Z', name: 'z' }] })), /^invalid_input/);
  await failsWith(batch(spec2({ group: { code: 'AxKey', name: 'x' }, children: [{ code: 'AxKey-WH-90', choices: { 1: '-WH', x: '-90' } }] })), /^invalid_input/);
  // 新しいまとまりの軸が無い・3 つ・並びが違う
  await failsWith(batch({ group: { code: 'NoAx', name: 'x' }, options: [], children: [{ code: 'NoAx-1', choices: { 1: '-1' } }] }), /^invalid_input: 軸/);
  await failsWith(batch(spec2({ group: { code: 'AxOrd', name: 'x' }, axes: [{ axis: 2, name: 'サイズ' }, { axis: 1, name: 'カラー' }] })), /^invalid_input: 軸/);
  const after = await counts();
  // 前からある単品 Zz-1 (1 回の登録) の分だけ増えている。それ以外は何も残っていない
  assert.deepEqual({ ...after, products: after.products - 1, skus: after.skus - 1, requests: after.requests - 1 }, before);
});

await ta('[B4] 開いて閉じない = commit で断る (variation_batch_unfinished・札も子も残らない)・閉じる時の子の確かめ (登録していない・この取引の子でない・子の request_id・カードの知らせ・違う人)', async () => {
  const before = await counts();
  await failsWith(batch(spec2({ group: { code: 'Unf', name: 'x' }, children: [{ code: 'Unf-WH-90', choices: { 1: '-WH', 2: '-90' } }] }), { skipClose: true }), /^variation_batch_unfinished/);
  // 子を登録しないで閉じる
  await failsWith(asEditor(async () => {
    await pg.query('begin');
    try {
      const rid = uuid();
      await pg.query(openSql, [rid, 'naka@test', null, OWN, JSON.stringify(spec2({ group: { code: 'NoReg', name: 'x' }, children: [{ code: 'NoReg-WH-90', choices: { 1: '-WH', 2: '-90' } }] }))]);
      await pg.query(closeSql, [rid, 'naka@test', OWN, null]);
    } finally { await pg.query('rollback'); }
  }), /^child_not_registered/);
  // 子の request_id がまとめての登録から決めた番号でない
  await failsWith(batch(spec2({ group: { code: 'BadRid', name: 'x' }, children: [{ code: 'BadRid-WH-90', choices: { 1: '-WH', 2: '-90' } }] }), { regRid: () => uuid() }), /^child_request_mismatch/);
  // 子にカードの知らせ (登録の card あり)
  const card = (code) => ({ schema_version: 'ph-card-v1', payload: { schema: 'ph-card-v1', code, kind: 'single', name: `子 ${code}`, price: 1500, shipping: { code: null, method: null, cost_jpy: null },
    amazon_url: null, asin: null, official_url: null, reference_urls: [], set_decision: null, yahoo: null, components: [], created_by: 'naka@test' } });
  await failsWith(batch(spec2({ group: { code: 'Card1', name: 'x' }, children: [{ code: 'Card1-WH-90', choices: { 1: '-WH', 2: '-90' } }] }), { childEntry: (code, e) => ({ ...e, card: card(code) }) }), /^child_card_event/);
  // 閉じるのは開いた人だけ
  await failsWith(asEditor(async () => {
    await pg.query('begin');
    try {
      const rid = uuid();
      const o = (await pg.query(openSql, [rid, 'naka@test', null, OWN, JSON.stringify(spec2({ group: { code: 'Who', name: 'x' }, children: [{ code: 'Who-WH-90', choices: { 1: '-WH', 2: '-90' } }] }))])).rows[0].r;
      await pg.query(regSql, [o.children[0].request_id, 'naka@test', null, OWN, 'e'.repeat(64), JSON.stringify(entry('Who-WH-90', 'x'))]);
      await pg.query(closeSql, [rid, 'other@test', OWN, null]);
    } finally { await pg.query('rollback'); }
  }), /^master_write_session_mismatch/);
  // 前の取引で登録した下書きを子にしようとする = child_not_new (この取引で作った子だけ)
  const rid0 = uuid();
  const pre = (sub) => { const h = crypto.createHash('sha256').update(`${rid0}:${sub}`).digest('hex'); return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`; };
  await editorCall(regSql, [pre('child:old-wh-90'), 'naka@test', null, OWN, 'e'.repeat(64), JSON.stringify(entry('Old-WH-90', '前の取引の子'))]);
  const e = await errOf(asEditor(async () => {
    await pg.query('begin');
    try {
      // 前の取引の子のコードと同じ子 = 開く時点で code_taken (新しいコードの決まり) = 前の取引の下書きは子にできない
      await pg.query(openSql, [rid0, 'naka@test', null, OWN, JSON.stringify(spec2({ group: { code: 'Old', name: 'x' }, children: [{ code: 'Old-WH-90', choices: { 1: '-WH', 2: '-90' } }] }))]);
    } finally { await pg.query('rollback'); }
  }));
  assert.match(String(e?.message), /^code_taken/);
  const after = await counts();
  assert.deepEqual({ ...after, products: after.products - 1, skus: after.skus - 1, requests: after.requests - 1 }, before, '前の取引の下書き 1 つのほかは何も残らない');
});

await ta('[B5] 1 つの取引で全部か何も無いか: 2 つ目の子の登録が断られる = 札・予約・1 つ目の子も残らない (アプリは取引ごと巻き戻す)', async () => {
  const before = await counts();
  await failsWith(batch(spec2({ group: { code: 'Half', name: 'x' }, children: [{ code: 'Half-WH-90', choices: { 1: '-WH', 2: '-90' } }, { code: 'Half-BK-90', choices: { 1: '-BK', 2: '-90' } }] }), { childEntry: (code, e) => (code === 'Half-BK-90' ? { ...e, sku: { ...e.sku, name: ' 空白' }, product: { ...e.product, name: ' 空白' } } : e) }), /invalid_value/);
  assert.deepEqual(await counts(), before);
  assert.equal((await one(`select count(*)::int as n from core.products where display_code = 'Half' or display_code like 'Half-%'`)).n, 0);
});

await ta('[B6] 今ある札 (夜間ロードの grp1・軸なし) に初めて軸を入れて足す → 同じ軸で足す・軸は変えない・選択肢 / 組がもうある = 断る。前からの子は選択肢 {} のまま', async () => {
  const r1 = await batch({ group: { product_id: GRP1 }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-RD', name: '赤' }], children: [{ code: 'grp1-RD', choices: { 1: '-RD' } }] });
  assert.equal(r1.close.revision, 1);
  assert.equal(r1.open.group_created, false);
  const p1 = (await outboxOf(GRP1))[0].payload;
  assert.equal(O.groupSnapshotShapeProblem(p1), null);
  assert.deepEqual(p1.group, { product_id: GRP1, sku_id: null, code: 'grp1', name: '札の子', kind: 'tag' });   // 名前 = 夜間ロードが子の名前から (【 より前)
  assert.deepEqual(p1.children.map((c) => [c.code, c.choices]), [['s006', {}], ['s007', {}], ['grp1-RD', { 1: '-RD' }]]);
  const r2 = await batch({ group: { product_id: GRP1 }, options: [{ axis: 1, code: '-BL', name: '青 (足し)' }], children: [{ code: 'grp1-BL', choices: { 1: '-BL' } }] });
  assert.equal(r2.close.revision, 2);
  assert.deepEqual(await q('select code, sort from core.variation_options where group_product_id = $1 order by sort', [GRP1]), [{ code: '-RD', sort: 0 }, { code: '-BL', sort: 1 }]);
  await failsWith(batch({ group: { product_id: GRP1 }, axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-YE', name: '黄' }], children: [{ code: 'grp1-YE', choices: { 1: '-YE' } }] }), /^axes_fixed/);
  await failsWith(batch({ group: { product_id: GRP1 }, options: [{ axis: 1, code: '-rd', name: '赤 2' }], children: [{ code: 'grp1-rd', choices: { 1: '-rd' } }] }), /^option_exists/);
  await failsWith(batch({ group: { product_id: GRP1 }, options: [{ axis: 1, code: '-R2', name: '赤' }], children: [{ code: 'grp1-R2', choices: { 1: '-R2' } }] }), /^option_name_exists/);
  await failsWith(batch({ group: { product_id: GRP1 }, children: [{ code: 'grp1-RD', choices: { 1: '-RD' } }] }), /^choice_exists/);
  // 2 軸のまとまりに縦を足す・同じ組
  const r3 = await batch({ group: { product_id: HAKAMA }, options: [{ axis: 2, code: '-95', name: '95cm' }], children: [{ code: 'Hakama-WH-95', choices: { 1: '-WH', 2: '-95' } }] });
  assert.equal(r3.close.revision, 2);
  await failsWith(batch({ group: { product_id: HAKAMA }, children: [{ code: 'Hakama-WH-90', choices: { 1: '-WH', 2: '-90' } }] }), /^choice_exists/);
  assert.equal(await revOf(GRP1), 2);
  assert.equal(await revOf(HAKAMA), 2);
});

await ta('[B7] 単品の代表 (s002・子 s004) に足す (スナップショットの kind = single)・子の無い単品 (s001) は今あるまとまりでない・親を持つ商品はまとまりにできない', async () => {
  const r = await batch({ group: { product_id: REP2 }, axes: [{ axis: 1, name: 'サイズ' }], options: [{ axis: 1, code: '-XL', name: 'XL' }], children: [{ code: 's002-XL', choices: { 1: '-XL' } }] });
  const p = (await outboxOf(REP2))[0].payload;
  assert.equal(O.groupSnapshotShapeProblem(p), null);
  assert.deepEqual([p.group.kind, p.group.code, p.group.sku_id], ['single', 's002', await skuIdOf('s002')]);
  assert.deepEqual(p.children.map((c) => c.code), ['s004', 's002-XL']);
  assert.equal(r.close.revision, 1);
  await failsWith(batch({ group: { product_id: await pidOf('s001') }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-1', name: '1' }], children: [{ code: 's001-1', choices: { 1: '-1' } }] }), /^not_a_group/);
  await failsWith(batch({ group: { product_id: await pidOf('s006') }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-1', name: '1' }], children: [{ code: 's006-1', choices: { 1: '-1' } }] }), /^not_a_group|^group_has_parent/);
  await failsWith(batch({ group: { product_id: '999999' }, children: [{ code: 'x-1', choices: { 1: '-1' } }] }), /^not_found/);
});

await ta('[B8] 1 つの取引で同じまとまりを 2 回変える = revision_twice (変わる取引で revision はちょうど 1)・取引ごと巻き戻る', async () => {
  const before = await counts();
  const rev = await revOf(GRP1);
  const e = await errOf(asEditor(async () => {
    await pg.query('begin');
    try {
      for (const c of ['-T1', '-T2']) {
        const rid = uuid();
        const o = (await pg.query(openSql, [rid, 'naka@test', null, OWN, JSON.stringify({ group: { product_id: GRP1 }, options: [{ axis: 1, code: c, name: `二回 ${c}` }], children: [{ code: `grp1${c}`, choices: { 1: c } }] })])).rows[0].r;
        await pg.query(regSql, [o.children[0].request_id, 'naka@test', null, OWN, 'e'.repeat(64), JSON.stringify(entry(`grp1${c}`, `二回 ${c}`))]);
        await pg.query(closeSql, [rid, 'naka@test', OWN, null]);
      }
      await pg.query('commit');
    } catch (x) { await pg.query('rollback'); throw x; }
  }));
  assert.match(String(e?.message), /^revision_twice/);
  assert.equal(await revOf(GRP1), rev);
  assert.deepEqual(await counts(), before);
});

await ta('[B9] 共通の欄 (common) は閉じるときに残し、後の revision のスナップショットにも出る・形が違えば断る', async () => {
  const common = { shipping: { code: 'S01', method: 'ゆうパケット', cost_jpy: 210 }, amazon_url: null, asin: null, official_url: 'https://maker.example/x', reference_urls: ['https://a.example'], yahoo: null };
  const r = await batch(spec2({ group: { code: 'Common1', name: '共通' }, children: [{ code: 'Common1-WH-90', choices: { 1: '-WH', 2: '-90' } }] }), { common });
  assert.deepEqual((await outboxOf(r.close.group_product_id))[0].payload.common, common);
  await failsWith(batch(spec2({ group: { code: 'Common2', name: 'x' }, children: [{ code: 'Common2-WH-90', choices: { 1: '-WH', 2: '-90' } }] }), { common: { shipping: null } }), /common_keys/);
});

console.log('\n名前を直す・revision');

await ta('[R1] まとまりの名前 (札)・軸の名前・選択肢名を直す: revision + 1・スナップショットに新しい名前・コードと番号はそのまま・見た revision が違う = version_conflict・変わらない = 何も書かない', async () => {
  const rev = await revOf(HAKAMA);
  const before = await counts();
  const noop = await editorCall('select ops.edit_variation_labels($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7::jsonb) as r', [uuid(), 'naka@test', null, OWN, HAKAMA, rev, JSON.stringify({ name: '袴', axes: [{ axis: 1, name: 'カラー' }] })]);
  assert.equal(noop.no_change, true);
  assert.deepEqual(await counts(), before);
  await failsWith(editorCall('select ops.edit_variation_labels($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7::jsonb) as r', [uuid(), 'naka@test', null, OWN, HAKAMA, rev - 1, '{"name":"新しい袴"}']), /^version_conflict/);
  const r = await editorCall('select ops.edit_variation_labels($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7::jsonb) as r', [uuid(), 'naka@test', '名前を直す', OWN, HAKAMA, rev,
    JSON.stringify({ name: '新しい袴', axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-WH', name: 'ホワイト' }] })]);
  assert.equal(r.revision, rev + 1);
  const p = (await outboxOf(HAKAMA)).pop().payload;
  assert.equal(p.revision, rev + 1);
  assert.equal(p.group.name, '新しい袴'); assert.equal(p.group.code, 'Hakama');
  assert.deepEqual(p.axes, [{ axis: 1, name: '色' }, { axis: 2, name: 'サイズ' }]);
  assert.equal(p.options.find((o) => o.code === '-WH').name, 'ホワイト');
  assert.deepEqual(await one('select display_code from core.products where product_id = $1', [HAKAMA]), { display_code: 'Hakama' });
  // 子の名前 (商品名) は自動では変えない
  assert.equal(p.children.find((c) => c.code === 'Hakama-WH-90').name, `袴【白】【90cm】【${JAN_A}】`);
});

await ta('[R2] 断る: 選択肢名がほかと同じ (NFKC)・単品の代表のまとまりの名前・知らない軸 / 選択肢 (番号は打ったとおり)・巻き戻した取引は revision を上げない', async () => {
  const rev = await revOf(HAKAMA);
  const call = (gid, seen, ch) => editorCall('select ops.edit_variation_labels($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7::jsonb) as r', [uuid(), 'naka@test', null, OWN, gid, seen, JSON.stringify(ch)]);
  await failsWith(call(HAKAMA, rev, { options: [{ axis: 1, code: '-BK', name: 'ホワイト' }] }), /^option_name_exists/);
  await failsWith(call(REP2, await revOf(REP2), { name: '代表の名前' }), /^group_name_is_single/);
  await failsWith(call(HAKAMA, rev, { axes: [{ axis: 2, name: '' }] }), /^invalid_value/);
  await failsWith(call(HAKAMA, rev, { options: [{ axis: 1, code: '-wh', name: 'x' }] }), /^not_found/);
  await failsWith(call(GRP1, await revOf(GRP1), { axes: [{ axis: 2, name: '縦' }] }), /^not_found/);
  // 取引の中で直してから巻き戻す = revision は上がらない・知らせも残らない
  const nOut = (await outboxOf(HAKAMA)).length;
  await asEditor(async () => {
    await pg.query('begin');
    await pg.query('select ops.edit_variation_labels($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6, $7::jsonb)', [uuid(), 'naka@test', null, OWN, HAKAMA, rev, '{"name":"巻き戻す"}']);
    await pg.query('rollback');
  });
  assert.equal(await revOf(HAKAMA), rev);
  assert.equal((await outboxOf(HAKAMA)).length, nOut);
});

console.log('\n親か子のどちらか一方');

await ta('[P1] products.parent が company = 親を持つ商品は子を持てない・子を持つ商品は親を持てない (commit のときの形・どのロールでも)・load の間は見ない (夜間ロードを止めない)', async () => {
  const setParent = async (child, parent, { immediate = true } = {}) => {
    await pg.query('begin');
    try {
      await pg.query(`select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())`);
      await pg.query('update core.products set parent_product_id = $2 where product_id = $1', [child, parent]);
      if (immediate) await pg.query('set constraints all immediate');
    } finally { await pg.query('rollback'); }
  };
  // 子を持つ札 (grp1) に親 = 2 段
  await failsWith(setParent(GRP1, HAKAMA), /^parent_two_level/);
  // 親を持つ子 (s006) を親にする = 2 段
  await failsWith(setParent(await pidOf('s001'), await pidOf('s006')), /^parent_two_level/);
  // load の間 (active の products.parent = load) は見ない
  await W2.setActiveMapOnly(pg, { ...ALL_COMPANY, 'products.parent': 'load' });
  try {
    assert.equal(await errOf(setParent(await pidOf('s001'), await pidOf('s006'))), null);
  } finally { await W2.setActiveMapOnly(pg, ALL_COMPANY); }
  const t = await one(`select tgdeferrable as d, tginitdeferred as i from pg_trigger where tgname = 'trg_products_parent_one_level'`);
  assert.deepEqual(t, { d: true, i: true });
});

console.log('\nNE 登録の CSV (まとまりの版 ne-reg-variation-v1)');

const opts = (o = {}) => ({ ownership: ALL_COMPANY, open: true, nowMs: NOW_MS, ...o });
const build = (codes, o = {}) => asEditor(() => G.buildRegExport(db, { actor: 'boss@test', kind: 'products', codes, requestId: uuid(), variation: o.variation }, opts()));
await ta('[N1] ポータルで作った札の子 = NE に無いまとまりでも代表商品コード = 予約のコード (打ったとおり)・lib の regMaterialOf = DB の ops.ne_reg_canonical・JAN = empty', async () => {
  const id = await skuIdOf('Hakama-WH-90');
  const canon = (await one('select ops.ne_reg_canonical($1, $2::date) as c', [id, '2030-01-10'])).c;
  assert.deepEqual(canon.blockers, []);
  assert.equal(canon.cells[0][7], 'Hakama');
  assert.equal(canon.cells[0][8], 'empty');
  assert.equal(canon.expected.values.parent, 'hakama');
  const ctx = { today: '2030-01-10', nc: await G.neCodeLookup(db, ['Hakama-WH-90']), live: new Map(), regByIds: new Map(), supRegByIds: new Map(), portalGroup: G.portalGroupLookup(db) };
  const m = await asEditor(() => G.regMaterialOf(db, id, ctx));
  assert.deepEqual(m.blockers, []);
  assert.deepEqual(m.cells.map((r) => r.map(String)), canon.cells);
  assert.deepEqual(m.expected, canon.expected);
  // 予約の無い (portal でない) まとまり = NE の書き方が要る (load の札 grp1 は NE にある = NE の書き方)
  const g1 = (await one('select ops.ne_reg_canonical($1, $2::date) as c', [await skuIdOf('grp1-RD'), '2030-01-10'])).c;
  assert.equal(g1.cells[0][7], 'grp1');
  // portalGroup を渡さない (古い呼び手) = 止まる理由 (DB と食い違う向き = 作らない側)
  const m2 = await asEditor(() => G.regMaterialOf(db, id, { ...ctx, portalGroup: undefined }));
  assert.ok(m2.blockers.some((b) => /代表 \(親\)/.test(b)));
});

await ta('[N2] まとまりで 1 ファイル: 全部の子 = 作れる (版 variation-v1・全部の行の代表 = Hakama)・子が欠ける = variation_incomplete・2 つのまとまり = variation_mixed・単品の版 = variation_file_required・子でない = not_ready', async () => {
  const kids = (await q(`select s.code from core.skus s join core.products p on p.product_id = s.product_id join ops.master_registrations r on r.sku_id = s.sku_id
     where p.parent_product_id = $1 and r.state = 'draft' order by s.code`, [HAKAMA])).map((r) => r.code);
  assert.deepEqual(kids, ['Hakama-BK-90', 'Hakama-WH-90', 'Hakama-WH-95']);
  const e1 = await errOf(build(kids.slice(0, 2), { variation: true }));
  assert.equal(e1?.reason, 'variation_incomplete', e1?.message);
  const e2 = await errOf(build([...kids, 'grp1-RD'], { variation: true }));
  assert.equal(e2?.reason, 'variation_mixed', e2?.message);
  const e3 = await errOf(build(['Hakama-WH-90']));
  assert.equal(e3?.reason, 'variation_file_required', e3?.message);
  const e4 = await errOf(build(['gate-single'], { variation: true }));
  assert.equal(e4?.reason, 'not_ready', e4?.message);
  const f = await build(kids, { variation: true });
  assert.equal(f.export.schema_version, 'ne-reg-variation-v1');
  assert.equal(f.export.trial, false);
  const rows = (await q('select cells from ops.ne_reg_export_rows where export_id = $1 order by row_no', [f.export.export_id])).map((r) => r.cells);
  assert.deepEqual(rows.map((c) => [c[0], c[7], c[8]]), [['Hakama-BK-90', 'Hakama', 'empty'], ['Hakama-WH-90', 'Hakama', 'empty'], ['Hakama-WH-95', 'Hakama', 'empty']]);
  // 生きているファイルのある子は廃止できない ([C2])・使わないにして戻す
  await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', OWN, await skuIdOf('Hakama-WH-95')]), /^live_file/);
  await asEditor(() => G.supersedeRegExport(db, { actor: 'boss@test', exportId: f.export.export_id, reason: '試験', correction: 'NE には取り込んでいない', confirm: true }, opts()));
  // 今ある札 (NE にある grp1) の子も variation-v1 で作れる (NE の書き方)・単品の版でも作れる (ポータルの札でない)
  const g = (await q(`select s.code from core.skus s join core.products p on p.product_id = s.product_id join ops.master_registrations r on r.sku_id = s.sku_id
     where p.parent_product_id = $1 and r.state = 'draft' order by s.code`, [GRP1])).map((r) => r.code);
  const fg = await build(g, { variation: true });
  assert.deepEqual((await q('select cells from ops.ne_reg_export_rows where export_id = $1 order by row_no', [fg.export.export_id])).map((r) => r.cells[7]), g.map(() => 'grp1'));
  await asEditor(() => G.supersedeRegExport(db, { actor: 'boss@test', exportId: fg.export.export_id, reason: '試験', correction: 'NE には取り込んでいない', confirm: true }, opts()));
  // lib の形の版の決まり = DB (variation-v1 は作れる版)
  assert.deepEqual((await one(`select ops.ne_reg_schema_rule('ne-reg-variation-v1') as r`)).r, { kind: 'products', sku_kind: 'single', header: G.REG_VARIATION_SCHEMA.header.join(','),
    buildable: true, why: null, trial_gate: false, jan: 'empty', parent: 'group' });
});

console.log('\n子の廃止');

await ta('[C1] 子の廃止: 下書きの子 → cancelled・revision + 1・スナップショットの廃止した子に移る・コードは使い回さない・まとまりやほかの子は変えない', async () => {
  const rev = await revOf(HAKAMA);
  const sid = await skuIdOf('Hakama-WH-95');
  const r = await editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'NE が受けなかった', OWN, sid]);
  assert.deepEqual([r.state, r.revision], ['cancelled', rev + 1]);
  assert.equal((await one('select state from ops.master_registrations where sku_id = $1', [sid])).state, 'cancelled');
  const p = (await outboxOf(HAKAMA)).pop().payload;
  assert.equal(O.groupSnapshotShapeProblem(p), null);
  assert.deepEqual(p.cancelled_children, [{ sku_id: sid, code: 'Hakama-WH-95' }]);
  assert.ok(!p.children.some((c) => c.code === 'Hakama-WH-95'));
  assert.ok(p.options.some((o) => o.code === '-95'), '選択肢は残る');
  assert.equal((await one('select parent_product_id::text as p from core.products where product_id = (select product_id from core.skus where sku_id = $1)', [sid])).p, HAKAMA, '親はそのまま');
  await failsWith(batch({ group: { product_id: HAKAMA }, children: [{ code: 'Hakama-WH-95', choices: { 1: '-WH', 2: '-95' } }] }), /^choice_exists|^code_taken/);
});

await ta('[C2] 廃止を断る: NE に一度でも現れた子 (seen_in_ne)・まとまりの子でない・廃止した子・理由なし', async () => {
  await newRunToday(['neonly1', 'common1-wh-90']);
  await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', OWN, await skuIdOf('Common1-WH-90')]), /^seen_in_ne/);
  await newRunToday(['neonly1']);
  // 今の NE から消えても前に見たコード (履歴) = まだ断る
  await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', OWN, await skuIdOf('Common1-WH-90')]), /^seen_in_ne/);
  await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', OWN, await skuIdOf('s001')]), /^not_variation_child/);
  await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', OWN, await skuIdOf('Hakama-WH-95')]), /^child_not_cancellable/);
  await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', '', OWN, await skuIdOf('Hakama-WH-90')]), /^invalid_input/);
});

console.log('\nNE に配った後');

await ta('[N3] まとまりの版のファイルを配る → 翌朝の照合で NE の代表 = Hakama (norm) = 期待値 = 今の Company DB の親 (3 者一致) → verified・NE 確認済み → その後は廃止できない・親は変えない (0065 の守り)', async () => {
  const kids = ['Hakama-BK-90', 'Hakama-WH-90'];   // Hakama-WH-95 は廃止した = 入れなくてよい
  const f = await build(kids, { variation: true });
  const iss = await asEditor(() => G.issueRegExport(db, { actor: 'boss@test', exportId: f.export.export_id }, opts()));
  assert.equal(iss.export.state, 'issued');
  const run = await newRunToday(['neonly1', 'Hakama-BK-90', 'Hakama-WH-90'], [{ code_norm: 'grp1', ne_code: 'grp1' }, { code_norm: 'hakama', ne_code: 'Hakama' }]);
  const snap = (await asRole('watch_writer', () => pg.query('select ops.snapshot_ne_reg_targets($1) as r', [run]))).rows[0].r;
  const ok = (v) => ({ st: 'ok', v });
  const want = new Map();
  for (const c of kids) {
    const s = await one('select s.name from core.skus s where s.code = $1', [c]);
    want.set(c.toLowerCase(), { code_norm: c.toLowerCase(), present: true, trusted: true, kind: 'single', cols: { name: ok(s.name), supplier: ok('0001'), cost: ok(300), price: ok(1500),
      tax_rate: ok(0.1), handling: ok('active'), parent: ok('hakama') } });
  }
  const observations = snap.targets.map((t) => want.get(t.code_norm) ?? { code_norm: t.code_norm, present: false, trusted: true, kind: null });
  assert.deepEqual(snap.targets.map((t) => t.code_norm).filter((c) => want.has(c)).sort(), ['hakama-bk-90', 'hakama-wh-90']);
  const at = new Date(Date.now() + 60000).toISOString();
  const w = (await asRole('watch_writer', () => pg.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run,
    fetch: { generation_id: 'gen_n3', products_rev: '1', sets_rev: '1', raw_hash: 'd'.repeat(64) }, products_at: at, sets_at: at, absence_trusted: true, observations })]))).rows[0].r;
  await asRole('watch_writer', () => pg.query('select ops.seal_ne_registration_run($1, $2, $3)', [run, w.observation_hash, 'e'.repeat(64)]));
  const c = (await asRole('watch_writer', () => pg.query('select ops.record_ne_registration_check($1) as r', [run]))).rows[0].r;
  assert.deepEqual([c.counts, c.cdb_drift], [{ verified: 2 }, []]);
  assert.deepEqual((await q(`select s.code, r.state from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id where s.code = any($1::text[]) order by s.code`, [kids])).map((r) => r.state),
    ['ne_confirmed', 'ne_confirmed']);
  // NE 確認済み = 凍結 (廃止できない・親は変えない)
  await failsWith(editorCall('select ops.cancel_variation_child($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'やめる', OWN, await skuIdOf('Hakama-WH-90')]), /^child_not_cancellable|^seen_in_ne/);
  const e = await errOf((async () => {
    await pg.query('begin');
    try {
      await pg.query(`select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())`);
      await pg.query('update core.products set parent_product_id = null, parent_set_by = null where product_id = $1', [await pidOf('Hakama-WH-90')]);
    } finally { await pg.query('rollback'); }
  })());
  assert.match(String(e?.message), /^parent_frozen/);
});

console.log('\nquarantined の代表の採用');

let RUN2 = null;
await ta('[A1] NE で直接作られた商品 (quarantined・親なし) が照合の確かめ待ちに入る (company のときだけ)・観測と封の後に採用: 今ある札 / 札を作る (ne_adopt) / 子の無い単品・1 回だけ・その後は確かめ待ちから外れる', async () => {
  // 夜間ロードが NE で見つけた商品 (切替の後 = quarantined・products.parent が company = 親を付けない)
  const extra = [sku('q001', '袋 赤【赤】', { representativeCode: 'grp1', representativeState: 'value' }), sku('q002', '新しい代表の子【白】', { representativeCode: 'NewRep', representativeState: 'value' }),
    sku('q003', '単品が代表の子', { representativeCode: 's005', representativeState: 'value' }), sku('q004', '代表なし')];
  const lr = await runInitialLoad(db, { ...planOf(extra), variationGroups: [...planOf().variationGroups, { code: 'NewRep', name: 'x', childCodes: ['q002'], status: 'active' }] }, { log: quiet, runId: 'load_vg5_q', now: LOAD_NOW });
  assert.equal(lr.ok, true, lr.error);
  assert.deepEqual((await q(`select s.code, r.state, p.parent_product_id from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id join core.products p on p.product_id = s.product_id
     where s.code like 'q00%' order by s.code`)).map((r) => [r.code, r.state, r.parent_product_id]), [['q001', 'quarantined', null], ['q002', 'quarantined', null], ['q003', 'quarantined', null], ['q004', 'quarantined', null]]);
  const tg = async () => (await q(`select code_norm, state from ops.v_ne_reg_targets where state = 'quarantined' order by code_norm`)).map((r) => r.code_norm);
  assert.deepEqual(await tg(), ['q001', 'q002', 'q003', 'q004']);
  await W2.setActiveMapOnly(pg, { ...ALL_COMPANY, 'products.parent': 'load' });
  try { assert.deepEqual(await tg(), []); } finally { await W2.setActiveMapOnly(pg, ALL_COMPANY); }
  // 照合 ②: 回 → NE の元のコード (代表 grp1 / NewRep・商品 s005) → 確かめ待ちの写し → 観測 → 封
  RUN2 = 'mc_20300111T000000000Z_bbbbbb';
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-11T00:00:00Z', 0)`, [RUN2]);
  await recordNeCodes(RUN2, ['neonly1', 'q001', 'q002', 'q003', 'q004'], [{ code_norm: 'grp1', ne_code: 'grp1' }, { code_norm: 'newrep', ne_code: 'NewRep' }]);
  const snap = (await asRole('watch_writer', () => pg.query('select ops.snapshot_ne_reg_targets($1) as r', [RUN2]))).rows[0].r;
  const ok = (v) => ({ st: 'ok', v });
  const obs = { q001: 'grp1', q002: 'newrep', q003: 's005', q004: null };
  const observations = snap.targets.map((t) => (Object.prototype.hasOwnProperty.call(obs, t.code_norm)
    ? { code_norm: t.code_norm, present: true, trusted: true, kind: 'single', cols: { name: ok('x'), parent: ok(obs[t.code_norm]) } }
    : { code_norm: t.code_norm, present: false, trusted: true, kind: null }));
  const w = (await asRole('watch_writer', () => pg.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: RUN2,
    fetch: { generation_id: 'gen_q', products_rev: '1', sets_rev: '1', raw_hash: 'c'.repeat(64) }, products_at: new Date().toISOString(), sets_at: new Date().toISOString(), absence_trusted: true, observations })]))).rows[0].r;
  await asRole('watch_writer', () => pg.query('select ops.seal_ne_registration_run($1, $2, $3)', [RUN2, w.observation_hash, 'e'.repeat(64)]));
  const adopt = (code) => skuIdOf(code).then((id) => editorCall('select ops.adopt_ne_parent_for_quarantined($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', 'NE から', OWN, id]));
  // 今ある札 (grp1)
  const revG = await revOf(GRP1);
  const a1 = await adopt('q001');
  assert.deepEqual([a1.group_product_id, a1.group_created, a1.revision, a1.compare_run_id], [GRP1, false, revG + 1, RUN2]);
  const p1 = (await outboxOf(GRP1)).pop().payload;
  assert.ok(p1.children.some((c) => c.code === 'q001' && JSON.stringify(c.choices) === '{}'));
  assert.deepEqual(await one(`select a.group_product_id::text as g, a.ne_rep_code, a.group_created, p.parent_set_by from ops.variation_parent_adoptions a join core.products p on p.product_id = a.product_id
     where a.sku_id = $1`, [await skuIdOf('q001')]), { g: GRP1, ne_rep_code: 'grp1', group_created: false, parent_set_by: 'manual' });
  await failsWith(adopt('q001'), /^already_adopted/);
  // 札を作る (NE の書き方 NewRep・名前 = 子の名前の【より前)
  const a2 = await adopt('q002');
  assert.equal(a2.group_created, true);
  assert.deepEqual(await one('select display_code, name from core.products where product_id = $1', [a2.group_product_id]), { display_code: 'NewRep', name: '新しい代表の子' });
  assert.deepEqual(await one('select code, source from ops.variation_group_codes where group_product_id = $1', [a2.group_product_id]), { code: 'NewRep', source: 'ne_adopt' });
  // 子の無い単品 (s005) = NE ではそれが代表
  const a3 = await adopt('q003');
  assert.deepEqual([a3.group_product_id, a3.group_created], [await pidOf('s005'), false]);
  assert.equal((await outboxOf(a3.group_product_id)).pop().payload.group.kind, 'single');
  await failsWith(adopt('q004'), /^ne_no_parent/);
  await failsWith(adopt('s001'), /^not_quarantined/);
  // 採用した商品は確かめ待ちから外れる
  assert.deepEqual(await tg(), ['q004']);
  // 夜間ロード (products.parent = company) は NE に無いポータルの札・子・親に触らない
  assert.deepEqual(await one('select display_code, name from core.products where product_id = $1', [HAKAMA]), { display_code: 'Hakama', name: '新しい袴' });
  assert.equal((await one('select count(*)::int as n from core.products where parent_product_id = $1', [HAKAMA])).n, 3);
});

await ta('[A2] 古い・無い観測では採用しない: NE の元のコードの回が新しい (封なし) = ne_not_fresh / 観測なし = ne_not_observed', async () => {
  const extra = [sku('q005', '後から見つけた【赤】', { representativeCode: 'grp1', representativeState: 'value' })];
  const lr = await runInitialLoad(db, planOf([sku('q001', '袋 赤【赤】'), sku('q002', 'x'), sku('q003', 'x'), sku('q004', 'x'), ...extra]), { log: quiet, runId: 'load_vg5_q5', now: LOAD_NOW });
  assert.equal(lr.ok, true, lr.error);
  const id = await skuIdOf('q005');
  await failsWith(editorCall('select ops.adopt_ne_parent_for_quarantined($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', null, OWN, id]), /^ne_not_observed/);
  const RUN3 = 'mc_20300112T000000000Z_cccccc';
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-12T00:00:00Z', 0)`, [RUN3]);
  await recordNeCodes(RUN3, ['neonly1'], [{ code_norm: 'grp1', ne_code: 'grp1' }]);
  await failsWith(editorCall('select ops.adopt_ne_parent_for_quarantined($1::uuid, $2, $3, $4::jsonb, $5::bigint) as r', [uuid(), 'naka@test', null, OWN, id]), /^ne_not_fresh/);
});

console.log('\n権限・関数の形・本文の差');

await ta('[S1] 権限: 画面のロールは 9 つの関数を実行できる・部品は実行できない・まとまりの表は読むだけ (直接書けない)・持ち主の手の DML も印が無ければ断る・security definer は search_path = pg_catalog, pg_temp・public の実行権なし', async () => {
  for (const sig of VARIATION_EDIT_FUNCTIONS) {
    const r = await one(`select has_function_privilege('master_edit', $1::regprocedure, 'execute') as me, has_function_privilege('public', $1::regprocedure, 'execute') as pub,
       array_to_string(proconfig, ',') as c from pg_proc where oid = $1::regprocedure`, [sig]);
    assert.deepEqual([r.me, r.pub, r.c], [true, false, 'search_path=pg_catalog, pg_temp'], sig);
  }
  for (const sig of VARIATION_OWNER_ONLY_FUNCTIONS) {
    const r = await one(`select has_function_privilege('master_edit', $1::regprocedure, 'execute') as me, has_function_privilege('public', $1::regprocedure, 'execute') as pub,
       has_function_privilege('watcher', $1::regprocedure, 'execute') as w, array_to_string(proconfig, ',') as c from pg_proc where oid = $1::regprocedure`, [sig]);
    assert.deepEqual([r.me, r.pub, r.w, r.c], [false, false, false, 'search_path=pg_catalog, pg_temp'], sig);
  }
  // 0067 の security definer の関数は全部 search_path を固定 (pg_temp が最後)・public の実行権なし
  const defs = await q(`select p.oid::regprocedure::text as sig, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p
     join pg_namespace n on n.oid = p.pronamespace where p.prosecdef and n.nspname in ('ops', 'core') and (p.proname like '%variation%' or p.proname in ('check_parent_one_level', 'adopt_ne_parent_for_quarantined', 'reserve_existing_variation_groups'))`);
  assert.ok(defs.length >= 15, String(defs.length));
  for (const d of defs) { assert.equal(d.c, 'search_path=pg_catalog, pg_temp', d.sig); assert.equal(d.pub, false, d.sig); }
  for (const t of VARIATION_SELECT) {
    const r = await one(`select has_table_privilege('master_edit', $1, 'select') as s, has_table_privilege('master_edit', $1, 'insert') as i, has_table_privilege('master_edit', $1, 'update') as u`, [t]);
    assert.deepEqual(r, { s: true, i: false, u: false }, t);
  }
  const e1 = await errOf(asEditor(() => pg.query(`insert into core.variation_axes (group_product_id, axis, name) values ($1, 2, 'x')`, [GRP1])));
  assert.equal(e1?.code, '42501');
  const e2 = await errOf(pg.query(`update core.variation_axes set name = '直接' where group_product_id = $1`, [GRP1]));
  assert.match(String(e2?.message), /^variation_write/);
  const e3 = await errOf(pg.query(`delete from core.variation_options where group_product_id = $1`, [GRP1]));
  assert.match(String(e3?.message), /^variation_write/);
  // まとまりの知らせを画面のロールの取引で直接は書けない (部品の実行権なし)
  const e4 = await errOf(asEditor(() => pg.query(`select ops.enqueue_group_snapshot($1, 99, '{}'::jsonb, gen_random_uuid(), 'x')`, [GRP1])));
  assert.equal(e4?.code, '42501');
});

/** migration の file にある関数の最後の定義 (create [or replace] function … plpgsql = end $$; / sql = $$;)。無ければ null */
const fnDefIn = (file, name) => {
  const t2 = fs.readFileSync(path.join(MIG_DIR, file), 'utf8').replace(/\r\n/g, '\n');
  const re = new RegExp(`\\ncreate (or replace )?function ${name.replace('.', '\\.')}\\(`, 'g');
  let m; let last = null;
  while ((m = re.exec(t2))) last = m.index + 1;
  if (last == null) return null;
  const ends = [t2.indexOf('\nend $$;\n', last), t2.indexOf('\n$$;\n', last)].filter((x) => x > 0);
  const end = Math.min(...ends);
  return t2.slice(last, end + (t2.startsWith('\nend $$;\n', end) ? 9 : 5)).replace(/^create (or replace )?function /, 'create function ');
};
function lineDiff(a, b) {
  const n = a.length, m = b.length;
  const L = Array.from({ length: n + 1 }, () => new Int32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) for (let j = m - 1; j >= 0; j--) L[i][j] = a[i] === b[j] ? L[i + 1][j + 1] + 1 : Math.max(L[i + 1][j], L[i][j + 1]);
  const removed = [], added = [];
  let i = 0, j = 0;
  while (i < n && j < m) { if (a[i] === b[j]) { i++; j++; } else if (L[i + 1][j] >= L[i][j + 1]) removed.push(a[i++]); else added.push(b[j++]); }
  while (i < n) removed.push(a[i++]); while (j < m) added.push(b[j++]);
  return { removed: removed.map((x) => x.trim()), added: added.map((x) => x.trim()) };
}
const F0067 = MIG_FILES.find((x) => x.startsWith('0067_'));
await ta('[S2] 本文の差: 0067 が置き換えた関数は前の最後の版から決めた行だけ (ne_reg_canonical = 代表の列・ne_reg_build = 版の決まり・guard_product_hub_outbox_session = まとまりの約束・ne_reg_schema_rule = 1 行・master_write_allowed = 足した行だけ)・後の migration が作り直していない', async () => {
  const WANT = {
    'ops.ne_reg_canonical': { base: '0065_ne_reg_csv_versions.sql',
      removed: ["if coalesce(v_par, '') = '' or v_pr.state is distinct from 'ok' then v_blk := v_blk || '代表 (親) の NE の書き方が確かめられない'::text;"],
      added: ['v_portal text;',
        '-- 🆕 0067: NE に無い (0041 に行が無い) まとまりは、ポータルで作った札 (予約 source = portal・同じまとまり) なら予約のコード (打ったとおり)',
        'select g.code into v_portal from ops.variation_group_codes g',
        "where g.company_id = 1 and g.code_norm = v_par and g.group_product_id = s.parent_product_id and g.source = 'portal';",
        "if coalesce(v_par, '') <> '' and v_pr.state is null and v_portal is not null then v_parc := v_portal;",
        "elsif coalesce(v_par, '') = '' or v_pr.state is distinct from 'ok' then v_blk := v_blk || '代表 (親) の NE の書き方が確かめられない'::text;"] },
    'ops.ne_reg_build': { base: '0065_ne_reg_csv_versions.sql', removed: [],
      added: ['v_vpar     bigint;   -- 🆕 0067: 品目の親', 'v_group    bigint;   -- 🆕 0067: まとまりの版のまとまり', 'v_left     text;     -- 🆕 0067: まとまりの NE 登録待ちの子でファイルに入っていないもの',
        '-- 🆕 0067: 代表の列の版の決まり (まとまりの版 = 全部が同じまとまりの子 / 単品の版 = ポータルで作ったまとまりの子は入れない)',
        'select pp.parent_product_id into v_vpar from core.products pp where pp.product_id = v_sku.product_id;',
        "if (v_rule ->> 'parent') = 'group' then",
        "if v_vpar is null then raise exception 'not_ready: % はまとまりの子でない (形の版 % はまとまりの子だけ)', v_sku.code, v_schema using errcode = 'P0001'; end if;",
        'if v_group is null then v_group := v_vpar;',
        "elsif v_group <> v_vpar then raise exception 'variation_mixed: 1 つのファイルは 1 つのまとまりの子だけ (%)', v_sku.code using errcode = 'P0001'; end if;",
        "elsif v_vpar is not null and exists (select 1 from ops.variation_group_codes g where g.group_product_id = v_vpar and g.source = 'portal') then",
        "raise exception 'variation_file_required: % はポータルで作ったまとまりの子 = ne-reg-variation-v1 のファイルで作る', v_sku.code using errcode = 'P0001';",
        'end if;',
        '-- 🆕 0067: まとまりの版 = そのまとまりの NE 登録待ちの子 (下書き / NE 登録待ち・生きているファイルなし) を全部入れる (まとまりで 1 ファイル)',
        'if v_group is not null then',
        "select pg_catalog.string_agg(k.code, '・' order by k.code_norm) into v_left",
        "from core.products cp join core.skus k on k.product_id = cp.product_id and k.sku_kind = 'single' join ops.master_registrations mr on mr.sku_id = k.sku_id",
        "where cp.parent_product_id = v_group and mr.state in ('draft', 'ne_pending')",
        "and not exists (select 1 from ops.ne_reg_export_items x where x.sku_id = k.sku_id and x.state in ('built', 'issued', 'import_declared', 'partial'))",
        'and not (k.sku_id = any (v_ids));',
        'if v_left is not null then',
        "raise exception 'variation_incomplete: まとまりの NE 登録待ちの子 (%) もこのファイルに入れる (まとまりで 1 ファイル)', v_left using errcode = 'P0001';",
        'end if;', 'end if;'] },
    'ops.guard_product_hub_outbox_session': { base: '0066_product_hub_outbox_namespace.sql',
      removed: ["raise exception 'group_snapshot_session_required: まとまりの知らせは、まとまりの約束の関数 (PR-5) の中だけで書く' using errcode = '42501';",
        'v_sess := ops.current_master_write_session();'],
      added: ['v_sess := ops.current_master_write_session();',
        '-- 🆕 0067: まとまりの約束 (まとめての登録を閉じる・子の廃止・名前を直す・代表の採用) の中で、約束のまとまり・request_id・作った人だけ',
        "if v_sess.session_id is null or v_sess.operation not in ('variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne')",
        "or (v_sess.versions ->> 'group_product_id') is distinct from new.group_product_id::text",
        'or new.request_id is distinct from v_sess.request_id or new.created_by is distinct from v_sess.actor_id then',
        "raise exception 'group_snapshot_session_required: まとまりの知らせは、まとまりの約束の関数の中で、約束のまとまり・request_id・人だけ' using errcode = '42501';",
        'end if;', 'return new;'] },
  };
  for (const [name, want] of Object.entries(WANT)) {
    const prev = MIG_FILES.filter((x) => x < F0067 && fnDefIn(x, name)).pop();
    assert.equal(prev, want.base, `${name} の元にした版 (0067 より前の最後の定義) が変わった = 0067 の関数を新しい版から写し直す`);
    const d = lineDiff(fnDefIn(prev, name).split('\n'), fnDefIn(F0067, name).split('\n'));
    assert.deepEqual(d, { removed: want.removed, added: want.added }, `${name}: ${prev} との差`);
    for (const later of MIG_FILES.filter((x) => x > F0067)) assert.equal(fnDefIn(later, name), null, `${later} も ${name} を作り直している = 0067 の試験の元を見直す`);
  }
  // 形の版の決まり = variation-v1 の 1 行 (buildable / why) だけ
  const sr = lineDiff(fnDefIn('0065_ne_reg_csv_versions.sql', 'ops.ne_reg_schema_rule').split('\n'), fnDefIn(F0067, 'ops.ne_reg_schema_rule').split('\n'));
  assert.deepEqual(sr, { removed: [`"buildable": false, "why": "まとまりの表と関数 (PR-5) の後に開く", "trial_gate": false, "jan": "empty", "parent": "group"}'::jsonb`],
    added: [`"buildable": true, "why": null, "trial_gate": false, "jan": "empty", "parent": "group"}'::jsonb`] });
  // 書いてよい (表・書き方) = 0054 の行は全部そのまま・足したのはまとまりの 5 つの操作の行だけ
  const wa = lineDiff(fnDefIn('0054_amazon_sku_maps.sql', 'ops.master_write_allowed').split('\n'), fnDefIn(F0067, 'ops.master_write_allowed').split('\n'));
  assert.deepEqual(wa.removed, ["('amazon_map_delete', 'core.listing_components', 'DELETE')) as m(op, tbl, act)"]);
  assert.ok(wa.added.length >= 10 && wa.added.every((l) => /^--|^\('(variation_|parent_adopt_ne)|^\('amazon_map_delete', 'core\.listing_components', 'DELETE'\),$/.test(l)), JSON.stringify(wa.added));
  // 権限は前のまま (create or replace): ne_reg_build = 画面のロール・canonical / schema_rule = だれにも
  const priv = async (sig) => one(`select has_function_privilege('public', $1::regprocedure, 'execute') as pub, has_function_privilege('master_edit', $1::regprocedure, 'execute') as me`, [sig]);
  assert.deepEqual(await priv('ops.ne_reg_build(jsonb, bytea)'), { pub: false, me: true });
  assert.deepEqual(await priv('ops.ne_reg_canonical(bigint, date)'), { pub: false, me: false });
  assert.deepEqual(await priv('ops.ne_reg_schema_rule(text)'), { pub: false, me: false });
});

await ta('[S3] 照合の確かめ待ちの view は今までの品目の行と列のまま (quarantined の行を足しただけ)・watcher が読める (関数の実行権が要らない)', async () => {
  const cols = await q(`select column_name as c from information_schema.columns where table_schema = 'ops' and table_name = 'v_ne_reg_targets' order by ordinal_position`);
  assert.deepEqual(cols.map((r) => r.c), ['item_id', 'export_id', 'sku_id', 'code_norm', 'sku_kind', 'state']);
  const n = await asRole('watcher', async () => (await pg.query('select count(*)::int as n from ops.v_ne_reg_targets')).rows[0].n);
  assert.ok(n >= 1);
});

console.log(`\n${passed} passed`);
