/**
 * test-amazon-map.mjs — Amazon SKU の対応の編集 (0054・lib/amazon-map-write.mjs・lib/amazon-map-migrate.mjs・engine.mjs の構成の意味・画面。
 *   Company DB構想 16 §3・§7 v2 H5 / M7 / M8 / M10 / M11・§8 契約 v3 = PR ⑦-1)
 *
 * Company DB = PGlite (Render と同じ条件の持ち主のロール deploy で migration)。保存は画面だけのロール master_edit で流す (権限が足りているかも確かめる)。
 * 固定する契約:
 *   1 今の本番は変わらない: 0053 (⑤-2b) までの DB と 0054 までの DB で、同じ材料の夜間ロード (持ち主は全部 load・段階 legacy_open) の結果 (出品・構成・変更の記録・report) が同じ。
 *     今のデータ (並びの隙間・FBM の完全一致・Sheet の構成) がある DB に 0054 を流しても失敗しない・何も変わらない・その後のロードも同じ
 *   2 門: 段階が new_open でない・持ち主表のハッシュが違う・listing_components.amazon が load・MASTER_EDIT_OPEN が無い = 409 切替前 (何も書かない)。
 *     画面のロールが関数を直接呼んでも DB が断る
 *   3 保存: 出品が無ければ作る・対応・構成 (manual・並び 0..N-1)・変更の記録 (人・request_id・理由・portal_amazon_map)・保存の記録 done (出品つき)。
 *     変えた行だけ時刻が進む (そのまま = 変えない / 数量・並び = updated_at / 足した = 両方)。変わらない保存 = no_change (何も書かない)
 *   4 版: 画面が読んだ後に出品・構成・対応が変わった = 409。同じ request_id = 前の結果 / 違う中身 = 409
 *   5 構成品は ある SKU・単品かセット・登録の状態が NE 確認済み以降。seller SKU・名前の形は写しの受け手と同じ決まり (DB でも)
 *   6 墓標: 理由が要る・構成を全部消す・行は消さない (持ち主の DELETE も TRUNCATE も拒む)・画面のロールに DELETE なし・再登録で active (登録日が新しい)
 *   7 不変条件 (commit のとき): Amazon (日本) の出品・listing_norm = norm(seller SKU)・active は構成 1 行以上で並び 0..N-1・墓標は構成 0 行 (どの表の書き込みからも)
 *   8 構成の書き手: company_owner / new_open の間、対応のある出品の構成は portal_amazon_map / amazon_map_migration の取引だけ (夜間ロードの名前では書けない)
 *   9 夜間ロード: 対応・墓標のある出品の構成は作らない (持ち主によらず)。持ち主 company = SKU マスタ・Sheet の構成を使わず、FBM の完全一致は対応の無い出品にだけ
 *  10 写しの決まった並べ方 (sku-map-canon-v1): Company DB から作った行が受け手の決まりに合う・UTC / JST の session で同じハッシュ (M6)
 *  11 移行: 影運転は止める項目を数えて必ず巻き戻す・古い表は変えない / apply は frozen だけ・止める項目 0・H0 と同じときだけ commit・同じ行は時刻だけ (記録・出品の version を増やさない)・
 *      FBM の自動の行は消える・移行の後の夜間ロード 2 回で構成が変わらない
 *  12 画面: 一覧・1 つ・未登録 (FBA / FBM・未判定)・変更の記録・つかいかた・保存 / 削除の API (名簿・Origin)
 * 使い方: node scripts/test-amazon-map.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';
import Database from 'better-sqlite3';

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
const C = await import('../lib/master-cutover.mjs');
const A = await import('../lib/amazon-map-write.mjs');
const M = await import('../lib/amazon-map-migrate.mjs');
const K = await import('../lib/sku-map-canonical.js');
const CLI = await import('./company-db/amazon-map-migrate.mjs');
const { MasterWriteError } = await import('../lib/master-write.mjs');
const { default: router, __setPgClientFactory, __setClock, __setOwnership, __setAmazonChannelsProvider } = await import('../apps/master-edit/router.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const rejectsWith = async (p, status, reason) => {
  try { await p; } catch (e) {
    assert.ok(e instanceof MasterWriteError, `MasterWriteError でない: ${e && e.stack}`);
    assert.equal(e.status, status, `${e.reason}: ${e.message}`);
    if (reason) assert.equal(e.reason, reason, e.message);
    return e;
  }
  assert.fail(`${status} ${reason || ''} にならなかった`);
};
const errOf = async (p) => { try { await p; return null; } catch (e) { return e; } };
const FW_SPACE = String.fromCharCode(0x3000);

const AMZ = 'main@A1VC38T7YXB528';
const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const LOAD_NOW = new Date('2030-01-05T03:00:00Z');
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual' }] };
const BUILDS = { render: ['r1'], minipc: ['m1'] };
const LEGACY_HASH = C.ownershipHash(MASTER_OWNERSHIP);
const stamp = () => new Date().toISOString();

/** 夜間ロードの材料 (sources.mjs が作る形)。Amazon の出品 = SKU マスタ (m_sku_master)・FBM の完全一致・Sheet だけ */
function makePlan() {
  const single = (code, name, extra = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, ...extra });
  const amazon = (listingCode, title, comps, evidenceSource) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title, status: 'active', components: comps, asinCandidates: [], fnskuCandidates: [], evidenceSource });
  const fromMaster = (code, qty, sortOrder) => ({ code, qty, sortOrder, resolution: 'imported', evidence: { source: 'm_sku_master' } });
  const fbm = (code) => [{ code, qty: 1, resolution: 'exact', evidence: { source: 'fbm_ne_code', seller_sku: code } }];
  return {
    skus: [single('a001', '単品 1'), single('a002', '単品 2'), single('a003', '単品 3 (FBM)'), single('a004', '単品 4 (FBM・墓標にする)'), single('a005', '単品 5 (Sheet)'),
      single('a006', '単品 6'), single('a007', '単品 7'),
      { code: 'aset1', name: 'セット 1', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE' } },
      { code: 'aexc', name: '例外の SKU', kind: 'exception', taxRate: null, taxClass: null, handling: 'active', salesClass: null, cost: null }],
    variationGroups: [],
    setComponents: [{ parentCode: 'aset1', childCode: 'a001', qty: 3, source: 'ne' }],
    listings: [
      amazon('pr_a001', 'SKU マスタの 1', [fromMaster('a001', 1, 0)], 'mirror_sku_master'),
      // 並びに隙間 (0, 2) = 今のデータにありうる形 (CSV の REPLACE)。0054 の不変条件は対応の無い出品を見ない = ロードは止まらない
      amazon('pr_pack2', 'SKU マスタの 2 個組', [fromMaster('a001', 2, 0), fromMaster('a002', 1, 2)], 'mirror_sku_master'),
      amazon('a003', '単品 3 (FBM)', fbm('a003'), 'amazon_fees_fbm'),
      amazon('a004', '単品 4 (FBM)', fbm('a004'), 'amazon_fees_fbm'),
      amazon('sheet_only1', null, [{ code: 'a005', qty: 1, resolution: 'imported', evidence: { source: 'fba_sheet' } }], 'fba_sheet'),
      { mall: 'rakuten', shopCode: 'main', listingCode: 'a001', status: 'active', components: [{ code: 'a001', qty: 1, resolution: 'exact', evidence: { source: 'rakuten_sku_map' } }] },
    ],
    observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
  };
}
const load = async (db, ownership = MASTER_OWNERSHIP, plan = makePlan(), runId = 'load_fixed') => {
  const r = await runInitialLoad(db, plan, { log: quiet, runId, ownership, now: LOAD_NOW, host: 'test' });
  assert.equal(r.ok, true, r.error);
  return r;
};
/** 比べる形 (番号・時刻・乱数は抜く) */
async function snapshot(db) {
  const q = async (sql) => (await db.query(sql)).rows;
  return {
    listings: await q(`select mall, shop_code, listing_code, title, status, version::text as v from core.listings order by mall, listing_code`),
    comps: await q(`select l.mall, l.listing_code, k.code, c.qty, c.sort_order, c.resolution, c.evidence, c.resolved_by_id from core.listing_components c
      join core.listings l on l.listing_id = c.listing_id join core.skus k on k.sku_id = c.sku_id order by l.mall, l.listing_code, k.code`),
    events: await q(`select entity_type, operation, entity_key, attribute, old_value, new_value, source_system, run_id, actor_id from events.master_change_events order by event_id`),
  };
}
const reportShape = (r) => Object.fromEntries(Object.entries(r.sections).map(([k, v]) => [k, [v.expected, v.applied, v.same, v.skipped.length, v.notes]]));

/** 1 つの Company DB (PGlite): 持ち主のロール deploy で migration・見張りと画面のロール・夜間ロード */
async function setupDb({ to = null } = {}) {
  const pg = new PGlite();
  const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet, to });
  return { pg, db, sessionUser };
}
async function asRole(E, role, fn) {
  await E.pg.query(`set role ${role}`);
  try { return await fn(); } finally { await E.pg.query('set role deploy'); }
}
async function asGate(E, host, fn) {
  await E.pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await E.pg.query(`set session authorization ${E.sessionUser}`); await E.pg.query('set role deploy'); }
}
const gateAck = (E, host, inst, build, ownership, phaseSeen) => asGate(E, host, () => C.recordLegacyGateAck(E.db, { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen }));
const advance = (E, to, evidence) => asRole(E, 'master_ops', () => C.advanceCutoverPhase(E.db, { to, actor: 'naka@test', evidence }));
/** 試験だけの切替: 本番の関数そのまま (門は弱めない)。upTo = frozen / company_owner / new_open */
async function openCutover(E, ownership, upTo = 'new_open') {
  const h = C.ownershipHash(ownership);
  const mh = await C.manifestHashOf(E.db, MANIFEST);
  const acks = async (own, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await gateAck(E, host, inst, build, own, phase); };
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  await advance(E, 'frozen', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH,
    manual_entries_stopped: [{ id: 'gas:logizard-sheet-and-sku-map', by: 't', at: stamp() }, { id: 'ne:item-screen', by: 't', at: stamp() }], drain: { done: true, checked_by: 't', checked_at: stamp() } });
  if (upTo === 'frozen') return;
  await acks(ownership, 'frozen');
  await advance(E, 'company_owner', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
  if (upTo === 'company_owner') return;
  await acks(ownership, 'company_owner');
  await asRole(E, 'master_ops', async () => {
    const p = (await E.db.query('select * from ops.registration_backfill_plan()')).rows[0];
    await E.db.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 'naka@test']);
  });
  await advance(E, 'new_open', { expected_builds: BUILDS, manifest_hash: mh, owner_hash: h });
}

console.log('今の本番は変わらない (0053 と 0054)');
await ta('[1] 同じ材料の夜間ロード (全部 load・legacy_open): 0053 までの DB と 0054 までの DB で出品・構成・変更の記録・report が同じ / 0054 を後から流しても何も変わらない・その後のロードも同じ', async () => {
  const E52 = await setupDb({ to: '0053' });
  const E53 = await setupDb();
  assert.equal((await E52.db.query("select count(*)::int as n from ops.schema_migrations where version = '0054'")).rows[0].n, 0);
  const r52 = await load(E52.db); const r53 = await load(E53.db);
  assert.deepEqual(reportShape(r53), reportShape(r52));
  const s52 = await snapshot(E52.db); const s53 = await snapshot(E53.db);
  assert.deepEqual(s53, s52);
  // 今のデータのまま: pr_pack2 の並びの隙間・FBM の完全一致・Sheet の構成がある
  assert.deepEqual(s52.comps.filter((c) => c.listing_code === 'pr_pack2').map((c) => [c.code, c.sort_order]), [['a001', 0], ['a002', 2]]);
  assert.ok(s52.comps.some((c) => c.listing_code === 'a003' && c.resolution === 'exact'));
  // 2 回目のロードも同じ
  const r52b = await load(E52.db, MASTER_OWNERSHIP, makePlan(), 'load_fixed_2'); const r53b = await load(E53.db, MASTER_OWNERSHIP, makePlan(), 'load_fixed_2');
  assert.deepEqual(reportShape(r53b), reportShape(r52b));
  assert.deepEqual(await snapshot(E53.db), await snapshot(E52.db));
  // 0053 の DB に 0054 を後から流す = 失敗しない・何も変わらない
  const before = await snapshot(E52.db);
  const res = await applyMigrations(E52.db, { log: quiet });
  assert.deepEqual(res.applied, ['0054']);
  assert.deepEqual(await snapshot(E52.db), before);
  assert.equal((await E52.db.query('select count(*)::int as n from core.amazon_sku_maps')).rows[0].n, 0);
  assert.equal((await E52.db.query('select count(*)::int as n from core.listing_components where updated_at is not null')).rows[0].n, 0);
  const r3 = await load(E52.db, MASTER_OWNERSHIP, makePlan(), 'load_fixed_3');
  assert.equal(r3.summary.listing_components.applied, 0);
  assert.deepEqual(await snapshot(E52.db), before);
  // 変更の記録の種類の CHECK は新しい値を受け、知らない値は今までどおり断る
  const e = await errOf(E52.db.query(`insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_key, new_value, actor_type, source_system)
    values (1, gen_random_uuid(), 'INSERT', 'nope', '{}', '{}', 'system', 'sql')`));
  assert.equal(e?.code, '23514');
});

await ta('[1] 0054 は ⑤-1・⑤-2a・⑤-2b (0053) の物を全部残す: 書いてよい (表・書き方) の行・約束 / 保存の記録の操作・変更の記録の種類 (名前も)・SKU が要る操作', async () => {
  const E53 = await setupDb({ to: '0053' });
  const E54 = await setupDb();
  const allowedRows = async (E) => {
    const src = (await E.db.query(`select pg_get_functiondef('ops.master_write_allowed(text, text, text)'::regprocedure) as d`)).rows[0].d;
    return [...src.matchAll(/\('([a-z_]+)', '([a-z_.]+)', '(INSERT|UPDATE|DELETE)'\)/g)].map((m) => `${m[1]} ${m[2]} ${m[3]}`).sort();
  };
  const r53 = await allowedRows(E53); const r54 = await allowedRows(E54);
  assert.ok(r53.length > 40, `0053 の行 ${r53.length}`);
  assert.deepEqual(r53.filter((x) => !r54.includes(x)), [], '0053 の行が 0054 で消えた');
  assert.deepEqual(r54.filter((x) => !r53.includes(x)).map((x) => x.split(' ')[0]).filter((op) => !op.startsWith('amazon_map_')), [], 'Amazon でない行が増えた');
  for (const x of r53) { const [op, tbl, act] = x.split(' '); assert.equal((await E54.db.query('select ops.master_write_allowed($1, $2, $3) as ok', [op, tbl, act])).rows[0].ok, true, x); }
  const checkOf = async (E, table, name) => (await E.db.query(`select pg_get_constraintdef(c.oid) as d from pg_constraint c where c.conrelid = $1::regclass and c.conname = $2`, [table, name])).rows[0]?.d ?? null;
  const opsOf = (d) => [...String(d).matchAll(/'([a-z_]+)'::text/g)].map((m) => m[1]).sort();
  for (const [t, n] of [['ops.master_write_sessions', 'ck_mws_operation'], ['ops.master_edit_requests', 'ck_mer_operation'], ['events.master_change_events', 'master_change_events_entity_type_check']]) {
    const a = opsOf(await checkOf(E53, t, n)); const b = opsOf(await checkOf(E54, t, n));
    assert.ok(a.length >= 9, `${n} (0053) ${a}`);
    assert.deepEqual(a.filter((x) => !b.includes(x)), [], `${n}: 0053 の値が 0054 で消えた`);
    assert.deepEqual(b.filter((x) => !a.includes(x)).sort(), n.startsWith('master_change') ? ['amazon_sku_map'] : ['amazon_map_delete', 'amazon_map_save'], n);
  }
  assert.match(await checkOf(E54, 'ops.master_write_sessions', 'ck_mws_sku_needed'), /sku_edit.*sku_create/);
  // ⑤-2b の約束 (SKU なし・出品なし) が入る / Amazon の約束は出品が要る / sku_edit は SKU が要る
  const ins = (op, sku, lid, src) => E54.db.query(`insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, listing_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
      actor_id, source_system, db_user, phase, owner_hash, ownership) values (gen_random_uuid(), txid_current(), gen_random_uuid(), $1, $2, $3, '{}', '{}', repeat('a', 64), repeat('a', 64), '{}', 'x', $4, 'x', 'new_open', repeat('a', 64), '{}')`, [op, sku, lid, src]);
  const codeOf = async (fn) => { await E54.pg.query('begin'); try { await fn(); return 'ok'; } catch (e) { return e.constraint || e.code; } finally { await E54.pg.query('rollback'); } };
  assert.equal(await codeOf(() => ins('supplier_create', null, null, 'portal_master_edit')), 'ok');
  assert.equal(await codeOf(() => ins('sku_edit', null, null, 'portal_master_edit')), 'ck_mws_sku_needed');
  assert.equal(await codeOf(() => ins('amazon_map_save', null, null, 'portal_amazon_map')), 'ck_mws_target');
  assert.equal(await codeOf(() => ins('supplier_create', null, null, 'portal_amazon_map')), 'ck_mws_source');
  await E53.pg.close(); await E54.pg.close();
});

// ── ここからの試験の DB (1 つ) ──
const E = await setupDb();
await createRoles(E.pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(E.pg, {});
await load(E.db);
const { pg, db } = E;
const q = async (sql, params) => (await db.query(sql, params)).rows;
const lidOf = async (code) => (await q("select listing_id::text as id from core.listings where mall = 'amazon' and listing_norm = core.norm_code($1)", [code]))[0]?.id ?? null;
const compsOfListing = async (code) => (await q(`select k.code, c.qty, c.sort_order, c.resolution from core.listing_components c join core.skus k on k.sku_id = c.sku_id
  where c.listing_id = (select listing_id from core.listings where mall = 'amazon' and listing_norm = core.norm_code($1)) order by c.sort_order, k.code`, [code])).map((r) => [r.code, r.qty, r.sort_order, r.resolution]);
const versionsOf = async (sku) => (await A.readAmazonMap(db, sku)).versions;
const uuid = () => crypto.randomUUID();
/** 画面と同じ形で保存・削除 (画面だけのロール master_edit で) */
async function save(sellerSku, name, components, { ownership = ALL_COMPANY, open = true, requestId = uuid(), versions, reason = 'テスト', actor = 'Naka@Test' } = {}) {
  const seen = { versions: versions ?? await versionsOf(sellerSku.trim().toLowerCase()) };
  return asRole(E, 'master_edit', () => A.saveAmazonMap(db, { actor, requestId, sellerSku, name, components, reason, seen }, { ownership, open }));
}
async function del(sellerSku, reason, { ownership = ALL_COMPANY, open = true, requestId = uuid(), versions, actor = 'naka@test' } = {}) {
  const seen = { versions: versions ?? await versionsOf(sellerSku) };
  return asRole(E, 'master_edit', () => A.deleteAmazonMap(db, { actor, requestId, sellerSku, reason, seen }, { ownership, open }));
}
/** 画面のロールで関数を直接呼ぶ (アプリを通さない) */
const callFn = (fn, args) => asRole(E, 'master_edit', () => db.query(`select ops.${fn}($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r`, args));
const skuIdOf = async (code) => Number((await q('select sku_id from core.skus where code = $1', [code]))[0].sku_id);

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'amazon-map-'));

console.log('\n門 (切替の前)');
await ta('[2] 切替の前 (legacy_open): 名簿の人でも 409 切替前・何も書かない (失敗の記録だけ)。画面のロールが関数を直接呼んでも DB が断る', async () => {
  const nMaps = async () => Number((await q('select count(*)::int as n from core.amazon_sku_maps'))[0].n);
  const rid = uuid();
  await rejectsWith(save('pr_a001', '名前', [{ code: 'a001', qty: 1 }], { requestId: rid }), 409, 'before_cutover');
  assert.equal(await nMaps(), 0);
  assert.deepEqual((await q('select status, operation, error ->> \'reason\' as why from ops.master_edit_requests where request_id = $1', [rid]))[0], { status: 'failed', operation: 'amazon_map_save', why: 'before_cutover' });
  const e = await errOf(callFn('save_amazon_sku_map', [uuid(), 'naka@test', null, JSON.stringify(ALL_COMPANY), 'a'.repeat(64),
    JSON.stringify({ seller_sku: 'zz1', name: 'x', components: [{ sku_id: await skuIdOf('a001'), code: 'a001', qty: 1 }], versions: { listing: null, map: null } })]));
  assert.match(e?.message || '', /before_cutover/);
  // 画面のロールは表に直接書けない・部品の関数も呼べない
  for (const sql of [`insert into core.amazon_sku_maps (listing_id, seller_sku, name, state, origin, registered_at, changed_at) values (1, 'x', 'x', 'active', 'portal', now(), now())`,
    'delete from core.amazon_sku_maps', `update core.listing_components set qty = 9`, `select ops.amazon_map_begin('amazon_map_save', gen_random_uuid(), 'x', null, '{}', 'x', '{}')`,
    `select ops.amazon_map_components('[]')`, `select core.amazon_map_problem(1)`]) {
    const x = await errOf(asRole(E, 'master_edit', () => db.query(sql)));
    assert.equal(x?.code, '42501', `${sql}: ${x?.code} ${x?.message}`);
  }
});

await openCutover(E, ALL_COMPANY);
assert.equal((await q('select phase from ops.master_cutover_state'))[0].phase, 'new_open');

console.log('\n保存');
await ta('[2] 開いた後でも: MASTER_EDIT_OPEN が無い・持ち主 listing_components.amazon が load・持ち主表が段階の記録と違う = 409', async () => {
  await rejectsWith(save('new_sku1', '名前', [{ code: 'a001', qty: 1 }], { open: false }), 409, 'before_cutover');
  await rejectsWith(save('new_sku1', '名前', [{ code: 'a001', qty: 1 }], { ownership: { ...ALL_COMPANY, 'listing_components.amazon': 'load' } }), 409, 'before_cutover');
  // 関数を直接: 持ち主表のハッシュは段階の記録と同じでも、listing_components.amazon が load なら断る (持ち主表の違い = ハッシュが違う = 先に断る)
  const e = await errOf(callFn('save_amazon_sku_map', [uuid(), 'naka@test', null, JSON.stringify({ ...ALL_COMPANY, 'listing_components.amazon': 'load' }), 'a'.repeat(64),
    JSON.stringify({ seller_sku: 'new_sku1', name: 'x', components: [{ sku_id: await skuIdOf('a001'), code: 'a001', qty: 1 }], versions: { listing: null, map: null } })]));
  assert.match(e?.message || '', /before_cutover/);
  assert.equal(await lidOf('new_sku1'), null);
});

await ta('[2] ⑤ だけ切り替えて Amazon SKU の持ち主は load のまま (段階 new_open・持ち主表のハッシュも同じ): 画面も DB の関数も 409 切替前 (何も書かない)', async () => {
  const OWN = { ...ALL_COMPANY, 'listing_components.amazon': 'load' };
  const E4 = await setupDb();
  await createRoles(E4.pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(E4.pg, {});
  await load(E4.db);
  await openCutover(E4, OWN);
  const v = (await A.readAmazonMap(E4.db, 'pr_a001')).versions;
  const e1 = await asRole(E4, 'master_edit', () => A.saveAmazonMap(E4.db, { actor: 'naka@test', requestId: uuid(), sellerSku: 'pr_a001', name: 'x', components: [{ code: 'a001', qty: 1 }], seen: { versions: v } },
    { ownership: OWN, open: true })).then(() => null, (e) => e);
  assert.equal(e1?.status, 409); assert.equal(e1?.reason, 'before_cutover'); assert.match(e1.message, /持ち主がまだ miniPC の SKU マスタの側/);
  const a001 = Number((await E4.db.query("select sku_id from core.skus where code = 'a001'")).rows[0].sku_id);
  const e2 = await errOf(asRole(E4, 'master_edit', () => E4.db.query('select ops.save_amazon_sku_map($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb)', [uuid(), 'naka@test', null, JSON.stringify(OWN), 'a'.repeat(64),
    JSON.stringify({ seller_sku: 'pr_a001', name: 'x', components: [{ sku_id: a001, code: 'a001', qty: 1 }], versions: v })])));
  assert.match(e2?.message || '(通った)', /before_cutover: Amazon SKU の対応 \(listing_components\.amazon\) の持ち主がまだ company でない/);
  assert.equal(Number((await E4.db.query('select count(*)::int as n from core.amazon_sku_maps')).rows[0].n), 0);
  await E4.pg.close();
});

let firstResult;
await ta('[3] 新しい seller SKU: 出品を作る・対応 (portal)・構成 (manual・並び 0..N-1)・変更の記録 (人・request_id・理由)・保存の記録 done (出品つき)・結果は DB が作る', async () => {
  const rid = uuid();
  const r = await save('  New_SKU1 ', '新しい 2 個組', [{ code: 'a002', qty: 2 }, { code: 'A001', qty: 1 }], { requestId: rid, reason: '新しく出品' });
  firstResult = r;
  assert.equal(r.ok, true); assert.equal(r.no_change, false); assert.equal(r.created, true); assert.equal(r.listing_created, true); assert.equal(r.seller_sku, 'new_sku1');
  assert.deepEqual(r.components, [{ code: 'a002', qty: 2, sort_order: 0 }, { code: 'a001', qty: 1, sort_order: 1 }]);
  assert.ok(r.notes.some((n) => /07:00/.test(n)) && r.notes.some((n) => /次の夜の取り込み/.test(n)));
  const lid = await lidOf('new_sku1');
  assert.ok(lid);
  assert.deepEqual((await q('select mall, shop_code, listing_code, title, status, created_by_type, created_by_id from core.listings where listing_id = $1', [lid]))[0],
    { mall: 'amazon', shop_code: AMZ, listing_code: 'new_sku1', title: '新しい 2 個組', status: 'active', created_by_type: 'human', created_by_id: 'naka@test' });
  assert.deepEqual((await q('select seller_sku, name, state, origin, registered_by, changed_by, registered_at = changed_at as same_t from core.amazon_sku_maps where listing_id = $1', [lid]))[0],
    { seller_sku: 'new_sku1', name: '新しい 2 個組', state: 'active', origin: 'portal', registered_by: 'naka@test', changed_by: 'naka@test', same_t: true });
  assert.deepEqual(await compsOfListing('new_sku1'), [['a002', 2, 0, 'manual'], ['a001', 1, 1, 'manual']]);
  const ev = await q(`select entity_type, operation, actor_type, actor_id, source_system, request_id, reason_text, db_user from events.master_change_events
    where request_id = $1 order by event_id`, [rid]);
  assert.ok(ev.length >= 4);
  assert.ok(ev.every((x) => x.actor_type === 'human' && x.actor_id === 'naka@test' && x.source_system === 'portal_amazon_map' && x.reason_text === '新しく出品' && x.db_user === 'master_edit'), JSON.stringify(ev));
  assert.deepEqual([...new Set(ev.map((x) => x.entity_type))].sort(), ['amazon_sku_map', 'listing', 'listing_component']);
  const req = (await q('select status, operation, target_code, sku_id, listing_id::text as listing_id, result from ops.master_edit_requests where request_id = $1', [rid]))[0];
  assert.deepEqual({ ...req, result: undefined }, { status: 'done', operation: 'amazon_map_save', target_code: 'new_sku1', sku_id: null, listing_id: lid, result: undefined });
  assert.equal(req.result.request_payload_hash, A.amazonMapPayloadHashOf(A.parseAmazonMapSave({ actor: 'naka@test', requestId: rid, sellerSku: 'new_sku1', name: '新しい 2 個組',
    components: [{ code: 'a002', qty: 2 }, { code: 'A001', qty: 1 }], reason: '新しく出品', seen: { versions: { listing: null, map: null } } })));
  // 約束の行 (operation・出品・出どころ)
  assert.deepEqual((await q('select operation, sku_id, listing_id::text as listing_id, source_system from ops.master_write_sessions where request_id = $1', [rid]))[0],
    { operation: 'amazon_map_save', sku_id: null, listing_id: lid, source_system: 'portal_amazon_map' });
});

await ta('[3] 直す: 数量・並びを変えた行だけ updated_at が進む・そのままの行は時刻を変えない・名前で changed_at / 変わらない保存 = no_change (何も書かない)', async () => {
  const lid = await lidOf('new_sku1');
  const times = async () => Object.fromEntries((await q(`select k.code, c.created_at::text as c, c.updated_at::text as u from core.listing_components c join core.skus k on k.sku_id = c.sku_id where c.listing_id = $1`, [lid])).map((r) => [r.code, [r.c, r.u]]));
  const mapT = async () => (await q('select registered_at::text as r, changed_at::text as c from core.amazon_sku_maps where listing_id = $1', [lid]))[0];
  const t0 = await times(); const m0 = await mapT();
  await pg.query("select pg_sleep(0.01)");
  // 同じ中身 = no_change
  const nEv = Number((await q('select count(*)::int as n from events.master_change_events'))[0].n);
  const r0 = await save('new_sku1', '新しい 2 個組', [{ code: 'a002', qty: 2 }, { code: 'a001', qty: 1 }]);
  assert.equal(r0.no_change, true);
  assert.equal(Number((await q('select count(*)::int as n from events.master_change_events'))[0].n), nEv);
  assert.deepEqual(await times(), t0); assert.deepEqual(await mapT(), m0);
  // a001 の数量だけ変える + a003 を足す
  const r1 = await save('new_sku1', '新しい 2 個組', [{ code: 'a002', qty: 2 }, { code: 'a001', qty: 3 }, { code: 'a003', qty: 1 }]);
  assert.equal(r1.components_changed, true); assert.equal(r1.name, null);
  const t1 = await times();
  assert.deepEqual(t1.a002, t0.a002);                              // そのまま
  assert.equal(t1.a001[0], t0.a001[0]); assert.notEqual(t1.a001[1], t0.a001[1]);   // 数量を変えた = updated_at だけ
  assert.equal(t1.a003[0], t1.a003[1]);                            // 足した = 両方とも今
  const m1 = await mapT();
  assert.equal(m1.r, m0.r); assert.notEqual(m1.c, m0.c);
  // 並べ替えだけ (a001 を先頭へ) = 並びの変わった行の updated_at
  await save('new_sku1', '新しい 2 個組', [{ code: 'a001', qty: 3 }, { code: 'a002', qty: 2 }, { code: 'a003', qty: 1 }]);
  assert.deepEqual(await compsOfListing('new_sku1'), [['a001', 3, 0, 'manual'], ['a002', 2, 1, 'manual'], ['a003', 1, 2, 'manual']]);
  // 外す: a003 の行は消える
  const r3 = await save('new_sku1', '名前を変えた', [{ code: 'a001', qty: 3 }, { code: 'a002', qty: 2 }]);
  assert.deepEqual(r3.name, { from: '新しい 2 個組', to: '名前を変えた' });
  assert.deepEqual(await compsOfListing('new_sku1'), [['a001', 3, 0, 'manual'], ['a002', 2, 1, 'manual']]);
  // 前からの出品 (夜間ロードの構成) を対応にする: 同じ行は触らない (resolution も)・違う行だけ manual
  const before = await q('select k.code, c.created_at::text as c, c.resolution from core.listing_components c join core.skus k on k.sku_id = c.sku_id where c.listing_id = $1 order by k.code', [await lidOf('pr_a001')]);
  await save('pr_a001', 'SKU マスタの 1', [{ code: 'a001', qty: 1 }]);
  assert.deepEqual(await q('select k.code, c.created_at::text as c, c.resolution from core.listing_components c join core.skus k on k.sku_id = c.sku_id where c.listing_id = $1 order by k.code', [await lidOf('pr_a001')]), before);
});

await ta('[4] 版: 画面が読んだ後に構成・対応・出品が変わった = 409 (何も書かない) / 同じ request_id = 前の結果・違う中身 = 409', async () => {
  const stale = await versionsOf('new_sku1');
  await save('new_sku1', '名前を変えた', [{ code: 'a001', qty: 4 }, { code: 'a002', qty: 2 }]);
  await rejectsWith(save('new_sku1', 'ほかの人', [{ code: 'a001', qty: 5 }], { versions: stale }), 409, 'version_conflict');
  assert.deepEqual(await compsOfListing('new_sku1'), [['a001', 4, 0, 'manual'], ['a002', 2, 1, 'manual']]);
  // 出品が無いと読んだのに、ほかの保存が作った
  await rejectsWith(save('new_sku1', 'x', [{ code: 'a001', qty: 1 }], { versions: { listing: null, map: null } }), 409, 'version_conflict');
  // 同じ request_id
  const rid = uuid();
  const v = await versionsOf('new_sku1');
  const r1 = await save('new_sku1', '名前 3', [{ code: 'a001', qty: 4 }, { code: 'a002', qty: 2 }], { requestId: rid, versions: v });
  const r2 = await save('new_sku1', '名前 3', [{ code: 'a001', qty: 4 }, { code: 'a002', qty: 2 }], { requestId: rid, versions: v });
  assert.equal(r2.replayed, true); assert.deepEqual({ ...r2, replayed: undefined }, { ...r1, replayed: undefined });
  await rejectsWith(save('new_sku1', '名前 4', [{ code: 'a001', qty: 4 }], { requestId: rid, versions: v }), 409, 'request_id_reused');
  await rejectsWith(save('new_sku1', '名前 3', [{ code: 'a001', qty: 4 }, { code: 'a002', qty: 2 }], { requestId: rid, versions: v, actor: 'other@test' }), 409, 'request_id_reused');
  assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [rid]))[0].n), 1);
});

await ta('[5] 構成品と形: 無い・例外の SKU・NE 確認前 (下書き) = 400 / seller SKU (大文字だけの違いは小文字に・全角の空白・制御文字・長すぎ)・名前 (空白だけ) / DB でも同じ決まり', async () => {
  await rejectsWith(save('new_sku2', 'x', [{ code: 'nosuch', qty: 1 }]), 400, 'invalid_input');
  await rejectsWith(save('new_sku2', 'x', [{ code: 'aexc', qty: 1 }]), 400, 'invalid_input');
  await pg.query(`select set_config('ops.registration_protocol', '1', false)`);
  await pg.query("update ops.master_registrations set state = 'draft' where sku_id = (select sku_id from core.skus where code = 'a007')");
  await pg.query(`select set_config('ops.registration_protocol', '', false)`);
  const e1 = await rejectsWith(save('new_sku2', 'x', [{ code: 'a007', qty: 1 }]), 400, 'invalid_input');
  assert.match(e1.message, /NE で確かめた商品ではありません/);
  await rejectsWith(save('new_sku2', 'x', [{ code: 'a001', qty: 1 }, { code: 'A001', qty: 1 }]), 400, 'invalid_input');   // 正規化で同じ
  await rejectsWith(save('new_sku2', 'x', [{ code: 'a001', qty: 1000 }]), 400, 'invalid_input');
  await rejectsWith(save('new_sku2', 'x', []), 400, 'invalid_input');
  await rejectsWith(save(`a${String.fromCharCode(9)}b`, 'x', [{ code: 'a001', qty: 1 }]), 400, 'invalid_input');   // 制御文字 (TAB)
  assert.equal(A.sellerSkuIn(` Ab-1${FW_SPACE}`), 'ab-1');   // 前後の空白 (全角も) を除いて小文字
  await rejectsWith(save('x'.repeat(256), 'x', [{ code: 'a001', qty: 1 }]), 400, 'invalid_input');
  await rejectsWith(save('new_sku2', FW_SPACE + FW_SPACE, [{ code: 'a001', qty: 1 }]), 400, 'invalid_input');
  // DB でも (画面を通さない): 下書きの構成品・例外の SKU・大文字の seller SKU・全角の空白で終わる seller SKU・名前が全角の空白だけ
  const own = JSON.stringify(ALL_COMPANY);
  const entry = (o) => JSON.stringify({ seller_sku: 'new_sku2', name: 'x', components: [{ sku_id: 0, code: 'a001', qty: 1 }], versions: { listing: null, map: null }, ...o });
  const a001 = await skuIdOf('a001'); const a007 = await skuIdOf('a007'); const aexc = await skuIdOf('aexc');
  const cases = [
    [{ components: [{ sku_id: a007, code: 'a007', qty: 1 }] }, /component_unusable.*状態 draft/],
    [{ components: [{ sku_id: aexc, code: 'aexc', qty: 1 }] }, /component_unusable.*例外の SKU/],
    [{ components: [{ sku_id: a001, code: 'a002', qty: 1 }] }, /component_unusable.*コードが違う/],
    [{ components: [{ sku_id: a001, code: 'a001', qty: 1.5 }] }, /invalid_value/],
    [{ components: [{ sku_id: a001, code: 'a001', qty: 1, extra: 1 }] }, /invalid_value/],
    [{ seller_sku: 'NEW_SKU2', components: [{ sku_id: a001, code: 'a001', qty: 1 }] }, /invalid_value: seller SKU が使えない形です .大文字を含む/],
    [{ seller_sku: `new_sku2${FW_SPACE}`, components: [{ sku_id: a001, code: 'a001', qty: 1 }] }, /invalid_value: seller SKU が使えない形です .前後に空白/],
    [{ name: FW_SPACE, components: [{ sku_id: a001, code: 'a001', qty: 1 }] }, /invalid_value: 名前/],
    [{ components: [{ sku_id: a001, code: 'a001', qty: 1 }], result: {} }, /invalid_input: 保存の結果/],
  ];
  for (const [o, re] of cases) {
    const e = await errOf(callFn('save_amazon_sku_map', [uuid(), 'naka@test', null, own, 'a'.repeat(64), entry(o)]));
    assert.match(e?.message || '(通った)', re, `${JSON.stringify(o)}: ${e?.message}`);
  }
  assert.equal(await lidOf('new_sku2'), null);
  await pg.query(`select set_config('ops.registration_protocol', '1', false)`);
  await pg.query("update ops.master_registrations set state = 'available' where sku_id = (select sku_id from core.skus where code = 'a007')");
  await pg.query(`select set_config('ops.registration_protocol', '', false)`);
});

await ta('[3] 関数の後は同じ取引でも約束が閉じている (画面のロールは書けない)・出どころの設定は元に戻る', async () => {
  await pg.query('begin');
  try {
    await pg.query('set local role master_edit');
    await pg.query(`select set_config('core.source_system', 'something', true)`);
    const v = await A.readAmazonMap(db, 'new_sku1');
    await db.query('select ops.save_amazon_sku_map($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb)', [uuid(), 'naka@test', null, JSON.stringify(ALL_COMPANY), 'b'.repeat(64),
      JSON.stringify({ seller_sku: 'new_sku1', name: '同じ取引', components: [{ sku_id: await skuIdOf('a001'), code: 'a001', qty: 4 }, { sku_id: await skuIdOf('a002'), code: 'a002', qty: 2 }], versions: v.versions })]);
    assert.deepEqual((await pg.query(`select current_setting('ops.master_write_session', true) as s, current_setting('core.source_system', true) as src`)).rows[0], { s: '', src: 'something' });
    const e = await errOf(pg.query("update core.amazon_sku_maps set name = 'こっそり' where seller_sku = 'new_sku1'"));
    assert.equal(e?.code, '42501');
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
});

await ta('[3] 画面のロールの守り (関数の中の書き込みも): 約束の出品でない行・Amazon の操作でない約束・約束なし = 42501 (間違えた関数を作っても書けない)', async () => {
  // 試験だけの「間違えた」security definer の関数 (持ち主が作る): 約束 (出品 A) を書いてから、別の出品 B の構成を直す / sku_edit の約束で構成を直す / 約束なしで直す
  const lidA = await lidOf('new_sku1'); const lidB = await lidOf('pr_a001');
  await pg.query(`create function ops.test_bad_writer(p_op text, p_lid bigint, p_target bigint, p_with_session boolean) returns void language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
    declare v uuid := gen_random_uuid();
    begin
      if p_with_session then
        insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, listing_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                               actor_id, source_system, db_user, phase, owner_hash, ownership)
          values (v, txid_current(), gen_random_uuid(), p_op, case when p_op = 'sku_edit' then (select min(sku_id) from core.skus) end, case when p_op = 'sku_edit' then null else p_lid end,
                  '{}', '{}', repeat('a', 64), repeat('a', 64), '{}', 'x', case when p_op = 'sku_edit' then 'portal_master_edit' else 'portal_amazon_map' end, 'master_edit', 'new_open', repeat('a', 64),
                  (select ownership from ops.master_write_sessions where operation = 'amazon_map_save' order by created_at desc limit 1));
        perform set_config('ops.master_write_session', v::text, true);
      end if;
      perform set_config('core.source_system', 'portal_amazon_map', true);
      update core.listing_components set qty = qty + 1 where listing_id = p_target;
    end $$`);
  await pg.query('grant execute on function ops.test_bad_writer(text, bigint, bigint, boolean) to master_edit');
  try {
    for (const [op, lid, target, withSession, re] of [['amazon_map_save', lidA, lidB, true, /master_write_target/], ['sku_edit', lidA, lidB, true, /master_write_operation/],
      ['amazon_map_save', lidB, lidB, false, /master_write_session_required/]]) {
      await pg.query('begin');
      try {
        await pg.query('set local role master_edit');
        const e = await errOf(pg.query('select ops.test_bad_writer($1, $2, $3, $4)', [op, lid, target, withSession]));
        assert.equal(e?.code, '42501', `${op}: ${e?.message}`); assert.match(e.message, re);
      } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
    }
  } finally { await pg.query('drop function ops.test_bad_writer(text, bigint, bigint, boolean)'); }
});

console.log('\n墓標');
await ta('[6] 墓標: 理由が要る・構成を全部消す・行は残る・持ち主でも DELETE / TRUNCATE は拒む・画面のロールに DELETE なし・もう墓標 = 409・無い = 404', async () => {
  await rejectsWith(del('new_sku1', ''), 400, 'invalid_input');
  const rid = uuid();
  const r = await del('new_sku1', '出品をやめた', { requestId: rid });
  assert.equal(r.state, 'deleted'); assert.deepEqual(r.removed.map((x) => [x.code, x.qty]), [['a001', 4], ['a002', 2]]);
  const lid = await lidOf('new_sku1');
  assert.deepEqual(await compsOfListing('new_sku1'), []);
  assert.deepEqual((await q('select state, deleted_by, deleted_reason, deleted_at is not null as d from core.amazon_sku_maps where listing_id = $1', [lid]))[0],
    { state: 'deleted', deleted_by: 'naka@test', deleted_reason: '出品をやめた', d: true });
  assert.equal((await q('select operation, listing_id::text as l from ops.master_edit_requests where request_id = $1', [rid]))[0].operation, 'amazon_map_delete');
  for (const sql of ['delete from core.amazon_sku_maps where listing_id = $1', 'truncate core.amazon_sku_maps']) {
    const e = await errOf(db.query(sql, sql.includes('$1') ? [lid] : undefined));
    assert.equal(e?.code, '42501', sql); assert.match(e.message, /amazon_map_no_delete/);
  }
  assert.equal(Number((await q('select count(*)::int as n from core.amazon_sku_maps where listing_id = $1', [lid]))[0].n), 1);
  const priv = (await q(`select has_table_privilege('master_edit', 'core.amazon_sku_maps', 'DELETE') as d, has_table_privilege('master_edit', 'core.amazon_sku_maps', 'INSERT') as i,
    has_table_privilege('master_edit', 'core.amazon_sku_maps', 'SELECT') as s, has_table_privilege('master_edit', 'core.listing_components', 'DELETE') as cd`))[0];
  assert.deepEqual(priv, { d: false, i: false, s: true, cd: false });
  await rejectsWith(del('new_sku1', 'もう一度'), 409, 'already_deleted');
  await rejectsWith(del('zz_none', 'ない'), 404, 'not_found');
});

console.log('\n夜間ロード');
await ta('[9] 夜間ロード (持ち主 company): 墓標の出品に完全一致を作り直さない・対応の出品は触らない・対応の無い FBM は完全一致・SKU マスタ / Sheet の構成は使わない', async () => {
  // a004 (FBM の完全一致の出品) を対応にしてから墓標にする
  await save('a004', '単品 4', [{ code: 'a004', qty: 2 }]);
  await del('a004', '試験: 墓標');
  assert.deepEqual(await compsOfListing('a004'), []);
  const plan = makePlan();
  plan.listings.push({ mall: 'amazon', shopCode: AMZ, listingCode: 'a006', title: 'FBM 新', status: 'active', components: [{ code: 'a006', qty: 1, resolution: 'exact', evidence: { source: 'fbm_ne_code' } }], asinCandidates: [], fnskuCandidates: [], evidenceSource: 'amazon_fees_fbm' });
  plan.listings.push({ mall: 'amazon', shopCode: AMZ, listingCode: 'pr_new_master', title: 'SKU マスタだけ', status: 'active', components: [{ code: 'a001', qty: 5, sortOrder: 0, resolution: 'imported', evidence: { source: 'm_sku_master' } }], asinCandidates: [], fnskuCandidates: [], evidenceSource: 'mirror_sku_master' });
  // SKU マスタの材料が対応と違っても、対応 (pr_a001) は変わらない
  plan.listings[0].components = [{ code: 'a002', qty: 9, sortOrder: 0, resolution: 'imported', evidence: { source: 'm_sku_master' } }];
  const r = await load(db, ALL_COMPANY, plan, 'load_company_1');
  assert.deepEqual(await compsOfListing('a004'), []);                              // 墓標 = 作り直さない
  assert.deepEqual(await compsOfListing('pr_a001'), [['a001', 1, 0, 'imported']]);  // 対応 = 触らない
  assert.deepEqual(await compsOfListing('a006'), [['a006', 1, 0, 'exact']]);        // 対応の無い FBM = 完全一致
  assert.deepEqual(await compsOfListing('pr_new_master'), []);                     // SKU マスタの構成は材料にしない (出品は作る)
  assert.ok(await lidOf('pr_new_master'));
  assert.ok(r.sections.listing_components.notes.some((n) => /Amazon の構成 \d+ 行は見送り/.test(n)), JSON.stringify(r.sections.listing_components.notes));
  assert.ok(r.sections.listing_components.notes.some((n) => /対応 \(Company DB\) がある出品の構成 \d+ 行は見送り/.test(n)));
  // 2 回目も同じ (変わらない)
  const r2 = await load(db, ALL_COMPANY, plan, 'load_company_2');
  assert.equal(r2.summary.listing_components.applied, 0);
});

await ta('[9] 持ち主 load でも、対応・墓標のある出品の構成は作らない (今は対応 0 件 = 今の動きのまま)', async () => {
  const r = await load(db, MASTER_OWNERSHIP, makePlan(), 'load_legacy_1');
  assert.deepEqual(await compsOfListing('a004'), []);
  assert.deepEqual(await compsOfListing('pr_a001'), [['a001', 1, 0, 'imported']]);
  assert.ok(r.sections.listing_components.notes.some((n) => /対応 \(Company DB\) がある出品の構成/.test(n)));
});

await ta('[6] 墓標から戻す: 同じ seller SKU をもう一度登録 = active・登録日が新しい・構成は足す (manual)・origin portal', async () => {
  const lid = await lidOf('a004');
  const before = (await q('select registered_at::text as r from core.amazon_sku_maps where listing_id = $1', [lid]))[0].r;
  const r = await save('a004', '単品 4 (戻した)', [{ code: 'a004', qty: 1 }]);
  assert.equal(r.revived, true);
  const m = (await q('select state, origin, registered_at::text as r, deleted_at, deleted_reason from core.amazon_sku_maps where listing_id = $1', [lid]))[0];
  assert.equal(m.state, 'active'); assert.equal(m.origin, 'portal'); assert.notEqual(m.r, before); assert.equal(m.deleted_at, null); assert.equal(m.deleted_reason, null);
  assert.deepEqual(await compsOfListing('a004'), [['a004', 1, 0, 'manual']]);
});

console.log('\n不変条件・書き手');
await ta('[8] 構成の書き手: new_open の間、対応のある出品の構成は夜間ロードの名前・取引の設定なしでは書けない (対応の無い出品は今までどおり)', async () => {
  const lid = await lidOf('a004');
  const a002 = await skuIdOf('a002');
  for (const src of ['company_db_load', '']) {
    await pg.query('begin');
    try {
      await pg.query(`select set_config('core.source_system', $1, true)`, [src]);
      const e = await errOf(pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 1, 'exact', 'system')`, [lid, a002]));
      assert.equal(e?.code, '42501', src); assert.match(e.message, /amazon_map_writer/);
    } finally { await pg.query('rollback'); }
  }
  await pg.query('begin');
  try {
    await pg.query(`select set_config('core.source_system', 'company_db_load', true)`);
    const e = await errOf(pg.query(`update core.amazon_sku_maps set name = '夜間' where listing_id = $1`, [lid]));
    assert.equal(e?.code, '42501');
  } finally { await pg.query('rollback'); }
  // 対応の無い出品 (a003) = 夜間ロードの名前で書ける
  await pg.query('begin');
  try {
    await pg.query(`select set_config('core.source_system', 'company_db_load', true)`);
    assert.equal(await errOf(pg.query(`update core.listing_components set qty = 1 where listing_id = (select listing_id from core.listings where listing_code = 'a003' and mall = 'amazon')`)), null);
  } finally { await pg.query('rollback'); }
});

await ta('[7] 不変条件 (commit のとき): 構成 0 行の active・並びの隙間・墓標に構成・出品のコード / モールを変える・Amazon でない出品の対応 = 23514 で取引ごと拒む', async () => {
  const lid = await lidOf('pr_a001');
  const a002 = await skuIdOf('a002');
  const tx = async (fn) => {
    await pg.query('begin');
    await pg.query(`select set_config('core.source_system', 'amazon_map_migration', true)`);
    let err = null;
    try { await fn(); await pg.query('commit'); } catch (e) { err = e; try { await pg.query('rollback'); } catch { /* */ } }
    return err;
  };
  const cases = [
    ['構成 0 行', () => pg.query('delete from core.listing_components where listing_id = $1', [lid]), /有効な対応に構成が無い/],
    ['並びの隙間', () => pg.query('update core.listing_components set sort_order = 3 where listing_id = $1', [lid]), /0\.\.0 でない/],
    ['並びの重なり', () => pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, 1, 0, 'manual', 'human')`, [lid, a002]), /0\.\.1 でない/],
    ['墓標に構成', async () => {
      await pg.query(`update core.amazon_sku_maps set state = 'deleted', deleted_at = now(), deleted_by = 't', deleted_reason = 't' where listing_id = $1`, [lid]);
    }, /墓標 \(deleted\) に構成が 1 行残っている/],
    ['出品のコード', () => pg.query(`update core.listings set listing_code = 'pr_a001x' where listing_id = $1`, [lid]), /出品のコード/],
    ['モール', () => pg.query(`update core.listings set mall = 'rakuten', shop_code = 'main' where listing_id = $1`, [lid]), /Amazon \(日本\) でない/],
    ['Amazon でない出品に対応', () => pg.query(`insert into core.amazon_sku_maps (listing_id, seller_sku, name, state, origin, registered_at, changed_at)
      select listing_id, 'a001', 'x', 'active', 'portal', now(), now() from core.listings where mall = 'rakuten' and listing_code = 'a001'`), /Amazon \(日本\) でない/],
  ];
  for (const [label, fn, re] of cases) {
    const e = await tx(fn);
    assert.equal(e?.code, '23514', `${label}: ${e?.code} ${e?.message}`); assert.match(e.message, re, label);
  }
  assert.deepEqual(await compsOfListing('pr_a001'), [['a001', 1, 0, 'imported']]);
  assert.equal((await q('select state from core.amazon_sku_maps where listing_id = $1', [lid]))[0].state, 'active');
  // 同じ取引で直せば通る (deferred = 途中の形は見ない)
  assert.equal(await tx(async () => {
    await pg.query('update core.listing_components set sort_order = 5 where listing_id = $1', [lid]);
    await pg.query('update core.listing_components set sort_order = 0 where listing_id = $1', [lid]);
  }), null);
});

await ta('[7] 表の CHECK (Codex #1586 R1 M1): 持ち主の手の SQL でも、seller SKU の前後の NBSP・全角の空白・TAB、名前が TAB・NBSP・全角の空白だけ = 23514', async () => {
  const lid = await lidOf('a003');
  const NB = String.fromCharCode(0xa0); const TAB = String.fromCharCode(9);
  const cases = [['a003' + NB, '名前', 'ck_asm_seller_sku'], [FW_SPACE + 'a003', '名前', 'ck_asm_seller_sku'], ['a003', TAB + TAB, 'ck_asm_name'], ['a003', NB, 'ck_asm_name'], ['a003', FW_SPACE + ' ', 'ck_asm_name'], ['A003', '名前', 'ck_asm_seller_sku']];
  for (const [sku, name, con] of cases) {
    await pg.query('begin');
    try {
      await pg.query(`select set_config('core.source_system', 'amazon_map_migration', true)`);
      const e = await errOf(pg.query(`insert into core.amazon_sku_maps (listing_id, seller_sku, name, state, origin, registered_at, changed_at) values ($1, $2, $3, 'active', 'legacy', now(), now())`, [lid, sku, name]));
      assert.equal(e?.code, '23514', `${JSON.stringify([sku, name])}: ${e?.code} ${e?.message}`); assert.equal(e.constraint, con);
    } finally { await pg.query('rollback'); }
  }
});

console.log('\n写しの並べ方');
await ta('[10] Company DB から作る写しの行: 受け手の決まりに合う・session の時間帯 (UTC / 東京) によらず同じハッシュ・墓標は入らない', async () => {
  const canon = await A.readCompanyAmazonMapCanon(db);
  assert.deepEqual(K.validateSkuMap(canon), []);
  assert.ok(!canon.master.some((m) => m.seller_sku === 'new_sku1'));   // 墓標
  assert.ok(canon.master.some((m) => m.seller_sku === 'a004'));
  const d1 = K.skuMapDigest(canon);
  await pg.query(`set timezone = 'Asia/Tokyo'`);
  const d2 = K.skuMapDigest(await A.readCompanyAmazonMapCanon(db));
  await pg.query(`set timezone = 'UTC'`);
  assert.equal(d2.content_hash, d1.content_hash);
  assert.ok(canon.components.every((c) => K.isCanonicalTimestamp(c.created_at) && K.isCanonicalTimestamp(c.updated_at)));
});

await ta('[6] 復元 (バックアップ): 墓標も含めて戻る (復元はユーザーの trigger を止めて消して入れ直す)・2 回目の復元 (行のある DB) も通る・復元の後も普通の DELETE は拒む', async () => {
  const { dumpCompanyDb, restoreCompanyDb } = await import('../apps/company-db/backup/dump.mjs');
  const lines = [];
  await dumpCompanyDb(db, (l) => lines.push(l), { log: quiet });
  const text = lines.join('\n');
  const dst = new PGlite(); const ddb = pgliteAdapter(dst);
  await applyMigrations(ddb, { log: quiet });
  const maps = async (x) => (await x.query(`select listing_id::text as l, seller_sku, state, deleted_reason, (extract(epoch from registered_at) * 1000)::bigint::text as r
    from core.amazon_sku_maps order by listing_id`)).rows;
  await restoreCompanyDb(ddb, text, { log: quiet });
  assert.deepEqual(await maps(ddb), await maps(db));
  assert.ok((await maps(ddb)).some((m) => m.state === 'deleted'));
  await restoreCompanyDb(ddb, text, { log: quiet });   // 行のある DB にもう一度 = 消して入れ直す (trigger は止めている)
  assert.deepEqual(await maps(ddb), await maps(db));
  const e = await errOf(ddb.query("delete from core.amazon_sku_maps where seller_sku = 'new_sku1'"));
  assert.match(e?.message || '', /amazon_map_no_delete/);
  await dst.close();
});

await ta('[6] 消えた対応 (Codex #1586 R1 High の手当て): 持ち主が trigger を止めて墓標を消しても、夜間ロードは自動の構成を作り直さない・報告に出る・切替の前提が止める', async () => {
  const E5 = await setupDb();
  await createRoles(E5.pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(E5.pg, {});
  await load(E5.db);
  await openCutover(E5, ALL_COMPANY);
  const v0 = (await A.readAmazonMap(E5.db, 'a004')).versions;
  await asRole(E5, 'master_edit', () => A.saveAmazonMap(E5.db, { actor: 'naka@test', requestId: uuid(), sellerSku: 'a004', name: '単品 4', components: [{ code: 'a004', qty: 1 }], seen: { versions: v0 } }, { ownership: ALL_COMPANY, open: true }));
  const v1 = (await A.readAmazonMap(E5.db, 'a004')).versions;
  await asRole(E5, 'master_edit', () => A.deleteAmazonMap(E5.db, { actor: 'naka@test', requestId: uuid(), sellerSku: 'a004', reason: '墓標', seen: { versions: v1 } }, { ownership: ALL_COMPANY, open: true }));
  const lid = Number((await E5.db.query("select listing_id from core.amazon_sku_maps where seller_sku = 'a004'")).rows[0].listing_id);
  assert.deepEqual((await E5.db.query('select * from ops.amazon_map_lost_listings()')).rows, []);
  // 持ち主の誤り: trigger を止めて墓標を消す (復元のやり方と同じ = 持ち主ならできる = 残る危うさ)
  await E5.pg.query('begin');
  await E5.pg.query('alter table core.amazon_sku_maps disable trigger user');
  await E5.pg.query('delete from core.amazon_sku_maps where listing_id = $1', [lid]);
  await E5.pg.query('alter table core.amazon_sku_maps enable trigger user');
  await E5.pg.query('commit');
  const lost = (await E5.db.query('select listing_id::text as l, seller_sku from ops.amazon_map_lost_listings()')).rows;
  assert.deepEqual(lost, [{ l: String(lid), seller_sku: 'a004' }]);
  const r = await load(E5.db, ALL_COMPANY, makePlan(), 'load_lost_1');
  const comps = (await E5.db.query('select count(*)::int as n from core.listing_components where listing_id = $1', [lid])).rows[0].n;
  assert.equal(comps, 0, '消えた墓標の出品に FBM の完全一致を作り直した');
  assert.deepEqual(r.conflicts.filter((c) => c.kind === 'amazon_map_lost').map((c) => [c.count, c.samples]), [[1, [[String(lid), 'a004']]]]);
  const rl = await load(E5.db, MASTER_OWNERSHIP, makePlan(), 'load_lost_2');   // 持ち主 load でも
  assert.equal((await E5.db.query('select count(*)::int as n from core.listing_components where listing_id = $1', [lid])).rows[0].n, 0);
  assert.ok(rl.conflicts.some((c) => c.kind === 'amazon_map_lost'));
  // 切替の前提: company_owner / new_open に進めない (差し込み口の表に載っている)
  assert.deepEqual((await E5.db.query("select ops.amazon_map_prereq('company_owner', 'new_open') as p")).rows[0].p.map((x) => x.slice(0, 16)), ['amazon_map_lost:']);
  assert.deepEqual((await E5.db.query("select ops.amazon_map_prereq('legacy_open', 'frozen') as p")).rows[0].p, []);
  assert.ok((await E5.db.query("select ops.master_cutover_prereq_problems('company_owner', 'new_open') as p")).rows[0].p.some((x) => x.startsWith('0054_amazon_map: amazon_map_lost')));
  await E5.pg.close();
});

await ta('[11] 影運転の先の確かめ (Codex #1586 R1 M2): 本番の URL が無い・同じホスト / ポート / DB 名 (別のユーザーでも)・つないだ DB の識別が同じ・本番に届かない = 断る', async () => {
  const PROD = 'postgres://owner:pw@db.example.internal:5432/company';
  assert.equal(CLI.sameDatabaseUrl('postgres://other:x@DB.example.internal/company', PROD), true);   // ユーザー・パスワード・既定のポートは見ない
  assert.equal(CLI.sameDatabaseUrl('postgres://owner:pw@db.example.internal:5432/company_test', PROD), false);
  assert.equal(CLI.sameDatabaseUrl('not a url', PROD), true);   // 読めない = 断る側
  const code = async (p) => { try { await p; return 'ok'; } catch (e) { return e.code; } };
  const fakeOpen = (ids) => async (url) => ({ query: async (sql) => {
    const id = ids[url]; if (!id) throw new Error('ECONNREFUSED');
    if (/pg_control_system/.test(sql)) { if (id.sys == null) throw new Error('permission denied'); return { rows: [{ s: id.sys }] }; }
    return { rows: [{ db: id.db, addr: id.addr, port: id.port }] };
  }, end: async () => {} });
  const T = 'postgres://t:pw@test-host:5432/company';
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: T, productionUrl: undefined, openClient: fakeOpen({}) })), 'AMAZON_MAP_MIGRATE_ARGS');
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: 'postgres://someone:x@db.example.internal/company', productionUrl: PROD, openClient: fakeOpen({}) })), 'AMAZON_MAP_MIGRATE_PRODUCTION');
  // 別の名前 (DNS の別名) でも同じ DB = 識別で断る
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: T, productionUrl: PROD, openClient: fakeOpen({ [T]: { db: 'company', sys: '7001', addr: '10.0.0.5', port: 5432 }, [PROD]: { db: 'company', sys: '7001', addr: '10.0.0.5', port: 5432 } }) })), 'AMAZON_MAP_MIGRATE_PRODUCTION');
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: T, productionUrl: PROD, openClient: fakeOpen({ [T]: { db: 'company', sys: null, addr: '10.0.0.5', port: 5432 }, [PROD]: { db: 'company', sys: null, addr: '10.0.0.5', port: 5432 } }) })), 'AMAZON_MAP_MIGRATE_PRODUCTION');
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: T, productionUrl: PROD, openClient: fakeOpen({ [T]: { db: 'company', sys: null, addr: '10.0.0.9', port: 5432 }, [PROD]: { db: 'company', sys: '7001', addr: '10.0.0.5', port: 5432 } }) })), 'AMAZON_MAP_MIGRATE_PRODUCTION');   // 識別が片方読めない + 同じ DB 名 = 断る側
  // 本番に届かない = 確かめられない = 断る
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: T, productionUrl: PROD, openClient: fakeOpen({ [T]: { db: 'company', sys: '9', addr: '10.0.0.9', port: 5432 } }) })), 'AMAZON_MAP_MIGRATE_ARGS');
  // 別のクラスター (system_identifier が違う)・同じクラスターの別の DB = 通す
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: T, productionUrl: PROD, openClient: fakeOpen({ [T]: { db: 'company', sys: '9', addr: '10.0.0.9', port: 5432 }, [PROD]: { db: 'company', sys: '7001', addr: '10.0.0.5', port: 5432 } }) })), 'ok');
  assert.equal(await code(CLI.assertShadowTarget({ targetUrl: T, productionUrl: PROD, openClient: fakeOpen({ [T]: { db: 'company_shadow', sys: '7001', addr: '10.0.0.5', port: 5432 }, [PROD]: { db: 'company', sys: '7001', addr: '10.0.0.5', port: 5432 } }) })), 'ok');
});

await ta('[11] fba.db (Codex #1586 R1 M3): 影運転も apply も Sheet にだけある SKU の一覧が要る・--fba-db が無い・ファイルが無い・sku_mapping の表が無い = すぐ断る', async () => {
  const legacy = { masterRows: [{ seller_sku: 'pr_a001', 商品名: 'x', created_at: '2026-01-01T00:00:00.000Z', updated_at: '2026-01-01T00:00:00.000Z' }], componentRows: [] };
  await assert.rejects(() => M.runAmazonMapMigration(db, legacy, { mode: 'shadow' }), (e) => e.code === 'AMAZON_MAP_MIGRATE_INVALID');
  await assert.rejects(() => M.runAmazonMapMigration(db, legacy, { mode: 'apply', expectHash: M.legacyDigest(legacy).content_hash }), (e) => e.code === 'AMAZON_MAP_MIGRATE_INVALID');
  assert.throws(() => CLI.sheetOnlyFrom(null, legacy), (e) => e.code === 'AMAZON_MAP_MIGRATE_ARGS');
  assert.throws(() => CLI.sheetOnlyFrom(path.join(tmp, 'nothing.db'), legacy), (e) => e.code === 'AMAZON_MAP_MIGRATE_ARGS');
  const empty = path.join(tmp, 'fba-empty.db'); new Database(empty).close();
  assert.throws(() => CLI.sheetOnlyFrom(empty, legacy), (e) => e.code === 'AMAZON_MAP_MIGRATE_ARGS' && /sku_mapping/.test(e.message));
  const fba = path.join(tmp, 'fba.db');
  { const f = new Database(fba); f.exec('create table sku_mapping (amazon_sku text)'); f.prepare('insert into sku_mapping values (?), (?)').run('PR_A001', 'sheet_x'); f.close(); }
  assert.deepEqual(CLI.sheetOnlyFrom(fba, legacy), ['sheet_x']);   // 正規化で SKU マスタにある = 数えない
});

console.log('\n移行 (影運転・apply)');
/** miniPC の warehouse.db の SKU マスタ (db.js と同じ形) */
function makeLegacy(file, masters, comps) {
  const s = new Database(file);
  s.exec(`CREATE TABLE m_sku_master (seller_sku TEXT NOT NULL PRIMARY KEY, 商品名 TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), created_by TEXT, updated_by TEXT, CHECK (trim(seller_sku) <> ''), CHECK (trim(商品名) <> ''),
      CHECK (seller_sku = lower(seller_sku) AND trim(seller_sku) = seller_sku));
    CREATE TABLE m_sku_components (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1, sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (seller_sku, ne_code), FOREIGN KEY (seller_sku) REFERENCES m_sku_master(seller_sku) ON DELETE CASCADE, CHECK (数量 > 0));`);
  for (const m of masters) s.prepare('insert into m_sku_master (seller_sku, 商品名, created_at, updated_at, created_by, updated_by) values (?, ?, ?, ?, ?, ?)').run(m[0], m[1], m[2], m[3], 'legacy@test', null);
  for (const c of comps) s.prepare('insert into m_sku_components (seller_sku, ne_code, 数量, sort_order, created_at, updated_at) values (?, ?, ?, ?, ?, ?)').run(...c);
  s.close();
}
const T1 = '2026-05-01T01:02:03.456Z'; const T2 = '2026-06-01T00:00:00.000Z';
const CLEAN_MASTERS = [['pr_a001', 'SKU マスタの 1', T1, T2], ['pr_pack2', 'SKU マスタの 2 個組', T1, T1], ['a003', '単品 3 を FBA でも', T1, T2], ['pr_new1', '新しい組 (出品なし)', T1, T1]];
const CLEAN_COMPS = [['pr_a001', 'a001', 1, 0, T1, T1], ['pr_pack2', 'a001', 2, 0, T1, T1], ['pr_pack2', 'a002', 1, 1, T1, T2], ['a003', 'a003', 2, 0, T1, T1],
  ['pr_new1', 'a005', 1, 0, T1, T1], ['pr_new1', 'a006', 3, 1, T2, T2]];

await ta('[11] 影運転: 止める項目を数える (無い NE コード・並びの隙間・構成なし・正規化で重なる seller SKU・大文字・形の違う時刻・親の無い構成・Sheet だけ) ・移せた SKU のハッシュが一致・必ず巻き戻す・古い表は変えない', async () => {
  const E2 = await setupDb();
  await load(E2.db);
  const file = path.join(tmp, 'legacy-shadow.db');
  makeLegacy(file, [...CLEAN_MASTERS, ['bad_ne', 'NE に無い', T1, T1], ['bad_sort', '並びの隙間', T1, T1], ['empty1', '構成なし', T1, T1], ['dup-a', '重なり 1', T1, T1], ['dup－a', '重なり 2', T1, T1],
    ['bad_ts', '時刻', '2026-05-01 01:02:03', T1]],
  [...CLEAN_COMPS, ['bad_ne', 'nosuch', 1, 0, T1, T1], ['bad_sort', 'a001', 1, 0, T1, T1], ['bad_sort', 'a002', 1, 2, T1, T1], ['dup-a', 'a001', 1, 0, T1, T1], ['dup－a', 'a002', 1, 0, T1, T1],
    ['bad_ts', 'a001', 1, 0, T1, T1]]);
  const bytes0 = crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
  const legacy = M.readLegacyAmazonMaps(file);
  const snap0 = await snapshot(E2.db);
  const r = await M.runAmazonMapMigration(E2.db, legacy, { mode: 'shadow', sheetOnly: ['sheet_only1'] });
  assert.equal(r.committed, false);
  assert.deepEqual(Object.fromEntries(Object.entries(r.blockers).map(([k, v]) => [k, v.count])),
    { not_in_company: 1, sort_gap: 1, no_components: 1, seller_sku_collision: 2, timestamp: 1, sheet_only: 1 });
  assert.equal(r.subset.skus, 4);
  assert.equal(r.subset.match, true, JSON.stringify(r.subset));
  assert.ok(r.legacy_digest.error);   // 形の違う時刻がある = 全体のハッシュは作れない
  assert.ok(r.counts.deleted >= 0 && r.counts.listings_created === 1);
  // 巻き戻した = DB は前と同じ・古い表のファイルも同じ
  assert.deepEqual(await snapshot(E2.db), snap0);
  assert.equal(Number((await E2.db.query('select count(*)::int as n from core.amazon_sku_maps')).rows[0].n), 0);
  assert.equal(crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex'), bytes0);
  // apply は frozen の間だけ・止める項目があれば断る
  await assert.rejects(() => M.runAmazonMapMigration(E2.db, legacy, { mode: 'apply', expectHash: 'a'.repeat(64), sheetOnly: [] }), /H0/);
});

await ta('[11] apply (切替の日 ③): frozen だけ・H0 と同じ・同じ行は時刻だけ (記録・出品の version を増やさない)・FBM の自動の行は消える・作り直したハッシュ = H0・移行の後の夜間ロード 2 回で構成が変わらない', async () => {
  const E3 = await setupDb();
  await createRoles(E3.pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(E3.pg, {});
  await load(E3.db);
  const file = path.join(tmp, 'legacy-apply.db');
  makeLegacy(file, CLEAN_MASTERS, CLEAN_COMPS);
  const legacy = M.readLegacyAmazonMaps(file);
  const h0 = M.legacyDigest(legacy).content_hash;
  assert.match(h0, /^[0-9a-f]{64}$/);
  // legacy_open = 断る
  await assert.rejects(() => M.runAmazonMapMigration(E3.db, legacy, { mode: 'apply', expectHash: h0, sheetOnly: [] }), (e) => e.code === 'AMAZON_MAP_MIGRATE_PHASE');
  await openCutover(E3, ALL_COMPANY, 'frozen');
  // 止める項目があれば断る (何も書かない)
  const fileBad = path.join(tmp, 'legacy-bad.db');
  makeLegacy(fileBad, [...CLEAN_MASTERS, ['empty1', '構成なし', T1, T1]], CLEAN_COMPS);
  const legacyBad = M.readLegacyAmazonMaps(fileBad);
  await assert.rejects(() => M.runAmazonMapMigration(E3.db, legacyBad, { mode: 'apply', expectHash: M.legacyDigest(legacyBad).content_hash, sheetOnly: [] }), (e) => e.code === 'AMAZON_MAP_MIGRATE_BLOCKED');
  // 本物
  const lidPr = (await E3.db.query("select listing_id::text as id, version::text as v from core.listings where listing_code = 'pr_a001'")).rows[0];
  const evBefore = Number((await E3.db.query('select coalesce(max(event_id), 0)::int as n from events.master_change_events')).rows[0].n);
  const r = await M.runAmazonMapMigration(E3.db, legacy, { mode: 'apply', expectHash: h0, actor: 'naka@test', sheetOnly: [] });
  assert.equal(r.committed, true); assert.equal(r.subset.match, true); assert.equal(r.subset.company.content_hash, h0);
  // 同じ = pr_a001 の a001・pr_pack2 の a001 (時刻だけ) / 直した = pr_pack2 の a002 (並び 2 → 1)・a003 (FBM の完全一致の行の数量 1 → 2) / 足した = pr_new1 の 2 行
  assert.deepEqual(r.counts, { listings_created: 1, maps: 4, same: 2, time_only: 2, updated: 2, inserted: 2, deleted: 0 });
  // 同じ行 (pr_a001 の a001) は時刻だけ = 構成・出品の識別の変更の記録なし (0049 の印 = listing_component の全部・listing の INSERT / DELETE / mall・shop_code・listing_code を増やさない)
  const ev = (await E3.db.query(`select entity_type, operation, attribute, source_system from events.master_change_events where event_id > $1
    and ((entity_type = 'listing_component' and (entity_key ->> 'listing_id')::bigint = $2) or (entity_type = 'listing' and entity_id = $2))`, [evBefore, lidPr.id])).rows;
  assert.deepEqual(ev, []);
  assert.ok((await E3.db.query(`select count(*)::int as n from events.master_change_events where event_id > $1 and source_system = 'amazon_map_migration'`, [evBefore])).rows[0].n > 0);
  assert.equal(Number((await E3.db.query("select count(*)::int as n from core.amazon_sku_maps where origin = 'legacy'")).rows[0].n), 4);
  // 移した値: 時刻は古い表のまま (文字 → 時刻 → 文字)
  const canon = await A.readCompanyAmazonMapCanon(E3.db);
  assert.equal(K.skuMapDigest(canon).content_hash, h0);
  assert.deepEqual(canon.master.find((m) => m.seller_sku === 'pr_a001'), { seller_sku: 'pr_a001', name: 'SKU マスタの 1', created_at: T1, updated_at: T2 });
  // 2 回目の apply は断る (1 回だけ)
  await assert.rejects(() => M.runAmazonMapMigration(E3.db, legacy, { mode: 'apply', expectHash: h0, sheetOnly: [] }), (e) => e.code === 'AMAZON_MAP_MIGRATE_EXISTS');
  // 移行の後の夜間ロード (持ち主 load のまま・frozen) 2 回 = 構成が変わらない
  for (const run of ['after_1', 'after_2']) {
    await load(E3.db, MASTER_OWNERSHIP, makePlan(), run);
    assert.equal(K.skuMapDigest(await A.readCompanyAmazonMapCanon(E3.db)).content_hash, h0, run);
  }
  // 持ち主 company で 2 回も同じ
  for (const run of ['after_c1', 'after_c2']) {
    await load(E3.db, ALL_COMPANY, makePlan(), run);
    assert.equal(K.skuMapDigest(await A.readCompanyAmazonMapCanon(E3.db)).content_hash, h0, run);
  }
});

console.log('\n画面 (router)');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test, other@test';
process.env.MASTER_EDIT_OPEN = '1';
__setPgClientFactory(async (url) => {
  const role = /master_edit@/.test(url) ? 'master_edit' : 'deploy';
  await pg.query(`set role ${role}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
__setClock(() => new Date('2030-01-10T03:00:00Z').getTime());
__setOwnership(ALL_COMPANY);
__setAmazonChannelsProvider(async () => new Map([['pr_a001', 'FBA'], ['a003', 'FBM'], ['zz_sold', 'FBA'], ['zz_fbm', 'FBM']]));
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'editor' ? { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }
    : s === 'viewer' ? { authenticated: true, email: 'viewer@test', role: 'user', allowedApps: ['master-edit'] } : null;
  next();
});
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-edit`;
async function call(method, url, { body, session = 'editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text, headers: r.headers };
}
function checkScripts(html, expected) {
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((x) => x[1]);
  assert.equal(scripts.length, expected, `<script> の数 ${scripts.length}`);
  for (const s of scripts) { new vm.Script(s); assert.ok(!/<%|%>/.test(s), 'EJS のタグが JS に残っている'); }
}
const decode = (s) => s.replace(/&#34;/g, '"').replace(/&quot;/g, '"').replace(/&amp;/g, '&');

await ta('[12] 一覧・1 つ・変更の記録・つかいかた・末尾の / (描画と画面の JS)', async () => {
  const bare = await fetch(`${BASE}/amazon`, { headers: { 'x-test-session': 'editor' }, redirect: 'manual' });
  assert.equal(bare.status, 301); assert.equal(bare.headers.get('location'), '/apps/master-edit/amazon/');
  let r = await call('GET', '/amazon/');
  assert.equal(r.status, 200); checkScripts(r.text, 0);
  assert.ok(r.text.includes('sku?sku=pr_a001') && r.text.includes('sku?sku=a004') && r.text.includes('削除済み (墓標)'));
  assert.ok(!r.text.includes('いまは保存できません'));
  r = await call('GET', '/amazon/?state=deleted');
  assert.ok(r.text.includes('sku?sku=new_sku1') && !r.text.includes('sku?sku=a004"'));
  r = await call('GET', '/amazon/?q=a006');   // NE コードでも探せる (pr_new1 は無い = この DB では無し)
  assert.equal(r.status, 200);
  r = await call('GET', '/amazon/sku?sku=PR_A001');
  assert.equal(r.status, 200); checkScripts(r.text, 1);
  assert.ok(r.text.includes('pr_a001') && r.text.includes('FBA') && r.text.includes('対応を直す'));
  const versions = JSON.parse(decode(/data-versions="([^"]*)"/.exec(r.text)[1]));
  assert.deepEqual(versions, await versionsOf('pr_a001'));
  r = await call('GET', '/amazon/sku?sku=a003');   // 対応なし = 今の構成 (FBM の完全一致) を見せる
  assert.ok(r.text.includes('対応なし') && r.text.includes('FBM の完全一致') && r.text.includes('新しい対応'));
  r = await call('GET', '/amazon/sku?sku=new_sku1');
  assert.ok(r.text.includes('削除済み (墓標) です') && r.text.includes('もう一度登録する') && !r.text.includes('id="del-box"'));
  r = await call('GET', '/amazon/sku?sku=' + encodeURIComponent(`a${String.fromCharCode(9)}b`));
  assert.equal(r.status, 400);
  r = await call('GET', '/amazon/sku/history?sku=new_sku1');
  assert.equal(r.status, 200); assert.ok(r.text.includes('出品をやめた') && r.text.includes('マスタの入力 (Amazon SKU)'));
  r = await call('GET', '/amazon/sku/history?sku=zz_nothing');
  assert.equal(r.status, 404);
  const m = await call('GET', '/manual');
  for (const word of ['Amazon SKU の対応を直す', '墓標', '未登録', '07:00', 'NE確認済み']) assert.ok(m.text.includes(word), `つかいかたに「${word}」が無い`);
  const idx = await call('GET', '/');
  assert.ok(idx.text.includes('href="amazon/"'));
});

await ta('[12] 保存・削除の API: 名簿の人だけ・Origin が要る・保存の結果・削除 (理由)・閉じていれば 409', async () => {
  const v = await versionsOf('pr_pack2');
  let r = await call('POST', '/api/amazon/save', { body: { request_id: uuid(), seller_sku: 'pr_pack2', name: '2 個組', components: [{ code: 'a001', qty: 2 }, { code: 'a002', qty: 1 }], seen: { versions: v } }, session: 'viewer' });
  assert.equal(r.status, 403);
  r = await call('POST', '/api/amazon/save', { body: { request_id: uuid(), seller_sku: 'pr_pack2', name: '2 個組', components: [{ code: 'a001', qty: 2 }], seen: { versions: v } }, origin: false });
  assert.equal(r.status, 403);
  r = await call('POST', '/api/amazon/save', { body: { request_id: uuid(), seller_sku: 'pr_pack2', name: '2 個組', components: [{ code: 'a001', qty: 2 }, { code: 'a002', qty: 1 }], seen: { versions: v } } });
  assert.equal(r.status, 200, r.text); assert.equal(r.j.created, true);
  assert.deepEqual(await compsOfListing('pr_pack2'), [['a001', 2, 0, 'imported'], ['a002', 1, 1, 'manual']]);   // 並び 2 → 1 の行だけ manual
  r = await call('POST', '/api/amazon/save', { body: { request_id: uuid(), seller_sku: 'pr_pack2', name: '2 個組', components: [{ code: 'a001', qty: 2 }], seen: { versions: v } } });
  assert.equal(r.status, 409); assert.equal(r.j.reason, 'version_conflict');
  r = await call('POST', '/api/amazon/delete', { body: { request_id: uuid(), seller_sku: 'pr_pack2', reason: '', seen: { versions: await versionsOf('pr_pack2') } } });
  assert.equal(r.status, 400);
  r = await call('POST', '/api/amazon/delete', { body: { request_id: uuid(), seller_sku: 'pr_pack2', reason: '画面から削除', seen: { versions: await versionsOf('pr_pack2') } } });
  assert.equal(r.status, 200, r.text); assert.equal(r.j.state, 'deleted');
  process.env.MASTER_EDIT_OPEN = '0';
  r = await call('POST', '/api/amazon/save', { body: { request_id: uuid(), seller_sku: 'pr_pack2', name: '2 個組', components: [{ code: 'a001', qty: 2 }], seen: { versions: await versionsOf('pr_pack2') } } });
  assert.equal(r.status, 409); assert.equal(r.j.reason, 'before_cutover');
  const page = await call('GET', '/amazon/sku?sku=pr_pack2');
  assert.ok(page.text.includes('いまは保存できません'));
  process.env.MASTER_EDIT_OPEN = '1';
  __setOwnership({ ...ALL_COMPANY, 'listing_components.amazon': 'load' });
  const page2 = await call('GET', '/amazon/');   // 持ち主表が段階の記録と違う = 閉じている (帯)
  assert.ok(page2.text.includes('いまは保存できません'));
  __setOwnership(ALL_COMPANY);
});

await ta('[12] 未登録 (M11): 直近 7 日の Amazon の注文で構成が無い SKU・FBA / FBM で分ける・売上の公開が欠けた日は「未判定」', async () => {
  // 注文 (出品の無いコード zz_sold (FBA)・zz_fbm (FBM)・構成のある pr_a001・墓標の pr_pack2 (構成なし))
  const lidPr = await lidOf('pr_a001'); const lidPack = await lidOf('pr_pack2');
  let n = 0;
  const order = async (date, lines, { cancelled = false } = {}) => {
    n++;
    const o = (await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, 'amazon', 'amazon', $1, 'mall_api', $2::date, $2::date, $3, $4, 1, now(), 't', 'h') returning order_id`, [`o${n}`, date, cancelled ? 'cancelled' : 'shipped', cancelled])).rows[0].order_id;
    let i = 0;
    for (const [lid, code, qty, cq = 0] of lines) {
      i++;
      await pg.query(`insert into core.order_lines (company_id, order_id, line_key, listing_id, unresolved_code, qty, cancelled_qty, received_batch_seq) values (1, $1, $2, $3, $4, $5, $6, 1)`, [o, `l${i}`, lid, code, qty, cq]);
    }
  };
  await order('2030-01-09', [[null, 'zz_sold', 2], [lidPr, null, 1]]);
  await order('2030-01-10', [[null, 'zz_sold', 1], [null, 'zz_fbm', 4], [lidPack, null, 1]]);
  await order('2029-12-01', [[null, 'zz_old', 1]]);   // 7 日より前
  // 取り消し (Codex #1586 R1 Low): 全部取り消した明細・取り消した注文は数えない / 一部の取り消しは引いた数
  await order('2030-01-10', [[null, 'zz_cancel', 2, 2], [null, 'zz_sold', 3, 1]]);
  await order('2030-01-10', [[null, 'zz_hdr', 1]], { cancelled: true });
  let r = await call('GET', '/amazon/unmapped');
  assert.equal(r.status, 200); checkScripts(r.text, 0);
  assert.ok(r.text.includes('未判定'));   // 売上の日次が公開されていない
  assert.ok(r.text.includes('zz_sold') && !r.text.includes('zz_fbm') && !r.text.includes('zz_old'));
  r = await call('GET', '/amazon/unmapped?channel=FBM');
  assert.ok(r.text.includes('zz_fbm') && !r.text.includes('zz_sold'));
  r = await call('GET', '/amazon/unmapped?channel=all');
  assert.ok(r.text.includes('zz_sold') && r.text.includes('zz_fbm') && r.text.includes('削除済み (墓標)'));
  const rows = (await q(`select code, units::int as units, orders::int as orders, map_state from ops.amazon_map_unmapped_recent('2030-01-10', 7) order by code`));
  assert.deepEqual(rows.map((x) => [x.code, x.units, x.orders, x.map_state]), [['pr_pack2', 1, 1, 'deleted'], ['zz_fbm', 4, 1, null], ['zz_sold', 5, 3, null]]);
  assert.ok(!r.text.includes('zz_cancel') && !r.text.includes('zz_hdr'));
  // 公開がそろえば未判定でない
  await pg.query(`insert into mart.sales_daily_runs (run_id, company_id, mall, scope_key, session_id, started_at, finished_at, n_dates, n_rows, n_orders) values ('r1', 1, 'amazon', 'amazon', 's1', now(), now(), 7, 0, 0)`);
  await pg.query(`insert into mart.sales_daily_published (company_id, mall, scope_key, date_jst, run_id) select 1, 'amazon', 'amazon', d::date, 'r1' from generate_series('2030-01-04'::date, '2030-01-10'::date, interval '1 day') d`);
  r = await call('GET', '/amazon/unmapped');
  assert.ok(!r.text.includes('未判定:'));
  // 画面のロールは注文の表を読めない (関数だけ)
  const e = await errOf(asRole(E, 'master_edit', () => db.query('select count(*) from core.order_lines')));
  assert.equal(e?.code, '42501');
});

server.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
