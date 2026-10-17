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
 *  13 自分で作る入口 (10/11): 一覧の上の「新しい seller SKU の対応を作る」・/amazon/new (形・もうある・もしかして・気をつけること)・確かめの API (読むだけ)・
 *     まだ売れていない・出品も無い seller SKU を作る画面から保存 → 最初の注文から結びつく・後の夜間の取り込みは ASIN だけ付ける
 * 使い方: node scripts/test-amazon-map.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';
import express from 'express';
import Database from 'better-sqlite3';
// 広げる道 PR-2: 画面は DB の active に従う。試験の DB は全部の列を company にする = このコードの能力も全部 (code_behind の試験だけ戻す)
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
// 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const C = await import('../lib/master-cutover.mjs');
const A = await import('../lib/amazon-map-write.mjs');
const M = await import('../lib/amazon-map-migrate.mjs');
const K = await import('../lib/sku-map-canonical.js');
const CLI = await import('./company-db/amazon-map-migrate.mjs');
const { MasterWriteError } = await import('../lib/master-write.mjs');
const { default: router, __setPgClientFactory, __setClock, __setAmazonChannelsProvider, __setExportLimits } = await import('../apps/master-edit/router.mjs');

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
      single('a006', '単品 6'), single('a007', '単品 7'), single('a008', '単品 8 (どこの出品にも使わない)'),
      // 半角カナと全角カナだけが違うコード = core.norm_code (NFKC を使わない) では別の商品 (#1689 Codex R1 M2 の試験)
      single('ﾊﾁ-01', 'はちみつ 半角のコード'), single('ハチ-01', 'はちみつ 全角のコード'),
      { code: 'aset1', name: 'セット 1', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE' } },
      { code: 'aexc', name: '例外の SKU', kind: 'exception', taxRate: null, taxClass: null, handling: 'active', salesClass: null, cost: null },
      // 使えない見本 (例外の SKU) を a00 の前方一致の段に (要確認も使えるようになった 0073 の後の試験 [14] の並び)
      { code: 'a00x', name: '例外の SKU 2', kind: 'exception', taxRate: null, taxClass: null, handling: 'active', salesClass: null, cost: null }],
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
  await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(E.db, ownership);   // 0055 (④a): 段階の持ち主表 = 持ち主の epoch (本番 = ④a の activate)
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
  const E53 = await setupDb({ to: '0054' });   // 0054 までの DB (0057 の登録日など後の migration は比べる対象の外)
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
  const res = await applyMigrations(E52.db, { log: quiet, to: '0054' });   // 0054 だけ (0055 = ④a は別の話)
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
  const E54 = await setupDb({ to: '0054' });   // 🆕 0067 (PR-5) も約束の操作・書いてよい行・変更の記録の種類を足す = 0054 の差は 0054 までの DB で見る (0067 の差は test-master-variation.mjs の [S2])
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
await W2.useReal0058(E.pg);   // 広げる道 PR-2: 本物の 0058 (PR-1) の上 (許可なし = 本番の今)
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
  // 広げる道 PR-2: 持ち主は DB の active (配った config ではない)。active だけ Amazon を load にした = 段階の記録と違う = 409
  await W2.setActiveMapOnly(pg, { ...ALL_COMPANY, 'listing_components.amazon': 'load' });
  try { await rejectsWith(save('new_sku1', '名前', [{ code: 'a001', qty: 1 }]), 409, 'before_cutover'); } finally { await W2.setActiveMapOnly(pg, ALL_COMPANY); }
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
  await W2.useReal0058(E4.pg);   // 広げる道 PR-2: 本物の 0058 (PR-1) の上 (許可なし = 本番の今)
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

await ta('[5] 構成品と形: 無い・例外の SKU・やめた (cancelled) = 400 / seller SKU (大文字だけの違いは小文字に・全角の空白・制御文字・長すぎ)・名前 (空白だけ) / DB でも同じ決まり', async () => {
  await rejectsWith(save('new_sku2', 'x', [{ code: 'nosuch', qty: 1 }]), 400, 'invalid_input');
  await rejectsWith(save('new_sku2', 'x', [{ code: 'aexc', qty: 1 }]), 400, 'invalid_input');
  await pg.query(`select set_config('ops.registration_protocol', '1', false)`);
  await pg.query("update ops.master_registrations set state = 'cancelled' where sku_id = (select sku_id from core.skus where code = 'a007')");
  await pg.query(`select set_config('ops.registration_protocol', '', false)`);
  const e1 = await rejectsWith(save('new_sku2', 'x', [{ code: 'a007', qty: 1 }]), 400, 'invalid_input');
  assert.match(e1.message, /構成品に使えない登録の状態です \(やめた商品/);
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
    [{ components: [{ sku_id: a007, code: 'a007', qty: 1 }] }, /component_unusable.*状態 cancelled/],
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
  await W2.useReal0058(E5.pg);   // 広げる道 PR-2: 本物の 0058 (PR-1) の上 (許可なし = 本番の今)
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
/** Sheet にだけある SKU の出品 (fixture の sheet_only1 = 夜間ロードが Sheet から a005 を作った) の構成の行の数 */
const sheetComps = async (db) => Number((await db.query(`select count(*)::int as n from core.listing_components c join core.listings l on l.listing_id = c.listing_id
  where l.mall = 'amazon' and l.listing_code = 'sheet_only1'`)).rows[0].n);
const CLEAN_MASTERS = [['pr_a001', 'SKU マスタの 1', T1, T2], ['pr_pack2', 'SKU マスタの 2 個組', T1, T1], ['a003', '単品 3 を FBA でも', T1, T2], ['pr_new1', '新しい組 (出品なし)', T1, T1]];
const CLEAN_COMPS = [['pr_a001', 'a001', 1, 0, T1, T1], ['pr_pack2', 'a001', 2, 0, T1, T1], ['pr_pack2', 'a002', 1, 1, T1, T2], ['a003', 'a003', 2, 0, T1, T1],
  ['pr_new1', 'a005', 1, 0, T1, T1], ['pr_new1', 'a006', 3, 1, T2, T2]];

await ta('[11] 影運転: 止める項目を数える (無い NE コード・並びの隙間・構成なし・正規化で重なる seller SKU・大文字・形の違う時刻・親の無い構成)・Sheet だけ = 気をつける (PR-D)・移せた SKU のハッシュが一致・必ず巻き戻す・古い表は変えない', async () => {
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
    { not_in_company: 1, sort_gap: 1, no_components: 1, seller_sku_collision: 2, timestamp: 1, sheet_only_has_components: 1 });
  assert.equal(r.blocker_total, 7);
  // #1651: Sheet にだけある SKU の出品に構成がある = 止める項目 (例 = seller SKU・SKU・出どころ)・何も消さない (下の snapshot が同じ)
  assert.deepEqual(r.blockers.sheet_only_has_components.samples, [{ seller_sku: 'sheet_only1', sku: 'a005', qty: 1, resolution: 'imported', source: 'fba_sheet' }]);
  // PR-D (10/8 中原さん「スプレッドシートは使用していないので無視」): Sheet にだけある SKU = 気をつける項目 (数・例は出す)
  assert.deepEqual(r.warnings.sheet_only, { count: 1, samples: [{ seller_sku: 'sheet_only1' }] });
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

await ta('[11] PR-D: Sheet にだけある SKU だけ (ほかは片付いた古い表) = 影運転は止める項目 0・ハッシュ一致 (気をつける項目に数と例)・巻き戻す', async () => {
  const E2 = await setupDb();
  await load(E2.db);
  const file = path.join(tmp, 'legacy-shadow-sheet-only.db');
  makeLegacy(file, CLEAN_MASTERS, CLEAN_COMPS);
  const legacy = M.readLegacyAmazonMaps(file);
  const snap0 = await snapshot(E2.db);
  const sheetOnly = ['pr_1272115_f_20231217_19336813_0004', 'sheet_only2'];
  const r = await M.runAmazonMapMigration(E2.db, legacy, { mode: 'shadow', sheetOnly });
  assert.equal(r.committed, false);
  assert.deepEqual(r.blockers, {});
  assert.equal(r.blocker_total, 0); assert.equal(r.blocked_skus, 0);
  assert.equal(r.subset.skus, 4); assert.equal(r.subset.match, true, JSON.stringify(r.subset));
  assert.equal(r.legacy_digest.content_hash, r.subset.company.content_hash);
  assert.deepEqual(r.warnings.sheet_only, { count: 2, samples: sheetOnly.map((s) => ({ seller_sku: s })) });
  assert.deepEqual(await snapshot(E2.db), snap0);
  // 一覧を渡さない = 今までどおり断る (0 件と読まない・Codex #1586 R1 M3)
  await assert.rejects(() => M.runAmazonMapMigration(E2.db, legacy, { mode: 'shadow' }), (e) => e.code === 'AMAZON_MAP_MIGRATE_INVALID');
  // #1651: Sheet にだけある SKU の出品に構成が 1 行でもある = 止める項目 (出どころによらない = FBM の完全一致・manual でも)・何も消さない
  const r2 = await M.runAmazonMapMigration(E2.db, legacy, { mode: 'shadow', sheetOnly: ['sheet_only1'] });
  assert.deepEqual(r2.blockers, { sheet_only_has_components: { count: 1, samples: [{ seller_sku: 'sheet_only1', sku: 'a005', qty: 1, resolution: 'imported', source: 'fba_sheet' }] } });
  assert.equal(r2.blocked_skus, 0); assert.equal(r2.subset.match, true);
  const lid = "(select listing_id from core.listings where mall = 'amazon' and listing_code = 'sheet_only1')";
  for (const [resolution, ev] of [['exact', '{"source":"fbm_ne_code","seller_sku":"sheet_only1"}'], ['manual', '{"source":"fba_sheet"}'], ['imported', '{"source":"somewhere"}']]) {
    await E2.db.query(`update core.listing_components set resolution = $1, evidence = $2::jsonb where listing_id = ${lid}`, [resolution, ev]);
    const r3 = await M.runAmazonMapMigration(E2.db, legacy, { mode: 'shadow', sheetOnly: ['sheet_only1'] });
    assert.deepEqual(r3.blockers.sheet_only_has_components.samples.map((x) => [x.resolution, x.source]), [[resolution, JSON.parse(ev).source]], ev);
    assert.equal(await sheetComps(E2.db), 1, ev);   // 消さない
  }
});

await ta('[11] apply (切替の日 ③): frozen だけ・H0 と同じ・同じ行は時刻だけ (記録・出品の version を増やさない)・FBM の自動の行は消える・作り直したハッシュ = H0・移行の後の夜間ロード 2 回で構成が変わらない', async () => {
  const E3 = await setupDb();
  await createRoles(E3.pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(E3.pg, {});
  await W2.useReal0058(E3.pg);   // 広げる道 PR-2: 本物の 0058 (PR-1) の上 (許可なし = 本番の今)
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
  // PR-D: Sheet にだけある SKU があっても、ほかの止める項目 (構成なし) は今までどおり止める。止める項目の一覧に sheet_only は出ない
  //   (構成の無い Sheet にだけある SKU = 今の本番の形 = 止める項目には出ない)
  await assert.rejects(() => M.runAmazonMapMigration(E3.db, legacyBad, { mode: 'apply', expectHash: M.legacyDigest(legacyBad).content_hash, sheetOnly: ['pr_1272115_F_20231217_19336813_0004'] }),
    (e) => e.code === 'AMAZON_MAP_MIGRATE_BLOCKED' && e.blockers.no_components?.count === 1 && Object.keys(e.blockers).length === 1 && !/sheet_only/.test(e.message));
  assert.equal(Number((await E3.db.query('select count(*)::int as n from core.amazon_sku_maps')).rows[0].n), 0);
  // #1651: Sheet にだけある SKU の出品 (fixture の sheet_only1) に構成がある = apply も止める (何も書かない・何も消さない)
  await assert.rejects(() => M.runAmazonMapMigration(E3.db, legacy, { mode: 'apply', expectHash: h0, actor: 'naka@test', sheetOnly: ['sheet_only1'] }),
    (e) => e.code === 'AMAZON_MAP_MIGRATE_BLOCKED' && e.blockers.sheet_only_has_components?.count === 1 && /sheet_only_has_components 1/.test(e.message));
  assert.equal(Number((await E3.db.query('select count(*)::int as n from core.amazon_sku_maps')).rows[0].n), 0);
  assert.equal(await sheetComps(E3.db), 1);
  // 本物 (今の本番の形 = Sheet にだけある SKU の出品に構成が無い)
  const lidPr = (await E3.db.query("select listing_id::text as id, version::text as v from core.listings where listing_code = 'pr_a001'")).rows[0];
  const evBefore = Number((await E3.db.query('select coalesce(max(event_id), 0)::int as n from events.master_change_events')).rows[0].n);
  // PR-D: Sheet にだけある SKU があっても apply は止まらない (気をつける項目に数と例)
  const r = await M.runAmazonMapMigration(E3.db, legacy, { mode: 'apply', expectHash: h0, actor: 'naka@test', sheetOnly: ['pr_1272115_f_20231217_19336813_0004'] });
  assert.equal(r.committed, true); assert.equal(r.subset.match, true); assert.equal(r.subset.company.content_hash, h0);
  assert.equal(r.blocker_total, 0); assert.deepEqual(r.blockers, {});
  assert.deepEqual(r.warnings.sheet_only, { count: 1, samples: [{ seller_sku: 'pr_1272115_f_20231217_19336813_0004' }] });
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
    // #1651: 持ち主 load の夜間ロードは Sheet にだけある SKU の出品に構成を作る (ハッシュは対応のある出品しか見ない = 気づかない)
    //   = widen の check / beforeCall が照らして断る分 (本物の PG: test-master-widen-amazon-pg [RC4])
    assert.equal(await sheetComps(E3.db), 1, run);
    assert.equal((await M.sheetOnlyComponents(E3.db, ['sheet_only1'])).length, 1, run);
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
// 持ち主表 = DB の active (広げる道 PR-2)
// 手数料の写し (fees) の区分 = 本物と同じ形 (取得の時刻つき・#1691 Codex R4: 未登録は 7 日より古い区分を使わない)。時計 (2030-01-10) の前の日に取った
const FEES_NOW = (await import('../apps/master-edit/amazon-read.mjs')).channelsFromFeeRows([['pr_a001', 'FBA'], ['a003', 'FBM'], ['zz_sold', 'FBA'], ['zz_fbm', 'FBM']].map(([s, c]) => ({ seller_sku: s, fulfillment_channel: c, fetched_at: '2030-01-09 12:00:00' })));
__setAmazonChannelsProvider(async () => FEES_NOW);
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
/**
 * 画面の JS が文法として読めること・EJS の出力が JS に混ざっていないこと。
 * マスタの入力の画面は JS を public/ のファイルに分けた (第 2 段 10/5) = <script src> は中身を HTTP で取ってきて同じに確かめ、返す (インラインの後ろに並べる)
 */
async function checkScripts(html, expected) {
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  assert.equal([...html.matchAll(/<script\b/gi)].length, all.length, 'script の開きと閉じの数が合わない');
  for (const m of all.filter((x) => /type="application\/json"/.test(x[1]))) JSON.parse(m[2]);   // 画面の JS に渡す値
  const scripts = all.filter((m) => !/\bsrc=/.test(m[1]) && !/type="application\/json"/.test(m[1])).map((m) => m[2]);
  if (expected != null) assert.equal(scripts.length, expected, `<script> の数 ${scripts.length}`);
  const files = [];
  for (const m of all.filter((x) => /\bsrc="\/apps\/master-edit\/public\//.test(x[1]))) {
    const src = /\bsrc="([^"]+)"/.exec(m[1])[1];
    const r = await fetch(ORIGIN + src.replace(/&amp;/g, '&'), { headers: { 'x-test-session': 'editor' } });
    assert.equal(r.status, 200, src);
    files.push(await r.text());
  }
  for (const x of [...scripts, ...files]) { new vm.Script(x); assert.ok(!/<%|%>/.test(x), 'EJS のタグが JS に残っている'); }
  return [...scripts, ...files];
}
const decode = (s) => s.replace(/&#34;/g, '"').replace(/&quot;/g, '"').replace(/&amp;/g, '&');

await ta('[12] 一覧・1 つ・変更の記録・つかいかた・末尾の / (描画と画面の JS)', async () => {
  const bare = await fetch(`${BASE}/amazon`, { headers: { 'x-test-session': 'editor' }, redirect: 'manual' });
  assert.equal(bare.status, 301); assert.equal(bare.headers.get('location'), '/apps/master-edit/amazon/');
  let r = await call('GET', '/amazon/');
  assert.equal(r.status, 200); await checkScripts(r.text, 0);
  assert.ok(r.text.includes('sku?sku=pr_a001') && r.text.includes('sku?sku=a004') && r.text.includes('削除済み (墓標)'));
  assert.ok(!r.text.includes('いまは保存できません'));
  r = await call('GET', '/amazon/?state=deleted');
  assert.ok(r.text.includes('sku?sku=new_sku1') && !r.text.includes('sku?sku=a004"'));
  r = await call('GET', '/amazon/?q=a006');   // NE コードでも探せる (pr_new1 は無い = この DB では無し)
  assert.equal(r.status, 200);
  // CSV (10/16): 一覧の右上のリンク (絞り込みのまま)・BOM・見出し・全件・状態で絞る・上限を超える = 413
  r = await call('GET', '/amazon/?state=active');
  assert.ok(r.text.includes('href="list.csv?state=active"'), 'CSV のリンクが絞り込みを持っていない');
  r = await call('GET', '/amazon/list.csv');
  assert.equal(r.status, 200);
  assert.match(r.headers.get('content-type'), /^text\/csv/);
  assert.match(r.headers.get('content-disposition'), /attachment; filename="amazon-sku_\d{8}-\d{4}\.csv"/);
  assert.ok(r.text.startsWith('"seller SKU","名前","構成 (NE の商品 × 数)"'));   // r.text = fetch の text() は BOM を外す
  const raw = Buffer.from(await (await fetch(`${BASE}/amazon/list.csv`, { headers: { 'x-test-session': 'editor' } })).arrayBuffer());
  assert.deepEqual([...raw.subarray(0, 3)], [0xef, 0xbb, 0xbf], 'BOM が無い (Excel で文字化けする)');
  const csvAll = r.text.split('\r\n').filter(Boolean);
  assert.ok(csvAll.some((l) => l.startsWith('"pr_a001",')) && csvAll.some((l) => l.startsWith('"new_sku1",') && l.includes('"削除済み (墓標)"')));
  const nMaps = Number((await q('select count(*)::int as n from core.amazon_sku_maps'))[0].n);
  assert.equal(csvAll.length - 1, nMaps, 'CSV が全件でない');
  r = await call('GET', '/amazon/list.csv?state=deleted');
  assert.ok(r.text.includes('"new_sku1",') && !r.text.includes('"pr_a001",'));
  __setExportLimits({ max: 1 });
  try {
    r = await call('GET', '/amazon/list.csv');
    assert.equal(r.status, 413); assert.ok(r.text.includes('絞ってから'));
    r = await call('GET', '/amazon/');
    assert.ok(r.text.includes('id="csv-link" aria-disabled="true"'));
  } finally { __setExportLimits(null); }
  // 数えた後に増えた (Codex R1 P2) = 読むのも max + 1 件で切って tooMany (数えた件数が 0 でも上限を超えて出さない)
  const { listAmazonMapsAll } = await import('../apps/master-edit/amazon-read.mjs');
  const staleCount = { query: (t, p) => (/^select count\(\*\)::int as n from core\.amazon_sku_maps/.test(t) ? { rows: [{ n: 0 }] } : db.query(t, p)) };
  const raced = await listAmazonMapsAll(staleCount, {}, { max: 1 });
  assert.equal(raced.tooMany, true); assert.equal(raced.atLeast, true); assert.equal(raced.rows.length, 0);
  r = await call('GET', '/amazon/sku?sku=PR_A001');
  assert.equal(r.status, 200);
  const amzJs = (await checkScripts(r.text, 0)).find((x) => x.includes('me-amazon.js — Amazon SKU の対応')) || '';
  for (const api of ["'/api/amazon/save'", "'/api/amazon/delete'", "'/api/lookup?code='"]) assert.ok(amzJs.includes(api), `画面が ${api} を呼んでいない`);
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
  assert.ok(idx.text.includes('href="/apps/master-edit/amazon/"'));
});

await ta('[12] 画面の属性のエスケープ (#1628 Codex R1 M4): 構成品のコードに引用符・< があっても属性が増えない (並べ替え・外すのボタンの読み上げの名前)', async () => {
  const ejs = (await import('ejs')).default;
  const { ui } = await import('../apps/master-edit/ui-format.mjs');
  const evil = 'x" onfocus="alert(1)" a="<b>';
  const html = await ejs.renderFile(path.join(path.dirname(fileURLToPath(import.meta.url)), '../apps/master-edit/views/amazon-sku.ejs'), {
    title: 't', username: 'u', displayName: 'u', canEdit: true, gateMessage: '', base: '/apps/master-edit', ui, assetV: 'v', ui2: true, nav: 'amazon', nowMs: Date.now(),
    amazonClosed: false, amazonClosedWhy: '', dbError: null, sku: 'pr_x', MAP_STATES: { active: '有効', deleted: '削除済み (墓標)' }, CHANNELS: { FBA: 'FBA', FBM: 'FBM' }, MAX_MAP_COMPONENTS: 20, MAX_MAP_QTY: 999,
    page: { channel: 'FBA', related: [], lastRequest: null, phase: null, cur: { listing: { listing_id: '1', listing_code: 'pr_x', title: 'T', status: 'active', asin: null }, map: null, fnsku: null, versions: {},
      components: [{ code: evil, name: 'N', qty: 1, sku_kind: 'single', reg_state: 'available', resolution: 'manual' }] } },
  });
  assert.ok(!/onfocus="/.test(html), '引用符で属性を抜けない (onfocus という属性ができない)');
  assert.ok(!html.includes('a="<b>'), '< をそのまま出さない');
  assert.ok(html.includes('x&quot; onfocus=&quot;alert(1)&quot; a=&quot;&lt;b&gt;'), 'エスケープして出す');
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
  // 変更の記録: どの構成品を足した・外したかを商品コードで出す (#1628 Codex R2 M3)
  const hh = await call('GET', '/amazon/sku/history?sku=pr_pack2');
  assert.equal(hh.status, 200);
  assert.match(hh.text, /構成 a002 を足した/, '足した構成品のコード: ' + [...hh.text.matchAll(/<span class="what">([^<]*)<\/span>/g)].map((m) => m[1]).join(' | '));
  assert.match(hh.text, /構成 a001 を外した/, '外した構成品のコード');
  process.env.MASTER_EDIT_OPEN = '0';
  r = await call('POST', '/api/amazon/save', { body: { request_id: uuid(), seller_sku: 'pr_pack2', name: '2 個組', components: [{ code: 'a001', qty: 2 }], seen: { versions: await versionsOf('pr_pack2') } } });
  assert.equal(r.status, 409); assert.equal(r.j.reason, 'before_cutover');
  const page = await call('GET', '/amazon/sku?sku=pr_pack2');
  assert.ok(page.text.includes('いまは保存できません'));
  process.env.MASTER_EDIT_OPEN = '1';
  await W2.setActiveMapOnly(pg, { ...ALL_COMPANY, 'listing_components.amazon': 'load' });   // 広げる道 PR-2: DB の active が段階の記録と違う = 閉じている (帯)
  try {
    const page2 = await call('GET', '/amazon/');
    assert.ok(page2.text.includes('いまは保存できません'));
  } finally { await W2.setActiveMapOnly(pg, ALL_COMPANY); }
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
  assert.equal(r.status, 200); await checkScripts(r.text, 0);
  assert.ok(r.text.includes('未判定'));   // 売上の日次が公開されていない
  assert.ok(r.text.includes('zz_sold') && !r.text.includes('zz_fbm') && !r.text.includes('zz_old'));
  // 🚨 10/12 中原さん: 自社発送 (FBM) は出さない (商品コード = Amazon SKU = 対応が要らない)。区分の引数 (?channel=) は捨てる = どの引数でも FBA だけ
  for (const ch of ['FBM', 'all', 'fbm']) {
    r = await call('GET', '/amazon/unmapped?channel=' + ch);
    assert.ok(r.text.includes('zz_sold') && !r.text.includes('zz_fbm'), ch);
  }
  assert.ok(!/\?channel=/.test(r.text) && !r.text.includes('chip-lab">区分'), '区分の切り替えが無い');
  // 区分が分からない SKU (墓標の pr_pack2 = 手数料の写しにも出品一覧にも無い) は一覧と件数に入れず、下の一言で出す (黙って隠さない)
  assert.ok(r.text.includes('id="unmapped-unknown"') && r.text.includes('sku?sku=pr_pack2'));
  assert.ok(r.text.includes('<b style="color:var(--text)">1</b> 件 (FBA)'), '件数も FBA だけ');
  {
    const page = await call('GET', '/amazon/');
    assert.ok(page.text.includes('売れたのに対応が無い SKU (FBA) — 1 件') && page.text.includes('href="unmapped">全部見る') && !page.text.includes('zz_fbm'));
  }
  // 区分の出どころ (#1691 Codex R4): 出品一覧 (0074・36 時間以内) が先・無い / 古い = fees (7 日以内)・どちらも古い・無い = 分からない・食い違い = 分からない
  const RU = await import('../apps/master-edit/amazon-read.mjs');
  const I = await import('../apps/company-db/ingest/amazon-listings.mjs');
  const NOW_MS = Date.parse('2030-01-10T03:00:00Z');
  const um = (channels, now = new Date(NOW_MS)) => asRole(E, 'master_edit', () => RU.amazonUnmapped(db, { now, channels }));
  const feeMap = (list, at) => RU.channelsFromFeeRows(list.map(([s, c]) => ({ seller_sku: s, fulfillment_channel: c, fetched_at: at })));
  const report = async (list, genIso, run) => {
    const rowsR = list.map(([s, f]) => I.listingRowOf({ seller_sku: s, item_name: s, asin: null, open_date: null, open_date_raw: null, fulfillment: f, channel_raw: f === 'FBM' ? 'DEFAULT' : 'AMAZON_JP', status: 'active', status_raw: 'Active', quantity: null, price: null }));
    const gen = Date.parse(genIso);
    const r0 = await I.ingestAmazonListings(db, { shop_code: I.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: run, checksum: I.listingChecksum(rowsR), row_count: rowsR.length, skipped_count: 0, rows: rowsR, force: true }, { nowMs: NOW_MS });
    assert.ok(['applied', 'refreshed'].includes(r0.status), JSON.stringify(r0));
  };
  // fees の時刻の読み方・正規化の衝突 (大文字小文字だけ違う SKU の区分が違う = 入れない・後勝ちにしない)
  assert.equal(RU.feesFetchedMs('2030-01-09 12:00:00'), Date.parse('2030-01-09T12:00:00Z'));
  assert.equal(RU.feesFetchedMs('きのう'), null);
  const fc = RU.channelsFromFeeRows([{ seller_sku: 'zz_sold', fulfillment_channel: 'FBA', fetched_at: '2030-01-09 00:00:00' }, { seller_sku: 'ZZ_SOLD', fulfillment_channel: 'FBM', fetched_at: '2030-01-09 01:00:00' },
    { seller_sku: 'zz_fbm', fulfillment_channel: 'FBM', fetched_at: '2030-01-08 00:00:00' }, { seller_sku: 'ZZ_FBM', fulfillment_channel: 'FBM', fetched_at: '2030-01-09 00:00:00' }]);
  assert.deepEqual([fc.get('zz_sold'), fc.conflicts.has('zz_sold'), fc.get('zz_fbm'), fc.at.get('zz_fbm')], [undefined, true, 'FBM', Date.parse('2030-01-09T00:00:00Z')]);
  let d = await um(fc);
  assert.deepEqual([d.decidable, d.rows.map((x) => x.code), d.unknown.map((x) => x.code).sort()], [true, [], ['pr_pack2', 'zz_sold']], 'fees の正規化の衝突 = 分からない (FBA とも FBM とも言わない)');
  // 両方が古い・無い = 区分を読めない (件数も「読めない」・0 件と言わない)
  __setAmazonChannelsProvider(async () => feeMap([['zz_sold', 'FBA'], ['zz_fbm', 'FBM']], '2029-12-01 00:00:00'));
  try {
    d = await um(feeMap([['zz_sold', 'FBA'], ['zz_fbm', 'FBM']], '2029-12-01 00:00:00'));
    assert.deepEqual([d.decidable, d.rows.length, d.unknown.length], [false, 0, 3], '古い fees は使わない');
    r = await call('GET', '/amazon/unmapped');
    assert.ok(r.text.includes('id="unmapped-undecidable"') && !r.text.includes('zz_sold'));
    assert.ok(r.text.includes('件数は読めない') && !/>0<\/b> 件 \(FBA\)/.test(r.text), '件数欄も「読めない」(0 件と言わない)');
    const page = await call('GET', '/amazon/');
    assert.ok(page.text.includes('売れたのに対応が無い SKU (FBA) — 区分を読めない') && !page.text.includes('(FBA) — 0 件'));
    __setAmazonChannelsProvider(async () => null);
    r = await call('GET', '/amazon/unmapped');
    assert.ok(r.text.includes('件数は読めない') && r.text.includes('読めないので出せません'), 'fees が読めない・出品一覧も無い');
    // 候補が 0 件 = 区分を読めなくても 0 件と確定 (R4 L1)
    d = await um(null, new Date('2031-06-01T03:00:00Z'));
    assert.deepEqual([d.decidable, d.rows.length, d.unknown.length], [true, 0, 0]);
    // 古い fees (zz_sold = FBM のまま) + 新しい出品一覧 (zz_sold = FBA) = 出品一覧が勝つ
    await report([['zz_sold', 'FBA'], ['zz_fbm', 'FBM']], '2030-01-10T00:00:00Z', 'r_um1');
    d = await um(feeMap([['zz_sold', 'FBM'], ['zz_fbm', 'FBA']], '2029-12-01 00:00:00'));
    assert.deepEqual([d.decidable, d.rows.map((x) => [x.code, x.channel_from]), d.unknown.map((x) => x.code)], [true, [['zz_sold', 'report']], ['pr_pack2']], '新しい出品一覧が古い fees に勝つ');
    // 新しい fees と新しい出品一覧が違う = 出品一覧が先 (毎日全件の今の姿)
    d = await um(feeMap([['zz_sold', 'FBM']], '2030-01-09 12:00:00'));
    assert.deepEqual(d.rows.map((x) => x.code), ['zz_sold']);
    // 出品一覧が古い (36 時間より前) = 新しい fees で決める・fees も無い SKU = 分からない
    d = await um(feeMap([['zz_sold', 'FBM'], ['zz_fbm', 'FBA']], '2030-01-09 12:00:00'), new Date(NOW_MS + 40 * 3600e3));
    assert.deepEqual([d.decidable, d.rows.map((x) => [x.code, x.channel_from])], [true, [['zz_fbm', 'fees']]], '古い出品一覧は使わない');
    // 出品一覧の中で大文字小文字だけ違う SKU の区分が違う = 分からない (fees にも回さない)
    await report([['zz_sold', 'FBA'], ['ZZ_SOLD', 'FBM'], ['zz_fbm', 'FBM']], '2030-01-10T01:00:00Z', 'r_um2');
    d = await um(feeMap([['zz_sold', 'FBA']], '2030-01-09 12:00:00'));
    assert.deepEqual([d.rows.map((x) => x.code), d.unknown.map((x) => [x.code, x.channel_from]).sort()], [[], [['pr_pack2', 'none'], ['zz_sold', 'conflict']]]);
  } finally { __setAmazonChannelsProvider(async () => FEES_NOW); }
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

console.log('\n自分で作る入口 (10/11: まだ売れていない seller SKU の対応を先に作る)');
const RD = await import('../apps/master-edit/amazon-read.mjs');
const newCheckAsEdit = (sku) => asRole(E, 'master_edit', () => A.newSellerSkuCheckInDb(db, sku));
const asEditRole = (fn) => asRole(E, 'master_edit', fn);

await ta('[13] 似ているか (打ち間違いの守り・止めない): 記号だけ違う・1〜2 文字違い (入れ替え・抜け)・連番の数字だけの違いは出さない・短い SKU は 1 文字だけ / 気をつけること (40 文字・全角・途中の空白)', async () => {
  assert.equal(RD.editDistance('kitten', 'sitting', 5), 3);
  assert.equal(RD.editDistance('pr_abc001', 'pr_acb001'), 1, '隣どうしの入れ替え = 1');
  assert.equal(RD.editDistance('abcdef', 'uvwxyz', 2), 3, 'max を超えたら max + 1');
  assert.deepEqual(RD.similarKind('pr_abc001', 'pr-abc001'), { kind: 'symbols', distance: 1 });
  assert.deepEqual(RD.similarKind('pr-abc-001', 'prabc001'), { kind: 'symbols', distance: 2 });
  assert.deepEqual(RD.similarKind('pr_abc001', 'pr_abc01'), { kind: 'typo', distance: 1 }, '0 が 1 つ抜けた');
  assert.deepEqual(RD.similarKind('pr_abc001', 'pr_acb001'), { kind: 'typo', distance: 1 }, '入れ替え');
  assert.deepEqual(RD.similarKind('pr_abc001', 'pr_abd002'), { kind: 'typo', distance: 2 }, '字の違いがあれば数字の違いも数える');
  assert.equal(RD.similarKind('pr_abc001', 'pr_abc002'), null, '連番 (同じ長さで数字だけ違う) は出さない');
  assert.equal(RD.similarKind('pr_abc019', 'pr_abc091'), null, '数字どうしの入れ替えも連番と見分けられない = 出さない');
  assert.equal(RD.similarKind('pr_abc001', 'pr_xyz001'), null, '3 文字違い');
  assert.equal(RD.similarKind('pr_abc001', 'pr_abc001'), null, '同じ = 似ているではない (もうある)');
  assert.deepEqual(RD.similarKind('a001', 'a01'), { kind: 'typo', distance: 1 });
  assert.equal(RD.similarKind('abcd', 'abxy'), null, '5 文字以下は 1 文字違いだけ');
  assert.deepEqual(RD.similarKind('prabc001', 'p-r-a-b-c-001'), { kind: 'symbols', distance: 5 }, '記号が 3 個以上違っても記号だけの違い');
  assert.deepEqual(RD.sellerSkuWarnings('pr_abc001'), []);
  assert.match(RD.sellerSkuWarnings('はちみつ').join(' '), /日本語など/);
  // 先に作るときの決まり (#1685 Codex R1 High・lib の newSellerSkuProblems = 確かめと保存で同じ)
  assert.deepEqual(A.newSellerSkuProblems('pr_abc001', 'pr_abc001'), []);
  assert.deepEqual(A.newSellerSkuProblems('a'.repeat(40), 'a'.repeat(40)), [], '40 文字ちょうどは作れる');
  assert.match(A.newSellerSkuProblems('a'.repeat(41), 'a'.repeat(41)).join(' '), /40 文字までです \(いま 41 文字\)/);
  assert.match(A.newSellerSkuProblems('ｐｒ＿ａ001', 'pr_a001').join(' '), /全角の英数字・記号か、Unicode のダッシュ[\s\S]*半角にそろえると pr_a001/);
  assert.match(A.newSellerSkuProblems('pr abc', 'prabc').join(' '), /途中に空白/);
  assert.deepEqual(A.newSellerSkuProblems('はちみつ', 'はちみつ'), [], '日本語は止めない (正規化しても同じ)');
});

await ta('[13] 確かめ (画面のロール・読むだけ): 形の誤り・もうある (有効 / 墓標 / 正規化で同じ)・出品だけある・似た SKU (近い順)・新しく作れる。何も書かない', async () => {
  const before = await q('select (select count(*)::int from core.listings) as l, (select count(*)::int from core.amazon_sku_maps) as m, (select count(*)::int from ops.master_edit_requests) as r');
  let c = await asEditRole(() => RD.checkNewAmazonSku(db, ''));
  assert.deepEqual([c.ok, c.reason, c.message], [false, 'shape', 'seller SKU を入れてください']);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, `a${String.fromCharCode(9)}b`));
  assert.equal(c.ok, false); assert.match(c.message, /seller SKU が使えない形です \(制御文字を含む\)/);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'a'.repeat(256)));
  assert.equal(c.ok, false); assert.match(c.message, /255 文字を超える/);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, ' PR_A001 '));
  assert.deepEqual({ next: c.next, sku: c.sku, open: c.open, kind: c.exists.kind, state: c.exists.state, same: c.exists.same, similar: c.similar }, { next: 'exists', sku: 'pr_a001', open: 'pr_a001', kind: 'map', state: 'active', same: true, similar: [] });
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'new_sku1'));
  assert.deepEqual([c.next, c.exists.state], ['exists', 'deleted'], '墓標も「もうある」');
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'ｐｒ＿ａ００１'));   // 全角 = 正規化すると pr_a001
  assert.deepEqual([c.next, c.sku, c.exists.seller_sku, c.exists.same], ['exists', 'ｐｒ＿ａ００１', 'pr_a001', false]);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'pr_a001' + String.fromCharCode(0x3000)));   // 後ろの全角の空白は sellerSkuIn が除く (今の決まりのまま)
  assert.deepEqual([c.ok, c.next, c.sku], [true, 'exists', 'pr_a001']);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'A006'));   // 夜間の取り込みの出品 (対応なし)
  assert.deepEqual({ next: c.next, open: c.open, kind: c.exists.kind, state: c.exists.state }, { next: 'create', open: 'a006', kind: 'listing', state: null });
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'pr_a01'));
  assert.equal(c.next, 'confirm');
  assert.deepEqual(c.similar[0], { seller_sku: 'pr_a001', name: (await q("select name from core.amazon_sku_maps where seller_sku = 'pr_a001'"))[0].name, state: 'active', asin: null, kind: 'typo', distance: 1 });
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'pr-a001x'));
  assert.equal(c.next, 'confirm'); assert.equal(c.similar[0].seller_sku, 'pr_a001'); assert.equal(c.similar[0].kind, 'typo');
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'new-sku1'));   // 記号だけ違う墓標
  assert.deepEqual(c.similar.map((s) => [s.seller_sku, s.kind, s.state]), [['new_sku1', 'symbols', 'deleted']]);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'pr_a002'));   // pr_a001 の連番 = 出さない
  assert.deepEqual([c.next, c.similar], ['create', []]);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, ' Brand_New_01 '));
  assert.deepEqual({ ok: c.ok, next: c.next, sku: c.sku, open: c.open, exists: c.exists, similar: c.similar, warnings: c.warnings }, { ok: true, next: 'create', sku: 'brand_new_01', open: 'brand_new_01', exists: null, similar: [], warnings: [] });
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'ブランド_01'));   // 日本語は正規化しても同じ = 止めない (気をつけること)
  assert.equal(c.next, 'confirm'); assert.match(c.warnings.join(' '), /日本語など/);
  // #1685 Codex R1 High: 出品の無い SKU を全角・Unicode のダッシュ・途中の空白・41 文字で先に作る = 止める (半角にそろえた形を案内)
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'ｂｒａｎｄ＿ｎｅｗ'));
  assert.deepEqual([c.ok, c.reason, c.canonical, c.next], [false, 'not_canonical', 'brand_new', null]);
  assert.match(c.message, /この形では先に作れません: 全角の英数字・記号か、Unicode のダッシュが入っています \(半角にそろえると brand_new\)/);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, `pr${String.fromCharCode(0x2212)}zz9`));
  assert.deepEqual([c.ok, c.reason, c.canonical], [false, 'not_canonical', 'pr-zz9']);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'pr zz 9'));
  assert.deepEqual([c.ok, c.canonical], [false, 'przz9']); assert.match(c.message, /途中に空白があります/);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'z'.repeat(41)));
  assert.deepEqual([c.ok, c.canonical], [false, null]); assert.match(c.message, /40 文字までです \(いま 41 文字\)/);
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'z'.repeat(40)));
  assert.equal(c.ok, true, '40 文字ちょうどは作れる');
  // #1685 Codex R1 Medium: 記号だけ違う候補は長さの差 (3 文字以上) によらず出す
  c = await asEditRole(() => RD.checkNewAmazonSku(db, 'n-e-w-s-k-u-1'));
  assert.deepEqual(c.similar.map((s) => [s.seller_sku, s.kind]), [['new_sku1', 'symbols']]);
  // #1685 Codex R1 Low: 長さの近い出品が上限より多い = 上限までと比べて incomplete (記号だけ違うものは別に全部)
  await pg.query('begin');
  try {
    await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) select 1, 'amazon', $1, 'zq_bulk_' || lpad(g::text, 5, '0'), 'active' from generate_series(1, $2::int) g`, [AMZ, RD.SIMILAR_SCAN_MAX + 1]);
    c = await asEditRole(() => RD.checkNewAmazonSku(db, 'zq_bulk_000011'));
    assert.equal(c.similar_incomplete, true);
    assert.ok(c.similar.some((s) => s.seller_sku === 'zq_bulk_00001'), JSON.stringify(c.similar));
    c = await asEditRole(() => RD.checkNewAmazonSku(db, 'z-q-b-u-l-k-0-0-0-0-1'));
    assert.ok(c.similar.some((s) => s.seller_sku === 'zq_bulk_00001' && s.kind === 'symbols'), '記号だけ違うものは長さの上限によらず');
    assert.equal(c.similar_symbols_incomplete, false);
    // #1685 Codex R2 Low: 記号だけ違う出品が上限 (50) より多い = symbols_incomplete・画面は「最大 50 件と比べました (もっとあります)」(「全部と比べた」と言わない)
    const SEPS = ['-', '_', '.', '/'];
    const variants = [];
    for (let i = 1; variants.length < RD.SIMILAR_SYMBOLS_MAX + 1; i++) {
      // 'zsym001' の 6 つの隙間に、i を 5 進で読んだ記号を入れる (0 = 何も入れない)。記号が 1 つ以上ある形だけ
      let x = i; let v = 'z';
      for (const ch of 'sym001') { const d = x % 5; x = Math.floor(x / 5); v += (d ? SEPS[d - 1] : '') + ch; }
      if (v !== 'zsym001') variants.push(v);
    }
    await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) select 1, 'amazon', $1, v, 'active' from unnest($2::text[]) as t(v)`, [AMZ, variants]);
    c = await asEditRole(() => RD.checkNewAmazonSku(db, 'zsym001'));
    assert.deepEqual([c.similar_symbols_incomplete, c.similar_incomplete, c.similar.length, c.similar.every((s) => s.kind === 'symbols')], [true, false, RD.SIMILAR_LIMIT, true]);
    const pageR = await call('GET', '/amazon/new?sku=zsym001');
    assert.ok(pageR.text.includes('id="sim-symbols-incomplete"') && pageR.text.includes('記号だけ違う出品は最大 50 件と比べました (もっとあります)'), '比べた範囲を正しく言う');
    assert.ok(!pageR.text.includes('全部と比べています'));
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  const after = await q('select (select count(*)::int from core.listings) as l, (select count(*)::int from core.amazon_sku_maps) as m, (select count(*)::int from ops.master_edit_requests) as r');
  assert.deepEqual(after, before, '確かめは何も書かない');
});

await ta('[13] 画面: 一覧の上の「新しい seller SKU の対応を作る」(名簿の人・保存を開いているときだけ)・/amazon/new = 形の誤り 400・もうある・もしかして → このまま作る・無ければ作る画面へ 303・確かめの API', async () => {
  let r = await call('GET', '/amazon/');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('id="h-create"') && r.text.includes('新しい seller SKU の対応を作る') && r.text.includes('action="/apps/master-edit/amazon/new"'));
  assert.ok(r.text.indexOf('id="create-sku"') < r.text.indexOf('id="list-search"'), '検索の欄より上');
  const js = (await checkScripts(r.text, 0)).find((x) => x.includes('me-amazon-new.js — 新しい seller SKU')) || '';
  assert.ok(js.includes("'/api/amazon/check?sku='") && js.includes("'/amazon/sku?sku='"), '画面の JS が確かめの API と作る画面を使う');
  r = await call('GET', '/amazon/', { session: 'viewer' });
  assert.equal(r.status, 200); assert.ok(!r.text.includes('id="create-sku"'), '名簿に無い人には出さない');
  process.env.MASTER_EDIT_OPEN = '0';
  try { r = await call('GET', '/amazon/'); assert.ok(!r.text.includes('id="create-sku"') && r.text.includes('いまは保存できません'), '保存を閉じているときは出さない'); } finally { process.env.MASTER_EDIT_OPEN = '1'; }
  // /amazon/new
  r = await call('GET', '/amazon/new');
  assert.equal(r.status, 200); await checkScripts(r.text, 0);
  assert.ok(r.text.includes('id="new-sku"') && !r.text.includes('id="new-result"'));
  r = await call('GET', '/amazon/new?sku=' + encodeURIComponent(' Brand_New_01 '));
  assert.equal(r.status, 303); assert.equal(r.headers.get('location'), '/apps/master-edit/amazon/sku?sku=brand_new_01');
  r = await call('GET', '/amazon/new?sku=A006');
  assert.equal(r.status, 303); assert.equal(r.headers.get('location'), '/apps/master-edit/amazon/sku?sku=a006', '出品だけある = 作る画面へ');
  r = await call('GET', '/amazon/new?sku=PR_A001');
  assert.equal(r.status, 200); await checkScripts(r.text, 0);
  assert.ok(r.text.includes('もうあります') && r.text.includes('href="/apps/master-edit/amazon/sku?sku=pr_a001"') && !r.text.includes('id="create-anyway"'));
  assert.ok(r.text.includes('id="new-sku" name="sku" value="PR_A001"'), '打った値を欄に残す');
  r = await call('GET', '/amazon/new?sku=new_sku1');
  assert.ok(r.text.includes('もうあります') && r.text.includes('開く (もう一度登録できます)'));
  r = await call('GET', '/amazon/new?sku=' + encodeURIComponent('ｐｒ＿ａ００１'));
  assert.ok(r.text.includes('もうあります') && /全角・ダッシュ・空白のちがいを除くと/.test(r.text));
  r = await call('GET', '/amazon/new?sku=' + encodeURIComponent(`a${String.fromCharCode(9)}b`));
  assert.equal(r.status, 400); assert.ok(r.text.includes('seller SKU が使えない形です') && r.text.includes('何も作っていません'));
  r = await call('GET', '/amazon/new?sku=' + 'x'.repeat(301));
  assert.equal(r.status, 400); assert.ok(r.text.includes('seller SKU が長すぎます'));
  r = await call('GET', '/amazon/new?sku=pr_a01');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('もしかして') && r.text.includes('href="/apps/master-edit/amazon/sku?sku=pr_a001"') && r.text.includes('1 文字違い'));
  assert.ok(r.text.includes('id="create-anyway"') && r.text.includes('href="/apps/master-edit/amazon/sku?sku=pr_a01" id="create-anyway"'), '打ち間違いでなければ、そのまま作れる');
  assert.ok(r.text.includes('data-raw="pr_a01" data-sku="pr_a01"'));
  r = await call('GET', '/amazon/new?sku=pr_a01&checked=pr_a01');
  assert.equal(r.status, 303); assert.equal(r.headers.get('location'), '/apps/master-edit/amazon/sku?sku=pr_a01', '画面でもう見た = 作る画面へ');
  r = await call('GET', '/amazon/new?sku=pr_a01&checked=pr_a99');
  assert.equal(r.status, 200, '別の値を見た印では通さない');
  r = await call('GET', '/amazon/new?sku=PR_A001&checked=pr_a001');
  assert.equal(r.status, 200, 'もうある SKU は checked でも作る画面へ送らない'); assert.ok(r.text.includes('もうあります'));
  // 名簿に無い人も確かめる・開くはできる (保存は作る画面の API が断る = 今までどおり)
  r = await call('GET', '/amazon/new?sku=pr_a01', { session: 'viewer' });
  assert.equal(r.status, 200); assert.ok(r.text.includes('保存できるのは名簿の人だけです'));
  // 確かめの API
  r = await call('GET', '/api/amazon/check?sku=pr_a01');
  assert.equal(r.status, 200); assert.equal(r.j.next, 'confirm'); assert.equal(r.j.similar[0].seller_sku, 'pr_a001');
  r = await call('GET', '/api/amazon/check?sku=' + encodeURIComponent('Brand_New_01'));
  assert.deepEqual([r.j.ok, r.j.next, r.j.sku], [true, 'create', 'brand_new_01']);
  r = await call('GET', '/api/amazon/check?sku=' + 'x'.repeat(301));
  assert.deepEqual([r.status, r.j.ok, r.j.reason], [200, false, 'shape']);
  r = await call('GET', '/api/amazon/check');
  assert.deepEqual([r.j.ok, r.j.message], [false, 'seller SKU を入れてください']);
  // #1685 Codex R1 High: 出品の無い SKU を全角などで先に作る = 確かめ・入口・作る画面・保存 (画面を飛ばした API) のどれでも断る
  const FW = 'ｂｒａｎｄ＿ｎｅｗ';
  r = await call('GET', '/api/amazon/check?sku=' + encodeURIComponent(FW));
  assert.deepEqual([r.status, r.j.ok, r.j.reason, r.j.canonical], [200, false, 'not_canonical', 'brand_new']);
  r = await call('GET', '/amazon/new?sku=' + encodeURIComponent(FW));
  assert.equal(r.status, 400); assert.ok(r.text.includes('この形では先に作れません') && r.text.includes('id="use-canonical" href="/apps/master-edit/amazon/new?sku=brand_new"'));
  r = await call('GET', '/amazon/new?sku=' + encodeURIComponent(FW) + '&checked=' + encodeURIComponent(FW));
  assert.equal(r.status, 400, 'checked でも通さない');
  r = await call('GET', '/amazon/sku?sku=' + encodeURIComponent(FW));
  assert.equal(r.status, 200); assert.ok(r.text.includes('id="new-sku-refused"') && !r.text.includes('id="save"'), '作る画面は先に知らせて保存のボタンを出さない');
  r = await call('GET', '/amazon/sku?sku=brand_new_99');
  assert.ok(!r.text.includes('id="new-sku-refused"') && r.text.includes('id="save"'), '半角なら作れる');
  for (const bad of [FW, 'brand new 2', `brand${String.fromCharCode(0x2010)}new3`, 'b'.repeat(41)]) {
    const sk = bad.trim().toLowerCase();
    const rr = await call('POST', '/api/amazon/save', { body: { request_id: uuid(), seller_sku: bad, name: '先に作る', components: [{ code: 'a002', qty: 1 }], seen: { versions: { listing: null, map: null } } } });
    assert.deepEqual([rr.status, rr.j.reason, rr.j.field], [400, 'invalid_input', 'seller_sku'], `${bad}: ${rr.text}`);
    assert.match(rr.j.error, /この形では先に作れません/);
    assert.equal(await lidOf(sk), null, `${bad}: 出品を作っていない`);
  }
  // 出品がもうある SKU (夜間の取り込みの表記) は今までどおり: 対応は出品の無い SKU の決まりで止めない
  assert.deepEqual((await newCheckAsEdit('a006')).problems, []);
});

await ta('[13] まだ売れていない・出品も無い seller SKU を作る画面から保存できる: 出品 (ASIN なし)・対応・構成 (manual) を作る → 最初の注文から結びつく・未登録に出ない → 後で Amazon に出品 = 夜間の取り込みが ASIN だけ付ける (構成・名前は変えない)', async () => {
  assert.equal(await lidOf('brand_new_01'), null, '始めは出品も無い');
  assert.equal((await q("select count(*)::int as n from core.order_lines where core.norm_code(unresolved_code) = 'brand_new_01'"))[0].n, 0, 'まだ売れていない');
  let r = await call('GET', '/amazon/sku?sku=brand_new_01');
  assert.equal(r.status, 200); await checkScripts(r.text, 0);
  assert.ok(r.text.includes('新しい対応') && r.text.includes('まだ売れていない') && r.text.includes('FBA / FBM 分からない') && r.text.includes('FBA / FBM はここでは選びません'));
  const versions = JSON.parse(decode(/data-versions="([^"]*)"/.exec(r.text)[1]));
  assert.deepEqual(versions, { listing: null, map: null });
  const rid = uuid();
  r = await call('POST', '/api/amazon/save', { body: { request_id: rid, seller_sku: 'brand_new_01', name: 'ブランド新 3 個組', components: [{ code: 'a002', qty: 3 }], reason: '売れる前に作る', seen: { versions } }, session: 'viewer' });
  assert.equal(r.status, 403, '保存できる人は今までどおり (名簿)');
  r = await call('POST', '/api/amazon/save', { body: { request_id: rid, seller_sku: 'brand_new_01', name: 'ブランド新 3 個組', components: [{ code: 'a002', qty: 3 }], reason: '売れる前に作る', seen: { versions } } });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.created, r.j.listing_created, r.j.state, r.j.seller_sku], [true, true, 'active', 'brand_new_01']);
  const lid = await lidOf('brand_new_01');
  assert.deepEqual((await q('select listing_code, title, status, catalog_item_id, created_by_type, created_by_id from core.listings where listing_id = $1', [lid]))[0],
    { listing_code: 'brand_new_01', title: 'ブランド新 3 個組', status: 'active', catalog_item_id: null, created_by_type: 'human', created_by_id: 'naka@test' });
  assert.deepEqual(await compsOfListing('brand_new_01'), [['a002', 3, 0, 'manual']]);
  // 最初の注文から結びつく (注文の受け口と同じ解き方) = 売上の日次で商品に分けられる
  assert.equal(String((await q("select core.resolve_listing_id(1::smallint, 'amazon', 'BRAND_NEW_01') as id"))[0].id), lid);
  const o = (await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
    values (1, 'amazon', 'amazon', 'o_brand_new', 'mall_api', '2030-01-10'::date, '2030-01-10'::date, 'shipped', false, 1, now(), 't', 'h') returning order_id`)).rows[0].order_id;
  await pg.query('insert into core.order_lines (company_id, order_id, line_key, listing_id, unresolved_code, qty, cancelled_qty, received_batch_seq) values (1, $1, $2, $3, null, 1, 0, 1)', [o, 'l1', lid]);
  assert.ok(!(await q("select code from ops.amazon_map_unmapped_recent('2030-01-10', 7)")).some((x) => x.code === 'brand_new_01'), '未登録に出ない');
  // もう一度入口で打つ = もうある
  r = await call('GET', '/amazon/new?sku=Brand_New_01');
  assert.ok(r.text.includes('もうあります') && r.text.includes('href="/apps/master-edit/amazon/sku?sku=brand_new_01"'));
  r = await call('GET', '/amazon/?q=brand_new');
  assert.ok(r.text.includes('sku?sku=brand_new_01') && r.text.includes('ブランド新 3 個組'));
  // 後で Amazon に出品 → 夜間の取り込み (SKU マスタの写しに違う構成・Amazon のタイトル・ASIN) = ASIN だけ付ける。構成・対応の名前は人が作ったまま
  const plan = makePlan();
  const AMAZON_RAW = 'BRAND_NEW_01';   // Amazon の本物の seller SKU (取り込みの表記 = 大文字)
  plan.listings.push({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode: AMAZON_RAW, title: 'Amazon のタイトル', status: 'active',
    components: [{ code: 'a001', qty: 1, sortOrder: 0, resolution: 'imported', evidence: { source: 'm_sku_master' } }], asinCandidates: [{ asin: 'B0NEW00001', source: 'listing_report' }], fnskuCandidates: [], evidenceSource: 'mirror_sku_master' });
  await load(db, ALL_COMPANY, plan, 'load_brand_new');
  const after = await A.readAmazonMap(db, 'brand_new_01');
  assert.deepEqual([after.listing.listing_id, after.listing.asin, after.map.name, after.map.state], [lid, 'B0NEW00001', 'ブランド新 3 個組', 'active']);
  assert.deepEqual(await compsOfListing('brand_new_01'), [['a002', 3, 0, 'manual']], '夜間の取り込みは対応のある出品の構成を変えない');
  // 写しの決まった形 (⑦-2 の古い表への写し) にも入る
  const canon = await A.readCompanyAmazonMapCanon(db, { sellerSkus: ['brand_new_01'] });
  assert.deepEqual([canon.master.map((m) => [m.seller_sku, m.name]), canon.components.map((x) => [x.ne_code, x.quantity])], [[['brand_new_01', 'ブランド新 3 個組']], [['a002', 3]]]);
  assert.deepEqual(K.validateSkuMap(canon), []);
  // 売上の集計 (rebuild-f-sales.js = LOWER(seller_sku) と写しの seller_sku の toLowerCase) と FBA 補充 (sheetless-mode.js の normSkuKey) の引き方で、本物の SKU が先に作った対応に当たる
  const { normSkuKey } = await import('../apps/fba-replenishment/sheetless-mode.js');
  const key = canon.master[0].seller_sku;
  assert.equal(AMAZON_RAW.toLowerCase(), key.toLowerCase(), '売上の集計の引き方');
  assert.equal(normSkuKey(AMAZON_RAW), normSkuKey(key), 'FBA 補充の引き方');
  assert.equal((await q('select listing_code from core.listings where listing_id = $1', [lid]))[0].listing_code, 'brand_new_01', '出品のコードは先に作った表記のまま (= 半角だから合う)');
  // 全角で先に作ろうとした = 断られる (上の試験) ので、Amazon の本物の出品が来てもずれた表記の対応は残らない
  assert.deepEqual(await q("select seller_sku from core.amazon_sku_maps where seller_sku <> core.norm_code(seller_sku)"), [], '正規化と違う表記の対応が無い');
});

console.log('\n構成の行の「NE の商品」の欄で探す (10/12)');
const R = await import('../apps/master-edit/amazon-read.mjs');
/** 登録の状態を試験のために変える (持ち主のロールの手の DML = 印が要る。[5] と同じ) */
const setReg = async (code, state) => {
  await pg.query(`select set_config('ops.registration_protocol', '1', false)`);
  try { await pg.query("update ops.master_registrations set state = $2, distribution_generation = case when $2 = 'distributable' then coalesce(distribution_generation, 'test-gen') else distribution_generation end where sku_id = (select sku_id from core.skus where code = $1)", [code, state]); }
  finally { await pg.query(`select set_config('ops.registration_protocol', '', false)`); }
};
const regOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = $1', [code]))[0]?.state ?? null;
const codesOf = (j) => j.items.map((x) => x.code);

await ta('[14] 探す API (画面のロール・読むだけ): コードの前方一致が上・部分一致も・全角 / 大文字も同じ・名前の言葉は全部を含む (かなの同一視)・JAN・2 文字から・20 件まで・長すぎ / 制御文字 = 400・名簿の外の人も見られる (読むだけ)', async () => {
  await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type)
    values (1, 'product', (select product_id from core.skus where code = 'a006'), 'jan', 'jan', '4901234567894', 'manual', 'human')`);
  const before = await snapshot(db);
  let r = await call('GET', '/api/amazon/components?q=a00');
  assert.equal(r.status, 200, r.text);
  assert.equal(r.headers.get('cache-control'), 'no-store');
  assert.deepEqual(codesOf(r.j), ['a001', 'a002', 'a003', 'a004', 'a005', 'a006', 'a007', 'a008', 'a00x'], '前方一致・コード順 (使えない例外の a00x は段の最後)');
  const a001 = r.j.items[0];
  assert.deepEqual([a001.name, a001.kind, a001.kind_label, a001.reg_state, a001.handling, a001.usable, a001.why], ['単品 1', 'single', '単品', 'available', 'active', true, null]);
  assert.equal(r.j.more, false); assert.equal(r.j.short, false);
  // ぴったり → 前方一致 → 部分一致 (コードの途中)
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('Ａ００１'));
  assert.equal(codesOf(r.j)[0], 'a001', '全角・大文字でもぴったりが一番上');
  r = await call('GET', '/api/amazon/components?q=et1');
  assert.deepEqual(codesOf(r.j), ['aset1'], 'コードの途中 (部分一致)');
  assert.equal(r.j.items[0].stock, null, 'セットは在庫を付けない (作れる数は構成品から)');
  // 名前の言葉 = 空白で区切った全部を含む (全角の空白でも)・かなの同一視 (ひらがな = カタカナ)
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('単品 FBM'));
  assert.deepEqual(codesOf(r.j), ['a003', 'a004']);
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('fbm' + FW_SPACE + '墓標'));
  assert.deepEqual(codesOf(r.j), ['a004']);
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('せっと'));
  assert.deepEqual(codesOf(r.j), ['aset1'], 'ひらがなでカタカナの名前に当たる');
  // 言葉ごとにコードに入っていてもよい (コード + 名前の言葉)
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('a00 墓標'));
  assert.deepEqual(codesOf(r.j), ['a004']);
  // JAN (ぴったり・上で a006 の商品に付けた)
  r = await call('GET', '/api/amazon/components?q=4901234567894');
  assert.deepEqual(codesOf(r.j), ['a006'], 'JAN で当たる');
  r = await call('GET', '/api/amazon/components?q=490123456789');
  assert.deepEqual(codesOf(r.j), [], 'JAN は途中では当てない (一覧の検索と同じ)');
  // 2 文字から・当たらない・LIKE の記号は文字として
  r = await call('GET', '/api/amazon/components?q=a');
  assert.deepEqual([r.status, r.j.items, r.j.short], [200, [], true]);
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('%%'));
  assert.deepEqual(codesOf(r.j), [], '% は何にでも当たる印にしない');
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('a_0'));
  assert.deepEqual(codesOf(r.j), [], '_ も文字として');
  r = await call('GET', '/api/amazon/components?q=' + 'x'.repeat(61));
  assert.equal(r.status, 400);
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent(`a0${String.fromCharCode(9)}`));
  assert.equal(r.status, 400);
  // 見られる人 = この画面と同じ (アプリの利用権)。読むだけ = 名簿の外の人も
  r = await call('GET', '/api/amazon/components?q=a00', { session: 'viewer' });
  assert.equal(r.status, 200);
  // 件数の上限 (20 件まで・more)
  const two = await asRole(E, 'master_edit', () => R.searchMapComponents(db, 'a0', { limit: 2 }));
  assert.deepEqual([two.items.length, two.more], [2, true]);
  const many = await asRole(E, 'master_edit', () => R.searchMapComponents(db, 'a0', { limit: 500 }));
  assert.ok(many.items.length <= R.COMPONENT_SEARCH_LIMIT, '上限より多くは返さない');
  // 在庫 (ロジザードの写し) = 単品・例外だけ (セットは null・写しに無い = 0・読めない = null)
  const st = { ok: true, asOf: '2030-01-10T03:00:00Z', map: new Map([['a001', 12]]) };
  const ws = await asRole(E, 'master_edit', () => R.searchMapComponents(db, 'a00', { stock: st }));
  assert.deepEqual(ws.items.slice(0, 2).map((x) => [x.code, x.stock]), [['a001', 12], ['a002', 0]]);
  assert.equal((await asRole(E, 'master_edit', () => R.searchMapComponents(db, 'aset', { stock: st }))).items[0].stock, null);
  // 言葉は全部見る (#1689 Codex R1 Low: 6 つ目より後を黙って捨てない)
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('a0 a0 a0 a0 a0 6'));
  assert.deepEqual(codesOf(r.j), ['a006'], '6 つ目の言葉も効く');
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('a0 a0 a0 a0 a0 a0 a0 zz'));
  assert.deepEqual(codesOf(r.j), [], '8 つ目の言葉で当たらなければ 0 件');
  const longQ = await asRole(E, 'master_edit', () => R.searchMapComponents(db, 'a'.repeat(R.COMPONENT_SEARCH_Q_MAX + 1)));
  assert.deepEqual([longQ.items, longQ.too_long], [[], true], '長すぎる語は切らずに探さない');
  assert.deepEqual(await snapshot(db), before, '何も書かない');
});

await ta('[14] 使えない商品 = 候補に出すが usable false と理由・同じ段の中では使える商品が上 / 決まりは保存と同じ (状態ごとに DB の関数が断る ⇔ 画面で選べない)・やめた商品はぴったりのときだけ', async () => {
  const keep = await regOf('a006');
  try {
    let r = await call('GET', '/api/amazon/components?q=aexc');
    assert.deepEqual([r.j.items[0].code, r.j.items[0].usable], ['aexc', false]);
    assert.match(r.j.items[0].why, /例外の SKU/);
    r = await call('GET', '/api/amazon/components?q=a00');
    assert.deepEqual(codesOf(r.j), ['a001', 'a002', 'a003', 'a004', 'a005', 'a006', 'a007', 'a008', 'a00x'], '使えない a00x (例外) は同じ段 (前方一致) の最後');
    assert.match(r.j.items.at(-1).why, /例外の SKU/);
    r = await call('GET', '/api/amazon/components?q=a00x');
    assert.deepEqual([codesOf(r.j)[0], r.j.items[0].usable], ['a00x', false], 'ぴったりは使えなくても一番上 (理由が分かる)');
    // 要確認 (NE で直接作った商品) は使える = 灰色にしない・同じ段で使える商品と並ぶ (中原さん 10/12 = B)
    await setReg('a002', 'quarantined');
    r = await call('GET', '/api/amazon/components?q=a00');
    assert.deepEqual(codesOf(r.j), ['a001', 'a002', 'a003', 'a004', 'a005', 'a006', 'a007', 'a008', 'a00x']);
    const q2 = r.j.items.find((x) => x.code === 'a002');
    assert.deepEqual([q2.usable, q2.ne_direct, q2.not_in_ne, q2.why], [true, true, false, null], '要確認 = 使える・「NE で直接作った」の印');
    await setReg('a002', 'available');
    // 状態ごとに: 画面の usable ⇔ 保存の DB の関数 (ops.amazon_map_components) が通す
    const id = await skuIdOf('a006');
    const STATES = ['draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available', 'quarantined', 'cancelled'];
    for (const s of STATES) {
      await setReg('a006', s);
      const j = (await call('GET', '/api/amazon/components?q=a006')).j;
      const it = j.items.find((x) => x.code === 'a006');
      assert.ok(it, `${s}: ぴったりなら出る`);
      const e = await errOf(db.query('select ops.amazon_map_components($1::jsonb) as r', [JSON.stringify([{ sku_id: id, code: 'a006', qty: 1 }])]));
      assert.equal(it.usable, !e, `${s}: 画面 ${it.usable} / DB ${e ? e.message : '通る'}`);
      assert.equal(it.usable, A.componentUnusable('single', s) === null, `${s}: lib と同じ`);
      if (!it.usable) assert.ok(it.why && it.why.length > 2, `${s}: 理由がある`);
      // 🆕 0073: 使える = 下書き・NE 登録待ち・NE 確認済み以降・要確認 / まだ NE に無い 2 つには「NE 未登録」・要確認には「NE で直接作った」の印
      assert.equal(it.usable, ['draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available', 'quarantined'].includes(s), `${s}: 使えるか`);
      assert.equal(it.ne_direct, s === 'quarantined', `${s}: NE で直接作った の印`);
      assert.equal(it.not_in_ne, ['draft', 'ne_pending'].includes(s), `${s}: NE 未登録の印`);
    }
    // やめた商品 = 前方一致・名前では出さない (一覧と同じ)・ぴったりなら理由つき
    r = await call('GET', '/api/amazon/components?q=a00');
    assert.ok(!codesOf(r.j).includes('a006'), 'やめた商品は前方一致では出さない');
    r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('単品 6'));
    assert.deepEqual(codesOf(r.j), [], '名前でも出さない');
    r = await call('GET', '/api/amazon/components?q=A006');
    assert.match(r.j.items[0].why, /登録をやめた/);
    // 状態の行が無い (切替の前の商品) = 使えない
    assert.match(R.componentUnusableWhy('single', null), /登録の状態が無い/);
    assert.equal(R.componentUnusableWhy('set', 'available'), null);
    assert.match(R.componentUnusableWhy('exception', 'available'), /例外の SKU/);
  } finally { await setReg('a006', keep); await setReg('a002', 'available'); }
  // 画面で選べる (usable) 候補は、保存で断られない (本物の保存を画面のロールで)
  const pick = (await call('GET', '/api/amazon/components?q=' + encodeURIComponent('単品 FBM'))).j.items.filter((x) => x.usable).map((x) => ({ code: x.code, qty: 1 }));
  const res = await save('pick_from_search', '探して選んだ', pick);
  assert.equal(res.state, 'active');
  assert.deepEqual(await compsOfListing('pick_from_search'), [['a003', 1, 0, 'manual'], ['a004', 1, 1, 'manual']]);
});

await ta('[14] コードは保存と同じ正規化だけ (#1689 Codex R1 M2): ﾊﾁ-01 と ハチ-01 は別の商品 = 探すとそれぞれ自分だけ・名前は NFKC (かな) で両方・2 つとも構成に入れて保存できる / 画面の JS のコードの比べ方 = normSku と同じ', async () => {
  const n = (await q("select core.norm_code('ﾊﾁ-01') as h, core.norm_code('ハチ-01') as z"))[0];
  assert.notEqual(n.h, n.z, '前提: DB は半角カナと全角カナを別にする');
  let r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('ﾊﾁ-01'));
  assert.deepEqual(codesOf(r.j), ['ﾊﾁ-01'], '半角カナのコード = 自分だけ (全角カナのコードを当てない)');
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('ハチ-01'));
  assert.deepEqual(codesOf(r.j), ['ハチ-01'], '全角カナのコード = 自分だけ');
  r = await call('GET', '/api/amazon/components?q=' + encodeURIComponent('ﾊﾁﾐﾂ'));
  assert.deepEqual(codesOf(r.j).slice().sort(), ['ﾊﾁ-01', 'ハチ-01'].sort(), '名前は NFKC (半角カナの言葉でひらがなの名前に当たる) = 両方');
  const res = await save('kana_both', '半角と全角', [{ code: 'ﾊﾁ-01', qty: 1 }, { code: 'ハチ-01', qty: 2 }]);
  assert.equal(res.state, 'active');
  assert.deepEqual(await compsOfListing('kana_both'), [['ﾊﾁ-01', 1, 0, 'manual'], ['ハチ-01', 2, 1, 'manual']], '保存でも別の商品 (2 回ありますにならない)');
  // 画面の JS のコードの比べ方 (重なりの確かめ「N 行目にもうあります」・ぴったりの自動の選択) = lib/sku-norm.js normSku と同じ答え
  const src = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '../apps/master-edit/public/me-amazon.js'), 'utf8');
  const m = /function normCode\(v\) \{[\s\S]*?\n  \}/.exec(src);
  assert.ok(m, '画面の JS に normCode がある');
  const normCode = vm.runInNewContext(`(${m[0]})`);
  const { normSku } = await import('../lib/sku-norm.js');
  for (const s of ['ﾊﾁ-01', 'ハチ-01', 'Ａ００１', 'a\u2010001', 'a\u2212001', 'a\uFF0D001', ' k 0\u300001 ', 'ｶﾞ1', 'ガ1', 'ABC-1', '\uFF41\uFF42']) {
    assert.equal(normCode(s), normSku(s), `画面と normSku が違う: ${JSON.stringify(s)}`);
  }
  assert.notEqual(normCode('ﾊﾁ-01'), normCode('ハチ-01'), '画面でも別の商品');
});

await ta('[14] 画面: 欄は候補つき (combobox・候補の箱・API を呼ぶ・何文字から)・保存できないときは出さない・つかいかたに一言', async () => {
  let r = await call('GET', '/amazon/sku?sku=pr_a001');
  assert.equal(r.status, 200);
  const js = (await checkScripts(r.text, 0)).find((x) => x.includes('me-amazon.js — Amazon SKU の対応')) || '';
  assert.ok(js.includes("'/api/amazon/components?q='"), '画面が探す API を呼ぶ');
  assert.ok(r.text.includes('id="comp-suggest"') && r.text.includes('role="listbox"'));
  assert.ok(/class="in mono c-code"[^>]*role="combobox"[^>]*aria-controls="comp-suggest"/.test(r.text), '今の行の欄');
  assert.ok(/<template id="comp-tpl">[\s\S]*?role="combobox"[\s\S]*?<\/template>/.test(r.text), '足す行の欄も');
  assert.equal(JSON.parse(decode(/<script type="application\/json" id="me-amazon">([\s\S]*?)<\/script>/.exec(r.text)[1])).searchMin, R.COMPONENT_SEARCH_MIN);
  assert.ok(r.text.includes('コード・名前・JAN で探せます'));
  process.env.MASTER_EDIT_OPEN = '0';
  try {
    r = await call('GET', '/amazon/sku?sku=pr_a001');
    assert.ok(r.text.includes('いまは保存できません'));
    assert.ok(!r.text.includes('id="comp-suggest"') && !r.text.includes('aria-controls="comp-suggest"'), '保存できない画面には候補を出さない');
  } finally { process.env.MASTER_EDIT_OPEN = '1'; }
  const m = await call('GET', '/manual');
  assert.ok(m.text.includes('NE の商品を探して選ぶ') && m.text.includes('灰色'), 'つかいかたに一言');
});

console.log('\nまだ NE に無い新商品・要確認も構成に (10/12・0073)');
await ta('[15] 下書き・NE 登録待ちの商品で対応を作れる (代表 = 1 行目でも)・写しの決まった形に入る (形の確かめも通る)・使っている下書きは「下書きを消す」が断る / 0073 の関数 = 前の版の写し + 🆕 0073 の所だけ', async () => {
  const keep = await regOf('a007');
  try {
    await setReg('a007', 'draft');
    const c = (await call('GET', '/api/amazon/components?q=a007')).j.items[0];
    assert.deepEqual([c.code, c.usable, c.not_in_ne, c.why], ['a007', true, true, null], '候補 = 使える・NE 未登録の印');
    let res = await save('notne_draft', 'まだ NE に無い 2 個組', [{ code: 'a007', qty: 2 }, { code: 'a001', qty: 1 }]);
    assert.equal(res.state, 'active');
    assert.deepEqual(await compsOfListing('notne_draft'), [['a007', 2, 0, 'manual'], ['a001', 1, 1, 'manual']], '代表 (1 行目) が NE 未登録でも作れる');
    await setReg('a007', 'ne_pending');
    res = await save('notne_pending', 'NE 登録待ち', [{ code: 'a007', qty: 1 }]);
    assert.equal(res.state, 'active');
    // 古い表への写し (⑦-2) の決まった形 = NE にあるかは見ない・形の確かめ (validateSkuMap) も通る
    const canon = await A.readCompanyAmazonMapCanon(db, { sellerSkus: ['notne_draft', 'notne_pending'] });
    assert.deepEqual(canon.components.map((x) => [x.seller_sku, x.ne_code, x.quantity]).sort(), [['notne_draft', 'a001', 1], ['notne_draft', 'a007', 2], ['notne_pending', 'a007', 1]]);
    assert.deepEqual(K.validateSkuMap(canon), []);
    // 使っている下書きは「下書きを消す」が断る (0071・出品の構成)
    await setReg('a007', 'draft');
    const chk = (await q('select ops.sku_draft_cancel_check($1) as r', [await skuIdOf('a007')]))[0].r;
    assert.ok(chk.problems.some((p) => p.reason === 'listing_component'), JSON.stringify(chk.problems));
    // 要確認 (NE で直接作った商品) も使える (中原さん 10/12 = B)・写しにも入る
    await setReg('a007', 'quarantined');
    res = await save('notne_q', 'NE で直接作った商品', [{ code: 'a007', qty: 3 }]);
    assert.equal(res.state, 'active');
    assert.deepEqual(await compsOfListing('notne_q'), [['a007', 3, 0, 'manual']]);
    // やめた・例外 は今までどおり断る ([5] [14])
    await setReg('a007', 'cancelled');
    await rejectsWith(save('notne_c', 'x', [{ code: 'a007', qty: 1 }]), 400, 'invalid_input');
    await rejectsWith(save('notne_e', 'x', [{ code: 'a00x', qty: 1 }]), 400, 'invalid_input');
    // 🆕 0073: 要確認をやめる (人の理由・運用のロールが直に) = 出品の構成に使われていれば断る / 使われていなければ今までどおりやめられる
    await setReg('a007', 'quarantined');
    const te = await errOf(pg.query("select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', '試験でやめる')", [await skuIdOf('a007')]));
    assert.match(te?.message || '(通った)', /^listing_component: 要確認の商品 a007 は販路の出品の構成に使われている \(amazon:notne_/);
    assert.equal(await regOf('a007'), 'quarantined', '何も変えない');
    const free = (await q(`select s.code from core.skus s where s.sku_kind = 'single' and not exists (select 1 from core.listing_components lc where lc.sku_id = s.sku_id) order by s.code limit 1`))[0]?.code;
    assert.ok(free, '出品の構成に無い単品が試験の DB にある');
    const keepFree = await regOf(free);
    try {
      await setReg(free, 'quarantined');
      await pg.query("select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', '試験でやめる')", [await skuIdOf(free)]);
      assert.equal(await regOf(free), 'cancelled', '使われていない要確認は今までどおりやめられる');
    } finally { await setReg(free, keepFree); }
  } finally { await setReg('a007', keep); }
  // migration の写し: 前の版 (0054 / 0067) から写して 🆕 0073 の所だけ変えた (番号を付け替えても名前で探す)
  const MIG = path.join(path.dirname(fileURLToPath(import.meta.url)), '../db/company/migrations');
  const rd = (f) => fs.readFileSync(path.join(MIG, f), 'utf8').replace(/\r\n/g, '\n');
  const mine = fs.readdirSync(MIG).find((f) => /^\d{4}_amazon_map_not_in_ne\.sql$/.test(f));
  const fnText = (t, name) => { const i = t.search(new RegExp(`create (?:or replace )?function ${name.replace(/\./g, '\\.')}\\(`)); if (i < 0) return null; const k = t.indexOf(' as $$', i); return t.slice(i, t.indexOf('$$;', k + 6) + 3).replace(/^create or replace function/, 'create function'); };
  const strip = (t) => { const out = []; let skip = false; for (const l of t.split('\n')) { if (/🆕 0073 ここから/.test(l)) { skip = true; continue; } if (/🆕 0073 ここまで/.test(l)) { skip = false; continue; } if (skip || /🆕 0073/.test(l)) continue; out.push(l); } return out.join('\n'); };
  const now1 = fnText(rd(mine), 'ops.amazon_map_components');
  assert.equal(mine, '0073_amazon_map_not_in_ne.sql', '番号 = 0073 (0072 = #1690 の NE から消す CSV)');
  assert.equal(strip(now1).replace("f.state not in ('draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available', 'quarantined')", "f.state not in ('ne_confirmed', 'distributable', 'available')"),
    fnText(rd('0054_amazon_sku_maps.sql'), 'ops.amazon_map_components'), 'amazon_map_components = 0054 の版 + 状態の 3 つだけ');
  const now2 = fnText(rd(mine), 'ops.cancel_variation_child');
  assert.notEqual(strip(now2), now2);
  assert.equal(strip(now2), fnText(rd('0067_variation_groups.sql'), 'ops.cancel_variation_child'), 'cancel_variation_child = 0067 の版 + 🆕 0073 の行だけ');
  for (const [name, after] of [['ops.amazon_map_components', '0055'], ['ops.cancel_variation_child', '0068']]) {
    const later = fs.readdirSync(MIG).filter((x) => /^\d{4}_.+\.sql$/.test(x) && x >= after && x !== mine);
    // 検査が空回りしない (#1694 Codex R1 Low): 0055〜 / 0068〜 の migration を本当に見ている (今の master で 0055〜0071 = 17 個・0068〜0071 = 4 個。増えるのはよい)
    assert.ok(later.length >= (after === '0055' ? 17 : 4), `${name}: 見た migration が少ない (${later.length} 個)`);
    assert.ok(later.every((f) => /^\d{4}_/.test(f) && f >= after) && later.includes('0071_sku_draft_cancel.sql'), later.join(','));
    for (const f of later) assert.equal(fnText(rd(f), name), null, `${f} にも ${name} がある (前の最新の版が変わった = 写し直す)`);
  }
  // fnText が本当に関数を見つけること (見つからない = いつも null = 空回り)
  assert.ok(fnText(rd('0054_amazon_sku_maps.sql'), 'ops.amazon_map_components') && fnText(rd('0067_variation_groups.sql'), 'ops.cancel_variation_child'));
  // JS の決まり = DB の決まり
  assert.deepEqual([...A.USABLE_REG_STATES], ['draft', 'ne_pending', 'ne_confirmed', 'distributable', 'available', 'quarantined']);
  // 0072 (#1690) はこの 2 つの関数を書き換えていない = 写しの前の最新の版は 0054 / 0067 のまま (上の later の検査に 0072 も入っている)
  assert.ok(fs.readdirSync(MIG).includes('0072_ne_delete_csv.sql'));
  assert.equal(fnText(rd('0072_ne_delete_csv.sql'), 'ops.amazon_map_components'), null);
  assert.equal(fnText(rd('0072_ne_delete_csv.sql'), 'ops.cancel_variation_child'), null);
  // ops.transition_sku_registration = 0071 の版 (前の最新・0072 は書き換えていない) + 🆕 0073 の行だけ
  const now3 = fnText(rd(mine), 'ops.transition_sku_registration');
  assert.ok(now3 && strip(now3) !== now3);
  assert.equal(strip(now3), fnText(rd('0071_sku_draft_cancel.sql'), 'ops.transition_sku_registration'), 'transition_sku_registration = 0071 の版 + 🆕 0073 の行だけ');
  // 🆕 0079 (要確認を確認済みにする): 0073 の後の版は、その migration の試験が「0073 の版 + 🆕 の行だけ」を確かめる = ここは 0073 より前だけ見る
  for (const f of fs.readdirSync(MIG).filter((x) => /^\d{4}_.+\.sql$/.test(x) && x >= '0072' && x < mine)) assert.equal(fnText(rd(f), 'ops.transition_sku_registration'), null, `${f} にも transition_sku_registration がある`);
  assert.deepEqual([...A.NOT_IN_NE_REG_STATES], ['draft', 'ne_pending']);
});

await ta('[15] #1690 (0072) の守り: NE から消す依頼の途中の商品を構成に入れる保存 = DB の trigger が断る → 400 の分かる文 (500 にしない・何も保存しない)', async () => {
  const sid = await skuIdOf('a003');
  await pg.query(`select set_config('ops.ne_del_protocol', '1', false)`);
  try {
    const bytes = Buffer.from('コード\r\na003\r\n');
    const eid = (await pg.query(`insert into ops.ne_del_exports (kind, schema_version, encoding, header, file_bytes, sha256, row_count, request_id, reason, built_by, built_day, db_user)
      values ('products', 'ne-del-products-v1', 'sjis', 'コード', $1, encode(sha256($1), 'hex'), 1, gen_random_uuid(), '試験', 'naka@test', current_date, current_user) returning export_id`, [bytes])).rows[0].export_id;
    await pg.query(`insert into ops.ne_del_items (export_id, sku_id, code_norm, ne_code, sku_kind, action, state, evidence, state_changed_by) values ($1, $2, 'a003', 'a003', 'single', 'delete', 'live', '{}', 'naka@test')`, [eid, sid]);
  } finally { await pg.query(`select set_config('ops.ne_del_protocol', '', false)`); }
  const e = await rejectsWith(save('nedel_live', 'x', [{ code: 'a003', qty: 1 }]), 400, 'invalid_input');
  assert.match(e.message, /NE から消す依頼の途中の商品があります .*a003/);
  assert.equal(await lidOf('nedel_live'), null, '何も保存していない');
});

await ta('[16] ASIN が出品 (夜間ロード) に無い対応 = Amazon の出品一覧 (0074・出品レポート) の ASIN・区分で埋める (一覧・CSV)・出品の ASIN は上書きしない・食い違う 2 行 = 埋めない・出品一覧が無い = 今のまま (10/16 中原さん)', async () => {
  const I = await import('../apps/company-db/ingest/amazon-listings.mjs');
  const noAsin = (await q(`select m.seller_sku from core.amazon_sku_maps m join core.listings l on l.listing_id = m.listing_id
     where l.catalog_item_id is null and m.state = 'active' order by m.seller_sku`)).map((x) => x.seller_sku);
  const withAsin = (await q(`select m.seller_sku, ci.asin from core.amazon_sku_maps m join core.listings l on l.listing_id = m.listing_id join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id
     where m.state = 'active' order by m.seller_sku limit 1`))[0];
  assert.ok(noAsin.length >= 2 && withAsin, `材料: ASIN の無い対応 2 つ以上と ASIN のある対応 (${noAsin.join(',')})`);
  const [fillSku, clashSku] = noAsin;
  const csvRow = async (sku) => {
    const lines = (await call('GET', '/amazon/list.csv')).text.split('\r\n');
    const head = lines[0].split('","').map((h) => h.replace(/"/g, ''));
    const line = lines.find((l) => l.startsWith(`"${sku}",`));
    assert.ok(line, `CSV に ${sku} がある`);
    const cells = line.slice(1, -1).split('","');
    return Object.fromEntries(head.map((h, i) => [h, cells[i]]));
  };
  // 出品一覧に無い = ASIN は空のまま・出どころも空
  let c = await csvRow(fillSku);
  assert.deepEqual([c.ASIN, c['ASIN の出どころ']], ['', '']);
  // 出品一覧: fillSku = ASIN と FBA (大文字で来ても正規化で同じ)・clashSku = 正規化で同じ 2 行の ASIN が違う・ASIN のある対応 = 別の ASIN (上書きしない)
  const rowsR = [
    [fillSku.toUpperCase(), 'B0FILL0001', 'FBA'], [clashSku, 'B0CLASH001', 'FBA'], [clashSku.toUpperCase(), 'B0CLASH002', 'FBA'], [withAsin.seller_sku, 'B0OTHER001', 'FBM'],
  ].map(([s, a, f]) => I.listingRowOf({ seller_sku: s, item_name: s, asin: a, open_date: null, open_date_raw: null, fulfillment: f, channel_raw: f === 'FBM' ? 'DEFAULT' : 'AMAZON_JP', status: 'inactive', status_raw: 'Inactive', quantity: null, price: null }));
  const gen = Date.parse('2030-02-01T00:00:00Z');
  const r0 = await I.ingestAmazonListings(db, { shop_code: I.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: 'r_asin_fill', checksum: I.listingChecksum(rowsR), row_count: rowsR.length, skipped_count: 0, rows: rowsR, force: true }, { nowMs: gen });
  assert.ok(['applied', 'refreshed'].includes(r0.status), JSON.stringify(r0));
  c = await csvRow(fillSku);
  assert.deepEqual([c.ASIN, c['ASIN の出どころ'], c['区分'], c['区分の出どころ']], ['B0FILL0001', '出品一覧', 'FBA', '出品一覧'], '停止中の出品でも出品一覧の ASIN・区分で埋める');
  c = await csvRow(clashSku);
  assert.deepEqual([c.ASIN, c['ASIN の出どころ']], ['', ''], '正規化で同じ 2 行の ASIN が食い違う = 埋めない');
  c = await csvRow(withAsin.seller_sku);
  assert.deepEqual([c.ASIN, c['ASIN の出どころ']], [withAsin.asin, '出品'], '出品の ASIN は出品一覧で上書きしない');
  // 一覧の画面: 埋めた ASIN に「出品一覧」の札
  const page = await call('GET', '/amazon/?q=' + encodeURIComponent(fillSku));
  assert.equal(page.status, 200);
  assert.ok(page.text.includes('B0FILL0001') && page.text.includes('>出品一覧</span>'), '一覧に出品一覧の ASIN と札');
  // 出品一覧を読めない (表が無い) = 今のまま (一覧・CSV は出す)
  const RD = await import('../apps/master-edit/amazon-read.mjs');
  const noReport = { query: (t, p) => (/to_regclass\('ops\.amazon_listing_report'\)/.test(t) ? { rows: [{ ok: false }] } : db.query(t, p)) };
  const all = await asRole(E, 'master_edit', () => RD.listAmazonMapsAll(noReport, {}));
  const fr = all.rows.find((x) => x.seller_sku === fillSku);
  assert.deepEqual([fr.asin, fr.asin_from, fr.report_fulfillment], [null, null, null], '出品一覧が無い = 埋めない');
});

await ta('[17] CSV の構成の商品コード = NE から来た商品なら NE の書き方 (10/16 中原さん・商品の一覧の CSV と同じ決まり): ok = NE の書き方 / 書き方が 2 つ・記録に無い = 今のコード + 最後の列「構成の商品コードの注意」/ 読めない = 1 つの注意 / 画面の一覧は今のまま', async () => {
  const RD = await import('../apps/master-edit/amazon-read.mjs');
  const NS = await import('../apps/master-edit/ne-spelling.mjs');
  // 材料: 構成が 2 行以上ある対応 (NE から来た商品だけ)
  const comps = await q(`select m.seller_sku, k.code, k.code_norm, c.qty, c.sort_order from core.amazon_sku_maps m join core.listing_components c on c.listing_id = m.listing_id
      join core.skus k on k.sku_id = c.sku_id left join ops.master_registrations mr on mr.sku_id = k.sku_id
     where m.state = 'active' and m.listing_id in (select listing_id from core.listing_components group by listing_id having count(*) >= 2)
       and not exists (select 1 from core.listing_components c2 join ops.master_registrations mr2 on mr2.sku_id = c2.sku_id where c2.listing_id = m.listing_id and mr2.origin = 'new_entry')
       and not exists (select 1 from core.listing_components c3 join core.skus k3 on k3.sku_id = c3.sku_id where c3.listing_id = m.listing_id and k3.code_norm !~ '^[a-z0-9_-]{1,30}$')
     order by m.seller_sku, c.sort_order`);
  assert.ok(comps.length >= 2, '材料: 構成が 2 行以上の対応');
  const sku = comps[0].seller_sku;
  const mine = comps.filter((x) => x.seller_sku === sku);
  const [okC, colC] = mine;
  assert.notEqual(okC.code_norm, colC.code_norm);
  const rowOf = async (dbx = db) => (await asRole(E, 'master_edit', () => RD.listAmazonMapsAll(dbx, {}))).rows.find((x) => x.seller_sku === sku);
  // NE の元のコードの記録がまだ無い = 今のコード + 「分からない」
  let r = await rowOf();
  const before = mine.map((x) => `${x.code}×${x.qty}`).join(', ');
  assert.equal(r.comps, before);
  assert.equal(r.comp_notes, mine.map((x) => `${x.code}: ${NS.SPELLING_NOTES.missing}`).join(' / '));
  assert.ok(!('comp_list' in r), '材料の列は CSV に渡さない');
  // 照合の回として入れる: 1 つ目 = ok (大文字の書き方)・2 つ目 = 書き方が 2 つ・ほかは記録に無い
  await q(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ('mc_20300301T220100000Z_a5a5a5', '2030-03-01T22:01:00Z', 0)`);
  await q('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: 'mc_20300301T220100000Z_a5a5a5', entries: [
    { code_norm: okC.code_norm, kind: 'product', state: 'ok', ne_code: okC.code_norm.toUpperCase(), spellings: [okC.code_norm.toUpperCase()] },
    { code_norm: colC.code_norm, kind: 'product', state: 'collided', ne_code: null, spellings: [colC.code_norm.toUpperCase(), colC.code_norm] },
  ] })]);
  r = await rowOf();
  const want = mine.map((x, i) => `${i === 0 ? okC.code_norm.toUpperCase() : x.code}×${x.qty}`).join(', ');
  assert.equal(r.comps, want);
  assert.equal(r.comp_notes, mine.slice(1).map((x, i) => `${x.code}: ${i === 0 ? NS.SPELLING_NOTES.collided : NS.SPELLING_NOTES.missing}`).join(' / '));
  // CSV: 構成の列は NE の書き方・新しい列は最後 (前の列の並びは変えない)
  const csv = (await call('GET', '/amazon/list.csv')).text.split('\r\n');
  const head = csv[0].slice(1, -1).split('","');
  assert.deepEqual([head[0], head[2], head.at(-1)], ['seller SKU', '構成 (NE の商品 × 数)', '構成の商品コードの注意']);
  const cells = csv.find((l) => l.startsWith(`"${sku}",`)).slice(1, -1).split('","');
  assert.deepEqual([cells[2], cells.at(-1)], [want, r.comp_notes]);
  // 画面の一覧は今のまま (core.skus.code)
  const page = await call('GET', '/amazon/?q=' + encodeURIComponent(sku));
  assert.ok(page.text.includes(okC.code) && !page.text.includes(okC.code_norm.toUpperCase() + '×'), '画面の一覧は変えない');
  // NE の元のコードを読めない (関数の権限が無い) = 構成は今のまま・注意は 1 つ
  const noFn = { query: (t, p) => (/has_function_privilege/.test(t) ? { rows: [{ ok: false }] } : db.query(t, p)) };
  r = await rowOf(noFn);
  assert.deepEqual([r.comps, r.comp_notes], [before, NS.SPELLING_NOTES.unreadable]);
});

console.log('\n未登録の広げる切り替え・CSV・その場で登録・構成の原価 (10/17 中原さん)');
await ta('[18] 未登録の期間 (7 / 30 / 90 日・全期間): 60 日ずつ区切って関数を呼んで足す (個数・注文・最後の日)・7 日は今までと同じ・全期間は売上の日次が始まる前で止まる・未判定の日の数え方 / 知らない ?days= は 7 日', async () => {
  const RU = await import('../apps/master-edit/amazon-read.mjs');
  let n = 0;
  const order = async (date, code, qty) => {
    n++;
    const o = (await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, 'amazon', 'amazon', $1, 'mall_api', $2::date, $2::date, 'shipped', false, 1, now(), 't', 'h') returning order_id`, [`p18-${n}`, date])).rows[0].order_id;
    await pg.query(`insert into core.order_lines (company_id, order_id, line_key, listing_id, unresolved_code, qty, cancelled_qty, received_batch_seq) values (1, $1, 'l1', null, $2, $3, 0, 1)`, [o, code, qty]);
  };
  // zz_sold: 今の 60 日の幅 (2029-11-12〜) の外の 2029-10-20 にも売れた = 90 日 / 全期間だけ足す。zz_old (2029-12-01) は 30 日より前・60 日の中
  await order('2029-10-20', 'zz_sold', 2);
  const um = (days) => asRole(E, 'master_edit', () => RU.amazonUnmapped(db, { now: new Date('2030-01-10T03:00:00Z'), channels: FEES_NOW, days }));
  const pick = (d) => Object.fromEntries([...d.rows, ...d.unknown, ...d.fbm].map((x) => [x.code, [x.units, x.orders, x.last_date]]));
  const d7 = await um(7);
  const direct = (await q(`select code, units::int as units, orders::int as orders, last_date::text as last_date from ops.amazon_map_unmapped_recent('2030-01-10', 7)`));
  assert.deepEqual(Object.keys(pick(d7)).sort(), direct.map((x) => x.code).sort(), '7 日 = 関数 1 回と同じ SKU');
  assert.deepEqual([d7.days, d7.period, d7.from], [7, '7', '2030-01-04']);
  const d30 = await um(30);
  assert.ok(!pick(d30).zz_old && pick(d30).zz_sold[0] === 5, '30 日: zz_old (40 日前) は出ない');
  const d90 = await um(90);
  assert.deepEqual(pick(d90).zz_sold, [7, 4, '2030-01-10'], '90 日: 2 つの幅の個数・注文を足す・最後の日はいちばん新しい日');
  assert.ok(pick(d90).zz_old, '90 日: zz_old が出る');
  assert.equal(d90.from, '2029-10-13');
  assert.equal(d90.undecided.length, 83, '90 日: 公開されていない日 (公開は 2030-01-04〜10 の 7 日だけ) = 83 日');
  const dAll = await um(null);
  assert.deepEqual([dAll.period, dAll.from], ['all', '2029-09-13'], '全期間: 公開が 1 日も無い 60 日の幅 (2029-09-13〜11-11) まで見て止まる');
  assert.deepEqual(pick(dAll).zz_sold, [7, 4, '2030-01-10']);
  assert.deepEqual(dAll.undecided, [], '全期間: 公開がいちばん初めの日より前は未判定に数えない');
  assert.deepEqual(RU.unmappedPeriodOf({ days: '3' }), { key: '7', days: 7 });
  assert.deepEqual(RU.unmappedPeriodOf({ days: 'all' }), { key: 'all', days: null });
  // 画面: 期間の札・90 日で zz_old・URL は ?days=90・既定の 7 日は引数なし
  let r = await call('GET', '/amazon/unmapped?days=90');
  assert.equal(r.status, 200); await checkScripts(r.text);
  assert.ok(r.text.includes('zz_old') && r.text.includes('aria-current="true">直近 90 日'), '90 日の画面');
  assert.ok(r.text.includes('83 日 (2029-10-13 〜'), '未判定の日が多い = 数と範囲');
  r = await call('GET', '/amazon/unmapped');
  assert.ok(!r.text.includes('zz_old') && r.text.includes('aria-current="true">直近 7 日') && r.text.includes('href="?days=all"'));
});

await ta('[18] 自社発送で seller SKU が NE の商品コードでない SKU = 別の一覧 (?fbm=1・既定は件数の一言だけ)・NE コードと同じ自社発送は数だけ・区分を読めないときは出さない', async () => {
  const RU = await import('../apps/master-edit/amazon-read.mjs');
  // a008 = NE の商品コード (Company DB にある) の自社発送の注文 (構成が無い)
  const o = (await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
    values (1, 'amazon', 'amazon', 'p18-fbm', 'mall_api', '2030-01-09'::date, '2030-01-09'::date, 'shipped', false, 1, now(), 't', 'h') returning order_id`)).rows[0].order_id;
  await pg.query(`insert into core.order_lines (company_id, order_id, line_key, listing_id, unresolved_code, qty, cancelled_qty, received_batch_seq) values (1, $1, 'l1', null, 'a008', 1, 0, 1)`, [o]);
  const fees = RU.channelsFromFeeRows([['pr_a001', 'FBA'], ['a003', 'FBM'], ['zz_sold', 'FBA'], ['zz_fbm', 'FBM'], ['a008', 'FBM']].map(([s, c]) => ({ seller_sku: s, fulfillment_channel: c, fetched_at: '2030-01-09 12:00:00' })));
  __setAmazonChannelsProvider(async () => fees);
  try {
    const d = await asRole(E, 'master_edit', () => RU.amazonUnmapped(db, { now: new Date('2030-01-10T03:00:00Z'), channels: fees }));
    assert.deepEqual([d.fbm.map((x) => x.code), d.fbmNeCode], [['zz_fbm'], 1], 'zz_fbm = NE コードでない / a008 = NE コードと同じ (数だけ)');
    assert.ok(!d.rows.some((x) => x.code === 'zz_fbm'), 'FBA の一覧には入れない');
    let r = await call('GET', '/amazon/unmapped');
    assert.ok(r.text.includes('id="unmapped-fbm"') && r.text.includes('NE の商品コードでない SKU — 1 件') && !r.text.includes('zz_fbm'), '既定は件数の一言だけ (今までの画面のまま)');
    assert.ok(r.text.includes('構成がまだ無い自社発送の SKU が 1 件'));
    r = await call('GET', '/amazon/unmapped?fbm=1');
    assert.ok(r.text.includes('id="unmapped-fbm-list"') && r.text.includes('data-sku="zz_fbm"') && !r.text.includes('data-sku="a008"'));
    // 区分を読めない = 出さない (0 件と言わない)
    const none = await asRole(E, 'master_edit', () => RU.amazonUnmapped(db, { now: new Date('2031-01-10T03:00:00Z'), channels: null, days: null }));
    assert.equal(none.decidable === false ? none.fbm.length : 0, 0);
  } finally { __setAmazonChannelsProvider(async () => FEES_NOW); }
  await pg.query(`delete from core.order_lines where order_id = $1`, [o]);
  await pg.query(`delete from core.orders where order_id = $1`, [o]);
});

await ta('[18] 未登録の CSV (?days=&fbm=): BOM・CRLF・見出し・一覧の列 (FBA / 自社発送 / 区分が分からない)・期間の列・式の注入の対策・区分を読めない = 503 / まだつないでいない SKU の CSV の形', async () => {
  const LC = await import('../apps/master-edit/list-csv.mjs');
  const res = await fetch(`${BASE}/amazon/unmapped.csv?days=90&fbm=1`, { headers: { 'x-test-session': 'viewer' } });
  assert.equal(res.status, 200, '見る人も CSV を出せる (読むだけ)');
  assert.match(res.headers.get('content-disposition'), /attachment; filename="amazon-unmapped_\d{8}-\d{4}\.csv"/);
  const raw = Buffer.from(await res.arrayBuffer());
  assert.deepEqual([...raw.subarray(0, 3)], [0xef, 0xbb, 0xbf], 'BOM');
  const lines = raw.toString('utf8').slice(1).split('\r\n');
  assert.equal(lines.at(-1), '', '最後も CRLF');
  assert.ok(lines[0].startsWith('"一覧","seller SKU","Amazon の商品名","区分"'));
  const row = (code) => lines.find((l) => l.split('","')[1] === code);
  assert.ok(row('zz_sold').startsWith('"FBA",') && row('zz_sold').includes(',"7","4","2030/01/10"'), '90 日の足した数');
  assert.ok(row('zz_fbm').startsWith('"自社発送 (seller SKU が NE コードでない)",'));
  assert.ok(row('pr_pack2').startsWith('"区分が分からない",'));
  assert.ok(lines[1].includes('"直近 90 日 (2029/10/13 〜 2030/01/10)"'), '期間の列');
  let r = await call('GET', '/amazon/unmapped.csv');
  assert.ok(!r.text.includes('zz_fbm') && !r.text.includes('zz_old'), '既定 = 7 日・自社発送の一覧は閉じたまま');
  // 式の注入の対策 (seller SKU・商品名が = + - @ で始まる)
  const csv = LC.buildUnmappedCsv({ rows: [{ code: '=1+2', title: '@SUM(A1)', channel: 'FBA', channel_from: 'fees', units: 1, orders: 1, last_date: '2030-01-10', map_state: null, listing_id: null }], unknown: [], fbm: [], today: '2030-01-10', from: '2030-01-04', undecided: [] }, { channels: { FBA: 'FBA' }, periodLabel: '直近 7 日' });
  assert.ok(csv.includes(`"'=1+2","'@SUM(A1)"`), csv);
  const ul = LC.buildUnlinkedCsv({ rows: [{ seller_sku: '-cmd', item_name: '+x', asin: null, open_date: '2030-01-01', status: 'active', listing_id: null, map_state: null, fba_row: true, fba_available: 3, last_sold: null, price: '1280' }], fba: { ok: true, date: '2030-01-10' }, sold: { ok: true }, load: { snapshot_at: '2030-01-09T14:30:00Z' } }, { statusWords: { active: '出品中' } });
  assert.ok(ul.startsWith('﻿"seller SKU","Amazon の商品名","ASIN","出品日"') && ul.includes(`"'-cmd","'+x","","2030/01/01","出品中","対応なし","3","","1280"`), ul);
  // 区分を読めない = 503 (0 件の CSV にしない)
  __setAmazonChannelsProvider(async () => null);
  const I = await import('../apps/company-db/ingest/amazon-listings.mjs');
  const keep = (await q('select load_id, snapshot_at from ops.amazon_listing_report_loads order by generation desc limit 1'))[0];
  await q(`update ops.amazon_listing_report_loads set snapshot_at = '2029-01-01T00:00:00Z' where load_id = $1`, [keep.load_id]);
  try {
    r = await call('GET', '/amazon/unmapped.csv');
    assert.equal(r.status, 503); assert.match(r.text, /読めない/);
  } finally {
    await q('update ops.amazon_listing_report_loads set snapshot_at = $2 where load_id = $1', [keep.load_id, keep.snapshot_at]);
    __setAmazonChannelsProvider(async () => FEES_NOW);
  }
  assert.ok(I.SHOP_CODE);
});

await ta('[18] その場で登録: 行のボタン (名簿の人・保存を開いているときだけ)・欄が読む API (版・名前の初めの値 = 出品一覧の商品名・先に作れない形)・保存は 1 つの画面と同じ道 (版・request_id の押し直し)', async () => {
  const I = await import('../apps/company-db/ingest/amazon-listings.mjs');
  // 出品一覧に zz_sold の Amazon の商品名 (前後に空白)
  const rowsR = [['zz_sold', '  国産 はちみつ' + String.fromCharCode(0x3000) + '2 個組  ', 'FBA'], ['zz_fbm', 'zz_fbm の商品', 'FBM']].map(([s, t, f]) => I.listingRowOf({ seller_sku: s, item_name: t, asin: null, open_date: null, open_date_raw: null, fulfillment: f, channel_raw: f === 'FBM' ? 'DEFAULT' : 'AMAZON_JP', status: 'active', status_raw: 'Active', quantity: null, price: null }));
  const gen = Date.parse('2030-02-02T00:00:00Z');
  const r0 = await I.ingestAmazonListings(db, { shop_code: I.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: 'r_inline', checksum: I.listingChecksum(rowsR), row_count: rowsR.length, skipped_count: 0, rows: rowsR, force: true }, { nowMs: gen });
  assert.ok(['applied', 'refreshed'].includes(r0.status), JSON.stringify(r0));
  let r = await call('GET', '/amazon/unmapped');
  assert.ok(r.text.includes('data-inline data-sku="zz_sold"') && r.text.includes('id="me-inline"'), '名簿の人にはその場で登録のボタン');
  const js = await checkScripts(r.text);
  const inl = js.find((x) => x.includes('me-amazon-inline.js — 一覧の行で')) || '';
  for (const api of ["'/api/amazon/map?sku='", "'/api/amazon/save'", "'/api/amazon/components?q='"]) assert.ok(inl.includes(api) || js.some((x) => x.includes(api)), `画面が ${api} を呼んでいない`);
  assert.ok(r.text.includes('国産 はちみつ'), '行に Amazon の商品名');
  r = await call('GET', '/amazon/unmapped', { session: 'viewer' });
  assert.ok(!r.text.includes('data-inline') && !r.text.includes('me-amazon-inline.js'), '見る人には出さない');
  process.env.MASTER_EDIT_OPEN = '0';
  try { r = await call('GET', '/amazon/unmapped'); assert.ok(!r.text.includes('data-inline'), '保存を開いていない = 出さない'); } finally { process.env.MASTER_EDIT_OPEN = '1'; }
  // 欄が読む API
  r = await call('GET', '/api/amazon/map?sku=ZZ_SOLD', { session: 'viewer' });
  assert.equal(r.status, 200);
  assert.deepEqual([r.j.seller_sku, r.j.versions, r.j.map, r.j.listing, r.j.components, r.j.name_default, r.j.name_from, r.j.new_sku_problems],
    ['zz_sold', { listing: null, map: null }, null, null, [], '国産 はちみつ' + String.fromCharCode(0x3000) + '2 個組', 'report', []], '名前の初めの値 = 出品一覧の商品名 (前後の空白なし)');
  r = await call('GET', '/api/amazon/map?sku=pr_pack2');
  assert.deepEqual([r.j.map.state, r.j.components, r.j.name_default, r.j.name_from], ['deleted', [], '2 個組', 'map'], '墓標 = 構成なし・今の名前');
  r = await call('GET', '/api/amazon/map?sku=' + encodeURIComponent('zz' + String.fromCharCode(9) + 'x'));
  assert.equal(r.status, 400);
  r = await call('GET', '/api/amazon/map?sku=' + encodeURIComponent('ｚｚ_full'));
  assert.ok(r.j.new_sku_problems.length > 0, '先に作れない形 = 理由を返す (保存も断る)');
  // 保存 = POST /api/amazon/save (1 つの画面と同じ)。同じ request_id の押し直し = 前の結果
  const m = (await call('GET', '/api/amazon/map?sku=zz_sold')).j;
  const rid = uuid();
  const body = { request_id: rid, seller_sku: 'zz_sold', name: m.name_default, components: [{ code: 'a006', qty: '2' }, { code: 'a007', qty: 1 }], reason: '未登録の一覧からその場で登録', seen: { versions: m.versions } };
  r = await call('POST', '/api/amazon/save', { body, session: 'viewer' });
  assert.equal(r.status, 403, '見る人は保存できない');
  r = await call('POST', '/api/amazon/save', { body });
  assert.equal(r.status, 200, r.text); assert.equal(r.j.created, true); assert.equal(r.j.listing_created, true);
  r = await call('POST', '/api/amazon/save', { body });
  assert.equal(r.status, 200); assert.equal(r.j.replayed, true, '押し直し = 前の結果 (2 回入らない)');
  assert.deepEqual(await compsOfListing('zz_sold'), [['a006', 2, 0, 'manual'], ['a007', 1, 1, 'manual']]);
  assert.equal((await q(`select reason_text from events.master_change_events where request_id = $1 and entity_type = 'amazon_sku_map' limit 1`, [rid]))[0]?.reason_text, '未登録の一覧からその場で登録', '記録に理由');
  // 開いた後に作られた = 古い版で保存 = 409 (何も保存しない)
  r = await call('POST', '/api/amazon/save', { body: { ...body, request_id: uuid(), components: [{ code: 'a006', qty: 1 }] } });
  assert.equal(r.status, 409); assert.equal(r.j.reason, 'version_conflict');
  r = await call('GET', '/api/amazon/map?sku=zz_sold');
  assert.deepEqual(r.j.components.map((c) => [c.code, c.qty]), [['a006', 2], ['a007', 1]], 'もう構成がある = 画面はその場では直さない (1 つの画面へ)');
});

await ta('[19] 構成の原価 (見るだけ): 保存してある構成の今日の原価 × 数・合計・状態 / 原価の無い構成品 = 合計は出せない (分かる分だけ)・読めない = 一言 (画面は出る)', async () => {
  const RU = await import('../apps/master-edit/amazon-read.mjs');
  const costOf = async (code) => Number((await q(`select cost_jpy from core.sku_costs where sku_id = (select sku_id from core.skus where code = $1) and valid_to is null`, [code]))[0].cost_jpy);
  const c6 = await costOf('a006'); const c7 = await costOf('a007');
  let r = await call('GET', '/amazon/sku?sku=zz_sold');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('id="comp-cost"') && r.text.includes('構成の原価'));
  const yen = (v) => `${v.toLocaleString('ja-JP')} 円`;
  const tot = /id="cost-total"><b>([^<]*)<\/b>/.exec(r.text)[1];
  assert.equal(tot, yen(c6 * 2 + c7), '合計 = Σ 原価 × 数');
  assert.ok(r.text.includes(yen(c6 * 2)) && r.text.includes('>確定</span>'), '小計と状態');
  // 原価の無い構成品 (例外の SKU には原価の行が無い) = 合計は出せない
  const ids = Object.fromEntries((await q(`select code, sku_id::text as id from core.skus where code in ('a006', 'aexc')`)).map((x) => [x.code, x.id]));
  const cc = await asRole(E, 'master_edit', () => RU.componentCosts(db, [{ sku_id: ids.a006, code: 'a006', name: 'x', qty: 3 }, { sku_id: ids.aexc, code: 'aexc', name: 'y', qty: 1 }], { today: '2030-01-10' }));
  assert.deepEqual([cc.total, cc.known, cc.missing, cc.warn, cc.lines[1].cost, cc.lines[0].subtotal], [null, c6 * 3, 1, true, null, c6 * 3]);
  // 原価の日の決まり: 今日より先の日の行は使わない (今日の行)
  const early = await asRole(E, 'master_edit', () => RU.componentCosts(db, [{ sku_id: ids.a006, code: 'a006', name: 'x', qty: 1 }], { today: '2000-01-01' }));
  assert.equal(early.total, null, 'その日にまだ原価が無い');
  // 読めない = 一言 (保存の画面は出る)
  const broken = { query: (t, p) => (/from core\.sku_costs y/.test(t) ? Promise.reject(Object.assign(new Error('x'), { code: '42501' })) : db.query(t, p)) };
  const bad = await RU.componentCosts(broken, [{ sku_id: ids.a006, code: 'a006', qty: 1 }], { today: '2030-01-10' });
  assert.equal(bad.error, '原価を読めません');
  // 構成が無い (墓標) = 原価の欄を出さない
  r = await call('GET', '/amazon/sku?sku=pr_pack2');
  assert.ok(!r.text.includes('id="comp-cost"'));
});

server.close();
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
