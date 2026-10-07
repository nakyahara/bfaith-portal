/**
 * test-master-widen-amazon-pg.mjs — 0059 (広げる道で listing_components.amazon を足す・Amazon SKU の対応の PR-B) を実 PostgreSQL の独立した接続と本物のロールで確かめる
 *
 * 固定する契約:
 *   [K]  skus.sku_kind だけを足す試み (今までどおり): 区分の判断の記録 (held)・最終形を見る / Amazon の対応の数は見ない (active 0・消えた対応があっても止まらない)
 *   [A]  listing_components.amazon だけを足す試み (sku_kind はもう company):
 *        区分の食い違い (held) があっても止まらない・区分の数を出さない / 消えた対応 1 件 = 断る / active 0 件 = 断る / 写しの証拠のハッシュ違い・数の違い = 断る /
 *        手の入口 = gas:logizard-sheet-and-sku-map / CLI の check がハッシュを照らす / 揃えば widen が通る (読むだけの判定と同じ数)
 *   [KA] 両方を足す試み: 区分と Amazon の両方を見る (どちらか 1 つでも断る)・揃えば通る
 *   [RC] (#1648 Codex R1 Medium 2) reconcile: 移行 → cancel → 古い表を直す (足す・直す・消す) → 新しい試み → reconcile → check → widen が通る /
 *        ハッシュ違い・止める項目・窓の外・frozen・portal の行・合わせた後のハッシュ違い = 断る (巻き戻す) / 2 回目は何も書かない / 墓標を戻す / 墓標は lost にならない
 *   [M]  移行の apply (amazon-map-migrate.mjs) の段階の条件: frozen は今までどおり / new_open は指した試み (--attempt) が Amazon を足す試みで、
 *        widen の判定と同じ共通の部品 (全部のプロセスの 2 版の ack が prepare の後・書きかけ 0・新しい・手の入口の停止) が通るときだけ (#1648 Codex R1 Medium 1) /
 *        試みを指さない・知らない試み・閉じた試み・sku_kind だけの試み・ack が足りない / 古い / 書きかけ・停止の前・widen の後 = 断る / 移行は epoch の共有の鍵を取る
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-widen-amazon-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロールをクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles as createWatchRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import * as OS from '../apps/company-db/load/ownership-state.mjs';
import * as W from '../apps/company-db/load/widen-state.mjs';
import * as M from '../lib/amazon-map-migrate.mjs';
import * as A0 from '../lib/amazon-map-write.mjs';
import * as K0 from '../lib/sku-map-canonical.js';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { ownershipHash, recordLegacyGateAckV2 } from '../lib/master-cutover.mjs';
import { cli as epochCli } from './company-db/master-ownership-epoch.mjs';
import { forceNewOpen, fakeLoad, hex } from './fixtures/master-widen.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0059 の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const errOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e; } return null; };

const AMZ = 'main@A1VC38T7YXB528';
const KEY_A = 'listing_components.amazon', KEY_K = 'skus.sku_kind';
const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE0 = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company' };   // sku_kind も Amazon も load
const BASE_K = { ...BASE0, [KEY_K]: 'company' };                                     // 10/7 の本番の形 (sku_kind は widen 済み)
const MANIFEST = { schema: 'test', entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual', owner_cols: ['skus.name'] },
  { id: 'ne:set-kind', kind: 'manual', owner_cols: [KEY_K] }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual', owner_cols: [KEY_A, 'skus.name', 'external_ids.jan'] }] };
const CAPABLE = [KEY_A, KEY_K, 'skus.name', 'products.name'];
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };

const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const setOf = (code) => ({ code, name: code, kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } });
const amazonListing = (listingCode, comps, evidenceSource) => ({ mall: 'amazon', shopCode: AMZ, marketplaceId: 'A1VC38T7YXB528', listingCode, title: listingCode, status: 'active', components: comps,
  asinCandidates: [], fnskuCandidates: [], evidenceSource });
const fbm = (code) => [{ code, qty: 1, resolution: 'exact', evidence: { source: 'fbm_ne_code' } }];
const PLAN = {
  skus: [...['a001', 'a002', 'a003', 'a004', 'a005', 'a006'].map(single), setOf('aset1')],
  variationGroups: [], setComponents: [{ parentCode: 'aset1', childCode: 'a001', qty: 3, source: 'ne' }],
  listings: [amazonListing('pr_a001', [{ code: 'a001', qty: 1, sortOrder: 0, resolution: 'imported', evidence: { source: 'm_sku_master' } }], 'mirror_sku_master'),
    amazonListing('a003', fbm('a003'), 'amazon_fees_fbm'), amazonListing('a004', fbm('a004'), 'amazon_fees_fbm')],
  observations: [], physicals: [], compliance: [], workers: [], suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
};

// 古い表 (miniPC の warehouse.db の SKU マスタ・db.js と同じ形)
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'amzB-widen-'));
const T1 = '2026-05-01T01:02:03.456Z', T2 = '2026-06-01T00:00:00.000Z';
const MASTERS = [['pr_a001', 'SKU マスタの 1', T1, T2], ['pr_pack2', 'SKU マスタの 2 個組', T1, T1], ['a003', '単品 3 を FBA でも', T1, T2]];
const COMPS = [['pr_a001', 'a001', 1, 0, T1, T1], ['pr_pack2', 'a001', 2, 0, T1, T1], ['pr_pack2', 'a002', 1, 1, T1, T2], ['a003', 'a003', 2, 0, T1, T1]];
function makeLegacy(dir, masters = MASTERS, comps = COMPS) {
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, 'warehouse.db');
  fs.rmSync(file, { force: true });
  const s = new Database(file);
  s.exec(`CREATE TABLE m_sku_master (seller_sku TEXT NOT NULL PRIMARY KEY, 商品名 TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, created_by TEXT, updated_by TEXT);
    CREATE TABLE m_sku_components (seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1, sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (seller_sku, ne_code));`);
  for (const m of masters) s.prepare('insert into m_sku_master values (?, ?, ?, ?, ?, ?)').run(m[0], m[1], m[2], m[3], 'legacy@test', null);
  for (const c of comps) s.prepare('insert into m_sku_components values (?, ?, ?, ?, ?, ?)').run(...c);
  s.close();
  return file;
}

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
/** 1 つの Company DB (0059 まで・ロール・本物の大きさでない材料のロード・段階 new_open = base) */
async function setupDb(base, { watcher = true } = {}) {   // watcher のログインは接続の上限がある = 要る DB だけ開く
  const name = `cdb_amzb_${crypto.randomBytes(4).toString('hex')}`;
  await admin.query(`create database ${name}`);
  dbNames.push(name);
  const u = new URL(url); u.pathname = `/${name}`;
  const roleUrl = (role, pw) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
  const open = async (role) => { const c = await openPgClient(role ? roleUrl(role, PW[role] ?? WPW[role]) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); clients.push(c); return c; };
  const O = await open(null), O2 = await open(null);
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: PW });
  await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  const [WA, GR, GM] = [watcher ? await open('watcher') : null, await open('master_gate_render'), await open('master_gate_minipc')];
  const r0 = await runInitialLoad(dbO, PLAN, { log: () => {}, runId: `amzb_load_${name}`, host: 'test' });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, base);
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const acks = async (prepared = null, active = base) => {
    for (const [host, inst] of [['render', 'r-a'], ['minipc', 'm-a']]) {
      await recordLegacyGateAckV2(dbGate[host], { host, instanceId: inst, buildId: 'b1', manifest: MANIFEST, ownership: active, phaseSeen: 'new_open',
        activeHashSeen: (await q('select active_hash from ops.master_ownership_state'))[0].active_hash, preparedHashSeen: prepared, capable: CAPABLE });
    }
  };
  await acks();   // manifest を DB に (prepare は「どれかのプロセスが見た一覧」だけ受ける)
  return { name, url: u.toString(), O, O2, dbO, dbO2: pgAdapter(O2), WA, dbWA: WA ? pgAdapter(WA) : null, q, acks };
}

/** 試みを作って、止める入口・ack・試みの中の 2 つのロードまで揃える。skuKind = prepared のロードの判断の記録 (undefined = 区分の食い違い 0) */
async function readyAttempt(E, { base, widen, stops, skuKind = undefined, beforeLoads = null }) {
  const a = await W.prepareWiden(E.dbO, { companyId: 1, map: widen, loaderFingerprint: hex('f'), manifest: MANIFEST, actor: 't' });
  for (const entryId of stops) await W.recordWidenManualStop(E.dbO, { attemptId: a.widen_prepare_id, entryId, stoppedBy: '中原' });
  if (beforeLoads) await beforeLoads(a);
  const last = (await E.q('select max(stopped_at) as t from ops.master_widen_manual_stops where widen_prepare_id = $1', [a.widen_prepare_id]))[0].t;
  const stopAt = new Date(last ?? a.prepared_at);
  await E.acks(ownershipHash(widen));
  const at = new Date(stopAt.getTime() + 1000).toISOString();
  const recovery = await fakeLoad(E.dbO, { epoch: 'active', hash: ownershipHash(base), completeAt: at });
  const prepared = await fakeLoad(E.dbO, { epoch: 'prepared', hash: ownershipHash(widen), completeAt: at, skuKind });
  return { ...a, id: a.widen_prepare_id, recovery, prepared, ev: { load_commit_seq: prepared.commitSeq, build_id: 'b1', generation_id: 'g1', generation_no: 1 } };
}
const check = (E, id) => W.widenCheck(E.dbWA, { attemptId: id, companyId: 1 });
const has = (r, re) => r.problems.some((x) => re.test(x));
/** 取引の中で mutate してから、読むだけの判定と apply の答えを比べる (apply は拒まれる = 取引ごと巻き戻す) */
async function variant(E, id, label, mutate, re) {
  await E.O.query('begin');
  try {
    await mutate(E.O);
    const r = (await E.O.query('select ops.widen_check_readonly($1::uuid, 1) as r', [id])).rows[0].r;
    assert.equal(r.ok, false, `${label}: 通ってしまった`);
    assert.ok(has(r, re), `${label}: ${JSON.stringify(r.problems)}`);
    await E.O.query('savepoint s');
    const e = await errOf(E.O, "select ops.widen_master_ownership($1::uuid, 1, 't', $2::jsonb)", [id, JSON.stringify({ load_commit_seq: '0', build_id: 'b', generation_id: 'g' })]);
    assert.ok(e && /widen_rejected/.test(e.message), `${label}: apply ${e?.message}`);
    assert.deepEqual(JSON.parse(e.detail).problems, r.problems, `${label}: 読むだけの判定と apply の答えが違う`);
    await E.O.query('rollback to savepoint s');
  } finally { await E.O.query('rollback'); }
}
const migrate = (E, legacyFile, attemptId = null, extra = {}) => {
  const legacy = M.readLegacyAmazonMaps(legacyFile);
  return M.runAmazonMapMigration(E.dbO, legacy, { mode: 'apply', expectHash: M.legacyDigest(legacy).content_hash, actor: 'naka@test', sheetOnly: [], attemptId, ...extra });
};
/** ack の行を直接足す (commit する・試験だけ)。fields = 列の上書き */
async function insertAck(E, attempt, fields) {
  const mh = (await E.q('select manifest_hash, active_hash, prepared_hash from ops.master_widen_attempts where widen_prepare_id = $1', [attempt]))[0];
  const f = { host: 'render', instance_id: 'r-x', build_id: 'b1', manifest_hash: mh.manifest_hash, owner_hash: mh.active_hash, phase_seen: 'new_open', inflight_count: 0, oldest_inflight_at: null,
    session_role: 'master_gate_render', ack_version: 2, active_hash_seen: mh.active_hash, prepared_hash_seen: mh.prepared_hash, capable: CAPABLE, acked_at: new Date().toISOString(), ...fields };
  if (f.ack_version === 1) { f.active_hash_seen = null; f.prepared_hash_seen = null; f.capable = null; }
  const cols = Object.keys(f);
  await E.O.query(`insert into ops.master_legacy_gate_acks (${cols.join(', ')}) values (${cols.map((k, i) => (k === 'capable' ? `$${i + 1}::text[]` : `$${i + 1}`)).join(', ')})`, cols.map((k) => f[k]));
}
/** PR-D: 10/8 の影運転で残る見込みの Sheet にだけある SKU (NE コードは空・他販路の売上 0) */
const SHEET_ONLY_SKU = 'pr_1272115_f_20231217_19336813_0004';
/** 10/8 の 2 件目: SKU マスタから消した後に Sheet の写しに大文字で残る (夜間ロードは正規化した小文字の出品を作る) */
const SHEET_ONLY_UPPER = 'pr_1272115_F_20220221_10927087_0001';
/**
 * 切替の前の夜間ロード (持ち主 load) が Sheet にだけある SKU から作った出品と自動の構成 (evidence fba_sheet) を、持ち主の接続で足す (#1651 Codex R1 High)。
 * 対応 (amazon_sku_maps) は無い = 0054 の書き手の守りの外。戻り値 = その出品の構成の行の数を返す関数
 */
async function seedSheetListing(E, listingCode, skuCode) {
  await E.O.query(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status) values (1, 'amazon', $1, $2, null, 'active')`, [AMZ, listingCode]);
  await E.O.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type, resolved_by_id, evidence)
    select 1, l.listing_id, k.sku_id, 1, 0, 'imported', 'system', 'load_t', '{"source":"fba_sheet"}'::jsonb from core.listings l, core.skus k
     where l.mall = 'amazon' and l.listing_code = $1 and k.code = $2`, [listingCode, skuCode]);
  return async () => (await E.q(`select count(*)::int as n from core.listing_components c join core.listings l on l.listing_id = c.listing_id where l.mall = 'amazon' and l.listing_code = $1`, [listingCode]))[0].n;
}
/** fba.db (Sheet の写し sku_mapping) を古い表の隣に作る。skus = Sheet の SKU (SKU マスタにある SKU を混ぜると数えない) */
function makeFba(dir, skus) {
  const fba = path.join(dir, 'fba.db');
  fs.rmSync(fba, { force: true });
  const d = new Database(fba); d.exec('create table sku_mapping (amazon_sku text)');
  for (const s of skus) d.prepare('insert into sku_mapping values (?)').run(s);
  d.close();
  return fba;
}
/** CLI (scripts/company-db/amazon-map-migrate.mjs --apply) を本物のプロセスで流す (#1648 Codex R2 Low 2)。COMPANY_DB_URL = 試験の DB。戻り値 { code, out } */
const CLI_MIGRATE = path.join(path.dirname(fileURLToPath(import.meta.url)), 'company-db', 'amazon-map-migrate.mjs');
function cliApply(E, legacyFile, extraArgs = []) {
  const fba = path.join(path.dirname(legacyFile), 'fba.db');
  if (!fs.existsSync(fba)) { const d = new Database(fba); d.exec('create table sku_mapping (amazon_sku text)'); d.close(); }
  const h0 = M.legacyDigest(M.readLegacyAmazonMaps(legacyFile)).content_hash;
  const r = spawnSync(process.execPath, [CLI_MIGRATE, '--apply', '--expect-hash', h0, '--legacy', legacyFile, '--fba-db', fba, '--actor', 'naka@test', '--yes', ...extraArgs],
    { env: { ...process.env, COMPANY_DB_URL: E.url }, encoding: 'utf8', timeout: 120000 });
  return { code: r.status, out: `${r.stdout || ''}${r.stderr || ''}` };
}
const phaseErr = (re) => (e) => e.code === 'AMAZON_MAP_MIGRATE_PHASE' && (!re || re.test(e.message));
/** 消えた対応を 1 件作る (trigger を止めて消す = 0054 の「普通の道では起きない」形) */
const loseOne = async (c) => {
  await c.query('alter table core.amazon_sku_maps disable trigger user');
  await c.query("delete from core.amazon_sku_maps where seller_sku = 'pr_pack2'");
  await c.query('alter table core.amazon_sku_maps enable trigger user');
};
/** active の対応を全部墓標にする (移行の書き手の印で・deferred の不変条件は commit の前なので巻き戻す取引の中だけ) */
const tombAll = async (c) => {
  await c.query("select set_config('core.source_system', 'amazon_map_migration', true), set_config('core.actor_type', 'system', true), set_config('core.actor_id', 't', true)");
  await c.query("update core.amazon_sku_maps set state = 'deleted', deleted_at = now(), deleted_by = 't', deleted_reason = '試験'");
};

try {
  // ═══ DB-A: 10/7 の本番の形 (sku_kind は company・Amazon は load) で Amazon だけを足す ═══
  const A = await setupDb(BASE_K);
  const WIDEN_A = { ...BASE_K, [KEY_A]: 'company' };
  const dirA = path.join(tmp, 'a');
  const legacyA = makeLegacy(dirA);
  let AT;   // 本番の試み

  await ta('[M0] 移行の apply: new_open で試みが無い = 断る (AMAZON_MAP_MIGRATE_PHASE・CLI も --attempt なしは断る)・広げてよいキーは 2 つ・ほかのキーは断る', async () => {
    assert.deepEqual((await A.q('select ops.master_widen_allowed_keys() as k'))[0].k, [KEY_A, KEY_K]);
    await assert.rejects(migrate(A, legacyA), phaseErr(/--attempt/));                          // 試みを指さない
    const c0 = cliApply(A, legacyA);                                                             // CLI も (new_open は --attempt が必須 = 断る・何も書かない)
    assert.equal(c0.code, 1, c0.out); assert.match(c0.out, /--attempt/);
    await assert.rejects(migrate(A, legacyA, crypto.randomUUID()), phaseErr(/attempt_missing/));   // 知らない試み
    await assert.rejects(W.prepareWiden(A.dbO, { companyId: 1, map: { ...WIDEN_A, 'products.status': 'company' }, loaderFingerprint: hex('f'), manifest: MANIFEST, actor: 't' }), /widen_key_not_allowed/);
    assert.equal((await A.q('select count(*)::int as n from core.amazon_sku_maps'))[0].n, 0);
  });

  await ta('[A1] Amazon だけの試み: 止める手の入口 = gas:logizard-sheet-and-sku-map・停止の前の移行は断る・停止の後は通る (試みの窓)・移行の前の判定 = active 0 件で断る', async () => {
    let id = null;
    AT = await readyAttempt(A, { base: BASE_K, widen: WIDEN_A, stops: [], skuKind: { format: 'sku-kind-v1', held: ['a002'], unverifiable: [] },
      beforeLoads: async (a) => {
        id = a.widen_prepare_id;
        assert.deepEqual(a.added_keys, [KEY_A]);
        assert.deepEqual(a.required_manual_entries, ['gas:logizard-sheet-and-sku-map']);
        // 停止の前・ack の前 = 窓でない (widen の判定と同じ共通の部品)
        await assert.rejects(migrate(A, legacyA, a.widen_prepare_id), phaseErr(/記録が prepare の前.*manual_stops: 止めた手の入口 \(\) が要る入口 \(gas:logizard-sheet-and-sku-map\)/));
        await W.recordWidenManualStop(A.dbO, { attemptId: a.widen_prepare_id, entryId: 'gas:logizard-sheet-and-sku-map', stoppedBy: '中原' });
      } });
    assert.equal(AT.id, id);
    // ne:set-kind はこの試みの入口でない = 記録できない
    await assert.rejects(W.recordWidenManualStop(A.dbO, { attemptId: AT.id, entryId: 'ne:set-kind', stoppedBy: 't' }), /entry_not_required/);
    const r = await check(A, AT.id);
    assert.equal(r.ok, false);
    assert.ok(has(r, /active の対応が 0 件/), JSON.stringify(r.problems));
    assert.ok(!has(r, /decisions|shape/), `区分の検査で止まらない: ${JSON.stringify(r.problems)}`);
    assert.deepEqual([r.counts.amazon_map_active, r.counts.amazon_map_lost, r.counts.held, r.counts.single_product_mismatch], [0, 0, undefined, undefined]);
  });

  await ta('[M1] 移行の apply は試みの窓で通る (H0 と同じ・widen_attempt を返す)・Sheet にだけある SKU は止めない (PR-D)・2 回目は断る (EXISTS)', async () => {
    const compsA = await seedSheetListing(A, SHEET_ONLY_SKU, 'a002');   // #1651 Codex R1 High: 切替の前の夜間ロードが Sheet から作った構成
    assert.equal(await compsA(), 1);
    const r = await migrate(A, legacyA, AT.id, { sheetOnly: [SHEET_ONLY_SKU] });
    assert.equal(r.committed, true); assert.equal(r.subset.match, true); assert.equal(r.widen_attempt, AT.id);
    assert.equal(r.blocker_total, 0); assert.deepEqual(r.warnings.sheet_only, { count: 1, samples: [{ seller_sku: SHEET_ONLY_SKU }] });
    assert.deepEqual([r.counts.sheet_only_removed, r.sheet_only_cleanup.left_after], [1, 0]);   // 段階 new_open (0054 の書き手の守りの中) でも消せる
    assert.equal(await compsA(), 0);
    assert.equal((await A.q("select count(*)::int as n from core.amazon_sku_maps where state = 'active' and origin = 'legacy'"))[0].n, 3);
    await assert.rejects(migrate(A, legacyA, AT.id), (e) => e.code === 'AMAZON_MAP_MIGRATE_EXISTS');
  });

  await ta('[M3] 移行の窓 = widen と同じ ack の条件 (#1648 Codex R1 Medium 1): 書きかけ・黙っている (古い)・1 版・prepare の前・見た prepared が違う・capable に無い = 断る / 直せば窓 (= EXISTS まで進む)', async () => {
    const at = (ms) => new Date(Date.now() + ms).toISOString();
    const prepAt = (await A.q('select prepared_at from ops.master_widen_attempts where widen_prepare_id = $1', [AT.id]))[0].prepared_at;
    const cases = [
      ['書きかけ', { instance_id: 'r-w', inflight_count: 2, oldest_inflight_at: at(-1000) }, /r-w: 書きかけが 2 件/],
      ['黙っている (古い ack)', { instance_id: 'r-old', acked_at: at(-20 * 60000) }, /r-old: 黙っている/],
      ['1 版の ack', { instance_id: 'r-v1', ack_version: 1 }, /r-v1: 記録が 1 版/],
      ['prepare の前の ack', { instance_id: 'r-pre', acked_at: new Date(new Date(prepAt).getTime() - 1000).toISOString() }, /r-pre: 記録が prepare の前/],
      ['見た prepared が違う', { instance_id: 'r-ph', prepared_hash_seen: hex('d') }, /r-ph: 見た prepared が試みのものでない/],
      ['capable に Amazon が無い', { instance_id: 'r-cap', capable: [KEY_K] }, /r-cap: build が足すキー .* を company にできない/],
    ];
    for (const [label, fields, re] of cases) {
      await insertAck(A, AT.id, fields);
      await assert.rejects(migrate(A, legacyA, AT.id), phaseErr(re), label);
      const w = (await A.q('select ops.amazon_map_migration_window($1::uuid) as r', [AT.id]))[0].r;
      assert.equal(w.ok, false, label);
      assert.deepEqual(w.problems, (await check(A, AT.id)).problems.filter((p) => !/^amazon_map|^loads|^material|^decisions|^shape/.test(p)), `${label}: widen の判定と同じ`);
      // 直す = そのプロセスの新しい記録 (止めたプロセス = stopped)
      await insertAck(A, AT.id, { instance_id: fields.instance_id, stopped: true, stopped_reason: '試験で止めた', inflight_count: 0, oldest_inflight_at: null, acked_at: at(0) });
    }
    // 全部直した = 窓 (= 窓の後の「もう対応がある」まで進む)
    await assert.rejects(migrate(A, legacyA, AT.id), (e) => e.code === 'AMAZON_MAP_MIGRATE_EXISTS');
    assert.equal((await A.q('select ops.amazon_map_migration_window($1::uuid) as r', [AT.id]))[0].r.ok, true);
    // ack が足りない = 場所 (minipc) の新しい記録が無い (全部止めた記録)
    await insertAck(A, AT.id, { host: 'minipc', instance_id: 'm-a', session_role: 'master_gate_minipc', stopped: true, stopped_reason: '試験', acked_at: at(0) });
    await assert.rejects(migrate(A, legacyA, AT.id), phaseErr(/ack: minipc: .* 分以内の記録が無い/));
    await A.acks(ownershipHash(WIDEN_A), BASE_K);   // 戻す (render・minipc の新しい 2 版)
    await assert.rejects(migrate(A, legacyA, AT.id), (e) => e.code === 'AMAZON_MAP_MIGRATE_EXISTS');
  });

  await ta('[A2] Amazon だけの試み: 区分の食い違い (held 1 件) があっても通る・数は Amazon の 3 つだけ / 消えた対応 1 件 = 断る / active 0 件 = 断る (読むだけと apply が同じ答え)', async () => {
    const r = await check(A, AT.id);
    assert.equal(r.ok, true, JSON.stringify(r.problems));
    assert.deepEqual([r.counts.amazon_map_active, r.counts.amazon_map_active_components, r.counts.amazon_map_lost], [3, 4, 0]);
    assert.equal(r.counts.held, undefined); assert.equal(r.counts.single_product_mismatch, undefined);
    await variant(A, AT.id, '消えた対応 1 件', loseOne, /消えた対応が 1 件/);
    await variant(A, AT.id, 'active 0 件', tombAll, /active の対応が 0 件/);
  });

  await ta('[A3] CLI の check: 古い表と Company DB のハッシュを照らす (同じ = ok・古い表を変えた = ok: false・DATA_DIR が無い = ok: false)', async () => {
    const run = async (args) => { const out = []; const code = await epochCli(['check', '--attempt', AT.id, '--company', '1', ...args], { env: { COMPANY_DB_WATCH_URL: 'postgres://watcher@localhost/x' }, connect: async () => ({ db: A.dbWA }), log: (m) => out.push(m) }); return { code, r: JSON.parse(out.join('\n')) }; };
    let x = await run(['--data-dir', dirA]);
    assert.equal(x.code, 0, JSON.stringify(x.r.problems)); assert.equal(x.r.amazon_map.match, true);
    assert.equal(x.r.amazon_map.legacy_hash, M.legacyDigest(M.readLegacyAmazonMaps(legacyA)).content_hash);
    const dirB = path.join(tmp, 'a-changed');
    makeLegacy(dirB, MASTERS, COMPS.map((c) => (c[0] === 'a003' ? [c[0], c[1], 3, c[3], c[4], c[5]] : c)));   // 古い表だけ数量が違う
    x = await run(['--data-dir', dirB]);
    assert.equal(x.code, 1); assert.ok(has(x.r, /amazon_map_hash: 古い表 .* のハッシュが違う/), JSON.stringify(x.r.problems));
    x = await run([]);
    assert.equal(x.code, 1); assert.ok(has(x.r, /amazon_map_hash: 照らせない .*DATA_DIR/), JSON.stringify(x.r.problems));
  });

  await ta('[A4] widen の写しの証拠: amazon_map が無い・ハッシュの形・古い表 ≠ Company DB・行の数が DB と違う = 断る / CLI の段 (鍵の後に読む) で古い表が違えば DB を呼ばずに断る', async () => {
    const H = M.legacyDigest(M.readLegacyAmazonMaps(legacyA)).content_hash;
    const good = { legacy_hash: H, company_hash: H, master_rows: 3, component_rows: 4 };
    const rej = (amazon_map, re) => assert.rejects(W.widenOwnership(A.dbO, { attemptId: AT.id, companyId: 1, actor: 't', evidence: { ...AT.ev, ...(amazon_map === undefined ? {} : { amazon_map }) } }), re);
    await rej(undefined, /evidence_invalid: Amazon SKU の対応を足すには/);
    await rej({ ...good, legacy_hash: 'x' }, /evidence_invalid: Amazon SKU の対応を足すには/);
    await rej({ ...good, company_hash: hex('e') }, /evidence_invalid: 古い表のハッシュ .* と違う/);
    await rej({ ...good, master_rows: 2 }, /evidence_invalid: ハッシュを作った行の数/);
    await rej({ ...good, component_rows: '5' }, /evidence_invalid: ハッシュを作った行の数/);
    await rej({ ...good, master_rows: 3.5 }, /evidence_invalid: ハッシュを作った行の数/);
    // CLI の段: 古い表が違う = 鍵の後に Company DB を読んで照らし、DB の関数を呼ばない (試みは prepared のまま)
    const dirB = path.join(tmp, 'a-changed2');
    const step = M.amazonWidenEvidenceStep(M.readLegacyAmazonMaps(makeLegacy(dirB, [...MASTERS, ['zz_new', '後から足した', T1, T1]], [...COMPS, ['zz_new', 'a004', 1, 0, T1, T1]])));
    await assert.rejects(W.widenOwnership(A.dbO, { attemptId: AT.id, companyId: 1, actor: 't', evidence: AT.ev, beforeCall: step.beforeCall }), (e) => e.code === 'AMAZON_MAP_HASH_MISMATCH');
    assert.equal(step.result().match, false);
    assert.equal((await A.q('select state from ops.master_widen_attempts where widen_prepare_id = $1', [AT.id]))[0].state, 'prepared');
    // 鍵の後に読んだ = 夜間ロード (epoch の共有の鍵) の最中は 5 秒で諦める (読む前に)
    await A.O2.query('begin'); await A.O2.query('select pg_advisory_xact_lock_shared(ops.master_ownership_lock_key())');
    let called = false;
    try {
      await assert.rejects(W.widenOwnership(A.dbO, { attemptId: AT.id, companyId: 1, actor: 't', evidence: AT.ev, lockTimeout: '500ms', beforeCall: async () => { called = true; return {}; } }), (e) => e.code === '55P03');
    } finally { await A.O2.query('commit'); }
    assert.equal(called, false, '鍵を取れないときは Company DB を読まない');
  });

  await ta('[A5] widen が通る (CLI の段 = 鍵の後にハッシュを照らす)・Amazon が company・sku_kind は company のまま・移行は試みが閉じたら断る', async () => {
    const before = await check(A, AT.id);
    assert.equal(before.ok, true, JSON.stringify(before.problems));
    const step = M.amazonWidenEvidenceStep(M.readLegacyAmazonMaps(legacyA));
    const r = await W.widenOwnership(A.dbO, { attemptId: AT.id, companyId: 1, actor: '中原', evidence: AT.ev, beforeCall: step.beforeCall });
    assert.deepEqual([r.widened, r.added_keys], [true, [KEY_A]]);
    assert.deepEqual(r.counts, before.counts);   // 読むだけの判定と同じ数
    assert.equal(step.result().match, true);
    const st = await OS.readOwnershipState(A.dbO);
    assert.deepEqual([st.active.map[KEY_A], st.active.map[KEY_K], st.prepared], ['company', 'company', null]);
    assert.deepEqual(await A.q('select phase, owner_hash from ops.master_cutover_state'), [{ phase: 'new_open', owner_hash: ownershipHash(WIDEN_A) }]);
    const ev = (await A.q("select detail -> 'evidence' -> 'amazon_map' as m from ops.master_widen_events where widen_prepare_id = $1 and action = 'widen'", [AT.id]))[0].m;
    assert.deepEqual(ev, { legacy_hash: step.result().legacy_hash, company_hash: step.result().company_hash, master_rows: 3, component_rows: 4 });
    assert.equal((await A.q('select ops.sku_kind_locked() as l'))[0].l, true);   // 区分の守りはそのまま
    await assert.rejects(migrate(A, legacyA, AT.id), phaseErr(/attempt_not_prepared/));   // widen の後 = 閉じた試み = 断る (EXISTS より前)
  });

  // ═══ DB-B: sku_kind も Amazon も load (0058 の前提の形) で、sku_kind だけの試み・両方の試み・移行の段階の条件 ═══
  const B = await setupDb(BASE0);
  const dirB = path.join(tmp, 'b');
  const legacyB = makeLegacy(dirB);
  const WIDEN_K = { ...BASE0, [KEY_K]: 'company' }, WIDEN_KA = { ...BASE0, [KEY_K]: 'company', [KEY_A]: 'company' };

  let KCANCELLED = null;
  await ta('[K1] sku_kind だけの試み (今までどおり): held 1 件 = 断る・最終形を数える / Amazon の数は見ない (active 0・消えた対応があっても止まらない) / 移行の apply は断る', async () => {
    const K = await readyAttempt(B, { base: BASE0, widen: WIDEN_K, stops: ['ne:set-kind'], skuKind: { format: 'sku-kind-v1', held: ['a002'], unverifiable: [] } });
    assert.deepEqual(K.required_manual_entries, ['ne:set-kind']);
    const r = await check(B, K.id);
    assert.equal(r.ok, false);
    assert.deepEqual(r.problems, ['decisions: sku_kind.held が 1 件 (区分の食い違いが残っている = 0 だけ)']);
    assert.deepEqual([r.counts.held, r.counts.single_product_mismatch, r.counts.non_set_parent_components, r.counts.amazon_map_active], [1, 0, 0, undefined]);
    await variant(B, K.id, '最終形 (セットに product_id)', (c) => c.query("update core.skus set product_id = (select p.product_id from core.products p where p.display_code = 'a005') where code = 'aset1'"), /区分と product_id の不整合が 1 件/);
    // 移行: sku_kind だけの試み = 窓でない
    await assert.rejects(migrate(B, legacyB, K.id), phaseErr(/not_amazon: .*listing_components\.amazon が無い/));
    await W.cancelWiden(B.dbO, { attemptId: K.id, actor: 't' });
    KCANCELLED = K.id;
    // held 0 の試みは Amazon の対応が 0 件でも通る (Amazon を足さない)
    const K2 = await readyAttempt(B, { base: BASE0, widen: WIDEN_K, stops: ['ne:set-kind'] });
    const r2 = await check(B, K2.id);
    assert.equal(r2.ok, true, JSON.stringify(r2.problems));
    assert.equal((await B.q('select count(*)::int as n from core.amazon_sku_maps'))[0].n, 0);
    // 消えた対応が 1 件あっても sku_kind だけの試みは止まらない (Amazon の数を見ない)
    await B.O.query('begin');
    try {
      await B.O.query("select set_config('core.source_system', 'amazon_map_migration', true), set_config('core.actor_type', 'system', true), set_config('core.actor_id', 't', true)");
      await B.O.query(`insert into core.amazon_sku_maps (listing_id, company_id, seller_sku, name, state, origin, registered_at, changed_at)
        select listing_id, 1, 'pr_a001', 'x', 'active', 'legacy', now(), now() from core.listings where listing_code = 'pr_a001'`);
      await B.O.query('set constraints all immediate');   // deferred の不変条件を先に流す (待っている trigger があると alter table できない)
      await B.O.query('alter table core.amazon_sku_maps disable trigger user');
      await B.O.query("delete from core.amazon_sku_maps where seller_sku = 'pr_a001'");
      await B.O.query('alter table core.amazon_sku_maps enable trigger user');
      assert.equal((await B.O.query('select count(*)::int as n from ops.amazon_map_lost_listings()')).rows[0].n, 1);
      const r3 = (await B.O.query('select ops.widen_check_readonly($1::uuid, 1) as r', [K2.id])).rows[0].r;
      assert.equal(r3.ok, true, JSON.stringify(r3.problems)); assert.equal(r3.counts.amazon_map_lost, undefined);
    } finally { await B.O.query('rollback'); }
    await W.cancelWiden(B.dbO, { attemptId: K2.id, actor: 't' });
  });

  let KA;
  await ta('[KA1] 両方の試み: 止める入口 2 つ・片方だけ止めた移行は断る・区分 (held) と Amazon (active 0) の両方で断る', async () => {
    KA = await readyAttempt(B, { base: BASE0, widen: WIDEN_KA, stops: ['ne:set-kind'], skuKind: { format: 'sku-kind-v1', held: ['a002'], unverifiable: [] },
      beforeLoads: async (a) => {
        assert.deepEqual(a.required_manual_entries, ['gas:logizard-sheet-and-sku-map', 'ne:set-kind']);
        await assert.rejects(migrate(B, legacyB, a.widen_prepare_id), phaseErr(/manual_stops: 止めた手の入口 \(ne:set-kind\)/));   // Amazon の入口を止める前
        await W.recordWidenManualStop(B.dbO, { attemptId: a.widen_prepare_id, entryId: 'gas:logizard-sheet-and-sku-map', stoppedBy: '中原' });
      } });
    const r = await check(B, KA.id);
    assert.equal(r.ok, false);
    assert.ok(has(r, /held が 1 件/) && has(r, /active の対応が 0 件/), JSON.stringify(r.problems));
    assert.equal(r.problems.length, 2, JSON.stringify(r.problems));
  });

  await ta('[M2] 移行は epoch の共有の鍵を取る = 試みの cancel (epoch の排他) を持つ取引の間は待つ・frozen は今までどおり (CLI で --attempt なしで通る)', async () => {
    await assert.rejects(migrate(B, legacyB, KCANCELLED), phaseErr(/attempt_not_prepared/));   // 違う試み (cancel した前の試み) を指す = 断る
    await B.O2.query('begin'); await B.O2.query('select pg_advisory_xact_lock(ops.master_ownership_lock_key())');
    const m = launch(migrate(B, legacyB, KA.id));
    await sleep(600);
    const waited = !m.done;
    await B.O2.query('rollback');
    const mr = await m.promise;
    assert.equal(waited, true, '移行は epoch の排他の鍵を待つ');
    assert.ok(mr.ok, mr.err?.message);
    assert.equal(mr.ok.committed, true); assert.equal(mr.ok.widen_attempt, KA.id);
    // frozen (今までどおり): 別の DB を frozen にして移行が通る (試みは見ない)
    const F = await setupDb(BASE0, { watcher: false });
    await F.O.query('begin'); await F.O.query("select set_config('ops.cutover_protocol', '1', true)"); await F.O.query("update ops.master_cutover_state set phase = 'frozen', owner_hash = null where id = 1"); await F.O.query('commit');
    // PR-D: CLI の影運転 (本物のプロセス) = Sheet にだけある SKU があっても止める項目 0・終了コード 0 (気をつける項目に出す) /
    //   ほかの止める項目 (構成なし) があれば今までどおり終了コード 1。影運転の先 = F・本番の代わり = DB-A (DB 名が違う = 通る)
    const dirF = path.join(tmp, 'f');
    const legacyF = makeLegacy(dirF);
    const fbaF = makeFba(dirF, [SHEET_ONLY_SKU, 'PR_A001']);   // PR_A001 = 正規化で SKU マスタにある = 数えない
    const compsF = await seedSheetListing(F, SHEET_ONLY_SKU, 'a002');   // #1651 Codex R1 High
    const shadow = (legacyFile, fba) => spawnSync(process.execPath, [CLI_MIGRATE, '--shadow', '--db-url', F.url, '--legacy', legacyFile, '--fba-db', fba],
      { env: { ...process.env, COMPANY_DB_URL: A.url }, encoding: 'utf8', timeout: 120000 });
    const sh = shadow(legacyF, fbaF);
    const shOut = `${sh.stdout || ''}${sh.stderr || ''}`;
    assert.equal(sh.status, 0, shOut);
    assert.match(shOut, /切替を止める項目: 0 件 \(止まる SKU 0\)/); assert.match(shOut, new RegExp(`気をつける: sheet_only 1 件 例 \\[\\{"seller_sku":"${SHEET_ONLY_SKU}"\\}\\]`));
    assert.match(shOut, /→ 一致/); assert.match(shOut, /巻き戻した \(影運転\)/);
    assert.match(shOut, /Sheet にだけある SKU の出品の自動の構成: 消す予定 1 行 \(出品 1\) 例 .*"sku":"a002".*・消した後に残る 0 行/);
    assert.equal(await compsF(), 1);   // 影運転は巻き戻す
    const dirFb = path.join(tmp, 'f-bad');
    const legacyFb = makeLegacy(dirFb, [...MASTERS, ['empty1', '構成なし', T1, T1]], COMPS);
    const shb = shadow(legacyFb, makeFba(dirFb, [SHEET_ONLY_SKU]));
    const shbOut = `${shb.stdout || ''}${shb.stderr || ''}`;
    assert.equal(shb.status, 1, shbOut);
    assert.match(shbOut, /切替を止める項目: 1 件 \(止まる SKU 1\)\s+no_components: 1 件/); assert.match(shbOut, /気をつける: sheet_only 1 件/);
    assert.doesNotMatch(shbOut.split('気をつける')[0], /sheet_only/);   // 止める項目の側には出ない
    assert.equal((await F.q('select count(*)::int as n from core.amazon_sku_maps'))[0].n, 0);   // 影運転は巻き戻す
    // #1651 Codex R1 High: 人が確定した行 (manual) = 止める (終了コード 1・何も書かない)
    await F.O.query(`update core.listing_components set resolution = 'manual' where listing_id = (select listing_id from core.listings where mall = 'amazon' and listing_code = $1)`, [SHEET_ONLY_SKU]);
    const fm = cliApply(F, legacyF);
    assert.equal(fm.code, 1, fm.out); assert.match(fm.out, /sheet_only_manual 1/);
    assert.equal((await F.q('select count(*)::int as n from core.amazon_sku_maps'))[0].n, 0); assert.equal(await compsF(), 1);
    await F.O.query(`update core.listing_components set resolution = 'imported' where listing_id = (select listing_id from core.listings where mall = 'amazon' and listing_code = $1)`, [SHEET_ONLY_SKU]);
    const fc = cliApply(F, legacyF);                                     // CLI で --attempt なし (#1648 Codex R2 Low 2)・fba.db に Sheet にだけある SKU (PR-D)
    assert.equal(fc.code, 0, fc.out); assert.match(fc.out, /✅ 移した \(commit\)/);
    assert.match(fc.out, /切替を止める項目: 0 件/); assert.match(fc.out, /気をつける: sheet_only 1 件/);
    assert.match(fc.out, /Sheet にだけある SKU の出品の自動の構成: 消した 1 行 \(出品 1\)/); assert.equal(await compsF(), 0);
    assert.equal((await F.q("select count(*)::int as n from core.amazon_sku_maps where state = 'active'"))[0].n, 3);
  });

  await ta('[KA2] 両方の試み: 移行の後も held が残れば断る (Amazon は通る)・消えた対応も断る / held 0 の試みで両方そろえば widen が通る', async () => {
    let r = await check(B, KA.id);
    assert.deepEqual(r.problems, ['decisions: sku_kind.held が 1 件 (区分の食い違いが残っている = 0 だけ)']);
    await W.cancelWiden(B.dbO, { attemptId: KA.id, actor: 't' });
    const KA2 = await readyAttempt(B, { base: BASE0, widen: WIDEN_KA, stops: ['gas:logizard-sheet-and-sku-map', 'ne:set-kind'] });
    r = await check(B, KA2.id);
    assert.equal(r.ok, true, JSON.stringify(r.problems));
    assert.deepEqual([r.counts.held, r.counts.amazon_map_active, r.counts.single_product_mismatch], [0, 3, 0]);
    await variant(B, KA2.id, '消えた対応 1 件', loseOne, /消えた対応が 1 件/);
    const step = M.amazonWidenEvidenceStep(M.readLegacyAmazonMaps(legacyB));
    const w = await W.widenOwnership(B.dbO, { attemptId: KA2.id, companyId: 1, actor: '中原', evidence: KA2.ev, beforeCall: step.beforeCall });
    assert.deepEqual(w.added_keys, [KEY_A, KEY_K]);
    const st = await OS.readOwnershipState(B.dbO);
    assert.deepEqual([st.active.map[KEY_A], st.active.map[KEY_K]], ['company', 'company']);
    assert.equal((await B.q('select ops.sku_kind_locked() as l'))[0].l, true);
  });

  try { await B.WA.end(); } catch { /* */ }   // watcher の接続の上限 (DB-B はもう読まない)
  // ═══ DB-C: 移行の後に cancel → 古い表を直す → 新しい試み → reconcile → check → widen (#1648 Codex R1 Medium 2・中原さんの決定 b) ═══
  const Cdb = await setupDb(BASE_K);
  const dirC = path.join(tmp, 'c');
  const legacyC1 = makeLegacy(dirC);
  // 古い表の 2 つ目: 足す (pr_new3 = 出品なし)・直す (a003 の数量 2 → 3・名前)・消す (pr_pack2)
  const MASTERS_C2 = [['pr_a001', 'SKU マスタの 1', T1, T2], ['a003', '単品 3 を FBA でも (直した)', T1, '2026-07-01T00:00:00.000Z'], ['pr_new3', '新しい組 3', T2, T2]];
  const COMPS_C2 = [['pr_a001', 'a001', 1, 0, T1, T1], ['a003', 'a003', 3, 0, T1, '2026-07-01T00:00:00.000Z'], ['pr_new3', 'a005', 1, 0, T2, T2], ['pr_new3', 'a006', 2, 1, T2, T2]];
  const dirC2 = path.join(tmp, 'c2');
  const legacyC2 = makeLegacy(dirC2, MASTERS_C2, COMPS_C2);
  const cdbHash = async (E) => K0.skuMapDigest(await A0.readCompanyAmazonMapCanon(E.dbO)).content_hash;
  const lhash = (file) => M.legacyDigest(M.readLegacyAmazonMaps(file)).content_hash;
  const reconcile = (E, file, attemptId, extra = {}) => M.runAmazonMapMigration(E.dbO, M.readLegacyAmazonMaps(file),
    { mode: 'reconcile', expectHash: lhash(file), actor: 'naka@test', sheetOnly: [], attemptId, ...extra });
  let C1, C2;

  await ta('[RC1] reconcile の準備: 移行 → cancel (対応は Company DB に残る) → 古い表を直す → 次の apply は EXISTS で断る (今までどおり) / 窓の外・空の DB の reconcile は断る', async () => {
    // 空の DB (DB-B の前の形) の reconcile = 合わせ直すものが無い (EMPTY)・窓の外 = PHASE
    C1 = await readyAttempt(Cdb, { base: BASE_K, widen: { ...BASE_K, [KEY_A]: 'company' }, stops: ['gas:logizard-sheet-and-sku-map'] });
    await assert.rejects(reconcile(Cdb, legacyC1, C1.id, { expectCdbHash: await cdbHash(Cdb) }), (e) => e.code === 'AMAZON_MAP_RECONCILE_EMPTY');
    const r = await migrate(Cdb, legacyC1, C1.id);
    assert.equal(r.committed, true);
    await W.cancelWiden(Cdb.dbO, { attemptId: C1.id, actor: 't' });
    assert.equal((await Cdb.q("select count(*)::int as n from core.amazon_sku_maps where state = 'active'"))[0].n, 3);
    // 窓の外 (cancel した試み・試みを指さない) = 断る
    await assert.rejects(reconcile(Cdb, legacyC2, C1.id, { expectCdbHash: await cdbHash(Cdb) }), phaseErr(/attempt_not_prepared/));
    await assert.rejects(reconcile(Cdb, legacyC2, null, { expectCdbHash: await cdbHash(Cdb) }), phaseErr(/--attempt/));
    // 新しい試み (窓) を開く
    C2 = await readyAttempt(Cdb, { base: BASE_K, widen: { ...BASE_K, [KEY_A]: 'company' }, stops: ['gas:logizard-sheet-and-sku-map'] });
    // 今までどおり: 対応がある DB の apply は EXISTS (reconcile のために外していない)
    await assert.rejects(migrate(Cdb, legacyC2, C2.id), (e) => e.code === 'AMAZON_MAP_MIGRATE_EXISTS');
    // 古い表が変わった = 判定は通っても (active 3・lost 0) CLI の check はハッシュ違い = widen できない
    const ck = await check(Cdb, C2.id);
    assert.equal(ck.ok, true, JSON.stringify(ck.problems));
    const out = []; const code = await epochCli(['check', '--attempt', C2.id, '--company', '1', '--data-dir', dirC2], { env: { COMPANY_DB_WATCH_URL: 'x' }, connect: async () => ({ db: Cdb.dbWA }), log: (m) => out.push(m) });
    assert.equal(code, 1); assert.ok(JSON.parse(out.join('\n')).problems.some((p) => /amazon_map_hash/.test(p)));
    const step = M.amazonWidenEvidenceStep(M.readLegacyAmazonMaps(legacyC2));
    await assert.rejects(W.widenOwnership(Cdb.dbO, { attemptId: C2.id, companyId: 1, actor: 't', evidence: C2.ev, beforeCall: step.beforeCall }), (e) => e.code === 'AMAZON_MAP_HASH_MISMATCH');
  });

  await ta('[RC2] reconcile が断る: 今の Company DB のハッシュが違う・止める項目・frozen でない窓・portal の行・合わせた後のハッシュが違う (巻き戻す) = どれも何も書かない', async () => {
    const h0 = await cdbHash(Cdb);
    await assert.rejects(reconcile(Cdb, legacyC2, C2.id, { expectCdbHash: hex('e') }), (e) => e.code === 'AMAZON_MAP_RECONCILE_CDB_HASH');
    await assert.rejects(reconcile(Cdb, legacyC2, C2.id, {}), (e) => e.code === 'AMAZON_MAP_MIGRATE_INVALID');   // --expect-cdb-hash が無い
    const bad = makeLegacy(path.join(tmp, 'c-bad'), [...MASTERS_C2, ['bad_ne', 'NE に無い', T1, T1]], [...COMPS_C2, ['bad_ne', 'nosuch', 1, 0, T1, T1]]);
    await assert.rejects(reconcile(Cdb, bad, C2.id, { expectCdbHash: h0 }), (e) => e.code === 'AMAZON_MAP_MIGRATE_BLOCKED');
    const gap = makeLegacy(path.join(tmp, 'c-gap'), MASTERS_C2, COMPS_C2.map((c) => (c[0] === 'pr_new3' && c[1] === 'a006' ? [c[0], c[1], c[2], 2, c[4], c[5]] : c)));
    await assert.rejects(reconcile(Cdb, gap, C2.id, { expectCdbHash: h0 }), (e) => e.code === 'AMAZON_MAP_MIGRATE_BLOCKED' && !!e.blockers.sort_gap);
    // portal の行 (load の間は無いはず) = 断る
    const setOrigin = async (o) => { await Cdb.O2.query('begin'); await Cdb.O2.query("select set_config('core.source_system', 'amazon_map_migration', true)"); await Cdb.O2.query("update core.amazon_sku_maps set origin = $1 where seller_sku = 'pr_a001'", [o]); await Cdb.O2.query('commit'); };
    await setOrigin('portal');
    try { await assert.rejects(reconcile(Cdb, legacyC2, C2.id, { expectCdbHash: h0 }), (e) => e.code === 'AMAZON_MAP_RECONCILE_PORTAL'); } finally { await setOrigin('legacy'); }
    const ev1 = (await Cdb.q('select count(*)::int as n from events.master_change_events'))[0].n;
    // 合わせた後のハッシュが違う (書いた後に取引の中で 1 行を変える = 試験だけの afterWrite) = 巻き戻す
    await assert.rejects(reconcile(Cdb, legacyC2, C2.id, { expectCdbHash: h0, afterWrite: (db) => db.query("update core.amazon_sku_maps set name = name || 'x' where seller_sku = 'pr_a001'") }),
      (e) => e.code === 'AMAZON_MAP_MIGRATE_MISMATCH');
    assert.equal(await cdbHash(Cdb), h0, '何も書いていない');
    assert.equal((await Cdb.q('select count(*)::int as n from events.master_change_events'))[0].n, ev1, '巻き戻した = 変更の記録も増えない');
    // frozen の DB の reconcile = 断る (frozen の道は apply)
    const F2 = await setupDb(BASE0, { watcher: false });
    await F2.O.query('begin'); await F2.O.query("select set_config('ops.cutover_protocol', '1', true)"); await F2.O.query("update ops.master_cutover_state set phase = 'frozen', owner_hash = null where id = 1"); await F2.O.query('commit');
    await assert.rejects(reconcile(F2, legacyC1, C2.id, { expectCdbHash: h0 }), phaseErr(/new_open/));
  });

  await ta('[RC3] reconcile が通る: 足す・直す・消す (墓標) → Company DB のハッシュ = 今の古い表・消えた対応 0 (墓標は行が残る)・判定 ok / 2 回目は何も書かない / 消した SKU を古い表に戻す = 墓標を戻す', async () => {
    const r = await reconcile(Cdb, legacyC2, C2.id, { expectCdbHash: await cdbHash(Cdb) });
    assert.equal(r.committed, true); assert.equal(r.widen_attempt, C2.id);
    assert.deepEqual([r.counts.maps_inserted, r.counts.maps_updated, r.counts.maps_tombstoned, r.counts.listings_created], [1, 1, 1, 1]);
    assert.equal(await cdbHash(Cdb), lhash(legacyC2));
    assert.deepEqual(await Cdb.q("select seller_sku, state, origin, deleted_reason is not null as r from core.amazon_sku_maps order by seller_sku"),
      [{ seller_sku: 'a003', state: 'active', origin: 'legacy', r: false }, { seller_sku: 'pr_a001', state: 'active', origin: 'legacy', r: false },
        { seller_sku: 'pr_new3', state: 'active', origin: 'legacy', r: false }, { seller_sku: 'pr_pack2', state: 'deleted', origin: 'legacy', r: true }]);
    assert.equal((await Cdb.q("select count(*)::int as n from core.listing_components c join core.amazon_sku_maps m using (listing_id) where m.seller_sku = 'pr_pack2'"))[0].n, 0, '墓標に構成は残さない');
    assert.equal((await Cdb.q('select count(*)::int as n from ops.amazon_map_lost_listings()'))[0].n, 0, '墓標は消えた対応 (lost) にならない');
    const ck = await check(Cdb, C2.id);
    assert.equal(ck.ok, true, JSON.stringify(ck.problems));
    assert.deepEqual([ck.counts.amazon_map_active, ck.counts.amazon_map_lost], [3, 0]);
    // 2 回目 = 何も書かない (変更の記録も出品の version も増えない)
    const ev = (await Cdb.q('select count(*)::int as n from events.master_change_events'))[0].n;
    const ver = await Cdb.q('select listing_id::text as id, version::text as v from core.listings order by 1');
    const r2 = await reconcile(Cdb, legacyC2, C2.id, { expectCdbHash: await cdbHash(Cdb) });
    assert.equal(r2.committed, true);
    assert.deepEqual([r2.counts.maps_inserted, r2.counts.maps_updated, r2.counts.maps_tombstoned, r2.counts.listings_created, r2.counts.updated, r2.counts.inserted, r2.counts.deleted, r2.counts.time_only], [0, 0, 0, 0, 0, 0, 0, 0]);
    assert.equal((await Cdb.q('select count(*)::int as n from events.master_change_events'))[0].n, ev);
    assert.deepEqual(await Cdb.q('select listing_id::text as id, version::text as v from core.listings order by 1'), ver);
    // 消した SKU を古い表に戻す = 墓標を active に戻す (構成も)・ハッシュ一致
    const dirC3 = path.join(tmp, 'c3');
    const legacyC3 = makeLegacy(dirC3, [...MASTERS_C2, ['pr_pack2', 'SKU マスタの 2 個組', T1, T1]], [...COMPS_C2, ['pr_pack2', 'a001', 2, 0, T1, T1], ['pr_pack2', 'a002', 1, 1, T1, T2]]);
    const r3 = await reconcile(Cdb, legacyC3, C2.id, { expectCdbHash: await cdbHash(Cdb) });
    assert.deepEqual([r3.counts.maps_inserted, r3.counts.maps_updated, r3.counts.maps_tombstoned], [0, 1, 0]);
    assert.equal(await cdbHash(Cdb), lhash(legacyC3));
    assert.equal((await Cdb.q("select state from core.amazon_sku_maps where seller_sku = 'pr_pack2'"))[0].state, 'active');
    // 古い表を C2 に戻して、もう一度合わせる (この後の widen は C2 の古い表で)
    await reconcile(Cdb, legacyC2, C2.id, { expectCdbHash: await cdbHash(Cdb) });
    assert.equal(await cdbHash(Cdb), lhash(legacyC2));
  });

  await ta('[RC4] 合わせ直した後: CLI の check (ハッシュ一致) → widen が通る (Amazon = company) → 窓が閉じた後の reconcile は断る / CLI の --reconcile と --cdb-hash', async () => {
    const out = []; const code = await epochCli(['check', '--attempt', C2.id, '--company', '1', '--data-dir', dirC2], { env: { COMPANY_DB_WATCH_URL: 'x' }, connect: async () => ({ db: Cdb.dbWA }), log: (m) => out.push(m) });
    assert.equal(code, 0, out.join('\n'));
    // CLI (本物のプロセス): --cdb-hash が今のハッシュを出す・--reconcile は 2 回目 = 何も変わらない (commit)
    const hc = spawnSync(process.execPath, [CLI_MIGRATE, '--cdb-hash'], { env: { ...process.env, COMPANY_DB_URL: Cdb.url }, encoding: 'utf8', timeout: 120000 });
    assert.equal(hc.status, 0, hc.stderr); assert.match(hc.stdout, new RegExp(await cdbHash(Cdb)));
    const rc = cliApply(Cdb, legacyC2, ['--reconcile', '--attempt', C2.id, '--expect-cdb-hash', await cdbHash(Cdb)]);
    assert.equal(rc.code, 2, rc.out);   // --apply と --reconcile を一緒に付けた = 引数不正 (cliApply は --apply を付ける)
    const fba = makeFba(dirC2, [SHEET_ONLY_UPPER]);   // #1651 Codex R1 High: 10/8 の 2 件目 (Sheet に大文字で残る) の出品の構成も reconcile で消す
    const compsC = await seedSheetListing(Cdb, SHEET_ONLY_UPPER.toLowerCase(), 'a002');
    const rc2 = spawnSync(process.execPath, [CLI_MIGRATE, '--reconcile', '--attempt', C2.id, '--expect-hash', lhash(legacyC2), '--expect-cdb-hash', await cdbHash(Cdb), '--legacy', legacyC2, '--fba-db', fba, '--actor', 'naka@test', '--yes'],
      { env: { ...process.env, COMPANY_DB_URL: Cdb.url }, encoding: 'utf8', timeout: 120000 });
    assert.equal(rc2.status, 0, `${rc2.stdout}${rc2.stderr}`); assert.match(rc2.stdout, /✅ 合わせ直した \(commit\)/);
    assert.match(rc2.stdout, /Sheet にだけある SKU の出品の自動の構成: 消した 1 行/); assert.equal(await compsC(), 0);
    const rc3 = spawnSync(process.execPath, [CLI_MIGRATE, '--reconcile', '--expect-hash', lhash(legacyC2), '--legacy', legacyC2, '--fba-db', fba, '--actor', 'naka@test', '--yes'],
      { env: { ...process.env, COMPANY_DB_URL: Cdb.url }, encoding: 'utf8', timeout: 120000 });
    assert.equal(rc3.status, 2, `${rc3.stdout}${rc3.stderr}`);   // --expect-cdb-hash と --attempt が無い
    // widen
    const step = M.amazonWidenEvidenceStep(M.readLegacyAmazonMaps(legacyC2));
    const w = await W.widenOwnership(Cdb.dbO, { attemptId: C2.id, companyId: 1, actor: '中原', evidence: C2.ev, beforeCall: step.beforeCall });
    assert.deepEqual([w.widened, w.added_keys, w.counts.amazon_map_active, w.counts.amazon_map_lost], [true, [KEY_A], 3, 0]);
    assert.equal((await OS.readOwnershipState(Cdb.dbO)).active.map[KEY_A], 'company');
    await assert.rejects(reconcile(Cdb, legacyC2, C2.id, { expectCdbHash: await cdbHash(Cdb) }), phaseErr(/attempt_not_prepared/));
  });

  await ta('[R] 権限: 0059 の数の関数・判定の本体・widen は watcher から呼べない (42501)・watcher の読むだけの判定は呼べる', async () => {
    for (const sql of ['select ops.widen_amazon_map_counts(1)', "select ops._widen_judge(gen_random_uuid(), 1)", "select ops.widen_master_ownership(gen_random_uuid(), 1, 'w', '{}'::jsonb)"]) {
      const e = await errOf(A.WA, sql);
      assert.equal(e?.code, '42501', `${sql}: ${e?.code} ${e?.message}`);
    }
    const r = (await A.WA.query('select ops.widen_check_readonly(gen_random_uuid(), 1) as r')).rows[0].r;
    assert.equal(r.ok, false); assert.match(r.problems[0], /attempt_missing/);
    const pub = await A.q(`select p.proname, has_function_privilege('public', p.oid, 'execute') as pub, p.prosecdef as d, array_to_string(p.proconfig, ',') as c
      from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname in ('widen_amazon_map_counts', '_widen_judge', 'widen_master_ownership', 'master_widen_allowed_keys') order by 1`);
    for (const f of pub) { assert.equal(f.pub, false, f.proname); assert.equal(f.c, 'search_path=pg_catalog, pg_temp', f.proname); }
    assert.deepEqual(pub.map((f) => [f.proname, f.d]), [['_widen_judge', false], ['master_widen_allowed_keys', false], ['widen_amazon_map_counts', false], ['widen_master_ownership', true]]);
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB を消せない ${n}: ${e.message}`); } }
  try { await admin.end(); } catch { /* */ }
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* */ }
}
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
