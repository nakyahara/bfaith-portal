/**
 * test-master-register.mjs — 新商品の登録・登録の状態・product-hub のカードの outbox (Company DB構想 14 ⑤-2a / 契約 v3 H3・Medium 1・§11)
 *
 * Company DB = PGlite (Render と同じ持ち主のロール deploy で migration)。product-hub = 一時の DATA_DIR の SQLite。本物の router を HTTP 越しにも通す。
 * ロール = ⑤-1 の scripts/company-db/create-master-edit-roles.mjs (登録・取り込みは画面だけのロール master_edit・backfill は master_ops・門の記録は master_gate_<場所>)。
 * 切替の段階は ⑤-1 の本物の関数で (門の記録 + 証拠つき)
 * 固定する契約:
 *   S 登録の状態 (0052・PR #1566 R1): 行が無い = 使えない (ビューに出ない)・書くのは security definer の関数だけ = 画面・運用のロールは GUC を立てても 42501 (H2)・
 *     根拠の表ができるまで (⑤-2b / ④) は NE 登録待ち・NE 確認済み・配る対象・利用可へ進めない (H3)・やめる = 人の理由・履歴は追記だけ・
 *     backfill = 段階 frozen / company_owner の間に 1 回だけ・(company_id, sku_id, code_norm, sku_kind) のハッシュが同じときだけ (M4)・下書きは触らない・
 *     backfill の前でも画面のロールが足した SKU は状態の行が要る・backfill の後はだれでも・夜間ロードが NE から作った SKU は同じ取引で quarantined・
 *     new_open は backfill がちょうど 1 回 かつ 状態の行の無い SKU が 0 件のときだけ (H1。⑤-1 の前提の差し込み口の表に 1 行 = 0052_registrations + 段階の行の保険の trigger)
 *   G 登録の関数 ops.register_new_sku (0052) と ⑤-1 (0051) の書き込みの約束: 画面のロールは core.products / core.skus / 知らせを直接足せない・
 *     下書きの状態を作る関数も実行できない・ops.begin_master_write で登録の約束 (sku_create) は作れない = 関数だけが登録の約束を書く・
 *     関数を直接呼んでも DB の決まり (コード・構成品・代表の仕入先・持ち主表・形) で断る・関数の外では登録の約束で書けない・
 *     変更の記録と状態の記録は約束の人・request_id・理由 (偽の core.actor_* は使わない)・登録の約束も done が要る (0051 の commit の確かめ)・
 *     DB の守りが見る列の持ち主のキーは NEW_ENTRY_KEYS に全部入っている・DB が段階 / 持ち主表で断った = 409 before_cutover・
 *     関数を直接呼んで値を偽っても DB が確かめる / 作り直す (単品の税率と税区分・名前・売価・発送方法・原価の出どころと日・セットだけの列・
 *     セットの導く値 (DB で導き直す)・構成品のコード・カードの写しの欄・rows_hash / カードの hash / 約束のハッシュは DB が作る。Codex R3 Medium 1)・
 *     保存の記録の結果は DB の値だけで作る (呼び手の result は断る・Codex R4 Medium)・カードの知らせの欄の名前は決まった ASCII だけ (Codex R4 Low)
 *   R 新商品の登録: 門 (段階 new_open・持ち主 company・MASTER_EDIT_OPEN) が閉じている = 409 何も書かない (失敗の記録は残る)・形の検査 400・
 *     コードの検査 409 (Company DB・名札・NE・消したコード)・単品 / セットを 1 つの取引で (SKU・商品・状態 draft・仕入先・原価・構成の依頼・知らせ・記録)・
 *     巻き戻る = 知らせも残らない・同じ request_id = 前の結果・編集の印に登録の状態・やめた商品は直せない
 *   O 知らせ (outbox) の取り込み: 1 回で 1 枚・2 回でも 1 枚 (冪等)・同じ商品コードのカード = 衝突 (増やさない・記録)・人が「既存のカードに結ぶ」で解く (M6)・SQLite の失敗 = failed のまま・もう一度で作れる・
 *     同じコードのカードが 2 枚以上 (一意の index が無い DB) = どれか決められない衝突 (作らない・結ばない・done にしない。Codex R2 Medium)・
 *     一覧の「登録の状態」と「カード」の絞り込みは両方効く (Codex R2 Low)・
 *     自動は回数の上限まで・中身は変えられない・hash が違う = 取り込まない・借り (lease) の間はほかが取らない・送料の対応が無い = 空 + 要確認
 *   H 画面 (master-edit): 新商品の画面の描画と JS・門が閉じている帯・登録の API (名簿・Origin)・保存の後にカードを作る・コードの確かめ・商品の画面のカードの箱ともう一度
 *   P product-hub: /new は切替の前は今までどおり (Company DB につながない)・切替の後は案内・途中 / 読めない = 止めている・ボードを開くと知らせを取り込む・要確認の札
 * 使い方: node scripts/test-master-register.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';
// 広げる道 PR-2: 画面は DB の active に従う。試験の DB は全部の列を company にする = このコードの能力も全部 (code_behind の試験だけ戻す)
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

// product-hub の SQLite は一時の場所 (本番の DB に触らない)。warehouse-mirror/db.js を読む前に決める
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'master-register-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
// 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const R = await import('../lib/master-register.mjs');
const O = await import('../lib/product-hub-outbox.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();   // 本番では server.js が起動時に行う
const PHDB = await import('../apps/product-hub/db.js');
const PH = await import('../apps/product-hub/services/cdb-card-intake.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const rejectsWith = async (p, status, reason) => {
  try { await p; } catch (e) {
    assert.ok(e instanceof W.MasterWriteError, `MasterWriteError でない: ${e && e.stack}`);
    assert.equal(e.status, status, `${e.reason}: ${e.message}`);
    if (reason) assert.equal(e.reason, reason, e.message);
    return e;
  }
  assert.fail(`${status} ${reason || ''} にならなかった`);
};

// ── Company DB (Render と同じ = 持ち主のロールで) ──
const pg = new PGlite();
const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;   // 試験の接続のログイン (superuser)。門のログインから戻るときに使う
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg, { leases: ['single', 'set'], futureSetLease: true });   // 広げる道 PR-2: 本物の 0058 の上で試験の許可を置く (この DB は構成も C = セットの許可は将来の形)
const q = async (sql, params) => (await db.query(sql, params)).rows;
const one = async (sql, params) => (await q(sql, params))[0];
/** 1 つの取引で流す (失敗は巻き戻して投げる) */
async function tx(fn) {
  await pg.query('begin');
  try { const r = await fn(); await pg.query('commit'); return r; } catch (e) { try { await pg.query('rollback'); } catch { /* */ } throw e; }
}

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
/** 日付 = 本物の今日 (0052 の登録の関数は原価の始まりを DB の今日 (東京) だけにする = 画面の「今日」と同じでないと断る) */
const LOAD_NOW = new Date(Date.now() - 5 * 86400e3);
const NOW = new Date();
const TODAY = (await pg.query("select (now() at time zone 'Asia/Tokyo')::date::text as d")).rows[0].d;
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210.4 }], ['S02', { method: '宅急便', cost: 520 }], ['S03', { method: '謎の便', cost: 300 }]]);
const RUN = 'mc_20300109T000000000Z_abcdef';
const uuid = () => crypto.randomUUID();
const SHA = 'a'.repeat(64);
const ACKS = (g) => ['minipc:products', 'minipc:components', 'minipc:registrations', 'render:products', 'render:components', 'render:registrations'].map((t) => ({ target: t, generation: g }));
const plan = async () => one('select * from ops.registration_backfill_plan()');
/** ⑤-3 の古い入口の一覧 (manifest) の形の例 (⑤-1 の試験と同じ) */
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }] };
/** 証拠の時刻は今の段階に入った後・サーバーの今以前 (⑤-1 #1563 R3) = 進める直前に作る */
const manualStopped = () => [{ id: 'ne.product_screen', by: 'naka@test', at: new Date().toISOString() }];
const drain = () => ({ done: true, checked_by: 'naka@test', checked_at: new Date().toISOString() });
const BUILDS = { render: ['r1'], minipc: ['m1'] };
const LEGACY_HASH = C.ownershipHash(MASTER_OWNERSHIP);
/** ロールを替えて fn を流す (PGlite = 1 接続。終わったら持ち主のロールに戻す) */
async function asRole(role, fn) {
  await pg.query(`set role ${role}`);
  try { return await fn(); } finally { await pg.query('set role deploy'); }
}
/** 画面だけのロール (登録・取り込みは必ずこれ = 権限が足りているかも確かめる) */
const asEditor = (fn) => asRole('master_edit', fn);
/** 場所ごとの門のログイン (DB の関数は session_user を見る = SET SESSION AUTHORIZATION) */
async function asGate(host, fn) {
  await pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await pg.query(`set session authorization ${sessionUser}`); await pg.query('set role deploy'); }
}
/** 切替の段階を 1 つ進める (⑤-1 の本物の関数: 門の記録 → 運用のロール master_ops が証拠つきで)。持ち主表 = company_owner から ALL_COMPANY */
async function toPhase(to) {
  const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
  // 0055 (④a): company_owner・new_open に進むのは持ち主の epoch が active で段階の持ち主表と同じときだけ = 試験で置く (本番 = ④a の activate)
  if (to !== 'frozen') await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(db, ALL_COMPANY);
  const own = to === 'frozen' ? MASTER_OWNERSHIP : ALL_COMPANY;
  for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) {
    await asGate(host, () => C.recordLegacyGateAck(db, { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
  }
  const mh = await C.manifestHashOf(db, MANIFEST);
  const evidence = to === 'frozen' ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: LEGACY_HASH, manual_entries_stopped: manualStopped(), drain: drain() }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL_COMPANY) };
  const r = await asRole('master_ops', () => C.advanceCutoverPhase(db, { to, actor: 'naka@test', evidence }));
  // 0058 (広げる道 PR-1): 単品の新商品は DB の関数自身が開放の許可を確かめる = new_open に進めたら試験の許可を置く (本番 = widen → 翌朝のゲート)
  if (to === 'new_open') await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(db, { withSet: true });   // 広げる道 PR-2: セットの登録は JS の門がセットの許可も見る = この DB (構成も C の将来の形) はセットの許可も置く
  return r;
}
/** 0052 の前提の関数 (差し込み口の表の 0052_registrations)。集める関数 (⑤-1) の答え = 「0052_registrations: 」つき */
const prereq = async (from, to) => (await one('select ops.master_registrations_prereq($1, $2) as p', [from, to])).p;
const prereqAll = async (from, to) => (await one('select ops.master_cutover_prereq_problems($1, $2) as p', [from, to])).p;
/** SQL の誤りの番号 (誤りが無ければ null) */
const pgCode = async (p) => { try { await p; } catch (e) { return e.code ?? String(e.message); } return null; };

function makePlan(extra = []) {
  const sku = (code, name, kind, taxRate, salesClass, cost, x = {}) => ({
    code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : taxRate === 0.1 ? 'STANDARD_10' : null, handling: 'active', salesClass,
    cost: cost == null ? null : { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' },
    standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...x,
  });
  return {
    skus: [
      sku('s001', '単品 1', 'single', 0.1, 3, 100, { representativeCode: 'grp1', representativeState: 'value' }),
      sku('s002', '単品 2', 'single', 0.08, 2, 200),
      sku('s003', '単品 3 (原価なし)', 'single', 0.1, 3, null, { representativeCode: 'grp1', representativeState: 'value' }),
      sku('set001', 'セット 1', 'set', 0.08, null, 400, { taxClass: 'MIXED' }),
      ...extra.map((c) => sku(c, `NE の新しい商品 ${c}`, 'single', 0.1, 3, 50)),
    ],
    variationGroups: [{ code: 'grp1', name: '名札', childCodes: ['s001', 's003'], status: 'active' }],
    setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 2, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' }],
    listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0003', name: '止めた仕入先' }],
    supplierSkus: [], primarySuppliers: [],
    reorder: { available: true, runId: 'pml_test' },
  };
}
const load = async (extra = [], ownership = MASTER_OWNERSHIP) => {
  const r = await runInitialLoad(db, makePlan(extra), { log: quiet, runId: `load_${crypto.randomBytes(3).toString('hex')}`, ownership, now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  return r;
};
await load();
await pg.query("update core.suppliers set active = false where code = '0003'");
await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-09T00:00:00Z', 0)`, [RUN]);

const skuId = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code]))?.id;
const regOf = async (code) => one('select r.state, r.origin, r.distribution_generation as gen from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = $1', [code]);
const transition = (id, to, { actorType = 'human', actor = 'naka@test', reason = null, evidence = {} } = {}) =>
  pg.query('select ops.transition_sku_registration($1, $2, $3, $4, $5, $6::jsonb) as r', [id, to, actorType, actor, reason, JSON.stringify(evidence)]);
/** 試験用の SKU を作る (状態の行つき = 新商品の登録と同じ取引の形) */
async function makeDraftSku(code) {
  return tx(async () => {
    const id = (await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', $1, $1) returning sku_id::text as id`, [code])).rows[0].id;
    await pg.query('select ops.create_sku_registration($1, $2)', [id, 'naka@test']);
    return id;
  });
}
const reg = (kind, code, values, card = {}, o = {}) => asEditor(() => R.registerNewSku(db, {
  actor: o.actor ?? 'Naka@Test', requestId: o.requestId ?? uuid(), kind, code, reason: o.reason ?? null, values, card,
}, { ownership: o.ownership ?? ALL_COMPANY, open: o.open ?? true, now: NOW, shippingRates: o.shippingRates === undefined ? RATES : o.shippingRates, beforeCommit: o.beforeCommit }));
const single = (over = {}) => ({ name: '新しい単品', standard_price: '1,980', shipping_code: 'S01', tax_rate: '10', ...over });

console.log('登録の状態 (0052)');

/** 試験だけ: 持ち主が印 (GUC) を立てて状態を直接書く (根拠の表ができるまで届かない状態を作る) */
async function forceState(id, state, gen = null) {
  await tx(async () => {
    await pg.query(`select set_config('ops.registration_protocol', '1', true)`);
    await pg.query('update ops.master_registrations set state = $2, distribution_generation = $3 where sku_id = $1', [id, state, gen]);
  });
}

await ta('[S1] 状態の行が無い SKU は使えない (利用可・配る対象のビューに出ない)・backfill の計画 = 行の無い SKU の件数と sha256', async () => {
  assert.equal((await q('select count(*)::int as n from ops.master_registrations'))[0].n, 0);
  assert.equal((await q('select count(*)::int as n from ops.v_sku_available'))[0].n, 0);
  assert.equal((await q('select count(*)::int as n from ops.v_sku_distributable'))[0].n, 0);
  const p = await plan();
  assert.equal(p.sku_count, Number((await one('select count(*)::int as n from core.skus')).n));
  assert.match(p.snapshot_hash, /^[0-9a-f]{64}$/);
});

await ta('[S2] 書くのは security definer の関数だけ: 画面・運用のロールは GUC を立てても表と履歴を直接書けない (42501)・持ち主の手の DML も拒む・作るのは 1 回だけ', async () => {
  const id = await skuId('s001');
  await assert.rejects(() => pg.query(`insert into ops.master_registrations (sku_id, state, origin, created_by, state_changed_by) values ($1, 'available', 'backfill', 'x', 'x')`, [id]), /関数/);
  const d = await makeDraftSku('tx-1');
  assert.deepEqual(await regOf('tx-1'), { state: 'draft', origin: 'new_entry', gen: null });
  await assert.rejects(() => pg.query(`update ops.master_registrations set state = 'available' where sku_id = $1`, [d]), /関数/);
  await assert.rejects(() => pg.query('delete from ops.master_registrations where sku_id = $1', [d]), /消さない/);
  await assert.rejects(() => pg.query('truncate ops.master_registrations'), /append-only/);
  await assert.rejects(() => pg.query('select ops.create_sku_registration($1, $2)', [d, 'naka@test']), /already_registered/);
  await assert.rejects(() => pg.query('update ops.master_registration_events set reason = $1', ['x']), /append-only/);
  await assert.rejects(() => pg.query('delete from ops.master_registration_events'), /append-only/);
  // 前からある SKU (別の取引で作った) を下書きにはできない
  await assert.rejects(() => pg.query('select ops.create_sku_registration($1, $2)', [id, 'naka@test']), /not_new_sku/);
  assert.equal(await regOf('s001'), undefined);
  // 迂回の試験: 画面・運用のロールが印 (GUC) を立てて直接書く = 権限で止まる
  for (const role of ['master_edit', 'master_ops']) {
    const codes = await asRole(role, async () => {
      await pg.query(`select set_config('ops.registration_protocol', '1', false)`);
      try {
        return [
          await pgCode(pg.query(`update ops.master_registrations set state = 'available' where sku_id = $1`, [d])),
          await pgCode(pg.query(`insert into ops.master_registrations (sku_id, state, origin, created_by, state_changed_by) values ($1, 'available', 'backfill', 'x', 'x')`, [id])),
          await pgCode(pg.query('delete from ops.master_registrations where sku_id = $1', [d])),
          await pgCode(pg.query(`insert into ops.master_registration_events (sku_id, to_state, actor_type, actor_id) values ($1, 'available', 'human', 'x')`, [d])),
          await pgCode(pg.query(`insert into ops.master_registration_backfill (sku_count, snapshot_hash, phase, actor) values (0, $1, 'frozen', 'x')`, ['a'.repeat(64)])),
          await pgCode(pg.query(`select ops.quarantine_unregistered_skus('x')`)),
        ];
      } finally { await pg.query(`select set_config('ops.registration_protocol', '', false)`); }
    });
    assert.deepEqual(codes, Array(6).fill('42501'), role);
  }
  // 画面のロールは状態を進める関数も実行できない
  assert.equal(await asRole('master_edit', () => pgCode(transition(d, 'cancelled', { reason: 'x' }))), '42501');
  assert.equal((await regOf('tx-1')).state, 'draft');
});

await ta('[S2c] 記録の偽造 (仮レビュー M-A): 画面のロールは変更の記録を書けない (42501)・偽の記録があっても前からある SKU は下書きにできない・同じ取引で直しただけの SKU も (backfill から外れない)', async () => {
  const id = await skuId('s002');
  const forge = `insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_id, entity_key, new_value, actor_type, source_system)
    values (1, gen_random_uuid(), 'INSERT', 'sku', $1, jsonb_build_object('sku_id', $1::bigint), '{}'::jsonb, 'human', 'forge')`;
  assert.equal(await asEditor(() => pgCode(pg.query(forge, [id]))), '42501');
  const p0 = await plan();
  // 持ち主が偽の記録を入れた取引の中でも (前の版はこれで通った) = SKU の行そのものを見る
  await pg.query('begin');
  try {
    await pg.query(forge, [id]);
    await assert.rejects(() => pg.query('select ops.create_sku_registration($1, $2)', [id, 'naka@test']), /not_new_sku/);
  } finally { await pg.query('rollback'); }
  // 同じ取引で前からある SKU を直してから (行の xmin は今の取引・created_at は古い) = not_new_sku
  await pg.query('begin');
  try {
    await pg.query('update core.skus set name = name where sku_id = $1', [id]);
    await assert.rejects(() => pg.query('select ops.create_sku_registration($1, $2)', [id, 'naka@test']), /not_new_sku/);
  } finally { await pg.query('rollback'); }
  // 画面のロールは ops.begin_master_write の前は直すこともできない (0051 の守り)・下書きの状態を作る関数は実行できない (登録の関数の中だけ)
  await asEditor(async () => {
    await pg.query('begin');
    try {
      await assert.rejects(() => pg.query('update core.skus set name = name where sku_id = $1', [id]), (e) => e.code === '42501' && /master_write_session_required/.test(e.message));
    } finally { await pg.query('rollback'); }
    await assert.rejects(() => pg.query('select ops.create_sku_registration($1, $2)', [id, 'naka@test']), (e) => e.code === '42501' && /permission denied/.test(e.message));
  });
  assert.equal(await regOf('s002'), undefined);
  assert.deepEqual(await plan(), p0);   // backfill の計画から外れていない
});

await ta('[S2b] 画面のロールは SKU・商品・カードの知らせを直接足せない (insert の権限なし = 42501)。新商品は登録の関数 ops.register_new_sku だけ', async () => {
  for (const sql of [`insert into core.skus (company_id, sku_kind, code, name, created_by_type, created_by_id) values (1, 'set', 'edit-direct', 'x', 'human', 'x')`,
    `insert into core.products (company_id, display_code, name, created_by_type, created_by_id) values (1, 'edit-direct', 'x', 'human', 'x')`,
    `insert into ops.product_hub_outbox (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
       select 1, sku_id, 'card_create', 'ph-card-v1', '{}', repeat('d', 64), gen_random_uuid(), 'x' from core.skus where code = 's001'`]) {
    const e = await asRole('master_edit', async () => { try { await tx(() => pg.query(sql)); return null; } catch (x) { return x; } });
    assert.equal(e?.code, '42501', sql); assert.match(e.message, /permission denied/);
  }
  assert.equal(await skuId('edit-direct'), undefined);
});

await ta('[S3] NE 登録待ち・NE 確認済みの根拠は 0053 (⑤-2b) の記録を関数が自分で読む (呼び手の根拠 = caller_evidence・記録が無い = no_evidence)・配る対象・利用可は ④ まで not_ready・一方向・やめる = 人の理由だけ (運用のロールも)', async () => {
  const d = await skuId('tx-1');
  await assert.rejects(() => transition(d, 'ne_pending', { evidence: { export_id: 7, sha256: SHA } }), /caller_evidence/);
  await assert.rejects(() => transition(d, 'ne_pending'), /no_evidence/);
  await assert.rejects(() => transition(d, 'ne_confirmed', { evidence: { compare_run_id: RUN, matched: true } }), /one_way/);
  await assert.rejects(() => transition(d, 'available', { evidence: { generation: 'g1', acks: ACKS('g1') } }), /one_way/);
  // 持ち主が状態を直接作った (試験だけ) としても、照合の確かめの記録が無い = 進めない
  await forceState(d, 'ne_pending');
  await assert.rejects(() => transition(d, 'ne_confirmed', { actorType: 'system', actor: 'compare', evidence: { compare_run_id: RUN, matched: true } }), /caller_evidence/);
  await assert.rejects(() => transition(d, 'ne_confirmed', { actorType: 'system', actor: 'compare' }), /no_evidence/);
  await forceState(d, 'ne_confirmed');
  await assert.rejects(() => transition(d, 'distributable', { actorType: 'system', actor: 'copy', evidence: { generation: 'g1' } }), /not_ready/);
  await forceState(d, 'distributable', 'g1');
  await assert.rejects(() => transition(d, 'available', { actorType: 'system', actor: 'copy', evidence: { generation: 'g1', acks: ACKS('g1') } }), /not_ready/);
  await forceState(d, 'draft');
  // やめる = 人が理由を書いてだけ
  const d2 = await makeDraftSku('tx-2');
  await assert.rejects(() => transition(d2, 'cancelled', { actorType: 'system', actor: 'x', reason: '理由' }), /人が理由/);
  await assert.rejects(() => transition(d2, 'cancelled', { reason: ' ' }), /人が理由/);
  await transition(d2, 'cancelled', { reason: '作るのをやめた' });
  assert.equal((await regOf('tx-2')).state, 'cancelled');
  await assert.rejects(() => transition(d2, 'draft'), /one_way/);
  const ev = await q('select from_state, to_state, actor_type, reason from ops.master_registration_events where sku_id = $1 order by event_id', [d2]);
  assert.deepEqual(ev.map((e) => [e.from_state, e.to_state, e.reason]), [[null, 'draft', null], ['draft', 'cancelled', '作るのをやめた']]);
  const d3 = await makeDraftSku('tx-3');
  await asRole('master_ops', () => transition(d3, 'cancelled', { reason: '運用でやめた' }));
  assert.equal((await regOf('tx-3')).state, 'cancelled');
});

await ta('[S4] 消した SKU のコード (使い回さない試験の材料) と、切替の前の登録は 409 切替前・何も書かない・失敗の記録は残る', async () => {
  await tx(async () => { await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'old-gone', '消したセット')`); });
  await pg.query(`delete from core.skus where code = 'old-gone'`);
  const id = uuid();
  const e = await rejectsWith(reg('single', 'new-a1', single(), {}, { requestId: id }), 409, 'before_cutover');
  assert.equal(e.extra.phase, 'legacy_open');
  assert.equal(await skuId('new-a1'), undefined);
  const rec = await one('select operation, status, error, target_code from ops.master_edit_requests where request_id = $1', [id]);
  assert.deepEqual([rec.operation, rec.status, rec.error.reason, rec.target_code], ['sku_create', 'failed', 'before_cutover', 'new-a1']);
  assert.equal((await q('select count(*)::int as n from ops.product_hub_outbox'))[0].n, 0);
});

await ta('[S5] backfill: 段階 frozen / company_owner の間だけ・計画と同じときだけ (消して同じコードで作り直した SKU = 件数が同じでも違うハッシュ)・backfill の前は new_open に進めない・1 回だけ', async () => {
  const p0 = await plan();
  await assert.rejects(() => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [p0.sku_count, p0.snapshot_hash, 'naka@test']), /wrong_phase/);
  await toPhase('frozen');
  await tx(async () => { await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'redo-1', '作り直す')`); });
  const p1 = await plan();
  await assert.rejects(() => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [p1.sku_count + 1, p1.snapshot_hash, 'naka@test']), /snapshot_mismatch/);
  await pg.query(`delete from core.skus where code = 'redo-1'`);
  await tx(async () => { await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'redo-1', '作り直す')`); });
  const p2 = await plan();
  assert.equal(p2.sku_count, p1.sku_count);
  assert.notEqual(p2.snapshot_hash, p1.snapshot_hash);
  await assert.rejects(() => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [p1.sku_count, p1.snapshot_hash, 'naka@test']), /snapshot_mismatch/);
  // backfill の前は new_open に進めない (段階の門・H1)
  await toPhase('company_owner');
  const probs = await prereq('company_owner', 'new_open');
  assert.ok(probs.some((x) => /^backfill_missing/.test(x)) && probs.some((x) => /^unregistered_skus/.test(x)), JSON.stringify(probs));
  assert.deepEqual(await prereq('legacy_open', 'frozen'), []);
  // ⑤-1 の差し込み口の表に 1 行 (集める関数は上書きしない = 後の migration の前提と並ぶ)
  assert.deepEqual(await q(`select name, fn::text as fn from ops.master_cutover_prereq_checks where name like '0052%'`), [{ name: '0052_registrations', fn: 'ops.master_registrations_prereq(text,text)' }]);
  assert.deepEqual(await prereqAll('company_owner', 'new_open'), probs.map((x) => `0052_registrations: ${x}`));
  await assert.rejects(() => toPhase('new_open'), /prereq_failed: 0052_registrations: backfill_missing/);
  // 保険の trigger: 持ち主が印を立てて段階の行を直接変えても、前提は外せない
  await assert.rejects(() => tx(async () => { await pg.query(`select set_config('ops.cutover_protocol', '1', true)`); await pg.query(`update ops.master_cutover_state set phase = 'new_open'`); }), /cutover_prereq.*backfill_missing/);
  // 保険の trigger: 差し込み口の表の行が消えても (表は追記だけ = 保守で trigger を止めて消した・後の migration の間違いなど)、段階の関数で new_open に進めない
  await tx(async () => {
    await pg.query('alter table ops.master_cutover_prereq_checks disable trigger trg_append_only_row');
    await pg.query(`delete from ops.master_cutover_prereq_checks where name = '0052_registrations'`);
    await pg.query('alter table ops.master_cutover_prereq_checks enable trigger trg_append_only_row');
  });
  try {
    assert.deepEqual(await prereqAll('company_owner', 'new_open'), []);
    await assert.rejects(() => toPhase('new_open'), /cutover_prereq.*backfill_missing/);
  } finally {
    await pg.query(`insert into ops.master_cutover_prereq_checks (name, fn) values ('0052_registrations', 'ops.master_registrations_prereq(text, text)')`);
  }
  assert.equal((await C.readCutoverPhase(db)).phase, 'company_owner');
  // 運用のロールで backfill
  const p3 = await plan();
  const r = await asRole('master_ops', async () => (await pg.query('select ops.backfill_sku_registrations($1, $2, $3, $4) as r', [p3.sku_count, p3.snapshot_hash, 'naka@test', '切替の試験'])).rows[0].r);
  assert.deepEqual([r.sku_count, r.phase], [p3.sku_count, 'company_owner']);
  for (const code of ['s001', 's002', 's003', 'set001', 'redo-1']) assert.deepEqual(await regOf(code), { state: 'available', origin: 'backfill', gen: null }, code);
  assert.equal((await regOf('tx-2')).state, 'cancelled');   // 前からある行は触らない
  assert.equal((await regOf('tx-1')).state, 'draft');
  assert.equal((await plan()).sku_count, 0);
  const p4 = await plan();
  await assert.rejects(() => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [0, p4.snapshot_hash, 'naka@test']), /already_done/);
  await assert.rejects(() => pg.query('delete from ops.master_registration_backfill'), /append-only/);
  assert.equal((await one("select count(*)::int as n from ops.master_registration_events where to_state = 'available' and from_state is null")).n, p3.sku_count);
  assert.ok((await q('select code from ops.v_sku_available')).some((x) => x.code === 's001'));
  assert.ok(!(await q('select code from ops.v_sku_available')).some((x) => x.code === 'tx-1'));
  assert.deepEqual(await prereq('company_owner', 'new_open'), []);
});

await ta('[S6] backfill の後は、状態の行の無い SKU を足した取引は commit できない (同じ取引で行を作れば通る)', async () => {
  await assert.rejects(() => tx(async () => { await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'no-reg', 'x')`); }), /unregistered_sku/);
  assert.equal(await skuId('no-reg'), undefined);
  await makeDraftSku('with-reg');
  assert.equal((await regOf('with-reg')).state, 'draft');
});

await ta('[S7] 夜間ロードが NE から作った SKU は同じ取引で quarantined (要確認)・自動では使えるにしない・前からある状態は変えない・2 回目は増えない', async () => {
  const r = await load(['s900', 's901']);
  assert.ok(r.sections.skus.notes.some((n) => /要確認 \(quarantined\) にした/.test(n)), JSON.stringify(r.sections.skus.notes));
  assert.deepEqual(await regOf('s900'), { state: 'quarantined', origin: 'ne_discovered', gen: null });
  const ev = await one("select actor_type, actor_id, evidence from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = 's900'");
  assert.deepEqual([ev.actor_type, ev.actor_id, ev.evidence.run_id], ['system', 'company_db_load', r.run_id]);
  assert.equal((await regOf('s001')).state, 'available');
  assert.ok(!(await q('select code from ops.v_sku_available')).some((x) => x.code === 's900'));
  await load(['s900']);
  assert.equal((await one("select count(*)::int as n from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = 's900'")).n, 1);
  assert.equal((await one('select ops.quarantine_unregistered_skus($1) as n', ['again'])).n, 0);
});

await ta('[S7b] 状態の行の無い SKU が残っていたら (trigger を止めて入れた = 復元の後など) new_open に進めない・要確認にすれば進める', async () => {
  await pg.query('alter table core.skus disable trigger trg_skus_registered');
  await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'ghost-1', '行の無い SKU')`);
  await pg.query('alter table core.skus enable trigger trg_skus_registered');
  const probs = await prereq('company_owner', 'new_open');
  assert.deepEqual(probs.map((x) => x.split(':')[0]), ['unregistered_skus']);
  await assert.rejects(() => toPhase('new_open'), /prereq_failed: 0052_registrations: unregistered_skus/);
  assert.equal((await one(`select ops.quarantine_unregistered_skus('fix') as n`)).n, 1);
  assert.deepEqual(await prereq('company_owner', 'new_open'), []);
});

await ta('[S8] 要確認 (quarantined) は根拠の表ができるまで使える側に進めない (人の理由があっても not_ready)・やめるのは人の理由で', async () => {
  const id = await skuId('s900');
  await assert.rejects(() => transition(id, 'ne_confirmed', { reason: 'NE で確かめた正しい商品', evidence: { compare_run_id: RUN, matched: true } }), /not_ready/);
  await assert.rejects(() => transition(id, 'available', { reason: '使う' }), /one_way/);
  assert.equal((await regOf('s900')).state, 'quarantined');
  await transition(await skuId('s901'), 'cancelled', { reason: 'NE の間違いの商品' });
  assert.equal((await regOf('s901')).state, 'cancelled');
});

console.log('\n新商品の登録 (画面 D の保存)');

await ta('[R1] 段階 new_open でも、持ち主が load (今の本番の持ち主表) = 409 / MASTER_EDIT_OPEN が無い = 409 / 段階が company_owner = 409', async () => {
  let e = await rejectsWith(reg('single', 'new-a1', single()), 409, 'before_cutover');
  assert.equal(e.extra.phase, 'company_owner');
  await toPhase('new_open');
  // 広げる道 PR-2: 持ち主は DB の active (配った config ではない) = DB を「名前が load」にして確かめる (段階の記録も同じ = widen の後と同じ形)
  await W2.setActiveOwnershipInDb(pg, { ...ALL_COMPANY, 'skus.name': 'load', 'products.name': 'load' });
  e = await rejectsWith(reg('single', 'new-a1', single()), 409, 'before_cutover');
  assert.ok(e.extra.load_keys.includes('skus.name'));
  await W2.setActiveOwnershipInDb(pg, ALL_COMPANY);
  e = await rejectsWith(reg('single', 'new-a1', single(), {}, { open: false }), 409, 'before_cutover');
  assert.equal(e.extra.open, false);
  await W2.setActiveOwnershipInDb(pg, { ...ALL_COMPANY, sku_components: 'load' });
  e = await rejectsWith(reg('set', 'new-set-x', { name: 'x', standard_price: '1', shipping_code: 'S01', components: [{ code: 's001', qty: 1 }] }, {}), 409, 'before_cutover');
  assert.deepEqual(e.extra.load_keys, ['sku_components']);
  await W2.setActiveOwnershipInDb(pg, ALL_COMPANY);
  assert.equal(await skuId('new-a1'), undefined);
});

await ta('[R2] 形の誤り (400): コード (大文字・set-・空白)・名前・売価・発送方法・単品の税率・種類に無い項目・URL・ASIN・セットの判断・Yahoo! の配送方法 / カテゴリ・構成', async () => {
  const b = async (kind, code, values, card, re) => { const e = await rejectsWith(reg(kind, code, values, card), 400); if (re) assert.match(e.message, re); return e; };
  await b('single', 'New-A1', single(), {}, /大文字/);
  await b('single', 'set-abc', single(), {}, /set-/);
  await b('single', ' abc', single(), {}, /空白/);
  await b('single', 'abc', single({ name: '' }), {}, /名前/);
  await b('single', 'abc', single({ standard_price: '' }), {}, /売価/);
  await b('single', 'abc', single({ shipping_code: '' }), {}, /発送方法/);
  await b('single', 'abc', single({ tax_rate: '' }), {}, /税率/);
  await b('single', 'abc', { ...single(), components: [{ code: 's001', qty: 1 }] }, {}, /単品の登録では/);
  await b('set', 'abc', { name: 'x', standard_price: '1', shipping_code: 'S01', tax_rate: '10', components: [{ code: 's001', qty: 1 }] }, {}, /セットの登録では/);
  await b('set', 'abc', { name: 'x', standard_price: '1', shipping_code: 'S01' }, {}, /構成品/);
  await b('single', 'abc', single(), { official_url: 'ftp://x' }, /URL/);
  await b('single', 'abc', single(), { reference_urls: ['https://a.example/', 'javascript:alert(1)'] }, /参考 URL 2 つ目/);
  await b('single', 'abc', single(), { reference_urls: Array.from({ length: 21 }, (_, i) => `https://a.example/${i}`) }, /20 個/);
  await b('single', 'abc', single(), { asin: 'B0123' }, /ASIN/);
  await b('single', 'abc', single(), { set_decision: { decision: 'none' } }, /作らない理由/);
  await b('single', 'abc', single(), { set_decision: { decision: 'none', reason_code: 'other' } }, /その他/);
  await b('single', 'abc', single(), { set_decision: { decision: 'maybe' } }, /作る \/ 作らない \/ 保留/);
  await b('set', 'abc', { name: 'x', standard_price: '1', shipping_code: 'S01', components: [{ code: 's001', qty: 1 }] }, { set_decision: { decision: 'hold' } }, /単品だけ/);
  await b('single', 'abc', single(), { yahoo: { delivery_label: 'どこでも便' } }, /Yahoo! の配送方法/);
  await b('single', 'abc', single(), { yahoo: { category_id: '12a' } }, /カテゴリ/);
  await b('single', 'abc', single(), { yahoo: { price: '0' } }, /Yahoo!売価は 1〜/);   // 仮レビュー L5
  await b('single', 'abc', single(), { yahoo: { price_sagawa: '０' } }, /佐川\)は 1〜/);
  await b('single', 'abc', single({ shipping_code: 'S99' }), {}, /送料の表にありません/);
  await b('single', 'abc', single({ primary_supplier: '0003' }), {}, /取引停止/);
  await b('set', 'abc', { name: 'x', standard_price: '1', shipping_code: 'S01', components: [{ code: 'set001', qty: 1 }] }, {}, /入れ子/);
  await b('set', 'abc', { name: 'x', standard_price: '1', shipping_code: 'S01', components: [{ code: 'tx-2', qty: 1 }] }, {}, /入れ子|やめた/);
  await rejectsWith(reg('single', 'abc', single(), {}, { shippingRates: null }), 503, 'shipping_rates_unavailable');
  assert.equal(await skuId('abc'), undefined);
});

await ta('[R3] コードの検査 (409): Company DB にある・代表の名札・NE の元のコード (0041)・前に使って消したコード', async () => {
  await pg.query(`insert into ops.master_ne_codes (code_norm, kind, state, ne_code, spellings) values ('ne-only-1', 'product', 'ok', 'ne-only-1', '["ne-only-1"]')`);
  for (const [code, reason] of [['s001', 'code_taken'], ['grp1', 'code_is_rep'], ['ne-only-1', 'code_in_ne'], ['old-gone', 'code_used_before']]) {
    const e = await rejectsWith(reg('single', code, single()), 409, reason);
    assert.equal(e.extra.field, 'code');
    assert.deepEqual((await R.checkNewCodeInDb(db, code)).reason, reason);
  }
  assert.equal((await R.checkNewCodeInDb(db, 'fresh-1')).ok, true);
  assert.equal((await R.checkNewCodeInDb(db, 'Fresh')).reason, 'code_shape');
});

let newA1Result;
await ta('[R4] 単品: 1 つの取引で 商品・SKU・状態 draft・代表の仕入先・原価 (今日から)・カードの知らせ・保存の記録。変更の記録は人・画面・request_id', async () => {
  const id = uuid();
  const card = {
    official_url: 'https://maker.example/a1', amazon_url: 'https://www.amazon.co.jp/dp/B0ABCDEFGH', asin: 'b0abcdefgh',
    reference_urls: ['https://ref.example/1', 'https://ref.example/2', 'https://ref.example/1'],
    set_decision: { decision: 'none', reason_code: 'low_demand' },
    yahoo: { price: '2,080', price_sagawa: '2280', delivery_label: 'ネコポス', category_id: '12345', path: 'コスメ:スキンケア' },
  };
  const r = await reg('single', 'new-a1', single({ sales_class: '3', primary_supplier: '1', reorder_months: '1.5', cost: { jpy: '800' }, expiry_managed: '1', inbound_date_managed: '0' }), card, { requestId: id, reason: '新しく仕入れる' });
  newA1Result = r;
  assert.equal(r.ok, true); assert.equal(r.state, 'draft'); assert.equal(r.card.status, 'pending');
  const row = await one(`select s.sku_kind, s.name, s.tax_rate::float8 as tax, s.tax_class, s.handling, s.standard_price_jpy::int as price, s.shipping_code, s.shipping_method,
      s.shipping_cost_jpy::int as ship, s.reorder_months::float8 as months, s.created_by_type, s.created_by_id,
      p.display_code, p.name as pname, p.sales_class, p.status, p.expiry_managed, p.inbound_date_managed
    from core.skus s join core.products p on p.product_id = s.product_id where s.code = 'new-a1'`);
  assert.deepEqual(row, { sku_kind: 'single', name: '新しい単品', tax: 0.1, tax_class: 'STANDARD_10', handling: 'active', price: 1980, shipping_code: 'S01', shipping_method: 'ゆうパケット',
    ship: 210, months: 1.5, created_by_type: 'human', created_by_id: 'naka@test', display_code: 'new-a1', pname: '新しい単品', sales_class: 3, status: 'active', expiry_managed: true, inbound_date_managed: false });
  assert.deepEqual(await regOf('new-a1'), { state: 'draft', origin: 'new_entry', gen: null });
  // 0057: 登録日 = 登録した取引の JST の今日・出どころ portal (登録の関数が明示して書く = 列の既定値なし・#1617 Codex R2 Low)
  assert.deepEqual(await one("select registered_on::text as d, registered_on_source as src from core.skus where code = 'new-a1'"), { d: TODAY, src: 'portal' });
  assert.deepEqual((await q(`select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id join core.skus k on k.sku_id = x.sku_id where k.code = 'new-a1' and x.is_primary`)).map((x) => x.code), ['0001']);
  assert.deepEqual(await q(`select c.cost_jpy::int as jpy, c.cost_source as src, c.cost_status as st, c.valid_from::text as f, c.valid_to, c.reason from core.sku_costs c join core.skus k on k.sku_id = c.sku_id where k.code = 'new-a1'`),
    [{ jpy: 800, src: 'manual', st: 'COMPLETE', f: TODAY, valid_to: null, reason: '新商品の登録' }]);
  const ob = await one(`select o.status, o.schema_version, o.payload, o.payload_hash, o.request_id::text as rid, o.created_by, o.sku_id::text as sid from ops.product_hub_outbox o join core.skus k on k.sku_id = o.sku_id where k.code = 'new-a1'`);
  assert.deepEqual([ob.status, ob.schema_version, ob.rid, ob.created_by], ['pending', 'ph-card-v1', id, 'naka@test']);
  assert.equal(ob.payload_hash, O.cardPayloadHash(ob.payload));
  assert.deepEqual([ob.payload.code, ob.payload.price, ob.payload.asin, ob.payload.reference_urls, ob.payload.shipping, ob.payload.set_decision, ob.payload.yahoo.price, ob.payload.yahoo.category_id],
    ['new-a1', 1980, 'B0ABCDEFGH', ['https://ref.example/1', 'https://ref.example/2'], { code: 'S01', method: 'ゆうパケット', cost_jpy: 210 }, { decision: 'none', reason_code: 'low_demand', reason_text: null }, 2080, 12345]);
  assert.equal(ob.sid, r.sku_id); assert.equal(ob.payload.cdb_sku_id, undefined);   // SKU は知らせの行 (DB が振った番号) で結ぶ (0052)
  const rec = await one('select operation, status, sku_id::text as sku_id from ops.master_edit_requests where request_id = $1', [id]);
  assert.deepEqual(rec, { operation: 'sku_create', status: 'done', sku_id: r.sku_id });
  const ev = await q('select distinct entity_type, actor_type, actor_id, source_system, reason_text from events.master_change_events where request_id = $1 order by entity_type', [id]);
  assert.deepEqual(ev.map((e) => e.entity_type), ['product', 'sku', 'sku_cost', 'supplier_sku']);
  for (const e of ev) assert.deepEqual([e.actor_type, e.actor_id, e.source_system, e.reason_text], ['human', 'naka@test', 'portal_master_edit', '新しく仕入れる']);
});

await ta('[R5] 同じ request_id = 前の結果 (何も増えない) / 違う中身・違う人 = 409 / 同じコードをもう一度 = 409 code_taken', async () => {
  const r0 = newA1Result;
  const again = await R.registerNewSku(db, { actor: 'naka@test', requestId: r0.request_id, kind: 'single', code: 'new-a1', reason: '新しく仕入れる',
    values: single({ sales_class: '3', primary_supplier: '1', reorder_months: '1.5', cost: { jpy: '800' }, expiry_managed: '1', inbound_date_managed: '0' }),
    card: {
      official_url: 'https://maker.example/a1', amazon_url: 'https://www.amazon.co.jp/dp/B0ABCDEFGH', asin: 'b0abcdefgh',
      reference_urls: ['https://ref.example/1', 'https://ref.example/2', 'https://ref.example/1'],
      set_decision: { decision: 'none', reason_code: 'low_demand' },
      yahoo: { price: '2,080', price_sagawa: '2280', delivery_label: 'ネコポス', category_id: '12345', path: 'コスメ:スキンケア' },
    } }, { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES });
  assert.equal(again.replayed, true); assert.equal(again.sku_id, r0.sku_id);
  assert.equal((await one("select count(*)::int as n from core.skus where code = 'new-a1'")).n, 1);
  assert.equal((await one('select count(*)::int as n from ops.product_hub_outbox')).n, 1);
  await rejectsWith(reg('single', 'new-a1', single({ name: '別の名前' }), {}, { requestId: r0.request_id }), 409, 'request_id_reused');
  await rejectsWith(reg('single', 'new-a1', single(), {}, { requestId: r0.request_id, actor: 'other@test' }), 409, 'request_id_reused');
  await rejectsWith(reg('single', 'new-a1', single()), 409, 'code_taken');
});

await ta('[R6] セット: 構成は構成の依頼 (元の構成なし・core.sku_components は書かない)・税率 / 取扱 / 原価は構成品から・状態 draft・知らせ', async () => {
  const r = await reg('set', 'new-set-1', { name: '新しいセット', standard_price: '3,600', shipping_code: 'S02', components: [{ code: 's002', qty: 1 }, { code: 'S001', qty: 2 }] },
    { reference_urls: ['https://ref.example/set'] });
  assert.deepEqual([r.kind, r.state, r.tax, r.handling, r.cost], ['set', 'draft', { rate: 0.08, class: 'MIXED' }, 'active', { jpy: 400, source: 'set_calc' }]);
  assert.ok(r.warnings.some((w) => /混ざって/.test(w)));
  const s = await one(`select sku_kind, product_id, tax_rate::float8 as tax, tax_class, handling, handling_own, set_sales_class_override from core.skus where code = 'new-set-1'`);
  assert.deepEqual(s, { sku_kind: 'set', product_id: null, tax: 0.08, tax_class: 'MIXED', handling: 'active', handling_own: 'active', set_sales_class_override: null });
  assert.equal((await one(`select count(*)::int as n from core.sku_components where parent_sku_id = $1`, [r.sku_id])).n, 0);
  const cr = await one(`select rows, base_rows, status, reason from ops.sku_component_requests where set_sku_id = $1`, [r.sku_id]);
  assert.deepEqual([cr.rows.map((x) => [x.code, x.qty, x.sort]), cr.base_rows, cr.status], [[['s002', 1, 1], ['s001', 2, 2]], [], 'open']);
  assert.deepEqual(await regOf('new-set-1'), { state: 'draft', origin: 'new_entry', gen: null });
  assert.deepEqual((await one(`select payload from ops.product_hub_outbox where sku_id = $1`, [r.sku_id])).payload.components, [{ code: 's002', qty: 1 }, { code: 's001', qty: 2 }]);
  // 画面の読み方 (編集の印) も通る: 今の構成なし + 依頼の構成
  const cur = await W.readCurrent(db, r.sku_id, TODAY);
  assert.deepEqual([cur.components.length, cur.component_request.rows.map((x) => x.code)], [0, ['s002', 's001']]);
});

await ta('[R7] セットの導く値が決まらない = 登録しない (上書き・例外原価で通る)・導けるのに上書き = 400', async () => {
  let e = await rejectsWith(reg('set', 'new-set-2', { name: 'x', standard_price: '1000', shipping_code: 'S01', components: [{ code: 's003', qty: 1 }] }), 400, 'set_underivable');
  assert.match(e.extra.blockers.join(' '), /s003 の原価/);
  e = await rejectsWith(reg('set', 'new-set-2', { name: 'x', standard_price: '1000', shipping_code: 'S01', components: [{ code: 's001', qty: 1 }], set_sales_class_override: '2' }), 400);
  assert.match(e.message, /上書きはできません/);
  const r = await reg('set', 'new-set-2', { name: 'x', standard_price: '1000', shipping_code: 'S01', components: [{ code: 's003', qty: 1 }], exception_cost: { jpy: '90', reason: '見積' } }, { create: false });
  assert.deepEqual([r.cost, r.card], [{ jpy: 90, source: 'manual' }, null]);
  assert.equal((await one(`select count(*)::int as n from ops.product_hub_outbox where sku_id = $1`, [r.sku_id])).n, 0);
  assert.equal((await one(`select cost_status from core.sku_costs where sku_id = $1`, [r.sku_id])).cost_status, 'OVERRIDDEN');
});

await ta('[R8] 巻き戻った登録は何も残さない (SKU・状態・知らせ・原価)・失敗の記録だけ残る', async () => {
  const id = uuid();
  const before = await one('select (select count(*) from core.skus)::int as s, (select count(*) from ops.product_hub_outbox)::int as o, (select count(*) from ops.master_registrations)::int as r, (select count(*) from events.master_change_events)::int as e');
  await assert.rejects(() => reg('single', 'new-rb', single({ cost: { jpy: '10' } }), {}, { requestId: id, beforeCommit: async () => { throw new Error('commit の前に落ちた'); } }), /commit の前に落ちた/);
  const after = await one('select (select count(*) from core.skus)::int as s, (select count(*) from ops.product_hub_outbox)::int as o, (select count(*) from ops.master_registrations)::int as r, (select count(*) from events.master_change_events)::int as e');
  assert.deepEqual(after, before);
  assert.equal((await one('select status from ops.master_edit_requests where request_id = $1', [id])).status, 'failed');
});

await ta('[R9] 編集の印に登録の状態が入る (状態が変わった = 違う印)・やめた商品は直せない・下書きは直せる', async () => {
  const tk = await makeDraftSku('tok-1');
  const tk0 = W.editTokenOf(await W.readCurrent(db, tk, TODAY));
  await transition(tk, 'cancelled', { reason: '試験' });
  const tk1 = W.editTokenOf(await W.readCurrent(db, tk, TODAY));
  assert.notEqual(tk1, tk0);
  await rejectsWith(asEditor(() => W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 'tok-1', reason: null, seen: { token: tk1 }, values: { name: 'x' } }, { ownership: ALL_COMPANY, open: true, now: NOW })), 400, 'cancelled_sku');
  const a1 = await skuId('new-a1');
  const tokA1 = W.editTokenOf(await W.readCurrent(db, a1, TODAY));
  const r = await asEditor(() => W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 'new-a1', reason: null, seen: { token: tokA1 }, values: { reorder_months: '2' } }, { ownership: ALL_COMPANY, open: true, now: NOW }));
  assert.deepEqual(r.changed.map((c) => c.field), ['reorder_months']);
});

await ta('[R10] 画面のロール (master_edit) で単品・セットを登録でき、カードの知らせも取り込める (状態の表は関数だけが書く)', async () => {
  const r = await asRole('master_edit', () => reg('single', 'role-1', single({ name: 'ロールの単品', cost: { jpy: '100' }, primary_supplier: '1' })));
  assert.equal(r.state, 'draft');
  assert.deepEqual(await regOf('role-1'), { state: 'draft', origin: 'new_entry', gen: null });
  const res = await asRole('master_edit', () => O.runCardOutbox(db, (ev) => PH.applyCdbCardEvent(ev), { eventId: r.card.event_id }));
  assert.equal(res[0].status, 'done', JSON.stringify(res));
  const s = await asRole('master_edit', () => reg('set', 'role-set-1', { name: 'ロールのセット', standard_price: '2000', shipping_code: 'S02', components: [{ code: 's001', qty: 2 }] }, { create: false }));
  assert.deepEqual([s.kind, s.state, s.cost], ['set', 'draft', { jpy: 200, source: 'set_calc' }]);
});

await ta('[R11] 同じコードを NE (夜間ロード) がちょうど先に入れていた = 409 code_taken (500 にしない)', async () => {
  // コードを確かめた後・入れる前に入った形を作る: 夜間ロードの取引を開いたまま…は 1 接続ではできないので、入れた後に確かめを通るよう NE の元のコードの表からは外しておく
  await tx(async () => {
    await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'race-1', 'NE から')`);
    await pg.query(`select ops.quarantine_unregistered_skus('race')`);
  });
  // registerInTx の確かめを飛ばせないので、ここでは unique の違反を 409 にする写し方だけを確かめる (本物の並びは test-master-register-pg.mjs)
  const e = await rejectsWith(reg('single', 'race-1', single()), 409, 'code_taken');
  assert.equal(e.extra.field, 'code');
});

console.log('\n登録の関数と ⑤-1 (0051) の書き込みの約束');

await ta('[G1] DB の守りが見る列の持ち主のキー (ops.master_edit_owner_keys) は、登録が入れた行 (単品・セット) で NEW_ENTRY_KEYS に全部入っている', async () => {
  const keysOf = async (tbl, sql, params) => {
    const out = new Set();
    for (const r of await q(sql, params)) for (const k of (await one('select ops.master_edit_owner_keys($1, $2, null, $3::jsonb) as k', [tbl, 'INSERT', JSON.stringify(r.j)])).k) out.add(k);
    return out;
  };
  const rows = {
    single: [['core.products', `select to_jsonb(p) as j from core.products p join core.skus k on k.product_id = p.product_id where k.code = $1`],
      ['core.skus', `select to_jsonb(k) as j from core.skus k where k.code = $1`],
      ['core.supplier_skus', `select to_jsonb(x) as j from core.supplier_skus x join core.skus k on k.sku_id = x.sku_id where k.code = $1`],
      ['core.sku_costs', `select to_jsonb(c) as j from core.sku_costs c join core.skus k on k.sku_id = c.sku_id where k.code = $1`]],
    set: [['core.skus', `select to_jsonb(k) as j from core.skus k where k.code = $1`],
      ['core.sku_costs', `select to_jsonb(c) as j from core.sku_costs c join core.skus k on k.sku_id = c.sku_id where k.code = $1`],
      ['ops.sku_component_requests', `select to_jsonb(r) as j from ops.sku_component_requests r join core.skus k on k.sku_id = r.set_sku_id where k.code = $1`]],
  };
  for (const [kind, code] of [['single', 'new-a1'], ['set', 'new-set-1']]) {
    const all = new Set();
    for (const [tbl, sql] of rows[kind]) for (const k of await keysOf(tbl, sql, [code])) all.add(k);
    for (const k of ['skus.sku_kind', 'skus.name', 'skus.standard_price', 'skus.shipping', 'sku_costs']) assert.ok(all.has(k), `${kind}: ${k} を DB が見ていない`);
    const missing = [...all].filter((k) => !R.NEW_ENTRY_KEYS[kind].includes(k));
    assert.deepEqual(missing, [], `${kind}: DB の守りが見るのに NEW_ENTRY_KEYS に無いキー`);
  }
});

/** 登録の関数を直接呼ぶ (画面を通らない = 画面のロールが関数だけで何ができるか)。entry は関数の形 (0052 の 8d) */
const regEntry = (code, over = {}) => ({
  kind: 'single', code, started_at: null,
  product: { name: '直接の単品', sales_class: 3, expiry_managed: false, inbound_date_managed: null },
  sku: { name: '直接の単品', tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', standard_price_jpy: 1000, shipping_code: 'S01', shipping_method: 'ゆうパケット',
    shipping_cost_jpy: 210, reorder_months: null, set_sales_class_override: null, handling_own: null },
  supplier_id: null, cost: null, component_request: null, card: null, ...over,
});
const setEntry = (code, rows) => regEntry(code, { kind: 'set', product: null, sku: { ...regEntry(code).sku, name: '直接のセット', handling_own: 'active' },
  component_request: { rows, rows_hash: 'f'.repeat(64), reason: '直接の試験' } });
const callReg = (rid, entry, { actor = 'naka@test', ownership = ALL_COMPANY } = {}) =>
  pg.query('select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r', [rid, actor, '直接の試験', JSON.stringify(ownership), 'e'.repeat(64), JSON.stringify(entry)]);
/** 画面のロールで 1 つの取引を流して必ず巻き戻す (誤りは返す) */
const editorTx = (fn) => asEditor(async () => { await pg.query('begin'); try { return await fn(); } catch (e) { return e; } finally { await pg.query('rollback'); } });

await ta('[G2] 登録の約束 (sku_create) は登録の関数だけが書く: begin では作れない・関数を直接呼んでも DB の決まりで断る・関数の外では登録の約束で書けない', async () => {
  // ops.begin_master_write は sku_edit だけ (画面のロールは登録の約束を作れない)
  const s1 = await skuId('s001');
  const eBegin = await editorTx(() => pg.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)',
    [uuid(), 'naka@test', null, JSON.stringify(ALL_COMPANY), 'sku_create', s1, '0'.repeat(64), 'e'.repeat(64), '{}']));
  assert.match(String(eBegin?.message), /知らない操作/);
  // 守りの保険 (画面のロールには権限が無い道): 持ち主が取引の中だけ権限を足しても、登録の約束が無ければ 42501 (知らせの insert・下書きの状態を作る関数)
  const withGrant = async (grant, sql, params) => {
    await pg.query('begin');
    try { await pg.query(grant); await pg.query('set role master_edit'); return await pg.query(sql, params).then(() => null, (e) => e); } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  };
  const eOb = await withGrant('grant insert on ops.product_hub_outbox to master_edit', `insert into ops.product_hub_outbox (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
     values (1, $1, 'card_create', 'ph-card-v1', '{}', repeat('d', 64), gen_random_uuid(), 'naka@test')`, [s1]);
  assert.equal(eOb?.code, '42501'); assert.match(eOb.message, /master_write_session_required: カードの知らせ/);
  const eCr = await withGrant('grant execute on function ops.create_sku_registration(bigint, text, text, text) to master_edit', 'select ops.create_sku_registration($1, $2)', [s1, 'naka@test']);
  assert.equal(eCr?.code, '42501'); assert.match(eCr.message, /master_write_session_required: 下書きの状態/);
  // 関数を直接呼んでも DB の決まりで断る
  const s002 = Number(await skuId('s002')); const set001 = Number(await skuId('set001')); const tx2 = Number(await skuId('tx-2'));
  const inactive = (await one(`select supplier_id::text as id from core.suppliers where code = '0003'`)).id;
  for (const [label, entry, re, opts] of [
    ['使われているコード', regEntry('s001'), /^code_taken/],
    ['形の違うコード', regEntry('Upper-1'), /^code_shape/],
    ['単品なのに商品が無い', regEntry('g-x1', { product: null }), /invalid_input: 単品は商品/],
    ['セットなのに構成が無い', regEntry('g-x2', { kind: 'set', product: null }), /invalid_input: セットは構成の依頼/],
    ['構成品が登録をやめた商品', setEntry('g-x3', [{ sku_id: tx2, code: 'tx-2', qty: 1, sort: 1 }]), /^component_unusable/],
    ['構成品がセット', setEntry('g-x4', [{ sku_id: set001, code: 'set001', qty: 1, sort: 1 }]), /^component_unusable/],
    ['代表の仕入先が取引停止 (0051 の業務の約束)', regEntry('g-x5', { supplier_id: inactive }), /master_write_invariant/],
    ['持ち主表が切替のときと違う', regEntry('g-x6'), /^before_cutover: 持ち主表/, { ownership: MASTER_OWNERSHIP }],
  ]) {
    const e = await editorTx(() => callReg(uuid(), entry, opts));
    assert.ok(e instanceof Error, `${label}: 断らなかった`); assert.match(String(e.message), re, label);
  }
  void s002;
  for (const c of ['g-x1', 'g-x2', 'g-x3', 'g-x4', 'g-x5', 'g-x6']) assert.equal(await skuId(c), undefined, c);
  // 通る呼び方: 約束の行 (operation = sku_create・編集の印なし・db_user = 画面のロール)・関数の外では登録の約束で書けない (同じ取引でも)
  const rid = uuid();
  const after = await editorTx(async () => {
    await pg.query(`select set_config('core.actor_id', 'forged@evil', true), set_config('core.actor_type', 'human', true)`);
    const r = (await callReg(rid, regEntry('g-ok-0'))).rows[0].r;
    const eUpd = await pg.query('update core.skus set name = $2 where sku_id = $1', [r.sku_id, '関数の外で直す']).then(() => null, (e) => e);
    return { r, eUpd };
  });
  assert.ok(!(after instanceof Error), after && after.message);
  assert.equal(after.eUpd?.code, '42501'); assert.match(after.eUpd.message, /master_write_session_required/);
  // 合っていれば commit できる・記録は約束の人・request_id・理由 (core.actor_* の偽の値は使わない)・done はちょうど 1 つ
  const rid2 = uuid();
  const r2 = await asEditor(async () => {
    await pg.query('begin');
    try {
      await pg.query(`select set_config('core.actor_id', 'forged@evil', true), set_config('core.actor_type', 'human', true)`);
      const r = (await callReg(rid2, regEntry('g-ok-1', { cost: { jpy: 50, source: 'manual', status: 'COMPLETE', valid_from: TODAY, reason: '直接' } }))).rows[0].r;
      await pg.query('commit');
      return r;
    } catch (e) { await pg.query('rollback'); throw e; }
  });
  assert.equal(r2.state, 'draft');
  assert.deepEqual(await one('select operation, edit_token, db_user, sku_id::text as sku_id, actor_id from ops.master_write_sessions where request_id = $1', [rid2]),
    { operation: 'sku_create', edit_token: '0'.repeat(64), db_user: 'master_edit', sku_id: r2.sku_id, actor_id: 'naka@test' });
  assert.deepEqual(await q('select distinct actor_id, request_id, reason_text, db_user from events.master_change_events where request_id = $1', [rid2]),
    [{ actor_id: 'naka@test', request_id: rid2, reason_text: '直接の試験', db_user: 'master_edit' }]);
  assert.deepEqual(await q('select actor_id, request_id, reason from ops.master_registration_events where sku_id = $1', [r2.sku_id]), [{ actor_id: 'naka@test', request_id: rid2, reason: '直接の試験' }]);
  assert.deepEqual(await q('select operation, status, sku_id::text as sku_id from ops.master_edit_requests where request_id = $1', [rid2]), [{ operation: 'sku_create', status: 'done', sku_id: r2.sku_id }]);
  assert.equal(Number((await one(`select count(*)::int as n from events.master_change_events where actor_id = 'forged@evil'`)).n), 0);
  // 登録の約束も done が要る (0051 の commit の確かめは操作を問わない): 持ち主が約束の行だけ書いて commit = 断る
  //   持ち主表は今の epoch (active) と同じにする (0055 の入れる時の確かめ = 違う持ち主表の行はそもそも入らない)
  const sid = await skuId('s001');
  await assert.rejects(() => tx(() => pg.query(`insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash,
      versions, actor_id, source_system, db_user, phase, owner_hash, ownership) values (gen_random_uuid(), txid_current(), gen_random_uuid(), 'sku_create', $1, '{}', '{}', repeat('0', 64), repeat('e', 64),
      '{}', 'x', 'portal_master_edit', 'deploy', 'new_open', repeat('a', 64), ops.master_ownership_active_map())`, [sid])), /master_write_session_unfinished/);
  assert.equal(await skuId('g-ok-0'), undefined);
});

await ta('[G3] DB が段階・持ち主表で断った (ops.register_new_sku) = 409 before_cutover (何も書かない・失敗の記録)。画面の門と DB の門の両方を通る', async () => {
  // 画面の門は通り、DB にだけ違う持ち主表を渡す (画面の門と DB の門が食い違った形)
  const tamper = { query: (t, p) => (/ops\.register_new_sku/.test(t) ? db.query(t, [p[0], p[1], p[2], JSON.stringify(MASTER_OWNERSHIP), p[4], p[5]]) : db.query(t, p)) };
  const id = uuid();
  const e = await rejectsWith(asEditor(() => R.registerNewSku(tamper, { actor: 'naka@test', requestId: id, kind: 'single', code: 'g-db-1', values: single(), card: {} },
    { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES })), 409, 'before_cutover');
  assert.match(e.message, /新商品はまだ NE・product-hub で登録します \(DB が断った: 持ち主表が切替のときの記録と違う\)/);
  assert.equal(await skuId('g-db-1'), undefined);
  assert.equal((await one('select status from ops.master_edit_requests where request_id = $1', [id])).status, 'failed');
  // 画面の登録も同じ関数 = 登録の約束 (人・理由・画面のロール)
  const r = await reg('single', 'g-ok-2', single({ name: '守りの単品' }), { create: false }, { reason: '守りの理由' });
  assert.deepEqual(await one('select operation, actor_id, reason, db_user from ops.master_write_sessions where request_id = $1', [r.request_id]),
    { operation: 'sku_create', actor_id: 'naka@test', reason: '守りの理由', db_user: 'master_edit' });
});

await ta('[G4] 画面のロールが登録の関数を直接呼んで値を偽っても、DB が確かめる / 作り直す (Codex R3 Medium 1)', async () => {
  const s001 = Number(await skuId('s001'));
  const ship = { code: 'S01', method: 'ゆうパケット', cost_jpy: 210 };
  const cardOf = (code, kind, name, price, comps = []) => O.cardEventOf(O.buildCardPayload({ code, kind, name, price, shipping: ship, card: {}, components: comps, actor: 'naka@test' }));
  const one1 = (code, over = {}, sku = {}) => { const e = regEntry(code); return { ...e, ...over, sku: { ...e.sku, ...sku } }; };
  const setRows = [{ sku_id: s001, code: 's001', qty: 2, sort: 1 }];
  const goodCost = { jpy: 200, source: 'set_calc', status: 'COMPLETE', valid_from: TODAY, reason: '構成品から計算' };
  const set1 = (code, over = {}, sku = {}) => { const e = setEntry(code, setRows); return { ...e, cost: goodCost, ...over, sku: { ...e.sku, ...sku } }; };
  // 正しい形は通る (取引は巻き戻す)
  for (const e of [one1('g4-a', { card: cardOf('g4-a', 'single', '直接の単品', 1000) }), set1('g4-b', { card: cardOf('g4-b', 'set', '直接のセット', 1000, [{ code: 's001', qty: 2 }]) })]) {
    const r = await editorTx(async () => (await callReg(uuid(), e)).rows[0].r);
    assert.ok(!(r instanceof Error), `${e.code}: ${r && r.message}`);
  }
  const manual = (status, from = TODAY, source = 'manual') => ({ jpy: 10, source, status, valid_from: from, reason: '直接' });
  for (const [label, entry, re] of [
    ['単品の税率が無い', one1('g4-1', {}, { tax_rate: null }), /^invalid_value: 単品の税率/],
    ['税率 8% と STANDARD_10', one1('g4-2', {}, { tax_rate: 0.08, tax_class: 'STANDARD_10' }), /^invalid_value: 税率/],
    ['名前が空', one1('g4-3', { product: { ...regEntry('g4-3').product, name: '' } }, { name: '' }), /^invalid_value: 名前/],
    ['商品と SKU の名前が違う', one1('g4-3b', { product: { ...regEntry('g4-3b').product, name: '別の名前' } }), /^invalid_value: 商品の名前/],
    ['売価 0', one1('g4-4', {}, { standard_price_jpy: 0 }), /^invalid_value: 売価/],
    ['発送方法が無い', one1('g4-5', {}, { shipping_method: null }), /^invalid_value: 送料コードと発送方法/],
    ['単品の原価が set_calc', one1('g4-6', { cost: manual('COMPLETE', TODAY, 'set_calc') }), /^invalid_value: 単品の原価/],
    ['単品の原価が MISSING', one1('g4-7', { cost: manual('MISSING') }), /^invalid_value: 単品の原価/],
    ['原価の始まりが先の日', one1('g4-8', { cost: manual('COMPLETE', '2099-01-01') }), /^invalid_value: 原価は/],
    ['原価の始まりが前の日', one1('g4-8b', { cost: manual('COMPLETE', '2020-01-01') }), /^invalid_value: 原価は/],
    ['単品にセットだけの列', one1('g4-9', {}, { handling_own: 'active' }), /^invalid_value: 単品にセットだけの列/],
    ['単品の取扱が中止', one1('g4-9b', {}, { handling: 'discontinued' }), /^invalid_value: 新しい単品の取扱/],
    ['セットの税率が構成品と違う', set1('g4-10', {}, { tax_rate: 0.08, tax_class: 'REDUCED_8' }), /^derived_mismatch/],
    ['セットの取扱が構成品と違う', set1('g4-11', {}, { handling: 'discontinued' }), /^derived_mismatch/],
    ['セットの原価が構成品の合計と違う', set1('g4-12', { cost: { ...goodCost, jpy: 1 } }), /^derived_mismatch/],
    ['セットの原価が MISSING', set1('g4-12b', { cost: { ...goodCost, source: 'manual', status: 'MISSING' } }), /^invalid_value: セットの原価/],
    ['導けるのに売上分類を上書き', set1('g4-13', {}, { set_sales_class_override: 1 }), /^derived_mismatch/],
    ['構成品のコードが SKU と違う', { ...set1('g4-14'), component_request: { rows: [{ ...setRows[0], code: 's002' }], rows_hash: 'f'.repeat(64), reason: 'x' } }, /^component_unusable/],
    ['構成品の並びが 1 からでない', { ...set1('g4-14b'), component_request: { rows: [{ ...setRows[0], sort: 2 }], rows_hash: 'f'.repeat(64), reason: 'x' } }, /^invalid_input: 構成品の行/],
    ['カードの売価が SKU と違う', one1('g4-15', { card: cardOf('g4-15', 'single', '直接の単品', 999) }), /^invalid_value: カードの知らせ/],
    ['カードに SKU の番号', one1('g4-16', { card: O.cardEventOf({ ...cardOf('g4-16', 'single', '直接の単品', 1000).payload, cdb_sku_id: '1' }) }), /^invalid_value: カードの知らせ/],
    ['カードの作った人が違う', one1('g4-17', { card: O.cardEventOf({ ...cardOf('g4-17', 'single', '直接の単品', 1000).payload, created_by: 'evil@x' }) }), /^invalid_value: カードの知らせ/],
  ]) {
    const e = await editorTx(() => callReg(uuid(), entry));
    assert.ok(e instanceof Error, `${label}: 断らなかった`); assert.match(String(e.message), re, label);
  }
  // rows_hash・カードの hash・約束のハッシュは DB が作る (入ってきた偽の hash は使わない)。DB の hash = 画面・取り込みと同じ形 (JS の stable)
  const rid = uuid();
  const card = { ...cardOf('g4-ok', 'set', '直接のセット', 1000, [{ code: 's001', qty: 2 }]), payload_hash: 'f'.repeat(64) };
  const r = await asEditor(async () => {
    await pg.query('begin');
    try { const x = (await callReg(rid, set1('g4-ok', { card }))).rows[0].r; await pg.query('commit'); return x; } catch (e) { await pg.query('rollback'); throw e; }
  });
  const req = await one('select rows, rows_hash from ops.sku_component_requests where set_sku_id = $1', [r.sku_id]);
  assert.equal(req.rows_hash, W.sha256(W.stable(req.rows))); assert.notEqual(req.rows_hash, 'f'.repeat(64));
  const ob = await one('select payload, payload_hash from ops.product_hub_outbox where sku_id = $1', [r.sku_id]);
  assert.equal(ob.payload_hash, O.cardPayloadHash(ob.payload)); assert.notEqual(ob.payload_hash, 'f'.repeat(64));
  const sess = await one('select payload_hash from ops.master_write_sessions where request_id = $1', [rid]);
  const done = await one('select payload_hash, result from ops.master_edit_requests where request_id = $1', [rid]);
  assert.equal(sess.payload_hash, done.payload_hash); assert.notEqual(done.payload_hash, 'e'.repeat(64));
  assert.equal(done.result.request_payload_hash, 'e'.repeat(64));
  assert.deepEqual(await one('select tax_rate::float8 as t, tax_class, handling from core.skus where sku_id = $1', [r.sku_id]), { t: 0.1, tax_class: 'STANDARD_10', handling: 'active' });
  assert.deepEqual(await one('select cost_jpy::int as j, cost_source as s, valid_from::text as f from core.sku_costs where sku_id = $1', [r.sku_id]), { j: 200, s: 'set_calc', f: TODAY });
});

await ta('[G5] 保存の記録の結果は DB の値だけで作る (呼び手の result は断る・Codex R4 Medium)・カードの知らせの欄の名前は決まった ASCII だけ (Codex R4 Low)', async () => {
  const s001 = Number(await skuId('s001'));
  // 呼び手が結果を渡す (ok:false・別のコード / 種類 / request_id / 税率 / 構成品) = 断る
  for (const forged of [{ ok: false }, { code: 'other', kind: 'set', request_id: uuid(), tax: { rate: 0.08, class: 'REDUCED_8' }, components: [{ code: 'x', qty: 9 }] }, {}]) {
    const e = await editorTx(() => callReg(uuid(), { ...regEntry('g5-x'), result: forged }));
    assert.ok(e instanceof Error); assert.match(String(e.message), /^invalid_input: 登録の結果 \(result\) は DB が作る/);
  }
  // 通った呼び出しの結果 = DB が書いた値そのもの (保存の記録と関数の答えが同じ)
  const rid = uuid();
  const rows = [{ sku_id: s001, code: 's001', qty: 3, sort: 1 }];
  const entry = { ...setEntry('g5-set', rows), sku: { ...setEntry('g5-set', rows).sku, name: '中止の印のセット_白ビ袋' },
    cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE', valid_from: TODAY, reason: '構成品から計算' } };
  const out = await asEditor(async () => {
    await pg.query('begin');
    try { const x = (await callReg(rid, entry)).rows[0].r; await pg.query('commit'); return x; } catch (e) { await pg.query('rollback'); throw e; }
  });
  const stored = (await one('select result from ops.master_edit_requests where request_id = $1', [rid])).result;
  assert.deepEqual(stored, out);
  const k = await one('select sku_id::text as id, code, sku_kind, tax_rate::float8 as t, tax_class, handling, shipping_code, shipping_method, shipping_cost_jpy::int as sc from core.skus where code = $1', ['g5-set']);
  assert.deepEqual([out.ok, out.code, out.kind, out.sku_id, out.state, out.request_id], [true, 'g5-set', 'set', k.id, 'draft', rid]);
  assert.deepEqual([out.tax, out.handling, out.cost, out.shipping, out.components], [{ rate: k.t, class: k.tax_class }, k.handling, { jpy: 300, source: 'set_calc' },
    { code: k.shipping_code, method: k.shipping_method, cost_jpy: k.sc }, [{ code: 's001', qty: 3 }]]);
  assert.deepEqual(out.warnings, ['名前の末尾に資材の印があります。資材は梱包アプリで登録します (新しい名前には付けない・D-47)']);
  assert.equal(out.ne_steps.length, 2); assert.equal(out.card, null); assert.equal(out.request_payload_hash, 'e'.repeat(64));
  // カードの知らせ: 知らない欄・ASCII でない欄の名前は断る (JS と DB で欄の並びが違うかもしれない)
  const card = (extra) => O.cardEventOf({ ...O.buildCardPayload({ code: 'g5-c', kind: 'single', name: '直接の単品', price: 1000,
    shipping: { code: 'S01', method: 'ゆうパケット', cost_jpy: 210 }, card: {}, components: [], actor: 'naka@test' }), ...extra });
  for (const [label, extra] of [['知らない欄', { extra_field: 1 }], ['ASCII でない欄の名前', { '\u{1F600}': 'x' }], ['入れ子の知らない欄', { yahoo: { price: 1, '\uE000': 2 } }],
    ['入れ子の知らない欄 (セットの判断)', { set_decision: { decision: 'hold', reason_code: null, reason_text: null, note: 'x' } }], ['参考 URL が文字でない', { reference_urls: [1] }]]) {
    const e = await editorTx(() => callReg(uuid(), regEntry('g5-c', { card: card(extra) })));
    assert.ok(e instanceof Error, `${label}: 断らなかった`); assert.match(String(e.message), /^invalid_value: カードの知らせ/, label);
  }
  // js_stable そのものも ASCII でないキーは断る
  await assert.rejects(() => pg.query(`select ops.js_stable('{"\u{1F600}": 1, "\uE000": 2}'::jsonb)`), (e) => /JSON のキーは ASCII だけ/.test(e.message));
  assert.equal((await one(`select ops.js_stable('{"b": 1, "a": [true, null, "x"]}'::jsonb) as s`)).s, W.stable({ b: 1, a: [true, null, 'x'] }));
});

console.log('\nproduct-hub のカードの知らせ (outbox)');

const ph = PHDB.getDB();
ph.prepare(`INSERT INTO ph_shipping_method_map (ne_label, rakuten_group) VALUES ('ゆうパケット', '9'), ('宅急便', '7')`).run();
const apply = (ev) => PH.applyCdbCardEvent(ev);
const outboxOf = async (code) => one(`select o.event_id::text as event_id, o.status, o.attempts, o.last_error, o.result, o.lease_owner from ops.product_hub_outbox o join core.skus k on k.sku_id = o.sku_id where k.code = $1`, [code]);
const draftOf = (code) => ph.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code);
/** 知らせの取り込み (画面だけのロール = 借りる・結果を書くのは関数だけ) */
const runCards = (opts) => asEditor(() => O.runCardOutbox(db, apply, opts));

await ta('[O1] 取り込む: カード 1 枚 (名前・売価・URL・ASIN・cdb_sku_id)・参考 URL・作らない判断・Yahoo! (ヤフーだけ別)・楽天の配送方法 (対応表)・記録', async () => {
  const ev = await outboxOf('new-a1');
  const res = await runCards({ eventId: ev.event_id });
  assert.equal(res.length, 1); assert.equal(res[0].status, 'done'); assert.equal(res[0].recorded, true);
  const d = draftOf('new-a1');
  assert.deepEqual([d.name, d.price, d.official_url, d.asin, d.amazon_url, d.created_by, String(d.cdb_sku_id)],
    ['新しい単品', 1980, 'https://maker.example/a1', 'B0ABCDEFGH', 'https://www.amazon.co.jp/dp/B0ABCDEFGH', 'naka@test', newA1Result.sku_id]);
  assert.deepEqual(ph.prepare('SELECT url FROM draft_reference_urls WHERE draft_id = ? ORDER BY sort').all(d.id).map((x) => x.url), ['https://ref.example/1', 'https://ref.example/2']);
  assert.deepEqual(ph.prepare('SELECT decision, reason_code FROM draft_set_decisions WHERE draft_id = ?').all(d.id), [{ decision: 'none', reason_code: 'low_demand' }]);
  assert.deepEqual(ph.prepare('SELECT yahoo_price, yahoo_price_sagawa, delivery_label, shipping_override, yahoo_category_id, yahoo_path, tax_rate FROM draft_yahoo WHERE draft_id = ?').get(d.id),
    { yahoo_price: 2080, yahoo_price_sagawa: 2280, delivery_label: 'ネコポス', shipping_override: 1, yahoo_category_id: 12345, yahoo_path: 'コスメ:スキンケア', tax_rate: null });
  assert.equal(ph.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(d.id).shipping_method_group, '9');
  const e = ph.prepare('SELECT outcome, draft_id, shipping_status FROM ph_cdb_card_events WHERE event_id = ?').get(ev.event_id);
  assert.deepEqual(e, { outcome: 'created', draft_id: d.id, shipping_status: 'mapped' });
  const after = await outboxOf('new-a1');
  assert.deepEqual([after.status, after.result.draft_id, after.lease_owner], ['done', d.id, null]);
  assert.ok(ph.prepare("SELECT 1 FROM draft_events WHERE draft_id = ? AND event = 'created_from_cdb'").get(d.id));
});

await ta('[O2] 冪等: もう一度流しても 1 枚 (済んだ知らせは取らない・同じ知らせを直接渡しても記録の答え・別の知らせでも同じ SKU なら結ぶだけ)', async () => {
  const ev = await outboxOf('new-a1');
  assert.deepEqual(await runCards({ eventId: ev.event_id, manual: true }), []);
  const payload = (await one('select payload from ops.product_hub_outbox where event_id = $1', [ev.event_id])).payload;
  const sid = await skuId('new-a1');
  const again = PH.applyCdbCardEvent({ event_id: ev.event_id, sku_id: sid, schema_version: 'ph-card-v1', payload });
  assert.deepEqual([again.outcome, again.replayed], ['created', true]);
  const other = PH.applyCdbCardEvent({ event_id: uuid(), sku_id: sid, schema_version: 'ph-card-v1', payload });
  assert.equal(other.outcome, 'linked');
  assert.equal(ph.prepare("SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'new-a1'").get().c, 1);
  // 済んだ知らせは変えられない
  await assert.rejects(() => pg.query(`update ops.product_hub_outbox set status = 'pending', done_at = null where event_id = $1`, [ev.event_id]), /済んだ/);
});

await ta('[O3] 同じ商品コードのカードがもうある = 衝突 (増やさない・直さない・記録)。古いカードを片付けてから「もう一度」で作れる', async () => {
  const r = await reg('single', 'new-a2', single({ name: '衝突する単品' }));
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('new-a2', '前から product-hub にあったカード', 'someone')`).run();
  const old = draftOf('new-a2');
  let res = await runCards({ eventId: r.card.event_id });
  assert.equal(res[0].status, 'conflict');
  assert.equal(ph.prepare("SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'new-a2'").get().c, 1);
  assert.equal(draftOf('new-a2').name, '前から product-hub にあったカード');
  assert.equal(draftOf('new-a2').cdb_sku_id, null);
  const ob = await outboxOf('new-a2');
  assert.deepEqual([ob.status, ob.result.conflict_draft_id], ['conflict', old.id]);
  assert.match(ob.last_error, /#\d+/);
  assert.equal(ph.prepare('SELECT outcome FROM ph_cdb_card_events WHERE event_id = ?').get(r.card.event_id).outcome, 'conflict');
  // 自動では衝突を試さない
  assert.deepEqual(await runCards({ eventId: r.card.event_id }), []);
  ph.prepare('DELETE FROM product_drafts WHERE id = ?').run(old.id);
  res = await runCards({ eventId: r.card.event_id, manual: true });
  assert.equal(res[0].status, 'done');
  assert.equal(String(draftOf('new-a2').cdb_sku_id), r.sku_id);
});

await ta('[O4] SQLite の失敗 = failed のまま (知らせは残る)・もう一度で作れる・自動は回数の上限まで (その後は人が押す)', async () => {
  const r = await reg('single', 'new-a3', single({ name: '失敗する単品' }));
  const boom = () => { throw new Error('SQLITE_BUSY: database is locked'); };
  let res = await O.runCardOutbox(db, boom, { eventId: r.card.event_id });
  assert.deepEqual([res[0].status, res[0].error], ['failed', 'SQLITE_BUSY: database is locked']);
  let ob = await outboxOf('new-a3');
  assert.deepEqual([ob.status, ob.attempts, ob.lease_owner], ['failed', 1, null]);
  assert.equal(draftOf('new-a3'), undefined);
  await pg.query(`update ops.product_hub_outbox set attempts = $2 where event_id = $1`, [r.card.event_id, O.CARD_MAX_AUTO_ATTEMPTS]);
  assert.deepEqual(await runCards({ eventId: r.card.event_id }), []);   // 自動はもう試さない
  res = await runCards({ eventId: r.card.event_id, manual: true });   // 人が押す
  assert.equal(res[0].status, 'done');
  ob = await outboxOf('new-a3');
  assert.deepEqual([ob.status, ob.attempts], ['done', O.CARD_MAX_AUTO_ATTEMPTS + 1]);
  assert.ok(draftOf('new-a3'));
});

await ta('[O5] 送料コードに楽天の配送方法の対応が無い = 楽天の配送方法は空・記録は unmapped・ボードの「要確認」に出る (選んだら消える)', async () => {
  const r = await reg('single', 'new-a4', single({ name: '謎の便の単品', shipping_code: 'S03' }), { yahoo: { price: '999' } });
  await runCards({ eventId: r.card.event_id });
  const d = draftOf('new-a4');
  assert.equal(ph.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(d.id), undefined);
  assert.equal(ph.prepare('SELECT shipping_status FROM ph_cdb_card_events WHERE draft_id = ?').get(d.id).shipping_status, 'unmapped');
  assert.deepEqual(ph.prepare('SELECT delivery_label, shipping_override FROM draft_yahoo WHERE draft_id = ?').get(d.id), { delivery_label: null, shipping_override: 0 });
  assert.ok(PH.cdbShippingCheckIds(ph).has(d.id));
  assert.ok(!PH.cdbShippingCheckIds(ph).has(draftOf('new-a1').id));
  assert.ok(ph.prepare("SELECT 1 FROM draft_events WHERE draft_id = ? AND event = 'cdb_shipping_unmapped'").get(d.id));
  // 名前が似ているだけでは推し量らない (対応表に無い = 空)
  assert.equal(PH.mapCdbShipping(ph, { code: 'X', method: 'ネコポス' }).status, 'unmapped');
  // 対応先が「現在使用不可」のグループ = 使えない = 要確認 (仮レビュー L4)
  ph.prepare(`INSERT INTO ph_shipping_method_map (ne_label, rakuten_group) VALUES ('古い便', '2')`).run();
  assert.equal(PH.mapCdbShipping(ph, { code: 'Y', method: '古い便' }).status, 'unmapped');
  ph.prepare(`INSERT INTO draft_rakuten (draft_id, shipping_method_group) VALUES (?, '1')`).run(d.id);
  assert.ok(!PH.cdbShippingCheckIds(ph).has(d.id));
});

await ta('[O6] 知らせの中身は変えられない・消せない・hash が違う知らせは取り込まない・借り (lease) の間はほかが取らない', async () => {
  const r = await reg('single', 'new-a5', single({ name: '借りの単品' }));
  await assert.rejects(() => pg.query(`update ops.product_hub_outbox set payload = '{}'::jsonb where event_id = $1`, [r.card.event_id]), /変えない/);
  await assert.rejects(() => pg.query(`delete from ops.product_hub_outbox where event_id = $1`, [r.card.event_id]), /消さない/);
  await assert.rejects(() => pg.query('truncate ops.product_hub_outbox'), /append-only/);
  // 画面のロールは状態・結果・借りの列を直接書けない (仮レビュー L7)・借りていない人の結果は書かれない
  assert.equal(await asEditor(() => pgCode(pg.query(`update ops.product_hub_outbox set status = 'done', done_at = now() where event_id = $1`, [r.card.event_id]))), '42501');
  assert.equal(await asEditor(() => pgCode(pg.query(`update ops.product_hub_outbox set lease_owner = 'x', leased_until = now() where event_id = $1`, [r.card.event_id]))), '42501');
  assert.equal((await asEditor(() => one(`select ops.finish_card_event($1::uuid, 'someone-else', 'done', '{}'::jsonb, null) as ok`, [r.card.event_id]))).ok, false);
  assert.equal((await outboxOf('new-a5')).status, 'pending');
  // 借りの間 (ほかの処理が取り込み中) は取らない
  await pg.query(`update ops.product_hub_outbox set lease_owner = 'other', leased_until = now() + interval '1 minute' where event_id = $1`, [r.card.event_id]);
  assert.deepEqual(await runCards({ eventId: r.card.event_id, manual: true }), []);
  assert.equal((await O.readCardEvent(db, r.sku_id)).leased, true);
  await pg.query(`update ops.product_hub_outbox set leased_until = now() - interval '1 second' where event_id = $1`, [r.card.event_id]);   // 期限切れ = 取れる
  assert.equal((await runCards({ eventId: r.card.event_id }))[0].status, 'done');
  // hash が違う知らせ (書いた後の中身と合わない) は取り込まない
  const sid = await skuId('s002');
  const bad = (await pg.query(`insert into ops.product_hub_outbox (sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
     values ($1, 'card_create', 'ph-card-v1', '{"schema":"ph-card-v1","code":"s002"}'::jsonb, $2, $3, 'x') returning event_id::text as id`, [sid, 'c'.repeat(64), uuid()])).rows[0].id;
  const res = await runCards({ eventId: bad });
  assert.deepEqual([res[0].status, /payload_hash_mismatch/.test(res[0].error)], ['failed', true]);
  assert.equal(draftOf('s002'), undefined);
});

await ta('[O7] 衝突を人が解く = 既存のカードをこの商品に結ぶ (SQLite で確かめ直して結ぶ → 知らせを done)・もう一度押しても同じ・確かめに落ちたら conflict のまま・SQLite だけ結べた後も冪等', async () => {
  const r = await reg('single', 'new-a6', single({ name: '結ぶ単品' }), { official_url: 'https://maker.example/a6', reference_urls: ['https://ref.example/a6'], yahoo: { price: '2100', path: 'a6' } });
  // 前からのカード: 売価と Yahoo!売価が入っている (変えない)・公式ページ URL・参考 URL・楽天の配送方法・Yahoo!path は空 (埋める)
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, price, created_by) VALUES ('New-A6 ', '前からのカード', 500, 'someone')`).run();
  PHDB.upsertDraftYahoo(ph, draftOf('new-a6').id, { yahoo_price: 999 });
  const old = draftOf('new-a6');
  await runCards({ eventId: r.card.event_id });
  assert.equal((await outboxOf('new-a6')).status, 'conflict');
  const link = (ev, o) => PH.linkCdbCardToExisting(ev, o);
  // 確かめに落ちる (カードの商品コードがその間に変わった) = conflict のまま・結ばない
  ph.prepare(`UPDATE product_drafts SET ne_code = 'other-code' WHERE id = ?`).run(old.id);
  // 画面が見ていたカードと違う番号 = 結ばない (仮レビュー L1)
  assert.deepEqual(await asEditor(() => O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test', expectedDraftId: old.id + 1000 })), { ok: false, reason: 'draft_mismatch', draft_id: old.id });
  assert.equal((await outboxOf('new-a6')).status, 'conflict');
  await assert.rejects(() => asEditor(() => O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test', expectedDraftId: old.id })), /商品コード/);
  assert.equal((await outboxOf('new-a6')).status, 'conflict');
  assert.equal(ph.prepare('SELECT cdb_sku_id FROM product_drafts WHERE id = ?').get(old.id).cdb_sku_id, null);
  ph.prepare(`UPDATE product_drafts SET ne_code = 'New-A6 ' WHERE id = ?`).run(old.id);
  const out = await asEditor(() => O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test', expectedDraftId: old.id }));
  assert.deepEqual([out.ok, out.draft_id, out.already], [true, old.id, false]);
  // 空の欄は画面 D の値で埋め、入っている欄は変えない (仮レビュー L2)
  assert.deepEqual(out.applied.sort(), ['Yahoo!path', '公式ページ URL', '参考 URL 1 件', '楽天の配送方法'].sort());
  assert.deepEqual(out.not_applied.map((x) => [x.label, x.card, x.entered]), [['売価', 500, 1980], ['Yahoo!売価', 999, 2100]]);
  const linked = draftOf('new-a6');
  assert.deepEqual([linked.price, linked.official_url], [500, 'https://maker.example/a6']);
  assert.equal(ph.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(old.id).shipping_method_group, '9');
  assert.deepEqual(ph.prepare('SELECT yahoo_price, yahoo_path FROM draft_yahoo WHERE draft_id = ?').get(old.id), { yahoo_price: 999, yahoo_path: 'a6' });
  assert.deepEqual((await outboxOf('new-a6')).result.not_applied.map((x) => x.label), ['売価', 'Yahoo!売価']);
  const ob = await outboxOf('new-a6');
  assert.deepEqual([ob.status, ob.result.outcome, ob.result.draft_id, ob.result.linked_by, ob.last_error], ['done', 'linked', old.id, 'naka@test', null]);
  assert.equal(String(ph.prepare('SELECT cdb_sku_id FROM product_drafts WHERE id = ?').get(old.id).cdb_sku_id), r.sku_id);
  assert.equal(ph.prepare("SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'new-a6'").get().c, 1);
  assert.equal(ph.prepare('SELECT outcome FROM ph_cdb_card_events WHERE event_id = ?').get(r.card.event_id).outcome, 'linked');
  assert.ok(ph.prepare("SELECT 1 FROM draft_events WHERE draft_id = ? AND event = 'cdb_card_linked'").get(old.id));
  const again = await asEditor(() => O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test', expectedDraftId: old.id }));
  assert.deepEqual([again.ok, again.already, again.reason], [true, true, 'already_done']);
  // SQLite では結べたが Postgres を done にできなかった = 次の取り込み (もう一度) が「もう結んである」で done にする
  const r2 = await reg('single', 'new-a7', single({ name: '結ぶ単品 2' }));
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('new-a7', '前からのカード 2', 'someone')`).run();
  await runCards({ eventId: r2.card.event_id });
  const payload = (await one('select payload from ops.product_hub_outbox where event_id = $1', [r2.card.event_id])).payload;
  PH.linkCdbCardToExisting({ event_id: r2.card.event_id, sku_id: r2.sku_id, schema_version: 'ph-card-v1', payload }, { draftId: draftOf('new-a7').id, actor: 'naka@test' });
  assert.equal((await outboxOf('new-a7')).status, 'conflict');
  const res = await runCards({ eventId: r2.card.event_id, manual: true });
  assert.deepEqual([res[0].status, res[0].result.outcome], ['done', 'linked']);
  // 衝突でない知らせは結ばない
  assert.equal((await O.linkCardToExisting(db, link, { skuId: await skuId('new-a1'), actor: 'x', expectedDraftId: 1 })).reason, 'already_done');
});

await ta('[O8] セットのカードに構成品 (draft_set_members) を入れる (仮レビュー L3)・工程は単品と同じ (セットの派生にしない)', async () => {
  const r = await reg('set', 'card-set-1', { name: 'カードのセット', standard_price: '3000', shipping_code: 'S02', components: [{ code: 's002', qty: 1 }, { code: 's001', qty: 3 }] });
  const res = await runCards({ eventId: r.card.event_id });
  assert.equal(res[0].status, 'done');
  const d = draftOf('card-set-1');
  assert.deepEqual(ph.prepare('SELECT member_ne_code AS c, qty, sort FROM draft_set_members WHERE set_draft_id = ? ORDER BY sort').all(d.id), [{ c: 's002', qty: 1, sort: 0 }, { c: 's001', qty: 3, sort: 1 }]);
  assert.equal(d.parent_draft_id, null);
});

await ta('[O9] カード作成待ち・衝突が一覧の「カード」で絞れる・1 行のまとめ (毎朝のまとめ用・読むだけ) (仮レビュー L6)・送料の対応の数え (読むだけ・L4)', async () => {
  const R2 = await import('../apps/master-edit/read.mjs');
  const waiting = (await R2.listSkus(db, { card: 'waiting' }, { now: NOW })).rows.map((x) => x.code);
  const conflict = (await R2.listSkus(db, { card: 'conflict' }, { now: NOW })).rows;
  const open = await q(`select k.code, o.status from ops.product_hub_outbox o join core.skus k on k.sku_id = o.sku_id where o.status <> 'done' order by k.code_norm`);
  assert.deepEqual(waiting, open.filter((x) => x.status !== 'conflict').map((x) => x.code));
  assert.deepEqual(conflict.map((x) => x.code), open.filter((x) => x.status === 'conflict').map((x) => x.code));
  for (const row of conflict) assert.ok(row.flags.includes('カードの衝突'), JSON.stringify(row.flags));
  // 登録の状態 × カード = 両方に合う商品だけ (Codex R2 Low: カードだけで絞っていた)。s002 = 利用可 (backfill) で知らせが失敗 = カードだけなら出る
  assert.ok(waiting.includes('s002'), JSON.stringify(waiting));
  const both = (await R2.listSkus(db, { reg: 'draft', card: 'waiting' }, { now: NOW })).rows.map((x) => x.code);
  const bothWant = (await q(`select k.code from core.skus k join ops.master_registrations r on r.sku_id = k.sku_id
     where r.state = 'draft' and exists (select 1 from ops.product_hub_outbox o where o.sku_id = k.sku_id and o.status in ('pending', 'failed')) order by k.code_norm`)).map((x) => x.code);
  assert.ok(bothWant.length > 0);
  assert.deepEqual(both, bothWant);
  assert.ok(!both.includes('s002'));
  assert.deepEqual((await R2.listSkus(db, { reg: 'available', card: 'waiting' }, { now: NOW })).rows.map((x) => x.code), ['s002']);
  const { summarize } = await import('./company-db/master-register-summary.mjs');
  const sm = await summarize(db);
  assert.match(sm.line, /新商品の登録: 下書き \d+/);
  const nWait = open.filter((x) => x.status !== 'conflict').length;
  assert.ok(sm.line.includes(nWait ? `カード作成待ち ${nWait}` : 'カード作成待ちなし'), sm.line);
  assert.equal(sm.counts.registrations.draft, Number((await one(`select count(*)::int as n from ops.master_registrations where state = 'draft'`)).n));
  // 見張りのロールでも読める
  const w = await asRole('watcher', () => summarize(db));
  assert.equal(w.line, sm.line);
  // 送料の対応の数え (送料の表 → ph_shipping_method_map)
  ph.prepare(`INSERT OR REPLACE INTO mirror_shipping_rates (shipping_code, 小分類区分名称, source_run_id, source_row_hash, synced_at) VALUES ('S01', 'ゆうパケット', 't', 'h', 'now'), ('S03', '謎の便', 't', 'h', 'now'), ('S04', '古い便', 't', 'h', 'now')`).run();
  const { shippingMapReport } = await import('../apps/product-hub/scripts/shipping-map-report.mjs');
  const rp = shippingMapReport(ph);
  const byCode = Object.fromEntries(rp.rows.map((x) => [x.code, x.status]));
  assert.deepEqual([byCode.S01, byCode.S03, byCode.S04], ['mapped', 'unmapped', 'unusable_group']);
  assert.ok(rp.orphanLabels.some((x) => x.label === '宅急便'));   // 対応表にあって送料の表に無い名前
});

await ta('[O10] 同じ商品コードのカードが 2 枚以上 (一意の index が張れない DB) = どれか決められない衝突: 作らない・結ばない・知らせは done にしない (Codex R2 Medium)', async () => {
  ph.exec('DROP INDEX IF EXISTS idx_product_drafts_ne_norm');   // 前からの重なりで index を張れなかった DB (db.js) と同じ形
  const link = (ev, o) => PH.linkCdbCardToExisting(ev, o);
  const cardsOf = (code) => ph.prepare('SELECT id, cdb_sku_id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? ORDER BY id').all(code);
  // 1. 取り込みの時に 2 枚 = 決められない衝突 (どちらにも結ばない・新しいカードも作らない)
  const r = await reg('single', 'dup-1', single({ name: '重なったコードの単品' }));
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('dup-1', '重なったカード 1', 'someone'), ('DUP-1 ', '重なったカード 2', 'someone')`).run();
  const ids = cardsOf('dup-1').map((x) => x.id);
  assert.equal(ids.length, 2);
  const res = await runCards({ eventId: r.card.event_id });
  assert.equal(res[0].status, 'conflict');
  let ob = await outboxOf('dup-1');
  assert.deepEqual([ob.status, ob.result.ambiguous, ob.result.conflict_draft_id, ob.result.conflict_draft_ids], ['conflict', true, null, ids]);
  assert.match(ob.last_error, /2 枚/);
  assert.deepEqual(cardsOf('dup-1').map((x) => x.cdb_sku_id), [null, null]);
  assert.equal(ph.prepare('SELECT outcome, conflict_draft_id FROM ph_cdb_card_events WHERE event_id = ?').get(r.card.event_id).outcome, 'conflict');
  // 「結ぶ」= 決められない (どちらの番号でも)・知らせは conflict のまま
  for (const id of ids) {
    const out = await asEditor(() => O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test', expectedDraftId: id }));
    assert.deepEqual([out.ok, out.reason, out.draft_ids], [false, 'ambiguous', ids]);
  }
  const payload = (await one('select payload from ops.product_hub_outbox where event_id = $1', [r.card.event_id])).payload;
  for (const id of ids) assert.throws(() => PH.linkCdbCardToExisting({ event_id: r.card.event_id, sku_id: r.sku_id, schema_version: 'ph-card-v1', payload }, { draftId: id, actor: 'x' }), /2 枚/);
  assert.equal((await outboxOf('dup-1')).status, 'conflict');
  assert.deepEqual(cardsOf('dup-1').map((x) => x.cdb_sku_id), [null, null]);
  // 2. 取り込みの時は 1 枚 (ふつうの衝突) → 結ぶ前に同じコードのカードが増えた = 結ぶときに確かめ直して結ばない (知らせは「決められない」に)
  const r2 = await reg('single', 'dup-2', single({ name: '後から重なる単品' }));
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('dup-2', '前からのカード', 'someone')`).run();
  await runCards({ eventId: r2.card.event_id });
  const first = draftOf('dup-2');
  assert.equal((await outboxOf('dup-2')).result.conflict_draft_id, first.id);
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('Dup-2', '後から増えたカード', 'someone')`).run();
  const out2 = await asEditor(() => O.linkCardToExisting(db, link, { skuId: r2.sku_id, actor: 'naka@test', expectedDraftId: first.id }));
  assert.deepEqual([out2.ok, out2.reason, out2.draft_ids.length], [false, 'ambiguous', 2]);
  ob = await outboxOf('dup-2');
  assert.deepEqual([ob.status, ob.result.ambiguous, ob.result.conflict_draft_id, ob.lease_owner], ['conflict', true, null, null]);
  assert.deepEqual(cardsOf('dup-2').map((x) => x.cdb_sku_id), [null, null]);
  // 3. 人が 1 枚に片付けて「もう一度」= ふつうの衝突に戻る → 結べる
  ph.prepare(`DELETE FROM product_drafts WHERE name = '後から増えたカード'`).run();
  const res2 = await runCards({ eventId: r2.card.event_id, manual: true });
  assert.equal(res2[0].status, 'conflict');
  assert.deepEqual([(await outboxOf('dup-2')).result.conflict_draft_id, (await outboxOf('dup-2')).result.ambiguous], [first.id, undefined]);
  const out3 = await asEditor(() => O.linkCardToExisting(db, link, { skuId: r2.sku_id, actor: 'naka@test', expectedDraftId: first.id }));
  assert.deepEqual([out3.ok, out3.draft_id], [true, first.id]);
  assert.equal((await outboxOf('dup-2')).status, 'done');
  // 4. SQLite で作った後に Postgres を done にできず (知らせは pending のまま)、その間に同じコードのカードが増えた = 前の答えを使い回さない (Codex R3 Medium 2)
  const r4 = await reg('single', 'dup-4', single({ name: '作った後に重なる単品' }));
  const ev4 = await one('select event_id::text as event_id, sku_id::text as sku_id, schema_version, payload from ops.product_hub_outbox where event_id = $1', [r4.card.event_id]);
  assert.equal(PH.applyCdbCardEvent(ev4).outcome, 'created');   // SQLite だけ済んだ
  const made = draftOf('dup-4');
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('Dup-4 ', '後から増えたカード 4', 'someone')`).run();
  let res4 = await runCards({ eventId: r4.card.event_id });
  assert.equal(res4[0].status, 'conflict');
  ob = await outboxOf('dup-4');
  assert.deepEqual([ob.status, ob.result.ambiguous, ob.result.conflict_draft_ids.length], ['conflict', true, 2]);
  assert.match(ob.last_error, /2 枚/);
  // 片付けて「もう一度」= 作ったカードがただ 1 枚 = 結んであった (done)
  ph.prepare(`DELETE FROM product_drafts WHERE name = '後から増えたカード 4'`).run();
  res4 = await runCards({ eventId: r4.card.event_id, manual: true });
  assert.deepEqual([res4[0].status, res4[0].result.outcome, res4[0].result.draft_id], ['done', 'linked', made.id]);
  // 5. もう結んである SKU (別の知らせ) でも、同じコードのカードが増えていれば「結んであった」にしない
  const r5 = await reg('single', 'dup-5', single({ name: '結んだ後に重なる単品' }));
  assert.equal((await runCards({ eventId: r5.card.event_id }))[0].status, 'done');
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('DUP-5', '後から増えたカード 5', 'someone')`).run();
  const ev5 = await one('select sku_id::text as sku_id, schema_version, payload from ops.product_hub_outbox where event_id = $1', [r5.card.event_id]);
  const again5 = PH.applyCdbCardEvent({ ...ev5, event_id: uuid() });
  assert.deepEqual([again5.outcome, again5.ambiguous, again5.conflict_draft_ids.length], ['conflict', true, 2]);
  // 済んだ知らせをもう一度直接渡しても、記録の答えを使い回さない
  const replay5 = PH.applyCdbCardEvent({ ...ev5, event_id: r5.card.event_id });
  assert.deepEqual([replay5.outcome, replay5.ambiguous], ['conflict', true]);
  ph.prepare(`DELETE FROM product_drafts WHERE name = '後から増えたカード 5'`).run();
  assert.equal(PH.applyCdbCardEvent({ ...ev5, event_id: uuid() }).outcome, 'linked');
  // dup-1 は「決められない」のまま画面の試験 [H5] へ (index は H5 の後で張り直す)
});

// ── 画面 (router) ──
console.log('\n画面 (master-edit)');
const MR = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://deploy@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test, other@test';
process.env.MASTER_EDIT_OPEN = '1';
/** 接続の URL のロールで動かす (画面の書き込み = master_edit・読むだけ = master_edit か持ち主)。閉じたら持ち主に戻す (⑤-1 の試験と同じ) */
const roleClient = async (url) => {
  const role = /master_edit@/.test(url) ? 'master_edit' : 'deploy';
  await pg.query(`set role ${role}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
};
MR.__setPgClientFactory(roleClient);
MR.__setClock(() => NOW.getTime());
MR.__setShippingRatesProvider(async () => RATES);   // 持ち主表 = DB の active (広げる道 PR-2)
let applierMode = 'real';
MR.__setCardApplier(async (ev) => { if (applierMode === 'boom') throw new Error('SQLite に書けない'); return PH.applyCdbCardEvent(ev); });
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'editor' ? { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'product-hub'] }
    : s === 'viewer' ? { authenticated: true, email: 'viewer@test', role: 'user', allowedApps: ['master-edit', 'product-hub'] } : null;
  next();
});
app.use('/apps/master-edit', MR.default);
const PHR = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', PHR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
/**
 * 画面の JS が文法として読めること・EJS の出力が JS に混ざっていないこと (属性つきの script も数える)。
 * マスタの入力の画面は JS を public/ のファイルに分けた (第 2 段 10/5) = <script src> は中身を HTTP で取ってきて同じに確かめ、返す (インラインの後ろに並べる)
 */
async function checkScripts(html, expected) {
  const opens = [...html.matchAll(/<script\b/gi)].length;
  const all = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)];
  for (const m of all.filter((x) => /type="application\/json"/.test(x[1]))) JSON.parse(m[2]);   // 画面の JS に渡す値
  const scripts = all.filter((m) => !/\bsrc=/.test(m[1]) && !/type="application\/json"/.test(m[1])).map((m) => m[2]);
  assert.equal(all.length, opens, 'script の開きと閉じの数が合わない');
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

await ta('[H1] 新商品の画面 (単品・セット): 描画・画面の JS・必須の印・product-hub と Yahoo! の欄・切替前の帯 (MASTER_EDIT_OPEN なし・持ち主 load)', async () => {
  let r = await call('GET', '/apps/master-edit/new?kind=single');
  assert.equal(r.status, 200, r.text.slice(0, 300));
  const sc = await checkScripts(r.text, 0);
  const newJs = sc.find((x) => x.includes('me-new.js — 新商品の登録の画面')) || '';
  for (const api of ["'/api/new'", "'/api/code-check?code='", "'/api/lookup?code='"]) assert.ok(newJs.includes(api), `画面が ${api} を呼んでいない`);
  for (const word of ['商品コード', '売価', '発送方法', '税率', 'Amazon URL', 'ASIN', '参考 URL', '公式ページ URL', 'セット商品を作るか', 'Yahoo!売価', 'Yahoo!売価 (佐川)', 'Yahoo!カテゴリID', 'Yahoo!path', '有効期限の管理', '入荷日の管理']) {
    assert.ok(r.text.includes(word), `単品の画面に「${word}」が無い`);
  }
  assert.match(r.text, /data-can-save="1"/); assert.ok(!/切替前です/.test(r.text));
  assert.match(r.text, /S03 謎の便 \/ 300 円/);
  r = await call('GET', '/apps/master-edit/new?kind=set');
  await checkScripts(r.text, 0);
  assert.match(r.text, /id="comp-rows"/); assert.ok(!r.text.includes('セット商品を作るか'), 'セットに「セット商品を作るか」を出さない');
  assert.ok(!r.text.includes('有効期限の管理'), 'セットにロジザードの欄を出さない');
  delete process.env.MASTER_EDIT_OPEN;
  r = await call('GET', '/apps/master-edit/new?kind=single');
  assert.match(r.text, /切替前です/); assert.match(r.text, /data-can-save="0"/); assert.match(r.text, /id="save" disabled/);
  process.env.MASTER_EDIT_OPEN = '1';
  // 広げる道 PR-2: 画面も DB の active を読む (配った config ではない)
  await W2.setActiveOwnershipInDb(pg, { ...ALL_COMPANY, sku_components: 'load' });
  assert.match((await call('GET', '/apps/master-edit/new?kind=set')).text, /切替前です/);   // セットは構成の持ち主も
  assert.ok(!/切替前です/.test((await call('GET', '/apps/master-edit/new?kind=single')).text), '単品は構成を書かない = 開いたまま');
  // DB の active が段階の記録と違う = 単品も閉じる (⑤-1 の門)
  await W2.setActiveMapOnly(pg, ALL_COMPANY);
  assert.match((await call('GET', '/apps/master-edit/new?kind=single')).text, /持ち主表が切替のときの記録と違う/);
  await W2.setActiveOwnershipInDb(pg, ALL_COMPANY);
  assert.ok(!/切替前です/.test((await call('GET', '/apps/master-edit/new?kind=single')).text));
});

await ta('[H2] 登録の API: 名簿・Origin・形の誤り 400・成功で product-hub のカードを同じ要求の中で作る・押し直しは前の結果', async () => {
  const body = { request_id: uuid(), kind: 'single', code: 'web-1', reason: '画面から', values: { name: '画面の単品', standard_price: '1500', shipping_code: 'S02', tax_rate: '0.08', expiry_managed: '0', inbound_date_managed: '1' },
    card: { create: true, official_url: 'https://maker.example/web1', reference_urls: ['https://ref.example/w'], set_decision: { decision: 'create', reason_text: '2 個セット' }, yahoo: { price: '', delivery_label: '' } } };
  assert.equal((await call('POST', '/apps/master-edit/api/new', { body, session: 'viewer' })).status, 403);
  assert.equal((await call('POST', '/apps/master-edit/api/new', { body, origin: false })).j.error, 'origin_mismatch');
  assert.equal((await call('POST', '/apps/master-edit/api/new', { body: { ...body, request_id: uuid(), code: 'Web-1' } })).status, 400);
  const ok = await call('POST', '/apps/master-edit/api/new', { body });
  assert.equal(ok.status, 200, ok.text);
  assert.deepEqual([ok.j.state, ok.j.card.status, ok.j.card_label], ['draft', 'done', 'カード作成済み']);
  const d = draftOf('web-1');
  assert.equal(d.id, ok.j.card.draft_id);
  assert.deepEqual(ph.prepare('SELECT decision, reason_text FROM draft_set_decisions WHERE draft_id = ?').all(d.id), [{ decision: 'hold', reason_text: '作る予定 (新商品の登録で「作る」): 2 個セット' }]);
  assert.equal(ph.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(d.id).shipping_method_group, '7');
  assert.equal(ph.prepare('SELECT COUNT(*) AS c FROM draft_yahoo WHERE draft_id = ?').get(d.id).c, 0);   // Yahoo! の欄が空 = 行を作らない
  const again = await call('POST', '/apps/master-edit/api/new', { body });
  assert.equal(again.j.replayed, true);
  assert.equal(ph.prepare("SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'web-1'").get().c, 1);
});

await ta('[H3] カードが作れなかった登録: 登録は成功・「カード作成待ち」・商品の画面の箱ともう一度 (名簿の人だけ)・コードの確かめ', async () => {
  applierMode = 'boom';
  const body = { request_id: uuid(), kind: 'single', code: 'web-2', values: { name: '画面の単品 2', standard_price: '1500', shipping_code: 'S01', tax_rate: '10' }, card: {} };
  const r = await call('POST', '/apps/master-edit/api/new', { body });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.card.status, r.j.card_label, r.j.card.error], ['failed', 'カード作成待ち (失敗)', 'SQLite に書けない']);
  let page = await call('GET', '/apps/master-edit/sku/web-2');
  assert.equal(page.status, 200);
  await checkScripts(page.text, 0);
  assert.match(page.text, /カード作成待ち \(失敗\)/); assert.match(page.text, /id="card-retry"/); assert.match(page.text, /<span class="b warn">下書き<\/span>/);
  assert.equal((await call('POST', '/apps/master-edit/api/sku/web-2/card-retry', { body: {}, session: 'viewer' })).status, 403);
  applierMode = 'real';
  const retry = await call('POST', '/apps/master-edit/api/sku/web-2/card-retry', { body: {} });
  assert.deepEqual([retry.status, retry.j.ok, retry.j.status], [200, true, 'done']);
  page = await call('GET', '/apps/master-edit/sku/web-2');
  assert.match(page.text, /カード作成済み/); assert.ok(!/id="card-retry"/.test(page.text));
  assert.equal((await call('POST', '/apps/master-edit/api/sku/web-2/card-retry', { body: {} })).status, 409);
  const cc = await call('GET', '/apps/master-edit/api/code-check?code=web-2');
  assert.deepEqual([cc.j.ok, cc.j.reason], [false, 'code_taken']);
  assert.equal((await call('GET', '/apps/master-edit/api/code-check?code=web-3')).j.ok, true);
});

await ta('[H4] 一覧: 登録の列・絞り込み (下書き・要確認・状態なし)・新商品への入口・つかいかた', async () => {
  let r = await call('GET', '/apps/master-edit/?reg=draft');
  assert.equal(r.status, 200);
  await checkScripts(r.text, 0);
  assert.ok(r.text.includes('sku/new-a1') && r.text.includes('sku/web-2') && !r.text.includes('sku/s001"'));
  assert.match(r.text, /href="new\?kind=single"/);
  r = await call('GET', '/apps/master-edit/?reg=quarantined');
  assert.ok(r.text.includes('sku/s900'));
  const R2 = await import('../apps/master-edit/read.mjs');
  assert.deepEqual((await R2.listSkus(db, { reg: 'quarantined' }, { now: NOW })).rows.map((x) => x.code), ['ghost-1', 'race-1', 's900']);
  assert.deepEqual((await R2.listSkus(db, { reg: 'ne_confirmed' }, { now: NOW })).rows.map((x) => x.code), []);
  assert.deepEqual((await R2.listSkus(db, { reg: 'none' }, { now: NOW })).rows.map((x) => x.code), []);
  // 登録の状態 × カード (Codex R2 Low): 画面の URL でも両方効く
  r = await call('GET', '/apps/master-edit/?reg=available&card=waiting');
  assert.equal(r.status, 200);
  assert.ok(r.text.includes('sku/s002"') && !r.text.includes('sku/web-2"'), '利用可 × カード作成待ち');
  const m = await call('GET', '/apps/master-edit/manual');
  for (const word of ['新商品を登録する', '大文字は使えません', '発送方法 要確認', 'カード作成待ち', '下書き']) assert.ok(m.text.includes(word), `つかいかたに「${word}」が無い`);
});

await ta('[H5] 衝突の画面: 「既存のカードをこの商品に結ぶ」が出る・名簿の人だけ・押すと done・もう一度押しても同じ', async () => {
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('web-9', '前からのカード', 'someone')`).run();
  const old = draftOf('web-9');
  const body = { request_id: uuid(), kind: 'single', code: 'web-9', values: { name: '衝突する単品', standard_price: '1500', shipping_code: 'S01', tax_rate: '10' }, card: {} };
  const r = await call('POST', '/apps/master-edit/api/new', { body });
  assert.deepEqual([r.status, r.j.card.status], [200, 'conflict']);
  let page = await call('GET', '/apps/master-edit/sku/web-9');
  await checkScripts(page.text, 0);
  assert.match(page.text, /id="card-link" data-draft="\d+"/); assert.match(page.text, /カードの衝突/);
  assert.equal((await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: { draft_id: String(old.id) }, session: 'viewer' })).status, 403);
  assert.equal((await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: {} })).status, 400);
  const mm = await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: { draft_id: '999999' } });
  assert.deepEqual([mm.status, mm.j.reason, mm.j.draft_id], [409, 'draft_mismatch', old.id]);
  const ok = await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: { draft_id: String(old.id) } });
  assert.deepEqual([ok.status, ok.j.ok, ok.j.draft_id], [200, true, old.id]);
  page = await call('GET', '/apps/master-edit/sku/web-9');
  assert.match(page.text, /カード作成済み/); assert.ok(!/id="card-link"/.test(page.text));
  const again = await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: { draft_id: String(old.id) } });
  assert.deepEqual([again.status, again.j.already], [200, true]);
  assert.equal((await call('POST', '/apps/master-edit/api/sku/nope/card-link', { body: { draft_id: '1' } })).status, 404);
  // 同じコードのカードが 2 枚以上 ([O10] の dup-1) = 「結ぶ」を出さない・押しても 409 ambiguous (何も結ばない)
  page = await call('GET', '/apps/master-edit/sku/dup-1');
  assert.equal(page.status, 200);
  await checkScripts(page.text, 0);
  assert.ok(!/id="card-link"/.test(page.text)); assert.match(page.text, /2 枚以上あります/);
  const dupIds = ph.prepare(`SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'dup-1' ORDER BY id`).all().map((x) => x.id);
  for (const d of dupIds) assert.ok(page.text.includes(`/apps/product-hub/detail/${d}"`), `#${d} へのリンク`);
  const amb = await call('POST', '/apps/master-edit/api/sku/dup-1/card-link', { body: { draft_id: String(dupIds[0]) } });
  assert.deepEqual([amb.status, amb.j.reason, amb.j.draft_ids], [409, 'ambiguous', dupIds]);
  assert.match(amb.j.error, /2 枚/);
  assert.equal((await outboxOf('dup-1')).status, 'conflict');
  // 片付け: 重なりを消して一意の index を張り直す (ほかの試験は index のある DB)
  ph.prepare('DELETE FROM product_drafts WHERE id = ?').run(dupIds[1]);
  ph.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_product_drafts_ne_norm ON product_drafts(LOWER(TRIM(ne_code)))');
});

console.log('\nproduct-hub (新規作成の入口・ボード)');

let phCalls = 0;
let phMode = 'pglite';
// 持ち主表 = DB の active (広げる道 PR-2・本物の PGlite の接続で読む)
// ⑤-3b: 古い新商品の作り方の門 (lib/master-legacy-gate.mjs・持ち主は Company DB の epoch) = 既定は「新商品の登録の列が全部 C = 閉じている」
let oldCreation = { readable: true, writable: false };
O.__setNewEntryLegacyGate(async () => oldCreation);
O.__setCompanyDbClientFactory(async (url) => {
  phCalls++;
  if (phMode === 'down') throw new Error('connect ECONNREFUSED');
  if (phMode === 'pglite') return roleClient(url);
  // 段階だけを返す偽の接続 (legacy_open / company_owner を試す)
  return { query: async (t) => (/master_cutover_state/.test(t) ? { rows: [{ phase: phMode, changed_at: '2030-01-01', changed_by: 'x' }] } : { rows: [] }), end: async () => {}, on: () => {} };
});

await ta('[P1] 切替の前 (MASTER_EDIT_OPEN なし) = /new は今までの画面のまま・Company DB につながない・ボードも取り込まない', async () => {
  delete process.env.MASTER_EDIT_OPEN;
  const n0 = phCalls;
  const r = await call('GET', '/apps/product-hub/new');
  assert.equal(r.status, 200);
  assert.match(r.text, /新規商品ドラフト/); assert.match(r.text, /id="create-btn"/); assert.ok(!r.text.includes('/apps/master-edit/new'));
  const b = await call('GET', '/apps/product-hub/board');
  assert.equal(b.status, 200, b.text.slice(0, 300));
  assert.equal(phCalls, n0, 'Company DB につないだ');
});

await ta('[P2] MASTER_EDIT_OPEN = 1: 段階 new_open = 案内だけ (新商品の登録へ)・legacy_open = 今までどおり・company_owner / 読めない = 止めている', async () => {
  process.env.MASTER_EDIT_OPEN = '1';
  phMode = 'pglite';
  let r = await call('GET', '/apps/product-hub/new');
  assert.equal(r.status, 200);
  assert.match(r.text, /\/apps\/master-edit\/new\?kind=single/); assert.match(r.text, /\/apps\/master-edit\/new\?kind=set/); assert.ok(!/id="create-btn"/.test(r.text));
  await checkScripts(r.text);
  phMode = 'legacy_open';
  r = await call('GET', '/apps/product-hub/new');
  assert.match(r.text, /id="create-btn"/);
  phMode = 'company_owner';
  r = await call('GET', '/apps/product-hub/new');
  assert.match(r.text, /いまは新商品の登録を止めています/); assert.match(r.text, /company_owner/); assert.ok(!/id="create-btn"/.test(r.text));
  phMode = 'down';
  r = await call('GET', '/apps/product-hub/new');
  assert.match(r.text, /いまは新商品の登録を止めています/); assert.ok(!/id="create-btn"/.test(r.text));
  phMode = 'pglite';
  // ⑤-3b: new_open でも、新商品の登録の列にまだ load がある (古い作り方の門が開いている・10/5 の 13 キー) = 今までの画面
  oldCreation = { readable: true, writable: true };
  r = await call('GET', '/apps/product-hub/new');
  assert.match(r.text, /id="create-btn"/); assert.ok(!r.text.includes('/apps/master-edit/new?kind=single'));
  // 古い作り方の門を読めない (持ち主を読めない) = 止めている
  oldCreation = { readable: false, writable: false, error: 'x' };
  r = await call('GET', '/apps/product-hub/new');
  assert.match(r.text, /いまは新商品の登録を止めています/);
  oldCreation = { readable: true, writable: false };
});

await ta('[P3] ボードを開く = 取り込み待ちの知らせからカードを作る (MASTER_EDIT_OPEN = 1)・要確認の札・Company DB に届かなくてもボードは出る', async () => {
  process.env.MASTER_EDIT_OPEN = '1';
  const r = await reg('single', 'board-1', single({ name: 'ボードで作る単品', shipping_code: 'S03' }));
  assert.equal((await outboxOf('board-1')).status, 'pending');
  const b = await call('GET', '/apps/product-hub/board');
  assert.equal(b.status, 200, b.text.slice(0, 300));
  assert.equal((await outboxOf('board-1')).status, 'done');
  const d = draftOf('board-1');
  assert.equal(String(d.cdb_sku_id), r.sku_id);
  const b2 = await call('GET', '/apps/product-hub/board');
  assert.match(b2.text, /⚠ 発送方法 要確認/);
  await checkScripts(b2.text);
  phMode = 'down';
  assert.equal((await call('GET', '/apps/product-hub/board')).status, 200);
  phMode = 'pglite';
});

await ta('[X1] 別の DB: 段階の門 (trigger) を止めて new_open にしても、backfill が無ければ画面 D は登録しない (409 backfill_missing・何も書かない)', async () => {
  const pg2 = new PGlite();
  try {
    const db2 = pgliteAdapter(pg2);
    await applyMigrations(db2, { log: quiet });
    await W2.useReal0058(pg2, { leases: ['single', 'set'], futureSetLease: true });   // 広げる道 PR-2: 本物の 0058 の上で試験の許可を置く (backfill の確かめまで進める)
    await pg2.query('alter table ops.master_cutover_state disable trigger trg_master_cutover_state_prereq');
    await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(db2, ALL_COMPANY);   // 0055 (④a): 段階の持ち主表 = 持ち主の epoch (本番 = ④a の activate)
    await pg2.query('begin');
    await pg2.query(`select set_config('ops.cutover_protocol', '1', true)`);
    await pg2.query(`update ops.master_cutover_state set phase = 'new_open', owner_hash = $1`, [C.ownershipHash(ALL_COMPANY)]);
    await pg2.query('commit');
    const e = await rejectsWith(R.registerNewSku(db2, { actor: 'naka@test', requestId: uuid(), kind: 'single', code: 'x-1', values: single(), card: {} },
      { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES }), 409, 'backfill_missing');
    assert.equal(e.extra.phase, 'new_open');
    // 登録の関数を直接呼んでも同じ (DB でも backfill を確かめる)
    await assert.rejects(() => pg2.query('select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb)',
      [uuid(), 'naka@test', null, JSON.stringify(ALL_COMPANY), 'e'.repeat(64), JSON.stringify(regEntry('x-2'))]), (er) => /^backfill_missing/.test(er.message));
    assert.equal((await pg2.query('select count(*)::int as n from core.skus')).rows[0].n, 0);
  } finally { await pg2.close(); }
});

await ta('[X2] 復元 (夜間のバックアップ): backfill の後の DB は状態・印・知らせごと戻り、SKU を足す取引の確かめも効く / backfill の前の DB は復元の後に backfill を流せば new_open の前提がそろう', async () => {
  const { dumpCompanyDb, restoreCompanyDb } = await import('../apps/company-db/backup/dump.mjs');
  const count = async (d) => (await d.query(`select (select count(*) from ops.master_registrations)::int as r, (select count(*) from ops.master_registration_backfill)::int as b,
      (select count(*) from ops.product_hub_outbox)::int as o, (select count(*) from core.skus)::int as s`)).rows[0];
  // 1. 今の試験の DB (backfill の後・new_open) を写して戻す
  const lines = [];
  await dumpCompanyDb(db, (l) => lines.push(l), { log: quiet });
  const pg3 = new PGlite();
  try {
    const db3 = pgliteAdapter(pg3);
    await applyMigrations(db3, { log: quiet });
    await restoreCompanyDb(db3, lines.join('\n'), { log: quiet });
    assert.deepEqual(await count(db3), await count(db));
    assert.deepEqual((await db3.query('select ops.master_cutover_prereq_problems($1, $2) as p', ['company_owner', 'new_open'])).rows[0].p, []);
    await pg3.query('begin');
    await pg3.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'after-restore', 'x')`);
    await assert.rejects(() => pg3.query('commit'), /unregistered_sku/);   // 復元は trigger を止めて入れる・戻した後は効く
  } finally { await pg3.close(); }
  // 2. backfill の前の DB (SKU だけ) を写して戻す → 状態の行は無い → 切替の手順どおり backfill → new_open の前提がそろう
  const pg4 = new PGlite(); const pg5 = new PGlite();
  try {
    const db4 = pgliteAdapter(pg4); const db5 = pgliteAdapter(pg5);
    await applyMigrations(db4, { log: quiet });
    assert.equal((await runInitialLoad(db4, makePlan(), { log: quiet, runId: 'load_restore', ownership: MASTER_OWNERSHIP, now: LOAD_NOW })).ok, true);
    const l4 = []; await dumpCompanyDb(db4, (l) => l4.push(l), { log: quiet });
    await applyMigrations(db5, { log: quiet });
    await restoreCompanyDb(db5, l4.join('\n'), { log: quiet });
    const c5 = await count(db5);
    assert.deepEqual([c5.r, c5.b], [0, 0]);
    assert.match((await db5.query('select ops.master_cutover_prereq_problems($1, $2) as p', ['company_owner', 'new_open'])).rows[0].p.join(' '), /backfill_missing.*unregistered_skus/);
    await pg5.query('begin');
    await pg5.query(`select set_config('ops.cutover_protocol', '1', true)`);
    await pg5.query(`update ops.master_cutover_state set phase = 'frozen'`);   // 試験だけ (段階は門の関数で進めるのが本番)
    await pg5.query('commit');
    const p5 = (await db5.query('select * from ops.registration_backfill_plan()')).rows[0];
    assert.equal(p5.sku_count, c5.s);
    await db5.query('select ops.backfill_sku_registrations($1, $2, $3)', [p5.sku_count, p5.snapshot_hash, 'naka@test']);
    await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(db5, ALL_COMPANY);   // 0055 (④a): 切替の手順の持ち主の epoch (本番 = ④a の activate)
    assert.deepEqual((await db5.query('select ops.master_cutover_prereq_problems($1, $2) as p', ['company_owner', 'new_open'])).rows[0].p, []);
  } finally { await pg4.close(); await pg5.close(); }
});

console.log('\n広げる道 PR-2 (DB の active に従う・二層の門)');
const { COMPANY_CAPABLE } = await import('../config/master-capability.mjs');
const { MASTER_OWNERSHIP: CONFIGURED } = await import('../config/master-ownership.mjs');
/** 本番の active (10/5 の 13 キー・skus.sku_kind は load)。WIDENED = widen の後 (sku_kind も company) = 10/7 からの configured (広げる道の手順の 1) */
const PROD13 = Object.freeze({ ...CONFIGURED, 'skus.sku_kind': 'load' });   // 🚨 COMPANY_CAPABLE から作らない (#1641 で skus.sku_kind が入った = 14 キー・Codex #1640 R6 Medium)
/** 旧 build (widen の前の能力 = 13 キー) = 今の COMPANY_CAPABLE から skus.sku_kind と listing_components.amazon (⑦-2 PR-A で入った) を明示して外す */
const OLD_CAPABLE = Object.freeze(COMPANY_CAPABLE.filter((k) => k !== 'skus.sku_kind' && k !== 'listing_components.amazon'));
const WIDENED = Object.freeze({ ...PROD13, 'skus.sku_kind': 'company' });
const snap = async () => one(`select (select count(*) from core.skus)::int as s, (select count(*) from ops.master_registrations)::int as r, (select count(*) from events.master_change_events)::int as e,
  (select count(*) from ops.product_hub_outbox)::int as o`);
/** DB を本番の形にして fn を流し、ALL_COMPANY・全部の能力・許可ありに戻す */
async function withDbOwner(map, { capable = null, leases = [] } = {}, fn) {
  await W2.setActiveOwnershipInDb(pg, map);
  OG.__setCapableForTest(capable);
  for (const k of ['single', 'set']) await W2.setTestLease(pg, k, leases.includes(k));
  try { return await fn(); } finally {
    await W2.setActiveOwnershipInDb(pg, ALL_COMPANY);
    OG.__setCapableForTest(Object.keys(MASTER_OWNERSHIP));
    for (const k of ['single', 'set']) await W2.setTestLease(pg, k, true, { futureSetLease: true });
  }
}
const saveName = async (code, name) => {
  const id = await skuId(code);
  const token = W.editTokenOf(await W.readCurrent(db, id, TODAY));
  return asEditor(() => W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code, reason: null, seen: { token }, values: { name } }, { open: true, now: NOW }));
};

await ta('[W1] 今の本番と同じ DB (active = 13 キー・許可なし。configured は区分も company = 配ってから widen までの間) = 今の動きのまま: 13 キーの保存は通る・新商品 (単品・セット) は 409 切替前 (区分が load)・画面も閉じる・product-hub は今までの画面', async () => {
  assert.deepEqual(Object.keys(PROD13).filter((k) => PROD13[k] === 'company').sort(), [...OLD_CAPABLE].sort());
  assert.equal(Object.values(PROD13).filter((v) => v === 'company').length, 13, '本番の active = 13 キー');
  assert.equal(PROD13['skus.sku_kind'], 'load');
  assert.notEqual(C.ownershipHash(WIDENED), C.ownershipHash(PROD13), 'widen の前後で持ち主表が違う');
  assert.equal(C.ownershipHash(WIDENED), C.ownershipHash(CONFIGURED), 'configured = widen の後の持ち主表 (10/7・広げる道の手順の 1)');
  assert.notEqual(C.ownershipHash(PROD13), C.ownershipHash(CONFIGURED), '配ってから widen までの間 = DB の active と configured は違う (画面は DB の active に従う)');
  await withDbOwner(PROD13, {}, async () => {
    const r = await saveName('s001', '13 キーの保存');
    assert.equal(r.ok, true);
    assert.deepEqual(r.changed.map((c) => c.field), ['name']);
    const b = await snap();
    let e = await rejectsWith(reg('single', 'w1-single', single()), 409, 'before_cutover');
    assert.deepEqual(e.extra.load_keys, ['skus.sku_kind']);
    e = await rejectsWith(reg('set', 'w1-set', { name: 'x', standard_price: '1', shipping_code: 'S01', components: [{ code: 's001', qty: 1 }] }), 409, 'before_cutover');
    assert.deepEqual(e.extra.load_keys.sort(), ['sku_components', 'skus.sku_kind']);
    // 許可があっても区分が load = 閉じたまま (許可は 2 層目 = 区分の門を開けない)
    await W2.setTestLease(pg, 'single', true);
    e = await rejectsWith(reg('single', 'w1-single', single()), 409, 'before_cutover');
    assert.deepEqual(e.extra.load_keys, ['skus.sku_kind']);
    assert.deepEqual(await snap(), b, '何も書かない');
    for (const kind of ['single', 'set']) {
      const p = await call('GET', `/apps/master-edit/new?kind=${kind}`);
      assert.match(p.text, /data-can-save="0"/); assert.match(p.text, /書く項目の持ち主がまだ NE/);
    }
    oldCreation = { readable: true, writable: true };   // 古い作り方の門 = 区分が load なので開いている (lib/master-legacy-gate.mjs)
    process.env.MASTER_EDIT_OPEN = '1';
    assert.equal((await O.newEntryGate()).mode, 'legacy');
  });
  oldCreation = { readable: true, writable: false };
});

await ta('[W2] DB の active に skus.sku_kind が入る (widen の後 = active と configured が同じ) = DB に従って単品だけ開く・許可 / 非常の止め / 関数なし / 読めない / code_behind・セットは閉じたまま', async () => {
  assert.equal(CONFIGURED['skus.sku_kind'], 'company', 'configured (配った config) も company = widen の後は active と同じ (画面は config を見ていない = W1)');
  const cap = [...COMPANY_CAPABLE].sort();   // 今のコードの能力 (#1641 で skus.sku_kind を含む)
  assert.ok(cap.includes('skus.sku_kind'));
  await withDbOwner(WIDENED, { capable: cap }, async () => {
    // 許可なし = 409 new_entry_closed (何も書かない)・画面も閉じる (許可が無いと書く)
    const b = await snap();
    let e = await rejectsWith(reg('single', 'w2-a', single()), 409, 'new_entry_closed');
    assert.equal(e.extra.cause, 'no_lease');
    assert.deepEqual(await snap(), b);
    let p = await call('GET', '/apps/master-edit/new?kind=single');
    assert.match(p.text, /data-can-save="0"/); assert.match(p.text, /新商品の開放の許可がありません/);
    // 許可あり = 開く (DB の active に従う)
    await W2.setTestLease(pg, 'single', true);
    p = await call('GET', '/apps/master-edit/new?kind=single');
    assert.match(p.text, /data-can-save="1"/);
    const ok = await reg('single', 'w2-a', single({ name: 'W2 単品' }));
    assert.equal(ok.ok, true, JSON.stringify(ok));
    assert.equal((await one("select sku_kind from core.skus where code = 'w2-a'")).sku_kind, 'single');
    // セットは構成 (sku_components) が load = 閉じたまま (許可があっても)
    await W2.setTestLease(pg, 'set', true, { futureSetLease: true });   // 将来の形 (本物の 0058 はまだセットの許可を出さない)
    e = await rejectsWith(reg('set', 'w2-set', { name: 'x', standard_price: '1', shipping_code: 'S01', components: [{ code: 's001', qty: 1 }] }), 409, 'before_cutover');
    assert.deepEqual(e.extra.load_keys, ['sku_components']);
    // 非常の止め (env MASTER_NEW_ENTRY_STOP=1) = 許可があっても閉じる・画面も
    process.env.MASTER_NEW_ENTRY_STOP = '1';
    try {
      e = await rejectsWith(reg('single', 'w2-b', single()), 409, 'new_entry_closed');
      assert.equal(e.extra.cause, 'stopped');
      p = await call('GET', '/apps/master-edit/new?kind=single');
      assert.match(p.text, /data-can-save="0"/); assert.match(p.text, /非常の止め/);
    } finally { delete process.env.MASTER_NEW_ENTRY_STOP; }
    // 許可を読めない (実行権が無い) = 503・関数が無い (0058 の前の DB) = 409 no_function
    await pg.query('revoke execute on function ops.acquire_new_entry_locks(text) from master_edit');   // 新規開始の鍵の関数を呼べない (段階の鍵より前) = 503
    e = await rejectsWith(reg('single', 'w2-b', single()), 503, 'new_entry_lease_unreadable');
    await pg.query('grant execute on function ops.acquire_new_entry_locks(text) to master_edit');
    await W2.hideLeaseFunctions(pg);
    try {
      e = await rejectsWith(reg('single', 'w2-b', single()), 409, 'new_entry_closed');
      assert.equal(e.extra.cause, 'no_function');
      p = await call('GET', '/apps/master-edit/new?kind=single');
      assert.match(p.text, /data-can-save="0"/); assert.match(p.text, /0058 の前/);
    } finally { await W2.restoreLeaseFunctions(pg); }
    // 既にある商品の保存 (13 キー) は許可・止めと関係なく通る (新規開始の門は新商品だけ)
    await W2.setTestLease(pg, 'single', false);
    process.env.MASTER_NEW_ENTRY_STOP = '1';
    try { assert.equal((await saveName('s001', '止めていても保存')).ok, true); } finally { delete process.env.MASTER_NEW_ENTRY_STOP; }
    // このコードの能力に sku_kind が無い (古い build) = 409 code_behind (保存も新商品も)
    await W2.setTestLease(pg, 'single', true);
    OG.__setCapableForTest(OLD_CAPABLE);   // 旧 build (sku_kind を扱えない)
    e = await rejectsWith(reg('single', 'w2-c', single()), 409, 'code_behind');
    assert.deepEqual(e.extra.code_behind, ['skus.sku_kind']);
    e = await rejectsWith(saveName('s001', '古い build'), 409, 'code_behind');
    p = await call('GET', '/apps/master-edit/new?kind=single');
    assert.match(p.text, /data-can-save="0"/); assert.match(p.text, /プログラムが古い/);
    assert.equal(await skuId('w2-c'), undefined);
  });
});

await ta('[W4] DB の関数が新規開始を拒んだ (PR-1 の 0058: P0001「new_entry_closed: …」) = 409 の分かる文・何も書かない', async () => {
  const b = await snap();
  const closingDb = { query: (t, p) => (/ops\.register_new_sku\(/.test(t)
    ? Promise.reject(Object.assign(new Error('new_entry_closed: 有効な許可が無い (single)'), { code: 'P0001' })) : db.query(t, p)) };
  const e = await rejectsWith(asEditor(() => R.registerNewSku(closingDb, { actor: 'naka@test', requestId: uuid(), kind: 'single', code: 'w4-a', values: single(), card: {} },
    { open: true, now: NOW, shippingRates: RATES })), 409, 'new_entry_closed');
  assert.match(e.message, /新商品の登録はまだ開いていません \(Company DB が断った: .*有効な許可が無い/);
  assert.equal(e.extra.cause, 'db');
  assert.equal(await skuId('w4-a'), undefined);
  const after = await snap();
  assert.deepEqual([after.s, after.r, after.o], [b.s, b.r, b.o]);
  // 本物の DB の関数の中で拒む形 (書き込みの約束を拒む trigger = 0058 の強制と同じ誤り) でも同じ
  await W2.enforceLeaseInDb(pg);
  try {
    const e2 = await rejectsWith(reg('single', 'w4-b', single()), 409, 'new_entry_closed');
    assert.equal(e2.extra.cause, 'db');
    assert.equal(await skuId('w4-b'), undefined);
  } finally { await W2.enforceLeaseInDb(pg, { closed: false }); }
  // 形の違う誤り (先頭が new_entry_closed でない・P0001 でない) は文にしない
  assert.equal(W.newEntryClosedFromDb(Object.assign(new Error('before_cutover: x'), { code: 'P0001' })), null);
  assert.equal(W.newEntryClosedFromDb(Object.assign(new Error('new_entry_closed: x'), { code: '42501' })), null);
});

await ta('[W5] readiness (Codex #1640 R1 Medium 3): 画面のロール・見張りで許可の関数を実際に呼ぶ (false は正常)・watcher の widen の確かめ・new_entry_gate (LOGIN・NOINHERIT)・Render で画面の接続先が無い / miniPC で見張りが無い = 足りない', async () => {
  const { checkReadiness } = await import('./company-db/master-legacy-readiness.mjs');
  // 本物のログインと同じ = session_user がそのロール (readiness は session_user を見る)。持ち主の URL = 試験の接続のログイン
  const open = async (url) => {
    const role = /master_edit@/.test(url) ? 'master_edit' : /watcher@/.test(url) ? 'watcher' : null;
    if (role) await pg.query(`set session authorization ${role}`);
    return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query(`set session authorization ${sessionUser}`); await pg.query('set role deploy'); } };
  };
  const env = { COMPANY_DB_MASTER_EDIT_URL: 'postgres://master_edit@x/db', COMPANY_DB_WATCH_URL: 'postgres://watcher@x/db', RENDER_GIT_COMMIT: 'a'.repeat(40) };
  const has = (r, re) => r.lines.some((l) => re.test(l));
  const prob = (r, re) => r.problems.some((l) => re.test(l));
  await W2.setTestLease(pg, 'single', false);   // 許可なし (false) = 正常
  try {
    let r = await checkReadiness({ host: 'render', env, open });
    assert.ok(has(r, /✓ 画面のロールで持ち主 \(DB の active\) を読める/), r.lines.join('\n'));
    assert.ok(has(r, /✓ 画面のロールで新商品の開放の許可を呼べる \(今 single = 許可なし\)/), r.lines.join('\n'));
    assert.ok(has(r, /✓ 見張り \(watcher\) で新商品の開放の許可を呼べる/), r.lines.join('\n'));
    assert.ok(has(r, /✓ 見張り \(watcher\) が ops\.widen_check_readonly/), r.lines.join('\n'));
    assert.ok(has(r, /✓ 許可を出す専用のログイン new_entry_gate がある/), r.lines.join('\n'));
    assert.ok(!prob(r, /画面のロール|見張り \(watcher\)|見張りの接続先|new_entry_gate|widen_check/), r.problems.join('\n'));
    assert.ok(has(r, /✓ watch_writer が ops\.close_new_entry_for_compare/) && has(r, /✓ watch_writer が ops\.record_new_entry_gate/)
      && has(r, /✓ new_entry_gate が ops\.grant_new_entry_lease/) && has(r, /✓ new_entry_gate が ops\.revoke_new_entry_lease/), r.lines.join('\n'));
    await pg.query('revoke execute on function ops.grant_new_entry_lease(text, text) from new_entry_gate');
    try {
      const r3 = await checkReadiness({ host: 'render', env, open });
      assert.ok(prob(r3, /new_entry_gate が ops\.grant_new_entry_lease\(text,text\) を実行できない/), r3.problems.join('\n'));
    } finally { await pg.query('grant execute on function ops.grant_new_entry_lease(text, text) to new_entry_gate'); }
    // 逆向き: watch_writer が許可を出せる = 権限が分かれていない = 足りない
    await pg.query('grant execute on function ops.grant_new_entry_lease(text, text) to watch_writer');
    try {
      const r4 = await checkReadiness({ host: 'render', env, open });
      assert.ok(prob(r4, /watch_writer が ops\.grant_new_entry_lease\(text,text\) を実行できてしまう/), r4.problems.join('\n'));
    } finally { await pg.query('revoke execute on function ops.grant_new_entry_lease(text, text) from watch_writer'); }
    assert.ok(has(r, /✓ 画面のロールが ops\.acquire_new_entry_locks\(text\) を実行できる/) && has(r, /✓ 画面のロールが ops\.ne_reg_file\(bigint\) を実行できる/)
      && has(r, /✓ 画面のロールは file_bytes の列を直接読めない/), r.lines.join('\n'));
    // 画面のロールが file_bytes を直接読める (0058 の列の外しが無い) = 足りない (Codex #1640 R3 Medium 2)
    await W2.hideRegFileBytes(pg, false);
    try {
      r = await checkReadiness({ host: 'render', env, open });
      assert.ok(prob(r, /file_bytes を直接読める/), r.problems.join('\n'));
    } finally { await W2.hideRegFileBytes(pg, true); }
    // watcher の実行権が無い = 呼んで落ちる = 足りない (has_function_privilege だけで緑にしない)
    await pg.query('revoke execute on function ops.new_entry_lease_valid(text) from watcher');
    await pg.query('revoke execute on function ops.widen_check_readonly(uuid, integer) from watcher');
    r = await checkReadiness({ host: 'render', env, open });
    assert.ok(prob(r, /見張り \(watcher\) で ops\.new_entry_lease_valid\(text\) を呼べない/), r.problems.join('\n'));
    assert.ok(prob(r, /見張り \(watcher\) が ops\.widen_check_readonly\(uuid,integer\) を実行できない/), r.problems.join('\n'));
    await pg.query('grant execute on function ops.new_entry_lease_valid(text) to watcher');
    await pg.query('grant execute on function ops.widen_check_readonly(uuid, integer) to watcher');
    // new_entry_gate が INHERIT = 足りない
    await pg.query('alter role new_entry_gate inherit');
    r = await checkReadiness({ host: 'render', env, open });
    assert.ok(prob(r, /new_entry_gate の形が違う/), r.problems.join('\n'));
    await pg.query('alter role new_entry_gate noinherit');
    // Render で画面の接続先が無い = 足りない / miniPC = 注意だけ。miniPC で見張りが無い = 足りない / Render = 注意だけ
    r = await checkReadiness({ host: 'render', env: { ...env, COMPANY_DB_MASTER_EDIT_URL: '' }, open });
    assert.ok(prob(r, /COMPANY_DB_MASTER_EDIT_URL/), r.problems.join('\n'));
    r = await checkReadiness({ host: 'minipc', env: { ...env, COMPANY_DB_MASTER_EDIT_URL: '' }, open });
    assert.ok(!prob(r, /COMPANY_DB_MASTER_EDIT_URL = master_edit/) && has(r, /⚠ 画面のロールの接続先/), r.lines.join('\n'));
    r = await checkReadiness({ host: 'minipc', env: { ...env, COMPANY_DB_WATCH_URL: '' }, open });
    assert.ok(prob(r, /見張りの接続先 \(COMPANY_DB_WATCH_URL = watcher\) が無い/), r.problems.join('\n'));
    r = await checkReadiness({ host: 'render', env: { ...env, COMPANY_DB_WATCH_URL: '' }, open });
    assert.ok(!prob(r, /見張りの接続先/), r.problems.join('\n'));
    // 接続先のログインの取り違え (Codex #1640 R2 Medium 4): 持ち主の URL・入れ替えた URL = 足りない (持ち主なら何でも呼べてしまう)
    r = await checkReadiness({ host: 'render', env: { ...env, COMPANY_DB_MASTER_EDIT_URL: 'postgres://owner@x/db', COMPANY_DB_WATCH_URL: 'postgres://owner@x/db' }, open });
    assert.ok(prob(r, /COMPANY_DB_MASTER_EDIT_URL のログインが .* \(期待 master_edit\)/), r.problems.join('\n'));
    assert.ok(prob(r, /COMPANY_DB_WATCH_URL のログインが .* \(期待 watcher\)/), r.problems.join('\n'));
    r = await checkReadiness({ host: 'render', env: { ...env, COMPANY_DB_MASTER_EDIT_URL: env.COMPANY_DB_WATCH_URL, COMPANY_DB_WATCH_URL: env.COMPANY_DB_MASTER_EDIT_URL }, open });
    assert.ok(prob(r, /ログインが watcher \(期待 master_edit\)/) && prob(r, /ログインが master_edit \(期待 watcher\)/), r.problems.join('\n'));
  } finally { await W2.setTestLease(pg, 'single', true); await pg.query('set role deploy'); }
});

await ta('[W3] DB の active を読めない (PR-1 の実行権の前) = 新商品・保存とも 503 owner_unreadable (fail-closed・何も書かない)', async () => {
  const b = await snap();
  await W2.revokeActiveMap(pg);
  try {
    await rejectsWith(reg('single', 'w3-a', single()), 503, 'owner_unreadable');
    await rejectsWith(saveName('s001', '読めない'), 503, 'owner_unreadable');
  } finally { await W2.grantActiveMap(pg); }
  assert.deepEqual(await snap(), b);
});

server.close();
try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows で開いたままのことがある */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
