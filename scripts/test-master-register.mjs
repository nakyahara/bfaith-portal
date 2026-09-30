/**
 * test-master-register.mjs — 新商品の登録・登録の状態・product-hub のカードの outbox (Company DB構想 14 ⑤-2a / 契約 v3 H3・Medium 1・§11)
 *
 * Company DB = PGlite (Render と同じ持ち主のロール deploy で migration)。product-hub = 一時の DATA_DIR の SQLite。本物の router を HTTP 越しにも通す
 * 固定する契約:
 *   S 登録の状態 (0051・PR #1566 R1): 行が無い = 使えない (ビューに出ない)・書くのは security definer の関数だけ = 画面・運用のロールは GUC を立てても 42501 (H2)・
 *     根拠の表ができるまで (⑤-2b / ④) は NE 登録待ち・NE 確認済み・配る対象・利用可へ進めない (H3)・やめる = 人の理由・履歴は追記だけ・
 *     backfill = 段階 frozen / company_owner の間に 1 回だけ・(company_id, sku_id, code_norm, sku_kind) のハッシュが同じときだけ (M4)・下書きは触らない・
 *     backfill の前でも画面のロールが足した SKU は状態の行が要る・backfill の後はだれでも・夜間ロードが NE から作った SKU は同じ取引で quarantined・
 *     new_open は backfill がちょうど 1 回 かつ 状態の行の無い SKU が 0 件のときだけ (H1)
 *   R 新商品の登録: 門 (段階 new_open・持ち主 company・MASTER_EDIT_OPEN) が閉じている = 409 何も書かない (失敗の記録は残る)・形の検査 400・
 *     コードの検査 409 (Company DB・名札・NE・消したコード)・単品 / セットを 1 つの取引で (SKU・商品・状態 draft・仕入先・原価・構成の依頼・知らせ・記録)・
 *     巻き戻る = 知らせも残らない・同じ request_id = 前の結果・編集の印に登録の状態・やめた商品は直せない
 *   O 知らせ (outbox) の取り込み: 1 回で 1 枚・2 回でも 1 枚 (冪等)・同じ商品コードのカード = 衝突 (増やさない・記録)・人が「既存のカードに結ぶ」で解く (M6)・SQLite の失敗 = failed のまま・もう一度で作れる・
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

// product-hub の SQLite は一時の場所 (本番の DB に触らない)。warehouse-mirror/db.js を読む前に決める
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'master-register-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
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
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
const q = async (sql, params) => (await db.query(sql, params)).rows;
const one = async (sql, params) => (await q(sql, params))[0];
/** 1 つの取引で流す (失敗は巻き戻して投げる) */
async function tx(fn) {
  await pg.query('begin');
  try { const r = await fn(); await pg.query('commit'); return r; } catch (e) { try { await pg.query('rollback'); } catch { /* */ } throw e; }
}

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const LOAD_NOW = new Date('2030-01-05T03:00:00Z');
const NOW = new Date('2030-01-10T03:00:00Z');
const TODAY = '2030-01-10';
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210.4 }], ['S02', { method: '宅急便', cost: 520 }], ['S03', { method: '謎の便', cost: 300 }]]);
const RUN = 'mc_20300109T000000000Z_abcdef';
const uuid = () => crypto.randomUUID();
const SHA = 'a'.repeat(64);
const ACKS = (g) => ['minipc:products', 'minipc:components', 'minipc:registrations', 'render:products', 'render:components', 'render:registrations'].map((t) => ({ target: t, generation: g }));
const plan = async () => one('select * from ops.registration_backfill_plan()');
const prereq = async (from, to) => (await one('select ops.master_cutover_prereq_problems($1, $2) as p', [from, to])).p;
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
const reg = (kind, code, values, card = {}, o = {}) => R.registerNewSku(db, {
  actor: o.actor ?? 'Naka@Test', requestId: o.requestId ?? uuid(), kind, code, reason: o.reason ?? null, values, card,
}, { ownership: o.ownership ?? ALL_COMPANY, open: o.open ?? true, now: NOW, shippingRates: o.shippingRates === undefined ? RATES : o.shippingRates, beforeCommit: o.beforeCommit });
const single = (over = {}) => ({ name: '新しい単品', standard_price: '1,980', shipping_code: 'S01', tax_rate: '10', ...over });

console.log('登録の状態 (0051)');

// 画面・運用のロール (本番は ⑤-1 の create-master-edit-roles.mjs が作る。権限は scripts/company-db/master-register-grants.mjs のとおり)
const { masterRegisterRoleStatements } = await import('./company-db/master-register-grants.mjs');
for (const r of ['master_edit', 'master_ops']) await pg.query(`create role ${r} nologin`);
for (const st of masterRegisterRoleStatements()) await pg.query(st);
/** ロールを替えて fn を流す (PGlite = 1 接続。終わったら持ち主のロールに戻す) */
async function asRole(role, fn) {
  await pg.query(`set role ${role}`);
  try { return await fn(); } finally { await pg.query('set role deploy'); }
}
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

await ta('[S2b] backfill の前でも、表の持ち主でないロール (画面) が SKU を足した取引は、状態の行が無いと commit できない', async () => {
  const code = await asRole('master_edit', () => pgCode(tx(() => pg.query(`insert into core.skus (company_id, sku_kind, code, name, created_by_type, created_by_id) values (1, 'set', 'edit-direct', 'x', 'human', 'x')`))));
  assert.equal(code, '23514');
  assert.equal(await skuId('edit-direct'), undefined);
});

await ta('[S3] 根拠の表ができるまで (⑤-2b / ④) は NE 登録待ち・NE 確認済み・配る対象・利用可へ進めない (not_ready)・一方向・やめる = 人の理由だけ (運用のロールも)', async () => {
  const d = await skuId('tx-1');
  await assert.rejects(() => transition(d, 'ne_pending', { evidence: { export_id: 7, sha256: SHA } }), /not_ready/);
  await assert.rejects(() => transition(d, 'ne_confirmed', { evidence: { compare_run_id: RUN, matched: true } }), /one_way/);
  await assert.rejects(() => transition(d, 'available', { evidence: { generation: 'g1', acks: ACKS('g1') } }), /one_way/);
  // 持ち主が状態を直接作った (試験だけ) としても、根拠の表が無い = 進めない
  await forceState(d, 'ne_pending');
  await assert.rejects(() => transition(d, 'ne_confirmed', { actorType: 'system', actor: 'compare', evidence: { compare_run_id: RUN, matched: true } }), /not_ready/);
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
  await C.advanceCutoverPhase(db, { to: 'frozen', actor: 'naka@test' });
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
  await C.advanceCutoverPhase(db, { to: 'company_owner', actor: 'naka@test' });
  const probs = await prereq('company_owner', 'new_open');
  assert.ok(probs.some((x) => /^backfill_missing/.test(x)) && probs.some((x) => /^unregistered_skus/.test(x)), JSON.stringify(probs));
  assert.deepEqual(await prereq('legacy_open', 'frozen'), []);
  await assert.rejects(() => C.advanceCutoverPhase(db, { to: 'new_open', actor: 'naka@test' }), /cutover_prereq.*backfill_missing/);
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
  await assert.rejects(() => C.advanceCutoverPhase(db, { to: 'new_open', actor: 'naka@test' }), /unregistered_skus/);
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
  await C.advanceCutoverPhase(db, { to: 'new_open', actor: 'naka@test' });
  e = await rejectsWith(reg('single', 'new-a1', single(), {}, { ownership: MASTER_OWNERSHIP }), 409, 'before_cutover');
  assert.ok(e.extra.load_keys.includes('skus.name'));
  e = await rejectsWith(reg('single', 'new-a1', single(), {}, { open: false }), 409, 'before_cutover');
  assert.equal(e.extra.open, false);
  e = await rejectsWith(reg('set', 'new-set-x', { name: 'x', standard_price: '1', shipping_code: 'S01', components: [{ code: 's001', qty: 1 }] }, {},
    { ownership: { ...ALL_COMPANY, sku_components: 'load' } }), 409, 'before_cutover');
  assert.deepEqual(e.extra.load_keys, ['sku_components']);
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
  assert.deepEqual((await q(`select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id join core.skus k on k.sku_id = x.sku_id where k.code = 'new-a1' and x.is_primary`)).map((x) => x.code), ['0001']);
  assert.deepEqual(await q(`select c.cost_jpy::int as jpy, c.cost_source as src, c.cost_status as st, c.valid_from::text as f, c.valid_to, c.reason from core.sku_costs c join core.skus k on k.sku_id = c.sku_id where k.code = 'new-a1'`),
    [{ jpy: 800, src: 'manual', st: 'COMPLETE', f: TODAY, valid_to: null, reason: '新商品の登録' }]);
  const ob = await one(`select o.status, o.schema_version, o.payload, o.payload_hash, o.request_id::text as rid, o.created_by from ops.product_hub_outbox o join core.skus k on k.sku_id = o.sku_id where k.code = 'new-a1'`);
  assert.deepEqual([ob.status, ob.schema_version, ob.rid, ob.created_by], ['pending', 'ph-card-v1', id, 'naka@test']);
  assert.equal(ob.payload_hash, O.cardPayloadHash(ob.payload));
  assert.deepEqual([ob.payload.code, ob.payload.price, ob.payload.asin, ob.payload.reference_urls, ob.payload.shipping, ob.payload.set_decision, ob.payload.yahoo.price, ob.payload.yahoo.category_id],
    ['new-a1', 1980, 'B0ABCDEFGH', ['https://ref.example/1', 'https://ref.example/2'], { code: 'S01', method: 'ゆうパケット', cost_jpy: 210 }, { decision: 'none', reason_code: 'low_demand', reason_text: null }, 2080, 12345]);
  assert.equal(ob.payload.cdb_sku_id, r.sku_id);
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
  await rejectsWith(W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 'tok-1', reason: null, seen: { token: tk1 }, values: { name: 'x' } }, { ownership: ALL_COMPANY, open: true, now: NOW }), 400, 'cancelled_sku');
  const a1 = await skuId('new-a1');
  const r = await W.saveSku(db, { actor: 'naka@test', requestId: uuid(), code: 'new-a1', reason: null, seen: { token: W.editTokenOf(await W.readCurrent(db, a1, TODAY)) }, values: { reorder_months: '2' } }, { ownership: ALL_COMPANY, open: true, now: NOW });
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

console.log('\nproduct-hub のカードの知らせ (outbox)');

const ph = PHDB.getDB();
ph.prepare(`INSERT INTO ph_shipping_method_map (ne_label, rakuten_group) VALUES ('ゆうパケット', '9'), ('宅急便', '7')`).run();
const apply = (ev) => PH.applyCdbCardEvent(ev);
const outboxOf = async (code) => one(`select o.event_id::text as event_id, o.status, o.attempts, o.last_error, o.result, o.lease_owner from ops.product_hub_outbox o join core.skus k on k.sku_id = o.sku_id where k.code = $1`, [code]);
const draftOf = (code) => ph.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code);

await ta('[O1] 取り込む: カード 1 枚 (名前・売価・URL・ASIN・cdb_sku_id)・参考 URL・作らない判断・Yahoo! (ヤフーだけ別)・楽天の配送方法 (対応表)・記録', async () => {
  const ev = await outboxOf('new-a1');
  const res = await O.runCardOutbox(db, apply, { eventId: ev.event_id });
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
  assert.deepEqual(await O.runCardOutbox(db, apply, { eventId: ev.event_id, manual: true }), []);
  const payload = (await one('select payload from ops.product_hub_outbox where event_id = $1', [ev.event_id])).payload;
  const again = PH.applyCdbCardEvent({ event_id: ev.event_id, schema_version: 'ph-card-v1', payload });
  assert.deepEqual([again.outcome, again.replayed], ['created', true]);
  const other = PH.applyCdbCardEvent({ event_id: uuid(), schema_version: 'ph-card-v1', payload });
  assert.equal(other.outcome, 'linked');
  assert.equal(ph.prepare("SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'new-a1'").get().c, 1);
  // 済んだ知らせは変えられない
  await assert.rejects(() => pg.query(`update ops.product_hub_outbox set status = 'pending', done_at = null where event_id = $1`, [ev.event_id]), /済んだ/);
});

await ta('[O3] 同じ商品コードのカードがもうある = 衝突 (増やさない・直さない・記録)。古いカードを片付けてから「もう一度」で作れる', async () => {
  const r = await reg('single', 'new-a2', single({ name: '衝突する単品' }));
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('new-a2', '前から product-hub にあったカード', 'someone')`).run();
  const old = draftOf('new-a2');
  let res = await O.runCardOutbox(db, apply, { eventId: r.card.event_id });
  assert.equal(res[0].status, 'conflict');
  assert.equal(ph.prepare("SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'new-a2'").get().c, 1);
  assert.equal(draftOf('new-a2').name, '前から product-hub にあったカード');
  assert.equal(draftOf('new-a2').cdb_sku_id, null);
  const ob = await outboxOf('new-a2');
  assert.deepEqual([ob.status, ob.result.conflict_draft_id], ['conflict', old.id]);
  assert.match(ob.last_error, /#\d+/);
  assert.equal(ph.prepare('SELECT outcome FROM ph_cdb_card_events WHERE event_id = ?').get(r.card.event_id).outcome, 'conflict');
  // 自動では衝突を試さない
  assert.deepEqual(await O.runCardOutbox(db, apply, { eventId: r.card.event_id }), []);
  ph.prepare('DELETE FROM product_drafts WHERE id = ?').run(old.id);
  res = await O.runCardOutbox(db, apply, { eventId: r.card.event_id, manual: true });
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
  assert.deepEqual(await O.runCardOutbox(db, apply, { eventId: r.card.event_id }), []);   // 自動はもう試さない
  res = await O.runCardOutbox(db, apply, { eventId: r.card.event_id, manual: true });   // 人が押す
  assert.equal(res[0].status, 'done');
  ob = await outboxOf('new-a3');
  assert.deepEqual([ob.status, ob.attempts], ['done', O.CARD_MAX_AUTO_ATTEMPTS + 1]);
  assert.ok(draftOf('new-a3'));
});

await ta('[O5] 送料コードに楽天の配送方法の対応が無い = 楽天の配送方法は空・記録は unmapped・ボードの「要確認」に出る (選んだら消える)', async () => {
  const r = await reg('single', 'new-a4', single({ name: '謎の便の単品', shipping_code: 'S03' }), { yahoo: { price: '999' } });
  await O.runCardOutbox(db, apply, { eventId: r.card.event_id });
  const d = draftOf('new-a4');
  assert.equal(ph.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(d.id), undefined);
  assert.equal(ph.prepare('SELECT shipping_status FROM ph_cdb_card_events WHERE draft_id = ?').get(d.id).shipping_status, 'unmapped');
  assert.deepEqual(ph.prepare('SELECT delivery_label, shipping_override FROM draft_yahoo WHERE draft_id = ?').get(d.id), { delivery_label: null, shipping_override: 0 });
  assert.ok(PH.cdbShippingCheckIds(ph).has(d.id));
  assert.ok(!PH.cdbShippingCheckIds(ph).has(draftOf('new-a1').id));
  assert.ok(ph.prepare("SELECT 1 FROM draft_events WHERE draft_id = ? AND event = 'cdb_shipping_unmapped'").get(d.id));
  // 名前が似ているだけでは推し量らない (対応表に無い = 空)
  assert.equal(PH.mapCdbShipping(ph, { code: 'X', method: 'ネコポス' }).status, 'unmapped');
  ph.prepare(`INSERT INTO draft_rakuten (draft_id, shipping_method_group) VALUES (?, '1')`).run(d.id);
  assert.ok(!PH.cdbShippingCheckIds(ph).has(d.id));
});

await ta('[O6] 知らせの中身は変えられない・消せない・hash が違う知らせは取り込まない・借り (lease) の間はほかが取らない', async () => {
  const r = await reg('single', 'new-a5', single({ name: '借りの単品' }));
  await assert.rejects(() => pg.query(`update ops.product_hub_outbox set payload = '{}'::jsonb where event_id = $1`, [r.card.event_id]), /変えない/);
  await assert.rejects(() => pg.query(`delete from ops.product_hub_outbox where event_id = $1`, [r.card.event_id]), /消さない/);
  await assert.rejects(() => pg.query('truncate ops.product_hub_outbox'), /append-only/);
  // 借りの間 (ほかの処理が取り込み中) は取らない
  await pg.query(`update ops.product_hub_outbox set lease_owner = 'other', leased_until = now() + interval '1 minute' where event_id = $1`, [r.card.event_id]);
  assert.deepEqual(await O.runCardOutbox(db, apply, { eventId: r.card.event_id, manual: true }), []);
  assert.equal((await O.readCardEvent(db, r.sku_id)).leased, true);
  await pg.query(`update ops.product_hub_outbox set leased_until = now() - interval '1 second' where event_id = $1`, [r.card.event_id]);   // 期限切れ = 取れる
  assert.equal((await O.runCardOutbox(db, apply, { eventId: r.card.event_id }))[0].status, 'done');
  // hash が違う知らせ (書いた後の中身と合わない) は取り込まない
  const sid = await skuId('s002');
  const bad = (await pg.query(`insert into ops.product_hub_outbox (sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
     values ($1, 'card_create', 'ph-card-v1', '{"schema":"ph-card-v1","code":"s002"}'::jsonb, $2, $3, 'x') returning event_id::text as id`, [sid, 'c'.repeat(64), uuid()])).rows[0].id;
  const res = await O.runCardOutbox(db, apply, { eventId: bad });
  assert.deepEqual([res[0].status, /payload_hash_mismatch/.test(res[0].error)], ['failed', true]);
  assert.equal(draftOf('s002'), undefined);
});

await ta('[O7] 衝突を人が解く = 既存のカードをこの商品に結ぶ (SQLite で確かめ直して結ぶ → 知らせを done)・もう一度押しても同じ・確かめに落ちたら conflict のまま・SQLite だけ結べた後も冪等', async () => {
  const r = await reg('single', 'new-a6', single({ name: '結ぶ単品' }));
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('New-A6 ', '前からのカード', 'someone')`).run();
  const old = draftOf('new-a6');
  await O.runCardOutbox(db, apply, { eventId: r.card.event_id });
  assert.equal((await outboxOf('new-a6')).status, 'conflict');
  const link = (ev, o) => PH.linkCdbCardToExisting(ev, o);
  // 確かめに落ちる (カードの商品コードがその間に変わった) = conflict のまま・結ばない
  ph.prepare(`UPDATE product_drafts SET ne_code = 'other-code' WHERE id = ?`).run(old.id);
  await assert.rejects(() => O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test' }), /商品コード/);
  assert.equal((await outboxOf('new-a6')).status, 'conflict');
  assert.equal(ph.prepare('SELECT cdb_sku_id FROM product_drafts WHERE id = ?').get(old.id).cdb_sku_id, null);
  ph.prepare(`UPDATE product_drafts SET ne_code = 'New-A6 ' WHERE id = ?`).run(old.id);
  const out = await O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test' });
  assert.deepEqual([out.ok, out.draft_id, out.already], [true, old.id, false]);
  const ob = await outboxOf('new-a6');
  assert.deepEqual([ob.status, ob.result.outcome, ob.result.draft_id, ob.result.linked_by, ob.last_error], ['done', 'linked', old.id, 'naka@test', null]);
  assert.equal(String(ph.prepare('SELECT cdb_sku_id FROM product_drafts WHERE id = ?').get(old.id).cdb_sku_id), r.sku_id);
  assert.equal(ph.prepare("SELECT COUNT(*) AS c FROM product_drafts WHERE LOWER(TRIM(ne_code)) = 'new-a6'").get().c, 1);
  assert.equal(ph.prepare('SELECT outcome FROM ph_cdb_card_events WHERE event_id = ?').get(r.card.event_id).outcome, 'linked');
  assert.ok(ph.prepare("SELECT 1 FROM draft_events WHERE draft_id = ? AND event = 'cdb_card_linked'").get(old.id));
  const again = await O.linkCardToExisting(db, link, { skuId: r.sku_id, actor: 'naka@test' });
  assert.deepEqual([again.ok, again.already, again.reason], [true, true, 'already_done']);
  // SQLite では結べたが Postgres を done にできなかった = 次の取り込み (もう一度) が「もう結んである」で done にする
  const r2 = await reg('single', 'new-a7', single({ name: '結ぶ単品 2' }));
  ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('new-a7', '前からのカード 2', 'someone')`).run();
  await O.runCardOutbox(db, apply, { eventId: r2.card.event_id });
  const payload = (await one('select payload from ops.product_hub_outbox where event_id = $1', [r2.card.event_id])).payload;
  PH.linkCdbCardToExisting({ event_id: r2.card.event_id, schema_version: 'ph-card-v1', payload }, { draftId: draftOf('new-a7').id, actor: 'naka@test' });
  assert.equal((await outboxOf('new-a7')).status, 'conflict');
  const res = await O.runCardOutbox(db, apply, { eventId: r2.card.event_id, manual: true });
  assert.deepEqual([res[0].status, res[0].result.outcome], ['done', 'linked']);
  // 衝突でない知らせは結ばない
  assert.equal((await O.linkCardToExisting(db, link, { skuId: await skuId('new-a1'), actor: 'x' })).reason, 'already_done');
});

// ── 画面 (router) ──
console.log('\n画面 (master-edit)');
const MR = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://test@localhost:5432/test';
process.env.MASTER_EDITORS = 'Naka@Test, other@test';
process.env.MASTER_EDIT_OPEN = '1';
MR.__setPgClientFactory(async () => ({ query: (t, p) => pg.query(t, p), end: async () => {}, on: () => {} }));
MR.__setClock(() => NOW.getTime());
MR.__setOwnership(ALL_COMPANY);
MR.__setShippingRatesProvider(async () => RATES);
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
/** 画面の JS が文法として読めること・EJS の出力が JS に混ざっていないこと (属性つきの script も数える) */
function checkScripts(html, expected) {
  const opens = [...html.matchAll(/<script\b/gi)].length;
  const scripts = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].filter((m) => !/\bsrc=/.test(m[1])).map((m) => m[2]);
  assert.equal([...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].length, opens, 'script の開きと閉じの数が合わない');
  if (expected != null) assert.equal(scripts.length, expected, `<script> の数 ${scripts.length}`);
  for (const s of scripts) { new vm.Script(s); assert.ok(!/<%|%>/.test(s), 'EJS のタグが JS に残っている'); }
  return scripts;
}

await ta('[H1] 新商品の画面 (単品・セット): 描画・画面の JS・必須の印・product-hub と Yahoo! の欄・切替前の帯 (MASTER_EDIT_OPEN なし・持ち主 load)', async () => {
  let r = await call('GET', '/apps/master-edit/new?kind=single');
  assert.equal(r.status, 200, r.text.slice(0, 300));
  const sc = checkScripts(r.text, 1);
  for (const api of ["'/api/new'", "'/api/code-check?code='", "'/api/lookup?code='"]) assert.ok(sc[0].includes(api), `画面が ${api} を呼んでいない`);
  for (const word of ['商品コード', '売価', '発送方法', '税率', 'Amazon URL', 'ASIN', '参考 URL', '公式ページ URL', 'セット商品を作るか', 'Yahoo!売価', 'Yahoo!売価 (佐川)', 'Yahoo!カテゴリID', 'Yahoo!path', '有効期限の管理', '入荷日の管理']) {
    assert.ok(r.text.includes(word), `単品の画面に「${word}」が無い`);
  }
  assert.match(r.text, /data-can-save="1"/); assert.ok(!/切替前です/.test(r.text));
  assert.match(r.text, /S03 謎の便 \/ 300 円/);
  r = await call('GET', '/apps/master-edit/new?kind=set');
  checkScripts(r.text, 1);
  assert.match(r.text, /id="comp-rows"/); assert.ok(!r.text.includes('セット商品を作るか'), 'セットに「セット商品を作るか」を出さない');
  assert.ok(!r.text.includes('有効期限の管理'), 'セットにロジザードの欄を出さない');
  delete process.env.MASTER_EDIT_OPEN;
  r = await call('GET', '/apps/master-edit/new?kind=single');
  assert.match(r.text, /切替前です/); assert.match(r.text, /data-can-save="0"/); assert.match(r.text, /id="save" disabled/);
  process.env.MASTER_EDIT_OPEN = '1';
  MR.__setOwnership(MASTER_OWNERSHIP);
  assert.match((await call('GET', '/apps/master-edit/new?kind=set')).text, /切替前です/);
  MR.__setOwnership({ ...ALL_COMPANY, sku_components: 'load' });
  assert.match((await call('GET', '/apps/master-edit/new?kind=set')).text, /切替前です/);   // セットは構成の持ち主も
  assert.ok(!/切替前です/.test((await call('GET', '/apps/master-edit/new?kind=single')).text));
  MR.__setOwnership(ALL_COMPANY);
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
  checkScripts(page.text, 1);
  assert.match(page.text, /カード作成待ち \(失敗\)/); assert.match(page.text, /id="card-retry"/); assert.match(page.text, /登録: 下書き/);
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
  checkScripts(r.text, 0);
  assert.ok(r.text.includes('sku/new-a1') && r.text.includes('sku/web-2') && !r.text.includes('sku/s001"'));
  assert.match(r.text, /href="new\?kind=single"/);
  r = await call('GET', '/apps/master-edit/?reg=quarantined');
  assert.ok(r.text.includes('sku/s900'));
  const R2 = await import('../apps/master-edit/read.mjs');
  assert.deepEqual((await R2.listSkus(db, { reg: 'quarantined' }, { now: NOW })).rows.map((x) => x.code), ['ghost-1', 'race-1', 's900']);
  assert.deepEqual((await R2.listSkus(db, { reg: 'ne_confirmed' }, { now: NOW })).rows.map((x) => x.code), []);
  assert.deepEqual((await R2.listSkus(db, { reg: 'none' }, { now: NOW })).rows.map((x) => x.code), []);
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
  checkScripts(page.text, 1);
  assert.match(page.text, /id="card-link" data-draft="\d+"/); assert.match(page.text, /カードの衝突/);
  assert.equal((await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: {}, session: 'viewer' })).status, 403);
  const ok = await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: {} });
  assert.deepEqual([ok.status, ok.j.ok, ok.j.draft_id], [200, true, old.id]);
  page = await call('GET', '/apps/master-edit/sku/web-9');
  assert.match(page.text, /カード作成済み/); assert.ok(!/id="card-link"/.test(page.text));
  const again = await call('POST', '/apps/master-edit/api/sku/web-9/card-link', { body: {} });
  assert.deepEqual([again.status, again.j.already], [200, true]);
  assert.equal((await call('POST', '/apps/master-edit/api/sku/nope/card-link', { body: {} })).status, 404);
});

console.log('\nproduct-hub (新規作成の入口・ボード)');

let phCalls = 0;
let phMode = 'pglite';
O.__setCompanyDbClientFactory(async () => {
  phCalls++;
  if (phMode === 'down') throw new Error('connect ECONNREFUSED');
  if (phMode === 'pglite') return { query: (t, p) => pg.query(t, p), end: async () => {}, on: () => {} };
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
  checkScripts(r.text);
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
  checkScripts(b2.text);
  phMode = 'down';
  assert.equal((await call('GET', '/apps/product-hub/board')).status, 200);
  phMode = 'pglite';
});

await ta('[X1] 別の DB: 段階の門 (trigger) を止めて new_open にしても、backfill が無ければ画面 D は登録しない (409 backfill_missing・何も書かない)', async () => {
  const pg2 = new PGlite();
  try {
    const db2 = pgliteAdapter(pg2);
    await applyMigrations(db2, { log: quiet });
    await pg2.query('alter table ops.master_cutover_state disable trigger trg_master_cutover_state_prereq');
    for (const to of ['frozen', 'company_owner', 'new_open']) await C.advanceCutoverPhase(db2, { to, actor: 'x@test' });
    const e = await rejectsWith(R.registerNewSku(db2, { actor: 'naka@test', requestId: uuid(), kind: 'single', code: 'x-1', values: single(), card: {} },
      { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES }), 409, 'backfill_missing');
    assert.equal(e.extra.phase, 'new_open');
    assert.equal((await pg2.query('select count(*)::int as n from core.skus')).rows[0].n, 0);
  } finally { await pg2.close(); }
});

server.close();
try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows で開いたままのことがある */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
