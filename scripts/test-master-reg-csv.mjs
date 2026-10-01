/**
 * test-master-reg-csv.mjs — ⑤-2b (Company DB構想 14 §10 契約 v3 H5・H6・H2・Medium 3 / migration 0053)
 *
 * Company DB = PGlite (持ち主のロール deploy で migration)。書き込みは画面だけのロール master_edit・照合の確かめは watch_writer で流す (権限も確かめる)
 * 固定する契約:
 *   C 新商品の NE 登録の CSV: 門 (切替の後だけ)・止まる理由 (原価・仕入先・NE にもうあるコード = 止める・構成品が NE 確認済みでない)・
 *     ファイル (見出し・UTF-8・BOM なし・CRLF・sha256・形の版・payload hash・印)・同じ番号 = 同じファイル・試し用の行の上限・
 *     配る前に直す = 自動で使わない / 配った後に NE に送る欄を直す = 409 (ほかの欄は直せる)・申告 (sha256・誰・いつ・draft → ne_pending)・
 *     翌朝の確かめ (申告の前の取得 = 待ち・違う列 = partial・全部合う = verified + ne_confirmed・無い = failed・信じられない = 待ち・申告の前に NE にある = 記録だけ・同じ回 = 1 回)・
 *     セットは同じ取得の構成品が全部合うときだけ・使わない (理由・直し方・確かめた) ・全部だめ = failed・実機の確かめの門・DB の守り
 *   J JAN: 足す・外すの記録 (誰・request_id・出どころ・理由)・書き換え / 物理の削除は拒む・SKU の version が変わる・一意 (409)・画面 B で直せる
 *     (新商品の登録 = 画面 D は JAN を受けない = 登録の後に商品の画面で)・夜間ロードは持ち主 'company' なら商品の JAN に触らない
 *   O セットの構成の観測: 夜間ロードが完全な取得を観測に残す (同じ材料 = 1 回)・持ち主 load = 今までどおり core を NE に合わせる /
 *     company = core を書かない・依頼が無い差 = 食い違い・依頼と同じ = 上げる・数は 0050 の厳密な整数・昇格の答え (もっと新しい観測など) は投げずに数える
 *   S 仕入先: 新しいコードの決まり・状態 ne_pending → 申告で ne_confirmed・申告の前は代表にできない・取引停止 (代表に使っている = 409 / 付け替え)・物理の削除は拒む
 *   D 書き込みの約束 (0051 の ops.master_write_sessions・#1571 R1 High 3): ⑤-2b の関数ごとに約束 (操作・DB が決めた相手・DB の payload_hash) と done・
 *     request_id は使い回せない・画面のロールは約束を作れない / CSV の表・JAN の行の守り (約束の操作・相手)・保存の直接の書き込みも配った CSV の欄は拒む・
 *     確かめる前の仕入先は設定に依らず代表にできない・仕入先の関数は画面のロールに渡さない・⑤-2b の操作の一覧は 1 か所
 *   P 照合 ② の観測 (registrationObservations) の形
 *   H 画面: NE 登録の CSV の画面と JS・名簿・作る → 配る → ダウンロード → 申告・商品の画面の JAN と CSV の箱・新商品の JAN
 * 使い方: node scripts/test-master-reg-csv.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'master-reg-csv-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad, observationQtyOf, PROMOTE_OUTCOMES } = await import('../apps/company-db/load/engine.mjs');
const { MASTER_OWNERSHIP } = await import('../config/master-ownership.mjs');
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const R = await import('../lib/master-register.mjs');
const G = await import('../lib/master-reg-csv.mjs');
const SUP = await import('../lib/master-supplier.mjs');
const CNE = await import('../apps/company-db/master-compare/compare-ne.mjs');

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
const pgErr = async (p, re) => { try { await p; } catch (e) { if (re) assert.match(String(e.message), re); return e; } assert.fail('拒まれなかった'); };
const uuid = () => crypto.randomUUID();
/** JAN のチェック数字を付ける (12 桁 → 13 桁) */
const jan13 = (b) => { const d = b.split('').map(Number).reverse(); const s = d.reduce((a, x, i) => a + x * (i % 2 === 0 ? 3 : 1), 0); return b + ((10 - (s % 10)) % 10); };
const J1 = jan13('490000000001'), J2 = jan13('490000000002'), J3 = jan13('490000000003'), J4 = jan13('490000000004');

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const OWN = JSON.stringify(ALL_COMPANY);
// 夜間ロードの日 = 本当の今日の 5 日前 (原価の始まり。⑤-2a の登録は DB の東京の今日の構成品の原価を見る = 本当の今日にも画面の今日 2030-01-10 にも原価がある)
const LOAD_NOW = new Date(Date.now() - 5 * 86400e3);
const NOW = new Date('2030-01-10T03:00:00Z');   // 画面の今日 (東京 2030-01-10 12:00)
const NOW_MS = NOW.getTime();
const TODAY = '2030-01-10';
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }], ['S02', { method: '宅急便', cost: 520 }]]);
const RUN1 = 'mc_20300110T000000000Z_aaaaaa';
/** ⑤-3 の古い入口の一覧 (manifest) の形の例・切替の証拠 (⑤-1 / ⑤-2a の試験と同じ) */
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }] };
/** 証拠の時刻は今の段階に入った後・サーバーの今以前 (⑤-1 #1563 R3 High 1) = 進める直前の今 */
const manualStopped = () => [{ id: 'ne.product_screen', by: 'naka@test', at: new Date().toISOString() }];
const drain = () => ({ done: true, checked_by: 'naka@test', checked_at: new Date().toISOString() });
const BUILDS = { render: ['r1'], minipc: ['m1'] };

function makePlan({ setComponents = null, material = null, extraSkus = [] } = {}) {
  const sku = (code, name, kind, taxRate, salesClass, cost, x = {}) => ({
    code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : taxRate === 0.1 ? 'STANDARD_10' : null, handling: 'active', salesClass,
    cost: cost == null ? null : { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' },
    standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...x,
  });
  const singles = ['s001', 's002', 's003', 's004', 's005', 's006', 's007'].map((c, i) => sku(c, `単品 ${i + 1}`, 'single', i === 1 ? 0.08 : 0.1, 3, 100 * (i + 1)));
  return {
    skus: [...singles, sku('set001', 'セット 1', 'set', 0.08, null, 400, { taxClass: 'MIXED' }), ...extraSkus],
    variationGroups: [{ code: 'grp1', name: '名札', childCodes: ['s006', 's007'], status: 'active' }],
    setComponents: setComponents ?? [{ parentCode: 'set001', childCode: 's001', qty: 2, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' }],
    listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }, { code: 'abc', name: '数字でない前からの仕入先' }],
    supplierSkus: singles.map((s) => ({ supplierCode: '0001', skuCode: s.code })),
    primarySuppliers: singles.map((s) => ({ skuCode: s.code, supplierCode: '0001' })),
    reorder: { available: true, runId: 'pml_test' },
    ...(material ? { material } : {}),
  };
}
/** 材料の世代 (set_components が matched・NE の完全な取得の時刻つき) */
const materialOf = (genId, completeAt) => ({
  products: { status: 'no_generation', content_hash: 'a'.repeat(64), row_count: 1, generation: null },
  set_components: { status: 'matched', content_hash: 'b'.repeat(64), row_count: 1, generation: { generation_id: genId, content_hash: 'b'.repeat(64), row_count: 1, source_complete_at: completeAt, created_at: completeAt } },
});

/**
 * 1 つの Company DB: migration・ロール・夜間ロード・切替 (⑤-1 の本物の関数: 場所ごとの門のログインが記録 → 運用のロール master_ops が証拠つきで 1 段ずつ。
 * frozen で backfill (master_ops)・company_owner から全部 company のハッシュ)・今日の照合の回と NE の元のコード
 */
async function setupDb() {
  const pg = new PGlite();
  const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;   // 試験の接続のログイン (superuser)。門のログインから戻るときに使う
  await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
  await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
  await pg.query('set role deploy');
  const db = pgliteAdapter(pg);
  await applyMigrations(db, { log: quiet });
  await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
  await createMasterEditRoles(pg, {});
  const r = await runInitialLoad(db, makePlan(), { log: quiet, runId: 'load_setup', now: LOAD_NOW });
  assert.equal(r.ok, true, r.error);
  const E0 = { pg, db, sessionUser };
  await toPhase(E0, 'frozen');
  const p = (await pg.query('select * from ops.registration_backfill_plan()')).rows[0];
  await as(E0, 'master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 'naka@test']));
  await toPhase(E0, 'company_owner');
  await toPhase(E0, 'new_open');
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
  await recordNeCodes(pg, RUN1, []);
  return E0;
}
/** 場所ごとの門のログイン (DB の関数は session_user を見る = SET SESSION AUTHORIZATION) */
async function asGate(E, host, fn) {
  await E.pg.query(`set session authorization master_gate_${host}`);
  try { return await fn(); } finally { await E.pg.query(`set session authorization ${E.sessionUser}`); await E.pg.query('set role deploy'); }
}
/** 切替の段階を 1 つ進める (門の記録 → master_ops が証拠つきで)。持ち主表 = company_owner から ALL_COMPANY */
async function toPhase(E, to) {
  const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
  const own = to === 'frozen' ? MASTER_OWNERSHIP : ALL_COMPANY;
  for (const [host, inst, buildId] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) {
    await asGate(E, host, () => C.recordLegacyGateAck(E.db, { host, instanceId: inst, buildId, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
  }
  const mh = await C.manifestHashOf(E.db, MANIFEST);
  const evidence = to === 'frozen' ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(MASTER_OWNERSHIP), manual_entries_stopped: manualStopped(), drain: drain() }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL_COMPANY) };
  return as(E, 'master_ops', () => C.advanceCutoverPhase(E.db, { to, actor: 'naka@test', evidence }));
}
/** NE の元のコード (0041) を書く = 前からある商品・代表の名札 + extra (NE にもうあるコード) */
async function recordNeCodes(pg, run, extra) {
  const codes = ['s001', 's002', 's003', 's004', 's005', 's006', 's007', 'set001', ...extra];
  const entries = [...codes.map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })), { code_norm: 'grp1', kind: 'rep', state: 'ok', ne_code: 'GRP1', spellings: ['GRP1'] }];
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries })]);
}
async function as(E, role, fn) { await E.pg.query(`set role ${role}`); try { return await fn(); } finally { await E.pg.query('set role deploy'); } }

const E = await setupDb();
const { pg, db } = E;
const q = async (sql, params) => (await db.query(sql, params)).rows;
const one = async (sql, params) => (await q(sql, params))[0];
const skuId = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code]))?.id;
const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = $1', [code]))?.state;
const tokenOf = async (code) => W.editTokenOf(await W.readCurrent(db, await skuId(code), TODAY));
async function save(code, values, { ownership = ALL_COMPANY, reason = 'テスト', requestId = uuid() } = {}) {
  const seen = { token: await tokenOf(code) };
  return as(E, 'master_edit', () => W.saveSku(db, { actor: 'naka@test', requestId, code, reason, seen, values }, { ownership, open: true, now: NOW, shippingRates: RATES }));
}
/** 新商品の登録 (⑤-2a)。原価の始まりは DB の東京の今日 (0052 の ops.register_new_sku が now() で確かめる) = 本当の今日 (画面の今日 2030-01-10 より前 = その日の原価にも出る) */
const reg = (kind, code, values, o = {}) => as(E, 'master_edit', () => R.registerNewSku(db, { actor: 'naka@test', requestId: uuid(), kind, code, values, card: { create: false } },
  { ownership: ALL_COMPANY, open: true, now: new Date(), shippingRates: RATES, ...o }));
const single = (over = {}) => ({ name: '新しい単品', standard_price: '1500', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', cost: { jpy: '300' }, ...over });
const opts = (o = {}) => ({ ownership: ALL_COMPANY, open: true, nowMs: NOW_MS, ...o });
/** 仕入先の道 (lib/master-supplier.mjs) の持ち主表 = 切替の後 (全部 company) */
const SOPT = { ownership: ALL_COMPANY };
const build = (kind, codes, o = {}) => as(E, 'master_edit', () => G.buildRegExport(db, { actor: 'boss@test', kind, codes, requestId: o.requestId ?? uuid() }, opts(o)));
const issue = (id, o = {}) => as(E, 'master_edit', () => G.issueRegExport(db, { actor: 'boss@test', exportId: id }, opts(o)));
const declare = (id, sha, result = 'ok', o = {}) => as(E, 'master_edit', () => G.declareRegExport(db, { actor: 'boss@test', exportId: id, sha256: sha, result, neMessage: o.msg ?? '1件成功しました。' }, opts(o)));
/**
 * 画面のロールで lib を通さずに DB の関数を呼ぶ (試験)。⑤-2b の関数は自分で約束 (0051 の ops.master_write_sessions) を書いて閉じる (#1571 R1 High 3) = begin は要らない
 */
const inSession = (fn) => as(E, 'master_edit', fn);
/** 試験の NE の元のコード (lib の neCodeLookup と同じ形: get は非同期) */
const ncOf = (m = new Map()) => ({ run: null, get: async (k) => m.get(k) });
const supersede = (id, x = {}) => as(E, 'master_edit', () => G.supersedeRegExport(db, { actor: 'boss@test', exportId: id, reason: '直したい', correction: 'NE には取り込んでいない', confirm: true, ...x }, opts()));
const itemsOf = async (id) => (await q('select i.state, s.code, i.failed_reason from ops.ne_reg_export_items i join core.skus s on s.sku_id = i.sku_id where i.export_id = $1 order by i.row_from', [id]));
const expOf = async (id) => one('select state, close_reason, trial, sha256, schema_version, header, payload_hash, aggregate_token, item_count, row_count from ops.ne_reg_exports where export_id = $1', [id]);
let runSeq = 0;
/** 照合の回 (確かめ用。今日の回より前の時刻 = 今日の回は RUN1 のまま) */
async function newRun() {
  runSeq++;
  const id = `mc_20300109T${String(runSeq).padStart(9, '0')}Z_bbbbbb`;
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)`, [id, `2030-01-09T0${runSeq % 10}:00:00Z`]);
  return id;
}
/** 照合の確かめ (watch_writer で関数を呼ぶ) */
/**
 * 照合 ② の確かめ (0052・#1571 Codex R1 High 2) = watch_writer で 3 段: 観測を残す → 受け取り (回が最後まで終わった) → 確かめ (回の番号だけ)
 * observe / seal / checkRun を別々に呼べる (途中で落ちた回の試験)。check = 3 段を続けて
 */
const FETCH = (run) => ({ generation_id: `gen_${run}`, products_rev: '7', sets_rev: '8', raw_hash: crypto.createHash('sha256').update(run).digest('hex') });
const observe = (run, observations, { productsAt = new Date(Date.now() + 60000).toISOString(), setsAt = productsAt, absenceTrusted = true, targets = null, fetch = FETCH(run) } = {}) =>
  as(E, 'watch_writer', async () => (await pg.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run, fetch,
    products_at: productsAt, sets_at: setsAt, absence_trusted: absenceTrusted, targets: targets ?? observations.map((o) => o.code_norm), observations })])).rows[0].r);
const seal = (run, hash, evidence = 'e'.repeat(64)) => as(E, 'watch_writer', async () => (await pg.query('select ops.seal_ne_registration_run($1, $2, $3) as r', [run, hash, evidence])).rows[0].r);
const checkRun = (run) => as(E, 'watch_writer', async () => (await pg.query('select ops.record_ne_registration_check($1) as r', [run])).rows[0].r);
const check = async (run, observations, o = {}) => { const w = await observe(run, observations, o); await seal(run, w.observation_hash); return checkRun(run); };
const ok = (v) => ({ st: 'ok', v });
/** 単品の NE の観測 (目標どおり)。over で列を変える */
const obsSingle = (code, over = {}) => ({ code_norm: code, present: true, trusted: true, kind: 'single', cols: {
  name: ok('新しい単品'), supplier: ok('0001'), cost: ok(300), price: ok(1500), tax_rate: ok(0.1), handling: ok('active'), parent: ok(null), ...over } });

// ── 新商品 (下書き) ──
await reg('single', 'new-a', single());
await save('new-a', { jan: J1 });   // 新商品の JAN = 登録の後に商品の画面で (JAN の約束)
await reg('single', 'new-b', single({ name: '新しい単品 B' }));
await reg('single', 'new-nocost', single({ cost: undefined }));
await reg('set', 'new-set', { name: '新しいセット', standard_price: '2500', shipping_code: 'S02', components: [{ code: 's001', qty: 1 }, { code: 's002', qty: 2 }] });

console.log('新商品の NE 登録の CSV (H5)');

await ta('[C1] 門: 切替の後でないと作れない (MASTER_EDIT_OPEN なし・持ち主表が記録と違う = 409)・名簿の形の誤り 400', async () => {
  await rejectsWith(build('products', ['new-a'], { open: false }), 409, 'before_cutover');
  await rejectsWith(build('products', ['new-a'], { ownership: MASTER_OWNERSHIP }), 409, 'before_cutover');
  await rejectsWith(build('nope', ['new-a']), 400);
  await rejectsWith(build('products', []), 400);
  await rejectsWith(build('products', ['new-a', 'NEW-A']), 400);
  await rejectsWith(as(E, 'master_edit', () => G.buildRegExport(db, { actor: 'boss@test', kind: 'products', codes: ['new-a'], requestId: 'x' }, opts())), 400);
  await rejectsWith(build('sets', ['new-a']), 400);
  assert.equal((await one('select count(*)::int as n from ops.ne_reg_exports')).n, 0);
});

await ta('[C2] 止まる理由: 原価が無い・構成品が NE 確認済みでない・NE にもうあるコード (止める = already_in_ne)・1 つでも止まれば作らない', async () => {
  let e = await rejectsWith(build('products', ['new-a', 'new-nocost']), 409, 'not_ready');
  assert.deepEqual(e.extra.items.map((x) => x.code), ['new-nocost']);
  assert.ok(e.extra.items[0].blockers.some((b) => /原価/.test(b)), JSON.stringify(e.extra.items));
  await reg('set', 'new-set-draft', { name: '下書きの単品のセット', standard_price: '2000', shipping_code: 'S02', components: [{ code: 'new-a', qty: 1 }], set_sales_class_override: '3' });
  e = await rejectsWith(build('sets', ['new-set-draft']), 409, 'not_ready');
  assert.ok(e.extra.items[0].blockers.some((b) => /new-a が NE 確認済みでない/.test(b)), JSON.stringify(e.extra.items));
  await reg('single', 'new-dup', single({ name: 'NE にもうある' }));
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ('mc_20300110T010000000Z_cccccc', '2030-01-10T01:00:00Z', 0)`);
  await recordNeCodes(pg, 'mc_20300110T010000000Z_cccccc', ['new-dup']);
  e = await rejectsWith(build('products', ['new-dup']), 409, 'already_in_ne');
  assert.match(e.message, /new-dup/);
  assert.equal((await one('select count(*)::int as n from ops.ne_reg_exports')).n, 0);
  const s = await G.regSummary(db, { nowMs: NOW_MS });
  const dup = s.candidates.find((c) => c.code === 'new-dup');
  assert.equal(dup.stop, 'already_in_ne');
  assert.ok(s.candidates.find((c) => c.code === 'new-a').blockers.length === 0, JSON.stringify(s.candidates));
});

let EXA = null;
await ta('[C3] 単品のファイル: 見出し・行 (空欄なし: JAN / 代表)・UTF-8・BOM なし・CRLF・sha256・形の版・payload hash・印・試し用・同じ番号 = 同じファイル', async () => {
  const rid = uuid();
  const r = await build('products', ['new-a', 'new-b'], { requestId: rid });
  EXA = r.export.export_id;
  const e = await expOf(EXA);
  assert.deepEqual([e.state, e.trial, e.schema_version, e.item_count, e.row_count], ['built', true, 'ne-reg-single-v1', 2, 2]);
  assert.equal(e.header, 'syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code');
  for (const k of ['sha256', 'payload_hash', 'aggregate_token']) assert.match(e[k], /^[0-9a-f]{64}$/);
  const bytes = Buffer.from((await one('select file_bytes from ops.ne_reg_exports where export_id = $1', [EXA])).file_bytes);
  assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'), e.sha256);
  assert.notEqual(bytes[0], 0xef, 'BOM が無い');
  assert.equal(bytes.toString('utf8'), `${e.header}\r\nnew-a,新しい単品,0001,300,1500,10,0,empty,${J1}\r\nnew-b,新しい単品 B,0001,300,1500,10,0,empty,empty\r\n`);
  assert.ok(!/zaiko_su|visible_flg/.test(bytes.toString('utf8')));
  assert.deepEqual((await itemsOf(EXA)).map((x) => [x.code, x.state]), [['new-a', 'built'], ['new-b', 'built']]);
  const it = await one("select expected, item_token, snapshot_hash from ops.ne_reg_export_items where export_id = $1 and code_norm = 'new-a'", [EXA]);
  assert.deepEqual(it.expected, { kind: 'single', values: { name: '新しい単品', supplier: '0001', cost: 300, price: 1500, tax_rate: 0.1, handling: 'active', parent: null } });
  // 印とハッシュは DB の関数が今の値から計算する (#1571 Codex R1 High 1): snapshot = 確かめる値と配る行
  assert.match(it.item_token, /^[0-9a-f]{64}$/);
  assert.equal((await one(`select encode(sha256(convert_to(jsonb_build_object('expected', i.expected, 'cells', (select jsonb_agg(r.cells order by r.row_no) from ops.ne_reg_export_rows r where r.item_id = i.item_id))::text, 'UTF8')), 'hex') as h
      from ops.ne_reg_export_items i where i.export_id = $1 and i.code_norm = 'new-a'`, [EXA])).h, it.snapshot_hash);
  assert.equal((await one('select cost_day::text as d from ops.ne_reg_exports where export_id = $1', [EXA])).d, TODAY);
  assert.equal((await q('select * from ops.ne_reg_export_rows where export_id = $1', [EXA])).length, 2);
  const again = await build('products', ['new-b', 'new-a'], { requestId: rid });
  assert.deepEqual([again.export.export_id, again.replayed], [EXA, true]);
  await rejectsWith(build('products', ['new-a'], { requestId: rid }), 409, 'request_id_reused');
  // 同じ商品を 2 つのファイルに入れない (生きているファイル)
  const e2 = await rejectsWith(build('products', ['new-a']), 409, 'not_ready');
  assert.ok(e2.extra.items[0].blockers.some((b) => /まだ終わっていない/.test(b)));
});

await ta('[C4] 配る前に NE に送る欄を直す = そのファイルは自動で「使わない」(保存は通る)・そのファイルは配れない。NE に送らない欄は触らない', async () => {
  await save('new-a', { reorder_months: '4' });
  assert.equal((await expOf(EXA)).state, 'built', 'NE に送らない欄はファイルを閉じない');
  const r = await save('new-a', { name: '新しい単品 改' });
  assert.ok(r.warnings.some((w) => new RegExp(`#${EXA}`).test(w)), JSON.stringify(r.warnings));
  assert.deepEqual([(await expOf(EXA)).state, (await expOf(EXA)).close_reason], ['closed', 'superseded']);
  assert.deepEqual((await itemsOf(EXA)).map((x) => x.state), ['superseded', 'superseded']);
  await rejectsWith(issue(EXA), 409, 'closed');
  await save('new-a', { name: '新しい単品' });
});

let EXB = null; let SHAB = null;
await ta('[C5] 配る = built → issued・byte 列はその後だけ・配った後は NE に送る欄 (名前・JAN・原価) を直せない (409)・送らない欄は直せる', async () => {
  const r = await build('products', ['new-a']);
  EXB = r.export.export_id; SHAB = r.export.sha256;
  assert.equal((await G.regExportFile(db, EXB)).bytes, null, 'built は配らない');
  const x = await issue(EXB);
  assert.equal(x.export.state, 'issued');
  assert.equal((await issue(EXB)).already, true);
  const f = await G.regExportFile(db, EXB);
  assert.equal(crypto.createHash('sha256').update(f.bytes).digest('hex'), SHAB);
  assert.match(f.file_name, /^ne_register_products_\d{8}_\d{6}_\d+_trial\.csv$/);
  assert.deepEqual((await itemsOf(EXB)).map((i) => i.state), ['issued']);
  for (const v of [{ name: '直したい' }, { jan: J2 }, { cost: { jpy: '999', reason: '改定' } }, { tax_rate: '8' }]) {
    const e = await rejectsWith(save('new-a', v), 409, 'reg_csv_issued');
    assert.match(e.message, new RegExp(`#${EXB}`));
  }
  // JAN の関数も自分で拒む (lib を通さずに呼んでも・#1571 R1 High 3)
  const aId = await skuId('new-a');
  await pgErr(as(E, 'master_edit', () => pg.query("select ops.edit_sku_jan(gen_random_uuid(), 'naka@test', null, $1::jsonb, $2::bigint, $3::jsonb, $4::jsonb)",
    [OWN, aId, JSON.stringify([J1]), JSON.stringify([J2])])), /reg_csv_issued/);
  const ok2 = await save('new-a', { reorder_months: '5' });
  assert.equal(ok2.ok, true);
});

await ta('[C6] 申告: sha256 が違う = 409・誰・いつ・結果・NE のメッセージを残す・ok = import_declared + 登録 draft → ne_pending (根拠 = ファイルと sha256)・もう一度 = 試みを足すだけ', async () => {
  await rejectsWith(declare(EXB, 'f'.repeat(64)), 409, 'sha256_mismatch');
  await rejectsWith(declare(EXB, 'xyz'), 400);
  const r = await declare(EXB, SHAB, 'ok');
  assert.deepEqual([r.state, r.ne_pending], ['declared', ['new-a']]);
  assert.equal(await regOf('new-a'), 'ne_pending');
  const ev = await one("select e.evidence, e.actor_id from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = 'new-a' and e.to_state = 'ne_pending'");
  // 根拠は DB の関数がこの申告の記録 (ファイル・品目・試み・sha256) から自分で書く
  const att = await one('select attempt_id::text as id from ops.ne_reg_attempts where export_id = $1', [EXB]);
  assert.deepEqual([String(ev.evidence.export_id), ev.evidence.sha256, String(ev.evidence.attempt_id), ev.evidence.result, ev.evidence.declared_by, ev.actor_id],
    [String(EXB), SHAB, att.id, 'ok', 'boss@test', 'boss@test']);
  assert.deepEqual((await itemsOf(EXB)).map((i) => i.state), ['import_declared']);
  const a = await q('select declared_by, result, ne_message, sha256 from ops.ne_reg_attempts where export_id = $1', [EXB]);
  assert.deepEqual(a.map((x) => [x.declared_by, x.result, x.ne_message, x.sha256]), [['boss@test', 'ok', '1件成功しました。', SHAB]]);
  const r2 = await declare(EXB, SHAB, 'partial', { msg: '1件成功、1件失敗しました。' });
  assert.equal(r2.again, true);
  assert.equal((await q('select 1 from ops.ne_reg_attempts where export_id = $1', [EXB])).length, 2);
  await pgErr(pg.query('delete from ops.ne_reg_attempts'), /append-only/);
  await pgErr(pg.query(`insert into ops.ne_reg_attempts (export_id, sha256, declared_by, result) values ($1, $2, 'x', 'ok')`, [EXB, 'e'.repeat(64)]), /sha256_mismatch/);
});

await ta('[C7] 翌朝の確かめ (単品): 申告の前の取得 = 待ち・違う列 = partial (ne_pending のまま)・全部合う = verified + ne_confirmed・同じ回をもう一度 = 何もしない', async () => {
  const r1 = await newRun();
  let r = await check(r1, [obsSingle('new-a')], { productsAt: new Date(Date.now() - 3600000).toISOString() });
  assert.deepEqual(r.counts, { waiting: 1 });
  const r2 = await newRun();
  r = await check(r2, [obsSingle('new-a', { price: ok(1480) })]);
  assert.deepEqual(r.counts, { partial: 1 });
  assert.deepEqual([(await itemsOf(EXB))[0].state, await regOf('new-a')], ['partial', 'ne_pending']);
  const detail = (await one('select detail from ops.ne_reg_checks where compare_run_id = $1', [r2])).detail;
  assert.equal(detail.compare.cols.price.ok, false);
  assert.deepEqual(detail.compare.not_compared, ['jan']);
  const r3 = await newRun();
  r = await check(r3, [obsSingle('new-a', { tax_rate: { st: 'invalid', v: null } })]);
  assert.deepEqual(r.counts, { partial: 1 }, '比べられない列は一致にしない');
  const r4 = await newRun();
  r = await check(r4, [obsSingle('new-a')]);
  assert.deepEqual(r.counts, { verified: 1 });
  assert.deepEqual([(await itemsOf(EXB))[0].state, await regOf('new-a')], ['verified', 'ne_confirmed']);
  const ev = await one("select e.evidence, e.actor_type from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = 'new-a' and e.to_state = 'ne_confirmed'");
  assert.deepEqual([ev.evidence.compare_run_id, ev.evidence.matched, ev.actor_type], [r4, true, 'system']);
  assert.deepEqual([(await expOf(EXB)).state, (await expOf(EXB)).close_reason], ['closed', 'finished']);
  r = await check(r4, [obsSingle('new-a')]);
  assert.deepEqual(r.counts, {});
  // watch_writer は表へ直接は書けない
  await as(E, 'watch_writer', () => pgErr(pg.query(`update ops.ne_reg_export_items set state = 'verified'`), /permission denied/));
  assert.equal((await save('new-a', { name: '確かめた後は直せる' })).ok, true, 'NE で確かめた後は新規登録の CSV では止めない (既にある商品の CSV = to_ne の道)');
  await save('new-a', { name: '新しい単品' });
});

await ta('[C8] 無い・信じられない・申告の前に NE にある: 申告の後の完全な取得に無い = failed (「無い」を信じてよいときだけ)・配っただけで NE にある = 記録だけ', async () => {
  const r = await build('products', ['new-b']);
  const id = r.export.export_id;
  await issue(id);
  const r1 = await newRun();
  let c = await check(r1, [obsSingle('new-b', { name: ok('新しい単品 B') })]);
  assert.deepEqual(c.counts, { in_ne_undeclared: 1 });
  assert.deepEqual((await itemsOf(id)).map((i) => i.state), ['issued']);
  await declare(id, r.export.sha256, 'ok');
  const r2 = await newRun();
  c = await check(r2, [{ code_norm: 'new-b', present: false, trusted: true, kind: null }], { absenceTrusted: false });
  assert.deepEqual(c.counts, { waiting: 1 });
  const r3 = await newRun();
  c = await check(r3, [{ code_norm: 'new-b', present: true, trusted: false, kind: 'single', cols: {} }]);
  assert.deepEqual(c.counts, { waiting: 1 });
  const r4 = await newRun();
  c = await check(r4, [{ code_norm: 'new-b', present: false, trusted: true, kind: null }]);
  assert.deepEqual(c.counts, { failed: 1 });
  assert.deepEqual((await itemsOf(id)).map((i) => [i.state, i.failed_reason]), [['failed', 'not_in_ne']]);
  assert.equal(await regOf('new-b'), 'ne_pending', '取り込めなかった = NE 登録待ちのまま');
  const again = await build('products', ['new-b']);
  assert.ok(again.export.export_id > id, '取り込めなかった商品は作り直せる');
  await supersede(again.export.export_id);
});

await ta('[C9] セット: 構成品が全部 NE 確認済み・構成品 1 つで 1 行 (並び = 依頼のとおり)・同じ取得の構成品が全部合うときだけ verified', async () => {
  const r = await build('sets', ['new-set']);
  const id = r.export.export_id;
  const f = Buffer.from((await one('select file_bytes from ops.ne_reg_exports where export_id = $1', [id])).file_bytes).toString('utf8');
  assert.equal(f, 'set_syohin_code,set_syohin_name,set_baika_tnk,tax_rate,syohin_code,suryo\r\nnew-set,新しいセット,2500,8,s001,1\r\nnew-set,新しいセット,2500,8,s002,2\r\n');
  await issue(id); await declare(id, r.export.sha256, 'ok');
  assert.equal(await regOf('new-set'), 'ne_pending');
  const obsSet = (children) => ({ code_norm: 'new-set', present: true, trusted: true, kind: 'set', cols: { name: ok('新しいセット'), price: ok(2500) }, children });
  const r1 = await newRun();
  let c = await check(r1, [obsSet([{ code_norm: 's001', st: 'ok', v: 1 }])]);
  assert.deepEqual(c.counts, { partial: 1 }, '構成品が足りない = partial');
  const r2 = await newRun();
  c = await check(r2, [obsSet([{ code_norm: 's001', st: 'ok', v: 1 }, { code_norm: 's002', st: 'ok', v: 3 }])]);
  assert.deepEqual(c.counts, { partial: 1 }, '数量が違う = partial');
  const r3 = await newRun();
  c = await check(r3, [obsSet([{ code_norm: 's002', st: 'ok', v: 2 }, { code_norm: 's001', st: 'ok', v: 1 }])]);
  assert.deepEqual(c.counts, { verified: 1 });
  assert.equal(await regOf('new-set'), 'ne_confirmed');
});

await ta('[C10] 使わない: 理由・NE で何をしたか・確かめた の印が要る → 生きている商品は superseded・ファイルは閉じる・登録の状態は変えない・直せるようになる / 全部だめ = failed', async () => {
  await reg('single', 'new-c', single({ name: 'C' }));
  const r = await build('products', ['new-c']);
  const id = r.export.export_id;
  await issue(id); await declare(id, r.export.sha256, 'ok');
  await rejectsWith(supersede(id, { reason: '' }), 400);
  await rejectsWith(supersede(id, { correction: '' }), 400);
  await rejectsWith(supersede(id, { confirm: false }), 400);
  await rejectsWith(save('new-c', { name: 'C 改' }), 409, 'reg_csv_issued');
  const s = await supersede(id, { reason: '売価を間違えた', correction: 'NE で取り込んだ商品を消した' });
  assert.equal(s.superseded, 1);
  const it = await one('select state, superseded_reason, superseded_correction from ops.ne_reg_export_items where export_id = $1', [id]);
  assert.deepEqual([it.state, it.superseded_reason, it.superseded_correction], ['superseded', '売価を間違えた', 'NE で取り込んだ商品を消した']);
  assert.equal(await regOf('new-c'), 'ne_pending');
  assert.equal((await save('new-c', { name: 'C 改' })).ok, true);
  await rejectsWith(supersede(id), 409, 'closed');
  await rejectsWith(declare(id, r.export.sha256), 409, 'closed');
  // 全部だめ
  await reg('single', 'new-d', single({ name: 'D' }));
  const d = await build('products', ['new-d']);
  await issue(d.export.export_id);
  const x = await declare(d.export.export_id, d.export.sha256, 'rejected_all', { msg: '0件成功、1件失敗しました。' });
  assert.equal(x.state, 'closed');
  assert.deepEqual((await itemsOf(d.export.export_id)).map((i) => [i.state, i.failed_reason]), [['failed', 'rejected_all']]);
  assert.equal(await regOf('new-d'), 'draft');
});

await ta('[C11] 実機の確かめの門: 試し用は 5 行まで・ok は試しのファイルの商品が全部 NE で確かめ済みのときだけ・ok の後は本番 (試し用でない)・ng で戻る', async () => {
  await reg('set', 'new-big', { name: '大きいセット', standard_price: '9000', shipping_code: 'S02',
    components: ['s001', 's002', 's003', 's004', 's005', 's006'].map((c) => ({ code: c, qty: 1 })) });
  const e = await rejectsWith(build('sets', ['new-big']), 409, 'trial_limit');
  assert.equal(e.extra.rows, 6);
  const setExp = (await one("select export_id::text as id from ops.ne_reg_exports where kind = 'sets' and state = 'closed' and close_reason = 'finished'")).id;
  const notDone = (await one("select export_id::text as id from ops.ne_reg_exports where kind = 'products' and close_reason = 'superseded' order by export_id desc limit 1")).id;
  await rejectsWith(as(E, 'master_edit', () => G.recordRegVerified(db, { actor: 'boss@test', kind: 'sets', result: 'ok' }, opts())), 400);
  await rejectsWith(as(E, 'master_edit', () => G.recordRegVerified(db, { actor: 'boss@test', kind: 'sets', result: 'ok', exportId: notDone }, opts())), 409, 'export_mismatch');
  await rejectsWith(as(E, 'master_edit', () => G.recordRegVerified(db, { actor: 'boss@test', kind: 'products', result: 'ok', exportId: notDone }, opts())), 409, 'not_verified');
  await as(E, 'master_edit', () => G.recordRegVerified(db, { actor: 'boss@test', kind: 'sets', result: 'ok', exportId: setExp, note: '中原さんと実機で' }, opts()));
  assert.equal(await G.regFormatVerified(db, G.REG_SCHEMAS.sets), true);
  assert.equal(await G.regFormatVerified(db, G.REG_SCHEMAS.products), false, '種類ごと');
  const big = await build('sets', ['new-big']);
  assert.equal((await expOf(big.export.export_id)).trial, false);
  await supersede(big.export.export_id);
  await as(E, 'master_edit', () => G.recordRegVerified(db, { actor: 'boss@test', kind: 'sets', result: 'ng', note: '取込でおかしかった' }, opts()));
  assert.equal(await G.regFormatVerified(db, G.REG_SCHEMAS.sets), false);
  const v = await one("select converter_version, col, reg_export_id::text as x from ops.ne_csv_verified where kind = 'sets' and result = 'ok'");
  assert.deepEqual([v.converter_version, v.col, v.x], ['ne-reg-set-v1', 'new_registration', setExp]);
});

await ta('[C12] DB の守り: 中身は変えない・消さない・一方向・生きているファイルは SKU ごとに 1 つ', async () => {
  const id = (await one('select export_id::text as id from ops.ne_reg_exports order by export_id limit 1')).id;
  await pgErr(pg.query('update ops.ne_reg_exports set sha256 = $2 where export_id = $1', [id, 'c'.repeat(64)]), /中身は書き換えない/);
  await pgErr(pg.query('delete from ops.ne_reg_exports where export_id = $1', [id]), /消さない/);
  await pgErr(pg.query(`update ops.ne_reg_export_items set expected = '{}'::jsonb where export_id = $1`, [id]), /中身は書き換えない/);
  await pgErr(pg.query(`update ops.ne_reg_export_items set state = 'built' where state = 'verified'`), /終わった商品/);
  await pgErr(pg.query('delete from ops.ne_reg_export_rows'), /append-only/);
  await reg('single', 'new-e', single({ name: 'E' }));
  const r = await build('products', ['new-e']);
  await pgErr(pg.query(`update ops.ne_reg_export_items set state = 'verified', verified_run = 'x', verified_at = now() where export_id = $1`, [r.export.export_id]), /進めない/);
  const it = await one('select * from ops.ne_reg_export_items where export_id = $1', [r.export.export_id]);
  await pgErr(pg.query(`insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by)
    select export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, 'x' from ops.ne_reg_export_items where item_id = $1`, [it.item_id]), /duplicate key|ux_ne_reg_items_live|unique/);
  await supersede(r.export.export_id);
});

await ta('[C13] 配る前に登録をやめた商品がある = 配らない (残りの商品も使わない・ファイルを閉じる = 作り直せる)・単品の税率を変える = それを依頼に入れたセットの配る前の CSV も使わない', async () => {
  await reg('single', 'new-f', single({ name: 'F' }));
  await reg('single', 'new-g', single({ name: 'G' }));
  const r = await build('products', ['new-f', 'new-g']);
  await pg.query('select ops.transition_sku_registration($1, $2, $3, $4, $5)', [await skuId('new-f'), 'cancelled', 'human', 'naka@test', 'やめる']);
  const x = await issue(r.export.export_id);
  assert.deepEqual([x.refused, x.codes], [true, ['new-f']]);
  assert.deepEqual((await itemsOf(r.export.export_id)).map((i) => i.state), ['superseded', 'superseded']);
  assert.equal((await build('products', ['new-g'])).export.state, 'built', '残りの商品は作り直せる');
  // 依頼だけの新しいセット (構成品 = 既にある単品 s007) の配る前の CSV は、s007 の税率を変えると使わないになる
  await reg('set', 'new-set-h', { name: 'H セット', standard_price: '1800', shipping_code: 'S02', components: [{ code: 's007', qty: 2 }] });
  const s = await build('sets', ['new-set-h']);
  const w = await save('s007', { tax_rate: '8' });
  assert.ok(w.warnings.some((m) => m.includes(`#${s.export.export_id}`)), JSON.stringify(w.warnings));
  assert.equal((await expOf(s.export.export_id)).state, 'closed');
  await save('s007', { tax_rate: '10' });
});

await ta('[C14] 権限の境界: 画面のロールは表を直接書けない・状態の関数を呼べない / 関数は呼び手の根拠を受けない・行から byte 列・確かめる値・NE の元のコードの回・状態を照らし直す', async () => {
  const denied = async (sql, params = []) => { const e = await as(E, 'master_edit', () => pgErr(pg.query(sql, params))); assert.equal(e.code, '42501', `${sql}: ${e.message}`); };
  // 1. 表の直接の書き込み = 権限なし (書くのは関数だけ)
  for (const sql of [
    `insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, created_by)
       values ('products', 'ne-reg-single-v1', 'a', 'utf8', true, 1, 1, repeat('a', 64), repeat('a', 64), repeat('a', 64), decode('00', 'hex'), gen_random_uuid(), 'x', 'x')`,
    "update ops.ne_reg_export_items set state = 'verified'", "update ops.ne_reg_exports set state = 'closed'",
    "insert into ops.ne_reg_attempts (export_id, sha256, declared_by, result) values (1, repeat('a', 64), 'x', 'ok')", 'delete from ops.ne_reg_checks',
    "insert into ops.ne_csv_verified (kind, col, encoding, header, converter_version, result, verified_by) values ('products', 'new_registration', 'utf8', 'a', 'ne-reg-single-v1', 'ok', 'x')",
    "update ops.supplier_registrations set state = 'ne_confirmed'", "update ops.master_registrations set state = 'ne_confirmed'",
  ]) await denied(sql);
  // 2. 状態を進める関数・照合の関数・仕入先の状態の関数は画面のロールに無い
  await denied("select ops.transition_sku_registration(1, 'ne_pending', 'human', 'x', null, '{}'::jsonb, null)");
  await denied("select ops.record_ne_registration_check('mc_20300110T000000000Z_aaaaaa')");
  await denied("select ops.record_ne_registration_observations('{}'::jsonb)");
  await denied("select ops.seal_ne_registration_run('mc_20300110T000000000Z_aaaaaa', repeat('a', 64), repeat('b', 64))");
  await denied(`select ops.declare_supplier_in_ne(gen_random_uuid(), 'x', '${OWN}'::jsonb, '0001', '0001', null)`);
  // 3. 状態の関数は呼び手の根拠を受けない (持ち主のロールでも)・NE 登録待ちの根拠 = 取り込んだと申告した品目だけ
  await reg('single', 'new-k', single({ name: 'K' }));
  const kid = await skuId('new-k');
  const tr = (to, type, ev = {}, reason = null) => pg.query('select ops.transition_sku_registration($1, $2, $3, $4, $5, $6::jsonb, null)', [kid, to, type, 'naka@test', reason, JSON.stringify(ev)]);
  await pgErr(tr('ne_pending', 'human', { export_id: '1', sha256: 'a'.repeat(64), attempt_id: '1' }), /caller_evidence/);
  await pgErr(tr('ne_pending', 'human'), /no_evidence/);
  const k = await build('products', ['new-k']);
  await pgErr(tr('ne_pending', 'human'), /no_evidence/);
  await issue(k.export.export_id);
  await pgErr(tr('ne_pending', 'human'), /no_evidence/);
  await declare(k.export.export_id, k.export.sha256);
  assert.equal(await regOf('new-k'), 'ne_pending');
  // 4. NE 確認済みの根拠 = 照合の確かめ (verified) だけ。partial・人・呼び手の根拠では進まない
  await pgErr(tr('ne_confirmed', 'system'), /no_evidence/);
  await pgErr(tr('ne_confirmed', 'system', { compare_run_id: RUN1, matched: true }), /caller_evidence/);
  const rp = await newRun();
  assert.deepEqual((await check(rp, [obsSingle('new-k', { name: ok('K'), price: ok(1) })])).counts, { partial: 1 });
  await pgErr(tr('ne_confirmed', 'system'), /no_evidence/);
  await pgErr(tr('ne_confirmed', 'human', {}, '見た'), /no_evidence/);
  const rv = await newRun();
  assert.deepEqual((await check(rv, [obsSingle('new-k', { name: ok('K') })])).counts, { verified: 1 });
  assert.equal(await regOf('new-k'), 'ne_confirmed');
  await pgErr(tr('distributable', 'system'), /not_ready/);
  // 5. 作る関数は自分で照らし直す (画面のロールで lib を通さずに呼ぶ)
  await reg('single', 'new-m', single({ name: 'M' }));
  const mid = await skuId('new-m');
  const cells = ['new-m', 'M', '0001', '300', '1500', '10', '0', 'empty', 'empty'];
  const header = G.REG_SCHEMAS.products.header.join(',');
  const expected = { kind: 'single', values: { name: 'M', supplier: '0001', cost: 300, price: 1500, tax_rate: 0.1, handling: 'active', parent: null } };
  const mark = (await one('select compare_run_id from ops.master_ne_code_mark')).compare_run_id;
  const item = (over = {}) => ({ sku_id: mid, expected, rows: [cells], ...over });
  const pay = (over = {}) => JSON.stringify({ request_id: uuid(), actor: 'boss@test', ownership: ALL_COMPANY, kind: 'products', schema_version: 'ne-reg-single-v1', header,
    ne_codes_run: mark, cost_day: TODAY, items: [item()], ...over });
  const bytesOf = (rows) => G.buildRegCsv(G.REG_SCHEMAS.products, rows).bytes;
  const callBuild = (payload, bytes = bytesOf([cells])) => inSession(() => pg.query('select ops.ne_reg_build($1::jsonb, $2::bytea) as r', [payload, bytes]));
  const kRow = ['new-k', ...cells.slice(1)];
  // 偽造 (#1571 Codex R1 High 1): 行・確かめる値・byte 列をそろえて偽っても、関数が Company DB の今の値から作った行と違えば拒む (翌朝の照合で偽の値が NE と合っても確かめにならない)
  const forge = async (cellIdx, cellVal, valKey, val) => {
    const fc = cells.map((x, i) => (i === cellIdx ? cellVal : x));
    const fe = { ...expected, values: { ...expected.values, [valKey]: val } };
    await pgErr(callBuild(pay({ items: [item({ expected: fe, rows: [fc] })] }), bytesOf([fc])), /not_canonical/);
  };
  await forge(4, '1400', 'price', 1400);           // 売価
  await forge(3, '1', 'cost', 1);                   // 原価
  await forge(2, '0002', 'supplier', '0002');       // 代表の仕入先
  await forge(1, '偽の名前', 'name', '偽の名前');     // 名前
  await forge(5, '8', 'tax_rate', 0.08);            // 税率
  await forge(7, 'grp1', 'parent', 'grp1');         // 代表 (親)
  await forge(8, J2, 'handling', 'active');         // JAN (行だけ偽る)
  await pgErr(callBuild(pay({ items: [item({ expected: { ...expected, values: { ...expected.values, price: 1400 } } })] })), /not_canonical/);   // 確かめる値だけ
  await pgErr(callBuild(pay({ items: [item({ rows: [kRow] })] }), bytesOf([kRow])), /not_canonical/);   // 先頭のセル (ほかの商品のコード)
  // 印とハッシュは関数が計算する = 送れば拒む (固定の値で通らない)
  await pgErr(callBuild(JSON.stringify({ ...JSON.parse(pay()), aggregate_token: 'a'.repeat(64), payload_hash: 'b'.repeat(64) })), /caller_hash/);
  await pgErr(callBuild(pay({ items: [item({ item_token: 'c'.repeat(64), snapshot_hash: 'd'.repeat(64) })] })), /caller_hash/);
  // 原価を見る日: 無い・東京の今日より前 = 拒む
  await pgErr(callBuild(pay({ cost_day: null })), /cost_day/);
  await pgErr(callBuild(pay({ cost_day: '2020-01-01' })), /cost_day/);
  await pgErr(callBuild(pay(), bytesOf([[...cells.slice(0, 4), '1400', ...cells.slice(5)]])), /bytes_mismatch/);
  await pgErr(callBuild(pay({ ne_codes_run: RUN1 })), /ne_codes_stale/);
  await pgErr(callBuild(pay({ items: [item({ sku_id: kid, rows: [kRow] })] }), bytesOf([kRow])), /not_ready/);
  await pgErr(callBuild(pay({ header: `${header},zaiko_su` })), /在庫の列/);
  // 名前に改行 (Company DB の値) = 関数の canonical が止める (CSV に書けない)
  const name0 = (await one('select name from core.skus where sku_id = $1', [mid])).name;
  await pg.query('update core.skus set name = $2 where sku_id = $1', [mid, `M${String.fromCharCode(10)}X`]);
  await pgErr(callBuild(pay()), /名前を CSV に書けない/);
  await pg.query('update core.skus set name = $2 where sku_id = $1', [mid, name0]);
  const built = (await callBuild(pay())).rows[0].r;
  assert.deepEqual([built.state, built.trial, built.sha256], ['built', true, crypto.createHash('sha256').update(bytesOf([cells])).digest('hex')]);
  const declareSql = "select ops.ne_reg_declare(gen_random_uuid(), 'boss@test', $3::jsonb, $1::bigint, $2, 'ok', null, null, null)";
  await pgErr(inSession(() => pg.query(declareSql, [built.export_id, built.sha256, OWN])), /not_issued/);
  await pgErr(inSession(() => pg.query(declareSql, [built.export_id, 'e'.repeat(64), OWN])), /sha256_mismatch/);
  await pgErr(inSession(() => pg.query("select ops.ne_reg_record_verified(gen_random_uuid(), 'boss@test', $3::jsonb, 'products', 'ne-reg-single-v1', $1, 'ok', null, $2::bigint)",
    [header, built.export_id, OWN])), /not_verified/);
  // 持ち主表が段階の記録と違う・切替の前の形 = 関数の門が拒む (lib を通さずに呼んでも)
  await pgErr(inSession(() => pg.query(declareSql, [built.export_id, built.sha256, JSON.stringify(MASTER_OWNERSHIP)])), /before_cutover/);
  await pgErr(callBuild(pay({ ownership: MASTER_OWNERSHIP })), /before_cutover/);
  await supersede(String(built.export_id));
  // NE にもうあるコード = 関数も止める (lib の確かめを通さずに呼んでも)
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ('mc_20300110T020000000Z_dddddd', '2030-01-10T02:00:00Z', 0)`);
  await recordNeCodes(pg, 'mc_20300110T020000000Z_dddddd', ['new-dup', 'new-m']);
  await pgErr(callBuild(pay({ ne_codes_run: 'mc_20300110T020000000Z_dddddd' })), /already_in_ne/);
  // 6. 関数の形: security definer・search_path = pg_catalog, pg_temp・public の実行権なし
  const want = ['close_reg_write', 'create_supplier', 'deactivate_supplier', 'declare_supplier_in_ne', 'edit_sku_jan', 'guard_reg_csv_live', 'guard_reg_csv_write',
    'ne_reg_build', 'ne_reg_canonical', 'ne_reg_declare', 'ne_reg_guard_on_save', 'ne_reg_issue', 'ne_reg_lock_export', 'ne_reg_lock_skus', 'ne_reg_ne_codes',
    'ne_reg_record_verified', 'ne_reg_supersede', 'ne_reg_supersede_built', 'open_reg_write', 'record_ne_registration_check', 'record_ne_registration_observations',
    'reg_write_gate', 'seal_ne_registration_run', 'transition_sku_registration'];
  const fns = await q(`select p.proname, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub
     from pg_proc p join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'ops' and p.prosecdef and (p.proname like 'ne!_reg!_%' escape '!' or p.proname = any($1::text[])) order by p.proname`, [want]);
  assert.deepEqual(fns.map((f) => f.proname), want);
  for (const f of fns) assert.deepEqual([f.proname, f.c, f.pub], [f.proname, 'search_path=pg_catalog, pg_temp', false]);
  const jg = await one(`select array_to_string(p.proconfig, ',') as c, p.prosecdef as d, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p
     join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'core' and p.proname = 'guard_master_edit_jan'`);
  assert.deepEqual([jg.c, jg.d, jg.pub], ['search_path=pg_catalog, pg_temp', true, false]);
});

await ta('[C15] 作る関数の門 (lib を通さずに呼んでも): 実機で確かめていない形は 5 行まで (trial_limit)・切替の前 (new_open でない) は作る / 配る / 申告 / 使わないを拒む', async () => {
  // 単品 6 つを 1 つのファイルに (単品の形はまだ実機で確かめていない = 試し用)
  const codes = ['new-t1', 'new-t2', 'new-t3', 'new-t4', 'new-t5', 'new-t6'];
  for (const c of codes) await reg('single', c, single({ name: `T ${c}` }));
  const mats = [];
  for (const c of codes) mats.push(await G.regMaterialOf(db, await skuId(c), { today: TODAY, nc: ncOf(), live: new Map(), regByIds: new Map(), supRegByIds: new Map() }));
  for (const m of mats) assert.deepEqual(m.blockers, []);
  const rows = mats.map((m) => m.cells[0].map(String));
  const mark = (await one('select compare_run_id from ops.master_ne_code_mark')).compare_run_id;
  const payload = JSON.stringify({ request_id: uuid(), actor: 'boss@test', ownership: ALL_COMPANY, kind: 'products', schema_version: 'ne-reg-single-v1',
    header: G.REG_SCHEMAS.products.header.join(','), ne_codes_run: mark, cost_day: TODAY,
    items: mats.map((m, i) => ({ sku_id: m.cur.sku_id, expected: m.expected, rows: [rows[i]] })) });
  await pgErr(inSession(() => pg.query('select ops.ne_reg_build($1::jsonb, $2::bytea) as r', [payload, G.buildRegCsv(G.REG_SCHEMAS.products, rows).bytes])), /trial_limit/);
  // 切替の前の DB (段階 legacy_open) = 関数が拒む
  const p0 = new PGlite();
  try {
    const d0 = pgliteAdapter(p0);
    await applyMigrations(d0, { log: quiet });
    for (const sql of ["select ops.ne_reg_issue(gen_random_uuid(), 'x', '{}'::jsonb, 1)", "select ops.ne_reg_supersede(gen_random_uuid(), 'x', '{}'::jsonb, 1, '理由', '直し方')",
      `select ops.ne_reg_declare(gen_random_uuid(), 'x', '{}'::jsonb, 1, '${'a'.repeat(64)}', 'ok', null, null, null)`,
      "select ops.create_supplier(gen_random_uuid(), 'x', null, '{}'::jsonb, '0050', '五十商事', null, null)",
      "select ops.edit_sku_jan(gen_random_uuid(), 'x', null, '{}'::jsonb, 1, '[]'::jsonb, '[]'::jsonb)"]) {
      await pgErr(p0.query(sql), /before_cutover/);
    }
  } finally { await p0.close(); }
  // 段階が new_open でない (company_owner・持ち主表のハッシュは同じ) = 関数の門が拒む (持ち主が取引の中だけ段階を戻して巻き戻す)
  const s6 = await skuId('s006');
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.cutover_protocol', '1', true)");
    await pg.query("update ops.master_cutover_state set phase = 'company_owner' where id = 1");
    for (const [sql, params] of [["select ops.edit_sku_jan(gen_random_uuid(), 'naka@test', null, $1::jsonb, $2::bigint, '[]'::jsonb, '[]'::jsonb)", [OWN, s6]],
      ["select ops.create_supplier(gen_random_uuid(), 'po@test', null, $1::jsonb, '0051', '五十一商事', null, null)", [OWN]],
      ["select ops.ne_reg_record_verified(gen_random_uuid(), 'boss@test', $1::jsonb, 'products', 'ne-reg-single-v1', $2, 'ng', null, null)", [OWN, G.REG_SCHEMAS.products.header.join(',')]]]) {
      await pg.query('savepoint t');
      await pgErr(pg.query(sql, params), /before_cutover/);
      await pg.query('rollback to savepoint t');
    }
  } finally { await pg.query('rollback'); }
  assert.equal((await one('select phase from ops.master_cutover_state where id = 1')).phase, 'new_open');
});

await ta('[C16] 状態の関数の根拠の照らし直し (持ち主が状態を戻した・記録を直接作った場合も): 人 / system の取り違え・使わないにした品目・全部だめの試みは根拠にならない', async () => {
  const forceState = async (code, state) => {
    const id = await skuId(code);
    await pg.query('begin');
    try {
      await pg.query(`select set_config('ops.registration_protocol', '1', true)`);
      await pg.query('update ops.master_registrations set state = $2 where sku_id = $1', [id, state]);
      await pg.query('commit');
    } catch (e) { await pg.query('rollback'); throw e; }
  };
  const tr = async (code, to, type) => pg.query("select ops.transition_sku_registration($1, $2, $3, 'naka@test', null, '{}'::jsonb, null)", [await skuId(code), to, type]);
  // 1. 申告した品目は、人の申告でだけ NE 登録待ちの根拠 (system では進めない)
  await reg('single', 'new-p', single({ name: 'P' }));
  const p = await build('products', ['new-p']);
  await issue(p.export.export_id);
  await declare(p.export.export_id, p.export.sha256);
  assert.equal(await regOf('new-p'), 'ne_pending');
  await forceState('new-p', 'draft');
  await pgErr(tr('new-p', 'ne_pending', 'system'), /no_evidence/);
  await tr('new-p', 'ne_pending', 'human');
  assert.equal(await regOf('new-p'), 'ne_pending');
  // 2. NE 確認済みは照合 (system) でだけ: verified の確かめがあっても人では進めない
  const rv = await newRun();
  assert.deepEqual((await check(rv, [obsSingle('new-p', { name: ok('P') })])).counts, { verified: 1 });
  await forceState('new-p', 'ne_pending');
  await pgErr(tr('new-p', 'ne_confirmed', 'human'), /no_evidence/);
  await tr('new-p', 'ne_confirmed', 'system');
  assert.equal(await regOf('new-p'), 'ne_confirmed');
  // 3. 申告の後に使わないにした品目は根拠にならない
  await reg('single', 'new-q', single({ name: 'Q' }));
  const qx = await build('products', ['new-q']);
  await issue(qx.export.export_id);
  await declare(qx.export.export_id, qx.export.sha256);
  await supersede(qx.export.export_id);
  await forceState('new-q', 'draft');
  await pgErr(tr('new-q', 'ne_pending', 'human'), /no_evidence/);
  // 4. 全部だめの試みに結んだ品目 (持ち主が直接作った記録) は根拠にならない
  await reg('single', 'new-r', single({ name: 'R' }));
  const rx = await build('products', ['new-r']);
  await issue(rx.export.export_id);
  const att = (await one("insert into ops.ne_reg_attempts (export_id, sha256, declared_by, result) values ($1, $2, 't', 'rejected_all') returning attempt_id::text as id", [rx.export.export_id, rx.export.sha256])).id;
  await pg.query("update ops.ne_reg_export_items set state = 'import_declared', attempt_id = $2 where export_id = $1", [rx.export.export_id, att]);
  await pg.query("update ops.ne_reg_exports set state = 'declared', declared_at = now(), declared_by = 't' where export_id = $1", [rx.export.export_id]);
  await pgErr(tr('new-r', 'ne_pending', 'human'), /no_evidence/);
  await supersede(rx.export.export_id);
  // 5. セットの構成品が NE 確認済みでない = 作る関数も止める (NE の元のコードにはあっても・lib の確かめを通さずに呼んでも)
  await reg('single', 'new-s1', single({ name: 'S1' }));
  await reg('set', 'new-sd', { name: '下書きの構成品のセット', standard_price: '2000', shipping_code: 'S02', components: [{ code: 'new-s1', qty: 1 }], set_sales_class_override: '3' });
  const run5 = 'mc_20300110T030000000Z_eeeeee';
  await pg.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T03:00:00Z', 0)`, [run5]);
  await recordNeCodes(pg, run5, ['new-dup', 'new-m', 'new-s1']);
  const m = await G.regMaterialOf(db, await skuId('new-sd'), { today: TODAY, nc: ncOf(new Map([['product|new-s1', { state: 'ok', ne_code: 'new-s1' }]])), live: new Map(),
    regByIds: new Map(), supRegByIds: new Map() });
  assert.ok(m.blockers.some((b) => /new-s1 が NE 確認済みでない/.test(b)), JSON.stringify(m.blockers));
  const rows = m.cells.map((r) => r.map(String));
  assert.deepEqual(rows, [['new-sd', '下書きの構成品のセット', '2000', '10', 'new-s1', '1']]);
  const pay = JSON.stringify({ request_id: uuid(), actor: 'boss@test', ownership: ALL_COMPANY, kind: 'sets', schema_version: 'ne-reg-set-v1', header: G.REG_SCHEMAS.sets.header.join(','),
    ne_codes_run: run5, cost_day: TODAY, items: [{ sku_id: m.cur.sku_id, expected: m.expected, rows }] });
  await pgErr(inSession(() => pg.query('select ops.ne_reg_build($1::jsonb, $2::bytea) as r', [pay, G.buildRegCsv(G.REG_SCHEMAS.sets, rows).bytes])), /new-s1 が NE 確認済みでない/);
});

await ta('[C17] 照合の確かめ = 受け取りのある回の残した観測だけ (#1571 Codex R1 High 2): 観測の後に落ちた回は確かめない・呼び手の JSON は受けない・受け取りの後に観測を足せない・知らないコード / 足りない観測 / 照合の回より後の取得 / 世代なし / 形・ハッシュ違い・同じ回の違う中身を拒む', async () => {
  await reg('single', 'new-v', single({ name: 'V' }));
  const vx = await build('products', ['new-v']);
  await issue(vx.export.export_id);
  await declare(vx.export.export_id, vx.export.sha256);
  assert.equal(await regOf('new-v'), 'ne_pending');
  // 1. 観測を残した後に落ちた回 (受け取りの前) = 確かめない (NE 確認済みにしない)
  const r1 = await newRun();
  const w1 = await observe(r1, [obsSingle('new-v', { name: ok('V') })]);
  assert.equal(w1.state, 'written');
  await pgErr(checkRun(r1), /not_sealed/);
  assert.deepEqual([await regOf('new-v'), (await itemsOf(vx.export.export_id))[0].state], ['ne_pending', 'import_declared']);
  // 2. 呼び手の観測の JSON を確かめの関数に渡す道は無い (前の形の関数は無い)
  await pgErr(as(E, 'watch_writer', () => pg.query('select ops.record_ne_registration_check($1::jsonb)', [JSON.stringify({ compare_run_id: r1, observations: [] })])), /does not exist|存在しません/);
  // 3. 受け取りの観測のハッシュが残した観測と違う = 拒む / 同じ回の違う観測 = 拒む・同じ = 何もしない
  await pgErr(seal(r1, 'f'.repeat(64)), /observation_hash_mismatch/);
  await pgErr(observe(r1, [obsSingle('new-v', { name: ok('偽') })]), /run_conflict/);
  assert.equal((await observe(r1, [obsSingle('new-v', { name: ok('V') })])).state, 'unchanged');
  // 4. 表は直接書けない (watch_writer)・受け取りの後は観測を足せない (持ち主のロールでも)・書き換えられない
  for (const sql of ["insert into ops.ne_reg_compare_receipts (compare_run_id, observation_hash, evidence_sha256) values ('x', repeat('a', 64), repeat('b', 64))",
    "insert into ops.ne_reg_compare_observations (compare_run_id, code_norm, observation) values ('x', 'y', '{}'::jsonb)"]) {
    const e = await as(E, 'watch_writer', () => pgErr(pg.query(sql)));
    assert.equal(e.code, '42501', sql);
  }
  assert.equal((await seal(r1, w1.observation_hash)).state, 'sealed');
  await pgErr(pg.query(`insert into ops.ne_reg_compare_observations (compare_run_id, code_norm, observation) values ($1, 'new-a', '{}'::jsonb)`, [r1]), /sealed_run/);
  await pgErr(pg.query(`update ops.ne_reg_compare_observations set observation = '{}'::jsonb where compare_run_id = $1`, [r1]), /append-only/);
  await pgErr(seal(r1, w1.observation_hash, 'd'.repeat(64)), /run_conflict/);
  // 5. 受け取りのある回 = 確かめる (verified + NE 確認済み)。確かめの記録に取得の世代・原本のハッシュ・結果の sha256
  assert.deepEqual((await checkRun(r1)).counts, { verified: 1 });
  assert.equal(await regOf('new-v'), 'ne_confirmed');
  const ck = await one("select c.detail from ops.ne_reg_checks c join core.skus s on s.sku_id = c.sku_id where s.code = 'new-v' and c.compare_run_id = $1", [r1]);
  assert.deepEqual([ck.detail.fetch_generation, ck.detail.evidence_sha256], [`gen_${r1}`, 'e'.repeat(64)]);
  // 6. 形: 知らないコード・確かめ待ちの商品の観測が足りない・照合の回より後の取得・取得の世代が無い・列の形 = 拒む (何も残さない)
  const r2 = await newRun();
  await pgErr(observe(r2, [obsSingle('zzz-none')]), /新規登録の CSV の商品でない/);
  await pgErr(observe(r2, [obsSingle('new-v')], { targets: ['new-v', 'new-a'] }), /ちょうど 1 つ/);
  await pgErr(observe(r2, [obsSingle('new-v')], { productsAt: '2031-01-01T00:00:00Z' }), /照合の回/);
  await pgErr(observe(r2, [obsSingle('new-v')], { fetch: { generation_id: 'g' } }), /fetch/);
  await pgErr(observe(r2, [{ ...obsSingle('new-v'), cols: { name: { st: 'yes', v: 'V' } } }]), /観測の形/);
  await pgErr(observe(r2, [{ ...obsSingle('new-v'), present: 'yes' }]), /観測の形/);
  assert.equal(Number((await one('select count(*)::int as n from ops.ne_reg_compare_runs where compare_run_id = $1', [r2])).n), 0);
  // 照合の回の記録が無い回 = 拒む
  await pgErr(observe('mc_20300109T999999999Z_cccccc', [obsSingle('new-v')]), /unknown_run/);
});

console.log('\nJAN (H6)');

await ta('[J1] JAN を足す・外す = 変更の記録 (人・request_id・出どころ・理由)・SKU と商品の version が変わる・編集の印が変わる', async () => {
  const v0 = (await one("select version::text as v from core.skus where code = 's003'")).v;
  const t0 = await tokenOf('s003');
  const rid = uuid();
  const r = await save('s003', { jan: J3 }, { requestId: rid, reason: 'JAN を入れた' });
  assert.deepEqual(r.changed.map((c) => [c.field, c.from, c.to]), [['jan', [], [J3]]]);
  const ev = await one("select operation, actor_type, actor_id, source_system, request_id, reason_text, new_value from events.master_change_events where entity_type = 'external_id' order by event_id desc limit 1");
  assert.deepEqual([ev.operation, ev.actor_type, ev.actor_id, ev.source_system, ev.request_id, ev.reason_text, ev.new_value.external_value, ev.new_value.resolution],
    ['INSERT', 'human', 'naka@test', 'portal_master_edit', W.janRequestId(rid), 'JAN を入れた', J3, 'manual']);   // JAN の約束の request_id (保存の request_id から決まる)
  assert.notEqual((await one("select version::text as v from core.skus where code = 's003'")).v, v0);
  assert.notEqual(await tokenOf('s003'), t0);
  const r2 = await save('s003', { jan: J4 });
  assert.deepEqual(r2.changed[0].to, [J4]);
  const upd = await one("select operation, attribute, old_value, new_value from events.master_change_events where entity_type = 'external_id' and operation = 'UPDATE' order by event_id desc limit 1");
  assert.deepEqual([upd.attribute, upd.old_value], ['valid_to', null]);
  assert.ok(upd.new_value);
  const active = await q("select external_value from core.external_ids e join core.skus s on s.product_id = e.entity_id where s.code = 's003' and e.system = 'jan' and e.valid_to is null");
  assert.deepEqual(active.map((x) => x.external_value), [J4]);
  const hist = await W.changesSince(db, { skuId: await skuId('s003'), productId: (await one("select product_id::text as p from core.skus where code = 's003'")).p });
  assert.ok(hist.filter((e) => e.entity_type === 'external_id').length >= 3, 'その間の変更に JAN も出る');
  assert.equal((await save('s003', { jan: `${J4}` })).no_change, true);
});

await ta('[J2] JAN の守り: 値の書き換え・物理の削除・外した行を戻す = 拒む (DB)。形・チェック数字 = 400・ほかの商品の有効な JAN = 409 (一意)', async () => {
  await pgErr(pg.query("update core.external_ids set external_value = '4900000000000' where system = 'jan' and external_value = $1", [J4]), /書き換えない/);
  await pgErr(pg.query("delete from core.external_ids where system = 'jan' and external_value = $1", [J4]), /消さない/);
  await pgErr(pg.query("update core.external_ids set valid_to = null where system = 'jan' and external_value = $1 and valid_to is not null", [J3]), /戻さない/);
  await rejectsWith(save('s004', { jan: '4900000000001' }), 400);
  await rejectsWith(save('s004', { jan: '12345' }), 400);
  const e = await rejectsWith(save('s004', { jan: J4 }), 409, 'jan_taken');
  assert.match(e.message, /s003/);
  await rejectsWith(reg('single', 'new-jan-dup', single({ jan: J4 })), 400);   // 新商品の登録は JAN を受けない (登録の後に商品の画面で)
  assert.equal(await skuId('new-jan-dup'), undefined);
  // lib を通さずに呼んでも: ほかの商品の有効な JAN = jan_taken・形 = invalid_input (何も書かない)
  const s4 = await skuId('s004');
  await pgErr(as(E, 'master_edit', () => pg.query("select ops.edit_sku_jan(gen_random_uuid(), 'naka@test', null, $1::jsonb, $2::bigint, '[]'::jsonb, $3::jsonb)",
    [OWN, s4, JSON.stringify([J4])])), /jan_taken/);
  await pgErr(as(E, 'master_edit', () => pg.query("select ops.edit_sku_jan(gen_random_uuid(), 'naka@test', null, $1::jsonb, $2::bigint, '[]'::jsonb, '[\"4900000000001\"]'::jsonb)",
    [OWN, s4])), /invalid_input/);
});

await ta('[J3] JAN の持ち主: 画面の保存は external_ids.jan が company のときだけ (ほかが company でも load なら切替前)・夜間ロードは company なら商品の JAN に触らない / load なら付ける', async () => {
  // 持ち主表が段階の記録と違う = 全部閉じる (#1563 R1)。JAN の欄の持ち主のキーも確かめる
  assert.deepEqual(W.SINGLE_FIELDS.jan.keys, ['external_ids.jan']);
  assert.ok(W.fieldOwnership('single', { ...ALL_COMPANY, 'external_ids.jan': 'load' }, true).jan.editable === false);
  const plan = makePlan();
  plan.observations.push({ skuCode: 's005', attribute: 'jan', scope: 'item', valueText: J2, source: 'product_hub', sourceRef: 'test:j3', observedAt: new Date(LOAD_NOW.getTime() - 86400e3).toISOString() });
  const rc = await runInitialLoad(db, plan, { log: quiet, runId: 'load_j3_company', ownership: ALL_COMPANY, now: LOAD_NOW });
  assert.equal(rc.ok, true, rc.error);
  assert.ok(rc.sections.jan.notes.some((n) => /Company DB が正/.test(n)), JSON.stringify(rc.sections.jan));
  assert.equal((await q("select 1 from core.external_ids where system = 'jan' and external_value = $1", [J2])).length, 0);
  const rl = await runInitialLoad(db, plan, { log: quiet, runId: 'load_j3_load', ownership: { ...ALL_COMPANY, 'external_ids.jan': 'load' }, now: LOAD_NOW });
  assert.equal(rl.ok, true, rl.error);
  assert.equal((await q("select 1 from core.external_ids where system = 'jan' and external_value = $1 and valid_to is null", [J2])).length, 1);
  const ev = await one("select actor_type, source_system from events.master_change_events where entity_type = 'external_id' and new_value ->> 'external_value' = $1", [J2]);
  assert.deepEqual([ev.actor_type, ev.source_system], ['system', 'company_db_load']);
});

await ta('[J4] 新商品の JAN = 登録の後に商品の画面で (登録は JAN を受けない = 400)・NE 登録の CSV の jan_code に入る・JAN の約束 (jan_edit) と done が残る', async () => {
  const jan = jan13('490000000009');
  await rejectsWith(reg('single', 'new-jan-x', single({ name: 'JAN つき', jan })), 400);
  await reg('single', 'new-jan', single({ name: 'JAN つき' }));
  const rid = uuid();
  const r = await save('new-jan', { jan }, { requestId: rid });
  assert.deepEqual([r.changed.map((c) => c.field), r.jan.changed[0].to], [['jan'], [jan]]);
  const x = await one("select e.resolution, e.resolved_by_id from core.external_ids e join core.skus s on s.product_id = e.entity_id where s.code = 'new-jan' and e.system = 'jan' and e.valid_to is null");
  assert.deepEqual([x.resolution, x.resolved_by_id], ['manual', 'naka@test']);
  // 保存の約束 (sku_edit) と JAN の約束 (jan_edit) が 1 つずつ・どちらも done がある (同じ取引)
  const sess = await q("select operation, db_user from ops.master_write_sessions where request_id in ($1, $2) order by operation", [rid, W.janRequestId(rid)]);
  assert.deepEqual(sess.map((s) => [s.operation, s.db_user]), [['jan_edit', 'master_edit'], ['sku_edit', 'master_edit']]);
  assert.deepEqual((await q("select operation, status from ops.master_edit_requests where request_id in ($1, $2) order by operation", [rid, W.janRequestId(rid)])).map((d) => [d.operation, d.status]),
    [['jan_edit', 'done'], ['sku_edit', 'done']]);
  const m = await G.regMaterialOf(db, await skuId('new-jan'), { today: TODAY, nc: ncOf(), live: new Map(), regByIds: new Map(), supRegByIds: new Map() });
  assert.equal(m.cells[0][8], jan);
});

await ta('[J5] JAN の行の権限: 画面のロールは core.external_ids を直接書けない (JAN の約束の関数の中だけ)・表の持ち主でないロールも商品の JAN の行だけ (trigger)', async () => {
  const pid = (await one("select product_id::text as p from core.skus where code = 's005'")).p;
  await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
    values (1, 'product', $1, 'rakuten', 'item_code', 'j5-item', 'manual', 'system', 'test')`, [pid]);
  const deny = async (sql, params) => { const e = await as(E, 'master_edit', () => pgErr(pg.query(sql, params))); assert.equal(e.code, '42501', e.message); };
  await deny(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
    values (1, 'product', $1, 'jan', 'jan', $2, 'manual', 'human', 'x')`, [pid, jan13('490000000078')]);
  await deny("update core.external_ids set valid_to = now() where external_value = 'j5-item'", []);
  await deny("delete from core.external_ids where external_value = 'j5-item'", []);
  // 持ち主でないロール (watch_writer に試験だけ列の権限を足す) = 商品の JAN の行だけ (core.guard_external_ids_writer)
  await pg.query('begin');
  try {
    await pg.query('grant usage on schema core to watch_writer');
    await pg.query('grant insert on core.external_ids to watch_writer');
    await pg.query('set local role watch_writer');
    await pgErr(pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
      values (1, 'product', $1, 'rakuten', 'item_code', 'j5-other', 'manual', 'human', 'x')`, [pid]), /external_id_writer/);
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
  assert.equal((await q("select 1 from core.external_ids where external_value = 'j5-item' and valid_to is null")).length, 1);
});

console.log('\n仕入先 (Medium 3)');

await ta('[S1] 新しいコードの決まり: 数字 1〜4 桁 → 4 桁に 0 で埋める・0000 / 9999 は不可・数字以外 / 5 桁以上は新しく作れない (前からある仕入先は変えない)', async () => {
  const v = (x) => SUP.validateNewSupplierCode(x);
  assert.deepEqual([v('12').code, v('１２').code, v(7).code, v('0123').code], ['0012', '0012', '0007', '0123']);
  for (const x of ['', 'ab', '12345', '0', '0000', '9999', '1-2']) assert.equal(v(x).ok, false, x);
  assert.equal((await one("select code from core.suppliers where code = 'abc'")).code, 'abc');
});

await ta('[S2] 作る = ne_pending・同じ 4 桁 = 409・名前に【…】= 400・申告の前は代表にできない (画面・DB)・申告 (NE で見たコード・誰・いつ) → ne_confirmed で代表にできる', async () => {
  const c = await SUP.createSupplier(db, { actor: 'po@test', code: '12', name: 'テスト商事', orderMethod: 'email', leadTimeDays: '7' }, SOPT);
  assert.deepEqual([c.code, c.state], ['0012', 'ne_pending']);
  await rejectsWith(SUP.createSupplier(db, { actor: 'po@test', code: '0012', name: '二重' }, SOPT), 409, 'supplier_code_taken');
  await rejectsWith(SUP.createSupplier(db, { actor: 'po@test', code: '1', name: '前からある 0001 と同じ' }, SOPT), 409, 'supplier_code_taken');
  await rejectsWith(SUP.createSupplier(db, { actor: 'po@test', code: '13', name: '【FAX発注】テスト' }, SOPT), 400);
  const e = await rejectsWith(save('s004', { primary_supplier: '0012' }), 400, 'supplier_not_confirmed');
  assert.match(e.message, /0012/);
  await pgErr(db.query('begin').then(async () => {
    await db.query("select set_config('core.source_system', 'portal_master_edit', true)");
    await db.query(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary) select 1, s.supplier_id, k.sku_id, true from core.suppliers s, core.skus k where s.code = '0012' and k.code = 's005'`);
  }).finally(() => db.query('rollback')), /supplier_not_confirmed/);
  await rejectsWith(reg('single', 'new-sup', single({ primary_supplier: '0012' })), 400, 'supplier_not_confirmed');
  await rejectsWith(SUP.declareSupplierInNe(db, { actor: 'po@test', code: '0012', neCode: '0013' }, SOPT), 400);
  await rejectsWith(SUP.declareSupplierInNe(db, { actor: 'po@test', code: '0001', neCode: '0001' }, SOPT), 409, 'not_new_supplier');
  const d = await SUP.declareSupplierInNe(db, { actor: 'po@test', code: '0012', neCode: '0012', note: 'NE の仕入先の画面で登録' }, SOPT);
  assert.equal(d.state, 'ne_confirmed');
  const r = await one("select declared_by, declared_at is not null as at, evidence from ops.supplier_registrations r join core.suppliers s on s.supplier_id = r.supplier_id where s.code = '0012'");
  assert.deepEqual([r.declared_by, r.at, r.evidence.ne_code], ['po@test', true, '0012']);
  assert.equal((await save('s004', { primary_supplier: '0012' })).ok, true);
  const ev = await q("select from_state, to_state from ops.supplier_registration_events e join core.suppliers s on s.supplier_id = e.supplier_id where s.code = '0012' order by event_id");
  assert.deepEqual(ev.map((x) => [x.from_state, x.to_state]), [[null, 'ne_pending'], ['ne_pending', 'ne_confirmed']]);
});

await ta('[S3] 取引停止: 代表に使っている = 409 (商品の一覧)・付け替え先を渡す = 同じ取引で付け替えて止める・物理の削除は拒む (二重を寄せる merge だけ通る)', async () => {
  // 0012 を代表にした新商品の NE 登録の CSV を配った = 付け替えは画面の保存と同じく 409 (使わないにすれば付け替えられる)
  await reg('single', 'new-sup2', single({ name: 'SUP2', primary_supplier: '0012' }));
  const sx = await build('products', ['new-sup2']);
  await issue(sx.export.export_id);
  await rejectsWith(SUP.deactivateSupplier(db, { actor: 'po@test', code: '0012', reason: '取引終了' }), 409, 'before_cutover');   // 持ち主表 = 今の本番 (load)
  const e = await rejectsWith(SUP.deactivateSupplier(db, { actor: 'po@test', code: '0012', reason: '取引終了' }, SOPT), 409, 'supplier_in_use');
  assert.deepEqual(e.extra.skus, ['new-sup2', 's004']);
  await pgErr(pg.query("update core.suppliers set active = false where code = '0012'"), /supplier_in_use/);
  const ei = await rejectsWith(SUP.deactivateSupplier(db, { actor: 'po@test', code: '0012', reason: '取引終了', reassignTo: '0002' }, SOPT), 409, 'reg_csv_issued');
  assert.match(ei.message, /new-sup2/);
  assert.equal((await one("select active from core.suppliers where code = '0012'")).active, true, '何もしていない');
  await supersede(sx.export.export_id);
  const r = await SUP.deactivateSupplier(db, { actor: 'po@test', code: '0012', reason: '取引終了', reassignTo: '0002' }, SOPT);
  assert.deepEqual([r.active, r.reassigned], [false, ['new-sup2', 's004']]);
  const prim = await q("select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id join core.skus k on k.sku_id = x.sku_id where k.code = 's004' and x.is_primary");
  assert.deepEqual(prim.map((p) => p.code), ['0002']);
  await pgErr(pg.query("delete from core.suppliers where code = '0012'"), /消さない/);
  await pgErr(pg.query("delete from core.suppliers where code = 'abc'"), /消さない/);
  await pg.query("insert into core.suppliers (company_id, code, name) values (1, '0077', '二重 A'), (1, '77', '二重 B')");
  const m = (await q('select * from core.merge_duplicate_suppliers()'))[0];
  assert.ok(Number(m.merged_suppliers) >= 1, JSON.stringify(m));
  assert.equal((await q("select 1 from core.suppliers where core.canonical_supplier_code(code) = '0077'")).length, 1);
});

await ta('[S4] 仕入先の状態は関数だけ: 持ち主のロールでも直接は書けない・前からある仕入先に状態の行を作れない・NE で見たコードは 4 桁に揃えて比べる', async () => {
  const old = (await one("select supplier_id::text as id from core.suppliers where code = '0002'")).id;
  await pgErr(pg.query("update ops.supplier_registrations set state = 'ne_pending'"), /でだけ書く/);
  await pgErr(pg.query("insert into ops.supplier_registrations (supplier_id, state, created_by) values ($1, 'ne_pending', 'x')", [old]), /でだけ書く/);
  await pgErr(pg.query("select ops.declare_supplier_in_ne(gen_random_uuid(), 'po@test', $1::jsonb, '0002', '0002', null)", [OWN]), /not_new_supplier/);
  await rejectsWith(SUP.createSupplier(db, { actor: 'po@test', code: '15', name: '十五商事' }), 409, 'before_cutover');   // 持ち主表 = 今の本番 (load)
  await rejectsWith(SUP.declareSupplierInNe(db, { actor: 'po@test', code: '0012', neCode: '0012' }), 409, 'before_cutover');
  await SUP.createSupplier(db, { actor: 'po@test', code: '15', name: '十五商事' }, SOPT);
  await pgErr(pg.query("select ops.declare_supplier_in_ne(gen_random_uuid(), 'po@test', $1::jsonb, '0015', '0016', null)", [OWN]), /ne_code_mismatch/);
  const d = await SUP.declareSupplierInNe(db, { actor: 'po@test', code: '0015', neCode: '15' }, SOPT);
  assert.equal(d.state, 'ne_confirmed');
  assert.equal((await SUP.declareSupplierInNe(db, { actor: 'po@test', code: '0015', neCode: '0015' }, SOPT)).already, true);
});

console.log('\n書き込みの約束 (0051 の ops.master_write_sessions に ⑤-2b の操作・#1571 R1 High 3)');

/**
 * 試験だけ: 持ち主のロールが約束の行を直接書き (取引の中だけの設定も)、画面のロールにして fn を流す (関数の中から書く形を作る)。最後は巻き戻す
 */
const asFakeSession = async (op, fn, { skuId = null, products = [], versions = {}, ownership = ALL_COMPANY, actor = 'boss@test' } = {}) => {
  await pg.query('begin');
  try {
    const sid = uuid();
    await pg.query(`insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
        actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
      values ($1, txid_current(), gen_random_uuid(), $2, $3::bigint, '{}', $4::bigint[], repeat('0', 64), repeat('a', 64), $5::jsonb, $6, null, 'portal_master_edit', 'master_edit',
              'new_open', repeat('b', 64), $7::jsonb)`, [sid, op, skuId, `{${products.join(',')}}`, JSON.stringify(versions), actor, JSON.stringify(ownership)]);
    await pg.query("select set_config('ops.master_write_session', $1, true)", [sid]);
    await pg.query('set local role master_edit');
    return await fn();
  } finally { await pg.query('rollback'); await pg.query('set role deploy'); }
};
const code42501 = async (p, re) => { const e = await pgErr(p, re); assert.equal(e.code, '42501', e.message); return e; };

await ta('[D1] ⑤-2b の関数ごとに約束 (操作・DB が決めた相手・DB が作った payload_hash) と done が 1 つずつ・request_id は使い回せない・画面のロールは約束を作れない (begin は sku_edit だけ・部品の関数も無い)', async () => {
  await reg('single', 'new-dd', single({ name: 'DD' }));
  const ddId = await skuId('new-dd');
  const sessOf = (rid) => one('select operation, sku_id::text as sku_id, versions, db_user, actor_id, payload_hash from ops.master_write_sessions where request_id = $1', [rid]);
  const doneOf = (rid) => one('select operation, status, payload_hash, sku_id::text as sku_id from ops.master_edit_requests where request_id = $1', [rid]);
  const ridBuild = uuid();
  const b = await build('products', ['new-dd'], { requestId: ridBuild });
  const id = b.export.export_id;
  const s1 = await sessOf(ridBuild);
  assert.deepEqual([s1.operation, s1.sku_id, s1.versions.export_id, s1.versions.sku_ids.map(String), s1.db_user, s1.actor_id],
    ['reg_csv_build', null, id, [ddId], 'master_edit', 'boss@test']);
  assert.equal(s1.payload_hash, b.export.payload_hash);   // DB が作った payload_hash
  assert.deepEqual(await doneOf(ridBuild), { operation: 'reg_csv_build', status: 'done', payload_hash: s1.payload_hash, sku_id: null });
  // 配る = reg_csv_issue (相手 = このファイル)
  const ridIssue = uuid();
  await as(E, 'master_edit', () => G.issueRegExport(db, { actor: 'boss@test', exportId: id, requestId: ridIssue }, opts()));
  assert.deepEqual([(await sessOf(ridIssue)).operation, (await sessOf(ridIssue)).versions.export_id, (await doneOf(ridIssue)).status], ['reg_csv_issue', id, 'done']);
  // 同じ request_id = 使い回せない
  await pgErr(as(E, 'master_edit', () => pg.query("select ops.ne_reg_supersede($1::uuid, 'boss@test', $2::jsonb, $3::bigint, '理由', '直し方')", [ridIssue, OWN, id])), /request_id_reused/);
  // 画面のロールは ⑤-2b の約束を begin で作れない・部品 (約束を書く・閉じる・門) を呼べない
  await pgErr(as(E, 'master_edit', () => pg.query(`select ops.begin_master_write(gen_random_uuid(), 'boss@test', null, $1::jsonb, 'reg_csv_issue', $2::bigint, repeat('0', 64), repeat('a', 64), '{}'::jsonb)`,
    [OWN, ddId])), /知らない操作/);
  for (const sql of ["select ops.open_reg_write('reg_csv_issue', gen_random_uuid(), 'x', null, '{}'::jsonb, null, null, null, repeat('a', 64), '{}'::jsonb)",
    "select ops.close_reg_write('{}'::jsonb, 'x')", "select ops.reg_write_gate('{}'::jsonb)", "select ops.ne_reg_supersede_built(array[1]::bigint[], 'x', 'x')"]) {
    await as(E, 'master_edit', () => code42501(pg.query(sql)));
  }
  // 保存の中の ops.ne_reg_guard_on_save = 保存の約束 (sku_edit) の中だけ
  await as(E, 'master_edit', () => code42501(pg.query("select ops.ne_reg_guard_on_save(array[$1]::bigint[], 'boss@test', '名前')", [ddId]), /master_write_session_required/));
  // 申告 = reg_csv_declare・その request_id が登録の状態の履歴に残る
  const ridDeclare = uuid();
  await as(E, 'master_edit', () => G.declareRegExport(db, { actor: 'boss@test', exportId: id, sha256: b.export.sha256, result: 'ok', requestId: ridDeclare }, opts()));
  assert.equal((await one("select e.request_id from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = 'new-dd' and e.to_state = 'ne_pending'")).request_id, ridDeclare);
  assert.equal((await doneOf(ridDeclare)).operation, 'reg_csv_declare');
  await supersede(id);
  // 操作の一覧: lib (REG_WRITE_OPERATIONS) = DB の CHECK から sku_edit・sku_create を除いたもの。sku_edit・sku_create の書いてよい行は 0051 / 0052 のまま
  const ck = (await one("select pg_get_constraintdef(oid) as d from pg_constraint where conname = 'ck_mws_operation'")).d;
  for (const op of [...Object.keys(G.REG_WRITE_OPERATIONS), 'sku_edit', 'sku_create']) assert.ok(ck.includes(`'${op}'`), op);
  const allowed = async (op, t, a) => (await one('select ops.master_write_allowed($1, $2, $3) as x', [op, t, a])).x;
  assert.deepEqual([await allowed('sku_edit', 'core.skus', 'UPDATE'), await allowed('sku_create', 'core.skus', 'INSERT'), await allowed('sku_edit', 'core.skus', 'INSERT'),
    await allowed('sku_edit', 'core.external_ids', 'INSERT'), await allowed('jan_edit', 'core.external_ids', 'INSERT'), await allowed('reg_csv_issue', 'core.skus', 'UPDATE')],
  [true, true, false, false, true, false]);
});

await ta('[D2] 新規登録の CSV の表の守り: 画面のロールが関数の中で書く = 約束の操作で書いてよい表・約束のファイルの行だけ / 保存・JAN の約束では「作っただけのファイルを使わないにする」だけ', async () => {
  await reg('single', 'new-ee', single({ name: 'EE' }));
  const ee = await build('products', ['new-ee']);
  const other = await build('products', ['new-dd']);   // [D1] で使わないにした = もう一度作れる
  const id = ee.export.export_id;
  await pg.query(`create function ops.zz_t_issue(p bigint) returns void language sql security definer set search_path = pg_catalog, pg_temp as $$
    update ops.ne_reg_export_items set state = 'issued', state_changed_at = now(), state_changed_by = 't' where export_id = p and state = 'built' $$`);
  await pg.query(`create function ops.zz_t_supersede(p bigint) returns void language sql security definer set search_path = pg_catalog, pg_temp as $$
    update ops.ne_reg_export_items set state = 'superseded', superseded_reason = 't', superseded_correction = 't', state_changed_at = now(), state_changed_by = 't' where export_id = p and state = 'built' $$`);
  await pg.query('grant execute on function ops.zz_t_issue(bigint), ops.zz_t_supersede(bigint) to master_edit');
  try {
    const call = (fn) => pg.query(`select ops.${fn}($1::bigint)`, [id]);
    await as(E, 'master_edit', () => code42501(call('zz_t_issue'), /master_write_session_required/));
    await asFakeSession('reg_csv_issue', () => code42501(call('zz_t_issue'), /master_write_target/), { versions: { export_id: other.export.export_id } });
    await asFakeSession('reg_csv_issue', () => call('zz_t_issue'), { versions: { export_id: id } });   // 約束のファイル = 通る
    await asFakeSession('supplier_create', () => code42501(call('zz_t_issue'), /master_write_operation/));
    const s6 = await skuId('s006');
    await asFakeSession('sku_edit', () => code42501(call('zz_t_issue'), /master_write_target/), { skuId: s6 });
    await asFakeSession('sku_edit', () => call('zz_t_supersede'), { skuId: s6 });   // 使わないにするだけ = 通る
    await asFakeSession('jan_edit', () => code42501(call('zz_t_issue'), /master_write_target/), { skuId: s6 });
    // 保存の中の「作っただけのファイルを使わないにする」= 保存の約束の人・相手の SKU だけ
    const eeId = await skuId('new-ee');
    const guardSql = 'select ops.ne_reg_guard_on_save(array[$1]::bigint[], $2, $3)';
    await asFakeSession('sku_edit', () => code42501(pg.query(guardSql, [eeId, 'boss@test', '名前']), /master_write_target/), { skuId: s6 });
    await asFakeSession('sku_edit', () => code42501(pg.query(guardSql, [eeId, 'other@test', '名前']), /master_write_session_mismatch/), { skuId: eeId });
    await asFakeSession('sku_edit', () => pg.query(guardSql, [eeId, 'boss@test', '名前']), { skuId: eeId });   // 相手の SKU = 通る (巻き戻す)
    // ⑤-2b の関数は、ほかの約束の中では始めない (1 つの取引に約束は 1 つずつ)
    await asFakeSession('sku_edit', () => pgErr(pg.query("select ops.ne_reg_issue(gen_random_uuid(), 'boss@test', $1::jsonb, $2::bigint)", [OWN, id]), /master_write_session_exists/), { skuId: s6 });
    assert.equal((await one('select state from ops.ne_reg_exports where export_id = $1', [id])).state, 'built', '巻き戻した');
  } finally {
    await pg.query('drop function ops.zz_t_issue(bigint), ops.zz_t_supersede(bigint)');
  }
  await supersede(id);
  // 1 つの取引で ⑤-2b の関数を 2 つ = 関数ごとに約束と done (前の関数が約束の設定を消す)
  await as(E, 'master_edit', async () => {
    await pg.query('begin');
    try {
      await pg.query("select ops.ne_reg_issue(gen_random_uuid(), 'boss@test', $1::jsonb, $2::bigint)", [OWN, other.export.export_id]);
      await pg.query("select ops.ne_reg_supersede(gen_random_uuid(), 'boss@test', $1::jsonb, $2::bigint, '理由', '取り込んでいない')", [OWN, other.export.export_id]);
      await pg.query('commit');
    } catch (e) { await pg.query('rollback'); throw e; }
  });
  assert.deepEqual(await expOf(other.export.export_id).then((e) => [e.state, e.close_reason]), ['closed', 'superseded']);
  assert.deepEqual((await q(`select s.operation from ops.master_write_sessions s join ops.master_edit_requests d on d.request_id = s.request_id
     where s.versions ->> 'export_id' = $1 and s.operation in ('reg_csv_issue', 'reg_csv_supersede') order by s.created_at`, [String(other.export.export_id)])).map((r) => r.operation),
  ['reg_csv_issue', 'reg_csv_supersede']);
});

await ta('[D3] JAN の行の守り (専用・core.guard_master_edit_jan): JAN の約束 (jan_edit) の中・約束の商品・external_ids.jan が company・約束の人の manual の行だけ', async () => {
  const s6 = await skuId('s006');
  const p6 = (await one('select product_id::text as p from core.skus where sku_id = $1', [s6])).p;
  const p7 = (await one("select product_id::text as p from core.skus where code = 's007'")).p;
  const J = jan13('490000000093');
  await pg.query(`create function core.zz_t_jan(p_product bigint, p_jan text, p_by text) returns void language sql security definer set search_path = pg_catalog, pg_temp as $$
    insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id)
      values (1, 'product', p_product, 'jan', 'jan', p_jan, 'manual', 'human', p_by) $$`);
  await pg.query('grant execute on function core.zz_t_jan(bigint, text, text) to master_edit');
  try {
    const call = (by = 'boss@test') => pg.query('select core.zz_t_jan($1::bigint, $2, $3)', [p6, J, by]);
    await as(E, 'master_edit', () => code42501(call(), /master_write_session_required/));
    await asFakeSession('sku_edit', () => code42501(call(), /master_write_session_required/), { skuId: s6, products: [p6] });
    // 約束の商品でない = JAN の守りが拒む (0051 の guard も SKU・商品の version で拒むが、それより前に)
    await asFakeSession('jan_edit', () => code42501(call(), /master_write_target: JAN の約束の商品の JAN の行でない/), { skuId: s6, products: [p7] });
    await asFakeSession('jan_edit', () => code42501(call(), /owner_not_company/), { skuId: s6, products: [p6], ownership: { ...ALL_COMPANY, 'external_ids.jan': 'load' } });
    await asFakeSession('jan_edit', () => code42501(call('other@test'), /master_write_session_mismatch/), { skuId: s6, products: [p6] });
    await asFakeSession('jan_edit', () => call(), { skuId: s6, products: [p6] });   // 約束どおり = 通る (巻き戻す)
  } finally {
    await pg.query('drop function core.zz_t_jan(bigint, text, text)');
  }
  assert.equal((await q('select 1 from core.external_ids where external_value = $1', [J])).length, 0);
  // ops.edit_sku_jan: 画面が見ていた JAN と違う = version_conflict・配った後 = reg_csv_issued
  await pgErr(as(E, 'master_edit', () => pg.query("select ops.edit_sku_jan(gen_random_uuid(), 'boss@test', null, $1::jsonb, $2::bigint, '[\"4900000000000\"]'::jsonb, '[]'::jsonb)", [OWN, s6])), /version_conflict/);
});

await ta('[D4] 保存 (sku_edit) の直接の書き込みも、新規登録の CSV が出ている商品の NE に送る欄は DB が拒む (ops.guard_reg_csv_live)・NE に送らない欄は通る / 確かめる前の仕入先は設定に依らず代表にできない', async () => {
  await reg('single', 'new-ff', single({ name: 'FF' }));
  const ff = await build('products', ['new-ff']);
  const fid = await skuId('new-ff');
  const pff = (await one('select product_id::text as p from core.skus where sku_id = $1', [fid])).p;
  await asFakeSession('sku_edit', () => code42501(pg.query("update core.skus set name = 'FF2' where sku_id = $1", [fid]), /reg_csv_issued/), { skuId: fid, products: [pff] });
  await asFakeSession('sku_edit', () => pg.query('update core.skus set reorder_months = 3 where sku_id = $1', [fid]), { skuId: fid, products: [pff] });
  // 代表の仕入先・原価も同じ (CSV の仕入先・原価の列)
  const sup2 = (await one("select supplier_id::text as id from core.suppliers where code = '0002'")).id;
  await asFakeSession('sku_edit', () => code42501(pg.query(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id)
    values (1, $1, $2, false, 'human', 'boss@test')`, [sup2, fid]).then(() => pg.query('update core.supplier_skus set is_primary = true where supplier_id = $1 and sku_id = $2', [sup2, fid])),
  /reg_csv_issued/), { skuId: fid, products: [pff] });
  await asFakeSession('sku_edit', () => code42501(pg.query("update core.sku_costs set valid_to = valid_from where sku_id = $1 and valid_to is null", [fid]), /reg_csv_issued/),
    { skuId: fid, products: [pff] });
  await supersede(ff.export.export_id);
  await asFakeSession('sku_edit', () => pg.query("update core.skus set name = 'FF2' where sku_id = $1", [fid]), { skuId: fid, products: [pff] });   // ファイルが無い = 通る
  // 確かめる前 (ne_pending) の仕入先 = 画面のロールの書き込みでは代表にできない (core.source_system の設定が無くても)
  await SUP.createSupplier(db, { actor: 'po@test', code: '17', name: '十七商事' }, SOPT);
  const sup = (await one("select supplier_id::text as id from core.suppliers where code = '0017'")).id;
  const s6 = await skuId('s006');
  await asFakeSession('sku_edit', () => pgErr(pg.query(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) values (1, $1, $2, true, 'human', 'boss@test')`,
    [sup, s6]), /supplier_not_confirmed/), { skuId: s6 });
});

await ta('[D5] 仕入先の関数 = 約束 (supplier_create / supplier_declare / supplier_deactivate・相手 = 仕入先と付け替える SKU) と done・画面のロールには渡さない・CSV のセルの制御文字 (U+2028 / U+2029 も)', async () => {
  const rid = uuid();
  await SUP.createSupplier(db, { actor: 'po@test', code: '18', name: '十八商事', requestId: rid }, SOPT);
  assert.deepEqual(await one("select s.operation, d.status, s.versions ->> 'supplier_code' as code from ops.master_write_sessions s join ops.master_edit_requests d on d.request_id = s.request_id where s.request_id = $1", [rid]),
    { operation: 'supplier_create', status: 'done', code: '0018' });
  const rid2 = uuid();
  await SUP.declareSupplierInNe(db, { actor: 'po@test', code: '0018', neCode: '18', requestId: rid2 }, SOPT);
  assert.equal((await one('select operation from ops.master_write_sessions where request_id = $1', [rid2])).operation, 'supplier_declare');
  for (const f of ['ops.create_supplier(uuid, text, text, jsonb, text, text, text, integer)', 'ops.declare_supplier_in_ne(uuid, text, jsonb, text, text, text)',
    'ops.deactivate_supplier(uuid, text, text, jsonb, text, text)', 'ops.open_reg_write(text, uuid, text, text, jsonb, bigint, bigint[], bigint[], text, jsonb)']) {
    assert.equal((await one('select has_function_privilege($1, $2, $3) as x', ['master_edit', f, 'execute'])).x, false, f);
  }
  // CSV のセル: 改行・U+2028・U+2029 = 書かない (null) / ふつうの文字・カンマ = 書く (lib の regQuote と同じ決まり)
  const cell = async (s) => (await one('select ops.ne_reg_csv_cell($1) as c', [s])).c;
  for (const cp of [10, 0x2028, 0x2029]) assert.equal(await cell(`a${String.fromCharCode(cp)}b`), null, `U+${cp.toString(16)}`);
  assert.deepEqual([await cell('ab'), await cell('a,b')], ['ab', '"a,b"']);
});

console.log('\nセットの構成の観測 (H2・夜間ロード)');

const E2 = await setupDb();
const q2 = async (sql, p) => (await E2.db.query(sql, p)).rows;
const comps2 = async (code) => (await q2(`select k.code, c.qty from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id
  where c.parent_sku_id = (select sku_id from core.skus where code = $1) order by k.code_norm`, [code])).map((r) => [r.code, Number(r.qty)]);
const GEN = (n) => `mat_2030010${n}T000000000Z_abcdef0${n}_abcdef`;
/** NE の完全な取得の時刻 (sync_meta の形 = UTC)。0050 は 36 時間より前・5 分より先を受けない = 本当の時計の近く */
const utcText = (ms) => new Date(ms).toISOString().replace('T', ' ').slice(0, 19);
// 材料の取得の時刻は試験の初めの今から決める (呼ぶたびに今を読むと、同じ材料の 2 回目が秒の境で違う時刻 = 違う中身になる)
const OBS_BASE = Date.now();
const OBS_AT = (n) => OBS_BASE - (10 - n) * 3600000;
const loadObs = (n, comps, ownership) => runInitialLoad(E2.db, makePlan({ setComponents: comps, material: materialOf(GEN(n), utcText(OBS_AT(n))) }),
  { log: quiet, runId: `load_obs_${n}_${crypto.randomBytes(2).toString('hex')}`, ownership, now: new Date(`2030-01-0${n}T03:00:00Z`) });

await ta('[O1] 持ち主 load (今): 夜間ロードが完全な取得の構成を観測に残す (回 = 材料の世代)・core は今までどおり NE に合わせる・同じ材料 = 何もしない・材料が確かめられない = 残さない', async () => {
  const comps = [{ parentCode: 'set001', childCode: 's001', qty: 3, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' }];
  const r = await loadObs(6, comps, MASTER_OWNERSHIP);
  assert.equal(r.ok, true, r.error);
  assert.deepEqual([r.set_observations.state, r.set_observations.run_id, r.set_observations.sets], ['written', GEN(6), 1]);
  assert.deepEqual(await comps2('set001'), [['s001', 3], ['s002', 1]]);
  const o = await q2('select rows from ops.ne_set_observations where run_id = $1', [GEN(6)]);
  assert.deepEqual(o[0].rows.map((x) => [x.code, x.qty, x.sort]), [['s001', 3, 1], ['s002', 1, 2]]);
  const run = (await q2('select complete, observed_at::text as at, requested_count, fetched_count, skipped_count, raw_hash, source_generation from ops.ne_set_observation_runs where run_id = $1', [GEN(6)]))[0];
  assert.deepEqual([run.complete, run.requested_count, run.fetched_count, run.skipped_count, run.raw_hash, run.source_generation], [true, 1, 1, 0, 'b'.repeat(64), GEN(6)]);
  assert.equal(Math.abs(new Date(run.at).getTime() - OBS_AT(6)) < 120000, true, run.at);
  const again = await loadObs(6, comps, MASTER_OWNERSHIP);
  assert.equal(again.set_observations.state, 'unchanged', JSON.stringify(again.set_observations));
  const noMat = await runInitialLoad(E2.db, makePlan({ setComponents: comps }), { log: quiet, runId: 'load_obs_nomat', now: new Date('2030-01-06T04:00:00Z') });
  assert.equal(noMat.set_observations.state, 'skipped');
  // 古い取得 (35 時間より前) = 見送り (0050 が受けない)・Company DB に無い構成品のセット = 入れない (ほかのセットは残す)
  const old = await runInitialLoad(E2.db, makePlan({ setComponents: comps, material: materialOf(GEN(5), utcText(Date.now() - 40 * 3600000)) }), { log: quiet, runId: 'load_obs_old', now: new Date('2030-01-06T05:00:00Z') });
  assert.deepEqual([old.ok, old.set_observations.state], [true, 'skipped']);
  assert.equal((await q2('select count(*)::int as n from ops.ne_set_observation_runs'))[0].n, 1);
  const unk = await loadObs(4, [{ parentCode: 'set001', childCode: 's001', qty: 3, source: 'ne' }, { parentCode: 'set001', childCode: 'zzz-none', qty: 1, source: 'ne' }], ALL_COMPANY);
  assert.deepEqual([unk.ok, unk.set_observations.state, unk.set_observations.sets, unk.set_observations.skipped], [true, 'written', 0, 1], JSON.stringify(unk.set_observations));
  assert.ok(unk.set_observations.note.includes('入れられないセット 1 件 (unknown_component 1)'), unk.set_observations.note);
  assert.deepEqual([unk.set_observations.complete, unk.set_observations.requested], [false, 1]);
  assert.deepEqual(await comps2('set001'), [['s001', 3], ['s002', 1]]);
});

await ta('[O2] 持ち主 company: 夜間ロードは core.sku_components を書かない・依頼の無い NE の差 = 食い違い (unrequested_diff)・NE が戻れば閉じる', async () => {
  const r = await loadObs(7, [{ parentCode: 'set001', childCode: 's001', qty: 5, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' }], ALL_COMPANY);
  assert.equal(r.ok, true, r.error);
  assert.deepEqual(await comps2('set001'), [['s001', 3], ['s002', 1]], 'core は書かない');
  assert.equal(r.set_observations.promotions.breaches, 1, JSON.stringify(r.set_observations));
  const b = await q2("select kind, status from ops.sku_component_breaches where status = 'open'");
  assert.deepEqual(b.map((x) => [x.kind, x.status]), [['unrequested_diff', 'open']]);
  const r2 = await loadObs(8, [{ parentCode: 'set001', childCode: 's001', qty: 3, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' }], ALL_COMPANY);
  assert.equal(r2.ok, true, r2.error);
  assert.equal((await q2("select count(*)::int as n from ops.sku_component_breaches where status = 'open'"))[0].n, 0, 'NE が今の構成に戻った = 閉じる');
});

await ta('[O3] 持ち主 company: 開いている依頼と同じ NE の観測 (依頼より後の完全な取得) = 夜間ロードの後に上げる (core を依頼の構成に・依頼 applied)', async () => {
  const id = (await q2("select sku_id::text as id from core.skus where code = 'set001'"))[0].id;
  const token = W.editTokenOf(await W.readCurrent(E2.db, id, TODAY));
  await as(E2, 'master_edit', () => W.saveSku(E2.db, { actor: 'naka@test', requestId: uuid(), code: 'set001', reason: '構成を変える', seen: { token },
    values: { components: [{ code: 's001', qty: 1 }, { code: 's003', qty: 2 }] } }, { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES }));
  assert.equal((await q2("select count(*)::int as n from ops.sku_component_requests where status = 'open'"))[0].n, 1);
  // 依頼 (今の実時刻) より後の NE の完全な取得 = 材料の時刻を少し後に (0050 は 5 分より先を受けない)
  const at = utcText(Date.now() + 60000);
  const r = await runInitialLoad(E2.db, makePlan({ setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set001', childCode: 's003', qty: 2, source: 'ne' }],
    material: materialOf('mat_20300109T000000000Z_abcdef09_abcdef', at) }), { log: quiet, runId: 'load_obs_9', ownership: ALL_COMPANY, now: NOW });
  assert.equal(r.ok, true, r.error);
  assert.equal(r.set_observations.promotions.promoted, 1, JSON.stringify(r.set_observations));
  assert.deepEqual(await comps2('set001'), [['s001', 1], ['s003', 2]]);
  assert.equal((await q2("select status from ops.sku_component_requests order by component_request_id desc limit 1"))[0].status, 'applied');
});

await ta('[O4] 観測の数は 0050 の厳密な整数 (数量 0.5・100000 の構成のセットは入れない・並び = 1〜N)・昇格の答えを分ける (もっと新しい観測がある = skipped・投げない)', async () => {
  // 1. 数量の形 (0050 の ops.ne_set_rows_problem = 1〜99,999 の整数)
  assert.deepEqual([1, 2, 99999, '3', 0, -1, 0.5, 100000, '1.0', '-1', null, undefined, Number.NaN, 2 ** 53].map(observationQtyOf),
    [1, 2, 99999, 3, null, null, null, null, null, null, null, null, null, null]);
  // 2. 数量 0.5 / 100000 の構成のセット = 入れない (完全な回に入れない・ロードは成功)
  for (const [i, qty] of [['a', 0.5], ['b', 100000]]) {
    const r = await runInitialLoad(E2.db, makePlan({ setComponents: [{ parentCode: 'set001', childCode: 's001', qty, source: 'ne' }, { parentCode: 'set001', childCode: 's003', qty: 2, source: 'ne' }],
      material: materialOf(`mat_20300109T000000000Z_abcdef0${i}_abcdef`, utcText(OBS_BASE - 2 * 3600000)) }), { log: quiet, runId: `load_obs_q${i}`, ownership: ALL_COMPANY, now: NOW });
    assert.deepEqual([r.ok, r.set_observations.state, r.set_observations.sets, r.set_observations.skipped], [true, 'written', 0, 1], JSON.stringify(r.set_observations));
  }
  assert.deepEqual(await comps2('set001'), [['s001', 1], ['s003', 2]]);
  // 3. 並び = 材料の行の順に 1〜N
  const o = await q2("select o.rows from ops.ne_set_observations o join ops.ne_set_observation_runs r on r.run_id = o.run_id order by o.observation_id desc limit 1");
  assert.deepEqual(o[0].rows.map((x) => x.sort), [1, 2]);
  // 4. 古い取得 (同じセットにもっと新しい完全な観測 = [O3] がある) = superseded_observation = skipped (投げない・other に入れない・core を変えない)
  const old = await runInitialLoad(E2.db, makePlan({ setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 4, source: 'ne' }, { parentCode: 'set001', childCode: 's003', qty: 2, source: 'ne' }],
    material: materialOf('mat_20300109T000000000Z_abcdef0c_abcdef', utcText(OBS_BASE - 3 * 3600000)) }), { log: quiet, runId: 'load_obs_old2', ownership: ALL_COMPANY, now: NOW });
  assert.equal(old.ok, true, old.error);
  const pr = old.set_observations.promotions;
  assert.deepEqual([pr.promoted, pr.skipped, pr.other, pr.errors, pr.reasons.superseded_observation], [0, 1, 0, 0, 1], JSON.stringify(old.set_observations));
  assert.deepEqual(await comps2('set001'), [['s001', 1], ['s003', 2]], 'core は変えない');
  // 分け方の一覧 (⑤-1 の昇格の答え): 食い違い・書かない・もう一度。原価の重なりは もう一度 (人が見る)
  assert.ok(PROMOTE_OUTCOMES.skipped.includes('superseded_observation') && PROMOTE_OUTCOMES.skipped.includes('observation_too_old') && PROMOTE_OUTCOMES.skipped.includes('not_a_set'));
  assert.ok(PROMOTE_OUTCOMES.retry.includes('cost_overlap') && PROMOTE_OUTCOMES.breaches.includes('underivable'));
});

await ta('[O5] 入れられないセットが 1 つでもある回 = 完全な回にしない (#1571 Codex R1 Medium 1): 構成の行が 0 のセット・数量の誤りのセット → complete = false・requested = NE のセットの数・上げない (食い違いも残さない)', async () => {
  const zero = { code: 'set002', name: 'セット 2', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: null,
    standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 };
  const breaches0 = (await q2("select count(*)::int as n from ops.sku_component_breaches"))[0].n;
  // 構成が今と違う set001 (完全な回なら候補) + 構成の行が 0 の set002
  const r = await runInitialLoad(E2.db, makePlan({ setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 7, source: 'ne' }, { parentCode: 'set001', childCode: 's003', qty: 2, source: 'ne' }],
    material: materialOf('mat_20300109T000000000Z_abcdef0d_abcdef', utcText(Date.now() + 60000)), extraSkus: [zero] }), { log: quiet, runId: 'load_obs_zero', ownership: ALL_COMPANY, now: NOW });
  assert.equal(r.ok, true, r.error);
  const so = r.set_observations;
  assert.deepEqual([so.state, so.complete, so.requested, so.sets, so.excluded, so.candidates, so.promotions], ['written', false, 2, 1, { no_rows: 1 }, [], undefined], JSON.stringify(so));
  const run = (await q2('select complete, requested_count, fetched_count, saved_count from ops.ne_set_observation_runs where run_id = $1', ['mat_20300109T000000000Z_abcdef0d_abcdef']))[0];
  assert.deepEqual([run.complete, run.requested_count, run.fetched_count, run.saved_count], [false, 2, 2, 1]);
  assert.equal((await q2("select count(*)::int as n from ops.sku_component_breaches"))[0].n, breaches0, '完全でない回は食い違いを残さない');
  assert.deepEqual(await comps2('set001'), [['s001', 1], ['s003', 2]], 'core は変えない');
  // 数量の誤りのセットだけの回も同じ (完全でない)
  const b = await runInitialLoad(E2.db, makePlan({ setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 0.5, source: 'ne' }],
    material: materialOf('mat_20300109T000000000Z_abcdef0e_abcdef', utcText(Date.now() + 60000)) }), { log: quiet, runId: 'load_obs_badqty', ownership: ALL_COMPANY, now: NOW });
  assert.deepEqual([b.set_observations.complete, b.set_observations.excluded, b.set_observations.sets], [false, { bad_qty: 1 }, 0], JSON.stringify(b.set_observations));
});

console.log('\n照合 ② の観測の形 (compare-ne)');

await ta('[P1] registrationObservations: 完全な取得の集合から、確かめ待ちの商品の列を送る形に (単品 7 列・セットの構成品)・無い / 衝突 / 取込の問題', async () => {
  const nm = new Map([
    ['new-x', { code: 'new-x', kind: 'single', cols: { name: CNE.textState('X', 'name'), handling: CNE.textState('取扱中', 'handling'), tax_rate: CNE.numState('"10"', 'tax'),
      standard_price_jpy: CNE.numState('1500', 'yen'), cost: CNE.numState('""', 'yen'), primary_supplier: CNE.textState('1', 'supplier'), parent: CNE.repState('', '""', 'new-x') } }],
    ['new-s', { code: 'new-s', kind: 'set', cols: { name: CNE.textState('S', 'name'), standard_price_jpy: CNE.numState('2500', 'yen') }, children: new Map([['s001', { code: 's001', st: CNE.numState('2', 'qty') }]]) }],
    ['bad', { code: 'bad', kind: 'single', cols: { name: CNE.textState('B', 'name') } }],
  ]);
  const r = CNE.registrationObservations(nm, [{ code_norm: 'new-x' }, { code_norm: 'new-s' }, { code_norm: 'gone' }, { code_norm: 'bad' }],
    { collided: new Set(['bad']), intBlocked: new Map(), absenceTrusted: true, productsAt: '2030-01-10 00:00:00', setsAt: '2030-01-10 00:01:00' });
  assert.deepEqual([r.products_at, r.sets_at, r.absence_trusted], ['2030-01-10T00:00:00.000Z', '2030-01-10T00:01:00.000Z', true]);
  const [x, s, g, b] = r.observations;
  assert.deepEqual(x.cols, { name: { st: 'ok', v: 'X' }, supplier: { st: 'ok', v: '0001' }, cost: { st: 'no_value', v: null }, price: { st: 'ok', v: 1500 }, tax_rate: { st: 'ok', v: 0.1 },
    handling: { st: 'ok', v: 'active' }, parent: { st: 'ok', v: null } });
  assert.deepEqual(s.children, [{ code_norm: 's001', st: 'ok', v: 2 }]);
  assert.deepEqual([g.present, g.trusted], [false, true]);
  assert.equal(b.trusted, false);
});

console.log('\n画面 (master-edit)');
const MR = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost:5432/test';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost:5432/test';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_DECISION_APPROVERS = 'boss@test';
process.env.MASTER_EDIT_OPEN = '1';
MR.__setPgClientFactory(async (url) => {
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); }, on: () => {} };
});
MR.__setClock(() => NOW_MS);
MR.__setOwnership(ALL_COMPANY);
MR.__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s ? { authenticated: true, email: s, displayName: s, role: 'user', allowedApps: ['master-edit'] } : null;
  next();
});
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'boss@test', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(ORIGIN + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'manual' });
  const buf = Buffer.from(await r.arrayBuffer());
  const text = buf.toString('utf8');
  let j = null; try { j = JSON.parse(text); } catch { /* HTML / CSV */ }
  return { status: r.status, j, text, buf, headers: r.headers };
}
function checkScripts(html, expected) {
  const scripts = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].filter((m) => !/\bsrc=/.test(m[1])).map((m) => m[2]);
  assert.equal([...html.matchAll(/<script\b/gi)].length, [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].length);
  if (expected != null) assert.equal(scripts.length, expected);
  for (const s of scripts) { new vm.Script(s); assert.ok(!/<%|%>/.test(s), 'EJS のタグが JS に残っている'); }
  return scripts;
}

try {
  await ta('[H1] NE 登録の CSV の画面: 描画・画面の JS・候補と止まる理由・ファイル・名簿でない人は見るだけ', async () => {
    await reg('single', 'new-web', single({ name: 'WEB' }));
    let r = await call('GET', '/apps/master-edit/reg-csv');
    assert.equal(r.status, 200, r.text.slice(0, 300));
    const sc = checkScripts(r.text, 1);
    for (const api of ["'/api/reg-csv/exports'", "'/api/reg-csv/verified'", "'/declare'", "'/supersede'", "'/issue'", "'/file'"]) assert.ok(sc[0].includes(api), `画面が ${api} を呼んでいない`);
    assert.match(r.text, /new-web/); assert.match(r.text, /NE にもうある/); assert.match(r.text, /試し用/);
    assert.match(r.text, /value="new-web"(?![^>]*disabled)/);
    r = await call('GET', '/apps/master-edit/reg-csv', { session: 'naka@test' });
    assert.match(r.text, /名簿の人だけ/); assert.match(r.text, /value="new-web" disabled/);
    const s = await call('GET', '/apps/master-edit/api/reg-csv/summary');
    assert.equal(s.j.applied, true);
  });

  await ta('[H2] API: 名簿 (MASTER_DECISION_APPROVERS)・Origin・作る → 配る → ダウンロード (sha256) → 申告・商品の画面の CSV の箱と JAN', async () => {
    const body = { kind: 'products', codes: ['new-web'], request_id: uuid() };
    assert.equal((await call('POST', '/apps/master-edit/api/reg-csv/exports', { body, session: 'naka@test' })).status, 403);
    assert.equal((await call('POST', '/apps/master-edit/api/reg-csv/exports', { body, origin: false })).status, 403);
    const b = await call('POST', '/apps/master-edit/api/reg-csv/exports', { body });
    assert.equal(b.status, 200, b.text);
    const id = b.j.export.export_id;
    assert.equal((await call('GET', `/apps/master-edit/api/reg-csv/exports/${id}/file`)).status, 409, '配る前はダウンロードできない');
    assert.equal((await call('POST', `/apps/master-edit/api/reg-csv/exports/${id}/issue`, { body: {} })).status, 200);
    const f = await call('GET', `/apps/master-edit/api/reg-csv/exports/${id}/file`);
    assert.equal(f.status, 200);
    assert.match(f.headers.get('content-disposition'), /ne_register_products_.*_trial\.csv/);
    const sha = crypto.createHash('sha256').update(f.buf).digest('hex');
    assert.equal(f.headers.get('x-content-sha256'), sha);
    let page = await call('GET', '/apps/master-edit/sku/new-web', { session: 'naka@test' });
    checkScripts(page.text);
    assert.match(page.text, /NE 登録の CSV/); assert.match(page.text, /配った後なので/);
    assert.match(page.text, /data-field="jan"/);
    const d = await call('POST', `/apps/master-edit/api/reg-csv/exports/${id}/declare`, { body: { sha256: sha, result: 'ok', ne_message: '1件成功しました。' } });
    assert.equal(d.status, 200, d.text);
    assert.equal(await regOf('new-web'), 'ne_pending');
    const bad = await call('POST', `/apps/master-edit/api/reg-csv/exports/${id}/declare`, { body: { sha256: 'a'.repeat(64), result: 'ok' } });
    assert.deepEqual([bad.status, bad.j.reason], [409, 'sha256_mismatch']);
    page = await call('GET', '/apps/master-edit/new?kind=single', { session: 'naka@test' });
    checkScripts(page.text, 1);
    assert.ok(!/data-field="jan"/.test(page.text), '新商品の登録の画面に JAN の欄は無い (登録の後に商品の画面で)');
    assert.ok(!/0012 テスト商事/.test(page.text), '取引停止の仕入先は選べない');
  });
} finally {
  await new Promise((r) => server.close(r));
}

try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows で開いたままのことがある */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.log('NG があります');
