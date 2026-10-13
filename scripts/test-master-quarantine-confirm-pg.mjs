/**
 * test-master-quarantine-confirm-pg.mjs — 0078 (要確認の商品を確認済みにする) を実 PostgreSQL の独立した接続と本物のログインのロールで確かめる
 *
 * 固定する契約:
 *   [R]  本物のログイン: master_edit (画面) = 確かめ・確認できる / master_ops が確認の関数を呼ぶ = 42501・ops.transition_sku_registration を直に呼んでも
 *        要確認は確認済みにならない (confirm_session_required) / watcher (読むだけの取引が既定) = 確かめ・一覧は読める・確認の関数は 42501
 *   [C0] 同じ request_id の同時の押し直し: 後の方は request_id の鍵で待つ → 前の答え (replayed)・履歴は 1 行 (#1715 Codex R3 M1)
 *   [C1] 2 人が同じ商品を同時に確認: 後の人は SKU の鍵で待つ → 先の人の commit の後に not_quarantined (二重に確認しない・履歴は 1 行)
 *   [C2] 確認と保存 (本物の保存の入口 lib/master-write.mjs の saveSku・master_edit のログイン・同じ SKU の鍵) が並ぶ: 保存が先 = 確認は待って、
 *        保存の後の値で確かめ直す (照合の後に名前を NE と違う値にした = not_confirmable) / 確認が先 = 保存は待つ (確認が commit してから進む・#1715 Codex R1 M4)
 *   [C3] 確認と照合 ② (NE のコードの入れ替え = 排他の鍵) が並ぶ: 照合が先 = 確認は待って、新しい回で確かめ直す (封をした回と NE のコードの回が違う = not_confirmable) /
 *        確認が先 = 照合は待つ (確認が commit してから進む)・デッドロックしない
 *   [C4] 確認とマスタの判断の取り消し (本物の入口 apps/master-decisions/decide.mjs の applyDecisions・持ち主のログイン = 画面と同じ・判断の候補の行 = for update) が並ぶ:
 *        取り消しが先 = 確認は待って、取り消しの後の判断で確かめ直す (まだ決めていない = not_confirmable) / 確認が先 = 取り消しは待つ (#1715 Codex R1 M4)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-quarantine-confirm-pg.mjs
 *   (使い捨ての PG: node C:/tmp/pg-embed/run-qconfirm.mjs scripts/test-master-quarantine-confirm-pg.mjs <リポジトリ>)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロールをクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles as createWatchRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen, hex } from './fixtures/master-widen.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import * as W from '../lib/master-write.mjs';
import { applyDecisions } from '../apps/master-decisions/decide.mjs';
import * as OG from '../lib/master-owner-gate.mjs';
OG.__setCapableForTest(OWNED_COLUMNS);   // 試験の DB は全部の列を company にする = このコードの能力も全部 (test-master-gone.mjs と同じ)

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0078 の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/** p が ms の間に終わらないこと (= 鍵で待っている) */
async function stillWaiting(p, ms = 700) { const t = Symbol('t'); return (await Promise.race([p.then(() => 'done', () => 'done'), sleep(ms).then(() => t)])) === t; }
/** 外から開ける扉 (本物の入口の commit の直前で止める) */
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
/** commit の文だけ扉で待たせる接続 (本物の入口 = applyDecisions の取引を開けたまま持つ) */
const commitGated = (db, g) => ({ ...db, query: async (sql, params) => { if (/^\s*commit\s*$/i.test(String(sql))) await g.wait(); return db.query(sql, params); } });

const ALL = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const OWN = JSON.stringify(ALL);
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };
const single = (code) => ({ code, name: `商品 ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, reorderMonths: 2 });
const BASE = ['a01', 'a02'];
const QS = ['q01', 'q02', 'q03', 'q04', 'q05', 'q06', 'q07'];
const planOf = (extra = []) => ({ skus: [...BASE, ...extra].map(single), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [...BASE, ...extra].map((c) => ({ supplierCode: '0001', skuCode: c })),
  primarySuppliers: [...BASE, ...extra].map((c) => ({ skuCode: c, supplierCode: '0001' })), reorder: { available: false, runId: null, reason: '試験' } });

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
try {
  const name = `cdb_qconfirm_${crypto.randomBytes(4).toString('hex')}`;
  await admin.query(`create database ${name}`);
  dbNames.push(name);
  const u = new URL(url); u.pathname = `/${name}`;
  const roleUrl = (role, pw) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
  const open = async (role) => { const c = await openPgClient(role ? roleUrl(role, PW[role] ?? WPW[role]) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); clients.push(c); return c; };
  const O = await open(null);
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: PW });
  await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  const r0 = await runInitialLoad(dbO, planOf(), { log: () => {}, runId: `qc_load_${name}`, host: 'test', now: new Date(Date.now() - 5 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, ALL);
  const r1 = await runInitialLoad(dbO, planOf(QS), { log: () => {}, runId: `qc_load2_${name}`, host: 'test', now: new Date(Date.now() - 86400e3) });
  assert.equal(r1.ok, true, r1.error);
  const [WA, WW, ME, ME2, MO] = [await open('watcher'), await open('watch_writer'), await open('master_edit'), await open('master_edit'), await open('master_ops')];
  const O2 = await open(null);
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const one = async (sql, p) => (await q(sql, p))[0];
  const sid = async (code) => String((await one('select sku_id from core.skus where code = $1', [code])).sku_id);
  const regOf = async (code) => (await one('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))?.state;
  const TODAY = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
  const SUP1 = String((await one(`select supplier_id from core.suppliers where code = '0001'`)).supplier_id);
  // 要確認の値をそろえる (持ち主の手 = 試験だけ。本番は画面の保存)
  for (const c of QS) {
    const id = await sid(c);
    await q(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary, created_by_type, created_by_id) values (1, $1, $2, true, 'human', 't')
      on conflict (supplier_id, sku_id) do update set is_primary = true`, [SUP1, id]);
    await q(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, reason, created_by_type, created_by_id) values (1, $1, 300, 'manual', 'COMPLETE', $2::date, 't', 'human', 't')`, [id, TODAY]);
    await q('update core.skus set reorder_months = 2 where sku_id = $1', [id]);
  }
  // 照合 ② の回 (本物の関数・本物の watch_writer のログイン)
  let runSeq = 0;
  const runIdAt = (at) => `mc_${at.toISOString().slice(0, 10).replace(/-/g, '')}T${String(++runSeq).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;
  const neCodesArg = async (run) => JSON.stringify({ compare_run_id: run, entries: (await q("select code_norm from core.skus where company_id = 1 order by 1")).map((c) => ({ code_norm: c.code_norm, kind: 'product', state: 'ok', ne_code: c.code_norm, spellings: [c.code_norm] })) });
  const obsOf = async (codeNorm) => {
    const k = await one('select sku_id from core.skus where code_norm = $1', [codeNorm]);
    const v = (await one('select ops.ne_reg_canonical($1, $2::date) as r', [k.sku_id, TODAY])).r.expected.values;
    const st = (x) => (x == null ? { st: 'no_value', v: null } : { st: 'ok', v: x });
    return { code_norm: codeNorm, present: true, trusted: true, kind: 'single', ne_code: codeNorm,
      cols: { name: st(v.name), supplier: st(v.supplier), cost: st(v.cost), price: st(v.price), tax_rate: st(v.tax_rate), handling: st(v.handling), parent: st(v.parent) } };
  };
  const sealRun = async ({ decisions = [], neCodes = true, obs = {} } = {}) => {
    const at = new Date();
    const run = runIdAt(at);
    await WW.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run, observed_at: at.toISOString(), decisions })]);
    if (neCodes) await WW.query('select ops.record_ne_codes($1::jsonb)', [await neCodesArg(run)]);
    const snap = (await WW.query('select ops.snapshot_ne_reg_targets($1) as r', [run])).rows[0].r;
    const observations = [];
    for (const t of snap.targets) { const b = await obsOf(t.code_norm); observations.push(obs[t.code_norm] ? obs[t.code_norm](b) : b); }
    const fetched = new Date(at.getTime() - 1000).toISOString();
    const w = (await WW.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run,
      fetch: { generation_id: `gen_${run.slice(-6)}`, products_rev: '1', sets_rev: '1', raw_hash: hex('c') }, products_at: fetched, sets_at: fetched, absence_trusted: true, observations })])).rows[0].r;
    await WW.query('select ops.seal_ne_registration_run($1, $2, $3)', [run, w.observation_hash, hex('e')]);
    return run;
  };
  const fp = (s) => crypto.createHash('sha256').update(s).digest('hex');
  const candOf = (code) => ({ fingerprint: fp(`${code}|cost`), subject_key: `cost:${code}`, code_norm: code, col: 'cost', child: null, cls: 'rule', reason_kind: 'company_owned', semantic: 'company_owned@1',
    print: { code_norm: code, col: 'cost', c: 300, n: 280 }, resolutions: ['accept_difference', 'fix_ne', 'fix_cdb'], proposal: { op: 'decide' } });
  const candQ06 = candOf('q06');
  const candQ05 = candOf('q05');
  await sealRun({ decisions: [candQ06] });
  await q(`insert into ops.master_decision_events (fingerprint, kind, resolution, actor_type, actor) values ($1, 'approved', 'accept_difference', 'user', 'naka@test')`, [candQ06.fingerprint]);
  const checkAs = async (c, code) => (await c.query('select ops.quarantine_confirm_check($1::bigint) as r', [await sid(code)])).rows[0].r;
  const confirmSql = 'select ops.confirm_quarantined_sku($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6) as r';
  const confirmArgs = async (code, hash = null) => [crypto.randomUUID(), 'naka@test', '要確認の確認', OWN, await sid(code), hash ?? (await checkAs(O, code)).check_hash];

  console.log('[R] 本物のログイン');
  await ta('[R1] master_edit = 確かめ・確認できる / master_ops = 確認の関数は 42501・状態の関数を直に呼んでも confirm_session_required / watcher (読むだけの取引) = 確かめ・一覧は読める・確認は 42501', async () => {
    const c = await checkAs(ME, 'q01');
    assert.equal(c.ok, true, JSON.stringify(c.items.filter((x) => !x.ok)));
    const r = (await ME.query(confirmSql, await confirmArgs('q01', c.check_hash))).rows[0].r;
    assert.equal(r.state, 'ne_confirmed');
    assert.equal(await regOf('q01'), 'ne_confirmed');
    const e1 = await errOf(MO.query(confirmSql, await confirmArgs('q02')));
    assert.equal(e1?.code, '42501');
    const e2 = await errOf(MO.query(`select ops.transition_sku_registration($1, 'ne_confirmed', 'human', 'naka@test', null, '{}'::jsonb, null)`, [await sid('q02')]));
    assert.match(e2?.message ?? '', /^confirm_session_required/);
    assert.equal((await WA.query('show default_transaction_read_only')).rows[0].default_transaction_read_only, 'on');
    assert.equal((await checkAs(WA, 'q02')).ok, true);
    assert.equal((await WA.query('select jsonb_array_length(ops.quarantine_confirm_list(null)) as n')).rows[0].n, QS.length - 1);
    const e3 = await errOf(WA.query(confirmSql, await confirmArgs('q02')));
    assert.equal(e3?.code, '42501');
    assert.equal(await regOf('q02'), 'quarantined');
  });

  console.log('[C] 競り合い');
  await ta('[C0] 同じ request_id の同時の押し直し (2 つの接続): 後の方は request_id の鍵で待つ → 先の commit の後に前の答え (replayed)・履歴は 1 行 (#1715 Codex R3 M1)', async () => {
    const a = await confirmArgs('q02');
    await ME.query('begin');
    const r1 = (await ME.query(confirmSql, a)).rows[0].r;
    const p2 = ME2.query(confirmSql, a);
    assert.equal(await stillWaiting(p2), true, '同じ request_id の押し直しが待っていない');
    await ME.query('commit');
    const r2 = (await p2).rows[0].r;
    assert.equal(r2.replayed, true);
    assert.equal(r2.event_id, r1.event_id);
    assert.equal((await one(`select count(*)::int as n from ops.master_registration_events where sku_id = $1 and to_state = 'ne_confirmed'`, [await sid('q02')])).n, 1);
    assert.equal(await regOf('q02'), 'ne_confirmed');
  });
  await ta('[C1] 2 人が同じ商品を同時に確認: 後の人は SKU の鍵で待つ → 先の commit の後に not_quarantined・履歴は 1 行', async () => {
    const [a1, a2] = [await confirmArgs('q07'), await confirmArgs('q07')];
    await ME.query('begin');
    await ME.query(confirmSql, a1);
    await ME2.query('begin');
    const p2 = ME2.query(confirmSql, a2);
    assert.equal(await stillWaiting(p2), true, '後の確認が待っていない');
    await ME.query('commit');
    const e = await errOf(p2);
    await ME2.query('rollback');
    assert.match(e?.message ?? '', /^not_quarantined/);
    assert.equal((await one(`select count(*)::int as n from ops.master_registration_events where sku_id = $1 and to_state = 'ne_confirmed'`, [await sid('q07')])).n, 1);
  });
  const dbME2 = pgAdapter(ME2);
  const tokenOf = async (code) => W.editTokenOf(await W.readCurrent(dbO, await sid(code), TODAY));
  const save = async (code, values, { beforeCommit } = {}) => W.saveSku(dbME2, { actor: 'naka@test', requestId: crypto.randomUUID(), code, reason: 'pg', seen: { token: await tokenOf(code) }, values },
    { open: true, beforeCommit });
  await ta('[C2] 確認と本物の保存 (saveSku・master_edit のログイン): 保存が先 = 確認は待って保存の後の値で確かめ直す (照合の後に名前を変えた = NE と違う = not_confirmable) / 確認が先 = 保存は待つ', async () => {
    const args = await confirmArgs('q03');
    const g = gate();
    const s = launch(save('q03', { name: '照合の後に変えた名前' }, { beforeCommit: g.wait }));
    await sleep(500);
    if (s.done) { const x = await s.promise; assert.fail(`保存が扉で止まっていない: ${x.err ? `${x.err.reason} ${x.err.message}` : JSON.stringify(x.ok)}`); }
    const p = ME.query(confirmSql, args);
    assert.equal(await stillWaiting(p), true, '確認が保存 (SKU の鍵) を待っていない');
    g.open();
    const rs = await s.promise;
    assert.ok(rs.ok, rs.err?.message);
    const e = await errOf(p);
    assert.match(e?.message ?? '', /^not_confirmable: q03 .*照合の後に社内の値を変えた/);
    assert.equal(await regOf('q03'), 'quarantined');
    assert.equal((await one(`select count(*)::int as n from ops.master_edit_requests where operation = 'sku_edit' and target_code = 'q03' and status = 'done'`)).n, 1);
    // 戻す (本物の保存)
    assert.ok((await save('q03', { name: '商品 q03' })).ok !== false);
    // 確認が先 = 保存は待つ (確認の取引の SKU の鍵)
    await ME.query('begin');
    await ME.query(confirmSql, await confirmArgs('q03'));
    const s2 = launch(save('q03', { reorder_months: '4' }));
    await sleep(700);
    assert.equal(s2.done, false, '保存が確認を待っていない');
    await ME.query('commit');
    const r2 = await s2.promise;   // 確認の後 = 編集の印 (登録の状態) が変わった = 版の食い違いで断るか、通る (どちらでも確認の後)
    assert.ok(r2.ok || r2.err?.reason === 'version_conflict', r2.err?.message);
    assert.equal(await regOf('q03'), 'ne_confirmed');
  });
  await ta('[C3] 確認と照合 ② の NE のコードの入れ替え: 照合が先 = 確認は待って新しい回で確かめ直す (封がまだ = not_confirmable) / 確認が先 = 照合は待つ・デッドロックしない', async () => {
    const at = new Date();
    const run = runIdAt(at);
    await WW.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run, observed_at: at.toISOString(), decisions: [] })]);
    const args = await confirmArgs('q04');
    await WW.query('begin');
    await WW.query('select ops.record_ne_codes($1::jsonb)', [await neCodesArg(run)]);
    const p = ME.query(confirmSql, args);
    assert.equal(await stillWaiting(p), true, '確認が NE のコードの入れ替えを待っていない');
    await WW.query('commit');
    const e = await errOf(p);
    assert.match(e?.message ?? '', /^not_confirmable: q04 .*NE の元のコードの回/);
    assert.equal(await regOf('q04'), 'quarantined');
    // その回を封まで進める → 確認できる。確認が先 = 照合 (次の回の NE のコード) は待つ
    const snap = (await WW.query('select ops.snapshot_ne_reg_targets($1) as r', [run])).rows[0].r;
    const observations = [];
    for (const t of snap.targets) observations.push(await obsOf(t.code_norm));
    const fetched = new Date(at.getTime() - 1000).toISOString();
    const w = (await WW.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: run,
      fetch: { generation_id: `gen_${run.slice(-6)}`, products_rev: '1', sets_rev: '1', raw_hash: hex('c') }, products_at: fetched, sets_at: fetched, absence_trusted: true, observations })])).rows[0].r;
    await WW.query('select ops.seal_ne_registration_run($1, $2, $3)', [run, w.observation_hash, hex('e')]);
    await ME.query('begin');
    await ME.query(confirmSql, await confirmArgs('q04'));
    const at2 = new Date(); const run2 = runIdAt(at2);
    await WW.query('select ops.record_decision_candidates($1::jsonb)', [JSON.stringify({ compare_run_id: run2, observed_at: at2.toISOString(), decisions: [] })]);
    const pw = WW.query('select ops.record_ne_codes($1::jsonb)', [await neCodesArg(run2)]);
    assert.equal(await stillWaiting(pw), true, '照合が確認を待っていない');
    await ME.query('commit');
    await pw;
    assert.equal(await regOf('q04'), 'ne_confirmed');
  });
  await ta('[C4] 確認と本物の判断の取り消し (applyDecisions・持ち主のログイン = 判断の画面と同じ): 取り消しが先 = 確認は待って取り消しの後の判断で確かめ直す (not_confirmable) / 確認が先 = 取り消しは待つ', async () => {
    // 前の試験で NE のコードの回が進んだ = 封をした回で確かめ直す。NE の原価 280・社内 300 = 差 (候補は承認済み・社内の値 300 に結びつく = ok)
    const neCost = { q05: (b) => ({ ...b, cols: { ...b.cols, cost: { st: 'ok', v: 280 } } }), q06: (b) => ({ ...b, cols: { ...b.cols, cost: { st: 'ok', v: 280 } } }) };
    await sealRun({ decisions: [candQ06, candQ05], obs: neCost });
    await q(`insert into ops.master_decision_events (fingerprint, kind, resolution, actor_type, actor) values ($1, 'approved', 'accept_difference', 'user', 'naka@test')`, [candQ05.fingerprint]);
    const lastEv = async (f) => Number((await one(`select max(event_id) as e from ops.master_decision_events where fingerprint = $1 and kind in ('approved', 'rejected', 'revoked')`, [f])).e);
    assert.equal((await checkAs(O, 'q06')).ok, true, JSON.stringify((await checkAs(O, 'q06')).items.filter((x) => !x.ok)));
    const args = await confirmArgs('q06');
    const g = gate();
    const rv = launch(applyDecisions(commitGated(pgAdapter(O2), g), { actor: 'naka@test', kind: 'revoked', items: [{ fingerprint: candQ06.fingerprint, shown_event_id: await lastEv(candQ06.fingerprint) }] }));
    await sleep(500);
    assert.equal(rv.done, false, '取り消しが扉で止まっていない');
    const p = ME.query(confirmSql, args);
    assert.equal(await stillWaiting(p), true, '確認が判断の取り消し (候補の行) を待っていない');
    g.open();
    const r = await rv.promise;
    assert.equal(r.ok?.applied?.length, 1, JSON.stringify(r.ok ?? r.err?.message));
    const e = await errOf(p);
    assert.match(e?.message ?? '', /^not_confirmable: q06 .*マスタの判断で決める/);
    assert.equal(await regOf('q06'), 'quarantined');
    // 確認が先 = 取り消しは待つ (確認の取引が候補の行を共有で持つ) → 確認の commit の後に取り消しが進む
    await ME.query('begin');
    await ME.query(confirmSql, await confirmArgs('q05'));
    const rv2 = launch(applyDecisions(pgAdapter(O2), { actor: 'naka@test', kind: 'revoked', items: [{ fingerprint: candQ05.fingerprint, shown_event_id: await lastEv(candQ05.fingerprint) }] }));
    await sleep(700);
    assert.equal(rv2.done, false, '取り消しが確認を待っていない');
    await ME.query('commit');
    assert.equal((await rv2.promise).ok?.applied?.length, 1);
    assert.equal(await regOf('q05'), 'ne_confirmed');
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB ${n} を消せない: ${e.message}`); } }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
