/**
 * test-master-widen-pg.mjs — 広げる道 PR-1 (0058) を実 PostgreSQL の独立した接続と本物のロールで確かめる (設計 = 広げる道 v12 §3・§13)
 *
 * 固定する契約 (§13 の番号):
 *   0  ロールの権限: watcher は読むだけの判定 (ops.widen_check_readonly) を呼べる・判定の本体 / apply / prepare / 保守の印は呼べない・保守の印の表は読めない /
 *      master_edit は active_map を関数でだけ読む (表は読めない) / 門のログインは 2 版の記録を書ける (見たハッシュが DB の今と違えば stale_ownership) /
 *      DB の持ち主の関数 (prepare / widen / cancel / 停止 / 保守の印) はほかのロールでは 42501
 *  13  base_commit_seq: ロードの途中 (epoch の共有の鍵) に prepare = ロードの commit を待ってから base に含める・2^53 を超える番号を文字のまま・identity の欠番があっても 2 つと数える
 *   2  widen が拒む (足すだけ・ack・停止・試みの中の commit の数と順・材料の 4 行 / matched / 世代 / ハッシュ / 指紋・時刻の形と境界・held・unverifiable・最終形・会社) と、
 *      全部そろえば通る。読むだけの判定 (watcher) と apply で同じ答え・写しの証拠のロードの番号 (Number に丸めると拒む)
 *   3  夜間ロードの最中 (epoch の共有の鍵) は widen が 5 秒で諦める (55P03)・保存の取引 (段階の共有の鍵) の途中は widen が待つ / 11 本番の大きさで widen の取引 2 秒以内
 *   4  widen の後の G18 (UPDATE・DELETE・DELETE → INSERT を持ち主 = ロード・画面のロールで拒む) と G24 (GUC だけ・前の取引の印・セッションの違う印・直接の INSERT・理由が空を拒む)
 *   5  G19 (deferred・保守の印でも最終形を守る・取引の途中の崩れは commit までに直せば通る)
 *   6  今の origin/master のロードを widen の後に: 区分の変わった材料では取引ごと失敗 (何も残らない)・区分の変わらない材料は今までどおり通る
 *   7  復元の 3 種類 (sku_kind が load のダンプ = 最終形が崩れていても通る / company で整合 = 通る / company で不整合 = 復元全体が rollback)
 *  18  開放の許可 (照合 ② の始めに閉じる → 結果 → 今回の回で grant・権限・取り消しと閉じるは保存の取引を待つ・停止の床・結果を書く前に落ちた再実行では出ない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-widen-pg.mjs
 *   (この PC では C:/tmp/pg-embed の run-conc.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロールをクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす (test:master-edit の最後)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles as createWatchRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { dumpCompanyDb, restoreCompanyDb } from '../apps/company-db/backup/dump.mjs';
import * as OS from '../apps/company-db/load/ownership-state.mjs';
import * as W from '../apps/company-db/load/widen-state.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { ownershipHash, recordLegacyGateAckV2 } from '../lib/master-cutover.mjs';
import { forceNewOpen, hex, ZERO_GATE } from './fixtures/master-widen.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (広げる道の実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-master-widen.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };
const errOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e; } return null; };
const denied = async (c, sql, p, label) => { const e = await errOf(c, sql, p); assert.ok(e, `${label}: 通ってしまった`); assert.equal(e.code, '42501', `${label}: ${e.code} ${e.message}`); };

const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', 'skus.tax_rate': 'company' };   // 切替の後 (new_open) の active の例
const WIDEN = { ...BASE, 'skus.sku_kind': 'company' };
const H_BASE = ownershipHash(BASE), H_WIDEN = ownershipHash(WIDEN);
const MANIFEST = { schema: 'test', entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual', owner_cols: ['skus.name'] },
  { id: 'ne:set-kind', kind: 'manual', owner_cols: ['skus.sku_kind'] }] };
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };

// 本番の大きさ (§13 #11: SKU 約 7,400・構成の行): 単品 5,000・セット 2,300 (構成 2 行ずつ)・例外 90
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const setOf = (code) => ({ code, name: code, kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 200, source: 'set_calc', status: 'COMPLETE' } });
const exc = (code) => ({ code, name: code, kind: 'exception', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 50, source: 'ne', status: 'COMPLETE' } });
const pad = (n) => String(n).padStart(4, '0');
const SINGLES = Array.from({ length: 5000 }, (_, i) => `p${pad(i)}`), SETS = Array.from({ length: 2300 }, (_, i) => `s${pad(i)}`), EXCS = Array.from({ length: 90 }, (_, i) => `x${pad(i)}`);
const planOf = ({ kindOf = {}, extraComponents = [] } = {}) => ({
  skus: [...SINGLES.map((c) => (kindOf[c] === 'set' ? setOf(c) : single(c))), ...SETS.map((c) => (kindOf[c] === 'single' ? single(c) : setOf(c))), ...EXCS.map(exc)],
  variationGroups: [],
  setComponents: [...SETS.filter((c) => kindOf[c] !== 'single').flatMap((c, i) => [{ parentCode: c, childCode: SINGLES[(2 * i) % 5000], qty: 1, source: 'ne' }, { parentCode: c, childCode: SINGLES[(2 * i + 1) % 5000], qty: 2, source: 'ne' }]),
    ...extraComponents],
  listings: [], observations: [], physicals: [], compliance: [], workers: [], suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
});

const dbName = `cdb_widen_${crypto.randomBytes(4).toString('hex')}`, db2Name = `${dbName}_r`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
await admin.query(`create database ${db2Name}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const u2 = new URL(url); u2.pathname = `/${db2Name}`;
const roleUrl = (role, pw) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
const clients = [];
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role, PW[role] ?? WPW[role]) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); clients.push(c); return c; };
try {
  const O = await open(null), O2 = await open(null);
  const dbO = pgAdapter(O), dbO2 = pgAdapter(O2);
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: PW });
  await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  const [WA, WW, E, GR, GM, P, NG] = [await open('watcher'), await open('watch_writer'), await open('master_edit'), await open('master_gate_render'), await open('master_gate_minipc'), await open('master_ops'), await open('new_entry_gate')];
  const dbWA = pgAdapter(WA), dbE = pgAdapter(E);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const t0 = Date.now();
  const r0 = await runInitialLoad(dbO, planOf(), { log: () => {}, runId: 'wpg_load_0', host: 'test' });
  assert.equal(r0.ok, true, r0.error);
  console.log(`  (準備: 本番の大きさのロード ${Date.now() - t0} ms・SKU ${(await q('select count(*)::int as n from core.skus'))[0].n}・構成 ${(await q('select count(*)::int as n from core.sku_components'))[0].n})`);
  await forceNewOpen(dbO, BASE);
  // sku_kind が load の間のダンプ (§13 #7 の 1 つ目・最終形が崩れた行を 1 つ含める = load なら復元は通る)
  await O.query("update core.skus set product_id = (select p.product_id from core.products p where p.display_code = 'p4999') where code = 's2298'");   // セットに product_id (単品の CHECK は表が守る)
  const dumpText = async () => { const lines = []; await dumpCompanyDb(dbO, (l) => { lines.push(l); }); return lines.join(String.fromCharCode(10)); };
  const dumpLoad = await dumpText();
  await O.query("update core.skus set product_id = null where code = 's2298'");
  const acks = async ({ prepared = null, capable = ['skus.sku_kind', 'skus.name'], extra = {} } = {}) => {
    for (const [host, inst] of [['render', 'r-a'], ['minipc', 'm-a']]) {
      await recordLegacyGateAckV2(dbGate[host], { host, instanceId: inst, buildId: 'b1', manifest: MANIFEST, ownership: BASE, phaseSeen: 'new_open',
        activeHashSeen: (await q('select active_hash from ops.master_ownership_state'))[0].active_hash, preparedHashSeen: prepared, capable, ...extra });
    }
  };
  await acks();   // manifest を DB に (prepare は「どれかのプロセスが見た一覧」だけ受ける)
  const prep = (db = dbO, extra = {}) => W.prepareWiden(db, { companyId: 1, map: WIDEN, loaderFingerprint: hex('f'), manifest: MANIFEST, actor: 't', ...extra });

  await ta('[0] ロールの権限: watcher = 読むだけの判定・master_edit = active_map の関数だけ・門 = 2 版の記録・DB の持ち主の関数はほかのロールで 42501', async () => {
    const a = await prep();
    // watcher: 読むだけの判定は呼べる (読み取り専用の取引でも = 書かない)・本体 / apply / prepare / cancel / 停止 / 保守の印 / 封の関数は呼べない・保守の印の表は読めない
    assert.equal((await WA.query('show default_transaction_read_only')).rows[0].default_transaction_read_only, 'on');
    const r = (await WA.query('select ops.widen_check_readonly($1::uuid, 1) as r', [a.widen_prepare_id])).rows[0].r;
    assert.equal(r.ok, false); assert.equal(r.widen_prepare_id, a.widen_prepare_id);
    await denied(WA, 'select ops._widen_judge($1::uuid, 1)', [a.widen_prepare_id], 'watcher → 判定の本体');
    await denied(WA, "select ops.widen_master_ownership($1::uuid, 1, 'w', '{}'::jsonb)", [a.widen_prepare_id], 'watcher → apply');
    await denied(WA, "select ops.cancel_master_widen($1::uuid, 'w')", [a.widen_prepare_id], 'watcher → cancel');
    await denied(WA, "select ops.begin_master_maintenance('x')", [], 'watcher → 保守の印');
    await denied(WA, 'select * from ops.master_maintenance_marks', [], 'watcher → 保守の印の表');
    await denied(WA, "select ops.record_new_entry_gate('mc_x', '2030-01-01T00:00:00Z', '2030-01-01T00:00:00Z', '{}'::jsonb)", [], 'watcher → 照合 ② の結果');
    assert.equal((await WA.query('select count(*)::int as n from ops.master_widen_attempts')).rows[0].n >= 1, true);   // 試みの表は読める
    // watch_writer: 判定・apply・保守の印は呼べない
    await denied(WW, 'select ops.widen_check_readonly($1::uuid, 1)', [a.widen_prepare_id], 'watch_writer → 判定');
    await denied(WW, "select ops.widen_master_ownership($1::uuid, 1, 'w', '{}'::jsonb)", [a.widen_prepare_id], 'watch_writer → apply');
    await denied(WW, 'select * from ops.master_maintenance_marks', [], 'watch_writer → 保守の印の表');
    // master_edit: active の持ち主表は関数でだけ (表は読めない・R1 H4)・広げる道の関数は呼べない
    assert.deepEqual((await E.query('select ops.master_ownership_active_map() as m')).rows[0].m, BASE);
    await denied(E, 'select active_map from ops.master_ownership_state', [], 'master_edit → 持ち主の表');
    await denied(E, "select ops.begin_master_maintenance('x')", [], 'master_edit → 保守の印');
    await denied(E, "update core.skus set sku_kind = 'set' where code = 'p0000'", [], 'master_edit → 区分の列');
    await denied(P, "select ops.prepare_master_widen(1, '{}'::jsonb, $1, '{}'::jsonb, 'x')", [hex('f')], 'master_ops → prepare');
    await denied(P, "select ops.record_widen_manual_stop($1::uuid, 'ne:set-kind', 'x')", [a.widen_prepare_id], 'master_ops → 停止');
    // 門のログイン: 2 版の記録 (見たハッシュが DB の今と違う = stale_ownership・場所の違うログイン = 42501)
    await assert.rejects(recordLegacyGateAckV2(dbGate.render, { host: 'render', instanceId: 'r-z', buildId: 'b1', manifest: MANIFEST, ownership: BASE, phaseSeen: 'new_open',
      activeHashSeen: H_BASE, preparedHashSeen: null, capable: ['skus.sku_kind'] }), /stale_ownership/);   // prepared を見ていない
    await assert.rejects(recordLegacyGateAckV2(dbGate.render, { host: 'minipc', instanceId: 'r-z', buildId: 'b1', manifest: MANIFEST, ownership: BASE, phaseSeen: 'new_open',
      activeHashSeen: H_BASE, preparedHashSeen: H_WIDEN, capable: [] }), (e) => e.code === '42501' && /gate_host_mismatch/.test(e.message));
    await assert.rejects(recordLegacyGateAckV2(dbGate.render, { host: 'render', instanceId: 'r-z', buildId: 'b1', manifest: MANIFEST, ownership: BASE, phaseSeen: 'new_open',
      activeHashSeen: H_BASE, preparedHashSeen: H_WIDEN, capable: ['Bad Key'] }), /capable/);
    await W.cancelWiden(dbO, { attemptId: a.widen_prepare_id, actor: 't' });
  });

  await ta('[13] base_commit_seq: ロードの途中に prepare = ロードの commit を待ってから base に含める・2^53 を超える番号を文字のまま', async () => {
    await O.query('alter table ops.master_load_commits alter column commit_seq restart with 9007199254740993');   // 2^53 + 1 (Number にすると 9007199254740992 に丸まる)
    const paused = gate(), go = gate();
    const load = launch(runInitialLoad(dbO2, planOf(), { log: () => {}, runId: 'wpg_load_13', host: 'test', afterEpochRead: async () => { paused.open(); await go.wait(); } }));
    await paused.wait();   // ロードは epoch の共有の鍵を持って読んだところ
    const p = launch(prep(dbO));
    await sleep(500);
    assert.equal(p.done, false, 'prepare はロードの取引 (epoch の共有の鍵) を待つ');
    go.open();
    const lr = await load.promise; assert.ok(lr.ok, lr.err?.message);
    assert.equal(lr.ok.load_commit_seq, '9007199254740993');   // engine の report も文字
    const pr = await p.promise; assert.ok(pr.ok, pr.err?.message);
    assert.equal(pr.ok.base_commit_seq, '9007199254740993');   // 待っていたロードの commit は base に入る (= 試みの中に数えない)
    assert.equal((await q('select base_commit_seq::text as b from ops.master_widen_attempts where widen_prepare_id = $1', [pr.ok.widen_prepare_id]))[0].b, '9007199254740993');
    assert.equal((await OS.latestLoadCommit(dbO)).commit_seq, '9007199254740993');
    const c = await W.widenCheck(dbWA, { attemptId: pr.ok.widen_prepare_id, companyId: 1 });
    assert.equal(c.counts.commits_after_base, 0); assert.equal(c.counts.base_commit_seq, '9007199254740993');
    await W.cancelWiden(dbO, { attemptId: pr.ok.widen_prepare_id, actor: 't' });
  });

  // ── 本番の試み (A): prepare → 停止 → ack → 回収のロード → prepared のロード → check → widen ──
  const A = await prep();
  const AID = A.widen_prepare_id;
  const check = async (db = dbWA) => W.widenCheck(db, { attemptId: AID, companyId: 1 });
  const has = (r, re) => r.problems.some((x) => re.test(x));
  /** 取引の中で mutate してから、読むだけの判定と apply の答えを比べる (apply は拒まれる = 取引ごと巻き戻す) */
  const variant = async (label, mutate, re, { compareApply = true } = {}) => {
    await O.query('begin');
    try {
      await mutate(O);
      const r = (await O.query('select ops.widen_check_readonly($1::uuid, 1) as r', [AID])).rows[0].r;
      assert.equal(r.ok, false, `${label}: 通ってしまった`);
      assert.ok(has(r, re), `${label}: ${JSON.stringify(r.problems)}`);
      if (compareApply) {
        await O.query('savepoint s');
        const e = await errOf(O, "select ops.widen_master_ownership($1::uuid, 1, 't', $2::jsonb)", [AID, JSON.stringify({ load_commit_seq: '0', build_id: 'b', generation_id: 'g' })]);
        assert.ok(e && /widen_rejected/.test(e.message), `${label}: apply ${e?.message}`);
        assert.deepEqual(JSON.parse(e.detail).problems, r.problems, `${label}: 読むだけの判定と apply の答えが違う`);   // 同じ本体 = 同じ答え
        await O.query('rollback to savepoint s');
      }
    } finally { await O.query('rollback'); }
  };
  const ins = (run, epoch, hash) => `insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, source_tz) values ('${run}', 'sqlite_initial_load', 'products', 'render', now(), 'success', 'UTC');
    insert into ops.master_load_commits (ingest_run_id, epoch, ownership_hash, host) values ('${run}', '${epoch}', '${hash}', 'test')`;
  let stopAt, recovery, prepared;
  const GEN = 'mat_20301010T000000000Z_aaaaaaaa_aaaaaa';

  await ta('[2a] widen が拒む (準備の途中): 停止・ack・試みの中の commit (0 個・順が逆・持ち主が違う)・会社 ≠ 1', async () => {
    let r = await check();
    assert.ok(has(r, /manual_stops/) && has(r, /記録が prepare の前|見た prepared/) && has(r, /commit が 0 個/), JSON.stringify(r.problems));
    await assert.rejects(W.widenCheck(dbWA, { attemptId: AID, companyId: 2 }), /unsupported_company/);
    await assert.rejects(W.widenOwnership(dbO, { attemptId: AID, companyId: 2, actor: 't', evidence: {} }), /unsupported_company/);
    await assert.rejects(prep(dbO, { companyId: 2 }), /unsupported_company/);
    await variant('順が逆 (prepared → active)', async (c) => { for (const s of ins('wpg_rev1', 'prepared', H_WIDEN).split(';')) await c.query(s); for (const s of ins('wpg_rev2', 'active', H_BASE).split(';')) await c.query(s); },
      /1 つ目 .* が回収のロード/, { compareApply: false });
    await variant('2 つ目の持ち主が試みの prepared でない', async (c) => { for (const s of ins('wpg_h1', 'active', H_BASE).split(';')) await c.query(s); for (const s of ins('wpg_h2', 'prepared', H_BASE).split(';')) await c.query(s); },
      /2 つ目 .* が試みの prepared のロードでない/, { compareApply: false });
    // 停止の記録 (DB の時刻) と 2 版の ack
    const s = await W.recordWidenManualStop(dbO, { attemptId: AID, entryId: 'ne:set-kind', stoppedBy: '中原' });
    stopAt = new Date(s.stopped_at);
    await acks({ prepared: H_WIDEN });
    r = await check();
    assert.ok(!has(r, /manual_stops|^ack/), JSON.stringify(r.problems));
  });

  const { fakeLoad } = await import('./fixtures/master-widen.mjs');
  const after = (ms) => new Date(stopAt.getTime() + ms).toISOString();

  await ta('[2b] 試みの中のロード: 回収のロードだけ = 1 個・identity の欠番があっても 2 つと数える・3 つ目 (夜間ロードが挟まる) は拒む', async () => {
    recovery = await fakeLoad(dbO, { epoch: 'active', hash: H_BASE, gen: GEN, completeAt: after(1000) });
    assert.ok(BigInt(recovery.commitSeq) > 9007199254740993n, recovery.commitSeq);   // 2^53 を超える (上の取り消した commit の分だけ進んでいる)
    let r = await check();
    assert.ok(has(r, /commit が 1 個/), JSON.stringify(r.problems));
    // identity の欠番 (中断したロード = 番号を取ってから巻き戻した)。prepared の番号が奇数 (= Number にすると丸まる) になる数だけ飛ばす
    const gaps = BigInt(recovery.commitSeq) % 2n === 0n ? 2 : 1;
    for (let i = 0; i < gaps; i++) { await O.query('begin'); for (const s of ins(`wpg_gap${i}`, 'active', H_BASE).split(';')) await O.query(s); await O.query('rollback'); }
    prepared = await fakeLoad(dbO, { epoch: 'prepared', hash: H_WIDEN, gen: GEN, completeAt: after(1000) });
    assert.equal(BigInt(prepared.commitSeq), BigInt(recovery.commitSeq) + BigInt(gaps) + 1n);   // 間は欠番
    assert.notEqual(String(Number(prepared.commitSeq)), prepared.commitSeq, '試験の番号は Number にすると丸まる値にする');
    r = await check();
    assert.equal(r.ok, true, JSON.stringify(r.problems));
    assert.deepEqual([r.loads.recovery.commit_seq, r.loads.prepared.commit_seq, r.counts.commits_after_base], [recovery.commitSeq, prepared.commitSeq, 2]);
    await variant('3 つ目の commit', async (c) => { for (const s of ins('wpg_third', 'active', H_WIDEN).split(';')) await c.query(s); }, /commit が 3 個/);
  });

  await ta('[2c] widen が拒む: ack (1 版・prepare の前・capable・見た prepared・書きかけ・一覧・黙っている) / 停止の止めた後の時刻', async () => {
    const mh = (await q('select manifest_hash from ops.master_widen_attempts where widen_prepare_id = $1', [AID]))[0].manifest_hash;
    const other = (await q(`insert into ops.master_legacy_manifests (manifest_hash, entries) values (ops.legacy_manifest_hash('{"entries":[{"id":"z","kind":"code"}]}'::jsonb), '{"entries":[{"id":"z","kind":"code"}]}'::jsonb)
      on conflict do nothing returning manifest_hash`))[0]?.manifest_hash ?? (await q(`select ops.legacy_manifest_hash('{"entries":[{"id":"z","kind":"code"}]}'::jsonb) as h`))[0].h;
    const ack = (fields) => async (c) => {
      const f = { host: 'render', instance_id: 'r-a', build_id: 'b1', manifest_hash: mh, owner_hash: H_BASE, phase_seen: 'new_open', inflight_count: 0, oldest_inflight_at: null, session_role: 'master_gate_render',
        ack_version: 2, active_hash_seen: H_BASE, prepared_hash_seen: H_WIDEN, capable: ['skus.sku_kind'], acked_at: new Date().toISOString(), ...fields };
      if (f.ack_version === 1) { f.active_hash_seen = null; f.prepared_hash_seen = null; f.capable = null; }
      const cols = Object.keys(f);
      await c.query(`insert into ops.master_legacy_gate_acks (${cols.join(', ')}) values (${cols.map((k, i) => (k === 'capable' ? `$${i + 1}::text[]` : `$${i + 1}`)).join(', ')})`, cols.map((k) => f[k]));
    };
    await variant('1 版の記録', ack({ ack_version: 1 }), /記録が 1 版/);
    await variant('prepare の前の記録', ack({ instance_id: 'r-b', acked_at: new Date(new Date(A.prepared_at).getTime() - 1000).toISOString() }), /記録が prepare の前/);
    await variant('capable に足すキーが無い', ack({ capable: ['skus.name'] }), /company にできない/);
    await variant('見た prepared が違う', ack({ prepared_hash_seen: H_BASE }), /見た prepared/);
    await variant('書きかけ', ack({ inflight_count: 1, oldest_inflight_at: new Date().toISOString() }), /書きかけが 1 件/);
    await variant('一覧が違う', ack({ manifest_hash: other }), /古い入口の一覧が試みのものと違う/);
    await variant('黙っているプロセス', ack({ instance_id: 'r-old', acked_at: new Date(Date.now() - 20 * 60000).toISOString() }), /r-old: 黙っている/);
    // 止まった記録のプロセスは外れる (通る)
    await O.query('begin');
    try {
      await ack({ instance_id: 'r-stop', stopped: true, stopped_reason: '止めた', acked_at: new Date(Date.now() - 20 * 60000).toISOString() })(O);
      assert.equal((await O.query('select ops.widen_check_readonly($1::uuid, 1) as r', [AID])).rows[0].r.ok, true);
    } finally { await O.query('rollback'); }
  });

  await ta('[2d] widen が拒む: 材料 (4 行・matched・ロードの中の世代・2 つのロードの世代 / ハッシュ・規則の指紋) と取得の時刻 (形・timezone なし・infinity・停止の前・+5 分の外 / 内)', async () => {
    const R = recovery.runId, Pr = prepared.runId;
    await variant('4 行の 1 つが欠ける', (c) => c.query("delete from ops.load_materials where ingest_run_id = $1 and entity = 'set_components'", [Pr]), /4 行がそろっていない/);
    await variant('matched でない', (c) => c.query("update ops.load_materials set status = 'mismatch', generation_id = null, source_complete_at = null, generation_created_at = null where ingest_run_id = $1 and entity = 'products'", [R]), /matched でない/);
    await variant('同じロードの中で世代が違う', (c) => c.query("update ops.load_materials set generation_id = 'mat_20301010T000000000Z_bbbbbbbb_bbbbbb' where ingest_run_id = $1 and entity = 'set_components'", [Pr]), /同じロードの中で/);
    await variant('2 つのロードで世代が違う', (c) => c.query("update ops.load_materials set generation_id = 'mat_20301010T000000000Z_bbbbbbbb_bbbbbb' where ingest_run_id = $1", [Pr]), /2 つのロードで材料の世代・ハッシュが違う/);
    await variant('2 つのロードでハッシュが違う', (c) => c.query("update ops.load_materials set content_hash = $2 where ingest_run_id = $1 and entity = 'products'", [Pr, hex('e')]), /2 つのロードで材料の世代・ハッシュが違う/);
    await variant('規則の指紋が 2 つで違う', (c) => c.query('update ops.load_materials set rule_fingerprint = $2 where ingest_run_id = $1', [Pr, hex('e')]), /規則の指紋/);
    await variant('規則の指紋が試みの版と違う', (c) => c.query('update ops.load_materials set rule_fingerprint = $1', [hex('e')]), /規則の指紋/);
    const at = (v) => (c) => c.query("update ops.load_materials set source_complete_at = $2 where ingest_run_id = $1 and entity = 'products'", [Pr, v]);
    await variant('timezone なし', at('2030-01-01 00:00:00'), /RFC 3339/);
    await variant('T はあるが offset なし', at(after(1000).replace('Z', '')), /RFC 3339/);
    await variant('infinity', at('infinity'), /RFC 3339/);
    await variant('ありえない日', at('2030-02-30T00:00:00Z'), /RFC 3339/);
    await variant('停止の前', at(after(-1000)), /停止 .* の前/);
    await variant('DB の今 + 5 分より先', at(new Date(Date.now() + 5 * 60000 + 30000).toISOString()), /\+ 5 分より先/);
    // + 5 分の内側 (境界の手前) と別の offset の書き方は通る
    for (const v of [new Date(Date.now() + 4 * 60000 + 30000).toISOString(), after(2000).replace('Z', '+00:00')]) {
      await O.query('begin');
      try { await at(v)(O); const r = (await O.query('select ops.widen_check_readonly($1::uuid, 1) as r', [AID])).rows[0].r; assert.equal(r.ok, true, `${v}: ${JSON.stringify(r.problems)}`); }
      finally { await O.query('rollback'); }
    }
  });

  await ta('[2e] widen が拒む: 判断の記録 (sku_kind が無い・形の版・held が配列でない / 文字でない / 空でない (件数の欄が 0 でも))・unverifiable (C の SKU に当たる・形)・最終形', async () => {
    const Pr = prepared.runId;
    const sk = (v) => (c) => c.query("update ops.load_decisions set payload = jsonb_set(payload, '{sku_kind}', $2::jsonb) where ingest_run_id = $1 and section = 'skus'", [Pr, JSON.stringify(v)]);
    const okSk = { format: 'sku-kind-v1', held: [], unverifiable: [] };
    await variant('sku_kind が無い', (c) => c.query("update ops.load_decisions set payload = payload - 'sku_kind' where ingest_run_id = $1", [Pr]), /sku_kind が無い/);
    await variant('判断の記録そのものが無い', (c) => c.query('delete from ops.load_decisions where ingest_run_id = $1', [Pr]), /sku_kind が無い/);
    await variant('形の版が違う', sk({ ...okSk, format: 'sku-kind-v0' }), /形の版/);
    await variant('行の format だけ上げても通らない (payload.sku_kind.format を見る)', async (c) => { await sk({ ...okSk, format: undefined })(c); await c.query("update ops.load_decisions set format = 'sku-kind-v1' where ingest_run_id = $1", [Pr]); }, /形の版/);
    await variant('held が配列でない', sk({ ...okSk, held: {} }), /held が配列でない/);
    await variant('held が文字でない', sk({ ...okSk, held: [1] }), /文字の配列でない/);
    await variant('held が空でない (件数の欄は 0)', sk({ ...okSk, held: ['p0001'], held_count: 0 }), /held が 1 件/);
    await variant('unverifiable が C の SKU に当たる', sk({ ...okSk, unverifiable: [{ reason: 'unknown_kind', raw_code: 'P0001', code_norm: 'p0001' }] }), /C の SKU 1 件に当たる/);
    await variant('unverifiable の code_norm が raw_code と合わない', sk({ ...okSk, unverifiable: [{ reason: 'norm_collision', raw_code: 'ZZ-1', code_norm: 'zz-2' }] }), /行の形が違う/);
    await variant('unverifiable の余分な欄', sk({ ...okSk, unverifiable: [{ reason: 'unknown_kind', raw_code: 'zz', code_norm: 'zz', affected_existing_cdb: 0 }] }), /行の形が違う/);
    await variant('empty_code なのに code_norm がある', sk({ ...okSk, unverifiable: [{ reason: 'empty_code', raw_code: '', code_norm: 'x' }] }), /行の形が違う/);
    await variant('知らない理由', sk({ ...okSk, unverifiable: [{ reason: 'other', raw_code: 'zz', code_norm: 'zz' }] }), /行の形が違う/);
    await variant('unverifiable が配列でない', sk({ ...okSk, unverifiable: 'x' }), /unverifiable が配列でない/);
    // C に無いコードの unverifiable・空のコードは通る (affected 0)
    await O.query('begin');
    try {
      await sk({ ...okSk, unverifiable: [{ reason: 'unknown_kind', raw_code: 'ＺＺ－９', code_norm: 'zz-9' }, { reason: 'empty_code', raw_code: '　', code_norm: null }] })(O);
      const r = (await O.query('select ops.widen_check_readonly($1::uuid, 1) as r', [AID])).rows[0].r;
      assert.equal(r.ok, true, JSON.stringify(r.problems)); assert.deepEqual([r.counts.unverifiable, r.counts.affected_existing_cdb], [2, 0]);
    } finally { await O.query('rollback'); }
    await variant('最終形 (セットに product_id)', (c) => c.query("update core.skus set product_id = (select p.product_id from core.products p where p.display_code = 'p0007') where code = 's0007'"), /区分と product_id の不整合が 1 件/);
    await variant('最終形 (セットでない親の構成)', (c) => c.query("insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) select 1, p.sku_id, c.sku_id, 1, 'ne' from core.skus p, core.skus c where p.code = 'p0007' and c.code = 'p0008'"),
      /セットでない親の構成が 1 件/);
  });

  await ta('[3] 夜間ロードの最中は widen が 5 秒で諦める (55P03)・写しの証拠が違えば拒む・保存の取引の途中は待ってから通る / [11] 本番の大きさで widen 2 秒以内・読むだけの判定と同じ答え', async () => {
    const ev = { load_commit_seq: prepared.commitSeq, build_id: 'b1', generation_id: 'g1', generation_no: 1 };
    // 夜間ロード = epoch の共有の鍵を持った取引
    await O2.query('begin'); await O2.query('select pg_advisory_xact_lock_shared(ops.master_ownership_lock_key())');
    const t = Date.now();
    await assert.rejects(W.widenOwnership(dbO, { attemptId: AID, companyId: 1, actor: 't', evidence: ev }), (e) => e.code === '55P03');
    const waited = Date.now() - t;
    assert.ok(waited >= 4500 && waited < 9000, `待った時間 ${waited} ms`);
    await O2.query('commit');
    // 写しの証拠 (activate と同じ): 読んだロードが prepared のロードでない・Number に丸めた番号・作り直しが無い
    await assert.rejects(W.widenOwnership(dbO, { attemptId: AID, companyId: 1, actor: 't', evidence: { ...ev, load_commit_seq: recovery.commitSeq } }), /evidence_invalid/);
    await assert.rejects(W.widenOwnership(dbO, { attemptId: AID, companyId: 1, actor: 't', evidence: { ...ev, load_commit_seq: String(Number(prepared.commitSeq)) } }), /evidence_invalid/);
    await assert.rejects(W.widenOwnership(dbO, { attemptId: AID, companyId: 1, actor: 't', evidence: { ...ev, build_id: '' } }), /evidence_invalid/);
    await assert.rejects(W.widenOwnership(pgAdapter(P), { attemptId: AID, companyId: 1, actor: 't', evidence: ev }), (e) => e.code === '42501');   // DB の持ち主だけ
    await WW.query("select ops.record_new_entry_gate('mc_before_widen', $1, $1, $2::jsonb)", [new Date(Date.now() - 1000).toISOString(), JSON.stringify(ZERO_GATE)]);   // [18] 用: widen の前に書いた照合 ② の結果
    const before = await check();
    assert.equal(before.ok, true, JSON.stringify(before.problems));
    // 保存の取引 (段階の共有の鍵 → マスタの書き込みの共有の鍵) の途中 = widen は待つ → 保存の commit の後に通る
    await O2.query('begin'); await O2.query("select pg_advisory_xact_lock_shared(hashtext('ops.master_cutover'))"); await O2.query('select pg_advisory_xact_lock_shared(core.master_write_lock_key())');
    const t1 = Date.now();
    const w = launch(W.widenOwnership(dbO, { attemptId: AID, companyId: 1, actor: '中原', evidence: ev }));
    await sleep(600);
    assert.equal(w.done, false, 'widen は保存の取引を待つ');
    const t2 = Date.now();
    await O2.query('commit');
    const wr = await w.promise; assert.ok(wr.ok, wr.err?.message);
    const took = Date.now() - t2;
    console.log(`  (widen の取引: 保存を待った後 ${took} ms・全体 ${Date.now() - t1} ms・SKU ${(await q('select count(*)::int as n from core.skus'))[0].n})`);
    assert.ok(took < 2000, `widen の取引 ${took} ms (2 秒以内)`);
    assert.deepEqual([wr.ok.widened, wr.ok.added_keys, wr.ok.loads.prepared.commit_seq], [true, ['skus.sku_kind'], prepared.commitSeq]);
    assert.deepEqual(wr.ok.counts, before.counts);   // 読むだけの判定と同じ数
    // 書いた結果: active ← prepared・prepared を消す・試み widened・段階は new_open のまま owner_hash = 新しい active・出来事
    const st = await OS.readOwnershipState(dbO);
    assert.deepEqual([st.active.hash, st.prepared], [H_WIDEN, null]);
    assert.deepEqual(await q('select phase, owner_hash from ops.master_cutover_state'), [{ phase: 'new_open', owner_hash: H_WIDEN }]);
    assert.equal((await q('select state from ops.master_widen_attempts where widen_prepare_id = $1', [AID]))[0].state, 'widened');
    assert.equal((await q("select count(*)::int as n from ops.master_ownership_events where action = 'widen'"))[0].n, 1);
    const ce = (await q("select from_phase, to_phase, jsonb_array_length(acks) as n from ops.master_cutover_events order by event_id desc limit 1"))[0];
    assert.deepEqual(ce, { from_phase: 'new_open', to_phase: 'new_open', n: 2 });
    const wev = (await q("select detail from ops.master_widen_events where widen_prepare_id = $1 and action = 'widen'", [AID]))[0].detail;
    assert.deepEqual([wev.loads.recovery.commit_seq, wev.loads.prepared.commit_seq, wev.loads.recovery.run_id], [recovery.commitSeq, prepared.commitSeq, recovery.runId]);
    assert.equal((await q('select ops.sku_kind_locked() as l'))[0].l, true);
    await assert.rejects(W.widenOwnership(dbO, { attemptId: AID, companyId: 1, actor: 't', evidence: ev }), /widen_rejected: .*attempt_not_prepared/);   // 2 回目
    await assert.rejects(prep(), /widen_nothing/);   // もう足すキーが無い
  });

  await ta('[4] widen の後の G18: 区分の UPDATE・DELETE・DELETE → INSERT を持ち主 (= 夜間ロード) と画面のロールで拒む・ほかの列は今までどおり / G24 の偽の印を拒む', async () => {
    const code = async (c, sql, p) => (await errOf(c, sql, p))?.code ?? null;
    assert.equal(await code(O, "update core.skus set sku_kind = 'set' where code = 'p0001'"), '42501');
    assert.equal(await code(O, "delete from core.skus where code = 'x0001'"), '42501');
    await O.query('begin');
    try { assert.match(String((await errOf(O, "delete from core.skus where code = 'x0002'"))?.message), /sku_kind_locked/); } finally { await O.query('rollback'); }   // DELETE → INSERT の 1 文目で止まる
    assert.equal(await code(E, "update core.skus set sku_kind = 'set' where code = 'p0001'"), '42501');
    assert.equal(await code(O, "update core.skus set name = name || '' where code = 'p0001'"), null);   // 区分でない列は通る
    // G24: 本物の印は同じ取引だけ・区分を変えられる (最終形を満たす形で)
    await O.query('begin');
    await O.query("select ops.begin_master_maintenance('NE の区分に合わせる (試験)')");
    await O.query("update core.skus set sku_kind = 'exception', product_id = null where code = 'p4998'");
    await O.query('commit');
    assert.equal((await q("select sku_kind from core.skus where code = 'p4998'"))[0].sku_kind, 'exception');
    const fake = async (label, setup) => {
      await O.query('begin');
      try { await setup(O); const e = await errOf(O, "update core.skus set sku_kind = 'set' where code = 'p4997'"); assert.match(String(e?.message), /sku_kind_locked/, label); } finally { await O.query('rollback'); }
    };
    await fake('GUC だけ', (c) => c.query("select set_config('ops.master_maintenance', gen_random_uuid()::text, true)"));
    const old = (await q('select mark_id::text as id from ops.master_maintenance_marks order by created_at desc limit 1'))[0].id;
    await fake('前の取引の印 (復元で戻った古い印と同じ)', (c) => c.query("select set_config('ops.master_maintenance', $1, true)", [old]));
    await fake('セッションのユーザーが違う印', async (c) => {
      await c.query('alter table ops.master_maintenance_marks disable trigger trg_master_maintenance_marks_guard');
      const id = (await c.query("insert into ops.master_maintenance_marks (mark_id, txid, session_role, reason) values (gen_random_uuid(), txid_current(), 'watcher', 'x') returning mark_id::text as id")).rows[0].id;
      await c.query('alter table ops.master_maintenance_marks enable trigger trg_master_maintenance_marks_guard');
      await c.query("select set_config('ops.master_maintenance', $1, true)", [id]);
    });
    assert.match(String((await errOf(O, "insert into ops.master_maintenance_marks (mark_id, txid, session_role, reason) values (gen_random_uuid(), txid_current(), session_user, 'x')"))?.message), /maintenance_mark_forged/);
    assert.equal(await code(O, "select ops.begin_master_maintenance('')"), '22023');
    assert.equal(await code(E, "select ops.begin_master_maintenance('x')"), '42501');
  });

  await ta('[5] G19: 行の最終形を commit のときに守る (保守の印でも)・取引の途中の崩れは commit までに直せば通る', async () => {
    const commitErr = async (fn) => { await O.query('begin'); try { await fn(O); await O.query('commit'); return null; } catch (e) { try { await O.query('rollback'); } catch { /* */ } return e; } };
    let e = await commitErr(async (c) => { await c.query("select ops.begin_master_maintenance('x')"); await c.query("update core.skus set sku_kind = 'set' where code = 'p4997'"); });
    assert.equal(e?.code, '23514', e?.message); assert.match(e.message, /sku_kind_shape/);
    e = await commitErr((c) => c.query("insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) select 1, p.sku_id, c.sku_id, 1, 'ne' from core.skus p, core.skus c where p.code = 'p0010' and c.code = 'p0011'"));
    assert.equal(e?.code, '23514', e?.message);
    e = await commitErr((c) => c.query("update core.skus set product_id = (select p.product_id from core.products p where p.display_code = 'p0012') where code = 'x0012'"));   // 区分を変えなくても最終形は守る (例外に product_id)
    assert.equal(e?.code, '23514', e?.message); assert.match(e.message, /sku_kind_shape/);
    e = await commitErr(async (c) => { await c.query("select ops.begin_master_maintenance('x')"); await c.query("update core.skus set sku_kind = 'exception' where code = 's0001'"); });
    assert.equal(e?.code, '23514', e?.message); assert.match(e.message, /セットでない SKU/);   // セット → 例外 (構成が残る)
    e = await commitErr(async (c) => {   // 途中で崩れても commit までに直せば通る
      await c.query("select ops.begin_master_maintenance('x')");
      await c.query("update core.skus set sku_kind = 'exception' where code = 'p4996'");
      await c.query("update core.skus set product_id = null where code = 'p4996'");
    });
    assert.equal(e, null, e?.message);
  });

  await ta('[6] 今の origin/master のロードを widen の後に: 区分の変わった材料 (単品 → セット・セット → 単品) は取引ごと失敗 / 変わらない材料は通る', async () => {
    const last = (await OS.latestLoadCommit(dbO)).commit_seq;
    await assert.rejects(runInitialLoad(dbO, planOf({ kindOf: { p4000: 'set' }, extraComponents: [{ parentCode: 'p4000', childCode: 'p4001', qty: 1, source: 'ne' }] }), { log: () => {}, runId: 'wpg_load_6a', host: 'test' }),
      /sku_kind_shape/);
    await assert.rejects(runInitialLoad(dbO, planOf({ kindOf: { s2299: 'single' } }), { log: () => {}, runId: 'wpg_load_6b', host: 'test' }), /sku_kind_shape|sku_kind_locked/);
    assert.equal((await OS.latestLoadCommit(dbO)).commit_seq, last);   // 何も残らない
    assert.deepEqual(await q("select code, sku_kind from core.skus where code in ('p4000', 's2299') order by code"), [{ code: 'p4000', sku_kind: 'single' }, { code: 's2299', sku_kind: 'set' }]);
    // 区分の変わらない材料 (p4998・p4996 は保守で例外にした = 材料も例外にそろえる)
    const ok = await runInitialLoad(dbO, { ...planOf(), skus: planOf().skus.map((s) => (['p4998', 'p4996'].includes(s.code) ? exc(s.code) : s)) }, { log: () => {}, runId: 'wpg_load_6c', host: 'test' });
    assert.equal(ok.ok, true, ok.error); assert.equal(typeof ok.load_commit_seq, 'string');
  });

  await ta('[7] 復元の 3 種類: sku_kind が load のダンプ (最終形が崩れていても) = 通る / company で整合 = 通る / company で不整合 = 復元全体が rollback', async () => {
    const T = await openPgClient(u2.toString()); clients.push(T);
    const dbT = pgAdapter(T);
    await applyMigrations(dbT, { log: () => {} });
    const r1 = await restoreCompanyDb(dbT, dumpLoad, { log: () => {} });
    assert.deepEqual(r1.kindShape, { single_product_mismatch: 1, non_set_parent_components: 0, locked: false });
    const dumpOk = await dumpText();
    const r2 = await restoreCompanyDb(dbT, dumpOk, { log: () => {} });
    assert.deepEqual(r2.kindShape, { single_product_mismatch: 0, non_set_parent_components: 0, locked: true });
    assert.equal((await T.query('select ops.sku_kind_locked() as l')).rows[0].l, true);
    // 不整合を含む company のダンプ (trigger を止めて作る = G19 を通っていない行)
    await O.query('begin');
    await O.query('alter table core.skus disable trigger user');
    await O.query("update core.skus set product_id = (select p.product_id from core.products p where p.display_code = 'p0020') where code = 's0020'");
    await O.query('alter table core.skus enable trigger user');
    await O.query('commit');
    const dumpBad = await dumpText();
    await O.query("update core.skus set product_id = null where code = 's0020'");   // 元へ (最終形が 0 に戻る = G19 を通る)
    const nBefore = (await T.query('select count(*)::int as n from ops.master_widen_events')).rows[0].n;
    await assert.rejects(restoreCompanyDb(dbT, dumpBad, { log: () => {} }), (e) => e.code === 'RESTORE_KIND_SHAPE' && /restore_kind_shape/.test(e.message));
    assert.equal((await T.query("select product_id is null as ok from core.skus where code = 's0020'")).rows[0].ok, true);   // 前の中身のまま (全体が rollback)
    assert.equal((await T.query('select count(*)::int as n from ops.master_widen_events')).rows[0].n, nBefore);
  });

  await ta('[18] 開放の許可 (lease・最小の計画 §3): 照合 ② の始めに閉じる → 結果 → 今回の回で grant・権限 (grant = new_entry_gate だけ・閉じる / 結果 = watch_writer だけ)・取り消しは保存の取引を待つ・停止の床・新しい結果で前の許可は無効・結果を書く前に落ちた再実行では出ない・期限の式', async () => {
    const at = (ms = -1000) => new Date(Date.now() + ms).toISOString();
    const close = (run) => WW.query('select ops.close_new_entry_for_compare($1) as r', [run]).then((x) => x.rows[0].r);
    const result = (run, kg = ZERO_GATE) => WW.query('select ops.record_new_entry_gate($1, $2, $2, $3::jsonb) as r', [run, at(), JSON.stringify(kg)]).then((x) => x.rows[0].r);
    const grant = (c, run) => c.query("select ops.grant_new_entry_lease('single', $1) as r", [run]).then((x) => x.rows[0].r);
    const valid = async (c = E) => (await c.query("select ops.new_entry_lease_valid('single') as v")).rows[0].v;
    // widen の前に書いた結果 (R0・[3] の前) では許可が出ない
    await assert.rejects(grant(NG, 'mc_before_widen'), /lease_denied: .*before_widen/);
    // 1 回の照合: 始めに閉じる → 結果 → grant (new_entry_gate だけ・ほかのロールは 42501)
    await close('mc_lease_1'); await result('mc_lease_1');
    for (const [c, who] of [[WW, 'watch_writer'], [E, 'master_edit'], [WA, 'watcher']]) {
      const e = await errOf(c, "select ops.grant_new_entry_lease('single', 'mc_lease_1')"); assert.equal(e?.code, '42501', `${who}: ${e?.message}`);
    }
    for (const [c, sql] of [[NG, "select ops.record_new_entry_gate('x', '2030-01-01T00:00:00Z', '2030-01-01T00:00:00Z', '{}'::jsonb)"], [NG, "select ops.close_new_entry_for_compare('x')"],
      [E, "select ops.close_new_entry_for_compare('x')"], [WA, "select ops.close_new_entry_for_compare('x')"], [NG, 'select ops.widen_check_readonly(gen_random_uuid(), 1)'],
      [NG, "select ops.new_entry_lease_valid('single')"], [NG, 'select * from ops.new_entry_gate_results']]) {
      const e = await errOf(c, sql); assert.equal(e?.code, '42501', `${sql} (${e?.message})`);
    }
    const l1 = await grant(NG, 'mc_lease_1');
    assert.equal(typeof l1.lease_id, 'string'); assert.equal(l1.compare_run_id, 'mc_lease_1');
    assert.equal(await valid(E), true); assert.equal(await valid(WA), true);
    // 期限 = 東京の今日の翌日 10:00 (DB と session の TimeZone に左右されない)
    const exp = async (c, t) => (await c.query("select to_char(ops.new_entry_lease_expiry($1::timestamptz) at time zone 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI') as e", [t])).rows[0].e;
    await O.query("set timezone = 'America/Los_Angeles'");
    assert.equal(await exp(O, '2030-01-10T23:59:00+09:00'), '2030-01-11 10:00');
    assert.equal(await exp(O, '2030-01-10T00:01:00+09:00'), '2030-01-11 10:00');
    await O.query('reset timezone');
    // 取り消しは保存の取引 (種類の共有の鍵) の完了を待つ → その後の保存は閉じる。許可の有無によらず停止の床を足す
    await O2.query('begin'); await O2.query("select ops._require_new_entry_lease('single')");
    const rv = launch(NG.query("select ops.revoke_new_entry_lease('single', 'ゲートの失敗 (試験)') as r"));
    await sleep(500);
    assert.equal(rv.done, false, '取り消しは保存の取引を待つ');
    await O2.query('commit');
    const rvr = await rv.promise; assert.ok(rvr.ok, rvr.err?.message);
    const maxR = async () => (await O.query('select max(result_id)::text as m from ops.new_entry_gate_results')).rows[0].m;
    assert.deepEqual([rvr.ok.rows[0].r.revoked, rvr.ok.rows[0].r.floor_result_id], [1, await maxR()]);
    assert.equal(await valid(), false);
    await O2.query('begin');
    await assert.rejects(O2.query("select ops._require_new_entry_lease('single')"), /new_entry_closed/);
    await O2.query('rollback');
    await assert.rejects(grant(NG, 'mc_lease_1'), /stop_floor/);   // 同じ結果でゲートを流し直しても開かない
    // 照合 ② の始めに閉じる = 保存の取引を待つ・前日の許可は照合の途中で無効・結果を書く前に落ちた再実行では出ない (同じ日のもっと前の成功の行は床より古い)
    await close('mc_lease_2'); await result('mc_lease_2'); await grant(NG, 'mc_lease_2');
    assert.equal(await valid(), true);
    await O2.query('begin'); await O2.query("select ops._require_new_entry_lease('single')");
    const cl = launch(close('mc_lease_3'));
    await sleep(500);
    assert.equal(cl.done, false, '閉じるのは保存の取引を待つ');
    await O2.query('commit');
    const clr = await cl.promise; assert.ok(clr.ok, clr.err?.message); assert.equal(clr.ok.revoked, 1);
    assert.equal(await valid(), false, '照合 ② の途中 = 前の許可は無効');
    await assert.rejects(grant(NG, 'mc_lease_2'), /stop_floor|compare_run_mismatch/);          // 今回の照合が結果を書く前に落ちた = 前の成功の行では出ない
    await assert.rejects(grant(NG, 'mc_lease_3'), /compare_run_mismatch/);                     // 今回の回の結果がまだ無い
    await result('mc_lease_3', { ...ZERO_GATE, raw_mismatch: 1 });
    await assert.rejects(grant(NG, 'mc_lease_3'), /kind_gate: .*raw_mismatch/);                // 区分のずれ = 出さない
    // 再試行も同じ組 (新しい回で 閉じる → 結果 → grant)
    await close('mc_lease_4'); await result('mc_lease_4'); await grant(NG, 'mc_lease_4');
    assert.equal(await valid(), true);
    // 新しい結果の行 (閉じる無しでも) = 前の許可は無効
    await result('mc_lease_5');
    assert.equal(await valid(), false);
    await assert.rejects(grant(NG, 'mc_lease_4'), /compare_run_mismatch/);
    // 最終形が崩れた (取引の途中) = 出さない
    await O.query('begin');
    await O.query("update core.skus set product_id = (select p.product_id from core.products p where p.display_code = 'p0030') where code = 'x0030'");
    const s9e = await errOf(O, "select ops.grant_new_entry_lease('single', 'mc_lease_5')");
    await O.query('rollback');
    assert.match(String(s9e?.message), /shape/);
    await grant(NG, 'mc_lease_5');
    assert.equal(await valid(), true);
  });

  console.log(`${passed} 件 PASS`);
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  for (const n of [dbName, db2Name]) { try { await admin.query(`drop database ${n} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); } }
  await admin.end();
}
