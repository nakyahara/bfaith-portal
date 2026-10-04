/**
 * test-master-edit-pg.mjs — マスタの入力 (lib/master-write.mjs・0051) の**同時実行**と**ロールの権限**を、実 PostgreSQL の独立した接続で確かめる
 *   (PGlite は 1 接続なので書けない。Codex ⑤-R1 H4・PR #1563 R1 H3 / M6・R2 H1 / H2 / M3 / M4 / M5 / M6 / M8 の試験)
 *
 * 接続は本番と同じロールでログインする (create-master-edit-roles.mjs で作る・パスワードは試験の回ごと):
 *   保存 (A・B・X) = master_edit / 門の記録 (GR・GM) = master_gate_render・master_gate_minipc / 段階を進める (P) = master_ops / NE の観測 (V) = master_observer /
 *   持ち主 (O・O2) = 夜間ロード・構成の依頼を上げる・ほかの処理の代わり
 * 固定する契約:
 *   1 同じ SKU を 2 人が同じ画面から保存: 後の人は SKU の鍵で待ち、前の人の commit の後に 409 (編集の印が違う)。両方は書かない
 *   2 同じ request_id が 2 つ並んで来る (押し直し): 後の方は request_id の鍵で待ち、前の方の結果をそのまま返す (記録は 1 行・変更も 1 回)
 *   3 同じ request_id を違う SKU へ同時に: 後の方は request_id の鍵で待ち、409 request_id_reused (500 にしない)
 *   4 画面を開いた後に別の接続が仕入先ごとの商品の行を足した (phantom) = 409
 *   5 CSV を作る側が CSV の鍵を持っている間は保存が待ち、その CSV (作った = 出ている) の列を変える保存は 409 csv_issued
 *   6 保存の途中で接続が切れた: 何も残らない (処理中の行を作らない) = 同じ request_id でもう一度保存できる
 *   7 切替の段階を変える取引 (排他の鍵) の途中は、保存が段階の共有の鍵で待つ
 *   8 構成の依頼を上げる × 単品の保存 (足す構成品): どちらが先でも、セットの値は新しい単品の値 / 後の保存は含むセットが増えたことに気づく
 *   9 仕入先を止める × 代表の仕入先にする: 止める側が先 = 保存は仕入先の行の鍵で待ってから 400 (取引停止) / 保存が先 = 止める側が待つ
 *  10 同じ request_id の失敗する保存が 2 つ並ぶ: 失敗の記録は 1 行だけ・両方とも同じ誤り (片方は記録を返す)
 *  11 上げる側が鍵を待つ間に「依頼にだけある構成品」の原価が 0 になった: 鍵の後に読み直して上げない (core は変えない・underivable)
 *  12 画面を開いた後に仕入先の有効が変わった = 編集の印が違う (409)
 *  13 ロールの権限: 変更の記録 (events) の偽の insert・version・仕入先の行の鍵・門・段階・観測を、渡していないロールからは 42501
 *   (切替は master_gate_render / master_gate_minipc の記録と master_ops の段階の関数で開く = 門の記録の約束を本物の権限で通す。観測は master_observer が書く)
 *   0 門の記録の場所はログインで決まる (Render のログインで minipc を名乗る = 42501)・まとめのロール master_gate ではログインできない・
 *     黙っているプロセス (24 時間以内に記録・最後が 15 分より前) は段階を止める → 止まった記録 (stopped) で外れる (#1563 仮レビュー Low 3)
 *  14 保存を開いていない = ほかの鍵を取る前に 409 (CSV の鍵・夜間ロードの鍵を持つ人がいても待たない。仮レビュー Low 1)
 *  15 夜間ロード × 保存 (仮レビュー M3): ロードが先 = 保存・昇格はマスタの書き込みの鍵で短く待って 409 nightly_load → ロードは最後まで /
 *     保存が先 = ロードは鍵で待ってから最後まで (行の鍵で待ち合わない = デッドロックしない)
 *  16 NE の観測の書き込み × 古い観測の昇格 (R3 M4): 昇格がセットの鍵を持っている間、新しい観測の書き込みは待つ (昇格の後に書かれる)
 *  17 画面のロールの直接の書き込み (R3 M2・R4 M2): begin_master_write の前は 42501・約束の相手でない SKU も 42501・偽の core.actor_* は記録に残らない
 *  18 新商品の NE 登録の CSV を作る取引 × 同じ商品の保存 (⑤-2b・0053): 保存は SKU の鍵で待ち、作った後の CSV (まだ配っていない) を「使わない」にしてから保存する
 *  19 JAN を 2 つの商品に同時に付ける (⑤-2b): 後の方は一意の索引で待ち、前の方の commit の後に 409 jan_taken (500 にしない・両方は付かない)
 *  20 同じファイルの「取り込んだ」の申告が 2 つ並ぶ (⑤-2b・DB の関数 ops.ne_reg_declare): 後の方は鍵で待ち「もう一度」・NE 登録待ちへは 1 回だけ・画面のロールは表を直接書けない・
 *     約束を書く部品の関数 (ops.open_reg_write) を呼べない・JAN の行を直接書けない・申告の約束 (reg_csv_declare) は関数が書く (0053・#1571 R1 High 3)
 *  (9・12 は 0053 で: 代表の仕入先に使っている仕入先は止められない = 待った後に supplier_in_use / 付け替えてから止める)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-edit-pg.mjs
 *   (この PC では C:/tmp/pg-embed の run-conc.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロール master_* をクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。
 *   package.json の test:company-db には入れない (PostgreSQL が要る)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS as OWNED_COLUMNS_FOR_BASE } from '../config/master-ownership.mjs';
// 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLUMNS_FOR_BASE.map((k) => [k, 'load'])));

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (実 PostgreSQL の同時実行の試験は飛ばす。PGlite の試験は scripts/test-master-edit.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const R = await import('../lib/master-register.mjs');
const G = await import('../lib/master-reg-csv.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/** 結果を待たずに投げる (待っている = done が false のまま) */
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
/** 外から開ける扉 (commit の直前で止める) */
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };
const denied = async (client, sql, label) => { await assert.rejects(() => client.query(sql), (e) => { assert.equal(e.code, '42501', `${label}: ${e.code} ${e.message}`); return true; }, label); };

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const NOW = new Date('2030-01-10T03:00:00Z');
/** ⑤-3 の manifest の形 ({ entries: [{ id, kind }] }・ASCII の id。手の入口 = ne:item-screen・gas:logizard-sheet-and-sku-map) */
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }, { id: 'gas:logizard-sheet-and-sku-map', kind: 'manual' }] };
/** 証拠の時刻は今の段階に入った後・サーバーの今以前 (R3 High 1) = 進める直前に作る */
const manualStopped = () => [{ id: 'gas:logizard-sheet-and-sku-map', by: 't', at: new Date().toISOString() }, { id: 'ne:item-screen', by: 't', at: new Date().toISOString() }];
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];   // ログインできるロール (master_gate はまとめ・ログインなし)
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const dbName = `cdb_me_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const O = await open(null);
const clients = [O];
try {
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: PW });
  const [A, B, GR, GM, P, V, O2] = [await open('master_edit'), await open('master_edit'), await open('master_gate_render'), await open('master_gate_minipc'),
    await open('master_ops'), await open('master_observer'), await open(null)];
  clients.push(A, B, GR, GM, P, V, O2);
  const [dbA, dbB, dbP, dbV] = [A, B, P, V].map(pgAdapter);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };   // 門の記録は場所ごとのログイン
  assert.deepEqual((await A.query('select session_user::text as s, current_user::text as c')).rows[0], { s: 'master_edit', c: 'master_edit' });
  // ロールの設定 (画面の接続と同じ。#1563 仮レビュー Low 4)
  assert.deepEqual((await A.query(`select current_setting('lock_timeout') as l, current_setting('idle_in_transaction_session_timeout') as i`)).rows[0], { l: '10s', i: '1min' });

  const single = (code, cost) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: cost, source: 'ne', status: 'COMPLETE' } });
  const plan = {
    skus: [single('p001', 100), single('p002', 200), single('p003', 300), single('p004', 400),
      { code: 'ps01', name: 'セット', kind: 'set', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: null, cost: { jpy: 100, source: 'set_calc', status: 'COMPLETE' } }],
    variationGroups: [], setComponents: [{ parentCode: 'ps01', childCode: 'p001', qty: 1, source: 'ne' }], listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }], supplierSkus: [{ supplierCode: '0001', skuCode: 'p001' }],
  };
  const r0 = await runInitialLoad(dbO, plan, { log: () => {}, runId: 'load_pg_1', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  // 切替を開く: 門の記録は場所ごとのログイン (master_gate_render / master_gate_minipc)、段階は master_ops (本物の権限で・本番の関数そのまま)
  const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(MASTER_OWNERSHIP);
  const mh = await C.manifestHashOf(dbO, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  const frozenEvidence = { expected_builds: builds, manifest_hash: mh, owner_hash: legacy, manual_entries_stopped: manualStopped(), drain: { done: true, checked_by: 't', checked_at: new Date().toISOString() } };
  await ta('[0] 門の記録の場所はログインで決まる・まとめのロールではログインできない・黙っているプロセスは段階を止める → 止まった記録で外れる (→ frozen)', async () => {
    const ack = (db, host, extra = {}) => C.recordLegacyGateAck(db, { host, instanceId: 'r-x', buildId: 'r1', manifest: MANIFEST, ownership: MASTER_OWNERSHIP, phaseSeen: 'legacy_open', ...extra });
    await assert.rejects(() => ack(dbGate.render, 'minipc'), (e) => { assert.equal(e.code, '42501'); assert.match(e.message, /gate_host_mismatch/); return true; });
    await assert.rejects(() => ack(dbGate.minipc, 'render'), (e) => e.code === '42501' && /gate_host_mismatch/.test(e.message));
    await assert.rejects(() => ack(dbO, 'render'), (e) => e.code === '42501' && /gate_host_mismatch/.test(e.message));   // 持ち主のロールでも書けない
    await assert.rejects(async () => { const c = await openPgClient(roleUrl('master_gate_render').replace('master_gate_render', 'master_gate')); await c.end(); }, /master_gate|login|password/i);
    // 黙っているプロセス (1 時間前の記録だけ) = 拒む
    await O.query(`insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count, session_role, acked_at)
      values ('render', 'r-old', 'r1', $1, $2, 'legacy_open', 0, 'master_gate_render', clock_timestamp() - interval '1 hour')`, [mh, legacy]);
    await assert.rejects(() => C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: frozenEvidence }), /render\/r-old: 黙っている/);
    assert.equal((await O.query('select phase from ops.master_cutover_state')).rows[0].phase, 'legacy_open');
    // 止まった記録 (Render のログインで r-old を止めたと書く) = 外れる → frozen
    const st = await C.recordLegacyGateAck(dbGate.render, { host: 'render', instanceId: 'r-old', buildId: 'r1', manifest: MANIFEST, ownership: MASTER_OWNERSHIP, phaseSeen: 'legacy_open',
      stopped: true, stoppedReason: 'Render の古い instance を止めた (試験)' });
    assert.equal(st.stopped, true);
    // 止まった記録は書きかけ 0 のときだけ (R4 High 1)
    await assert.rejects(() => C.recordLegacyGateAck(dbGate.render, { host: 'render', instanceId: 'r-old', buildId: 'r1', manifest: MANIFEST, ownership: MASTER_OWNERSHIP, phaseSeen: 'legacy_open',
      stopped: true, stoppedReason: 'x', inflightCount: 1, oldestInflightAt: new Date().toISOString() }), /書きかけ 0 のときだけ/);
    const r = await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: frozenEvidence });
    assert.deepEqual(r.acks.map((a) => [a.host, a.instance_id, a.stopped ?? false]), [['minipc', 'm-a', false], ['render', 'r-a', false], ['render', 'r-old', true]]);
    assert.deepEqual((await q("select session_role, stopped, stopped_reason from ops.master_legacy_gate_acks where instance_id = 'r-old' order by ack_id")).map((x) => [x.session_role, x.stopped, !!x.stopped_reason]),
      [['master_gate_render', false, false], ['master_gate_render', true, true]]);
  });
  assert.equal((await O.query('select phase from ops.master_cutover_state')).rows[0].phase, 'frozen', '[0] で frozen に進んでいない');
  await acks(ALL_COMPANY, 'frozen');
  // 0055 (④a): company_owner に進むのは持ち主の epoch が active で段階の持ち主表と同じときだけ = 試験で置く (本番 = ④a の activate)
  await (await import('./fixtures/master-epoch.mjs')).seedActiveEpoch(dbO, ALL_COMPANY);
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(ALL_COMPANY, 'company_owner');
  // 0052 (⑤-2a): new_open の前に既存の SKU の登録の状態 (backfill) が要る (運用のロールで)
  { const p = (await dbP.query('select * from ops.registration_backfill_plan()')).rows[0]; await dbP.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 't@test']); }
  await C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  assert.equal((await O.query('select phase from ops.master_cutover_state')).rows[0].phase, 'new_open');

  const tokenOf = async (code) => W.editTokenOf(await W.readCurrent(dbO, (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id, '2030-01-10'));
  const save = (db, code, values, { token, requestId = crypto.randomUUID(), beforeCommit, open = true } = {}) =>
    W.saveSku(db, { actor: 'naka@test', requestId, code, reason: 'pg', seen: { token }, values }, { ownership: ALL_COMPANY, open, now: NOW, beforeCommit });

  await ta('[1] 同じ SKU を 2 人が同じ画面から保存 = 後の人は待ってから 409 (両方は書かない)', async () => {
    const token = await tokenOf('p001');
    const g = gate();
    const a = launch(save(dbA, 'p001', { name: 'A の名前' }, { token, beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'p001', { name: 'B の名前' }, { token }));
    await sleep(500);
    assert.equal(b.done, false, 'B は SKU の鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'version_conflict', rb.err?.message);
    assert.equal((await q("select name from core.skus where code = 'p001'"))[0].name, 'A の名前');
    assert.deepEqual((await q("select distinct db_user from events.master_change_events where source_system = 'portal_master_edit'")).map((r) => r.db_user), ['master_edit']);
  });

  await ta('[2] 同じ request_id が 2 つ並んで来る = 後の方は前の方の結果 (記録 1 行・変更 1 回)', async () => {
    const token = await tokenOf('p002');
    const id = crypto.randomUUID();
    const g = gate();
    const a = launch(save(dbA, 'p002', { standard_price: '1234' }, { token, requestId: id, beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'p002', { standard_price: '1234' }, { token, requestId: id }));
    await sleep(500);
    assert.equal(b.done, false);
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.ok?.replayed, true, rb.err?.message);
    assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [id]))[0].n), 1);
    assert.equal(Number((await q("select count(*)::int as n from events.master_change_events where request_id = $1 and attribute = 'standard_price_jpy'", [id]))[0].n), 1);
  });

  await ta('[3] 同じ request_id を違う SKU へ同時に = 後の方は待ってから 409 request_id_reused (500 にしない)', async () => {
    const id = crypto.randomUUID();
    const g = gate();
    const a = launch(save(dbA, 'p002', { standard_price: '1300' }, { token: await tokenOf('p002'), requestId: id, beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'p003', { standard_price: '1300' }, { token: await tokenOf('p003'), requestId: id }));
    await sleep(500);
    assert.equal(b.done, false, 'B は request_id の鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'request_id_reused', rb.err?.message);
    assert.notEqual(Number((await q("select standard_price_jpy from core.skus where code = 'p003'"))[0].standard_price_jpy), 1300);
  });

  await ta('[4] 画面を開いた後に別の接続が行を足した (phantom: 仕入先ごとの商品) = 409', async () => {
    const token = await tokenOf('p002');
    await O.query("insert into core.supplier_skus (company_id, supplier_id, sku_id) select 1, s.supplier_id, k.sku_id from core.suppliers s, core.skus k where s.code = '0002' and k.code = 'p002'");
    await assert.rejects(() => save(dbA, 'p002', { name: '直す' }, { token }), (e) => e.reason === 'version_conflict');
  });

  await ta('[5] CSV を作る側が CSV の鍵を持っている間は保存が待つ → 作った CSV (出ている) の列は 409 csv_issued', async () => {
    const token = await tokenOf('p001');
    await O.query('begin');
    await O.query("select pg_advisory_xact_lock(hashtext('ops.ne_csv'))");
    const ex = (await O.query(`insert into ops.ne_csv_exports (kind, col, ne_column, converter_version, encoding, trial, row_count, sha256, file_bytes, compare_run_id, created_by)
      values ('products', 'name', 'syohin_name', 'v1', 'utf8', true, 1, repeat('a', 64), decode('00', 'hex'), 'mc_20300101T000000000Z_abcdef', 'x@test') returning export_id`)).rows[0].export_id;
    await O.query(`insert into ops.ne_csv_export_rows (export_id, source, code_norm, col, ne_code, target, cell, cdb_version, evidence) values ($1, 'to_ne', 'p001', 'name', 'p001', '{"value":"x"}', 'x', 1, '{}')`, [ex]);
    const a = launch(save(dbA, 'p001', { name: 'CSV の後' }, { token }));
    await sleep(500);
    assert.equal(a.done, false, '保存は CSV の鍵で待つ');
    await O.query('commit');
    const ra = await a.promise;
    assert.equal(ra.err?.reason, 'csv_issued', ra.err?.message);
  });

  await ta('[6] 保存の途中で接続が切れた = 何も残らない (処理中の行なし) → 同じ request_id でもう一度保存できる', async () => {
    const X = await openPgClient(roleUrl('master_edit'));
    X.on('error', () => {});
    try {
    const token = await tokenOf('p002');
    const id = crypto.randomUUID();
    const pid = (await X.query('select pg_backend_pid() as p')).rows[0].p;
    let terminated = null;
    const x = launch(save(pgAdapter(X), 'p002', { name: '切れる保存' }, { token, requestId: id,
      beforeCommit: async () => { terminated = (await O.query('select pg_terminate_backend($1, 5000) as t', [pid])).rows[0].t; } }));   // 切れるまで待つ (PG14+)
    const rx = await x.promise;
    assert.equal(terminated, true, '接続を切れた');
    assert.ok(rx.err, '切れた保存は失敗');
    assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [id]))[0].n), 0);
    assert.notEqual((await q("select name from core.skus where code = 'p002'"))[0].name, '切れる保存');
    const again = await save(dbA, 'p002', { name: '切れる保存' }, { token: await tokenOf('p002'), requestId: id });
    assert.equal(again.ok, true);
    } finally { try { await X.end(); } catch { /* 切れている */ } }
  });

  await ta('[7] 切替の段階を変える取引 (排他の鍵) の途中は、保存が段階の共有の鍵で待つ', async () => {
    await O.query('begin');
    await O.query("select pg_advisory_xact_lock(hashtext('ops.master_cutover'))");
    const a = launch(save(dbA, 'p001', { reorder_months: '3' }, { token: await tokenOf('p001') }));
    await sleep(500);
    assert.equal(a.done, false, '保存は段階の鍵で待つ');
    await O.query('rollback');
    const ra = await a.promise;
    assert.ok(ra.ok, ra.err?.message);
  });

  let runSeq = 0;
  /** NE の観測を 1 回分 (観測のロール master_observer で・完全な回) */
  const observe = async (rows, at) => {
    const runId = `ne_pg_${++runSeq}`;
    await W.recordNeSetObservations(dbV, { run_id: runId, observed_at: at, complete: true, requested: 1, fetched: 1, raw_hash: 'c'.repeat(64), source_generation: runId, sets: [{ set_code: 'ps01', rows }] });
    return (await q('select observation_id::text as id from ops.ne_set_observations where run_id = $1', [runId]))[0].id;
  };
  const setCost = async () => Number((await q("select c.cost_jpy from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = 'ps01' and c.valid_to is null"))[0]?.cost_jpy);
  const setComps = async () => (await q("select k.code, c.qty, c.sort_order from core.sku_components c join core.skus s on s.sku_id = c.parent_sku_id join core.skus k on k.sku_id = c.child_sku_id where s.code = 'ps01' order by c.sort_order")).map((r) => `${r.code}x${r.qty}`);

  await ta('[8] 単品の保存が先 → 上げる側 (持ち主) は単品の鍵で待ち、新しい単品の値でセットを計算する', async () => {
    await save(dbA, 'ps01', { components: [{ code: 'p001', qty: 1 }, { code: 'p002', qty: 1 }] }, { token: await tokenOf('ps01') });
    const obsId = await observe([{ code: 'p001', qty: 1, sort: 1 }, { code: 'p002', qty: 1, sort: 2 }], new Date(Date.now() + 60000).toISOString());
    const g = gate();
    const a = launch(save(dbA, 'p002', { cost: { jpy: '250', reason: '先に直す' } }, { token: await tokenOf('p002'), beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(W.promoteComponentRequest(dbO, obsId, { ownership: ALL_COMPANY, now: NOW }));
    await sleep(500);
    assert.equal(b.done, false, '上げる側は p002 の鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.ok?.promoted, true, JSON.stringify(rb.ok || rb.err?.message));
    assert.equal(await setCost(), 100 + 250);
  });

  await ta('[8] 上げる側が先 → 単品の保存は鍵で待ち、含むセットが増えたことに気づいて 409 (古い値のままのセットを残さない)', async () => {
    await save(dbA, 'ps01', { components: [{ code: 'p001', qty: 1 }, { code: 'p002', qty: 1 }, { code: 'p003', qty: 1 }] }, { token: await tokenOf('ps01') });
    const obsId = await observe([{ code: 'p001', qty: 1, sort: 1 }, { code: 'p002', qty: 1, sort: 2 }, { code: 'p003', qty: 1, sort: 3 }], new Date(Date.now() + 120000).toISOString());
    const token = await tokenOf('p003');
    const g = gate();
    const b = launch(W.promoteComponentRequest(dbO, obsId, { ownership: ALL_COMPANY, now: NOW, beforeCommit: g.wait }));
    await sleep(300);
    const a = launch(save(dbA, 'p003', { cost: { jpy: '333', reason: '後から直す' } }, { token }));
    await sleep(500);
    assert.equal(a.done, false, '単品の保存は p003 の鍵で待つ');
    g.open();
    assert.equal((await b.promise).ok?.promoted, true);
    const ra = await a.promise;
    assert.ok(['retry', 'version_conflict'].includes(ra.err?.reason), ra.err?.message || 'ok になった');
    assert.equal(await setCost(), 100 + 250 + 300);
    const s = await save(dbA, 'p003', { cost: { jpy: '333', reason: '読み直した' } }, { token: await tokenOf('p003') });
    assert.ok(s.derived.some((d) => d.code === 'ps01' && d.col === 'cost' && d.to === 100 + 250 + 333), JSON.stringify(s.derived));
  });

  await ta('[9] 仕入先を止める × 代表の仕入先にする: 止める側が先 = 保存は待ってから 400 (取引停止) / 保存が先 = 止める側が待つ', async () => {
    await O.query('begin');
    await O.query("update core.suppliers set active = false where code = '0002'");
    const a = launch(save(dbA, 'p003', { primary_supplier: '0002' }, { token: await tokenOf('p003') }));
    await sleep(500);
    assert.equal(a.done, false, '保存は仕入先の行の鍵で待つ');
    await O.query('commit');
    const ra = await a.promise;
    assert.equal(ra.err?.status, 400, ra.err?.message); assert.match(ra.err.message, /取引停止/);
    const g = gate();
    const a2 = launch(save(dbA, 'p003', { primary_supplier: '0001' }, { token: await tokenOf('p003'), beforeCommit: g.wait }));
    await sleep(300);
    const o = launch(O.query("update core.suppliers set active = false where code = '0001'"));
    await sleep(500);
    assert.equal(o.done, false, '止める側は保存が持つ仕入先の行の鍵で待つ');
    g.open();
    assert.ok((await a2.promise).ok);
    // 0053 (⑤-2b・Medium 3): 待った後、代表の仕入先に使われている (p003) と分かって止めない (付け替えてから止める = [12])
    const ro = await o.promise;
    assert.match(String(ro.err?.message), /supplier_in_use/, ro.err ? ro.err.message : '止められてしまった');
  });

  await ta('[10] 同じ request_id の失敗する保存が 2 つ並ぶ = 失敗の記録は 1 行だけ・両方とも同じ誤り', async () => {
    const id = crypto.randomUUID();
    await O.query('begin');
    await O.query("select pg_advisory_xact_lock(hashtextextended('ops.master_edit_request:' || $1::text, 0))", [id]);
    const bad = { token: 'a'.repeat(64), requestId: id };   // 編集の印が違う = 409 で失敗する
    const a = launch(save(dbA, 'p001', { name: '失敗' }, bad));
    const b = launch(save(dbB, 'p001', { name: '失敗' }, bad));
    await sleep(500);
    assert.deepEqual([a.done, b.done], [false, false]);
    await O.query('commit');
    const [ra, rb] = [await a.promise, await b.promise];
    assert.deepEqual([ra.err?.reason, rb.err?.reason], ['version_conflict', 'version_conflict']);
    assert.equal(Number((await q("select count(*)::int as n from ops.master_edit_requests where request_id = $1 and status = 'failed'", [id]))[0].n), 1);
    assert.equal(Number((await q('select count(*)::int as n from ops.master_edit_requests where request_id = $1', [id]))[0].n), 1);
  });

  await ta('[11] 上げる側が鍵を待つ間に「依頼にだけある構成品」の原価が 0 になった = 鍵の後に読み直して上げない (core は変えない・underivable) → 戻すと上げる', async () => {
    await save(dbA, 'ps01', { components: [{ code: 'p001', qty: 1 }, { code: 'p002', qty: 1 }, { code: 'p003', qty: 1 }, { code: 'p004', qty: 1 }] }, { token: await tokenOf('ps01') });
    const rows = [{ code: 'p001', qty: 1, sort: 1 }, { code: 'p002', qty: 1, sort: 2 }, { code: 'p003', qty: 1, sort: 3 }, { code: 'p004', qty: 1, sort: 4 }];
    const obsId = await observe(rows, new Date(Date.now() + 180000).toISOString());
    const before = { comps: await setComps(), cost: await setCost() };
    const g = gate();
    const a = launch(save(dbA, 'p004', { cost: { jpy: '0', reason: '無償に' } }, { token: await tokenOf('p004'), beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(W.promoteComponentRequest(dbO, obsId, { ownership: ALL_COMPANY, now: NOW }));
    await sleep(500);
    assert.equal(b.done, false, '上げる側は p004 の鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.deepEqual([rb.ok?.promoted, rb.ok?.reason], [false, 'underivable'], JSON.stringify(rb.ok || rb.err?.message));
    assert.match(rb.ok.blockers.join(' '), /p004 の原価/);
    assert.deepEqual({ comps: await setComps(), cost: await setCost() }, before);
    assert.deepEqual((await q("select kind from ops.sku_component_breaches where status = 'open' and set_sku_id = (select sku_id from core.skus where code = 'ps01')")).map((r) => r.kind), ['underivable']);
    await save(dbA, 'p004', { cost: { jpy: '400', reason: '戻した' } }, { token: await tokenOf('p004') });
    const r2 = await W.promoteComponentRequest(dbO, await observe(rows, new Date(Date.now() + 200000).toISOString()), { ownership: ALL_COMPANY, now: NOW });
    assert.equal(r2.promoted, true, JSON.stringify(r2));
    assert.deepEqual(await setComps(), ['p001x1', 'p002x1', 'p003x1', 'p004x1']);
    assert.equal(Number((await q("select count(*)::int as n from ops.sku_component_breaches where status = 'open'"))[0].n), 0);
  });

  await ta('[12] 画面を開いた後に仕入先の有効が変わった = 編集の印が違う (409) → 開き直すと保存できる', async () => {
    // p001 の仕入先 0001 を止める。0052: [9] で代表にした p003 を、同じ取引で 0003 に付け替えてから (名前は [5] の CSV が出ているので発注の月数で)
    await O.query('begin');
    await O.query("insert into core.suppliers (company_id, code, name) values (1, '0003', '三番')");
    await O.query(`insert into core.supplier_skus (company_id, supplier_id, sku_id, is_primary)
      select 1, s.supplier_id, k.sku_id, false from core.suppliers s, core.skus k where s.code = '0003' and k.code = 'p003'`);
    await O.query("update core.supplier_skus set is_primary = false where sku_id = (select sku_id from core.skus where code = 'p003') and is_primary");
    await O.query("update core.supplier_skus set is_primary = true where sku_id = (select sku_id from core.skus where code = 'p003') and supplier_id = (select supplier_id from core.suppliers where code = '0003')");
    await O.query("update core.suppliers set active = false where code = '0001'");
    await O.query('commit');
    const token = await tokenOf('p001');
    await O.query("update core.suppliers set active = true where code = '0001'");
    await assert.rejects(() => save(dbA, 'p001', { reorder_months: '4' }, { token }), (e) => e.reason === 'version_conflict');
    const s = await save(dbA, 'p001', { reorder_months: '4' }, { token: await tokenOf('p001') });
    assert.equal(s.ok, true);
  });

  await ta('[13] ロールの権限: 渡していない書き込み・関数は 42501 (変更の記録の偽の insert を含む)。保存の記録は全部 db_user = master_edit', async () => {
    const fakeEvent = "insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_key, new_value, actor_type, source_system) values (1, gen_random_uuid(), 'INSERT', 'sku', '{}', '{}', 'human', 'fake')";
    const fakeAck = "insert into ops.master_legacy_gate_acks (host, instance_id, build_id, manifest_hash, owner_hash, phase_seen, inflight_count) values ('render', 'x', 'b', repeat('a', 64), repeat('a', 64), 'new_open', 0)";
    const ackFn = "select ops.record_legacy_gate_ack('render', 'x', 'b', '{\"entries\":[{\"id\":\"a.b\",\"kind\":\"code\"}]}'::jsonb, repeat('a', 64), 'new_open', 0, null)";
    const phaseFn = "select ops.set_master_cutover_phase('new_open', 'x', '{}'::jsonb, null)";
    const obsFn = "select ops.record_ne_set_observations('{}'::jsonb)";
    // master_edit (画面)
    for (const [sql, label] of [[fakeEvent, 'edit: events の偽の insert'], ['update core.skus set version = version where false', 'edit: skus.version'],
      ['select 1 from core.suppliers for share', 'edit: 仕入先の行の鍵 (直接)'], ["update core.suppliers set active = false where false", 'edit: 仕入先の有効'],
      ["insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, 1, 2, 1, 'manual')", 'edit: 構成の直接の書き込み'],
      [fakeAck, 'edit: 門の記録 (直接)'], [ackFn, 'edit: 門の記録の関数'], [phaseFn, 'edit: 段階の関数'], [obsFn, 'edit: 観測の関数'],
      ["insert into ops.ne_set_observation_runs (run_id, observed_at, complete, saved_count, skipped_count, content_hash) values ('x', now(), false, 0, 0, repeat('a', 32))", 'edit: 観測の直接の書き込み']]) await denied(A, sql, label);
    await A.query('begin');
    await A.query("select core.lock_suppliers_for_share(array(select supplier_id from core.suppliers))");   // 関数を通せば鍵は掛けられる
    await A.query('rollback');
    // master_gate (門の記録)
    for (const [sql, label] of [[fakeAck, 'gate: 門の記録 (直接)'], [fakeEvent, 'gate: events'], [phaseFn, 'gate: 段階の関数'], [obsFn, 'gate: 観測の関数'],
      ["update ops.master_cutover_state set note = 'x'", 'gate: 段階の表']]) await denied(GR, sql, label);
    // master_ops (段階)
    for (const [sql, label] of [[ackFn, 'ops: 門の記録の関数'], [fakeAck, 'ops: 門の記録 (直接)'], ["update ops.master_cutover_state set note = 'x'", 'ops: 段階の表 (直接)'], [obsFn, 'ops: 観測の関数'],
      ['update core.skus set name = name where false', 'ops: skus']]) await denied(P, sql, label);
    // master_observer (観測)
    for (const [sql, label] of [["insert into ops.ne_set_observation_runs (run_id, observed_at, complete, saved_count, skipped_count, content_hash) values ('x', now(), false, 0, 0, repeat('a', 32))", 'observer: 観測の直接の書き込み'],
      [ackFn, 'observer: 門の記録の関数'], [phaseFn, 'observer: 段階の関数'], [fakeEvent, 'observer: events'], ['update core.skus set name = name where false', 'observer: skus']]) await denied(V, sql, label);
    // 本物の記録: 保存の記録は全部 master_edit・門の記録と観測は関数を通って残った
    assert.equal(Number((await q("select count(*)::int as n from events.master_change_events where request_id in (select request_id::text from ops.master_edit_requests) and db_user <> 'master_edit'"))[0].n), 0);
    assert.ok(Number((await q("select count(*)::int as n from events.master_change_events where db_user = 'master_edit'"))[0].n) > 10);
    assert.equal(Number((await q('select count(*)::int as n from ops.master_legacy_gate_acks'))[0].n), 8);   // 場所ごと 3 回 × 2 + [0] の黙っている記録と止まった記録
    assert.equal(Number((await q("select count(*)::int as n from ops.ne_set_observation_runs where run_id like 'ne_pg_%'"))[0].n), runSeq);
  });

  await ta('[14] 保存を開いていない (MASTER_EDIT_OPEN なし) = ほかの鍵を取る前に 409 切替前 (CSV の鍵・夜間ロードの鍵を持つ人がいても待たない)', async () => {
    const token = await tokenOf('p001');
    await O.query('begin');
    try {
      await O.query("select pg_advisory_xact_lock(hashtext('ops.ne_csv'))");
      await O.query(C.MASTER_WRITE_EXCLUSIVE_LOCK_SQL);
      const t0 = Date.now();
      const r = await save(dbA, 'p001', { reorder_months: '7' }, { token, open: false }).then((ok) => ({ ok }), (err) => ({ err }));
      assert.equal(r.err?.reason, 'before_cutover', r.err?.message || 'ok になった');
      assert.ok(Date.now() - t0 < 2000, `鍵を待った (${Date.now() - t0}ms)`);
    } finally { await O.query('rollback'); }
  });

  /** 夜間ロードの接続を包む: マスタの書き込みの鍵を取った直後に止める (扉が開くまで) */
  const pauseAfterLock = (client, g) => ({
    query: async (text, params) => { const r = await client.query(text, params); if (text === C.MASTER_WRITE_EXCLUSIVE_LOCK_SQL) await g.wait(); return r; },
    exec: (text) => client.query(text),
  });
  const loadAgain = (db, runId) => runInitialLoad(db, plan, { log: () => {}, runId, ownership: ALL_COMPANY, now: new Date('2030-01-05T03:00:00Z') });

  await ta('[15] 夜間ロードが先 = 保存・昇格はマスタの書き込みの鍵で短く待って 409 nightly_load (記録も 409・行の鍵で待ち合わない) → ロードは最後まで', async () => {
    const g = gate();
    const l = launch(loadAgain(pauseAfterLock(O2, g), 'load_pg_lock1'));
    await sleep(300);
    assert.equal(l.done, false, 'ロードは鍵を取って止まっている');
    const id = crypto.randomUUID();
    const t0 = Date.now();
    const r = await save(dbA, 'p001', { reorder_months: '5' }, { token: await tokenOf('p001'), requestId: id }).then((ok) => ({ ok }), (err) => ({ err }));
    const waited = Date.now() - t0;
    assert.deepEqual([r.err?.status, r.err?.reason], [409, 'nightly_load'], r.err?.message || 'ok になった');
    assert.ok(waited >= 2000 && waited < 9000, `待った長さ ${waited}ms (MASTER_WRITE_WAIT = ${W.MASTER_WRITE_WAIT})`);
    const rec = (await q('select status, error from ops.master_edit_requests where request_id = $1', [id]))[0];
    assert.deepEqual([rec.status, rec.error.status, rec.error.reason], ['failed', 409, 'nightly_load']);
    assert.equal((await A.query(`select current_setting('lock_timeout') as l`)).rows[0].l, '10s', '鍵の待ちの長さを接続に残さない');
    const obsId = (await q('select max(observation_id)::text as id from ops.ne_set_observations'))[0].id;
    const rp = await W.promoteComponentRequest(dbO, obsId, { ownership: ALL_COMPANY, now: NOW }).catch((e) => ({ thrown: e.message }));
    assert.deepEqual([rp.promoted, rp.reason], [false, 'nightly_load'], JSON.stringify(rp));   // 投げない (夜間の段を止めない)
    g.open();
    const rl = await l.promise;
    assert.equal(rl.ok?.ok, true, rl.err?.stack || JSON.stringify(rl.ok?.error));
    const s = await save(dbA, 'p001', { reorder_months: '5' }, { token: await tokenOf('p001') });
    assert.equal(s.ok, true);
  });

  await ta('[15] 保存が先 = 夜間ロードはマスタの書き込みの鍵で待ち、保存の commit の後に最後まで (デッドロックしない)', async () => {
    const g = gate();
    const a = launch(save(dbA, 'p002', { cost: { jpy: '260', reason: 'ロードの前' } }, { token: await tokenOf('p002'), beforeCommit: g.wait }));
    await sleep(300);
    const l = launch(loadAgain(pgAdapter(O2), 'load_pg_lock2'));
    await sleep(700);
    assert.equal(l.done, false, 'ロードは保存が終わるまで鍵で待つ');
    g.open();
    const ra = await a.promise;
    assert.ok(ra.ok, ra.err?.message);
    const rl = await l.promise;
    assert.equal(rl.ok?.ok, true, rl.err?.stack || JSON.stringify(rl.ok?.error));
  });

  await ta('[16] NE の観測の書き込み × 古い観測の昇格: 昇格がセットの鍵を持っている間、新しい観測の書き込みは待つ → 昇格の後に書かれ、次の昇格はそれを見る', async () => {
    const A = [{ code: 'p001', qty: 1, sort: 1 }, { code: 'p002', qty: 1, sort: 2 }];
    const B = [{ code: 'p001', qty: 2, sort: 1 }, { code: 'p002', qty: 1, sort: 2 }];
    await save(dbA, 'ps01', { components: A.map(({ code, qty }) => ({ code, qty })) }, { token: await tokenOf('ps01') });
    const a = await observe(A, new Date(Date.now() + 240000).toISOString());
    const g = gate();
    const pa = launch(W.promoteComponentRequest(dbO, a, { ownership: ALL_COMPANY, now: NOW, beforeCommit: g.wait }));
    await sleep(300);
    const runB = `ne_pg_${++runSeq}`;
    const pb = launch(W.recordNeSetObservations(dbV, { run_id: runB, observed_at: new Date(Date.now() + 250000).toISOString(), complete: true, requested: 1, fetched: 1,
      raw_hash: 'c'.repeat(64), source_generation: runB, sets: [{ set_code: 'ps01', rows: B }] }));
    await sleep(500);
    assert.equal(pb.done, false, '観測の書き込みは、昇格が持つセットの鍵で待つ');
    g.open();
    assert.equal((await pa.promise).ok?.promoted, true);
    assert.equal((await pb.promise).ok?.state, 'written');
    assert.deepEqual(await setComps(), ['p001x1', 'p002x1']);
    const b = (await q('select observation_id::text as id from ops.ne_set_observations where run_id = $1', [runB]))[0].id;
    assert.equal((await W.promoteComponentRequest(dbO, a, { ownership: ALL_COMPANY, now: NOW })).reason, 'superseded_observation');
    assert.equal((await W.promoteComponentRequest(dbO, b, { ownership: ALL_COMPANY, now: NOW })).reason, 'unrequested_diff');   // NE がまた変わった = NE でやること
  });

  await ta('[17] 画面のロールの直接の書き込み: begin_master_write の前は 42501 (偽の core.actor_* を付けても)・偽の記録は残らない', async () => {
    await A.query('begin');
    try {
      await A.query("select set_config('core.actor_type', 'human', true), set_config('core.actor_id', 'forged@evil', true), set_config('core.source_system', 'portal_master_edit', true)");
      await assert.rejects(() => A.query("update core.skus set name = '偽' where code = 'p003'"), (e) => e.code === '42501' && /master_write_session_required/.test(e.message));
    } finally { await A.query('rollback'); }
    // 直接 begin した後でも、約束の相手 (p003) でない SKU は書けない (R4 M2)
    const sid3 = (await q("select sku_id::text as id from core.skus where code = 'p003'"))[0].id;
    const v3 = (await q('select ops.master_edit_versions($1::bigint) as v', [sid3]))[0].v;
    await A.query('begin');
    try {
      await A.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)', [crypto.randomUUID(), 'naka@test', null, JSON.stringify(ALL_COMPANY), 'sku_edit', sid3, 'b'.repeat(64), 'c'.repeat(64), JSON.stringify(v3)]);
      await assert.rejects(() => A.query("update core.skus set name = '相手でない' where code = 'p004'"), (e) => e.code === '42501' && /master_write_target/.test(e.message));
    } finally { await A.query('rollback'); }
    // 約束した取引は done を書かずに commit できない (R5 M2)
    await A.query('begin');
    let eCommit = null;
    try {
      await A.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)', [crypto.randomUUID(), 'naka@test', null, JSON.stringify(ALL_COMPANY), 'sku_edit', sid3, 'b'.repeat(64), 'c'.repeat(64), JSON.stringify((await q('select ops.master_edit_versions($1::bigint) as v', [sid3]))[0].v)]);
      await A.query("update core.skus set reorder_months = reorder_months + 1 where code = 'p003'");
      eCommit = await A.query('commit').then(() => null, (e) => e);
    } finally { try { await A.query('rollback'); } catch { /* */ } }
    assert.equal(eCommit?.code, '42501'); assert.match(eCommit.message, /master_write_session_unfinished/);
    await denied(A, "insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions, actor_id, source_system, db_user, phase, owner_hash, ownership) values (gen_random_uuid(), txid_current(), gen_random_uuid(), 'sku_edit', 1, '{}', '{}', repeat('a', 64), repeat('a', 64), '{}', 'x', 'portal_master_edit', 'x', 'new_open', repeat('a', 64), '{}')", 'edit: sessions に直接');
    assert.equal(Number((await q("select count(*)::int as n from events.master_change_events where actor_id = 'forged@evil'"))[0].n), 0);
    assert.equal(Number((await q("select count(*)::int as n from ops.master_write_sessions where db_user = 'master_edit'"))[0].n) > 10, true);
  });

  // ─── ⑤-2b (0053): 新商品の NE 登録の CSV (書くのは DB の関数 = 画面のロール master_edit で)・JAN ───
  const RUN = 'mc_20300110T000000000Z_aaaaaa';
  await O.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN]);
  await O.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN, entries: ['p001', 'p002', 'p003', 'p004', 'ps01'].map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
  const rates = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
  await R.registerNewSku(dbA, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code: 'pnew', card: { create: false },
    values: { name: '新しい単品', standard_price: '1500', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', cost: { jpy: '300' } } },
  { ownership: ALL_COMPANY, open: true, now: new Date(), shippingRates: rates });   // 原価の始まり = DB の東京の今日 (0052)

  await ta('[18] NE 登録の CSV を作る取引の途中は、同じ商品の保存が SKU の鍵で待つ → 作った後に保存 = そのファイル (まだ配っていない) を使わないにして保存する', async () => {
    const g = gate();
    const a = launch(G.buildRegExport(dbA, { actor: 'boss@test', kind: 'products', codes: ['pnew'], requestId: crypto.randomUUID() },
      { ownership: ALL_COMPANY, open: true, nowMs: NOW.getTime(), beforeCommit: g.wait }));
    await sleep(300);
    const token = await tokenOf('pnew');
    const b = launch(save(dbB, 'pnew', { name: '直した名前' }, { token }));
    await sleep(500);
    assert.equal(b.done, false, '保存は SKU の鍵で待つ');
    g.open();
    const ra = await a.promise;
    assert.ok(ra.ok, ra.err?.message);
    const rb = await b.promise;
    assert.ok(rb.ok, rb.err?.message);
    assert.ok(rb.ok.warnings.some((w) => w.includes(`#${ra.ok.export.export_id}`)), JSON.stringify(rb.ok.warnings));
    assert.deepEqual((await q('select state, close_reason from ops.ne_reg_exports where export_id = $1', [ra.ok.export.export_id]))[0], { state: 'closed', close_reason: 'superseded' });
  });

  await ta('[19] 同じ JAN を 2 つの商品に同時に = 後の方は一意の索引で待ち、前の方の commit の後に 409 jan_taken (両方は付かない)', async () => {
    const jan = '4900000000108';
    assert.equal(W.janValid(jan), true);
    const g = gate();
    const a = launch(save(dbA, 'p001', { jan }, { token: await tokenOf('p001'), beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(save(dbB, 'p002', { jan }, { token: await tokenOf('p002') }));
    await sleep(500);
    assert.equal(b.done, false, '後の方は一意の索引で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'jan_taken', rb.err?.message || 'ok になった');
    assert.equal(Number((await q("select count(*)::int as n from core.external_ids where system = 'jan' and external_value = $1 and valid_to is null", [jan]))[0].n), 1);
  });

  await ta('[20] 同じファイルの申告が 2 つ並ぶ (画面のロール・DB の関数) = 後の方は鍵で待ち、前の方の後に「もう一度」(試みを足すだけ)・NE 登録待ちへは 1 回だけ', async () => {
    // 代表の仕入先 = 0003 ([12] で作った・使える。0002 は [9] で止めた)
    await R.registerNewSku(dbA, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code: 'pnew2', card: { create: false },
      values: { name: '新しい単品 2', standard_price: '1600', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0003', cost: { jpy: '310' } } },
    { ownership: ALL_COMPANY, open: true, now: new Date(), shippingRates: rates });   // 原価の始まり = DB の東京の今日 (0052)
    const o = { ownership: ALL_COMPANY, open: true, nowMs: NOW.getTime() };
    const b0 = await G.buildRegExport(dbA, { actor: 'boss@test', kind: 'products', codes: ['pnew2'], requestId: crypto.randomUUID() }, o);
    await G.issueRegExport(dbA, { actor: 'boss@test', exportId: b0.export.export_id }, o);
    const dec = (db, extra = {}) => G.declareRegExport(db, { actor: 'boss@test', exportId: b0.export.export_id, sha256: b0.export.sha256, result: 'ok' }, { ...o, ...extra });
    const g = gate();
    const a = launch(dec(dbA, { beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(dec(dbB));
    await sleep(500);
    assert.equal(b.done, false, '後の申告は鍵で待つ');
    g.open();
    const [ra, rb] = [await a.promise, await b.promise];
    assert.deepEqual([ra.ok?.state, ra.ok?.ne_pending, rb.ok?.again], ['declared', ['pnew2'], true], JSON.stringify([ra.ok || ra.err?.message, rb.ok || rb.err?.message]));
    assert.equal(Number((await q("select count(*)::int as n from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = 'pnew2' and e.to_state = 'ne_pending'"))[0].n), 1);
    assert.equal(Number((await q('select count(*)::int as n from ops.ne_reg_attempts where export_id = $1', [b0.export.export_id]))[0].n), 2);
    // 画面のロールは表を直接書けない (本物のログイン)
    await denied(A, `update ops.ne_reg_export_items set state = 'verified' where export_id = ${Number(b0.export.export_id)}`, 'ne_reg_export_items の直接の update');
    await denied(A, `select ops.transition_sku_registration(1, 'ne_confirmed', 'system', 'x', null, '{}'::jsonb, null)`, '状態の関数');
    // 本物のログインでも: 約束を書く部品の関数は呼べない・JAN の行は直接書けない (JAN の約束の関数の中だけ)。申告の約束 (reg_csv_declare) と done は関数が書く
    await denied(A, "select ops.open_reg_write('reg_csv_supersede', gen_random_uuid(), 'boss@test', null, '{}'::jsonb, null, null, null, repeat('a', 64), '{}'::jsonb)", '約束を書く部品の関数');
    await denied(A, `insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id, evidence)
      select 1, 'product', product_id, 'jan', 'jan', '4900000000146', 'manual', 'human', 'x', '{}'::jsonb from core.skus where code = 'p003'`, 'JAN の直接の書き込み');
    const ev = (await q("select e.actor_id, e.request_id from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = 'pnew2' and e.to_state = 'ne_pending'"))[0];
    const sess = await q("select s.actor_id, s.db_user, s.operation, d.status from ops.master_write_sessions s join ops.master_edit_requests d on d.request_id = s.request_id where s.request_id::text = $1", [ev.request_id]);
    assert.deepEqual([ev.actor_id, sess.map((s) => [s.actor_id, s.db_user, s.operation, s.status])], ['boss@test', [['boss@test', 'master_edit', 'reg_csv_declare', 'done']]]);
  });
} finally {
  for (const c of clients.reverse()) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
