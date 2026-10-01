/**
 * test-master-register-pg.mjs — 新商品の登録・登録の状態・カードの知らせ (0052・lib/master-register.mjs・lib/product-hub-outbox.mjs) の**同時実行**と**ロールの権限**を、
 *   実 PostgreSQL の独立した接続で確かめる (PGlite は 1 接続なので書けない。PR #1566 Codex R1 M7 / 契約 v3 M5・仮レビュー M-A / L7)
 *
 * 接続は本番と同じロールでログインする (⑤-1 の create-master-edit-roles.mjs で作る・パスワードは試験の回ごと):
 *   登録・取り込み (A・B) = master_edit / backfill・段階 (P) = master_ops / 門の記録 (GR・GM) = master_gate_render・master_gate_minipc /
 *   持ち主 (O・O2) = 夜間ロード・migration・ほかの処理の代わり
 * 固定する契約:
 *   0 backfill の前は new_open に進めない (⑤-1 の前提の差し込み口の表に 0052 が 1 行 = 0052_registrations)
 *   1 同じ新しいコードを 2 人が同時に登録: 後の人はコードの鍵で待ち、前の人の commit の後に 409 code_taken (SKU は 1 つ)
 *   2 夜間ロードと登録: (a) 登録が先 (commit 前) = ロードはマスタの書き込みの鍵で待ってから同じ行に重ね、状態は draft のまま (要確認にしない)
 *                       (b) 夜間ロードのような取引が先に同じコードを入れた (commit 前・鍵なし) = 登録は unique で待ち、commit の後に 409 code_taken (500 にしない)
 *                       (c) 夜間ロードがマスタの書き込みの鍵を持っている = 登録は 3 秒待って 409 nightly_load (何も書かない)
 *   3 backfill の途中に SKU を足す: 足す側は SKU の表の鍵で待ち、backfill の後なので状態の行が無い = commit で断られる
 *   4 遅らせた制約の trigger は commit のときに効く (取引の中では見えている・状態の行を同じ取引で作れば通る)
 *   5 カードの取り込みが 2 つ (画面のロール・関数だけ): 借りの間はほかが取らない・借りが切れたらほかが取って済ませる・先の取り込みの結果は書かれない (カードは 1 枚)
 *   6 画面・運用のロール (本物のログイン) は印 (GUC) を立てても状態の表・履歴・backfill の印を直接書けない (42501)・要確認にする関数も実行できない・
 *     知らせの状態の列も書けない (L7)・画面のロールの登録は関数を通して状態を作れる
 *   7 記録の偽造 (仮レビュー M-A): 画面のロールは変更の記録を書けない (42501)・偽の記録があっても前からある SKU は下書きにできない・
 *     同じ取引で直しただけの前からある SKU も下書きにできない (= backfill の計画から外れない)・画面のロールは begin の前は直すこともできない (42501)
 *   8 登録の関数 ops.register_new_sku と ⑤-1 (0051) の書き込みの約束 (本物のログイン = SET ROLE でない): 画面のロールは SKU・知らせを直接足せない (42501)・
 *     下書きの状態を作る関数も実行できない・関数の中の書き込みも 0051 の guard が見る (代表の仕入先の業務の約束で断る)・
 *     変更の記録と状態の記録は登録の約束の人・request_id・理由 (core.actor_* の偽の値は使わない)・関数の外では同じ取引でも登録の約束で書けない・
 *     登録の約束は db_user = master_edit (ログインした役)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-register-pg.mjs
 *   (この PC では C:/tmp/pg-embed の run-conc.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロール master_* をクラスタに作る)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { MASTER_OWNERSHIP } from '../config/master-ownership.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (新商品の登録の実 PostgreSQL の同時実行の試験は飛ばす。PGlite の試験は scripts/test-master-register.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const R = await import('../lib/master-register.mjs');
const O = await import('../lib/product-hub-outbox.mjs');
const C = await import('../lib/master-cutover.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };
const codeOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e.code; } return null; };

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const NOW = new Date('2030-01-10T03:00:00Z');
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne:item-screen', kind: 'manual' }] };
/** 証拠の時刻は今の段階に入った後・サーバーの今以前 (⑤-1 #1563 R3) = 進める直前に作る */
const manualStopped = () => [{ id: 'ne:item-screen', by: 't', at: new Date().toISOString() }];
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const sku = (code, name) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const planOf = (extra = []) => ({
  skus: [sku('p001', '単品 1'), sku('p002', '単品 2'), ...extra.map((c) => sku(c, `NE の ${c}`))],
  variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
});
const dbName = `cdb_mr_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;
const stateOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = $1', [code]))[0]?.state ?? null;

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  await createMasterEditRoles(M, { pw: PW });
  const [A, B, P, GR, GM, O2] = [await open('master_edit'), await open('master_edit'), await open('master_ops'), await open('master_gate_render'), await open('master_gate_minipc'), await open(null)];
  clients.push(A, B, P, GR, GM, O2);
  const [dbA, dbB, dbP, dbO2] = [A, B, P, O2].map(pgAdapter);
  const dbGate = { render: pgAdapter(GR), minipc: pgAdapter(GM) };
  const reg = (db, code, { requestId = crypto.randomUUID(), beforeCommit, card = {} } = {}) => R.registerNewSku(db, {
    actor: 'naka@test', requestId, kind: 'single', code, values: { name: `新商品 ${code}`, standard_price: '1000', shipping_code: 'S01', tax_rate: '10' }, card,
  }, { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES, beforeCommit });
  const r0 = await runInitialLoad(dbM, planOf(), { log: () => {}, runId: 'load_pg_1', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  // 切替を company_owner まで (⑤-1 の本物の関数・門の記録は場所ごとのログイン・段階は master_ops)
  const h = C.ownershipHash(ALL_COMPANY), legacy = C.ownershipHash(MASTER_OWNERSHIP);
  const mh = await C.manifestHashOf(dbM, MANIFEST);
  const builds = { render: ['r1'], minipc: ['m1'] };
  const acks = async (ownership, phase) => { for (const [host, inst, build] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await C.recordLegacyGateAck(dbGate[host], { host, instanceId: inst, buildId: build, manifest: MANIFEST, ownership, phaseSeen: phase }); };
  await acks(MASTER_OWNERSHIP, 'legacy_open');
  await C.advanceCutoverPhase(dbP, { to: 'frozen', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: legacy, manual_entries_stopped: manualStopped(), drain: { done: true, checked_by: 't', checked_at: new Date().toISOString() } } });
  await acks(ALL_COMPANY, 'frozen');
  await C.advanceCutoverPhase(dbP, { to: 'company_owner', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });
  await acks(ALL_COMPANY, 'company_owner');
  const toNewOpen = () => C.advanceCutoverPhase(dbP, { to: 'new_open', actor: 't@test', evidence: { expected_builds: builds, manifest_hash: mh, owner_hash: h } });

  await ta('[0] backfill の前は new_open に進めない (⑤-1 の前提の差し込み口の表に 0052 が 1 行)', async () => {
    await assert.rejects(() => toNewOpen(), /prereq_failed: 0052_registrations: backfill_missing/);
    assert.equal((await q('select phase from ops.master_cutover_state'))[0].phase, 'company_owner');
  });

  await ta('[7] (backfill の前) 記録の偽造 (仮レビュー M-A): 画面のロールは変更の記録を書けない・偽の記録があっても前からある SKU は下書きにできない・同じ取引で直しただけの SKU も (backfill から外れない)', async () => {
    const id = (await q("select sku_id::text as id from core.skus where code = 'p002'"))[0].id;
    const p0 = (await P.query('select * from ops.registration_backfill_plan()')).rows[0];
    const forge = `insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_id, entity_key, new_value, actor_type, source_system)
      values (1, gen_random_uuid(), 'INSERT', 'sku', $1, jsonb_build_object('sku_id', $1::bigint), '{}'::jsonb, 'human', 'forge')`;
    assert.equal(await codeOf(A, forge, [id]), '42501');
    // 持ち主が同じ取引に偽の記録を入れても、関数は SKU の行そのものを見る
    await O2.query('begin');
    await O2.query(forge, [id]);
    await assert.rejects(() => O2.query('select ops.create_sku_registration($1, $2)', [id, 't@test']), /not_new_sku/);
    await O2.query('rollback');
    // 持ち主が同じ取引で前からある SKU を直してから呼ぶ (行の xmin は今の取引・created_at は古い)
    await O2.query('begin');
    await O2.query('update core.skus set name = name where sku_id = $1', [id]);
    await assert.rejects(() => O2.query('select ops.create_sku_registration($1, $2)', [id, 't@test']), /not_new_sku/);
    await O2.query('rollback');
    // 画面のロールは begin_master_write の前は直せない (0051 の守り)・下書きの状態を作る関数は実行できない (登録の関数の中だけ)
    assert.equal(await codeOf(A, 'update core.skus set name = name where sku_id = $1', [id]), '42501');
    assert.equal(await codeOf(A, 'select ops.create_sku_registration($1, $2)', [id, 't@test']), '42501');
    assert.equal(await stateOf('p002'), null);   // 下書きにならない = backfill の計画から外れない
    const p1 = (await P.query('select * from ops.registration_backfill_plan()')).rows[0];
    assert.equal(p1.sku_count, p0.sku_count); assert.equal(p1.snapshot_hash, p0.snapshot_hash);
  });
  await ta('[3] backfill (運用のロール) の途中に SKU を足す = SKU の表の鍵で待ち、backfill の後なので状態の行が無い取引は commit で断られる', async () => {
    const p = (await P.query('select * from ops.registration_backfill_plan()')).rows[0];
    await P.query('begin');
    await P.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 'test@test']);
    const b = launch((async () => {
      await O2.query('begin');
      try {
        await O2.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'bf-1', 'backfill の途中')`);
        await O2.query('commit');
      } catch (e) { try { await O2.query('rollback'); } catch { /* */ } throw e; }
    })());
    await sleep(500);
    assert.equal(b.done, false, '足す側は backfill の SKU の表の鍵で待つ');
    await P.query('commit');
    const rb = await b.promise;
    assert.match(String(rb.err?.message), /unregistered_sku/);
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'bf-1'"))[0].n, 0);
    await toNewOpen();
    assert.equal((await q('select phase from ops.master_cutover_state'))[0].phase, 'new_open');
  });

  await ta('[4] 遅らせた制約の trigger は commit のときに効く (取引の中では見えている・同じ取引で状態の行を作れば通る)・画面のロールも同じ', async () => {
    await O2.query('begin');
    await O2.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'dt-1', 'x')`);
    assert.equal((await O2.query(`select count(*)::int as n from core.skus where code = 'dt-1'`)).rows[0].n, 1);
    await assert.rejects(() => O2.query('commit'), /unregistered_sku/);
    await O2.query('begin');
    const id = (await O2.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'dt-2', 'x') returning sku_id`)).rows[0].sku_id;
    await O2.query('select ops.create_sku_registration($1, $2)', [id, 'test@test']);
    await O2.query('commit');
    assert.equal(await stateOf('dt-2'), 'draft');
    // 画面のロールは SKU を直接足せない (insert の権限なし)。足すのは登録の関数だけ (状態の行も同じ取引で作る)
    assert.equal(await codeOf(A, `insert into core.skus (company_id, sku_kind, code, name, created_by_type, created_by_id) values (1, 'set', 'dt-3', 'x', 'human', 'x')`), '42501');
  });

  await ta('[1] 同じ新しいコードを 2 人 (画面のロール) が同時に登録 = 後の人はコードの鍵で待ち、前の人の commit の後に 409 code_taken', async () => {
    const g = gate();
    const a = launch(reg(dbA, 'race-1', { beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(reg(dbB, 'race-1'));
    await sleep(500);
    assert.equal(b.done, false, 'B はコードの鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok, (await a.promise).err?.message);
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'code_taken', rb.err?.message);
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'race-1'"))[0].n, 1);
  });

  await ta('[2a] 登録が先 (commit の前) に夜間ロードが同じコードを入れる = ロードはマスタの書き込みの鍵で待って同じ行に重ね、状態は draft のまま', async () => {
    const g = gate();
    const a = launch(reg(dbA, 'lr-1', { beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(runInitialLoad(dbO2, planOf(['lr-1']), { log: () => {}, runId: 'load_pg_2', now: new Date('2030-01-09T03:00:00Z') }));
    await sleep(700);
    assert.equal(b.done, false, 'ロードは登録 (マスタの書き込みの共有の鍵) で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.ok(rb.ok?.ok, rb.err?.message);
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'lr-1'"))[0].n, 1);
    assert.equal(await stateOf('lr-1'), 'draft');
  });

  await ta('[2b] 夜間ロードのような取引が先 (commit の前・鍵なし) に同じコードを入れた = 登録は unique で待ち、commit の後に 409 code_taken (500 にしない)', async () => {
    await O2.query('begin');
    await O2.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'lr-2', 'NE から')`);
    await O2.query(`select ops.quarantine_unregistered_skus('load_pg_3')`);
    const a = launch(reg(dbA, 'lr-2'));
    await sleep(500);
    assert.equal(a.done, false, '登録は unique で待つ');
    await O2.query('commit');
    const ra = await a.promise;
    assert.equal(ra.err?.reason, 'code_taken', ra.err?.message);
    assert.equal(await stateOf('lr-2'), 'quarantined');
  });

  await ta('[2c] 夜間ロードがマスタの書き込みの鍵 (排他) を持っている = 登録は短く待って 409 nightly_load (何も書かない)', async () => {
    await O2.query('begin');
    await O2.query('select pg_advisory_xact_lock(core.master_write_lock_key())');
    const t0 = Date.now();
    const r = await reg(dbA, 'nl-1').then(() => null, (e) => e);
    assert.equal(r?.reason, 'nightly_load', r?.message);
    assert.ok(Date.now() - t0 >= 2500, `待った時間 ${Date.now() - t0} ms`);
    await O2.query('rollback');
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'nl-1'"))[0].n, 0);
    assert.equal((await reg(dbA, 'nl-1')).state, 'draft');   // 鍵が外れれば通る (同じ request_id ではない = 新しい登録)
  });

  await ta('[5] カードの取り込みが 2 つ (画面のロール): 借りの間はほかが取らない・借りが切れたらほかが取って済ませる・先の取り込みの結果は書かれない (カードは 1 枚)', async () => {
    const r = await reg(dbA, 'lease-1');
    const cards = new Map();
    const applyWith = (who) => async (ev) => {
      const k = String(ev.sku_id);   // SKU は知らせの行の番号 (0052)
      if (cards.has(k)) return { outcome: 'linked', draft_id: cards.get(k).id };
      cards.set(k, { id: cards.size + 1, by: who });
      return { outcome: 'created', draft_id: cards.get(k).id };
    };
    const g = gate();
    const slow = launch(O.runCardOutbox(dbA, async (ev) => { await g.wait(); return applyWith('A')(ev); }, { eventId: r.card.event_id, owner: 'consumer-A' }));
    await sleep(300);
    assert.deepEqual(await O.runCardOutbox(dbB, applyWith('B'), { eventId: r.card.event_id, owner: 'consumer-B' }), [], '借りの間は取らない');
    await M.query(`update ops.product_hub_outbox set leased_until = now() - interval '1 second' where event_id = $1`, [r.card.event_id]);   // 借りが切れた (持ち主の手で)
    const rb = await O.runCardOutbox(dbB, applyWith('B'), { eventId: r.card.event_id, owner: 'consumer-B' });
    assert.deepEqual([rb[0].status, rb[0].recorded], ['done', true]);
    g.open();
    const ra = (await slow.promise).ok;
    assert.equal(ra[0].recorded, false, '先の取り込みの結果は書かれない');
    assert.equal(cards.size, 1);
    const ob = (await q('select status, result, lease_owner from ops.product_hub_outbox where event_id = $1', [r.card.event_id]))[0];
    assert.deepEqual([ob.status, ob.result.outcome, ob.lease_owner], ['done', 'created', null]);
  });

  await ta('[6] 画面・運用のロール (本物のログイン) は印 (GUC) を立てても状態の表・履歴・backfill の印・知らせの状態を直接書けない (42501)', async () => {
    const d = (await q("select sku_id::text as id from core.skus where code = 'race-1'"))[0].id;
    const any = (await q("select sku_id::text as id from core.skus where code = 'p001'"))[0].id;
    for (const c of [A, P]) {
      await c.query(`select set_config('ops.registration_protocol', '1', false)`);
      const codes = [
        await codeOf(c, `update ops.master_registrations set state = 'available' where sku_id = $1`, [d]),
        await codeOf(c, `insert into ops.master_registrations (sku_id, state, origin, created_by, state_changed_by) values ($1, 'available', 'backfill', 'x', 'x')`, [any]),
        await codeOf(c, 'delete from ops.master_registrations where sku_id = $1', [d]),
        await codeOf(c, `insert into ops.master_registration_events (sku_id, to_state, actor_type, actor_id) values ($1, 'available', 'human', 'x')`, [d]),
        await codeOf(c, `insert into ops.master_registration_backfill (id, sku_count, snapshot_hash, phase, actor) values (1, 0, $1, 'frozen', 'x')`, ['a'.repeat(64)]),
        await codeOf(c, `select ops.quarantine_unregistered_skus('x')`),
        await codeOf(c, `update ops.product_hub_outbox set status = 'done', done_at = now() where status <> 'done'`),
      ];
      assert.deepEqual(codes, Array(7).fill('42501'));
      await c.query(`select set_config('ops.registration_protocol', '', false)`);
    }
    assert.equal(await codeOf(A, `select ops.transition_sku_registration($1, 'cancelled', 'human', 'x', '理由')`, [d]), '42501');   // 画面は状態を進めない
    assert.equal(await codeOf(P, `select ops.create_sku_registration($1, 'x')`, [d]), '42501');                                    // 運用は下書きを作らない
    assert.equal(await stateOf('race-1'), 'draft');
    // 運用のロールはやめる (cancelled) だけ
    await P.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'ops@test', '試験でやめる')`, [d]);
    assert.equal(await stateOf('race-1'), 'cancelled');
  });

  await ta('[8] 登録の関数 (本物のログイン): 直接の insert = 42501・関数の中も 0051 の guard が見る・記録は登録の約束の人・関数の外では書けない', async () => {
    const p1 = (await q("select sku_id::text as id from core.skus where code = 'p001'"))[0].id;
    // 画面のロールは SKU・知らせを直接足せない・下書きの状態を作る関数も実行できない
    assert.equal(await codeOf(A, `insert into core.skus (company_id, sku_kind, code, name, created_by_type, created_by_id) values (1, 'set', 'g8-0', 'x', 'human', 'x')`), '42501');
    assert.equal(await codeOf(A, `insert into ops.product_hub_outbox (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
      values (1, $1, 'card_create', 'ph-card-v1', '{}'::jsonb, $2, $3::uuid, 'naka@test')`, [p1, 'e'.repeat(64), crypto.randomUUID()]), '42501');
    assert.equal(await codeOf(A, 'select ops.create_sku_registration($1, $2)', [p1, 'naka@test']), '42501');
    const entry = (code, over = {}) => ({ kind: 'single', code, started_at: null, product: { name: code, sales_class: 3, expiry_managed: false, inbound_date_managed: null },
      sku: { name: code, tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', standard_price_jpy: 1000, shipping_code: 'S01', shipping_method: 'ゆうパケット', shipping_cost_jpy: 210,
        reorder_months: null, set_sales_class_override: null, handling_own: null }, supplier_id: null, cost: null, component_request: null, card: null, result: {}, ...over });
    const call = (rid, e) => A.query('select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r', [rid, 'naka@test', '本物のログインの試験', JSON.stringify(ALL_COMPANY), 'e'.repeat(64), JSON.stringify(e)]);
    // 関数の中の書き込みも 0051 の guard が見る (取引停止の仕入先を代表にする = 業務の約束で断る)
    await M.query(`insert into core.suppliers (company_id, code, name, active) values (1, '0099', '止めた仕入先', false) on conflict do nothing`);
    const stopped = (await q(`select supplier_id::text as id from core.suppliers where code = '0099'`))[0].id;
    await A.query('begin');
    await assert.rejects(() => call(crypto.randomUUID(), entry('g8-x', { supplier_id: stopped })), (e) => e.code === '42501' && /master_write_invariant/.test(e.message));
    await A.query('rollback');
    // 合っていれば通る・偽の core.actor_id は記録に残らない・関数の外では (同じ取引でも) 登録の約束で書けない
    const rid = crypto.randomUUID();
    await A.query('begin');
    await A.query(`select set_config('core.actor_id', 'forged@evil', true), set_config('core.actor_type', 'human', true)`);
    const r1 = (await call(rid, entry('g8-1'))).rows[0].r;
    assert.equal(await codeOf(A, 'update core.skus set name = $2 where sku_id = $1', [r1.sku_id, '外で直す']), '42501');
    await A.query('rollback');
    await A.query('begin');
    await A.query(`select set_config('core.actor_id', 'forged@evil', true), set_config('core.actor_type', 'human', true)`);
    const r2 = (await call(rid, entry('g8-1'))).rows[0].r;
    await A.query('commit');
    assert.deepEqual(await q(`select distinct actor_id, request_id, reason_text, db_user from events.master_change_events where request_id = $1`, [rid]),
      [{ actor_id: 'naka@test', request_id: rid, reason_text: '本物のログインの試験', db_user: 'master_edit' }]);
    assert.deepEqual(await q('select actor_id, request_id, reason from ops.master_registration_events where sku_id = $1', [r2.sku_id]), [{ actor_id: 'naka@test', request_id: rid, reason: '本物のログインの試験' }]);
    assert.deepEqual(await q('select operation, status from ops.master_edit_requests where request_id = $1', [rid]), [{ operation: 'sku_create', status: 'done' }]);
    // 画面の登録も同じ関数 = 登録の約束 (db_user = ログインした役)
    const r = await reg(dbA, 'g8-2');
    assert.deepEqual(await q('select operation, actor_id, db_user, phase from ops.master_write_sessions where request_id = $1', [r.request_id]),
      [{ operation: 'sku_create', actor_id: 'naka@test', db_user: 'master_edit', phase: 'new_open' }]);
  });

} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
