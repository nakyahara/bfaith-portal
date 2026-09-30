/**
 * test-master-register-pg.mjs — 新商品の登録・登録の状態・カードの知らせ (0051・lib/master-register.mjs・lib/product-hub-outbox.mjs) の**同時実行**を、
 *   実 PostgreSQL の独立した接続で確かめる (PGlite は 1 接続なので書けない。PR #1566 Codex R1 M7 / 契約 v3 M5)
 *
 * 固定する契約:
 *   1 同じ新しいコードを 2 人が同時に登録: 後の人はコードの鍵で待ち、前の人の commit の後に 409 code_taken (SKU は 1 つ)
 *   2 夜間ロードと登録: (a) 登録が先 (commit 前) = ロードは待ってから同じ行に重ね、状態は draft のまま (要確認にしない)
 *                       (b) ロードが先 (commit 前) = 登録は unique で待ち、ロードの commit の後に 409 code_taken (500 にしない)
 *   3 backfill の途中に SKU を足す: 足す側は SKU の表の鍵で待ち、backfill の後なので状態の行が無い = commit で断られる
 *   4 遅らせた制約の trigger は commit のときに効く (取引の中では見えている・状態の行を同じ取引で作れば通る)
 *   5 カードの取り込みが 2 つ: 借り (lease) の間はほかが取らない・借りが切れたらほかが取って済ませる・先の取り込みの結果は書かれない (カードは 1 枚)
 *   0 (準備の中) backfill の前は new_open に進めない
 *   6 画面・運用のロール (本物のログイン) は印 (GUC ops.registration_protocol) を立てても状態の表・履歴・backfill の印を直接書けない (42501)・
 *     要確認にする関数も実行できない・画面のロールの登録は関数を通して状態を作れる (PR #1566 R1 H2)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-register-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す)。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { MASTER_OWNERSHIP } from '../config/master-ownership.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (新商品の登録の実 PostgreSQL の同時実行の試験は飛ばす。PGlite の試験は scripts/test-master-register.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const R = await import('../lib/master-register.mjs');
const O = await import('../lib/product-hub-outbox.mjs');
const { masterRegisterRoleStatements } = await import('./company-db/master-register-grants.mjs');
const C = await import('../lib/master-cutover.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const gate = () => { let open; const p = new Promise((r) => { open = r; }); return { wait: () => p, open }; };

const ALL_COMPANY = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const NOW = new Date('2030-01-10T03:00:00Z');
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]);
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
const M = await openPgClient(u.toString()), A = await openPgClient(u.toString()), B = await openPgClient(u.toString());
for (const c of [M, A, B]) c.on('error', (e) => console.error(`[pg] ${e.message}`));
const dbM = pgAdapter(M), dbA = pgAdapter(A), dbB = pgAdapter(B);
const q = async (sql, p) => (await M.query(sql, p)).rows;
const reg = (db, code, { requestId = crypto.randomUUID(), beforeCommit, card = {} } = {}) => R.registerNewSku(db, {
  actor: 'naka@test', requestId, kind: 'single', code, values: { name: `新商品 ${code}`, standard_price: '1000', shipping_code: 'S01', tax_rate: '10' }, card,
}, { ownership: ALL_COMPANY, open: true, now: NOW, shippingRates: RATES, beforeCommit });
const stateOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = $1', [code]))[0]?.state ?? null;

try {
  await applyMigrations(dbM, { log: () => {} });
  const r0 = await runInitialLoad(dbM, planOf(), { log: () => {}, runId: 'load_pg_1', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  for (const to of ['frozen', 'company_owner']) await C.advanceCutoverPhase(dbM, { to, actor: 'test@test' });

  await ta('[0] backfill の前は new_open に進めない (段階の門)', async () => {
    await assert.rejects(() => C.advanceCutoverPhase(dbM, { to: 'new_open', actor: 'test@test' }), /cutover_prereq.*backfill_missing/);
  });

  await ta('[3] backfill の途中に SKU を足す = SKU の表の鍵で待ち、backfill の後なので状態の行が無い取引は commit で断られる', async () => {
    const p = (await q('select * from ops.registration_backfill_plan()'))[0];
    await A.query('begin');
    await A.query('select ops.backfill_sku_registrations($1, $2, $3)', [p.sku_count, p.snapshot_hash, 'test@test']);
    const b = launch((async () => {
      await B.query('begin');
      try {
        await B.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'bf-1', 'backfill の途中')`);
        await B.query('commit');
      } catch (e) { try { await B.query('rollback'); } catch { /* */ } throw e; }
    })());
    await sleep(500);
    assert.equal(b.done, false, '足す側は backfill の SKU の表の鍵で待つ');
    await A.query('commit');
    const rb = await b.promise;
    assert.match(String(rb.err?.message), /unregistered_sku/);
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'bf-1'"))[0].n, 0);
    await C.advanceCutoverPhase(dbM, { to: 'new_open', actor: 'test@test' });
  });

  await ta('[4] 遅らせた制約の trigger は commit のときに効く (取引の中では見えている・同じ取引で状態の行を作れば通る)', async () => {
    await B.query('begin');
    await B.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'dt-1', 'x')`);
    assert.equal((await B.query(`select count(*)::int as n from core.skus where code = 'dt-1'`)).rows[0].n, 1);
    await assert.rejects(() => B.query('commit'), /unregistered_sku/);
    await B.query('begin');
    const id = (await B.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'dt-2', 'x') returning sku_id`)).rows[0].sku_id;
    await B.query('select ops.create_sku_registration($1, $2)', [id, 'test@test']);
    await B.query('commit');
    assert.equal(await stateOf('dt-2'), 'draft');
  });

  await ta('[1] 同じ新しいコードを 2 人が同時に登録 = 後の人はコードの鍵で待ち、前の人の commit の後に 409 code_taken', async () => {
    const g = gate();
    const a = launch(reg(dbA, 'race-1', { beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(reg(dbB, 'race-1'));
    await sleep(500);
    assert.equal(b.done, false, 'B はコードの鍵で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.equal(rb.err?.reason, 'code_taken', rb.err?.message);
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'race-1'"))[0].n, 1);
  });

  await ta('[2a] 登録が先 (commit の前) に夜間ロードが同じコードを入れる = ロードは待って同じ行に重ね、状態は draft のまま', async () => {
    const g = gate();
    const a = launch(reg(dbA, 'lr-1', { beforeCommit: g.wait }));
    await sleep(300);
    const b = launch(runInitialLoad(dbB, planOf(['lr-1']), { log: () => {}, runId: 'load_pg_2', now: new Date('2030-01-09T03:00:00Z') }));
    await sleep(700);
    assert.equal(b.done, false, 'ロードは登録の行で待つ');
    g.open();
    assert.ok((await a.promise).ok);
    const rb = await b.promise;
    assert.ok(rb.ok?.ok, rb.err?.message);
    assert.equal((await q("select count(*)::int as n from core.skus where code = 'lr-1'"))[0].n, 1);
    assert.equal(await stateOf('lr-1'), 'draft');
  });

  await ta('[2b] 夜間ロードが先 (commit の前) に同じコードを入れた = 登録は unique で待ち、ロードの commit の後に 409 code_taken (500 にしない)', async () => {
    await B.query('begin');
    await B.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'lr-2', 'NE から')`);
    await B.query(`select ops.quarantine_unregistered_skus('load_pg_3')`);
    const a = launch(reg(dbA, 'lr-2'));
    await sleep(500);
    assert.equal(a.done, false, '登録は unique で待つ');
    await B.query('commit');
    const ra = await a.promise;
    assert.equal(ra.err?.reason, 'code_taken', ra.err?.message);
    assert.equal(await stateOf('lr-2'), 'quarantined');
  });

  await ta('[5] カードの取り込みが 2 つ: 借りの間はほかが取らない・借りが切れたらほかが取って済ませる・先の取り込みの結果は書かれない (カードは 1 枚)', async () => {
    const r = await reg(dbM, 'lease-1');
    const cards = new Map();   // 取り込み先 (product-hub の代わり。cdb_sku_id で冪等)
    const applyWith = (who) => async (ev) => {
      const k = String(ev.payload.cdb_sku_id);
      if (cards.has(k)) return { outcome: 'linked', draft_id: cards.get(k).id };
      cards.set(k, { id: cards.size + 1, by: who });
      return { outcome: 'created', draft_id: cards.get(k).id };
    };
    const g = gate();
    const slow = launch(O.runCardOutbox(dbA, async (ev) => { await g.wait(); return applyWith('A')(ev); }, { eventId: r.card.event_id, owner: 'consumer-A' }));
    await sleep(300);
    assert.deepEqual(await O.runCardOutbox(dbB, applyWith('B'), { eventId: r.card.event_id, owner: 'consumer-B' }), [], '借りの間は取らない');
    await M.query(`update ops.product_hub_outbox set leased_until = now() - interval '1 second' where event_id = $1`, [r.card.event_id]);   // 借りが切れた
    const rb = await O.runCardOutbox(dbB, applyWith('B'), { eventId: r.card.event_id, owner: 'consumer-B' });
    assert.deepEqual([rb[0].status, rb[0].recorded], ['done', true]);
    g.open();
    const ra = (await slow.promise).ok;
    assert.equal(ra[0].recorded, false, '先の取り込みの結果は書かれない');
    assert.equal(cards.size, 1);
    const ob = (await q('select status, result, lease_owner from ops.product_hub_outbox where event_id = $1', [r.card.event_id]))[0];
    assert.deepEqual([ob.status, ob.result.outcome, ob.lease_owner], ['done', 'created', null]);
  });
  await ta('[6] 画面・運用のロール (本物のログイン) は印 (GUC) を立てても状態の表・履歴・backfill の印を直接書けない (42501)・画面のロールの登録は関数を通して通る', async () => {
    const tag = crypto.randomBytes(3).toString('hex');
    const editRole = `me_edit_${tag}`, opsRole = `me_ops_${tag}`;
    for (const r of [editRole, opsRole]) {
      await M.query(`create role ${r} login password 'pw' noinherit`);
      await M.query(`grant connect on database ${dbName} to ${r}`);
    }
    for (const st of masterRegisterRoleStatements({ editRole, opsRole })) await M.query(st);
    const roleUrl = (r) => { const x = new URL(u.toString()); x.username = r; x.password = 'pw'; return x.toString(); };
    const E = await openPgClient(roleUrl(editRole)), P = await openPgClient(roleUrl(opsRole));
    for (const c of [E, P]) c.on('error', () => {});
    try {
      const d = (await q("select sku_id::text as id from core.skus where code = 'race-1'"))[0].id;
      const any = (await q("select sku_id::text as id from core.skus where code = 'p001'"))[0].id;
      const code = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e.code; } return null; };
      for (const c of [E, P]) {
        await c.query(`select set_config('ops.registration_protocol', '1', false)`);
        const codes = [
          await code(c, `update ops.master_registrations set state = 'available' where sku_id = $1`, [d]),
          await code(c, `insert into ops.master_registrations (sku_id, state, origin, created_by, state_changed_by) values ($1, 'available', 'backfill', 'x', 'x')`, [any]),
          await code(c, 'delete from ops.master_registrations where sku_id = $1', [d]),
          await code(c, `insert into ops.master_registration_events (sku_id, to_state, actor_type, actor_id) values ($1, 'available', 'human', 'x')`, [d]),
          await code(c, `insert into ops.master_registration_backfill (id, sku_count, snapshot_hash, phase, actor) values (1, 0, $1, 'frozen', 'x')`, ['a'.repeat(64)]),
          await code(c, `select ops.quarantine_unregistered_skus('x')`),
        ];
        assert.deepEqual(codes, Array(6).fill('42501'));
      }
      assert.equal(await code(E, `select ops.transition_sku_registration($1, 'cancelled', 'human', 'x', '理由')`, [d]), '42501');
      assert.equal(await stateOf('race-1'), 'draft');
      // 画面のロールで登録 = 関数を通して状態 draft を作る
      const r = await reg(pgAdapter(E), 'role-pg-1');
      assert.equal(r.state, 'draft');
      assert.equal(await stateOf('role-pg-1'), 'draft');
      // 画面のロールが関数を通さずに SKU を足した取引は commit で断られる
      await E.query('begin');
      await E.query(`insert into core.skus (company_id, sku_kind, code, name, created_by_type, created_by_id) values (1, 'set', 'role-direct', 'x', 'human', 'x')`);
      await assert.rejects(() => E.query('commit'), /unregistered_sku/);
    } finally { for (const c of [E, P]) { try { await c.end(); } catch { /* */ } } }
  });
} finally {
  for (const c of [A, B, M]) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
