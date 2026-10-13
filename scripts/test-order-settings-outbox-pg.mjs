/**
 * test-order-settings-outbox-pg.mjs — 発注の設定の知らせ (0075・CompanyDB構想 21 C1) の**同時実行**と**ロールの権限**を、実 PostgreSQL の独立した接続で確かめる
 *   (PGlite は 1 接続なので「同時に来たとき」を書けない)。接続は本番と同じロール master_edit でログインする (create-master-edit-roles.mjs・パスワードは試験の回ごと)。
 * 固定する契約:
 *   C1 2 つの接続が同時に借りる = 同じ知らせを 2 つが取らない (for update skip locked)・合わせると全部
 *   C2 借りの間はほかが取らない・借りが切れたらほかが取る・前の借り手の結果は書けない・今の借り手は書ける
 *   C3 積むのは登録と同じ取引だけ: commit した登録に別の取引 (別の接続) から積む = 断る (xmin)・同じ取引なら積める・画面のロールは表に直接書けない (本物のログイン)
 *   C4 人が閉じる vs 借りる が同時: 借りる取引がまだ commit していない間に閉じる = 待ってから「借りの最中」で閉じない (false)
 *   C5 ほかの取引が行を握っていても、借りるのは待たずに飛ばす (夜間の段が止まらない)
 * 使い方: node C:/tmp/pg-embed/run-posync.mjs scripts/test-order-settings-outbox-pg.mjs <作業の木> (使い捨ての PostgreSQL・TEST_PG_URL を渡す)
 *   🚨 localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (発注の設定の知らせの実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-order-settings-outbox.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
const killer = setTimeout(() => { console.error('NG 時間切れ (300 秒)'); process.exit(3); }, 300e3);
killer.unref();

const X = await import('../lib/order-settings-outbox.mjs');
let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; return { ok: r }; }, (e) => { s.done = true; return { err: e }; }); return s; };
const codeOf = async (c, sql, p) => { try { await c.query(sql, p); } catch (e) { return e.code; } return null; };

const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const N = 30;
const CODES = Array.from({ length: N }, (_, i) => `pos-${String(i + 1).padStart(3, '0')}`);
const sku = (code) => ({ code, name: `NE の ${code}`, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const plan = {
  skus: [...CODES, 'pos-late', 'pos-same', 'pos-x1'].map(sku), variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
};
const dbName = `posync_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;
const payload = (code) => X.orderPayloadOf(code, { supplier_code: '0001', patch: { order_lot: '6' }, new_condition: null, new_material: null });

/** 持ち主の 1 つの取引で「登録の記録 (sku_create・done)」を書いてから知らせを積む (登録の関数の代わり = 同じ取引の印だけを使う) */
async function seed(c, code, { commitBeforeEnqueue = false } = {}) {
  const rid = crypto.randomUUID();
  const s = (await c.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0];
  await c.query('begin');
  await c.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, result, started_at)
    values ($1, 1, 'sku_create', $2, $3, 'naka@test', $4, 'done', '{}'::jsonb, now())`, [rid, code, s.id, 'a'.repeat(64)]);
  if (commitBeforeEnqueue) { await c.query('commit'); return { rid, skuId: s.id }; }
  const r = (await c.query('select ops.enqueue_order_settings($1::uuid, $2, $3::jsonb) as r', [rid, 'naka@test', JSON.stringify([{ code, payload: payload(code) }])])).rows[0].r;
  await c.query('commit');
  return { rid, skuId: s.id, eventId: r[0].event_id };
}
const claimSql = `select event_id::text as event_id, code from ops.claim_order_settings_events($1, $2, null, $3::uuid, $4, $5, 5)`;

try {
  await applyMigrations(pgAdapter(M), { log: () => {} });
  await createMasterEditRoles(M, { pw: PW });
  const r0 = await runInitialLoad(pgAdapter(M), plan, { log: () => {}, runId: 'load_posync_1', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  const [A, B] = [await open('master_edit'), await open('master_edit')];
  clients.push(A, B);
  for (const c of CODES) await seed(M, c);
  assert.equal((await q(`select count(*)::int as n from ops.order_settings_outbox where status = 'pending'`))[0].n, N);

  await ta('[C1] 2 つの接続が同時に借りる = 重ならない・合わせると全部 (A が取引の中で借りたまま = B は待たずに残りを取る)', async () => {
    await A.query('begin');
    const a = (await A.query(claimSql, ['conc-a', 'auto', null, 20, 120])).rows;
    const t0 = Date.now();
    const b = (await B.query(claimSql, ['conc-b', 'auto', null, 200, 120])).rows;
    assert.ok(Date.now() - t0 < 3000, `B が A を待った (${Date.now() - t0} ms)`);
    await A.query('commit');
    assert.equal(a.length, 20);
    assert.equal(b.length, N - 20);
    const ids = new Set([...a, ...b].map((x) => x.event_id));
    assert.equal(ids.size, N, '同じ知らせを 2 つが借りた');
    // 本当に同時 (Promise.all) にもう一度: 借りが切れた後に 2 つが同時に取る
    await M.query(`update ops.order_settings_outbox set leased_until = now() - interval '1 second' where status = 'pending'`);
    const [x, y] = await Promise.all([A.query(claimSql, ['conc-x', 'auto', null, 200, 120]), B.query(claimSql, ['conc-y', 'auto', null, 200, 120])]);
    const xs = x.rows.map((r) => r.event_id); const ys = y.rows.map((r) => r.event_id);
    assert.equal(xs.filter((id) => ys.includes(id)).length, 0, '同時に借りて重なった');
    assert.equal(xs.length + ys.length, N);
  });

  await ta('[C2] 借りの間はほかが取らない・切れたらほかが取る・前の借り手の結果は書けない・今の借り手は書ける (閉じた後はだれも変えられない)', async () => {
    const { eventId: ev } = await seed(M, 'pos-same');
    assert.equal((await A.query(claimSql, ['lease-a', 'auto', ev, 1, 10])).rows.length, 1);
    assert.equal((await B.query(claimSql, ['lease-b', 'manual', ev, 1, 10])).rows.length, 0, '借りの間に B が取った');
    await M.query(`update ops.order_settings_outbox set leased_until = now() - interval '1 second' where event_id = $1::uuid`, [ev]);
    assert.equal((await B.query(claimSql, ['lease-b', 'auto', ev, 1, 10])).rows.length, 1);
    const fin = async (c, owner, st) => (await c.query('select ops.finish_order_settings_event($1::uuid, $2, $3, $4::jsonb, $5) as ok', [ev, owner, st, st === 'done' ? '{"outcome":"written"}' : null, st === 'done' ? null : 'x'])).rows[0].ok;
    assert.equal(await fin(A, 'lease-a', 'failed'), false, '前の借り手が書けた');
    assert.equal(await fin(B, 'lease-b', 'done'), true);
    assert.equal(await fin(B, 'lease-b', 'failed'), false, '閉じた知らせを変えた');
    assert.deepEqual((await q('select status, attempts, lease_owner from ops.order_settings_outbox where event_id = $1::uuid', [ev]))[0], { status: 'done', attempts: 2, lease_owner: null });
  });

  await ta('[C3] 積むのは登録と同じ取引だけ: commit した登録に別の接続から積む = 断る・同じ取引なら積める・画面のロール (本物のログイン) は表に直接書けない / 状態を直せない', async () => {
    const { rid } = await seed(M, 'pos-late', { commitBeforeEnqueue: true });
    const items = JSON.stringify([{ code: 'pos-late', payload: payload('pos-late') }]);
    await A.query('begin');
    await assert.rejects(() => A.query('select ops.enqueue_order_settings($1::uuid, $2, $3::jsonb)', [rid, 'naka@test', items]), /not_in_registration/);
    await A.query('rollback');
    await M.query('begin');
    await assert.rejects(() => M.query('select ops.enqueue_order_settings($1::uuid, $2, $3::jsonb)', [rid, 'naka@test', items]), /not_in_registration/, '持ち主でも別の取引からは積めない');
    await M.query('rollback');
    assert.equal((await q(`select count(*)::int as n from ops.order_settings_outbox where code = 'pos-late'`))[0].n, 0);
    assert.equal(await codeOf(A, `insert into ops.order_settings_outbox (sku_id, code, registration_kind, registration_request_id, delivery_request_id, schema_version, payload, payload_hash, created_by)
      select sku_id, code, 'single', $1::uuid, $1::uuid, 'po-settings-v1', '{}'::jsonb, repeat('0', 64), 'naka@test' from core.skus where code = 'pos-late'`, [rid]), '42501');
    assert.equal(await codeOf(A, `update ops.order_settings_outbox set status = 'done'`), '42501');
    assert.equal(await codeOf(A, `select ops.order_settings_payload_problem('{}'::jsonb)`), '42501');
    assert.equal((await A.query(`select count(*)::int as n from ops.order_settings_outbox`)).rows[0].n > 0, true, '画面のロールが読めない');
  });

  await ta('[C4] 人が閉じる vs 借りる が同時: 借りる取引がまだ commit していない = 閉じるは待つ → 借りの最中なので閉じない (false)・借りが終われば閉じられる', async () => {
    const { eventId: ev } = await seed(M, 'pos-x1');
    await M.query(`update ops.order_settings_outbox set status = 'failed', attempts = 1 where event_id = $1::uuid`, [ev]);
    await A.query('begin');
    assert.equal((await A.query(claimSql, ['race-a', 'sweep', ev, 1, 120])).rows.length, 1);
    const d = launch(B.query('select ops.dismiss_order_settings_event($1::uuid, $2, $3) as ok', [ev, 'naka@test', '同時に閉じる']));
    await sleep(400);
    assert.equal(d.done, false, '閉じるが借りる取引を待たなかった');
    await A.query('commit');
    const r = await d.promise;
    assert.ok(r.ok, String(r.err));
    assert.equal(r.ok.rows[0].ok, false, '借りの最中に閉じた');
    await A.query('select ops.finish_order_settings_event($1::uuid, $2, $3, null, $4)', [ev, 'race-a', 'failed', 'まだ書けない']);
    assert.equal((await B.query('select ops.dismiss_order_settings_event($1::uuid, $2, $3) as ok', [ev, 'naka@test', '商品の画面で入れた'])).rows[0].ok, true);
  });

  await ta('[C5] ほかの取引が行を握っていても、借りるのは待たずに飛ばす (夜間の段・画面を開いたときの届け直しが止まらない)', async () => {
    await M.query(`update ops.order_settings_outbox set leased_until = now() - interval '1 second' where status = 'pending'`);
    const ev = (await q(`select event_id::text as id from ops.order_settings_outbox where status = 'pending' order by created_at limit 1`))[0].id;
    await M.query('begin');
    await M.query('select 1 from ops.order_settings_outbox where event_id = $1::uuid for update', [ev]);
    await B.query(`set statement_timeout = '3s'`);
    const t0 = Date.now();
    const got = (await B.query(claimSql, ['skip-b', 'auto', null, 200, 120])).rows.map((r) => r.event_id);
    assert.ok(Date.now() - t0 < 2500, '握られた行を待った');
    assert.ok(!got.includes(ev), '握られた行を借りた');
    assert.ok(got.length >= 1);
    await M.query('rollback');
    await B.query(`set statement_timeout = 0`);
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database if exists ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  try { await admin.end(); } catch { /* */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.log('NG があります');
