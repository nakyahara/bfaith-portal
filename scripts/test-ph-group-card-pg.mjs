/**
 * test-ph-group-card-pg.mjs — product-hub のまとまりのカードの取り込み (Company DB構想 20 v7 §⑤ / §⑩ の PR-4) を、実 PostgreSQL の独立した接続と本物のログインで確かめる
 *   (PGlite は 1 接続 = 同時に借りる・借りの期限・ほかの接続の取り込みは試せない。PGlite の試験は scripts/test-ph-group-card.mjs)
 *
 * 固定する契約:
 *   1 2 つの接続 (本物のログイン master_edit) が同時にまとまりの知らせを取り込む = 同じ知らせは 1 つの接続だけが借りる・全部 done・
 *     カードはまとまりで 1 枚・最後は一番大きい revision の姿
 *   2 順番が逆 (後の revision を別の接続が先に取り込む) = 前の revision は stale で済み (古い姿に戻さない)
 *   3 借りたまま落ちた知らせ = 期限の間はほかの接続が取らない・借りた人でなければ結果を書けない・期限の後はほかの接続が取り込める (冪等 = 2 枚にならない)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-ph-group-card-pg.mjs
 *   🚨 使い捨ての PostgreSQL だけ (新しい DB を作って最後に消す・ロール master_* をクラスタに作る)。localhost 以外の URL は拒む。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (まとまりのカードの実 PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-ph-group-card.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'vg4-group-card-pg-'));
process.env.DATA_DIR = DATA_DIR;
const { openPgClient, pgAdapter, applyMigrations } = await import('./company-db/migrate.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const O = await import('../lib/product-hub-outbox.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const PG = await import('../apps/product-hub/services/cdb-group-intake.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, ...x });
const rep = (code) => ({ representativeCode: code, representativeState: 'value' });
const plan = {
  skus: [sku('p001', '子 1【赤】', rep('grpa')), sku('p002', '子 2【青】', rep('grpa')), sku('p003', '子 3【白】', rep('grpb'))],
  variationGroups: [{ code: 'grpa', name: '札 a', childCodes: ['p001', 'p002'], status: 'active' }, { code: 'grpb', name: '札 b', childCodes: ['p003'], status: 'active' }],
  setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [],
};
const dbName = `cdb_vg4_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  await createMasterEditRoles(M, { pw: PW });
  const r0 = await runInitialLoad(dbM, plan, { log: () => {}, runId: 'load_vg4_pg', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  const [A, B] = [await open('master_edit'), await open('master_edit')];
  clients.push(A, B);
  const [dbA, dbB] = [A, B].map(pgAdapter);
  const ph = PHDB.getDB();
  const apply = (ev) => PG.applyCdbGroupEvent(ev);
  const tagOf = async (code) => (await q(`select product_id::text as id from core.products p where p.display_code = $1 and not exists (select 1 from core.skus k where k.product_id = p.product_id)`, [code]))[0].id;
  const [ga, gb] = [await tagOf('grpa'), await tagOf('grpb')];
  async function snap(gid, revision) {
    const g = (await q('select p.product_id::text as pid, p.display_code, p.name from core.products p where p.product_id = $1', [gid]))[0];
    const kids = await q(`select s.sku_id::text as sku_id, s.code, s.name, s.standard_price_jpy::text as price from core.products c join core.skus s on s.product_id = c.product_id and s.sku_kind = 'single'
      where c.parent_product_id = $1 order by s.sku_id`, [gid]);
    return { schema: 'ph-group-v1', revision, created_by: 'naka@test', group: { product_id: g.pid, sku_id: null, code: g.display_code, name: g.name, kind: 'tag' }, axes: [], options: [],
      children: kids.map((k) => ({ sku_id: k.sku_id, code: k.code, name: k.name, price: k.price == null ? null : Number(k.price), choices: {}, jans: [] })), cancelled_children: [],
      common: { shipping: null, amazon_url: null, asin: null, official_url: null, reference_urls: [], yahoo: null } };
  }
  const enqueue = async (gid, rev) => (await q('select ops.enqueue_group_snapshot($1, $2, $3::jsonb, gen_random_uuid(), $4)::text as id', [gid, rev, JSON.stringify(await snap(gid, rev)), 'naka@test']))[0].id;
  const draftOf = (code) => ph.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code);

  await ta('[1] 2 つの接続 (本物のログイン) が同時に取り込む = 同じ知らせは 1 つだけが借りる・全部 done・カードはまとまりで 1 枚・最後は一番大きい revision', async () => {
    const evs = [await enqueue(ga, 1), await enqueue(ga, 2), await enqueue(gb, 1), await enqueue(ga, 4), await enqueue(gb, 3)];
    const [ra, rb] = await Promise.all([O.runGroupOutbox(dbA, apply, { limit: 3, owner: 'vg4-a' }), O.runGroupOutbox(dbB, apply, { limit: 3, owner: 'vg4-b' })]);
    const all = [...ra, ...rb];
    assert.equal(all.length, evs.length, `借りた数 ${ra.length} + ${rb.length}`);
    assert.deepEqual(new Set(all.map((x) => x.event_id)), new Set(evs));
    assert.ok(all.every((x) => x.status === 'done' && x.recorded), JSON.stringify(all.map((x) => [x.revision, x.status, x.error])));
    assert.deepEqual((await q('select distinct status from ops.product_hub_outbox where group_product_id is not null')).map((r) => r.status), ['done']);
    assert.deepEqual([ph.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE cdb_group_product_id = ?`).get(Number(ga)).c, draftOf('grpa').cdb_group_revision], [1, 4]);
    assert.deepEqual([ph.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE cdb_group_product_id = ?`).get(Number(gb)).c, draftOf('grpb').cdb_group_revision], [1, 3]);
    assert.deepEqual(ph.prepare('SELECT code FROM ph_cdb_group_children WHERE draft_id = ? AND active = 1 ORDER BY code').all(draftOf('grpa').id).map((r) => r.code), ['p001', 'p002']);
  });

  await ta('[2] 順番が逆: 後の revision (7) を接続 A が先に取り込み、前の revision (6) を接続 B が後で = stale で済み (古い姿に戻さない)', async () => {
    const e6 = await enqueue(ga, 6);
    const e7 = await enqueue(ga, 7);
    const r7 = await O.runGroupOutbox(dbA, apply, { eventId: e7 });
    const r6 = await O.runGroupOutbox(dbB, apply, { eventId: e6 });
    assert.deepEqual([r7[0].result.outcome, r6[0].result.outcome, r6[0].status], ['replaced', 'stale', 'done']);
    assert.equal(draftOf('grpa').cdb_group_revision, 7);
  });

  await ta('[3] 借りたまま落ちた = 期限の間はほかが取らない・借りた人でなければ書けない・期限の後はほかが取り込む (2 枚にならない)', async () => {
    const e8 = await enqueue(ga, 8);
    // A が借りて、結果を書かずに落ちた (取り込みもしていない)
    const lease = (await A.query(`select event_id::text as id from ops.claim_outbox_events('vg4-crash', 'auto', 'variation_group', $1::uuid, null, 1, 600, 5)`, [e8])).rows;
    assert.equal(lease.length, 1);
    assert.deepEqual(await O.runGroupOutbox(dbB, apply, {}), [], '借りている途中の知らせを取った');
    assert.equal((await B.query(`select ops.finish_card_event($1::uuid, 'vg4-b', 'done', '{}'::jsonb, null) as ok`, [e8])).rows[0].ok, false, '借りた人でないのに書けた');
    // 期限が過ぎた (持ち主が時計を進めた形)
    await M.query(`update ops.product_hub_outbox set leased_until = now() - interval '1 second' where event_id = $1`, [e8]);
    const r = await O.runGroupOutbox(dbB, apply, { owner: 'vg4-b2' });
    assert.deepEqual(r.map((x) => [x.event_id, x.status, x.result.outcome, x.recorded]), [[e8, 'done', 'replaced', true]]);
    assert.equal((await A.query(`select ops.finish_card_event($1::uuid, 'vg4-crash', 'failed', null, 'x') as ok`, [e8])).rows[0].ok, false, '落ちた人が後から結果を上書きした');
    assert.deepEqual([ph.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE cdb_group_product_id = ?`).get(Number(ga)).c, draftOf('grpa').cdb_group_revision], [1, 8]);
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せなかった: ${e.message}`); }
  await admin.end();
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
