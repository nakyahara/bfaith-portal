/**
 * test-master-bulk-pg.mjs — まとめて変える (apps/master-edit/bulk.mjs・PR2) を、実 PostgreSQL の独立した接続と本番と同じロール master_edit のログインで確かめる
 *   (PGlite は 1 接続 = 同時に 2 つの保存を走らせられない)
 * 固定する契約:
 *   1 本物のログイン (master_edit) で、確かめ (読むだけの取引) → 20 件ずつの保存 → 同じセットの構成品のやり直しまで通る (権限が足りている・変更の記録の db_user = master_edit)
 *   2 二重押しが 2 つの接続で同時に来る = どの商品も 1 回だけ書く (変更の記録・保存の記録)。両方とも成功の答え (片方は前の結果)・やり直しが重なっても
 *   3 一括の途中で、ほかの人が同じセットを別の接続で直す = 待ち合ってもデッドロックしない・ほかの人の変更があった構成品は「だめ」(やり直さない)・
 *     セットの税率は最後に保存した構成品の値から計算した値 (構成品の今の値と合う)
 *   4 一括の保存と、同じ商品の 1 件の保存が同時 = 片方だけ書く (後の方は version_conflict)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-bulk-pg.mjs
 *   (この PC では C:/tmp/pg-embed の run-*.mjs が使い捨ての PostgreSQL を起動して TEST_PG_URL を渡す)。localhost 以外の URL は拒む
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (まとめて変えるの本物の PostgreSQL の試験は飛ばす。PGlite の試験は scripts/test-master-bulk.mjs)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

(await import('../lib/master-owner-gate.mjs')).__setCapableForTest(OWNED_COLUMNS);
const W = await import('../lib/master-write.mjs');
const B = await import('../apps/master-edit/bulk.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const ALL = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const NOW = new Date('2030-01-10T03:00:00Z');
const TODAY = '2030-01-10';
const PW = Object.fromEntries(['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate', 'watcher', 'watch_writer'].map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const sku = (code, kind, taxRate, cost) => ({ code, name: code, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
const singles = Array.from({ length: 24 }, (_, i) => sku(`p${String(i).padStart(3, '0')}`, 'single', 0.1, 100 + i));
const plan = {
  // ps01 = p000..p003・ps02 = p000 + p010
  skus: [...singles, sku('ps01', 'set', 0.1, 406), sku('ps02', 'set', 0.1, 210)],
  setComponents: [...['p000', 'p001', 'p002', 'p003'].map((c) => ({ parentCode: 'ps01', childCode: c, qty: 1, source: 'ne' })),
    { parentCode: 'ps02', childCode: 'p000', qty: 1, source: 'ne' }, { parentCode: 'ps02', childCode: 'p010', qty: 1, source: 'ne' }],
  variationGroups: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: singles.map((s) => ({ supplierCode: '0001', skuCode: s.code })),
  primarySuppliers: singles.map((s) => ({ skuCode: s.code, supplierCode: '0001' })), reorder: { available: true, runId: 'pml_bulkpg' },
};

const dbName = `cdb_bulk_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const O = await open(null);
const clients = [O];
const q = async (sql, p) => (await O.query(sql, p)).rows;
try {
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createRoles(O, { watcherPw: PW.watcher, writerPw: PW.watch_writer });
  await createMasterEditRoles(O, { pw: PW });
  const r0 = await runInitialLoad(dbO, plan, { log: () => {}, runId: 'load_bulkpg', now: new Date('2030-01-05T03:00:00Z') });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, ALL);
  const [E1, E2, E3] = [await open('master_edit'), await open('master_edit'), await open('master_edit')];
  clients.push(E1, E2, E3);
  const [db1, db2, db3] = [E1, E2, E3].map(pgAdapter);
  assert.equal((await E1.query('select session_user::text as s')).rows[0].s, 'master_edit');

  const preview = (db, codes, field, value) => B.bulkPreview(db, { codes, field, value }, { now: NOW, suppliers: [{ code: '0001', name: 'AMC' }], actor: 'naka@test' });
  const apply = async (db, pv, { actor = 'naka@test' } = {}) => {
    const todo = pv.items.filter((x) => x.verdict === 'chg');
    const out = [];
    for (let i = 0; i < todo.length; i += B.BULK_CHUNK) {
      const part = todo.slice(i, i + B.BULK_CHUNK);
      out.push(...(await B.bulkApplyChunk(db, { actor, ticket: pv.ticket, field: pv.field, value: pv.value, reason: pv.reason, total: todo.length,
        items: part.map((x) => ({ code: x.code, token: x.token, self: x.self, mac: x.mac })) }, { open: true, now: NOW })).results);
    }
    return out;
  };
  const tax = async (code) => (await q('select tax_rate::float8 as r, tax_class as c from core.skus where code = $1', [code]))[0];
  const nEv = async (rid) => Number((await q('select count(*)::int as n from events.master_change_events where request_id = $1', [rid]))[0].n);

  await ta('[1] 本物のログイン (master_edit) で 確かめ → 20 件ずつ → 同じセットの構成品のやり直し・記録の db_user = master_edit', async () => {
    const codes = singles.map((s) => s.code);   // 24 件 = 20 + 4
    const ins = await B.bulkInspect(db1, { codes }, { now: NOW });
    assert.equal(ins.items.filter((x) => x.fields.tax_rate.verdict === 'ok').length, 24);
    const pv = await preview(db1, codes, 'tax_rate', 0.08);
    assert.equal(pv.counts.chg, 24);
    assert.deepEqual(pv.linked.map((l) => [l.code, l.after.class]).sort(), [['ps01', 'REDUCED_8'], ['ps02', 'REDUCED_8']]);
    const res = await apply(db1, pv);
    assert.equal(res.length, 24); assert.ok(res.every((r) => r.ok), JSON.stringify(res.filter((r) => !r.ok)));
    // ps01 の構成品 p001〜p003 と ps02 の p010 = 前の構成品の保存でセットの版が上がった = やり直し
    assert.deepEqual(res.filter((r) => r.retried).map((r) => r.code), ['p001', 'p002', 'p003', 'p010']);
    assert.deepEqual(await tax('ps01'), { r: 0.08, c: 'REDUCED_8' }); assert.deepEqual(await tax('ps02'), { r: 0.08, c: 'REDUCED_8' });
    assert.deepEqual((await q("select distinct db_user from events.master_change_events where source_system = 'portal_master_edit'")).map((r) => r.db_user), ['master_edit']);
  });

  await ta('[2] 二重押しが 2 つの接続で同時 = どの商品も 1 回だけ書く (やり直しが重なっても)・両方とも成功の答え', async () => {
    const codes = ['p000', 'p001', 'p002', 'p003', 'p004'];
    const pv = await preview(db1, codes, 'tax_rate', 0.1);
    const bulkId = B.opKeyOf(B.readTicket(pv.ticket));
    const [a, b] = await Promise.all([apply(db1, pv), apply(db2, pv)]);
    for (const res of [a, b]) assert.ok(res.every((r) => r.ok), JSON.stringify(res.filter((r) => !r.ok)));
    for (const c of codes) {
      const n0 = await nEv(B.bulkRequestId(bulkId, c, 0)), n1 = await nEv(B.bulkRequestId(bulkId, c, 1));
      assert.ok((n0 > 0) !== (n1 > 0), `${c}: 1 回目 ${n0}・やり直し ${n1} = どちらか片方だけ`);
      const tx = Number((await q(`select count(*)::int as n from events.master_change_events where attribute = 'tax_rate' and entity_type = 'sku'
        and entity_id = (select sku_id from core.skus where code = $1) and request_id = any($2::text[])`, [c, [B.bulkRequestId(bulkId, c, 0), B.bulkRequestId(bulkId, c, 1)]]))[0].n);
      assert.equal(tx, 1, `${c} の税率は 1 回だけ変わる`);
    }
    assert.ok([...a, ...b].some((r) => r.replayed), '片方は前の結果');
    assert.deepEqual(await tax('ps01'), { r: 0.1, c: 'STANDARD_10' });
  });

  await ta('[3] 一括の途中でほかの人が同じセットを別の接続で直す = デッドロックしない・ほかの人の変更の後の構成品は「だめ」・セットの税率は構成品の今の値と合う', async () => {
    const codes = ['p000', 'p001', 'p002', 'p003'];
    const pv = await preview(db1, codes, 'tax_rate', 0.08);
    const other = (async () => {
      await new Promise((r) => setTimeout(r, 30));
      const id = (await q("select sku_id::text as id from core.skus where code = 'ps01'"))[0].id;
      for (let i = 0; i < 5; i++) {
        try {
          const token = W.editTokenOf(await W.readCurrent(db3, id, TODAY));
          return await W.saveSku(db3, { actor: 'other@test', requestId: crypto.randomUUID(), code: 'ps01', reason: 'ほかの人', seen: { token }, values: { standard_price: 2000 + i } }, { open: true, now: NOW });
        } catch (e) { if (e.reason !== 'version_conflict') throw e; }
      }
      return null;
    })();
    const [res, o] = await Promise.all([apply(db1, pv), other]);
    assert.ok(o && o.ok, 'ほかの人の保存は通る (待ち合うだけ)');
    // ほかの人の保存より後に読み直した構成品は「だめ」(やり直さない)。どこで重なるかは時刻しだい = 結果とセットの値が合うことを見る
    for (const r of res) if (!r.ok) assert.equal(r.error.reason, 'version_conflict', JSON.stringify(r));
    const comps = (await q(`select k.tax_rate::text as t from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id where c.parent_sku_id = (select sku_id from core.skus where code = 'ps01')`)).map((r) => r.t);
    const want = (await import('../lib/master-set-rules.js')).deriveSetTaxCdb(comps);
    const got = await tax('ps01');
    assert.deepEqual(got, { r: want.taxRate, c: want.taxClass }, `セットの税率 = 構成品の今の値から (${comps.join(',')})`);
  });

  await ta('[4] 一括の保存と同じ商品の 1 件の保存が同時 = 片方だけ書く (後の方は version_conflict)', async () => {
    const pv = await preview(db1, ['p020'], 'standard_price', 3000);
    const token = W.editTokenOf(await W.readCurrent(db2, (await q("select sku_id::text as id from core.skus where code = 'p020'"))[0].id, TODAY));
    const [res, one] = await Promise.allSettled([
      apply(db1, pv),
      W.saveSku(db2, { actor: 'other@test', requestId: crypto.randomUUID(), code: 'p020', reason: 'ほかの人', seen: { token }, values: { standard_price: 4000 } }, { open: true, now: NOW }),
    ]);
    const bulkOk = res.status === 'fulfilled' && res.value[0].ok;
    const oneOk = one.status === 'fulfilled';
    assert.ok(bulkOk !== oneOk, `片方だけ (一括 ${bulkOk}・1 件 ${oneOk})`);
    if (!bulkOk) assert.equal(res.value[0].error.reason, 'version_conflict');
    else assert.equal(one.reason.reason, 'version_conflict');
    const price = Number((await q("select standard_price_jpy from core.skus where code = 'p020'"))[0].standard_price_jpy);
    assert.equal(price, bulkOk ? 3000 : 4000);
  });
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
