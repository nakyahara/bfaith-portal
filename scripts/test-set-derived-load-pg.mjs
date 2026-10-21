/**
 * test-set-derived-load-pg.mjs — 夜間ロードの後のセットの導く値の計算し直し (lib/master-write.mjs の recomputeSetAfterLoad・recomputeSetsAfterLoad) を、
 * 実 PostgreSQL の独立した接続で確かめる (鍵の順・待ち合わせ。PGlite は 1 接続なので書けない)
 *
 * 固定する契約:
 *   [P1] 構成品を直す取引 (画面の保存と同じ鍵 = マスタの書き込みの共有 → SKU の鍵) が途中 = 計算し直しは SKU の鍵で待ち、その commit の後の構成品の原価で計算する (古い値で書かない)
 *   [P2] 夜間ロード (マスタの書き込みの排他の鍵) の最中 = 計算し直しは短く待って nightly_load (何も書かない・投げない)
 *   [P3] 計算し直しが鍵を持っている間 (commit の前)、同じセットの構成品の保存は待つ = 計算し直しの後に保存 (デッドロックしない)
 *   [P4] 本物の夜間ロード (runInitialLoad) の commit の後に計算し直す (pg の接続で begin / commit が通る)
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-set-derived-load-pg.mjs
 *   (使い捨ての PG: node C:/tmp/pg-embed/run-pg.mjs scripts/test-set-derived-load-pg.mjs <リポジトリ>)
 *   🚨 使い捨ての PostgreSQL だけ。localhost 以外の URL は拒む (本番を渡さない)。TEST_PG_URL が無ければ飛ばす
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { forceNewOpen } from './fixtures/master-widen.mjs';
import { MASTER_OWNERSHIP } from '../config/master-ownership.mjs';
import { SKU_LOCK_SQL } from '../apps/master-decisions/ne-csv-lock.mjs';
import { MASTER_WRITE_EXCLUSIVE_LOCK_SQL, MASTER_WRITE_SHARED_LOCK_SQL } from '../lib/master-cutover.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (セットの導く値の計算し直しの実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
const watchdog = setTimeout(() => { console.error('  NG  試験が 600 秒で終わらない (止める)'); process.exit(1); }, 600000);
watchdog.unref();
const W = await import('../lib/master-write.mjs');

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const quiet = () => {};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ALL_LOAD = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'load']));
const sku = (code, kind, jpy, x = {}) => ({ code, name: code, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: kind === 'set' ? null : 3,
  cost: { jpy, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, ...x });
const planOf = (comps) => ({ skus: [sku('pa1', 'single', 300), sku('pa2', 'single', 500), sku('setx', 'set', 600)], variationGroups: [],
  setComponents: comps.map(([child, qty]) => ({ parentCode: 'setx', childCode: child, qty, source: 'ne' })), listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] });
/** 状態を知らない約束 (promise) が終わったか */
const settled = async (p, ms = 300) => Promise.race([p.then(() => true, () => true), sleep(ms).then(() => false)]);

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
try {
  const name = `cdb_setder_${crypto.randomBytes(4).toString('hex')}`;
  await admin.query(`create database ${name}`);
  dbNames.push(name);
  const u = new URL(url); u.pathname = `/${name}`;
  const open = async () => { const c = await openPgClient(u.toString()); c.on('error', (e) => console.error(`[pg] ${e.message}`)); clients.push(c); return c; };
  const O = await open();
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: quiet });
  await createMasterEditRoles(O, {});
  const r0 = await runInitialLoad(dbO, planOf([['pa1', 2]]), { log: quiet, runId: `setder_load0_${name}`, host: 'test', ownership: ALL_LOAD, now: new Date(Date.now() - 3 * 86400e3) });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, MASTER_OWNERSHIP);
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const sid = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id;
  const openCost = async (code) => Number((await q(`select c.cost_jpy from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = $1 and c.valid_to is null`, [code]))[0].cost_jpy);
  const setComps = async (rows) => {
    await O.query('delete from core.sku_components where parent_sku_id = $1', [await sid('setx')]);
    for (const [child, qty] of rows) await O.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, $3, 'ne')`, [await sid('setx'), await sid(child), qty]);
  };
  const X = await open();   // 計算し直し (夜間ロードの後の接続)
  const dbX = pgAdapter(X);
  const S = await open();   // 構成品を直す取引 (画面の保存と同じ鍵の順)
  const setx = await sid('setx');

  await ta('[P1] 構成品を直す取引が途中 = 計算し直しは SKU の鍵で待ち、commit の後の構成品の原価で計算する', async () => {
    await setComps([['pa1', 1], ['pa2', 1]]);   // 夜間ロードが構成を書き直した (300 + 500 = 800 のはず)
    await S.query('begin');
    await S.query(MASTER_WRITE_SHARED_LOCK_SQL);
    await S.query(SKU_LOCK_SQL, [await sid('pa1')]);
    await S.query(`update core.sku_costs set cost_jpy = 400 where sku_id = $1 and valid_to is null`, [await sid('pa1')]);
    const p = W.recomputeSetAfterLoad(dbX, setx, { ownership: MASTER_OWNERSHIP, runId: 'r_p1', host: 'render-nightly' });
    assert.equal(await settled(p, 800), false, '計算し直しは SKU の鍵で待つ');
    await S.query('commit');
    const r = await p;
    assert.deepEqual([r.changed, r.reason], [true, 'recomputed'], JSON.stringify(r));
    assert.equal(await openCost('setx'), 900, 'commit の後の構成品の原価 (400 + 500)');
  });

  await ta('[P2] 夜間ロード (マスタの書き込みの排他) の最中 = 短く待って nightly_load (何も書かない)', async () => {
    await setComps([['pa2', 2]]);
    await S.query('begin');
    await S.query(MASTER_WRITE_EXCLUSIVE_LOCK_SQL);
    try {
      const t = Date.now();
      const r = await W.recomputeSetAfterLoad(dbX, setx, { ownership: MASTER_OWNERSHIP, runId: 'r_p2' });
      assert.deepEqual([r.changed, r.reason], [false, 'nightly_load'], JSON.stringify(r));
      assert.ok(Date.now() - t >= 2500, '短く待つ (3 秒)');
    } finally { await S.query('rollback'); }
    assert.equal(await openCost('setx'), 900, '何も書かない');
    // 鍵が空いた後 = 計算し直す (次の晩と同じ)
    const r = await W.recomputeSetAfterLoad(dbX, setx, { ownership: MASTER_OWNERSHIP, runId: 'r_p2b' });
    assert.equal(r.changed, true, JSON.stringify(r));
    assert.equal(await openCost('setx'), 1000);
  });

  await ta('[P3] 計算し直しが鍵を持っている間は、同じセットの構成品の保存が待つ (計算し直しの後に保存・デッドロックしない)', async () => {
    await setComps([['pa1', 1], ['pa2', 1]]);
    let release;
    const gate = new Promise((r) => { release = r; });
    let holding = false;
    const p = W.recomputeSetAfterLoad(dbX, setx, { ownership: MASTER_OWNERSHIP, runId: 'r_p3', beforeCommit: async () => { holding = true; await gate; } });
    for (let i = 0; i < 50 && !holding; i++) await sleep(100);
    assert.equal(holding, true, '計算し直しが commit の前で止まっている');
    await S.query('begin');
    await S.query(MASTER_WRITE_SHARED_LOCK_SQL);
    const s = S.query(SKU_LOCK_SQL, [await sid('pa2')]);
    assert.equal(await settled(s, 500), false, '保存は SKU の鍵で待つ');
    release();
    const r = await p;
    assert.equal(r.changed, true, JSON.stringify(r));
    await s;
    await S.query('rollback');
    assert.equal(await openCost('setx'), 900);
  });

  await ta('[P4] 本物の夜間ロードの commit の後に計算し直す (pg の接続で)', async () => {
    const r = await runInitialLoad(dbO, planOf([['pa2', 3]]), { log: quiet, runId: `setder_load1_${name}`, host: 'render-nightly', ownership: MASTER_OWNERSHIP });
    assert.equal(r.ok, true, r.error);
    assert.equal(r.set_derived.changed_parents, 1);
    assert.equal(r.set_derived.changed, 1, JSON.stringify(r.set_derived));
    assert.equal(await openCost('setx'), 1500);
    const ev = await q(`select distinct actor_type, actor_id, source_system, run_id, reason_text from events.master_change_events where run_id = $1 and reason_text = $2`, [r.run_id, W.LOAD_DERIVED_WHY]);
    assert.deepEqual(ev, [{ actor_type: 'system', actor_id: 'render-nightly', source_system: 'company_db_load', run_id: r.run_id, reason_text: W.LOAD_DERIVED_WHY }]);
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); } }
  await admin.end();
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
