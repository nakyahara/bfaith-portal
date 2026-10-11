/**
 * test-master-gone-pg.mjs — 0070 (業務で使える商品を「NE から消えた」でやめる) を実 PostgreSQL の独立した接続と本物のログインのロールで確かめる
 *
 * 固定する契約:
 *   [R]  本物のログイン: master_edit (画面の lib) と master_ops (CLI) = やめられる / master_ops が ops.transition_sku_registration を直に呼んでも available はやめられない /
 *        watcher = 確かめは読める・動かす関数は 42501 / どのロールも数えの表を直に読めない (watcher だけ読める)
 *   [C1] 競り合い (照合 ② の記録が先): 記録の取引 (watch_writer・その取得にコードがある) が開いている間、やめる関数は許可の鍵で待つ → 記録が commit された後に
 *        「一番新しい取得にある」で断る (待つ前に読んだ古い取得の数で通さない)
 *   [C2] 競り合い (やめるが先): やめる取引が開いている間、照合 ② の記録は待つ → やめるが commit された後に記録が進む (どちらも止まらない・デッドロックしない)
 *   [C3] 夜間ロード (マスタの書き込みの鍵の排他) の間は、やめる関数は待つ (lock_timeout = 55P03 = 画面の「ほかの処理が同じ商品を使っています」)
 *   [H1〜H5] 🆕 #1683 Codex R1 High 2: 参照を作る道 (構成の子・構成の依頼・代表 (親)・出品の構成) とやめる関数の競り合い。どちらが先でも
 *        「やめた SKU への参照」は残らない (参照が先 = やめるが待って確かめで断る / やめるが先 = 参照が待って sku_cancelled で断る)・デッドロックしない
 *   [E]  夜間ロード (持ち主 = load の列): 材料に、やめた SKU の構成の子・代表・出品の構成があっても、ロードは止まらず飛ばす (skip の理由)・DB に参照は入らない
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-gone-pg.mjs
 *   (使い捨ての PG: node C:/tmp/pg-embed/run-gone.mjs scripts/test-master-gone-pg.mjs <リポジトリ>)
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
import { cancelSkuGoneFromNe, listGoneChecks } from '../lib/master-gone.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0070 の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
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
async function stillWaiting(p, ms = 600) { const t = Symbol('t'); return (await Promise.race([p.then(() => 'done', () => 'done'), sleep(ms).then(() => t)])) === t; }

const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', 'skus.sku_kind': 'company', 'listing_components.amazon': 'company' };   // 10/9 の本番の形
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
// zgone (先に作る = sku_id が小さい) と agone (後 = 大きい) = コードの順と sku_id の順が逆 ([D] の鍵の順の試験)
const GONE = ['zgone', 'p01', 'p02', 'p03', 'p04', 'p05', 'p06', 'p07', 'p08', 'p09', 'p10', 'p11', 'p12', 'agone'];
const setSku = (code) => ({ ...single(code), kind: 'set', cost: { jpy: 100, source: 'set_calc', status: 'COMPLETE' } });
const rakuten = (code, comps) => ({ mall: 'rakuten', shopCode: 'main', listingCode: code, status: 'active', components: comps.map((c) => ({ code: c, qty: 1, resolution: 'exact', evidence: { source: 'rakuten_sku_map' } })) });
const PLAN = { skus: [...[...GONE, 'keep', 'k2', 'k3', 'k4', 'k5', 'k6'].map(single), setSku('pset'), setSku('pset2'), setSku('pset3')],
  variationGroups: [],
  setComponents: [{ parentCode: 'pset', childCode: 'keep', qty: 1, source: 'ne' }, { parentCode: 'pset2', childCode: 'k4', qty: 1, source: 'ne' }, { parentCode: 'pset2', childCode: 'k5', qty: 1, source: 'ne' },
    { parentCode: 'pset3', childCode: 'k6', qty: 1, source: 'ne' }],
  listings: [rakuten('r-l1', ['k4', 'k5']), rakuten('r-l2', ['k6'])], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] };
let runN = 0;
const runId = () => `mc_20301011T${String(++runN).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;
let tick = 0;
const jstToday = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
const fetchAt = (day) => { tick++; return day === 0 ? new Date(Math.max(Date.now() - 20 * 60e3, Date.parse(`${jstToday()}T00:01:00+09:00`)) + tick * 1000) : new Date(Date.parse(`${jstToday()}T12:00:00+09:00`) + day * 86400e3 + tick * 1000); };

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
try {
  const name = `cdb_gone_${crypto.randomBytes(4).toString('hex')}`;
  await admin.query(`create database ${name}`);
  dbNames.push(name);
  const u = new URL(url); u.pathname = `/${name}`;
  const roleUrl = (role, pw) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
  const done = async (...cs) => { for (const c of cs) { try { await c.end(); } catch { /* */ } } };   // 使った接続を閉じる (master_edit の接続は 5 まで)
  const open = async (role) => { const c = await openPgClient(role ? roleUrl(role, PW[role] ?? WPW[role]) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); clients.push(c); return c; };
  const O = await open(null);
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: PW });
  await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  const r0 = await runInitialLoad(dbO, PLAN, { log: () => {}, runId: `gone_load_${name}`, host: 'test' });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, BASE);
  const [WA, WW, ME, MO] = [await open('watcher'), await open('watch_writer'), await open('master_edit'), await open('master_ops')];
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const sid = async (code) => String((await q('select sku_id from core.skus where code = $1', [code]))[0].sku_id);
  const regOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))[0]?.state;
  const obs = async (gone) => {
    const rows = await q("select code_norm, sku_kind from core.skus where company_id = 1 and sku_kind in ('single', 'set') order by 1");
    return { format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' }, trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' },
      rows: rows.filter((r) => !gone.includes(r.code_norm)).map((r) => (r.sku_kind === 'set' ? [r.code_norm, 'set', null, null, null] : [r.code_norm, 'single', 'ok', null, null])) };
  };
  /** 別の日の取得の数え (試験だけ: 記録の回を置いて本物の ops._record_ne_absence を持ち主が流す) */
  const feed = async (gone, day) => {
    const when = fetchAt(day).toISOString();
    await O.query('begin');
    try {
      await O.query("select set_config('ops.parent_gate_protocol', '1', true)");
      const id = (await O.query(`insert into ops.master_parent_gate_results (company_id, compare_run_id, ne_generation_id, ne_raw_hash, products_complete_at, setproducts_complete_at, material_generation_id,
          evidence_sha256, obs_hash, counts, counted, excluded, samples, owner_at_record, recorded_by) values (1, $1, $2, repeat('1', 64), $3, $3, 'mat_x', repeat('2', 64), repeat('3', 64), '{}'::jsonb, 0, '{}'::jsonb, '{}'::jsonb, 'load', 't') returning result_id`,
        [runId(), `ne_${crypto.randomBytes(4).toString('hex')}`, when])).rows[0].result_id;
      await O.query('select ops._record_ne_absence($1, $2::jsonb)', [id, JSON.stringify(await obs(gone))]);
      await O.query('commit');
    } catch (e) { await O.query('rollback'); throw e; }
  };
  const recordSql = 'select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb) as r';
  const recordArgs = async (gone) => {
    const when = new Date(Date.now() - 1000).toISOString();
    return [runId(), JSON.stringify({ generation_id: `ne_${crypto.randomBytes(3).toString('hex')}`, raw_hash: hex('c'), products_complete_at: when, setproducts_complete_at: when }), 'mat_x', hex('d'), JSON.stringify(await obs(gone))];
  };
  const record = async (gone) => WW.query(recordSql, await recordArgs(gone));
  for (const day of [-2, -1, 0]) await feed(GONE, day);
  const cancel = (client, skuId, reason = 'NE で消えた') => client.query('select ops.cancel_sku_gone_from_ne($1::uuid, $2, $3, $4::bigint) as r', [crypto.randomUUID(), 'naka@test', reason, skuId]);
  const parentSql = async (c, childCode, parentCode) => {
    await c.query("select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())");
    return c.query(`update core.products set parent_product_id = (select product_id from core.skus where code = $2), parent_set_by = 'manual' where product_id = (select product_id from core.skus where code = $1)`, [childCode, parentCode]);
  };
  let listingN = 0;
  const listingSql = async (c, code) => {
    const lid = (await c.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', $1, 'active') returning listing_id`, [`r-${code}-${++listingN}`])).rows[0].listing_id;
    return c.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, (select sku_id from core.skus where code = $2), 1, 0, 'exact', 'system')`, [lid, code]);
  };

  await ta('[R] 本物のログイン: master_edit (画面の lib)・master_ops (CLI の lib) = やめる / master_ops の transition を直に = 断る / watcher = 確かめだけ / 表は watcher だけ読める', async () => {
    const r1 = await cancelSkuGoneFromNe(pgAdapter(ME), { actor: 'naka@test', code: 'p01', reason: 'NE で消えた', requestId: crypto.randomUUID(), confirm: true }, { open: true });
    assert.equal(r1.state, 'cancelled'); assert.equal(await regOf('p01'), 'cancelled');
    assert.equal(r1.evidence.streak_days, 3);
    const checks = await listGoneChecks(pgAdapter(MO), ['p02']);
    assert.equal(checks[0].ok, true);
    const r2 = await cancelSkuGoneFromNe(pgAdapter(MO), { actor: 'naka@test', skuId: checks[0].sku_id, reason: 'NE で消えた', requestId: crypto.randomUUID() }, { open: true, requireConfirm: false });
    assert.equal(r2.state, 'cancelled');
    assert.equal((await q('select db_user from ops.master_write_sessions where request_id = $1', [r2.request_id]))[0].db_user, 'master_ops');
    const e1 = await errOf(MO.query(`select ops.transition_sku_registration($1, 'cancelled', 'human', 'naka@test', 'NE で消えた')`, [await sid('p03')]));
    assert.match(e1?.message ?? '', /gone_session_required/);
    assert.equal((await WA.query('select ops.sku_gone_from_ne_check($1::bigint) as r', [await sid('p03')])).rows[0].r.ok, true);
    assert.equal((await errOf(cancel(WA, await sid('p03'))))?.code, '42501');
    for (const [c, label] of [[ME, 'master_edit'], [MO, 'master_ops'], [WW, 'watch_writer']]) {
      for (const t of ['ops.master_ne_absence', 'ops.master_ne_absence_mark']) assert.equal((await errOf(c.query(`select count(*) from ${t}`)))?.code, '42501', `${label} ${t}`);
    }
    assert.ok(Number((await WA.query('select count(*)::int as n from ops.master_ne_absence')).rows[0].n) >= 10);
    for (const [c, label] of [[ME, 'master_edit'], [MO, 'master_ops']]) {
      assert.equal((await errOf(c.query("select ops._record_ne_absence(1, '{}'::jsonb)")))?.code, '42501', label);
      assert.equal((await errOf(c.query("select ops.sku_ref_cancelled('{1}'::bigint[])")))?.code, '42501', label);
    }
  });

  await ta('[C1] 照合 ② の記録が先 (p03 が NE に戻った取得・未 commit): やめるは許可の鍵で待つ → 記録の commit の後に「一番新しい取得にある」で断る', async () => {
    const W2 = await open('watch_writer');
    try {
    await W2.query('begin');
    await W2.query(recordSql, await recordArgs(GONE.filter((c) => c !== 'p03')));
    const p = cancel(ME, await sid('p03'));
    p.catch(() => {});
    assert.equal(await stillWaiting(p), true, 'やめるが記録を待たずに進んだ');
    await W2.query('commit');
    const e = await errOf(p);
    assert.match(e?.message ?? '', /not_gone_from_ne: .*一番新しい完全な NE の取得/);
    assert.equal(await regOf('p03'), 'available');
    } finally { await done(W2); }
  });

  await ta('[C2] やめるが先 (未 commit): 照合 ② の記録は待つ → やめるの commit の後に記録が進む (デッドロックしない・同じ日 = 日数はそのまま)', async () => {
    const M2 = await open('master_edit');
    try {
    await M2.query('begin');
    await cancel(M2, await sid('p04'));
    const p = record(GONE.filter((c) => c !== 'p03'));
    p.catch(() => {});
    assert.equal(await stillWaiting(p), true, '記録がやめるを待たずに進んだ');
    await M2.query('commit');
    const r = (await p).rows[0].r;
    assert.ok(r.result_id);
    assert.equal(await regOf('p04'), 'cancelled');
    assert.equal((await q("select streak from ops.master_ne_absence where code_norm = 'p04'"))[0].streak, 3);
    } finally { await done(M2); }
  });

  await ta('[C3] 夜間ロード (マスタの書き込みの鍵の排他) の間はやめるが待つ = lock_timeout で 55P03 (画面の 409 locked)・何も変えない', async () => {
    const L = await open(null);
    await L.query('begin');
    await L.query('select pg_advisory_xact_lock(core.master_write_lock_key())');
    const M3 = await open('master_edit');
    try {
    await M3.query(`set lock_timeout = '500ms'`);
    const e = await errOf(cancel(M3, await sid('p05')));
    assert.equal(e?.code, '55P03', e?.message);
    await L.query('rollback');
    assert.equal(await regOf('p05'), 'available');
    const ok = (await cancel(M3, await sid('p05'))).rows[0].r;
    assert.equal(ok.state, 'cancelled');
    } finally { await done(M3, L); }
  });

  /** 参照が先: A (持ち主・未 commit) が参照を作る → やめるが待つ → A の commit の後にやめるが確かめで断る */
  const refFirst = async (label, code, mk, re) => {
    const A = await open(null); const M = await open('master_edit');
    try {
      await A.query('begin');
      await mk(A, code);
      const p = cancel(M, await sid(code));
      p.catch(() => {});
      assert.equal(await stillWaiting(p), true, `${label}: やめるが参照の取引を待たずに進んだ`);
      await A.query('commit');
      const e = await errOf(p);
      assert.match(e?.message ?? '', re, label);
      assert.equal(await regOf(code), 'available', label);
    } finally { await done(A, M); }
  };
  /** やめるが先: C (master_edit・未 commit) がやめる → A (持ち主) の参照が待つ → C の commit の後に A が sku_cancelled */
  const cancelFirst = async (label, code, mk) => {
    const C = await open('master_edit'); const A = await open(null);
    try {
      await C.query('begin');
      await cancel(C, await sid(code));
      await A.query('begin');
      const p = mk(A, code);
      p.catch(() => {});
      assert.equal(await stillWaiting(p), true, `${label}: 参照がやめるの取引を待たずに進んだ`);
      await C.query('commit');
      const e = await errOf(p);
      await A.query('rollback');
      assert.equal(e?.code, '23514', `${label}: ${e?.message}`); assert.match(e.message, new RegExp(`sku_cancelled: .*${code}`), label);
      assert.equal(await regOf(code), 'cancelled', label);
    } finally { await done(C, A); }
  };
  const compSql = (c, code) => c.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, (select sku_id from core.skus where code = 'pset'), (select sku_id from core.skus where code = $1), 1, 'manual')`, [code]);
  const reqSql = async (c, code) => c.query(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id)
      values (1, (select sku_id from core.skus where code = 'pset'), $1::jsonb, repeat('7', 64), '[]'::jsonb, 't', gen_random_uuid())`, [JSON.stringify([{ sku_id: Number(await sid(code)), code, qty: 1, sort: 1 }])]);

  await ta('[H1] 構成の子: 参照が先 = やめるは set_component で断る / やめるが先 = 構成の insert が sku_cancelled', async () => {
    await refFirst('構成 (参照が先)', 'p06', compSql, /not_gone_from_ne: .*ほかのセットの構成品/);
    await cancelFirst('構成 (やめるが先)', 'p07', compSql);
  });
  await ta('[H2] 構成の依頼: 参照が先 = やめるは component_request で断る / やめるが先 = 依頼の insert が sku_cancelled', async () => {
    await refFirst('構成の依頼 (参照が先)', 'p08', reqSql, /not_gone_from_ne: .*開いている構成の依頼/);
    await q("update ops.sku_component_requests set status = 'cancelled', closed_at = now(), closed_by = 't', close_reason = 'withdrawn' where status = 'open'");   // セットごとに開いた依頼は 1 つ
    await cancelFirst('構成の依頼 (やめるが先)', 'p09', reqSql);
  });
  await ta('[H3] 代表 (親): 参照が先 = やめるは parent_of_others で断る / やめるが先 = 親を付ける update が sku_cancelled', async () => {
    await refFirst('代表 (参照が先)', 'p12', (c, code) => parentSql(c, 'k2', code), /not_gone_from_ne: .*代表 \(親\)/);
    await cancelFirst('代表 (やめるが先)', 'p10', (c, code) => parentSql(c, 'k3', code));
  });
  await ta('[H4] 出品の構成: 参照が先 = やめるは listing_component で断る / やめるが先 = 出品の構成の insert が sku_cancelled', async () => {
    await refFirst('出品の構成 (参照が先)', 'p08', listingSql, /not_gone_from_ne: .*販路の出品の構成/);   // p08 = [H2] で依頼を閉じた後 = ほかに使われていない
    await cancelFirst('出品の構成 (やめるが先)', 'p11', listingSql);
    const e = await errOf(listingSql(O, 'p05'));   // やめた後に足す (競り合いでない) も断る
    assert.equal(e?.code, '23514'); assert.match(e.message, /sku_cancelled: .*p05/);
  });

  await ta('[E] 夜間ロード: 材料に、やめた SKU の構成の子 (p01)・代表 (p02 が keep の親)・出品の構成 (p05) があっても止まらず飛ばす・DB に参照は入らない', async () => {
    const PLAN2 = { ...PLAN,
      setComponents: [...PLAN.setComponents, { parentCode: 'pset', childCode: 'p01', qty: 1, source: 'ne' }],
      variationGroups: [{ code: 'p02', name: 'まとまり p02', childCodes: ['keep'], status: 'active' }],
      listings: [{ mall: 'rakuten', shopCode: 'main', listingCode: 'r-load-p05', status: 'active', components: [{ code: 'p05', qty: 1, resolution: 'exact', evidence: { source: 'rakuten_sku_map' } }] }] };
    const r = await runInitialLoad(dbO, PLAN2, { log: () => {}, runId: `gone_load2_${name}`, host: 'test' });
    assert.equal(r.ok, true, r.error);
    const txt = JSON.stringify(r);
    assert.ok(txt.includes('child_cancelled'), '構成の skip の理由 child_cancelled が報告に無い');
    assert.ok(txt.includes('rep_cancelled'), '代表の skip の理由 rep_cancelled が報告に無い');
    assert.ok(txt.includes('出品の構成に入れない'), '出品の構成の skip が報告に無い');
    const refs = await q(`select (select count(*)::int from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id where k.code = 'p01') as comp,
      (select count(*)::int from core.listing_components lc join core.skus k on k.sku_id = lc.sku_id where k.code = 'p05') as lc,
      (select count(*)::int from core.products p where p.parent_product_id = (select product_id from core.skus where code = 'p02')) as par`);
    assert.deepEqual(refs[0], { comp: 0, lc: 0, par: 0 });
    for (const c of ['p01', 'p02', 'p05']) assert.equal(await regOf(c), 'cancelled');
  });
  await ta('[E2] #1683 Codex R2 Medium 1: やめた子・商品は「わざと除く」= 親 / 出品のほかの古い自動の構成は消える (DB が A + C・材料が A + やめた X → C が消え X は入らない / 全部やめた = 古い構成が全部消える)', async () => {
    const before = await q(`select (select string_agg(k.code, ',' order by k.code) from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id join core.skus k on k.sku_id = c.child_sku_id where p.code = 'pset2') as s2,
      (select string_agg(k.code, ',' order by k.code) from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id join core.skus k on k.sku_id = lc.sku_id where l.listing_code = 'r-l1') as l1`);
    assert.deepEqual(before[0], { s2: 'k4,k5', l1: 'k4,k5' });
    const PLAN3 = { ...PLAN,
      setComponents: [{ parentCode: 'pset', childCode: 'keep', qty: 1, source: 'ne' }, { parentCode: 'pset2', childCode: 'k4', qty: 1, source: 'ne' }, { parentCode: 'pset2', childCode: 'p01', qty: 1, source: 'ne' },
        { parentCode: 'pset3', childCode: 'p02', qty: 1, source: 'ne' }],
      listings: [rakuten('r-l1', ['k4', 'p05']), rakuten('r-l2', ['p05'])] };
    const r = await runInitialLoad(dbO, PLAN3, { log: () => {}, runId: `gone_load3_${name}`, host: 'test' });
    assert.equal(r.ok, true, r.error);
    const comps = async (parent) => (await q('select k.code from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id join core.skus k on k.sku_id = c.child_sku_id where p.code = $1 order by 1', [parent])).map((x) => x.code);
    const lcs = async (lc) => (await q('select k.code from core.listing_components c join core.listings l on l.listing_id = c.listing_id join core.skus k on k.sku_id = c.sku_id where l.listing_code = $1 order by 1', [lc])).map((x) => x.code);
    assert.deepEqual(await comps('pset2'), ['k4'], 'pset2 = A だけ (C = k5 が消える・X = p01 は入らない)');
    assert.deepEqual(await comps('pset3'), [], 'pset3 = 子が全部やめた = 古い自動の構成 (k6) が消える');
    assert.deepEqual(await lcs('r-l1'), ['k4'], 'r-l1 = A だけ');
    assert.deepEqual(await lcs('r-l2'), [], 'r-l2 = 全部やめた = 古い自動の構成が消える');
  });

  await ta('[D] #1683 Codex R2 Medium 2: CLI の複数件の取消は sku_id の昇順で鍵を取る = ほかの処理 (Amazon の保存・構成の昇格 = sku_id の昇順の鍵) と並んでもデッドロックしない', async () => {
    const [zid, aid] = [await sid('zgone'), await sid('agone')];
    assert.ok(BigInt(zid) < BigInt(aid), 'zgone の sku_id が agone より小さい (コードの順と逆) 前提');
    const { runGone } = await import('./company-db/master-gone-from-ne.mjs');
    const dry = await runGone(pgAdapter(MO), { apply: false, codes: ['agone', 'zgone'] }, { log: () => {} });
    assert.equal(dry.checks.filter((c) => c.ok).length, 2, JSON.stringify(dry.checks.map((c) => c.problems)));
    const X = await open(null); const M = await open('master_ops');
    try {
      await X.query('begin');
      await X.query(`set local lock_timeout = '5s'`);
      const lockSql = "select pg_advisory_xact_lock(hashtextextended('core.sku:' || $1::text, 0))";   // 0054 の Amazon の保存・構成の昇格と同じ鍵 (sku_id の昇順)
      await X.query(lockSql, [zid]);
      const p = runGone(pgAdapter(M), { apply: true, codes: ['agone', 'zgone'], expectHash: dry.planHash, actor: 'naka@test', reason: 'NE で消えた' }, { log: () => {} });
      p.catch(() => {});
      assert.equal(await stillWaiting(p, 800), true, 'CLI がほかの処理の鍵を待たずに進んだ');
      await X.query(lockSql, [aid]);   // 逆順だと CLI が aid を持って zid を待つ = ここでデッドロック
      await X.query('commit');
      const r = await p;
      assert.equal(r.exitCode, 0, JSON.stringify(r.error));
      assert.deepEqual(r.applied.map((a) => a.code), ['zgone', 'agone']);
      assert.equal(await regOf('zgone'), 'cancelled'); assert.equal(await regOf('agone'), 'cancelled');
    } finally { try { await X.query('rollback'); } catch { /* */ } await done(X, M); }
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB ${n} を消せない: ${e.message}`); } }
  try { await admin.end(); } catch { /* */ }
}
console.log(`${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
