/**
 * test-master-ne-delete-pg.mjs — 0072 (NE から消す CSV) を実 PostgreSQL の独立した接続と本物のログインのロールで確かめる
 *
 * 固定する契約:
 *   [R]  本物のログイン: master_edit (画面の lib) = 確かめる・作る・受け取る・取り下げる / master_ops = 作れない (42501) / watcher = 確かめ・一覧だけ・file_bytes は SELECT できない (ロールの script を流し直しても) /
 *        どのアプリのロールも表を直に読めない (バイト列は ops.ne_del_download だけ)
 *   [R2] 本番の順 (ロールがある DB に 0072 を当て、ロールの script はまだ流さない) でも watcher は file_bytes を SELECT できない
 *   [C1] 参照が先 (出品の構成の insert・未 commit): 作るは登録の状態の行で待つ → 参照の commit の後に「出品の構成」で断る (何も書かない)
 *   [C2] 作るが先 (未 commit): 参照 (出品の構成・構成の子・構成の依頼) は待つ → 作るの commit の後に sku_ne_deleting で断る (デッドロックしない)
 *   [C3] 同じ SKU を 2 人が同時に作る: 2 つ目は待つ → 1 つ目の commit の後に「依頼の途中」で断る (live は 1 つ)
 *   [C4] 受け取りが先 (未 commit): 取り下げは品目の行で待つ → 受け取りの commit の後に取り下げが進む / 取り下げの後の受け取り = file_changed
 *   [C5] 夜間ロード (マスタの書き込みの鍵の排他) の間は作るが待つ = lock_timeout で 55P03 (画面の 409 locked)
 *   [G]  セットの「NE から消えた → やめる」: 構成を外す・依頼の品目 gone・中身の単品をやめられる / 夜間ロードは古い NE の行からやめたセットの構成を戻さない・
 *        依頼の途中の子・出品の構成は飛ばして止まらない
 * 使い方: TEST_PG_URL=postgres://postgres:pw@localhost:54329/postgres node scripts/test-master-ne-delete-pg.mjs
 *   (使い捨ての PG: node C:/tmp/pg-embed/run-nedel.mjs scripts/test-master-ne-delete-pg.mjs <リポジトリ>)
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
import * as ND from '../lib/master-ne-delete.mjs';
import { cancelSkuGoneFromNe } from '../lib/master-gone.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (0072 の実 PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }
const watchdog = setTimeout(() => { console.error('  NG  試験が 900 秒で終わらない (止める)'); process.exit(1); }, Number(process.env.NEDEL_TEST_MS || 900000));
watchdog.unref();

let passed = 0;
async function ta(name, fn) {
  const t = Date.now();
  try { await fn(); passed++; console.log(`  ok  ${name} (${Date.now() - t} ms)`); } catch (e) { console.error(`  NG  ${name} (${Date.now() - t} ms)\n      ${e.stack || e.message}`); process.exitCode = 1; }
}
const errOf = async (p) => { try { await p; } catch (e) { return e; } return null; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function stillWaiting(p, ms = 600) { const t = Symbol('t'); return (await Promise.race([p.then(() => 'done', () => 'done'), sleep(ms).then(() => t)])) === t; }

const ALL_LOAD = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'load']));
const BASE = { ...ALL_LOAD, 'skus.name': 'company', 'products.name': 'company', 'skus.sku_kind': 'company', 'listing_components.amazon': 'company' };   // 10/9 の本番の形
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries(ROLES.map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
const WPW = { watcher: `w_${crypto.randomBytes(12).toString('hex')}`, watch_writer: `ww_${crypto.randomBytes(12).toString('hex')}` };
const single = (code) => ({ code, name: code, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: 100, source: 'ne', status: 'COMPLETE' } });
const setSku = (code) => ({ ...single(code), kind: 'set', cost: { jpy: 100, source: 'set_calc', status: 'COMPLETE' } });
const rakuten = (code, comps) => ({ mall: 'rakuten', shopCode: 'main', listingCode: code, status: 'active', components: comps.map((c) => ({ code: c, qty: 1, resolution: 'exact', evidence: { source: 'rakuten_sku_map' } })) });
const SINGLES = ['d01', 'd02', 'd03', 'd04', 'd05', 'd06', 'd07', 'd08', 'd09', 'd10', 'c1', 'c2', 'c3'];
const PLAN = { skus: [...SINGLES.map(single), setSku('dset'), setSku('dset2')],
  variationGroups: [],
  setComponents: [{ parentCode: 'dset', childCode: 'c1', qty: 1, source: 'ne' }, { parentCode: 'dset', childCode: 'c2', qty: 2, source: 'ne' }, { parentCode: 'dset2', childCode: 'c3', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [], suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [] };
const jstToday = () => new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
let runN = 0;
const runId = () => `mc_20301012T${String(++runN).padStart(9, '0')}Z_${crypto.randomBytes(3).toString('hex')}`;
let tick = 0;
const fetchAt = (day) => { tick++; return day === 0 ? new Date(Math.max(Date.now() - 20 * 60e3, Date.parse(`${jstToday()}T00:01:00+09:00`)) + tick * 1000) : new Date(Date.parse(`${jstToday()}T12:00:00+09:00`) + day * 86400e3 + tick * 1000); };
const zero = () => ({ ne_own: 0, ne_allocated: 0, ne_on_order: 0, fba: 0, logizard: 0, logizard_allocated: 0, po_backorder: 0 });
const zeroReader = async (skus) => new Map(skus.map((s) => [s.code_norm, zero()]));

const dbNames = [];
const admin = await openPgClient(url);
const clients = [admin];
try {
  const name = `cdb_nedel_${crypto.randomBytes(4).toString('hex')}`;
  await admin.query(`create database ${name}`);
  dbNames.push(name);
  const u = new URL(url); u.pathname = `/${name}`;
  const roleUrl = (role, pw) => { const x = new URL(u.toString()); x.username = role; x.password = pw; return x.toString(); };
  const done = async (...cs) => { for (const c of cs) { try { await c.end(); } catch { /* */ } } };
  const open = async (role) => { const c = await openPgClient(role ? roleUrl(role, PW[role] ?? WPW[role]) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); clients.push(c); return c; };
  const O = await open(null);
  const dbO = pgAdapter(O);
  await applyMigrations(dbO, { log: () => {} });
  await createMasterEditRoles(O, { pw: PW });
  await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
  const r0 = await runInitialLoad(dbO, PLAN, { log: () => {}, runId: `nedel_load_${name}`, host: 'test' });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbO, BASE);
  const [WA, ME, MO] = [await open('watcher'), await open('master_edit'), await open('master_ops')];
  const q = async (sql, p) => (await O.query(sql, p)).rows;
  const sid = async (code) => String((await q('select sku_id from core.skus where code = $1', [code]))[0].sku_id);
  const regOf = async (code) => (await q('select r.state from ops.master_registrations r join core.skus k on k.sku_id = r.sku_id where k.code = $1', [code]))[0]?.state;
  const liveOf = async (code) => Number((await q("select count(*)::int as n from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = $1 and i.state = 'live'", [code]))[0].n);
  const ALL = [...SINGLES, 'dset', 'dset2'];
  let ncN = 0;
  const neCodes = async (codes) => {
    const at = new Date(Date.now() - 60e3 + ++ncN * 10);
    const run = `mc_${at.toISOString().replace(/[-:]/g, '').replace(/\.(\d{3})Z$/, '$1Z')}_${crypto.randomBytes(3).toString('hex')}`;
    await q('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, $2, 0)', [run, at.toISOString()]);
    await q('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: codes.map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
  };
  await neCodes(ALL);
  for (const [source, scope] of [['ne', 'main'], ['logizard', 'wh1']]) {
    const run = `nedel_${source}`;
    await q(`insert into ops.ingest_runs (ingest_run_id, source_system, entity, scope_key, started_at, status, complete) values ($1, $2, 'stock_daily', $3, now(), 'success', true)`, [run, source, scope]);
    await q(`insert into snapshots.stock_capture_days (snapshot_date, source, scope_key, company_id, status, ingest_run_id, captured_at) values ($1, $2, $3, 1, 'building', $4, now())`, [jstToday(), source, scope, run]);
    if (source === 'ne') for (const c of SINGLES) {
      await q(`insert into snapshots.sku_stock_daily (snapshot_date, source, scope_key, source_code, company_id, sku_id, qty, captured_at, ingest_run_id) values ($1, 'ne', 'main', $2, 1, (select sku_id from core.skus where code = $2), 0, now(), $3)`, [jstToday(), c, run]);
    }
    await q(`update snapshots.stock_capture_days set status = 'complete', completed_at = now() where snapshot_date = $1 and source = $2`, [jstToday(), source]);
  }
  const obs = async (gone) => {
    const rows = await q("select code_norm, sku_kind from core.skus where company_id = 1 and sku_kind in ('single', 'set') order by 1");
    return { format: 'parent-obs-v1', complete: true, untrusted: [], rep_spellings: { state: 'ok' }, trust: { fetch_counts: 'ok', integrity: 'ok', kind_gate_integrity: 'ok', rep_spellings: 'ok' },
      rows: rows.filter((r) => !gone.includes(r.code_norm)).map((r) => (r.sku_kind === 'set' ? [r.code_norm, 'set', null, null, null] : [r.code_norm, 'single', 'ok', null, null])) };
  };
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
  await feed([], -3);
  /** 画面のロールが関数を直に (競り合いの試験 = 取引を開けたまま) */
  const buildSql = async (c, codes) => {
    const ids = await Promise.all(codes.map(sid));
    return c.query('select ops.ne_del_build($1::uuid, $2, $3, $4::bigint[], $5::jsonb) as r', [crypto.randomUUID(), 'naka@test', '試験', ids, JSON.stringify(Object.fromEntries(ids.map((i) => [i, zero()])))]);
  };
  let listingN = 0;
  const listingSql = async (c, code) => {
    const lid = (await c.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'rakuten', '', $1, 'active') returning listing_id`, [`r-${code}-${++listingN}`])).rows[0].listing_id;
    return c.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, (select sku_id from core.skus where code = $2), 1, 0, 'exact', 'system')`, [lid, code]);
  };
  const compSql = (c, code) => c.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, (select sku_id from core.skus where code = 'dset2'), (select sku_id from core.skus where code = $1), 1, 'manual')`, [code]);
  const reqSql = async (c, code) => c.query(`insert into ops.sku_component_requests (company_id, set_sku_id, rows, rows_hash, base_rows, requested_by, edit_request_id)
      values (1, (select sku_id from core.skus where code = 'dset2'), $1::jsonb, repeat('7', 64), '[]'::jsonb, 't', gen_random_uuid())`, [JSON.stringify([{ sku_id: Number(await sid(code)), code, qty: 1, sort: 1 }])]);

  await ta('[R] 本物のログイン: master_edit = 確かめる・作る・受け取る・取り下げる (lib) / master_ops = 作れない / watcher = 確かめ・一覧だけ / 表はアプリのロールで読めない', async () => {
    const c = await ND.checkNeDelete(pgAdapter(ME), { codes: ['d01'] }, { stockReader: zeroReader });
    assert.equal(c.all_ok, true, JSON.stringify(c.rows));
    const b = await ND.buildNeDelete(pgAdapter(ME), { actor: 'naka@test', codes: ['d01'], reason: '販売をやめた', confirm: true }, { open: true, stockReader: zeroReader });
    assert.equal(b.exports[0].codes[0], 'd01');
    const f = await ND.downloadNeDelete(pgAdapter(ME), { actor: 'naka@test', exportId: b.exports[0].export_id });
    assert.equal(f.bytes.toString('hex'), ND.delCsvBytes(['d01']).toString('hex'));
    assert.equal((await q('select db_user from ops.ne_del_exports where export_id = $1', [b.exports[0].export_id]))[0].db_user, 'master_edit');
    const item = (await q("select item_id from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = 'd01'"))[0].item_id;
    const w = await ND.withdrawNeDelete(pgAdapter(ME), { actor: 'naka@test', itemId: item, reason: 'まだ使う' }, { open: true });
    assert.equal(w.state, 'withdrawn');
    assert.equal((await errOf(buildSql(MO, ['d02'])))?.code, '42501');
    assert.equal((await errOf(buildSql(WA, ['d02'])))?.code, '42501');
    assert.equal((await WA.query('select ops.ne_del_check($1::bigint, null, $2::jsonb) as r', [await sid('d02'), JSON.stringify(zero())])).rows[0].r.ok, true);
    assert.ok((await WA.query('select ops.ne_del_overview(null) as r')).rows[0].r.items.length >= 1);
    assert.equal((await errOf(WA.query('select ops.ne_del_download($1, $2::bigint)', ['w@test', b.exports[0].export_id])))?.code, '42501');
    // 🆕 #1690 Codex R1 Medium 1: watcher (本物のログイン) は file_bytes を直に SELECT できない (ほかの列は読める)
    assert.equal((await errOf(WA.query('select file_bytes from ops.ne_del_exports')))?.code, '42501');
    assert.equal((await errOf(WA.query('select * from ops.ne_del_exports')))?.code, '42501');
    assert.ok((await WA.query('select export_id, sha256, built_by from ops.ne_del_exports')).rows.length >= 1);
    // ロールの script を流し直しても戻らない
    await createWatchRoles(O, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
    await createMasterEditRoles(O, { pw: PW });
    assert.equal((await errOf(WA.query('select file_bytes from ops.ne_del_exports')))?.code, '42501');
    for (const [c2, label] of [[ME, 'master_edit'], [MO, 'master_ops']]) {
      for (const t of ['ops.ne_del_exports', 'ops.ne_del_items']) assert.equal((await errOf(c2.query(`select count(*) from ${t}`)))?.code, '42501', `${label} ${t}`);
      assert.equal((await errOf(c2.query("select ops.ne_del_csv_bytes('{a}'::text[])")))?.code, '42501', label);
      assert.equal((await errOf(c2.query("select ops.sku_ref_ne_deleting('{1}'::bigint[])")))?.code, '42501', label);
    }
  });

  await ta('[C1] 参照が先 (出品の構成・未 commit): 作るは待つ → 参照の commit の後に「出品の構成」で断る (何も書かない)', async () => {
    const A = await open(null); const M = await open('master_edit');
    try {
      await A.query('begin');
      await listingSql(A, 'd02');
      const p = buildSql(M, ['d02']);
      p.catch(() => {});
      assert.equal(await stillWaiting(p), true, '作るが参照の取引を待たずに進んだ');
      await A.query('commit');
      const e = await errOf(p);
      assert.match(e?.message ?? '', /not_deletable: .*販路の出品の構成/);
      assert.equal(await liveOf('d02'), 0);
    } finally { await done(A, M); }
  });

  for (const [label, code, mk] of [['出品の構成', 'd03', listingSql], ['構成の子', 'd04', compSql], ['構成の依頼', 'd05', reqSql]]) {
    await ta(`[C2] 作るが先 (未 commit): ${label} の insert は待つ → 作るの commit の後に sku_ne_deleting で断る`, async () => {
      const M = await open('master_edit'); const A = await open(null);
      try {
        await M.query('begin');
        await buildSql(M, [code]);
        await A.query('begin');
        const p = mk(A, code);
        p.catch(() => {});
        assert.equal(await stillWaiting(p), true, `${label}: 参照が作るの取引を待たずに進んだ`);
        await M.query('commit');
        const e = await errOf(p);
        await A.query('rollback');
        assert.equal(e?.code, '23514', `${label}: ${e?.message}`); assert.match(e.message, new RegExp(`sku_ne_deleting: .*${code}`));
        assert.equal(await liveOf(code), 1);
      } finally { await done(M, A); }
    });
  }

  await ta('[C3] 同じ SKU を 2 人が同時に作る: 2 つ目は待つ → 1 つ目の commit の後に「依頼の途中」で断る (live は 1 つ)', async () => {
    const M1 = await open('master_edit'); const M2 = await open('master_edit');
    try {
      await M1.query('begin');
      await buildSql(M1, ['d06']);
      const p = buildSql(M2, ['d06', 'd07']);
      p.catch(() => {});
      assert.equal(await stillWaiting(p), true);
      await M1.query('commit');
      const e = await errOf(p);
      assert.match(e?.message ?? '', /not_deletable: .*d06 \(もう NE から消す依頼の途中/);
      assert.equal(await liveOf('d06'), 1); assert.equal(await liveOf('d07'), 0);
    } finally { await done(M1, M2); }
  });

  await ta('[C4] 受け取りが先 (未 commit): 取り下げは待つ → 受け取りの commit の後に進む / 取り下げの後の受け取り = file_changed', async () => {
    const b = await ND.buildNeDelete(pgAdapter(ME), { actor: 'naka@test', codes: ['d08'], reason: '販売をやめた', confirm: true }, { open: true, stockReader: zeroReader });
    const id = b.exports[0].export_id;
    const item = (await q("select item_id from ops.ne_del_items i join core.skus k on k.sku_id = i.sku_id where k.code = 'd08' and i.state = 'live'"))[0].item_id;
    const D = await open('master_edit'); const W = await open('master_edit');
    try {
      await D.query('begin');
      await D.query('select ops.ne_del_download($1, $2::bigint)', ['naka@test', id]);
      const p = W.query('select ops.ne_del_withdraw($1::uuid, $2, $3, $4::bigint) as r', [crypto.randomUUID(), 'naka@test', 'まだ使う', item]);
      p.catch(() => {});
      assert.equal(await stillWaiting(p), true, '取り下げが受け取りを待たずに進んだ');
      await D.query('commit');
      assert.equal((await p).rows[0].r.state, 'withdrawn');
      const e = await errOf(ND.downloadNeDelete(pgAdapter(ME), { actor: 'naka@test', exportId: id }));
      assert.equal(e?.reason, 'file_changed');
    } finally { await done(D, W); }
  });

  await ta('[C5] 夜間ロード (マスタの書き込みの鍵の排他) の間は作るが待つ = lock_timeout で 55P03・何も書かない', async () => {
    const L = await open(null);
    await L.query('begin');
    await L.query('select pg_advisory_xact_lock(core.master_write_lock_key())');
    const M = await open('master_edit');
    try {
      await M.query(`set lock_timeout = '500ms'`);
      const e = await errOf(buildSql(M, ['d09']));
      assert.equal(e?.code, '55P03', e?.message);
      await L.query('rollback');
      assert.equal(await liveOf('d09'), 0);
    } finally { await done(M, L); }
  });

  await ta('[G] セット: 依頼 → 3 日 NE に無い → やめる (構成を外す・品目 gone) → 中身もやめられる / 夜間ロードは古い NE の行から構成を戻さない・依頼の途中の子と出品は飛ばす', async () => {
    const b = await ND.buildNeDelete(pgAdapter(ME), { actor: 'naka@test', codes: ['dset', 'c1', 'c2'], reason: 'セットごとやめる', confirm: true }, { open: true, stockReader: zeroReader });
    assert.deepEqual(b.exports.map((e) => e.kind), ['sets', 'products']);
    for (const day of [-2, -1, 0]) await feed(['dset', 'c1', 'c2'], day);
    await neCodes(ALL.filter((c) => !['dset', 'c1', 'c2'].includes(c)));
    const e = await errOf(cancelSkuGoneFromNe(pgAdapter(ME), { actor: 'naka@test', code: 'c1', reason: 'NE から消えた', confirm: true }, { open: true }));
    assert.match(e?.message ?? '', /ほかのセットの構成品/);
    const r = await cancelSkuGoneFromNe(pgAdapter(ME), { actor: 'naka@test', code: 'dset', reason: 'NE から消えた', confirm: true }, { open: true });
    assert.deepEqual(r.components_removed.map((x) => x.code), ['c1', 'c2']);
    for (const c of ['c1', 'c2']) assert.equal((await cancelSkuGoneFromNe(pgAdapter(ME), { actor: 'naka@test', code: c, reason: 'NE から消えた', confirm: true }, { open: true })).state, 'cancelled');
    assert.equal(Number((await q("select count(*)::int as n from ops.ne_del_items where state = 'gone'"))[0].n), 3);
    // 依頼の途中の子 (c3 = dset2 の中身) と出品の構成 (d10)
    await ND.buildNeDelete(pgAdapter(ME), { actor: 'naka@test', codes: ['dset2', 'c3', 'd10'], reason: '試験', confirm: true }, { open: true, stockReader: zeroReader });
    const PLAN2 = { ...PLAN, listings: [rakuten('r-d10', ['d10'])] };
    const lr = await runInitialLoad(dbO, PLAN2, { log: () => {}, runId: `nedel_load2_${name}`, host: 'test' });
    assert.equal(lr.ok, true, lr.error);
    assert.equal(Number((await q("select count(*)::int as n from core.sku_components where parent_sku_id = $1", [await sid('dset')]))[0].n), 0);
    assert.equal(Number((await q("select count(*)::int as n from core.sku_components where parent_sku_id = $1 and child_sku_id = $2", [await sid('dset2'), await sid('c3')]))[0].n), 1);
    const reasons = (lr.unresolved?.set_components || []).map((x) => x.reason_code);
    assert.ok(reasons.includes('parent_cancelled') && reasons.includes('child_ne_deleting'), JSON.stringify(reasons));
    assert.equal(Number((await q("select count(*)::int as n from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id where l.listing_code = 'r-d10'"))[0].n), 0);
    assert.equal(await regOf('dset'), 'cancelled');
  });
  await ta('[R2] 本番の順 (ロールがある DB に 0072 を当てて、ロールの script はまだ流さない): watcher は ops.ne_del_exports の file_bytes を SELECT できない (migration の最後で外す)', async () => {
    const name2 = `cdb_nedel2_${crypto.randomBytes(4).toString('hex')}`;
    await admin.query(`create database ${name2}`);
    dbNames.push(name2);
    const u2 = new URL(url); u2.pathname = `/${name2}`;
    const O2 = await openPgClient(u2.toString()); clients.push(O2);
    await applyMigrations(pgAdapter(O2), { log: () => {}, to: '0071' });
    await createWatchRoles(O2, { watcherPw: WPW.watcher, writerPw: WPW.watch_writer });
    await applyMigrations(pgAdapter(O2), { log: () => {} });
    const x = new URL(u2.toString()); x.username = 'watcher'; x.password = WPW.watcher;
    const W2 = await openPgClient(x.toString()); clients.push(W2);
    try {
      assert.equal((await errOf(W2.query('select file_bytes from ops.ne_del_exports')))?.code, '42501');
      await W2.query('select export_id, sha256 from ops.ne_del_exports');
      assert.equal((await errOf(W2.query('select file_bytes from ops.ne_reg_exports')))?.code, '42501');   // 0058 の守りもそのまま
    } finally { await done(W2, O2); }
  });
} finally {
  for (const c of clients.slice(1)) { try { await c.end(); } catch { /* */ } }
  for (const n of dbNames) { try { await admin.query(`drop database if exists ${n} with (force)`); } catch (e) { console.error(`DB ${n} を消せない: ${e.message}`); } }
  try { await admin.end(); } catch { /* */ }
}
console.log(`${passed} passed${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
