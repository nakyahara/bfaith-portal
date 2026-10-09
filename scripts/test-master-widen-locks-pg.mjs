/**
 * test-master-widen-locks-pg.mjs — 広げる道 PR-1 (0058) の鍵の順 (設計 §3.10・PR-2 Codex R3 / R4・R19) を本物の PostgreSQL で確かめる
 *
 * 9 つの道を交差させる (🆕 PR-2 #1640 が載った後: アプリの道は PR-2 の lib = 新規開始の鍵 lib/master-owner-gate.mjs の acquireNewEntryLocksInTx → 段階 → 土台の門 → 新規開始の門):
 *   A = 配る (アプリ lib/master-reg-csv.mjs の issueRegExport = 初めて配るだけ ops.acquire_new_entry_locks を段階の鍵より前に・画面のロール master_edit)
 *   B = 配る (DB の関数 ops.ne_reg_issue を master_edit で直接呼ぶ = アプリの鍵なし)
 *   C = 照合 ② の始めに閉じる (ops.close_new_entry_for_compare・watch_writer = 許可の排他)
 *   D = 照合の確かめ (ops.record_ne_registration_check・watch_writer = 🆕 0065: マスタの書き込み (共有) → ne_reg_check → SKU → 親子 (共有) → CSV)
 *   F = CSV を作る (DB の関数 ops.ne_reg_build を master_edit で直接 = request → 許可 → 段階 → マスタの書き込み → SKU → CSV)
 *   G = CSV を作る (アプリ buildRegExport = request → 許可 → 段階 → マスタの書き込み → SKU → CSV → NE のコード → ops.ne_reg_build)
 *   R = 新商品の登録 (アプリ registerNewSku = request → 許可 → 段階 → マスタの書き込み → 新しいコード → ops.register_new_sku)
 *   S = 新商品の登録 (DB の関数 ops.register_new_sku を master_edit で直接 = 許可 → 段階 → マスタの書き込み → 新しいコード)
 *   V = 許可の取り消し (ops.revoke_new_entry_lease・new_entry_gate = 単品の許可の排他だけ)
 * 段階ごとの barrier: 試験の接続が §3.10 の表の鍵 n を 1 つ排他で持ち、道を (順を変えて) 始める → 全部が「その鍵で待つ」か「終わる」まで待つ →
 *   待っている道ごとに pg_locks から {道・持っている鍵の番号・待っている鍵の番号} を記録し、「待っている鍵の番号 ≥ 持っている全部の鍵の番号」
 *   (= 小さい番号へ戻らない) を確かめる → 鍵を放す → deadlock (40P01) なしに終わる (閉じた後の配るは new_entry_closed)。
 * あわせて: ops.acquire_new_entry_locks の鍵とモード・ops.ne_reg_file も許可の共有の鍵を返すまで持つ (R19)・照合 ② の始めに閉じるのは配る取引を待つ・
 *   結果を書く前に落ちた再実行では開かない。
 * 🆕 0065 (#1664 Codex R1 High) [22]: 夜間ロードが代表を書く直前で止まっている間、照合の確かめは待ち、ロードの後の Company DB の値で 3 者一致を比べる。
 * 使い方: TEST_PG_URL=postgres://… node scripts/test-master-widen-locks-pg.mjs (localhost だけ・使い捨ての DB を作って消す)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter, applyMigrations } from './company-db/migrate.mjs';
import { createMasterEditRoles } from './company-db/create-master-edit-roles.mjs';
import { createRoles } from './company-db/create-watch-roles.mjs';
import { runInitialLoad } from '../apps/company-db/load/engine.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';
import { forceNewOpen, seedNewEntryLease } from './fixtures/master-widen.mjs';

const url = process.env.TEST_PG_URL || '';
if (!url) { console.log('⏭️ TEST_PG_URL が無い (広げる道の鍵の順の本物の PostgreSQL の試験は飛ばす)'); process.exit(0); }
const u0 = new URL(url);
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(u0.hostname)) { console.error('localhost 以外の PostgreSQL には流さない'); process.exit(2); }

const R = await import('../lib/master-register.mjs');
const G = await import('../lib/master-reg-csv.mjs');
// 広げる道 PR-2 (#1640): 持ち主は DB の active (この DB は全部の列が C) = このコードの能力も全部にする (本番は config/master-capability.mjs の COMPANY_CAPABLE)
(await import('../lib/master-owner-gate.mjs')).__setCapableForTest(OWNED_COLUMNS);

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const launch = (p) => { const s = { done: false }; s.promise = p.then((r) => { s.done = true; s.ok = r; return { ok: r }; }, (e) => { s.done = true; s.err = e; return { err: e }; }); return s; };

const ALL_COMPANY = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const OWN = JSON.stringify(ALL_COMPANY);
const LOAD_NOW = new Date(Date.now() - 5 * 86400e3);
const NOW_MS = Date.parse('2030-01-10T03:00:00Z');
const RUN1 = 'mc_20300110T000000000Z_aaaaaa';
const RUNC = 'mc_20300109T000000001Z_bbbbbb';
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }], ['S02', { method: '宅急便', cost: 520 }]]);
const ROLES = ['master_edit', 'master_gate_render', 'master_gate_minipc', 'master_ops', 'master_observer', 'new_entry_gate'];
const PW = Object.fromEntries([...ROLES, 'watcher', 'watch_writer'].map((r) => [r, `t_${crypto.randomBytes(12).toString('hex')}`]));
function makePlan() {
  const sku = (code, name, taxRate, cost) => ({ code, name, kind: 'single', taxRate, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3, cost: { jpy: cost, source: 'ne', status: 'COMPLETE' },
    standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
  const singles = ['s001', 's002', 's003'].map((c, i) => sku(c, `単品 ${i + 1}`, 0.1, 100 * (i + 1)));
  return {
    skus: singles, variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], workers: [],
    suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: singles.map((s) => ({ supplierCode: '0001', skuCode: s.code })),
    primarySuppliers: singles.map((s) => ({ skuCode: s.code, supplierCode: '0001' })), reorder: { available: true, runId: 'pml_locks' },
  };
}

const dbName = `cdb_wl_${crypto.randomBytes(4).toString('hex')}`;
const admin = await openPgClient(url);
await admin.query(`create database ${dbName}`);
const u = new URL(url); u.pathname = `/${dbName}`;
const roleUrl = (role) => { const x = new URL(u.toString()); x.username = role; x.password = PW[role]; return x.toString(); };
const open = async (role) => { const c = await openPgClient(role ? roleUrl(role) : u.toString()); c.on('error', (e) => console.error(`[pg ${role || 'owner'}] ${e.message}`)); return c; };
const M = await open(null);
const clients = [M];
const q = async (sql, p) => (await M.query(sql, p)).rows;
const one = async (sql, p) => (await q(sql, p))[0];

try {
  const dbM = pgAdapter(M);
  await applyMigrations(dbM, { log: () => {} });
  await createRoles(M, { watcherPw: PW.watcher, writerPw: PW.watch_writer });
  await createMasterEditRoles(M, { pw: PW });
  const r0 = await runInitialLoad(dbM, makePlan(), { log: () => {}, runId: 'load_locks', now: LOAD_NOW });
  assert.equal(r0.ok, true, r0.error);
  await forceNewOpen(dbM, ALL_COMPANY);
  await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [RUN1]);
  await M.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: RUN1, entries: ['s001', 's002', 's003'].map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
  await seedNewEntryLease(dbM, { runId: RUN1 });
  assert.equal((await one("select ops.new_entry_lease_valid('single') as v")).v, true);

  const E1 = await open('master_edit'); const E2 = await open('master_edit'); const E3 = await open('master_edit'); const E4 = await open('master_edit');
  const WW = await open('watch_writer'); const WW2 = await open('watch_writer'); const NG = await open('new_entry_gate'); const T = await open(null);
  await M.query('alter role master_edit connection limit 12');   // 試験だけ (使い捨てのクラスタ): 画面のロールの道を 7 本同時に開く (本番の上限 5 は create-master-edit-roles.mjs のまま)
  const E5 = await open('master_edit'); const E6 = await open('master_edit'); const E7 = await open('master_edit'); const NG2 = await open('new_entry_gate');
  clients.push(E1, E2, E3, E4, WW, WW2, NG, T, E5, E6, E7, NG2);
  const [dbE5, dbE6] = [E5, E6].map(pgAdapter);
  const [dbE1, dbE2, dbE3] = [E1, E2, E3].map(pgAdapter);
  const pidOf = async (c) => (await c.query('select pg_backend_pid() as p')).rows[0].p;
  const PID = { A: await pidOf(E1), B: await pidOf(E2), C: await pidOf(WW), D: await pidOf(WW2), F: await pidOf(E4), G: await pidOf(E5), R: await pidOf(E6), S: await pidOf(E7), V: await pidOf(NG2) };
  const single = (name) => ({ name, standard_price: '1500', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', sales_class: '3', expiry_managed: '0', reorder_months: '1', cost: { jpy: '300' } });
  const reg = (code) => R.registerNewSku(dbE3, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code, values: single(`鍵の順 ${code}`), card: { create: false } },
    { ownership: ALL_COMPANY, open: true, now: new Date(), shippingRates: RATES });
  const opts = { ownership: ALL_COMPANY, open: true, nowMs: NOW_MS };
  const build = async (code) => (await G.buildRegExport(dbE3, { actor: 'boss@test', kind: 'products', codes: [code], requestId: crypto.randomUUID() }, opts)).export;
  const skuIdOf = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;

  // C の材料: 配って申告した商品 X を観測した、受け取りのある照合の回 (確かめは何度呼んでも同じ鍵を同じ順に取る)
  await reg('lk-x');
  const ex = await build('lk-x');
  await G.issueRegExport(dbE3, { actor: 'boss@test', exportId: ex.export_id }, opts);
  await G.declareRegExport(dbE3, { actor: 'boss@test', exportId: ex.export_id, sha256: ex.sha256, result: 'ok', neMessage: '1件成功しました。' }, opts);
  // 🆕 0063: 配っただけ (申告なし) の商品 Y も同じ回で確かめる = D は Y の登録の状態を draft → ne_pending → ne_confirmed に進める (同じ鍵の順の中で)
  await reg('lk-y');
  const ey = await build('lk-y');
  await G.issueRegExport(dbE3, { actor: 'boss@test', exportId: ey.export_id }, opts);
  await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-09T00:00:00Z', 0)`, [RUNC]);
  await WW.query('select ops.snapshot_ne_reg_targets($1)', [RUNC]);
  const okc = (v) => ({ st: 'ok', v });
  const at = new Date(Date.now() + 60000).toISOString();
  const obs = await WW.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: RUNC,
    fetch: { generation_id: 'gen_locks', products_rev: '7', sets_rev: '8', raw_hash: 'a'.repeat(64) }, products_at: at, sets_at: at, absence_trusted: true,
    observations: ['lk-x', 'lk-y'].map((c) => ({ code_norm: c, present: true, trusted: true, kind: 'single', cols: { name: okc(`鍵の順 ${c}`), supplier: okc('0001'), cost: okc(300), price: okc(1500), tax_rate: okc(0.1), handling: okc('active'), parent: okc(null) } })) })]);
  await WW.query('select ops.seal_ne_registration_run($1, $2, $3)', [RUNC, obs.rows[0].r.observation_hash, 'e'.repeat(64)]);

  // §3.10 の鍵の番号 (key → 番号)。request・SKU の鍵は回ごとに足す
  const keyRow = await one(`select hashtext('ops.new_entry_lease:single')::bigint::text as l1, hashtext('ops.new_entry_lease:set')::bigint::text as l2,
      hashtext('ops.master_cutover')::bigint::text as cut, core.master_write_lock_key()::text as mw,
      hashtext('ops.ne_reg_check')::bigint::text as chk, hashtext('ops.ne_csv')::bigint::text as csv, hashtext('ops.ne_codes')::bigint::text as nec,
      core.parent_lock_key()::text as par`);
  // 🆕 0065 (#1664 Codex R1 High): 親子の鍵 (0036) = SKU (7) の後・CSV (8) の前 (保存が代表を変える順 = SKU → 親子 → CSV と同じ) = 7.5
  const NUM = new Map([[keyRow.l1, 2], [keyRow.l2, 2], [keyRow.cut, 4], [keyRow.mw, 5], [keyRow.chk, 6], [keyRow.par, 7.5], [keyRow.csv, 8], [keyRow.nec, 9]]);
  const KEY = { 2: keyRow.l1, 4: keyRow.cut, 5: keyRow.mw, 6: keyRow.chk, 7.5: keyRow.par, 8: keyRow.csv, 9: keyRow.nec };
  const skuKey = async (id) => (await one(`select hashtextextended('core.sku:' || $1::text, 0)::text as k`, [id])).k;
  const addSku = async (code) => { const k = await skuKey(await skuIdOf(code)); NUM.set(k, 7); return k; };
  const addRequest = async (rid) => { const k = (await one(`select hashtextextended('ops.ne_reg_request:' || $1::text, 0)::text as k`, [rid])).k; NUM.set(k, 1); return k; };
  /** 🆕 PR-2: 登録の request (保存と同じ ops.master_edit_request) = 1・新しいコードの鍵 = SKU と同じ段 (7) */
  const addEditRequest = async (rid) => { const k = (await one(`select hashtextextended('ops.master_edit_request:' || $1::text, 0)::text as k`, [rid])).k; NUM.set(k, 1); return k; };
  const addNewCode = async (code) => { const k = (await one(`select hashtextextended('core.new_code:' || core.norm_code($1), 0)::text as k`, [code])).k; NUM.set(k, 7); return k; };
  await addSku('lk-x');
  await addSku('lk-y');
  /** pg_locks の advisory (bigint の鍵 = objsubid 1) を道ごとに: { held: [番号], waiting: 番号 | null, unknown: [鍵] } */
  const locksOf = async () => {
    const rows = await q(`select pid, granted, ((classid::bigint << 32) | objid::bigint)::text as key from pg_locks where locktype = 'advisory' and objsubid = 1 and pid = any($1::int[])`,
      [Object.values(PID)]);
    const out = {};
    for (const [name, pid] of Object.entries(PID)) {
      const mine = rows.filter((r) => r.pid === pid);
      out[name] = {
        held: mine.filter((r) => r.granted && NUM.has(r.key)).map((r) => NUM.get(r.key)).sort((a, b) => a - b),
        waiting: mine.filter((r) => !r.granted).map((r) => (NUM.has(r.key) ? NUM.get(r.key) : `?${r.key}`))[0] ?? null,
        unknown: mine.filter((r) => r.granted && !NUM.has(r.key)).map((r) => r.key),
      };
    }
    return out;
  };
  const LOG = [];
  /** 道が全部「advisory の鍵で待つ」か「終わる」まで待つ (最大 15 秒) → 記録 {回・道・持っている鍵の番号・待っている鍵の番号} */
  const settle = async (round, paths) => {
    const t0 = Date.now();
    for (;;) {
      const st = await locksOf();
      const act = (await q(`select pid, wait_event_type, wait_event from pg_stat_activity where pid = any($1::int[])`, [Object.values(PID)]));
      const waitingAdvisory = (name) => act.some((a) => a.pid === PID[name] && a.wait_event_type === 'Lock' && a.wait_event === 'advisory');
      if (Object.entries(paths).every(([name, s]) => s.done || waitingAdvisory(name))) {
        for (const [name, s] of Object.entries(paths)) if (!s.done) LOG.push({ round, path: name, held: st[name].held, waiting: st[name].waiting, unknown: st[name].unknown.length });
        return st;
      }
      if (Date.now() - t0 > 15000) throw new Error(`道が鍵で待つのでも終わるのでもない (${round}): ${JSON.stringify(act)}`);
      await sleep(30);
    }
  };
  const assertMonotonic = (round, st, paths) => {
    for (const [name, s] of Object.entries(paths)) {
      if (s.done) continue;
      const w = st[name].waiting;
      assert.equal(typeof w, 'number', `${round} ${name}: 知らない鍵で待っている ${w}`);
      for (const h of st[name].held) assert.ok(h <= w, `${round} ${name}: 鍵 ${w} を待ちながら、より後ろの鍵 ${h} を持っている (§3.10 の順の違反) ${JSON.stringify(st[name])}`);
    }
  };
  // 道: A = 配る (アプリ = request の直後に ops.acquire_new_entry_locks)・B = 配る (DB の関数を master_edit で直接)・C = 照合 ② の始めに閉じる (許可の排他)・
  //     D = 照合の確かめ (0053・ne_reg_check → SKU → CSV)・F = CSV を作る (DB の関数 ops.ne_reg_build を master_edit で直接 = request → 許可 → 段階 → … の順)
  const pathA = (exportId) => G.issueRegExport(dbE1, { actor: 'boss@test', exportId }, opts);
  const pathB = (exportId) => E2.query('select ops.ne_reg_issue($1::uuid, $2, $3::jsonb, $4::bigint) as r', [crypto.randomUUID(), 'boss@test', OWN, exportId]).then((r) => r.rows[0].r);
  const pathC = (run) => WW.query('select ops.close_new_entry_for_compare($1) as r', [run]).then((r) => r.rows[0].r);
  const pathD = () => WW2.query('select ops.record_ne_registration_check($1) as r', [RUNC]).then((r) => r.rows[0].r);
  // 0065: 作れる単品の版 = ne-reg-single-v2 (v1 は鍵の前に schema_not_buildable)
  const HEADER = 'syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code';
  const pathF = (rid, skuId) => E4.query('select ops.ne_reg_build($1::jsonb, $2::bytea) as r', [JSON.stringify({ request_id: rid, actor: 'boss@test', ownership: ALL_COMPANY, reason: null,
    kind: 'products', schema_version: 'ne-reg-single-v2', header: HEADER, ne_codes_run: RUN1, cost_day: new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10),
    items: [{ sku_id: skuId, expected: {}, rows: [['x']] }] }), Buffer.from('x')]).then((r) => r.rows[0].r);
  // 許可を開け直す (本番と同じ: NE のコードの回 → 照合 ② の始めに閉じる → 結果 → その回で grant)
  let reopenN = 0;
  const reopen = async () => {
    reopenN++;
    const run = `mc_20300110T${String(100000000 + reopenN).padStart(9, '0')}Z_dddddd`;
    await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-10T00:00:00Z', 0)`, [run]);
    await M.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: ['s001', 's002', 's003'].map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
    await seedNewEntryLease(dbM, { runId: run });
    assert.equal((await one("select ops.new_entry_lease_valid('single') as v")).v, true);
  };
  const closedOk = (r) => !r.err || r.err.reason === 'new_entry_closed' || /new_entry_closed|入口は閉じている/.test(String(r.err.message));
  // 🆕 PR-2 の道: G = アプリの作る・R = アプリの登録・S = DB の登録の関数を直接・V = 単品の許可の取り消し
  const pathG = (rid, code) => G.buildRegExport(dbE5, { actor: 'boss@test', kind: 'products', codes: [code], requestId: rid }, opts).then((r) => r.export);
  const pathR = (rid, code) => R.registerNewSku(dbE6, { actor: 'naka@test', requestId: rid, kind: 'single', code, values: single(`鍵の順 ${code}`), card: { create: false } },
    { ownership: ALL_COMPANY, open: true, now: new Date(), shippingRates: RATES });
  const SUP0001 = (await one("select supplier_id::text as id from core.suppliers where code = '0001'")).id;   // 単品は代表の仕入先が要る (0060)
  const DIRECT_ENTRY = (code) => ({ kind: 'single', code, started_at: null, product: { name: '直接の単品', sales_class: 3, expiry_managed: false, inbound_date_managed: null },
    sku: { name: '直接の単品', tax_rate: 0.1, tax_class: 'STANDARD_10', handling: 'active', standard_price_jpy: 1000, shipping_code: 'S01', shipping_method: 'ゆうパケット',
      shipping_cost_jpy: 210, reorder_months: 1, set_sales_class_override: null, handling_own: null }, supplier_id: SUP0001, cost: null, component_request: null, card: null });
  const pathS = (code) => E7.query('select ops.register_new_sku($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r',
    [crypto.randomUUID(), 'naka@test', '直接の試験', OWN, 'e'.repeat(64), JSON.stringify(DIRECT_ENTRY(code))]).then((r) => r.rows[0].r);
  const pathV = () => NG2.query("select ops.revoke_new_entry_lease('single', '試験: 鍵の順の取り消し') as r").then((r) => r.rows[0].r);

  let n = 0;
  await ta('[22] 0065 (#1664 Codex R1 High): 夜間ロードが代表 (親) を書く直前で止まっている間、照合の確かめ (3 者一致) はマスタの書き込みの鍵で待ち、ロードの commit の後の Company DB の値で比べる (古い値で verified にしない)', async () => {
    const { setActiveOwnershipInDb } = await import('./fixtures/master-widen-pr1.mjs');
    // 下書きの lk-p に代表 s001 (単品の代表) を付けて配る (期待値の代表 = s001)
    await reg('lk-p');
    const parentSql = `update core.products set parent_product_id = (select k.product_id from core.skus k where k.code = $2) where product_id = (select product_id from core.skus where code = $1)`;
    await M.query('begin');
    await M.query(`select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())`);
    await M.query(parentSql, ['lk-p', 's001']);
    await M.query('commit');
    const ep = await build('lk-p');
    await G.issueRegExport(dbE3, { actor: 'boss@test', exportId: ep.export_id }, opts);
    const expParent = (await one(`select i.expected -> 'values' ->> 'parent' as p from ops.ne_reg_export_items i where i.export_id = $1`, [ep.export_id])).p;
    assert.equal(expParent, 's001');
    // 照合の回 (NE の観測 = 配った値どおり・代表 s001)。ほかの生きている品目は「信用できない観測」= 待ち
    const RUNP = 'mc_20300109T000000022Z_cccccc';
    await M.query(`insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, '2030-01-09T00:00:00Z', 0)`, [RUNP]);
    const snap = (await WW.query('select ops.snapshot_ne_reg_targets($1) as r', [RUNP])).rows[0].r;
    const okc = (v) => ({ st: 'ok', v });
    const at = new Date(Date.now() + 60000).toISOString();
    const observations = snap.targets.map((t) => (t.code_norm === 'lk-p'
      ? { code_norm: 'lk-p', present: true, trusted: true, kind: 'single', cols: { name: okc('鍵の順 lk-p'), supplier: okc('0001'), cost: okc(300), price: okc(1500), tax_rate: okc(0.1), handling: okc('active'), parent: okc('s001') } }
      : { code_norm: t.code_norm, present: true, trusted: false, kind: t.sku_kind, cols: {} }));
    assert.ok(observations.some((o) => o.code_norm === 'lk-p'));
    const w = (await WW.query('select ops.record_ne_registration_observations($1::jsonb) as r', [JSON.stringify({ compare_run_id: RUNP,
      fetch: { generation_id: 'gen_locks_p', products_rev: '7', sets_rev: '8', raw_hash: 'b'.repeat(64) }, products_at: at, sets_at: at, absence_trusted: true, observations })])).rows[0].r;
    await WW.query('select ops.seal_ne_registration_run($1, $2, $3)', [RUNP, w.observation_hash, 'f'.repeat(64)]);
    // 持ち主 load の間の夜間ロード (代表を NE から写す道) = マスタの書き込み (排他) → 親子 (排他) を取り、代表を書く直前で止まる
    await setActiveOwnershipInDb(M, { ...ALL_COMPANY, 'products.parent': 'load' });
    const L = await open(null); clients.push(L);
    try {
      await L.query('begin');
      await L.query('select pg_advisory_xact_lock(core.master_write_lock_key())');
      await L.query(`select set_config('core.parent_protocol', '1', true), pg_advisory_xact_lock(core.parent_lock_key())`);
      const chk = launch(WW2.query('select ops.record_ne_registration_check($1) as r', [RUNP]).then((r) => r.rows[0].r));
      await sleep(800);
      assert.equal(chk.done, false, '照合の確かめはロードの取引を待つ');
      const waitKey = (await one(`select ((classid::bigint << 32) | objid::bigint)::text as k from pg_locks where locktype = 'advisory' and objsubid = 1 and not granted and pid = $1`, [PID.D]))?.k;
      assert.equal(waitKey, keyRow.mw, '待っているのはマスタの書き込みの鍵 (共有)');
      // ロードが代表を NE の値 (ここでは s002 = 配った値と違う) に書いて commit
      await L.query(parentSql, ['lk-p', 's002']);
      await L.query('commit');
      const r = await chk.promise;
      assert.ok(r.ok, r.err && r.err.message);
      const lkp = r.ok.cdb_drift.find((x) => x.code === 'lk-p');
      assert.ok(lkp, `ロードの後の Company DB の代表 (s002) で比べた = 3 者一致でない ${JSON.stringify(r.ok)}`);
      assert.deepEqual(lkp.cols.parent, { ok: false, expected: 's001', cdb: 's002' });
      assert.equal(r.ok.counts.verified ?? 0, 0, '古い値 (s001) で verified にしない');
      const it = await one(`select i.state, r2.state as reg from ops.ne_reg_export_items i join ops.master_registrations r2 on r2.sku_id = i.sku_id where i.export_id = $1`, [ep.export_id]);
      assert.deepEqual([it.state, it.reg], ['partial', 'draft']);
    } finally {
      try { await L.query('rollback'); } catch { /* commit 済み */ }
      await setActiveOwnershipInDb(M, ALL_COMPANY);
    }
    // 逆順のデッドロックが無い: 保存が代表を変える取引 (マスタの書き込み 共有 → SKU → 親子 排他 → CSV) が SKU の鍵を持っている間に照合の確かめが始まる →
    //   確かめは SKU の鍵で待ち (親子の鍵はまだ持たない)、保存は親子の鍵 (排他) を取れて終わる → 確かめも終わる (40P01 なし)
    const S = await open(null); clients.push(S);
    try {
      await S.query('begin');
      await S.query('select pg_advisory_xact_lock_shared(core.master_write_lock_key())');
      await S.query(`select pg_advisory_xact_lock(hashtextextended('core.sku:' || $1::text, 0))`, [await skuIdOf('lk-p')]);
      const chk2 = launch(WW2.query('select ops.record_ne_registration_check($1) as r', [RUNP]).then((r) => r.rows[0].r));
      await sleep(500);
      assert.equal(chk2.done, false, '確かめは保存の SKU の鍵を待つ');
      const par = launch(S.query('select pg_advisory_xact_lock(core.parent_lock_key())'));
      await sleep(2500);   // deadlock_timeout (1 秒) より長く待つ = 逆順なら PostgreSQL が 40P01 で片方を落とす
      assert.equal(par.done, true, '保存は親子の鍵 (排他) を取れる (確かめは親子の鍵をまだ持っていない)');
      assert.ok(par.ok, par.err && par.err.message);
      await S.query('commit');
      const r2 = await chk2.promise;
      assert.ok(r2.ok, `確かめも終わる (deadlock なし): ${r2.err && `${r2.err.code} ${r2.err.message}`}`);
    } finally { try { await S.query('rollback'); } catch { /* */ } }
  });

  await ta('[18a] アプリの鍵の入口 ops.acquire_new_entry_locks (PR-2 Codex R3): §3.10 の 2 (許可・種類の順に全部) を共有で取り、取引の終わりまで持つ・戻り値 = 今の許可が有効か', async () => {
    await E1.query('begin');
    assert.equal((await E1.query("select ops.acquire_new_entry_locks('single') as v")).rows[0].v, true);
    const st = await locksOf();
    assert.deepEqual(st.A.held, [2, 2]);   // 種類の順に全部 (single → set・#1644 Codex R1 Medium 2 でセットの道も同じ鍵)
    const mode = await q(`select mode from pg_locks where pid = $1 and locktype = 'advisory' order by mode`, [PID.A]);
    assert.deepEqual(mode.map((x) => x.mode), ['ShareLock', 'ShareLock']);
    await E1.query('commit');
    assert.deepEqual((await locksOf()).A.held, []);
  });
  await ta('[18] 鍵の順 (§3.10・PR-2 Codex R4): 9 つの道 (配る = アプリ / DB を直接・照合 ② の始めに閉じる・照合の確かめ・CSV を作る = DB を直接 / アプリ (PR-2)・登録 = アプリ (PR-2) / DB を直接・許可の取り消し) を、鍵ごとの barrier と始める順を変えて交差させる = どの道も小さい番号の鍵へ戻らない・deadlock なし・全部終わる (閉じた後の配るは new_entry_closed)', async () => {
    const BARRIERS = [2, 4, 5, 6, 7, 7.5, 8, 9];   // 🆕 0065: 7.5 = 親子の鍵 (排他 = 夜間ロード・代表を変える保存の代わり)
    const ORDERS = [['A', 'B', 'C', 'D', 'F', 'G', 'R', 'S', 'V'], ['C', 'V', 'D', 'A', 'R', 'F', 'S', 'B', 'G'], ['F', 'S', 'B', 'G', 'D', 'C', 'V', 'A', 'R']];
    for (const b of BARRIERS) {
      for (const order of ORDERS) {
        n++;
        const round = `barrier ${b}・順 ${order.join('')}`;
        const ya = `lk-a${n}`, yb = `lk-b${n}`, yf = `lk-c${n}`;
        await reg(ya); await reg(yb); await reg(yf);
        const ea = await build(ya), eb = await build(yb);
        const kA = await addSku(ya); await addSku(yb); await addSku(yf);
        const ridF = crypto.randomUUID(); await addRequest(ridF);
        const skuF = await skuIdOf(yf);
        // 🆕 PR-2: G = 登録済みの yg をアプリで作る・R = yr をアプリで登録・S = ys を DB の関数で直接登録
        const yg = `lk-g${n}`, yr = `lk-r${n}`, ys = `lk-s${n}`;
        await reg(yg); await addSku(yg);
        const ridG = crypto.randomUUID(); await addRequest(ridG);
        const ridR = crypto.randomUUID(); await addEditRequest(ridR); await addNewCode(yr); await addNewCode(ys);
        await T.query('begin');
        await T.query('select pg_advisory_xact_lock($1::bigint)', [b === 7 ? kA : KEY[b]]);   // 7 = A の SKU の鍵
        const paths = {};
        const start = { A: () => launch(pathA(ea.export_id)), B: () => launch(pathB(eb.export_id)), C: () => launch(pathC(`close_round_${n}`)), D: () => launch(pathD()), F: () => launch(pathF(ridF, skuF)),
          G: () => launch(pathG(ridG, yg)), R: () => launch(pathR(ridR, yr)), S: () => launch(pathS(ys)), V: () => launch(pathV()) };
        for (const p of order) { paths[p] = start[p](); await sleep(60); }
        let st;
        try { st = await settle(round, paths); } finally { await T.query('commit'); }
        assertMonotonic(round, st, paths);
        const res = Object.fromEntries(await Promise.all(Object.entries(paths).map(async ([k, s]) => [k, await s.promise])));
        for (const [name, r] of Object.entries(res)) assert.ok(!r.err || r.err.code !== '40P01', `${round} ${name}: deadlock`);
        assert.ok(closedOk(res.A), `${round} A: ${res.A.err && res.A.err.message}`);
        assert.ok(closedOk(res.B), `${round} B: ${res.B.err && res.B.err.message}`);
        assert.ok(!res.C.err, `${round} C: ${res.C.err && res.C.err.message}`);
        assert.ok(!res.D.err, `${round} D: ${res.D.err && res.D.err.message}`);
        assert.ok(closedOk(res.G), `${round} G: ${res.G.err && res.G.err.message}`);
        assert.ok(closedOk(res.R), `${round} R: ${res.R.err && res.R.err.message}`);
        assert.ok(!res.V.err, `${round} V: ${res.V.err && res.V.err.message}`);
        if (res.G.ok) assert.equal(res.G.ok.state, 'built', round);
        if (res.R.ok) assert.equal(res.R.ok.ok, true, round);
        // S は形の照らし直しで落ちてよい (試験の偽の約束) = 鍵の順だけを見る。deadlock 以外の失敗は許す
        if (res.A.ok) assert.equal(res.A.ok.export.state, 'issued', round);
        if (res.B.ok) assert.equal(res.B.ok.state, 'issued', round);
        // F は中身の照らし直しで落ちる (試験の偽の行) = 鍵の順だけを見る。deadlock 以外の失敗は許す
        await reopen();
      }
    }
    const waits = LOG.filter((x) => x.waiting != null);
    console.log(`  (barrier の記録 ${LOG.length} 件・例: ${JSON.stringify(waits.slice(0, 5))})`);
    assert.ok(LOG.some((x) => x.path === 'C' && x.waiting === 2 && x.held.length === 0), '照合 ② の始めに閉じるのは許可の排他の鍵で待つ (何も持たずに)');
    assert.ok(LOG.some((x) => x.path === 'A' && x.waiting === 7 && x.held.includes(2) && x.held.includes(5)), 'アプリの配るは許可・マスタの書き込みの鍵を持って SKU の鍵で待つ');
    assert.ok(LOG.some((x) => x.path === 'B' && x.waiting >= 4 && x.held.includes(2)), 'DB の関数を直接呼んでも許可の鍵を段階の鍵より先に取る');
    assert.ok(LOG.some((x) => x.path === 'F' && x.waiting === 4 && x.held.includes(1) && x.held.includes(2)), 'CSV を作る DB の関数は request → 許可の鍵を持って段階の鍵で待つ (R4)');
    assert.ok(LOG.some((x) => x.path === 'D' && x.waiting === 6), '照合の確かめは ne_reg_check の鍵で待つ (許可の鍵は取らない)');
    // 🆕 0065 (#1664 Codex R1 High): 照合の確かめは最初にマスタの書き込みの鍵 (共有) で夜間ロード (排他) を待つ (何も持たずに)・SKU の後・CSV の前に親子の鍵 (共有) で待つ
    assert.ok(LOG.some((x) => x.path === 'D' && x.waiting === 5 && x.held.length === 0), '照合の確かめはマスタの書き込みの鍵で待つ (何も持たずに)');
    assert.ok(LOG.some((x) => x.path === 'D' && x.waiting === 7.5 && x.held.includes(5) && x.held.includes(6) && !x.held.includes(8)), '照合の確かめは書き込み・確かめの鍵を持って親子の鍵で待つ (CSV の鍵より前)');
    // 🆕 PR-2 の道も小さい番号へ戻らない (上の assertMonotonic) うえで、それぞれの鍵の順で実際に待った
    assert.ok(LOG.some((x) => x.path === 'G' && x.waiting >= 4 && x.held.includes(1) && x.held.includes(2)), 'アプリの作るは request → 許可の鍵を持って段階の鍵より後ろで待つ');
    assert.ok(LOG.some((x) => x.path === 'R' && x.waiting >= 4 && x.held.includes(1) && x.held.includes(2)), 'アプリの登録は request → 許可の鍵を持って段階の鍵より後ろで待つ');
    assert.ok(LOG.some((x) => x.path === 'S' && x.waiting >= 4 && x.held.includes(2)), 'DB の登録の関数を直接呼んでも許可の鍵を段階の鍵より先に取る');
    assert.ok(LOG.some((x) => x.path === 'V' && x.waiting === 2 && x.held.length === 0), '許可の取り消しは許可の排他の鍵で待つ (何も持たずに)');
    assert.deepEqual(LOG.filter((x) => x.unknown > 0), [], '表に無い advisory の鍵を持って待つ道が無い (持っている鍵は全部 §3.10 の番号で比べた)');
    // 🆕 0063: D (照合の確かめ) が申告した X も、申告なしの Y も NE 確認済みにした (Y = 下書き → NE 登録待ち → NE 確認済み・system)
    const regs = await M.query(`select s.code, r.state, i.state as item_state, i.attempt_id is null as undeclared from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id
      join ops.ne_reg_export_items i on i.sku_id = r.sku_id where s.code in ('lk-x', 'lk-y') order by s.code`);
    assert.deepEqual(regs.rows.map((x) => [x.code, x.state, x.item_state, x.undeclared]), [['lk-x', 'ne_confirmed', 'verified', false], ['lk-y', 'ne_confirmed', 'verified', true]]);
    const evy = await M.query(`select e.from_state, e.to_state, e.actor_type, e.actor_id from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id
      where s.code = 'lk-y' and e.to_state in ('ne_pending', 'ne_confirmed') order by e.event_id`);
    assert.deepEqual(evy.rows.map((x) => [x.from_state, x.to_state, x.actor_type, x.actor_id]), [['draft', 'ne_pending', 'system', 'ne_compare'], ['ne_pending', 'ne_confirmed', 'system', 'ne_compare']]);
  });

  await ta('[18b] ops.ne_reg_file は許可の共有の鍵を返すまで持つ (R19): 取り消し (排他) は渡し終えるのを待つ・取り消しの後は渡さない', async () => {
    n++;
    const y = `lk-f${n}`;
    await reg(y);
    const e = await build(y);
    await G.issueRegExport(dbE1, { actor: 'boss@test', exportId: e.export_id }, opts);
    await E2.query('begin');
    const f = (await E2.query('select sha256 from ops.ne_reg_file($1::bigint)', [e.export_id])).rows[0];
    assert.equal(f.sha256, e.sha256);
    const rv = launch(NG.query("select ops.revoke_new_entry_lease('single', '試験: 渡している途中の取り消し') as r"));
    await sleep(500);
    assert.equal(rv.done, false, '取り消しは ne_reg_file の取引 (許可の共有の鍵) を待つ');
    await E2.query('commit');
    const r = await rv.promise; assert.ok(r.ok, r.err && r.err.message);
    await assert.rejects(E2.query('select sha256 from ops.ne_reg_file($1::bigint)', [e.export_id]), /reg_file_expired: lease_invalid/);
    await reopen();
  });

  await ta('[21] 照合 ② の始めに閉じる (許可の排他の鍵) は配る取引の完了を待つ・閉じた後の配る / 登録は閉じる (409)・結果を書く前に落ちた再実行では開かない', async () => {
    n++;
    const y = `lk-i${n}`, y2 = `lk-j${n}`;
    await reg(y); await reg(y2);
    const e = await build(y);
    const e2 = await build(y2);
    await E1.query('begin');
    await E1.query('select ops.ne_reg_issue($1::uuid, $2, $3::jsonb, $4::bigint) as r', [crypto.randomUUID(), 'boss@test', OWN, e.export_id]);
    const cl = launch(pathC('close_21'));
    await sleep(500);
    assert.equal(cl.done, false, '閉じるのは配る取引を待つ');
    await E1.query('commit');
    const cr = await cl.promise; assert.ok(cr.ok, cr.err && cr.err.message); assert.equal(cr.ok.revoked, 1);
    assert.equal((await one('select state from ops.ne_reg_exports where export_id = $1', [e.export_id])).state, 'issued');
    await assert.rejects(G.issueRegExport(dbE1, { actor: 'boss@test', exportId: e2.export_id }, opts), (err) => err.status === 409 || /new_entry_closed|入口は閉じている/.test(err.message));
    await assert.rejects(reg(`lk-k${n}`), (err) => err.status === 409 || /new_entry_closed|入口は閉じている/.test(err.message));
    await assert.rejects(E2.query('select sha256 from ops.ne_reg_file($1::bigint)', [e.export_id]), /reg_file_expired: lease_invalid/);
    // 結果を書く前に落ちた (close_21 の結果が無い) = 前の成功の回でも今回の回でも grant できない
    const prev = (await one('select compare_run_id from ops.new_entry_gate_results order by result_id desc limit 1')).compare_run_id;
    await assert.rejects(NG.query("select ops.grant_new_entry_lease('single', $1)", [prev]), /stop_floor/);
    await assert.rejects(NG.query("select ops.grant_new_entry_lease('single', 'close_21')"), /compare_run_mismatch/);
    assert.equal((await one("select ops.new_entry_lease_valid('single') as v")).v, false);
  });

  console.log(`\n${passed} 件 PASS`);
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  try { await admin.query(`drop database ${dbName} with (force)`); } catch (e) { console.error(`DB を消せない: ${e.message}`); }
  try { await admin.end(); } catch { /* */ }
}
