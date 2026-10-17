/**
 * test-master-movements.mjs — 商品ごとの入出庫履歴と月別のまとめ (apps/master-edit/movements.mjs・0083 仮の読むだけの関数・CompanyDB構想 26 の PR-1・PR-2)
 *
 * Company DB = PGlite (migration・見張りと画面のロール)。ロジザードの日次は本物の取込 → 日の締めで作る。発注アプリ = 一時の DATA_DIR の warehouse-mirror.db。
 * 本物の router を HTTP 越しに通す (画面のロール master_edit で読む = 権限が足りているかも確かめる)
 * 確かめること:
 *   1 計算 (純粋な関数): 区間の調整 = 在庫の差 − 出荷 + 仕入・取れなかった日はまとめて 1 行・時刻のずれの組 (+N の側に N 個以上の出荷)・錨の前は在庫数が空欄・
 *     仕入は取得の直前・最後の締めの後はいまのロジザードで合わせる・ページの切れ目は区間の始まり・月別の空欄 (データの無い月)・移行の発注は除く・単価の無い行 = 一部
 *   2 DB の関数: master_edit は 4 つの関数を呼べる・出荷の表 / 取得の回の表は読めない (42501)・public の実行権なし・search_path 固定・取消の伝票 / 明細は数えない
 *   3 API: 単品の履歴 (区間の最後 = ロジザードの数)・発注アプリの権限が無い人には仕入の行を出さない (調整に入る)・ページ (200 行・区間の途中で切らない・続き)・
 *     セット = 400・無いコード = 404・関数が無い (0083 の前) = 503 と言葉
 * 使い方: node scripts/test-master-movements.mjs
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import express from 'express';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const SCRATCH = await temporaryTestDataDir(import.meta.url, 'master-movements-');
process.env.DATA_DIR = SCRATCH;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles, MOVEMENT_FUNCTIONS } = await import('./company-db/create-master-edit-roles.mjs');
const { seedMovementsDb, seedMovementsMirror, SEED_NOW } = await import('./fixtures/movements-seed.mjs');
const M = await import('../apps/master-edit/movements.mjs');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 8).join('\n      ')); } };
const quiet = () => {};
const J = (s) => `${s}+09:00`;   // 日本時間の時刻
const isoJ = (s) => new Date(J(s)).toISOString();

console.log('1 計算 (純粋な関数)');
const A = (day, qty, hh = '18:00') => ({ day, status: 'complete', gen: isoJ(`${day}T${hh}:00`), qty });
await t('区間の調整・取れなかった日・時刻のずれの組・錨の前は空欄・仕入は取得の直前', () => {
  const anchors = [A('2026-10-01', 100), A('2026-10-02', 90), { day: '2026-10-03', status: 'missing', gen: null, qty: null }, A('2026-10-04', 75), A('2026-10-05', 123), A('2026-10-06', 123)];
  const ships = [
    { at: isoJ('2026-10-05T19:00:00'), qty: 2, slip: 'S4', shopName: '雑貨イズムYahoo!店' }, { at: isoJ('2026-10-04T10:00:00'), qty: 5, slip: 'S3', shopName: '雑貨イズム楽天市場店' },
    { at: isoJ('2026-10-03T10:00:00'), qty: 5, slip: 'S2', shopName: 'FBA納品' }, { at: isoJ('2026-10-02T10:00:00'), qty: 10, slip: 'S1', shopName: '雑貨イズムAmazon店' },
    { at: isoJ('2026-09-30T10:00:00'), qty: 4, slip: 'S0', shopName: '雑貨イズム楽天市場店' }];
  const totals = new Map([[1, 10], [2, 10], [4, 2]]);   // 区間 1 = 10/01→10/02・2 = 10/02→10/04・3 = 10/04→10/05・4 = 10/05→10/06
  const receipts = [{ slip: 'LZ-1', date: '2026-10-05', qty: 50, defective: 1, supplierName: 'AMC', actor: 'naka@test', amount: 5000, costUnknown: false }];
  const rows = M.buildMovementRows({ anchors, totals, receipts, ships, after: null, before: null, live: null, truncated: false, poNote: null });
  const v = rows.map((r) => [r.kind, r.in, r.out, r.balance, r.slip]);
  assert.deepEqual(v, [
    ['adjust', 2, null, 123, null],        // 10/06: 出荷 2 (10/05 19:00 の確定) を数えたのに在庫は動かない = +2
    ['ship', null, 2, 121, 'S4'],
    ['adjust', null, 2, 123, null],        // 10/05: 50 の仕入の後、在庫は 48 しか増えていない = −2 (ロジザードは 18 時の前に減った)
    ['receipt', 50, null, 125, 'LZ-1'],
    ['adjust', null, 5, 75, null],         // 10/02 → 10/04 (10/03 は取れなかった日): 90 − 10 = 80 のはずが 75
    ['ship', null, 5, 80, 'S3'], ['ship', null, 5, 85, 'S2'],
    ['ship', null, 10, 90, 'S1'],          // 10/02 は合う = 調整の行なし
    ['ship', null, 4, null, 'S0'],         // 錨 (10/01) の前 = 在庫数は空欄
  ]);
  assert.match(rows[0].reason, /時刻のずれの見込み \(10\/5 の調整と打ち消し合う\)/);
  assert.match(rows[2].reason, /時刻のずれの見込み \(10\/6/);
  assert.match(rows[4].reason, /10\/3 は在庫が取れなかった日 = 10\/2 からまとめて/);
  assert.equal(rows[4].label, '在庫の調整 (ずれ)');
  assert.equal(rows[3].reason, '仕入 (入荷) · 不良品 1 は入れていない');
  assert.equal(rows[3].dateOnly, true); assert.equal(rows[3].day, '2026-10-05'); assert.equal(rows[3].slipKind, 'logizard'); assert.equal(rows[3].actor, 'naka@test');
  assert.equal(rows[3].at, new Date(Date.parse(isoJ('2026-10-05T18:00:00')) - 1).toISOString(), '仕入はその日の取得の直前');
  assert.deepEqual([rows[1].party, rows[5].party, rows[6].party, rows[7].party], ['Yahoo!', '楽天市場', 'FBA納品', 'Amazon']);
});
await t('組にしない: +N の側の区間に N 個の出荷が無い (仕入の後の他勘定出庫など)・同じ向き・8 区間より離れている', () => {
  const anchors = [A('2026-10-01', 10), A('2026-10-02', 15), A('2026-10-03', 10)];
  const { adj } = M.adjustmentsOf(anchors, new Map(), []);
  assert.deepEqual(adj.map((a) => [a.u, a.hint]), [[5, null], [-5, null]]);
  // 10/02 に −3・その後は 7 のまま・+3 の側 = 出荷 3 があるのに在庫が動かない区間
  const days10 = Array.from({ length: 10 }, (_, i) => A(`2026-10-${String(i + 1).padStart(2, '0')}`, i === 0 ? 10 : 7));
  const near = M.adjustmentsOf(days10, new Map([[8, 3]]), []);   // 7 区間あと = 組
  assert.deepEqual(near.adj.filter((a) => a.u).map((a) => [a.day, a.u, a.hint && a.hint.pairDay]), [['2026-10-02', -3, '2026-10-09'], ['2026-10-09', 3, '2026-10-02']]);
  const far = M.adjustmentsOf(days10, new Map([[9, 3]]), []);    // 8 区間あと = 組にしない
  assert.deepEqual(far.adj.filter((a) => a.u).map((a) => [a.day, a.u, a.hint]), [['2026-10-02', -3, null], ['2026-10-10', 3, null]]);
});
await t('権限が無い = 仕入の行を出さず、仕入の分は調整に入る (理由に書く)', () => {
  const anchors = [A('2026-10-04', 75), A('2026-10-05', 125)];
  const rows = M.buildMovementRows({ anchors, totals: new Map(), receipts: null, ships: [], after: null, before: null, live: null, truncated: false, poNote: '発注アプリの権限が無いので、仕入もここに入っています' });
  assert.deepEqual(rows.map((r) => [r.kind, r.in, r.balance]), [['adjust', 50, 125]]);
  assert.match(rows[0].reason, /発注アプリの権限が無いので、仕入もここに入っています/);
});
await t('最後の締めの後: いまのロジザードの写しの時刻で合わせる・写しの後の動きは写しの数から', () => {
  const anchors = [A('2026-10-06', 123)];
  const ships = [{ at: isoJ('2026-10-07T12:00:00'), qty: 1, slip: 'S6' }, { at: isoJ('2026-10-07T10:00:00'), qty: 3, slip: 'S5' }];
  const live = { asOf: isoJ('2026-10-07T11:00:00'), qty: 118 };
  const rows = M.buildMovementRows({ anchors, totals: new Map(), receipts: [], ships, after: null, before: null, live, truncated: false, poNote: null });
  assert.deepEqual(rows.map((r) => [r.kind, r.in, r.out, r.balance, !!r.live]), [['ship', null, 1, 117, false], ['adjust', null, 2, 118, true], ['ship', null, 3, 120, false]]);
  const same = M.buildMovementRows({ anchors, totals: new Map(), receipts: [], ships, after: null, before: null, live: { ...live, qty: 120 }, truncated: false, poNote: null });
  assert.deepEqual(same.map((r) => [r.kind, r.balance]), [['ship', 119], ['ship', 120]], '合う = 調整の行なし');
});
await t('ページの切れ目 = 区間の始まり (錨の前は日の始まり)・窓の外の行は出さない', () => {
  const anchors = [A('2026-10-01', 100), A('2026-10-02', 90)];
  assert.equal(M.pageFloorOf(anchors, Date.parse(isoJ('2026-10-02T10:00:00'))), Date.parse(isoJ('2026-10-01T18:00:00')));
  assert.equal(M.pageFloorOf(anchors, Date.parse(isoJ('2026-10-01T18:00:00'))), Date.parse(isoJ('2026-10-01T00:00:00')), '錨ちょうど = その区間の前 (錨の前の日の始まり)');
  assert.equal(M.pageFloorOf(anchors, Date.parse(isoJ('2026-09-30T00:00:00'))), Date.parse(isoJ('2026-09-29T00:00:00')), '日の始まりちょうど = 前の日から');
  const ships = [{ at: isoJ('2026-10-02T10:00:00'), qty: 10, slip: 'S1' }];
  const rows = M.buildMovementRows({ anchors, totals: new Map([[1, 10]]), receipts: [{ slip: 'old', date: '2026-09-30', qty: 3, defective: 0 }], ships,
    after: Date.parse(isoJ('2026-10-01T18:00:00')), before: null, live: null, truncated: false, poNote: null });
  assert.deepEqual(rows.map((r) => r.slip), ['S1'], '切れ目より前の仕入は次のページ');
});
await t('月別: データの無い月は空欄・移行の発注は除く・単価の無い行 = 一部・発注アプリの権限が無い = 欄なし', () => {
  const months = ['2026-10-01', '2026-09-01', '2026-07-01', '2026-06-01', '2024-12-01'];
  const ship = [{ month: '2026-10-01', orderQty: 32, shipQty: 26 }, { month: '2026-09-01', orderQty: 4, shipQty: 4 }];
  const po = {
    receipts: [{ date: '2026-10-05', qty: 50, amount: 5000, costUnknown: false }, { date: '2026-10-06', qty: 2, amount: 0, costUnknown: true }],
    orders: [{ issuedAt: isoJ('2026-09-20T10:00:00'), qty: 80, unitCost: 372, origin: null }, { issuedAt: '2026-07-12T15:30:00.000Z', qty: 100, unitCost: 10, origin: 'migration' },
      { issuedAt: '2026-09-30T15:30:00.000Z', qty: 5, unitCost: null, origin: 'supplement' }],   // 9/30 15:30Z = 10/01 00:30 JST = 10 月
  };
  const r = M.buildMonthlySummary({ months, ship, po });
  assert.deepEqual(r.map((m) => [m.month, m.orderQty, m.shipQty, m.receiptQty, m.receiptAmount, m.receiptAmountPartial, m.poQty, m.poAmount, m.poAmountPartial, m.orderAmount]), [
    ['2026-10', 32, 26, 52, 5000, true, 5, 0, true, null],
    ['2026-09', 4, 4, 0, 0, false, 80, 29760, false, null],
    ['2026-07', 0, 0, 0, 0, false, 0, 0, false, null],          // 移行の発注 (7/13 の取り込み) は入れない
    ['2026-06', 0, 0, null, null, false, null, null, false, null],   // 発注アプリの前 = 空欄
    ['2024-12', null, null, null, null, false, null, null, false, null],   // Company DB の出荷の前 = 空欄
  ]);
  const no = M.buildMonthlySummary({ months, ship, po: null });
  assert.equal('receiptQty' in no[0], false);
});
await t('関数が無い・権限が無い = no_privilege の言葉 (落とさない)', async () => {
  const bad = { query: async () => { throw Object.assign(new Error('function ops.sku_stock_anchors does not exist'), { code: '42883' }); } };
  const r = await M.readMovementsPage(bad, { sku_id: '1', code: 'x' }, {});
  assert.deepEqual([r.ok, r.reason], [false, 'no_privilege']);
  assert.match(r.error, /0083/);
  const m = await M.readMonthly({ query: async () => { throw Object.assign(new Error('permission denied'), { code: '42501' }); } }, { sku_id: '1', code: 'x' }, {});
  assert.deepEqual([m.ok, m.reason], [false, 'no_privilege']);
  assert.deepEqual(await M.readMovementsPage(bad, { sku_id: '1', code: 'x' }, { before: 'nope' }).then((x) => [x.ok, x.reason]), [false, 'bad_cursor']);
});

console.log('2 DB の関数 (PGlite)');
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await pg.query('reset role');   // 見本のデータは試験の接続 (superuser) で入れる
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
await pg.query(`insert into core.products (company_id, name) values (1, '見本A'), (1, '見本B')`);
await pg.query(`insert into core.skus (company_id, product_id, sku_kind, code, name) select 1, product_id, 'single', case name when '見本A' then 'mv-a' else 'mv-b' end, name from core.products`);
await pg.query(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'mv-set', 'セット')`);
const skuA = (await one(`select sku_id::text as id from core.skus where code = 'mv-a'`)).id;
const skuB = (await one(`select sku_id::text as id from core.skus where code = 'mv-b'`)).id;
await seedMovementsDb(pg, db, { skuA, skuB });
await pg.query('reset role');

async function asEditor(fn) { await pg.query('set role master_edit'); try { return await fn(); } finally { await pg.query('reset role'); } }
await t('master_edit は 4 つの関数を呼べる・出荷の表と取得の回の表は読めない・public の実行権なし・search_path 固定', async () => {
  await asEditor(async () => {
    const an = (await pg.query(`select snapshot_date::text as d, status, qty from ops.sku_stock_anchors($1::bigint, '2026-09-01')`, [skuA])).rows;
    assert.deepEqual(an.map((r) => [r.d, r.status, r.qty]), [['2026-10-01', 'complete', 100], ['2026-10-02', 'complete', 90], ['2026-10-03', 'missing', null], ['2026-10-04', 'complete', 75], ['2026-10-05', 'complete', 123], ['2026-10-06', 'complete', 123]]);
    const cap = (await pg.query(`select captured_at from ops.sku_stock_anchors($1::bigint, '2026-10-01') where status = 'complete' limit 1`, [skuA])).rows[0].captured_at;
    assert.equal(new Date(cap).toISOString(), '2026-10-01T09:00:00.000Z', '取得時刻 = 取得の回の世代 (ロジザードは captured_at が空)');
    const bounds = ['2026-10-01T09:00:00Z', '2026-10-02T09:00:00Z', '2026-10-04T09:00:00Z', '2026-10-05T09:00:00Z', '2026-10-06T09:00:00Z'];
    const tot = (await pg.query('select bucket, qty::int as q from ops.sku_shipment_totals($1::bigint, $2::timestamptz[]) order by 1', [skuA, bounds])).rows;
    assert.deepEqual(tot.map((r) => [r.bucket, r.q]), [[1, 10], [2, 10], [4, 2]]);
    const sh = (await pg.query(`select ne_slip_no, shop_name, qty::int as q from ops.sku_movement_shipments($1::bigint, null, null, 10)`, [skuA])).rows;
    assert.deepEqual(sh.map((r) => [r.ne_slip_no, r.q]), [['S6', 1], ['S5', 3], ['S4', 2], ['S3', 5], ['S2', 5], ['S1', 10], ['S0', 4]]);
    assert.equal(sh.find((r) => r.ne_slip_no === 'S2').shop_name, 'FBA納品');
    assert.equal(Object.keys(sh[0]).includes('tracking_no'), false);
    const lim = (await pg.query(`select count(*)::int as n from ops.sku_movement_shipments($1::bigint, null, null, 99999)`, [skuB])).rows[0].n;
    assert.equal(lim, 255);
    const mo = (await pg.query(`select month::text as m, order_qty::int as o, ship_qty::int as s from ops.sku_shipments_monthly($1::bigint, '2026-09-01')`, [skuA])).rows;
    assert.deepEqual(mo.map((r) => [r.m, r.o, r.s]), [['2026-09-01', 4, 4], ['2026-10-01', 32, 26]]);   // 10 月の受注 = 出荷 26 + 出していない 6
    for (const sql of ['select 1 from core.shipments limit 1', 'select 1 from core.shipment_lines limit 1', 'select 1 from core.ne_shops limit 1', 'select 1 from ops.ingest_runs limit 1']) {
      await assert.rejects(() => pg.query(sql), (e) => e.code === '42501', sql);
    }
  });
  const fns = (await pg.query(`select p.oid::regprocedure::text as f, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub,
      has_function_privilege('master_edit', p.oid, 'execute') as me, has_function_privilege('watcher', p.oid, 'execute') as w
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname in ('sku_stock_anchors', 'sku_shipment_totals', 'sku_movement_shipments', 'sku_shipments_monthly') order by 1`)).rows;
  assert.equal(fns.length, 4);
  assert.deepEqual(fns.map((f) => f.f.replace(/,\s*/g, ',')).sort(), MOVEMENT_FUNCTIONS.map((f) => f.replace(/,\s*/g, ',')).sort());
  for (const f of fns) { assert.equal(f.d, true, f.f); assert.match(f.c, /search_path=pg_catalog, pg_temp/, f.f); assert.equal(f.pub, false, f.f); assert.equal(f.me, true, f.f); assert.equal(f.w, false, f.f); }
});

console.log('3 API (本物の router)');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const mirrorDb = initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
seedMovementsMirror(mirrorDb);
const { default: router, __setPgClientFactory, __setClock } = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/mv';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/mv';
process.env.MASTER_EDITORS = 'naka@test';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  // 1 つの PGlite を順番に使う (同時の要求でロールが混ざらない)
  let release;
  const prev = chain; chain = new Promise((r) => { release = r; });
  await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (q, p) => pg.query(q, p), end: async () => { await pg.query('reset role'); release(); }, on: () => {} };
});
__setClock(() => SEED_NOW);
const app = express();
app.use((req, res, next) => {
  const s = req.headers['x-test-session'];
  req.session = s === 'po' ? { authenticated: true, email: 'naka@test', allowedApps: ['master-edit', 'purchase-orders'] }
    : { authenticated: true, email: 'other@test', allowedApps: ['master-edit'] };
  next();
});
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const BASE = `http://127.0.0.1:${server.address().port}/apps/master-edit`;
const get = async (path, session = 'po') => { const r = await fetch(BASE + path, { headers: { 'x-test-session': session, Accept: 'application/json' } }); return { status: r.status, j: await r.json() }; };

try {
  await t('単品の履歴 (発注アプリの権限あり): 出荷・仕入・調整・最後の締め・いまのロジザード・区間の最後 = ロジザードの数', async () => {
    const { status, j } = await get('/api/sku/MV-A/movements');
    assert.equal(status, 200, JSON.stringify(j));
    assert.deepEqual([j.ok, j.next, j.po, j.lastClose.day, j.firstDay, j.liveAt], [true, null, 'ok', '2026-10-06', '2026-10-01', '2026-10-07T02:00:00.000Z']);
    assert.deepEqual(j.rows.map((r) => [r.kind, r.in, r.out, r.balance, r.slip]), [
      ['ship', null, 1, 117, 'S6'], ['adjust', null, 2, 118, null], ['ship', null, 3, 120, 'S5'],
      ['adjust', 2, null, 123, null], ['ship', null, 2, 121, 'S4'], ['adjust', null, 2, 123, null], ['receipt', 50, null, 125, 'LZ-1'],
      ['adjust', null, 5, 75, null], ['ship', null, 5, 80, 'S3'], ['ship', null, 5, 85, 'S2'], ['ship', null, 10, 90, 'S1'], ['ship', null, 4, null, 'S0']]);
    const rc = j.rows.find((r) => r.kind === 'receipt');
    assert.deepEqual([rc.party, rc.actor, rc.slipKind], ['AMC', 'naka@test', 'logizard']);
  });
  await t('発注アプリの権限が無い人: 仕入の行を出さない (仕入の分は調整に入る・理由に書く)', async () => {
    const { j } = await get('/api/sku/mv-a/movements', 'plain');
    assert.equal(j.po, 'denied');
    assert.equal(j.rows.some((r) => r.kind === 'receipt'), false);
    const adj = j.rows.find((r) => r.kind === 'adjust' && r.balance === 123 && r.in === 48);
    assert.ok(adj, JSON.stringify(j.rows.filter((r) => r.kind === 'adjust')));
    assert.match(adj.reason, /発注アプリの権限が無いので、仕入もここに入っています/);
    assert.equal(JSON.stringify(j).includes('naka@test'), false, '取り込んだ人も出さない');
  });
  await t('ページ: 出荷 200 行ずつ・区間の途中で切らない (切れ目 = 区間の始まりの取得時刻)・続きは錨の前 = 在庫数は空欄・その後は続きなし', async () => {
    const p1 = await get('/api/sku/mv-b/movements');
    assert.equal(p1.status, 200, JSON.stringify(p1.j).slice(0, 300));
    const ships1 = p1.j.rows.filter((r) => r.kind === 'ship');
    assert.equal(ships1.length, 225, '200 行目の区間 (10/02) は全部');
    assert.equal(p1.j.next, '2026-10-01T09:00:00.000Z');
    assert.equal(p1.j.rows.some((r) => r.kind === 'adjust'), false, '毎日 45 ずつ減る = 合う');
    assert.equal(ships1[ships1.length - 1].balance, 299);
    assert.equal(ships1[0].balance, 75);
    const p2 = await get('/api/sku/mv-b/movements?before=' + encodeURIComponent(p1.j.next));
    assert.equal(p2.j.rows.length, 30);
    assert.equal(p2.j.rows.every((r) => r.balance === null), true);
    assert.equal(p2.j.next, null);
    assert.equal(p2.j.liveAt, null, 'いまのロジザードは最初のページだけ');
  });
  await t('月別 (13 か月・新しい月から)・権限が無い人には仕入・発注の欄を出さない', async () => {
    const { j } = await get('/api/sku/mv-a/monthly');
    assert.equal(j.ok, true, JSON.stringify(j));
    assert.equal(j.months.length, 13);
    assert.deepEqual(j.months[0], { month: '2026-10', orderQty: 32, shipQty: 26, orderAmount: null, shipAmount: null, receiptQty: 50, receiptAmount: 5000, receiptAmountPartial: false, poQty: 0, poAmount: 0, poAmountPartial: false });
    assert.deepEqual([j.months[1].month, j.months[1].orderQty, j.months[1].poQty, j.months[1].poAmount], ['2026-09', 4, 80, 29760]);
    assert.deepEqual([j.months[3].month, j.months[3].poQty, j.months[4].month, j.months[4].poQty], ['2026-07', 0, '2026-06', null], '7 月の移行の発注は入れない・6 月は発注アプリの前');
    assert.deepEqual([j.months[12].month, j.months[12].orderQty], ['2025-10', 0]);
    const no = await get('/api/sku/mv-a/monthly', 'plain');
    assert.equal(no.j.po, 'denied');
    assert.equal('receiptQty' in no.j.months[0], false);
  });
  await t('セット = 400・無いコード = 404・続きの位置が読めない = 400', async () => {
    assert.deepEqual([(await get('/api/sku/mv-set/movements')).status, (await get('/api/sku/mv-set/monthly')).j.reason], [400, 'not_single']);
    assert.equal((await get('/api/sku/nope/movements')).status, 404);
    assert.equal((await get('/api/sku/mv-a/movements?before=xyz')).status, 400);
  });
  await t('関数の実行権が無い (流し直しの前) = 503 と言葉', async () => {
    for (const f of MOVEMENT_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
    try {
      const { status, j } = await get('/api/sku/mv-a/movements');
      assert.deepEqual([status, j.reason], [503, 'no_privilege']);
      assert.equal((await get('/api/sku/mv-a/monthly')).status, 503);
    } finally {
      for (const f of MOVEMENT_FUNCTIONS) await pg.query(`grant execute on function ${f} to master_edit`);
    }
  });
} finally {
  server.close();
}

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
