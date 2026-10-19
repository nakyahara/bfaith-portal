/**
 * test-master-sales-chart.mjs — 商品の画面の「販売数のグラフ」(apps/master-edit/sales-chart.mjs・0086 の読むだけの関数 ops.sku_sales_daily・10/17 中原さん)
 *
 * Company DB = PGlite (migration・見張りと画面のロール)。注文は本物の取込 (core.apply_order_batch)・在庫は本物の取込 → 日の締め。本物の router を HTTP 越しに通す (画面のロール master_edit で読む)
 * 確かめること:
 *   1 計算 (純粋な関数): 週 = 月曜はじまり 52・月 24・日 90 / 今日は入れない / いちばん新しい週・月は途中 / 2025-01 より前は空欄 (0 と出さない)・途中からの期間 /
 *     モールの分け方 (楽天・Amazon・Yahoo!・その他) / 在庫 = 期間の最後の締め・欠品の日数・締めの無い期間は空
 *   2 DB の関数: 数え方 = 売上日次 (0021) を末端の SKU まで展開した mart.sales_expanded_to_skus (0042) と同じ数 (日 × モール)・メルカリも入る・取消の注文 / 明細の取消 / 外れた明細は数えない・
 *     出品だけの明細は出品の構成で・NE のセット (入れ子も) は構成品へ・セットの画面 = そのセットとして売れた数 / master_edit は呼べる・注文の表は読めない (42501)・public の実行権なし・search_path 固定
 *   3 API: 単品 (在庫の線あり)・セット (在庫なし)・無いコード = 404・関数の実行権が無い = 503 と言葉・期限切れ = 503 (timeout)
 *     長く待つ読み (?wait=long・画面の「もう一度読む」): この値だけ 30 秒・ほかの値はふつうの 8 秒・文も期限で止める・同時に 1 本まで (busy)
 * 使い方: node scripts/test-master-sales-chart.mjs
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import express from 'express';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const SCRATCH = await temporaryTestDataDir(import.meta.url, 'master-sales-chart-');
process.env.DATA_DIR = SCRATCH;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles, SALES_CHART_FUNCTIONS } = await import('./company-db/create-master-edit-roles.mjs');
const { HEAVY_ENTRY_MANIFEST } = await import('./company-db/heavy-entry-manifest.mjs');
const { seedSalesChartDb, SALES_NOW } = await import('./fixtures/sales-chart-seed.mjs');
const C = await import('../apps/master-edit/sales-chart.mjs');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e).split('\n').slice(0, 8).join('\n      ')); } };
const quiet = () => {};

console.log('1 計算 (純粋な関数)');
await t('期間の区切り: 週 = 月曜はじまり 52 (最後 = 前日を含む週)・月 = 24 か月・日 = 90 日', () => {
  const w = C.bucketsOf('week', '2026-10-17');   // 10/17 = 土曜
  assert.equal(w.length, 52);
  assert.deepEqual(w[51], { from: '2026-10-12', to: '2026-10-18' });
  assert.deepEqual(w[0], { from: '2025-10-20', to: '2025-10-26' });
  assert.equal(C.mondayOf('2026-10-12'), '2026-10-12', '月曜はその日');
  assert.equal(C.mondayOf('2026-10-18'), '2026-10-12', '日曜は前の月曜');
  const m = C.bucketsOf('month', '2026-10-17');
  assert.equal(m.length, 24);
  assert.deepEqual([m[0], m[23]], [{ from: '2024-11-01', to: '2024-11-30' }, { from: '2026-10-01', to: '2026-10-31' }]);
  assert.deepEqual(C.bucketsOf('month', '2026-03-05').find((b) => b.from === '2026-02-01'), { from: '2026-02-01', to: '2026-02-28' }, 'うるう年でない 2 月');
  const d = C.bucketsOf('day', '2026-10-17');
  assert.deepEqual([d.length, d[0].from, d[89].from], [90, '2026-07-20', '2026-10-17']);
});
await t('数の組み立て: モールの分け方・途中の期間・記録の前は空欄・途中から・在庫 (最後の締め・欠品の日)・前日より後は入れない', () => {
  const sales = [
    { day: '2026-10-17', mall: 'rakuten', qty: 3, viaSet: 1 }, { day: '2026-10-16', mall: 'amazon', qty: 2, viaSet: 0 }, { day: '2026-10-12', mall: 'yahoo', qty: 1, viaSet: 0 },
    { day: '2026-10-12', mall: 'mercari', qty: 4, viaSet: 0 }, { day: '2026-10-12', mall: 'aupay', qty: 1, viaSet: 1 }, { day: '2026-10-11', mall: 'other', qty: 5, viaSet: 0 },
    { day: '2026-10-18', mall: 'rakuten', qty: 99, viaSet: 0 },   // 今日 = 入れない
    { day: '2024-12-31', mall: 'rakuten', qty: 7, viaSet: 0 }, { day: '2025-01-01', mall: 'rakuten', qty: 2, viaSet: 0 },
  ];
  const anchors = [{ day: '2026-10-13', status: 'complete', qty: 4 }, { day: '2026-10-14', status: 'complete', qty: 0 }, { day: '2026-10-15', status: 'missing', qty: null },
    { day: '2026-10-16', status: 'complete', qty: 0 }, { day: '2026-10-17', status: 'complete', qty: 6 }, { day: '2026-10-10', status: 'complete', qty: 9 }];
  const r = C.buildSalesChart({ sales, anchors, through: '2026-10-17' });
  const w = r.views.week.buckets;
  assert.deepEqual([w[51].partial, w[51].qty, w[51].total, w[51].viaSet], [true, { rakuten: 3, amazon: 2, yahoo: 1, other: 5 }, 11, 2]);
  assert.deepEqual(w[51].stock, { end: 6, endDay: '2026-10-17', days: 4, zeroDays: 2 });
  assert.deepEqual([w[50].from, w[50].partial, w[50].total, w[50].stock], ['2026-10-05', false, 5, { end: 9, endDay: '2026-10-10', days: 1, zeroDays: 0 }]);
  assert.equal(w[49].stock, null, '締めの無い週 = 空');
  assert.equal(r.stockFrom, '2026-10-10');
  const m = r.views.month.buckets;
  assert.deepEqual([m[0].from, m[0].noData, m[0].total, m[0].qty], ['2024-11-01', true, null, null], '2025-01 より前 = 空欄 (0 と出さない)');
  assert.deepEqual([m[1].from, m[1].noData, m[1].total], ['2024-12-01', true, null], '12/31 の注文も記録の前 (数えない)');
  assert.deepEqual([m[2].from, m[2].noData, m[2].partialData, m[2].total], ['2025-01-01', false, false, 2]);
  assert.deepEqual([m[23].partial, m[23].total], [true, 16]);
  assert.deepEqual(r.views.month.totals, { rakuten: 5, amazon: 2, yahoo: 1, other: 10, total: 18, viaSet: 2 });
  const wk = C.buildSalesChart({ sales, anchors: null, through: '2025-01-03' }).views.week.buckets;
  assert.deepEqual([wk[51].from, wk[51].partialData, wk[51].noData, wk[51].total, wk[51].stock], ['2024-12-30', true, false, 2, null], '週の途中から (12/30〜) = 途中から・在庫なし (セット)');
  const dd = r.views.day.buckets;
  assert.deepEqual([dd[89].from, dd[89].total, dd[89].stock.end, dd[88].stock.zeroDays, dd[87].stock], ['2026-10-17', 3, 6, 1, null]);
  assert.equal(dd.some((b) => b.partial), false, '日は途中にならない');
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
await pg.query('reset role');
const ids = await seedSalesChartDb(pg, db);
await pg.query('reset role');
const all = async (sql, p = []) => (await pg.query(sql, p)).rows;
// ── 境目の見本 (Codex R1 Medium 3): sc-d と 入れ子 6 段のセット (N1 ⊃ D・N2 ⊃ N1 … N6 ⊃ N5)・循環 (C1 = D + C2・C2 = C1)・
//    出品 lx = D×1 + N1×1 (同じ出品に D そのものと D を含むセット)・ln5 = N5×1 (出品から 5 段)・ln6 = N6×1 (6 段 = 届かない)
const mkSku = async (code, kind) => {
  const p = kind === 'single' ? (await all(`insert into core.products (company_id, name) values (1, $1) returning product_id`, [`見本 ${code}`]))[0].product_id : null;
  return Number((await all(`insert into core.skus (company_id, product_id, sku_kind, code, name) values (1, $1, $2, $3, $3) returning sku_id`, [p, kind, code]))[0].sku_id);
};
ids.D = await mkSku('sc-d', 'single');
const N = [ids.D];
for (let i = 1; i <= 6; i++) N.push(await mkSku(`sc-n${i}`, 'set'));
for (let i = 1; i <= 6; i++) await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'ne')`, [N[i], N[i - 1]]);
const C1 = await mkSku('sc-c1', 'set'), C2 = await mkSku('sc-c2', 'set');
await pg.query(`insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'ne'), (1, $1, $3, 1, 'ne'), (1, $3, $1, 1, 'ne')`, [C1, ids.D, C2]);
for (const [mall, code, comps] of [['rakuten', 'lx', [[ids.D, 1], [N[1], 1]]], ['amazon', 'ln5', [[N[5], 1]]], ['amazon', 'ln6', [[N[6], 1]]]]) {
  const lid = (await all(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, $1, '', $2, 'active') returning listing_id`, [mall, code]))[0].listing_id;
  for (const [s, q] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, $3, 'imported', 'system')`, [lid, s, q]);
}
{
  let seq = 5000;
  const order = (mall, scope, no, at, line, shop) => pg.query(`select core.apply_order_batch(1::smallint, $1, $2, $3, $4::bigint, $5::jsonb, $6::jsonb)`, [mall, scope, no, ++seq,
    JSON.stringify({ source_system: 'mall_api', shop_code: shop, ordered_at: `${at}T10:00:00+09:00`, status: 'shipped', amount_source: 'mall_api', source_updated_at: '2026-10-18T00:00:00Z', transform_version: 't-1', content_hash: 'b' + seq }),
    JSON.stringify([{ line_key: '1', listing_code: null, sku_code: null, qty: 1, cancelled_qty: 0, unit_price_jpy: null, line_amount_jpy: 1000, tax_rate: null, amount_source: 'mall_api', ...line }])]);
  await order('yahoo', 'main', 'Y-N5', '2026-10-02', { sku_code: 'sc-n5' }, '2');            // 5 段のセット = D に届く
  await order('yahoo', 'main', 'Y-N6', '2026-10-03', { sku_code: 'sc-n6' }, '2');            // 6 段 = 届かない (0042 も深すぎて展開しない)
  await order('aupay', 'main', 'U-C2', '2026-10-04', { sku_code: 'sc-c2' }, '5');            // 循環 (C2 → C1 → D・C1 → C2 は止める)
  await order('rakuten', 'main', 'R-LX', '2026-10-05', { listing_code: 'lx' }, '1');         // 同じ出品に D と D を含むセット = 2 (うちセット経由 1)
  await order('rakuten', 'main', 'R-DL', '2026-10-06', { sku_code: 'sc-d', listing_code: 'lx' }, '1');   // SKU と出品の両方が分かる明細 = SKU だけで数える (二重にしない)
  await order('amazon', 'jp', 'M-L5', '2026-10-07', { listing_code: 'ln5' }, '4');           // 出品から 5 段 = 届く
  await order('amazon', 'jp', 'M-L6', '2026-10-08', { listing_code: 'ln6' }, '4');           // 出品から 6 段 = 届かない
  await order('aupay', 'main', 'U-C1', '2026-10-09', { sku_code: 'sc-c1', qty: 2 }, '5');    // 循環の入口のセット 2 つ
  await order('rakuten', 'main', 'R-FUT', '2099-01-01', { sku_code: 'sc-d' }, '1');          // 先の日付 = 入れない (Codex R1 Medium 1)
}
async function asEditor(fn) { await pg.query('set role master_edit'); try { return await fn(); } finally { await pg.query('reset role'); } }
const fnRows = (sku, from = '2024-01-01') => all(`select date_jst::text as d, mall, qty::int as q, via_set::int as v from ops.sku_sales_daily($1::bigint, $2::date) order by 1, 2`, [sku, from]);

await t('数え方: 取消の注文・明細の取消・外れた明細は数えない・出品だけの明細は出品の構成・NE のセット (入れ子も) は構成品へ・メルカリも入る・セット経由を分ける', async () => {
  const a = await asEditor(() => fnRows(ids.A));
  assert.deepEqual(a.map((r) => [r.d, r.mall, r.q, r.v]), [
    // 2024-12-20 の 9 個 = Company DB の注文の始まり (2025-01-01) より前 = 返さない
    ['2026-09-01', 'aupay', 3, 0],       // 4 − 明細の取消 1
    ['2026-10-01', 'mercari', 1, 0],      // メルカリ (売上日次には無い)
    ['2026-10-10', 'yahoo', 3, 3],        // セット S2 (= S×1 + A×1 = A 3 個) を 1 つ・取消の注文 (A 5) は 0 = 足して 3
    ['2026-10-15', 'amazon', 8, 2],       // 出品 am-s (セット S = A×2) 1 つ + まとめ売りの出品 am-a3 (A×3) 2 つ (セット経由ではない)
    ['2026-10-16', 'rakuten', 3, 0],      // SKU の明細 2 + 出品だけの明細 r-a 1 (外れた明細 A 10 は数えない)
    ['2026-10-18', 'rakuten', 7, 0],      // 今日 (関数は返す・グラフは入れない)
  ]);
  const b = await asEditor(() => fnRows(ids.B));
  assert.deepEqual(b.map((r) => [r.d, r.mall, r.q, r.v]), [['2026-10-10', 'yahoo', 1, 1], ['2026-10-15', 'amazon', 1, 1], ['2026-10-16', 'rakuten', 1, 0]]);
  const s = await asEditor(() => fnRows(ids.S));
  assert.deepEqual(s.map((r) => [r.d, r.mall, r.q, r.v]), [['2026-10-10', 'yahoo', 1, 1], ['2026-10-15', 'amazon', 1, 0]], 'セットの画面 = そのセットとして売れた数 (大きいセット S2 の分はセット経由)');
  assert.deepEqual((await asEditor(() => fnRows(ids.A, '2026-10-15'))).map((r) => r.d), ['2026-10-15', '2026-10-16', '2026-10-18'], 'p_from から');
});
await t('境目 (Codex R1 Medium 3): 入れ子 5 段は届く・6 段は届かない (出品からも)・循環は止める・同じ出品に SKU とそれを含むセット・SKU と出品の両方が分かる明細は二重にしない・先の日付は入れない', async () => {
  const d = await asEditor(() => fnRows(ids.D));
  assert.deepEqual(d.map((r) => [r.d, r.mall, r.q, r.v]), [
    ['2026-10-02', 'yahoo', 1, 1], ['2026-10-04', 'aupay', 1, 1], ['2026-10-05', 'rakuten', 2, 1], ['2026-10-06', 'rakuten', 1, 0], ['2026-10-07', 'amazon', 1, 1], ['2026-10-09', 'aupay', 2, 2]]);
});
await t('売上日次 (0021) を末端の SKU まで展開した数 (0042 の mart.sales_expanded_to_skus) と同じ (日 × モール・メルカリは売上日次に無い)', async () => {
  for (const m of ['rakuten', 'amazon', 'yahoo', 'aupay']) {
    const scope = m === 'amazon' ? 'jp' : 'main';
    let r = (await all(`select * from mart.refresh_sales_daily(1::smallint, $1, $2, 31, true, 'test')`, [m, scope]))[0];
    while (Number(r.remaining) > 0) r = (await all(`select * from mart.refresh_sales_daily(1::smallint, $1, $2, 31, false, 'test')`, [m, scope]))[0];
  }
  for (const [name, sku] of [['A', ids.A], ['B', ids.B], ['D (境目)', ids.D]]) {
    // 0042 は期間を引数で受ける = こちらの関数と同じ期間 (2025-01-01 〜 先の日付の前) にそろえて比べる
    const mart = await all(`select date_jst::text as d, mall, sum(units)::int as q, coalesce(sum(units) filter (where via_set), 0)::int as v from mart.sales_expanded_to_skus(1::smallint, '2025-01-01', '2098-12-31')
      where sku_id = $1 group by 1, 2 order by 1, 2`, [sku]);
    const mine = (await fnRows(sku)).filter((r) => r.mall !== 'mercari');
    assert.ok(mart.length > 0, name);
    // 数 (q) は全部同じ。うちセット経由 (v) だけ、同じ出品に D そのものと D を含むセットがある行 (10/05 の lx) で違う:
    //   0042 は行 × SKU に 1 つの印 (どれかの道がセットを通れば全部をセット経由) / こちらは道ごと (2 のうちセットを通ったのは 1) = こちらを正とする (README に書いた)
    const known = name.startsWith('D') ? { d: '2026-10-05', mart: 2, mine: 1 } : null;
    if (known) {
      assert.deepEqual([mart.find((r) => r.d === known.d).v, mine.find((r) => r.d === known.d).v], [known.mart, known.mine]);
      for (const r of mart) if (r.d === known.d) r.v = known.mine;
    }
    assert.deepEqual(mine, mart, `${name}: ${JSON.stringify(mine)} != 0042 ${JSON.stringify(mart)}`);
  }
});
await t('master_edit は呼べる・注文の表は読めない・public の実行権なし・security definer・search_path 固定・見張りには渡さない・重い入口の棚卸しにある', async () => {
  await asEditor(async () => {
    for (const sql of ['select 1 from core.orders limit 1', 'select 1 from core.order_lines limit 1']) await assert.rejects(() => pg.query(sql), (e) => e.code === '42501', sql);
  });
  const fns = await all(`select p.oid::regprocedure::text as f, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub,
      has_function_privilege('master_edit', p.oid, 'execute') as me, has_function_privilege('watcher', p.oid, 'execute') as w
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'ops' and p.proname = 'sku_sales_daily'`);
  assert.deepEqual(fns.map((f) => f.f.replace(/,\s*/g, ',')), SALES_CHART_FUNCTIONS.map((f) => f.replace(/,\s*/g, ',')));
  for (const f of fns) { assert.equal(f.d, true); assert.match(f.c, /search_path=pg_catalog, pg_temp/); assert.equal(f.pub, false); assert.equal(f.me, true); assert.equal(f.w, false); }
  assert.ok(HEAVY_ENTRY_MANIFEST.some((e) => e.sig === SALES_CHART_FUNCTIONS[0] && e.cls === 'light' && e.public === false));
  const old = await all(`select count(*)::int as n from ops.sku_sales_daily($1::bigint, '1900-01-01')`, [ids.A]);
  assert.equal(old[0].n, 6, 'p_from が古すぎても 2025-01-01 から (2024-12-20 の注文は返さない)');
});

console.log('3 API (本物の router)');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { default: router, __setPgClientFactory, __setClock, SALES_CHART_LIMITS } = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/sc';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/sc';
process.env.MASTER_EDITORS = 'naka@test';
let chain = Promise.resolve();
const factory = async (url) => {
  let release;
  const prev = chain; chain = new Promise((r) => { release = r; });
  await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (q, p) => pg.query(q, p), end: async () => { await pg.query('reset role'); release(); }, on: () => {} };
};
__setPgClientFactory(factory);
__setClock(() => SALES_NOW);
const app = express();
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const BASE = `http://127.0.0.1:${server.address().port}/apps/master-edit`;
const get = async (path) => { const r = await fetch(BASE + path, { headers: { Accept: 'application/json' } }); return { status: r.status, j: await r.json() }; };

try {
  await t('単品: 週・月・日の数 (前日まで・メルカリはその他)・在庫の線 (最後の締め・欠品の日)・金額は返さない', async () => {
    const { status, j } = await get('/api/sku/SC-A/sales-chart');
    assert.equal(status, 200, JSON.stringify(j));
    assert.deepEqual([j.ok, j.kind, j.through, j.dataFrom, j.stockFrom], [true, 'single', '2026-10-17', '2025-01-01', '2026-10-13']);
    const w = j.views.week.buckets;
    assert.equal(w.length, 52);
    assert.deepEqual([w[51].from, w[51].partial, w[51].qty, w[51].total, w[51].viaSet], ['2026-10-12', true, { rakuten: 3, amazon: 8, yahoo: 0, other: 0 }, 11, 2], '今日 (10/18) の 7 個は入れない');
    assert.deepEqual(w[51].stock, { end: 3, endDay: '2026-10-16', days: 3, zeroDays: 1 }, '10/15 は取れなかった日');
    assert.deepEqual([w[50].qty.yahoo, w[50].total], [3, 3]);
    assert.deepEqual(j.views.month.totals, { rakuten: 3, amazon: 8, yahoo: 3, other: 4, total: 18, viaSet: 5 }, '2024-12 の 9 個は記録の前 (数えない)');
    assert.deepEqual([j.views.month.buckets[1].from, j.views.month.buckets[1].noData], ['2024-12-01', true]);
    assert.equal(j.views.day.buckets.length, 90);
    assert.deepEqual(j.malls.map((m) => m.label), ['楽天', 'Amazon', 'Yahoo!', 'その他']);
    assert.equal(/amount|jpy|price/i.test(JSON.stringify(j)), false, '金額を返さない');
  });
  await t('セット: そのセットとして売れた数・在庫の線なし', async () => {
    const { status, j } = await get('/api/sku/sc-s/sales-chart');
    assert.equal(status, 200, JSON.stringify(j));
    assert.deepEqual([j.kind, j.stockFrom, j.views.month.totals.total, j.views.month.totals.viaSet], ['set', null, 2, 1]);
    assert.equal(j.views.week.buckets.every((b) => b.stock === null), true);
  });
  await t('無いコード = 404・関数の実行権が無い (流し直しの前) = 503 と言葉・期限切れ = 503 (timeout)', async () => {
    assert.equal((await get('/api/sku/nope/sales-chart')).status, 404);
    for (const f of SALES_CHART_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
    try {
      const { status, j } = await get('/api/sku/sc-a/sales-chart');
      assert.deepEqual([status, j.reason], [503, 'no_privilege']);
      assert.match(j.error, /0086/);
    } finally { for (const f of SALES_CHART_FUNCTIONS) await pg.query(`grant execute on function ${f} to master_edit`); }
    const keep = SALES_CHART_LIMITS.timeMs;
    SALES_CHART_LIMITS.timeMs = 0;
    try {
      const { status, j } = await get('/api/sku/sc-a/sales-chart');
      assert.deepEqual([status, j.reason], [503, 'timeout']);
    } finally { SALES_CHART_LIMITS.timeMs = keep; }
    // 接続の初めの設定 (set statement_timeout ほか) が遅い = 期限で打ち切り、接続を閉じる (Codex R2 Medium)・閉じるのが遅くても待たない (R3 Medium)
    let ended = 0;
    __setPgClientFactory(async () => ({ query: (q) => (/^set /.test(q) ? new Promise((ok) => setTimeout(() => ok({ rows: [] }), 1500)) : Promise.resolve({ rows: [] })), end: () => { ended++; return new Promise((ok) => setTimeout(ok, 3000)); }, on: () => {} }));
    SALES_CHART_LIMITS.timeMs = 300;
    try {
      const t0 = Date.now();
      const { status, j } = await get('/api/sku/sc-a/sales-chart');
      const ms = Date.now() - t0;
      assert.deepEqual([status, j.reason, ended], [503, 'timeout', 1]);
      assert.ok(ms < 1200, `期限で止まる (${ms} ms)`);
    } finally { SALES_CHART_LIMITS.timeMs = keep; __setPgClientFactory(factory); }
    const bad = await C.readSalesChart({ query: async () => { throw Object.assign(new Error('canceling statement due to statement timeout'), { code: '57014' }); } }, { sku_id: '1', sku_kind: 'single' });
    assert.deepEqual([bad.ok, bad.reason], [false, 'timeout']);
  });
  // 🆕 10/19 中原さん「表示できなかったものはボタンを押したら表示されるように」= 画面の「もう一度読む」= ?wait=long (30 秒まで)
  await t('長く待つ読み (?wait=long): この値だけ 30 秒・ほかの値はふつうの 8 秒・時間切れの形・1 つの文も期限で止める・同時に 1 本まで (busy)', async () => {
    const keep = { ...SALES_CHART_LIMITS };
    // ふつうの期限を 0 にする = 長い読みだけ通る
    SALES_CHART_LIMITS.timeMs = 0;
    try {
      const short = await get('/api/sku/sc-a/sales-chart');
      assert.deepEqual([short.status, short.j.ok, short.j.reason], [503, false, 'timeout'], JSON.stringify(short.j));
      assert.match(short.j.error, /時間がかかりすぎました/);
      const long = await get('/api/sku/sc-a/sales-chart?wait=long');
      assert.equal(long.status, 200, JSON.stringify(long.j));
      assert.deepEqual([long.j.ok, long.j.kind, long.j.views.month.totals.total], [true, 'single', 18], 'ふつうと同じ中身');
      for (const q of ['?wait=LONG', '?wait=1', '?wait=30000', '?wait=long&wait=long', '?wait[]=long', '?wait=long%20', '?wait=']) {
        const r = await get('/api/sku/sc-a/sales-chart' + q);
        assert.deepEqual([q, r.status, r.j.reason], [q, 503, 'timeout'], 'ほかの値 = ふつうの期限 (長くしない)');
      }
    } finally { Object.assign(SALES_CHART_LIMITS, keep); }

    // statement_timeout = min(30 秒, 残りの時間) (ふつうの 20 秒の上限より長く・期限は越えない) / ふつうは 8 秒まで。文が期限で止まった (57014) = 503 timeout (long: true)・接続を閉じる
    const sets = [];
    let ended = 0, mode = 'ok', gate = null;
    const fake = async () => ({
      query: async (q) => {
        const m = /^set statement_timeout = '(\d+)(ms|s)'/.exec(q);
        if (m) { sets.push(Number(m[1]) * (m[2] === 's' ? 1000 : 1)); return { rows: [] }; }
        if (/^set /.test(q)) return { rows: [] };
        if (/core\.skus/.test(q)) return { rows: [{ sku_id: '1', code: 'SC-A', code_norm: 'SC-A', sku_kind: 'single' }] };
        if (gate) await gate.wait;
        if (mode === 'cancel') throw Object.assign(new Error('canceling statement due to statement timeout'), { code: '57014' });
        if (mode === 'locked') throw Object.assign(new Error('lock not available'), { code: '55P03' });
        return { rows: [] };
      },
      end: async () => { ended++; }, on: () => {},
    });
    let opened = 0;
    __setPgClientFactory(async (...a) => { opened++; return fake(...a); });
    try {
      let r = await get('/api/sku/sc-a/sales-chart?wait=long');
      assert.equal(r.status, 200, JSON.stringify(r.j));
      const longSets = sets.splice(0).filter((ms) => ms !== 20000);   // 20000 = 接続の初めの設定 (20s)
      assert.ok(longSets.length >= 1 && longSets.every((ms) => ms > 20000 && ms <= 30000), `長い読みの文の上限: ${longSets}`);
      r = await get('/api/sku/sc-a/sales-chart');
      assert.equal(r.status, 200);
      const shortSets = sets.splice(0).filter((ms) => ms !== 20000);
      assert.ok(shortSets.length >= 1 && shortSets.every((ms) => ms <= 8000), `ふつうの文の上限: ${shortSets}`);

      mode = 'cancel'; ended = 0;
      r = await get('/api/sku/sc-a/sales-chart?wait=long');
      assert.equal(r.status, 503);
      assert.deepEqual(Object.keys(r.j).sort(), ['error', 'ok', 'reason'], '時間切れの形 = ほかの読めないときと同じ (画面は reason を見る)');
      assert.deepEqual([r.j.ok, r.j.reason, ended], [false, 'timeout', 1], '期限で止まった = 時間切れ・接続を閉じた');
      mode = 'ok';

      // 同時に 1 本まで: 1 本目を止めている間の 2 本目 = 503 busy (接続を開かない)・ふつうの読みは通る / 1 本目が終わったら (失敗でも) また読める
      let open; gate = { wait: new Promise((ok) => { open = ok; }) };
      const first = get('/api/sku/sc-a/sales-chart?wait=long');
      while (opened < 4) await new Promise((ok) => setTimeout(ok, 10));   // 1 本目が DB に入るまで (ここまでの 3 回 + 1)
      const before = opened;
      const busy = await get('/api/sku/sc-a/sales-chart?wait=long');
      assert.deepEqual([busy.status, busy.j.ok, busy.j.reason, opened], [503, false, 'busy', before], JSON.stringify(busy.j));
      assert.match(busy.j.error, /少し待ってから/);
      gate = null; open();
      assert.equal((await first).status, 200);
      mode = 'cancel';
      assert.equal((await get('/api/sku/sc-a/sales-chart?wait=long')).j.reason, 'timeout');
      mode = 'ok';
      assert.equal((await get('/api/sku/sc-a/sales-chart?wait=long')).status, 200, '失敗の後も数が戻る (busy にならない)');

      // DB との道が黙って止まる (statement_timeout も効かない) = 期限 + answerGraceMs で時間切れと答える (読み込み中のままにしない)。
      //   枠は接続を閉じ終わるまで返さない = その間の長い読みは busy (止まった接続を積み上げない・Codex R2 High)。後から来た答え (ここでは 55P03 の誤り) は捨てる (2 度送って落ちない・Codex R2 Medium)
      Object.assign(SALES_CHART_LIMITS, { longMs: 100, answerGraceMs: 150 });
      let open2; gate = { wait: new Promise((ok) => { open2 = ok; }) };
      const openedBefore = opened, endedBefore = ended;
      const t0 = Date.now();
      const stuck = await get('/api/sku/sc-a/sales-chart?wait=long');
      assert.deepEqual([stuck.status, stuck.j.reason], [503, 'timeout'], JSON.stringify(stuck.j));
      assert.ok(Date.now() - t0 < 2000, `止まった読みも期限 + 少しで答える (${Date.now() - t0} ms)`);
      const again = await get('/api/sku/sc-a/sales-chart?wait=long');
      assert.deepEqual([again.status, again.j.reason, opened], [503, 'busy', openedBefore + 1], '止まった接続が残る間は次の長い読みを重ねない (接続を開かない)');
      mode = 'locked'; gate = null; open2();
      while (ended < endedBefore + 1) await new Promise((ok) => setTimeout(ok, 10));   // 止めていた読みが後から誤りで終わって接続を閉じる (答えは送らない)
      mode = 'ok';
      assert.equal((await get('/api/sku/sc-a/sales-chart?wait=long')).status, 200, '接続を閉じたら枠が戻る');
    } finally { __setPgClientFactory(factory); gate = null; Object.assign(SALES_CHART_LIMITS, keep); }
  });
} finally {
  server.close();
}

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
