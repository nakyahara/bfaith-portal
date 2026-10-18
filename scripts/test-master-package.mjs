/**
 * test-master-package.mjs — 梱包した状態の 縦・横・高さ・重さ (中原さん 2026-10-17・Company DB の 0086・lib/master-package.mjs)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 全部の migration を流した新しい DB・切替の後)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   U 値の形: cm (小数 1 けた) → mm・g (整数)・範囲・全角・空 = null・登録の値の形 (知らない項目 = 400)・出どころの言葉・順位 (JS = DB の core.package_source_rank)
 *   D 0086 の形: 表・関数 (security definer・search_path・public の実行権なし)・守り (関数の外では書けない・記録は追記だけ・TRUNCATE できない)・
 *     画面のロール master_edit = 人の値・記録・ビュー・出どころ (core.product_physicals) の select と書く関数の実行だけ
 *   M まとめた値 (core.v_product_packages): 軸ごとに 人 → 実測 → Amazon のカタログ → … → 定形外の見込み・同じ順位は新しい観測・出どころ × 参照ごとの最新の行だけ・先の時刻の行は使わない
 *   S 商品の画面: 「大きさ・重さ」の節 (欄・いま使う値・出どころの表・食い違いの印)・保存 (入れる・変える・軸を外す・全部外す・記録 via sku_page)・押し直し・
 *     ほかの人が変えた後 = 409 (編集の印・DB の関数も最後の記録と照らす)・門の外の直の呼び出し = 断る・セット = not_single・形の誤り = 400
 *   R 新商品の登録 (単品) / まとまり (全部の子に同じ値): 入れてよい・入れなくてよい (「値か入力なし」に数えない)・セットは断る・押し直し = 前の答え・形の誤り = 何も作らない
 *   L 一覧 (大きさ・重さの列 = いつもは出さない・並べられない) と CSV (最後の 5 列)
 *   Y 夜間ロード: 出どころの行を足しても人の値は変わらない (別の表)・まとめた値は人のまま・夜間ロードのコードは人の表を知らない
 *   B ロールの流し直しの前: 欄を出さない (見るだけ)・値を送ったら 503 (登録も・何も作らない)・ほかの欄の保存はそのまま
 * 使い方: node scripts/test-master-package.mjs  (画面の動き = scripts/test-master-package-ui.mjs)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pkg-'));

const { setupVariationDb, basePlan } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const PK = await import('../lib/master-package.mjs');
const RD = await import('../apps/master-edit/read.mjs');
const CSV = await import('../apps/master-edit/list-csv.mjs');
const COLS = await import('../apps/master-edit/list-columns.mjs');
const ROLES = await import('./company-db/create-master-edit-roles.mjs');
const T = await setupVariationDb();
const { pg, db, q, one, asEditor } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/pk';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/pk';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const MR = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
MR.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
const RATES = new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]);
MR.__setShippingRatesProvider(async () => RATES);
MR.__setPhDraftLookup(async () => null);
const app = express();
app.set('view engine', 'ejs');
const SESS = { editor: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] },
  viewer: { authenticated: true, email: 'see@test', displayName: '見る人', role: 'user', allowedApps: ['master-edit'] } };
app.use((req, res, next) => { req.session = SESS[req.headers['x-test-session'] || 'editor']; next(); });
app.use('/apps/master-edit', MR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url, { body, session = 'editor', origin = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(ORIGIN + '/apps/master-edit' + url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(60e3) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
const uuid = () => crypto.randomUUID();
const VALUES = { standard_price: '2980', cost: { jpy: '1200' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' };
/** 単品の登録の本文 (pkg = 梱包の大きさ・重さ / undefined = 送らない) */
function single(code, pkg, { kind = 'single' } = {}) {
  const b = { request_id: uuid(), kind, code, values: { ...VALUES, name: `試し ${code}` }, card: { create: false },
    ...(kind === 'single' ? { logizard_barcode: { kind: 'lz_assign' } } : {}), ...(pkg !== undefined ? { package: pkg } : {}) };
  if (kind === 'set') { delete b.values.tax_rate; delete b.values.sales_class; delete b.values.primary_supplier; delete b.values.cost; delete b.values.expiry_managed; delete b.values.inbound_date_managed; b.values.components = [{ code: 's001', qty: '2' }]; }
  b.blanks = RB.missingBlankKeys(kind, b, { orderApplicable: true });
  return b;
}
function variation(group, pkg) {
  const b = { request_id: uuid(), group: { mode: 'new', code: group, name: `試し ${group}` }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-A', name: '色A' }, { axis: 1, code: '-B', name: '色B' }],
    children: ['-A', '-B'].map((c) => ({ code: group + c, choices: { 1: c }, name: `試し ${group}【色${c}】`, jan: '', lz_barcode: { kind: 'lz_assign' } })),
    values: { ...VALUES }, card: { official_url: '', amazon_url: '', asin: '' }, ...(pkg !== undefined ? { package: pkg } : {}) };
  b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
  return b;
}
const humanOf = async (code) => (await q(`select x.length_mm, x.width_mm, x.height_mm, x.weight_g, x.set_via from core.skus k join core.product_packages x on x.product_id = k.product_id where k.code = $1`, [code]))[0] || null;
const eventsOf = async (code) => (await q(`select e.from_value as f, e.to_value as t, e.via, e.actor_id, e.reason from ops.product_package_events e join core.skus k on k.sku_id = e.sku_id
  where k.code = $1 order by e.event_id`, [code]));
const viewOf = async (code) => (await q(`select v.* from core.v_product_packages v join core.skus k on k.product_id = v.product_id where k.code = $1`, [code]))[0] || null;
const pidOf = async (code) => (await one('select product_id::text as id from core.skus where code = $1', [code])).id;
const skuIdOf = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;
const tok = (t) => (/data-token="([0-9a-f]{64})"/.exec(t) || [])[1];
const DAY = 86400e3;
const ago = (d) => new Date(Date.now() - d * DAY).toISOString();
/** 出どころの行 (夜間ロードと同じ表に、持ち主のロールで入れる) */
async function phys(code, { src, ref, l = null, w = null, h = null, g = null, at }) {
  await pg.query(`insert into core.product_physicals (company_id, product_id, scope, length_mm, width_mm, height_mm, weight_g, source_system, source_ref, is_measured, observed_at, created_by_type, created_by_id)
    values (1, $1, 'package', $2, $3, $4, $5, $6, $7, $8, $9, 'system', 'test')`, [await pidOf(code), l, w, h, g, src, ref, src === 'measured', at]);
}

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('梱包の大きさ・重さ (0086)');

await ta('[U1] 値の形: cm (小数 1 けた) → mm・g (整数)・全角・単位の字・空 = null・範囲・形の誤りの一言と欄', async () => {
  assert.equal(PK.cmIn('23.5'), 235); assert.equal(PK.cmIn('２３．５'), 235); assert.equal(PK.cmIn(' 12 '), 120); assert.equal(PK.cmIn('3cm'), 30); assert.equal(PK.cmIn(0.1), 1);
  assert.equal(PK.cmIn(''), null); assert.equal(PK.cmIn(null), null); assert.equal(PK.cmIn(undefined), null);
  for (const bad of ['23.55', '-1', '1e2', 'abc', '0', '0.0', '300.1', '1000']) assert.throws(() => PK.cmIn(bad, '梱包の縦', 'package'), (e) => e.status === 400 && e.extra.field === 'package', bad);
  assert.equal(PK.cmIn('300'), 3000);
  assert.equal(PK.gramIn('250'), 250); assert.equal(PK.gramIn('1,200'), 1200); assert.equal(PK.gramIn('２５０ｇ'.replace('ｇ', 'g')), 250); assert.equal(PK.gramIn(''), null);
  for (const bad of ['0', '12.5', '100001', 'x']) assert.throws(() => PK.gramIn(bad, '梱包の重さ', 'pkg_weight'), (e) => e.status === 400 && e.extra.field === 'pkg_weight', bad);
  assert.equal(PK.packageFieldIn('pkg_height', '3'), 30);
  assert.throws(() => PK.packageFieldIn('pkg_length', '1.25'), (e) => e.extra.field === 'package');
  assert.throws(() => PK.packageFieldIn('pkg_weight', '1.5'), (e) => e.extra.field === 'pkg_weight');
  assert.deepEqual(PK.packageIn({ length: '23.5', width: '12', height: '3', weight: '250' }), { length_mm: 235, width_mm: 120, height_mm: 30, weight_g: 250 });
  assert.deepEqual(PK.packageIn({ weight: '80' }), { length_mm: null, width_mm: null, height_mm: null, weight_g: 80 });
  assert.equal(PK.packageIn({ length: '', width: '', height: '', weight: '' }), null, '全部空 = 入れない');
  assert.equal(PK.packageIn(undefined), null);
  for (const bad of [[], 'x', { depth: '1' }]) assert.throws(() => PK.packageIn(bad), (e) => e.status === 400, JSON.stringify(bad));
  assert.equal(PK.cmText(235), '23.5'); assert.equal(PK.cmText(120), '12'); assert.equal(PK.cmText(null), '');
  assert.equal(PK.packageText({ length_mm: 235, width_mm: 120, height_mm: null, weight_g: 1200 }), '23.5×12×? cm · 1,200 g');
  assert.equal(PK.sourceWords('measured', 'pm_skus'), '定形外 (実測)');
  assert.equal(PK.sourceWords('measured', 'fbx_weight_current:X00A'), 'FBA 納品 (実測)');
  assert.equal(PK.sourceWords('amazon_catalog', 'fbx_weight_refs:X00A'), 'FBA 納品 (Amazon のカタログ)');
  assert.equal(PK.sourceWords('human', null), '人');
});

await ta('[U2] 出どころの順位: JS (SOURCE_RANK) と DB (core.package_source_rank) が同じ答え・人がいちばん先・知らない出どころ = 5', async () => {
  for (const s of ['human', 'measured', 'amazon_catalog', 'yahoo', 'supplier', 'postage_estimate', 'manual', 'nope']) {
    assert.equal(Number((await one('select core.package_source_rank($1) as r', [s])).r), PK.sourceRank(s), s);
  }
  assert.equal(PK.sourceRank('human'), 0);
  assert.ok(PK.sourceRank('measured') < PK.sourceRank('amazon_catalog') && PK.sourceRank('amazon_catalog') < PK.sourceRank('postage_estimate'));
});

await ta('[D1] 0086 の形: 表・関数 (security definer・search_path 固定・public の実行権なし)・画面のロールの権限 (読む・関数だけ)・ほかのロールは読めない', async () => {
  for (const t of ['core.product_packages', 'ops.product_package_events', 'core.v_product_packages']) assert.ok((await one('select to_regclass($1) is not null as ok', [t])).ok, t);
  const f = await one(`select p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p
    join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname = 'set_product_package'`);
  assert.deepEqual([f.d, f.pub], [true, false]);
  assert.match(f.c, /search_path=pg_catalog, pg_temp/);
  assert.equal((await one("select has_function_privilege('public', 'ops.guard_package_tables()'::regprocedure, 'execute') as ok")).ok, false);
  for (const t of [...ROLES.PACKAGE_SELECT, ...ROLES.PACKAGE_SOURCE_SELECT]) {
    const p = await one(`select has_table_privilege('master_edit', $1, 'select') as s, has_table_privilege('master_edit', $1, 'insert') as i, has_table_privilege('master_edit', $1, 'update') as u,
      has_table_privilege('master_edit', $1, 'delete') as d, has_table_privilege('master_edit', $1, 'truncate') as tr`, [t]);
    assert.deepEqual(p, { s: true, i: false, u: false, d: false, tr: false }, t);
  }
  for (const t of ROLES.PACKAGE_SELECT) for (const r of ['master_ops', 'master_observer', 'master_gate']) assert.equal((await one(`select has_table_privilege($1, $2, 'select') as s`, [r, t])).s, false, `${r} が ${t} を読める`);
  for (const fn of ROLES.PACKAGE_EDIT_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [fn])).ok, true, fn);
  for (const fn of ROLES.PACKAGE_OWNER_ONLY_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [fn])).ok, false, fn);
  assert.equal(PK.PACKAGE_FUNCTION, ROLES.PACKAGE_EDIT_FUNCTIONS[0]);
  assert.equal(await PK.packageReady(db), true);
  assert.equal(await asEditor(() => PK.packageReady(db)), true);
});

await ta('[M1] まとめた値: 軸ごとに 人 → 実測 → Amazon → 定形外の見込み・同じ順位は新しい観測・出どころ × 参照の最新の行だけ・先の時刻の行は使わない', async () => {
  // s002: 定形外 (実測 250 g・厚み 30 mm・3 日前) / FBA 納品 (実測 260 g・1 日前) / FBA 納品の Amazon (300 g・今) / 先の時刻の実測 (999 g) / 定形外の古い行 (240 g・10 日前)
  await phys('s002', { src: 'measured', ref: 'pm_skus', g: 240, h: 28, at: ago(10) });
  await phys('s002', { src: 'measured', ref: 'pm_skus', g: 250, h: 30, at: ago(3) });
  await phys('s002', { src: 'measured', ref: 'fbx_weight_current:X00S002', g: 260, at: ago(1) });
  await phys('s002', { src: 'amazon_catalog', ref: 'fbx_weight_refs:X00S002', g: 300, at: ago(0.01) });
  await phys('s002', { src: 'measured', ref: 'fbx_weight_current:X00FUT', g: 999, at: new Date(Date.now() + 2 * DAY).toISOString() });
  let v = await viewOf('s002');
  assert.deepEqual([v.weight_g, v.weight_source, v.weight_ref, v.height_mm, v.height_source, v.height_ref, v.length_mm, v.width_mm], [260, 'measured', 'fbx_weight_current:X00S002', 30, 'measured', 'pm_skus', null, null]);
  // 定形外の見込みだけの商品 = 見込みを使う (ほかに無い)
  await phys('s003', { src: 'postage_estimate', ref: 'pm_skus', g: 90, at: ago(2) });
  v = await viewOf('s003');
  assert.deepEqual([v.weight_g, v.weight_source], [90, 'postage_estimate']);
  // 商品の画面の材料: 出どころ (順位 → 新しい順)・いま使う値・食い違い (重さ 250 / 260 / 300)
  const pv = await asEditor(async () => PK.readPackageView(db, await pidOf('s002')));
  assert.deepEqual(pv.sources.map((s) => [s.source, s.ref, s.weight_g]), [['measured', 'fbx_weight_current:X00S002', 260], ['measured', 'pm_skus', 250], ['amazon_catalog', 'fbx_weight_refs:X00S002', 300]]);
  assert.deepEqual(pv.resolved.weight_g && [pv.resolved.weight_g.value, pv.resolved.weight_g.words], [260, 'FBA 納品 (実測)']);
  assert.deepEqual(pv.resolved.height_mm && [pv.resolved.height_mm.value, pv.resolved.height_mm.words], [30, '定形外 (実測)']);
  assert.deepEqual(pv.conflicts, { weight_g: [260, 250, 300] });
  assert.equal(pv.human, null);
  // 🆕 Codex R1 M: 商品の絞り込みが元の表 (出どころ・人) の読み取りまで届く (全部の商品の履歴を読んで並べない)
  const plan = (await q('explain select * from core.v_product_packages where product_id = $1', [await pidOf('s002')])).map((r) => r['QUERY PLAN']).join('\n');
  for (const t of ['product_physicals', 'product_packages']) {
    const i = plan.search(new RegExp(`on ${t}\\b`));
    assert.ok(i >= 0, `${t} を読む行が無い:\n${plan}`);
    const block = plan.slice(i).split('\n').slice(0, 3).join('\n');
    assert.match(block, /product_id = /, `${t} の読み取りに商品の条件が届いていない:\n${plan}`);
  }
  assert.ok(!/CTE Scan/.test(plan), `CTE を実体化している:\n${plan}`);
});

let PAGE_TOKEN_BEFORE = null;
await ta('[S1] 商品の画面: 節 (欄・いま使う値・出どころの表・食い違い)・保存 (入れる → 軸を変える・外す → 全部外す・記録 via sku_page)・押し直し・変わらない保存は書かない', async () => {
  let page = await call('GET', '/sku/s002');
  assert.equal(page.status, 200);
  PAGE_TOKEN_BEFORE = tok(page.text);
  assert.match(page.text, /<section class="panel" id="p-pkg"/);
  const iLogi = page.text.indexOf('id="p-logi"'), iPkg = page.text.indexOf('id="p-pkg"'), iTax = page.text.indexOf('id="h-tax"');
  assert.ok(iLogi > 0 && iPkg > iLogi && iTax > iPkg, '「大きさ・重さ」は物流の下・税の上');
  for (const f of ['pkg_length', 'pkg_width', 'pkg_height', 'pkg_weight']) assert.match(page.text, new RegExp(`id="f-${f}"[^>]*data-field="${f}" data-dirty-field="${f}"`), f);
  assert.match(page.text, /id="f-pkg_weight"[^>]*placeholder="260"/, '空の欄の薄い字 = いま使う出どころの値');
  assert.match(page.text, /id="f-pkg_height"[^>]*placeholder="3"/);
  assert.match(page.text, /id="pkg-conflict"[^>]*>[\s\S]*?食い違い: 重さ</);
  assert.match(page.text, /いま使う値: 260 g \(FBA 納品 \(実測\)\)/);
  assert.match(page.text, /<th scope="row">定形外 \(実測\)<\/th>/);
  assert.match(page.text, /data-pkg-use="0"/);
  assert.match(page.text, /public\/me-package\.js/);
  const rid = uuid();
  const body = { request_id: rid, seen: { token: PAGE_TOKEN_BEFORE }, values: { pkg_length: '23.5', pkg_width: '12', pkg_height: '3', pkg_weight: '255' } };
  const a = await call('POST', '/api/sku/s002', { body });
  assert.equal(a.status, 200, a.text);
  assert.deepEqual(a.j.changed.map((c) => [c.field, c.from, c.to, c.ne]), [['pkg_length', null, 23.5, 'none'], ['pkg_width', null, 12, 'none'], ['pkg_height', null, 3, 'none'], ['pkg_weight', null, 255, 'none']]);
  assert.deepEqual(a.j.ne_steps, []);
  assert.deepEqual(await humanOf('s002'), { length_mm: 235, width_mm: 120, height_mm: 30, weight_g: 255, set_via: 'sku_page' });
  const v = await viewOf('s002');
  assert.deepEqual([v.length_mm, v.length_source, v.weight_g, v.weight_source, v.height_source], [235, 'human', 255, 'human', 'human'], '人の値がいちばん先');
  const again = await call('POST', '/api/sku/s002', { body });
  assert.deepEqual([again.status, again.j.replayed], [200, true]);
  assert.equal((await eventsOf('s002')).length, 1, '押し直しで 2 回書いた');
  // 軸を変える・外す (外した軸 = 出どころの値に戻る)
  page = await call('GET', '/sku/s002');
  assert.match(page.text, /id="f-pkg_length"[^>]*value="23.5"/);
  assert.match(page.text, /<tr data-src="human"><th scope="row">人/);
  const b = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { pkg_length: '23.5', pkg_width: '12', pkg_height: '3', pkg_weight: '' } } });
  assert.equal(b.status, 200, b.text);
  assert.deepEqual(b.j.changed.map((c) => [c.field, c.from, c.to]), [['pkg_weight', 255, null]]);
  assert.equal((await viewOf('s002')).weight_source, 'measured', '外した軸は出どころの値');
  // 変わらない保存 = 書かない
  page = await call('GET', '/sku/s002');
  const n = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { pkg_length: '23.5', pkg_width: '12.0', pkg_height: '3', pkg_weight: '' } } });
  assert.equal(n.j.no_change, true);
  // 全部外す = 行を消す (記録は残る)
  const c = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { pkg_length: '', pkg_width: '', pkg_height: '', pkg_weight: '' } } });
  assert.equal(c.status, 200, c.text);
  assert.equal(await humanOf('s002'), null);
  const ev = await eventsOf('s002');
  assert.deepEqual(ev.map((e) => [e.f && e.f.weight_g, e.t && e.t.length_mm, e.via, e.actor_id]), [[null, 235, 'sku_page', 'naka@test'], [255, 235, 'sku_page', 'naka@test'], [null, null, 'sku_page', 'naka@test']]);
  assert.equal(ev[2].t, null);
});

await ta('[S2] ほかの人が変えた後 = 409 (編集の印)・記録の無い商品の印は前と同じ・DB の関数も最後の記録と照らす・門の外の直の呼び出し = 断る・セット = not_single・形の誤り = 400', async () => {
  const W = await import('../lib/master-write.mjs');
  const today = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
  const id1 = await skuIdOf('s001');
  const cur0 = await asEditor(() => W.readCurrent(db, id1, today));
  assert.deepEqual(cur0.pkg, { human: null, event_id: null });
  assert.equal(W.editTokenOf({ ...cur0, pkg: undefined }), W.editTokenOf(cur0), '記録の無い商品の印が変わった');
  let page = await call('GET', '/sku/s001');
  const old = tok(page.text);
  const a = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: old }, values: { pkg_weight: '100' } } });
  assert.equal(a.status, 200, a.text);
  const b = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: old }, values: { pkg_weight: '120' } } });
  assert.deepEqual([b.status, b.j.reason], [409, 'version_conflict']);
  assert.equal((await humanOf('s001')).weight_g, 100);
  // 直に呼ぶ = 門を通った保存・登録の取引の外 = 断る
  await asEditor(async () => {
    for (const via of ['sku_page', 'register', 'variation']) {
      await pg.query('begin');
      try {
        await assert.rejects(pg.query('select ops.set_product_package($1::uuid, $2, $3::bigint, null, null, null, 50, $4::bigint, $5, null)',
          [uuid(), 'naka@test', id1, via === 'sku_page' ? (await one('select max(event_id)::text as e from ops.product_package_events')).e : null, via]), /master_write_session_required/, via);
      } finally { await pg.query('rollback'); }
    }
  });
  // 保存の約束の中でも、見ていた記録が今と違えば断る・ほかの SKU の約束では書けない・範囲の外は断る
  await asEditor(async () => {
    const cur = await W.readCurrent(db, id1, today);
    const RID = uuid();
    const begin = (sid) => pg.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)',
      [RID, 'naka@test', null, T.OWN, 'sku_edit', sid, W.editTokenOf(cur), 'a'.repeat(64), JSON.stringify(W.editVersionsOf(cur))]);
    const fn = (sid, seen, g = 50, { rid = RID, actor = 'naka@test' } = {}) => pg.query("select ops.set_product_package($1::uuid, $5, $2::bigint, null, null, null, $3, $4::bigint, 'sku_page', null)", [rid, sid, g, seen, actor]);
    await pg.query('begin');
    try { await begin(id1); await assert.rejects(fn(id1, null), /version_conflict/); } finally { await pg.query('rollback'); }
    await pg.query('begin');
    try { await begin(id1); await assert.rejects(fn(id1, cur.pkg.event_id, 0), /invalid_input/); } finally { await pg.query('rollback'); }
    await pg.query('begin');
    try { await begin(id1); await assert.rejects(fn(await skuIdOf('s003'), null), /master_write_session_required/); } finally { await pg.query('rollback'); }
    // 🆕 Codex R1 M: 約束と違う request_id・人の記録は書かない (保存の記録と大きさ・重さの記録がずれない)
    await pg.query('begin');
    try { await begin(id1); await assert.rejects(fn(id1, cur.pkg.event_id, 50, { rid: uuid() }), /master_write_session_required/, 'ほかの request_id'); } finally { await pg.query('rollback'); }
    await pg.query('begin');
    try { await begin(id1); await assert.rejects(fn(id1, cur.pkg.event_id, 50, { actor: 'other@test' }), /master_write_session_required/, 'ほかの人'); } finally { await pg.query('rollback'); }
    await pg.query('begin');
    try { await begin(id1); const r = (await fn(id1, cur.pkg.event_id, 50)).rows[0]; assert.equal(Object.values(r)[0].changed, true, '約束どおりの request_id・人 = 書ける'); } finally { await pg.query('rollback'); }
  });
  // 新商品の登録の約束も request_id・人と結ぶ (まとまりの子 = 子の登録の番号): 登録だけ別の番号で呼ぶと断る
  {
    const R = await import('../lib/master-register.mjs');
    const b = single('pk-s2x');
    await asEditor(async () => {
      // registerNewSku は自分で begin / commit する = 登録の関数の後・同じ取引の中で直に呼ぶ形を beforeCommit で見る
      let seen = null;
      const r = await R.registerNewSku(db, { actor: 'naka@test', requestId: b.request_id, kind: 'single', code: b.code, reason: null, values: b.values, card: b.card, logizardBarcode: b.logizard_barcode },
        { open: true, shippingRates: RATES, beforeCommit: async () => {
          const sid = (await one('select sku_id::text as id from core.skus where code = $1', [b.code])).id;
          for (const [rid, actor] of [[uuid(), 'naka@test'], [b.request_id, 'other@test']]) {
            await pg.query('savepoint s');
            try { await assert.rejects(pg.query("select ops.set_product_package($1::uuid, $2, $3::bigint, null, null, null, 50, null, 'register', null)", [rid, actor, sid]), /master_write_session_required/); }
            finally { await pg.query('rollback to savepoint s'); }
          }
          seen = (await pg.query("select ops.set_product_package($1::uuid, 'naka@test', $2::bigint, null, null, null, 50, null, 'register', null) as r", [b.request_id, sid])).rows[0].r;
        } });
      assert.equal(r.code, b.code);
      assert.equal(seen && seen.changed, true, '登録の約束どおり = 書ける');
    });
    assert.equal((await humanOf('pk-s2x')).weight_g, 50);
  }
  // 形の誤り = 400 (欄つき)・何も書かない
  page = await call('GET', '/sku/s001');
  for (const [values, field] of [[{ pkg_length: '1.25' }, 'package'], [{ pkg_height: '0' }, 'package'], [{ pkg_weight: '1.5' }, 'pkg_weight'], [{ pkg_weight: '100001' }, 'pkg_weight']]) {
    const r = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values } });
    assert.deepEqual([r.status, r.j.reason, r.j.field], [400, 'invalid_input', field], JSON.stringify(values));
  }
  assert.equal((await humanOf('s001')).weight_g, 100);
});

await ta('[D2] 守り (記録がある後): 持ち主でも関数の外では書けない・記録は追記だけ (印があっても)・TRUNCATE できない', async () => {
  const pid = await pidOf('s003');
  await assert.rejects(pg.query("insert into core.product_packages (product_id, weight_g, set_by, set_via, request_id) values ($1, 10, 'x@test', 'sku_page', $2)", [pid, uuid()]), /ops.set_product_package の中でだけ/);
  await assert.rejects(pg.query('truncate core.product_packages'), /./);
  await assert.rejects(pg.query('truncate ops.product_package_events'), /./);
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.package_protocol', '1', true)");
    await assert.rejects(pg.query('update ops.product_package_events set reason = null'), /追記だけ/);
  } finally { await pg.query('rollback'); }
  // 画面のロールは表に直接書けない
  await asEditor(() => assert.rejects(pg.query("insert into core.product_packages (product_id, weight_g, set_by, set_via, request_id) values ($1, 10, 'x@test', 'sku_page', $2)", [pid, uuid()]), /permission denied/));
});

await ta('[R1] 新商品の登録 (単品): 入れてよい (同じ取引・記録 via register)・入れなくてよい (「値か入力なし」に数えない)・押し直し = 前の答え・形の誤り / セット = 400 (何も作らない)', async () => {
  for (const kind of ['single', 'variation', 'set']) assert.ok(!RB.applicableBlankKeys(kind, {}, { orderApplicable: true }).some((k) => /pkg|package/.test(k)), `${kind}: 大きさ・重さを「値か入力なし」に数えた`);
  const b = single('pk-r1', { length: '20', width: '15.5', height: '2.5', weight: '180' });
  const r = await call('POST', '/api/new', { body: b });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual(await humanOf('pk-r1'), { length_mm: 200, width_mm: 155, height_mm: 25, weight_g: 180, set_via: 'register' });
  const ev = await eventsOf('pk-r1');
  assert.deepEqual(ev.map((e) => [e.f, e.via, e.actor_id]), [[null, 'register', 'naka@test']]);
  const again = await call('POST', '/api/new', { body: b });
  assert.deepEqual([again.status, again.j.replayed], [200, true], again.text);
  assert.equal((await eventsOf('pk-r1')).length, 1);
  // 同じ番号で大きさ・重さだけ違う = 違う中身
  const diff = await call('POST', '/api/new', { body: { ...b, package: { ...b.package, weight: '181' } } });
  assert.deepEqual([diff.status, diff.j.reason], [409, 'request_id_reused']);
  // 入れない = 人の値なし (古い画面と同じ)・全部空 = 入れない
  const r2 = await call('POST', '/api/new', { body: single('pk-r2') });
  assert.equal(r2.status, 200, r2.text);
  assert.equal(await humanOf('pk-r2'), null);
  const r3 = await call('POST', '/api/new', { body: single('pk-r3', { length: '', width: '', height: '', weight: '' }) });
  assert.equal(r3.status, 200, r3.text);
  assert.equal(await humanOf('pk-r3'), null);
  // 形の誤り・セット = 400・商品は作らない
  const n1 = await call('POST', '/api/new', { body: single('pk-r4', { length: '1.25' }) });
  assert.deepEqual([n1.status, n1.j.field], [400, 'package']);
  const n2 = await call('POST', '/api/new', { body: single('pk-r5', { weight: 'x' }) });
  assert.deepEqual([n2.status, n2.j.field], [400, 'package']);
  const n3 = await call('POST', '/api/new', { body: single('pkset1', { weight: '100' }, { kind: 'set' }) });
  assert.deepEqual([n3.status, n3.j.field], [400, 'package']);
  assert.equal((await one("select count(*)::int as n from core.skus where code in ('pk-r4', 'pk-r5', 'pkset1')")).n, 0, '断った登録で商品ができた');
  // 新商品の画面の欄 (単品 = 発送の節・入れなくても可 / セット = 無い)
  const page = await call('GET', '/new?kind=single');
  const iShip = page.text.indexOf('id="sec-ship"'), iPk = page.text.indexOf('id="np-length"'), iCard = page.text.indexOf('id="sec-card"');
  assert.ok(iShip > 0 && iPk > iShip && iCard > iPk, '新商品の画面の大きさ・重さの欄が発送の節に無い');
  assert.match(page.text, /梱包の重さ<span class="opt">入れなくても可<\/span>/);
  assert.ok(!/data-none="package/.test(page.text), '「入力なし」の札が付いた');
  const setPage = await call('GET', '/new?kind=set');
  assert.ok(!/id="np-length"/.test(setPage.text), 'セットに大きさ・重さの欄がある');
});

await ta('[V1] まとまりの登録: 全部の子に同じ値 (記録は子ごと・via variation)・入れなくてよい・形の誤り = 何も作らない', async () => {
  const r = await call('POST', '/api/new/variation', { body: variation('pkv1', { length: '30', width: '20', height: '5', weight: '400' }) });
  assert.equal(r.status, 200, r.text);
  for (const c of ['pkv1-A', 'pkv1-B']) {
    assert.deepEqual(await humanOf(c), { length_mm: 300, width_mm: 200, height_mm: 50, weight_g: 400, set_via: 'variation' }, c);
    assert.deepEqual((await eventsOf(c)).map((e) => [e.f, e.via]), [[null, 'variation']], c);
  }
  const r2 = await call('POST', '/api/new/variation', { body: variation('pkv2') });
  assert.equal(r2.status, 200, r2.text);
  assert.equal(await humanOf('pkv2-A'), null);
  const r3 = await call('POST', '/api/new/variation', { body: variation('pkv3', { height: '-1' }) });
  assert.deepEqual([r3.status, r3.j.field], [400, 'package']);
  assert.equal((await one("select count(*)::int as n from core.skus where code like 'pkv3-%'")).n, 0);
  const vp = await call('GET', '/new?kind=variation');
  assert.match(vp.text, /id="np-length"/);
  assert.match(vp.text, /全部の子に同じ値 \(サイズで違うときは空のまま登録して/);
});

await ta('[L1] 一覧の「大きさ・重さ」の列 (いつもは出さない・並べられない・最後に足す)・CSV の最後の 5 列 (まとめた値と出どころ)', async () => {
  assert.equal(COLS.COLUMN_IDS.at(-1), 'pkg', '前の列の並びを変えない (最後に足す)');
  assert.ok(!COLS.DEFAULT_VIEW.shown.includes('pkg'));
  assert.ok(!COLS.SORTABLE.includes('pkg'));
  const data = await asEditor(() => RD.listSkus(db, RD.normalizeFilters({ q: 'pk-r1' }), { mode: 'all' }));
  const row = data.rows.find((r) => r.code === 'pk-r1');
  assert.deepEqual([row.pkg.length_mm, row.pkg.weight_g, row.pkg.sources.weight_g], [200, 180, 'human']);
  const csv = CSV.buildListCsv(data, {}, {});
  const lines = csv.replace(/^﻿/, '').trim().split('\r\n');
  const head = lines[0].split(',');
  assert.deepEqual(head.slice(-6), ['"商品コードの注意"', '"梱包の縦 (cm)"', '"梱包の横 (cm)"', '"梱包の高さ (cm)"', '"梱包の重さ (g)"', '"大きさ・重さの出どころ"']);
  assert.ok(lines.find((l) => l.startsWith('"pk-r1"')).endsWith('"20","15.5","2.5","180","縦・横・高さ・重さ = 人"'));
  // 出どころが混ざる商品 (s002 = 人の値なし・重さ = FBA 納品・高さ = 定形外)
  const d2 = await asEditor(() => RD.listSkus(db, RD.normalizeFilters({ q: 's002' }), { mode: 'all' }));
  const l2 = CSV.buildListCsv(d2, {}, {}).replace(/^﻿/, '').trim().split('\r\n').find((l) => l.startsWith('"s002"'));
  assert.ok(l2.endsWith('"","","3","260","高さ = 定形外 (実測) / 重さ = FBA 納品 (実測)"'), l2);
  const list = await call('GET', '/?q=pk-r1');
  assert.match(list.text, /<td data-col="pkg" hidden><span title="[^"]*">20×15.5×2.5 cm · 180 g<\/span><\/td>/);
});

await ta('[Y1] 夜間ロード: 出どころの行を足しても人の値は変わらない・まとめた値は人のまま・夜間ロードのコードは人の表を知らない', async () => {
  const before = await humanOf('s001');
  assert.equal(before.weight_g, 100);
  const plan = basePlan();
  plan.physicals = [{ skuCode: 's001', scope: 'package', weightG: 999, heightMm: 44, source: 'measured', sourceRef: 'pm_skus', isMeasured: true, observedAt: null }];
  const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
  const lr = await runInitialLoad(db, plan, { log: () => {}, runId: 'load_pk_y1', now: new Date(Date.now() - 3 * 86400e3) });
  assert.equal(lr.ok, true, lr.error);
  assert.equal((await one("select count(*)::int as n from core.product_physicals ph join core.skus k on k.product_id = ph.product_id where k.code = 's001' and ph.weight_g = 999")).n, 1, '夜間ロードが出どころの行を入れていない');
  assert.deepEqual(await humanOf('s001'), before);
  const v = await viewOf('s001');
  assert.deepEqual([v.weight_g, v.weight_source, v.height_mm, v.height_source], [100, 'human', 44, 'measured'], '人の値が先・人の値の無い軸は出どころ');
  const src = fs.readFileSync(new URL('../apps/company-db/load/engine.mjs', import.meta.url), 'utf8') + fs.readFileSync(new URL('../apps/company-db/load/sources.mjs', import.meta.url), 'utf8');
  assert.ok(!/product_packages|product_package_events/.test(src));
});

await ta('[B1] ロールの流し直しの前: 欄を出さない (見るだけ)・値を送ったら 503 (登録も・何も作らない)・ほかの欄の保存はそのまま・流し直すと戻る', async () => {
  for (const f of ROLES.PACKAGE_EDIT_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
  for (const t of [...ROLES.PACKAGE_SELECT, ...ROLES.PACKAGE_SOURCE_SELECT]) await pg.query(`revoke select on ${t} from master_edit`);
  try {
    assert.equal(await PK.packageReady(db), false);
    const page = await call('GET', '/new?kind=single');
    assert.ok(!/id="np-length"/.test(page.text), '欄が出ている');
    const n = await call('POST', '/api/new', { body: single('pk-b1', { weight: '10' }) });
    assert.deepEqual([n.status, n.j.reason], [503, 'db_not_ready']);
    assert.equal((await one("select count(*)::int as n from core.skus where code = 'pk-b1'")).n, 0);
    const ok = await call('POST', '/api/new', { body: single('pk-b2') });
    assert.equal(ok.status, 200, ok.text);
    const sp = await call('GET', '/sku/s003');
    assert.equal(sp.status, 200);
    assert.ok(!/id="f-pkg_weight"/.test(sp.text), '欄が出ている');
    assert.match(sp.text, /id="pkg-not-ready"/);
    const s = await call('POST', '/api/sku/s003', { body: { request_id: uuid(), seen: { token: tok(sp.text) }, values: { reorder_months: '3' } } });
    assert.equal(s.status, 200, s.text);
    const s2 = await call('POST', '/api/sku/s003', { body: { request_id: uuid(), seen: { token: tok((await call('GET', '/sku/s003')).text) }, values: { pkg_weight: '50' } } });
    assert.deepEqual([s2.status, s2.j.reason], [503, 'db_not_ready']);
    const list = await call('GET', '/?q=s00');
    assert.equal(list.status, 200, '一覧が止まった');
  } finally {
    await (await import('./company-db/create-master-edit-roles.mjs')).createMasterEditRoles(pg, {});
  }
  assert.equal(await PK.packageReady(db), true, 'ロールを流し直すと戻る');
});

await ta('[B2] 見るだけの人: 欄は 🔒 (人の値)・「この値を入れる」も無い・まとめた値と出どころは見える', async () => {
  const page = await call('GET', '/sku/s001', { session: 'viewer' });
  assert.equal(page.status, 200);
  assert.ok(!/id="f-pkg_weight"/.test(page.text) && !/data-pkg-use=/.test(page.text) && !/me-package\.js/.test(page.text));
  assert.match(page.text, /<span>100 g \(人の値\)<\/span>/);
  assert.match(page.text, /いま使う値: 100 g \(人\)/);
});

await ta('[H1] つかいかた: 大きさ・重さの節 (入れなくても登録できる・出どころ・食い違い・CSV)', async () => {
  const r = await call('GET', '/manual');
  for (const w of ['id="package"', '入れなくても登録できます', '食い違い', 'この値を入れる', '縦・横はまだどのアプリにもありません', '一覧の CSV の最後の 5 列']) assert.ok(r.text.includes(w), `つかいかたに「${w}」が無い`);
});

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
server.close();
await pg.close();
process.exit(process.exitCode || 0);
