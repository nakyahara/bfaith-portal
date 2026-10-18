/**
 * test-master-label.mjs — 表示の項目 = 原産国・内容量・メーカー型番・消費期限の目安 (Company DB構想 27 ④-1・Company DB の 0099 仮番号・lib/master-label.mjs)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 全部の migration を流した新しい DB・切替の後)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   U 値の形: 原産国 (NFKC・「製」・よくある言い方)・内容量 (数 小数 2 けた + 単位 ml / g / 枚 / 個・片方だけ = 400)・型番・日数・登録の値の形 (知らない項目・まとまりの型番 = 400)・
 *     内容量の直し方 (L → ml・kg → g・cc・本 / mg = 直せない・丸めない)・product-hub の原産国の書き方 (日本製 ↔ 日本・海外製 + 国名)
 *   D 0099 の形: 表・関数 (security definer・search_path・public の実行権なし)・守り (関数の外では書けない・記録は追記だけ・TRUNCATE できない)・
 *     画面のロール master_edit = 人の値・記録・ビューの select と書く関数の実行だけ・ほかのロールは読めない
 *   M いま使う値 (core.v_product_label_facts): 人 → 内容量だけ夜間ロードの値 (出どころ = 解決に使った観測)・人の値と記録の番号は 1 つの SELECT (全部外した後も番号を返す)
 *   S 商品の画面: 「表示の項目」の節 (欄・いま使う値・product-hub の今の値の参考・「この値を入れる」)・保存 (入れる・変える・外す・全部外す・記録 via sku_page)・押し直し・
 *     ほかの人が変えた後 = 409・門の外の直の呼び出し = 断る・形の誤り = 400・セットには節が無い
 *   R 新商品の登録 (単品) / まとまり (全部の子に同じ値・型番は聞かない): 入れてよい・入れなくてよい (「値か入力なし」に数えない)・セットは断る・押し直し = 前の答え
 *   Y 夜間ロード: product-hub の内容量の観測で core.products.net_content が変わっても人の値は変わらない・ビューは人のまま・夜間ロードのコードは人の表を知らない
 *   B ロールの流し直しの前: 欄を出さない (見るだけ)・値を送ったら 503 (登録も・何も作らない)・ほかの欄の保存はそのまま
 * 使い方: node scripts/test-master-label.mjs  (画面の動き = scripts/test-master-label-ui.mjs)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';
import Database from 'better-sqlite3';

const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lbl-'));

const { setupVariationDb, basePlan } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const LB = await import('../lib/master-label.mjs');
const ROLES = await import('./company-db/create-master-edit-roles.mjs');
const T = await setupVariationDb();
const { pg, db, q, one, asEditor } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/lb';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/lb';
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
// product-hub の SQLite (試験だけ・本番の列名と同じ最小の表)
const ph = new Database(':memory:');
ph.exec(`CREATE TABLE product_drafts (id INTEGER PRIMARY KEY AUTOINCREMENT, ne_code TEXT NOT NULL UNIQUE, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft');
  CREATE TABLE draft_page_info (draft_id INTEGER PRIMARY KEY, origin_type TEXT, origin_country TEXT, content_volume TEXT);
  CREATE TABLE draft_rakuten (draft_id INTEGER PRIMARY KEY, article_number TEXT);
  CREATE TABLE draft_sku_attributes (draft_id INTEGER NOT NULL, sku_code TEXT NOT NULL, name TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (draft_id, sku_code, name));`);
ph.prepare("INSERT INTO product_drafts (id, ne_code, name) VALUES (11, 's002', '単品 2'), (12, 'ws100', 'ウールストール'), (13, 's003', '単品 3')").run();
ph.prepare("INSERT INTO draft_page_info (draft_id, origin_type, origin_country, content_volume) VALUES (11, '海外製', '中国', '0.5L'), (12, '日本製', '', '200g'), (13, '海外製', '', '30錠')").run();
ph.prepare("INSERT INTO draft_rakuten (draft_id, article_number) VALUES (11, 'MDL-2'), (12, 'WS-PAGE')").run();
ph.prepare("INSERT INTO draft_sku_attributes (draft_id, sku_code, name, value) VALUES (12, 'ws100-br', 'メーカー型番', 'WS-BR')").run();
let phBroken = false;
MR.__setPhLabelLookup(async (code) => { if (phBroken) throw new Error('product-hub の表が読めない (試験)'); return LB.readPhLabelRefs(ph, code); });

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
/** 単品の登録の本文 (label = 表示の項目 / undefined = 送らない) */
function single(code, label, { kind = 'single' } = {}) {
  const b = { request_id: uuid(), kind, code, values: { ...VALUES, name: `試し ${code}` }, card: { create: false },
    ...(kind === 'single' ? { logizard_barcode: { kind: 'lz_assign' } } : {}), ...(label !== undefined ? { label } : {}) };
  if (kind === 'set') { delete b.values.tax_rate; delete b.values.sales_class; delete b.values.primary_supplier; delete b.values.cost; delete b.values.expiry_managed; delete b.values.inbound_date_managed; b.values.components = [{ code: 's001', qty: '2' }]; }
  b.blanks = RB.missingBlankKeys(kind, b, { orderApplicable: true });
  return b;
}
function variation(group, label) {
  const b = { request_id: uuid(), group: { mode: 'new', code: group, name: `試し ${group}` }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-A', name: '色A' }, { axis: 1, code: '-B', name: '色B' }],
    children: ['-A', '-B'].map((c) => ({ code: group + c, choices: { 1: c }, name: `試し ${group}【色${c}】`, jan: '', lz_barcode: { kind: 'lz_assign' } })),
    values: { ...VALUES }, card: { official_url: '', amazon_url: '', asin: '' }, ...(label !== undefined ? { label } : {}) };
  b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
  return b;
}
const humanOf = async (code) => (await q(`select x.country_of_origin as c, x.net_content::text as n, x.net_content_uom as u, x.model_number as m, x.shelf_life_days as d, x.set_via as via
  from core.skus k join core.product_label_facts x on x.product_id = k.product_id where k.code = $1`, [code]))[0] || null;
const eventsOf = async (code) => (await q(`select e.from_value as f, e.to_value as t, e.via, e.actor_id from ops.product_label_fact_events e join core.skus k on k.sku_id = e.sku_id
  where k.code = $1 order by e.event_id`, [code]));
const viewOf = async (code) => (await q(`select v.country_of_origin, v.country_source, v.net_content::text as net_content, v.net_content_uom, v.net_source, v.model_number, v.model_source,
  v.shelf_life_days, v.shelf_source from core.v_product_label_facts v join core.skus k on k.product_id = v.product_id where k.code = $1`, [code]))[0] || null;
const pidOf = async (code) => (await one('select product_id::text as id from core.skus where code = $1', [code])).id;
const skuIdOf = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;
const tok = (t) => (/data-token="([0-9a-f]{64})"/.exec(t) || [])[1];
const save = async (code, values, { token = null, session = 'editor' } = {}) => {
  const t = token || tok((await call('GET', `/sku/${code}`)).text);
  return call('POST', `/api/sku/${code}`, { body: { request_id: uuid(), seen: { token: t }, values }, session });
};
/** 夜間ロード (本物の engine)。obs = 足す観測 */
async function nightly(runId, obs = []) {
  const plan = basePlan();
  plan.observations = obs;
  const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
  const lr = await runInitialLoad(db, plan, { log: () => {}, runId, now: new Date(Date.now() - 2 * 86400e3) });
  assert.equal(lr.ok, true, lr.error);
  return lr;
}

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('表示の項目 (0099)');

await ta('[U1] 値の形: 原産国・内容量 (数 + 単位)・型番・日数・空 = null・形の誤りの一言と欄', async () => {
  assert.equal(LB.countryIn('中国製'), '中国'); assert.equal(LB.countryIn(' 日本国 '), '日本'); assert.equal(LB.countryIn('Made in Japan'), '日本'); assert.equal(LB.countryIn('ＵＳＡ'), 'アメリカ');
  assert.equal(LB.countryIn('ベトナム'), 'ベトナム'); assert.equal(LB.countryIn(''), null); assert.equal(LB.countryIn(null), null);
  for (const bad of ['製', 'あ'.repeat(41), '中\n国', 12]) assert.throws(() => LB.countryIn(bad), (e) => e.status === 400 && e.extra.field === 'lbl_country', String(bad));
  assert.equal(LB.netNumIn('50'), '50'); assert.equal(LB.netNumIn('１．５０'), '1.5'); assert.equal(LB.netNumIn('1,000'), '1000'); assert.equal(LB.netNumIn('050'), '50'); assert.equal(LB.netNumIn(''), null);
  for (const bad of ['0', '0.00', '1.234', '-1', 'abc', '123456789']) assert.throws(() => LB.netNumIn(bad), (e) => e.status === 400 && e.extra.field === 'lbl_net', bad);
  assert.equal(LB.netUomIn('ML'), 'ml'); assert.equal(LB.netUomIn('枚'), '枚'); assert.equal(LB.netUomIn(''), null);
  for (const bad of ['L', 'kg', '本']) assert.throws(() => LB.netUomIn(bad), (e) => e.extra.field === 'lbl_net_uom', bad);
  assert.equal(LB.modelIn(' AB-1 '), 'AB-1'); assert.equal(LB.modelIn(''), null);
  assert.throws(() => LB.modelIn('x'.repeat(61)), (e) => e.extra.field === 'lbl_model');
  assert.equal(LB.shelfIn('365'), 365); assert.equal(LB.shelfIn('７３０日'), 730); assert.equal(LB.shelfIn(''), null);
  for (const bad of ['0', '3651', '1.5', 'x']) assert.throws(() => LB.shelfIn(bad), (e) => e.extra.field === 'lbl_shelf', bad);
  assert.deepEqual(LB.labelFactsIn({ country: '中国製', net: '50', net_uom: 'ml', model: 'X-1', shelf: '365' }),
    { country_of_origin: '中国', net_content: '50', net_content_uom: 'ml', model_number: 'X-1', shelf_life_days: 365 });
  assert.equal(LB.labelFactsIn({ country: '', net: '', net_uom: '', model: '', shelf: '' }), null, '全部空 = 入れない');
  assert.equal(LB.labelFactsIn(undefined), null);
  for (const bad of [[], 'x', { origin: '日本' }, { net: '50' }, { net_uom: 'g' }]) assert.throws(() => LB.labelFactsIn(bad), (e) => e.status === 400, JSON.stringify(bad));
  assert.throws(() => LB.labelFactsIn({ model: 'X' }, { noModel: true }), (e) => e.status === 400 && /まとまりの登録ではメーカー型番を聞きません/.test(e.message));
});

await ta('[U2] 内容量の直し方 (丸めない)・比べ方・product-hub の原産国の書き方', async () => {
  assert.deepEqual(LB.netToMaster('0.5', 'L'), { num: '500', uom: 'ml' });
  assert.deepEqual(LB.netToMaster('1.25', 'kg'), { num: '1250', uom: 'g' });
  assert.deepEqual(LB.netToMaster('30', 'cc'), { num: '30', uom: 'ml' });
  assert.deepEqual(LB.netToMaster('50', 'mL'), { num: '50', uom: 'ml' });
  assert.equal(LB.netToMaster('500', 'mg'), null); assert.equal(LB.netToMaster('30', '錠'), null); assert.equal(LB.netToMaster('1.234', 'g'), null); assert.equal(LB.netToMaster('0', 'g'), null);
  assert.equal(LB.netEqual({ num: '0.5', uom: 'L' }, { num: '500', uom: 'ml' }), true);
  assert.equal(LB.netEqual({ num: '50', uom: 'ml' }, { num: '50', uom: 'g' }), false);
  assert.equal(LB.netEqual({ num: '30', uom: '錠' }, { num: '30', uom: '個' }), null, '錠と個は比べない');
  assert.deepEqual(LB.netFromText('500ml×2'), { num: '500', uom: 'ml', text: '500ml×2', master: { num: '500', uom: 'ml' } });
  assert.equal(LB.netFromText('たくさん'), null);
  assert.deepEqual(LB.phCountryOf({ origin_type: '日本製', origin_country: '' }), { value: '日本', state: 'value' });
  assert.deepEqual(LB.phCountryOf({ origin_type: '海外製', origin_country: '中華人民共和国' }), { value: '中国', state: 'value' });
  assert.deepEqual(LB.phCountryOf({ origin_type: '海外製', origin_country: '' }), { value: null, state: 'no_country' });
  assert.deepEqual(LB.phCountryOf(null), { value: null, state: 'empty' });
  assert.deepEqual(LB.phCountryFields('日本'), { origin_type: '日本製', origin_country: '' });
  assert.deepEqual(LB.phCountryFields('中国'), { origin_type: '海外製', origin_country: '中国' });
  assert.equal(LB.phCountryFields(''), null);
});

await ta('[D1] 0099 の形: 表・関数 (security definer・search_path 固定・public の実行権なし)・画面のロールの権限 (読む・関数だけ)・ほかのロールは読めない', async () => {
  for (const t of ['core.product_label_facts', 'ops.product_label_fact_events', 'core.v_product_label_facts']) assert.ok((await one('select to_regclass($1) is not null as ok', [t])).ok, t);
  const f = await one(`select p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p
    join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname = 'set_product_label_facts'`);
  assert.deepEqual([f.d, f.pub], [true, false]);
  assert.match(f.c, /search_path=pg_catalog, pg_temp/);
  assert.equal((await one("select has_function_privilege('public', 'ops.guard_label_tables()'::regprocedure, 'execute') as ok")).ok, false);
  for (const t of ROLES.LABEL_SELECT) {
    const p = await one(`select has_table_privilege('master_edit', $1, 'select') as s, has_table_privilege('master_edit', $1, 'insert') as i, has_table_privilege('master_edit', $1, 'update') as u,
      has_table_privilege('master_edit', $1, 'delete') as d, has_table_privilege('master_edit', $1, 'truncate') as tr`, [t]);
    assert.deepEqual(p, { s: true, i: false, u: false, d: false, tr: false }, t);
    for (const r of ['master_ops', 'master_observer', 'master_gate']) assert.equal((await one(`select has_table_privilege($1, $2, 'select') as s`, [r, t])).s, false, `${r} が ${t} を読める`);
  }
  for (const fn of ROLES.LABEL_EDIT_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [fn])).ok, true, fn);
  for (const fn of ROLES.LABEL_OWNER_ONLY_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [fn])).ok, false, fn);
  assert.equal(LB.LABEL_FUNCTION, ROLES.LABEL_EDIT_FUNCTIONS[0]);
  assert.equal(await LB.labelReady(db), true);
  assert.equal(await asEditor(() => LB.labelReady(db)), true);
  // 既にある列は変えない (注釈だけ)・夜間ロードの規則も変えない
  assert.equal((await one("select count(*)::int as n from core.rule_versions")).n, 1, '規則の版を足した');
  assert.match((await one("select col_description('core.products'::regclass, (select attnum from pg_attribute where attrelid = 'core.products'::regclass and attname = 'net_content')) as c")).c, /v_product_label_facts/);
});

await ta('[M1] いま使う値: 人の値が無い = 内容量だけ夜間ロードの値 (出どころつき)・ほかは空・人の値と記録の番号は 1 つの読み', async () => {
  await nightly('load_lbl_m1', [{ skuCode: 's001', attribute: 'net_content', scope: 'item', valueNum: 100, unit: 'ml', rawText: '100ml', source: 'product_hub', sourceRef: 'draft_page_info:1', observedAt: new Date(Date.now() - 3 * 86400e3).toISOString() }]);
  const v = await viewOf('s001');
  assert.deepEqual(v, { country_of_origin: null, country_source: null, net_content: '100.00', net_content_uom: 'ml', net_source: 'product_hub', model_number: null, model_source: null, shelf_life_days: null, shelf_source: null });
  assert.equal(await viewOf('s002'), null, '何も無い商品は行が無い');
  const lv = await asEditor(async () => LB.readLabelView(db, await pidOf('s001')));
  assert.deepEqual([lv.human, lv.event_id, lv.resolved.net.text, lv.resolved.net.words, lv.resolved.net.master], [null, null, '100ml', '夜間の取り込み (product-hub)', { num: '100', uom: 'ml' }]);
});

let TOKEN_S002 = null;
await ta('[S1] 商品の画面: 節 (欄・いま使う値・product-hub の今の値の参考)・保存 (入れる → 変える・外す → 全部外す・記録 via sku_page)・押し直し・変わらない保存は書かない', async () => {
  let page = await call('GET', '/sku/s002');
  assert.equal(page.status, 200);
  TOKEN_S002 = tok(page.text);
  assert.match(page.text, /<section class="panel" id="p-label"/);
  const iSup = page.text.indexOf('id="h-sup"'), iLbl = page.text.indexOf('id="p-label"');
  assert.ok(iSup > 0 && iLbl > iSup, '「表示の項目」は仕入先の下');
  for (const f of ['lbl_country', 'lbl_net', 'lbl_net_uom', 'lbl_model', 'lbl_shelf']) assert.match(page.text, new RegExp(`id="f-${f}"[^>]*data-field="${f}" data-dirty-field="${f}"`), f);
  assert.match(page.text, /product-hub の下書き #11: 海外製 中国 = 中国 <button type="button" class="btn sm" data-lbl-use="country" data-v="中国"/);
  assert.match(page.text, /product-hub の下書き #11: 0\.5L <button type="button" class="btn sm" data-lbl-use="net" data-v="500" data-uom="ml"/, 'L → ml にちょうど直して入れる');
  assert.match(page.text, /\(ページの型番\): <span class="mono">MDL-2<\/span> <button[^>]*data-lbl-use="model" data-v="MDL-2"/);
  assert.match(page.text, /public\/me-label\.js/);
  const rid = uuid();
  const body = { request_id: rid, seen: { token: TOKEN_S002 }, values: { lbl_country: '中国製', lbl_net: '500', lbl_net_uom: 'ml', lbl_model: 'MDL-2', lbl_shelf: '' } };
  const a = await call('POST', '/api/sku/s002', { body });
  assert.equal(a.status, 200, a.text);
  assert.deepEqual(a.j.changed.map((c) => [c.field, c.from, c.to, c.ne]), [['lbl_country', null, '中国', 'none'], ['lbl_net', null, '500ml', 'none'], ['lbl_model', null, 'MDL-2', 'none']]);
  assert.deepEqual(a.j.ne_steps, []);
  assert.deepEqual(await humanOf('s002'), { c: '中国', n: '500.00', u: 'ml', m: 'MDL-2', d: null, via: 'sku_page' });
  const v = await viewOf('s002');
  assert.deepEqual([v.country_source, v.net_source, v.model_source, v.shelf_source], ['human', 'human', 'human', null]);
  const again = await call('POST', '/api/sku/s002', { body });
  assert.deepEqual([again.status, again.j.replayed], [200, true]);
  assert.equal((await eventsOf('s002')).length, 1, '押し直しで 2 回書いた');
  // 同じ値は「同じ」の札・ボタンは出さない
  page = await call('GET', '/sku/s002');
  assert.match(page.text, /id="f-lbl_country"[^>]*value="中国"/);
  assert.match(page.text, /海外製 中国 = 中国 <span class="b ok">同じ<\/span>/);
  assert.match(page.text, /0\.5L <span class="b ok">同じ<\/span>/);
  // 変える・外す (日数を足す・型番を外す)
  const b = await save('s002', { lbl_country: '中国', lbl_net: '500', lbl_net_uom: 'ml', lbl_model: '', lbl_shelf: '730' });
  assert.equal(b.status, 200, b.text);
  assert.deepEqual(b.j.changed.map((c) => [c.field, c.from, c.to]), [['lbl_model', 'MDL-2', null], ['lbl_shelf', null, 730]]);
  // 変わらない保存 = 書かない (数の書き方が違っても同じ)
  const n = await save('s002', { lbl_country: '中国', lbl_net: '500.0', lbl_net_uom: 'ml', lbl_model: '', lbl_shelf: '730' });
  assert.equal(n.j.no_change, true);
  // 内容量の片方だけ = 400
  const half = await save('s002', { lbl_net: '', lbl_net_uom: 'ml' });
  assert.deepEqual([half.status, half.j.field], [400, 'lbl_net']);
  // 全部外す = 行を消す (記録は残る)・番号は残る (全部外した後も入れ直せる)
  const c = await save('s002', { lbl_country: '', lbl_net: '', lbl_net_uom: '', lbl_model: '', lbl_shelf: '' });
  assert.equal(c.status, 200, c.text);
  assert.equal(await humanOf('s002'), null);
  const ev = await eventsOf('s002');
  assert.deepEqual(ev.map((e) => [e.f && e.f.model_number, e.t && e.t.country_of_origin, e.via, e.actor_id]), [[null, '中国', 'sku_page', 'naka@test'], ['MDL-2', '中国', 'sku_page', 'naka@test'], [null, null, 'sku_page', 'naka@test']]);
  const hl = await asEditor(async () => LB.readHumanLabel(db, await pidOf('s002')));
  assert.deepEqual(hl, { human: null, event_id: String(ev.length ? (await one("select max(event_id)::text as e from ops.product_label_fact_events e join core.skus k on k.sku_id = e.sku_id where k.code = 's002'")).e : null) });
  const d = await save('s002', { lbl_country: '日本' });
  assert.equal(d.status, 200, d.text);
  assert.equal((await humanOf('s002')).c, '日本');
});

await ta('[S2] ほかの人が変えた後 = 409・記録の無い商品の印は前と同じ・DB の関数も最後の記録と照らす・門の外の直の呼び出し = 断る・セットには節が無い', async () => {
  const W = await import('../lib/master-write.mjs');
  const today = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
  const id3 = await skuIdOf('s003');
  const cur0 = await asEditor(() => W.readCurrent(db, id3, today));
  assert.deepEqual(cur0.label, { human: null, event_id: null });
  assert.equal(W.editTokenOf({ ...cur0, label: undefined }), W.editTokenOf(cur0), '記録の無い商品の印が変わった');
  const old = tok((await call('GET', '/sku/s003')).text);
  const a = await save('s003', { lbl_shelf: '365' }, { token: old });
  assert.equal(a.status, 200, a.text);
  const b = await save('s003', { lbl_shelf: '400' }, { token: old });
  assert.deepEqual([b.status, b.j.reason], [409, 'version_conflict']);
  assert.equal((await humanOf('s003')).d, 365);
  // 直に呼ぶ = 門を通った保存・登録の取引の外 = 断る
  const call0 = (rid, sid, seen, via, actor = 'naka@test', country = '日本') => pg.query("select ops.set_product_label_facts($1::uuid, $2, $3::bigint, $4, null, null, null, null, $5::bigint, $6, null) as r", [rid, actor, sid, country, seen, via]);
  await asEditor(async () => {
    const last = (await one("select max(event_id)::text as e from ops.product_label_fact_events where product_id = $1", [await pidOf('s003')])).e;
    for (const via of ['sku_page', 'register', 'variation']) {
      await pg.query('begin');
      try { await assert.rejects(call0(uuid(), id3, via === 'sku_page' ? last : null, via), /master_write_session_required/, via); } finally { await pg.query('rollback'); }
    }
    // 保存の約束の中でも、見ていた記録が違えば断る・約束と違う request_id・人・ほかの SKU は断る・形の誤りは断る
    const cur = await W.readCurrent(db, id3, today);
    const RID = uuid();
    const begin = (sid) => pg.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)',
      [RID, 'naka@test', null, T.OWN, 'sku_edit', sid, W.editTokenOf(cur), 'a'.repeat(64), JSON.stringify(W.editVersionsOf(cur))]);
    const cases = [
      [() => call0(RID, id3, null, 'sku_page'), /version_conflict/],
      [() => call0(RID, id3, cur.label.event_id, 'sku_page', 'naka@test', '中国製'), /invalid_input/],
      [() => call0(RID, id3, cur.label.event_id, 'sku_page', 'naka@test', ' 日本'), /invalid_input/],
      [() => call0(uuid(), id3, cur.label.event_id, 'sku_page'), /master_write_session_required/],
      [() => call0(RID, id3, cur.label.event_id, 'sku_page', 'other@test'), /master_write_session_required/],
    ];
    for (const [fn, re] of cases) {
      await pg.query('begin');
      try { await begin(id3); await assert.rejects(fn(), re); } finally { await pg.query('rollback'); }
    }
    for (const [args, re] of [[['1', 'ml'], /invalid_input/], [[null, 'ml'], /invalid_input/], [['1.234', 'ml'], /invalid_input/], [['5', 'L'], /invalid_input/]]) {
      await pg.query('begin');
      try {
        await begin(id3);
        await assert.rejects(pg.query("select ops.set_product_label_facts($1::uuid, 'naka@test', $2::bigint, null, $3::numeric, $4, null, null, $5::bigint, 'sku_page', null)",
          [RID, id3, args[0] === '1' ? null : args[0], args[1], cur.label.event_id]), re, JSON.stringify(args));
      } finally { await pg.query('rollback'); }
    }
    await pg.query('begin');
    try { await begin(id3); const r = (await call0(RID, id3, cur.label.event_id, 'sku_page')).rows[0].r; assert.equal(r.changed, true, '約束どおり = 書ける'); } finally { await pg.query('rollback'); }
  });
  // 形の誤り = 400 (欄つき)・何も書かない
  for (const [values, field] of [[{ lbl_country: '製' }, 'lbl_country'], [{ lbl_net: '0', lbl_net_uom: 'g' }, 'lbl_net'], [{ lbl_net: '5', lbl_net_uom: 'L' }, 'lbl_net_uom'], [{ lbl_shelf: '0' }, 'lbl_shelf']]) {
    const r = await save('s003', values);
    assert.deepEqual([r.status, r.j.reason, r.j.field], [400, 'invalid_input', field], JSON.stringify(values));
  }
  assert.equal((await humanOf('s003')).d, 365);
  // product-hub の内容量が直せない (錠) = 入れるボタンを出さない・海外製で国名が空 = 入れられない
  const p3 = await call('GET', '/sku/s003');
  assert.match(p3.text, /30錠 = ml \/ g \/ 枚 \/ 個 にちょうど直せないので入れられません/);
  assert.match(p3.text, /海外製 \(国名なし\) = 入れられません/);
  assert.ok(!/data-lbl-use="net"/.test(p3.text));
});

await ta('[S3] まとまりの子: 子の SKU の型番の行だけ参考に出す (親のページの型番・原産国・内容量は子に出さない)・product-hub が読めない = 知らせだけ (画面は止めない)', async () => {
  const p = await call('GET', '/sku/ws100-BR');
  assert.equal(p.status, 200);
  assert.match(p.text, /product-hub の下書き #12 \(まとまりの子の行\) \(SKU ごとの型番\): <span class="mono">WS-BR<\/span>/);
  assert.ok(!/WS-PAGE/.test(p.text), '親のページの型番を子に出した');
  assert.ok(!/200g/.test(p.text) && !/data-lbl-use="country"/.test(p.text), '親のページの値を子に出した');
  const p2 = await call('GET', '/sku/ws100-NV');
  assert.ok(!/product-hub の下書き/.test(p2.text), '型番の行の無い子に参考が出た');
  phBroken = true;
  try {
    const p3 = await call('GET', '/sku/s001');
    assert.equal(p3.status, 200);
    assert.match(p3.text, /id="lbl-ph-error"/);
    assert.match(p3.text, /id="f-lbl_country"/);
  } finally { phBroken = false; }
});

await ta('[D2] 守り: 持ち主でも関数の外では書けない・記録は追記だけ (印があっても)・TRUNCATE できない・画面のロールは表に直接書けない', async () => {
  const pid = await pidOf('s001');
  await assert.rejects(pg.query("insert into core.product_label_facts (product_id, shelf_life_days, set_by, set_via, request_id) values ($1, 10, 'x@test', 'sku_page', $2)", [pid, uuid()]), /ops.set_product_label_facts の中でだけ/);
  await assert.rejects(pg.query('truncate core.product_label_facts'), /./);
  await assert.rejects(pg.query('truncate ops.product_label_fact_events'), /./);
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.label_protocol', '1', true)");
    await assert.rejects(pg.query('update ops.product_label_fact_events set reason = null'), /追記だけ/);
  } finally { await pg.query('rollback'); }
  await asEditor(() => assert.rejects(pg.query("insert into core.product_label_facts (product_id, shelf_life_days, set_by, set_via, request_id) values ($1, 10, 'x@test', 'sku_page', $2)", [pid, uuid()]), /permission denied/));
});

await ta('[R1] 新商品の登録 (単品): 入れてよい (同じ取引・記録 via register)・入れなくてよい (「値か入力なし」に数えない)・押し直し = 前の答え・形の誤り / セット = 400 (何も作らない)', async () => {
  for (const kind of ['single', 'variation', 'set']) assert.ok(!RB.applicableBlankKeys(kind, {}, { orderApplicable: true }).some((k) => /^(lbl_|label$)|country|shelf|net_content|model_number/.test(k)), `${kind}: 表示の項目を「値か入力なし」に数えた`);
  const b = single('lb-r1', { country: '日本', net: '30', net_uom: '枚', model: 'LB-R1', shelf: '' });
  const r = await call('POST', '/api/new', { body: b });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual(await humanOf('lb-r1'), { c: '日本', n: '30.00', u: '枚', m: 'LB-R1', d: null, via: 'register' });
  assert.deepEqual((await eventsOf('lb-r1')).map((e) => [e.f, e.via, e.actor_id]), [[null, 'register', 'naka@test']]);
  const again = await call('POST', '/api/new', { body: b });
  assert.deepEqual([again.status, again.j.replayed], [200, true], again.text);
  assert.equal((await eventsOf('lb-r1')).length, 1);
  const diff = await call('POST', '/api/new', { body: { ...b, label: { ...b.label, shelf: '30' } } });
  assert.deepEqual([diff.status, diff.j.reason], [409, 'request_id_reused']);
  const r2 = await call('POST', '/api/new', { body: single('lb-r2') });
  assert.equal(r2.status, 200, r2.text);
  assert.equal(await humanOf('lb-r2'), null);
  const r3 = await call('POST', '/api/new', { body: single('lb-r3', { country: '', net: '', net_uom: '', model: '', shelf: '' }) });
  assert.equal(r3.status, 200, r3.text);
  assert.equal(await humanOf('lb-r3'), null);
  const n1 = await call('POST', '/api/new', { body: single('lb-r4', { net: '50' }) });
  assert.deepEqual([n1.status, n1.j.field], [400, 'label']);
  const n2 = await call('POST', '/api/new', { body: single('lb-r5', { shelf: '0' }) });
  assert.deepEqual([n2.status, n2.j.field], [400, 'label']);
  const n3 = await call('POST', '/api/new', { body: single('lbset1', { country: '日本' }, { kind: 'set' }) });
  assert.deepEqual([n3.status, n3.j.field], [400, 'label']);
  assert.equal((await one("select count(*)::int as n from core.skus where code in ('lb-r4', 'lb-r5', 'lbset1')")).n, 0, '断った登録で商品ができた');
  const page = await call('GET', '/new?kind=single');
  assert.match(page.text, /id="np-lbl-country"/);
  assert.match(page.text, /原産国<span class="opt">入れなくても可<\/span>/);
  assert.match(page.text, /id="np-lbl-model"/);
  const setPage = await call('GET', '/new?kind=set');
  assert.ok(!/id="np-lbl-country"/.test(setPage.text), 'セットに表示の項目の欄がある');
  // セットの商品の画面には節が無い
  const rs = await call('POST', '/api/new', { body: single('lbset2', undefined, { kind: 'set' }) });
  assert.equal(rs.status, 200, rs.text);
  const sp = await call('GET', '/sku/lbset2');
  assert.equal(sp.status, 200);
  assert.ok(!/id="p-label"/.test(sp.text), 'セットの画面に表示の項目がある');
});

await ta('[V1] まとまりの登録: 全部の子に同じ値 (記録は子ごと・via variation)・型番は聞かない (400)・入れなくてよい・形の誤り = 何も作らない', async () => {
  const r = await call('POST', '/api/new/variation', { body: variation('lbv1', { country: '韓国', net: '200', net_uom: 'g', shelf: '180' }) });
  assert.equal(r.status, 200, r.text);
  for (const c of ['lbv1-A', 'lbv1-B']) {
    assert.deepEqual(await humanOf(c), { c: '韓国', n: '200.00', u: 'g', m: null, d: 180, via: 'variation' }, c);
    assert.deepEqual((await eventsOf(c)).map((e) => [e.f, e.via]), [[null, 'variation']], c);
  }
  const r2 = await call('POST', '/api/new/variation', { body: variation('lbv2') });
  assert.equal(r2.status, 200, r2.text);
  assert.equal(await humanOf('lbv2-A'), null);
  const r3 = await call('POST', '/api/new/variation', { body: variation('lbv3', { model: 'X' }) });
  assert.deepEqual([r3.status, r3.j.field], [400, 'label']);
  const r4 = await call('POST', '/api/new/variation', { body: variation('lbv4', { net: '1.234', net_uom: 'g' }) });
  assert.deepEqual([r4.status, r4.j.field], [400, 'label']);
  assert.equal((await one("select count(*)::int as n from core.skus where code like 'lbv3-%' or code like 'lbv4-%'")).n, 0);
  const vp = await call('GET', '/new?kind=variation');
  assert.match(vp.text, /id="np-lbl-country"/);
  assert.ok(!/id="np-lbl-model"/.test(vp.text), 'まとまりで型番を聞いた');
});

await ta('[Y1] 夜間ロード: product-hub の内容量の観測で core.products.net_content が変わっても人の値は変わらない・ビューは人のまま・人の値を外すと夜間の値に戻る・夜間ロードのコードは人の表を知らない', async () => {
  const W = await import('../lib/master-write.mjs');
  // s001 に人の値 (50 g)
  const a = await save('s001', { lbl_net: '50', lbl_net_uom: 'g', lbl_country: '日本' });
  assert.equal(a.status, 200, a.text);
  const before = await humanOf('s001');
  const evBefore = (await eventsOf('s001')).length;
  // 夜間ロード: product-hub の内容量が 120ml に変わった (新しい観測)
  await nightly('load_lbl_y1', [{ skuCode: 's001', attribute: 'net_content', scope: 'item', valueNum: 120, unit: 'ml', rawText: '120ml', source: 'product_hub', sourceRef: 'draft_page_info:1', observedAt: new Date(Date.now() - 2.5 * 86400e3).toISOString() }]);
  const p = await one("select p.net_content::text as n, p.net_content_uom as u from core.products p join core.skus k on k.product_id = p.product_id where k.code = 's001'");
  assert.deepEqual(p, { n: '120.00', u: 'ml' }, '夜間ロードが core.products の内容量を直していない (試験の前提)');
  assert.deepEqual(await humanOf('s001'), before, '夜間ロードが人の値を変えた');
  assert.equal((await eventsOf('s001')).length, evBefore, '夜間ロードが記録を足した');
  let v = await viewOf('s001');
  assert.deepEqual([v.net_content, v.net_content_uom, v.net_source, v.country_of_origin], ['50.00', 'g', 'human', '日本'], 'いま使う値が人の値でない');
  // もう 1 回 (同じ材料・再送) でも変わらない
  await nightly('load_lbl_y1b', [{ skuCode: 's001', attribute: 'net_content', scope: 'item', valueNum: 120, unit: 'ml', rawText: '120ml', source: 'product_hub', sourceRef: 'draft_page_info:1', observedAt: new Date(Date.now() - 2.5 * 86400e3).toISOString() }]);
  assert.deepEqual(await humanOf('s001'), before);
  // 人の値の内容量を外す = 夜間の値に戻る (原産国は人のまま)
  const c = await save('s001', { lbl_net: '', lbl_net_uom: '' });
  assert.equal(c.status, 200, c.text);
  v = await viewOf('s001');
  assert.deepEqual([v.net_content, v.net_content_uom, v.net_source, v.country_source], ['120.00', 'ml', 'product_hub', 'human']);
  // 夜間ロードのコードは人の表を知らない (読まない・書かない)
  const src = fs.readFileSync(new URL('../apps/company-db/load/engine.mjs', import.meta.url), 'utf8') + fs.readFileSync(new URL('../apps/company-db/load/sources.mjs', import.meta.url), 'utf8');
  assert.ok(!/product_label_facts|product_label_fact_events|v_product_label_facts/.test(src));
  assert.ok(W.SINGLE_FIELDS.lbl_net.keys.length === 0, '表示の項目に持ち主のキーを付けた');
});

await ta('[B1] ロールの流し直しの前: 欄を出さない (見るだけ)・値を送ったら 503 (登録も・何も作らない)・ほかの欄の保存はそのまま・流し直すと戻る', async () => {
  for (const f of ROLES.LABEL_EDIT_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
  for (const t of ROLES.LABEL_SELECT) await pg.query(`revoke select on ${t} from master_edit`);
  try {
    assert.equal(await LB.labelReady(db), false);
    const page = await call('GET', '/new?kind=single');
    assert.ok(!/id="np-lbl-country"/.test(page.text), '欄が出ている');
    const n = await call('POST', '/api/new', { body: single('lb-b1', { country: '日本' }) });
    assert.deepEqual([n.status, n.j.reason], [503, 'db_not_ready']);
    assert.equal((await one("select count(*)::int as n from core.skus where code = 'lb-b1'")).n, 0);
    const ok = await call('POST', '/api/new', { body: single('lb-b2') });
    assert.equal(ok.status, 200, ok.text);
    const sp = await call('GET', '/sku/s003');
    assert.equal(sp.status, 200);
    assert.ok(!/id="f-lbl_country"/.test(sp.text), '欄が出ている');
    assert.match(sp.text, /id="lbl-not-ready"/);
    const s = await save('s003', { reorder_months: '3' });
    assert.equal(s.status, 200, s.text);
    const s2 = await save('s003', { lbl_country: '日本' });
    assert.deepEqual([s2.status, s2.j.reason], [503, 'db_not_ready']);
  } finally {
    await (await import('./company-db/create-master-edit-roles.mjs')).createMasterEditRoles(pg, {});
  }
  assert.equal(await LB.labelReady(db), true, 'ロールを流し直すと戻る');
});

await ta('[B2] 見るだけの人: 欄は 🔒 (人の値)・「この値を入れる」も無い・いま使う値は見える', async () => {
  const page = await call('GET', '/sku/s001', { session: 'viewer' });
  assert.equal(page.status, 200);
  assert.ok(!/id="f-lbl_country"/.test(page.text) && !/data-lbl-use=/.test(page.text) && !/me-label\.js/.test(page.text));
  assert.match(page.text, /<span>日本 \(人の値\)<\/span>/);
  assert.match(page.text, /いま使う値: 120ml \(夜間の取り込み \(product-hub\)\)/);
});

await ta('[H1] つかいかた: 表示の項目の節', async () => {
  const r = await call('GET', '/manual');
  for (const w of ['id="label"', '入れなくても登録できます', 'この値を入れる', '製造 (入荷) から何日もつか']) assert.ok(r.text.includes(w), `つかいかたに「${w}」が無い`);
});

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
server.close();
await pg.close();
process.exit(process.exitCode || 0);
