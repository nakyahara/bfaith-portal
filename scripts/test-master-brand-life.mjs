/**
 * test-master-brand-life.mjs — ブランドの名簿と商品ごとのブランド・商品の段階 (区分・終売の予定日)・初めて売った日 (中原さん 2026-10-17・Company DB の 0088・lib/master-brand-life.mjs)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 全部の migration を流した新しい DB・切替の後)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   D 0088 の形と権限: 表・関数 (security definer・search_path・public の実行権なし)・画面のロール = 表の select と関数の実行だけ (要求の控え・部品は無い)・守り (関数の外では書けない・記録は追記だけ・TRUNCATE できない)
 *   C 名簿に足す (POST /api/brands): 足す・自社ブランドとして足す・同じと見る名前 (メーカーと同じ決まり) = 足さずにそれ (印も名簿のまま)・押し直し = 前の答え・同じ番号で違う中身 = 409・形・名簿の人・Origin
 *   O 自社ブランドの印 (POST /api/brands/own): 変える・押し直し・見ていた印と違う = 409・同じ = 変えない
 *   R 単品の登録: ブランド・区分・終売の予定日を付ける (同じ取引)・🚨 「値か入力なし」に数えない (無くても通る)・名簿に無い / 形が違う = 400 (何も作らない)・セットは聞かない・押し直し = 2 回書かない
 *   V まとまりの登録: 全部の子に同じ値・今あるまとまりの読み込み = 子の値
 *   S 商品の画面の保存: 付ける・変える・外す (記録・via sku_page)・板 (仕入先の板の下)・ほかの人が変えた後 = 409・DB の関数は門を通った取引の中だけ・無い商品の印は前と同じ
 *   F 初めて売った日: NE の出荷 (FBA 納品・取り消しは数えない・受注日)・モールの注文 (取り消しは数えない・出品の構成・セットの構成品)・いちばん早い日と出どころ・記録の始まりの近く = 「これより前かも」・商品の画面
 *   L 一覧 (列 = いつもの列に出さない・絞り込み「終売の予定が近い」ほか・詳細検索の印でも残る) と CSV (メーカーの後ろの 4 列)
 *   Y 夜間ロード: products.brand (属性の解決) を書いても、ポータルのブランドは変わらない
 *   B ロールの流し直しの前: 欄を出さない・値を送ったら 503・ほかの保存はそのまま
 * 使い方: node scripts/test-master-brand-life.mjs
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
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'brandlife-'));

const { setupVariationDb, basePlan } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const BL = await import('../lib/master-brand-life.mjs');
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

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/bl';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/bl';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const MR = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
MR.__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
MR.__setShippingRatesProvider(async () => new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]));
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
const today = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400e3).toISOString().slice(0, 10);
const VALUES = { standard_price: '2980', cost: { jpy: '1200' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' };
/** 単品の登録の本文 (必須でない項目は 値 か 入力なし。extra = ブランド・区分・終売の予定日) */
function single(code, extra = {}, { kind = 'single' } = {}) {
  const b = { request_id: uuid(), kind, code, values: { ...VALUES, name: `試し ${code}`, ...extra }, card: { create: false },
    ...(kind === 'single' ? { logizard_barcode: { kind: 'lz_assign' } } : {}) };
  if (kind === 'set') { for (const k of ['tax_rate', 'sales_class', 'primary_supplier', 'cost', 'expiry_managed', 'inbound_date_managed']) delete b.values[k]; b.values.components = [{ code: 's001', qty: '2' }]; }
  b.blanks = RB.missingBlankKeys(kind, b, { orderApplicable: true });
  return b;
}
function variation(group, extra = {}) {
  const b = { request_id: uuid(), group: { mode: 'new', code: group, name: `試し ${group}` }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-A', name: '色A' }, { axis: 1, code: '-B', name: '色B' }],
    children: ['-A', '-B'].map((c) => ({ code: group + c, choices: { 1: c }, name: `試し ${group}【色${c}】`, jan: '', lz_barcode: { kind: 'lz_assign' } })),
    values: { ...VALUES, ...extra }, card: { official_url: '', amazon_url: '', asin: '' } };
  b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
  return b;
}
const lifeOf = async (code) => (await q(`select b.name as brand, b.own_brand as own, l.stage, l.end_planned_on::text as end_on from core.skus k
  left join core.product_brands x on x.product_id = k.product_id left join core.brands b on b.brand_id = x.brand_id
  left join core.product_lifecycle l on l.product_id = k.product_id where k.code = $1`, [code]))[0] || null;
const brandEvents = async (code) => q(`select e.from_brand_id::text as f, e.to_brand_id::text as t, e.via from ops.product_brand_events e join core.skus k on k.sku_id = e.sku_id where k.code = $1 order by e.event_id`, [code]);
const lifeEvents = async (code) => q(`select e.from_stage as fs, e.to_stage as ts, e.from_end::text as fe, e.to_end::text as te, e.via from ops.product_lifecycle_events e join core.skus k on k.sku_id = e.sku_id
  where k.code = $1 order by e.event_id`, [code]);
const tok = (t) => (/data-token="([0-9a-f]{64})"/.exec(t) || [])[1];
const create = (name, { own, rid = uuid(), session = 'editor' } = {}) => call('POST', '/api/brands', { body: { request_id: rid, name, ...(own !== undefined ? { own } : {}) }, session });

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('ブランド・商品の段階 (0088)');

await ta('[D1] 0088 の形と権限: 表・関数 (security definer・search_path 固定・public の実行権なし)・画面のロール = select と関数の実行だけ', async () => {
  for (const t of ['core.brands', 'core.product_brands', 'core.product_lifecycle', 'ops.product_brand_events', 'ops.brand_requests', 'ops.brand_flag_events', 'ops.product_lifecycle_events']) {
    assert.ok((await one('select to_regclass($1) is not null as ok', [t])).ok, t);
  }
  const fns = await q(`select p.proname as n, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p
    join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname in ('create_brand', 'set_brand_own', 'set_product_brand', 'set_product_lifecycle', 'sku_first_sold', 'sales_records_from') order by 1`);
  assert.deepEqual(fns.map((f) => [f.n, f.d, f.pub]), ['create_brand', 'sales_records_from', 'set_brand_own', 'set_product_brand', 'set_product_lifecycle', 'sku_first_sold'].map((n) => [n, true, false]));
  for (const f of fns) assert.match(f.c, /search_path=pg_catalog, pg_temp/, f.n);
  for (const f of ROLES.BRAND_LIFE_OWNER_ONLY_FUNCTIONS) {
    assert.equal((await one("select has_function_privilege('public', $1::regprocedure, 'execute') as ok", [f])).ok, false, `public が ${f} を実行できる`);
    assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [f])).ok, false, `master_edit が ${f} を実行できる`);
  }
  for (const t of ROLES.BRAND_LIFE_SELECT) {
    const p = await one(`select has_table_privilege('master_edit', $1, 'select') as s, has_table_privilege('master_edit', $1, 'insert') as i, has_table_privilege('master_edit', $1, 'update') as u,
      has_table_privilege('master_edit', $1, 'delete') as d, has_table_privilege('master_edit', $1, 'truncate') as tr`, [t]);
    assert.deepEqual(p, { s: true, i: false, u: false, d: false, tr: false }, t);
    for (const r of ['master_ops', 'master_observer', 'master_gate']) assert.equal((await one(`select has_table_privilege($1, $2, 'select') as s`, [r, t])).s, false, `${r} が ${t} を読める`);
  }
  assert.equal((await one("select has_table_privilege('master_edit', 'ops.brand_requests', 'select') as s")).s, false, '要求の控えを読める');
  for (const f of ROLES.BRAND_LIFE_EDIT_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [f])).ok, true, f);
  // 注文・出荷の表は画面のロールに読ませない (初めて売った日は関数で)
  for (const t of ['core.orders', 'core.order_lines', 'core.shipments', 'core.shipment_lines']) assert.equal((await one(`select has_table_privilege('master_edit', $1, 'select') as s`, [t])).s, false, t);
  assert.equal(await BL.brandLifeReady(db), true);
  assert.deepEqual(await q(`select p.proname from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.prosecdef and n.nspname = 'ops'
    and (p.proname like '%brand%' or p.proname like '%lifecycle%' or p.proname like '%first_sold%') and p.prosrc ~* 'temp(orary)?\\s+table'`), []);
});

await ta('[D2] 守り: 関数の外では書けない (持ち主のロールでも)・画面のロールは直接書けない・記録は追記だけ・TRUNCATE できない・名前の形', async () => {
  await assert.rejects(pg.query("insert into core.brands (name, name_norm, created_by_id, created_request_id) values ('直接', '直接', 'x@test', gen_random_uuid())"), /ops.create_brand/);
  await assert.rejects(asEditor(() => pg.query("insert into core.brands (name, name_norm, created_by_id, created_request_id) values ('直接', '直接', 'x@test', gen_random_uuid())")), /permission denied/);
  const pid = (await one("select product_id::text as id from core.skus where code = 's001'")).id;
  await assert.rejects(pg.query("insert into core.product_lifecycle (product_id, stage, set_by, set_via, request_id) values ($1, 'staple', 'x@test', 'sku_page', gen_random_uuid())", [pid]), /ops.create_brand|関数の中/);
  for (const t of ['core.brands', 'core.product_brands', 'core.product_lifecycle', 'ops.product_brand_events', 'ops.product_lifecycle_events']) await assert.rejects(pg.query(`truncate ${t}`), /./, t);
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.brand_life_protocol', '1', true)");
    await assert.rejects(pg.query("insert into core.brands (name, name_norm, created_by_id, created_request_id) values ('はちみつ', 'はちみつ', 'x@test', gen_random_uuid())"), /ck_brands_norm/);
  } finally { await pg.query('rollback'); }
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.brand_life_protocol', '1', true)");
    await assert.rejects(pg.query("insert into core.product_lifecycle (product_id, set_by, set_via, request_id) values ($1, 'x@test', 'sku_page', gen_random_uuid())", [pid]), /ck_product_lifecycle_some/);
  } finally { await pg.query('rollback'); }
});

let OWN = null; let OTHER = null;
await ta('[C1] 名簿に足す: 足す・自社ブランドとして足す・同じと見る名前 = 足さずにそれ (印も名簿のまま)・押し直し = 前の答え・同じ番号で違う中身 = 409', async () => {
  const rid = uuid();
  const a = await create('はちみつ工房', { own: true, rid });
  assert.equal(a.status, 200, a.text);
  assert.deepEqual([a.j.brand.name, a.j.brand.own, a.j.existed, a.j.replayed], ['はちみつ工房', true, false, false]);
  OWN = a.j.brand;
  // 同じと見る名前 (カタカナ・半角カナ・空白) = 足さない・自社でないと言っても名簿の印のまま
  for (const name of ['ハチミツ工房', 'ﾊﾁﾐﾂ 工房', '　はちみつ　工房 ']) {
    const b = await create(name, { own: false });
    assert.deepEqual([b.status, b.j.brand.id, b.j.brand.name, b.j.brand.own, b.j.existed], [200, OWN.id, 'はちみつ工房', true, true], name);
  }
  const again = await create('はちみつ工房', { own: true, rid });
  assert.deepEqual([again.status, again.j.brand.id, again.j.replayed], [200, OWN.id, true]);
  // 同じ番号で違う名前・違う印 = 409
  assert.deepEqual([(await create('別のブランド', { own: true, rid })).j.reason, (await create('はちみつ工房', { own: false, rid })).j.reason], ['request_id_reused', 'request_id_reused']);
  assert.equal((await one("select count(*)::int as n from core.brands where name = '別のブランド'")).n, 0);
  const o = await create('  よそ の　ブランド ');
  assert.deepEqual([o.status, o.j.brand.name, o.j.brand.own], [200, 'よそ の ブランド', false]);
  OTHER = o.j.brand;
  // メーカーの名簿とは別 (同じ名前でも足せる・メーカーの名簿は増えない)
  const mk = (await one('select count(*)::int as n from core.makers')).n;
  assert.equal((await create('三河テキスタイル')).status, 200);
  assert.equal((await one('select count(*)::int as n from core.makers')).n, mk);
});

await ta('[C2] 名簿に足す: 形 (空・101 字・改行・own の形) = 400・名簿の人だけ (403)・Origin が無い = 403', async () => {
  for (const name of ['', '   ', 'x'.repeat(101), 'あ\nい', null]) {
    const r = await create(name);
    assert.deepEqual([r.status, r.j.reason, r.j.field], [400, 'invalid_input', 'brand'], String(name));
  }
  assert.deepEqual([(await create('形', { own: 'yes' })).status], [400]);
  assert.equal((await create('見る人のブランド', { session: 'viewer' })).status, 403);
  assert.equal((await call('POST', '/api/brands', { body: { request_id: uuid(), name: 'Origin 無し' }, origin: false })).status, 403);
});

await ta('[O1] 自社ブランドの印: 変える (記録)・押し直し = 前の答え・見ていた印と違う = 409・同じ = 変えない・名簿の人だけ', async () => {
  const rid = uuid();
  const a = await call('POST', '/api/brands/own', { body: { request_id: rid, brand_id: OTHER.id, own: true, seen_own: false } });
  assert.deepEqual([a.status, a.j.brand.own, a.j.changed], [200, true, true], a.text);
  const again = await call('POST', '/api/brands/own', { body: { request_id: rid, brand_id: OTHER.id, own: true, seen_own: false } });
  assert.deepEqual([again.status, again.j.replayed, again.j.brand.own], [200, true, true]);
  const c = await call('POST', '/api/brands/own', { body: { request_id: uuid(), brand_id: OTHER.id, own: false, seen_own: false } });
  assert.deepEqual([c.status, c.j.reason], [409, 'version_conflict']);
  const same = await call('POST', '/api/brands/own', { body: { request_id: uuid(), brand_id: OTHER.id, own: true, seen_own: true } });
  assert.deepEqual([same.status, same.j.changed], [200, false]);
  const back = await call('POST', '/api/brands/own', { body: { request_id: uuid(), brand_id: OTHER.id, own: false, seen_own: true } });
  assert.deepEqual([back.status, back.j.brand.own], [200, false]);
  assert.deepEqual((await q('select from_own, to_own, changed, actor_id from ops.brand_flag_events where brand_id = $1::bigint order by event_id', [OTHER.id])).map((e) => [e.from_own, e.to_own, e.changed, e.actor_id]),
    [[false, true, true, 'naka@test'], [true, true, false, 'naka@test'], [true, false, true, 'naka@test']]);   // 変えなかった要求も残す (changed = false)
  assert.equal((await call('POST', '/api/brands/own', { body: { request_id: uuid(), brand_id: OTHER.id, own: true, seen_own: false }, session: 'viewer' })).status, 403);
  assert.equal((await call('POST', '/api/brands/own', { body: { request_id: uuid(), brand_id: '999999', own: true, seen_own: false } })).j.reason, 'brand_unknown');
  // 🆕 #brand Codex R1 M: 変えなかった要求 (もうその印) も残す = 応答が届かずに押し直しても、その間にほかの人が戻した印を上書きしない
  const late = uuid();
  const t1 = await call('POST', '/api/brands/own', { body: { request_id: uuid(), brand_id: OTHER.id, own: true, seen_own: false } });
  assert.equal(t1.j.changed, true);
  const t2 = await call('POST', '/api/brands/own', { body: { request_id: late, brand_id: OTHER.id, own: true, seen_own: false } });   // 同時に押した後着 = 変えない
  assert.deepEqual([t2.status, t2.j.changed], [200, false]);
  const t3 = await call('POST', '/api/brands/own', { body: { request_id: uuid(), brand_id: OTHER.id, own: false, seen_own: true } });   // ほかの人が戻す
  assert.equal(t3.j.changed, true);
  const t4 = await call('POST', '/api/brands/own', { body: { request_id: late, brand_id: OTHER.id, own: true, seen_own: false } });   // 後着の押し直し = 前の答え
  assert.deepEqual([t4.status, t4.j.replayed, t4.j.changed], [200, true, false]);
  assert.equal((await one('select own_brand from core.brands where brand_id = $1::bigint', [OTHER.id])).own_brand, false, '押し直しで戻した印を上書きした');
});

await ta('[R1] 単品の登録: ブランド・区分・終売の予定日を付ける (同じ取引・記録 via register)・押し直し = 2 回書かない・🚨 「値か入力なし」に数えない', async () => {
  const b = single('bl-r1', { brand: OWN.id, life_stage: 'seasonal', end_planned_on: '2027-03-31' });
  for (const k of ['brand', 'life_stage', 'end_planned_on']) assert.ok(!b.blanks.includes(k), `入力なしの印に ${k}`);
  assert.ok(!RB.BLANK_KEYS.single.some((k) => ['brand', 'life_stage', 'end_planned_on'].includes(k)) && !RB.BLANK_KEYS.variation.some((k) => ['brand', 'life_stage', 'end_planned_on'].includes(k)));
  const r = await call('POST', '/api/new', { body: b });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual(r.j.brand, { id: OWN.id, name: 'はちみつ工房', own: true });
  assert.deepEqual(r.j.life, { stage: 'seasonal', end_planned_on: '2027-03-31' });
  assert.deepEqual(await lifeOf('bl-r1'), { brand: 'はちみつ工房', own: true, stage: 'seasonal', end_on: '2027-03-31' });
  assert.deepEqual((await brandEvents('bl-r1')).map((e) => [e.f, e.t, e.via]), [[null, OWN.id, 'register']]);
  assert.deepEqual((await lifeEvents('bl-r1')).map((e) => [e.fs, e.ts, e.fe, e.te, e.via]), [[null, 'seasonal', null, '2027-03-31', 'register']]);
  const again = await call('POST', '/api/new', { body: b });
  assert.deepEqual([again.status, again.j.replayed], [200, true]);
  assert.equal((await brandEvents('bl-r1')).length, 1);
  assert.equal((await lifeEvents('bl-r1')).length, 1);
  // 無くても通る (どれも入れない = 行が無い)・終売の予定日だけ = 区分なしの行
  const n = await call('POST', '/api/new', { body: single('bl-r1b') });
  assert.equal(n.status, 200, n.text);
  assert.deepEqual(await lifeOf('bl-r1b'), { brand: null, own: null, stage: null, end_on: null });
  const e = await call('POST', '/api/new', { body: single('bl-r1c', { end_planned_on: '2026-12-31', brand: '', life_stage: '' }) });
  assert.equal(e.status, 200, e.text);
  assert.deepEqual(await lifeOf('bl-r1c'), { brand: null, own: null, stage: null, end_on: '2026-12-31' });
});

await ta('[R2] 単品の登録の断り: 名簿に無いブランド・番号の形・区分の形・日付の形 = 400 (何も作らない)・セットは聞かない', async () => {
  const cases = [[{ brand: '999999' }, 'brand_unknown', 'brand'], [{ brand: 'abc' }, 'invalid_input', 'brand'], [{ life_stage: 'forever' }, 'invalid_input', 'life_stage'],
    [{ end_planned_on: '2026-02-30' }, 'invalid_input', 'end_planned_on'], [{ end_planned_on: '1999-12-31' }, 'invalid_input', 'end_planned_on'], [{ end_planned_on: '2026/12/31' }, 'invalid_input', 'end_planned_on']];
  let i = 0;
  for (const [extra, reason, field] of cases) {
    const r = await call('POST', '/api/new', { body: single(`bl-r2-${i++}`, extra) });
    assert.deepEqual([r.status, r.j.reason, r.j.field], [400, reason, field], JSON.stringify(extra));
  }
  assert.equal((await one("select count(*)::int as n from core.skus where code like 'bl-r2-%'")).n, 0, '断った登録で商品ができた');
  const s = await call('POST', '/api/new', { body: single('blset2', { brand: OWN.id }, { kind: 'set' }) });
  assert.deepEqual([s.status, s.j.field], [400, 'brand']);
  const s2 = await call('POST', '/api/new', { body: single('blset1', {}, { kind: 'set' }) });
  assert.equal(s2.status, 200, s2.text);
});

await ta('[V1] まとまりの登録: 全部の子に同じブランド・区分・終売の予定日 (記録は子ごと・via variation)・今あるまとまりを読む = 子の値', async () => {
  const r = await call('POST', '/api/new/variation', { body: variation('blv1', { brand: OTHER.id, life_stage: 'trial', end_planned_on: '2027-01-15' }) });
  assert.equal(r.status, 200, r.text);
  for (const c of ['blv1-A', 'blv1-B']) {
    assert.deepEqual(await lifeOf(c), { brand: 'よそ の ブランド', own: false, stage: 'trial', end_on: '2027-01-15' }, c);
    assert.deepEqual((await brandEvents(c)).map((e) => e.via), ['variation']);
    assert.deepEqual((await lifeEvents(c)).map((e) => e.via), ['variation']);
  }
  const V = await import('../lib/master-variation.mjs');
  const gid = (await one("select product_id::text as id from core.products where display_code = 'blv1'")).id;
  const grp = await asEditor(() => V.readVariationGroup(db, gid));
  assert.deepEqual(grp.children.map((k) => [k.values.brand, k.values.life_stage, k.values.end_planned_on]), [[OTHER.id, 'trial', '2027-01-15'], [OTHER.id, 'trial', '2027-01-15']]);
  const r2 = await call('POST', '/api/new/variation', { body: variation('blv2', { brand: '999999' }) });
  assert.deepEqual([r2.status, r2.j.reason], [400, 'brand_unknown']);
  assert.equal((await one("select count(*)::int as n from core.skus where code like 'blv2-%'")).n, 0);
  const r3 = await call('POST', '/api/new/variation', { body: variation('blv3') });
  assert.equal(r3.status, 200, r3.text);
  assert.deepEqual(await lifeOf('blv3-A'), { brand: null, own: null, stage: null, end_on: null });
});

await ta('[S1] 商品の画面の保存: 板 (仕入先の板の下・別の板)・付ける → 変える → 外す (記録・via sku_page)・前 → 後・押し直し = 前の答え・変わらない = 書かない', async () => {
  let page = await call('GET', '/sku/s001');
  assert.equal(page.status, 200);
  const iSup = page.text.indexOf('aria-labelledby="h-sup"'), iBl = page.text.indexOf('id="sec-brandlife"'), iRow = page.text.indexOf('data-row="brand"');
  assert.ok(iSup > 0 && iBl > iSup && iRow > iBl, 'ブランドの板が仕入先の板の下に無い');
  assert.match(page.text, /id="f-brand-in"[^>]*role="combobox"/);
  assert.match(page.text, /id="f-life_stage"[^>]*data-field="life_stage"/);
  assert.match(page.text, /type="date" id="f-end_planned_on"/);
  assert.match(page.text, /id="first-sold"><b>まだ売れていません<\/b>/);
  assert.match(page.text, /public\/me-maker\.js/);
  const data = JSON.parse(/<script type="application\/json" id="me-brand">([\s\S]*?)<\/script>/.exec(page.text)[1]);
  assert.ok(data.items.some(([id, n, own]) => id === OWN.id && n === 'はちみつ工房' && own === true));
  const rid = uuid();
  const body = { request_id: rid, seen: { token: tok(page.text) }, values: { brand: OWN.id, life_stage: 'staple' } };
  const a = await call('POST', '/api/sku/s001', { body });
  assert.equal(a.status, 200, a.text);
  assert.deepEqual(a.j.changed.map((c) => [c.field, c.from, c.to, c.ne]), [['brand', null, 'はちみつ工房', 'none'], ['life_stage', null, '定番', 'none']]);
  assert.deepEqual(a.j.ne_steps, []);
  assert.deepEqual([(await call('POST', '/api/sku/s001', { body })).j.replayed], [true]);
  page = await call('GET', '/sku/s001');
  assert.match(page.text, /id="brand-head">ブランド はちみつ工房 \(自社\)</);
  assert.match(page.text, /<option value="staple" selected>定番<\/option>/);
  // 終売の予定日だけ足す (区分はそのまま)・あと N 日の札
  const soon = addDays(today, 10);
  const b = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { end_planned_on: soon, life_stage: 'staple' } } });
  assert.deepEqual(b.j.changed.map((c) => [c.field, c.from, c.to]), [['end_planned_on', null, soon]]);
  assert.deepEqual(await lifeOf('s001'), { brand: 'はちみつ工房', own: true, stage: 'staple', end_on: soon });
  page = await call('GET', '/sku/s001');
  assert.match(page.text, /id="end-badge">あと 10 日</);
  // ブランドを変える・区分を外す (予定日は残る)・全部外す (行が消える)
  const c = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { brand: OTHER.id, life_stage: '' } } });
  assert.deepEqual(c.j.changed.map((x) => [x.field, x.from, x.to]), [['brand', 'はちみつ工房', 'よそ の ブランド'], ['life_stage', '定番', null]]);
  assert.deepEqual(await lifeOf('s001'), { brand: 'よそ の ブランド', own: false, stage: null, end_on: soon });
  page = await call('GET', '/sku/s001');
  const d = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { brand: '', life_stage: '', end_planned_on: '' } } });
  assert.deepEqual(d.j.changed.map((x) => x.field), ['brand', 'end_planned_on']);
  assert.deepEqual(await lifeOf('s001'), { brand: null, own: null, stage: null, end_on: null });
  assert.equal((await one("select count(*)::int as n from core.product_lifecycle l join core.skus k on k.product_id = l.product_id where k.code = 's001'")).n, 0, '空にしたのに行が残る');
  assert.deepEqual((await brandEvents('s001')).map((e) => [e.f, e.t, e.via]), [[null, OWN.id, 'sku_page'], [OWN.id, OTHER.id, 'sku_page'], [OTHER.id, null, 'sku_page']]);
  assert.deepEqual((await lifeEvents('s001')).map((e) => [e.fs, e.ts, e.fe, e.te]), [[null, 'staple', null, null], ['staple', 'staple', null, soon], ['staple', null, soon, soon], [null, null, soon, null]]);
  page = await call('GET', '/sku/s001');
  const e = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { brand: '', life_stage: '', end_planned_on: '' } } });
  assert.equal(e.j.no_change, true);
});

await ta('[S2] ほかの人が変えた後 = 409 (編集の印)・DB の関数は門を通った取引の中だけ・見ていた値と照らす・無い商品の印は前と同じ・形の誤り = 400', async () => {
  const page = await call('GET', '/sku/s002');
  const old = tok(page.text);
  const W = await import('../lib/master-write.mjs');
  const id2 = (await one("select sku_id::text as id from core.skus where code = 's002'")).id;
  const cur0 = await asEditor(() => W.readCurrent(db, id2, today));
  assert.deepEqual([cur0.brand, cur0.lifecycle], [null, null]);
  assert.equal(W.editTokenOf({ ...cur0, brand: undefined, lifecycle: undefined }), W.editTokenOf(cur0), 'ブランド・段階の無い商品の印が変わった');
  const a = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: old }, values: { life_stage: 'trial' } } });
  assert.equal(a.status, 200, a.text);
  const b = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: old }, values: { brand: OWN.id } } });
  assert.deepEqual([b.status, b.j.reason], [409, 'version_conflict']);
  assert.deepEqual(await lifeOf('s002'), { brand: null, own: null, stage: 'trial', end_on: null });
  await asEditor(async () => {
    for (const via of ['sku_page', 'register', 'variation']) {
      await pg.query('begin');
      try { await assert.rejects(pg.query('select ops.set_product_brand($1::uuid, $2, $3::bigint, $4::bigint, null, $5, null)', [uuid(), 'naka@test', id2, OWN.id, via]), /master_write_session_required/, via); }
      finally { await pg.query('rollback'); }
      await pg.query('begin');
      try { await assert.rejects(pg.query("select ops.set_product_lifecycle($1::uuid, $2, $3::bigint, 'staple', null, $4, null, $5, null)", [uuid(), 'naka@test', id2, via === 'sku_page' ? 'trial' : null, via]), /master_write_session_required/, via); }
      finally { await pg.query('rollback'); }
    }
    // 保存の約束 (この SKU) の中でも、見ていた値が今と違えば断る
    const cur = await W.readCurrent(db, id2, today);
    await pg.query('begin');
    try {
      await pg.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)',
        [uuid(), 'naka@test', null, T.OWN, 'sku_edit', id2, W.editTokenOf(cur), 'a'.repeat(64), JSON.stringify(W.editVersionsOf(cur))]);
      await assert.rejects(pg.query("select ops.set_product_lifecycle($1::uuid, 'naka@test', $2::bigint, 'staple', null, null, null, 'sku_page', null)", [uuid(), id2]), /version_conflict/);
    } finally { await pg.query('rollback'); }
  });
  const p2 = await call('GET', '/sku/s002');
  for (const [values, field] of [[{ brand: '999999' }, 'brand'], [{ life_stage: 'x' }, 'life_stage'], [{ end_planned_on: '2026-13-01' }, 'end_planned_on']]) {
    const r = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: tok(p2.text) }, values } });
    assert.deepEqual([r.status, r.j.field], [400, field], JSON.stringify(values));
  }
  // セットには付けない (DB の関数)・セットの画面には板が無い
  const setId = (await one("select sku_id::text as id from core.skus where code = 'blset1'")).id;
  await asEditor(async () => {
    await pg.query('begin');
    try { await assert.rejects(pg.query("select ops.set_product_brand($1::uuid, 'naka@test', $2::bigint, $3::bigint, null, 'sku_page', null)", [uuid(), setId, OWN.id]), /not_single/); }
    finally { await pg.query('rollback'); }
  });
  const sp = await call('GET', '/sku/blset1');
  assert.ok(!/id="sec-brandlife"/.test(sp.text));
});

await ta('[F1] 初めて売った日: NE の出荷 (受注日・FBA 納品 / 取り消しは数えない)・モールの注文 (出品の構成・セットの構成品・取り消しは数えない)・いちばん早い日と出どころ・商品の画面', async () => {
  const sku = async (code) => (await one('select sku_id::text as id from core.skus where code = $1', [code])).id;
  const [s002, s003, ws] = [await sku('s002'), await sku('s003'), await sku('ws100-BR')];
  let seq = 0;
  const ship = async ({ shop, orderDate, shipDate, lines, cancelled = false, lineCancelled = false }) => {
    const sid = (await one(`insert into core.shipments (company_id, ne_slip_no, shop_code, status, is_cancelled, shipped_at, ship_date_jst, order_date_jst, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, $1, $2, $3, $4, $5::date::timestamptz, $5::date, $6::date, 1, now(), 't', 'h') returning shipment_id::text as id`,
    [`SLIP-${++seq}`, shop, cancelled ? 'cancelled' : 'shipped', cancelled, shipDate, orderDate])).id;
    for (const [i, [id, qty]] of lines.entries()) {
      await pg.query('insert into core.shipment_lines (company_id, shipment_id, line_no, sku_id, qty, is_cancelled, received_batch_seq) values (1, $1, $2, $3, $4, $5, 1)', [sid, String(i + 1), id, qty, lineCancelled]);
    }
  };
  const order = async ({ mall = 'rakuten', date, skuId = null, listingId = null, qty = 1, cancelledQty = 0, cancelled = false }) => {
    const oid = (await one(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, $1, $2, $3, 'mall_api', $4::date::timestamptz, $4::date, $5, $6, 1, now(), 't', 'h') returning order_id::text as id`,
    [mall, mall === 'amazon' ? 'jp' : 'main', `O-${++seq}`, date, cancelled ? 'cancelled' : 'shipped', cancelled])).id;
    await pg.query('insert into core.order_lines (company_id, order_id, line_key, sku_id, listing_id, qty, cancelled_qty, received_batch_seq) values (1, $1, $2, $3, $4, $5, $6, 1)', [oid, `L-${seq}`, skuId, listingId, qty, cancelledQty]);
  };
  // s002: FBA 納品 (店 15) が一番早い = 数えない・取り消した伝票 / 明細も数えない・楽天の出荷 (受注日 2025-03-01・出荷 2025-03-03)
  await ship({ shop: '15', orderDate: '2025-01-20', shipDate: '2025-01-20', lines: [[s002, 10]] });
  await ship({ shop: '1', orderDate: '2025-02-01', shipDate: '2025-02-02', lines: [[s002, 1]], cancelled: true });
  await ship({ shop: '1', orderDate: '2025-02-10', shipDate: '2025-02-11', lines: [[s002, 1]], lineCancelled: true });
  await ship({ shop: '1', orderDate: '2025-03-01', shipDate: '2025-03-03', lines: [[s002, 2]] });
  // 取り消した注文・全部取り消した明細は数えない
  await order({ date: '2025-02-15', skuId: s002, cancelled: true });
  await order({ date: '2025-02-16', skuId: s002, qty: 1, cancelledQty: 1 });
  // s003: 出荷は 2025-06・Amazon の出品 (FBA のお客の注文 = 出荷の記録に無い) が 2025-05-05 = 注文が先
  await ship({ shop: '1', orderDate: '2025-06-10', shipDate: '2025-06-11', lines: [[s003, 1]] });
  const lid = (await one(`insert into core.listings (company_id, mall, shop_code, listing_code) values (1, 'rakuten', '', 'bl-listing-s003') returning listing_id::text as id`)).id;
  await pg.query("insert into core.listing_components (company_id, listing_id, sku_id, qty, resolution, resolved_by_type) values (1, $1, $2, 1, 'manual', 'human')", [lid, s003]);
  await order({ date: '2025-05-05', listingId: lid });
  // ws100-BR: セット (構成品に ws100-BR) の注文 = 構成品の初めて売った日
  const setSku = (await one(`select k.sku_id::text as id from core.skus k where k.code = 'blset1'`)).id;
  await pg.query("insert into core.sku_components (company_id, parent_sku_id, child_sku_id, qty, source) values (1, $1, $2, 1, 'manual') on conflict do nothing", [setSku, ws]);
  await order({ mall: 'yahoo', date: '2025-08-08', skuId: setSku });
  // 🆕 #brand Codex R1 M: SKU の分かる明細 (s001) に出品 (構成 = s003) も付いている = s003 に数えない (出品の構成へ進むのは SKU の分からない明細だけ)
  {
    const oid = (await one(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, 'rakuten', 'main', 'O-both', 'mall_api', '2025-04-01'::date::timestamptz, '2025-04-01', 'shipped', 1, now(), 't', 'h') returning order_id::text as id`)).id;
    await pg.query('insert into core.order_lines (company_id, order_id, line_key, sku_id, listing_id, qty, received_batch_seq) values (1, $1, $2, $3, $4, 1, 1)', [oid, 'L-both', await sku('s001'), lid]);
  }
  // 記録の始まりの近く = 2025-01-05 の出荷 (ほかの商品)
  const s001 = await sku('s001');
  await ship({ shop: '10', orderDate: '2025-01-05', shipDate: '2025-01-06', lines: [[s001, 3]] });
  const towel = await sku('towel-gift');
  const got = await asEditor(() => BL.firstSoldOf(db, [s001, s002, s003, ws, towel]));
  assert.deepEqual(Object.fromEntries([...got].map(([k, v]) => [k, [v.date, v.via, v.detail]])), {
    [s001]: ['2025-01-05', 'shipment', '卸'], [s002]: ['2025-03-01', 'shipment', '雑貨イズム楽天市場店'], [s003]: ['2025-05-05', 'order', 'rakuten'], [ws]: ['2025-08-08', 'order', 'yahoo'],
  });
  const from = await asEditor(() => BL.salesRecordsFrom(db));
  assert.deepEqual(from, { shipment: '2025-01-06', order: '2025-02-15' });
  const v1 = BL.firstSoldView(got.get(s001), from);
  assert.deepEqual([v1.text, v1.source, v1.beforeRecords], ['2025/01/05', 'NE の出荷 (卸)', true]);
  assert.match(v1.note, /それより前から売っていたかもしれません/);
  // s002 (2025-03-01) = モールの注文の記録の始まり (2025-02-15) から 14 日の中 = これより前かも / s003 (2025-05-05) = 遠い
  assert.equal(BL.firstSoldView(got.get(s002), from).beforeRecords, true);
  const v2 = BL.firstSoldView(got.get(s003), from);
  assert.deepEqual([v2.text, v2.source, v2.beforeRecords], ['2025/05/05', '楽天の注文', false]);
  assert.deepEqual([BL.firstSoldView(null, from).text], ['まだ売れていません']);
  // 🆕 #brand Codex R1 L: 記録の始まりより前の日 = その記録の「すぐ後」ではない (出荷 2025-01-01〜・注文 2026-01-01〜・初めて 2025-06-01 = 札なし)
  assert.equal(BL.firstSoldView({ date: '2025-06-01', via: 'shipment', detail: '卸' }, { shipment: '2025-01-01', order: '2026-01-01' }).beforeRecords, false);
  assert.equal(BL.firstSoldView({ date: '2026-01-10', via: 'order', detail: 'amazon' }, { shipment: '2025-01-01', order: '2026-01-01' }).beforeRecords, true);
  // 商品の画面
  const page = await call('GET', '/sku/s003');
  assert.match(page.text, /id="first-sold"><b>2025\/05\/05<\/b>/);
  assert.match(page.text, /楽天の注文から/);
  assert.match(page.text, /記録: 出荷 2025\/01\/06〜 · モールの注文 2025\/02\/15〜/);
  const p1 = await call('GET', '/sku/s001');
  assert.match(p1.text, /id="first-sold-early">これより前かも</);
  // 5,000 個を超える = 断る
  await asEditor(() => assert.rejects(pg.query('select * from ops.sku_first_sold(array(select generate_series(1, 5001)::bigint))'), /5,000/));
});

await ta('[L1] 一覧の列 (いつもの列に出さない・メーカーの後ろ)・絞り込み (終売の予定が近い・予定日あり・区分・区分なし)・詳細検索の印でも残る・CSV の最後の 4 列', async () => {
  for (const id of ['brand', 'stage', 'end_on']) { assert.ok(COLS.COLUMN_IDS.includes(id)); assert.ok(!COLS.DEFAULT_VIEW.shown.includes(id)); assert.ok(!COLS.SORTABLE.includes(id)); }
  assert.deepEqual(COLS.COLUMN_IDS.slice(-5), ['maker', 'pkg', 'brand', 'stage', 'end_on']);   // 🆕 0087 の大きさ・重さの列はメーカーのすぐ後ろ
  // 段階を付けておく: bl-r1 = 季節・2027-03-31 / blv1-* = 試し・2027-01-15 / s002 = 試し / 近い = towel-gift (今日 + 30) / 過ぎた = towel-gift-2p (今日 − 3)
  const setLife = async (code, values) => {
    const page = await call('GET', `/sku/${code}`);
    const r = await call('POST', `/api/sku/${code}`, { body: { request_id: uuid(), seen: { token: tok(page.text) }, values } });
    assert.equal(r.status, 200, r.text);
  };
  await setLife('towel-gift', { end_planned_on: addDays(today, 30), brand: OWN.id });
  await setLife('towel-gift-2p', { end_planned_on: addDays(today, -3) });
  await setLife('towel-gift-3p', { end_planned_on: addDays(today, BL.END_SOON_DAYS + 1) });
  const codes = async (f) => (await asEditor(() => RD.listSkus(db, RD.normalizeFilters(f), { mode: 'all' }))).rows.map((r) => r.code).sort();
  assert.deepEqual(await codes({ life: 'end_soon' }), ['towel-gift', 'towel-gift-2p']);
  assert.deepEqual(await codes({ life: 'end_set' }), ['bl-r1', 'bl-r1c', 'blv1-A', 'blv1-B', 'towel-gift', 'towel-gift-2p', 'towel-gift-3p']);
  assert.deepEqual(await codes({ life: 'trial' }), ['blv1-A', 'blv1-B', 's002']);
  assert.deepEqual(await codes({ life: 'seasonal' }), ['bl-r1']);
  const none = await codes({ life: 'stage_none', kind: 'single' });
  assert.ok(none.includes('s001') && none.includes('towel-gift') && !none.includes('s002') && !none.includes('bl-r1'), none.join(','));
  assert.ok(!(await codes({ life: 'stage_none' })).includes('blset1'), 'セットが区分なしに入る');
  assert.equal(RD.normalizeFilters({ life: 'nope' }).life, '');
  // 一覧の画面 (列は描いて hidden・札と絞り込みの欄)
  const list = await call('GET', '/?life=end_soon');
  assert.equal(list.status, 200);
  assert.match(list.text, /<td data-col="brand" hidden>はちみつ工房 <span class="b ok">自社<\/span><\/td>/);
  assert.match(list.text, /<td data-col="end_on" hidden>[0-9/]+ <span class="b warn">あと 30 日<\/span><\/td>/);
  assert.match(list.text, /<span class="b err">過ぎた<\/span>/);
  assert.match(list.text, /<a class="chip on t-warn" href="[^"]*"[^>]*>終売の予定が近い/);
  assert.match(list.text, /<select class="in" name="life" id="f-life"[^>]*>/);
  // 詳細検索の印 (POST /api/search) でも残る
  const s = await call('POST', '/api/search', { body: { codes: 'towel-gift', life: 'end_soon' } });
  assert.match(s.j.url, /life=end_soon/);
  // CSV = いちばん後ろに 4 列 (メーカー・🆕 0087 の大きさ・重さの 5 列の後ろ)
  const data = await asEditor(() => RD.listSkus(db, RD.normalizeFilters({ q: 'towel-gift' }), { mode: 'all' }));
  const csv = CSV.buildListCsv(data, {}, {});
  const lines = csv.replace(/^﻿/, '').trim().split('\r\n');
  assert.deepEqual(lines[0].split(',').slice(-5), ['"大きさ・重さの出どころ"', '"ブランド"', '"自社ブランド"', '"定番/季節/試し"', '"終売の予定日"']);
  assert.equal(lines[0].split(',').at(-10), '"メーカー"');
  const row = lines.find((l) => l.startsWith('"towel-gift"'));
  assert.deepEqual(row.split(',').slice(-4), ['"はちみつ工房"', '"自社"', '""', `"${addDays(today, 30).replace(/-/g, '/')}"`]);
  const rowR1 = (await asEditor(() => RD.listSkus(db, RD.normalizeFilters({ q: 'bl-r1' }), { mode: 'all' })));
  const l2 = CSV.buildListCsv(rowR1, {}, {}).replace(/^﻿/, '').trim().split('\r\n').find((l) => l.startsWith('"bl-r1"'));
  assert.deepEqual(l2.split(',').slice(-4), ['"はちみつ工房"', '"自社"', '"季節"', '"2027/03/31"']);
});

await ta('[N1] 新商品の画面: 単品 = ブランド・段階の板 (任意・税・分類・仕入先の次・飛び先の帯)・「あと N つ」に数えない / セット = 無い / まとまり = 共通の欄の塊', async () => {
  const page = await call('GET', '/new?kind=single');
  const iTax = page.text.indexOf('id="sec-tax"'), iBl = page.text.indexOf('id="sec-brandlife"'), iOrder = page.text.indexOf('id="sec-order"');
  assert.ok(iTax > 0 && iBl > iTax && (iOrder < 0 || iOrder > iBl), '板の場所');
  assert.match(page.text, /<a href="#sec-brandlife" data-sec="sec-brandlife"><span class="n">4<\/span> ブランド・段階<\/a>/);
  assert.ok(!/data-none="brand"|data-none="life_stage"|data-none="end_planned_on"/.test(page.text), '入力なしの札がある');
  const pd = JSON.parse(/<script type="application\/json" id="me-new">([\s\S]*?)<\/script>/.exec(page.text)[1]);
  assert.ok(!pd.blankKeys.some((k) => ['brand', 'life_stage', 'end_planned_on'].includes(k)));
  const setPage = await call('GET', '/new?kind=set');
  assert.ok(!/id="sec-brandlife"|id="f-brand-in"/.test(setPage.text));
  const vp = await call('GET', '/new?kind=variation');
  assert.match(vp.text, /id="sub-brandlife"/);
  assert.match(vp.text, /id="f-brand-in"/);
  assert.match(vp.text, /全部の子に同じ値 · 定番 = ずっと売る/);
});

await ta('[Y1] 夜間ロード: 商品の brand の列 (属性の解決) を書いても、ポータルのブランドは変わらない (別の表)・夜間ロードのコードはブランドの表を知らない', async () => {
  const before = await lifeOf('towel-gift');
  assert.equal(before.brand, 'はちみつ工房');
  const plan = basePlan();
  plan.observations = [{ skuCode: 'towel-gift', attribute: 'brand', scope: 'item', valueText: 'モールのブランド', source: 'product_hub', sourceRef: 'draft_page_info:1', observedAt: null }];
  const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
  const lr = await runInitialLoad(db, plan, { log: () => {}, runId: 'load_bl_y1', now: new Date(Date.now() - 3 * 86400e3) });
  assert.equal(lr.ok, true, lr.error);
  assert.equal((await one("select p.brand from core.products p join core.skus k on k.product_id = p.product_id where k.code = 'towel-gift'")).brand, 'モールのブランド');
  assert.deepEqual(await lifeOf('towel-gift'), before);
  const src = fs.readFileSync(new URL('../apps/company-db/load/engine.mjs', import.meta.url), 'utf8') + fs.readFileSync(new URL('../apps/company-db/load/sources.mjs', import.meta.url), 'utf8');
  assert.ok(!/product_brands|core\.brands|product_lifecycle/.test(src));
});

await ta('[B1] ロールの流し直しの前 (master_edit に関数の実行が無い) = 欄を出さない・値を送ったら 503・ほかの保存はそのまま・名簿に足す = 503', async () => {
  for (const f of ROLES.BRAND_LIFE_EDIT_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
  for (const t of ROLES.BRAND_LIFE_SELECT) await pg.query(`revoke select on ${t} from master_edit`);
  try {
    assert.equal(await BL.brandLifeReady(db), false);
    const np = await call('GET', '/new?kind=single');
    assert.ok(!/id="sec-brandlife"|id="me-brand"/.test(np.text), '新商品の画面に欄が出ている');
    const sp = await call('GET', '/sku/s003');
    assert.equal(sp.status, 200);
    assert.ok(!/id="sec-brandlife"/.test(sp.text), '商品の画面に板が出ている');
    const list = await call('GET', '/');
    assert.equal(list.status, 200);
    assert.ok(!/終売の予定が近い/.test(list.text.replace(/<th[^>]*>[\s\S]*?<\/th>/g, '')), '札が出ている');
    const ok = await call('POST', '/api/new', { body: single('bl-b1') });
    assert.equal(ok.status, 200, ok.text);
    const n = await call('POST', '/api/new', { body: single('bl-b2', { life_stage: 'staple' }) });
    assert.deepEqual([n.status, n.j.reason], [503, 'db_not_ready']);
    assert.equal((await one("select count(*)::int as n from core.skus where code = 'bl-b2'")).n, 0);
    const s = await call('POST', '/api/sku/s003', { body: { request_id: uuid(), seen: { token: tok(sp.text) }, values: { reorder_months: '3' } } });
    assert.equal(s.status, 200, s.text);
    const s2 = await call('POST', '/api/sku/s003', { body: { request_id: uuid(), seen: { token: tok((await call('GET', '/sku/s003')).text) }, values: { brand: OWN.id } } });
    assert.deepEqual([s2.status, s2.j.reason], [503, 'db_not_ready']);
    assert.equal((await create('流し直しの前')).status, 503);
    // 一覧の絞り込み = 表を読めない = 当てない (区分なし = 単品の全部)
    const fresh = { query: (t, p) => db.query(t, p) };   // 表を読めるかの控えは接続ごと (要求ごとの db) = 新しい入れ物で聞く
    assert.deepEqual((await asEditor(() => RD.listSkus(fresh, RD.normalizeFilters({ life: 'end_soon' }), { mode: 'all' }))).rows, []);
  } finally {
    await (await import('./company-db/create-master-edit-roles.mjs')).createMasterEditRoles(pg, {});
  }
  assert.equal(await BL.brandLifeReady(db), true, 'ロールを流し直すと戻る');
});

await ta('[I1] 読み込みの順 (#brand Codex R1 M): メーカー・ブランド・保存・登録の部品はどれを先に読み込んでも動く (読み込みの輪で未初期化の値を読まない)・字数の上限はメーカーと同じ', async () => {
  const { execFileSync } = await import('node:child_process');
  for (const m of ['lib/master-maker.mjs', 'lib/master-brand-life.mjs', 'lib/master-write.mjs', 'lib/master-register.mjs', 'lib/master-variation.mjs', 'apps/master-edit/list-csv.mjs']) {
    const url = new URL(`../${m}`, import.meta.url).href;
    const out = execFileSync(process.execPath, ['-e', `import(${JSON.stringify(url)}).then((x) => console.log('ok ' + Object.keys(x).length))`], { encoding: 'utf8' });
    assert.match(out, /^ok \d+/, m);
  }
  const MK = await import('../lib/master-maker.mjs');
  assert.equal(BL.BRAND_NAME_MAX, MK.MAKER_NAME_MAX);
  assert.equal(BL.brandNameNorm('ﾊﾁﾐﾂ 工房'), MK.makerNameNorm('ハチミツ工房'));
});

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
server.close();
await pg.close();
process.exit(process.exitCode || 0);
