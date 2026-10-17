/**
 * test-master-maker.mjs — メーカー (製造元) の名簿と商品ごとのメーカー (中原さん 2026-10-17・Company DB の 0083・lib/master-maker.mjs)
 *
 * Company DB = PGlite (scripts/fixtures/master-variation-db.mjs = 全部の migration を流した新しい DB・切替の後・products.parent も company)。本物の router を HTTP 越しに通す。
 * 固定する契約:
 *   N 名前の形: JS (lib/master-maker.mjs) と DB (core.maker_name_clean / core.maker_name_norm / ops.maker_name_problem) が同じ答え (かな・半角カナ・全角英数・空白)
 *   D 0083 (新しい DB に流す): 表・関数・security definer・search_path・public の実行権なし・守り (関数の外では書けない・記録は追記だけ・TRUNCATE できない)・
 *     画面のロール master_edit = 3 つの表の select と 4 つの関数の実行だけ (insert / update / delete は無い)・ロールの流し直しの前 = 欄を出さない
 *   C 名簿に足す (POST /api/makers): 足す・同じと見る名前 = 足さずにそれを返す (かな・幅・空白)・押し直し = 前の答え・同じ番号で違う名前 = 409・形・名簿の人・Origin
 *   R 単品の登録 (POST /api/new): メーカーを付ける・入力なし (理由に「入力なし: メーカー」)・どちらでもない = 400・両方 = 400・名簿に無い = 400 (何も作らない)・
 *     セットは聞かない・押し直し = 前の答え (2 回書かない)・🆕 0083 の欄が無い DB (ロールの流し直しの前) = 数えない・値を送ったら 503
 *   V まとまりの登録 (POST /api/new/variation): 全部の子に同じメーカー・入力なしも数える・今あるまとまりの読み込み = 子のメーカー
 *   S 商品の画面の保存 (POST /api/sku/:code = saveSku): 付ける・変える・外す・ほかの人が変えた後 = 409 (編集の印)・DB の関数も見ていたメーカーと照らす・押し直し
 *   L 一覧 (メーカーの列は いつもの列に出さない・出せる) と CSV (最後の列「メーカー」)・商品の画面の見出し・欄の場所 (代表の仕入先のすぐ下)
 *   Y 夜間ロード: products.manufacturer (属性の解決) を書いても、ポータルのメーカー (core.product_makers) は変わらない
 * 使い方: node scripts/test-master-maker.mjs
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
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'maker-'));

const { setupVariationDb, basePlan } = await import('./fixtures/master-variation-db.mjs');
const RB = await import('../lib/master-register-blanks.mjs');
const MK = await import('../lib/master-maker.mjs');
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

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/mk';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/mk';
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
/** 単品の登録の本文 (必須でない項目は 値 か 入力なし。maker = 番号 / undefined = メーカーを入力なし) */
function single(code, maker, { blanks = 'auto', kind = 'single' } = {}) {
  const b = { request_id: uuid(), kind, code, values: { ...VALUES, name: `試し ${code}`, ...(maker !== undefined ? { maker } : {}) }, card: { create: false },
    ...(kind === 'single' ? { logizard_barcode: { kind: 'lz_assign' } } : {}) };
  if (kind === 'set') { delete b.values.tax_rate; delete b.values.sales_class; delete b.values.primary_supplier; delete b.values.cost; delete b.values.expiry_managed; delete b.values.inbound_date_managed; b.values.components = [{ code: 's001', qty: '2' }]; }
  b.blanks = blanks === 'auto' ? RB.missingBlankKeys(kind, b, { orderApplicable: true }) : blanks;
  return b;
}
function variation(group, maker) {
  const b = { request_id: uuid(), group: { mode: 'new', code: group, name: `試し ${group}` }, axes: [{ axis: 1, name: 'カラー' }],
    options: [{ axis: 1, code: '-A', name: '色A' }, { axis: 1, code: '-B', name: '色B' }],
    children: ['-A', '-B'].map((c) => ({ code: group + c, choices: { 1: c }, name: `試し ${group}【色${c}】`, jan: '', lz_barcode: { kind: 'lz_assign' } })),
    values: { ...VALUES, ...(maker !== undefined ? { maker } : {}) }, card: { official_url: '', amazon_url: '', asin: '' } };
  b.blanks = RB.missingBlankKeys('variation', b, { orderApplicable: true });
  return b;
}
const makerOf = async (code) => (await q(`select m.maker_id::text as id, m.name from core.skus k join core.product_makers x on x.product_id = k.product_id join core.makers m on m.maker_id = x.maker_id
  where k.code = $1`, [code]))[0] || null;
const eventsOf = async (code) => (await q(`select e.from_maker_id::text as f, e.to_maker_id::text as t, e.via, e.actor_id, e.reason from ops.product_maker_events e join core.skus k on k.sku_id = e.sku_id
  where k.code = $1 order by e.event_id`, [code]));
const tok = (t) => (/data-token="([0-9a-f]{64})"/.exec(t) || [])[1];
const create = (name, rid = uuid(), session = 'editor') => call('POST', '/api/makers', { body: { request_id: rid, name }, session });

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
console.log('メーカー (0083)');

await ta('[N] 名前の形: JS と DB が同じ答え (ひらがな = カタカナ・半角カナ・全角英数・大文字小文字・空白)・問題 (空・長い・改行)', async () => {
  const samples = ['ハチミツ本舗', 'はちみつ 本舗', 'ﾊﾁﾐﾂ本舗', '　はちみつ　　本舗 ', 'ＡＢＣ　Ｃｏ．', 'abc co.', 'ABC CO.', 'ヴィ゙', 'ゝゞ堂', '株式会社 山田'];
  for (const s of samples) {
    const r = await one('select core.maker_name_norm($1) as n, core.maker_name_clean($1) as c, ops.maker_name_problem($1) as p', [s]);
    assert.deepEqual([MK.makerNameNorm(MK.makerNameClean(s)), MK.makerNameClean(s), MK.makerNameProblem(s)], [await (async () => (await one('select core.maker_name_norm(core.maker_name_clean($1)) as n', [s])).n)(), r.c, r.p], s);
  }
  assert.equal(MK.makerNameNorm('はちみつ 本舗'), MK.makerNameNorm('ﾊﾁﾐﾂ本舗'));
  assert.equal(MK.makerNameNorm('ＡＢＣ　Ｃｏ．'), MK.makerNameNorm('abc co.'));
  for (const [s, p] of [['', 'empty'], ['   ', 'empty'], ['a'.repeat(101), 'too_long'], ['あ\nい', 'control'], [null, 'empty']]) {
    assert.equal(MK.makerNameProblem(s), p, String(s));
    assert.equal((await one('select ops.maker_name_problem($1) as p', [s])).p, p, String(s));
  }
  assert.equal(MK.makerNameProblem('a'.repeat(100)), null);
});

await ta('[D1] 0083 を新しい DB に流した形: 表・関数 (security definer・search_path 固定・public の実行権なし)・画面のロールの権限 (読む・関数だけ)', async () => {
  for (const t of ['core.makers', 'core.product_makers', 'ops.product_maker_events']) assert.ok((await one('select to_regclass($1) is not null as ok', [t])).ok, t);
  const fns = await q(`select p.proname as n, p.prosecdef as d, array_to_string(p.proconfig, ',') as c, has_function_privilege('public', p.oid, 'execute') as pub from pg_proc p
    join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'ops' and p.proname in ('create_maker', 'set_product_maker') order by 1`);
  assert.deepEqual(fns.map((f) => [f.n, f.d, f.pub]), [['create_maker', true, false], ['set_product_maker', true, false]]);
  for (const f of fns) assert.match(f.c, /search_path=pg_catalog, pg_temp/, f.n);
  for (const f of ['core.maker_name_norm(text)', 'core.maker_name_clean(text)', 'ops.maker_name_problem(text)', 'ops.guard_maker_tables()']) {
    assert.equal((await one("select has_function_privilege('public', $1::regprocedure, 'execute') as ok", [f])).ok, false, `public が ${f} を実行できる`);
  }
  // master_edit = 3 つの表の select・4 つの関数の実行。insert / update / delete / truncate は無い。名前の問題の部品・守りの関数は無い
  for (const t of ROLES.MAKER_SELECT) {
    const p = await one(`select has_table_privilege('master_edit', $1, 'select') as s, has_table_privilege('master_edit', $1, 'insert') as i, has_table_privilege('master_edit', $1, 'update') as u,
      has_table_privilege('master_edit', $1, 'delete') as d, has_table_privilege('master_edit', $1, 'truncate') as tr`, [t]);
    assert.deepEqual(p, { s: true, i: false, u: false, d: false, tr: false }, t);
    for (const r of ['master_ops', 'master_observer', 'master_gate']) assert.equal((await one(`select has_table_privilege($1, $2, 'select') as s`, [r, t])).s, false, `${r} が ${t} を読める`);
  }
  for (const f of ROLES.MAKER_EDIT_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [f])).ok, true, f);
  for (const f of ROLES.MAKER_OWNER_ONLY_FUNCTIONS) assert.equal((await one("select has_function_privilege('master_edit', $1::regprocedure, 'execute') as ok", [f])).ok, false, f);
  assert.equal(await MK.makerReady(db), true);
  // どの security definer の関数も一時の表を作らない (0083 も)
  assert.deepEqual(await q(`select p.proname from pg_proc p join pg_namespace n on n.oid = p.pronamespace where p.prosecdef and n.nspname = 'ops' and p.proname like '%maker%' and p.prosrc ~* 'temp(orary)?\\s+table'`), []);
});

await ta('[D2] 守り: 関数の外では書けない (持ち主のロールでも)・記録は追記だけ・TRUNCATE できない・画面のロールは表に直接書けない', async () => {
  await assert.rejects(pg.query("insert into core.makers (name, name_norm, created_by_id, created_request_id) values ('直接', '直接', 'x@test', gen_random_uuid())"), /ops.create_maker/);
  await assert.rejects(asEditor(() => pg.query("insert into core.makers (name, name_norm, created_by_id, created_request_id) values ('直接', '直接', 'x@test', gen_random_uuid())")), /permission denied/);
  for (const t of ['core.makers', 'core.product_makers', 'ops.product_maker_events']) await assert.rejects(pg.query(`truncate ${t}`), /./, t);
  // 書く関数は表の CHECK でも名前の形を守る (掃除した名前・同じと見る形)
  for (const [sql, re] of [["insert into core.makers (name, name_norm, created_by_id, created_request_id) values (' 前に空白', ' 前に空白', 'x@test', gen_random_uuid())", /ck_makers_name/],
    ["insert into core.makers (name, name_norm, created_by_id, created_request_id) values ('はちみつ', 'はちみつ', 'x@test', gen_random_uuid())", /ck_makers_norm/]]) {
    await pg.query('begin');
    try {
      await pg.query("select set_config('ops.maker_protocol', '1', true)");
      await assert.rejects(pg.query(sql), re);
    } finally { await pg.query('rollback'); }
  }
});

let HONPO = null;
await ta('[C1] 名簿に足す: 足す → 同じと見る名前 (ひらがな・半角カナ・空白) = 足さずにそれ・押し直し = 前の答え・同じ番号で違う名前 = 409', async () => {
  const rid = uuid();
  const a = await create('はちみつ 本舗', rid);
  assert.equal(a.status, 200, a.text);
  assert.deepEqual([a.j.ok, a.j.maker.name, a.j.existed, a.j.replayed], [true, 'はちみつ 本舗', false, false]);
  HONPO = a.j.maker;
  for (const name of ['ハチミツ本舗', 'ﾊﾁﾐﾂ本舗', '　はちみつ　　本舗 ', 'ハチミツ　本舗']) {
    const b = await create(name);
    assert.deepEqual([b.status, b.j.maker.id, b.j.maker.name, b.j.existed], [200, HONPO.id, 'はちみつ 本舗', true], name);
  }
  const again = await create('はちみつ 本舗', rid);
  assert.deepEqual([again.status, again.j.maker.id, again.j.replayed], [200, HONPO.id, true]);
  const other = await create('別の会社', rid);
  assert.deepEqual([other.status, other.j.reason], [409, 'request_id_reused']);
  assert.equal((await one("select count(*)::int as n from core.makers where name_norm = core.maker_name_norm('ハチミツ本舗')")).n, 1);
  // 名前は掃除してから残す (前後の空白・続いた空白)
  const c = await create('  三河　 テキスタイル  ');
  assert.equal(c.j.maker.name, '三河 テキスタイル');
  assert.equal((await one('select created_by_id from core.makers where maker_id = $1::bigint', [c.j.maker.id])).created_by_id, 'naka@test');
});

await ta('[C2] 名簿に足す: 形 (空・101 字・改行) = 400・名簿の人だけ (403)・Origin が無い = 403・同じ名前を同時に 2 回 = 1 つ', async () => {
  for (const name of ['', '   ', 'x'.repeat(101), 'あ\nい', null]) {
    const r = await create(name);
    assert.deepEqual([r.status, r.j.reason, r.j.field], [400, 'invalid_input', 'maker'], String(name));
  }
  assert.equal((await create('見る人の会社', uuid(), 'viewer')).status, 403);
  const r = await call('POST', '/api/makers', { body: { request_id: uuid(), name: 'Origin 無し' }, origin: false });
  assert.equal(r.status, 403);
  const [x, y] = await Promise.all([create('同時の会社'), create('どうじの会社'.replace('どうじ', '同時'))]);
  assert.deepEqual([x.status, y.status, x.j.maker.id === y.j.maker.id], [200, 200, true]);
});

await ta('[R1] 単品の登録: メーカーを付ける (同じ取引)・記録 (どこから register・理由)・押し直し = 前の答え (2 回書かない)', async () => {
  const b = single('mk-r1', HONPO.id);
  assert.ok(!b.blanks.includes('maker'));
  const r = await call('POST', '/api/new', { body: b });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual(r.j.maker, { id: HONPO.id, name: 'はちみつ 本舗' });
  assert.deepEqual(await makerOf('mk-r1'), { id: HONPO.id, name: 'はちみつ 本舗' });
  const ev = await eventsOf('mk-r1');
  assert.deepEqual(ev.map((e) => [e.f, e.t, e.via, e.actor_id]), [[null, HONPO.id, 'register', 'naka@test']]);
  const again = await call('POST', '/api/new', { body: b });
  assert.equal(again.status, 200, again.text);
  assert.equal(again.j.replayed, true);
  assert.equal((await eventsOf('mk-r1')).length, 1);
  assert.equal((await one("select count(*)::int as n from core.product_makers x join core.skus k on k.product_id = x.product_id where k.code = 'mk-r1'")).n, 1);
});

await ta('[R2] 単品の登録の「値か入力なし」: 入力なし = 通る (理由に「入力なし: メーカー」)・どちらでもない = 400 blank_unmarked・両方 = 400・名簿に無い = 400 (何も作らない)', async () => {
  const b = single('mk-r2');
  assert.ok(b.blanks.includes('maker'));
  const r = await call('POST', '/api/new', { body: b });
  assert.equal(r.status, 200, r.text);
  assert.equal(await makerOf('mk-r2'), null);
  const reason = (await one("select reason from ops.master_registration_events e join core.skus k on k.sku_id = e.sku_id where k.code = 'mk-r2' order by e.event_id limit 1")).reason;
  assert.match(reason, /入力なし: .*メーカー/);
  const nb = single('mk-r2b'); nb.blanks = nb.blanks.filter((k) => k !== 'maker');
  const n1 = await call('POST', '/api/new', { body: nb });
  assert.deepEqual([n1.status, n1.j.reason, n1.j.field, n1.j.blank_key], [400, 'blank_unmarked', 'maker', 'maker']);
  const both = single('mk-r2c', HONPO.id); both.blanks = [...both.blanks, 'maker'];
  const n2 = await call('POST', '/api/new', { body: both });
  assert.deepEqual([n2.status, n2.j.reason, n2.j.field], [400, 'blank_with_value', 'maker']);
  const n3 = await call('POST', '/api/new', { body: single('mk-r2d', '999999') });
  assert.deepEqual([n3.status, n3.j.reason, n3.j.field], [400, 'maker_unknown', 'maker']);
  const n4 = await call('POST', '/api/new', { body: single('mk-r2e', 'abc') });
  assert.deepEqual([n4.status, n4.j.reason, n4.j.field], [400, 'invalid_input', 'maker']);
  assert.equal((await one("select count(*)::int as n from core.skus where code in ('mk-r2b', 'mk-r2c', 'mk-r2d', 'mk-r2e')")).n, 0, '断った登録で商品ができた');
  // セットはメーカーを聞かない (送ったら 400・数えない)
  const sb = single('mkset1', undefined, { kind: 'set' });
  assert.ok(!sb.blanks.includes('maker'));
  const s0 = await call('POST', '/api/new', { body: sb });
  assert.equal(s0.status, 200, s0.text);
  const sb2 = single('mkset2', undefined, { kind: 'set' });
  const s1 = await call('POST', '/api/new', { body: { ...sb2, values: { ...sb2.values, maker: HONPO.id } } });
  assert.deepEqual([s1.status, s1.j.field], [400, 'maker']);
});

await ta('[V1] まとまりの登録: 全部の子に同じメーカー (記録は子ごと・via variation)・入力なしも数える・今あるまとまりを読む = 子のメーカー', async () => {
  const r = await call('POST', '/api/new/variation', { body: variation('mkv1', HONPO.id) });
  assert.equal(r.status, 200, r.text);
  for (const c of ['mkv1-A', 'mkv1-B']) {
    assert.deepEqual(await makerOf(c), { id: HONPO.id, name: 'はちみつ 本舗' }, c);
    assert.deepEqual((await eventsOf(c)).map((e) => [e.f, e.t, e.via]), [[null, HONPO.id, 'variation']], c);
  }
  const V = await import('../lib/master-variation.mjs');
  const gid = (await one("select product_id::text as id from core.products where display_code = 'mkv1'")).id;
  const grp = await asEditor(() => V.readVariationGroup(db, gid));
  assert.deepEqual(grp.children.map((k) => k.values.maker), [HONPO.id, HONPO.id]);
  // 入力なし = 通る (子の理由に「入力なし: メーカー」)・どちらでもない = 400
  const b2 = variation('mkv2');
  assert.ok(b2.blanks.includes('maker'));
  const r2 = await call('POST', '/api/new/variation', { body: b2 });
  assert.equal(r2.status, 200, r2.text);
  assert.equal(await makerOf('mkv2-A'), null);
  const b3 = variation('mkv3'); b3.blanks = b3.blanks.filter((k) => k !== 'maker');
  const r3 = await call('POST', '/api/new/variation', { body: b3 });
  assert.deepEqual([r3.status, r3.j.reason, r3.j.field], [400, 'blank_unmarked', 'maker']);
  const r4 = await call('POST', '/api/new/variation', { body: variation('mkv4', '999999') });
  assert.deepEqual([r4.status, r4.j.reason], [400, 'maker_unknown']);
  assert.equal((await one("select count(*)::int as n from core.skus where code like 'mkv3-%' or code like 'mkv4-%'")).n, 0);
});

let MIKAWA = null;
await ta('[S1] 商品の画面の保存: 付ける → 変える → 外す (記録・via sku_page)・見出しと欄 (代表の仕入先のすぐ下)・押し直し = 前の答え', async () => {
  MIKAWA = (await create('三河テキスタイル')).j.maker;
  let page = await call('GET', '/sku/s001');
  assert.equal(page.status, 200);
  const iSup = page.text.indexOf('data-row="primary_supplier"'), iMk = page.text.indexOf('data-row="maker"'), iList = page.text.indexOf('この商品の仕入先');
  assert.ok(iSup > 0 && iMk > iSup && iList > iMk, 'メーカーの欄が代表の仕入先のすぐ下に無い');
  assert.match(page.text, /id="f-maker-in"[^>]*role="combobox"/);
  assert.match(page.text, /id="f-maker-same"[^>]*>仕入先と同じ</);
  assert.match(page.text, /public\/me-maker\.js/);
  const data = JSON.parse(/<script type="application\/json" id="me-maker">([\s\S]*?)<\/script>/.exec(page.text)[1]);
  assert.ok(data.makers.some(([id, n]) => id === HONPO.id && n === 'はちみつ 本舗'));
  assert.equal(data.supplierNow, '0001');
  const rid = uuid();
  const body = { request_id: rid, seen: { token: tok(page.text) }, values: { maker: HONPO.id } };
  const a = await call('POST', '/api/sku/s001', { body });
  assert.equal(a.status, 200, a.text);
  assert.deepEqual(a.j.changed.map((c) => [c.field, c.from, c.to, c.ne]), [['maker', null, 'はちみつ 本舗', 'none']]);
  assert.deepEqual(a.j.ne_steps, []);
  const again = await call('POST', '/api/sku/s001', { body });
  assert.deepEqual([again.status, again.j.replayed], [200, true]);
  page = await call('GET', '/sku/s001');
  assert.match(page.text, /id="maker-head">メーカー はちみつ 本舗</);
  const b = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { maker: MIKAWA.id } } });
  assert.deepEqual(b.j.changed.map((c) => [c.field, c.from, c.to]), [['maker', 'はちみつ 本舗', MIKAWA.name]]);
  page = await call('GET', '/sku/s001');
  const c = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { maker: '' } } });
  assert.deepEqual(c.j.changed.map((x) => [x.field, x.from, x.to]), [['maker', MIKAWA.name, null]]);
  assert.equal(await makerOf('s001'), null);
  assert.deepEqual((await eventsOf('s001')).map((e) => [e.f, e.t, e.via]), [[null, HONPO.id, 'sku_page'], [HONPO.id, MIKAWA.id, 'sku_page'], [MIKAWA.id, null, 'sku_page']]);
  // 変わらない保存 = 書かない
  page = await call('GET', '/sku/s001');
  const d = await call('POST', '/api/sku/s001', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { maker: '' } } });
  assert.equal(d.j.no_change, true);
  assert.equal((await eventsOf('s001')).length, 3);
});

await ta('[S2] ほかの人が変えた後の保存 = 409 version_conflict (編集の印)・DB の関数も見ていたメーカーと照らす・メーカーの無い商品の印は前と同じ・名簿に無い = 400', async () => {
  let page = await call('GET', '/sku/s002');
  const old = tok(page.text);
  const W = await import('../lib/master-write.mjs');
  const today = (await one(`select (now() at time zone 'Asia/Tokyo')::date::text as d`)).d;
  const id2 = (await one("select sku_id::text as id from core.skus where code = 's002'")).id;
  const cur0 = await asEditor(() => W.readCurrent(db, id2, today));
  assert.equal(cur0.maker, null);
  assert.equal(W.editTokenOf({ ...cur0, maker: undefined }), W.editTokenOf(cur0), 'メーカーの無い商品の印が変わった');
  const a = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: old }, values: { maker: HONPO.id } } });
  assert.equal(a.status, 200, a.text);
  const b = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: old }, values: { maker: MIKAWA.id } } });
  assert.deepEqual([b.status, b.j.reason], [409, 'version_conflict']);
  assert.deepEqual(await makerOf('s002'), { id: HONPO.id, name: 'はちみつ 本舗' });
  // DB の関数を直に呼ぶ = 門を通った保存・登録の取引の外 = 断る (商品の画面 = 保存の約束が無い / 登録 = この取引でその SKU を登録していない)
  await asEditor(async () => {
    for (const via of ['sku_page', 'register', 'variation']) {
      await pg.query('begin');
      try { await assert.rejects(pg.query('select ops.set_product_maker($1::uuid, $2, $3::bigint, $4::bigint, $5::bigint, $6, null)', [uuid(), 'naka@test', id2, MIKAWA.id, via === 'sku_page' ? HONPO.id : null, via]), /master_write_session_required/, via); }
      finally { await pg.query('rollback'); }
    }
  });
  // 保存の約束 (ops.begin_master_write・この SKU) の中でも、見ていたメーカーが今と違えば断る (DB でも照らす)・ほかの SKU の約束では書けない
  await asEditor(async () => {
    const cur = await W.readCurrent(db, id2, today);
    const begin = () => pg.query('select ops.begin_master_write($1::uuid, $2, $3, $4::jsonb, $5, $6::bigint, $7, $8, $9::jsonb)',
      [uuid(), 'naka@test', null, T.OWN, 'sku_edit', id2, W.editTokenOf(cur), 'a'.repeat(64), JSON.stringify(W.editVersionsOf(cur))]);
    await pg.query('begin');
    try {
      await begin();
      await assert.rejects(pg.query("select ops.set_product_maker($1::uuid, 'naka@test', $2::bigint, $3::bigint, null, 'sku_page', null)", [uuid(), id2, MIKAWA.id]), /version_conflict/);
    } finally { await pg.query('rollback'); }
    const id1 = (await one("select sku_id::text as id from core.skus where code = 's001'")).id;
    await pg.query('begin');
    try {
      await begin();
      await assert.rejects(pg.query("select ops.set_product_maker($1::uuid, 'naka@test', $2::bigint, $3::bigint, null, 'sku_page', null)", [uuid(), id1, MIKAWA.id]), /master_write_session_required/);
    } finally { await pg.query('rollback'); }
  });
  page = await call('GET', '/sku/s002');
  const c = await call('POST', '/api/sku/s002', { body: { request_id: uuid(), seen: { token: tok(page.text) }, values: { maker: '999999' } } });
  assert.deepEqual([c.status, c.j.reason, c.j.field], [400, 'maker_unknown', 'maker']);
  // セット・例外には付けない (DB の関数)
  const setId = (await one("select sku_id::text as id from core.skus where code = 'mkset1'"))?.id;
  // 記録は追記だけ (印があっても変えない・消さない)
  await pg.query('begin');
  try {
    await pg.query("select set_config('ops.maker_protocol', '1', true)");
    await assert.rejects(pg.query('update ops.product_maker_events set reason = null'), /./);
  } finally { await pg.query('rollback'); }
  assert.ok(setId, 'セット mkset1 が無い');
  {
    await asEditor(async () => {
      await pg.query('begin');
      try { await assert.rejects(pg.query("select ops.set_product_maker($1::uuid, 'naka@test', $2::bigint, $3::bigint, null, 'sku_page', null)", [uuid(), setId, HONPO.id]), /not_single/); }
      finally { await pg.query('rollback'); }
    });
  }
});

await ta('[L1] 一覧のメーカーの列 (いつもの列に出さない・出せる・並べられない)・CSV の最後の列「メーカー」・新商品の画面の欄 (単品 = 代表の仕入先のすぐ下・入力なし / セット = 無い)', async () => {
  assert.ok(COLS.COLUMN_IDS.includes('maker'));
  assert.ok(!COLS.DEFAULT_VIEW.shown.includes('maker'));
  assert.ok(!COLS.SORTABLE.includes('maker'));
  assert.equal(COLS.COLUMN_IDS.at(-1), 'maker', '前の列の並びを変えない (最後に足す)');
  const data = await asEditor(() => RD.listSkus(db, RD.normalizeFilters({ q: 'mk-r1' }), { mode: 'all' }));
  const row = data.rows.find((r) => r.code === 'mk-r1');
  assert.equal(row.maker, 'はちみつ 本舗');
  const csv = CSV.buildListCsv(data, {}, {});
  const lines = csv.replace(/^﻿/, '').trim().split('\r\n');
  const head = lines[0].split(',');
  assert.equal(head.at(-1), '"メーカー"');
  assert.equal(head.at(-2), '"商品コードの注意"');
  assert.ok(lines.find((l) => l.startsWith('"mk-r1"')).endsWith('"はちみつ 本舗"'));
  const list = await call('GET', '/?q=mk-r1');
  assert.match(list.text, /<td data-col="maker" hidden>はちみつ 本舗<\/td>/);
  const page = await call('GET', '/new?kind=single');
  const iSup = page.text.indexOf('data-row="primary_supplier"'), iMk = page.text.indexOf('data-row="maker"'), iEnd = page.text.indexOf('</section>', iSup);
  assert.ok(iSup > 0 && iMk > iSup && iMk < iEnd, '新商品の画面のメーカーの欄が代表の仕入先のすぐ下に無い');
  assert.match(page.text, /data-none="maker"/);
  assert.match(page.text, /id="f-maker"[^>]*data-none-part="maker"/);
  const setPage = await call('GET', '/new?kind=set');
  assert.ok(!/data-row="maker"/.test(setPage.text) && !/me-maker\.js/.test(setPage.text), 'セットにメーカーの欄がある');
  const vp = await call('GET', '/new?kind=variation');
  assert.match(vp.text, /data-row="maker"/);
  assert.match(vp.text, /data-none="maker"/);
});

await ta('[Y1] 夜間ロード: 商品の manufacturer の列 (属性の解決) を書いても、ポータルのメーカーは変わらない (別の表)', async () => {
  const before = await makerOf('s002');
  assert.ok(before);
  const plan = basePlan();
  plan.observations = [{ skuCode: 's002', attribute: 'manufacturer', scope: 'item', valueText: 'NE から来た製造元', source: 'product_hub', sourceRef: 'draft_page_info:1', observedAt: null }];
  const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
  const lr = await runInitialLoad(db, plan, { log: () => {}, runId: 'load_mk_y1', now: new Date(Date.now() - 3 * 86400e3) });
  assert.equal(lr.ok, true, lr.error);
  assert.equal((await one("select p.manufacturer from core.products p join core.skus k on k.product_id = p.product_id where k.code = 's002'")).manufacturer, 'NE から来た製造元');
  assert.deepEqual(await makerOf('s002'), before);
  // 夜間ロードのコードはメーカーの表を知らない
  const src = fs.readFileSync(new URL('../apps/company-db/load/engine.mjs', import.meta.url), 'utf8') + fs.readFileSync(new URL('../apps/company-db/load/sources.mjs', import.meta.url), 'utf8');
  assert.ok(!/product_makers|core\.makers/.test(src));
});

await ta('[B1] ロールの流し直しの前 (master_edit に関数の実行が無い) = 欄を出さない・メーカーを数えない・値を送ったら 503・商品の画面の保存はそのまま', async () => {
  for (const f of ROLES.MAKER_EDIT_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
  for (const t of ROLES.MAKER_SELECT) await pg.query(`revoke select on ${t} from master_edit`);
  try {
    assert.equal(await MK.makerReady(db), false);
    const page = await call('GET', '/new?kind=single');
    assert.ok(!/data-row="maker"/.test(page.text) && !/me-maker\.js/.test(page.text), '欄が出ている');
    // 古い画面と同じ = メーカーを送らない・入力なしの印も無い = 通る (数えない)
    const b = single('mk-b1'); b.blanks = b.blanks.filter((k) => k !== 'maker');
    const r = await call('POST', '/api/new', { body: b });
    assert.equal(r.status, 200, r.text);
    const reason = (await one("select reason from ops.master_registration_events e join core.skus k on k.sku_id = e.sku_id where k.code = 'mk-b1' order by e.event_id limit 1")).reason;
    assert.ok(!/メーカー/.test(reason), reason);
    // 入力なしの印を付けて送っても使わない (数えない項目)
    const r2 = await call('POST', '/api/new', { body: single('mk-b2') });
    assert.equal(r2.status, 200, r2.text);
    const n = await call('POST', '/api/new', { body: single('mk-b3', HONPO.id) });
    assert.deepEqual([n.status, n.j.reason], [503, 'db_not_ready']);
    assert.equal((await one("select count(*)::int as n from core.skus where code = 'mk-b3'")).n, 0);
    // 商品の画面: 欄なし・ほかの欄の保存はそのまま (メーカーの表を読めなくても止めない)
    const sp = await call('GET', '/sku/s003');
    assert.equal(sp.status, 200);
    assert.ok(!/data-row="maker"/.test(sp.text));
    const s = await call('POST', '/api/sku/s003', { body: { request_id: uuid(), seen: { token: tok(sp.text) }, values: { reorder_months: '3' } } });
    assert.equal(s.status, 200, s.text);
    const s2 = await call('POST', '/api/sku/s003', { body: { request_id: uuid(), seen: { token: tok((await call('GET', '/sku/s003')).text) }, values: { maker: HONPO.id } } });
    assert.deepEqual([s2.status, s2.j.reason], [503, 'db_not_ready']);
    assert.equal((await create('流し直しの前')).status, 503);
  } finally {
    await (await import('./company-db/create-master-edit-roles.mjs')).createMasterEditRoles(pg, {});
  }
  assert.equal(await MK.makerReady(db), true, 'ロールを流し直すと戻る');
});

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
server.close();
await pg.close();
process.exit(process.exitCode || 0);
