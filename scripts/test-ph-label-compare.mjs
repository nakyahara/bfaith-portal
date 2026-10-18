/**
 * test-ph-label-compare.mjs — product-hub のページ表記・楽天の型番と、商品マスタ (Company DB) の表示の項目を比べる印 (Company DB構想 27 ④-2・読むだけ)
 *
 * Company DB = PGlite (最新の migration まで = 0099 の表示の項目)。product-hub = 一時の DATA_DIR の SQLite (本物の router)。
 * 固定する契約:
 *   J 項目ごとの印 (純粋な関数): 一致・違う・product-hub は空・商品マスタに無い・比べられない (直せない内容量・海外製で国名なし)・夜間の取り込みの値とは比べない (自分と比べない)・
 *     「入れる」値 (原産国 = 日本製 / 海外製 + 国名の 1 組・内容量 = 数 + 単位)
 *   P 本物の SQL: 単品・人の値が無い商品 (内容量だけ夜間の値)・無い商品コード・セット (構成品の原産国が全部同じときだけ参考)・🚨 読むだけの取引・0099 の前の DB = ready false
 *   C 接続: JAN と同じ接続の道 (readCdbRowsWithin・同じ同時の上限)
 *   R router: GET /api/drafts/:id/label-check (単品 / まとまり / セット)・読めない = 200 で「読めない」・🚨 Company DB にも product-hub の SQLite にも何も書かない
 *   H 画面: 詳細のページ表記のタブに箱・画面の JS が読める (vm)
 * 使い方: node scripts/test-ph-label-compare.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
import express from 'express';

const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phlbl-data-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;
delete process.env.COMPANY_DB_URL;
delete process.env.COMPANY_DB_WATCH_URL;

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const J = await import('../apps/product-hub/services/cdb-jan-compare.mjs');
const L = await import('../apps/product-hub/services/cdb-label-compare.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};

console.log('J 項目ごとの印');
const H = (value) => ({ value, source: 'human', human: true, words: '人が入れた値' });
await ta('[J1] 原産国: 一致 (書き方の違いはそろえる)・違う・product-hub は空・海外製で国名なし = 比べられない・商品マスタに無い・「入れる」は 1 組', () => {
  assert.equal(L.judgeCountry(H('日本'), { origin_type: '日本製', origin_country: '' }).state, 'match');
  assert.equal(L.judgeCountry(H('中国'), { origin_type: '海外製', origin_country: '中華人民共和国' }).state, 'match');
  const d = L.judgeCountry(H('中国'), { origin_type: '日本製', origin_country: '' });
  assert.deepEqual([d.state, d.severity, d.put], ['differs', 'warn', { origin_type: '海外製', origin_country: '中国' }]);
  assert.deepEqual(L.judgeCountry(H('日本'), null).put, { origin_type: '日本製', origin_country: '' });
  assert.equal(L.judgeCountry(H('日本'), null).state, 'ph_empty');
  assert.equal(L.judgeCountry(H('中国'), { origin_type: '海外製', origin_country: '' }).state, 'not_comparable');
  assert.equal(L.judgeCountry(null, { origin_type: '日本製' }).state, 'master_empty');
});
await ta('[J2] 内容量: L ↔ ml は同じ・単位違いは違う・錠は比べない・夜間の取り込みの値 (product-hub 自身) とは比べない (印も「入れる」も無い)', () => {
  const m = { num: '500', uom: 'ml', text: '500ml', source: 'human', human: true, words: '人' };
  assert.equal(L.judgeNet(m, '0.5L').state, 'match');
  assert.equal(L.judgeNet(m, '500ml×2').state, 'match', '夜間ロードと同じ読み方 (先頭の数と単位)');
  assert.deepEqual([L.judgeNet(m, '500g').state, L.judgeNet(m, '500g').put], ['differs', '500ml']);
  assert.equal(L.judgeNet(m, '30錠').state, 'not_comparable');
  assert.equal(L.judgeNet(m, '').state, 'ph_empty');
  const fromPh = L.judgeNet({ ...m, source: 'product_hub', human: false }, '120ml');
  assert.deepEqual([fromPh.state, fromPh.severity, fromPh.put], ['from_ph', 'none', null]);
});
await ta('[J3] メーカー型番: 一致・違う・空', () => {
  assert.equal(L.judgeModel(H('AB-1'), ' AB-1 ').state, 'match');
  assert.equal(L.judgeModel(H('AB-1'), 'AB-2').state, 'differs');
  assert.equal(L.judgeModel(H('AB-1'), '').state, 'ph_empty');
  assert.equal(L.judgeModel(null, 'AB-1').state, 'master_empty');
});

console.log('P 本物の SQL');
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const sku = (code, name, x = {}) => ({ code, name, kind: 'single', taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: 100, source: 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S01', shippingMethod: 'ゆうパケット', shippingCostJpy: 210, reorderMonths: 2, ...x });
const plan = {
  skus: [sku('a001', '人の値あり'), sku('a002', '人の値あり 2'), sku('a003', '夜間の内容量だけ'), sku('a004', '何も無い'),
    sku('g-red', '色 赤'), sku('g-blue', '色 青'),
    sku('set001', 'セット', { kind: 'set', cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE' } }), sku('set002', 'セット 2', { kind: 'set', cost: { jpy: 300, source: 'set_calc', status: 'COMPLETE' } })],
  variationGroups: [],
  setComponents: [{ parentCode: 'set001', childCode: 'a001', qty: 2, source: 'ne' }, { parentCode: 'set001', childCode: 'a002', qty: 1, source: 'ne' },
    { parentCode: 'set002', childCode: 'a001', qty: 1, source: 'ne' }, { parentCode: 'set002', childCode: 'a004', qty: 1, source: 'ne' }],
  listings: [], physicals: [], compliance: [], workers: [],
  observations: [{ skuCode: 'a003', attribute: 'net_content', scope: 'item', valueNum: 120, unit: 'ml', rawText: '120ml', source: 'product_hub', sourceRef: 'draft_page_info:9', observedAt: '2026-09-01T00:00:00Z' }],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_phlbl' },
};
const loadR = await runInitialLoad(db, plan, { log: quiet, runId: 'load_phlbl', ownership: MASTER_OWNERSHIP, now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(loadR.ok, true, loadR.error);
/** 人の値を入れる (試験だけ: 守りの印を立てて持ち主のロールで直に入れる。画面からの道は scripts/test-master-label.mjs) */
async function human(code, { c = null, n = null, u = null, m = null, d = null } = {}) {
  await pg.query('begin');
  await pg.query("select set_config('ops.label_protocol', '1', true)");
  await pg.query(`insert into core.product_label_facts (product_id, country_of_origin, net_content, net_content_uom, model_number, shelf_life_days, set_by, set_via, request_id)
    select product_id, $2, $3, $4, $5, $6, 'naka@test', 'sku_page', gen_random_uuid() from core.skus where code = $1`, [code, c, n, u, m, d]);
  await pg.query('commit');
}
await human('a001', { c: '中国', n: '500', u: 'ml', m: 'AB-1', d: 365 });
await human('a002', { c: '中国', n: '200', u: 'g' });
await human('g-red', { c: '日本', n: '50', u: 'ml', m: 'G-RED' });
await human('g-blue', { c: '日本', n: '50', u: 'ml', m: 'G-BLUE' });
const cdbFingerprint = async () => (await pg.query(`select (select count(*) from core.product_label_facts)::int as n, (select count(*) from ops.product_label_fact_events)::int as e,
  (select md5(coalesce(string_agg(product_id::text || ':' || coalesce(net_content::text, '-'), ',' order by product_id), '')) from core.products) as p`)).rows[0];
const cdbBefore = await cdbFingerprint();
const stmts = [];
const pgClient = { query: (sql, params) => { stmts.push(String(sql).split('\n')[0].trim()); return pg.query(sql, params); } };
L.__setCdbLabelReader(async (codes) => ({ ok: true, ...(await L.queryCdbLabels(pgClient, 1, codes)) }));

await ta('[P1] 単品の行・人の値が無い商品 (内容量だけ夜間の値・出どころ)・無い商品コード・セットの構成品の原産国・🚨 読むだけの取引', async () => {
  stmts.length = 0;
  const r = await L.readCdbLabels(['a001', 'A003', 'a004', 'nope', 'set001']);
  assert.equal(r.ok, true); assert.equal(r.ready, true);
  const a1 = r.byCode.get('a001');
  assert.deepEqual([a1.country_of_origin, a1.country_source, a1.net_content, a1.net_content_uom, a1.model_number, a1.shelf_life_days], ['中国', 'human', '500.00', 'ml', 'AB-1', 365]);
  const a3 = r.byCode.get('a003');
  assert.deepEqual([a3.country_of_origin, a3.net_content, a3.net_source], [null, '120.00', 'product_hub']);
  assert.deepEqual([r.byCode.get('a004').sku_kind, r.byCode.get('a004').country_of_origin], ['single', null]);
  assert.equal(r.byCode.get('nope').sku_id, null);
  const s = r.byCode.get('set001');
  assert.equal(s.sku_kind, 'set');
  assert.deepEqual(s.comps.map((c) => [c.code, c.country, c.source]), [['a001', '中国', 'human'], ['a002', '中国', 'human']]);
  assert.deepEqual([stmts[0], stmts.at(-1)], ['begin transaction read only', 'rollback']);
});
await ta('[P2] 0099 の前の DB (ビューが無い) = ready false (読めないにしない)', async () => {
  const fake = { query: async (sql, p) => (/to_regclass\('core\.v_product_label_facts'\)/.test(sql) ? { rows: [{ ok: false }] } : pg.query(sql, p)) };
  const x = await L.queryCdbLabels(fake, 1, ['a001']);
  assert.deepEqual(x, { ready: false, rows: [] });
});
await ta('[C1] 接続: JAN と同じ道 (readCdbRowsWithin・接続の作り方の差し替えも同じ)・接続先が無い = 読めない', async () => {
  L.__setCdbLabelReader(null);
  try {
    assert.deepEqual(await L.readCdbLabels(['a001'], { env: {} }), { ok: false, reason: 'Company DB の接続先が無い' });
    const opts = [];
    J.__setCdbJanConnect(async (url, o) => { opts.push([url, o.application_name]); return { query: (s, p) => pg.query(s, p), end: async () => {} }; });
    const r = await L.readCdbLabels(['a001'], { env: { COMPANY_DB_URL: 'postgres://owner' } });
    assert.equal(r.ok, true, r.reason);
    assert.equal(r.byCode.get('a001').country_of_origin, '中国');
    assert.deepEqual(opts, [['postgres://owner', 'product-hub-cdb-label']]);
    await new Promise((r) => setTimeout(r, 50));   // 枠は接続を閉じ終わった後に返す
    assert.equal(J.__janReadInFlight(), 0, '同時の枠を返していない');
  } finally { J.__setCdbJanConnect(null); L.__setCdbLabelReader(async (codes) => ({ ok: true, ...(await L.queryCdbLabels(pgClient, 1, codes)) })); }
});

// ─── product-hub の SQLite ───
const ph = PHDB.getDB();
let mirrorPid = 910000;
const mirror = (code, repCode) => ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', ?, '2026-10-13T00:00:00Z')`).run(++mirrorPid, code, repCode);
for (const c of ['a001', 'a002', 'a003', 'a004', 'set001', 'set002']) mirror(c, null);
mirror('g-red', 'grpx'); mirror('g-blue', 'grpx');
const draft = (code, { pi = null, article = null } = {}) => {
  ph.prepare('INSERT INTO product_drafts (ne_code, name) VALUES (?, ?)').run(code, `下書き ${code}`);
  const d = ph.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get(code);
  if (pi) ph.prepare('INSERT INTO draft_page_info (draft_id, origin_type, origin_country, content_volume) VALUES (?, ?, ?, ?)').run(d.id, pi.type ?? null, pi.country ?? null, pi.volume ?? null);
  if (article != null) ph.prepare('INSERT INTO draft_rakuten (draft_id, article_number) VALUES (?, ?)').run(d.id, article);
  return d;
};
const dA1 = draft('a001', { pi: { type: '日本製', volume: '0.5L' }, article: 'AB-1' });   // 原産国 = 違う・内容量 = 一致・型番 = 一致
const dA3 = draft('a003', { pi: { volume: '120ml' } });                                   // 内容量 = 夜間の値 (product-hub 自身) = 比べない
const dSet = draft('set001', { pi: { type: '日本製' } });                                  // セット: 構成品が全部 中国 = 参考 (違う)
const dSet2 = draft('set002');                                                             // セット: 原産国の無い構成品がある = 参考なし
const dG = draft('grpx', { pi: { type: '日本製', volume: '50ml' } });                     // まとまり: 全部の子が 日本・50ml
const phFingerprint = () => {
  const t = ['product_drafts', 'draft_page_info', 'draft_rakuten', 'draft_events'];
  return Object.fromEntries(t.map((n) => [n, crypto.createHash('sha256').update(JSON.stringify(ph.prepare(`SELECT * FROM ${n} ORDER BY rowid`).all())).digest('hex')]));
};

const app = express();
app.set('view engine', 'ejs');
app.use(express.json());
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['product-hub'] }; next(); });
const PHR = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', PHR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
async function call(method, url) {
  const r = await fetch(ORIGIN + url, { method, headers: { Accept: 'application/json', Origin: ORIGIN }, signal: AbortSignal.timeout(30000) });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
function checkScripts(html) {
  for (const m of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
    if (/type="application\/json"/.test(m[1]) || /\bsrc=/.test(m[1])) continue;
    new vm.Script(m[2]);
    assert.ok(!/<%|%>/.test(m[2]), 'EJS のタグが JS に残っている');
  }
}
const phBefore = phFingerprint();
const check = async (d) => (await call('GET', `/apps/product-hub/api/drafts/${d.id}/label-check`));

try {
  await ta('[R1] 単品: 原産国 = 違う (入れる 1 組)・内容量 = 一致 (L ↔ ml)・型番 = 一致・消費期限は参考だけ・まとめの印 = 一番重い', async () => {
    const r = await check(dA1);
    assert.equal(r.status, 200);
    assert.deepEqual([r.j.ok, r.j.readable, r.j.ready, r.j.mode], [true, true, true, 'single']);
    const I = r.j.items;
    assert.deepEqual([I.country.state, I.country.master, I.country.ph, I.country.put], ['differs', '中国', '日本製', { origin_type: '海外製', origin_country: '中国' }]);
    assert.deepEqual([I.net.state, I.net.master, I.net.ph], ['match', '500ml', '0.5L']);
    assert.deepEqual([I.model.state, I.model.put], ['match', 'AB-1']);
    assert.deepEqual([I.shelf.value, I.shelf.human], [365, true]);
    assert.deepEqual(r.j.summary, { severity: 'warn', label: '商品マスタと違う' });
  });
  await ta('[R2] 人の値が無い商品: 内容量は夜間の取り込み (product-hub 自身) の値 = 比べない・原産国 / 型番 = 商品マスタに無い・印なし', async () => {
    const r = await check(dA3);
    const I = r.j.items;
    assert.deepEqual([I.net.state, I.net.put, I.country.state, I.model.state], ['from_ph', null, 'master_empty', 'master_empty']);
    assert.deepEqual(r.j.summary, { severity: 'none', label: '' });
  });
  await ta('[R3] セット: 構成品が全部同じ人の原産国 = 参考 (違う・入れる 1 組)・内容量と型番は出さない / 原産国の無い構成品がある = 参考なし', async () => {
    const r = await check(dSet);
    assert.deepEqual([r.j.mode, r.j.items, r.j.set.country, r.j.set.state, r.j.set.put], ['set', null, '中国', 'differs', { origin_type: '海外製', origin_country: '中国' }]);
    assert.deepEqual(r.j.set.codes, ['a001', 'a002']);
    const r2 = await check(dSet2);
    assert.deepEqual([r2.j.mode, r2.j.set.country, r2.j.set.put], ['set', null, null]);
    assert.match(r2.j.set.note, /原産国が入っていない構成品があります/);
  });
  await ta('[R4] まとまり: 子ごとの値を並べる・全部の子で同じ原産国・内容量だけページの欄と比べる・型番は比べない', async () => {
    const r = await check(dG);
    assert.equal(r.j.mode, 'variation');
    assert.deepEqual(r.j.kids.map((k) => [k.code, k.country.value, k.net.text, k.model.value]), [['g-blue', '日本', '50ml', 'G-BLUE'], ['g-red', '日本', '50ml', 'G-RED']]);
    assert.deepEqual([r.j.items.country.state, r.j.items.net.state, r.j.items.model], ['match', 'match', null]);
    // 子で違う = 比べない (入れるも出さない)
    await pg.query('begin'); await pg.query("select set_config('ops.label_protocol', '1', true)");
    await pg.query("update core.product_label_facts set country_of_origin = '中国' where product_id = (select product_id from core.skus where code = 'g-blue')");
    await pg.query('commit');
    const r2 = await check(dG);
    assert.deepEqual([r2.j.items.country.state, r2.j.items.country.put], ['master_empty', null]);
    assert.match(r2.j.items.country.detail, /子で違う/);
  });
  await ta('[R5] 読めない = 200 で「読めない」・無いドラフト = 404・0099 の前 = ready false', async () => {
    L.__setCdbLabelReader(async () => ({ ok: false, reason: '試験で読めない' }));
    try {
      const r = await check(dA1);
      assert.deepEqual([r.status, r.j.readable, r.j.reason, r.j.summary.label], [200, false, '試験で読めない', '読めない']);
    } finally { L.__setCdbLabelReader(async (codes) => ({ ok: true, ...(await L.queryCdbLabels(pgClient, 1, codes)) })); }
    L.__setCdbLabelReader(async () => ({ ok: true, ready: false, rows: [] }));
    try {
      const r = await check(dA1);
      assert.deepEqual([r.status, r.j.ready, r.j.mode], [200, false, null]);
    } finally { L.__setCdbLabelReader(async (codes) => ({ ok: true, ...(await L.queryCdbLabels(pgClient, 1, codes)) })); }
    assert.equal((await call('GET', '/apps/product-hub/api/drafts/999999/label-check')).status, 404);
  });
  await ta('[H1] 詳細画面: ページ表記のタブに箱 (読むだけの説明・自動保存の説明)・画面の JS が読める', async () => {
    const r = await call('GET', `/apps/product-hub/detail/${dA1.id}`);
    assert.equal(r.status, 200, r.text.slice(0, 300));
    const iInfo = r.text.indexOf('id="tab-info"'), iBox = r.text.indexOf('id="cdb-label-box"'), iOrigin = r.text.indexOf('id="pi-origin-type"');
    assert.ok(iInfo > 0 && iBox > iInfo && iOrigin > iBox, '箱がページ表記のタブの上に無い');
    assert.match(r.text, new RegExp(`/apps/product-hub/api/drafts/${dA1.id}/label-check`));
    assert.match(r.text, /自動で保存されます/);
    assert.match(r.text, /カテゴリ・属性を保存/);
    checkScripts(r.text);
  });
  await ta('[W1] 🚨 Company DB にも product-hub の SQLite にも何も書いていない', async () => {
    // [R4] で試験のために人の値を 1 つ直した (書いたのは試験だけ) = 行の数と商品の内容量で照らす
    const now = await cdbFingerprint();
    assert.deepEqual([now.n, now.e, now.p], [cdbBefore.n, cdbBefore.e, cdbBefore.p]);
    assert.deepEqual(phFingerprint(), phBefore);
  });
} finally {
  server.close();
}
console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
await pg.close();
process.exit(process.exitCode || 0);
