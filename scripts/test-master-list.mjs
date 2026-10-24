/**
 * test-master-list.mjs — 商品・セットの一覧 (apps/master-edit の「商品・セット」) の PR1 (10/8): 見出しで並び替え・列を選んで人ごとに保存・仕入先の列
 *
 * Company DB = PGlite (Render と同じ持ち主のロール deploy で migration・画面のロール master_edit で読む)。本物の router を HTTP 越しに通す (セッションは x-test-session)。
 * 列の設定の置き場 = 使い捨ての SQLite (本番 = Render の warehouse-mirror.db の master_edit_view_prefs)
 * 確かめること:
 *   1 並べられる全部の列 × 昇順 / 降順: 絞った全件 (全部のコード) の並び = 一覧に出す値で手で並べた並び (空は最後・同じ値はコード順)・1 ページ目の HTML も同じ・
 *     見出しの aria-sort と「もう一度押すと逆」のリンク・表の上の「並び: ○○」・CSV も同じ並び
 *   2 古い URL (reg_desc・kind・profit_asc・rate_asc・空) の読み替え・絞り込みのフォーム / 詳細検索の印 / ページ送りが並びを持ち回る
 *   3 許していない列・向き (FBA・売れた数・注文残・対応が必要・知らない名前・SQL のような字・配列) = コード順 (SQL に入れない)・向きの誤り = 昇順
 *   4 列の設定: 保存・読む・初期に戻す (行を消す)・他人の設定は読めない / 変えない・メールの大文字小文字は同じ人・入力の確かめ (知らない列・多すぎる・重なる・
 *     コードが左端でない / 外した・知らない項目 = 400)・Origin と JSON の守り・置き場が無い (読めない = いつもの列で一覧は出る / 保存は 503)
 *   5 画面の描画: 保存した列の並び・出さない列は hidden・仕入先の列 (出すと名前の下の字を消す)・注文残の権限・列の板の材料 (JSON)・画面の JS が読める
 *   6 在庫の並び (ロジザードの写し・セットは作れる数) = 一覧の値と同じ・写しが読めない = コード順・全部のコードも同じ
 *   8 詳細検索のメーカー (0084・複数・メーカー未登録)・URL / 印に残る・並び・件数・CSV・知らない値は捨てる・索引で引く・仕入先を名前で選ぶ名簿 (10/24)
 * 使い方: node scripts/test-master-list.mjs
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import vm from 'node:vm';
import express from 'express';
import Database from 'better-sqlite3';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
// ロジザードの写し (warehouse-mirror.db) = 使い捨ての DATA_DIR (router を読む前に。warehouse-mirror/db.js は読み込んだ時に DATA_DIR を決める)
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'mlist1-'));
process.env.DATA_DIR = DATA_TMP;
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const R = await import('../apps/master-edit/read.mjs');
const LC = await import('../apps/master-edit/list-columns.mjs');
const VP = await import('../apps/master-edit/view-prefs.mjs');
const { expectedOrder, distinctValues, nullCount } = await import('./fixtures/master-list-order.mjs');
const { default: router, __setPgClientFactory, __setClock, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');
const { __clearStockCache } = await import('../apps/master-edit/extras.mjs');
const mirrorDb = (await import('../apps/warehouse-mirror/db.js')).initMirrorDB();
/** ロジザードの写し (商品ID ごと・ロケの行を足す)。rows = [[商品ID, 数], …]・空 = 写しが無い */
function setStock(rows) {
  mirrorDb.prepare('delete from mirror_logizard_stock').run();
  const ins = mirrorDb.prepare(`insert into mirror_logizard_stock (商品ID, 商品名, ブロック略称, ロケ, 品質区分名, 在庫数, 引当数, captured_at, synced_at) values (?, ?, 'P', ?, '良品', ?, 0, ?, ?)`);
  const at = new Date(NOW.getTime() - 20 * 60e3).toISOString();
  rows.forEach(([code, n], i) => ins.run(code, code, `L-${i}`, n, at, at));
  __clearStockCache();
}

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const NOW = new Date('2030-01-10T03:00:00Z');

// ── Company DB (PGlite) ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg);
/** 並びを確かめる材料: どの列にも違う値・同じ値 (コード順になるか)・空 (最後になるか) がある */
const sku = (code, name, kind, o = {}) => ({
  code, name, kind, taxRate: o.tax === undefined ? 0.1 : o.tax, taxClass: o.tax === 0.08 ? 'REDUCED_8' : o.tax === null ? null : 'STANDARD_10', handling: o.off ? 'discontinued' : 'active',
  salesClass: o.sales === undefined ? 1 : o.sales,
  cost: o.cost == null ? null : { jpy: o.cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' },
  standardPriceJpy: o.price === undefined ? 1000 : o.price, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: o.ship === undefined ? 300 : o.ship, reorderMonths: 2,
  ...(o.reg ? { registeredOn: o.reg } : {}),
});
const SKUS = [
  sku('a01', 'あいう 石鹸', 'single', { cost: 100, price: 1000, reg: '2024-01-05' }),
  sku('a02', 'アイウ 石鹸', 'single', { tax: 0.08, sales: 2, cost: 200, price: 1500, reg: '2024-01-05' }),
  sku('a03', 'ABC タオル', 'single', { tax: null, sales: 3, cost: 50, price: 800 }),
  sku('a04', 'abc タオル', 'single', { sales: null, cost: null, price: 1200, off: true, reg: '2023-12-31' }),
  sku('a05', '漢字 商品', 'single', { sales: 4, cost: 300, price: null, reg: '2025-06-01' }),
  sku('a06', '同じ名前', 'single', { tax: 0.08, cost: 100, price: 1000, reg: '2024-01-05', ship: null }),
  sku('a07', '同じ名前', 'single', { cost: 100, price: 1000, off: true }),
  sku('a08', 'ｶﾀｶﾅ', 'single', { sales: 3, cost: 80, price: 900, reg: '2026-10-01' }),
  sku('a09', 'zeta', 'single', { sales: 2, cost: 10, price: 50, reg: '2022-02-02' }),
  sku('a10', '0番', 'single', { cost: 999, price: 999, reg: '2024-01-05' }),
  sku('b01', 'セット A', 'set', { cost: 400, price: 3000 }),
  sku('b02', 'セット B', 'set', { cost: null, price: 1200, off: true }),
  sku('b03', 'セット C', 'set', { cost: 210, price: null }),
  sku('x01', '例外 1', 'exception', { sales: null, cost: 500, price: 2000 }),
  sku('x02', '例外 2', 'exception', { tax: null, sales: null, cost: null, price: null }),
];
const lr = await runInitialLoad(db, {
  skus: SKUS, variationGroups: [],
  setComponents: [{ parentCode: 'b01', childCode: 'a01', qty: 2, source: 'ne' }, { parentCode: 'b01', childCode: 'a02', qty: 1, source: 'ne' },
    { parentCode: 'b02', childCode: 'a04', qty: 1, source: 'ne' }, { parentCode: 'b03', childCode: 'a03', qty: 1, source: 'ne' }, { parentCode: 'b03', childCode: 'a08', qty: 2, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }, { code: '0002', name: 'ビーフリー' }, { code: '0010', name: '東和' }],
  supplierSkus: [['a01', '0002'], ['a02', '0001'], ['a03', '0010'], ['a04', '0001'], ['a06', '0002'], ['a07', '0001'], ['a09', '0010'], ['a10', '0002'], ['a10', '0001']].map(([s, c]) => ({ supplierCode: c, skuCode: s })),
  primarySuppliers: [['a01', '0002'], ['a02', '0001'], ['a03', '0010'], ['a04', '0001'], ['a06', '0002'], ['a07', '0001'], ['a09', '0010'], ['a10', '0002']].map(([s, c]) => ({ skuCode: s, supplierCode: c })),
  reorder: { available: true, runId: 'pml_list' },
}, { log: quiet, runId: 'load_list', now: new Date('2030-01-05T03:00:00Z') });
assert.equal(lr.ok, true, lr.error);
const ALL_CODES = SKUS.map((s) => s.code);

// ── 本物の router ──
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/list';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/list';
process.env.MASTER_EDITORS = 'naka@test';
let chain = Promise.resolve();
const sqlLog = [];
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => { sqlLog.push(t); return pg.query(t, p); }, end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setClock(() => NOW.getTime());
__setShippingRatesProvider(async () => new Map());
// 列の設定の置き場 (使い捨ての SQLite)。prefsDown = 置き場が開けない
let prefsDb = new Database(':memory:');
let prefsDown = false;
VP.__setViewPrefsDbProvider(async () => { if (prefsDown) throw new Error('warehouse-mirror.db が初期化されていません'); return prefsDb; });
const SESSIONS = {
  editor: { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] },
  upper: { authenticated: true, email: '  NAKA@Test ', displayName: '中原', role: 'user', allowedApps: ['master-edit'] },
  other: { authenticated: true, email: 'other@test', displayName: '他の人', role: 'user', allowedApps: ['master-edit'] },
  admin: { authenticated: true, email: 'admin@test', displayName: '管理', role: 'admin', allowedApps: '*' },
};
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = SESSIONS[req.headers['x-test-session']] || null; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const BASE = `${ORIGIN}/apps/master-edit`;
async function call(method, url, { body, session = 'editor', origin = true, ctype = true } = {}) {
  const headers = { Accept: 'application/json', 'x-test-session': session };
  if (body !== undefined && ctype) headers['Content-Type'] = 'application/json';
  if (origin) headers.Origin = ORIGIN;
  const r = await fetch(BASE + url, { method, headers, body: body === undefined ? undefined : (typeof body === 'string' ? body : JSON.stringify(body)), redirect: 'manual' });
  const text = await r.text();
  let j = null; try { j = JSON.parse(text); } catch { /* HTML */ }
  return { status: r.status, j, text };
}
const listOrder = (html) => [...html.matchAll(/<a class="rowlink" href="sku\/([^"]+)"/g)].map((m) => decodeURIComponent(m[1]));
const thead = (html) => /<thead>([\s\S]*?)<\/thead>/.exec(html)[1];
const headIds = (html) => [...thead(html).matchAll(/<th [^>]*data-c="([^"]+)"/g)].map((m) => m[1]);
const thOf = (html, id) => new RegExp(`<th [^>]*data-c="${id}"[^>]*>[\\s\\S]*?<\\/th>`).exec(thead(html))?.[0];
const codesOf = async (qs, session = 'editor') => { const r = await call('GET', `/api/codes${qs}`, { session }); assert.equal(r.status, 200, r.text); return r.j.codes; };
const cfgOf = (html) => JSON.parse(/<script type="application\/json" id="me-list-cfg">([\s\S]*?)<\/script>/.exec(html)[1]);
/** 一覧に出す値 (並べる関数を使わない正しい並びの材料) = 絞った全件の中身 (mode all = CSV と同じ行) */
const shownRows = async (extras = {}) => (await R.listSkus(db, {}, { now: NOW, extras, mode: 'all' })).rows;

console.log('並び替え');
await ta('[1] 並べられる全部の列 × 昇順 / 降順 = 一覧に出す値で並べた並び (空は最後・同じ値はコード順)・1 ページ目の HTML・見出しの印・CSV も同じ', async () => {
  const rows = await shownRows();
  assert.equal(rows.length, SKUS.length);
  const cols = LC.SORTABLE.filter((c) => c !== 'stock');   // 在庫はロジザードの写しが要る = [6]
  assert.deepEqual(cols, ['code', 'name', 'kind', 'state', 'reg', 'price', 'cost', 'tax', 'profit', 'rate', 'sales_class', 'sup']);
  for (const col of cols) {
    // 材料が並びを確かめられる形か (違う値が 2 つ以上・コード以外は空の行もある)
    assert.ok(distinctValues(rows, col) >= 2, `${col}: 違う値が 2 つ以上`);
    if (!['code', 'name', 'kind', 'state'].includes(col)) assert.ok(nullCount(rows, col) >= 1, `${col}: 空の行がある (最後になるか)`);
    for (const dir of ['asc', 'desc']) {
      const qs = `?sort=${col}${dir === 'desc' ? '&dir=desc' : ''}`;
      const want = expectedOrder(rows, col, dir);
      assert.deepEqual(await codesOf(qs), want, `${col} ${dir}: 全部のコード`);
      const r = await call('GET', `/${qs}`);
      assert.equal(r.status, 200);
      assert.deepEqual(listOrder(r.text), want.slice(0, 100), `${col} ${dir}: 1 ページ目`);
      // 見出し: 並べている列だけ aria-sort・押すと逆の向き (昇順 → 降順 → 昇順)
      const th = thOf(r.text, col);
      assert.match(th, new RegExp(`aria-sort="${dir === 'asc' ? 'ascending' : 'descending'}"`), `${col} ${dir}: aria-sort`);
      const next = col === 'code' ? (dir === 'asc' ? '?dir=desc' : './') : (dir === 'asc' ? `?sort=${col}&amp;dir=desc` : `?sort=${col}`);
      assert.ok(th.includes(`<a class="sorth" href="${next}"`), `${col} ${dir}: もう一度押すと逆 (${next}) ${th}`);
      assert.equal((thead(r.text).match(/aria-sort=/g) || []).length, 1, '並べている見出しは 1 つ');
      const words = LC.SORT_WORDS[LC.COLUMN_BY_ID[col].sort];
      assert.ok(r.text.includes(`並び: <b>${LC.COLUMN_BY_ID[col].label}</b> ${words[dir === 'asc' ? 0 : 1]}</span>`), `${col} ${dir}: 表の上の並びの言葉`);
      assert.ok(r.text.includes(`<use href="#${dir === 'asc' ? 'i-sortup' : 'i-sortdown'}"/></svg>並び:`));
    }
  }
  // ほかの列の見出し = 押すと昇順 (並べていない列)
  const r = await call('GET', '/?sort=price&dir=desc');
  assert.ok(thOf(r.text, 'cost').includes('<a class="sorth" href="?sort=cost"'), '並べていない列 = 昇順へ (dir は付けない)');
  assert.ok(thOf(r.text, 'code').includes('<a class="sorth" href="./"'), 'コードの昇順 = 何も付けない URL');
  // CSV も同じ並び
  const csv = await (await fetch(`${BASE}/list.csv?sort=cost&dir=desc`, { headers: { 'x-test-session': 'editor' } })).text();
  assert.deepEqual(csv.replace(/^\ufeff/, '').trim().split('\r\n').slice(1).map((l) => /^"([^"]*)"/.exec(l)[1]), expectedOrder(rows, 'cost', 'desc'), 'CSV も原価の大きい順');
  // 絞っても並ぶ (絞った後に並べる)・ページ分け (offset) の 2 ページ目も同じ並びの続き
  const singles = rows.filter((x) => x.kind === 'single');
  assert.deepEqual(await codesOf('?kind=single&sort=name&dir=desc'), expectedOrder(singles, 'name', 'desc'));
  {
    const old = R.LIST_LIMIT;
    const p1 = await R.listSkus(db, { sort: 'price', dir: 'desc' }, { now: NOW });
    const p2 = await R.listSkus(db, { sort: 'price', dir: 'desc', offset: '5' }, { now: NOW });
    assert.equal(old, 100);
    assert.deepEqual(p2.rows.map((x) => x.code), p1.rows.slice(5).map((x) => x.code), 'offset = 同じ並びの続き');
  }
});

await ta('[2] 古い URL の並び (reg_desc・kind・profit_asc・rate_asc・空) の読み替え・絞り込みのフォーム / 詳細検索の印 / ページ送り / コピー / CSV が並びを持ち回る', async () => {
  for (const [old, now] of [['reg_desc', '?sort=reg&dir=desc'], ['kind', '?sort=kind'], ['profit_asc', '?sort=profit'], ['rate_asc', '?sort=rate'], ['', '']]) {
    assert.deepEqual(await codesOf(old ? `?sort=${old}` : ''), await codesOf(now), `${old || '(空)'} = ${now || 'コード順'}`);
    const f = R.normalizeFilters({ sort: old });
    const n = R.normalizeFilters(Object.fromEntries(new URLSearchParams(now)));
    assert.deepEqual([f.sort, f.dir], [n.sort, n.dir], `${old}: 決まった形`);
  }
  // 古い名前の向きは名前の通り (reg_desc に dir=asc を付けても新しい順)・コード順に dir=desc = コードの逆の順
  assert.deepEqual(R.normalizeFilters({ sort: 'reg_desc', dir: 'asc' }), { ...R.normalizeFilters({}), sort: 'reg', dir: 'desc' });
  assert.deepEqual([R.normalizeFilters({ dir: 'desc' }).sort, R.normalizeFilters({ dir: 'desc' }).dir], ['', 'desc']);
  assert.deepEqual(await codesOf('?dir=desc'), [...ALL_CODES].sort().reverse());
  // 古い URL で開いた画面のリンクは新しい形 (絞る欄・もっと絞る・詳細検索・コピー・CSV)
  const r = await call('GET', '/?sort=reg_desc&kind=single');
  assert.ok(r.text.includes('<input type="hidden" name="sort" value="reg"><input type="hidden" name="dir" value="desc">') || /name="sort" value="reg"[\s\S]{0,80}name="dir" value="desc"/.test(r.text), '絞る欄のフォームが並びを持ち回る');
  assert.match(r.text, /id="copy-all" data-url="api\/codes\?kind=single&amp;sort=reg&amp;dir=desc"/);
  assert.match(r.text, /id="csv-link" href="list\.csv\?kind=single&amp;sort=reg&amp;dir=desc"/);
  assert.match(r.text, /<form method="get" action="" class="adv-grid" id="adv-form" data-api="api\/search">\s*<input type="hidden" name="sort" value="reg"><input type="hidden" name="dir" value="desc">/, '詳細検索のフォーム');
  assert.match(r.text, /<details class="more filters-more"[\s\S]*?<input type="hidden" name="sort" value="reg">\s*<input type="hidden" name="dir" value="desc">/, 'もっと絞る のフォーム');
  // 詳細検索の印 (POST api/search) も sort と dir を残す
  const sr = await call('POST', '/api/search', { body: { codes: 'a01\na02\na09', sort: 'price', dir: 'desc' } });
  assert.equal(sr.status, 200, sr.text);
  assert.match(sr.j.url, /sort=price&dir=desc/);
  assert.deepEqual(await codesOf('?' + sr.j.url.split('?')[1]), ['a02', 'a01', 'a09'], '印の中身・売価の大きい順');
  // ページ送りのリンク (101 件以上の一覧で見る = 試験は R.listSkus の offset。ここはリンクの形だけ: 並びを持ち回る)
  assert.ok(!/href="\?offset=/.test(r.text));
});

await ta('[3] 許していない列名・向き = コード順 / 昇順 (SQL に入れない)', async () => {
  const codeAsc = await codesOf('');
  for (const bad of ['fba', 'sold', 'po', 'flags', 'nope', 'price;drop table core.skus', 'name collate "C"', 'PRICE', 'profit_desc', 'kind,name', '__proto__', 'constructor']) {
    const f = R.normalizeFilters({ sort: bad, dir: 'desc' });
    assert.equal(f.sort, '', `${bad}: 捨てる`);
    sqlLog.length = 0;
    assert.deepEqual(await codesOf(`?sort=${encodeURIComponent(bad)}`), codeAsc, `${bad}: コード順`);
    assert.ok(sqlLog.length > 0);
    // 人の入力そのものが SQL に入っていない (ほかの SQL の字と重ならない値だけで確かめる)
    if (['nope', 'price;drop table core.skus', 'name collate "C"', 'PRICE', 'kind,name'].includes(bad)) assert.ok(sqlLog.every((t) => !t.includes(bad)), `${bad}: SQL に入れない`);
    assert.ok(sqlLog.every((t) => !/drop table/i.test(t)));
    const r = await call('GET', `/?sort=${encodeURIComponent(bad)}`);
    assert.equal(r.status, 200);
    assert.deepEqual(listOrder(r.text), codeAsc);
    assert.ok(r.text.includes('並び: <b>コード</b> 0→9・A→Z の順</span>'));
  }
  // 並べられない列の見出しはリンクにしない (押せない)
  const r = await call('GET', '/', { session: 'admin' });
  for (const id of ['fba', 'sold', 'po', 'flags']) assert.ok(thOf(r.text, id) && !thOf(r.text, id).includes('class="sorth"'), `${id}: 並べられない`);
  // 配列 (?sort=a&sort=b)・向きの誤り
  assert.deepEqual(await codesOf('?sort=price&sort=name'), codeAsc, '配列 = コード順');
  assert.deepEqual(R.normalizeFilters({ sort: ['price'] }).sort, '');
  const rows = await shownRows();
  for (const d of ['sideways', 'DESC', 'desc;', '']) assert.deepEqual(await codesOf(`?sort=price&dir=${encodeURIComponent(d)}`), expectedOrder(rows, 'price', 'asc'), `dir=${d} = 昇順`);
  // ORDER BY は決まった式だけ (人の入力を入れない)
  assert.equal(R.listOrderSql('nope', 'desc'), 's.code_norm');
  assert.equal(R.listOrderSql('price', 'desc;drop'), 's.standard_price_jpy asc nulls last, s.code_norm');
  assert.equal(R.listOrderSql('reg', 'desc', { hasRegOn: false }), 's.code_norm', '登録日の列が無い DB = コード順');
});

console.log('列の設定 (人ごと)');
// 画像の列 (10/17) は並びにあって出さない (出さない列の描き方・取りに行かないを見る)
const goodView = () => ({ order: ['code', 'img', 'sup', 'name', 'price', 'cost', 'kind', 'state', 'reg', 'tax', 'profit', 'rate', 'sales_class', 'stock', 'fba', 'sold', 'po', 'flags', 'maker', 'pkg', 'brand', 'stage', 'end_on'],   // 🆕 0084: メーカー・🆕 0087: 大きさ・重さ・🆕 0088: ブランド・段階 (後ろ・出さない)
  shown: ['code', 'sup', 'name', 'price', 'cost', 'stock', 'flags'] });
const rowsIn = () => prefsDb.prepare('select email, prefs_json from master_edit_view_prefs order by email').all();
await ta('[4] 列の設定: 保存・読む・初期に戻す・他人の設定は読めない / 変えない・入力の確かめ・Origin と JSON の守り・置き場が無い', async () => {
  // 何も保存していない = いつもの列 (今までの一覧と同じ 16 列・仕入先は出さない)
  let r = await call('GET', '/api/view-prefs');
  assert.equal(r.status, 200);
  assert.deepEqual([r.j.ok, r.j.saved, r.j.view], [true, false, { order: [...LC.DEFAULT_VIEW.order], shown: [...LC.DEFAULT_VIEW.shown] }]);
  assert.deepEqual(r.j.view.shown, ['code', 'img', 'name', 'kind', 'state', 'reg', 'price', 'cost', 'tax', 'profit', 'rate', 'sales_class', 'stock', 'fba', 'sold', 'po', 'flags'], 'いつもの列 = 10/8 までの一覧の列 + 画像 (10/17・コードのすぐ後ろ)');
  // 保存 → 読む (同じ人の別の書き方のメール = 同じ設定)
  r = await call('POST', '/api/view-prefs', { body: goodView() });
  assert.equal(r.status, 200, r.text);
  assert.deepEqual([r.j.ok, r.j.saved, r.j.view], [true, true, goodView()]);
  assert.deepEqual(rowsIn().map((x) => x.email), ['naka@test'], '鍵 = メール (小文字)');
  assert.deepEqual((await call('GET', '/api/view-prefs', { session: 'upper' })).j.view, goodView(), '「  NAKA@Test 」= 同じ人 (会社と家の PC で同じ)');
  // 他人: 読めない (いつもの列)・保存しても中原さんの設定は変わらない
  r = await call('GET', '/api/view-prefs', { session: 'other' });
  assert.deepEqual([r.j.saved, r.j.view.order], [false, [...LC.DEFAULT_VIEW.order]], '他人の設定は見えない');
  const otherView = { order: [...LC.DEFAULT_VIEW.order], shown: ['code', 'name'] };
  assert.equal((await call('POST', '/api/view-prefs', { session: 'other', body: otherView })).status, 200);
  assert.deepEqual((await call('GET', '/api/view-prefs')).j.view, goodView(), '他人の保存で中原さんの設定は変わらない');
  assert.deepEqual((await call('GET', '/api/view-prefs', { session: 'other' })).j.view, otherView);
  assert.deepEqual(rowsIn().map((x) => x.email), ['naka@test', 'other@test']);
  // 画像の列 (10/17) より前に保存した設定 (並びに img が無い) = コードのすぐ後ろに画像を出す (ほかの列の並び・出す / 出さないはそのまま)
  {
    const old = { v: 1, order: goodView().order.filter((x) => x !== 'img'), shown: goodView().shown.filter((x) => x !== 'img') };
    prefsDb.prepare('update master_edit_view_prefs set prefs_json = ? where email = ?').run(JSON.stringify(old), 'naka@test');
    const v = (await call('GET', '/api/view-prefs')).j.view;
    assert.deepEqual(v.order, ['code', 'img', ...old.order.slice(1)], '画像はコードのすぐ後ろ');
    assert.deepEqual(v.shown, ['code', 'img', ...old.shown.slice(1)], '画像は出す・ほかはそのまま');
    // 画像を外して保存した人 = その後は出さない (並びに img がある)
    assert.equal((await call('POST', '/api/view-prefs', { body: goodView() })).status, 200);
    assert.deepEqual((await call('GET', '/api/view-prefs')).j.view, goodView(), '外した画像は出さないまま');
  }
  // 人の名前を送っても使わない (知らない項目 = 400)
  for (const extra of [{ email: 'other@test' }, { user: 'other@test' }, { reset: true }]) {
    r = await call('POST', '/api/view-prefs', { body: { ...goodView(), ...extra } });
    assert.deepEqual([r.status, r.j.reason], [400, 'invalid_input'], JSON.stringify(extra));
  }
  assert.deepEqual((await call('GET', '/api/view-prefs', { session: 'other' })).j.view, otherView, '他人の設定は変わっていない');
  // 入力の確かめ
  const bad = [
    [{ order: [...goodView().order, 'nope'], shown: ['code'] }, /列が多すぎます|知らない列/],
    [{ order: ['code', 'nope'], shown: ['code'] }, /知らない列 nope/],
    [{ order: goodView().order, shown: ['code', 'secret_cost'] }, /知らない列 secret_cost/],
    [{ order: [...goodView().order, ...goodView().order], shown: ['code'] }, /列が多すぎます/],
    [{ order: ['code', 'name', 'name'], shown: ['code'] }, /同じ列 name が 2 回/],
    [{ order: goodView().order, shown: ['code', 'name', 'name'] }, /同じ列 name が 2 回/],
    [{ order: ['name', 'code'], shown: ['code'] }, /いつも左端/],
    [{ order: goodView().order, shown: ['name'] }, /外せません/],
    [{ order: 'code,name', shown: ['code'] }, /並びにしてください/],
    [{ order: goodView().order }, /並びにしてください/],
    [{ order: [1, 2], shown: ['code'] }, /知らない列/],
    [{ order: ['code', { id: 'name' }], shown: ['code'] }, /知らない列/],
    [{ order: ['code', '__proto__'], shown: ['code'] }, /知らない列/],
    [[], /形が違います/],
    [{}, /並びにしてください/],
  ];
  for (const [body, re] of bad) {
    r = await call('POST', '/api/view-prefs', { body });
    assert.equal(r.status, 400, `${JSON.stringify(body).slice(0, 80)}: ${r.text}`);
    assert.match(r.j.error, re);
  }
  // JSON の文字 1 つ ("code") = express.json が断る (400・オブジェクトと配列だけ受ける)
  assert.equal((await call('POST', '/api/view-prefs', { body: '"code"' })).status, 400);
  assert.deepEqual((await call('GET', '/api/view-prefs')).j.view, goodView(), '断った保存は何も変えない');
  // 足りない列は後ろに出さない形で足す (後から列を足した・板に出さない列)
  r = await call('POST', '/api/view-prefs', { body: { order: ['code', 'name', 'price'], shown: ['code', 'price'] } });
  assert.equal(r.status, 200);
  // 画像 (10/17・addShown) だけは足りなければコードのすぐ後ろに出す (列を足す前の画面から送った設定でも画像が出る)
  assert.deepEqual(r.j.view.order.slice(0, 4), ['code', 'img', 'name', 'price']);
  assert.equal(r.j.view.order.length, LC.COLUMN_IDS.length);
  assert.deepEqual(r.j.view.shown, ['code', 'img', 'price']);
  // Origin・JSON の守り (書く API)
  assert.equal((await call('POST', '/api/view-prefs', { body: goodView(), origin: false })).status, 403, 'Origin が無い');
  assert.equal((await call('POST', '/api/view-prefs', { body: goodView(), ctype: false })).status, 415, 'JSON でない');
  {
    const x = await fetch(BASE + '/api/view-prefs', { method: 'POST', headers: { 'x-test-session': 'editor', Origin: 'http://evil.example', 'Content-Type': 'application/json' }, body: JSON.stringify(goodView()) });
    assert.equal(x.status, 403, 'よそのサイト');
  }
  // 保存し直し → 初期に戻す (いつもの列を保存 = 行を消す)
  assert.equal((await call('POST', '/api/view-prefs', { body: goodView() })).status, 200);
  r = await call('POST', '/api/view-prefs', { body: { order: [...LC.DEFAULT_VIEW.order], shown: [...LC.DEFAULT_VIEW.shown] } });
  assert.deepEqual([r.status, r.j.saved], [200, false]);
  assert.deepEqual(rowsIn().map((x) => x.email), ['other@test'], '初期に戻す = 中原さんの行だけ消える (他人の行は残る)');
  assert.equal((await call('GET', '/api/view-prefs')).j.saved, false);
  // 壊れた行 = いつもの列 (画面は止めない)
  prefsDb.prepare("insert into master_edit_view_prefs (email, prefs_json, updated_at) values ('naka@test', '{壊れた', 'x')").run();
  r = await call('GET', '/api/view-prefs');
  assert.deepEqual([r.status, r.j.saved, r.j.view.order], [200, false, [...LC.DEFAULT_VIEW.order]]);
  assert.equal((await call('GET', '/')).status, 200);
  prefsDb.prepare("delete from master_edit_view_prefs where email = 'naka@test'").run();
  // 置き場が無い: 一覧はいつもの列で出す (知らせを板に)・読む API は 503・保存は 503
  prefsDown = true;
  try {
    r = await call('GET', '/');
    assert.equal(r.status, 200);
    assert.deepEqual(headIds(r.text), LC.DEFAULT_VIEW.order.filter((x) => x !== 'po'));
    assert.match(r.text, /id="cp-unreadable"[^>]*>[\s\S]*?列の設定を読めません/);
    r = await call('GET', '/api/view-prefs');
    assert.deepEqual([r.status, r.j.ok], [503, false]);
    r = await call('POST', '/api/view-prefs', { body: goodView() });
    assert.deepEqual([r.status, r.j.reason], [503, 'store_unavailable']);
  } finally { prefsDown = false; }
  // セッションのメールが無い = 保存しない (鍵が無い)
  assert.equal(VP.prefsKeyOf(''), null); assert.equal(VP.prefsKeyOf(null), null); assert.equal(VP.prefsKeyOf(' A@B '), 'a@b');
  await assert.rejects(() => VP.saveViewPrefs('', goodView()), (e) => e.status === 403);
});

await ta('[5] 画面の描画: 保存した列の並び・出さない列は hidden・仕入先の列 (名前の下の字を消す)・注文残の権限・列の板の材料・画面の JS', async () => {
  assert.equal((await call('POST', '/api/view-prefs', { body: goodView() })).status, 200);
  let r = await call('GET', '/?sort=sup');
  assert.equal(r.status, 200);
  // 見出しの並び = 保存した並び (注文残は発注アプリの権限が無い人には描かない)・出さない列は hidden (見出しもセルも)
  const want = goodView().order.filter((x) => x !== 'po');
  assert.deepEqual(headIds(r.text), want);
  const shown = new Set(goodView().shown);
  for (const id of want) {
    const th = thOf(r.text, id);
    assert.equal(/ hidden[ >]/.test(th.split('>')[0] + '>'), !shown.has(id), `${id}: 見出しの hidden`);
  }
  const tr = r.text.split('<tr>').find((x) => x.includes('href="sku/a01"'));
  const cellToId = Object.fromEntries(LC.LIST_COLUMNS.map((c) => [c.cell, c.id]));
  const tds = [...tr.matchAll(/<td [^>]*data-col="([^"]+)"([^>]*)>/g)].map((m) => [cellToId[m[1]], / hidden/.test(m[2])]);
  assert.deepEqual(tds.map((x) => x[0]), want, 'セルも同じ並び');
  for (const [id, hid] of tds) assert.equal(hid, !shown.has(id), `${id}: セルの hidden`);
  assert.equal(tds.filter(([, hid]) => !hid).length, goodView().shown.length);
  // コードは左端 (横に送っても残る印)・列の数
  assert.match(thead(r.text), /^<tr><th scope="col" class="c-code sortable"/);
  assert.match(tr, /<td class="c-code" data-col="code">/);
  assert.match(r.text, /id="colcnt">7\/22</);   // 🆕 0084: メーカー・🆕 0087: 大きさ・重さ・🆕 0088: ブランド・段階の 3 列 (出さない) で 5 つ増えた
  // 仕入先の列: 単品 = コード + 名前・セット = 構成品から・代表なし = —。出している間は名前の下の「仕入先」の字を消す (class)
  assert.match(r.text, /<table class="tbl click show-sup" id="list-tbl">/);
  const cell = (code, id) => { const t = r.text.split('<tr>').find((x) => x.includes(`href="sku/${code}"`)); return new RegExp(`<td [^>]*data-col="${LC.COLUMN_BY_ID[id].cell}"[^>]*>([\\s\\S]*?)<\\/td>`).exec(t)[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim(); };
  assert.equal(cell('a01', 'sup'), '0002 ビーフリー');
  assert.equal(cell('a10', 'sup'), '0002 ビーフリー', '代表の仕入先 (ほかの仕入先 0001 は代表でない)');
  assert.equal(cell('b01', 'sup'), '構成品から');
  assert.equal(cell('a05', 'sup'), '—');
  assert.equal(cell('x01', 'sup'), '—');
  assert.match(tr, /<span class="sub sup-sub">仕入先 0002<\/span>/, '名前の下の字は残す (列を出している間は CSS で消す)');
  assert.ok(thOf(r.text, 'sup').includes('<span class="newtag" aria-hidden="true">新</span>'));
  // 列の板の材料 (JSON): 板に出す列 (注文残は出さない)・今の設定 (全部の列のまま)・いつもの列・保存してある
  const cfg = cfgOf(r.text);
  assert.deepEqual(cfg.columns.map((c) => c.id), want);
  assert.deepEqual(cfg.columns.filter((c) => c.fixed).map((c) => c.id), ['code']);
  assert.deepEqual(cfg.columns.filter((c) => !c.sortable).map((c) => c.id), ['img', 'fba', 'sold', 'flags', 'maker', 'pkg', 'brand', 'stage', 'end_on']);   // 🆕 0084 メーカー・🆕 0087 大きさ・重さ・🆕 0088 の 3 列も並べられない
  assert.deepEqual(cfg.view, goodView(), '設定は全部の列 (注文残も) のまま渡す = 保存し直しで注文残の設定を消さない');
  assert.deepEqual(cfg.defaultView, { order: [...LC.DEFAULT_VIEW.order], shown: [...LC.DEFAULT_VIEW.shown] });
  assert.deepEqual([cfg.saved, cfg.readable, cfg.who], [true, true, '中原']);
  assert.match(r.text, /<button type="button" class="btn sm colbtn" id="colbtn" aria-haspopup="dialog" aria-expanded="false" aria-controls="colpanel"/);
  assert.match(r.text, /<div class="colpanel" id="colpanel" role="dialog" aria-labelledby="cp-title" hidden>/);
  for (const id of ['cp-save', 'cp-reset', 'cp-x', 'collist']) assert.ok(r.text.includes(`id="${id}"`), id);
  assert.match(r.text, /中原 さんの設定 · 会社の PC と家の PC で同じになります/);
  assert.match(r.text, /<span id="fx-legend"><span class="fx">計算<\/span> = 構成品から計算した値 · <span class="fx warnx">仮<\/span> = 税率か送料が未入力のまま計算した利益<\/span>/, '「計算」「仮」の札の説明は残す');
  // 画面の JS (me-list.js) を読む・文法として読める・EJS のタグが残っていない
  const src = /<script src="(\/apps\/master-edit\/public\/me-list\.js\?v=[0-9a-f]{12})" defer><\/script>/.exec(r.text)?.[1];
  assert.ok(src, 'me-list.js を読む');
  const js = await (await fetch(ORIGIN + src, { headers: { 'x-test-session': 'editor' } })).text();
  new vm.Script(js);
  assert.ok(!/<%[=-]?|%>/.test(r.text), 'EJS のタグが画面に残っていない');
  // 注文残の権限がある人 = 注文残も描く (その人の設定 = いつもの列)
  r = await call('GET', '/', { session: 'admin' });
  assert.deepEqual(headIds(r.text), [...LC.DEFAULT_VIEW.order]);
  assert.ok(!/ hidden/.test(thOf(r.text, 'po').split('>')[0]), '注文残を出す');
  assert.ok(/ hidden/.test(thOf(r.text, 'sup').split('>')[0]), 'いつもの列 = 仕入先は出さない');
  assert.match(r.text, /<table class="tbl click" id="list-tbl">/);
  assert.match(r.text, /id="colcnt">17\/23</);   // 画像の列 (出す)・メーカー (0084)・大きさ・重さ (0087)・ブランド・段階の 3 列 (0088) (出さない)
  // 中身が無い一覧の行 = 全部の列にまたがる
  r = await call('GET', '/?q=' + encodeURIComponent('当たらない名前'));
  assert.match(r.text, /<td colspan="22" class="muted"/);   // 画像の列 + 🆕 0084 メーカー + 🆕 0087 大きさ・重さ + 🆕 0088 の 3 列
  // 名前の下の「仕入先」の字はいつもの列のときは出る (仕入先の列を出していない)
  assert.equal((await call('POST', '/api/view-prefs', { body: { order: [...LC.DEFAULT_VIEW.order], shown: [...LC.DEFAULT_VIEW.shown] } })).status, 200);
  r = await call('GET', '/');
  assert.match(r.text, /<table class="tbl click" id="list-tbl">/);
  assert.match(r.text, /<span class="sub sup-sub">仕入先 0002<\/span>/);
});

await ta('[6] 在庫の並び (ロジザード・セットは作れる数) = 一覧の値と同じ (画面・全部のコード・CSV)・読めない = コード順', async () => {
  // 写し (大文字の商品ID・同じ商品の 2 つのロケは足す) = 一覧の在庫の列と同じ読み
  setStock([['A01', 3], ['a01', 2], ['a02', 12], ['a03', 0], ['a04', 7], ['a06', 5], ['a08', 3], ['x01', 40], ['X02', 2]]);
  const map = new Map([['a01', 5], ['a02', 12], ['a03', 0], ['a04', 7], ['a06', 5], ['a08', 3], ['x01', 40], ['x02', 2]]);
  const extras = { stock: { ok: true, asOf: NOW.toISOString(), stale: false, map } };
  const rows = await shownRows(extras);
  assert.ok(distinctValues(rows, 'stock') >= 4);
  assert.deepEqual(rows.find((x) => x.code === 'b01').buildable, 2, 'b01 = min(5/2, 12/1)');
  for (const dir of ['asc', 'desc']) {
    const want = expectedOrder(rows, 'stock', dir);
    const page = await R.listSkus(db, { sort: 'stock', dir: dir === 'desc' ? 'desc' : '' }, { now: NOW, extras });
    assert.deepEqual(page.rows.map((x) => x.code), want, `在庫 ${dir}`);
    const codes = await R.listSkus(db, { sort: 'stock', dir: dir === 'desc' ? 'desc' : '' }, { now: NOW, extras, mode: 'codes' });
    assert.deepEqual(codes.codes, want, `在庫 ${dir}: 全部のコード`);
  }
  // 本物の router (写しを読む): 画面の 1 ページ目・全部のコード (コピー)・CSV が同じ並び
  for (const dir of ['asc', 'desc']) {
    const want = expectedOrder(rows, 'stock', dir);
    const qs = `?sort=stock${dir === 'desc' ? '&dir=desc' : ''}`;
    assert.deepEqual(listOrder((await call('GET', '/' + qs)).text), want, `画面 ${dir}`);
    assert.deepEqual(await codesOf(qs), want, `全部のコード ${dir} (コピーも写しを読んで並べる)`);
    const csv = await (await fetch(`${BASE}/list.csv${qs}`, { headers: { 'x-test-session': 'editor' } })).text();
    assert.deepEqual(csv.replace(/^\ufeff/, '').trim().split('\r\n').slice(1).map((l) => /^"([^"]*)"/.exec(l)[1]), want, `CSV ${dir}`);
  }
  // 写しが読めない = 値が全部空 = コード順 (画面・全部のコードとも)
  const none = await R.listSkus(db, { sort: 'stock', dir: 'desc' }, { now: NOW, extras: { stock: { ok: false, error: 'x' } } });
  assert.deepEqual(none.rows.map((x) => x.code), [...ALL_CODES].sort());
  setStock([]);
  assert.deepEqual(await codesOf('?sort=stock&dir=desc'), [...ALL_CODES].sort(), '写しが無い = コード順');
  assert.deepEqual(listOrder((await call('GET', '/?sort=stock&dir=desc')).text), [...ALL_CODES].sort());
});

await ta('[7] 画像の列 (10/17 中原さん): キャッシュの URL を出す (楽天を待たない)・無い = 取得中で 1 回だけ取りに行く・楽天に無い = 印・楽天のドメイン以外は出さない・列を出さない人は取りに行かない・CSV に入れない', async () => {
  // 画像のキャッシュ = picking.db (同じ DATA_DIR)。楽天の呼び出しは差し替え (実 API は叩かない)
  const PK = await import('../apps/picking/db.js');
  const IM = await import('../apps/picking/images.js');
  const LI = await import('../apps/master-edit/list-images.mjs');
  const pdb = PK.getDB();
  const put = (ne, o) => pdb.prepare(`INSERT OR REPLACE INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, variant_image_url, status, fetched_at)
    VALUES (?, ?, ?, NULL, ?, ?, ?)`).run(ne, o.mn ?? null, o.white ?? null, o.variant ?? null, o.status ?? 'ok', PK.utcNow());
  put('a01', { variant: 'https://image.rakuten.co.jp/b-faith/cabinet/a01-red.jpg', white: 'https://image.rakuten.co.jp/b-faith/cabinet/a01.jpg' });
  put('a02', { status: 'not_found' });
  put('a03', { white: 'https://evil.example.com/a03.jpg' });
  put('a06', { white: 'https://image.rakuten.co.jp/b-faith/cabinet/q"><script>x</script>.jpg' });
  put('b01', { mn: 'b01', white: 'https://image.rakuten.co.jp/b-faith/cabinet/set-b01.jpg' });   // セット自身の商品ページ
  put('b02', { mn: 'a04', white: 'https://image.rakuten.co.jp/b-faith/cabinet/a04-single.jpg' });   // ハイフン削りなどで単品の商品に当たった
  const calls = [];
  let open = null;
  const gate = new Promise((res) => { open = res; });   // 楽天の応答を止めておく (解決の途中で開き直す)
  LI.__setListImageDeps({
    loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map([['a04', 'a04']]) }),
    fetchDetails: async (mns) => {
      calls.push([...mns]);
      await gate;
      return { items: mns.includes('a04') ? [{ manageNumber: 'a04', whiteBgImage: { location: '/a04.jpg' } }] : [], failed: mns.filter((m) => m !== 'a04').map((m) => ({ manageNumber: m, reason: `Not found for inputs; manageNumber=${m}` })) };
    },
  });
  const drain = () => IM.queueEnsureImages([], '');
  const imgCell = (text, code) => {
    const tr = text.split('<tr>').find((x) => x.includes(`href="sku/${code}"`));
    return /<td class="c-img" data-col="img"([^>]*)>([\s\S]*?)<\/td>/.exec(tr);
  };
  try {
    assert.equal((await call('POST', '/api/view-prefs', { body: { order: [...LC.DEFAULT_VIEW.order], shown: [...LC.DEFAULT_VIEW.shown] } })).status, 200);
    let r = await call('GET', '/');
    assert.equal(r.status, 200);
    // 見出し = コードのすぐ後ろ・並べられない (リンクなし)
    assert.deepEqual(headIds(r.text).slice(0, 3), ['code', 'img', 'name']);
    assert.ok(!thOf(r.text, 'img').includes('class="sorth"'), '並べられない');
    // キャッシュ済み = <img> (大きさ固定・lazy・referrer を送らない・alt = 名前)・バリエーション画像が先
    assert.equal(imgCell(r.text, 'a01')[2], '<span class="thumb"><img src="https://image.rakuten.co.jp/b-faith/cabinet/a01-red.jpg" alt="あいう 石鹸" width="52" height="52" loading="lazy" decoding="async" referrerpolicy="no-referrer"></span>');
    assert.match(imgCell(r.text, 'b01')[2], /src="https:\/\/image\.rakuten\.co\.jp\/b-faith\/cabinet\/set-b01\.jpg"/, 'セット = セット自身の画像');
    assert.match(imgCell(r.text, 'b02')[2], /^<span class="thumb none not_own" title="セット自身の楽天の画像がありません/, 'セットに単品の商品の写真は出さない');
    assert.ok(!r.text.includes('a04-single.jpg'));
    // 楽天に無い = 印 (img は出さない)
    assert.match(imgCell(r.text, 'a02')[2], /^<span class="thumb none not_found" title="楽天に画像がありません">/);
    assert.ok(!imgCell(r.text, 'a02')[2].includes('<img'));
    // 楽天のドメイン以外 = 出さない
    assert.ok(!r.text.includes('evil.example.com'), 'ドメイン外の URL は画面に出さない');
    assert.match(imgCell(r.text, 'a03')[2], /thumb none not_found/);
    // URL の中の " < > は属性の外に出ない
    assert.match(imgCell(r.text, 'a06')[2], /src="https:\/\/image\.rakuten\.co\.jp\/b-faith\/cabinet\/q&#34;&gt;&lt;script&gt;x&lt;\/script&gt;\.jpg"/);
    assert.ok(!r.text.includes('<script>x</script>'));
    // キャッシュに無い = 取得中 (画面は待たずに出す)・裏で 1 回だけ取りに行く
    assert.match(imgCell(r.text, 'a04')[2], /^<span class="thumb none pending" title="楽天の画像を取得中[^"]*"><svg class="ic" aria-hidden="true"><use href="#i-image"\/><\/svg><span class="tw" aria-hidden="true">取得中<\/span>/);
    assert.match(imgCell(r.text, 'x02')[2], /thumb none pending/, '例外の SKU もコードで引く');
    r = await call('GET', '/');   // 解決が終わる前に開き直した = 積み増さない
    assert.match(imgCell(r.text, 'a04')[2], /thumb none pending/);
    open();
    await drain();
    assert.equal(calls.length, 1, `楽天は 1 回 (${JSON.stringify(calls)})`);
    assert.ok(calls[0].includes('a04'));
    assert.ok(!calls[0].includes('a01') && !calls[0].includes('a02'), 'キャッシュ済み・今日の「楽天に無い」は取りに行かない');
    r = await call('GET', '/');
    assert.match(imgCell(r.text, 'a04')[2], /src="https:\/\/image\.rakuten\.co\.jp\/b-faith\/cabinet\/a04\.jpg"/, '解決した後は画像');
    // 3 字のコードで変換表にも無い = 楽天に問い合わせる候補が無い = error (30 分後にまた見る。楽天は呼ばない)
    assert.match(imgCell(r.text, 'a05')[2], /thumb none error" title="楽天の画像を取れませんでした/);
    await drain();
    assert.equal(calls.length, 1, '開き直しても取りに行かない');
    // 列を出していない人 = セルは hidden・キャッシュは読む・取りに行かない
    pdb.prepare('DELETE FROM pk_product_images WHERE ne_code = ?').run('a05');
    assert.equal((await call('POST', '/api/view-prefs', { body: goodView() })).status, 200);
    r = await call('GET', '/');
    assert.match(imgCell(r.text, 'a05')[1], / hidden/);
    assert.match(imgCell(r.text, 'a05')[2], /thumb none uncached/);
    assert.match(imgCell(r.text, 'a01')[2], /<img src=/, '出していない列でもキャッシュの画像は描く (列の板で出したらすぐ見える)');
    await drain();
    assert.equal(calls.length, 1, '列を出していない = 取りに行かない');
    // CSV に画像は入れない
    const csv = await (await fetch(`${BASE}/list.csv`, { headers: { 'x-test-session': 'editor' } })).text();
    assert.ok(!/画像|rakuten\.co\.jp/.test(csv.split('\r\n')[0]) && !csv.includes('a01-red.jpg'), 'CSV に画像の列・URL は無い');
    // picking.db が読めない = 全部印で一覧は出す (画像は「あれば嬉しい」もの)
    LI.__setListImageDeps(null);
    pdb.exec('ALTER TABLE pk_product_images RENAME TO pk_product_images_x');
    try {
      r = await call('GET', '/');
      assert.equal(r.status, 200);
      assert.match(imgCell(r.text, 'a01')[2], /thumb none none/);
    } finally { pdb.exec('ALTER TABLE pk_product_images_x RENAME TO pk_product_images'); }
  } finally {
    LI.__setListImageDeps(null);
    await call('POST', '/api/view-prefs', { body: { order: [...LC.DEFAULT_VIEW.order], shown: [...LC.DEFAULT_VIEW.shown] } });
    try { pdb.close(); } catch { /* 閉じられなくてもよい */ }
  }
});

console.log('詳細検索の「名前で選ぶ」(仕入先・メーカー)');
// 🆕 2026-10-24: メーカー (0084) の名簿と商品ごとのメーカー。書くのは本来 ops.create_maker / ops.set_product_maker だけ = 試験は持ち主のロールで守りの印を付けて直接入れる
async function seedMakers() {
  await pg.exec(`begin;
    select set_config('ops.maker_protocol', '1', true);
    insert into core.makers (name, name_norm, active, created_by_id, created_request_id) values
      ('はちみつ本舗', core.maker_name_norm('はちみつ本舗'), true, 'naka@test', gen_random_uuid()),
      ('ミカワ', core.maker_name_norm('ミカワ'), true, 'naka@test', gen_random_uuid()),
      ('古いメーカー', core.maker_name_norm('古いメーカー'), false, 'naka@test', gen_random_uuid());
    insert into core.product_makers (product_id, maker_id, set_by, set_via, request_id)
      select k.product_id, m.maker_id, 'naka@test', 'sku_page', gen_random_uuid() from core.skus k join core.makers m
        on (k.code, m.name) in (('a01', 'はちみつ本舗'), ('a02', 'はちみつ本舗'), ('a03', 'ミカワ'), ('a04', '古いメーカー'));
    commit;`);
  const ids = Object.fromEntries((await pg.query('select name, maker_id::text as id from core.makers')).rows.map((r) => [r.name, r.id]));
  return { HONPO: ids['はちみつ本舗'], MIKAWA: ids['ミカワ'], OLD: ids['古いメーカー'] };
}
await ta('[8] メーカーで絞る (複数・メーカー未登録)・URL に残る (選ぶ欄の複数 / カンマ / 印)・並び・件数・CSV・知らない値は捨てる (SQL に入れない)・主キーで引く・仕入先を名前で選ぶ名簿', async () => {
  const { HONPO, MIKAWA, OLD } = await seedMakers();
  const codeAsc = await codesOf('');
  // 決まった形: none が先・番号は小さい順・カンマ。知らない値は捨てる
  assert.equal(R.normalizeFilters({ makers: [MIKAWA, HONPO, 'none', HONPO] }).makers, ['none', ...[HONPO, MIKAWA].sort((a, b) => a - b)].join(','));
  for (const bad of ['0', '01', 'abc', "1'or'1'='1", '1)--', '12345678901234567890', '-1', '1.5', '{1}', 'NONE', '__proto__']) assert.equal(R.normalizeFilters({ makers: bad }).makers, '', bad);
  assert.equal(R.normalizeFilters({ makers: '1;drop table core.skus' }).makers, '1', '区切り (; 空白) で分けて番号だけ残す');
  assert.equal(R.normalizeFilters({ makers: { a: '1' } }).makers, '', 'オブジェクト (?makers[a]=1) = 捨てる');
  // Codex R1 Medium: ?makers[toString]=1 (Express がオブジェクトにする) で 500 にしない・配列の中のオブジェクトも捨てる
  assert.equal(R.normalizeFilters({ makers: { toString: '1' } }).makers, '');
  assert.equal(R.normalizeFilters({ makers: ['2', { toString: '1' }, '3'] }).makers, '2,3');
  assert.equal((await call('GET', '/?makers[toString]=1')).status, 200, 'オブジェクトの入力で落ちない');
  assert.equal((await call('GET', '/api/codes?makers[toString]=1&makers[valueOf]=2')).status, 200);
  // Codex R1 Medium: 501 件選んだ = 500 件までを使って知らせる (黙って落とさない)
  const many = Array.from({ length: R.MULTI_MAX + 1 }, (_, i) => String(1000 + i));
  assert.equal(R.normalizeFilters({ makers: many }).makers.split(',').length, R.MULTI_MAX + 1);
  const cut = await R.listSkus(db, { makers: many.join(',') }, { now: NOW });
  assert.deepEqual([cut.multiCut, cut.total], [true, 0], '501 件 = 500 件まで使ったと知らせる');
  {
    // Codex R2 Low: 未登録 + 500 件 = 未登録も 1 件に数えて 500 件 (番号は 499 件だけ SQL へ)
    const cap = [];
    const c2 = await R.listSkus({ query: (t, p) => { cap.push(p || []); return pg.query(t, p); } }, { makers: ['none', ...many.slice(0, R.MULTI_MAX)].join(',') }, { now: NOW });
    const arr = cap.flat().find((x) => Array.isArray(x) && x.includes('1000'));
    assert.deepEqual([c2.multiCut, arr.length, c2.total], [true, R.MULTI_MAX - 1, 6]);
  }
  assert.ok(R.ADV_KEYS.includes('makers'));
  // 絞る (単品の商品の行ごと = 一覧の「メーカー」の列と同じ)
  const by = (qs) => codesOf(qs);
  assert.deepEqual(await by(`?makers=${HONPO}`), ['a01', 'a02']);
  assert.deepEqual(await by(`?makers=${HONPO}&makers=${MIKAWA}`), ['a01', 'a02', 'a03'], '選ぶ欄の複数 (?makers=1&makers=2)');
  assert.deepEqual(await by(`?makers=${encodeURIComponent(`${MIKAWA},${HONPO}`)}`), ['a01', 'a02', 'a03'], 'カンマで区切っても同じ');
  assert.deepEqual(await by(`?makers=${OLD}`), ['a04'], '使っていないメーカーでも絞れる');
  assert.deepEqual(await by('?makers=none'), ['a05', 'a06', 'a07', 'a08', 'a09', 'a10'], 'メーカー未登録 = 単品でメーカーが無い (セット・例外は入れない)');
  assert.deepEqual(await by(`?makers=none&makers=${MIKAWA}`), ['a03', 'a05', 'a06', 'a07', 'a08', 'a09', 'a10'], '未登録 + メーカー = どちらか');
  assert.deepEqual(await by(`?makers=${HONPO}&kind=set`), [], 'セットはメーカーを持たない');
  assert.deepEqual(await by('?makers=999999'), [], '名簿に無い番号 = 当たらない');
  assert.deepEqual(await by(`?makers=${HONPO}&sups=2`), ['a01'], 'ほかの詳細検索と一緒 (仕入先 0002 かつ はちみつ本舗)');
  // 知らない値 = 条件ごと捨てる (全部)・人の入力を SQL に入れない (番号は $番号の bigint[])
  for (const bad of ["1'or'1'='1", 'x)--', 'abc']) {
    sqlLog.length = 0;
    assert.deepEqual(await by(`?makers=${encodeURIComponent(bad)}`), codeAsc, bad);
    assert.ok(sqlLog.every((t) => !t.includes(bad) && !/drop table/i.test(t)), `${bad}: SQL に入れない`);
  }
  sqlLog.length = 0;
  await by(`?makers=${HONPO}`);
  assert.ok(sqlLog.some((t) => /x\.maker_id = any\(\$\d+::bigint\[\]\)/.test(t)) && sqlLog.every((t) => !t.includes(`= any('{${HONPO}}`)), '番号は $番号で渡す');
  // 並び・ページ分け・件数・CSV
  const rows = (await shownRows()).filter((x) => ['a01', 'a02', 'a03'].includes(x.code));
  assert.deepEqual(await by(`?makers=${HONPO},${MIKAWA}&sort=price&dir=desc`), expectedOrder(rows, 'price', 'desc'), '絞ったものを並べる');
  const p2 = await R.listSkus(db, { makers: `${HONPO},${MIKAWA}`, offset: '1' }, { now: NOW });
  assert.deepEqual([p2.total, p2.rows.map((x) => x.code)], [3, ['a02', 'a03']], 'ページ分け (offset) と件数');
  let r = await call('GET', `/?makers=${MIKAWA}&makers=${HONPO}&kind=single`);
  assert.equal(r.status, 200);
  assert.deepEqual(listOrder(r.text), ['a01', 'a02', 'a03']);
  assert.ok(r.text.includes('<b>3</b> 件'), '件数');
  const mk = [HONPO, MIKAWA].sort((a, b) => a - b).join('%2C');
  assert.match(r.text, new RegExp(`id="copy-all" data-url="api/codes\\?kind=single&amp;makers=${mk}"`), 'コピーのリンクも同じ条件 (決まった形)');
  assert.match(r.text, new RegExp(`id="csv-link" href="list\\.csv\\?kind=single&amp;makers=${mk}"`));
  assert.match(r.text, new RegExp(`<details class="more filters-more"[\\s\\S]*?<input type="hidden" name="makers" value="${mk.replace('%2C', ',')}">`), 'もっと絞る のフォームが持ち回る');
  assert.match(r.text, new RegExp(`<a class="chip[^"]*" href="\\?makers=${mk}"`), '区分の札 (全部) を押しても残る');
  assert.match(r.text, /<details class="more adv" open id="adv">[\s\S]*?使っている/, '詳細検索の板を開いておく');
  const csv = await (await fetch(`${BASE}/list.csv?makers=${HONPO}`, { headers: { 'x-test-session': 'editor' } })).text();
  assert.deepEqual(csv.replace(/^﻿/, '').trim().split('\r\n').slice(1).map((l) => /^"([^"]*)"/.exec(l)[1]), ['a01', 'a02'], 'CSV も同じ条件');
  // 選ぶ欄: 名簿 (使うメーカーが先・使っていない印)・選んでいる物・メーカー未登録
  const mkSel = /<select multiple class="in pk-native" name="makers" id="adv-makers"[^>]*>([\s\S]*?)<\/select>/.exec(r.text)[1];
  const opts = [...mkSel.matchAll(/<option value="([^"]+)"([^>]*)>([^<]*)<\/option>/g)].map((m) => [m[1], / selected/.test(m[2]), / data-off/.test(m[2]), m[3]]);
  assert.deepEqual(opts, [['none', false, false, 'メーカー未登録'], [HONPO, true, false, 'はちみつ本舗'], [MIKAWA, true, false, 'ミカワ'], [OLD, false, true, '古いメーカー (使っていない)']]);
  r = await call('GET', '/?makers=none&makers=987654');
  const opts2 = [.../<select multiple class="in pk-native" name="makers"[^>]*>([\s\S]*?)<\/select>/.exec(r.text)[1].matchAll(/<option value="([^"]+)"[^>]*?( selected)?>/g)].filter((m) => m[2]).map((m) => m[1]);
  assert.deepEqual(opts2, ['none', '987654'], '名簿に無い番号も選んだまま残す (条件を黙って落とさない)');
  assert.ok(r.text.includes('src="/apps/master-edit/public/me-pick.js?v='), '画面の JS');
  // 長い条件の印 (POST api/search): 選ぶ欄の複数 = 改行でつないで送る (me-shell.js)。印の中身 = 決まった形
  const sr = await call('POST', '/api/search', { body: { makers: `${MIKAWA}\n${HONPO}`, codes: 'a01\na02\na03\na04', sort: 'code', dir: 'desc' } });
  assert.equal(sr.status, 200, sr.text);
  assert.match(sr.j.url, /\?dir=desc&s=[A-Za-z0-9_-]+$/, '詳細検索の項目は印に');
  assert.deepEqual(await codesOf('?' + sr.j.url.split('?')[1]), ['a03', 'a02', 'a01'], '印の中身 (メーカー + コード)・並び');
  r = await call('GET', '/?' + sr.j.url.split('?')[1]);
  assert.match(r.text, new RegExp(`<option value="${HONPO}" data-name="はちみつ本舗" selected>`), '印で開いても選んだまま');
  assert.equal((await call('POST', '/api/search', { body: { makers: '1'.repeat(30) } })).status, 413, '1 つの値の長さの上限');
  // 表が読めない (0084 の前・ロールの流し直しの前) = 選んだメーカーは当たらない・未登録 = 単品の全部・欄は出さない (条件は隠した欄で持ち回る)
  await pg.query('revoke select on core.product_makers from master_edit');
  try {
    assert.deepEqual(await by(`?makers=${HONPO}`), []);
    assert.deepEqual(await by('?makers=none&kind=single'), ['a01', 'a02', 'a03', 'a04', 'a05', 'a06', 'a07', 'a08', 'a09', 'a10']);
    r = await call('GET', `/?makers=${HONPO}`);
    assert.equal(r.status, 200);
    assert.ok(!r.text.includes('id="adv-makers"'), 'メーカーの欄は出さない');
    assert.ok(r.text.includes(`<input type="hidden" name="makers" value="${HONPO}">`), '条件は隠した欄で持ち回る');
  } finally { await pg.query('grant select on core.product_makers to master_edit'); }
  // 速さ: 商品ごとに主キー (product_id) で引ける形 (列を関数で包まない)。小さい表は planner が全部読むので seqscan を止めて見る
  {
    const cap = [];
    await R.listSkus({ query: (t, p) => { cap.push([t, p]); return pg.query(t, p); } }, { makers: `none,${HONPO}` }, { now: NOW, mode: 'codes' });
    const [t, p] = cap.find(([x]) => /core\.product_makers x where x\.product_id = s\.product_id and x\.maker_id/.test(x));
    await pg.exec('set enable_seqscan = off');
    try {
      const plan = JSON.stringify((await pg.query(`explain (format json) ${t}`, p)).rows[0]);
      assert.match(plan, /product_makers_pkey|ix_product_makers_maker/, `メーカーの表は索引で引く ${plan.slice(0, 400)}`);
    } finally { await pg.exec('reset enable_seqscan'); }
  }
  // 仕入先を名前で選ぶ名簿: 全部の仕入先 (取引中が先・取引なしは印)・選ぶとコードの欄に足す (画面の JS) = コードの欄の条件のまま
  await pg.query("insert into core.suppliers (company_id, code, name, active) values (1, '0099', 'やめた問屋 <b>', false)");
  r = await call('GET', '/');
  const sup = /<div class="pick" id="sup-pick" data-pick="into" data-into="adv-sups"[^>]*hidden>[\s\S]*?<select multiple id="sup-pick-src" hidden aria-hidden="true" tabindex="-1">([\s\S]*?)<\/select>/.exec(r.text);
  assert.ok(sup, '仕入先の選ぶ部品 (JS が無いときは隠す)');
  assert.deepEqual([...sup[1].matchAll(/<option value="([^"]+)" data-code="[^"]+" data-name="([^"]*)"( data-off)?>/g)].map((m) => [m[1], m[2], !!m[3]]),
    [['0001', 'AMC', false], ['0002', 'ビーフリー', false], ['0010', '東和', false], ['0099', 'やめた問屋 &lt;b&gt;', true]], '取引中が先・取引なしは印・名前は字のまま (HTML にしない)');
  assert.deepEqual(await by('?sups=0099'), [], '選んだ仕入先 = コードの欄の条件');
  assert.deepEqual(await by('?sups=0002%0A0010'), ['a01', 'a03', 'a06', 'a09', 'a10']);
  const choices = await R.advPickChoices(db);
  assert.deepEqual(choices.makers.map((m) => m[1]), ['はちみつ本舗', 'ミカワ', '古いメーカー']);
  assert.deepEqual(choices.makers.map((m) => m[2]), [true, true, false]);
});

server.close();
try { mirrorDb.close(); fs.rmSync(DATA_TMP, { recursive: true, force: true }); } catch { /* 消せなくてもよい */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
