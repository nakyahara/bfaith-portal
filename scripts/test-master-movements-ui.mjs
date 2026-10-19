/**
 * test-master-movements-ui.mjs — 商品の画面の「入出庫の履歴」と「月別のまとめ」(public/me-moves.js・CompanyDB構想 26 の PR-1・PR-2) を、描いたページで本当に動かす試験
 *
 * Company DB = PGlite (夜間ロードで商品を作り、見本は scripts/fixtures/movements-seed.mjs)・本物の router を express に載せ、Playwright (Chromium) で開く。
 * 確かめること:
 *   1 単品 (発注アプリの権限あり): 表に出荷・仕入・「在庫の調整 (ずれ)」(薄い行・理由)・在庫数・ロジザードの伝票NO・最後の締めと「いまのロジザード」の見出し・月別 13 か月 (仕入・発注の欄あり・空欄の月)
 *   2 「もっと前を見る」で続きが足される (区間の途中で切れない・錨の前は在庫数「—」)・最後まで読んだらボタンを隠す
 *   3 発注アプリの権限が無い人: 仕入の行も仕入・発注の欄も出さない (見出しにそう書く)
 *   4 セット: 表を出さず、構成品の画面へのリンク (画面の JS も読まない)
 *   5 スマホ 390 の幅でページが横にはみ出さない (表は表の箱の中で横に送る)・履歴の枠は画面の高さの 65% まで
 *   6 履歴の枠 (10/19 中原さん「下にスクロールするのが長すぎる」): 高さを決めて枠の中でスクロール (14 行ほど)・見出しの行は枠の上に残る・
 *     件数 (新しい方から N 件 / 全 N 件・枠の中でスクロール)・「もっと前を見る」は枠の中の末尾に足し、枠のスクロールの位置を変えない・
 *     Tab で枠に来て (tabindex・role=region・aria-label) キーで送れる・その下の月別のまとめはページの近くにある
 *   7 件数の言い方: 1 日の出荷を全部は読めなかった = 「読めた N 件」(全 と言わない)・画面の高さが変わったら「枠の中でスクロール」を数え直す
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 を付けたときだけ
 * 使い方: node scripts/test-master-movements-ui.mjs
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import express from 'express';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の JS の試験 (test-master-movements-ui) を飛ばす'); process.exit(0); }
const SCRATCH = await temporaryTestDataDir(import.meta.url, 'master-movements-ui-');
process.env.DATA_DIR = SCRATCH;
let chromium, browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の JS の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / この試験だけ飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const { seedMovementsDb, seedMovementsMirror, SEED_NOW } = await import('./fixtures/movements-seed.mjs');
const quiet = () => {};

// ── Company DB (PGlite): 持ち主のロール deploy で migration・ロール・夜間ロードで商品 (単品 2・セット 1) ──
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const sku = (code, name, kind) => ({ code, name, kind, taxRate: 0.1, taxClass: 'STANDARD_10', handling: 'active', salesClass: kind === 'set' ? null : 3,
  cost: { jpy: 100, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2 });
const lr = await runInitialLoad(db, {
  skus: [sku('mv-a', '動きの見本 A', 'single'), sku('mv-b', '動きの見本 B', 'single'), sku('mv-set', '動きの見本 セット', 'set')], variationGroups: [],
  setComponents: [{ parentCode: 'mv-set', childCode: 'mv-a', qty: 1, source: 'ne' }, { parentCode: 'mv-set', childCode: 'mv-b', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_mv' },
}, { log: quiet, runId: 'load_mv', ownership: MASTER_OWNERSHIP, now: new Date(SEED_NOW - 30 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
const idOf = async (code) => (await pg.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0].id;
await seedMovementsDb(pg, db, { skuA: await idOf('mv-a'), skuB: await idOf('mv-b') });

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const mirrorDb = initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
seedMovementsMirror(mirrorDb);

// ── 本物の router ──
const { default: router, __setPgClientFactory, __setClock, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/mvui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/mvui';
process.env.MASTER_EDITORS = 'naka@test';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  // 1 つの PGlite を順番に使う (同時の要求でロールが混ざらない)
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setClock(() => SEED_NOW);
__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
const app = express();
app.set('view engine', 'ejs');
let SESSION_APPS = ['master-edit', 'purchase-orders'];
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: SESSION_APPS }; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;

let passed = 0;
async function ta(name, fn, viewport = { width: 1440, height: 900 }) {
  const ctx = await browser.newContext({ viewport, locale: 'ja-JP', timezoneId: 'Asia/Tokyo' });
  const p = await ctx.newPage();
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  try {
    await fn(p);
    assert.deepEqual(errors, [], '画面の JS の例外');
    passed++; console.log(`  ok  ${name}`);
  } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
  await ctx.close();
}
const cells = (p, sel) => p.$$eval(sel, (trs) => trs.map((tr) => [...tr.querySelectorAll('td')].map((td) => td.textContent.trim())));

try {
  await ta('[1] 単品 (発注アプリの権限あり): 出荷・仕入・在庫の調整 (ずれ)・在庫数・ロジザードの伝票NO・見出し・月別 13 か月', async (p) => {
    await p.goto(B + '/sku/mv-a');
    await p.waitForSelector('#mv-rows tr');
    const rows = await cells(p, '#mv-rows tr');
    // 日時 / 処理 / 入庫 / 出庫 / 在庫数 / 伝票番号 / 店舗・仕入先 / 理由 / 担当者
    assert.deepEqual(rows.map((r) => [r[1], r[2], r[3], r[4]]), [
      ['出荷', '', '1', '117'], ['在庫の調整 (ずれ)', '', '2', '118'], ['出荷', '', '3', '120'],
      ['在庫の調整 (ずれ)', '2', '', '123'], ['出荷', '', '2', '121'], ['在庫の調整 (ずれ)', '', '2', '123'], ['仕入', '50', '', '125'],
      ['在庫の調整 (ずれ)', '', '5', '75'], ['出荷', '', '5', '80'], ['出荷', '', '5', '85'], ['出荷', '', '10', '90'], ['出荷', '', '4', '—']]);
    assert.equal(rows[0][0], '2026/10/07 12:00');
    assert.match(rows[6][0], /^2026\/10\/05日付だけ$/);
    assert.equal(rows[6][5], 'LZ-1ロジザードの伝票NO');
    assert.deepEqual([rows[6][6], rows[6][8]], ['AMC', 'naka@test']);
    assert.deepEqual([rows[9][6], rows[10][6]], ['FBA納品', 'Amazon']);
    assert.match(rows[3][7], /時刻のずれの見込み/);
    assert.equal(await p.$$eval('#mv-rows tr.mv-adj', (x) => x.length), 4);
    const head = await p.textContent('#mv-head');
    assert.match(head, /最後の締め 2026\/10\/06 \(18:00 の取得\)/);
    assert.match(head, /2026\/10\/01 より前は在庫数を出せません/);
    assert.match(head, /いまのロジザード 2026\/10\/07 11:00 で合わせています/);
    assert.equal(await p.isHidden('#mv-more'), true);
    // 「枠の中でスクロール」は枠に入りきらないときだけ添える
    const over = await p.evaluate(() => { const b = document.getElementById('mv-box'); return b.scrollHeight > b.clientHeight + 1; });
    assert.equal(await p.textContent('#mv-msg'), '全 12 件' + (over ? ' (枠の中でスクロール)' : ''));
    await p.waitForSelector('#mo-rows tr');
    const mo = await cells(p, '#mo-rows tr');
    assert.equal(mo.length, 13);
    assert.deepEqual(await p.$$eval('#mo-tbl thead th', (x) => x.map((th) => th.textContent)), ['月', '受注数', '売上数', '仕入数', '発注数', '受注金額', '売上金額', '仕入金額', '発注金額']);
    assert.deepEqual(mo[0], ['2026/10', '32', '26', '50', '0', '', '', '5,000', '0']);
    assert.deepEqual(mo[1], ['2026/09', '4', '4', '0', '80', '', '', '0', '29,760']);
    assert.deepEqual(mo[4], ['2026/06', '0', '0', '', '', '', '', '', ''], '発注アプリの前 = 空欄');
    if (process.env.MASTER_MOVES_SHOT) await p.locator('#p-moves').screenshot({ path: process.env.MASTER_MOVES_SHOT });   // 目で見るとき (写しの置き場)
  });

  await ta('[2] もっと前を見る: 続きを足す (区間の途中で切れない・錨の前は「—」)・最後まで読んだらボタンを隠す', async (p) => {
    await p.goto(B + '/sku/mv-b');
    await p.waitForSelector('#mv-rows tr');
    assert.equal(await p.$$eval('#mv-rows tr', (x) => x.length), 225);
    assert.equal(await p.isVisible('#mv-more'), true);
    await p.click('#mv-more');
    await p.waitForFunction(() => document.querySelectorAll('#mv-rows tr').length === 255);
    const last = await cells(p, '#mv-rows tr:last-child');
    assert.deepEqual([last[0][0], last[0][4]], ['2026/09/30 10:00', '—']);
    assert.equal(await p.isHidden('#mv-more'), true);
    assert.equal(await p.textContent('#mv-msg'), '全 255 件 (枠の中でスクロール)');
  });

  await ta('[3] 発注アプリの権限が無い人: 仕入の行も仕入・発注の欄も出さない (見出しにそう書く)', async (p) => {
    SESSION_APPS = ['master-edit'];
    try {
      await p.goto(B + '/sku/mv-a');
      await p.waitForSelector('#mv-rows tr');
      const rows = await cells(p, '#mv-rows tr');
      assert.equal(rows.some((r) => r[1] === '仕入'), false);
      assert.ok(rows.some((r) => r[1] === '在庫の調整 (ずれ)' && r[2] === '48' && /仕入もここに入っています/.test(r[7])));
      assert.match(await p.textContent('#mv-head'), /発注アプリの権限が無いので仕入の行は出しません/);
      await p.waitForSelector('#mo-rows tr');
      assert.deepEqual(await p.$$eval('#mo-tbl thead th', (x) => x.map((th) => th.textContent)), ['月', '受注数', '売上数', '受注金額', '売上金額']);
      assert.deepEqual((await cells(p, '#mo-rows tr'))[0], ['2026/10', '32', '26', '', '']);
    } finally { SESSION_APPS = ['master-edit', 'purchase-orders']; }
  });

  await ta('[4] セット: 表を出さず構成品の画面へのリンク・画面の JS も読まない', async (p) => {
    await p.goto(B + '/sku/mv-set');
    assert.match(await p.textContent('#mv-set'), /セットには在庫がありません/);
    assert.deepEqual(await p.$$eval('#mv-set a', (x) => x.map((a) => [a.textContent, new URL(a.href).pathname])), [['mv-a', '/apps/master-edit/sku/mv-a'], ['mv-b', '/apps/master-edit/sku/mv-b']]);
    assert.equal(await p.$('#mv-tbl'), null);
    assert.equal(await p.$('script[src*="me-moves.js"]'), null);
  });

  await ta('[5] スマホ 390: ページが横にはみ出さない (表は表の箱の中で横に送る)', async (p) => {
    await p.goto(B + '/sku/mv-a');
    await p.waitForSelector('#mv-rows tr');
    await p.waitForSelector('#mo-rows tr');
    const w = await p.evaluate(() => ({ doc: document.documentElement.scrollWidth, view: window.innerWidth, box: document.querySelector('#p-moves .tblwrap').scrollWidth > document.querySelector('#p-moves .tblwrap').clientWidth }));
    assert.ok(w.doc <= w.view, `横にはみ出す: ${w.doc} > ${w.view}`);
    assert.equal(w.box, true, '表は箱の中で横に送る');
    // 行の多い商品: 枠は画面の高さの 65% まで (844 × 0.65 = 548)・横も枠の中で送る・ページははみ出さない
    await p.goto(B + '/sku/mv-b');
    await p.waitForSelector('#mv-rows tr');
    const v = await p.evaluate(() => { const b = document.getElementById('mv-box'); return { h: b.clientHeight, sh: b.scrollHeight, sw: b.scrollWidth > b.clientWidth, doc: document.documentElement.scrollWidth, view: window.innerWidth }; });
    assert.ok(v.h <= 549 && v.sh > v.h, `スマホの枠の高さ ${v.h}`);
    assert.equal(v.sw, true, '表は枠の中で横に送る');
    assert.ok(v.doc <= v.view, `横にはみ出す: ${v.doc} > ${v.view}`);
  }, { width: 390, height: 844 });

  await ta('[6] 履歴の枠: 高さを決めて中でスクロール・見出しの行が残る・件数・続きは枠の中に足してスクロールの位置を変えない・キーボード', async (p) => {
    await p.goto(B + '/sku/mv-b');
    await p.waitForSelector('#mv-rows tr');
    assert.equal(await p.textContent('#mv-msg'), '新しい方から 225 件 (枠の中でスクロール) · もっと前があります');
    const a11y = await p.$eval('#mv-box', (b) => ({ tab: b.getAttribute('tabindex'), role: b.getAttribute('role'), label: b.getAttribute('aria-label'), inBox: !!document.getElementById('mv-rows').closest('#mv-box') }));
    assert.deepEqual(a11y, { tab: '0', role: 'region', label: '入出庫の履歴の表 (枠の中でスクロール)', inBox: true });
    const m = await p.evaluate(() => {
      const b = document.getElementById('mv-box');
      b.scrollIntoView({ block: 'center' });   // 上の帯 (sticky) に隠れない所で測る
      const rows = [...document.querySelectorAll('#mv-rows tr')];
      const head = document.querySelector('#mv-tbl thead tr').getBoundingClientRect().height;
      const bt = b.getBoundingClientRect();
      const seen = rows.filter((r) => { const x = r.getBoundingClientRect(); return x.top >= bt.top + head - 1 && x.bottom <= bt.bottom + 1; }).length;
      return { h: b.clientHeight, sh: b.scrollHeight, seen, gap: document.getElementById('p-monthly').getBoundingClientRect().top - bt.bottom };
    });
    console.log(`      (枠の高さ ${m.h}px・中身 ${m.sh}px・見える行 ${m.seen}・枠の下から月別のまとめまで ${Math.round(m.gap)}px)`);
    assert.ok(m.h <= 600 && m.h >= 400, `枠の高さ ${m.h} (min(600px, 65vh) = 900 の 65% = 585)`);
    assert.ok(m.sh > m.h * 5, '225 行は枠の中でスクロール');
    assert.ok(m.seen >= 12 && m.seen <= 15, `見える行 ${m.seen} (12〜15 行ほど)`);
    assert.ok(m.gap < 400, '月別のまとめが枠のすぐ下にある (225 行でページが伸びない)');
    // 枠の中を送っても見出しの行は枠の上に残る
    await p.$eval('#mv-box', (b) => { b.scrollTop = 2000; });
    const stick = await p.evaluate(() => {
      const b = document.getElementById('mv-box').getBoundingClientRect();
      const th = document.querySelector('#mv-tbl thead th').getBoundingClientRect();
      const under = document.elementFromPoint(th.left + 5, th.top + th.height / 2);
      return { d: th.top - b.top, isHead: !!(under && under.closest('thead')), bg: getComputedStyle(document.querySelector('#mv-tbl thead th')).backgroundColor };
    });
    assert.match(stick.bg, /^rgb\(\d+, \d+, \d+\)$/, `見出しの背景は塗りつぶし (透けない): ${stick.bg}`);
    assert.ok(stick.d >= 0 && stick.d <= 2, `見出しの行が枠の上に残る (${stick.d})`);
    assert.equal(stick.isHead, true, '見出しの行が行の上に描かれる (下の行が透けない)');
    // 「もっと前を見る」= 枠の中の末尾に足す・枠のスクロールの位置はそのまま
    const before = await p.$eval('#mv-box', (b) => b.scrollTop);
    assert.equal(before, 2000);
    await p.click('#mv-more');
    await p.waitForFunction(() => document.querySelectorAll('#mv-rows tr').length === 255);
    assert.equal(await p.$eval('#mv-box', (b) => b.scrollTop), before, '続きを足してもスクロールの位置を変えない');
    assert.equal(await p.$eval('#mv-rows tr:last-child', (tr) => !!tr.closest('#mv-box')), true);
    // キーボード: Tab で枠に来られる・矢印 / PageDown で送れる
    await p.$eval('#mv-box', (b) => { b.scrollTop = 0; });
    await p.focus('#mv-box');
    assert.equal(await p.evaluate(() => document.activeElement.id), 'mv-box');
    await p.keyboard.press('PageDown');
    await p.waitForFunction(() => document.getElementById('mv-box').scrollTop > 100);
  });

  await ta('[7] 件数の言い方: 1 日の出荷を全部は読めなかった = 「全」と言わない・画面の高さが変わったら「枠の中でスクロール」を数え直す', async (p) => {
    // API の答えを差し替える (読めなかった日がある・続きは無い・6 行)
    const row = (i) => ({ kind: 'ship', at: new Date(SEED_NOW - i * 3600e3).toISOString(), label: '出荷', in: null, out: 1, balance: null, slip: 'S' + i, party: '楽天', reason: '', actor: null });
    await p.route('**/api/sku/mv-a/movements*', (r) => r.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, rows: [0, 1, 2, 3, 4, 5].map(row), next: null, truncated: true, lastClose: null, firstDay: null }) }));
    await p.goto(B + '/sku/mv-a');
    await p.waitForFunction(() => document.querySelectorAll('#mv-rows tr').length === 6);
    const cutMsg = '1 日の出荷が多すぎて全部は読めませんでした (その日の在庫数は出していません) · 読めた 6 件';
    assert.equal(await p.textContent('#mv-msg'), cutMsg + ' (枠の中でスクロール)', '低い画面 (300 の 65% = 195px) では 6 行が入りきらない');
    await p.setViewportSize({ width: 1440, height: 900 });
    await p.waitForFunction((t) => document.getElementById('mv-msg').textContent === t, cutMsg);
  }, { width: 1440, height: 300 });
} finally {
  await browser.close();
  server.close();
}
console.log(`\n${passed} ok`);
