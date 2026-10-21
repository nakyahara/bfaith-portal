/**
 * test-master-sales-chart-ui.mjs — 商品の画面の「販売数のグラフ」(public/me-sales-chart.js・10/17 中原さん) を、描いたページで本当に動かす試験
 *
 * Company DB = PGlite (夜間ロードで商品を作り、見本は scripts/fixtures/sales-chart-seed.mjs)・本物の router を express に載せ、Playwright (Chromium) で開く。
 * 確かめること:
 *   1 単品: 週 (52) の積み上げの棒・凡例 (モールの名前と合計 = 色だけに頼らない)・まとめの文・在庫のグラフ (別のグラフ)・欠品の帯・途中の週は薄い・表 (新しい順)
 *   2 切り替え (月 24・日 90): 押した印 (aria-pressed)・まとめと表が変わる・記録の前の月は空欄
 *   3 キーボード: グラフを選ぶと最後の期間の値が出る・← → で動く・読み上げの文 (aria-live) にも同じ値 / かざすと値が出る
 *   4 セット: そのセットとして売れた数・在庫のグラフは出さない
 *   5 スマホ 390 の幅でページが横にはみ出さない (グラフは箱の幅で描く)
 *   6 読めない (関数の実行権が無い) = 言葉だけ出す (グラフは出さない・画面のほかの所は動く)
 *   7 時間切れ (8 秒) = 言葉と「もう一度読む」→ ?wait=long で読み直す: 読む間はボタンを止めて「読み込み中…」・2 度押しでも 1 回だけ読む・
 *     また時間切れ = 言葉とボタンがまた出る・読めたらグラフ (ボタンは消える)・読めない (時間切れ以外) はボタンを出さない / 8 スマホ 390 でボタンがはみ出さない
 *   🆕 10/19: グラフは商品の画面のタブ「販売数のグラフ」の中 (開いたときに読む) = #tab=sales で開く (タブそのものの試験は test-master-edit-ui [80]〜[81])
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 を付けたときだけ
 * 使い方: node scripts/test-master-sales-chart-ui.mjs
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import express from 'express';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の JS の試験 (test-master-sales-chart-ui) を飛ばす'); process.exit(0); }
const SCRATCH = await temporaryTestDataDir(import.meta.url, 'master-sales-chart-ui-');
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
const { createMasterEditRoles, SALES_CHART_FUNCTIONS } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const { seedSalesChartDb, SALES_NOW } = await import('./fixtures/sales-chart-seed.mjs');
const quiet = () => {};

// ── Company DB (PGlite): 持ち主のロール deploy で migration・ロール・夜間ロードで商品 (単品 2・セット 2 = 入れ子) ──
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
  skus: [sku('sc-a', '販売数の見本 A', 'single'), sku('sc-b', '販売数の見本 B', 'single'), sku('sc-s', '販売数の見本 セット', 'set'), sku('sc-s2', '販売数の見本 大きいセット', 'set')], variationGroups: [],
  setComponents: [{ parentCode: 'sc-s', childCode: 'sc-a', qty: 2, source: 'ne' }, { parentCode: 'sc-s', childCode: 'sc-b', qty: 1, source: 'ne' },
    { parentCode: 'sc-s2', childCode: 'sc-s', qty: 1, source: 'ne' }, { parentCode: 'sc-s2', childCode: 'sc-a', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: [], primarySuppliers: [], reorder: { available: true, runId: 'pml_sc' },
}, { log: quiet, runId: 'load_sc', ownership: MASTER_OWNERSHIP, now: new Date(SALES_NOW - 30 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
const idOf = async (code) => Number((await pg.query('select sku_id from core.skus where code = $1', [code])).rows[0].sku_id);
await seedSalesChartDb(pg, db, { A: await idOf('sc-a'), B: await idOf('sc-b'), S: await idOf('sc-s'), S2: await idOf('sc-s2') });
await pg.query('set role deploy');

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

// ── 本物の router ──
const { default: router, __setPgClientFactory, __setClock, __setShippingRatesProvider, SALES_CHART_LIMITS } = await import('../apps/master-edit/router.mjs');
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/scui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/scui';
process.env.MASTER_EDITORS = 'naka@test';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setClock(() => SALES_NOW);
__setShippingRatesProvider(async () => new Map([['S02', { method: '宅急便', cost: 520 }]]));
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
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
const ready = (p) => p.waitForFunction(() => !document.getElementById('sc-chart').hidden && document.querySelector('#sc-sales svg'));

try {
  await ta('[1] 単品 (週): 積み上げの棒・凡例 (名前と合計)・まとめ・在庫のグラフ (別)・欠品の帯・途中の週は薄い・表', async (p) => {
    await p.goto(B + '/sku/sc-a#tab=sales');
    await ready(p);
    assert.match(await p.textContent('#sc-sum'), /直近 52 週の合計 18 個 \(週平均 [\d.]+ 個・途中の週を除く\) · いちばん多い週 2026\/10\/12〜10\/18 の 11 個 · うちセット経由 5 個 · 在庫 2026\/10\/16 の締め 3 個 · 欠品 \(在庫 0\) の日 1 日/);
    const legend = await p.$$eval('#sc-legend li', (x) => x.map((li) => li.textContent.trim()));
    assert.deepEqual(legend.slice(0, 4), ['楽天3', 'Amazon8', 'Yahoo!3', 'その他 (au PAY・Qoo10・LINE ギフト・メルカリ)4']);
    assert.ok(legend.some((x) => /在庫/.test(x)) && legend.some((x) => /欠品 \(在庫 0\) の日がある週/.test(x)) && legend.some((x) => /薄い棒 = 途中の週/.test(x)), legend.join(' / '));
    const segs = await p.$$eval('#sc-sales svg .sc-c1, #sc-sales svg .sc-c2, #sc-sales svg .sc-c3, #sc-sales svg .sc-c4', (x) => x.map((e) => e.getAttribute('class')));
    assert.deepEqual(segs.sort(), ['sc-c1', 'sc-c2', 'sc-c3', 'sc-c4', 'sc-c4'].sort(), '棒の段: 9 月 1 日の週 (au PAY)・10 月 1 日の週 (メルカリ)・10/10 の週 (Yahoo!)・最後の週 (楽天 + Amazon)');
    assert.equal(await p.$$eval('#sc-sales svg .sc-partial', (x) => x.length), 1, '途中の週は 1 本');
    assert.equal(await p.$$eval('#sc-sales svg .sc-out', (x) => x.length), 1, '欠品の帯 (10/14 の週)');
    assert.equal(await p.$$eval('#sc-stock svg .sc-out', (x) => x.length), 1);
    assert.equal(await p.$$eval('#sc-stock svg .sc-line', (x) => x.length), 1);
    assert.match(await p.textContent('#sc-stock-h'), /在庫 \(ロジザードの倉庫・その週の最後の締め\) · 2026\/10\/13 から/);
    assert.equal(await p.$eval('#sc-sales svg', (s) => s.getAttribute('aria-hidden')), 'true', '絵は読み上げない (まとめ・表で読む)');
    assert.deepEqual(await p.$$eval('#sc-thead th', (x) => x.map((th) => th.textContent)), ['期間', '楽天', 'Amazon', 'Yahoo!', 'その他', '合計', 'うちセット経由', '在庫 (最後の締め)', '欠品の日']);
    const rows = await cells(p, '#sc-rows tr');
    assert.equal(rows.length, 52);
    assert.deepEqual(rows[0], ['2026/10/12〜10/18途中 (前日まで)', '3', '8', '0', '0', '11', '2', '310/16 の締め', '1']);
    assert.deepEqual(rows[1], ['2026/10/5〜10/11', '0', '0', '3', '0', '3', '3', '', '']);
    if (process.env.MASTER_SALES_SHOT) await p.locator('#p-sales').screenshot({ path: process.env.MASTER_SALES_SHOT });   // 目で見るとき (写しの置き場)
  });

  await ta('[2] 切り替え: 月 (24 か月・記録の前は空欄)・日 (90 日)・押した印', async (p) => {
    await p.goto(B + '/sku/sc-a#tab=sales');
    await ready(p);
    await p.click('#sc-unit button[data-v="month"]');
    assert.equal(await p.getAttribute('#sc-unit button[data-v="month"]', 'aria-pressed'), 'true');
    assert.equal(await p.getAttribute('#sc-unit button[data-v="week"]', 'aria-pressed'), 'false');
    assert.match(await p.textContent('#sc-sum'), /直近 24 か月の合計 18 個 \(月平均 [\d.]+ 個・途中の月を除く\) · いちばん多い月 2026年10月 の 15 個/);
    let rows = await cells(p, '#sc-rows tr');
    assert.equal(rows.length, 24);
    assert.deepEqual(rows[0], ['2026年10月途中 (前日まで)', '3', '8', '3', '1', '15', '5', '310/16 の締め', '1']);
    assert.deepEqual(rows[1], ['2026年9月', '0', '0', '0', '3', '3', '0', '', '']);
    assert.deepEqual(rows[23], ['2024年11月注文の記録の前', '', '', '', '', '', '', '', ''], '記録の前 = 空欄 (0 と出さない)');
    assert.match(await p.textContent('#sc-stock-h'), /その月の最後の締め/);
    await p.click('#sc-unit button[data-v="day"]');
    assert.match(await p.textContent('#sc-sum'), /直近 90 日の合計 18 個 \(1 日平均 0\.2 個\) · いちばん多い日 2026\/10\/15 \(木\) の 8 個/);
    rows = await cells(p, '#sc-rows tr');
    assert.equal(rows.length, 90);
    assert.deepEqual(rows[0], ['2026/10/17 (土)', '0', '0', '0', '0', '0', '0', '', '']);
    assert.deepEqual(rows[1], ['2026/10/16 (金)', '3', '0', '0', '0', '3', '0', '3', '0']);
    assert.deepEqual(rows[3].slice(-2), ['0', '1'], '10/14 = 在庫 0 = 欠品の日');
  });

  await ta('[3] キーボード: 選ぶと最後の期間・← → で動く・読み上げにも同じ値 / かざすと値が出る', async (p) => {
    await p.goto(B + '/sku/sc-a#tab=sales');
    await ready(p);
    await p.focus('#sc-chart');
    await p.waitForSelector('#sc-tip:not([hidden])');
    assert.match(await p.textContent('#sc-tip'), /2026\/10\/12〜10\/18 \(途中\)合計11 個その他0Yahoo!0Amazon8楽天3うちセット経由2在庫 \(10\/16 の締め\)3欠品 \(在庫 0\) の日1 日/);
    assert.match(await p.textContent('#sc-live'), /2026\/10\/12〜10\/18 \(途中・前日まで\)、合計 11 個、楽天 3、Amazon 8、Yahoo! 0、その他 0、うちセット経由 2、在庫 3 個 \(10\/16 の締め\)・欠品の日 1 日/);
    await p.keyboard.press('ArrowLeft');
    assert.match(await p.textContent('#sc-tip'), /^2026\/10\/5〜10\/11合計3 個/);
    assert.match(await p.textContent('#sc-live'), /Yahoo! 3、その他 0、うちセット経由 3、在庫の記録なし/);
    await p.keyboard.press('Home');
    assert.match(await p.textContent('#sc-tip'), /^2025\/10\/20〜10\/26合計0 個/);
    await p.keyboard.press('Escape');
    assert.equal(await p.isHidden('#sc-tip'), true);
    // かざす: 9/1 (火) を含む週 (8/31〜・au PAY 3) の柱
    await p.keyboard.press('Tab');
    const idx = await p.evaluate(() => [...document.querySelectorAll('#sc-rows tr')].reverse().findIndex((tr) => tr.firstElementChild.textContent.startsWith('2026/8/31〜')));
    assert.ok(idx > 0);
    const box = await p.locator(`#sc-sales svg .sc-hit[data-i="${idx}"]`).boundingBox();
    await p.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await p.waitForSelector('#sc-tip:not([hidden])');
    assert.match(await p.textContent('#sc-tip'), /^2026\/8\/31〜9\/6合計3 個その他3/);
  });

  await ta('[4] セット: そのセットとして売れた数・在庫のグラフは出さない', async (p) => {
    await p.goto(B + '/sku/sc-s#tab=sales');
    await ready(p);
    assert.match(await p.textContent('#p-sales .panel-h'), /このセットとして売れた数/);
    assert.match(await p.textContent('#sc-sum'), /直近 52 週の合計 2 個 .*うちセット経由 1 個$/);
    assert.equal(await p.isHidden('#sc-stock-h'), true);
    assert.equal(await p.$('#sc-stock svg'), null);
    assert.equal((await p.$$eval('#sc-legend li', (x) => x.map((li) => li.textContent))).some((x) => /在庫/.test(x)), false);
    assert.deepEqual(await p.$$eval('#sc-thead th', (x) => x.length), 7);
    assert.match(await p.textContent('#sc-notes'), /^(?!.*ロジザード)/);
  });

  await ta('[5] スマホ 390: ページが横にはみ出さない (グラフは箱の幅で描く)', async (p) => {
    await p.goto(B + '/sku/sc-a#tab=sales');
    await ready(p);
    const w = await p.evaluate(() => ({ doc: document.documentElement.scrollWidth, view: window.innerWidth, svg: document.querySelector('#sc-sales svg').getBoundingClientRect().width, box: document.getElementById('sc-chart').clientWidth }));
    assert.ok(w.doc <= w.view, `横にはみ出す: ${w.doc} > ${w.view}`);
    assert.ok(w.svg <= w.box + 0.5 && w.svg > 250, JSON.stringify(w));
    await p.click('#sc-unit button[data-v="day"]');
    assert.ok(await p.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
    if (process.env.MASTER_SALES_SHOT_SP) { await p.focus('#sc-chart'); await p.locator('#p-sales').screenshot({ path: process.env.MASTER_SALES_SHOT_SP }); }
  }, { width: 390, height: 844 });

  await ta('[6] 読めない (関数の実行権が無い) = 言葉だけ (グラフは出さない)', async (p) => {
    await pg.query('reset role');
    for (const f of SALES_CHART_FUNCTIONS) await pg.query(`revoke execute on function ${f} from master_edit`);
    await pg.query('set role deploy');
    try {
      await p.goto(B + '/sku/sc-a#tab=sales');
      await p.waitForFunction(() => /0086/.test(document.getElementById('sc-sum').textContent));
      assert.equal(await p.isHidden('#sc-chart'), true);
      assert.equal(await p.isHidden('#sc-table-box'), true);
    } finally {
      await pg.query('reset role');
      for (const f of SALES_CHART_FUNCTIONS) await pg.query(`grant execute on function ${f} to master_edit`);
      await pg.query('set role deploy');
    }
  });
  // 🆕 10/19 中原さん「表示できなかったものはボタンを押したら表示されるように」: ふつうの期限を 0 にする = 画面を開いたときの読みは必ず時間切れ (本物のサーバーの答え)
  const SHORT_MS = SALES_CHART_LIMITS.timeMs;
  await ta('[7] 時間切れ = 言葉と「もう一度読む」→ 長く待って読む (読む間はボタンを止める・2 度押しでも 1 回)・また時間切れ = もう一度出す・読めたらグラフ', async (p) => {
    SALES_CHART_LIMITS.timeMs = 0;
    try {
      const longCalls = [];
      p.on('request', (q) => { if (/\/sales-chart\?wait=long$/.test(q.url())) longCalls.push(q.url()); });
      // 1 回目の長い読み = 手前の中継の時間切れ (504・reason の無い JSON) にする (30 秒は待たない)。サーバーの時間切れ (503 timeout) と同じ扱いになる (Codex R1 Medium)
      await p.route('**/sales-chart?wait=long', (route) => route.fulfill({ status: 504, contentType: 'application/json', body: JSON.stringify({ error: 'Gateway Timeout' }) }));
      await p.goto(B + '/sku/sc-a#tab=sales');
      await p.waitForSelector('#sc-retry:not([hidden])');
      assert.match(await p.textContent('#sc-sum'), /時間がかかりました \(よく売れる商品は時間がかかることがあります\)/);
      assert.equal(await p.textContent('#sc-retry-btn'), 'もう一度読む (時間がかかることがあります)');
      assert.deepEqual([await p.isHidden('#sc-chart'), await p.isHidden('#sc-wait'), await p.isEnabled('#sc-retry-btn')], [true, true, true]);
      await p.click('#sc-retry-btn');
      await p.waitForFunction(() => /長く待っても読み切れませんでした/.test(document.getElementById('sc-sum').textContent));
      assert.deepEqual([await p.isVisible('#sc-retry-btn'), await p.isEnabled('#sc-retry-btn'), await p.isHidden('#sc-wait'), await p.isHidden('#sc-chart')], [true, true, true, true], 'また時間切れ = 言葉とボタン');
      // 2 回目 = 本物のサーバー (?wait=long = 30 秒まで) へ。答えを止めておいて、読む間の様子を見る
      await p.unroute('**/sales-chart?wait=long');
      let release; const held = new Promise((ok) => { release = ok; });
      await p.route('**/sales-chart?wait=long', async (route) => { await held; await route.continue(); });
      await p.click('#sc-retry-btn');
      await p.waitForSelector('#sc-wait:not([hidden])');
      assert.equal(await p.isDisabled('#sc-retry-btn'), true, '読む間はボタンを止める');
      assert.match(await p.textContent('#sc-sum'), /^読み込み中…/);
      assert.equal(await p.getAttribute('#p-sales', 'aria-busy'), 'true');
      await p.evaluate(() => { const b = document.getElementById('sc-retry-btn'); b.click(); b.dispatchEvent(new MouseEvent('click', { bubbles: true })); });   // 止めたボタンを押しても読まない
      release();
      await ready(p);
      assert.equal(longCalls.length, 2, `長い読みは押した 2 回だけ: ${longCalls.length}`);
      assert.deepEqual([await p.isHidden('#sc-retry'), await p.getAttribute('#p-sales', 'aria-busy')], [true, 'false'], '読めたらボタンは消える');
      assert.match(await p.textContent('#sc-sum'), /直近 52 週の合計 18 個/);
      assert.equal((await cells(p, '#sc-rows tr')).length, 52);
    } finally { SALES_CHART_LIMITS.timeMs = SHORT_MS; }
  });

  await ta('[8] スマホ 390: 時間切れの言葉とボタンが横にはみ出さない / 時間切れ以外で読めない = ボタンは出さない', async (p) => {
    SALES_CHART_LIMITS.timeMs = 0;
    try {
      await p.goto(B + '/sku/sc-a#tab=sales');
      await p.waitForSelector('#sc-retry:not([hidden])');
      const w = await p.evaluate(() => ({ doc: document.documentElement.scrollWidth, view: window.innerWidth, btn: document.getElementById('sc-retry-btn').getBoundingClientRect().right, box: document.getElementById('p-sales').getBoundingClientRect().right }));
      assert.ok(w.doc <= w.view && w.btn <= w.box + 0.5, JSON.stringify(w));
      if (process.env.MASTER_SALES_SHOT_RETRY) await p.locator('#p-sales').screenshot({ path: process.env.MASTER_SALES_SHOT_RETRY });
    } finally { SALES_CHART_LIMITS.timeMs = SHORT_MS; }
    await p.route('**/sales-chart', (route) => route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ ok: false, reason: 'error', error: '販売数を読めません' }) }));
    await p.reload();   // 同じアドレス (#tab=sales) への goto は # だけの移り = 読み直さない
    await p.waitForFunction(() => document.getElementById('sc-sum').textContent === '販売数を読めません');
    assert.equal(await p.isHidden('#sc-retry'), true);
    // 画面を開いたときにつながらない = ボタンは出さない (時間切れだけ・Codex R1 Medium)
    await p.unroute('**/sales-chart');
    await p.route('**/sales-chart', (route) => route.abort());
    await p.reload();
    await p.waitForFunction(() => /つながりませんでした/.test(document.getElementById('sc-sum').textContent));
    assert.equal(await p.isHidden('#sc-retry'), true);
  }, { width: 390, height: 844 });
} finally {
  await browser.close();
  server.close();
}
console.log(`\n${passed} ok`);
