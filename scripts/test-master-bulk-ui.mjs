/**
 * test-master-bulk-ui.mjs — 一覧で選んで、まとめて変える (public/me-bulk.js) を、描いたページで本当に動かす (Playwright・本物の router・PGlite)
 * 確かめること:
 *   1 選ぶ: 行のチェック・Shift で間・見出しのチェック = このページ・帯の件数 (表示中 / ほかのページ)・読み直しても残る・選ぶのをやめる
 *   2 絞り込みを変えた = 前の条件で選んだ分を「残す / 外す」で聞く・帯にいつも「別の条件で選んだ」(Codex High 2)・選んだものだけ表示
 *   3 200 件の上限 = 選ぶ時点で数える (帯の文・「まとめて変える」を押せない・検索結果すべては押せない)(Codex M6)
 *   4 原価: 理由は初期値なし = 選ぶまで次へ進めない (M4)・前と後は見出しへ (保存のボタンへ移らない)・確かめのチェックを入れるまで保存できない・
 *     Enter を続けて押しても保存しない (High 1)・合計 = 選んだ + 一緒に変わるセット (M2)・保存 → 結果 → 閉じると一覧を読み直して変えた行が光る
 *   5 だめな分は直し方ごと (M7)・選び直せる分だけ選び直す (1 件の画面で直す分は入れない)
 *   6 390 幅: 帯と引き出しが画面に収まる・横にスワイプの案内 (M8)・ページが横にはみ出さない
 * 使い方: node scripts/test-master-bulk-ui.mjs   (Playwright / Chromium が無い = 失敗。飛ばすのは MASTER_EDIT_UI_SKIP=1)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = まとめて変えるの画面の試験を飛ばす'); process.exit(0); }
let browser;
try {
  const { chromium } = await import('playwright');
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const OG = await import('../lib/master-owner-gate.mjs');
const { OWNED_COLUMNS } = await import('../config/master-ownership.mjs');
OG.__setCapableForTest(OWNED_COLUMNS);
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
const { forceNewOpen } = await import('./fixtures/master-widen.mjs');
const W = await import('../lib/master-write.mjs');
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'mlist2-bulk-ui-'));
const { default: router, __setPgClientFactory, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');

const quiet = () => {};
const ALL = Object.fromEntries(OWNED_COLUMNS.map((k) => [k, 'company']));
const RATES = new Map([['S02', { method: '宅急便', cost: 520 }]]);
const pg = new PGlite();
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const sku = (code, name, kind, taxRate, cost, extra = {}) => ({
  code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : 'STANDARD_10', handling: 'active', salesClass: 3,
  cost: { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2, ...extra,
});
const singles = [
  sku('a001', 'エプロン ピンク', 'single', 0.1, 100), sku('a002', 'エプロン ブルー', 'single', 0.1, 200), sku('a003', 'エプロン 黄', 'single', 0.1, 300),
  sku('a004', 'エプロン 緑', 'single', 0.1, 400), sku('b001', 'エプロン 先の日の原価', 'single', 0.1, 700), sku('x001', 'エプロン 例外', 'exception', 0.1, 50),
  ...Array.from({ length: 230 }, (_, i) => sku(`z${String(i).padStart(3, '0')}`, `たくさん ${i}`, 'single', 0.1, 100 + i)),
];
const lr = await runInitialLoad(db, {
  skus: [...singles, sku('s001', 'エプロン セット', 'set', 0.1, 300)], variationGroups: [],
  setComponents: [{ parentCode: 's001', childCode: 'a001', qty: 1, source: 'ne' }, { parentCode: 's001', childCode: 'a002', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: singles.filter((s) => s.kind === 'single').map((s) => ({ supplierCode: '0001', skuCode: s.code })),
  primarySuppliers: singles.filter((s) => s.kind === 'single').map((s) => ({ skuCode: s.code, supplierCode: '0001' })), reorder: { available: true, runId: 'pml_bui' },
}, { log: quiet, runId: 'load_bui', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
await forceNewOpen(db, ALL);
const sid = async (code) => (await pg.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0].id;
{
  const id = await sid('b001');
  const tomorrow = W.jstDate(new Date(Date.now() + 86400e3));
  await pg.query('update core.sku_costs set valid_to = $2::date - 1 where sku_id = $1 and valid_to is null', [id, tomorrow]);
  await pg.query(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from, created_by_type, created_by_id) values (1, $1, 999, 'ne', 'COMPLETE', $2, 'human', 't')`, [id, tomorrow]);
}
async function as(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
const costOf = async (code) => Number((await pg.query(`select c.cost_jpy from core.sku_costs c where c.sku_id = $1 and c.valid_to is null`, [await sid(code)])).rows[0].cost_jpy);
async function changeBehind(code, values) {
  const token = W.editTokenOf(await W.readCurrent(db, await sid(code), W.jstDate(new Date())));
  return as('master_edit', () => W.saveSku(db, { actor: 'other@test', requestId: crypto.randomUUID(), code, reason: '画面の外で', seen: { token }, values }, { open: true, shippingRates: RATES }));
}

process.env.COMPANY_DB_URL = 'postgres://owner@localhost/bui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/bui';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let chain = Promise.resolve();
let beforeApply = null;   // 保存の要求の前に 1 回だけ (確かめと保存の間に、ほかの人が直す)
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit/api/bulk/apply', async (req, res, next) => { if (beforeApply) { const f = beforeApply; beforeApply = null; await f(); } next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const BASE = `http://127.0.0.1:${server.address().port}/apps/master-edit`;

let passed = 0;
async function ta(name, fn, viewport = { width: 1440, height: 900 }) {
  const ctx = await browser.newContext({ viewport, locale: 'ja-JP' });
  const p = await ctx.newPage();
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  try {
    await fn(p, ctx);
    assert.deepEqual(errors, [], '画面の JS の例外');
    passed++; console.log(`  ok  ${name}`);
  } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
  await ctx.close();
}
const n = (p) => p.textContent('#bk-n');
const ck = (p, code) => p.locator(`.rowck[data-code="${code}"]`);
const q = (s) => `${BASE}/?q=${encodeURIComponent(s)}`;

await ta('[1] 選ぶ: 行・Shift で間・見出し = このページ・帯 (表示中)・読み直しても残る・やめる', async (p) => {
  await p.goto(q('エプロン'));
  assert.equal(await p.isHidden('#bk-selbar'), true);
  await ck(p, 'a001').click();
  await ck(p, 'a004').click({ modifiers: ['Shift'] });
  assert.equal(await n(p), '4');
  assert.match(await p.textContent('#bk-where'), /表示中 4/);
  assert.equal(await p.evaluate(() => document.getElementById('ck-page').indeterminate), true);
  await p.click('#ck-page');
  const rows = await p.locator('.rowck').count();
  assert.equal(await n(p), String(rows));
  assert.equal(await p.isHidden('#bk-sel-page'), true);
  await p.reload();
  assert.equal(await n(p), String(rows), 'タブの中で覚える');
  assert.equal(await ck(p, 'a002').isChecked(), true);
  assert.equal(await p.locator('tr.bk-sel').count(), rows);
  await p.click('#bk-clear');
  assert.equal(await p.isHidden('#bk-selbar'), true);
  assert.equal(await ck(p, 'a002').isChecked(), false);
});

await ta('[2] 絞り込みを変えた = 前の条件で選んだ分を「残す / 外す」で聞く・帯にいつも「別の条件で選んだ」・選んだものだけ表示 (High 2)', async (p) => {
  await p.goto(q('エプロン'));
  await ck(p, 'a001').click(); await ck(p, 'a002').click();
  await p.goto(q('たくさん 1'));
  assert.equal(await n(p), '2');
  assert.equal(await p.isVisible('#bk-prev'), true);
  assert.match(await p.textContent('#bk-prev-t'), /前の条件で選んだ 2 件/);
  assert.match(await p.textContent('#bk-where'), /表示中 0 · 別の条件で選んだ 2/);
  await p.click('#bk-prev-keep');
  assert.equal(await p.isHidden('#bk-prev'), true);
  assert.match(await p.textContent('#bk-where'), /別の条件で選んだ 2/, '残した後も帯に出る');
  await ck(p, 'z010').click();
  assert.equal(await n(p), '3');
  // もう一度条件を変えると、また聞く → 外す = 見えない 3 件を外す
  await p.goto(q('エプロン ピンク'));
  assert.equal(await p.isVisible('#bk-prev'), true);
  assert.match(await p.textContent('#bk-where'), /表示中 1 · 別の条件で選んだ 2/);
  await p.click('#bk-prev-drop');
  assert.equal(await n(p), '1');
  // 選んだものだけ表示 (詳細検索の商品コード)
  await p.goto(q('エプロン'));
  await ck(p, 'a003').click();
  await p.goto(q('たくさん 2'));
  await p.click('#bk-prev-keep');
  await Promise.all([p.waitForURL(/[?&]s=/), p.click('#bk-show')]);
  assert.deepEqual((await p.locator('.rowck').evaluateAll((xs) => xs.map((x) => x.dataset.code))).sort(), ['a001', 'a003']);
  assert.equal(await p.isHidden('#bk-prev'), true, '選んだものだけの一覧では聞かない');
  assert.match(await p.textContent('#bk-where'), /表示中 2/);
  await p.click('#bk-clear');
});

await ta('[3] 200 件の上限 = 選ぶ時点で数える (帯の文・まとめて変えるを押せない・検索結果すべては押せない)(M6)', async (p) => {
  await p.goto(`${BASE}/?q=${encodeURIComponent('たくさん')}`);
  await p.click('#ck-page');
  assert.equal(await n(p), '100');
  assert.equal(await p.isVisible('#bk-sel-all'), true);
  assert.equal(await p.isDisabled('#bk-sel-all'), true, '230 件 > 200 = 押せない');
  assert.match(await p.getAttribute('#bk-sel-all', 'title'), /200 件まで/);
  await p.goto(`${BASE}/?q=${encodeURIComponent('たくさん')}&offset=100`);
  assert.match(await p.textContent('#bk-where'), /表示中 0 · ほかのページ 100/);
  await p.click('#ck-page');
  await p.goto(`${BASE}/?q=${encodeURIComponent('たくさん')}&offset=200`);
  await p.click('#ck-page');
  assert.equal(await n(p), '230');
  assert.equal(await p.isVisible('#bk-limit'), true);
  assert.match(await p.textContent('#bk-limit'), /230 件選択中 · 1 回は 200 件まで。30 件減らしてください/);
  assert.equal(await p.isDisabled('#bk-go'), true);
  await p.click('#ck-page');   // このページの 30 件を外す
  assert.equal(await n(p), '200');
  assert.equal(await p.isHidden('#bk-limit'), true); assert.equal(await p.isDisabled('#bk-go'), false);
  await p.click('#bk-clear');
});

await ta('[4] 原価: 理由は選ぶまで進めない (M4)・前と後は見出しへ・確かめのチェックまで保存できない・Enter の連打で保存しない (High 1)・合計 = 選んだ + セット (M2)・結果・読み直し', async (p) => {
  await p.goto(q('エプロン'));
  for (const c of ['a001', 'a002', 'x001', 's001']) await ck(p, c).click();
  await p.click('#bk-go');
  await p.waitForSelector('.bk-tile[data-field="cost"]');
  assert.match(await p.textContent('.bk-tile[data-field="cost"]'), /2 件 変えられる.*対象外 2/);
  assert.match(await p.textContent('.bk-tile[data-field="standard_price"]'), /3 件 変えられる/);
  await p.keyboard.press('1');
  await p.waitForSelector('#bk-yen');
  assert.equal(await p.locator('input[name="bk-rsn"]:checked').count(), 0, '理由は初期値なし');
  await p.fill('#bk-yen', '250');
  assert.equal(await p.isDisabled('#bk-next'), true, '理由を選ぶまで進めない');
  assert.match(await p.textContent('#bk-live'), /2 件.*250 円/);
  await p.click('label.rp:has-text("メーカーからの値上げ通知")');
  assert.equal(await p.isDisabled('#bk-next'), false);
  await p.focus('#bk-yen');
  await p.keyboard.press('Enter');
  await p.waitForSelector('#bk-sum');
  assert.equal(await p.evaluate(() => document.activeElement.id), 'bk-h', '見出しへ (保存のボタンへ移らない)');
  assert.match(await p.textContent('#bk-sum'), /選んだ商品 2 件 \+ 一緒に変わるセット 1 件 = 合計 3 件/);
  assert.equal(await p.isDisabled('#bk-apply'), true, '確かめのチェックまで押せない');
  assert.match(await p.textContent('#bk-apply'), /合計 3 件を変える \(セット 1 を含む\)/);
  await p.keyboard.press('Enter'); await p.keyboard.press('Enter');
  await p.waitForTimeout(300);
  assert.equal(await costOf('a001'), 100, 'Enter の連打で保存しない');
  assert.equal(await p.isVisible('#bk-sum'), true, 'まだ前と後');
  // 一緒に変わるセットの段
  await p.click('[data-tab="linked"]');
  assert.match(await p.textContent('.bk-pv.link'), /s001.*300 円.*500 円.*構成品から計算し直します/s);
  await p.check('#bk-ok');
  await p.click('#bk-apply');
  await p.waitForSelector('#bk-rok');
  assert.equal((await p.textContent('#bk-rok')).replace(/\D/g, ''), '2');
  assert.match(await p.textContent('#bk-body'), /一緒に変わったセット 1 件/);
  // 2 回に分けて変わったセット (a001 の保存 → a002 の保存) は、最初の値 → 最後の値で 1 行
  assert.match(await p.textContent('.bk-pv.link'), /s001.*300 円.*500 円/s);
  assert.equal(await costOf('a001'), 250); assert.equal(await costOf('a002'), 250); assert.equal(await costOf('s001'), 500);
  await Promise.all([p.waitForLoadState('load'), p.click('[data-act="done"]')]);
  await p.waitForSelector('tr.bk-just');
  assert.ok(await p.locator('tr.bk-just .rowck[data-code="a001"]').count(), '変えた行が光る');
});

await ta('[5] だめな分は直し方ごと (M7)・選び直せる分だけ選び直す (1 件の画面で直す分は入れない)', async (p) => {
  await p.goto(q('エプロン'));
  for (const c of ['a003', 'a004', 'b001']) await ck(p, c).click();
  await p.click('#bk-go');
  await p.waitForSelector('.bk-tile[data-field="cost"]');
  assert.match(await p.textContent('.bk-tile[data-field="cost"]'), /2 件 変えられる.*保存できない 1/, '先の日の原価は最初から「保存できない」(M5)');
  await p.click('.bk-tile[data-field="cost"]');
  await p.fill('#bk-yen', '444');
  await p.click('label.rp:has-text("その他")');
  assert.equal(await p.isDisabled('#bk-next'), true, 'その他は書くまで進めない');
  await p.fill('#bk-reason-other', '送料込みに変わった');
  await p.click('#bk-next');
  await p.waitForSelector('#bk-ok');
  beforeApply = () => changeBehind('a004', { standard_price: 1234 });
  await p.check('#bk-ok');
  await p.click('#bk-apply');
  await p.waitForSelector('#bk-rok');
  assert.equal(await p.locator('.bk-grp[data-group="one"] .bk-fail').count(), 1);
  assert.match(await p.textContent('.bk-grp[data-group="one"]'), /1 件の画面で直す.*b001.*先の日の原価/s);
  assert.match(await p.textContent('.bk-grp[data-group="latest"]'), /最新の値を確かめてから選び直す.*a004/s);
  assert.match(await p.textContent('[data-act="reselect"]'), /選び直せる 1 件だけ選び直す/);
  await p.click('[data-act="reselect"]');
  await p.waitForLoadState('load');
  await p.waitForSelector('#bk-selbar:not([hidden])');
  assert.equal(await n(p), '1');
  assert.equal(await ck(p, 'a004').isChecked(), true);
  assert.equal(await ck(p, 'b001').isChecked(), false);
  assert.ok(await p.locator('tr.bk-failed .rowck[data-code="a004"]').count(), '赤い印');
  assert.equal(await costOf('a003'), 444);
});

await ta('[6] 390 幅: 帯と引き出しが画面に収まる・横にスワイプの案内・ページが横にはみ出さない (M8)', async (p) => {
  await p.goto(q('エプロン'));
  assert.equal(await p.isVisible('#bk-swipe'), true);
  // 🚨 390 幅は上の帯 (名前・ダッシュボード) が前から少しはみ出す (この PR の前から・まとめて変えるの無い人の画面も同じ) = 選んでも広げない を見る
  const sw0 = await p.evaluate(() => document.scrollingElement.scrollWidth);
  await ck(p, 'a001').click();
  const bar = await p.locator('#bk-selbar').boundingBox();
  assert.ok(bar.x >= 0 && bar.x + bar.width <= 390, JSON.stringify(bar));
  assert.equal(await p.evaluate(() => document.scrollingElement.scrollWidth), sw0, '帯でページが横に広がった');
  await p.locator('#list-tbl').evaluate((t) => { t.closest('.tblwrap').scrollLeft = 200; t.closest('.tblwrap').dispatchEvent(new Event('scroll')); });
  assert.equal(await p.isHidden('#bk-swipe'), true, '送ったら案内は消える');
  const code = await p.locator('#list-tbl td[data-col="code"]').first().boundingBox();
  assert.ok(code.x < 100, `コードの列が残る (${code.x})`);
  await p.click('#bk-go');
  await p.waitForSelector('.bk-tile');
  await p.waitForTimeout(400);   // 出てくる動き (0.2 秒) の後に測る
  const dw = await p.locator('#bk-drawer').boundingBox();
  assert.ok(dw.x >= 0 && dw.width <= 390, JSON.stringify(dw));
  await p.keyboard.press('Escape');
  assert.equal(await p.isHidden('#bk-drawer'), true);
  await p.click('#bk-clear');
}, { width: 390, height: 844 });

await browser.close();
server.close();
console.log(`\n${passed} 件 ok`);
