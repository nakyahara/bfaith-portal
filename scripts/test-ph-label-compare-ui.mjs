/**
 * test-ph-label-compare-ui.mjs — product-hub の「商品マスタ (Company DB) の値」の箱 (Company DB構想 27 ④-2・views/_cdb-label.ejs) を描いたページで動かす
 *   (Playwright・本物の router・product-hub の SQLite)。Company DB の読み手は差し替え (本物の SQL は scripts/test-ph-label-compare.mjs が PGlite で確かめる)。
 * 確かめること:
 *   1 単品: 開いた後にページ表記のタブに箱 (原産国 = 違う・内容量 = 一致・型番 = product-hub は空・消費期限 = 参考)・画面のエラーなし
 *   2 「マスタの値を入れる」(原産国) = ページ表記の 2 つの欄に 1 組で入る → 今の自動保存で product-hub に保存される (押した人の操作)・比べ直して一致になる
 *   3 「マスタの値を入れる」(型番) = 楽天のタブの欄に入るだけ (保存はそのタブのボタン = まだ保存しない)
 *   4 読めない = 「読めない」だけ (ボタンなし)・0090 の前 = 箱を出さない
 *   5 390 幅: 箱がページを横にはみ出させない
 * 使い方: node scripts/test-ph-label-compare-ui.mjs   (Playwright / Chromium が無い = 失敗。飛ばすのは PH_UI_SKIP=1)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

if (process.env.PH_UI_SKIP === '1') { console.log('⏭️ PH_UI_SKIP=1 = 表示の項目の箱の画面の試験を飛ばす'); process.exit(0); }
let browser;
try {
  const { chromium } = await import('playwright');
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = PH_UI_SKIP=1`);
  process.exit(1);
}
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phlbl-data-ui-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;
delete process.env.COMPANY_DB_URL;
delete process.env.COMPANY_DB_WATCH_URL;
const LG = await import('../lib/master-legacy-gate.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const L = await import('../apps/product-hub/services/cdb-label-compare.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

// Company DB (差し替え): 商品コード → ビューの行
const CDB = new Map([['a001', { country_of_origin: '中国', country_source: 'human', net_content: '500.00', net_content_uom: 'ml', net_source: 'human', model_number: 'AB-1', model_source: 'human', shelf_life_days: 365, shelf_source: 'human' }]]);
let mode = 'ok';
L.__setCdbLabelReader(async (codes) => {
  if (mode === 'down') return { ok: false, reason: 'テストで読めない' };
  if (mode === 'before') return { ok: true, ready: false, rows: [] };
  return { ok: true, ready: true, rows: codes.map((c, i) => { const v = CDB.get(c.toLowerCase()); return { ord: i + 1, sku_id: v ? '1' : null, sku_kind: v ? 'single' : null, product_id: v ? '10' : null, ...(v || {}) }; }) };
});

const ph = PHDB.getDB();
ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (920001, 'a001', 'x', '単品', '取扱中', 'ok', NULL, '2026-10-13T00:00:00Z')`).run();
ph.prepare('INSERT INTO product_drafts (ne_code, name) VALUES (?, ?)').run('a001', '下書き a001');
const dA1 = ph.prepare("SELECT * FROM product_drafts WHERE ne_code = 'a001'").get();
ph.prepare("INSERT INTO draft_page_info (draft_id, origin_type, origin_country, content_volume) VALUES (?, '日本製', '', '0.5L')").run(dA1.id);
const pageInfo = () => ph.prepare('SELECT origin_type, origin_country, content_volume FROM draft_page_info WHERE draft_id = ?').get(dA1.id);
const article = () => (ph.prepare('SELECT article_number FROM draft_rakuten WHERE draft_id = ?').get(dA1.id) || {}).article_number ?? null;

LG.__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open', changed_at: '2030-01-01', changed_by: 'x' }));
const app = express();
app.set('view engine', 'ejs');
app.use(express.json());
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['product-hub'] }; next(); });
const PHR = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', PHR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const ctx = await browser.newContext();
ctx.setDefaultTimeout(15000);
await ctx.route(/^https?:\/\/(?!127\.0\.0\.1)/, (route) => route.abort());
async function openPage(url, { width } = {}) {
  const page = await ctx.newPage();
  if (width) await page.setViewportSize({ width, height: 900 });
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e && e.message)));
  await page.goto(ORIGIN + url, { waitUntil: 'domcontentloaded' });
  await page.locator('.tab-btn[data-tab="tab-info"]').click();
  return { page, errors };
}
const itemRow = (page, name) => page.locator(`#cdb-label-box tr[data-label-item="${name}"]`);

try {
  await ta('[1] 単品: ページ表記のタブに箱 (原産国 = 違う・内容量 = 一致・型番 = product-hub は空・消費期限 = 参考)', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dA1.id}`);
    await itemRow(page, '原産国').waitFor({ state: 'visible' });
    assert.equal(await itemRow(page, '原産国').getAttribute('data-state'), 'differs');
    assert.match(await itemRow(page, '原産国').textContent(), /中国 \(人が入れた値\)日本製商品マスタと違う/);
    assert.equal(await itemRow(page, '内容量').getAttribute('data-state'), 'match');
    assert.equal(await itemRow(page, '内容量').locator('button').count(), 0, '一致にボタンが出た');
    assert.equal(await itemRow(page, 'メーカー型番').getAttribute('data-state'), 'ph_empty');
    assert.match(await itemRow(page, '消費期限の目安').textContent(), /365 日/);
    assert.match(await page.locator('#cdb-label-summary').textContent(), /商品マスタと違う/);
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[2] 「マスタの値を入れる」(原産国) = 2 つの欄に 1 組で入る → 自動保存で product-hub に保存 (押した人の操作)・比べ直して一致', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dA1.id}`);
    await itemRow(page, '原産国').locator('button').waitFor({ state: 'visible' });
    assert.deepEqual(pageInfo(), { origin_type: '日本製', origin_country: '', content_volume: '0.5L' }, '押す前に変わった');
    await itemRow(page, '原産国').locator('button').click();
    assert.deepEqual([await page.inputValue('#pi-origin-type'), await page.inputValue('#pi-origin-country')], ['海外製', '中国']);
    assert.match(await page.locator('#cdb-label-note').textContent(), /自動で保存されます/);
    await page.waitForFunction(() => /自動保存しました/.test(document.getElementById('pi-result').textContent), null, { timeout: 8000 });
    assert.deepEqual(pageInfo(), { origin_type: '海外製', origin_country: '中国', content_volume: '0.5L' });
    await page.waitForFunction(() => document.querySelector('#cdb-label-box tr[data-label-item="原産国"]')?.getAttribute('data-state') === 'match', null, { timeout: 8000 });
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[3] 「マスタの値を入れる」(型番) = 楽天のタブの欄に入るだけ (保存はそのタブのボタン)', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dA1.id}`);
    await itemRow(page, 'メーカー型番').locator('button').waitFor({ state: 'visible' });
    await itemRow(page, 'メーカー型番').locator('button').click();
    assert.equal(await page.inputValue('#rk-article'), 'AB-1');
    assert.match(await page.locator('#cdb-label-note').textContent(), /カテゴリ・属性を保存/);
    await page.waitForTimeout(1500);
    assert.equal(article(), null, '型番を押しただけで保存した');
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[4] 読めない = 「読めない」だけ (ボタンなし)・0090 の前 = 箱を出さない', async () => {
    mode = 'down';
    let { page, errors } = await openPage(`/apps/product-hub/detail/${dA1.id}`);
    await page.locator('#cdb-label-summary').getByText('読めない').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#cdb-label-box button').count(), 0);
    assert.deepEqual(errors, []);
    await page.close();
    mode = 'before';
    ({ page, errors } = await openPage(`/apps/product-hub/detail/${dA1.id}`));
    await page.waitForTimeout(800);
    assert.equal(await page.locator('#cdb-label-box').isVisible(), false);
    assert.deepEqual(errors, []);
    await page.close();
    mode = 'ok';
  });
  await ta('[5] 390 幅: 箱がページを横にはみ出させない', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dA1.id}`, { width: 390 });
    await itemRow(page, '原産国').waitFor({ state: 'visible' });
    const m = await page.evaluate(() => { const r = document.getElementById('cdb-label-box').getBoundingClientRect(); return { right: r.right, w: window.innerWidth }; });
    assert.ok(m.right <= m.w + 1, JSON.stringify(m));
    assert.deepEqual(errors, []);
    await page.close();
  });
} finally {
  await browser.close();
  server.close();
}
console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
