/**
 * test-ph-jan-link-ui.mjs — JAN を商品マスタの JAN に結ぶ画面 (Company DB構想 21 §5 B2 の段 2) を描いたページで動かす (Playwright・本物の router・SQLite)
 *   Company DB の読み手は差し替え (本物の SQL と書き方の決まりは scripts/test-ph-jan-link.mjs)。
 * 確かめること:
 *   1 「数える」= 数の札と要確認の一覧 (理由つき)・まだ書かない・「移す」は数えた後だけ押せる
 *   2 「移す」= 確認 (キャンセル = 書かない) → OK で書く → 結果・2 回目は押せない (数え直すまで)
 *   3 数えた後に中身が変わった = 書かずに止まり、今の中身を見せ直す
 *   4 390 幅: ページが横にはみ出さない・画面のエラーなし
 * 使い方: node scripts/test-ph-jan-link-ui.mjs   (Playwright / Chromium が無い = 失敗。飛ばすのは PH_UI_SKIP=1)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

if (process.env.PH_UI_SKIP === '1') { console.log('⏭️ PH_UI_SKIP=1 = JAN を結ぶ画面の試験を飛ばす'); process.exit(0); }
let browser;
try {
  const { chromium } = await import('playwright');
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = PH_UI_SKIP=1`);
  process.exit(1);
}
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phjan-data-linkui-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;
delete process.env.COMPANY_DB_URL;
delete process.env.COMPANY_DB_WATCH_URL;
const LG = await import('../lib/master-legacy-gate.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const J = await import('../apps/product-hub/services/cdb-jan-compare.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

// Company DB (差し替え): 商品コード → 有効な JAN [値, 行番号]
const CDB = new Map([['a001', [['4900000001006', 11]]], ['a003', [['4900000001037', 13]]]]);
J.__setCdbJanReader(async (items) => ({ ok: true, rows: items.map((it, i) => {
  const jans = CDB.get(it.code.toLowerCase());
  return { ord: i + 1, sku_id: jans ? String(i + 1) : null, sku_kind: jans ? 'single' : null, product_id: jans ? String(100 + i) : null,
    jans: (jans || []).map(([v, row]) => ({ row: String(row), value: v, norm: v })), jan_norm: it.jan || null, jan_holder: null };
}) }));

const ph = PHDB.getDB();
let mirrorPid = 900000;
for (const c of ['a001', 'a003']) ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', NULL, '2026-10-13T00:00:00Z')`).run(++mirrorPid, c);
ph.prepare('INSERT INTO product_drafts (ne_code, name, jan_code) VALUES (?, ?, ?)').run('a001', '結べる商品', '4900000001006');
ph.prepare('INSERT INTO product_drafts (ne_code, name, jan_code) VALUES (?, ?, ?)').run('a003', '違う JAN の商品', '4900000001006');
const idOf = (c) => ph.prepare('SELECT id FROM product_drafts WHERE ne_code = ?').get(c).id;
const stateOf = (c) => ph.prepare('SELECT jan_link_state, jan_cdb_row, jan_linked_by FROM product_drafts WHERE ne_code = ?').get(c);

LG.__setLegacyPhaseReader(async () => ({ readable: true, phase: 'legacy_open', changed_at: '2030-01-01', changed_by: 'x' }));
const app = express();
app.set('view engine', 'ejs');
app.use(express.json());
app.use((req, res, next) => { req.session = { authenticated: true, email: 'boss@test', displayName: '管理者', role: 'admin', allowedApps: ['product-hub'] }; next(); });
const PHR = await import('../apps/product-hub/router.js');
app.use('/apps/product-hub', PHR.default);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const ORIGIN = `http://127.0.0.1:${server.address().port}`;
const ctx = await browser.newContext();
ctx.setDefaultTimeout(15000);
await ctx.route(/^https?:\/\/(?!127\.0\.0\.1)/, (route) => route.abort());
async function openPage(width) {
  const page = await ctx.newPage();
  if (width) await page.setViewportSize({ width, height: 900 });
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e && e.message)));
  await page.goto(ORIGIN + '/apps/product-hub/jan-link', { waitUntil: 'domcontentloaded' });
  return { page, errors };
}
const tileNums = (page) => page.$$eval('#jl-tiles .jl-tile .n', (ns) => ns.map((n) => Number(n.textContent)));

try {
  await ta('[1] 数える = 札 (結べる 1・要確認 1) と要確認の一覧 (理由つき)・まだ書かない・移すは数えた後だけ', async () => {
    const { page, errors } = await openPage();
    assert.equal(await page.locator('#jl-apply').isDisabled(), true);
    await page.locator('#jl-preview').click();
    await page.locator('#jl-tiles:not([hidden])').waitFor();
    assert.deepEqual((await tileNums(page)).slice(0, 3), [1, 1, 0]);
    assert.match(await page.locator('details.jl-list').first().textContent(), /違う JAN の商品[\s\S]*商品マスタにはこの商品の別の JAN がある[\s\S]*product-hub の JAN を商品マスタの JAN と同じ値に直す/);
    assert.deepEqual(await page.locator('details.jl-list').first().locator('th').allTextContents(), ['商品', '商品コード', 'product-hub の JAN', '理由', '直し方']);
    assert.equal(await page.locator('#jl-apply').isDisabled(), false);
    assert.equal(await page.locator('#jl-apply').textContent(), '③ 移す (2 件を書く)');
    assert.equal(stateOf('a001').jan_link_state, null, '数えただけで書いた');
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[2] 移す: 確認でキャンセル = 書かない → OK = 書く・結果・2 回目は押せない', async () => {
    const { page, errors } = await openPage();
    await page.locator('#jl-preview').click();
    await page.locator('#jl-tiles:not([hidden])').waitFor();
    page.once('dialog', (d) => d.dismiss());
    await page.locator('#jl-apply').click();
    await page.waitForTimeout(300);
    assert.equal(stateOf('a001').jan_link_state, null, 'キャンセルしたのに書いた');
    let msg = '';
    page.once('dialog', (d) => { msg = d.message(); d.accept(); });
    await page.locator('#jl-apply').click();
    await page.locator('#jl-result.ok').waitFor();
    assert.match(msg, /結ぶ: 1 件[\s\S]*要確認にする: 1 件/);
    assert.match(await page.locator('#jl-result').textContent(), /結んだ 1 件・要確認 1 件/);
    assert.deepEqual(stateOf('a001'), { jan_link_state: 'linked', jan_cdb_row: 11, jan_linked_by: 'boss@test' });
    assert.equal(stateOf('a003').jan_link_state, 'needs_review');
    assert.equal(await page.locator('#jl-apply').isDisabled(), true, '書いた後も移すが押せる');
    // 数え直すと「書く物はありません」
    await page.locator('#jl-preview').click();
    await page.waitForFunction(() => document.getElementById('jl-apply').textContent.includes('書く物はありません'));
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[3] 数えた後に中身が変わった = 書かずに止まり、今の中身を見せ直す', async () => {
    ph.prepare('UPDATE product_drafts SET jan_code = ? WHERE id = ?').run('4900000001037', idOf('a003'));   // 直した = 結べるに
    const { page } = await openPage();
    await page.locator('#jl-preview').click();
    await page.waitForFunction(() => document.getElementById('jl-apply').textContent.includes('1 件を書く'));
    ph.prepare('UPDATE product_drafts SET jan_code = NULL WHERE id = ?').run(idOf('a001'));   // 別の人が空にした
    page.once('dialog', (d) => d.accept());
    await page.locator('#jl-apply').click();
    await page.locator('#jl-result.bad').waitFor();
    assert.match(await page.locator('#jl-result').textContent(), /数えた後に内容が変わりました/);
    assert.equal(stateOf('a003').jan_link_state, 'needs_review', '止まらずに書いた');
    assert.deepEqual((await tileNums(page)).slice(0, 3), [1, 0, 1]);   // 今の中身 (結べる 1・外す 1)
    await page.close();
  });
  await ta('[4] 390 幅: ページが横にはみ出さない・画面のエラーなし', async () => {
    const { page, errors } = await openPage(390);
    await page.locator('#jl-preview').click();
    await page.locator('#jl-tiles:not([hidden])').waitFor();
    const sw = await page.evaluate(() => document.documentElement.scrollWidth);
    assert.ok(sw <= 391, `横幅 ${sw}`);
    assert.deepEqual(errors, []);
    await page.close();
  });
} finally {
  await browser.close();
  server.close();
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
process.exit(process.exitCode || 0);
