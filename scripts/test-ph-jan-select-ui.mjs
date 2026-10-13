/**
 * test-ph-jan-select-ui.mjs — 商品マスタ (Company DB) の JAN から選ぶ画面 (Company DB構想 21 §5 B2 の段 3) を、描いたページで本当に動かす
 *   (Playwright・本物の router・product-hub の SQLite)。Company DB の読み手は差し替え (本物の SQL は scripts/test-ph-jan-enforce.mjs が PGlite で確かめる)。
 * 確かめること:
 *   1 単品: 開いた後に選ぶ欄に商品マスタの JAN が並ぶ・「楽天のカタログID」の印 (結ばれていない・今は止めません)・選ぶ → 保存 (/jan-select) →
 *     印が「選んである」に・保存に使う欄 (#f-jan) と「保存済み」がそろう (後の保存で JAN を手の道で送り直さない)・カタログIDなしの理由も外れる
 *   2 まとまり: SKU ごとに選ぶ・SKU のなしの理由が外れる・手の道 (/sku-jans) で送り直さない
 *   3 商品マスタに JAN が無い = 選ぶ欄は使えず「足す」の案内 (master-edit の商品の画面へのリンク)・読めない = 「読めない」だけ
 *   4 止める設定 (PH_JAN_ENFORCE=1) = 「手で打つ」が無い・印は「出品は止まります」・説明も「今は入っています」/ 既定 = 「手で打つ」で今までどおり打てる
 *   5 390 幅: はみ出さない・画面のエラーなし (画像を scratchpad に残す: PHJAN3_SHOT_DIR)
 * 使い方: node scripts/test-ph-jan-select-ui.mjs   (Playwright / Chromium が無い = 失敗。飛ばすのは PH_UI_SKIP=1)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

if (process.env.PH_UI_SKIP === '1') { console.log('⏭️ PH_UI_SKIP=1 = JAN を選ぶ画面の試験を飛ばす'); process.exit(0); }
let browser;
try {
  const { chromium } = await import('playwright');
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = PH_UI_SKIP=1`);
  process.exit(1);
}
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phjan3-data-ui-'));
process.env.DATA_DIR = DATA_DIR;
delete process.env.MASTER_EDIT_OPEN;
delete process.env.COMPANY_DB_URL;
delete process.env.COMPANY_DB_WATCH_URL;
delete process.env.PH_JAN_ENFORCE;
const SHOT_DIR = process.env.PHJAN3_SHOT_DIR || null;
const LG = await import('../lib/master-legacy-gate.mjs');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const PHDB = await import('../apps/product-hub/db.js');
const J = await import('../apps/product-hub/services/cdb-jan-compare.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

// Company DB (差し替え): 商品コード → 有効な JAN [行番号, 値]
const CDB = new Map([
  ['b001', [[11, '4900000001006'], [12, '4900000001013']]],
  ['b002', []],
  ['k-red', [[21, '4900000001075']]], ['k-blu', [[22, '4900000001082']]], ['k-wht', [[23, '4900000001099']]],
]);
let readable = true;
J.__setCdbJanReader(async (items) => {
  if (!readable) return { ok: false, reason: 'テストで読めない' };
  return { ok: true, rows: items.map((it, i) => {
    const jans = CDB.get(it.code.toLowerCase());
    return { ord: i + 1, sku_id: jans ? String(i + 1) : null, sku_kind: jans ? 'single' : null, product_id: jans ? String(100 + i) : null,
      jans: (jans || []).map(([row, v]) => ({ row: String(row), value: v, norm: v })), jan_norm: it.jan || null, jan_holder: null };
  }) };
});

const ph = PHDB.getDB();
let mirrorPid = 920000;
const mirror = (code, repCode) => ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', ?, '2026-10-13T00:00:00Z')`).run(++mirrorPid, code, repCode);
for (const c of ['b001', 'b002']) mirror(c, null);
mirror('k-red', 'grpk'); mirror('k-blu', 'grpk'); mirror('k-wht', 'grpk');
const draft = (code, jan) => {
  ph.prepare('INSERT INTO product_drafts (ne_code, name, jan_code) VALUES (?, ?, ?)').run(code, `下書き ${code}`, jan);
  return ph.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get(code);
};
const dB1 = draft('b001', '4900000001013');   // 手で打った (商品マスタと同じ値でも結ばれていない)
const dB2 = draft('b002', null);              // 商品マスタに JAN が無い
const dK = draft('grpk', null);
ph.prepare(`INSERT INTO draft_rakuten (draft_id, catalog_id_exemption_reason) VALUES (?, 5)`).run(dB1.id);   // 両方 (JAN + 理由)
ph.prepare('INSERT INTO draft_sku_catalog_exemptions (draft_id, sku_code, reason) VALUES (?, ?, 3)').run(dK.id, 'k-red');

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
async function openDetail(id, { width } = {}) {
  const page = await ctx.newPage();
  if (width) await page.setViewportSize({ width, height: 900 });
  const errors = [];
  const posts = [];
  page.on('pageerror', (e) => errors.push(String(e && e.message)));
  page.on('request', (r) => { if (r.method() === 'POST') posts.push({ url: r.url().replace(ORIGIN, ''), body: r.postData() }); });
  await page.goto(`${ORIGIN}/apps/product-hub/detail/${id}`, { waitUntil: 'domcontentloaded' });
  await page.locator('.tab-btn[data-tab="tab-category"]').click();
  return { page, errors, posts };
}
// 画像 (人が見た目を確かめる用): 単品 = カタログID の行・まとまり = SKU の表
const shot = async (page, name) => {
  if (!SHOT_DIR) return;
  fs.mkdirSync(SHOT_DIR, { recursive: true });
  const el = (await page.locator('#rk-sku-grid').count()) ? page.locator('#rk-sku-grid') : page.locator('#rk-attrs tbody tr').first();
  await el.scrollIntoViewIfNeeded();
  await el.screenshot({ path: path.join(SHOT_DIR, name) });
};

try {
  await ta('[1] 単品: 商品マスタの JAN が並ぶ・結ばれていない (今は止めません)・選ぶ → 選んである・#f-jan と保存済みがそろう・理由も外れる・手の道で送り直さない', async () => {
    const { page, errors, posts } = await openDetail(dB1.id);
    const sel = page.locator('.jan-pick-select[data-code="b001"]');
    await page.waitForFunction(() => document.querySelectorAll('.jan-pick-select[data-code="b001"] option').length === 3);
    assert.deepEqual(await sel.locator('option').allTextContents(), ['— 商品マスタの JAN から選ぶ —', '4900000001006', '4900000001013']);
    assert.equal(await sel.isEnabled(), true);
    const state = page.locator('.jan-pick-state[data-code="b001"]');
    assert.match(await state.locator('.jan-cdb-pill').textContent(), /^楽天のカタログID: JAN と「なしの理由」の両方 — 今は止めません$/);
    assert.match(await state.textContent(), /止める設定 \(PH_JAN_ENFORCE\) を入れると/);
    assert.equal(await page.locator('#f-jan').isVisible(), false, '手で打つ欄が最初から出ている');
    assert.equal(await page.locator('#rk-catalog-with .jan-pick-links a').getAttribute('href'), '/apps/master-edit/sku/b001');
    await shot(page, 'phjan3-single-before.png');
    await sel.selectOption('12');
    await page.waitForFunction(() => /選んである/.test(document.querySelector('.jan-pick-state[data-code="b001"] .jan-cdb-pill')?.textContent || ''));
    assert.match(await state.locator('.jan-cdb-pill').textContent(), /^楽天のカタログID: 商品マスタの JAN を選んである$/);
    assert.ok(await state.locator('.jan-cdb-pill').evaluate((el) => el.classList.contains('ok')));
    const row = ph.prepare('SELECT jan_code, jan_cdb_row, jan_link_state FROM product_drafts WHERE id = ?').get(dB1.id);
    assert.deepEqual(row, { jan_code: '4900000001013', jan_cdb_row: 12, jan_link_state: 'linked' });
    assert.equal(ph.prepare('SELECT catalog_id_exemption_reason AS r FROM draft_rakuten WHERE draft_id = ?').get(dB1.id).r, null);
    assert.equal(await page.locator('#f-jan').inputValue(), '4900000001013');
    assert.equal(await page.locator('#rk-catalog-exemption').inputValue(), '');
    assert.match(await sel.locator('option:checked').textContent(), /4900000001013 {2}✓ 選んである/);
    await shot(page, 'phjan3-single-after.png');
    // 後の保存 (カテゴリ・属性を保存) で、JAN を手の道 (POST /api/drafts/:id) で送り直さない・理由も送り直さない
    const before = posts.length;
    await page.locator('#rk-save-btn-cat').click();
    await page.waitForTimeout(800);
    const later = posts.slice(before);
    assert.ok(!later.some((p) => new RegExp(`/api/drafts/${dB1.id}$`).test(p.url) && /jan_code/.test(p.body || '')), JSON.stringify(later));
    const rk = later.find((p) => /\/rakuten$/.test(p.url));
    assert.ok(rk, '保存が送られていない');
    assert.equal(JSON.parse(rk.body).catalog_id_exemption_reason, '');
    assert.equal(ph.prepare('SELECT jan_link_state FROM product_drafts WHERE id = ?').get(dB1.id).jan_link_state, 'linked');
    assert.deepEqual(errors, []);
    await page.close();
  });

  await ta('[2] まとまり: SKU ごとに選ぶ・SKU のなしの理由が外れる・/sku-jans で送り直さない', async () => {
    const { page, errors, posts } = await openDetail(dK.id);
    await page.waitForFunction(() => document.querySelectorAll('.jan-pick-select[data-code="k-red"] option').length === 2);
    // 赤は「IDなし」(理由 3) で描かれている = IDあり に切り替えてから選ぶ
    await page.locator('.sku-grid-catalog-cell[data-code="k-red"] .sku-catalog-mode[value="with"]').check();
    await page.locator('.jan-pick-select[data-code="k-red"]').selectOption('21');
    await page.waitForFunction(() => /選んである/.test(document.querySelector('.jan-pick-state[data-code="k-red"] .jan-cdb-pill')?.textContent || ''));
    assert.deepEqual(ph.prepare('SELECT sku_code, jan_code, jan_cdb_row, jan_link_state FROM draft_sku_jans WHERE draft_id = ?').all(dK.id), [{ sku_code: 'k-red', jan_code: '4900000001075', jan_cdb_row: 21, jan_link_state: 'linked' }]);
    assert.equal(ph.prepare('SELECT COUNT(*) AS n FROM draft_sku_catalog_exemptions WHERE draft_id = ?').get(dK.id).n, 0);
    assert.equal(await page.locator('.sku-jan-input[data-code="k-red"]').inputValue(), '4900000001075');
    assert.equal(await page.locator('.sku-exemption-select[data-code="k-red"]').inputValue(), '');
    // ほかの SKU の印 = どちらも無い (今は止めません)
    assert.match(await page.locator('.jan-pick-state[data-code="k-blu"] .jan-cdb-pill').textContent(), /JAN も「なしの理由」も無い — 今は止めません/);
    await shot(page, 'phjan3-variation.png');
    const before = posts.length;
    await page.locator('#rk-save-btn-cat').click();
    await page.waitForTimeout(800);
    assert.ok(!posts.slice(before).some((p) => /\/sku-jans$|\/sku-catalog-exemptions$/.test(p.url)), JSON.stringify(posts.slice(before)));
    assert.deepEqual(errors, []);
    await page.close();
  });

  await ta('[3] 商品マスタに JAN が無い = 選べず「足す」の案内 / 読めない = 「読めない」だけ', async () => {
    let { page, errors } = await openDetail(dB2.id);
    // IDなし で描かれている = IDあり にすると選ぶ欄
    await page.locator('input[name="rk-catalog-mode"][value="with"]').check();
    await page.waitForFunction(() => /JAN が無い/.test(document.querySelector('.jan-pick-select[data-code="b002"] option')?.textContent || ''));
    assert.equal(await page.locator('.jan-pick-select[data-code="b002"]').isDisabled(), true);
    assert.equal(await page.locator('#rk-catalog-with .jan-pick-links a').getAttribute('href'), '/apps/master-edit/sku/b002');
    assert.match(await page.locator('.jan-pick-state[data-code="b002"] .jan-cdb-pill').textContent(), /JAN も「なしの理由」も無い/);
    assert.deepEqual(errors, []);
    await page.close();
    readable = false;
    try {
      ({ page, errors } = await openDetail(dB1.id));
      await page.waitForFunction(() => /読めない/.test(document.querySelector('.jan-pick-select[data-code="b001"] option')?.textContent || ''));
      assert.equal(await page.locator('.jan-pick-select[data-code="b001"]').isDisabled(), true);
      assert.deepEqual(errors, []);
      await page.close();
    } finally { readable = true; }
  });

  await ta('[4] 止める設定 = 「手で打つ」が無い・「出品は止まります」・説明も / 既定 = 「手で打つ」で今までどおり打てる', async () => {
    let { page, errors } = await openDetail(dK.id);
    await page.locator('.sku-grid-catalog-cell[data-code="k-blu"] .sku-catalog-mode[value="with"]').check();
    await page.locator('.sku-grid-catalog-cell[data-code="k-blu"] .jan-manual-toggle').click();
    const input = page.locator('.sku-jan-input[data-code="k-blu"]');
    assert.equal(await input.isVisible(), true);
    await input.fill('4900000001082');
    await input.dispatchEvent('change');
    await page.waitForFunction(() => /結ばれていない/.test(document.querySelector('.jan-pick-state[data-code="k-blu"] .jan-cdb-pill')?.textContent || ''));
    assert.equal(ph.prepare(`SELECT jan_code FROM draft_sku_jans WHERE draft_id = ? AND sku_code = 'k-blu'`).get(dK.id).jan_code, '4900000001082');
    assert.deepEqual(errors, []);
    await page.close();
    process.env.PH_JAN_ENFORCE = '1';
    try {
      ({ page, errors } = await openDetail(dK.id));
      assert.equal(await page.locator('.jan-manual-toggle').count(), 0);
      await page.waitForFunction(() => /出品は止まります/.test(document.querySelector('.jan-pick-state[data-code="k-blu"] .jan-cdb-pill')?.textContent || ''));
      assert.match(await page.locator('details.jan-cdb-help').textContent(), /今は入っています = 出品は止まります/);
      // 選び直せば出せる印に
      await page.locator('.jan-pick-select[data-code="k-blu"]').selectOption('22');
      await page.waitForFunction(() => /選んである/.test(document.querySelector('.jan-pick-state[data-code="k-blu"] .jan-cdb-pill')?.textContent || ''));
      await shot(page, 'phjan3-enforce.png');
      assert.deepEqual(errors, []);
      await page.close();
    } finally { delete process.env.PH_JAN_ENFORCE; }
  });

  await ta('[5] 390 幅: 選ぶ欄と印がページを横にはみ出させない・画面のエラーなし', async () => {
    const { page, errors } = await openDetail(dK.id, { width: 390 });
    await page.waitForFunction(() => document.querySelectorAll('.jan-pick-select[data-code="k-wht"] option').length >= 2);
    const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    assert.ok(over <= 1, `横にはみ出し ${over}px`);
    await shot(page, 'phjan3-390.png');
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
