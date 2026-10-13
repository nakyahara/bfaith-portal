/**
 * test-ph-jan-compare-ui.mjs — product-hub の JAN の印 (Company DB構想 21 §5 B1) を、描いたページで本当に動かす (Playwright・本物の router・product-hub の SQLite)
 *   Company DB の読み手は差し替え (本物の SQL は scripts/test-ph-jan-compare.mjs が PGlite で確かめる)。
 * 確かめること:
 *   1 詳細 (単品): 開いた後に JAN の欄の下に印 (違う + Company DB の JAN)・基本情報のまとめ・見かたの説明が開く・画面のエラーなし
 *   2 詳細 (まとまり): 子の SKU ごとに印 (違う / 一致 / product-hub は空)・印の無い子は空のまま
 *   3 JAN を直して保存 → 比べ直して印が変わる (待たずに)
 *   4 楽天に登録の確認の文に JAN の一言 (キャンセル = 登録しない)・比べている途中は待たずに「読めない」の一言 (詳細・ボード)
 *   5 ボード: 画面に出ているカードの ID だけ照らす (除外は送らない・数えない)・違うカードにだけ札・上の欄に件数・読めない = 「読めない」だけ (札なし)
 *   6 390 幅: 詳細の印がページを横にはみ出させない
 * 使い方: node scripts/test-ph-jan-compare-ui.mjs   (Playwright / Chromium が無い = 失敗。飛ばすのは PH_UI_SKIP=1)
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

if (process.env.PH_UI_SKIP === '1') { console.log('⏭️ PH_UI_SKIP=1 = JAN の印の画面の試験を飛ばす'); process.exit(0); }
let browser;
try {
  const { chromium } = await import('playwright');
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = PH_UI_SKIP=1`);
  process.exit(1);
}
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'phjan-data-ui-'));
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

// Company DB (差し替え): 商品コード → 有効な JAN
const CDB = new Map([['a001', ['4900000001006']], ['a003', ['4900000001037']], ['g-red', ['4900000001075']], ['g-blue', ['4900000001082']], ['g-wht', ['4900000001099']]]);
let readable = true;
let slowMs = 0;   // Company DB の答えを遅らせる (待たないことの確かめ)
J.__setCdbJanReader(async (items) => {
  if (slowMs) await new Promise((r) => setTimeout(r, slowMs));
  if (!readable) return { ok: false, reason: 'テストで読めない' };
  return { ok: true, rows: items.map((it, i) => {
    const jans = CDB.get(it.code.toLowerCase());
    const holder = it.jan ? [...CDB.entries()].find(([c, js]) => c !== it.code.toLowerCase() && js.includes(it.jan))?.[0] ?? null : null;
    return { ord: i + 1, sku_id: jans ? String(i + 1) : null, sku_kind: jans ? 'single' : null, product_id: jans ? String(100 + i) : null,
      jans: (jans || []).map((v, k) => ({ row: String(k + 1), value: v, norm: v })), jan_norm: it.jan || null, jan_holder: holder };
  }) };
});

const ph = PHDB.getDB();
let mirrorPid = 900000;
const mirror = (code, repCode) => ph.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 代表商品コード, updated_at)
  VALUES (?, ?, 'x', '単品', '取扱中', 'ok', ?, '2026-10-13T00:00:00Z')`).run(++mirrorPid, code, repCode);
for (const c of ['a001', 'a003']) mirror(c, null);
mirror('g-red', 'grpx'); mirror('g-blue', 'grpx'); mirror('g-wht', 'grpx');
const draft = (code, jan) => {
  ph.prepare('INSERT INTO product_drafts (ne_code, name, jan_code) VALUES (?, ?, ?)').run(code, `下書き ${code}`, jan);
  return ph.prepare('SELECT * FROM product_drafts WHERE ne_code = ?').get(code);
};
const dA1 = draft('a001', '4900000001006');
const dA3 = draft('a003', '4900000001006');   // a001 の JAN を入れてしまった = 違う + ほかの商品
const dG = draft('grpx', null);
// 除外のカード (ボードに出ない) = JAN が違っても照らさない・数えない
mirror('a004', null);
const dX = draft('a004', '4900000001006');
ph.prepare(`UPDATE product_drafts SET status = 'excluded' WHERE id = ?`).run(dX.id);
ph.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)').run(dG.id, 'g-red', '4900000001013');   // 違う
ph.prepare('INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, ?, ?)').run(dG.id, 'g-blue', '4900000001082');  // 一致

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
// 外のサイト (画像・フォントなど) には出ない
await ctx.route(/^https?:\/\/(?!127\.0\.0\.1)/, (route) => route.abort());
// B2 の段 3: 保存に使う JAN の入力は隠してある (商品マスタから選ぶ)。止める設定の前は「手で打つ」で出せる
async function revealManual(page, code) {
  const input = page.locator(`.sku-jan-input[data-code="${code}"]`);
  if (await input.isVisible()) return;
  await page.locator(`.sku-grid-catalog-cell[data-code="${code}"] .jan-manual-toggle`).click();
  await input.waitFor({ state: 'visible' });
}
async function openPage(url, { width } = {}) {
  const page = await ctx.newPage();
  if (width) await page.setViewportSize({ width, height: 900 });
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e && e.message)));
  await page.goto(ORIGIN + url, { waitUntil: 'domcontentloaded' });
  return { page, errors };
}

try {
  await ta('[1] 詳細 (単品): 開いた後に JAN の欄の下に印 (違う + Company DB の JAN + ほかの商品)・まとめ・見かたの説明・画面のエラーなし', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dA3.id}`);
    const mark = page.locator('.jan-cdb-mark[data-code="a003"]');
    await mark.locator('.jan-cdb-pill').waitFor({ state: 'attached' });
    assert.equal(await mark.locator('.jan-cdb-pill').textContent(), 'Company DB: Company DB と違う');
    assert.ok(await mark.locator('.jan-cdb-pill').evaluate((el) => el.classList.contains('warn')));
    assert.match(await mark.locator('.jan-cdb-detail').first().textContent(), /Company DB の JAN = 4900000001037。この JAN は Company DB ではほかの商品 \(a001\) の JAN/);
    // 直し方は状態ごと (違う = ここの JAN を直す)
    assert.match(await mark.locator('.jan-cdb-detail').nth(1).textContent(), /^直し方: product-hub の JAN を商品マスタの JAN と同じ値に直す/);
    assert.equal(await page.locator('#jan-cdb-summary .jan-cdb-pill').textContent(), '商品マスタ (Company DB) と比べた JAN: Company DB と違う');
    await page.locator('details.jan-cdb-help summary').click();
    assert.ok(await page.locator('details.jan-cdb-help li').first().isVisible());
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[2] 詳細 (まとまり): 子の SKU ごとに印 (違う / 一致 / product-hub は空)', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dG.id}`);
    await page.locator('.jan-cdb-mark[data-code="g-red"] .jan-cdb-pill').waitFor({ state: 'attached' });
    const pill = (c) => page.locator(`.jan-cdb-mark[data-code="${c}"] .jan-cdb-pill`).textContent();
    assert.equal(await pill('g-red'), 'Company DB: Company DB と違う');
    assert.equal(await pill('g-blue'), 'Company DB: 一致');
    assert.equal(await pill('g-wht'), 'Company DB: product-hub は空');
    assert.equal(await page.locator('#jan-cdb-summary .jan-cdb-pill').textContent(), '商品マスタ (Company DB) と比べた JAN: Company DB と違う (1 SKU)');
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[3] 子の JAN を Company DB と同じに直して保存 → 比べ直して印が「一致」に変わる', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dG.id}`);
    await page.locator('.jan-cdb-mark[data-code="g-red"] .jan-cdb-pill').waitFor({ state: 'attached' });
    await page.locator('.tab-btn[data-tab="tab-category"]').click();   // 表はカテゴリ・属性タブ
    const input = page.locator('.sku-jan-input[data-code="g-red"]');
    await revealManual(page, 'g-red');   // B2 の段 3: JAN は商品マスタから選ぶ形 = 手で打つ欄は「手で打つ」で出す (止める設定の前だけ)
    await input.fill('4900000001075');
    await input.dispatchEvent('change');
    await page.waitForFunction(() => document.querySelector('.jan-cdb-mark[data-code="g-red"] .jan-cdb-pill')?.textContent === 'Company DB: 一致');
    assert.equal(ph.prepare(`SELECT jan_code FROM draft_sku_jans WHERE draft_id = ? AND sku_code = 'g-red'`).get(dG.id).jan_code, '4900000001075');
    assert.deepEqual(errors, []);
    await page.close();
  });
  await ta('[4] 楽天に登録の確認の文に JAN の一言 (キャンセル = 登録しない)', async () => {
    const { page } = await openPage(`/apps/product-hub/detail/${dA3.id}`);
    await page.locator('.jan-cdb-mark[data-code="a003"] .jan-cdb-pill').waitFor({ state: 'attached' });
    const registerCalls = [];
    page.on('request', (r) => { if (/\/rakuten\/register$/.test(r.url())) registerCalls.push(r.url()); });
    let dialogText = null;
    page.once('dialog', async (d) => { dialogText = d.message(); await d.dismiss(); });
    const confirmed = page.waitForEvent('dialog', { timeout: 15000 }).catch(() => null);
    await page.evaluate(() => document.getElementById('rk-register-btn').click());
    await confirmed;
    await page.waitForTimeout(300);
    assert.ok(dialogText, '確認が出ない');
    assert.match(dialogText, /⚠ JAN Company DB と違う: a003 の JAN 4900000001006/);
    assert.match(dialogText, /楽天に「公開」で登録します/);
    assert.deepEqual(registerCalls, [], 'キャンセルしたのに登録を送った');
    await page.close();
  });
  await ta('[3b] 🚨 続けて保存しても照らしは並べない: 走っている間の保存は、終わった後に 1 回だけやり直す (最後の値で印)', async () => {
    const { page, errors } = await openPage(`/apps/product-hub/detail/${dG.id}`);
    await page.locator('.jan-cdb-mark[data-code="g-red"] .jan-cdb-pill').waitFor({ state: 'attached' });
    await page.locator('.tab-btn[data-tab="tab-category"]').click();
    const checks = [];
    page.on('request', (r) => { if (/\/jan-check$/.test(r.url())) checks.push(Date.now()); });
    slowMs = 600;
    try {
      // 青 → 白 → 赤 の順に続けて直す (保存はすぐ・照らしは遅い)
      for (const [code, v] of [['g-blue', '4900000001099'], ['g-wht', '4900000001082'], ['g-red', '4900000001020']]) {
        const input = page.locator(`.sku-jan-input[data-code="${code}"]`);
        if (!(await page.locator(`.sku-grid-catalog-cell[data-code="${code}"] .sku-catalog-with`).isVisible())) await page.locator(`.sku-catalog-cell[data-code="${code}"] .sku-catalog-mode[value="with"], .sku-grid-catalog-cell[data-code="${code}"] .sku-catalog-mode[value="with"]`).first().check();
        await revealManual(page, code);
        await input.fill(v);
        await input.dispatchEvent('change');
        await page.waitForTimeout(60);
      }
      await page.waitForFunction(() => document.querySelector('.jan-cdb-mark[data-code="g-red"] .jan-cdb-pill')?.textContent === 'Company DB: Company DB と違う', null, { timeout: 15000 });
      await page.waitForTimeout(800);
      assert.ok(checks.length >= 1 && checks.length <= 2, `照らした回数 ${checks.length} (保存は 3 回)`);
      assert.equal(await page.locator('.jan-cdb-mark[data-code="g-wht"] .jan-cdb-pill').textContent(), 'Company DB: Company DB と違う');
      assert.equal(await page.locator('.jan-cdb-mark[data-code="g-blue"] .jan-cdb-pill').textContent(), 'Company DB: Company DB と違う');
    } finally { slowMs = 0; }
    assert.deepEqual(errors, []);
    // 元に戻す (後の試験の前提)
    ph.prepare(`UPDATE draft_sku_jans SET jan_code = '4900000001075' WHERE draft_id = ? AND sku_code = 'g-red'`).run(dG.id);
    ph.prepare(`DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code IN ('g-blue', 'g-wht')`).run(dG.id);
    ph.prepare(`INSERT INTO draft_sku_jans (draft_id, sku_code, jan_code) VALUES (?, 'g-blue', '4900000001082')`).run(dG.id);
    await page.close();
  });
  await ta('[4b] 🚨 登録の確認は Company DB を待たない: 比べている途中なら「読めない」の一言ですぐ確認を出す', async () => {
    slowMs = 3000;
    try {
      const { page } = await openPage(`/apps/product-hub/detail/${dA3.id}`);
      await page.locator('#rk-register-btn').waitFor({ state: 'attached' });
      let dialogText = null;
      page.once('dialog', async (d) => { dialogText = d.message(); await d.dismiss(); });
      const t0 = Date.now();
      const shown = page.waitForEvent('dialog', { timeout: 15000 }).catch(() => null);
      await page.evaluate(() => document.getElementById('rk-register-btn').click());
      await shown;
      const took = Date.now() - t0;
      assert.ok(dialogText, '確認が出ない');
      assert.ok(took < 2000, `確認まで ${took} ms (Company DB を待った)`);
      assert.match(dialogText, /⚠ JAN の比べ: 読めない \(商品マスタと比べ終わっていません\)/);
      await page.close();
    } finally { slowMs = 0; }
  });
  await ta('[5] ボード: 違うカードにだけ札・上の欄に件数 / 読めない = 「読めない」だけ (札なし)', async () => {
    const asked = [];
    const { page, errors } = await (async () => {
      const page = await ctx.newPage();
      const errors = [];
      page.on('pageerror', (e) => errors.push(String(e && e.message)));
      page.on('request', (r) => { if (/\/api\/board\/jan-check/.test(r.url())) asked.push(new URL(r.url()).searchParams.get('ids')); });
      await page.goto(ORIGIN + '/apps/product-hub/board', { waitUntil: 'domcontentloaded' });
      return { page, errors };
    })();
    await page.locator('#kb-jan-status:not([hidden])').waitFor();
    // 照らすのは画面に出ているカードの ID だけ (除外のカードは出ていない = 送らない・数えない)
    const onScreen = await page.$$eval('.kb-card[data-draft]', (cs) => [...new Set(cs.map((c) => c.dataset.draft))].sort());
    assert.equal(asked.length, 1);
    assert.deepEqual(asked[0].split(',').sort(), onScreen);
    assert.ok(!onScreen.includes(String(dX.id)), '除外のカードがボードに出ている (試験の前提)');
    assert.equal(await page.locator('#kb-jan-status').textContent(), '⚠ JAN 要確認 1 件');   // a003 (まとまりは [3] で直した・除外の a004 は数えない)
    assert.equal(await page.locator(`.kb-card[data-draft="${dA3.id}"] .kb-tag.jan-check`).count(), 1);
    assert.equal(await page.locator(`.kb-card[data-draft="${dA3.id}"] .kb-tag.jan-check`).textContent(), '⚠ JAN Company DB と違う');
    assert.equal(await page.locator(`.kb-card[data-draft="${dA1.id}"] .kb-tag.jan-check`).count(), 0);
    assert.equal(await page.locator('.kb-tag.jan-check').count(), 1);
    assert.deepEqual(errors, []);
    await page.close();
    // 比べている途中にボードから出品 = 確認の文に「読めない」の一言 (待たない)
    slowMs = 3000;
    try {
      const { page: pl } = await openPage('/apps/product-hub/board');
      await pl.locator(`.kb-card[data-draft="${dA3.id}"]`).first().waitFor({ state: 'attached' });
      assert.match(await pl.evaluate((id) => janNoteOf(id), dA3.id), /JAN の比べ: 読めない/);
      await pl.close();
    } finally { slowMs = 0; }
    readable = false;
    try {
      const { page: p2, errors: e2 } = await openPage('/apps/product-hub/board');
      await p2.locator('#kb-jan-status:not([hidden])').waitFor();
      assert.equal(await p2.locator('#kb-jan-status').textContent(), 'JAN の比べ: 読めない');
      assert.equal(await p2.locator('.kb-tag.jan-check').count(), 0);
      assert.deepEqual(e2, []);
      await p2.close();
      const { page: p3 } = await openPage(`/apps/product-hub/detail/${dA3.id}`);
      await p3.locator('.jan-cdb-mark[data-code="a003"] .jan-cdb-pill').waitFor({ state: 'attached' });
      assert.equal(await p3.locator('.jan-cdb-mark[data-code="a003"] .jan-cdb-pill').textContent(), 'Company DB: 読めない');
      assert.equal(await p3.locator('#jan-cdb-summary .jan-cdb-pill').textContent(), 'JAN の比べ: 読めない');
      await p3.close();
    } finally { readable = true; }
  });
  await ta('[6] 390 幅: 詳細の印がページを横にはみ出させない', async () => {
    const { page } = await openPage(`/apps/product-hub/detail/${dA3.id}`, { width: 390 });
    await page.locator('#jan-cdb-summary .jan-cdb-pill').waitFor({ state: 'attached' });
    const box = await page.locator('#jan-cdb-summary').boundingBox();
    assert.ok(box && box.x + box.width <= 390 + 1, `まとめが画面の外へ ${JSON.stringify(box)}`);
    await page.close();
  });
} finally {
  await browser.close();
  server.close();
  try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows はハンドルが残ることがある */ }
}
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
process.exit(process.exitCode || 0);
