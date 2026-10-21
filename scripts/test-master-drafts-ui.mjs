/**
 * test-master-drafts-ui.mjs — 「下書き」の画面 (views/drafts.ejs) と、セットの登録が product-hub の作るセットの下書きを引き継ぐ流れ (2026-10-21) を、本物の router で描いたページで動かす (Playwright・Chromium)
 *
 * 固定する動き:
 *   ① 左の列の「下書き」→ product-hub のセットの下書き (まだ登録していない) とマスタの入力の下書き・「セットとして登録する」= コード・名前・構成を入れた登録の画面 →
 *      残りを入れて保存 = その下書きを引き継ぐ (新しいカードは作らない・Company DB の SKU に結ぶ)・できた商品の画面の上に「引き継ぎました」
 *   ② 構成の違うセットを同じコードで登録 = 断る (理由の 1 行 + product-hub でカードを開くリンク)・何も保存しない
 *   ③ スマホの幅 (390): 「下書き」の画面が横にはみ出さない (表は囲いの中で横に送る)
 *   ④ 🆕 10/21: product-hub の「セット商品を作る」で作ってコードを入れたセットのカードも出る → 「セットとして登録する」→ 保存で引き継ぐ
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-master-drafts-ui.mjs   (入れてある Chrome で動かす = node --import <use-chrome.mjs> scripts/test-master-drafts-ui.mjs)
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 下書きの画面の試験を飛ばす'); process.exit(0); }
let chromium; let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 下書きの画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'drafts-ui-'));
process.env.DATA_DIR = DATA_DIR;

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const T = await setupVariationDb();
const { pg, q } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
const ph = (await import('../apps/product-hub/db.js')).initProductHubDB();
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/drui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/drui';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const { default: router, __setPgClientFactory, __setShippingRatesProvider, __setPhDraftLookup } = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => new Map([['S01', { method: 'ゆうパケット', cost: 210 }]]));
__setPhDraftLookup(async () => null);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'product-hub'] }; next(); });
app.use('/apps/master-edit', router);
app.get('/', (req, res) => res.send('<p>portal</p>'));
app.get('/apps/product-hub/detail/:id', (req, res) => res.send(`<p>product-hub #${req.params.id}</p>`));
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;

/** product-hub に 単品のカードと、新商品の登録で決めたセットの下書き (作るセット) を置く (本番の取り込みと同じ形の行) */
function plannedCard(parentCode, setCode, qty) {
  let parent = ph.prepare('SELECT id FROM product_drafts WHERE ne_code = ?').get(parentCode);
  if (!parent) parent = { id: Number(ph.prepare(`INSERT INTO product_drafts (ne_code, name, created_by) VALUES (?, ?, 'ui')`).run(parentCode, `カード ${parentCode}`).lastInsertRowid) };
  const id = Number(ph.prepare(`INSERT INTO product_drafts (ne_code, name, status, parent_draft_id, provisional_code, price, created_by) VALUES (?, ?, 'draft', ?, 1, 5960, 'ui')`)
    .run(setCode, `${parentCode} ${qty}個セット`, parent.id).lastInsertRowid);
  ph.prepare('INSERT INTO draft_set_members (set_draft_id, member_ne_code, qty, sort) VALUES (?, ?, ?, 0)').run(id, parentCode, qty);
  const rid = `00000000-0000-4000-8000-${String(id).padStart(12, '0')}`;
  ph.prepare(`INSERT INTO ph_cdb_set_plans (request_id, schema, parent_code, parent_name, plan_hash, request_hash, state, created_by, parent_draft_id) VALUES (?, 'set-plan-v1', ?, 'x', ?, ?, 'applied', 'ui', ?)`)
    .run(rid, parentCode, 'a'.repeat(64), 'b'.repeat(64), parent.id);
  ph.prepare(`INSERT INTO ph_cdb_set_plan_items (request_id, sort, qty, code, code_key, name, set_draft_id, outcome) VALUES (?, 0, ?, ?, ?, 'x', ?, 'created')`).run(rid, qty, setCode, setCode.toLowerCase(), id);
  return id;
}
const takeId = plannedCard('s001', 'ui-take-2', 2);
const ntId = plannedCard('s002', 'ui-nt-2', 2);
// 🆕 10/21: product-hub の「セット商品を作る」で作って手でコードを入れたセットのカード (作るセットの行は無い)
const manId = (() => {
  const parent = ph.prepare('SELECT id FROM product_drafts WHERE ne_code = ?').get('s002');
  const id = Number(ph.prepare(`INSERT INTO product_drafts (ne_code, name, status, parent_draft_id, provisional_code, created_by) VALUES ('ui-man-3', 's002 3個セット (手で作った)', 'draft', ?, 1, 'ui')`).run(parent.id).lastInsertRowid);
  ph.prepare('INSERT INTO draft_set_members (set_draft_id, member_ne_code, qty, sort) VALUES (?, ?, 3, 0)').run(id, 's002');
  return id;
})();
const cardsOf = (code) => ph.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').all(code);
const skuOf = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0]?.id;

let passed = 0;
async function ta(name, fn, viewport = { width: 1440, height: 1000 }) {
  const ctx = await browser.newContext({ viewport, locale: 'ja-JP' });
  const p = await ctx.newPage();
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  p.on('console', (m) => { if (m.type() === 'error' && !/favicon|Failed to load resource/.test(m.text())) errors.push(`console: ${m.text()}`); });
  try {
    await fn(p, ctx);
    assert.deepEqual(errors, [], '画面の JS の例外');
    passed++; console.log(`  ok  ${name}`);
  } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
  await ctx.close();
}
const noneRest = (p) => p.evaluate(() => {
  // 🆕 2026-10-21: セットの発送方法は必須 (入力なしにできない) = 空なら送料の表の先頭を選ぶ
  const s = document.getElementById('shipping');
  if (s && !s.value && s.options.length > 1) { s.value = s.options[1].value; s.dispatchEvent(new Event('input', { bubbles: true })); s.dispatchEvent(new Event('change', { bubbles: true })); }
  document.querySelectorAll('input[type="checkbox"][data-none]').forEach((c) => { if (!c.disabled && !c.checked && !window.MasterEdit.none.has(c.getAttribute('data-none'))) c.click(); });
});
console.log('下書きの画面・作るセットの下書きを引き継ぐ (画面)');

await ta('① 左の列の「下書き」→ セットとして登録する → コード・名前・構成が入った登録の画面 → 保存 = その下書きを引き継ぐ (新しいカードは作らない)', async (p) => {
  await p.goto(B + '/');
  await p.click('nav.rail a[aria-label="下書き"]');
  await p.waitForURL(/\/drafts$/);
  const row = p.locator(`#drafts-ph-tbl tr[data-ph-draft="${takeId}"]`);
  assert.equal(await row.count(), 1, 'まだ登録していないセットの下書きが出ない');
  assert.match(await row.textContent(), /ui-take-2[\s\S]*s001 2個セット[\s\S]*s001 × 2[\s\S]*まだ登録していません/);
  assert.equal(await row.locator('a', { hasText: 'product-hub で開く' }).getAttribute('href'), `/apps/product-hub/detail/${takeId}`);
  await Promise.all([p.waitForURL(/\/new\?kind=set&from_card=/), row.locator('a', { hasText: 'セットとして登録する' }).click()]);
  assert.match(await p.textContent('#prefill-note'), new RegExp(`セットの下書き #${takeId} \\(ui-take-2\\) を、このセットのカードとして引き継いで登録します`));
  assert.equal(await p.inputValue('#code'), 'ui-take-2');
  assert.equal(await p.inputValue('#f-name'), 's001 2個セット');
  const r1 = p.locator('#comp-rows tr.comp-row').nth(0);
  assert.deepEqual([await r1.locator('.c-code').inputValue(), await r1.locator('.c-qty').inputValue()], ['s001', '2']);
  await p.waitForFunction(() => /単品 1/.test(document.querySelector('#comp-rows .c-name').textContent), null, { timeout: 10000 });
  await p.waitForSelector('#code-msg.ok');
  assert.equal(await p.isHidden('#comp-suggest'), true, '🆕 10/21: 下書きを入れた後に構成品の候補の箱 (探しています…) を残さない');
  assert.deepEqual(await p.evaluate(() => [...document.querySelectorAll('#jump a[data-sec]')].slice(0, 2).map((a) => a.getAttribute('data-sec'))), ['sec-comp', 'sec-code'], '🆕 10/21: セットは 構成 → コードと名前');
  // 2 行目 (空の行) は外す
  const rows = await p.locator('#comp-rows tr.comp-row').count();
  if (rows > 1) await p.locator('#comp-rows tr.comp-row').nth(1).locator('button[data-act="del"]').click();
  await p.fill('#f-standard_price', '5800');
  await p.fill('#f-reorder_months', '1');
  await noneRest(p);
  assert.equal(await p.isDisabled('#save'), false, await p.textContent('#save'));
  await Promise.all([p.waitForURL(/\/sku\/ui-take-2$/), p.click('#save')]);
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), new RegExp(`product-hub のセットの下書き #${takeId} を、このセットの出品カードとして引き継ぎました`));
  assert.match(await p.textContent('#saved-note'), /売価 \(カード 5960 \/ 入れた値 5800\)/);
  const cards = cardsOf('ui-take-2');
  assert.equal(cards.length, 1, '新しいカードを作らない');
  assert.deepEqual([cards[0].id, String(cards[0].cdb_sku_id), cards[0].provisional_code], [takeId, await skuOf('ui-take-2'), 1]);
  // 「下書き」の画面: product-hub の節から消えて、マスタの入力の下書きに出る
  await p.goto(B + '/drafts');
  assert.equal(await p.locator(`tr[data-ph-draft="${takeId}"]`).count(), 0);
  assert.equal(await p.locator('#drafts-cdb-tbl tr[data-cdb-draft="ui-take-2"]').count(), 1);
});

await ta('② 構成が違う = 断る (理由の 1 行・product-hub でカードを開くリンク)・何も保存しない', async (p) => {
  await p.goto(B + `/new?kind=set&from_card=${ntId}`);
  await p.waitForFunction(() => /単品 2/.test(document.querySelector('#comp-rows .c-name').textContent), null, { timeout: 10000 });
  await p.locator('#comp-rows tr.comp-row').nth(0).locator('.c-qty').fill('3');
  const rows = await p.locator('#comp-rows tr.comp-row').count();
  if (rows > 1) await p.locator('#comp-rows tr.comp-row').nth(1).locator('button[data-act="del"]').click();
  await p.fill('#f-standard_price', '8000');
  await p.fill('#f-reorder_months', '1');
  await noneRest(p);
  await p.waitForSelector('#code-msg.ok');
  await p.click('#save');
  await p.waitForSelector('.result.err');
  assert.match(await p.textContent('.result.err'), /カードの構成 \(s002×2\) と入れた構成 \(s002×3\) がちがいます\)。構成をカードと同じにするか、別のコードにしてください。何も保存していません/);
  assert.equal(await p.getAttribute('#held-card', 'href'), `/apps/product-hub/detail/${ntId}`);
  assert.match(await p.textContent('#held-card'), new RegExp(`product-hub でカード #${ntId} を開く`));
  assert.equal(await skuOf('ui-nt-2'), undefined);
  assert.equal(cardsOf('ui-nt-2')[0].cdb_sku_id, null);
});

await ta('③ スマホの幅 (390): 「下書き」の画面が横にはみ出さない (表は囲いの中で横に送る)・手で作ったセットのカードも出る', async (p) => {
  await p.goto(B + '/drafts');
  const m = await p.evaluate(() => ({ sw: document.documentElement.scrollWidth, w: window.innerWidth,
    wide: [...document.querySelectorAll('body *')].filter((x) => x.getBoundingClientRect().right > window.innerWidth + 1 && !x.closest('.tblwrap, .scrollx')).slice(0, 6).map((x) => x.tagName + '.' + x.className + ' ' + x.textContent.slice(0, 30)) }));
  assert.ok(m.sw <= m.w + 1, `横にはみ出す (${m.sw} > ${m.w}) ${JSON.stringify(m.wide)}`);
  assert.equal(await p.locator(`tr[data-ph-draft="${ntId}"]`).count(), 1);
  // 手で作ったセットのカードも出る (どこで作ったかの一言)
  assert.match(await p.locator(`tr[data-ph-draft="${manId}"]`).textContent(), /ui-man-3[\s\S]*s002 × 3[\s\S]*product-hub で作った/);
}, { width: 390, height: 844 });

await ta('④ 手で作ったセットのカード (product-hub の「セット商品を作る」・コードを入れた) も「セットとして登録する」→ 保存で引き継ぐ', async (p) => {
  await p.goto(B + '/drafts');
  const man = p.locator(`tr[data-ph-draft="${manId}"]`);
  assert.match(await man.textContent(), /product-hub で作った/);
  await Promise.all([p.waitForURL(/from_card=/), man.locator('a', { hasText: 'セットとして登録する' }).click()]);
  assert.equal(await p.inputValue('#code'), 'ui-man-3');
  await p.waitForFunction(() => /単品 2/.test(document.querySelector('#comp-rows .c-name').textContent), null, { timeout: 10000 });
  const rows = await p.locator('#comp-rows tr.comp-row').count();
  if (rows > 1) await p.locator('#comp-rows tr.comp-row').nth(1).locator('button[data-act="del"]').click();
  await p.fill('#f-standard_price', '8000');
  await p.fill('#f-reorder_months', '1');
  await noneRest(p);
  await p.waitForSelector('#code-msg.ok');
  await p.click('#save');
  await p.waitForURL(/\/sku\/ui-man-3$/, { timeout: 15000 }).catch(async () => { throw new Error(`保存できない: ${await p.textContent('#result')} / ${await p.textContent('#msg')} / ${await p.textContent('#save')} / ${await p.isDisabled('#save')}`); });
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), new RegExp(`セットの下書き #${manId} を、このセットの出品カードとして引き継ぎました`));
  assert.deepEqual([cardsOf('ui-man-3').length, String(cardsOf('ui-man-3')[0].cdb_sku_id)], [1, await skuOf('ui-man-3')]);
});

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
await browser.close();
server.close();
await pg.close();
try { fs.rmSync(DATA_DIR, { recursive: true, force: true }); } catch { /* Windows で開いたままのことがある */ }
process.exit(process.exitCode || 0);
