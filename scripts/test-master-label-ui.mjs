/**
 * test-master-label-ui.mjs — 「表示の項目」(views/_label.ejs・_label-new.ejs・public/me-label.js・Company DB の 0090) を本物の router で描いたページで動かす (Playwright・Chromium)
 *
 * 固定する動き:
 *   ① 商品の画面: product-hub の今の値の「この値を入れる」= 欄に入れるだけ (原産国・内容量の数 + 単位・型番)・未保存の「前 → 後」・保存 → Company DB・元に戻す
 *   ② 単品の新商品: 表示の項目を入れて保存 → 送る本文の label・Company DB に付く / 入れないで保存 = 送らない (あと N つに数えない)
 *   ③ まとまり: 共通の欄に表示の項目 (型番は無い)・入れただけで未保存に数える
 *   ④ スマホの幅 (390): 商品の画面の「表示の項目」が横にはみ出さない
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-master-label-ui.mjs   (入れてある Chrome で動かす = node --import <use-chrome.mjs> scripts/test-master-label-ui.mjs)
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 表示の項目の画面の試験を飛ばす'); process.exit(0); }
let chromium; let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 表示の項目の画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lbl-ui-'));

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const T = await setupVariationDb();
const { pg, q } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/lbui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/lbui';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const LB = await import('../lib/master-label.mjs');
const { default: router, __setPgClientFactory, __setShippingRatesProvider, __setPhDraftLookup, __setPhLabelLookup } = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]));
__setPhDraftLookup(async () => null);
const ph = new Database(':memory:');
ph.exec(`CREATE TABLE product_drafts (id INTEGER PRIMARY KEY AUTOINCREMENT, ne_code TEXT NOT NULL UNIQUE, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft');
  CREATE TABLE draft_page_info (draft_id INTEGER PRIMARY KEY, origin_type TEXT, origin_country TEXT, content_volume TEXT);
  CREATE TABLE draft_rakuten (draft_id INTEGER PRIMARY KEY, article_number TEXT);`);
ph.prepare("INSERT INTO product_drafts (id, ne_code, name) VALUES (21, 's002', '単品 2')").run();
ph.prepare("INSERT INTO draft_page_info (draft_id, origin_type, origin_country, content_volume) VALUES (21, '海外製', '中国', '1.5kg')").run();
ph.prepare("INSERT INTO draft_rakuten (draft_id, article_number) VALUES (21, 'UI-MDL')").run();
__setPhLabelLookup(async (code) => LB.readPhLabelRefs(ph, code));
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', router);
app.get('/', (req, res) => res.send('<p>portal</p>'));
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;
const humanOf = async (code) => (await q(`select x.country_of_origin as c, x.net_content::text as n, x.net_content_uom as u, x.model_number as m, x.shelf_life_days as d, x.set_via as via
  from core.skus k join core.product_label_facts x on x.product_id = k.product_id where k.code = $1`, [code]))[0] || null;

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
  for (const id of ['po-cond-mode', 'po-mat-mode']) { const sg = document.getElementById(id); if (sg && !sg.getAttribute('data-value')) sg.querySelector('button[data-v="none"]').click(); }
  document.querySelectorAll('input[type="checkbox"][data-none]').forEach((c) => { if (!c.disabled && !c.checked && !window.MasterEdit.none.has(c.getAttribute('data-none'))) c.click(); });
});
const fillSingle = async (p, code) => {
  await p.fill('#code', code);
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', '表示の項目の試験 ' + code);
  await p.fill('#f-standard_price', '1200');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.click('#f-sales_class button[data-v="3"]');
  await p.selectOption('#f-primary_supplier', '0034');
  await p.fill('#f-reorder_months', '1');
  await p.click('#f-expiry button[data-v="0"]');
  if (await p.locator('#f-lzbc-blank').count()) await p.check('#f-lzbc-blank');
};
console.log('表示の項目 (画面)');

await ta('① 商品の画面: 「この値を入れる」= 欄に入れるだけ・未保存の「前 → 後」・元に戻す・保存 → Company DB', async (p) => {
  await p.goto(B + '/sku/s002');
  assert.deepEqual(await p.evaluate(() => window.MasterEdit.dirty().items), [], '開いただけで未保存');
  await p.click('button[data-lbl-use="country"]');
  await p.click('button[data-lbl-use="net"]');
  await p.click('button[data-lbl-use="model"]');
  assert.deepEqual([await p.inputValue('#f-lbl_country'), await p.inputValue('#f-lbl_net'), await p.inputValue('#f-lbl_net_uom'), await p.inputValue('#f-lbl_model')], ['中国', '1500', 'g', 'UI-MDL'], 'kg → g に直して入れる');
  const items = await p.evaluate(() => window.MasterEdit.dirty().items);
  for (const w of ['原産国', '内容量', 'メーカー型番']) assert.ok(items.some((x) => x.startsWith(w)), `未保存に ${w} が無い: ${JSON.stringify(items)}`);
  assert.equal(await humanOf('s002'), null, '「この値を入れる」だけで保存した');
  await p.click('#revert');
  assert.deepEqual([await p.inputValue('#f-lbl_country'), await p.inputValue('#f-lbl_net')], ['', '']);
  // 打って保存
  await p.fill('#f-lbl_country', '中国製');
  await p.fill('#f-lbl_net', '1500');
  await p.selectOption('#f-lbl_net_uom', 'g');
  await p.fill('#f-lbl_shelf', '365');
  assert.equal(await p.isDisabled('#save'), false);
  await Promise.all([p.waitForEvent('load'), p.click('#save')]);
  await p.waitForSelector('#saved-note');
  assert.deepEqual(await humanOf('s002'), { c: '中国', n: '1500.00', u: 'g', m: null, d: 365, via: 'sku_page' });
  assert.equal(await p.inputValue('#f-lbl_country'), '中国');
  assert.match(await p.textContent('#lbl-net-now'), /いま使う値: 1500g \(人が入れた値\)/);
  assert.match(await p.textContent('#saved-note'), /原産国: \(空\) → 中国/);
});

await ta('② 単品の新商品: 表示の項目を入れて保存 → 送る本文の label・Company DB / 入れないで保存 = 送らない (あと N つに数えない)', async (p) => {
  await p.goto(B + '/new?kind=single');
  await fillSingle(p, 'lbui-1');
  assert.equal(await p.locator('#checklist li', { hasText: '原産国' }).count(), 0, 'あと N つに表示の項目がある');
  await p.fill('#np-lbl-country', '日本');
  await p.fill('#np-lbl-net', '30');
  await p.selectOption('#np-lbl-net-uom', '枚');
  await p.fill('#np-lbl-model', 'UI-1');
  await noneRest(p);
  let sent = null;
  p.on('request', (r) => { if (r.url().endsWith('/api/new') && r.method() === 'POST') sent = JSON.parse(r.postData()); });
  assert.equal(await p.isDisabled('#save'), false, await p.textContent('#save'));
  await Promise.all([p.waitForURL(/\/sku\/lbui-1$/), p.click('#save')]);
  assert.deepEqual(sent.label, { country: '日本', net: '30', net_uom: '枚', model: 'UI-1', shelf: '' });
  assert.ok(!sent.blanks.some((k) => /^(lbl_|label$)|country|shelf/.test(k)));
  assert.deepEqual(await humanOf('lbui-1'), { c: '日本', n: '30.00', u: '枚', m: 'UI-1', d: null, via: 'register' });
  await p.goto(B + '/new?kind=single');
  await fillSingle(p, 'lbui-2');
  await noneRest(p);
  sent = null;
  await Promise.all([p.waitForURL(/\/sku\/lbui-2$/), p.click('#save')]);
  assert.equal(sent.label, undefined);
  assert.equal(await humanOf('lbui-2'), null);
});

await ta('③ まとまり: 共通の欄に表示の項目 (型番は無い)・入れただけで未保存に数える・確かめの中身に出る', async (p) => {
  await p.goto(B + '/new?kind=variation');
  assert.equal(await p.locator('#sec-common #np-lbl-country').count(), 1);
  assert.equal(await p.locator('#np-lbl-model').count(), 0, 'まとまりで型番を聞いた');
  assert.deepEqual(await p.evaluate(() => window.MasterEdit.dirty().items), [], '開いただけで未保存');
  await p.fill('#np-lbl-country', '韓国');
  assert.deepEqual(await p.evaluate(() => window.MasterEdit.dirty().items), ['表示の項目']);
});

await ta('④ スマホの幅 (390): 商品の画面の「表示の項目」が横にはみ出さない', async (p) => {
  await p.goto(B + '/sku/s002');
  await p.locator('#p-label').scrollIntoViewIfNeeded();
  const m = await p.evaluate(() => {
    const r = document.getElementById('p-label').getBoundingClientRect();
    const wide = [...document.querySelectorAll('#p-label *')].filter((x) => x.getBoundingClientRect().right > window.innerWidth + 1).slice(0, 8).map((x) => x.tagName + '#' + x.id + '.' + x.className + ':' + Math.round(x.getBoundingClientRect().right));
    return { sw: document.documentElement.scrollWidth, w: window.innerWidth, right: r.right, inH: document.getElementById('f-lbl_country').getBoundingClientRect().height, wide };
  });
  assert.ok(m.sw <= m.w + 1, `横にはみ出す (${m.sw} > ${m.w}) ${JSON.stringify(m.wide)}`);
  assert.ok(m.right <= m.w + 1, JSON.stringify(m));
  assert.ok(m.inH >= 40, JSON.stringify(m));
}, { width: 390, height: 844 });

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
await browser.close();
server.close();
await pg.close();
process.exit(process.exitCode || 0);
