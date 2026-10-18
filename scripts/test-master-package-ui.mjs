/**
 * test-master-package-ui.mjs — 「大きさ・重さ」(views/_package.ejs・_package-new.ejs・public/me-package.js・Company DB の 0087) を本物の router で描いたページで動かす (Playwright・Chromium)
 *
 * 固定する動き:
 *   ① 商品の画面: 「この値を入れる」= その出どころの持っている軸だけ欄に入れる (未保存の「前 → 後」に出る)・cm で打つ・保存 → Company DB (mm・g)・元に戻す
 *   ② 単品の新商品: 大きさ・重さを入れて保存 → 送る本文の package・Company DB に付く / 入れないで保存 = 送らない (あと N つに数えない)
 *   ③ まとまり: 共通の欄に大きさ・重さ (全部の子に同じ値の一言)
 *   ④ スマホの幅 (390): 商品の画面の「大きさ・重さ」が横にはみ出さない
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-master-package-ui.mjs   (入れてある Chrome で動かす = node --import <use-chrome.mjs> scripts/test-master-package-ui.mjs)
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 大きさ・重さの画面の試験を飛ばす'); process.exit(0); }
let chromium; let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 大きさ・重さの画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pkg-ui-'));

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const T = await setupVariationDb();
const { pg, q } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/pkui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/pkui';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const { default: router, __setPgClientFactory, __setShippingRatesProvider, __setPhDraftLookup } = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => new Map([['B2', { method: '宅急便コンパクト', cost: 520 }]]));
__setPhDraftLookup(async () => null);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit'] }; next(); });
app.use('/apps/master-edit', router);
app.get('/', (req, res) => res.send('<p>portal</p>'));
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;

// 出どころ (夜間ロードと同じ表): s002 = 定形外 (実測 250 g・厚み 30 mm) と FBA 納品 (実測 260 g)
{
  const pid = (await q("select product_id from core.skus where code = 's002'"))[0].product_id;
  const ins = (g, h, src, ref, d) => pg.query(`insert into core.product_physicals (company_id, product_id, scope, height_mm, weight_g, source_system, source_ref, is_measured, observed_at, created_by_type, created_by_id)
    values (1, $1, 'package', $2, $3, $4, $5, true, now() - ($6 || ' days')::interval, 'system', 'test')`, [pid, h, g, src, ref, String(d)]);
  await ins(250, 30, 'measured', 'pm_skus', 3);
  await ins(260, null, 'measured', 'fbx_weight_current:X00UI', 1);
}
const humanOf = async (code) => (await q(`select x.length_mm, x.width_mm, x.height_mm, x.weight_g, x.set_via from core.skus k join core.product_packages x on x.product_id = k.product_id where k.code = $1`, [code]))[0] || null;

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
  await p.fill('#f-name', '大きさの試験 ' + code);
  await p.fill('#f-standard_price', '1200');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.click('#f-sales_class button[data-v="3"]');
  await p.selectOption('#f-primary_supplier', '0034');
  await p.fill('#f-reorder_months', '1');
  await p.click('#f-expiry button[data-v="0"]');
  if (await p.locator('#f-lzbc-blank').count()) await p.check('#f-lzbc-blank');
};
console.log('大きさ・重さ (画面)');

await ta('① 商品の画面: 「この値を入れる」(持っている軸だけ)・未保存の「前 → 後」・cm で打って保存 → Company DB (mm・g)・元に戻す', async (p) => {
  await p.goto(B + '/sku/s002');
  assert.equal(await p.getAttribute('#f-pkg_weight', 'placeholder'), '260', '空の欄の薄い字 = いま使う値');
  // 1 行目 = FBA 納品 (実測 260 g・重さだけ) → 重さだけ入る
  await p.click('button[data-pkg-use="0"]');
  assert.deepEqual([await p.inputValue('#f-pkg_weight'), await p.inputValue('#f-pkg_height')], ['260', '']);
  // 2 行目 = 定形外 (250 g・厚み 3 cm) → 重さと高さ
  await p.click('button[data-pkg-use="1"]');
  assert.deepEqual([await p.inputValue('#f-pkg_weight'), await p.inputValue('#f-pkg_height')], ['250', '3']);
  assert.deepEqual((await p.evaluate(() => window.MasterEdit.dirty().items)).sort(), ['梱包の重さ (g): (空) → 250 g', '梱包の高さ (cm): (空) → 3 cm'].sort());
  // 元に戻す = 欄は空に戻る
  await p.click('#revert');
  assert.deepEqual([await p.inputValue('#f-pkg_weight'), await p.inputValue('#f-pkg_height')], ['', '']);
  // 打って保存 (cm)
  await p.fill('#f-pkg_length', '23.5');
  await p.fill('#f-pkg_width', '12');
  await p.fill('#f-pkg_height', '2.5');
  await p.fill('#f-pkg_weight', '255');
  assert.equal(await p.isDisabled('#save'), false);
  await Promise.all([p.waitForEvent('load'), p.click('#save')]);
  await p.waitForSelector('#saved-note');
  assert.deepEqual(await humanOf('s002'), { length_mm: 235, width_mm: 120, height_mm: 25, weight_g: 255, set_via: 'sku_page' });
  assert.equal(await p.inputValue('#f-pkg_length'), '23.5');
  assert.match(await p.textContent('#pkg-weight-now'), /いま使う値: 255 g \(人\)/);
  assert.match(await p.textContent('#saved-note'), /梱包の縦 \(cm\): \(空\) → 23\.5/);
});

await ta('② 単品の新商品: 大きさ・重さを入れて保存 → 送る本文の package・Company DB / 入れないで保存 = 送らない (あと N つに数えない)', async (p) => {
  await p.goto(B + '/new?kind=single');
  await fillSingle(p, 'pkui-1');
  assert.equal(await p.locator('#checklist li', { hasText: '梱包' }).count(), 0, 'あと N つに大きさ・重さがある');
  await p.fill('#np-length', '20');
  await p.fill('#np-width', '15.5');
  await p.fill('#np-height', '2');
  await p.fill('#np-weight', '180');
  await noneRest(p);
  let sent = null;
  p.on('request', (r) => { if (r.url().endsWith('/api/new') && r.method() === 'POST') sent = JSON.parse(r.postData()); });
  assert.equal(await p.isDisabled('#save'), false, await p.textContent('#save'));
  await Promise.all([p.waitForURL(/\/sku\/pkui-1$/), p.click('#save')]);
  assert.deepEqual(sent.package, { length: '20', width: '15.5', height: '2', weight: '180' });
  assert.ok(!sent.blanks.some((k) => /pkg|package/.test(k)));
  assert.deepEqual(await humanOf('pkui-1'), { length_mm: 200, width_mm: 155, height_mm: 20, weight_g: 180, set_via: 'register' });
  // 入れない = 送らない
  await p.goto(B + '/new?kind=single');
  await fillSingle(p, 'pkui-2');
  await noneRest(p);
  sent = null;
  await Promise.all([p.waitForURL(/\/sku\/pkui-2$/), p.click('#save')]);
  assert.equal(sent.package, undefined);
  assert.equal(await humanOf('pkui-2'), null);
});

await ta('③ まとまり: 共通の欄に大きさ・重さ (全部の子に同じ値の一言)', async (p) => {
  await p.goto(B + '/new?kind=variation');
  assert.equal(await p.locator('#sec-common #np-length').count(), 1);
  assert.match(await p.textContent('#sec-common'), /全部の子に同じ値 \(サイズで違うときは空のまま登録して/);
  assert.deepEqual(await p.evaluate(() => window.MasterEdit.dirty().items), [], '開いただけで未保存');
  await p.fill('#np-weight', '400');
  // 🆕 Codex R1 M: 大きさ・重さだけ入れた = 未保存に数える (離れるときの確認が出る)
  assert.deepEqual(await p.evaluate(() => window.MasterEdit.dirty().items), ['大きさ・重さ']);
  let asked = null;
  p.once('dialog', async (d) => { asked = d.type(); await d.dismiss(); });
  await p.evaluate(() => { location.href = '/'; }).catch(() => {});
  await p.waitForTimeout(500);
  assert.equal(asked, 'beforeunload', '離れるときの確認が出ない');
});

await ta('④ スマホの幅 (390): 商品の画面の「大きさ・重さ」が横にはみ出さない', async (p) => {
  await p.goto(B + '/sku/s002');
  await p.locator('#p-pkg').scrollIntoViewIfNeeded();
  const m = await p.evaluate(() => {
    const r = document.getElementById('p-pkg').getBoundingClientRect();
    const wide = [...document.querySelectorAll('body *')].filter((x) => x.getBoundingClientRect().right > window.innerWidth + 1 && !x.closest('.tblwrap, .scrollx')).slice(0, 8).map((x) => x.tagName + '#' + x.id + '.' + x.className + ':' + Math.round(x.getBoundingClientRect().right) + (x.closest('#p-pkg') ? ' (p-pkg)' : '') + ' ' + x.textContent.slice(0, 40));
    return { sw: document.documentElement.scrollWidth, w: window.innerWidth, right: r.right, inH: document.getElementById('f-pkg_length').getBoundingClientRect().height, wide };
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
