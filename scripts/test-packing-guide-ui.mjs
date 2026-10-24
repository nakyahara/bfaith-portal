/**
 * test-packing-guide-ui.mjs — 商品の画面の「梱包指示」(views/_packing-guide.ejs・public/me-packing-guide.js・Company DB の 0101) を本物の router で描いたページで動かす (Playwright・Chromium)
 *
 * 固定する動き (Codex 第 1 段階レビューの指摘も):
 *   ① 写真を足す (選ぶ → 縮めて送る → 並びに出る)・並べ替え・一言・注意点 → 「梱包指示を保存」→ Company DB
 *   ② 送っている間は打てない (返事で、送った後の入力を消さない)
 *   ③ 梱包指示を直したまま右の「保存する」(ほかの欄) → 読み直しても、保存していない梱包指示の変更は戻る (消えない)・保存してある指示が変わっていたら戻さない
 *   ④ 中身と版を 1 つの文で読む (別々に読むと「足す前の写真 + 足した後の版」になる)
 *   ⑤ スマホの幅 (390): 横にはみ出さない
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-packing-guide-ui.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 梱包指示の画面の試験を飛ばす'); process.exit(0); }
let chromium; let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 梱包指示の画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pgd-ui-'));

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const T = await setupVariationDb();
const { pg, db, q, asEditor } = T;
const PG = await import('../lib/packing-guide.mjs');
const { default: sharp } = await import('sharp');
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/pgdui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/pgdui';
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
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;
const skuIdOf = async (code) => (await q('select sku_id::text as id from core.skus where code = $1', [code]))[0].id;
const guideOf = async (code) => { const id = await skuIdOf(code); return asEditor(() => PG.readPackingGuide(db, id)); };
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pgd-ui-img-'));
const imgFile = async (name, color, w = 2400, h = 1800) => {
  const f = path.join(tmp, name);
  fs.writeFileSync(f, await sharp({ create: { width: w, height: h, channels: 3, background: color } }).jpeg().toBuffer());
  return f;
};

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
console.log('梱包指示の画面 (商品の画面)');

await ta('① 写真を足す (縮めて送る → 並びに出る)・並べ替え・一言・注意点 → 「梱包指示を保存」→ Company DB', async (p) => {
  await p.goto(B + '/sku/s001');
  assert.equal(await p.isDisabled('#pgd-save'), true, '変えていない = 押せない');
  for (const [n, c] of [['a.jpg', { r: 200, g: 50, b: 50 }], ['b.jpg', { r: 50, g: 200, b: 50 }]]) {
    const before = await p.locator('#pgd-photos .pgd-ph').count();
    await p.setInputFiles('#pgd-file', await imgFile(n, c));
    await p.waitForFunction((k) => document.querySelectorAll('#pgd-photos .pgd-ph').length === k, before + 1, { timeout: 20000 });
  }
  let g = await guideOf('s001');
  assert.deepEqual(g.photos.map((x) => [x.position, x.width, x.height]), [[1, 1280, 960], [2, 1280, 960]], '画面が縮めて・サーバーが作り直した');
  // 2 枚目を前へ・一言・注意点
  await p.locator('#pgd-photos .pgd-ph').nth(1).locator('[data-act="up"]').click();
  await p.locator('#pgd-photos .pgd-ph').nth(0).locator('input').fill('最初に底へ');
  await p.fill('#pgd-note', '割れ物。\n天地無用');
  assert.equal(await p.isDisabled('#pgd-save'), false);
  assert.ok((await p.evaluate(() => window.MasterEdit.dirty().items)).includes('梱包指示: まだ保存していません'));
  await p.click('#pgd-save');
  await p.waitForFunction(() => /保存しました/.test(document.getElementById('pgd-msg').textContent), null, { timeout: 15000 });
  const g2 = await guideOf('s001');
  assert.equal(g2.note, '割れ物。\n天地無用');
  assert.deepEqual(g2.photos.map((x) => [x.photo_id, x.caption]), [[g.photos[1].photo_id, '最初に底へ'], [g.photos[0].photo_id, '']]);
  assert.equal(await p.isDisabled('#pgd-save'), true, '保存した = 未保存なし');
  assert.equal((await p.evaluate(() => window.MasterEdit.dirty().items)).includes('梱包指示: まだ保存していません'), false);
});

await ta('② 送っている間は打てない (返事が遅くても、送った後に打った字を消さない)', async (p) => {
  await p.goto(B + '/sku/s001');
  let release;
  const held = new Promise((r) => { release = r; });
  await p.route('**/api/sku/s001/packing-guide', async (route) => { await held; await route.continue(); });
  await p.fill('#pgd-note', 'A');
  await p.click('#pgd-save');
  await p.waitForFunction(() => /保存しています/.test(document.getElementById('pgd-msg').textContent));
  assert.equal(await p.isDisabled('#pgd-note'), true, '送っている間は注意点を打てない');
  assert.equal(await p.locator('#pgd-photos input').first().isDisabled(), true, '一言も');
  assert.equal(await p.locator('#pgd-photos [data-act="rm"]').first().isDisabled(), true, '外すも');
  release();
  await p.waitForFunction(() => /保存しました/.test(document.getElementById('pgd-msg').textContent), null, { timeout: 15000 });
  assert.equal(await p.isDisabled('#pgd-note'), false, '返事の後は打てる');
  assert.equal(await p.inputValue('#pgd-note'), 'A');
  assert.equal((await guideOf('s001')).note, 'A');
});

await ta('③ 梱包指示を直したまま右の「保存する」→ 読み直しても梱包指示の変更は戻る (消えない)・保存してある指示が変わっていたら戻さない', async (p) => {
  await p.goto(B + '/sku/s001');
  await p.fill('#pgd-note', 'まだ保存していない注意点');
  await p.locator('#pgd-photos .pgd-ph').nth(0).locator('input').fill('まだの一言');
  // ほかの欄 (右の「保存する」) を変えて保存 = 確かめずに読み直す
  await p.fill('#f-reorder_months', '5');
  await Promise.all([p.waitForEvent('load'), p.click('#save')]);
  await p.waitForSelector('#saved-note');
  assert.equal(await p.inputValue('#pgd-note'), 'まだ保存していない注意点', '梱包指示の変更が戻る');
  assert.equal(await p.locator('#pgd-photos .pgd-ph').nth(0).locator('input').inputValue(), 'まだの一言');
  assert.match(await p.textContent('#pgd-msg'), /保存していなかった梱包指示の変更を戻しました/);
  assert.equal(await p.isDisabled('#pgd-save'), false, '未保存のまま = 保存できる');
  assert.equal((await guideOf('s001')).note, 'A', 'Company DB はまだ変わっていない');
  // 読み直す前に、ほかの人が指示を変えた = 戻さない (古い下書きで上書きしない)
  await p.fill('#pgd-note', '古くなる下書き');
  const g = await guideOf('s001');
  const sid = await skuIdOf('s001');
  await asEditor(() => PG.savePackingGuide(db, { requestId: crypto.randomUUID(), actor: 'other@test', via: 'sku_page', skuId: sid, seenEventId: g.event_id, note: 'ほかの人の注意点', photos: g.photos.map((x) => ({ photo_id: x.photo_id, caption: x.caption })) }));
  await p.reload();
  assert.equal(await p.inputValue('#pgd-note'), 'ほかの人の注意点', '保存してある指示が変わった = 下書きは捨てる');
  assert.equal(await p.isDisabled('#pgd-save'), true);
});

await ta('④ 中身と版を 1 つの文で読む (足す前の写真 + 足した後の版、にならない)', async () => {
  const sid = await skuIdOf('s001');
  const seen = [];
  const spy = { query: (t, params) => { seen.push(t); return db.query(t, params); } };
  await asEditor(() => PG.readPackingGuide(spy, sid));
  assert.equal(seen.length, 1, seen.join('\n---\n'));
});

await ta('⑤ スマホの幅 (390): 梱包指示の節が横にはみ出さない', async (p) => {
  await p.goto(B + '/sku/s001');
  await p.locator('#p-pgd').scrollIntoViewIfNeeded();
  assert.equal(await p.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1), true);
}, { width: 390, height: 844 });

await browser.close();
server.close();
clearTimeout(killer);
console.log(`${passed} passed`);
process.exit(process.exitCode || 0);
