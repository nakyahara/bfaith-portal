/**
 * test-master-maker-ui.mjs — メーカーの欄 (views/_maker.ejs・public/me-maker.js・Company DB の 0083) を本物の router で描いたページで動かす (Playwright・Chromium)
 *
 * 固定する動き:
 *   ① 単品の新商品: 名簿から探す (ひらがなで打ってカタカナの名前・半角カナ)・↑↓ と Enter で選ぶ・打っただけでは値にしない (あと N つに残る・欄を出ると一言)・
 *      日本語入力の確定の Enter では選ばない (#1724 と同じ)・変換中も探す・新しいメーカーとして登録 (確かめの窓・同じと見る名前は足さずに選ぶ)・
 *      「仕入先と同じ」(代表の仕入先の名前・先に仕入先を選ぶ)・「入力なし」と値は両方持たない・保存 → Company DB に付く
 *   ② まとまり: 送る本文に共通のメーカー (全部の子)・入力なしも「すすみぐあい」に数える
 *   ③ 商品の画面: 変えると未保存の「前 → 後」に名前・保存 → Company DB・空にして保存 = 外す・元に戻す
 *   + スマホの幅 (390): 横にはみ出さない・押す所は 44px
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-master-maker-ui.mjs   (入れてある Chrome で動かす = node --import <use-chrome.mjs> scripts/test-master-maker-ui.mjs)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = メーカーの画面の試験を飛ばす'); process.exit(0); }
let chromium; let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG メーカーの画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'maker-ui-'));

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const T = await setupVariationDb();
const { pg, q, one } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/mkui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/mkui';
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

// 名簿 (前からある 2 つ)
const MK = await import('../lib/master-maker.mjs');
const addMaker = async (name) => { await pg.query('set role master_edit'); try { return (await MK.createMaker(T.db, { requestId: crypto.randomUUID(), actor: 'naka@test', name })).maker; } finally { await pg.query('set role deploy'); } };
const HONPO = await addMaker('ハチミツ本舗');
const YAMADA = await addMaker('山田製作所');
const makerOf = async (code) => (await q(`select m.name from core.skus k join core.product_makers x on x.product_id = k.product_id join core.makers m on m.maker_id = x.maker_id where k.code = $1`, [code]))[0]?.name ?? null;

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
const hid = (p) => p.inputValue('#f-maker');
const opts = (p) => p.$$eval('#f-maker-list .co', (xs) => xs.map((x) => x.textContent.trim()));
const remain = async (p) => Number((/あと\s*(\d+)\s*つ/.exec(await p.textContent('#remain')) || [])[1] || 0);
const noneRest = (p) => p.evaluate(() => {
  for (const id of ['po-cond-mode', 'po-mat-mode']) { const sg = document.getElementById(id); if (sg && !sg.getAttribute('data-value')) sg.querySelector('button[data-v="none"]').click(); }
  document.querySelectorAll('input[type="checkbox"][data-none]').forEach((c) => { if (!c.disabled && !c.checked && !window.MasterEdit.none.has(c.getAttribute('data-none'))) c.click(); });
});
const fillSingle = async (p, code) => {
  await p.fill('#code', code);
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'メーカーの試験 ' + code);
  await p.fill('#f-standard_price', '1200');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.click('#f-sales_class button[data-v="3"]');
  await p.selectOption('#f-primary_supplier', '0034');
  await p.fill('#f-reorder_months', '1');
  await p.click('#f-expiry button[data-v="0"]');
  if (await p.locator('#f-lzbc-blank').count()) await p.check('#f-lzbc-blank');
};
console.log('メーカーの欄 (画面)');

await ta('① 単品の新商品: 探して選ぶ (ひらがな → カタカナ・半角カナ)・打っただけでは値にしない・確定の Enter では選ばない・新しく登録 (同じ名前は選ぶ)・仕入先と同じ・入力なし・保存', async (p) => {
  await p.goto(B + '/new?kind=single');
  // 欄の場所 = 代表の仕入先のすぐ下 (同じ区切り)
  assert.equal(await p.evaluate(() => document.querySelector('[data-row="primary_supplier"]').nextElementSibling.getAttribute('data-row')), 'maker');
  await fillSingle(p, 'mkui-1');
  const n0 = await remain(p);
  assert.ok(await p.locator('#checklist li', { hasText: 'メーカー' }).count() === 1, 'あと N つにメーカーが無い');
  // ひらがなで打つ = カタカナの名前が出る (かなの同一視)
  await p.click('#f-maker-in');
  await p.keyboard.type('はちみつ');
  assert.equal(await p.isVisible('#f-maker-list'), true);
  assert.deepEqual((await opts(p)).slice(0, 1), ['ハチミツ本舗']);
  assert.ok((await opts(p)).at(-1).includes('「はちみつ」を新しいメーカーとして登録'), '名簿に無い名前 = 最後に新規登録');
  assert.equal(await hid(p), '', '打っただけでは値にしない');
  assert.equal(await remain(p), n0);
  // 日本語入力の確定の Enter (compositionend の直後・keyCode 13・isComposing なし) = 選ばない・候補はそのまま (名前がぴったり = 候補の 1 つ目を選んでいる形)
  await p.fill('#f-maker-in', 'ハチミツ本舗');
  assert.equal(await p.getAttribute('#f-maker-in', 'aria-activedescendant'), 'mk-opt-0', 'ぴったりの名前 = 1 つ目を選んだ形');
  await p.evaluate(() => {
    const el = document.getElementById('f-maker-in');
    el.dispatchEvent(new CompositionEvent('compositionstart', { bubbles: true }));
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 229, isComposing: true, bubbles: true, cancelable: true }));   // 変換中の Enter
    el.dispatchEvent(new CompositionEvent('compositionend', { bubbles: true, data: 'ハチミツ本舗' }));
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 13, bubbles: true, cancelable: true }));                       // 確定の Enter (Windows の Chrome)
  });
  assert.equal(await hid(p), '', '確定の Enter で選んだ');
  assert.equal(await p.isVisible('#f-maker-list'), true, '確定の Enter で候補が閉じた');
  // キーを離した後の Enter = 選ぶ → 値・あと N つが 1 減る
  await p.evaluate(() => document.getElementById('f-maker-in').dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true })));
  await p.keyboard.press('Enter');
  assert.equal(await hid(p), HONPO.id);
  assert.equal(await p.inputValue('#f-maker-in'), 'ハチミツ本舗');
  assert.equal(await p.isVisible('#f-maker-list'), false);
  assert.equal(await remain(p), n0 - 1);
  // 打ち直す = 値をやめる (あと N つに戻る)・欄を出ると一言
  await p.fill('#f-maker-in', 'ﾊﾁﾐﾂ');
  assert.equal(await hid(p), '');
  assert.deepEqual((await opts(p)).slice(0, 1), ['ハチミツ本舗'], '半角カナでも当たる');
  await p.click('#f-name');
  await p.waitForFunction(() => /一覧から選ぶか/.test(document.getElementById('f-maker-msg').textContent));
  assert.equal(await remain(p), n0);
  // 新しいメーカー (確かめの窓 = OK) → 名簿に足して選ぶ
  let dialogs = [];
  p.on('dialog', async (d) => { dialogs.push(d.message()); await d.accept(); });
  await p.fill('#f-maker-in', 'みかわ 織物');
  await p.click('#f-maker-list .co.mk-new');
  await p.waitForFunction(() => document.getElementById('f-maker').value !== '');
  assert.match(dialogs[0], /「みかわ 織物」を新しいメーカーとして名簿に登録しますか/);
  assert.match(await p.textContent('#f-maker-msg'), /名簿に登録して選びました/);
  const mikawa = await one("select maker_id::text as id, name from core.makers where name = 'みかわ 織物'");
  assert.equal(await hid(p), mikawa.id);
  // 同じと見る名前 (カタカナ・空白なし) = 候補に「同じ名前」・新規登録の行は出さない
  await p.fill('#f-maker-in', 'ミカワ織物');
  const o2 = await opts(p);
  assert.ok(o2[0].startsWith('みかわ 織物') && o2[0].includes('同じ名前'), o2.join(' | '));
  assert.ok(!o2.some((x) => x.includes('新しいメーカーとして登録')), '同じと見る名前があるのに新規登録が出る');
  await p.keyboard.press('Enter');   // 同じ名前が選ばれている
  assert.equal(await hid(p), mikawa.id);
  // 仕入先と同じ = 代表の仕入先 (0034 三河テキスタイル) の名前 → 名簿に無い = 確かめて足す
  dialogs = [];
  await p.click('#f-maker-same');
  await p.waitForFunction((id) => document.getElementById('f-maker').value !== id, mikawa.id);
  assert.match(dialogs[0], /代表の仕入先と同じ名前「三河テキスタイル」/);
  assert.equal(await p.inputValue('#f-maker-in'), '三河テキスタイル');
  // 2 回目 = 名簿にある = 確かめ無しで選ぶ
  dialogs = [];
  await p.fill('#f-maker-in', '');
  await p.click('#f-maker-same');
  assert.equal(await p.inputValue('#f-maker-in'), '三河テキスタイル');
  assert.deepEqual(dialogs, []);
  assert.equal((await one("select count(*)::int as n from core.makers where name_norm = core.maker_name_norm('三河テキスタイル')")).n, 1);
  // 入力なし = 値を消す / 値を選ぶ = 入力なしが外れる (両方は持たない)
  await p.check('input[data-none="maker"]');
  assert.deepEqual([await hid(p), await p.inputValue('#f-maker-in')], ['', '']);
  assert.match(await p.textContent('#checklist'), /メーカー[\s\S]*入力なし/);
  await p.fill('#f-maker-in', '山田');
  await p.keyboard.press('ArrowDown');
  await p.keyboard.press('Enter');
  assert.equal(await hid(p), YAMADA.id);
  assert.equal(await p.isChecked('input[data-none="maker"]'), false, '選んだのに入力なしのまま');
  // 保存 → Company DB に付く (送る本文 = values.maker・blanks にメーカーは無い)
  await noneRest(p);
  let sent = null;
  p.on('request', (r) => { if (r.url().endsWith('/api/new') && r.method() === 'POST') sent = JSON.parse(r.postData()); });
  assert.equal(await p.isDisabled('#save'), false, await p.textContent('#save'));
  await Promise.all([p.waitForURL(/\/sku\/mkui-1$/), p.click('#save')]);
  assert.equal(sent.values.maker, YAMADA.id);
  assert.ok(!sent.blanks.includes('maker'));
  assert.equal(await makerOf('mkui-1'), '山田製作所');
  // できた商品の画面の見出しにメーカー
  assert.match(await p.textContent('#maker-head'), /メーカー 山田製作所/);
});

await ta('①b 単品の新商品: メーカーを入力なしで保存 = 理由に「入力なし: メーカー」・セットの新商品にはメーカーの欄が無い', async (p) => {
  await p.goto(B + '/new?kind=single');
  await fillSingle(p, 'mkui-2');
  await noneRest(p);
  assert.equal(await p.isChecked('input[data-none="maker"]'), true);
  await Promise.all([p.waitForURL(/\/sku\/mkui-2$/), p.click('#save')]);
  assert.equal(await makerOf('mkui-2'), null);
  const reason = (await one("select reason from ops.master_registration_events e join core.skus k on k.sku_id = e.sku_id where k.code = 'mkui-2' order by e.event_id limit 1")).reason;
  assert.match(reason, /入力なし: .*メーカー/);
  await p.goto(B + '/new?kind=set');
  assert.equal(await p.locator('#f-maker-in').count(), 0);
});

await ta('② まとまり: 共通の欄にメーカー (代表の仕入先のすぐ下)・すすみぐあいに数える・送る本文の values.maker', async (p) => {
  await p.goto(B + '/new?kind=variation');
  const row = p.locator('[data-row="maker"]');
  assert.equal(await row.count(), 1);
  assert.equal(await p.evaluate(() => document.querySelector('[data-row="maker"]').previousElementSibling.querySelector('#f-primary_supplier') !== null), true, '代表の仕入先のすぐ下でない');
  const issues = async () => p.evaluate(() => document.body.innerText.includes('メーカー (一覧から選ぶ)'));
  assert.equal(await issues(), true, 'すすみぐあいにメーカーが無い');
  await p.fill('#f-maker-in', '山田');
  await p.keyboard.press('ArrowDown');
  await p.keyboard.press('Enter');
  assert.equal(await hid(p), YAMADA.id);
  assert.equal(await issues(), false, '選んだのにすすみぐあいに残る');
  await p.check('input[data-none="maker"]');
  assert.equal(await hid(p), '');
  assert.equal(await issues(), false, '入力なし = 数え終わり');
  await p.fill('#f-maker-in', 'ハチ');
  await p.keyboard.press('ArrowDown');
  await p.keyboard.press('Enter');
  assert.equal(await p.isChecked('input[data-none="maker"]'), false);
  // 送る本文 (collect) に共通のメーカー
  const body = await p.evaluate(() => { const el = document.getElementById('f-maker'); return el.value; });
  assert.equal(body, HONPO.id);
});

await ta('③ 商品の画面: 選ぶと未保存の「前 → 後」に名前・保存 → Company DB・空にして保存 = 外す・元に戻す', async (p) => {
  await p.goto(B + '/sku/s002');
  assert.equal(await p.evaluate(() => document.querySelector('[data-row="primary_supplier"]').nextElementSibling.getAttribute('data-row')), 'maker');
  await p.fill('#f-maker-in', '山田');
  await p.keyboard.press('ArrowDown');
  await p.keyboard.press('Enter');
  assert.equal(await hid(p), YAMADA.id);
  const items = await p.evaluate(() => window.MasterEdit.dirty().items);
  assert.deepEqual(items, ['メーカー: なし → 山田製作所'], '未保存の「前 → 後」に名前');
  // 元に戻す = 名前の欄も戻る
  await p.click('#revert');
  assert.deepEqual([await hid(p), await p.inputValue('#f-maker-in')], ['', '']);
  // 選んで保存
  await p.fill('#f-maker-in', '山田');
  await p.keyboard.press('ArrowDown');
  await p.keyboard.press('Enter');
  await p.click('#save');
  await p.waitForFunction(() => /保存しました|変えました|保存/.test(document.getElementById('result')?.textContent || ''), null, { timeout: 15000 }).catch(() => {});
  for (let i = 0; i < 50 && (await makerOf('s002')) !== '山田製作所'; i++) await p.waitForTimeout(200);
  assert.equal(await makerOf('s002'), '山田製作所');
  // 空にして保存 = 外す
  await p.goto(B + '/sku/s002');
  assert.equal(await p.inputValue('#f-maker-in'), '山田製作所');
  await p.fill('#f-maker-in', '');
  assert.equal(await hid(p), '');
  await p.click('#save');
  for (let i = 0; i < 50 && (await makerOf('s002')) !== null; i++) await p.waitForTimeout(200);
  assert.equal(await makerOf('s002'), null);
});

await ta('④ スマホの幅 (390): 横にはみ出さない・押す所は 44px・候補は画面の中', async (p) => {
  await p.goto(B + '/new?kind=single');
  const sw0 = await p.evaluate(() => document.documentElement.scrollWidth);
  assert.ok(sw0 <= 391, `開いたときから横にはみ出す (${sw0})`);
  await p.fill('#f-maker-in', 'は');
  const m = await p.evaluate(() => {
    const r = (s) => document.querySelector(s).getBoundingClientRect();
    return { sw: document.documentElement.scrollWidth, w: window.innerWidth, inH: r('#f-maker-in').height, btnH: r('#f-maker-same').height, list: r('#f-maker-list') };
  });
  assert.ok(m.sw <= m.w + 1, `横にはみ出す (${m.sw} > ${m.w})`);
  assert.ok(m.inH >= 44 && m.btnH >= 44, JSON.stringify(m));
  assert.ok(m.list.right <= m.w + 1 && m.list.left >= -1, JSON.stringify(m.list));
}, { width: 390, height: 844 });

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
await browser.close();
server.close();
await pg.close();
process.exit(process.exitCode || 0);
