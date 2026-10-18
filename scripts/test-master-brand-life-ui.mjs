/**
 * test-master-brand-life-ui.mjs — ブランド・商品の段階の欄 (views/_brand.ejs・_brandlife-new.ejs・_brandlife-sku.ejs・public/me-maker.js の brand・Company DB の 0085) を
 * 本物の router で描いたページで動かす (Playwright・Chromium)
 *
 * 固定する動き:
 *   ① 単品の新商品: 板の場所 (税・分類・仕入先の次・飛び先の帯)・名簿から探す (ひらがな → カタカナ・「自社」の札)・打っただけでは値にしない (保存を止める)・
 *      「自社ブランドとして登録」(確かめの窓・名簿に印)・日本語入力の確定の Enter では選ばない・区分と終売の予定日・保存 → Company DB (「入力なし」に数えない)
 *   ② まとまり: 共通の欄の塊・今あるまとまりを選ぶ = 子の値が全部同じなら写す・選び直したら持ち越さない
 *   ③ 商品の画面: 変えると未保存の「前 → 後」・元に戻す・保存 → Company DB・「自社ブランドにする」(名簿の印・確かめの窓)・選んでいない名前 = 保存を止める・発売日 (読むだけ)
 *   ④ スマホの幅 (390): 横にはみ出さない・押す所は 44px
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-master-brand-life-ui.mjs   (入れてある Chrome で動かす = node --import <use-chrome.mjs> scripts/test-master-brand-life-ui.mjs)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = ブランド・商品の段階の画面の試験を飛ばす'); process.exit(0); }
let chromium; let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG ブランド・商品の段階の画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const killer = setTimeout(() => { console.error('時間切れ (420 秒)'); process.exit(3); }, 420e3);
killer.unref();
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'brandlife-ui-'));

const { setupVariationDb } = await import('./fixtures/master-variation-db.mjs');
const T = await setupVariationDb();
const { pg, q, one } = T;
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/blui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/blui';
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

// 名簿 (前からある 2 つ: 自社 1・他社 1)
const BL = await import('../lib/master-brand-life.mjs');
const addBrand = async (name, own) => { await pg.query('set role master_edit'); try { return (await BL.createBrand(T.db, { requestId: crypto.randomUUID(), actor: 'naka@test', name, own })).brand; } finally { await pg.query('set role deploy'); } };
const KOBO = await addBrand('ハチミツ工房', true);
const YOSO = await addBrand('よそブランド', false);
// ブランド・段階の付いた今あるまとまり (②)
{
  const V = await import('../lib/master-variation.mjs');
  await pg.query('set role master_edit');
  try {
    await V.registerVariationBatch(T.db, { actor: 'naka@test', requestId: crypto.randomUUID(), group: { mode: 'new', code: 'blgrp', name: 'ブランドのまとまり' }, axes: [{ axis: 1, name: 'カラー' }],
      options: [{ axis: 1, code: '-A', name: 'A' }, { axis: 1, code: '-B', name: 'B' }],
      children: [{ code: 'blgrp-A', choices: { 1: '-A' }, name: 'ブランドのまとまり【A】' }, { code: 'blgrp-B', choices: { 1: '-B' }, name: 'ブランドのまとまり【B】' }],
      values: { standard_price: '1000', tax_rate: '0.1', sales_class: '3', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0', brand: YOSO.id, life_stage: 'seasonal', end_planned_on: '2027-02-28' } },
    { open: true, shippingRates: new Map() });
  } finally { await pg.query('set role deploy'); }
}
const lifeOf = async (code) => (await q(`select b.name as brand, l.stage, l.end_planned_on::text as end_on from core.skus k
  left join core.product_brands x on x.product_id = k.product_id left join core.brands b on b.brand_id = x.brand_id
  left join core.product_lifecycle l on l.product_id = k.product_id where k.code = $1`, [code]))[0] || null;

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
const hid = (p) => p.inputValue('#f-brand');
const opts = (p) => p.$$eval('#f-brand-list .co', (xs) => xs.map((x) => x.textContent.trim()));
const remain = async (p) => Number((/あと\s*(\d+)\s*つ/.exec(await p.textContent('#remain')) || [])[1] || 0);
const noneRest = (p) => p.evaluate(() => {
  for (const id of ['po-cond-mode', 'po-mat-mode']) { const sg = document.getElementById(id); if (sg && !sg.getAttribute('data-value')) sg.querySelector('button[data-v="none"]').click(); }
  document.querySelectorAll('input[type="checkbox"][data-none]').forEach((c) => { if (!c.disabled && !c.checked && !window.MasterEdit.none.has(c.getAttribute('data-none'))) c.click(); });
});
const fillSingle = async (p, code) => {
  await p.fill('#code', code);
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'ブランドの試験 ' + code);
  await p.fill('#f-standard_price', '1200');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.click('#f-sales_class button[data-v="3"]');
  await p.selectOption('#f-primary_supplier', '0034');
  await p.fill('#f-reorder_months', '1');
  await p.click('#f-expiry button[data-v="0"]');
  if (await p.locator('#f-lzbc-blank').count()) await p.check('#f-lzbc-blank');
};
const pickBrand = async (p, typed) => { await p.fill('#f-brand-in', typed); await p.keyboard.press('ArrowDown'); await p.keyboard.press('Enter'); };
console.log('ブランド・商品の段階の欄 (画面)');

await ta('① 単品の新商品: 板の場所・探して選ぶ・打っただけでは保存を止める・確定の Enter では選ばない・自社ブランドとして登録・区分と予定日・保存 (「入力なし」に数えない)', async (p) => {
  await p.goto(B + '/new?kind=single');
  assert.equal(await p.evaluate(() => document.getElementById('sec-tax').nextElementSibling.id), 'sec-brandlife', '板が「税・分類・仕入先」の次に無い');
  assert.equal(await p.textContent('#jump a[data-sec="sec-brandlife"]'), '4 ブランド・段階');
  await fillSingle(p, 'blui-1');
  await noneRest(p);
  const n0 = await remain(p);
  assert.equal(n0, 0, await p.textContent('#checklist'));
  assert.equal(await p.locator('#checklist li', { hasText: 'ブランド' }).count(), 0, '必須でないのに「あと N つ」に出る');
  // ひらがな = カタカナの名前・「自社」の札・名簿に無い = 新しい / 自社ブランドの 2 つ
  await p.click('#f-brand-in');
  await p.keyboard.type('はちみつ');
  const o = await opts(p);
  assert.ok(o[0].startsWith('ハチミツ工房') && o[0].includes('自社'), o.join(' | '));
  assert.ok(o.at(-2).includes('「はちみつ」を新しいブランドとして登録') && o.at(-1).includes('「はちみつ」を自社ブランドとして登録'), o.join(' | '));
  // 打っただけ = 値にしない・保存を止める (あと 1 つ = ブランドを一覧から選ぶ)
  assert.equal(await hid(p), '');
  assert.equal(await remain(p), 1);
  assert.equal(await p.locator('#checklist li', { hasText: 'ブランドを一覧から選ぶ' }).count(), 1);
  // 日本語入力の確定の Enter = 選ばない
  await p.fill('#f-brand-in', 'ハチミツ工房');
  await p.evaluate(() => {
    const el = document.getElementById('f-brand-in');
    el.dispatchEvent(new CompositionEvent('compositionend', { bubbles: true, data: 'ハチミツ工房' }));
    el.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 13, bubbles: true, cancelable: true }));
  });
  assert.equal(await hid(p), '', '確定の Enter で選んだ');
  await p.evaluate(() => document.getElementById('f-brand-in').dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true })));
  await p.keyboard.press('Enter');
  assert.equal(await hid(p), KOBO.id);
  assert.equal(await p.isVisible('#f-brand-owntag'), true, '自社ブランドの札が出ない');
  assert.equal(await remain(p), 0);
  // 自社ブランドとして新しく登録 (確かめの窓)
  const dialogs = [];
  p.on('dialog', async (d) => { dialogs.push(d.message()); await d.accept(); });
  await p.fill('#f-brand-in', 'みつばち 舎');
  await p.click('#f-brand-list .co.mk-new:has-text("自社ブランドとして登録")');
  await p.waitForFunction(() => document.getElementById('f-brand').value !== '');
  assert.match(dialogs[0], /「みつばち 舎」を自社ブランドとして名簿に登録しますか/);
  assert.match(await p.textContent('#f-brand-msg'), /自社ブランドとして登録して選びました/);
  const mitsu = await one("select brand_id::text as id, own_brand from core.brands where name = 'みつばち 舎'");
  assert.deepEqual([await hid(p), mitsu.own_brand], [mitsu.id, true]);
  assert.equal(await p.isVisible('#f-brand-owntag'), true);
  // 区分・終売の予定日
  await p.selectOption('#f-life_stage', 'trial');
  await p.fill('#f-end_planned_on', '2027-04-01');
  let sent = null;
  p.on('request', (r) => { if (r.url().endsWith('/api/new') && r.method() === 'POST') sent = JSON.parse(r.postData()); });
  assert.equal(await p.isDisabled('#save'), false, await p.textContent('#save'));
  await Promise.all([p.waitForURL(/\/sku\/blui-1$/), p.click('#save')]);
  assert.deepEqual([sent.values.brand, sent.values.life_stage, sent.values.end_planned_on], [mitsu.id, 'trial', '2027-04-01']);
  for (const k of ['brand', 'life_stage', 'end_planned_on']) assert.ok(!sent.blanks.includes(k), k);
  assert.deepEqual(await lifeOf('blui-1'), { brand: 'みつばち 舎', stage: 'trial', end_on: '2027-04-01' });
  // できた商品の画面: 見出し・板・発売日 (まだ売れていない)
  assert.match(await p.textContent('#brand-head'), /ブランド みつばち 舎 \(自社\)/);
  assert.equal(await p.inputValue('#f-life_stage'), 'trial');
  assert.match(await p.textContent('#first-sold'), /まだ売れていません/);
});

await ta('①b 単品の新商品: どれも入れなくても保存できる (空 = なし)', async (p) => {
  await p.goto(B + '/new?kind=single');
  await fillSingle(p, 'blui-2');
  await noneRest(p);
  await Promise.all([p.waitForURL(/\/sku\/blui-2$/), p.click('#save')]);
  assert.deepEqual(await lifeOf('blui-2'), { brand: null, stage: null, end_on: null });
});

await ta('② まとまり: 共通の欄の塊・今あるまとまり = 子の値が全部同じなら写す・選び直したら持ち越さない', async (p) => {
  await p.goto(B + '/new?kind=variation');
  assert.equal(await p.locator('#sub-brandlife').count(), 1);
  assert.equal(await p.locator('[data-row="brand"]').count(), 1);
  const pick = async (code) => {
    await p.click('[data-mode="add"]');
    await p.fill('#g-q', code);
    await p.waitForSelector(`#g-results .gitem:has(.code:text-is("${code}"))`);
    await p.click(`#g-results .gitem:has(.code:text-is("${code}"))`);
    await p.waitForSelector('#g-picked .picked');
  };
  await pick('blgrp');
  await p.waitForFunction((id) => document.getElementById('f-brand').value === id, YOSO.id);
  assert.deepEqual([await p.inputValue('#f-brand-in'), await p.inputValue('#f-life_stage'), await p.inputValue('#f-end_planned_on')], ['よそブランド', 'seasonal', '2027-02-28']);
  await p.click('#g-picked [data-act="repick"]');
  await pick('ws100');
  await p.waitForFunction(() => document.getElementById('f-brand').value === '');
  assert.deepEqual([await p.inputValue('#f-brand-in'), await p.inputValue('#f-life_stage'), await p.inputValue('#f-end_planned_on')], ['', '', '']);
  // 打っただけ = すすみぐあいに「ブランドを一覧から選ぶ」
  await p.fill('#f-brand-in', 'よそ');
  await p.waitForFunction(() => document.body.innerText.includes('ブランドを一覧から選ぶ'));
  await p.keyboard.press('ArrowDown');
  await p.keyboard.press('Enter');
  assert.equal(await hid(p), YOSO.id);
  await p.waitForFunction(() => !document.body.innerText.includes('ブランドを一覧から選ぶ'));
});

await ta('③ 商品の画面: 前 → 後・元に戻す・保存 → Company DB・空にして保存 = 外す・選んでいない名前 = 保存を止める', async (p0, ctx) => {
  let p = p0;
  await p.goto(B + '/sku/s002');
  assert.equal(await p.evaluate(() => document.querySelector('section[aria-labelledby="h-sup"]').nextElementSibling.id), 'sec-brandlife', '板が仕入先の板の下に無い');
  await pickBrand(p, 'よそ');
  await p.selectOption('#f-life_stage', 'staple');
  await p.fill('#f-end_planned_on', '2027-05-05');
  const items = await p.evaluate(() => window.MasterEdit.dirty().items);
  assert.deepEqual(items, ['ブランド: なし → よそブランド', '区分 (定番・季節・試し): (なし) → 定番', '終売の予定日: (空) → 2027-05-05']);
  await p.click('#revert');
  assert.deepEqual([await hid(p), await p.inputValue('#f-brand-in'), await p.inputValue('#f-life_stage'), await p.inputValue('#f-end_planned_on')], ['', '', '', '']);
  await pickBrand(p, 'よそ');
  await p.selectOption('#f-life_stage', 'staple');
  await p.click('#save');
  for (let i = 0; i < 50 && (await lifeOf('s002')).brand !== 'よそブランド'; i++) await p.waitForTimeout(200);
  assert.deepEqual(await lifeOf('s002'), { brand: 'よそブランド', stage: 'staple', end_on: null });
  p = await ctx.newPage();
  await p.goto(B + '/sku/s002');
  assert.equal(await p.inputValue('#f-brand-in'), 'よそブランド');
  // 選んでいない名前 = 保存を止める
  await p.fill('#f-brand-in', 'まだ選んでいない');
  assert.equal(await p.isDisabled('#save'), true);
  assert.match(await p.textContent('#needreason-t'), /ブランドは一覧から選ぶか/);
  // 空にする・区分なし = 外す
  await p.fill('#f-brand-in', '');
  await p.selectOption('#f-life_stage', '');
  assert.equal(await p.isDisabled('#save'), false);
  await p.click('#save');
  for (let i = 0; i < 50 && (await lifeOf('s002')).brand !== null; i++) await p.waitForTimeout(200);
  assert.deepEqual(await lifeOf('s002'), { brand: null, stage: null, end_on: null });
});

await ta('③b 商品の画面: 「自社ブランドにする」= 名簿の印 (確かめの窓・保存の箱とは別)・発売日は読むだけ', async (p) => {
  const dialogs = [];
  p.on('dialog', async (d) => { dialogs.push(d.message()); await d.accept(); });
  await p.goto(B + '/sku/s003');
  await pickBrand(p, 'よそ');
  assert.equal(await p.isVisible('#f-brand-owntag'), false);
  assert.equal(await p.textContent('#f-brand-own'), '自社ブランドにする');
  await p.click('#f-brand-own');
  await p.waitForFunction(() => /自社ブランドにしました/.test(document.getElementById('f-brand-msg').textContent));
  assert.match(dialogs[0], /「よそブランド」を自社ブランドにしますか/);
  assert.equal((await one('select own_brand from core.brands where brand_id = $1::bigint', [YOSO.id])).own_brand, true);
  assert.equal(await p.isVisible('#f-brand-owntag'), true);
  assert.equal(await p.textContent('#f-brand-own'), '自社ブランドから外す');
  // 印を変えても商品の未保存は ブランドの付け替えだけ
  assert.deepEqual(await p.evaluate(() => window.MasterEdit.dirty().items), ['ブランド: なし → よそブランド']);
  await p.click('#f-brand-own');
  await p.waitForFunction(() => /自社ブランドから外しました/.test(document.getElementById('f-brand-msg').textContent));
  assert.equal((await one('select own_brand from core.brands where brand_id = $1::bigint', [YOSO.id])).own_brand, false);
  assert.equal(await p.locator('#first-sold input, #first-sold select').count(), 0, '発売日に欄がある');
  assert.match(await p.textContent('#first-sold-src'), /自動 \(人は入れません\)/);
});

await ta('④ スマホの幅 (390): 横にはみ出さない・押す所は 44px・候補は画面の中', async (p) => {
  await p.goto(B + '/sku/s001');
  await p.fill('#f-brand-in', 'は');
  const m = await p.evaluate(() => {
    const r = (s) => document.querySelector(s).getBoundingClientRect();
    return { sw: document.documentElement.scrollWidth, w: window.innerWidth, inH: r('#f-brand-in').height, stH: r('#f-life_stage').height, enH: r('#f-end_planned_on').height, list: r('#f-brand-list') };
  });
  assert.ok(m.sw <= m.w + 1, `横にはみ出す (${m.sw} > ${m.w})`);
  assert.ok(m.inH >= 44 && m.stH >= 44 && m.enH >= 44, JSON.stringify(m));
  assert.ok(m.list.right <= m.w + 1 && m.list.left >= -1, JSON.stringify(m.list));
  await p.goto(B + '/new?kind=single');
  assert.ok((await p.evaluate(() => document.documentElement.scrollWidth)) <= 391, '新商品の画面が横にはみ出す');
  // 一覧の「もっと絞る」(商品の段階の選ぶ欄) も画面の中
  await p.goto(B + '/');
  await p.evaluate(() => { document.querySelectorAll('details').forEach((d) => { d.open = true; }); });
  const over = await p.evaluate(() => [...document.querySelectorAll('.filters-more label, #f-life')].map((e) => e.getBoundingClientRect().right).filter((r) => r > window.innerWidth + 1));
  assert.deepEqual(over, [], '一覧の絞り込みがはみ出す');
}, { width: 390, height: 844 });

console.log(`\n${passed} ok${process.exitCode ? ' (NG あり)' : ''}`);
await browser.close();
server.close();
await pg.close();
process.exit(process.exitCode || 0);
