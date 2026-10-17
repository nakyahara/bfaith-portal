/**
 * test-master-variation-oldnames-ui.mjs — まとまりの登録の画面の 2 つの改善 (2026-10-17) を、本物の router で描いたページで本当に動かす (Playwright・Chrome)
 *
 *   1. 前からある子の選択肢の名前を、子の商品名から仮に入れる (中原さん「既存のバリエーションに新規追加をする際に既存の過去のバリエーションの名前も
 *      すべて入力する必要があり大変だった」)。NE で作ったまとまり (軸の記録なし) の最初の 1 回だけ出る「前からある子 N 件の選択肢を確かめる」の表:
 *      [N1] 読み取りの決まり (1 軸・2 軸・迷う = 空)
 *      [N2] 2 軸 (【レッド】【S】・【レッド / M】・【ネイビー・S】・かっこ無し = 空)・人が直した名前は残る・重なりの確かめはそのまま・「確かめた」まで保存できない → 保存 = 選択肢の記録
 *      [N3] 1 軸の迷う名前 (同じ名前の子が 2 つ = 両方空・まとまりの名前で始まらない = 空・終わりの【JAN】は外す)
 *   2. まとまりの登録にも参考 URL (単品の登録と同じ欄・動き):
 *      [N4] 値か入力なし (あと N つ・入力なしで値を消す・値を入れると外れる)・形の確かめ・行を足す / 外す・送る本文・まとまりの共通の欄・product-hub のまとまりのカード
 *      [N5] 20 個まで (足すボタン)・API の確かめ (形 = 400 card.reference_urls・印の無い本文 = 400 blank_unmarked)
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-master-variation-oldnames-ui.mjs   (1 つだけ = VG7_UI_ONLY=N2)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = まとまりの画面の試験を飛ばす'); process.exit(0); }
let chromium; let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG まとまりの画面の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / 飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'vg7-oldnames-ui-'));
process.env.DATA_DIR = DATA_TMP;

const { setupVariationDb, skuOf } = await import('./fixtures/master-variation-db.mjs');
const V = await import('../lib/master-variation.mjs');
// NE で作ったまとまり (軸の記録なし) を 2 つ足す: kz300 = 2 軸の形の子の名前 / kz400 = 1 軸の迷う名前
const rep = (code, name, g) => skuOf(code, name, { representativeCode: g, representativeState: 'value' });
const EXTRA = [
  rep('kz300-RD-S', 'キッズ浴衣【レッド】【S】', 'kz300'), rep('kz300-RD-M', 'キッズ浴衣【レッド / M】', 'kz300'),
  rep('kz300-NV-S', 'キッズ浴衣【ネイビー・S】', 'kz300'), rep('kz300-NV-M', 'キッズ浴衣 ネイビー M', 'kz300'),
  rep('kz400-BR', 'ポーチ【ブラウン】', 'kz400'), rep('kz400-BE', 'ポーチ【ブラウン】', 'kz400'),
  rep('kz400-GY', 'ポーチ【グレー】【4901234567894】', 'kz400'), rep('kz400-NV', '旧ポーチ【ネイビー】', 'kz400'),
  rep('kz500-BR-S', 'バッグ【S / BR】', 'kz500'),   // 横と縦が入れ替わっていそう (縦の名前 = 横の文字)
];
const T = await setupVariationDb({ extraSkus: EXTRA, extraGroups: [
  { code: 'kz300', name: 'キッズ浴衣', childCodes: ['kz300-RD-S', 'kz300-RD-M', 'kz300-NV-S', 'kz300-NV-M'], status: 'active' },
  { code: 'kz400', name: 'ポーチ', childCodes: ['kz400-BR', 'kz400-BE', 'kz400-GY', 'kz400-NV'], status: 'active' },
  { code: 'kz500', name: 'バッグ', childCodes: ['kz500-BR-S'], status: 'active' }] });
const { db, q, one } = T;
const RATES = new Map([['A1', { method: 'ネコポス', cost: 280 }], ['B2', { method: '宅急便コンパクト', cost: 520 }], ['C3', { method: '宅急便 60', cost: 780 }]]);
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

// ── 本物の router (試験の PGlite につなぐ・本番の DB の URL ではない) ──
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/vg7';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/vg7';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const { default: router, __setPgClientFactory, __setShippingRatesProvider, __setPhDraftLookup } = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await T.pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => T.pg.query(t, p), end: async () => { await T.pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => RATES);
__setPhDraftLookup(async () => null);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] }; next(); });
app.use('/apps/master-edit', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;

let passed = 0;
async function ta(name, fn, viewport = { width: 1440, height: 1000 }) {
  if (process.env.VG7_UI_ONLY && !name.includes(process.env.VG7_UI_ONLY)) return;
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
const settle = async (p) => { await p.waitForTimeout(50); await p.waitForFunction(() => document.querySelector('#f').getAttribute('data-check') === 'done', null, { timeout: 15000 }); await p.waitForTimeout(100); };
const info = (p) => p.evaluate(() => ({
  save: document.querySelector('#save').disabled ? 'disabled' : 'enabled',
  issues: Array.from(document.querySelectorAll('#checks li button')).map((li) => li.innerText.replace(/\s+/g, ' ').replace('ここへ →', '').trim()),
  remain: (document.querySelector('#remain') || { innerText: '' }).innerText.replace(/\s+/g, ' '),
}));
const fill = (p, sel, v) => p.fill(sel, v);
async function fillCommon(p, { price = '2980', cost = '1100', tax = '0.1', sales = '3', sup = '0001', months = '2', expiry = '0', ship = 'B2' } = {}) {
  await fill(p, '#c-price', price); await fill(p, '#c-cost', cost);
  await p.click(`#c-tax button[data-v="${tax}"]`); await p.click(`#c-sales button[data-v="${sales}"]`);
  await p.selectOption('#f-primary_supplier', sup); await p.selectOption('#c-ship', ship);
  await fill(p, '#c-months', months); await p.click(`#c-expiry button[data-v="${expiry}"]`);
}
/** 値の無い必須でない項目を全部「入力なし」にする (keep = 入力なしにしない項目) */
const noneRest = (p, keep = []) => p.evaluate((k) => {
  const lz = document.getElementById('lz-blank-rest');
  if (lz && !lz.checked && !lz.closest('[hidden]')) lz.click();
  for (const id of ['po-cond-mode', 'po-mat-mode']) {
    const sg = document.getElementById(id);
    if (sg && !sg.getAttribute('data-value')) sg.querySelector('button[data-v="none"]').click();
  }
  document.querySelectorAll('input[type="checkbox"][data-none]').forEach((c) => {
    if (c.disabled || c.checked || k.includes(c.getAttribute('data-none')) || window.MasterEdit.none.has(c.getAttribute('data-none'))) return;
    c.click();
  });
}, keep);
async function pickGroup(p, q0, code) {
  await p.click('[data-mode="add"]');
  await fill(p, '#g-q', q0);
  await p.waitForSelector(`#g-results .gitem:has(.code:text-is("${code}"))`);
  await p.click(`#g-results .gitem:has(.code:text-is("${code}"))`);
  await p.waitForSelector('#g-picked .picked');
}
/** 前からある子の表の値 { 子のコード: [横の名前, 縦の名前] } と「仮」の印の数 */
const oldNames = (p) => p.evaluate(() => {
  const out = {};
  document.querySelectorAll('#old-box input[data-of="hname"]').forEach((e) => { out[e.dataset.oc] = [e.value]; });
  document.querySelectorAll('#old-box input[data-of="vname"]').forEach((e) => { out[e.dataset.oc].push(e.value); });
  return out;
});
const guessTags = (p) => p.locator('#old-box .gtag:not([hidden])').count();
/** 確かめの窓を開いて保存 (本文を返す) */
async function saveAndBody(p) {
  await settle(p);
  assert.equal(await p.isDisabled('#save2'), false, JSON.stringify(await info(p)));
  await p.click('#save2');
  await p.waitForSelector('#confirm-bg.on');
  const confirmText = await p.innerText('#confirm-body');
  await p.check('#fix-ok');
  const bodies = [];
  p.on('request', (rq) => { if (/\/api\/new\/variation$/.test(rq.url())) bodies.push(JSON.parse(rq.postData())); });
  await p.click('#confirm-yes');
  await p.waitForSelector('#result .result', { timeout: 60000 });
  return { body: bodies[0], result: (await p.innerText('#result')).replace(/\s+/g, ' '), confirmText };
}

console.log('前からある子の名前を、子の商品名から仮に入れる');
await ta('[N1] 読み取りの決まり: 1 軸 =【…】1 つ / かっこ無しの 1 語・2 軸 =【…】【…】/【a / b】/【a・b】/ かっこ無しの「a / b」・終わりの【JAN】は外す・迷う = 空', async (p) => {
  await p.goto(B + '/new?kind=variation');
  const g = (base, name, ac) => p.evaluate(([b0, n0, a0]) => window.MasterEdit.__guessOldNames(b0, n0, a0), [base, name, ac]);
  const W = 'ウールストール';
  const cases = [
    [W + '【ブラウン】', 1, ['ブラウン', '']], [W + '【ブラウン】【4901234567894】', 1, ['ブラウン', '']], [W + ' ブラウン', 1, ['ブラウン', '']], [W + '（ブラウン）', 1, ['ブラウン', '']],
    [W + 'ブラウン', 1, ['', '']], [W + ' 大判 ブラウン', 1, ['', '']], [W + '【ブラウン】【M】', 1, ['', '']], [W + ' 大判【ブラウン】', 1, ['', '']], ['ストール【ブラウン】', 1, ['', '']],
    [W + '【】', 1, ['', '']], [W, 1, ['', '']], [W + '【ブラウン', 1, ['', '']], [W + '【4901234567894】', 1, ['', '']],
    [W + '【ブラウン】【M】', 2, ['ブラウン', 'M']], [W + '【ブラウン / M】', 2, ['ブラウン', 'M']], [W + '【ブラウン・M】', 2, ['ブラウン', 'M']], [W + '【ブラウン×M】', 2, ['ブラウン', 'M']],
    [W + ' ブラウン / M', 2, ['ブラウン', 'M']], [W + '【ブラウン】【M】【4901234567894】', 2, ['ブラウン', 'M']], [W + '【ネイビー・ホワイト / M】', 2, ['ネイビー・ホワイト', 'M']],
    [W + '【ブラウン】', 2, ['', '']], [W + '【ライト・ブラウン・M】', 2, ['', '']], [W + '【ブラウン / M / 限定】', 2, ['', '']], [W + ' ブラウン M', 2, ['', '']], [W + '【ブラウン】【M】【限定】', 2, ['', '']],
    [W + '【ブラウン / 】', 2, ['', '']],
  ];
  for (const [name, ac, want] of cases) assert.deepEqual(await g(W, name, ac), want, `${name} (${ac} 軸)`);
  // まとまりの名前と子の名前で全角・半角が違っても読む (NFKC でそろえる)
  assert.deepEqual(await g('ＡＢＣ バッグ', 'ABC バッグ【ブラック】', 1), ['ブラック', '']);
});

await ta('[N2] 2 軸の NE のまとまり (kz300): 名前を仮に入れる (読めない子は空)・「仮」の印・人が直した名前は残る (縦軸を外す / 足すでも)・重なりの確かめはそのまま・「確かめた」まで保存できない → 保存 = 前からある子の選択肢を記録', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'kz3', 'kz300');
  await fill(p, '#ax-0', 'カラー');
  await p.click('#ax-v button[data-v="1"]');
  await fill(p, '#ax-1', 'サイズ');
  await fill(p, '#opt-0', 'ホワイト / -WH');
  await fillCommon(p);
  await settle(p);
  assert.deepEqual(await oldNames(p), { 'kz300-NV-M': ['', ''], 'kz300-NV-S': ['ネイビー', 'S'], 'kz300-RD-M': ['レッド', 'M'], 'kz300-RD-S': ['レッド', 'S'] });
  assert.equal(await guessTags(p), 6, '仮に入れた名前ごとに「仮」');
  assert.equal(await p.locator('#old-box input[data-oc="kz300-NV-M"].guess').count(), 0, '読めない子は空 (仮の印も無い)');
  assert.match(await p.innerText('#old-box .oldbox'), /子の商品名 \(まとまりの名前のあとの【…】\) から名前を仮に入れました/);
  let i = await info(p);
  assert.equal(i.save, 'disabled');
  for (const re of [/^前からある kz300-NV-M: カラーの名前/, /^前からある kz300-NV-M: サイズの名前/, /^前からある子の名前 \(商品名から仮に入れた 3 件\) を見て「確かめた」にチェック/]) {
    assert.ok(i.issues.some((x) => re.test(x)), `${re} ${JSON.stringify(i.issues)}`);
  }
  await fill(p, '#old-box input[data-oc="kz300-NV-M"][data-of="hname"]', 'ネイビー');
  await fill(p, '#old-box input[data-oc="kz300-NV-M"][data-of="vname"]', 'M');
  // 人が直す: RD-S の色を「ワインレッド」= 同じ「-RD」に名前が 2 つ (仮の「レッド」と) = 重なりの確かめ
  await fill(p, '#old-box input[data-oc="kz300-RD-S"][data-of="hname"]', 'ワインレッド');
  await settle(p);
  i = await info(p);
  assert.ok(i.issues.some((x) => /^前からある kz300-RD-S: カラーの「-RD」に名前が 2 つ|^前からある kz300-RD-S: .*「-RD」に名前が 2 つ/.test(x)), JSON.stringify(i.issues));
  assert.equal(await p.locator('#old-box input[data-oc="kz300-RD-S"][data-of="hname"].guess').count(), 0);
  await fill(p, '#old-box input[data-oc="kz300-RD-M"][data-of="hname"]', 'ワインレッド');
  await settle(p);
  i = await info(p);
  assert.ok(!i.issues.some((x) => /名前が 2 つ/.test(x)), JSON.stringify(i.issues));
  assert.ok(i.issues.some((x) => /^前からある子の名前 \(商品名から仮に入れた 3 件\)/.test(x)), '縦の名前はまだ仮 (RD-S・RD-M・NV-S)');
  // 確かめた → 縦軸を外す (かっこ 2 つ / 区切り = 1 軸では読めない = 空・人が直した名前は残る) → 足す = 仮に戻る・確かめたのチェックは出し直し
  await p.check('#old-ok');
  await settle(p);
  assert.ok(!(await info(p)).issues.some((x) => /確かめた/.test(x)));
  await p.click('#ax-v button[data-v="0"]');
  await settle(p);
  assert.deepEqual(await oldNames(p), { 'kz300-NV-M': ['ネイビー'], 'kz300-NV-S': ['ネイビー・S'], 'kz300-RD-M': ['ワインレッド'], 'kz300-RD-S': ['ワインレッド'] }, '1 軸 = かっこ 1 つの中は丸ごと (コードの形が合わない = 確かめで止まる)');
  assert.ok((await info(p)).issues.some((x) => /^前からある kz300-NV-S: まとまりのコード \+ 文字 が この子のコードと合いません/.test(x)));
  await p.click('#ax-v button[data-v="1"]');
  await fill(p, '#ax-1', 'サイズ');
  await settle(p);
  assert.deepEqual(await oldNames(p), { 'kz300-NV-M': ['ネイビー', 'M'], 'kz300-NV-S': ['ネイビー', 'S'], 'kz300-RD-M': ['ワインレッド', 'M'], 'kz300-RD-S': ['ワインレッド', 'S'] });
  assert.equal(await guessTags(p), 4, '人が直した 4 つ (RD の色 2・NV-M の 2) は仮ではない');
  i = await info(p);
  assert.ok(i.issues.some((x) => /^前からある子の名前 \(商品名から仮に入れた 3 件\)/.test(x)), JSON.stringify(i.issues));
  assert.equal(await p.isChecked('#old-ok'), false, '作り直した = もう一度確かめる');
  await p.check('#old-ok');
  await noneRest(p);
  const { body, result, confirmText } = await saveAndBody(p);
  assert.match(confirmText, /前からある子 4 件の選択肢も記録します \(うち名前を商品名から仮に入れた 3 件 = 確かめ済み\)/);
  assert.match(result, /2 件を下書きにしました/);
  assert.deepEqual(body.axes, [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }]);
  const g = await V.readVariationGroup(db, (await one(`select product_id::text as id from core.products where display_code = 'kz300'`)).id);
  assert.deepEqual(g.options.map((o) => [o.axis, o.code, o.name]).sort(), [[1, '-NV', 'ネイビー'], [1, '-RD', 'ワインレッド'], [1, '-WH', 'ホワイト'], [2, '-M', 'M'], [2, '-S', 'S']]);
  // 新しい子の名前 = まとまりの名前【横】【縦】(前からある子の名前の読み取りと同じ形)
  assert.deepEqual((await q(`select code, name from core.skus where code like 'kz300-WH-%' order by code`)).map((r) => [r.code, r.name]), [['kz300-WH-M', 'キッズ浴衣【ホワイト】【M】'], ['kz300-WH-S', 'キッズ浴衣【ホワイト】【S】']]);
  // 2 回目 = 軸の記録がある = 前からある子の表は出ない
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'kz3', 'kz300');
  await settle(p);
  assert.equal(await p.locator('#old-box .oldbox').count(), 0);
});

await ta('[N3b] 横と縦が入れ替わっていそう (kz500「バッグ【S / BR】」・コード -BR-S) = 仮の名前だけ空に戻す・人が打った名前は残す (Codex R1 M2)', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'kz5', 'kz500');
  await fill(p, '#ax-0', 'カラー');
  await settle(p);
  assert.deepEqual(await oldNames(p), { 'kz500-BR-S': ['S / BR'] }, '1 軸 = かっこの中を丸ごと (仮)');
  await fill(p, '#old-box input[data-oc="kz500-BR-S"][data-of="hname"]', 'ブラウン');
  await p.click('#ax-v button[data-v="1"]');
  await fill(p, '#ax-1', 'サイズ');
  await settle(p);
  assert.deepEqual(await oldNames(p), { 'kz500-BR-S': ['ブラウン', ''] }, '縦の仮「BR」= 横の文字 = 空・人が打った「ブラウン」は残る');
  assert.equal(await guessTags(p), 0);
  await p.click('#ax-v button[data-v="0"]');
  await p.click('#ax-v button[data-v="1"]');
  await settle(p);
  assert.deepEqual(await oldNames(p), { 'kz500-BR-S': ['ブラウン', ''] });
});

await ta('[N3] 1 軸の迷う名前 (kz400): 同じ名前の子が 2 つ (違う文字) = 両方空・まとまりの名前で始まらない = 空・終わりの【JAN】は外す・読めた 1 つだけ「仮」', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'kz4', 'kz400');
  await fill(p, '#ax-0', 'カラー');
  await settle(p);
  assert.deepEqual(await oldNames(p), { 'kz400-BE': [''], 'kz400-BR': [''], 'kz400-GY': ['グレー'], 'kz400-NV': [''] });
  assert.equal(await guessTags(p), 1);
  const i = await info(p);
  assert.ok(i.issues.some((x) => /^前からある子の名前 \(商品名から仮に入れた 1 件\)/.test(x)), JSON.stringify(i.issues));
  for (const c of ['kz400-BE', 'kz400-BR', 'kz400-NV']) assert.ok(i.issues.some((x) => x.startsWith(`前からある ${c}: カラーの名前`)), c);
  // 🆕 Codex R2: 直した後に (縦軸を足す / 外すを挟んでも) 元の仮の値へ戻した = 仮のまま・確かめたのチェックも出し直す (ほかの欄で描き直した後でも)
  const GY = '#old-box input[data-oc="kz400-GY"][data-of="hname"]';
  const ask = async () => (await info(p)).issues.some((x) => /^前からある子の名前 \(商品名から仮に入れた 1 件\)/.test(x));
  await fill(p, GY, 'グレー杢');
  await p.click('#ax-v button[data-v="1"]');
  await p.click('#ax-v button[data-v="0"]');
  await settle(p);
  assert.equal(await p.inputValue(GY), 'グレー杢', '人が打った名前は残る');
  assert.equal(await ask(), false);
  await fill(p, '#c-months', '3');   // ほかの欄 = 全部描き直す (仮が 0 件 = チェックは隠れる)
  await settle(p);
  assert.equal(await p.isVisible('#old-ok'), false);
  await fill(p, GY, 'グレー');
  await settle(p);
  assert.equal(await ask(), true, '元の仮の値 = 確かめが要る');
  assert.equal(await guessTags(p), 1);
  assert.equal(await p.isVisible('#old-ok'), true, '確かめたのチェックが出る');
  await p.check('#old-ok');
  await settle(p);
  assert.equal(await ask(), false);
});

console.log('\n参考 URL (まとまりの登録)');
await ta('[N4] 参考 URL: 値か入力なし (あと N つ)・形の確かめ・行を足す / 外す・入力なし = 値を消す / 値を入れると外れる・送る本文・まとまりの共通の欄・product-hub のまとまりのカード', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await fill(p, '#g-code', 'refui'); await fill(p, '#g-name', '参考の試験');
  await fill(p, '#ax-0', 'カラー'); await fill(p, '#opt-0', 'ホワイト / -WH\nブラック / -BK');
  await fillCommon(p);
  await noneRest(p, ['card.reference_urls']);
  await settle(p);
  let i = await info(p);
  assert.deepEqual(i.issues, ['参考 URL (値か「入力なし」)'], '残りは参考 URL だけ');
  assert.match(i.remain, /あと\s*1\s*つ|1/);
  // 形の確かめ (サーバーと同じ決まり)
  await fill(p, '#ref-rows .ref-url', 'example.com/a');
  await settle(p);
  i = await info(p);
  assert.ok(i.issues.some((x) => /^参考 URL 1 つ目: http:\/\/ か https:\/\/ で始まる URL を入れてください/.test(x)), JSON.stringify(i.issues));
  assert.equal(i.save, 'disabled');
  await fill(p, '#ref-rows .ref-url', 'https://maker.example/a');
  await settle(p);
  assert.deepEqual((await info(p)).issues, []);
  // 行を足す (番号・空の行は送らない) / 外す (番号を振り直す)
  await p.click('#ref-add');
  await p.click('#ref-add');
  assert.equal(await p.locator('#ref-rows .urlrow').count(), 3);
  await fill(p, '#ref-rows .urlrow:nth-child(2) .ref-url', 'https://maker.example/b');
  await fill(p, '#ref-rows .urlrow:nth-child(3) .ref-url', 'ftp://bad.example');
  await settle(p);
  assert.ok((await info(p)).issues.some((x) => /^参考 URL 3 つ目: /.test(x)));
  await p.click('#ref-rows .urlrow:nth-child(3) [data-ref-del]');
  await p.click('#ref-rows .urlrow:nth-child(1) [data-ref-del]');
  assert.deepEqual(await p.$$eval('#ref-rows .urlrow', (rs) => rs.map((r) => [r.querySelector('.idx').textContent, r.querySelector('.ref-url').value, r.querySelector('.ref-url').getAttribute('aria-label')])),
    [['1', 'https://maker.example/b', '参考 URL 1']]);
  await settle(p);
  assert.deepEqual((await info(p)).issues, []);
  // 入力なし = 値を消す・数えない → 値を入れると外れる
  await p.click('[data-row="card.reference_urls"] .nonechk');
  assert.equal(await p.inputValue('#ref-rows .ref-url'), '', '入力なしを押すと値を消す');
  await settle(p);
  assert.deepEqual((await info(p)).issues, [], '入力なし = 数えない');
  await fill(p, '#ref-rows .ref-url', 'https://maker.example/b');
  assert.equal(await p.isChecked('[data-none="card.reference_urls"]'), false, '値を入れたら入力なしが外れる');
  await p.click('#ref-add');
  await fill(p, '#ref-rows .urlrow:nth-child(2) .ref-url', 'https://maker.example/c');
  await p.click('#ref-add');   // 空の行 = 送らない
  const { body, result } = await saveAndBody(p);
  assert.match(result, /2 件を下書きにしました/);
  assert.deepEqual(body.card.reference_urls, ['https://maker.example/b', 'https://maker.example/c']);
  assert.ok(!body.blanks.includes('card.reference_urls'));
  const gid = (await one(`select group_product_id::text as id from ops.variation_group_codes where code_norm = 'refui'`)).id;
  assert.deepEqual((await one('select common from ops.variation_group_revisions where group_product_id = $1', [gid])).common.reference_urls, ['https://maker.example/b', 'https://maker.example/c']);
  assert.deepEqual((await q('select payload from ops.product_hub_outbox where group_product_id = $1', [gid]))[0].payload.common.reference_urls, ['https://maker.example/b', 'https://maker.example/c']);
  // 保存の直後に作るまとまりのカード (product-hub) にも入る
  const ph = (await import('../apps/product-hub/db.js')).getDB();
  const d = ph.prepare('SELECT id FROM product_drafts WHERE cdb_group_product_id = ?').get(Number(gid));
  assert.ok(d, 'まとまりのカードがある');
  assert.deepEqual(ph.prepare('SELECT url FROM draft_reference_urls WHERE draft_id = ? ORDER BY sort, id').all(d.id).map((x) => x.url), ['https://maker.example/b', 'https://maker.example/c']);
  // 保存の後は行を足せない
  await p.click('#ref-add', { force: true }).catch(() => {});
  assert.equal(await p.locator('#ref-rows .urlrow').count(), 3);
});

await ta('[N5] 参考 URL は 20 個まで (足すボタン)・API: 形の違う URL = 400 card.reference_urls / 入力なしの印も値も無い = 400 blank_unmarked (field = card.reference_urls)・何も保存しない', async (p) => {
  await p.goto(B + '/new?kind=variation');
  for (let k = 1; k < 20; k++) await p.click('#ref-add');
  assert.equal(await p.locator('#ref-rows .urlrow').count(), 20);
  await p.click('#ref-add');
  assert.equal(await p.locator('#ref-rows .urlrow').count(), 20, '21 行目は足さない');
  assert.match(await p.innerText('#toast'), /参考 URL は 20 個までです/);
  assert.match(await p.innerText('[data-row="card.reference_urls"]'), /20 個まで/);
  const base = { group: { mode: 'new', code: 'refapi', name: '参考 API' }, axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-WH', name: 'ホワイト' }],
    children: [{ code: 'refapi-WH', choices: { 1: '-WH' }, name: '参考 API【ホワイト】', jan: '', lz_barcode: { kind: 'lz_assign' } }],
    values: { standard_price: '1', cost: { jpy: '1' }, tax_rate: '0.1', sales_class: '1', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'A1' },
    card: { official_url: 'https://a.example', amazon_url: 'https://www.amazon.co.jp/dp/B000000000' } };
  const post = (body) => p.evaluate(async ([b0, x]) => { const r = await fetch(b0 + '/api/new/variation', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ request_id: crypto.randomUUID(), ...x }) }); return [r.status, await r.json()]; }, [B, body]);
  const ORDER = ['order_settings.order_lot', 'order_settings.condition', 'order_settings.material', 'order_settings.case_lot', 'order_settings.case_group', 'order_settings.capacity_per_unit'];
  let r = await post({ ...base, card: { ...base.card, reference_urls: ['ftp://x.example'] }, blanks: [...ORDER, 'children.jan'] });
  assert.deepEqual([r[0], r[1].field], [400, 'card.reference_urls'], JSON.stringify(r));
  assert.match(r[1].error, /参考 URL 1 つ目は http:\/\/ か https:\/\/ で始まる URL/);
  r = await post({ ...base, card: { ...base.card, reference_urls: [] }, blanks: [...ORDER, 'children.jan'] });
  assert.deepEqual([r[0], r[1].reason, r[1].field, r[1].screen_outdated], [400, 'blank_unmarked', 'card.reference_urls', undefined], JSON.stringify(r));
  // 前の画面 (card に reference_urls が無い) の新しい登録 = 400 (画面を開き直す)
  const legacy = { ...base, blanks: [...ORDER, 'children.jan'] };
  r = await post(legacy);
  assert.deepEqual([r[0], r[1].reason, r[1].field, r[1].screen_outdated], [400, 'blank_unmarked', 'card.reference_urls', true], JSON.stringify(r));
  assert.match(r[1].error, /前の画面には参考 URL の欄がありません/);
  assert.equal((await q(`select 1 from core.skus where code = 'refapi-WH'`)).length, 0, '何も保存しない');
  // 🆕 #Codex R1 M3: 前の画面で保存できた登録 (デプロイの前) の押し直し = 前の答え (参考 URL で断らない)
  const BLK = await import('../lib/master-register-blanks.mjs');
  const rid = crypto.randomUUID();
  const old = BLK.prepareRegisterBody('variation', { ...legacy, request_id: rid }, { skip: ['card.reference_urls'] });
  assert.equal(old.ok, true, JSON.stringify(old.json));
  const first = await T.asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: rid, reason: old.body.reason, group: legacy.group, axes: legacy.axes, options: legacy.options,
    children: legacy.children, values: legacy.values, card: legacy.card, childReasons: old.childReasons }, { open: true, shippingRates: RATES, requireLz: true, orderOutbox: true }));
  assert.equal(first.ok, true);
  r = await post({ ...legacy, request_id: rid });
  assert.deepEqual([r[0], r[1].ok, r[1].replayed], [200, true, true], JSON.stringify(r).slice(0, 400));
  assert.equal((await q(`select 1 from core.skus where code = 'refapi-WH'`)).length, 1, '1 回だけ');
});

console.log(`\n${passed} ok`);
await browser.close();
server.close();
