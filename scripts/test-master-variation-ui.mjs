/**
 * test-master-variation-ui.mjs — 新商品の登録「色違い・サイズ違いのまとまり」の画面 (PR-7) を、本物の router で描いたページで本当に動かす (Playwright・Chromium)
 *
 * 見た目と動きの正本 = 見本 v2 (色違いの登録_見本_20261009.html)。見本の 8 つの場面を本物の DB (PGlite・products.parent も company) で同じ操作をして、保存まで流す:
 *   ① 新しいまとまり (色 4 × サイズ 4・エンジ × 90 を作らない・JAN・この商品だけの売価・手で直した名前・JAN をまとめて貼る・作らないを見る・確かめの窓 = 直せない 3 つにチェックで保存)
 *   ② 今あるまとまりに新色 (売価が 2 種類 = 写した元の子・選ぶまで保存できない)
 *   ③ 前回作らなかった組み合わせ (最初は「作らない」・押したら作る)
 *   ④ NE で作ったまとまりに足す (前からある子の文字 = コードの末尾から仮に・色の名前 = 子の商品名から仮に (🆕 2026-10-17・「確かめた」まで保存できない))
 *   ⑤ 40 色 × 3 サイズ = 120 (上限ちょうど・保存できる)
 *   ⑥ 上限を超える (45 色) = 保存を止め、色そのものを分ける → 保存 → 「残りを足す」→ 同じまとまりに続けて保存 / NE 登録の CSV = 1 回目の CSV を先に作ってから残りを足す・未処理の回が重なると 1 つのファイル
 *   ⑦ 誤り (「-」の付け忘れ = 誤り + 「- を付ける」・重なり・JAN の形とほかの商品の JAN・まとまりのコードがもうある = 「このまとまりに足す」)
 *   ⑧ 空から / 切替の前 (products.parent が load = まとまりの札と欄が出ない・単品とセットは今までどおり)
 *   + スマホの幅 (390・360): 横にはみ出さない・押す所は 44px 以上・種類は縦・手順の帯「4/7 選択肢」・キーボードの間は下の帯を小さく・30 件をこえたら「パソコンがおすすめ」
 *   + 商品の画面の代表 (親) = 見るだけ (保存の欄が無い)
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 のときだけ
 * 使い方: node scripts/test-master-variation-ui.mjs   (1 つだけ = VG7_UI_ONLY=①)
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
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'vg7-ui-'));
process.env.DATA_DIR = DATA_TMP;

const { setupVariationDb, jan13 } = await import('./fixtures/master-variation-db.mjs');
const V = await import('../lib/master-variation.mjs');
const T = await setupVariationDb();
const { pg, db, q, one, asEditor } = T;
const RATES = new Map([['A1', { method: 'ネコポス', cost: 280 }], ['B2', { method: '宅急便コンパクト', cost: 520 }], ['C3', { method: '宅急便 60', cost: 780 }]]);
const VALUES = Object.freeze({ standard_price: '3980', cost: { jpy: '1650' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0034', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0', shipping_code: 'B2' });
const reg = (input) => asEditor(() => V.registerVariationBatch(db, { actor: 'naka@test', requestId: crypto.randomUUID(), ...input }, { open: true, shippingRates: RATES }));
// 見本と同じ今あるまとまり: blanket-fl (1 軸・グレーだけ売価 4,280) / hakama-kids (2 軸・ブラック × 110 は前回作らなかった)
const BL = await reg({ group: { mode: 'new', code: 'blanket-fl', name: 'フランネル ブランケット' }, axes: [{ axis: 1, name: 'カラー' }],
  options: [{ axis: 1, code: '-BR', name: 'ブラウン' }, { axis: 1, code: '-BE', name: 'ベージュ' }, { axis: 1, code: '-GY', name: 'グレー' }],
  children: [{ code: 'blanket-fl-BR', choices: { 1: '-BR' }, name: 'フランネル ブランケット【ブラウン】' }, { code: 'blanket-fl-BE', choices: { 1: '-BE' }, name: 'フランネル ブランケット【ベージュ】' },
    { code: 'blanket-fl-GY', choices: { 1: '-GY' }, name: 'フランネル ブランケット【グレー】', price: '4280' }], values: VALUES });
const HK = await reg({ group: { mode: 'new', code: 'hakama-kids', name: '子ども袴 3点セット' }, axes: [{ axis: 1, name: 'カラー' }, { axis: 2, name: 'サイズ' }],
  options: [{ axis: 1, code: '-WH', name: 'ホワイト' }, { axis: 1, code: '-BK', name: 'ブラック' }, { axis: 2, code: '-100', name: '100cm' }, { axis: 2, code: '-110', name: '110cm' }],
  children: [{ code: 'hakama-kids-WH-100', choices: { 1: '-WH', 2: '-100' }, name: '子ども袴 3点セット【ホワイト】【100cm】' }, { code: 'hakama-kids-WH-110', choices: { 1: '-WH', 2: '-110' }, name: '子ども袴 3点セット【ホワイト】【110cm】' },
    { code: 'hakama-kids-BK-100', choices: { 1: '-BK', 2: '-100' }, name: '子ども袴 3点セット【ブラック】【100cm】' }], values: { ...VALUES, standard_price: '14800' } });
// ほかの商品の JAN (⑦ の重なり)
const TAKEN_JAN = jan13('490123456789');
{
  const s3 = (await one(`select sku_id::text as id from core.skus where code = 's003'`)).id;
  await asEditor(async () => { await pg.query('begin'); try { await pg.query('select ops.edit_sku_jan($1::uuid, $2, $3, $4::jsonb, $5::bigint, $6::jsonb, $7::jsonb)', [crypto.randomUUID(), 'naka@test', null, T.OWN, s3, '[]', JSON.stringify([TAKEN_JAN])]); await pg.query('commit'); } catch (e) { await pg.query('rollback'); throw e; } });
}
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();

// ── 本物の router ──
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/vg7';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/vg7';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_DECISION_APPROVERS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
const { default: router, __setPgClientFactory, __setShippingRatesProvider, __setPhDraftLookup } = await import('../apps/master-edit/router.mjs');
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setShippingRatesProvider(async () => RATES);
let PH_DRAFTS = new Map([['kimono-obi', 31]]);
__setPhDraftLookup(async (norm) => PH_DRAFTS.get(norm) ?? null);
const app = express();
app.set('view engine', 'ejs');
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: ['master-edit', 'purchase-orders'] }; next(); });
app.use('/apps/master-edit', router);
app.get('/', (req, res) => res.send('<p>portal</p>'));
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;
const SHOT = process.env.VG7_SHOT_DIR || null;   // 目で見る (見本と並べる) ときだけ

let passed = 0;
async function ta(name, fn, viewport = { width: 1440, height: 1000 }, scale = 1) {
  if (process.env.VG7_UI_ONLY && !name.includes(process.env.VG7_UI_ONLY)) return;
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: scale, locale: 'ja-JP' });
  const p = await ctx.newPage();
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  p.on('console', (m) => { if (m.type() === 'error' && !/favicon|Failed to load resource/.test(m.text())) errors.push(`console: ${m.text()}`); });
  try {
    await fn(p, ctx);
    assert.deepEqual(errors, [], '画面の JS の例外');
    passed++; console.log(`  ok  ${name}`);
  } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; if (SHOT) await p.screenshot({ path: path.join(SHOT, `vg7-ng-${passed}.png`), fullPage: true }).catch(() => {}); }
  await ctx.close();
}
const shot = async (p, name, sel = null, off = 70) => {
  if (!SHOT) return;
  if (sel) await p.evaluate(([s, o]) => { const el = document.querySelector(s); window.scrollTo(0, el.getBoundingClientRect().top + window.scrollY - o); }, [sel, off]);
  await p.waitForTimeout(300);
  await p.screenshot({ path: path.join(SHOT, `vg7-${name}.png`) });
};
/** 打った値の DB の確かめ (少し待ってからまとめて) が終わるまで待つ */
const settle = async (p) => { await p.waitForTimeout(50); await p.waitForFunction(() => document.querySelector('#f').getAttribute('data-check') === 'done', null, { timeout: 15000 }); await p.waitForTimeout(100); };
const info = (p) => p.evaluate(() => ({
  remain: document.querySelector('#remain').innerText.replace(/\s+/g, ' '),
  rows: document.querySelectorAll('#kids-box .krow').length,
  save: document.querySelector('#save').disabled ? 'disabled' : 'enabled',
  issues: Array.from(document.querySelectorAll('#checks li button')).map((li) => li.innerText.replace(/\s+/g, ' ').replace('ここへ →', '').trim()),
  overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
}));
const fill = async (p, sel, v) => { await p.fill(sel, v); };
async function fillCommon(p, { price = '12800', cost = '5200', tax = '0.1', sales = '1', sup = '0001', months = '3', expiry = '0', ship = 'C3' } = {}) {
  await fill(p, '#c-price', price); await fill(p, '#c-cost', cost);
  await p.click(`#c-tax button[data-v="${tax}"]`); await p.click(`#c-sales button[data-v="${sales}"]`);
  await p.selectOption('#f-primary_supplier', sup); await p.selectOption('#c-ship', ship);
  await fill(p, '#c-months', months); await p.click(`#c-expiry button[data-v="${expiry}"]`);
}
/**
 * 🆕 2026-10-13 (入力なし): 値の無い必須でない項目を全部「入力なし」にする (画面でチェックを押すのと同じ・発注条件グループ / 原料グループは「なし」)。
 *   ほかの決まりを確かめる試験で、保存の前に使う
 */
const noneRest = (p) => p.evaluate(() => {
  let n = 0;
  // 🆕 0080: ロジザードのバーコードの無い子 = 「空欄で送る」(入力なしと同じ形の印)
  const lz = document.getElementById('lz-blank-rest');
  if (lz && !lz.checked && !lz.closest('[hidden]')) lz.click();
  for (const id of ['po-cond-mode', 'po-mat-mode']) {
    const sg = document.getElementById(id);
    if (sg && !sg.getAttribute('data-value')) { sg.querySelector('button[data-v="none"]').click(); n++; }
  }
  document.querySelectorAll('input[type="checkbox"][data-none]').forEach((c) => {
    if (c.disabled || c.checked || window.MasterEdit.none.has(c.getAttribute('data-none'))) return;
    c.click(); n++;
  });
  return n;
});
/** 保存 = 確かめの窓 (直せない 3 つにチェック) → 保存 → 結果。残りの必須でない項目は「入力なし」にしてから */
async function saveViaConfirm(p) {
  await settle(p);
  await noneRest(p);
  await settle(p);
  assert.equal(await p.isDisabled('#save2'), false, JSON.stringify(await info(p)));
  await p.click('#save2');
  await p.waitForSelector('#confirm-bg.on');
  assert.equal(await p.isDisabled('#confirm-yes'), true, '直せない 3 つにチェックするまで押せない');
  assert.match(await p.textContent('#confirm-fixed'), /保存の後は直せない 3 つ/);
  await p.check('#fix-ok');
  await p.click('#confirm-yes');
  await p.waitForSelector('#result .result', { timeout: 60000 });
  return (await p.innerText('#result')).replace(/\s+/g, ' ');
}
const kidsOf = async (code) => (await q(`select s.code from core.skus s join core.products p on p.product_id = s.product_id
   where p.parent_product_id = (select group_product_id from ops.variation_group_codes where code_norm = lower($1)) order by s.code`, [code])).map((r) => r.code);
const colorLines = (n) => [['ホワイト', 'WH'], ['ブラック', 'BK'], ['ネイビー', 'NV'], ['グレー', 'GY'], ['ベージュ', 'BE'], ['ブラウン', 'BR'], ['レッド', 'RD'], ['ピンク', 'PK'], ['オレンジ', 'OR'], ['イエロー', 'YE'],
  ['グリーン', 'GR'], ['ブルー', 'BL'], ['パープル', 'PU'], ['カーキ', 'KH'], ['アイボリー', 'IV'], ['チャコール', 'CH'], ['ワイン', 'WN'], ['マスタード', 'MS'], ['ミント', 'MT'], ['サックス', 'SX'],
  ['ラベンダー', 'LV'], ['モカ', 'MO'], ['テラコッタ', 'TC'], ['オリーブ', 'OL'], ['ボルドー', 'BD'], ['スモーキーピンク', 'SP'], ['ライトグレー', 'LG'], ['ダークグレー', 'DG'], ['キャメル', 'CM'], ['ターコイズ', 'TQ'],
  ['コーラル', 'CO'], ['ローズ', 'RS'], ['セージ', 'SG'], ['インディゴ', 'IN'], ['シルバー', 'SV'], ['ゴールド', 'GD'], ['クリーム', 'CR'], ['ミルクティー', 'MK'], ['ピスタチオ', 'PT'], ['スカイ', 'SK'],
  ['ラスト', 'RU'], ['ネイビーストライプ', 'NS'], ['レッドチェック', 'RC'], ['ドット', 'DT'], ['ボーダー', 'BO']].slice(0, n).map(([a, b]) => `${a} / -${b}`).join('\n');
async function newGroup(p, { code, name, h, v = null, hl, vl = '' }) {
  await fill(p, '#g-code', code); await fill(p, '#g-name', name);
  await fill(p, '#ax-0', h);
  if (v) { await p.click('#ax-v button[data-v="1"]'); await fill(p, '#ax-1', v); }
  await fill(p, '#opt-0', hl);
  if (v) await fill(p, '#opt-1', vl);
}
async function pickGroup(p, q0, code) {
  await p.click('[data-mode="add"]');
  await fill(p, '#g-q', q0);
  await p.waitForSelector(`#g-results .gitem:has(.code:text-is("${code}"))`);
  await p.click(`#g-results .gitem:has(.code:text-is("${code}"))`);
  await p.waitForSelector('#g-picked .picked');
}

/** 読むだけのまとまりの API 3 つの [状態, reason] (探す・1 つを読む・確かめる) */
const readApis = (p) => p.evaluate(async ([base, gid]) => {
  const one = async (r) => { const j = await r.json().catch(() => ({})); return [r.status, j.reason ?? null]; };
  return [await one(await fetch(base + '/api/variation/groups?q=b')), await one(await fetch(base + '/api/variation/groups/' + gid)),
    await one(await fetch(base + '/api/variation/check', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ group_code: 'x', codes: [], jans: [] }) }))];
}, [B, BL.group_product_id]);

console.log('見本の 8 つの場面 (1440 幅)');
await ta('[①] 新しいまとまり (色 4 × サイズ 4): コードのでき方・エンジ × 90 を作らない (マス目)・JAN で名前が変わる・この商品だけの売価・手で直した名前・JAN をまとめて貼る・作らないを見る・確かめの窓 → 15 件が下書き', async (p) => {
  await p.goto(B + '/new?kind=variation');
  assert.equal(await p.locator('.kindcards.three .kindcard').count(), 3);
  assert.equal(await p.getAttribute('.kindcard[data-kind="variation"]', 'aria-current'), 'page');
  await newGroup(p, { code: 'hakama', name: '子ども袴 2点セット', h: 'カラー', v: 'サイズ', hl: 'ホワイト / -WH\nブラック / -BK\nネイビー / -NV\nエンジ / -EN', vl: '90cm / -90\n100cm / -100\n110cm / -110\n120cm / -120' });
  await fillCommon(p);
  await settle(p);
  assert.match(await p.innerText('#anat'), /hakama[\s\S]*-WH[\s\S]*-90[\s\S]*hakama-WH-90/);
  assert.match(await p.innerText('#g-code-msg'), /使えます/);
  assert.equal((await info(p)).rows, 16);
  await p.click('button[data-mx="-en|-90"]');
  assert.match(await p.innerText('#kids-box .kidtop'), /作る 15 件 · 作らない 1 件/);
  await p.click('#kids-box .kidtop [data-filter="off"]');
  assert.deepEqual(await p.$$eval('#kids-box .krow', (els) => els.map((e) => e.dataset.code)), ['hakama-EN-90']);
  await p.click('[data-filter="all"]');
  // JAN を入れると名前に【JAN】が付く (自動の名前)
  await fill(p, '#kids-box .krow[data-key="-wh|-90"] [data-k="jan"]', jan13('458012345001'));
  assert.equal(await p.textContent('#kids-box .krow[data-key="-wh|-90"] .nm'), `子ども袴 2点セット【ホワイト】【90cm】【${jan13('458012345001')}】`);
  // この商品だけの売価・手で直した名前
  await p.click('#kids-box .krow[data-key="-bk|-120"] [data-open]');
  await fill(p, '#kids-box .krow[data-key="-bk|-120"] [data-k="price"]', '13800');
  assert.match(await p.innerText('#kids-box .krow[data-key="-bk|-120"] .c-money'), /この商品だけ 13,800 円/);
  await p.click('#kids-box .krow[data-key="-nv|-110"] [data-open]');
  await fill(p, '#kids-box .krow[data-key="-nv|-110"] [data-k="name"]', '子ども袴 2点セット【ネイビー】【110cm】限定柄');
  assert.match(await p.innerText('#kids-box .krow[data-key="-nv|-110"] .tags'), /名前を手で変更/);
  // JAN をまとめて貼る (見つからないコードは数える)
  await p.click('[data-act="janpaste"]');
  await fill(p, '#jan-paste', `hakama-WH-100\t${jan13('458012345002')}\nhakama-XX-1\t4580123450051`);
  await p.click('[data-act="janapply"]');
  assert.match(await p.textContent('#jan-msg'), /1 件入れました · 見つからないコード 1 件/);
  await shot(p, '1-top');
  await shot(p, '3-kids', '#sec-kids');
  const r = await saveViaConfirm(p);
  assert.match(r, /15 件を下書きにしました/);
  assert.match(r, /この回の 15 件を 1 つのファイルにして NE に取り込みます \(決まり = 1 回目の CSV を先に作ってから残りを足す。未処理の回が重なると 1 つのファイルにまとまる\)/, '残りが無くても決まりの言葉を出す');
  assert.match(r, /出品カードはまとまりで 1 枚 \(NE の写し待ち\) · カード作成済み/, 'まとまりのカード (#1675) を保存の後に 1 回試す: ' + r);
  assert.equal((await one("select status from ops.product_hub_outbox where group_product_id = (select group_product_id from ops.variation_group_codes where code_norm = 'hakama')")).status, 'done');
  assert.equal((await kidsOf('hakama')).length, 15);
  const nv = await one(`select name from core.skus where code = 'hakama-NV-110'`);
  assert.equal(nv.name, '子ども袴 2点セット【ネイビー】【110cm】限定柄');
  assert.equal((await one(`select standard_price_jpy::int as p from core.skus where code = 'hakama-BK-120'`)).p, 13800);
  assert.equal((await one(`select name from core.skus where code = 'hakama-WH-100'`)).name, `子ども袴 2点セット【ホワイト】【100cm】【${jan13('458012345002')}】`);
  assert.equal(await p.evaluate(() => window.MasterEdit.dirty().n), 0, '保存の後は未保存 0');
  assert.equal(await p.isDisabled('#save'), true, '保存の後は押せない (同じ登録を 2 回しない)');
});

await ta('[②] 今あるまとまりに新色 (売価が 2 種類): 探して選ぶ・写した元の子・違う値は自動で選ばない (選ぶまで保存できない)・今ある 3 件は変えない → 2 件', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'blanket', 'blanket-fl');
  assert.match(await p.innerText('#c-copied'), /blanket-fl-BE から写しました \(今ある子 3 件を見ました\)/);
  assert.match(await p.innerText('#cf-price'), /売価が 2 種類あります/);
  await fill(p, '#opt-0', 'ネイビー / -NV\nモスグリーン / -MG');
  await settle(p);
  const i0 = await info(p);
  assert.equal(i0.save, 'disabled');
  assert.ok(i0.issues.some((x) => /売価 \(今ある子で 2 種類・選ぶ\)/.test(x)), JSON.stringify(i0.issues));
  await shot(p, '7-add-conflict', '#sec-common');
  await p.click('[data-cf="price"][data-v="3980"]');
  await settle(p);
  await noneRest(p);   // 🆕 2026-10-13: 必須でない項目は「入力なし」
  await settle(p);
  assert.equal((await info(p)).save, 'enabled');
  assert.match(await p.innerText('#opt-eq'), /今ある 3 件は変えません/);
  const r = await saveViaConfirm(p);
  assert.match(r, /2 件を下書きにしました/);
  assert.deepEqual(await kidsOf('blanket-fl'), ['blanket-fl-BE', 'blanket-fl-BR', 'blanket-fl-GY', 'blanket-fl-MG', 'blanket-fl-NV']);
  assert.equal((await one(`select standard_price_jpy::int as p from core.skus where code = 'blanket-fl-NV'`)).p, 3980);
});

await ta('[③] 前回作らなかった組み合わせ: ブラック × 110 は最初は「作らない」(前回なし)・押したら作る・今ある子は「今ある」', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'hakama-k', 'hakama-kids');
  await fill(p, '#opt-0', 'ネイビー / -NV');
  await settle(p);
  const cell = 'button[data-mx="-bk|-110"]';
  assert.equal(await p.getAttribute(cell, 'aria-pressed'), 'false');
  assert.equal((await p.textContent(cell)).trim(), '前回なし');
  assert.equal(await p.locator('#kids-box .matrix button.cell[disabled]').count(), 3, '今ある 3 件');
  assert.match(await p.innerText('#opt-eq'), /新しい組み合わせ 2[\s\S]*前回は作らなかった 1[\s\S]*作る 2 商品/);
  await p.click(cell);
  assert.equal(await p.getAttribute(cell, 'aria-pressed'), 'true');
  assert.match(await p.innerText('#kids-box .krow[data-key="-bk|-110"] .tags'), /前回は作らなかった → 今回作る/);
  await shot(p, '8-prev', '#sec-kids');
  const r = await saveViaConfirm(p);
  assert.match(r, /3 件を下書きにしました/);
  assert.deepEqual(await kidsOf('hakama-kids'), ['hakama-kids-BK-100', 'hakama-kids-BK-110', 'hakama-kids-NV-100', 'hakama-kids-NV-110', 'hakama-kids-WH-100', 'hakama-kids-WH-110']);
});

await ta('[④] NE で作ったまとまり (記録なし) に足す: 前からある子の文字 = コードの末尾から仮・色の名前 = 子の商品名から仮 (「確かめた」まで保存できない)・人が直した名前は残る・確かめ (重なり) は仮の名前にも効く・最初の 1 回だけ記録', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'ws1', 'ws100');
  await fill(p, '#ax-0', 'カラー');
  await fill(p, '#opt-0', 'ワインレッド / -WR');
  await settle(p);
  assert.deepEqual(await p.$$eval('#old-box input[data-of="hnum"]', (els) => els.map((e) => [e.dataset.oc, e.value])), [['ws100-BR', '-BR'], ['ws100-GY', '-GY'], ['ws100-NV', '-NV']]);
  // 🆕 2026-10-17: 色の名前 = 子の商品名 (ウールストール【ブラウン】) から仮に入れる (印「仮」)・確かめたのチェックまで保存できない
  const hnames = () => p.$$eval('#old-box input[data-of="hname"]', (els) => els.map((e) => e.value));
  assert.deepEqual(await hnames(), ['ブラウン', 'グレー', 'ネイビー']);
  assert.match(await p.innerText('#old-box .oldbox'), /ウールストール【ブラウン】/, '今の商品名も見せる');
  assert.equal(await p.locator('#old-box input.guess').count(), 3);
  assert.equal(await p.locator('#old-box .gtag:not([hidden])').count(), 3);
  let i4 = await info(p);
  assert.equal(i4.save, 'disabled');
  assert.ok(i4.issues.some((x) => /^前からある子の名前 \(商品名から仮に入れた 3 件\) を見て「確かめた」にチェック/.test(x)), JSON.stringify(i4.issues));
  await shot(p, '9-ne', '#sec-opts');
  // 確かめ (重なり) は仮の名前と人が直した名前の間でも効く・直した名前は「仮」ではない
  await fill(p, '#old-box input[data-oc="ws100-GY"][data-of="hname"]', 'ブラウン');
  await settle(p);
  assert.ok((await info(p)).issues.some((x) => /前からある ws100-GY: 名前「ブラウン」の文字が 2 つ/.test(x)), JSON.stringify((await info(p)).issues));
  assert.equal(await p.locator('#old-box input[data-oc="ws100-GY"].guess').count(), 0, '人が直した = 仮の印が外れる');
  assert.equal(await p.locator('#old-box .gtag:not([hidden])').count(), 2);
  await fill(p, '#old-box input[data-oc="ws100-GY"][data-of="hname"]', 'グレー');
  assert.equal(await p.locator('#old-box .gtag:not([hidden])').count(), 3, '仮に入れた値に戻した = 仮のまま (確かめが要る・Codex R1 M1)');
  await fill(p, '#old-box input[data-oc="ws100-GY"][data-of="hname"]', 'グレー杢');
  // 縦軸を足す = かっこ 1 つの名前は 2 軸では読めない (空)・人が直した GY は残る → 外す = 仮に戻る (確かめたのチェックも出し直し)
  await p.click('#ax-v button[data-v="1"]');
  await settle(p);
  assert.deepEqual(await hnames(), ['', 'グレー杢', '']);
  await p.click('#ax-v button[data-v="0"]');
  await settle(p);
  assert.deepEqual(await hnames(), ['ブラウン', 'グレー杢', 'ネイビー']);
  i4 = await info(p);
  assert.ok(i4.issues.some((x) => /^前からある子の名前 \(商品名から仮に入れた 2 件\)/.test(x)), JSON.stringify(i4.issues));
  await p.check('#old-ok');
  await settle(p);
  assert.ok(!(await info(p)).issues.some((x) => /確かめた/.test(x)));
  // 今ある子で売価・仕入先が違う (グレーだけ 3,280 円・三河) = 自動では選ばない = 人が選ぶ
  assert.match(await p.innerText('#cf-price'), /売価が 2 種類あります/);
  await p.click('[data-cf="price"][data-v="2980"]');
  await p.click('[data-cf="supplier"][data-v="0001"]');
  await settle(p);
  const r = await saveViaConfirm(p);
  assert.match(r, /1 件を下書きにしました/);
  const g = await V.readVariationGroup(db, (await one(`select product_id::text as id from core.products where display_code = 'ws100'`)).id);
  assert.deepEqual(g.options.map((o) => [o.code, o.name]), [['-BR', 'ブラウン'], ['-GY', 'グレー杢'], ['-NV', 'ネイビー'], ['-WR', 'ワインレッド']]);
  // 2 回目 = 記録のあるまとまり (確かめの欄は出ない・軸は 🔒)
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'ws1', 'ws100');
  assert.equal(await p.locator('#old-box .oldbox').count(), 0);
  assert.match(await p.innerText('#axes-box'), /カラー/);
});

await ta('[⑤] 40 色 × 3 サイズ = 120 (上限ちょうど): 「40 × 3 = 120」・作る 120 / 120・分ける案は出ない・保存できる', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await newGroup(p, { code: 'roomwear', name: 'やわらか ルームウェア', h: 'カラー', v: 'サイズ', hl: colorLines(40), vl: 'S / -S\nM / -M\nL / -L' });
  await fillCommon(p, { price: '2980', cost: '1200', sales: '3', sup: '0034', months: '2', ship: 'A1' });
  await settle(p);
  assert.match(await p.innerText('#opt-eq'), /カラー 40[\s\S]*×[\s\S]*サイズ 3[\s\S]*=[\s\S]*120 商品/);
  assert.match(await p.innerText('#hud-meter'), /作る 120 \/ 120/);
  assert.equal(await p.locator('#kids-box .plan').count(), 0);
  const r = await saveViaConfirm(p);
  assert.match(r, /120 件を下書きにしました/);
  assert.equal((await kidsOf('roomwear')).length, 120);
});

await ta('[⑥] 上限を超える (45 色 × 3 = 135): 保存を止める・色そのものを分ける (40 色 = 120 件 + 残り 5 色)・保存 → 「残りの 5 色を足す」→ 同じまとまりに 15 件 (まだ CSV にしていない 120 件と 1 つのファイル) / NE 登録の CSV の画面 = 未処理の 2 回が 1 つのファイルにまとまる', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await newGroup(p, { code: 'roomwear2', name: 'ルームウェア 2', h: 'カラー', v: 'サイズ', hl: colorLines(45), vl: 'S / -S\nM / -M\nL / -L' });
  await fillCommon(p, { price: '2980', cost: '1200', sales: '3', sup: '0034', months: '2', ship: 'A1' });
  await settle(p);
  const i0 = await info(p);
  assert.equal(i0.save, 'disabled');
  assert.match(await p.innerText('#kids-box .plan'), /子が 135 件[\s\S]*カラー 40 色 × サイズ 3 = 120 件[\s\S]*残りの 5 色/);
  await shot(p, '10-over', '#sec-kids');
  await p.click('[data-act="split"]');
  await settle(p);
  assert.match(await p.innerText('#next-box'), /次に足す色 \(5\)/);
  assert.match(await p.innerText('#hud-meter'), /作る 120 \/ 120/);
  const r = await saveViaConfirm(p);
  assert.match(r, /120 件を下書きにしました[\s\S]*1 回目の CSV を先に作ってから残りを足してください。未処理の回が重なると 1 つのファイルにまとまります/);
  assert.equal(await p.locator('#result [data-act="next"]').count(), 1);
  await p.click('#result [data-act="next"]');
  await p.waitForSelector('#g-picked .picked .pc:text-is("roomwear2")');
  await settle(p);
  assert.equal((await p.inputValue('#opt-0')).split('\n').length, 5);
  assert.match(await p.innerText('#opt-eq'), /作る 15 商品/);
  // まだ CSV にしていない 120 件と 1 つのファイルにまとまる、と前もって言う (#1679 Codex R2 Medium 2)
  assert.match(await p.innerText('#opt-eq .vpend'), /まだ NE 登録の CSV にしていない子が 120 件あります = 今回の分と 1 つのファイルにまとまります/);
  const r2 = await saveViaConfirm(p);
  assert.match(r2, /15 件を下書きにしました/);
  // 保存の後の案内 = まだ CSV にしていない 120 件と今回の 15 件を合わせて 1 つのファイル (#1679 Codex R3 Low)
  assert.match(r2, /まだ CSV にしていない 120 件と今回の 15 件を合わせて 1 つのファイル \(135 行\) にして NE に取り込みます \(決まり = 1 回目の CSV を先に作ってから残りを足す。未処理の回が重なると 1 つのファイルにまとまる\)/);
  assert.doesNotMatch(r2, /この回の 15 件/);
  assert.equal((await kidsOf('roomwear2')).length, 135);
  // NE 登録の CSV の画面 (🆕 10/20 ボタン 1 つ): まとまりの 135 件はふつうの単品と同じ「次の CSV」に入る (まとまりごとのボタンは無い)・登録した回は「これまでの記録」
  await p.goto(B + '/reg-csv');
  assert.match(await p.textContent('#next'), /roomwear2/);
  assert.equal(await p.locator('button[data-act="build-all"]').count(), 1, 'ボタンは「NE 登録の CSV を作る」1 つ');
  assert.equal(await p.locator('button[data-act="build-variation"]').count(), 0, 'まとまりごとのボタンは無い');
  const card = p.locator('#vcands .vgroup:has(.mono:text-is("roomwear2"))');
  assert.match(await card.textContent(), /回 1[\s\S]*子 120 件[\s\S]*回 2[\s\S]*子 15 件/);
});

await ta('[⑥b] 1 つのファイルの上限 (試験は 136 行) を超える: 足す画面 = 「先に今ある分の CSV を作ってから」で保存を止める・API も 409 / NE 登録の CSV の画面 = 作れない理由と手順 (ボタンは押せない)', async (p) => {
  V.__setMaxFileRowsForTest(136);
  try {
    await p.goto(B + '/new?kind=variation');
    await pickGroup(p, 'roomwear2', 'roomwear2');
    await fill(p, '#opt-0', 'テスト色 / -ZZ');
    await settle(p);
    const i0 = await info(p);
    assert.equal(i0.save, 'disabled');
    assert.ok(i0.issues.some((x) => /まだ NE 登録の CSV にしていない子が 135 件あります。今回の 3 件と合わせると 138 件で、1 つのファイル \(136 行まで\) にできません → 先に「NE 登録の CSV」で今ある分のファイルを作ってから足す/.test(x)), JSON.stringify(i0.issues));
    // API も断る (画面を通らない送り方でも・何も保存しない)
    const before = (await kidsOf('roomwear2')).length;
    const gid = (await one("select group_product_id::text as id from ops.variation_group_codes where code_norm = 'roomwear2'")).id;
    const r = await p.evaluate(async ([base, gid]) => { const x = await fetch(base + '/api/new/variation', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ request_id: crypto.randomUUID(), group: { mode: 'add', product_id: gid }, options: [{ axis: 1, code: '-ZZ', name: 'テスト色' }], children: ['-S', '-M', '-L'].map((v) => ({ code: 'roomwear2-ZZ' + v, choices: { 1: '-ZZ', 2: v }, name: 'x' + v })), values: { standard_price: '2980', cost: { jpy: '1200' }, tax_rate: '0.1', sales_class: '3', primary_supplier: '0034', reorder_months: '2', expiry_managed: '0', inbound_date_managed: '0' },
      blanks: ['maker', 'shipping_code', 'order_settings.order_lot', 'order_settings.condition', 'order_settings.material', 'order_settings.case_lot', 'order_settings.case_group', 'order_settings.capacity_per_unit', 'card.official_url', 'card.amazon', 'card.reference_urls', 'children.jan'] }) }); return [x.status, (await x.json()).reason]; }, [B, gid]);
    assert.deepEqual(r, [409, 'variation_pending_too_many']);
    assert.equal((await kidsOf('roomwear2')).length, before);
    // NE 登録の CSV の画面: 上限 14 行 = roomwear2 (2 回・135 行) も hakama (1 回・15 行) も作れない → 理由と手順・ボタンは押せない
    V.__setMaxFileRowsForTest(14);
    await p.goto(B + '/reg-csv');
    // 🆕 10/20: 入らないまとまりは「直すもの」に 1 行 (手順つき)・次の CSV には入れない
    const fix = await p.textContent('#fix');
    assert.match(fix, /まとまり roomwear2: まだ CSV にしていない子が 135 件 = 1 つのファイル \(14 行まで\) に入りません。作らない子を、その子の画面の「このまとまりから外す \(廃止\)」で減らしてください/);
    assert.match(fix, /まとまり hakama: まだ CSV にしていない子が 15 件 = 1 つのファイル \(14 行まで\)/);
    assert.doesNotMatch(await p.textContent('#next'), /まとまり (roomwear2|hakama)\)/);
    await shot(p, '6b-too-many', '#fix');
  } finally { V.__setMaxFileRowsForTest(null); }
});

await ta('[⑦] 誤り: 「-」の付け忘れ = 誤り + 「- を付ける」(押した行が光る・黙っては直さない)・文字の重なり (大文字小文字)・名前の重なり・JAN の形とほかの商品の JAN・まとまりのコードがもうある (このまとまりに足す)・product-hub の下書き', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await newGroup(p, { code: 'Mofu-Blanket', name: 'もふもふ ブランケット', h: 'カラー', hl: 'ホワイト / WH\nグレー / -GY\nホワイト / -WT\nチャコール / -gy\nベージュ / -BE\nネイビー / -NV' });
  await fillCommon(p, { price: '2,980', sales: '3', sup: '0034', months: '2' });
  await settle(p);
  const i0 = await info(p);
  assert.equal(i0.save, 'disabled');
  assert.ok(i0.issues.some((x) => /1 行目「ホワイト」: 「-」から入れます \(→ -WH\)/.test(x)), JSON.stringify(i0.issues));
  assert.ok(i0.issues.some((x) => /4 行目「チャコール」: コードにつける文字が 2 行目 と同じ/.test(x)), JSON.stringify(i0.issues));
  assert.ok(i0.issues.some((x) => /3 行目「ホワイト」: 名前が 1 行目 と同じ/.test(x)), JSON.stringify(i0.issues));
  await p.click('[data-fix="0"]');
  assert.ok((await p.inputValue('#opt-0')).startsWith('ホワイト / -WH\n'));
  assert.equal(await p.locator('#opt-p-0 tr.flash').count(), 1, '直した行が光る');
  // JAN の形 (チェック数字) とほかの商品の JAN
  await fill(p, '#kids-box .krow[data-key="-be|"] [data-k="jan"]', '4901234567890');
  await fill(p, '#kids-box .krow[data-key="-nv|"] [data-k="jan"]', TAKEN_JAN);
  await settle(p);
  const i1 = await info(p);
  assert.ok(i1.issues.some((x) => /Mofu-Blanket-BE: JAN: 最後の数字 \(チェック数字\) が合いません/.test(x)), JSON.stringify(i1.issues));
  assert.ok(i1.issues.some((x) => /Mofu-Blanket-NV: JAN: ほかの商品 \(s003\) が使っています/.test(x)), JSON.stringify(i1.issues));
  await shot(p, '11-bad-fixed', '#sec-opts');
  // まとまりのコードがもうある = 「このまとまりに足す」で今あるまとまりへ
  await fill(p, '#g-code', 'BLANKET-FL');
  await settle(p);
  assert.match(await p.innerText('#g-code-msg'), /もうあるまとまり/);
  // product-hub の下書きと同じ管理番号
  await fill(p, '#g-code', 'kimono-obi');
  await settle(p);
  assert.match(await p.innerText('#g-code-msg'), /product-hub に同じ管理番号「kimono-obi」の下書きカード \(#31\)/);
  await fill(p, '#g-code', 'blanket-fl');
  await settle(p);
  await p.click('#g-code-msg [data-gotopick]');
  await p.waitForSelector('#g-picked .picked .pc:text-is("blanket-fl")');
});

await ta('[⑦b] 子のロジザードの値 = ほかの子の JAN (#1719 Codex R6): 前の子の値 = 後の子の JAN / 後の子の値 = 前の子の JAN の両方の向きで打ったその場で印・確かめに子の値を送る (lz_children)・DB の確かめの lz_kid_clash も出す', async (p) => {
  const sent = [];
  p.on('request', (r) => { if (r.url().endsWith('/api/variation/check')) sent.push(JSON.parse(r.postData() || '{}')); });
  await p.goto(B + '/new?kind=variation');
  await newGroup(p, { code: 'lz-ord', name: '順番の試し', h: 'カラー', hl: 'ホワイト / -WH\nグレー / -GY\nネイビー / -NV' });
  await fillCommon(p, { price: '2,980', sales: '3', sup: '0034', months: '2' });
  const row = (k, f) => `#kids-box .krow[data-key="${k}|"] [data-k="${f}"]`;
  const JX = jan13('458777700101'), JY = jan13('458777700102');
  // ① 前の子 (WH) の値 = 後の子 (NV) の JAN
  await fill(p, row('-nv', 'jan'), JX);
  await fill(p, row('-wh', 'lzbc'), JX);
  await settle(p);
  let i = await info(p);
  assert.ok(i.issues.some((x) => x === 'lz-ord-WH: ロジザードのバーコードが lz-ord-NV の JAN と同じ'), JSON.stringify(i.issues));
  assert.equal(i.save, 'disabled');
  const last = sent[sent.length - 1];
  assert.deepEqual(last.lz_children.find((k) => k.code === 'lz-ord-WH'), { code: 'lz-ord-WH', jan: '', lz: JX }, JSON.stringify(last));
  // ② 後の子 (NV) の値 = 前の子 (WH) の JAN (向きを逆に)
  await fill(p, row('-nv', 'jan'), '');
  await fill(p, row('-wh', 'lzbc'), '');
  await fill(p, row('-wh', 'jan'), JY);
  await fill(p, row('-nv', 'lzbc'), JY);
  await settle(p);
  i = await info(p);
  assert.ok(i.issues.some((x) => x === 'lz-ord-NV: ロジザードのバーコードが lz-ord-WH の JAN と同じ'), JSON.stringify(i.issues));
  assert.ok(!i.issues.some((x) => /^lz-ord-WH: ロジザードのバーコードが/.test(x)), JSON.stringify(i.issues));
  // ③ DB の確かめが子の値の重なりを返した (lz_kid_clash) = 画面に出す
  await fill(p, row('-nv', 'lzbc'), 'X00LZUI001');
  await p.route('**/api/variation/check', async (route) => {
    const res = await route.fetch();
    const j = await res.json();
    await route.fulfill({ response: res, json: { ...j, lz_kid_clash: { 'lz-ord-GY': 'zz-kid' } } });
  });
  await fill(p, row('-gy', 'lzbc'), 'X00LZUI002');
  await settle(p);
  i = await info(p);
  assert.ok(i.issues.some((x) => x === 'lz-ord-GY: ロジザードのバーコードが zz-kid の JAN と同じ (DB の確かめ)'), JSON.stringify(i.issues));
  assert.ok(!i.issues.some((x) => /^lz-ord-NV: ロジザードのバーコードが/.test(x)), JSON.stringify(i.issues));
});

await ta('[⑧] 空から: あと N つ・7 の確かめは「まだ入れていない」・保存は押せない / 切替の前 (products.parent が load) = まとまりの札と欄が出ない・単品とセットは今までどおり', async (p) => {
  await p.goto(B + '/new?kind=variation');
  const i0 = await info(p);
  assert.equal(i0.save, 'disabled');
  assert.match(i0.remain, /下書き保存まで あと\s*\d+\s*つ/);
  assert.equal(await p.evaluate(() => window.MasterEdit.dirty().n), 0);
  // 単品の画面に 3 枚 (まとまりは NEW)
  await p.goto(B + '/new?kind=single');
  assert.equal(await p.locator('.kindcards.three .kindcard').count(), 3);
  assert.equal(await p.locator('#gate-note').count(), 0);
  await T.W2.setActiveOwnershipInDb(pg, { ...T.ALL_COMPANY, 'products.parent': 'load' });
  try {
    await p.goto(B + '/new?kind=variation');
    assert.match(await p.innerText('#gate-note'), /まだ選べません/);
    assert.equal(await p.locator('#f').count(), 0, 'まとまりの欄は出ない');
    assert.equal(await p.locator('.kindcard').count(), 2);
    await p.goto(B + '/new?kind=single');
    assert.equal(await p.locator('.kindcards .kindcard').count(), 2);
    assert.equal(await p.locator('.kindcard[data-kind="variation"]').count(), 0);
    assert.match(await p.innerText('#gate-note'), /まだ選べません/);
    assert.ok(await p.locator('#code').isVisible(), '単品の欄は今までどおり');
    // API も断る (DB が持ち主を見る)
    const r = await p.evaluate(async (base) => (await fetch(base + '/api/new/variation', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ request_id: crypto.randomUUID(), group: { mode: 'new', code: 'gx', name: 'x' }, axes: [{ axis: 1, name: '色' }], options: [{ axis: 1, code: '-A', name: 'A' }], children: [{ code: 'gx-A', choices: { 1: '-A' }, name: 'x' }], values: { standard_price: '1', tax_rate: '0.1', sales_class: '1', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0' },
      blanks: ['cost', 'maker', 'shipping_code', 'inbound_date_managed', 'order_settings.order_lot', 'order_settings.condition', 'order_settings.material', 'order_settings.case_lot', 'order_settings.case_group', 'order_settings.capacity_per_unit', 'card.official_url', 'card.amazon', 'card.reference_urls', 'children.jan'] }) })).json(), B);
    assert.equal(r.reason, 'parent_not_company');
    // 🆕 #1679 Codex R1 Low: 読むだけのまとまりの API (探す・1 つを読む・確かめる) も同じ持ち主の門で閉じる
    assert.deepEqual(await readApis(p), [[409, 'parent_not_company'], [409, 'parent_not_company'], [409, 'parent_not_company']]);
  } finally { await T.W2.setActiveOwnershipInDb(pg, T.ALL_COMPANY); }
  assert.deepEqual((await readApis(p)).map((x) => x[0]), [200, 200, 200]);
  // 持ち主を読めない (関数の実行権が無い) = 503 / このコードが扱えない持ち主 (code_behind) = 409
  await T.W2.revokeActiveMap(pg);
  try { assert.deepEqual(await readApis(p), [[503, 'owner_unreadable'], [503, 'owner_unreadable'], [503, 'owner_unreadable']]); } finally { await T.W2.grantActiveMap(pg); }
  OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.filter((k) => k !== 'products.parent'));
  try { assert.deepEqual(await readApis(p), [[409, 'parent_not_company'], [409, 'parent_not_company'], [409, 'parent_not_company']]); }
  finally { OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS); }
});

await ta('[+] 商品の画面の代表 (親) = 見るだけ (保存の欄が無い・登録の時に 1 回だけと案内)・API で parent_code を送ると 400', async (p) => {
  await p.goto(B + '/sku/blanket-fl-NV');
  const row = p.locator('[data-row="parent_code"]');
  assert.match(await row.innerText(), /blanket-fl[\s\S]*フランネル ブランケット[\s\S]*登録の時に 1 回だけ/);
  assert.equal(await row.locator('input').count(), 0);
  const r = await p.evaluate(async (base) => (await fetch(base + '/api/sku/s001', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ request_id: crypto.randomUUID(), seen: { token: 'a'.repeat(64) }, values: { parent_code: 'hakama' } }) })).status, B);
  assert.equal(r, 400);
  // 一覧の札から新しいまとまりを開ける (種類の札)
  await p.goto(B + '/new?kind=set');
  await p.click('.kindcard[data-kind="variation"]');
  await p.waitForURL(/kind=variation/);
});

console.log('\n入力なし (2026-10-13)');
await ta('[⑪] 入力なし (まとまり): 必須でない項目 (原価・発送方法・入荷日の管理・発注の設定・公式ページ URL・Amazon URL・JAN の無い子) は 値か入力なし・値を入れると外れる・JAN が全部そろうと JAN の行は消える・確かめの窓に一覧・記録 (理由) に残る・印の無い API は 400', async (p) => {
  await p.goto(B + '/new?kind=variation');
  assert.equal(await p.getAttribute('#c-inbound', 'data-value'), '', '入荷日の管理は初めは選ばない形');
  await newGroup(p, { code: 'nonegrp', name: '入力なしの試験', h: 'カラー', hl: 'ホワイト / -WH\nブラック / -BK' });
  await fillCommon(p, { cost: '', ship: '' });
  await settle(p);
  let i0 = await info(p);
  assert.equal(i0.save, 'disabled');
  for (const re of [/^原価 \(値か「入力なし」\)/, /^発送方法 \(値か「入力なし」\)/, /^ロジザードの入荷日の管理 \(値か「入力なし」\)/, /^発注の設定: 発注ロット \(値か「入力なし」\)/,
    /^公式ページ URL \(値か「入力なし」\)/, /^Amazon URL \(値か「入力なし」\)/, /^参考 URL \(値か「入力なし」\)/, /^JAN の無い子 \(2 件\) \(値か「入力なし」\)/]) assert.ok(i0.issues.some((x) => re.test(x)), re + ' ' + JSON.stringify(i0.issues));
  // 原価: 入力なし → 値を入れると外れる
  await p.click('#sec-common .f:has(#c-cost) .nonechk');
  await settle(p);
  assert.ok(!(await info(p)).issues.some((x) => /^原価/.test(x)), '入力なしにした原価は数えない');
  await fill(p, '#c-cost', '900');
  assert.equal(await p.isChecked('[data-none="cost"]'), false, '値を入れたらチェックが外れる');
  await fill(p, '#c-cost', '');
  await settle(p);
  assert.ok((await info(p)).issues.some((x) => /^原価/.test(x)), '値を消しただけ = まだ');
  // 入荷日の管理: 入力なし = 選んだのを消す
  await p.click('#c-inbound button[data-v="1"]');
  await p.click('#sec-common .f:has(#c-inbound) .nonechk');
  assert.equal(await p.getAttribute('#c-inbound', 'data-value'), '');
  // JAN: 1 件だけ入れても行は出たまま・全部の子に入れると行が消えてチェックも外れる
  assert.equal(await p.isHidden('#row-jan-none'), false);
  await p.click('#row-jan-none .nonechk');
  await settle(p);
  assert.ok(!(await info(p)).issues.some((x) => /^JAN の無い子/.test(x)));
  await fill(p, '#kids-box .krow[data-key="-wh|"] [data-k="jan"]', jan13('458012349001'));
  await fill(p, '#kids-box .krow[data-key="-bk|"] [data-k="jan"]', jan13('458012349002'));
  await settle(p);
  assert.equal(await p.isHidden('#row-jan-none'), true, '全部の子に JAN = 行を出さない');
  assert.equal(await p.isChecked('[data-none="children.jan"]'), false, '全部の子に JAN = チェックを外す (両方は持たない)');
  await fill(p, '#kids-box .krow[data-key="-bk|"] [data-k="jan"]', '');
  await settle(p);
  assert.ok((await info(p)).issues.some((x) => /^JAN の無い子 \(1 件\)/.test(x)), '1 件消すとまた聞く');
  // 🆕 #1706 Codex R1 M1: 原価 = 子ごとの実際の原価。共通を入力なし → 子だけの原価を 1 つ = まだ入力なしのまま (原価の無い子がある)・全部の子に入れる = 入力なしが外れる
  await p.click('#sec-common .f:has(#c-cost) .nonechk');
  await p.click('#kids-box .krow[data-key="-wh|"] [data-open]');
  await fill(p, '#kids-box .krow[data-key="-wh|"] [data-k="cost"]', '700');
  await settle(p);
  assert.equal(await p.isChecked('[data-none="cost"]'), true, '原価の無い子がある = 入力なしのまま');
  assert.ok(!(await info(p)).issues.some((x) => /^原価/.test(x)));
  await p.click('#kids-box .krow[data-key="-bk|"] [data-open]');
  await fill(p, '#kids-box .krow[data-key="-bk|"] [data-k="cost"]', '800');
  await settle(p);
  assert.equal(await p.isChecked('[data-none="cost"]'), false, '全部の子に原価 = 入力なしが外れる (両方は持たない)');
  assert.ok(!(await info(p)).issues.some((x) => /^原価/.test(x)), '全部の子に原価 = 値がある');
  await fill(p, '#kids-box .krow[data-key="-bk|"] [data-k="cost"]', '');
  await settle(p);
  assert.ok((await info(p)).issues.some((x) => /^原価 \(値か「入力なし」\)/.test(x)), '原価の無い子がまたできた = また聞く');
  // 押す所は 44px
  const small = await p.evaluate(() => [...document.querySelectorAll('.nonechk')].filter((x) => x.offsetParent !== null).map((x) => x.getBoundingClientRect()).filter((r) => r.height < 44 || r.width < 44).length);
  assert.equal(small, 0);
  // 印の無い API (古い画面) = 400 (どの項目か)・何も書かない
  const raw = await p.evaluate(async (base) => { const r = await fetch(base + '/api/new/variation', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ request_id: crypto.randomUUID(), group: { mode: 'new', code: 'nonegrp', name: 'x' }, axes: [{ axis: 1, name: 'カラー' }], options: [{ axis: 1, code: '-WH', name: 'ホワイト' }], children: [{ code: 'nonegrp-WH', choices: { 1: '-WH' }, name: 'x' }], values: { standard_price: '1', tax_rate: '0.1', sales_class: '1', primary_supplier: '0001', reorder_months: '1', expiry_managed: '0' } }) }); return [r.status, await r.json()]; }, B);
  assert.deepEqual([raw[0], raw[1].reason, raw[1].field, raw[1].screen_outdated], [400, 'blank_unmarked', 'cost', true], JSON.stringify(raw));
  assert.equal((await kidsOf('nonegrp')).length, 0);
  // 残りを入力なし → 確かめの窓に「入力なし」の一覧 → 保存 → 子の登録の記録 (理由) に残る
  await noneRest(p);
  await settle(p);
  assert.equal((await info(p)).save, 'enabled', JSON.stringify(await info(p)));
  await p.click('#save2');
  await p.waitForSelector('#confirm-bg.on');
  assert.match(await p.innerText('#confirm-body'), /入力なし\s*原価・メーカー・発送方法・ロジザードの入荷日の管理・発注ロット・発注条件グループ・原料グループ・ケースロット・ケースグループ・容量 \/ 個・公式ページ URL・Amazon URL か ASIN・参考 URL・JAN \(JAN の無い子\)/);
  await p.check('#fix-ok');
  const bodies = [];
  p.on('request', (rq) => { if (/\/api\/new\/variation$/.test(rq.url())) bodies.push(JSON.parse(rq.postData())); });
  await p.click('#confirm-yes');
  await p.waitForSelector('#result .result', { timeout: 60000 });
  assert.match(await p.innerText('#result'), /2 件を下書きにしました/);
  assert.deepEqual(bodies[0].blanks, ['cost', 'maker', 'shipping_code', 'inbound_date_managed', 'order_settings.order_lot', 'order_settings.condition', 'order_settings.material', 'order_settings.case_lot', 'order_settings.case_group', 'order_settings.capacity_per_unit', 'card.official_url', 'card.amazon', 'card.reference_urls', 'children.jan']);
  // 🆕 0080: ロジザードのバーコード = 子ごと (JAN のある子 = 初めは JAN・無い子 = 「空欄で送る」の印で空欄)・全部の子に 1 行ずつ
  const lzOf = Object.fromEntries(bodies[0].children.map((k) => [k.code, k.lz_barcode]));
  assert.deepEqual(lzOf['nonegrp-BK'], { kind: 'lz_assign' });
  assert.equal(lzOf['nonegrp-WH'].kind, 'value');
  assert.deepEqual((await q(`select s.code, r.rule from ops.sku_logizard_barcode_rules r join core.skus s on s.sku_id = r.sku_id where s.code in ('nonegrp-WH', 'nonegrp-BK') order by s.code`)).map((r) => [r.code, r.rule]),
    [['nonegrp-BK', 'lz_assign'], ['nonegrp-WH', 'jan']]);
  // 子ごとの記録 (#1706 Codex R1 M1): 原価・JAN は その子に無いときだけ (WH = 子だけの原価 700・JAN あり / BK = どちらも無い)
  const reasons = Object.fromEntries((await q(`select s.code, e.reason from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code in ('nonegrp-WH', 'nonegrp-BK')`)).map((r) => [r.code, r.reason]));
  assert.deepEqual(reasons, {
    'nonegrp-BK': '新商品の登録 (色違い・サイズ違い) · 入力なし: 原価・メーカー・発送方法・入荷日の管理・公式ページURL・Amazon・参考URL・JAN · 発注の設定の入力なし: 発注ロット・発注条件グループ・原料グループ・ケースロット・ケースグループ・容量',
    'nonegrp-WH': '新商品の登録 (色違い・サイズ違い) · 入力なし: メーカー・発送方法・入荷日の管理・公式ページURL・Amazon・参考URL · 発注の設定の入力なし: 発注ロット・発注条件グループ・原料グループ・ケースロット・ケースグループ・容量',
  });
  assert.deepEqual((await q(`select s.code, c.cost_jpy::int as c from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code in ('nonegrp-WH', 'nonegrp-BK') order by s.code`)).map((r) => [r.code, r.c]), [['nonegrp-WH', 700]]);
  assert.equal((await one("select inbound_date_managed from core.products where display_code = 'nonegrp-WH'")).inbound_date_managed, null, '入荷日の管理を入力なし = 不明のまま');
});

console.log('\nまとまりの操作 (PR-7b)');
await ta('[⑨] 名前を直す (今あるまとまりを選んだとき): 窓 = まとまりの名前・軸の名前・選択肢名 (コードにつける文字は 🔒)・変えた所だけ送る・入れた選択肢はそのまま・新しい子の名前は新しいまとまりの名前から', async (p) => {
  await p.goto(B + '/new?kind=variation');
  await pickGroup(p, 'blanket', 'blanket-fl');
  await fill(p, '#opt-0', 'ホワイト / -WH');
  await p.click('#g-picked [data-act="rename"]');
  await p.waitForSelector('#vl-bg.on #vl-name');
  assert.equal(await p.isDisabled('#vl-yes'), true, '変えるまで押せない');
  assert.match(await p.innerText('#vl-body'), /-BR[\s\S]*-BE[\s\S]*-GY/);
  await p.fill('#vl-name', 'フランネル ブランケット 冬');
  await p.fill('#vl-body .vl-opt[data-code="-BR"]', 'チョコ');
  await p.click('#vl-yes');
  await p.waitForFunction(() => !document.querySelector('#vl-bg').classList.contains('on'));
  await p.waitForFunction(() => /フランネル ブランケット 冬/.test(document.querySelector('#g-picked').innerText));
  assert.equal(await p.inputValue('#opt-0'), 'ホワイト / -WH', '入れた選択肢はそのまま');
  await settle(p);
  assert.match(await p.textContent('#kids-box .krow .nm'), /^フランネル ブランケット 冬【ホワイト】/);
  assert.match(await p.innerText('#opt-p-0'), /チョコ/);
  const g = await V.readVariationGroup(db, BL.group_product_id);
  assert.deepEqual([g.name, g.options.find((o) => o.code === '-BR').name], ['フランネル ブランケット 冬', 'チョコ']);
  assert.equal((await one(`select name from core.skus where code = 'blanket-fl-BR'`)).name, 'フランネル ブランケット【ブラウン】', '子の商品名は自動では変えない');
});

await ta('[⑩] 商品の画面: まとまりの子 = 「まとまりの名前を直す」「このまとまりから外す (廃止)」(理由が要る・通ったら読み直す) / NE で直接作られた商品 = 「NE の代表を採用する」(照合がまだ = 断りの文) / products.parent が load = 出さない', async (p) => {
  await p.goto(B + '/sku/hakama-kids-WH-100');
  assert.equal(await p.locator('[data-vops="labels"]').count(), 1);
  await p.click('[data-vops="cancel"]');
  await p.waitForSelector('#vc-bg.on');
  assert.equal(await p.isDisabled('#vc-yes'), true, '理由を入れるまで押せない');
  await p.fill('#vc-reason', 'この色は作らない');
  await p.click('#vc-yes');
  await p.waitForFunction(() => !document.querySelector('#vc-bg').classList.contains('on'));
  await p.waitForLoadState('load');
  await p.waitForFunction(() => /やめた|登録をやめた/.test(document.body.innerText));
  assert.equal((await one(`select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = 'hakama-kids-WH-100'`)).state, 'cancelled');
  // NE で直接作られた商品 (quarantined) = 採用のボタン。照合の観測がまだ = 断りの文 (何もしない)
  const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
  const { basePlan, skuOf } = await import('./fixtures/master-variation-db.mjs');
  const lr = await runInitialLoad(db, basePlan([skuOf('q-ui-RD', 'ウールストール【レッド】', { representativeCode: 'ws100', representativeState: 'value' })]), { log: () => {}, runId: 'load_vg7b_ui', now: new Date(Date.now() - 4 * 86400e3) });
  assert.equal(lr.ok, true, lr.error);
  await p.goto(B + '/sku/q-ui-RD');
  assert.equal(await p.locator('[data-vops="cancel"]').count(), 0);
  await p.click('[data-vops="adopt"]');
  await p.waitForSelector('#vc-bg.on');
  assert.equal(await p.isDisabled('#vc-yes'), false, '採用の理由はなくてもよい');
  await p.click('#vc-yes');
  await p.waitForFunction(() => /照合|NE の/.test(document.querySelector('#vc-msg').textContent));
  assert.ok(await p.isVisible('#vc-bg.on'), '断られたら窓は閉じない');
  await p.keyboard.press('Escape');
  await T.W2.setActiveOwnershipInDb(pg, { ...T.ALL_COMPANY, 'products.parent': 'load' });
  try {
    await p.goto(B + '/sku/hakama-kids-WH-110');
    assert.equal(await p.locator('[data-vops]').count(), 0);
  } finally { await T.W2.setActiveOwnershipInDb(pg, T.ALL_COMPANY); }
});

console.log('\nスマホの幅');
for (const w of [390, 360]) {
  await ta(`[📱${w}] はみ出さない・押す所は 44px 以上・種類は縦・手順の帯「n/7」(前へ / 次へ)・キーボードの間は下の帯を小さく・30 件をこえたら「パソコンがおすすめ」`, async (p) => {
    const over = async (label) => {
      const ov = await p.evaluate(() => {
        const W = document.documentElement.clientWidth;
        const wide = Array.from(document.querySelectorAll('body *')).filter((el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.right > W + 1 && !el.closest('.scrollx') && !el.closest('.mxscroll') && getComputedStyle(el).position !== 'fixed'; }).slice(0, 5).map((el) => el.tagName + '.' + el.className + ' ' + Math.round(el.getBoundingClientRect().right));
        const small = Array.from(document.querySelectorAll('#new-page button, #new-page input:not([type=checkbox]), #new-page select, #new-page textarea')).filter((el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0 && r.height < 43.5 && !el.closest('[hidden]') && !el.closest('.jump'); }).slice(0, 5).map((el) => el.tagName + '#' + el.id + '.' + el.className + ' h=' + Math.round(el.getBoundingClientRect().height));
        return { sw: document.documentElement.scrollWidth, W, wide, small };
      });
      assert.equal(ov.sw, ov.W, `${label}: 横にはみ出す ${JSON.stringify(ov.wide)}`);
      assert.deepEqual(ov.wide, [], label);
      assert.deepEqual(ov.small, [], `${label}: 押す所が小さい`);
    };
    await p.goto(B + '/new?kind=variation');
    await over('空');
    const cards = await p.$$eval('.kindcards.three .kindcard', (els) => els.map((e) => Math.round(e.getBoundingClientRect().left)));
    assert.equal(new Set(cards).size, 1, '種類は縦に 1 列');
    assert.match(await p.innerText('#jumpm'), /1\/7 種類/);
    await newGroup(p, { code: 'phone1', name: 'スマホ ルームウェア', h: 'カラー', v: 'サイズ', hl: colorLines(12), vl: 'S / -S\nM / -M\nL / -L' });
    await fillCommon(p, { price: '2980', sales: '3', sup: '0034', months: '2', ship: 'A1' });
    await settle(p);
    await over('36 件');
    assert.match(await p.innerText('#kids-box'), /36 件あります。パソコンがおすすめです/);
    await p.evaluate(() => { const el = document.querySelector('#sec-opts'); window.scrollTo(0, el.getBoundingClientRect().top + window.scrollY - 60); });
    await p.waitForTimeout(500);
    assert.match(await p.innerText('#jumpm-now'), /4\/7 選択肢/);
    if (w === 390) await shot(p, '12-phone-opts');
    await p.click('[data-stepnav="1"]');
    await p.waitForTimeout(700);
    assert.match(await p.innerText('#jumpm-now'), /5\/7 共通の欄/);
    await p.focus('#c-price');
    assert.equal(await p.evaluate(() => document.body.classList.contains('kb')), true);
    if (w === 390) { await p.evaluate(() => { const el = document.querySelector('#klist'); window.scrollTo(0, el.getBoundingClientRect().top + window.scrollY - 120); }); await shot(p, '13-phone-kids'); }
    // 今あるまとまり (②) と誤り (⑦) もはみ出さない
    await p.goto(B + '/new?kind=variation');
    await pickGroup(p, 'blanket', 'blanket-fl');
    await settle(p);
    await over('今あるまとまり');
    await p.goto(B + '/new?kind=variation');
    await newGroup(p, { code: 'phone-bad', name: 'x', h: 'カラー', hl: 'ホワイト / WH\nグレー / -GY\nチャコール / -gy' });
    await settle(p);
    await over('誤り');
  }, { width: w, height: 844 }, 2);
}

console.log(`\n${passed} ok`);
await browser.close();
server.close();
