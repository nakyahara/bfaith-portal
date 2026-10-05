/**
 * test-master-edit-ui.mjs — マスタの入力の新しい画面 (一覧・1 つの商品) の JS を、描いたページで本当に動かす試験 (#1589 Codex R1 M4)
 *
 * Company DB = PGlite (切替の段階まで本物の関数で進める)・本物の router を express に載せ、Playwright (Chromium) で開く。保存も本当に流す。
 * 確かめること:
 *   1 未保存の数 = data-dirty-field の欄だけ (構成の見せ方の切り替え・保存の理由は 0 件・保存する欄を変えると 1 件)
 *   2 離れるときの確認: 左の列のリンク・全体から探す・ブラウザの戻る。「ここに残る」で押した所へフォーカスが戻る・「捨てて移る」で移る
 *   3 Ctrl+S で保存 (本当に DB に入る)・保存の後は離れても聞かない
 *   4 通信が切れた保存の押し直しは同じ request_id (2 回入らない)
 *   5 開き直しが要る 409 (その間の変更) の後は、欄を触っても保存のボタンが戻らない
 *   6 構成の行を並べ替えると、読み上げの名前が今の行番号・コードになる
 *   7 1280×720 の 100 / 125 / 150% (= 幅 1280 / 1024 / 853) と 1024×768 で、ページが横にはみ出さない (セットの構成の表は囲いの中で横に送る)
 *   8 JAN の欄に打ったまま (Enter を押さずに) ほかの欄も変えて Ctrl+S = JAN も保存する / 形の違う JAN なら保存しない (#1589 Codex R2 M1)
 *   9 画面を開いた後に登録をやめた (cancelled_sku) = 開き直しが要る (欄を触っても保存のボタンが戻らない)・やめた商品は初めから見るだけ (M3)
 *  10 先の日付の原価がある = 該当する原価の欄だけ閉じる (ほかの欄は保存できる) (M2)
 *  11 保存が通ったら画面を読み直す (10/5): 読み直すまでは入力の場所を閉じる (#1589 Codex R3 M1)・欄と見出しは保存した後の値・上に知らせが 1 回・続けて直せる・戻る 1 回で一覧
 *  12 変わった項目が無い保存 (5 と 5.0) の後は未保存が残らない (R3 L3)
 *  13 登録をやめた商品はカードの操作 (もう一度作る・結ぶ) も出さない (R3 M2・画面だけ。API の拒否は master からある穴 = 別 PR)
 *  14 絞る欄の Enter = 絞る・IME の変換を確かめる Enter では送らない・表の行の Enter = 開く・パンくずで同じ絞り込みへ戻る (10/5)
 *  15 かなの同一視 (ひらがな・カタカナ・半角カナ・全角英数・大文字小文字) = 一覧・全体から探す・画面とサーバーの決まりが同じ (10/5)
 *  16 原価を変える理由 = 選ぶ (既定 = メーカーからの値上げ通知)・その他は書かないと保存できない・記録に残る (10/5)
 *  17 1440 / 1280 / 1024 幅: 一覧をスクロールしても見出しの行が上の帯の下に見えている・横に送っても列がずれない (10/5)
 *  18 詳細検索: 商品コードを複数 (貼り付け・大文字・全角) = ぴったり・見つからないコード・URL に残る・クリア (10/5)
 *  19 詳細検索の項目: JAN・仕入先・代表 (親)・商品名・原価 / 売価の範囲・税率・売上分類・取扱区分 (10/5)
 *  20 注文残 (発注アプリ)・在庫 (ロジザード): 一覧の列・絞り込み・1 つの商品の画面の内訳と時刻・セットは作れる数・古い / 読めない (10/5)
 *  21 在庫の範囲はセットなら作れる数で絞る (#1620 Codex R1 M1)
 *  22 注文残は発注アプリの利用権がある人だけ (列・絞り込み・内訳) (#1620 Codex R1 M3)
 *  23 詳細検索の長い条件 = 本物の HTTP で 500 件の境目・印 (?s=)・GET なら 431・Origin と JSON の守り・期限切れ (#1620 Codex R1 M2)
 *  24 FBA (JP) の在庫 (参考・Company DB の在庫の日次): 一覧の列と見出しの時刻・1 × 1 の出品だけ・「—」と 0・単品の画面の内訳とまとめ売り / セットの出品・古い (26 時間) / 読めない (権限なし)・流し直しで戻る (10/5)
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 を付けたときだけ (#1589 Codex R2 M4 = 成功と見分けがつかないので黙って飛ばさない)
 * 使い方: node scripts/test-master-edit-ui.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の JS の試験 (test-master-edit-ui) を飛ばす'); process.exit(0); }
let chromium;
let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の JS の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / この試験だけ飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
// 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const { seedActiveEpoch } = await import('./fixtures/master-epoch.mjs');
// 発注アプリの台帳・ロジザードの写し (warehouse-mirror.db) = 使い捨ての DATA_DIR (router を読み込む前に。warehouse-mirror/db.js は読み込んだ時に DATA_DIR を決める)
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'meux-ui-'));
process.env.DATA_DIR = DATA_TMP;
const { default: router, __setPgClientFactory, __setOwnership, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');
const { __clearStockCache } = await import('../apps/master-edit/extras.mjs');
const { __clearSearchTokens } = await import('../apps/master-edit/search-token.mjs');

const quiet = () => {};
const ALL = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }], ['S02', { method: '宅急便', cost: 520 }]]);
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }] };
const BUILDS = { render: ['r1'], minipc: ['m1'] };

// ── Company DB (PGlite) ──
const pg = new PGlite();
const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
const sku = (code, name, kind, taxRate, salesClass, cost) => ({
  code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : 'STANDARD_10', handling: 'active', salesClass,
  cost: { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2,
});
const singles = [sku('s001', '単品 1', 'single', 0.1, 3, 100), sku('s002', '単品 2 (長い名前の試験: 国産 有機 シリコン 保存袋 Sサイズ 3枚入 まとめ買い 12 個セット)', 'single', 0.1, 3, 200), sku('s003', '単品 3', 'single', 0.08, 1, 50),
  // 長い一覧 (見出しの行を上に残す試験) と かなの同一視の試験の名前 (10/5)
  sku('k001', '国産 はちみつ 500g', 'single', 0.08, 1, 300), sku('k002', 'ハチミツ レモン', 'single', 0.08, 1, 310), sku('k003', 'ﾊﾁﾐﾂ ｷｬﾝﾃﾞｨ', 'single', 0.08, 1, 120),
  sku('k004', 'ＡＢＣ 保存袋', 'single', 0.1, 3, 90), sku('k005', 'abc 小袋', 'single', 0.1, 3, 80), sku('k006', 'みかん', 'single', 0.08, 1, 70),
  ...Array.from({ length: 40 }, (_, i) => sku(`z${String(i).padStart(3, '0')}`, `一覧を長くする単品 ${i}`, 'single', 0.1, 3, 100 + i))];
const lr = await runInitialLoad(db, {
  skus: [...singles, sku('set001', 'セット 1', 'set', 0.1, null, 300)], variationGroups: [{ code: 'k001', name: '国産 はちみつ', childCodes: ['k002'], status: 'active' }],
  setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: singles.map((s) => ({ supplierCode: '0001', skuCode: s.code })), primarySuppliers: singles.map((s) => ({ skuCode: s.code, supplierCode: '0001' })),
  reorder: { available: true, runId: 'pml_ui' },
}, { log: quiet, runId: 'load_ui', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
async function as(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
async function asGate(host, fn) { await pg.query(`set session authorization master_gate_${host}`); try { return await fn(); } finally { await pg.query(`set session authorization ${sessionUser}`); await pg.query('set role deploy'); } }
async function toPhase(to) {
  const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
  const own = to === 'frozen' ? MASTER_OWNERSHIP : ALL;
  for (const [host, inst, buildId] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await asGate(host, () => C.recordLegacyGateAck(db, { host, instanceId: inst, buildId, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
  const mh = await C.manifestHashOf(db, MANIFEST);
  if (to !== 'frozen') await seedActiveEpoch(db, ALL);
  const now = new Date().toISOString();
  const evidence = to === 'frozen'
    ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(MASTER_OWNERSHIP), manual_entries_stopped: [{ id: 'ne.product_screen', by: 'naka@test', at: now }], drain: { done: true, checked_by: 'naka@test', checked_at: now } }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL) };
  return as('master_ops', () => C.advanceCutoverPhase(db, { to, actor: 'naka@test', evidence }));
}
await toPhase('frozen');
const bp = (await pg.query('select * from ops.registration_backfill_plan()')).rows[0];
await as('master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [bp.sku_count, bp.snapshot_hash, 'naka@test']));
await toPhase('company_owner');
await toPhase('new_open');
// ── 発注アプリの台帳 (注文残) とロジザードの写し (在庫) の見本 (PR2) ──
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const mirrorDb = initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
{
  const now = new Date().toISOString();
  mirrorDb.prepare(`insert into po_settings (key, value, effective_at) values ('tracking_started_at', '2026-07-13T00:00:00.000Z', ?)`).run(now);
  const po = (sup, name, issuedAt, poNo, items) => {
    const id = mirrorDb.prepare(`insert into po_orders (supplier_code, supplier_name, status, created_at, updated_at) values (?, ?, 'draft', ?, ?)`).run(sup, name, now, now).lastInsertRowid;
    const ids = items.map((it) => mirrorDb.prepare('insert into po_order_items (order_id, product_code, product_key, product_name, qty, promised_date) values (?, ?, ?, ?, ?, ?)')
      .run(id, it.code, it.code.trim().toLowerCase(), it.code, it.qty, it.promised || null).lastInsertRowid);
    mirrorDb.prepare(`update po_orders set status = 'issued', issued_at = ?, po_number = ?, tracking_mode = 'tracked' where id = ?`).run(issuedAt, poNo, id);
    return ids;
  };
  const [k001Item] = po('0001', 'AMC', '2026-09-20T01:00:00.000Z', 'PO-2026-0001', [{ code: 'K001', qty: 10 }, { code: 's002', qty: 4, promised: '2026-10-20' }]);
  // 一部取消 3 = 残 7
  mirrorDb.prepare(`insert into po_item_events (order_item_id, event_type, qty, effective_date, recorded_at, actor_type) values (?, 'cancel', 3, '2026-09-25', '2026-09-25T00:00:00.000Z', 'user')`).run(k001Item);
  po('0001', 'AMC', '2020-01-01T00:00:00.000Z', 'PO-2020-0001', [{ code: 'k003', qty: 50 }]);   // 境界より前 = 数えない
  const lz = mirrorDb.prepare(`insert into mirror_logizard_stock (商品ID, 商品名, ブロック略称, ロケ, 品質区分名, 在庫数, 引当数, captured_at, synced_at) values (?, ?, ?, ?, ?, ?, 0, ?, ?)`);
  const cap = new Date(Date.now() - 20 * 60e3).toISOString();
  for (const [code, block, loke, q, n] of [['K001', 'P', 'A-01', '良品', 12], ['k001', 'P', 'A-02', '不良', 3], ['s001', 'P', 'B-01', '良品', 8], ['S001', 'R', 'Z-01', '良品', 2], ['s002', 'P', 'B-02', '良品', 5]]) lz.run(code, code, block, loke, q, n, cap, now);
}
// ── FBA (JP) の在庫の日次の見本 (Company DB・10/5): k001 × 1 の出品 2 つ・k001 × 3 (まとめ売り)・k001 + k002 (セットの出品)・k006 × 1 (在庫 0)。k002 だけの出品は無い ──
const { ingestStockDay } = await import('../apps/company-db/ingest/stock-daily.mjs');
const FBA_CAPTURED = new Date(Date.now() - 3 * 3600e3).toISOString();
{
  const sid = async (code) => Number((await pg.query('select sku_id from core.skus where code = $1', [code])).rows[0].sku_id);
  const k1 = await sid('k001'), k2 = await sid('k002'), k6 = await sid('k006');
  const listing = async (code, comps) => {
    const id = (await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', $1, 'active') returning listing_id`, [code])).rows[0].listing_id;
    let i = 0;
    for (const [skuId, qty] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, $3, $4, 'exact', 'system')`, [id, skuId, qty, i++]);
  };
  await listing('pr-k001', [[k1, 1]]); await listing('pr-k001-b', [[k1, 1]]); await listing('pr-k001-3p', [[k1, 3]]); await listing('pr-k1k2', [[k1, 1], [k2, 1]]); await listing('pr-k006', [[k6, 1]]);
  const r = (code, a, x, pr, c, w = 0) => ({ code, fba_available: a, fba_fc_transfer: x, fba_fc_processing: pr, fba_customer_order: c, fba_inbound_working: w, fba_inbound_shipped: 0, fba_inbound_received: 0 });
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  const got = await ingestStockDay(db, { source: 'fba_jp', snapshot_date: today, captured_at: FBA_CAPTURED,
    rows: [r('pr-k001', 20, 1, 0, 2, 5), r('pr-k001-b', 4, 0, 1, 0), r('pr-k001-3p', 6, 0, 0, 0), r('pr-k1k2', 3, 0, 0, 0), r('pr-k006', 0, 0, 0, 0)] }, { todayJst: today });
  assert.equal(got.day_status, 'complete');
}
const row = async (code) => (await pg.query('select s.name, s.reorder_months::float8 as months, s.handling from core.skus s where s.code = $1', [code])).rows[0];
/** 画面の外で同じ商品を直す (その間の変更 = 画面の編集の印が古くなる) */
async function changeBehind(code, values) {
  const id = (await pg.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0].id;
  const token = W.editTokenOf(await W.readCurrent(db, id, W.jstDate(new Date())));
  return as('master_edit', () => W.saveSku(db, { actor: 'other@test', requestId: crypto.randomUUID(), code, reason: '画面の外で', seen: { token }, values }, { ownership: ALL, open: true, shippingRates: RATES }));
}

// ── 本物の router ──
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/ui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/ui';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  // 1 つの PGlite を順番に使う (同時の要求でロールが混ざらない)
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
__setOwnership(ALL);
__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
// 利用権 (試験の途中で変える: 発注アプリの利用権が無い人には注文残を出さない = #1620 Codex R1 M3)
let SESSION_APPS = ['master-edit', 'purchase-orders'];
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: SESSION_APPS }; next(); });
app.use('/apps/master-edit', router);
app.get('/', (req, res) => res.send('<p>portal</p>'));
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;

let passed = 0;
async function ta(name, fn, viewport = { width: 1440, height: 900 }, scale = 1) {
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: scale, locale: 'ja-JP' });
  const p = await ctx.newPage();
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  try {
    await fn(p, ctx);
    assert.deepEqual(errors, [], '画面の JS の例外');
    passed++; console.log(`  ok  ${name}`);
  } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
  await ctx.close();
}
const dirty = (p) => p.evaluate(() => window.MasterEdit.dirty().n);
const active = (p) => p.evaluate(() => document.activeElement && (document.activeElement.id || document.activeElement.getAttribute('aria-label') || document.activeElement.tagName));

await ta('[1] 未保存の数: 構成の見せ方・保存の理由は 0 件、保存する欄を変えると 1 件 (保存のボタンもそれに合わせる)', async (p) => {
  await p.goto(B + '/sku/set001');
  assert.ok((await p.content()).includes('セット 1'), '描けている');
  await p.click('#set-mode button[data-v="edit"]');
  await p.click('#set-mode button[data-v="now"]');
  await p.fill('#reason', '理由だけ入れた');
  assert.equal(await dirty(p), 0);
  assert.equal(await p.isDisabled('#save'), true);
  assert.equal(await p.isHidden('#unsaved'), true);
  await p.fill('#f-name', 'セット 1 改');
  assert.equal(await dirty(p), 1);
  assert.equal(await p.isDisabled('#save'), false);
  assert.match(await p.textContent('#unsaved'), /未保存 1 件/);
  await p.fill('#f-name', 'セット 1');
  assert.equal(await dirty(p), 0);
});

await ta('[2] 離れるときの確認: 左の列のリンク (ここに残る = フォーカスが戻る)・全体から探す・ブラウザの戻る (捨てて移る)', async (p) => {
  await p.goto(B + '/');
  await Promise.all([p.waitForNavigation(), p.click('a.rowlink:has-text("s002")')]);
  await p.fill('#f-reorder_months', '4');
  await p.focus('.rail a[aria-label="つかいかた"]');
  await p.keyboard.press('Enter');
  assert.equal(await p.locator('#leave-bg.on').count(), 1);
  assert.match(await p.textContent('#leave-list'), /推奨保有月数/);
  assert.equal(await active(p), 'leave-stay');
  for (let i = 0; i < 3; i++) await p.keyboard.press('Tab');
  assert.equal(await active(p), 'leave-stay', 'Tab は窓の中で回る');
  await p.click('#leave-stay');
  assert.equal(await active(p), 'つかいかた', '「ここに残る」で押した所へ戻る');
  assert.match(p.url(), /sku\/s002$/);
  await p.keyboard.press('Control+k');
  await p.keyboard.type('つかいかた');
  await p.keyboard.press('Enter');
  assert.equal(await p.locator('#leave-bg.on').count(), 1, '全体から探すからの移動も聞く');
  await p.click('#leave-stay');
  await p.goBack();
  await p.waitForTimeout(200);
  assert.equal(await p.locator('#leave-bg.on').count(), 1, 'ブラウザの戻るも聞く');
  assert.match(p.url(), /sku\/s002$/);
  await Promise.all([p.waitForNavigation(), p.click('#leave-drop')]);
  assert.match(p.url(), /master-edit\/$/);
  assert.equal((await row('s002')).months, 2, '捨てたので保存していない');
});

await ta('[3] Ctrl+S で保存 (DB に入る)・保存の後は離れても聞かない・/ は絞る欄の無い画面では何もしない', async (p) => {
  await p.goto(B + '/sku/s003');
  await p.keyboard.press('/');
  assert.equal(await p.locator('#palette-bg.on').count(), 0);
  await p.fill('#f-reorder_months', '5');
  await p.fill('#reason', 'Ctrl+S の試験');
  await p.keyboard.press('Control+s');
  await p.waitForSelector('.result.ok');
  assert.equal((await row('s003')).months, 5);
  assert.equal(await dirty(p), 0);
  await Promise.all([p.waitForNavigation(), p.click('.rail a[aria-label="つかいかた"]')]);
});

await ta('[4] 通信が切れた保存の押し直しは同じ request_id (2 回入らない)', async (p) => {
  await p.goto(B + '/sku/s001');
  const bodies = [];
  let first = true;
  await p.route('**/api/sku/s001', async (route) => {
    bodies.push(JSON.parse(route.request().postData()));
    if (first) { first = false; await route.abort('connectionreset'); } else await route.continue();
  });
  await p.fill('#f-reorder_months', '3');
  await p.click('#save');
  await p.waitForSelector('#msg.err');
  assert.match(await p.textContent('#msg'), /通信できませんでした/);
  assert.equal(await p.isDisabled('#save'), false);
  await p.click('#save');
  await p.waitForSelector('.result.ok');
  assert.equal(bodies.length, 2);
  assert.equal(bodies[0].request_id, bodies[1].request_id);
  assert.match(bodies[0].request_id, /^[0-9a-f-]{36}$/);
  assert.equal((await row('s001')).months, 3);
});

await ta('[5] 開き直しが要る 409 (その間の変更) の後は、欄や理由を触っても保存のボタンが戻らない・Ctrl+S も保存しない', async (p) => {
  await p.goto(B + '/sku/s002');
  await changeBehind('s002', { name: '画面の外で直した' });
  await p.fill('#f-reorder_months', '6');
  await p.click('#save');
  await p.waitForSelector('.result.err');
  assert.match(await p.textContent('.result.err'), /その間の変更/);
  assert.equal(await p.locator('#reload').count(), 1);
  assert.equal(await p.isDisabled('#save'), true);
  await p.fill('#reason', '触った');
  await p.fill('#f-reorder_months', '7');
  assert.equal(await p.isDisabled('#save'), true, '欄を触っても戻らない');
  await p.keyboard.press('Control+s');
  await p.waitForTimeout(200);
  assert.equal((await row('s002')).months, 2, '保存していない');
  await Promise.all([p.waitForNavigation(), p.click('#reload')]);
  assert.match(await p.inputValue('#f-name'), /画面の外で直した/);
});

await ta('[6] 構成の行を並べ替えると、読み上げの名前が今の行番号・コードになる', async (p) => {
  await p.goto(B + '/sku/set001');
  await p.click('#set-mode button[data-v="edit"]');
  const rows = p.locator('#comp-rows tr.comp-row');
  assert.equal(await rows.nth(1).locator('.c-qty').getAttribute('aria-label'), '2 行目 (s002) の数');
  await rows.nth(1).locator('button[data-act="up"]').click();
  assert.equal(await rows.nth(0).locator('.c-qty').getAttribute('aria-label'), '1 行目 (s002) の数');
  assert.equal(await rows.nth(0).locator('button[data-act="del"]').getAttribute('aria-label'), '1 行目 (s002) を外す');
  assert.equal(await rows.nth(1).locator('button[data-act="up"]').getAttribute('aria-label'), '2 行目 (s001) を上へ');
  assert.equal(await dirty(p), 1, '並びも依頼の中身');
});

const jan13 = (b) => { const d = b.split('').map(Number).reverse(); const sum = d.reduce((a, x, i) => a + x * (i % 2 === 0 ? 3 : 1), 0); return b + ((10 - (sum % 10)) % 10); };
const jansOf = async (code) => (await pg.query(`select e.external_value as v from core.external_ids e join core.skus s on s.product_id = e.entity_id
   where s.code = $1 and e.entity_type = 'product' and e.system = 'jan' and e.valid_to is null order by 1`, [code])).rows.map((r) => r.v);

await ta('[8] JAN の欄に打ったまま (Enter なし) ほかの欄も変えて Ctrl+S = JAN も保存する / 形の違う JAN なら何も保存しない', async (p) => {
  const good = jan13('490000000777');
  await p.goto(B + '/sku/s003');
  await p.fill('#f-reorder_months', '7');
  await p.fill('#jan-in', '1234567');
  assert.equal(await dirty(p), 2, '打ったままの JAN も未保存に数える');
  await p.keyboard.press('Control+s');
  await p.waitForTimeout(300);
  assert.match(await p.textContent('#msg'), /JAN の欄を直してから保存/);
  assert.equal(await active(p), 'jan-in');
  assert.equal((await row('s003')).months, 5, '形の違う JAN のときは何も保存しない');
  await p.fill('#jan-in', good);
  await p.keyboard.press('Control+s');
  await p.waitForSelector('.result.ok');
  assert.equal((await row('s003')).months, 7);
  assert.deepEqual(await jansOf('s003'), [good], 'JAN も保存した');
  assert.equal(await dirty(p), 0);
});

/** 試験だけ: 登録の状態を直に変える (本番は ops.transition_sku_registration の決まった進み方だけ。表の守りの印を同じ取引の中で立てる) */
async function setReg(code, state) {
  await pg.query('begin');
  try {
    await pg.query("select pg_catalog.set_config('ops.registration_protocol', '1', true)");
    await pg.query('update ops.master_registrations set state = $2, state_changed_at = now() where sku_id = (select sku_id from core.skus where code = $1)', [code, state]);
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
}

await ta('[9] 開いた後に登録をやめた = cancelled_sku は開き直しが要る / やめた商品は初めから見るだけ (帯・保存のボタンなし)', async (p) => {
  await p.goto(B + '/sku/s001');
  await setReg('s001', 'cancelled');
  try {
    await p.fill('#f-reorder_months', '9');
    await p.click('#save');
    await p.waitForSelector('.result.err');
    assert.equal(await p.locator('#reload').count(), 1, '開き直すボタン');
    await p.fill('#reason', '触った');
    assert.equal(await p.isDisabled('#save'), true, '欄を触っても保存のボタンが戻らない');
    await Promise.all([p.waitForNavigation(), p.click('#reload')]);
    assert.equal(await p.locator('#cancelled-band').count(), 1, 'やめた = 理由の帯');
    assert.equal(await p.locator('#save').count(), 0, 'やめた = 保存のボタンを出さない');
    assert.equal(await p.locator('#f-name').count(), 0, 'やめた = 入力欄を出さない');
  } finally {
    await setReg('s001', 'available');
  }
});

await ta('[10] 先の日付の原価 (使っているセット) = 原価の欄だけ閉じる・ほかの欄は保存できる', async (p) => {
  const fut = new Date(Date.now() + 40 * 86400e3 + 9 * 3600e3).toISOString().slice(0, 10);
  await pg.query("update core.sku_costs set valid_to = $1::date - 1 where valid_to is null and sku_id = (select sku_id from core.skus where code = 'set001')", [fut]);
  await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 999, 'set_calc', 'COMPLETE', $1::date from core.skus where code = 'set001'", [fut]);
  try {
    await p.goto(B + '/sku/s002');
    assert.equal(await p.locator('#btn-cost-open').count(), 0, '原価を変えるボタンを出さない');
    assert.match(await p.textContent('#cost-future'), /set001/);
    await p.fill('#f-reorder_months', '8');
    await p.click('#save');
    await p.waitForSelector('.result.ok');
    assert.equal((await row('s002')).months, 8);
  } finally {
    await pg.query("delete from core.sku_costs where valid_from = $1::date and sku_id = (select sku_id from core.skus where code = 'set001')", [fut]);
    await pg.query("update core.sku_costs set valid_to = null where valid_to = $1::date - 1 and sku_id = (select sku_id from core.skus where code = 'set001')", [fut]);
  }
});

await ta('[11] 保存が通ったら画面を読み直す (10/5): 読み直すまでは打てない・欄は保存した後の値・上に知らせが 1 回・続けて別の欄を直して保存できる・戻る 1 回で一覧へ', async (p) => {
  await p.goto(B + '/');
  await Promise.all([p.waitForNavigation(), p.click('a.rowlink:has-text("s003")')]);
  await p.fill('#f-reorder_months', '6');
  // 読み直しを一度止めて (ME.reloadPage を差し替え)、読み直すまでは入力の場所が閉じていることを見る
  await p.evaluate(() => { window.__reload = window.MasterEdit.reloadPage; window.MasterEdit.reloadPage = () => { window.__reloadAsked = true; }; });
  await p.click('#save');
  await p.waitForFunction(() => window.__reloadAsked === true);
  assert.match(await p.textContent('#msg'), /読み直しています/);
  for (const sel of ['#f-name', '#f-reorder_months', '#jan-in', '#reason', '.handling-top .seg button']) assert.equal(await p.locator(sel).first().isDisabled(), true, `${sel} は読み直すまで閉じる`);
  assert.equal(await p.evaluate(() => document.getElementById('f').firstElementChild.hasAttribute('inert')), true);
  assert.equal(await dirty(p), 0, '保存した = 未保存 0 (離れても聞かない)');
  await p.evaluate(() => window.__reload());
  await p.waitForSelector('#saved-note');
  assert.equal(await p.inputValue('#f-reorder_months'), '6', '欄は保存した後の値');
  assert.match(await p.textContent('#saved-note'), /保存しました[\s\S]*推奨保有月数/);
  assert.match(await p.textContent('#toast-t'), /保存しました \(推奨保有月数/);
  assert.equal((await row('s003')).months, 6);
  assert.equal(await dirty(p), 0);
  assert.equal(await p.isDisabled('#f-name'), false, '読み直した後は打てる');
  await p.reload();
  assert.equal(await p.locator('#saved-note').count(), 0, '知らせは 1 回だけ');
  // 続けて直す (読み直しで新しい編集の印)
  await p.fill('#f-name', '単品 3 改');
  await p.keyboard.press('Control+s');
  await p.waitForSelector('#saved-note');
  assert.equal(await p.inputValue('#f-name'), '単品 3 改');
  assert.equal(await p.textContent('h1'), '単品 3 改', '見出しも保存した後の値');
  assert.equal((await row('s003')).name, '単品 3 改');
  await p.goBack();
  await p.waitForSelector('#list-tbl');
  assert.match(p.url(), /master-edit\/$/, '戻る 1 回で一覧 (同じ画面が履歴に 2 つ並ばない)');
});

await ta('[12] 変わった項目が無い保存 (6 → 6.0) の後は未保存が残らない (保存のボタン・離れるときの確認も)', async (p) => {
  await p.goto(B + '/sku/s003');
  await p.fill('#f-reorder_months', '6.0');
  assert.equal(await dirty(p), 1);
  await p.click('#save');
  await p.waitForSelector('.result');
  assert.match(await p.textContent('.result'), /変わった項目がありません/);
  assert.equal(await dirty(p), 0);
  assert.equal(await p.isDisabled('#save'), true);
  await Promise.all([p.waitForNavigation(), p.click('.rail a[aria-label="つかいかた"]')]);
});

await ta('[13] 登録をやめた商品は、カードの操作 (もう一度作る) も出さない', async (p) => {
  const MR = await import('../lib/master-register.mjs');
  await as('master_edit', () => MR.registerNewSku(db, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code: 'ui-card-1',
    values: { name: 'カードの試験', standard_price: '1500', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001' }, card: { create: true } },
  { ownership: ALL, open: true, shippingRates: RATES }));
  await p.goto(B + '/sku/ui-card-1');
  assert.equal(await p.locator('#card-retry').count(), 1, '下書きのうちは出す (名簿の人)');
  const id = (await pg.query("select sku_id::text as id from core.skus where code = 'ui-card-1'")).rows[0].id;
  await pg.query('select ops.transition_sku_registration($1, $2, $3, $4, $5)', [id, 'cancelled', 'human', 'naka@test', '試験でやめた']);
  await p.goto(B + '/sku/ui-card-1');
  assert.equal(await p.locator('#cancelled-band').count(), 1);
  assert.equal(await p.locator('#card-retry').count(), 0, 'やめた商品にカードの操作を出さない');
  assert.equal(await p.locator('#card-link').count(), 0);
});

const listCodes = (p) => p.$$eval('#list-tbl a.rowlink', (as) => as.map((a) => a.textContent.trim()));

await ta('[14] 絞る欄の Enter (10/5): 絞る欄で Enter = 絞る (URL の q)・IME の変換を確かめる Enter では送らない・↓ で表へ移った後の Enter = その行を開く・パンくずで同じ絞り込みの一覧へ戻る', async (p) => {
  await p.goto(B + '/');
  // 作った keydown (ブラウザ自身は送らない) で、画面の JS の決まりそのものを見る: 変換中 = 送らない / 変換でない = 送る
  await p.focus('#q');
  await p.fill('#q', 'みかん');
  await p.evaluate(() => document.getElementById('q').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', isComposing: true, keyCode: 229, bubbles: true, cancelable: true })));
  await p.waitForTimeout(300);
  assert.doesNotMatch(p.url(), /q=/, '変換を確かめる Enter (isComposing) では送らない');
  await p.evaluate(() => document.getElementById('q').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 229, bubbles: true, cancelable: true })));
  await p.waitForTimeout(300);
  assert.doesNotMatch(p.url(), /q=/, 'keyCode 229 (IME が受けた Enter) では送らない');
  await Promise.all([p.waitForNavigation(), p.evaluate(() => document.getElementById('q').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 13, bubbles: true, cancelable: true })))]);
  assert.equal(new URL(p.url()).searchParams.get('q'), 'みかん', '画面の JS の Enter で送る');
  assert.deepEqual(await listCodes(p), ['k006']);
  // 本物の IME (CDP): 変換中の Enter は字が確かまるだけ → もう一度 Enter で絞る
  await p.goto(B + '/');
  await p.click('#q');
  const cdp = await p.context().newCDPSession(p);
  await cdp.send('Input.imeSetComposition', { text: 'はちみつ', selectionStart: 4, selectionEnd: 4 });
  await cdp.send('Input.dispatchKeyEvent', { type: 'rawKeyDown', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 229, nativeVirtualKeyCode: 229 });
  await cdp.send('Input.insertText', { text: 'はちみつ' });
  await p.waitForTimeout(300);
  assert.doesNotMatch(p.url(), /q=/, 'IME の変換を確かめる Enter では送らない');
  assert.equal(await p.inputValue('#q'), 'はちみつ');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Enter')]);
  assert.equal(new URL(p.url()).searchParams.get('q'), 'はちみつ');
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003']);
  // ↓ で表の 1 行目へ・Enter = その行を開く (絞る欄の外)
  await p.focus('#q');
  await p.keyboard.press('ArrowDown');
  assert.equal(await p.evaluate(() => document.activeElement.textContent.trim()), 'k001');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Enter')]);
  assert.match(p.url(), /sku\/k001$/);
  // パンくずの「商品・セット」= さっきの絞り込みの一覧へ
  assert.equal(new URL(await p.locator('.crumb a').first().evaluate((a) => a.href)).searchParams.get('q'), 'はちみつ');
  await Promise.all([p.waitForNavigation(), p.click('.crumb a')]);
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003']);
});

await ta('[15] かなの同一視 (10/5): ひらがな・カタカナ・半角カナ・全角英数・大文字小文字が同じ商品に当たる (一覧・全体から探す)・画面とサーバーの決まりが同じ', async (p) => {
  for (const q of ['はちみつ', 'ハチミツ', 'ﾊﾁﾐﾂ', 'ﾊﾁみつ']) {
    await p.goto(B + '/?q=' + encodeURIComponent(q));
    assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003'], q);
  }
  for (const q of ['ＡＢＣ', 'abc', 'ABC', 'Ａｂｃ']) {
    await p.goto(B + '/?q=' + encodeURIComponent(q));
    assert.deepEqual(await listCodes(p), ['k004', 'k005'], q);
  }
  // 全体から探す (Ctrl+K) の「〜で商品・セットを絞る」も同じ一覧
  await p.goto(B + '/sku/s001');
  await p.keyboard.press('Control+k');
  await p.keyboard.type('ﾊﾁﾐﾂ');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Enter')]);
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003']);
  // 操作の候補もかなを同じに見る (「ツカイカタ」で「つかいかた」)
  await p.keyboard.press('Control+k');
  await p.keyboard.type('ツカイカタ');
  assert.match(await p.textContent('#pal-res'), /つかいかた/);
  await p.keyboard.press('Escape');
  // 画面の JS (ME.fold) とサーバー (search-fold.mjs) が同じ答え
  const { foldSearch } = await import('../apps/master-edit/search-fold.mjs');
  const samples = ['はちみつ', 'ﾊﾁﾐﾂ', 'ｶﾞｷﾞｸﾞ', 'ＡＢＣ１２３', 'ゔゝゞぁゖ', 'ラーメン', 'ｧｨｩ', 'Ｍｉｘ ミックス みっくす'];
  assert.deepEqual(await p.evaluate((xs) => xs.map((x) => window.MasterEdit.fold(x)), samples), samples.map(foldSearch));
});

await ta('[16] 原価を変える理由 (10/5): 既定 = メーカーからの値上げ通知で保存できる・その他で空なら保存できない・その他に書いた文が記録に残る', async (p) => {
  const costReason = async (code) => (await pg.query('select c.reason from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = $1 and c.valid_to is null', [code])).rows[0].reason;
  await p.goto(B + '/sku/k006');
  await p.click('#btn-cost-open');
  assert.equal(await p.isChecked('input[name="cost-reason-pick"][value="メーカーからの値上げ通知"]'), true, '最初から選ばれている');
  assert.equal(await p.isHidden('#cost-reason'), true, 'その他の欄は隠れている');
  await p.fill('#cost-jpy', '75');
  assert.equal(await dirty(p), 1, '理由の選択は未保存に数えない (原価だけ)');
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.equal(await costReason('k006'), 'メーカーからの値上げ通知');
  assert.match(await p.textContent('#sku-page'), /メーカーからの値上げ通知/, '原価の履歴に出る');
  // その他 = 書かないと保存できない
  await p.goto(B + '/sku/k005');
  await p.click('#btn-cost-open');
  await p.fill('#cost-jpy', '85');
  await p.check('input[name="cost-reason-pick"][data-other]');
  assert.equal(await p.isVisible('#cost-reason'), true, 'その他 = 書く欄が出る');
  assert.equal(await active(p), 'cost-reason', '書く欄へ');
  assert.equal(await p.isDisabled('#save'), true, 'その他で空 = 保存できない');
  await p.keyboard.press('Control+s');
  await p.waitForTimeout(200);
  assert.equal(await costReason('k005'), 'initial load load_ui', '保存していない (今の原価は取り込みの行のまま)');
  await p.fill('#cost-reason', '送料込みの仕入値に変わった');
  assert.equal(await p.isDisabled('#save'), false);
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.equal(await costReason('k005'), '送料込みの仕入値に変わった');
  assert.match(await p.textContent('#sku-page'), /送料込みの仕入値に変わった/, '原価の履歴に出る');
  // 選び直すと書く欄は隠れる
  await p.click('#btn-cost-open');
  await p.check('input[name="cost-reason-pick"][data-other]');
  await p.check('input[name="cost-reason-pick"][value="メーカーからの値下げ通知"]');
  assert.equal(await p.isHidden('#cost-reason'), true);
});

// ── 詳細検索・注文残・在庫 (PR2・10/5) ──
const SHOT2 = process.env.MASTER_EDIT_UI_SHOTS2 || '';
// 列の位置は見出しから (登録日の列 (0057) などで位置が変わっても同じ)
const cells = (p) => p.$eval('#list-tbl', (tbl) => {
  const h = [...tbl.querySelectorAll('thead th')].map((th) => th.textContent.trim());
  const iS = h.findIndex((x) => x.startsWith('在庫'));
  const iP = h.findIndex((x) => x.startsWith('注文残'));
  const iF = h.findIndex((x) => x.startsWith('FBA'));
  return [...tbl.querySelectorAll('tbody tr')].map((tr) => { const t = [...tr.children].map((td) => td.textContent.replace(/\s+/g, ' ').trim()); return { code: t[0], stock: iS < 0 ? undefined : t[iS], po: iP < 0 ? undefined : t[iP], fba: iF < 0 ? undefined : t[iF] }; });
});
const advGo = async (p, fill) => {
  await p.goto(B + '/');
  await p.click('#adv > summary');
  await fill();
  await Promise.all([p.waitForNavigation(), p.click('#adv-form button[type="submit"]')]);
};

await ta('[18] 詳細検索 (10/5): 商品コードを複数 (Excel の列の貼り付け・大文字・全角) = ぴったり・見つからないコードを上に出す・URL に残る・クリア', async (p) => {
  await advGo(p, () => p.fill('#adv-codes', 'K001\tｋ００２\r\nnope-1, s003\nNOPE-2\nk001'));
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 's003']);
  assert.match(await p.textContent('#not-found'), /2 件 見つからない[\s\S]*nope-1, NOPE-2/);
  const u = new URL(p.url());
  assert.match(u.searchParams.get('codes'), /NOPE-2/, '条件は URL に');
  assert.equal((await p.inputValue('#adv-codes')).split('\n').length, 6, '貼り付けた値は 1 行 1 つに直して欄に残る');
  assert.equal(await p.getAttribute('#adv', 'open'), '', '詳細検索を使っている = 板を開いておく');
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/詳細検索_商品コードを複数.png`, fullPage: true });
  await p.reload();
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 's003'], '戻る・開き直すで同じ結果');
  // 札・絞る欄と一緒に使える (区分の札を押しても詳細検索の条件は残る)
  await Promise.all([p.waitForNavigation(), p.click('.chips a.chip:has-text("単品")')]);
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 's003']);
  await Promise.all([p.waitForNavigation(), p.click('#adv-form a:has-text("クリア")')]);
  assert.equal(new URL(p.url()).search, '');
  assert.ok((await listCodes(p)).length > 40, 'クリア = 全部');
  assert.equal(await p.locator('#not-found').count(), 0);
});

await ta('[19] 詳細検索の項目 (10/5): JAN (複数)・仕入先 (0001 と 1)・代表 (親)・商品名 (かな)・原価 / 売価の範囲・税率・売上分類・取扱区分', async (p) => {
  const q = async (qs) => { await p.goto(B + '/?' + new URLSearchParams(qs)); return listCodes(p); };
  assert.deepEqual(await q({ jans: `0000000\n${jan13('490000000777')}` }), ['s003']);
  const bySup = await q({ sups: '1', kind: 'single' });
  assert.ok(bySup.includes('k001') && bySup.includes('s001') && !bySup.includes('set001'), '代表の仕入先 0001 (1 と書いても同じ)');
  assert.deepEqual(await q({ sups: '9999' }), []);
  assert.deepEqual(await q({ parents: 'K001' }), ['k001', 'k002'], '代表 k001 = 子 k002 と代表そのもの');
  assert.deepEqual(await q({ name: 'ﾊﾁﾐﾂ' }), ['k001', 'k002', 'k003']);
  assert.deepEqual(await q({ cost_min: '305', cost_max: '310' }), ['k002']);
  assert.deepEqual(await q({ price_min: '1,200' }), ['ui-card-1'], '売価の範囲 (カンマ付きでも)');
  assert.deepEqual(await q({ name: 'はちみつ', tax: '8' }), ['k001', 'k002', 'k003']);
  assert.deepEqual(await q({ name: 'はちみつ', tax: '10' }), []);
  assert.deepEqual(await q({ name: 'はちみつ', sales: '1' }), ['k001', 'k002', 'k003']);
  assert.deepEqual(await q({ name: 'はちみつ', sales: '3' }), []);
  assert.deepEqual(await q({ codes: 'set001\ns001', sales: '3' }), ['s001', 'set001'], 'セットの売上分類は構成品から導いて絞る');
  assert.deepEqual(await q({ codes: 'k001\nk002', state: 'discontinued' }), []);
  // 画面の部品から: 選ぶ欄・範囲
  await advGo(p, async () => { await p.fill('#adv-name', 'はちみつ'); await p.selectOption('#adv-tax', '8'); await p.fill('input[name="cost_max"]', '200'); });
  assert.deepEqual(await listCodes(p), ['k003']);
});

await ta('[20] 注文残 (発注アプリ)・在庫 (ロジザード) (10/5): 一覧の列・「注文残あり」と在庫の範囲で絞る・1 つの商品の画面の内訳と「いつの写しか」・セットは作れる数・古い / 読めない', async (p) => {
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\ns001\ns002\nset001\nk006' }));
  const c = Object.fromEntries((await cells(p)).map((x) => [x.code, x]));
  assert.deepEqual([c.k001.stock, c.k001.po], ['15', '7'], 'k001: 在庫 = 全部のロケ・品質区分の合計 (大文字の商品ID も同じ)・注文残 = 10 − 取消 3');
  assert.deepEqual([c.s001.stock, c.s001.po], ['10', ''], '注文残 0 = 空');
  assert.deepEqual([c.s002.stock, c.s002.po], ['5', '4']);
  assert.equal(c.set001.stock, '5作れる', 'セット = 構成品から作れる数 (s001 10 ÷ 1・s002 5 ÷ 1 の小さい方)');
  assert.deepEqual([c.k006.stock, c.k006.po], ['0', ''], '写しに無い = 0');
  const th = await p.textContent('#list-tbl thead');
  assert.match(th, /在庫\d{2}:\d{2}/, '見出しに写しの時刻');
  assert.doesNotMatch(th, /古い|読めない/);
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/一覧_在庫と注文残の列.png`, fullPage: true });
  const q = async (qs) => { await p.goto(B + '/?' + new URLSearchParams(qs)); return listCodes(p); };
  assert.deepEqual(await q({ po: '1' }), ['k001', 's002'], '注文残あり (境界より前の発注は入らない)');
  const st10 = await q({ stock_min: '10', kind: 'single' });
  assert.deepEqual(st10, ['k001', 's001']);
  assert.deepEqual(await q({ stock_max: '0', name: 'みかん' }), ['k006'], '在庫 0 (写しに無い) も範囲に入る');
  // 1 つの商品の画面
  await p.goto(B + '/sku/k001');
  assert.equal(await p.textContent('#ref-stock'), '15');
  assert.match(await p.textContent('#ref-stock-when'), /時点/);
  assert.doesNotMatch(await p.textContent('#ref-stock-when'), /古い/);
  assert.equal(await p.textContent('#ref-po'), '7');
  const lines = await p.textContent('#ref-po-lines');
  assert.match(lines, /AMC/); assert.match(lines, /未定/);
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/単品_在庫と注文残.png`, fullPage: true });
  await p.goto(B + '/sku/s002');
  assert.match(await p.textContent('#ref-po-lines'), /10\/20 \(火\) 回答/);
  await p.goto(B + '/sku/set001');
  assert.equal(await p.textContent('#ref-stock'), '5');
  assert.match(await p.textContent('#ref-box'), /作れる数/);
  assert.equal(await p.locator('#ref-po').count(), 0, 'セットは注文残を出さない (発注は単品)');
  // 古い (2 時間より前)
  mirrorDb.prepare('update mirror_logizard_stock set captured_at = ?').run(new Date(Date.now() - 3 * 3600e3).toISOString());
  __clearStockCache();
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
  assert.match(await p.textContent('#list-tbl thead'), /古い/);
  await p.goto(B + '/sku/k001');
  assert.match(await p.textContent('#ref-stock-when'), /古い/);
  // 読めない (写しが無い)
  const keep = mirrorDb.prepare('select * from mirror_logizard_stock').all();
  mirrorDb.prepare('delete from mirror_logizard_stock').run();
  __clearStockCache();
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
  assert.match(await p.textContent('#list-tbl thead'), /読めない/);
  assert.equal((await cells(p))[0].stock, '—');
  assert.deepEqual(await q({ stock_min: '1' }), [], '読めないときは在庫の範囲で当てない');
  await p.goto(B + '/sku/k001');
  assert.match(await p.textContent('#ref-stock-when'), /読めない/);
  const ins = mirrorDb.prepare(`insert into mirror_logizard_stock (${Object.keys(keep[0]).map((k) => `"${k}"`).join(', ')}) values (${Object.keys(keep[0]).map(() => '?').join(', ')})`);
  for (const r of keep) ins.run(...Object.values(r));
  mirrorDb.prepare('update mirror_logizard_stock set captured_at = ?').run(new Date(Date.now() - 20 * 60e3).toISOString());
  __clearStockCache();
});

await ta('[21] 在庫の範囲で絞る = 一覧に出す値 (セットは作れる数) で絞る (#1620 Codex R1 M1)', async (p) => {
  const q = async (qs) => { await p.goto(B + '/?' + new URLSearchParams(qs)); return listCodes(p); };
  // set001 は構成品 s001 (10) と s002 (5) から 5 作れる (セット自身の行はロジザードに無い)
  assert.deepEqual(await q({ codes: 'set001', stock_min: '1' }), ['set001'], '作れる数 5 は 1 以上に入る');
  assert.deepEqual(await q({ codes: 'set001', stock_max: '0' }), [], '作れる数 5 は 0 以下に入らない');
  assert.deepEqual(await q({ codes: 'set001\ns001\ns002', stock_min: '5', stock_max: '5' }), ['s002', 'set001'], '単品は在庫・セットは作れる数');
  assert.equal(await p.textContent('#list-tbl tbody tr:has-text("set001") td:nth-child(9)'), '5作れる');   // 9 番目 = 在庫 (4 番目に登録日の列・0057)
});

await ta('[22] 注文残は発注アプリの利用権がある人だけ (#1620 Codex R1 M3): 無い人 = 列・絞り込み・内訳を出さず「権限がないので出せません」', async (p) => {
  SESSION_APPS = ['master-edit'];
  try {
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\ns002' }));
    assert.doesNotMatch(await p.textContent('#list-tbl thead'), /注文残/, '列を出さない');
    assert.deepEqual((await cells(p)).map((x) => x.code), ['k001', 's002']);
    assert.equal(await p.locator('#list-tbl tbody tr').first().locator('td').count(), 11, '行の欄も 1 つ少ない (12 → 11。FBA (JP) の列を足した)');
    assert.equal(await p.locator('input[name="po"]').count(), 0, '「注文残あり」を出さない');
    assert.match(await p.textContent('#po-denied'), /注文残 \(発注アプリの権限がないので出せません\)/);
    // URL に po=1 を付けても、注文残のある商品だけに絞らない (どの商品に注文残があるかを出さない)
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\ns001\ns002', po: '1' }));
    assert.deepEqual(await listCodes(p), ['k001', 's001', 's002']);
    await p.goto(B + '/sku/k001');
    assert.match(await p.textContent('#ref-po-why'), /注文残 \(発注アプリの権限がないので出せません\)/);
    assert.equal(await p.locator('#ref-po').count(), 0);
    assert.equal(await p.locator('#ref-po-lines').count(), 0, '内訳 (仕入先・発注日・数・納期) を出さない');
    assert.doesNotMatch(await p.content(), /PO-2026-0001|AMC<\/td>/);
    assert.equal(await p.textContent('#ref-stock'), '15', '在庫 (ロジザード) は出す');
  } finally { SESSION_APPS = ['master-edit', 'purchase-orders']; }
  // ある人 (* も同じ)
  SESSION_APPS = '*';
  try {
    await p.goto(B + '/sku/k001');
    assert.equal(await p.textContent('#ref-po'), '7');
  } finally { SESSION_APPS = ['master-edit', 'purchase-orders']; }
});

await ta('[24] FBA (JP) の在庫 (10/5): 一覧の列 (参考・灰色)・見出しの下に何時時点か・1 × 1 の出品だけ・「—」と 0・単品の画面の内訳・まとめ売り / セットの出品は別・古い / 読めない', async (p) => {
  const hhmm = (iso) => { const d = new Date(Date.parse(iso) + 9 * 3600e3); return `${d.getUTCMonth() + 1}/${d.getUTCDate()} ${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`; };
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\nk002\nk006\ns001' }));
  const c = Object.fromEntries((await cells(p)).map((x) => [x.code, x]));
  assert.deepEqual([c.k001.fba, c.k002.fba, c.k006.fba, c.s001.fba], ['24', '—', '0', '—'], 'k001 = 1 × 1 の出品 2 つ (20 + 4)・まとめ売り / セットの出品は入れない / k002 = 1 × 1 の出品なし = — / k006 = レポートに 0');
  const th = p.locator('#th-fba');
  assert.equal((await th.textContent()).trim(), 'FBA (JP)' + hhmm(FBA_CAPTURED), '見出しの下に取得の時刻 (月/日 時:分)');
  assert.match(await th.getAttribute('title'), /FBA \(日本\) の販売可能 · .* 時点 \(朝のレポート\) · 参考 · この SKU 1 個の出品だけの合計/);
  assert.equal(await th.evaluate((x) => x.className), 'n ref');
  // 参考の列 = 灰色・小さめ (在庫の列と同じ見せ方)
  const [fbaStyle, stockStyle] = await p.evaluate(() => {
    const td = (i) => document.querySelector('#list-tbl tbody tr').children[i];
    const h = [...document.querySelectorAll('#list-tbl thead th')].map((x) => x.textContent.trim());
    const pick = (el) => { const s = getComputedStyle(el); return [s.color, s.fontSize]; };
    return [pick(td(h.findIndex((x) => x.startsWith('FBA')))), pick(td(h.findIndex((x) => x.startsWith('在庫'))))];
  });
  assert.deepEqual(fbaStyle, stockStyle, 'FBA の列は在庫の列と同じ灰色・大きさ');
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/一覧_FBAの列.png`, fullPage: true });
  // 単品の画面
  await p.goto(B + '/sku/k001');
  assert.equal(await p.textContent('#ref-fba'), '24');
  assert.match(await p.textContent('#ref-fba-when'), /時点 \(Amazon の朝のレポート\)/);
  assert.doesNotMatch(await p.textContent('#ref-fba-when'), /古い/);
  const parts = await p.$$eval('#ref-fba-parts tbody tr', (trs) => trs.map((tr) => [...tr.children].map((td) => td.textContent.trim())));
  assert.deepEqual(parts, [['販売可能', '24'], ['FC 移管中', '1'], ['FC 処理中', '1'], ['出荷待ち (注文の引き当て)', '2'], ['入荷待ち (納品の途中)', '5']]);
  assert.match(await p.textContent('#ref-fba-skus'), /出品 SKU 2 つの合計: pr-k001 20 · pr-k001-b 4/);
  assert.match(await p.textContent('#ref-fba-bundles'), /まとめ売り・セットの出品 \(上の数に入れていない\): pr-k001-3p ×3 = 6 · pr-k1k2 \(ほか 1 品と\) = 3/);
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/単品_FBAの内訳.png`, fullPage: true });
  await p.goto(B + '/sku/k002');
  assert.equal(await p.textContent('#ref-fba'), '—');
  assert.match(await p.textContent('#ref-fba-none'), /この SKU 1 個だけの出品が、その日の FBA のレポートにありません/);
  assert.match(await p.textContent('#ref-fba-bundles'), /pr-k1k2 \(ほか 1 品と\) = 3/);
  // 古い (取得が 26 時間より前)
  const keepCap = (await pg.query("select captured_at from snapshots.stock_capture_days where source = 'fba_jp'")).rows[0].captured_at;
  await pg.query("update snapshots.stock_capture_days set captured_at = now() - interval '27 hours' where source = 'fba_jp'");
  try {
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
    assert.match(await p.textContent('#th-fba'), /^FBA \(JP\)古い \d+\/\d+ \d{2}:\d{2}$/);
    assert.equal(await p.locator('#th-fba').evaluate((x) => x.className), 'n ref stale');
    assert.equal((await cells(p))[0].fba, '24', '古くても値は出す');
    await p.goto(B + '/sku/k001');
    assert.match(await p.textContent('#ref-fba-when'), /^古い · /);
    assert.equal(await p.locator('#ref-fba-when').evaluate((x) => x.className), 'when stale');
  } finally { await pg.query("update snapshots.stock_capture_days set captured_at = $1 where source = 'fba_jp'", [keepCap]); }
  // 読めない (画面のロールに権限が無い = 流し直しの前の本番) → 流し直すと戻る
  await pg.query('revoke select on snapshots.stock_capture_days from master_edit');
  try {
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
    assert.equal((await p.textContent('#th-fba')).trim(), 'FBA (JP)読めない');
    assert.equal(await p.locator('#th-fba').evaluate((x) => x.className), 'n ref bad');
    assert.equal((await cells(p))[0].fba, '—');
    assert.equal((await cells(p))[0].stock, '15', 'ロジザードの在庫はそのまま');
    await p.goto(B + '/sku/k001');
    assert.match(await p.textContent('#ref-fba-when'), /読めない \(画面のロールに FBA の在庫を読む権限がまだ無い/);
    assert.equal(await p.locator('#ref-fba-parts').count(), 0);
    assert.equal(await p.textContent('#ref-stock'), '15');
  } finally { await createMasterEditRoles(pg, {}); }
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
  assert.equal((await cells(p))[0].fba, '24', '流し直した = 読める');
});

await ta('[23] 詳細検索の長い条件 (#1620 Codex R1 M2): 本物の HTTP で商品コード 500 件 = 印 (?s=) で開ける・501 件 = 500 件まで・GET に載せると 431・Origin と JSON の守り・期限切れ', async (p) => {
  const origin = new URL(B).origin;
  const code30 = (i) => `nope-${String(i).padStart(4, '0')}-${'x'.repeat(30)}`.slice(0, 30);
  const codes500 = ['k001', ...Array.from({ length: 499 }, (_, i) => code30(i))];
  const post = (body, headers = {}) => fetch(B + '/api/search', { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json', Accept: 'application/json', ...headers }, body: JSON.stringify(body) });
  // GET の URL にそのまま載せる = Express に届く前に 431 (これを避けるための印)
  const raw = await fetch(B + '/?' + new URLSearchParams({ codes: codes500.join('\n') }));
  assert.equal(raw.status, 431, `GET に 500 件 = ${raw.status}`);
  // 守り = 保存の POST と同じ (Origin が Host と同じ・JSON)
  assert.equal((await post({ codes: 'k001' }, { Origin: 'https://evil.example' })).status, 403);
  assert.equal((await fetch(B + '/api/search', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).status, 403, 'Origin なし');
  assert.equal((await post({ codes: 'k001' }, { 'Content-Type': 'application/x-www-form-urlencoded' })).status, 415);
  // 500 件 = 全部使う
  const j = await (await post({ codes: codes500.join('\n'), kind: 'single', q: '' })).json();
  assert.equal(j.ok, true);
  assert.match(j.url, /^\/apps\/master-edit\/\?kind=single&s=[A-Za-z0-9_-]{22}$/, j.url);
  const page500 = await (await fetch(origin + j.url)).text();
  assert.match(page500, /499 件 見つからない/);
  assert.doesNotMatch(page500, /500 件までを使いました/);
  assert.match(page500, /href="sku\/k001"/);
  // 501 件 = 500 件まで (知らせる)
  const j2 = await (await post({ codes: [...codes500, 'k002'].join('\n') })).json();
  const page501 = await (await fetch(origin + j2.url)).text();
  assert.match(page501, /500 件までを使いました/);
  assert.doesNotMatch(page501, /href="sku\/k002"/, '501 件目は使わない');
  // 画面から: 500 件を貼って「この条件で探す」= 印の URL で開く・区分の札を押しても条件が残る (URL は短いまま)
  await p.goto(B + '/');
  await p.click('#adv > summary');
  await p.fill('#adv-codes', codes500.join('\n'));
  await Promise.all([p.waitForNavigation(), p.click('#adv-form button[type="submit"]')]);
  assert.match(p.url(), /\?s=[A-Za-z0-9_-]{22}$/);
  assert.deepEqual(await listCodes(p), ['k001']);
  assert.match(await p.textContent('#not-found'), /499 件 見つからない/);
  assert.equal((await p.inputValue('#adv-codes')).split('\n').length, 500, '欄には条件が戻る');
  await Promise.all([p.waitForNavigation(), p.click('.chips a.chip:has-text("単品")')]);
  assert.ok(p.url().length < 200, `札の URL も短い (${p.url().length})`);
  assert.deepEqual(await listCodes(p), ['k001']);
  // 期限切れ・再起動で消えた印
  __clearSearchTokens();
  await p.reload();
  assert.match(await p.textContent('#search-expired'), /条件の期限が切れました。もう一度検索してください/);
  assert.deepEqual(await listCodes(p), [], '消えた条件で全部を出さない');
  // 短い条件は今までどおり GET (URL に条件が残る)
  await p.goto(B + '/');
  await p.click('#adv > summary');
  await p.fill('#adv-codes', 'k001\nk002');
  await Promise.all([p.waitForNavigation(), p.click('#adv-form button[type="submit"]')]);
  assert.equal(new URL(p.url()).searchParams.get('codes').replace(/\r/g, ''), 'k001\nk002');
});

const SHOTDIR = process.env.MASTER_EDIT_UI_SHOTS || '';
for (const [label, vp] of [['1440', { width: 1440, height: 900 }], ['1280', { width: 1280, height: 720 }], ['1024', { width: 1024, height: 768 }]]) {
  await ta(`[17] ${label} 幅: 一覧をスクロールしても見出しの行 (コード・名前・…) が上の帯のすぐ下に見えている・横に送っても列がずれない (10/5)`, async (p) => {
    await p.goto(B + '/');
    // 画面の出だしの動き (.page の rise) が終わってから測る (動きの途中は数 px 動く)
    await p.waitForFunction(() => document.querySelector('.page').getAnimations().every((a) => a.playState !== 'running'));
    if (SHOTDIR) await p.screenshot({ path: `${SHOTDIR}/一覧_${label}_上.png` });
    const pos = () => p.evaluate(() => {
      const th = document.querySelector('#list-tbl thead th'); const r = th.getBoundingClientRect();
      // 見えている所 (表の囲いの左から 40px) で、見出しの行の高さの真ん中 = 一番上に見えているのが見出しか (横に送っても)
      const wr = th.closest('.tblwrap').getBoundingClientRect();
      const hit = document.elementFromPoint(wr.left + 40, r.top + r.height / 2);
      const td = document.querySelector('#list-tbl tbody tr td');
      return { top: r.top, left: r.left, tdLeft: td.getBoundingClientRect().left, hdr: document.querySelector('.hdr').getBoundingClientRect().bottom, seen: !!(hit && hit.closest('thead')), text: th.textContent.trim() };
    });
    const before = await pos();
    assert.ok(before.top > before.hdr + 100, '始めは表の上 (動かしていない)');
    await p.evaluate(() => window.scrollTo(0, document.querySelector('#list-tbl').getBoundingClientRect().top + window.scrollY + 500));
    await p.waitForTimeout(150);
    const after = await pos();
    assert.ok(Math.abs(after.top - after.hdr) <= 1.5, `見出しの行が帯の下 (${after.top} / 帯 ${after.hdr})`);
    assert.equal(after.seen, true, '見出しの行が行の上に見えている (重なりで隠れない)');
    assert.equal(after.text, 'コード');
    assert.ok(Math.abs(after.left - after.tdLeft) <= 1, '見出しと行の左の位置が同じ');
    // 横に送れる幅なら、送っても見出しと行の列がずれない
    const wrapScroll = await p.evaluate(() => { const w = document.querySelector('#list-tbl').closest('.tblwrap'); w.scrollLeft = 120; return w.scrollLeft; });
    if (wrapScroll > 0) {
      await p.waitForTimeout(50);
      const s = await pos();
      assert.ok(Math.abs(s.left - s.tdLeft) <= 1, '横に送っても列がずれない');
      assert.equal(s.seen, true);
      if (SHOTDIR) await p.screenshot({ path: `${SHOTDIR}/一覧_${label}_スクロールして横にも送った.png` });
      await p.evaluate(() => { document.querySelector('.tblwrap').scrollLeft = 0; });
    }
    if (SHOTDIR) await p.screenshot({ path: `${SHOTDIR}/一覧_${label}_スクロールした.png` });
    // 表の終わりより下では見出しも表の中に止まる (表の外へ出ない)
    await p.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await p.waitForTimeout(150);
    const end = await p.evaluate(() => { const t = document.querySelector('#list-tbl'); const th = t.querySelector('thead th').getBoundingClientRect(); return { thBottom: th.bottom, tblBottom: t.getBoundingClientRect().bottom }; });
    assert.ok(end.thBottom <= end.tblBottom + 1, '見出しは表の外へ出ない');
    const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
    assert.ok(sw <= cw + 1, `ページが横にはみ出さない ${sw} > ${cw}`);
  }, vp);
}

// 拡大 125% / 150% = 画面の CSS の幅が 1/1.25・1/1.5 になる。MASTER_EDIT_UI_SHOTS=フォルダ を付けると、そのフォルダに写しを残す (目で見る用)
const SHOTS = process.env.MASTER_EDIT_UI_SHOTS || '';
for (const [label, vp, scale] of [['1280×720 100%', { width: 1280, height: 720 }, 1], ['1280×720 125%', { width: 1024, height: 576 }, 1.25], ['1280×720 150%', { width: 853, height: 480 }, 1.5], ['1024×768', { width: 1024, height: 768 }, 1]]) {
  await ta(`[7] ${label}: ページが横にはみ出さない (一覧・単品・セットの 3 つの表)`, async (p) => {
    for (const [url, act] of [['/', null], ['/sku/s001', null], ['/sku/set001', 'cmp'], ['/sku/set001', 'edit'], ['/sku/set001', 'now']]) {
      await p.goto(B + url);
      if (act) await p.click(`#set-mode button[data-v="${act}"]`).catch(() => {});
      const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
      assert.ok(sw <= cw + 1, `${url} ${act || ''}: 横幅 ${sw} > ${cw}`);
      // 板 (左の列の箱) の中身が板からはみ出さない = 表は囲い (.scrollx) の中で横に送る (右の保存の箱に重ならない)
      const spill = await p.evaluate(() => [...document.querySelectorAll('.page .panel')].filter((x) => x.offsetParent !== null && x.scrollWidth > x.clientWidth + 1).map((x) => (x.querySelector('h2') || x).textContent.trim().slice(0, 20)));
      assert.deepEqual(spill, [], `${url} ${act || ''}: 板からはみ出している`);
      // 試験のデータは短くて収まってしまう = 長い名前・拡大で広がったときの逃げ道 (横に送る囲い) があることも見る
      const bare = await p.evaluate(() => [...document.querySelectorAll('table.comp')].filter((t) => { const w = t.parentElement; return !w || !w.classList.contains('scrollx') || getComputedStyle(w).overflowX !== 'auto'; }).length);
      assert.equal(bare, 0, `${url} ${act || ''}: セットの表が横に送る囲いに入っていない`);
      if (SHOTS) await p.screenshot({ path: `${SHOTS}/${label.replace(/[×% ]/g, '_')}_${url.replace(/\W+/g, '_')}${act || ''}.png`, fullPage: true });
    }
  }, vp, scale);
}

await browser.close();
server.close();
try { mirrorDb.close(); fs.rmSync(DATA_TMP, { recursive: true, force: true }); } catch { /* 消せなくてもよい */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
