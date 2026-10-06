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
 *  25〜29 第 2 段 (10/5): 新商品の登録 (単品・セット)・Amazon SKU・NE 登録の CSV・変更の記録 = 未保存 (data-dirty-field だけ)・離れるときの確認・保存 → 読み直し / できた商品の画面へ・
 *        削除は 1 回だけ確かめる・申告はファイルを落として sha256 を照合・1440 / 1280 / 1024 / 150% で横にはみ出さない
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
  assert.deepEqual([c.k001.fba, c.k002.fba, c.k006.fba, c.s001.fba], ['24', '0出品なし', '0', '0出品なし'], 'k001 = 1 × 1 の出品 2 つ (20 + 4)・まとめ売り / セットの出品は入れない / k002・s001 = 1 × 1 の出品の行なし = 0 + 「出品なし」(mart.v_sku_stock と同じ 0) / k006 = レポートに 0');
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
  assert.equal(await p.textContent('#ref-fba'), '0');
  assert.match(await p.textContent('#ref-fba-none'), /FBA の出品なし \(この SKU 1 個だけの出品の行が、その日の FBA のレポートに無い = 販売可能 0 と数える\)/);
  assert.equal(await p.locator('#ref-fba-parts').count(), 0);
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

// ─── 第 2 段 (10/5): 新商品の登録・Amazon SKU・NE 登録の CSV・変更の記録 を新しいデザインに ───
const skuRow = async (code) => (await pg.query('select s.name, s.standard_price_jpy::int as price, s.tax_rate::float8 as tax, s.shipping_code from core.skus s where s.code = $1', [code])).rows[0];
const mapOf = async (sku) => (await pg.query('select m.name, m.state from core.amazon_sku_maps m where m.seller_sku = $1', [sku])).rows[0];

await ta('[25] 新商品の登録 (単品・10/5): あと N つ (① がそろうまで押せない)・① を押すとその欄へ・コードをその場で確かめる・理由は数えない・離れるときの確認 (左の列・種類の札)・Ctrl+S で下書き → できた商品の画面へ (知らせ・戻る 1 回で一覧)', async (p) => {
  await p.goto(B + '/');
  await Promise.all([p.waitForNavigation(), p.click('.ph-actions a:has-text("新しい単品")')]);
  assert.match(p.url(), /new\?kind=single$/);
  assert.equal(await dirty(p), 0);
  assert.match(await p.textContent('#remain'), /あと\s*5\s*つ/);
  assert.equal(await p.isDisabled('#save'), true);
  assert.match(await p.textContent('#save'), /あと 5 つ: 商品コード/);
  await p.fill('#reason', '理由だけ');
  assert.equal(await dirty(p), 0, '保存の理由は数えない');
  await p.click('#checklist button:has-text("税率")');
  assert.equal(await p.evaluate(() => !!document.activeElement.closest('#f-tax_rate')), true, '① の税率を押すと税率の欄へ: ' + await p.evaluate(() => document.activeElement.outerHTML.slice(0, 120)));
  // コードをその場で確かめる (もうある・大文字 = 使えない)
  await p.fill('#code', 's001');
  await p.waitForSelector('#code-msg.err');
  assert.match(await p.textContent('#code-msg'), /もう Company DB にあります/);
  await p.fill('#code', 'UI-NEW');
  await p.waitForSelector('#code-msg.err');
  assert.match(await p.textContent('#code-msg'), /大文字は使えません/);
  await p.fill('#code', 'ui-new-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'UI 新商品 1');
  await p.fill('#f-standard_price', '1280');
  assert.match(await p.textContent('#save'), /あと 2 つ: 税率/);
  await p.click('#f-tax_rate button[data-v="0.08"]');
  await p.selectOption('#shipping', 'S02');
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  assert.equal(await p.isDisabled('#save'), false);
  assert.equal(await p.locator('#jump a.todo').count(), 0, '飛び先の帯に黄色が残らない');
  // 出品カードを作らない = ③ を出さない (product-hub につながない)
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), true);
  assert.match(await p.textContent('#checklist'), /カードを作らない/);
  // ② (NE 登録の CSV まで) も入れる
  await p.fill('#cost-jpy', '500');
  await p.selectOption('#f-primary_supplier', '0001');
  assert.ok(await dirty(p) >= 6, String(await dirty(p)));
  // 離れるときの確認 (左の列・種類の札)
  await p.click('.rail a[aria-label="つかいかた"]');
  assert.equal(await p.locator('#leave-bg.on').count(), 1);
  assert.match(await p.textContent('#leave-list'), /名前: UI 新商品 1/);
  await p.click('#leave-stay');
  await p.click('a.kindcard[href="?kind=set"]');
  assert.equal(await p.locator('#leave-bg.on').count(), 1, '種類を変える (画面が変わる) も聞く');
  await p.click('#leave-stay');
  assert.match(p.url(), /new\?kind=single$/);
  // Ctrl+S = 下書きを保存 → できた商品の画面へ (履歴を置き換える)・上に知らせ
  await Promise.all([p.waitForURL(/\/sku\/ui-new-1$/), p.keyboard.press('Control+s')]);
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /保存しました/);
  assert.deepEqual(await skuRow('ui-new-1'), { name: 'UI 新商品 1', price: 1280, tax: 0.08, shipping_code: 'S02' });
  assert.match(await p.content(), /下書きです/);
  await p.goBack();
  await p.waitForSelector('#list-tbl');
  assert.match(p.url(), /master-edit\/$/, '戻る 1 回で一覧 (入力の途中の画面へは戻らない)');
});

await ta('[26] 新商品の登録 (セット・10/5): 空の行は数えない・構成品のコードで名前と計算の見込み (8% と 10% = 8%・分類は小さい番号・原価の合計)・並べ替えで読み上げの名前・あと N つ', async (p) => {
  await p.goto(B + '/new?kind=set');
  assert.equal(await dirty(p), 0, '空の 2 行は数えない');
  assert.match(await p.textContent('#remain'), /あと\s*5\s*つ/);
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('s001');
  await rows.nth(0).locator('.c-code').press('Enter');
  assert.equal(await p.evaluate(() => document.activeElement.classList.contains('c-qty')), true, 'コードで Enter = 数の欄へ (保存しない)');
  await rows.nth(1).locator('.c-code').fill('s003');
  await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /単品 3/.test(document.querySelectorAll('#comp-rows .c-name')[1].textContent) && /単品 1/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.match(await p.textContent('#t-tax'), /8%[\s\S]*混ざって/);
  assert.match(await p.textContent('#t-sales'), /1\s*自社/);
  assert.match(await p.textContent('#t-cost'), /150\s*円/);
  assert.equal(await dirty(p), 1, '構成は 1 件 (中身どうし)');
  await rows.nth(1).locator('button[data-act="up"]').click();
  assert.equal(await rows.nth(0).locator('.c-qty').getAttribute('aria-label'), '1 行目 (s003) の数');
  assert.match(await p.textContent('#remain'), /あと\s*4\s*つ/);
  assert.equal(await p.isDisabled('#save'), true);
  // 売上分類の上書き (#1628 Codex R1 M2): 構成品から導ける (3 と 1 = 1) 間は押せない
  const ovr = p.locator('#f-set_sales_class_override');
  assert.equal(await ovr.isDisabled(), true, '導ける間は上書きを押せない');
  assert.match(await p.textContent('#override-hint'), /導けるので、上書きはできません/);
  // 売上分類が未入力の構成品 (ui-new-1 = [25] で分類を入れずに登録) = 導けない = 上書きで決める
  await rows.nth(0).locator('.c-code').fill('ui-new-1');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /UI 新商品 1/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.equal(await ovr.isDisabled(), false, '導けないときは上書きできる');
  assert.match(await p.textContent('#t-sales'), /決まりません/);
  await p.click('#sec-comp details.more > summary');
  await ovr.selectOption('2');
  assert.match(await p.textContent('#t-sales'), /2\s*取引先限定[\s\S]*導けないので/);
  // 導ける構成に戻す = サーバーが断る形 = ① で止める (保存のボタンも)
  await rows.nth(0).locator('.c-code').fill('s003');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /単品 3/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.match(await p.textContent('#t-sales'), /上書きできません/);
  assert.match(await p.textContent('#checklist'), /売上分類の上書きを空にする/);
  assert.equal(await ovr.isDisabled(), false, '入れてある上書きは空にできる');
  await ovr.selectOption('');
  assert.equal(await ovr.isDisabled(), true);
  assert.ok(!/売上分類の上書きを空にする/.test(await p.textContent('#checklist')));
  // 照合中は保存しない (#1628 Codex R2 M1): 導けない構成 + 上書き (保存できる) から、導ける構成品へ打ち直した直後 = 古い答えで保存しない
  const newCalls = [];
  p.on('request', (rq) => { if (/\/api\/new$/.test(rq.url())) newCalls.push(rq.url()); });
  await p.fill('#code', 'ui-set-race');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', '照合中の試験');
  await p.fill('#f-standard_price', '1500');
  await p.selectOption('#shipping', 'S02');
  await rows.nth(0).locator('.c-code').fill('ui-new-1');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /UI 新商品 1/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  await ovr.selectOption('2');
  assert.equal(await p.isDisabled('#save'), false, '導けない構成 + 上書き = 保存できる');
  await rows.nth(0).locator('.c-code').fill('s003');   // 欄を離れない (照合は打つのが止まってから)
  assert.equal(await p.isDisabled('#save'), true, '打ち直した直後 = 照合中 = 押せない');
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/);
  await p.keyboard.press('Control+s');
  await p.waitForFunction(() => /単品 3/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.deepEqual(newCalls, [], '照合中の Ctrl+S で送らない');
  assert.match(await p.textContent('#checklist'), /売上分類の上書きを空にする/, '照合の後は導ける = 上書きを空にするまで止める');
  assert.equal(await p.isDisabled('#save'), true);
  await ovr.selectOption('');
  assert.equal(await p.isDisabled('#save'), false, '上書きを空にすれば保存できる');
});

await ta('[30] 新商品の登録 (#1628 Codex R2 M2): カードを作らないでも値の残る欄は隠さない (保存のときに確かめる)・カードの欄の誤りは開いてその欄へ・空にすると畳む', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.fill('#amazon-url', 'not-a-url');
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), false, '値が残っている = 隠さない');
  assert.match(await p.textContent('#card-off-note'), /要らなければ空にしてください/);
  await p.fill('#code', 'ui-nocard-1');
  await p.waitForSelector('#code-msg.ok, #code-msg.err, #code-msg.warn');
  assert.match(await p.getAttribute('#code-msg', 'class'), /ok/, await p.textContent('#code-msg'));
  await p.fill('#f-name', 'カードの試験');
  await p.fill('#f-standard_price', '900');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.click('#save');
  await p.waitForSelector('.result.err');
  assert.match(await p.textContent('.result.err'), /URL/);
  assert.equal(await p.isHidden('#card-fields'), false);
  assert.equal(await p.locator('[data-row="card.amazon_url"].err').count(), 1, '誤りの欄に印');
  assert.equal(await active(p), 'amazon-url', '誤りの欄へ');
  assert.equal(await skuRow('ui-nocard-1'), undefined, '登録していない');
  await p.fill('#amazon-url', '');
  await p.focus('#f-name');
  await p.waitForFunction(() => document.getElementById('card-fields').hidden === true);
  assert.match(await p.textContent('#card-off-note'), /カードは作りません \(保存しても/);
});

await ta('[31] 新商品 (セット・#1628 Codex R3 M1 / L3): 構成品の照合が 503 = 答えにせず 3 秒後にもう一度 (直れば保存の止めが外れる)・A → B → A と打ち直したら前の A の答えを使わない', async (p) => {
  let n503 = 0;
  await p.route('**/api/lookup?code=k001', async (route) => {
    if (n503 === 0) { n503++; await route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'Company DB につながりません' }) }); }
    else await route.continue();
  });
  await p.goto(B + '/new?kind=set');
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('k001');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /3 秒後にもう一度/.test(document.querySelector('#comp-rows .c-name').textContent));
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/, '503 は「確かめている」のまま (できないコードと決めない)');
  assert.ok(!/構成品にできないコード/.test(await p.textContent('#checklist')));
  await p.waitForFunction(() => /国産 はちみつ/.test(document.querySelector('#comp-rows .c-name').textContent), null, { timeout: 10000 });
  assert.equal(n503, 1);
  assert.ok(!/構成品を確かめています|構成品にできないコード/.test(await p.textContent('#checklist')), '直ったら止めが外れる');
  // A → B → A (欄を離れずに 0.5 秒以内) = 前の A の答えは捨てる (① で止まる)
  await rows.nth(0).locator('.c-code').fill('k002');
  await rows.nth(0).locator('.c-code').fill('k001');
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/, '打ち直した瞬間に古い答えを捨てる');
  await p.waitForFunction(() => /国産 はちみつ/.test(document.querySelector('#comp-rows .c-name').textContent));
  assert.ok(!/構成品を確かめています/.test(await p.textContent('#checklist')));
});

await ta('[32] 新商品 (#1628 Codex R3 M2): 「作らない」の理由の欄は、カードを作らないときも出す・理由が無くて断られたら理由の欄へ', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.click('#set-plan button[data-v="none"]');
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), false);
  assert.equal(await p.isHidden('#row-set-reason'), false, '作らない理由の欄も出す');
  await p.fill('#code', 'ui-setplan-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'セット判断の試験');
  await p.fill('#f-standard_price', '700');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.click('#save');
  await p.waitForSelector('.result.err');
  assert.match(await p.textContent('.result.err'), /作らない理由を選んでください/);
  assert.equal(await p.locator('#row-set-reason.err').count(), 1, '理由の欄に印');
  assert.equal(await active(p), 'set-reason', '理由の欄へ');
  assert.equal(await skuRow('ui-setplan-1'), undefined);
  // 「まだ決めない」に戻して空にすれば畳む
  await p.click('#set-plan button[data-v=""]');
  await p.focus('#f-name');
  await p.waitForFunction(() => document.getElementById('card-fields').hidden === true);
});

await ta('[33] 新商品 (セット・#1628 Codex R4 M1): 確かめ直しは 3 回まで → 「もう一度確かめる」(保存は止めたまま)・401/403 は自動で繰り返さない・行を消したら確かめ直しも止まる', async (p) => {
  await p.addInitScript(() => { window.__meRetryWaits = [150, 300, 450]; });   // 本物は 3 秒・10 秒・30 秒
  const hits = { k002: 0, k003: 0, k004: 0 };
  let k002Down = true;
  await p.route('**/api/lookup?code=*', async (route) => {
    const code = new URL(route.request().url()).searchParams.get('code');
    if (code in hits) hits[code]++;
    if (code === 'k002' && k002Down) return route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'Company DB につながりません' }) });
    if (code === 'k003') { await new Promise((r) => setTimeout(r, 700)); try { await route.fulfill({ status: 503, contentType: 'application/json', body: '{}' }); } catch { /* 画面が止めた (行を消した) */ } return; }
    if (code === 'k004') return route.fulfill({ status: 403, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'このアプリの権限がありません' }) });
    return route.continue();
  });
  await p.goto(B + '/new?kind=set');
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('k002');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForSelector('#comp-rows tr.comp-row:nth-child(1) button[data-act="relookup"]', { timeout: 10000 });
  assert.equal(hits.k002, 4, '最初の 1 回 + 確かめ直し 3 回で止まる');
  await p.waitForTimeout(1200);
  assert.equal(hits.k002, 4, '上限の後は自動で聞かない');
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/, '保存は止めたまま');
  k002Down = false;
  await rows.nth(0).locator('button[data-act="relookup"]').click();
  await p.waitForFunction(() => /ハチミツ/.test(document.querySelector('#comp-rows .c-name').textContent));
  assert.ok(!/構成品を確かめています/.test(await p.textContent('#checklist')), '「もう一度確かめる」で直る');
  // 401/403 = 自動で繰り返さない (ログインし直す・開き直す)
  await rows.nth(1).locator('.c-code').fill('k004');
  await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForSelector('#comp-rows tr.comp-row:nth-child(2) button[data-act="relookup"]');
  assert.match(await rows.nth(1).locator('.c-name').textContent(), /ログインし直すか/);
  await p.waitForTimeout(800);
  assert.equal(hits.k004, 1, '403 は自動で繰り返さない');
  // 返事待ちの行を消したら、遅れて来た返事で確かめ直しを始めない (#1628 Codex R4 M1)
  await rows.nth(1).locator('.c-code').fill('k003');
  await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /引き当てています/.test(document.querySelectorAll('#comp-rows .c-name')[1].textContent));
  await rows.nth(1).locator('button[data-act="del"]').click();
  await p.waitForTimeout(2000);
  assert.equal(hits.k003, 1, '消した行は確かめ直さない');
});

await ta('[34] 新商品 (#1628 Codex R4 M2): カードを作らない + Yahoo! の欄だけ値がある = 「Yahoo! の欄も確かめる」と出す・Yahoo!売価 0 で保存 = その欄を開いて止める', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.click('#sec-yahoo > summary');
  await p.fill('#y-price', '0');
  await p.click('#sec-yahoo > summary');   // 畳む
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), true, 'カードの欄は空 = 畳む');
  assert.match(await p.textContent('#card-off-note'), /「Yahoo! を楽天と変えるときだけ」の欄に入れた値は保存のときに確かめます/);
  await p.fill('#code', 'ui-yahoo-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'Yahoo! の試験');
  await p.fill('#f-standard_price', '800');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.click('#save');
  assert.match(await p.textContent('#msg'), /Yahoo!売価は 1 円以上/);
  assert.equal(await p.evaluate(() => document.getElementById('sec-yahoo').open), true, 'Yahoo! の欄を開く');
  assert.equal(await active(p), 'y-price', 'その欄へ');
  assert.equal(await skuRow('ui-yahoo-1'), undefined);
  await p.fill('#y-price', '');
  await p.focus('#f-name');
  await p.waitForFunction(() => /保存しても product-hub/.test(document.getElementById('card-off-note').textContent));
});

await ta('[27] Amazon SKU (10/5): 新しい対応 = 名前で未保存 1 件 (理由は数えない)・離れるときの確認・Ctrl+S → 読み直し (知らせ)・削除の理由は数えない・削除は 1 回だけ確かめる (Esc で戻る)・墓標・変更の記録のカード', async (p) => {
  await p.goto(B + '/amazon/');
  assert.match(await p.textContent('#h-um'), /売れたのに対応が無い SKU/);
  await p.fill('#open-sku', 'pr-k001-b');
  await Promise.all([p.waitForNavigation(), p.press('#open-sku', 'Enter')]);
  assert.match(p.url(), /amazon\/sku\?sku=pr-k001-b$/);
  assert.match(await p.textContent('.idrow'), /対応なし/);
  assert.equal(await dirty(p), 0);
  assert.equal(await p.inputValue('#comp-rows tr.comp-row .c-code'), 'k001', '今の構成 (夜間の取り込み) から始まる');
  assert.match(await p.textContent('#comp-rows tr.comp-row'), /代表/);
  await p.fill('#name', 'UI の出品');
  assert.equal(await dirty(p), 1);
  assert.match(await p.textContent('#save-diff'), /名前 \(社内\)[\s\S]*UI の出品/);
  assert.match(await p.textContent('#save-impact-list'), /新しく作ります[\s\S]*07:00|新しく作ります[\s\S]*7:00/);
  await p.fill('#reason', 'UI の試験');
  assert.equal(await dirty(p), 1, '保存の理由は数えない');
  await p.click('.rail a[aria-label="商品・セット"]');
  assert.equal(await p.locator('#leave-bg.on').count(), 1);
  await p.click('#leave-stay');
  await p.keyboard.press('Control+s');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /保存しました[\s\S]*新しい対応を作りました/);
  assert.deepEqual(await mapOf('pr-k001-b'), { name: 'UI の出品', state: 'active' });
  assert.equal(await dirty(p), 0);
  assert.equal(await p.inputValue('#name'), 'UI の出品', '読み直した後の値');
  // 削除 = 理由を入れると押せる・未保存には数えない・1 回だけ確かめる
  await p.click('#del-box > summary');
  assert.equal(await p.isDisabled('#del'), true);
  await p.fill('#del-reason', 'UI の試験で消す');
  assert.equal(await dirty(p), 0, '削除の理由は数えない');
  assert.equal(await p.isDisabled('#del'), false);
  await p.click('#del');
  assert.equal(await p.locator('#del-bg.on').count(), 1);
  assert.match(await p.textContent('#del-why'), /UI の試験で消す/);
  assert.equal(await active(p), 'del-stay', '確かめの窓は「やめる」から');
  await p.keyboard.press('Escape');
  assert.equal(await p.locator('#del-bg.on').count(), 0);
  assert.equal(await active(p), 'del', 'Esc で押した所へ戻る');
  assert.equal((await mapOf('pr-k001-b')).state, 'active', 'やめたので消していない');
  await p.click('#saved-note-close');
  await p.click('#del');
  await p.click('#del-go');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /削除 \(墓標に\) しました/);
  assert.equal((await mapOf('pr-k001-b')).state, 'deleted');
  assert.match(await p.content(), /削除済み \(墓標\) です/);
  await Promise.all([p.waitForNavigation(), p.click('.ph-actions a:has-text("変更の記録")')]);
  assert.ok(await p.locator('.hcard').count() >= 2, '保存 1 回 = 1 枚');
  assert.match(await p.textContent('#hist-cards'), /UI の試験で消す/);
  await p.click('#hist-filter button[data-hf="load"]').catch(() => {});
  // 対応の無い seller SKU を、夜間の取り込みの名前・構成のまま (変えた欄 0) 新しく登録できる (#1628 Codex R1 M1)
  await pg.query("update core.listings set title = 'はちみつ 3 個組' where listing_code = 'pr-k001-3p'");
  await p.goto(B + '/amazon/sku?sku=pr-k001-3p');
  assert.equal(await dirty(p), 0);
  assert.equal(await p.inputValue('#name'), 'はちみつ 3 個組');
  assert.equal(await p.isDisabled('#save'), false, '初期値のままでも保存できる');
  assert.match(await p.textContent('#save-impact-list'), /新しく作ります \(今の名前・構成のまま\)/);
  assert.match(await p.textContent('#save-empty'), /このまま保存すると|この名前・構成のまま保存すると/);
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /新しい対応を作りました/);
  assert.deepEqual(await mapOf('pr-k001-3p'), { name: 'はちみつ 3 個組', state: 'active' });
  assert.equal(await p.isDisabled('#save'), true, '作った後は変えた欄が無ければ押せない');
});

await ta('[28] NE 登録の CSV (10/5): 選んだ数で作る → 配る (ダウンロード) → 申告の書きかけ = 未保存 (離れるときの確認) → 違うファイルは照合で止める → 同じファイルで申告 → 読み直し', async (p, ctx) => {
  process.env.MASTER_DECISION_APPROVERS = 'naka@test';
  // 今日の照合の回と NE の元のコード (前からある商品 = NE にある / ui-new-1 = 無い)
  const run = 'mc_' + new Date().toISOString().replace(/[-:.]/g, '') + '_abcdef';
  await pg.query('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, now(), 0)', [run]);
  const codes = (await pg.query("select code_norm from core.skus where code_norm <> 'ui-new-1'")).rows.map((r) => r.code_norm);
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: codes.map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
  try {
    await p.goto(B + '/reg-csv');
    const pick = p.locator('input.pick[value="ui-new-1"]');
    assert.equal(await pick.isDisabled(), false, await p.textContent('#cands'));
    const buildBtn = p.locator('button[data-act="build"][data-kind="products"]');
    assert.equal(await buildBtn.isDisabled(), true, '0 件は押せない');
    await pick.check();
    assert.match(await buildBtn.textContent(), /選んだ単品 1 件/);
    assert.equal(await dirty(p), 0, '選ぶだけは未保存にしない');
    await buildBtn.click();
    await p.waitForSelector('.saved-note:has-text("を作りました")');
    const card = p.locator('article.exp').first();
    assert.match(await card.textContent(), /配る \(ダウンロード\)/);
    const [dl] = await Promise.all([p.waitForEvent('download'), card.locator('button[data-act="issue"]').click()]);
    const file = path.join(DATA_TMP, 'ui-reg.csv');
    await dl.saveAs(file);
    await p.waitForSelector('.saved-note:has-text("配りました")');
    const c2 = p.locator('article.exp').first();
    await c2.locator('button[data-act="show-declare"]').first().click();
    await c2.locator('input[name="ne_message"]').fill('1件成功しました。');
    assert.equal(await dirty(p), 1, '申告の書きかけ = 未保存');
    await p.click('.rail a[aria-label="つかいかた"]');
    assert.equal(await p.locator('#leave-bg.on').count(), 1);
    assert.match(await p.textContent('#leave-list'), /の申告 \(書きかけ\)/);
    await p.click('#leave-stay');
    const bad = path.join(DATA_TMP, 'ui-bad.csv');
    fs.writeFileSync(bad, 'not the file');
    await c2.locator('input[data-sha-file]').setInputFiles(bad);
    await p.waitForSelector('[data-drop].bad');
    await c2.locator('label.choice.r-ok').click();
    await c2.locator('button[data-act="declare"]').click();
    assert.match(await c2.locator('[data-drawer="declare"] [data-msg]').textContent(), /sha256 が合いません/);
    await c2.locator('input[data-sha-file]').setInputFiles(file);
    await p.waitForSelector('[data-drop].done');
    // いまの時刻を入れる (#1628 Codex R1 M3): 秒まで・今の時刻 = 配った直後 (配った時刻の秒が 0 でない) でも断られない
    await c2.locator('[data-now]').click();
    const imp = await c2.locator('input[name="imported_at"]').inputValue();
    assert.match(imp, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/, '秒まで入る: ' + imp);
    assert.ok(Math.abs(new Date(imp).getTime() - Date.now()) < 5000, imp);
    await c2.locator('button[data-act="declare"]').click();
    await p.waitForSelector('.saved-note:has-text("申告しました")');
    const st = (await pg.query("select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code_norm = 'ui-new-1'")).rows[0].state;
    assert.equal(st, 'ne_pending');
    const att = (await pg.query('select imported_at, declared_at from ops.ne_reg_attempts order by attempt_id desc limit 1')).rows[0];
    assert.ok(att.imported_at && new Date(att.imported_at) <= new Date(att.declared_at), JSON.stringify(att));
    assert.equal(await dirty(p), 0);
  } finally { delete process.env.MASTER_DECISION_APPROVERS; }
});

// 第 2 段の画面の幅: 1440 / 1280 / 1024 と 1280×720 の 150% で横にはみ出さない・板からはみ出さない・構成の表は横に送る囲いの中。
// MASTER_EDIT_UI_SHOTS_STAGE2=フォルダ を付けると 1440 / 1280 / 1024 の写しを残す (目で見る用。MASTER_EDIT_UI_SHOTS2 は [20] / [24] の写しで使っている)
const SHOTS2 = process.env.MASTER_EDIT_UI_SHOTS_STAGE2 || '';
const fillNew = async (p) => { await p.fill('#code', 'shot-new-1'); await p.waitForSelector('#code-msg.ok'); await p.fill('#f-name', '国産 はちみつ レモン 500g'); await p.fill('#f-standard_price', '1680'); await p.click('#f-tax_rate button[data-v="0.08"]'); };
const fillSet = async (p) => {
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('k001'); await rows.nth(0).locator('.c-code').press('Tab');
  await rows.nth(1).locator('.c-code').fill('k002'); await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /ハチミツ/.test(document.querySelectorAll('#comp-rows .c-name')[1].textContent));
  await p.fill('#f-name', 'はちみつ 2 種 ギフト');
};
const SCREENS2 = [
  ['新商品_単品', '/new?kind=single', fillNew], ['新商品_セット', '/new?kind=set', fillSet],
  ['Amazon_一覧', '/amazon/', null], ['Amazon_未登録', '/amazon/unmapped?channel=all', null], ['Amazon_1つのSKU', '/amazon/sku?sku=pr-k001', null],
  ['Amazon_変更の記録', '/amazon/sku/history?sku=pr-k001-b', null], ['NE登録のCSV', '/reg-csv', null], ['変更の記録', '/sku/s003/history', null],
];
for (const [label, vp, scale] of [['1440', { width: 1440, height: 900 }, 1], ['1280', { width: 1280, height: 720 }, 1], ['1024', { width: 1024, height: 768 }, 1], ['1280×720 150%', { width: 853, height: 480 }, 1.5]]) {
  await ta(`[29] ${label}: 第 2 段の画面 (新商品・Amazon・NE 登録の CSV・変更の記録) が横にはみ出さない`, async (p) => {
    process.env.MASTER_DECISION_APPROVERS = 'naka@test';   // NE 登録の CSV を操作できる人の画面で
    try {
    for (const [name, url, prep] of SCREENS2) {
      await p.goto(B + url);
      await p.waitForFunction(() => document.querySelector('.page').getAnimations().every((a) => a.playState !== 'running'));
      if (prep) await prep(p);
      const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
      assert.ok(sw <= cw + 1, `${name}: 横幅 ${sw} > ${cw}`);
      const spill = await p.evaluate(() => [...document.querySelectorAll('.page .panel, .page .filecard')].filter((x) => x.offsetParent !== null && x.scrollWidth > x.clientWidth + 1).map((x) => (x.querySelector('h2, .ttl') || x).textContent.trim().slice(0, 20)));
      assert.deepEqual(spill, [], `${name}: 板からはみ出している`);
      const bare = await p.evaluate(() => [...document.querySelectorAll('table.comp')].filter((t) => { const w = t.parentElement; return !w || !w.classList.contains('scrollx') || getComputedStyle(w).overflowX !== 'auto'; }).length);
      assert.equal(bare, 0, `${name}: 構成の表が横に送る囲いに入っていない`);
      if (SHOTS2 && scale === 1) await p.evaluate(() => { if (document.activeElement) document.activeElement.blur(); window.scrollTo(0, 0); }).then(() => p.waitForTimeout(150)).then(() => p.screenshot({ path: `${SHOTS2}/${name}_${label}.png`, fullPage: true }));
    }
    } finally { delete process.env.MASTER_DECISION_APPROVERS; }
  }, vp, scale);
}

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
