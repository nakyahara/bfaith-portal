#!/usr/bin/env node
/**
 * 米国FBA在庫補充 PR1 (見るだけ) の試験。SP-API にも miniPC にも行かない。DATA_DIR は一時フォルダ。
 *   node scripts/test-fba-us-inventory-view.mjs
 *
 * ① miniPC: 取れたままのレポートの保存 (fba-us-reports-store.js)
 * ② miniPC: 毎朝のスナップショット本体から保存が呼ばれる・失敗しても今までの結果を変えない
 * ③ Render: 画面用の組み立て (us-view.js) = 取れなかった値を 0 にしない
 * ④ Render: SKU → 自社の商品コード / miniPC の呼び出し / 画面 / 組み込み
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-us-view-'));
process.env.DATA_DIR = tmp;
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}\n     ${e.stack.split('\n').slice(0, 4).join('\n     ')}`); }
}
const quiet = () => {};
const T0 = '2026-09-20T22:00:00Z', F0 = '2026-09-20T22:01:00Z';

const store = await imp('apps/warehouse/fba-us-reports-store.js');
const { runFbaReportSnapshot } = await imp('apps/warehouse/fba-report-snapshot.js');
const { buildUsInventoryView } = await imp('apps/fba-replenishment-us/us-view.js');

const rRow = (sku, over = {}) => ({ 'Merchant SKU': sku, 'Product Name': `name ${sku}`, FNSKU: `X0${sku}`, ASIN: `B0${sku}`, Available: '10', Working: '0', Shipped: '5', Receiving: '1', 'FC Transfer': '0', 'FC Processing': '0', 'Customer Order': '2', Unfulfillable: '0', 'Units Sold Last 30 Days': '30', 'Recommended replenishment qty': '40', 'Recommended ship date': '2026-10-01', Price: '19.99', ...over });
const pRow = (sku, over = {}) => ({ sku, fnsku: `X0${sku}`, asin: `B0${sku}`, 'units-shipped-t7': '7', 'units-shipped-t30': '28', 'units-shipped-t90': '90', 'your-price': '19.99', ...over });

console.log('① 取れたままのレポートの保存');
await t('両方取れた回: 日付のファイル (行つき) と最後の取得 (行なし) を書く / 読むと最新の日が返る', async () => {
  const dir = path.join(tmp, 'a');
  const r = store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-23T22:00:00Z', fetchedAt: '2026-09-23T22:01:00Z', results: { restock: [rRow('a')], planning: [pRow('a')], errors: [] } });
  assert.deepEqual([r.dated, r.restock, r.planning], [true, true, true]);
  const la = JSON.parse(fs.readFileSync(path.join(dir, 'last-attempt.json'), 'utf8'));
  assert.equal(la.reports.restock.rows, undefined, '最後の取得に行を持たせている');
  assert.deepEqual([la.reports.restock.ok, la.reports.restock.row_count], [true, 1]);
  const got = store.readLatestUsReports({ dir });
  assert.deepEqual([got.latest.business_date, got.latest.reports.restock.rows[0]['Merchant SKU'], got.latest.reports.planning.fetched_at], ['2026-09-24', 'a', '2026-09-23T22:01:00Z']);
  assert.deepEqual(fs.readdirSync(dir).filter((f) => f.endsWith('.tmp')), [], '一時ファイルが残っている');
});
await t('同じ日の 2 回目で PLANNING だけ失敗 → PLANNING は前の回のまま (時刻も前の回) / RESTOCK は今回の分', async () => {
  const dir = path.join(tmp, 'b');
  store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-23T22:00:00Z', fetchedAt: '2026-09-23T22:01:00Z', results: { restock: [rRow('a')], planning: [pRow('a')], errors: [] } });
  store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-24T00:59:00Z', fetchedAt: '2026-09-24T01:00:00Z', results: { restock: [rRow('a', { Available: '9' })], planning: null, errors: [{ report: 'planning', error: 'FATAL' }] } });
  const got = store.readLatestUsReports({ dir });
  assert.deepEqual([got.latest.reports.restock.rows[0].Available, got.latest.reports.restock.fetched_at, got.latest.reports.planning.fetched_at], ['9', '2026-09-24T01:00:00Z', '2026-09-23T22:01:00Z']);
  assert.deepEqual([got.last_attempt.reports.planning.ok, got.last_attempt.reports.planning.error], [false, 'FATAL']);
});
await t('取得そのものが失敗 / 行が 0 件: 日付のファイルは書かない (前の日を残す)・最後の取得には失敗を書く', async () => {
  const dir = path.join(tmp, 'c');
  store.saveUsReportRun({ dir, businessDate: '2026-09-23', attemptedAt: '2026-09-22T22:00:00Z', fetchedAt: '2026-09-22T22:01:00Z', results: { restock: [rRow('a')], planning: [pRow('a')], errors: [] } });
  const r = store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-23T22:00:00Z', results: null, error: 'Access to requested resource is denied.' });
  assert.equal(r.dated, false);
  const r2 = store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-24T01:00:00Z', fetchedAt: '2026-09-24T01:01:00Z', results: { restock: [], planning: [], errors: [] } });
  assert.equal(r2.dated, false, '行が 0 件の回を「取れた」にしている');
  const got = store.readLatestUsReports({ dir });
  assert.deepEqual([got.latest.business_date, got.last_attempt.reports.restock.error, got.last_attempt.attempted_at], ['2026-09-23', 'no_rows', '2026-09-24T01:00:00Z']);
  assert.equal(fs.existsSync(path.join(dir, '2026-09-24.json')), false);
});
await t('いちばん新しい日のファイルが壊れていたら 1 つ前の日を返し、読めなかったことを残す / 保存フォルダが無ければ空で返す / 古い日は消す', async () => {
  const dir = path.join(tmp, 'd');
  store.saveUsReportRun({ dir, businessDate: '2026-09-22', attemptedAt: T0, fetchedAt: F0, results: { restock: [rRow('a')], planning: null, errors: [] } });
  fs.writeFileSync(path.join(dir, '2026-09-23.json'), '{ broken');
  const got = store.readLatestUsReports({ dir });
  assert.deepEqual([got.latest.business_date, got.file_errors[0].file], ['2026-09-22', '2026-09-23.json']);
  store._resetSaveFailure();
  assert.deepEqual(store.readLatestUsReports({ dir: path.join(tmp, 'none') }), { schema: 1, last_attempt: null, latest: null, file_errors: [], save_failure: null });
  fs.writeFileSync(path.join(dir, '2025-01-01.json'), '{}');
  store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: T0, fetchedAt: F0, results: { restock: [rRow('a')], planning: null, errors: [] }, now: new Date('2026-09-24T00:00:00Z') });
  assert.equal(fs.existsSync(path.join(dir, '2025-01-01.json')), false, '400 日より前のファイルが残っている');
  assert.throws(() => store.saveUsReportRun({ dir, businessDate: '../x', attemptedAt: 't', results: null }), /business_date が不正/);
});

console.log('② 毎朝のスナップショット本体からの呼び出し');
const fakeDb = (over = {}) => ({ saveRestockInventoryToDailySnapshot: (rows) => ({ updated: 0, inserted: rows.length }), saveRestockLatest: (rows) => ({ saved: rows.length }), updateFnskuBatch: () => {}, savePlanningData: (rows) => rows.length, savePlanningLatest: (rows) => ({ saved: rows.length }), syncFnskuBatch: () => {}, saveUsDailySnapshots: () => ({ inserted: 1, updated: 0 }), saveStockExport: () => ({ saved: true }), ...over });
const usCtx = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
const fetchBoth = async () => ({ restock: [rRow('a')], planning: [pRow('a')], errors: [] });
await t('米国の取得のあとに 1 回だけ呼ぶ (取れた行・同じ business_date・取得時刻つき) / 最後の行は今までと同じ', async () => {
  const seen = [];
  const r = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-24', fetchReports: fetchBoth, usContext: usCtx, saveUsRaw: (a) => { seen.push(a); return { dated: true }; }, log: quiet, warn: quiet });
  assert.equal(seen.length, 1);
  assert.deepEqual([seen[0].businessDate, seen[0].results.restock.length, typeof seen[0].fetchedAt, typeof seen[0].attemptedAt], ['2026-09-24', 1, 'string', 'string']);
  assert.match(r.lastLine, /US planning=1 restock=1$/);
  assert.deepEqual(r.us.rawSaved, { dated: true });
});
await t('米国の取得が例外 → 失敗を残す (error つき・行なし) / 全体は今までどおり ok', async () => {
  const seen = [];
  const r = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-24', fetchReports: async (ctx) => { if (ctx) throw new Error('US 403'); return fetchBoth(); }, usContext: usCtx, saveUsRaw: (a) => { seen.push(a); return { dated: false }; }, log: quiet, warn: quiet });
  assert.deepEqual([seen.length, seen[0].error, seen[0].results, r.ok, r.us.error], [1, 'US 403', null, true, 'US 403']);
});
await t('🚨 保存に失敗しても、今までの保存・ok・最後の行の頭は変えない (警告と一言だけ) / 米国の env が無ければ呼ばない', async () => {
  const warns = [];
  const r = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-24', fetchReports: fetchBoth, usContext: usCtx, saveUsRaw: () => { throw new Error('EPERM'); }, log: quiet, warn: (...a) => warns.push(a.join(' ')) });
  assert.deepEqual([r.ok, r.us.inserted, r.lastLine.startsWith('✅')], [true, 1, true]);
  assert.match(r.lastLine, /US planning=1 restock=1 \(取れたままのレポートを残せなかった: EPERM\)$/);
  assert.ok(warns.some((w) => /取れたままのレポートを残せなかった/.test(w)));
  let called = 0;
  await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-24', fetchReports: fetchBoth, usContext: { market: 'us' }, saveUsRaw: () => { called++; }, log: quiet, warn: quiet });
  assert.equal(called, 0);
});
await t('既定の保存先は DATA_DIR/fba-us-reports (試験では一時フォルダ)・fba.db の保存が競合 (FBA_DB_*) しても取れた行は先に残っている', async () => {
  const ext = () => { throw Object.assign(new Error('x'), { code: 'FBA_DB_EXTERNAL_WRITE' }); };
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb({ saveUsDailySnapshots: ext }), businessDate: '2026-09-21', fetchReports: fetchBoth, usContext: usCtx, log: quiet, warn: quiet }), (e) => e.code === 'FBA_DB_EXTERNAL_WRITE');
  assert.equal(fs.existsSync(path.join(tmp, 'fba-us-reports', '2026-09-21.json')), true);
});

console.log('③ 画面用の組み立て');
const now = new Date('2026-09-24T03:00:00Z');
const payload = (restockRows, planningRows, over = {}) => ({
  last_attempt: { business_date: '2026-09-24', attempted_at: '2026-09-23T22:00:00Z', reports: { restock: { ok: true }, planning: { ok: true } }, error: null },
  latest: { business_date: '2026-09-24', reports: {
    restock: restockRows ? { ok: true, fetched_at: '2026-09-23T22:01:00Z', rows: restockRows } : { ok: false, error: 'FATAL', rows: null },
    planning: planningRows ? { ok: true, fetched_at: '2026-09-23T22:02:00Z', rows: planningRows } : { ok: false, error: 'FATAL', rows: null },
  } },
  file_errors: [], ...over,
});
const master = (m) => (skus) => new Map(skus.map((s) => [s, m[s] || { route: 'none', components: [] }]));
await t('RESTOCK と PLANNING を SKU で合わせる (大文字小文字は無視・表示は RESTOCK の表記)・在庫日数 = (販売可能+準備中+輸送中+受領中) ÷ 日販', async () => {
  const v = buildUsInventoryView(payload([rRow('Card-A')], [pRow('card-a')]), { now, resolveSkus: master({ 'card-a': { route: 'master', components: [{ ne_code: 'card', qty: 20 }] } }) });
  assert.equal(v.rows.length, 1);
  const r = v.rows[0];
  assert.deepEqual([r.sku, r.available, r.on_hand, r.sold_30d, r.sold_30d_source, r.sold_7d, r.cover_days, r.price_usd, r.amazon_reco_qty, r.mapping.route], ['Card-A', 10, 16, 30, 'restock', 7, 16, 19.99, 40, 'master']);
  assert.deepEqual([v.totals.skus, v.totals.available, v.totals.inbound, v.totals.sold_30d], [1, 10, 6, 30]);
  assert.deepEqual(v.warnings, []);
});
await t('🚨 取れなかった値は 0 にしない: 空 / 列なし / 数字でない は null + 理由・在庫日数も null・列が 1 行にも無ければ帯を出す', async () => {
  const noShipped = rRow('a'); delete noShipped.Shipped;
  const v = buildUsInventoryView(payload([rRow('b', { Available: '', 'Units Sold Last 30 Days': '--' }), noShipped], [pRow('a'), pRow('b')]), { now, resolveSkus: master({}) });
  const a = v.rows.find((r) => r.sku === 'a'); const b = v.rows.find((r) => r.sku === 'b');
  assert.deepEqual([b.available, b.on_hand, b.sold_30d, b.sold_30d_source, b.cover_days], [null, null, 28, 'planning', null]);
  assert.ok(b.unknown.includes('販売可能: 空') && b.unknown.includes('30日販売(RESTOCK): 数字でない'));
  assert.deepEqual([a.shipped, a.on_hand], [null, null]);
  assert.ok(a.unknown.includes('輸送中: 列なし'));
  assert.ok(!v.warnings.some((w) => /見つからない列/.test(w.text)), 'ほかの行に列があるのに「列が無い」の帯を出している');
  const allNo = buildUsInventoryView(payload([noShipped], [pRow('a')]), { now, resolveSkus: master({}) });
  assert.ok(allNo.warnings.some((w) => /RESTOCK に見つからない列: shipped \(/.test(w.text)), '列が 1 行にも無いのに帯が無い');
});
await t('列名の大文字小文字・カンマつき数字に耐える / PLANNING にしか無い SKU は在庫の内訳を「—」(RESTOCK に行なし)', async () => {
  const v = buildUsInventoryView(payload([{ 'merchant sku': 'x', 'MERCHANT SKU': 'a', AVAILABLE: '1,234', working: '0', shipped: '0', receiving: '0', 'units sold last 30 days': '3' }], [pRow('a'), pRow('only-p')]), { now, resolveSkus: master({}) });
  const a = v.rows.find((r) => r.sku === 'a'); const p = v.rows.find((r) => r.sku === 'only-p');
  assert.deepEqual([a.available, a.on_hand], [1234, 1234]);
  assert.deepEqual([p.in_restock, p.available, p.sold_30d, p.sold_30d_source], [false, null, 28, 'planning']);
  assert.ok(p.unknown[0].startsWith('RESTOCK にこの SKU の行なし'));
});
await t('帯: RESTOCK が取れていない / 36 時間より古い / 最新の取得が失敗 (表は前の分) / 保存ファイルを読めない / 同じ SKU が 2 行 / 結びつかない SKU', async () => {
  const noR = buildUsInventoryView(payload(null, [pRow('a')]), { now, resolveSkus: master({}) });
  assert.ok(noR.warnings.some((w) => w.level === 'error' && /RESTOCK \(在庫の内訳・30日販売\) が取れていません: FATAL/.test(w.text)));
  const old = buildUsInventoryView(payload([rRow('a')], [pRow('a')]), { now: new Date('2026-09-25T12:00:00Z'), resolveSkus: master({}) });
  assert.ok(old.warnings.some((w) => /RESTOCK が古い \(37 時間前/.test(w.text)));
  const failed = buildUsInventoryView(payload([rRow('a')], [pRow('a')], { last_attempt: { business_date: '2026-09-25', attempted_at: '2026-09-24T22:00:00Z', error: 'US 403', reports: { restock: { ok: false }, planning: { ok: false } } }, file_errors: [{ file: '2026-09-25.json', error: 'bad' }] }), { now, resolveSkus: master({}) });
  assert.ok(failed.warnings.some((w) => /最新の取得 \(2026-09-25\) で RESTOCK が失敗しています: US 403。在庫の内訳・30日販売は前に取れた分/.test(w.text)));
  assert.ok(failed.warnings.some((w) => /最新の取得 \(2026-09-25\) で PLANNING が失敗しています: US 403/.test(w.text)));
  assert.ok(failed.warnings.some((w) => /保存ファイルを読めませんでした \(2026-09-25\.json\)/.test(w.text)));
  const dup = buildUsInventoryView(payload([rRow('a'), rRow('A', { Available: '99' })], [pRow('a')]), { now, resolveSkus: master({}) });
  assert.deepEqual([dup.rows.length, dup.rows[0].available], [1, 10]);
  assert.ok(dup.warnings.some((w) => /同じ SKU が 2 行: A/.test(w.text)));
  assert.ok(dup.warnings.some((w) => /結びつかない SKU が 1 件: a/.test(w.text)));
  const none = buildUsInventoryView({ last_attempt: null, latest: null, file_errors: [] }, { now });
  assert.deepEqual([none.rows.length, none.warnings[0].level], [0, 'error']);
});
await t('並び = 30日販売の多い順 (分からないものは最後)・商品コードと同じ文字列で結びつけた SKU と、調べられなかった SKU は帯で分けて出す', async () => {
  const v = buildUsInventoryView(payload([rRow('low', { 'Units Sold Last 30 Days': '1' }), rRow('none', { 'Units Sold Last 30 Days': '' }), rRow('high', { 'Units Sold Last 30 Days': '99' })], null), { now, resolveSkus: master({ low: { route: 'product_code', components: [{ ne_code: 'low', qty: 1 }] }, high: { route: 'unknown', components: [], error: 'mirror 未初期化' } }) });
  assert.deepEqual(v.rows.map((r) => r.sku), ['high', 'low', 'none']);
  assert.ok(v.warnings.some((w) => w.level === 'info' && /1 個として結びつけた SKU: low/.test(w.text)));
  assert.ok(v.warnings.some((w) => w.level === 'error' && /調べられませんでした: mirror 未初期化/.test(w.text)));
});

await t('🚨 上の合計: 1 SKU でも分からなければ合計は null + 分からない SKU の数 (0 として足さない。Codex PR1 R1 Medium 1)', async () => {
  const v = buildUsInventoryView(payload([rRow('a'), rRow('b', { Available: '', 'Units Sold Last 30 Days': '' })], null), { now, resolveSkus: master({}) });
  assert.deepEqual([v.totals.available, v.totals.sold_30d, v.totals.selling_skus, v.totals.inbound], [null, null, null, 12]);
  assert.deepEqual([v.totals_unknown.available, v.totals_unknown.sold_30d, v.totals_unknown.inbound], [1, 1, 0]);
  const ok = buildUsInventoryView(payload([rRow('a'), rRow('b', { 'Units Sold Last 30 Days': '0' })], null), { now, resolveSkus: master({}) });
  assert.deepEqual([ok.totals.available, ok.totals.sold_30d, ok.totals.selling_skus], [20, 30, 1]);
});

console.log('③-2 保存 → 読み取り → 画面 を通しで');
const endToEnd = (dir) => buildUsInventoryView({ ok: true, ...store.readLatestUsReports({ dir }) }, { now: new Date('2026-09-24T06:00:00Z'), resolveSkus: master({}) });
await t('同じ日の再実行で PLANNING だけ失敗 → 表の 7日販売は前の回の分・帯で「PLANNING が失敗・前の分」と出す (Codex PR1 R1 Medium 2)', async () => {
  const dir = path.join(tmp, 'e2e-1');
  store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-23T22:00:00Z', fetchedAt: '2026-09-23T22:01:00Z', results: { restock: [rRow('a')], planning: [pRow('a')], errors: [] } });
  assert.deepEqual(endToEnd(dir).warnings.filter((w) => !/結びつかない SKU/.test(w.text)), []);
  store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-24T01:00:00Z', fetchedAt: '2026-09-24T01:01:00Z', results: { restock: [rRow('a', { Available: '8' })], planning: null, errors: [{ report: 'planning', error: 'FATAL' }] } });
  const v = endToEnd(dir);
  assert.deepEqual([v.rows[0].available, v.rows[0].sold_7d], [8, 7]);
  assert.ok(v.warnings.some((w) => /PLANNING が失敗しています: FATAL。7日・90日販売は前に取れた分 \(2026-09-23T22:01:00Z の取得\)/.test(w.text)), JSON.stringify(v.warnings));
});
await t('🚨 取れたのに日付のファイルへ保存できない → 例外 (朝の処理は警告に落とす)・最後の取得に save_error・画面は赤帯「保存できませんでした・表は前の分」・一時ファイルを残さない (Codex PR1 R1 Medium 3)', async () => {
  const dir = path.join(tmp, 'e2e-2');
  store.saveUsReportRun({ dir, businessDate: '2026-09-23', attemptedAt: '2026-09-22T22:00:00Z', fetchedAt: '2026-09-22T22:01:00Z', results: { restock: [rRow('a')], planning: [pRow('a')], errors: [] } });
  fs.mkdirSync(path.join(dir, '2026-09-24.json'));   // 同じ名前のフォルダ = rename が必ず失敗する
  assert.throws(() => store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-23T22:00:00Z', fetchedAt: '2026-09-23T22:01:00Z', results: { restock: [rRow('a', { Available: '1' })], planning: [pRow('a')], errors: [] } }), /米国のレポートを保存できなかった/);
  const la = JSON.parse(fs.readFileSync(path.join(dir, 'last-attempt.json'), 'utf8'));
  assert.deepEqual([la.reports.restock.ok, typeof la.save_error], [true, 'string']);
  const v = endToEnd(dir);
  assert.equal(v.rows[0].available, 10, '前の日の分が出ていない');
  assert.ok(v.warnings.some((w) => w.level === 'error' && /取れたのに保存できませんでした/.test(w.text)), JSON.stringify(v.warnings));
  assert.deepEqual(fs.readdirSync(dir).filter((f) => f.endsWith('.tmp')), [], '一時ファイルが残っている');
});
await t('JSON として読めても形が違うファイル ({} / 別の日付 / ok なのに行なし) は採らず、前の日へ進む・理由を残す (Codex PR1 R1 Low 4)', async () => {
  const dir = path.join(tmp, 'e2e-3');
  store.saveUsReportRun({ dir, businessDate: '2026-09-21', attemptedAt: T0, fetchedAt: F0, results: { restock: [rRow('a')], planning: null, errors: [] } });
  fs.writeFileSync(path.join(dir, '2026-09-22.json'), JSON.stringify({ schema: 1, market: 'us', business_date: '2026-09-22', reports: { restock: { ok: true, rows: [] }, planning: { ok: false, rows: null } } }));
  fs.writeFileSync(path.join(dir, '2026-09-23.json'), JSON.stringify({ schema: 1, market: 'us', business_date: '2026-09-01', reports: {} }));
  fs.writeFileSync(path.join(dir, '2026-09-24.json'), '{}');
  const got = store.readLatestUsReports({ dir });
  assert.equal(got.latest.business_date, '2026-09-21');
  assert.deepEqual(got.file_errors.map((e) => e.file), ['2026-09-24.json', '2026-09-23.json', '2026-09-22.json']);
  assert.ok(got.file_errors.every((e) => /^形がおかしい: /.test(e.error)));
  // 同じ日の合わせで前の回のファイルが壊れていたら、今回の分で作り直す
  const r = store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: T0, fetchedAt: F0, results: { restock: [rRow('a')], planning: null, errors: [] } });
  assert.deepEqual([r.dated, store.readLatestUsReports({ dir }).latest.reports.planning.ok], [true, false]);
});

// 「そのファイルだけ書けない」を作る: 保存部品と同じ fs の renameSync を、指定した行き先のときだけ失敗させる
//   (読み取り専用にする方法は Linux では親フォルダが書ければ置き換えられてしまう = OS に依存しない形に。Codex PR1 R3 Low)
const blocked = new Set();
const realRename = fs.renameSync;
fs.renameSync = function (from, to) {
  if (blocked.has(path.resolve(String(to)))) throw Object.assign(new Error(`EPERM: operation not permitted, rename -> ${to}`), { code: 'EPERM' });
  return realRename.apply(this, arguments);
};
const lockFile = (file) => blocked.add(path.resolve(file));
const unlockFile = (file) => blocked.delete(path.resolve(file));
await t('🚨 最後の取得 (last-attempt.json) だけ書けない回: 日付のファイルの attempt の方が新しいので、PLANNING の失敗が画面に出る・メモリにも保存失敗が残る (Codex PR1 R2 Medium)', async () => {
  const dir = path.join(tmp, 'e2e-4');
  store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-23T22:00:00Z', fetchedAt: '2026-09-23T22:01:00Z', results: { restock: [rRow('a')], planning: [pRow('a')], errors: [] } });
  const la = path.join(dir, 'last-attempt.json');
  lockFile(la);
  try {
    assert.throws(() => store.saveUsReportRun({ dir, businessDate: '2026-09-24', attemptedAt: '2026-09-24T01:00:00Z', fetchedAt: '2026-09-24T01:01:00Z', results: { restock: [rRow('a', { Available: '8' })], planning: null, errors: [{ report: 'planning', error: 'FATAL' }] } }));
    const got = store.readLatestUsReports({ dir });
    assert.deepEqual([got.last_attempt.attempted_at, got.last_attempt.reports.planning.ok], ['2026-09-24T01:00:00Z', false], '古い「両方成功」を最後の取得にしている');
    assert.equal(typeof got.save_failure.error, 'string');
    const v = endToEnd(dir);
    assert.equal(v.rows[0].available, 8);
    assert.ok(v.warnings.some((w) => /PLANNING が失敗しています: FATAL/.test(w.text)), JSON.stringify(v.warnings));
    assert.ok(v.warnings.some((w) => w.level === 'error' && /米国のレポートを保存できませんでした/.test(w.text)));
  } finally { unlockFile(la); }
  // 次に保存できた回でメモリの失敗は消える
  store.saveUsReportRun({ dir, businessDate: '2026-09-25', attemptedAt: '2026-09-24T22:00:00Z', fetchedAt: '2026-09-24T22:01:00Z', results: { restock: [rRow('a')], planning: [pRow('a')], errors: [] } });
  assert.equal(store.readLatestUsReports({ dir }).save_failure, null);
});
await t('🚨 朝の処理を実際の保存部品で通す: どちらのファイルも書けない → 朝の処理は ok のまま・最後の行に一言・画面の口にはメモリの保存失敗が出る', async () => {
  const dir = path.join(tmp, 'fba-us-reports');   // 既定の置き場所 (DATA_DIR = 一時フォルダ)
  fs.mkdirSync(dir, { recursive: true });
  const dated = path.join(dir, '2026-09-26.json');
  const la = path.join(dir, 'last-attempt.json');
  fs.writeFileSync(dated, '{}'); fs.writeFileSync(la, '{}');
  lockFile(dated); lockFile(la);
  try {
    const r = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-26', fetchReports: fetchBoth, usContext: usCtx, log: quiet, warn: quiet });
    assert.deepEqual([r.ok, r.us.inserted], [true, 1]);
    assert.match(r.lastLine, /US planning=1 restock=1 \(取れたままのレポートを残せなかった: /);
    const got = store.readLatestUsReports();
    assert.equal(got.save_failure.business_date, '2026-09-26');
    const v = buildUsInventoryView({ ok: true, ...got }, { now, resolveSkus: master({}) });
    assert.ok(v.warnings.some((w) => w.level === 'error' && /米国のレポートを保存できませんでした \(2026-09-26\)/.test(w.text)));
  } finally { unlockFile(dated); unlockFile(la); }
  store._resetSaveFailure();
});
await t('形の検査: 行が null・取得時刻が時刻でない・ok が true/false でない・最後の取得の時刻がおかしい は採らない (Codex PR1 R2 Low)', async () => {
  const base = (r) => ({ schema: 1, market: 'us', business_date: '2026-09-24', reports: { restock: { ok: true, fetched_at: '2026-09-23T22:01:00Z', rows: [rRow('a')] }, planning: { ok: false, rows: null }, ...r } });
  assert.equal(store.validateDated(base({}), '2026-09-24'), null);
  assert.match(store.validateDated(base({ restock: { ok: true, fetched_at: '2026-09-23T22:01:00Z', rows: [null] } }), '2026-09-24'), /行でないもの/);
  assert.match(store.validateDated(base({ restock: { ok: true, fetched_at: 'bad-date', rows: [rRow('a')] } }), '2026-09-24'), /fetched_at が時刻でない/);
  assert.match(store.validateDated(base({ planning: { ok: 'yes', rows: null } }), '2026-09-24'), /ok が true\/false でない/);
  assert.match(store.validateDated({ ...base({}), attempt: { attempted_at: 't', reports: {} } }, '2026-09-24'), /attempt: attempted_at が時刻でない/);
  const dir = path.join(tmp, 'e2e-5');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'last-attempt.json'), JSON.stringify({ attempted_at: 'x', reports: {} }));
  const got = store.readLatestUsReports({ dir });
  assert.deepEqual([got.last_attempt, got.file_errors[0].file], [null, 'last-attempt.json']);
});

console.log('④ SKU → 自社の商品コード / miniPC の呼び出し / 画面 / 組み込み');
const { resolveUsSkus, fetchUsReportsFromMiniPC } = await imp('apps/fba-replenishment-us/router.js');
await t('SKU マスタ (構成・数量) → 無ければ商品コード直一致 (1 個) → 無ければ none / 写しを読めなければ unknown', async () => {
  const Database = (await import('better-sqlite3')).default;
  const mdb = new Database(':memory:');
  mdb.exec(`CREATE TABLE mirror_sku_resolved (seller_sku TEXT, ne_code TEXT, quantity INTEGER, source TEXT, 商品名 TEXT, sort_order INTEGER);
            CREATE TABLE mirror_products (商品コード TEXT, 商品名 TEXT);`);
  mdb.prepare('INSERT INTO mirror_sku_resolved VALUES (?,?,?,?,?,?)').run('cardstand-r-40', 'cardstand-r', 40, 'master', 'カードスタンド', 0);
  mdb.prepare('INSERT INTO mirror_sku_resolved VALUES (?,?,?,?,?,?)').run('set-1', 'b', 1, 'master', 's', 1);
  mdb.prepare('INSERT INTO mirror_sku_resolved VALUES (?,?,?,?,?,?)').run('set-1', 'a', 2, 'master', 's', 0);
  mdb.prepare('INSERT INTO mirror_sku_resolved VALUES (?,?,?,?,?,?)').run('old-auto', 'z', 1, 'auto', 'z', 0);
  mdb.prepare('INSERT INTO mirror_products VALUES (?,?)').run('SandalwoodIncense30', '白檀線香');
  const m = resolveUsSkus(['cardstand-r-40', 'set-1', 'sandalwoodincense30', 'wirestand2.0_1', 'old-auto'], { mdb });
  assert.deepEqual(m.get('cardstand-r-40'), { route: 'master', components: [{ ne_code: 'cardstand-r', qty: 40 }], name: 'カードスタンド' });
  assert.deepEqual(m.get('set-1').components, [{ ne_code: 'a', qty: 2 }, { ne_code: 'b', qty: 1 }]);
  assert.deepEqual(m.get('sandalwoodincense30'), { route: 'product_code', components: [{ ne_code: 'SandalwoodIncense30', qty: 1 }], name: '白檀線香' });
  assert.deepEqual([m.get('wirestand2.0_1').route, m.get('old-auto').route], ['none', 'none'], "source='auto' の古い行を採っている");
  const broken = resolveUsSkus(['a'], { mdb: { prepare: () => { throw new Error('no such table'); } } });
  assert.deepEqual(broken.get('a'), { route: 'unknown', components: [], error: 'no such table' });
});
const jsonRes = (status, body, ct = 'application/json') => ({ status, ok: status >= 200 && status < 300, headers: { get: () => ct }, json: async () => body, text: async () => JSON.stringify(body) });
await t('miniPC: 認証ヘッダを付けた GET / 404 は「miniPC の更新がまだ」/ 503 は 1 回だけやり直す / ok でない応答は例外', async () => {
  const calls = [];
  const ok = await fetchUsReportsFromMiniPC({ fetchImpl: async (url, init) => { calls.push([url, init.headers.Authorization.startsWith('Bearer'), init.redirect]); return jsonRes(200, { ok: true, latest: null }); } });
  assert.deepEqual([ok.ok, calls[0][0].endsWith('/service-api/fba/us/reports/latest'), calls[0][1], calls[0][2]], [true, true, true, 'manual']);
  await assert.rejects(fetchUsReportsFromMiniPC({ fetchImpl: async () => jsonRes(404, {}) }), /miniPC の更新と WarehouseServer の再起動がまだ/);
  let n = 0;
  const retried = await fetchUsReportsFromMiniPC({ fetchImpl: async () => (++n === 1 ? jsonRes(503, {}, 'text/html') : jsonRes(200, { ok: true })) });
  assert.deepEqual([retried.ok, n], [true, 2]);
  let m = 0;
  await assert.rejects(fetchUsReportsFromMiniPC({ fetchImpl: async () => { m++; return jsonRes(401, {}); } }), /断られた/);
  assert.equal(m, 1, '401 をやり直している');
  await assert.rejects(fetchUsReportsFromMiniPC({ fetchImpl: async () => jsonRes(200, { ok: false }) }), /応答の形がおかしい/);
});
await t('画面: EJS が描ける・画面の JS が文法として正しい・API は絶対パス (相対だと /apps/api/... に飛ぶ)', async () => {
  const ejs = (await import('ejs')).default;
  const html = await ejs.renderFile(path.join(root, 'views', 'fba-replenishment-us.ejs'), { username: 'u@example.com', displayName: '中原' });
  assert.match(html, /<h1><span>🗽<\/span> 米国FBA在庫補充<\/h1>/);
  assert.match(html, /var ALLOC_API = '\/apps\/fba-replenishment-us\/api\/allocation';/);
  assert.match(html, /中原/);
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  assert.doesNotThrow(() => new Function(script), '画面の JS が文法エラー');
  assert.match(script, /var API = '\/apps\/fba-replenishment-us\/api\/inventory';/);
});
await t('組み込み: 権限つきで日本版より前に mount / ポータルのカード (id = 権限のキー) / miniPC の口は fba.db を読まない', async () => {
  const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  const us = server.indexOf("app.use('/apps/fba-replenishment-us', requireAppAccess('fba-replenishment-us'), fbaUsRouter);");
  const jp = server.indexOf("app.use('/apps/fba-replenishment', requireAppAccess('fba-replenishment'), fbaRouter);");
  assert.ok(us > 0 && jp > us);
  const { apps } = await imp('lib/portal-apps.js');
  const card = apps.find((a) => a.id === 'fba-replenishment-us');
  assert.deepEqual([card.path, card.category, card.status], ['/apps/fba-replenishment-us', 'fba', 'active']);
  const svc = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'fba-service.js'), 'utf8');
  assert.match(svc, /router\.get\('\/us\/reports\/latest', \(req, res\) => \{\r?\n  try \{\r?\n    okResponse\(res, readLatestUsReports\(\)\);/);
});

fs.rmSync(tmp, { recursive: true, force: true });
console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
