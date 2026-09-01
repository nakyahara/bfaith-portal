// 売上同期の自動運賃 (FBA運賃 / Easy Ship運賃) の統合テスト
//   一時ディレクトリに warehouse-mirror.db を実初期化 (DATA_DIR) し、mart_amazon_monthly_summary に by_segment を入れて
//   syncSegmentSalesForMonth() / syncAndRefreshMonth() / runMgmtAutoSync() / POST /api/freight を実行して検証する。
//   node --test apps/mgmt-accounting/easy-ship-freight.test.mjs
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import express from 'express';

// DATA_DIR は db.js の import 時に評価されるため、動的 import の前に設定する。import に失敗しても一時ディレクトリは片付ける
const prevDataDir = process.env.DATA_DIR;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mgmt-easyship-'));
process.env.DATA_DIR = tmp;
let db, router, syncSegmentSalesForMonth, syncAndRefreshMonth, runMgmtAutoSync, AUTO_FREIGHT, CARRIERS;
try {
  const dbMod = await import('../warehouse-mirror/db.js');
  const routerMod = await import('./router.js');
  ({ syncSegmentSalesForMonth, syncAndRefreshMonth, runMgmtAutoSync, AUTO_FREIGHT, CARRIERS } = routerMod);
  router = routerMod.default;
  db = dbMod.initMirrorDB();
} catch (e) {
  fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  if (prevDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prevDataDir;
  throw e;
}
after(() => {
  if (db) db.close();
  if (prevDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prevDataDir;
  fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 }); // 失敗は握り潰さない
});

const EASY = 'Amazon Easy Ship料金';
const NOW = '2026-09-01 08:00:00';
function seg(over = {}) {
  return { 商品売上: 0, 商品の売上税: 0, 配送料: 0, 配送料の税金: 0, ギフト包装手数料: 0, ギフト包装の税金: 0, Amazonポイント費用: 0,
    プロモーション割引額: 0, プロモーション割引の税金: 0, 手数料: 0, FBA手数料: 0, トランザクション他: 0, その他: 0, 合計: 0, 原価合計: 0, 行数: 0, ...over };
}
function putAmazonSummary(ym, bySegment) {
  db.prepare(`INSERT OR REPLACE INTO mart_amazon_monthly_summary
    (year_month, total_rows, resolved_count, unresolved_count, by_tax, by_segment, excluded, mf_row, ad_cost, confirmed_at, csv_filename)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(ym, 0, 0, 0, '{}', JSON.stringify(bySegment), '{}', '{}', 0, NOW, 'test');
}
const freightRows = ym => db.prepare('SELECT carrier, amount, cost_scope, note, entered_by FROM mgmt_freight_costs WHERE year_month = ? ORDER BY carrier').all(ym);
const insertFreight = (ym, carrier, amount, user, note = '請求書') => db.prepare(`INSERT INTO mgmt_freight_costs
  (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
  VALUES (?, ?, ?, 'shared', NULL, NULL, ?, ?, ?, ?)`).run(ym, carrier, amount, note, user, NOW, NOW);

// POST /api/freight を express 経由で叩く (セッションは email を持つスタブ)
async function postFreight(body, email = 'tester@example.com') {
  const app = express();
  app.use(express.json());
  app.use('/m', (req, _res, next) => { req.session = { authenticated: true, email }; next(); }, router);
  const server = app.listen(0);
  try {
    const res = await fetch('http://127.0.0.1:' + server.address().port + '/m/api/freight', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    return { status: res.status, json: await res.json() };
  } finally {
    server.close();
  }
}

test('AUTO_FREIGHT / CARRIERS: Easy Ship運賃 が FBA運賃 の次に定義されている', () => {
  assert.deepEqual(AUTO_FREIGHT.map(a => a.carrier), ['FBA運賃', 'Easy Ship運賃']);
  assert.equal(AUTO_FREIGHT[1].segKey, EASY);
  assert.equal(CARRIERS.indexOf('Easy Ship運賃'), CARRIERS.indexOf('FBA運賃') + 1);
});

test('売上同期: Easy Ship運賃 が by_segment の Amazon Easy Ship料金 (全セグメント合計・税込負数) から |x|/1.1 で自動投入される', () => {
  // PR #1043 以降の確定月: by_segment に 3列 (Easy Ship / 在庫保管 / 長期) がある。Easy Ship は other 行 (SKUなし行) にだけ入る
  putAmazonSummary('2026-07', {
    '1': seg({ 商品売上: 100000, 手数料: -10000, FBA手数料: -1100, [EASY]: 0, FBA在庫保管手数料: 0, FBA長期在庫保管手数料: 0, 原価合計: 40000 }),
    other: seg({ 手数料: -5500, FBA手数料: -2200, その他: 0, 合計: -7700, [EASY]: -5500, FBA在庫保管手数料: -1100, FBA長期在庫保管手数料: -550 }),
  });
  const r = syncSegmentSalesForMonth(db, '2026-07', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 3000, 'Easy Ship運賃': 5000 }); // |-3300|/1.1, |-5500|/1.1
  assert.deepEqual(r.auto_freight_skipped, {});
  assert.equal(r.fba_freight_tax_excluded, 3000); // 後方互換
  assert.deepEqual(freightRows('2026-07'), [
    { carrier: 'Easy Ship運賃', amount: 5000, cost_scope: 'shared', note: 'auto from mart_amazon_monthly_summary.by_segment.Amazon Easy Ship料金', entered_by: 'system-sync' },
    { carrier: 'FBA運賃', amount: 3000, cost_scope: 'shared', note: 'auto from mart_amazon_monthly_summary.by_segment.FBA手数料', entered_by: 'system-sync' },
  ]);
  // PF手数料 (seg1) に Easy Ship は入らない (other 行は segment_sales の対象外)
  const seg1 = db.prepare("SELECT pf_fee FROM mart_monthly_segment_sales WHERE year_month = '2026-07' AND mall_id = 'amazon_jp' AND segment = 1").get();
  assert.equal(seg1.pf_fee, Math.round(10000 / 1.1));
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM mart_monthly_segment_sales WHERE year_month = '2026-07' AND mall_id = 'amazon_jp'").get().n, 1);
});

test('売上同期: 取り消し/訂正の正値が混ざる月は符号付きネットで計算し、ネットゼロなら行を作らず stale 自動行も消える', () => {
  // 6月形式 (別名2種) + 取り消しの正値: -5500 + 1100 = -4400 → 4000
  putAmazonSummary('2026-08', {
    '1': seg({ 商品売上: 50000, [EASY]: 0 }),
    other: seg({ [EASY]: -5500 + 1100, FBA手数料: -1100 }),
  });
  let r = syncSegmentSalesForMonth(db, '2026-08', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 1000, 'Easy Ship運賃': 4000 });
  // 再確定でネットゼロになった → Easy Ship運賃 の自動行は消える (FBA運賃 は残る)
  putAmazonSummary('2026-08', { '1': seg({ 商品売上: 50000 }), other: seg({ [EASY]: -1100 + 1100, FBA手数料: -1100 }) });
  r = syncSegmentSalesForMonth(db, '2026-08', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 1000 });
  assert.deepEqual(freightRows('2026-08').map(x => [x.carrier, x.amount]), [['FBA運賃', 1000]]);
});

test('売上同期の再実行: 列が無い月 (PR #1043 より前の確定・旧月) は Easy Ship運賃 を作らず、stale な自動行は消える', () => {
  putAmazonSummary('2026-07', {
    '1': seg({ 商品売上: 100000, 手数料: -10000, FBA手数料: -1100, 原価合計: 40000 }),
    other: seg({ 手数料: -5500, FBA手数料: -2200, 合計: -7700 }),
  });
  const r = syncSegmentSalesForMonth(db, '2026-07', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 3000 });
  assert.deepEqual(freightRows('2026-07').map(x => [x.carrier, x.amount]), [['FBA運賃', 3000]]);
});

test('売上同期: historical-import 由来 (note なし) の自動 carrier 行は、ネットゼロ/列なしの月で stale として消える (DELETE と UPSERT の所有権条件が同じ)', () => {
  insertFreight('2026-05', 'FBA運賃', 9999, 'historical-import', null);
  insertFreight('2026-05', 'Easy Ship運賃', 8888, 'historical-import', null);
  // FBA手数料 はネットゼロ・Easy Ship 列は無い月
  putAmazonSummary('2026-05', { '1': seg({ 商品売上: 10000, FBA手数料: -500 }), other: seg({ FBA手数料: 500 }) });
  const r = syncSegmentSalesForMonth(db, '2026-05', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, {});
  assert.deepEqual(r.auto_freight_skipped, {});
  assert.deepEqual(freightRows('2026-05'), []); // 古い運賃が残らない
  // 金額があれば historical-import 行は自動値で更新される (従来どおり)
  insertFreight('2026-05', 'FBA運賃', 9999, 'historical-import', null);
  putAmazonSummary('2026-05', { '1': seg({ 商品売上: 10000, FBA手数料: -1100 }) });
  const r2 = syncSegmentSalesForMonth(db, '2026-05', NOW);
  assert.deepEqual(r2.auto_freight_tax_excluded, { 'FBA運賃': 1000 });
  assert.deepEqual(freightRows('2026-05').map(x => [x.carrier, x.amount, x.entered_by]), [['FBA運賃', 1000, 'system-sync']]);
});

test('売上同期: 別 carrier の手入力行は消えず、同名 carrier の手入力行は上書きせずスキップして警告を返す', () => {
  insertFreight('2026-06', 'ヤマト', 12345, 'tester');
  insertFreight('2026-06', 'Easy Ship運賃', 777, 'tester'); // 旧データ等で存在し得る手入力行
  putAmazonSummary('2026-06', { other: seg({ [EASY]: -1100, FBA手数料: -2200, 合計: -3300 }) });
  const r = syncSegmentSalesForMonth(db, '2026-06', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 2000 });
  assert.deepEqual(r.auto_freight_skipped, { 'Easy Ship運賃': { auto_amount: 1000, existing_amount: 777, entered_by: 'tester' } });
  assert.deepEqual(freightRows('2026-06'), [
    { carrier: 'Easy Ship運賃', amount: 777, cost_scope: 'shared', note: '請求書', entered_by: 'tester' }, // 手入力のまま
    { carrier: 'FBA運賃', amount: 2000, cost_scope: 'shared', note: 'auto from mart_amazon_monthly_summary.by_segment.FBA手数料', entered_by: 'system-sync' },
    { carrier: 'ヤマト', amount: 12345, cost_scope: 'shared', note: '請求書', entered_by: 'tester' },
  ]);
});

test('POST /api/freight: 自動 carrier は 400 で拒否、手入力の競合更新では entered_by が更新される', async () => {
  // 自動 carrier を含む保存は拒否 (画面は送らないが API 直叩き・旧運用を防ぐ)
  const rejected = await postFreight({ year_month: '2026-06', items: [{ carrier: 'FBA運賃', amount: 1 }, { carrier: 'ヤマト', amount: 2 }] });
  assert.equal(rejected.status, 400);
  assert.equal(rejected.json.error, 'auto_carrier');
  assert.deepEqual(rejected.json.carriers, ['FBA運賃']);
  assert.equal(freightRows('2026-06').find(x => x.carrier === 'FBA運賃').amount, 2000); // 変わっていない
  // 既存 (別ユーザー) の手入力行を更新すると entered_by も更新される
  const ok = await postFreight({ year_month: '2026-06', items: [{ carrier: 'ヤマト', amount: 500, cost_scope: 'shared', note: '訂正' }] }, 'second@example.com');
  assert.equal(ok.status, 200);
  const yamato = freightRows('2026-06').find(x => x.carrier === 'ヤマト');
  assert.deepEqual([yamato.amount, yamato.note, yamato.entered_by], [500, '訂正', 'second@example.com']);
  // items が配列でなければ 400
  assert.equal((await postFreight({ year_month: '2026-06', items: 'x' })).status, 400);
});

test('確定済み月の再同期: Easy Ship運賃 が shared 運賃として PL に按分され、粗利が更新される (confirmed 維持)', () => {
  // 2026-07 を確定済みにしてから、Easy Ship 列付きの by_segment で再同期
  db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_closing (year_month, fiscal_year, fiscal_month, status, freight_total, material_total, confirmed_at, confirmed_by, calc_version)
    VALUES ('2026-07', 2026, 1, 'confirmed', 0, 0, '2026-08-05 09:00:00', 'tester', 'v1')`).run();
  putAmazonSummary('2026-07', {
    '1': seg({ 商品売上: 100000, 手数料: -10000, FBA手数料: -1100, [EASY]: 0, 原価合計: 40000 }),
    other: seg({ 手数料: -5500, FBA手数料: -2200, [EASY]: -5500, 合計: -7700 }),
  });
  const r = syncAndRefreshMonth(db, '2026-07', NOW);
  assert.equal(r.refreshed, true);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 3000, 'Easy Ship運賃': 5000 });
  const pl = db.prepare("SELECT sales, cost, pf_fee, freight, gross_profit FROM mgmt_monthly_pl WHERE year_month = '2026-07' AND mall_id = 'amazon_jp' AND segment = 1").get();
  // 唯一のセグメント行なので shared 運賃 (3000 + 5000) が全額按分される
  assert.equal(pl.freight, 8000);
  assert.equal(pl.gross_profit, pl.sales - pl.cost - pl.pf_fee - pl.freight);
  const closing = db.prepare("SELECT status, freight_total, confirmed_at FROM mgmt_monthly_closing WHERE year_month = '2026-07'").get();
  assert.equal(closing.status, 'confirmed');
  assert.equal(closing.freight_total, 8000);
  assert.equal(closing.confirmed_at, '2026-08-05 09:00:00'); // 人が確定した日時は保持
});

test('自動同期スケジューラ: スキップは例外にならず件数と月×carrier を集約して返す (凍結月はスキップ)', () => {
  // このテスト内で前提を作る (他テストの状態に依存しない): 2026-04 に手入力の Easy Ship運賃 + Easy Ship 列付き summary → 1件スキップ。
  // 2026-02 (凍結) は summary があっても同期されない
  insertFreight('2026-04', 'Easy Ship運賃', 555, 'tester');
  putAmazonSummary('2026-04', { '1': seg({ 商品売上: 1000 }), other: seg({ [EASY]: -1100 }) });
  putAmazonSummary('2026-02', { other: seg({ [EASY]: -1100 }) });
  const r = runMgmtAutoSync(db);
  assert.ok(r.months >= 2);
  assert.ok(r.freight_skipped_detail.includes('2026-04 Easy Ship運賃'), JSON.stringify(r.freight_skipped_detail));
  assert.equal(r.freight_skipped, r.freight_skipped_detail.length);
  assert.ok(!r.freight_skipped_detail.some(s => s.startsWith('2026-02 ')));
  assert.deepEqual(freightRows('2026-04').map(x => [x.carrier, x.amount, x.entered_by]), [['Easy Ship運賃', 555, 'tester']]); // 手入力のまま
  assert.deepEqual(freightRows('2026-02'), []); // 凍結月には自動運賃を作らない
});
