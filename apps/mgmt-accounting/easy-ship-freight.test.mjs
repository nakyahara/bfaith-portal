// 売上同期の自動運賃 (FBA運賃 / Easy Ship運賃) の統合テスト
//   一時ディレクトリに warehouse-mirror.db を実初期化 (DATA_DIR) し、mart_amazon_monthly_summary に by_segment を入れて
//   syncSegmentSalesForMonth() / syncAndRefreshMonth() を実行 → mgmt_freight_costs の自動行・PL再計算を検証する。
//   node --test apps/mgmt-accounting/easy-ship-freight.test.mjs
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// DATA_DIR は db.js の import 時に評価されるため、動的 import の前に設定する。import に失敗しても一時ディレクトリは片付ける
const prevDataDir = process.env.DATA_DIR;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mgmt-easyship-'));
process.env.DATA_DIR = tmp;
let db, syncSegmentSalesForMonth, syncAndRefreshMonth, AUTO_FREIGHT, CARRIERS;
try {
  const dbMod = await import('../warehouse-mirror/db.js');
  const routerMod = await import('./router.js');
  ({ syncSegmentSalesForMonth, syncAndRefreshMonth, AUTO_FREIGHT, CARRIERS } = routerMod);
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
const insertManualFreight = (ym, carrier, amount, user) => db.prepare(`INSERT INTO mgmt_freight_costs
  (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
  VALUES (?, ?, ?, 'shared', NULL, NULL, '請求書', ?, ?, ?)`).run(ym, carrier, amount, user, NOW, NOW);

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

test('売上同期: 別 carrier の手入力行は消えず、同名 carrier の手入力行は上書きせずスキップして警告を返す', () => {
  insertManualFreight('2026-06', 'ヤマト', 12345, 'tester');
  insertManualFreight('2026-06', 'Easy Ship運賃', 777, 'tester'); // 画面からは入れられないが API/旧データで存在し得る
  putAmazonSummary('2026-06', { other: seg({ [EASY]: -1100, FBA手数料: -2200, 合計: -3300 }) });
  const r = syncSegmentSalesForMonth(db, '2026-06', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 2000 });
  assert.deepEqual(r.auto_freight_skipped, { 'Easy Ship運賃': { auto_amount: 1000, existing_amount: 777, entered_by: 'tester' } });
  assert.deepEqual(freightRows('2026-06'), [
    { carrier: 'Easy Ship運賃', amount: 777, cost_scope: 'shared', note: '請求書', entered_by: 'tester' }, // 手入力のまま
    { carrier: 'FBA運賃', amount: 2000, cost_scope: 'shared', note: 'auto from mart_amazon_monthly_summary.by_segment.FBA手数料', entered_by: 'system-sync' },
    { carrier: 'ヤマト', amount: 12345, cost_scope: 'shared', note: '請求書', entered_by: 'tester' },
  ]);
  // historical-import 由来 (Excel 取込・値は自動計算と同じ) は従来どおり自動値で更新される
  db.prepare("UPDATE mgmt_freight_costs SET entered_by = 'historical-import', note = NULL WHERE year_month = '2026-06' AND carrier = 'Easy Ship運賃'").run();
  const r2 = syncSegmentSalesForMonth(db, '2026-06', NOW);
  assert.deepEqual(r2.auto_freight_tax_excluded, { 'FBA運賃': 2000, 'Easy Ship運賃': 1000 });
  assert.deepEqual(r2.auto_freight_skipped, {});
  const es = freightRows('2026-06').find(x => x.carrier === 'Easy Ship運賃');
  assert.equal(es.amount, 1000);
  assert.equal(es.entered_by, 'system-sync');
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
