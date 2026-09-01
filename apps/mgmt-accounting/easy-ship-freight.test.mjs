// 売上同期の自動運賃 (FBA運賃 / Easy Ship運賃) の統合テスト
//   一時ディレクトリに warehouse-mirror.db を実初期化 (DATA_DIR) し、mart_amazon_monthly_summary に by_segment を入れて
//   syncSegmentSalesForMonth() を実行 → mgmt_freight_costs の自動行を検証する。
//   node --test apps/mgmt-accounting/easy-ship-freight.test.mjs
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// DATA_DIR は db.js の import 時に評価されるため、動的 import の前に設定する
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mgmt-easyship-'));
process.env.DATA_DIR = tmp;
const { initMirrorDB } = await import('../warehouse-mirror/db.js');
const { syncSegmentSalesForMonth, AUTO_FREIGHT, CARRIERS } = await import('./router.js');

const EASY = 'Amazon Easy Ship料金';
let db;
before(() => { db = initMirrorDB(); });
after(() => { try { db.close(); } catch {} try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {} });

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
  assert.equal(r.fba_freight_tax_excluded, 3000); // 後方互換
  const rows = freightRows('2026-07');
  assert.deepEqual(rows, [
    { carrier: 'Easy Ship運賃', amount: 5000, cost_scope: 'shared', note: 'auto from mart_amazon_monthly_summary.by_segment.Amazon Easy Ship料金', entered_by: 'system-sync' },
    { carrier: 'FBA運賃', amount: 3000, cost_scope: 'shared', note: 'auto from mart_amazon_monthly_summary.by_segment.FBA手数料', entered_by: 'system-sync' },
  ]);
  // PF手数料 (seg1) に Easy Ship は入らない (other 行は segment_sales の対象外)
  const seg1 = db.prepare("SELECT pf_fee FROM mart_monthly_segment_sales WHERE year_month = '2026-07' AND mall_id = 'amazon_jp' AND segment = 1").get();
  assert.equal(seg1.pf_fee, Math.round(10000 / 1.1));
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM mart_monthly_segment_sales WHERE year_month = '2026-07' AND mall_id = 'amazon_jp'").get().n, 1);
});

test('売上同期の再実行: 列が無い月 (PR #1043 より前の確定・旧月) は Easy Ship運賃 を作らず、stale な自動行は消える', () => {
  // 同じ月を「列なし」の by_segment で再確定した想定 → Easy Ship運賃 の自動行は消え、FBA運賃 は残る
  putAmazonSummary('2026-07', {
    '1': seg({ 商品売上: 100000, 手数料: -10000, FBA手数料: -1100, 原価合計: 40000 }),
    other: seg({ 手数料: -5500, FBA手数料: -2200, 合計: -7700 }),
  });
  const r = syncSegmentSalesForMonth(db, '2026-07', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'FBA運賃': 3000 });
  assert.deepEqual(freightRows('2026-07').map(x => [x.carrier, x.amount]), [['FBA運賃', 3000]]);
});

test('売上同期: 手入力の運賃行は自動行の消去対象にならない', () => {
  db.prepare(`INSERT INTO mgmt_freight_costs (year_month, carrier, amount, cost_scope, target_segment, target_mall_id, note, entered_by, entered_at, updated_at)
    VALUES ('2026-06', 'ヤマト', 12345, 'shared', NULL, NULL, '請求書', 'tester', ?, ?)`).run(NOW, NOW);
  putAmazonSummary('2026-06', { other: seg({ [EASY]: -1100, 合計: -1100 }) });
  const r = syncSegmentSalesForMonth(db, '2026-06', NOW);
  assert.deepEqual(r.auto_freight_tax_excluded, { 'Easy Ship運賃': 1000 });
  assert.deepEqual(freightRows('2026-06').map(x => [x.carrier, x.amount]), [['Easy Ship運賃', 1000], ['ヤマト', 12345]]);
});
