#!/usr/bin/env node
/**
 * test-amazon-ads-fetch.mjs — apps/warehouse/fetch-amazon-ads.js (Amazon SP の SKU 別広告費の取込) の保存の部分の試験。API には繋がない。
 *   2026-09-27 の作り直し (Company DB構想 11 の Codex 設計レビュー D1): 日ごとの置き換え / 取得の完全性の記録 ads_fetch_days / 古い取得で戻さない /
 *   値の検査 (読めない行があれば期間ごと書かない) / 数量に購入件数を混ぜない / JST の日付
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { aggregateReportRows, saveAdProduct, parseArgs, jstToday, datesBetween, REPORT_TYPE } from '../apps/warehouse/fetch-amazon-ads.js';

let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };

function openDb() {
  const db = new Database(':memory:');
  // 本番 (warehouse.db) の fact_ad_spend と同じ定義
  db.exec(`CREATE TABLE fact_ad_spend (日付 TEXT NOT NULL, モール TEXT NOT NULL, キャンペーンID TEXT NOT NULL, 広告タイプ TEXT NOT NULL, ターゲット TEXT NOT NULL, ターゲット粒度 TEXT NOT NULL,
    クリック数 INTEGER DEFAULT 0, インプレッション INTEGER DEFAULT 0, 広告費 REAL DEFAULT 0, 広告経由売上 REAL DEFAULT 0, 広告経由数量 INTEGER DEFAULT 0, ingested_at TEXT NOT NULL,
    PRIMARY KEY (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度))`);
  return db;
}
const row = (x) => ({ date: '2026-09-01', campaignId: 111, advertisedSku: 'SKU-A', advertisedAsin: 'B000000001', impressions: 100, clicks: 10, cost: 123.45, sales1d: 2000, unitsSoldClicks1d: 2, purchases1d: 1, ...x });
const rowsOf = (db, d) => db.prepare(`select キャンペーンID c, ターゲット t, ターゲット粒度 g, クリック数 k, 広告費 cost, 広告経由売上 s, 広告経由数量 q from fact_ad_spend where 日付 = ? order by c, t`).all(d);
const recOf = (db, d) => db.prepare(`select generation, report_id, row_count, cost_total from ads_fetch_days where report_type = ? and profile_id = 'P1' and date_jst = ?`).get(REPORT_TYPE, d);
const W = { from: '2026-09-01', to: '2026-09-03' };

console.log('合算と検査');
t('同じ (日, キャンペーン, 対象) の広告グループを合算・費用は 2 桁に丸める・SKU があれば SKU (小文字)・無ければ ASIN・どちらも無ければ粒度 none (費用を捨てない)', () => {
  const m = aggregateReportRows([row({}), row({ cost: 0.1, clicks: 1, impressions: 5, sales1d: 0, unitsSoldClicks1d: 0 }), row({ advertisedSku: '', advertisedAsin: 'B0ASIN' }), row({ advertisedSku: null, advertisedAsin: null, cost: 5 })], W);
  const d1 = m.get('2026-09-01');
  assert.deepEqual(d1.map((a) => [a.target, a.granularity, a.clicks, a.impressions, a.cost, a.sales1d, a.qty1d]).sort(), [
    ['', 'none', 10, 100, 5, 2000, 2], ['b0asin', 'asin', 10, 100, 123.45, 2000, 2], ['sku-a', 'sku', 11, 105, 123.55, 2000, 2]].sort());
});
t('🚨 数量は unitsSoldClicks1d だけ (購入件数 purchases1d を混ぜない)・売上 (sales1d) と数量が無い行は null (0 にしない)・合算の中に 1 つでも無ければ null', () => {
  const m = aggregateReportRows([row({ unitsSoldClicks1d: undefined, purchases1d: 3 }), row({ date: '2026-09-02', sales1d: null })], W);
  assert.deepEqual([m.get('2026-09-01')[0].qty1d, m.get('2026-09-01')[0].sales1d, m.get('2026-09-02')[0].sales1d, m.get('2026-09-02')[0].qty1d], [null, 2000, null, 2]);
});
t('🚨 読めない行は例外 (期間ごと書かない): 期間外・実在しない日付・キャンペーン無し・費用/クリック/表示の欠落・負・数でない・小数のクリック・配列でない', () => {
  throws(() => aggregateReportRows([row({ date: '2026-08-31' })], W), /期間/);
  throws(() => aggregateReportRows([row({ date: '2026-02-30' })], { from: '2026-02-01', to: '2026-03-31' }), /日付/);
  throws(() => aggregateReportRows([row({ campaignId: '' })], W), /campaignId/);
  throws(() => aggregateReportRows([row({ cost: undefined })], W), /cost/);
  throws(() => aggregateReportRows([row({ cost: -1 })], W), /cost/);
  throws(() => aggregateReportRows([row({ cost: 'abc' })], W), /cost/);
  throws(() => aggregateReportRows([row({ clicks: 1.5 })], W), /clicks/);
  throws(() => aggregateReportRows([row({ impressions: null })], W), /impressions/);
  throws(() => aggregateReportRows([row({ sales1d: -5 })], W), /sales1d/);
  throws(() => aggregateReportRows({ rows: [] }, W), /配列/);
});

console.log('日ごとの置き換えと取得の記録');
t('期間の日を置き換え・行の無い日も「0 行で取れた」と記録 (費用の合計・行数・レポート・世代)', () => {
  const db = openDb();
  const r = saveAdProduct(db, [row({}), row({ campaignId: 222, advertisedSku: 'SKU-B', cost: 10 }), row({ date: '2026-09-02' })], { ...W, generation: 1000, reportId: 'R1', profileId: 'P1', now: () => 'T1' });
  assert.deepEqual([r.days, r.rows, r.skippedOlder], [3, 3, []]);
  assert.deepEqual(rowsOf(db, '2026-09-01').map((x) => [x.c, x.t, x.cost]), [['111', 'sku-a', 123.45], ['222', 'sku-b', 10]]);
  assert.deepEqual(recOf(db, '2026-09-01'), { generation: 1000, report_id: 'R1', row_count: 2, cost_total: 133.45 });
  assert.deepEqual(recOf(db, '2026-09-03'), { generation: 1000, report_id: 'R1', row_count: 0, cost_total: 0 });
});
t('🚨 取り直しで行が減った日は消える (以前の UPSERT では残り続けた)・全部の行が消えた日も 0 行に置き換わる・ほかの広告タイプ / モールの行は触らない', () => {
  const db = openDb();
  saveAdProduct(db, [row({}), row({ campaignId: 222, advertisedSku: 'SKU-B' }), row({ date: '2026-09-02' })], { ...W, generation: 1000, reportId: 'R1', profileId: 'P1' });
  db.prepare(`insert into fact_ad_spend (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度, 広告費, ingested_at) values ('2026-09-01', 'amazon', '999', 'SB', 'x', 'sku', 7, 'T'), ('2026-09-01', 'rakuten', '999', 'SP', 'x', 'sku', 7, 'T')`).run();
  const r = saveAdProduct(db, [row({ cost: 50 })], { ...W, generation: 2000, reportId: 'R2', profileId: 'P1' });
  assert.deepEqual([r.days, r.rows], [3, 1]);
  assert.deepEqual(rowsOf(db, '2026-09-01').map((x) => [x.c, x.t, x.cost]), [['111', 'sku-a', 50], ['999', 'x', 7], ['999', 'x', 7]]);   // SB と楽天はそのまま
  assert.deepEqual([rowsOf(db, '2026-09-02').length, recOf(db, '2026-09-02').row_count, recOf(db, '2026-09-02').report_id], [0, 0, 'R2']);
});
t('🚨 後から届いた古い取得 (世代が小さい) では、新しい取得で置き換えた日を戻さない (その日だけ飛ばす)。記録の無い日・古い日は置き換える', () => {
  const db = openDb();
  saveAdProduct(db, [row({ cost: 50 })], { from: '2026-09-01', to: '2026-09-01', generation: 2000, reportId: 'R2', profileId: 'P1' });
  const r = saveAdProduct(db, [row({ cost: 999 }), row({ date: '2026-09-02', cost: 1 })], { ...W, generation: 1500, reportId: 'R1', profileId: 'P1' });
  assert.deepEqual([r.days, r.skippedOlder], [2, ['2026-09-01']]);
  assert.deepEqual([rowsOf(db, '2026-09-01')[0].cost, recOf(db, '2026-09-01').report_id, rowsOf(db, '2026-09-02')[0].cost, recOf(db, '2026-09-02').report_id], [50, 'R2', 1, 'R1']);
});
t('🚨 読めない行が 1 つでもあるレポートは何も書かない (前の取得の行も記録も残る)', () => {
  const db = openDb();
  saveAdProduct(db, [row({})], { ...W, generation: 1000, reportId: 'R1', profileId: 'P1' });
  throws(() => saveAdProduct(db, [row({ cost: 1 }), row({ date: '2026-09-02', clicks: 'x' })], { ...W, generation: 2000, reportId: 'R2', profileId: 'P1' }), /clicks/);
  assert.deepEqual([rowsOf(db, '2026-09-01')[0].cost, recOf(db, '2026-09-01').report_id], [123.45, 'R1']);
});
t('世代・レポート・期間が無ければ例外 (記録の無い置き換えをしない)', () => {
  const db = openDb();
  throws(() => saveAdProduct(db, [], { ...W, generation: 0, reportId: 'R', profileId: 'P1' }), /generation/);
  throws(() => saveAdProduct(db, [], { ...W, generation: 1, reportId: '', profileId: 'P1' }), /reportId/);
  throws(() => saveAdProduct(db, [], { from: '2026-09-03', to: '2026-09-01', generation: 1, reportId: 'R', profileId: 'P1' }), /期間/);
});

console.log('日付 (JST)');
t('ふだんは JST の昨日まで直近 N 日 (朝 7:30 JST = UTC の前日 22:30 でも JST で数える)・--from/--to はそのまま・不正は例外', () => {
  const now = Date.parse('2026-09-26T22:30:00Z');   // JST 9/27 07:30
  assert.equal(jstToday(now), '2026-09-27');
  assert.deepEqual(parseArgs(['7'], now), { days: 30, from: '2026-08-28', to: '2026-09-26' });   // daily-sync は引数が無いと '7' を足す (無視される)
  assert.deepEqual(parseArgs(['--days', '3'], now), { days: 3, from: '2026-09-24', to: '2026-09-26' });
  assert.deepEqual(parseArgs(['--from', '2026-04-01', '--to', '2026-04-30'], now), { days: 30, from: '2026-04-01', to: '2026-04-30' });
  throws(() => parseArgs(['--days', '0'], now), /days/);
  throws(() => parseArgs(['--from', '2026-04-30', '--to', '2026-04-01'], now), /期間/);
  assert.equal(datesBetween('2026-09-29', '2026-10-02').length, 4);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
