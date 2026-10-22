#!/usr/bin/env node
/**
 * test-amazon-ads-attrib.mjs — apps/warehouse/fetch-amazon-ads.js の帰属売上と広告グループの表 (fact_amazon_ads_sku_attrib。2026-10-22) の試験。API には繋がない (:memory: の SQLite)。
 *   ① 🚨 既存の fact_ad_spend と ads_fetch_days は、足した列があっても・新しい表を書いても・新しい表が失敗しても **同じ値** (Company DB への送り・v4 view・mirror が読む)
 *   ② 新しい表: 広告グループを分けて残す・同じ鍵の行は合算・売上/購入/数量の 1/7/14/30 日・無ければ null・SKU/ASIN の決め方は fact_ad_spend と同じ
 *   ③ 守り (fact_ad_spend と同じ): 取得の完全性の記録 (ads_attrib_fetch_days)・読めない行があれば期間ごと書かない・古い取得で戻さない・同じ世代の別のレポートは失敗・プロファイルは 1 つ
 *   ④ 後から伸びる 7〜30 日の値: 直近の取り直しで置き換わる
 */
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import {
  saveAdProduct, saveAdAttrib, aggregateAttribRows, REPORT_TYPE,
  BASE_REPORT_COLUMNS, ATTRIB_EXTRA_COLUMNS, REPORT_COLUMNS, ATTRIB_METRICS, isColumnRejected,
} from '../apps/warehouse/fetch-amazon-ads.js';

let ok = 0, ng = 0;
const t = (name, fn) => { try { fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message)); } };
const throws = (fn, re) => { let e = null; try { fn(); } catch (x) { e = x; } if (!e) throw new Error('did not throw'); if (re && !re.test(e.message)) throw new Error(`wrong error: ${e.message}`); };

function openDb() {
  const db = new Database(':memory:');
  // 本番 (warehouse.db) の fact_ad_spend と同じ定義 (test-amazon-ads-fetch.mjs と同じ)
  db.exec(`CREATE TABLE fact_ad_spend (日付 TEXT NOT NULL, モール TEXT NOT NULL, キャンペーンID TEXT NOT NULL, 広告タイプ TEXT NOT NULL, ターゲット TEXT NOT NULL, ターゲット粒度 TEXT NOT NULL,
    クリック数 INTEGER DEFAULT 0, インプレッション INTEGER DEFAULT 0, 広告費 REAL DEFAULT 0, 広告経由売上 REAL DEFAULT 0, 広告経由数量 INTEGER DEFAULT 0, ingested_at TEXT NOT NULL,
    PRIMARY KEY (日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度))`);
  return db;
}
/** 以前から頼んでいる列だけの行 */
const baseRow = (x) => ({ date: '2026-09-01', campaignId: 111, campaignName: 'Camp A', adGroupId: 9001, adGroupName: 'AG 1', advertisedSku: 'SKU-A', advertisedAsin: 'B000000001',
  impressions: 100, clicks: 10, cost: 123.45, sales1d: 2000, sales7d: 3000, sales14d: 3500, sales30d: 4000, purchases1d: 1, unitsSoldClicks1d: 2, ...x });
/** 足した列もある行 */
const fullRow = (x) => baseRow({ purchases7d: 2, purchases14d: 3, purchases30d: 4, unitsSoldClicks7d: 3, unitsSoldClicks14d: 4, unitsSoldClicks30d: 5, ...x });
const W = { from: '2026-09-01', to: '2026-09-03' };
const factOf = (db) => db.prepare(`select 日付, モール, キャンペーンID, 広告タイプ, ターゲット, ターゲット粒度, クリック数, インプレッション, 広告費, 広告経由売上, 広告経由数量, ingested_at from fact_ad_spend order by 1, 3, 5, 6`).all();
const factRecOf = (db) => db.prepare(`select * from ads_fetch_days order by date_jst`).all();
const attribOf = (db, d) => db.prepare(`select * from fact_amazon_ads_sku_attrib where date_jst = ? order by campaign_id, ad_group_id, target`).all(d);
const recOf = (db, d) => db.prepare(`select generation, report_id, row_count, cost_total from ads_attrib_fetch_days where report_type = ? and profile_id = 'P1' and date_jst = ?`).get(REPORT_TYPE, d);
const args = (x) => ({ ...W, generation: 1000, reportId: 'R1', profileId: 'P1', now: () => 'T1', ...x });

console.log('① 既存の表は変わらない');
t('頼む列: 以前の列は同じ順のまま全部残り、足したのは 7/14/30 日の購入と数量だけ', () => {
  assert.deepEqual([...BASE_REPORT_COLUMNS], ['date', 'campaignId', 'campaignName', 'adGroupId', 'adGroupName', 'advertisedAsin', 'advertisedSku', 'impressions', 'clicks', 'cost',
    'sales1d', 'sales7d', 'sales14d', 'sales30d', 'purchases1d', 'unitsSoldClicks1d']);
  assert.deepEqual([...REPORT_COLUMNS], [...BASE_REPORT_COLUMNS, ...ATTRIB_EXTRA_COLUMNS]);
  assert.deepEqual([...ATTRIB_EXTRA_COLUMNS].sort(), ['purchases14d', 'purchases30d', 'purchases7d', 'unitsSoldClicks14d', 'unitsSoldClicks30d', 'unitsSoldClicks7d']);
  for (const [, src] of ATTRIB_METRICS) assert.ok(REPORT_COLUMNS.includes(src), `頼んでいない列を読む: ${src}`);
});
t('🚨 足した列のある行でも fact_ad_spend と ads_fetch_days は同じ値 (以前の列だけの行と比べる)・新しい表を書いても変わらない', () => {
  const rowsBase = [baseRow({}), baseRow({ adGroupId: 9002, cost: 10, clicks: 1, impressions: 5 }), baseRow({ date: '2026-09-02', campaignId: 222, advertisedSku: '', cost: 7 })];
  const rowsFull = rowsBase.map((r) => fullRow(r));
  const a = openDb(), b = openDb();
  saveAdProduct(a, rowsBase, args());
  saveAdProduct(b, rowsFull, args());
  saveAdAttrib(b, rowsFull, args());
  assert.deepEqual(factOf(b), factOf(a));
  assert.deepEqual(factRecOf(b), factRecOf(a));
  // 広告グループは fact_ad_spend では今までどおり合算 (1 行)
  assert.deepEqual(factOf(b).filter((r) => r.日付 === '2026-09-01').map((r) => [r.ターゲット, r.クリック数, r.広告費, r.広告経由売上, r.広告経由数量]), [['sku-a', 11, 133.45, 4000, 4]]);
});
t('🚨 新しい表だけが読めない行 (例: purchases7d が数でない) でも、fact_ad_spend は書ける (main は fact を先に保存し、新しい表は別の取引で失敗するだけ)', () => {
  const db = openDb();
  const rows = [fullRow({ purchases7d: 'abc' })];
  const f = saveAdProduct(db, rows, args());
  throws(() => saveAdAttrib(db, rows, args()), /purchases7d/);
  assert.equal(f.rows, 1);
  assert.equal(factOf(db).length, 1);
  assert.equal(attribOf(db, '2026-09-01').length, 0);
  assert.equal(db.prepare('select count(*) n from ads_attrib_fetch_days').get().n, 0);
});
t('新しい表の記録は別の表 (ads_attrib_fetch_days)。ads_fetch_days には行を足さない', () => {
  const db = openDb();
  saveAdAttrib(db, [fullRow({})], args());
  assert.equal(db.prepare(`select count(*) n from sqlite_master where name = 'ads_fetch_days'`).get().n, 0);
  assert.deepEqual(recOf(db, '2026-09-01'), { generation: 1000, report_id: 'R1', row_count: 1, cost_total: 123.45 });
});
t('createReport が列を受け付けなかった (400 で column) ときだけ以前の列で頼み直す', () => {
  assert.equal(isColumnRejected(400, { code: '400', detail: 'Invalid column purchases7d' }), true);
  assert.equal(isColumnRejected(400, { detail: 'startDate is invalid' }), false);
  assert.equal(isColumnRejected(401, { detail: 'column' }), false);
});

console.log('② 新しい表の中身');
t('広告グループを分けて残す・同じ (日, キャンペーン, 広告グループ, 対象) は合算・SKU があれば SKU (小文字)・無ければ ASIN・どちらも無ければ none', () => {
  const m = aggregateAttribRows([fullRow({}), fullRow({ cost: 0.1, clicks: 1, impressions: 5 }), fullRow({ adGroupId: 9002 }), fullRow({ advertisedSku: '', advertisedAsin: 'B0ASIN' }), fullRow({ advertisedSku: null, advertisedAsin: null })], W);
  const d1 = m.get('2026-09-01').map((a) => [a.adGroupId, a.target, a.granularity, a.clicks, a.cost, a.sourceRows, a.sku, a.asin]).sort();
  assert.deepEqual(d1, [
    ['9001', '', 'none', 10, 123.45, 1, null, null], ['9001', 'b0asin', 'asin', 10, 123.45, 1, null, 'B0ASIN'],
    ['9001', 'sku-a', 'sku', 11, 123.55, 2, 'SKU-A', 'B000000001'], ['9002', 'sku-a', 'sku', 10, 123.45, 1, 'SKU-A', 'B000000001']].sort());
});
t('売上・購入・数量の 1/7/14/30 日を合算 (売上は 2 桁)・1 つでも無ければ null (0 にしない)・購入件数と数量は混ぜない', () => {
  const m = aggregateAttribRows([fullRow({ sales7d: 1000.005 }), fullRow({ sales7d: 1, purchases30d: undefined }), fullRow({ date: '2026-09-02', unitsSoldClicks14d: null })], W);
  const a = m.get('2026-09-01')[0];
  assert.deepEqual([a.sales_1d, a.sales_7d, a.sales_30d, a.purchases_1d, a.purchases_7d, a.purchases_30d, a.units_1d, a.units_7d, a.units_30d], [4000, 1001.01, 8000, 2, 4, null, 4, 6, 10]);
  const b = m.get('2026-09-02')[0];
  assert.deepEqual([b.units_14d, b.units_7d, b.purchases_14d], [null, 3, 3]);
  const baseOnly = aggregateAttribRows([baseRow({})], W).get('2026-09-01')[0];   // 以前の列だけで頼み直した回
  assert.deepEqual([baseOnly.purchases_1d, baseOnly.purchases_7d, baseOnly.units_1d, baseOnly.units_30d, baseOnly.sales_30d], [1, null, 2, null, 4000]);
});
t('同じ鍵で ASIN が食い違う → ASIN は null・名前は空でない最後の値', () => {
  const a = aggregateAttribRows([fullRow({ advertisedAsin: 'B01' }), fullRow({ advertisedAsin: 'B02', campaignName: 'New name', adGroupName: '' })], W).get('2026-09-01')[0];
  assert.deepEqual([a.asin, a.campaignName, a.adGroupName], [null, 'New name', 'AG 1']);
});
t('保存すると列に入る (鍵・名前・表示・クリック・費用・各日数・行数・世代・レポート)', () => {
  const db = openDb();
  const r = saveAdAttrib(db, [fullRow({}), fullRow({ adGroupId: 9002, cost: 5 }), fullRow({ date: '2026-09-02' })], args());
  assert.deepEqual([r.days, r.rows], [3, 3]);
  const x = attribOf(db, '2026-09-01')[0];
  assert.deepEqual([x.campaign_id, x.ad_group_id, x.target, x.target_granularity, x.campaign_name, x.ad_group_name, x.advertised_sku, x.advertised_asin, x.impressions, x.clicks, x.cost,
    x.sales_1d, x.sales_7d, x.sales_14d, x.sales_30d, x.purchases_1d, x.purchases_7d, x.purchases_14d, x.purchases_30d, x.units_1d, x.units_7d, x.units_14d, x.units_30d, x.source_rows, x.generation, x.report_id, x.ingested_at],
  ['111', '9001', 'sku-a', 'sku', 'Camp A', 'AG 1', 'SKU-A', 'B000000001', 100, 10, 123.45, 2000, 3000, 3500, 4000, 1, 2, 3, 4, 2, 3, 4, 5, 1, 1000, 'R1', 'T1']);
  assert.deepEqual(recOf(db, '2026-09-01'), { generation: 1000, report_id: 'R1', row_count: 2, cost_total: 128.45 });
  assert.deepEqual(recOf(db, '2026-09-03'), { generation: 1000, report_id: 'R1', row_count: 0, cost_total: 0 });
});

console.log('③ 守り');
t('🚨 読めない行が 1 つでもあれば何も書かない (前の取得の行と記録が残る): 期間外・キャンペーン/広告グループ無し・費用の欠落・負の購入・小数の数量・配列でない', () => {
  const db = openDb();
  saveAdAttrib(db, [fullRow({})], args());
  for (const bad of [{ date: '2026-08-31' }, { campaignId: '' }, { adGroupId: null }, { cost: undefined }, { purchases14d: -1 }, { unitsSoldClicks30d: 1.5 }]) {
    throws(() => saveAdAttrib(db, [fullRow({ cost: 1 }), fullRow({ date: '2026-09-02', ...bad })], args({ generation: 2000, reportId: 'R2' })));
  }
  throws(() => aggregateAttribRows({ rows: [] }, W), /配列/);
  assert.deepEqual([attribOf(db, '2026-09-01')[0].cost, recOf(db, '2026-09-01').report_id], [123.45, 'R1']);
});
t('🚨 後から届いた古い取得では戻さない (その日だけ飛ばす)・同じ世代の同じレポートは入れ直さない・同じ世代の別のレポートは期間ごと失敗 (途中まで書いた日も巻き戻る)・別のプロファイルは失敗', () => {
  const db = openDb();
  saveAdAttrib(db, [fullRow({ cost: 50 })], args({ from: '2026-09-01', to: '2026-09-01', generation: 2000, reportId: 'R2' }));
  const r = saveAdAttrib(db, [fullRow({ cost: 999 }), fullRow({ date: '2026-09-02', cost: 1 })], args({ generation: 1500, reportId: 'R1' }));
  assert.deepEqual([r.days, r.skippedOlder], [2, ['2026-09-01']]);
  assert.deepEqual([attribOf(db, '2026-09-01')[0].cost, attribOf(db, '2026-09-02')[0].cost], [50, 1]);
  const same = saveAdAttrib(db, [fullRow({ cost: 50 })], args({ from: '2026-09-01', to: '2026-09-01', generation: 2000, reportId: 'R2' }));
  assert.deepEqual([same.days, same.skippedSame], [0, ['2026-09-01']]);
  throws(() => saveAdAttrib(db, [fullRow({ cost: 7 })], args({ from: '2026-09-01', to: '2026-09-01', generation: 2000, reportId: 'RX' })), /同じ世代/);
  assert.deepEqual([attribOf(db, '2026-09-01')[0].cost, recOf(db, '2026-09-01').report_id], [50, 'R2']);
  // 途中の日で衝突 (1 日目は記録なし = 書ける・2 日目が同じ世代の別レポート) → 1 日目の書き込みも記録も巻き戻る
  const db2 = openDb();
  saveAdAttrib(db2, [fullRow({ date: '2026-09-02', cost: 7 })], args({ from: '2026-09-02', to: '2026-09-02', generation: 5000, reportId: 'RX' }));
  throws(() => saveAdAttrib(db2, [fullRow({ cost: 11 }), fullRow({ date: '2026-09-02', cost: 22 })], args({ from: '2026-09-01', to: '2026-09-02', generation: 5000, reportId: 'RY' })), /同じ世代/);
  assert.deepEqual([attribOf(db2, '2026-09-01').length, recOf(db2, '2026-09-01'), attribOf(db2, '2026-09-02')[0].cost, recOf(db2, '2026-09-02').report_id], [0, undefined, 7, 'RX']);
  throws(() => saveAdAttrib(db, [], args({ generation: 9000, reportId: 'RP', profileId: 'P2' })), /別の広告プロファイル/);
  throws(() => saveAdAttrib(db, [], args({ generation: 0 })), /generation/);
  throws(() => saveAdAttrib(db, [], args({ from: '2026-09-03', to: '2026-09-01' })), /期間/);
});

console.log('④ 後から伸びる 7〜30 日の値');
t('翌朝の取り直し (新しい世代) で同じ日の 7/14/30 日の値が伸びた値に置き換わる・行が消えた鍵は消える', () => {
  const db = openDb();
  saveAdAttrib(db, [fullRow({ sales30d: 4000, purchases30d: 4 }), fullRow({ adGroupId: 9002 })], args({ from: '2026-09-01', to: '2026-09-01' }));
  saveAdAttrib(db, [fullRow({ sales30d: 6500, purchases30d: 7 })], args({ from: '2026-09-01', to: '2026-09-01', generation: 2000, reportId: 'R2' }));
  const x = attribOf(db, '2026-09-01');
  assert.deepEqual(x.map((r) => [r.ad_group_id, r.sales_30d, r.purchases_30d, r.report_id]), [['9001', 6500, 7, 'R2']]);
});

console.log(`\n${ok} ok / ${ng} NG`);
process.exitCode = ng ? 1 : 0;
