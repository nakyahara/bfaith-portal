#!/usr/bin/env node
/**
 * test-amazon-sku-asin-map.mjs — SKU → ASIN の専用のマップ (lib/amazon-sku-asin-map.js) と、それを使う広告の割り振りの試験
 *
 *   1. マップの決まり (DB を読まない): 1 SKU に ASIN 2 つ (今の ASIN = 見た日が新しい方・両方とも割り振りに使う)・同じ日は手数料の写しが先・
 *      1 ASIN に SKU 2 つ・2 つの出どころで同じ対 (1 回だけ数える)・形の正規化 (大小・空白)・ASIN の形でないもの / 知らない出どころは捨てる
 *   2. 写しから読む: 手数料の写しの fetched_at (UTC) → JST の日・価格の写しは対ごとの最後の日・表が無い出どころは degraded (落ちない)・キャッシュ
 *   3. amazon-dashboard の広告の割り振り (本番と同じく 日次の財務の asin_norm = '')
 *      - ASIN の粒度の広告が SKU に結びつく (1 ASIN に 2 SKU = 売上の比・ASIN の付け替えの前後の両方・負の売上の SKU は 0・マップに無い ASIN は配れない分へ)
 *      - 二重に配らない: SKU ごとの広告費の合計 = キャンペーンの合計 (会社の合計は変わらない)
 *      - 前 (マップが空 = 直す前と同じ) と後の SKU 別の広告費・利益の表を出す (PR の本文の「数字の変わり方」)
 *      - SKU 指定の滝の広告費 = SKU の表の広告費
 *      - 表の asin の欄 = 今の ASIN (無ければ '')
 *
 * 実行: node scripts/test-amazon-sku-asin-map.mjs
 */
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const SCRATCH = await temporaryTestDataDir(import.meta.url, 'sku-asin-map-');
process.env.DATA_DIR = SCRATCH;

const M = await import('../lib/amazon-sku-asin-map.js');
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');

let pass = 0, fail = 0;
const ok = (cond, name, extra) => {
  if (cond) { pass++; console.log(`  ✅ ${name}`); } else { fail++; console.log(`  ❌ ${name}${extra === undefined ? '' : ` ${JSON.stringify(extra)}`}`); }
};
const near = (a, b, eps = 0.01) => Math.abs(a - b) <= eps;
const sorted = (set) => [...set].sort();

console.log('\n── 1. マップの決まり (DB を読まない) ──');
{
  const m = M.buildSkuAsinMap([
    // 1 SKU に ASIN 2 つ (付け替え): 価格の写しの古い日 = 旧・手数料の写しの新しい日 = 新
    { sku: 'PR_B ', asin: 'b0bold0001', seen: '2026-08-01', source: 'price_snapshot' },
    { sku: 'pr_b', asin: 'B0BNEW0001', seen: '2026-09-20', source: 'sku_fees' },
    // 同じ日なら手数料の写しが先
    { sku: 'pr_t', asin: 'B0TPRICE01', seen: '2026-09-01', source: 'price_snapshot' },
    { sku: 'pr_t', asin: 'B0TFEES001', seen: '2026-09-01', source: 'sku_fees' },
    // 1 ASIN に SKU 2 つ・2 つの出どころで同じ対
    { sku: 'pr_a_fba', asin: 'B0AAAAAAAA', seen: '2026-09-02', source: 'sku_fees' },
    { sku: 'pr_a_fba', asin: ' b0aaaaaaaa', seen: '2026-09-03', source: 'price_snapshot' },
    { sku: 'pr_a_fbm', asin: 'B0AAAAAAAA', seen: '', source: 'sku_fees' },   // 見た日が読めない = '' (いちばん古い扱い)
    // 捨てるもの
    { sku: 'pr_x', asin: 'N/A', seen: '2026-09-01', source: 'sku_fees' },
    { sku: 'pr_x', asin: '   ', seen: '2026-09-01', source: 'sku_fees' },
    { sku: '  ', asin: 'B0XXXXXXXX', seen: '2026-09-01', source: 'sku_fees' },
    { sku: 'pr_x', asin: 'B0XXXXXXXX', seen: '2026-09-01', source: 'finance' },   // 財務は出どころにしない
  ]);
  ok(M.currentAsin(m, 'pr_b') === 'B0BNEW0001', '1 SKU に ASIN 2 つ: 今の ASIN = 見た日が新しい方', M.currentAsin(m, 'pr_b'));
  ok(JSON.stringify(sorted(M.asinsOfSku(m, 'PR_B'))) === JSON.stringify(['b0bnew0001', 'b0bold0001']), '1 SKU に ASIN 2 つ: 割り振りには両方 (SKU の大小・空白は揃える)', sorted(M.asinsOfSku(m, 'pr_b')));
  ok(M.currentAsin(m, 'pr_t') === 'B0TFEES001', '同じ日なら手数料の写しが先');
  ok(JSON.stringify(sorted(m.skusByAsin.get('b0aaaaaaaa'))) === JSON.stringify(['pr_a_fba', 'pr_a_fbm']), '1 ASIN に SKU 2 つ');
  ok(M.asinsOfSku(m, 'pr_a_fba').size === 1 && m.skusByAsin.get('b0aaaaaaaa').size === 2, '2 つの出どころで同じ対 = 1 回だけ (Set)');
  ok(M.currentAsin(m, 'pr_a_fba') === 'B0AAAAAAAA' && m.asinBySku.get('pr_a_fba').source === 'price_snapshot', '今の ASIN は大文字・見た日の新しい出どころ');
  ok(M.currentAsin(m, 'pr_x') === '' && M.asinsOfSku(m, 'pr_x').size === 0, 'ASIN の形でないもの・知らない出どころ (finance) は捨てる');
  ok(M.currentAsin(m, 'nope') === '' && M.asinsOfSku(m, 'nope').size === 0, 'マップに無い SKU = \'\' と空の Set');
  ok(JSON.stringify(m.stats) === JSON.stringify({ skus: 4, asins: 5, pairs: 6, skusWithManyAsins: 2, asinsWithManySkus: 1, dropped: 4 }), '数 (SKU 4・ASIN 5・対 6・ASIN が 2 つ以上の SKU 2・SKU が 2 つ以上の ASIN 1・捨てた 4)', m.stats);
  ok(M.currentAsin(null, 'pr_b') === '' && M.asinsOfSku(undefined, 'pr_b').size === 0, 'マップが無くても落ちない');
}

console.log('\n── 2. 写しから読む ──');
initMirrorDB();
const db = getMirrorDB();
const insFees = db.prepare(`INSERT INTO mirror_amazon_sku_fees (seller_sku, asin, fulfillment_channel, fetched_at) VALUES (?, ?, ?, ?)`);
const insSnap = db.prepare(`INSERT INTO mirror_amazon_price_snapshot_daily (date_jst, seller_sku, asin, channel, source_run_id, source_row_hash, synced_at) VALUES (?, ?, ?, 'FBA', 'p', 'h', 't')`);
{
  insFees.run('pr_utc', 'B0UTC00001', 'FBA', '2026-09-30 16:00:00');   // UTC 16:00 = JST 10/1 01:00
  insSnap.run('2026-09-30', 'pr_utc', 'B0UTCSNAP1');
  insSnap.run('2026-09-01', 'PR_HIST', 'B0HIST0001');
  insSnap.run('2026-09-15', 'pr_hist', 'B0HIST0001');
  insSnap.run('2026-09-10', 'pr_hist', 'B0HIST0002');
  const m = M.loadSkuAsinMap(db, { cache: false });
  ok(m.asinBySku.get('pr_utc')?.seen === '2026-10-01' && M.currentAsin(m, 'pr_utc') === 'B0UTC00001', '手数料の写しの fetched_at (UTC) は JST の日で比べる (10/1 > 価格の写しの 9/30)', m.asinBySku.get('pr_utc'));
  ok(m.asinBySku.get('pr_hist')?.seen === '2026-09-15' && M.currentAsin(m, 'pr_hist') === 'B0HIST0001', '価格の写しは対ごとの最後の日 (SKU の大小は揃える)', m.asinBySku.get('pr_hist'));
  ok(M.asinsOfSku(m, 'pr_hist').size === 2 && m.degraded.length === 0, '価格の写しの履歴の ASIN も全部割り振りに使う');
  // キャッシュ: 10 分は同じもの・消せば読み直す
  const c1 = M.loadSkuAsinMap(db);
  insFees.run('pr_new', 'B0NEW00001', 'FBA', '2026-10-01 00:00:00');
  ok(M.loadSkuAsinMap(db) === c1 && M.currentAsin(c1, 'pr_new') === '', 'キャッシュの中は同じもの');
  M.clearSkuAsinMapCache(db);
  ok(M.currentAsin(M.loadSkuAsinMap(db), 'pr_new') === 'B0NEW00001', 'キャッシュを消すと読み直す');
  db.exec('DELETE FROM mirror_amazon_sku_fees; DELETE FROM mirror_amazon_price_snapshot_daily;');
  M.clearSkuAsinMapCache(db);
}
{
  // 表が無い出どころは飛ばす (別の接続で試す = 本物の表は消さない)
  const fake = { prepare: (sql) => ({ all: () => { if (/mirror_amazon_price_snapshot_daily/.test(sql)) throw new Error('no such table: mirror_amazon_price_snapshot_daily'); return [{ sku: 'pr_f', asin: 'B0FFFFFFFF', seen: '2026-10-01' }]; } }) };
  const origWarn = console.warn; console.warn = () => {};
  const m = M.readSkuAsinMap(fake);
  console.warn = origWarn;
  ok(JSON.stringify(m.degraded) === JSON.stringify(['price_snapshot']) && M.currentAsin(m, 'pr_f') === 'B0FFFFFFFF', '表が無い出どころは飛ばし degraded に名前 (残りの出どころは使う)', m.degraded);
}

console.log('\n── 3. amazon-dashboard の広告の割り振り (財務の asin_norm は本番と同じく空) ──');
const q = await import('../apps/amazon-dashboard/queries.js');
const FROM = '2026-09-01', TO = '2026-09-10', LAST = '2026-09-11';   // 決済の最後の日 9/11 → そろった日 = 9/10
const SKUS = [
  // [seller_sku, 1 日の本体の売上 (税抜)]
  ['pr_a_fba', 3000], ['pr_a_fbm', 1000],   // 同じ ASIN B0AAAAAAAA (FBA と自社発送)
  ['pr_b', 2000],                           // ASIN を付け替えた (旧 B0BOLD0001 = 価格の写しの 8 月・新 B0BNEW0001 = 手数料の写し)
  ['pr_c', 1500],                           // ASIN が無い (SKU の粒度の広告だけ)
  ['pr_d', -500], ['pr_e', 800],            // 同じ ASIN B0DDDDDDDD・pr_d は売上が負 (返金が多い)
];
const ADS = [
  // [target, 粒度, 1 日の広告費, 1 日の広告経由の売上]
  ['b0aaaaaaaa', 'asin', 400, 2000],
  ['b0bold0001', 'asin', 100, 0],
  ['b0bnew0001', 'asin', 200, 0],
  ['pr_c', 'sku', 300, 1500],
  ['b0zzzzzzzz', 'asin', 50, 0],          // 出品に無い ASIN (マップに無い) = 配れない分へ
  ['b0dddddddd', 'asin', 120, 0],
];
const AUTO = 330;   // オート (SKU も ASIN も無い) = 配れない分へ
const DAYS = 10;
{
  const insFin = db.prepare(`INSERT INTO mirror_amazon_finance_sku_daily (date_jst, seller_sku, asin_norm, product_name, units_ordered, units_net_sold,
    sales_principal_jpy, profit_amount, cogs_amount, is_cost_complete, cost_status, source_run_id, source_row_hash, synced_at)
    VALUES (?, ?, '', '', 1, 1, ?, ?, 0, 1, 'complete', 't', 'h', 't')`);
  const insAd = db.prepare(`INSERT INTO mirror_amazon_ads_sku_daily (date_jst, mall, campaign_id, ad_type, target, target_granularity, ad_cost, ad_sales, source_run_id, source_row_hash, synced_at)
    VALUES (?, 'amazon', ?, 'SP', ?, ?, ?, ?, 't', 'h', 't')`);
  const insCamp = db.prepare(`INSERT INTO mirror_amazon_ads_campaign_daily (date_jst, mall, campaign_id, ad_type, ad_cost, source_run_id, source_row_hash, synced_at)
    VALUES (?, 'amazon', ?, 'SP', ?, 't', 'h', 't')`);
  db.transaction(() => {
    for (let i = 1; i <= 11; i++) {
      const d = `2026-09-${String(i).padStart(2, '0')}`;
      for (const [sku, rev] of SKUS) insFin.run(d, sku, rev, rev * 0.4);
      if (d > TO) continue;
      ADS.forEach(([t, g, cost, sales], k) => { insAd.run(d, `C${k}`, t, g, cost, sales); insCamp.run(d, `C${k}`, cost); });
      insCamp.run(d, 'CAUTO', AUTO);
    }
  })();
}
const campaignTotal = DAYS * (ADS.reduce((s, a) => s + a[2], 0) + AUTO);
const run = () => {
  const r = q.getSkuProfit(FROM, TO, { limit: 100, sort: 'seller_sku', dir: 'asc' });
  return { r, by: Object.fromEntries(r.rows.map((x) => [x.seller_sku, x])) };
};

// 前 = マップが空 (直す前と同じ = 財務の asin_norm が空なので ASIN の粒度の広告は 1 つも結びつかない)
M.clearSkuAsinMapCache(db);
const before = run();
ok(before.r.settled.effective_to === TO && before.r.ad_campaign_total === campaignTotal, `前: 期間の終わり = ${TO}・キャンペーンの合計 ${campaignTotal}`, before.r.settled);
ok(before.r.rows.every((x) => x.asin === ''), '前: asin の欄は全部空');
ok(Object.values(before.by).filter((x) => x.ad_direct > 0).map((x) => x.seller_sku).join() === 'pr_c', '前: SKU に直接貼れるのは SKU の粒度の pr_c だけ');

// 後 = 手数料の写しと価格の写しに対を入れる
insFees.run('pr_a_fba', 'B0AAAAAAAA', 'AFN', '2026-09-20 00:00:00');
insFees.run('pr_a_fbm', 'B0AAAAAAAA', 'MFN', '2026-09-20 00:00:00');
insSnap.run('2026-08-01', 'pr_b', 'B0BOLD0001');
insFees.run('pr_b', 'B0BNEW0001', 'AFN', '2026-09-20 00:00:00');
insFees.run('pr_d', 'B0DDDDDDDD', 'AFN', '2026-09-20 00:00:00');
insSnap.run('2026-09-05', 'PR_E', 'B0DDDDDDDD');   // SKU の大小が違っても結びつく
insSnap.run('2026-09-06', 'pr_e', 'B0DDDDDDDD');   // 同じ対が 2 日 = 1 回だけ
M.clearSkuAsinMapCache(db);
const after = run();
const A = after.by;
ok(near(A.pr_a_fba.ad_direct, 3000) && near(A.pr_a_fbm.ad_direct, 1000), '1 ASIN に 2 SKU: ASIN の広告 4,000 を売上の比 3:1 で (3,000 / 1,000)', [A.pr_a_fba.ad_direct, A.pr_a_fbm.ad_direct]);
ok(near(A.pr_b.ad_direct, 3000), 'ASIN を付け替えた SKU: 旧 ASIN 1,000 + 新 ASIN 2,000 の両方が届く', A.pr_b.ad_direct);
ok(near(A.pr_c.ad_direct, 3000), 'SKU の粒度は前と同じ (3,000)', A.pr_c.ad_direct);
ok(near(A.pr_e.ad_direct, 1200) && near(A.pr_d.ad_direct, 0), '売上が負の SKU は 0 として数える (同じ ASIN の 1,200 は全部 pr_e)', [A.pr_d.ad_direct, A.pr_e.ad_direct]);
ok(after.r.ad_unallocated === DAYS * (50 + AUTO), `マップに無い ASIN とオートだけが配れない分 (${DAYS * (50 + AUTO)})`, after.r.ad_unallocated);
const sumAd = (x) => Object.values(x.by).reduce((s, r) => s + r.ad_direct + r.ad_allocated, 0);
ok(Math.abs(sumAd(after) - campaignTotal) <= Object.keys(after.by).length && Math.abs(sumAd(before) - campaignTotal) <= Object.keys(before.by).length,
  `二重に配らない: SKU ごとの広告費の合計 = キャンペーンの合計 (前 ${sumAd(before)} / 後 ${sumAd(after)} / ${campaignTotal}・丸めの差だけ)`);
const sumProfit = (x) => Object.values(x.by).reduce((s, r) => s + r.profit_after_ads, 0);
ok(Math.abs(sumProfit(after) - sumProfit(before)) <= 2 * SKUS.length, `会社の広告後の利益の合計は変わらない (前 ${sumProfit(before)} / 後 ${sumProfit(after)}・丸めの差だけ)`);
ok(A.pr_b.asin === 'B0BNEW0001' && A.pr_e.asin === 'B0DDDDDDDD' && A.pr_c.asin === '', '表の asin = 今の ASIN (付け替えた SKU は新しい方・無い SKU は \'\')', [A.pr_b.asin, A.pr_e.asin, A.pr_c.asin]);
for (const sku of ['pr_a_fba', 'pr_b', 'pr_d']) {
  const wf = q.getWaterfall(FROM, TO, sku);
  const ad = wf.steps.find((s) => s.key === 'ad_cost').amount;
  ok(Math.abs(ad - (A[sku].ad_direct + A[sku].ad_allocated)) <= 1, `SKU 指定の滝の広告費 = SKU の表の広告費 (${sku}: ${ad})`);
}
ok(q.getSkuProfit(FROM, TO, { q: 'b0bnew' }).rows.map((x) => x.seller_sku).join() === 'pr_b', '表の検索は ASIN でも引ける (B0BNEW → pr_b)');

console.log('\n  数字の変わり方 (10 日・本体の売上だけの単純な fixture・円):');
console.log('  | SKU | 売上 | 前: 直接 / 按分 / 広告費 | 後: 直接 / 按分 / 広告費 | 広告後の利益 前 → 後 |');
for (const [sku] of SKUS) {
  const b = before.by[sku], a = after.by[sku];
  console.log(`  | ${sku} | ${a.revenue_excl} | ${b.ad_direct} / ${b.ad_allocated} / ${b.ad_direct + b.ad_allocated} | ${a.ad_direct} / ${a.ad_allocated} / ${a.ad_direct + a.ad_allocated} | ${b.profit_after_ads} → ${a.profit_after_ads} |`);
}
console.log(`  | 合計 | | 配れない分 ${before.r.ad_unallocated} | 配れない分 ${after.r.ad_unallocated} | ${sumProfit(before)} → ${sumProfit(after)} |`);

db.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件 OK / ${fail} 件 NG`);
process.exitCode = fail ? 1 : 0;
