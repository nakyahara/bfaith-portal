#!/usr/bin/env node
/**
 * test-amazon-sku-asin-map.mjs — SKU → ASIN の専用のマップ (lib/amazon-sku-asin-map.js) と、それを使う広告の割り振りの試験
 *
 *   1. マップの決まり (DB を読まない): 1 SKU に ASIN 2 つ (今の ASIN = 見た日が新しい方・両方とも割り振りに使う)・同じ日は手数料の写しが先・
 *      1 ASIN に SKU 2 つ・2 つの出どころで同じ対 (1 回だけ数える)・形の正規化 (大小・空白)・ASIN の形 (英数字 10 文字) でないもの / 知らない出どころは捨てる・
 *      「今の ASIN」の全順序 (入力の順に依らない)
 *   2. 写しから読む: 手数料の写しの fetched_at (UTC) → JST の日・価格の写しは対ごとの最後の日・表が無い出どころは degraded (落ちない)・
 *      キャッシュ (10 分で失効・degraded のマップは 30 秒で失効)
 *   3. amazon-dashboard の広告の割り振り (本番と同じく 日次の財務の asin_norm = '')
 *      - ASIN の粒度の広告が SKU に結びつく (1 ASIN に 2 SKU = 売上の比・ASIN の付け替えの前後の両方・負の売上の SKU は 0・マップに無い ASIN は配れない分へ)
 *      - 円の整数で厳密に (Codex #1604 R1 High): SKU の広告費の合計 = キャンペーンの合計・広告後の利益の合計は前後で同じ・
 *        各 SKU で 広告前 − 直接 − 按分 = 広告後・SKU 指定の滝 / 広告タブ / 売れ筋 = SKU の表
 *      - 前 (マップが空 = 直す前と同じ) と後の SKU 別の広告費・利益の表を出す (PR の本文の「数字の変わり方」)
 *   4. 割り振りの端の形: 同じ ASIN の群が全部 売上 0 か負 (等分)・広告経由も確定も 0 で配れない額が残る・ASIN が時期をまたいで別の SKU に移る・
 *      1/3 の端数・apportionYen (最大剰余法) の性質
 *   5. supplier-sales: 公開の画面に ASIN が出る (CSV には ASIN の列は無い = 前から)・ふだんは degradedLookups = []
 *   6. degraded が利用側に伝わる: 価格の写しが読めない → dashboard・supplier-sales の degradedLookups・margin-alert の通知の 1 行
 *
 * 実行: node scripts/test-amazon-sku-asin-map.mjs
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import ejs from 'ejs';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const SCRATCH = await temporaryTestDataDir(import.meta.url, 'sku-asin-map-');
process.env.DATA_DIR = SCRATCH;
const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const M = await import('../lib/amazon-sku-asin-map.js');
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');

let pass = 0, fail = 0;
const ok = (cond, name, extra) => {
  if (cond) { pass++; console.log(`  ✅ ${name}`); } else { fail++; console.log(`  ❌ ${name}${extra === undefined ? '' : ` ${JSON.stringify(extra)}`}`); }
};
const sorted = (set) => [...set].sort();
const quiet = (fn) => { const w = console.warn; console.warn = () => {}; try { return fn(); } finally { console.warn = w; } };

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
{
  // ASIN の形 = 英数字 10 文字 (Codex #1604 R1 Low: NULL・NONE・短い / 長い壊れた値を有効にしない)
  const bad = ['NULL', 'NONE', 'B0SHORT', 'B0ALPHA', 'B0TOOLONG001', 'B0AAAAAAA-', 'B0AAAAAAA ', 'Ｂ0AAAAAAAA'];
  const m = M.buildSkuAsinMap([...bad.map((asin, i) => ({ sku: `pr_bad${i}`, asin, seen: '2026-09-01', source: 'sku_fees' })),
    { sku: 'pr_good', asin: ' b0good0001 ', seen: '2026-09-01', source: 'sku_fees' }]);
  ok(m.stats.dropped === bad.length && bad.every((_, i) => M.currentAsin(m, `pr_bad${i}`) === ''), 'ASIN の形: 10 文字でないもの (NULL・NONE・B0ALPHA・12 文字・trim 後 9 文字)・記号・全角は捨てる', m.stats);
  ok(M.currentAsin(m, 'pr_good') === 'B0GOOD0001' && M.ASIN_RE.test('B0GOOD0001') && !M.ASIN_RE.test('B0GOOD000'), 'ASIN の形: 英数字 10 文字だけが残る (前後の空白は trim・小文字は大文字に)', [...m.asinBySku.keys()]);
}
{
  // 「今の ASIN」の全順序: 見た日 → 出どころ → ASIN → SKU。入力の順をどう並べ替えても同じ (Codex #1604 R1 Medium)
  const obs = [
    { sku: 'pr_s', asin: 'B0SSSSSS03', seen: '2026-09-01', source: 'price_snapshot' },
    { sku: 'pr_s', asin: 'B0SSSSSS02', seen: '2026-09-01', source: 'price_snapshot' },
    { sku: 'pr_s', asin: 'B0SSSSSS09', seen: '2026-09-01', source: 'sku_fees' },
    { sku: 'pr_s', asin: 'B0SSSSSS01', seen: '2026-08-31', source: 'sku_fees' },
  ];
  const perms = (a) => (a.length <= 1 ? [a] : a.flatMap((x, i) => perms([...a.slice(0, i), ...a.slice(i + 1)]).map((p) => [x, ...p])));
  const got = new Set(perms(obs).map((p) => M.currentAsin(M.buildSkuAsinMap(p), 'pr_s')));
  ok(got.size === 1 && got.has('B0SSSSSS09'), '同じ SKU: 24 通りの入力の順のどれでも 同じ日 → 手数料の写し → の 1 つ (B0SSSSSS09)', [...got]);
  const h = (asin, seen, source, sku) => ({ asin, seen, source, sku });
  const list = [h('B0CCCCCCCC', '2026-09-01', 'price_snapshot', 'pr_1'), h('B0BBBBBBBB', '2026-09-01', 'sku_fees', 'pr_3'),
    h('B0BBBBBBBB', '2026-09-01', 'sku_fees', 'pr_2'), h('B0ZZZZZZZZ', '2026-09-02', 'price_snapshot', 'pr_4'), h('B0AAAAAAAA', '2026-09-01', 'sku_fees', 'pr_5')];
  const order = [...list].sort(M.compareAsinHits).map((x) => `${x.asin}/${x.sku}`);
  ok(JSON.stringify(order) === JSON.stringify(['B0ZZZZZZZZ/pr_4', 'B0AAAAAAAA/pr_5', 'B0BBBBBBBB/pr_2', 'B0BBBBBBBB/pr_3', 'B0CCCCCCCC/pr_1']),
    'compareAsinHits: 見た日が新しい → 手数料の写し → ASIN の順 → SKU の順', order);
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
  // キャッシュ: 10 分は同じもの・10 分で読み直す・消せば読み直す
  M.clearSkuAsinMapCache(db);
  const T0 = 1_800_000_000_000;
  const c1 = M.loadSkuAsinMap(db, { now: T0 });
  insFees.run('pr_new', 'B0NEW00001', 'FBA', '2026-10-01 00:00:00');
  ok(M.loadSkuAsinMap(db, { now: T0 + M.CACHE_TTL_MS - 1 }) === c1 && M.currentAsin(c1, 'pr_new') === '', 'キャッシュ: 10 分の 1 ms 前までは同じもの (足した対はまだ見えない)');
  const c2 = M.loadSkuAsinMap(db, { now: T0 + M.CACHE_TTL_MS });
  ok(c2 !== c1 && M.currentAsin(c2, 'pr_new') === 'B0NEW00001', 'キャッシュ: 10 分で失効して読み直す');
  M.clearSkuAsinMapCache(db);
  ok(M.loadSkuAsinMap(db) !== c2, 'キャッシュを消すと読み直す');
  db.exec('DELETE FROM mirror_amazon_sku_fees; DELETE FROM mirror_amazon_price_snapshot_daily;');
  M.clearSkuAsinMapCache(db);
}
{
  // 表が無い出どころは飛ばす (別の接続で試す = 本物の表は消さない)
  let broken = true;
  const fake = { prepare: (sql) => ({ all: () => { if (broken && /mirror_amazon_price_snapshot_daily/.test(sql)) throw new Error('no such table: mirror_amazon_price_snapshot_daily'); return [{ sku: 'pr_f', asin: 'B0FFFFFFFF', seen: '2026-10-01' }]; } }) };
  const m = quiet(() => M.readSkuAsinMap(fake));
  ok(JSON.stringify(m.degraded) === JSON.stringify(['price_snapshot']) && M.currentAsin(m, 'pr_f') === 'B0FFFFFFFF', '表が無い出どころは飛ばし degraded に名前 (残りの出どころは使う)', m.degraded);
  ok(JSON.stringify(M.asinMapDiagnostics(m).degradedLookups) === JSON.stringify(['asinPrice']) && M.asinMapDiagnostics(m).asinMap.skus === 1,
    '応答の診断: degradedLookups = site-products と同じ名前 (asinPrice)・asinMap = マップの数', M.asinMapDiagnostics(m));
  ok(JSON.stringify(M.asinMapDiagnostics(null)) === JSON.stringify({ degradedLookups: [], asinMap: null }), 'マップを読んでいないときの診断 = [] / null');
  // degraded のマップは 30 秒しか持たない (Codex #1604 R1 Medium: 表が戻っても 10 分は配れない分に回ったまま、にしない)
  const T0 = 1_900_000_000_000;
  const d1 = quiet(() => M.loadSkuAsinMap(fake, { now: T0 }));
  broken = false;   // 表が戻った
  ok(M.loadSkuAsinMap(fake, { now: T0 + M.DEGRADED_CACHE_TTL_MS - 1 }) === d1, 'degraded のマップ: 30 秒の 1 ms 前までは同じもの');
  const d2 = M.loadSkuAsinMap(fake, { now: T0 + M.DEGRADED_CACHE_TTL_MS });
  ok(d2 !== d1 && d2.degraded.length === 0, 'degraded のマップ: 30 秒で失効して読み直す (表が戻れば degraded でなくなる)', d2.degraded);
  ok(M.DEGRADED_CACHE_TTL_MS < M.CACHE_TTL_MS && M.loadSkuAsinMap(fake, { now: T0 + M.DEGRADED_CACHE_TTL_MS + M.DEGRADED_CACHE_TTL_MS }) === d2,
    '読み直した (degraded でない) マップはふつうの 10 分を持つ');
}

console.log('\n── 3. amazon-dashboard の広告の割り振り (財務の asin_norm は本番と同じく空) ──');
const q = await import('../apps/amazon-dashboard/queries.js');
const FROM = '2026-09-01', TO = '2026-09-10';   // 決済の最後の日 9/11 → そろった日 = 9/10
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
const campaignTotal = DAYS * (ADS.reduce((s, a) => s + a[2], 0) + AUTO);
const runSkuProfit = (from = FROM, to = TO) => {
  const r = q.getSkuProfit(from, to, { limit: 1000, sort: 'seller_sku', dir: 'asc' });
  return { r, by: Object.fromEntries(r.rows.map((x) => [x.seller_sku, x])) };
};
const sumAd = (x) => x.r.rows.reduce((s, r) => s + r.ad_direct + r.ad_allocated, 0);
const sumProfit = (x) => x.r.rows.reduce((s, r) => s + r.profit_after_ads, 0);
/** 円の整数の約束 (Codex #1604 R1 High) を厳密に確かめる。fully = キャンペーンの合計を全部配れたはずの期間 */
function checkExact(label, x, { fully = true } = {}) {
  const { r } = x;
  ok(r.rows.every((row) => [row.ad_direct, row.ad_allocated, row.profit_before_ads, row.profit_after_ads, row.profit_after_easy_ship].every(Number.isInteger)),
    `${label}: 円の列は全部整数`);
  ok(r.rows.every((row) => row.profit_before_ads - row.ad_direct - row.ad_allocated === row.profit_after_ads && row.profit_after_ads - row.easy_ship === row.profit_after_easy_ship),
    `${label}: 各 SKU で 広告前 − 直接 − 按分 = 広告後 (と 広告後 − Easy Ship = Easy Ship 後) が厳密`, r.rows.filter((row) => row.profit_before_ads - row.ad_direct - row.ad_allocated !== row.profit_after_ads));
  if (fully) ok(sumAd(x) === r.ad_campaign_total, `${label}: SKU の広告費の合計 = キャンペーンの合計 (${sumAd(x)} = ${r.ad_campaign_total}) が厳密`);
  else ok(sumAd(x) + r.ad_unallocated === r.ad_campaign_total, `${label}: 配れない額が残る期間 = SKU の広告費の合計 + 配れない分 = キャンペーンの合計 (${sumAd(x)} + ${r.ad_unallocated} = ${r.ad_campaign_total})`);
}

// 前 = マップが空 (直す前と同じ = 財務の asin_norm が空なので ASIN の粒度の広告は 1 つも結びつかない)
M.clearSkuAsinMapCache(db);
const before = runSkuProfit();
ok(before.r.settled.effective_to === TO && before.r.ad_campaign_total === campaignTotal, `前: 期間の終わり = ${TO}・キャンペーンの合計 ${campaignTotal}`, before.r.settled);
ok(before.r.rows.every((x) => x.asin === ''), '前: asin の欄は全部空');
ok(Object.values(before.by).filter((x) => x.ad_direct > 0).map((x) => x.seller_sku).join() === 'pr_c', '前: SKU に直接貼れるのは SKU の粒度の pr_c だけ');
checkExact('前', before);

// 後 = 手数料の写しと価格の写しに対を入れる
insFees.run('pr_a_fba', 'B0AAAAAAAA', 'AFN', '2026-09-20 00:00:00');
insFees.run('pr_a_fbm', 'B0AAAAAAAA', 'MFN', '2026-09-20 00:00:00');
insSnap.run('2026-08-01', 'pr_b', 'B0BOLD0001');
insFees.run('pr_b', 'B0BNEW0001', 'AFN', '2026-09-20 00:00:00');
insFees.run('pr_d', 'B0DDDDDDDD', 'AFN', '2026-09-20 00:00:00');
insSnap.run('2026-09-05', 'PR_E', 'B0DDDDDDDD');   // SKU の大小が違っても結びつく
insSnap.run('2026-09-06', 'pr_e', 'B0DDDDDDDD');   // 同じ対が 2 日 = 1 回だけ
M.clearSkuAsinMapCache(db);
const after = runSkuProfit();
const A = after.by;
ok(A.pr_a_fba.ad_direct === 3000 && A.pr_a_fbm.ad_direct === 1000, '1 ASIN に 2 SKU: ASIN の広告 4,000 を売上の比 3:1 で (3,000 / 1,000)', [A.pr_a_fba.ad_direct, A.pr_a_fbm.ad_direct]);
ok(A.pr_b.ad_direct === 3000, 'ASIN を付け替えた SKU: 旧 ASIN 1,000 + 新 ASIN 2,000 の両方が届く', A.pr_b.ad_direct);
ok(A.pr_c.ad_direct === 3000, 'SKU の粒度は前と同じ (3,000)', A.pr_c.ad_direct);
ok(A.pr_e.ad_direct === 1200 && A.pr_d.ad_direct === 0, '売上が負の SKU は 0 として数える (同じ ASIN の 1,200 は全部 pr_e)', [A.pr_d.ad_direct, A.pr_e.ad_direct]);
ok(after.r.ad_unallocated === DAYS * (50 + AUTO), `マップに無い ASIN とオートだけが配れない分 (${DAYS * (50 + AUTO)})`, after.r.ad_unallocated);
// 按分 3,800 = 広告経由の売上 pr_a_fba 15,000 : pr_a_fbm 5,000 : pr_c 15,000 → 1,628.57… / 542.86… / 1,628.57… → 端数を寄せて 1,629 / 543 / 1,628
//   (同じ端数の pr_a_fba と pr_c は SKU の文字の順で pr_a_fba が先に 1 円をもらう = 決定的)
ok(A.pr_a_fba.ad_allocated === 1629 && A.pr_a_fbm.ad_allocated === 543 && A.pr_c.ad_allocated === 1628,
  '按分の端数は最大剰余法で寄せる (1,629 / 543 / 1,628・同じ端数は SKU の順)', [A.pr_a_fba.ad_allocated, A.pr_a_fbm.ad_allocated, A.pr_c.ad_allocated]);
checkExact('後', after);
ok(sumProfit(after) === sumProfit(before), `会社の広告後の利益の合計は前後で 1 円も変わらない (前 ${sumProfit(before)} / 後 ${sumProfit(after)})`);
ok(A.pr_b.asin === 'B0BNEW0001' && A.pr_e.asin === 'B0DDDDDDDD' && A.pr_c.asin === '', '表の asin = 今の ASIN (付け替えた SKU は新しい方・無い SKU は \'\')', [A.pr_b.asin, A.pr_e.asin, A.pr_c.asin]);
ok(JSON.stringify(after.r.degradedLookups) === '[]' && after.r.asinMap?.asinsWithManySkus === 2 && after.r.asinMap?.skusWithManyAsins === 1,
  'SKU の表の診断: degradedLookups = []・asinMap (1 ASIN に複数 SKU = 2・ASIN を付け替えた SKU = 1)', { d: after.r.degradedLookups, m: after.r.asinMap });
// 滝 (SKU 指定) = SKU の表
for (const sku of SKUS.map((s) => s[0])) {
  const wf = q.getWaterfall(FROM, TO, sku);
  const step = (k) => wf.steps.find((s) => s.key === k).amount;
  const row = A[sku];
  ok(step('ad_cost') === row.ad_direct + row.ad_allocated && step('profit_before_ads') === row.profit_before_ads && step('profit_after_ads') === row.profit_after_ads
    && wf.incl.profit_after_ads === row.profit_after_ads_incl && Array.isArray(wf.degradedLookups),
  `SKU 指定の滝 = SKU の表 (${sku}: 広告費 ${step('ad_cost')}・広告前 ${step('profit_before_ads')}・広告後 ${step('profit_after_ads')}・税込の計算も)`,
  { wf: [step('ad_cost'), step('profit_before_ads'), step('profit_after_ads'), wf.incl.profit_after_ads], row: [row.ad_direct + row.ad_allocated, row.profit_before_ads, row.profit_after_ads, row.profit_after_ads_incl] });
}
{
  const wf = q.getWaterfall(FROM, TO, null);
  const step = (k) => wf.steps.find((s) => s.key === k).amount;
  ok(step('ad_cost') === sumAd(after) && step('profit_after_ads') === sumProfit(after), `会社の滝 = SKU の表の合計 (広告費 ${step('ad_cost')}・広告後 ${step('profit_after_ads')})`,
    [step('ad_cost'), sumAd(after), step('profit_after_ads'), sumProfit(after)]);
}
// 広告タブ・売れ筋 = SKU の表 (同じ整数)
{
  const ads = q.getAdsAnalysis(FROM, TO);
  const mism = ads.skus.filter((s) => s.ad_cost !== A[s.seller_sku].ad_direct + A[s.seller_sku].ad_allocated || s.ad_direct !== A[s.seller_sku].ad_direct || s.profit_after_ads !== A[s.seller_sku].profit_after_ads);
  ok(ads.skus.length >= 5 && mism.length === 0, `広告タブの SKU の広告費・直接・広告後の利益 = SKU の表 (${ads.skus.length} SKU)`, mism);
  ok(ads.totals.campaign_total === ads.totals.sku_direct_total + ads.totals.unallocated && ads.totals.sku_direct_total === Object.values(A).reduce((s, r) => s + r.ad_direct, 0),
    `広告タブの合計: 直接 ${ads.totals.sku_direct_total} + 配れない分 ${ads.totals.unallocated} = キャンペーンの合計 ${ads.totals.campaign_total}・直接 = SKU の表の直接の合計`, ads.totals);
  ok(JSON.stringify(ads.degradedLookups) === '[]' && ads.asinMap?.pairs > 0, '広告タブの診断: degradedLookups = []・asinMap あり');
  const best = q.getBestsellers(FROM, TO, 'sales');
  const bm = best.ranking.filter((r) => r.profit_after_ads !== A[r.seller_sku].profit_after_ads);
  ok(best.ranking.length === SKUS.length && bm.length === 0, '売れ筋の広告後の利益 = SKU の表', bm);
  ok(JSON.stringify(best.degradedLookups) === '[]', '売れ筋の診断: degradedLookups = []');
  const diag = q.getDiagnosis();
  ok(Array.isArray(diag.degradedLookups) && diag.degradedLookups.length === 0 && 'asinMap' in diag, '診断タブの診断: degradedLookups = []');
}
ok(q.getSkuProfit(FROM, TO, { q: 'b0bnew' }).rows.map((x) => x.seller_sku).join() === 'pr_b', '表の検索は ASIN でも引ける (B0BNEW → pr_b)');

console.log('\n  数字の変わり方 (10 日・本体の売上だけの単純な fixture・円):');
console.log('  | SKU | 売上 | 前: 直接 / 按分 / 広告費 | 後: 直接 / 按分 / 広告費 | 広告後の利益 前 → 後 |');
for (const [sku] of SKUS) {
  const b = before.by[sku], a = after.by[sku];
  console.log(`  | ${sku} | ${a.revenue_excl} | ${b.ad_direct} / ${b.ad_allocated} / ${b.ad_direct + b.ad_allocated} | ${a.ad_direct} / ${a.ad_allocated} / ${a.ad_direct + a.ad_allocated} | ${b.profit_after_ads} → ${a.profit_after_ads} |`);
}
console.log(`  | 合計 | | 配れない分 ${before.r.ad_unallocated}・広告費 ${sumAd(before)} | 配れない分 ${after.r.ad_unallocated}・広告費 ${sumAd(after)} | ${sumProfit(before)} → ${sumProfit(after)} |`);

console.log('\n── 4. 割り振りの端の形 (9 月より前の別の期間・別の SKU) ──');
{
  // (a) 同じ ASIN の群の SKU が全部 売上 0 か負 → 等分 (8/1〜8/2)
  db.transaction(() => {
    for (const d of ['2026-08-01', '2026-08-02']) {
      insFin.run(d, 'pr_z1', 0, 0);
      insFin.run(d, 'pr_z2', -300, -120);
      insFin.run(d, 'pr_z3', 1000, 400);
    }
    insAd.run('2026-08-01', 'Z1', 'b0zerogrp1', 'asin', 1001, 0);   // 1,001 を 2 等分 = 500.5 ずつ
    insCamp.run('2026-08-01', 'Z1', 1001);
    insAd.run('2026-08-01', 'Z2', 'pr_z3', 'sku', 300, 600);
    insCamp.run('2026-08-01', 'Z2', 300);
    insCamp.run('2026-08-01', 'ZAUTO', 200);
  })();
  insFees.run('pr_z1', 'B0ZEROGRP1', 'AFN', '2026-09-20 00:00:00');
  insFees.run('pr_z2', 'B0ZEROGRP1', 'MFN', '2026-09-20 00:00:00');
  M.clearSkuAsinMapCache(db);
  const x = runSkuProfit('2026-08-01', '2026-08-02');
  const Z = x.by;
  ok(Z.pr_z1.ad_direct === 501 && Z.pr_z2.ad_direct === 500, '群の SKU が全部 売上 0 か負 → 等分 (500.5 / 500.5 → 端数は SKU の順で 501 / 500)', [Z.pr_z1.ad_direct, Z.pr_z2.ad_direct]);
  ok(Z.pr_z3.ad_direct === 300 && Z.pr_z3.ad_allocated === 200 && Z.pr_z1.ad_allocated === 0, '配れない分 200 は広告経由の売上のある pr_z3 へ', [Z.pr_z3.ad_direct, Z.pr_z3.ad_allocated]);
  checkExact('全部 0 か負の群', x);
}
{
  // (b) 広告経由の売上も確定の売上も 0 で、配れない額が残る (7/1)
  db.transaction(() => {
    insFin.run('2026-07-01', 'pr_n1', 0, 0);
    insAd.run('2026-07-01', 'N1', 'b0nobasis1', 'asin', 100, 0);
    insCamp.run('2026-07-01', 'N1', 100);
    insCamp.run('2026-07-01', 'NAUTO', 500);
  })();
  insFees.run('pr_n1', 'B0NOBASIS1', 'AFN', '2026-09-20 00:00:00');
  M.clearSkuAsinMapCache(db);
  const x = runSkuProfit('2026-07-01', '2026-07-01');
  ok(x.by.pr_n1.ad_direct === 100 && x.by.pr_n1.ad_allocated === 0 && x.r.ad_unallocated === 500 && x.r.ad_campaign_total === 600,
    '按分の基準が全部 0 → 配れない分 500 は SKU に配らない (ASIN の 100 だけ貼る)', [x.by.pr_n1, x.r.ad_unallocated]);
  checkExact('配れない額が残る', x, { fully: false });
}
{
  // (c) ASIN が時期をまたいで別の SKU に移る (6 月): B0MOVED001 = 6/1 の価格の写しでは pr_m_old・今の手数料の写しでは pr_m_new
  db.transaction(() => {
    for (const d of ['2026-06-01', '2026-06-02']) insFin.run(d, 'pr_m_old', 1000, 400);
    for (const d of ['2026-06-10', '2026-06-11']) { insFin.run(d, 'pr_m_old', 1000, 400); insFin.run(d, 'pr_m_new', 2000, 800); }
    insAd.run('2026-06-01', 'MV', 'b0moved001', 'asin', 900, 0); insCamp.run('2026-06-01', 'MV', 900);
    insAd.run('2026-06-10', 'MV', 'b0moved001', 'asin', 900, 0); insCamp.run('2026-06-10', 'MV', 900);
  })();
  insSnap.run('2026-06-01', 'pr_m_old', 'B0MOVED001');
  insFees.run('pr_m_new', 'B0MOVED001', 'AFN', '2026-09-20 00:00:00');
  M.clearSkuAsinMapCache(db);
  const p1 = runSkuProfit('2026-06-01', '2026-06-02');
  ok(p1.by.pr_m_old.ad_direct === 900 && !p1.by.pr_m_new, 'ASIN の移動: 前の SKU だけに財務の行がある期間 → 全部 前の SKU', p1.by.pr_m_old);
  const p2 = runSkuProfit('2026-06-10', '2026-06-11');
  // 期間で絞らない (迷った所 3) = 両方に財務の行がある期間は売上の比で両方に配る (前の SKU に今の広告が混ざり得る = 分かっている限界)。合計は二重にならない
  ok(p2.by.pr_m_old.ad_direct === 300 && p2.by.pr_m_new.ad_direct === 600, 'ASIN の移動: 両方に財務の行がある期間は売上の比 (300 / 600・期間で絞らない限界を固定)', [p2.by.pr_m_old.ad_direct, p2.by.pr_m_new.ad_direct]);
  checkExact('ASIN の移動 (前)', p1);
  checkExact('ASIN の移動 (後)', p2);
  ok(p2.r.asinMap.asinsWithManySkus === after.r.asinMap.asinsWithManySkus + 2, 'ASIN の移動・全部 0 の群は「1 ASIN に複数 SKU」の数に入る (診断の asinMap)', p2.r.asinMap);
}
{
  // (d) 1/3 の端数 (5/1): オート 1,000 を広告経由の売上 1:1:1 で 3 等分 → 別々に丸めると 333 × 3 = 999 (master の前から)
  db.transaction(() => {
    for (const d of ['2026-05-01']) {
      ['pr_r1', 'pr_r2', 'pr_r3'].forEach((sku, k) => { insFin.run(d, sku, 1000, 400); insAd.run(d, `R${k}`, sku, 'sku', 100, 500); insCamp.run(d, `R${k}`, 100); });
      insCamp.run(d, 'RAUTO', 1000);
    }
  })();
  const x = runSkuProfit('2026-05-01', '2026-05-01');
  ok(['pr_r1', 'pr_r2', 'pr_r3'].map((s) => x.by[s].ad_allocated).join() === '334,333,333', '1/3 の端数: 334 / 333 / 333 (別々に丸めた 333 × 3 = 999 にならない)', ['pr_r1', 'pr_r2', 'pr_r3'].map((s) => x.by[s].ad_allocated));
  checkExact('1/3 の端数', x);
}
{
  // (e) apportionYen の性質 (乱数・決定的な種)
  let seed = 20261004;
  const rnd = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
  let bad = 0, det = 0;
  for (let t = 0; t < 300; t++) {
    const n = 1 + Math.floor(rnd() * 8);
    const items = Array.from({ length: n }, (_, i) => ({ key: `k${i}`, value: Math.round(rnd() * 5_000_000) / 1000 }));
    const target = Math.round(items.reduce((s, x) => s + x.value, 0));
    const out = q.apportionYen(items, target);
    const sum = [...out.values()].reduce((s, v) => s + v, 0);
    if (sum !== target || items.some((x) => !Number.isInteger(out.get(x.key)) || Math.abs(out.get(x.key) - x.value) >= 1)) bad++;
    const rev = q.apportionYen([...items].reverse(), target);
    if (items.some((x) => rev.get(x.key) !== out.get(x.key))) det++;
  }
  ok(bad === 0, 'apportionYen: 300 回とも 合計 = 目標・各額は元との差が 1 円未満の整数', bad);
  ok(det === 0, 'apportionYen: 入力の順を逆にしても同じ (決定的)', det);
  const down = q.apportionYen([{ key: 'b', value: 0.9 }, { key: 'a', value: 0.9 }, { key: 'c', value: 0.2 }], 1);
  ok(down.get('a') === 1 && down.get('b') === 0 && down.get('c') === 0, 'apportionYen: 端数が同じなら key の順 (a が先)', Object.fromEntries(down));
  const less = q.apportionYen([{ key: 'a', value: 1.9 }, { key: 'b', value: 1.1 }], 1);
  ok(less.get('a') + less.get('b') === 1 && less.get('b') === 0, 'apportionYen: 目標が切り捨ての合計より小さいときは端数の小さい方から引く', Object.fromEntries(less));
  ok(q.apportionYen([], 0).size === 0, 'apportionYen: 空でも落ちない');
}

console.log('\n── 5. supplier-sales (公開の画面に ASIN が出る) ──');
const { getSupplierReport, getSupplierDailyDetail, MALL_LABELS, SOKUHO_MALL_COLUMNS } = await import('../apps/supplier-sales/aggregate.js');
const { buildCsv, buildDailyCsv } = await import('../apps/supplier-sales/csv.js');
const { collectMarginRows, formatMarginAlertMessage } = await import('../apps/profit-analysis/margin-alert-job.js');
const SUP = 'SUPX';
const SUP_OPTS = { period: 'custom', start: '2026-09-01', end: '2026-09-05' };
{
  db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 取扱区分, 標準売価, 原価, 原価状態, 送料, 消費税率, 在庫数, 引当数, 仕入先コード, 売上分類, updated_at)
    VALUES ('ne-sup-a', '仕入先の商品', '単品', '取扱中', 1000, 300, 'COMPLETE', 100, 0.1, 10, 1, ?, 1, 't')`).run(SUP);
  for (const sku of ['pr_a_fba', 'pr_c']) db.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, synced_at) VALUES (?, 'ne-sup-a', 1, 'master', 't')`).run(sku);
  M.clearSkuAsinMapCache(db);
  const rep = getSupplierReport(db, SUP, SUP_OPTS);
  const p = rep.products.find((x) => x.ne_code === 'ne-sup-a');
  const L = (id) => p?.listings.find((l) => l.mall === 'amazon' && l.listingId === id);
  ok(L('pr_a_fba')?.asin === 'B0AAAAAAAA' && L('pr_c')?.asin === '', '仕入先レポートの出品の ASIN = マップの今の ASIN (無い SKU は \'\')', p?.listings);
  ok(JSON.stringify(rep.degradedLookups) === '[]' && rep.asinMap?.pairs > 0, '仕入先レポートの診断: degradedLookups = []・asinMap あり', { d: rep.degradedLookups, m: rep.asinMap });
  const html = await ejs.renderFile(path.join(REPO, 'views/supplier-sales-public.ejs'), {
    title: 't', supplierName: 'テスト仕入先', token: 'x'.repeat(43), mallLabels: MALL_LABELS, sokuhoMallDefs: SOKUHO_MALL_COLUMNS,
    period: rep.period, sokuho: rep.sokuho, products: rep.products, totals: rep.totals,
    query: { period: 'custom', start: SUP_OPTS.start, end: SUP_OPTS.end, tab: 'kakutei' },
  });
  ok(html.includes('pr_a_fba / B0AAAAAAAA') && !html.includes('pr_c / '), '公開の画面: 「SKU / ASIN」に ASIN が出る (ASIN の無い SKU は SKU だけ)');
  ok(!/degradedLookups|asinPrice|asinFees/.test(html), '公開の画面: 診断 (degradedLookups) は出さない');
  const csv = buildCsv(rep, MALL_LABELS);
  const det = getSupplierDailyDetail(db, SUP, SUP_OPTS);
  const dcsv = buildDailyCsv(det, MALL_LABELS);
  ok(csv.includes('pr_a_fba') && !csv.includes('B0AAAAAAAA') && !dcsv.includes('B0AAAAAAAA') && det.rows.some((r) => r.asin === 'B0AAAAAAAA'),
    '公開の CSV (サマリ・日次) には ASIN の列が無い (前からの列のまま・日次の行の値には ASIN が入る)');
}

console.log('\n── 6. degraded が利用側に伝わる (価格の写しが読めない) ──');
{
  db.exec('DROP TABLE mirror_amazon_price_snapshot_daily');
  M.clearSkuAsinMapCache(db);
  const x = quiet(() => runSkuProfit());
  ok(JSON.stringify(x.r.degradedLookups) === JSON.stringify(['asinPrice']), 'dashboard の SKU の表: degradedLookups = [asinPrice]', x.r.degradedLookups);
  ok(x.by.pr_b.ad_direct === 2000 && x.r.ad_unallocated === after.r.ad_unallocated + 1000, '読めない間は価格の写しだけにある旧 ASIN の広告 1,000 が配れない分に回る (= degradedLookups で分かる)', [x.by.pr_b.ad_direct, x.r.ad_unallocated]);
  checkExact('degraded', x);
  ok(JSON.stringify(q.getAdsAnalysis(FROM, TO).degradedLookups) === '["asinPrice"]' && JSON.stringify(q.getBestsellers(FROM, TO, 'sales').degradedLookups) === '["asinPrice"]'
    && JSON.stringify(q.getWaterfall(FROM, TO, 'pr_b').degradedLookups) === '["asinPrice"]',
  'dashboard の広告タブ・売れ筋・SKU 指定の滝も degradedLookups = [asinPrice]');
  const rep = getSupplierReport(db, SUP, SUP_OPTS);
  const det = getSupplierDailyDetail(db, SUP, SUP_OPTS);
  ok(JSON.stringify(rep.degradedLookups) === '["asinPrice"]' && JSON.stringify(det.degradedLookups) === '["asinPrice"]', 'supplier-sales のレポート・日次も degradedLookups = [asinPrice]', [rep.degradedLookups, det.degradedLookups]);
  const mr = quiet(() => collectMarginRows(db, FROM, TO));
  ok(JSON.stringify(mr.amazonDegradedLookups) === '["asinPrice"]', 'margin-alert: amazonDegradedLookups = [asinPrice]', mr.amazonDegradedLookups);
  const text = formatMarginAlertMessage({ todayJst: '2026-10-04', from: FROM, to: TO, thresholdPct: 10, result: { flagged: [], newItems: [], contItems: [], lossCount: 0, mallCounts: {}, excludedCount: 0 },
    isFirstRun: false, skipped: mr.skipped.filter((m) => m === 'amazon'), skipReasons: {}, amazonWindow: mr.amazonWindow, amazonDegradedLookups: mr.amazonDegradedLookups });
  ok(text.includes('⚠️ Amazon の SKU と ASIN の対応の一部が読めない (asinPrice)'), 'margin-alert の通知に 1 行出る', text.split('\n').slice(0, 5));
  const textOk = formatMarginAlertMessage({ todayJst: '2026-10-04', from: FROM, to: TO, thresholdPct: 10, result: { flagged: [], newItems: [], contItems: [], lossCount: 0, mallCounts: {}, excludedCount: 0 },
    isFirstRun: false, skipped: [], skipReasons: {}, amazonWindow: mr.amazonWindow });
  ok(!textOk.includes('ASIN の対応'), 'margin-alert: 読めているときは出ない');
}

db.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件 OK / ${fail} 件 NG`);
process.exitCode = fail ? 1 : 0;
