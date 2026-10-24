#!/usr/bin/env node
import { temporaryTestRoot } from '../../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
// ai-insights: 事実の JSON の「Amazon の健康状態」(facts.amazon_health) の試験
// 実行: node apps/ai-insights/scripts/test-amazon-health.mjs
//
// 試すこと (表はミニPC の取得スクリプトの CREATE TABLE をそのまま使う。:memory: の SQLite):
//   1. 表が無い → 全項目 no_data (表が無い)・例外を出さない
//   2. 表が空 → 全項目 no_data (表が空)
//   3. 古い (最新が期間の最後の日より 3 日を超えて前) → no_data (古い)。ちょうど 3 日前は ok
//   4. ふつう → 各項目の数字・並び順 (見本のデータで手計算した値)
//   5. 返品が 0 件でも取得の記録が新しければ ok (0 件)
//   6. 読めない表 (列が違う) → その項目だけ no_data (読めない)・他の項目は ok
//   7. sales:amazon / margin:amazon が禁止の期間は節ごと出さない
//   8. 月次 (月の値・前月・前の 3 か月の中央値・広告はその月)
//   9. Render の mirror (本物の表の形): カート価格だけ mirror_amazon_price_snapshot_daily から読める・売上は finance の統合 view
//  10. プロンプト: 新しいルールが入る・金額の *_disp が付く・amazon_health の数字を根拠に使える

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

// 🚨 取得スクリプトは import 'dotenv/config' を持つ → 無い場所を指して .env を読まない
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'ai-insights-amazon-'));
process.env.DOTENV_CONFIG_PATH = path.join(scratch, 'no-such.env');
process.env.DATA_DIR = scratch;

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1')), '../../..');
const load = (rel) => import(pathToFileURL(path.join(repoRoot, rel)).href);

const { default: Database } = await import('better-sqlite3');
const { buildAmazonHealthFacts, amazonHealthSuppressReason, STALE_AFTER_DAYS } = await load('apps/ai-insights/facts-amazon.js');
const ST = await load('apps/warehouse/fetch-amazon-sales-traffic.js');
const LI = await load('apps/warehouse/fetch-amazon-listing-issues.js');
const RET = await load('apps/warehouse/fetch-amazon-returns.js');
const RB = await load('apps/warehouse/fetch-amazon-reimbursements.js');
const BSR = await load('apps/warehouse/fetch-amazon-bsr.js');
const FP = await load('apps/warehouse/fba-planning-history.js');
const ADS = await load('apps/warehouse/fetch-amazon-ads.js');
const SP = await load('apps/warehouse/sp-api-report.js');
const { buildPrompt, enrichFactsDisplay, validateOutput, PROMPT_VERSION } = await load('scripts/ai-insights/weekly-prompt.js');
const { buildMonthlyPrompt, MONTHLY_PROMPT_VERSION } = await load('scripts/ai-insights/monthly-prompt.js');

let pass = 0, fail = 0;
function ok(cond, name, extra) {
  if (cond) { pass++; console.log(`  ✅ ${name}`); }
  else { fail++; console.log(`  ❌ ${name}${extra !== undefined ? ` — ${JSON.stringify(extra)?.slice(0, 400)}` : ''}`); }
}

function addDays(ymd, delta) {
  const [y, m, d] = ymd.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d) + delta * 86400000).toISOString().slice(0, 10);
}
function range(from, toIncl) {
  const out = [];
  for (let d = from; d <= toIncl; d = addDays(d, 1)) out.push(d);
  return out;
}
function ins(db, table, row) {
  const cols = Object.keys(row);
  db.prepare(`INSERT INTO ${table} (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`).run(...cols.map((c) => row[c]));
}

// fetch-amazon-prices.js の表 (CREATE は main の中にあり export されていないため同じ形で作る)
const PRICE_DDL = `CREATE TABLE IF NOT EXISTS fact_amazon_price_snapshot (
  date_jst TEXT NOT NULL, seller_sku TEXT NOT NULL, asin TEXT NOT NULL, channel TEXT,
  my_price REAL, buybox_price REAL, buybox_is_mine INTEGER, fetched_at TEXT NOT NULL,
  PRIMARY KEY (date_jst, seller_sku))`;

function createAll(db) {
  ST.ensureSalesTrafficTables(db);
  LI.ensureListingTables(db);
  RET.ensureReturnsTable(db);
  RB.ensureReimbursementsTable(db);
  BSR.ensureRankTables(db);
  FP.ensurePlanningHistoryTables(db);
  ADS.ensureAttribTables(db);
  SP.ensureReportFetchDays(db);
  db.exec(PRICE_DDL);
}

// 対象週: 2026-10-12(月)〜2026-10-18(日)。前週 10-05〜10-11。4 週の中央値 09-14〜10-11
const PS = '2026-10-12';
const PE = '2026-10-19';
const REF = '2026-10-18';
const weekly = (extra = {}) => ({
  periodStart: PS, periodEnd: PE, granularity: 'week', prevStart: addDays(PS, -7),
  baseline: [1, 2, 3, 4].map((w) => [addDays(PS, -7 * w), addDays(PS, -7 * (w - 1))]),
  ...extra,
});
const ITEM_KEYS = ['store_traffic', 'sku_conversion_drops', 'listing_problems', 'buybox_lost', 'returns', 'reimbursements', 'bsr_drops', 'long_term_storage', 'ads_poor_roas'];
const T = { report_id: 'r1', generation: 1, fetched_at: '2026-10-19T00:00:00Z' };

/** ふつうの見本データ */
function seedNormal(db, { skipReturnsRows = false } = {}) {
  // 店全体: 09-14〜10-18。09-14 の週だけセッション 2,000/日 (中央値の試験)。今週はセッション 1,200・48 個・カート 90%
  for (const d of range('2026-09-14', REF)) {
    const cur = d >= PS;
    const first = d < '2026-09-21';
    ins(db, 'amazon_sales_traffic_daily', {
      date_jst: d, sessions: cur ? 1200 : first ? 2000 : 1000, page_views: 1500,
      units_ordered: cur ? 48 : 50, ordered_product_sales: 100000, buy_box_percentage: cur ? 90 : 95, ...T,
    });
  }
  // SKU: 前週と今週
  const SKUS = {
    'SKU-A': { asin: 'B0AAAAAAAA', prev: [100, 10, 30000, 95], cur: [100, 4, 12000, 95] },   // 転換率 10% → 4%
    'SKU-B': { asin: 'B0BBBBBBBB', prev: [50, 2, 50000, 95], cur: [50, 2, 50000, 70] },     // カート 95 → 70
    'SKU-C': { asin: 'B0CCCCCCCC', prev: [80, 4, 8000, 95], cur: [80, 4, 8000, 95] },       // 変化なし
    'SKU-D': { asin: 'B0DDDDDDDD', prev: [2, 1, 1000, 95], cur: [2, 0, 0, 95] },            // セッションが少ない → 対象外
  };
  for (const d of range('2026-10-05', REF)) {
    for (const [sku, s] of Object.entries(SKUS)) {
      const [sess, units, sales, bb] = d >= PS ? s.cur : s.prev;
      ins(db, 'amazon_sales_traffic_sku_daily', {
        date_jst: d, seller_sku: sku, child_asin: s.asin, sessions: sess, page_views: sess * 1.5,
        buy_box_percentage: bb, units_ordered: units, ordered_product_sales: sales, ...T,
      });
    }
  }
  // 出品の状態 (10-18 の写し)
  const LS = { marketplace_id: 'A1VC38T7YXB528', is_discoverable: 1, warning_count: 0, search_suppressed: 0, source: 'scan', fetched_at: T.fetched_at };
  ins(db, 'amazon_listing_status_daily', { ...LS, date_jst: REF, sku_key: 'sku-a', seller_sku: 'SKU-A', asin: 'B0AAAAAAAA', item_name: '商品A', status: 'DISCOVERABLE', is_buyable: 0, error_count: 1, suppressed: 1 });
  ins(db, 'amazon_listing_status_daily', { ...LS, date_jst: REF, sku_key: 'sku-z', seller_sku: 'SKU-Z', asin: 'B0ZZZZZZZZ', item_name: '商品Z', status: 'BUYABLE,DISCOVERABLE', is_buyable: 1, error_count: 2, suppressed: 0 });
  ins(db, 'amazon_listing_status_daily', { ...LS, date_jst: REF, sku_key: 'sku-c', seller_sku: 'SKU-C', asin: 'B0CCCCCCCC', item_name: '商品C', status: 'BUYABLE,DISCOVERABLE', is_buyable: 1, error_count: 0, suppressed: 0 });
  ins(db, 'amazon_listing_issue_daily', { date_jst: REF, sku_key: 'sku-a', issue_key: '90220|ERROR|[]', code: '90220', severity: 'ERROR', message: '必須の項目が足りません', fetched_at: T.fetched_at });
  ins(db, 'amazon_listing_fetch_runs', { date_jst: REF, status: 'complete', listings: 3, not_found: 0, windows: 1, capped_windows: 0, calls: 1, errors: 0, lookup_requested: 0, started_at: T.fetched_at, finished_at: T.fetched_at });
  // カート価格: SKU-B は 10-16〜18 の 3 日が他社
  for (const d of range(PS, REF)) {
    ins(db, 'fact_amazon_price_snapshot', { date_jst: d, seller_sku: 'SKU-B', asin: 'B0BBBBBBBB', my_price: 2980, buybox_price: d >= '2026-10-16' ? 2780 : 2980, buybox_is_mine: d >= '2026-10-16' ? 0 : 1, fetched_at: T.fetched_at });
    ins(db, 'fact_amazon_price_snapshot', { date_jst: d, seller_sku: 'SKU-C', asin: 'B0CCCCCCCC', my_price: 1000, buybox_price: 1000, buybox_is_mine: 1, fetched_at: T.fetched_at });
  }
  // 返品・補てん + 取得の記録
  for (const d of range('2026-10-05', REF)) {
    for (const rt of ['GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA', 'GET_FBA_REIMBURSEMENTS_DATA']) {
      ins(db, 'sp_report_fetch_days', { report_type: rt, date_jst: d, generation: 1, report_id: 'r1', window_from: '2026-10-05', window_to: REF, row_count: 0, fetched_at: T.fetched_at });
    }
  }
  if (!skipReturnsRows) {
    const R = { fnsku: 'X0', license_plate_number: '', occurrence: 1, ...T, first_seen_at: T.fetched_at };
    ins(db, 'amazon_fba_returns', { ...R, order_id: 'o1', seller_sku: 'SKU-A', return_date: '2026-10-13T10:00:00+09:00', return_date_jst: '2026-10-13', asin: 'B0AAAAAAAA', product_name: '商品A', quantity: 2, reason: 'DEFECTIVE' });
    ins(db, 'amazon_fba_returns', { ...R, order_id: 'o2', seller_sku: 'SKU-A', return_date: '2026-10-14T10:00:00+09:00', return_date_jst: '2026-10-14', asin: 'B0AAAAAAAA', product_name: '商品A', quantity: 1, reason: 'UNWANTED_ITEM' });
    ins(db, 'amazon_fba_returns', { ...R, order_id: 'o3', seller_sku: 'SKU-C', return_date: '2026-10-15T10:00:00+09:00', return_date_jst: '2026-10-15', asin: 'B0CCCCCCCC', product_name: '商品C', quantity: 1, reason: 'DEFECTIVE' });
    ins(db, 'amazon_fba_returns', { ...R, order_id: 'o4', seller_sku: 'SKU-A', return_date: '2026-10-07T10:00:00+09:00', return_date_jst: '2026-10-07', asin: 'B0AAAAAAAA', product_name: '商品A', quantity: 1, reason: 'DEFECTIVE' });
  }
  const RBR = { seller_sku: 'SKU-A', fnsku: 'X0', occurrence: 1, ...T, first_seen_at: T.fetched_at };
  ins(db, 'amazon_fba_reimbursements', { ...RBR, reimbursement_id: 'rb1', approval_date: '2026-10-14T00:00:00+09:00', approval_date_jst: '2026-10-14', reason: 'Lost_Warehouse', amount_total: 3000, quantity_reimbursed_total: 2 });
  ins(db, 'amazon_fba_reimbursements', { ...RBR, reimbursement_id: 'rb2', approval_date: '2026-10-06T00:00:00+09:00', approval_date_jst: '2026-10-06', reason: 'Damaged_Warehouse', amount_total: 1000, quantity_reimbursed_total: 1 });
  // BSR: B0AAAAAAAA の大分類が 1,000 → 2,500 位 (分類の順位は小さいが大分類を使う)。B0CCCCCCCC は 500 位のまま
  for (const d of range('2026-10-05', REF)) {
    const cur = d >= PS;
    const RK = { marketplace_id: 'A1VC38T7YXB528', observed_at: T.fetched_at };
    ins(db, 'amazon_catalog_rank_daily', { ...RK, date_jst: d, asin: 'B0AAAAAAAA', rank_type: 'display_group', category_id: 'home_display_on_website', category_title: 'ホーム&キッチン', rank: cur ? 2500 : 1000 });
    ins(db, 'amazon_catalog_rank_daily', { ...RK, date_jst: d, asin: 'B0AAAAAAAA', rank_type: 'classification', category_id: '123', category_title: '保存容器', rank: cur ? 30 : 20 });
    ins(db, 'amazon_catalog_rank_daily', { ...RK, date_jst: d, asin: 'B0CCCCCCCC', rank_type: 'display_group', category_id: 'home_display_on_website', category_title: 'ホーム&キッチン', rank: 500 });
  }
  // 在庫計画: 10-18 と 10-11 の写し
  const PD = { market: 'jp', fetched_at: T.fetched_at, saved_at: T.fetched_at, report_rows: 2, saved_rows: 2, skipped_no_stock: 0, missing_columns: '[]' };
  for (const d of ['2026-10-11', REF]) ins(db, 'fba_planning_daily_days', { ...PD, snapshot_date: d });
  ins(db, 'fba_planning_daily', { snapshot_date: REF, market: 'jp', amazon_sku: 'SKU-A', asin: 'B0AAAAAAAA', available: 40, ais_est_181_210: 1200, ais_est_365_plus: 800,
    inv_age_181_270: 10, inv_age_271_365: 5, inv_age_365_plus: 3, estimated_storage_cost_next_month: 500, estimated_excess_qty: 20, recommended_action: 'Create removal order' });
  ins(db, 'fba_planning_daily', { snapshot_date: REF, market: 'jp', amazon_sku: 'SKU-C', asin: 'B0CCCCCCCC', available: 10, ais_est_211_240: 300,
    inv_age_181_330: 4, estimated_storage_cost_next_month: 100 });
  ins(db, 'fba_planning_daily', { snapshot_date: '2026-10-11', market: 'jp', amazon_sku: 'SKU-A', asin: 'B0AAAAAAAA', available: 40, ais_est_181_210: 1000 });
  // 広告: 09-28〜10-18 (週次の窓 = 09-28〜10-04)。SKU-A は 1,000 円/日で 14 日売上 1,500 円 (ROAS 1.5)・SKU-C は 500 円で 3,000 円・粒度なし 200 円
  for (const d of range('2026-09-28', REF)) {
    const A = { campaign_id: 'c1', ad_group_id: 'g1', impressions: 100, source_rows: 1, generation: 1, report_id: 'r1', ingested_at: T.fetched_at };
    ins(db, 'fact_amazon_ads_sku_attrib', { ...A, date_jst: d, target: 'sku-a', target_granularity: 'sku', advertised_sku: 'SKU-A', clicks: 10, cost: 1000, sales_14d: 1500 });
    ins(db, 'fact_amazon_ads_sku_attrib', { ...A, date_jst: d, target: 'sku-c', target_granularity: 'sku', advertised_sku: 'SKU-C', clicks: 5, cost: 500, sales_14d: 3000 });
    ins(db, 'fact_amazon_ads_sku_attrib', { ...A, date_jst: d, ad_group_id: 'g2', target: '', target_granularity: 'none', clicks: 2, cost: 200, sales_14d: 0 });
    ins(db, 'ads_attrib_fetch_days', { report_type: 'spAdvertisedProduct', profile_id: 'p1', date_jst: d, generation: 1, report_id: 'r1', window_from: '2026-09-18', window_to: REF, row_count: 3, cost_total: 1700, fetched_at: T.fetched_at });
  }
}

// ═══ 1. 表が無い ═══
console.log('\n■ 1. 表が無い');
{
  const db = new Database(':memory:');
  let out = null, err = null;
  try { out = buildAmazonHealthFacts(db, weekly()); } catch (e) { err = e; }
  ok(!err && out, '例外を出さずに節を作る', err?.message);
  ok(ITEM_KEYS.every((k) => out[k]?.status === 'no_data' && /表が無い/.test(out[k].reason)), '全項目 no_data (表が無い)', ITEM_KEYS.map((k) => out?.[k]?.reason));
  ok(out.available_items.length === 0 && out.no_data_items.length === ITEM_KEYS.length, 'available_items 0 / no_data_items 9', out);
  ok(out.period.from === PS && out.period.to === REF && out.prev_period.from === '2026-10-05' && out.stale_after_days === STALE_AFTER_DAYS, '期間と古さの基準', out.period);
  ok(JSON.stringify(out).length < 4000, 'データなしの節は小さい (プロンプトを膨らませない)', JSON.stringify(out).length);
  db.close();
}

// ═══ 2. 表が空 ═══
console.log('\n■ 2. 表が空');
{
  const db = new Database(':memory:');
  createAll(db);
  const out = buildAmazonHealthFacts(db, weekly());
  ok(ITEM_KEYS.every((k) => out[k]?.status === 'no_data' && /表が空/.test(out[k].reason)), '全項目 no_data (表が空)', ITEM_KEYS.map((k) => out[k]?.reason));
  db.close();
}

// ═══ 3. 古い ═══
console.log('\n■ 3. 古い (取れていない)');
{
  const db = new Database(':memory:');
  createAll(db);
  seedNormal(db);
  // 店全体のアクセスを 10-14 で止める (10-18 の 4 日前) / 在庫計画は 10-15 (ちょうど 3 日前) まで
  db.prepare("DELETE FROM amazon_sales_traffic_daily WHERE date_jst > '2026-10-14'").run();
  db.prepare("DELETE FROM fba_planning_daily WHERE snapshot_date = ?").run(REF);
  db.prepare("DELETE FROM fba_planning_daily_days WHERE snapshot_date = ?").run(REF);
  ins(db, 'fba_planning_daily_days', { snapshot_date: '2026-10-15', market: 'jp', fetched_at: T.fetched_at, saved_at: T.fetched_at, report_rows: 1, saved_rows: 1, skipped_no_stock: 0, missing_columns: '[]' });
  ins(db, 'fba_planning_daily', { snapshot_date: '2026-10-15', market: 'jp', amazon_sku: 'SKU-A', ais_est_181_210: 700 });
  // カート価格は 10-14 で止める
  db.prepare("DELETE FROM fact_amazon_price_snapshot WHERE date_jst > '2026-10-14'").run();
  const out = buildAmazonHealthFacts(db, weekly());
  ok(out.store_traffic.status === 'no_data' && /古い/.test(out.store_traffic.reason) && out.store_traffic.latest === '2026-10-14',
    '店全体のアクセス: 最新 10-14 → no_data (古い)', out.store_traffic);
  ok(out.buybox_lost.status === 'no_data' && /古い/.test(out.buybox_lost.reason), 'カート価格: 最新 10-14 → no_data (古い)', out.buybox_lost);
  ok(out.long_term_storage.status === 'ok' && out.long_term_storage.as_of === '2026-10-15' && out.long_term_storage.ais_est_yen === 700,
    '在庫計画: 最新 10-15 (ちょうど 3 日前) → ok', out.long_term_storage);
  ok(out.sku_conversion_drops.status === 'ok', '他の項目は今までどおり ok (SKU のアクセス)', out.sku_conversion_drops.reason);
  // 取得が止まって返品の記録も古い → no_data
  db.prepare("DELETE FROM sp_report_fetch_days WHERE date_jst > '2026-10-10'").run();
  db.prepare('DELETE FROM amazon_fba_returns').run();
  const out2 = buildAmazonHealthFacts(db, weekly());
  ok(out2.returns.status === 'no_data' && /古い/.test(out2.returns.reason), '返品: 取得の記録が 10-10 で止まり返品も無い → no_data (古い)', out2.returns);
  db.close();
}

// ═══ 4. ふつう ═══
console.log('\n■ 4. ふつう');
let normal;
{
  const db = new Database(':memory:');
  createAll(db);
  seedNormal(db);
  normal = buildAmazonHealthFacts(db, weekly());
  const o = normal;
  ok(o.available_items.length === ITEM_KEYS.length, '全項目 ok', o.no_data_items.map((k) => [k, o[k].reason]));

  const st = o.store_traffic;
  ok(st.this_week.sessions === 8400 && st.this_week.units_ordered === 336 && st.this_week.cvr_pct === 4 && st.this_week.buy_box_pct === 90
    && st.this_week.ordered_sales_yen === 700000 && st.this_week.days === 7,
  '店全体 今週: セッション 8,400・336 個・転換率 4.0%・カート 90%・注文売上 70 万', st.this_week);
  ok(st.prev_week.sessions === 7000 && st.prev_week.cvr_pct === 5 && st.prev_week.buy_box_pct === 95, '店全体 前週: 7,000・5.0%・95%', st.prev_week);
  ok(st.prior_4w_median.sessions === 7000 && st.prior_4w_median.cvr_pct === 5 && st.prior_4w_median.periods_used === 4,
    '4 週の中央値 (7000,7000,7000,14000 → 7000 / 5,5,5,2.5 → 5)', st.prior_4w_median);
  ok(st.sessions_change_pct === 20 && st.cvr_change_pt === -1 && st.buy_box_change_pt === -5, '前週比 +20% / -1.0pt / -5.0pt', st);

  const sd = o.sku_conversion_drops;
  ok(sd.checked_skus === 3 && sd.matched_skus === 2, '比べた SKU 3 (D はセッション不足)・落ちた SKU 2', sd);
  ok(sd.items[0].sku === 'SKU-B' && sd.items[0].drop === 'buy_box' && sd.items[0].buy_box_change_pt === -25 && sd.items[0].prev_week_sales_yen === 350000,
    '1 位 SKU-B (前週売上 35 万・カート -25pt)', sd.items[0]);
  ok(sd.items[1].sku === 'SKU-A' && sd.items[1].drop === 'cvr' && sd.items[1].cvr_pct === 4 && sd.items[1].prev_week_cvr_pct === 10 && sd.items[1].cvr_change_pt === -6,
    '2 位 SKU-A (転換率 10% → 4%)', sd.items[1]);
  ok(sd.items[1].product_name === '商品A', '商品名は出品の写しから', sd.items[1]);

  const lp = o.listing_problems;
  ok(lp.as_of === REF && lp.snapshot === 'complete' && lp.problem_skus_total === 2 && lp.not_buyable_skus === 1 && lp.selling_problem_skus === 1,
    '出品: 問題 2 SKU (うち売れ筋 1)・買えない 1', lp);
  ok(lp.items.length === 1 && lp.items[0].sku === 'SKU-A' && lp.items[0].sales_28d_yen === 294000 && lp.items[0].top_error?.code === '90220' && lp.items[0].suppressed === true,
    '売れ筋の SKU-A (28 日 29.4 万・ERROR 90220・停止)', lp.items[0]);

  const bb = o.buybox_lost;
  ok(bb.as_of === REF && bb.lost_now_skus === 1 && bb.items[0].sku === 'SKU-B' && bb.items[0].lost_days === 3 && bb.items[0].seen_days === 7
    && bb.items[0].buybox_price_yen === 2780 && bb.items[0].sales_28d_yen === 700000,
  'カート: SKU-B が他社 (今週 3/7 日・カート価格 2,780 円・28 日 70 万)', bb);

  const rt = o.returns;
  ok(rt.returned_units === 4 && rt.prev_week_returned_units === 1, '返品: 今週 4 個・前週 1 個', rt);
  ok(rt.top_reasons[0].reason === 'DEFECTIVE' && rt.top_reasons[0].units === 3 && rt.top_reasons[1].reason === 'UNWANTED_ITEM', '理由の上位 DEFECTIVE 3・UNWANTED_ITEM 1', rt.top_reasons);
  ok(rt.top_skus[0].sku === 'SKU-A' && rt.top_skus[0].returned_units === 3 && rt.top_skus[0].sold_units === 28 && rt.top_skus[0].return_rate_pct === 10.7
    && rt.top_skus[0].reasons.length === 2, 'SKU-A 3 個 / 販売 28 個 = 10.7%', rt.top_skus[0]);

  const rb = o.reimbursements;
  ok(rb.cases === 1 && rb.amount_yen === 3000 && rb.prev_week_amount_yen === 1000 && rb.by_reason[0].reason === 'Lost_Warehouse', '補てん: 今週 3,000 円 (前週 1,000 円)', rb);

  const bs = o.bsr_drops;
  ok(bs.checked_asins === 2 && bs.matched_asins === 1, 'BSR: 2 ASIN を比べて 1 つ悪化', bs);
  ok(bs.items[0].asin === 'B0AAAAAAAA' && bs.items[0].rank_type === 'display_group' && bs.items[0].rank === 2500 && bs.items[0].prev_week_rank === 1000
    && bs.items[0].rank_worse_pct === 150 && bs.items[0].prev_week_sales_yen === 210000, 'B0AAAAAAAA 大分類 1,000 → 2,500 位 (+150%・前週売上 21 万)', bs.items[0]);

  const lt = o.long_term_storage;
  ok(lt.as_of === REF && lt.ais_est_yen === 2300 && lt.aged_181_plus_units === 22 && lt.storage_next_month_yen === 600 && lt.excess_units === 20,
    '長期保管料: 見込み 2,300 円・181 日以上 22 個 (区分を重ねない)・保管料 600 円・過剰 20 個', lt);
  ok(lt.prev_week_snapshot?.as_of === '2026-10-11' && lt.prev_week_snapshot.ais_est_yen === 1000, '前週の写し 10-11 の見込み 1,000 円', lt.prev_week_snapshot);
  ok(lt.top_skus[0].sku === 'SKU-A' && lt.top_skus[0].ais_est_yen === 2000 && lt.top_skus[0].product_name === '商品A', '上位 SKU-A 2,000 円', lt.top_skus[0]);

  const ad = o.ads_poor_roas;
  ok(ad.window.from === '2026-09-28' && ad.window.to === '2026-10-04', '広告の窓 = 14 日前にずらした週 (09-28〜10-04)', ad.window);
  ok(ad.spend_yen === 11900 && ad.sales_14d_yen === 31500 && ad.roas_14d === 2.65, '広告 全体: 費用 11,900 円・14 日売上 31,500 円・ROAS 2.65', ad);
  ok(ad.matched_targets === 1 && ad.items[0].sku === 'SKU-A' && ad.items[0].spend_yen === 7000 && ad.items[0].sales_14d_yen === 10500 && ad.items[0].roas_14d === 1.5,
    'ROAS が悪い SKU-A (7,000 円で 10,500 円・1.5)', ad.items);
  ok(JSON.stringify(o).length < 12000, `節の大きさは控えめ (${JSON.stringify(o).length} 字)`);
  db.close();
}

// ═══ 5. 返品 0 件 ═══
console.log('\n■ 5. 返品が 0 件の週');
{
  const db = new Database(':memory:');
  createAll(db);
  seedNormal(db, { skipReturnsRows: true });
  const out = buildAmazonHealthFacts(db, weekly());
  ok(out.returns.status === 'ok' && out.returns.returned_units === 0 && out.returns.top_skus.length === 0,
    '返品の表が空でも取得の記録が新しい → ok (0 個)', out.returns);
  db.close();
}

// ═══ 6. 読めない表 ═══
console.log('\n■ 6. 読めない表');
{
  const db = new Database(':memory:');
  createAll(db);
  seedNormal(db);
  db.exec('DROP TABLE amazon_catalog_rank_daily');
  db.exec('CREATE TABLE amazon_catalog_rank_daily (date_jst TEXT, asin TEXT)');
  for (const d of range('2026-10-05', REF)) db.prepare('INSERT INTO amazon_catalog_rank_daily VALUES (?, ?)').run(d, 'B0AAAAAAAA');
  const out = buildAmazonHealthFacts(db, weekly());
  ok(out.bsr_drops.status === 'no_data' && /読めない/.test(out.bsr_drops.reason), 'BSR だけ no_data (読めない)', out.bsr_drops);
  ok(out.available_items.length === ITEM_KEYS.length - 1, '他の 8 項目は ok', out.no_data_items);
  db.close();
}

// ═══ 7. 禁止領域 ═══
console.log('\n■ 7. Amazon の売上・粗利が禁止の期間');
{
  ok(amazonHealthSuppressReason({ prohibited_topic_areas: [{ area: 'sales:yahoo' }] }) === null, '他モールの禁止では出す');
  const reason = amazonHealthSuppressReason({ prohibited_topic_areas: [{ area: 'margin:amazon' }] });
  ok(/margin:amazon/.test(reason || ''), 'margin:amazon → 出さない理由', reason);
  const db = new Database(':memory:');
  createAll(db);
  seedNormal(db);
  const out = buildAmazonHealthFacts(db, weekly({ suppressReason: amazonHealthSuppressReason({ prohibited_topic_areas: [{ area: 'sales:amazon' }] }) }));
  ok(out.status === 'no_data' && /sales:amazon/.test(out.reason) && out.store_traffic === undefined && out.no_data_items.length === ITEM_KEYS.length,
    'sales:amazon → 節ごと no_data (数字を渡さない)', out);
  db.close();
}

// ═══ 8. 月次 ═══
console.log('\n■ 8. 月次');
{
  const db = new Database(':memory:');
  createAll(db);
  // 06-01〜09-30: 9 月はセッション 1,500/日・60 個、8 月 1,000/日・50 個、6・7 月 1,000/日・40 個
  for (const d of range('2026-06-01', '2026-09-30')) {
    const m = d.slice(0, 7);
    ins(db, 'amazon_sales_traffic_daily', {
      date_jst: d, sessions: m === '2026-09' ? 1500 : 1000, page_views: 2000,
      units_ordered: m === '2026-09' ? 60 : m === '2026-08' ? 50 : 40, ordered_product_sales: 50000, buy_box_percentage: 92, ...T,
    });
  }
  for (const d of range('2026-09-01', '2026-09-30')) {
    ins(db, 'fact_amazon_ads_sku_attrib', { date_jst: d, campaign_id: 'c1', ad_group_id: 'g1', target: 'sku-a', target_granularity: 'sku', advertised_sku: 'SKU-A',
      impressions: 100, clicks: 10, cost: 1000, sales_14d: 1000, source_rows: 1, generation: 1, report_id: 'r1', ingested_at: T.fetched_at });
  }
  const out = buildAmazonHealthFacts(db, {
    periodStart: '2026-09-01', periodEnd: '2026-10-01', granularity: 'month', prevStart: '2026-08-01',
    baseline: [['2026-08-01', '2026-09-01'], ['2026-07-01', '2026-08-01'], ['2026-06-01', '2026-07-01']],
  });
  const st = out.store_traffic;
  ok(st.status === 'ok' && st.this_month.sessions === 45000 && st.this_month.cvr_pct === 4 && st.this_month.days === 30, '9 月: セッション 45,000・転換率 4.0%', st.this_month);
  ok(st.prev_month.sessions === 31000 && st.prev_month.cvr_pct === 5, '8 月: 31,000・5.0%', st.prev_month);
  ok(st.prior_3m_median.periods_used === 3 && st.prior_3m_median.cvr_pct === 4, '前の 3 か月の中央値 (5, 4, 4 → 4)', st.prior_3m_median);
  ok(st.cvr_change_pt === -1 && st.sessions_change_pct === 45.2, '前月比 -1.0pt・セッション +45.2%', st);
  const ad = out.ads_poor_roas;
  ok(ad.status === 'ok' && ad.window.from === '2026-09-01' && ad.window.to === '2026-09-30' && ad.items[0]?.roas_14d === 1 && ad.items[0].spend_yen === 30000,
    '広告はその月の窓・SKU-A の ROAS 1.0', ad);
  ok(out.sku_conversion_drops.status === 'no_data' && /表が空/.test(out.sku_conversion_drops.reason), 'SKU の表は空 → no_data', out.sku_conversion_drops);
  db.close();
}

// ═══ 9. Render の mirror ═══
console.log('\n■ 9. Render の mirror (本物の表の形)');
{
  const { initMirrorDB, getMirrorDB } = await load('apps/warehouse-mirror/db.js');
  initMirrorDB();
  const db = getMirrorDB();
  const M = { source_run_id: 'run1', source_row_hash: 'h', synced_at: T.fetched_at };
  for (const d of range(PS, REF)) {
    ins(db, 'mirror_amazon_price_snapshot_daily', { ...M, source_row_hash: `p-${d}`, date_jst: d, seller_sku: 'SKU-B', asin: 'B0BBBBBBBB', my_price: 2980, buybox_price: 2780, buybox_is_mine: 0, fetched_at: T.fetched_at });
  }
  for (const d of range('2026-09-21', REF)) {
    ins(db, 'mirror_amazon_finance_sku_daily', { ...M, source_row_hash: `f-${d}`, date_jst: d, seller_sku: 'SKU-B', asin_norm: 'B0BBBBBBBB', product_name: '商品B',
      units_ordered: 1, units_net_sold: 1, sales_principal_jpy: 1000, cost_status: 'complete' });
  }
  const out = buildAmazonHealthFacts(db, weekly());
  ok(out.buybox_lost.status === 'ok' && out.buybox_lost.items[0]?.sku === 'SKU-B' && out.buybox_lost.items[0].lost_days === 7,
    'カートは mirror_amazon_price_snapshot_daily から読める', out.buybox_lost);
  ok(/決済ベース/.test(out.buybox_lost.sales_basis) && out.buybox_lost.items[0].sales_28d_yen > 0 && out.buybox_lost.items[0].product_name === '商品B',
    '売上・商品名は finance の統合 view (決済ベース)', out.buybox_lost);
  const others = ITEM_KEYS.filter((k) => k !== 'buybox_lost');
  ok(others.every((k) => out[k].status === 'no_data' && /表が無い/.test(out[k].reason)), 'mirror に無い 8 項目は no_data (表が無い)', others.map((k) => out[k].reason));
  try { db.close(); } catch { /* noop */ }
}

// ═══ 10. プロンプト ═══
console.log('\n■ 10. プロンプト');
{
  const input = {
    meta: { period_start: PS, period_label: `${PS}〜${REF}` },
    constraints: { prohibited_topic_areas: [] },
    coverage: [],
    previous_open_topics: [],
    facts: { amazon_health: JSON.parse(JSON.stringify(normal)) },
  };
  enrichFactsDisplay(input);
  ok(input.facts.amazon_health.listing_problems.items[0].sales_28d_disp === '29.4万円', '金額に *_disp (万円) が付く', input.facts.amazon_health.listing_problems.items[0]);
  const p = buildPrompt(input);
  ok(PROMPT_VERSION === 'wk-v2' && /facts\.amazon_health/.test(p) && /no_data の項目は論点にも根拠にも使わず、推測で埋めない/.test(p), '週次: 新しいルール 11 (wk-v2)');
  const mp = buildMonthlyPrompt({ ...input, meta: { month_ym: '2026-09' } });
  ok(MONTHLY_PROMPT_VERSION === 'mo-v2' && /facts\.amazon_health/.test(mp) && /推測で埋めない/.test(mp), '月次: 新しいルール 10 (mo-v2)');
  let err = null;
  try {
    validateOutput({
      summary: 'Amazon のカート獲得率が低下',
      topics: [{ title: 'SKU-B のカート獲得率 95%→70%', category: 'sales', evidence: 'カート獲得率 70% (前週 95%)・前週売上 35万円', confidence: 'med' }],
    }, input);
  } catch (e) { err = e; }
  ok(!err, 'amazon_health の数字 (70・95・35万円) は根拠に使える (数字の検査を通る)', err?.message);
  let err2 = null;
  try {
    validateOutput({ summary: 'x', topics: [{ title: 't', evidence: 'カート獲得率 61%', category: 'sales' }] }, input);
  } catch (e) { err2 = e; }
  ok(err2 && /61/.test(err2.message), 'JSON に無い数字 (61) は今までどおり落ちる', err2?.message);
}

console.log(`\n═══ 結果: ${pass} PASS / ${fail} FAIL ═══`);
process.exitCode = fail > 0 ? 1 : 0;
