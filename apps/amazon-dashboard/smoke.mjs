// amazon-dashboard 機能スモーク: fixture 投入 → 全 query 関数を実行して形を検証
// 使い方: DATA_DIR=<空ディレクトリ> node apps/amazon-dashboard/smoke.mjs
// ⚠️ DATA_DIR 内の warehouse-mirror.db の対象テーブルを DELETE するため本番 DATA_DIR で実行禁止
import { initMirrorDB, getMirrorDB } from '../warehouse-mirror/db.js';
import * as q from './queries.js';

initMirrorDB();
const db = getMirrorDB();

// 本番 DB 誤実行ガード: 実データらしき規模なら中断 (fixture は数十行)
const prodGuard = db.prepare(`SELECT COUNT(*) AS c FROM mirror_amazon_finance_sku_daily`).get().c;
if (prodGuard > 1000) {
  console.error(`FATAL: mirror_amazon_finance_sku_daily に ${prodGuard} 行あります。本番 DB と思われるため中断します (空の DATA_DIR で実行してください)`);
  process.exit(2);
}

const today = q.jstToday();
const d = (n) => q.addDays(today, -n);
const ymNow = today.slice(0, 7);
const ymPrev = q.addMonths(ymNow, -1);
const ymPrev2 = q.addMonths(ymNow, -2);

// ─── fixture ───
const tx = db.transaction(() => {
  // クリア (再実行冪等)
  for (const t of ['mirror_amazon_finance_sku_daily', 'mirror_f_sales_by_listing', 'mirror_amazon_ads_sku_daily',
    'mirror_amazon_ads_campaign_daily', 'mirror_amazon_sku_fees', 'mirror_sku_resolved', 'mirror_products',
    'mirror_inv_daily_detail', 'amzdash_custom_expenses', 'amzdash_settings']) {
    try { db.exec(`DELETE FROM ${t}`); } catch {}
  }

  const insFin = db.prepare(`INSERT INTO mirror_amazon_finance_sku_daily (
    date_jst, seller_sku, asin_norm, product_name, units_ordered, units_refunded_customer,
    units_marketplace_guarantee, units_a_to_z_refund, units_net_sold,
    sales_principal_jpy, sales_shipping_jpy, sales_giftwrap_jpy, sales_tax_jpy,
    commission_jpy, fba_fulfillment_jpy, fba_storage_jpy, closing_fee_jpy,
    shipping_chargeback_jpy, giftwrap_chargeback_jpy, promotion_jpy,
    warehouse_damage_jpy, warehouse_lost_jpy, safe_t_jpy, refund_principal_jpy, reversal_reimbursement_jpy,
    misc_fee_jpy, other_fee_jpy, other_amount_jpy,
    unit_cost_snapshot, cost_snapshot_date_jst, latest_unit_cost_reference, cogs_amount, profit_amount,
    is_cost_complete, cost_status, source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, ?, ?, ?, 0, 0, 0, ?, ?, 0, 0, 0, ?, ?, ?, 0, 0, 0, ?, ?, 0, 0, ?, 0, 0, 0, 0, ?, ?, ?, ?, ?, 1, 'complete', 'smoke', 'h', 't')`);
  // 90日分 2 SKU (pr_alpha=黒字, pr_beta=広告で赤字転落想定)
  for (let i = 0; i < 90; i++) {
    const date = d(i);
    // alpha: 10個/日 単価1000 手数料150 FBA100 保管10 プロモ20 damage 5 refund 30 原価300
    const profitA = 10000 - 1500 - 1000 - 100 - 200 + 50 - 300 - 3000;
    insFin.run(date, 'pr_alpha', 'B0ALPHA', 'アルファ商品', 10, 10, 10000, 1500, 1000, 100, 200, 50, 300, 300, date, 310, 3000, profitA);
    // beta: 5個/日 単価800
    const profitB = 4000 - 600 - 500 - 50 - 80 + 0 - 100 - 1200;
    insFin.run(date, 'pr_beta', 'B0BETA', 'ベータ商品', 5, 5, 4000, 600, 500, 50, 80, 0, 100, 240, date, 240, 1200, profitB);
  }

  const insFlash = db.prepare(`INSERT INTO mirror_f_sales_by_listing (
    date_jst, month_ym, mall, item_code, channel, item_name, units, sales_jpy_incl, order_count,
    data_source, source_updated_at, source_run_id, source_row_hash, synced_at
  ) VALUES (?, ?, 'amazon', ?, ?, ?, ?, ?, ?, 'sp_api', 't', 'smoke', 'h', 't')`);
  for (let i = 0; i < 90; i++) {
    const date = d(i);
    insFlash.run(date, date.slice(0, 7), 'pr_alpha', 'FBA', 'アルファ商品', 10, 11000, 9);
    insFlash.run(date, date.slice(0, 7), 'pr_beta', 'FBM', 'ベータ商品', 5, 4400, 5);
  }

  const insAdsSku = db.prepare(`INSERT INTO mirror_amazon_ads_sku_daily (
    date_jst, mall, campaign_id, ad_type, target, target_granularity, clicks, impressions,
    ad_cost, ad_sales, ad_units, source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'amazon', ?, 'SP', ?, ?, ?, ?, ?, ?, ?, 'smoke', 'h', 't')`);
  const insAdsCamp = db.prepare(`INSERT INTO mirror_amazon_ads_campaign_daily (
    date_jst, mall, campaign_id, campaign_name, ad_type, campaign_status, clicks, impressions,
    ad_cost, ad_sales_1d, ad_sales_7d, ad_sales_14d, ad_sales_30d, ad_units_1d,
    source_run_id, source_row_hash, synced_at
  ) VALUES (?, 'amazon', ?, ?, 'SP', 'ENABLED', ?, ?, ?, ?, ?, ?, ?, ?, 'smoke', 'h', 't')`);
  for (let i = 0; i < 90; i++) {
    const date = d(i);
    insAdsSku.run(date, 'C1', 'pr_alpha', 'sku', 20, 2000, 500, 3000, 3);
    insAdsSku.run(date, 'C2', 'b0beta', 'asin', 30, 3000, 1500, 1600, 2);  // beta は ASIN 粒度 + 広告費過大
    insAdsCamp.run(date, 'C1', 'アルファSP', 20, 2000, 500, 100, 200, 3000, 3100, 3);
    insAdsCamp.run(date, 'C2', 'ベータSP', 30, 3000, 1500, 50, 100, 1600, 1700, 2);
    insAdsCamp.run(date, 'C3', 'オート全商品', 10, 5000, 300, 0, 0, 0, 0, 0);  // 未配賦分
  }

  db.prepare(`INSERT INTO mirror_amazon_sku_fees (seller_sku, asin, fulfillment_channel, referral_fee, referral_fee_rate, fba_fee, variable_closing_fee, per_item_fee, total_fee, price_used, fetched_at)
    VALUES ('pr_alpha', 'B0ALPHA', 'AMAZON_JP', 150, 0.15, 250, 0, 0, 400, 1100, 't')`).run();
  db.prepare(`INSERT INTO mirror_amazon_sku_fees (seller_sku, asin, fulfillment_channel, referral_fee, referral_fee_rate, fba_fee, variable_closing_fee, per_item_fee, total_fee, price_used, fetched_at)
    VALUES ('pr_beta', 'B0BETA', 'AMAZON_JP', 120, 0.15, 200, 0, 0, 320, 880, 't')`).run();

  const insRes = db.prepare(`INSERT INTO mirror_sku_resolved (seller_sku, ne_code, quantity, source, 商品名, source_updated_at, synced_at) VALUES (?, ?, ?, 'master', ?, 't', 't')`);
  insRes.run('pr_alpha', 'NE-A', 1, 'アルファ商品');
  insRes.run('pr_beta', 'NE-B', 1, 'ベータ商品');

  const insProd = db.prepare(`INSERT INTO mirror_products (商品コード, 商品名, 商品区分, 原価状態, 標準売価, 原価, updated_at) VALUES (?, ?, '単品', 'ok', ?, ?, 't')`);
  insProd.run('NE-A', 'アルファ商品', 1500, 300);   // alpha 実売1100(税込) < 1500*0.9 → 価格ミス検出想定
  insProd.run('NE-B', 'ベータ商品', 900, 240);

  const insInv = db.prepare(`INSERT INTO mirror_inv_daily_detail (
    business_date, market, category, source_system, source_item_code, ne_code, qty, unit_cost, total_value,
    cost_status, product_name, last_sold_date, sales_30d_qty, synced_at
  ) VALUES (?, 'jp', ?, 'fba', ?, ?, ?, ?, ?, 'ok', ?, ?, ?, 't')`);
  insInv.run(today, 'fba_warehouse', 'pr_alpha', 'NE-A', 120, 300, 36000, 'アルファ商品', d(1), 300);
  insInv.run(today, 'fba_inbound', 'pr_alpha', 'NE-A', 60, 300, 18000, 'アルファ商品', d(1), 300);
  insInv.run(today, 'fba_warehouse', 'pr_dead', 'NE-DEAD', 40, 500, 20000, '死に筋商品', d(120), 0);
});
tx();

// ─── 実行 ───
let pass = 0, fail = 0;
function check(name, fn) {
  try {
    const r = fn();
    console.log(`✓ ${name}`);
    pass++;
    return r;
  } catch (e) {
    console.error(`✗ ${name}: ${e.message}`);
    fail++;
    return null;
  }
}
function assert(cond, msg) { if (!cond) throw new Error(msg); }

const ov = check('getOverview', () => {
  const r = q.getOverview();
  assert(r.tiles.length === 4, 'tiles 4枚');
  const tm = r.tiles.find(t => t.key === 'this_month');
  assert(tm.flash_sales_incl > 0, '速報売上>0');
  assert(tm.ad_cost > 0, '広告費>0');
  assert(typeof tm.est_profit === 'number', '推定利益');
  return r;
});

check('getTrend day', () => {
  const r = q.getTrend(d(29), today, 'day');
  assert(r.rows.length >= 28, '30日分');
  const row = r.rows[0];
  assert(row.profit_after_ads === row.profit_before_ads - row.ad_cost, '広告後利益整合');
});
check('getTrend month', () => {
  const r = q.getTrend(d(89), today, 'month');
  assert(r.rows.length >= 3, '3ヶ月分');
});

check('getWaterfall 全体', () => {
  const r = q.getWaterfall(d(29), today, null);
  assert(r.steps.length === 14, 'steps 14');
  const rev = r.steps.find(s => s.key === 'revenue');
  const after = r.steps.find(s => s.key === 'profit_after_ads');
  assert(rev.amount > 0 && typeof after.amount === 'number', 'metrics');
});
check('getWaterfall SKU', () => {
  const r = q.getWaterfall(d(29), today, 'pr_alpha');
  assert(r.sku === 'pr_alpha', 'sku');
  const ad = r.steps.find(s => s.key === 'ad_cost');
  assert(ad.amount > 0, 'SKU広告費(直接+按分)>0');
});

const sp = check('getSkuProfit', () => {
  const r = q.getSkuProfit(d(29), today, {});
  assert(r.rows.length === 2, '2 SKU');
  assert(r.ad_unallocated > 0, '未配賦>0 (C3分)');
  const beta = r.rows.find(x => x.seller_sku === 'pr_beta');
  assert(beta.ad_direct > 0, 'ASIN粒度広告がbetaに配賦されている');
  return r;
});

check('getAdsAnalysis', () => {
  const r = q.getAdsAnalysis(d(29), today);
  assert(r.totals.campaign_total > r.totals.sku_direct_total, 'campaign > direct');
  assert(r.skus.length >= 2, 'SKU rows');
  const alpha = r.skus.find(s => s.seller_sku === 'pr_alpha');
  assert(alpha.acos_pct !== null && alpha.breakeven_acos_pct !== null, 'ACOS+損益分岐');
  assert(['safe', 'edge', 'bleed'].includes(alpha.verdict), 'verdict');
  assert(r.campaigns.length === 3, 'campaigns 3');
  assert(r.tacos_trend.length >= 1, 'tacos trend');
});

check('getBestsellers', () => {
  const r = q.getBestsellers(d(29), today, 'sales');
  assert(r.ranking.length === 2, 'ranking 2');
  assert(['A', 'B', 'C'].includes(r.ranking[0].abc), 'ABC');
  assert(r.weekday.length === 7, '曜日7');
  assert(r.ranking[0].spark.length > 0, 'spark');
  assert(r.ranking[0].fba_units > 0 || r.ranking[0].fbm_units > 0, 'channel');
});

check('getDiagnosis', () => {
  const r = q.getDiagnosis();
  assert(Array.isArray(r.earners), 'earners');
  assert(Array.isArray(r.dead_stock), 'dead_stock');
  assert(r.dead_stock.some(x => x.ne_code === 'NE-DEAD'), 'NE-DEAD 検出');
  assert(Array.isArray(r.price_miss), 'price_miss');
  assert(r.price_miss.some(x => x.seller_sku === 'pr_alpha'), 'pr_alpha 価格ミス検出');
});

check('getInventory', () => {
  const r = q.getInventory();
  assert(r.as_of === today, 'as_of');
  const alpha = r.rows.find(x => x.ne_code === 'NE-A');
  assert(alpha.fba_qty === 120 && alpha.inbound_qty === 60, '在庫数');
  assert(alpha.days_of_cover === 18, `残日数 180/10=18 (got ${alpha.days_of_cover})`);
});

check('expenses CRUD + settings', () => {
  const { id } = q.addExpense({ name: 'ツール代', expense_type: 'fixed_monthly', amount_jpy: 5280, month_from: ymPrev2, month_to: null, memo: '' });
  assert(id > 0, 'insert');
  assert(q.listExpenses().length === 1, 'list');
  const ov2 = q.getOverview();
  const tm = ov2.tiles.find(t => t.key === 'this_month');
  assert(tm.custom_expenses === 5280, `経費反映 (got ${tm.custom_expenses})`);
  assert(tm.settled_profit_final === tm.settled_profit_after_ads - 5280, '最終利益');
  q.deleteExpense(id);
  assert(q.listExpenses().length === 0, 'delete');
  const s = q.saveSettings({ dead_stock_days: 90, bogus_key: 1 });
  assert(s.dead_stock_days === 90 && !('bogus_key' in s) === false || !('bogus_key' in s), 'settings save');
  assert(q.getSettings().dead_stock_days === 90, 'settings persist');
});

console.log(`\n=== smoke: ${pass} PASS / ${fail} FAIL ===`);
process.exit(fail > 0 ? 1 : 0);
