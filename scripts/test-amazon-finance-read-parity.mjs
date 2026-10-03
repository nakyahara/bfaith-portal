#!/usr/bin/env node
/**
 * test-amazon-finance-read-parity.mjs — Amazon の財務の読み口を 1 か所にまとめても (F4-1)、
 * 全部の読み手の応答が **1 バイトも変わらない** ことを確かめる (before / after の JSON の一致)。
 *
 * 設計 = AI_reference CompanyDB構想/19 §7 段 1・§10 F4-1 (合格条件 = 画面の値が前と同じ)。
 * Codex R-F4-1 の条件 3 (全部の API の before/after の JSON が一致する試験)。
 *
 * やり方:
 *   1. 決まった中身の fixture (warehouse-mirror.db) を一時の DATA_DIR に作る (時刻は 2026-10-03 12:00 JST に固定・乱数も固定)
 *   2. Amazon の財務を読む全部の口を呼ぶ (HTTP の口は express に mount して本当に GET する)
 *        amazon-dashboard の /api/v1/* (JSON・CSV)・margin-alert の集計と通知文・amazon-pricing の 360 行 / 指紋 / 鮮度 / 判定の run・
 *        supplier-sales の /api/summary・/api/health・CSV・purchase-orders の mall-sales・site-products の /products・
 *        統合の view v_mall_finance_daily_unified (定義の文と中身)・ai-insights の週次 / 月次の入力
 *   3. 各応答の生の文字の sha256 と中身を golden (scripts/fixtures/amazon-finance-read-parity.golden.json) と比べる
 *      golden は **読み口を入れる前の origin/master (1fdd5efa)** のコードで作った (--write)。
 *
 * 使い方:
 *   node scripts/test-amazon-finance-read-parity.mjs            比べる (違えば exit 1・実際の値を一時の場所に書く)
 *   node scripts/test-amazon-finance-read-parity.mjs --write    golden を作り直す (🚨 値を変える PR だけ。差分を PR に載せる)
 *   node scripts/test-amazon-finance-read-parity.mjs --write --code-root <dir>
 *        <dir> (別の worktree) のアプリのコードを呼んで golden を作る。読み口を入れる前のコードで golden を作るときに使う
 *        (例: git worktree add <dir> 1fdd5efa → <dir>/node_modules を用意 → この試験をこの枝から --code-root <dir> で流す)。
 *        比べるとき (--write なし) にも使える = 「前のコード」と golden の一致をいつでも確かめ直せる
 *
 * 🚨 F4 (Amazon 財務の利用側の切替) の間だけの試験。F4-6 で旧い写しを消すときに一緒に消す
 *    (それまでに Amazon の財務の画面の値を変える PR は --write で golden を作り直し、差分を PR の本文に書く)。
 */
import { temporaryTestDataDir } from './test-temp-dir.mjs';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';

const WRITE = process.argv.includes('--write');
// 呼ぶアプリのコードの場所 (既定 = この試験のある repo)。Codex #1599 R1 M4: golden を「前のコード」で作った証跡を再現できるように
const codeRootArg = process.argv.indexOf('--code-root');
const CODE_ROOT_ARG = codeRootArg >= 0 ? process.argv[codeRootArg + 1] : null;
const SCRATCH = await temporaryTestDataDir(import.meta.url, 'afin-parity-');
process.env.DATA_DIR = SCRATCH;
process.env.SITE_PRODUCTS_READ_TOKEN = 'parity-read-token';
process.env.AI_READ_TOKEN = 'parity-ai-read-token';
delete process.env.MARGIN_ALERT_THRESHOLD_PCT;

// ── 時刻と乱数を固定 (応答の generated_at・判定の run の id を決まった値にする) ──
const FIXED_NOW = Date.parse('2026-10-03T03:00:00.000Z');   // = 2026-10-03 12:00 JST (土)
const RealDate = Date;
class FixedDate extends RealDate {
  constructor(...a) { if (a.length === 0) super(FIXED_NOW); else super(...a); }
  static now() { return FIXED_NOW; }
}
globalThis.Date = FixedDate;
let seed = 20261003;
Math.random = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const GOLDEN = path.join(REPO, 'scripts/fixtures/amazon-finance-read-parity.golden.json');
const CODE = CODE_ROOT_ARG ? path.resolve(CODE_ROOT_ARG) : REPO;
if (!fs.existsSync(path.join(CODE, 'apps/warehouse-mirror/db.js'))) throw new Error(`--code-root にアプリのコードが無い: ${CODE}`);
const imp = (p) => import(pathToFileURL(path.join(CODE, p)).href);

const { initMirrorDB, getMirrorDB } = await imp('apps/warehouse-mirror/db.js');
initMirrorDB();
const db = getMirrorDB();

// ─────────────────────────── fixture ───────────────────────────
// NOT NULL で既定値の無い列は型で埋める (TEXT = 'x'・数 = 0)。CHECK のある列は呼び手が渡す
const colCache = new Map();
function ins(table, row) {
  if (!colCache.has(table)) colCache.set(table, db.prepare(`SELECT name, type, "notnull" nn, dflt_value d, pk FROM pragma_table_info(?)`).all(table));
  const vals = { ...row };
  for (const c of colCache.get(table)) {
    if (c.name in vals || !c.nn || c.d != null) continue;
    vals[c.name] = /INT|REAL|NUM/i.test(c.type || '') ? 0 : 'x';
  }
  const cols = Object.keys(vals);
  db.prepare(`INSERT INTO ${table} (${cols.map((c) => `"${c}"`).join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`).run(...cols.map((c) => vals[c]));
}
const addDays = (ymd, n) => { const d = new RealDate(`${ymd}T00:00:00Z`); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
const r2 = (x) => Math.round(x * 100) / 100;

const FIN_FROM = '2025-09-20';
const FIN_LAST = '2026-09-27';   // 決済の最後の日 (この翌日に Easy Ship だけの行がある = 最後の日の判定に入れない)
const FLASH_LAST = '2026-10-02';

// Amazon の SKU: [seller_sku, asin_norm, product_name, FBA?, cost_status, is_cost_complete, 1 日の数量の基準, 単価]
const AMZ = [
  ['pr_alpha', 'B0ALPHA', 'アルファ商品', true, 'complete', 1, 9, 1000],
  ['pr_beta', '', '', false, 'missing_cost', 0, 4, 800],
  ['PR_Gamma', 'B0GAMMA', '', true, 'partial_cost', 0, 3, 2400],
  ['pr_delta', '', 'デルタ', true, 'complete', 1, 1, 500],
  ['pr_unres', '', '解決しない', false, 'late_bound_after_close', 0, 2, 700],
];
db.transaction(() => {
  let i = 0;
  for (let d = FIN_FROM; d <= FIN_LAST; d = addDays(d, 1), i++) {
    AMZ.forEach(([sku, asin, name, fba, cs, icc, base, price], k) => {
      if (sku === 'pr_delta' && i % 4 !== 0) return;          // 売れる日が少ない SKU
      if (sku === 'pr_unres' && i % 3 === 1) return;
      const units = base + ((i * (k + 3)) % 5) - 1;           // 日によって 数量が動く (0 や負の日もある)
      const refundUnits = (i + k) % 11 === 0 ? 1 : 0;
      const principal = units * price;
      const shipping = fba ? 0 : (units > 0 ? 350 : 0);
      const gift = (i + k) % 29 === 0 ? 220 : 0;
      const tax = Math.round((principal + shipping + gift) * 0.1);
      const commission = r2(principal * 0.15 * 1.1);
      const fbaFul = fba && units > 0 ? r2(units * 318) : 0;
      const fbaSto = fba && i % 30 === 5 ? r2(41.8 * (k + 1)) : 0;
      const promo = (i + k) % 7 === 0 ? r2(principal * 0.05) : 0;
      const promoTax = promo === 0 ? 0 : ((i + k) % 17 === 0 ? null : r2(promo / 11));   // ときどき null (まだ届いていない)
      const points = (i + k) % 13 === 0 ? 30 * Math.max(units, 0) : 0;
      const refund = refundUnits ? price : 0;
      const damage = (i + k) % 37 === 0 ? 400 : 0;
      const safeT = (i + k) % 53 === 0 ? 1200 : 0;
      const misc = (i + k) % 19 === 0 ? 11 : 0;
      const other = (i + k) % 23 === 0 ? -5 : 0;
      const unitCost = cs === 'missing_cost' ? null : 300 + k * 50;
      const cogs = unitCost == null ? 0 : r2(units * unitCost);
      const easyShip = !fba && units > 0 ? r2(units * 495) : 0;
      const profit = r2(principal + shipping + gift - commission - fbaFul - fbaSto - promo - points - refund + damage + safeT - cogs);
      ins('mirror_amazon_finance_sku_daily', {
        date_jst: d, seller_sku: sku, asin_norm: asin, product_name: name,
        units_ordered: Math.max(units, 0), units_refunded_customer: refundUnits, units_marketplace_guarantee: 0, units_a_to_z_refund: 0,
        units_net_sold: units - refundUnits,
        sales_principal_jpy: principal, sales_shipping_jpy: shipping, sales_giftwrap_jpy: gift, sales_tax_jpy: tax,
        commission_jpy: commission, fba_fulfillment_jpy: fbaFul, fba_storage_jpy: fbaSto, closing_fee_jpy: 0,
        shipping_chargeback_jpy: shipping ? r2(shipping * 0.1) : 0, giftwrap_chargeback_jpy: 0,
        promotion_jpy: promo, promotion_tax_jpy: promoTax, points_jpy: points,
        warehouse_damage_jpy: damage, warehouse_lost_jpy: 0, safe_t_jpy: safeT, refund_principal_jpy: refund, reversal_reimbursement_jpy: 0,
        misc_fee_jpy: misc, other_fee_jpy: 0, other_amount_jpy: other,
        unit_cost_snapshot: unitCost, cost_snapshot_date_jst: unitCost == null ? null : d, latest_unit_cost_reference: unitCost,
        cogs_amount: cogs, profit_amount: profit, is_cost_complete: icc, cost_status: cs, easy_ship_jpy: easyShip,
        source_run_id: 'parity', source_row_hash: `h${i}${k}`, synced_at: `2026-09-${String(20 + (i % 8)).padStart(2, '0')}T22:00:00Z`,
      });
    });
  }
  // Easy Ship の割り振りだけの行 (決済の最後の日より後) = 最後の日の判定に入れない (#1520)
  ins('mirror_amazon_finance_sku_daily', { date_jst: '2026-09-28', seller_sku: 'pr_beta', easy_ship_jpy: 990, cost_status: 'missing_cost', source_run_id: 'parity', source_row_hash: 'es', synced_at: '2026-09-29T22:00:00Z' });

  // 月の手数料 (14 か月)
  let m = 0;
  for (let ym = '2025-08'; ym <= '2026-09'; m++) {
    const types = { storage: -(30000 + m * 1000), long_term_storage: m % 3 === 0 ? -5000 : 0, removal: -(m * 120), easy_ship: m >= 12 ? -(200000 + m) : 0, other_account_fee: m % 4 === 1 ? 1500 : -300 };
    for (const [t, a] of Object.entries(types)) if (a !== 0) ins('mirror_amazon_account_fees_monthly', { date_jst: `${ym}-01`, fee_type: t, amount_jpy: a, row_count: 1 + m, source_run_id: 'parity', source_row_hash: 'h', synced_at: '2026-10-01T22:00:00Z' });
    const [y, mo] = ym.split('-').map(Number);
    ym = mo === 12 ? `${y + 1}-01` : `${y}-${String(mo + 1).padStart(2, '0')}`;
  }

  // 速報 (受注ベース)・広告
  i = 0;
  for (let d = FIN_FROM; d <= FLASH_LAST; d = addDays(d, 1), i++) {
    AMZ.forEach(([sku, , name, fba, , , base, price], k) => {
      ins('mirror_f_sales_by_listing', { date_jst: d, month_ym: d.slice(0, 7), mall: 'amazon', item_code: sku, channel: fba ? 'FBA' : 'FBM', item_name: name || null, units: base, sales_jpy_incl: base * price * 1.1, order_count: base, data_source: 'sp_api', source_updated_at: 't', source_run_id: 'parity', source_row_hash: 'h', synced_at: 't' });
    });
    ins('mirror_amazon_ads_sku_daily', { date_jst: d, mall: 'amazon', campaign_id: 'C1', ad_type: 'SP', target: 'pr_alpha', target_granularity: 'sku', clicks: 20, impressions: 2000, ad_cost: 500 + (i % 5) * 10, ad_sales: 3000, ad_units: 3, source_run_id: 'p', source_row_hash: 'h', synced_at: 't' });
    ins('mirror_amazon_ads_sku_daily', { date_jst: d, mall: 'amazon', campaign_id: 'C2', ad_type: 'SP', target: 'b0gamma', target_granularity: 'asin', clicks: 30, impressions: 3000, ad_cost: 1500, ad_sales: i % 9 === 0 ? 0 : 1600, ad_units: 2, source_run_id: 'p', source_row_hash: 'h', synced_at: 't' });
    ins('mirror_amazon_ads_campaign_daily', { date_jst: d, mall: 'amazon', campaign_id: 'C1', campaign_name: 'アルファSP', ad_type: 'SP', campaign_status: 'ENABLED', clicks: 20, impressions: 2000, ad_cost: 500 + (i % 5) * 10, ad_sales_14d: 3000, ad_units_1d: 3, source_run_id: 'p', source_row_hash: 'h', synced_at: 't' });
    ins('mirror_amazon_ads_campaign_daily', { date_jst: d, mall: 'amazon', campaign_id: 'C2', campaign_name: 'ガンマSP', ad_type: 'SP', campaign_status: 'ENABLED', clicks: 30, impressions: 3000, ad_cost: 1500, ad_sales_14d: 1600, ad_units_1d: 2, source_run_id: 'p', source_row_hash: 'h', synced_at: 't' });
    ins('mirror_amazon_ads_campaign_daily', { date_jst: d, mall: 'amazon', campaign_id: 'C3', campaign_name: 'オート', ad_type: 'SP', campaign_status: 'PAUSED', clicks: 10, impressions: 5000, ad_cost: 300, ad_sales_14d: 0, ad_units_1d: 0, source_run_id: 'p', source_row_hash: 'h', synced_at: 't' });
  }

  // 手数料・SKU の対・商品マスタ・セット・在庫・価格
  const fees = [['pr_alpha', 'B0ALPHA', 'AFN', 0.15, 318, 1000], ['pr_beta', 'B0BETA', 'MFN', 0.15, null, 800], ['PR_Gamma', 'B0GAMMA', 'AFN', 0.1, 434, 2400], ['pr_delta', 'B0DELTA', 'AFN', 0.08, 290, 500]];
  for (const [sku, asin, ch, rate, fba, price] of fees) {
    ins('mirror_amazon_sku_fees', { seller_sku: sku, asin, fulfillment_channel: ch, referral_fee: r2(price * rate), referral_fee_rate: rate, fba_fee: fba, variable_closing_fee: 0, per_item_fee: 0, total_fee: r2(price * rate + (fba || 0)), price_used: price, fetched_at: '2026-10-02T21:00:00Z' });
  }
  const resolved = [['pr_alpha', 'ne-a', 1, 'master'], ['pr_beta', 'ne-b', 1, 'master'], ['PR_Gamma', 'ne-a', 2, 'master'], ['PR_Gamma', 'ne-c', 1, 'master'], ['pr_delta', 'ne-d', 1, 'auto']];
  resolved.forEach(([sku, ne, q, src], n) => ins('mirror_sku_resolved', { seller_sku: sku, ne_code: ne, quantity: q, source: src, 商品名: n === 0 ? 'アルファ (マスタ)' : null, sort_order: n, synced_at: 't' }));
  const prods = [['ne-a', '商品A', '単品', 1500, 300, 'COMPLETE', 'S01'], ['ne-b', '商品B', '単品', 1200, null, 'MISSING', 'S01'], ['ne-c', '商品C', '単品', 2000, 500, 'COMPLETE', 'S02'], ['ne-d', '商品D', '単品', 600, 200, 'OVERRIDDEN', 'S01'], ['set-x', 'セットX', 'セット', 3500, null, 'COMPLETE', 'S01']];
  for (const [code, name, kind, price, cost, cst, sup] of prods) {
    ins('mirror_products', { 商品コード: code, 商品名: name, 商品区分: kind, 取扱区分: '取扱中', 標準売価: price, 原価: cost, 原価状態: cst, 送料: 100, 消費税率: 0.1, 在庫数: 10, 引当数: 1, 仕入先コード: sup, 売上分類: 1, updated_at: '2026-10-01T00:00:00Z' });
  }
  ins('mirror_set_components', { セット商品コード: 'set-x', 構成商品コード: 'ne-a', 数量: 2, updated_at: 't' });
  ins('mirror_set_components', { セット商品コード: 'set-x', 構成商品コード: 'ne-b', 数量: 1, updated_at: 't' });
  for (const [ne, cat, qty, last] of [['ne-a', 'fba_warehouse', 40, '2026-10-01'], ['ne-a', 'own_warehouse', 12, '2026-10-01'], ['ne-b', 'fba_warehouse', 7, '2026-06-01'], ['ne-c', 'own_warehouse', 3, null]]) {
    ins('mirror_inv_daily_detail', { business_date: '2026-10-02', market: 'jp', category: cat, source_system: 'ne', source_item_code: ne, ne_code: ne, qty, unit_cost: 300, total_value: qty * 300, cost_status: 'complete', product_name: ne, last_sold_date: last, new_product_launch_date: ne === 'ne-c' ? '2026-08-01' : null, sales_30d_qty: ne === 'ne-c' ? 0 : 50, synced_at: 't' });
  }
  for (const d of ['2026-10-01', '2026-10-02']) {
    ins('mirror_amazon_price_snapshot_daily', { date_jst: d, seller_sku: 'pr_alpha', asin: 'B0ALPHA', channel: 'FBA', my_price: 1000, buybox_price: 980, buybox_is_mine: 0, fetched_at: 't', source_run_id: 'p', source_row_hash: 'h', synced_at: `${d}T21:00:00Z` });
    ins('mirror_amazon_price_snapshot_daily', { date_jst: d, seller_sku: 'PR_Gamma', asin: 'B0GAMMA', channel: 'FBA', my_price: 2400, buybox_price: 2400, buybox_is_mine: 1, fetched_at: 't', source_run_id: 'p', source_row_hash: 'h', synced_at: `${d}T21:00:00Z` });
    ins('mirror_amazon_price_snapshot_daily', { date_jst: d, seller_sku: 'pr_beta', asin: 'B0BETA', channel: 'FBM', my_price: 800, buybox_price: null, buybox_is_mine: null, fetched_at: 't', source_run_id: 'p', source_row_hash: 'h', synced_at: `${d}T21:00:00Z` });
  }
  ins('mirror_f_sales_velocity_by_product_mall', { 商品コード: 'ne-a', mall: 'amazon', qty_7d: 60, qty_30d: 260, as_of_date: '2026-10-02', synced_at: 't' });

  // ほかのモールの財務 (統合の view・supplier-sales・margin-alert・ai-insights の相手)
  i = 0;
  for (let d = '2026-07-01'; d <= '2026-09-30'; d = addDays(d, 1), i++) {
    const base = { source_run_id: 'p', source_row_hash: 'h', synced_at: 't', product_name: 'x' };
    ins('mirror_rakuten_finance_sku_daily', { ...base, date_jst: d, rakuten_code: 'rk-a', ne_code: 'ne-a', sku_resolution: 'resolved', units_net_sold: 2 + (i % 3), gross_sales_jpy_incl: 3300, variable_margin_jpy_incl: i % 5 === 0 ? -100 : 600, shipping_quality: 'actual', cost_status: i % 6 === 0 ? 'missing_cost' : 'complete' });
    ins('mirror_yahoo_finance_sku_daily', { ...base, date_jst: d, yahoo_sku_key: 'y-b', ne_code: 'ne-b', resolution_method: 'sub_match', units_net_sold: 1, gross_sales_jpy_incl: 1320, variable_margin_partial_jpy_incl: 200, variable_margin_full_jpy_incl: i % 2 ? 180 : null, shipping_quality: 'actual', cost_status: 'complete' });
    if (i % 2 === 0) ins('mirror_aupay_finance_sku_daily', { ...base, date_jst: d, aupay_sku_key: 'au-setx', ne_code: 'set-x', resolution_method: 'master_match', units_net_sold: 1, gross_sales_jpy_incl: 3850, variable_margin_partial_jpy_incl: 700, shipping_quality: 'actual', cost_status: 'complete' });
    if (i % 3 === 0) ins('mirror_qoo10_finance_sku_daily', { ...base, date_jst: d, sku_code: 'q-a', ne_code: 'ne-a', resolution_method: 'master_match', units_net_sold: 1, customer_paid_jpy_incl: 1500, net_settlement_api_jpy_incl: 1350, variable_margin_jpy_incl: 250, variable_margin_full_jpy_incl: 240, margin_confidence: i % 2 ? 'full' : 'partial_pending_settlement_csv', shipping_quality: 'actual_api', cost_status: 'complete' });
    if (i % 4 === 0) ins('mirror_linegift_finance_sku_daily', { ...base, date_jst: d, sku_code: 'lg-c', ne_code: 'ne-c', resolution_method: 'master_match', units_net_sold: 1, gross_sales_jpy_incl: 2200, variable_margin_jpy_incl: -50, shipping_quality: 'actual_api', cost_status: 'complete' });
  }

  // ai-insights の取得完了の印 (sync_run_chunks)
  for (const ent of ['f_sales_by_listing', 'amazon_finance_sku_daily', 'rakuten_finance_sku_daily', 'yahoo_finance_sku_daily', 'aupay_finance_sku_daily', 'qoo10_finance_sku_daily', 'linegift_finance_sku_daily']) {
    ins('sync_run_chunks', { run_id: `run-${ent}`, entity: ent, chunk_index: 0, chunk_count: 1, row_count: 10, payload_checksum: 'c', contract_version: 1, scope_from: '2026-07-01', scope_to: ent === 'amazon_finance_sku_daily' ? '2026-09-24' : '2026-09-30', received_at: 't', applied_at: 't' });
  }
})();

// ─────────────────────────── 呼ぶ ───────────────────────────
const out = {};
const record = (key, value) => { out[key] = value; };
function call(key, fn) {
  try { record(key, { ok: true, value: fn() }); } catch (e) { record(key, { ok: false, error: String(e?.message || e) }); }
}

const express = (await import('express')).default;
const dashQ = await imp('apps/amazon-dashboard/queries.js');
const dashRouter = (await imp('apps/amazon-dashboard/router.js')).default;
const supplierRouter = (await imp('apps/supplier-sales/router.js')).default;
const supplierAgg = await imp('apps/supplier-sales/aggregate.js');
const supplierCsv = await imp('apps/supplier-sales/csv.js');
const siteRouter = (await imp('apps/site-products/router.js')).default;
const poRouter = (await imp('apps/purchase-orders/router.js')).default;
const marginJob = await imp('apps/profit-analysis/margin-alert-job.js');
const pricingDb = await imp('apps/amazon-pricing/db.js');
const pricingRead = await imp('apps/amazon-pricing/read-model.js');
const pricingEval = await imp('apps/amazon-pricing/evaluate.js');
const aiDb = await imp('apps/ai-insights/db.js');
const aiFacts = await imp('apps/ai-insights/facts.js');
const aiMonthly = await imp('apps/ai-insights/facts-monthly.js');
const pricingRouter = (await imp('apps/amazon-pricing/router.js')).default;
const { aiInsightsApiRouter } = await imp('apps/ai-insights/router.js');

const app = express();
app.use('/amazon-dashboard', dashRouter);
app.use('/supplier-sales', supplierRouter);
app.use('/site', siteRouter);
app.use('/purchase-orders', express.json(), poRouter);
app.use('/amazon-pricing', pricingRouter);            // 本番は requireAppAccess の後ろ (session 無し = actor 'unknown')
app.use('/api/ai-insights', aiInsightsApiRouter);      // 本番と同じ口 (x-read-token)
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}`;
async function get(key, url, headers = {}, method = 'GET') {
  const res = await fetch(base + url, { headers, method, body: method === 'POST' ? '{}' : undefined });
  const buf = Buffer.from(await res.arrayBuffer());
  const ct = res.headers.get('content-type') || '';
  const text = buf.toString('utf8');
  let body = text;
  if (ct.includes('json')) { try { body = JSON.parse(text); } catch { /* 生のまま */ } }
  record(key, { status: res.status, content_type: ct, content_disposition: res.headers.get('content-disposition') || null, sha256: crypto.createHash('sha256').update(buf).digest('hex'), body });
}

try {
  // amazon-dashboard (/api/v1/*)
  const D = '/amazon-dashboard/api/v1';
  await get('dash.overview', `${D}/overview`);
  for (const [p, g] of [['30d', 'day'], ['90d', 'week'], ['12m', 'month'], ['last_month', 'day'], ['this_year', 'week']]) await get(`dash.trend.${p}.${g}`, `${D}/trend?preset=${p}&granularity=${g}`);
  await get('dash.trend.custom', `${D}/trend?preset=custom&from=2026-09-20&to=2026-10-02&granularity=day`);
  for (const p of ['7d', '30d', 'last_month', '12m']) await get(`dash.waterfall.${p}`, `${D}/waterfall?preset=${p}`);
  for (const s of ['pr_alpha', 'PR_Gamma', 'pr_beta', 'nope']) await get(`dash.waterfall.30d.${s}`, `${D}/waterfall?preset=30d&sku=${encodeURIComponent(s)}`);
  await get('dash.sku-profit.30d', `${D}/sku-profit?preset=30d`);
  await get('dash.sku-profit.90d.points', `${D}/sku-profit?preset=90d&sort=points&dir=desc`);
  await get('dash.sku-profit.12m.asc', `${D}/sku-profit?preset=12m&sort=margin_pct&dir=asc`);
  await get('dash.sku-profit.q', `${D}/sku-profit?preset=30d&q=gam`);
  await get('dash.sku-profit.page', `${D}/sku-profit?preset=30d&sort=seller_sku&dir=asc&limit=2&offset=1`);
  await get('dash.sku-profit.custom-after-settled', `${D}/sku-profit?preset=custom&from=2026-09-29&to=2026-10-02`);
  await get('dash.sku-profit.csv', `${D}/sku-profit.csv?preset=30d`);
  for (const p of ['30d', '90d', '7d']) await get(`dash.ads.${p}`, `${D}/ads?preset=${p}`);
  for (const [p, a] of [['30d', 'sales'], ['30d', 'units'], ['90d', 'profit'], ['7d', 'sales']]) await get(`dash.bestsellers.${p}.${a}`, `${D}/bestsellers?preset=${p}&axis=${a}`);
  await get('dash.diagnosis', `${D}/diagnosis`);
  await get('dash.account-fees', `${D}/account-fees`);
  await get('dash.account-fees.36', `${D}/account-fees?months=36`);
  await get('dash.account-fees.1', `${D}/account-fees?months=1`);
  call('dash.lastSettledDate', () => dashQ.lastSettledDate(db));
  call('dash.settledCompleteDate', () => dashQ.settledCompleteDate(db));
  call('dash.settledWindow', () => dashQ.settledWindow(db, '2026-09-01', '2026-10-02'));
  call('dash.getSkuProfit.direct', () => dashQ.getSkuProfit('2026-09-01', '2026-09-30', { limit: 3, sort: 'revenue_excl' }));

  // margin-alert (毎朝の GChat の通知: 集計 → 判定 → 文)
  {
    const todayJst = dashQ.jstToday();
    const to = dashQ.addDays(todayJst, -1);
    const from = dashQ.addDays(todayJst, -30);
    for (const pageSize of [10000, 2]) {
      call(`margin.collect.page${pageSize}`, () => marginJob.collectMarginRows(db, from, to, { amazonPageSize: pageSize }));
    }
    call('margin.message', () => {
      const { rows, skipped, skipReasons, amazonWindow } = marginJob.collectMarginRows(db, from, to);
      const result = marginJob.classifyMarginRows(rows, marginJob.resolveThresholdPct(), null);
      return { result, text: marginJob.formatMarginAlertMessage({ todayJst, from, to, thresholdPct: marginJob.resolveThresholdPct(), result, isFirstRun: true, skipped, skipReasons, amazonWindow }) };
    });
  }

  // amazon-pricing (価格改定の 360 行・判定)
  pricingDb.initAmazonPricing();
  call('pricing.REQUIRED_MIRROR_TABLES', () => pricingRead.REQUIRED_MIRROR_TABLES);
  call('pricing.LISTING_360_SQL', () => pricingRead.LISTING_360_SQL);
  call('pricing.view_sql', () => db.prepare(`SELECT sql FROM sqlite_master WHERE name = 'v_ap_listing_360'`).get()?.sql ?? null);
  call('pricing.view_rows', () => db.prepare(`SELECT * FROM v_ap_listing_360 ORDER BY seller_sku`).all());
  call('pricing.mirrorTablesAvailable', () => pricingRead.mirrorTablesAvailable(db));
  call('pricing.loadListings', () => pricingRead.loadListings(db));
  call('pricing.loadListing.PR_Gamma', () => pricingRead.loadListing(db, 'PR_Gamma'));
  call('pricing.inputFingerprint', () => pricingRead.inputFingerprint(db));
  call('pricing.dataFreshness', () => pricingRead.dataFreshness(db));
  call('pricing.runEvaluation', () => {
    const r = pricingEval.runEvaluation(db, { trigger: 'test', actorId: 'parity', force: true });
    return { ...r, evaluations: pricingDb.evaluationsOfRun(db, r.run.run_id) };
  });
  // HTTP の口 (Codex #1599 R1 M4)。/api/evaluations/run は書き込む (ap_eval_runs・ap_evaluations) が、一時の DATA_DIR の fixture の中だけ
  const P = '/amazon-pricing/api';
  await get('pricing.http.health', `${P}/health`);
  await get('pricing.http.listings', `${P}/listings.json`);
  await get('pricing.http.listings.q', `${P}/listings.json?q=gam`);
  await get('pricing.http.export.csv', `${P}/export.csv`);
  await get('pricing.http.evaluations.run', `${P}/evaluations/run`, { 'content-type': 'application/json', origin: base }, 'POST');   // 画面からの操作と同じ (CSRF の 2 段の守り = Origin と JSON)
  await get('pricing.http.health.after-run', `${P}/health`);

  // supplier-sales (社内の口。公開の口 public-router.js も同じ getSupplierReport / getSupplierDailyDetail を使う)
  const S = '/supplier-sales/api';
  await get('supplier.health', `${S}/health`);
  for (const code of ['S01', 'S02']) {
    for (const p of ['7d', '30d', '90d']) await get(`supplier.summary.${code}.${p}`, `${S}/summary?code=${code}&period=${p}`);
  }
  await get('supplier.summary.S01.custom', `${S}/summary?code=S01&period=custom&start=2025-10-01&end=2026-09-30`);
  await get('supplier.summary.csv', `${S}/summary.csv?code=S01&period=30d`);
  await get('supplier.daily.csv', `${S}/daily.csv?code=S01&period=30d`);
  call('supplier.confirmedCutoff', () => supplierAgg.confirmedCutoff(db));
  call('supplier.getSupplierDailyDetail', () => supplierAgg.getSupplierDailyDetail(db, 'S01', { period: '7d' }));
  call('supplier.buildDailyCsv', () => supplierCsv.buildDailyCsv(supplierAgg.getSupplierDailyDetail(db, 'S02', { period: '90d' }), 'S02'));

  // purchase-orders (商品ごとのモール別の販売数・Amazon は統合の view の FBA/FBM)
  for (const [code, days] of [['ne-a', 30], ['ne-a', 90], ['ne-a', 365], ['ne-b', 180], ['ne-c', 90]]) {
    await get(`po.mall-sales.${code}.${days}`, `/purchase-orders/api/products/${code}/mall-sales?days=${days}`);
  }

  // site-products (asinFinance を含む lookup)
  await get('site.products', '/site/products', { 'x-read-token': 'parity-read-token' });

  // 統合の view (warehouse-mirror/db.js が作る・読み手 = purchase-orders・ai-insights)
  call('unified.view_sql', () => db.prepare(`SELECT sql FROM sqlite_master WHERE name = 'v_mall_finance_daily_unified'`).get()?.sql ?? null);
  call('unified.amazon_rows', () => db.prepare(`SELECT * FROM v_mall_finance_daily_unified WHERE mall = 'amazon' AND date_jst >= '2026-09-01' ORDER BY date_jst, sku_key`).all());
  call('unified.by_mall', () => db.prepare(`SELECT mall, COUNT(*) n, SUM(units_net_sold) u, SUM(sales_gross_jpy_incl) s, SUM(margin_jpy_for_reporting) m, SUM(is_fba) f FROM v_mall_finance_daily_unified GROUP BY mall ORDER BY mall`).all());

  // ai-insights (週次 / 月次の AI レポートの入力)
  aiDb.initAiInsightsTables(db);
  call('ai.weekly', () => aiFacts.buildWeeklyReportInput(db, { periodStart: '2026-09-21' }));
  call('ai.monthly', () => aiMonthly.buildMonthlyReportInput(db, { month: '2026-08' }));
  // HTTP の口 (Codex #1599 R1 M4・PC の runner が取る /api/ai-insights/report-input)
  const AI = { 'x-read-token': 'parity-ai-read-token' };
  await get('ai.http.report-input.weekly', '/api/ai-insights/report-input?type=weekly&period_start=2026-09-21', AI);
  await get('ai.http.report-input.weekly.default', '/api/ai-insights/report-input?type=weekly', AI);
  await get('ai.http.report-input.monthly', '/api/ai-insights/report-input?type=monthly&month=2026-08', AI);
  await get('ai.http.report-input.monthly.default', '/api/ai-insights/report-input?type=monthly', AI);
} finally {
  await new Promise((resolve) => server.close(resolve));
}

// ─────────────────────────── 比べる ───────────────────────────
// 関数の戻り値は JSON の文字の sha256 も持たせる (HTTP の口は生の bytes の sha256)
for (const v of Object.values(out)) {
  if (!('sha256' in v)) v.sha256 = crypto.createHash('sha256').update(JSON.stringify(v.ok ? v.value : v.error) ?? 'undefined').digest('hex');
}
const serialized = JSON.stringify(out, null, 1) + '\n';
db.close();

if (WRITE) {
  fs.mkdirSync(path.dirname(GOLDEN), { recursive: true });
  fs.writeFileSync(GOLDEN, serialized);
  console.log(`golden を書いた: ${path.relative(REPO, GOLDEN)} (${Object.keys(out).length} 口・${serialized.length} 文字・呼んだコード = ${CODE})`);
} else {
  const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
  let fail = 0;
  const keys = new Set([...Object.keys(golden), ...Object.keys(out)]);
  for (const k of keys) {
    const a = golden[k], b = out[k];
    if (!a || !b) { fail++; console.log(`  ❌ ${k}: ${!a ? 'golden に無い' : '今回の応答が無い'}`); continue; }
    if (a.sha256 !== b.sha256 || JSON.stringify(a) !== JSON.stringify(b)) { fail++; console.log(`  ❌ ${k}: 応答が変わった`); continue; }
    if (a.ok === false || (a.status && a.status !== 200)) console.log(`  ⚠️ ${k}: 前も後も失敗 / 200 以外 (同じなので一致として数える) ${a.error || a.status}`);
  }
  const errs = Object.entries(out).filter(([, v]) => v.ok === false || (v.status && v.status !== 200)).map(([k]) => k);
  if (fail) {
    const actual = path.join(os.tmpdir(), `amazon-finance-read-parity.actual.${process.pid}.json`);
    fs.writeFileSync(actual, serialized);
    console.log(`\n❌ ${fail} / ${keys.size} 口で応答が golden と違う (今回の値 = ${actual})`);
    process.exitCode = 1;
  } else {
    console.log(`✅ ${keys.size} 口の応答が golden と 1 バイトも違わない (golden = 読み口を入れる前のコード・呼んだコード = ${CODE})${errs.length ? `・前も後も失敗 / 200 以外 = ${errs.join(', ')}` : ''}`);
  }
}
