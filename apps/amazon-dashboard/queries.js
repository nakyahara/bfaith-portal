/**
 * amazon-dashboard queries.js — データ層 (Render warehouse-mirror.db 読み取り + 自前設定表)
 *
 * 使用テーブル (mirror_* = miniPC 同期、amzdash_* = 本アプリ専用):
 *   mirror_amazon_finance_sku_daily   確定利益 (settlement 起点、税抜、日次×SKU)
 *   mirror_f_sales_by_listing         速報売上 (受注ベース、税込、mall='amazon')
 *   mirror_amazon_ads_sku_daily       広告費 SKU/ASIN 別 (PR-A で同期)
 *   mirror_amazon_ads_campaign_daily  広告費 キャンペーン単位 全額 (PR-A で同期)
 *   mirror_amazon_sku_fees            手数料参考値 (TTL 7日)
 *   mirror_sku_resolved               seller_sku → ne_code (セット構成込み)
 *   mirror_products                   商品マスタ (原価/標準売価)
 *   mirror_inv_daily_detail           在庫詳細 (business_date 別)
 *   amzdash_custom_expenses           カスタム経費 (本アプリ管理)
 *   amzdash_settings                  診断閾値等 (本アプリ管理)
 *
 * 精度ラベル方針 (要件定義 §9-2):
 *   確定 = settlement 由来 / 速報 = 受注ベース / 推定 = 速報売上−手数料参考値−原価−広告費
 *   の 3 種を API レスポンスの precision フィールドで常に区別する。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

// 消費税率 (速報→税抜の概算換算用。軽減税率 SKU は過大控除になるが「推定」ラベル前提)
const TAX_RATE = 1.1;
// settlement 確定待ちとみなす日数 (直近 N 日は網掛け表示)
export const SETTLEMENT_PENDING_DAYS = 14;

let _initialized = false;
export function ensureAppTables() {
  if (_initialized) return;
  const db = getMirrorDB();
  db.exec(`CREATE TABLE IF NOT EXISTS amzdash_custom_expenses (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL,
    expense_type  TEXT NOT NULL CHECK (expense_type IN ('fixed_monthly','oneoff','sales_pct')),
    amount_jpy    REAL NOT NULL DEFAULT 0,   -- sales_pct の場合は % 値 (例 1.5)
    month_from    TEXT NOT NULL,             -- YYYY-MM (oneoff は当月のみ)
    month_to      TEXT,                      -- NULL = 継続中 (fixed_monthly/sales_pct)
    memo          TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    deleted_at    TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS amzdash_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`);
  _initialized = true;
}

// ─── 診断閾値 (設定タブで変更可、デフォルトは要件定義 F-6) ───
export const DEFAULT_SETTINGS = {
  acos_safe_ratio: 0.8,       // 実ACOS < 損益分岐×0.8 = 🟢
  loss_months: 2,             // 赤字SKU: 広告後利益<0 が N ヶ月連続
  margin_drop_pt: 3,          // 悪化SKU: 利益率 前月比 -Npt 超
  ad_bleed_days: 30,          // 広告垂れ流し: 実ACOS>損益分岐 が N 日継続
  dead_stock_days: 60,        // 死に筋: 販売0 が N 日 + 在庫あり
  price_miss_ratio: 0.9,      // 価格ミス疑い: 実売単価(税込) < 標準売価×N
  abc_a_pct: 80,              // ABC: A = 売上累積 N% まで
  abc_b_pct: 95,
  new_product_days: 60,       // 新商品バッジ: 初売上から N 日
};

export function getSettings() {
  ensureAppTables();
  const db = getMirrorDB();
  const rows = db.prepare(`SELECT key, value FROM amzdash_settings`).all();
  const out = { ...DEFAULT_SETTINGS };
  for (const r of rows) {
    if (r.key in out) {
      const n = Number(r.value);
      if (Number.isFinite(n)) out[r.key] = n;
    }
  }
  return out;
}

export function saveSettings(patch) {
  ensureAppTables();
  const db = getMirrorDB();
  const now = new Date().toISOString();
  const stmt = db.prepare(`
    INSERT INTO amzdash_settings (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
  `);
  const tx = db.transaction(() => {
    for (const [k, v] of Object.entries(patch)) {
      if (!(k in DEFAULT_SETTINGS)) continue;
      const n = Number(v);
      if (!Number.isFinite(n)) continue;
      stmt.run(k, String(n), now);
    }
  });
  tx();
  return getSettings();
}

// ─── 日付 helper (JST。Date.toISOString の UTC 罠に注意: feedback_jst_to_iso_string_trap) ───
export function jstToday() {
  return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
export function addDays(dateStr, n) {
  const d = new Date(dateStr + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
export function monthOf(dateStr) { return dateStr.slice(0, 7); }
export function monthStart(ym) { return `${ym}-01`; }
export function monthEnd(ym) {
  const [y, m] = ym.split('-').map(Number);
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return `${ym}-${String(lastDay).padStart(2, '0')}`;
}
export function addMonths(ym, n) {
  const [y, m] = ym.split('-').map(Number);
  const d = new Date(Date.UTC(y, m - 1 + n, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
export function isValidDate(s) {
  if (typeof s !== 'string' || !DATE_RE.test(s)) return false;
  const d = new Date(s + 'T00:00:00Z');
  return !isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// preset → {from, to} (JST)
export function resolvePeriod(preset, fromQ, toQ) {
  const today = jstToday();
  if (isValidDate(fromQ) && isValidDate(toQ) && fromQ <= toQ) return { from: fromQ, to: toQ, preset: 'custom' };
  const ym = monthOf(today);
  const map = {
    today: { from: today, to: today },
    yesterday: { from: addDays(today, -1), to: addDays(today, -1) },
    '7d': { from: addDays(today, -6), to: today },
    '14d': { from: addDays(today, -13), to: today },
    '30d': { from: addDays(today, -29), to: today },
    '90d': { from: addDays(today, -89), to: today },
    this_month: { from: monthStart(ym), to: today },
    last_month: { from: monthStart(addMonths(ym, -1)), to: monthEnd(addMonths(ym, -1)) },
    this_year: { from: `${today.slice(0, 4)}-01-01`, to: today },
    '12m': { from: monthStart(addMonths(ym, -11)), to: today },
  };
  const r = map[preset] || map['30d'];
  return { ...r, preset: map[preset] ? preset : '30d' };
}

// ─── 速報売上 (mirror_f_sales_by_listing、税込) ───
function flashSales(db, from, to) {
  return db.prepare(`
    SELECT COALESCE(SUM(sales_jpy_incl),0) AS sales_incl,
           COALESCE(SUM(units),0) AS units,
           COALESCE(SUM(order_count),0) AS orders
    FROM mirror_f_sales_by_listing
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 広告費 (campaign 全額 = 正本) ───
function adCost(db, from, to) {
  return db.prepare(`
    SELECT COALESCE(SUM(ad_cost),0) AS ad_cost,
           COALESCE(SUM(ad_sales_14d),0) AS ad_sales_14d,
           COALESCE(SUM(clicks),0) AS clicks,
           COALESCE(SUM(impressions),0) AS impressions
    FROM mirror_amazon_ads_campaign_daily
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 確定 (settlement fact、税抜) ───
function settledSummary(db, from, to) {
  return db.prepare(`
    SELECT
      COALESCE(SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy),0) AS revenue_excl,
      COALESCE(SUM(units_net_sold),0) AS units_net,
      COALESCE(SUM(profit_amount),0) AS profit_before_ads,
      COALESCE(SUM(refund_principal_jpy),0) AS refunds,
      COALESCE(SUM(warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy),0) AS reimbursements,
      COALESCE(SUM(cogs_amount),0) AS cogs,
      COUNT(DISTINCT date_jst) AS days_with_data
    FROM mirror_amazon_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
  `).get(from, to);
}

// ─── 推定利益 (速報売上 − 手数料参考値 − 最新原価 − 広告費)。カバー率併記で正直に ───
// 原価: mirror_sku_resolved (セット構成) × mirror_products.原価 (最新原価 = 参考値)
// 手数料: mirror_amazon_sku_fees.total_fee (TTL 7日キャッシュ、税込想定 → /1.1)
function estimatedProfit(db, from, to) {
  const row = db.prepare(`
    WITH sku_cost AS (
      SELECT LOWER(r.seller_sku) AS sku,
             SUM(r.quantity * p.原価) AS unit_cost,
             MIN(CASE WHEN p.原価 IS NULL THEN 0 ELSE 1 END) AS cost_ok
      FROM mirror_sku_resolved r
      LEFT JOIN mirror_products p ON p.商品コード = r.ne_code
      GROUP BY LOWER(r.seller_sku)
    )
    SELECT
      COALESCE(SUM(f.sales_jpy_incl),0) AS sales_incl_total,
      COALESCE(SUM(CASE WHEN fee.total_fee IS NOT NULL AND c.cost_ok = 1 THEN f.sales_jpy_incl END),0) AS sales_incl_covered,
      COALESCE(SUM(CASE WHEN fee.total_fee IS NOT NULL AND c.cost_ok = 1
        THEN f.sales_jpy_incl / ${TAX_RATE} - f.units * fee.total_fee / ${TAX_RATE} - f.units * c.unit_cost
      END),0) AS est_profit_before_ads
    FROM mirror_f_sales_by_listing f
    LEFT JOIN mirror_amazon_sku_fees fee ON LOWER(fee.seller_sku) = LOWER(f.item_code)
    LEFT JOIN sku_cost c ON c.sku = LOWER(f.item_code)
    WHERE f.mall = 'amazon' AND f.date_jst >= ? AND f.date_jst <= ?
  `).get(from, to);
  const coverage = row.sales_incl_total > 0 ? row.sales_incl_covered / row.sales_incl_total : 0;
  return { est_profit_before_ads: row.est_profit_before_ads, coverage_pct: Math.round(coverage * 1000) / 10 };
}

// ─── カスタム経費 (月次): 対象月に効く経費合計。sales_pct は確定売上(税抜)基準 ───
function customExpensesForMonth(db, ym, settledRevenueExcl) {
  ensureAppTables();
  const rows = db.prepare(`
    SELECT * FROM amzdash_custom_expenses
    WHERE deleted_at IS NULL
      AND month_from <= ?
      AND (
        (expense_type = 'oneoff' AND month_from = ?)
        OR (expense_type IN ('fixed_monthly','sales_pct') AND (month_to IS NULL OR month_to >= ?))
      )
  `).all(ym, ym, ym);
  let total = 0;
  const items = [];
  for (const r of rows) {
    let amount = 0;
    if (r.expense_type === 'sales_pct') amount = settledRevenueExcl * (r.amount_jpy / 100);
    else amount = r.amount_jpy;
    total += amount;
    items.push({ id: r.id, name: r.name, expense_type: r.expense_type, amount_jpy: Math.round(amount) });
  }
  return { total: Math.round(total), items };
}

// ─── 概要タブ: タイル 4 枚 ───
export function getOverview() {
  const db = getMirrorDB();
  const today = jstToday();
  const ym = monthOf(today);
  const lastYm = addMonths(ym, -1);
  const periods = [
    { key: 'today', label: '今日', from: today, to: today },
    { key: 'yesterday', label: '昨日', from: addDays(today, -1), to: addDays(today, -1) },
    { key: 'this_month', label: '今月', from: monthStart(ym), to: today },
    { key: 'last_month', label: '先月', from: monthStart(lastYm), to: monthEnd(lastYm) },
  ];
  const tiles = periods.map(p => {
    const flash = flashSales(db, p.from, p.to);
    const ads = adCost(db, p.from, p.to);
    const settled = settledSummary(db, p.from, p.to);
    const est = estimatedProfit(db, p.from, p.to);
    const daysInPeriod = Math.round((new Date(p.to + 'T00:00:00Z') - new Date(p.from + 'T00:00:00Z')) / 86400000) + 1;
    const tile = {
      ...p,
      flash_sales_incl: Math.round(flash.sales_incl),
      flash_units: flash.units,
      flash_orders: flash.orders,
      ad_cost: Math.round(ads.ad_cost),
      est_profit: Math.round(est.est_profit_before_ads - ads.ad_cost),
      est_coverage_pct: est.coverage_pct,
      settled_revenue_excl: Math.round(settled.revenue_excl),
      settled_profit_before_ads: Math.round(settled.profit_before_ads),
      settled_profit_after_ads: Math.round(settled.profit_before_ads - ads.ad_cost),
      settled_refunds: Math.round(settled.refunds),
      settled_days: settled.days_with_data,
      days_in_period: daysInPeriod,
      settled_coverage_pct: Math.round((settled.days_with_data / daysInPeriod) * 1000) / 10,
    };
    // 月タイルのみカスタム経費を載せる
    if (p.key === 'this_month' || p.key === 'last_month') {
      const targetYm = p.key === 'this_month' ? ym : lastYm;
      const exp = customExpensesForMonth(db, targetYm, settled.revenue_excl);
      tile.custom_expenses = exp.total;
      tile.settled_profit_final = tile.settled_profit_after_ads - exp.total;
    }
    return tile;
  });
  return { generated_at: new Date().toISOString(), today, tiles, pending_days: SETTLEMENT_PENDING_DAYS };
}

// ─── トレンド (日次/週次/月次) ───
export function getTrend(from, to, granularity) {
  const db = getMirrorDB();
  const bucketExpr = granularity === 'month' ? `substr(date_jst, 1, 7)`
    : granularity === 'week' ? `strftime('%Y-W%W', date_jst)`
    : `date_jst`;
  const flash = db.prepare(`
    SELECT ${bucketExpr.replace(/date_jst/g, 'date_jst')} AS bucket,
           MIN(date_jst) AS bucket_start,
           SUM(sales_jpy_incl) AS flash_sales_incl, SUM(units) AS flash_units
    FROM mirror_f_sales_by_listing
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to);
  const settled = db.prepare(`
    SELECT ${bucketExpr} AS bucket,
           SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS revenue_excl,
           SUM(profit_amount) AS profit_before_ads,
           SUM(refund_principal_jpy) AS refunds
    FROM mirror_amazon_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to);
  const ads = db.prepare(`
    SELECT ${bucketExpr} AS bucket, SUM(ad_cost) AS ad_cost
    FROM mirror_amazon_ads_campaign_daily
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
    GROUP BY bucket ORDER BY bucket
  `).all(from, to);

  const map = new Map();
  const ensure = (b, start) => {
    if (!map.has(b)) map.set(b, { bucket: b, bucket_start: start || b, flash_sales_incl: 0, flash_units: 0, revenue_excl: 0, profit_before_ads: 0, refunds: 0, ad_cost: 0 });
    return map.get(b);
  };
  for (const r of flash) Object.assign(ensure(r.bucket, r.bucket_start), { flash_sales_incl: r.flash_sales_incl || 0, flash_units: r.flash_units || 0 });
  for (const r of settled) Object.assign(ensure(r.bucket), { revenue_excl: r.revenue_excl || 0, profit_before_ads: r.profit_before_ads || 0, refunds: r.refunds || 0 });
  for (const r of ads) Object.assign(ensure(r.bucket), { ad_cost: r.ad_cost || 0 });
  const rows = [...map.values()].sort((a, b) => a.bucket < b.bucket ? -1 : 1).map(r => ({
    ...r,
    profit_after_ads: r.profit_before_ads - r.ad_cost,
    margin_pct: r.revenue_excl > 0 ? Math.round((r.profit_before_ads - r.ad_cost) / r.revenue_excl * 1000) / 10 : null,
    tacos_pct: r.revenue_excl > 0 ? Math.round(r.ad_cost / r.revenue_excl * 1000) / 10 : null,
  }));
  const pendingFrom = addDays(jstToday(), -(SETTLEMENT_PENDING_DAYS - 1));
  return { from, to, granularity, rows, settlement_pending_from: pendingFrom };
}

// ─── 広告費の SKU 配賦 (共通ロジック) ───
// 1. direct: mirror_amazon_ads_sku_daily の target を SKU/ASIN 両対応で SKU に貼る
//    (asin 粒度の行は同一 ASIN を持つ SKU 群に広告経由売上→無ければ確定売上比で按分)
// 2. unallocated = campaign 合計 − direct 合計 → 各 SKU の広告経由売上比 (fallback 確定売上比) で按分
// 戻り値: Map<seller_sku, {direct, allocated, ad_sales}>
function allocateAdCost(db, from, to, skuRows) {
  const adRows = db.prepare(`
    SELECT LOWER(target) AS target, target_granularity, SUM(ad_cost) AS cost, SUM(ad_sales) AS sales
    FROM mirror_amazon_ads_sku_daily
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
    GROUP BY LOWER(target), target_granularity
  `).all(from, to);
  const campaignTotal = adCost(db, from, to).ad_cost;

  const bySku = new Map();       // LOWER(seller_sku) → skuRow
  const byAsin = new Map();      // LOWER(asin) → [skuRow]
  for (const r of skuRows) {
    const skuKey = r.seller_sku.toLowerCase();
    bySku.set(skuKey, r);
    const asinKey = (r.asin_norm || '').toLowerCase();
    if (asinKey) {
      if (!byAsin.has(asinKey)) byAsin.set(asinKey, []);
      byAsin.get(asinKey).push(r);
    }
  }
  const result = new Map();      // seller_sku → {direct, allocated, ad_sales}
  const ensure = (sku) => {
    if (!result.has(sku)) result.set(sku, { direct: 0, allocated: 0, ad_sales: 0 });
    return result.get(sku);
  };

  // matchedDirect = 現 SKU 群に直接貼れた広告費のみ。
  // 未マッチ direct (販売0 SKU の広告等) も Auto-Targeting も campaignTotal に含まれるため、
  // unallocated = campaignTotal − matchedDirect の一本で二重計上しない (Codex R1 Medium #1)。
  let matchedDirect = 0;
  for (const a of adRows) {
    if (a.target_granularity === 'sku' && bySku.has(a.target)) {
      const e = ensure(bySku.get(a.target).seller_sku);
      e.direct += a.cost;
      e.ad_sales += a.sales;
      matchedDirect += a.cost;
    } else if (a.target_granularity === 'asin' && byAsin.has(a.target)) {
      // ASIN 行 → 同 ASIN の SKU 群に確定売上比で按分 (行単位の広告経由売上内訳は持っていない)
      const group = byAsin.get(a.target);
      const totalRev = group.reduce((s, r) => s + (r.revenue_excl || 0), 0);
      for (const r of group) {
        const share = totalRev > 0 ? (r.revenue_excl || 0) / totalRev : 1 / group.length;
        const e = ensure(r.seller_sku);
        e.direct += a.cost * share;
        e.ad_sales += a.sales * share;
      }
      matchedDirect += a.cost;
    }
    // 未マッチ direct は unallocated 側に自然に残る (campaignTotal に含まれるため)
  }
  // 未配賦 = campaign 正本 − SKU に貼れた分。配賦総額が campaign 正本を超えない invariant
  const unallocated = Math.max(0, campaignTotal - matchedDirect);
  const directTotal = matchedDirect;
  // 按分基準: 広告経由売上比 → 全ゼロなら確定売上比
  let basisTotal = 0;
  for (const [, v] of result) basisTotal += v.ad_sales;
  const useRevenueBasis = basisTotal <= 0;
  if (useRevenueBasis) {
    basisTotal = skuRows.reduce((s, r) => s + Math.max(0, r.revenue_excl || 0), 0);
  }
  if (basisTotal > 0) {
    for (const r of skuRows) {
      const e = ensure(r.seller_sku);
      const basis = useRevenueBasis ? Math.max(0, r.revenue_excl || 0) : e.ad_sales;
      e.allocated = unallocated * (basis / basisTotal);
    }
  }
  return { alloc: result, campaignTotal, directTotal, unallocated: Math.round(unallocated) };
}

// ─── 商品名フォールバック解決 ───
// settlement fact の product_name は空の SKU が多い (2026-07-06 実データで確認) ため、
// SKUマスタ (mirror_sku_resolved.商品名 / mirror_products.商品名) → 速報 listing 名 の順で補完。
// マップ構築は速報全期間 GROUP BY を含むため 10 分キャッシュ。
// キャッシュは DB 接続単位 (WeakMap)。initMirrorDB() 再実行で接続が差し替わっても
// 旧 DB 由来の名前を返さない (Codex 指摘)。TTL 10 分。
const _nameCacheByDb = new WeakMap();
function getNameMap(db) {
  const now = Date.now();
  const cached = _nameCacheByDb.get(db);
  if (cached && now - cached.at < 10 * 60 * 1000) return cached.map;
  const map = new Map();
  // 1) 速報 listing 名 (優先度低: 先に入れて後からマスタ名で上書き)。
  //    全期間 GROUP BY は重いため直近 180 日に限定 (idx_mfsbl_mall (mall, date_jst) が効く)。
  //    それ以前にしか売れていない SKU はマスタ名 (2) 側で概ね拾える。
  const flashFrom = addDays(jstToday(), -180);
  for (const x of db.prepare(`
    SELECT LOWER(item_code) AS sku, MAX(NULLIF(TRIM(item_name), '')) AS name
    FROM mirror_f_sales_by_listing
    WHERE mall = 'amazon' AND date_jst >= ?
    GROUP BY LOWER(item_code)
  `).all(flashFrom)) {
    if (x.name) map.set(x.sku, x.name);
  }
  // 2) SKUマスタ経由 (mirror_sku_resolved → mirror_products)
  for (const x of db.prepare(`
    SELECT LOWER(r.seller_sku) AS sku,
           MAX(COALESCE(NULLIF(TRIM(r.商品名), ''), NULLIF(TRIM(p.商品名), ''))) AS name
    FROM mirror_sku_resolved r
    LEFT JOIN mirror_products p ON p.商品コード = r.ne_code
    GROUP BY LOWER(r.seller_sku)
  `).all()) {
    if (x.name) map.set(x.sku, x.name);
  }
  _nameCacheByDb.set(db, { at: now, map });
  return map;
}
function attachProductNames(db, rows, skuField = 'seller_sku') {
  if (!rows.some(r => !r.product_name || !String(r.product_name).trim())) return rows;
  const map = getNameMap(db);
  for (const r of rows) {
    if (!r.product_name || !String(r.product_name).trim()) {
      r.product_name = map.get(String(r[skuField] || '').toLowerCase()) || '';
    }
  }
  return rows;
}

// ─── SKU 別 確定集計 (利益分析/広告/売れ筋/診断の共通ベース) ───
function settledBySku(db, from, to) {
  return attachProductNames(db, db.prepare(`
    SELECT seller_sku, MAX(asin_norm) AS asin_norm, MAX(product_name) AS product_name,
      SUM(units_net_sold) AS units_net,
      SUM(units_ordered) AS units_ordered,
      SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS revenue_excl,
      SUM(sales_principal_jpy) AS principal_excl,
      SUM(commission_jpy + fba_fulfillment_jpy + fba_storage_jpy + closing_fee_jpy
          + shipping_chargeback_jpy + giftwrap_chargeback_jpy + misc_fee_jpy + other_fee_jpy) AS fees,
      SUM(promotion_jpy) AS promotion,
      SUM(refund_principal_jpy) AS refunds,
      SUM(units_refunded_customer + units_marketplace_guarantee + units_a_to_z_refund) AS units_refunded,
      SUM(warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy) AS reimbursements,
      SUM(cogs_amount) AS cogs,
      SUM(profit_amount) AS profit_before_ads,
      MAX(cost_status) AS cost_status_sample,
      MIN(is_cost_complete) AS all_cost_complete
    FROM mirror_amazon_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ?
    GROUP BY seller_sku
  `).all(from, to));
}

// ─── 利益分析タブ: ウォーターフォール ───
export function getWaterfall(from, to, sku) {
  const db = getMirrorDB();
  const skuCond = sku ? `AND seller_sku = ?` : '';
  const params = sku ? [from, to, sku] : [from, to];
  const s = db.prepare(`
    SELECT
      COALESCE(SUM(sales_principal_jpy),0) AS principal,
      COALESCE(SUM(sales_shipping_jpy),0) AS shipping,
      COALESCE(SUM(sales_giftwrap_jpy),0) AS giftwrap,
      COALESCE(SUM(promotion_jpy),0) AS promotion,
      COALESCE(SUM(refund_principal_jpy),0) AS refunds,
      COALESCE(SUM(commission_jpy),0) AS commission,
      COALESCE(SUM(fba_fulfillment_jpy),0) AS fba_fulfillment,
      COALESCE(SUM(fba_storage_jpy),0) AS fba_storage,
      COALESCE(SUM(closing_fee_jpy),0) AS closing_fee,
      COALESCE(SUM(shipping_chargeback_jpy + giftwrap_chargeback_jpy),0) AS chargebacks,
      COALESCE(SUM(misc_fee_jpy + other_fee_jpy + other_amount_jpy),0) AS other_fees,
      COALESCE(SUM(warehouse_damage_jpy),0) AS reimb_damage,
      COALESCE(SUM(warehouse_lost_jpy),0) AS reimb_lost,
      COALESCE(SUM(safe_t_jpy),0) AS reimb_safe_t,
      COALESCE(SUM(reversal_reimbursement_jpy),0) AS reimb_reversal,
      COALESCE(SUM(cogs_amount),0) AS cogs,
      COALESCE(SUM(profit_amount),0) AS profit_before_ads
    FROM mirror_amazon_finance_sku_daily
    WHERE date_jst >= ? AND date_jst <= ? ${skuCond}
  `).get(...params);

  // 広告費: 全体 = campaign 正本 / SKU 指定 = direct + 按分
  let adCostValue;
  if (sku) {
    const skuRows = settledBySku(db, from, to);
    const { alloc } = allocateAdCost(db, from, to, skuRows);
    const a = alloc.get(sku) || { direct: 0, allocated: 0 };
    adCostValue = a.direct + a.allocated;
  } else {
    adCostValue = adCost(db, from, to).ad_cost;
  }

  const revenue = s.principal + s.shipping + s.giftwrap;
  const reimbTotal = s.reimb_damage + s.reimb_lost + s.reimb_safe_t + s.reimb_reversal;
  const steps = [
    { key: 'revenue', label: '総売上 (税抜)', amount: revenue, kind: 'total' },
    { key: 'promotion', label: 'プロモーション', amount: s.promotion, kind: 'cost' },
    { key: 'refunds', label: '返金', amount: s.refunds, kind: 'cost' },
    { key: 'commission', label: '販売手数料', amount: s.commission, kind: 'cost' },
    { key: 'fba_fulfillment', label: 'FBA配送代行', amount: s.fba_fulfillment, kind: 'cost' },
    { key: 'fba_storage', label: '在庫保管料', amount: s.fba_storage, kind: 'cost' },
    { key: 'closing_fee', label: 'カテゴリー成約料', amount: s.closing_fee, kind: 'cost' },
    { key: 'chargebacks', label: 'チャージバック', amount: s.chargebacks, kind: 'cost' },
    { key: 'other_fees', label: 'その他フィー', amount: s.other_fees, kind: 'cost' },
    { key: 'reimbursements', label: '補填 (damage/lost/SAFE-T)', amount: reimbTotal, kind: 'income' },
    { key: 'cogs', label: '原価 (snapshot)', amount: s.cogs, kind: 'cost' },
    { key: 'profit_before_ads', label: '補填込み粗利 (広告前)', amount: s.profit_before_ads, kind: 'subtotal' },
    { key: 'ad_cost', label: '広告費', amount: adCostValue, kind: 'cost', precision: sku ? 'allocated' : 'actual' },
    { key: 'profit_after_ads', label: '広告後利益', amount: s.profit_before_ads - adCostValue, kind: 'total' },
  ];
  return { from, to, sku: sku || null, steps: steps.map(x => ({ ...x, amount: Math.round(x.amount) })) };
}

// ─── 利益分析タブ: SKU テーブル ───
export function getSkuProfit(from, to, opts = {}) {
  const db = getMirrorDB();
  const skuRows = settledBySku(db, from, to);
  const { alloc, campaignTotal, unallocated } = allocateAdCost(db, from, to, skuRows);

  let rows = skuRows.map(r => {
    const a = alloc.get(r.seller_sku) || { direct: 0, allocated: 0, ad_sales: 0 };
    const adTotal = a.direct + a.allocated;
    const profitAfter = r.profit_before_ads - adTotal;
    return {
      seller_sku: r.seller_sku,
      asin: r.asin_norm,
      product_name: r.product_name,
      units_net: r.units_net,
      revenue_excl: Math.round(r.revenue_excl),
      fees: Math.round(r.fees),
      promotion: Math.round(r.promotion),
      refunds: Math.round(r.refunds),
      reimbursements: Math.round(r.reimbursements),
      cogs: Math.round(r.cogs),
      profit_before_ads: Math.round(r.profit_before_ads),
      ad_direct: Math.round(a.direct),
      ad_allocated: Math.round(a.allocated),
      ad_sales: Math.round(a.ad_sales),
      profit_after_ads: Math.round(profitAfter),
      margin_pct: r.revenue_excl > 0 ? Math.round(profitAfter / r.revenue_excl * 1000) / 10 : null,
      cost_status: r.all_cost_complete === 1 ? 'complete' : r.cost_status_sample,
      // 色分け用: gross 黒字なのに広告で赤字 = 'ad_bleed'、両方赤 = 'loss'
      flag: profitAfter < 0 ? (r.profit_before_ads >= 0 ? 'ad_bleed' : 'loss') : 'ok',
    };
  });

  const q = (opts.q || '').trim().toLowerCase();
  if (q) rows = rows.filter(r => r.seller_sku.toLowerCase().includes(q) || (r.product_name || '').toLowerCase().includes(q) || (r.asin || '').toLowerCase().includes(q));

  const sortKey = ['revenue_excl', 'units_net', 'profit_before_ads', 'profit_after_ads', 'margin_pct', 'ad_direct', 'refunds', 'seller_sku'].includes(opts.sort) ? opts.sort : 'profit_after_ads';
  const dir = opts.dir === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    const av = a[sortKey], bv = b[sortKey];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    return av < bv ? -dir : av > bv ? dir : 0;
  });

  const total = rows.length;
  const limit = Math.min(Number(opts.limit) || 100, 20000);
  const offset = Math.max(Number(opts.offset) || 0, 0);
  return {
    from, to, total,
    ad_campaign_total: Math.round(campaignTotal),
    ad_unallocated: unallocated,
    rows: rows.slice(offset, offset + limit),
  };
}

// ─── 広告タブ ───
export function getAdsAnalysis(from, to) {
  const db = getMirrorDB();
  const settings = getSettings();
  const skuRows = settledBySku(db, from, to);
  const { alloc, campaignTotal, directTotal, unallocated } = allocateAdCost(db, from, to, skuRows);

  // fee 参考値 + 原価 (損益分岐 ACOS 用)
  const feeMap = new Map(db.prepare(`SELECT LOWER(seller_sku) AS sku, total_fee, price_used FROM mirror_amazon_sku_fees`).all().map(r => [r.sku, r]));
  const costRows = db.prepare(`
    SELECT LOWER(r.seller_sku) AS sku, SUM(r.quantity * p.原価) AS unit_cost
    FROM mirror_sku_resolved r LEFT JOIN mirror_products p ON p.商品コード = r.ne_code
    GROUP BY LOWER(r.seller_sku)
  `).all();
  const costMap = new Map(costRows.map(r => [r.sku, r.unit_cost]));

  const skus = skuRows.filter(r => {
    const a = alloc.get(r.seller_sku);
    return (a && (a.direct > 0 || a.ad_sales > 0)) || r.revenue_excl > 0;
  }).map(r => {
    const a = alloc.get(r.seller_sku) || { direct: 0, allocated: 0, ad_sales: 0 };
    const avgPriceExcl = r.units_net > 0 ? r.principal_excl / r.units_net : null;
    const fee = feeMap.get(r.seller_sku.toLowerCase());
    const unitCost = costMap.get(r.seller_sku.toLowerCase());
    // 損益分岐ACOS = (税抜売価 − 手数料(税抜換算) − 原価) / 税抜売価
    let breakevenAcos = null;
    if (avgPriceExcl > 0 && fee && fee.total_fee != null && unitCost != null) {
      breakevenAcos = Math.round((avgPriceExcl - fee.total_fee / TAX_RATE - unitCost) / avgPriceExcl * 1000) / 10;
    }
    const acos = a.ad_sales > 0 ? Math.round((a.direct / a.ad_sales) * 1000) / 10 : null;
    const tacos = r.revenue_excl > 0 ? Math.round((a.direct + a.allocated) / r.revenue_excl * 1000) / 10 : null;
    let verdict = 'no_data';
    if (acos !== null && breakevenAcos !== null) {
      if (acos < breakevenAcos * settings.acos_safe_ratio) verdict = 'safe';
      else if (acos <= breakevenAcos) verdict = 'edge';
      else verdict = 'bleed';
    }
    return {
      seller_sku: r.seller_sku, product_name: r.product_name,
      revenue_excl: Math.round(r.revenue_excl),
      profit_after_ads: Math.round(r.profit_before_ads - a.direct - a.allocated),
      ad_cost: Math.round(a.direct + a.allocated),
      ad_direct: Math.round(a.direct),
      ad_sales: Math.round(a.ad_sales),
      acos_pct: acos, breakeven_acos_pct: breakevenAcos, tacos_pct: tacos, verdict,
    };
  }).sort((a, b) => b.ad_cost - a.ad_cost);

  const campaigns = db.prepare(`
    SELECT campaign_id, MAX(campaign_name) AS campaign_name, MAX(campaign_status) AS status,
      SUM(ad_cost) AS ad_cost, SUM(clicks) AS clicks, SUM(impressions) AS impressions,
      SUM(ad_sales_14d) AS ad_sales_14d, SUM(ad_units_1d) AS ad_units
    FROM mirror_amazon_ads_campaign_daily
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
    GROUP BY campaign_id ORDER BY SUM(ad_cost) DESC
  `).all(from, to).map(c => ({
    ...c,
    ad_cost: Math.round(c.ad_cost),
    ad_sales_14d: Math.round(c.ad_sales_14d),
    acos_pct: c.ad_sales_14d > 0 ? Math.round(c.ad_cost / c.ad_sales_14d * 1000) / 10 : null,
    ctr_pct: c.impressions > 0 ? Math.round(c.clicks / c.impressions * 10000) / 100 : null,
    cpc: c.clicks > 0 ? Math.round(c.ad_cost / c.clicks * 10) / 10 : null,
  }));

  // TACOS 月次トレンド (直近 12 ヶ月、確定売上基準)
  const tacosTrend = db.prepare(`
    WITH ad AS (
      SELECT substr(date_jst,1,7) AS ym, SUM(ad_cost) AS ad_cost
      FROM mirror_amazon_ads_campaign_daily WHERE mall='amazon' GROUP BY ym
    ), rev AS (
      SELECT substr(date_jst,1,7) AS ym,
             SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS revenue
      FROM mirror_amazon_finance_sku_daily GROUP BY ym
    )
    SELECT rev.ym, rev.revenue, COALESCE(ad.ad_cost,0) AS ad_cost,
           CASE WHEN rev.revenue > 0 THEN ROUND(COALESCE(ad.ad_cost,0)/rev.revenue*1000)/10.0 END AS tacos_pct
    FROM rev LEFT JOIN ad ON ad.ym = rev.ym
    ORDER BY rev.ym DESC LIMIT 12
  `).all().reverse();

  return {
    from, to,
    totals: {
      campaign_total: Math.round(campaignTotal),
      sku_direct_total: Math.round(directTotal),
      unallocated,
      unallocated_pct: campaignTotal > 0 ? Math.round(unallocated / campaignTotal * 1000) / 10 : 0,
    },
    skus, campaigns, tacos_trend: tacosTrend,
  };
}

// ─── 売れ筋分析タブ ───
export function getBestsellers(from, to, axis) {
  const db = getMirrorDB();
  const settings = getSettings();
  const days = Math.round((new Date(to + 'T00:00:00Z') - new Date(from + 'T00:00:00Z')) / 86400000) + 1;
  const prevTo = addDays(from, -1);
  const prevFrom = addDays(prevTo, -(days - 1));

  const cur = settledBySku(db, from, to);
  const prevMap = new Map(settledBySku(db, prevFrom, prevTo).map(r => [r.seller_sku, r]));
  const { alloc } = allocateAdCost(db, from, to, cur);

  // FBA/FBM 内訳 (速報系、channel 列)
  const channelRows = db.prepare(`
    SELECT LOWER(item_code) AS sku, channel, SUM(units) AS units
    FROM mirror_f_sales_by_listing
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
    GROUP BY LOWER(item_code), channel
  `).all(from, to);
  const channelMap = new Map();
  for (const r of channelRows) {
    const e = channelMap.get(r.sku) || { FBA: 0, FBM: 0 };
    e[r.channel === 'FBA' ? 'FBA' : 'FBM'] += r.units;
    channelMap.set(r.sku, e);
  }

  // 初売上日 (新商品バッジ用、速報系の全期間 MIN)
  const firstSaleMap = new Map(db.prepare(`
    SELECT LOWER(item_code) AS sku, MIN(date_jst) AS first_date
    FROM mirror_f_sales_by_listing WHERE mall = 'amazon' GROUP BY LOWER(item_code)
  `).all().map(r => [r.sku, r.first_date]));

  const newCutoff = addDays(jstToday(), -settings.new_product_days);

  let rows = cur.map(r => {
    const a = alloc.get(r.seller_sku) || { direct: 0, allocated: 0 };
    const prev = prevMap.get(r.seller_sku);
    const ch = channelMap.get(r.seller_sku.toLowerCase()) || { FBA: 0, FBM: 0 };
    const firstSale = firstSaleMap.get(r.seller_sku.toLowerCase()) || null;
    const profitAfter = r.profit_before_ads - a.direct - a.allocated;
    return {
      seller_sku: r.seller_sku, asin: r.asin_norm, product_name: r.product_name,
      units_net: r.units_net,
      revenue_excl: Math.round(r.revenue_excl),
      profit_after_ads: Math.round(profitAfter),
      margin_pct: r.revenue_excl > 0 ? Math.round(profitAfter / r.revenue_excl * 1000) / 10 : null,
      prev_units: prev ? prev.units_net : 0,
      units_growth_pct: prev && prev.units_net > 0
        ? Math.round((r.units_net - prev.units_net) / prev.units_net * 1000) / 10
        : (r.units_net > 0 ? null : 0),   // null = 前期実績なし (NEW 扱い)
      fba_units: ch.FBA, fbm_units: ch.FBM,
      is_new: firstSale !== null && firstSale >= newCutoff,
      first_sale_date: firstSale,
    };
  });

  // ABC 分析 (売上累積構成比)
  const byRevenue = [...rows].sort((a, b) => b.revenue_excl - a.revenue_excl);
  const totalRevenue = byRevenue.reduce((s, r) => s + Math.max(0, r.revenue_excl), 0);
  let cum = 0;
  const abcCount = { A: 0, B: 0, C: 0 };
  const abcRevenue = { A: 0, B: 0, C: 0 };
  for (const r of byRevenue) {
    cum += Math.max(0, r.revenue_excl);
    const pct = totalRevenue > 0 ? cum / totalRevenue * 100 : 100;
    r.abc = pct <= settings.abc_a_pct ? 'A' : pct <= settings.abc_b_pct ? 'B' : 'C';
    abcCount[r.abc]++;
    abcRevenue[r.abc] += Math.max(0, r.revenue_excl);
  }

  const axisKey = axis === 'units' ? 'units_net' : axis === 'profit' ? 'profit_after_ads' : 'revenue_excl';
  rows = [...rows].sort((a, b) => b[axisKey] - a[axisKey]);

  // 急上昇 / 急落 (前期 5 個以上売れた SKU のみ、ノイズ除去)
  const movers = rows.filter(r => r.prev_units >= 5 || r.units_net >= 5);
  const risers = [...movers].filter(r => r.units_growth_pct !== null && r.units_growth_pct > 0).sort((a, b) => b.units_growth_pct - a.units_growth_pct).slice(0, 20);
  const fallers = [...movers].filter(r => r.units_growth_pct !== null && r.units_growth_pct < 0).sort((a, b) => a.units_growth_pct - b.units_growth_pct).slice(0, 20);

  // 曜日パターン (全体、販売数)
  const weekday = db.prepare(`
    SELECT CAST(strftime('%w', date_jst) AS INTEGER) AS dow, SUM(units) AS units, COUNT(DISTINCT date_jst) AS days
    FROM mirror_f_sales_by_listing
    WHERE mall = 'amazon' AND date_jst >= ? AND date_jst <= ?
    GROUP BY dow ORDER BY dow
  `).all(from, to).map(r => ({ ...r, avg_units: r.days > 0 ? Math.round(r.units / r.days * 10) / 10 : 0 }));

  // 上位 30 SKU の日次スパークライン (確定数量)
  const topSkus = rows.slice(0, 30).map(r => r.seller_sku);
  const sparkMap = new Map();
  if (topSkus.length > 0) {
    const ph = topSkus.map(() => '?').join(',');
    const sparkRows = db.prepare(`
      SELECT seller_sku, date_jst, SUM(units_net_sold) AS units
      FROM mirror_amazon_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ? AND seller_sku IN (${ph})
      GROUP BY seller_sku, date_jst ORDER BY date_jst
    `).all(from, to, ...topSkus);
    for (const r of sparkRows) {
      if (!sparkMap.has(r.seller_sku)) sparkMap.set(r.seller_sku, []);
      sparkMap.get(r.seller_sku).push({ d: r.date_jst, u: r.units });
    }
  }
  for (const r of rows.slice(0, 30)) r.spark = sparkMap.get(r.seller_sku) || [];

  return {
    from, to, prev_from: prevFrom, prev_to: prevTo, axis: axisKey,
    total_skus: rows.length,
    abc: { count: abcCount, revenue: { A: Math.round(abcRevenue.A), B: Math.round(abcRevenue.B), C: Math.round(abcRevenue.C) }, total_revenue: Math.round(totalRevenue) },
    ranking: rows.slice(0, 100),
    risers, fallers, weekday,
  };
}

// ─── 診断タブ (ルール 6 種、F-6) ───
export function getDiagnosis() {
  const db = getMirrorDB();
  const settings = getSettings();
  const today = jstToday();
  const ym = monthOf(today);
  const m1 = addMonths(ym, -1);   // 前月 (確定済み想定)
  const m2 = addMonths(ym, -2);   // 前々月

  const cur = settledBySku(db, monthStart(m1), monthEnd(m1));
  const prev = settledBySku(db, monthStart(m2), monthEnd(m2));
  const prevMap = new Map(prev.map(r => [r.seller_sku, r]));
  const { alloc: allocCur } = allocateAdCost(db, monthStart(m1), monthEnd(m1), cur);
  const { alloc: allocPrev } = allocateAdCost(db, monthStart(m2), monthEnd(m2), prev);

  const profitAfter = (r, alloc) => {
    const a = alloc.get(r.seller_sku) || { direct: 0, allocated: 0 };
    return r.profit_before_ads - a.direct - a.allocated;
  };

  // 赤字SKU判定用: 直近 loss_months ヶ月分の SKU 別広告後利益 (m1 から遡る)
  // (Codex R2 Medium #1: loss_months 設定を実際に判定へ反映)
  const lossMonths = Math.max(2, Math.min(12, Math.round(settings.loss_months)));
  const monthlyProfit = [];   // [{ym, map<sku, profit_after>}] 新しい順
  for (let i = 0; i < lossMonths; i++) {
    const targetYm = addMonths(ym, -(i + 1));
    let skuAgg, alloc;
    if (i === 0) { skuAgg = cur; alloc = allocCur; }
    else if (i === 1) { skuAgg = prev; alloc = allocPrev; }
    else {
      skuAgg = settledBySku(db, monthStart(targetYm), monthEnd(targetYm));
      alloc = allocateAdCost(db, monthStart(targetYm), monthEnd(targetYm), skuAgg).alloc;
    }
    monthlyProfit.push({ ym: targetYm, map: new Map(skuAgg.map(r => [r.seller_sku, profitAfter(r, alloc)])) });
  }

  // ABC A 群 (稼ぎ頭)
  const byProfit = [...cur].sort((a, b) => profitAfter(b, allocCur) - profitAfter(a, allocCur));
  const totalProfit = byProfit.reduce((s, r) => s + Math.max(0, profitAfter(r, allocCur)), 0);
  let cum = 0;
  const earners = [];
  for (const r of byProfit) {
    const p = profitAfter(r, allocCur);
    if (p <= 0) break;
    cum += p;
    earners.push({ seller_sku: r.seller_sku, product_name: r.product_name, profit_after_ads: Math.round(p), revenue_excl: Math.round(r.revenue_excl) });
    if (totalProfit > 0 && cum / totalProfit * 100 >= settings.abc_a_pct) break;
  }

  // 赤字 SKU (loss_months ヶ月連続 広告後赤字。全対象月に実績があり、かつ全月赤字)
  const lossSkus = cur.filter(r => {
    return monthlyProfit.every(mp => {
      const p = mp.map.get(r.seller_sku);
      return p !== undefined && p < 0;
    });
  }).map(r => ({
    seller_sku: r.seller_sku, product_name: r.product_name,
    profit_cur: Math.round(monthlyProfit[0].map.get(r.seller_sku)),
    profit_prev: Math.round(monthlyProfit[1].map.get(r.seller_sku)),
    months_checked: lossMonths,
    revenue_excl: Math.round(r.revenue_excl),
  })).sort((a, b) => a.profit_cur - b.profit_cur).slice(0, 50);

  // 悪化 SKU (利益率 前月比 -Npt 超)
  const worsened = cur.filter(r => r.revenue_excl > 10000).map(r => {
    const rPrev = prevMap.get(r.seller_sku);
    if (!rPrev || rPrev.revenue_excl <= 10000) return null;
    const mCur = profitAfter(r, allocCur) / r.revenue_excl * 100;
    const mPrev = profitAfter(rPrev, allocPrev) / rPrev.revenue_excl * 100;
    const drop = mPrev - mCur;
    if (drop < settings.margin_drop_pt) return null;
    return {
      seller_sku: r.seller_sku, product_name: r.product_name,
      margin_cur_pct: Math.round(mCur * 10) / 10, margin_prev_pct: Math.round(mPrev * 10) / 10,
      drop_pt: Math.round(drop * 10) / 10, revenue_excl: Math.round(r.revenue_excl),
    };
  }).filter(Boolean).sort((a, b) => b.drop_pt - a.drop_pt).slice(0, 50);

  // 広告垂れ流し (直近 ad_bleed_days 日の**合算** ACOS > 損益分岐 ACOS)
  // 日次連続性ではなく期間合算での判定 (仕様、Codex R2 Medium #2 で明確化)。
  // 単日ノイズ排除のため期間広告費 ¥1,000 未満は対象外。
  const adFrom = addDays(today, -(settings.ad_bleed_days - 1));
  const recent = settledBySku(db, adFrom, today);
  const { alloc: allocRecent } = allocateAdCost(db, adFrom, today, recent);
  const feeMap = new Map(db.prepare(`SELECT LOWER(seller_sku) AS sku, total_fee FROM mirror_amazon_sku_fees`).all().map(r => [r.sku, r.total_fee]));
  const costMap = new Map(db.prepare(`
    SELECT LOWER(r.seller_sku) AS sku, SUM(r.quantity * p.原価) AS unit_cost
    FROM mirror_sku_resolved r LEFT JOIN mirror_products p ON p.商品コード = r.ne_code
    GROUP BY LOWER(r.seller_sku)
  `).all().map(r => [r.sku, r.unit_cost]));
  const adBleed = recent.map(r => {
    const a = allocRecent.get(r.seller_sku);
    if (!a || a.direct < 1000 || a.ad_sales <= 0) return null;
    const acos = a.direct / a.ad_sales * 100;
    const avgPrice = r.units_net > 0 ? r.principal_excl / r.units_net : null;
    const fee = feeMap.get(r.seller_sku.toLowerCase());
    const cost = costMap.get(r.seller_sku.toLowerCase());
    if (!avgPrice || fee == null || cost == null) return null;
    const breakeven = (avgPrice - fee / TAX_RATE - cost) / avgPrice * 100;
    if (acos <= breakeven) return null;
    return {
      seller_sku: r.seller_sku, product_name: r.product_name,
      acos_pct: Math.round(acos * 10) / 10, breakeven_acos_pct: Math.round(breakeven * 10) / 10,
      ad_cost: Math.round(a.direct), ad_sales: Math.round(a.ad_sales),
    };
  }).filter(Boolean).sort((a, b) => (b.acos_pct - b.breakeven_acos_pct) - (a.acos_pct - a.breakeven_acos_pct)).slice(0, 50);

  // 死に筋 (販売0 が N 日 + FBA在庫あり)
  const deadCutoff = addDays(today, -settings.dead_stock_days);
  const latestInvDate = db.prepare(`SELECT MAX(business_date) AS d FROM mirror_inv_daily_detail WHERE market = 'jp' AND category IN ('fba_warehouse','fba_inbound')`).get()?.d;
  let deadStock = [];
  if (latestInvDate) {
    deadStock = db.prepare(`
      SELECT ne_code, MAX(product_name) AS product_name, SUM(qty) AS qty,
             SUM(COALESCE(total_value,0)) AS stock_value, MAX(last_sold_date) AS last_sold_date
      FROM mirror_inv_daily_detail
      WHERE business_date = ? AND market = 'jp' AND category IN ('fba_warehouse','fba_inbound')
      GROUP BY ne_code
      HAVING SUM(qty) > 0 AND (MAX(last_sold_date) IS NULL OR MAX(last_sold_date) < ?)
      ORDER BY stock_value DESC LIMIT 50
    `).all(latestInvDate, deadCutoff).map(r => ({ ...r, stock_value: Math.round(r.stock_value) }));
  }

  // 価格ミス疑い (実売単価(税込換算) < 標準売価 × ratio)
  const priceMiss = attachProductNames(db, db.prepare(`
    WITH sold AS (
      SELECT seller_sku, SUM(sales_principal_jpy) AS principal, SUM(units_net_sold) AS units,
             MAX(product_name) AS product_name
      FROM mirror_amazon_finance_sku_daily
      WHERE date_jst >= ? AND date_jst <= ?
      GROUP BY seller_sku HAVING SUM(units_net_sold) >= 3
    )
    SELECT s.seller_sku, s.product_name,
           ROUND(s.principal / s.units * ${TAX_RATE}) AS avg_price_incl,
           p.標準売価 AS standard_price
    FROM sold s
    JOIN mirror_sku_resolved r ON r.seller_sku = s.seller_sku AND r.source = 'master'
    JOIN mirror_products p ON p.商品コード = r.ne_code AND p.標準売価 > 0
    WHERE (SELECT COUNT(*) FROM mirror_sku_resolved r2 WHERE r2.seller_sku = s.seller_sku) = 1
      AND s.principal / s.units * ${TAX_RATE} < p.標準売価 * ?
    ORDER BY p.標準売価 - s.principal / s.units * ${TAX_RATE} DESC
    LIMIT 50
  `).all(monthStart(m1), today, settings.price_miss_ratio));

  return {
    generated_at: new Date().toISOString(),
    base_month: m1, prev_month: m2, settings,
    earners: earners.slice(0, 30), loss_skus: lossSkus, worsened, ad_bleed: adBleed,
    dead_stock: deadStock, dead_stock_as_of: latestInvDate, price_miss: priceMiss,
  };
}

// ─── 在庫タブ (v1: mirror_inv_daily_detail の最新スナップショット) ───
export function getInventory() {
  const db = getMirrorDB();
  const latest = db.prepare(`SELECT MAX(business_date) AS d FROM mirror_inv_daily_detail WHERE market = 'jp' AND category IN ('fba_warehouse','fba_inbound')`).get()?.d;
  if (!latest) return { as_of: null, rows: [], summary: null };
  const rows = db.prepare(`
    SELECT ne_code, MAX(product_name) AS product_name,
      SUM(CASE WHEN category = 'fba_warehouse' THEN qty ELSE 0 END) AS fba_qty,
      SUM(CASE WHEN category = 'fba_inbound' THEN qty ELSE 0 END) AS inbound_qty,
      SUM(COALESCE(total_value,0)) AS stock_value,
      MAX(COALESCE(sales_30d_qty,0)) AS sales_30d,
      MAX(last_sold_date) AS last_sold_date
    FROM mirror_inv_daily_detail
    WHERE business_date = ? AND market = 'jp' AND category IN ('fba_warehouse','fba_inbound')
    GROUP BY ne_code
    HAVING SUM(qty) > 0
  `).all(latest).map(r => {
    const dailyVelocity = r.sales_30d / 30;
    const daysOfCover = dailyVelocity > 0 ? Math.round((r.fba_qty + r.inbound_qty) / dailyVelocity) : null;
    return { ...r, stock_value: Math.round(r.stock_value), days_of_cover: daysOfCover };
  }).sort((a, b) => {
    if (a.days_of_cover === null && b.days_of_cover === null) return b.stock_value - a.stock_value;
    if (a.days_of_cover === null) return 1;
    if (b.days_of_cover === null) return -1;
    return a.days_of_cover - b.days_of_cover;
  });
  const summary = {
    total_value: Math.round(rows.reduce((s, r) => s + r.stock_value, 0)),
    sku_count: rows.length,
    low_cover_count: rows.filter(r => r.days_of_cover !== null && r.days_of_cover < 30).length,
    no_velocity_count: rows.filter(r => r.days_of_cover === null).length,
  };
  return { as_of: latest, rows: rows.slice(0, 300), summary };
}

// ─── カスタム経費 CRUD ───
export function listExpenses() {
  ensureAppTables();
  const db = getMirrorDB();
  return db.prepare(`SELECT * FROM amzdash_custom_expenses WHERE deleted_at IS NULL ORDER BY month_from DESC, id DESC`).all();
}
export function addExpense({ name, expense_type, amount_jpy, month_from, month_to, memo }) {
  ensureAppTables();
  const db = getMirrorDB();
  if (!name || !['fixed_monthly', 'oneoff', 'sales_pct'].includes(expense_type)) throw new Error('invalid expense');
  if (!/^\d{4}-\d{2}$/.test(month_from || '')) throw new Error('month_from must be YYYY-MM');
  if (month_to && !/^\d{4}-\d{2}$/.test(month_to)) throw new Error('month_to must be YYYY-MM');
  const amount = Number(amount_jpy);
  if (!Number.isFinite(amount) || amount < 0) throw new Error('amount_jpy must be >= 0');
  const now = new Date().toISOString();
  const info = db.prepare(`
    INSERT INTO amzdash_custom_expenses (name, expense_type, amount_jpy, month_from, month_to, memo, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(String(name).slice(0, 100), expense_type, amount, month_from, month_to || null, String(memo || '').slice(0, 500), now, now);
  return { id: info.lastInsertRowid };
}
export function deleteExpense(id) {
  ensureAppTables();
  const db = getMirrorDB();
  const info = db.prepare(`UPDATE amzdash_custom_expenses SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL`).run(new Date().toISOString(), Number(id));
  return { deleted: info.changes > 0 };
}
