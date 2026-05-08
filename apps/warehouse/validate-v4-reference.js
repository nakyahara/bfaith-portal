#!/usr/bin/env node
/**
 * validate-v4-reference.js — Phase 1 #1-3 v4 validation report
 *
 * f_amazon_finance_sku_daily_v1 を月次に rollup して
 * v_amazon_sku_profit_actual_v4 (gross_margin_with_reimbursement_excl_tax) と比較。
 *
 * #1-8a の DQ runner との違い:
 *   - 本 script: 人間が読む report (markdown 出力可)、bucket ルール docs と連動
 *   - DQ runner: 機械的 gate (severity / threshold / exit code 1)
 *
 * 使い方:
 *   DATA_DIR=... node apps/warehouse/validate-v4-reference.js
 *   DATA_DIR=... node apps/warehouse/validate-v4-reference.js --month 2026-04
 *   DATA_DIR=... node apps/warehouse/validate-v4-reference.js --markdown > report.md
 */

import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}

const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthFilter = getArg('--month');  // YYYY-MM (オプション、未指定で全月)
const isMarkdown = args.includes('--markdown');

if (!DATA_DIR) {
  console.error('FATAL: DATA_DIR is required');
  process.exit(2);
}

const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exit(2);
}

const db = new Database(dbPath, { readonly: true });

// 月別合計比較
const monthFilterClause = monthFilter ? `WHERE substr(date_jst, 1, 7) = '${monthFilter}'` : '';
const monthFilterClauseV4 = monthFilter ? `WHERE year_month = '${monthFilter}'` : '';

const monthly = db.prepare(`
  WITH daily_monthly AS (
    SELECT
      substr(date_jst, 1, 7) AS month_jst,
      SUM(units_ordered) AS units,
      SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS revenue,
      SUM(cogs_amount) AS cogs,
      SUM(profit_amount) AS profit
    FROM f_amazon_finance_sku_daily_v1 ${monthFilterClause}
    GROUP BY 1
  ),
  v4_monthly AS (
    SELECT
      year_month AS month_jst,
      SUM(qty_ordered) AS units,
      SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy) AS revenue,
      SUM(cogs_excl_tax) AS cogs,
      SUM(gross_margin_excl_tax)
        + SUM(warehouse_damage_jpy) + SUM(warehouse_lost_jpy) + SUM(safe_t_jpy)
        + SUM(refund_principal_jpy) + SUM(reversal_reimbursement_jpy) AS profit
    FROM v_amazon_sku_profit_actual_v4 ${monthFilterClauseV4}
    GROUP BY 1
  )
  SELECT d.month_jst, d.units AS units_d, v.units AS units_v4,
         ROUND(d.revenue, 0) AS revenue_d, ROUND(v.revenue, 0) AS revenue_v4,
         ROUND(d.cogs, 0) AS cogs_d, ROUND(v.cogs, 0) AS cogs_v4,
         ROUND(d.profit, 0) AS profit_d, ROUND(v.profit, 0) AS profit_v4,
         ROUND(d.profit - v.profit, 0) AS profit_diff,
         ROUND(ABS(d.profit - v.profit) / NULLIF(ABS(v.profit), 0) * 100, 3) AS profit_diff_pct
  FROM daily_monthly d JOIN v4_monthly v ON v.month_jst = d.month_jst
  ORDER BY d.month_jst
`).all();

// SKU x 月の差絶対値 TOP
const topDiff = db.prepare(`
  WITH daily_sm AS (
    SELECT substr(date_jst,1,7) AS month_jst, seller_sku, SUM(profit_amount) AS profit_d
    FROM f_amazon_finance_sku_daily_v1 ${monthFilterClause}
    GROUP BY 1,2
  ),
  v4_sm AS (
    SELECT year_month AS month_jst, seller_sku,
      SUM(gross_margin_excl_tax) + SUM(warehouse_damage_jpy) + SUM(warehouse_lost_jpy)
      + SUM(safe_t_jpy) + SUM(refund_principal_jpy) + SUM(reversal_reimbursement_jpy) AS profit_v4
    FROM v_amazon_sku_profit_actual_v4 ${monthFilterClauseV4}
    GROUP BY 1,2
  )
  SELECT COALESCE(d.month_jst, v.month_jst) AS month_jst,
         COALESCE(d.seller_sku, v.seller_sku) AS seller_sku,
         ROUND(COALESCE(d.profit_d, 0), 0) AS profit_d,
         ROUND(COALESCE(v.profit_v4, 0), 0) AS profit_v4,
         ROUND(COALESCE(d.profit_d, 0) - COALESCE(v.profit_v4, 0), 0) AS diff
  FROM daily_sm d
  LEFT JOIN v4_sm v ON v.month_jst = d.month_jst AND v.seller_sku = d.seller_sku
  WHERE ABS(COALESCE(d.profit_d, 0) - COALESCE(v.profit_v4, 0)) > 100
  ORDER BY ABS(COALESCE(d.profit_d, 0) - COALESCE(v.profit_v4, 0)) DESC
  LIMIT 20
`).all();

// 集合差
const setDiff = db.prepare(`
  WITH daily_sku AS (
    SELECT DISTINCT substr(date_jst,1,7) AS month_jst, seller_sku FROM f_amazon_finance_sku_daily_v1 ${monthFilterClause}
  ),
  v4_sku AS (
    SELECT DISTINCT year_month AS month_jst, seller_sku FROM v_amazon_sku_profit_actual_v4 ${monthFilterClauseV4}
  ),
  ll AS (
    SELECT v.month_jst, COUNT(*) AS legacy_only
    FROM v4_sku v LEFT JOIN daily_sku d ON d.month_jst = v.month_jst AND d.seller_sku = v.seller_sku
    WHERE d.seller_sku IS NULL
    GROUP BY 1
  ),
  dd AS (
    SELECT d.month_jst, COUNT(*) AS daily_only
    FROM daily_sku d LEFT JOIN v4_sku v ON v.month_jst = d.month_jst AND v.seller_sku = d.seller_sku
    WHERE v.seller_sku IS NULL
    GROUP BY 1
  )
  SELECT COALESCE(ll.month_jst, dd.month_jst) AS month_jst,
         COALESCE(ll.legacy_only, 0) AS legacy_only,
         COALESCE(dd.daily_only, 0) AS daily_only
  FROM ll FULL OUTER JOIN dd USING (month_jst)
  ORDER BY 1
`).all();

// cost_status 内訳
const costStatus = db.prepare(`
  SELECT substr(date_jst, 1, 7) AS month_jst, cost_status, COUNT(*) AS rows,
         ROUND(SUM(profit_amount), 0) AS profit_jpy
  FROM f_amazon_finance_sku_daily_v1 ${monthFilterClause}
  GROUP BY 1, 2 ORDER BY 1, 2
`).all();

db.close();

// ============================================================
// 出力
// ============================================================
if (isMarkdown) {
  // Markdown 出力
  console.log(`# v4 Validation Report\n\n**生成日時**: ${new Date().toISOString()}`);
  console.log(`**スコープ**: ${monthFilter || '全月'}\n`);
  console.log(`**比較対象**: \`v_amazon_sku_profit_actual_v4.gross_margin_with_reimbursement_excl_tax\` (PR #61 で追加)\n`);

  console.log('## A. 月別合計比較\n');
  console.log('| 月 | units (d/v4) | revenue (d/v4) | cogs (d/v4) | profit (d/v4) | profit diff | diff % |');
  console.log('|---|---|---|---|---|---|---|');
  for (const r of monthly) {
    console.log(`| ${r.month_jst} | ${r.units_d?.toLocaleString()}/${r.units_v4?.toLocaleString()} | ${r.revenue_d?.toLocaleString()}/${r.revenue_v4?.toLocaleString()} | ${r.cogs_d?.toLocaleString()}/${r.cogs_v4?.toLocaleString()} | ${r.profit_d?.toLocaleString()}/${r.profit_v4?.toLocaleString()} | ${r.profit_diff?.toLocaleString()} | ${r.profit_diff_pct}% |`);
  }

  console.log('\n## B. SKU x 月で profit 差絶対値 TOP 20\n');
  if (topDiff.length === 0) {
    console.log('(差 100 円超の SKU 無し)');
  } else {
    console.log('| 月 | SKU | profit (daily) | profit (v4) | diff |');
    console.log('|---|---|---|---|---|');
    for (const r of topDiff) {
      console.log(`| ${r.month_jst} | ${r.seller_sku} | ${r.profit_d?.toLocaleString()} | ${r.profit_v4?.toLocaleString()} | ${r.diff?.toLocaleString()} |`);
    }
  }

  console.log('\n## C. 集合差 (SKU レベル)\n');
  console.log('| 月 | legacy_only (v4 のみ) | daily_only (daily のみ) |');
  console.log('|---|---|---|');
  for (const r of setDiff) {
    console.log(`| ${r.month_jst} | ${r.legacy_only} | ${r.daily_only} |`);
  }

  console.log('\n## D. cost_status 内訳 (daily fact)\n');
  console.log('| 月 | cost_status | rows | profit_jpy |');
  console.log('|---|---|---|---|');
  for (const r of costStatus) {
    console.log(`| ${r.month_jst} | ${r.cost_status} | ${r.rows?.toLocaleString()} | ${r.profit_jpy?.toLocaleString()} |`);
  }
} else {
  // 通常出力
  console.log('=== A. 月別合計比較 ===');
  console.table(monthly);
  console.log('\n=== B. SKU x 月で profit 差絶対値 TOP 20 (>100円) ===');
  console.table(topDiff);
  console.log('\n=== C. 集合差 ===');
  console.table(setDiff);
  console.log('\n=== D. cost_status 内訳 ===');
  console.table(costStatus);
}
