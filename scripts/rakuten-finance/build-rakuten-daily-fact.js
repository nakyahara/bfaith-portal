#!/usr/bin/env node
/**
 * f_rakuten_finance_sku_daily_v1 build entrypoint
 *
 * Phase 1a ticket: #R-1
 * - DDL: sql/rakuten/f_rakuten_finance_sku_daily_v1.sql
 * - Build SQL: sql/rakuten/build_f_rakuten_finance_sku_daily_v1.sql
 * - Contract: docs/contracts/raw_rakuten_orders.contract.md (v1)
 * - 設計書: g:/共有ドライブ/AI_reference/システム設計/楽天Phase1a設計書_v0.3_20260509.md
 *
 * 不変条件 (Codex Round 11-13、Amazon Phase 1 #1-1 から継承):
 *   - 既存 (date_jst, rakuten_code) は UPSERT で snapshot 列以外を更新 (snapshot 不変)
 *   - cogs_amount は「既存 snapshot 原価 × 新 units」で再計算
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data node scripts/rakuten-finance/build-rakuten-daily-fact.js --month 2026-04
 *   --dry-run でシミュレーション
 */

import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

// ============================================================
// CLI args
// ============================================================
const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}
const dataDir = (getArg('--data-dir') || process.env.DATA_DIR || '').trim();
const monthStr = getArg('--month');
const dryRun = args.includes('--dry-run');

if (!dataDir) {
  console.error('FATAL: --data-dir or DATA_DIR is required.');
  console.error('  Reason: process.cwd() fallback can silently create a stray DB in worktree.');
  process.exit(2);
}
if (!monthStr || !/^\d{4}-\d{2}$/.test(monthStr)) {
  console.error('FATAL: --month YYYY-MM is required (例: --month 2026-04)');
  process.exit(2);
}

const dbPath = path.join(dataDir, 'warehouse.db');
if (!fs.existsSync(dbPath)) {
  console.error(`FATAL: warehouse.db not found at ${dbPath}`);
  process.exit(2);
}

const yearMonthInt = parseInt(monthStr.replace('-', ''), 10);
const buildDate = new Date().toISOString().slice(0, 10);

console.log('=== f_rakuten_finance_sku_daily_v1 build ===');
console.log(`DB: ${dbPath}`);
console.log(`Month: ${monthStr} (year_month_int=${yearMonthInt})`);
console.log(`Build date: ${buildDate}`);
console.log(`Dry run: ${dryRun}`);
console.log('');

// ============================================================
// SQL files load (CLI で --ddl-path / --build-sql-path 指定可)
// ============================================================
const cliDdlPath = getArg('--ddl-path');
const cliBuildSqlPath = getArg('--build-sql-path');
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '..', '..');
const ddlPath = cliDdlPath || path.join(repoRoot, 'sql', 'rakuten', 'f_rakuten_finance_sku_daily_v1.sql');
const buildSqlPath = cliBuildSqlPath || path.join(repoRoot, 'sql', 'rakuten', 'build_f_rakuten_finance_sku_daily_v1.sql');

if (!fs.existsSync(ddlPath)) {
  console.error(`FATAL: DDL not found at ${ddlPath}`);
  process.exit(2);
}
if (!fs.existsSync(buildSqlPath)) {
  console.error(`FATAL: build SQL not found at ${buildSqlPath}`);
  process.exit(2);
}

const ddlSql = fs.readFileSync(ddlPath, 'utf8');
const rawBuildSql = fs.readFileSync(buildSqlPath, 'utf8');

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 30000');

try {
  // ============================================================
  // 1. DDL 実行
  // ============================================================
  console.log('--- 1. DDL exec ---');
  db.exec(ddlSql);
  console.log('  ✓ DDL applied (CREATE TABLE IF NOT EXISTS)');

  // ============================================================
  // 2. 対象月の既存 row 数
  // ============================================================
  const beforeCount = db.prepare(`
    SELECT COUNT(*) AS c FROM f_rakuten_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
  `).get(monthStr).c;
  console.log(`  Existing rows for ${monthStr}: ${beforeCount.toLocaleString()}`);

  // ============================================================
  // 3. build SQL 実行 (CTE A→B→C→D + UPSERT、tx 化)
  // ============================================================
  console.log('--- 3. build SQL exec (silver + UPSERT、tx) ---');
  const statements = rawBuildSql
    .split(';')
    .map(s => s.trim())
    .filter(s => {
      if (!s) return false;
      const codeOnly = s
        .split('\n')
        .filter(l => !l.trim().startsWith('--') && l.trim() !== '')
        .join('\n')
        .trim();
      return codeOnly.length > 0;
    });
  console.log(`  Parsed ${statements.length} executable statements`);

  if (dryRun) {
    console.log(`  (dry-run) would execute ${statements.length} statements in a single tx`);
  } else {
    let totalChanges = 0;
    const runBuildTx = db.transaction(() => {
      for (const sql of statements) {
        const hasParams = sql.includes(':year_month_int') || sql.includes(':build_date');
        if (hasParams) {
          const stmt = db.prepare(sql);
          const info = stmt.run({ year_month_int: yearMonthInt, build_date: buildDate });
          if (info.changes > 0) totalChanges += info.changes;
        } else {
          db.exec(sql);
        }
      }
    });
    runBuildTx.immediate();
    console.log(`  ✓ Total inserted/changed rows: ${totalChanges.toLocaleString()}`);
  }

  // ============================================================
  // 4. 結果サマリ
  // ============================================================
  console.log('--- 4. result summary ---');
  const afterCount = db.prepare(`
    SELECT COUNT(*) AS c FROM f_rakuten_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
  `).get(monthStr).c;
  console.log(`  After build: ${afterCount.toLocaleString()} rows for ${monthStr}`);

  if (!dryRun && afterCount > 0) {
    const summary = db.prepare(`
      SELECT
        COUNT(*) AS row_count,
        SUM(units_ordered) AS sum_units_ordered,
        SUM(units_cancelled) AS sum_units_cancelled,
        SUM(units_net_sold) AS sum_units_net_sold,
        ROUND(SUM(gross_sales_jpy_incl), 0) AS sum_gross_sales,
        ROUND(SUM(coupon_shop_jpy_incl), 0) AS sum_coupon_shop,
        ROUND(SUM(coupon_all_jpy_incl), 0) AS sum_coupon_all,
        ROUND(SUM(refund_amount_jpy_incl), 0) AS sum_refund,
        ROUND(SUM(mall_fee_jpy_incl), 0) AS sum_mall_fee,
        ROUND(SUM(shipping_cost_jpy_incl), 0) AS sum_shipping,
        ROUND(SUM(cogs_amount_jpy_incl), 0) AS sum_cogs,
        ROUND(SUM(net_sales_jpy_incl), 0) AS sum_net_sales,
        ROUND(SUM(variable_margin_jpy_incl), 0) AS sum_variable_margin,
        ROUND(SUM(refund_adjusted_net_sales_jpy_incl), 0) AS sum_refund_adjusted,
        SUM(CASE WHEN cost_status = 'complete' THEN 1 ELSE 0 END) AS complete_count,
        SUM(CASE WHEN cost_status = 'missing_cost' THEN 1 ELSE 0 END) AS missing_count,
        SUM(price_variance_warning) AS price_variance_count,
        SUM(CASE WHEN shipping_quality = 'missing' THEN 1 ELSE 0 END) AS shipping_missing_count
      FROM f_rakuten_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
    `).get(monthStr);

    console.log('  monthly totals (税込):');
    console.log(`    gross_sales (principal+postage):      ¥${summary.sum_gross_sales?.toLocaleString()}`);
    console.log(`    coupon_shop (自社負担):                -¥${summary.sum_coupon_shop?.toLocaleString()}`);
    console.log(`    coupon_all  (全店、参考):              -¥${summary.sum_coupon_all?.toLocaleString()}`);
    console.log(`    net_sales   (gross - coupon_shop):    ¥${summary.sum_net_sales?.toLocaleString()}`);
    console.log(`    cogs (snapshot 原価 × units_net_sold): -¥${summary.sum_cogs?.toLocaleString()}`);
    console.log(`    shipping_cost (自社負担):              -¥${summary.sum_shipping?.toLocaleString()}`);
    console.log(`    mall_fee (10%):                       -¥${summary.sum_mall_fee?.toLocaleString()}`);
    console.log(`    refund_amount:                         -¥${summary.sum_refund?.toLocaleString()}`);
    console.log(`    variable_margin:                       ¥${summary.sum_variable_margin?.toLocaleString()}`);
    const margin_pct = summary.sum_gross_sales > 0
      ? (summary.sum_variable_margin / summary.sum_gross_sales * 100).toFixed(2)
      : 'n/a';
    console.log(`    粗利率 (variable_margin / gross_sales): ${margin_pct}%`);
    console.log(`    refund_adjusted_net_sales:             ¥${summary.sum_refund_adjusted?.toLocaleString()}`);
    console.log(`  units:`);
    console.log(`    ordered:    ${summary.sum_units_ordered?.toLocaleString()}`);
    console.log(`    cancelled:  ${summary.sum_units_cancelled?.toLocaleString()}`);
    console.log(`    net_sold:   ${summary.sum_units_net_sold?.toLocaleString()}`);
    console.log(`  品質:`);
    console.log(`    cost_status complete:           ${summary.complete_count?.toLocaleString()}`);
    console.log(`    cost_status missing_cost:       ${summary.missing_count?.toLocaleString()}`);
    console.log(`    price_variance_warning:         ${summary.price_variance_count?.toLocaleString()}`);
    console.log(`    shipping_quality missing:       ${summary.shipping_missing_count?.toLocaleString()}`);

    // f_sales_by_listing 楽天分との突合 (validation Layer 1、簡易)
    try {
      const listing = db.prepare(`
        SELECT
          COUNT(*) AS row_count,
          ROUND(SUM(売上金額), 0) AS listing_sales,
          SUM(数量) AS listing_units
        FROM f_sales_by_listing
        WHERE モール = 'rakuten' AND substr(日付, 1, 7) = ?
      `).get(monthStr);
      console.log('');
      console.log('  --- validation: f_sales_by_listing (楽天) との突合 ---');
      console.log(`    listing rows:          ${listing.row_count?.toLocaleString()}`);
      console.log(`    listing 売上 (税込):    ¥${listing.listing_sales?.toLocaleString()}`);
      console.log(`    daily fact gross_sales: ¥${summary.sum_gross_sales?.toLocaleString()}`);
      const diff = (summary.sum_gross_sales || 0) - (listing.listing_sales || 0);
      const diffPct = listing.listing_sales ? (Math.abs(diff) / listing.listing_sales * 100).toFixed(3) : 'n/a';
      console.log(`    diff:                  ¥${diff.toLocaleString()} (${diffPct}%)`);
      console.log(`    listing 数量:           ${listing.listing_units?.toLocaleString()}`);
      console.log(`    daily fact units_ordered: ${summary.sum_units_ordered?.toLocaleString()}`);
    } catch (e) {
      console.log('  (validation スキップ:', e.message, ')');
    }

    // fact_returns 突合 (validation Layer 2、D 案前提の整合性確認 + Phase 1b 按分対象モニタリング)
    try {
      const returns = db.prepare(`
        SELECT
          COUNT(*) AS row_count,
          SUM(数量) AS sum_units_returned,
          ROUND(SUM(返金額), 0) AS sum_refund
        FROM fact_returns
        WHERE モール = 'rakuten' AND 注文日 IS NOT NULL
          AND substr(注文日, 1, 7) = ?
      `).get(monthStr);

      const negCount = db.prepare(`
        SELECT COUNT(*) AS c
        FROM f_rakuten_finance_sku_daily_v1
        WHERE substr(date_jst, 1, 7) = ?
          AND units_cancelled > units_ordered
      `).get(monthStr).c;

      // Phase 1b 按分対象モニタリング: refund 注文日 と silver date_jst が日付不一致な (date, code) ペア
      const dateMismatchSkus = db.prepare(`
        WITH silver AS (
          SELECT substr(order_date,1,10) AS date_jst, item_number AS rakuten_code, SUM(units) AS units
          FROM raw_rakuten_orders
          WHERE order_status IN (500,600,700) AND substr(order_date,1,7) = ?
          GROUP BY substr(order_date,1,10), item_number
        ),
        ret AS (
          SELECT 注文日 AS date_jst, モール商品コード AS rakuten_code, SUM(数量) AS units
          FROM fact_returns
          WHERE モール='rakuten' AND substr(注文日,1,7) = ?
            AND モール商品コード IN (SELECT DISTINCT rakuten_code FROM silver)
          GROUP BY 注文日, モール商品コード
        )
        SELECT
          COUNT(*) AS pair_count,
          SUM(ret.units) AS lost_units
        FROM ret LEFT JOIN silver
          ON silver.date_jst = ret.date_jst AND silver.rakuten_code = ret.rakuten_code
        WHERE silver.rakuten_code IS NULL
      `).get(monthStr, monthStr);

      console.log('');
      console.log('  --- validation: fact_returns との突合 (D 案前提) ---');
      console.log(`    fact_returns rows:        ${returns.row_count?.toLocaleString()}`);
      console.log(`    fact_returns 数量合計:    ${returns.sum_units_returned?.toLocaleString()}`);
      console.log(`    daily fact units_cancelled: ${summary.sum_units_cancelled?.toLocaleString()}`);
      const unitsDiff = (summary.sum_units_cancelled || 0) - (returns.sum_units_returned || 0);
      console.log(`    units 差分: ${unitsDiff}`);
      console.log(`    fact_returns 返金額:      ¥${returns.sum_refund?.toLocaleString()}`);
      console.log(`    daily fact refund_amount: ¥${summary.sum_refund?.toLocaleString()}`);
      console.log(`    負値ガード発動 (units_cancelled > units_ordered) 件数: ${negCount}`);

      console.log('');
      console.log('  --- Phase 1b 按分対象モニタリング (refund date != sales date) ---');
      console.log(`    日付不一致 (date, code) ペア数: ${dateMismatchSkus.pair_count}`);
      console.log(`    回収できなかった cancel units:  ${dateMismatchSkus.lost_units || 0}`);
      console.log(`    → Phase 1b 「同月・同 rakuten_code 按分」で回収予定`);
      console.log(`    → 設計メモ: g:/共有ドライブ/AI_reference/システム設計/楽天Phase1b案_C-lite検討メモ_20260509.md`);
    } catch (e) {
      console.log('  (fact_returns validation スキップ:', e.message, ')');
    }
  }

  console.log('\n✓ Build complete');
  process.exit(0);
} catch (e) {
  console.error('FATAL:', e.message);
  console.error(e.stack);
  process.exit(1);
} finally {
  db.close();
}
