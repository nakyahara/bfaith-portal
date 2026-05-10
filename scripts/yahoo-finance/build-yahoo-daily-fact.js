#!/usr/bin/env node
/**
 * f_yahoo_finance_sku_daily_v1 build entrypoint
 *
 * Yahoo Phase 1 ticket: Y-1
 * - DDL: sql/yahoo/f_yahoo_finance_sku_daily_v1.sql
 * - Map DDL: sql/yahoo/f_yahoo_sku_map.sql
 * - Build SQL: sql/yahoo/build_f_yahoo_finance_sku_daily_v1.sql
 * - 設計書: g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1a設計書_v0.4_20260510.md
 *
 * 不変条件 (楽天 Phase 1a/1b から継承):
 *   - 既存 (date_jst, yahoo_sku_key) は UPSERT で snapshot 列以外を更新 (snapshot 不変)
 *   - cogs / variable_margin_partial は「既存 snapshot 原価 × 新 units_net_sold」で再計算
 *
 * Phase 1a 特徴 (楽天と違う点):
 *   - 月次フルリビルド loop なし (按分なしなので増分 UPSERT で OK)
 *   - units_cancelled = 0 (Phase 1c-3 で fact_returns 投入後に再計算)
 *   - partial margin (6 要素) のみ計上、full margin は Phase 1c-3 で backfill
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data \
 *     node scripts/yahoo-finance/build-yahoo-daily-fact.js --month 2026-04
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

console.log('=== f_yahoo_finance_sku_daily_v1 build (Phase 1a) ===');
console.log(`DB: ${dbPath}`);
console.log(`Month: ${monthStr} (year_month_int=${yearMonthInt})`);
console.log(`Build date: ${buildDate}`);
console.log(`Dry run: ${dryRun}`);
console.log('');

// ============================================================
// SQL files load
// ============================================================
const cliDdlPath = getArg('--ddl-path');
const cliMapDdlPath = getArg('--map-ddl-path');
const cliBuildSqlPath = getArg('--build-sql-path');
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '..', '..');
const ddlPath = cliDdlPath || path.join(repoRoot, 'sql', 'yahoo', 'f_yahoo_finance_sku_daily_v1.sql');
const mapDdlPath = cliMapDdlPath || path.join(repoRoot, 'sql', 'yahoo', 'f_yahoo_sku_map.sql');
const buildSqlPath = cliBuildSqlPath || path.join(repoRoot, 'sql', 'yahoo', 'build_f_yahoo_finance_sku_daily_v1.sql');

for (const p of [ddlPath, mapDdlPath, buildSqlPath]) {
  if (!fs.existsSync(p)) {
    console.error(`FATAL: SQL file not found at ${p}`);
    process.exit(2);
  }
}

const ddlSql = fs.readFileSync(ddlPath, 'utf8');
const mapDdlSql = fs.readFileSync(mapDdlPath, 'utf8');
const rawBuildSql = fs.readFileSync(buildSqlPath, 'utf8');

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 30000');

try {
  // ============================================================
  // 1. DDL 実行 (sku_map + main fact + view)
  // ============================================================
  console.log('--- 1. DDL exec ---');
  db.exec(mapDdlSql);
  console.log('  ✓ f_yahoo_sku_map applied');
  db.exec(ddlSql);
  console.log('  ✓ f_yahoo_finance_sku_daily_v1 + views applied');

  // Phase 1c-3 用 migration framework (現状追加列なし、将来 ALTER TABLE 追加時用)
  // 楽天 Phase 1b で確立した PRAGMA table_info 方式と同型 (Codex R5 #1 反映)
  const existingCols = new Set(
    db.prepare("PRAGMA table_info(f_yahoo_finance_sku_daily_v1)").all().map(c => c.name)
  );
  // Phase 1a 時点で全列 DDL に含まれてるので追加なし
  // Phase 1c-3 着手時にここに以下を追加:
  //   { name: 'settlement_fee_jpy_incl', def: 'REAL' }
  //   { name: 'psr_jpy_incl', def: 'REAL' }
  //   { name: 'ad_spend_jpy_incl', def: 'REAL' }
  //   ... (full margin 用列)

  // ============================================================
  // 2. 対象月の既存 row 数
  // ============================================================
  const beforeCount = db.prepare(`
    SELECT COUNT(*) AS c FROM f_yahoo_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
  `).get(monthStr).c;
  console.log(`  Existing rows for ${monthStr}: ${beforeCount.toLocaleString()}`);

  // ============================================================
  // 3. build SQL 実行 (CTE A→B→C→D + UPSERT、tx)
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
  // 4. 結果サマリ (target month)
  // ============================================================
  console.log(`\n--- 4. result summary for target month ${monthStr} ---`);
  const afterCount = db.prepare(`
    SELECT COUNT(*) AS c FROM f_yahoo_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
  `).get(monthStr).c;
  console.log(`  Total rows: ${afterCount.toLocaleString()}`);

  if (!dryRun && afterCount > 0) {
    const summary = db.prepare(`
      SELECT
        COUNT(*) AS row_count,
        SUM(units_ordered) AS sum_units_ordered,
        SUM(units_net_sold) AS sum_units_net_sold,
        ROUND(SUM(sales_principal_jpy_incl), 0) AS sum_principal,
        ROUND(SUM(coupon_shop_jpy_incl), 0) AS sum_coupon,
        ROUND(SUM(use_point_jpy_incl), 0) AS sum_use_point,
        ROUND(SUM(net_sales_before_point_jpy_incl), 0) AS sum_net_before_point,
        ROUND(SUM(listing_sales_estimated_jpy_incl), 0) AS sum_listing_est,
        ROUND(SUM(mall_fee_jpy_incl), 0) AS sum_mall_fee,
        ROUND(SUM(shipping_cost_jpy_incl), 0) AS sum_shipping,
        ROUND(SUM(cogs_amount_jpy_incl), 0) AS sum_cogs,
        ROUND(SUM(variable_margin_partial_jpy_incl), 0) AS sum_partial_margin,
        SUM(CASE WHEN cost_status = 'complete' THEN 1 ELSE 0 END) AS complete_count,
        SUM(CASE WHEN cost_status = 'missing_cost' THEN 1 ELSE 0 END) AS missing_count,
        SUM(unresolved_sku_flag) AS unresolved_count,
        SUM(CASE WHEN resolution_method = 'sub_match' THEN 1 ELSE 0 END) AS sub_match_count,
        SUM(CASE WHEN resolution_method = 'parent_match' THEN 1 ELSE 0 END) AS parent_match_count,
        SUM(CASE WHEN resolution_method = 'manual_map' THEN 1 ELSE 0 END) AS manual_map_count,
        SUM(price_variance_warning) AS price_variance_count,
        SUM(CASE WHEN shipping_quality = 'missing' THEN 1 ELSE 0 END) AS shipping_missing_count
      FROM f_yahoo_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
    `).get(monthStr);

    console.log('  monthly totals (税込):');
    console.log(`    sales_principal:                       ¥${summary.sum_principal?.toLocaleString()}`);
    console.log(`    coupon_shop (商品単位):                 -¥${summary.sum_coupon?.toLocaleString()}`);
    console.log(`    use_point (お客様 PT):                  -¥${summary.sum_use_point?.toLocaleString()}`);
    console.log(`    net_sales_before_point:                ¥${summary.sum_net_before_point?.toLocaleString()}`);
    console.log(`    listing_sales_estimated:               ¥${summary.sum_listing_est?.toLocaleString()} (← f_sales_by_listing 突合 reference)`);
    console.log(`    cogs (snapshot × units_net_sold):      -¥${summary.sum_cogs?.toLocaleString()}`);
    console.log(`    mall_fee (estimated 10%):              -¥${summary.sum_mall_fee?.toLocaleString()}`);
    console.log(`    shipping_cost (店負担):                -¥${summary.sum_shipping?.toLocaleString()}`);
    console.log(`    variable_margin_partial (6 要素):       ¥${summary.sum_partial_margin?.toLocaleString()}`);
    const margin_pct = summary.sum_principal > 0
      ? (summary.sum_partial_margin / summary.sum_principal * 100).toFixed(2)
      : 'n/a';
    console.log(`    粗利率 partial (margin / principal):    ${margin_pct}% (= 暫定値、Phase 1c-3 で full に補正)`);
    console.log(`  units:`);
    console.log(`    ordered:    ${summary.sum_units_ordered?.toLocaleString()}`);
    console.log(`    net_sold:   ${summary.sum_units_net_sold?.toLocaleString()} (= ordered, Phase 1c-3 で fact_returns 控除)`);
    console.log(`  品質:`);
    console.log(`    cost_status complete:                ${summary.complete_count?.toLocaleString()}`);
    console.log(`    cost_status missing_cost:            ${summary.missing_count?.toLocaleString()}`);
    console.log(`    SKU 解決 sub_match (CI):             ${summary.sub_match_count?.toLocaleString()} (variant 別、case-insensitive)`);
    console.log(`    SKU 解決 parent_match (CI):          ${summary.parent_match_count?.toLocaleString()} (item_id / 単品)`);
    console.log(`    SKU 解決 manual_map (CI):            ${summary.manual_map_count?.toLocaleString()}`);
    console.log(`    unresolved (variant 粒度保持):        ${summary.unresolved_count?.toLocaleString()}`);
    console.log(`    price_variance_warning:              ${summary.price_variance_count?.toLocaleString()}`);
    console.log(`    shipping_quality missing:            ${summary.shipping_missing_count?.toLocaleString()}`);

    // ============================================================
    // 5. validation: f_sales_by_listing 突合 (Phase 1c-3 で MF 突合に発展)
    // ============================================================
    try {
      const listing = db.prepare(`
        SELECT
          COUNT(*) AS row_count,
          ROUND(SUM(売上金額), 0) AS listing_sales,
          SUM(数量) AS listing_units
        FROM f_sales_by_listing
        WHERE モール = 'yahoo' AND substr(日付, 1, 7) = ?
      `).get(monthStr);
      console.log('');
      console.log('  --- validation: f_sales_by_listing (Yahoo) との突合 ---');
      console.log(`    listing rows:                ${listing.row_count?.toLocaleString()}`);
      console.log(`    listing 売上 (税込):          ¥${listing.listing_sales?.toLocaleString()}`);
      console.log(`    daily fact gross_sales:      ¥${summary.sum_principal?.toLocaleString()}`);
      console.log(`    daily fact listing_estimated: ¥${summary.sum_listing_est?.toLocaleString()}`);
      const diff_gross = (summary.sum_principal || 0) - (listing.listing_sales || 0);
      const diff_est = (summary.sum_listing_est || 0) - (listing.listing_sales || 0);
      console.log(`    diff (gross - listing):      ¥${diff_gross.toLocaleString()} (${listing.listing_sales ? (Math.abs(diff_gross) / listing.listing_sales * 100).toFixed(3) : 'n/a'}%)`);
      console.log(`    diff (estimated - listing):  ¥${diff_est.toLocaleString()} (${listing.listing_sales ? (Math.abs(diff_est) / listing.listing_sales * 100).toFixed(3) : 'n/a'}%)`);
      console.log(`    listing 数量:                ${listing.listing_units?.toLocaleString()}`);
      console.log(`    daily fact units_ordered:    ${summary.sum_units_ordered?.toLocaleString()}`);
      console.log(`    → estimated と listing の差が小さければ「listing は use_point 控除後」仮説が正しい`);
      console.log(`    → 業務確認 TODO: Phase 1c-3 で MF 会計突合 (Codex R4 #5)`);
    } catch (e) {
      console.log('  (validation スキップ:', e.message, ')');
    }

    // ============================================================
    // 6. whitelist カバー率 (Phase 0a DQ メトリクス先取り)
    // ============================================================
    try {
      const coverage = db.prepare(`
        WITH all_orders AS (
          SELECT COUNT(*) AS total FROM raw_yahoo_orders WHERE substr(order_time, 1, 7) = ?
        ),
        whitelist AS (
          SELECT COUNT(*) AS wl FROM raw_yahoo_orders
          WHERE substr(order_time, 1, 7) = ?
            AND order_status = '5' AND pay_status = '1' AND ship_status = '3'
        )
        SELECT total, wl, ROUND(wl * 100.0 / total, 2) AS pct
        FROM all_orders, whitelist
      `).get(monthStr, monthStr);
      console.log('');
      console.log(`  --- whitelist カバー率 (order=5 pay=1 ship=3) ---`);
      console.log(`    全 raw lines: ${coverage.total?.toLocaleString()} / whitelist: ${coverage.wl?.toLocaleString()} (${coverage.pct}%)`);
      console.log(`    → 90% 未満なら error、95% 未満で warn (Codex R2 確定閾値)`);
    } catch (e) { /* skip */ }
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
