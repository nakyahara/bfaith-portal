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

const buildDate = new Date().toISOString().slice(0, 10);

// Phase 1b 月次フルリビルド (Codex 事前レビュー 2026-05-10):
// 月またぎ refund (例: 4/15 注文 → 5/3 cancel) を取り込むため、対象月 + 前 2 ヶ月を毎回再 build。
// 各月で「rakuten_code 単位の月集計按分」が走るため、過去月の数字も最新 fact_returns で更新される。
// --skip-back-months 1 で 2 ヶ月、0 で対象月のみ (CLI override)
function previousMonth(yyyymm, n) {
  const [y, m] = yyyymm.split('-').map(Number);
  const date = new Date(y, m - 1 - n, 1);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}
const skipBackArg = getArg('--skip-back-months');
const skipBackMonths = skipBackArg !== null ? parseInt(skipBackArg, 10) : 2; // default: 2 (= 3 ヶ月 build)
const monthsToBuild = [];
for (let i = skipBackMonths; i >= 0; i--) {
  monthsToBuild.push(previousMonth(monthStr, i));
}

console.log('=== f_rakuten_finance_sku_daily_v1 build (Phase 1b) ===');
console.log(`DB: ${dbPath}`);
console.log(`Target month: ${monthStr}`);
console.log(`Build months (Phase 1b 月またぎ refund 取り込み): ${monthsToBuild.join(', ')}`);
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
  // 1. DDL 実行 + Phase 1b migration (既存 table への新列追加)
  // ============================================================
  console.log('--- 1. DDL exec ---');
  db.exec(ddlSql);
  console.log('  ✓ DDL applied (CREATE TABLE IF NOT EXISTS)');

  // CREATE TABLE IF NOT EXISTS は既存 table に新列を追加しない。
  // Phase 1b で追加した列を既存 table に ALTER TABLE で migrate する (idempotent)
  const existingCols = new Set(
    db.prepare("PRAGMA table_info(f_rakuten_finance_sku_daily_v1)").all().map(c => c.name)
  );
  const phase1bColumns = [
    { name: 'allocated_units_cancelled',         def: 'INTEGER NOT NULL DEFAULT 0' },
    { name: 'units_cancelled_same_day_matched',  def: 'INTEGER NOT NULL DEFAULT 0' },
    { name: 'allocation_method',                 def: "TEXT NOT NULL DEFAULT 'no_refund'" },
    { name: 'cancel_exceeds_ordered_warning',    def: 'INTEGER NOT NULL DEFAULT 0' },
    { name: 'allocated_refund_amount_jpy_incl',  def: 'REAL NOT NULL DEFAULT 0' },
    { name: 'refund_amount_same_day_matched_jpy_incl', def: 'REAL NOT NULL DEFAULT 0' },
  ];
  for (const c of phase1bColumns) {
    if (!existingCols.has(c.name)) {
      console.log(`  Migrating: ALTER TABLE ADD COLUMN ${c.name}`);
      db.exec(`ALTER TABLE f_rakuten_finance_sku_daily_v1 ADD COLUMN ${c.name} ${c.def}`);
    }
  }

  // ============================================================
  // 2. build SQL parse (1 回、loop 内で再利用)
  // ============================================================
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

  // ============================================================
  // 3. Phase 1b 月 loop build (各月で UPSERT、snapshot 温存)
  // ============================================================
  for (const m of monthsToBuild) {
    const ymInt = parseInt(m.replace('-', ''), 10);
    const beforeCount = db.prepare(`
      SELECT COUNT(*) AS c FROM f_rakuten_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
    `).get(m).c;

    console.log(`\n--- Build ${m} (year_month_int=${ymInt}, existing=${beforeCount.toLocaleString()}) ---`);

    if (dryRun) {
      console.log(`  (dry-run) would execute ${statements.length} statements`);
      continue;
    }

    let totalChanges = 0;
    const runBuildTx = db.transaction(() => {
      for (const sql of statements) {
        const hasParams = sql.includes(':year_month_int') || sql.includes(':build_date');
        if (hasParams) {
          const stmt = db.prepare(sql);
          const info = stmt.run({ year_month_int: ymInt, build_date: buildDate });
          if (info.changes > 0) totalChanges += info.changes;
        } else {
          db.exec(sql);
        }
      }
    });
    runBuildTx.immediate();

    const afterCount = db.prepare(`
      SELECT COUNT(*) AS c FROM f_rakuten_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
    `).get(m).c;
    console.log(`  ✓ Inserted/changed: ${totalChanges.toLocaleString()} rows、After build: ${afterCount.toLocaleString()} rows for ${m}`);
  }

  // ============================================================
  // 4. 結果サマリ (target month のみ)
  // ============================================================
  console.log(`\n--- 4. result summary for target month ${monthStr} ---`);
  const afterCount = db.prepare(`
    SELECT COUNT(*) AS c FROM f_rakuten_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
  `).get(monthStr).c;
  console.log(`  Total rows: ${afterCount.toLocaleString()}`);

  if (!dryRun && afterCount > 0) {
    const summary = db.prepare(`
      SELECT
        COUNT(*) AS row_count,
        SUM(units_ordered) AS sum_units_ordered,
        SUM(units_cancelled) AS sum_units_cancelled,
        SUM(units_net_sold) AS sum_units_net_sold,
        SUM(allocated_units_cancelled) AS sum_allocated_cancelled,
        SUM(units_cancelled_same_day_matched) AS sum_same_day_matched,
        SUM(cancel_exceeds_ordered_warning) AS sum_warning,
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
    console.log(`  units (Phase 1b 按分後):`);
    console.log(`    ordered:                    ${summary.sum_units_ordered?.toLocaleString()}`);
    console.log(`    cancelled (allocated):      ${summary.sum_units_cancelled?.toLocaleString()}`);
    console.log(`    net_sold:                   ${summary.sum_units_net_sold?.toLocaleString()}`);
    console.log(`  Phase 1b allocation audit:`);
    console.log(`    allocated_cancelled:        ${summary.sum_allocated_cancelled?.toLocaleString()}`);
    console.log(`    same_day_matched (audit):   ${summary.sum_same_day_matched?.toLocaleString()}`);
    console.log(`    cancel_exceeds_warning rows: ${summary.sum_warning?.toLocaleString()}`);
    console.log(`  品質:`);
    console.log(`    cost_status complete:           ${summary.complete_count?.toLocaleString()}`);
    console.log(`    cost_status missing_cost:       ${summary.missing_count?.toLocaleString()}`);
    console.log(`    price_variance_warning:         ${summary.price_variance_count?.toLocaleString()}`);
    console.log(`    shipping_quality missing:       ${summary.shipping_missing_count?.toLocaleString()}`);

    // Phase 1b 月合計一致 check (rakuten_code 単位、SUM(allocated) = month_total_cancelled)
    // Codex 事後レビュー指摘 #1: alloc 起点 LEFT JOIN だと returns-only code を見落とす
    // → ret_per_code 起点でも検出 (FULL OUTER 相当を UNION で実装)
    try {
      const allocCheck = db.prepare(`
        WITH alloc_per_code AS (
          SELECT rakuten_code, SUM(allocated_units_cancelled) AS sum_alloc,
                 SUM(allocated_refund_amount_jpy_incl) AS sum_alloc_refund
          FROM f_rakuten_finance_sku_daily_v1
          WHERE substr(date_jst, 1, 7) = ?
          GROUP BY rakuten_code
        ),
        ret_per_code AS (
          SELECT モール商品コード AS rakuten_code, SUM(数量) AS sum_ret,
                 SUM(返金額) AS sum_ret_refund
          FROM fact_returns
          WHERE モール = 'rakuten' AND 注文日 IS NOT NULL
            AND substr(注文日, 1, 7) = ?
          GROUP BY モール商品コード
        ),
        all_codes AS (
          SELECT rakuten_code FROM alloc_per_code
          UNION
          SELECT rakuten_code FROM ret_per_code
        )
        SELECT
          COUNT(*) AS total_codes,
          SUM(CASE WHEN a.sum_alloc IS NULL AND r.sum_ret > 0 THEN 1 ELSE 0 END) AS returns_only_codes,
          SUM(CASE WHEN a.sum_alloc IS NULL THEN COALESCE(r.sum_ret, 0) ELSE 0 END) AS returns_only_units,
          SUM(CASE WHEN a.sum_alloc IS NULL THEN COALESCE(r.sum_ret_refund, 0) ELSE 0 END) AS returns_only_refund,
          SUM(CASE WHEN COALESCE(a.sum_alloc, 0) <> COALESCE(r.sum_ret, 0) THEN 1 ELSE 0 END) AS mismatch_count,
          ROUND(SUM(COALESCE(a.sum_alloc, 0))) AS total_alloc,
          ROUND(SUM(COALESCE(r.sum_ret, 0))) AS total_ret,
          ROUND(SUM(COALESCE(a.sum_alloc_refund, 0)), 2) AS total_alloc_refund,
          ROUND(SUM(COALESCE(r.sum_ret_refund, 0)), 2) AS total_ret_refund,
          -- silver-coded のみ (returns-only 除く) で LRM 完全一致を確認
          ROUND(SUM(CASE WHEN a.sum_alloc IS NOT NULL THEN COALESCE(a.sum_alloc_refund, 0) ELSE 0 END), 2) AS silver_alloc_refund,
          ROUND(SUM(CASE WHEN a.sum_alloc IS NOT NULL THEN COALESCE(r.sum_ret_refund, 0) ELSE 0 END), 2) AS silver_ret_refund
        FROM all_codes c
        LEFT JOIN alloc_per_code a USING (rakuten_code)
        LEFT JOIN ret_per_code r USING (rakuten_code)
      `).get(monthStr, monthStr);
      console.log('');
      console.log(`  --- Phase 1b 按分一致 check (Codex post 指摘反映、returns-only 検出 + LRM verify) ---`);
      console.log(`    総 code 数 (silver ∪ fact_returns): ${allocCheck.total_codes}`);
      console.log(`    silver-coded 一致 (units):         ${allocCheck.total_alloc} = ${allocCheck.total_ret - (allocCheck.returns_only_units || 0)} ${(allocCheck.mismatch_count - (allocCheck.returns_only_codes || 0)) === 0 ? '✓ LRM 完璧' : '⚠️ LRM 残差 bug'}`);
      const lrmRefundDiff = (allocCheck.silver_alloc_refund || 0) - (allocCheck.silver_ret_refund || 0);
      console.log(`    silver-coded 一致 (refund):        ¥${allocCheck.silver_alloc_refund?.toLocaleString()} vs ¥${allocCheck.silver_ret_refund?.toLocaleString()} (差分 ¥${lrmRefundDiff.toFixed(2)}) ${Math.abs(lrmRefundDiff) <= 1 ? '✓ LRM ¥1 以内' : '⚠️ ' + Math.abs(lrmRefundDiff).toFixed(0) + '円ズレ'}`);
      console.log(`    returns-only (silver にない、按分対象外): ${allocCheck.returns_only_codes || 0} code / ${allocCheck.returns_only_units || 0} units / ¥${allocCheck.returns_only_refund?.toLocaleString() || 0}`);
      console.log(`      → status=900 完全キャンセル等。Phase 1a/1b 設計仕様内 (売上 fact に立てない注文を控除しない)`);
    } catch (e) {
      console.log('  (allocation check スキップ:', e.message, ')');
    }

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
      console.log('  --- 旧 Phase 1a LEFT JOIN 漏れ (Phase 1b 按分で救済済、audit 表示) ---');
      console.log(`    日付不一致 (date, code) ペア数: ${dateMismatchSkus.pair_count}`);
      console.log(`    旧 Phase 1a で漏れた units:     ${dateMismatchSkus.lost_units || 0}`);
      console.log(`    → Phase 1b 月次按分で救済済 (allocated 列に反映)`);
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
