#!/usr/bin/env node
/**
 * f_amazon_finance_sku_daily_v1 build entrypoint
 *
 * Phase 1 ticket: #1-1
 * - DDL: sql/amazon/f_amazon_finance_sku_daily_v1.sql
 * - Build SQL: sql/amazon/build_f_amazon_finance_sku_daily_v1.sql
 * - Mapping: docs/amazon-finance/canonical-metric-mapping.md (v1)
 *
 * 不変条件 (Codex Round 6 + Round 2 #1):
 *   - 既存 (date_jst, seller_sku) は UPSERT で snapshot 列以外を更新 (snapshot 不変)
 *   - rebuild 時の --force-rebuild は撤廃 (Codex Round 2 #1: snapshot を破壊するため)
 *     → UPSERT で「refund/adjustment が後追いで来た」「units が変わった」ケースは安全に扱える
 *     → snapshot 原価を本当にリセットしたい場合は別 ops スクリプトで明示的に行う
 *
 * 使い方:
 *   node scripts/amazon-finance/build-daily-fact.js --data-dir <DATA_DIR> --month 2026-04
 *   node scripts/amazon-finance/build-daily-fact.js --data-dir <DATA_DIR> --month 2026-04 --dry-run
 */

import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

// ============================================================
// CLI args parsing
// ============================================================
const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}
const dataDir = (getArg('--data-dir') || process.env.DATA_DIR || '').trim();
const monthStr = getArg('--month'); // 'YYYY-MM' format
const dryRun = args.includes('--dry-run');
// 旧 --force-rebuild は snapshot 破壊リスクで撤廃 (Codex Round 2 #1)。
// 互換のため受け取りはするが警告のみ、実動作は通常 build と同じ。
const forceRebuildLegacy = args.includes('--force-rebuild');
if (forceRebuildLegacy) {
  console.warn('  ⚠ --force-rebuild は撤廃されました (snapshot 不変条件保護のため)。');
  console.warn('  ⚠ UPSERT で「数量変動 / refund 後追い」は通常 build で安全に扱えます。');
  console.warn('  ⚠ 本当に snapshot 原価をリセットしたい場合は別 ops スクリプトを使用してください。');
}

if (!dataDir) {
  console.error('FATAL: --data-dir or DATA_DIR is required.');
  console.error('  例: node scripts/amazon-finance/build-daily-fact.js --data-dir C:/Users/bfaith/bfaith-portal/data --month 2026-04');
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

console.log('=== f_amazon_finance_sku_daily_v1 build ===');
console.log(`DB: ${dbPath}`);
console.log(`Month: ${monthStr} (year_month_int=${yearMonthInt})`);
console.log(`Build date: ${buildDate}`);
console.log(`Dry run: ${dryRun}`);
console.log('');

// ============================================================
// SQL files load
// ============================================================
// CLI で --ddl-path / --build-sql-path 指定可、未指定なら repo 構造想定で path 解決
const cliDdlPath = getArg('--ddl-path');
const cliBuildSqlPath = getArg('--build-sql-path');
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '..', '..');
const ddlPath = cliDdlPath || path.join(repoRoot, 'sql', 'amazon', 'f_amazon_finance_sku_daily_v1.sql');
const buildSqlPath = cliBuildSqlPath || path.join(repoRoot, 'sql', 'amazon', 'build_f_amazon_finance_sku_daily_v1.sql');

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

// build SQL の :year_month_int / :build_date は better-sqlite3 の named parameter で渡す
// ただし build SQL 内の WITH 構造のため、prepare するには 1 つの statement にする必要あり
// SQL ファイル先頭の行コメントは除外

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
    SELECT COUNT(*) AS c FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
  `).get(monthStr).c;
  console.log(`  Existing rows for ${monthStr}: ${beforeCount.toLocaleString()}`);

  // ============================================================
  // 3. build SQL 実行 (silver dedup temp + UPSERT、Codex Round 2 #2 対応で 1 tx 化)
  // ============================================================
  console.log('--- 3. build SQL exec (silver dedup + UPSERT、tx) ---');
  // build SQL は複数 statement (DROP TABLE / CREATE TEMP / INSERT...ON CONFLICT / CREATE INDEX / DROP TABLE)
  // bind パラメータを含むのは CREATE TEMP TABLE と INSERT 文のみ
  // 全 statement を db.transaction(...).immediate() で原子化 (途中失敗時のデータ整合性保護)

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
    let totalInserted = 0;
    const runBuildTx = db.transaction(() => {
      for (const sql of statements) {
        const hasParams = sql.includes(':year_month_int') || sql.includes(':build_date');
        if (hasParams) {
          const stmt = db.prepare(sql);
          const info = stmt.run({ year_month_int: yearMonthInt, build_date: buildDate });
          if (info.changes > 0) totalInserted += info.changes;
        } else {
          db.exec(sql);
        }
      }
    });
    runBuildTx.immediate();  // BEGIN IMMEDIATE で write lock 即取得 (cross-process 安全)
    console.log(`  ✓ Total inserted/changed rows: ${totalInserted.toLocaleString()}`);
  }

  // ============================================================
  // 4. 結果サマリー
  // ============================================================
  console.log('--- 4. result summary ---');
  const afterCount = db.prepare(`
    SELECT COUNT(*) AS c FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
  `).get(monthStr).c;
  console.log(`  After build: ${afterCount.toLocaleString()} rows for ${monthStr}`);

  if (!dryRun && afterCount > 0) {
    const summary = db.prepare(`
      SELECT
        COUNT(*) AS row_count,
        ROUND(SUM(units_ordered), 0) AS sum_units_ordered,
        ROUND(SUM(units_net_sold), 0) AS sum_units_net_sold,
        ROUND(SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy), 0) AS sum_revenue_jpy,
        ROUND(SUM(commission_jpy + fba_fulfillment_jpy + fba_storage_jpy + shipping_chargeback_jpy + giftwrap_chargeback_jpy + promotion_jpy), 0) AS sum_fees_jpy,
        ROUND(SUM(refund_principal_jpy), 0) AS sum_refund_principal_jpy,
        ROUND(SUM(warehouse_damage_jpy + warehouse_lost_jpy + safe_t_jpy + reversal_reimbursement_jpy), 0) AS sum_reimbursement_jpy,
        ROUND(SUM(cogs_amount), 0) AS sum_cogs_jpy,
        ROUND(SUM(profit_amount), 0) AS sum_profit_jpy,
        SUM(CASE WHEN cost_status = 'complete' THEN 1 ELSE 0 END) AS complete_count,
        SUM(CASE WHEN cost_status = 'missing_cost' THEN 1 ELSE 0 END) AS missing_count,
        SUM(CASE WHEN cost_status = 'partial_cost' THEN 1 ELSE 0 END) AS partial_count
      FROM f_amazon_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?
    `).get(monthStr);
    console.log('  monthly totals:');
    console.log(`    revenue (principal+shipping+giftwrap): ${summary.sum_revenue_jpy?.toLocaleString()} 円`);
    console.log(`    fees: ${summary.sum_fees_jpy?.toLocaleString()} 円`);
    console.log(`    refund_principal: ${summary.sum_refund_principal_jpy?.toLocaleString()} 円`);
    console.log(`    reimbursement: ${summary.sum_reimbursement_jpy?.toLocaleString()} 円`);
    console.log(`    cogs: ${summary.sum_cogs_jpy?.toLocaleString()} 円`);
    console.log(`    profit_amount: ${summary.sum_profit_jpy?.toLocaleString()} 円`);
    console.log(`    units_ordered: ${summary.sum_units_ordered?.toLocaleString()}`);
    console.log(`    units_net_sold: ${summary.sum_units_net_sold?.toLocaleString()}`);
    console.log(`  cost_status:`);
    console.log(`    complete: ${summary.complete_count?.toLocaleString()}`);
    console.log(`    missing_cost: ${summary.missing_count?.toLocaleString()}`);
    console.log(`    partial_cost: ${summary.partial_count?.toLocaleString()}`);

    // v4 比較 (Codex Round 2 #3: gross_margin_with_reimbursement_excl_tax を validation 対象に)
    try {
      const v4 = db.prepare(`
        SELECT
          ROUND(SUM(qty_net_sold), 0) AS v4_qty_net_sold,
          ROUND(SUM(sales_principal_jpy + sales_shipping_jpy + sales_giftwrap_jpy), 0) AS v4_revenue_jpy,
          ROUND(SUM(cogs_excl_tax), 0) AS v4_cogs_jpy,
          ROUND(SUM(gross_margin_with_reimbursement_excl_tax), 0) AS v4_profit_jpy
        FROM v_amazon_sku_profit_actual_v4
        WHERE year_month_int = ?
      `).get(yearMonthInt);
      const profitDiff = (summary.sum_profit_jpy || 0) - (v4.v4_profit_jpy || 0);
      const profitDiffPct = v4.v4_profit_jpy ? Math.abs(profitDiff) / Math.abs(v4.v4_profit_jpy) * 100 : null;
      console.log('');
      console.log(`  --- v4 比較 (validation reference) ---`);
      console.log(`    daily revenue:  ${summary.sum_revenue_jpy?.toLocaleString()} vs v4: ${v4.v4_revenue_jpy?.toLocaleString()} (diff: ${(summary.sum_revenue_jpy - v4.v4_revenue_jpy).toLocaleString()})`);
      console.log(`    daily cogs:     ${summary.sum_cogs_jpy?.toLocaleString()} vs v4: ${v4.v4_cogs_jpy?.toLocaleString()} (diff: ${(summary.sum_cogs_jpy - v4.v4_cogs_jpy).toLocaleString()})`);
      console.log(`    daily profit:   ${summary.sum_profit_jpy?.toLocaleString()} vs v4: ${v4.v4_profit_jpy?.toLocaleString()} (diff: ${profitDiff.toLocaleString()}, ${profitDiffPct?.toFixed(3)}%)`);
      console.log(`    daily units:    ${summary.sum_units_net_sold?.toLocaleString()} vs v4: ${v4.v4_qty_net_sold?.toLocaleString()}`);
    } catch (e) {
      console.log('  (v4 比較スキップ:', e.message, ')');
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
