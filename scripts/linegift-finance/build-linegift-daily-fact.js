#!/usr/bin/env node
/**
 * f_linegift_finance_sku_daily_v1 build entrypoint — LINEギフト Phase 1 A-2
 *
 * - DDL:       sql/linegift/f_linegift_finance_sku_daily_v1.sql
 * - Build SQL: sql/linegift/build_f_linegift_finance_sku_daily_v1.sql
 * - 設計書:    g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.5_20260515.md
 *
 * 不変条件 (au PAY / 楽天 / Yahoo Phase 1 継承):
 *   - 既存 (date_jst, sku_code) は UPSERT で snapshot 列以外を更新 (snapshot 不変)
 *   - cogs / variable_margin は「既存 snapshot 原価 × 新 units_net_sold」で再計算
 *
 * LINEギフト 特徴 (設計書 v0.5):
 *   - 月次 rolling rebuild: --month 指定月 + 前 2 ヶ月 (status flip / cost snapshot 補完のため)
 *   - 按分なし (1注文1明細、stock_count を SUM するだけ)
 *   - mall_fee は API 実額 (raw.fee)、calc_method='actual_api' 固定
 *   - shipping は raw.shipping_fee (現状 NULL → 0)、quality='no_shipping_in_api' 想定
 *   - margin_confidence='provisional_full_candidate' (Phase A、4 要素 = gross - cogs - mall_fee - shipping)
 *   - 90日境界 frozen horizon: build SQL 内で Step 3.5 (raw freeze first) → Step 3.6 (fact orphan delete) を実行
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data \
 *     node scripts/linegift-finance/build-linegift-daily-fact.js --month 2026-05
 *   --dry-run でシミュレーション
 *   --single-month で rolling rebuild 無効化 (指定月のみ build)
 */
import Database from 'better-sqlite3';
import path from 'node:path';
import fs from 'node:fs';

const args = process.argv.slice(2);
function getArg(flag) {
  const i = args.indexOf(flag);
  return i >= 0 && i < args.length - 1 ? args[i + 1] : null;
}
const dataDir = (getArg('--data-dir') || process.env.DATA_DIR || '').trim();
const monthStr = getArg('--month');
const dryRun = args.includes('--dry-run');
const singleMonth = args.includes('--single-month');

if (!dataDir) {
  console.error('FATAL: --data-dir or DATA_DIR is required (process.cwd() fallback で worktree に stray DB が出来る事故防止)');
  process.exit(2);
}
if (!monthStr || !/^\d{4}-\d{2}$/.test(monthStr)) {
  console.error('FATAL: --month YYYY-MM is required (例: --month 2026-05)');
  process.exit(2);
}
const dbPath = path.join(dataDir, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const buildDate = new Date().toISOString().slice(0, 10);

// 月次 rolling: 指定月 + 前 2 ヶ月 (古い順に build)
function prevMonths(ym, n) {
  const [y, m] = ym.split('-').map(Number);
  const out = [];
  for (let i = n; i >= 0; i--) {
    let yy = y, mm = m - i;
    while (mm <= 0) { mm += 12; yy -= 1; }
    out.push(`${yy}-${String(mm).padStart(2, '0')}`);
  }
  return out;
}
const targetMonths = singleMonth ? [monthStr] : prevMonths(monthStr, 2);

// repo root
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '..', '..');
const ddlPath = getArg('--ddl-path') || path.join(repoRoot, 'sql', 'linegift', 'f_linegift_finance_sku_daily_v1.sql');
const buildSqlPath = getArg('--build-sql-path') || path.join(repoRoot, 'sql', 'linegift', 'build_f_linegift_finance_sku_daily_v1.sql');
for (const p of [ddlPath, buildSqlPath]) {
  if (!fs.existsSync(p)) { console.error(`FATAL: SQL file not found at ${p}`); process.exit(2); }
}
const ddlSql = fs.readFileSync(ddlPath, 'utf8');
const rawBuildSql = fs.readFileSync(buildSqlPath, 'utf8');

console.log('=== f_linegift_finance_sku_daily_v1 build (Phase 1 A-2) ===');
console.log(`DB: ${dbPath}`);
console.log(`Target months: ${targetMonths.join(', ')}${singleMonth ? ' (single-month mode)' : ' (rolling: 指定月+前2か月)'}`);
console.log(`Build date: ${buildDate}`);
console.log(`Dry run: ${dryRun}`);
console.log('');

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 30000');

try {
  // 1. DDL
  console.log('--- 1. DDL exec ---');
  db.exec(ddlSql);  console.log('  ✓ f_linegift_finance_sku_daily_v1 + views applied');

  // build SQL を ; で分割 (コメントのみ statement は除外、au PAY 同型)
  const statements = rawBuildSql.split(';').map(s => s.trim()).filter(s => {
    if (!s) return false;
    const codeOnly = s.split('\n').filter(l => !l.trim().startsWith('--') && l.trim() !== '').join('\n').trim();
    return codeOnly.length > 0;
  });
  console.log(`  Parsed ${statements.length} executable statements`);

  // 2. 各対象月を build (古い順)
  for (const ym of targetMonths) {
    const yearMonthInt = parseInt(ym.replace('-', ''), 10);
    const beforeCount = db.prepare("SELECT COUNT(*) AS c FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(ym).c;
    console.log(`\n--- 2. build ${ym} (year_month_int=${yearMonthInt}, 既存 ${beforeCount} 行) ---`);
    if (dryRun) { console.log(`  (dry-run) would execute ${statements.length} statements`); continue; }
    let totalChanges = 0;
    const tx = db.transaction(() => {
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
    tx.immediate();
    const afterCount = db.prepare("SELECT COUNT(*) AS c FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(ym).c;
    console.log(`  ✓ changes=${totalChanges.toLocaleString()}, rows for ${ym}: ${afterCount.toLocaleString()}`);
  }

  // 3. 主対象月 (monthStr) のサマリ
  if (!dryRun) {
    console.log(`\n--- 3. summary for ${monthStr} ---`);
    const s = db.prepare(`
      SELECT COUNT(*) AS rows, SUM(units_net_sold) AS units,
        ROUND(SUM(sales_principal_jpy_incl),0) AS principal,
        ROUND(SUM(cogs_amount_jpy_incl),0) AS cogs,
        ROUND(SUM(mall_fee_jpy_incl),0) AS mall_fee,
        ROUND(SUM(shipping_cost_jpy_incl),0) AS shipping,
        ROUND(SUM(variable_margin_jpy_incl),0) AS margin,
        SUM(CASE WHEN cost_status='complete' THEN 1 ELSE 0 END) AS complete_cnt,
        SUM(CASE WHEN cost_status='partial_cost' THEN 1 ELSE 0 END) AS partial_cnt,
        SUM(CASE WHEN cost_status='missing_cost' THEN 1 ELSE 0 END) AS missing_cnt,
        SUM(unresolved_sku_flag) AS unresolved_cnt,
        SUM(CASE WHEN resolution_method='master_match' THEN 1 ELSE 0 END) AS master_cnt,
        SUM(CASE WHEN resolution_method='parent_match' THEN 1 ELSE 0 END) AS parent_cnt,
        SUM(is_frozen_after_horizon) AS frozen_cnt,
        AVG(received_lag_days) AS avg_received_lag_days,
        AVG(delivered_lag_days) AS avg_delivered_lag_days
      FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
    `).get(monthStr);
    console.log(`  rows: ${s.rows?.toLocaleString()} / units: ${s.units?.toLocaleString()}`);
    console.log(`  monthly totals (税込):`);
    console.log(`    sales_principal:                 ¥${s.principal?.toLocaleString()}`);
    console.log(`    cogs (snapshot × units):        -¥${s.cogs?.toLocaleString()}`);
    console.log(`    mall_fee (API actual):          -¥${s.mall_fee?.toLocaleString()}`);
    console.log(`    shipping_cost (店負担):         -¥${s.shipping?.toLocaleString()}`);
    console.log(`    variable_margin (4 要素):        ¥${s.margin?.toLocaleString()}`);
    const pct = s.principal > 0 ? (s.margin / s.principal * 100).toFixed(2) : 'n/a';
    console.log(`    粗利率 (provisional_full_candidate): ${pct}%`);
    console.log(`  品質:`);
    console.log(`    cost_status complete / partial / missing: ${s.complete_cnt?.toLocaleString()} / ${s.partial_cnt?.toLocaleString()} / ${s.missing_cnt?.toLocaleString()}`);
    console.log(`    SKU 解決 master_match / parent_match / unresolved: ${s.master_cnt?.toLocaleString()} / ${s.parent_cnt?.toLocaleString()} / ${s.unresolved_cnt?.toLocaleString()}`);
    console.log(`    frozen_after_horizon: ${s.frozen_cnt?.toLocaleString()}`);
    console.log(`    avg lag (received/delivered): ${s.avg_received_lag_days?.toFixed(1) || 'n/a'} / ${s.avg_delivered_lag_days?.toFixed(1) || 'n/a'} 日`);

    // f_sales_by_listing 突合 (LINEギフト)
    try {
      const lst = db.prepare("SELECT ROUND(SUM(売上金額),0) AS sales, SUM(数量) AS units FROM f_sales_by_listing WHERE モール='linegift' AND substr(日付,1,7)=?").get(monthStr);
      const diff = (s.principal || 0) - (lst.sales || 0);
      console.log(`  --- f_sales_by_listing (linegift) 突合 ---`);
      console.log(`    listing 売上: ¥${lst.sales?.toLocaleString() || 0} / fact gross: ¥${s.principal?.toLocaleString() || 0} / diff: ¥${diff.toLocaleString()} (${lst.sales ? (Math.abs(diff)/lst.sales*100).toFixed(3) : 'n/a'}%)`);
    } catch (e) { console.log(`  (listing 突合スキップ: ${e.message})`); }

    // whitelist カバー率
    try {
      const cov = db.prepare(`
        SELECT (SELECT COUNT(*) FROM raw_linegift_orders WHERE substr(received_date_jst,1,7)=? OR (received_date_jst IS NULL AND substr(bought_date_jst,1,7)=?)) AS total,
               (SELECT COUNT(*) FROM raw_linegift_orders WHERE substr(received_date_jst,1,7)=? AND status='received') AS wl
      `).get(monthStr, monthStr, monthStr);
      console.log(`  --- whitelist カバー率 (received_date_jst 基準) ---`);
      console.log(`    対象 raw lines: ${cov.total?.toLocaleString()} / whitelist (received): ${cov.wl?.toLocaleString()} (${cov.total ? (cov.wl/cov.total*100).toFixed(2) : 'n/a'}%)`);
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
