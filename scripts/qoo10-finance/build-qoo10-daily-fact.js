#!/usr/bin/env node
/**
 * f_qoo10_finance_sku_daily_v1 build entrypoint — Qoo10 Phase 1 A-2
 *
 * - DDL:       sql/qoo10/f_qoo10_finance_sku_daily_v1.sql
 * - Build SQL: sql/qoo10/build_f_qoo10_finance_sku_daily_v1.sql
 * - 設計書:    g:/共有ドライブ/AI_reference/システム設計/Qoo10Phase1設計書_v0.11_20260518.md §6-2 §7
 *
 * 不変条件 (au PAY / 楽天 / Yahoo / LINEギフト Phase 1 継承):
 *   - 既存 (date_jst, sku_code) は UPSERT で snapshot 列以外を更新 (snapshot 不変)
 *   - cogs / variable_margin は「既存 snapshot 原価 × 新 units_net_sold」で再計算
 *
 * Qoo10 特徴 (設計書 v0.11):
 *   - 月次 rolling rebuild: --month 指定月 + 前 2 ヶ月 (status flip / cost snapshot 補完のため)
 *   - 按分なし (1 orderNo 1 明細、order_qty を SUM するだけ)
 *   - whitelist = 'Delivered(5)' AND legacy_fields_missing = 0 (旧 raw 17,252 行は fact 対象外)
 *   - mall_fee は SUM(gmv_list - settle_price) (API 自動計算、calc_method='actual_api')
 *   - shipping は raw.shipping_rate (現状 0)、quality='no_shipping_in_api' 想定
 *   - margin_confidence='partial_pending_settlement_csv' (Phase A、メガ割ショップ負担は Phase B-2)
 *   - メガ割/メガポ/other_promo は行レベル判別 → 別列集計
 *   - settle_price_formula_scope: domestic_non_cod / oversea / cod / mixed 行レベル判別
 *   - 90日境界 frozen horizon: build SQL 内で Step 0 (raw freeze first, order_date 基準 76d) → Step 3.6 (fact orphan delete)
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data \
 *     node scripts/qoo10-finance/build-qoo10-daily-fact.js --month 2026-05
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
const ddlPath = getArg('--ddl-path') || path.join(repoRoot, 'sql', 'qoo10', 'f_qoo10_finance_sku_daily_v1.sql');
const buildSqlPath = getArg('--build-sql-path') || path.join(repoRoot, 'sql', 'qoo10', 'build_f_qoo10_finance_sku_daily_v1.sql');
for (const p of [ddlPath, buildSqlPath]) {
  if (!fs.existsSync(p)) { console.error(`FATAL: SQL file not found at ${p}`); process.exit(2); }
}
const ddlSql = fs.readFileSync(ddlPath, 'utf8');
const rawBuildSql = fs.readFileSync(buildSqlPath, 'utf8');

console.log('=== f_qoo10_finance_sku_daily_v1 build (Phase 1 A-2) ===');
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
  db.exec(ddlSql);  console.log('  ✓ f_qoo10_finance_sku_daily_v1 + views applied');

  // 2026-05-19 A-2 patch: 既存テーブルに match_tier 列がなければ ALTER TABLE で追加。
  // CREATE TABLE IF NOT EXISTS では既存テーブルへの列追加ができないため、PRAGMA で判定して migrate。
  // ⚠️ ALTER 直後の既存行は DEFAULT 'unresolved' で埋まるが、本 script は rolling 3 ヶ月を毎回 rebuild するため
  //    対象月の match_tier は UPSERT で正しく更新される。rolling 範囲外の月で既存行が残っている場合は
  //    --month YYYY-MM --single-month で 1 回追加 rebuild すれば良い (Codex R1 patch Medium #2 反映)。
  const existingCols = db.prepare("PRAGMA table_info(f_qoo10_finance_sku_daily_v1)").all().map(c => c.name);
  if (!existingCols.includes('match_tier')) {
    console.log('  → match_tier 列なし → ALTER TABLE ADD COLUMN (既存行は DEFAULT "unresolved"、rolling 月 rebuild で更新)');
    db.exec(`
      ALTER TABLE f_qoo10_finance_sku_daily_v1
      ADD COLUMN match_tier TEXT NOT NULL DEFAULT 'unresolved'
      CHECK (match_tier IN ('combined', 'option_only', 'seller_only', 'unresolved'))
    `);
    const orphanCount = db.prepare(`SELECT COUNT(*) AS c FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) NOT IN (${targetMonths.map(m => `'${m}'`).join(',')})`).get().c;
    console.log(`  ✓ match_tier column added (rolling 範囲外で 'unresolved' のまま残る行: ${orphanCount}、必要なら追加 rebuild)`);
  }

  // build SQL を ; で分割 (コメントのみ statement は除外、LINEギフト 同型)
  const statements = rawBuildSql.split(';').map(s => s.trim()).filter(s => {
    if (!s) return false;
    const codeOnly = s.split('\n').filter(l => !l.trim().startsWith('--') && l.trim() !== '').join('\n').trim();
    return codeOnly.length > 0;
  });
  console.log(`  Parsed ${statements.length} executable statements`);

  // 2. 各対象月を build (古い順)
  for (const ym of targetMonths) {
    const yearMonthInt = parseInt(ym.replace('-', ''), 10);
    const beforeCount = db.prepare("SELECT COUNT(*) AS c FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(ym).c;
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
    const afterCount = db.prepare("SELECT COUNT(*) AS c FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(ym).c;
    console.log(`  ✓ changes=${totalChanges.toLocaleString()}, rows for ${ym}: ${afterCount.toLocaleString()}`);
  }

  // 3. 主対象月 (monthStr) のサマリ
  if (!dryRun) {
    console.log(`\n--- 3. summary for ${monthStr} ---`);
    const s = db.prepare(`
      SELECT COUNT(*) AS rows, SUM(units_net_sold) AS units,
        ROUND(SUM(gmv_list_price_jpy_incl),0) AS gmv_list,
        ROUND(SUM(customer_paid_jpy_incl),0) AS customer_paid,
        ROUND(SUM(net_settlement_api_jpy_incl),0) AS net_settle,
        ROUND(SUM(platform_fee_jpy_incl),0) AS platform_fee,
        ROUND(SUM(cogs_amount_jpy_incl),0) AS cogs,
        ROUND(SUM(shipping_cost_jpy_incl),0) AS shipping,
        ROUND(SUM(variable_margin_jpy_incl),0) AS margin,
        SUM(megawari_order_count) AS megawari_cnt,
        ROUND(SUM(megawari_discount_amount_jpy_incl),0) AS megawari_amt,
        SUM(megapo_order_count) AS megapo_cnt,
        ROUND(SUM(megapo_discount_amount_jpy_incl),0) AS megapo_amt,
        SUM(other_promo_order_count) AS other_promo_cnt,
        SUM(domestic_non_cod_line_count) AS dnoncod_total,
        SUM(domestic_non_cod_formula_match_count) AS dnoncod_match,
        SUM(CASE WHEN cost_status='complete' THEN 1 ELSE 0 END) AS complete_cnt,
        SUM(CASE WHEN cost_status='missing_cost' THEN 1 ELSE 0 END) AS missing_cnt,
        SUM(unresolved_sku_flag) AS unresolved_cnt,
        SUM(CASE WHEN resolution_method='master_match' THEN 1 ELSE 0 END) AS master_cnt,
        SUM(CASE WHEN match_tier='combined' THEN 1 ELSE 0 END) AS tier_combined_cnt,
        SUM(CASE WHEN match_tier='option_only' THEN 1 ELSE 0 END) AS tier_option_cnt,
        SUM(CASE WHEN match_tier='seller_only' THEN 1 ELSE 0 END) AS tier_seller_cnt,
        SUM(CASE WHEN match_tier='unresolved' THEN 1 ELSE 0 END) AS tier_unresolved_cnt,
        SUM(is_frozen_after_horizon) AS frozen_cnt,
        SUM(CASE WHEN settle_price_formula_scope='domestic_non_cod' THEN 1 ELSE 0 END) AS scope_dnoncod,
        SUM(CASE WHEN settle_price_formula_scope='oversea' THEN 1 ELSE 0 END) AS scope_oversea,
        SUM(CASE WHEN settle_price_formula_scope='cod' THEN 1 ELSE 0 END) AS scope_cod,
        SUM(CASE WHEN settle_price_formula_scope='mixed' THEN 1 ELSE 0 END) AS scope_mixed,
        AVG(delivered_lag_days) AS avg_delivered_lag_days
      FROM f_qoo10_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
    `).get(monthStr);
    console.log(`  rows: ${s.rows?.toLocaleString()} / units: ${s.units?.toLocaleString()}`);
    console.log(`  monthly totals (税込):`);
    console.log(`    gmv_list (orderPrice × qty):       ¥${s.gmv_list?.toLocaleString()}`);
    console.log(`    customer_paid (total):              ¥${s.customer_paid?.toLocaleString()}`);
    console.log(`    net_settlement_api (settle_price):  ¥${s.net_settle?.toLocaleString()}  ★ ショップ受取`);
    console.log(`    platform_fee (gmv - settle):       -¥${s.platform_fee?.toLocaleString()}`);
    console.log(`    cogs (snapshot × units):           -¥${s.cogs?.toLocaleString()}`);
    console.log(`    shipping_cost (店負担):            -¥${s.shipping?.toLocaleString()}`);
    console.log(`    variable_margin (net-cogs-ship):    ¥${s.margin?.toLocaleString()}`);
    // 粗利率を 4 通り表示 (中原さん 2026-05-19 確定: 「原価と送料を引いた率」両方見たい)
    //   ★ 商品粗利 = gmv - cogs - shipping (手数料引かず) = 「この商品を送って原価+送料引いたら何%残るか」
    //   ★ 正味受取粗利 = net_settle - cogs - shipping (= variable_margin) = 手数料も引いた後の実効率
    const productMargin = (s.gmv_list || 0) - (s.cogs || 0) - (s.shipping || 0);
    const pmRateGmv = s.gmv_list > 0 ? (productMargin / s.gmv_list * 100).toFixed(2) : 'n/a';
    const pmRateNet = s.net_settle > 0 ? (productMargin / s.net_settle * 100).toFixed(2) : 'n/a';
    const vmRateGmv = s.gmv_list > 0 ? ((s.margin || 0) / s.gmv_list * 100).toFixed(2) : 'n/a';
    const vmRateNet = s.net_settle > 0 ? ((s.margin || 0) / s.net_settle * 100).toFixed(2) : 'n/a';
    console.log(`    ─────────────────────────────────────`);
    console.log(`    ★ 商品粗利     (gmv - cogs - shipping): ¥${productMargin.toLocaleString()}`);
    console.log(`         ÷ 売上(gmv) = ${pmRateGmv}% / ÷ 受取(net) = ${pmRateNet}%`);
    console.log(`    ★ 正味受取粗利 (net - cogs - shipping): ¥${(s.margin || 0).toLocaleString()}  (= variable_margin、partial_pending_settlement_csv)`);
    console.log(`         ÷ 売上(gmv) = ${vmRateGmv}% / ÷ 受取(net) = ${vmRateNet}%`);
    console.log(`  promo (行レベル判別):`);
    console.log(`    megawari (~20% off):  ${s.megawari_cnt?.toLocaleString()} 注文 / ¥${s.megawari_amt?.toLocaleString()}`);
    console.log(`    megapo   (~10% off):  ${s.megapo_cnt?.toLocaleString()} 注文 / ¥${s.megapo_amt?.toLocaleString()}`);
    console.log(`    other_promo:           ${s.other_promo_cnt?.toLocaleString()} 注文`);
    console.log(`  scope: domestic_non_cod / oversea / cod / mixed: ${s.scope_dnoncod} / ${s.scope_oversea} / ${s.scope_cod} / ${s.scope_mixed}`);
    const formulaMatchPct = s.dnoncod_total > 0 ? (s.dnoncod_match / s.dnoncod_total * 100).toFixed(2) : 'n/a';
    console.log(`  settle_price_formula_match (domestic_non_cod 母集団): ${s.dnoncod_match} / ${s.dnoncod_total} = ${formulaMatchPct}%`);
    console.log(`  品質:`);
    console.log(`    cost_status complete / missing: ${s.complete_cnt?.toLocaleString()} / ${s.missing_cnt?.toLocaleString()}`);
    console.log(`    SKU 解決 master_match / unresolved: ${s.master_cnt?.toLocaleString()} / ${s.unresolved_cnt?.toLocaleString()}`);
    console.log(`    match_tier: combined ${s.tier_combined_cnt} / option_only ${s.tier_option_cnt} / seller_only ${s.tier_seller_cnt} / unresolved ${s.tier_unresolved_cnt}`);
    console.log(`    frozen_after_horizon: ${s.frozen_cnt?.toLocaleString()}`);
    console.log(`    avg delivered lag: ${s.avg_delivered_lag_days?.toFixed(1) || 'n/a'} 日`);

    // f_sales_by_listing 突合 (qoo10)
    try {
      const lst = db.prepare("SELECT ROUND(SUM(売上金額),0) AS sales, SUM(数量) AS units FROM f_sales_by_listing WHERE モール='qoo10' AND substr(日付,1,7)=?").get(monthStr);
      const diff = (s.net_settle || 0) - (lst.sales || 0);
      console.log(`  --- f_sales_by_listing (qoo10) 突合 ---`);
      console.log(`    listing 売上: ¥${lst.sales?.toLocaleString() || 0} / fact net_settle: ¥${s.net_settle?.toLocaleString() || 0} / diff: ¥${diff.toLocaleString()} (${lst.sales ? (Math.abs(diff)/lst.sales*100).toFixed(3) : 'n/a'}%)`);
    } catch (e) { console.log(`  (listing 突合スキップ: ${e.message})`); }

    // whitelist カバー率 (Delivered(5) 比率、~97% 想定)
    try {
      const cov = db.prepare(`
        SELECT
          (SELECT COUNT(*) FROM raw_qoo10_orders WHERE substr(order_date,1,7)=? AND legacy_fields_missing=0) AS total,
          (SELECT COUNT(*) FROM raw_qoo10_orders WHERE substr(order_date,1,7)=? AND legacy_fields_missing=0 AND shipping_status='Delivered(5)') AS wl
      `).get(monthStr, monthStr);
      console.log(`  --- whitelist カバー率 (Delivered(5) / 全 status、新 raw のみ) ---`);
      console.log(`    対象 raw lines: ${cov.total?.toLocaleString()} / whitelist (Delivered(5)): ${cov.wl?.toLocaleString()} (${cov.total ? (cov.wl/cov.total*100).toFixed(2) : 'n/a'}%)`);
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
