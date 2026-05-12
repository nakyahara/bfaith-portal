#!/usr/bin/env node
/**
 * f_aupay_finance_sku_daily_v1 build entrypoint — au PAY マーケット Phase 1 A-1
 *
 * - DDL:       sql/aupay/f_aupay_finance_sku_daily_v1.sql
 * - Map DDL:   sql/aupay/f_aupay_sku_map.sql
 * - Build SQL: sql/aupay/build_f_aupay_finance_sku_daily_v1.sql
 * - 設計書:    g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md
 *
 * 不変条件 (楽天/Yahoo Phase 1 継承):
 *   - 既存 (date_jst, aupay_sku_key) は UPSERT で snapshot 列以外を更新 (snapshot 不変)
 *   - cogs / variable_margin_partial は「既存 snapshot 原価 × 新 units_net_sold」で再計算
 *
 * au PAY 特徴 (設計書 v0.4):
 *   - 月次 rolling rebuild: --month で指定した月 + 前 2 ヶ月を build (楽天 Phase 1b 同型、status 遷移/manual map/cost snapshot 補完のため)
 *   - クーポン (couponTotalPrice、注文単位) を明細に LRM 按分 (detail.discount とは別物)
 *   - ポイント (giftPoint / usePoint / useAuPointPrice / premiumShopPrice) は別列保持、partial margin に入れない (Phase B で確定)
 *   - mall_fee = (gross_sales - coupon_shop) × 成約手数料率 (config/aupay_mall_fee_rates.json、未確定なら NULL → calc_method='unknown')
 *   - partial margin (5 要素) = gross_sales - cogs - mall_fee - shipping - coupon
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data \
 *     node scripts/aupay-finance/build-aupay-daily-fact.js --month 2026-05
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
const ddlPath = getArg('--ddl-path') || path.join(repoRoot, 'sql', 'aupay', 'f_aupay_finance_sku_daily_v1.sql');
const mapDdlPath = getArg('--map-ddl-path') || path.join(repoRoot, 'sql', 'aupay', 'f_aupay_sku_map.sql');
const buildSqlPath = getArg('--build-sql-path') || path.join(repoRoot, 'sql', 'aupay', 'build_f_aupay_finance_sku_daily_v1.sql');
for (const p of [ddlPath, mapDdlPath, buildSqlPath]) {
  if (!fs.existsSync(p)) { console.error(`FATAL: SQL file not found at ${p}`); process.exit(2); }
}
const ddlSql = fs.readFileSync(ddlPath, 'utf8');
const mapDdlSql = fs.readFileSync(mapDdlPath, 'utf8');
const rawBuildSql = fs.readFileSync(buildSqlPath, 'utf8');

// 成約手数料率の設定 (config/aupay_mall_fee_rates.json、無ければ NULL)
// 形式想定: { "b-faith01": [ { "effective_date": "2024-01-01", "rate": 0.10 }, ... ] }
function loadMallFeeRate(storeId, ymStr) {
  const cfgPath = path.join(repoRoot, 'config', 'aupay_mall_fee_rates.json');
  if (!fs.existsSync(cfgPath)) return null;
  try {
    const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
    const list = (cfg[storeId] || []).slice().sort((a, b) => (a.effective_date < b.effective_date ? 1 : -1));
    const ymFirst = `${ymStr}-01`;
    for (const e of list) { if (e.effective_date <= ymFirst) return e.rate; }
    return null;
  } catch { return null; }
}

console.log('=== f_aupay_finance_sku_daily_v1 build (Phase 1 A-1) ===');
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
  db.exec(mapDdlSql);  console.log('  ✓ f_aupay_sku_map applied');
  db.exec(ddlSql);     console.log('  ✓ f_aupay_finance_sku_daily_v1 + device/settlement fact + views applied');

  // build SQL を ; で分割 (コメントのみ statement は除外、Yahoo build runner と同型)
  const statements = rawBuildSql.split(';').map(s => s.trim()).filter(s => {
    if (!s) return false;
    const codeOnly = s.split('\n').filter(l => !l.trim().startsWith('--') && l.trim() !== '').join('\n').trim();
    return codeOnly.length > 0;
  });
  console.log(`  Parsed ${statements.length} executable statements`);

  // 2. 各対象月を build (古い順)
  for (const ym of targetMonths) {
    const yearMonthInt = parseInt(ym.replace('-', ''), 10);
    const mallFeeRate = loadMallFeeRate('b-faith01', ym);
    const beforeCount = db.prepare("SELECT COUNT(*) AS c FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(ym).c;
    console.log(`\n--- 2. build ${ym} (year_month_int=${yearMonthInt}, mall_fee_rate=${mallFeeRate === null ? 'NULL (unknown)' : mallFeeRate}, 既存 ${beforeCount} 行) ---`);
    if (dryRun) { console.log(`  (dry-run) would execute ${statements.length} statements`); continue; }
    let totalChanges = 0;
    const tx = db.transaction(() => {
      for (const sql of statements) {
        const hasParams = sql.includes(':year_month_int') || sql.includes(':build_date') || sql.includes(':mall_fee_rate');
        if (hasParams) {
          const stmt = db.prepare(sql);
          const info = stmt.run({ year_month_int: yearMonthInt, build_date: buildDate, mall_fee_rate: mallFeeRate });
          if (info.changes > 0) totalChanges += info.changes;
        } else {
          db.exec(sql);
        }
      }
    });
    tx.immediate();
    const afterCount = db.prepare("SELECT COUNT(*) AS c FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(ym).c;
    console.log(`  ✓ changes=${totalChanges.toLocaleString()}, rows for ${ym}: ${afterCount.toLocaleString()}`);
  }

  // 3. 主対象月 (monthStr) のサマリ
  if (!dryRun) {
    console.log(`\n--- 3. summary for ${monthStr} ---`);
    const s = db.prepare(`
      SELECT COUNT(*) AS rows, SUM(units_net_sold) AS units,
        ROUND(SUM(sales_principal_jpy_incl),0) AS principal,
        ROUND(SUM(coupon_shop_jpy_incl),0) AS coupon,
        ROUND(SUM(gift_point_jpy_incl),0) AS gift_pt,
        ROUND(SUM(use_ponta_point_jpy_incl + use_au_point_jpy_incl),0) AS use_pt,
        ROUND(SUM(cogs_amount_jpy_incl),0) AS cogs,
        ROUND(SUM(COALESCE(mall_fee_jpy_incl,0)),0) AS mall_fee,
        ROUND(SUM(shipping_cost_jpy_incl),0) AS shipping,
        ROUND(SUM(variable_margin_partial_jpy_incl),0) AS margin,
        SUM(CASE WHEN cost_status='complete' THEN 1 ELSE 0 END) AS complete_cnt,
        SUM(CASE WHEN cost_status='missing_cost' THEN 1 ELSE 0 END) AS missing_cnt,
        SUM(unresolved_sku_flag) AS unresolved_cnt,
        SUM(CASE WHEN resolution_method='master_match' THEN 1 ELSE 0 END) AS master_cnt,
        SUM(CASE WHEN resolution_method='manual_map' THEN 1 ELSE 0 END) AS manual_cnt,
        SUM(CASE WHEN mall_fee_calc_method='unknown' THEN 1 ELSE 0 END) AS fee_unknown_cnt,
        SUM(price_variance_warning) AS pv_cnt
      FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?
    `).get(monthStr);
    console.log(`  rows: ${s.rows?.toLocaleString()} / units: ${s.units?.toLocaleString()}`);
    console.log(`  monthly totals (税込):`);
    console.log(`    sales_principal:                ¥${s.principal?.toLocaleString()}`);
    console.log(`    coupon_shop (ストアクーポン):    -¥${s.coupon?.toLocaleString()}`);
    console.log(`    cogs (snapshot × units):        -¥${s.cogs?.toLocaleString()}`);
    console.log(`    mall_fee (成約手数料率):        -¥${s.mall_fee?.toLocaleString()} ${s.fee_unknown_cnt > 0 ? `(⚠️ unknown ${s.fee_unknown_cnt} 行、成約手数料率 未設定)` : ''}`);
    console.log(`    shipping_cost (店負担):         -¥${s.shipping?.toLocaleString()}`);
    console.log(`    variable_margin_partial (5要素): ¥${s.margin?.toLocaleString()}`);
    const pct = s.principal > 0 ? (s.margin / s.principal * 100).toFixed(2) : 'n/a';
    console.log(`    粗利率 partial:                  ${pct}% ${s.fee_unknown_cnt > 0 ? '(成約手数料率未設定なので過大、Phase B で確定)' : '(暫定、Phase B で full に補正)'}`);
    console.log(`  保留 (Phase B で確定):`);
    console.log(`    gift_point (付与):              ¥${s.gift_pt?.toLocaleString()}`);
    console.log(`    use_point (Ponta + au、顧客支払): ¥${s.use_pt?.toLocaleString()}`);
    console.log(`  品質:`);
    console.log(`    cost_status complete / missing_cost: ${s.complete_cnt?.toLocaleString()} / ${s.missing_cnt?.toLocaleString()}`);
    console.log(`    SKU 解決 master_match / manual_map / unresolved: ${s.master_cnt?.toLocaleString()} / ${s.manual_cnt?.toLocaleString()} / ${s.unresolved_cnt?.toLocaleString()}`);
    console.log(`    price_variance_warning: ${s.pv_cnt?.toLocaleString()}`);

    // device / settlement 別
    const dev = db.prepare("SELECT device, COUNT(DISTINCT aupay_sku_key) AS skus, SUM(units_net_sold) AS units, ROUND(SUM(sales_principal_jpy_incl),0) AS sales FROM f_aupay_sku_device_daily WHERE substr(date_jst,1,7)=? GROUP BY device ORDER BY sales DESC").all(monthStr);
    console.log(`  デバイス別:`);
    for (const d of dev) console.log(`    ${d.device}: ${d.units} units / ¥${d.sales?.toLocaleString()}`);
    const set = db.prepare("SELECT settlement_name, SUM(units_net_sold) AS units, ROUND(SUM(sales_principal_jpy_incl),0) AS sales FROM f_aupay_sku_settlement_daily WHERE substr(date_jst,1,7)=? GROUP BY settlement_name ORDER BY sales DESC LIMIT 5").all(monthStr);
    console.log(`  決済手段別 (top 5):`);
    for (const x of set) console.log(`    ${x.settlement_name}: ${x.units} units / ¥${x.sales?.toLocaleString()}`);

    // f_sales_by_listing 突合
    try {
      const lst = db.prepare("SELECT ROUND(SUM(売上金額),0) AS sales, SUM(数量) AS units FROM f_sales_by_listing WHERE モール='aupay' AND substr(日付,1,7)=?").get(monthStr);
      const diff = (s.principal || 0) - (lst.sales || 0);
      console.log(`  --- f_sales_by_listing (aupay) 突合 ---`);
      console.log(`    listing 売上: ¥${lst.sales?.toLocaleString()} / fact gross: ¥${s.principal?.toLocaleString()} / diff: ¥${diff.toLocaleString()} (${lst.sales ? (Math.abs(diff)/lst.sales*100).toFixed(3) : 'n/a'}%)`);
    } catch (e) { /* skip */ }

    // whitelist カバー率
    try {
      const cov = db.prepare(`
        SELECT (SELECT COUNT(*) FROM raw_aupay_orders WHERE substr(replace(order_date,'/','-'),1,7)=?) AS total,
               (SELECT COUNT(*) FROM raw_aupay_orders WHERE substr(replace(order_date,'/','-'),1,7)=? AND order_status='完了' AND item_cancel_status='N') AS wl
      `).get(monthStr, monthStr);
      console.log(`  --- whitelist カバー率 ---`);
      console.log(`    全 raw lines: ${cov.total?.toLocaleString()} / whitelist: ${cov.wl?.toLocaleString()} (${cov.total ? (cov.wl/cov.total*100).toFixed(2) : 'n/a'}%)`);
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
