#!/usr/bin/env node
/**
 * run-aupay-finance-dq.js — au PAY マーケット Phase 1 A-2 DQ gate
 *
 * monthly validation: f_aupay_finance_sku_daily_v1 (A-1 で構築) の品質指標を 11 check で評価し
 * `dq_run_results` に severity 付きで記録。severity='error' が 1 件以上あれば exit 1 (gate failure)。
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data node apps/warehouse/run-aupay-finance-dq.js --month 2026-05
 *
 * 11 check (severity / threshold):
 *   1. row_count_drift                  (error: rows = 0。当月の月初の 0 行は条件つきで warn = finance-dq-month-mode.js の decideMonthStartEmpty)
 *   2. listing_diff_pct                 (warn 1% / error 5%、当月 5%/15%) — f_sales_by_listing (aupay) vs fact gross
 *   3. missing_cost_rate_pct            (warn 5% / error 10%)
 *   4. shipping_missing_rate_pct        (warn 5% / error 10%)
 *   5. unresolved_sku_rate_pct          (warn 12% / error 20%) — au PAY は親 SKU が意図的に unresolved 維持 (~9%)、想定外の急増検知用に閾値高め
 *   6. whitelist_coverage_pct           (warn < 95% / error < 90%、当月 80%/70%)
 *   7. resolved_but_zero_cost_count     (error: 1 件以上 — ne_code 解決済なのに cogs=0/unit_cost NULL。原価状態=OVERRIDDEN の意図的 0 円は除外)
 *   8. normalized_collision_count       (warn: 1 件以上 — m_products に LOWER(TRIM(商品コード)) 重複)
 *   9. request_price_reconcile_diff_pct (warn 0.5% / error 1%) — SUM(request_price) ≈ SUM(net_sales_after_coupon - use_ponta - use_au + item_option + gift_wrapping)
 *  10. mall_fee_rate_missing_pct        (warn 50% / error 80%) — mall_fee_calc_method='unknown' の比率
 *  11. allocation_conservation_diff_pct (error: 0.01% 超) — SUM(coupon_shop) (fact) ≈ SUM(注文単位 coupon_total_price) (raw)、LRM 保存則
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/auPAYマーケットPhase1設計書_v0.4_20260512.md §8
 */
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { monthMode, pickThresholds, modeLabel, decideMonthStartEmpty, monthStartEmptyNote, MONTH_ROW_COUNT_CHECK, monthRowCountDetails, prepareMonthHighWater, applyMonthStartSkip, resolveDqNow, monthStartRamp, monthStartRampWindow, recentListingJpy, applyMonthStartRamp, monthStartRampNote, monthStartRampOlder, listingOlderPart, whitelistOlderPart, NO_LISTING_RAMP_FLAG } from './finance-dq-month-mode.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthStr = getArg('--month');
const runId = getArg('--run-id') || `dq-aupay-${monthStr}-${new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)}`;
if (!DATA_DIR || !monthStr) { console.error('FATAL: DATA_DIR and --month YYYY-MM are required.'); process.exit(2); }
if (!/^\d{4}-\d{2}$/.test(monthStr)) { console.error('FATAL: --month must be YYYY-MM'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const checkedAt = new Date().toISOString();
// 月の判定の「今」(試験だけ: env FINANCE_DQ_ALLOW_NOW=1 のときだけ --now を受ける。daily-sync は渡さない)
let now;
try { now = resolveDqNow(getArg('--now')); }
catch (e) { console.error(`FATAL: ${e.message}`); process.exit(2); }
// daily-sync はこの回のモールの取込が ❌ のとき --no-month-start-grace を付ける (= 当月 0 行は猶予なしで CRITICAL)
const noMonthStartGrace = args.includes('--no-month-start-grace');
// daily-sync はこの回の f_sales の再構築が ❌ のとき --no-listing-ramp を付ける (= listing_diff_pct の月初の立ち上がりを使わない。比べる相手が古い)
const noListingRamp = args.includes(NO_LISTING_RAMP_FLAG);

const THRESHOLDS_PAST = {
  row_count_drift:                  { warn: 0,    error: 0 },
  // au PAY は f_sales_by_listing (NE 経由) と fact (au PAY API 直) でソース経路が違い、構造的に ~5-6% の乖離が常時ある
  listing_diff_pct:                 { warn: 3.0,  error: 8.0 },
  // missing_cost / shipping_missing は unresolved 親 SKU (~9-10%、意図的に粒度を潰さない層) で必ず欠ける = unresolved_sku_rate と同水準が正常。
  // 「解決済なのに原価ゼロ」は resolved_but_zero_cost_count (error=1) が別途厳格に拾うので、ここは急増検知用に unresolved と同じ閾値にする
  missing_cost_rate_pct:            { warn: 12.0, error: 20.0 },
  shipping_missing_rate_pct:        { warn: 12.0, error: 20.0 },
  unresolved_sku_rate_pct:          { warn: 12.0, error: 20.0 },  // au PAY: 親 SKU が意図的に unresolved (~9%)、急増検知用
  whitelist_coverage_pct:           { warn: 95.0, error: 90.0 },
  resolved_but_zero_cost_count:     { warn: 1,    error: 1 },
  normalized_collision_count:       { warn: 1,    error: 999 },
  request_price_reconcile_diff_pct: { warn: 0.5,  error: 1.0 },
  // config/aupay_mall_fee_rates.json で成約手数料率 (現状 13% 暫定) を設定済 → 通常 0% (全 store に率あり)。
  // unknown 比率が上がる = config 欠損/破損 or 新 store。Phase B で actual_statement に置換しても calc_method='unknown' でない限り問題なし
  mall_fee_rate_missing_pct:        { warn: 5.0,  error: 50.0 },
  allocation_conservation_diff_pct: { warn: 0.01, error: 0.01 },
};
const THRESHOLDS_CURRENT = {
  ...THRESHOLDS_PAST,
  listing_diff_pct:       { warn: 5.0, error: 15.0 },
  whitelist_coverage_pct: { warn: 80.0, error: 70.0 },
};
// 月の判定は共通ヘルパー (当月 / 前月+月初14日以内 / 過去)。前月の月初は出荷完了への遷移ラグで
// coverage が構造的に低いので whitelist_coverage_pct だけ当月閾値を使う (Qoo10 2026-08 の再発防止と同型)
const mode = monthMode(monthStr, { now });
const isCur = mode === 'current';
const THRESHOLDS = pickThresholds(mode, THRESHOLDS_PAST, THRESHOLDS_CURRENT);
// 月初の立ち上がり (finance-dq-month-mode.js の applyMonthStartRamp): 当月の 7 日目までは listing_diff_pct・whitelist_coverage_pct の error を、
// 「足りない向き」かつ「直近 2 日 + 今日より前の日には差が無い (ふだんのしきい値で error でない)」かつ「足りない分 ≤ 直近の受注」の
// ときだけ ⚠️ に下げる (出荷待ちの差。6〜10 月の毎月 2〜4 日に ❌ だった)
const ramp = monthStartRamp('aupay', monthStr, { now, noGrace: noMonthStartGrace, noListingRamp });
const rampedChecks = [];

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
const issues = [];
let hasError = false;
// 月初の猶予で通すときの判定 (null = 猶予なし)。猶予の中は SKIP_IN_MONTH_START_GRACE の検査を info に落とす (applyMonthStartSkip)
let monthStartGrace = null;
function recordResult(name, severity, actual, threshold, details = null) {
  ({ severity, details } = applyMonthStartSkip(monthStartGrace, name, severity, details));
  db.prepare(`INSERT OR REPLACE INTO dq_run_results (run_id, check_name, severity, actual_value, threshold_value, details_json, checked_at) VALUES (?,?,?,?,?,?,?)`)
    .run(runId, name, severity, actual, threshold, details ? JSON.stringify(details) : null, checkedAt);
  if (severity === 'error') hasError = true;
  if (severity !== 'info') issues.push({ name, severity, actual, threshold, details });
}

console.log(`=== au PAY DQ gate: ${runId} (month=${monthStr}, ${modeLabel(mode)} mode) ===`);
// 月初の猶予の印 (PR #1572 R2): dq_run_results の DELETE より前に、mall・月ごとの消えない印 (dq_month_high_water) を付ける。
// 一度 0 でなくなった月は、同じ run_id で流し直しても印が残る (前の記録は初回だけ移す)
prepareMonthHighWater(db, { mall: 'aupay', ym: monthStr, at: checkedAt,
  count: db.prepare("SELECT COUNT(*) AS c FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst, 1, 7) = ?").get(monthStr).c });
db.prepare(`DELETE FROM dq_run_results WHERE run_id = ?`).run(runId);

// Check 1: row_count_drift
const dailyCount = db.prepare("SELECT COUNT(*) AS c FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr).c;
recordResult(MONTH_ROW_COUNT_CHECK, 'info', dailyCount, null, monthRowCountDetails('aupay', monthStr, dailyCount));
if (dailyCount === 0) {
  // 月初の猶予 (finance-dq-month-mode.js の decideMonthStartEmpty): 当月・月初の日数の中・この月が一度も 0 でなくなっていない・
  // 前月の終わりまで新しい・daily-sync が禁じていない、を全部満たすときだけ ⚠️ 警告で続ける。早く終わらずに残りの検査も流す
  // (行数で比べる検査 = SKIP_IN_MONTH_START_GRACE だけ info に落とす。raw・全期間・原価の検査はそのまま)
  const g = decideMonthStartEmpty(db, { mall: 'aupay', ym: monthStr, now, noGrace: noMonthStartGrace });
  if (g.grace) {
    monthStartGrace = g;
    recordResult('row_count_drift', 'warn', dailyCount, 0, { daily_row_count: dailyCount, month_start_grace: true, jst_day: g.calendar.dayOfMonth, grace_days: g.graceDays, prev_month_max_date: g.prev?.maxDate ?? null });
    console.log(`  ${monthStartEmptyNote('f_aupay_finance_sku_daily_v1', monthStr, g)}`);
  } else {
    recordResult('row_count_drift', 'error', dailyCount, 0, { daily_row_count: dailyCount, month_start_grace_denied: g.reasons });
    console.error(`  ⚠️ CRITICAL: f_aupay_finance_sku_daily_v1 に ${monthStr} のデータが 0 行`);
    if (g.calendar.mode === 'current') console.error(`  → 月初の猶予を使わない理由: ${g.reasons.join(' / ')}`);
    printSummary();
    process.exit(1);
  }
}
if (dailyCount > 0) recordResult('row_count_drift', 'info', dailyCount, 0, { daily_row_count: dailyCount });

// Check 2: listing_diff_pct
const factGross = db.prepare("SELECT SUM(gross_sales_jpy_incl) AS p FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr).p || 0;
let listingTotal = 0, listingAvail = false;
try { const r = db.prepare("SELECT SUM(売上金額) AS p FROM f_sales_by_listing WHERE モール='aupay' AND substr(日付,1,7) = ?").get(monthStr); listingTotal = r?.p || 0; listingAvail = listingTotal > 0; } catch (e) { console.log(`  (listing 突合スキップ: ${e.message})`); }
if (listingAvail) {
  const diffPct = listingTotal !== 0 ? Math.abs(factGross - listingTotal) / Math.abs(listingTotal) * 100 : 0;
  const listingSeverity = (pct) => (pct > THRESHOLDS.listing_diff_pct.error ? 'error' : pct > THRESHOLDS.listing_diff_pct.warn ? 'warn' : 'info');
  const lw = monthStartRampWindow(ramp, 'listing_diff_pct');
  const ld = applyMonthStartRamp(monthStartGrace ? null : ramp, 'listing_diff_pct', listingSeverity(diffPct),
    { fact_gross_jpy: factGross, listing_jpy: listingTotal, diff_jpy: factGross - listingTotal },
    { shortfall: listingTotal - factGross, explainedBy: recentListingJpy(db, 'aupay', lw),
      // 窓より前の古い部分 (月の 1 日〜窓の前の日) の listing と fact を、同じしきい値で比べる
      older: monthStartRampOlder(lw, (from, to) => listingOlderPart(recentListingJpy(db, 'aupay', { from, to }),
        Number(db.prepare('SELECT SUM(gross_sales_jpy_incl) AS p FROM f_aupay_finance_sku_daily_v1 WHERE date_jst BETWEEN ? AND ?').get(from, to)?.p || 0), listingSeverity)) });
  if (ld.ramped) rampedChecks.push({ checkName: 'listing_diff_pct', value: diffPct });
  recordResult('listing_diff_pct', ld.severity, diffPct, THRESHOLDS.listing_diff_pct.error, ld.details);
} else { recordResult('listing_diff_pct', 'info', null, THRESHOLDS.listing_diff_pct.error, { skipped: true }); }

// Check 3: missing_cost_rate_pct
const cs = db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN cost_status='missing_cost' THEN 1 ELSE 0 END) AS missing FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const missCostRate = cs.total > 0 ? cs.missing / cs.total * 100 : 0;
recordResult('missing_cost_rate_pct', missCostRate > THRESHOLDS.missing_cost_rate_pct.error ? 'error' : missCostRate > THRESHOLDS.missing_cost_rate_pct.warn ? 'warn' : 'info', missCostRate, THRESHOLDS.missing_cost_rate_pct.error, { total_rows: cs.total, missing_count: cs.missing });

// Check 4: shipping_missing_rate_pct
const ss = db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN shipping_quality='missing' THEN 1 ELSE 0 END) AS missing FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const shipMissRate = ss.total > 0 ? ss.missing / ss.total * 100 : 0;
recordResult('shipping_missing_rate_pct', shipMissRate > THRESHOLDS.shipping_missing_rate_pct.error ? 'error' : shipMissRate > THRESHOLDS.shipping_missing_rate_pct.warn ? 'warn' : 'info', shipMissRate, THRESHOLDS.shipping_missing_rate_pct.error, { total_rows: ss.total, missing_count: ss.missing });

// Check 5: unresolved_sku_rate_pct
const us = db.prepare("SELECT COUNT(*) AS total, SUM(unresolved_sku_flag) AS unresolved, SUM(CASE WHEN resolution_method='master_match' THEN 1 ELSE 0 END) AS master, SUM(CASE WHEN resolution_method='manual_map' THEN 1 ELSE 0 END) AS manual FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const unresRate = us.total > 0 ? us.unresolved / us.total * 100 : 0;
recordResult('unresolved_sku_rate_pct', unresRate > THRESHOLDS.unresolved_sku_rate_pct.error ? 'error' : unresRate > THRESHOLDS.unresolved_sku_rate_pct.warn ? 'warn' : 'info', unresRate, THRESHOLDS.unresolved_sku_rate_pct.error, { total_rows: us.total, unresolved: us.unresolved, master_match: us.master, manual_map: us.manual });

// Check 6: whitelist_coverage_pct
const cov = db.prepare(`SELECT
  (SELECT COUNT(*) FROM raw_aupay_orders WHERE substr(replace(order_date,'/','-'),1,7)=?) AS total,
  (SELECT COUNT(*) FROM raw_aupay_orders WHERE substr(replace(order_date,'/','-'),1,7)=? AND order_status='完了' AND item_cancel_status='N') AS wl`).get(monthStr, monthStr);
const wlPct = cov.total > 0 ? cov.wl / cov.total * 100 : 0;
// 月初の立ち上がりの ③: whitelist に入っていない行の数 ≤ 直近 2 日 + 今日の受注の行の数
const wlWin = monthStartRampWindow(ramp, 'whitelist_coverage_pct');
const wlRecent = wlWin ? db.prepare("SELECT COUNT(*) AS c FROM raw_aupay_orders WHERE substr(replace(order_date,'/','-'),1,10) BETWEEN ? AND ?").get(wlWin.from, wlWin.to).c : null;
const wlSeverity = (pct) => (pct < THRESHOLDS.whitelist_coverage_pct.error ? 'error' : pct < THRESHOLDS.whitelist_coverage_pct.warn ? 'warn' : 'info');
const wl = applyMonthStartRamp(monthStartGrace ? null : ramp, 'whitelist_coverage_pct', wlSeverity(wlPct),
  { total_lines: cov.total, whitelist_lines: cov.wl }, { shortfall: (cov.total || 0) - (cov.wl || 0), explainedBy: wlRecent,
    // 窓より前の古い部分: その期間の受注のうち whitelist に入った割合を、同じしきい値で判定 (古い注文が止まっていれば ❌ のまま)
    older: monthStartRampOlder(wlWin, (from, to) => { const o = db.prepare("SELECT COUNT(*) AS t, COALESCE(SUM(order_status = '完了' AND item_cancel_status = 'N'), 0) AS w FROM raw_aupay_orders WHERE substr(replace(order_date, '/', '-'), 1, 10) BETWEEN ? AND ?").get(from, to); return whitelistOlderPart(o.t, o.w, wlSeverity); }) });
if (wl.ramped) rampedChecks.push({ checkName: 'whitelist_coverage_pct', value: wlPct });
recordResult('whitelist_coverage_pct', wl.severity, wlPct, THRESHOLDS.whitelist_coverage_pct.warn, wl.details);

// Check 7: resolved_but_zero_cost_count
// 原価状態='OVERRIDDEN' AND 原価=0 は人手の意図的 0 円上書き → snapshot=0 (lookup 成功で 0 円) の行のみ除外。
// snapshot NULL (lookup 失敗) / snapshot>0 で cogs=0 (計算異常) は error のまま。判定は実行時点の m_products (as-of ではない)
const zc = db.prepare("SELECT COUNT(*) AS cnt, SUM(CASE WHEN f.unit_cost_snapshot_incl = 0 AND EXISTS (SELECT 1 FROM m_products mp WHERE LOWER(TRIM(mp.商品コード)) = LOWER(TRIM(f.ne_code)) AND mp.原価状態 = 'OVERRIDDEN' AND mp.原価 = 0) THEN 1 ELSE 0 END) AS intentional_zero FROM f_aupay_finance_sku_daily_v1 f WHERE substr(f.date_jst,1,7)=? AND f.ne_code IS NOT NULL AND f.units_net_sold > 0 AND (f.cogs_amount_jpy_incl = 0 OR f.unit_cost_snapshot_incl IS NULL)").get(monthStr);
const zcCnt = zc.cnt - (zc.intentional_zero || 0);
recordResult('resolved_but_zero_cost_count', zcCnt >= THRESHOLDS.resolved_but_zero_cost_count.error ? 'error' : zcCnt >= THRESHOLDS.resolved_but_zero_cost_count.warn ? 'warn' : 'info', zcCnt, THRESHOLDS.resolved_but_zero_cost_count.error, { count: zcCnt, intentional_zero_cost_exempted: zc.intentional_zero || 0 });

// Check 8: normalized_collision_count
const coll = db.prepare("SELECT COUNT(*) AS cnt FROM (SELECT LOWER(TRIM(商品コード)) AS k, COUNT(*) AS c FROM m_products WHERE 商品コード IS NOT NULL AND TRIM(商品コード)<>'' GROUP BY k HAVING COUNT(*) > 1)").get();
recordResult('normalized_collision_count', coll.cnt >= THRESHOLDS.normalized_collision_count.error ? 'error' : coll.cnt >= THRESHOLDS.normalized_collision_count.warn ? 'warn' : 'info', coll.cnt, THRESHOLDS.normalized_collision_count.warn, { collision_keys: coll.cnt });

// Check 9: request_price_reconcile_diff_pct
//   request_price_jpy_incl ≈ net_sales_after_coupon - use_ponta - use_au + item_option + gift_wrapping
const rc = db.prepare(`SELECT
  SUM(request_price_jpy_incl) AS sum_req,
  SUM(net_sales_after_coupon_jpy_incl - use_ponta_point_jpy_incl - use_au_point_jpy_incl + item_option_jpy_incl + gift_wrapping_jpy_incl) AS sum_calc
  FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?`).get(monthStr);
const reqDiffPct = (rc.sum_req && rc.sum_req !== 0) ? Math.abs(rc.sum_req - rc.sum_calc) / Math.abs(rc.sum_req) * 100 : 0;
recordResult('request_price_reconcile_diff_pct', reqDiffPct > THRESHOLDS.request_price_reconcile_diff_pct.error ? 'error' : reqDiffPct > THRESHOLDS.request_price_reconcile_diff_pct.warn ? 'warn' : 'info', reqDiffPct, THRESHOLDS.request_price_reconcile_diff_pct.error, { sum_request_price: rc.sum_req, sum_reconcile: rc.sum_calc, diff: (rc.sum_req||0)-(rc.sum_calc||0) });

// Check 10: mall_fee_rate_missing_pct
const fm = db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN mall_fee_calc_method='unknown' THEN 1 ELSE 0 END) AS unknown FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const feeMissPct = fm.total > 0 ? fm.unknown / fm.total * 100 : 0;
recordResult('mall_fee_rate_missing_pct', feeMissPct > THRESHOLDS.mall_fee_rate_missing_pct.error ? 'error' : feeMissPct > THRESHOLDS.mall_fee_rate_missing_pct.warn ? 'warn' : 'info', feeMissPct, THRESHOLDS.mall_fee_rate_missing_pct.error, { total_rows: fm.total, unknown_count: fm.unknown });

// Check 11: allocation_conservation_diff_pct (LRM 保存則: SUM(coupon_shop) ≈ raw の注文単位 coupon_total_price 合計)
const ac = db.prepare(`SELECT
  (SELECT SUM(coupon_shop_jpy_incl) FROM f_aupay_finance_sku_daily_v1 WHERE substr(date_jst,1,7)=?) AS fact_coupon,
  (SELECT SUM(c) FROM (SELECT order_id, MAX(coupon_total_price) AS c FROM raw_aupay_orders WHERE substr(replace(order_date,'/','-'),1,7)=? AND order_status='完了' AND item_cancel_status='N' GROUP BY order_id)) AS raw_coupon`).get(monthStr, monthStr);
const allocDiffPct = (ac.raw_coupon && ac.raw_coupon !== 0) ? Math.abs((ac.fact_coupon||0) - ac.raw_coupon) / Math.abs(ac.raw_coupon) * 100 : (ac.fact_coupon ? 100 : 0);
recordResult('allocation_conservation_diff_pct', allocDiffPct > THRESHOLDS.allocation_conservation_diff_pct.error ? 'error' : 'info', allocDiffPct, THRESHOLDS.allocation_conservation_diff_pct.error, { fact_coupon: ac.fact_coupon, raw_coupon: ac.raw_coupon, diff: (ac.fact_coupon||0)-(ac.raw_coupon||0) });

function printSummary() {
  console.log('\n--- DQ check summary ---');
  for (const r of db.prepare("SELECT check_name, severity, actual_value, threshold_value FROM dq_run_results WHERE run_id=? ORDER BY check_name").all(runId)) {
    const icon = r.severity === 'error' ? '❌' : r.severity === 'warn' ? '⚠️' : 'ℹ️';
    console.log(`  ${icon} ${r.check_name}: ${r.actual_value !== null ? r.actual_value.toFixed(3) : 'n/a'} (threshold=${r.threshold_value !== null ? r.threshold_value.toFixed(3) : 'n/a'}, ${r.severity})`);
  }
  console.log('');
  if (hasError) console.log(`❌ DQ gate FAILED (${issues.filter(i=>i.severity==='error').length} error, ${issues.filter(i=>i.severity==='warn').length} warn)`);
  else if (issues.length > 0) console.log(`⚠️  DQ gate passed with ${issues.filter(i=>i.severity==='warn').length} warning(s)`);
  else console.log(`✅ DQ gate passed (no error, no warn)`);
}
printSummary();
// 月初の猶予で通した回は、最後の行を「⚠️ 月初の猶予: …」にする (daily-sync はこの行を要約に出し、warn を立てて見出しを ⚠️ にする)
if (monthStartGrace && !hasError) console.log(monthStartEmptyNote('f_aupay_finance_sku_daily_v1', monthStr, monthStartGrace));
// 月初の立ち上がりで下げた回も、最後の行を「⚠️ 月初の立ち上がり: …」にする (daily-sync は見出しを ⚠️ にする)
else if (rampedChecks.length > 0 && !hasError) console.log(monthStartRampNote(ramp, rampedChecks));
db.close();
process.exit(hasError ? 1 : 0);
