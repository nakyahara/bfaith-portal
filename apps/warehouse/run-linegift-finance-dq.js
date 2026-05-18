#!/usr/bin/env node
/**
 * run-linegift-finance-dq.js — LINEギフト Phase 1 A-2 DQ gate
 *
 * monthly validation: f_linegift_finance_sku_daily_v1 (A-2 で構築) の品質指標を 12 check で評価し
 * `dq_run_results` に severity 付きで記録。severity='error' が 1 件以上あれば exit 1 (gate failure)。
 *
 * 使い方:
 *   DATA_DIR=C:/Users/bfaith/bfaith-portal/data node apps/warehouse/run-linegift-finance-dq.js --month 2026-05
 *
 * 12 check (severity / threshold、設計書 v0.5 §8):
 *   1. row_count_drift                        (error: rows = 0、当月 < 50% of 7日平均)
 *   2. listing_diff_pct                       (warn 1% / error 5%、当月 5%/15%) — f_sales_by_listing (linegift) vs fact gross
 *                                              ※ 重複 3ヶ月期間は受注日 vs 受取日のズレで informational に格下げ (Codex #9)
 *   3. missing_cost_rate_pct                  (warn 0.5% / error 1%) — 100% master_match のはず
 *   4. shipping_missing_rate_pct              (Phase A 無効化: 'no_shipping_in_api' が常態) — Phase B で実額入れたら有効化
 *   5. unmatched_sku_rate_pct                 (warn 0.5% / error 1%、Codex #6) — 100% master_match のはず
 *   6. fallback_to_item_code_rate_pct         (warn: 1件以上、Codex #6) — variation.code 欠損 → item.code fallback (parent_match) 件数
 *   7. whitelist_coverage_pct                 (warn 90% / error 80%、当月 70%) — received 比率 (91% 想定)
 *   8. resolved_but_zero_cost_count           (error: 1 件以上) — ne_code 解決済なのに cogs=0/unit_cost NULL
 *   9. fee_rate_drift_pct                     (warn 0.5% / error 1%、LINEギフト特有) — SUM(mall_fee)/SUM(sales_principal) が 12.97% 中央値からドリフト
 *  10. provisional_state_age_days             (warn: 7日以上、LINEギフト特有、Codex #1) — gift_message_send/payment のまま 7日以上動いてない注文数 (status flip 失敗検知)
 *  11. horizon_frozen_observed_count          (info、LINEギフト特有、Codex R5 critical 反映で意味変更) — monthStr 月に final observation (last_seen_at) を持つ frozen 行数 = 凍結に向かう正常な observation 監視指標
 *                                              ※ 設計書 §8 #11「frozen=1 行が再取得時に値変動」検知は raw に frozen_at + snapshot hash を追加すれば厳密実装可能、現スキーマでは測定不能のため info 化
 *  12. received_missing_received_on_count     (error: 1件以上、LINEギフト特有、Codex Round 2 #4) — status='received' なのに received_on_unix IS NULL or =0
 *  13. monthless_received_rows                (error: 1件以上、LINEギフト特有、Codex R3 medium 反映) — status='received' で received/last_seen/bought 全て月不明 (どの月の DQ にも乗らない孤児行) を全期間 global で常時検知
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.5_20260515.md §8
 */
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
const monthStr = getArg('--month');
const runId = getArg('--run-id') || `dq-linegift-${monthStr}-${new Date().toISOString().replace(/[:.]/g, '').slice(0, 15)}`;
if (!DATA_DIR || !monthStr) { console.error('FATAL: DATA_DIR and --month YYYY-MM are required.'); process.exit(2); }
if (!/^\d{4}-\d{2}$/.test(monthStr)) { console.error('FATAL: --month must be YYYY-MM'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const checkedAt = new Date().toISOString();
function isCurrentMonth(ym) { const nowJst = new Date(Date.now() + 9 * 3600 * 1000); return ym === nowJst.toISOString().slice(0, 7); }

// 重複期間: 既存 linegift_accounting CSV (受注日基準) と新 fact (受取日基準) が並走している月 → listing_diff_pct を informational に格下げ
// (設計書 §8 #2、Codex #9: 受注日 vs 受取日で 1〜数日ずれて売上が流れるため、構造的に diff が出る)
//
// 並走期間 = LINEギフト 受注 API backfill 範囲 (~3ヶ月) と CSV (2022-07〜2026-02 投入済) の重複部分
// = 2026-02 以降のすべての backfill 月
//
// CSV 投入を停止して新 fact のみ運用に切替後 (Phase B 完了後想定) は空集合に戻す
const DUPLICATE_PERIOD_MONTHS = new Set([
  '2026-02', '2026-03', '2026-04', '2026-05',
]);

const FEE_RATE_TARGET = 0.1295;  // 12.95% (実機 verify 値、A-1 backfill で確認)

const THRESHOLDS_PAST = {
  row_count_drift:                       { warn: 0,    error: 0 },
  // listing 突合: ソース経路差で常時 ~1-2% 乖離 (LINEギフト 既存 CSV vs 新 API)。重複期間は informational
  listing_diff_pct:                      { warn: 1.0,  error: 5.0 },
  missing_cost_rate_pct:                 { warn: 0.5,  error: 1.0 },
  shipping_missing_rate_pct:             { warn: 100,  error: 100 },  // Phase A 無効化
  unmatched_sku_rate_pct:                { warn: 0.5,  error: 1.0 },
  fallback_to_item_code_rate_pct:        { warn: 1,    error: 999 },  // 1件以上 warn (variation.code 欠損 = LINEギフト API 仕様変化)
  whitelist_coverage_pct:                { warn: 90.0, error: 80.0 },
  resolved_but_zero_cost_count:          { warn: 1,    error: 1 },
  fee_rate_drift_pct:                    { warn: 0.5,  error: 1.0 },  // 12.97% 中央値からの絶対ドリフト pp
  provisional_state_age_days:            { warn: 1,    error: 999 },  // 7日以上動いてない provisional が 1 件でも warn
  horizon_frozen_observed_count:         { warn: 999999, error: 999999 },  // info 固定 (Codex R5 critical 反映、現スキーマでは違反検知不能)
  received_missing_received_on_count:    { warn: 1,    error: 1 },    // 1件以上 error
  monthless_received_rows:               { warn: 1,    error: 1 },    // 全期間 global、月割不能孤児行を常時 error
};
const THRESHOLDS_CURRENT = {
  ...THRESHOLDS_PAST,
  listing_diff_pct:       { warn: 5.0, error: 15.0 },
  whitelist_coverage_pct: { warn: 70.0, error: 60.0 },  // 当月 70% (received への遷移ラグ考慮)
};
const isCur = isCurrentMonth(monthStr);
const isDuplicatePeriod = DUPLICATE_PERIOD_MONTHS.has(monthStr);
const THRESHOLDS = isCur ? THRESHOLDS_CURRENT : THRESHOLDS_PAST;

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
const issues = [];
let hasError = false;
function recordResult(name, severity, actual, threshold, details = null) {
  db.prepare(`INSERT OR REPLACE INTO dq_run_results (run_id, check_name, severity, actual_value, threshold_value, details_json, checked_at) VALUES (?,?,?,?,?,?,?)`)
    .run(runId, name, severity, actual, threshold, details ? JSON.stringify(details) : null, checkedAt);
  if (severity === 'error') hasError = true;
  if (severity !== 'info') issues.push({ name, severity, actual, threshold, details });
}

console.log(`=== LINEギフト DQ gate: ${runId} (month=${monthStr}, ${isCur ? 'CURRENT' : 'PAST'} mode${isDuplicatePeriod ? ', DUPLICATE_PERIOD' : ''}) ===`);
db.prepare(`DELETE FROM dq_run_results WHERE run_id = ?`).run(runId);

// Check 1: row_count_drift (rows=0 がエラー、当月のみ「直近日次 < 7日平均×0.5」も warn、Codex R1 #1 反映)
const dailyCount = db.prepare("SELECT COUNT(*) AS c FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr).c;
if (dailyCount === 0) {
  recordResult('row_count_drift', 'error', dailyCount, 0, { daily_row_count: dailyCount });
  console.error(`  ⚠️ CRITICAL: f_linegift_finance_sku_daily_v1 に ${monthStr} のデータが 0 行`);
  printSummary();
  process.exit(1);
}
if (isCur) {
  // 当月のみ partial ingest 検知 (Codex R2 #1 反映、date spine で「fact に存在する日だけ」見ない)
  // 朝 07:00 cron 運用なので「期待する最新日 = JST yesterday」。spine = 期待 latest を含む直近 8 calendar days。
  // 月初/月跨ぎでも spine が必ず 8 日揃い、欠損日は 0 として扱われる (drift 発火可能)
  const recent = db.prepare(`
    WITH RECURSIVE date_spine(d) AS (
      SELECT date('now', '+9 hours', '-8 days')
      UNION ALL
      SELECT date(d, '+1 day') FROM date_spine WHERE d < date('now', '+9 hours', '-1 day')
    )
    SELECT date_spine.d AS date_jst, COALESCE(c.cnt, 0) AS cnt
    FROM date_spine
    LEFT JOIN (SELECT date_jst, COUNT(*) AS cnt FROM f_linegift_finance_sku_daily_v1 GROUP BY date_jst) c
      ON c.date_jst = date_spine.d
    ORDER BY date_spine.d
  `).all();
  // recent[最後] = 期待 latest (= yesterday)、recent[0..-1] = 7 prior days
  const latest = recent[recent.length - 1];
  const priorDays = recent.slice(0, -1);
  const avg7 = priorDays.reduce((sum, r) => sum + r.cnt, 0) / priorDays.length;
  const ratio = avg7 > 0 ? latest.cnt / avg7 : null;
  const allZero = recent.every(r => r.cnt === 0);
  // Codex R3 #1 反映: avg7=0 を一律 info にせず本物の停止を区別
  //  - allZero (8 日連続 0) = 取り込み完全停止 → error
  //  - avg7=0 だが latest>0 = 新規運用 → info
  //  - avg7=0 だが latest=0 = 新規運用 + 当日もまだ → info (起動初日)
  //  - avg7>0 だが ratio<0.5 = partial ingest 疑い → warn
  //  - avg7>0 だが ratio>=0.5 = 正常 → info
  if (allZero) {
    recordResult('row_count_drift', 'error', latest.cnt, 0, { expected_latest_date: latest.date_jst, latest_count: latest.cnt, spine_days: recent.length, note: '直近 8 日連続 row=0 (取り込み完全停止)' });
  } else if (avg7 > 0 && ratio < 0.5) {
    recordResult('row_count_drift', 'warn', latest.cnt, Math.round(avg7 * 0.5), { expected_latest_date: latest.date_jst, latest_count: latest.cnt, avg7_prior_days: avg7.toFixed(1), ratio: ratio.toFixed(3), spine_days: recent.length, note: '期待 latest (yesterday) row 数 < 7日平均×0.5 (partial ingest 疑い)' });
  } else if (avg7 === 0) {
    recordResult('row_count_drift', 'info', latest.cnt, 0, { expected_latest_date: latest.date_jst, latest_count: latest.cnt, note: '7日平均=0 だが latest>0 (新規運用)、ratio 比較スキップ' });
  } else {
    recordResult('row_count_drift', 'info', latest.cnt, Math.round(avg7 * 0.5), { expected_latest_date: latest.date_jst, latest_count: latest.cnt, avg7_prior_days: avg7.toFixed(1), ratio: ratio !== null ? ratio.toFixed(3) : 'n/a', spine_days: recent.length });
  }
} else {
  recordResult('row_count_drift', 'info', dailyCount, 0, { daily_row_count: dailyCount, note: 'PAST mode は rows>0 のみチェック' });
}

// Check 2: listing_diff_pct (重複期間は informational)
const factGross = db.prepare("SELECT SUM(gross_sales_jpy_incl) AS p FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr).p || 0;
let listingTotal = 0, listingAvail = false;
try { const r = db.prepare("SELECT SUM(売上金額) AS p FROM f_sales_by_listing WHERE モール='linegift' AND substr(日付,1,7) = ?").get(monthStr); listingTotal = r?.p || 0; listingAvail = listingTotal > 0; }
catch (e) { console.log(`  (listing 突合スキップ: ${e.message})`); }
if (listingAvail) {
  const diffPct = listingTotal !== 0 ? Math.abs(factGross - listingTotal) / Math.abs(listingTotal) * 100 : 0;
  let severity;
  if (isDuplicatePeriod) severity = 'info';  // 重複期間は info に格下げ
  else if (diffPct > THRESHOLDS.listing_diff_pct.error) severity = 'error';
  else if (diffPct > THRESHOLDS.listing_diff_pct.warn) severity = 'warn';
  else severity = 'info';
  recordResult('listing_diff_pct', severity, diffPct, THRESHOLDS.listing_diff_pct.error, { fact_gross_jpy: factGross, listing_jpy: listingTotal, diff_jpy: factGross - listingTotal, duplicate_period: isDuplicatePeriod });
} else { recordResult('listing_diff_pct', 'info', null, THRESHOLDS.listing_diff_pct.error, { skipped: true }); }

// Check 3: missing_cost_rate_pct
const cs = db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN cost_status='missing_cost' THEN 1 ELSE 0 END) AS missing FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const missCostRate = cs.total > 0 ? cs.missing / cs.total * 100 : 0;
recordResult('missing_cost_rate_pct',
  missCostRate > THRESHOLDS.missing_cost_rate_pct.error ? 'error' : missCostRate > THRESHOLDS.missing_cost_rate_pct.warn ? 'warn' : 'info',
  missCostRate, THRESHOLDS.missing_cost_rate_pct.error, { total_rows: cs.total, missing_count: cs.missing });

// Check 4: shipping_missing_rate_pct (Phase A 無効化)
const ss = db.prepare("SELECT COUNT(*) AS total, SUM(CASE WHEN shipping_quality='missing' THEN 1 ELSE 0 END) AS missing FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const shipMissRate = ss.total > 0 ? ss.missing / ss.total * 100 : 0;
recordResult('shipping_missing_rate_pct', 'info', shipMissRate, THRESHOLDS.shipping_missing_rate_pct.error,
  { total_rows: ss.total, missing_count: ss.missing, note: 'Phase A 無効化 (no_shipping_in_api が常態)' });

// Check 5: unmatched_sku_rate_pct
const us = db.prepare("SELECT COUNT(*) AS total, SUM(unresolved_sku_flag) AS unresolved, SUM(CASE WHEN resolution_method='master_match' THEN 1 ELSE 0 END) AS master, SUM(CASE WHEN resolution_method='parent_match' THEN 1 ELSE 0 END) AS parent FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const unresRate = us.total > 0 ? us.unresolved / us.total * 100 : 0;
recordResult('unmatched_sku_rate_pct',
  unresRate > THRESHOLDS.unmatched_sku_rate_pct.error ? 'error' : unresRate > THRESHOLDS.unmatched_sku_rate_pct.warn ? 'warn' : 'info',
  unresRate, THRESHOLDS.unmatched_sku_rate_pct.error, { total_rows: us.total, unresolved: us.unresolved, master_match: us.master, parent_match: us.parent });

// Check 6: fallback_to_item_code_rate_pct (parent_match 件数 = variation.code 欠損 fallback)
const fb = us.parent || 0;
recordResult('fallback_to_item_code_rate_pct',
  fb >= THRESHOLDS.fallback_to_item_code_rate_pct.warn ? 'warn' : 'info',
  fb, THRESHOLDS.fallback_to_item_code_rate_pct.warn, { parent_match_count: fb, note: 'variation.code 欠損 → item.code fallback (LINEギフト API 仕様変化検知)' });

// Check 7: whitelist_coverage_pct (received_date_jst 基準)
const cov = db.prepare(`SELECT
  (SELECT COUNT(*) FROM raw_linegift_orders WHERE substr(received_date_jst,1,7)=? OR (received_date_jst IS NULL AND substr(bought_date_jst,1,7)=?)) AS total,
  (SELECT COUNT(*) FROM raw_linegift_orders WHERE substr(received_date_jst,1,7)=? AND status='received') AS wl`).get(monthStr, monthStr, monthStr);
const wlPct = cov.total > 0 ? cov.wl / cov.total * 100 : 0;
recordResult('whitelist_coverage_pct',
  wlPct < THRESHOLDS.whitelist_coverage_pct.error ? 'error' : wlPct < THRESHOLDS.whitelist_coverage_pct.warn ? 'warn' : 'info',
  wlPct, THRESHOLDS.whitelist_coverage_pct.warn, { total_lines: cov.total, whitelist_lines: cov.wl });

// Check 8: resolved_but_zero_cost_count
const zc = db.prepare("SELECT COUNT(*) AS cnt FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7)=? AND ne_code IS NOT NULL AND units_net_sold > 0 AND (cogs_amount_jpy_incl = 0 OR unit_cost_snapshot_incl IS NULL)").get(monthStr);
recordResult('resolved_but_zero_cost_count',
  zc.cnt >= THRESHOLDS.resolved_but_zero_cost_count.error ? 'error' : zc.cnt >= THRESHOLDS.resolved_but_zero_cost_count.warn ? 'warn' : 'info',
  zc.cnt, THRESHOLDS.resolved_but_zero_cost_count.error, { count: zc.cnt });

// Check 9: fee_rate_drift_pct (LINEギフト特有、12.95% 中央値からのドリフト)
const fr = db.prepare("SELECT SUM(mall_fee_jpy_incl) AS fee, SUM(sales_principal_jpy_incl) AS sales FROM f_linegift_finance_sku_daily_v1 WHERE substr(date_jst,1,7) = ?").get(monthStr);
const actualRate = (fr.sales && fr.sales > 0) ? fr.fee / fr.sales : 0;
const drift = Math.abs(actualRate - FEE_RATE_TARGET) * 100;  // pp
recordResult('fee_rate_drift_pct',
  drift > THRESHOLDS.fee_rate_drift_pct.error ? 'error' : drift > THRESHOLDS.fee_rate_drift_pct.warn ? 'warn' : 'info',
  drift, THRESHOLDS.fee_rate_drift_pct.error, { actual_rate_pct: actualRate * 100, target_rate_pct: FEE_RATE_TARGET * 100, drift_pp: drift, mall_fee_jpy: fr.fee, sales_jpy: fr.sales });

// Check 10: provisional_state_age_days (LINEギフト特有、whitelist 外で 7日以上動いてない注文)
// Codex R1 #4: bought_date_jst は JST、julianday('now') は UTC → JST 0-9時で 1日ずれる罠
// → date('now', '+9 hours') と date(bought_date_jst, '+7 days') の date 文字列比較で JST 統一
const ps = db.prepare(`
  SELECT COUNT(*) AS cnt FROM raw_linegift_orders
  WHERE status IN ('gift_message_send', 'payment', 'gift_message_wait', 'cvs')
    AND substr(bought_date_jst, 1, 7) = ?
    AND date(bought_date_jst, '+7 days') <= date('now', '+9 hours')
`).get(monthStr);
recordResult('provisional_state_age_days',
  ps.cnt >= THRESHOLDS.provisional_state_age_days.warn ? 'warn' : 'info',
  ps.cnt, THRESHOLDS.provisional_state_age_days.warn, { stale_provisional_count: ps.cnt, note: '7日以上 status flip 待ち = 取り込み漏れ可能性' });

// Check 11: horizon_frozen_observed_count (LINEギフト特有、Codex R5 critical 反映で意味変更)
// 意味: monthStr 月に final observation (last_seen_at) を持つ frozen 行数 = 凍結に向かう正常な observation 監視指標。
//   現スキーマには frozen 化時点の snapshot/timestamp が無く、設計書 §8 #11「frozen 行の値変動検知」は厳密実装不能。
//   将来 raw_linegift_orders に frozen_at + frozen_snapshot_hash 列を追加すれば再実装可能。
//   今は info 扱いで件数モニタのみ (運用上は「正常に凍結に向かっている件数」として把握)。
const hfc = db.prepare(`
  SELECT COUNT(*) AS cnt FROM raw_linegift_orders
  WHERE is_frozen_after_horizon = 1
    AND substr(last_seen_at, 1, 7) = ?
`).get(monthStr);
recordResult('horizon_frozen_observed_count',
  'info',
  hfc.cnt, THRESHOLDS.horizon_frozen_observed_count.error, { count: hfc.cnt, note: 'monthStr 月に final observation を持つ frozen 行数 (info 監視指標、現スキーマでは違反検知不能)' });

// Check 12: received_missing_received_on_count (LINEギフト特有、Codex Round 2 #4)
// status='received' なのに received_on_unix が NULL = recognized_on=received_on_jst 方針が崩れた異常。
// Codex R2 #3 反映: received_date_jst が NULL なので bought_date_jst だけで月割りすると「前月購入・当月受取」を
// 当月 gate で見逃すリスク。COALESCE で last_seen_at → bought_date_jst の優先順位で「どこかの月で必ず拾う」
// Codex R4 #2 + R5 #2 反映: TRIM 対象を ASCII space + tab/CR/LF + 全角スペース (U+3000) に明示拡張、
//   NULLIF で空文字も NULL 扱い。日本語データの全角SP混入も取りこぼさない。
//   trim 対象: ' ' (0x20) || char(9) (\t) || char(10) (\n) || char(13) (\r) || '　' (U+3000)
const TRIM_CHARS = "' ' || char(9) || char(10) || char(13) || '　'";
const rmc = db.prepare(`
  SELECT COUNT(*) AS cnt FROM raw_linegift_orders
  WHERE status = 'received'
    AND (received_on_unix IS NULL OR received_on_unix = 0)
    AND COALESCE(
          substr(NULLIF(TRIM(received_date_jst, ${TRIM_CHARS}), ''), 1, 7),
          substr(NULLIF(TRIM(last_seen_at, ${TRIM_CHARS}), ''), 1, 7),
          substr(NULLIF(TRIM(bought_date_jst, ${TRIM_CHARS}), ''), 1, 7)
        ) = ?
`).get(monthStr);
recordResult('received_missing_received_on_count',
  rmc.cnt >= THRESHOLDS.received_missing_received_on_count.error ? 'error' : 'info',
  rmc.cnt, THRESHOLDS.received_missing_received_on_count.error, { count: rmc.cnt, note: 'recognized_on=received_on_jst 方針が API 仕様変化で崩れる検知' });

// Check 13: monthless_received_rows (LINEギフト特有、Codex R3 medium 反映、global 常時)
// status='received' で received_date_jst / last_seen_at / bought_date_jst 全て NULL/'' = 月割不能 = どの月の DQ にも乗らない孤児行。
// fact build 側も received_date_jst IS NOT NULL で弾くため、売上欠落 + DQ 未検知の同時発生を防止する保険 check
// Codex R4 #2 + R5 #2 反映: TRIM 対象を ASCII space + tab/CR/LF + 全角SP に明示拡張、check #12 と整合
const orphan = db.prepare(`
  SELECT COUNT(*) AS cnt FROM raw_linegift_orders
  WHERE status = 'received'
    AND NULLIF(TRIM(received_date_jst, ${TRIM_CHARS}), '') IS NULL
    AND NULLIF(TRIM(last_seen_at, ${TRIM_CHARS}), '') IS NULL
    AND NULLIF(TRIM(bought_date_jst, ${TRIM_CHARS}), '') IS NULL
`).get();
recordResult('monthless_received_rows',
  orphan.cnt >= THRESHOLDS.monthless_received_rows.error ? 'error' : 'info',
  orphan.cnt, THRESHOLDS.monthless_received_rows.error, { count: orphan.cnt, note: '月割不能な received 孤児行 (global、month gate 不能)' });

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
db.close();
process.exit(hasError ? 1 : 0);
