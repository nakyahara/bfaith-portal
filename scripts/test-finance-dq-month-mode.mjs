/**
 * test-finance-dq-month-mode.mjs — apps/warehouse/finance-dq-month-mode.js の単体テスト
 * 使い方: node scripts/test-finance-dq-month-mode.mjs
 *   後半は 5 本の DQ (楽天・Yahoo・au PAY・LINE ギフト・Qoo10) と 3 本の sync を子プロセスで流す (一時の DATA_DIR に SQLite を作る・本番の DB には触らない)。daily-sync は静的に読む
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import {
  monthMode, pickThresholds, modeLabel, RECENT_PAST_GRACE_DAYS,
  monthStartEmptyGrace, monthStartEmptyNote, parseNowArg, resolveDqNow, prevMonthOf,
  MONTH_START_GRACE, MONTH_START_JANUARY_EXTRA_DAYS, monthStartGraceDays, MONTH_START_GRACE_PREFIX, isMonthStartGraceSummary,
  SKIP_IN_MONTH_START_GRACE, applyMonthStartSkip, MONTH_ROW_COUNT_CHECK, monthRowCountDetails, monthHadRowsBefore, prevMonthFreshness, decideMonthStartEmpty,
  ensureMonthHighWater, markMonthHighWater, migrateLegacyHighWater, prepareMonthHighWater, isRealYmd, highWaterReady, shouldSkipEmptyMonthClear,
  MONTH_START_RAMP, MONTH_START_RAMP_CHECKS, MONTH_START_RAMP_PREFIX, monthStartRamp, monthStartRampWindow, applyMonthStartRamp, monthStartRampNote,
  MONTH_START_RAMP_MAX, NO_LISTING_RAMP_FLAG, monthStartRampOlderRange, monthStartRampOlder, listingOlderPart, whitelistOlderPart,
} from '../apps/warehouse/finance-dq-month-mode.js';

let failures = 0;
function check(name, cond, extra = '') {
  if (cond) console.log(`PASS: ${name}`);
  else { failures++; console.log(`FAIL: ${name} ${extra}`); }
}
// JST の日時から UTC Date を作る (JST = UTC+9)
const jst = (y, m, d, h = 12) => new Date(Date.UTC(y, m - 1, d, h - 9));

// ── monthMode ──
check('当月 → current', monthMode('2026-09', { now: jst(2026, 9, 5) }) === 'current');
check('前月・9/1 → recent_past', monthMode('2026-08', { now: jst(2026, 9, 1) }) === 'recent_past');
check('前月・9/14 (境界) → recent_past', monthMode('2026-08', { now: jst(2026, 9, 14) }) === 'recent_past');
check('前月・9/15 → past', monthMode('2026-08', { now: jst(2026, 9, 15) }) === 'past');
check('前々月・9/5 → past', monthMode('2026-07', { now: jst(2026, 9, 5) }) === 'past');
check('来月 (未来) → past 扱い', monthMode('2026-10', { now: jst(2026, 9, 5) }) === 'past');
check('年またぎ: 2027-01-05 に 2026-12 → recent_past', monthMode('2026-12', { now: jst(2027, 1, 5) }) === 'recent_past');
check('年またぎ: 2027-01-20 に 2026-12 → past', monthMode('2026-12', { now: jst(2027, 1, 20) }) === 'past');
check('JST 深夜の日付ずれ: 9/1 00:30 JST (=8/31 15:30 UTC) は 2026-09 が current', monthMode('2026-09', { now: jst(2026, 9, 1, 0.5) }) === 'current');
check('JST 深夜の日付ずれ: 同時刻に 2026-08 は recent_past', monthMode('2026-08', { now: jst(2026, 9, 1, 0.5) }) === 'recent_past');
check('graceDays を変えられる (7 日なら 9/8 は past)', monthMode('2026-08', { now: jst(2026, 9, 8), graceDays: 7 }) === 'past');
check('既定 graceDays は 14', RECENT_PAST_GRACE_DAYS === 14);

// ── pickThresholds ──
const PAST = { row_count_drift: { warn: 0, error: 0 }, whitelist_coverage_pct: { warn: 95, error: 90 }, missing_cost_rate_pct: { warn: 5, error: 10 } };
const CURRENT = { ...PAST, whitelist_coverage_pct: { warn: 70, error: 60 }, missing_cost_rate_pct: { warn: 50, error: 90 } };
const rp = pickThresholds('recent_past', PAST, CURRENT);
check('recent_past: whitelist は CURRENT (70/60)', rp.whitelist_coverage_pct.warn === 70 && rp.whitelist_coverage_pct.error === 60);
check('recent_past: 他のチェックは PAST のまま (missing_cost 5/10)', rp.missing_cost_rate_pct.warn === 5 && rp.missing_cost_rate_pct.error === 10);
check('recent_past: 元の PAST 表を壊さない', PAST.whitelist_coverage_pct.warn === 95);
check('current → CURRENT 表そのもの', pickThresholds('current', PAST, CURRENT) === CURRENT);
check('past → PAST 表そのもの', pickThresholds('past', PAST, CURRENT) === PAST);
const noWl = { row_count_drift: { warn: 0, error: 0 } };
check('whitelist を持たない表 (楽天・Amazon 型) の recent_past は PAST と同じ', pickThresholds('recent_past', noWl, { ...noWl }) === noWl);

// ── 2026-08 の実測で再現: 9/1〜9/5 の Qoo10 coverage は grace 下で error にならない ──
const observed = { '2026-09-01': 76.7, '2026-09-02': 80.6, '2026-09-03': 85.3, '2026-09-04': 89.2, '2026-09-05': 89.2 };
let blocked = 0;
for (const [d, pct] of Object.entries(observed)) {
  const [y, m, day] = d.split('-').map(Number);
  const t = pickThresholds(monthMode('2026-08', { now: jst(y, m, day) }), PAST, CURRENT).whitelist_coverage_pct;
  if (pct <= t.error) blocked++;
}
check('2026-08 実測 (9/1〜9/5) は grace 下で 1 日も error にならない', blocked === 0, `blocked=${blocked}`);
check('同じ実測を PAST 閾値で見ると 5 日全部 error (= 今回の障害)', Object.values(observed).filter((p) => p <= PAST.whitelist_coverage_pct.error).length === 5);

// ── modeLabel ──
check('modeLabel current', modeLabel('current') === 'CURRENT');
check('modeLabel past', modeLabel('past') === 'PAST');
check('modeLabel recent_past は grace を含む', /grace/.test(modeLabel('recent_past')) && /14/.test(modeLabel('recent_past')));

// ── 月初の猶予 (PR #1572。当月 0 行を ⚠️ 警告・exit 0 にしてよいか) ──
// 日数は表 (MONTH_START_GRACE) から読む = 数字を差し替えても境界の試験はそのまま効く
const GD = (mall, ym) => monthStartGraceDays(mall, ym);
const G = (ym, now, mall = 'yahoo') => monthStartEmptyGrace(ym, { now, graceDays: GD(mall, ym) }).grace;
const iso = (s) => new Date(s);
const dd = (n) => String(n).padStart(2, '0');
const throws = (fn) => { try { fn(); return false; } catch { return true; } };

// 表と日数
check('表: 5 モール (楽天・Yahoo・LINE・au PAY・Qoo10) がそろい、書き換えられない',
  ['rakuten', 'yahoo', 'linegift', 'aupay', 'qoo10'].every((m) => Object.isFrozen(MONTH_START_GRACE[m])) && Object.isFrozen(MONTH_START_GRACE) && Object.keys(MONTH_START_GRACE).length === 5);
check('表: 実測からの日数 (楽天・Yahoo・LINE・Qoo10 = 3 / au PAY = 4)',
  MONTH_START_GRACE.rakuten.graceDays === 3 && MONTH_START_GRACE.yahoo.graceDays === 3 && MONTH_START_GRACE.linegift.graceDays === 3 && MONTH_START_GRACE.qoo10.graceDays === 3 && MONTH_START_GRACE.aupay.graceDays === 4);
check('表: 1 月は年末年始の分 (+3) を足す / ほかの月は足さない',
  GD('yahoo', '2027-01') === MONTH_START_GRACE.yahoo.graceDays + MONTH_START_JANUARY_EXTRA_DAYS && MONTH_START_JANUARY_EXTRA_DAYS === 3 && GD('yahoo', '2026-10') === MONTH_START_GRACE.yahoo.graceDays && GD('yahoo', '2026-12') === MONTH_START_GRACE.yahoo.graceDays);
check('表: 1 月を足してもどれも 前月の猶予 (14) 以下', Object.keys(MONTH_START_GRACE).every((m) => GD(m, '2027-01') <= RECENT_PAST_GRACE_DAYS));
check('表: 知らないモールは投げる', throws(() => monthStartGraceDays('amazon', '2026-10')) && throws(() => monthStartGraceDays('__proto__', '2026-10')));

// 暦の上の判定 (条件 ①)
for (const mall of Object.keys(MONTH_START_GRACE)) {
  const g = GD(mall, '2026-10');
  check(`暦 ${mall}: 当月 1 日 → 猶予 / ${g} 日 (最後の日) → 猶予 / ${g + 1} 日 → なし`,
    G('2026-10', jst(2026, 10, 1, 7), mall) && G('2026-10', jst(2026, 10, g, 7), mall) && !G('2026-10', jst(2026, 10, g + 1, 7), mall));
}
check('暦: 前月 (10/1 に 2026-09 = recent_past) → 猶予なし', G('2026-09', jst(2026, 10, 1, 7)) === false);
check('暦: 前々月・1 年前・未来の月 → 猶予なし', !G('2026-08', jst(2026, 10, 1, 7)) && !G('2025-10', jst(2026, 10, 1, 7)) && !G('2026-11', jst(2026, 10, 1, 7)));
const v1 = monthStartEmptyGrace('2026-10', { now: jst(2026, 10, 1, 7), graceDays: 3 });
check('暦: 返り値に mode・JST の日・日数', v1.mode === 'current' && v1.dayOfMonth === 1 && v1.graceDays === 3, JSON.stringify(v1));
// 月末の境界
check('月末: 10/31 23:59 JST の 2026-10 → なし (31 日)', G('2026-10', iso('2026-10-31T23:59:59+09:00')) === false);
check('月末: 11/1 00:00 JST の 2026-11 → 猶予 / 2026-10 (前月になった) → なし', G('2026-11', iso('2026-11-01T00:00:00+09:00')) && !G('2026-10', iso('2026-11-01T00:00:00+09:00')));
check('月末: 2 月 → 3 月 も同じ', G('2027-03', iso('2027-03-01T07:00:00+09:00')) && !G('2027-02', iso('2027-03-01T07:00:00+09:00')));
// 年の境界 (12/31 → 1/1)
check('年: 12/31 23:59:59 JST に 2027-01 (未来) / 2026-12 (31 日) → どちらもなし', !G('2027-01', iso('2026-12-31T23:59:59+09:00')) && !G('2026-12', iso('2026-12-31T23:59:59+09:00')));
check('年: 1/1 00:00 JST に 2027-01 → 猶予 / 2026-12 (前月) → なし', G('2027-01', iso('2027-01-01T00:00:00+09:00')) && !G('2026-12', iso('2027-01-01T00:00:00+09:00')));
const janLast = GD('yahoo', '2027-01');
check(`年: 1 月は ${janLast} 日まで猶予 / ${janLast + 1} 日はなし (年末年始の分)`,
  G('2027-01', iso(`2027-01-${dd(janLast)}T07:00:00+09:00`)) && !G('2027-01', iso(`2027-01-${dd(janLast + 1)}T07:00:00+09:00`)));
// UTC と JST の境界 (UTC 15:00 = JST 翌日 0:00)
check('UTC 境界: 2026-09-30T14:59:59Z (= 9/30 23:59 JST) に 2026-10 → 未来 = なし', G('2026-10', iso('2026-09-30T14:59:59Z')) === false);
check('UTC 境界: 2026-09-30T15:00:00Z (= 10/1 00:00 JST) に 2026-10 → 猶予 (UTC ではまだ 9/30) / 2026-09 → なし', G('2026-10', iso('2026-09-30T15:00:00Z')) && !G('2026-09', iso('2026-09-30T15:00:00Z')));
const yg = GD('yahoo', '2026-10');
check(`UTC 境界: 10/${yg} 23:59 JST (UTC 14:59) → 猶予 / 10/${yg + 1} 00:00 JST (UTC ではまだ 10/${yg}) → なし`,
  G('2026-10', iso(`2026-10-${dd(yg)}T14:59:59Z`)) && !G('2026-10', iso(`2026-10-${dd(yg)}T15:00:00Z`)));
check('UTC 境界: 年 2026-12-31T15:00:00Z (= 1/1 00:00 JST) に 2027-01 → 猶予', G('2027-01', iso('2026-12-31T15:00:00Z')) === true);
check('graceDays 0 → 1 日でも猶予なし', monthStartEmptyGrace('2026-10', { now: jst(2026, 10, 1, 7), graceDays: 0 }).grace === false);
check('graceDays 不正 (未指定 / -1 / 1.5 / 15 / "3") は投げる',
  [undefined, -1, 1.5, 15, '3'].every((d) => throws(() => monthStartEmptyGrace('2026-10', { now: jst(2026, 10, 1), graceDays: d }))));
check('now が日時でなければ投げる', throws(() => monthStartEmptyGrace('2026-10', { now: new Date('x'), graceDays: 3 })));
check('prevMonthOf: 10 月 → 9 月 / 1 月 → 前の年の 12 月', prevMonthOf('2026-10') === '2026-09' && prevMonthOf('2027-01') === '2026-12');

// 最後の行・daily-sync の見分け・飛ばす検査
const note = monthStartEmptyNote('f_yahoo_finance_sku_daily_v1', '2026-10', { calendar: v1 });
check('note: 「⚠️ 月初の猶予:」で始まり 月・日・猶予の日数・CRITICAL になる日を書く', note.startsWith(MONTH_START_GRACE_PREFIX) && note.includes('2026-10') && note.includes('JST 1 日') && note.includes('3 日まで') && note.includes('4 日の朝'), note);
check('isMonthStartGraceSummary: 月初の猶予の行だけ true (検査の warn つきの合格・✅・❌・空は false)',
  isMonthStartGraceSummary(note) && isMonthStartGraceSummary(`  ${note}`) && !isMonthStartGraceSummary('⚠️  DQ gate passed with 2 warning(s)')
  && !isMonthStartGraceSummary('✅ DQ gate passed (no error, no warn)') && !isMonthStartGraceSummary('❌ DQ gate FAILED') && !isMonthStartGraceSummary(undefined));
check('applyMonthStartSkip: 猶予の中は listing_diff / whitelist_coverage / fee_rate_drift を info に (元の判定を残す)・ほかの検査と猶予でない回は変えない', (() => {
  const a = applyMonthStartSkip({}, 'whitelist_coverage_pct', 'error', { x: 1 });
  const b = applyMonthStartSkip({}, 'monthless_received_rows', 'error', null);
  const c = applyMonthStartSkip(null, 'listing_diff_pct', 'error', null);
  return a.severity === 'info' && a.details.severity_without_grace === 'error' && a.details.x === 1 && a.details.skipped_by_month_start_grace === true
    && b.severity === 'error' && c.severity === 'error' && SKIP_IN_MONTH_START_GRACE.join() === 'listing_diff_pct,whitelist_coverage_pct,fee_rate_drift_pct';
})());

// --now は試験専用の env があるときだけ (Low)
check('resolveDqNow: --now が無ければ今', Math.abs(resolveDqNow(null, {}).getTime() - Date.now()) < 5000);
check('resolveDqNow: env FINANCE_DQ_ALLOW_NOW が無いと --now は投げる (本番で月の判定を動かせない)', throws(() => resolveDqNow('2026-10-01T07:00:00+09:00', {})) && throws(() => resolveDqNow('2026-10-01T07:00:00+09:00', { FINANCE_DQ_ALLOW_NOW: 'true' })));
check('resolveDqNow: env FINANCE_DQ_ALLOW_NOW=1 なら受ける', resolveDqNow('2026-10-01T07:00:00+09:00', { FINANCE_DQ_ALLOW_NOW: '1' }).getTime() === jst(2026, 10, 1, 7).getTime());
check('parseNowArg: +09:00 と Z を受ける / 時差の無い日時・日付だけ・でたらめは投げる',
  parseNowArg('2026-09-30T22:00:00Z').getTime() === jst(2026, 10, 1, 7).getTime() && ['2026-10-01T07:00:00', '2026-10-01', 'now', ''].every((s) => throws(() => parseNowArg(s))));

// ── DB を見る判定 (条件 ②③④。メモリの SQLite) ──
const DQ_DDL = `CREATE TABLE dq_run_results (run_id TEXT, check_name TEXT, severity TEXT, actual_value REAL, threshold_value REAL, details_json TEXT, checked_at TEXT, PRIMARY KEY (run_id, check_name));`;
/** DQ の入口と同じく印の表を作り、前の記録の移しも済ませた DB (ensure = false なら作らない / migrate = false なら移さない) */
function memDb({ ensure = true, migrate = true } = {}) { const d = new Database(':memory:'); d.exec(DQ_DDL); d.exec('CREATE TABLE f_yahoo_finance_sku_daily_v1 (date_jst TEXT, k TEXT)'); if (ensure) ensureMonthHighWater(d); if (ensure && migrate) migrateLegacyHighWater(d, 'test'); return d; }
const putDq = (d, runId, check, actual, details, checkedAt = '2026-10-01T22:05:00.000Z') => d.prepare('INSERT INTO dq_run_results VALUES (?, ?, ?, ?, NULL, ?, ?)')
  .run(runId, check, 'info', actual, details === undefined ? null : (typeof details === 'string' ? details : JSON.stringify(details)), checkedAt);
/** 前の PR より前の、全部の検査を流した run を 1 つ置く (row_count_drift + そのモールの検査の名前) */
const LEGACY_CHECKS = {
  yahoo: ['listing_diff_pct', 'missing_cost_rate_pct', 'shipping_missing_rate_pct', 'unresolved_sku_rate_pct', 'whitelist_coverage_pct', 'resolved_but_zero_cost_count', 'normalized_collision_count'],
  aupay: ['normalized_collision_count', 'request_price_reconcile_diff_pct', 'allocation_conservation_diff_pct'],
  linegift: ['fee_rate_drift_pct', 'monthless_received_rows'],
  qoo10: ['settle_price_formula_match_pct', 'match_tier_distribution'],
  rakuten: ['listing_diff_pct', 'date_mismatch_units'],
  amazon: ['monthly_total_diff_pct', 'long_only_skus'],
};
function putLegacyRun(d, runId, mall, rowDetails, checkedAt) {
  putDq(d, runId, 'row_count_drift', rowDetails?.daily_row_count ?? 1, rowDetails, checkedAt);
  for (const c of LEGACY_CHECKS[mall]) putDq(d, runId, c, 0, {}, checkedAt);
}
const hw = (d, mall, ym) => d.prepare('SELECT * FROM dq_month_high_water WHERE mall = ? AND month = ?').get(mall, ym);
{
  const d = memDb();
  check('印: 印が無い → 一度も行が無い (false)', monthHadRowsBefore(d, 'yahoo', '2026-10') === false);
  check('印: 0 行では印を付けない', markMonthHighWater(d, 'yahoo', '2026-10', 0, 'x') === false && monthHadRowsBefore(d, 'yahoo', '2026-10') === false);
  markMonthHighWater(d, 'aupay', '2026-10', 12, 'x'); markMonthHighWater(d, 'yahoo', '2026-09', 12, 'x');
  check('印: ほかのモール・ほかの月の印は数えない', monthHadRowsBefore(d, 'yahoo', '2026-10') === false);
  markMonthHighWater(d, 'yahoo', '2026-10', 5, '2026-10-02T00:00:00Z');
  check('印: 行が 1 以上 → true', monthHadRowsBefore(d, 'yahoo', '2026-10') === true);
  markMonthHighWater(d, 'yahoo', '2026-10', 2, '2026-10-03T00:00:00Z');
  const h = hw(d, 'yahoo', '2026-10');
  check('印: 減らない (5 → 2 を付けても 5)・最初に見た時刻は変わらない', h.max_row_count === 5 && h.first_nonzero_at === '2026-10-02T00:00:00Z', JSON.stringify(h));
  check('印: 表の形で 0 以下の印は入らない (CHECK)', throws(() => d.prepare("INSERT INTO dq_month_high_water VALUES ('yahoo', '2026-11', 'x', 0, 'dq')").run()));
  check('印: 表が無い DB → true (DQ は猶予を使わない向き)', monthHadRowsBefore(memDb({ ensure: false }), 'yahoo', '2026-10') === true);
  ensureMonthHighWater(d);
  check('印: 表を作り直しても (CREATE IF NOT EXISTS) 印は残る', monthHadRowsBefore(d, 'yahoo', '2026-10') === true);
}
{
  // 🚨 R2 High 1: 同じ run_id の再実行で印が消えない (DQ と同じ順: 印を付ける → dq_run_results の DELETE → 0 行の回)
  const d = memDb();
  prepareMonthHighWater(d, { mall: 'yahoo', ym: '2026-10', count: 3, at: 't1' });
  putDq(d, 'same-run', MONTH_ROW_COUNT_CHECK, 3, monthRowCountDetails('yahoo', '2026-10', 3));
  d.prepare('DELETE FROM dq_run_results WHERE run_id = ?').run('same-run');
  prepareMonthHighWater(d, { mall: 'yahoo', ym: '2026-10', count: 0, at: 't2' });
  check('印 (R2 High 1): 同じ run_id の記録を消して 0 行で流し直しても、印は残る (true)', monthHadRowsBefore(d, 'yahoo', '2026-10') === true && hw(d, 'yahoo', '2026-10').max_row_count === 3);
}
{
  // 前の PR より前の記録を 1 回だけ移す (run_id の形に頼らない)
  const d = memDb({ migrate: false });
  putLegacyRun(d, 'dq-yahoo-2026-10-20261001T2205', 'yahoo', { daily_row_count: 0 });
  putLegacyRun(d, 'dq-rakuten-2026-10-20261002T2205', 'rakuten', { daily_row_count: 40 });
  putLegacyRun(d, 'my-manual-check', 'aupay', { daily_row_count: 7 }, '2026-10-02T00:30:00.000Z');          // 手で付けた run_id (JST 10/2 09:30)
  putLegacyRun(d, 'line-test', 'linegift', { expected_latest_date: '2026-10-01', latest_count: 0, ratio: '0.000' }, '2026-10-02T23:00:00.000Z');   // JST 10/3 = 当月の「直近 8 日」の形
  putLegacyRun(d, 'qoo10-hand', 'qoo10', { daily_row_count: 0 }, '2026-10-02T00:00:00.000Z');               // 手で付けた・0 行
  putLegacyRun(d, 'yahoo-broken', 'yahoo', 'not json', '2026-08-15T00:00:00.000Z');                         // 壊れた details = 行があった側 (JST 8/15 = 2026-08)
  putLegacyRun(d, 'dq-amazon-2026-10-x', 'amazon', { daily_row_count: 9 });                                 // Amazon は対象外
  putDq(d, 'no-signature', 'row_count_drift', 5, { daily_row_count: 5 }, '2026-10-02T00:00:00.000Z');       // モールが当てられない
  putDq(d, 'r1-form', MONTH_ROW_COUNT_CHECK, 4, monthRowCountDetails('qoo10', '2026-09', 4));               // R1 の形
  const n = migrateLegacyHighWater(d, 'now');
  check('移す: 既定の run_id の 0 行の run → 印なし (Yahoo 2026-10)', !hw(d, 'yahoo', '2026-10'));
  check('移す: 既定の run_id の行のある run → 印 (楽天 2026-10)', hw(d, 'rakuten', '2026-10')?.max_row_count === 40);
  check('移す (R2): 手で付けた run_id でも、検査の組み合わせでモール・checked_at の JST の月で印 (au PAY 2026-10)', hw(d, 'aupay', '2026-10')?.max_row_count === 7 && hw(d, 'aupay', '2026-10').source === 'legacy');
  check('移す: 手で付けた run_id の LINE の「直近 8 日」の形 → 印 (JST 10/3 = 2026-10)', !!hw(d, 'linegift', '2026-10'));
  check('移す: 手で付けた run_id の 0 行 → 印なし (Qoo10 2026-10)', !hw(d, 'qoo10', '2026-10'));
  check('移す: 壊れた details → 行があった側に数える (Yahoo 2026-08 の印)', !!hw(d, 'yahoo', '2026-08'));
  check('移す: R1 の形 (month_row_count) も数える (Qoo10 2026-09)', hw(d, 'qoo10', '2026-09')?.max_row_count === 4);
  check('移す: Amazon とモールが当てられない run は数えない', d.prepare("SELECT COUNT(*) AS c FROM dq_month_high_water WHERE mall NOT IN ('rakuten','yahoo','aupay','linegift','qoo10')").get().c === 0 && n === 5, `marked=${n}`);
  putLegacyRun(d, 'dq-qoo10-2026-11-later', 'qoo10', { daily_row_count: 3 });
  check('移す: 2 回目は何もしない (済みの印)・後から足した記録は移さない', migrateLegacyHighWater(d, 'now2') === null && !hw(d, 'qoo10', '2026-11'));
  check('移す: dq_run_results が無い DB でも落ちない (移すもの 0)', (() => { const e = new Database(':memory:'); ensureMonthHighWater(e); return migrateLegacyHighWater(e, 'x') === 0; })());
}
{
  const d = memDb();
  const fr = (max) => { d.exec('DELETE FROM f_yahoo_finance_sku_daily_v1'); if (max) d.prepare('INSERT INTO f_yahoo_finance_sku_daily_v1 VALUES (?, ?)').run(max, 'a'); return prevMonthFreshness(d, 'f_yahoo_finance_sku_daily_v1', '2026-10', 5); };
  const a = fr('2026-09-30'); const b = fr('2026-09-25'); const c = fr('2026-09-24'); const e = fr(null);
  check('前月の新しさ: 末日 → ok (0 日) / 5 日前 → ok / 6 日前 → だめ / 前月の行が無い → だめ',
    a.ok && a.gapDays === 0 && b.ok && b.gapDays === 5 && !c.ok && c.gapDays === 6 && !e.ok && e.maxDate === null, JSON.stringify([a, b, c, e]));
  const bad = ['2026-09-99', '2026-09-31', '2026-09-3', '2026-09-30x'].map((s) => fr(s));
  check('前月の新しさ (R2): 実在しない日付 (9/99・9/31)・形の違う日付 → だめ (invalid)', bad.every((x) => !x.ok && x.invalid === true), JSON.stringify(bad));
  check('isRealYmd: 実在の日だけ true (うるう年も見る)', isRealYmd('2028-02-29') && !isRealYmd('2026-02-29') && !isRealYmd('2026-09-31') && !isRealYmd('2026-13-01') && !isRealYmd('2026-9-1') && !isRealYmd(null));
  d.exec('DELETE FROM f_yahoo_finance_sku_daily_v1'); d.prepare('INSERT INTO f_yahoo_finance_sku_daily_v1 VALUES (?, ?)').run('2026-12-28', 'a');
  const j = prevMonthFreshness(d, 'f_yahoo_finance_sku_daily_v1', '2027-01', 5);
  check('前月の新しさ: 年の境界 (1 月に 12/28 まで = 3 日前) → ok', j.ok && j.prevYm === '2026-12' && j.gapDays === 3, JSON.stringify(j));
  check('前月の新しさ: 表の名前が違えば投げる', throws(() => prevMonthFreshness(d, 'f_yahoo_finance_sku_daily_v1; DROP TABLE x', '2026-10', 5)));
}
{
  const d = memDb();
  d.prepare('INSERT INTO f_yahoo_finance_sku_daily_v1 VALUES (?, ?)').run('2026-09-30', 'a');
  const day1 = jst(2026, 10, 1, 7);
  const ok = decideMonthStartEmpty(d, { mall: 'yahoo', ym: '2026-10', now: day1 });
  check('判定: 当月 1 日・印なし・前月は末日まで・禁じていない → 猶予', ok.grace === true && ok.reasons.length === 0 && ok.prev.maxDate === '2026-09-30', JSON.stringify(ok.reasons));
  const ng1 = decideMonthStartEmpty(d, { mall: 'yahoo', ym: '2026-10', now: day1, noGrace: true });
  check('判定: 取込が ❌ (noGrace) → 猶予なし', ng1.grace === false && ng1.reasons.some((r) => r.includes('取込が ❌')));
  const ng2 = decideMonthStartEmpty(d, { mall: 'yahoo', ym: '2026-10', now: jst(2026, 10, GD('yahoo', '2026-10') + 1, 7) });
  check('判定: 猶予を過ぎた → 猶予なし', ng2.grace === false && ng2.reasons.some((r) => r.includes('過ぎた')));
  const ng3 = decideMonthStartEmpty(d, { mall: 'yahoo', ym: '2026-09', now: day1 });
  check('判定: 前月 → 猶予なし', ng3.grace === false && ng3.reasons.includes('当月でない'));
  markMonthHighWater(d, 'yahoo', '2026-10', 3, 'x');
  const ng4 = decideMonthStartEmpty(d, { mall: 'yahoo', ym: '2026-10', now: jst(2026, 10, 2, 7) });
  check('判定 (High 1): 一度 0 でなくなった月の 0 行 → 猶予なし', ng4.grace === false && ng4.hadRowsBefore === true && ng4.reasons.some((r) => r.includes('前に行があった')));
  const d2 = memDb(); d2.prepare('INSERT INTO f_yahoo_finance_sku_daily_v1 VALUES (?, ?)').run('2026-09-20', 'a');
  const ng5 = decideMonthStartEmpty(d2, { mall: 'yahoo', ym: '2026-10', now: day1 });
  check('判定 (High 2): 前月の最新が 9/20 (末日の 10 日前) = 前月の終わりから止まっている → 猶予なし', ng5.grace === false && ng5.reasons.some((r) => r.includes('止まっている疑い')));
  const ng6 = decideMonthStartEmpty(memDb(), { mall: 'yahoo', ym: '2026-10', now: day1 });
  check('判定 (High 2): 前月の行が無い → 猶予なし', ng6.grace === false && ng6.reasons.some((r) => r.includes('行が無い')));
  const d3 = memDb(); d3.prepare('INSERT INTO f_yahoo_finance_sku_daily_v1 VALUES (?, ?)').run('2026-09-99', 'a');
  const ng7 = decideMonthStartEmpty(d3, { mall: 'yahoo', ym: '2026-10', now: day1 });
  check('判定 (R2 Medium 1): 前月の最新の日付が 2026-09-99 (実在しない) → 猶予なし', ng7.grace === false && ng7.reasons.some((r) => r.includes('実在の日でない')), JSON.stringify(ng7.reasons));
}

{
  // 🚨 R3 Medium: 前の記録を移すときの例外は「dq_run_results が無い」だけ 0 件扱い。ほかは投げ直し、移し済みの印を付けない
  const stub = (err) => ({ prepare: (sql) => {
    if (sql.includes('FROM dq_month_high_water_legacy WHERE id = 1')) return { get: () => undefined };
    if (sql.includes('FROM dq_run_results r')) return { all: () => { throw new Error(err); } };
    if (sql.includes('INSERT INTO dq_month_high_water_legacy')) return { run: () => { stub.inserted = true; } };
    throw new Error('想定外の SQL: ' + sql);
  } });
  stub.inserted = false;
  check('移す (R3): 読み取りの失敗 (disk I/O error) は投げ直す・移し済みの印を付けない', throws(() => migrateLegacyHighWater(stub('disk I/O error'), 'x')) && stub.inserted === false);
  check('移す (R3): 「no such table: dq_run_results」だけ 0 件扱いで移し済み', migrateLegacyHighWater(stub('no such table: dq_run_results'), 'x') === 0 && stub.inserted === true);
  check('移す (R3): ほかの表が無いエラー (no such table: dq_run_results_old) は 0 件扱いにしない', throws(() => migrateLegacyHighWater(stub('no such table: dq_run_results_old'), 'x')));
  // 本物の SQLite: 移し済みの表が別の形 (marked の列が無い) → 移しの INSERT が失敗 → 取引ごと戻る → 猶予なし
  const d = memDb({ ensure: false });
  d.exec('CREATE TABLE dq_month_high_water_legacy (id INTEGER PRIMARY KEY, migrated_at TEXT)');
  d.prepare('INSERT INTO f_yahoo_finance_sku_daily_v1 VALUES (?, ?)').run('2026-09-30', 'a');
  const p = prepareMonthHighWater(d, { mall: 'yahoo', ym: '2026-09', count: 5, at: 'x' });
  check('準備 (R3): 移しが失敗すると ok:false・取引ごと戻る (この回の印も付かない)・highWaterReady は false',
    p.ok === false && highWaterReady(d) === false && (() => { try { return !d.prepare("SELECT 1 FROM dq_month_high_water WHERE mall = 'yahoo'").get(); } catch { return true; } })());
  const g = decideMonthStartEmpty(d, { mall: 'yahoo', ym: '2026-10', now: jst(2026, 10, 1, 7) });
  check('判定 (R3): 印を確かめられない DB → 当月 1 日でも猶予なし (理由 = 印を確かめられない)', g.grace === false && g.reasons.some((r) => r.includes('印を確かめられない')), JSON.stringify(g.reasons));
  check('sync (R3): 印を確かめられない DB → 空の chunk を見送らない (今までどおり送る)', shouldSkipEmptyMonthClear(d, { mall: 'yahoo', ym: '2026-10', now: jst(2026, 10, 1, 7) }) === false);
  const ok = memDb(); migrateLegacyHighWater(ok, 'x');
  check('highWaterReady: 表があり移し済み → true / 表が無い → false', highWaterReady(ok) === true && highWaterReady(memDb({ ensure: false })) === false);
}

// ══ 月初の立ち上がり (2026-10-04): 行のある当月の 7 日目 (Qoo10 は 12 日目) までは、listing_diff_pct・whitelist_coverage_pct の error を
//    「足りない向き」かつ「足りない分 ≤ 直近 settleDays 日 + 今日の受注」のときだけ ⚠️ に下げる ══
{
  // 表と暦
  check('立ち上がり: 表は 5 モール・書き換えられない・日数は 1〜14・検査は 2 つだけ',
    Object.isFrozen(MONTH_START_RAMP) && Object.keys(MONTH_START_RAMP).length === 5
    && Object.values(MONTH_START_RAMP).every((s) => Object.isFrozen(s) && Object.isFrozen(s.settleDays) && s.rampDays >= 1 && s.rampDays <= 14
      && Object.keys(s.settleDays).every((k) => MONTH_START_RAMP_CHECKS.includes(k)))
    && MONTH_START_RAMP_CHECKS.length === 2);
  check('立ち上がり: 楽天は whitelist を持たない・Qoo10 は listing (もともと info) を持たない',
    !Object.hasOwn(MONTH_START_RAMP.rakuten.settleDays, 'whitelist_coverage_pct') && !Object.hasOwn(MONTH_START_RAMP.qoo10.settleDays, 'listing_diff_pct'));
  for (const mall of Object.keys(MONTH_START_RAMP)) {
    const n = MONTH_START_RAMP[mall].rampDays;
    check(`立ち上がり ${mall}: 当月 1 日・${n} 日 (境界) は active / ${n + 1} 日は過ぎた / 前月・未来の月は当月でない`,
      monthStartRamp(mall, '2026-10', { now: jst(2026, 10, 1, 7) }).active && monthStartRamp(mall, '2026-10', { now: jst(2026, 10, n, 7) }).active
      && !monthStartRamp(mall, '2026-10', { now: jst(2026, 10, n + 1, 7) }).active
      && !monthStartRamp(mall, '2026-09', { now: jst(2026, 10, 2, 7) }).active && !monthStartRamp(mall, '2026-11', { now: jst(2026, 10, 2, 7) }).active);
  }
  check('立ち上がり: --no-month-start-grace (取込が ❌) なら active にしない・理由を残す',
    (() => { const r = monthStartRamp('rakuten', '2026-10', { now: jst(2026, 10, 2, 7), noGrace: true }); return !r.active && r.reasons.some((x) => x.includes('取込が ❌')); })());
  check('立ち上がり: JST の日付で数える (10/7 23:30 JST は 7 日 / 10/8 00:30 JST は 8 日)',
    monthStartRamp('yahoo', '2026-10', { now: jst(2026, 10, 7, 23.5) }).dayOfMonth === 7 && monthStartRamp('yahoo', '2026-10', { now: jst(2026, 10, 8, 0.5) }).dayOfMonth === 8);
  {
    const j = monthStartRamp('aupay', '2027-01', { now: jst(2027, 1, 9, 7) });
    check(`立ち上がり: 1 月は日数と settleDays に年末年始の ${MONTH_START_JANUARY_EXTRA_DAYS} 日を足す`,
      j.active && j.rampDays === MONTH_START_RAMP.aupay.rampDays + MONTH_START_JANUARY_EXTRA_DAYS && j.settleDays.listing_diff_pct === MONTH_START_RAMP.aupay.settleDays.listing_diff_pct + MONTH_START_JANUARY_EXTRA_DAYS, JSON.stringify(j));
    // R1 Low: rampDays は上限 14 日で丸める (Qoo10 の 1 月は 12 + 3 = 15 → 14 日 = +2)。settleDays は丸めない (6 + 3 = 9)
    const q14 = monthStartRamp('qoo10', '2027-01', { now: jst(2027, 1, 14, 7) });
    const q15 = monthStartRamp('qoo10', '2027-01', { now: jst(2027, 1, 15, 7) });
    check('立ち上がり (R1 Low): Qoo10 の 1 月は上限 14 日に丸める (1/14 は active・1/15 は過ぎた)・settleDays は丸めず +3',
      MONTH_START_RAMP_MAX === 14 && MONTH_START_RAMP.qoo10.rampDays + MONTH_START_JANUARY_EXTRA_DAYS > MONTH_START_RAMP_MAX
      && q14.active && q14.rampDays === MONTH_START_RAMP_MAX && !q15.active && q15.reasons.some((x) => x.includes('14 日まで'))
      && q14.settleDays.whitelist_coverage_pct === MONTH_START_RAMP.qoo10.settleDays.whitelist_coverage_pct + MONTH_START_JANUARY_EXTRA_DAYS, JSON.stringify(q15));
  }
  check('立ち上がり (R1 Medium): --no-listing-ramp は active を変えず listing だけを禁じる (理由を残す)・whitelist は禁じない',
    (() => { const r = monthStartRamp('aupay', '2026-10', { now: jst(2026, 10, 4, 7), noListingRamp: true });
      return r.active && Object.hasOwn(r.deniedChecks, 'listing_diff_pct') && r.deniedChecks.listing_diff_pct.includes(NO_LISTING_RAMP_FLAG) && !Object.hasOwn(r.deniedChecks, 'whitelist_coverage_pct')
        && Object.keys(monthStartRamp('aupay', '2026-10', { now: jst(2026, 10, 4, 7) }).deniedChecks).length === 0; })());
  check('立ち上がり: 知らないモール・壊れた now は投げる', throws(() => monthStartRamp('amazon', '2026-10')) && throws(() => monthStartRamp('yahoo', '2026-10', { now: new Date('x') })));
  // 範囲
  const r4 = monthStartRamp('rakuten', '2026-10', { now: jst(2026, 10, 4, 7) });
  const w4 = monthStartRampWindow(r4, 'listing_diff_pct');
  check('範囲: 10/4・2 日 → 10/2〜10/4 (今日まで)', w4 && w4.from === '2026-10-02' && w4.to === '2026-10-04' && w4.settleDays === 2, JSON.stringify(w4));
  const w2 = monthStartRampWindow(monthStartRamp('rakuten', '2026-10', { now: jst(2026, 10, 2, 7) }), 'listing_diff_pct');
  check('範囲: 10/2 → 前月に出ない (10/1〜10/2)', w2 && w2.from === '2026-10-01' && w2.to === '2026-10-02', JSON.stringify(w2));
  check('範囲: 表に無い検査 (楽天の whitelist)・立ち上がりの外・null は null',
    monthStartRampWindow(r4, 'whitelist_coverage_pct') === null && monthStartRampWindow(monthStartRamp('rakuten', '2026-10', { now: jst(2026, 10, 20, 7) }), 'listing_diff_pct') === null && monthStartRampWindow(null, 'listing_diff_pct') === null);
  // ② 古い部分の範囲 (R1 High) = 月の 1 日〜窓の前の日
  const o4 = monthStartRampOlderRange(w4);
  check('古い部分: 10/4・2 日 → 10/1〜10/1 (窓 10/2〜10/4 の前の日まで)', o4 && !o4.empty && o4.from === '2026-10-01' && o4.to === '2026-10-01', JSON.stringify(o4));
  check('古い部分: 10/2 (窓が 1 日から) → empty / 窓が無い → null', monthStartRampOlderRange(w2)?.empty === true && monthStartRampOlderRange(null) === null);
  {
    const qs = MONTH_START_RAMP.qoo10.settleDays.whitelist_coverage_pct;
    const q10 = monthStartRampOlderRange(monthStartRampWindow(monthStartRamp('qoo10', '2026-10', { now: jst(2026, 10, 10, 7) }), 'whitelist_coverage_pct'));
    check(`古い部分: Qoo10 10/10 (settleDays ${qs}) → 10/1〜10/${dd(9 - qs)}`, q10 && q10.from === '2026-10-01' && q10.to === `2026-10-${dd(9 - qs)}`, JSON.stringify(q10));
  }
  const sev15 = (p) => (p > 15 ? 'error' : p > 5 ? 'warn' : 'info');
  const sevWl = (p) => (p < 70 ? 'error' : p < 80 ? 'warn' : 'info');
  check('古い部分の listing: 累計の差 % をしきい値で判定 / listing 0・fact 0 は info / listing 0・fact あり は error / 数えられない → 投げる',
    listingOlderPart(754197, 730199, sev15).severity === 'info' && listingOlderPart(100, 0, sev15).severity === 'error' && listingOlderPart(100, 90, sev15).severity === 'warn'
    && listingOlderPart(100, 130, sev15).severity === 'error' && listingOlderPart(0, 0, sev15).severity === 'info' && listingOlderPart(0, 5, sev15).severity === 'error'
    && throws(() => listingOlderPart(null, 1, sev15)));
  check('古い部分の whitelist: 入った割合 % をしきい値で判定 / 行 0 は info / 数えられない → 投げる',
    whitelistOlderPart(30, 0, sevWl).severity === 'error' && whitelistOlderPart(29, 29, sevWl).severity === 'info' && whitelistOlderPart(0, 0, sevWl).severity === 'info' && throws(() => whitelistOlderPart(undefined, 0, sevWl)));
  check('古い部分: monthStartRampOlder は窓が無ければ null・古い部分が無ければ empty・数えるのが投げたら severity なし (下げない側)',
    monthStartRampOlder(null, () => ({})) === null && monthStartRampOlder(w2, () => { throw new Error('x'); }).empty === true
    && (() => { const o = monthStartRampOlder(w4, () => { throw new Error('db'); }); return !o.empty && !o.severity && o.error === 'db' && o.from === '2026-10-01'; })()
    && monthStartRampOlder(w4, () => ({ severity: 'info', value: 1, from: 'x', to: 'y' })).from === '2026-10-01');
  // 判定
  // 古い部分は 10/1 に差が無い (10/4 の朝の形: 754,197 円 と 730,199 円 = 3.2%) を既定に
  const OLDER_OK = { empty: false, from: '2026-10-01', to: '2026-10-01', severity: 'info', value: 3.2 };
  const A = (sev, amounts, ramp = r4, name = 'listing_diff_pct') => applyMonthStartRamp(ramp, name, sev, { x: 1 }, { older: OLDER_OK, ...amounts });
  const ok = A('error', { shortfall: 354367, explainedBy: 1075151 });
  check('判定: 足りない向き・直近で説明できる → warn・元の判定と数字を details に残す',
    ok.severity === 'warn' && ok.ramped && ok.details.month_start_ramp === true && ok.details.severity_without_ramp === 'error' && ok.details.x === 1 && ok.details.ramp_shortfall === 354367 && ok.details.ramp_window_from === '2026-10-02');
  const ex = A('error', { shortfall: -720784, explainedBy: 1075151 });
  check('判定 (本物の異常): 実績が多い向き (二重計上・比べる相手が古い) → error のまま・理由', ex.severity === 'error' && !ex.ramped && /多い/.test(ex.details.month_start_ramp_denied));
  const big = A('error', { shortfall: 1719944, explainedBy: 1075151 });
  check('判定 (本物の異常): 足りない分が直近の受注より大きい (前の日の分まで欠けている) → error のまま', big.severity === 'error' && /説明できない/.test(big.details.month_start_ramp_denied));
  check('判定: 足りない分が 0 なのに error (raw が 0 行など) → error のまま', A('error', { shortfall: 0, explainedBy: 0 }).severity === 'error');
  check('判定: 数えられない (null・NaN) → error のまま', A('error', { shortfall: 5, explainedBy: null }).severity === 'error' && A('error', { shortfall: NaN, explainedBy: 9 }).severity === 'error');
  check('判定: warn・info は触らない / 立ち上がりの外・null の ramp・ほかの検査は触らない',
    A('warn', { shortfall: 1, explainedBy: 9 }).severity === 'warn' && A('info', { shortfall: 1, explainedBy: 9 }).details.x === 1 && !A('info', { shortfall: 1, explainedBy: 9 }).details.month_start_ramp
    && A('error', { shortfall: 1, explainedBy: 9 }, monthStartRamp('rakuten', '2026-10', { now: jst(2026, 10, 8, 7) })).severity === 'error'
    && A('error', { shortfall: 1, explainedBy: 9 }, null).severity === 'error' && A('error', { shortfall: 1, explainedBy: 9 }, r4, 'missing_cost_rate_pct').severity === 'error');
  check('判定: 境界 (足りない分 = 直近の受注) は下げる / 1 多いと下げない',
    A('error', { shortfall: 100, explainedBy: 100 }).severity === 'warn' && A('error', { shortfall: 101, explainedBy: 100 }).severity === 'error');
  // ② 古い部分 (R1 High)
  check('判定 (R1 High): 古い部分を確かめていない (older なし) → error のまま・理由',
    (() => { const r = A('error', { shortfall: 100, explainedBy: 300, older: undefined }); return r.severity === 'error' && /確かめていない/.test(r.details.month_start_ramp_denied); })());
  check('判定 (R1 High): 古い部分が error (前の日の欠け) → 直近で説明できても error のまま・理由と数字を details に',
    (() => { const r = A('error', { shortfall: 100, explainedBy: 300, older: { empty: false, from: '2026-10-01', to: '2026-10-01', severity: 'error', value: 100 } });
      return r.severity === 'error' && !r.ramped && /窓より前/.test(r.details.month_start_ramp_denied) && r.details.ramp_older_value === 100 && r.details.ramp_older_severity === 'error'; })());
  check('判定 (R1 High): 古い部分を数えられない (severity なし・おかしな値) → error のまま',
    A('error', { shortfall: 1, explainedBy: 9, older: { empty: false, from: 'a', to: 'b', error: 'db' } }).severity === 'error'
    && A('error', { shortfall: 1, explainedBy: 9, older: { empty: false, from: 'a', to: 'b', severity: 'ok' } }).severity === 'error');
  check('判定: 古い部分が warn (1 日の小さい揺れ・ふだんのしきい値の warn) → 下げる / 古い部分が無い (empty) → 下げる・印を details に',
    A('error', { shortfall: 1, explainedBy: 9, older: { ...OLDER_OK, severity: 'warn', value: 9 } }).severity === 'warn'
    && (() => { const r = A('error', { shortfall: 1, explainedBy: 9, older: { empty: true, from: '2026-10-01', to: null } }); return r.severity === 'warn' && r.details.ramp_older_empty === true; })());
  check('判定 (R1 Medium): --no-listing-ramp の回は listing を下げない (理由 = f_sales) / whitelist は下げる',
    (() => { const rr = monthStartRamp('aupay', '2026-10', { now: jst(2026, 10, 4, 7), noListingRamp: true });
      const l = applyMonthStartRamp(rr, 'listing_diff_pct', 'error', null, { shortfall: 200, explainedBy: 500, older: OLDER_OK });
      const w = applyMonthStartRamp(rr, 'whitelist_coverage_pct', 'error', null, { shortfall: 41, explainedBy: 73, older: OLDER_OK });
      return l.severity === 'error' && /f_sales/.test(l.details.month_start_ramp_denied) && w.severity === 'warn'; })());
  // R1 High の反例 (Codex が前のコードで warn になると確かめた 2 つ) を関数で: 「月全体の足りない分 ≤ 直近の全部の受注」だけでは下げない
  {
    // Qoo10・10 日目: 1〜4 日の 40 件が未配送・5〜10 日の 60 件は配送済み (60% = error)。足りない 40 ≤ 直近の受注 (窓の全部) でも、窓より前は 0%
    const rq = monthStartRamp('qoo10', '2026-10', { now: jst(2026, 10, 10, 7) });
    const wq = monthStartRampWindow(rq, 'whitelist_coverage_pct');
    const oldQ = monthStartRampOlder(wq, (from, to) => { const n = Number(to.slice(8)) - Number(from.slice(8)) + 1; return whitelistOlderPart(n * 10, 0, (p) => (p <= 60 ? 'error' : p <= 70 ? 'warn' : 'info')); });
    const q = applyMonthStartRamp(rq, 'whitelist_coverage_pct', 'error', null, { shortfall: 40, explainedBy: 70, older: oldQ });
    check('反例 (R1 High・Qoo10 10 日目): 1〜4 日の 40 件が未配送・5〜10 日は配送済み → 足りない 40 ≤ 直近 70 でも error のまま (窓より前 0%)',
      q.severity === 'error' && /窓より前/.test(q.details.month_start_ramp_denied) && q.details.ramp_older_value === 0, JSON.stringify(q.details));
    // 楽天・4 日目: 1 日の 100 円が丸ごと欠け・2〜4 日は各 100 円で正常 (25% = error)。足りない 100 ≤ 直近 300 でも、窓より前 (1 日) は 100%
    const rr = monthStartRamp('rakuten', '2026-10', { now: jst(2026, 10, 4, 7) });
    const oldR = monthStartRampOlder(monthStartRampWindow(rr, 'listing_diff_pct'), () => listingOlderPart(100, 0, sev15));
    const r = applyMonthStartRamp(rr, 'listing_diff_pct', 'error', null, { shortfall: 100, explainedBy: 300, older: oldR });
    check('反例 (R1 High・楽天 4 日目): 1 日の 100 円が丸ごと欠け・2〜4 日は正常 → 足りない 100 ≤ 直近 300 でも error のまま (窓より前 100%)',
      r.severity === 'error' && /窓より前/.test(r.details.month_start_ramp_denied) && r.details.ramp_older_from === '2026-10-01' && r.details.ramp_older_to === '2026-10-01', JSON.stringify(r.details));
  }
  const rn = monthStartRampNote(r4, [{ checkName: 'listing_diff_pct', value: 19.37 }]);
  check('最後の行: 「⚠️ 月初の立ち上がり:」で始まり、daily-sync の isMonthStartGraceSummary が拾う / 0 行の猶予の行とは別の印',
    rn.startsWith(MONTH_START_RAMP_PREFIX) && isMonthStartGraceSummary(rn) && rn.includes('listing_diff_pct 19.4%') && rn.includes('7 日まで') && !rn.startsWith(MONTH_START_GRACE_PREFIX), rn);

  // 実測で試す: 2026-06〜10 の当月の DQ で error になった日 (miniPC の dq_run_results) から、settleDays の境目に近いものを選んだ (PR #1613 の本文の表)
  //   [モール, JST の日, 検査, 足りない分, 直近 settleDays 日 + 今日の受注, 古い部分 (null = 窓が 1 日から = 無い / [値 %, 見積もり方])]
  //   足りない分 = その朝の dq_run_results。直近の受注 = 2026-10-04 の f_sales_by_listing / raw で窓 (今日を含む) を数え直した値
  //   古い部分 = Yahoo は出荷日でその朝の状態を作り直した実数・楽天・au PAY・Qoo10 は出荷 (配送完了) の日を持たないので回帰の見積もり
  const OBSERVED = [
    ['rakuten', '2026-09-02', 'listing_diff_pct', 373622, 1327292, null], ['rakuten', '2026-09-06', 'listing_diff_pct', 1387571, 4191077, [0.9, '推定']], ['rakuten', '2026-07-06', 'listing_diff_pct', 1457056, 3455152, [0.8, '推定']],
    ['yahoo', '2026-07-03', 'listing_diff_pct', 337379, 598671, null], ['yahoo', '2026-09-07', 'listing_diff_pct', 273938, 1006585, [3.0, '実数']], ['yahoo', '2026-08-03', 'whitelist_coverage_pct', 190, 601, null],
    ['aupay', '2026-07-04', 'listing_diff_pct', 78446, 165946, [1.0, '推定']], ['aupay', '2026-10-02', 'whitelist_coverage_pct', 22, 43, null], ['aupay', '2026-06-04', 'whitelist_coverage_pct', 35, 87, [96.4, '推定']],
    ['qoo10', '2026-09-11', 'whitelist_coverage_pct', 154, 228, [90.4, '推定']], ['qoo10', '2026-06-12', 'whitelist_coverage_pct', 163, 224, [92.2, '推定']], ['qoo10', '2026-08-08', 'whitelist_coverage_pct', 36, 70, [80.6, '推定']],
    ['qoo10', '2026-06-08', 'whitelist_coverage_pct', 174, 250, [80.6, '推定']],
  ];
  const OBS_SEV = { listing_diff_pct: (p) => (p > 15 ? 'error' : p > 5 ? 'warn' : 'info'), whitelist_coverage_pct: (p, mall) => (mall === 'qoo10' ? (p <= 60 ? 'error' : p <= 70 ? 'warn' : 'info') : (p < 70 ? 'error' : p < 80 ? 'warn' : 'info')) };
  let down = 0;
  for (const [mall, d, name, shortfall, explainedBy, old] of OBSERVED) {
    const [y, m, day] = d.split('-').map(Number);
    const ramp = monthStartRamp(mall, d.slice(0, 7), { now: jst(y, m, day, 8) });
    const older = monthStartRampOlder(monthStartRampWindow(ramp, name), () => ({ severity: OBS_SEV[name](old[0], mall), value: old[0] }));
    const r = applyMonthStartRamp(ramp, name, 'error', null, { shortfall, explainedBy, older });
    if (r.severity === 'warn' && (old === null) === (older?.empty === true)) down++;
  }
  check(`実測: 6〜10 月の月初の空振り (出荷待ち) ${OBSERVED.length} 件は全部 ⚠️ に下がる (古い部分の有る無しも表と合う)`, down === OBSERVED.length, `down=${down}`);
  {
    // Qoo10 の settleDays を 5 にすると、7 日目の古い部分 (1 日の注文) は約半分が配送完了にならず 60% の線を割る (6 にした理由)
    const r7 = monthStartRamp('qoo10', '2026-09', { now: jst(2026, 9, 7, 8) });
    check('実測: Qoo10 の 7 日目は settleDays 6 なら古い部分が無い (5 だと 9/1 の注文が古い部分になり、約半分が未配送で error)',
      MONTH_START_RAMP.qoo10.settleDays.whitelist_coverage_pct === 6 && monthStartRampOlderRange(monthStartRampWindow(r7, 'whitelist_coverage_pct'))?.empty === true);
  }
  const r1004 = (mall) => monthStartRamp(mall, '2026-10', { now: jst(2026, 10, 4, 9) });
  check('実測 (本物): 10/4 の楽天・Yahoo・au PAY の listing (比べる相手が 10/2 のまま = fact が多い) は ❌ のまま',
    [['rakuten', -720784, 1075151], ['yahoo', -182836, 281513], ['aupay', -52491, 124699]].every(([m, s, e]) => applyMonthStartRamp(r1004(m), 'listing_diff_pct', 'error', null, { shortfall: s, explainedBy: e, older: OLDER_OK }).severity === 'error'));
  check('実測 (本物): 10/4 は f_sales の再構築が打ち切られた朝 → --no-listing-ramp なら向きに頼らず ❌',
    ['rakuten', 'yahoo', 'aupay'].every((m) => applyMonthStartRamp(monthStartRamp(m, '2026-10', { now: jst(2026, 10, 4, 9), noListingRamp: true }), 'listing_diff_pct', 'error', null, { shortfall: 200, explainedBy: 500, older: OLDER_OK }).severity === 'error'));
}

// ── 5 本の DQ と 3 本の sync を子プロセスで (一時の DATA_DIR の SQLite・本番の DB には触らない) ──
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const readSql = (rel) => fs.readFileSync(path.join(repoRoot, rel), 'utf8');
const COMMON_DDL = `${DQ_DDL}
  CREATE TABLE m_products (商品コード TEXT, 原価状態 TEXT, 原価 REAL);
  CREATE TABLE f_sales_by_listing (日付 TEXT, モール TEXT, 売上金額 REAL);
  CREATE TABLE sync_contracts (entity TEXT PRIMARY KEY, contract_version INTEGER, source_object TEXT, target_table TEXT);
  INSERT INTO sync_contracts VALUES ('aupay_finance_sku_daily', 1, 'f_aupay_finance_sku_daily_v1', 'mirror_aupay_finance_sku_daily'),
    ('linegift_finance_sku_daily', 1, 'f_linegift_finance_sku_daily_v1', 'mirror_linegift_finance_sku_daily'),
    ('qoo10_finance_sku_daily', 1, 'f_qoo10_finance_sku_daily_v1', 'mirror_qoo10_finance_sku_daily');`;
const RAW_DDL = {
  rakuten: `CREATE TABLE raw_rakuten_orders (order_date TEXT, order_status INTEGER, item_number TEXT);
    CREATE TABLE fact_returns (モール TEXT, 注文日 TEXT, モール商品コード TEXT, 数量 INTEGER);`,
  yahoo: 'CREATE TABLE raw_yahoo_orders (order_time TEXT, order_status TEXT, pay_status TEXT, ship_status TEXT);',
  aupay: 'CREATE TABLE raw_aupay_orders (order_id TEXT, order_date TEXT, order_status TEXT, item_cancel_status TEXT, coupon_total_price REAL);',
  linegift: `CREATE TABLE raw_linegift_orders (order_id TEXT PRIMARY KEY, status TEXT, sku_code TEXT, stock_count INTEGER, selling_price REAL, fee REAL, bought_date_jst TEXT, received_date_jst TEXT,
    bought_on_unix INTEGER, received_on_unix INTEGER, first_seen_at TEXT, last_seen_at TEXT, is_frozen_after_horizon INTEGER NOT NULL DEFAULT 0);`,
  qoo10: readSql('sql/qoo10/raw_qoo10_orders.sql'),
};
const MALLS = ['rakuten', 'yahoo', 'aupay', 'linegift', 'qoo10'].map((mall) => ({ mall, table: `f_${mall}_finance_sku_daily_v1`, script: `run-${mall}-finance-dq.js` }));
/** fact に 1 行足す (NOT NULL の列は型に合う値で埋める。検査が error にならない値) */
function addFactRow(db, table, date, key = 'k1') {
  const cols = db.prepare(`PRAGMA table_info(${table})`).all();
  // CHECK (列 IN ('a', ...)) の列は最初の値を使う (DDL から読む)
  const ddl = db.prepare(`SELECT sql FROM sqlite_master WHERE name = ?`).get(table).sql;
  const allowed = Object.fromEntries([...ddl.matchAll(/(\w+)\s+IN\s*\(\s*'([^']+)'/g)].map((m) => [m[1], m[2]]));
  const v = {};
  for (const c of cols) {
    if (c.name === 'date_jst') v[c.name] = date;
    else if (c.pk) v[c.name] = key;
    else if (c.name === 'cost_status') v[c.name] = 'complete';
    else if (c.notnull && c.dflt_value == null && allowed[c.name]) v[c.name] = allowed[c.name];
    else if (c.notnull && c.dflt_value == null) v[c.name] = /INT|REAL|NUM/i.test(c.type) ? 0 : 'x';
  }
  const names = Object.keys(v);
  db.prepare(`INSERT INTO ${table} (${names.join(', ')}) VALUES (${names.map(() => '?').join(', ')})`).run(...names.map((n) => v[n]));
}
function makeDir(mall, table) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `dq-month-start-${mall}-`));
  const db = new Database(path.join(dir, 'warehouse.db'));
  db.exec(COMMON_DDL); db.exec(RAW_DDL[mall]); db.exec(readSql(`sql/${mall}/${table}.sql`));
  db.close();
  return dir;
}
const withDb = (dir, fn) => { const d = new Database(path.join(dir, 'warehouse.db')); try { return fn(d); } finally { d.close(); } };
// env: DATA_DIR は --data-dir より優先 = 必ず一時のディレクトリで上書きする (本番の DATA_DIR を継がない)。--now は試験専用の env と一緒に
function runNode(dir, rel, args, { allowNow = true } = {}) {
  const env = { ...process.env, DATA_DIR: dir };
  delete env.FINANCE_DQ_ALLOW_NOW; delete env.RENDER_MIRROR_URL; delete env.MIRROR_SYNC_KEY;
  if (allowNow) env.FINANCE_DQ_ALLOW_NOW = '1';
  const r = spawnSync(process.execPath, [path.join(repoRoot, rel), ...args], { encoding: 'utf8', env });
  const out = String(r.stdout || '').trim();
  return { code: r.status, out, err: String(r.stderr || ''), last: out.split('\n').pop() || '' };
}
const runDq = (dir, script, month, now, runId, extra = [], opt) => runNode(dir, `apps/warehouse/${script}`, ['--data-dir', dir, '--month', month, '--run-id', runId, ...(now ? ['--now', now] : []), ...extra], opt);
const res = (dir, runId) => withDb(dir, (d) => Object.fromEntries(d.prepare('SELECT check_name, severity, details_json FROM dq_run_results WHERE run_id = ?').all(runId).map((x) => [x.check_name, { severity: x.severity, details: x.details_json ? JSON.parse(x.details_json) : null }])));
const DAY = (n, ym = '2026-10') => `${ym}-${dd(n)}T07:00:00+09:00`;

for (const { mall, table, script } of MALLS) {
  const dir = makeDir(mall, table);
  try {
    withDb(dir, (d) => addFactRow(d, table, '2026-09-30'));          // 前月は末日まで入っている (新しい)
    const g = GD(mall, '2026-10');

    const r1 = runDq(dir, script, '2026-10', DAY(1), 't-day1');
    const s1 = res(dir, 't-day1');
    check(`${mall}: 当月 1 日の 0 行 → exit 0・最後の行が ⚠️ 月初の猶予・row_count_drift = warn・印 (month_row_count 0) を残す`,
      r1.code === 0 && isMonthStartGraceSummary(r1.last) && s1.row_count_drift?.severity === 'warn' && s1.row_count_drift.details.month_start_grace === true && s1[MONTH_ROW_COUNT_CHECK]?.details?.month_row_count === 0,
      `code=${r1.code} last=${r1.last} err=${r1.err.slice(-300)}`);
    check(`${mall}: 猶予の中も ほかの検査を流す (早く終わらない = 記録が row_count_drift と印だけではない)`, Object.keys(s1).length > 3, Object.keys(s1).join(','));

    const r2 = runDq(dir, script, '2026-10', DAY(g), 't-last');
    check(`${mall}: 猶予の最後の日 (${g} 日) の 0 行 → exit 0`, r2.code === 0 && res(dir, 't-last').row_count_drift?.severity === 'warn', `code=${r2.code} last=${r2.last}`);
    const r3 = runDq(dir, script, '2026-10', DAY(g + 1), 't-after');
    check(`${mall}: 猶予を過ぎた当月 (${g + 1} 日) の 0 行 → exit 1・CRITICAL・error`, r3.code === 1 && /CRITICAL/.test(r3.err) && res(dir, 't-after').row_count_drift?.severity === 'error', `code=${r3.code}`);
    const r4 = runDq(dir, script, '2026-09', DAY(1), 't-prev-month', [], {});
    // 前月 (9 月) は行がある = 0 行ではない。0 行の前月は別のディレクトリで見る
    check(`${mall}: 行のある前月は 0 行の扱いにならない (exit 0 か検査の結果しだい・CRITICAL の 0 行ではない)`, !/のデータが 0 行/.test(r4.err), r4.err.slice(-200));
    const r5 = runDq(dir, script, '2026-10', '2026-09-30T14:59:59Z', 't-utc-before');
    const r6 = runDq(dir, script, '2026-10', '2026-09-30T15:00:00Z', 't-utc-after');
    check(`${mall}: UTC 14:59:59Z (= 9/30 JST) に 2026-10 → 未来の月 = exit 1 / 15:00Z (= 10/1 JST) → exit 0`, r5.code === 1 && r6.code === 0, `codes=${r5.code},${r6.code}`);
    const r7l = runDq(dir, script, '2026-10', DAY(1), 't-nolistingramp', [NO_LISTING_RAMP_FLAG]);
    check(`${mall} (R1 Medium): --no-listing-ramp (f_sales が ❌) だけなら 0 行の猶予は止めない → 当月 1 日の 0 行は exit 0・⚠️ 月初の猶予`,
      r7l.code === 0 && isMonthStartGraceSummary(r7l.last) && r7l.last.startsWith(MONTH_START_GRACE_PREFIX), `code=${r7l.code} last=${r7l.last} ${r7l.err.slice(-200)}`);
    const r7 = runDq(dir, script, '2026-10', DAY(1), 't-nograce', ['--no-month-start-grace']);
    check(`${mall}: --no-month-start-grace (daily-sync: 取込が ❌) → 当月 1 日でも exit 1・CRITICAL`, r7.code === 1 && /取込が ❌/.test(r7.err), `code=${r7.code}`);
    const r8 = runDq(dir, script, '2026-10', DAY(1), 't-noenv', [], { allowNow: false });
    check(`${mall}: env FINANCE_DQ_ALLOW_NOW が無い --now → exit 2 (FATAL)`, r8.code === 2 && /試験専用/.test(r8.err), `code=${r8.code}`);
    const r9 = runDq(dir, script, '2026-10', '2026-10-01T07:00:00', 't-badnow');
    check(`${mall}: --now に時差が無い → exit 2`, r9.code === 2, `code=${r9.code}`);

    // High 1: 一度 0 でなくなった月 → 0 行に戻ると猶予の中でも CRITICAL
    withDb(dir, (d) => addFactRow(d, table, '2026-10-01'));
    const h1 = runDq(dir, script, '2026-10', DAY(2), 't-had-rows');
    check(`${mall}: 当月に行がある回 (2 日) は印 month_row_count = 1 を残す`, res(dir, 't-had-rows')[MONTH_ROW_COUNT_CHECK]?.details?.month_row_count === 1, `code=${h1.code} ${h1.err.slice(-200)}`);
    withDb(dir, (d) => d.prepare(`DELETE FROM ${table} WHERE substr(date_jst, 1, 7) = '2026-10'`).run());
    const h2 = runDq(dir, script, '2026-10', DAY(2), 't-vanished');
    check(`${mall} (High 1): 一度 0 でなくなった月が 0 行に戻った → 猶予の中 (2 日) でも exit 1・CRITICAL・理由 = 前に行があった`,
      h2.code === 1 && /前に行があった/.test(h2.err) && res(dir, 't-vanished').row_count_drift?.severity === 'error', `code=${h2.code} ${h2.err.slice(-300)}`);
    check(`${mall}: 印 dq_month_high_water に この月の印 (max_row_count 1・source dq) が残っている`,
      withDb(dir, (d) => d.prepare("SELECT max_row_count, source FROM dq_month_high_water WHERE mall = ? AND month = '2026-10'").get(mall))?.max_row_count === 1);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
  // 🚨 R2 High 1: 同じ run_id で流し直す (行のある回 → 行が消えて同じ run_id でもう一度)
  const dir3 = makeDir(mall, table);
  try {
    withDb(dir3, (d) => { addFactRow(d, table, '2026-09-30'); addFactRow(d, table, '2026-10-01'); });
    const s1 = runDq(dir3, script, '2026-10', DAY(2), 'same-run-id');
    withDb(dir3, (d) => d.prepare(`DELETE FROM ${table} WHERE substr(date_jst, 1, 7) = '2026-10'`).run());
    const s2 = runDq(dir3, script, '2026-10', DAY(2), 'same-run-id');
    check(`${mall} (R2 High 1): 同じ run_id で 行あり → 0 行 と流し直すと exit 1・CRITICAL (前の記録が消えても印は残る)`,
      s1.code !== null && s2.code === 1 && /前に行があった/.test(s2.err) && res(dir3, 'same-run-id').row_count_drift?.severity === 'error', `codes=${s1.code},${s2.code} ${s2.err.slice(-300)}`);
  } finally {
    fs.rmSync(dir3, { recursive: true, force: true });
  }
  // R2 High 1: この PR より前に、手で付けた run_id で「行がある」を見た記録だけがある DB (初回に印へ移す)
  const dir4 = makeDir(mall, table);
  try {
    withDb(dir4, (d) => { addFactRow(d, table, '2026-09-30'); putLegacyRun(d, 'my-own-check', mall, { daily_row_count: 12 }, '2026-10-01T23:00:00.000Z'); });   // JST 10/2 08:00
    const l = runDq(dir4, script, '2026-10', DAY(2), 't-legacy');
    check(`${mall} (R2 High 1): PR 前の手で付けた run_id の「行あり」の記録 → 印へ移り、0 行の 2 日は exit 1・CRITICAL`,
      l.code === 1 && /前に行があった/.test(l.err), `code=${l.code} ${l.err.slice(-300)}`);
  } finally {
    fs.rmSync(dir4, { recursive: true, force: true });
  }
  // High 2: 前月の終わりから止まっている (前月の最新が 9/20) → 1 日でも CRITICAL
  const dir2 = makeDir(mall, table);
  try {
    withDb(dir2, (d) => addFactRow(d, table, '2026-09-20'));
    const s = runDq(dir2, script, '2026-10', DAY(1), 't-stale');
    check(`${mall} (High 2): 前月の最新が 9/20 (末日の 10 日前) → 当月 1 日でも exit 1・CRITICAL`, s.code === 1 && /止まっている疑い/.test(s.err), `code=${s.code} ${s.err.slice(-300)}`);
    withDb(dir2, (d) => addFactRow(d, table, '2026-09-99', 'k9'));
    const bd = runDq(dir2, script, '2026-10', DAY(1), 't-bad-date');
    check(`${mall} (R2 Medium 1): 前月の最新の日付が 2026-09-99 (実在しない) → 当月 1 日でも exit 1・CRITICAL`, bd.code === 1 && /実在の日でない/.test(bd.err), `code=${bd.code} ${bd.err.slice(-300)}`);
    const p = runDq(dir2, script, '2026-08', DAY(1), 't-past-empty');
    check(`${mall}: 前々月 (2026-08) の 0 行 → exit 1・CRITICAL`, p.code === 1 && /のデータが 0 行/.test(p.err), `code=${p.code}`);
  } finally {
    fs.rmSync(dir2, { recursive: true, force: true });
  }
}

// R3 Medium (子プロセス): 印の準備が失敗する DB → DQ はほかの検査を流しつつ、当月 1 日でも CRITICAL
for (const { mall, table, script } of MALLS) {
  const dir = makeDir(mall, table);
  try {
    withDb(dir, (d) => { addFactRow(d, table, '2026-09-30'); d.exec('CREATE TABLE dq_month_high_water_legacy (id INTEGER PRIMARY KEY, migrated_at TEXT)'); });
    const r = runDq(dir, script, '2026-10', DAY(1), 't-hw-broken');
    check(`${mall} (R3 Medium): 印の準備が失敗する DB → 当月 1 日の 0 行でも exit 1・理由 = 印を確かめられない`,
      r.code === 1 && /印を確かめられない/.test(r.err) && /印を準備できない/.test(r.err), `code=${r.code} ${r.err.slice(-300)}`);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}

// Medium 1: 猶予の中も 全期間・raw の検査は流れる (LINE の monthless_received_rows) / 飛ばすのは行数で比べる検査だけ
{
  const dir = makeDir('linegift', 'f_linegift_finance_sku_daily_v1');
  try {
    withDb(dir, (d) => {
      addFactRow(d, 'f_linegift_finance_sku_daily_v1', '2026-09-30');
      d.prepare(`INSERT INTO raw_linegift_orders (order_id, status, sku_code, stock_count, selling_price, fee) VALUES ('orphan', 'received', 'a', 1, 1000, 130)`).run();
    });
    const r = runDq(dir, 'run-linegift-finance-dq.js', '2026-10', DAY(1), 't-orphan');
    const s = res(dir, 't-orphan');
    check('linegift (Medium 1): 猶予の中でも 月の分からない received の行 (monthless_received_rows) は error → exit 1',
      r.code === 1 && s.monthless_received_rows?.severity === 'error' && s.row_count_drift?.severity === 'warn', `code=${r.code} ${JSON.stringify(s.monthless_received_rows)}`);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}
{
  const dir = makeDir('yahoo', 'f_yahoo_finance_sku_daily_v1');
  try {
    withDb(dir, (d) => {
      addFactRow(d, 'f_yahoo_finance_sku_daily_v1', '2026-09-30');
      for (let i = 0; i < 5; i++) d.prepare(`INSERT INTO raw_yahoo_orders VALUES ('2026-10-01 00:1${i}:00', '2', '1', '1')`).run();   // 1 日の受注・まだ出荷していない
    });
    const r = runDq(dir, 'run-yahoo-finance-dq.js', '2026-10', DAY(1), 't-wl');
    const s = res(dir, 't-wl');
    check('yahoo (Medium 1): 猶予の中は whitelist_coverage (出荷の進み具合) を info に落とし、元の判定 (error) を details に残す → exit 0',
      r.code === 0 && s.whitelist_coverage_pct?.severity === 'info' && s.whitelist_coverage_pct.details.severity_without_grace === 'error', `code=${r.code} ${JSON.stringify(s.whitelist_coverage_pct)}`);
    const r2 = runDq(dir, 'run-yahoo-finance-dq.js', '2026-10', DAY(GD('yahoo', '2026-10') + 1), 't-wl-after');
    check('yahoo: 猶予を過ぎたら CRITICAL で止まる (whitelist を info に落とすのは猶予の中だけ)', r2.code === 1, `code=${r2.code}`);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}

// High 1 (sync): 猶予の中で一度も 0 でなくなっていない月の 0 行 → 空の chunk を送らない / 一度 0 でなくなった月の 0 行 → 今までどおり送る
for (const mall of ['aupay', 'linegift', 'qoo10']) {
  const table = `f_${mall}_finance_sku_daily_v1`;
  const dir = makeDir(mall, table);
  try {
    const sync = (now, opt) => runNode(dir, `apps/warehouse/sync-${mall}-finance-daily.js`, ['--data-dir', dir, '--month', '2026-10', '--dry-run', '--now', now], opt);
    const z = sync(DAY(1));
    check(`sync ${mall}: 印の表がまだ無い (DQ が一度も流れていない) → 判定できない = 今までどおり空の chunk を送る`, z.code === 0 && /empty chunk/.test(z.out), `code=${z.code} ${z.out.slice(-300)}`);
    withDb(dir, (d) => { ensureMonthHighWater(d); migrateLegacyHighWater(d, 'test'); });   // DQ が一度流れた後と同じ (印の表があり、前の記録の移しが済み)
    const a = sync(DAY(1));
    check(`sync ${mall}: 猶予の中・印なしの 0 行 → 空の chunk を送らない (Render を消さない)・exit 0`, a.code === 0 && /Render のその月を消さない/.test(a.out) && !/empty chunk/.test(a.out), `code=${a.code} ${a.out.slice(-300)} ${a.err.slice(-200)}`);
    const b = sync(DAY(GD(mall, '2026-10') + 1));
    check(`sync ${mall}: 猶予を過ぎた 0 行 → 今までどおり空の chunk で消す`, b.code === 0 && /empty chunk/.test(b.out) && !/Render のその月を消さない/.test(b.out), `code=${b.code} ${b.out.slice(-300)}`);
    withDb(dir, (d) => markMonthHighWater(d, mall, '2026-10', 4, 'x'));
    const c = sync(DAY(2));
    check(`sync ${mall}: 一度 0 でなくなった月の 0 行 → 猶予の中でも今までどおり空の chunk を送る`, c.code === 0 && /empty chunk/.test(c.out), `code=${c.code} ${c.out.slice(-300)}`);
    const e = sync(DAY(1), { allowNow: false });
    check(`sync ${mall}: env が無い --now → exit 2`, e.code === 2, `code=${e.code}`);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}

// ── 月初の立ち上がりを子プロセスで: 2026-10-04 の朝の数字の形 (楽天・Yahoo・au PAY・Qoo10)・月半ば・月末・本物の異常・LINE ギフトの 0 行 ──
{
  const FACT_COL = { rakuten: 'gross_sales_jpy_incl', yahoo: 'listing_sales_estimated_jpy_incl', aupay: 'gross_sales_jpy_incl' };
  // 2026-10-04 の朝の形 (f_sales_by_listing は 11:30 の作り直しの後の値 = 07:00 に f_sales が通っていればこうだった)
  const OCT = {
    rakuten: { fact: { '2026-10-01': 730199, '2026-10-02': 635378, '2026-10-03': 109404 }, listing: { '2026-10-01': 754197, '2026-10-02': 649348, '2026-10-03': 425803 } },
    yahoo: { fact: { '2026-10-01': 207355, '2026-10-02': 142865, '2026-10-03': 52915 }, listing: { '2026-10-01': 220299, '2026-10-02': 149795, '2026-10-03': 131718 },
      raw: [['2026-10-01', 130, 129], ['2026-10-02', 100, 96], ['2026-10-03', 92, 32]] },
    aupay: { fact: { '2026-10-01': 40345, '2026-10-02': 21896, '2026-10-03': 30595 }, listing: { '2026-10-01': 40345, '2026-10-02': 27392, '2026-10-03': 97307 },
      raw: [['2026-10-01', 29, 29], ['2026-10-02', 14, 14], ['2026-10-03', 57, 18], ['2026-10-04', 2, 0]] },
  };
  const addRaw = (d, mall, rows) => {
    let n = 0;
    for (const [date, total, wl] of rows) {
      for (let i = 0; i < total; i++) {
        const done = i < wl;
        n++;
        if (mall === 'yahoo') d.prepare('INSERT INTO raw_yahoo_orders VALUES (?, ?, ?, ?)').run(`${date} 10:00:00`, done ? '5' : '2', '1', done ? '3' : '1');
        else if (mall === 'aupay') d.prepare('INSERT INTO raw_aupay_orders VALUES (?, ?, ?, ?, 0)').run(`o${n}`, `${date.replace(/-/g, '/')} 10:00:00`, done ? '完了' : '新規受付', 'N');
        else if (mall === 'qoo10') d.prepare(`INSERT INTO raw_qoo10_orders (order_id, source_type, source_order_key, pack_no, shipping_status, item_code, seller_item_code, order_date, first_seen_at, last_seen_at, last_api_snapshot_at, synced_at)
          VALUES (?, 'api_v3', ?, ?, ?, 'i', 's', ?, 'x', 'x', 'x', 'x')`).run(`api:${n}`, String(n), n, done ? 'Delivered(5)' : 'Shipping(4)', `${date} 10:00:00`);
      }
    }
  };
  const setup = (mall, spec) => {
    const table = `f_${mall}_finance_sku_daily_v1`;
    const dir = makeDir(mall, table);
    withDb(dir, (d) => {
      addFactRow(d, table, '2026-09-30', 'prev');
      let k = 0;
      for (const [date, p] of Object.entries(spec.fact || {})) {
        addFactRow(d, table, date, `k${++k}`);
        if (FACT_COL[mall]) d.prepare(`UPDATE ${table} SET ${FACT_COL[mall]} = ? WHERE rowid = (SELECT MAX(rowid) FROM ${table})`).run(p);
      }
      for (const [date, p] of Object.entries(spec.listing || {})) d.prepare('INSERT INTO f_sales_by_listing (日付, モール, 売上金額) VALUES (?, ?, ?)').run(date, mall, p);
      if (mall === 'aupay') d.prepare("UPDATE f_aupay_finance_sku_daily_v1 SET mall_fee_calc_method = 'estimated_rate'").run();   // 既定の unknown だと別の検査 (mall_fee_rate_missing_pct) が error
      addRaw(d, mall, spec.raw || []);
    });
    return { dir, table, script: `run-${mall}-finance-dq.js` };
  };
  const at = (day, h = 9) => `2026-10-${dd(day)}T${dd(h)}:00:00+09:00`;
  const errLines = (r) => r.out.split('\n').filter((l) => /❌/.test(l)).join(' | ');

  for (const mall of ['rakuten', 'yahoo', 'aupay']) {
    const { dir, script } = setup(mall, OCT[mall]);
    try {
      const n = MONTH_START_RAMP[mall].rampDays;
      const a = runDq(dir, script, '2026-10', at(4), 'r-oct4');
      const s = res(dir, 'r-oct4');
      check(`${mall} (10/4 の朝の形): listing_diff_pct は ⚠️ warn に下がり (元は error)・exit 0・最後の行が ⚠️ 月初の立ち上がり`,
        a.code === 0 && s.listing_diff_pct?.severity === 'warn' && s.listing_diff_pct.details.severity_without_ramp === 'error' && a.last.startsWith(MONTH_START_RAMP_PREFIX) && isMonthStartGraceSummary(a.last),
        `code=${a.code} last=${a.last} ld=${JSON.stringify(s.listing_diff_pct)} err=${a.err.slice(-300)} ${errLines(a)}`);
      if (mall === 'aupay') check('aupay (10/4 の朝の形): whitelist_coverage_pct 59.8% も ⚠️ に下がる (入っていない 41 行 ≤ 10/2〜10/4 の 73 行)',
        s.whitelist_coverage_pct?.severity === 'warn' && s.whitelist_coverage_pct.details.ramp_shortfall === 41 && s.whitelist_coverage_pct.details.ramp_explained_by === 73, JSON.stringify(s.whitelist_coverage_pct));
      if (mall === 'yahoo') check('yahoo (10/4 の朝の形): whitelist_coverage_pct 79.8% はもともと warn = 触らない (立ち上がりの印なし)',
        s.whitelist_coverage_pct?.severity === 'warn' && !s.whitelist_coverage_pct.details.month_start_ramp, JSON.stringify(s.whitelist_coverage_pct));
      const b = runDq(dir, script, '2026-10', at(n + 1), 'r-after');
      check(`${mall}: 同じ数字でも ${n + 1} 日 (立ち上がりの外) なら今までどおり ❌ (exit 1・listing_diff_pct error)`, b.code === 1 && res(dir, 'r-after').listing_diff_pct?.severity === 'error', `code=${b.code}`);
      const c = runDq(dir, script, '2026-10', at(4), 'r-nograce', ['--no-month-start-grace']);
      check(`${mall}: 取込が ❌ の朝 (--no-month-start-grace) は下げない → exit 1`, c.code === 1 && res(dir, 'r-nograce').listing_diff_pct?.severity === 'error', `code=${c.code}`);
      const nl = runDq(dir, script, '2026-10', at(4), 'r-nolisting', [NO_LISTING_RAMP_FLAG]);
      const snl = res(dir, 'r-nolisting');
      check(`${mall} (R1 Medium): f_sales の再構築が ❌ の朝 (--no-listing-ramp) は listing を下げない → exit 1・理由 = f_sales`,
        nl.code === 1 && snl.listing_diff_pct?.severity === 'error' && /f_sales/.test(snl.listing_diff_pct.details.month_start_ramp_denied || ''), `code=${nl.code} ${JSON.stringify(snl.listing_diff_pct)}`);
      if (mall === 'aupay') check('aupay (R1 Medium): --no-listing-ramp でも whitelist (raw どうし) の立ち上がりは続ける (⚠️ のまま)',
        snl.whitelist_coverage_pct?.severity === 'warn' && snl.whitelist_coverage_pct.details.month_start_ramp === true, JSON.stringify(snl.whitelist_coverage_pct));
      check(`${mall} (R1 High): 10/4 の朝の形の古い部分 (10/1) は差が小さい (error でない) = 数字を details に残す`,
        s.listing_diff_pct?.details?.ramp_older_from === '2026-10-01' && s.listing_diff_pct.details.ramp_older_to === '2026-10-01' && s.listing_diff_pct.details.ramp_older_severity !== 'error'
        && Number.isFinite(s.listing_diff_pct.details.ramp_older_value), JSON.stringify(s.listing_diff_pct?.details));
      // 本物の異常 1: 比べる相手が古い (10/4 の本物の朝 = f_sales が打ち切られて listing が 10/1 だけ) → fact が多い向き → ❌
      withDb(dir, (d) => d.prepare("DELETE FROM f_sales_by_listing WHERE 日付 > '2026-10-01'").run());
      const e = runDq(dir, script, '2026-10', at(4), 'r-stale');
      const se = res(dir, 'r-stale');
      check(`${mall} (本物の異常): 比べる相手が古い (fact が多い向き) → 立ち上がりの中でも ❌ (exit 1)・理由を details に`,
        e.code === 1 && se.listing_diff_pct?.severity === 'error' && /多い/.test(se.listing_diff_pct.details.month_start_ramp_denied || ''), `code=${e.code} ${JSON.stringify(se.listing_diff_pct)}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  }
  // 本物の異常 2: fact が前の日の分まで欠けている (10/1 と 10/2 の出荷が丸ごと無い) → 足りない分 > 直近 2 日 + 今日の受注 → ❌
  {
    const { dir, script } = setup('rakuten', { fact: { '2026-10-03': 109404 }, listing: OCT.rakuten.listing });
    try {
      const r = runDq(dir, script, '2026-10', at(4), 'r-missing');
      const s = res(dir, 'r-missing');
      check('rakuten (本物の異常): fact が 10/1・10/2 の分まで欠けている → 立ち上がりの中でも ❌ (窓より前の 10/1 が 100% 欠け)',
        r.code === 1 && s.listing_diff_pct?.severity === 'error' && /窓より前/.test(s.listing_diff_pct.details.month_start_ramp_denied || '') && s.listing_diff_pct.details.ramp_older_value === 100,
        `code=${r.code} ${JSON.stringify(s.listing_diff_pct)}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  }
  // R1 High の反例 (楽天・4 日目): 1 日の 100 円が丸ごと欠け・2〜4 日は各 100 円で正常 (25% = error)。前のコードは 足りない 100 ≤ 直近 300 で ⚠️ にしていた
  {
    const days = { '2026-10-01': 100, '2026-10-02': 100, '2026-10-03': 100, '2026-10-04': 100 };
    const { dir, script } = setup('rakuten', { fact: { '2026-10-02': 100, '2026-10-03': 100, '2026-10-04': 100 }, listing: days });
    try {
      const r = runDq(dir, script, '2026-10', at(4), 'r-codex');
      const s = res(dir, 'r-codex');
      check('反例 (R1 High・楽天 4 日目・子プロセス): 1 日の 100 円が丸ごと欠け → 足りない 100 ≤ 直近 300 でも ❌ (exit 1)・理由 = 窓より前 (10/1 が 100%)',
        r.code === 1 && s.listing_diff_pct?.severity === 'error' && s.listing_diff_pct.details.ramp_shortfall === 100 && s.listing_diff_pct.details.ramp_explained_by === 300
        && /窓より前/.test(s.listing_diff_pct.details.month_start_ramp_denied || '') && s.listing_diff_pct.details.ramp_older_value === 100, `code=${r.code} ${JSON.stringify(s.listing_diff_pct)}`);
      withDb(dir, (d) => addFactRow(d, 'f_rakuten_finance_sku_daily_v1', '2026-10-01', 'k-fix'));
      withDb(dir, (d) => d.prepare("UPDATE f_rakuten_finance_sku_daily_v1 SET gross_sales_jpy_incl = 100 WHERE rakuten_code = 'k-fix'").run());
      const ok = runDq(dir, script, '2026-10', at(4), 'r-codex-fixed');
      check('反例の対 (楽天): 1 日の分が入れば差 0% = info・exit 0 (下げる必要も無い)', ok.code === 0 && res(dir, 'r-codex-fixed').listing_diff_pct?.severity === 'info', `code=${ok.code} ${errLines(ok)}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  }
  // R1 Medium の反例 (楽天・4 日目): f_sales が古い = 比べる相手 1000・fact 800 (20% = error)・窓の listing 500。古い部分 (10/1) は 500 と 480 で正常
  //   → f_sales が ✅ の朝なら ⚠️、❌ の朝 (--no-listing-ramp) は 200 ≤ 500 でも ❌
  {
    const { dir, script } = setup('rakuten', { fact: { '2026-10-01': 480, '2026-10-02': 200, '2026-10-03': 120 }, listing: { '2026-10-01': 500, '2026-10-02': 250, '2026-10-03': 250 } });
    try {
      const a = runDq(dir, script, '2026-10', at(4), 'r-fsales-ok');
      const b = runDq(dir, script, '2026-10', at(4), 'r-fsales-ng', [NO_LISTING_RAMP_FLAG]);
      const sb = res(dir, 'r-fsales-ng');
      check('反例 (R1 Medium・楽天): 1000・800・窓 500 → f_sales ✅ の朝は ⚠️ (exit 0) / f_sales ❌ の朝 (--no-listing-ramp) は 200 ≤ 500 でも ❌ (exit 1)',
        a.code === 0 && res(dir, 'r-fsales-ok').listing_diff_pct?.severity === 'warn' && b.code === 1 && sb.listing_diff_pct?.severity === 'error'
        && sb.listing_diff_pct.details.ramp_shortfall === 200 && sb.listing_diff_pct.details.ramp_explained_by === 500 && /f_sales/.test(sb.listing_diff_pct.details.month_start_ramp_denied || ''),
        `codes=${a.code},${b.code} ${JSON.stringify(sb.listing_diff_pct)}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  }
  // 月半ば・月末: 立ち上がりの外は今までどおり (小さい差は info で通る / 月半ばに差が急に増えたら ❌)
  {
    const days = {}; const lst = {};
    for (let i = 1; i <= 30; i++) { const d = `2026-10-${dd(i)}`; days[d] = 500000; lst[d] = 510000; }
    const { dir, script } = setup('rakuten', { fact: days, listing: lst });
    try {
      const m = runDq(dir, script, '2026-10', at(15), 'r-mid');
      check('rakuten (月半ば 10/15): 差 2% は今までどおり info・exit 0・立ち上がりの行は出ない', m.code === 0 && res(dir, 'r-mid').listing_diff_pct?.severity === 'info' && !m.last.startsWith(MONTH_START_RAMP_PREFIX), `code=${m.code} last=${m.last} ${errLines(m)}`);
      const e = runDq(dir, script, '2026-10', at(31), 'r-end');
      check('rakuten (月末 10/31): 差 2% は info・exit 0', e.code === 0 && res(dir, 'r-end').listing_diff_pct?.severity === 'info', `code=${e.code}`);
      withDb(dir, (d) => d.prepare("UPDATE f_rakuten_finance_sku_daily_v1 SET gross_sales_jpy_incl = 0 WHERE date_jst BETWEEN '2026-10-10' AND '2026-10-14'").run());
      const g = runDq(dir, script, '2026-10', at(15), 'r-mid-gap');
      const sg = res(dir, 'r-mid-gap');
      check('rakuten (本物の異常・月半ば): 10/10〜14 の fact が消えた (差 18%) → 今までどおり ❌ (立ち上がりの外は下げない・判定の跡も付けない)',
        g.code === 1 && sg.listing_diff_pct?.severity === 'error' && !sg.listing_diff_pct.details.month_start_ramp_denied && !sg.listing_diff_pct.details.ramp_window_from, `code=${g.code} ${JSON.stringify(sg.listing_diff_pct)}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  }
  // Qoo10: 10/4 の朝の形 (whitelist 34.4% = daily-sync が exit 1 になった原因) → ⚠️・13 日なら ❌・本物の停滞 (古い注文の配送が止まった) は ❌
  {
    const q = (rows, nFact = 10) => {
      const r = setup('qoo10', { raw: rows });
      withDb(r.dir, (d) => { for (let i = 0; i < nFact; i++) addFactRow(d, r.table, '2026-10-01', `q${i}`); });
      return r;
    };
    const { dir, script } = q([['2026-10-01', 14, 10], ['2026-10-02', 7, 1], ['2026-10-03', 9, 0], ['2026-10-04', 2, 0]]);
    try {
      const a = runDq(dir, script, '2026-10', at(4), 'q-oct4');
      const s = res(dir, 'q-oct4');
      check('qoo10 (10/4 の朝の形): whitelist_coverage_pct 34.4% → ⚠️ warn (入っていない 21 行 ≤ 10/1〜10/4 の 32 行)',
        s.whitelist_coverage_pct?.severity === 'warn' && s.whitelist_coverage_pct.details.severity_without_ramp === 'error' && s.whitelist_coverage_pct.details.ramp_shortfall === 21,
        `code=${a.code} ${JSON.stringify(s.whitelist_coverage_pct)} ${errLines(a)}`);
      const errs = Object.entries(s).filter(([, v]) => v.severity === 'error').map(([k]) => k);
      check('qoo10 (10/4 の朝の形): error の検査が 0 = exit 0 (daily-sync の dq_fail にならない)・最後の行が ⚠️ 月初の立ち上がり', a.code === 0 && errs.length === 0 && a.last.startsWith(MONTH_START_RAMP_PREFIX), `code=${a.code} errors=${errs.join(',')} last=${a.last}`);
      const n = MONTH_START_RAMP.qoo10.rampDays;
      const b = runDq(dir, script, '2026-10', at(n + 1), 'q-after');
      check(`qoo10: 同じ数字でも ${n + 1} 日なら今までどおり ❌`, b.code === 1 && res(dir, 'q-after').whitelist_coverage_pct?.severity === 'error', `code=${b.code}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
    const rows = []; for (let i = 1; i <= 10; i++) rows.push([`2026-10-${dd(i)}`, 10, i <= 2 ? 10 : 0]);   // 10/3 以降の注文が 1 件も配送完了にならない
    const st = q(rows);
    try {
      const r = runDq(st.dir, st.script, '2026-10', at(10), 'q-stuck');
      check('qoo10 (本物の異常): 10/3〜10/10 の注文が 1 件も配送完了にならない (入っていない 80 行 > 10/4〜10/10 の 70 行) → 立ち上がりの中でも ❌',
        r.code === 1 && res(st.dir, 'q-stuck').whitelist_coverage_pct?.severity === 'error', `code=${r.code} ${JSON.stringify(res(st.dir, 'q-stuck').whitelist_coverage_pct)}`);
    } finally { fs.rmSync(st.dir, { recursive: true, force: true }); }
    // R1 High の反例 (Qoo10・10 日目): 1〜4 日の 40 件が未配送・5〜10 日の 60 件は全部配送済み (60% = error)。前のコードは 足りない 40 ≤ 直近 60 で ⚠️ にしていた
    const cx = []; for (let i = 1; i <= 10; i++) cx.push([`2026-10-${dd(i)}`, 10, i <= 4 ? 0 : 10]);
    const qc = q(cx);
    try {
      const r = runDq(qc.dir, qc.script, '2026-10', at(10), 'q-codex');
      const sq = res(qc.dir, 'q-codex').whitelist_coverage_pct;
      check('反例 (R1 High・Qoo10 10 日目・子プロセス): 1〜4 日の 40 件が未配送 → 足りない 40 ≤ 直近の受注でも ❌ (exit 1)・理由 = 窓より前 (10/1〜10/3 が 0%)',
        r.code === 1 && sq?.severity === 'error' && sq.details.ramp_shortfall === 40 && sq.details.ramp_shortfall <= sq.details.ramp_explained_by
        && /窓より前/.test(sq.details.month_start_ramp_denied || '') && sq.details.ramp_older_value === 0 && sq.details.ramp_older_from === '2026-10-01', `code=${r.code} ${JSON.stringify(sq)}`);
    } finally { fs.rmSync(qc.dir, { recursive: true, force: true }); }
  }
  // LINE ギフト: 10/4 の 0 行 (取込が 10/1 から止まっている) は立ち上がりの対象ではない = 今までどおり ❌ (0 行の猶予は 3 日まで)
  {
    const { dir, script } = setup('linegift', {});
    try {
      const a = runDq(dir, script, '2026-10', at(4), 'l-oct4', ['--no-month-start-grace']);
      const b = runDq(dir, script, '2026-10', at(4), 'l-oct4-flag-off');
      check('linegift (10/4 の朝の形): 当月 0 行 → --no-month-start-grace でも無しでも exit 1・CRITICAL (立ち上がりは行のある月だけ)',
        a.code === 1 && b.code === 1 && /のデータが 0 行/.test(a.err) && /のデータが 0 行/.test(b.err) && !a.last.startsWith(MONTH_START_RAMP_PREFIX), `codes=${a.code},${b.code}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  }
  // R1 High (whitelist の古い部分を Yahoo・LINE ギフトの SQL でも): 窓より前の受注が止まっていれば、足りない分 ≤ 直近の受注でも ❌ / 止まっていなければ ⚠️
  {
    // Yahoo・4 日目 (窓 10/2〜10/4): 10/1 の 30 件が出荷されない・10/2〜10/4 の 30 件は出荷済み → 25% (error)・足りない 30 ≤ 直近 30
    const bad = setup('yahoo', { fact: { '2026-10-01': 1000 }, listing: { '2026-10-01': 1000 }, raw: [['2026-10-01', 30, 0], ['2026-10-02', 10, 10], ['2026-10-03', 10, 10], ['2026-10-04', 10, 10]] });
    const good = setup('yahoo', { fact: { '2026-10-01': 1000 }, listing: { '2026-10-01': 1000 }, raw: [['2026-10-01', 10, 10], ['2026-10-02', 10, 0], ['2026-10-03', 10, 0], ['2026-10-04', 10, 0]] });
    try {
      runDq(bad.dir, bad.script, '2026-10', at(4), 'y-old-stuck');
      runDq(good.dir, good.script, '2026-10', at(4), 'y-old-ok');
      const b = res(bad.dir, 'y-old-stuck').whitelist_coverage_pct; const g = res(good.dir, 'y-old-ok').whitelist_coverage_pct;
      check('yahoo (R1 High): whitelist の古い部分 (10/1) の 30 件が出荷されない → 足りない 30 ≤ 直近 30 でも ❌・理由 = 窓より前 / 10/1 が出荷済みで 10/2〜10/4 が待ち → ⚠️',
        b?.severity === 'error' && /窓より前/.test(b.details.month_start_ramp_denied || '') && b.details.ramp_older_value === 0 && b.details.ramp_shortfall === 30 && b.details.ramp_explained_by === 30
        && g?.severity === 'warn' && g.details.month_start_ramp === true && g.details.ramp_older_value === 100, `bad=${JSON.stringify(b)} good=${JSON.stringify(g)}`);
    } finally { fs.rmSync(bad.dir, { recursive: true, force: true }); fs.rmSync(good.dir, { recursive: true, force: true }); }
    // LINE ギフト・5 日目 (窓 10/2〜10/5): 10/1 の 10 件が受け取られない・10/2〜10/5 は 40 件のうち 20 件が受取済み → 40% (error)・足りない 30 ≤ 直近 40
    const lg = (rows) => {
      const r = setup('linegift', {});
      withDb(r.dir, (d) => {
        addFactRow(d, r.table, '2026-10-02', 'l-oct');
        let n = 0;
        for (const [bought, total, received] of rows) for (let i = 0; i < total; i++) {
          n++;
          const done = i < received;
          d.prepare('INSERT INTO raw_linegift_orders (order_id, status, sku_code, stock_count, selling_price, fee, bought_date_jst, received_date_jst) VALUES (?, ?, ?, 1, 1000, 100, ?, ?)')
            .run(`lg${n}`, done ? 'received' : 'gift_message_send', 'sku', bought, done ? '2026-10-05' : null);
        }
      });
      return r;
    };
    const lbad = lg([['2026-10-01', 10, 0], ['2026-10-02', 10, 5], ['2026-10-03', 10, 5], ['2026-10-04', 10, 5], ['2026-10-05', 10, 5]]);
    const lgood = lg([['2026-10-01', 10, 10], ['2026-10-02', 10, 0], ['2026-10-03', 10, 0], ['2026-10-04', 10, 0], ['2026-10-05', 10, 0]]);
    try {
      runDq(lbad.dir, lbad.script, '2026-10', at(5), 'l-old-stuck');
      runDq(lgood.dir, lgood.script, '2026-10', at(5), 'l-old-ok');
      const b = res(lbad.dir, 'l-old-stuck').whitelist_coverage_pct; const g = res(lgood.dir, 'l-old-ok').whitelist_coverage_pct;
      check('linegift (R1 High): whitelist の古い部分 (10/1) の 10 件が受け取られない → 足りない 30 ≤ 直近 40 でも ❌・理由 = 窓より前 / 10/1 が受取済みで 10/2〜10/5 が待ち → ⚠️',
        b?.severity === 'error' && /窓より前/.test(b.details.month_start_ramp_denied || '') && b.details.ramp_older_value === 0 && b.details.ramp_shortfall === 30 && b.details.ramp_explained_by === 40
        && g?.severity === 'warn' && g.details.month_start_ramp === true && g.details.ramp_older_value === 100, `bad=${JSON.stringify(b)} good=${JSON.stringify(g)}`);
    } finally { fs.rmSync(lbad.dir, { recursive: true, force: true }); fs.rmSync(lgood.dir, { recursive: true, force: true }); }
  }
  // #1572 と重ならない: 当月 0 行の猶予の回は「⚠️ 月初の猶予:」の行 (立ち上がりの行ではない)
  {
    const { dir, script } = setup('yahoo', { listing: { '2026-10-01': 220299 }, raw: [['2026-10-01', 130, 53]] });
    try {
      const a = runDq(dir, script, '2026-10', at(1), 'y-empty');
      const s = res(dir, 'y-empty');
      check('yahoo: 当月 0 行の猶予 (1 日) は今までどおり 0 行の猶予の行・listing と whitelist は info (立ち上がりで二重に触らない)',
        a.code === 0 && a.last.startsWith(MONTH_START_GRACE_PREFIX) && s.listing_diff_pct?.severity === 'info' && s.whitelist_coverage_pct?.severity === 'info' && !s.whitelist_coverage_pct.details.month_start_ramp,
        `code=${a.code} last=${a.last} ${JSON.stringify(s.whitelist_coverage_pct)}`);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  }
}

// ── daily-sync (静的に読む。本物の daily-sync は流さない) ──
{
  const src = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  check('daily-sync: --now も FINANCE_DQ_ALLOW_NOW も渡さない (試験専用の口を本番で使わない)', !/--now\b/.test(src) && !src.includes('FINANCE_DQ_ALLOW_NOW'));
  const calls = [
    ['run-rakuten-finance-dq.js', 'rkResult', 'rakutenFinanceDqResult'],
    ['run-yahoo-finance-dq.js', 'yahooResult', 'yahooFinanceDqResult'],
    ['run-aupay-finance-dq.js', 'aupayResult', 'aupayFinanceDqResult'],
    ['run-linegift-finance-dq.js', 'linegiftResult', 'linegiftFinanceDqResult'],
    ['run-qoo10-finance-dq.js', 'qoo10Result', 'qoo10FinanceDqResult'],
  ];
  for (const [script, imp, v] of calls) {
    check(`daily-sync: ${script} に取込の結果で猶予の禁止 (monthStartGraceFlag(${imp})) と f_sales の結果で listing の立ち上がりの禁止 (listingRampFlag(fSalesResult)) を渡し、結果に warn を付ける`,
      src.includes(`${script} --data-dir \${DATA_DIR_ARG} --month \${`) && new RegExp(`${script.replace(/\./g, '\\.')} --data-dir \\$\\{DATA_DIR_ARG\\} --month \\$\\{\\w+\\}\\$\\{monthStartGraceFlag\\(${imp}(, \\.\\.\\.\\w+)?\\)\\}\\$\\{listingRampFlag\\(fSalesResult\\)\\}\``).test(src)
      && src.includes(`...${v}, warn: dqMonthStartWarn(${v}) }`));
  }
  check('daily-sync (R1 Medium): f_sales の再構築 (fSalesResult) は当月の finance DQ より前に流れる',
    src.indexOf("const fSalesResult = runScript('apps/warehouse/rebuild-f-sales.js'") > 0 && src.indexOf("const fSalesResult = runScript('apps/warehouse/rebuild-f-sales.js'") < src.indexOf('run-rakuten-finance-dq.js --data-dir'));
  check('daily-sync: Yahoo は月初の猶予の間、前月も build → DQ → (DQ が通れば) sync', /Yahoo finance build \$\{yahooPrevYm\} \(月初の前月\)/.test(src) && /Yahoo finance DQ \$\{yahooPrevYm\} \(月初の前月\)/.test(src)
    && /if \(yahooPrevDq\.success\) \{\n\s+const yahooPrevSync = runScript\(\n\s+`apps\/warehouse\/sync-yahoo-finance-daily\.js --data-dir \$\{DATA_DIR_ARG\} --month \$\{yahooPrevYm\}`/.test(src));
  check('daily-sync (R2 Medium 2): 前月の build と DQ の結果を当月の DQ の旗に渡す (どちらか ❌ なら猶予を禁じる)',
    src.includes('yahooPrevSteps.push(yahooPrevBuild);') && src.includes('yahooPrevSteps.push(yahooPrevDq);') && src.includes('monthStartGraceFlag(yahooResult, ...yahooPrevSteps)')
    && src.indexOf('yahooPrevSteps.push(yahooPrevDq);') < src.indexOf('monthStartGraceFlag(yahooResult, ...yahooPrevSteps)'));
  check('daily-sync (R2 Low 1): 1 行ごとの印は resultIcon (warn を見る)', src.includes('const icon = resultIcon(r);') && !src.includes("const icon = r.skipped ? '⏸️' : (r.success ? '✅' : '❌');"));
  // 補助の関数を取り出して動かす (見出し allOk の式は daily-sync の本物と同じかも確かめる)
  const fnSrc = (name) => { const m = src.match(new RegExp(`function ${name}\\([^)]*\\) \\{[^\\n]*\\}`)); return m ? m[0] : null; };
  const flagSrc = fnSrc('monthStartGraceFlag'); const warnSrc = fnSrc('dqMonthStartWarn'); const iconSrc = fnSrc('resultIcon'); const lrSrc = fnSrc('listingRampFlag');
  check('daily-sync (R1 Medium): listingRampFlag がある', !!lrSrc);
  if (lrSrc) {
    // eslint-disable-next-line no-new-func
    const lr = new Function(`${lrSrc}\nreturn listingRampFlag;`)();
    check('daily-sync (R1 Medium): f_sales ✅ → 旗なし / ❌ (打ち切り)・見送り (gated)・結果なし → --no-listing-ramp',
      lr({ success: true }) === '' && lr({ success: false }) === ` ${NO_LISTING_RAMP_FLAG}` && lr({ success: false, blocked: true, gated: true }) === ` ${NO_LISTING_RAMP_FLAG}` && lr(undefined) === ` ${NO_LISTING_RAMP_FLAG}`);
  }
  const allOkSrc = 'const allOk = results.every(r => r.success && r.warn !== true) && urgentWarnings.length === 0 && diskWarnings.length === 0;';
  check('daily-sync: 補助の関数 3 つと見出しの式がある', !!flagSrc && !!warnSrc && !!iconSrc && src.includes(allOkSrc));
  const grace0 = () => ({ success: true, summary: note });
  if (flagSrc && warnSrc && iconSrc) {
    // eslint-disable-next-line no-new-func
    const f = new Function('isMonthStartGraceSummary', `${flagSrc}\n${warnSrc}\n${iconSrc}\nreturn { monthStartGraceFlag, dqMonthStartWarn, resultIcon };`)(isMonthStartGraceSummary);
    check('daily-sync: 取込が ✅ → 旗なし / ❌ → --no-month-start-grace', f.monthStartGraceFlag({ success: true }) === '' && f.monthStartGraceFlag({ success: false }) === ' --no-month-start-grace' && f.monthStartGraceFlag(undefined) === ' --no-month-start-grace');
    const ok = { success: true }; const ng = { success: false };
    // 🚨 R3 High: 取込の設定が足りない朝は取込が ❌ (exit 0 で何もせず抜けない) → daily-sync は猶予を禁じる → 当月 0 行は CRITICAL
    // 取込のスクリプトは一時のディレクトリで流す (.env を読まない・本番の DATA_DIR を使わない)。設定は空にして、網には出ない
    const runImport = (rel, envOver) => {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'dq-import-'));
      try {
        const env = { ...process.env, DATA_DIR: tmp, ...envOver };
        const r = spawnSync(process.execPath, [path.join(repoRoot, rel), '7'], { encoding: 'utf8', env, cwd: tmp, timeout: 60000 });
        return { code: r.status, out: `${r.stdout}\n${r.stderr}` };
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    };
    const au = runImport('apps/warehouse/aupay-orders.js', { AUPAY_PROXY_SECRET: '', AUPAY_API_KEY: '' });
    check('取込 (R3 High): au PAY の設定 (AUPAY_PROXY_SECRET) が無い → exit 1 (何もせず exit 0 で抜けない)', au.code === 1 && /設定が足りない/.test(au.out), `code=${au.code} ${au.out.slice(-300)}`);
    for (const [mall, rel, envOver, msg] of [
      ['rakuten', 'apps/warehouse/rakuten-orders.js', { RAKUTEN_SERVICE_SECRET: '', RAKUTEN_LICENSE_KEY: '' }, /環境変数が不足/],
      ['yahoo', 'apps/warehouse/yahoo-orders.js', { YAHOO_PROXY_SECRET: '', AUPAY_PROXY_SECRET: '' }, /FATAL: YAHOO_PROXY_SECRET/],
      ['qoo10', 'apps/warehouse/qoo10-orders.js', { QOO10_CERT_KEY: '' }, /QOO10_CERT_KEY 未設定/],
    ]) {
      const r = runImport(rel, envOver);
      check(`取込 (R3 High): ${mall} の設定が無い → exit 1 (前から ❌。同じ穴は無い)`, r.code === 1 && msg.test(r.out), `code=${r.code} ${r.out.slice(-200)}`);
    }
    {
      // LINE ギフトは取込が repo の data/ に鍵の lock を置くので流さず、設定が無いと投げて exit 1 になる形を読む
      const lg = fs.readFileSync(path.join(repoRoot, 'apps/warehouse/linegift-orders.js'), 'utf8').replace(/\r\n/g, '\n');
      check('取込 (R3 High): linegift は設定 (LINEGIFT_ACCESS_TOKEN) が無いと投げ、catch で process.exit(1) (同じ穴は無い)',
        lg.includes("if (!accessToken) throw new Error('LINEGIFT_ACCESS_TOKEN 未設定") && /\} catch \(e\) \{\n\s+releaseLock\(\);\n\s+console\.error\(`\[linegift\] 致命的エラー[^\n]*\n\s+process\.exit\(1\);/.test(lg));
    }
    check('daily-sync (R3 High): 取込は --dry-run を付けずに流す (dry-run は何もせず exit 0 なので)', !/runScript\('apps\/warehouse\/(rakuten|yahoo|aupay|qoo10|linegift)-orders\.js[^']*--dry-run/.test(src));
    {
      // 通しで: au PAY の取込 (設定の欠け) → daily-sync の旗 → au PAY の当月 DQ (1 日・前月は新しい・印なし) → exit 1
      const auResult = { success: au.code === 0 };
      const flag = f.monthStartGraceFlag(auResult).trim();
      const dir = makeDir('aupay', 'f_aupay_finance_sku_daily_v1');
      try {
        withDb(dir, (d) => addFactRow(d, 'f_aupay_finance_sku_daily_v1', '2026-09-30'));
        const r = runDq(dir, 'run-aupay-finance-dq.js', '2026-10', DAY(1), 't-au-import-ng', flag ? [flag] : []);
        const r0 = runDq(dir, 'run-aupay-finance-dq.js', '2026-10', DAY(1), 't-au-import-ok', []);
        check('通し (R3 High): au PAY の取込が設定の欠けで ❌ の朝 → --no-month-start-grace が渡り、当月 1 日の 0 行は exit 1・CRITICAL (取込 ✅ の朝なら exit 0)',
          flag === '--no-month-start-grace' && r.code === 1 && /取込が ❌/.test(r.err) && r0.code === 0, `flag=${flag} codes=${r.code},${r0.code}`);
      } finally { fs.rmSync(dir, { recursive: true, force: true }); }
    }
    check('daily-sync (R2 Medium 2): Yahoo: 取込・前月の build・前月の DQ が全部 ✅ → 旗なし / どれか ❌ → 猶予を禁じる / 月初でない (前月の工程なし) → 取込だけで決まる',
      f.monthStartGraceFlag(ok, ok, ok) === '' && f.monthStartGraceFlag(ok, ng) === ' --no-month-start-grace' && f.monthStartGraceFlag(ok, ok, ng) === ' --no-month-start-grace'
      && f.monthStartGraceFlag(ng, ok, ok) === ' --no-month-start-grace' && f.monthStartGraceFlag(ok, ...[]) === '' && f.monthStartGraceFlag() === ' --no-month-start-grace');
    check('daily-sync (R2 Low 1): 1 行の印: warn つきの成功 ⚠️ / 成功 ✅ / 失敗 ❌ / 見送り ⏸️',
      f.resultIcon({ success: true, warn: f.dqMonthStartWarn(grace0()) }) === '⚠️' && f.resultIcon({ success: true }) === '✅' && f.resultIcon({ success: true, warn: false }) === '✅'
      && f.resultIcon({ success: false }) === '❌' && f.resultIcon({ success: false, skipped: true }) === '⏸️' && f.resultIcon({ success: false, warn: true }) === '❌');
    const grace = grace0();
    const plainWarn = { success: true, summary: '⚠️  DQ gate passed with 1 warning(s)' };
    const results = [{ success: true, summary: 'ok' }, { ...grace, warn: f.dqMonthStartWarn(grace) }];
    const allOk = results.every((r) => r.success && r.warn !== true);
    check('daily-sync: 月初の猶予の DQ は warn = 見出しが ⚠️ (allOk にならない)', results[1].warn === true && allOk === false);
    check('daily-sync: 月初の立ち上がりで下げた DQ (最後の行が ⚠️ 月初の立ち上がり) も warn = 見出しが ⚠️',
      f.dqMonthStartWarn({ success: true, summary: monthStartRampNote(monthStartRamp('qoo10', '2026-10', { now: jst(2026, 10, 4, 9) }), [{ checkName: 'whitelist_coverage_pct', value: 34.4 }]) }) === true);
    check('daily-sync: 検査の warn つきの合格・失敗した DQ は warn にしない (今までどおり)', f.dqMonthStartWarn(plainWarn) === false && f.dqMonthStartWarn({ success: false, summary: note }) === false);
  }
}

console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
