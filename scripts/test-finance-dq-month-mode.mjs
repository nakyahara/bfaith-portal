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
  ensureMonthHighWater, markMonthHighWater, migrateLegacyHighWater, prepareMonthHighWater, isRealYmd,
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
/** DQ の入口と同じく印の表を作った DB (ensure = false なら作らない) */
function memDb({ ensure = true } = {}) { const d = new Database(':memory:'); d.exec(DQ_DDL); d.exec('CREATE TABLE f_yahoo_finance_sku_daily_v1 (date_jst TEXT, k TEXT)'); if (ensure) ensureMonthHighWater(d); return d; }
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
  const d = memDb();
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
    withDb(dir, (d) => ensureMonthHighWater(d));
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
    check(`daily-sync: ${script} に取込の結果で猶予の禁止 (monthStartGraceFlag(${imp})) を渡し、結果に warn を付ける`,
      src.includes(`${script} --data-dir \${DATA_DIR_ARG} --month \${`) && new RegExp(`${script.replace(/\./g, '\\.')} --data-dir \\$\\{DATA_DIR_ARG\\} --month \\$\\{\\w+\\}\\$\\{monthStartGraceFlag\\(${imp}(, \\.\\.\\.\\w+)?\\)\\}`).test(src)
      && src.includes(`...${v}, warn: dqMonthStartWarn(${v}) }`));
  }
  check('daily-sync: Yahoo は月初の猶予の間、前月も build → DQ → (DQ が通れば) sync', /Yahoo finance build \$\{yahooPrevYm\} \(月初の前月\)/.test(src) && /Yahoo finance DQ \$\{yahooPrevYm\} \(月初の前月\)/.test(src)
    && /if \(yahooPrevDq\.success\) \{\n\s+const yahooPrevSync = runScript\(\n\s+`apps\/warehouse\/sync-yahoo-finance-daily\.js --data-dir \$\{DATA_DIR_ARG\} --month \$\{yahooPrevYm\}`/.test(src));
  check('daily-sync (R2 Medium 2): 前月の build と DQ の結果を当月の DQ の旗に渡す (どちらか ❌ なら猶予を禁じる)',
    src.includes('yahooPrevSteps.push(yahooPrevBuild);') && src.includes('yahooPrevSteps.push(yahooPrevDq);') && src.includes('monthStartGraceFlag(yahooResult, ...yahooPrevSteps)')
    && src.indexOf('yahooPrevSteps.push(yahooPrevDq);') < src.indexOf('monthStartGraceFlag(yahooResult, ...yahooPrevSteps)'));
  check('daily-sync (R2 Low 1): 1 行ごとの印は resultIcon (warn を見る)', src.includes('const icon = resultIcon(r);') && !src.includes("const icon = r.skipped ? '⏸️' : (r.success ? '✅' : '❌');"));
  // 補助の関数を取り出して動かす (見出し allOk の式は daily-sync の本物と同じかも確かめる)
  const fnSrc = (name) => { const m = src.match(new RegExp(`function ${name}\\([^)]*\\) \\{[^\\n]*\\}`)); return m ? m[0] : null; };
  const flagSrc = fnSrc('monthStartGraceFlag'); const warnSrc = fnSrc('dqMonthStartWarn'); const iconSrc = fnSrc('resultIcon');
  const allOkSrc = 'const allOk = results.every(r => r.success && r.warn !== true) && urgentWarnings.length === 0 && diskWarnings.length === 0;';
  check('daily-sync: 補助の関数 3 つと見出しの式がある', !!flagSrc && !!warnSrc && !!iconSrc && src.includes(allOkSrc));
  const grace0 = () => ({ success: true, summary: note });
  if (flagSrc && warnSrc && iconSrc) {
    // eslint-disable-next-line no-new-func
    const f = new Function('isMonthStartGraceSummary', `${flagSrc}\n${warnSrc}\n${iconSrc}\nreturn { monthStartGraceFlag, dqMonthStartWarn, resultIcon };`)(isMonthStartGraceSummary);
    check('daily-sync: 取込が ✅ → 旗なし / ❌ → --no-month-start-grace', f.monthStartGraceFlag({ success: true }) === '' && f.monthStartGraceFlag({ success: false }) === ' --no-month-start-grace' && f.monthStartGraceFlag(undefined) === ' --no-month-start-grace');
    const ok = { success: true }; const ng = { success: false };
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
    check('daily-sync: 検査の warn つきの合格・失敗した DQ は warn にしない (今までどおり)', f.dqMonthStartWarn(plainWarn) === false && f.dqMonthStartWarn({ success: false, summary: note }) === false);
  }
}

console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
