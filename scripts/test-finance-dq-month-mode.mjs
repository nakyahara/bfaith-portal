/**
 * test-finance-dq-month-mode.mjs — apps/warehouse/finance-dq-month-mode.js の単体テスト
 * 使い方: node scripts/test-finance-dq-month-mode.mjs
 *   後半は 4 本の DQ スクリプト (Yahoo・au PAY・LINE ギフト・Qoo10) を子プロセスで流す (空の SQLite を一時の DATA_DIR に作る・本番の DB には触らない)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import {
  monthMode, pickThresholds, modeLabel, RECENT_PAST_GRACE_DAYS,
  monthStartEmptyGrace, monthStartEmptyNote, MONTH_START_EMPTY_GRACE_DAYS, parseNowArg,
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

// ── monthStartEmptyGrace (月初の猶予: 当月 0 行を ⚠️ 警告・exit 0 にしてよいか。2026-10-01) ──
const G = (ym, now, graceDays = 6) => monthStartEmptyGrace(ym, { now, graceDays }).grace;
const iso = (s) => new Date(s);
check('猶予: 当月 1 日 (10/1 07:00 JST) の 0 行 → 猶予', G('2026-10', jst(2026, 10, 1, 7)) === true);
check('猶予: 当月 6 日 (猶予の最後の日) → 猶予', G('2026-10', jst(2026, 10, 6, 7)) === true);
check('猶予: 当月 7 日 (猶予を過ぎた) → 猶予なし = CRITICAL', G('2026-10', jst(2026, 10, 7, 7)) === false);
check('猶予: 当月 31 日 → 猶予なし', G('2026-10', jst(2026, 10, 31, 7)) === false);
check('猶予: 前月 (10/1 に 2026-09 = recent_past) の 0 行 → 猶予なし = CRITICAL', G('2026-09', jst(2026, 10, 1, 7)) === false);
check('猶予: 前々月 (10/1 に 2026-08) → 猶予なし', G('2026-08', jst(2026, 10, 1, 7)) === false);
check('猶予: 1 年前の同じ月 (2025-10 を 2026-10-01 に) → 猶予なし', G('2025-10', jst(2026, 10, 1, 7)) === false);
check('猶予: 未来の月 (10/1 に 2026-11) → 猶予なし', G('2026-11', jst(2026, 10, 1, 7)) === false);
const v1 = monthStartEmptyGrace('2026-10', { now: jst(2026, 10, 1, 7), graceDays: 6 });
check('猶予: 返り値に mode・JST の日・日数', v1.mode === 'current' && v1.dayOfMonth === 1 && v1.graceDays === 6, JSON.stringify(v1));
// 月末の境界
check('月末: 10/31 23:59 JST の 2026-10 → 猶予なし (31 日)', G('2026-10', iso('2026-10-31T23:59:59+09:00')) === false);
check('月末: 11/1 00:00 JST の 2026-11 → 猶予 (1 日)', G('2026-11', iso('2026-11-01T00:00:00+09:00')) === true);
check('月末: 11/1 00:00 JST の 2026-10 (前月になった) → 猶予なし', G('2026-10', iso('2026-11-01T00:00:00+09:00')) === false);
check('月末: 2 月 (2027-02-28 → 03-01) も同じ: 3/1 に 2027-03 は猶予・2027-02 は猶予なし',
  G('2027-03', iso('2027-03-01T07:00:00+09:00')) === true && G('2027-02', iso('2027-03-01T07:00:00+09:00')) === false);
// 年の境界 (12/31 → 1/1)
check('年: 12/31 23:59:59 JST に 2027-01 (未来) → 猶予なし', G('2027-01', iso('2026-12-31T23:59:59+09:00')) === false);
check('年: 12/31 23:59:59 JST に 2026-12 (31 日) → 猶予なし', G('2026-12', iso('2026-12-31T23:59:59+09:00')) === false);
check('年: 1/1 00:00 JST に 2027-01 → 猶予', G('2027-01', iso('2027-01-01T00:00:00+09:00')) === true);
check('年: 1/1 00:00 JST に 2026-12 (前月) → 猶予なし', G('2026-12', iso('2027-01-01T00:00:00+09:00')) === false);
check('年: 1/6 (年末年始明けの最後の猶予の日) に 2027-01 → 猶予 / 1/7 → なし',
  G('2027-01', iso('2027-01-06T07:00:00+09:00')) === true && G('2027-01', iso('2027-01-07T07:00:00+09:00')) === false);
// UTC と JST の境界 (UTC 15:00 = JST 翌日 0:00)
check('UTC 境界: 2026-09-30T14:59:59Z (= 9/30 23:59 JST) に 2026-10 → 未来 = 猶予なし', G('2026-10', iso('2026-09-30T14:59:59Z')) === false);
check('UTC 境界: 2026-09-30T15:00:00Z (= 10/1 00:00 JST) に 2026-10 → 猶予 (UTC ではまだ 9/30)', G('2026-10', iso('2026-09-30T15:00:00Z')) === true);
check('UTC 境界: 2026-09-30T15:00:00Z に 2026-09 → 前月 = 猶予なし', G('2026-09', iso('2026-09-30T15:00:00Z')) === false);
check('UTC 境界: 2026-10-06T14:59:59Z (= 10/6 23:59 JST) → 猶予 (6 日)', G('2026-10', iso('2026-10-06T14:59:59Z')) === true);
check('UTC 境界: 2026-10-06T15:00:00Z (= 10/7 00:00 JST) → 猶予なし (UTC ではまだ 10/6)', G('2026-10', iso('2026-10-06T15:00:00Z')) === false);
check('UTC 境界: 年 2026-12-31T15:00:00Z (= 1/1 00:00 JST) に 2027-01 → 猶予', G('2027-01', iso('2026-12-31T15:00:00Z')) === true);
// モールごとの日数 (Qoo10 は配送完了待ちで長い)
check('日数: yahoo / aupay / linegift = 6・qoo10 = 9',
  MONTH_START_EMPTY_GRACE_DAYS.yahoo === 6 && MONTH_START_EMPTY_GRACE_DAYS.aupay === 6 && MONTH_START_EMPTY_GRACE_DAYS.linegift === 6 && MONTH_START_EMPTY_GRACE_DAYS.qoo10 === 9);
check('日数: 書き換えられない (freeze)', Object.isFrozen(MONTH_START_EMPTY_GRACE_DAYS));
check('日数: どれも 前月の猶予 (14) 以下', Object.values(MONTH_START_EMPTY_GRACE_DAYS).every((d) => d <= RECENT_PAST_GRACE_DAYS));
check('Qoo10: 9 日 → 猶予 / 10 日 → なし',
  G('2026-10', jst(2026, 10, 9, 7), MONTH_START_EMPTY_GRACE_DAYS.qoo10) === true && G('2026-10', jst(2026, 10, 10, 7), MONTH_START_EMPTY_GRACE_DAYS.qoo10) === false);
check('graceDays 0 → 1 日でも猶予なし', G('2026-10', jst(2026, 10, 1, 7), 0) === false);
// 日数が壊れていたら投げる (= 呼び手のスクリプトが落ちて exit 1。猶予は出さない向き)
const throws = (fn) => { try { fn(); return false; } catch { return true; } };
check('graceDays 不正 (未指定 / -1 / 1.5 / 15 / "6") は投げる',
  [undefined, -1, 1.5, 15, '6'].every((d) => throws(() => monthStartEmptyGrace('2026-10', { now: jst(2026, 10, 1), graceDays: d }))));
check('now が日時でなければ投げる', throws(() => monthStartEmptyGrace('2026-10', { now: new Date('x'), graceDays: 6 })));
// 最後の行 (daily-sync は子の最後の行を要約に出す)
const note = monthStartEmptyNote('f_yahoo_finance_sku_daily_v1', '2026-10', v1);
check('note: ⚠️ で始まり 月・日・猶予の日数・CRITICAL になる日を書く', note.startsWith('⚠️') && note.includes('2026-10') && note.includes('JST 1 日') && note.includes('6 日まで') && note.includes('7 日の朝'), note);
// --now
check('parseNowArg: +09:00 と Z を受ける', parseNowArg('2026-10-01T07:00:00+09:00').getTime() === jst(2026, 10, 1, 7).getTime() && parseNowArg('2026-09-30T22:00:00Z').getTime() === jst(2026, 10, 1, 7).getTime());
check('parseNowArg: 時差の無い日時・日付だけ・でたらめは投げる', ['2026-10-01T07:00:00', '2026-10-01', 'now', ''].every((s) => throws(() => parseNowArg(s))));

// ── 4 本の DQ スクリプトを子プロセスで (空の SQLite・一時の DATA_DIR) ──
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MALLS = [
  { mall: 'yahoo', script: 'run-yahoo-finance-dq.js', table: 'f_yahoo_finance_sku_daily_v1' },
  { mall: 'aupay', script: 'run-aupay-finance-dq.js', table: 'f_aupay_finance_sku_daily_v1' },
  { mall: 'linegift', script: 'run-linegift-finance-dq.js', table: 'f_linegift_finance_sku_daily_v1' },
  { mall: 'qoo10', script: 'run-qoo10-finance-dq.js', table: 'f_qoo10_finance_sku_daily_v1' },
];
function runDq(dir, script, month, now, runId) {
  const args = [path.join(repoRoot, 'apps', 'warehouse', script), '--data-dir', dir, '--month', month, '--run-id', runId];
  if (now) args.push('--now', now);
  // DATA_DIR は env が --data-dir より優先 = 必ず一時のディレクトリで上書きする (本番の DATA_DIR を継がない)
  const r = spawnSync(process.execPath, args, { encoding: 'utf8', env: { ...process.env, DATA_DIR: dir } });
  const out = String(r.stdout || '').trim();
  return { code: r.status, out, err: String(r.stderr || ''), last: out.split('\n').pop() || '' };
}
for (const { mall, script, table } of MALLS) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `dq-month-start-${mall}-`));
  try {
    const db = new Database(path.join(dir, 'warehouse.db'));
    db.exec(`CREATE TABLE ${table} (date_jst TEXT, sku_code TEXT);
      CREATE TABLE dq_run_results (run_id TEXT, check_name TEXT, severity TEXT, actual_value REAL, threshold_value REAL, details_json TEXT, checked_at TEXT, PRIMARY KEY (run_id, check_name));`);
    db.close();
    const sev = (runId) => {
      const d = new Database(path.join(dir, 'warehouse.db'), { readonly: true });
      try { return d.prepare(`SELECT severity, details_json FROM dq_run_results WHERE run_id = ? AND check_name = 'row_count_drift'`).get(runId); } finally { d.close(); }
    };
    const graceDays = MONTH_START_EMPTY_GRACE_DAYS[mall];
    const dd = (n) => String(n).padStart(2, '0');

    const r1 = runDq(dir, script, '2026-10', '2026-10-01T07:00:00+09:00', 't-day1');
    const s1 = sev('t-day1');
    check(`${mall}: 当月 1 日の 0 行 → exit 0・最後の行が ⚠️ 月初の猶予・row_count_drift = warn`,
      r1.code === 0 && r1.last.startsWith('⚠️ 月初の猶予') && s1?.severity === 'warn' && JSON.parse(s1.details_json).month_start_grace === true,
      `code=${r1.code} last=${r1.last} sev=${JSON.stringify(s1)} err=${r1.err.slice(-300)}`);

    const r2 = runDq(dir, script, '2026-10', `2026-10-${dd(graceDays)}T07:00:00+09:00`, 't-last');
    check(`${mall}: 猶予の最後の日 (${graceDays} 日) の 0 行 → exit 0`, r2.code === 0 && sev('t-last')?.severity === 'warn', `code=${r2.code} last=${r2.last}`);

    const r3 = runDq(dir, script, '2026-10', `2026-10-${dd(graceDays + 1)}T07:00:00+09:00`, 't-after');
    check(`${mall}: 猶予を過ぎた当月 (${graceDays + 1} 日) の 0 行 → exit 1・CRITICAL・row_count_drift = error`,
      r3.code === 1 && /CRITICAL/.test(r3.err) && sev('t-after')?.severity === 'error', `code=${r3.code} last=${r3.last}`);

    const r4 = runDq(dir, script, '2026-09', '2026-10-01T07:00:00+09:00', 't-prev');
    check(`${mall}: 前月 (10/1 に 2026-09) の 0 行 → exit 1・CRITICAL`, r4.code === 1 && /CRITICAL/.test(r4.err) && sev('t-prev')?.severity === 'error', `code=${r4.code} last=${r4.last}`);

    const r5 = runDq(dir, script, '2026-10', '2026-09-30T14:59:59Z', 't-utc-before');
    check(`${mall}: UTC 2026-09-30T14:59:59Z (= 9/30 JST) に 2026-10 → 未来の月 = exit 1`, r5.code === 1, `code=${r5.code}`);
    const r6 = runDq(dir, script, '2026-10', '2026-09-30T15:00:00Z', 't-utc-after');
    check(`${mall}: UTC 2026-09-30T15:00:00Z (= 10/1 00:00 JST) に 2026-10 → 猶予 = exit 0`, r6.code === 0, `code=${r6.code} last=${r6.last}`);

    const r7 = runDq(dir, script, '2027-01', '2027-01-01T07:00:00+09:00', 't-newyear');
    const r8 = runDq(dir, script, '2026-12', '2027-01-01T07:00:00+09:00', 't-newyear-prev');
    check(`${mall}: 年の境界: 1/1 に 2027-01 → exit 0 / 2026-12 → exit 1`, r7.code === 0 && r8.code === 1, `codes=${r7.code},${r8.code}`);

    const r9 = runDq(dir, script, '2026-10', '2026-10-01T07:00:00', 't-badnow');
    check(`${mall}: --now に時差が無い → exit 2 (FATAL)`, r9.code === 2, `code=${r9.code}`);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
