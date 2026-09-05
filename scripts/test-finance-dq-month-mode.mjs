/**
 * test-finance-dq-month-mode.mjs — apps/warehouse/finance-dq-month-mode.js の単体テスト
 * 使い方: node scripts/test-finance-dq-month-mode.mjs
 */
import { monthMode, pickThresholds, modeLabel, RECENT_PAST_GRACE_DAYS } from '../apps/warehouse/finance-dq-month-mode.js';

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

console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURES`);
process.exit(failures === 0 ? 0 : 1);
