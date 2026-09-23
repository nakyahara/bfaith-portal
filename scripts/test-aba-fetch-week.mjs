import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * ABA 週次レポート取込 (apps/aba-keywords/fetch-aba-search-terms.js) の「週の同定」と「失敗の分類」
 * 実行: node scripts/test-aba-fetch-week.mjs
 *
 * 守りたいこと (2026-09-23 の事故 = 8 週間 1 週も取れず毎朝 ✅):
 *   ① createReport の期間は UTC の日曜 00:00:00Z 〜 土曜 23:59:59Z (JST で送ると Amazon は土曜と解釈して FATAL)
 *   ② 対象週は直近の完了週を複数見る (1 週だけだと未公開の間に対象が回転して二度と試されない)
 *   ③ FATAL の理由文を読んで分類する: 引数/設定の誤りは即ハード失敗・10 日過ぎた未生成もハード失敗・それ以外だけ正常 skip
 * SP-API は呼ばない (純粋な関数だけ)。import しても main は走らない
 */
const m = await import('../apps/aba-keywords/fetch-aba-search-terms.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const dow = (ymd) => new Date(`${ymd}T00:00:00Z`).getUTCDay();

console.log('[1] createReport の期間は UTC の日曜〜土曜');
{
  const b = m.reportPeriodBody('2026-08-30', '2026-09-05');
  eq(b, { dataStartTime: '2026-08-30T00:00:00Z', dataEndTime: '2026-09-05T23:59:59Z' }, 'T00:00:00Z / T23:59:59Z (JST の +09:00 ではない)');
  ok(!/\+09:00/.test(JSON.stringify(b)), '+09:00 を含まない');
  ok(new Date(b.dataStartTime).getUTCDay() === 0 && new Date(b.dataEndTime).getUTCDay() === 6, 'UTC で 日曜 〜 土曜');
}

console.log('[2] 対象週 = 直近の完了週を複数 (日曜始まり・土曜終わり・連続)');
{
  const w = m.recentCompletedWeeks(3);
  eq(w.length, 3, '3 週');
  ok(w.every((x) => dow(x.weekStart) === 0 && dow(x.weekEnd) === 6), '全部 日曜〜土曜');
  ok(w.every((x) => (Date.parse(x.weekEnd) - Date.parse(x.weekStart)) === 6 * 86400000), '各週は 7 日');
  ok(Date.parse(w[0].weekStart) - Date.parse(w[1].weekStart) === 7 * 86400000 && Date.parse(w[1].weekStart) - Date.parse(w[2].weekStart) === 7 * 86400000, '1 週ずつ遡る');
  ok(Date.parse(w[0].weekEnd) < Date.now(), '直近の週は終わっている (当週は含まない)');
  // 🚨 完了の判定は UTC (Codex #1411): 日曜 07:00 JST = 土曜 22:00Z はまだその週が終わっていない
  const sunday0700jst = Date.parse('2026-09-26T22:00:00Z');          // = 2026-09-27 07:00 JST (日曜)
  const w1 = m.recentCompletedWeeks(1, sunday0700jst)[0];
  eq(w1.weekEnd, '2026-09-19', '日曜 07:00 JST (土曜 22:00Z) では 9/19 (土) までの週が直近の完了週 — 9/26 はまだ終わっていない');
  const sunday0930jst = Date.parse('2026-09-27T00:30:00Z');          // = 2026-09-27 09:30 JST (日曜)
  const w2 = m.recentCompletedWeeks(1, sunday0930jst)[0];
  eq(w2.weekEnd, '2026-09-26', '日曜 09:30 JST (日曜 00:30Z) では 9/26 (土) までの週が完了');
  ok(m.recentCompletedWeeks(2, sunday0930jst).every((x) => Date.parse(x.weekEnd) + 86400000 <= sunday0930jst), '返す週の 23:59:59Z は全部「今」より前');
}

console.log('[2b] 時間予算: 残りが 1 週分に足りなければ持ち越す');
{
  const MIN = 60 * 1000;
  eq(m.shouldDefer(0 * MIN, 50 * MIN, 15 * MIN), false, '開始直後は着手する');
  eq(m.shouldDefer(30 * MIN, 50 * MIN, 15 * MIN), false, '残り 20 分 (≥ 15 分) なら着手する');
  eq(m.shouldDefer(36 * MIN, 50 * MIN, 15 * MIN), true, '残り 14 分 (< 15 分) なら持ち越す');
  ok(m.shouldDefer(0) === false, '既定の予算 (50 分) と 1 週分 (12 分) でも開始直後は着手する');
  eq(m.budgetIsUsable(10 * MIN, 12 * MIN), false, '1 週分より短い予算は受け付けない (毎回何もせず持ち越すのを防ぐ)');
  eq(m.budgetIsUsable(12 * MIN, 12 * MIN), true, '1 週分ちょうどなら受け付ける');
  ok(m.budgetIsUsable(50 * MIN) === true, '既定の予算は使える');
}

console.log('[3] 失敗の分類');
{
  eq(m.classifyReportFailure('dataStartTime must be a Sunday when reportPeriod=WEEK', 3), 'config', '引数の誤り → config (日数に関わらず即ハード失敗)');
  eq(m.classifyReportFailure('Access to requested resource is denied', 1), 'config', '権限 → config');
  eq(m.classifyReportFailure('Report is not available yet', 3), 'unpublished', '未公開 (3 日) → unpublished');
  eq(m.classifyReportFailure(null, 3), 'unpublished', '理由文なし (3 日) → unpublished');
  eq(m.classifyReportFailure(null, 11), 'stale', '理由文なし (11 日) → stale (恒久障害を疑う)');
  eq(m.classifyReportFailure('something else', 12), 'stale', '未知の理由文 (12 日) → stale');
  eq(m.classifyReportFailure('Report period must be complete', 2), 'unpublished', '「must be」単独では config にしない (未観測の文言は 10 日ルールに任せる)');
  eq(m.classifyReportFailure('Operation not permitted', 1), 'config', 'not permitted → config');
}

console.log('[4] import しても main は走らない');
{
  const { pathToFileURL } = await import('node:url');
  const path = await import('node:path');
  const selfUrl = pathToFileURL(path.resolve('apps/aba-keywords/fetch-aba-search-terms.js')).href;
  ok(typeof m.isDirectRun === 'function' && m.isDirectRun(process.argv[1], selfUrl) === false, 'このテストから import したときは直接起動ではない (main は走らない)');
  ok(m.isDirectRun(path.resolve('apps/aba-keywords/fetch-aba-search-terms.js'), selfUrl) === true, '自分自身を argv[1] にすれば直接起動');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
