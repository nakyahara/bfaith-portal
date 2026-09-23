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
}

console.log('[3] 失敗の分類');
{
  eq(m.classifyReportFailure('dataStartTime must be a Sunday when reportPeriod=WEEK', 3), 'config', '引数の誤り → config (日数に関わらず即ハード失敗)');
  eq(m.classifyReportFailure('Access to requested resource is denied', 1), 'config', '権限 → config');
  eq(m.classifyReportFailure('Report is not available yet', 3), 'unpublished', '未公開 (3 日) → unpublished');
  eq(m.classifyReportFailure(null, 3), 'unpublished', '理由文なし (3 日) → unpublished');
  eq(m.classifyReportFailure(null, 11), 'stale', '理由文なし (11 日) → stale (恒久障害を疑う)');
  eq(m.classifyReportFailure('something else', 12), 'stale', '未知の理由文 (12 日) → stale');
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
