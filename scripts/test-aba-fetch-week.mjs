import { temporaryTestDataDir } from './test-temp-dir.mjs';
// DATA_DIR は db.js の import 時に固定される → 子プロセスの起動前に専用の一時ディレクトリを渡す ([5] が aba.db を作るため)
const DATA_DIR = await temporaryTestDataDir(import.meta.url, 'aba-fetch-week-');
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
  // 境界値 (Codex R3): --budget-min 12 で台帳照会に数 ms かかっても、最初の未処理週は着手する
  eq(m.canStartWeek({ mustStart: true, remainingMs: 12 * MIN - 5, reserveMs: 12 * MIN }), true, '最初の未処理週は残りが予約時間を割っていても着手する');
  eq(m.canStartWeek({ mustStart: false, remainingMs: 12 * MIN - 5, reserveMs: 12 * MIN }), false, '2 週目以降は残りが予約時間未満なら持ち越す');
  eq(m.canStartWeek({ mustStart: false, remainingMs: 12 * MIN, reserveMs: 12 * MIN }), true, '2 週目以降でも残りが予約時間以上なら着手する');
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

console.log('[5] 保持期限の削除 (pruneOldWeeks) は、消した週の台帳に pruned_at を同じトランザクションで残す');
{
  const path = await import('node:path');
  const fs = await import('node:fs');
  ok(process.env.DATA_DIR === DATA_DIR && !fs.existsSync(path.join(DATA_DIR, 'aba.db')), 'aba.db は専用の一時ディレクトリに作る (worktree の data/ に触らない)');
  const abadb = await import('../apps/aba-keywords/db.js');
  const db = abadb.initAbaDB();
  const insWeek = db.prepare(`INSERT INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count, parsed_count, mode, skipped_count) VALUES (?, ?, datetime('now'), 1, 1, 1, 'full', 0)`);
  const insTerm = db.prepare(`INSERT INTO aba_search_terms (week_start, department, search_term, search_frequency_rank, click_position, asin) VALUES (?, 'amazon.co.jp', ?, 1, 1, ?)`);
  insWeek.run('2026-09-13', '2026-09-19'); insTerm.run('2026-09-13', '新しい語', 'B0NEWNEW01');
  insWeek.run('2026-07-05', '2026-07-11'); insTerm.run('2026-07-05', '古い語', 'B0OLDOLD01'); insTerm.run('2026-07-05', '監視の語', 'B0WATCHW01');
  insWeek.run('2026-06-28', '2026-07-04'); insTerm.run('2026-06-28', 'もっと古い語', 'B0OLDOLD02');
  db.prepare(`INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count) VALUES ('B0WATCHW01', datetime('now'), datetime('now'), 1)`).run();
  m.pruneOldWeeks(db, '2026-09-13', 8);   // cutoff = 2026-07-19
  const weeks = db.prepare('SELECT week_start, pruned_at FROM aba_weeks ORDER BY week_start').all();
  eq(weeks.map((w) => [w.week_start, w.pruned_at != null]), [['2026-06-28', true], ['2026-07-05', true], ['2026-09-13', false]], '期限より古い週だけ pruned_at が付く (残した週には付かない)');
  eq(db.prepare('SELECT asin FROM aba_search_terms ORDER BY asin').all().map((r) => r.asin), ['B0NEWNEW01', 'B0WATCHW01'], '非監視の語は消え、監視 ASIN の語と新しい週は残る');
  const before = weeks.find((w) => w.week_start === '2026-07-05').pruned_at;
  m.pruneOldWeeks(db, '2026-09-13', 8);
  eq(db.prepare(`SELECT pruned_at FROM aba_weeks WHERE week_start = '2026-07-05'`).get().pruned_at, before, '2 回目は pruned_at を上書きしない (最初に消した時刻のまま)');
  // 監視 ASIN の語が残っていても、prune した週は「全部そろっている」とも「無い」とも言えない (service-api 側)
  const svc = await import('../apps/warehouse/aba-service.js');
  const r = svc.lookupAsins(db, ['B0WATCHW01', 'B0OLDOLD01'], { weekStart: '2026-07-05' });
  eq(r.items.map((i) => [i.asin, i.status, i.coverage, i.reason]), [['B0WATCHW01', 'found', 'partial', 'pruned'], ['B0OLDOLD01', 'not_covered', 'unknown', 'pruned']], 'prune した週は found でも partial・無くても not_covered (pruned)');
  abadb.closeAbaDB();
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
