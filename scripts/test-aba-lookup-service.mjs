import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * ABA 参照サービス (apps/warehouse/aba-service.js): 取込済みの週から ASIN の検索語を引く (走査しない)
 * 実行: node scripts/test-aba-lookup-service.mjs
 *
 * 守りたいこと (Codex #1414 R1):
 *   ① 「行が無い」を「該当なし」と言えるのは、その週の取込が full で捨てた行が 0 のとき / watched ならその週を走査済みのときだけ。
 *      判定に使うのは取込時の mode (aba_weeks.mode) であって、いまの env ではない。mode 不明 (旧い週) は not_covered
 *   ② found も「行がある」の意味。coverage (complete / partial / unknown) を別に付ける
 *   ③ 週は固定できる (week_start)。指定した週が無ければ最新に代替しない
 *   ④ 監視登録は既定でしない。register:true のときだけ。失敗しても照会は返す
 *   ⑤ 指標は原値。集計や換算を足さない
 */
import path from 'node:path';
import http from 'node:http';
import fs from 'node:fs';
if (!process.env.DATA_DIR) { process.env.DATA_DIR = path.join(process.cwd(), '.tmp-aba-lookup-test'); }
fs.mkdirSync(process.env.DATA_DIR, { recursive: true });
try { fs.unlinkSync(path.join(process.env.DATA_DIR, 'aba.db')); } catch { /* 無ければ無視 */ }

const express = (await import('express')).default;
const abadb = await import('../apps/aba-keywords/db.js');
const svc = await import('../apps/warehouse/aba-service.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const item = (r, asin) => r.items.find((i) => i.asin === asin);

const db = abadb.initAbaDB();
ok(db.prepare("PRAGMA table_info(aba_weeks)").all().some((c) => c.name === 'mode') && db.prepare("PRAGMA table_info(aba_weeks)").all().some((c) => c.name === 'skipped_count'), 'aba_weeks に mode / skipped_count の列がある');

console.log('[1] 週が 1 つも無ければ no_week。監視登録は既定でしない');
{
  const r = svc.lookupAsins(db, ['B0AAAAAAA1']);
  eq([r.week, item(r, 'B0AAAAAAA1').status, item(r, 'B0AAAAAAA1').reason, r.registered], [null, 'no_week', 'no_ingested_week', false], '週なし → no_week');
  ok(!db.prepare('SELECT 1 FROM aba_watch_asins WHERE asin = ?').get('B0AAAAAAA1'), '既定では監視に登録しない');
}

// 取込済みの週: 9/13 = full・捨てた行 0 / 9/6 = full・捨てた行 3 / 8/30 = watched / 8/23 = mode 不明 (旧い版で取り込んだ週)
const insWeek = db.prepare(`INSERT INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count, parsed_count, mode, skipped_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
insWeek.run('2026-09-13', '2026-09-19', '2026-09-24T22:05:00Z', 439692, 1313530, 1313530, 'full', 0);
insWeek.run('2026-09-06', '2026-09-12', '2026-09-24T22:00:00Z', 400000, 1200000, 1200000, 'full', 3);
insWeek.run('2026-08-30', '2026-09-05', '2026-09-24T21:55:00Z', 12, 30, 1250000, 'watched', 0);
insWeek.run('2026-08-23', '2026-08-29', '2026-08-01T00:00:00Z', 10, 20, 1000000, null, null);
// prune した full の週 (非監視の語を消した) と、捨てた行がある watched の週
db.prepare(`INSERT INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count, parsed_count, mode, skipped_count, pruned_at) VALUES ('2026-07-05', '2026-07-11', '2026-07-12T00:00:00Z', 400000, 1200000, 1200000, 'full', 0, '2026-09-24T22:10:00Z')`).run();
insWeek.run('2026-07-12', '2026-07-18', '2026-07-19T00:00:00Z', 5, 12, 1100000, 'watched', 4);
db.prepare(`INSERT INTO aba_search_terms (week_start, department, search_term, search_frequency_rank, click_position, asin, click_share, conversion_share) VALUES ('2026-07-05', 'amazon.co.jp', '残った語', 700, 1, 'B0GGGGGGG7', 0.4, 0.4)`).run();
db.prepare(`INSERT INTO aba_search_terms (week_start, department, search_term, search_frequency_rank, click_position, asin, click_share, conversion_share) VALUES ('2026-07-12', 'amazon.co.jp', 'ひば油 スプレー', 800, 1, 'B0HHHHHHH8', 0.2, 0.2)`).run();
db.prepare(`INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count, last_scanned_week) VALUES ('B0HHHHHHH8', datetime('now'), datetime('now'), 1, '2026-07-12')`).run();
db.prepare(`INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count, last_scanned_week) VALUES ('B0IIIIIII9', datetime('now'), datetime('now'), 1, '2026-07-12')`).run();
const ins = db.prepare(`INSERT INTO aba_search_terms (week_start, department, search_term, search_frequency_rank, click_position, asin, click_share, conversion_share) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
ins.run('2026-09-13', 'amazon.co.jp', 'ハッカ油 スプレー', 1200, 1, 'B0AAAAAAA1', 0.31, 0.28);
ins.run('2026-09-13', 'amazon.co.jp', 'ハッカ油 スプレー', 1200, 2, 'B0BBBBBBB2', 0.12, 0.10);
ins.run('2026-09-13', 'amazon.co.jp', 'ハッカ油', 300, 3, 'B0AAAAAAA1', 0.05, 0.04);
ins.run('2026-09-06', 'amazon.co.jp', '虫除け スプレー', 900, 1, 'B0CCCCCCC3', 0.2, 0.2);
ins.run('2026-08-30', 'amazon.co.jp', 'ひば油', 500, 1, 'B0DDDDDDD4', 0.3, 0.3);
ins.run('2026-08-30', 'amazon.co.jp', 'ひば油', 500, 2, 'B0EEEEEEE5', 0.1, 0.1);   // 監視 ASIN D と同じ語に出た未監視 E (部分的な結果)
db.prepare(`INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count, last_scanned_week) VALUES ('B0DDDDDDD4', datetime('now'), datetime('now'), 1, '2026-08-30')`).run();
db.prepare(`INSERT INTO aba_watch_asins (asin, first_queried_at, last_queried_at, query_count, last_scanned_week) VALUES ('B0FFFFFFF6', datetime('now'), datetime('now'), 1, '2026-09-13')`).run();   // 新しい週の走査実績しかない

console.log('[2] full・捨てた行 0 の週 (最新): found は原値・順位昇順、無ければ none (証明 = 週の取込完了)');
{
  const r = svc.lookupAsins(db, ['B0AAAAAAA1', 'B0BBBBBBB2', 'B0CCCCCCC3', 'B0ZZZZZZZ9']);
  eq([r.week.week_start, r.week.mode, r.week.skipped_count, r.week_coverage, r.requested_week], ['2026-09-13', 'full', 0, 'complete', null], '対象週 = 取込済みの最新週 (mode と捨てた行数つき)');
  const a = item(r, 'B0AAAAAAA1');
  eq([a.status, a.proof, a.coverage, a.terms.map((t) => t.search_term)], ['found', 'week_ingested', 'complete', ['ハッカ油', 'ハッカ油 スプレー']], 'found・coverage complete・順位の昇順');
  eq(a.terms[1], { search_term: 'ハッカ油 スプレー', department: 'amazon.co.jp', search_frequency_rank: 1200, click_position: 1, click_share: 0.31, conversion_share: 0.28 }, '指標は原値 (集計・換算なし)');
  ok(!('top3_click_share' in a.terms[0]) && !('score' in a.terms[0]), '合計や点数を足さない');
  eq(item(r, 'B0BBBBBBB2').terms.length, 1, '同じ語の別の上位 ASIN は自分の行だけ');
  eq([item(r, 'B0CCCCCCC3').status, item(r, 'B0CCCCCCC3').proof], ['none', 'week_ingested'], '前の週にしか無い ASIN は「最新週には無い」(証明つき)');
  eq([item(r, 'B0ZZZZZZZ9').status, item(r, 'B0ZZZZZZZ9').coverage], ['none', 'complete'], '未知の ASIN も full・捨てた行 0 なら none');
}

console.log('[3] 判定は取込時の mode (env ではない)。捨てた行がある週・watched の週・mode 不明の週は「無い」と言わない');
{
  const saved = process.env.ABA_INGEST_MODE;
  process.env.ABA_INGEST_MODE = 'full';   // env が full でも、週の mode で判定する
  let r = svc.lookupAsins(db, ['B0CCCCCCC3', 'B0ZZZZZZZ9'], { weekStart: '2026-09-06' });
  eq([r.week.week_start, r.week_coverage], ['2026-09-06', 'partial'], '捨てた行 3 の full 週は partial');
  eq([item(r, 'B0CCCCCCC3').status, item(r, 'B0CCCCCCC3').coverage], ['found', 'partial'], 'found でも coverage partial (全部そろった保証が無い)');
  eq([item(r, 'B0ZZZZZZZ9').status, item(r, 'B0ZZZZZZZ9').reason], ['not_covered', 'incomplete_ingest'], '行が無くても none とは言わない (incomplete_ingest)');
  r = svc.lookupAsins(db, ['B0DDDDDDD4', 'B0EEEEEEE5', 'B0FFFFFFF6', 'B0ZZZZZZZ9'], { weekStart: '2026-08-30' });
  eq([item(r, 'B0DDDDDDD4').status, item(r, 'B0DDDDDDD4').coverage, item(r, 'B0DDDDDDD4').proof], ['found', 'complete', 'scanned'], 'watched: その週を走査済みの監視 ASIN は found・complete');
  eq([item(r, 'B0EEEEEEE5').status, item(r, 'B0EEEEEEE5').coverage], ['found', 'partial'], 'watched: 監視 ASIN と同じ語に出ただけの ASIN は found でも partial');
  eq([item(r, 'B0FFFFFFF6').status, item(r, 'B0FFFFFFF6').reason], ['not_covered', 'not_watched'], '新しい週の走査実績 (9/13) では 8/30 週の「無い」を証明しない (= ではなく ≥ にしない)');
  eq([item(r, 'B0ZZZZZZZ9').status, item(r, 'B0ZZZZZZZ9').reason], ['not_covered', 'not_watched'], 'watched で未走査の ASIN は not_covered');
  r = svc.lookupAsins(db, ['B0ZZZZZZZ9'], { weekStart: '2026-08-23' });
  eq([r.week_coverage, item(r, 'B0ZZZZZZZ9').status, item(r, 'B0ZZZZZZZ9').reason], ['unknown', 'not_covered', 'mode_unknown'], 'mode 不明 (旧い週) は not_covered (full と推定しない)');
  // prune した週 (Codex R2 #1): 残った語があっても complete と言わない・無くても none と言わない
  r = svc.lookupAsins(db, ['B0GGGGGGG7', 'B0ZZZZZZZ9'], { weekStart: '2026-07-05' });
  eq(r.week_coverage, 'partial', 'prune した full の週は partial');
  eq([item(r, 'B0GGGGGGG7').status, item(r, 'B0GGGGGGG7').coverage, item(r, 'B0GGGGGGG7').reason], ['found', 'partial', 'pruned'], 'prune 後に残った語は found でも partial (reason pruned)');
  eq([item(r, 'B0ZZZZZZZ9').status, item(r, 'B0ZZZZZZZ9').reason], ['not_covered', 'pruned'], 'prune 後に無い ASIN は none ではなく not_covered (pruned)');
  // watched で捨てた行がある週 (Codex R2 #2): 走査済みでも complete と言わない
  r = svc.lookupAsins(db, ['B0HHHHHHH8', 'B0IIIIIII9'], { weekStart: '2026-07-12' });
  eq([item(r, 'B0HHHHHHH8').status, item(r, 'B0HHHHHHH8').coverage], ['found', 'partial'], 'watched・走査済みでも捨てた行があれば found は partial');
  eq([item(r, 'B0IIIIIII9').status, item(r, 'B0IIIIIII9').reason], ['not_covered', 'incomplete_ingest'], 'watched・走査済みでも捨てた行があれば「無い」とは言わない (incomplete_ingest)');
  if (saved === undefined) delete process.env.ABA_INGEST_MODE; else process.env.ABA_INGEST_MODE = saved;
}

console.log('[4] 週の固定: 指定した週が無ければ最新に代替しない');
{
  const r = svc.lookupAsins(db, ['B0AAAAAAA1'], { weekStart: '2026-06-28' });
  eq([r.week, r.requested_week, item(r, 'B0AAAAAAA1').status, item(r, 'B0AAAAAAA1').reason], [null, '2026-06-28', 'no_week', 'week_not_found'], '無い週 → no_week (week_not_found)');
}

console.log('[5] 監視登録は register:true のときだけ。失敗しても照会は返す');
{
  const r = svc.lookupAsins(db, ['B0AAAAAAA1', 'B0ZZZZZZZ9'], { register: true });
  ok(r.registered === true && db.prepare('SELECT query_count FROM aba_watch_asins WHERE asin = ?').get('B0ZZZZZZZ9')?.query_count === 1, 'register:true で登録される');
  const r2 = svc.lookupAsins(db, ['B0ZZZZZZZ9'], { register: true });
  ok(db.prepare('SELECT query_count FROM aba_watch_asins WHERE asin = ?').get('B0ZZZZZZZ9')?.query_count === 2 && r2.items[0].status === 'none', '2 回目は query_count が増える');
  // 登録が失敗しても (別接続が書込ロックを握っている) 照会は返る
  const Database = (await import('better-sqlite3')).default;
  const other = new Database(path.join(process.env.DATA_DIR, 'aba.db'));
  other.pragma('busy_timeout = 0');
  other.exec('BEGIN IMMEDIATE');
  db.pragma('busy_timeout = 50');
  const r3 = svc.lookupAsins(db, ['B0AAAAAAA1'], { register: true });
  db.pragma('busy_timeout = 10000');
  other.exec('ROLLBACK'); other.close();
  ok(r3.items[0].status === 'found' && r3.registered === false && r3.register_errors.length === 1 && /BUSY/i.test(r3.register_errors[0].error), `書込ロック中でも照会は返り、登録の失敗は register_errors に (${JSON.stringify(r3.register_errors)})`);
}

console.log('[6] service-api の口');
{
  const app = express();
  app.use(express.json());
  app.use('/service-api/aba', svc.default);
  const server = http.createServer(app);
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = async (body) => {
    const res = await fetch(base + '/service-api/aba/lookup', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    return { status: res.status, body: await res.json().catch(() => ({})) };
  };
  eq((await call({})).status, 400, 'asins 無しは 400');
  eq((await call({ asins: 'bad, also-bad' })).status, 400, '形式違いだけなら 400');
  eq((await call({ asins: ['B0AAAAAAA1', 'B0AAAAAAA2', 'B0AAAAAAA3', 'B0AAAAAAA4', 'B0AAAAAAA5', 'B0AAAAAAA6'] })).status, 400, '6 件は 400');
  eq((await call({ asins: 'B0AAAAAAA1', week_start: '9/13' })).status, 400, 'week_start の形式違いは 400');
  const r = await call({ asins: 'b0aaaaaaa1, https://www.amazon.co.jp/dp/B0ZZZZZZZ9/ref=x nope' });
  eq(r.status, 200, '正常');
  ok(r.body.ok === true && r.body.result && r.body.result.week.week_start === '2026-09-13' && r.body.result.week_coverage === 'complete', 'okResponse の形 {ok, result:{week, week_coverage, items, …}}');
  eq(r.body.result.items.map((i) => [i.asin, i.status]), [['B0AAAAAAA1', 'found'], ['B0ZZZZZZZ9', 'none']], '大文字化・URL から抽出。形式違いは invalid に');
  eq([r.body.result.invalid, r.body.result.registered], [['nope'], false], 'invalid を返す・既定では登録しない');
  const r2 = await call({ asins: 'B0AAAAAAA1', week_start: '2026-09-06' });
  eq([r2.body.result.week.week_start, r2.body.result.items[0].status, r2.body.result.items[0].reason], ['2026-09-06', 'not_covered', 'incomplete_ingest'], '週を固定して照会できる');
  server.close();
}

abadb.closeAbaDB();
console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
