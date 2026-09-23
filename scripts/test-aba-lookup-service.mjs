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
  // prune 済みで mode / skipped_count も不明な週 (Codex R3 任意): pruned を優先して partial / reason=pruned
  db.prepare(`INSERT INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count, parsed_count, mode, skipped_count, pruned_at) VALUES ('2026-06-21', '2026-06-27', '2026-06-28T00:00:00Z', 1, 1, 1, NULL, NULL, '2026-09-24T22:10:00Z')`).run();
  db.prepare(`INSERT INTO aba_search_terms (week_start, department, search_term, search_frequency_rank, click_position, asin, click_share, conversion_share) VALUES ('2026-06-21', 'amazon.co.jp', '残った語2', 900, 1, 'B0GGGGGGG7', 0.1, 0.1)`).run();
  r = svc.lookupAsins(db, ['B0GGGGGGG7', 'B0ZZZZZZZ9'], { weekStart: '2026-06-21' });
  eq([r.week_coverage, item(r, 'B0GGGGGGG7').coverage, item(r, 'B0GGGGGGG7').reason, item(r, 'B0ZZZZZZZ9').reason], ['partial', 'partial', 'pruned', 'pruned'], 'prune 済み + mode 不明の週は pruned が優先 (unknown / mode_unknown にならない)');
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

// ─── 検索語 → クリック上位 3 の ASIN (競合 ASIN の自動取得・2026-09-23) ───
ins.run('2026-09-13', 'amazon.co.jp', 'iphone15 ケース', 50, 1, 'B0JJJJJJJ1', 0.2, 0.1);
ins.run('2026-09-13', 'amazon.co.jp', 'iphone15 ケース', 50, 2, 'B0JJJJJJJ2', 0.1, 0.05);
const termItem = (r, term) => r.items.find((i) => i.term === term);

console.log('[7] lookupTerms: 語 → その週のクリック上位 3 (部門ごと)');
{
  const dep = (i) => i.departments.map((g) => [g.department, g.search_frequency_rank, g.asins.map((x) => x.asin + ':' + x.click_position)]);
  const r = svc.lookupTerms(db, ['ハッカ油 スプレー', 'ハッカ油', '載っていない語']);
  eq([r.week.week_start, r.week_coverage, r.requested_week], ['2026-09-13', 'complete', null], '対象週 = 取込済みの最新週');
  const a = termItem(r, 'ハッカ油 スプレー');
  eq([a.status, a.matched_term, a.coverage, dep(a)], ['found', 'ハッカ油 スプレー', 'complete', [['amazon.co.jp', 1200, ['B0AAAAAAA1:1', 'B0BBBBBBB2:2']]]], 'found・部門ごと・クリック順位の昇順');
  eq(a.departments[0].asins[0], { asin: 'B0AAAAAAA1', click_position: 1, product_title: null, click_share: 0.31, conversion_share: 0.28 }, '指標は原値');
  ok(!('search_frequency_rank' in a) && !('department' in a), '部門をまたいだ順位・部門を上の階層に出さない');
  eq(dep(termItem(r, 'ハッカ油')), [['amazon.co.jp', 300, ['B0AAAAAAA1:3']]], '上位 3 のうち保存された行だけ');
  const none = termItem(r, '載っていない語');
  eq([none.status, none.coverage, none.variants], ['none', 'complete', ['載っていない語']], 'complete の週で、試した表記のどれもレポートに無い語は none (variants = 試した表記)');

  const v = svc.lookupTerms(db, ['iPhone15 ケース', 'ｉＰｈｏｎｅ15 ケース']);
  eq(v.items.map((i) => [i.status, i.matched_term]), [['found', 'iphone15 ケース'], ['found', 'iphone15 ケース']], '大文字・全角英数は 小文字 / NFKC で当てる (matched_term に当たった語)');
  eq(v.items[1].variants, ['ｉＰｈｏｎｅ15 ケース', 'ｉｐｈｏｎｅ15 ケース', 'iphone15 ケース'], '試す表記 = 送られたまま → 小文字 → NFKC+小文字 (同じものは 1 回)');

  // 保存側は語を加工しない (空白 2 つ・前後の空白も原文のまま) → 送られたままの語で当たる (Codex #1420 R1 #1)
  ins.run('2026-09-13', 'amazon.co.jp', 'oil  spray', 70, 1, 'B0LLLLLLL1', 0.3, 0.2);
  const raw = svc.lookupTerms(db, ['oil  spray', 'oil spray']);
  eq(raw.items.map((i) => [i.term, i.status, i.matched_term]), [['oil  spray', 'found', 'oil  spray'], ['oil spray', 'none', null]],
    '空白 2 つの語は送られたままで当たる。整えた形 (空白 1 つ) は別の語 = その表記は無い');

  const w = svc.lookupTerms(db, ['虫除け スプレー', '無い語'], { weekStart: '2026-09-06' });
  eq(w.items.map((i) => [i.status, i.coverage, i.reason]), [['found', 'partial', null], ['not_covered', 'unknown', 'incomplete_ingest']], '捨てた行がある週: found は partial・無い語は none と言わない');
  const x = svc.lookupTerms(db, ['ひば油', '無い語'], { weekStart: '2026-08-30' });
  eq(x.items.map((i) => [i.status, i.coverage, i.reason, i.departments.length ? i.departments[0].asins.length : 0]),
    [['found', 'partial', null, 2], ['not_covered', 'unknown', 'watched_mode', 0]],
    'watched の週: found でも partial (監視 ASIN の無い部門の上位 3 は保存していない — R1 #2)・無い語は not_covered');
  const y = svc.lookupTerms(db, ['無い語'], { weekStart: '2026-08-23' });
  eq([y.items[0].status, y.items[0].reason], ['not_covered', 'mode_unknown'], 'mode 不明の週は not_covered');
  const z = svc.lookupTerms(db, ['残った語', '無い語'], { weekStart: '2026-07-05' });
  eq(z.items.map((i) => [i.status, i.coverage, i.reason]), [['found', 'partial', 'pruned'], ['not_covered', 'unknown', 'pruned']], 'prune した週は found でも partial・無い語は not_covered');
  const n = svc.lookupTerms(db, ['ハッカ油'], { weekStart: '2026-06-28' });
  eq([n.week, n.items[0].status, n.items[0].reason, n.items[0].departments], [null, 'no_week', 'week_not_found', []], '無い週 → no_week (最新に代替しない)');

  // 部門が複数ある週: 部門を固定して索引で引く。部門の一覧は毎回取る (覚えない)
  eq(svc.departmentsOf(db, '2026-09-13'), ['amazon.co.jp'], '部門の一覧');
  ins.run('2026-09-13', 'Books', 'ハッカ油 本', 900, 1, 'B0KKKKKKK1', 0.3, 0.3);
  ins.run('2026-09-13', 'Zz', 'ハッカ油 スプレー', 1300, 1, 'B0KKKKKKK2', 0.5, 0.5);
  eq(svc.departmentsOf(db, '2026-09-13'), ['Books', 'Zz', 'amazon.co.jp'].sort(), '部門が増えればすぐ一覧に出る (索引を飛びながら取る)');
  eq(svc.departmentsOf(db, '2026-06-28'), [], '行の無い週は空');
  const b = svc.lookupTerms(db, ['ハッカ油 本', 'ハッカ油 スプレー']);
  eq(dep(b.items[0]), [['Books', 900, ['B0KKKKKKK1:1']]], '別の部門の語も引ける');
  eq(dep(b.items[1]).sort(), [['Zz', 1300, ['B0KKKKKKK2:1']], ['amazon.co.jp', 1200, ['B0AAAAAAA1:1', 'B0BBBBBBB2:2']]].sort(),
    '同じ語が複数の部門にあれば部門ごとに {部門, その部門の順位, 上位 3} を返す (部門と順位の組み合わせを崩さない — R1 #3)');
  ok(!db.prepare('SELECT 1 FROM aba_watch_asins WHERE asin = ?').get('B0JJJJJJJ1'), '語の照会では監視登録しない');
  eq(svc.normalizeTerm('  ハッカ油　 スプレー\n'), 'ハッカ油 スプレー', '語の検査用の形 (全角空白・改行)');
  eq([svc.normalizeTerm(''), svc.normalizeTerm('x'.repeat(201))], [null, null], '空・201 文字は null');
}

console.log('[8] service-api /terms の口');
{
  const app = express();
  app.use(express.json());
  app.use('/service-api/aba', svc.default);
  const server = http.createServer(app);
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = async (body) => {
    const res = await fetch(base + '/service-api/aba/terms', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    return { status: res.status, body: await res.json().catch(() => ({})) };
  };
  eq((await call({})).status, 400, 'terms 無しは 400');
  eq((await call({ terms: 'ハッカ油' })).status, 400, '配列でなければ 400');
  eq((await call({ terms: ['', '  '] })).status, 400, '空の語だけなら 400');
  eq((await call({ terms: Array.from({ length: 51 }, (_, i) => `語${i}`) })).status, 400, '51 語は 400');
  eq((await call({ terms: ['ハッカ油'], week_start: '9/13' })).status, 400, 'week_start の形式違いは 400');
  eq((await call({ terms: [123] })).status, 400, '文字列でない語だけなら 400');
  eq((await call({ terms: [' '.repeat(1000) + 'x'] })).status, 400, '送られたままの長さが 200 文字を超えれば (整えると短くても) 400');
  const r = await call({ terms: ['ハッカ油 スプレー', ' ハッカ油　スプレー ', 'ハッカ油 スプレー', '', '無い語'] });
  ok(r.status === 200 && r.body.ok === true && r.body.result.week.week_start === '2026-09-13', '正常 (okResponse の形)');
  eq(r.body.result.items.map((i) => [i.term, i.status, i.matched_term]),
    [['ハッカ油 スプレー', 'found', 'ハッカ油 スプレー'], [' ハッカ油　スプレー ', 'found', 'ハッカ油 スプレー'], ['無い語', 'none', null]],
    '送られた語ごとに返す (同じ語は 1 回)。表記が違えば整えた形で当たり、matched_term に当たった語');
  eq(r.body.result.invalid, [''], '空の語は invalid に');
  server.close();
}

abadb.closeAbaDB();
console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
