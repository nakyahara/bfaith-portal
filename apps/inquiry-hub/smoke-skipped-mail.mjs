// 🗑️取り込まなかったメールの記録 (skipped-mail.js + Gmail 同期の onSkip + /mail-rules の集計と CSV) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-skipped-mail.mjs
// ⭐時計は 1 つ (NOW = 実行時刻) にそろえる: 画面は実時計で期間を切るので、固定の日付で記録すると日が経ったときに落ちる
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) { console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-skipped-smoke)'); process.exit(2); }
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
process.env.DATA_DIR = fs.mkdtempSync(path.join(baseDir, 'smoke-skipped-'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { addMailRule, evaluateMailRules } = await import('./mail-rules.js');
const { recordSkippedMails, summarizeSkippedMails, listSkippedMails, skippedMailsCsv, domainOf, subjectPattern, SKIPPED_KEEP_DAYS, SUMMARY_GROUP_LIMIT } = await import('./skipped-mail.js');
const { mapThread, createGmailAdapter } = await import('./sync/adapters/gmail.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();
let passed = 0, failed = 0;
const check = (name, cond, detail) => { if (cond) { passed++; console.log(`  PASS ${name}`); } else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); } };
const NOW = new Date();
const ago = (days, hours = 0) => NOW.getTime() - days * 86400000 - hours * 3600000;
const isoAgo = (days, hours = 0) => new Date(ago(days, hours)).toISOString();
const ORDER = '【楽天市場】注文内容ご確認 (自動配信メール)';
const rec = (threadId, from, subject, receivedAtMs, extra = {}) => ({ threadId, from, subject, receivedAtMs, ruleId: 5, ruleName: '楽天 注文確認', ...extra });

console.log('1. 記録 (同じスレッドは 1 行・本文は持たない・期間は activity_at・30 日で消す)');
{
  const r1 = recordSkippedMails([
    rec('t1', 'Order@Mail.RMS.rakuten.co.jp', ORDER, ago(1, 2)), rec('t2', 'order@mail.rms.rakuten.co.jp', ORDER, ago(1, 1)),
    rec('t3', 'rms-info@mail.rms.rakuten.co.jp', '【重要】お買い物マラソン エントリー締切は 9/28 です\r\n(改行つき)', ago(2)),
    rec('t4', 'seller-notification@amazon.co.jp', 'x'.repeat(500), ago(3), { ruleId: 9, ruleName: 'Amazon 通知' }),
    { threadId: '', from: 'a@b', subject: 'スレッド ID なしは捨てる' }, null,
  ], { now: NOW });
  check('記録した件数 (スレッド ID なし・null は数えない)', r1.recorded === 4 && r1.purged === 0, JSON.stringify(r1));
  const t3 = db.prepare(`SELECT * FROM skipped_mail_log WHERE thread_id = 't3'`).get();
  check('差出人は小文字・ドメインを分けて持つ・件名の改行は空白に・activity_at は受信時刻', t3.from_domain === 'mail.rms.rakuten.co.jp' && !/[\r\n]/.test(t3.subject) && t3.received_at === isoAgo(2) && t3.activity_at === isoAgo(2), JSON.stringify(t3));
  check('件名は 200 文字で切る', db.prepare(`SELECT length(subject) AS n FROM skipped_mail_log WHERE thread_id = 't4'`).get().n === 200);
  check('本文の列が無い', !db.prepare(`SELECT name FROM pragma_table_info('skipped_mail_log')`).all().some((c) => /body/i.test(c.name)));
  recordSkippedMails([rec('t1', 'order@mail.rms.rakuten.co.jp', ORDER, ago(1, 2))], { now: new Date(NOW.getTime() + 3600000) });   // 同じ窓を読み直した同期
  const t1 = db.prepare(`SELECT seen_count, first_seen_at, last_seen_at FROM skipped_mail_log WHERE thread_id = 't1'`).get();
  check('同じスレッドは 1 行 (件数を同期の回数で水増ししない)', db.prepare(`SELECT COUNT(*) AS n FROM skipped_mail_log`).get().n === 4 && t1.seen_count === 2 && t1.first_seen_at < t1.last_seen_at, JSON.stringify(t1));

  // 🚨 古いスレッドに今日の返信が付いて skip された (判定に使う最初の受信は 50 日前)。期間は「いちばん新しい受信」で切る = 記録直後に消えない・画面に出る (Codex #1400 R1)
  const r3 = recordSkippedMails([rec('reply', 'rms-info@mail.rms.rakuten.co.jp', 'Re: 【重要】出店規約の変更について (同意期限あり)', ago(50), { latestReceivedAtMs: ago(0, 1) })], { now: NOW });
  const rp = db.prepare(`SELECT received_at, activity_at FROM skipped_mail_log WHERE thread_id = 'reply'`).get();
  check('🚨 返信が付いた古いスレッド: 記録直後に消えない・受信時刻は最初の受信のまま・activity_at は今日', r3.purged === 0 && rp && rp.received_at === isoAgo(50) && rp.activity_at === isoAgo(0, 1), JSON.stringify({ r3, rp }));
  check('🚨 その記録が直近 14 日の集計に出る', summarizeSkippedMails({ days: 14, now: NOW }).groups.some((g) => g.subjects.concat(g.rareSubjects).some((s) => s.includes('同意期限'))));

  // 保持: activity_at から 30 日。skip が 0 件の回 (空の配列) でも消す
  recordSkippedMails([rec('old', 'x@old.example', '古い', ago(40))], { now: new Date(ago(35)) });   // 35 日前の同期で記録された (その時点では 5 日前の受信)
  check('35 日前の時点では残っている', !!db.prepare(`SELECT 1 FROM skipped_mail_log WHERE thread_id = 'old'`).get());
  const r2 = recordSkippedMails([], { now: NOW });
  check('🚨 skip が 0 件の回でも、保持期間 (30 日) を過ぎた行を消す', SKIPPED_KEEP_DAYS === 30 && r2.recorded === 0 && r2.purged === 1 && !db.prepare(`SELECT 1 FROM skipped_mail_log WHERE thread_id = 'old'`).get(), JSON.stringify(r2));
  // 受信時刻が分からない再観測で、保持期限を延ばさない
  recordSkippedMails([rec('nots', 'a@nots.example', '時刻なし', null)], { now: new Date(ago(20)) });
  recordSkippedMails([rec('nots', 'a@nots.example', '時刻なし', null)], { now: NOW });
  check('受信時刻の無い行: 最初に見た時刻を保つ (再観測のたびに保持期限が延びない)', db.prepare(`SELECT activity_at FROM skipped_mail_log WHERE thread_id = 'nots'`).get().activity_at === isoAgo(20));
  // 未来・範囲の外の受信時刻は「分からない」= 期間が先へ延びない・同じバッチの正常な行を巻き戻さない (Codex #1400 R2)
  const rf = recordSkippedMails([rec('future', 'a@future.example', '未来', ago(-400)), rec('huge', 'a@huge.example', '範囲の外', 1e20), rec('okrow', 'a@ok.example', '正常', ago(1))], { now: NOW });
  const fa = (id) => db.prepare(`SELECT received_at, activity_at FROM skipped_mail_log WHERE thread_id = ?`).get(id);
  check('🚨 未来・範囲の外の受信時刻: 例外にせず、activity_at は記録した時刻 (先へ延びない)・同じバッチの正常な行も入る', rf.recorded === 3 && fa('future').received_at === null && fa('future').activity_at === NOW.toISOString() && fa('huge').activity_at === NOW.toISOString() && fa('okrow').activity_at === isoAgo(1), JSON.stringify([fa('future'), fa('huge'), fa('okrow')]));
  db.exec(`DELETE FROM skipped_mail_log WHERE thread_id IN ('future', 'huge', 'okrow')`);
  check('domainOf / subjectPattern', domainOf('A@B.Example') === 'b.example' && domainOf('no-at') === '' && domainOf(null) === '' && subjectPattern('注文番号 ２６３９４７-20260921-0001 のご確認') === '注文番号 #-#-# のご確認');
}

console.log('2. 集計 (差出人のドメイン × ルール。件名は数字を寄せた型で数える・多い型 3 つ + まれな型 2 つ)');
{
  // 注文番号つきの件名 60 通 (1 通ずつ違う文字列) + 型の違う自動配信 3 種 + 締切の通知 1 通。今までの「多い件名 3 つ」では締切の通知が例に出ない形
  const many = [];
  for (let i = 0; i < 60; i++) many.push(rec(`o${i}`, 'order@mall.example', `ご注文 ${100000 + i} を受け付けました`, ago(1, i % 20)));
  for (let i = 0; i < 9; i++) many.push(rec(`s${i}`, 'order@mall.example', `発送完了のお知らせ (注文 ${200000 + i})`, ago(2)));
  for (let i = 0; i < 5; i++) many.push(rec(`c${i}`, 'order@mall.example', `キャンセル受付 ${300000 + i}`, ago(2)));
  for (let i = 0; i < 4; i++) many.push(rec(`p${i}`, 'order@mall.example', `入金確認 ${400000 + i}`, ago(2)));
  many.push(rec('deadline', 'order@mall.example', '【要対応】特商法表記の修正期限は 10/5 です', ago(3)));
  recordSkippedMails(many, { now: NOW });
  const s = summarizeSkippedMails({ days: 14, now: NOW });
  const g = s.groups[0];
  check('件数の多い順・合計は全体', g.fromDomain === 'mall.example' && g.count === 79 && s.total === 79 + 5 /* 1. の 4 通 + 返信の付いたスレッド。時刻なしの行は 20 日前 = 14 日の外 */ && s.groupCount === s.groups.length, JSON.stringify([g.fromDomain, g.count, s.total, s.groupCount]));
  check('注文番号つきの件名は 1 つの型に寄る (型は 5 種類)', g.patternCount === 5 && g.subjects.length === 3 && /ご注文 \d+ を受け付けました/.test(g.subjects[0]), JSON.stringify([g.patternCount, g.subjects]));
  check('🚨 多数派に埋もれた締切の通知が「まれな件名」に出る (これを見つけるための画面)', g.rareSubjects.length === 2 && g.rareSubjects[0].includes('修正期限は 10/5') && !g.subjects.some((x) => x.includes('修正期限')), JSON.stringify(g.rareSubjects));
  const rk = s.groups.find((x) => x.fromDomain === 'mail.rms.rakuten.co.jp');
  check('ルール名と最後の受信は、いちばん新しい行から', rk && rk.ruleName === '楽天 注文確認' && rk.lastActivityAt === isoAgo(0, 1) && rk.rareSubjects.length === 0, JSON.stringify(rk));
  check('期間の外は数えない', summarizeSkippedMails({ days: 1, now: NOW }).total < s.total);
  // 上位だけを出すときは、全体の組の数が分かる
  const lots = []; for (let i = 0; i < SUMMARY_GROUP_LIMIT + 7; i++) lots.push(rec(`d${i}`, `a@d${String(i).padStart(3, '0')}.example`, '件名', ago(1)));
  recordSkippedMails(lots, { now: NOW });
  const s2 = summarizeSkippedMails({ days: 14, now: NOW });
  check('上位 100 組だけを返し、全体の組の数と合計は切らない', s2.shownGroups === SUMMARY_GROUP_LIMIT && s2.groupCount === s.groupCount + SUMMARY_GROUP_LIMIT + 7 && s2.total === s.total + SUMMARY_GROUP_LIMIT + 7, JSON.stringify([s2.shownGroups, s2.groupCount, s2.total]));
  const csv = skippedMailsCsv(listSkippedMails({ now: NOW }));
  check('CSV: BOM・見出し・CRLF・全行', csv.charCodeAt(0) === 0xFEFF && csv.split('\r\n')[0].endsWith('activity_at,received_at,from_address,from_domain,subject,rule_id,rule_name,seen_count') && csv.trim().split('\r\n').length === db.prepare(`SELECT COUNT(*) AS n FROM skipped_mail_log WHERE activity_at >= ?`).get(isoAgo(SKIPPED_KEEP_DAYS)).n + 1);   // CSV は保持期間 (30 日) の全行 = 14 日の集計より多い
  check('CSV: 式に見える件名を無害化', skippedMailsCsv([{ subject: '=HYPERLINK("http://x")' }]).includes(`"'=HYPERLINK(""http://x"")"`));
}

console.log('3. Gmail 同期: skip にしたスレッドの 差出人・件名・ルール が onSkip に渡る (本文は渡さない)');
{
  addMailRule({ name: '楽天の自動配信', matchMode: 'all', priority: 10, action: 'skip', conditions: [{ field: 'from', op: 'contains', value: '@mail.rms.rakuten.co.jp' }] });
  const b64 = (t) => Buffer.from(t, 'utf8').toString('base64url');
  const message = (id, from, subject, body, atMs) => ({ id, internalDate: String(atMs),
    payload: { mimeType: 'text/plain', headers: [{ name: 'From', value: from }, { name: 'To', value: 'shop@example.com' }, { name: 'Subject', value: subject }], body: { data: b64(body) } } });
  const thread = (id, from, subject, body, atMs = ago(1)) => ({ id, messages: [message(`${id}-m1`, from, subject, body, atMs)] });
  const got = [];
  const skippedItem = mapThread(thread('g1', 'RMS <rms-info@mail.rms.rakuten.co.jp>', '【重要】エントリー締切は 9/28', '本文は記録しない'), { onSkip: (x) => got.push(x) });
  check('skip は null のまま (今までの契約)', skippedItem === null);
  check('onSkip に渡る内容 (本文は無い)', got.length === 1 && got[0].threadId === 'g1' && got[0].from === 'rms-info@mail.rms.rakuten.co.jp' && got[0].subject === '【重要】エントリー締切は 9/28'
    && got[0].receivedAtMs === ago(1) && got[0].latestReceivedAtMs === ago(1) && got[0].ruleName === '楽天の自動配信' && Number.isInteger(got[0].ruleId) && !('body' in got[0]) && !JSON.stringify(got[0]).includes('本文は記録しない'), JSON.stringify(got));
  // 返信の付いたスレッド: 判定は最初の受信・期間の時刻はいちばん新しい受信
  const multi = { id: 'g4', messages: [message('g4-m1', 'rms-info@mail.rms.rakuten.co.jp', '規約の変更', 'a', ago(50)), message('g4-m2', 'rms-info@mail.rms.rakuten.co.jp', 'Re: 規約の変更', 'b', ago(0, 2))] };
  const got4 = []; mapThread(multi, { onSkip: (x) => got4.push(x) });
  check('🚨 返信の付いたスレッド: receivedAtMs は最初の受信・latestReceivedAtMs はいちばん新しい受信', got4.length === 1 && got4[0].receivedAtMs === ago(50) && got4[0].latestReceivedAtMs === ago(0, 2) && got4[0].subject === '規約の変更', JSON.stringify(got4));
  const kept = mapThread(thread('g2', 'お客さま <customer@example.org>', '商品について', 'こんにちは'), { onSkip: (x) => got.push(x) });
  check('取り込むスレッドでは onSkip を呼ばない', kept && kept.externalInquiryId === 'g2' && got.length === 1);
  const warns = []; const origWarn = console.warn; console.warn = (...a) => warns.push(a.join(' '));
  let r3;
  try { r3 = mapThread(thread('g3', 'x@mail.rms.rakuten.co.jp', '件名は警告に出さない', '本文も出さない'), { onSkip: () => { throw new Error('記録に失敗'); } }); } finally { console.warn = origWarn; }
  check('🚨 onSkip の中の例外でスレッドの処理を失敗にしない・黙らない (警告にメールの中身は出さない)', r3 === null && warns.length === 1 && warns[0].includes('記録に失敗') && !warns[0].includes('件名は警告に出さない') && !warns[0].includes('本文も出さない'), JSON.stringify(warns));
  check('evaluateMailRules の戻り値に ruleId / ruleName がある (記録の前提)', (() => { const r = evaluateMailRules({ from: 'a@mail.rms.rakuten.co.jp', subject: '' }); return r && Number.isInteger(r.ruleId) && r.ruleName === '楽天の自動配信'; })());

  // 同期の本体
  const threads = { g1: thread('g1', 'rms-info@mail.rms.rakuten.co.jp', '【重要】エントリー締切は 9/28', 'x'), g2: thread('g2', 'customer@example.org', '商品について', 'y') };
  const mkFetch = (ids) => async (url) => {
    const u = String(url);
    const json = (o) => ({ ok: true, status: 200, json: async () => o, text: async () => JSON.stringify(o) });
    if (u.includes('oauth2') || u.includes('/token')) return json({ access_token: 'tok', expires_in: 3600 });
    if (u.includes('/messages?')) return json({ messages: ids.map((id) => ({ id: `${id}-m1`, threadId: id })) });
    const m = u.match(/threads\/([^?]+)/); if (m) return json(threads[decodeURIComponent(m[1])]);
    return { ok: false, status: 404, json: async () => ({}), text: async () => 'not found' };
  };
  const mk = (skipRecorder, ids = ['g1', 'g2']) => createGmailAdapter({ clientId: 'c', clientSecret: 's', refreshToken: 'r', fetchImpl: mkFetch(ids), sleepMs: 0, skipRecorder });
  const callFetch = (a) => a.fetchNew({ sinceIso: isoAgo(2), untilIso: NOW.toISOString() });
  const calls = [];
  let out1 = null, err1 = null; try { out1 = await callFetch(mk((list) => calls.push(list.map((x) => x.threadId)))); } catch (e) { err1 = e; }
  check('同期: 取り込むスレッドだけが返り、記録には skip のスレッドだけが渡る', !err1 && out1 && out1.inquiries.length === 1 && out1.inquiries[0].externalInquiryId === 'g2' && JSON.stringify(calls) === '[["g1"]]', err1 ? String(err1.message) : JSON.stringify(calls));
  let out0 = null, err0 = null; try { out0 = await callFetch(mk((list) => calls.push(list.map((x) => x.threadId)), ['g2'])); } catch (e) { err0 = e; }
  check('🚨 skip が 0 件の回も記録の関数を呼ぶ (空の配列 = 古い行を消す機会)', !err0 && out0 && out0.inquiries.length === 1 && JSON.stringify(calls) === '[["g1"],[]]', JSON.stringify(calls));
  const w2 = []; console.warn = (...a) => w2.push(a.join(' '));
  let out2 = null, err2 = null; try { out2 = await callFetch(mk(() => { throw new Error('DB が壊れている'); })); } catch (e) { err2 = e; } finally { console.warn = origWarn; }
  check('🚨 記録が失敗しても同期は続く・警告を出す', !err2 && out2 && out2.inquiries.length === 1 && w2.some((x) => x.includes('DB が壊れている')), err2 ? String(err2.message) : JSON.stringify(w2));
  let out3 = null, err3 = null; try { out3 = await callFetch(mk(null)); } catch (e) { err3 = e; }
  check('skipRecorder = null なら記録しない', !err3 && out3 && out3.inquiries.length === 1);
  const before = db.prepare(`SELECT COUNT(*) AS n FROM skipped_mail_log`).get().n;
  let out4 = null, err4 = null; try { out4 = await callFetch(mk(undefined)); } catch (e) { err4 = e; }
  check('既定では skipped_mail_log に入る (本物の記録の関数)', !err4 && out4 && db.prepare(`SELECT COUNT(*) AS n FROM skipped_mail_log`).get().n === before + 1 && !!db.prepare(`SELECT 1 FROM skipped_mail_log WHERE thread_id = 'g1'`).get());
}

console.log('4. 画面と CSV (認証は通さない = ポータルの requireAppAccess の外側で router だけを見る)');
{
  // 上位 100 組に入るよう 3 通にする (1 通だけの組は、件数の同点で表の外に出る)
  recordSkippedMails([1, 2, 3].map((i) => rec(`xss${i}`, 'a@xss.example', '<script>alert(1)</script>', ago(0, 3), { ruleId: 1, ruleName: '<b>r</b>' })), { now: NOW });
  const app = express();
  app.use((req, _res, next) => { req.session = { email: 'tester@example.com', displayName: 'テスター' }; next(); });
  app.use('/apps/inquiry-hub', routerModule.default);
  const server = app.listen(0); const port = server.address().port;
  try {
    const page = await (await fetch(`http://127.0.0.1:${port}/apps/inquiry-hub/mail-rules`)).text();
    check('メールルールの画面に集計が出る (件名の例・まれな件名・CSV の入口)', page.includes('🗑️ 取り込まなかったメール') && page.includes('mall.example') && page.includes('まれな件名') && page.includes('修正期限は 10/5') && page.includes('/apps/inquiry-hub/mail-rules/skipped.csv'));
    check('上位だけを出しているときは、そう書く', page.includes(`件数の多い ${SUMMARY_GROUP_LIMIT} 組だけ`));
    check('件名・ルール名は HTML として解釈されない', !page.includes('<script>alert(1)</script>') && page.includes('&lt;script&gt;alert(1)&lt;/script&gt;') && !page.includes('<b>r</b>'));
    const res = await fetch(`http://127.0.0.1:${port}/apps/inquiry-hub/mail-rules/skipped.csv`);
    const csv = await res.text();
    const n = db.prepare(`SELECT COUNT(*) AS n FROM skipped_mail_log WHERE activity_at >= ?`).get(isoAgo(SKIPPED_KEEP_DAYS)).n;
    check('CSV の口 (全行)', res.status === 200 && /text\/csv/.test(res.headers.get('content-type')) && /attachment/.test(res.headers.get('content-disposition')) && csv.trim().split('\r\n').length === n + 1, `${csv.trim().split('\r\n').length} vs ${n + 1}`);
    // 🚨 集計が壊れても、ルールの画面は出す
    db.exec(`ALTER TABLE skipped_mail_log RENAME TO skipped_mail_log_x`);
    const broken = await fetch(`http://127.0.0.1:${port}/apps/inquiry-hub/mail-rules`);
    const brokenPage = await broken.text();
    db.exec(`ALTER TABLE skipped_mail_log_x RENAME TO skipped_mail_log`);
    check('🚨 集計が壊れてもルールの画面は出る', broken.status === 200 && brokenPage.includes('集計を表示できません') && brokenPage.includes('ルールを手動追加'));
  } finally { server.close(); }
}

console.log('5. 🚨 「全件 CSV」は 5 万件を超えても切らない (Codex #1400 R1)');
{
  const big = []; for (let i = 0; i < 50010; i++) big.push(rec(`big${i}`, 'bulk@bulk.example', `件名 ${i}`, ago(1)));
  const t0 = Date.now(); recordSkippedMails(big, { now: NOW });
  const rows = listSkippedMails({ now: NOW });
  check(`5 万件超でも全行 (${rows.length} 行・記録 ${Date.now() - t0} ms)`, rows.filter((r) => r.from_domain === 'bulk.example').length === 50010);
  const s = summarizeSkippedMails({ days: 14, now: NOW });
  check('集計も全行を数える', s.groups[0].fromDomain === 'bulk.example' && s.groups[0].count === 50010 && s.groups[0].patternCount === 1);
}

console.log(`\n結果: ${passed} passed, ${failed} failed`);
// fetch の直後に process.exit() しない (Windows の Node は libuv の assertion で異常終了する。#1386)
process.exitCode = failed ? 1 : 0;
setTimeout(() => process.exit(failed ? 1 : 0), 10000).unref();
