// 🗑️取り込まなかったメールの記録 (skipped-mail.js + Gmail 同期の onSkip + /mail-rules の集計と CSV) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-skipped-mail.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) { console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-skipped-smoke)'); process.exit(2); }
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
process.env.DATA_DIR = fs.mkdtempSync(path.join(baseDir, 'smoke-skipped-'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { addMailRule, evaluateMailRules } = await import('./mail-rules.js');
const { recordSkippedMails, summarizeSkippedMails, listSkippedMails, skippedMailsCsv, domainOf, SKIPPED_KEEP_DAYS } = await import('./skipped-mail.js');
const { mapThread, createGmailAdapter } = await import('./sync/adapters/gmail.js');
const routerModule = await import('./router.js');

initInquiryHubDB();
const db = getDB();
let passed = 0, failed = 0;
const check = (name, cond, detail) => { if (cond) { passed++; console.log(`  PASS ${name}`); } else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); } };
const NOW = new Date('2026-09-21T03:00:00Z');
const ms = (iso) => Date.parse(iso);

console.log('1. 記録 (同じスレッドは 1 行・本文は持たない・30 日で消す)');
{
  const r1 = recordSkippedMails([
    { threadId: 't1', from: 'Order@Mail.RMS.rakuten.co.jp', subject: '【楽天市場】注文内容ご確認 (自動配信メール)', receivedAtMs: ms('2026-09-20T01:00:00Z'), ruleId: 5, ruleName: '楽天 注文確認' },
    { threadId: 't2', from: 'order@mail.rms.rakuten.co.jp', subject: '【楽天市場】注文内容ご確認 (自動配信メール)', receivedAtMs: ms('2026-09-20T02:00:00Z'), ruleId: 5, ruleName: '楽天 注文確認' },
    { threadId: 't3', from: 'rms-info@mail.rms.rakuten.co.jp', subject: '【重要】お買い物マラソン エントリー締切は 9/28 です\r\n(改行つき)', receivedAtMs: ms('2026-09-19T05:00:00Z'), ruleId: 5, ruleName: '楽天 注文確認' },
    { threadId: 't4', from: 'seller-notification@amazon.co.jp', subject: 'x'.repeat(500), receivedAtMs: ms('2026-09-18T05:00:00Z'), ruleId: 9, ruleName: 'Amazon 通知' },
    { threadId: '', from: 'a@b', subject: 'スレッド ID なしは捨てる' }, null,
  ], { now: NOW });
  check('記録した件数 (スレッド ID なし・null は数えない)', r1.recorded === 4 && r1.purged === 0, JSON.stringify(r1));
  const t3 = db.prepare(`SELECT * FROM skipped_mail_log WHERE thread_id = 't3'`).get();
  check('差出人は小文字・ドメインを分けて持つ・件名の改行は空白に', t3.from_domain === 'mail.rms.rakuten.co.jp' && !/[\r\n]/.test(t3.subject) && t3.received_at === '2026-09-19T05:00:00.000Z', JSON.stringify(t3));
  check('件名は 200 文字で切る', db.prepare(`SELECT length(subject) AS n FROM skipped_mail_log WHERE thread_id = 't4'`).get().n === 200);
  check('本文の列が無い', !db.prepare(`SELECT name FROM pragma_table_info('skipped_mail_log')`).all().some((c) => /body/i.test(c.name)));
  // 同じ窓を読み直した同期: 行は増えず、見た回数と最後に見た時刻だけ進む
  recordSkippedMails([{ threadId: 't1', from: 'order@mail.rms.rakuten.co.jp', subject: '【楽天市場】注文内容ご確認 (自動配信メール)', receivedAtMs: ms('2026-09-20T01:00:00Z'), ruleId: 5, ruleName: '楽天 注文確認' }], { now: new Date('2026-09-21T04:00:00Z') });
  const t1 = db.prepare(`SELECT seen_count, first_seen_at, last_seen_at FROM skipped_mail_log WHERE thread_id = 't1'`).get();
  check('同じスレッドは 1 行 (件数を同期の回数で水増ししない)', db.prepare(`SELECT COUNT(*) AS n FROM skipped_mail_log`).get().n === 4 && t1.seen_count === 2 && t1.first_seen_at < t1.last_seen_at, JSON.stringify(t1));
  // 30 日を過ぎた行は、次の記録のときに消える
  recordSkippedMails([{ threadId: 'old', from: 'x@old.example', subject: '古い', receivedAtMs: ms('2026-08-01T00:00:00Z'), ruleId: 1, ruleName: 'r' }], { now: NOW });
  const r2 = recordSkippedMails([], { now: NOW });
  check('保持期間 (30 日) を過ぎた行を消す', SKIPPED_KEEP_DAYS === 30 && r2.purged === 0 && !db.prepare(`SELECT 1 FROM skipped_mail_log WHERE thread_id = 'old'`).get(), '古い行が残っている');
  check('domainOf', domainOf('A@B.Example') === 'b.example' && domainOf('no-at') === '' && domainOf(null) === '');
}

console.log('2. 集計 (差出人のドメイン × ルール。件名の例つき)');
{
  const s = summarizeSkippedMails({ days: 14, now: NOW });
  check('合計と並び (件数の多い順)', s.total === 4 && s.groups[0].fromDomain === 'mail.rms.rakuten.co.jp' && s.groups[0].count === 3 && s.groups[0].ruleName === '楽天 注文確認', JSON.stringify(s.groups.map((g) => [g.fromDomain, g.count])));
  check('🚨 件名の例に、注文通知に混ざった運営の通知が見える (これを見つけるための画面)', s.groups[0].subjects.length === 2 && s.groups[0].subjects.some((x) => x.includes('エントリー締切')), JSON.stringify(s.groups[0].subjects));
  check('期間の外は数えない', summarizeSkippedMails({ days: 1, now: NOW }).total === 0 && summarizeSkippedMails({ days: 2, now: NOW }).total === 3);
  const csv = skippedMailsCsv(listSkippedMails({ now: NOW }));
  check('CSV: BOM・見出し・4 行・CRLF', csv.charCodeAt(0) === 0xFEFF && csv.split('\r\n')[0].endsWith('received_at,from_address,from_domain,subject,rule_id,rule_name,seen_count') && csv.trim().split('\r\n').length === 5);
  check('CSV: 式に見える件名を無害化', skippedMailsCsv([{ subject: '=HYPERLINK("http://x")' }]).includes(`"'=HYPERLINK(""http://x"")"`));
}

console.log('3. Gmail 同期: skip にしたスレッドの 差出人・件名・ルール が onSkip に渡る (本文は渡さない)');
{
  addMailRule({ name: '楽天の自動配信', matchMode: 'all', priority: 10, action: 'skip', conditions: [{ field: 'from', op: 'contains', value: '@mail.rms.rakuten.co.jp' }] });
  const b64 = (t) => Buffer.from(t, 'utf8').toString('base64url');
  const thread = (id, from, subject, body) => ({ id, messages: [{ id: `${id}-m1`, internalDate: String(ms('2026-09-20T01:00:00Z')),
    payload: { mimeType: 'text/plain', headers: [{ name: 'From', value: from }, { name: 'To', value: 'shop@example.com' }, { name: 'Subject', value: subject }], body: { data: b64(body) } } }] });
  const got = [];
  const skippedItem = mapThread(thread('g1', 'RMS <rms-info@mail.rms.rakuten.co.jp>', '【重要】エントリー締切は 9/28', '本文は記録しない'), { onSkip: (x) => got.push(x) });
  check('skip は null のまま (今までの契約)', skippedItem === null);
  check('onSkip に渡る内容', got.length === 1 && got[0].threadId === 'g1' && got[0].from === 'rms-info@mail.rms.rakuten.co.jp' && got[0].subject === '【重要】エントリー締切は 9/28'
    && got[0].receivedAtMs === ms('2026-09-20T01:00:00Z') && got[0].ruleName === '楽天の自動配信' && Number.isInteger(got[0].ruleId) && !('body' in got[0]) && !JSON.stringify(got[0]).includes('本文は記録しない'), JSON.stringify(got));
  const kept = mapThread(thread('g2', 'お客さま <customer@example.org>', '商品について', 'こんにちは'), { onSkip: (x) => got.push(x) });
  check('取り込むスレッドでは onSkip を呼ばない', kept && kept.externalInquiryId === 'g2' && got.length === 1);
  check('🚨 onSkip の中の例外で、スレッドの処理を失敗にしない', mapThread(thread('g3', 'x@mail.rms.rakuten.co.jp', 's', 'b'), { onSkip: () => { throw new Error('記録に失敗'); } }) === null);
  check('evaluateMailRules の戻り値に ruleId / ruleName がある (記録の前提)', (() => { const r = evaluateMailRules({ from: 'a@mail.rms.rakuten.co.jp', subject: '' }); return r && Number.isInteger(r.ruleId) && r.ruleName === '楽天の自動配信'; })());

  // 同期の本体: 記録の失敗は同期を止めない (取り込むスレッドは返る)。記録には skip のスレッドだけが渡る
  const threads = { g1: thread('g1', 'rms-info@mail.rms.rakuten.co.jp', '【重要】エントリー締切は 9/28', 'x'), g2: thread('g2', 'customer@example.org', '商品について', 'y') };
  const fetchImpl = async (url) => {
    const u = String(url);
    const json = (o) => ({ ok: true, status: 200, json: async () => o, text: async () => JSON.stringify(o) });
    if (u.includes('oauth2') || u.includes('/token')) return json({ access_token: 'tok', expires_in: 3600 });
    if (u.includes('/messages?')) return json({ messages: [{ id: 'g1-m1', threadId: 'g1' }, { id: 'g2-m1', threadId: 'g2' }] });
    const m = u.match(/threads\/([^?]+)/); if (m) return json(threads[decodeURIComponent(m[1])]);
    return { ok: false, status: 404, json: async () => ({}), text: async () => 'not found' };
  };
  const recorded = [];
  const mk = (skipRecorder) => createGmailAdapter({ clientId: 'c', clientSecret: 's', refreshToken: 'r', fetchImpl, sleepMs: 0, skipRecorder });
  const a1 = mk((list) => recorded.push(...list));
  const callFetch = (a) => a.fetchNew({ sinceIso: '2026-09-19T00:00:00Z', untilIso: '2026-09-21T00:00:00Z' });
  let out1 = null, err1 = null; try { out1 = await callFetch(a1); } catch (e) { err1 = e; }
  check('同期: 取り込むスレッドだけが返り、記録には skip のスレッドだけが渡る', !err1 && out1 && out1.inquiries.length === 1 && out1.inquiries[0].externalInquiryId === 'g2' && recorded.length === 1 && recorded[0].threadId === 'g1', err1 ? String(err1.message) : JSON.stringify({ n: out1?.inquiries?.length, recorded }));
  let out2 = null, err2 = null; try { out2 = await callFetch(mk(() => { throw new Error('DB が壊れている'); })); } catch (e) { err2 = e; }
  check('🚨 記録が失敗しても同期は続く', !err2 && out2 && out2.inquiries.length === 1, err2 ? String(err2.message) : '');
  let out3 = null, err3 = null; try { out3 = await callFetch(mk(null)); } catch (e) { err3 = e; }
  check('skipRecorder = null なら記録しない', !err3 && out3 && out3.inquiries.length === 1);
}

console.log('4. 画面と CSV');
{
  const app = express();
  app.use((req, _res, next) => { req.session = { email: 'tester@example.com', displayName: 'テスター' }; next(); });
  app.use('/apps/inquiry-hub', routerModule.default);
  const server = app.listen(0); const port = server.address().port;
  try {
    const page = await (await fetch(`http://127.0.0.1:${port}/apps/inquiry-hub/mail-rules`)).text();
    check('メールルールの画面に集計が出る (件名の例・CSV の入口)', page.includes('🗑️ 取り込まなかったメール') && page.includes('mail.rms.rakuten.co.jp') && page.includes('エントリー締切') && page.includes('/apps/inquiry-hub/mail-rules/skipped.csv'));
    check('件名は HTML として解釈されない', (() => { recordSkippedMails([{ threadId: 'xss', from: 'a@x.example', subject: '<script>alert(1)</script>', receivedAtMs: Date.now(), ruleId: 1, ruleName: '<b>r</b>' }]); return true; })());
    const page2 = await (await fetch(`http://127.0.0.1:${port}/apps/inquiry-hub/mail-rules`)).text();
    check('エスケープされている', !page2.includes('<script>alert(1)</script>') && page2.includes('&lt;script&gt;'));
    const res = await fetch(`http://127.0.0.1:${port}/apps/inquiry-hub/mail-rules/skipped.csv`);
    const csv = await res.text();
    check('CSV の口', res.status === 200 && /text\/csv/.test(res.headers.get('content-type')) && /attachment/.test(res.headers.get('content-disposition')) && csv.includes('from_domain'));
  } finally { server.close(); }
}

console.log(`\n結果: ${passed} passed, ${failed} failed`);
// fetch の直後に process.exit() しない (Windows の Node は libuv の assertion で異常終了する。#1386)
process.exitCode = failed ? 1 : 0;
setTimeout(() => process.exit(failed ? 1 : 0), 10000).unref();
