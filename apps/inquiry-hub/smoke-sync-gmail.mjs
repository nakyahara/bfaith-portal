// Gmail受信同期アダプターのスモーク: HTTPをモックして契約 (トークン/窓/スレッド化/ルール適用/import_done) を検証
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-sync-gmail.mjs
import fs from 'fs';
import path from 'path';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-gmail-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-gmail-'));
process.env.DATA_DIR = workDir;

const { initInquiryHubDB, getDB } = await import('./db.js');
const { runSync } = await import('./sync/engine.js');
const { addMailRule } = await import('./mail-rules.js');
const { createGmailAdapter, resolveGmailTransportFromEnv, parseFromHeader, htmlToText, mapThread } = await import('./sync/adapters/gmail.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

const b64url = s => Buffer.from(s, 'utf8').toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
const NOW_MS = Date.parse('2026-07-18T09:00:00Z');
const gmailMsg = ({ id, from, to = 'info@b-faith.biz', subject = '在庫はありますか', text = '本文です', atMs = NOW_MS - 3600e3, attachments = [] }) => ({
  id, threadId: 'th-1', internalDate: String(atMs),
  payload: {
    mimeType: 'multipart/mixed',
    headers: [
      { name: 'From', value: from }, { name: 'To', value: to }, { name: 'Subject', value: subject },
    ],
    parts: [
      { mimeType: 'text/plain', body: { data: b64url(text) } },
      ...attachments.map((a, i) => ({ mimeType: 'image/jpeg', filename: a, partId: String(i), body: { size: 1234, attachmentId: 'volatile-' + Math.abs(i) } })),
    ],
  },
});

// ─── 1. ユーティリティ ───
console.log('1. ユーティリティ');
{
  check('parseFromHeader: 表示名+アドレス', JSON.stringify(parseFromHeader('山田 太郎 <Taro@Example.com>')) === JSON.stringify({ name: '山田 太郎', mailbox: 'taro@example.com' }));
  check('parseFromHeader: アドレスのみ', parseFromHeader('a@b.com').mailbox === 'a@b.com');
  check('parseFromHeader: 不正はnull', parseFromHeader('こんにちは').mailbox === null);
  check('htmlToText: タグ除去+改行', htmlToText('<p>こんにちは</p><br><div>世界 &amp; 平和</div>') === 'こんにちは\n\n世界 & 平和');
  check('env解決: INQUIRY優先', resolveGmailTransportFromEnv({ INQUIRY_GMAIL_CLIENT_ID: 'a', INQUIRY_GMAIL_CLIENT_SECRET: 'b', INQUIRY_GMAIL_REFRESH_TOKEN: 'c' })?.clientId === 'a');
  check('env解決: PO_GMAIL_*フォールバック', resolveGmailTransportFromEnv({ PO_GMAIL_CLIENT_ID: 'p', PO_GMAIL_CLIENT_SECRET: 'q', PO_GMAIL_REFRESH_TOKEN: 'r' })?.clientId === 'p');
  check('env解決: 不足はnull', resolveGmailTransportFromEnv({ PO_GMAIL_CLIENT_ID: 'p' }) === null);
  check('env解決: INQUIRY不完全なら混成せずPOセットへ', resolveGmailTransportFromEnv({
    INQUIRY_GMAIL_CLIENT_ID: 'a',
    PO_GMAIL_CLIENT_ID: 'p', PO_GMAIL_CLIENT_SECRET: 'q', PO_GMAIL_REFRESH_TOKEN: 'r',
  })?.clientId === 'p');
  let eCfg = null;
  try { createGmailAdapter({}); } catch (e) { eCfg = e; }
  check('設定なしは生成時throw', eCfg !== null);
}

// ─── 2. mapThread (ルール適用込み) ───
console.log('2. mapThread');
{
  const noRule = { ruleEvaluator: () => null };
  const thread = {
    id: 'th-1',
    messages: [
      gmailMsg({ id: 'm1', from: '顧客 <customer@gmail.com>', atMs: NOW_MS - 7200e3, attachments: ['photo.jpg'] }),
      gmailMsg({ id: 'm2', from: '雑貨イズム <info@b-faith.biz>', atMs: NOW_MS - 3600e3, text: '在庫ございます' }),
      gmailMsg({ id: 'm3', from: '顧客 <customer@gmail.com>', atMs: NOW_MS - 60e3, text: 'では購入します' }),
    ],
  };
  const r = mapThread(thread, noRule);
  check('externalInquiryId=threadId', r.externalInquiryId === 'th-1');
  check('subject/顧客名/identifier', r.subject === '在庫はありますか' && r.customerName === '顧客' && r.customerIdentifier === 'customer@gmail.com');
  check('メッセージ3件・時系列順', r.messages.length === 3 && r.messages[0].externalMessageId === 'm1' && r.messages[2].externalMessageId === 'm3');
  check('b-faith.bizドメイン=shop/outgoing', r.messages[1].senderType === 'shop' && r.messages[1].isIncoming === 0);
  check('顧客=customer/incoming', r.messages[0].senderType === 'customer' && r.messages[0].isIncoming === 1);
  check('本文デコード', r.messages[1].bodyText === '在庫ございます');
  check('添付はsynthetic採番に委ねる (externalAttachmentId=undefined)', r.messages[0].attachments.length === 1 && r.messages[0].attachments[0].externalAttachmentId === undefined && r.messages[0].attachments[0].fileName === 'photo.jpg');
  check('receivedAt=最初のメッセージ', r.receivedAt === NOW_MS - 7200e3);
  check('initialInternalStatusなし (通常取込)', r.initialInternalStatus === undefined);

  const skipped = mapThread(thread, { ruleEvaluator: () => ({ action: 'skip', ruleId: 1 }) });
  check('skipルール一致でnull', skipped === null);
  const done = mapThread(thread, { ruleEvaluator: () => ({ action: 'import_done', ruleId: 2 }) });
  check('import_doneでinitialInternalStatus=done', done.initialInternalStatus === 'done');

  // ルール評価は最初の顧客メッセージで行う
  let evalArg = null;
  mapThread(thread, { ruleEvaluator: (m) => { evalArg = m; return null; } });
  check('ルール評価は最初の顧客メッセージ (from/subject/body)', evalArg.from === 'customer@gmail.com' && evalArg.subject === '在庫はありますか' && evalArg.body === '本文です');

  // Googleグループ (info@) のDMARC From書き換えの復元 (2026-07-18 実測パターン)
  const groupMsg = (over = {}) => {
    const g = gmailMsg({ id: 'gm1', from: `"'出品者向け通知' via 会社のお問い合わせ" <info@b-faith.biz>`, subject: '注文確定のお知らせ' });
    g.payload.headers.push(...(over.extraHeaders || []));
    return { id: 'th-g', messages: [g] };
  };
  const gr = mapThread(groupMsg({ extraHeaders: [{ name: 'X-Original-Sender', value: 'auto-confirm@amazon.co.jp' }] }), noRule);
  check('グループ書き換え: X-Original-Senderで元差出人に復元 → customer/incoming',
    gr.messages[0].senderType === 'customer' && gr.messages[0].isIncoming === 1 && gr.customerIdentifier === 'auto-confirm@amazon.co.jp');
  check('グループ書き換え: 表示名からvia以降を除去', gr.customerName === '出品者向け通知');
  const gr2 = mapThread(groupMsg({ extraHeaders: [{ name: 'Reply-To', value: 'buyer@example.com' }] }), noRule);
  check('X-Original-Sender無し+via表記はReply-Toで復元', gr2.messages[0].isIncoming === 1 && gr2.customerIdentifier === 'buyer@example.com');
  const gr3 = mapThread({ id: 'th-s', messages: [gmailMsg({ id: 'gs1', from: '雑貨イズム <info@b-faith.biz>' })] }, noRule);
  check('書き換え痕跡なしのinfo@発信はshopのまま', gr3.messages[0].senderType === 'shop');
  // ルール評価も復元後のFromで行われる (skipルールが元差出人ドメインで効く)
  let grEval = null;
  mapThread(groupMsg({ extraHeaders: [{ name: 'X-Original-Sender', value: 'auto-confirm@amazon.co.jp' }] }), { ruleEvaluator: (m) => { grEval = m; return null; } });
  check('ルール評価は復元後のFrom', grEval.from === 'auto-confirm@amazon.co.jp');

  let eBad = null;
  try { mapThread({ id: 'x', messages: [] }, noRule); } catch (e) { eBad = e; }
  check('空スレッドは contract_violation', eBad?.errorType === 'contract_violation');
  let eBad2 = null;
  try { mapThread({ id: 'x', messages: [{ id: 'm', payload: { headers: [] } }] }, noRule); } catch (e) { eBad2 = e; }
  check('internalDate欠落は contract_violation', eBad2?.errorType === 'contract_violation');
}

// ─── 3. fetchNew (HTTPモック) ───
console.log('3. fetchNew');
function mockFetch(handler) {
  const calls = [];
  const fn = async (url, opts) => {
    calls.push({ url, opts });
    if (url.includes('oauth2.googleapis.com')) return { ok: true, status: 200, json: async () => ({ access_token: 'tok', expires_in: 3600 }) };
    const r = handler(url, calls.length);
    if (r instanceof Error) throw r;
    return { ok: (r.status ?? 200) < 400, status: r.status ?? 200, json: async () => r.body };
  };
  fn.calls = calls;
  return fn;
}
{
  const CRED = { clientId: 'c', clientSecret: 's', refreshToken: 'r', sleepMs: 0, ruleEvaluator: () => null };
  const threads = {
    'th-1': { id: 'th-1', messages: [gmailMsg({ id: 'm1', from: 'a@gmail.com' })] },
    'th-2': { id: 'th-2', messages: [gmailMsg({ id: 'm2', from: 'ads@spam.example', subject: '広告' })] },
  };
  const f = mockFetch((url) => {
    if (url.includes('/messages?')) {
      const page2 = url.includes('pageToken');
      return { body: page2
        ? { messages: [{ id: 'm2', threadId: 'th-2' }] }
        : { messages: [{ id: 'm1', threadId: 'th-1' }, { id: 'm1b', threadId: 'th-1' }], nextPageToken: 'p2' } };
    }
    const tid = url.match(/threads\/([^?]+)/)?.[1];
    return { body: threads[tid] };
  });
  const ad = createGmailAdapter({ ...CRED, fetchImpl: f });
  const r = await ad.fetchNew({ sinceIso: new Date(NOW_MS - 3600e3).toISOString(), untilIso: new Date(NOW_MS).toISOString() });
  check('ページング+threadId重複排除 (list2回+threads2回)', f.calls.filter(c => c.url.includes('/messages?')).length === 2 && f.calls.filter(c => c.url.includes('/threads/')).length === 2);
  check('2スレッド取得', r.inquiries.length === 2);
  const listUrl = f.calls.find(c => c.url.includes('/messages?')).url;
  check('after:秒-1 + -in:chats のクエリ', decodeURIComponent(listUrl).includes('-in:chats after:' + (Math.floor((NOW_MS - 3600e3) / 1000) - 1)));

  // skipルールで除外
  const ad2 = createGmailAdapter({ ...CRED, fetchImpl: f, ruleEvaluator: (m) => (m.subject === '広告' ? { action: 'skip', ruleId: 9 } : null) });
  const r2 = await ad2.fetchNew({ sinceIso: new Date(NOW_MS - 3600e3).toISOString(), untilIso: new Date(NOW_MS).toISOString() });
  check('skipルールでスレッド除外', r2.inquiries.length === 1 && r2.inquiries[0].externalInquiryId === 'th-1');

  const fAuthNg = mockFetch(() => ({ status: 401, body: { error: { message: 'Invalid Credentials' } } }));
  let e401 = null;
  try { await createGmailAdapter({ ...CRED, fetchImpl: fAuthNg }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { e401 = e; }
  check('401は auth', e401?.errorType === 'auth');

  const fTokenNg = (() => { const fn = async (url) => ({ ok: false, status: 400, json: async () => ({ error: 'invalid_grant' }) }); fn.calls = []; return fn; })();
  let eTok = null;
  try { await createGmailAdapter({ ...CRED, fetchImpl: fTokenNg }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eTok = e; }
  check('refresh token失効は auth (invalid_grant)', eTok?.errorType === 'auth' && /invalid_grant/.test(eTok.message));

  const fMany = mockFetch((url) => url.includes('/messages?')
    ? { body: { messages: [{ id: 'x', threadId: 'tx' }], nextPageToken: 'more' } }
    : { body: threads['th-1'] });
  let eMany = null;
  try { await createGmailAdapter({ ...CRED, fetchImpl: fMany, maxListPages: 3 }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eMany = e; }
  check('maxListPages超過は window_too_large', eMany?.errorType === 'window_too_large');
}

// ─── 4. エンジン結合 (mail_rules 実物 + runSync) ───
console.log('4. エンジン結合');
{
  addMailRule({ name: 'スパム除去', matchMode: 'all', priority: 10, action: 'skip',
    conditions: [{ field: 'from', op: 'ends_with', value: '@spam.example' }] });
  addMailRule({ name: '自動配信は完了扱い', matchMode: 'all', priority: 20, action: 'import_done',
    conditions: [{ field: 'subject', op: 'contains', value: '注文キャンセル完了' }] });

  const shopId = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール','info@b-faith.biz')").run().lastInsertRowid;
  let threads = {
    'g1': { id: 'g1', messages: [gmailMsg({ id: 'a1', from: '顧客 <c1@gmail.com>' })] },
    'g2': { id: 'g2', messages: [gmailMsg({ id: 'a2', from: 'x <bot@spam.example>' })] },
    'g3': { id: 'g3', messages: [gmailMsg({ id: 'a3', from: 'no-reply@rakuten.co.jp', subject: '注文キャンセル完了のお知らせ' })] },
  };
  const f = mockFetch((url) => {
    if (url.includes('/messages?')) return { body: { messages: Object.keys(threads).map(t => ({ id: 'x' + t, threadId: t })) } };
    const tid = url.match(/threads\/([^?]+)/)?.[1];
    return { body: threads[tid] };
  });
  const ad = createGmailAdapter({ clientId: 'c', clientSecret: 's', refreshToken: 'r', sleepMs: 0, fetchImpl: f });

  const r1 = await runSync(shopId, ad, { now: NOW_MS });
  check('初回同期: skip除外で2問い合わせ', r1.ok && r1.stats.newInquiries === 2, JSON.stringify(r1));
  const inqs = db.prepare('SELECT * FROM inquiries WHERE shop_id = ? ORDER BY external_inquiry_id').all(shopId);
  check('通常取込=open/未読', inqs.find(i => i.external_inquiry_id === 'g1').internal_status === 'open' && inqs.find(i => i.external_inquiry_id === 'g1').is_unread === 1);
  const g3 = inqs.find(i => i.external_inquiry_id === 'g3');
  check('import_done=done/既読/completed_at', g3.internal_status === 'done' && g3.is_unread === 0 && !!g3.completed_at);
  check('skipされたスレッドはDBに無い', !inqs.find(i => i.external_inquiry_id === 'g2'));

  const r2 = await runSync(shopId, ad, { now: NOW_MS + 15 * 60000 });
  check('再同期は冪等 (新規0)', r2.ok && r2.stats.newInquiries === 0 && r2.stats.newMessages === 0, JSON.stringify(r2));

  // done スレッドに顧客の追い返信 → 再オープン+未読化 (エンジン既存ロジック)
  threads.g3.messages.push(gmailMsg({ id: 'a4', from: '顧客 <c2@gmail.com>', text: 'キャンセルできてますか?', atMs: NOW_MS + 20 * 60000 }));
  const r3 = await runSync(shopId, ad, { now: NOW_MS + 30 * 60000 });
  const g3b = db.prepare('SELECT * FROM inquiries WHERE shop_id = ? AND external_inquiry_id = ?').get(shopId, 'g3');
  check('done扱いスレッドへの顧客返信で再オープン', r3.ok && r3.stats.reopened === 1 && g3b.internal_status === 'open' && g3b.is_unread === 1, JSON.stringify(r3));

  check('sent_at がUTC正準形式', /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(
    db.prepare('SELECT sent_at FROM inquiry_messages LIMIT 1').get().sent_at));
}

check('DBは一時サブディレクトリのみに作成', fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && !fs.existsSync(path.join(baseDir, 'inquiry-hub.db')));

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
