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
// 他のsmokeと同じDATA_DIRを共有して連続実行しても誤検知しないよう、開始時点の状態を記録
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const { runSync } = await import('./sync/engine.js');
const { addMailRule } = await import('./mail-rules.js');
const { createGmailAdapter, resolveGmailTransportFromEnv, parseFromHeader, htmlToText, mapThread, normalizeMailBody } = await import('./sync/adapters/gmail.js');

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
  // 自動配信メールの空行だらけ本文を詰める (2026-07-25 実測: スマホで画面が延々と間延びしていた)
  check('normalizeMailBody: 3連以上の空行を1行に詰める',
    normalizeMailBody('件名\n\n\n\n\n96\n\n\n\n\n本文') === '件名\n\n96\n\n本文');
  check('normalizeMailBody: 行末空白 (全角含む) 除去+CRLF正規化',
    normalizeMailBody('a  \r\nb　\r\n\r\nc') === 'a\nb\n\nc');
  check('normalizeMailBody: 前後の空白行を除去', normalizeMailBody('\n\n  本文  \n\n\n') === '本文');
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
  // 添付の外部IDは partId (MIME構造上の位置。再取得しても安定 = 同名・同サイズでも取り違えない)。
  // partId が無い形のレスポンスでは undefined = エンジンの synthetic 採番に委ねる (2026-08-02)
  {
    const a0 = r.messages[0].attachments[0];
    check('添付の外部ID', r.messages[0].attachments.length === 1 && a0.fileName === 'photo.jpg'
      && a0.externalAttachmentId === (a0.partId ? `part:${a0.partId}` : undefined), JSON.stringify(a0));
  }
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

  // 並列取得: 多数スレッドでも全件取得+途中失敗で全体throw
  const manyThreads = {};
  for (let i = 0; i < 23; i++) manyThreads['tp-' + i] = { id: 'tp-' + i, messages: [gmailMsg({ id: 'pm' + i, from: `c${i}@gmail.com` })] };
  const fPar = mockFetch((url) => {
    if (url.includes('/messages?')) return { body: { messages: Object.keys(manyThreads).map(t => ({ id: 'x' + t, threadId: t })) } };
    return { body: manyThreads[url.match(/threads\/([^?]+)/)[1]] };
  });
  const rPar = await createGmailAdapter({ ...CRED, fetchImpl: fPar, concurrency: 5 }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() });
  check('並列取得で全23スレッド取得', rPar.inquiries.length === 23);
  // レートリミッターは全ワーカー共有 (並列でも最小間隔×リクエスト数の時間がかかる)
  const t0 = Date.now();
  await createGmailAdapter({ ...CRED, sleepMs: 30, fetchImpl: fPar, concurrency: 5 }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() });
  const elapsed = Date.now() - t0;
  check('共有レートリミッター (24req×30ms間隔 ≥ 600ms)', elapsed >= 600, `elapsed=${elapsed}ms`);
  const fParFail = mockFetch((url) => {
    if (url.includes('/messages?')) return { body: { messages: Object.keys(manyThreads).map(t => ({ id: 'x' + t, threadId: t })) } };
    const tid = url.match(/threads\/([^?]+)/)[1];
    if (tid === 'tp-11') return { status: 500, body: { error: { message: 'boom' } } };
    return { body: manyThreads[tid] };
  });
  let ePar = null;
  try { await createGmailAdapter({ ...CRED, fetchImpl: fParFail, concurrency: 5 }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { ePar = e; }
  check('並列中の1件失敗で全体throw (部分成功なし)', ePar?.errorType === 'fetch_failed');
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

// ─── 5. 送信 (sendReply + outbox worker) ───
console.log('5. 送信');
{
  const { createReplyJob, runOutboxPass, SendRejectedError } = await import('./outbox.js');
  const { runInquiryHubOutboxTick, startInquiryHubOutboxCron } = await import('./sync/cron.js');
  const CRED = { clientId: 'c', clientSecret: 's', refreshToken: 'r', sleepMs: 0, ruleEvaluator: () => null };
  const threadMeta = { id: 'g1', messages: [{ id: 'a1', payload: { headers: [{ name: 'Message-ID', value: '<orig-123@mail.gmail.com>' }] } }] };

  // 5a. sendReply 単体 (live): スレッド返信ヘッダ+raw組み立て+send呼び出し
  let sendBody = null;
  const fSend = mockFetch((url, n) => {
    if (url.includes('/threads/')) return { body: threadMeta };
    return { body: { id: 'sent-001', threadId: 'g1' } };
  });
  const origFetch = fSend;
  const fSendCapture = async (url, opts) => {
    if (url.includes('messages/send')) sendBody = JSON.parse(opts.body);
    return origFetch(url, opts);
  };
  fSendCapture.calls = fSend.calls;
  const adLive = createGmailAdapter({ ...CRED, fetchImpl: fSendCapture, sendMode: 'live' });
  const sent = await adLive.sendReply({
    inquiry: { external_inquiry_id: 'g1', customer_identifier: 'Customer@Gmail.com', subject: '在庫はありますか' },
    bodyText: 'ございます。\nよろしくお願いします',
  });
  check('live送信: externalReplyId=Gmail messageId', sent.externalReplyId === 'sent-001');
  check('send payload に threadId (スレッド維持)', sendBody?.threadId === 'g1');
  const mime = Buffer.from(sendBody.raw.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  check('To=顧客 (小文字化)', mime.includes('To: customer@gmail.com'));
  check('Subject=Re:付き (RFC2047)', /Subject: =\?UTF-8\?B\?/.test(mime));
  check('In-Reply-To/References=元Message-ID', mime.includes('In-Reply-To: <orig-123@mail.gmail.com>') && mime.includes('References: <orig-123@mail.gmail.com>'));
  check('From=info@b-faith.biz', mime.includes('<info@b-faith.biz>'));
  check('本文base64が復元できる', Buffer.from(mime.split('\r\n\r\n')[1].replace(/\r\n/g, ''), 'base64').toString('utf8') === 'ございます。\nよろしくお願いします');

  // 5b. dryrun: send APIを呼ばない
  const fDry = mockFetch((url) => url.includes('/threads/') ? { body: threadMeta } : { body: {} });
  const adDry = createGmailAdapter({ ...CRED, fetchImpl: fDry, sendMode: 'dryrun' });
  const dry = await adDry.sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: 'c@x.com', subject: 'a' }, bodyText: 'x' });
  check('dryrun: 実送信せず dryRun:true', dry.dryRun === true && dry.externalReplyId.startsWith('dryrun:') && !fDry.calls.some(c => c.url.includes('messages/send')));

  // 5c. 拒否系 = SendRejectedError (未送信確定)
  let eNoTo = null;
  try { await adDry.sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: '', subject: 'a' }, bodyText: 'x' }); } catch (e) { eNoTo = e; }
  check('宛先不明は SendRejectedError', eNoTo instanceof SendRejectedError);
  let eInj = null;
  try { await adDry.sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: 'c@x.com', subject: 'a\r\nBcc: evil@x.com' }, bodyText: 'x' }); } catch (e) { eInj = e; }
  check('件名の改行 (ヘッダインジェクション) は SendRejectedError', eInj instanceof SendRejectedError);
  const f400 = mockFetch((url) => url.includes('/threads/') ? { body: threadMeta } : { status: 400, body: { error: { message: 'Invalid' } } });
  let e400 = null;
  try { await createGmailAdapter({ ...CRED, fetchImpl: f400, sendMode: 'live' }).sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: 'c@x.com', subject: 'a' }, bodyText: 'x' }); } catch (e) { e400 = e; }
  check('send 4xx は SendRejectedError (未送信確定)', e400 instanceof SendRejectedError);
  const f500 = mockFetch((url) => url.includes('/threads/') ? { body: threadMeta } : { status: 500, body: { error: { message: 'boom' } } });
  let e500 = null;
  try { await createGmailAdapter({ ...CRED, fetchImpl: f500, sendMode: 'live' }).sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: 'c@x.com', subject: 'a' }, bodyText: 'x' }); } catch (e) { e500 = e; }
  check('send 5xx は汎用エラー (→unknown)', e500 !== null && !(e500 instanceof SendRejectedError));

  // 5c-2. 宛先フォールバック (2026-08-15 実測: customer_identifier='' の問い合わせで送信が
  // send_failed になった)。スレッドの実効差出人 (自社ドメイン以外の最後の顧客) から宛先を復元する
  {
    const metaFb = { id: 'g1', messages: [
      { id: 'm1', payload: { headers: [{ name: 'From', value: 'B-Faith <info@b-faith.biz>' }] } },
      { id: 'm2', payload: { headers: [{ name: 'From', value: '復元 太郎 <Fukugen@Example.com>' }, { name: 'Message-ID', value: '<mid-2@x>' }] } },
      { id: 'm3', payload: { headers: [{ name: 'From', value: 'B-Faith <info@b-faith.biz>' }, { name: 'Message-ID', value: '<mid-3@x>' }] } },
    ] };
    let fbBody = null;
    const fFb = mockFetch((url) => url.includes('/threads/') ? { body: metaFb } : { body: { id: 'sent-fb', threadId: 'g1' } });
    const fFbCap = async (url, opts) => { if (url.includes('messages/send')) fbBody = JSON.parse(opts.body); return fFb(url, opts); };
    fFbCap.calls = fFb.calls;
    const sentFb = await createGmailAdapter({ ...CRED, fetchImpl: fFbCap, sendMode: 'live' })
      .sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: '', subject: 'a' }, bodyText: 'x' });
    const mimeFb = Buffer.from(fbBody.raw.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
    check('宛先フォールバック: スレッド末尾から顧客差出人を復元 (自社ドメインは飛ばす・小文字化)',
      sentFb.externalReplyId === 'sent-fb' && mimeFb.includes('To: fukugen@example.com'));
    check('宛先フォールバック: In-Reply-To は従来どおり最終メッセージ', mimeFb.includes('In-Reply-To: <mid-3@x>'));

    // DMARC書き換え (via グループ) のメッセージからも X-Original-Sender で復元できる
    const metaDm = { id: 'g1', messages: [
      { id: 'm1', payload: { headers: [
        { name: 'From', value: "'ある送信元' via B-Faith <info@b-faith.biz>" },
        { name: 'X-Original-Sender', value: 'customer-x@example.net' },
        { name: 'Message-ID', value: '<mid-dm@x>' },
      ] } },
    ] };
    let dmBody = null;
    const fDm = mockFetch((url) => url.includes('/threads/') ? { body: metaDm } : { body: { id: 'sent-dm', threadId: 'g1' } });
    const fDmCap = async (url, opts) => { if (url.includes('messages/send')) dmBody = JSON.parse(opts.body); return fDm(url, opts); };
    fDmCap.calls = fDm.calls;
    await createGmailAdapter({ ...CRED, fetchImpl: fDmCap, sendMode: 'live' })
      .sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: '', subject: 'a' }, bodyText: 'x' });
    const mimeDm = Buffer.from(dmBody.raw.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
    check('宛先フォールバック: DMARC書き換えは X-Original-Sender から復元', mimeDm.includes('To: customer-x@example.net'));

    // 顧客が1人も居ないスレッド (自社発の通知のみ) は従来どおり拒否 (未送信確定)
    const metaOwn = { id: 'g1', messages: [{ id: 'm1', payload: { headers: [{ name: 'From', value: 'B-Faith <info@b-faith.biz>' }] } }] };
    const fOwn = mockFetch((url) => url.includes('/threads/') ? { body: metaOwn } : { body: {} });
    let eOwn = null;
    try {
      await createGmailAdapter({ ...CRED, fetchImpl: fOwn, sendMode: 'dryrun' })
        .sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: '', subject: 'a' }, bodyText: 'x' });
    } catch (e) { eOwn = e; }
    check('宛先フォールバック: 顧客不在スレッドは SendRejectedError のまま', eOwn instanceof SendRejectedError);
  }

  // 5d-1. DRYRUNではジョブを一切消費しない (tick=プレビューのみ / runOutboxPass直呼びでもpendingへ戻す)
  const mailShop = db.prepare("SELECT id FROM shops WHERE channel_type = 'email'").get().id;
  const inqRow = db.prepare("SELECT * FROM inquiries WHERE shop_id = ? AND external_inquiry_id = 'g1'").get(mailShop);
  const job = createReplyJob({ inquiryId: inqRow.id, channelType: 'email', bodyText: '返信テストです', createdBy: 'tester', clientOperationId: 'op-send-1', baseConversationRev: inqRow.conversation_rev });
  delete process.env.INQUIRY_HUB_MAIL_SEND_MODE; // 既定=dryrun
  const preview = await runInquiryHubOutboxTick({ adapters: { email: adDry } });
  check('dryrun tick: claimせずプレビューのみ', preview.mode === 'dryrun' && preview.previewed >= 1, JSON.stringify(preview));
  check('dryrun tick: ジョブはpendingのまま (消費しない)', db.prepare('SELECT status FROM outbox_replies WHERE id = ?').get(job.id).status === 'pending');
  // チャネル別モデル (Step 4改修): isLive=false のアダプターのチャネルは claim されずプレビューのみ。
  // dryrunアダプターがrunOutboxPassへ直接渡っても防御層が pending に戻す
  check('adDry.isLive=false / adLive.isLive=true', adDry.isLive === false && adLive.isLive === true);
  const passDry = await runOutboxPass({ email: adDry }, { executor: 'server' });
  check('runOutboxPass防御: dryRun結果は pending に戻す', passDry.results.some(r => r.id === job.id && r.outcome === 'dryrun')
    && db.prepare('SELECT status, error_message FROM outbox_replies WHERE id = ?').get(job.id).status === 'pending');
  // dryrunアダプターだけのtickはlive処理ゼロ (プレビューのみ)・実送信APIも呼ばれない
  const sendCallsBefore = fSendCapture.calls.filter(c => c.url.includes('messages/send')).length;
  const tickDry = await runInquiryHubOutboxTick({ adapters: { email: adDry } });
  check('dryrunアダプターのtickはプレビューのみ (実送信ゼロ)', (tickDry.mode === 'dryrun' || tickDry.preview)
    && fSendCapture.calls.filter(c => c.url.includes('messages/send')).length === sendCallsBefore
    && db.prepare('SELECT status FROM outbox_replies WHERE id = ?').get(job.id).status === 'pending');

  // 5d-2. live: pending → sent + 会話記録 + waiting_reply (isLive=trueのアダプターは通常処理)
  const pass = await runInquiryHubOutboxTick({ adapters: { email: adLive } });
  check('outbox tick (live): sent', pass.results?.some(r => r.id === job.id && r.outcome === 'sent'), JSON.stringify(pass));
  const jobRow = db.prepare('SELECT * FROM outbox_replies WHERE id = ?').get(job.id);
  check('external_reply_id=sent-001+sent_at刻印', jobRow.status === 'sent' && jobRow.external_reply_id === 'sent-001' && !!jobRow.sent_at);
  const inqAfter = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inqRow.id);
  check('会話に送信メッセージ追加+rev++/waiting_reply', inqAfter.conversation_rev === inqRow.conversation_rev + 1 && inqAfter.internal_status === 'waiting_reply'
    && !!db.prepare("SELECT 1 FROM inquiry_messages WHERE inquiry_id = ? AND external_message_id = 'sent-001'").get(inqRow.id));

  // 5f. From実測検証 + 楽天マスクアドレス宛ガード (2026-08-24 実測: send-asエイリアス未設定で
  // Gmail が From を d.nakahara@ に置き換え → 楽天中継が 552 sender rejected でバウンス)
  {
    const metaMask = { id: 'g9', messages: [{ id: 'm1', payload: { headers: [{ name: 'Message-ID', value: '<mid-9@x>' }] } }] };
    // 読み戻し (send-verify-from) が「置き換わったFrom」を返すモック
    const fMask = mockFetch((url) => {
      if (url.includes('/threads/')) return { body: metaMask };
      if (url.includes('messages/send')) return { body: { id: 'sent-mask', threadId: 'g9' } };
      return { body: { id: 'sent-mask', payload: { headers: [{ name: 'From', value: 'D Nakahara <d.nakahara@b-faith.biz>' }] } } };
    });
    const adMask = createGmailAdapter({ ...CRED, fetchImpl: fMask, sendMode: 'live' });
    const rMask = await adMask.sendReply({ inquiry: { external_inquiry_id: 'g9', customer_identifier: 'abc123@pc.fw.rakuten.ne.jp', subject: 'a' }, bodyText: 'x' });
    check('From置き換わり検出: sent+warning (実測アドレス入り)', rMask.externalReplyId === 'sent-mask' && /d\.nakahara@b-faith\.biz/.test(rMask.warning || ''), JSON.stringify(rMask));
    check('マスクアドレス宛のwarningは不達 (バウンス) を明記', /バウンス/.test(rMask.warning || ''));

    // 置き換わりが既知になった後のマスクアドレス宛は送信前に止める (送るだけ100%バウンス)
    const sendCalls0 = fMask.calls.filter(c => c.url.includes('messages/send')).length;
    let eMask = null;
    try { await adMask.sendReply({ inquiry: { external_inquiry_id: 'g9', customer_identifier: 'xyz@pc.fw.rakuten.ne.jp', subject: 'a' }, bodyText: 'x' }); } catch (e) { eMask = e; }
    check('置き換わり既知後のマスクアドレス宛は SendRejectedError (未送信+直し方入り)',
      eMask instanceof SendRejectedError && /他のメールアドレスからメールを送信/.test(eMask.message)
      && fMask.calls.filter(c => c.url.includes('messages/send')).length === sendCalls0, String(eMask?.message));
    // 通常宛先は届く (楽天中継を通らない) ので警告付きで送れる
    const rPlain = await adMask.sendReply({ inquiry: { external_inquiry_id: 'g9', customer_identifier: 'c@example.com', subject: 'a' }, bodyText: 'x' });
    check('置き換わり既知でも通常宛先は送信可 (warning付き)', rPlain.externalReplyId === 'sent-mask' && !!rPlain.warning);

    // 設定が直った (実測From=info@) 後はマスクアドレス宛も再び通る+warningなし
    const fOk = mockFetch((url) => {
      if (url.includes('/threads/')) return { body: metaMask };
      if (url.includes('messages/send')) return { body: { id: 'sent-ok', threadId: 'g9' } };
      return { body: { id: 'sent-ok', payload: { headers: [{ name: 'From', value: '雑貨イズム <info@b-faith.biz>' }] } } };
    });
    const adOk = createGmailAdapter({ ...CRED, fetchImpl: fOk, sendMode: 'live' });
    const rOk = await adOk.sendReply({ inquiry: { external_inquiry_id: 'g9', customer_identifier: 'abc123@pc.fw.rakuten.ne.jp', subject: 'a' }, bodyText: 'x' });
    check('From=info@ならマスクアドレス宛OK+warningなし', rOk.externalReplyId === 'sent-ok' && !rOk.warning);
    // 読み戻し自体の失敗は fail-soft (送信は完了扱い・warningなし)
    const fVerifyNg = mockFetch((url) => {
      if (url.includes('/threads/')) return { body: metaMask };
      if (url.includes('messages/send')) return { body: { id: 'sent-vng', threadId: 'g9' } };
      return { status: 500, body: { error: { message: 'boom' } } };
    });
    const rVng = await createGmailAdapter({ ...CRED, fetchImpl: fVerifyNg, sendMode: 'live' })
      .sendReply({ inquiry: { external_inquiry_id: 'g9', customer_identifier: 'c@example.com', subject: 'a' }, bodyText: 'x' });
    check('From読み戻し失敗はfail-soft (sent扱い・warningなし)', rVng.externalReplyId === 'sent-vng' && !rVng.warning);

    // warning は outbox の error_message に保存される (status=sentのまま。詳細画面のジョブ履歴に出る)
    const inqRow2 = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inqRow.id);
    const jobWarn = createReplyJob({ inquiryId: inqRow.id, channelType: 'email', bodyText: '警告テスト', createdBy: 'tester', clientOperationId: 'op-send-warn', baseConversationRev: inqRow2.conversation_rev });
    await runOutboxPass({ email: adMask }, { executor: 'server' });
    const jobWarnRow = db.prepare('SELECT * FROM outbox_replies WHERE id = ?').get(jobWarn.id);
    check('outbox: sent+warningがerror_messageに保存', jobWarnRow.status === 'sent' && /実際の送信元/.test(jobWarnRow.error_message || ''), JSON.stringify({ s: jobWarnRow.status, e: jobWarnRow.error_message }));
  }

  // 5e. cron dark launch
  delete process.env.INQUIRY_HUB_OUTBOX_CRON_ENABLED;
  check('outbox cron: flag未設定は起動しない', startInquiryHubOutboxCron() === null);
}

check('DBは一時サブディレクトリのみに作成', fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
