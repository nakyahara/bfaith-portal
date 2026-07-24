// Yahoo!受信同期アダプターのスモーク: HTTPをモックして契約 (窓スキャン/候補選定/マッピング/失敗時全体throw) を検証
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-sync-yahoo.mjs
// DATA_DIR 直下は使わず毎回ユニークな一時サブディレクトリを作る (既存ファイルは一切削除しない)
import fs from 'fs';
import path from 'path';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。作業ディレクトリを指定してください (例: DATA_DIR=c:/tmp/ih-yahoo-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-yahoo-'));
process.env.DATA_DIR = workDir;

// db.js は import 時点で DATA_DIR を定数化するため、env 差し替え後に動的 import する
const { initInquiryHubDB, getDB } = await import('./db.js');
const { runSync } = await import('./sync/engine.js');
const { createYahooAdapter, mapTopicDetail, unixSecToMs, resolveYahooTransportFromEnv,
  DEFAULT_LIST_LOOKBACK_DAYS } = await import('./sync/adapters/yahoo.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── フィクスチャ (時刻はUNIX秒) ───
const NOW_MS = Date.parse('2026-07-17T09:00:00Z');
const sec = deltaMin => Math.floor(NOW_MS / 1000) + deltaMin * 60;
const CFG = { proxyUrl: 'http://localhost:18080', proxySecret: 'ps', sleepMs: 0 };

const headline = (over = {}) => ({
  topicId: 'topic-a', userIdx: 'u1', isUnread: true, isUserUnRead: false, isNoAnswer: false,
  isCompleted: false, completeConditionId: 0, completeConditionShortName: null,
  userPostTime: sec(-30), sellerPostTime: null,
  qaType: 'item', isPrivate: true, category: 11, title: '在庫について',
  body: '…', messageCount: 1, userMaskedId: 'ab***', itemCode: 'item-1', orderId: 'ord-1',
  firstPoster: 'user', serviceType: 'shopping', memo: null,
  ...over,
});
const detailBody = (over = {}) => ({
  topic: {
    accessUserType: 'seller', userLastReadTime: String(sec(-30)), isUserUnRead: false,
    sellerLastReadTime: String(sec(-60)), isSellerUnRead: true, isPrivate: true,
    isComplete: false, completeConditionId: null, isMail: false,
    userMaskedIdx: 'ab***', itemcode: 'item-1', orderid: 'ord-1', categoryid: 11,
    categoryName: ['商品の質問'], title: '在庫について', isAucChild: false, qaType: 'item',
    ...(over.topic || {}),
  },
  messages: over.messages || [
    { messageId: 1, postUserType: 'user', bid: null, postdate: String(sec(-30)), body: '在庫ありますか', fileList: [] },
  ],
});
const listBody = (headlines, count = headlines.length, start = 1) =>
  ({ summary: { filter: 'all', unansweredCount: 1, topic: { start, end: start + headlines.length - 1, count } }, headlines });

function mockFetch(handler) {
  const calls = [];
  const fn = async (url) => {
    calls.push(url);
    const r = handler(url, calls.length);
    if (r instanceof Error) throw r;
    return { status: r.status ?? 200, text: async () => JSON.stringify(r.body ?? {}) };
  };
  fn.calls = calls;
  return fn;
}
const paramOf = (url, key) => new URL(url).searchParams.get(key);

// ─── 1. ユーティリティ ───
console.log('1. ユーティリティ');
check('unixSecToMs: 文字列秒→ms', unixSecToMs('1784252091') === 1784252091000);
check('unixSecToMs: 不正はnull', unixSecToMs('x') === null && unixSecToMs(null) === null);
check('env解決: 揃っていれば設定を返す', resolveYahooTransportFromEnv({ YAHOO_PROXY_URL: 'u', YAHOO_PROXY_SECRET: 's' })?.proxySecret === 's');
check('env解決: 不足はnull', resolveYahooTransportFromEnv({ YAHOO_PROXY_URL: 'u' }) === null);
let eCfg = null;
try { createYahooAdapter({ sleepMs: 0 }); } catch (e) { eCfg = e; }
check('プロキシ設定なしは生成時throw', eCfg !== null);

// ─── 2. マッピング ───
console.log('2. マッピング');
{
  const d = detailBody({
    messages: [
      { messageId: 1, postUserType: 'user', postdate: String(sec(-120)), body: '質問', fileList: [] },
      { messageId: 2, postUserType: 'seller', postdate: String(sec(-90)), body: '回答', fileList: [] },
      { messageId: 3, postUserType: 'user', postdate: String(sec(-30)), body: '追い質問', fileList: [{ fileId: 'f1', fileName: 'photo.jpg' }] },
    ],
  });
  const r = mapTopicDetail('topic-a', d, headline());
  check('externalInquiryId=topicId', r.externalInquiryId === 'topic-a');
  check('subject=topic.title', r.subject === '在庫について');
  check('customerIdentifier=userMaskedIdx', r.customerIdentifier === 'ab***');
  check('注文/商品の紐付け', r.orderNumber === 'ord-1' && r.productCode === 'item-1');
  check('externalStatus open', r.externalStatus === 'open');
  check('externalIsRead = !isSellerUnRead', r.externalIsRead === false);
  check('receivedAt=最初のメッセージ時刻', r.receivedAt === sec(-120) * 1000);
  check('メッセージ3件', r.messages.length === 3);
  const [m1, m2, m3] = r.messages;
  check('m:1 user→customer/incoming', m1.externalMessageId === 'm:1' && m1.senderType === 'customer' && m1.isIncoming === 1);
  check('m:2 seller→shop/outgoing', m2.externalMessageId === 'm:2' && m2.senderType === 'shop' && m2.isIncoming === 0);
  check('添付 fileId/fileName', m3.attachments[0].externalAttachmentId === 'f1' && m3.attachments[0].fileName === 'photo.jpg');
  check('completed変換', mapTopicDetail('t', detailBody({ topic: { isComplete: true } })).externalStatus === 'completed');
  let badType = null;
  try { mapTopicDetail('t', detailBody({ messages: [{ messageId: 1, postUserType: 'admin', postdate: String(sec(-1)), body: 'x' }] })); }
  catch (e) { badType = e; }
  check('未知postUserTypeは contract_violation', badType?.errorType === 'contract_violation');
  let badShape = null;
  try { mapTopicDetail('t', { nope: 1 }); } catch (e) { badShape = e; }
  check('詳細構造不一致は contract_violation', badShape?.errorType === 'contract_violation');
}

// ─── 3. 窓スキャンと候補選定 ───
console.log('3. 窓スキャンと候補選定');
{
  // h1: 顧客新着 (since以降) → 候補 / h2: 店舗返信のみ最近 (userPostTimeは古いが窓内) → 候補
  // h3: 窓内だが変化なし → 候補外 / h4: lookbackより古い → スキャン打ち切り
  const h1 = headline({ topicId: 't-new', userPostTime: sec(-10) });
  const h2 = headline({ topicId: 't-seller', userPostTime: sec(-60 * 24 * 2), sellerPostTime: sec(-5) });
  const h3 = headline({ topicId: 't-old-quiet', userPostTime: sec(-60 * 24 * 3), sellerPostTime: sec(-60 * 24 * 3) });
  const h4 = headline({ topicId: 't-ancient', userPostTime: sec(-60 * 24 * 30) }); // lookback14日より古い
  const f = mockFetch((url) => {
    if (url.includes('externalTalkList')) return { body: listBody([h1, h2, h3, h4], 100) };
    const topicId = paramOf(url, 'topicId');
    return { body: detailBody({ messages: [{ messageId: 1, postUserType: 'user', postdate: String(sec(-10)), body: 'q', fileList: [] }] }) };
  });
  const ad = createYahooAdapter({ ...CFG, fetchImpl: f });
  const r = await ad.fetchNew({ sinceIso: new Date(NOW_MS - 3600e3).toISOString(), untilIso: new Date(NOW_MS).toISOString() });
  check('候補=顧客新着+店舗返信ありの2件', r.inquiries.length === 2, `got=${r.inquiries.length}`);
  const detailCalls = f.calls.filter(u => u.includes('externalTalkDetail'));
  check('詳細取得は候補のみ (2回)', detailCalls.length === 2);
  check('詳細対象 = t-new / t-seller',
    detailCalls.some(u => paramOf(u, 'topicId') === 't-new') && detailCalls.some(u => paramOf(u, 'topicId') === 't-seller'));
  check('lookback超で打ち切り (一覧1ページのみ)', f.calls.filter(u => u.includes('externalTalkList')).length === 1);
  check('X-Proxy-Secret以外のヘッダを使わない (URLにsecretなし)', !f.calls.some(u => u.includes('ps')));
}

// ─── 4. ページング ───
console.log('4. ページング');
{
  const mk = (i, delta) => headline({ topicId: `t-${i}`, userPostTime: sec(delta) });
  const page1 = Array.from({ length: 20 }, (_, i) => mk(i, -i));
  const page2 = [mk(20, -21), mk(21, -22)];
  const f = mockFetch((url) => {
    if (url.includes('externalTalkList')) {
      const start = Number(paramOf(url, 'start'));
      return { body: listBody(start === 1 ? page1 : page2, 22, start) };
    }
    return { body: detailBody() };
  });
  const ad = createYahooAdapter({ ...CFG, fetchImpl: f });
  const r = await ad.fetchNew({ sinceIso: new Date(NOW_MS - 3600e3).toISOString(), untilIso: new Date(NOW_MS).toISOString() });
  const listCalls = f.calls.filter(u => u.includes('externalTalkList'));
  check('2ページ目まで取得 (start=1,21)', listCalls.length === 2 && paramOf(listCalls[1], 'start') === '21');
  check('窓内22件全て候補', r.inquiries.length === 22);
}

// ─── 5. 失敗系 ───
console.log('5. 失敗系');
{
  const f403 = mockFetch(() => ({ status: 403, body: '<Error><Message>x</Message><Code>px-04306</Code></Error>' }));
  // textはJSON.stringifyされるためXML判定用に生textを返すfetchを直接作る
  const rawFetch = (status, text) => {
    const fn = async () => ({ status, text: async () => text });
    fn.calls = [];
    return fn;
  };
  let e403 = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: rawFetch(403, '<Error><Message>IP</Message><Code>px-04306</Code></Error>') }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { e403 = e; }
  check('403は auth + Codeのみ抽出 (本文露出なし)', e403?.errorType === 'auth' && /px-04306/.test(e403.message) && !/IP/.test(e403.message.replace('HTTP', '')));

  let eBadJson = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: rawFetch(200, '<html>gateway error</html>') }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eBadJson = e; }
  check('非JSONレスポンスは contract_violation', eBadJson?.errorType === 'contract_violation');

  // 詳細途中失敗 → 全体throw (部分成功なし)
  const fPartial = mockFetch((url) => {
    if (url.includes('externalTalkList')) return { body: listBody([headline({ topicId: 'a', userPostTime: sec(-5) }), headline({ topicId: 'b', userPostTime: sec(-6) })], 2) };
    if (paramOf(url, 'topicId') === 'a') return { body: detailBody() };
    return { status: 500, body: { error: 'boom' } };
  });
  let ePartial = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: fPartial }).fetchNew({ sinceIso: new Date(NOW_MS - 3600e3).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { ePartial = e; }
  check('詳細2件目失敗で全体throw', ePartial?.errorType === 'fetch_failed');

  const fMany = mockFetch(() => ({ body: listBody(Array.from({ length: 20 }, (_, i) => headline({ topicId: `x${i}`, userPostTime: sec(-i) })), 99999) }));
  let eMany = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: fMany, maxPages: 2 }).fetchNew({ sinceIso: new Date(NOW_MS - 3600e3).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eMany = e; }
  check('maxPages超過は window_too_large', eMany?.errorType === 'window_too_large');

  const timeoutErr = new Error('aborted'); timeoutErr.name = 'TimeoutError';
  let eTimeout = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: mockFetch(() => timeoutErr) }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eTimeout = e; }
  check('タイムアウトは fetch_failed', eTimeout?.errorType === 'fetch_failed' && /タイムアウト/.test(eTimeout.message));

  // userPostTime不正を0扱いで打ち切ると取りこぼしを成功コミットしてしまう (Codex R1 high)
  const fBadTime = mockFetch(() => ({ body: listBody([headline({ topicId: 'bad', userPostTime: undefined })], 1) }));
  let eBadTime = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: fBadTime }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eBadTime = e; }
  check('userPostTime不正は contract_violation (0扱い打ち切り禁止)', eBadTime?.errorType === 'contract_violation');
  const fBadSeller = mockFetch(() => ({ body: listBody([headline({ topicId: 'bad2', sellerPostTime: 'zzz' })], 1) }));
  let eBadSeller = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: fBadSeller }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eBadSeller = e; }
  check('sellerPostTime不正 (存在時) も contract_violation', eBadSeller?.errorType === 'contract_violation');

  // 非JSONエラーに本文を含めない (PII露出防止。Codex R1 medium)
  let eNoBody = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: rawFetch(200, '<html>secret-customer-text</html>') }).fetchNew({ sinceIso: new Date(NOW_MS - 1e5).toISOString(), untilIso: new Date(NOW_MS).toISOString() }); }
  catch (e) { eNoBody = e; }
  check('非JSONエラーは本文を含めない', eNoBody !== null && !/secret-customer-text/.test(eNoBody.message));
}

// ─── 6. エンジン結合 ───
console.log('6. エンジン結合');
{
  const shopId = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('yahoo','Yahoo店','b-faith01')").run().lastInsertRowid;
  let msgs = [{ messageId: 1, postUserType: 'user', postdate: String(sec(-30)), body: '質問です', fileList: [] }];
  let userPost = sec(-30), sellerPost = null;
  const f = mockFetch((url) => {
    if (url.includes('externalTalkList')) return { body: listBody([headline({ topicId: 'yt-1', userPostTime: userPost, sellerPostTime: sellerPost })], 1) };
    return { body: detailBody({ messages: msgs }) };
  });
  const ad = createYahooAdapter({ ...CFG, fetchImpl: f });

  const r1 = await runSync(shopId, ad, { now: NOW_MS });
  check('初回同期: 1問い合わせ+1メッセージ', r1.ok && r1.stats.newInquiries === 1 && r1.stats.newMessages === 1, JSON.stringify(r1));

  const r2 = await runSync(shopId, ad, { now: NOW_MS + 15 * 60000 });
  check('再同期は冪等 (新規0)', r2.ok && r2.stats.newInquiries === 0 && r2.stats.newMessages === 0, JSON.stringify(r2));

  // 店舗返信 (Yahoo管理画面から) → sellerPostTime だけ進む → 候補に入り新着msg1
  msgs = [...msgs, { messageId: 2, postUserType: 'seller', postdate: String(sec(20)), body: '回答です', fileList: [] }];
  sellerPost = sec(20);
  const r3 = await runSync(shopId, ad, { now: NOW_MS + 30 * 60000 });
  check('店舗返信を取り込み (sellerPostTime浮上なしでも一覧窓内なら検出)', r3.ok && r3.stats.newMessages === 1, JSON.stringify(r3));

  const inq = db.prepare('SELECT * FROM inquiries WHERE shop_id = ?').get(shopId);
  check('channel_type=yahoo で保存', inq.channel_type === 'yahoo' && inq.external_inquiry_id === 'yt-1');
  const ids = db.prepare('SELECT external_message_id FROM inquiry_messages WHERE inquiry_id = ? ORDER BY id').all(inq.id).map(x => x.external_message_id);
  check('メッセージID = m:1 / m:2', ids.join(',') === 'm:1,m:2');
  check('sent_at がUTC正準形式', /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(
    db.prepare('SELECT sent_at FROM inquiry_messages WHERE inquiry_id = ? LIMIT 1').get(inq.id).sent_at));
}

// ─── 7. 返信送信 (sendReply) ───
console.log('7. 返信送信');
{
  const { SendRejectedError } = await import('./outbox.js');
  const okInq = { external_inquiry_id: '33c2dfab10ef4a' };

  const fNone = mockFetch(() => ({ body: {} }));
  const adDry = createYahooAdapter({ ...CFG, fetchImpl: fNone });
  const dry = await adDry.sendReply({ inquiry: okInq, bodyText: 'ご返信です' });
  check('dryrun (既定): dryRun:true + API未呼び出し', dry.dryRun === true && fNone.calls.length === 0);
  check('isLive フラグ', adDry.isLive === false && createYahooAdapter({ ...CFG, fetchImpl: fNone, sendMode: 'live' }).isLive === true);

  let eTopic = null;
  try { await adDry.sendReply({ inquiry: { external_inquiry_id: 'bad topic!' }, bodyText: 'x' }); } catch (e) { eTopic = e; }
  check('topicId不正は SendRejectedError', eTopic instanceof SendRejectedError);
  let eLong = null;
  try { await adDry.sendReply({ inquiry: okInq, bodyText: 'あ'.repeat(2001) }); } catch (e) { eLong = e; }
  check('2000字超は SendRejectedError (公式上限)', eLong instanceof SendRejectedError && /2000/.test(eLong.message));

  // live: passthroughへPOST + messageid → m:<id> (受信のID体系と整合)
  let sentReq = null;
  const fLive = (() => {
    const fn = async (url, opts) => { sentReq = { url, opts }; return { status: 200, text: async () => JSON.stringify({ topicid: okInq.external_inquiry_id, messageid: 4, postdate: '1784254138' }) }; };
    fn.calls = []; return fn;
  })();
  const adLive = createYahooAdapter({ ...CFG, fetchImpl: fLive, sendMode: 'live' });
  const live = await adLive.sendReply({ inquiry: okInq, bodyText: '在庫ございます' });
  check('live: passthrough URL+X-Proxy-Secret+{topicId,message}', sentReq.url === 'http://localhost:18080/yahoo/externalTalkAdd'
    && sentReq.opts.headers['X-Proxy-Secret'] === 'ps' && JSON.parse(sentReq.opts.body).topicId === okInq.external_inquiry_id);
  check('live: externalReplyId=m:<messageid> (受信同期と二重表示しない)', live.externalReplyId === 'm:4');
  const dryOverride = await adLive.sendReply({ inquiry: okInq, bodyText: 'x', dryRun: true });
  check('liveアダプターでも dryRun:true 強制', dryOverride.dryRun === true);

  const f400 = (() => { const fn = async () => ({ status: 400, text: async () => JSON.stringify({ error: { reason: 'invalid topic' } }) }); fn.calls = []; return fn; })();
  let e400 = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: f400, sendMode: 'live' }).sendReply({ inquiry: okInq, bodyText: 'x' }); } catch (e) { e400 = e; }
  check('4xxは SendRejectedError (reason抽出)', e400 instanceof SendRejectedError && /invalid topic/.test(e400.message));
  const f500 = (() => { const fn = async () => ({ status: 500, text: async () => 'boom' }); fn.calls = []; return fn; })();
  let e500 = null;
  try { await createYahooAdapter({ ...CFG, fetchImpl: f500, sendMode: 'live' }).sendReply({ inquiry: okInq, bodyText: 'x' }); } catch (e) { e500 = e; }
  check('5xxは汎用エラー (→unknown)', e500 !== null && !(e500 instanceof SendRejectedError));

  // 2xxでもmessageid欠落/非JSONは結果不明 (unknown)。sent確定させると次回同期と二重表示になる (Codex R1 high)
  for (const [label, text] of [['messageid欠落', JSON.stringify({ topicid: 'x' })], ['非JSON', '<html>ok?</html>']]) {
    const fNoId = (() => { const fn = async () => ({ status: 200, text: async () => text }); fn.calls = []; return fn; })();
    let eNoId = null;
    try { await createYahooAdapter({ ...CFG, fetchImpl: fNoId, sendMode: 'live' }).sendReply({ inquiry: okInq, bodyText: 'x' }); } catch (e) { eNoId = e; }
    check(`2xxでも${label}は汎用エラー (→unknown、SendRejectedにしない)`, eNoId !== null && !(eNoId instanceof SendRejectedError) && /結果不明/.test(eNoId.message));
  }
}

check('DBは一時サブディレクトリのみに作成 (ベース直下に漏れない)',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && !fs.existsSync(path.join(baseDir, 'inquiry-hub.db')));

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
