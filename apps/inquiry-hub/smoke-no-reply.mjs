// 🚫返信不可アドレス + 🔴配信失敗 (バウンス) 検知のスモーク
//   2026-08-26 本番事故 (no-reply@mercari-shops.com 宛の返信が黙ってバウンス) の再発防止テスト。
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-no-reply.mjs
import fs from 'fs';
import path from 'path';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-noreply-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-noreply-'));
process.env.DATA_DIR = workDir;

const { initInquiryHubDB, getDB } = await import('./db.js');
const { runSync } = await import('./sync/engine.js');
const { createReplyJob, runOutboxPass, SendRejectedError } = await import('./outbox.js');
const { classifyReplyDestination, blockedReplyDestination, isBounceSignature } = await import('./no-reply.js');
const { createGmailAdapter, mapThread, isBounceMessage } = await import('./sync/adapters/gmail.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

const b64url = s => Buffer.from(s, 'utf8').toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
const NOW_MS = Date.parse('2026-08-26T03:20:00Z');   // JST 12:20 (事故当日のバウンス受信時刻)
const msg = ({ id, from, subject = 'お問い合わせ', text = '本文', atMs = NOW_MS, headers = [] }) => ({
  id, threadId: 'th-1', internalDate: String(atMs),
  payload: {
    mimeType: 'multipart/mixed',
    headers: [{ name: 'From', value: from }, { name: 'To', value: 'info@b-faith.biz' },
      { name: 'Subject', value: subject }, ...headers],
    parts: [{ mimeType: 'text/plain', body: { data: b64url(text) } }],
  },
});
const noRule = { ruleEvaluator: () => null };

// ─── 1. 返信不可アドレスの判定 ───
console.log('1. classifyReplyDestination');
{
  const merc = classifyReplyDestination('no-reply@mercari-shops.com');
  check('事故の実アドレスを返信不可と判定', merc.sendable === false && merc.kind === 'notify_only_domain');
  check('返信不可には「どこで返信するか」の案内が付く', /メルカリShops/.test(merc.guide || ''));
  check('メルカリは通常のローカル部でも通知専用扱い', classifyReplyDestination('shop@mercari.com').sendable === false);
  check('no-reply系 (区切り記号・+タグ違いも吸収)',
    ['noreply@x.jp', 'no-reply@x.jp', 'No.Reply+ec@X.jp', 'donotreply@x.jp', 'do-not-reply@x.jp']
      .every(a => !classifyReplyDestination(a).sendable));
  check('mailer-daemon / postmaster は返信不可',
    !classifyReplyDestination('mailer-daemon@googlemail.com').sendable
    && !classifyReplyDestination('postmaster@x.jp').sendable);
  check('空・不正アドレスは返信不可 (invalid)',
    classifyReplyDestination('').kind === 'invalid' && classifyReplyDestination('abc').kind === 'invalid');
  // 誤爆防止 — 顧客の実アドレスを塞いだら返信できなくなるので前方一致を広げないこと
  const legit = ['customer@gmail.com', 'norikoreply@gmail.com', 'bouncehouse@gmail.com',
    'no.tanaka@example.jp', 'reply@example.jp', 'postmaster-club@example.jp'];
  check('通常の顧客アドレスは送信可', legit.every(a => classifyReplyDestination(a).sendable),
    JSON.stringify(legit.filter(a => !classifyReplyDestination(a).sendable)));
  // 楽天マスクアドレスは楽天SMTP経路で実際に届く → 塞いではいけない
  check('楽天あんしんメルアドは送信可のまま', classifyReplyDestination('abc123@pc.fw.rakuten.ne.jp').sendable);

  // blockedReplyDestination = 「確実に届かない」ものだけ塞ぐ。
  // 空/解析不能は送信時にスレッドから宛先を復元する経路があるので塞いではいけない (2026-08-15の対応)
  check('blocked: no-reply は塞ぐ', !!blockedReplyDestination('no-reply@mercari-shops.com'));
  check('blocked: 空・null は塞がない (スレッドから復元される)',
    blockedReplyDestination('') === null && blockedReplyDestination(null) === null);
  check('blocked: 解析不能な文字列も塞がない', blockedReplyDestination('顧客名しかない') === null);
  check('blocked: 通常の顧客アドレスは塞がない', blockedReplyDestination('customer@gmail.com') === null);
}

// ─── 2. バウンス判定 ───
console.log('2. isBounceSignature / isBounceMessage');
{
  check('MAILER-DAEMON 発', isBounceSignature({ from: 'mailer-daemon@googlemail.com' }));
  check('RFC3464 delivery-status',
    isBounceSignature({ from: 'x@y.jp', contentType: 'multipart/report; report-type=delivery-status; boundary=b' }));
  check('X-Failed-Recipients ヘッダ', isBounceSignature({ from: 'x@y.jp', hasFailedRecipients: true }));
  check('件名 (日本語の配信未完了)', isBounceSignature({ from: 'x@y.jp', subject: '** 配信未完了 **' }));
  check('通常の顧客メールはバウンスでない',
    !isBounceSignature({ from: 'customer@gmail.com', subject: '在庫はありますか' }));
  check('isBounceMessage が payload から判定',
    isBounceMessage(msg({ id: 'b', from: 'Mail Delivery Subsystem <mailer-daemon@googlemail.com>' }).payload));
}

// ─── 3. mapThread: バウンスの扱い ───
console.log('3. mapThread');
{
  // 事故の再現形: メルカリShops通知 → 店舗が返信 → バウンス、が1スレッドに並ぶ
  const thread = { id: 'th-1', messages: [
    msg({ id: 'm1', from: 'メルカリShops <no-reply@mercari-shops.com>', atMs: NOW_MS - 172800e3, text: '購入者からのお問い合わせ' }),
    msg({ id: 'm2', from: '雑貨イズム <info@b-faith.biz>', atMs: NOW_MS - 96000e3, text: '承知いたしました' }),
    msg({ id: 'm3', from: 'Mail Delivery Subsystem <mailer-daemon@googlemail.com>', atMs: NOW_MS,
      subject: 'Delivery Status Notification (Failure)', text: '配信未完了' }),
  ] };
  const r = mapThread(thread, noRule);
  check('バウンスは senderType=system', r.messages[2].senderType === 'system');
  check('バウンスも is_incoming=1 (未読化して気付けるように)', r.messages[2].isIncoming === 1);
  check('deliveryFailedAt = バウンス受信時刻', r.deliveryFailedAt === NOW_MS);
  check('customer_identifier がバウンスに乗っ取られない',
    r.customerIdentifier === 'no-reply@mercari-shops.com', String(r.customerIdentifier));

  // 店舗発で始まりバウンスだけが返ってきたスレッド (最初の受信=バウンス)
  const onlyBounce = mapThread({ id: 'th-2', messages: [
    msg({ id: 'n1', from: '雑貨イズム <info@b-faith.biz>', atMs: NOW_MS - 3600e3 }),
    msg({ id: 'n2', from: 'Mail Delivery Subsystem <mailer-daemon@googlemail.com>', atMs: NOW_MS, subject: '配信未完了' }),
  ] }, noRule);
  check('受信がバウンスのみでも identifier を mailer-daemon にしない',
    onlyBounce.customerIdentifier !== 'mailer-daemon@googlemail.com', String(onlyBounce.customerIdentifier));

  const clean = mapThread({ id: 'th-3', messages: [msg({ id: 'c1', from: '顧客 <customer@gmail.com>' })] }, noRule);
  check('バウンスが無ければ deliveryFailedAt は付かない', clean.deliveryFailedAt === undefined);
  check('通常の顧客メッセージは customer のまま', clean.messages[0].senderType === 'customer');
}

// ─── 4. sendReply: 返信不可宛は未送信で拒否 ───
console.log('4. sendReply ガード');
{
  const mockFetch = (handler) => {
    const calls = [];
    const fn = async (url, opts) => {
      calls.push({ url, opts });
      if (url.includes('oauth2.googleapis.com')) {
        return { ok: true, status: 200, json: async () => ({ access_token: 'tok', expires_in: 3600 }) };
      }
      const r = handler(url, opts);
      return { ok: (r.status ?? 200) < 400, status: r.status ?? 200, json: async () => r.body };
    };
    fn.calls = calls;
    return fn;
  };
  const CRED = { clientId: 'c', clientSecret: 's', refreshToken: 'r', sleepMs: 0, ruleEvaluator: () => null };
  const meta = { id: 'g1', messages: [{ id: 'a1', payload: { headers: [{ name: 'Message-ID', value: '<orig@x>' }] } }] };
  const f = mockFetch((url) => url.includes('/threads/') ? { body: meta } : { body: { id: 'sent-1', threadId: 'g1' } });
  const ad = createGmailAdapter({ ...CRED, fetchImpl: f, sendMode: 'live' });

  let eNr = null;
  try {
    await ad.sendReply({
      inquiry: { external_inquiry_id: 'g1', customer_identifier: 'no-reply@mercari-shops.com', subject: 'お問い合わせ' },
      bodyText: 'x',
    });
  } catch (e) { eNr = e; }
  check('no-reply宛は SendRejectedError (未送信確定 = 自動再送しない)', eNr instanceof SendRejectedError);
  check('エラー文に返信先の案内が入る', /メルカリShops/.test(String(eNr?.message || '')), String(eNr?.message || ''));
  check('送信APIを一切叩いていない', !f.calls.some(c => c.url.includes('messages/send')));

  // 宛先フォールバックがバウンス送信者を拾わない (バウンスはスレッド末尾に付くため危険)
  const metaFb = { id: 'g2', messages: [
    { id: 'm1', payload: { headers: [{ name: 'From', value: 'B-Faith <info@b-faith.biz>' }] } },
    { id: 'm2', payload: { headers: [{ name: 'From', value: '顧客 <Kokyaku@Example.com>' }] } },
    { id: 'm3', payload: { headers: [{ name: 'From', value: 'B-Faith <info@b-faith.biz>' }] } },
    { id: 'm4', payload: { headers: [
      { name: 'From', value: 'Mail Delivery Subsystem <mailer-daemon@googlemail.com>' },
      { name: 'Message-ID', value: '<b@x>' },
    ] } },
  ] };
  let fbRaw = null;
  const fFb = mockFetch((url, opts) => {
    if (url.includes('/threads/')) return { body: metaFb };
    fbRaw = JSON.parse(opts.body).raw;
    return { body: { id: 'sent-fb', threadId: 'g2' } };
  });
  await createGmailAdapter({ ...CRED, fetchImpl: fFb, sendMode: 'live' })
    .sendReply({ inquiry: { external_inquiry_id: 'g2', customer_identifier: '', subject: 'a' }, bodyText: 'x' });
  const mime = Buffer.from(fbRaw.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  check('宛先フォールバックはバウンス送信者を飛ばして顧客を選ぶ',
    mime.includes('To: kokyaku@example.com') && !mime.includes('mailer-daemon'));

  // 通常の顧客宛はこれまでどおり送れる (ガードが送信全体を壊していないこと)
  const fOk = mockFetch((url) => url.includes('/threads/') ? { body: meta } : { body: { id: 'sent-ok', threadId: 'g1' } });
  const okSent = await createGmailAdapter({ ...CRED, fetchImpl: fOk, sendMode: 'live' })
    .sendReply({ inquiry: { external_inquiry_id: 'g1', customer_identifier: 'customer@gmail.com', subject: 'a' }, bodyText: 'x' });
  check('通常の顧客宛は従来どおり送信できる', okSent.externalReplyId === 'sent-ok');
}

// ─── 5. エンジン: delivery_failed_at の反映 ───
console.log('5. 同期での配信失敗フラグ');
{
  db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール','info@b-faith.biz')").run();
  const shopId = db.prepare("SELECT id FROM shops WHERE channel_type='email'").get().id;
  const fakeAdapter = (items) => ({ channelType: 'email', fetchNew: async () => ({ inquiries: items }) });
  const item = (over = {}) => ({
    externalInquiryId: 'th-bounce', customerIdentifier: 'no-reply@mercari-shops.com',
    subject: 'お問い合わせ', receivedAt: NOW_MS - 172800e3,
    messages: [{ externalMessageId: 'x1', senderType: 'customer', isIncoming: 1, bodyText: 'q', sentAt: NOW_MS - 172800e3 }],
    ...over,
  });

  await runSync(shopId, fakeAdapter([item()]));
  const before = db.prepare("SELECT * FROM inquiries WHERE external_inquiry_id='th-bounce'").get();
  check('バウンス無しでは delivery_failed_at は NULL', before.delivery_failed_at === null);

  const r2 = await runSync(shopId, fakeAdapter([item({ deliveryFailedAt: NOW_MS })]));
  const after = db.prepare("SELECT * FROM inquiries WHERE external_inquiry_id='th-bounce'").get();
  check('deliveryFailedAt で delivery_failed_at が立つ',
    after.delivery_failed_at === '2026-08-26T03:20:00Z', String(after.delivery_failed_at));
  check('⚠️要確認も立つ (既存の導線に乗せる)', after.needs_attention === 1);
  check('同期サマリに件数が出る', r2.stats.deliveryFailed === 1, JSON.stringify(r2.stats));
  check('対応履歴に delivery_failed が残る',
    !!db.prepare("SELECT 1 FROM inquiry_activity_logs WHERE inquiry_id=? AND action_type='delivery_failed'").get(after.id));

  // 新規メッセージが無い再同期でも冪等 (2回目はログを増やさない = 前進のみ)
  await runSync(shopId, fakeAdapter([item({ deliveryFailedAt: NOW_MS - 3600e3 })]));
  const kept = db.prepare("SELECT * FROM inquiries WHERE external_inquiry_id='th-bounce'").get();
  check('古いバウンスで巻き戻らない', kept.delivery_failed_at === '2026-08-26T03:20:00Z');
  check('重複ログを作らない',
    db.prepare("SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id=? AND action_type='delivery_failed'").get(after.id).c === 1);

  // ─── 6. 返信ジョブ作成のガード + 送信成功でのフラグ解除 ───
  console.log('6. 返信ジョブ');
  const inq = db.prepare("SELECT * FROM inquiries WHERE external_inquiry_id='th-bounce'").get();
  const blocked = createReplyJob({
    inquiryId: inq.id, channelType: 'email', bodyText: 'テスト', createdBy: 'smoke',
    clientOperationId: 'op-blocked', baseConversationRev: inq.conversation_rev,
  });
  check('返信不可アドレスはジョブ作成の時点で断る',
    blocked.id === null && /メルカリShops/.test(blocked.conflict || ''), JSON.stringify(blocked));
  check('断られたジョブはDBに残らない', db.prepare('SELECT COUNT(*) c FROM outbox_replies').get().c === 0);

  // 返信可能なアドレスに直せば作成でき、送信成功で配信失敗フラグが消える
  db.prepare("UPDATE inquiries SET customer_identifier='customer@gmail.com' WHERE id=?").run(inq.id);
  const inq2 = db.prepare('SELECT * FROM inquiries WHERE id=?').get(inq.id);
  const job = createReplyJob({
    inquiryId: inq2.id, channelType: 'email', bodyText: 'あらためてご連絡します', createdBy: 'smoke',
    clientOperationId: 'op-ok', baseConversationRev: inq2.conversation_rev,
  });
  check('返信可能なアドレスならジョブを作れる', job.created === true && job.id > 0, JSON.stringify(job));
  await runOutboxPass({
    email: { channelType: 'email', isLive: true, sendReply: async () => ({ externalReplyId: 'sent-999' }) },
  });
  const cleared = db.prepare('SELECT * FROM inquiries WHERE id=?').get(inq.id);
  check('送信成功で delivery_failed_at が解除される', cleared.delivery_failed_at === null, String(cleared.delivery_failed_at));
}

// ─── 7. 画面描画 (返信不可バナー / 配信失敗バナー / 送信ボタン無効化) ───
console.log('7. 画面描画');
{
  process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED = 'true';
  const express = (await import('express')).default;
  const routerModule = await import('./router.js');
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const srv = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${srv.address().port}/apps/inquiry-hub`;

  const shopId = db.prepare("SELECT id FROM shops WHERE channel_type='email'").get().id;
  const mk = (extId, identifier, failedAt) => {
    const id = db.prepare(`INSERT INTO inquiries
        (channel_type, shop_id, external_inquiry_id, customer_identifier, subject, received_at, delivery_failed_at)
      VALUES ('email',?,?,?,?,'2026-08-24T00:00:00Z',?)`)
      .run(shopId, extId, identifier, '件名', failedAt || null).lastInsertRowid;
    db.prepare(`INSERT INTO inquiry_messages
        (inquiry_id, external_message_id, sender_type, sender_name, message_body_text, is_incoming, received_at)
      VALUES (?,?, 'customer', '顧客', '本文', 1, '2026-08-24T00:00:00Z')`).run(id, `m-${extId}`);
    return id;
  };
  const idNg = mk('ui-ng', 'no-reply@mercari-shops.com', '2026-08-26T03:20:00Z');
  const idOk = mk('ui-ok', 'customer@gmail.com', null);
  const idEmpty = mk('ui-empty', null, null);

  const htmlNg = await (await fetch(`${base}/inquiries/${idNg}`)).text();
  check('返信不可: 🚫バナーが出る', htmlNg.includes('このアドレスにメール返信はできません'));
  check('返信不可: 返信先の案内が出る', htmlNg.includes('メルカリShops'));
  check('返信不可: 送信ボタンが disabled', /id="replyBtn" disabled/.test(htmlNg));
  check('配信失敗: 🔴バナーが出る', htmlNg.includes('前の返信が届いていません'));

  const htmlOk = await (await fetch(`${base}/inquiries/${idOk}`)).text();
  check('通常アドレス: バナーは出ず送信ボタンは有効',
    !htmlOk.includes('このアドレスにメール返信はできません') && /id="replyBtn"(?! disabled)/.test(htmlOk));
  const htmlEmpty = await (await fetch(`${base}/inquiries/${idEmpty}`)).text();
  check('アドレス空: 塞がない (送信ボタンは有効)',
    !htmlEmpty.includes('このアドレスにメール返信はできません') && /id="replyBtn"(?! disabled)/.test(htmlEmpty));

  const list = await (await fetch(`${base}/?view=inbox`)).text();
  check('一覧に配信失敗バッジが出る', list.includes('配信失敗'));

  await new Promise(r => srv.close(r));
}

console.log(`\n結果: PASS ${passed} / FAIL ${failed}`);
try { db.close(); } catch { /* close済みは無視 */ }
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
