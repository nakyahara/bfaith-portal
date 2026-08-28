#!/usr/bin/env node
/** test-yahoo-review-sender.mjs — PR-Y-C4 Yahoo 送信 (文面・Gmail経路・アダプタ) のスモーク。
 *  実行: node apps/warehouse/test-yahoo-review-sender.mjs */
import Database from 'better-sqlite3';
import {
  buildFollowMail, buildCouponMail, buildCouponMailLowRating, messageIdFor, sampleContext,
  TEMPLATE_BUILDERS, ALLOWED_LINK_HOSTS, SHOP_SIGNATURE, FROM_ADDRESS,
} from './yahoo-review-mail-lib.js';
import { messageIdFor as rakutenMessageIdFor } from './rakuten-review-mail-lib.js';
import {
  buildRawMessage, mimeWord, encodeAddressHeader, createGmailSender, resolveGmailCredentials, assertFromVerified,
  recordFromVerification, ensureFromVerificationLedger, invalidateFromVerification, FROM_VERIFY_TTL_DAYS,
} from './yahoo-mail-send-lib.js';
import { monthlyCouponFor, couponTimeToIso, resolveRecipient, buildMailForAction, createYahooSenderAdapter } from './yahoo-review-sender-adapter.js';
import { createSenderEngine, classifySendError, couponUsableCheck } from './rakuten-review-sender-lib.js';
import { ensureYahooCouponLedger, monthlyCouponPeriod, makeOperationId, reserveMonth, markSubmitting, markIssued } from './yahoo-review-coupon-lib.js';
import { ensureYahooCampaignSources } from './yahoo-review-campaign-adapter.js';
import {
  loadYahooSuppressKey, hmacEmail, addSuppression, releaseSuppression, isSuppressedHash, suppressionStats, extractEmails,
} from './yahoo-review-suppression-lib.js';
const SUPPRESS_KEY = Buffer.alloc(32, 7);

let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };
const GMAIL_ENV = { PO_GMAIL_CLIENT_ID: 'cid', PO_GMAIL_CLIENT_SECRET: 'sec', PO_GMAIL_REFRESH_TOKEN: 'rt' };

console.log('=== 1. 文面 (Yahoo のリンクだけ・宛名なし・署名あり) ===');
{
  const mails = Object.keys(TEMPLATE_BUILDERS).map((t) => TEMPLATE_BUILDERS[t](sampleContext(t)));
  const urls = mails.flatMap((m) => m.text.match(/https?:\/\/[^\s)]+/g) || []);
  check('URL が1つ以上ある', urls.length >= 4);
  check('リンクは Yahoo 系ドメインのみ (楽天の URL が混ざらない)',
    urls.every((u) => { try { const h = new URL(u).hostname; return ALLOWED_LINK_HOSTS.includes(h); } catch { return false; } }),
    urls.filter((u) => { try { return !ALLOWED_LINK_HOSTS.includes(new URL(u).hostname); } catch { return true; } }).join(','));
  check('すべて https', urls.every((u) => u.startsWith('https://')));
  check('宛名は「お客様」固定 (氏名の差し込み変数が残っていない)',
    mails.every((m) => m.text.startsWith('お客様\n') && !/\[%/.test(m.text) && !/様$/m.test(m.subject)));
  check('署名が入っている', mails.every((m) => m.text.includes(SHOP_SIGNATURE)));
  check('配信停止の案内が入っている', mails.every((m) => m.text.includes('配信停止')));
  check('件名は空でなく改行を含まない', mails.every((m) => m.subject.length > 5 && !/[\r\n]/.test(m.subject)));
  const f = buildFollowMail({ orderNumber: 'b-faith01-10288444', shippingIso: '2026-08-18T00:00:00+09:00' });
  check('フォロー: 発送日は JST 表記・注文IDが入る', f.text.includes('2026年8月18日') && f.text.includes('b-faith01-10288444'));
  // JST 15:00Z = 翌日 00:00 JST。UTC 日付で出すと 1 日ずれる
  const f2 = buildFollowMail({ orderNumber: 'x-1', shippingIso: '2026-08-17T15:00:00Z' });
  check('フォロー: JST 境界 (15:00Z=翌日) を取り違えない', f2.text.includes('2026年8月18日'), f2.text.match(/\d+年\d+月\d+日/)?.[0]);
  const c = buildCouponMail({ couponUrl: 'https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', couponEndIso: '2026-10-31T23:00:59+09:00' });
  check('クーポン: URL と期限が本文に出る', c.text.includes('ABCDEF0123456789ABCD') && c.text.includes('2026年10月31日'));
  const low = buildCouponMailLowRating({ couponUrl: 'https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', couponEndIso: '2026-10-31T23:00:59+09:00' });
  check('低評価: 件名は変わるが特典 (5％・URL) は同一 (規約)',
    low.subject !== c.subject && low.text.includes('5％割引') && c.text.includes('5％割引') && low.text.includes('ABCDEF0123456789ABCD'));
  let bad = 0;
  for (const fn of [() => buildFollowMail({ orderNumber: 'x' }), () => buildCouponMail({ couponUrl: 'x' }), () => buildCouponMailLowRating({})]) {
    try { fn(); } catch { bad++; }
  }
  check('必須項目が欠けたら throw (空欄のまま送らない)', bad === 3);
}

console.log('=== 2. Message-ID は楽天と衝突しない ===');
{
  check('Yahoo は yrc- prefix', /^<yrc-/.test(messageIdFor(7, 'abcdef1234567890')));
  check('同じ action id でも楽天と別物', messageIdFor(7, 'abc') !== rakutenMessageIdFor(7, 'abc'));
  check('決定的 (同じ入力なら同じ)', messageIdFor(7, 'abc') === messageIdFor(7, 'abc') && messageIdFor(7, 'abc') !== messageIdFor(8, 'abc'));
}

console.log('=== 3. RFC822 の組み立て (ヘッダインジェクション防止) ===');
{
  const raw = buildRawMessage({ to: 'a@example.com', from: '"雑貨イズム" <info@b-faith.biz>', subject: '【テスト】日本語', text: '本文\n2行目', messageId: '<yrc-1-abc@b-faith.biz>' });
  const decoded = Buffer.from(raw.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
  check('base64url (+ / = を含まない)', !/[+/=]/.test(raw));
  check('必須ヘッダが揃う', ['From:', 'To: a@example.com', 'Subject: ', 'Message-ID: <yrc-1-abc@b-faith.biz>', 'MIME-Version: 1.0'].every((h) => decoded.includes(h)));
  check('非ASCII件名は RFC2047 エンコード', /Subject: =\?UTF-8\?B\?/.test(decoded));
  // 2026-08-28 実機で差出人「雑貨イズム」が文字化けした:
  // 件名だけエンコードして From の表示名は生の日本語を置いていた
  const decodeWords = (x) => x.replace(/=\?UTF-8\?B\?([^?]*)\?=(\r\n )?/g, (_, b) => Buffer.from(b, 'base64').toString('utf8'));
  const head = decoded.split('\r\n\r\n')[0];
  check('From の表示名も RFC2047 エンコードされる (生の日本語を置かない)',
    /^From: =\?UTF-8\?B\?[^?]+\?= <info@b-faith\.biz>$/m.test(head), head.match(/^From: .*$/m)?.[0]);
  check('From を復号すると元の表示名とアドレスに戻る',
    decodeWords(head.match(/^From: (.*)$/m)[1]) === '雑貨イズム <info@b-faith.biz>');
  check('件名を復号すると元に戻る (分割しても壊れない)',
    decodeWords(head.split('Subject: ')[1].split('\r\nMessage-ID')[0]) === '【テスト】日本語');
  {
    // encoded-word は 1 個 75 文字以内。文字数で切ると日本語 20 文字 = 60 バイトで超えるため
    const longSubject = `【雑貨イズム】${'あ'.repeat(60)}`;
    const h2 = Buffer.from(buildRawMessage({ to: 'a@example.com', from: '"雑貨イズム" <info@b-faith.biz>', subject: longSubject, text: 't', messageId: '<m@b>' })
      .replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8').split('\r\n\r\n')[0];
    const words = h2.match(/=\?UTF-8\?B\?[^?]*\?=/g) || [];
    const longest = Math.max(...words.map((w) => w.length));
    check('長い件名でも encoded-word は 75 文字以内', words.length > 1 && longest <= 75, `最長 ${longest}`);
    check('長い件名も復号すれば元どおり', decodeWords(h2.split('Subject: ')[1].split('\r\nMessage-ID')[0]) === longSubject);
  }
  check('ASCII の表示名は引用符つきのまま (エンコードしない)',
    encodeAddressHeader('From', '"Zakka Ism" <info@b-faith.biz>') === '"Zakka Ism" <info@b-faith.biz>');
  check('表示名なしの素のアドレスはそのまま', encodeAddressHeader('From', 'info@b-faith.biz') === 'info@b-faith.biz');
  check('ASCII はエンコードしない (mimeWord)', mimeWord('plain ascii') === 'plain ascii');
  check('本文は base64 で往復する', Buffer.from(decoded.split('\r\n\r\n').slice(1).join('\r\n\r\n').replace(/\r\n/g, ''), 'base64').toString('utf8') === '本文\n2行目');
  check('一括配信ヘッダ (Auto-Submitted / Precedence)', decoded.includes('Auto-Submitted: auto-generated') && decoded.includes('Precedence: bulk'));
  let inj = 0;
  for (const p of [{ to: 'a@example.com\r\nBcc: evil@example.com' }, { subject: 'x\nBcc: evil@example.com' }, { messageId: '<a\r\n@b>' }]) {
    try { buildRawMessage({ to: 'a@example.com', from: 'f@b-faith.biz', subject: 's', text: 't', messageId: '<m@b>', ...p }); } catch (e) { if (e.responseCode === 400) inj++; }
  }
  check('CR/LF 混入は 400 で拒否 (= failed_safe に落ちる)', inj === 3);
  let badTo = false;
  try { buildRawMessage({ to: 'not-an-email', from: 'f@b-faith.biz', subject: 's', text: 't', messageId: '<m@b>' }); } catch (e) { badTo = e.responseCode === 400; }
  check('宛先の形式不正も 400', badTo);
}

console.log('=== 4. Gmail エラーの分類 (未送信確定 / 結果不明) ===');
{
  check('env 解決は INQUIRY_ 優先 → PO_ フォールバック',
    resolveGmailCredentials({ ...GMAIL_ENV, INQUIRY_GMAIL_CLIENT_ID: 'i', INQUIRY_GMAIL_CLIENT_SECRET: 'i', INQUIRY_GMAIL_REFRESH_TOKEN: 'i' }).source === 'INQUIRY_GMAIL_'
    && resolveGmailCredentials(GMAIL_ENV).source === 'PO_GMAIL_');
  let missing = false;
  try { resolveGmailCredentials({}); } catch (e) { missing = /GMAIL_KEY_MISSING/.test(e.message); }
  check('未設定は GMAIL_KEY_MISSING', missing);

  const mkFetch = (plan) => async (url) => {
    if (String(url).includes('oauth2')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
    const p = plan.shift();
    if (p.throw) { const e = new Error('boom'); e.name = p.throw; throw e; }
    return { ok: p.status < 400, status: p.status, json: async () => (p.body ?? { error: { status: p.errStatus || 'X' } }) };
  };
  const send = (plan) => createGmailSender({ env: GMAIL_ENV, fromAddress: FROM_ADDRESS, fetchImpl: mkFetch(plan), minIntervalMs: 0 })
    .sendMail({ to: 'a@example.com', from: 'f@b-faith.biz', subject: 's', text: 't', messageId: '<m@b>' });

  const cases = [
    ['400 (宛先不正など) = 未送信確定 → rejected', [{ status: 400, errStatus: 'INVALID_ARGUMENT' }], 'rejected'],
    ['403 (権限) = 未送信確定 → rejected', [{ status: 403, errStatus: 'PERMISSION_DENIED' }], 'rejected'],
    ['429 (制限) = 送ったか不明 → unknown', [{ status: 429, errStatus: 'RESOURCE_EXHAUSTED' }], 'unknown'],
    ['500 = 送ったか不明 → unknown', [{ status: 500, errStatus: 'INTERNAL' }], 'unknown'],
    ['タイムアウト = 送ったか不明 → unknown', [{ throw: 'TimeoutError' }], 'unknown'],
    ['接続断 = 送ったか不明 → unknown', [{ throw: 'TypeError' }], 'unknown'],
  ];
  for (const [name, plan, want] of cases) {
    // eslint-disable-next-line no-await-in-loop
    const got = await send(plan).then(() => 'sent', (e) => classifySendError(e).kind);
    check(name, got === want, `got=${got}`);
  }
  const okSend = await send([{ status: 200, body: { id: 'msg1', threadId: 'th1' } }]);
  check('成功は gmailMessageId を返す', okSend.gmailMessageId === 'msg1');

  // トークン取得の失敗は「送信要求を出す前」= 未送信確定
  const tokFail = createGmailSender({
    env: GMAIL_ENV, fromAddress: FROM_ADDRESS, minIntervalMs: 0,
    fetchImpl: async () => ({ ok: false, status: 400, json: async () => ({ error: 'invalid_grant' }) }),
  });
  const tokKind = await tokFail.sendMail({ to: 'a@example.com', from: 'f@b', subject: 's', text: 't', messageId: '<m@b>' })
    .then(() => 'sent', (e) => ({ kind: classifySendError(e).kind, pii: /a@example\.com/.test(e.message) }));
  check('トークン失効は rejected (未送信確定) で PII を含まない', tokKind.kind === 'rejected' && tokKind.pii === false);
}

console.log('=== 5. From 置き換わりの検出と LIVE ゲート ===');
{
  const db = new Database(':memory:');
  ensureFromVerificationLedger(db);
  const NOW = '2026-08-28T03:00:00.000Z';
  let gate = null;
  try { assertFromVerified(db, FROM_ADDRESS, NOW); } catch (e) { gate = e.message; }
  check('未検証なら送信させない', /FROM_NOT_VERIFIED/.test(gate || ''));
  recordFromVerification(db, { fromAddress: FROM_ADDRESS, observedFrom: 'd.nakahara@b-faith.biz', nowIso: NOW });
  let mism = null;
  try { assertFromVerified(db, FROM_ADDRESS, NOW); } catch (e) { mism = e.message; }
  check('From が置き換わっていたら送信させない', /FROM_MISMATCH/.test(mism || ''));
  recordFromVerification(db, { fromAddress: FROM_ADDRESS, observedFrom: FROM_ADDRESS, nowIso: NOW });
  check('一致していれば通る', !!assertFromVerified(db, FROM_ADDRESS, NOW));
  let stale = null;
  const later = new Date(Date.parse(NOW) + (FROM_VERIFY_TTL_DAYS + 1) * 86400000).toISOString();
  try { assertFromVerified(db, FROM_ADDRESS, later); } catch (e) { stale = e.message; }
  check(`${FROM_VERIFY_TTL_DAYS}日を過ぎたら再検証を要求`, /FROM_VERIFY_STALE/.test(stale || ''));

  // 読み戻しに失敗して途中で落ちても、古い成功レコードで通してはいけない (Codex Y-C4 R1 Medium)
  invalidateFromVerification(db, FROM_ADDRESS, NOW);
  let incomplete = null;
  try { assertFromVerified(db, FROM_ADDRESS, NOW); } catch (e) { incomplete = e.message; }
  check('検証を開始した時点でゲートは閉じる (途中終了で素通りしない)', /FROM_MISMATCH/.test(incomplete || ''));

  // verifyFrom: 送信 → 送信済みメッセージの From を読み戻す
  const mkSender = (fromHeader) => createGmailSender({
    env: GMAIL_ENV, fromAddress: FROM_ADDRESS, minIntervalMs: 0,
    fetchImpl: async (url) => {
      if (String(url).includes('oauth2')) return { ok: true, json: async () => ({ access_token: 't', expires_in: 3600 }) };
      if (String(url).includes('messages/send')) return { ok: true, status: 200, json: async () => ({ id: 'm1' }) };
      return { ok: true, status: 200, json: async () => ({ payload: { headers: [{ name: 'From', value: fromHeader }] } }) };
    },
  });
  const good = await mkSender('"雑貨イズム" <info@b-faith.biz>').verifyFrom({ to: 'x@b-faith.biz', subject: 's', text: 't', messageId: '<v@b>' });
  check('送信済みの From を読み戻して一致を確認', good.ok === true && good.observedFrom === 'info@b-faith.biz');
  const bad = await mkSender('d.nakahara@b-faith.biz').verifyFrom({ to: 'x@b-faith.biz', subject: 's', text: 't', messageId: '<v@b>' });
  check('置き換わりを検出する', bad.ok === false && bad.observedFrom === 'd.nakahara@b-faith.biz');
  db.close();
}

console.log('=== 6. アダプタ (クーポン正規化・宛先解決・エンジン結線) ===');
{
  check('画面表記 → ISO (JST)', couponTimeToIso('2026/09/01 00:00', '00') === '2026-09-01T00:00:00+09:00'
    && couponTimeToIso('2026/10/31 23:00', '59') === '2026-10-31T23:00:59+09:00' && couponTimeToIso('2026-09-01', '00') === null);
  const db = new Database(':memory:');
  ensureYahooCouponLedger(db);
  const NOW = '2026-09-15T03:00:00.000Z'; // JST 12:00
  const period = monthlyCouponPeriod('2026-09', '2026-08-28T00:00:00.000Z');
  reserveMonth(db, { month: '2026-09', period, operationId: makeOperationId('2026-09', 'TEST01'), nowIso: '2026-08-28T00:00:00.000Z' });
  markSubmitting(db, '2026-09', '2026-08-28T00:00:00.000Z');
  check('未発行のうちはクーポン不可 (メールを送らせない)', couponUsableCheck(monthlyCouponFor(db, '2026-09'), NOW, createYahooSenderAdapter().couponUrlOk) === false);
  markIssued(db, {
    month: '2026-09', couponId: 'ABCDEF0123456789ABCD',
    couponUrl: 'https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', nowIso: '2026-08-28T00:00:00.000Z',
  });
  const A = createYahooSenderAdapter();
  check('発行済み・期間内なら使える', couponUsableCheck(monthlyCouponFor(db, '2026-09'), NOW, A.couponUrlOk) === true);
  check('開始前 (8/31 JST) は配らない', couponUsableCheck(monthlyCouponFor(db, '2026-09'), '2026-08-31T05:00:00.000Z', A.couponUrlOk) === false);
  check('終了後 (11/1 JST) は配らない', couponUsableCheck(monthlyCouponFor(db, '2026-09'), '2026-11-01T00:00:00.000Z', A.couponUrlOk) === false);
  check('楽天のURL判定を継承していない (楽天クーポンは Yahoo で使えない)',
    couponUsableCheck({ status: 'issued', pc_get_url: 'https://coupon.rakuten.co.jp/getCoupon?getkey=x&rt=', coupon_start: '2026-09-01T00:00:00+09:00', coupon_end: '2026-10-31T23:00:59+09:00' }, NOW, A.couponUrlOk) === false);
  // 月初の発行が落ちても、今使える発行済みクーポンがあれば送れる (Codex Y-C5 R1 Medium)。
  // Yahoo のクーポンは月初〜翌月末で 2 か月ぶんが重なるため、当月キーだけを見ると全部止まってしまう
  const OCT = '2026-10-15T03:00:00.000Z'; // JST 10/15 12:00 — 9月分 (10/31まで) が有効
  check('当月分が無くても、期間内の発行済みクーポンにフォールバックする',
    couponUsableCheck(monthlyCouponFor(db, '2026-10', OCT), OCT, A.couponUrlOk) === true);
  check('フォールバックで返るのは実際に有効な 9 月分', monthlyCouponFor(db, '2026-10', OCT)?.month === '2026-09');
  const NOV = '2026-11-15T03:00:00.000Z'; // 9月分も切れている
  check('どれも期間外ならフォールバックしない (期限切れを配らない)',
    couponUsableCheck(monthlyCouponFor(db, '2026-11', NOV), NOV, A.couponUrlOk) === false);
  check('当月分が issued ならそれを優先 (フォールバックしない)', monthlyCouponFor(db, '2026-09', NOW)?.month === '2026-09');
  check('存在しない月は null', monthlyCouponFor(db, '2027-01', '2027-01-15T03:00:00.000Z') === null);
  db.close();

  const mail = buildMailForAction({
    action_type: 'coupon', has_low_active_review: 0,
    monthlyCoupon: { pc_get_url: 'https://shopping.yahoo.co.jp/coupon/interior/ABCDEF0123456789ABCD', coupon_end: '2026-10-31T23:00:59+09:00' },
  });
  check('coupon の文面が組める', mail.text.includes('ABCDEF0123456789ABCD'));
  let noCoupon = false;
  try { buildMailForAction({ action_type: 'coupon', monthlyCoupon: null }); } catch { noCoupon = true; }
  check('クーポンURLが無ければ組み立てない (空欄で送らない)', noCoupon);

  // 宛先解決: 最新の注文状態でもう一度ゲートする
  // VPS /yahoo/orderContact の応答形 (yahoo-order-contact-lib の契約)
  const contact = (over) => async () => ({
    ok: true, status: 200, headers: { get: () => null },
    json: async () => ({ ok: true, contact: { orderId: 'b-faith01-10288444', email: 'buyer@example.com', orderStatus: '5', shipStatus: '3', shipDate: '2026-08-18', socialGiftType: '0', ...over } }),
  });
  const opts = (over) => ({ proxyUrl: 'https://proxy.example', secret: 's', fetchImpl: contact(over) });
  const act = { order_number: 'b-faith01-10288444' };
  check('正常なら実アドレスを返す (DB には書かない)', await resolveRecipient(null, act, SUPPRESS_KEY, opts({})) === 'buyer@example.com');
  for (const [name, over, code] of [
    ['キャンセル済みは送らない', { orderStatus: '4' }, 'order_cancelled'],
    ['未発送は送らない', { shipStatus: '1' }, 'not_shipped'],
    ['ソーシャルギフトは送らない', { socialGiftType: '2' }, 'social_gift'],
    ['メールが無ければ送らない', { email: '' }, 'no_email'],
    ['注文IDの取り違えは送らない', { orderId: 'b-faith01-99999999' }, 'order_id_mismatch'],
  ]) {
    // eslint-disable-next-line no-await-in-loop
    const e = await resolveRecipient(null, act, SUPPRESS_KEY, opts(over)).then(() => null, (x) => x);
    check(name, e?.code === code && e?.retryable === false, `got=${e?.code}`);
  }

  const E = createSenderEngine(A);
  check('エンジンは yahoo テーブルに束縛される', E.mall === 'yahoo' && E.tables.actions === 'yahoo_campaign_actions');
  check('楽天の既定を継承していない (fromHeader/messageIdFor)',
    E.adapter.fromHeader.includes('info@b-faith.biz') && /^<yrc-/.test(E.adapter.messageIdFor(1, 'x')));
  let incomplete = false;
  try { createSenderEngine({ mall: 'yahoo', monthlyCouponFor, resolveRecipient, buildMail: buildMailForAction, fromHeader: 'x' }); } catch (e) { incomplete = /messageIdFor/.test(e.message); }
  check('必須項目 (messageIdFor/couponUrlOk) の指定漏れは throw', incomplete);
}

console.log('=== 7. 配信停止 (送信直前の HMAC 照合) ===');
{
  const KEY = SUPPRESS_KEY;
  let keyOk = true;
  try { loadYahooSuppressKey({}); keyOk = false; } catch { /* 未設定は throw が正 */ }
  try { loadYahooSuppressKey({ YAHOO_SUPPRESS_HMAC_KEY: 'ab' }); keyOk = false; } catch { /* 短すぎも throw */ }
  check('鍵は 32byte hex 必須 (未設定・不正は throw)', keyOk && loadYahooSuppressKey({ YAHOO_SUPPRESS_HMAC_KEY: '7'.repeat(64) }).length === 32);
  check('HMAC は大文字小文字・前後空白を吸収 (同じ人を別人にしない)',
    hmacEmail(' Buyer@Example.COM ', KEY) === hmacEmail('buyer@example.com', KEY));
  check('鍵が違えば別のハッシュ (楽天の鍵では照合できない)', hmacEmail('a@b.com', KEY) !== hmacEmail('a@b.com', Buffer.alloc(32, 8)));

  const db = new Database(':memory:');
  ensureYahooCampaignSources(db);
  const NOW = '2026-08-28T03:00:00.000Z';
  addSuppression(db, { email: 'stop@example.com', reason: '返信で配信停止', key: KEY, nowIso: NOW });
  check('登録すると照合に当たる', isSuppressedHash(db, hmacEmail('stop@example.com', KEY)));
  check('生アドレスは保存していない',
    db.prepare('SELECT COUNT(*) n FROM yahoo_contact_suppressions WHERE email_hash LIKE ?').get('%@%').n === 0);
  addSuppression(db, { email: 'MIXED@Example.com', reason: 'x', key: KEY, nowIso: NOW });
  check('大文字で登録しても小文字で当たる (正規化)', isSuppressedHash(db, hmacEmail('mixed@example.com', KEY)));
  let needBy = false;
  try { releaseSuppression(db, { email: 'stop@example.com', key: KEY }); } catch { needBy = true; }
  check('解除は by 必須・解除後は当たらない',
    needBy && releaseSuppression(db, { email: 'stop@example.com', key: KEY, by: '中原' })
    && !isSuppressedHash(db, hmacEmail('stop@example.com', KEY)));
  addSuppression(db, { email: 'stop@example.com', reason: '再登録', key: KEY, nowIso: NOW });
  check('再登録すると復活する (released_by が消える)', isSuppressedHash(db, hmacEmail('stop@example.com', KEY)));
  const st = suppressionStats(db);
  check('件数を数えられる', st.active === 2 && st.released === 0, JSON.stringify(st));
  // 取込の件数表示が信用できること (ON CONFLICT でも changes>0 になる — Codex Y-C4 R3 Medium)
  const again = addSuppression(db, { email: 'mixed@example.com', reason: 're', key: KEY, nowIso: NOW });
  check('既に停止中の再登録は新規に数えない', again.inserted === false && again.reactivated === false);
  releaseSuppression(db, { email: 'mixed@example.com', key: KEY, by: '中原' });
  const back = addSuppression(db, { email: 'mixed@example.com', reason: 're', key: KEY, nowIso: NOW });
  check('解除済みの再登録は「停止に戻した」として数える', back.inserted === false && back.reactivated === true);
  check('初めてのアドレスだけ新規', addSuppression(db, { email: 'brand@new.com', reason: 'x', key: KEY, nowIso: NOW }).inserted === true);

  // 旧スキーマ (email_hash/reason/created_at のみ) の DB でも動くこと — Codex Y-C4 R3 High
  const legacy = new Database(':memory:');
  legacy.exec(`CREATE TABLE yahoo_contact_suppressions (email_hash TEXT PRIMARY KEY, reason TEXT NOT NULL, created_at TEXT NOT NULL)`);
  legacy.prepare('INSERT INTO yahoo_contact_suppressions VALUES (?,?,?)').run(hmacEmail('old@example.com', KEY), '旧データ', NOW);
  ensureYahooCampaignSources(legacy);
  const cols = new Set(legacy.prepare('PRAGMA table_info(yahoo_contact_suppressions)').all().map((c) => c.name));
  check('旧スキーマに不足列を後付けする (source/evidence/released_by)',
    ['source', 'evidence', 'released_by'].every((c) => cols.has(c)));
  check('旧データは消えず、照合にも当たる', isSuppressedHash(legacy, hmacEmail('old@example.com', KEY)));
  check('後付け後も登録できる (send 中に落ちない)',
    addSuppression(legacy, { email: 'new@example.com', reason: 'x', key: KEY, nowIso: NOW }).inserted === true);
  legacy.close();

  // 本丸: 送信直前に落ちること (VIEW の masked_email_hash は NULL なので planner のゲートでは止まらない)
  const contact2 = (email) => async () => ({
    ok: true, status: 200, headers: { get: () => null },
    json: async () => ({ ok: true, contact: { orderId: 'b-faith01-1', email, orderStatus: '5', shipStatus: '3', shipDate: '2026-08-18', socialGiftType: '0' } }),
  });
  const act2 = { order_number: 'b-faith01-1' };
  const o = (email) => ({ proxyUrl: 'https://proxy.example', secret: 's', fetchImpl: contact2(email) });
  const e1 = await resolveRecipient(db, act2, KEY, o('STOP@example.com')).then(() => null, (x) => x);
  check('配信停止済みには送らない (大文字表記でも)', e1?.code === 'suppressed' && e1?.retryable === false, `got=${e1?.code}`);
  check('停止していない人には送る', await resolveRecipient(db, act2, KEY, o('ok@example.com')) === 'ok@example.com');
  const e2 = await resolveRecipient(db, act2, null, o('ok@example.com')).then(() => null, (x) => x);
  check('鍵が無ければ照合できないので送らない (fail-closed)', e2?.code === 'suppression_key_missing' && e2?.retryable === false);
  db.close();

  const csv = (...lines) => lines.join(String.fromCharCode(13, 10)); // vendor の CSV は CRLF
  check('CSV からアドレスを抽出 (列名に依存しない・重複排除・正規化)',
    JSON.stringify(extractEmails(csv('氏名,メールアドレス,登録日', '山田,A@Example.com,2026-01-01', '田中,b@example.jp,2026-02-02', '重複,a@example.com,x')))
      === JSON.stringify(['a@example.com', 'b@example.jp']));
  check('アドレスが無い CSV は空 (誤検出しない)', extractEmails(csv('氏名,登録日', '山田,2026-01-01')).length === 0);
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exitCode = failed > 0 ? 1 : 0;
