/**
 * Gmail (メールチャネル) 受信同期アダプター (設計書§4、Step 3受信)
 * sync/engine.js のアダプター契約 (fetchNew) を実装する。
 *
 * 前提 (2026-07-18 確定):
 * - 問い合わせメールボックス = info@b-faith.biz。ただし OAuth トークンは d.nakahara@b-faith.biz
 *   (PO_GMAIL_*) で、info@ 宛メールは転送で同じ受信箱に入る運用 → 1トークンで両方見える
 * - env は INQUIRY_GMAIL_CLIENT_ID/SECRET/REFRESH_TOKEN を優先し、無ければ PO_GMAIL_* に
 *   フォールバック (将来 info@ 専用トークンに切り替えるときは INQUIRY_* を設定するだけ)
 * - 可視性の実証は scripts/contract-test-gmail.mjs (read-only) で行う
 *
 * 同期方式:
 * - Gmail の `q=after:<epoch秒>` は「メッセージ受信日時」で絞れる → 古いスレッドへの返信も
 *   新しいメッセージとして窓に入るため、楽天/Yahoo!のような lookback 補正は不要
 * - messages.list (窓内) → threadId を集約 → threads.get (format=full) でスレッド全量を upsert
 * - スパム/ゴミ箱は Gmail の既定で検索対象外。チャットは -in:chats で明示除外
 *
 * メールルール (mail-rules.js):
 * - スレッドの最初の顧客メッセージで evaluateMailRules を評価
 *   skip → スレッドごと取り込まない / import_done → initialInternalStatus='done' で取り込む
 * - 判定は同期プロセス内 (このアダプターの mapThread) で行い、DB へは触らない
 *
 * 送受信の区別: From のドメインが b-faith.biz → shop (自社)、それ以外 → customer
 */
import nodemailer from 'nodemailer';
import { evaluateMailRules } from '../../mail-rules.js';
import { classifyReplyDestination, isBounceSignature, RAKUTEN_MASKED_RE } from '../../no-reply.js';
import { isComposeThread } from '../../compose-id.js';
import { checkRecipientDomain } from '../../mx-check.js';
import { SendRejectedError } from '../../outbox.js';

const GMAIL_BASE = 'https://gmail.googleapis.com/gmail/v1/users/me';
/** 送信時の差出人 (cfg 未指定時の既定)。画面の「送信元」表示でも使う */
export const DEFAULT_FROM_ADDRESS = 'info@b-faith.biz';
export const DEFAULT_FROM_NAME = '雑貨イズム（B-Faith株式会社）';
const TOKEN_URL = 'https://oauth2.googleapis.com/token';
const OWN_DOMAIN = 'b-faith.biz';
const PAGE_SIZE = 100;
const DEFAULT_MAX_LIST_PAGES = 200;   // 200ページ=20,000通。超過は全体失敗 (バックフィル幅を分割)
const BODY_MAX_CHARS = 100000;        // 本文の保存上限 (異常サイズの防御)

/** env から Gmail transport 設定を解決 (INQUIRY_GMAIL_* 優先、PO_GMAIL_* フォールバック)。
 * 資格情報はセット単位で選ぶ (一部だけ INQUIRY_* があるときに PO_* と混成しない。Codexレビュー反映) */
export function resolveGmailTransportFromEnv(env = process.env) {
  for (const prefix of ['INQUIRY_GMAIL_', 'PO_GMAIL_']) {
    const clientId = env[`${prefix}CLIENT_ID`], clientSecret = env[`${prefix}CLIENT_SECRET`], refreshToken = env[`${prefix}REFRESH_TOKEN`];
    if (clientId && clientSecret && refreshToken) return { clientId, clientSecret, refreshToken };
  }
  return null;
}

/** "表示名 <a@b>" → { name, mailbox } (mailboxは小文字。パース不能は mailbox=null) */
export function parseFromHeader(v) {
  const s = String(v || '').trim();
  const m = s.match(/^(.*?)\s*<([^<>]+)>\s*$/);
  const rawBox = (m ? m[2] : s).trim().toLowerCase();
  const mailbox = /^[^\s@]+@[^\s@]+$/.test(rawBox) ? rawBox : null;
  let name = m ? m[1].trim().replace(/^"(.*)"$/, '$1').trim() : '';
  return { name: name || null, mailbox };
}

const headerOf = (payload, name) => {
  const h = (payload?.headers || []).find(x => String(x.name).toLowerCase() === name.toLowerCase());
  return h ? String(h.value) : '';
};

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** 楽天あんしんメルアドのマスクアドレス (判定は no-reply.js。ここは従来の import 元との互換で再export)。
 * 2026-08-24 実測: Gmail API経由の送信は send-as エイリアス (From=info@) があっても
 * SMTPエンベロープに認証アカウント (d.nakahara@) が出るため、楽天中継が
 * `552 5.2.0 <d.nakahara@…> sender rejected` で拒否する (RMS登録アドレス以外は不可)。
 * → この宛先だけは楽天提供の公式SMTP (sub.fw.rakuten.ne.jp:587) 経由で送る (sendReply 参照)。
 *   メールディーラー・メールワイズ・Re:lation 等の他ツールと同じ方式 */
export { RAKUTEN_MASKED_RE };

/** 楽天あんしんメルアド公式SMTPの接続設定を env から解決。
 * SMTP AUTH ID/パスワードは R-Login「あんしんメルアドサービスメニュー」→「メールサーバ設定情報」で確認。
 * 未設定なら null (マスクアドレス宛の送信は明確な案内付きで拒否される) */
export function resolveRakutenMaskSmtpFromEnv(env = process.env) {
  const user = env.INQUIRY_RAKUTEN_SMTP_USER, pass = env.INQUIRY_RAKUTEN_SMTP_PASS;
  if (!user || !pass) return null;
  return {
    host: env.INQUIRY_RAKUTEN_SMTP_HOST || 'sub.fw.rakuten.ne.jp',
    port: Number(env.INQUIRY_RAKUTEN_SMTP_PORT) || 587,
    user, pass,
  };
}

/**
 * From ヘッダから実効差出人を解決する (受信同期の顧客判定と送信時の宛先フォールバックで共用)。
 * 厳格DMARCの送信元 (Amazon等) は Googleグループが From を
 * 「'元差出人' via グループ名 <info@b-faith.biz>」に書き換える (2026-07-18実測)。
 * そのまま判定すると自社ドメイン=店舗側メッセージと誤判定するため、
 * グループが残す X-Original-Sender (無ければ Reply-To) から元の差出人を復元する
 */
export function effectiveFromOf(payload) {
  let from = parseFromHeader(headerOf(payload, 'From'));
  const looksRewritten = from.mailbox && from.mailbox.endsWith('@' + OWN_DOMAIN)
    && (headerOf(payload, 'X-Original-Sender') || / via /i.test(headerOf(payload, 'From')));
  if (looksRewritten) {
    const orig = parseFromHeader(headerOf(payload, 'X-Original-Sender'));
    const fallback = parseFromHeader(headerOf(payload, 'Reply-To'));
    const eff = orig.mailbox ? orig : fallback;
    if (eff.mailbox) {
      // 表示名は「'元差出人' via グループ名」の via 以降を落とす
      const cleanName = (from.name || '').replace(/\s*via\s.+$/i, '').replace(/^'(.*)'$/, '$1').trim();
      from = { name: eff.name || cleanName || null, mailbox: eff.mailbox };
    }
  }
  return from;
}

const b64urlToBuf = s => Buffer.from(String(s || '').replace(/-/g, '+').replace(/_/g, '/'), 'base64');

/** ざっくりHTML→テキスト (text/plain が無いメール用。表示・ルール評価に十分な精度でよい) */
export function htmlToText(html) {
  return String(html || '')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n').replace(/<\/(p|div|tr|li|h[1-6])>/gi, '\n')
    .replace(/<[^>]+>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
}

/** 保存前の本文正規化: 行末空白の除去 + 3行以上の連続空行を1行に詰める。
 * 自動配信メール (Amazon等) は空行を大量に含み、そのまま保存すると表示が延々と間延びする */
export function normalizeMailBody(text) {
  return String(text || '')
    .replace(/\r\n?/g, '\n')
    .replace(/[ \t　]+$/gm, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/** payload を再帰して text/plain・text/html・添付を収集 */
export function walkPayload(payload, out = { text: '', html: '', attachments: [] }) {
  if (!payload) return out;
  const mime = String(payload.mimeType || '');
  const filename = String(payload.filename || '');
  if (filename) {
    out.attachments.push({
      // Gmail の attachmentId は取得ごとに変わり得る (安定しない) ため外部IDにしない。
      // 代わりに partId (MIME構造上の位置。再取得しても安定) を使う。
      // これで同名・同サイズの添付が同じメールに複数あっても1件に潰れない (Codexレビュー Medium-3)。
      // partId が無い旧レスポンス形は undefined → エンジンの決定的 synthetic 採番に委ねる
      externalAttachmentId: payload.partId ? `part:${payload.partId}` : undefined,
      partId: payload.partId ?? null,
      fileName: filename,
      contentType: mime || null,
      fileSize: payload.body?.size ?? null,
    });
  } else if (mime === 'text/plain' && payload.body?.data && !out.text) {
    out.text = b64urlToBuf(payload.body.data).toString('utf8');
  } else if (mime === 'text/html' && payload.body?.data && !out.html) {
    out.html = b64urlToBuf(payload.body.data).toString('utf8');
  }
  for (const p of payload.parts || []) walkPayload(p, out);
  return out;
}

/** Gmailメッセージ1通がバウンス (配信失敗通知) かどうか。判定ロジックの本体は no-reply.js */
export function isBounceMessage(payload) {
  return isBounceSignature({
    from: parseFromHeader(headerOf(payload, 'From')).mailbox || headerOf(payload, 'From'),
    contentType: headerOf(payload, 'Content-Type'),
    subject: headerOf(payload, 'Subject'),
    hasFailedRecipients: !!headerOf(payload, 'X-Failed-Recipients'),
  });
}

/** Gmailスレッド1件 → エンジン契約 inquiry (メールルール適用込み)。skip判定なら null (エクスポートはテスト用) */
export function mapThread(thread, { ruleEvaluator = evaluateMailRules } = {}) {
  const msgs = thread?.messages;
  if (!Array.isArray(msgs) || msgs.length === 0) {
    const e = new Error(`Gmailスレッド契約違反: messages がありません (${thread?.id})`);
    e.errorType = 'contract_violation';
    throw e;
  }
  const mapped = msgs.map(m => {
    const payload = m.payload || {};
    // DMARC書き換え復元込みの実効差出人 (effectiveFromOf 参照)
    const from = effectiveFromOf(payload);
    const fromDomain = from.mailbox ? from.mailbox.slice(from.mailbox.indexOf('@') + 1) : '';
    const isShop = fromDomain === OWN_DOMAIN;
    // バウンス (配信失敗通知)。返信が顧客に届かなかったことを示す唯一の手掛かりなので、
    // 顧客メッセージと混ぜずに system として記録し、チケットに「配信失敗」の印を付ける
    // (2026-08-26 no-reply@mercari-shops.com 宛の返信が黙って消えた事故。no-reply.js 参照)
    const isBounce = isBounceMessage(payload);
    const { text, html, attachments } = walkPayload(payload);
    const bodyText = normalizeMailBody(text || htmlToText(html)).slice(0, BODY_MAX_CHARS);
    const atMs = Number(m.internalDate) || null;
    if (!m.id || !atMs) {
      const e = new Error(`Gmailメッセージ契約違反: id/internalDate がありません (thread ${thread.id})`);
      e.errorType = 'contract_violation';
      throw e;
    }
    return {
      raw: { from, subject: headerOf(payload, 'Subject'), to: headerOf(payload, 'To'), replyTo: headerOf(payload, 'Reply-To') },
      isBounce,
      msg: {
        externalMessageId: m.id,
        // バウンスは顧客の発言ではないので sender_type='system'。ただし is_incoming は 1 のまま
        // にして未読化・「新着」への復帰を効かせる (気付かないまま埋もれるのが一番まずい)
        senderType: isBounce ? 'system' : (isShop ? 'shop' : 'customer'),
        senderName: from.name ?? from.mailbox ?? null,
        bodyText: bodyText || null,
        isIncoming: isShop && !isBounce ? 0 : 1,
        sentAt: atMs, receivedAt: atMs,
        attachments,
      },
    };
  });
  mapped.sort((a, b) => a.msg.sentAt - b.msg.sentAt);

  // 顧客の同一性・メールルール評価には「バウンス以外の」最初の受信メッセージを使う。
  // バウンスを拾うと customer_identifier が mailer-daemon@… になり、以後の返信が
  // 配信通知サーバー宛に飛ぶ (2026-08-26)
  // バウンスしか受信が無いスレッド (店舗発 → 配信失敗) では mapped[0] に落とす。
  // customerIdentifier は「受信メッセージのとき」だけ入るので、結果として NULL になり
  // mailer-daemon@… が顧客として登録されない
  const firstIncoming = mapped.find(x => x.msg.isIncoming && !x.isBounce) || mapped[0];
  // 配信失敗が観測された最新時刻 (engine が inquiries.delivery_failed_at に反映する)
  const bounceAts = mapped.filter(x => x.isBounce).map(x => x.msg.sentAt).filter(Boolean);
  const deliveryFailedAt = bounceAts.length ? Math.max(...bounceAts) : null;
  const rule = ruleEvaluator({
    from: firstIncoming.raw.from.mailbox || '',
    to: firstIncoming.raw.to,
    reply_to: firstIncoming.raw.replyTo,
    subject: firstIncoming.raw.subject,
    body: firstIncoming.msg.bodyText || '',
  });
  if (rule?.action === 'skip') return null;

  return {
    externalInquiryId: thread.id,
    customerName: firstIncoming.msg.isIncoming ? (firstIncoming.raw.from.name || null) : null,
    customerIdentifier: firstIncoming.msg.isIncoming ? firstIncoming.raw.from.mailbox : null,
    subject: firstIncoming.raw.subject || null,
    orderNumber: null, productCode: null, productName: null,
    externalStatus: null, externalIsRead: null,
    ...(deliveryFailedAt ? { deliveryFailedAt } : {}),
    ...(rule?.action === 'import_done' ? { initialInternalStatus: 'done' } : {}),
    // メールルールのフォルダ振り分け (2026-08-17)。新規作成時のみ適用 (既存チケットのフォルダは動かさない)
    ...(rule?.folderId ? { initialFolderId: rule.folderId } : {}),
    // メールルールのラベル付与 (2026-08-24)。同じく新規作成時のみ
    ...(rule?.labelId ? { initialLabelId: rule.labelId } : {}),
    receivedAt: mapped[0].msg.sentAt,
    messages: mapped.map(x => x.msg),
  };
}

/**
 * @param {object} cfg
 *   clientId, clientSecret, refreshToken: OAuth (必須。resolveGmailTransportFromEnv 参照)
 *   requestTimeoutMs?: 既定30秒
 *   fetchImpl?: fetch 差し替え (テスト用)
 *   sleepMs?: リクエスト間隔 (既定120ms。Gmailのquotaは十分広い)
 *   maxListPages?: 一覧ページ上限 (既定200)
 *   concurrency?: スレッド取得の並列数 (既定5。quota 250units/s に対し余裕圏内)
 *   ruleEvaluator?: メールルール評価の差し替え (テスト用)
 *   sendMode?: 'dryrun' (既定) | 'live' — 返信送信 (sendReply)。dryrunは組み立てとログのみで実送信しない
 *   fromAddress?: 送信Fromアドレス (既定 info@b-faith.biz。d.nakahara側でinfo@のsend-asエイリアスが
 *                 未設定だとGmailが認証ユーザーのアドレスに置き換える → live送信のたびに読み戻しで
 *                 実Fromを検証し、置き換わっていたら warning を返す。sendReply 参照)
 *   fromName?: Fromの表示名 (既定 '雑貨イズム（B-Faith株式会社）' — メールディーラーのFrom表示名と同一)
 *   rakutenSmtp?: 楽天あんしんメルアド公式SMTP設定 (resolveRakutenMaskSmtpFromEnv)。
 *                 マスクアドレス (…@fw.rakuten.ne.jp) 宛はこの経路で送る。未設定なら明確な案内付きで拒否
 *   smtpTransportFactory?: SMTPトランスポートの差し替え (テスト用。既定=nodemailer)
 */
export function createGmailAdapter(cfg = {}) {
  const { clientId, clientSecret, refreshToken } = cfg;
  if (!clientId || !clientSecret || !refreshToken) {
    throw new Error('createGmailAdapter: clientId / clientSecret / refreshToken は必須です (env INQUIRY_GMAIL_* または PO_GMAIL_*)');
  }
  const fetchImpl = cfg.fetchImpl ?? fetch;
  const sleepMs = cfg.sleepMs ?? 120;
  const requestTimeoutMs = cfg.requestTimeoutMs ?? 30000;
  const maxListPages = cfg.maxListPages ?? DEFAULT_MAX_LIST_PAGES;
  const concurrency = cfg.concurrency ?? 5;
  const ruleEvaluator = cfg.ruleEvaluator ?? evaluateMailRules;
  const sendMode = cfg.sendMode ?? 'dryrun';
  const fromAddress = cfg.fromAddress ?? DEFAULT_FROM_ADDRESS;
  const fromName = cfg.fromName ?? DEFAULT_FROM_NAME;
  // 楽天あんしんメルアド公式SMTP (resolveRakutenMaskSmtpFromEnv)。マスクアドレス宛の送信に必須
  const rakutenSmtp = cfg.rakutenSmtp ?? null;
  // テスト差し替え用。既定は nodemailer (STARTTLS必須・タイムアウトはGmailと同じ設定に揃える)
  const smtpTransportFactory = cfg.smtpTransportFactory ?? (s => nodemailer.createTransport({
    host: s.host, port: s.port, secure: false, requireTLS: true,
    auth: { user: s.user, pass: s.pass },
    connectionTimeout: requestTimeoutMs, greetingTimeout: requestTimeoutMs, socketTimeout: requestTimeoutMs,
  }));
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  // live送信後にSentメッセージを読み戻して実測したFrom (送信元の置き換わり検出)。
  // fromAddress の send-as エイリアスが未設定だと Gmail は From を認証ユーザーの
  // アドレスに黙って置き換える (通常宛先には届くが表示上の事故になる)。
  // null=未観測 / 一致していれば fromAddress / 置き換わっていれば実測アドレス
  let observedActualFrom = null;

  let tokenCache = { token: null, exp: 0 };
  async function accessToken() {
    if (tokenCache.token && Date.now() < tokenCache.exp) return tokenCache.token;
    let res;
    try {
      res = await fetchImpl(TOKEN_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type: 'refresh_token' }).toString(),
        signal: AbortSignal.timeout(requestTimeoutMs),
      });
    } catch (err) {
      const e = new Error(`Gmailトークン取得の接続失敗: ${err?.message || err}`);
      e.errorType = 'fetch_failed';
      throw e;
    }
    const j = await res.json().catch(() => ({}));
    if (!res.ok || !j.access_token) {
      const e = new Error(`Gmailトークン取得失敗 (HTTP ${res.status}): ${j.error_description || j.error || '不明'}`);
      e.errorType = 'auth';
      throw e;
    }
    tokenCache = { token: j.access_token, exp: Date.now() + Math.max(0, (j.expires_in || 3600) - 60) * 1000 };
    return tokenCache.token;
  }

  // 全ワーカー共有のスロット式レートリミッター (最小間隔 sleepMs)。
  // 並列ワーカーごとの個別sleepだと concurrency×レートでquota超過し得る (Codexレビュー反映):
  // 既定 120ms間隔 ≈ 8.3req/s ≈ 83units/s (threads.get=10units) で上限250units/sに対し余裕
  let nextSlotAt = 0;
  async function throttle() {
    const now = Date.now();
    const slot = Math.max(now, nextSlotAt);
    nextSlotAt = slot + sleepMs;
    if (slot > now) await sleep(slot - now);
  }
  let requests = 0;
  async function apiGet(pathAndQuery, label) {
    if (requests > 0) await throttle();
    requests++;
    const token = await accessToken();
    let res;
    try {
      res = await fetchImpl(`${GMAIL_BASE}/${pathAndQuery}`, {
        headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(requestTimeoutMs),
      });
    } catch (err) {
      const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
      const e = new Error(timedOut ? `Gmail API タイムアウト (${requestTimeoutMs}ms, ${label})` : `Gmail API 接続失敗 (${label}): ${err?.message || err}`);
      e.errorType = 'fetch_failed';
      throw e;
    }
    const j = await res.json().catch(() => null);
    if (!res.ok) {
      // エラー本文は message のみ (本文・ヘッダの露出防止)
      const e = new Error(`Gmail API HTTP ${res.status} (${label}): ${j?.error?.message || '不明'}`);
      e.errorType = res.status === 401 || res.status === 403 ? 'auth'
        : res.status === 429 ? 'rate_limited' : 'fetch_failed';
      throw e;
    }
    if (j === null) {
      const e = new Error(`Gmail APIレスポンスがJSONでありません (${label})`);
      e.errorType = 'contract_violation';
      throw e;
    }
    return j;
  }

  // ─── 送信 (outbox.js の sendReply 契約) ───

  /** 非ASCIIヘッダの RFC2047 (UTF-8/Base64) エンコード */
  const mimeWord = s => /^[\x20-\x7e]*$/.test(s) ? s
    : s.match(/.{1,20}/gs).map(w => `=?UTF-8?B?${Buffer.from(w, 'utf8').toString('base64')}?=`).join('\r\n ');
  /** ヘッダ値のCR/LF/NUL拒否 (ヘッダインジェクション対策) */
  const headerSafe = (name, v) => {
    if (/[\r\n\0]/.test(String(v || ''))) throw new SendRejectedError(`${name} に改行を含めることはできません`);
    return String(v || '');
  };

  async function apiPost(pathAndQuery, body, label) {
    await throttle();
    requests++;
    const token = await accessToken();
    let res;
    try {
      res = await fetchImpl(`${GMAIL_BASE}/${pathAndQuery}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(requestTimeoutMs),
      });
    } catch (err) {
      // 送信要求を発した後の失敗 = 結果不明 → 汎用エラー (outboxがunknown化。自動再送しない)
      const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
      throw new Error(timedOut ? `Gmail送信タイムアウト (${requestTimeoutMs}ms, ${label}) — 結果不明` : `Gmail送信の接続失敗 (${label}): ${err?.message || err}`);
    }
    const j = await res.json().catch(() => null);
    if (!res.ok) {
      const msg = `Gmail API HTTP ${res.status} (${label}): ${j?.error?.message || '不明'}`;
      // 4xx = リクエストが受理されなかった (未送信確定) → SendRejectedError。5xx = 結果不明
      if (res.status >= 400 && res.status < 500) throw new SendRejectedError(msg);
      throw new Error(msg);
    }
    return j;
  }

  return {
    channelType: 'email',
    isLive: sendMode === 'live',

    /**
     * 返信送信 (outbox.js §8.3 契約)。未送信確定の失敗のみ SendRejectedError を throw。
     * 送信要求後の失敗は汎用エラー (→ unknown、自動再送しない)。
     * スレッド返信: Gmailスレッドの最後のメッセージから Message-ID を取り、In-Reply-To/References を付ける。
     * 新規メール (compose.js。external_inquiry_id が 'compose:…') はスレッドが存在しないので
     * スレッドメタの取得・Re: の付与・In-Reply-To を行わず、宛先も画面で入力したものだけを使う。
     * 送信できたら実際のGmailスレッドIDを externalThreadId として返す (outbox.js が仮IDと差し替える)。
     * 引数 dryRun: true はアダプターのモード設定に関係なく実送信を禁止する
     * (プレビュー経路がliveアダプターを受け取っても安全なように、呼び出し単位で強制。Codexレビュー反映)
     */
    async sendReply({ inquiry, bodyText, attachments = [], dryRun = false }) {
      requests = 0;
      // 宛先はドメインだけ小文字化する (RFC上ローカル部は大小を区別し得るため原文のまま送る。
      // 受信同期由来の customer_identifier は元から小文字なので従来の返信の挙動は変わらない)
      const toRaw = String(inquiry?.customer_identifier || '').trim();
      const toAt = toRaw.lastIndexOf('@');
      let to = toAt > 0 ? `${toRaw.slice(0, toAt)}@${toRaw.slice(toAt + 1).toLowerCase()}` : toRaw.toLowerCase();
      const threadId = String(inquiry?.external_inquiry_id || '');
      if (!threadId) throw new SendRejectedError('Gmailスレッド外部IDがありません');
      // このアプリから出す新規メール (返信ではない)。Gmailスレッドはまだ存在しない
      const isNewMail = isComposeThread(threadId);
      const subjectBase = headerSafe('件名', String(inquiry?.subject || (isNewMail ? '' : 'お問い合わせについて')).trim()
        || (isNewMail ? '' : 'お問い合わせについて'));
      if (isNewMail && !subjectBase) throw new SendRejectedError('件名がありません (未送信)');
      const subject = isNewMail || /^re:/i.test(subjectBase) ? subjectBase : `Re: ${subjectBase}`;

      // スレッド返信ヘッダの材料 + 宛先フォールバック (この時点の失敗 = 未送信確定)
      let inReplyTo = '', references = '';
      // 新規メールの宛先は画面で入力されたものが全て (遡れるスレッドが無いのでフォールバックしない)
      if (isNewMail && !EMAIL_RE.test(to)) {
        throw new SendRejectedError(`新規メールの宛先が不正です (customer_identifier='${inquiry?.customer_identifier || ''}'。未送信)`);
      }
      if (!isNewMail) try {
        const t = await apiGet(`threads/${encodeURIComponent(threadId)}?format=metadata`
          + '&metadataHeaders=Message-ID&metadataHeaders=References'
          + '&metadataHeaders=From&metadataHeaders=Reply-To&metadataHeaders=X-Original-Sender', 'send-thread-meta');
        const msgs = t.messages || [];
        const last = msgs[msgs.length - 1];
        const msgId = headerOf(last?.payload, 'Message-ID');
        const prevRefs = headerOf(last?.payload, 'References');
        if (msgId) {
          inReplyTo = headerSafe('In-Reply-To', msgId);
          references = headerSafe('References', prevRefs ? `${prevRefs} ${msgId}`.slice(-2000) : msgId);
        }
        // 宛先フォールバック (2026-08-15 実測): customer_identifier が空の問い合わせがあり
        // 送信できなかった — 店舗発のみで始まったスレッドや From 解析不能ケースで空になる。
        // スレッド内の最後の顧客メッセージ (自社ドメイン以外の実効差出人) から宛先を復元する
        // 2026-08-26: 配信失敗通知 (mailer-daemon@…) や no-reply アドレスは候補から外す。
        // バウンスはスレッドの最後に付くため、素直に遡ると配信通知サーバー宛に返信してしまう
        if (!EMAIL_RE.test(to)) {
          for (let i = msgs.length - 1; i >= 0; i--) {
            const eff = effectiveFromOf(msgs[i].payload || {});
            if (eff.mailbox && EMAIL_RE.test(eff.mailbox) && !eff.mailbox.endsWith('@' + OWN_DOMAIN)
              && classifyReplyDestination(eff.mailbox).sendable) {
              to = eff.mailbox;
              console.log(`[gmail-send] 宛先をスレッドから復元: ${to} (customer_identifier が空。thread=${threadId.slice(0, 12)}…)`);
              break;
            }
          }
        }
      } catch (e) {
        throw new SendRejectedError(`返信ヘッダの取得に失敗 (未送信): ${String(e?.message || e).slice(0, 200)}`);
      }
      if (!EMAIL_RE.test(to)) {
        throw new SendRejectedError(`宛先メールアドレスを特定できません (customer_identifier='${inquiry?.customer_identifier || ''}'。`
          + 'スレッド内にも顧客の差出人が見つかりません — 顧客への送信手段がないメール (自社発の通知等) の可能性があります)');
      }
      // 宛先ドメインの事前確認 (2026-08-27)。画面のジョブ作成時にも見ているが、
      // スレッドから宛先を復元した場合はここが初めての確認になる。
      // 「確実に受け取れない」ときだけ未送信で止める (DNS不通なら止めない。mx-check.js)
      const domainCheck = await checkRecipientDomain(to);
      if (!domainCheck.ok) throw new SendRejectedError(`${domainCheck.reason} (未送信)`);

      // ─── 返信不可アドレス宛は未送信で止める (2026-08-26 本番事故。no-reply.js 参照) ───
      // no-reply@mercari-shops.com 宛の返信が Gmail のリトライ後にバウンスし、画面上は
      // 「送信済み」なのに顧客には何も届いていなかった。送る前に止めて返信先を案内する
      const destination = classifyReplyDestination(to);
      if (!destination.sendable) {
        throw new SendRejectedError(`${destination.reason} (未送信)。${destination.guide}`);
      }
      headerSafe('宛先', to);

      // ─── 楽天マスクアドレス宛: 楽天公式SMTP経由で送る (2026-08-24 実測+Web調査) ───
      // Gmail API経由はSMTPエンベロープが認証アカウント (d.nakahara@) になり、楽天中継の
      // 「RMS登録アドレス以外は拒否」に当たって 552 sender rejected でバウンスする。
      // 楽天が店舗向けに提供する sub.fw.rakuten.ne.jp:587 (SMTP AUTH) なら
      // From・エンベロープともRMS登録済みの info@ を名乗れる (メールディーラー等と同方式)
      if (RAKUTEN_MASKED_RE.test(to)) {
        if (!rakutenSmtp) {
          // SMTP未設定でGmail経由に流すと100%バウンス+顧客に届かないため、未送信で止めて直し方を出す
          throw new SendRejectedError('楽天マスクアドレス宛はGmail経由では楽天側に拒否されます (未送信)。'
            + 'Render envに INQUIRY_RAKUTEN_SMTP_USER / INQUIRY_RAKUTEN_SMTP_PASS を設定してください'
            + ' (R-Login「あんしんメルアドサービスメニュー」→「メールサーバ設定情報」のSMTP AUTH ID/パスワード)');
        }
        if (dryRun || sendMode !== 'live') {
          console.log(`[gmail-send DRYRUN] 楽天SMTP経路 To=${to} Subject=${subject.slice(0, 40)} atts=${attachments.length} (実送信していません)`);
          return { dryRun: true, externalReplyId: `dryrun:${threadId}` };
        }
        const transport = smtpTransportFactory(rakutenSmtp);
        let info;
        try {
          info = await transport.sendMail({
            from: { name: fromName, address: fromAddress },
            to,
            subject,
            text: String(bodyText || ''),
            // エンベロープ (MAIL FROM) を明示的に info@ にする — 楽天の許可判定はここを見る
            envelope: { from: fromAddress, to: [to] },
            ...(inReplyTo ? { inReplyTo } : {}),
            ...(references ? { references: references.split(/\s+/).filter(Boolean) } : {}),
            attachments: attachments.map(a => ({
              filename: String(a.fileName || 'attachment').replace(/[\r\n\0]/g, '_').slice(0, 150),
              content: a.buffer,
              ...(a.contentType ? { contentType: a.contentType } : {}),
            })),
          });
        } catch (err) {
          const code = Number(err?.responseCode);
          const msg = `楽天SMTP送信失敗 (${code || err?.code || '接続エラー'}): ${String(err?.message || err).slice(0, 200)}`;
          // SMTPの4xx/5xx応答 = サーバーが受け付けなかった (未送信確定)。接続断・タイムアウト等は結果不明
          if (code >= 400) throw new SendRejectedError(msg);
          throw new Error(`${msg} — 結果不明`);
        } finally {
          try { transport.close?.(); } catch { /* 接続の後始末のみ */ }
        }
        const extId = String(info?.messageId || '').trim();
        console.log(`[gmail-send] 楽天SMTP送信完了 To=${to} messageId=${extId || '(不明)'}`);
        // この経路は Gmail を通らないので新規メールでもスレッドIDは分からない
        // (仮ID 'compose:…' のまま残る。画面表示・以後の返信送信はそれで成立する)
        return extId ? { externalReplyId: extId } : {};
      }

      const commonHeaders = [
        `From: ${mimeWord(fromName)} <${headerSafe('From', fromAddress)}>`,
        `To: ${to}`,
        `Subject: ${mimeWord(subject)}`,
        inReplyTo ? `In-Reply-To: ${inReplyTo}` : null,
        references ? `References: ${references}` : null,
        'MIME-Version: 1.0',
      ].filter(Boolean);
      const b64wrap = buf => buf.toString('base64').replace(/(.{76})/g, '$1\r\n');
      const bodyB64 = b64wrap(Buffer.from(String(bodyText || ''), 'utf8'));
      let mime;
      if (attachments.length) {
        // 送信用添付 (2026-08-20 スタッフ要望): multipart/mixed で本文+添付を組み立てる。
        // 実体・形式・サイズの検証は reply-attachments.js 済み。ここでは合計サイズだけ防衛的に再確認
        const total = attachments.reduce((s, a) => s + (a.buffer?.length || 0), 0);
        if (total > 10 * 1024 * 1024) throw new SendRejectedError(`添付の合計が大きすぎます (${Math.round(total / 1048576)}MB。未送信)`);
        // boundary の '=_' は base64 本文 (英数+/=のみ、= は末尾) に現れない並び
        const boundary = `=_bfaith_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
        const attPart = (a) => {
          const rawName = String(a.fileName || 'attachment').replace(/[\r\n\0"]/g, '_').slice(0, 150);
          const ascii = rawName.replace(/[^\x20-\x7e]/g, '_');
          const pct = encodeURIComponent(rawName).replace(/['()*]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
          return `--${boundary}\r\n`
            + `Content-Type: ${headerSafe('添付Content-Type', a.contentType || 'application/octet-stream')}; name="${ascii}"\r\n`
            + `Content-Disposition: attachment; filename="${ascii}"; filename*=UTF-8''${pct}\r\n`
            + 'Content-Transfer-Encoding: base64\r\n\r\n' + b64wrap(a.buffer);
        };
        mime = [...commonHeaders, `Content-Type: multipart/mixed; boundary="${boundary}"`].join('\r\n')
          + '\r\n\r\n'
          + `--${boundary}\r\nContent-Type: text/plain; charset="UTF-8"\r\nContent-Transfer-Encoding: base64\r\n\r\n${bodyB64}\r\n`
          + attachments.map(attPart).join('\r\n') + `\r\n--${boundary}--`;
      } else {
        mime = [...commonHeaders, 'Content-Type: text/plain; charset="UTF-8"', 'Content-Transfer-Encoding: base64'].join('\r\n')
          + '\r\n\r\n' + bodyB64;
      }
      const raw = Buffer.from(mime).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

      if (dryRun || sendMode !== 'live') {
        // dryRun: true を返す = outbox はジョブを消費しない (sent確定させると未送信メールが
        // 送信済み扱いになり顧客返信が欠落する。Codexレビュー反映)。実送信は一切しない
        console.log(`[gmail-send DRYRUN] To=${to} Subject=${subject.slice(0, 40)} thread=${threadId.slice(0, 12)}… bytes=${mime.length} atts=${attachments.length} (実送信していません)`);
        return { dryRun: true, externalReplyId: `dryrun:${threadId}` };
      }
      // 新規メールは threadId を渡さない (Gmail が新しいスレッドを起こす)。返信は既存スレッドに載せる
      const sent = await apiPost('messages/send', isNewMail ? { raw } : { raw, threadId }, 'send');
      if (!sent?.id) throw new Error('Gmail送信レスポンスに id がありません — 結果不明');
      console.log(`[gmail-send] ${isNewMail ? '新規メール' : '返信'}送信完了 To=${to} messageId=${sent.id}${isNewMail ? ` thread=${sent.threadId || '(不明)'}` : ''}`);

      // From実測検証 (2026-08-24 バウンス実測の再発防止): 送ったメッセージを読み戻し、
      // Gmail が From を置き換えていないか確認する。置き換わっていたら warning を返して
      // ジョブ履歴に表示 (送信自体は完了しているので throw はしない。検証失敗も fail-soft)
      let warning = null;
      try {
        const m = await apiGet(`messages/${encodeURIComponent(sent.id)}?format=metadata&metadataHeaders=From`, 'send-verify-from');
        const actual = parseFromHeader(headerOf(m?.payload, 'From')).mailbox;
        if (actual) {
          observedActualFrom = actual;
          if (actual !== fromAddress.toLowerCase()) {
            warning = `⚠️実際の送信元が ${actual} でした (設定は ${fromAddress})。`
              + `Gmailの「他のメールアドレスからメールを送信」に ${fromAddress} を追加してください`;
            console.error(`[gmail-send] ${warning} (messageId=${sent.id})`);
          }
        }
      } catch (e) {
        console.warn(`[gmail-send] From実測検証をスキップ (送信は完了): ${String(e?.message || e).slice(0, 120)}`);
      }
      // 新規メールは実スレッドIDを返す (outbox.js が仮ID 'compose:…' と差し替え、
      // 以後この会話に届く返信が同じチケットに載るようにする)
      const threadOut = isNewMail && sent.threadId ? { externalThreadId: String(sent.threadId) } : {};
      return { externalReplyId: sent.id, ...threadOut, ...(warning ? { warning } : {}) };
    },

    /**
     * 添付の実体取得 (画面の添付表示用。attachments.js から呼ばれる)。
     * Gmail の attachmentId は取得ごとに変わり得るため同期時に保存していない (walkPayload 参照)。
     * そのためメッセージを取り直し、ファイル名 (+サイズ) で part を特定してから本体を取る。
     * @returns {{ buffer: Buffer, contentType: string|null, fileName: string|null }}
     */
    async fetchAttachment({ externalAttachmentId, externalMessageId, fileName, fileSize, maxBytes = null }) {
      requests = 0;
      const msgId = String(externalMessageId || '');
      if (!msgId || msgId.startsWith('syn:')) {
        throw new Error('このメッセージにはGmailのメッセージIDがありません (再同期が必要です)');
      }
      const m = await apiGet(`messages/${encodeURIComponent(msgId)}?format=full`, 'attachment-message');
      const parts = [];
      (function walk(p) {
        if (!p) return;
        parts.push(p);
        for (const c of p.parts || []) walk(c);
      })(m.payload);
      const withFile = parts.filter(p => p.filename && p.body?.attachmentId);
      // 同期時に保存した partId (MIME構造上の位置) が最も確実。無い場合 (旧データ) は
      // ファイル名 + サイズ、最後にファイル名だけ、の順にフォールバックする
      const wantPartId = String(externalAttachmentId || '').startsWith('part:')
        ? String(externalAttachmentId).slice(5) : null;
      // partId を保存してあるならそれと一致必須 (見つからなければ失敗させる)。
      // 名前へのフォールバックは partId を保存していない旧データ (syn:) のときだけ
      // — 一致しないのに同名の別添付を返すと、別の顧客の写真を表示しかねない
      const part = wantPartId
        ? withFile.find(p => String(p.partId) === wantPartId)
        : (withFile.find(p => p.filename === fileName && (fileSize == null || p.body?.size === fileSize))
          || withFile.find(p => p.filename === fileName));
      if (!part) throw new Error(`添付「${fileName || '(名称不明)'}」がメール内に見つかりません (削除された、またはメールの構成が変わった可能性)`);
      if (maxBytes && part.body?.size > maxBytes) {
        throw new Error(`添付が大きすぎます (${Math.round(part.body.size / 1048576)}MB。上限${Math.round(maxBytes / 1048576)}MB)`);
      }
      const a = await apiGet(`messages/${encodeURIComponent(msgId)}/attachments/${encodeURIComponent(part.body.attachmentId)}`, 'attachment-body');
      if (!a?.data) throw new Error('Gmail添付レスポンスに data がありません');
      return { buffer: b64urlToBuf(a.data), contentType: part.mimeType || null, fileName: part.filename || fileName || null };
    },

    /** 認証状態 (運用管理画面用)。refresh token は再認可レス無期限運用のため期限は返さない */
    async getAuthStatus() {
      const p = await apiGet('profile', 'profile');
      return { ok: !!p.emailAddress, authExpiresAt: null, emailAddress: p.emailAddress };
    },

    /** sync/engine.js 契約。部分成功は返さない (途中失敗は全体 throw) */
    async fetchNew({ sinceIso, untilIso }) {
      requests = 0;
      const sinceMs = Date.parse(sinceIso);
      const untilMs = Date.parse(untilIso);
      if (!Number.isFinite(sinceMs) || !Number.isFinite(untilMs)) {
        throw new Error(`fetchNew: 不正なウィンドウです since=${sinceIso} until=${untilIso}`);
      }
      // after: は秒粒度で「以降」。境界の取りこぼしを避けるため1秒引く (重複はUPSERTで無害)
      const q = encodeURIComponent(`-in:chats after:${Math.max(0, Math.floor(sinceMs / 1000) - 1)}`);

      // 1. 窓内メッセージ一覧 → threadId 集約
      const threadIds = new Set();
      let pageToken = '';
      for (let page = 1; ; page++) {
        if (page > maxListPages) {
          const e = new Error(`Gmail一覧が ${maxListPages} ページを超過しました。バックフィル幅を分割してください`);
          e.errorType = 'window_too_large';
          throw e;
        }
        const j = await apiGet(`messages?q=${q}&maxResults=${PAGE_SIZE}${pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : ''}`, `list p${page}`);
        for (const m of j.messages || []) {
          if (!m?.threadId) {
            const e = new Error(`Gmail一覧契約違反: threadId がありません (p${page})`);
            e.errorType = 'contract_violation';
            throw e;
          }
          threadIds.add(m.threadId);
        }
        pageToken = j.nextPageToken || '';
        if (!pageToken) break;
      }

      // 2. スレッド全量取得 → メールルール適用込みで契約形へ。
      // 並列取得 (既定5)。Gmail quota は 250units/s・threads.get=10units なので5並列は余裕圏内。
      // ノイズ量実測 (2026-07-18: 3日で2,700+スレッド) に対して逐次+120msでは10分リースを超えるため
      const ids = [...threadIds];
      const inquiries = [];
      let skipped = 0;
      let cursor = 0;
      const worker = async () => {
        for (;;) {
          const i = cursor++;
          if (i >= ids.length) return;
          const tid = ids[i];
          const t = await apiGet(`threads/${encodeURIComponent(tid)}?format=full`, `thread ${String(tid).slice(0, 10)}…`);
          const item = mapThread(t, { ruleEvaluator });
          if (item) inquiries.push(item);
          else skipped++;
        }
      };
      // 1本でも失敗したら全体throw (契約どおり部分成功を返さない)。Promise.allで最初のrejectを伝播
      await Promise.all(Array.from({ length: Math.min(concurrency, ids.length || 1) }, () => worker()));
      if (skipped > 0) console.log(`[gmail-adapter] メールルールで ${skipped} スレッドをスキップ (ノイズ除去)`);
      return { inquiries };
    },
  };
}
