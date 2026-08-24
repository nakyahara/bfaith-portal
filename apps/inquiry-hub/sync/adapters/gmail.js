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
import { evaluateMailRules } from '../../mail-rules.js';
import { SendRejectedError } from '../../outbox.js';

const GMAIL_BASE = 'https://gmail.googleapis.com/gmail/v1/users/me';
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

/** 楽天あんしんメルアドサービスのマスクアドレス (…@pc.fw.rakuten.ne.jp / @mb.fw.rakuten.ne.jp 等)。
 * 楽天側の中継サーバーは「店舗としてRMSに登録済みの送信元アドレス」以外からのメールを
 * `552 5.2.0 sender rejected` で拒否する (2026-08-24 実測: From が d.nakahara@ に
 * 置き換わった返信がバウンスした)。宛先がこれに該当し、かつ実効Fromの置き換わりが
 * 既知のときは送信前に止める (sendReply 参照) */
export const RAKUTEN_MASKED_RE = /@(?:[a-z0-9-]+\.)?fw\.rakuten\.ne\.jp$/i;

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
      msg: {
        externalMessageId: m.id,
        senderType: isShop ? 'shop' : 'customer',
        senderName: from.name ?? from.mailbox ?? null,
        bodyText: bodyText || null,
        isIncoming: isShop ? 0 : 1,
        sentAt: atMs, receivedAt: atMs,
        attachments,
      },
    };
  });
  mapped.sort((a, b) => a.msg.sentAt - b.msg.sentAt);

  const firstIncoming = mapped.find(x => x.msg.isIncoming) || mapped[0];
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
    ...(rule?.action === 'import_done' ? { initialInternalStatus: 'done' } : {}),
    // メールルールのフォルダ振り分け (2026-08-17)。新規作成時のみ適用 (既存チケットのフォルダは動かさない)
    ...(rule?.folderId ? { initialFolderId: rule.folderId } : {}),
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
  const fromAddress = cfg.fromAddress ?? 'info@b-faith.biz';
  const fromName = cfg.fromName ?? '雑貨イズム（B-Faith株式会社）';
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  // live送信後にSentメッセージを読み戻して実測したFrom (送信元の置き換わり検出)。
  // fromAddress の send-as エイリアスが未設定だと Gmail は From を認証ユーザーの
  // アドレスに黙って置き換える — 通常宛先には届くが、楽天マスクアドレス宛は拒否される。
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
     * 引数 dryRun: true はアダプターのモード設定に関係なく実送信を禁止する
     * (プレビュー経路がliveアダプターを受け取っても安全なように、呼び出し単位で強制。Codexレビュー反映)
     */
    async sendReply({ inquiry, bodyText, attachments = [], dryRun = false }) {
      requests = 0;
      let to = String(inquiry?.customer_identifier || '').trim().toLowerCase();
      const threadId = String(inquiry?.external_inquiry_id || '');
      if (!threadId) throw new SendRejectedError('Gmailスレッド外部IDがありません');
      const subjectBase = headerSafe('件名', String(inquiry?.subject || 'お問い合わせについて').trim() || 'お問い合わせについて');
      const subject = /^re:/i.test(subjectBase) ? subjectBase : `Re: ${subjectBase}`;

      // スレッド返信ヘッダの材料 + 宛先フォールバック (この時点の失敗 = 未送信確定)
      let inReplyTo = '', references = '';
      try {
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
        if (!EMAIL_RE.test(to)) {
          for (let i = msgs.length - 1; i >= 0; i--) {
            const eff = effectiveFromOf(msgs[i].payload || {});
            if (eff.mailbox && EMAIL_RE.test(eff.mailbox) && !eff.mailbox.endsWith('@' + OWN_DOMAIN)) {
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
      headerSafe('宛先', to);

      // 楽天マスクアドレス宛ガード: 実効Fromの置き換わりが既知なら送るだけ無駄 (100%バウンス)
      // なので未送信で止め、直し方をジョブ履歴に出す。初回は送信後の読み戻し検証で検出する
      if (RAKUTEN_MASKED_RE.test(to) && observedActualFrom && observedActualFrom !== fromAddress.toLowerCase()) {
        throw new SendRejectedError(`楽天マスクアドレス宛は送信元 ${fromAddress} 以外拒否されますが、`
          + `実際の送信元が ${observedActualFrom} になっています (未送信)。`
          + `Gmail (${observedActualFrom}) の設定「他のメールアドレスからメールを送信」に ${fromAddress} を追加してから再送してください`);
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
      const sent = await apiPost('messages/send', { raw, threadId }, 'send');
      if (!sent?.id) throw new Error('Gmail送信レスポンスに id がありません — 結果不明');
      console.log(`[gmail-send] 送信完了 To=${to} messageId=${sent.id}`);

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
              + `Gmailの「他のメールアドレスからメールを送信」に ${fromAddress} を追加してください`
              + (RAKUTEN_MASKED_RE.test(to) ? '。楽天マスクアドレス宛のため不達 (バウンス) になります — 設定後に再送が必要です' : '');
            console.error(`[gmail-send] ${warning} (messageId=${sent.id})`);
          }
        }
      } catch (e) {
        console.warn(`[gmail-send] From実測検証をスキップ (送信は完了): ${String(e?.message || e).slice(0, 120)}`);
      }
      return warning ? { externalReplyId: sent.id, warning } : { externalReplyId: sent.id };
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
