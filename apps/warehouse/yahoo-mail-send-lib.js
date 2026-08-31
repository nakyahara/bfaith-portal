/**
 * yahoo-mail-send-lib.js — Yahoo 版レビューキャンペーンの送信経路 (Gmail API) (PR-Y-C4)
 *
 * 楽天版は「あんしんメルアドリレー」の SMTP (sub.fw.rakuten.ne.jp) を使うが、これは
 * 楽天の匿名アドレス専用。Yahoo は注文 API から実アドレスが返るので Gmail から送る。
 *
 * 前提 (2026-08-28 確認、apps/inquiry-hub/sync/adapters/gmail.js 冒頭と同じ):
 * - info@b-faith.biz は独立アカウントではなく d.nakahara@b-faith.biz の
 *   **転送 + send-as エイリアス**。したがって info@ 用のアプリパスワードは存在しないし作れない
 * - すでに inquiry-hub が同じトークンで From=info@ の本番送信を行っている
 * - env は INQUIRY_GMAIL_CLIENT_ID/SECRET/REFRESH_TOKEN を優先し、無ければ PO_GMAIL_* に
 *   フォールバック (inquiry-hub と同じ解決順)
 *
 * 設計上の要点:
 * - **From 置き換わりの検出を LIVE ゲートにする** (要件設計 v0.2 ⑦): send-as エイリアスが外れると
 *   Gmail は From を認証ユーザー (d.nakahara@) に黙って差し替える。届きはするが差出人が変わる事故。
 *   `verifyFrom()` が実送信 → 送信済みメッセージの From ヘッダ読み戻し で確認し、台帳に記録する。
 *   `assertFromVerified()` を通らない限り LIVE 送信を始めない
 * - **エラーはエンジンの 3 分類に正しく落とす** (rakuten-review-sender-lib の classifySendError):
 *     未送信が確定 (トークン取得失敗・4xx)      → err.responseCode を立てる → failed_safe (終端)
 *     送ったかどうか不明 (5xx・切断・タイムアウト・429) → responseCode なし → ambiguous (バッチ即時中断)
 *   429 は「送信前に拒否された」可能性が高いが、断定できないので安全側 (人の確認) に倒す
 * - **PII を例外に載せない** (約款第10条): Gmail のエラー本文には宛先が入り得るため、
 *   例外メッセージには HTTP status と error.status (INVALID_ARGUMENT 等の英数字) だけを載せる
 */

const TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GMAIL_BASE = 'https://gmail.googleapis.com/gmail/v1/users/me';
const REQUEST_TIMEOUT_MS = 30000;
/** From 検証の有効期間 (これを過ぎたら再検証しないと LIVE 送信させない) */
export const FROM_VERIFY_TTL_DAYS = 90;

/** env から Gmail の資格情報を解決 (INQUIRY_GMAIL_* 優先、PO_GMAIL_* フォールバック) */
export function resolveGmailCredentials(env = process.env) {
  for (const prefix of ['INQUIRY_GMAIL_', 'PO_GMAIL_']) {
    const clientId = (env[`${prefix}CLIENT_ID`] || '').trim();
    const clientSecret = (env[`${prefix}CLIENT_SECRET`] || '').trim();
    const refreshToken = (env[`${prefix}REFRESH_TOKEN`] || '').trim();
    if (clientId && clientSecret && refreshToken) return { clientId, clientSecret, refreshToken, source: prefix };
  }
  throw new Error('GMAIL_KEY_MISSING: INQUIRY_GMAIL_* または PO_GMAIL_* (CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN) が未設定 (miniPC リポジトリ直下 .env)');
}

/** From 検証の台帳 (LIVE ゲート) */
export function ensureFromVerificationLedger(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS yahoo_mail_from_verifications (
    from_address TEXT PRIMARY KEY,
    verified_at  TEXT NOT NULL,
    observed_from TEXT NOT NULL,
    message_id   TEXT,
    note         TEXT
  )`);
}

export function recordFromVerification(db, { fromAddress, observedFrom, messageId = null, note = null, nowIso = new Date().toISOString() }) {
  ensureFromVerificationLedger(db);
  db.prepare(`
    INSERT INTO yahoo_mail_from_verifications (from_address, verified_at, observed_from, message_id, note)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(from_address) DO UPDATE SET verified_at = excluded.verified_at,
      observed_from = excluded.observed_from, message_id = excluded.message_id, note = excluded.note
  `).run(String(fromAddress).toLowerCase(), nowIso, String(observedFrom).toLowerCase(), messageId, note);
}

/**
 * 検証を「未完了」にする (Codex Y-C4 R1 Medium)。
 * verify-from は「実送信 → 送信済みメッセージの From を読み戻す」の 2 段階で、後段が落ちると
 * 台帳が更新されない。古い成功レコードが残っていると LIVE ゲートを素通りするので、
 * **実送信の前に必ず無効化してから**始める (成功したら上書きされる)。
 */
export function invalidateFromVerification(db, fromAddress, nowIso = new Date().toISOString()) {
  recordFromVerification(db, { fromAddress, observedFrom: '(verifying)', messageId: null, note: 'verify_incomplete', nowIso });
}

export function getFromVerification(db, fromAddress) {
  ensureFromVerificationLedger(db);
  return db.prepare('SELECT * FROM yahoo_mail_from_verifications WHERE from_address = ?').get(String(fromAddress).toLowerCase()) || null;
}

/**
 * LIVE 送信の前提チェック。検証済みでない / 期限切れ / 観測 From が違う → throw
 * (「届くけれど差出人が別人」を無言で量産しないためのゲート)
 */
export function assertFromVerified(db, fromAddress, nowIso = new Date().toISOString()) {
  const row = getFromVerification(db, fromAddress);
  const want = String(fromAddress).toLowerCase();
  if (!row) throw new Error(`FROM_NOT_VERIFIED: ${want} からの実送信検証がまだ (先に verify-from を実行)`);
  if (row.observed_from !== want) throw new Error(`FROM_MISMATCH: 検証時の実 From は ${row.observed_from} (send-as エイリアスが外れている)`);
  const age = Date.parse(nowIso) - Date.parse(row.verified_at);
  if (!Number.isFinite(age)) throw new Error('FROM_VERIFY_BROKEN: 検証時刻を読めない');
  if (age > FROM_VERIFY_TTL_DAYS * 86400000) {
    throw new Error(`FROM_VERIFY_STALE: 検証から ${Math.floor(age / 86400000)} 日経過 (${FROM_VERIFY_TTL_DAYS} 日以内に再検証)`);
  }
  return row;
}

/** ヘッダ値に CR/LF/NUL を通さない (ヘッダインジェクション対策) */
function headerSafe(name, v) {
  const s = String(v ?? '');
  if (/[\r\n\0]/.test(s)) throw Object.assign(new Error(`MAIL_HEADER: ${name} に改行/NUL は入れられない`), { responseCode: 400 });
  return s;
}

/**
 * 非ASCIIヘッダの RFC2047 (UTF-8/Base64) エンコード。
 * encoded-word は 1 個 75 文字以内という決まりがあるので、**バイト数**で分割する
 * (文字数で切ると日本語は 1 文字 3 バイトなので上限を超える)。文字の途中では切らない。
 */
export function mimeWord(s) {
  if (/^[\x20-\x7e]*$/.test(s)) return s;
  const MAX_BYTES = 45; // base64 で 60 文字 + `=?UTF-8?B?` `?=` の 12 文字 = 72 < 75
  const words = [];
  let buf = '';
  for (const ch of String(s)) { // コードポイント単位 (サロゲートペアを割らない)
    if (Buffer.byteLength(buf + ch, 'utf8') > MAX_BYTES) { words.push(buf); buf = ''; }
    buf += ch;
  }
  if (buf) words.push(buf);
  return words.map((w) => `=?UTF-8?B?${Buffer.from(w, 'utf8').toString('base64')}?=`).join('\r\n ');
}

/**
 * From/To などアドレスヘッダの表示名をエンコードする。
 * 表示名に日本語をそのまま置くと受信側で文字化けする (2026-08-28 実機で「雑貨イズム」が化けた)。
 * 非ASCII は encoded-word にする (encoded-word は引用符で囲んではいけない)。
 */
export function encodeAddressHeader(name, v) {
  const s = headerSafe(name, v);
  const m = s.match(/^\s*(.*?)\s*<([^<>]+)>\s*$/);
  if (!m) return s; // 表示名なしの素のアドレス
  const display = m[1].replace(/^"(.*)"$/, '$1').trim();
  const addr = m[2].trim();
  if (!display) return `<${addr}>`;
  if (/^[\x20-\x7e]*$/.test(display)) return `"${display.replace(/(["\\])/g, '\\$1')}" <${addr}>`;
  return `${mimeWord(display)} <${addr}>`;
}

/** RFC822 を組み立てて Gmail API の raw (base64url) にする */
export function buildRawMessage({ to, from, subject, text, messageId }) {
  const t = headerSafe('To', to);
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(t)) throw Object.assign(new Error('MAIL_TO: 宛先の形式が不正'), { responseCode: 400 });
  const headers = [
    `From: ${encodeAddressHeader('From', from)}`,
    `To: ${t}`,
    `Subject: ${mimeWord(headerSafe('Subject', subject))}`,
    `Message-ID: ${headerSafe('Message-ID', messageId)}`,
    'MIME-Version: 1.0',
    'Content-Type: text/plain; charset="UTF-8"',
    'Content-Transfer-Encoding: base64',
    // 一括配信であることを機械可読にしておく (自動応答の抑止・苦情処理の慣行)
    'Auto-Submitted: auto-generated',
    'Precedence: bulk',
  ];
  const body = Buffer.from(String(text), 'utf8').toString('base64').replace(/(.{76})/g, '$1\r\n');
  const raw = `${headers.join('\r\n')}\r\n\r\n${body}`;
  return Buffer.from(raw, 'utf8').toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/**
 * Gmail 送信クライアント。
 * @param {object} opts { env, fromAddress, fetchImpl, minIntervalMs }
 */
export function createGmailSender({ env = process.env, fromAddress, fetchImpl = fetch, minIntervalMs = 1000 } = {}) {
  const { clientId, clientSecret, refreshToken, source } = resolveGmailCredentials(env);
  if (!fromAddress) throw new Error('createGmailSender: fromAddress は必須');
  let tokenCache = { token: null, exp: 0 };
  let lastAt = 0;

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  async function throttle() {
    const wait = lastAt + minIntervalMs - Date.now();
    if (wait > 0) await sleep(wait);
    lastAt = Date.now();
  }

  /** トークン取得の失敗 = 送信要求を出す前 → 未送信確定 (responseCode を立てて failed_safe に落とす) */
  async function accessToken() {
    if (tokenCache.token && Date.now() < tokenCache.exp) return tokenCache.token;
    let res;
    try {
      res = await fetchImpl(TOKEN_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type: 'refresh_token' }).toString(),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch {
      throw Object.assign(new Error('GMAIL_TOKEN_FETCH_FAILED: トークン取得の接続失敗 (未送信)'), { responseCode: 401 });
    }
    const j = await res.json().catch(() => ({}));
    if (!res.ok || !j.access_token) {
      // error は 'invalid_grant' 等の英数字コードのみ (PII なし)
      const code = String(j.error || 'unknown').replace(/[^\w-]/g, '');
      throw Object.assign(new Error(`GMAIL_TOKEN_REJECTED: HTTP ${res.status} ${code} (未送信)`), { responseCode: res.status || 401 });
    }
    tokenCache = { token: j.access_token, exp: Date.now() + Math.max(60, (j.expires_in || 3600) - 120) * 1000 };
    return tokenCache.token;
  }

  async function api(method, pathAndQuery, body) {
    await throttle();
    const token = await accessToken();
    let res;
    try {
      res = await fetchImpl(`${GMAIL_BASE}/${pathAndQuery}`, {
        method,
        headers: { Authorization: `Bearer ${token}`, ...(body ? { 'Content-Type': 'application/json' } : {}) },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (err) {
      // 要求を出した後の失敗 = 送ったか不明 → responseCode を立てない (= ambiguous → バッチ中断)
      const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
      throw new Error(timedOut ? 'GMAIL_TIMEOUT: 応答なし (送信結果不明)' : 'GMAIL_CONNECTION_LOST: 接続断 (送信結果不明)');
    }
    const j = await res.json().catch(() => null);
    if (!res.ok) {
      // error.status は INVALID_ARGUMENT / PERMISSION_DENIED 等の英数字 enum (宛先は含まない)
      const st = String(j?.error?.status || '').replace(/[^A-Z_]/g, '') || 'UNKNOWN';
      if (res.status === 429 || res.status === 408) {
        throw new Error(`GMAIL_THROTTLED: HTTP ${res.status} ${st} (送信結果不明)`);
      }
      if (res.status >= 400 && res.status < 500) {
        throw Object.assign(new Error(`GMAIL_REJECTED: HTTP ${res.status} ${st} (未送信)`), { responseCode: res.status });
      }
      throw new Error(`GMAIL_SERVER_ERROR: HTTP ${res.status} ${st} (送信結果不明)`);
    }
    return j;
  }

  return {
    credentialSource: source,
    fromAddress,

    /** エンジンの sendFn 契約: resolve = 受理 / throw = classifySendError で分類 */
    async sendMail({ to, from, subject, text, messageId }) {
      const raw = buildRawMessage({ to, from: from || fromAddress, subject, text, messageId });
      const r = await api('POST', 'messages/send', { raw });
      return { gmailMessageId: r?.id || null, threadId: r?.threadId || null };
    },

    /**
     * From 置き換わりの実測。指定アドレスへ実際に 1 通送り、送信済みメッセージの From を読み戻す。
     * @returns { observedFrom, ok, gmailMessageId }
     */
    async verifyFrom({ to, subject, text, messageId }) {
      const sent = await this.sendMail({ to, from: fromAddress, subject, text, messageId });
      if (!sent.gmailMessageId) throw new Error('VERIFY_FROM: 送信結果に messageId が無い');
      const meta = await api('GET', `messages/${encodeURIComponent(sent.gmailMessageId)}?format=metadata&metadataHeaders=From`);
      const hdr = (meta?.payload?.headers || []).find((h) => String(h.name).toLowerCase() === 'from');
      const m = String(hdr?.value || '').match(/<([^>]+)>|([^\s<>]+@[^\s<>]+)/);
      const observedFrom = String(m?.[1] || m?.[2] || '').toLowerCase();
      return { observedFrom, ok: observedFrom === String(fromAddress).toLowerCase(), gmailMessageId: sent.gmailMessageId };
    },
  };
}
