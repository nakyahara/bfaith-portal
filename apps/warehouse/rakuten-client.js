/**
 * 楽天RMS API 共通クライアント
 *
 * 全ての楽天 RMS 呼び出しを 1 つのキュー (この helper) に通して、
 *   - プロセス内グローバル直列化 (最低 N ms 間隔 + jitter)
 *   - 429 / Retry-After 尊重 + 5xx exponential backoff retry
 *   - 認証ヘッダー自動付与 (RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY)
 * を一元的に提供する。
 *
 * 注意 (cross-process):
 *   daily-sync (apps/warehouse/rakuten-orders.js) は WarehouseServer とは別プロセスで
 *   起動されるため、このモジュールの module-level state は共有されない。
 *   実害は時間帯分離 (daily-sync 07:00 / UI 業務時間) で実質回避できているが、
 *   完全な cross-process 制御が必要になったらファイルベース timestamp を導入する。
 */
import 'dotenv/config';

const RAKUTEN_HOST = 'api.rms.rakuten.co.jp';
const MIN_GAP_MS = 1100;
const JITTER_MS = 200; // 0..+200ms (最低1100ms厳守、上ぶれのみ)
const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_MAX_ATTEMPTS = 4; // 初回 1 + retry 3 = 計4回

// プロセス内グローバル直列化
let queueTail = Promise.resolve();
let lastCallStartedAt = 0;

function makeAuthHeader() {
  const sec = process.env.RAKUTEN_SERVICE_SECRET || '';
  const key = process.env.RAKUTEN_LICENSE_KEY || '';
  if (!sec || !key) return null;
  return `ESA ${Buffer.from(`${sec}:${key}`).toString('base64')}`;
}

function parseRetryAfter(headerValue) {
  if (!headerValue) return null;
  const asInt = parseInt(headerValue, 10);
  if (!Number.isNaN(asInt)) return Math.max(0, asInt * 1000);
  const asDate = Date.parse(headerValue);
  if (!Number.isNaN(asDate)) return Math.max(0, asDate - Date.now());
  return null;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/** バイナリレスポンスを上限つきで読む (Content-Length を信用せず実バイト数で打ち切る) */
async function readWithLimit(response, maxBytes) {
  const declared = Number(response.headers.get('content-length'));
  if (maxBytes && Number.isFinite(declared) && declared > maxBytes) {
    throw Object.assign(new Error(`レスポンスが大きすぎます (${Math.round(declared / 1048576)}MB > 上限${Math.round(maxBytes / 1048576)}MB)`), { tooLarge: true });
  }
  const reader = response.body?.getReader?.();
  if (!reader) {
    const buf = Buffer.from(await response.arrayBuffer());
    if (maxBytes && buf.length > maxBytes) {
      throw Object.assign(new Error(`レスポンスが大きすぎます (上限${Math.round(maxBytes / 1048576)}MB)`), { tooLarge: true });
    }
    return buf;
  }
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (maxBytes && total > maxBytes) {
      await reader.cancel().catch(() => { /* 既に切れている場合は無視 */ });
      throw Object.assign(new Error(`レスポンスが大きすぎます (上限${Math.round(maxBytes / 1048576)}MB)`), { tooLarge: true });
    }
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks, total);
}

/**
 * 楽天 RMS への単発リクエスト。直列キューに必ず乗る。
 *
 * @param {object} opts
 * @param {string} opts.path - 例: /es/2.0/items/search?cursorMark=*
 * @param {'GET'|'POST'} [opts.method='GET']
 * @param {any} [opts.body] - POST 時の JSON body
 * @param {string} [opts.rawBody] - JSON化せずそのまま送る生ボディ (XML API 用。
 *   Content-Type は headers で指定する。クーポンAPI 1.0 は application/xml — PR-C3 実測)
 * @param {FormData} [opts.formData] - multipart/form-data 送信用 (R-Cabinet file/insert)。
 *   Content-Type は fetch が boundary 付きで自動設定するため headers で指定しないこと
 * @param {object} [opts.headers] - 追加ヘッダー
 * @param {number} [opts.timeoutMs=60000]
 * @param {number} [opts.maxAttempts=4] - 初回 + retry 含む総試行回数
 * @param {'json'|'buffer'} [opts.responseType='json'] - 'buffer' はレスポンスを Buffer で返す
 *   (問い合わせ添付ファイル等のバイナリ用。data が Buffer になり contentType が付く)
 * @param {number|null} [opts.maxBytes=null] - responseType='buffer' 時の受信上限 (超過は throw)
 * @returns {Promise<{status:number, data:any, attempts:number, contentType?:string}>}
 */
export function rakutenRequest(opts) {
  const job = queueTail.then(() => doRakutenRequest(opts));
  // tail を新しい job に差し替え。失敗してもチェーンを切らさないため catch で吸収。
  queueTail = job.catch(() => undefined);
  return job;
}

async function doRakutenRequest({
  path,
  method = 'GET',
  body,
  rawBody,
  formData,
  headers = {},
  timeoutMs = DEFAULT_TIMEOUT_MS,
  maxAttempts = DEFAULT_MAX_ATTEMPTS,
  responseType = 'json',
  maxBytes = null,
}) {
  const auth = makeAuthHeader();
  if (!auth) {
    throw new Error('Rakuten credentials missing (RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY)');
  }

  // 直前の呼び出しから MIN_GAP_MS+jitter 経過するまで待機 (jitter は上ぶれのみ)
  const elapsed = Date.now() - lastCallStartedAt;
  const jitter = Math.random() * JITTER_MS; // 0..+200ms
  const requiredGap = MIN_GAP_MS + jitter;
  if (elapsed < requiredGap) {
    await sleep(requiredGap - elapsed);
  }

  const url = `https://${RAKUTEN_HOST}${path}`;
  const finalHeaders = {
    Authorization: auth,
    // formData のときは Content-Type を付けない (fetch が boundary 付きで自動設定する)
    ...(body !== undefined && formData === undefined ? { 'Content-Type': 'application/json; charset=utf-8' } : {}),
    ...headers,
  };
  const requestBody = formData !== undefined ? formData
    : (rawBody !== undefined ? rawBody : (body !== undefined ? JSON.stringify(body) : undefined));

  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    lastCallStartedAt = Date.now();

    try {
      const response = await fetch(url, {
        method,
        headers: finalHeaders,
        body: requestBody,
        signal: AbortSignal.timeout(timeoutMs),
      });

      const contentType = response.headers.get('content-type') || undefined;
      // バイナリ (添付ファイル) は Buffer のまま返す。エラー時はJSON/テキストなので
      // 成功レスポンスだけ Buffer 化し、失敗は従来どおり文字列で扱う。
      // Content-Length は信用せず読みながら打ち切る (巨大レスポンスでのメモリ枯渇防止)
      if (responseType === 'buffer' && response.status >= 200 && response.status < 300) {
        const buf = await readWithLimit(response, maxBytes);
        return { status: response.status, data: buf, attempts: attempt, contentType };
      }

      const text = await response.text();
      let data;
      try { data = text ? JSON.parse(text) : null; } catch { data = text; }

      // 成功
      if (response.status >= 200 && response.status < 300) {
        return { status: response.status, data, attempts: attempt, contentType };
      }

      // リトライ可能 (429 / 5xx)
      const isRetryable = response.status === 429 || (response.status >= 500 && response.status < 600);
      if (isRetryable && attempt < maxAttempts) {
        const retryAfter = parseRetryAfter(response.headers.get('retry-after'));
        const backoff = 1000 * Math.pow(2, attempt - 1); // 1s, 2s, 4s
        const backoffJitter = Math.random() * 250;
        const waitMs = Math.max(retryAfter ?? 0, backoff + backoffJitter);
        await sleep(waitMs);
        continue;
      }

      // 非リトライ or 最終試行失敗 → そのまま返す (呼び元が status を見て判断)
      return { status: response.status, data, attempts: attempt, contentType };
    } catch (e) {
      // ネットワークエラー / timeout
      lastError = e;
      if (e?.tooLarge) throw e;   // サイズ超過は再試行しても結果が変わらない (帯域の無駄)
      if (attempt < maxAttempts) {
        const waitMs = 1000 * Math.pow(2, attempt - 1) + Math.random() * 250;
        await sleep(waitMs);
        continue;
      }
    }
  }

  throw lastError || new Error('Rakuten request failed after retries');
}

/**
 * RMS のエラーレスポンス本文から code / message を取り出す (ログ・画面表示用)。
 *
 * RMS は API 系統ごとに形が違う。**認証エラー (401) は errors[] 形式**で返るので、
 * これを拾えないと「HTTP 401: {}」としか出ず原因が分からない (2026-08-30 の
 * ライセンスキー失効で実際に起きた):
 *   - 共通エラー   : { errors: [{ code: 'GA0001', message: 'Un-Authorised' }] }
 *   - 受注API系    : { MessageModelList: [{ messageType: 'ERROR', messageCode, message }] }
 *   - その他       : { code, message } / { error, message }
 * 本文をそのまま出すと注文者情報や問い合わせ本文が混ざるので、**構造化フィールドだけ**を拾う。
 */
export function describeRmsError(data) {
  if (data == null) return { code: null, message: null };
  if (typeof data === 'string') {
    const t = data.trim();
    return { code: null, message: t ? t.slice(0, 200) : null };
  }
  if (typeof data !== 'object') return { code: null, message: null };

  const errs = Array.isArray(data.errors) ? data.errors.filter(e => e && typeof e === 'object') : null;
  if (errs && errs.length > 0) {
    const e = errs[0];
    return {
      code: typeof e.code === 'string' ? e.code : null,
      message: typeof e.message === 'string' ? e.message : null,
    };
  }
  const list = Array.isArray(data.MessageModelList) ? data.MessageModelList : null;
  if (list && list.length > 0) {
    const e = list.find(m => m && m.messageType === 'ERROR') || list[0];
    return {
      code: e && typeof e.messageCode === 'string' ? e.messageCode : null,
      message: e && typeof e.message === 'string' ? e.message : null,
    };
  }
  return {
    code: typeof data.code === 'string' ? data.code : (typeof data.error === 'string' ? data.error : null),
    message: typeof data.message === 'string' ? data.message : null,
  };
}

/** describeRmsError の結果を ' (GA0001: Un-Authorised)' の形の接尾辞にする。何も取れなければ '' */
export function rmsErrorSuffix(data) {
  const { code, message } = describeRmsError(data);
  if (!code && !message) return '';
  return ` (${[code, message].filter(Boolean).join(': ').slice(0, 200)})`;
}

/** 401/403 のときに人が次に何をすればいいかを1行で添える (ライセンスキーは90日で失効する) */
export const RMS_AUTH_HINT = ' ← ライセンスキー失効の可能性 (RMS で再発行 → miniPC .env の RAKUTEN_LICENSE_KEY → Restart-Service WarehouseServer)';
