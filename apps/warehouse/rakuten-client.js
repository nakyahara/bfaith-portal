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
 * @returns {Promise<{status:number, data:any, attempts:number}>}
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

      const text = await response.text();
      let data;
      try { data = text ? JSON.parse(text) : null; } catch { data = text; }

      // 成功
      if (response.status >= 200 && response.status < 300) {
        return { status: response.status, data, attempts: attempt };
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
      return { status: response.status, data, attempts: attempt };
    } catch (e) {
      // ネットワークエラー / timeout
      lastError = e;
      if (attempt < maxAttempts) {
        const waitMs = 1000 * Math.pow(2, attempt - 1) + Math.random() * 250;
        await sleep(waitMs);
        continue;
      }
    }
  }

  throw lastError || new Error('Rakuten request failed after retries');
}
