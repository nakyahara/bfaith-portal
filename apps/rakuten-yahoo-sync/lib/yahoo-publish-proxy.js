/**
 * Yahoo Shopping publish API を vps-proxy 経由で叩く client (Phase E-5b)。
 *
 * 設計原則 (Codex Phase E R3-R4 + E-5a):
 *   - Render に Yahoo OAuth token / RSA 公開鍵を置かない
 *   - vps-proxy (133.167.122.198:8080) の /yahoo/* endpoint 経由 (X-Proxy-Secret 認証)
 *   - 必須 env: YAHOO_PROXY_BASE_URL / YAHOO_PROXY_SECRET
 *   - vps-proxy 側で token + 署名 (X-sws-signature) を自動付与する
 *
 * 提供 endpoint:
 *   GET  /yahoo/getLeadTimeList     — 発送日設定マスタ ID 一覧
 *   POST /yahoo/editItem            — form-encoded fields で商品登録
 *   POST /yahoo/uploadItemImage     — multipart binary で画像 upload
 */

const DEFAULT_TIMEOUT_MS = 60_000;

export class YahooProxyError extends Error {
  constructor(endpoint, message, { status = null, body = null } = {}) {
    super(`[yahoo-proxy:${endpoint}] ${message}`);
    this.name = 'YahooProxyError';
    this.endpoint = endpoint;
    this.status = status;
    this.body = body;
  }
}

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    const err = new YahooProxyError('config', `${name} not configured (fail-closed)`);
    err.statusCode = 503;
    throw err;
  }
  return v.trim();
}

function getProxyBaseUrl() {
  return requireEnv('YAHOO_PROXY_BASE_URL').replace(/\/+$/, '');
}

function getProxyHeaders() {
  return { 'X-Proxy-Secret': requireEnv('YAHOO_PROXY_SECRET') };
}

/**
 * GET /yahoo/getLeadTimeList?seller_id=xxx で発送日設定マスタを取得。
 *   - response は XML
 *   - <LeadTime><Id>1000</Id>...</LeadTime> を抽出
 * @returns {Promise<{ids: number[]}>}
 */
export async function fetchLeadTimeList({ sellerId = null, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const seller = sellerId || requireEnv('YAHOO_SELLER_ID');
  const url = `${getProxyBaseUrl()}/yahoo/getLeadTimeList?seller_id=${encodeURIComponent(seller)}`;
  const res = await fetch(url, {
    method: 'GET',
    headers: getProxyHeaders(),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new YahooProxyError('getLeadTimeList', `HTTP ${res.status}`, { status: res.status, body: text.slice(0, 300) });
  }
  // 簡易 XML パース: <Id>NNN</Id> を全部抽出 (LeadTime 要素内)
  const ids = [];
  const idRe = /<Id>\s*(\d+)\s*<\/Id>/g;
  let m;
  while ((m = idRe.exec(text)) !== null) {
    const n = parseInt(m[1], 10);
    if (Number.isFinite(n)) ids.push(n);
  }
  return { ids };
}

/**
 * Yahoo Shopping API XML response の semantic check (Codex E-5b R1 H-1 + R2 Medium 反映):
 *   HTTP 200 でも以下を fail-closed に処理:
 *     - <Error><Code>...<Message>...</Error>     → throw (Yahoo 仕様の error 形式)
 *     - <Status>NG</Status> / <Result>NG</Result> → throw
 *     - <Status>OK</Status> も <Result>OK</Result> も無い場合 → throw (未知形式 fail-closed)
 *   Yahoo Shopping API は editItem / uploadItemImage / getLeadTimeList 全てで
 *   ResultSet > Result > Status (OK / Error) を返す仕様。
 */
function assertYahooXmlOk(endpoint, xml) {
  if (!xml || typeof xml !== 'string') {
    throw new YahooProxyError(endpoint, 'empty Yahoo response body', { body: '' });
  }
  // <Error> 要素を検知 (Code / Message を抽出)
  const errMatch = xml.match(/<Error\b[^>]*>([\s\S]*?)<\/Error>/i);
  if (errMatch) {
    const codeMatch = errMatch[1].match(/<Code>\s*([^<]+)\s*<\/Code>/i);
    const msgMatch = errMatch[1].match(/<Message>\s*([^<]+)\s*<\/Message>/i);
    const code = codeMatch?.[1]?.trim() || 'unknown_error';
    const msg = msgMatch?.[1]?.trim() || 'unknown';
    throw new YahooProxyError(endpoint, `Yahoo XML <Error>: ${code} ${msg}`, { body: xml.slice(0, 500) });
  }
  // OK 指標を厳格チェック (Codex E-5b R2 Medium: 未知形式は fail-closed)
  const statusMatch = xml.match(/<Status>\s*([^<]+)\s*<\/Status>/i);
  const resultMatch = xml.match(/<Result>\s*([^<]+)\s*<\/Result>/i);
  const statusOk = statusMatch && /^ok$/i.test(statusMatch[1].trim());
  const resultOk = resultMatch && /^ok$/i.test(resultMatch[1].trim());
  if (statusMatch && !statusOk) {
    throw new YahooProxyError(endpoint, `Yahoo XML <Status>${statusMatch[1].trim()}</Status>`, { body: xml.slice(0, 500) });
  }
  if (resultMatch && !resultOk) {
    throw new YahooProxyError(endpoint, `Yahoo XML <Result>${resultMatch[1].trim()}</Result>`, { body: xml.slice(0, 500) });
  }
  if (!statusOk && !resultOk) {
    throw new YahooProxyError(
      endpoint,
      'Yahoo XML has no <Status>OK</Status> / <Result>OK</Result> (unknown response format, fail-closed)',
      { body: xml.slice(0, 500) }
    );
  }
}

/**
 * POST /yahoo/editItem に form-encoded body で商品登録。
 *
 * @param {object} fields  field_name → value (string|number|array)
 *   - array は subcodes 用 → "subcodes=v1,v2,v3" 形式に joined
 *   - seller_id は自動付与 (Codex E-5b R1 H-2: 呼び出し側 forgetting 防止)
 */
export async function callEditItem(fields, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const seller = requireEnv('YAHOO_SELLER_ID');
  const params = new URLSearchParams();
  // Codex E-5b R1 H-2: seller_id を必ず付ける (fields に既存なら上書きしない)
  params.append('seller_id', seller);
  for (const [k, v] of Object.entries(fields || {})) {
    if (k === 'seller_id') continue; // fields 側の上書きは無視 (env が真)
    if (v === null || v === undefined) continue;
    if (Array.isArray(v)) {
      params.append(k, v.join(','));
    } else {
      params.append(k, String(v));
    }
  }
  const url = `${getProxyBaseUrl()}/yahoo/editItem`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      ...getProxyHeaders(),
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: params.toString(),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new YahooProxyError('editItem', `HTTP ${res.status}`, { status: res.status, body: text.slice(0, 500) });
  }
  // Codex E-5b R1 H-1: HTTP 200 でも XML semantic check
  assertYahooXmlOk('editItem', text);
  return { status: res.status, body: text };
}

/**
 * POST /yahoo/uploadItemImage に multipart で 1 枚 upload。
 *
 * @param {{ buffer: Buffer, fileName: string, itemCode: string }} args
 * @returns {Promise<{status, body}>}
 */
export async function callUploadItemImage({ buffer, fileName, itemCode }, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  if (!buffer || !(buffer instanceof Uint8Array)) {
    throw new YahooProxyError('uploadItemImage', 'buffer is required');
  }
  if (!fileName) throw new YahooProxyError('uploadItemImage', 'fileName is required');
  if (!itemCode) throw new YahooProxyError('uploadItemImage', 'itemCode is required');
  const seller = requireEnv('YAHOO_SELLER_ID');
  const form = new FormData();
  form.append('seller_id', seller);
  form.append('item_code', itemCode);
  form.append('file_name', fileName);
  const blob = new Blob([buffer], { type: 'image/jpeg' });
  form.append('file', blob, fileName);

  const url = `${getProxyBaseUrl()}/yahoo/uploadItemImage`;
  const res = await fetch(url, {
    method: 'POST',
    headers: getProxyHeaders(),
    body: form,
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new YahooProxyError('uploadItemImage', `HTTP ${res.status}`, { status: res.status, body: text.slice(0, 500) });
  }
  // Codex E-5b R1 H-1: HTTP 200 でも XML semantic check
  assertYahooXmlOk('uploadItemImage', text);
  return { status: res.status, body: text };
}

export { assertYahooXmlOk };
