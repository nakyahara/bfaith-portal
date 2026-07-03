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
 *   POST /yahoo/uploadItemImage     — JSON {fileName,itemCode,bufferBase64} で画像 upload
 *                                     (VPS proxy 側で Yahoo 公式 contract の multipart に再構築)
 */

const DEFAULT_TIMEOUT_MS = 60_000;
const MAX_IMAGE_BYTES = 2 * 1024 * 1024;

// Yahoo uploadItemImage の画像命名規約 (AI_reference 設計書 v6 §3.6):
//   - メイン: {item_code}.jpg
//   - サブ:   {item_code}_{1..20}.jpg  (1 桁、 zero-padding なし)
//   - item_code: 英数字 / _ / - で 1-80 文字
const ITEM_CODE_RE = /^[A-Za-z0-9_-]{1,80}$/;
const FILE_NAME_RE_GENERIC = /^[A-Za-z0-9_-]{1,80}(_(?:[1-9]|1[0-9]|20))?\.jpg$/;

// uploadLibImage 用 filename 規約 (Yahoo it-14061 修正 2026-06-27):
//   `{item_code}_lib_{1..20}.jpg` (1 桁、 zero-padding なし)
//   item_code は uploadItemImage と同じ ^[A-Za-z0-9_-]{1,80}$
const LIB_FILE_NAME_RE = /^[A-Za-z0-9_-]{1,80}_lib_([1-9]|1[0-9]|20)\.jpg$/;

export function validateUploadLibFileName(fileName, itemCode) {
  if (typeof fileName !== 'string' || fileName.length === 0 || fileName.length > 100) {
    throw new YahooProxyError('uploadLibImage', `fileName invalid length: ${fileName?.length}`);
  }
  if (fileName.includes('/') || fileName.includes('\\')) {
    throw new YahooProxyError('uploadLibImage', `fileName must be basename: ${JSON.stringify(fileName)}`);
  }
  if (/[\x00-\x1F\x7F]/.test(fileName)) {
    throw new YahooProxyError('uploadLibImage', 'fileName has control chars');
  }
  if (!LIB_FILE_NAME_RE.test(fileName)) {
    throw new YahooProxyError('uploadLibImage', `fileName format invalid (expected {item_code}_lib_N.jpg): ${fileName}`);
  }
  if (!ITEM_CODE_RE.test(itemCode)) {
    throw new YahooProxyError('uploadLibImage', `itemCode format invalid: ${itemCode}`);
  }
  if (!fileName.startsWith(`${itemCode}_lib_`)) {
    throw new YahooProxyError('uploadLibImage', `fileName ${fileName} does not match itemCode ${itemCode}`);
  }
}

export function validateUploadFileName(fileName, itemCode) {
  if (typeof fileName !== 'string' || fileName.length === 0 || fileName.length > 100) {
    throw new YahooProxyError('uploadItemImage', `fileName invalid length: ${fileName?.length}`);
  }
  if (fileName.includes('/') || fileName.includes('\\')) {
    throw new YahooProxyError('uploadItemImage', `fileName must be basename: ${JSON.stringify(fileName)}`);
  }
  if (/[\x00-\x1F\x7F]/.test(fileName)) {
    throw new YahooProxyError('uploadItemImage', 'fileName has control chars');
  }
  if (!FILE_NAME_RE_GENERIC.test(fileName)) {
    throw new YahooProxyError('uploadItemImage', `fileName format invalid (expected {item_code}.jpg or {item_code}_N.jpg): ${fileName}`);
  }
  if (!ITEM_CODE_RE.test(itemCode)) {
    throw new YahooProxyError('uploadItemImage', `itemCode format invalid: ${itemCode}`);
  }
  // ITEM_CODE_RE 通過後 → [A-Za-z0-9_-] のみ → regex literal で escape 不要
  const expected = new RegExp(`^${itemCode}(?:_(?:[1-9]|1[0-9]|20))?\\.jpg$`);
  if (!expected.test(fileName)) {
    throw new YahooProxyError('uploadItemImage', `fileName ${fileName} does not match itemCode ${itemCode}`);
  }
}

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
 * R12 (2026-07-03): 4xx/5xx の Yahoo XML 応答から <Code>/<Message> を抽出して
 * エラーメッセージに含める。 従来は「HTTP 400」だけで真因 (it-xxxx) が UI に出ず、
 * 中原さんが原因を判断できなかった。
 */
function httpErrorDetail(text) {
  const t = String(text || '');
  const code = t.match(/<Code>\s*([^<]+)\s*<\/Code>/i)?.[1]?.trim();
  const msg = t.match(/<Message>\s*([\s\S]*?)\s*<\/Message>/i)?.[1]?.trim();
  if (code || msg) return ` — Yahoo応答: ${[code, msg].filter(Boolean).join(' ')}`;
  const plain = t.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  return plain ? ` — 応答: ${plain.slice(0, 300)}` : '';
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
    throw new YahooProxyError('getLeadTimeList', `HTTP ${res.status}${httpErrorDetail(text)}`, { status: res.status, body: text.slice(0, 300) });
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
    throw new YahooProxyError('editItem', `HTTP ${res.status}${httpErrorDetail(text)}`, { status: res.status, body: text.slice(0, 1000) });
  }
  // Codex E-5b R1 H-1: HTTP 200 でも XML semantic check
  assertYahooXmlOk('editItem', text);
  return { status: res.status, body: text };
}

/**
 * POST /yahoo/uploadItemImage に JSON で 1 枚 upload。
 *
 * 旧 RakutenYahooSync (Downloads/RakutenYahooSync/src/services/yahoo-api.js:250) で
 * 動作実績ある Yahoo 公式 contract:
 *   - URL: ${apiBase}/uploadItemImage?seller_id=xxx
 *   - form: `file` 単数のみ + Blob(image/jpeg) + filename
 *   - headers: Authorization のみ (Content-Type は undici が boundary 付きで自動付与)
 *
 * bfaith-portal → VPS proxy は JSON {fileName, itemCode, bufferBase64} で送り、
 * VPS proxy 側で上記 contract に再構築する。
 * 余計な field (seller_id / item_code / file_name) を multipart に入れる旧実装は
 * Yahoo が 400 を返すので廃止。
 *
 * @param {{ buffer: Buffer|Uint8Array, fileName: string, itemCode: string }} args
 * @returns {Promise<{status, body}>}
 */
export async function callUploadItemImage({ buffer, fileName, itemCode }, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  if (!buffer || !(buffer instanceof Uint8Array)) {
    throw new YahooProxyError('uploadItemImage', 'buffer must be Buffer/Uint8Array');
  }
  if (!fileName) throw new YahooProxyError('uploadItemImage', 'fileName is required');
  if (!itemCode) throw new YahooProxyError('uploadItemImage', 'itemCode is required');
  // smoke gate: fileName 規約 + itemCode との完全結合
  validateUploadFileName(fileName, itemCode);
  // smoke gate: JPEG magic FF D8
  if (buffer.length < 4 || buffer[0] !== 0xFF || buffer[1] !== 0xD8) {
    const head = Buffer.from(buffer.slice(0, 4)).toString('hex');
    throw new YahooProxyError('uploadItemImage', `invalid JPEG magic: ${head}`);
  }
  // smoke gate: 2MB 未満 (Yahoo 仕様)
  if (buffer.length >= MAX_IMAGE_BYTES) {
    throw new YahooProxyError('uploadItemImage', `image too large: ${buffer.length}B (>= ${MAX_IMAGE_BYTES}B)`);
  }

  const bufBase = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  const payload = {
    fileName,
    itemCode,
    bufferBase64: bufBase.toString('base64'),
  };
  const url = `${getProxyBaseUrl()}/yahoo/uploadItemImage`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { ...getProxyHeaders(), 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new YahooProxyError('uploadItemImage', `HTTP ${res.status}${httpErrorDetail(text)}`, { status: res.status, body: text.slice(0, 500) });
  }
  // Codex E-5b R1 H-1: HTTP 200 でも XML semantic check
  assertYahooXmlOk('uploadItemImage', text);
  // Codex Phase E image-html R1: Yahoo response の <Url><ModeA>URL</ModeA></Url> を抽出して返す。
  //   additional1 / sp_additional の楽天画像 URL を Yahoo CDN URL に置換する rewriter で使う。
  const yahooUrl = extractYahooImageUrl(text);
  return { status: res.status, body: text, yahooUrl };
}

/**
 * Yahoo uploadItemImage の response XML から <Url><ModeA>...</ModeA></Url> の URL を抽出。
 *   shape: <ResultSet><Result><Status>OK</Status><Id>...</Id><Name>...</Name>
 *            <Url><ModeA>https://item-shopping.c.yimg.jp/...</ModeA></Url></Result></ResultSet>
 *   抽出失敗時は null (caller 側で fail-closed)。
 */
export function extractYahooImageUrl(xml) {
  if (!xml || typeof xml !== 'string') return null;
  // Codex R3 medium 反映: regex より具体的な階層 match で誤抽出を避ける
  //   (例えば caption に <ModeA> という文字列が混入しないよう Url 直下に限定)
  const m = xml.match(/<Url\b[^>]*>[\s\S]*?<ModeA\b[^>]*>([\s\S]*?)<\/ModeA>[\s\S]*?<\/Url>/i);
  if (!m) return null;
  const url = m[1].trim();
  return url || null;
}

/**
 * POST /yahoo/uploadLibImage に JSON で 1 枚 upload (Yahoo it-14061 修正 2026-06-27)。
 *
 * uploadItemImage と別 API (Yahoo 仕様で 「商品画像/商品詳細画像」 とは別の 「追加画像 (lib)」):
 *   - additional1/sp_additional の <img src> に貼れる URL を発行する唯一の手段
 *   - Yahoo response は <Status>OK</Status> のみで <Url> 返さないので、 VPS proxy 側で
 *     `https://shopping.c.yimg.jp/lib/{store}/{fileName}` を合成して返却 (Codex R2 critical)
 *   - 200 時のレスポンスは JSON { ok:true, yahooUrl, body } 形式
 *
 * @param {{ buffer: Buffer|Uint8Array, fileName: string, itemCode: string }} args
 * @returns {Promise<{status, yahooUrl, body}>}
 */
export async function callUploadLibImage({ buffer, fileName, itemCode }, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  if (!buffer || !(buffer instanceof Uint8Array)) {
    throw new YahooProxyError('uploadLibImage', 'buffer must be Buffer/Uint8Array');
  }
  if (!fileName) throw new YahooProxyError('uploadLibImage', 'fileName is required');
  if (!itemCode) throw new YahooProxyError('uploadLibImage', 'itemCode is required');
  validateUploadLibFileName(fileName, itemCode);
  if (buffer.length < 4 || buffer[0] !== 0xFF || buffer[1] !== 0xD8) {
    const head = Buffer.from(buffer.slice(0, 4)).toString('hex');
    throw new YahooProxyError('uploadLibImage', `invalid JPEG magic: ${head}`);
  }
  if (buffer.length >= MAX_IMAGE_BYTES) {
    throw new YahooProxyError('uploadLibImage', `image too large: ${buffer.length}B (>= ${MAX_IMAGE_BYTES}B)`);
  }

  const bufBase = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  const payload = {
    fileName,
    itemCode,
    bufferBase64: bufBase.toString('base64'),
  };
  const url = `${getProxyBaseUrl()}/yahoo/uploadLibImage`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { ...getProxyHeaders(), 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new YahooProxyError('uploadLibImage', `HTTP ${res.status}${httpErrorDetail(text)}`, { status: res.status, body: text.slice(0, 500) });
  }
  // proxy は 200 時に JSON { ok, yahooUrl, body }、 それ以外は XML をそのまま返す
  let parsed;
  try { parsed = JSON.parse(text); }
  catch (e) {
    throw new YahooProxyError('uploadLibImage', `proxy returned non-JSON 200: ${text.slice(0, 200)}`);
  }
  if (!parsed?.ok || !parsed?.yahooUrl) {
    throw new YahooProxyError('uploadLibImage', `proxy did not return yahooUrl: ${text.slice(0, 200)}`);
  }
  // Yahoo XML semantic check (proxy が body を渡してくる)
  if (parsed.body) assertYahooXmlOk('uploadLibImage', parsed.body);
  return { status: res.status, yahooUrl: parsed.yahooUrl, body: parsed.body };
}

export { assertYahooXmlOk };
