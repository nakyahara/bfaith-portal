/**
 * API プロキシ（さくらVPS用） — au PAY + Yahoo Shopping 統合
 *
 * VPSの固定IP（133.167.122.198）から各APIにリクエストを中継する。
 *
 * au PAY:
 *   GET http://133.167.122.198:8080/wmshopapi/...
 *   Header: X-Proxy-Secret
 *
 * Yahoo Shopping:
 *   GET  http://133.167.122.198:8080/yahoo/health
 *   GET  http://133.167.122.198:8080/yahoo/orderList?startDate=...&endDate=...
 *   POST http://133.167.122.198:8080/yahoo/orderInfo  body: { orderIds: [...] }
 *   POST http://133.167.122.198:8080/yahoo/token/init  body: { code: "認可コード" }
 *   POST http://133.167.122.198:8080/yahoo/access-token (returns current access_token, refreshes if expired)
 *   GET  http://133.167.122.198:8080/yahoo/item-search?query=...&results=20 (public API, appid)
 *   GET  http://133.167.122.198:8080/yahoo/category-search?category_id=... (public API, appid)
 *   GET  http://133.167.122.198:8080/yahoo/auth-url
 *   Header: X-Proxy-Secret
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PROXY_PORT || 8080;
const PROXY_SECRET = process.env.PROXY_SECRET || '';
// rotation 用: PROXY_SECRET_NEXT が設定されていれば旧 + 新の両方を受け入れる (二重受け入れ期間)
const PROXY_SECRET_NEXT = process.env.PROXY_SECRET_NEXT || '';

// /yahoo/access-token 専用 secret (生 OAuth token を払い出すため PROXY_SECRET と分離、漏えい時の blast radius 隔離)
// 設定されていない場合は /yahoo/access-token は 503 で fail-closed。
const YAHOO_TOKEN_MINT_SECRET = process.env.YAHOO_TOKEN_MINT_SECRET || '';
const YAHOO_TOKEN_MINT_SECRET_NEXT = process.env.YAHOO_TOKEN_MINT_SECRET_NEXT || '';

const TOKEN_REFRESH_TIMEOUT_MS = parseInt(process.env.YAHOO_TOKEN_REFRESH_TIMEOUT_MS, 10) || 15000;

class TokenUpstreamError extends Error { constructor(m) { super(m); this.name = 'TokenUpstreamError'; } }
class TokenLocalError    extends Error { constructor(m) { super(m); this.name = 'TokenLocalError'; } }

// ─── au PAY設定 ───
const AUPAY_API_KEY = process.env.AUPAY_API_KEY || '';
const AUPAY_BASE = 'https://api.manager.wowma.jp';

// ─── Yahoo設定 ───
const YAHOO_CLIENT_ID = process.env.YAHOO_CLIENT_ID || '';
const YAHOO_CLIENT_SECRET = process.env.YAHOO_CLIENT_SECRET || '';
const YAHOO_SELLER_ID = process.env.YAHOO_SELLER_ID || '';
const YAHOO_PUBLIC_KEY_PATH = process.env.YAHOO_PUBLIC_KEY_PATH || path.join(__dirname, 'yahoo-public-key.pem');
const YAHOO_SIGNATURE_VERSION = process.env.YAHOO_SIGNATURE_VERSION || '4';
const YAHOO_TOKEN_URL = 'https://auth.login.yahoo.co.jp/yconnect/v2/token';
const YAHOO_API_BASE = 'https://circus.shopping.yahooapis.jp/ShoppingWebService/V1';
// 一般公開 Yahoo!ショッピング API (appid=Client ID 認証)。出店者 API (circus) と別系統。
// カテゴリ自動学習の AI 補完で「未進出カテゴリ」を Yahoo 全体から推定するのに使う。
const YAHOO_PUBLIC_API_BASE = 'https://shopping.yahooapis.jp/ShoppingWebService';
const YAHOO_REDIRECT_URI = process.env.YAHOO_REDIRECT_URI || 'https://b-faith.biz';
// git pull deploy 時にトークンを repo ディレクトリ外に置けるよう env で上書き可能にする
// (.env に YAHOO_TOKEN_FILE=/home/rocky/yahoo-tokens.json を設定推奨)
const TOKEN_FILE = process.env.YAHOO_TOKEN_FILE || path.join(__dirname, 'yahoo-tokens.json');

if (!PROXY_SECRET) { console.error('PROXY_SECRET is required'); process.exit(1); }

function ts() { return new Date().toISOString().slice(0, 19); }

// ─── Yahoo トークン管理 ───

const EMPTY_TOKENS = Object.freeze({ access_token: '', refresh_token: '', expires_at: 0 });

// loadTokens(): 未初期化 (ENOENT) は空 token を返す。それ以外 (parse / read / permission error) は throw。
// 「壊れた token file」を「未初期化」と取り違えないため。
function loadTokens() {
  let raw;
  try {
    raw = fs.readFileSync(TOKEN_FILE, 'utf-8');
  } catch (e) {
    if (e.code === 'ENOENT') return { ...EMPTY_TOKENS };
    throw new TokenLocalError(`yahoo-tokens.json read error: ${e.code} ${e.message}`);
  }
  try {
    return JSON.parse(raw);
  } catch (e) {
    throw new TokenLocalError(`yahoo-tokens.json parse error: ${e.message}`);
  }
}

// safeLoadTokens(): /yahoo/health 用。read/parse 失敗を握りつぶして空 token を返す (起動時の startup 列挙にも使う)。
function safeLoadTokens() {
  try { return loadTokens(); }
  catch { return { ...EMPTY_TOKENS }; }
}

function saveTokens(tokens) {
  fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokens, null, 2));
}

async function refreshAccessTokenInner() {
  let tokens;
  try {
    tokens = loadTokens();
  } catch (e) {
    throw new TokenLocalError(`yahoo-tokens.json 読み込み失敗: ${e.message}`);
  }
  if (!tokens.refresh_token) {
    throw new TokenLocalError('refresh_token がありません。/yahoo/token/init で初期化してください');
  }
  if (!YAHOO_CLIENT_ID || !YAHOO_CLIENT_SECRET) {
    throw new TokenLocalError('YAHOO_CLIENT_ID / YAHOO_CLIENT_SECRET が未設定');
  }

  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: tokens.refresh_token,
    client_id: YAHOO_CLIENT_ID,
    client_secret: YAHOO_CLIENT_SECRET,
  });

  // mutex で shared Promise になるため、ハングを絶対に許さない。AbortSignal.timeout で必ず終わる契約にする。
  let res;
  try {
    res = await fetch(YAHOO_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
      signal: AbortSignal.timeout(TOKEN_REFRESH_TIMEOUT_MS),
    });
  } catch (e) {
    throw new TokenUpstreamError(`Yahoo token endpoint network/timeout: ${e.message}`);
  }
  let data;
  try {
    data = await res.json();
  } catch (e) {
    throw new TokenUpstreamError(`Yahoo token endpoint invalid JSON: ${e.message}`);
  }
  if (data.error) {
    throw new TokenUpstreamError(`トークンリフレッシュ失敗: ${data.error} - ${data.error_description || ''}`);
  }

  const refreshTokenRotated = !!data.refresh_token;
  const updated = {
    access_token: data.access_token,
    refresh_token: data.refresh_token || tokens.refresh_token,
    expires_at: Date.now() + (data.expires_in || 3600) * 1000 - 60000,
    refresh_token_issued_at: refreshTokenRotated
      ? new Date().toISOString()
      : (tokens.refresh_token_issued_at || null),
    refresh_token_expires_at: refreshTokenRotated
      ? new Date(Date.now() + 28 * 86400000).toISOString()
      : (tokens.refresh_token_expires_at || null),
    updated_at: new Date().toISOString(),
  };
  try {
    saveTokens(updated);
  } catch (e) {
    throw new TokenLocalError(`yahoo-tokens.json 書き込み失敗: ${e.message}`);
  }
  console.log(`[${ts()}] Yahoo トークンリフレッシュ成功`);
  return updated;
}

// In-process mutex so that concurrent /yahoo/access-token + /yahoo/orderInfo
// invocations don't trigger N parallel refreshes against Yahoo's token endpoint.
let _inFlightRefresh = null;
async function refreshAccessToken() {
  if (_inFlightRefresh) return _inFlightRefresh;
  _inFlightRefresh = (async () => {
    try {
      return await refreshAccessTokenInner();
    } finally {
      _inFlightRefresh = null;
    }
  })();
  return _inFlightRefresh;
}

async function getAccessToken() {
  const tokens = loadTokens();
  if (tokens.access_token && tokens.expires_at > Date.now()) return tokens.access_token;
  const updated = await refreshAccessToken();
  return updated.access_token;
}

// Returns the current access_token along with metadata. Refreshes only if expired.
// Used by /yahoo/access-token for downstream apps (Render / local dev) that need
// to call Yahoo's product APIs directly with the same OAuth token.
async function getAccessTokenWithMeta() {
  const tokens = loadTokens();
  if (tokens.access_token && tokens.expires_at > Date.now()) {
    return { access_token: tokens.access_token, expires_at: tokens.expires_at, refreshed: false };
  }
  const updated = await refreshAccessToken();
  return { access_token: updated.access_token, expires_at: updated.expires_at, refreshed: true };
}

async function initTokenFromCode(code) {
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    client_id: YAHOO_CLIENT_ID,
    client_secret: YAHOO_CLIENT_SECRET,
    redirect_uri: YAHOO_REDIRECT_URI,
  });
  const res = await fetch(YAHOO_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });
  const data = await res.json();
  if (data.error) throw new Error(`トークン取得失敗: ${data.error} - ${data.error_description || ''}`);

  const tokens = {
    access_token: data.access_token,
    refresh_token: data.refresh_token,
    expires_at: Date.now() + (data.expires_in || 3600) * 1000 - 60000,
    refresh_token_issued_at: new Date().toISOString(),
    refresh_token_expires_at: new Date(Date.now() + 28 * 86400000).toISOString(),
    updated_at: new Date().toISOString(),
  };
  saveTokens(tokens);
  return tokens;
}

// ─── Yahoo 一般公開 API (appid=Client ID 認証、token 不要) ───

async function yahooPublicGet(apiPath, params = {}) {
  const u = new URL(`${YAHOO_PUBLIC_API_BASE}${apiPath}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
  }
  u.searchParams.set('appid', YAHOO_CLIENT_ID);
  const res = await fetch(u, { signal: AbortSignal.timeout(15000) });
  const body = await res.text();
  return { status: res.status, contentType: res.headers.get('content-type') || 'application/json', body };
}

// ─── Yahoo API呼び出し ───

async function callYahooAPI(endpoint, xmlBody) {
  const accessToken = await getAccessToken();
  const url = `${YAHOO_API_BASE}/${endpoint}`;
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
    'Content-Type': 'application/xml; charset=utf-8',
  };

  // 公開鍵認証（RSA公開鍵で「ストアアカウント:タイムスタンプ」を暗号化）
  try {
    if (fs.existsSync(YAHOO_PUBLIC_KEY_PATH)) {
      const publicKeyPem = fs.readFileSync(YAHOO_PUBLIC_KEY_PATH, 'utf-8');
      const publicKey = crypto.createPublicKey(publicKeyPem);
      const timestamp = Math.floor(Date.now() / 1000).toString();
      const message = `${YAHOO_SELLER_ID}:${timestamp}`;
      const encrypted = crypto.publicEncrypt(
        { key: publicKey, padding: crypto.constants.RSA_PKCS1_PADDING },
        Buffer.from(message, 'utf-8')
      );
      headers['X-sws-signature'] = encrypted.toString('base64');
      headers['X-sws-signature-version'] = YAHOO_SIGNATURE_VERSION;
    }
  } catch (e) { console.log(`[${ts()}] 署名スキップ: ${e.message}`); }

  const res = await fetch(url, { method: 'POST', headers, body: xmlBody });
  return await res.text();
}

async function yahooOrderList(startDate, endDate) {
  const xml = `<Req>
  <Search>
    <Result>2000</Result>
    <Start>1</Start>
    <Sort>+order_time</Sort>
    <Condition>
      <OrderTimeFrom>${startDate.length===8?startDate+"000000":startDate}</OrderTimeFrom>
      <OrderTimeTo>${endDate.length===8?endDate+"235959":endDate}</OrderTimeTo>
    </Condition>
    <Field>OrderId,OrderTime,OrderStatus</Field>
  </Search>
  <SellerId>${YAHOO_SELLER_ID}</SellerId>
</Req>`;
  return await callYahooAPI('orderList', xml);
}

/**
 * Yahoo API の汎用呼び出し (Phase E-5b で追加)。
 *   - method, contentType, body を指定可能 (既存 callYahooAPI は POST + XML 固定)
 *   - editItem (form-encoded) / uploadItemImage (multipart) / getLeadTimeList (GET) で共有
 *   - 署名 (RSA + X-sws-signature) と Bearer token は既存と同じロジック
 *   - body は Buffer 受け取り可 (binary multipart 用途)
 */
async function callYahooAPIRaw(endpoint, { method = 'POST', contentType = 'application/xml; charset=utf-8', body = null, queryString = '', rawUrl = null } = {}) {
  const accessToken = await getAccessToken();
  // rawUrl があれば base 無視で直接使う (Phase E-7-a: getLeadTimeList は別 base /shopping/ で叩く必要があるため)
  const url = rawUrl || `${YAHOO_API_BASE}/${endpoint}${queryString ? '?' + queryString : ''}`;
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
  };
  if (body !== null) headers['Content-Type'] = contentType;
  try {
    if (fs.existsSync(YAHOO_PUBLIC_KEY_PATH)) {
      const publicKeyPem = fs.readFileSync(YAHOO_PUBLIC_KEY_PATH, 'utf-8');
      const publicKey = crypto.createPublicKey(publicKeyPem);
      const timestamp = Math.floor(Date.now() / 1000).toString();
      const message = `${YAHOO_SELLER_ID}:${timestamp}`;
      const encrypted = crypto.publicEncrypt(
        { key: publicKey, padding: crypto.constants.RSA_PKCS1_PADDING },
        Buffer.from(message, 'utf-8')
      );
      headers['X-sws-signature'] = encrypted.toString('base64');
      headers['X-sws-signature-version'] = YAHOO_SIGNATURE_VERSION;
    }
  } catch (e) { console.log(`[${ts()}] 署名スキップ: ${e.message}`); }
  const fetchOpts = { method, headers };
  if (body !== null) fetchOpts.body = body;
  const res = await fetch(url, fetchOpts);
  const text = await res.text();
  return { status: res.status, body: text };
}

async function yahooOrderInfo(orderId) {
  // 2026-05-11 fix: <Field> を <Target> 内に戻す + Item. prefix 削除 + IsGetOrderDetail 削除
  //   (5/8 PROXY_SECRET rotation 修正のついでに 4/11 修正前の壊れた構造に regression していた)
  //   公式仕様: https://developer.yahoo.co.jp/webapi/shopping/orderInfo.html
  //   memory: project_yahoo_api.md (2026-04-11/12 bug 修正履歴) 参照
  const xml = `<Req>
  <Target>
    <OrderId>${orderId}</OrderId>
    <Field>OrderId,OrderTime,LastUpdateTime,OrderStatus,PayStatus,ShipStatus,TotalPrice,PayCharge,ShipCharge,Discount,UsePoint,LineId,ItemId,Title,SubCode,UnitPrice,OriginalPrice,Quantity,ItemTaxRatio,CouponDiscount</Field>
  </Target>
  <SellerId>${YAHOO_SELLER_ID}</SellerId>
</Req>`;
  return await callYahooAPI('orderInfo', xml);
}

// ─── HTTPリクエストボディ読み取り ───

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks).toString()));
    req.on('error', reject);
  });
}

// ─── Yahoo myItemList XML parser (Phase E-7-a) ───
//   Yahoo myItemList API のレスポンス XML を JSON 風 object に変換する。
//   RYS client は { items: [{ItemCode, Name?, HasSubCode?}], totalResultsAvailable, totalResultsReturned, firstResultPosition }
//   を期待 (yahoo-myitemlist-proxy.js)。 XML スキーマ:
//     <ResultSet totalResultsAvailable="N" firstResultPosition="K" [totalResultsReturned="M"]>
//       <Result>
//         <ItemCode>...</ItemCode>
//         <Name>...</Name>
//         <HasSubCode>0|1</HasSubCode>
//       </Result>
//       ...
//     </ResultSet>
//   依存ゼロ (regex で抽出)、 XML entity (&lt; 等) は decodeXmlEntities で復元。
function decodeXmlEntities(s) {
  return String(s)
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&apos;/g, "'")
    .replace(/&amp;/g, '&');
}
/**
 * Phase E-11-b: Yahoo getItemDetail (item info) の XML response から
 *   productCategory + path を抜き出す。
 *
 * Yahoo!ショッピングストアエディタ API の itemInfo (getItemDetail) は
 *   ItemCode 指定で 1 件の商品情報 XML を返す。 我々が欲しいのは:
 *     - <ProductCategory>NNN</ProductCategory>   (数値、 Yahoo カテゴリ ID)
 *     - <Path>some-path</Path>                   (ストア内 path)
 *   他にも多数フィールドあるが、 学習辞書では category + path しか使わない。
 *
 *   XML shape は ResultSet で wrap される or single Result block の可能性両方ある:
 *     <ResultSet> <Result> <ItemCode>...</ItemCode> <ProductCategory>...</ProductCategory> <Path>...</Path> ... </Result> </ResultSet>
 *   single レスポンスでも Result wrapper があるので、 そこから抜く。
 *   完全一致でなくケースバラつきも考慮 (例: <productCategory>) して i フラグで対応。
 */
function parseGetItemDetailXml(xml) {
  const out = { ItemCode: null, ProductCategory: null, Path: null, Name: null };
  if (typeof xml !== 'string' || xml.length === 0) return out;
  const tag = (name) => {
    const m = xml.match(new RegExp(`<${name}>([^<]*)</${name}>`, 'i'));
    return m ? decodeXmlEntities(m[1]).trim() : null;
  };
  out.ItemCode = tag('ItemCode');
  const pc = tag('ProductCategory');
  if (pc != null && pc !== '') {
    const n = parseInt(pc, 10);
    out.ProductCategory = Number.isFinite(n) ? n : null;
  }
  out.Path = tag('Path');
  out.Name = tag('Name');
  return out;
}

function parseMyItemListXml(xml) {
  const out = { items: [], totalResultsAvailable: null, totalResultsReturned: null, firstResultPosition: null };
  if (typeof xml !== 'string' || xml.length === 0) return out;
  const rsMatch = xml.match(/<ResultSet\s+([^>]*)>/);
  if (rsMatch) {
    const attrs = rsMatch[1];
    const totalMatch = attrs.match(/totalResultsAvailable\s*=\s*"(\d+)"/);
    const firstMatch = attrs.match(/firstResultPosition\s*=\s*"(\d+)"/);
    const returnedMatch = attrs.match(/totalResultsReturned\s*=\s*"(\d+)"/);
    if (totalMatch) out.totalResultsAvailable = parseInt(totalMatch[1], 10);
    if (firstMatch) out.firstResultPosition = parseInt(firstMatch[1], 10);
    if (returnedMatch) out.totalResultsReturned = parseInt(returnedMatch[1], 10);
  }
  const resultRegex = /<Result>([\s\S]*?)<\/Result>/g;
  let m;
  while ((m = resultRegex.exec(xml)) !== null) {
    const block = m[1];
    const codeMatch = block.match(/<ItemCode>([^<]*)<\/ItemCode>/);
    if (!codeMatch) continue;
    const item = { ItemCode: decodeXmlEntities(codeMatch[1]) };
    const nameMatch = block.match(/<Name>([^<]*)<\/Name>/);
    if (nameMatch) item.Name = decodeXmlEntities(nameMatch[1]);
    const subMatch = block.match(/<HasSubCode>([^<]*)<\/HasSubCode>/);
    if (subMatch) {
      const v = subMatch[1].trim();
      item.HasSubCode = v === '1' || v.toLowerCase() === 'true';
    }
    out.items.push(item);
  }
  // totalResultsReturned が XML 属性に無くても items.length で補う (RYS の strict validation 用)
  if (out.totalResultsReturned === null) out.totalResultsReturned = out.items.length;
  return out;
}

// multipart / binary body 用 (Phase E-5b で追加、 uploadItemImage が使う)
function readBodyBuffer(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

// ─── HTTPサーバー ───

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const pathname = url.pathname;

  // ─── ヘルスチェック（認証不要）───
  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK');
    return;
  }

  // ─── 認証チェック ───
  const secret = req.headers['x-proxy-secret'] || '';
  // rotation 期間中は PROXY_SECRET と PROXY_SECRET_NEXT 両方を許容する
  const acceptedSecrets = [PROXY_SECRET, PROXY_SECRET_NEXT].filter(Boolean);
  if (!acceptedSecrets.includes(secret)) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Forbidden' }));
    return;
  }

  try {
    // ═══════════════════════════════════════
    // au PAY Market ルート（/wmshopapi/...）
    // ═══════════════════════════════════════
    if (pathname.startsWith('/wmshopapi/')) {
      if (!AUPAY_API_KEY) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'AUPAY_API_KEY not configured' }));
        return;
      }

      const targetUrl = AUPAY_BASE + req.url;
      const response = await fetch(targetUrl, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${AUPAY_API_KEY}`,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
      });
      const data = await response.text();
      console.log(`[${ts()}] auPay ${req.url.slice(0, 80)} -> ${response.status} (${data.length} bytes)`);
      res.writeHead(response.status, { 'Content-Type': response.headers.get('content-type') || 'application/xml' });
      res.end(data);
      return;
    }

    // ═══════════════════════════════════════
    // Yahoo Shopping ルート（/yahoo/...）
    // ═══════════════════════════════════════

    if (pathname === '/yahoo/health') {
      const tokens = safeLoadTokens();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        hasTokens: !!tokens.access_token,
        tokenExpiry: tokens.expires_at ? new Date(tokens.expires_at).toISOString() : null,
        refreshTokenExpiresAt: tokens.refresh_token_expires_at || null,
        sellerId: YAHOO_SELLER_ID,
      }));
      return;
    }

    if (pathname === '/yahoo/auth-url') {
      const authUrl = `https://auth.login.yahoo.co.jp/yconnect/v2/authorization?response_type=code&client_id=${YAHOO_CLIENT_ID}&redirect_uri=${encodeURIComponent(YAHOO_REDIRECT_URI)}&scope=openid`;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ url: authUrl }));
      return;
    }

    if (pathname === '/yahoo/token/init' && req.method === 'POST') {
      const body = await readBody(req);
      const { code } = JSON.parse(body);
      if (!code) throw new Error('code が必要です');
      const tokens = await initTokenFromCode(code);
      console.log(`[${ts()}] Yahoo トークン初期化成功`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, expires_at: new Date(tokens.expires_at).toISOString() }));
      return;
    }

    if (pathname === '/yahoo/token/refresh' && req.method === 'GET') {
      await refreshAccessToken();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    if (pathname === '/yahoo/access-token' && req.method === 'POST') {
      // 専用 secret (X-Token-Mint-Secret) 必須。未設定なら 503 fail-closed (PROXY_SECRET 漏えい時の blast radius 隔離)
      const acceptedMintSecrets = [YAHOO_TOKEN_MINT_SECRET, YAHOO_TOKEN_MINT_SECRET_NEXT].filter(Boolean);
      if (acceptedMintSecrets.length === 0) {
        console.error(`[${ts()}] /yahoo/access-token: YAHOO_TOKEN_MINT_SECRET not configured`);
        res.writeHead(503, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
        res.end(JSON.stringify({ ok: false, error: 'mint endpoint disabled' }));
        return;
      }
      const mintSecret = req.headers['x-token-mint-secret'] || '';
      if (!acceptedMintSecrets.includes(mintSecret)) {
        res.writeHead(401, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
        res.end(JSON.stringify({ ok: false, error: 'mint secret invalid' }));
        return;
      }

      try {
        const meta = await getAccessTokenWithMeta();
        res.writeHead(200, {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-store',
        });
        res.end(JSON.stringify({
          ok: true,
          access_token: meta.access_token,
          token_type: 'Bearer',
          expires_at: new Date(meta.expires_at).toISOString(),
          refreshed: meta.refreshed,
        }));
        console.log(`[${ts()}] /yahoo/access-token ok refreshed=${meta.refreshed} expires_at=${new Date(meta.expires_at).toISOString()}`);
      } catch (e) {
        // upstream (Yahoo 起因) と local (ファイル/設定起因) を分離。レスポンス本文は秘匿のまま。
        console.error(`[${ts()}] /yahoo/access-token failed: name=${e.name} msg=${e.message}`);
        if (e instanceof TokenLocalError) {
          res.writeHead(500, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
          res.end(JSON.stringify({ ok: false, error: 'token config error' }));
        } else {
          // TokenUpstreamError + 未分類 (安全側で upstream 扱い)
          res.writeHead(502, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
          res.end(JSON.stringify({ ok: false, error: 'upstream refresh failed' }));
        }
      }
      return;
    }

    // 一般公開 API: 近傍商品検索 (カテゴリ AI 補完用)。GET 限定、appid 未設定なら 503。
    if (pathname === '/yahoo/item-search') {
      if (req.method !== 'GET') {
        res.writeHead(405, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Method Not Allowed' }));
        return;
      }
      if (!YAHOO_CLIENT_ID) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'YAHOO_CLIENT_ID (appid) not configured' }));
        return;
      }
      const query = url.searchParams.get('query') || '';
      if (!query) throw new Error('query が必要です');
      const results = Math.min(Math.max(parseInt(url.searchParams.get('results'), 10) || 20, 1), 50);
      const r = await yahooPublicGet('/V3/itemSearch', { query, results });
      res.writeHead(r.status, { 'Content-Type': r.contentType });
      res.end(r.body);
      return;
    }

    // 一般公開 API: カテゴリツリー取得 (category_id 指定、子カテゴリ含む)。GET 限定。
    if (pathname === '/yahoo/category-search') {
      if (req.method !== 'GET') {
        res.writeHead(405, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Method Not Allowed' }));
        return;
      }
      if (!YAHOO_CLIENT_ID) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'YAHOO_CLIENT_ID (appid) not configured' }));
        return;
      }
      const categoryId = url.searchParams.get('category_id') || '1';
      if (!/^\d+$/.test(categoryId)) throw new Error('category_id は数値で指定してください');
      const r = await yahooPublicGet('/V1/categorySearch', { category_id: categoryId });
      res.writeHead(r.status, { 'Content-Type': r.contentType });
      res.end(r.body);
      return;
    }

    if (pathname === '/yahoo/orderList') {
      const startDate = url.searchParams.get('startDate');
      const endDate = url.searchParams.get('endDate');
      if (!startDate || !endDate) throw new Error('startDate と endDate が必要です');
      console.log(`[${ts()}] Yahoo orderList: ${startDate} → ${endDate}`);
      const xml = await yahooOrderList(startDate, endDate);
      res.writeHead(200, { 'Content-Type': 'application/xml' });
      res.end(xml);
      return;
    }

    if (pathname === '/yahoo/orderInfo' && req.method === 'POST') {
      const body = await readBody(req);
      const { orderIds } = JSON.parse(body);
      if (!orderIds || !orderIds.length) throw new Error('orderIds が必要です');
      const results = [];
      for (let i = 0; i < orderIds.length; i++) {
        console.log(`[${ts()}] Yahoo orderInfo: ${orderIds[i]} (${i + 1}/${orderIds.length})`);
        const xml = await yahooOrderInfo(orderIds[i]);
        results.push({ orderId: orderIds[i], xml });
        if (i < orderIds.length - 1) await new Promise(r => setTimeout(r, 1100));
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, results }));
      return;
    }

    if (pathname === '/yahoo/orderInfo' && req.method === 'GET') {
      const orderId = url.searchParams.get('orderId');
      if (!orderId) throw new Error('orderId が必要です');
      console.log(`[${ts()}] Yahoo orderInfo: ${orderId}`);
      const xml = await yahooOrderInfo(orderId);
      res.writeHead(200, { 'Content-Type': 'application/xml' });
      res.end(xml);
      return;
    }

    // ─── Phase E-5b: Yahoo publish 系 endpoint (RYS = RakutenYahooSync 用) ───

    // GET /yahoo/getLeadTimeList?seller_id=xxx
    //   Yahoo getLeadTimeList API へ token + 署名付きで forward
    //   leads(発送日設定)マスタの id 一覧を取得 (RYS readiness preflight 用)
    //   注: getLeadTimeList は他 publish 系 (editItem / myItemList) と base が違う。
    //       publish 系: https://circus.shopping.yahooapis.jp/ShoppingWebService/V1/...
    //       lead time:  https://circus.shopping.yahooapis.jp/shopping/getLeadTimeList
    //       (ローカル RYS src/services/yahoo-lead-time.js の LEAD_TIME_ENDPOINT='/shopping/getLeadTimeList' と整合)
    if (pathname === '/yahoo/getLeadTimeList' && req.method === 'GET') {
      const sellerId = url.searchParams.get('seller_id') || YAHOO_SELLER_ID;
      console.log(`[${ts()}] Yahoo getLeadTimeList: seller=${sellerId}`);
      const leadTimeUrl = `https://circus.shopping.yahooapis.jp/shopping/getLeadTimeList?seller_id=${encodeURIComponent(sellerId)}`;
      const r = await callYahooAPIRaw('getLeadTimeList', {
        method: 'GET',
        body: null,
        rawUrl: leadTimeUrl,
      });
      res.writeHead(r.status, { 'Content-Type': 'application/xml' });
      res.end(r.body);
      return;
    }

    // POST /yahoo/editItem
    //   caller (RYS) が application/x-www-form-urlencoded で fields を投げる
    //   そのまま Yahoo editItem.xml に forward (token + 署名は proxy 側で付与)
    if (pathname === '/yahoo/editItem' && req.method === 'POST') {
      // Codex R1 軽微 1 対応: form-encoded のみ許可 (JSON 等の誤投入を早く検知)
      const contentType = req.headers['content-type'] || '';
      if (!contentType.startsWith('application/x-www-form-urlencoded')) {
        throw new Error('editItem は application/x-www-form-urlencoded である必要があります');
      }
      const formBody = await readBody(req);
      console.log(`[${ts()}] Yahoo editItem: body=${formBody.length}b`);
      const r = await callYahooAPIRaw('editItem', {
        method: 'POST',
        contentType,
        body: formBody,
      });
      res.writeHead(r.status, { 'Content-Type': 'application/xml' });
      res.end(r.body);
      return;
    }

    // POST /yahoo/my-item-list
    //   request body: { query: string, offset?: number, results?: number }
    //   Yahoo myItemList API (GET + query string、 start は 1-based) へ token + 署名付きで forward。
    //   レスポンス XML を JSON に変換して返す (RYS client 期待 contract):
    //     { ok, items: [{ItemCode, Name?, HasSubCode?}], totalResultsAvailable, totalResultsReturned, firstResultPosition }
    if (pathname === '/yahoo/my-item-list' && req.method === 'POST') {
      const raw = await readBody(req);
      let body;
      try { body = JSON.parse(raw); }
      catch (_) {
        throw new Error('my-item-list: invalid JSON body');
      }
      const queryParam = String(body.query || '').trim();
      const offset = Number.isInteger(body.offset) ? body.offset : 0;
      const results = Number.isInteger(body.results) ? body.results : 100;
      if (!queryParam) throw new Error('my-item-list: query is required (non-empty string)');
      if (offset < 0 || results < 1 || results > 100) {
        throw new Error('my-item-list: invalid offset/results (offset>=0, 1<=results<=100)');
      }
      const start = offset + 1; // RYS は 0-based offset、 Yahoo API は 1-based start
      const qs = new URLSearchParams({
        seller_id: YAHOO_SELLER_ID,
        query: queryParam,
        start: String(start),
        results: String(results),
      }).toString();
      console.log(`[${ts()}] Yahoo myItemList: q=${queryParam} start=${start} results=${results}`);
      const r = await callYahooAPIRaw('myItemList', {
        method: 'GET',
        body: null,
        queryString: qs,
      });
      if (r.status !== 200) {
        // 上流エラーはそのまま XML で返す (RYS は !== 200 を errored として扱う)
        res.writeHead(r.status, { 'Content-Type': 'application/xml' });
        res.end(r.body);
        return;
      }
      const parsed = parseMyItemListXml(r.body);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        items: parsed.items,
        totalResultsAvailable: parsed.totalResultsAvailable,
        totalResultsReturned: parsed.totalResultsReturned,
        firstResultPosition: parsed.firstResultPosition,
      }));
      return;
    }

    // POST /yahoo/get-item-detail
    //   Phase E-11-b: 既存 Yahoo 出品 1 件の category/path を取得する。
    //   request body: { itemCode: string }
    //   Yahoo itemInfo (getItemDetail) API へ token + 署名付きで forward。
    //   レスポンス XML を JSON に変換して返す:
    //     { ok: true, ItemCode, ProductCategory, Path, Name }
    if (pathname === '/yahoo/get-item-detail' && req.method === 'POST') {
      const raw = await readBody(req);
      let body;
      try { body = JSON.parse(raw); }
      catch (_) {
        throw new Error('get-item-detail: invalid JSON body');
      }
      const itemCode = String(body.itemCode || '').trim();
      if (!itemCode) throw new Error('get-item-detail: itemCode is required');
      const qs = new URLSearchParams({
        seller_id: YAHOO_SELLER_ID,
        item_code: itemCode,
      }).toString();
      console.log(`[${ts()}] Yahoo getItemDetail: item_code=${itemCode}`);
      const r = await callYahooAPIRaw('itemInfo', {
        method: 'GET',
        body: null,
        queryString: qs,
      });
      if (r.status !== 200) {
        // 上流エラーはそのまま XML で返す
        res.writeHead(r.status, { 'Content-Type': 'application/xml' });
        res.end(r.body);
        return;
      }
      const parsed = parseGetItemDetailXml(r.body);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        ItemCode: parsed.ItemCode,
        ProductCategory: parsed.ProductCategory,
        Path: parsed.Path,
        Name: parsed.Name,
      }));
      return;
    }

    // POST /yahoo/uploadItemImage
    //   caller (RYS) が multipart/form-data で binary + meta を投げる
    //   Content-Type に boundary が含まれるのでそのまま forward
    if (pathname === '/yahoo/uploadItemImage' && req.method === 'POST') {
      const contentType = req.headers['content-type'];
      if (!contentType || !contentType.startsWith('multipart/form-data')) {
        throw new Error('uploadItemImage は multipart/form-data である必要があります');
      }
      const buf = await readBodyBuffer(req);
      console.log(`[${ts()}] Yahoo uploadItemImage: ${buf.length}b multipart`);
      const r = await callYahooAPIRaw('uploadItemImage', {
        method: 'POST',
        contentType,
        body: buf,
      });
      res.writeHead(r.status, { 'Content-Type': 'application/xml' });
      res.end(r.body);
      return;
    }

    // ─── 404 ───
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));

  } catch (e) {
    console.error(`[${ts()}] ERROR: ${e.message}`);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: e.message }));
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`API Proxy running on port ${PORT} (au PAY + Yahoo Shopping)`);
  if (AUPAY_API_KEY) console.log(`  au PAY: API Key ${AUPAY_API_KEY.slice(0, 8)}...`);
  else console.log('  au PAY: API Key未設定（au PAYルートは無効）');
  if (YAHOO_CLIENT_ID && YAHOO_SELLER_ID) {
    console.log(`  Yahoo: Seller ${YAHOO_SELLER_ID}`);
    console.log(`  Yahoo: Public Key ${fs.existsSync(YAHOO_PUBLIC_KEY_PATH) ? 'Found (v' + YAHOO_SIGNATURE_VERSION + ')' : 'Not found'}`);
    // startup 列挙では safeLoadTokens を使う (壊れた token file で crashloop しない)
    const tokens = safeLoadTokens();
    console.log(`  Yahoo: Tokens ${tokens.access_token ? 'Loaded' : 'Not initialized'}`);
    console.log(`  Yahoo: /yahoo/access-token mint ${YAHOO_TOKEN_MINT_SECRET ? 'ENABLED' : 'disabled (set YAHOO_TOKEN_MINT_SECRET to enable)'}`);
  } else {
    console.log('  Yahoo: 設定不足（YAHOO_CLIENT_ID / YAHOO_SELLER_ID）');
  }
});
