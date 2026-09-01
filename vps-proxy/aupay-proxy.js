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

// --self-test は起動前に処理する。secret も外部通信も要らない純粋なパーサ検証なので、
// PROXY_SECRET の必須チェックより前に置く (手元でも VPS 上でも同じコードを確かめられるように)
if (process.argv.includes('--self-test')) { runSelfTest(); }

if (!PROXY_SECRET) { console.error('PROXY_SECRET is required'); process.exit(1); }

function ts() { return new Date().toISOString().slice(0, 19); }
// PR-Y-B: /yahoo/orderContact の純粋ロジック (テスト可能な別モジュール)
const yahooOrderContact = require('./yahoo-order-contact.js');

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
  const auth = res.headers.get('x-sws-authorize-status');
  if (auth && auth !== 'authorized') console.warn(`[${ts()}] ⚠️ X-SWS-Authorize-Status=${auth} (${endpoint}) 公開鍵認証に失敗。期限切れなら暗号鍵管理で再発行`);
  return await res.text();
}

/** callYahooAPI と同じ認証で、HTTP status と公開鍵認証ヘッダも返す (PR-Y-B: orderContact 用) */
async function callYahooAPIWithMeta(endpoint, xmlBody) {
  const accessToken = await getAccessToken();
  const url = `${YAHOO_API_BASE}/${endpoint}`;
  const headers = { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/xml; charset=utf-8' };
  if (fs.existsSync(YAHOO_PUBLIC_KEY_PATH)) {
    const publicKey = crypto.createPublicKey(fs.readFileSync(YAHOO_PUBLIC_KEY_PATH, 'utf-8'));
    const encrypted = crypto.publicEncrypt({ key: publicKey, padding: crypto.constants.RSA_PKCS1_PADDING },
      Buffer.from(`${YAHOO_SELLER_ID}:${Math.floor(Date.now() / 1000)}`, 'utf-8'));
    headers['X-sws-signature'] = encrypted.toString('base64');
    headers['X-sws-signature-version'] = YAHOO_SIGNATURE_VERSION;
  }
  const res = await fetch(url, { method: 'POST', headers, body: xmlBody });
  return { status: res.status, authorizeStatus: res.headers.get('x-sws-authorize-status'), retryAfter: res.headers.get('retry-after'), text: await res.text() };
}

// ─── PR-Y-B: /yahoo/orderContact — 注文 1 件の宛先を「保存せず」返す ───
// 約款第10条 (購入者PIIを保持しない) を守るため、プロキシは値をログ・ファイルに一切書かない。
// 直列化 (同時 1 件・1.1 秒間隔 = Yahoo の 1 req/秒目安) して他の orderInfo 呼び出しと競合させない
// Yahoo orderInfo 系 (orderInfo バッチ / GET / orderContact) は 1 本のキューで直列化し 1.1 秒間隔にする
// (Codex Y-B R1 Medium: orderContact だけ直列化しても orderInfo バッチと並走すれば Yahoo には近接リクエストが飛ぶ)
let yahooChain = Promise.resolve();
let lastYahooCallAt = 0;
function yahooSerialized(fn) {
  const run = async () => {
    const wait = 1100 - (Date.now() - lastYahooCallAt);
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastYahooCallAt = Date.now();
    return await fn();
  };
  const p = yahooChain.then(run, run);
  yahooChain = p.catch(() => {});
  return p;
}
function yahooOrderContactSerialized(orderId) {
  const xml = `<Req><Target><OrderId>${orderId}</OrderId><Field>${yahooOrderContact.CONTACT_FIELDS}</Field></Target><SellerId>${YAHOO_SELLER_ID}</SellerId></Req>`;
  return yahooSerialized(() => callYahooAPIWithMeta('orderInfo', xml));
}

// ─── Yahoo 問い合わせ管理API (circus REST。inquiry-hub 受信同期+返信送信用) ───
// Bearer token + 公開鍵署名 (X-sws-signature) 必須 (署名なしは px-04102 で401、Step 0実測)。
// 署名は callYahooAPI と同方式

function yahooCircusHeaders(accessToken) {
  const headers = { 'Authorization': `Bearer ${accessToken}` };
  try {
    if (fs.existsSync(YAHOO_PUBLIC_KEY_PATH)) {
      const publicKeyPem = fs.readFileSync(YAHOO_PUBLIC_KEY_PATH, 'utf-8');
      const publicKey = crypto.createPublicKey(publicKeyPem);
      const timestamp = Math.floor(Date.now() / 1000).toString();
      const encrypted = crypto.publicEncrypt(
        { key: publicKey, padding: crypto.constants.RSA_PKCS1_PADDING },
        Buffer.from(`${YAHOO_SELLER_ID}:${timestamp}`, 'utf-8')
      );
      headers['X-sws-signature'] = encrypted.toString('base64');
      headers['X-sws-signature-version'] = YAHOO_SIGNATURE_VERSION;
    }
  } catch (e) { console.log(`[${ts()}] 署名スキップ: ${e.message}`); }
  return headers;
}

async function yahooCircusGet(apiPath, params = {}) {
  const accessToken = await getAccessToken();
  const u = new URL(`${YAHOO_API_BASE}${apiPath}`);
  u.searchParams.set('sellerId', YAHOO_SELLER_ID);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') u.searchParams.set(k, String(v));
  }
  const res = await fetch(u, { headers: yahooCircusHeaders(accessToken), signal: AbortSignal.timeout(30000) });
  const body = await res.text();
  return { status: res.status, contentType: res.headers.get('content-type') || 'application/json', body };
}

// 添付ファイルの受信上限 (呼び出し元とは独立に、この中継自身も自衛する)
const YAHOO_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024;

// バイナリGET (問い合わせ添付ファイルの取得。投稿時の Content-Type のまま返る)。
// Content-Length は信用せず、読みながら実バイト数で打ち切る (巨大レスポンスでのメモリ枯渇防止)
async function yahooCircusGetBinary(apiPath, params = {}, maxBytes = YAHOO_ATTACHMENT_MAX_BYTES) {
  const accessToken = await getAccessToken();
  const u = new URL(`${YAHOO_API_BASE}${apiPath}`);
  u.searchParams.set('sellerId', YAHOO_SELLER_ID);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') u.searchParams.set(k, String(v));
  }
  const res = await fetch(u, { headers: yahooCircusHeaders(accessToken), signal: AbortSignal.timeout(30000) });
  const contentType = res.headers.get('content-type') || 'application/octet-stream';
  const declared = Number(res.headers.get('content-length'));
  if (Number.isFinite(declared) && declared > maxBytes) {
    return { status: 413, contentType: 'application/json', buffer: Buffer.from(JSON.stringify({ error: 'attachment too large' })), tooLarge: true };
  }
  const reader = res.body?.getReader?.();
  if (!reader) {
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > maxBytes) {
      return { status: 413, contentType: 'application/json', buffer: Buffer.from(JSON.stringify({ error: 'attachment too large' })), tooLarge: true };
    }
    return { status: res.status, contentType, buffer: buf };
  }
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxBytes) {
      await reader.cancel().catch(() => {});
      return { status: 413, contentType: 'application/json', buffer: Buffer.from(JSON.stringify({ error: 'attachment too large' })), tooLarge: true };
    }
    chunks.push(Buffer.from(value));
  }
  return { status: res.status, contentType, buffer: Buffer.concat(chunks, total) };
}

// POST (メッセージ投稿等)。⚠️リトライしない (送信系の再試行は二重投稿になる。結果不明の扱いは呼び出し元=outboxに委ねる)
async function yahooCircusPost(apiPath, queryParams = {}, jsonBody = {}, method = 'POST') {
  const accessToken = await getAccessToken();
  const u = new URL(`${YAHOO_API_BASE}${apiPath}`);
  for (const [k, v] of Object.entries(queryParams)) {
    if (v !== undefined && v !== null && v !== '') u.searchParams.set(k, String(v));
  }
  const headers = { ...yahooCircusHeaders(accessToken), 'Content-Type': 'application/json' };
  const res = await fetch(u, {
    method, headers, body: JSON.stringify(jsonBody), signal: AbortSignal.timeout(30000),
  });
  const body = await res.text();
  return { status: res.status, contentType: res.headers.get('content-type') || 'application/json', body };
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
  if (body !== null && contentType) headers['Content-Type'] = contentType;
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
    <Field>OrderId,OrderTime,LastUpdateTime,OrderStatus,PayStatus,ShipStatus,ShipDate,SocialGiftType,TotalPrice,PayCharge,ShipCharge,Discount,UsePoint,LineId,ItemId,Title,SubCode,UnitPrice,OriginalPrice,Quantity,ItemTaxRatio,CouponDiscount</Field>
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
 * Phase E-11-b hotfix: Yahoo getItem (商品詳細取得) の XML response から
 *   productCategory + path を抜き出す。
 *
 * 当初 itemInfo というエンドポイント名で実装したが、 Yahoo API は px-04400 (URL 不存在) を返した。
 * 設計書 v6 (AI_reference) を確認したところ正しいエンドポイントは getItem だった (GET /V1/getItem)。
 *
 * Response XML shape:
 *   <ProductCategory>NNN</ProductCategory>   (数値、 Yahoo カテゴリ ID)
 *   <PathList>
 *     <Path origFlag="1">メインパス</Path>    ← origFlag="1" がメイン path
 *     <Path>サブパス</Path>                    ← メイン以外の表示パス
 *   </PathList>
 *   <Name>...</Name>
 *
 *   <Path>直書き  ではなく <PathList> 内の <Path> なので、 古い実装の単純 tag マッチでは取れない。
 */
function parseGetItemDetailXml(xml) {
  const out = {
    ItemCode: null, ProductCategory: null, Path: null, Name: null,
    // 価格一括改定ツール (M0 2026-08-24) で追加。更新前の設定価格を API で読むために必要。
    // Price は「商品単位の設定価格」。バリエーション商品は SubCodes[] に sub_code 別価格が入る
    // (現行の出品運用ではサブコード別価格は使わず item 価格を継承する方針 — variation-resolver.js 参照)
    Price: null, SubCodes: [],
    // 発送まわり (価格一括改定ツール 2026-08-31 追加)。同じ商品でもモールで配送方法が違い、
    // それが売価差の理由になるため、画面で並べて見えるようにする
    Delivery: null, PostageSet: null, ShipWeight: null,
  };
  if (typeof xml !== 'string' || xml.length === 0) return out;
  // PR #322 で getItem 対応した時 path/name が null だった件:
  //   Yahoo getItem の <Path>/<Name> は CDATA wrap (`<![CDATA[...]]>`) で返ってくる。
  //   `[^<]*` だと CDATA の `<!` で停止して空文字を返してた。
  //   helper を CDATA / plain text 両方に対応させる。
  const unwrapCdata = (s) => {
    if (typeof s !== 'string') return null;
    const m = s.match(/<!\[CDATA\[([\s\S]*?)\]\]>/);
    return m ? m[1] : s;
  };
  const tag = (name) => {
    // multiline + non-greedy で開閉タグ間の全テキストを掴み、 CDATA を unwrap、 entity decode + trim
    const m = xml.match(new RegExp(`<${name}\\b[^>]*>([\\s\\S]*?)</${name}>`, 'i'));
    if (!m) return null;
    const inner = unwrapCdata(m[1]);
    return decodeXmlEntities(inner).trim();
  };
  out.ItemCode = tag('ItemCode');
  const pc = tag('ProductCategory');
  if (pc != null && pc !== '') {
    const n = parseInt(pc, 10);
    out.ProductCategory = Number.isFinite(n) ? n : null;
  }
  // path 抽出: <PathList> 内の origFlag="1" の <Path>...</Path> を優先、
  //   無ければ最初の <Path>...</Path> を採用。 CDATA wrap も unwrap する。
  out.Path = (() => {
    const listMatch = xml.match(/<PathList[^>]*>([\s\S]*?)<\/PathList>/i);
    const scope = listMatch ? listMatch[1] : xml;
    // origFlag="1" の <Path ...>VALUE</Path>
    const orig = scope.match(/<Path\b[^>]*\borigFlag\s*=\s*"1"[^>]*>([\s\S]*?)<\/Path>/i);
    if (orig) return decodeXmlEntities(unwrapCdata(orig[1])).trim();
    // 最初の <Path>...</Path> (origFlag 無し)
    const any = scope.match(/<Path\b[^>]*>([\s\S]*?)<\/Path>/i);
    if (any) return decodeXmlEntities(unwrapCdata(any[1])).trim();
    return null;
  })();
  out.Name = tag('Name');

  // --- 価格 ---
  // <Price> はバリエーション情報 (SubCodeInfo / Options) の中にも現れうるので、
  // 商品単位の価格を取るときはバリエーションブロックを除いた範囲から探す
  // (Path が <PathList> 内に限定して探しているのと同じ考え方)。
  // 🚨実測 (2026-08-31、合皮補修シート 0726-001802):
  //   <Result> 直下に <Price>698</Price> があり、その後ろに
  //   <Options>…</Options> と <SubCodes><SubCode code="0726-001802-BK" …><Price></Price>…
  //   が並ぶ。**SubCodes を除外しないと Price が13個** (商品1 + サブ12) になり、
  //   「1つでなければ読めない扱い」の安全弁に引っかかって商品価格まで null になっていた。
  const withoutVariationBlocks = xml
    .replace(/<SubCodes\b[\s\S]*?<\/SubCodes>/gi, '')
    .replace(/<SubCodeInfo\b[\s\S]*?<\/SubCodeInfo>/gi, '')
    .replace(/<Options\b[\s\S]*?<\/Options>/gi, '');
  // この値は「更新前価格の照合 (楽観ロック)」に使うので、読めない形は黙って数値化せず null にする。
  // parseInt だと "1080abc" が 1080 に、"1,2,3" が 123 に化けて安全確認が無効になるため、
  // カンマ・空白を除いたうえで全文が整数 (または .00 のような無害な小数) であることを検証する。
  const toIntPrice = (raw) => {
    if (typeof raw !== 'string') return null;
    const s = raw.replace(/\s/g, '');
    if (s === '') return null;
    // 末尾の .00 のような無害な小数部だけ許して切り離す (.99 のような端数は読めない扱い)
    const m = s.match(/^([\d,]+)(?:\.0+)?$/);
    if (!m) return null;
    const digits = m[1];
    // カンマは3桁区切りとしてのみ許容する。"1,2,3" を 123 と読むと照合が無意味になる
    const valid = digits.includes(',')
      ? /^([1-9]\d{0,2})(,\d{3})+$/.test(digits)
      : /^(0|[1-9]\d*)$/.test(digits);
    if (!valid) return null;
    const n = Number(digits.replace(/,/g, ''));
    return Number.isSafeInteger(n) ? n : null;
  };
  const priceIn = (scope, name = 'Price') => {
    // 同じスコープに Price が複数あるのは想定外の構造。取り違えるくらいなら読めない扱いにする (fail closed)
    const all = scope.match(new RegExp(`<${name}\\b[^>]*>([\\s\\S]*?)</${name}>`, 'gi')) || [];
    if (all.length !== 1) return null;
    const m = all[0].match(new RegExp(`<${name}\\b[^>]*>([\\s\\S]*?)</${name}>`, 'i'));
    return toIntPrice(decodeXmlEntities(unwrapCdata(m[1])).trim());
  };
  out.Price = priceIn(withoutVariationBlocks);

  // 発送まわり (商品単位)。SubCodes 内にも同名タグがあるので、除外済みの範囲から取る
  const textIn = (scope, name) => {
    const m = scope.match(new RegExp(`<${name}\\b[^>]*>([\\s\\S]*?)</${name}>`, 'i'));
    if (!m) return null;
    const v = decodeXmlEntities(unwrapCdata(m[1])).trim();
    return v === '' ? null : v;
  };
  out.Delivery = textIn(withoutVariationBlocks, 'Delivery');
  out.PostageSet = textIn(withoutVariationBlocks, 'PostageSet');
  out.ShipWeight = textIn(withoutVariationBlocks, 'ShipWeight');

  // サブコード別の価格。★実測の形 (2026-08-31):
  //   <SubCodes>
  //     <SubCode code="0726-001802-BK" quantity="22" stockClose="0">
  //       <Option name="カラー" value="ブラック(黒)"/> … <Price></Price>
  //     </SubCode> × 色数
  //   </SubCodes>
  // 個別商品コードは **要素の中身ではなく code 属性**。<Price> が空なら商品価格を継承する運用。
  // (旧実装は <SubCodeInfo> という存在しないタグを探していたため、カラバリが常に 0 件だった)
  const subBlocks = [
    ...(xml.match(/<SubCode\b[^>]*>[\s\S]*?<\/SubCode>/gi) || []),
    ...(xml.match(/<SubCode\b[^>]*\/>/gi) || []),          // 子要素なしの自己終了タグ
  ];
  for (const block of subBlocks) {
    const attr = block.match(/<SubCode\b[^>]*\bcode\s*=\s*"([^"]*)"/i)
      || block.match(/<SubCode\b[^>]*\bcode\s*=\s*'([^']*)'/i);
    let code = attr ? decodeXmlEntities(attr[1]).trim() : null;
    if (!code) {
      // 後方互換: 属性が無く要素の中身がコードの形 (旧実装の想定)
      const inner = block.match(/<SubCode\b[^>]*>([\s\S]*?)<\/SubCode>/i);
      const text = inner ? decodeXmlEntities(unwrapCdata(inner[1])).trim() : '';
      if (text && !text.includes('<')) code = text;
    }
    if (!code) continue;
    out.SubCodes.push({ SubCode: code, Price: priceIn(block) });
  }
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

// JSON body 用 (size cap 付き)。
//   Codex Phase E upload contract rewrite R1 H-1:
//   uploadItemImage の bufferBase64 上限を防御するため、 Content-Length 事前 check +
//   streaming chunk 累計 cap (超過後は drain して body は捨てる) で 413 を確実に返す。
const UPLOAD_JSON_BODY_MAX_BYTES = 4 * 1024 * 1024; // 2MB JPEG → base64 2.67MB + JSON overhead 余裕
function readJsonBodyWithCap(req, maxBytes = UPLOAD_JSON_BODY_MAX_BYTES) {
  return new Promise((resolve, reject) => {
    const cl = req.headers['content-length'];
    if (cl) {
      const n = Number(cl);
      if (Number.isFinite(n) && n > maxBytes) {
        return reject(Object.assign(new Error(`request body too large: ${n}B > ${maxBytes}B`), { httpStatus: 413 }));
      }
    }
    let total = 0;
    let rejected = false;
    const chunks = [];
    req.on('data', c => {
      total += c.length;
      if (total > maxBytes) {
        if (!rejected) {
          rejected = true;
          reject(Object.assign(new Error(`request body too large: > ${maxBytes}B`), { httpStatus: 413 }));
        }
        return; // drain: 残り chunk は捨てる (socket は閉じない → 413 response が届く)
      }
      if (!rejected) chunks.push(c);
    });
    req.on('end', () => {
      if (!rejected) resolve(Buffer.concat(chunks).toString('utf8'));
    });
    req.on('error', e => {
      if (!rejected) {
        rejected = true;
        reject(e);
      }
    });
  });
}

// uploadItemImage の画像命名規約 (bfaith-portal apps/rakuten-yahoo-sync/lib/yahoo-publish-proxy.js と同期):
//   メイン: {item_code}.jpg / サブ: {item_code}_{1..20}.jpg
//   item_code: [A-Za-z0-9_-] 1-80 chars
const UPLOAD_ITEM_CODE_RE = /^[A-Za-z0-9_-]{1,80}$/;
const UPLOAD_FILE_NAME_RE_GENERIC = /^[A-Za-z0-9_-]{1,80}(_(?:[1-9]|1[0-9]|20))?\.jpg$/;
function validateUploadFileName(fileName, itemCode) {
  if (typeof fileName !== 'string' || fileName.length === 0 || fileName.length > 100) {
    throw new Error(`fileName invalid length: ${fileName?.length}`);
  }
  if (fileName.includes('/') || fileName.includes('\\')) {
    throw new Error(`fileName must be basename: ${JSON.stringify(fileName)}`);
  }
  if (/[\x00-\x1F\x7F]/.test(fileName)) {
    throw new Error('fileName has control chars');
  }
  if (!UPLOAD_FILE_NAME_RE_GENERIC.test(fileName)) {
    throw new Error(`fileName format invalid: ${fileName}`);
  }
  if (!UPLOAD_ITEM_CODE_RE.test(itemCode)) {
    throw new Error(`itemCode format invalid: ${itemCode}`);
  }
  const expected = new RegExp(`^${itemCode}(?:_(?:[1-9]|1[0-9]|20))?\\.jpg$`);
  if (!expected.test(fileName)) {
    throw new Error(`fileName ${fileName} does not match itemCode ${itemCode}`);
  }
}

// uploadLibImage の画像命名規約 (Yahoo it-14061 修正 2026-06-27):
//   ストア内 library 画像。 uploadItemImage と別 API。
//   公式仕様: 255 byte 以内、 半角英数字 + - _ . のみ。
//   実運用: {item_code}_lib_{N}.jpg (1-20)。 path separator/制御文字は不可。
//   itemCode は監査ログ用 (Yahoo には送らない、 prefix 検証のみ)。
const UPLOAD_LIB_FILE_NAME_RE = /^[A-Za-z0-9_.-]{1,80}\.(?:jpe?g|png|gif)$/i;
function validateUploadLibFileName(fileName, itemCode) {
  if (typeof fileName !== 'string' || fileName.length === 0 || fileName.length > 100) {
    throw new Error(`fileName invalid length: ${fileName?.length}`);
  }
  if (fileName.includes('/') || fileName.includes('\\')) {
    throw new Error(`fileName must be basename: ${JSON.stringify(fileName)}`);
  }
  if (/[\x00-\x1F\x7F]/.test(fileName)) {
    throw new Error('fileName has control chars');
  }
  if (!UPLOAD_LIB_FILE_NAME_RE.test(fileName)) {
    throw new Error(`fileName format invalid (expected [A-Za-z0-9_.-]+\\.(jpg|png|gif)): ${fileName}`);
  }
  // itemCode prefix 検証 (任意): bfaith 側で `{itemCode}_lib_N.jpg` 命名で送るので prefix 一致を確認
  if (itemCode != null) {
    if (!UPLOAD_ITEM_CODE_RE.test(itemCode)) {
      throw new Error(`itemCode format invalid: ${itemCode}`);
    }
    if (!fileName.startsWith(`${itemCode}_lib_`)) {
      throw new Error(`fileName ${fileName} does not match itemCode ${itemCode} (expected ${itemCode}_lib_N.jpg)`);
    }
  }
}

// uploadLibImage の Yahoo response は <Status>OK</Status> のみで URL を返さない (Codex R2 critical)。
// 公式 URL pattern は `https://shopping.c.yimg.jp/lib/{store}/{fileName}` 固定なので、
// proxy 側で合成して bfaith に返す。
function buildLibImageUrl(fileName) {
  return `https://shopping.c.yimg.jp/lib/${encodeURIComponent(YAHOO_SELLER_ID)}/${encodeURIComponent(fileName)}`;
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

    // 問い合わせ管理API passthrough (inquiry-hub 受信同期 + yahoo-inquiry-alert 用。
    // read-onlyのみ、パラメータ許可リスト方式 — 指定されたものだけ検証して転送する)
    if (pathname === '/yahoo/externalTalkList' && req.method === 'GET') {
      const start = url.searchParams.get('start') || '1';
      const result = url.searchParams.get('result') || '20';
      if (!/^\d+$/.test(start) || Number(start) < 1) throw new Error('start は1以上の数値で指定してください');
      if (!/^\d+$/.test(result) || Number(result) < 1 || Number(result) > 20) throw new Error('result は1〜20で指定してください (API上限20)');
      const params = { start, result };
      // 絞り込み系 (yahoo-inquiry-alert が使う)。値は公式仕様の列挙値のみ許可
      const optional = {
        requestFilter: /^(answered|unanswered|completed)(,(answered|unanswered|completed))*$/,
        serviceType: /^(shp|auc)$/,
        sort: /^(userPostTime|sellerPostTime)$/,
        sortOrder: /^(asc|desc)$/,
      };
      for (const [name, re] of Object.entries(optional)) {
        const v = url.searchParams.get(name);
        if (v == null) continue;
        if (!re.test(v)) throw new Error(`${name} が不正です`);
        params[name] = v;
      }
      const filterLabel = params.requestFilter ? ` filter=${params.requestFilter}/${params.serviceType || '-'}` : '';
      const r = await yahooCircusGet('/externalTalkList', params);
      console.log(`[${ts()}] Yahoo externalTalkList start=${start}${filterLabel} -> ${r.status} (${r.body.length} bytes)`);
      res.writeHead(r.status, { 'Content-Type': r.contentType });
      res.end(r.body);
      return;
    }

    // メッセージ投稿 passthrough (inquiry-hub Step 5。変更系はこの1本のみ・厳重ガード)
    // 公式仕様: POST /externalTalkAdd?topicId=... body={sellerId, body(2000字上限)} → {topicid, messageid, postdate}
    if (pathname === '/yahoo/externalTalkAdd' && req.method === 'POST') {
      const reqBody = await readBody(req);
      let parsed;
      try { parsed = JSON.parse(reqBody); } catch {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'JSONボディが必要です' }));
        return;
      }
      if (typeof parsed.topicId !== 'string' || typeof parsed.message !== 'string') {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'topicId / message は文字列で指定してください' }));
        return;
      }
      const topicId = parsed.topicId.trim();
      const message = parsed.message;
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(topicId)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'topicId が不正です' }));
        return;
      }
      if (!message.trim() || message.length > 2000) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'message は1〜2000文字で指定してください (Yahoo!公式上限)' }));
        return;
      }
      const r = await yahooCircusPost('/externalTalkAdd', { topicId }, { sellerId: YAHOO_SELLER_ID, body: message });
      console.log(`[${ts()}] Yahoo externalTalkAdd ${topicId.slice(0, 12)}… -> ${r.status} (${r.body.length} bytes)`);
      res.writeHead(r.status, { 'Content-Type': r.contentType });
      res.end(r.body);
      return;
    }

    // 質問完了 passthrough (inquiry-hub「送信して回答完了」2026-08-26 スタッフ要望。変更系2本目)
    // 公式仕様: PUT /externalTalkComplete?topicId=... body={sellerId, completeConditionId?} → {status:'ok'}
    //   completeConditionId: 未指定/1=通常完了 (出店者の返信が1回以上ある場合のみ可) 2=電話 3=メール 4=同一質問 5=回答不要
    // inquiry-hub は返信投稿の直後にだけ呼ぶ (未指定=通常完了)。リトライしない (冪等だが上流の負荷を増やさない)
    if (pathname === '/yahoo/externalTalkComplete' && req.method === 'PUT') {
      const reqBody = await readBody(req);
      let parsed;
      try { parsed = JSON.parse(reqBody); } catch {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'JSONボディが必要です' }));
        return;
      }
      const topicId = typeof parsed.topicId === 'string' ? parsed.topicId.trim() : '';
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(topicId)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'topicId が不正です' }));
        return;
      }
      const cond = parsed.completeConditionId;
      if (cond !== undefined && cond !== null && !/^[1-5]$/.test(String(cond))) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'completeConditionId は 1〜5 か未指定 (Yahoo!公式)' }));
        return;
      }
      const body = { sellerId: YAHOO_SELLER_ID, ...(cond != null ? { completeConditionId: String(cond) } : {}) };
      const r = await yahooCircusPost('/externalTalkComplete', { topicId }, body, 'PUT');
      console.log(`[${ts()}] Yahoo externalTalkComplete ${topicId.slice(0, 12)}… -> ${r.status} (${r.body.length} bytes)`);
      res.writeHead(r.status, { 'Content-Type': r.contentType });
      res.end(r.body);
      return;
    }

    if (pathname === '/yahoo/externalTalkDetail' && req.method === 'GET') {
      const topicId = url.searchParams.get('topicId') || '';
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(topicId)) throw new Error('topicId が不正です');
      const r = await yahooCircusGet('/externalTalkDetail', { topicId });
      console.log(`[${ts()}] Yahoo externalTalkDetail ${topicId.slice(0, 12)}… -> ${r.status} (${r.body.length} bytes)`);
      res.writeHead(r.status, { 'Content-Type': r.contentType });
      res.end(r.body);
      return;
    }

    // 添付ファイル取得 passthrough (inquiry-hub 添付表示。2026-08-02)
    // 公式: GET /externalTalkFileDownload?key=<objectKey>&sellerId=... → 投稿時のContent-Typeでバイナリ応答
    // read-only。key は externalTalkDetail の fileList[].objectKey をそのまま渡す
    if (pathname === '/yahoo/externalTalkFile' && req.method === 'GET') {
      const key = url.searchParams.get('key') || '';
      if (!key || key.length > 512) throw new Error('key (objectKey) が不正です');
      const r = await yahooCircusGetBinary('/externalTalkFileDownload', { key });
      console.log(`[${ts()}] Yahoo externalTalkFileDownload ${key.slice(0, 16)}… -> ${r.status} (${r.buffer.length} bytes)`);
      res.writeHead(r.status, {
        'Content-Type': r.contentType,
        'Content-Length': String(r.buffer.length),
        'X-Content-Type-Options': 'nosniff',
      });
      res.end(r.buffer);
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
      // PR-Y-B (Codex R1 High): orderId は XML に埋め込むため形式検証 (注入で Field を差し替えられると PII が混入する)
      const badId = orderIds.find((id) => !yahooOrderContact.isValidOrderId(id));
      if (badId !== undefined) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'invalid_order_id' }));
        return;
      }
      const results = [];
      for (let i = 0; i < orderIds.length; i++) {
        console.log(`[${ts()}] Yahoo orderInfo: ${orderIds[i]} (${i + 1}/${orderIds.length})`);
        const xml = await yahooSerialized(() => yahooOrderInfo(orderIds[i])); // 共通レートリミッタ (1.1秒間隔) — orderContact と共有
        results.push({ orderId: orderIds[i], xml });
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, results }));
      return;
    }

    // PR-Y-B: POST /yahoo/orderContact { orderId } → { ok, contact: { orderId, orderStatus, shipStatus, shipDate, socialGiftType, email } }
    // ログには orderId と結果コードだけ (メールアドレス・レスポンス本文は出さない)
    if (pathname === '/yahoo/orderContact' && req.method === 'POST') {
      let orderId;
      try { ({ orderId } = JSON.parse(await readBody(req))); } catch { orderId = null; }
      if (!yahooOrderContact.isValidOrderId(orderId)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'invalid_order_id' }));
        return;
      }
      let meta;
      try {
        meta = await yahooOrderContactSerialized(orderId);
      } catch (e) {
        console.log(`[${ts()}] Yahoo orderContact ${orderId}: upstream failure (${String(e.message).slice(0, 60)})`);
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'upstream_failure' }));
        return;
      }
      if (meta.status === 429) {
        console.log(`[${ts()}] Yahoo orderContact ${orderId}: 429`);
        res.writeHead(429, { 'Content-Type': 'application/json', ...(meta.retryAfter ? { 'Retry-After': meta.retryAfter } : {}) });
        res.end(JSON.stringify({ ok: false, error: 'rate_limited' }));
        return;
      }
      if (!yahooOrderContact.authorizeStatusOk(meta.authorizeStatus)) {
        // HTTP 200 でも公開鍵認証失敗はここで止める (Yahoo 公式: 認証結果はヘッダにのみ出る)
        console.log(`[${ts()}] Yahoo orderContact ${orderId}: public key auth ${meta.authorizeStatus || 'none'}`);
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'public_key_auth_failed', authorizeStatus: meta.authorizeStatus || 'none' }));
        return;
      }
      let parsed;
      try {
        parsed = yahooOrderContact.parseOrderContactXml(meta.text);
      } catch (e) {
        // PII を含む本文を共通例外ログに近づけない (Codex Y-B R1 Medium)
        console.log(`[${ts()}] Yahoo orderContact ${orderId}: parse_failed`);
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'parse_failed' }));
        return;
      }
      if (!parsed.ok) {
        console.log(`[${ts()}] Yahoo orderContact ${orderId}: ${parsed.error} ${parsed.code || ''} (http ${meta.status})`);
        res.writeHead(meta.status >= 500 ? 502 : 404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: parsed.error, code: parsed.code || null }));
        return;
      }
      if (parsed.contact.orderId !== orderId) {
        // 取り違え防止 (Codex Y-B R2 High): 要求した注文IDと違う注文の宛先は返さない
        console.log(`[${ts()}] Yahoo orderContact ${orderId}: order_id_mismatch`);
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'order_id_mismatch' }));
        return;
      }
      console.log(`[${ts()}] Yahoo orderContact ${orderId}: ok ship=${parsed.contact.shipStatus} gift=${parsed.contact.socialGiftType} email=${parsed.contact.email ? 'present' : 'empty'}`);
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      res.end(JSON.stringify({ ok: true, contact: parsed.contact }));
      return;
    }

    if (pathname === '/yahoo/orderInfo' && req.method === 'GET') {
      const orderId = url.searchParams.get('orderId');
      if (!yahooOrderContact.isValidOrderId(orderId)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'invalid_order_id' }));
        return;
      }
      console.log(`[${ts()}] Yahoo orderInfo: ${orderId}`);
      const xml = await yahooSerialized(() => yahooOrderInfo(orderId));
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
      // ★seller_id は VPS の env を正とする (呼び出し側が付けた値は捨てる)。
      //   別の店に書き込む経路を作らない。同じ店なので既存の呼び出し側の挙動は変わらない
      // ★seller_id は VPS の env を正とする。空なら送らない (空の店に書き込む事故を作らない)
      if (!YAHOO_SELLER_ID) throw new Error('editItem: YAHOO_SELLER_ID が未設定のため送信しません');
      const { body: editBody, fields: editFields, itemCode: editItemCode, mismatch } =
        withSellerId(formBody, YAHOO_SELLER_ID);
      // 呼び出し側が別の店を指定していた = どこかの設定が食い違っている。黙って書き換えず止める
      if (mismatch) {
        throw new Error(`editItem: 呼び出し側の seller_id (${mismatch}) が VPS の設定と違います`);
      }
      console.log(`[${ts()}] Yahoo editItem: item_code=${editItemCode || '(なし)'} fields=${editFields.join(',')} body=${editBody.length}b`);
      const r = await callYahooAPIRaw('editItem', {
        method: 'POST',
        contentType,
        body: editBody,
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
    //     { ok: true, ItemCode, ProductCategory, Path, Name, Price, SubCodes:[{SubCode,Price}] }
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
      console.log(`[${ts()}] Yahoo getItem: item_code=${itemCode}`);
      const r = await callYahooAPIRaw('getItem', {
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
      // ★raw=true: 応答XMLをそのまま返す。editItem が「送った項目だけ変える」のか
      //   「送らなかった項目を消す」のかは、前後の全項目を突き合わせないと分からない (M0 検証用)。
      //   read-only・secret 必須・商品カタログなので個人情報は含まない
      if (body.raw === true) {
        // ★生XMLを返すのは **検証用の商品コード (zz-) だけ**。
        //   本番カタログの応答をそのまま外に出す経路を作らない (Codex R1)
        if (!/^zz-/i.test(itemCode)) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: 'raw は検証用商品 (zz- で始まる商品コード) でのみ使えます' }));
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, itemCode, xml: String(r.body), length: String(r.body).length }));
        return;
      }
      const parsed = parseGetItemDetailXml(r.body);
      // 調査用: body.debugRaw=true で、応答XMLの「タグの出方」と先頭部分を返す。
      // 商品カタログの構造を知らないとパーサを直せないため (PII は無い / secret 必須 / read-only)。
      // 価格や在庫を書き換える経路ではない
      const debug = body.debugRaw === true ? (() => {
        const counts = {};
        for (const m of String(r.body).matchAll(/<([A-Za-z][\w.]*)\b/g)) {
          counts[m[1]] = (counts[m[1]] || 0) + 1;
        }
        const body = String(r.body);
        const sub = body.match(/<SubCodes>[\s\S]{0,1800}/i);
        const opt = body.match(/<Options>[\s\S]{0,600}/i);
        return {
          tagCounts: counts, head: body.slice(0, 1200), length: body.length,
          subCodesBlock: sub ? sub[0] : null,
          optionsBlock: opt ? opt[0] : null,
        };
      })() : undefined;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        ItemCode: parsed.ItemCode,
        ProductCategory: parsed.ProductCategory,
        Path: parsed.Path,
        Name: parsed.Name,
        // 価格一括改定ツール向け (2026-08-24 追加)
        Price: parsed.Price,
        SubCodes: parsed.SubCodes,
        // 発送まわり (2026-08-31 追加)。モールごとの配送方法を画面で見比べるため
        Delivery: parsed.Delivery,
        PostageSet: parsed.PostageSet,
        ShipWeight: parsed.ShipWeight,
        debug,
      }));
      return;
    }

    // POST /yahoo/uploadItemImage
    //   caller (RYS bfaith-portal) は JSON {fileName, itemCode, bufferBase64} で送る。
    //   VPS proxy 側で旧 RakutenYahooSync (Downloads/RakutenYahooSync/src/services/yahoo-api.js:250)
    //   と同じ Yahoo 公式 contract に再構築:
    //     - URL: ?seller_id=xxx  (URL query)
    //     - form: `file` 単数のみ + Blob(image/jpeg) + filename
    //     - headers: Authorization のみ (Content-Type は undici が boundary 付きで自動付与)
    //   旧透過 forward は余計な field (seller_id/item_code/file_name) を Yahoo に送ってしまい
    //   HTTP 400 になっていた (2026-06-25/26 smoke で連続失敗)。
    if (pathname === '/yahoo/uploadItemImage' && req.method === 'POST') {
      const ct = (req.headers['content-type'] || '').toLowerCase();
      if (!ct.includes('application/json')) {
        res.writeHead(415, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'uploadItemImage requires application/json' }));
        return;
      }

      let rawBody;
      try {
        rawBody = await readJsonBodyWithCap(req);
      } catch (e) {
        const code = e.httpStatus || 500;
        console.error(`[${ts()}] uploadItemImage body read error: ${e.message}`);
        res.writeHead(code, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
        return;
      }

      let payload;
      try { payload = JSON.parse(rawBody); }
      catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `invalid JSON: ${e.message}` }));
        return;
      }

      const { fileName, bufferBase64, itemCode } = payload || {};
      if (!fileName || !bufferBase64 || !itemCode) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'fileName, itemCode, bufferBase64 are required' }));
        return;
      }
      // type guard: Buffer.from(non-string, 'base64') は環境により TypeError → 外側 catch 500 化を避ける
      if (typeof fileName !== 'string' || typeof bufferBase64 !== 'string' || typeof itemCode !== 'string') {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'fileName, itemCode, bufferBase64 must be string' }));
        return;
      }
      try {
        validateUploadFileName(fileName, itemCode);
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
        return;
      }

      const buf = Buffer.from(bufferBase64, 'base64');
      if (buf.length < 4 || buf[0] !== 0xFF || buf[1] !== 0xD8) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `invalid JPEG magic: ${buf.slice(0, 4).toString('hex')}` }));
        return;
      }
      if (buf.length >= 2 * 1024 * 1024) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `image too large: ${buf.length}B (>= 2MB)` }));
        return;
      }

      // 旧 RakutenYahooSync yahoo-api.js:250 と同じ contract で再構築:
      //   form.append('file', Blob(image/jpeg), fileName)  だけ
      //   URL ?seller_id=xxx
      //   Content-Type は undici に boundary 付きで自動付与させる (contentType: null)
      const form = new FormData();
      form.append('file', new Blob([buf], { type: 'image/jpeg' }), fileName);

      console.log(`[${ts()}] Yahoo uploadItemImage: itemCode=${itemCode} file=${fileName} ${buf.length}B (json→multipart, seller_id in URL)`);
      const r = await callYahooAPIRaw('uploadItemImage', {
        method: 'POST',
        contentType: null,
        body: form,
        queryString: `seller_id=${encodeURIComponent(YAHOO_SELLER_ID)}`,
      });
      console.log(`[${ts()}] Yahoo uploadItemImage response: status=${r.status} body=${String(r.body).substring(0, 300)}`);
      res.writeHead(r.status, { 'Content-Type': 'application/xml' });
      res.end(r.body);
      return;
    }

    // POST /yahoo/uploadLibImage  (Yahoo it-14061 修正 2026-06-27)
    //   caller (RYS bfaith-portal) は JSON {fileName, itemCode, bufferBase64} で送る。
    //   uploadItemImage と別の API で、 「追加画像 (lib)」 用。 additional1/sp_additional の
    //   <img src> に貼れる画像をここで upload する。
    //   contract:
    //     - URL: ${apiBase}/uploadLibImage?seller_id=xxx
    //     - form: `file` 単数 + Blob + filename + (optional) directory
    //     - headers: Authorization + 署名 (uploadItemImage と同じ)
    //   response は <Status>OK</Status> のみで <Url> を返さないので、 proxy 側で
    //   `https://shopping.c.yimg.jp/lib/{store}/{fileName}` を合成して bfaith に返す
    //   (公式仕様、 Codex R2 critical)。
    if (pathname === '/yahoo/uploadLibImage' && req.method === 'POST') {
      const ct = (req.headers['content-type'] || '').toLowerCase();
      if (!ct.includes('application/json')) {
        res.writeHead(415, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'uploadLibImage requires application/json' }));
        return;
      }

      let rawBody;
      try {
        rawBody = await readJsonBodyWithCap(req);
      } catch (e) {
        const code = e.httpStatus || 500;
        console.error(`[${ts()}] uploadLibImage body read error: ${e.message}`);
        res.writeHead(code, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
        return;
      }

      let payload;
      try { payload = JSON.parse(rawBody); }
      catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `invalid JSON: ${e.message}` }));
        return;
      }

      const { fileName, bufferBase64, itemCode } = payload || {};
      if (!fileName || !bufferBase64) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'fileName, bufferBase64 are required' }));
        return;
      }
      if (typeof fileName !== 'string' || typeof bufferBase64 !== 'string' ||
          (itemCode != null && typeof itemCode !== 'string')) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'fileName, bufferBase64 must be string' }));
        return;
      }
      try {
        validateUploadLibFileName(fileName, itemCode);
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
        return;
      }

      const buf = Buffer.from(bufferBase64, 'base64');
      // JPEG/PNG/GIF magic check
      const head = buf.slice(0, 4).toString('hex');
      const isJpeg = head.startsWith('ffd8');
      const isPng = head.startsWith('89504e47');
      const isGif = head.startsWith('474946');
      if (!isJpeg && !isPng && !isGif) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `invalid image magic: ${head}` }));
        return;
      }
      // 2MB cap (公式: 2022/5 以降 2MB、 以前は 500KB だったので店舗により im-06001 が出る可能性あり、
      // その場合は bfaith 側で更に圧縮して retry)
      if (buf.length >= 2 * 1024 * 1024) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: `image too large: ${buf.length}B (>= 2MB)` }));
        return;
      }

      const mimeType = isJpeg ? 'image/jpeg' : (isPng ? 'image/png' : 'image/gif');
      const form = new FormData();
      form.append('file', new Blob([buf], { type: mimeType }), fileName);

      console.log(`[${ts()}] Yahoo uploadLibImage: itemCode=${itemCode || '?'} file=${fileName} ${buf.length}B mime=${mimeType}`);
      const r = await callYahooAPIRaw('uploadLibImage', {
        method: 'POST',
        contentType: null,
        body: form,
        queryString: `seller_id=${encodeURIComponent(YAHOO_SELLER_ID)}`,
      });
      console.log(`[${ts()}] Yahoo uploadLibImage response: status=${r.status} body=${String(r.body).substring(0, 300)}`);

      // R2 critical: response から URL を取れないので、 上流が 200 (= <Status>OK</Status> 想定) なら
      // proxy 側で合成 URL を json body で返す。 200 でなければ Yahoo XML をそのまま forward。
      if (r.status === 200) {
        const yahooUrl = buildLibImageUrl(fileName);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, yahooUrl, body: r.body }));
        return;
      }
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

/**
 * editItem の form body の seller_id を、VPS が持っている値に差し替える。
 * ★呼び出し側が付けた seller_id は捨てる (別の店に書き込む経路を作らない)。
 *   ただし呼び出し側が **別の店** を指定していたら mismatch として返す。
 *   黙って書き換えると、設定の食い違いに気づけないまま別の店の商品を触りうる。
 * @returns {{body:string, fields:string[], itemCode:string|null, mismatch:string|null}}
 */
function withSellerId(formBody, sellerId) {
  const params = new URLSearchParams(String(formBody || ''));
  const given = params.getAll('seller_id').map((v) => String(v).trim()).filter(Boolean);
  const mismatch = given.find((v) => v !== String(sellerId || '').trim()) || null;
  params.delete('seller_id');
  const fields = [...new Set([...params.keys()])];
  const itemCode = params.get('item_code');
  params.append('seller_id', String(sellerId || ''));
  return { body: params.toString(), fields, itemCode, mismatch };
}

// ─── self-test ───
// `node aupay-proxy.js --self-test` でパーサだけを検証する (サーバは起動しない)。
// 外部通信も secret も要らないので、デプロイ前の手元でも VPS 上でも同じコードを確かめられる。
// 呼び出しはファイル冒頭 (PROXY_SECRET チェックの前)。function 宣言なのでホイスティングで届く。
function runSelfTest() {
  let failed = 0;
  const check = (label, actual, expected) => {
    const a = JSON.stringify(actual), e = JSON.stringify(expected);
    if (a === e) { console.log(`  ok   ${label}`); return; }
    failed++;
    console.log(`  FAIL ${label}\n       expected: ${e}\n       actual  : ${a}`);
  };

  console.log('parseGetItemDetailXml:');
  // 1) バリエーション無し。Price は商品直下
  const plain = `<?xml version="1.0"?><ResultSet><Result><ItemCode><![CDATA[abc-001]]></ItemCode>
    <Name><![CDATA[テスト商品]]></Name><Price>1080</Price>
    <ProductCategory>1815</ProductCategory>
    <PathList><Path origFlag="1"><![CDATA[店:カテゴリ]]></Path></PathList></Result></ResultSet>`;
  const r1 = parseGetItemDetailXml(plain);
  check('plain: Price', r1.Price, 1080);
  check('plain: Name', r1.Name, 'テスト商品');
  check('plain: ItemCode', r1.ItemCode, 'abc-001');
  check('plain: SubCodes', r1.SubCodes, []);

  // 2) バリエーションあり (SKU別に価格を持つ形)。商品価格がサブの価格に引っ張られないこと。
  //    ※旧テストは M0 時点で想像した <SubCodeInfo> 構造だった。実測の形 (code 属性 + <SubCodes> 内) に直した
  const withSub = `<ResultSet><Result><ItemCode>v-001</ItemCode><Name>バリ商品</Name>
    <Price>2000</Price>
    <SubCodes>
      <SubCode code="v-001-a" quantity="1"><Price>2100</Price></SubCode>
      <SubCode code="v-001-b" quantity="2"><Price>2200</Price></SubCode>
    </SubCodes>
    </Result></ResultSet>`;
  const r2 = parseGetItemDetailXml(withSub);
  check('variation: 商品Price', r2.Price, 2000);
  check('variation: SubCodes', r2.SubCodes, [
    { SubCode: 'v-001-a', Price: 2100 },
    { SubCode: 'v-001-b', Price: 2200 },
  ]);

  // 2b) ★実測の形 (2026-08-31 合皮補修シート): SubCodes/SubCode の code 属性 + 空 Price (商品価格を継承)
  const real = `<?xml version="1.0" encoding="UTF-8"?><ResultSet totalResultsReturned="1"><Result>
    <ItemCode>0726-001802</ItemCode>
    <Name><![CDATA[合皮 補修 シート]]></Name>
    <OriginalPrice></OriginalPrice><Price>698</Price><SalePrice></SalePrice>
    <Options><Option type="1" name="カラー" specId="">
      <Value specValue="" name="ブラック(黒)"/><Value specValue="" name="オフホワイト"/>
    </Option></Options>
    <SubCodes>
      <SubCode code="0726-001802-BK" quantity="22" stockClose="0">
        <Option name="カラー" value="ブラック(黒)"/><Price></Price><PostageSet></PostageSet>
      </SubCode>
      <SubCode code="0726-001802-OW" quantity="78" stockClose="0">
        <Option name="カラー" value="オフホワイト"/><Price></Price><PostageSet></PostageSet>
      </SubCode>
    </SubCodes>
    </Result></ResultSet>`;
  const rr = parseGetItemDetailXml(real);
  check('実測形: 商品Price (SubCodes を除外しないと13個になって null になる)', rr.Price, 698);
  check('実測形: 個別商品コードは code 属性から取る', rr.SubCodes, [
    { SubCode: '0726-001802-BK', Price: null },
    { SubCode: '0726-001802-OW', Price: null },
  ]);
  check('実測形: ItemCode', rr.ItemCode, '0726-001802');

  // 3) サブの塊が商品Price より前にあっても取り違えない
  const subFirst = `<ResultSet><Result><ItemCode>v-002</ItemCode>
    <SubCodes><SubCode code="v-002-a"><Price>500</Price></SubCode></SubCodes>
    <Price>900</Price><Name>順序違い</Name></Result></ResultSet>`;
  check('variation(順序違い): 商品Price', parseGetItemDetailXml(subFirst).Price, 900);

  // 4) 表記ゆれ (カンマ・小数・空白) と欠落
  check('カンマ表記', parseGetItemDetailXml('<Result><Price>1,080</Price></Result>').Price, 1080);
  check('小数表記(.00)', parseGetItemDetailXml('<Result><Price>1080.00</Price></Result>').Price, 1080);
  check('0円', parseGetItemDetailXml('<Result><Price>0</Price></Result>').Price, 0);
  check('Price欠落', parseGetItemDetailXml('<Result><Name>x</Name></Result>').Price, null);
  check('空Price', parseGetItemDetailXml('<Result><Price></Price></Result>').Price, null);
  check('空XML', parseGetItemDetailXml('').Price, null);

  // 5) 読めない形は「それらしい数値」に化けさせず null にする (照合に使う値なので fail closed)
  check('末尾ゴミ', parseGetItemDetailXml('<Result><Price>1080abc</Price></Result>').Price, null);
  check('区切り誤り', parseGetItemDetailXml('<Result><Price>1,2,3</Price></Result>').Price, null);
  check('区切り誤り2', parseGetItemDetailXml('<Result><Price>12,34</Price></Result>').Price, null);
  check('大きい桁の区切り', parseGetItemDetailXml('<Result><Price>1,234,567</Price></Result>').Price, 1234567);
  check('端数あり小数', parseGetItemDetailXml('<Result><Price>1080.99</Price></Result>').Price, null);
  check('負数', parseGetItemDetailXml('<Result><Price>-100</Price></Result>').Price, null);
  check('先頭ゼロ', parseGetItemDetailXml('<Result><Price>0080</Price></Result>').Price, null);
  check('全角', parseGetItemDetailXml('<Result><Price>１０８０</Price></Result>').Price, null);
  // 同一スコープに Price が複数 = 想定外の構造。取り違えるより読めない扱いにする
  check('Price重複', parseGetItemDetailXml('<Result><Price>100</Price><Price>200</Price></Result>').Price, null);

  // 7) editItem の seller_id は VPS の値が正 (呼び出し側の値は捨てる)
  check('seller_id: 呼び出し側の値を捨てる',
    new URLSearchParams(withSellerId('seller_id=other&item_code=zz-1&price=100', 'mystore').body).getAll('seller_id'),
    ['mystore']);
  check('seller_id: 他の項目はそのまま',
    withSellerId('item_code=zz-1&price=100', 'mystore').fields, ['item_code', 'price']);
  check('seller_id: 複数指定されても1つに',
    new URLSearchParams(withSellerId('seller_id=a&seller_id=b&item_code=zz-1', 'mystore').body).getAll('seller_id'),
    ['mystore']);
  check('seller_id: 日本語の値が壊れない',
    new URLSearchParams(withSellerId('item_code=zz-1&name=' + encodeURIComponent('テスト商品'), 'mystore').body).get('name'),
    'テスト商品');
  check('seller_id: 同じ店なら mismatch にしない',
    withSellerId('seller_id=mystore&item_code=zz-1', 'mystore').mismatch, null);
  check('seller_id: 別の店を指定されたら mismatch',
    withSellerId('seller_id=otherstore&item_code=zz-1', 'mystore').mismatch, 'otherstore');
  check('seller_id: 未指定なら mismatch にしない',
    withSellerId('item_code=zz-1', 'mystore').mismatch, null);
  check('editItem: item_code をログ用に取り出す',
    withSellerId('item_code=zz-yahoo-m0-0901&price=100', 'mystore').itemCode, 'zz-yahoo-m0-0901');

  // 6) 既存の挙動が壊れていないこと (Path は PathList 内の origFlag=1 優先)
  const paths = `<Result><PathList><Path>その他</Path><Path origFlag="1">本命</Path></PathList></Result>`;
  check('Path: origFlag優先', parseGetItemDetailXml(paths).Path, '本命');

  console.log(failed === 0 ? '\n✅ self-test 全件 pass' : `\n❌ ${failed} 件 FAIL`);
  process.exit(failed === 0 ? 0 : 1);
}

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
