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
 *   GET  http://133.167.122.198:8080/yahoo/auth-url
 *   Header: X-Proxy-Secret
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PROXY_PORT || 8080;
const PROXY_SECRET = process.env.PROXY_SECRET || '';

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
const YAHOO_REDIRECT_URI = process.env.YAHOO_REDIRECT_URI || 'https://b-faith.biz';
const TOKEN_FILE = path.join(__dirname, 'yahoo-tokens.json');

if (!PROXY_SECRET) { console.error('PROXY_SECRET is required'); process.exit(1); }

function ts() { return new Date().toISOString().slice(0, 19); }

// ─── Yahoo トークン管理 ───

function loadTokens() {
  try { return JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf-8')); }
  catch { return { access_token: '', refresh_token: '', expires_at: 0 }; }
}

function saveTokens(tokens) {
  fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokens, null, 2));
}

async function refreshAccessToken() {
  const tokens = loadTokens();
  if (!tokens.refresh_token) throw new Error('refresh_token がありません。/yahoo/token/init で初期化してください');

  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: tokens.refresh_token,
    client_id: YAHOO_CLIENT_ID,
    client_secret: YAHOO_CLIENT_SECRET,
  });

  const res = await fetch(YAHOO_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });
  const data = await res.json();
  if (data.error) throw new Error(`トークンリフレッシュ失敗: ${data.error} - ${data.error_description || ''}`);

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
  saveTokens(updated);
  console.log(`[${ts()}] Yahoo トークンリフレッシュ成功`);
  return updated.access_token;
}

async function getAccessToken() {
  const tokens = loadTokens();
  if (tokens.access_token && tokens.expires_at > Date.now()) return tokens.access_token;
  return await refreshAccessToken();
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

async function yahooOrderInfo(orderId) {
  const xml = `<Req>
  <Target>
    <OrderId>${orderId}</OrderId>
    <IsGetOrderDetail>true</IsGetOrderDetail>
  </Target>
  <SellerId>${YAHOO_SELLER_ID}</SellerId>
  <Field>OrderId,OrderTime,LastUpdateTime,OrderStatus,PayStatus,ShipStatus,TotalPrice,PayCharge,ShipCharge,Discount,UsePoint,Item.LineId,Item.ItemId,Item.Title,Item.SubCode,Item.UnitPrice,Item.OriginalPrice,Item.Quantity,Item.ItemTaxRatio,Item.CouponDiscount</Field>
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
  if (secret !== PROXY_SECRET) {
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
      const tokens = loadTokens();
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

    if (pathname === '/yahoo/token/refresh') {
      await refreshAccessToken();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
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
    const tokens = loadTokens();
    console.log(`  Yahoo: Tokens ${tokens.access_token ? 'Loaded' : 'Not initialized'}`);
  } else {
    console.log('  Yahoo: 設定不足（YAHOO_CLIENT_ID / YAHOO_SELLER_ID）');
  }
});
