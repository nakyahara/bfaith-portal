/**
 * 楽天順位チェッカー — Express Router for B-Faith Portal
 *
 * Environment variables:
 *   RAKUTEN_APP_ID       — 楽天 applicationId
 *   RAKUTEN_ACCESS_KEY   — 楽天 accessKey
 *   RAKUTEN_SHOP_CODE    — 楽天 shopCode (default: b-faith)
 *   YAHOO_APP_ID         — Yahoo appId
 *   AMAZON_ACCESS_KEY    — Amazon PA-API accessKey
 *   AMAZON_SECRET_KEY    — Amazon PA-API secretKey
 *   AMAZON_ASSOCIATE_TAG — Amazon associateTag
 */
import { Router } from 'express';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// ── Constants ──
const RAKUTEN_API_BASE = 'https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601';
const YAHOO_API_BASE = 'https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch';
const AMAZON_HOST = 'webservices.amazon.co.jp';
const AMAZON_REGION = 'us-west-2';
const AMAZON_SERVICE = 'ProductAdvertisingAPI';
const AMAZON_ENDPOINT = `https://${AMAZON_HOST}/paapi5/searchitems`;

// Data file — stored in portal's data/ directory for persistence
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const DATA_FILE = path.join(DATA_DIR, 'ranking-checker.json');

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function readJson(filepath, defaultValue) {
  try {
    return JSON.parse(fs.readFileSync(filepath, 'utf-8'));
  } catch { return defaultValue; }
}

function writeJson(filepath, obj) {
  ensureDataDir();
  fs.writeFileSync(filepath, JSON.stringify(obj, null, 2), 'utf-8');
}

// ── Config from environment variables ──
function getConfig() {
  return {
    applicationId: process.env.RAKUTEN_APP_ID || '',
    accessKey: process.env.RAKUTEN_ACCESS_KEY || '',
    shopCode: process.env.RAKUTEN_SHOP_CODE || 'b-faith',
    yahooAppId: process.env.YAHOO_APP_ID || '',
    amazonAccessKey: process.env.AMAZON_ACCESS_KEY || '',
    amazonSecretKey: process.env.AMAZON_SECRET_KEY || '',
    amazonAssociateTag: process.env.AMAZON_ASSOCIATE_TAG || '',
  };
}

// ── AWS SigV4 signing ──
function hmacSha256(keyBytes, message) {
  return crypto.createHmac('sha256', keyBytes).update(message, 'utf-8').digest();
}

function getSignatureKey(secretKey, dateStamp, region, service) {
  const kDate = hmacSha256('AWS4' + secretKey, dateStamp);
  const kRegion = hmacSha256(kDate, region);
  const kService = hmacSha256(kRegion, service);
  return hmacSha256(kService, 'aws4_request');
}

function buildAmazonSignedHeaders(accessKey, secretKey, payloadJson) {
  const now = new Date();
  const amzDate = now.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
  const dateStamp = amzDate.slice(0, 8);
  const contentType = 'application/json; charset=utf-8';
  const amzTarget = 'com.amazon.paapi5.v1.ProductAdvertisingAPIv1.SearchItems';
  const canonicalUri = '/paapi5/searchitems';
  const canonicalHeaders =
    `content-type:${contentType}\nhost:${AMAZON_HOST}\nx-amz-date:${amzDate}\nx-amz-target:${amzTarget}\n`;
  const signedHeaders = 'content-type;host;x-amz-date;x-amz-target';
  const payloadHash = crypto.createHash('sha256').update(payloadJson, 'utf-8').digest('hex');
  const canonicalRequest = `POST\n${canonicalUri}\n\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`;
  const credentialScope = `${dateStamp}/${AMAZON_REGION}/${AMAZON_SERVICE}/aws4_request`;
  const canonicalRequestHash = crypto.createHash('sha256').update(canonicalRequest, 'utf-8').digest('hex');
  const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${credentialScope}\n${canonicalRequestHash}`;
  const signingKey = getSignatureKey(secretKey, dateStamp, AMAZON_REGION, AMAZON_SERVICE);
  const signature = crypto.createHmac('sha256', signingKey).update(stringToSign, 'utf-8').digest('hex');
  const authorization =
    `AWS4-HMAC-SHA256 Credential=${accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  return {
    'Content-Type': contentType,
    'Content-Encoding': 'amz-1.0',
    'Host': AMAZON_HOST,
    'X-Amz-Date': amzDate,
    'X-Amz-Target': amzTarget,
    'Authorization': authorization,
  };
}

// ── Proxy helper ──
async function proxyRequest(targetUrl, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    const resp = await fetch(targetUrl, {
      method: options.method || 'GET',
      headers: options.headers || {},
      body: options.body || undefined,
      signal: controller.signal,
    });
    const data = await resp.text();
    return { status: resp.status, body: data };
  } finally {
    clearTimeout(timeout);
  }
}

// ── Routes ──

// Serve main HTML
router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Serve static assets (logo, etc.)
router.get('/rakuten-logo.svg', (req, res) => {
  res.sendFile(path.join(__dirname, 'rakuten-logo.svg'));
});

// Rakuten API proxy
router.get('/api/rakuten', async (req, res) => {
  const query = new URL(req.url, 'http://localhost').search?.slice(1) || '';
  try {
    const result = await proxyRequest(RAKUTEN_API_BASE + '?' + query, {
      headers: {
        'Origin': 'https://rakuten.co.jp',
        'Referer': 'https://rakuten.co.jp/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    });
    res.status(result.status).type('json').send(result.body);
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
});

// Yahoo API proxy
router.get('/api/yahoo', async (req, res) => {
  const query = new URL(req.url, 'http://localhost').search?.slice(1) || '';
  try {
    const result = await proxyRequest(YAHOO_API_BASE + '?' + query, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
    });
    res.status(result.status).type('json').send(result.body);
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
});

// Amazon API proxy
router.get('/api/amazon', async (req, res) => {
  const params = new URLSearchParams(new URL(req.url, 'http://localhost').search || '');
  const keyword = params.get('keyword') || '';
  const itemPage = parseInt(params.get('itemPage') || '1', 10);
  const config = getConfig();
  if (!config.amazonAccessKey || !config.amazonSecretKey || !config.amazonAssociateTag) {
    return res.status(400).json({ error: 'Amazon API credentials not configured' });
  }
  const payload = {
    Keywords: keyword,
    Resources: ['ItemInfo.Title', 'Images.Primary.Medium', 'Offers.Listings.Price'],
    ItemCount: 10, ItemPage: itemPage,
    PartnerTag: config.amazonAssociateTag, PartnerType: 'Associates',
    Marketplace: 'www.amazon.co.jp',
  };
  const payloadJson = JSON.stringify(payload);
  try {
    const headers = buildAmazonSignedHeaders(config.amazonAccessKey, config.amazonSecretKey, payloadJson);
    const result = await proxyRequest(AMAZON_ENDPOINT, { method: 'POST', headers, body: payloadJson });
    res.status(result.status).type('json').send(result.body);
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
});

// Data endpoints
router.get('/data', (req, res) => {
  res.json(readJson(DATA_FILE, { products: [] }));
});

router.post('/data', (req, res) => {
  writeJson(DATA_FILE, req.body);
  res.json({ ok: true });
});

// Config endpoint (read-only, from env vars)
router.get('/config', (req, res) => {
  res.json(getConfig());
});

// Config POST — no-op on Render (env vars are read-only), but accept gracefully
router.post('/config', (req, res) => {
  res.json({ ok: true, note: 'Config is managed via environment variables on Render' });
});

// Log viewer (auto-check log)
router.get('/logs', (req, res) => {
  const LOG_FILE = path.join(DATA_DIR, 'ranking-checker.log');
  const lines = parseInt(req.query.lines || '200', 10);
  try {
    const content = fs.readFileSync(LOG_FILE, 'utf-8');
    const allLines = content.split('\n');
    const tail = allLines.slice(-lines).join('\n');
    res.type('text/plain; charset=utf-8').send(tail);
  } catch {
    res.type('text/plain').send('(ログファイルなし)');
  }
});

// 手動で自動チェックをテスト実行
router.post('/run-check', async (req, res) => {
  try {
    const { runAutoCheck } = await import('./auto-check.js');
    res.json({ ok: true, message: '自動チェック開始（バックグラウンド実行）' });
    runAutoCheck().catch(e => console.error('[RankCheck] テスト実行エラー:', e.message));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

export default router;
