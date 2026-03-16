/**
 * 順位自動チェック（ポータル内蔵版）
 * auto_check.js の ESM + 環境変数対応版
 * scheduler.js から呼ばれる
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const RAKUTEN_API_BASE = 'https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601';
const YAHOO_API_BASE = 'https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch';
const AMAZON_HOST = 'webservices.amazon.co.jp';
const AMAZON_REGION = 'us-west-2';
const AMAZON_SERVICE = 'ProductAdvertisingAPI';
const AMAZON_ENDPOINT = `https://${AMAZON_HOST}/paapi5/searchitems`;
const RAKUTEN_HITS = 30;
const YAHOO_HITS = 50;
const AMAZON_HITS = 10;
const MAX_RANK = 100;
const YAHOO_MAX_RANK = 100;
const API_DELAY = 1100;
const KW_DELAY = 500;
const CONCURRENCY = 3;

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const DATA_FILE = path.join(DATA_DIR, 'ranking-checker.json');
const LOG_FILE = path.join(DATA_DIR, 'ranking-checker.log');

// ── Utilities ──

function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  const line = `[${ts}] ${msg}`;
  console.log('[RankCheck]', msg);
  try { fs.appendFileSync(LOG_FILE, line + '\n', 'utf-8'); } catch {}
}

function readJson(filepath, defaultValue) {
  try { return JSON.parse(fs.readFileSync(filepath, 'utf-8')); } catch { return defaultValue; }
}

function writeJson(filepath, obj) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(filepath, JSON.stringify(obj, null, 2), 'utf-8');
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function today() {
  // JST (UTC+9)
  const d = new Date(Date.now() + 9 * 60 * 60 * 1000);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

// ── URL helpers ──

function normalizeUrl(url) {
  if (!url) return '';
  url = url.trim().replace(/^https?:\/\//, '');
  if (url.startsWith('www.')) url = url.slice(4);
  url = url.replace(/\/+$/, '').split('?')[0].split('#')[0];
  return url.toLowerCase();
}

function urlsMatch(a, b) { return normalizeUrl(a) === normalizeUrl(b); }
function codeToRakutenUrl(code) { return `https://item.rakuten.co.jp/b-faith/${code.replace(/\/+$/, '')}/`; }
function codeToYahooUrl(code) { return `https://store.shopping.yahoo.co.jp/b-faith01/${code.replace(/\/+$/, '')}.html`; }

function codeFromRakutenUrl(url) {
  const m = url.match(/item\.rakuten\.co\.jp\/b-faith\/([^/?#]+)/i);
  return m ? m[1] : '';
}

function getRakutenUrl(product) {
  return product.own_url || (product.product_code ? codeToRakutenUrl(product.product_code) : '');
}

function getYahooUrl(product) {
  return product.yahoo_url || (product.product_code ? codeToYahooUrl(product.product_code) : '');
}

function getAmazonAsin(product) {
  if (product.amazon_asin) return product.amazon_asin;
  const url = product.amazon_url || '';
  if (url) {
    const m = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/i);
    return m ? m[1] : '';
  }
  return '';
}

// ── Config from env ──

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

// ── API callers ──

async function fetchJson(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    if (!resp.ok) {
      const body = await resp.text();
      throw new Error(`HTTP ${resp.status}: ${body.slice(0, 200)}`);
    }
    return await resp.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function rakutenApiSearch(keyword, page, appId, accessKey) {
  const params = new URLSearchParams({
    applicationId: appId, accessKey, keyword,
    hits: String(RAKUTEN_HITS), page: String(page), format: 'json', sort: 'standard',
  });
  return fetchJson(RAKUTEN_API_BASE + '?' + params, {
    headers: {
      'Origin': 'https://rakuten.co.jp',
      'Referer': 'https://rakuten.co.jp/',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    },
  });
}

async function yahooApiSearch(keyword, start, appId) {
  const params = new URLSearchParams({
    appid: appId, query: keyword,
    results: String(YAHOO_HITS), start: String(start), sort: '-score',
  });
  return fetchJson(YAHOO_API_BASE + '?' + params, {
    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
  });
}

// ── Amazon SigV4 ──

function hmacSha256(keyBytes, message) {
  return crypto.createHmac('sha256', keyBytes).update(message, 'utf-8').digest();
}

function getSignatureKey(secretKey, dateStamp, region, service) {
  const kDate = hmacSha256('AWS4' + secretKey, dateStamp);
  const kRegion = hmacSha256(kDate, region);
  const kService = hmacSha256(kRegion, service);
  return hmacSha256(kService, 'aws4_request');
}

async function amazonApiSearch(keyword, itemPage, accessKey, secretKey, partnerTag) {
  const payload = {
    Keywords: keyword, Resources: ['ItemInfo.Title'],
    ItemCount: AMAZON_HITS, ItemPage: itemPage,
    PartnerTag: partnerTag, PartnerType: 'Associates',
    Marketplace: 'www.amazon.co.jp',
  };
  const payloadJson = JSON.stringify(payload);
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
  const headers = {
    'Content-Type': contentType, 'Content-Encoding': 'amz-1.0',
    'Host': AMAZON_HOST, 'X-Amz-Date': amzDate,
    'X-Amz-Target': amzTarget, 'Authorization': authorization,
  };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    const resp = await fetch(AMAZON_ENDPOINT, {
      method: 'POST', headers, body: payloadJson, signal: controller.signal,
    });
    const text = await resp.text();
    if (!resp.ok) throw new Error(`Amazon API HTTP ${resp.status}: ${text.slice(0, 200)}`);
    return JSON.parse(text);
  } finally {
    clearTimeout(timeout);
  }
}

// ── Ranking check logic ──

async function checkRakuten(product, appId, accessKey) {
  const keyword = product.keyword;
  const ownUrl = getRakutenUrl(product);
  const comp1Url = product.competitor1_url || '';
  const comp2Url = product.competitor2_url || '';
  const targets = {};
  if (ownUrl) targets.own = ownUrl;
  if (comp1Url) targets.comp1 = comp1Url;
  if (comp2Url) targets.comp2 = comp2Url;
  const found = {};
  let organicRank = 0;
  let ownReviewCount = null;
  const maxPages = Math.min(34, Math.ceil(MAX_RANK / RAKUTEN_HITS));

  for (let page = 1; page <= maxPages; page++) {
    if (organicRank >= MAX_RANK) break;
    if (page > 1) await sleep(API_DELAY);
    let data;
    try {
      data = await rakutenApiSearch(keyword, page, appId, accessKey);
    } catch (e) {
      log(`  楽天API失敗 page=${page}: ${e.message}`);
      break;
    }
    const items = data.Items || [];
    if (!items.length) break;
    if (page === 1) log(`  楽天API応答: count=${data.count || 0}`);

    for (const wrapper of items) {
      if (organicRank >= MAX_RANK) break;
      const item = wrapper.Item || {};
      const itemUrl = item.itemUrl || '';
      organicRank++;
      if (!itemUrl) continue;
      for (const [key, targetUrl] of Object.entries(targets)) {
        if (found[key]) continue;
        if (urlsMatch(targetUrl, itemUrl)) {
          found[key] = organicRank;
          log(`  ★ 楽天 ${key} 発見! rank=${organicRank}`);
          if (key === 'own' && item.reviewCount != null) {
            ownReviewCount = item.reviewCount;
          }
        }
      }
      if (Object.keys(found).length === Object.keys(targets).length) break;
    }
    if (Object.keys(found).length === Object.keys(targets).length) break;
    if (page >= (data.pageCount || 0)) break;
  }

  return {
    own_rank: found.own || null,
    competitor1_rank: comp1Url ? (found.comp1 || null) : null,
    competitor2_rank: comp2Url ? (found.comp2 || null) : null,
    review_count: ownReviewCount,
  };
}

async function checkYahoo(keyword, targetUrl, appId) {
  let position = 0;
  for (let start = 1; start < YAHOO_MAX_RANK; start += YAHOO_HITS) {
    if (start > 1) await sleep(API_DELAY);
    let data;
    try {
      data = await yahooApiSearch(keyword, start, appId);
    } catch (e) {
      log(`  Yahoo API失敗 start=${start}: ${e.message}`);
      break;
    }
    const hits = data.hits || [];
    if (!hits.length) break;
    for (const item of hits) {
      position++;
      if (urlsMatch(targetUrl, item.url || '')) {
        log(`  ★ Yahoo! 発見! rank=${position}`);
        return position;
      }
    }
    const total = data.totalResultsAvailable || 0;
    if (start + YAHOO_HITS > total || start + YAHOO_HITS > YAHOO_MAX_RANK) break;
  }
  return null;
}

async function checkAmazon(keyword, targetAsin, accessKey, secretKey, partnerTag) {
  let position = 0;
  for (let page = 1; page <= 10; page++) {
    if (page > 1) await sleep(API_DELAY);
    let data;
    try {
      data = await amazonApiSearch(keyword, page, accessKey, secretKey, partnerTag);
    } catch (e) {
      log(`  Amazon API失敗 page=${page}: ${e.message}`);
      break;
    }
    const searchResult = data.SearchResult || {};
    const items = searchResult.Items || [];
    if (!items.length) break;
    for (const item of items) {
      position++;
      if ((item.ASIN || '').toUpperCase() === targetAsin.toUpperCase()) {
        log(`  ★ Amazon 発見! rank=${position}`);
        return position;
      }
    }
  }
  return null;
}

// ── Main export ──

export async function runAutoCheck() {
  log('='.repeat(50));
  log('自動順位チェック開始');

  const config = getConfig();
  const { applicationId: appId, accessKey, yahooAppId } = config;

  if (!appId || !accessKey) {
    log('エラー: RAKUTEN_APP_ID / RAKUTEN_ACCESS_KEY が未設定');
    return;
  }

  const { amazonAccessKey, amazonSecretKey, amazonAssociateTag: amazonPartnerTag } = config;
  log(yahooAppId ? 'Yahoo!: 有効' : 'Yahoo!: 未設定');
  log(amazonAccessKey ? 'Amazon: 有効' : 'Amazon: 未設定');

  const data = readJson(DATA_FILE, { products: [] });
  const products = data.products || [];

  if (!products.length) {
    log('登録商品がありません');
    return;
  }

  // product_code 自動抽出
  for (const p of products) {
    if (!p.product_code && p.own_url) {
      const code = codeFromRakutenUrl(p.own_url);
      if (code) p.product_code = code;
    }
  }

  const todayStr = today();
  log(`対象: ${products.length} 件, 日付: ${todayStr}`);

  const unchecked = products.filter(p => {
    const h = p.history || [];
    return !h.length || h[h.length - 1].date !== todayStr;
  });

  if (!unchecked.length) {
    log('本日は全商品チェック済みです');
    return;
  }

  log(`未チェック: ${unchecked.length} 件`);
  let checked = 0;

  for (let i = 0; i < unchecked.length; i += CONCURRENCY) {
    const batch = unchecked.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(async (product) => {
      const idx = products.indexOf(product);
      const label = `[${idx + 1}/${products.length}]`;
      log(`${label} "${product.keyword}"`);

      // Rakuten
      let result;
      try {
        result = await checkRakuten(product, appId, accessKey);
        if (result.review_count != null) product.review_count = result.review_count;
      } catch (e) {
        log(`  ${label} 楽天エラー: ${e.message}`);
        result = { own_rank: 'error', competitor1_rank: product.competitor1_url ? 'error' : null, competitor2_rank: product.competitor2_url ? 'error' : null };
      }

      // Yahoo
      let yahooRank = null;
      const yahooUrl = getYahooUrl(product);
      if (yahooUrl && yahooAppId) {
        await sleep(API_DELAY);
        try { yahooRank = await checkYahoo(product.keyword, yahooUrl, yahooAppId); }
        catch (e) { yahooRank = 'error'; }
      }

      // Amazon
      let amazonRank = null;
      const amazonAsin = getAmazonAsin(product);
      if (amazonAsin && amazonAccessKey) {
        await sleep(API_DELAY);
        try { amazonRank = await checkAmazon(product.keyword, amazonAsin, amazonAccessKey, amazonSecretKey, amazonPartnerTag); }
        catch (e) { amazonRank = 'error'; }
      }

      // Save history
      if (!product.history) product.history = [];
      const entry = { date: todayStr, ...result };
      if (yahooUrl) entry.yahoo_own_rank = yahooRank;
      if (amazonAsin) entry.amazon_own_rank = amazonRank;
      const todayIdx = product.history.findIndex(e => e.date === todayStr);
      if (todayIdx >= 0) product.history[todayIdx] = entry;
      else product.history.push(entry);
    }));

    checked += batch.length;
    writeJson(DATA_FILE, { products });
    if (i + CONCURRENCY < unchecked.length) await sleep(KW_DELAY);
  }

  log(`自動順位チェック完了: ${checked} 件`);
  log('='.repeat(50));
}

export { DATA_FILE, readJson, writeJson, getRakutenUrl, getYahooUrl, getAmazonAsin, today, log };
