/**
 * 順位自動チェック（SQLite版）
 *
 * Phase 1 変更点 (2026-04-21):
 *   - ranking-checker.json の読み書きを全廃、ranking-checker.db に移行
 *   - 商品 × 履歴を全件メモリ展開していた設計を、1商品ずつ SELECT / UPSERT へ
 *   - run_state / run_log を追加、途中停止からの再開と事後調査が可能
 *   - 圏外は null、API失敗は -1 (INTEGER)。文字列 'error' は廃止
 *
 * env:
 *   RANKCHECK_DEBUG=true     詳細ログ復活
 *   RANKCHECK_PROGRESS_EVERY 進捗 DB 書込間隔 (既定10商品)
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import * as rdb from './db.js';

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
const KW_DELAY = 1200;
const CONCURRENCY = 1;
const RETRY_MAX = 5;
const RETRY_DELAY = 3000;

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const LOG_FILE = path.join(DATA_DIR, 'ranking-checker.log');

const DEBUG_LOG = process.env.RANKCHECK_DEBUG === 'true';
const PROGRESS_EVERY = Math.max(1, parseInt(process.env.RANKCHECK_PROGRESS_EVERY || '10', 10));

// 進捗トラッキング（メモリ上、UI用）
const checkProgress = { running: false, total: 0, done: 0, current: '', startedAt: null, runId: null };

// ── Utilities ──

function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  const line = `[${ts}] ${msg}`;
  console.log('[RankCheck]', msg);
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFileSync(LOG_FILE, line + '\n', 'utf-8');
  } catch {}
}

function debugLog(msg) { if (DEBUG_LOG) log(msg); }

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function today() {
  const d = new Date(Date.now() + 9 * 60 * 60 * 1000);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

// ── URL helpers ──

function normalizeUrl(url) {
  if (!url) return '';
  url = url.trim().replace(/^https?:\/\//, '');
  if (url.startsWith('www.')) url = url.slice(4);
  url = url.split('?')[0].split('#')[0].replace(/\/+$/, '');
  return url.toLowerCase();
}

function urlsMatch(a, b) { return normalizeUrl(a) === normalizeUrl(b); }
function codeToRakutenUrl(code) { return `https://item.rakuten.co.jp/b-faith/${code.replace(/\/+$/, '')}/`; }
function codeToYahooUrl(code) { return `https://store.shopping.yahoo.co.jp/b-faith01/${code.replace(/\/+$/, '')}.html`; }

function codeFromRakutenUrl(url) {
  const m = (url || '').match(/item\.rakuten\.co\.jp\/b-faith\/([^/?#]+)/i);
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
  for (let attempt = 1; attempt <= RETRY_MAX; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    try {
      const resp = await fetch(url, { ...options, signal: controller.signal });
      if (resp.status === 429 && attempt < RETRY_MAX) {
        log(`  ⚠ 429 Rate Limit, ${RETRY_DELAY}ms待ってリトライ (${attempt}/${RETRY_MAX})`);
        clearTimeout(timeout);
        await sleep(RETRY_DELAY);
        continue;
      }
      if (!resp.ok) {
        const body = await resp.text();
        throw new Error(`HTTP ${resp.status}: ${body.slice(0, 200)}`);
      }
      return await resp.json();
    } catch (e) {
      clearTimeout(timeout);
      if (attempt < RETRY_MAX && e.message && e.message.includes('abort')) {
        log(`  ⚠ タイムアウト, リトライ (${attempt}/${RETRY_MAX})`);
        await sleep(RETRY_DELAY);
        continue;
      }
      throw e;
    } finally {
      clearTimeout(timeout);
    }
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

// 順位結果の表現: 数値(1..100) = 順位、null = 圏外/対象なし、-1 = APIエラー
const RANK_ERROR = -1;

async function checkRakuten(product, appId, accessKey) {
  const keyword = product.keyword;
  const ownUrl = getRakutenUrl(product);
  const comp1Url = product.competitor1_url || '';
  const comp2Url = product.competitor2_url || '';
  const targets = {};
  if (ownUrl) targets.own = ownUrl;
  if (comp1Url) targets.comp1 = comp1Url;
  if (comp2Url) targets.comp2 = comp2Url;

  debugLog(`  product_code="${product.product_code || ''}", own_url="${product.own_url || ''}"`);
  debugLog(`  target_own="${ownUrl ? normalizeUrl(ownUrl) : '(なし)'}"`);
  if (!Object.keys(targets).length) {
    log(`  ⚠ 比較対象URLなし → 圏外になります`);
  }

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
      if (page === 1 && e.message && e.message.includes('429')) {
        log(`  ⚠ page=1失敗のため5秒待って再試行...`);
        await sleep(5000);
        try {
          data = await rakutenApiSearch(keyword, page, appId, accessKey);
        } catch (e2) {
          log(`  楽天API再試行も失敗: ${e2.message}`);
          return { own_rank: RANK_ERROR, competitor1_rank: comp1Url ? RANK_ERROR : null, competitor2_rank: comp2Url ? RANK_ERROR : null, review_count: null };
        }
      } else {
        // page=2以降の失敗: これまで見つけたものだけ返す
        break;
      }
    }
    const items = data.Items || [];
    if (!items.length) break;
    if (page === 1) {
      debugLog(`  楽天API応答: count=${data.count || 0}, pages=${data.pageCount || 0}`);
      if (DEBUG_LOG) {
        for (let i = 0; i < Math.min(3, items.length); i++) {
          const u = (items[i].Item || {}).itemUrl || '';
          debugLog(`  #${i + 1}: ${normalizeUrl(u)}`);
        }
      }
    }

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
          if (key === 'own' && item.reviewCount != null) ownReviewCount = item.reviewCount;
        }
      }
      if (Object.keys(found).length === Object.keys(targets).length) break;
    }
    if (Object.keys(found).length === Object.keys(targets).length) break;
    if (page >= (data.pageCount || 0)) break;
  }

  log(`  探索完了: ${organicRank}件チェック, 発見=${JSON.stringify(found)}`);
  return {
    own_rank: found.own != null ? found.own : null,
    competitor1_rank: comp1Url ? (found.comp1 != null ? found.comp1 : null) : null,
    competitor2_rank: comp2Url ? (found.comp2 != null ? found.comp2 : null) : null,
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
      return RANK_ERROR;
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
      return RANK_ERROR;
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

// ── Progress ──

export function getCheckProgress() {
  return { ...checkProgress };
}

// ── Main ──

export async function runAutoCheck({ force = false } = {}) {
  if (checkProgress.running) {
    log('既にチェック実行中です');
    return;
  }

  log('='.repeat(50));
  log(`自動順位チェック開始${force ? '（強制再チェック）' : ''}`);

  // クラッシュ後の stale running は env 検証より前に片付ける。
  // env不備で return するたびに running が残ると、/run-status が永遠に嘘をつく。
  const staleFixed = rdb.markStaleRunning();
  if (staleFixed > 0) log(`stale running ${staleFixed} 件を failed に遷移`);

  const config = getConfig();
  const { applicationId: appId, accessKey, yahooAppId } = config;
  if (!appId || !accessKey) {
    log('エラー: RAKUTEN_APP_ID / RAKUTEN_ACCESS_KEY が未設定');
    return;
  }
  const { amazonAccessKey, amazonSecretKey, amazonAssociateTag: amazonPartnerTag } = config;
  log(yahooAppId ? 'Yahoo!: 有効' : 'Yahoo!: 未設定');
  log(amazonAccessKey ? 'Amazon: 有効' : 'Amazon: 未設定');

  const todayStr = today();
  const totalAll = rdb.countProducts();

  if (totalAll === 0) {
    log('登録商品がありません');
    return;
  }

  // 対象IDを事前 materialize。配列長から totalTarget を決めつつ、
  // 下流のループでもこの配列をそのまま使う（iteratorを長時間保持しない）。
  const targetIds = force ? rdb.listAllIds() : rdb.listUncheckedIdsForDate(todayStr);
  const totalTarget = targetIds.length;

  log(`対象: ${totalAll} 件, 日付: ${todayStr}, 今回処理予定: ${totalTarget} 件`);

  if (totalTarget === 0) {
    log('本日は全商品チェック済みです');
    return;
  }

  const runId = rdb.startRun(totalTarget);
  rdb.logRun(runId, 'info', `run開始 force=${force} target=${totalTarget}`);

  checkProgress.running = true;
  checkProgress.total = totalTarget;
  checkProgress.done = 0;
  checkProgress.current = '';
  checkProgress.startedAt = new Date().toISOString();
  checkProgress.runId = runId;

  let done = 0;
  let finishStatus = 'completed';
  let finishError = null;

  try {
    // CONCURRENCY=1 前提の逐次処理。並列化するなら chunk で集めて Promise.all。
    for (const productId of targetIds) {
      const product = rdb.getProduct(productId);
      if (!product) {
        // 実行中に /data で削除されたケース。スキップして進める。
        rdb.logRun(runId, 'warn', `product消失スキップ id=${productId}`);
        done++;
        checkProgress.done = done;
        continue;
      }
      // product_code が空で own_url から取れるなら補完して DB にも反映
      if (!product.product_code && product.own_url) {
        const code = codeFromRakutenUrl(product.own_url);
        if (code) {
          product.product_code = code;
          rdb.updateProductCode(product.id, code);
        }
      }

      checkProgress.current = product.keyword;
      log(`[${done + 1}/${totalTarget}] "${product.keyword}"`);

      let result;
      try {
        result = await checkRakuten(product, appId, accessKey);
      } catch (e) {
        log(`  楽天エラー: ${e.message}`);
        rdb.logRun(runId, 'error', `楽天エラー: ${e.message}`, product.id);
        result = {
          own_rank: RANK_ERROR,
          competitor1_rank: product.competitor1_url ? RANK_ERROR : null,
          competitor2_rank: product.competitor2_url ? RANK_ERROR : null,
          review_count: null,
        };
      }
      // review_count は「取得できたときだけ上書き」ポリシー。
      // 一時的に検索結果に出なかった日や API 失敗日に古い値を null で消すと、
      // UIのレビュー数バッジがチラつくため、前日値を据え置きにする意図。
      if (result.review_count != null) {
        rdb.updateProductReviewCount(product.id, result.review_count);
      }

      // Yahoo（サーバーIPからYahoo APIが無効のため意図的に disabled のまま）
      let yahooRank = null;
      const yahooUrl = getYahooUrl(product);
      if (false && yahooUrl && yahooAppId) {
        await sleep(API_DELAY);
        try { yahooRank = await checkYahoo(product.keyword, yahooUrl, yahooAppId); }
        catch (e) {
          log(`  Yahoo例外: ${e.message}`);
          rdb.logRun(runId, 'error', `Yahoo例外: ${e.message}`, product.id);
          yahooRank = RANK_ERROR;
        }
      }

      let amazonRank = null;
      const amazonAsin = getAmazonAsin(product);
      if (amazonAsin && amazonAccessKey) {
        await sleep(API_DELAY);
        try { amazonRank = await checkAmazon(product.keyword, amazonAsin, amazonAccessKey, amazonSecretKey, amazonPartnerTag); }
        catch (e) {
          log(`  Amazon例外: ${e.message}`);
          rdb.logRun(runId, 'error', `Amazon例外: ${e.message}`, product.id);
          amazonRank = RANK_ERROR;
        }
      }

      // 1商品 = 1回の UPSERT。トランザクション不要（既に原子的）。
      rdb.upsertHistory({
        product_id: product.id,
        date: todayStr,
        own_rank: result.own_rank,
        competitor1_rank: result.competitor1_rank,
        competitor2_rank: result.competitor2_rank,
        yahoo_own_rank: yahooUrl ? yahooRank : null,
        amazon_own_rank: amazonAsin ? amazonRank : null,
      });

      done++;
      checkProgress.done = done;
      if (done % PROGRESS_EVERY === 0 || done === totalTarget) {
        rdb.updateRunProgress(runId, done);
      }

      if (done < totalTarget) await sleep(KW_DELAY);
    }

    log(`自動順位チェック完了: ${done} 件`);
    log('='.repeat(50));
  } catch (e) {
    finishStatus = 'failed';
    finishError = e.message;
    log(`チェック中断: ${e.message}`);
    rdb.logRun(runId, 'error', `中断: ${e.message}`);
    throw e;
  } finally {
    rdb.updateRunProgress(runId, done);
    rdb.finishRun(runId, finishStatus, finishError);
    checkProgress.running = false;
    checkProgress.current = '';
  }
}

export { today, log, debugLog, getRakutenUrl, getYahooUrl, getAmazonAsin };
