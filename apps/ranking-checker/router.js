/**
 * 楽天順位チェッカー — Express Router for B-Faith Portal (SQLite版)
 *
 * Phase 1 変更点:
 *   - GET  /data         : DB 内容を legacy JSON 形 { products: [...] } で返却 (UI互換)
 *   - POST /data         : 商品マスタ全件保存。同一性は client_id (UI UUID)。
 *                          無い商品は削除（履歴は FK CASCADE で同時削除）。history は無視。
 *   - POST /data/import  : JSONファイルからの復元専用。history も DB に投入する。
 *   - GET  /run-status   : 最新/実行中 run の状態 (Phase2 miniPC 連携用)
 *
 * Environment variables:
 *   RAKUTEN_APP_ID / RAKUTEN_ACCESS_KEY / RAKUTEN_SHOP_CODE
 *   YAHOO_APP_ID
 *   AMAZON_ACCESS_KEY / AMAZON_SECRET_KEY / AMAZON_ASSOCIATE_TAG
 */
import express, { Router } from 'express';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import * as rdb from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// ── Payload validation helpers ──
// Throws { status, body } on invalid input; caller catches and responds.

const STRING_FIELDS = [
  'client_id', 'id', 'keyword', 'product_code',
  'own_url', 'yahoo_url', 'amazon_url', 'amazon_asin',
  'competitor1_url', 'competitor2_url',
];

function typeLabel(v) {
  if (v === null) return 'null';
  if (Array.isArray(v)) return 'array';
  return typeof v;
}

function validateStringOrNull(p, field, idx) {
  const v = p[field];
  if (v == null) return null;
  if (typeof v !== 'string') {
    const err = new Error(`products[${idx}].${field} must be string or null, got ${typeLabel(v)}`);
    err.status = 400; err.body = { error: 'invalid_field_type', field, index: idx };
    throw err;
  }
  return v.length > 0 ? v : null;
}

function validateIntOrNull(p, field, idx) {
  const v = p[field];
  if (v == null) return null;
  if (typeof v === 'boolean' || typeof v !== 'number' || !Number.isFinite(v)) {
    const err = new Error(`products[${idx}].${field} must be integer or null, got ${typeLabel(v)}`);
    err.status = 400; err.body = { error: 'invalid_field_type', field, index: idx };
    throw err;
  }
  return Math.round(v);
}

function validateHistoryEntry(h, idx, hi) {
  if (!h || typeof h !== 'object' || Array.isArray(h)) {
    const err = new Error(`products[${idx}].history[${hi}] must be object`);
    err.status = 400; err.body = { error: 'invalid_history', index: idx, history_index: hi };
    throw err;
  }
  if (typeof h.date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(h.date)) {
    const err = new Error(`products[${idx}].history[${hi}].date must be YYYY-MM-DD string`);
    err.status = 400; err.body = { error: 'invalid_history_date', index: idx, history_index: hi };
    throw err;
  }
  for (const field of ['own_rank', 'competitor1_rank', 'competitor2_rank', 'yahoo_own_rank', 'amazon_own_rank']) {
    const v = h[field];
    if (v == null) continue;
    // 許容型: number | string ('error' または数字文字列)。boolean/object/array は拒否。
    if (typeof v !== 'number' && typeof v !== 'string') {
      const err = new Error(`products[${idx}].history[${hi}].${field} must be number/string/null, got ${typeLabel(v)}`);
      err.status = 400; err.body = { error: 'invalid_rank_type', index: idx, history_index: hi, field };
      throw err;
    }
  }
}

/**
 * 1商品分の payload を validate + normalize。
 * opts.allowHistory=true のとき p.history も検証して normalized に含める。
 * opts.seenClientIds=Set<string> を渡すと duplicate client_id を 400 で弾く。
 * エラー時は err.status / err.body が設定された Error を throw する。
 */
function normalizeProductInput(p, idx, opts = {}) {
  if (!p || typeof p !== 'object' || Array.isArray(p)) {
    const err = new Error(`products[${idx}] は非null object である必要があります`);
    err.status = 400; err.body = { error: 'invalid_product', index: idx };
    throw err;
  }
  for (const field of STRING_FIELDS) validateStringOrNull(p, field, idx);
  validateIntOrNull(p, 'review_count', idx);

  const clientIdRaw = validateStringOrNull(p, 'client_id', idx);
  const idRaw = validateStringOrNull(p, 'id', idx);
  const clientId = clientIdRaw || idRaw || rdb.generateClientId();

  if (opts.seenClientIds) {
    if (opts.seenClientIds.has(clientId)) {
      const err = new Error(`products[${idx}] の client_id "${clientId}" が payload 内で重複しています`);
      err.status = 400; err.body = { error: 'duplicate_client_id', index: idx, client_id: clientId };
      throw err;
    }
    opts.seenClientIds.add(clientId);
  }

  const result = {
    client_id: clientId,
    keyword: p.keyword || '',
    product_code: p.product_code || null,
    own_url: p.own_url || null,
    yahoo_url: p.yahoo_url || null,
    amazon_url: p.amazon_url || null,
    amazon_asin: p.amazon_asin || null,
    competitor1_url: p.competitor1_url || null,
    competitor2_url: p.competitor2_url || null,
    review_count: p.review_count != null ? Math.round(p.review_count) : null,
  };

  if (opts.allowHistory) {
    if (p.history != null && !Array.isArray(p.history)) {
      const err = new Error(`products[${idx}].history must be array or null`);
      err.status = 400; err.body = { error: 'invalid_history_field', index: idx };
      throw err;
    }
    const history = Array.isArray(p.history) ? p.history : [];
    for (let hi = 0; hi < history.length; hi++) validateHistoryEntry(history[hi], idx, hi);
    result.history = history;
  }

  return result;
}

const RAKUTEN_API_BASE = 'https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601';
const YAHOO_API_BASE = 'https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch';
const AMAZON_HOST = 'webservices.amazon.co.jp';
const AMAZON_REGION = 'us-west-2';
const AMAZON_SERVICE = 'ProductAdvertisingAPI';
const AMAZON_ENDPOINT = `https://${AMAZON_HOST}/paapi5/searchitems`;

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');

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

// ── AWS SigV4 ──
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
    'Content-Type': contentType, 'Content-Encoding': 'amz-1.0',
    'Host': AMAZON_HOST, 'X-Amz-Date': amzDate,
    'X-Amz-Target': amzTarget, 'Authorization': authorization,
  };
}

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
  } finally { clearTimeout(timeout); }
}

// ── Routes ──

router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

router.get('/rakuten-logo.svg', (req, res) => {
  res.sendFile(path.join(__dirname, 'rakuten-logo.svg'));
});

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
  } catch (e) { res.status(502).json({ error: e.message }); }
});

router.get('/api/yahoo', async (req, res) => {
  const query = new URL(req.url, 'http://localhost').search?.slice(1) || '';
  try {
    const result = await proxyRequest(YAHOO_API_BASE + '?' + query, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
    });
    res.status(result.status).type('json').send(result.body);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

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
  } catch (e) { res.status(502).json({ error: e.message }); }
});

// ── Data endpoints (SQLite-backed) ──

router.get('/data', (req, res) => {
  try {
    res.json(rdb.exportLegacyShape());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * POST /data — UIからの全件保存リクエスト。
 *
 * 受信契約 { products: [...], confirmEmpty?: boolean }:
 *   - `products` が配列でない / undefined → 400。空ボディや形式違いによる
 *     全削除事故を防ぐ。
 *   - `products === []` のときは `confirmEmpty: true` を要求。
 *     UIが意図せず空送信しても全消去にならない。
 *   - 商品同一性は **client_id (= UI UUID)** のみ。keyword や product_code の
 *     編集は属性の変更として扱い、履歴を保持する。
 *   - 受信した `history` はサーバー側で無視。UIからの全件POSTを軽くするため。
 *
 * 1トランザクションに包んで原子的に実行。
 */
router.post('/data', (req, res) => {
  try {
    const body = req.body;
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'invalid_body', detail: 'JSONオブジェクトを送ってください' });
    }
    if (!Array.isArray(body.products)) {
      return res.status(400).json({ error: 'products_not_array', detail: '`products` 配列が必要です' });
    }

    // 先に全件 validate してから空判定する。詳細は normalizeProductInput を参照。
    const seenClientIds = new Set();
    const normalized = [];
    try {
      for (let i = 0; i < body.products.length; i++) {
        normalized.push(normalizeProductInput(body.products[i], i, { seenClientIds }));
      }
    } catch (e) {
      if (e && e.status === 400) return res.status(400).json({ ...e.body, detail: e.message });
      throw e;
    }

    if (normalized.length === 0 && body.confirmEmpty !== true) {
      return res.status(400).json({
        error: 'empty_without_confirm',
        detail: '最終的な保存対象が 0 件になる場合は confirmEmpty:true を明示してください',
      });
    }

    const incomingClientIds = seenClientIds;

    const db = rdb.getDb();
    const apply = db.transaction(() => {
      for (const p of normalized) rdb.upsertProduct(p);

      // 既存で incoming に存在しない client_id を削除。
      // history は FK CASCADE で同時に消える（= ユーザーが商品を削除した意図通り）。
      let deleted = 0;
      const existing = [];
      for (const row of rdb.iterAllProducts()) {
        existing.push({ id: row.id, client_id: row.client_id });
      }
      for (const ex of existing) {
        if (!incomingClientIds.has(ex.client_id)) {
          rdb.deleteProduct(ex.id);
          deleted++;
        }
      }
      return { inserted_or_updated: normalized.length, deleted };
    });

    const summary = apply();
    res.json({ ok: true, ...summary });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * POST /data/import — JSONバックアップからの復元用（履歴含む）。
 *
 * 通常保存 POST /data は history を無視するため、インポートでは履歴が失われる。
 * 本エンドポイントは history も DB に書き込むことで復元を可能にする。
 *
 * 受信契約 { products: [...], replaceAll?: boolean, confirmEmpty?: boolean }:
 *   - products 配列必須（非配列→400）
 *   - replaceAll=true の場合、incoming に無い商品は削除（CASCADEで履歴消去）
 *     empty は confirmEmpty:true 必須
 *   - replaceAll=false (既定) はマージ、既存商品の履歴は保持
 *   - body は import JSON 相当なので 50mb まで許容
 */
router.post('/data/import', express.json({ limit: '50mb' }), (req, res) => {
  try {
    const body = req.body;
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'invalid_body' });
    }
    if (!Array.isArray(body.products)) {
      return res.status(400).json({ error: 'products_not_array' });
    }
    const replaceAll = body.replaceAll === true;

    // 先に全件 validate/normalize。duplicate / 型違反を含めて 400 で早期返却。
    const seenClientIds = new Set();
    const normalized = [];
    try {
      for (let i = 0; i < body.products.length; i++) {
        normalized.push(normalizeProductInput(body.products[i], i, { seenClientIds, allowHistory: true }));
      }
    } catch (e) {
      if (e && e.status === 400) return res.status(400).json({ ...e.body, detail: e.message });
      throw e;
    }

    if (replaceAll && normalized.length === 0 && body.confirmEmpty !== true) {
      return res.status(400).json({ error: 'empty_without_confirm' });
    }

    const incomingClientIds = seenClientIds;
    const db = rdb.getDb();

    const apply = db.transaction(() => {
      let productsUpserted = 0;
      let historyUpserted = 0;
      for (const p of normalized) {
        const productId = rdb.upsertProduct(p);
        productsUpserted++;
        for (const h of p.history) {
          if (!h || !h.date) continue;
          rdb.upsertHistory({
            product_id: productId,
            date: h.date,
            own_rank: encodeImportRank(h.own_rank),
            competitor1_rank: encodeImportRank(h.competitor1_rank),
            competitor2_rank: encodeImportRank(h.competitor2_rank),
            yahoo_own_rank: encodeImportRank(h.yahoo_own_rank),
            amazon_own_rank: encodeImportRank(h.amazon_own_rank),
          });
          historyUpserted++;
        }
      }

      let deleted = 0;
      if (replaceAll) {
        const existing = [];
        for (const row of rdb.iterAllProducts()) {
          existing.push({ id: row.id, client_id: row.client_id });
        }
        for (const ex of existing) {
          if (!incomingClientIds.has(ex.client_id)) {
            rdb.deleteProduct(ex.id);
            deleted++;
          }
        }
      }

      return { productsUpserted, historyUpserted, deleted, mode: replaceAll ? 'replace' : 'merge' };
    });

    res.json({ ok: true, ...apply() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * legacy JSON の rank 表現 ('error' / null / 数値) を DB 表現 (-1 / null / 1..100) へ。
 *
 * validateHistoryEntry() が boolean / object / array を既に弾いているので、
 * ここでは number / string / null のみを想定する。とはいえ防御的に型を再確認する。
 */
function encodeImportRank(v) {
  if (v === null || v === undefined || v === '') return null;
  if (typeof v !== 'number' && typeof v !== 'string') return null;
  if (v === 'error') return -1;
  const n = Number(v);
  if (Number.isFinite(n)) {
    const r = Math.round(n);
    if (r === -1) return -1;
    if (r >= 1 && r <= 100) return r;
    return null;
  }
  return -1;
}

router.get('/config', (req, res) => { res.json(getConfig()); });
router.post('/config', (req, res) => { res.json({ ok: true, note: 'Config is managed via environment variables on Render' }); });

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

router.post('/run-check', async (req, res) => {
  try {
    const { runAutoCheck, getCheckProgress } = await import('./auto-check.js');
    const progress = getCheckProgress();
    if (progress.running) {
      return res.json({ ok: false, message: '既にチェック実行中です', progress });
    }
    res.json({ ok: true, message: 'サーバー側で順位チェック開始（タブを閉じても継続します）' });
    runAutoCheck({ force: true }).catch(e => console.error('[RankCheck] チェック実行エラー:', e.message));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

router.get('/check-progress', async (req, res) => {
  try {
    const { getCheckProgress } = await import('./auto-check.js');
    res.json(getCheckProgress());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/**
 * 最新 run メタ情報。Phase2 以降の miniPC 実行側からの状態参照や、
 * 再開可能性の確認に利用する。
 */
router.get('/run-status', (req, res) => {
  try {
    res.json({ latest: rdb.getLatestRun(), running: rdb.getRunningRun() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// テスト用にエクスポート。本番実装では router 経由で呼ばれる。
export { normalizeProductInput, encodeImportRank };

export default router;
