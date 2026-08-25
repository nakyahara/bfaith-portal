/**
 * Notion API thin wrapper (undici fetch、 fail-closed、 rate limit、 429/5xx retry)。
 * ローカル RYS src/lib/notion-client.js の ES module 翻訳。
 *
 * 設計原則 (Codex Phase B1 提案):
 *   - process.env.RYS_NOTION_TOKEN / NOTION_PRODUCT_MASTER_DB_ID を fail-closed で検証
 *   - 連続リクエスト間 min interval (default 350ms = ~3 req/sec、 Notion 制限内)
 *   - 429: Retry-After header を尊重、 なければ exponential backoff (base 1s) + jitter
 *   - 5xx: 同様に backoff retry
 *   - 4xx (429 以外) は即時 throw
 */

const DEFAULT_BASE = 'https://api.notion.com/v1';
const DEFAULT_VERSION = '2022-06-28';
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RETRIES = 5;
const DEFAULT_MIN_INTERVAL_MS = 350; // ~3 req/sec

export class NotionApiError extends Error {
  constructor(endpoint, message, { status = null, body = null, cause = null, retryAfterMs = null } = {}) {
    super(`[notion:${endpoint}] ${message}`);
    this.name = 'NotionApiError';
    this.endpoint = endpoint;
    this.status = status;
    this.body = body;
    if (cause) this.cause = cause;
    // Codex Phase E-14 R5 H-2: 429/503 時の Retry-After 値 (ms) を caller が読めるよう保持
    this.retryAfterMs = retryAfterMs;
  }
}

export function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    throw new NotionApiError('config', `${name} is not set (fail-closed)`);
  }
  return v.trim();
}

export function getConfig() {
  return {
    token: requireEnv('RYS_NOTION_TOKEN'),
    databaseId: requireEnv('NOTION_PRODUCT_MASTER_DB_ID'),
    base: process.env.NOTION_API_BASE || DEFAULT_BASE,
    version: process.env.NOTION_VERSION || DEFAULT_VERSION,
    maxRetries: parseInt(process.env.NOTION_MAX_RETRIES, 10) || DEFAULT_MAX_RETRIES,
    minIntervalMs: parseInt(process.env.NOTION_MIN_INTERVAL_MS, 10) || DEFAULT_MIN_INTERVAL_MS,
    timeoutMs: parseInt(process.env.NOTION_TIMEOUT_MS, 10) || DEFAULT_TIMEOUT_MS,
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, Math.max(0, ms)));

let _lastRequestAt = 0;
async function waitForSlot(minIntervalMs) {
  const now = Date.now();
  const elapsed = now - _lastRequestAt;
  if (elapsed < minIntervalMs) await sleep(minIntervalMs - elapsed);
  _lastRequestAt = Date.now();
}

export function parseRetryAfter(headers) {
  if (!headers) return null;
  const v = typeof headers.get === 'function' ? headers.get('retry-after') : headers['retry-after'];
  if (!v) return null;
  const raw = Array.isArray(v) ? v[0] : v;
  const sec = parseFloat(raw);
  if (!Number.isFinite(sec) || sec < 0) return null;
  return Math.ceil(sec * 1000);
}

export function backoffDelay(attempt) {
  const base = 1000 * Math.pow(2, attempt);
  const jitter = Math.random() * (base * 0.25);
  return Math.min(60_000, base + jitter);
}

async function rawRequest(endpoint, { method = 'GET', body = null, cfg }) {
  const url = `${cfg.base}${endpoint}`;
  const headers = {
    'Authorization': `Bearer ${cfg.token}`,
    'Notion-Version': cfg.version,
    'Accept': 'application/json',
  };
  if (body !== null) headers['Content-Type'] = 'application/json';

  const res = await fetch(url, {
    method,
    headers,
    body: body !== null ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(cfg.timeoutMs),
  });
  let parsed = null;
  let raw = '';
  try {
    raw = await res.text();
    parsed = raw ? JSON.parse(raw) : null;
  } catch (_) {
    parsed = null;
  }
  return { status: res.status, headers: res.headers, body: parsed, raw };
}

export async function notionRequest(endpoint, opts = {}) {
  // Codex Phase E-14 R6 H-1: 部分 cfg を渡しても token/base/version 等が欠落しないよう、
  //   getConfig() 結果と opts.cfg を spread merge する (caller は { timeoutMs, maxRetries: 0 } 等の
  //   部分指定が可能、 既存 caller は cfg 未指定 or 完全 cfg のいずれでも互換)。
  const cfg = { ...getConfig(), ...(opts.cfg || {}) };
  const maxRetries = opts.maxRetries ?? cfg.maxRetries;
  let lastErr = null;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    await waitForSlot(cfg.minIntervalMs);
    let res;
    try {
      res = await rawRequest(endpoint, { ...opts, cfg });
    } catch (e) {
      lastErr = e;
      if (attempt >= maxRetries) {
        throw new NotionApiError(endpoint, `network error after ${maxRetries + 1} attempts: ${e.message}`, { cause: e });
      }
      await sleep(backoffDelay(attempt));
      continue;
    }
    if (res.status >= 200 && res.status < 300) return res.body;
    if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
      const retryAfterMs = parseRetryAfter(res.headers);
      if (attempt >= maxRetries) {
        // Codex Phase E-14 R5 H-2: NotionApiError に retryAfterMs を載せる
        //   caller (createPageWithBudget 等) が残り budget 計算に使うため
        throw new NotionApiError(endpoint, `${res.status} after ${maxRetries + 1} attempts`, {
          status: res.status, body: res.body, retryAfterMs,
        });
      }
      const wait = retryAfterMs !== null ? retryAfterMs : backoffDelay(attempt);
      await sleep(wait);
      continue;
    }
    throw new NotionApiError(
      endpoint,
      `${res.status} ${res.body?.code || ''} ${res.body?.message || ''}`.trim(),
      { status: res.status, body: res.body }
    );
  }
  throw lastErr || new NotionApiError(endpoint, 'unknown retry exit');
}

/**
 * databases/:id/query を pagination 完走まで pull する。
 */
export async function queryDatabaseAll(opts = {}) {
  const cfg = { ...getConfig(), ...(opts.cfg || {}) };
  const dbId = cfg.databaseId;
  const pages = [];
  let cursor = null;
  const pageSize = opts.pageSize || 100;
  // 既定 50 ページ (=5,000件) は同期処理の暴走ガード。大きな一括照会は呼び出し側が明示して広げる
  const maxPages = Number.isInteger(opts.maxPages) && opts.maxPages > 0 ? opts.maxPages : 50;
  for (let i = 0; i < maxPages; i += 1) {
    const body = { page_size: pageSize };
    if (cursor) body.start_cursor = cursor;
    if (opts.filter) body.filter = opts.filter;
    if (opts.sorts) body.sorts = opts.sorts;
    const res = await notionRequest(`/databases/${dbId}/query`, { method: 'POST', body, cfg });
    if (!res || !Array.isArray(res.results)) {
      throw new NotionApiError('queryDatabase', 'malformed response (missing results)', { body: res });
    }
    pages.push(...res.results);
    if (!res.has_more || !res.next_cursor) {
      return { pages, totalPages: pages.length };
    }
    cursor = res.next_cursor;
  }
  throw new NotionApiError('queryDatabase', `pagination did not complete in ${maxPages} iterations (got ${pages.length} pages)`);
}

/**
 * Notion master page の properties を update する。
 *   Phase E-11 後追い: Yahoo!カテゴリID と Yahoo!path を Notion 直書きする経路。
 *   secret hygiene 上 RYS_NOTION_TOKEN は env のみで、 関数内で getConfig() 経由でしか触らない。
 *
 * @param {string} pageId  Notion page UUID (notion_overrides.notion_page_id)
 * @param {object} props   { 'Yahoo!カテゴリID': 43494, 'Yahoo!path': '生活雑貨・日用品:掃除用品' }
 *   Notion property 名は商品マスター DB のプロパティ表示名 (extractors と同名)
 */
export async function patchPageProperties(pageId, props, opts = {}) {
  if (!pageId || typeof pageId !== 'string') throw new Error('patchPageProperties: pageId required');
  if (!props || typeof props !== 'object') throw new Error('patchPageProperties: props required');
  // Notion API は properties value を type 別の object で要求する
  const properties = {};
  for (const [name, value] of Object.entries(props)) {
    if (value === null || value === undefined) continue;
    if (typeof value === 'number') {
      properties[name] = { number: value };
    } else if (typeof value === 'string') {
      properties[name] = { rich_text: [{ type: 'text', text: { content: value } }] };
    } else {
      // pre-formatted Notion property value をそのまま渡せる脱出口
      properties[name] = value;
    }
  }
  return await notionRequest(`/pages/${pageId}`, { method: 'PATCH', body: { properties }, ...opts });
}

/**
 * Phase E-14 (2026-06-28): Notion master DB に新規 page を作成する。
 *   楽天 RMS → Notion master 未登録 SKU を一括追加するための endpoint で使う。
 *
 * @param {object} properties  Notion API の properties value 形式
 *   (例: { 'Name': {title:[...]}, '商品コード': {rich_text:[...]}, ... })
 * @param {object} [opts]      { cfg?, databaseId? }
 * @returns {object} Notion page object (含 id)
 */
export async function createPage(properties, opts = {}) {
  if (!properties || typeof properties !== 'object') throw new Error('createPage: properties required');
  const cfg = { ...getConfig(), ...(opts.cfg || {}) };
  const dbId = opts.databaseId || cfg.databaseId;
  if (!dbId) throw new Error('createPage: databaseId required');
  return await notionRequest('/pages', {
    method: 'POST',
    body: { parent: { database_id: dbId }, properties },
    cfg,
    ...(opts.maxRetries != null ? { maxRetries: opts.maxRetries } : {}),
  });
}

/**
 * Phase E-14: Notion master DB を `商品コード` で query して既存 page を検索。
 *   重複 page 作成防止の pre-check で使う (Codex R1 H-1 第 2 段)。
 *
 * @param {string} manageNumber 楽天 manage_number (= Notion `商品コード` rich_text)
 * @returns {object|null} 一致した Notion page object (なければ null)
 */
export async function findPageByManageNumber(manageNumber, opts = {}) {
  if (!manageNumber || typeof manageNumber !== 'string') {
    throw new Error('findPageByManageNumber: manageNumber required');
  }
  const cfg = { ...getConfig(), ...(opts.cfg || {}) };
  const dbId = opts.databaseId || cfg.databaseId;
  const body = {
    filter: {
      property: '商品コード',
      rich_text: { equals: manageNumber },
    },
    page_size: 1,
  };
  const res = await notionRequest(`/databases/${dbId}/query`, {
    method: 'POST',
    body,
    cfg,
    ...(opts.maxRetries != null ? { maxRetries: opts.maxRetries } : {}),
  });
  return res?.results?.[0] || null;
}

// テスト用 (rate-limit 状態をリセット)
export function _resetForTest() {
  _lastRequestAt = 0;
}
