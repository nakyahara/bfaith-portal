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
  constructor(endpoint, message, { status = null, body = null, cause = null } = {}) {
    super(`[notion:${endpoint}] ${message}`);
    this.name = 'NotionApiError';
    this.endpoint = endpoint;
    this.status = status;
    this.body = body;
    if (cause) this.cause = cause;
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
  const cfg = opts.cfg || getConfig();
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
      if (attempt >= maxRetries) {
        throw new NotionApiError(endpoint, `${res.status} after ${maxRetries + 1} attempts`, { status: res.status, body: res.body });
      }
      const retryAfter = parseRetryAfter(res.headers);
      const wait = retryAfter !== null ? retryAfter : backoffDelay(attempt);
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
  const cfg = opts.cfg || getConfig();
  const dbId = cfg.databaseId;
  const pages = [];
  let cursor = null;
  const pageSize = opts.pageSize || 100;
  for (let i = 0; i < 50; i += 1) {
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
  throw new NotionApiError('queryDatabase', `pagination did not complete in 50 iterations (got ${pages.length} pages)`);
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

// テスト用 (rate-limit 状態をリセット)
export function _resetForTest() {
  _lastRequestAt = 0;
}
