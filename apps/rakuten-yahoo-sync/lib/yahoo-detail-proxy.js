/**
 * Phase E-11-b: VPS proxy 経由で Yahoo getItemDetail (= itemInfo) を呼ぶ薄いクライアント。
 *
 * 設計原則:
 *   - vps-proxy `/yahoo/get-item-detail` endpoint へ JSON POST
 *   - secret は env (YAHOO_PROXY_SECRET) で送る、 yahoo-publish-proxy と同パターン
 *   - response: { ok, ItemCode, ProductCategory, Path, Name, Price, SubCodes }
 *
 * 提供:
 *   - fetchYahooItemDetail(itemCode): Promise<{ ok, ItemCode, ProductCategory, Path, Name }>
 *   - fetchYahooItemDetailsBulk(itemCodes, { concurrency }): Promise<{ items, failed }>
 */

import { YahooProxyError } from './yahoo-publish-proxy.js';

const DEFAULT_TIMEOUT_MS = 20_000;
const DEFAULT_CONCURRENCY = 3;

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
  return {
    'X-Proxy-Secret': requireEnv('YAHOO_PROXY_SECRET'),
    'Content-Type': 'application/json',
  };
}

/**
 * 1 件の Yahoo 商品詳細を取得。
 *   res: { ok, ItemCode, ProductCategory: number|null, Path: string|null, Name: string|null }
 */
export async function fetchYahooItemDetail(itemCode, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  if (typeof itemCode !== 'string' || itemCode.trim() === '') {
    throw new Error('fetchYahooItemDetail: itemCode required');
  }
  const url = `${getProxyBaseUrl()}/yahoo/get-item-detail`;
  const res = await fetch(url, {
    method: 'POST',
    headers: getProxyHeaders(),
    body: JSON.stringify({ itemCode: itemCode.trim() }),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new YahooProxyError('get-item-detail', `HTTP ${res.status}`, { status: res.status, body: text.slice(0, 300) });
  }
  let parsed;
  try { parsed = JSON.parse(text); }
  catch (_) {
    throw new YahooProxyError('get-item-detail', 'invalid JSON response from vps-proxy', { body: text.slice(0, 300) });
  }
  if (!parsed?.ok) {
    throw new YahooProxyError('get-item-detail', `vps-proxy returned not ok`, { body: text.slice(0, 300) });
  }
  return {
    ok: true,
    ItemCode: parsed.ItemCode || itemCode.trim(),
    ProductCategory: Number.isFinite(parsed.ProductCategory) ? parsed.ProductCategory : null,
    Path: typeof parsed.Path === 'string' && parsed.Path.length > 0 ? parsed.Path : null,
    Name: typeof parsed.Name === 'string' && parsed.Name.length > 0 ? parsed.Name : null,
    // 設定価格 (価格一括改定ツール F2)。VPS 側の Price 抽出が未デプロイの間は undefined のまま来る。
    // 0 や null に丸めず素通しする — 呼び出し側が「未デプロイ/取得不能」と「0円」を区別できるように
    Price: parsed.Price,
    SubCodes: Array.isArray(parsed.SubCodes) ? parsed.SubCodes : undefined,
  };
}

/**
 * 複数 itemCode の category/path を bulk 取得 (chunk 単位で Promise.all、 簡易 concurrency)。
 *
 * @returns {Promise<{ items: Array<{ItemCode,ProductCategory,Path,Name}>, failed: Array<{itemCode,reason}> }>}
 */
export async function fetchYahooItemDetailsBulk(itemCodes, { concurrency = DEFAULT_CONCURRENCY } = {}) {
  if (!Array.isArray(itemCodes)) return { items: [], failed: [] };
  const items = [];
  const failed = [];
  for (let i = 0; i < itemCodes.length; i += concurrency) {
    const chunk = itemCodes.slice(i, i + concurrency);
    const results = await Promise.all(chunk.map((c) => fetchYahooItemDetail(c).then(
      (r) => ({ ok: true, r, c }),
      (e) => ({ ok: false, e, c }),
    )));
    for (const x of results) {
      if (x.ok) items.push(x.r);
      else failed.push({ itemCode: x.c, reason: x.e.message || String(x.e) });
    }
  }
  return { items, failed };
}
