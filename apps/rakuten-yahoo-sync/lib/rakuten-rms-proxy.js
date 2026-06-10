/**
 * 楽天 RMS 商品 API を miniPC bfaith-portal 経由で叩く client (mercari-sync 同型)。
 *
 * 設計原則 (Codex Phase E R3-R4 確定):
 *   - Render 上に楽天キー (RAKUTEN_SERVICE_SECRET / LICENSE_KEY) を置かない
 *   - miniPC bfaith-portal の /service-api/rakuten-rms/* を Cloudflare Tunnel 経由で叩く
 *   - 必須 env: WAREHOUSE_URL / WAREHOUSE_SERVICE_TOKEN / CF_ACCESS_CLIENT_ID / CF_ACCESS_CLIENT_SECRET
 *
 * 提供 endpoint (mercari-sync で実証済):
 *   GET  /service-api/rakuten-rms/items/all-codes        — itemNumber/manageNumber マップ (warehouse cache)
 *   POST /service-api/rakuten-rms/items/details-bulk     — 詳細 (chunk 50 件、 1 件取得もここで)
 *
 * 単一商品の詳細を取りたいときは details-bulk に 1 件入れる:
 *   const items = await fetchItemDetailsBulk([manageNumber]);
 *   const item = items[0];
 */

const ITEM_DETAILS_BULK_CHUNK_SIZE = 50;
const ITEM_DETAILS_BULK_TIMEOUT_MS = 110_000;
const ALL_CODES_TIMEOUT_MS = 300_000;

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    const err = new Error(`${name} not configured on Render (fail-closed)`);
    err.statusCode = 503;
    throw err;
  }
  return v.trim();
}

function getWarehouseUrl() {
  return requireEnv('WAREHOUSE_URL').replace(/\/+$/, '');
}

function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': requireEnv('CF_ACCESS_CLIENT_ID'),
    'CF-Access-Client-Secret': requireEnv('CF_ACCESS_CLIENT_SECRET'),
    'Authorization': `Bearer ${requireEnv('WAREHOUSE_SERVICE_TOKEN')}`,
    'Content-Type': 'application/json',
  };
}

async function callProxy(path, { method = 'GET', body = null, timeoutMs = 60_000 } = {}) {
  const url = `${getWarehouseUrl()}${path}`;
  const opts = {
    method,
    headers: getServiceHeaders(),
    signal: AbortSignal.timeout(timeoutMs),
  };
  if (body !== null) opts.body = JSON.stringify(body);
  const res = await fetch(url, opts);
  let parsed = null;
  try {
    parsed = await res.json();
  } catch (_) {
    /* non-JSON response */
  }
  if (!res.ok || (parsed && parsed.ok === false)) {
    const msg = parsed?.message || parsed?.error || `HTTP ${res.status}`;
    const err = new Error(`[rakuten-rms-proxy:${path}] ${msg}`);
    err.statusCode = res.status;
    err.body = parsed;
    throw err;
  }
  return parsed;
}

/**
 * 全商品の itemNumber → manageNumber マップを取得 (miniPC cache 経由)。
 */
export async function fetchAllItemCodes() {
  const data = await callProxy('/service-api/rakuten-rms/items/all-codes', { timeoutMs: ALL_CODES_TIMEOUT_MS });
  return data?.mapping || {};
}

/**
 * manageNumber 配列を渡して商品詳細を bulk 取得。
 *   1 件取得したい場合は [manageNumber] で呼んで .items[0] を取り出す。
 */
/**
 * bulk 詳細取得。 失敗 code の情報を残すため { items, failed } 形式で返す。
 *   warehouse 側 (rakuten-rms-service.js) は HTTP 200 + status='partial' で個別失敗を返す仕様。
 *   failed[i] = { manageNumber, reason } を集めて caller に返す。
 *   Codex E-4 R1 M-1 対応。
 */
export async function fetchItemDetailsBulkDetailed(manageNumbers) {
  if (!Array.isArray(manageNumbers) || manageNumbers.length === 0) return { items: [], failed: [] };
  const items = [];
  const failed = [];
  for (let i = 0; i < manageNumbers.length; i += ITEM_DETAILS_BULK_CHUNK_SIZE) {
    const chunk = manageNumbers.slice(i, i + ITEM_DETAILS_BULK_CHUNK_SIZE);
    const data = await callProxy('/service-api/rakuten-rms/items/details-bulk', {
      method: 'POST',
      body: { itemCodes: chunk },
      timeoutMs: ITEM_DETAILS_BULK_TIMEOUT_MS,
    });
    if (Array.isArray(data?.items)) items.push(...data.items);
    // warehouse は failedCodes / failed / errors のどれかで返す可能性あり、 全部拾う
    const failedList = data?.failedCodes || data?.failed || data?.errors || [];
    if (Array.isArray(failedList)) {
      for (const f of failedList) {
        if (typeof f === 'string') failed.push({ manageNumber: f, reason: 'unknown' });
        else if (f && typeof f === 'object') failed.push({ manageNumber: f.manageNumber || f.itemCode || f.code || '?', reason: f.reason || f.error || f.message || data?.status || 'unknown' });
      }
    }
  }
  return { items, failed };
}

/**
 * 後方互換 wrapper (items 配列だけ返す)。 caller が失敗情報を必要としないとき用。
 */
export async function fetchItemDetailsBulk(manageNumbers) {
  return (await fetchItemDetailsBulkDetailed(manageNumbers)).items;
}

/**
 * 1 件取得 wrapper。 partial failure や not found を区別して返す。
 *   - 成功: { item, status: 'found' }
 *   - 個別失敗 (RMS 500/404 等): { item: null, status: 'failed', reason }
 *   - bulk に items=[], failed=[] (= 不明): { item: null, status: 'not_found' }
 */
export async function fetchItemDetail(manageNumber) {
  const { items, failed } = await fetchItemDetailsBulkDetailed([manageNumber]);
  if (items.length > 0) return { item: items[0], status: 'found' };
  if (failed.length > 0) {
    return { item: null, status: 'failed', reason: failed[0].reason || 'rms_failure' };
  }
  return { item: null, status: 'not_found' };
}
