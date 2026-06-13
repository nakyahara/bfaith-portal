/**
 * Yahoo!ショッピング myItemList を VPS proxy 経由で取得する client (Phase E-7-a)。
 *
 * 設計原則:
 *   - Render 上に Yahoo OAuth キーを置かない。 VPS proxy 経由 (E-5b-A PR #279 と同じパターン)
 *   - 必須 env: YAHOO_PROXY_BASE_URL / YAHOO_PROXY_SECRET
 *   - VPS proxy 側で新規 endpoint /yahoo/my-item-list を実装する必要がある (中原さん作業、 PR #279 同型)
 *
 * VPS proxy contract (期待):
 *   POST /yahoo/my-item-list
 *   Headers: X-Proxy-Secret
 *   Body:    { query: string, offset?: number, results?: number }
 *   Response: {
 *     ok: true,
 *     totalResultsAvailable: number,
 *     totalResultsReturned: number,
 *     firstResultPosition: number,
 *     items: [{ ItemCode: string, Name?: string, HasSubCode?: boolean | '0'|'1' }, ...]
 *   }
 *
 * R2 High A-6: totalResultsAvailable と取得件数の不一致は baseline incomplete 扱い (呼び出し側で扱う)。
 * R2 High A-6: page 切り (results 100 まで) は呼び出し側で iterate、 retry / jitter は呼び出し側 helper に寄せる。
 */

const DEFAULT_TIMEOUT_MS = 60_000;
const MAX_RESULTS_PER_PAGE = 100; // myItemList max

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    const err = new Error(`${name} not configured on Render (fail-closed)`);
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

async function callProxy(path, body, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const url = `${getProxyBaseUrl()}${path}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: getProxyHeaders(),
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  let parsed = null;
  try { parsed = await res.json(); } catch (_) { /* non-JSON */ }
  if (!res.ok || (parsed && parsed.ok === false)) {
    const msg = (parsed && (parsed.error || parsed.message)) || `HTTP ${res.status}`;
    const err = new Error(`yahoo-myitemlist-proxy ${path} failed: ${msg}`);
    err.statusCode = res.status;
    err.proxyResponse = parsed;
    throw err;
  }
  if (!parsed) {
    throw new Error(`yahoo-myitemlist-proxy ${path} returned non-JSON (HTTP ${res.status})`);
  }
  return parsed;
}

/**
 * 1 query (英数字 1 文字) の myItemList を全件取得する async generator。
 * VPS proxy が 100 件ずつ返してくる前提で offset を進めながら回す。
 * R2 High A-6: totalResultsAvailable と totalResultsReturned の不一致を呼び出し側 (syncYahooBaseline) が
 * 検出できるよう、 result/totalResultsAvailable を 1 page ずつ yield する。
 *
 * @param {string} query    — 検索 query (英数字 1 文字)
 * @param {object} opts
 * @param {number} opts.pageSize    — 1 page あたりの取得件数 (default 100)
 * @param {function} opts.delayMs   — page 間 jitter 関数 (PR #209 helper を渡す)
 * @param {function} opts.callApi   — VPS proxy 呼び出し関数 (テスト時にモック)
 * @yields {{ items: array, totalResultsAvailable: number, page: number, offset: number }}
 */
async function* iterateMyItemList(query, opts = {}) {
  // Codex E-7-a R1 Medium-2: pageSize validation。
  //   `||` だと 0 が 100 に化ける、 100 超は VPS proxy 側で受け入れられても Yahoo 仕様外。
  const pageSize = opts.pageSize ?? MAX_RESULTS_PER_PAGE;
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > MAX_RESULTS_PER_PAGE) {
    throw new Error(`iterateMyItemList: pageSize must be integer 1..${MAX_RESULTS_PER_PAGE} (got ${pageSize})`);
  }
  const delay = opts.delayMs || (() => 0);
  const callApi = opts.callApi || ((body) => callProxy('/yahoo/my-item-list', body, { timeoutMs: DEFAULT_TIMEOUT_MS }));

  let offset = 0;
  let page = 0;
  let totalResultsAvailable = null;

  while (true) {
    if (page > 0) {
      const ms = delay(page);
      if (ms > 0) await new Promise((resolve) => setTimeout(resolve, ms));
    }
    const res = await callApi({ query, offset, results: pageSize });
    // Codex E-7-a R6 High: items 欠落 / null / 非 array は contract 違反 → throw (sync 全体 failed)。
    //   [] へ丸めると 「items 0 件 + totalResultsReturned=0」 で all-complete 扱いになり空 baseline 確立する穴。
    if (!Array.isArray(res.items)) {
      throw new Error(
        `yahoo-myitemlist-proxy: response.items must be an array ` +
        `(query=${query}, offset=${offset}, gotType=${res.items === null ? 'null' : typeof res.items})`
      );
    }
    const items = res.items;
    if (totalResultsAvailable === null) totalResultsAvailable = res.totalResultsAvailable ?? null;

    // Codex E-7-a R5 High-2: 補完しない (proxy 報告を raw 値のまま yield)。
    //   補完を入れると store-sync 側で consistency 検証 (報告 vs 実数 / 位置 vs offset+1) が無効化される。
    //   呼び出し側が補完が必要なら自分で `?? items.length` 等を書くこと。
    yield {
      items,
      totalResultsAvailable: res.totalResultsAvailable ?? null,
      totalResultsReturned: res.totalResultsReturned,
      firstResultPosition: res.firstResultPosition,
      page,
      offset,
    };

    if (items.length === 0) break;
    if (items.length < pageSize) break;
    if (totalResultsAvailable !== null && offset + items.length >= totalResultsAvailable) break;

    offset += items.length;
    page += 1;

    // 暴走防止: 100 page (10,000 件) は myItemList の現実的上限を超えるので保険
    if (page > 100) {
      throw new Error(
        `yahoo-myitemlist-proxy iterate runaway: query=${query} page>100 ` +
        `(offset=${offset}, totalAvailable=${totalResultsAvailable})`
      );
    }
  }
}

export {
  MAX_RESULTS_PER_PAGE,
  iterateMyItemList,
  // testing
  callProxy,
};
