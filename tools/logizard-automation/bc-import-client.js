/**
 * bc-import-client.js — ポータルの「ロジザードに取り込む」の機械の口 (apps/lz-bc-import/runner-router.js) を呼ぶ (miniPC の bc-import-runner.js)
 *
 * env: LZ_BC_RUNNER_TOKEN (Bearer・このフォルダの .env)・LZ_BC_PORTAL_URL (既定 https://bfaith-portal.onrender.com)
 * 届かない・断られた = BcClientError (呼び手は「押さない」側に倒す)。
 */
export const DEFAULT_URL = 'https://bfaith-portal.onrender.com';
export const BASE_PATH = '/apps/lz-bc-import/api';

export class BcClientError extends Error {
  constructor(code, message, status = null) { super(message); this.code = code; this.status = status; }
}

/**
 * @param {object} [o]
 * @param {string} [o.url]
 * @param {string} [o.token]
 * @param {typeof fetch} [o.fetchImpl]
 * @param {number} [o.timeoutMs]
 */
export function createBcImportClient({ url = process.env.LZ_BC_PORTAL_URL || DEFAULT_URL, token = process.env.LZ_BC_RUNNER_TOKEN, fetchImpl = fetch, timeoutMs = 20000 } = {}) {
  let origin;
  try { origin = new URL(String(url).trim()).origin; } catch { throw new BcClientError('bad_url', `LZ_BC_PORTAL_URL が URL でない: ${url}`); }
  if (!/^https:/.test(origin) && !/^http:\/\/(127\.0\.0\.1|localhost)(:\d+)?$/.test(origin)) throw new BcClientError('bad_url', 'https だけ (Bearer を載せるため。試験の localhost は除く)');
  if (!token) throw new BcClientError('no_token', 'LZ_BC_RUNNER_TOKEN が無い (このフォルダの .env)');
  async function call(p, body, ms = timeoutMs) {
    let res;
    try {
      res = await fetchImpl(`${origin}${BASE_PATH}${p}`, {
        method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {}), signal: AbortSignal.timeout(ms),
      });
    } catch (e) { throw new BcClientError('unreachable', `ポータルの口に届かない: ${String(e && e.message).slice(0, 160)}`); }
    let j = null;
    try { j = await res.json(); } catch { /* 下で */ }
    if (!res.ok || !j || j.ok === false) throw new BcClientError((j && j.error) || `http_${res.status}`, (j && j.message) || `HTTP ${res.status}`, res.status);
    return j;
  }
  return {
    claim: (b) => call('/claim', b),
    executing: (jobId, claimToken) => call(`/jobs/${encodeURIComponent(String(jobId))}/executing`, { claim_token: claimToken }),
    reportImport: (jobId, b) => call(`/jobs/${encodeURIComponent(String(jobId))}/import`, b),
    // ② の書き出しのファイル (約 0.8 MB) をポータルに送る → ポータルが共有ドライブに書く (Drive の応答を待つので長め)
    uploadExport: (jobId, b) => call(`/jobs/${encodeURIComponent(String(jobId))}/export-file`, b, Math.max(timeoutMs, 120000)),
    final: (jobId, b) => call(`/jobs/${encodeURIComponent(String(jobId))}/final`, b),
  };
}
