/**
 * product-hub → miniPC の service-api「Amazon サジェスト収集」を呼ぶクライアント (SP広告KW PR1・2026-09-23)
 *
 * 口 = miniPC の POST /service-api/keyword-suggest (apps/warehouse/keyword-suggest-service.js)。
 * 🚨 Render からは Amazon を直接叩かない (サジェストの口は公式 API ではない → 会社の回線から人と同じ頻度帯で。
 *   『Amazon_SP広告KW自動生成_設計方針_20260922.md』§1)。**miniPC が落ちていれば「いま取れません」で止める**。
 *   Render 側で代わりに取りに行く経路は作らない。
 *
 * 作りは apps/fba-box/images.js の miniPC 呼び出しと同じ (CF Access + Bearer・redirect は追わない・タイムアウト)。
 * 種 KW 1 つ = 47 回 × 0.2 秒 ≈ 10 秒 (再試行込みで最大 20 秒前後) なので、1 回の呼び出しは 1 種だけ。
 */

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
const TIMEOUT_MS = 45_000;   // 1 種 ≈ 10〜20 秒 + 往復。Render のプロキシ (~100 秒) の内側に収める

export function suggestConfigured() {
  return !!process.env.WAREHOUSE_SERVICE_TOKEN;
}

function serviceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    Authorization: `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    Accept: 'application/json',
    'Content-Type': 'application/json',
  };
}

/** 実際に miniPC を叩く関数。テストで差し替え可 (null を渡すと既定に戻る) */
const defaultFetcher = async (body) => {
  const res = await fetch(`${WAREHOUSE_URL}/service-api/keyword-suggest`, {
    method: 'POST', headers: serviceHeaders(), redirect: 'manual', body: JSON.stringify(body),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  let j = null;
  try { j = await res.json(); } catch (_) { /* JSON でない (プロキシのエラーページ等) */ }
  if (res.status === 429) { const e = new Error('miniPC が別の収集を処理中です。少し待ってからもう一度押してください'); e.code = 'busy'; throw e; }
  if (!res.ok) { const e = new Error(`miniPC が応答できません (HTTP ${res.status})`); e.code = 'unreachable'; throw e; }
  if (!j || j.ok !== true || !j.result) { const e = new Error((j && (j.message || j.error)) || 'miniPC の応答を解釈できません'); e.code = 'bad_response'; throw e; }
  return j.result;
};
let fetcher = defaultFetcher;
export function _setSuggestFetcher(fn) { fetcher = fn || defaultFetcher; }

const PREFIX_STATUSES = ['success', 'empty', 'failed', 'unrun'];
const nonNegInt = (v) => Number.isInteger(v) && v >= 0;

/**
 * miniPC の応答の形を検査する。壊れていれば理由の文字列、正常なら null。
 * 「0 件」と「壊れている」を混ぜないために、内訳の欠落・矛盾・未知の状態も受け取らない (PR #1408 R2 #8)
 */
export function validateSuggestResult(result, seed) {
  const s = result?.summary;
  if (!Array.isArray(result?.prefixes) || !s || typeof s !== 'object') {
    return 'miniPC の応答に取得状態 (prefixes/summary) がありません (miniPC の版が古い可能性)';
  }
  if (!Array.isArray(result.suggestions)) return 'miniPC の応答に suggestions がありません';
  if (result.seed !== seed) return `miniPC の応答が別の種のものです (${result.seed})`;
  for (const k of ['requested', 'success', 'empty', 'failed', 'unrun']) {
    if (!nonNegInt(s[k])) return `miniPC の応答の取得状態が壊れています (summary.${k} が数でない)`;
  }
  if (s.requested <= 0 || result.prefixes.length !== s.requested) {
    return `miniPC の応答の取得状態が壊れています (prefix ${result.prefixes.length} 件 / requested ${s.requested})`;
  }
  if (s.success + s.empty + s.failed + s.unrun !== s.requested) {
    return `miniPC の応答の取得状態が壊れています (内訳の合計 ${s.success + s.empty + s.failed + s.unrun} ≠ requested ${s.requested})`;
  }
  const counts = { success: 0, empty: 0, failed: 0, unrun: 0 };
  for (const p of result.prefixes) {
    if (!p || !PREFIX_STATUSES.includes(p.status)) return `miniPC の応答に未知の prefix 状態があります (${p && p.status})`;
    counts[p.status] += 1;
  }
  for (const k of PREFIX_STATUSES) {
    if (counts[k] !== s[k]) return `miniPC の応答の取得状態が壊れています (${k}: prefix 別 ${counts[k]} ≠ summary ${s[k]})`;
  }
  if (result.suggestions.some((x) => !x || typeof x.keyword !== 'string')) return 'miniPC の応答の suggestions に keyword の無い要素があります';
  return null;
}

/**
 * 種 KW 1 つのサジェストを miniPC に頼む。
 * @param {string} seed
 * @param {{alphabet?: boolean}} [opts]
 * @returns {Promise<{ok:true, result:object}|{ok:false, code:string, message:string}>} throw しない
 *   result = suggest.js の戻り値 (seed / total / suggestions[] / prefixes[] / summary / fetchedAt / options)
 */
export async function collectSuggestions(seed, { alphabet = false } = {}) {
  if (!suggestConfigured()) {
    return { ok: false, code: 'not_configured', message: 'Render の WAREHOUSE_SERVICE_TOKEN が未設定です (miniPC を呼べません)' };
  }
  try {
    const result = await fetcher({ seed, hiragana: true, alphabet: !!alphabet });
    // 呼び手が「失敗」と「0 件」を見分けられるよう、状態の無い応答は受け取らない。
    // 形だけ揃った空の応答 ({prefixes:[], summary:{}}) や別の種の応答も「0 件」にしない (PR #1408 R1 #4)
    const bad = validateSuggestResult(result, seed);
    if (bad) return { ok: false, code: 'bad_response', message: bad };
    return { ok: true, result };
  } catch (e) {
    const timeout = e && (e.name === 'TimeoutError' || e.name === 'AbortError');
    return {
      ok: false,
      code: e.code || (timeout ? 'timeout' : 'unreachable'),
      message: timeout ? `miniPC からの応答が ${TIMEOUT_MS / 1000} 秒以内に来ませんでした` : (e.message || String(e)),
    };
  }
}
