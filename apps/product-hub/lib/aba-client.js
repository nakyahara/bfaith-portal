/**
 * product-hub → miniPC の service-api「ABA 参照」を呼ぶクライアント (SP広告KW PR2-B2・2026-09-23)
 *
 * 口 = miniPC の POST /service-api/aba/lookup (apps/warehouse/aba-service.js・PR #1414)。
 * 🚨 **走査しない・レポートを取りに行かない**。miniPC の aba.db に**取込済みの週**を引くだけ (方針 B・
 *   『Amazon_SP広告KW自動生成_設計方針_20260922.md』§5「進め方の決定」)。Render から Amazon (SP-API) は呼ばない。
 *   miniPC が落ちていれば「いま引けません」で止める (代わりに取りに行く経路は作らない)。
 *
 * 作りは lib/keyword-suggest-client.js と同じ (CF Access + Bearer・redirect は追わない・タイムアウト・throw しない)。
 * 1 回の呼び出しは ASIN 1 つ (画面の「注文ワードを引く」1 押し = 1 ASIN。応答は数秒)。
 *
 * 応答の意味 (miniPC 側の契約。ここで形を検査してから呼び手に渡す):
 *   status = found (行がある。注文の証明ではない) / none (その週のクリック上位 3 に入っていない = 証明つき) /
 *            not_covered (取込が不完全で「無い」と言えない) / no_week (取込済みの週がまだ無い)
 *   coverage = complete / partial / unknown = その ASIN について「上位 3 の語を網羅している」と言えるか
 *   🚨 none は coverage=complete のときしか受け取らない (証明の無い「該当なし」を画面に出さない。該当なし ≠ 注文なし)
 */

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
const TIMEOUT_MS = 30_000;   // aba.db の索引引き = 1 秒前後。full 取込 (朝 7 時台) の書込と重なっても busy_timeout 10 秒の内側

export const ABA_STATUSES = ['found', 'none', 'not_covered', 'no_week'];
export const ABA_COVERAGES = ['complete', 'partial', 'unknown'];

export function abaConfigured() {
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
  const res = await fetch(`${WAREHOUSE_URL}/service-api/aba/lookup`, {
    method: 'POST', headers: serviceHeaders(), redirect: 'manual', body: JSON.stringify(body),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  let j = null;
  try { j = await res.json(); } catch (_) { /* JSON でない (プロキシのエラーページ等) */ }
  // 503 = miniPC が aba.db を開けない (文言は miniPC のものをそのまま)
  if (res.status === 503) { const e = new Error(`miniPC: ${(j && j.message) || 'ABA のデータベースを開けません'}`); e.code = 'unavailable'; throw e; }
  if (res.status === 400) { const e = new Error(`miniPC が受け付けません: ${(j && j.message) || `HTTP ${res.status}`}`); e.code = 'bad_request'; throw e; }
  if (!res.ok) { const e = new Error(`miniPC が応答できません (HTTP ${res.status})`); e.code = 'unreachable'; throw e; }
  if (!j || j.ok !== true || !j.result) { const e = new Error((j && (j.message || j.error)) || 'miniPC の応答を解釈できません'); e.code = 'bad_response'; throw e; }
  return j.result;
};
let fetcher = defaultFetcher;
export function _setAbaFetcher(fn) { fetcher = fn || defaultFetcher; }

const YMD = /^\d{4}-\d{2}-\d{2}$/;

/**
 * miniPC の応答の形を検査する。壊れていれば理由の文字列、正常なら null。
 * 「該当なし」と「壊れている」「証明が無い」を混ぜないために、状態の欠落・矛盾・未知の値は受け取らない
 */
export function validateAbaResult(result, asin) {
  if (!result || typeof result !== 'object' || !Array.isArray(result.items)) {
    return 'miniPC の応答に items がありません (miniPC の版が古い可能性)';
  }
  if (result.items.length !== 1) return `miniPC の応答の件数が違います (${result.items.length} 件 / 1 件を頼んだ)`;
  const it = result.items[0];
  if (!it || it.asin !== asin) return `miniPC の応答が別の ASIN のものです (${it && it.asin})`;
  if (!ABA_STATUSES.includes(it.status)) return `miniPC の応答に未知の状態があります (${it && it.status})`;
  if (!ABA_COVERAGES.includes(it.coverage)) return `miniPC の応答に未知の網羅状態があります (${it && it.coverage})`;
  if (!Array.isArray(it.terms)) return 'miniPC の応答に terms がありません';
  if (it.terms.some((t) => !t || typeof t.search_term !== 'string')) return 'miniPC の応答の terms に search_term の無い要素があります';
  if (it.status === 'found' && it.terms.length === 0) return 'miniPC の応答が壊れています (found なのに terms が空)';
  if (it.status !== 'found' && it.terms.length > 0) return `miniPC の応答が壊れています (${it.status} なのに terms がある)`;
  if (it.status === 'none' && it.coverage !== 'complete') return 'miniPC の応答が壊れています (証明の無い「該当なし」)';
  if (it.status === 'no_week') {
    if (result.week != null) return 'miniPC の応答が壊れています (no_week なのに週がある)';
    return null;
  }
  const w = result.week;
  if (!w || typeof w !== 'object' || !YMD.test(String(w.week_start || '')) || !YMD.test(String(w.week_end || ''))) {
    return 'miniPC の応答に対象週 (week.week_start / week_end) がありません';
  }
  return null;
}

/**
 * ASIN 1 つの ABA 検索語を miniPC に頼む (aba.db の取込済み週を引くだけ)。
 * @param {string} asin 正規化済み (10 桁)
 * @param {{weekStart?:string|null, register?:boolean}} [opts] weekStart = 固定したい週 (無ければ取込済みの最新週)。register = miniPC の監視 ASIN に登録 (保持期限のあとも語が残る)
 * @returns {Promise<{ok:true, result:object}|{ok:false, code:string, message:string}>} throw しない
 *   result = aba-service.js の戻り値 (week / requested_week / week_coverage / items[1] / registered / register_errors / invalid)
 */
export async function lookupAbaTerms(asin, { weekStart = null, register = false } = {}) {
  if (!abaConfigured()) {
    return { ok: false, code: 'not_configured', message: 'Render の WAREHOUSE_SERVICE_TOKEN が未設定です (miniPC を呼べません)' };
  }
  try {
    const body = { asins: [asin], register: !!register };
    if (weekStart) body.week_start = weekStart;
    const result = await fetcher(body);
    const bad = validateAbaResult(result, asin);
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
