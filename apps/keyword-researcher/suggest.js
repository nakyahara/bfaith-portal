/**
 * Amazon サジェスト取得モジュール
 *
 * 叩く口 = https://completion.amazon.co.jp/api/2017/suggestions (Amazon の検索ボックスが裏で呼ぶ JSON の口)。
 * 🚨 HTML の解析はしない (スクレイピングではない) が、**公式 API でもない** — SP-API にも Ads API にも無く、
 *   規約・レート制限の取り決めが無い。人が検索ボックスに打つのと同じ回線 (miniPC) から、同じ頻度帯で叩く前提
 *   (中原さん 2026-09-23『Amazon_SP広告KW自動生成_設計方針_20260922.md』§1)。Render (データセンター IP) からは叩かない。
 *
 * 2026-09-23 SP広告KW PR1 で作り直した点 (Codex 設計レビュー R1 #8 / R2 #3):
 *   - prefix ごとに success / empty / failed / unrun を返す。**通信エラーを「0 件」と混ぜない**
 *     (混ぜると「十分に調べて何も無かった」に見える)
 *   - タイムアウト (AbortController) と再試行 1 回。上限で打ち切った prefix は unrun として残す
 *   - 既存の戻り値 (seed / total / suggestions[]{keyword, source, depth}) はそのまま。MCP と router は結果を素通しするだけ
 *   - User-Agent は既定でブラウザ (これまでどおり)。`userAgent: 'plain'` で素の UA。
 *     素の UA で同じ結果が返ることを確かめたら既定を切り替える (偽装をやめる)
 */

const SUGGEST_URL = 'https://completion.amazon.co.jp/api/2017/suggestions';
const MARKETPLACE_ID = 'A1VC38T7YXB528'; // Amazon.co.jp
const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';
const PLAIN_UA = 'bfaith-portal keyword-suggest/1.0';

// 五十音 (46 文字) + アルファベット（掛け合わせ用）
const HIRAGANA = [
  'あ','い','う','え','お','か','き','く','け','こ',
  'さ','し','す','せ','そ','た','ち','つ','て','と',
  'な','に','ぬ','ね','の','は','ひ','ふ','へ','ほ',
  'ま','み','む','め','も','や','ゆ','よ',
  'ら','り','る','れ','ろ','わ','を','ん',
];
const ALPHABET = 'abcdefghijklmnopqrstuvwxyz'.split('');

/** テストで Amazon を呼ばないための差し替え口 */
let fetchImpl = (...args) => globalThis.fetch(...args);
export function _setFetchForTest(fn) { fetchImpl = fn || ((...args) => globalThis.fetch(...args)); }

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * prefix 1 つ分を取る (状態つき)。throw しない。
 * @returns {Promise<{status:'success'|'empty'|'failed', suggestions:string[], error:string|null, httpStatus:number|null}>}
 */
async function fetchOne(prefix, { timeoutMs = 8000, userAgent = 'browser' } = {}) {
  const params = new URLSearchParams({ mid: MARKETPLACE_ID, alias: 'aps', prefix });
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetchImpl(`${SUGGEST_URL}?${params}`, {
      headers: {
        'User-Agent': userAgent === 'plain' ? PLAIN_UA : BROWSER_UA,
        'Accept': 'application/json',
      },
      signal: ac.signal,
    });
    if (!res.ok) return { status: 'failed', suggestions: [], error: `HTTP ${res.status}`, httpStatus: res.status };
    let data;
    try { data = await res.json(); } catch (e) { return { status: 'failed', suggestions: [], error: 'JSON でない応答', httpStatus: res.status }; }
    const suggestions = (Array.isArray(data?.suggestions) ? data.suggestions : [])
      .map(s => (s && typeof s.value === 'string') ? s.value.trim() : '')
      .filter(Boolean);
    return { status: suggestions.length ? 'success' : 'empty', suggestions, error: null, httpStatus: res.status };
  } catch (err) {
    const msg = err && err.name === 'AbortError' ? `timeout ${timeoutMs}ms` : String(err && err.message || err);
    return { status: 'failed', suggestions: [], error: msg, httpStatus: null };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * 単一キーワードのサジェストを取得 (互換: 配列だけ返す。失敗は [] — 状態が要るときは getSuggestions を使う)
 * @param {string} prefix
 * @returns {Promise<string[]>}
 */
async function fetchSuggestions(prefix) {
  const r = await fetchOne(prefix);
  if (r.status === 'failed') console.error(`[Suggest] "${prefix}" 取得エラー:`, r.error);
  return r.suggestions;
}

/**
 * キーワードのサジェストを網羅的に取得
 * @param {string} seed - シードキーワード
 * @param {object} options
 * @param {boolean} options.hiragana - 五十音掛け合わせ（デフォルト: true）
 * @param {boolean} options.alphabet - アルファベット掛け合わせ（デフォルト: false）
 * @param {number} options.depth - 深掘り階層数（デフォルト: 1）
 * @param {number} options.delayMs - リクエスト間隔ms（デフォルト: 200）
 * @param {number} options.timeoutMs - 1 回の取得の上限ms（デフォルト: 8000）
 * @param {number} options.maxRequests - 総リクエスト数の上限。超えた prefix は unrun（デフォルト: 0 = 上限なし）
 * @param {number} options.retries - 失敗した prefix の再試行回数（デフォルト: 1）
 * @param {'browser'|'plain'} options.userAgent - 送る UA（デフォルト: browser）
 * @returns {Promise<object>} { seed, total, suggestions:[{keyword, source, depth}], prefixes:[{prefix, status, count, error, fetchedAt, attempts}], summary, fetchedAt }
 */
async function getSuggestions(seed, options = {}) {
  const {
    hiragana = true,
    alphabet = false,
    depth = 1,
    delayMs = 200,
    timeoutMs = 8000,
    maxRequests = 0,
    retries = 1,
    userAgent = 'browser',
  } = options;

  const allKeywords = new Map(); // keyword -> { source, depth }
  const prefixes = [];           // 取りに行った (行かなかった) prefix の記録
  let requests = 0;
  const capped = () => maxRequests > 0 && requests >= maxRequests;

  /** 1 prefix を取って記録する。上限に達していれば unrun。再試行は失敗のときだけ */
  async function collect(prefix, source, depthLevel, { first = false } = {}) {
    if (capped()) {
      prefixes.push({ prefix, source, status: 'unrun', count: 0, error: 'maxRequests に達したため未実行', fetchedAt: null, attempts: 0 });
      return;
    }
    if (!first) await delay(delayMs);
    let r = null, attempts = 0;
    for (let i = 0; i <= retries; i++) {
      if (i > 0) { if (capped()) break; await delay(delayMs * 2); }
      attempts++; requests++;
      r = await fetchOne(prefix, { timeoutMs, userAgent });
      if (r.status !== 'failed') break;
    }
    prefixes.push({ prefix, source, status: r.status, count: r.suggestions.length, error: r.error, fetchedAt: new Date().toISOString(), attempts });
    for (const kw of r.suggestions) {
      if (!allKeywords.has(kw)) allKeywords.set(kw, { source, depth: depthLevel });
    }
  }

  // 1. ベースサジェスト取得
  await collect(seed, 'base', 0, { first: true });

  // 2. 五十音掛け合わせ
  if (hiragana) {
    for (const char of HIRAGANA) await collect(`${seed} ${char}`, `hiragana:${char}`, 0);
  }

  // 3. アルファベット掛け合わせ
  if (alphabet) {
    for (const char of ALPHABET) await collect(`${seed} ${char}`, `alphabet:${char}`, 0);
  }

  // 4. 深掘り（depth >= 2 の場合、取得したサジェストをさらに展開）。広告KWの収集では使わない (設計 §4.3)
  if (depth >= 2) {
    const level1Keywords = [...allKeywords.keys()];
    for (const kw of level1Keywords) await collect(kw, `deep:${kw}`, 1);
  }

  // 結果を配列に変換 (seed そのものは除く)
  const suggestions = [];
  for (const [keyword, meta] of allKeywords) {
    if (keyword.toLowerCase() !== seed.toLowerCase()) suggestions.push({ keyword, ...meta });
  }
  suggestions.sort((a, b) => a.keyword.localeCompare(b.keyword, 'ja'));

  const summary = { requested: prefixes.length, success: 0, empty: 0, failed: 0, unrun: 0, requests };
  for (const p of prefixes) summary[p.status]++;

  return { seed, total: suggestions.length, suggestions, prefixes, summary, fetchedAt: new Date().toISOString(), options: { hiragana, alphabet, depth, delayMs, timeoutMs, maxRequests, retries, userAgent } };
}

export { fetchSuggestions, fetchOne, getSuggestions, HIRAGANA, ALPHABET };
