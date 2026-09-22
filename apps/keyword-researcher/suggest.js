/**
 * Amazon サジェスト取得モジュール
 *
 * 叩く口 = https://completion.amazon.co.jp/api/2017/suggestions (Amazon の検索ボックスが裏で呼ぶ JSON の口)。
 * 🚨 HTML の解析はしない (スクレイピングではない) が、**公式 API でもない** — SP-API にも Ads API にも無く、
 *   規約・レート制限の取り決めが無い。人が検索ボックスに打つのと同じ回線 (miniPC) から、同じ頻度帯で叩く前提
 *   (中原さん 2026-09-23『Amazon_SP広告KW自動生成_設計方針_20260922.md』§1)。Render (データセンター IP) からは叩かない。
 *
 * 2026-09-23 SP広告KW PR1 で作り直した点 (Codex 設計レビュー R1 #8 / R2 #3、PR #1408 レビュー R1 #1 #4・R2 #1 #2 #7 #8):
 *   - prefix ごとに success / empty / failed / unrun を返す。**通信エラーを「0 件」と混ぜない**
 *     (混ぜると「十分に調べて何も無かった」に見える)。HTTP 200 でも形が想定外なら failed
 *   - 1 回のタイムアウト (残り時間の内側) と再試行 1 回。上限で打ち切った prefix は unrun として残す
 *   - 全体の期限 (deadlineMs) と外からの中断 (signal) は**実行中の取得と待ちにも効く** (run 用の AbortController に集約)。
 *     打ち切りで終わった試行は unrun (理由つき)、確定した失敗 (503 など) は打ち切られても failed のまま。summary.stopped に理由
 *   - 🚨 fetch が signal を無視しても戻る (raceAbort)。戻ったあと裏で残るのは「送信済みの 1 リクエスト」だけで、新しい送信はしない
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

const abortError = () => { const e = new Error('aborted'); e.name = 'AbortError'; return e; };

/** 待つ。signal が中断されたら待ち切らずに戻る */
function delay(ms, signal) {
  return new Promise(resolve => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() { clearTimeout(t); signal?.removeEventListener('abort', done); resolve(); }
    signal?.addEventListener('abort', done, { once: true });
  });
}

/** promise が signal を無視しても、中断されたら AbortError で戻る (待ち続けない) */
function raceAbort(promise, signal) {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(abortError());
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(abortError());
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (v) => { signal.removeEventListener('abort', onAbort); resolve(v); },
      (e) => { signal.removeEventListener('abort', onAbort); reject(e); },
    );
  });
}

/**
 * prefix 1 つ分を取る (状態つき)。throw しない。
 * @param {object} opts timeoutMs = この 1 回の上限 / signal = 外からの中断 (期限・切断)
 * @returns {Promise<{status:'success'|'empty'|'failed', suggestions:string[], error:string|null, httpStatus:number|null, aborted:boolean}>}
 *   aborted = **外からの中断で終わった** (自分のタイムアウトや HTTP エラーではない)。呼び手はこれを unrun にする
 */
async function fetchOne(prefix, { timeoutMs = 8000, userAgent = 'browser', signal = null } = {}) {
  const params = new URLSearchParams({ mid: MARKETPLACE_ID, alias: 'aps', prefix });
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), Math.max(1, timeoutMs));
  const onOuterAbort = () => ac.abort();
  if (signal) { if (signal.aborted) ac.abort(); else signal.addEventListener('abort', onOuterAbort, { once: true }); }
  const outerAborted = () => !!(signal && signal.aborted);
  try {
    const res = await raceAbort(fetchImpl(`${SUGGEST_URL}?${params}`, {
      headers: {
        'User-Agent': userAgent === 'plain' ? PLAIN_UA : BROWSER_UA,
        'Accept': 'application/json',
      },
      signal: ac.signal,
    }), ac.signal);
    if (!res.ok) return { status: 'failed', suggestions: [], error: `HTTP ${res.status}`, httpStatus: res.status, aborted: false };
    let data;
    try {
      data = await raceAbort(res.json(), ac.signal);
    } catch (e) {
      if (ac.signal.aborted) throw e;   // 本文を読んでいる途中の timeout / 中断は「JSON でない」ではない
      return { status: 'failed', suggestions: [], error: 'JSON でない応答', httpStatus: res.status, aborted: false };
    }
    // HTTP 200 でも形が想定外 ({} / null / エラーオブジェクト / 仕様変更) は「0 件」ではなく failed (PR #1408 R1 #4)
    if (!data || typeof data !== 'object' || !Array.isArray(data.suggestions)) {
      return { status: 'failed', suggestions: [], error: '応答の形が想定外 (suggestions が配列でない)', httpStatus: res.status, aborted: false };
    }
    // 要素は全部 {value: string} のはず。1 つでも違えば黙って捨てずに failed (R2 #8)
    if (data.suggestions.some(s => !s || typeof s.value !== 'string')) {
      return { status: 'failed', suggestions: [], error: '応答の形が想定外 (value の無い要素あり)', httpStatus: res.status, aborted: false };
    }
    const suggestions = data.suggestions.map(s => s.value.trim()).filter(Boolean);
    return { status: suggestions.length ? 'success' : 'empty', suggestions, error: null, httpStatus: res.status, aborted: false };
  } catch (err) {
    const isAbort = err && err.name === 'AbortError';
    const aborted = isAbort && outerAborted();
    const msg = aborted ? '中断' : isAbort ? `timeout ${timeoutMs}ms` : String(err && err.message || err);
    return { status: 'failed', suggestions: [], error: msg, httpStatus: null, aborted };
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', onOuterAbort);
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
 * @param {number} options.timeoutMs - 1 回の取得の上限ms（デフォルト: 8000。期限の残りより長くはならない）
 * @param {number} options.maxRequests - 総リクエスト数の上限。超えた prefix は unrun（デフォルト: 0 = 上限なし）
 * @param {number} options.deadlineMs - 全体の期限ms。過ぎたら実行中の取得も止め、残りは unrun（デフォルト: 0 = 期限なし）
 * @param {AbortSignal} options.signal - 外からの中断。実行中の取得も止め、以後は unrun
 * @param {number} options.retries - 失敗した prefix の再試行回数（デフォルト: 1）
 * @param {'browser'|'plain'} options.userAgent - 送る UA（デフォルト: browser）
 * @returns {Promise<object>} { seed, total, suggestions:[{keyword, source, depth}], prefixes:[{prefix, status, count, error, fetchedAt, attempts}],
 *   summary:{requested, success, empty, failed, unrun, requests, stopped:null|'maxRequests'|'deadline'|'aborted'}, fetchedAt }
 */
async function getSuggestions(seed, options = {}) {
  const {
    hiragana = true,
    alphabet = false,
    depth = 1,
    delayMs = 200,
    timeoutMs = 8000,
    maxRequests = 0,
    deadlineMs = 0,
    signal = null,
    retries = 1,
    userAgent = 'browser',
  } = options;

  const allKeywords = new Map(); // keyword -> { source, depth }
  const prefixes = [];           // 取りに行った (行かなかった) prefix の記録
  const startedAt = Date.now();
  const deadlineAt = deadlineMs > 0 ? startedAt + deadlineMs : Infinity;
  let requests = 0;
  let stopped = null;            // 最初に打ち切った理由

  // 期限と外からの中断を 1 つの signal にまとめ、実行中の取得と待ちにも効かせる (R2 #1)
  const run = new AbortController();
  const onOuterAbort = () => run.abort();
  if (signal) { if (signal.aborted) run.abort(); else signal.addEventListener('abort', onOuterAbort, { once: true }); }
  const deadlineTimer = Number.isFinite(deadlineAt) ? setTimeout(() => run.abort(), deadlineMs) : null;

  /** これ以上取りに行かない理由 (null = 行ける)。外からの中断 > 期限 > 上限 の順に見る */
  const stopReason = () => {
    if (signal?.aborted) return 'aborted';
    if (Date.now() >= deadlineAt || run.signal.aborted) return 'deadline';
    if (maxRequests > 0 && requests >= maxRequests) return 'maxRequests';
    return null;
  };
  const STOP_TEXT = { maxRequests: 'maxRequests に達したため未実行', deadline: '全体の期限に達したため未実行', aborted: '中断されたため未実行' };
  const unrun = (prefix, source, reason, attempts = 0) => {
    stopped = stopped || reason;
    prefixes.push({ prefix, source, status: 'unrun', count: 0, error: STOP_TEXT[reason], fetchedAt: null, attempts });
  };

  /** 1 prefix を取って記録する。打ち切りなら unrun (理由つき)。再試行は失敗のときだけ */
  async function collect(prefix, source, depthLevel, { first = false } = {}) {
    let stop = stopReason();
    if (stop) return unrun(prefix, source, stop);
    if (!first) await delay(delayMs, run.signal);
    let r = null, attempts = 0;
    for (let i = 0; i <= retries; i++) {
      if (i > 0) { if (stopReason()) break; await delay(delayMs * 2, run.signal); }
      if (stopReason()) break;                       // 待っている間に打ち切られた → 手元の結果 (確定した失敗) はそのまま
      attempts++; requests++;
      // 1 回の timeout はそのまま渡す。期限は run.signal (期限のタイマー) が実行中の取得を止める → その試行は unrun (期限)
      r = await fetchOne(prefix, { timeoutMs, userAgent, signal: run.signal });
      if (r.status !== 'failed') break;
    }
    // 取りに行く前に打ち切られた / **この試行が**外からの中断・期限で終わった = 失敗ではなく「取れていない」(unrun)。
    // 503 や timeout で確定した失敗は、あとで打ち切られても failed のまま (R2 #7)
    if (!r || r.aborted) return unrun(prefix, source, stopReason() || 'aborted', attempts);
    prefixes.push({ prefix, source, status: r.status, count: r.suggestions.length, error: r.error, fetchedAt: new Date().toISOString(), attempts });
    for (const kw of r.suggestions) {
      if (!allKeywords.has(kw)) allKeywords.set(kw, { source, depth: depthLevel });
    }
  }

  try {
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
  } finally {
    if (deadlineTimer) clearTimeout(deadlineTimer);
    signal?.removeEventListener('abort', onOuterAbort);
  }

  // 結果を配列に変換 (seed そのものは除く)
  const suggestions = [];
  for (const [keyword, meta] of allKeywords) {
    if (keyword.toLowerCase() !== seed.toLowerCase()) suggestions.push({ keyword, ...meta });
  }
  suggestions.sort((a, b) => a.keyword.localeCompare(b.keyword, 'ja'));

  const summary = { requested: prefixes.length, success: 0, empty: 0, failed: 0, unrun: 0, requests, stopped };
  for (const p of prefixes) summary[p.status]++;

  return {
    seed, total: suggestions.length, suggestions, prefixes, summary, fetchedAt: new Date().toISOString(),
    options: { hiragana, alphabet, depth, delayMs, timeoutMs, maxRequests, deadlineMs, retries, userAgent },
  };
}

export { fetchSuggestions, fetchOne, getSuggestions, HIRAGANA, ALPHABET };
