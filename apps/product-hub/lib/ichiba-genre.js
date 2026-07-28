/**
 * 楽天市場ジャンルツリー + ジャンル自動提案 (2026-07-28 中原さん指示)。
 *
 * 背景: 入力者はジャンルIDを知らない。RMS 画面と同じ「ツリーをたどって選ぶ」UI が要る。
 * ただし RMS ナビゲーションAPI 2.0 は **root(0) 以外の子ジャンル一覧を返さない**
 * (2026-07-28 プローブ実測: children は常に null、キーワード検索も無し)。
 * → 公開の楽天ウェブサービス **IchibaGenre/Search** を使う (任意ノードの子が取れる。
 *   ジャンルID体系は RMS の genreId と同一)。applicationId は ranking-checker と同じ
 *   env RAKUTEN_APP_ID (Render 設定済み)。
 *
 * AI 初期値 (中原さん: 「カテゴリの初期値はAIが入れる想定。違うと思ったら人が修正」):
 *   IchibaItem/Search で商品名検索 → 上位ヒットのジャンル多数決 → 候補を返す。
 *   最終ジャンルは選択後に RMS の属性辞書 fetch (末端でなければ 404) で検証される。
 *
 * レート: 楽天ウェブサービスは 1req/秒目安 → 直列化 + 24h キャッシュ。
 */

// ⚠️ ホスト事情 (2026-07-28 実測):
//   - 新ホスト openapi.rakuten.co.jp/ichibams には IchibaItem はあるが **IchibaGenre は無い** (404)
//   - 旧ホスト app.rakuten.co.jp は両方生きているが、Render の RAKUTEN_APP_ID は
//     新システム (accessKey ペア) の ID なので旧ホストでは「specify valid applicationId」になる
//   → 商品検索 = 新ホスト (ranking-checker と同じ資格情報で今すぐ動く)
//     ジャンルツリー = 旧ホスト (専用の旧式 applicationId = env RAKUTEN_WS_APP_ID が必要)
const ICHIBA_GENRE_URL = 'https://app.rakuten.co.jp/services/api/IchibaGenre/Search/20140222';
const ICHIBA_ITEM_URL_NEW = 'https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20260401';
const ICHIBA_ITEM_URL_OLD = 'https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601';
const ICHIBA_HEADERS = {
  // ranking-checker の実績ヘッダー (新ホストはこれで通っている)
  'Origin': 'https://rakuten.co.jp',
  'Referer': 'https://rakuten.co.jp/',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
};
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const MIN_GAP_MS = 1100;

const childrenCache = new Map(); // genreId -> { fetchedAt, children, current, parents }
const inflight = new Map();      // genreId -> Promise (同一IDの並行要求はAPIを1回にする)
let queueTail = Promise.resolve();
let queueDepth = 0;
const MAX_QUEUE_DEPTH = 50;      // 操作集中時に待ち行列が伸び続けないための早期エラー
let lastCallAt = 0;

// 旧式 (app.rakuten.co.jp) の applicationId。ジャンルツリー専用。
// webservice.rakuten.co.jp で無料登録して Render env RAKUTEN_WS_APP_ID に設定する
function wsAppId() {
  return (process.env.RAKUTEN_WS_APP_ID || '').trim() || null;
}
// 新システム (openapi.rakuten.co.jp/ichibams) の資格情報。ranking-checker と共通
function msCreds() {
  const applicationId = (process.env.RAKUTEN_APP_ID || '').trim();
  const accessKey = (process.env.RAKUTEN_ACCESS_KEY || '').trim();
  return applicationId && accessKey ? { applicationId, accessKey } : null;
}

function serialized(fn) {
  if (queueDepth >= MAX_QUEUE_DEPTH) {
    return Promise.reject(new Error('楽天ウェブサービスへの要求が混み合っています。少し待って再試行してください'));
  }
  queueDepth += 1;
  const job = queueTail.then(async () => {
    const wait = MIN_GAP_MS - (Date.now() - lastCallAt);
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastCallAt = Date.now();
    return fn();
  }).finally(() => { queueDepth -= 1; });
  queueTail = job.catch(() => undefined);
  return job;
}

async function ichibaGet(url) {
  return serialized(async () => {
    const doFetch = async () => {
      const res = await fetch(url, { headers: ICHIBA_HEADERS, signal: AbortSignal.timeout(20_000) });
      const data = await res.json().catch(() => null);
      return { status: res.status, data, retryAfterMs: parseRetryAfterHeader(res.headers) };
    };
    let r = await doFetch();
    // 429 は Retry-After (無ければ 1.5s) を待って 1 回だけ再試行
    if (r.status === 429) {
      await new Promise((resolve) => setTimeout(resolve, r.retryAfterMs ?? 1500));
      r = await doFetch();
    }
    return r;
  });
}

function parseRetryAfterHeader(headers) {
  const v = headers?.get?.('retry-after');
  if (!v) return null;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? null : Math.max(0, n * 1000);
}

/**
 * ジャンルの子一覧 (+自分と親のパス)。genreId=0 が root。
 * @returns {{ok:true, current, parents:[], children:[{genreId,name,level}]}|{ok:false,error}}
 */
export async function fetchGenreChildren(genreId) {
  const id = String(genreId ?? '0').trim() || '0';
  if (!/^\d{1,12}$/.test(id)) return { ok: false, error: 'ジャンルIDが不正です' };
  const cached = childrenCache.get(id);
  if (cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) {
    return { ok: true, ...cached };
  }
  const app = wsAppId();
  if (!app) {
    return {
      ok: false, needsSetup: true,
      error: 'ジャンルツリー用の楽天ウェブサービスID (RAKUTEN_WS_APP_ID) が未設定です。webservice.rakuten.co.jp でアプリ登録し、Render の env に設定してください',
    };
  }
  // 同じ未キャッシュIDへの並行要求は 1 回の API 呼び出しに束ねる (stampede 防止)
  if (inflight.has(id)) return inflight.get(id);
  const job = (async () => {
    const url = `${ICHIBA_GENRE_URL}?applicationId=${encodeURIComponent(app)}&genreId=${id}&format=json`;
    const r = await ichibaGet(url);
    if (r.status !== 200 || !r.data) {
      // 一時障害なら期限切れキャッシュで凌ぐ (無ければエラー)
      if (cached) return { ok: true, ...cached, stale: true };
      const msg = r.data?.error_description || r.data?.error || `HTTP ${r.status}`;
      return { ok: false, error: `ジャンル取得に失敗: ${msg}` };
    }
    const entry = normalizeGenreSearch(r.data);
    childrenCache.set(id, { fetchedAt: Date.now(), ...entry });
    if (childrenCache.size > 2000) {
      // 全 clear は再取得の集中を招くので、古い方から 500 件だけ落とす (Map は挿入順)
      let n = 0;
      for (const key of childrenCache.keys()) {
        childrenCache.delete(key);
        if (++n >= 500) break;
      }
    }
    return { ok: true, ...entry };
  })().finally(() => inflight.delete(id));
  inflight.set(id, job);
  return job;
}

/** IchibaGenre/Search 応答の正規化 (pure、テスト可能) */
export function normalizeGenreSearch(data) {
  const pick = (g) => ({
    genreId: String(g.genreId),
    name: g.genreName || '',
    level: Number(g.genreLevel) || 0,
  });
  const current = data?.current ? pick(data.current) : null;
  const parents = (Array.isArray(data?.parents) ? data.parents : [])
    .map((p) => pick(p.parent || p)).filter((p) => p.genreId !== '0');
  const children = (Array.isArray(data?.children) ? data.children : [])
    .map((c) => pick(c.child || c));
  return { current, parents, children };
}

/** parents + current からフルパス文字列を作る */
export function genrePathOf(entry) {
  const names = [...(entry.parents || []).map((p) => p.name)];
  if (entry.current && entry.current.genreId !== '0') names.push(entry.current.name);
  return names.join(' > ');
}

/**
 * 商品名から楽天市場の上位ヒット商品のジャンル多数決で候補を出す (AI 初期値)。
 * @returns {{ok:true, candidates:[{genreId, path, count}]}|{ok:false,error}}
 */
export async function suggestGenreByName(name, { maxCandidates = 3 } = {}) {
  const keyword = String(name || '').trim().slice(0, 100);
  if (!keyword) return { ok: false, error: '商品名が空です' };
  // 新ホスト優先 (ranking-checker と同じ資格情報 = Render 設定済みで今すぐ動く)。
  // 無ければ旧ホスト (RAKUTEN_WS_APP_ID)。両方無ければ設定案内
  const ms = msCreds();
  const ws = wsAppId();
  let url;
  if (ms) {
    url = `${ICHIBA_ITEM_URL_NEW}?applicationId=${encodeURIComponent(ms.applicationId)}`
      + `&accessKey=${encodeURIComponent(ms.accessKey)}&keyword=${encodeURIComponent(keyword)}&hits=30&format=json`;
  } else if (ws) {
    url = `${ICHIBA_ITEM_URL_OLD}?applicationId=${encodeURIComponent(ws)}`
      + `&keyword=${encodeURIComponent(keyword)}&hits=30&format=json`;
  } else {
    return { ok: false, error: '楽天ウェブサービスの資格情報が未設定です (RAKUTEN_APP_ID+RAKUTEN_ACCESS_KEY または RAKUTEN_WS_APP_ID)' };
  }
  const r = await ichibaGet(url);
  if (r.status !== 200 || !r.data) {
    const msg = r.data?.errors?.errorMessage || r.data?.error_description || r.data?.error || `HTTP ${r.status}`;
    return { ok: false, error: `商品検索に失敗: ${msg}` };
  }
  const counts = new Map();
  for (const it of r.data.Items || []) {
    const gid = String((it.Item || it).genreId || '').trim();
    if (!gid) continue;
    counts.set(gid, (counts.get(gid) || 0) + 1);
  }
  if (counts.size === 0) return { ok: true, candidates: [] };
  const top = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, maxCandidates);
  // パスの補完は呼び出し側 (router) が RMS 属性辞書経由で行う
  // (旧ホストIDが無くても動かすため。RMS 辞書は末端でないジャンルを 404 で弾く効果もある)
  return { ok: true, candidates: top.map(([gid, count]) => ({ genreId: gid, count })) };
}
