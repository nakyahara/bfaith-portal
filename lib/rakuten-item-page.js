/**
 * 楽天の商品ページ (公開ページ) から楽天ジャンルIDを読む (2026-09-04 中原さん要望)。
 *
 * 🚨 商品検索API では引けない (2026-09-04 実測): API の itemCode は
 *    `<店舗コード>:<数字>` の内部ID で、**商品ページURLの末尾 (商品管理番号) とは別物**。
 *    URL 末尾を itemCode に渡すと "itemCode is not valid"、keyword 検索でもヒットしない。
 *    → 公開の商品ページに埋め込まれた JSON から読む。
 *
 * 🚨 **取得は miniPC が行う** (2026-09-04): Render から同じ URL を取っても
 *    ジャンルIDを読み取れなかった (miniPC = 日本のIP からは同じ実装で必ず取れる)。
 *    楽天への口は miniPC に集約する方針とも合う。このファイルの取得系は miniPC 側で動く。
 *
 * ⚠️ 利用者が貼った URL を取りに行くので **SSRF を作らない**:
 *    ホストは item.rakuten.co.jp 固定、URLは自前で組み直す (クエリ・ユーザ情報を捨てる)、
 *    リダイレクトは追わない、応答サイズと時間に上限。
 *
 * 置き場所: product-hub (呼ぶ側) と warehouse (取りに行く側) の両方が使うので、
 * アプリ中立な lib/ に置く。片方の app 配下に置くと依存方向がねじれる (Codex R1)。
 */

//
// 🚨 商品検索API では引けない (2026-09-04 実測): API の itemCode は
//    `<店舗コード>:<数字>` の内部ID で、**商品ページURLの末尾 (商品管理番号) とは別物**。
//    URL 末尾を itemCode に渡すと "itemCode is not valid"、keyword 検索でもヒットしない。
//    → 公開の商品ページに埋め込まれた JSON から読む (3商品で実測・一意に正解が取れた)。
//
// ⚠️ 利用者が貼った URL を取りに行くので **SSRF を作らない**:
//    ホストは item.rakuten.co.jp 固定、URLは自前で組み直す (クエリ・ユーザ情報を捨てる)、
//    リダイレクトは追わない、応答サイズと時間に上限。

/** ページ取得の直列化 (商品検索APIとは別の待ち行列。人の操作時だけなので間隔は広め) */
const PAGE_MIN_GAP_MS = 2000;
let pageQueueTail = Promise.resolve();
let pageQueueDepth = 0;
let lastPageFetchAt = 0;
// 待ち行列は「実行中1 + 待ち1」まで。取得は最大15秒 + 間隔2秒なので最悪 2*(15+2)=34秒 で、
// 呼び出し側のタイムアウト(40秒)に収まる。それ以上は待たせずに断る (人の操作なので
// 長く待たせる意味がない。ここが唯一の待ち行列 — 外側にセマフォを重ねない)
const MAX_PAGE_QUEUE = 2;
const pageGenreCache = new Map(); // canonical url -> { fetchedAt, result }
const MAX_PAGE_BYTES = 3 * 1024 * 1024;
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_PAGE_CACHE = 200;       // 期限切れを消すだけだと、調べた URL の分だけ増え続ける

/** キャッシュに入れる。期限切れを掃除し、上限を超えたら古い順に捨てる (Map は挿入順) */
function rememberPageGenre(url, result) {
  const now = Date.now();
  for (const [k, v] of pageGenreCache) {
    if (now - v.fetchedAt >= CACHE_TTL_MS) pageGenreCache.delete(k);
  }
  pageGenreCache.set(url, { fetchedAt: now, result });
  while (pageGenreCache.size > MAX_PAGE_CACHE) {
    pageGenreCache.delete(pageGenreCache.keys().next().value);
  }
}

/**
 * 応答を上限まで**ストリームで**読む。arrayBuffer() だと Content-Length 無し (chunked) の
 * 巨大応答を全部メモリに載せてから判定することになる (Codex R1 high)
 */
async function readCapped(res, maxBytes) {
  const reader = res.body?.getReader?.();
  if (!reader) return null;
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxBytes) {
      await reader.cancel().catch(() => {});
      return null;
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks);
}

/**
 * 楽天の商品ページURLを検証して正規化する (pure・smoke対象)。
 * 受け付けるのは https://item.rakuten.co.jp/<店舗>/<商品管理番号>/ だけ。
 * クエリ (?rafcid=...) やフラグメントは捨て、**自前で組み直した URL** を返す
 * @returns {{shopCode, itemCode, url}|null}
 */
export function parseRakutenItemUrl(input) {
  const raw = String(input || '').trim();
  if (!raw) return null;
  // new URL は "/../etc" を "/etc" に畳んでしまうので、元の文字列の段階で弾く
  // (素直な商品ページURL以外を受け付けない)
  if (/\/\.\.?(\/|$)/.test(raw)) return null;
  let u;
  try { u = new URL(raw); } catch (_) { return null; }
  if (u.protocol !== 'https:' || u.hostname.toLowerCase() !== 'item.rakuten.co.jp') return null;
  if (u.username || u.password || u.port) return null; // 認証情報つき/別ポートは受けない
  let segs;
  try {
    // 壊れた percent encoding ("/shop/%/") は URIError になる → 不正な URL として扱う
    segs = u.pathname.split('/').filter(Boolean).map((s) => decodeURIComponent(s));
  } catch (_) { return null; }
  // 「/店舗/商品/」ちょうどだけ。3階層以上を受けると、余分なパスを黙って捨てて
  // 別の URL を取りに行くことになる (Codex R2)
  if (segs.length !== 2) return null;
  const [shopCode, itemCode] = segs;
  // 店舗コード・商品管理番号に使える文字だけ (パス操作の余地を残さない)
  if (!/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(shopCode)) return null;
  if (!/^[a-z0-9][a-z0-9._-]{0,127}$/i.test(itemCode)) return null;
  return { shopCode, itemCode, url: `https://item.rakuten.co.jp/${shopCode}/${itemCode}/` };
}

/**
 * 商品ページの HTML から楽天ジャンルIDを取り出す (pure・smoke対象)。
 * ページ内の埋め込み JSON (HTMLエスケープ済み) の genreId だけを見る。
 * `genre_id` のような緩い一致だと、全ページに出る 100005 のようなノイズを拾う。
 * **一意に決まらなければ null** (誤ったIDを返さない — 出品先を間違える方が困る)
 */
export function extractGenreIdFromHtml(html) {
  const ids = [...new Set([...String(html || '').matchAll(/&quot;genreId&quot;:\s*(\d{3,10})/g)].map((m) => m[1]))];
  return ids.length === 1 ? ids[0] : null;
}

/** 商品ページの文字コード (楽天は EUC-JP)。meta/ヘッダに従い、不明なら EUC-JP で読む */
export function decodeItemPage(buf, contentType) {
  const head = new TextDecoder('latin1').decode(buf.subarray(0, 2048));
  const fromMeta = head.match(/charset=["']?([\w-]+)/i)?.[1];
  const fromHeader = String(contentType || '').match(/charset=([\w-]+)/i)?.[1];
  const label = (fromHeader || fromMeta || 'euc-jp').toLowerCase();
  try {
    return new TextDecoder(label).decode(buf);
  } catch (_) {
    return new TextDecoder('euc-jp').decode(buf);
  }
}

/**
 * 他社の商品ページURLから楽天ジャンルIDを調べる。
 * @returns {{ok:true, genreId, shopCode, itemCode, url, cached}|{ok:false, error}}
 */
export async function genreIdFromItemUrl(input) {
  const parsed = parseRakutenItemUrl(input);
  if (!parsed) {
    return { ok: false, error: '楽天の商品ページURL (https://item.rakuten.co.jp/店舗コード/商品管理番号/) を貼ってください' };
  }
  const cached = pageGenreCache.get(parsed.url);
  if (cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) {
    return { ...cached.result, cached: true };
  }
  if (pageQueueDepth >= MAX_PAGE_QUEUE) {
    return { ok: false, error: '商品ページの取得が混み合っています。少し待って再試行してください' };
  }
  pageQueueDepth += 1;
  const job = pageQueueTail.then(async () => {
    const wait = PAGE_MIN_GAP_MS - (Date.now() - lastPageFetchAt);
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastPageFetchAt = Date.now();
    let res;
    try {
      res = await fetch(parsed.url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
          'Accept-Language': 'ja',
        },
        redirect: 'manual',           // 転送先は保証できないので追わない
        signal: AbortSignal.timeout(15_000),
      });
    } catch (e) {
      return { ok: false, error: `商品ページを取得できませんでした (${String(e.message || e).slice(0, 120)})` };
    }
    // 読まずに抜けるときは本文を捨てる (Node の fetch は body を放置すると接続の解放が
    // GC 任せになり、異常応答が続くとソケットとメモリを圧迫する — Codex R2 medium)
    const discard = async () => { await res.body?.cancel().catch(() => {}); };
    if (res.status >= 300 && res.status < 400) {
      await discard();
      return { ok: false, error: '商品ページが移動しています (短縮URLではなく item.rakuten.co.jp の URL を貼ってください)' };
    }
    if (res.status !== 200) {
      await discard();
      return { ok: false, error: `商品ページを取得できませんでした (HTTP ${res.status})。URL が正しいか確認してください` };
    }
    const len = Number(res.headers.get('content-length') || 0);
    if (len && len > MAX_PAGE_BYTES) {
      await discard();
      return { ok: false, error: '商品ページが大きすぎます' };
    }
    const buf = await readCapped(res, MAX_PAGE_BYTES);
    if (!buf) return { ok: false, error: '商品ページが大きすぎます' };
    const genreId = extractGenreIdFromHtml(decodeItemPage(buf, res.headers.get('content-type')));
    if (!genreId) {
      return { ok: false, error: 'このページからはジャンルIDを読み取れませんでした (楽天のページ構成が変わった可能性があります)' };
    }
    return { ok: true, genreId, shopCode: parsed.shopCode, itemCode: parsed.itemCode, url: parsed.url };
  }).finally(() => { pageQueueDepth -= 1; });
  pageQueueTail = job.catch(() => undefined);
  const result = await job;
  if (result.ok) rememberPageGenre(parsed.url, result);
  return result;
}
