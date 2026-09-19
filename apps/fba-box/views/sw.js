/* FBA箱詰め記録 作業画面 — Service Worker
 *
 * 目的: **本社が別のアプリをマージしただけで、いろはの箱詰めが真っ白なエラーページになる**のを止める。
 *   Render は永続ディスク付きサービスなのでゼロダウンタイムデプロイができず、マージのたびに
 *   1〜3 分ほど 502 になる (AI_reference『ピッキング支援システム_ミニPC移行手順_20260812.md』)。
 *   ポータルはモノレポ同居なので、箱詰めと関係のないマージでも巻き添えで落ちる。
 *   中原さん 2026-09-03「Render は社内アプリのマージで頻繁に 502 になる。取れない間は良いが落ちないように」
 *   → いろは在庫化 (apps/iroha-work/views/sw.js) と同じ作りをこの画面にも入れる (中原さん 2026-09-18)
 *
 * 画面 (HTML) と画面の部品 (place-queue.js) は network-first:
 *   つながれば最新を取ってキャッシュも更新 / 失敗・5xx なら最後に取れたものを返す。
 * API はここでは触らない — 画面側の JS が「更新中」と出して自分で再接続する。
 *
 * 🚨 place-queue.js も必ず一緒に持つ。画面 HTML だけをキャッシュから出しても、部品が 502 だと
 *   画面は「部品を読み込めませんでした」で止まる (index.html の createPlaceQueue ガード) = 意味がない。
 *
 * 保存するのは登録済み端末に返る本物の画面だけ (未登録は /enroll へリダイレクト = res.redirected → 保存しない)。
 * 初回表示は SW を通らないので、install 時と画面からの依頼 ('cache-shell') で取りに行って保存する。
 * 端末が失効したときは画面から 'clear-shell' が来て消す。
 */
const CACHE = 'fba-box-shell-v1';
const SCOPE = '/apps/fba-box/';
const PARTS = [SCOPE + 'place-queue.js'];   // 画面と一緒に持つ部品 (これが無いと画面は動かない)
const NET_TIMEOUT_MS = 8000;   // キャッシュがある時だけ、この時間で諦めてキャッシュを出す

/** 本物の画面 (ok・リダイレクトなし・HTML) だけ保存 */
async function putShell(cache, res, redirected) {
  try {
    const ct = res.headers.get('content-type') || '';
    if (res.ok && !redirected && ct.includes('text/html')) await cache.put(SCOPE, res);
  } catch (e) { /* 保存できなくても表示は続ける */ }
}

/** 部品 (JS) を保存。リダイレクト (= 未登録で /enroll へ) は保存しない */
async function putPart(cache, url, res, redirected) {
  try {
    const ct = res.headers.get('content-type') || '';
    if (res.ok && !redirected && ct.includes('javascript')) await cache.put(url, res);
  } catch (e) { /* 同上 */ }
}

/** 今の (端末 Cookie で取れる) 画面と部品を取りに行って保存する */
async function precacheShell() {
  const cache = await caches.open(CACHE);
  try {
    const res = await fetch(SCOPE, { credentials: 'same-origin', cache: 'no-store', redirect: 'follow' });
    await putShell(cache, res, res.redirected);
  } catch (e) { /* オフライン等。次の機会に */ }
  for (const url of PARTS) {
    try {
      const res = await fetch(url, { credentials: 'same-origin', cache: 'no-store', redirect: 'follow' });
      await putPart(cache, url, res, res.redirected);
    } catch (e) { /* 同上 */ }
  }
}

self.addEventListener('install', (e) => {
  self.skipWaiting();
  e.waitUntil(precacheShell());
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k.startsWith('fba-box-shell-') && k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim()),
  );
});

self.addEventListener('message', (e) => {
  if (e.data === 'cache-shell') e.waitUntil(precacheShell());
  if (e.data === 'clear-shell') e.waitUntil(caches.delete(CACHE).catch(() => {}));
});

/** network-first (5xx・失敗ならキャッシュ)。key = キャッシュ上の名前 */
async function networkFirst(reqUrl, key, isPart) {
  const cache = await caches.open(CACHE);
  const cached = await cache.match(key);
  const ac = new AbortController();
  // キャッシュがある時だけ、ヘッダ+本文が届かなければ諦めてキャッシュを出す。
  // fetch はヘッダ到着で resolve するので、本文を読み切るまでタイマーを生かす
  const tm = cached ? setTimeout(() => ac.abort(), NET_TIMEOUT_MS) : null;
  try {
    const res = await fetch(reqUrl, { credentials: 'same-origin', redirect: 'follow', signal: ac.signal });
    const body = await res.arrayBuffer();
    if (tm) clearTimeout(tm);
    if (res.status >= 500 && cached) return cached;   // 更新中 (502/503) は最後に取れたものを出す
    const full = new Response(body, { status: res.status, statusText: res.statusText, headers: res.headers });
    const keep = full.clone();
    if (isPart) await putPart(cache, key, keep, res.redirected);
    else await putShell(cache, keep, res.redirected);
    return full;
  } catch (err) {
    if (tm) clearTimeout(tm);
    if (cached) return cached;
    if (isPart) throw err;   // 部品は作り話を返さない (画面側のガードが「読み込めません」を出す)
    return new Response(
      '<!doctype html><html lang="ja"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
      + '<title>接続できません</title><body style="font-family:sans-serif;padding:24px;background:#F8F9FA;color:#212529">'
      + '<h1 style="font-size:1.2rem">サーバーにつながりません</h1>'
      + '<p>アプリを更新しているところかもしれません。<br>少し待ってから、もう一度開いてください。</p>'
      + '<p style="color:#868E96;font-size:.9rem">記録した分は端末に残っています。消えません。</p></body></html>',
      { status: 503, headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' } },
    );
  }
}

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;
  // 画面本体 (enroll / admin / API / 箱札 は素通し)
  if (req.mode === 'navigate') {
    if (url.pathname !== SCOPE) return;
    e.respondWith(networkFirst(req.url, SCOPE, false));
    return;
  }
  // 画面の部品
  if (PARTS.includes(url.pathname)) e.respondWith(networkFirst(req.url, url.pathname, true));
});
