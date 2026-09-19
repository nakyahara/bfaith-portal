/* FBA箱詰め記録 作業画面 — Service Worker
 *
 * 目的: **本社が別のアプリをマージしただけで、いろはの箱詰めが真っ白なエラーページになる**のを止める。
 *   Render は永続ディスク付きサービスなのでゼロダウンタイムデプロイができず、マージのたびに
 *   1〜3 分ほど 502 になる (AI_reference『ピッキング支援システム_ミニPC移行手順_20260812.md』)。
 *   ポータルはモノレポ同居なので、箱詰めと関係のないマージでも巻き添えで落ちる。
 *   中原さん 2026-09-03「Render は社内アプリのマージで頻繁に 502 になる。取れない間は良いが落ちないように」
 *   → いろは在庫化 (apps/iroha-work/views/sw.js) と同じ作りをこの画面にも入れる (中原さん 2026-09-18)
 *
 * 画面 (HTML) は network-first: つながれば最新を取ってキャッシュも更新 / 失敗・5xx なら最後に取れた画面を返す。
 * API・画像はここでは触らない — 画面側の JS が「更新中」と出して自分で再接続する。
 *
 * 🚨 **持ち物は画面 HTML の 1 つだけ**。送信キュー (place-queue.js) はサーバーが画面に埋め込んで返すので、
 *   「画面」と「部品」で版が食い違う余地が無い (別々に持つ作りは Codex #1366 R2 で穴が 4 つ出た)。
 *
 * 保存するのは登録済み端末に返る本物の画面だけ (未登録は /enroll へリダイレクト = res.redirected → 保存しない)。
 * 初回表示は SW を通らないので、install 時と画面からの依頼 ('cache-shell') で取りに行って保存する。
 * 🚨 端末の登録が切れたら (リダイレクト・401・403)、**その場で控えを捨てる** — 登録画面の掃除に頼らない
 *   (裏の取り直しで気づいても捨てていなかった = Codex #1366 R1 #2)。
 */
const CACHE = 'fba-box-shell-v3';   // v3: 部品は画面に埋め込み、持ち物は画面だけにした
const SCOPE = '/apps/fba-box/';
const NET_TIMEOUT_MS = 8000;   // キャッシュがある時だけ、この時間で諦めてキャッシュを出す

/** 端末の登録が切れた・ログアウトした = 前の画面を出してはいけない */
const unauthorized = (res, redirected) => redirected || res.status === 401 || res.status === 403;
async function dropCache() { try { await caches.delete(CACHE); } catch (e) { /* 無視 */ } }

/** 本物の画面 (ok・リダイレクトなし・HTML) だけ保存 */
async function putShell(cache, res, redirected) {
  try {
    const ct = res.headers.get('content-type') || '';
    if (res.ok && !redirected && ct.includes('text/html')) await cache.put(SCOPE, res);
  } catch (e) { /* 保存できなくても表示は続ける */ }
}

/** 今の (端末 Cookie で取れる) 画面を取りに行って保存する */
async function precacheShell() {
  try {
    const res = await fetch(SCOPE, { credentials: 'same-origin', cache: 'no-store', redirect: 'follow' });
    if (unauthorized(res, res.redirected)) { await dropCache(); return; }
    const cache = await caches.open(CACHE);
    await putShell(cache, res, res.redirected);
  } catch (e) { /* オフライン等。次の機会に */ }
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
  if (e.data === 'clear-shell') e.waitUntil(dropCache());
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || req.mode !== 'navigate') return;
  const url = new URL(req.url);
  if (url.pathname !== SCOPE) return;   // 画面本体だけ (enroll / admin / API / 箱札 は素通し)
  e.respondWith((async () => {
    const cache = await caches.open(CACHE);
    const cached = await cache.match(SCOPE);
    const ac = new AbortController();
    // キャッシュがある時だけ、ヘッダ+本文が NET_TIMEOUT_MS 以内に届かなければ諦めてキャッシュを出す。
    // fetch はヘッダ到着で resolve するので、本文を読み切るまでタイマーを生かす
    const tm = cached ? setTimeout(() => ac.abort(), NET_TIMEOUT_MS) : null;
    try {
      // navigate モードの Request はそのまま再利用できないブラウザがあるので URL で取り直す
      const res = await fetch(req.url, { credentials: 'same-origin', redirect: 'follow', signal: ac.signal });
      const body = await res.arrayBuffer();   // 本文まで読み切る
      if (tm) clearTimeout(tm);
      if (res.status >= 500 && cached) return cached;   // 更新中 (502/503) は最後に取れた画面を出す
      const full = new Response(body, { status: res.status, statusText: res.statusText, headers: res.headers });
      if (unauthorized(res, res.redirected)) { await dropCache(); return full; }
      await putShell(cache, full.clone(), res.redirected);   // 本文はメモリ上なので即完了
      return full;
    } catch (err) {
      if (tm) clearTimeout(tm);
      if (cached) return cached;
      return new Response(
        '<!doctype html><html lang="ja"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
        + '<title>接続できません</title><body style="font-family:sans-serif;padding:24px;background:#F8F9FA;color:#212529">'
        + '<h1 style="font-size:1.2rem">サーバーにつながりません</h1>'
        + '<p>アプリを更新しているところかもしれません。<br>少し待ってから、もう一度開いてください。</p>'
        + '<p style="color:#868E96;font-size:.9rem">記録した分は端末に残っています。消えません。</p></body></html>',
        { status: 503, headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' } },
      );
    }
  })());
});
