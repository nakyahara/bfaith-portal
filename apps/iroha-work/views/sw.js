/* いろは在庫化 作業画面 — Service Worker
 *
 * 目的: Render が再起動中 (502/503) でも、ホーム画面から開いた画面が「真っ白なエラーページ」にならない。
 *   画面 (HTML) は network-first: つながれば最新を取ってキャッシュも更新 / 失敗・5xx なら最後に取れた画面を返す。
 *   API・画像はここでは触らない — 画面側の JS が前回の一覧を保持し、自動で再接続する。
 * 中原さん 2026-09-03「Render は社内アプリのマージで頻繁に 502 になる。取れない間は良いが落ちないように」
 *
 * 保存するのは登録済み端末に返る本物の画面だけ (未登録は /enroll へリダイレクト = res.redirected → 保存しない)。
 * 初回表示は SW を通らないので、install 時と画面からの依頼 ('cache-shell') で今の画面を取りに行って保存する
 * (Codex R1 #2)。登録解除・失効時は画面 (作業画面の 401 処理・登録画面) から 'clear-shell' が来て消す (R1 #1 / R2 #1)。
 */
const CACHE = 'iroha-work-shell-v5';   // v5: 職員モードのボタンと、下見でも見えるゲージ
const SCOPE = '/apps/iroha-work/';
const NET_TIMEOUT_MS = 8000;   // キャッシュがある時だけ、この時間で諦めてキャッシュを出す (Codex R1 #5)

/** 本物の画面 (ok・リダイレクトなし・HTML) だけ保存。書き込み完了まで待つ (Codex R1 #6) */
async function putShell(cache, res, redirected) {
  try {
    const ct = res.headers.get('content-type') || '';
    if (res.ok && !redirected && ct.includes('text/html')) await cache.put(SCOPE, res);
  } catch (e) { /* 保存できなくても表示は続ける */ }
}

/** 今の (認証 Cookie で取れる) 画面を取りに行って保存する */
async function precacheShell() {
  try {
    const cache = await caches.open(CACHE);
    const res = await fetch(SCOPE, { credentials: 'same-origin', cache: 'no-store', redirect: 'follow' });
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
      .then(keys => Promise.all(keys.filter(k => k.startsWith('iroha-work-shell-') && k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim()),
  );
});

self.addEventListener('message', (e) => {
  if (e.data === 'cache-shell') e.waitUntil(precacheShell());
  if (e.data === 'clear-shell') e.waitUntil(caches.delete(CACHE).catch(() => {}));
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || req.mode !== 'navigate') return;
  const url = new URL(req.url);
  if (url.pathname !== SCOPE) return;   // 画面本体だけ (enroll / admin / API は素通し)
  e.respondWith((async () => {
    const cache = await caches.open(CACHE);
    const cached = await cache.match(SCOPE);
    const ac = new AbortController();
    // キャッシュがある時だけ、ヘッダ+本文が NET_TIMEOUT_MS 以内に届かなければ諦めてキャッシュを出す。
    // fetch はヘッダ到着で resolve するので、本文を読み切るまでタイマーを生かす (Codex R2 #2)
    const tm = cached ? setTimeout(() => ac.abort(), NET_TIMEOUT_MS) : null;
    try {
      // navigate モードの Request はそのまま再利用できないブラウザがあるので URL で取り直す
      const res = await fetch(req.url, { credentials: 'same-origin', redirect: 'follow', signal: ac.signal });
      const body = await res.arrayBuffer();   // 本文まで読み切る (画面 HTML は数十KB)
      if (tm) clearTimeout(tm);
      if (res.status >= 500 && cached) return cached;
      const full = new Response(body, { status: res.status, statusText: res.statusText, headers: res.headers });
      await putShell(cache, full.clone(), res.redirected);   // 本文はメモリ上なので即完了
      return full;
    } catch (err) {
      if (tm) clearTimeout(tm);
      if (cached) return cached;
      return new Response(
        '<!doctype html><html lang="ja"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">' +
        '<title>接続できません</title><body style="font-family:sans-serif;padding:24px;background:#F3F5F9;color:#0F1728">' +
        '<h1 style="font-size:1.2rem">サーバーにつながりません</h1><p>少し待ってから、もう一度開いてください。</p></body></html>',
        { status: 503, headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' } },
      );
    }
  })());
});
