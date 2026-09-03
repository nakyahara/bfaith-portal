/* いろは在庫化 作業画面 — Service Worker
 *
 * 目的: Render が再起動中 (502/503) でも、ホーム画面から開いた画面が「真っ白なエラーページ」にならない。
 *   画面 (HTML) は network-first: つながれば最新を取ってキャッシュも更新 / 失敗・5xx なら最後に取れた画面を返す。
 *   API・画像はここでは触らない — 画面側の JS が前回の一覧を保持し、自動で再接続する。
 * 中原さん 2026-09-03「Render は社内アプリのマージで頻繁に 502 になる。取れない間は良いが落ちないように」
 *
 * 注意: キャッシュするのは登録済み端末に返る本物の画面だけ。未登録端末は /enroll へリダイレクトされるので
 *   (res.redirected) 保存しない。キャッシュ名を変えると古いものは activate で消える。
 */
const CACHE = 'iroha-work-shell-v1';
const SCOPE = '/apps/iroha-work/';

self.addEventListener('install', () => { self.skipWaiting(); });

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k.startsWith('iroha-work-shell-') && k !== CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim()),
  );
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || req.mode !== 'navigate') return;
  const url = new URL(req.url);
  if (url.pathname !== SCOPE) return;   // 画面本体だけ (enroll / admin / API は素通し)
  e.respondWith((async () => {
    const cache = await caches.open(CACHE);
    try {
      const res = await fetch(req);
      if (res.ok && !res.redirected) cache.put(SCOPE, res.clone());
      if (res.status >= 500) {
        const hit = await cache.match(SCOPE);
        if (hit) return hit;
      }
      return res;
    } catch (err) {
      const hit = await cache.match(SCOPE);
      if (hit) return hit;
      return new Response(
        '<!doctype html><html lang="ja"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">' +
        '<title>接続できません</title><body style="font-family:sans-serif;padding:24px;background:#F3F5F9;color:#0F1728">' +
        '<h1 style="font-size:1.2rem">サーバーにつながりません</h1><p>少し待ってから、もう一度開いてください。</p></body></html>',
        { status: 503, headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' } },
      );
    }
  })());
});
