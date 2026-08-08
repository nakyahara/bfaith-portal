// Service Worker — content script からの依頼で miniPC の portal API を叩く。
// MV3 では host_permissions に載せた宛先への SW fetch は CORS 制約を受けないため、
// サーバー側に CORS ヘッダを足さずに済む (content script から直接 fetch しないのはそのため)。
// APIトークンをページ側の world に置かない、という意味でもこの経路にしている。

const DEFAULTS = { baseUrl: 'https://wh.bfaith-wh.uk', token: '' };
const BASE_PATH = '/apps/select-set/ext-api';

function getConfig() {
  return new Promise((resolve) => chrome.storage.local.get(DEFAULTS, resolve));
}

async function api(path, opts = {}) {
  const cfg = await getConfig();
  if (!cfg.token) throw new Error('APIトークン未設定 (拡張機能のオプションで設定してください)');
  const base = cfg.baseUrl.replace(/\/+$/, '');
  const headers = { 'x-api-key': cfg.token };
  if (opts.body) headers['Content-Type'] = 'application/json';

  // 🚨 接続先 (miniPC) は Cloudflare Access の後ろにあり、素で叩くと
  //    cloudflareaccess.com のログイン画面へ 302 される (2026-08-08 実測)。
  //    credentials:'include' にすると、ブラウザが持っている CF_Authorization クッキーが乗って通る。
  //    = 拡張に認証情報を持たせずに済む。代わりにCF Accessのセッションが切れると通らなくなるので、
  //      そのときは「サイトを一度開いてログインしてください」と案内する。
  const res = await fetch(base + BASE_PATH + path, { ...opts, headers, credentials: 'include' });

  const ct = res.headers.get('content-type') || '';
  if (/cloudflareaccess\.com/.test(res.url || '') || (!ct.includes('json') && (res.redirected || res.status === 302))) {
    throw new Error(
      `接続先のログインセッションが切れています。ブラウザで ${base} を開いてログインしてから、もう一度お試しください。`,
    );
  }

  let json = null;
  try { json = await res.json(); } catch { /* JSONでない */ }
  if (!json) {
    throw new Error(`応答がJSONではありません (HTTP ${res.status})。接続先URLを確認してください。`);
  }
  if (!res.ok || json.success === false) {
    const msg = json.error && json.error.message ? json.error.message : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return json.data;
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  (async () => {
    try {
      let data;
      if (msg.type === 'ping') {
        data = await api('/api/v1/ping');
      } else if (msg.type === 'sets') {
        data = await api('/api/v1/sets');
      } else if (msg.type === 'expand') {
        data = await api('/api/v1/expand', {
          method: 'POST',
          body: JSON.stringify({ setCode: msg.setCode, op: msg.op, quantity: msg.quantity }),
        });
      } else {
        throw new Error(`未知のメッセージ: ${msg.type}`);
      }
      sendResponse({ ok: true, data });
    } catch (e) {
      sendResponse({ ok: false, error: e.message });
    }
  })();
  return true; // 非同期 sendResponse
});
