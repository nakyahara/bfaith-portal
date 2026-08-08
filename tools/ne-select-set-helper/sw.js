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
  const headers = { 'x-api-key': cfg.token };
  if (opts.body) headers['Content-Type'] = 'application/json';
  const res = await fetch(cfg.baseUrl.replace(/\/+$/, '') + BASE_PATH + path, { ...opts, headers });
  let json = null;
  try { json = await res.json(); } catch { /* JSONでない */ }
  if (!res.ok || !json || json.success === false) {
    const msg = json && json.error && json.error.message ? json.error.message : `HTTP ${res.status}`;
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
