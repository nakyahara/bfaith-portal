// Service Worker — content script からの依頼で miniPC API を叩く。
// MV3 では host_permissions に載せた宛先への SW fetch は CORS 制約を受けないため、
// サーバー側に CORS ヘッダを足さずに済む (content script から直接 fetch しないのはそのため)。

const DEFAULTS = { baseUrl: 'https://wh.bfaith-wh.uk', token: '' };

function getConfig() {
  return new Promise((resolve) => chrome.storage.local.get(DEFAULTS, resolve));
}

async function api(path, opts = {}) {
  const cfg = await getConfig();
  if (!cfg.token) throw new Error('APIトークン未設定 (拡張機能のオプションで設定)');
  const headers = { 'x-api-key': cfg.token };
  if (opts.body) headers['Content-Type'] = 'application/json';
  const res = await fetch(cfg.baseUrl.replace(/\/+$/, '') + path, { ...opts, headers });
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try { const j = await res.json(); if (j.error) msg = `${j.error} (HTTP ${res.status})`; } catch { /* JSONでない */ }
    throw new Error(msg);
  }
  return res.json();
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  (async () => {
    try {
      let data;
      if (msg.type === 'reverse') {
        data = await api(`/aba-ext-api/reverse?asin=${encodeURIComponent(msg.asin)}`);
      } else if (msg.type === 'catalog') {
        data = await api('/aba-ext-api/catalog', { method: 'POST', body: JSON.stringify({ asins: msg.asins }) });
      } else if (msg.type === 'status') {
        data = await api('/aba-ext-api/status');
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
