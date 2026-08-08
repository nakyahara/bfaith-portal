// Service Worker — content script からの依頼で portal の ext-api を叩く。
// MV3 では host_permissions に載せた宛先への SW fetch は CORS 制約を受けないため、
// サーバー側に CORS ヘッダを足さずに済む (content script から直接 fetch しないのはそのため)。
// APIトークンをページ側の world に置かない、という意味でもこの経路にしている。
// (ne-select-set-helper と同じ作り)

const DEFAULTS = { baseUrl: 'https://wh.bfaith-wh.uk', token: '' };
const BASE_PATH = '/apps/fba-replenishment/ext-api';

function getConfig() {
  return new Promise((resolve) => chrome.storage.local.get(DEFAULTS, resolve));
}

async function request(path) {
  const cfg = await getConfig();
  if (!cfg.token) throw new Error('APIトークン未設定 (拡張機能のオプションで設定してください)');
  const res = await fetch(cfg.baseUrl.replace(/\/+$/, '') + BASE_PATH + path, {
    headers: { 'x-api-key': cfg.token },
  });
  return res;
}

/** 下見 (JSON) */
async function apiJson(path) {
  const res = await request(path);
  let json = null;
  try { json = await res.json(); } catch { /* JSONでない */ }
  if (!res.ok || !json || json.success === false) {
    const msg = json && json.error && json.error.message ? json.error.message : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return json.data;
}

/** CSV本体 (text)。ファイル名は Content-Disposition から取る */
async function apiCsv(path) {
  const res = await request(path);
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const j = await res.json();
      if (j && j.error && j.error.message) msg = j.error.message;
    } catch { /* CSVでもJSONでもない */ }
    throw new Error(msg);
  }
  const csv = await res.text();
  const cd = res.headers.get('content-disposition') || '';
  const m = cd.match(/filename="([^"]+)"/);
  return {
    csv,
    filename: m ? m[1] : 'fukutu.csv',
    rows: Number(res.headers.get('x-fukutsu-rows') || 0),
  };
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  (async () => {
    try {
      let data;
      if (msg.type === 'plan') {
        data = await apiJson(`/plan?wf=${encodeURIComponent(msg.wf)}`);
      } else if (msg.type === 'planCsv') {
        const q = msg.date ? `&date=${encodeURIComponent(msg.date)}` : '';
        data = await apiCsv(`/plan-csv?wf=${encodeURIComponent(msg.wf)}${q}`);
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
