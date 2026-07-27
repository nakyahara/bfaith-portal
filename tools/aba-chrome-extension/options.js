const DEFAULTS = { baseUrl: 'https://wh.bfaith-wh.uk', token: '' };

const $ = (id) => document.getElementById(id);

chrome.storage.local.get(DEFAULTS, (cfg) => {
  $('baseUrl').value = cfg.baseUrl;
  $('token').value = cfg.token;
});

function show(msg, ok) {
  const r = $('result');
  r.style.display = 'block';
  r.className = ok ? 'ok' : 'ng';
  r.textContent = msg;
}

$('save').addEventListener('click', () => {
  chrome.storage.local.set({
    baseUrl: $('baseUrl').value.trim() || DEFAULTS.baseUrl,
    token: $('token').value.trim(),
  }, () => show('保存しました', true));
});

$('test').addEventListener('click', () => {
  chrome.storage.local.set({
    baseUrl: $('baseUrl').value.trim() || DEFAULTS.baseUrl,
    token: $('token').value.trim(),
  }, () => {
    chrome.runtime.sendMessage({ type: 'status' }, (res) => {
      if (chrome.runtime.lastError) return show(`エラー: ${chrome.runtime.lastError.message}`, false);
      if (!res || !res.ok) return show(`接続失敗: ${res ? res.error : '応答なし'}`, false);
      const weeks = res.data.weeks || [];
      const latest = weeks[0];
      show(
        `接続OK\n取込済み週: ${weeks.length}件` +
        (latest ? `\n最新週: ${latest.week_start}〜${latest.week_end} (${Number(latest.row_count).toLocaleString()}行)` : '\n(まだレポート未取込)'),
        true
      );
    });
  });
});
