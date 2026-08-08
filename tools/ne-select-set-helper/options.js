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

function save(then) {
  chrome.storage.local.set({
    baseUrl: $('baseUrl').value.trim() || DEFAULTS.baseUrl,
    token: $('token').value.trim(),
  }, then);
}

$('save').addEventListener('click', () => save(() => show('保存しました', true)));

$('test').addEventListener('click', () => {
  save(() => {
    chrome.runtime.sendMessage({ type: 'sets' }, (res) => {
      if (chrome.runtime.lastError) return show(`エラー: ${chrome.runtime.lastError.message}`, false);
      if (!res || !res.ok) return show(`接続失敗: ${res ? res.error : '応答なし'}`, false);
      const codes = res.data.setCodes || [];
      show(`接続OK\n登録されている選べるセット: ${codes.length}件\n${codes.join('\n')}`, true);
    });
  });
});
