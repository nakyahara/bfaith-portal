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
  chrome.storage.local.set(
    { baseUrl: $('baseUrl').value.trim() || DEFAULTS.baseUrl, token: $('token').value.trim() },
    then,
  );
}

$('save').addEventListener('click', () => save(() => show('保存しました', true)));

// 接続テストは実在しないプランIDで叩く。認証が通っていれば 404 (=「見つからない」) が返るので、
// 401/503 (トークン不正・未設定) と区別できる。Amazonのデータには一切触らない。
$('test').addEventListener('click', () => {
  save(() => {
    chrome.runtime.sendMessage({ type: 'plan', wf: 'wf00000000-0000-0000-0000-000000000000' }, (res) => {
      if (chrome.runtime.lastError) return show(`エラー: ${chrome.runtime.lastError.message}`, false);
      if (!res) return show('応答がありません', false);
      if (res.ok) return show('接続OK', true);
      const e = String(res.error || '');
      if (/納品プラン|見つかり|輸送箱/.test(e)) return show(`接続OK (認証は通っています)\n${e}`, true);
      show(`接続失敗: ${e}`, false);
    });
  });
});
