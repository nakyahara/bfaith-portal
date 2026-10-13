/*
 * me-poq.js — 発注の設定の届け待ち (🆕 0075・CompanyDB構想 21 C1) の動き
 *   - 「もう一度届ける」= POST /api/order-settings-outbox/:id/retry (人の「もう一度」= 要確認も試す)。答えを行に出して、届いたら画面を読み直す
 *   - 「閉じる」= 行の中に理由の欄を出す → POST /api/order-settings-outbox/:id/dismiss { reason } (失敗 / 要確認だけ・商品の画面で入れ直した後など)
 */
(function () {
  'use strict';
  var page = document.getElementById('poq-page');
  if (!page) return;
  var base = page.getAttribute('data-base') || '';
  var msg = document.getElementById('poq-msg');
  function say(t, bad) { if (!msg) return; msg.textContent = t; msg.className = 'fmsg ' + (bad ? 'err' : 'ok'); }
  function post(path, body) {
    return fetch(base + path, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) })
      .then(function (res) { return res.json().catch(function () { return { ok: false, error: '答えを読めませんでした (' + res.status + ')' }; }).then(function (j) { return { status: res.status, j: j }; }); });
  }
  function rowOf(el) { while (el && !(el.classList && el.classList.contains('poq-row'))) el = el.parentNode; return el; }
  function codeOf(row) { var a = row.querySelector('.code'); return a ? a.textContent : ''; }
  function busy(row, on) { Array.prototype.forEach.call(row.querySelectorAll('button'), function (b) { b.disabled = !!on; }); }

  page.addEventListener('click', function (ev) {
    var btn = ev.target.closest ? ev.target.closest('button[data-act]') : null;
    if (!btn) return;
    var row = rowOf(btn);
    if (!row) return;
    var id = row.getAttribute('data-event');
    var act = btn.getAttribute('data-act');
    if (act === 'retry') {
      busy(row, true); say(codeOf(row) + ' を発注アプリへ届けています…');
      post('/api/order-settings-outbox/' + encodeURIComponent(id) + '/retry').then(function (x) {
        if (x.j && x.j.ok) { say(codeOf(row) + ': 届きました。画面を読み直します'); setTimeout(function () { window.location.reload(); }, 700); return; }
        busy(row, false);
        say(codeOf(row) + ': ' + ((x.j && (x.j.label ? x.j.label + ' — ' : '') + (x.j.error || '')) || ('届けられませんでした (' + x.status + ')')), true);
      }).catch(function () { busy(row, false); say('通信できませんでした。もう一度押してください', true); });
      return;
    }
    if (act === 'dismiss') {
      var act2 = row.querySelector('.poq-act');
      if (row.querySelector('.poq-reason')) return;
      var box = document.createElement('span');
      box.className = 'poq-reason';
      box.innerHTML = '<input type="text" maxlength="500" placeholder="閉じる理由 (例: 商品の画面で発注ロットを入れ直した)" aria-label="閉じる理由"> '
        + '<button class="btn danger sm" type="button" data-act="dismiss-go">閉じる</button> <button class="btn ghost sm" type="button" data-act="dismiss-cancel">やめる</button>';
      act2.appendChild(box);
      box.querySelector('input').focus();
      return;
    }
    if (act === 'dismiss-cancel') { var b = row.querySelector('.poq-reason'); if (b) b.parentNode.removeChild(b); return; }
    if (act === 'dismiss-go') {
      var input = row.querySelector('.poq-reason input');
      var reason = input ? input.value.trim() : '';
      if (!reason) { say('閉じる理由を書いてください', true); if (input) input.focus(); return; }
      busy(row, true); say(codeOf(row) + ' の知らせを閉じています…');
      post('/api/order-settings-outbox/' + encodeURIComponent(id) + '/dismiss', { reason: reason }).then(function (x) {
        if (x.j && x.j.ok) { say(codeOf(row) + ': 閉じました。画面を読み直します'); setTimeout(function () { window.location.reload(); }, 700); return; }
        busy(row, false);
        say(codeOf(row) + ': ' + ((x.j && x.j.error) || ('閉じられませんでした (' + x.status + ')')), true);
      }).catch(function () { busy(row, false); say('通信できませんでした。もう一度押してください', true); });
    }
  });
})();
