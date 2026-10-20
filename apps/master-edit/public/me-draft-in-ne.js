/*
 * me-draft-in-ne.js — 「NE にあるので登録済みにする」(0098・lib/master-draft-in-ne.mjs)。商品の画面 (views/_draftinne.ejs) と「NE 登録の CSV」の画面 (views/reg-csv.ejs の行ごと) の両方
 *   - ボタン = [data-dine-code] (商品コード) と [data-dine-hash] (画面を開いた時の DB の確かめの表)。押す → POST /api/sku/:code/draft-in-ne → 1 行
 *     「登録済みにしました。NE とちがう所は『NEへ送る』に出ます」(DB の答えの message)。data-dine-reload = 通ったら読み直す (商品の画面)
 *   - 通信が切れて押し直しても同じ request_id (DB は同じ番号の 2 回目に前の答えを返す)。断られた = DB の文のまま (ふつうの言葉)
 *   - 商品の画面で未保存の変更がある間は押さない (先に保存か元に戻す = 読み直しで消えないように)
 */
(function () {
  'use strict';
  var holder = document.getElementById('dine') || document.getElementById('rc');
  var base = (holder && holder.getAttribute('data-base')) || '';
  var newId = function () {
    if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
    var b = new Uint8Array(16); window.crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return (x + 256).toString(16).slice(1); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  };
  function dirtyCount() { try { var ME = window.MasterEdit; return ME && ME.dirty ? Number(ME.dirty().n || 0) : 0; } catch (e) { return 0; } }
  function msgOf(btn) {
    var row = btn.closest('[data-dine-row]') || btn.closest('#dine');
    return (row && row.querySelector('[data-dine-msg]')) || null;
  }
  function say(el, text, kind) {
    if (!el) return;
    el.textContent = text; el.classList.toggle('err', kind === 'err'); el.classList.toggle('ok', kind === 'ok');
  }
  Array.prototype.forEach.call(document.querySelectorAll('[data-dine-code]'), function (btn) {
    var requestId = newId(), busy = false;
    btn.addEventListener('click', function () {
      if (busy || btn.disabled) return;
      var msg = msgOf(btn);
      if (btn.hasAttribute('data-dine-reload') && dirtyCount() > 0) { say(msg, '先に、変えた欄を保存するか元に戻してください (登録済みにすると画面を読み直します)', 'err'); return; }
      busy = true; btn.disabled = true; say(msg, '登録済みにしています…');
      var code = btn.getAttribute('data-dine-code') || '';
      fetch(base + '/api/sku/' + encodeURIComponent(code) + '/draft-in-ne', {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: requestId, check_hash: btn.getAttribute('data-dine-hash') || '' }),
      }).then(function (res) {
        return res.json().catch(function () { return { ok: false, error: '答えを読めませんでした (' + res.status + ')' }; }).then(function (j) { return { status: res.status, j: j }; });
      }).then(function (x) {
        busy = false;
        if (x.j && x.j.ok) {
          say(msg, x.j.message || '登録済みにしました。NE とちがう所は『NEへ送る』に出ます', 'ok');
          btn.hidden = true;
          if (btn.hasAttribute('data-dine-reload')) setTimeout(function () { window.location.reload(); }, 1600);
          return;
        }
        btn.disabled = false;
        say(msg, (x.j && x.j.error) || ('できませんでした (' + x.status + ')'), 'err');
      }).catch(function () {
        busy = false; btn.disabled = false;
        say(msg, '通信が切れました。何が起きたか分からないので、もう一度押してください (同じ操作として送ります)', 'err');
      });
    });
  });
})();
