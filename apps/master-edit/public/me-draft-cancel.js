/*
 * me-draft-cancel.js — 商品の画面の「この下書きを消す」(0071・lib/master-draft-cancel.mjs・views/_deldraft.ejs)
 *   - 「この下書きを消す」→ 確かめの窓 (一緒に片付けるもの・理由) → 理由を書くと「消す」が押せる → POST /api/sku/:code/draft-cancel
 *   - 未保存の変更がある間は窓を開かない (先に保存か元に戻す = 読み直しで消えないように)
 *   - 通信が切れて押し直しても同じ request_id (DB は同じ番号の 2 回目に前の答えを返す)。理由を変えた押し直し = 新しい番号
 *   - 通ったら: product-hub のカードの結果を出す → 読み直す (カードの除外に失敗したときは読み直さずに知らせる = 人が product-hub で除外にする)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var box = document.getElementById('deldraft');
  var open = document.getElementById('deldraft-open');
  var bg = document.getElementById('deldraft-bg');
  if (!box || !open || !bg) return;
  var reason = $('#deldraft-reason'), yes = $('#deldraft-yes'), no = $('#deldraft-no'), msg = $('#deldraft-msg');
  var base = box.getAttribute('data-base') || '', code = box.getAttribute('data-code') || '';
  var newId = function () {
    if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
    var b = new Uint8Array(16); window.crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return (x + 256).toString(16).slice(1); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  };
  var requestId = newId(), sentReason = null, busy = false, done = false;
  function say(text, kind) { msg.textContent = text; msg.classList.toggle('err', kind === 'err'); msg.classList.toggle('ok', kind === 'ok'); }
  function state() { yes.disabled = busy || done || !reason.value.trim(); no.disabled = busy; }
  function dirtyCount() { try { var ME = window.MasterEdit; return ME && ME.dirty ? Number(ME.dirty().n || 0) : 0; } catch (e) { return 0; } }
  function show() { bg.classList.add('on'); state(); reason.focus(); }
  function close() { if (busy) return; bg.classList.remove('on'); open.focus(); }
  open.addEventListener('click', function () {
    if (dirtyCount() > 0) {
      var m = $('#deldraft .hint') || box;
      m.textContent = '先に、変えた欄を保存するか元に戻してください (消すと画面を読み直します)';
      return;
    }
    show();
  });
  reason.addEventListener('input', function (e) { e.stopPropagation(); state(); });
  reason.addEventListener('keydown', function (e) { if (e.key === 'Enter') { e.preventDefault(); if (!yes.disabled) yes.click(); } });
  no.addEventListener('click', close);
  bg.addEventListener('click', function (e) { if (e.target === bg) close(); });
  /** product-hub のカードの結果 → 1 行 */
  function cardWords(ph) {
    if (!ph) return '';
    if (ph.error) return ph.error;
    var cards = Array.isArray(ph.cards) ? ph.cards : [];
    var w = cards.map(function (c) {
      return c.outcome === 'excluded' ? 'product-hub のカード #' + c.draft_id + ' を「除外」にしました'
        : c.outcome === 'already' ? 'product-hub のカード #' + c.draft_id + ' はもう除外でした'
          : c.outcome === 'kept_listed' ? 'product-hub のカード #' + c.draft_id + ' は出品済みなので除外にしていません (product-hub で確かめてください)'
            : 'product-hub のカード #' + c.draft_id + ' はちょうどほかの操作が変えました (product-hub で確かめてください)';
    });
    if (Array.isArray(ph.same_code) && ph.same_code.length) w.push('同じコードの結んでいないカード (' + ph.same_code.map(function (x) { return '#' + x; }).join('・') + ') は触っていません');
    return w.join(' · ');
  }
  yes.addEventListener('click', function () {
    if (yes.disabled) return;
    var text = reason.value.trim();
    if (sentReason !== null && sentReason !== text) requestId = newId();   // 理由を変えた押し直し = 別の操作 (前の番号は前の理由のもの)
    sentReason = text;
    busy = true; state(); say('消しています…');
    fetch(base + '/api/sku/' + encodeURIComponent(code) + '/draft-cancel', {
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ request_id: requestId, reason: text, confirm: true }),
    }).then(function (res) {
      return res.json().catch(function () { return { ok: false, error: '答えを読めませんでした (' + res.status + ')' }; }).then(function (j) { return { status: res.status, j: j }; });
    }).then(function (x) {
      busy = false;
      if (x.j && x.j.ok) {
        done = true; state();
        var ph = x.j.product_hub;
        var head = x.j.replayed ? 'もう消してありました (やめた)' : '下書きを消しました (やめた)';
        var cw = cardWords(ph);
        if (ph && ph.error) {
          say(head + '。' + cw, 'err');
          no.disabled = false; no.textContent = '閉じて読み直す';
          no.onclick = function () { window.location.reload(); };
          return;
        }
        say(head + (cw ? '。' + cw : '') + '。画面を読み直します', 'ok');
        setTimeout(function () { window.location.reload(); }, cw ? 1600 : 700);
        return;
      }
      state();
      say((x.j && x.j.error) || ('できませんでした (' + x.status + ')'), 'err');
    }).catch(function () {
      busy = false; state();
      say('通信が切れました。何が起きたか分からないので、もう一度押してください (同じ操作として送ります)', 'err');
    });
  });
  // 窓の中だけで Tab が回る・Esc で閉じる・窓が開いている間は保存のキーなどを効かせない
  window.addEventListener('keydown', function (e) {
    if (!bg.classList.contains('on')) return;
    if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); close(); return; }
    if (e.key === 'Tab') {
      var f = $$('input, button:not([disabled])', bg);
      if (!f.length) return;
      if (e.shiftKey && document.activeElement === f[0]) { e.preventDefault(); f[f.length - 1].focus(); }
      else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { e.preventDefault(); f[0].focus(); }
      else if (!bg.contains(document.activeElement)) { e.preventDefault(); f[0].focus(); }
      e.stopPropagation(); return;
    }
    // 理由の欄の中のコピー・貼り付けは通す (保存のキー Ctrl + S・Ctrl + Enter は止める)
    if ((e.ctrlKey || e.metaKey) && (e.target !== reason || /^(s|S|Enter)$/.test(e.key))) { e.preventDefault(); e.stopPropagation(); }
  }, true);
})();
