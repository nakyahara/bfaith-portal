/*
 * me-gone.js — 商品の画面の「NE から消えた (やめる)」(0070・lib/master-gone.mjs)
 *   - 理由を書き、「NE で消えたのを確かめた」に印を付けると押せる → 1 回だけ確かめる窓 (理由も見せる) → POST /api/sku/:code/gone
 *   - 未保存の変更がある間は押せない (先に保存か元に戻す = 読み直しで消えないように)
 *   - 通信が切れて押し直しても同じ request_id (DB は同じ番号の 2 回目に前の答えを返す)。通ったら画面を読み直す
 *   - 未保存の数には数えない (通常の保存とは別の操作)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var box = document.getElementById('gone-box');
  var btn = document.getElementById('gone-go');
  var bg = document.getElementById('gone-bg');
  if (!box || !btn || !bg) return;
  var reason = $('#gone-reason'), seen = $('#gone-seen'), msg = $('#gone-msg');
  var base = box.getAttribute('data-base') || '', code = box.getAttribute('data-code') || '';
  var newId = function () {
    if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
    var b = new Uint8Array(16); window.crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return (x + 256).toString(16).slice(1); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  };
  var requestId = newId(), sentReason = null, busy = false, done = false, back = null;  function say(text, bad) { msg.textContent = text; msg.classList.toggle('err', !!bad); msg.classList.toggle('ok', !bad && done); }
  function state() { btn.disabled = busy || done || !reason.value.trim() || !seen.checked; }
  reason.addEventListener('input', function (e) { e.stopPropagation(); state(); });
  reason.addEventListener('keydown', function (e) { if (e.key === 'Enter') e.preventDefault(); });
  seen.addEventListener('change', function (e) { e.stopPropagation(); state(); });
  function dirtyCount() { try { var ME = window.MasterEdit; return ME && ME.dirty ? Number(ME.dirty().n || 0) : 0; } catch (e) { return 0; } }
  function close(restore) { bg.classList.remove('on'); if (restore && back) back.focus(); }
  btn.addEventListener('click', function () {
    if (btn.disabled) return;
    if (dirtyCount() > 0) { say('先に、変えた欄を保存するか元に戻してください (やめた にすると画面を読み直します)', true); return; }
    back = btn; $('#gone-why').textContent = reason.value.trim();
    bg.classList.add('on'); $('#gone-stay').focus();
  });
  $('#gone-stay').addEventListener('click', function () { close(true); });
  bg.addEventListener('click', function (e) { if (e.target === bg) close(true); });
  $('#gone-ok').addEventListener('click', function () {
    close(false);
    var text = reason.value.trim();
    if (sentReason !== null && sentReason !== text) requestId = newId();   // 理由を変えた押し直し = 別の操作 (前の番号は前の理由のもの)
    sentReason = text;
    busy = true; state(); say('やめた にしています…');
    fetch(base + '/api/sku/' + encodeURIComponent(code) + '/gone', {
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ request_id: requestId, reason: text, confirm: true }),
    }).then(function (res) {
      return res.json().catch(function () { return { ok: false, error: '答えを読めませんでした (' + res.status + ')' }; }).then(function (j) { return { status: res.status, j: j }; });
    }).then(function (x) {
      busy = false;
      if (x.j && x.j.ok) {
        done = true; state();
        say((x.j.replayed ? 'もう やめた になっていました' : 'やめた にしました') + '。画面を読み直します');
        setTimeout(function () { window.location.reload(); }, 600);
        return;
      }
      state();
      say((x.j && x.j.error) || ('できませんでした (' + x.status + ')'), true);
    }).catch(function () {
      busy = false; state();
      say('通信が切れました。何が起きたか分からないので、もう一度押してください (同じ操作として送ります)', true);
    });
  });
  // 窓の中だけで Tab が回る・Esc で閉じる・窓が開いている間は保存のキーなどを効かせない
  window.addEventListener('keydown', function (e) {
    if (!bg.classList.contains('on')) return;
    if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); close(true); return; }
    if (e.key === 'Tab') {
      var f = $$('button', bg);
      if (e.shiftKey && document.activeElement === f[0]) { e.preventDefault(); f[f.length - 1].focus(); }
      else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { e.preventDefault(); f[0].focus(); }
      else if (!bg.contains(document.activeElement)) { e.preventDefault(); f[0].focus(); }
      e.stopPropagation(); return;
    }
    if (e.ctrlKey || e.metaKey) { e.preventDefault(); e.stopPropagation(); }
  }, true);
  state();
})();
