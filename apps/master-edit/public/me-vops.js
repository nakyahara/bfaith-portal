/*
 * me-vops.js — まとまりの操作の窓 (PR-7b・views/_vops.ejs)。新商品の登録 (色違い・サイズ違い) と商品の画面で同じ部品
 *   MasterEdit.vops.openLabels(groupId, { onDone }) = まとまりの名前・軸の名前・選択肢名を直す (POST /api/variation/groups/:id/labels・見た revision つき・変えた所だけ送る)
 *   MasterEdit.vops.openCancel(code, { onDone })   = まとまりの子の廃止 (POST /api/sku/:code/variation-cancel・理由が要る)
 *   MasterEdit.vops.openAdopt(code, { onDone })    = NE で直接作られた商品の代表を NE から 1 回だけ採用 (POST /api/sku/:code/adopt-parent)
 *   商品の画面のボタン = [data-vops="labels|cancel|adopt"] (data-gid / data-code)。通ったら画面を読み直す
 *   操作 1 回に request_id 1 つ (通信が切れて押し直しても 2 回しない)。返事が来たら新しい番号
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var esc = function (x) { return String(x == null ? '' : x).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); };
  var vl = document.getElementById('vl-bg'), vc = document.getElementById('vc-bg');
  if (!vl || !vc) return;
  var BASE = vl.getAttribute('data-base') || '';
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function post(path, body) {
    return fetch(BASE + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { ok: r.ok && j.ok, status: r.status, j: j }; }); });
  }
  function say(el, t, cls) { el.className = 'msgline ' + (cls || ''); el.textContent = t || ''; }
  var opener = null;
  function openBox(bg, focusEl) { opener = document.activeElement; bg.classList.add('on'); setTimeout(function () { if (focusEl) focusEl.focus(); }, 0); }
  function closeBox(bg) { bg.classList.remove('on'); if (opener && opener.focus) try { opener.focus(); } catch (e) { /* */ } }

  /* ---------- 名前を直す ---------- */
  var L = null;   // { gid, group, rid, onDone, busy }
  function labelInputs() {
    var g = L.group;
    var h = '';
    if (g.kind === 'tag') h += '<div class="f"><label class="lab" for="vl-name">まとまりの名前</label><div class="ctl"><input class="in w-l" id="vl-name" maxlength="255" value="' + esc(g.name) + '" data-was="' + esc(g.name) + '"></div></div>';
    else h += '<div class="f"><div class="lab">まとまりの名前</div><div class="ctl"><span class="lockval"><svg class="ic" aria-hidden="true"><use href="#i-lock"/></svg>' + esc(g.name) + '</span><span class="hint">単品が代表のまとまりの名前 = その単品 (' + esc(g.code) + ') の名前 = 商品の画面で直します</span></div></div>';
    if (!g.axes.length) h += '<div class="hint" style="margin:6px 0">このまとまりには軸と選択肢の記録がまだありません (はじめて色・サイズを足すときに入れます)。</div>';
    g.axes.forEach(function (a) {
      h += '<div class="f"><label class="lab" for="vl-ax-' + a.axis + '">' + (a.axis === 1 ? '横軸' : '縦軸') + 'の名前</label><div class="ctl"><input class="in w-s" id="vl-ax-' + a.axis + '" data-axis="' + a.axis + '" maxlength="100" value="' + esc(a.name) + '" data-was="' + esc(a.name) + '"></div></div>';
      var opts = g.options.filter(function (o) { return o.axis === a.axis; });
      if (opts.length) {
        h += '<div class="scrollx"><table class="opttbl"><thead><tr><th>コードにつける文字</th><th>' + esc(a.name) + 'の名前</th></tr></thead><tbody>'
          + opts.map(function (o) { return '<tr><td class="num"><span class="lockval" style="padding:0 6px"><svg class="ic s" aria-hidden="true"><use href="#i-lock"/></svg>' + esc(o.code) + '</span></td><td><input class="in vl-opt" data-axis="' + o.axis + '" data-code="' + esc(o.code) + '" maxlength="100" value="' + esc(o.name) + '" data-was="' + esc(o.name) + '" aria-label="' + esc(o.code) + ' の名前"></td></tr>'; }).join('')
          + '</tbody></table></div>';
      }
    });
    return h;
  }
  function labelChanges() {
    var out = {};
    var n = $('#vl-name');
    if (n && n.value.trim() !== n.getAttribute('data-was')) out.name = n.value.trim();
    var axes = $$('#vl-body input[data-axis]:not(.vl-opt)').filter(function (x) { return x.value.trim() !== x.getAttribute('data-was'); }).map(function (x) { return { axis: Number(x.getAttribute('data-axis')), name: x.value.trim() }; });
    if (axes.length) out.axes = axes;
    var opts = $$('#vl-body .vl-opt').filter(function (x) { return x.value.trim() !== x.getAttribute('data-was'); }).map(function (x) { return { axis: Number(x.getAttribute('data-axis')), code: x.getAttribute('data-code'), name: x.value.trim() }; });
    if (opts.length) out.options = opts;
    return out;
  }
  function labelState() {
    if (!L || !L.group) return;
    var ch = labelChanges();
    var n = Object.keys(ch).length;
    var empty = $$('#vl-body input').some(function (x) { return !x.value.trim(); });
    $('#vl-yes').disabled = L.busy || !n || empty;
    say($('#vl-msg'), empty ? '空の名前にはできません' : n ? '' : '名前を変えると押せます', empty ? 'err' : '');
  }
  function openLabels(gid, o) {
    L = { gid: String(gid), group: null, rid: uuid(), onDone: o && o.onDone, busy: false };
    $('#vl-body').innerHTML = '<div class="empty">読んでいます…</div>';
    $('#vl-reason').value = '';
    say($('#vl-msg'), '');
    $('#vl-yes').disabled = true;
    openBox(vl, null);
    fetch(BASE + '/api/variation/groups/' + encodeURIComponent(L.gid), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (!L || L.gid !== String(gid)) return;
        if (!x.r.ok || !x.j.ok) { $('#vl-body').innerHTML = '<div class="empty">' + esc(x.j.error || ('読めませんでした (HTTP ' + x.r.status + ')')) + '</div>'; return; }
        L.group = x.j.group;
        $('#vl-h').lastChild.textContent = 'まとまり ' + L.group.code + ' の名前を直す';
        $('#vl-body').innerHTML = labelInputs();
        labelState();
        var f = $('#vl-body input'); if (f) f.focus();
      })
      .catch(function () { $('#vl-body').innerHTML = '<div class="empty">つながりません。少し待ってからもう一度</div>'; });
  }
  vl.addEventListener('input', labelState);
  $('#vl-no').addEventListener('click', function () { closeBox(vl); L = null; });
  $('#vl-yes').addEventListener('click', function () {
    if (!L || !L.group || L.busy) return;
    var ch = labelChanges();
    L.busy = true; labelState(); say($('#vl-msg'), '直しています…');
    var me = L;
    post('/api/variation/groups/' + encodeURIComponent(me.gid) + '/labels', { request_id: me.rid, seen_revision: me.group.revision, changes: ch, reason: $('#vl-reason').value.trim() })
      .then(function (x) {
        me.busy = false; me.rid = uuid();
        if (!x.ok) { say($('#vl-msg'), x.j.error || ('HTTP ' + x.status), 'err'); labelState(); return; }
        closeBox(vl); L = null;
        if (ME.toast) ME.toast(x.j.no_change ? '変わる所がありませんでした' : '名前を直しました (社内だけ・NE には送りません)');
        if (me.onDone) me.onDone(x.j);
      })
      .catch(function () { me.busy = false; labelState(); say($('#vl-msg'), '通信できませんでした。もう一度押してください (同じ操作は 2 回しません)', 'err'); });
  });

  /* ---------- 子の廃止 / 代表の採用 ---------- */
  var K = null;   // { kind: cancel | adopt, code, rid, onDone, busy }
  var KIND = {
    cancel: { t: 'このまとまりから外す (廃止)', yes: '廃止する', need: true, path: '/variation-cancel',
      d: 'この子 (<b class="mono">%c</b>) の登録をやめます (登録の状態 = やめた)。コードは使い回しません。まとまりやほかの子は変えません。廃止できるのは、下書き / NE 登録待ちで、終わっていない NE 登録の CSV が無く、NE に一度も現れていない子だけです (Company DB が確かめます)。' },
    adopt: { t: 'NE の代表を採用する', yes: '採用する', need: false, path: '/adopt-parent',
      d: 'NE で直接作られた商品 (<b class="mono">%c</b>・要確認) の代表を、最新の照合で NE に見えた代表から 1 回だけ Company DB に入れます。採用した後は変えません。NE の代表が読めない・無い・2 つ以上に当たるときは何もしません。' },
  };
  function kState() { if (!K) return; var need = KIND[K.kind].need; var v = $('#vc-reason').value.trim(); $('#vc-yes').disabled = K.busy || (need && !v); }
  function openOp(kind, code, o) {
    K = { kind: kind, code: String(code), rid: uuid(), onDone: o && o.onDone, busy: false };
    var d = KIND[kind];
    $('#vc-t').textContent = d.t;
    $('#vc-d').innerHTML = d.d.replace('%c', esc(code));
    $('#vc-yes').textContent = d.yes;
    $('#vc-yes').className = 'btn ' + (kind === 'cancel' ? 'danger' : 'pri');
    $('#vc-req').hidden = !d.need;
    $('#vc-reason').value = '';
    $('#vc-reason').placeholder = kind === 'cancel' ? '例: この色は作らないことになった' : '例: NE で直接作ってしまった商品の代表をそろえる';
    say($('#vc-msg'), '');
    kState();
    openBox(vc, $('#vc-reason'));
  }
  $('#vc-reason').addEventListener('input', kState);
  $('#vc-no').addEventListener('click', function () { closeBox(vc); K = null; });
  $('#vc-yes').addEventListener('click', function () {
    if (!K || K.busy) return;
    var me = K;
    me.busy = true; kState(); say($('#vc-msg'), 'しています…');
    post('/api/sku/' + encodeURIComponent(me.code) + KIND[me.kind].path, { request_id: me.rid, reason: $('#vc-reason').value.trim() })
      .then(function (x) {
        me.busy = false; me.rid = uuid();
        if (!x.ok) { say($('#vc-msg'), x.j.error || ('HTTP ' + x.status), 'err'); kState(); return; }
        closeBox(vc); K = null;
        if (ME.toast) ME.toast(me.kind === 'cancel' ? me.code + ' を廃止しました' : me.code + ' の代表を ' + (x.j.group_code || '') + ' にしました');
        if (me.onDone) me.onDone(x.j);
      })
      .catch(function () { me.busy = false; kState(); say($('#vc-msg'), '通信できませんでした。もう一度押してください (同じ操作は 2 回しません)', 'err'); });
  });
  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    if (vl.classList.contains('on')) { closeBox(vl); L = null; }
    if (vc.classList.contains('on')) { closeBox(vc); K = null; }
  });

  ME.vops = { openLabels: openLabels, openCancel: function (c, o) { openOp('cancel', c, o); }, openAdopt: function (c, o) { openOp('adopt', c, o); } };
  // 商品の画面のボタン (通ったら読み直す)
  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('[data-vops]');
    if (!b || b.disabled) return;
    var reload = function () { if (ME.reloadPage) ME.reloadPage(); else location.reload(); };
    var k = b.getAttribute('data-vops');
    if (k === 'labels') openLabels(b.getAttribute('data-gid'), { onDone: reload });
    else if (k === 'cancel') openOp('cancel', b.getAttribute('data-code'), { onDone: reload });
    else if (k === 'adopt') openOp('adopt', b.getAttribute('data-code'), { onDone: reload });
  });
})();
