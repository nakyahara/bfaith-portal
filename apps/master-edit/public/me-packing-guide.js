/*
 * me-packing-guide.js — 商品の画面の「梱包指示」の欄の動き (views/_packing-guide.ejs・Company DB の 0101・lib/packing-guide.mjs)
 *   - 写真を足す = 選んだ / 撮った写真をここで縮めて (長辺 1600px の JPEG) すぐ送る (POST /api/sku/:code/packing-guide/photo・サーバーがもう一度作り直す)
 *   - 並べ替え・一言・外す・注意点 = 「梱包指示を保存」(POST /api/sku/:code/packing-guide・開いたときの版 seen_event_id。違えば 409)
 *   - 未保存は MasterEdit.dirty() に足す (離れるときの確認に出る)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var dataEl = document.getElementById('me-pgd');
  var box = document.getElementById('p-pgd');
  if (!dataEl || !box) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P || !P.canWrite) return;
  var ME = window.MasterEdit = window.MasterEdit || {};
  var LONG_EDGE = 1600;
  var VIA_WORDS = { sku_page: '商品の画面', packing: '梱包の画面' };
  var busy = false;
  var stale = false;
  /** 今の画面の写真の並び (外す印つき) */
  var rows = [];
  /** 保存してある指示 (比べる元) */
  var base = '';

  function normNote(s) { s = String(s || '').replace(/\r\n?/g, '\n'); return s.trim() === '' ? '' : s; }
  function snap(note, list) { return JSON.stringify({ note: normNote(note), photos: list }); }
  function snapOf(g) { return snap(g.note, g.photos.map(function (p) { return { photo_id: p.photo_id, caption: String(p.caption || '').trim() }; })); }
  function keep() { return rows.filter(function (r) { return !r.removed; }).map(function (r) { return { photo_id: r.photo_id, caption: r.caption.trim() }; }); }
  function noteNow() { var t = $('#pgd-note'); return t ? normNote(t.value) : ''; }
  function snapshot() { return snap(noteNow(), keep()); }
  function isDirty() { return snapshot() !== base; }
  function rowOf(p) { return { photo_id: p.photo_id, caption: p.caption || '', url: P.base + p.url, width: p.width, height: p.height, removed: false }; }
  function setBase(g) {
    P.guide = g;
    rows = g.photos.map(rowOf);
    var t = $('#pgd-note'); if (t) t.value = g.note || '';
    base = snapOf(g);
  }
  function msg(t, cls) { var m = $('#pgd-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function el(tag, attrs, kids) {
    var e = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) {
      if (k === 'text') e.textContent = attrs[k];
      else if (k === 'on') Object.keys(attrs.on).forEach(function (ev) { e.addEventListener(ev, attrs.on[ev]); });
      else if (k === 'value') e.value = attrs[k];
      else if (attrs[k] !== false && attrs[k] != null) e.setAttribute(k, attrs[k] === true ? '' : attrs[k]);
    });
    (kids || []).forEach(function (c) { if (c) e.appendChild(c); });
    return e;
  }
  function icon(name) {
    var s = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    s.setAttribute('class', 'ic s'); s.setAttribute('aria-hidden', 'true');
    var u = document.createElementNS('http://www.w3.org/2000/svg', 'use'); u.setAttribute('href', '#i-' + name); s.appendChild(u);
    return s;
  }
  function move(i, d) {
    var j = i + d;
    if (j < 0 || j >= rows.length) return;
    var x = rows[i]; rows[i] = rows[j]; rows[j] = x;
    render(); changed();
    var li = box.querySelectorAll('.pgd-ph')[j];
    var f = li && li.querySelector(d < 0 ? '[data-act="up"]' : '[data-act="down"]');
    if (f && !f.disabled) f.focus();
  }
  function render() {
    var list = $('#pgd-photos');
    list.textContent = '';
    var n = 0;
    rows.forEach(function (r, i) {
      if (!r.removed) n++;
      var cap = el('input', { class: 'in', type: 'text', maxlength: String(P.captionMax), value: r.caption, placeholder: '一言 (例: 底に緩衝材)', 'aria-label': '写真 ' + (i + 1) + ' の一言', disabled: r.removed,
        on: { input: function (e) { r.caption = e.target.value; changed(); } } });
      list.appendChild(el('li', { class: 'pgd-ph' + (r.removed ? ' removed' : '') }, [
        el('a', { href: r.url, target: '_blank', rel: 'noopener' }, [el('img', { src: r.url, alt: '入れ方の写真 ' + (i + 1), width: r.width, height: r.height, loading: 'lazy' })]),
        el('div', { class: 'pgd-cap' }, [el('span', { class: 'pgd-no', text: r.removed ? '外す' : String(n) }), cap]),
        el('div', { class: 'pgd-ops' }, [
          el('button', { type: 'button', class: 'btn sm', 'data-act': 'up', title: '前へ', 'aria-label': '写真 ' + (i + 1) + ' を前へ', disabled: i === 0, on: { click: function () { move(i, -1); } } }, [icon('up')]),
          el('button', { type: 'button', class: 'btn sm', 'data-act': 'down', title: '後ろへ', 'aria-label': '写真 ' + (i + 1) + ' を後ろへ', disabled: i === rows.length - 1, on: { click: function () { move(i, 1); } } }, [icon('down')]),
          el('button', { type: 'button', class: 'btn sm', 'data-act': 'rm', 'aria-pressed': r.removed ? 'true' : 'false',
            on: { click: function () { r.removed = !r.removed; render(); changed(); } } }, [icon(r.removed ? 'undo' : 'trash'), document.createTextNode(r.removed ? '戻す' : '外す')]),
        ]),
      ]));
    });
    var none = $('#pgd-nophoto'); if (none) none.hidden = rows.length > 0;
    // 足せるか = サーバーの今の写真の数 (外す印は保存するまで枠を空けない)
    var full = P.guide.photos.length >= P.max;
    var add = $('#pgd-add-btn'); if (add) add.classList.toggle('off', full || busy || stale);
    var file = $('#pgd-file'); if (file) file.disabled = full || busy || stale;
    var hint = $('#pgd-add-hint');
    if (hint) hint.textContent = full ? ('写真は ' + P.max + ' 枚までです。' + (rows.some(function (r) { return r.removed; }) ? '「梱包指示を保存」で外してから足せます' : 'どれかを外して保存してから足せます'))
      : 'iPad・スマホはカメラで撮れます。大きい写真は縮めて送ります';
  }
  function changed() {
    var d = isDirty();
    box.classList.toggle('po-dirty', d);
    var s = $('#pgd-save'); if (s) s.disabled = busy || stale || !d;
    var bd = $('#pgd-badge');
    if (bd) { var has = noteNow() !== '' || keep().length > 0; bd.className = 'b ' + (has ? 'info' : 'mute'); bd.textContent = has ? '指示あり' : 'まだ無し'; }
    if (ME.setUnsaved && ME.dirty) ME.setUnsaved(ME.dirty().n);
  }
  // 離れるときの確認 (me-shell.js) に梱包指示の未保存も出す
  var prevDirty = ME.dirty;
  ME.dirty = function () {
    var d = prevDirty ? prevDirty() : { n: 0, items: [], impacts: [] };
    if (!isDirty()) return d;
    return { n: d.n + 1, items: d.items.concat(['梱包指示: まだ保存していません']), impacts: d.impacts };
  };
  box.addEventListener('input', function (e) { if (e.target && e.target.id === 'pgd-note') changed(); });

  function post(path, body) {
    return fetch(P.base + '/api/sku/' + encodeURIComponent(P.code) + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); });
  }
  function lastLine(g) {
    var l = $('#pgd-last');
    if (l) l.textContent = g.updated_at ? '最後に変えた: ' + (g.updated_by || '—') + ' · ' + new Date(g.updated_at).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' }) + ' · ' + (VIA_WORDS[g.updated_via] || '') : '';
  }

  function save() {
    if (busy || stale || !isDirty()) return;
    busy = true; changed(); msg('保存しています…');
    post('/packing-guide', { request_id: uuid(), seen_event_id: P.guide.event_id, note: noteNow(), photos: keep() })
      .then(function (x) {
        busy = false;
        if (x.r.ok && x.j.ok) {
          setBase(x.j.guide); lastLine(x.j.guide); msg(x.j.changed ? '保存しました' : '変わったところはありませんでした', 'ok');
          if (ME.toast) ME.toast('梱包指示を保存しました');
        } else {
          if (x.j.reason === 'version_conflict' || x.j.reason === 'unknown_photo') stale = true;
          msg((x.j.error || ('HTTP ' + x.r.status)) + (stale ? ' (画面を開き直すと今の指示が出ます)' : ''), 'err');
        }
        render(); changed();
      })
      .catch(function () { busy = false; render(); changed(); msg('通信できませんでした。保存されたか分からないので、画面を開き直して確かめてください', 'err'); });
  }

  /** 写真を縮める (長辺 1600px の JPEG)。ブラウザが EXIF の向きを直して描く。読めない = null (そのまま送ってサーバーに任せる) */
  function shrink(file) {
    return new Promise(function (ok) {
      var url = URL.createObjectURL(file);
      var img = new Image();
      img.onload = function () {
        try {
          var w = img.naturalWidth, h = img.naturalHeight, k = Math.min(1, LONG_EDGE / Math.max(w, h));
          var c = document.createElement('canvas'); c.width = Math.round(w * k); c.height = Math.round(h * k);
          c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
          URL.revokeObjectURL(url);
          ok(c.toDataURL('image/jpeg', 0.85));
        } catch (e) { URL.revokeObjectURL(url); ok(null); }
      };
      img.onerror = function () { URL.revokeObjectURL(url); ok(null); };
      img.src = url;
    });
  }
  function asDataUrl(file) {
    return new Promise(function (ok, ng) { var r = new FileReader(); r.onload = function () { ok(r.result); }; r.onerror = ng; r.readAsDataURL(file); });
  }
  function addPhoto(file) {
    if (!file || busy || stale) return;
    busy = true; render(); changed(); msg('写真を縮めて送っています…');
    var opened = snapOf(P.guide);
    shrink(file).then(function (d) { return d || asDataUrl(file); })
      .then(function (data) { return post('/packing-guide/photo', { request_id: uuid(), image: data, caption: '' }); })
      .then(function (x) {
        busy = false;
        if (!(x.r.ok && x.j.ok)) { msg(x.j.error || ('HTTP ' + x.r.status), 'err'); render(); changed(); return; }
        var g = x.j.guide;
        var added = g.photos.filter(function (p) { return p.photo_id === x.j.photo_id; })[0];
        var others = { note: g.note, photos: g.photos.filter(function (p) { return p.photo_id !== x.j.photo_id; }) };
        if (snapOf(others) === opened) {
          // ほかの人が間に変えていない = 版を進めて、画面の未保存の変更 (並び・一言・注意点) はそのまま
          var wasDirty = isDirty();
          P.guide = g;
          base = snapOf(g);
          if (added) rows.push(rowOf(added));
          msg('写真を足しました' + (wasDirty ? ' (ほかの変更はまだ保存していません)' : ''), 'ok');
        } else {
          stale = true;
          msg('写真を足しました。ほかの人がこの商品の梱包指示を変えていたので、画面を開き直してください', 'err');
        }
        lastLine(g); render(); changed();
        if (ME.toast) ME.toast('写真を足しました');
      })
      .catch(function () { busy = false; render(); changed(); msg('通信できませんでした。写真が足されたか分からないので、画面を開き直して確かめてください', 'err'); });
  }
  var fileIn = $('#pgd-file');
  if (fileIn) fileIn.addEventListener('change', function () { var f = fileIn.files && fileIn.files[0]; fileIn.value = ''; addPhoto(f); });
  var saveBtn = $('#pgd-save'); if (saveBtn) saveBtn.addEventListener('click', save);

  setBase(P.guide);
  render();
  changed();
})();
