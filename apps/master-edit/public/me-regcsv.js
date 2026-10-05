/*
 * me-regcsv.js — NE 登録の CSV の画面の動き (第 2 段 10/5)
 *   - 作る: 選んだ数をボタンに出す (0 件なら押せない)。作れる商品を全部選ぶ
 *   - 配る: 押すとファイルをダウンロードして画面を読み直す
 *   - 申告: CSV のファイルを落とす (押して選ぶ) と、その場で sha256 を出して配ったファイルと照合する (64 文字を写さない)。結果は大きな 3 択
 *   - 使わない・実機で確かめた
 *   - 申告・使わないの書きかけ = 未保存 (data-dirty-field)。離れるときに確かめる
 *   - 送り方 (API) は前と同じ: POST /api/reg-csv/exports・/api/reg-csv/exports/:id/issue・/declare・/supersede・/api/reg-csv/verified・GET /file
 *   - 通ったら画面を読み直す (新しい状態)。上に結果を 1 回だけ出す
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var ME = window.MasterEdit = window.MasterEdit || {};
  var root = document.getElementById('rc');
  if (!root) return;
  var base = root.getAttribute('data-base') || '';
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  var buildIds = { products: uuid(), sets: uuid() };   // 作る 1 回に 1 つ (押し直しても 2 つ作らない)
  function post(path, body) {
    return fetch(base + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body || {}) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { ok: r.ok && j.ok !== false, status: r.status, j: j }; }); });
  }
  var errText = function (j, status) { return (j.error || ('HTTP ' + status)) + (Array.isArray(j.items) ? '\n' + j.items.map(function (x) { return x.code + ': ' + (x.blockers || []).join(' / '); }).join('\n') : ''); };
  function say(el, t, ok) { if (!el) return; el.className = el.className.replace(/\b(ok|err)\b/g, '').trim() + ' ' + (ok ? 'ok' : 'err'); el.textContent = t; el.style.whiteSpace = 'pre-wrap'; }

  /* ---------- 書きかけ (未保存) ---------- */
  var initial = new Map();
  function valueOf(el) { return el.type === 'checkbox' || el.type === 'radio' ? (el.checked ? '1' : '') : el.value; }
  $$('[data-dirty-field]', root).forEach(function (el) { initial.set(el, valueOf(el)); });
  var done = false;
  function dirtyDrawers() {
    if (done) return [];
    return $$('.drawer', root).filter(function (d) { return $$('[data-dirty-field]', d).some(function (el) { return !el.disabled && valueOf(el) !== initial.get(el); }); });
  }
  function drawerName(d) { var card = d.closest('.exp'); return 'ファイル #' + card.getAttribute('data-id') + ' の' + (d.getAttribute('data-drawer') === 'declare' ? '申告' : '「使わない」') + ' (書きかけ)'; }
  function update() { ME.setUnsaved(dirtyDrawers().length); }
  ME.dirty = function () { var ds = dirtyDrawers(); return { n: ds.length, items: ds.map(drawerName), impacts: [] }; };
  ME.review = function () {
    var d = dirtyDrawers()[0]; if (!d) return;
    d.hidden = false; d.scrollIntoView({ behavior: 'smooth', block: 'center' });
    var f = $('input:not([type=file]), button', d); if (f) f.focus({ preventScroll: true });
  };
  root.addEventListener('input', update);
  root.addEventListener('change', update);

  /* ---------- 作る: 選んだ数 ---------- */
  function picks(kind) { return $$('input.pick[data-kind]:checked', root).filter(function (x) { return !kind || x.getAttribute('data-kind') === kind; }); }
  function pickState() {
    ['products', 'sets'].forEach(function (k) {
      var n = picks(k).length, s = $('[data-n="' + k + '"]', root); if (s) s.textContent = String(n);
      var b = $('button[data-act="build"][data-kind="' + k + '"]', root); if (b && !b.hasAttribute('data-locked')) b.disabled = n === 0;
    });
    var all = $('#pick-all'), can = $$('input.pick[data-kind]:not(:disabled)', root);
    if (all) { all.checked = can.length > 0 && can.every(function (x) { return x.checked; }); all.indeterminate = !all.checked && can.some(function (x) { return x.checked; }); }
  }
  // 名簿でない・閉じている = サーバーが disabled で描いた = 触らない (選んでも押せないまま)
  $$('button[data-act="build"]', root).forEach(function (b) { if (b.disabled) b.setAttribute('data-locked', '1'); });
  root.addEventListener('change', function (e) {
    if (e.target.id === 'pick-all') { $$('input.pick[data-kind]:not(:disabled)', root).forEach(function (x) { x.checked = e.target.checked; }); }
    if (e.target.classList && e.target.classList.contains('pick')) pickState();
  });
  pickState();

  /* ---------- 申告: ファイルを落とすと sha256 を照合 ---------- */
  function hex(buf) { return Array.prototype.map.call(new Uint8Array(buf), function (x) { return x.toString(16).padStart(2, '0'); }).join(''); }
  function hashFile(drop, file) {
    var card = drop.closest('.exp'), want = card.getAttribute('data-sha'), id = card.getAttribute('data-id');
    var t = $('[data-drop-t]', drop), s = $('[data-drop-s]', drop), shaIn = $('input[name="sha256"]', card);
    drop.classList.remove('done', 'bad');
    if (!file) return;
    if (!(window.crypto && crypto.subtle && file.arrayBuffer)) { t.textContent = file.name; s.textContent = 'このブラウザではその場で照合できません。下の「sha256 を貼る」を使ってください'; drop.classList.add('bad'); return; }
    t.textContent = file.name; s.textContent = '照合しています…';
    file.arrayBuffer().then(function (buf) { return crypto.subtle.digest('SHA-256', buf); }).then(function (d) {
      var h = hex(d);
      if (shaIn) { shaIn.value = h; shaIn.dispatchEvent(new Event('input', { bubbles: true })); }
      if (h === want) { drop.classList.add('done'); s.textContent = '配ったファイル #' + id + ' と同じです (sha256 が一致)'; }
      else { drop.classList.add('bad'); s.textContent = '配ったファイル #' + id + ' と違います。別のファイルか、Excel などで開いて保存し直したファイルです (このままでは申告できません)'; }
      var ic = $('.fileico use', drop); if (ic) ic.setAttribute('href', h === want ? '#i-check' : '#i-warn');
    }).catch(function () { drop.classList.add('bad'); s.textContent = '読めませんでした。もう一度落とすか、sha256 を貼ってください'; });
  }
  $$('[data-drop]', root).forEach(function (drop) {
    var input = $('input[data-sha-file]', drop);
    input.addEventListener('change', function () { hashFile(drop, input.files && input.files[0]); });
    drop.addEventListener('dragover', function (e) { e.preventDefault(); drop.classList.add('over'); });
    drop.addEventListener('dragleave', function () { drop.classList.remove('over'); });
    drop.addEventListener('drop', function (e) { e.preventDefault(); drop.classList.remove('over'); var f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]; hashFile(drop, f); });
  });
  // いまの時刻を入れる (日本時間のまま・1 分前 = 「今より後」で断られないように)
  root.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('[data-now]'); if (!b) return;
    var inp = $('input[name="imported_at"]', b.closest('.drawer'));
    var d = new Date(Date.now() - 60000), p = function (n) { return String(n).padStart(2, '0'); };
    inp.value = d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + 'T' + p(d.getHours()) + ':' + p(d.getMinutes());
    inp.dispatchEvent(new Event('input', { bubbles: true }));
  });

  /* ---------- 通ったら読み直す (上に 1 回だけ知らせる) ---------- */
  var NOTE_KEY = 'master-edit:regcsv-note';
  function finish(text) {
    done = true; update();
    try { sessionStorage.setItem(NOTE_KEY, JSON.stringify({ at: Date.now(), t: text })); } catch (e) { /* 置けない = 知らせを出さないだけ */ }
    if (ME.reloadPage) ME.reloadPage(); else location.reload();
  }
  (function showNote() {
    var n = null;
    try { var raw = sessionStorage.getItem(NOTE_KEY); if (raw) { sessionStorage.removeItem(NOTE_KEY); n = JSON.parse(raw); } } catch (e) { return; }
    if (!n || !n.t || !(Date.now() - Number(n.at) < 5 * 60000)) return;
    var box = document.createElement('div');
    box.className = 'result ok saved-note'; box.setAttribute('role', 'status');
    var rt = document.createElement('div'); rt.className = 'rt'; rt.textContent = n.t; box.appendChild(rt);
    var ph = $('.ph', root); ph.parentNode.insertBefore(box, ph.nextSibling);
    if (ME.toast) ME.toast(n.t);
  })();
  function download(id) {
    var a = document.createElement('a');
    a.href = base + '/api/reg-csv/exports/' + id + '/file'; a.setAttribute('download', '');
    document.body.appendChild(a); a.click(); a.remove();
  }

  /* ---------- ボタン ---------- */
  root.addEventListener('click', function (ev) {
    var b = ev.target.closest && ev.target.closest('button[data-act]'); if (!b || b.disabled) return;
    var card = b.closest('.exp');
    var act = b.getAttribute('data-act');
    var cardMsg = card ? $('[data-msg-card]', card) : null;
    if (act === 'show-declare' || act === 'show-supersede') {
      var d = $(act === 'show-declare' ? '[data-drawer="declare"]' : '[data-drawer="supersede"]', card);
      var other = $(act === 'show-declare' ? '[data-drawer="supersede"]' : '[data-drawer="declare"]', card);
      if (other && !other.hidden) { other.hidden = true; var ob = $('[aria-controls="' + other.id + '"]', card); if (ob) ob.setAttribute('aria-expanded', 'false'); }
      d.hidden = !d.hidden; b.setAttribute('aria-expanded', d.hidden ? 'false' : 'true');
      if (!d.hidden) { d.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); var f = $('input:not([type=file])', d); if (f && f.offsetParent) f.focus({ preventScroll: true }); else { var dz = $('input[data-sha-file]', d); if (dz) dz.focus({ preventScroll: true }); } }
      return;
    }
    if (act === 'close-drawer') {
      var dr = b.closest('.drawer'); dr.hidden = true;
      var opener = $('[aria-controls="' + dr.id + '"]', card); if (opener) { opener.setAttribute('aria-expanded', 'false'); opener.focus(); }
      return;
    }
    var id = card ? card.getAttribute('data-id') : null;
    var drawer = b.closest('.drawer');
    var dmsg = drawer ? $('[data-msg]', drawer) : card ? cardMsg : $('#build-msg');
    b.disabled = true;
    var fail = function (t) { say(dmsg, t, false); b.disabled = false; };
    var p;
    if (act === 'build') {
      var kind = b.getAttribute('data-kind');
      var codes = picks(kind).map(function (x) { return x.value; });
      var bm = $('#build-msg');
      if (!codes.length) { say(bm, '作る商品に印を付けてください', false); b.disabled = false; return; }
      say(bm, 'CSV を作っています…', true);
      p = post('/api/reg-csv/exports', { kind: kind, codes: codes, request_id: buildIds[kind] }).then(function (r) {
        if (!r.ok) { say(bm, errText(r.j, r.status), false); buildIds[kind] = uuid(); b.disabled = false; return; }
        finish('ファイル #' + (r.j.export && r.j.export.export_id || '') + ' を作りました (' + codes.length + ' 件)。次は「配る (ダウンロード)」');
      });
    } else if (act === 'issue') {
      say(cardMsg, '配っています…', true);
      p = post('/api/reg-csv/exports/' + id + '/issue', {}).then(function (r) {
        if (!r.ok || r.j.refused) { say(cardMsg, r.j.refused ? '使わないにした商品があるので配れません (ファイルを閉じました)。作り直してください' : errText(r.j, r.status), false); b.disabled = false; return; }
        download(id);
        setTimeout(function () { finish('ファイル #' + id + ' を配りました (ダウンロード)。NE の「商品一括登録」で取り込んで、結果をここで申告してください'); }, 1200);
      });
    } else if (act === 'declare') {
      var res = $('input[type=radio]:checked', drawer);
      var sha = $('input[name="sha256"]', drawer).value.trim().toLowerCase();
      if (!sha) { fail('取り込んだファイルを落とすか、sha256 を貼ってください'); var dz2 = $('input[data-sha-file]', drawer); if (dz2) dz2.focus(); return; }
      if (sha !== card.getAttribute('data-sha')) { fail('配ったファイル #' + id + ' と違うファイルです (sha256 が合いません)。配ったファイルをそのまま取り込んでください'); return; }
      if (!res) { fail('NE の結果を選んでください'); var r0 = $('input[type=radio]', drawer); if (r0) r0.focus(); return; }
      var at = $('input[name="imported_at"]', drawer).value;
      say(dmsg, '申告しています…', true);
      p = post('/api/reg-csv/exports/' + id + '/declare', { sha256: sha, result: res.value, ne_message: $('input[name="ne_message"]', drawer).value, imported_at: at ? new Date(at).toISOString() : null, note: $('input[name="note"]', drawer).value })
        .then(function (r) { if (!r.ok) { fail(errText(r.j, r.status)); return; } finish('ファイル #' + id + ' を「取り込んだ」と申告しました。翌朝の照合で NE と同じか確かめます'); });
    } else if (act === 'supersede') {
      say(dmsg, '使わないにしています…', true);
      p = post('/api/reg-csv/exports/' + id + '/supersede', { reason: $('input[name="reason"]', drawer).value, correction: $('input[name="correction"]', drawer).value, confirm: $('input[name="confirm"]', drawer).checked })
        .then(function (r) { if (!r.ok) { fail(errText(r.j, r.status)); return; } finish('ファイル #' + id + ' を「使わない」にしました。直してから、もう一度 CSV を作ってください'); });
    } else if (act === 'verified') {
      say(cardMsg, '記録しています…', true);
      p = post('/api/reg-csv/verified', { kind: b.getAttribute('data-kind'), result: b.getAttribute('data-result'), export_id: id })
        .then(function (r) { if (!r.ok) { fail(errText(r.j, r.status)); return; } finish(b.getAttribute('data-result') === 'ok' ? 'この形を「実機で確かめた」にしました' : '「問題があった」と記録しました'); });
    } else { b.disabled = false; return; }
    p.catch(function () { fail('通信できませんでした。もう一度押してください'); });
  });
  update();
})();
