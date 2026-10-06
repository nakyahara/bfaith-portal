/*
 * me-amazon.js — Amazon SKU の対応 (1 つの seller SKU) の画面の動き (第 2 段 10/5)
 *   - 未保存の数 = data-dirty-field の欄だけ (名前 (社内)・構成)。構成は「中身どうし」(NE コード × 数の並び) をくらべる。
 *     削除の理由・保存の理由は数えない (削除は通常の保存とは別の操作 = 理由を入れて「削除する」→ 1 回だけ確かめる)
 *   - 右の「保存すると変わること」= 前 → 後 と、保存するとこうなる (代表・翌朝 7:00 の写し)
 *   - 保存・削除の送り方は前と同じ (POST /api/amazon/save { request_id, seller_sku, name, components, reason, seen: { versions } } / /api/amazon/delete)
 *   - 保存・削除が通ったら画面を読み直す (保存した後の値・新しい版で続けて直せる)。上に結果を 1 回だけ出す
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var ME = window.MasterEdit = window.MasterEdit || {};
  var dataEl = document.getElementById('me-amazon');
  var form = document.getElementById('f');
  var scope = document.getElementById('amazon-sku-page');
  if (!dataEl || !form || !scope) return;
  var P = JSON.parse(dataEl.textContent);
  var D = form.dataset;
  var BASE = P.base;
  var canSave = D.canSave === '1';
  var versions = JSON.parse(D.versions || '{}');

  /* ---------- 欄の値・未保存 ---------- */
  var rowsEl = $('#comp-rows');
  function components() {
    return $$('tr.comp-row', rowsEl).map(function (tr) { return { code: $('.c-code', tr).value.trim(), qty: $('.c-qty', tr).value.trim() }; })
      .filter(function (r) { return r.code || r.qty; });
  }
  var compText = function (rows) { return rows.length ? rows.map(function (r) { return r.code + '×' + r.qty; }).join(', ') : 'なし'; };
  function valueOf(el) { return el.id === 'comp' ? JSON.stringify(components()) : el.value; }
  function tracked() { return $$('[data-dirty-field]', scope).filter(function (el) { return !el.disabled; }); }
  var initial = new Map();
  tracked().forEach(function (el) { initial.set(el, valueOf(el)); });
  var initialRows = rowsEl ? rowsEl.innerHTML : '';
  var saved = false, busy = false, mustReload = false;
  function diffItems() {
    if (saved) return [];
    var out = [];
    tracked().forEach(function (el) {
      if (!initial.has(el) || valueOf(el) === initial.get(el)) return;
      if (el.id === 'comp') out.push({ k: 'components', label: '構成 (NE コード × 数)', from: compText(JSON.parse(initial.get(el) || '[]')), to: compText(components()) });
      else out.push({ k: el.getAttribute('data-dirty-field'), label: el.getAttribute('data-label') || el.id, from: initial.get(el) || '(空)', to: el.value || '(空)' });
    });
    return out;
  }
  function impacts(items) {
    var out = [];
    if (!items.length && !newReady()) return out;
    var rows = components();
    if (P.isNew) out.push([0, 'この seller SKU の対応を新しく作ります' + (items.length ? '' : ' (今の名前・構成のまま)')]);
    if (P.deleted) out.push([1, '削除済み (墓標) の seller SKU をもう一度登録します (登録日は新しくなります)']);
    if (items.some(function (x) { return x.k === 'components'; })) {
      if (rows.length && rows[0].code) out.push([0, '代表の NE コードは ' + rows[0].code + ' (1 行目)']);
      if (!rows.length) out.push([1, '構成が 1 行もありません (保存できません。やめるときは下の「削除する」)']);
    }
    out.push([0, '古い表 (miniPC の SKU マスタ・Render) と FBA 補充には、翌朝 7:00 の写しで届きます']);
    return out;
  }
  /**
   * 対応がまだ無い seller SKU = 夜間の取り込みの名前・構成のままでも新しく登録できる (変えた欄が 0 でも保存できる・#1628 Codex R1 M1)。
   * 名前と 1 行以上の構成があるときだけ (中身の確かめはサーバー)
   */
  function newReady() { return !!P.isNew && !saved && !!$('#name') && $('#name').value.trim() !== '' && components().some(function (r) { return r.code; }); }
  var saveBtn = $('#save'), revertBtn = $('#revert');
  var lastState = { n: 0, items: [], impacts: [] };
  function update() {
    var items = diffItems();
    $$('.f[data-row]', scope).forEach(function (f) { f.classList.toggle('dirty', items.some(function (it) { return it.k === f.getAttribute('data-row'); })); });
    $('#save-diff').innerHTML = items.map(function (it) {
      return '<li><div class="k"><span>' + esc(it.label) + '</span></div><div class="v"><span class="from">' + esc(it.from) + '</span><span class="muted" aria-label="から">→</span><span class="to">' + esc(it.to) + '</span></div></li>';
    }).join('');
    var empty = $('#save-empty');
    empty.hidden = items.length > 0;
    if (canSave && P.isNew && !items.length) empty.textContent = newReady() ? 'この名前・構成のまま保存すると、この seller SKU の対応を新しく作ります (直すところがあれば直してから)。' : '名前と構成 (1 行以上) を入れると、新しい対応を作れます。';
    var cnt = $('#save-count'); cnt.textContent = items.length + ' 件'; cnt.className = 'b count ' + (items.length ? 'warn' : 'mute');
    var imp = impacts(items);
    $('#save-impact').hidden = imp.length === 0;
    $('#save-impact-list').innerHTML = imp.map(function (x) { return '<li class="' + (x[0] ? 'warn' : '') + '">' + esc(x[1]) + '</li>'; }).join('');
    if (saveBtn) saveBtn.disabled = !canSave || saved || busy || mustReload || (items.length === 0 && !newReady());
    if (revertBtn) revertBtn.disabled = saved || items.length === 0;
    // 代表の印 = 1 行目
    $$('tr.comp-row', rowsEl).forEach(function (tr, i) {
      var b = $('.repbadge', tr);
      if (i === 0 && !b) { b = document.createElement('span'); b.className = 'repbadge'; b.textContent = '代表'; $('.c-name', tr).insertAdjacentElement('afterend', b); }
      if (i > 0 && b) b.remove();
    });
    ME.setUnsaved(items.length);
    lastState = { n: items.length, items: items.map(function (it) { return it.label + ': ' + it.from + ' → ' + it.to; }), impacts: imp.map(function (x) { return x[1]; }) };
  }
  ME.dirty = function () { return { n: lastState.n, items: lastState.items, impacts: lastState.impacts }; };
  form.addEventListener('input', update);
  form.addEventListener('change', update);

  /* ---------- 構成の行 ---------- */
  function relabel(tr, i) {
    var code = $('.c-code', tr).value.trim(), who = (i + 1) + ' 行目' + (code ? ' (' + code + ')' : '');
    $('.no', tr).textContent = String(i + 1);
    $('.c-code', tr).setAttribute('aria-label', (i + 1) + ' 行目の NE コード');
    $('.c-qty', tr).setAttribute('aria-label', who + ' の数');
    var acts = { up: 'を上へ', down: 'を下へ', del: 'を外す' };
    $$('button[data-act]', tr).forEach(function (b) { b.setAttribute('aria-label', who + ' ' + acts[b.getAttribute('data-act')]); });
  }
  function renumber() { $$('tr.comp-row', rowsEl).forEach(relabel); update(); }
  function lookup(tr) {
    var code = $('.c-code', tr).value.trim();
    var cell = $('.c-name', tr), reg = $('.c-reg', tr);
    if (!code) { cell.textContent = ''; reg.textContent = ''; return; }
    cell.textContent = '引き当てています…';
    fetch(BASE + '/api/lookup?code=' + encodeURIComponent(code), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if ($('.c-code', tr).value.trim() !== code) return;
        if (!x.r.ok || !x.j.ok) { cell.textContent = x.j.error || '見つかりません'; cell.classList.add('bad'); reg.textContent = ''; return; }
        cell.classList.toggle('bad', x.j.item.kind === 'exception');
        cell.textContent = x.j.item.name + (x.j.item.kind === 'exception' ? ' (例外の SKU = 構成品にできません)' : '');
        reg.innerHTML = '<span class="muted">保存のときに確かめます</span>';
      })
      .catch(function () { cell.textContent = '引き当てできません (通信)'; cell.classList.add('bad'); });
  }
  if (rowsEl && canSave) {
    rowsEl.addEventListener('click', function (e) {
      var b = e.target.closest('button[data-act]'); if (!b || b.disabled) return;
      var tr = b.closest('tr'), act = b.getAttribute('data-act');
      if (act === 'del') { var next = tr.nextElementSibling || tr.previousElementSibling; tr.remove(); if (next) $('.c-code', next).focus(); else $('#comp-add').focus(); }
      if (act === 'up' && tr.previousElementSibling) { tr.parentNode.insertBefore(tr, tr.previousElementSibling); b.focus(); }
      if (act === 'down' && tr.nextElementSibling) { tr.parentNode.insertBefore(tr.nextElementSibling, tr); b.focus(); }
      renumber();
    });
    rowsEl.addEventListener('change', function (e) { if (e.target.classList.contains('c-code')) { var tr = e.target.closest('tr'); relabel(tr, $$('tr.comp-row', rowsEl).indexOf(tr)); lookup(tr); } });
    rowsEl.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' || e.isComposing || e.keyCode === 229) return;
      if (e.target.classList.contains('c-code')) { e.preventDefault(); var q = $('.c-qty', e.target.closest('tr')); if (q) { q.focus(); q.select(); } }
    });
    $('#comp-add').addEventListener('click', function () {
      if ($$('tr.comp-row', rowsEl).length >= (P.maxComponents || 20)) { ME.toast('構成は ' + (P.maxComponents || 20) + ' 行までです'); return; }
      rowsEl.appendChild($('#comp-tpl').content.firstElementChild.cloneNode(true));
      renumber();
      $('.c-code', rowsEl.lastElementChild).focus();
    });
  }
  if (revertBtn) revertBtn.addEventListener('click', function () {
    tracked().forEach(function (el) { if (initial.has(el) && el.id !== 'comp') el.value = initial.get(el); });
    if (rowsEl) rowsEl.innerHTML = initialRows;
    $$('.f.err', scope).forEach(function (f) { f.classList.remove('err'); });
    update(); ME.toast('変更を元に戻しました');
  });

  /* ---------- 保存・削除 (送り方は前と同じ) ---------- */
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  // 保存 1 回に 1 つ (通信が切れて押し直しても 2 回入らない)。返事が来た後にもう一度保存するときは新しい番号
  var requestId = uuid();
  function msg(id, t, cls) { var m = $('#' + id); if (!m) return; m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; }
  var REOPEN = ['version_conflict', 'request_id_reused', 'retry', 'norm_collision', 'already_deleted', 'before_cutover', 'processing', 'abandoned'];
  function showError(j, status) {
    var reopen = REOPEN.indexOf(j.reason) >= 0;
    $('#result').innerHTML = '<div class="result err" role="alert"><div class="rt">' + esc(j.error || ('HTTP ' + status)) + '</div>'
      + (reopen ? '<button type="button" class="btn sm" id="reload">画面を開き直す</button>' : '') + '</div>';
    var b = $('#reload'); if (b) b.addEventListener('click', function () { saved = true; update(); location.reload(); });
    if (j.field) { var row = $('[data-row="' + String(j.field).replace(/"/g, '') + '"]', scope); if (row) { row.classList.add('err'); row.scrollIntoView({ behavior: 'smooth', block: 'center' }); } }
    return reopen;
  }
  function resultHtml(j) {
    var li = function (a) { return a.map(function (x) { return '<li>' + esc(x) + '</li>'; }).join(''); };
    if (j.no_change) return '<div class="rt">変わった項目がありません (何も保存していません) · 画面は今の値です</div><button type="button" class="btn ghost sm" id="saved-note-close">閉じる</button>';
    return '<div class="rt"><svg class="ic s" aria-hidden="true"><use href="#i-check"/></svg> ' + (j.state === 'deleted' ? '削除 (墓標に) しました' : '保存しました') + (j.replayed ? ' (前に保存した結果)' : '') + ' · 画面は保存した後の値です</div><ul>'
      + (j.created ? '<li>新しい対応を作りました' + (j.listing_created ? ' (出品も作りました)' : '') + '</li>' : '')
      + (j.revived ? '<li>削除済みの seller SKU をもう一度登録しました</li>' : '')
      + (j.name ? '<li>名前: ' + esc(j.name.from) + ' → ' + esc(j.name.to) + '</li>' : '')
      + (Array.isArray(j.components) ? '<li>構成: ' + esc(compText(j.components)) + '</li>' : '')
      + (Array.isArray(j.removed) ? '<li>外した構成: ' + esc(j.removed.map(function (c) { return c.code + '×' + c.qty; }).join(', ') || 'なし') + '</li>' : '')
      + '</ul>' + (j.notes && j.notes.length ? '<div class="sec2">気をつけること</div><ul>' + li(j.notes) + '</ul>' : '')
      + '<button type="button" class="btn ghost sm" id="saved-note-close">閉じる</button>';
  }
  var NOTE_KEY = 'master-edit:amazon-saved-note';
  function keepNote(j) {
    try { sessionStorage.setItem(NOTE_KEY, JSON.stringify({ sku: P.sku, at: Date.now(), j: { no_change: !!j.no_change, state: j.state, replayed: !!j.replayed, created: !!j.created, listing_created: !!j.listing_created, revived: !!j.revived, name: j.name || null, components: j.components || null, removed: j.removed || null, notes: j.notes || [] } })); } catch (e) { /* 置けない = 知らせを出さないだけ */ }
  }
  function showNote() {
    var n = null;
    try { var raw = sessionStorage.getItem(NOTE_KEY); if (raw) { sessionStorage.removeItem(NOTE_KEY); n = JSON.parse(raw); } } catch (e) { return; }
    if (!n || !n.j || n.sku !== P.sku || !(Date.now() - Number(n.at) < 5 * 60000)) return;
    var box = document.createElement('div');
    box.id = 'saved-note'; box.className = 'result ok saved-note'; box.setAttribute('role', 'status');
    box.innerHTML = resultHtml(n.j);
    var ph = $('#sku-ph'); ph.parentNode.insertBefore(box, ph.nextSibling);
    $('#saved-note-close').addEventListener('click', function () { box.remove(); });
    ME.toast(n.j.no_change ? '変わった項目がありません' : n.j.state === 'deleted' ? '削除 (墓標に) しました' : '保存しました');
  }
  function lockAfterSave() {
    var main = form.firstElementChild;
    main.setAttribute('inert', '');
    $$('input, select, textarea, button', main).forEach(function (x) { x.disabled = true; });
    ['#reason', '#revert'].forEach(function (s) { var x = $(s); if (x) x.disabled = true; });
  }
  function post(path, body, btn, msgId) {
    busy = true; if (btn) btn.disabled = true; update(); msg(msgId, '保存しています…'); $('#result').innerHTML = '';
    $$('.f.err', scope).forEach(function (f) { f.classList.remove('err'); });
    return fetch(BASE + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        if (x.r.ok && x.j.ok) {
          // 変わった項目が無い (数 1 → 01 など) ときも読み直す = 欄・比べる基準・「元に戻す」の先をサーバーの値にそろえる (#1628 Codex R5 L2)
          saved = true; lockAfterSave(); update(); msg(msgId, x.j.no_change ? '変わった項目がありません。画面を読み直しています…' : '保存しました。保存した後の値を読み直しています…', 'ok');
          keepNote(x.j);
          if (ME.reloadPage) ME.reloadPage(); else location.reload();
          return;
        }
        msg(msgId, '', 'err');
        if (showError(x.j, x.r.status)) mustReload = true;
        requestId = uuid();   // 返事が来た = この番号の保存は終わった。直してもう一度保存するときは新しい番号
        update(); delState();
      })
      .catch(function () { busy = false; update(); delState(); msg(msgId, '通信できませんでした。もう一度押してください (同じ保存は 2 回入りません)', 'err'); });
  }
  function doSave() {
    if (!saveBtn || saveBtn.disabled || busy) return;
    post('/api/amazon/save', { request_id: requestId, seller_sku: P.sku, name: $('#name').value, components: components(), reason: $('#reason').value, seen: { versions: versions } }, saveBtn, 'msg');
  }
  if (saveBtn && canSave) saveBtn.addEventListener('click', doSave);
  form.addEventListener('submit', function (e) { e.preventDefault(); });

  // 削除 = 理由を入れると押せる → 1 回だけ確かめる (理由も見せる)。未保存には数えない (通常の保存とは別の操作)
  var delBtn = $('#del'), delReason = $('#del-reason'), delBg = $('#del-bg'), delReturn = null;
  function delState() { if (delBtn) delBtn.disabled = !canSave || busy || saved || mustReload || !(delReason && delReason.value.trim()); }
  if (delReason) delReason.addEventListener('input', function (e) { e.stopPropagation(); delState(); });
  function closeDel(restore) { if (!delBg) return; delBg.classList.remove('on'); if (restore && delReturn) delReturn.focus(); }
  if (delBtn && delBg) {
    delBtn.addEventListener('click', function () {
      if (delBtn.disabled) return;
      if (ME.dirty().n > 0) { ME.toast('先に構成・名前の変更を保存するか、元に戻してください (削除は別の操作です)'); return; }
      delReturn = delBtn; $('#del-why').textContent = delReason.value.trim();
      delBg.classList.add('on'); $('#del-stay').focus();
    });
    $('#del-stay').addEventListener('click', function () { closeDel(true); });
    delBg.addEventListener('click', function (e) { if (e.target === delBg) closeDel(true); });
    $('#del-go').addEventListener('click', function () {
      closeDel(false);
      post('/api/amazon/delete', { request_id: requestId, seller_sku: P.sku, reason: delReason.value, seen: { versions: versions } }, delBtn, 'del-msg');
    });
    // 窓の中だけで Tab が回る・Esc で閉じる・窓が開いている間は全体から探す・保存のキーを効かせない
    window.addEventListener('keydown', function (e) {
      if (!delBg.classList.contains('on')) return;
      if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); closeDel(true); return; }
      if (e.key === 'Tab') {
        var f = $$('button', delBg);
        if (e.shiftKey && document.activeElement === f[0]) { e.preventDefault(); f[f.length - 1].focus(); }
        else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { e.preventDefault(); f[0].focus(); }
        else if (!delBg.contains(document.activeElement)) { e.preventDefault(); f[0].focus(); }
        e.stopPropagation(); return;
      }
      if (e.ctrlKey || e.metaKey) { e.preventDefault(); e.stopPropagation(); }
    }, true);
  }

  /* ---------- 保存の箱へ ---------- */
  var box = $('#savebox'), boxToggle = $('#savebox-toggle');
  function review() {
    if (!box) return;
    box.classList.add('open'); if (boxToggle) boxToggle.setAttribute('aria-expanded', 'true');
    box.scrollIntoView({ behavior: 'smooth', block: 'center' });
    (saveBtn && !saveBtn.disabled ? saveBtn : box).focus({ preventScroll: true });
  }
  ME.review = review;
  if (boxToggle) boxToggle.addEventListener('click', function () { var on = !box.classList.contains('open'); box.classList.toggle('open', on); boxToggle.setAttribute('aria-expanded', on ? 'true' : 'false'); });
  ME.onSave = canSave ? function () {
    if (saveBtn && !saveBtn.disabled) { doSave(); return; }
    if (mustReload) { ME.toast('この画面は古くなりました。「画面を開き直す」を押してください'); var rb = $('#reload'); if (rb) rb.focus(); return; }
    ME.toast(saved ? '保存しました。保存した後の値を読み直しています' : '保存する変更がありません');
  } : null;

  update();
  delState();
  showNote();
})();
