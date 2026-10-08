/*
 * me-list.js — 商品・セットの一覧 (画面 A) の「列」の板と表の動き (10/8 PR1・見本 masterlist_mock)
 *   - 右上の「列」: チェックで出す・出さない / つまみ ⋮⋮ を動かすか ↑↓ で並べ替え = 表がすぐ変わる (読み直さない)。
 *     「この列で保存」= 自分の設定としてサーバーに保存 (POST api/view-prefs・ログインのメールごと = 会社と家の PC で同じ)。
 *     「初期に戻す」= いつもの列に戻す (保存するまでは この画面だけ)。保存していない変更があれば「列」に「未保存」
 *   - コードの列はいつも左端・外せない (横に送っても残る = どの行か分かる)。表のセルは全部の列を描いてあり、出さない列は hidden
 *   - 行のどこを押しても その商品を開く (コードの列を横に残すため、コードのリンクの「行いっぱいの当たり」が使えない = ここで開く)
 *   - 並び替えは見出しのリンク (サーバーが全件で並べる)。ここでは押した後に「読み込み中」に見せるだけ
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function ic(id) { return '<svg class="ic s" aria-hidden="true"><use href="#' + id + '"/></svg>'; }
  var ME = window.MasterEdit || {};
  var toast = ME.toast || function () {};

  var tbl = $('#list-tbl'), cfgEl = $('#me-list-cfg');
  if (!tbl || !cfgEl) return;
  var cfg;
  try { cfg = JSON.parse(cfgEl.textContent); } catch (e) { return; }
  var COL = {}, CELL = {}; cfg.columns.forEach(function (c) { COL[c.id] = c; CELL[c.cell || c.id] = c.id; });
  var present = function (id) { return Object.prototype.hasOwnProperty.call(COL, id); };
  function copy(v) { return { order: v.order.slice(), shown: v.shown.slice() }; }
  function same(a, b) { return a.order.join(',') === b.order.join(',') && a.shown.join(',') === b.shown.join(','); }
  var saved = copy(cfg.view), view = copy(cfg.view), DEF = copy(cfg.defaultView);
  var colbtn = $('#colbtn'), panel = $('#colpanel'), list = $('#collist');

  /* ---------- 表に当てる (セルの並べ替えと出し入れ・読み直さない) ---------- */
  function apply() {
    var order = view.order.filter(present);
    var shown = {}; view.shown.forEach(function (id) { shown[id] = true; });
    $$('tr', tbl).forEach(function (tr) {
      var cells = {};
      // 見出し = data-c (列の id)・セル = data-col (前からの名前。cell → id は板の材料の columns)
      $$(':scope > [data-c], :scope > [data-col]', tr).forEach(function (td) { var id = td.getAttribute('data-c') || CELL[td.getAttribute('data-col')]; if (id) cells[id] = td; });
      if (!cells.code) return;   // 「当てはまる商品がありません」の行
      order.forEach(function (id) { var td = cells[id]; if (!td) return; tr.appendChild(td); td.hidden = !shown[id]; });
    });
    tbl.classList.toggle('show-sup', !!shown.sup);
    var cnt = $('#colcnt'); if (cnt) cnt.textContent = order.filter(function (id) { return shown[id]; }).length + '/' + order.length;
    if (colbtn) colbtn.classList.toggle('unsaved', !same(view, saved));
  }
  /** 板の中の並び (この人に出す列だけ) を入れ替えて、全部の並びに戻す (板に出さない列 = 注文残の権限が無い人 の場所はそのまま) */
  function movePanel(id, delta) {
    var po = view.order.filter(present);
    var i = po.indexOf(id), j = i + delta;
    if (i < 1 || j < 1 || j >= po.length) return false;   // コード (0 番) は動かさない・コードより上にも行かない
    var t = po[i]; po[i] = po[j]; po[j] = t;
    var k = 0;
    view.order = view.order.map(function (x) { return present(x) ? po[k++] : x; });
    view.shown = view.order.filter(function (x) { return view.shown.indexOf(x) >= 0; });
    return true;
  }
  function moveBefore(id, beforeId) {
    var po = view.order.filter(present).filter(function (x) { return x !== id; });
    var at = po.indexOf(beforeId); if (at < 1) at = 1;
    po.splice(at, 0, id);
    var k = 0;
    view.order = view.order.map(function (x) { return present(x) ? po[k++] : x; });
    view.shown = view.order.filter(function (x) { return view.shown.indexOf(x) >= 0; });
  }

  /* ---------- 板 ---------- */
  function renderPanel() {
    var po = view.order.filter(present);
    list.innerHTML = po.map(function (id, i) {
      var c = COL[id], on = view.shown.indexOf(id) >= 0;
      return '<li data-id="' + esc(id) + '" class="' + (on ? '' : 'off') + '" draggable="' + (c.fixed ? 'false' : 'true') + '">'
        + '<span class="grip" aria-hidden="true">' + (c.fixed ? '' : '⋮⋮') + '</span>'
        + '<label><input type="checkbox" ' + (on ? 'checked ' : '') + (c.fixed ? 'disabled ' : '') + 'aria-label="' + esc(c.label) + ' を出す">' + esc(c.label)
        + (c.isNew ? ' <span class="newtag">新</span>' : '')
        + (c.fixed ? ' <span class="lock">' + ic('i-lock') + '外せない</span>' : '') + (!c.sortable && !c.fixed ? ' <span class="nosort">並べられない</span>' : '') + '</label>'
        + '<button type="button" class="iconbtn" data-mv="-1"' + (i <= 1 || c.fixed ? ' disabled' : '') + ' aria-label="' + esc(c.label) + ' を上へ">' + ic('i-up') + '</button>'
        + '<button type="button" class="iconbtn" data-mv="1"' + (i === po.length - 1 || c.fixed ? ' disabled' : '') + ' aria-label="' + esc(c.label) + ' を下へ">' + ic('i-down') + '</button></li>';
    }).join('');
    $('#cp-save').disabled = same(view, saved);
    $('#cp-reset').disabled = same(view, DEF);
  }
  function isOpen() { return panel && !panel.hidden; }
  function openPanel() {
    panel.hidden = false; colbtn.setAttribute('aria-expanded', 'true'); renderPanel();
    var f = $('input:not([disabled])', list); if (f) f.focus();
  }
  function closePanel(restore) {
    if (!isOpen()) return;
    panel.hidden = true; colbtn.setAttribute('aria-expanded', 'false');
    if (restore !== false) colbtn.focus();
  }
  if (colbtn && panel && list) {
    colbtn.addEventListener('click', function () { if (isOpen()) closePanel(); else openPanel(); });
    $('#cp-x').addEventListener('click', function () { closePanel(); });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && isOpen()) { e.preventDefault(); closePanel(); } });
    document.addEventListener('mousedown', function (e) { if (isOpen() && !panel.contains(e.target) && !colbtn.contains(e.target)) closePanel(false); });
    list.addEventListener('change', function (e) {
      var li = e.target.closest('li'); if (!li) return;
      var id = li.getAttribute('data-id');
      if (COL[id] && COL[id].fixed) return;
      var s = view.shown.filter(function (x) { return x !== id; });
      if (e.target.checked) s.push(id);
      view.shown = view.order.filter(function (x) { return s.indexOf(x) >= 0; });
      renderPanel(); apply();
      var nb = $('li[data-id="' + id + '"] input', list); if (nb) nb.focus();
    });
    list.addEventListener('click', function (e) {
      var b = e.target.closest('[data-mv]'); if (!b || b.disabled) return;
      var id = b.closest('li').getAttribute('data-id'), d = Number(b.getAttribute('data-mv'));
      if (!movePanel(id, d)) return;
      renderPanel(); apply();
      var nb = $('li[data-id="' + id + '"] [data-mv="' + d + '"]', list);
      if (nb && !nb.disabled) nb.focus(); else { var other = $('li[data-id="' + id + '"] [data-mv="' + (-d) + '"]', list); if (other) other.focus(); }
    });
    var dragId = null;
    list.addEventListener('dragstart', function (e) {
      var li = e.target.closest('li'); if (!li || li.getAttribute('draggable') !== 'true') { e.preventDefault(); return; }
      dragId = li.getAttribute('data-id'); li.classList.add('dragging');
      try { e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', dragId); } catch (err) { /* */ }
    });
    list.addEventListener('dragover', function (e) {
      var li = e.target.closest('li'); if (!li || !dragId || li.getAttribute('data-id') === 'code') return;
      e.preventDefault();
      $$('li.over', list).forEach(function (x) { x.classList.remove('over'); }); li.classList.add('over');
    });
    list.addEventListener('drop', function (e) {
      e.preventDefault();
      var li = e.target.closest('li'); if (!li || !dragId) return;
      var to = li.getAttribute('data-id');
      if (to !== dragId) moveBefore(dragId, to);
      dragId = null; renderPanel(); apply();
    });
    list.addEventListener('dragend', function () { dragId = null; $$('li', list).forEach(function (x) { x.classList.remove('over', 'dragging'); }); });
    $('#cp-reset').addEventListener('click', function () {
      view = copy(DEF); renderPanel(); apply();
      toast('いつもの列に戻しました (まだ保存していません)');
      var s = $('#cp-save'); if (s && !s.disabled) s.focus();
    });
    var saving = false;
    $('#cp-save').addEventListener('click', function () {
      if (saving) return;
      var btn = this; saving = true; btn.disabled = true; btn.setAttribute('aria-busy', 'true');
      fetch(new URL('api/view-prefs', location.href).href, {
        method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ order: view.order, shown: view.shown })
      }).then(function (r) { return r.json().catch(function () { return { ok: false, error: '保存できませんでした (' + r.status + ')' }; }); })
        .then(function (j) {
          if (!j || !j.ok) { toast((j && j.error) || '保存できませんでした'); return; }
          saved = copy(j.view); view = copy(j.view);
          toast(j.saved ? '列を保存しました (' + (cfg.who || '') + ' さんの設定 · 会社と家の PC で同じ)' : 'いつもの列にしました (保存)');
        }, function () { toast('通信できませんでした。もう一度「この列で保存」を押してください'); })
        .then(function () { saving = false; btn.removeAttribute('aria-busy'); renderPanel(); apply(); });
    });
  }

  /* ---------- 行のどこを押しても開く (コードの列を横に残すと、リンクの当たりがコードの欄だけになるため) ---------- */
  function rowLink(e) {
    if (e.target.closest('a, button, input, label, select, textarea, summary, .c-chk')) return null;   // .c-chk = まとめて変えるの左のチェックの欄 (欄のすき間を押しても開かない)
    var tr = e.target.closest('tbody tr'); if (!tr || !tbl.contains(tr)) return null;
    return $('a.rowlink', tr);
  }
  tbl.addEventListener('click', function (e) {
    if (e.defaultPrevented || e.button !== 0) return;
    var a = rowLink(e); if (!a) return;
    var sel = window.getSelection ? String(window.getSelection()) : '';
    if (sel) return;   // 字を選んでいる (コピーしたい) ときは開かない
    if (e.ctrlKey || e.metaKey || e.shiftKey) { window.open(a.href, '_blank', 'noopener'); return; }
    if (ME.requestNavigate) ME.requestNavigate(a.href, a); else location.href = a.href;
  });
  tbl.addEventListener('auxclick', function (e) {
    if (e.button !== 1) return;
    var a = rowLink(e); if (!a) return;
    e.preventDefault(); window.open(a.href, '_blank', 'noopener');
  });

  /* ---------- 横に送ったら、残しているコードの列に影 ---------- */
  var wrap = $('#tblwrap');
  if (wrap) {
    var mark = function () { wrap.classList.toggle('scrolled', wrap.scrollLeft > 4); };
    wrap.addEventListener('scroll', mark, { passive: true }); mark();
  }

  /* ---------- 見出しを押した = 並べ直した一覧を読み込み中 ---------- */
  tbl.addEventListener('click', function (e) {
    var s = e.target.closest && e.target.closest('a.sorth');
    if (!s || e.defaultPrevented || e.button !== 0 || e.ctrlKey || e.metaKey || e.shiftKey) return;
    setTimeout(function () { if (!e.defaultPrevented) tbl.classList.add('loading'); }, 0);
  });
  window.addEventListener('pageshow', function () { tbl.classList.remove('loading'); });

  apply();
})();
