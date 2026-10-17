/*
 * me-amazon-inline.js — 一覧の行で「その場で登録」(10/17 中原さん・views/amazon-unmapped.ejs と views/amazon-unlinked.ejs の button[data-inline])
 *   - 押すと行の下に小さな欄が開く: GET /api/amazon/map?sku= (読むだけ) で 版・名前の初めの値 (今の対応 → Amazon の出品一覧の商品名 → 出品のタイトル)・
 *     まだ出品の無い SKU を先に作れない形 を読んでから、構成の行 (NE の商品 × 数・1 行目が代表) と名前 (社内) を入れる
 *   - NE の商品の欄 = 候補つき (public/me-comp-suggest.js = Amazon SKU の画面と同じ API・同じ決まり・日本語入力の確定の Enter の守り #1724)。
 *     名前が空なら、選んだ (使える) NE の商品の名前を入れる (me-amazon.js の fillNameFrom と同じ)
 *   - 保存 = POST /api/amazon/save (1 つの画面と同じ道 = lib/amazon-map-write.mjs の門・版・request_id・構成品の決まり)。
 *     request_id は保存 1 回に 1 つ (通信が切れて押し直しても 2 回入らない・返事が来たら新しい番号)。理由 = 「◯◯ の一覧からその場で登録」
 *   - もう構成がある SKU (開いた間に誰かが作った) = その場では直さない (1 つの画面へ)
 *   - 開いて何か入れた欄がある (構成・名前を開いたときから変えた) = 離れるときに確かめる (MasterEdit.dirty)
 *   - 🆕 #Codex R1 M: 結果の分からない保存 (通信が切れた) の後は、欄を閉じて同じ番号・同じ中身だけを送り直す (中身を変えて同じ番号で送らない)。
 *     送っている間も欄は触れない
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  var cfgEl = document.getElementById('me-inline');
  if (!cfgEl || !ME.compSuggest) return;
  var CFG = JSON.parse(cfgEl.textContent || '{}');
  var BASE = CFG.base || '';
  var MAXC = CFG.maxComponents || 20, MAXQ = CFG.maxQty || 999;
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function $(s, r) { return (r || document).querySelector(s); }
  function $$(s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  var normCode = ME.normCode;
  var seq = 0;
  var open = [];   // 開いている欄 (離れるときの確かめ)
  ME.dirty = function () {
    var items = open.filter(function (E) { return !E.saved && E.touched(); }).map(function (E) { return E.sku + ' (その場で登録の途中)'; });
    return { n: items.length, items: items, impacts: [] };
  };
  function refreshUnsaved() { if (ME.setUnsaved) ME.setUnsaved(ME.dirty().n); }
  var ICON = function (id) { return '<svg class="ic s" aria-hidden="true"><use href="#' + id + '"/></svg>'; };
  var REOPEN = ['version_conflict', 'request_id_reused', 'retry', 'norm_collision', 'already_deleted'];

  function Editor(btn) {
    var E = this;
    E.n = ++seq; E.btn = btn; E.sku = btn.getAttribute('data-sku'); E.saved = false; E.busy = false; E.versions = null; E.requestId = uuid();
    E.row = btn.closest('li');
    var li = document.createElement('li');
    li.className = 'inl-box'; li.setAttribute('data-inline-for', E.sku);
    var sid = 'inl' + E.n;
    li.innerHTML = '<div class="inl" role="group" aria-labelledby="' + sid + '-h">'
      + '<div class="inl-h" id="' + sid + '-h">' + ICON('i-plus') + '<span><b class="mono">' + esc(E.sku) + '</b> の対応をその場で作る</span><span class="hint inl-st">読んでいます…</span></div>'
      + '<div class="inl-body" hidden>'
      + '<div class="callout warn inl-warn" hidden></div>'
      + '<div class="scrollx"><table class="comp inl-comp"><caption class="sr">構成 (1 行目が代表)</caption><thead><tr><th scope="col">#</th><th scope="col">NE の商品 (コード・名前・JAN で探せます)</th><th scope="col">数</th><th scope="col"><span class="sr">並べ替え・外す</span></th></tr></thead><tbody class="inl-rows"></tbody></table></div>'
      + '<div class="csug" id="' + sid + '-sug" role="listbox" aria-label="NE の商品の候補" hidden></div><div class="sr" role="status" aria-live="polite" id="' + sid + '-say"></div>'
      + '<div class="inl-line"><button type="button" class="btn ghost sm inl-add">' + ICON('i-plus') + 'NE の商品を足す (セット)</button><span class="hint">1 行目が代表 · 1〜' + MAXC + ' 行 · 数は 1〜' + MAXQ + '</span></div>'
      + '<div class="inl-line"><label class="lab2" for="' + sid + '-name">名前 (社内)</label><input class="in inl-name" id="' + sid + '-name" maxlength="255" autocomplete="off"><span class="hint inl-from"></span></div>'
      + '<div class="inl-line inl-acts"><button type="button" class="btn pri inl-save">保存する</button><button type="button" class="btn ghost inl-cancel">やめる</button>'
      + '<a class="btn ghost sm" href="' + esc(BASE + '/amazon/sku?sku=' + encodeURIComponent(E.sku)) + '">1 つの画面で開く</a></div>'
      + '<div class="msgline inl-msg" role="status" aria-live="polite"></div>'
      + '</div></div>';
    E.row.parentNode.insertBefore(li, E.row.nextSibling);
    E.li = li; E.body = $('.inl-body', li); E.rows = $('.inl-rows', li); E.name = $('.inl-name', li); E.msgEl = $('.inl-msg', li); E.st = $('.inl-st', li);
    E.sug = ME.compSuggest({ root: E.rows, listbox: $('#' + sid + '-sug', li), say: $('#' + sid + '-say', li), base: BASE, min: CFG.min || 2, idPrefix: sid + '-opt-',
      usedAt: function (inp, code) { var n = normCode(code), at = 0; $$('tr', E.rows).some(function (tr, i) { if ($('.c-code', tr) !== inp && normCode($('.c-code', tr).value) === n) { at = i + 1; return true; } return false; }); return at; },
      onChoose: function (inp, it) { var tr = inp.closest('tr'); $('.c-name', tr).textContent = it.name || ''; $('.c-name', tr).classList.remove('bad'); E.renumber(); E.fillName(it.code, it.name); var q = $('.c-qty', tr); if (q) { q.focus(); q.select(); } refreshUnsaved(); } });
    E.rows.addEventListener('keydown', function (e) {
      if (E.sug.onKey(e)) return;
      if (e.key === 'Enter' && e.target.classList.contains('c-code')) { e.preventDefault(); var q = $('.c-qty', e.target.closest('tr')); if (q) { q.focus(); q.select(); } }
    });
    E.rows.addEventListener('change', function (e) { if (e.target.classList.contains('c-code')) E.lookup(e.target.closest('tr')); });
    E.rows.addEventListener('click', function (e) {
      var b = e.target.closest('button[data-act]'); if (!b || E.saved) return;
      var tr = b.closest('tr'), act = b.getAttribute('data-act');
      if (act === 'del') { if ($$('tr', E.rows).length > 1) tr.remove(); else { $('.c-code', tr).value = ''; $('.c-name', tr).textContent = ''; $('.c-qty', tr).value = '1'; } }
      if (act === 'up' && tr.previousElementSibling) tr.parentNode.insertBefore(tr, tr.previousElementSibling);
      if (act === 'down' && tr.nextElementSibling) tr.parentNode.insertBefore(tr.nextElementSibling, tr);
      E.renumber(); b.focus && document.contains(b) && b.focus(); refreshUnsaved();
    });
    li.addEventListener('input', refreshUnsaved);
    $('.inl-add', li).addEventListener('click', function () {
      var trs = $$('tr', E.rows);
      var last = trs[trs.length - 1];
      if (last && !$('.c-code', last).value.trim()) { $('.c-code', last).focus(); return; }
      if (trs.length >= MAXC) { if (ME.toast) ME.toast('構成は ' + MAXC + ' 行までです'); return; }
      E.addRow(); $('.c-code', E.rows.lastElementChild).focus();
    });
    $('.inl-cancel', li).addEventListener('click', function () { E.close(); });
    $('.inl-save', li).addEventListener('click', function () { E.save(); });
    btn.setAttribute('aria-expanded', 'true');
    open.push(E);
    E.load(true);
  }
  Editor.prototype.touched = function () { return $$('.c-code', this.rows).some(function (x) { return x.value.trim(); }) || (this.name0 != null && this.name.value !== this.name0); };
  /** 欄を触れなくする / 戻す (送っている間・結果の分からない保存の後) */
  Editor.prototype.lock = function (on) { $$('input, button.iconbtn, .inl-add', this.body).forEach(function (x) { x.disabled = !!on; }); };
  Editor.prototype.msg = function (t, cls) { this.msgEl.className = 'msgline inl-msg' + (cls ? ' ' + cls : ''); this.msgEl.textContent = t || ''; };
  Editor.prototype.addRow = function (c) {
    var E = this, i = $$('tr', E.rows).length, sid = 'inl' + E.n;
    var tr = document.createElement('tr');
    tr.className = 'comp-row';
    tr.innerHTML = '<td class="no"></td><td><input class="in mono c-code" style="width:150px" autocomplete="off" spellcheck="false" placeholder="コード・名前・JAN" role="combobox" aria-autocomplete="list" aria-expanded="false" aria-controls="' + sid + '-sug"> <span class="c-name"></span></td>'
      + '<td><input class="in c-qty" style="width:70px;text-align:right" inputmode="numeric" autocomplete="off" value="1"></td>'
      + '<td class="acts"><button type="button" class="iconbtn" data-act="up">' + ICON('i-up') + '</button><button type="button" class="iconbtn" data-act="down">' + ICON('i-down') + '</button><button type="button" class="iconbtn del" data-act="del">' + ICON('i-trash') + '</button></td>';
    if (c) { $('.c-code', tr).value = c.code; $('.c-qty', tr).value = String(c.qty); $('.c-name', tr).textContent = c.name || ''; }
    E.rows.appendChild(tr);
    E.renumber(i);
  };
  Editor.prototype.renumber = function () {
    $$('tr', this.rows).forEach(function (tr, i) {
      var code = $('.c-code', tr).value.trim(), who = (i + 1) + ' 行目' + (code ? ' (' + code + ')' : '');
      $('.no', tr).textContent = String(i + 1);
      $('.c-code', tr).setAttribute('aria-label', (i + 1) + ' 行目の NE コード' + (i === 0 ? ' (代表)' : ''));
      $('.c-qty', tr).setAttribute('aria-label', who + ' の数');
      var words = { up: 'を上へ', down: 'を下へ', del: 'を外す' };
      $$('button[data-act]', tr).forEach(function (b) { b.setAttribute('aria-label', who + ' ' + words[b.getAttribute('data-act')]); });
    });
  };
  /** 名前 (社内) が空なら、使える NE の商品の名前を入れる (me-amazon.js の fillNameFrom と同じ。書いてあれば触らない) */
  Editor.prototype.fillName = function (code, name) {
    if (this.saved || !name || this.name.value.trim() !== '') return;
    this.name.value = String(name).slice(0, 255);
    $('.inl-from', this.li).textContent = 'NE の商品名 (' + code + ') を入れました。直してもかまいません';
  };
  /** コードを打って欄を出た: 候補の API (保存と同じ決まり) で、ぴったりの使える商品なら名前を出す (名前が空なら入れる) */
  Editor.prototype.lookup = function (tr) {
    var E = this, code = $('.c-code', tr).value.trim(), cell = $('.c-name', tr);
    E.renumber();
    if (!code) { cell.textContent = ''; return; }
    if (Array.from(code).length < (CFG.min || 2)) { cell.textContent = '保存のときに確かめます'; return; }
    fetch(BASE + '/api/amazon/components?q=' + encodeURIComponent(code), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        if (!j || !j.ok || !tr.isConnected || $('.c-code', tr).value.trim() !== code) return;
        var hit = (j.items || []).filter(function (it) { return normCode(it.code) === normCode(code); })[0];
        if (!hit) { cell.textContent = '見つかりません (Company DB に無いコード)'; cell.classList.add('bad'); return; }
        cell.classList.toggle('bad', !hit.usable);
        cell.textContent = hit.usable ? (hit.name || '') : (hit.name || '') + ' (使えません: ' + hit.why + ')';
        if (hit.usable) E.fillName(hit.code, hit.name);
      })
      .catch(function () { cell.textContent = '確かめられません (通信)。保存のときに確かめます'; });
  };
  /** 版・名前の初めの値を読む。first = 開いたとき (欄を作る) / それ以外 = 読み直し (入れた値は残す) */
  Editor.prototype.load = function (first) {
    var E = this;
    E.st.textContent = '読んでいます…';
    return fetch(BASE + '/api/amazon/map?sku=' + encodeURIComponent(E.sku), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (!x.r.ok || !x.j.ok) { E.st.textContent = (x.j && x.j.error) || '読めませんでした (HTTP ' + x.r.status + ')'; E.st.classList.add('bad'); return; }
        var j = x.j, warn = $('.inl-warn', E.li);
        E.versions = j.versions; E.body.hidden = false; E.st.textContent = j.map && j.map.state === 'deleted' ? '削除済み (墓標) の SKU をもう一度登録します' : j.listing ? '出品はある (構成なし)' : '出品も無い (保存すると出品も作ります)';
        E.st.classList.remove('bad');
        var blocked = '';
        if (j.new_sku_problems && j.new_sku_problems.length) blocked = 'まだ出品の無い seller SKU は、この形では先に作れません: ' + j.new_sku_problems.join(' / ');
        else if (j.components && j.components.length) blocked = 'この SKU には、もう構成があります (' + j.components.map(function (c) { return c.code + '×' + c.qty; }).join(', ') + ')。1 つの画面で直してください';
        warn.hidden = !blocked; warn.textContent = blocked;
        $('.inl-save', E.li).disabled = !!blocked;
        if (first) {
          E.addRow();
          E.name.value = j.name_default || '';
          E.name0 = E.name.value;
          $('.inl-from', E.li).textContent = j.name_default ? (j.name_from === 'report' ? 'Amazon の出品一覧の商品名を入れました。直してもかまいません' : j.name_from === 'listing' ? '出品のタイトルを入れました。直してもかまいません' : '今の名前です') : '空なら、選んだ NE の商品の名前が入ります';
          if (!blocked) $('.c-code', E.rows).focus();
        }
      })
      .catch(function () { E.st.textContent = '通信できませんでした。「やめる」を押して、もう一度開いてください'; E.st.classList.add('bad'); });
  };
  Editor.prototype.close = function () {
    var E = this;
    if (!E.saved && E.touched() && !window.confirm(E.sku + ' に入れた構成を捨てて閉じますか')) return;
    E.sug.close(); E.sug.destroy();
    E.li.remove(); open = open.filter(function (x) { return x !== E; });
    E.btn.setAttribute('aria-expanded', 'false');
    if (!E.saved) E.btn.focus();
    refreshUnsaved();
  };
  Editor.prototype.save = function () {
    var E = this, saveBtn = $('.inl-save', E.li);
    if (E.busy || E.saved || saveBtn.disabled) return;
    // 結果の分からない保存の後 = 同じ番号で同じ中身をもう一度 (欄は閉じてある)
    if (E.pendingBody) { E.send(E.pendingBody); return; }
    var trs = $$('tr', E.rows), comps = [];
    for (var i = 0; i < trs.length; i++) {
      var code = $('.c-code', trs[i]).value.trim(), qty = $('.c-qty', trs[i]).value.trim();
      if (!code) { if (trs.length > 1) { E.msg((i + 1) + ' 行目の NE の商品 (コード) が空です。何も保存していません (いらない行は右の「外す」)', 'err'); $('.c-code', trs[i]).focus(); return; } continue; }
      comps.push({ code: code, qty: qty });
    }
    if (!comps.length) { E.msg('NE の商品を 1 つ以上入れてください。何も保存していません', 'err'); $('.c-code', E.rows).focus(); return; }
    if (!E.name.value.trim()) { E.msg('名前 (社内) を入れてください。何も保存していません', 'err'); E.name.focus(); return; }
    E.send({ request_id: E.requestId, seller_sku: E.sku, name: E.name.value.trim(), components: comps, reason: CFG.reason || 'その場で登録', seen: { versions: E.versions } });
  };
  Editor.prototype.send = function (body) {
    var E = this, saveBtn = $('.inl-save', E.li), comps = body.components;
    E.busy = true; saveBtn.disabled = true; E.lock(true); E.msg('保存しています…');
    fetch(BASE + '/api/amazon/save', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        E.busy = false;
        // 返事が読めない (JSON でない = 途中の機械の誤りなど) = 保存されたか分からない = 送り直しと同じに扱う
        if (!x.j || (!x.j.ok && !x.j.reason && !x.j.error)) { E.unknown(body); return; }
        E.pendingBody = null;
        if (x.r.ok && x.j.ok) {
          E.saved = true; E.sug.close();
          var comp = (x.j.components || []).map(function (c) { return c.code + '×' + c.qty; }).join(', ');
          E.body.innerHTML = '<div class="result ok" role="status"><div class="rt">' + ICON('i-check') + ' 保存しました' + (x.j.replayed ? ' (前に保存した結果)' : '') + ' · ' + esc(x.j.name && x.j.name.to ? x.j.name.to : body.name) + '</div>'
            + '<ul><li>構成: ' + esc(comp || comps.map(function (c) { return c.code + '×' + c.qty; }).join(', ')) + '</li>' + (x.j.listing_created ? '<li>出品も作りました</li>' : '') + '<li>古い表 (miniPC の SKU マスタ) と FBA 補充には、翌朝 7:00 の写しで届きます</li></ul>'
            + '<a class="btn soft sm" href="' + esc(BASE + '/amazon/sku?sku=' + encodeURIComponent(E.sku)) + '">1 つの画面で見る</a> <button type="button" class="btn ghost sm inl-close">閉じる</button></div>';
          $('.inl-close', E.li).addEventListener('click', function () { E.close(); });
          E.st.textContent = '登録しました';
          E.row.classList.add('done');
          E.btn.disabled = true; E.btn.textContent = '登録しました';
          if (ME.toast) ME.toast(E.sku + ' の対応を保存しました');
          refreshUnsaved();
          return;
        }
        E.requestId = uuid();   // 返事が来た = この番号の保存は終わった。直してもう一度は新しい番号
        saveBtn.disabled = false; E.lock(false);
        var reopen = REOPEN.indexOf(x.j.reason) >= 0;
        E.msg((x.j.error || '保存できませんでした (HTTP ' + x.r.status + ')') + (reopen ? ' →「読み直す」で今の版を読んでから、もう一度保存してください' : ''), 'err');
        if (reopen) {
          saveBtn.disabled = true;
          var rb = document.createElement('button'); rb.type = 'button'; rb.className = 'btn sm inl-reload'; rb.textContent = '読み直す';
          rb.addEventListener('click', function () { rb.remove(); E.msg(''); E.load(false); });
          E.msgEl.appendChild(document.createTextNode(' ')); E.msgEl.appendChild(rb);
        }
      })
      .catch(function () { E.busy = false; E.unknown(body); });
  };
  /** 保存されたか分からない = 欄を閉じたまま、もう一度押すと同じ番号・同じ中身で送る */
  Editor.prototype.unknown = function (body) {
    this.pendingBody = body;
    $('.inl-save', this.li).disabled = false;
    this.msg('通信できませんでした。もう一度「保存する」を押すと、同じ中身をもう一度送ります (同じ保存は 2 回入りません。欄は送った中身のまま閉じています)', 'err');
  };

  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('button[data-inline]');
    if (!b || b.disabled) return;
    e.preventDefault();
    var cur = open.filter(function (E) { return E.btn === b; })[0];
    if (cur) { cur.close(); return; }
    new Editor(b);
  });
})();
