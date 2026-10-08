/*
 * me-variant.js — 色違い・サイズ違いの代表を選ぶ部品 (0061・2026-10-08 中原さんの決定 a)。views/_variant-parent.ejs と一緒に使う
 *   - 「ひとつだけの商品 / ほかの商品の色違い・サイズ違い」を先に選ぶ (ふだんは 1 つ目 = 何も出さない)。2 つ目を選ぶと探す欄が出る
 *   - 探す: 打つと 250 ms 後に GET /api/variation-parents?q=…&self=… (名札・まとまりに入っていない単品。兄弟の商品コードでもその名札が出る)
 *     ↑↓ で候補・Enter で選ぶ・Esc で閉じる。NE にまだ無い代表は選べない (理由を出す = NE 登録の CSV で止まるので先に分かるように)
 *   - 新商品の登録 (data-mode = new): 選んだコードを隠しの欄 #f-variation_parent に入れる (me-new.js が登録で送る・未保存に数える)
 *   - 商品の画面 (data-mode = sku・下書きの単品): 「代表を保存」= POST /api/sku/:code/variation-parent { request_id, seen, parent }。通ったら画面を開き直す
 */
(function () {
  'use strict';
  var root = document.getElementById('vp');
  if (!root) return;
  var $ = function (s) { return root.querySelector(s); };
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var mode = root.getAttribute('data-mode');
  var can = root.getAttribute('data-can') === '1';
  var self = root.getAttribute('data-self') || '';
  var seen = root.getAttribute('data-seen') || '';
  var page = document.getElementById(mode === 'new' ? 'new-page' : 'sku-page');
  var base = root.getAttribute('data-base') || '';
  var hidden = document.getElementById('f-variation_parent');
  var seg = $('#vp-mode'), pick = $('#vp-pick'), chosen = $('#vp-chosen'), search = $('#vp-search'), q = $('#vp-q'), list = $('#vp-list'), msg = $('#vp-msg');
  var changeBtn = $('#vp-change'), saveBtn = $('#vp-save');
  var items = [], active = -1, timer = null, seq = 0, picked = null;

  function say(t, cls) { msg.className = 'fmsg ' + (cls || ''); msg.textContent = t || ''; }
  function setValue(code) {
    hidden.value = code || '';
    hidden.dispatchEvent(new Event('change', { bubbles: true }));
    if (saveBtn) saveBtn.disabled = !can || (hidden.value.toLowerCase() === seen.toLowerCase());
  }
  function kidsText(c) { return c.children ? '色違い・サイズ違い ' + c.children + ' 品 (' + c.sample.join('・') + (c.children > c.sample.length ? ' ほか' : '') + ')' : 'まだ色違いなし'; }
  function neBadge(c) {
    if (!c.ne || !c.ne.known) return '<span class="b mute">NE: 確かめられない</span>';
    return c.ne.ok ? '<span class="b ok"><svg class="ic" aria-hidden="true"><use href="#i-check"/></svg>NE にある (' + esc(c.ne.ne_code) + ')</span>' : '<span class="b err">NE にまだ無い</span>';
  }
  function showChosen(c) {
    chosen.innerHTML = '<span class="vp-ico" aria-hidden="true"><svg class="ic"><use href="#i-layers"/></svg></span>'
      + '<span class="vp-main"><span class="vp-code mono">' + esc(c.code) + '</span> <span class="vp-name">' + esc(c.name || '') + '</span>'
      + '<span class="vp-sub">' + (c.kind === 'tag' ? '名札 (まとまり)' : '単品が代表') + ' · ' + esc(kidsText(c)) + (c.via ? ' · ' + esc(c.via) + ' のまとまり' : '') + '</span></span>'
      + '<span class="vp-side">' + neBadge(c) + '</span>';
    chosen.hidden = false; search.hidden = true; if (changeBtn) changeBtn.hidden = false;
  }
  function openSearch() {
    chosen.hidden = true; search.hidden = false; if (changeBtn) changeBtn.hidden = true;
    q.value = ''; close(); q.focus();
  }
  function close() { list.hidden = true; q.setAttribute('aria-expanded', 'false'); q.removeAttribute('aria-activedescendant'); active = -1; }
  function draw() {
    if (!items.length) { list.innerHTML = '<li class="vp-empty" role="presentation">見つかりません (名札・兄弟の商品コード・名前の一部で探せます)</li>'; list.hidden = false; q.setAttribute('aria-expanded', 'true'); return; }
    list.innerHTML = items.map(function (c, i) {
      return '<li role="option" id="vp-opt-' + i + '" data-i="' + i + '" aria-selected="' + (i === active ? 'true' : 'false') + '"' + (c.disabled ? ' aria-disabled="true"' : '') + ' class="vp-opt' + (i === active ? ' on' : '') + (c.disabled ? ' off' : '') + '">'
        + '<span class="vp-kind ' + (c.kind === 'tag' ? 'tag' : 'single') + '">' + (c.kind === 'tag' ? '名札' : '単品') + '</span>'
        + '<span class="vp-main"><span class="vp-code mono">' + esc(c.code) + '</span> <span class="vp-name">' + esc(c.name || '') + '</span>'
        + '<span class="vp-sub">' + esc(kidsText(c)) + (c.via ? ' · ' + esc(c.via) + ' はこのまとまり' : '') + (c.disabled ? ' · 選べない: ' + esc(c.disabled) : '') + '</span></span>'
        + '<span class="vp-side">' + neBadge(c) + '</span></li>';
    }).join('');
    list.hidden = false; q.setAttribute('aria-expanded', 'true');
    if (active >= 0) { q.setAttribute('aria-activedescendant', 'vp-opt-' + active); var el = document.getElementById('vp-opt-' + active); if (el && el.scrollIntoView) el.scrollIntoView({ block: 'nearest' }); }
  }
  function find() {
    var text = q.value.trim();
    if (text.length < 2) { items = []; close(); say(text ? 'あと 1 文字' : ''); return; }
    var my = ++seq;
    say('探しています…', 'info');
    fetch(base + '/api/variation-parents?q=' + encodeURIComponent(text) + (self ? '&self=' + encodeURIComponent(self) : ''), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (my !== seq) return;
        if (!x.r.ok || !x.j.ok) { items = []; close(); say(x.j.error || '探せませんでした', 'err'); return; }
        items = x.j.items || []; active = items.findIndex(function (c) { return !c.disabled; });
        say(items.length ? items.length + ' 件' : '', '');
        draw();
      })
      .catch(function () { if (my === seq) { items = []; close(); say('通信できませんでした。もう一度打ってください', 'err'); } });
  }
  function choose(i) {
    var c = items[i]; if (!c) return;
    if (c.disabled) { say('選べません: ' + c.disabled, 'err'); return; }
    picked = c; close(); showChosen(c); setValue(c.code);
    say(mode === 'sku' ? '「代表を保存」で決まります' : '選びました (下書きの保存で決まります)', 'ok');
    if (changeBtn) changeBtn.focus();
  }

  if (seg) seg.addEventListener('change', function () {
    var has = seg.getAttribute('data-value') === 'has';
    pick.hidden = !has;
    if (!has) { setValue(''); picked = null; say(''); return; }
    if (!hidden.value) openSearch();
  });
  if (q) {
    q.addEventListener('input', function () { clearTimeout(timer); timer = setTimeout(find, 250); });
    q.addEventListener('keydown', function (e) {
      if (e.isComposing || e.keyCode === 229) return;
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        if (!items.length) return; e.preventDefault();
        var step = e.key === 'ArrowDown' ? 1 : -1, n = items.length, i = active;
        for (var k = 0; k < n; k++) { i = (i + step + n) % n; if (!items[i].disabled) break; }
        active = i; draw();
      } else if (e.key === 'Enter') { e.preventDefault(); if (active >= 0 && !list.hidden) choose(active); }
      else if (e.key === 'Escape') { if (!list.hidden) { e.preventDefault(); close(); } }
    });
    q.addEventListener('blur', function () { setTimeout(function () { if (!list.contains(document.activeElement)) close(); }, 150); });
  }
  list.addEventListener('mousedown', function (e) { e.preventDefault(); });   // 押した瞬間に入力の欄から離れて候補が消えないように
  list.addEventListener('click', function (e) { var li = e.target.closest('li[data-i]'); if (li) choose(Number(li.getAttribute('data-i'))); });
  if (changeBtn) changeBtn.addEventListener('click', openSearch);

  /* ---------- 商品の画面: 代表を保存 ---------- */
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  var requestId = uuid(), busy = false;
  if (saveBtn) saveBtn.addEventListener('click', function () {
    if (busy || saveBtn.disabled) return;
    busy = true; saveBtn.disabled = true; say('保存しています…', 'info');
    fetch(base + '/api/sku/' + encodeURIComponent(self) + '/variation-parent', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ request_id: requestId, seen: seen || null, parent: hidden.value || null }) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        if (x.r.ok && x.j.ok) {
          say(x.j.no_change ? '変わりませんでした' : '保存しました' + ((x.j.superseded || []).length ? ' (まだ配っていなかった NE 登録の CSV を使わないにしました = 作り直してください)' : '') + '。開き直しています…', 'ok');
          setTimeout(function () { location.reload(); }, x.j.superseded && x.j.superseded.length ? 1600 : 500);
          return;
        }
        requestId = uuid();
        say(x.j.error || ('HTTP ' + x.r.status), 'err');
        saveBtn.disabled = !can || hidden.value.toLowerCase() === seen.toLowerCase();
      })
      .catch(function () { busy = false; saveBtn.disabled = false; say('通信できませんでした。もう一度「代表を保存」を押してください (同じ保存は 2 回入りません)', 'err'); });
  });
  if (page && can && mode === 'sku') setValue(hidden.value);
})();
