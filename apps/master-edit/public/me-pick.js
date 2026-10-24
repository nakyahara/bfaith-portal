/*
 * me-pick.js — 一覧の詳細検索の「名前で選ぶ」(仕入先・メーカー。2026-10-24 中原さん「仕入先をコードでなく名前で選びたい・メーカーでも絞りたい」)
 *   部品 = views/index.ejs の .pick[data-pick]。名簿 = 中の <select multiple> の選択肢 (data-name・data-code・data-off = 取引なし / 使っていない・data-none = メーカー未登録)
 *   - data-pick="into" (仕入先): 選ぶ = コードの欄 (#adv-sups) に足す・外す = 欄から消す。コードの欄に手で打ったコードも選んだ札に出る (0001 と 1 は同じ)
 *   - data-pick="select" (メーカー): 選ぶ = 選ぶ欄 (name=makers) の選択。JS が無いときは選ぶ欄をそのまま使う (Ctrl を押して複数)
 *   - 打つと名簿から探す: かなの同一視 (search-fold.mjs と同じ = NFKC・小文字・ひらがな → カタカナ) + 空白で区切った言葉が全部入っている。
 *     🚨 日本語の変換中は探さない (確定してから)。確定の Enter (Windows の Chrome・Edge は確定の後に keyCode 13 が来る) では選ばない・検索も送らない
 *     (me-comp-suggest.js と同じ = compositionend の後、次のキーを離すまでの 1 秒以内の Enter は確定の Enter)
 *   - ↑↓ で移る・Enter で選ぶ / 外す (箱は開いたまま = 続けて選べる)・Esc で閉じる・押す / タップでも。選んだ物は札 (× で外す)
 */
(function () {
  'use strict';
  var roots = document.querySelectorAll('.pick[data-pick]');
  if (!roots.length) return;
  var SHOW_MAX = 100;
  function fold(s) { return String(s == null ? '' : s).normalize('NFKC').toLowerCase().replace(/[ぁ-ゖゝゞ]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) + 0x60); }); }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  /** 仕入先のコードの同じと見る形 = サーバーと同じ (lib/sku-norm.js の normSku = 全角 → 半角・ダッシュ → ハイフン・空白を除く・小文字 + 先頭の 0 を除く。0001 と 1 は同じ・Codex R1 Low) */
  function normSku(v) {
    return String(v == null ? '' : v).replace(/[！-～]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) - 0xFEE0); }).replace(/　/g, ' ')
      .replace(/[−‐-―﹘﹣－]/g, '-').trim().replace(/\s+/g, '').toLowerCase();
  }
  function codeKey(s) { return normSku(s).replace(/^0+(?=.)/, ''); }
  var TOKEN_RE = /[^\s,、，;；]+/g;   // read.mjs の splitMulti と同じ区切り

  Array.prototype.forEach.call(roots, function (root, n) {
    var mode = root.getAttribute('data-pick');
    var src = root.querySelector('select');
    var into = mode === 'into' ? document.getElementById(root.getAttribute('data-into')) : null;
    if (!src || (mode === 'into' && !into)) return;
    var noun = root.getAttribute('data-noun') || '';
    var offWord = root.getAttribute('data-off-word') || '';
    var lab = root.querySelector('.lab');
    var id = 'pk' + n;
    var items = Array.prototype.map.call(src.options, function (o, i) {
      var code = o.getAttribute('data-code') || '';
      var name = o.getAttribute('data-name') || o.textContent;
      return { i: i, v: o.value, code: code, name: name, off: o.hasAttribute('data-off'), none: o.hasAttribute('data-none'), opt: o, key: code ? codeKey(code) : '', f: fold(code + ' ' + name) };
    });
    var byKey = Object.create(null);   // 名簿に無いコード (constructor など) を名簿の物と見ない (Codex R1 Low)
    items.forEach(function (it) { if (it.key && !byKey[it.key]) byKey[it.key] = it; });

    // ── 画面の部品 (札・探す欄・候補の箱・読み上げ) ──
    var chips = document.createElement('div');
    chips.className = 'pk-chips'; chips.id = id + '-chips'; chips.setAttribute('role', 'list'); chips.setAttribute('aria-label', '選んだ' + noun);
    var box = document.createElement('div'); box.className = 'pk-box';
    var inp = document.createElement('input');
    inp.className = 'in'; inp.id = id + '-in'; inp.type = 'text';
    inp.setAttribute('role', 'combobox'); inp.setAttribute('aria-autocomplete', 'list'); inp.setAttribute('aria-expanded', 'false'); inp.setAttribute('aria-controls', id + '-list');
    inp.setAttribute('autocomplete', 'off'); inp.setAttribute('spellcheck', 'false'); inp.setAttribute('enterkeyhint', 'search');
    inp.placeholder = noun + 'の名前' + (mode === 'into' ? 'かコード' : '') + 'で探す';
    if (lab) { if (!lab.id) lab.id = id + '-lab'; inp.setAttribute('aria-labelledby', lab.id); if (lab.tagName === 'LABEL') lab.htmlFor = inp.id; }
    var list = document.createElement('div');
    list.className = 'csug'; list.id = id + '-list'; list.hidden = true;
    list.setAttribute('role', 'listbox'); list.setAttribute('aria-multiselectable', 'true'); list.setAttribute('aria-label', noun + 'の候補');
    var live = document.createElement('span'); live.className = 'sr'; live.setAttribute('role', 'status'); live.setAttribute('aria-live', 'polite');
    box.appendChild(inp); box.appendChild(list);
    root.appendChild(chips); root.appendChild(box); root.appendChild(live);
    if (mode === 'select') src.hidden = true;   // JS がある = 選ぶ欄は隠す (送る値はこの欄のまま)
    root.hidden = false;

    // ── 選んでいる物 ──
    function tokens() { return String(into ? into.value : '').match(TOKEN_RE) || []; }
    function isPicked(it) {
      if (mode === 'select') return it.opt.selected;
      return tokens().some(function (t) { return codeKey(t) === it.key; });
    }
    function picked() {
      if (mode === 'select') return items.filter(function (it) { return it.opt.selected; });
      var seen = {}, out = [];
      tokens().forEach(function (t) { var it = byKey[codeKey(t)]; if (it && !seen[it.i]) { seen[it.i] = 1; out.push(it); } });
      return out;
    }
    function fire(el) { el.dispatchEvent(new Event('input', { bubbles: true })); el.dispatchEvent(new Event('change', { bubbles: true })); }
    function setPicked(it, on) {
      if (mode === 'select') { if (it.opt.selected !== on) { it.opt.selected = on; fire(src); } }
      else if (on) {
        if (!isPicked(it)) { var v = into.value.replace(/\s+$/, ''); into.value = (v ? v + '\n' : '') + it.code; fire(into); }
      } else {
        var rest = tokens().filter(function (t) { return codeKey(t) !== it.key; });
        into.value = rest.join('\n'); fire(into);
      }
      say((on ? '選びました: ' : '外しました: ') + label(it) + ' (いま ' + picked().length + ' つ)');
      drawChips();
    }
    function label(it) { return (it.code ? it.code + ' ' : '') + it.name; }
    function say(t) { live.textContent = ''; setTimeout(function () { live.textContent = t; }, 30); }

    function drawChips() {
      var ps = picked();
      chips.innerHTML = ps.map(function (it) {
        return '<span class="pk-chip' + (it.off ? ' off' : '') + '" role="listitem" data-i="' + it.i + '">' + (it.code ? '<span class="cc">' + esc(it.code) + '</span>' : '')
          + '<span class="nm">' + esc(it.name) + '</span>' + (it.off && offWord ? '<span class="tg">' + esc(offWord) + '</span>' : '')
          + '<button type="button" aria-label="' + esc(label(it)) + ' を外す" title="外す">×</button></span>';
      }).join('');
      if (S.open) draw();
    }
    chips.addEventListener('click', function (e) {
      var b = e.target.closest('button'); if (!b) return;
      var c = b.closest('.pk-chip'); var it = c && items[Number(c.getAttribute('data-i'))];
      if (!it) return;
      setPicked(it, false);
      inp.focus();
    });
    if (into) into.addEventListener('input', function (e) { if (e.isTrusted !== false) drawChips(); });

    // ── 候補の箱 ──
    var S = { open: false, q: null, shown: [], sel: -1 };
    function matches(q) {
      var words = fold(q).split(/\s+/).filter(Boolean);
      if (!words.length) return items;
      return items.filter(function (it) { return words.every(function (w) { return it.f.indexOf(w) >= 0; }); });
    }
    function draw() {
      var hits = matches(S.q || '');
      S.shown = hits.slice(0, SHOW_MAX);
      if (S.sel >= S.shown.length) S.sel = S.shown.length - 1;
      var head = hits.length > SHOW_MAX ? '<div class="cs-h"><span>' + hits.length + ' 件のうち ' + SHOW_MAX + ' 件</span><span>名前で絞ってください</span></div>' : '';
      list.innerHTML = head + (S.shown.length ? S.shown.map(function (it, k) {
        var on = isPicked(it);
        return '<div class="co pk-o' + (k === S.sel ? ' sel' : '') + (it.off ? ' pk-off' : '') + '" role="option" id="' + id + '-o' + k + '" data-k="' + k + '" aria-selected="' + on + '">'
          + '<span class="l1"><span class="ck" aria-hidden="true">✓</span>' + (it.code ? '<span class="cc">' + esc(it.code) + '</span>' : '')
          + '<span class="cn">' + esc(it.name) + '</span>' + (it.off && offWord ? '<span class="tg">' + esc(offWord) + '</span>' : '') + '</span></div>';
      }).join('') : '<div class="cs-empty">「' + esc(S.q) + '」に当たる' + noun + 'はありません</div>');
      if (S.sel >= 0) { inp.setAttribute('aria-activedescendant', id + '-o' + S.sel); var el = document.getElementById(id + '-o' + S.sel); if (el && el.scrollIntoView) el.scrollIntoView({ block: 'nearest' }); }
      else inp.removeAttribute('aria-activedescendant');
    }
    function open(q) {
      S.q = q; S.open = true; S.sel = -1;
      list.hidden = false; inp.setAttribute('aria-expanded', 'true');
      // 板 (.panel = backdrop-filter で重なりの単位) ごと後ろの一覧の板より上に出す = 候補の下の方も押せる (Codex R2 Medium・me-amazon.js と同じ)
      var pnl = root.closest('.panel'); if (pnl) pnl.classList.add('sug-on');
      draw();
    }
    function close() {
      var pnl = root.closest('.panel'); if (pnl) pnl.classList.remove('sug-on');
      S.open = false; S.sel = -1; list.hidden = true;
      inp.setAttribute('aria-expanded', 'false'); inp.removeAttribute('aria-activedescendant');
    }
    function move(d) {
      if (!S.shown.length) return;
      S.sel = S.sel < 0 ? (d > 0 ? 0 : S.shown.length - 1) : Math.max(0, Math.min(S.shown.length - 1, S.sel + d));
      draw();
    }
    function toggle(k) { var it = S.shown[k]; if (!it) return; S.sel = k; setPicked(it, !isPicked(it)); }

    // ── 打つ (変換中は探さない) ──
    var composing = false, imeEndAt = -1;
    inp.addEventListener('compositionstart', function () { composing = true; });
    inp.addEventListener('compositionend', function (e) { composing = false; imeEndAt = e.timeStamp; open(inp.value); });
    inp.addEventListener('input', function (e) { if (composing || e.isComposing) return; open(inp.value); });
    inp.addEventListener('keyup', function () { imeEndAt = -1; });
    inp.addEventListener('focus', function () { if (!S.open) open(inp.value); });
    inp.addEventListener('click', function () { if (!S.open) open(inp.value); });
    inp.addEventListener('keydown', function (e) {
      var confirmed = imeEndAt >= 0 && e.timeStamp - imeEndAt < 1000; imeEndAt = -1;
      if (e.key === 'Enter' && confirmed) { e.preventDefault(); return; }   // 日本語入力の確定の Enter = 字が確かまるだけ (選ばない・送らない)
      if (composing || e.isComposing || e.keyCode === 229) return;
      if (e.key === 'ArrowDown') { e.preventDefault(); if (!S.open) open(inp.value); else move(1); return; }
      if (e.key === 'ArrowUp') { e.preventDefault(); if (S.open) move(-1); return; }
      if (e.key === 'Enter') {
        // 箱が開いている = 選ぶ (選んでいる行が無ければ何もしない・検索は送らない)。閉じている = いつもの Enter (詳細検索を送る)
        if (S.open) { e.preventDefault(); if (S.sel >= 0) toggle(S.sel); }
        return;
      }
      if (e.key === 'Escape' && S.open) { e.preventDefault(); e.stopPropagation(); close(); return; }
      if (e.key === 'Tab') close();
    });
    list.addEventListener('mousedown', function (e) { e.preventDefault(); });   // 押しても欄から出ない (箱を閉じない)
    list.addEventListener('click', function (e) {
      var o = e.target.closest('.co[data-k]'); if (!o || composing) return;   // 変換中は選ばない (確定した字が後から来る)
      toggle(Number(o.getAttribute('data-k')));
      inp.focus();
    });
    root.addEventListener('focusout', function (e) { if (!root.contains(e.relatedTarget)) { composing = false; close(); } });
    drawChips();
  });
})();
