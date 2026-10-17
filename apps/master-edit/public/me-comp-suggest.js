/*
 * me-comp-suggest.js — 構成の行の「NE の商品」の欄の候補 (combobox) の部品 (10/17・未登録の一覧の「その場で登録」で使う)
 *   決まりは Amazon SKU の画面 (public/me-amazon.js の「欄で探す」) と同じ:
 *   - 打つと (min 文字から・250ms 待って・日本語入力の変換中も) GET /api/amazon/components?q= → 欄の下に候補 (コード・名前・単品 / セット / 例外・登録の状態・在庫)
 *   - ↑↓ と Enter・マウス・タッチで選ぶ。使えない商品 (サーバーが理由を付けた物・この構成のほかの行にもうある物) は灰色で選べない = 画面で選べる物は保存で断られない
 *   - ぴったりのコードが一番上で使えるときだけ、はじめから選んだ状態。欄の値が変わった後の古い候補は選べない (#1689 Codex R1 M1)
 *   - 日本語入力の確定の Enter (#1724・10/16): compositionend の後、次のキーを離すまでの 1 秒以内の Enter は確定の Enter と見て、箱も欄もそのまま
 *     (Windows の Chrome・Edge は確定の Enter の keydown が compositionend の後に keyCode 13・isComposing なしで来る)
 *   使い方: MasterEdit.compSuggest({ root, listbox, say, base, min, usedAt(input, code) → 行番号 (0 = 無い), onChoose(input, item) })
 *     root = 欄 (.c-code) を含む要素 (行が足されても効く = 委ねる)・listbox = 候補の箱 (root の外でよい。親を基準に置く)
 *   🚨 me-amazon.js は前からの同じ動きを持ったまま (この部品に置き換えるのは別の PR)
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var REG_LABEL = { draft: '下書き', ne_pending: 'NE登録待ち', ne_confirmed: 'NE確認済み', distributable: '配る対象', available: '登録済み', quarantined: '要確認', cancelled: 'やめた' };
  var REG_CLS = { draft: 'warn', ne_pending: 'warn', ne_confirmed: 'ok', distributable: 'ok', available: 'ok', quarantined: 'err', cancelled: 'mute' };
  // コードの同じ = lib/sku-norm.js normSku と同じ決まり (NFKC は掛けない = core.norm_code と同じ・#1689 Codex R1 M2)
  function normCode(v) {
    if (v == null) return '';
    return String(v).replace(/[！-～]/g, function (ch) { return String.fromCharCode(ch.charCodeAt(0) - 0xFEE0); }).replace(/　/g, ' ')
      .replace(/[−‐-―﹘﹣－]/g, '-').trim().replace(/\s+/g, '').toLowerCase();
  }
  // かなの同一視 (search-fold.mjs と同じ: NFKC・小文字・ひらがな → カタカナ)
  function fold(s) { return String(s == null ? '' : s).normalize('NFKC').toLowerCase().replace(/[ぁ-ゖゝゞ]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) + 0x60); }); }
  function mark(text, ranges) {
    if (!ranges.length) return esc(text);
    ranges.sort(function (a, b) { return a[0] - b[0]; });
    var out = '', at = 0;
    ranges.forEach(function (r) { if (r[0] < at) return; out += esc(text.slice(at, r[0])) + '<mark>' + esc(text.slice(r[0], r[1])) + '</mark>'; at = r[1]; });
    return out + esc(text.slice(at));
  }
  function hits(text, words, f) {
    var t = f(text); if (t.length !== text.length) return [];
    var rs = [];
    words.forEach(function (w) { var k = f(w); if (!k) return; var i = t.indexOf(k); if (i >= 0) rs.push([i, i + k.length]); });
    return rs;
  }
  ME.normCode = ME.normCode || normCode;
  ME.REG_LABEL = REG_LABEL; ME.REG_CLS = REG_CLS;

  ME.compSuggest = function (o) {
    var root = o.root, sug = o.listbox, say = o.say || null, BASE = o.base || '', MIN = o.min || 2;
    var idp = o.idPrefix || 'csx-opt-';
    var S = { input: null, items: [], sel: -1, seq: 0, timer: 0, ctl: null, q: '', forQ: null };
    function placeSug() {
      if (sug.hidden || !S.input) return;
      var host = sug.offsetParent || sug.parentNode, hr = host.getBoundingClientRect(), ir = S.input.getBoundingClientRect();
      var w = Math.min(560, hr.width);
      sug.style.width = w + 'px';
      sug.style.left = Math.max(0, Math.min(ir.left - hr.left, hr.width - w)) + 'px';
      var hdr = document.querySelector('.hdr'), top0 = hdr ? hdr.getBoundingClientRect().bottom : 0;
      var below = window.innerHeight - ir.bottom - 12, above = ir.top - top0 - 12;
      var up = below < 220 && above > below;
      sug.style.maxHeight = Math.max(120, Math.min(420, up ? above : below)) + 'px';
      sug.classList.toggle('up', up);
      sug.style.top = (up ? ir.top - hr.top - 4 - sug.offsetHeight : ir.bottom - hr.top + 4) + 'px';
    }
    function setSel(i) {
      S.sel = i;
      Array.prototype.forEach.call(sug.querySelectorAll('.co'), function (x, k) { x.classList.toggle('sel', k === i); x.setAttribute('aria-selected', k === i ? 'true' : 'false'); });
      if (S.input) { if (i >= 0) S.input.setAttribute('aria-activedescendant', idp + i); else S.input.removeAttribute('aria-activedescendant'); }
      var el = document.getElementById(idp + i); if (el && el.scrollIntoView) el.scrollIntoView({ block: 'nearest' });
    }
    function closeSug() {
      clearTimeout(S.timer); S.seq++;
      if (S.ctl) { try { S.ctl.abort(); } catch (e) { /* 返事を捨てるだけ */ } S.ctl = null; }
      if (S.input) { S.input.setAttribute('aria-expanded', 'false'); S.input.removeAttribute('aria-activedescendant'); }
      sug.hidden = true; sug.innerHTML = ''; sug.classList.remove('stale');
      S.items = []; S.sel = -1; S.input = null; S.forQ = null;
    }
    function showSug(html) {
      sug.innerHTML = html; sug.hidden = false;
      if (S.input) S.input.setAttribute('aria-expanded', 'true');
      placeSug();
    }
    function renderSug(j) {
      var words = S.q.split(/\s+/).filter(Boolean);
      S.forQ = S.q; sug.classList.remove('stale');
      S.items = (j.items || []).map(function (it) {
        var at = it.usable && o.usedAt ? o.usedAt(S.input, it.code) : 0;
        return { it: it, off: !it.usable || at > 0, why: !it.usable ? it.why : at ? 'この構成の ' + at + ' 行目にもうあります (同じ品は 1 行にして数で)' : '' };
      });
      if (!S.items.length) {
        showSug('<div class="cs-empty">「' + esc(S.q) + '」に当たる NE の商品はありません。コードの一部・名前の言葉 (空白で区切ると全部を含む)・JAN で探せます</div>');
        if (say) say.textContent = '候補はありません';
        S.sel = -1; return;
      }
      var stockNote = j.stock ? (j.stock.stale ? '<span class="warn">在庫 = ロジザードの写し (古い)</span>' : '<span>在庫 = ロジザードの写し</span>') : '';
      var h = '<div class="cs-h"><span>候補 ' + S.items.length + ' 件' + (j.more ? ' (上から ' + S.items.length + ' 件。もっと打つと絞れます)' : '') + '</span>' + stockNote + '</div>';
      S.items.forEach(function (x, i) {
        var it = x.it;
        h += '<div class="co' + (x.off ? ' off' : '') + '" role="option" id="' + idp + i + '" data-i="' + i + '" aria-selected="false"' + (x.off ? ' aria-disabled="true"' : '') + '>'
          + '<div class="l1"><span class="cc">' + mark(it.code, hits(it.code, [S.q.replace(/\s+/g, '')], normCode)) + '</span>'
          + '<span class="b ' + (it.kind === 'exception' ? 'err' : 'mute') + '">' + esc(it.kind_label || it.kind) + '</span>'
          + (it.reg_state ? '<span class="b ' + (REG_CLS[it.reg_state] || 'mute') + '">' + esc(REG_LABEL[it.reg_state] || it.reg_state) + '</span>' : '<span class="b mute">状態なし</span>')
          + (it.not_in_ne && !x.off ? '<span class="b warn">NE 未登録 (登録待ち)</span>' : '')
          + (it.ne_direct && !x.off ? '<span class="b info">要確認 (NE で直接作った商品)</span>' : '')
          + (it.handling === 'discontinued' ? '<span class="b warn">中止</span>' : '')
          + (it.stock != null ? '<span class="st">在庫 ' + Number(it.stock).toLocaleString('ja-JP') + '</span>' : '') + '</div>'
          + '<div class="cn">' + mark(it.name || '', hits(it.name || '', words, fold)) + '</div>'
          + (x.off ? '<div class="why"><svg class="ic s" aria-hidden="true"><use href="#i-lock"/></svg>使えません: ' + esc(x.why) + '</div>' : '')
          + '</div>';
      });
      showSug(h);
      var first = S.items[0];
      setSel(first && !first.off && normCode(first.it.code) === normCode(S.q) ? 0 : -1);
      if (say) say.textContent = '候補 ' + S.items.length + ' 件 (選べる ' + S.items.filter(function (x) { return !x.off; }).length + ' 件)。↓ で選んで Enter';
    }
    function searchNow(inp, q, my) {
      var ctl = window.AbortController ? new AbortController() : null;
      if (S.ctl) { try { S.ctl.abort(); } catch (e) { /* 前の問い合わせ */ } }
      S.ctl = ctl;
      fetch(BASE + '/api/amazon/components?q=' + encodeURIComponent(q), { headers: { Accept: 'application/json' }, signal: ctl ? ctl.signal : undefined })
        .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
        .then(function (x) {
          if (my !== S.seq || S.input !== inp || inp.value.trim() !== q || document.activeElement !== inp) return;
          if (!x.r.ok || !x.j.ok) { S.items = []; S.sel = -1; showSug('<div class="cs-empty bad">' + esc(x.j.error || '探せませんでした (HTTP ' + x.r.status + ')') + '</div>'); return; }
          renderSug(x.j);
        })
        .catch(function (err) {
          if (err && err.name === 'AbortError') return;
          if (my !== S.seq || S.input !== inp) return;
          S.items = []; S.sel = -1; showSug('<div class="cs-empty bad">つながりません。コードをそのまま打っても保存のときに確かめます</div>');
        });
    }
    function staleSug(inp) {
      if (S.input !== inp || inp.value.trim() === S.forQ) return;
      S.forQ = null;
      if (S.sel >= 0) setSel(-1);
      sug.classList.add('stale');
    }
    function queueSug(inp) {
      staleSug(inp);
      var q = inp.value.trim();
      if (S.input && S.input !== inp) closeSug();
      if (Array.from(q).length < MIN) { closeSug(); return; }
      S.input = inp; S.q = q;
      clearTimeout(S.timer);
      var my = ++S.seq;
      if (sug.hidden) { S.items = []; S.sel = -1; showSug('<div class="cs-empty">探しています…</div>'); }
      S.timer = setTimeout(function () { if (my === S.seq) searchNow(inp, q, my); }, 250);
    }
    function chooseSug(i) {
      var x = S.items[i], inp = S.input;
      if (!x || !inp) return;
      if (S.forQ == null || inp.value.trim() !== S.forQ) return;   // 古い候補は入れない
      if (x.off) { if (ME.toast) ME.toast('使えません: ' + x.why); return; }
      closeSug();
      inp.value = x.it.code;
      if (o.onChoose) o.onChoose(inp, x.it);
      if (say) say.textContent = x.it.code + ' を入れました';
    }
    function moveSel(d) {
      if (!S.items.length) return;
      var i = S.sel;
      for (var k = 0; k < S.items.length; k++) {
        i += d;
        if (i < 0) { setSel(-1); return; }
        if (i >= S.items.length) return;
        if (!S.items[i].off) { setSel(i); return; }
      }
    }
    var isCode = function (el) { return el && el.classList && el.classList.contains('c-code'); };
    var imeEnd = null, imeEndAt = 0;
    root.addEventListener('input', function (e) { if (isCode(e.target)) queueSug(e.target); });
    root.addEventListener('compositionend', function (e) {
      if (!isCode(e.target)) return;
      imeEnd = e.target; imeEndAt = e.timeStamp;
      if (S.input !== e.target || e.target.value.trim() !== S.q) queueSug(e.target);
    });
    root.addEventListener('keyup', function () { imeEnd = null; });
    root.addEventListener('focusout', function (e) { imeEnd = null; if (e.target === S.input) closeSug(); });
    /** 欄のキー。true = 候補の箱が使った (呼び手は今までの Enter の動きをしない) */
    function onKey(e) {
      var confirmed = imeEnd === e.target && e.timeStamp - imeEndAt < 1000; imeEnd = null;
      if (!isCode(e.target)) return false;
      if (confirmed && e.key === 'Enter') { e.preventDefault(); return true; }   // 日本語入力の確定の Enter
      if (e.isComposing || e.keyCode === 229) return true;
      var open = !sug.hidden && S.input === e.target;
      if (e.key === 'ArrowDown') { e.preventDefault(); if (!open) queueSug(e.target); else moveSel(1); return true; }
      if (e.key === 'ArrowUp' && open) { e.preventDefault(); moveSel(-1); return true; }
      if (e.key === 'Enter' && open && S.sel >= 0 && S.forQ != null && e.target.value.trim() === S.forQ) { e.preventDefault(); chooseSug(S.sel); return true; }
      if (e.key === 'Escape' && open) { e.preventDefault(); e.stopPropagation(); closeSug(); return true; }
      if (e.key === 'Enter' || e.key === 'Tab') closeSug();
      return false;
    }
    sug.addEventListener('mousedown', function (e) { e.preventDefault(); });
    sug.addEventListener('click', function (e) { var x = e.target.closest('.co'); if (x) chooseSug(Number(x.getAttribute('data-i'))); });
    window.addEventListener('resize', placeSug);
    window.addEventListener('scroll', placeSug, true);
    return { onKey: onKey, close: closeSug, place: placeSug };
  };
})();
