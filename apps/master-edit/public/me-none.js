/*
 * me-none.js — 新商品の登録の「入力なし」のチェック (中原さん 2026-10-13「必須でないところは入力なしのチェックボックスをつけるなどして全部そろわないと入力できない」)
 *   - 必須でない項目 = 値を入れる か「入力なし」のどちらか。どちらでもない項目があるうちは下書きの保存を押せない
 *     (数えるのは新商品・まとまりの画面の JS。サーバー = lib/master-register-blanks.mjs も同じ決まりで確かめる)
 *   - 値を入れると「入力なし」は自動で外れる (両方は持たない)。「入力なし」を押すと、その項目の値を空にする
 *   - 印: チェックの入った項目は札が光る (.nonechk.on)・行 (.f) は .isnone で欄を薄く
 *   - 欄の形: <input type="checkbox" data-none="KEY"> と、値の入れ物 [data-none-part="KEY"] (input・select・textarea・.seg・中に input を持つ箱)
 *   - 画面ごとの特別な項目 (まとまりの JAN = 全部の子に JAN があれば値あり) は ME.none.define(KEY, { has: fn, clear: false })
 *   - 値を JS で入れた (今あるまとまりから写した など) ときは、画面が ME.none.refresh() を呼ぶ = 値のある項目のチェックを外す
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var custom = {};
  var clearing = false;
  var sel = function (key) { return String(key).replace(/["\\]/g, ''); };
  function box(key) { return document.querySelector('input[type="checkbox"][data-none="' + sel(key) + '"]'); }
  function parts(key) { return $$('[data-none-part="' + sel(key) + '"]'); }
  function fields(el) {
    if (/^(INPUT|SELECT|TEXTAREA)$/.test(el.tagName)) return [el];
    return $$('input, select, textarea', el).filter(function (e) { return e.type !== 'checkbox' && e.type !== 'radio'; });
  }
  function partHas(el) {
    if (el.classList.contains('seg')) return !!el.getAttribute('data-value');
    return fields(el).some(function (e) { return String(e.value == null ? '' : e.value).trim() !== ''; });
  }
  /** この項目に値があるか */
  function has(key) {
    if (custom[key] && typeof custom[key].has === 'function') return !!custom[key].has();
    return parts(key).some(partHas);
  }
  /** 「入力なし」にしているか */
  function on(key) { var b = box(key); return !!b && b.checked; }
  function mark(key) {
    var b = box(key); if (!b) return;
    var lab = b.closest('.nonechk'); if (lab) lab.classList.toggle('on', b.checked);
    var row = b.closest('.f, .nonebar'); if (row) row.classList.toggle('isnone', b.checked);
  }
  function setOff(key) { var b = box(key); if (b && b.checked) { b.checked = false; mark(key); } }
  /** 値を空にする (「入力なし」を押したとき)。空にした欄に input / change を出す = 画面の数え直し */
  function clear(key) {
    if (custom[key] && custom[key].clear === false) return;
    clearing = true;
    try {
      parts(key).forEach(function (el) {
        if (el.classList.contains('seg')) {
          if (!el.getAttribute('data-value')) return;
          el.setAttribute('data-value', '');
          $$('button', el).forEach(function (bt) { bt.classList.remove('on'); bt.setAttribute('aria-pressed', 'false'); });
          el.dispatchEvent(new Event('change', { bubbles: true }));
          return;
        }
        fields(el).forEach(function (e) {
          if (e.value === '') return;
          e.value = '';
          e.dispatchEvent(new Event('input', { bubbles: true }));
          e.dispatchEvent(new Event('change', { bubbles: true }));
        });
      });
    } finally { clearing = false; }
  }
  /** 値を入れた = その項目の「入力なし」を外す (画面の数え直しより先 = capture で) */
  function onValue(e) {
    if (clearing) return;
    var t = e.target;
    if (!t || !t.closest || (t.matches && t.matches('input[data-none]'))) return;
    var p = t.closest('[data-none-part]');
    if (!p) return;
    var key = p.getAttribute('data-none-part');
    if (on(key) && has(key)) setOff(key);
  }
  /**
   * チェックを押した = 値を空にする。チェックの box は input → change の順に出る = input (先) で空にする
   * (画面の数え直しが input で走ると、値が残ったまま「値がある = チェックを外す」になるので、その前に)
   */
  function onBox(e) {
    var t = e.target;
    if (!t || !t.matches || !t.matches('input[type="checkbox"][data-none]')) return false;
    var key = t.getAttribute('data-none');
    if (t.checked) clear(key);
    mark(key);
    return true;
  }
  document.addEventListener('input', function (e) { if (!onBox(e)) onValue(e); }, true);
  document.addEventListener('change', function (e) { if (!onBox(e)) onValue(e); }, true);
  // まとまりの 1 つの欄が複数の項目の部品のとき (今は無い) にも、値のある項目のチェックは外す
  function refresh() {
    $$('input[type="checkbox"][data-none]').forEach(function (b) {
      var key = b.getAttribute('data-none');
      if (b.checked && has(key)) b.checked = false;
      mark(key);
    });
  }
  // 「空の欄を全部『入力なし』にする」(Yahoo!・ケース・容量 = ふだんは使わない欄だけに置く)。値のある欄・押せない欄はそのまま
  document.addEventListener('click', function (e) {
    var b = e.target && e.target.closest ? e.target.closest('[data-none-all]') : null;
    if (!b || b.disabled) return;
    var root = document.querySelector(b.getAttribute('data-none-all'));
    if (!root) return;
    $$('input[type="checkbox"][data-none]', root).forEach(function (c) {
      var key = c.getAttribute('data-none');
      if (c.disabled || c.checked || has(key)) return;
      c.checked = true;
      c.dispatchEvent(new Event('change', { bubbles: true }));
    });
  });
  ME.none = {
    on: on,
    has: has,
    /** 値か入力なしか (どちらでもない = false) */
    done: function (key) { return has(key) || on(key); },
    /** この画面にチェックがあり、押せるか (権限が無い・見るだけ = 数えない) */
    present: function (key) { var b = box(key); return !!b && !b.disabled; },
    define: function (key, spec) { custom[key] = spec || {}; },
    refresh: refresh,
    /** 入力なしにしている項目 (keys の中で・並びは keys のまま) */
    marked: function (keys) { return keys.filter(function (k) { return on(k) && !has(k); }); },
  };
  refresh();
})();
