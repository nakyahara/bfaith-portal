/*
 * me-history.js — 変更の記録の画面 (商品・Amazon SKU) の「見るもの」の札 (第 2 段 10/5)
 *   すべて / 人の変更 / 夜間の取り込みなど / 原価 で、カード (保存 1 回 = 1 枚) を絞る。サーバーには聞かない (画面にある分だけ)
 */
(function () {
  'use strict';
  var bar = document.getElementById('hist-filter');
  var cards = document.getElementById('hist-cards');
  if (!bar || !cards) return;
  var btns = Array.prototype.slice.call(bar.querySelectorAll('button[data-hf]'));
  function apply(f) {
    btns.forEach(function (b) { b.setAttribute('aria-pressed', b.getAttribute('data-hf') === f ? 'true' : 'false'); });
    var shown = 0;
    Array.prototype.forEach.call(cards.querySelectorAll('.hcard'), function (c) {
      var on = !f || (f === 'cost' ? c.getAttribute('data-cost') === '1' : c.getAttribute('data-who') === f);
      c.hidden = !on; if (on) shown++;
    });
    // 日の見出し: その日のカードが 1 枚も見えなければ隠す
    var days = Array.prototype.slice.call(cards.querySelectorAll('[data-day]'));
    days.forEach(function (d) {
      var n = d.nextElementSibling, any = false;
      while (n && !n.hasAttribute('data-day')) { if (n.classList.contains('hcard') && !n.hidden) any = true; n = n.nextElementSibling; }
      d.hidden = !any;
    });
    var st = document.getElementById('hist-status'); if (st) st.textContent = shown + ' 枚を出しています';
  }
  bar.addEventListener('click', function (e) { var b = e.target.closest('button[data-hf]'); if (b && !b.disabled) apply(b.getAttribute('data-hf')); });
})();
