/*
 * me-package.js — 商品の画面の「大きさ・重さ」(0087・views/_package.ejs)
 *   「この値を入れる」= その出どころの値を欄に入れる (出どころが持っている軸だけ・持っていない軸はそのまま)。
 *   保存は右の「保存する」(欄は data-dirty-field = 未保存に数える)。欄を変えたら input を送って画面 (me-sku.js) に数え直してもらう
 */
(function () {
  'use strict';
  var el = document.getElementById('me-package');
  if (!el) return;
  var D; try { D = JSON.parse(el.textContent); } catch (e) { return; }
  if (!D || !D.editable) return;
  var FIELDS = { length: 'f-pkg_length', width: 'f-pkg_width', height: 'f-pkg_height', weight: 'f-pkg_weight' };
  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('button[data-pkg-use]');
    if (!b || b.disabled) return;
    var s = D.sources[Number(b.getAttribute('data-pkg-use'))];
    if (!s) return;
    var put = [];
    Object.keys(FIELDS).forEach(function (k) {
      var inp = document.getElementById(FIELDS[k]);
      if (!inp || inp.disabled || !s[k]) return;
      inp.value = s[k];
      inp.dispatchEvent(new Event('input', { bubbles: true }));
      put.push(k);
    });
    var M = window.MasterEdit;
    if (M && M.toast) M.toast(put.length ? s.words + ' の値を欄に入れました (保存は「保存する」)' : 'この出どころには入れられる値がありません');
  });
})();
