/*
 * me-label.js — 商品の画面の「表示の項目」(0099・views/_label.ejs)
 *   「この値を入れる」= その値を欄に入れるだけ (原産国 / 内容量の数 + 単位 / メーカー型番)。
 *   保存は右の「保存する」(欄は data-dirty-field = 未保存に数える)。欄を変えたら input / change を送って画面 (me-sku.js) に数え直してもらう
 */
(function () {
  'use strict';
  var box = document.getElementById('p-label');
  if (!box) return;
  function put(id, v) {
    var el = document.getElementById(id);
    if (!el || el.disabled) return false;
    el.value = v;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
  }
  box.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('button[data-lbl-use]');
    if (!b || b.disabled) return;
    var kind = b.getAttribute('data-lbl-use');
    var v = b.getAttribute('data-v') || '';
    var ok = false;
    if (kind === 'country') ok = put('f-lbl_country', v);
    else if (kind === 'model') ok = put('f-lbl_model', v);
    else if (kind === 'net') ok = put('f-lbl_net', v) && put('f-lbl_net_uom', b.getAttribute('data-uom') || '');
    var M = window.MasterEdit;
    if (M && M.toast) M.toast(ok ? '欄に入れました (保存は「保存する」)' : '入れられませんでした (欄が無い・直せない)');
  });
})();
