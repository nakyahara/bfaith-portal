/*
 * me-prevnext.js — 1 つの商品の画面の「← 前へ」「次へ →」(10/17 中原さん)
 *   - 来た一覧 = 最後に見た一覧 (me-shell.js が sessionStorage に覚えた URL・パンくず「商品・セット」の戻り先と同じ) の条件のまま、
 *     サーバー (api/neighbors) にこの商品の前・次と場所 (12 / 340) を聞く。並び・絞り込みは一覧と同じ (サーバーが同じ listSkus で決める)
 *   - 前へ・次へのリンクは /sku/<コード> だけ = 覚えた一覧は変えない → 続けて「次へ」を押しても同じ一覧の中を進む
 *   - 未保存のまま押した = me-shell.js のリンクの見張りが「離れるときの確認」を出す (ここでは何もしない)
 *   - 一覧から来ていない (覚えた一覧が無い)・期限切れ (410)・件数が多すぎる (413)・読めない = 出さない (パンくずで一覧へ戻れる)
 *   - 一覧に無い (直して絞り込みに合わなくなった など) = 場所は「一覧に無い」・前へ・次へは押せない
 *   - パンくずの戻り先を、この商品が載る一覧のページ (offset) に直す (次へで次のページへ進んだ後も、戻るとその商品が見える)
 *   - キー (Alt + ← / →) は付けない: Windows の Chrome・Edge では「戻る・進む」なので重なる
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  var box = document.getElementById('sku-pn');
  var back = document.querySelector('a[data-list-back]');
  if (!box || !back || !window.fetch || !window.URL || !window.URLSearchParams || !ME.lastList) return;
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var basePath = back.pathname;               // 一覧の path (…/master-edit/)
  var list = ME.lastList(basePath);
  if (!list) return;                          // 一覧から来ていない = 出さない
  var code = box.getAttribute('data-code') || '';
  if (!code) return;
  var listUrl = new URL(list, location.href);
  var qs = new URLSearchParams(listUrl.search);
  qs.delete('offset');                        // ページ分けは見ない (一覧の全件の中の場所)
  qs.set('cur', code);
  var api = new URL('api/neighbors?' + qs.toString(), new URL(basePath, location.href)).href;
  var skuHref = function (c) { return basePath + 'sku/' + encodeURIComponent(c); };
  var num = function (n) { return Number(n).toLocaleString('ja-JP'); };

  function link(dir, c) {
    var label = dir === 'prev' ? '← 前へ' : '次へ →';
    var what = dir === 'prev' ? '前の商品' : '次の商品';
    if (!c) return '<button type="button" class="btn sm" id="pn-' + dir + '" disabled title="' + (dir === 'prev' ? '一覧の最初の商品です' : '一覧の最後の商品です') + '">' + label + '</button>';
    return '<a class="btn sm" id="pn-' + dir + '" rel="' + dir + '" href="' + esc(skuHref(c)) + '" title="' + what + ' ' + esc(c) + '" aria-label="' + what + ' (' + esc(c) + ') へ">' + label + '</a>';
  }
  function render(j) {
    var pos = j.pos
      ? '<span class="pn-pos" id="pn-pos" title="来た一覧の中の場所">' + num(j.pos) + ' / ' + num(j.total) + '</span>'
      : '<span class="pn-pos out" id="pn-pos" title="この商品は来た一覧の条件に入っていません (直して条件に合わなくなった など)。「商品・セット」で一覧へ戻れます">一覧に無い (全 ' + num(j.total) + ' 件)</span>';
    box.innerHTML = link('prev', j.pos ? j.prev : null) + pos + link('next', j.pos ? j.next : null);
    box.hidden = false;
    // パンくずの戻り先 = この商品が載る一覧のページ
    if (j.pos && j.offset != null) {
      try {
        var b = new URL(back.getAttribute('href'), location.href);
        if (j.offset > 0) b.searchParams.set('offset', String(j.offset)); else b.searchParams.delete('offset');
        back.setAttribute('href', b.pathname + b.search);
      } catch (err) { /* そのまま */ }
    }
  }
  fetch(api, { headers: { Accept: 'application/json' }, credentials: 'same-origin' })
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (j) { if (j && j.ok && typeof j.total === 'number') render(j); })
    .catch(function () { /* 読めない = 出さないだけ */ });
})();
