/*
 * me-amazon-new.js — 新しい seller SKU の対応を「自分で作る」入口の動き (10/11・一覧の上と /amazon/new の欄 = views/_amazon-create.ejs)
 *   - 打つとその場で確かめる (GET /api/amazon/check?sku= ・読むだけ): 形の誤り / もうある (開くへ) / 出品だけある / 似た SKU (もしかして) / 気をつけること / 新しく作れる
 *   - 「作り始める」: 今の欄の値の答えを画面に出した後なら checked に入れて送る (= /amazon/new は「もしかして」をもう一度出さずに作る画面へ)。
 *     答えが出る前に押した = checked なしで送る (/amazon/new が同じ確かめをして、似た SKU があればそこで見せる)
 *   - もうある (対応・墓標) のときは「作り始める」をその画面を開くボタンにする
 */
(function () {
  'use strict';
  var form = document.getElementById('create-form');
  if (!form) return;
  var input = document.getElementById('new-sku');
  var checkedEl = document.getElementById('new-sku-checked');
  var out = document.getElementById('new-sku-out');
  var go = document.getElementById('new-sku-go');
  var BASE = form.getAttribute('data-base') || '';
  var GO_HTML = go.innerHTML;
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function skuHref(s) { return BASE + '/amazon/sku?sku=' + encodeURIComponent(s); }
  var ICON = { ok: 'i-check', err: 'i-warn', warn: 'i-warn', info: 'i-info' };
  function line(cls, html) { return '<div class="fmsg ' + cls + '"><svg class="ic s" aria-hidden="true"><use href="#' + (ICON[cls] || 'i-info') + '"/></svg><span>' + html + '</span></div>'; }
  function badge(st) {
    if (st === 'active') return '<span class="b ok"><span class="dot" aria-hidden="true"></span>有効</span>';
    if (st === 'deleted') return '<span class="b mute">削除済み (墓標)</span>';
    return '<span class="b warn">出品だけ (対応なし)</span>';
  }
  var timer = null, seq = 0, shown = null;   // shown = 画面に答えを出した値 (欄の原文) と答え
  function setGo(ex) {
    if (ex) { go.innerHTML = '開く<svg class="ic s" aria-hidden="true"><use href="#i-chevr"/></svg>'; go.setAttribute('aria-label', ex + ' を開く (もうあります)'); }
    else { go.innerHTML = GO_HTML; go.removeAttribute('aria-label'); }
  }
  function render(raw, j) {
    var h = '';
    if (!j || j.ok !== true) {
      h = line('err', esc((j && (j.message || j.error)) || '確かめられません'));
      setGo(null);
    } else if (j.next === 'exists') {
      var ex = j.exists;
      h = line('warn', 'もうあります: <a class="linkchip" href="' + esc(skuHref(ex.seller_sku)) + '"><span class="mono">' + esc(ex.seller_sku) + '</span></a> ' + badge(ex.state) + ' <span class="muted">' + esc(ex.name || '') + '</span>' +
        (ex.same ? '' : ' <span class="muted">(全角・ダッシュ・空白のちがいを除くと同じ SKU)</span>') + (ex.state === 'deleted' ? ' <span class="muted">開いて保存すると、もう一度登録できます</span>' : ''));
      setGo(ex.seller_sku);
    } else {
      if (j.exists && j.exists.kind === 'listing') {
        h += line('ok', 'Amazon の出品はあります (対応はまだ): <span class="mono">' + esc(j.exists.seller_sku) + '</span>' + (j.exists.name ? ' <span class="muted">' + esc(j.exists.name) + '</span>' : '') + '。「作り始める」で対応を作れます');
      } else if (!j.similar.length && !j.warnings.length) {
        h += line('ok', '<span class="mono">' + esc(j.sku) + '</span> は、まだありません。「作り始める」で新しく作れます');
      } else {
        h += line('info', '<span class="mono">' + esc(j.sku) + '</span> は、まだありません');
      }
      if (j.similar && j.similar.length) {
        h += '<div class="lab2">もしかして (似た seller SKU' + (j.similar_total > j.similar.length ? ' ' + j.similar_total + ' 件のうち近い ' + j.similar.length + ' 件' : ' ' + j.similar.length + ' 件') + ')</div><ul class="simlist">';
        j.similar.forEach(function (s) {
          h += '<li><a class="linkchip" href="' + esc(skuHref(s.seller_sku)) + '"><span class="mono">' + esc(s.seller_sku) + '</span></a>' + badge(s.state) +
            '<span class="b mute">' + (s.kind === 'symbols' ? '記号だけ違う' : esc(s.distance) + ' 文字違い') + '</span><span class="nm">' + esc(s.name || '') + '</span></li>';
        });
        h += '</ul><div class="hint">打ち間違いでなければ、そのまま「作り始める」</div>';
      }
      if (j.warnings && j.warnings.length) {
        h += '<ul class="warnlist">' + j.warnings.map(function (w) { return '<li><svg class="ic s" aria-hidden="true"><use href="#i-warn"/></svg><span>' + esc(w) + '</span></li>'; }).join('') + '</ul>';
      }
      setGo(null);
    }
    out.innerHTML = h;
    shown = { raw: raw, j: j };
  }
  function check() {
    var raw = input.value;
    shown = null;
    if (!raw.trim()) { out.innerHTML = ''; setGo(null); return; }
    var my = ++seq;
    out.innerHTML = line('info', '確かめています…');
    fetch(BASE + '/api/amazon/check?sku=' + encodeURIComponent(raw), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (my !== seq || input.value !== raw) return;
        if (x.r.ok) render(raw, x.j);
        else { out.innerHTML = line('warn', 'いまは確かめられません (' + esc(x.j.error || x.r.status) + ')。「作り始める」を押すと、もう一度確かめます'); setGo(null); }
      })
      .catch(function () { if (my === seq) { out.innerHTML = line('warn', '確かめられません (通信)。「作り始める」を押すと、もう一度確かめます'); setGo(null); } });
  }
  input.addEventListener('input', function () { clearTimeout(timer); shown = null; checkedEl.value = ''; timer = setTimeout(check, 400); });
  form.addEventListener('submit', function (e) {
    var raw = input.value;
    if (!raw.trim()) { e.preventDefault(); input.focus(); return; }
    var j = shown && shown.raw === raw ? shown.j : null;
    if (j && j.ok === true && j.next === 'exists') { e.preventDefault(); location.href = skuHref(j.exists.seller_sku); return; }
    // 答えを見た後 = 似た SKU・気をつけることはもう見せた (サーバーはもう一度は止めない)。見る前 = サーバーが確かめて見せる
    checkedEl.value = j && j.ok === true ? j.sku : '';
    // /amazon/new が「作る前に見てください」を出した画面で、同じ値のまま押した = もう見た
    var srv = document.getElementById('new-result');
    if (!j && srv && srv.getAttribute('data-sku') && srv.getAttribute('data-raw') === raw) checkedEl.value = srv.getAttribute('data-sku');
  });
  // /amazon/new を開き直したとき (値が入っている) は、すぐ確かめる。サーバーが答えを出している画面ではくり返さない
  if (input.value.trim() && !document.getElementById('new-result')) check();
})();
