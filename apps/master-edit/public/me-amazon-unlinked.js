/*
 * me-amazon-unlinked.js — 「まだつないでいない Amazon の SKU」の動き (10/12・views/amazon-unlinked.ejs)
 *   - 「隠す」: 窓で理由 (バリエーションの親 / 使っていない・古い SKU / その他 = ひとこと必須) を選んで POST /api/amazon/unlinked/hide { hide: true }。
 *     通ったら行を薄くして外す (画面を読み直さない = 続けて隠せる)。隠すのは一覧の表示だけ (Amazon・対応には触らない)
 *   - 「元に戻す」(隠した SKU の一覧): POST { hide: false } → 行を外す
 *   - 押し直しても大丈夫 (同じ状態への操作 = same = 何も書かない)
 */
(function () {
  'use strict';
  var list = document.getElementById('unlinked-list');
  var cfgEl = document.getElementById('me-unlinked');
  if (!list || !cfgEl) return;
  var BASE = (JSON.parse(cfgEl.textContent || '{}').base) || '';
  var bg = document.getElementById('hide-bg');
  var go = document.getElementById('hide-go');
  var note = document.getElementById('hide-note');
  var hmsg = document.getElementById('hide-msg');
  var msg = document.getElementById('ul-msg');
  var total = document.getElementById('ul-total');
  var hiddenCount = document.getElementById('hidden-count');
  var target = null, opener = null, busy = false;
  function setMsg(el, text, kind) { if (!el) return; el.textContent = text || ''; el.className = 'msgline' + (kind ? ' ' + kind : ''); }
  function reason() { var r = bg.querySelector('input[name="hide-reason"]:checked'); return r ? r.value : ''; }
  function ready() { var r = reason(); go.disabled = busy || !r || (r === 'other' && !note.value.trim()); }
  function bump(el, d) { if (!el) return; var n = Number(String(el.textContent).replace(/[^0-9]/g, '')) || 0; el.textContent = Math.max(0, n + d).toLocaleString('ja-JP'); }
  function post(body) {
    return fetch(BASE + '/api/amazon/unlinked/hide', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); });
  }
  function rowOf(sku) {
    var rows = list.querySelectorAll('li.ul-row');
    for (var i = 0; i < rows.length; i++) if (rows[i].getAttribute('data-sku') === sku) return rows[i];
    return null;
  }
  function removeRow(li) {
    li.classList.add('gone');
    setTimeout(function () { if (li.parentNode) li.parentNode.removeChild(li); }, 220);
    bump(total, -1);
  }
  function openHide(btn) {
    target = btn.getAttribute('data-sku'); opener = btn;
    document.getElementById('hide-sku').textContent = target;
    document.getElementById('hide-name').textContent = btn.getAttribute('data-name') || '';
    var checked = bg.querySelector('input[name="hide-reason"]:checked'); if (checked) checked.checked = false;
    note.value = ''; setMsg(hmsg, ''); busy = false; ready();
    bg.classList.add('on');
    var first = bg.querySelector('input[name="hide-reason"]'); if (first) first.focus();
  }
  function closeHide() { bg.classList.remove('on'); target = null; if (opener) { opener.focus(); opener = null; } }
  list.addEventListener('click', function (ev) {
    var btn = ev.target.closest('button[data-act]');
    if (!btn) return;
    ev.preventDefault();
    if (btn.getAttribute('data-act') === 'hide') { openHide(btn); return; }
    // 元に戻す
    var sku = btn.getAttribute('data-sku');
    btn.disabled = true; setMsg(msg, sku + ' を一覧に戻しています…');
    post({ seller_sku: sku, hide: false }).then(function (x) {
      if (x.r.ok && x.j.ok) { var li = rowOf(sku); if (li) removeRow(li); bump(hiddenCount, x.j.status === 'shown' ? -1 : 0); setMsg(msg, sku + ' を一覧に戻しました (「まだつないでいない SKU」に出ます)', 'ok'); }
      else { btn.disabled = false; setMsg(msg, (x.j && (x.j.error || x.j.message)) || '戻せませんでした (HTTP ' + x.r.status + ')', 'err'); }
    }).catch(function () { btn.disabled = false; setMsg(msg, '通信が切れました。何も変わっていなければ、もう一度押してください', 'err'); });
  });
  bg.addEventListener('change', ready);
  note.addEventListener('input', ready);
  document.getElementById('hide-stay').addEventListener('click', closeHide);
  bg.addEventListener('click', function (ev) { if (ev.target === bg) closeHide(); });
  document.addEventListener('keydown', function (ev) { if (ev.key === 'Escape' && bg.classList.contains('on') && !busy) closeHide(); });
  go.addEventListener('click', function () {
    if (!target || go.disabled) return;
    var sku = target;
    busy = true; ready(); setMsg(hmsg, '隠しています…');
    post({ seller_sku: sku, hide: true, reason: reason(), note: note.value.trim() || null }).then(function (x) {
      busy = false;
      if (x.r.ok && x.j.ok) {
        var li = rowOf(sku); if (li) removeRow(li);
        if (x.j.status === 'hidden') bump(hiddenCount, 1);
        opener = null; closeHide();
        setMsg(msg, sku + (x.j.status === 'same' ? ' はもう隠してあります' : ' を隠しました (「隠した SKU」から元に戻せます)'), 'ok');
      } else { ready(); setMsg(hmsg, (x.j && (x.j.error || x.j.message)) || '隠せませんでした (HTTP ' + x.r.status + ')', 'err'); }
    }).catch(function () { busy = false; ready(); setMsg(hmsg, '通信が切れました。もう一度押してください (2 回入ることはありません)', 'err'); });
  });
})();
