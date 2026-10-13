/*
 * me-lzcsv.js — 「ロジザード登録の CSV」の画面の動き (views/lz-csv.ejs・lib/lz-new-export.mjs・0081)
 *   - 「N 件の CSV を作ってダウンロード」= POST /api/lz-csv/build (画面で見た商品と行番号)。作れたらファイルをダウンロードして画面を読み直す
 *   - 写しが古い = 作れない (画面がボタンを止める・DB も断る = #1721 R2)
 *   - 通信が分からないまま終わった = 同じ request_id で送り直す (2 回作らない)
 *   - 「取り消す」= 理由を聞いて POST /api/lz-csv/:id/void
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var dataEl = document.getElementById('me-lzc');
  if (!dataEl) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var busy = false, pending = null;
  function msg(t, cls) { var m = $('#lzc-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function post(url, body) {
    return fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { return { r: r, j: j }; }); });
  }
  var btn = $('#lzc-build');
  if (btn) btn.addEventListener('click', function () {
    if (busy || !P.ready.length) return;
    if (P.stale) { msg('ロジザードの商品の一覧の写しが古いので作れません (毎晩の取り込みの後に作るか、予備の GAS で)', 'err'); return; }
    var items = P.ready.map(function (x) { return { sku_id: x.sku_id, rule_row: x.rule_row }; });
    var key = JSON.stringify(items);
    if (!pending || pending.key !== key) pending = { key: key, rid: uuid() };
    busy = true; btn.disabled = true; msg('作っています…');
    post(P.base + '/api/lz-csv/build', { request_id: pending.rid, items: items }).then(function (x) {
      busy = false;
      var sure = !!x.j && (x.r.ok ? x.j.ok === true : x.r.status >= 400 && x.r.status < 500);
      if (!sure) { btn.disabled = false; msg('作れたか分かりません (HTTP ' + x.r.status + ')。もう一度押してください (同じ CSV として送り直します)', 'err'); return; }
      pending = null;
      if (!x.r.ok || !x.j.ok) { btn.disabled = false; msg(x.j.error || ('HTTP ' + x.r.status), 'err'); return; }
      msg('CSV #' + x.j.export_id + ' (' + x.j.rows + ' 件) を作りました。ダウンロードします', 'ok');
      var a = document.createElement('a');
      a.href = P.base + '/lz-csv/' + encodeURIComponent(x.j.export_id) + '/file'; a.download = 'logizard_bc_upload.csv';
      document.body.appendChild(a); a.click(); a.remove();
      setTimeout(function () { location.reload(); }, 1500);
    }).catch(function () { busy = false; btn.disabled = false; msg('通信できませんでした。作れたか分からないので、もう一度押してください (同じ CSV として送り直します)', 'err'); });
  });
  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('button[data-void]');
    if (!b || busy) return;
    var id = b.getAttribute('data-void');
    var reason = window.prompt('CSV #' + id + ' を取り消します。理由を書いてください (例: ロジザードで取り込めなかった)。取り消すと、この中の商品はもう一度出せます', '');
    if (reason == null) return;
    if (!reason.trim()) { msg('理由が要ります', 'err'); return; }
    busy = true;
    post(P.base + '/api/lz-csv/' + encodeURIComponent(id) + '/void', { reason: reason.trim() }).then(function (x) {
      busy = false;
      if (!x.j || !x.r.ok || !x.j.ok) { msg((x.j && x.j.error) || ('HTTP ' + x.r.status), 'err'); return; }
      location.reload();
    }).catch(function () { busy = false; msg('通信できませんでした。画面を開き直して確かめてください', 'err'); });
  });
})();
