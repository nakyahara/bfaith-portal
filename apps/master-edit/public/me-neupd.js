/*
 * me-neupd.js — 「NEへ送る」の画面の動き (views/ne-update.ejs・lib/master-ne-update.mjs・0094)
 *   - 「今すぐ NE へ送る」= POST /api/ne-update/send-now (頼みを積む = miniPC の送り手が数分で上げる)
 *   - 「CSV を作る (NE にアップする用)」= POST /api/ne-update/csv → 作ったファイルを 1 つずつ「ダウンロード」(POST /api/ne-update/files/:id/download・sha256 を照らしてから保存)
 *   - 通信が分からないまま終わった = 同じ request_id で送り直す (2 回作らない・2 回頼まない)
 *   - (名簿の人だけ) NE の実物の試しの結果 = POST /api/ne-update/verified
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var dataEl = document.getElementById('me-nu');
  if (!dataEl) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var busy = false;
  // 押した番号 (結果が分からないまま画面を開き直しても同じ番号で送り直す = 2 回作らない・頼まない)
  var KEY = 'me-neupd-pending';
  var pending = (function () { try { return JSON.parse(sessionStorage.getItem(KEY) || '{}') || {}; } catch (e) { return {}; } })();
  function keep() { try { sessionStorage.setItem(KEY, JSON.stringify(pending)); } catch (e) { /* 残せなくても動く */ } }
  function msg(t, cls, id) { var m = $(id || '#nu-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function post(url, body) {
    return fetch(url, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { return { r: r, j: j }; }); });
  }
  /** 結果がはっきりしたか (ok か、4xx の断り)。分からない = もう一度押せば同じ番号で送り直す */
  function sure(x) { return !!x.j && (x.r.ok ? x.j.ok === true : x.r.status >= 400 && x.r.status < 500); }
  function hex(buf) { return Array.prototype.map.call(new Uint8Array(buf), function (x) { return (x + 256).toString(16).slice(1); }).join(''); }

  var send = $('#nu-send');
  if (send) send.addEventListener('click', function () {
    if (busy) return;
    pending.send = pending.send || uuid(); keep();
    busy = true; send.disabled = true; msg('送るように頼んでいます…');
    post(P.base + '/api/ne-update/send-now', { request_id: pending.send }).then(function (x) {
      busy = false;
      if (!sure(x)) { send.disabled = false; msg('頼めたか分かりません。もう一度押してください (同じ頼みとして送り直します)', 'err'); return; }
      pending.send = null; keep();
      send.disabled = false;
      if (!x.r.ok || !x.j.ok) { msg(x.j.error || ('できませんでした (' + x.r.status + ')'), 'err'); return; }
      msg('NE へ送る頼みを受け付けました。数分のうちに送ります (NE に入った分は、翌朝 NE と同じになれば一覧から消えます)', 'ok');
    }).catch(function () { busy = false; send.disabled = false; msg('通信できませんでした。もう一度押してください (同じ頼みとして送り直します)', 'err'); });
  });

  function download(id, kind) {
    msg('ファイルを受け取っています…');
    fetch(P.base + '/api/ne-update/files/' + encodeURIComponent(id) + '/download', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(function (res) {
        if (!res.ok) return res.json().catch(function () { return {}; }).then(function (j) { throw new Error(j.error || ('受け取れませんでした (' + res.status + ')')); });
        var sha = res.headers.get('X-Content-SHA256') || '';
        var m = /filename="([^"]+)"/.exec(res.headers.get('Content-Disposition') || '');
        return res.arrayBuffer().then(function (buf) {
          var check = window.crypto && window.crypto.subtle ? window.crypto.subtle.digest('SHA-256', buf).then(hex) : Promise.resolve(sha);
          return check.then(function (got) {
            if (sha && got !== sha) throw new Error('受け取ったファイルが記録と違います (保存しません)。もう一度押してください');
            var a = document.createElement('a');
            a.href = URL.createObjectURL(new Blob([buf], { type: 'text/csv' }));
            a.download = m ? m[1] : ('ne-update-' + id + '.csv');
            document.body.appendChild(a); a.click(); a.remove();
            setTimeout(function () { URL.revokeObjectURL(a.href); }, 5000);
            msg('CSV を保存しました。NE の [商品] → [商品管理の一括登録] で' + (kind === 'sets' ? '「セット商品情報 CSV」' : '「商品 CSV」') + 'として取り込んでください', 'ok');
          });
        });
      })
      .catch(function (e) { msg(e.message || String(e), 'err'); });
  }

  var csv = $('#nu-csv');
  if (csv) csv.addEventListener('click', function () {
    if (busy) return;
    pending.csv = pending.csv || uuid(); keep();
    busy = true; csv.disabled = true; msg('作っています…');
    post(P.base + '/api/ne-update/csv', { request_id: pending.csv }).then(function (x) {
      busy = false;
      if (!sure(x)) { csv.disabled = false; msg('作れたか分かりません。もう一度押してください (同じ CSV として送り直します)', 'err'); return; }
      pending.csv = null; keep();
      csv.disabled = false;
      if (!x.r.ok || !x.j.ok) { msg(x.j.error || ('できませんでした (' + x.r.status + ')'), 'err'); return; }
      if (x.j.state !== 'built' || !x.j.files.length) { msg(x.j.reason || '送る所がありませんでした', 'err'); return; }
      var box = $('#nu-files');
      box.innerHTML = '';
      x.j.files.forEach(function (f, i) {
        var b = document.createElement('button');
        b.type = 'button'; b.className = 'btn sm'; b.style.marginRight = '8px';
        b.textContent = 'ダウンロード ' + (x.j.files.length > 1 ? (i + 1) + ' / ' + x.j.files.length + ' ' : '') + '(' + (f.kind === 'sets' ? 'セット' : '単品') + ' ' + f.line_count + ' 件)';
        b.addEventListener('click', function () { download(f.file_id, f.kind); });
        box.appendChild(b);
      });
      msg(x.j.files.length > 1 ? 'CSV を ' + x.j.files.length + ' つ作りました。1 つずつダウンロードして、順に NE に取り込んでください' + (x.j.left ? ' (残り ' + x.j.left + ' 件は次の回)' : '') : 'CSV を作りました。ダウンロードして NE に取り込んでください', 'ok');
      if (x.j.files.length === 1) download(x.j.files[0].file_id, x.j.files[0].kind);
    }).catch(function () { busy = false; csv.disabled = false; msg('通信できませんでした。もう一度押してください (同じ CSV として送り直します)', 'err'); });
  });

  Array.prototype.forEach.call(document.querySelectorAll('#nu-files [data-dl]'), function (b) {
    b.addEventListener('click', function () { download(b.getAttribute('data-dl'), b.getAttribute('data-kind')); });
  });

  var vs = $('#nu-v-save');
  if (vs) vs.addEventListener('click', function () {
    var t = ($('#nu-v-target').value || '').split(':');
    var body = { kind: t[0], col: t[1], entry: $('#nu-v-entry').value, result: $('#nu-v-result').value, note: $('#nu-v-note').value || null };
    if (!window.confirm('「' + $('#nu-v-target').selectedOptions[0].textContent + '」を ' + body.result + ' で残します。よろしいですか')) return;
    post(P.base + '/api/ne-update/verified', body).then(function (x) {
      if (!x.j || !x.r.ok || !x.j.ok) { msg((x.j && x.j.error) || ('できませんでした (' + x.r.status + ')'), 'err', '#nu-v-msg'); return; }
      msg('残しました', 'ok', '#nu-v-msg');
    }).catch(function () { msg('通信できませんでした', 'err', '#nu-v-msg'); });
  });
})();
