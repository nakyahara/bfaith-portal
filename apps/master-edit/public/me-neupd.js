/*
 * me-neupd.js — 「NEへ送る」の画面の動き (views/ne-update.ejs・lib/master-ne-update.mjs・0094)
 *   - 「CSV を作る (NE にアップする用)」= POST /api/ne-update/csv → 作ったファイルを 1 つずつ「ダウンロード」(POST /api/ne-update/files/:id/download・sha256 を照らしてから保存)
 *   - 「今すぐ NE へ送る」(API の口が開いているときだけ) = POST /api/ne-update/send-now (頼みを積む = miniPC の送り手が数分で上げる)
 *   - 押している間 (作る・受け取る・頼む) は全部のボタンを止める (二重に押さない)。通信は 30 秒で打ち切る
 *   - 通信が分からないまま終わった = 同じ request_id で送り直す (2 回作らない・2 回頼まない)
 *   - 受け取りを DB が断った (作った後に値が変わった・もう出した) = そのボタンを外して「もう一度 CSV を作る」を案内
 *   - (名簿の人だけ) NE の実物の試しの結果 = POST /api/ne-update/verified
 *   - 🆕 0101: 「セットの中身の CSV を作る」= POST /api/ne-update/set-comp-csv (その場で作って渡す・sha256 を照らしてから保存・DB に記録しない = 何度押しても同じ中身)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var dataEl = document.getElementById('me-nu');
  if (!dataEl) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var TIMEOUT_MS = 30000;
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
  /** 押している間は全部のボタンを止める */
  function lock(on) {
    busy = on;
    Array.prototype.forEach.call(document.querySelectorAll('#nu-csv, #nu-send, #nu-files button'), function (b) {
      if (on) { b.setAttribute('data-was', b.disabled ? '1' : ''); b.disabled = true; } else { b.disabled = b.getAttribute('data-was') === '1'; b.removeAttribute('data-was'); }
    });
  }
  function timed(url, opts) {
    var ac = window.AbortController ? new AbortController() : null;
    var t = ac ? setTimeout(function () { ac.abort(); }, TIMEOUT_MS) : null;
    if (ac) opts.signal = ac.signal;
    return fetch(url, opts).finally(function () { if (t) clearTimeout(t); });
  }
  function post(url, body) {
    return timed(url, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { return { r: r, j: j }; }); });
  }
  /** 結果がはっきりしたか (ok か、4xx の断り)。分からない = もう一度押せば同じ番号で送り直す */
  function sure(x) { return !!x.j && (x.r.ok ? x.j.ok === true : x.r.status >= 400 && x.r.status < 500); }
  function hex(buf) { return Array.prototype.map.call(new Uint8Array(buf), function (x) { return (x + 256).toString(16).slice(1); }).join(''); }

  var send = $('#nu-send');
  if (send) send.addEventListener('click', function () {
    if (busy) return;
    pending.send = pending.send || uuid(); keep();
    lock(true); msg('NE へ送る頼みを出しています…');
    post(P.base + '/api/ne-update/send-now', { request_id: pending.send }).then(function (x) {
      lock(false);
      if (!sure(x)) { msg('頼めたか分かりません。もう一度押してください (同じ頼みとして送り直します)', 'err'); return; }
      pending.send = null; keep();
      if (!x.r.ok || !x.j.ok) { msg(x.j.error || ('できませんでした (' + x.r.status + ')'), 'err'); return; }
      msg(x.j.state === 'waiting'
        ? 'NE へ送る頼みを受け付けました。数分のうちに送ります (NE に入った分は、翌朝 NE と同じになれば一覧から消えます)'
        : 'この頼みはもう済んでいます。画面を開き直すと、いまの様子が分かります', 'ok');
    }).catch(function () { lock(false); msg('通信できませんでした。もう一度押してください (同じ頼みとして送り直します)', 'err'); });
  });

  function download(id, kind, btn) {
    if (busy) return;
    lock(true); msg('CSV を受け取っています…');
    timed(P.base + '/api/ne-update/files/' + encodeURIComponent(id) + '/download', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(function (res) {
        if (!res.ok) {
          return res.json().catch(function () { return {}; }).then(function (j) {
            var e = new Error(j.error || ('受け取れませんでした (' + res.status + ')'));
            e.stale = res.status === 409;
            throw e;
          });
        }
        var sha = res.headers.get('X-Content-SHA256') || '';
        var m = /filename="([^"]+)"/.exec(res.headers.get('Content-Disposition') || '');
        return res.arrayBuffer().then(function (buf) {
          var check = window.crypto && window.crypto.subtle ? window.crypto.subtle.digest('SHA-256', buf).then(hex) : Promise.resolve(sha);
          return check.then(function (got) {
            if (sha && got !== sha) throw new Error('受け取った CSV が記録と違います (保存しません)。もう一度押してください');
            var a = document.createElement('a');
            a.href = URL.createObjectURL(new Blob([buf], { type: 'text/csv' }));
            a.download = m ? m[1] : 'ne-update.csv';
            document.body.appendChild(a); a.click(); a.remove();
            setTimeout(function () { URL.revokeObjectURL(a.href); }, 5000);
            lock(false);
            // 🆕 0096: 前に受け取った CSV が、この CSV の直しを古い値で書いている = その CSV を後に取り込むと戻る
            var older = Number(res.headers.get('X-NE-Older') || 0) > 0;
            msg('CSV を保存しました。NE の [商品] → [商品管理の一括登録] で' + (kind === 'sets' ? '「セット商品情報 CSV」' : '「商品 CSV」') + 'として取り込んでください'
              + (older ? '。前に受け取った CSV をまだ取り込んでいなければ、それは取り込まないでください (この CSV の直しが古い値に戻ります)' : ''), older ? 'err' : 'ok');
          });
        });
      })
      .catch(function (e) {
        lock(false);
        // DB が断った (作った後に値が変わった・ほかの CSV でもう出した・前の日の CSV) = このボタンはもう使えない → 外して作り直しを案内
        if (e && e.stale && btn && btn.parentNode) btn.parentNode.removeChild(btn);
        msg(e && e.name === 'AbortError' ? '時間がかかりすぎました。もう一度押してください' : (e.message || String(e)), 'err');
      });
  }

  var csv = $('#nu-csv');
  if (csv) csv.addEventListener('click', function () {
    if (busy) return;
    pending.csv = pending.csv || uuid(); keep();
    lock(true); msg('CSV を作っています…');
    post(P.base + '/api/ne-update/csv', { request_id: pending.csv }).then(function (x) {
      lock(false);
      if (!sure(x)) { msg('作れたか分かりません。もう一度押してください (同じ CSV として送り直します)', 'err'); return; }
      pending.csv = null; keep();
      if (!x.r.ok || !x.j.ok) { msg(x.j.error || ('できませんでした (' + x.r.status + ')'), 'err'); return; }
      // 押し直しで前の答え (もう受け取った = sent) が返っても、ファイルがあれば出す
      if (!x.j.files || !x.j.files.length) { msg(x.j.reason || 'NE とちがう所で、いま CSV にできるものはありませんでした', 'err'); return; }
      var box = $('#nu-files');
      box.innerHTML = '';
      var first = null;
      x.j.files.forEach(function (f, i) {
        var b = document.createElement('button');
        b.type = 'button'; b.className = 'btn sm'; b.style.marginRight = '8px';
        // 🆕 0096: 「単品の CSV」「セットの CSV」(名前はサーバーが付ける = lib/master-ne-update.mjs の labelFiles)
        b.textContent = 'ダウンロード: ' + (f.label || (f.kind === 'sets' ? 'セットの CSV' : '単品の CSV')) + ' (' + f.line_count + ' 件)';
        b.addEventListener('click', function () { download(f.file_id, f.kind, b); });
        box.appendChild(b);
        if (!first) first = { f: f, b: b };
      });
      var apart = x.j.apart > 0 ? ' (' + x.j.apart + ' 件は NE の今の値が分からない欄があるので、別の CSV にしました)' : '';
      msg((x.j.files.length > 1 ? 'CSV を ' + x.j.files.length + ' つ作りました。1 つずつダウンロードして NE に取り込んでください' : 'CSV を作りました。NE に取り込んでください') + apart, 'ok');
      if (x.j.files.length === 1) download(first.f.file_id, first.f.kind, first.b);
    }).catch(function () { lock(false); msg('通信できませんでした。もう一度押してください (同じ CSV として送り直します)', 'err'); });
  });

  Array.prototype.forEach.call(document.querySelectorAll('#nu-files [data-dl]'), function (b) {
    b.addEventListener('click', function () { download(b.getAttribute('data-dl'), b.getAttribute('data-kind'), b); });
  });

  var sc = $('#nu-sc-csv');
  if (sc) sc.addEventListener('click', function () {
    if (busy) return;
    lock(true); sc.disabled = true; msg('セットの中身の CSV を作っています…', '', '#nu-sc-msg');
    var done = function () { lock(false); sc.disabled = false; };
    timed(P.base + '/api/ne-update/set-comp-csv', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(function (res) {
        if (!res.ok) return res.json().catch(function () { return {}; }).then(function (j) { throw new Error(j.error || ('作れませんでした (' + res.status + ')')); });
        var sha = res.headers.get('X-Content-SHA256') || '';
        var m = /filename="([^"]+)"/.exec(res.headers.get('Content-Disposition') || '');
        var n = res.headers.get('X-Set-Count') || '';
        return res.arrayBuffer().then(function (buf) {
          var check = window.crypto && window.crypto.subtle ? window.crypto.subtle.digest('SHA-256', buf).then(hex) : Promise.resolve(sha);
          return check.then(function (got) {
            if (sha && got !== sha) throw new Error('受け取った CSV が壊れています (保存しません)。もう一度押してください');
            var a = document.createElement('a');
            a.href = URL.createObjectURL(new Blob([buf], { type: 'text/csv' }));
            a.download = m ? m[1] : 'ne_set_components.csv';
            document.body.appendChild(a); a.click(); a.remove();
            setTimeout(function () { URL.revokeObjectURL(a.href); }, 5000);
            done();
            msg('セットの中身の CSV を保存しました (' + n + ' セット)。NE の [商品] → [商品管理の一括登録] で「セット商品情報 CSV」として取り込んでください (前に作ったセットの中身の CSV は捨ててください = 取り込むと前の中身に戻ります)。翌朝 NE とそろえば、その夜に一覧から消えます', 'ok', '#nu-sc-msg');
          });
        });
      })
      .catch(function (e) { done(); msg(e && e.name === 'AbortError' ? '時間がかかりすぎました。もう一度押してください' : (e.message || String(e)), 'err', '#nu-sc-msg'); });
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
