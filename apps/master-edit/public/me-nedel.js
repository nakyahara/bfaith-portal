/*
 * me-nedel.js — 「NE から消す」の画面 (🆕 0072・lib/master-ne-delete.mjs・views/ne-delete.ejs)
 *   - 確かめる: POST /api/ne-delete/check { codes } → 1 行ずつ「消せる / 止まる理由 / 気をつけること / 在庫」(何も変えない)
 *   - 全部「消せる」+ 理由 → 確かめの窓 → POST /api/ne-delete/build → できたファイル (セット → 単品) をダウンロード
 *   - ダウンロード = POST /api/ne-delete/exports/:id/download (受け取った日時を DB が残す = GET にしない) → バイト列の sha256 を記録と照らしてから保存
 *   - 取り下げ (理由) / 断られた単品を選んで「取扱中止」の CSV
 *   - 押し直し (通信が切れた) = 同じ request_id (DB は同じ番号の 2 回目に前の答えを返す)。中身を変えた押し直し = 新しい番号
 *   🚨 画面に出す文字は textContent だけ (DB・入力の文字を HTML にしない)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var root = document.getElementById('nd');
  if (!root) return;
  var base = root.getAttribute('data-base') || '';
  var can = root.getAttribute('data-can') === '1';
  var newId = function () {
    if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
    var b = new Uint8Array(16); window.crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return (x + 256).toString(16).slice(1); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  };
  function el(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
  function say(node, text, bad) { if (!node) return; node.textContent = text || ''; node.classList.toggle('err', !!bad); node.classList.toggle('ok', !!text && !bad); }
  function post(path, body) {
    return fetch(base + path, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) })
      .then(function (res) { return res.json().catch(function () { return { ok: false, error: '答えを読めませんでした (' + res.status + ')' }; }).then(function (j) { return { status: res.status, j: j }; }); });
  }
  function errText(x) { return (x.j && x.j.error) || ('できませんでした (' + x.status + ')'); }
  function modal(bg, on) { bg.classList.toggle('on', !!on); }
  function hex(buf) { return Array.prototype.map.call(new Uint8Array(buf), function (x) { return (x + 256).toString(16).slice(1); }).join(''); }

  // ─── ダウンロード (POST・バイト列の sha256 を照らしてから保存) ───
  function download(id, msg) {
    say(msg, 'ファイル #' + id + ' を受け取っています…');
    fetch(base + '/api/ne-delete/exports/' + encodeURIComponent(id) + '/download', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(function (res) {
        if (!res.ok) return res.json().catch(function () { return {}; }).then(function (j) { throw new Error(j.error || ('受け取れませんでした (' + res.status + ')')); });
        var sha = res.headers.get('X-Content-SHA256') || '';
        var cd = res.headers.get('Content-Disposition') || '';
        var m = /filename="([^"]+)"/.exec(cd);
        return res.arrayBuffer().then(function (buf) {
          var check = window.crypto && window.crypto.subtle ? window.crypto.subtle.digest('SHA-256', buf).then(hex) : Promise.resolve(sha);
          return check.then(function (got) {
            if (sha && got !== sha) throw new Error('受け取ったファイルが記録と違います (保存しません)。もう一度押してください');
            var a = document.createElement('a');
            a.href = URL.createObjectURL(new Blob([buf], { type: 'text/csv' }));
            a.download = m ? m[1] : ('ne-delete-' + id + '.csv');
            document.body.appendChild(a); a.click(); a.remove();
            setTimeout(function () { URL.revokeObjectURL(a.href); }, 5000);
            say(msg, 'ファイル #' + id + ' を保存しました (' + a.download + ')。NE に取り込んでください');
          });
        });
      })
      .catch(function (e) { say(msg, e.message || String(e), true); });
  }
  $$('[data-dl]').forEach(function (b) { b.addEventListener('click', function () { download(b.getAttribute('data-dl'), $('#nl-msg')); }); });

  // ─── 確かめる → 作る ───
  var codesIn = $('#nd-codes'), checkBtn = $('#nd-check'), results = $('#nd-results'), tbody = $('#nd-table tbody'), goBox = $('#nd-go-box');
  var reason = $('#nd-reason'), buildBtn = $('#nd-build'), msg = $('#nd-msg'), built = $('#nd-built');
  var checked = null, buildId = newId(), sentKey = null, busy = false;
  function codesOf() { return (codesIn.value || '').split(/[\s,、]+/).map(function (s) { return s.trim(); }).filter(Boolean); }
  function buildState() {
    var ok = !!checked && checked.all_ok && checked.rows.length > 0;
    buildBtn.disabled = busy || !can || !ok || !reason.value.trim();
    $('#nd-n').textContent = checked ? String(checked.rows.length) : '0';
  }
  function stockText(r) {
    if (!r.stock) return 'セット (在庫は中身の単品)';
    var d = r.stock.db || {}, a = r.stock.app || {};
    var parts = [];
    parts.push('日次: NE ' + (d.ne_qty == null ? '?' : d.ne_qty) + (d.ne_day ? ' (' + d.ne_day + ')' : '') + ' · ロジザード ' + (d.logizard_qty == null ? '?' : d.logizard_qty));
    if (a.unreadable) parts.push('今: 読めない');
    else parts.push('今: NE ' + a.ne_own + ' · 引当 ' + a.ne_allocated + ' · NE 発注残 ' + a.ne_on_order + ' · FBA ' + a.fba + ' · ロジザード ' + a.logizard + ' (引当 ' + a.logizard_allocated + ') · 注文残 ' + (a.po_backorder_hidden ? '— (発注アプリの権限が要る)' : a.po_backorder));
    return parts.join(' / ');
  }
  function render(data) {
    tbody.textContent = '';
    (data.missing || []).forEach(function (c) {
      var tr = el('tr', 'row-del');
      tr.appendChild(el('td', 'code mono', c)); tr.appendChild(el('td', null, '—'));
      var td = el('td'); var d = el('div', 'why-blk'); d.appendChild(el('span', null, 'Company DB にこのコードの商品がありません')); td.appendChild(d); tr.appendChild(td);
      tr.appendChild(el('td', null, '')); tbody.appendChild(tr);
    });
    (data.rows || []).forEach(function (r) {
      var tr = el('tr', r.ok ? '' : 'row-del');
      var tdc = el('td', 'code'); var a = el('a', null, r.code || r.sku_id); a.href = base + '/sku/' + encodeURIComponent(r.code || ''); tdc.appendChild(a);
      if (r.ne_code && r.ne_code !== r.code) tdc.appendChild(el('span', 'sub', 'NE: ' + r.ne_code));
      tr.appendChild(tdc);
      var tdk = el('td'); tdk.appendChild(el('span', 'b ' + (r.sku_kind === 'set' ? 'vio' : 'mute'), r.sku_kind === 'set' ? 'セット' : r.sku_kind === 'single' ? '単品' : String(r.sku_kind || ''))); tr.appendChild(tdk);
      var tdw = el('td');
      if (r.ok) tdw.appendChild(el('span', 'why-ok', '消せます'));
      (r.problems || []).forEach(function (p) { var d = el('div', 'why-blk'); var s = el('span'); s.appendChild(el('b', null, p.word + ': ')); s.appendChild(document.createTextNode(p.text)); d.appendChild(s); tdw.appendChild(d); });
      (r.warnings || []).forEach(function (w) { var d = el('div', 'why-wrn'); var s = el('span'); s.appendChild(el('b', null, w.word + ': ')); s.appendChild(document.createTextNode(w.text)); d.appendChild(s); tdw.appendChild(d); });
      tr.appendChild(tdw);
      tr.appendChild(el('td', 'hint', stockText(r)));
      tbody.appendChild(tr);
    });
    results.hidden = false;
    goBox.hidden = !(data.all_ok && data.rows.length);
  }
  if (checkBtn) checkBtn.addEventListener('click', function () {
    var codes = codesOf();
    if (!codes.length) { say(msg, '商品コードを入れてください', true); return; }
    checkBtn.disabled = true; say(msg, '確かめています…'); built.hidden = true;
    post('/api/ne-delete/check', { codes: codes }).then(function (x) {
      checkBtn.disabled = !can;
      if (!x.j || !x.j.ok) { say(msg, errText(x), true); return; }
      if (x.j.unavailable) { say(msg, 'Company DB に 0072 がまだありません', true); return; }
      checked = x.j; render(x.j); buildState();
      var n = x.j.rows.filter(function (r) { return r.ok; }).length;
      say(msg, x.j.all_ok ? ('全部 (' + n + ' 件) 消せます。理由を書いて「消す CSV を作る」') : ('消せないものがあります (消せる ' + n + ' / ' + (x.j.rows.length + (x.j.missing || []).length) + ' 件)。止まっている商品を外して確かめ直してください'), !x.j.all_ok);
    }).catch(function () { checkBtn.disabled = !can; say(msg, 'つながりませんでした。もう一度', true); });
  });
  if (codesIn) codesIn.addEventListener('input', function () { checked = null; goBox.hidden = true; buildState(); });
  if (reason) reason.addEventListener('input', buildState);
  var bg = $('#nd-bg');
  if (buildBtn) buildBtn.addEventListener('click', function () {
    if (buildBtn.disabled) return;
    $('#nd-bg-n').textContent = String(checked.rows.length); $('#nd-bg-why').textContent = reason.value.trim();
    modal(bg, true); $('#nd-bg-stay').focus();
  });
  if (bg) {
    $('#nd-bg-stay').addEventListener('click', function () { modal(bg, false); buildBtn.focus(); });
    bg.addEventListener('click', function (e) { if (e.target === bg) modal(bg, false); });
    $('#nd-bg-ok').addEventListener('click', function () {
      modal(bg, false);
      var codes = checked.rows.map(function (r) { return r.code; });
      var key = JSON.stringify([codes, reason.value.trim()]);
      if (sentKey !== null && sentKey !== key) buildId = newId();   // 中身を変えた押し直し = 別の操作
      sentKey = key;
      busy = true; buildState(); say(msg, '消す CSV を作っています…');
      post('/api/ne-delete/build', { codes: codes, reason: reason.value.trim(), request_id: buildId, confirm: true }).then(function (x) {
        busy = false;
        if (!x.j || !x.j.ok) { buildState(); say(msg, errText(x), true); return; }
        say(msg, (x.j.replayed ? 'もう作ってありました' : '作りました') + '。下のボタンでダウンロードして、NE に取り込んでください (セットを先に)');
        built.textContent = '';
        (x.j.exports || []).forEach(function (e, i) {
          var card = el('article', 'filecard');
          var fh = el('div', 'fh');
          fh.appendChild(el('span', 'ttl', (i + 1) + '. ファイル #' + e.export_id));
          fh.appendChild(el('span', 'b ' + (e.kind === 'sets' ? 'vio' : 'mute'), e.kind === 'sets' ? 'NE の [セット商品マスタ] で取り込む' : 'NE の [商品マスタ] で取り込む'));
          var b = el('button', 'btn pri sm', 'ダウンロード'); b.type = 'button'; b.addEventListener('click', function () { download(e.export_id, msg); });
          fh.appendChild(b); card.appendChild(fh);
          card.appendChild(el('div', 'fmeta', e.row_count + ' 行 · ' + (e.codes || []).join('・')));
          built.appendChild(card);
        });
        var again = el('div', 'hint', '画面を読み直すと「依頼の途中」に出ます。'); built.appendChild(again);
        built.hidden = false; checked = null; goBox.hidden = true; buildId = newId(); sentKey = null;
      }).catch(function () { busy = false; buildState(); say(msg, 'つながりませんでした。もう一度押してください (同じ操作は 2 回入りません)', true); });
    });
  }

  // ─── 取り下げ ───
  var wbg = $('#nw-bg'), wreason = $('#nw-reason'), wok = $('#nw-bg-ok'), lmsg = $('#nl-msg');
  var wItem = null, wId = newId(), wKey = null;
  $$('[data-withdraw]').forEach(function (b) {
    b.addEventListener('click', function () {
      wItem = b.getAttribute('data-withdraw'); $('#nw-bg-code').textContent = b.getAttribute('data-code') || '';
      wreason.value = ''; wok.disabled = true; modal(wbg, true); wreason.focus();
    });
  });
  if (wbg) {
    wreason.addEventListener('input', function () { wok.disabled = !wreason.value.trim(); });
    $('#nw-bg-stay').addEventListener('click', function () { modal(wbg, false); });
    wbg.addEventListener('click', function (e) { if (e.target === wbg) modal(wbg, false); });
    wok.addEventListener('click', function () {
      var key = JSON.stringify([wItem, wreason.value.trim()]);
      if (wKey !== null && wKey !== key) wId = newId();
      wKey = key; wok.disabled = true; say(lmsg, '取り下げています…');
      post('/api/ne-delete/items/' + encodeURIComponent(wItem) + '/withdraw', { reason: wreason.value.trim(), request_id: wId }).then(function (x) {
        if (!x.j || !x.j.ok) { wok.disabled = false; say(lmsg, errText(x), true); return; }
        modal(wbg, false); say(lmsg, '取り下げました。画面を読み直します'); setTimeout(function () { window.location.reload(); }, 600);
      }).catch(function () { wok.disabled = false; say(lmsg, 'つながりませんでした。もう一度', true); });
    });
  }

  // ─── 断られた単品 → 取扱中止の CSV ───
  var hbtn = $('#nh-build'), hreason = $('#nh-reason'), hbg = $('#nh-bg'), hId = newId(), hKey = null;
  function picked() { return $$('.nd-pick').filter(function (c) { return c.checked; }).map(function (c) { return c.value; }); }
  function hState() { if (!hbtn) return; var n = picked().length; $('#nh-n').textContent = String(n); hbtn.disabled = !can || !n || !hreason.value.trim(); }
  $$('.nd-pick').forEach(function (c) { c.addEventListener('change', hState); });
  if (hreason) hreason.addEventListener('input', hState);
  if (hbtn) hbtn.addEventListener('click', function () {
    if (hbtn.disabled) return;
    $('#nh-bg-n').textContent = String(picked().length); $('#nh-bg-why').textContent = hreason.value.trim();
    modal(hbg, true); $('#nh-bg-stay').focus();
  });
  if (hbg) {
    $('#nh-bg-stay').addEventListener('click', function () { modal(hbg, false); });
    hbg.addEventListener('click', function (e) { if (e.target === hbg) modal(hbg, false); });
    $('#nh-bg-ok').addEventListener('click', function () {
      modal(hbg, false);
      var ids = picked(); var key = JSON.stringify([ids, hreason.value.trim()]);
      if (hKey !== null && hKey !== key) hId = newId();
      hKey = key; hbtn.disabled = true; say(lmsg, '取扱中止の CSV を作っています…');
      post('/api/ne-delete/handling', { item_ids: ids, reason: hreason.value.trim(), request_id: hId, confirm: true }).then(function (x) {
        if (!x.j || !x.j.ok) { hState(); say(lmsg, errText(x), true); return; }
        say(lmsg, '作りました (ファイル #' + x.j.export.export_id + ')。ダウンロードして、NE の [商品一括登録] で取り込んでください');
        download(x.j.export.export_id, lmsg);
        setTimeout(function () { window.location.reload(); }, 2500);
      }).catch(function () { hState(); say(lmsg, 'つながりませんでした。もう一度押してください (同じ操作は 2 回入りません)', true); });
    });
  }
  if (codesIn && codesIn.value.trim() && can && checkBtn) checkBtn.click();   // 商品の画面から来た (?codes=) = すぐ確かめる (読むだけ)
})();
