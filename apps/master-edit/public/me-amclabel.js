/*
 * me-amclabel.js — 「AMC 商品ラベルのバーコード印字」の欄の動き (views/_amc-label.ejs・発注アプリの po_barcode_labels に保存・Company DB構想 21 A1)
 *   - 刷る値は候補 (Company DB の FNSKU・JAN) から選ぶ。手で打つときは理由が要る。今の値が候補に無いときは「今の値のまま」も選べる
 *   - 自分の「ラベルの印字を保存」(POST /api/sku/:code/amc-label・開いたときの版 version)。Company DB の保存とは別。
 *     未保存の数は MasterEdit.dirty() に足す (離れるときの確認に出る)
 *   - 入荷アプリの「入庫で BC シールを貼る」とは合わせない。印字済みなのにシールを貼る = 印を出すだけ
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var dataEl = document.getElementById('me-amc');
  var box = document.getElementById('sec-amc');
  if (!dataEl || !box) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var ME = window.MasterEdit = window.MasterEdit || {};
  var saveBtn = $('#amc-save');
  var busy = false;
  var initial = null;

  function statusNow() { var s = $('#amc-status'); return s ? s.getAttribute('data-value') || '' : ''; }
  function pickNow() { var r = $('input[name="amc-pick"]:checked', box); return r ? r.value : ''; }
  /** JAN のチェック数字 (サーバーの amc-label.mjs janCheckOk と同じ) */
  function janOk(v) {
    if (!/^\d{8}$/.test(v) && !/^\d{13}$/.test(v)) return false;
    var d = v.split('').map(Number), check = d.pop(), sum = 0;
    for (var i = d.length - 1, w = 3; i >= 0; i--, w = w === 3 ? 1 : 3) sum += d[i] * w;
    return (10 - (sum % 10)) % 10 === check;
  }
  function isOther() { var c = $('#amc-mx'); return !!(c && c.checked); }
  function typeOf(v) {
    v = String(v || '').trim();
    var other = isOther();
    if (!v) return '';
    if (/^X0[0-9A-Z]{8}$/i.test(v)) return other ? 'FNSKU の形です (例外の印を外してください)' : 'FNSKU の形';
    if (janOk(v)) return other ? 'JAN の形です (例外の印を外してください)' : 'JAN の形 (チェック数字 OK)';
    if (/^\d{8}$/.test(v) || /^\d{13}$/.test(v)) return 'JAN のチェック数字が合いません (打ち間違い?)';
    return other ? '例外の形 (理由を書いてください)' : 'FNSKU・JAN の形ではありません (ほかの形なら「例外の形」に印)';
  }
  /** 今の選びで刷る値 (画面の見え方だけ。保存はサーバーが Company DB の値で決める) */
  function valueNow() {
    var p = pickNow();
    if (p === 'none') return '';
    if (p === 'keep') return P.value || '';
    if (p === 'manual') return ($('#amc-mv') || {}).value ? $('#amc-mv').value.trim() : '';
    var i = /^c(\d+)$/.exec(p);
    return i && P.cands[+i[1]] ? P.cands[+i[1]].value : '';
  }
  /** 送る本文 */
  function collect() {
    var p = pickNow(), pick;
    var i = /^c(\d+)$/.exec(p);
    if (i && P.cands[+i[1]]) {
      var c = P.cands[+i[1]];
      pick = { kind: c.kind, row: c.row, value: c.value };
      if (c.kind === 'fnsku') pick.listing_id = c.listing_id;
    } else if (p === 'manual') pick = { kind: isOther() ? 'manual_other' : 'manual', value: $('#amc-mv').value.trim(), reason: $('#amc-mr').value.trim() };
    else pick = { kind: p || 'none' };
    return { version: P.version, status: statusNow(), note: ($('#amc-note') || {}).value ? $('#amc-note').value.trim() : '', pick: pick };
  }
  function isDirty() { return JSON.stringify(collect()) !== initial; }

  function show() {
    var st = statusNow(), p = pickNow(), v = valueNow();
    var hint = $('#amc-status-hint'); if (hint) hint.textContent = (P.hints[st] || '') + (P.version == null ? ' · 保存すると発注アプリに行ができます' : '');
    var man = $('#amc-manual'); if (man) man.hidden = p !== 'manual';
    var ty = $('#amc-mv-type'); if (ty) ty.textContent = p === 'manual' ? typeOf(v) : '';
    $$('.amc-cand', box).forEach(function (l) { var r = $('input', l); l.classList.toggle('on', !!(r && r.checked)); });
    var w = '';
    if ((st === 'printed' || st === 'requested') && !v) w = '刷る値が空です (分かるなら選んでおくのがおすすめ)';
    if (st === 'not_needed' && v) w = '「不要」なのに刷る値があります';
    var wm = $('#amc-warn'); if (wm) wm.textContent = w;
    // 入荷アプリとの食い違い (自動では合わせない)
    var mm = $('#amc-mismatch'), mt = $('#amc-mismatch-t');
    var bad = !!(P.inbound && P.inbound.needed === true && st === 'printed');
    if (mm) mm.hidden = !bad;
    if (mt && bad) mt.textContent = '入荷アプリでは「入庫で BC シールを貼る」(' + P.inbound.bc_seal + ') になっています。ラベルに印字済みなら、入荷アプリの値も確かめてください (自動では直しません)';
  }
  function changed() {
    show();
    var d = isDirty();
    box.classList.toggle('po-dirty', d);
    // まだ発注アプリに行が無い (未登録・version = null) = 変えなくても保存できる (「未設定・値なし」の行を作る。Codex #1701 R3 M)。
    //   離れるときの確認 (未保存) には数えない (何も変えていない)
    if (saveBtn) saveBtn.disabled = !P.canWrite || busy || !(d || P.version == null);
    if (ME.setUnsaved && ME.dirty) ME.setUnsaved(ME.dirty().n);
  }
  // 離れるときの確認 (me-shell.js) に AMC ラベルの未保存も出す
  var base = ME.dirty;
  ME.dirty = function () {
    var d = base ? base() : { n: 0, items: [], impacts: [] };
    if (!isDirty()) return d;
    return { n: d.n + 1, items: d.items.concat(['AMC 商品ラベルの印字 (発注アプリ): まだ保存していません']), impacts: d.impacts };
  };
  box.addEventListener('input', changed);
  box.addEventListener('change', changed);

  function msg(t, cls) { var m = $('#amc-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function markErr(field) {
    $$('.f.err', box).forEach(function (f) { f.classList.remove('err'); });
    if (!field) return;
    var row = $('[data-row="amc.' + String(field).replace(/"/g, '') + '"]', box);
    if (!row) return;
    row.classList.add('err');
    var c = $('input:not([disabled]), button:not([disabled])', row); if (c) c.focus();
  }
  function nowRow(label) {
    var badge = { printed: 'ok', requested: 'info', unset: 'warn', not_needed: 'mute' };
    var st = $('#amc-now-st'); if (st) { st.className = 'b ' + (badge[label.status] || 'mute'); st.textContent = P.words[label.status] || label.status; }
    var v = $('#amc-now-v'); if (v) v.textContent = label.barcodeValue || '値なし';
  }
  function save() {
    if (!saveBtn || busy || !P.canWrite) return;
    var body = collect();
    if ((body.pick.kind === 'manual' || body.pick.kind === 'manual_other') && !body.pick.reason) { msg('手で打つときは理由が要ります', 'err'); markErr('manual_reason'); return; }
    body.request_id = uuid();
    busy = true; changed(); msg('発注アプリに保存しています…');
    fetch(P.base + '/api/sku/' + encodeURIComponent(P.code) + '/amc-label', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        if (x.r.ok && x.j.ok) {
          P.version = x.j.label.version;
          P.value = x.j.label.barcodeValue || '';
          nowRow(x.j.label);
          initial = JSON.stringify(collect());
          markErr(null);
          msg('発注アプリに保存しました', 'ok');
          if (ME.toast) ME.toast('AMC 商品ラベルの印字を保存しました (発注アプリ)');
          // 候補の印・「今の値のまま」を今の値で出し直す = 画面を読み直す (Company DB の欄に未保存があれば読み直さない)
          if (ME.dirty && ME.dirty().n === 0 && ME.reloadPage) ME.reloadPage();
          changed();
          return;
        }
        var t = x.j.error || ('HTTP ' + x.r.status);
        if (x.j.reason === 'stale' || x.j.reason === 'candidate_gone' || x.j.reason === 'candidate_changed' || x.j.reason === 'recheck_failed' || x.j.reason === 'recheck_unavailable' || x.j.reason === 'value_of_other') t += ' (画面を開き直すと今の値が出ます)';
        msg(t, 'err');
        markErr(x.j.field);
        changed();
      })
      .catch(function () { busy = false; changed(); msg('通信できませんでした。保存されたか分からないので、画面を開き直して確かめてください', 'err'); });
  }
  if (saveBtn) saveBtn.addEventListener('click', save);

  initial = JSON.stringify(collect());
  changed();
})();
