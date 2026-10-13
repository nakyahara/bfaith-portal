/*
 * me-lzbarcode.js — 「ロジザードに送るバーコード」の欄の動き (views/_lz-barcode.ejs・Company DB の ops.sku_logizard_barcode_rules・CompanyDB構想 23)
 *   - 決め方 (自動 / FNSKU / JAN / 空欄) を切り替えると「今の答え」が変わる (答えはサーバーが決め方ごとに計算して渡した物 = 画面で決まりを持たない)
 *   - 決め方 FNSKU / JAN で候補が 2 つ以上 = 送る 1 本を選ぶ (選ばない = 要確認のまま)
 *   - 自分の「決め方を保存」(POST /api/sku/:code/logizard-barcode・開いたときの行番号 seen_row)。Company DB の「保存する」とは別。
 *     未保存の数は MasterEdit.dirty() に足す (離れるときの確認に出る)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var dataEl = document.getElementById('me-lzbc');
  var box = document.getElementById('sec-lzbc');
  if (!dataEl || !box) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var ME = window.MasterEdit = window.MasterEdit || {};
  var saveBtn = $('#lzbc-save');
  var busy = false;
  var initial = null;
  // 決め方ごとの「選んだ 1 本」(Codex PR R1 M1: ほかの決め方へ持ち越さない)。初めの値は今の決め方のときだけ。'' = 「選ばない」を押した
  var pinBy = {};
  if (P.rule && P.pinned) pinBy[P.rule] = P.pinned;
  // 押し直しの番号 (Codex PR R1 M4): 通信が分からないまま終わった保存は、同じ中身なら同じ request_id で送り直す (2 回書かない・stale にしない)
  var pending = null;
  var BADGE = { fnsku: 'info', jan: 'vio', blank: 'mute', ambiguous: 'warn', waiting: 'warn' };

  function ruleNow() { var s = $('#lzbc-rule'); return s ? s.getAttribute('data-value') || '' : ''; }
  function pinList(rule) { return rule === 'fnsku' ? P.cands.fnsku : rule === 'jan' ? P.cands.jan : []; }
  function pinNow() { var p = pinBy[ruleNow()]; return p ? p : null; }
  function collect() { var rule = ruleNow(); return { rule: rule, pinned: (rule === 'fnsku' || rule === 'jan') ? pinNow() : null }; }
  function isDirty() { return JSON.stringify(collect()) !== initial; }
  /** 今の選びの答え (サーバーの答え + 選んだ 1 本) */
  function answerNow() {
    var c = collect();
    var a = P.answers[c.rule] || { state: 'ambiguous', value: null, reason: '' };
    if (c.pinned) {
      var list = pinList(c.rule);
      if (list.indexOf(c.pinned) >= 0) return { state: c.rule, value: c.pinned, reason: '選んだ 1 本を送ります' };
      return { state: 'ambiguous', value: null, reason: '選んだ ' + c.pinned + ' が今の候補にありません。選び直してください' };
    }
    return a;
  }
  function drawPins() {
    var rule = ruleNow(), list = pinList(rule), row = $('#lzbc-pin-row'), host = $('#lzbc-pins');
    if (!row || !host) return;
    var cur = pinNow();
    row.hidden = !(list.length > 1 || (cur && (rule === 'fnsku' || rule === 'jan')));
    host.textContent = '';
    if (row.hidden) return;
    var items = list.slice();
    if (cur && items.indexOf(cur) < 0 && (rule === 'fnsku' || rule === 'jan')) items.push(cur);   // 前に選んだ値が候補から消えた = 出しておく (要確認)
    items.forEach(function (v) {
      var lab = document.createElement('label'); lab.className = 'amc-cand';
      var r = document.createElement('input'); r.type = 'radio'; r.name = 'lzbc-pin'; r.value = v; r.checked = v === cur; r.disabled = !P.canWrite;
      var body = document.createElement('span'); body.className = 'body';
      var val = document.createElement('span'); val.className = 'v mono'; val.textContent = v;
      var b = document.createElement('span'); b.className = 'b ' + (rule === 'fnsku' ? 'info' : 'vio'); b.textContent = rule === 'fnsku' ? 'FNSKU' : 'JAN';
      body.appendChild(val); body.appendChild(b);
      if (list.indexOf(v) < 0) { var g = document.createElement('span'); g.className = 'b warn'; g.textContent = '候補に無い'; body.appendChild(g); }
      lab.appendChild(r); lab.appendChild(body); host.appendChild(lab);
    });
    var none = document.createElement('label'); none.className = 'amc-cand';
    var rn = document.createElement('input'); rn.type = 'radio'; rn.name = 'lzbc-pin'; rn.value = ''; rn.checked = !cur; rn.disabled = !P.canWrite;
    var nb = document.createElement('span'); nb.className = 'body'; var nv = document.createElement('span'); nv.className = 'v'; nv.textContent = '選ばない';
    var nm = document.createElement('span'); nm.className = 'meta'; nm.textContent = '候補が 1 つになるまで要確認のまま';
    nb.appendChild(nv); nb.appendChild(nm); none.appendChild(rn); none.appendChild(nb); host.appendChild(none);
  }
  function show() {
    var rule = ruleNow();
    var h = $('#lzbc-rule-hint'); if (h) h.textContent = P.hints[rule] || '';
    var a = answerNow();
    var v = $('#lzbc-v'); if (v) v.textContent = a.state === 'blank' ? '空欄' : (a.value || '—');
    var st = $('#lzbc-st'); if (st) { st.className = 'b ' + (BADGE[a.state] || 'mute'); st.textContent = P.states[a.state] || a.state; }
    var rs = $('#lzbc-reason'); if (rs) rs.textContent = a.reason || (a.state === 'blank' ? 'ロジザードが番号を付けます (入荷アプリの値札で刷って貼る)' : 'Company DB の今の値から決めました (CSV を作るときにもう一度決めます)');
    $$('#lzbc-pins .amc-cand').forEach(function (l) { var r = $('input', l); l.classList.toggle('on', !!(r && r.checked)); });
  }
  function changed() {
    show();
    var d = isDirty();
    box.classList.toggle('po-dirty', d);
    // 決め方の行がまだ無い (この機能の前の商品) = 変えなくても保存できる (今の選び = 自動 を決め方として残す)
    if (saveBtn) saveBtn.disabled = !P.canWrite || busy || !(d || !P.seenRow);
    if (ME.setUnsaved && ME.dirty) ME.setUnsaved(ME.dirty().n);
  }
  var base = ME.dirty;
  ME.dirty = function () {
    var d = base ? base() : { n: 0, items: [], impacts: [] };
    if (!isDirty()) return d;
    return { n: d.n + 1, items: d.items.concat(['ロジザードに送るバーコードの決め方: まだ保存していません']), impacts: d.impacts };
  };
  var seg = $('#lzbc-rule');
  if (seg) {
    seg.addEventListener('click', function (e) {
      var b = e.target.closest ? e.target.closest('button[data-v]') : null;
      if (!b || b.disabled) return;
      seg.setAttribute('data-value', b.getAttribute('data-v'));
      $$('button', seg).forEach(function (x) { var on = x === b; x.classList.toggle('on', on); x.setAttribute('aria-pressed', on ? 'true' : 'false'); });
      drawPins();
      changed();
    });
  }
  box.addEventListener('change', function (e) { if (e.target && e.target.name === 'lzbc-pin') { pinBy[ruleNow()] = e.target.value; changed(); } });

  function msg(t, cls) { var m = $('#lzbc-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function save() {
    if (!saveBtn || busy || !P.canWrite) return;
    var c = collect();
    var key = JSON.stringify([c.rule, c.pinned, P.seenRow]);
    if (!pending || pending.key !== key) pending = { key: key, rid: uuid() };
    var body = { request_id: pending.rid, rule: c.rule, pinned: c.pinned, seen_row: P.seenRow };
    busy = true; changed(); msg('保存しています…');
    fetch(P.base + '/api/sku/' + encodeURIComponent(P.code) + '/logizard-barcode', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        // 番号を捨てるのは、ちゃんとした返事 (成功 か はっきりした 4xx) のときだけ (Codex PR R2 M2)。
        //   5xx・本文の読めない返事 = 保存されたか分からない = 同じ番号のまま (もう一度押すと同じ保存として送り直す)
        var sure = !!x.j && (x.r.ok ? x.j.ok === true : x.r.status >= 400 && x.r.status < 500);
        if (!sure) { msg('保存されたか分かりません (HTTP ' + x.r.status + ')。もう一度「決め方を保存」を押してください (同じ保存として送り直します)', 'err'); changed(); return; }
        pending = null;
        if (x.r.ok && x.j.ok) {
          P.seenRow = x.j.rule_row || P.seenRow;
          P.rule = x.j.rule; P.pinned = x.j.pinned || null;
          initial = JSON.stringify(collect());
          msg(x.j.changed ? '保存しました' : '今の決め方と同じです (書いていません)', 'ok');
          if (ME.toast && x.j.changed) ME.toast('ロジザードに送るバーコードの決め方を保存しました');
          if (ME.dirty && ME.dirty().n === 0 && ME.reloadPage) ME.reloadPage();
          changed();
          return;
        }
        var t = x.j.error || ('HTTP ' + x.r.status);
        msg(t, 'err');
        changed();
      })
      .catch(function () { busy = false; changed(); msg('通信できませんでした。保存されたか分からないので、もう一度「決め方を保存」を押してください (同じ保存として送り直します)', 'err'); });
  }
  if (saveBtn) saveBtn.addEventListener('click', save);

  drawPins();
  initial = JSON.stringify(P.seenRow ? { rule: P.rule, pinned: P.pinned } : collect());
  changed();
})();
