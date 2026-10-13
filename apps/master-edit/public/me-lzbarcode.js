/*
 * me-lzbarcode.js — 商品の画面の「ロジザードのバーコード」の欄の動き (views/_lz-barcode.ejs・Company DB の ops.sku_logizard_barcode_rules・CompanyDB構想 23 v4)
 *   - 値を打つ (FNSKU = X00 + 英数字 7 字・大文字にそろえる / JAN = 8・13 けた・チェック数字) か「空欄 (ロジザードが番号を付ける)」に印
 *   - 自分の「バーコードを保存」(POST /api/sku/:code/logizard-barcode・開いたときの行番号 seen_row)。Company DB の「保存する」とは別。
 *     未保存の数は MasterEdit.dirty() に足す (離れるときの確認に出る)
 *   - 通信が分からないまま終わった保存は、同じ中身なら同じ request_id で送り直す (2 回書かない・stale にしない)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var dataEl = document.getElementById('me-lzbc');
  var box = document.getElementById('sec-lzbc');
  if (!dataEl || !box) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P || !P.canWrite) return;
  var ME = window.MasterEdit = window.MasterEdit || {};
  var saveBtn = $('#lzbc-save'), inp = $('#lzbc-in'), blank = $('#lzbc-blank');
  var busy = false, pending = null;
  var initial = JSON.stringify(P.kind === 'lz_assign' ? { blank: true, value: '' } : { blank: false, value: P.value || '' });

  function janOk(v) {
    if (!/^\d{8}$/.test(v) && !/^\d{13}$/.test(v)) return false;
    var d = v.split('').map(Number), check = d.pop(), sum = 0;
    for (var i = d.length - 1, w = 3; i >= 0; i--, w = w === 3 ? 1 : 3) sum += d[i] * w;
    return (10 - (sum % 10)) % 10 === check;
  }
  /** 打った値 → { kind, value } か { problem } (サーバーの lzValueShape と同じ決まり) */
  function shape(raw) {
    var v = String(raw || '').normalize('NFKC').trim().toUpperCase();
    if (!v) return { problem: '' };
    if (/^X00[0-9A-Z]{7}$/.test(v)) return { kind: 'fnsku', value: v };
    if (/^\d+$/.test(v)) {
      if (v.length !== 8 && v.length !== 13) return { problem: 'JAN は 8 けたか 13 けた (いま ' + v.length + ' けた)' };
      return janOk(v) ? { kind: 'jan', value: v } : { problem: 'JAN の最後の数字 (チェック数字) が合いません' };
    }
    return { problem: /^X0/.test(v) ? 'FNSKU は X00 + 英数字 7 字の 10 文字' : 'FNSKU (X00…) か JAN (数字) を入れてください' };
  }
  function now() { return { blank: !!(blank && blank.checked), value: blank && blank.checked ? '' : String(inp.value || '').normalize('NFKC').trim().toUpperCase() }; }
  function isDirty() { return JSON.stringify(now()) !== initial; }
  function body() {
    var n = now();
    if (n.blank) return { kind: 'lz_assign' };
    var s = shape(n.value);
    return s.kind ? { kind: 'value', value: s.value } : null;
  }
  function msg(t, cls) { var m = $('#lzbc-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function changed() {
    var n = now(), s = n.blank ? null : shape(n.value);
    if (inp) inp.disabled = n.blank;
    var m2 = $('#lzbc-msg2');
    if (m2) m2.textContent = n.blank ? 'ロジザードが番号を付けます' : s && s.kind ? (s.kind === 'fnsku' ? 'FNSKU の形' : 'JAN の形 (チェック数字 OK)') : (s ? s.problem : '');
    var d = isDirty();
    box.classList.toggle('po-dirty', d);
    if (saveBtn) saveBtn.disabled = busy || !d || !body();
    if (ME.setUnsaved && ME.dirty) ME.setUnsaved(ME.dirty().n);
  }
  var base = ME.dirty;
  ME.dirty = function () {
    var d = base ? base() : { n: 0, items: [], impacts: [] };
    if (!isDirty()) return d;
    return { n: d.n + 1, items: d.items.concat(['ロジザードのバーコード: まだ保存していません']), impacts: d.impacts };
  };
  // 値を打ったら「空欄」の印を外す (両方は持たない)
  if (inp) inp.addEventListener('input', function () { if (blank && blank.checked && inp.value.trim()) blank.checked = false; changed(); });
  if (blank) blank.addEventListener('change', function () { if (blank.checked && inp) inp.value = ''; changed(); });
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function save() {
    var bc = body();
    if (!saveBtn || busy || !bc) return;
    var key = JSON.stringify([bc, P.seenRow]);
    if (!pending || pending.key !== key) pending = { key: key, rid: uuid() };
    busy = true; changed(); msg('保存しています…');
    fetch(P.base + '/api/sku/' + encodeURIComponent(P.code) + '/logizard-barcode', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ request_id: pending.rid, barcode: bc, seen_row: P.seenRow }) })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        // 番号を捨てるのは、ちゃんとした返事 (成功 か はっきりした 4xx) のときだけ。5xx・本文の読めない返事 = 同じ番号のまま
        var sure = !!x.j && (x.r.ok ? x.j.ok === true : x.r.status >= 400 && x.r.status < 500);
        if (!sure) { msg('保存されたか分かりません (HTTP ' + x.r.status + ')。もう一度「バーコードを保存」を押してください (同じ保存として送り直します)', 'err'); changed(); return; }
        pending = null;
        if (x.r.ok && x.j.ok) {
          P.seenRow = x.j.rule_row || P.seenRow;
          initial = JSON.stringify(now());
          msg(x.j.changed ? '保存しました' : '今と同じです (書いていません)', 'ok');
          if (ME.toast && x.j.changed) ME.toast('ロジザードのバーコードを保存しました');
          if (ME.dirty && ME.dirty().n === 0 && ME.reloadPage) ME.reloadPage();
          changed();
          return;
        }
        msg(x.j.error || ('HTTP ' + x.r.status), 'err');
        changed();
      })
      .catch(function () { busy = false; changed(); msg('通信できませんでした。保存されたか分からないので、もう一度「バーコードを保存」を押してください (同じ保存として送り直します)', 'err'); });
  }
  if (saveBtn) saveBtn.addEventListener('click', save);
  changed();
})();
