/*
 * me-order.js — 「発注の設定」の欄の動き (views/_order.ejs・発注アプリに保存)
 *   - 発注条件グループ / 原料グループ: なし / 今あるグループ / 新しく作る を切り替える
 *   - 発注条件グループは代表の仕入先のものだけ (新商品の登録 = 代表の仕入先の欄を選び直すとその場で絞る)
 *   - 新商品の登録 (mode = new): 下書きの保存と一緒に送る = MasterEdit.orderSettings.collect() を me-new.js が本文の order_settings に入れる
 *   - 商品の画面 (mode = sku): 自分の「発注の設定を保存」(POST /api/sku/:code/order-settings・開いたときの印 seen)。Company DB の保存とは別。
 *     未保存の数は MasterEdit.dirty() に足す (離れるときの確認に出る)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var dataEl = document.getElementById('me-order');
  var box = document.getElementById('sec-order');
  if (!dataEl || !box) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var ME = window.MasterEdit = window.MasterEdit || {};
  var val = function (id) { var e = document.getElementById(id); return e ? e.value.trim() : ''; };
  var segVal = function (id) { var s = document.getElementById(id); return s ? s.getAttribute('data-value') || '' : ''; };
  function normSup(v) { var t = String(v == null ? '' : v).trim(); return /^\d+$/.test(t) ? String(parseInt(t, 10)) : t; }

  /* ---------- なし / 今ある / 新しく作る ---------- */
  function showModes() {
    var c = segVal('po-cond-mode'), m = segVal('po-mat-mode');
    var cp = $('#po-cond-pick'), cn = $('#po-cond-new'), mp = $('#po-mat-pick'), mn = $('#po-mat-new');
    if (cp) cp.hidden = c !== 'pick'; if (cn) cn.hidden = c !== 'new';
    if (mp) mp.hidden = m !== 'pick'; if (mn) mn.hidden = m !== 'new';
  }
  ['po-cond-mode', 'po-mat-mode'].forEach(function (id) { var e = document.getElementById(id); if (e) e.addEventListener('change', function () { showModes(); changed(); }); });
  // 新しい発注条件の単位 (決まりで決まる)
  var typeSel = $('#po-nc-type');
  function showUnit() { var u = $('#po-nc-unit'); if (u && typeSel) u.textContent = (typeSel.value.split('|')[1] || ''); }
  if (typeSel) typeSel.addEventListener('change', showUnit);

  /* ---------- 代表の仕入先で発注条件グループを絞る ---------- */
  function supplierNow() {
    if (P.mode === 'new') { var s = document.getElementById('f-primary_supplier'); return s ? normSup(s.value) : ''; }
    return P.supplier || '';
  }
  function filterConditions() {
    var sup = supplierNow();
    var sel = $('#po-condition_id'); if (!sel) return;
    $$('option[data-sup]', sel).forEach(function (o) {
      var mine = !!sup && o.getAttribute('data-sup') === sup;
      var keep = mine || (o.selected && P.mode === 'sku');   // 商品の画面 = 今の値 (ほかの仕入先のグループ) は消さない
      o.hidden = !keep; o.disabled = !keep;
      if (!keep && o.selected) sel.value = '';
    });
    var hint = $('#po-cond-hint');
    var n = $$('option[data-sup]', sel).filter(function (o) { return !o.disabled; }).length;
    if (hint) hint.textContent = !sup ? '先に代表の仕入先を選んでください (その仕入先のグループだけ選べます)'
      : n ? '代表の仕入先 (' + sup + ') のグループだけ選べます (最低金額・最低数量など)' : '代表の仕入先 (' + sup + ') のグループはまだありません。「新しく作る」で作れます';
    var ns = $('#po-cond-new-sup'); if (ns) ns.textContent = sup ? ' ' + sup : ' (先に代表の仕入先を)';
  }
  if (P.mode === 'new') { var supSel = document.getElementById('f-primary_supplier'); if (supSel) supSel.addEventListener('change', filterConditions); }

  /* ---------- 集める ---------- */
  /** 送る値。何も入れていない (全部空・なし) = null (新商品の登録では発注アプリに何も書かない) */
  function collect(opts) {
    var all = opts && opts.all;
    var out = { order_lot: val('po-order_lot'), capacity_per_unit: val('po-capacity_per_unit'), case_group: val('po-case_group'), case_lot: val('po-case_lot') };
    var c = segVal('po-cond-mode'), m = segVal('po-mat-mode');
    out.condition_id = c === 'pick' ? val('po-condition_id') : '';
    out.material_group_id = m === 'pick' ? val('po-material_group_id') : '';
    if (c === 'new') {
      var tu = val('po-nc-type').split('|');
      out.new_condition = { condition_id: val('po-nc-id'), display_name: val('po-nc-name'), condition_type: tu[0] || '', unit: tu[1] || '', condition_value: val('po-nc-value') };
      delete out.condition_id;
    }
    if (m === 'new') {
      out.new_material = { group_id: val('po-nm-id'), name: val('po-nm-name'), min_order_qty: val('po-nm-min'), unit: val('po-nm-unit') };
      delete out.material_group_id;
    }
    if (all) return out;
    var empty = Object.keys(out).every(function (k) { return k === 'new_condition' || k === 'new_material' ? !out[k] : out[k] === ''; });
    return empty ? null : out;
  }
  ME.orderSettings = { collect: collect };

  /* ---------- 商品の画面: 自分の保存 ---------- */
  var saveBtn = $('#po-save');
  var initial = null;   // 開いたときの値 (下で絞ってから)
  var seen = P.seen == null ? null : P.seen;
  var busy = false;
  function isDirty() { return JSON.stringify(collect({ all: true })) !== initial; }
  function changed() {
    if (P.mode !== 'sku') return;
    var d = isDirty();
    box.classList.toggle('po-dirty', d);
    if (saveBtn) saveBtn.disabled = !P.canWrite || busy || !d;
    if (ME.setUnsaved && ME.dirty) ME.setUnsaved(ME.dirty().n);
  }
  if (P.mode === 'sku') {
    // 離れるときの確認 (me-shell.js) に発注の設定の未保存も出す (Company DB の欄の数に 1 足す)
    var base = ME.dirty;
    ME.dirty = function () {
      var d = base ? base() : { n: 0, items: [], impacts: [] };
      if (!isDirty()) return d;
      return { n: d.n + 1, items: d.items.concat(['発注の設定 (発注アプリ): まだ保存していません']), impacts: d.impacts };
    };
    box.addEventListener('input', changed);
    box.addEventListener('change', changed);
  }
  function msg(t, cls) { var m = $('#po-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function markErr(field) {
    $$('.f.err', box).forEach(function (f) { f.classList.remove('err'); });
    if (!field) return;
    var row = $('[data-row="' + String(field).replace(/"/g, '') + '"]', box);
    if (!row) return;
    row.classList.add('err');
    var det = row.closest('details'); if (det) det.open = true;
    var c = $('input:not([disabled]), select:not([disabled])', row); if (c) c.focus();
  }
  function save() {
    if (!saveBtn || busy || !P.canWrite) return;
    busy = true; changed(); msg('発注アプリに保存しています…');
    var body = { request_id: uuid(), seen: { updated_at: seen }, values: collect({ all: true }) };
    fetch(P.base + '/api/sku/' + encodeURIComponent(P.code) + '/order-settings', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        if (x.r.ok && x.j.ok) {
          seen = x.j.row ? x.j.row.updated_at : null;
          initial = JSON.stringify(collect({ all: true }));
          markErr(null);
          msg(x.j.changed ? '発注アプリに保存しました' : '変わっていません (同じ値)', 'ok');
          if (ME.toast) ME.toast(x.j.changed ? '発注の設定を保存しました (発注アプリ)' : '発注の設定は変わっていません');
          // 新しく作ったグループは「今あるグループ」に並べ直す = 画面を読み直す (Company DB の欄に未保存があれば読み直さない)
          if (x.j.created && (x.j.created.condition || x.j.created.material) && ME.dirty && ME.dirty().n === 0 && ME.reloadPage) ME.reloadPage();
          changed();
          return;
        }
        msg(x.j.error || ('HTTP ' + x.r.status), 'err');
        if (x.j.reason === 'stale') msg((x.j.error || '') + ' (画面を開き直すと今の値が出ます)', 'err');
        markErr(x.j.field);
        changed();
      })
      .catch(function () { busy = false; changed(); msg('通信できませんでした。もう一度「発注の設定を保存」を押してください', 'err'); });
  }
  if (saveBtn) saveBtn.addEventListener('click', save);

  showModes();
  showUnit();
  filterConditions();
  initial = JSON.stringify(collect({ all: true }));
  changed();
})();
