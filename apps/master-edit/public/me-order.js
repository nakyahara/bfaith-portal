/*
 * me-order.js — 「発注の設定」の欄の動き (views/_order.ejs・発注アプリに保存)
 *   - 発注条件グループ / 原料グループ: なし / 今あるグループ / 新しく作る を切り替える
 *   - 発注条件グループは代表の仕入先のものだけ (新商品の登録 = 代表の仕入先の欄を選び直すとその場で絞る)
 *   - 新商品の登録 (mode = new): 下書きの保存と一緒に送る = MasterEdit.orderSettings.collect() を me-new.js が本文の order_settings に入れる
 *   - 商品の画面 (mode = sku): 自分の「発注の設定を保存」(POST /api/sku/:code/order-settings・開いたときの印 seen)。Company DB の保存とは別。
 *     未保存の数は MasterEdit.dirty() に足す (離れるときの確認に出る)
 *   🆕 2026-10-13 (入力なし): 新商品の登録では、6 つの項目 (発注ロット・発注条件グループ・原料グループ・ケースロット・ケースグループ・容量) は
 *     どれも「値」か「入力なし」(グループは「なし」= 入力なし)。MasterEdit.orderSettings.items() = 画面が数える項目 (書けないとき = 空)
 *   🆕 ⑤-1 / ⑤-2 (CompanyDB構想 27 §2.4): 先方品番・入数 (発注アプリの先方番号対応表・仕入先ごと)
 *     - 新商品の登録 (単品): 2 つの欄を collect() の vendor_code / qty_per_unit に入れる (任意・「入力なし」には数えない)
 *     - 商品の画面: 1 行ずつ「保存」(POST /api/sku/:code/vendor-code・開いたときの版 base_updated_at)・「消す」(…/delete)。
 *       発注の設定の保存とは別。未保存の行は MasterEdit.dirty() に足す
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
  if (P.mode === 'new') { var supSel = document.getElementById('f-primary_supplier'); if (supSel) supSel.addEventListener('change', function () { filterConditions(); groupFilter('po-condition_id', { pick: false }); }); }

  /* ---------- グループを名前・ID で絞る (10/16 中原さん「グループ名で検索できるように」) ----------
   *   選ぶ欄の上の「名前・ID で絞る」に打つと、選ぶ欄の候補をその言葉を全部含むものだけにする (空白で区切ると全部を含む)。
   *   かなの同一視 = search-fold.mjs と同じ (NFKC・小文字・ひらがな → カタカナ)。選んでいるグループは絞っても消さない。
   *   当たりが 1 つで、まだ選んでいない = それを選ぶ (change を出す = 未保存・数え直し)。
   *   候補は消して並べ直す (option の hidden は Safari が見ない)。発注条件は並べ直した後に代表の仕入先の絞り込みをかけ直す */
  function fold(s) { return String(s == null ? '' : s).normalize('NFKC').toLowerCase().replace(/[ぁ-ゖゝゞ]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) + 0x60); }); }
  var groupAll = {};
  // 絞り込みが出す change (未保存・数え直しのため) と、人が選んだ change を分ける
  var autoFiring = false;
  function fireAuto(sel) { autoFiring = true; try { sel.dispatchEvent(new Event('change', { bubbles: true })); } finally { autoFiring = false; } }
  /** opts.pick = false: 当たりが 1 つでも選ばない (代表の仕入先を変えたときの絞り直し) */
  function groupFilter(selId, opts) {
    var q = $('.po-gq[data-for="' + selId + '"]'), sel = document.getElementById(selId);
    if (!q || !sel || !groupAll[selId]) return;
    var words = fold(q.value).split(/\s+/).filter(Boolean);
    var hit = function (o) { var t = fold(o.value + ' ' + o.textContent); return words.every(function (w) { return t.indexOf(w) >= 0; }); };
    // 絞り込みが自分で選んだグループ (人が選んだのではない) が、打ち直した言葉に当たらなくなった = 選ぶのを取り消す (Codex R1 Medium)
    var auto = sel.getAttribute('data-auto-pick');
    if (auto && sel.value === auto) {
      var ao = groupAll[selId].filter(function (o) { return o.value === auto; })[0];
      if (!ao || (words.length && !hit(ao))) { sel.value = ''; sel.removeAttribute('data-auto-pick'); fireAuto(sel); }
    }
    var cur = sel.value;
    var keep = groupAll[selId].filter(function (o) { return (o.value === cur && cur !== '') || hit(o); });
    $$('option', sel).forEach(function (o) { if (o.value !== '') sel.removeChild(o); });
    keep.forEach(function (o) { sel.appendChild(o); });
    sel.value = cur;
    if (selId === 'po-condition_id') filterConditions();
    // 当たり = 言葉を含み、選べる (代表の仕入先の絞り込みで消えていない) もの。選んでいるだけで当たらないグループは数えない
    var live = $$('option', sel).filter(function (o) { return o.value !== '' && !o.disabled && hit(o); });
    if ((!opts || opts.pick !== false) && words.length && live.length === 1 && !sel.value) {
      sel.value = live[0].value;
      sel.setAttribute('data-auto-pick', sel.value);
      fireAuto(sel);
    }
    var n = document.getElementById(q.id + '-n');
    if (n) {
      n.hidden = !words.length;
      n.textContent = live.length ? '当たり ' + live.length + ' 件' + (live.length > 1 ? ' (下の欄から選ぶ)' : '') : '当たるグループはありません (言葉を短くする・「新しく作る」)';
    }
  }
  $$('.po-gq').forEach(function (q) {
    var selId = q.getAttribute('data-for'), sel = document.getElementById(selId);
    if (!sel) return;
    groupAll[selId] = $$('option', sel).filter(function (o) { return o.value !== ''; });
    // 変換中 (IME) は絞るだけで選ばない = 確定 (compositionend) の後に選ぶ (Codex R1 Medium: 「かみ」の変換中に「カミ」を選んで「紙」に確定しても残る)
    q.addEventListener('input', function (e) { groupFilter(selId, { pick: !e.isComposing }); });
    q.addEventListener('compositionend', function () { groupFilter(selId); });
    // 人が選ぶ欄で選んだ = 絞り込みが選んだ印を外す (打ち直しても取り消さない)
    sel.addEventListener('change', function (e) { if (!autoFiring) sel.removeAttribute('data-auto-pick'); });
    // 絞る欄の Enter = 登録 (form) に送らない。選べていれば選ぶ欄へ
    q.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' || e.isComposing || e.keyCode === 229) return;
      e.preventDefault();
      if (sel.value) sel.focus();
    });
  });

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
    // 🆕 ⑤-2: 新商品の登録の先方品番・入数 (欄がある画面だけ)
    if (document.getElementById('po-vendor_code')) { out.vendor_code = val('po-vendor_code'); out.qty_per_unit = val('po-qty_per_unit'); }
    if (all) return out;
    var empty = Object.keys(out).every(function (k) { return k === 'new_condition' || k === 'new_material' ? !out[k] : out[k] === ''; });
    return empty ? null : out;
  }
  /**
   * 新商品の登録の項目 [{ key, t, ok (値か入力なし), none (入力なし), focus }]。書けない (権限・読めない・見るだけ) = 空 (数えない・サーバーも数えない)。
   *   グループ: なし = 入力なし / 今あるグループ = 選んだら値 / 新しく作る = 名前・ID・決まり (・値) を入れたら値 (中身はサーバーが確かめる)
   */
  function items() {
    if (P.mode !== 'new' || !P.canWrite || !P.noneOn) return [];   // 数えるかはサーバーと同じ答え (noneOn)
    var N = ME.none;
    var chk = function (key, t, focus) {
      var none = !!N && N.on(key) && !N.has(key);
      return { key: key, t: t, ok: none || (!!N && N.has(key)), none: none, focus: focus };
    };
    var grp = function (key, segId, pickId, t, newOk, newFocus) {
      var m = segVal(segId);
      var ok = m === 'none' || (m === 'pick' && !!val(pickId)) || (m === 'new' && newOk());
      return { key: key, t: t, ok: ok, none: m === 'none', focus: m === 'pick' ? '#' + pickId : m === 'new' ? newFocus : '#' + segId + ' button' };
    };
    return [
      chk('order_settings.order_lot', '発注ロット', '#po-order_lot'),
      grp('order_settings.condition', 'po-cond-mode', 'po-condition_id', '発注条件グループ', function () { return !!(val('po-nc-name') && val('po-nc-id') && val('po-nc-type') && val('po-nc-value')); }, '#po-nc-name'),
      grp('order_settings.material', 'po-mat-mode', 'po-material_group_id', '原料グループ', function () { return !!(val('po-nm-name') && val('po-nm-id')); }, '#po-nm-name'),
      chk('order_settings.case_lot', 'ケースロット', '#po-case_lot'),
      chk('order_settings.case_group', 'ケースグループ', '#po-case_group'),
      chk('order_settings.capacity_per_unit', '容量 / 個', '#po-capacity_per_unit'),
    ];
  }
  ME.orderSettings = { collect: collect };
  ME.orderSettings.items = items;

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
    // 離れるときの確認 (me-shell.js) に発注の設定の未保存も出す (Company DB の欄の数に 1 足す)。🆕 先方品番の行の未保存も 1 行ずつ足す
    var base = ME.dirty;
    ME.dirty = function () {
      var d = base ? base() : { n: 0, items: [], impacts: [] };
      var items = d.items.slice(); var n = d.n;
      if (isDirty()) { n++; items.push('発注の設定 (発注アプリ): まだ保存していません'); }
      vRows().forEach(function (r) { if (vDirty(r)) { n++; items.push('先方品番 (仕入先 ' + r.getAttribute('data-sup') + '・発注アプリ): まだ保存していません'); } });
      return { n: n, items: items, impacts: d.impacts };
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
          // 保存したグループ = もう人が決めた値 (絞り込みが選んだ印を外す = 打ち直しても取り消さない・Codex R2 Medium)
          ['po-condition_id', 'po-material_group_id'].forEach(function (id) { var s = document.getElementById(id); if (s) s.removeAttribute('data-auto-pick'); });
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

  /* ---------- 🆕 ⑤-1 商品の画面: 先方品番・入数 (仕入先ごと・1 行ずつ保存) ---------- */
  function vRows() { return $$('.po-vrow', box); }
  function vVals(r) { var c = $('.po-v-code', r), q = $('.po-v-qpu', r); return JSON.stringify([c ? c.value.trim() : '', q ? q.value.trim() : '']); }
  function vDirty(r) { return r.getAttribute('data-init') != null && vVals(r) !== r.getAttribute('data-init'); }
  function vMsg(r, t, cls) { var m = $('.po-v-msg', r); if (m) { m.className = 'msgline po-v-msg ' + (cls || ''); m.textContent = t || ''; } }
  function vState(r) {
    var s = $('.po-v-save', r); var busyRow = r.getAttribute('data-busy') === '1';
    if (s) s.disabled = busyRow || !vDirty(r) || !$('.po-v-code', r).value.trim();
    var del = $('.po-v-del', r); if (del) del.disabled = busyRow;
    // 送っている間は欄も閉じる (送っていない値を「保存した」にしない・Codex #⑤ R1 M1)
    $$('input[data-v-edit]', r).forEach(function (inp) { inp.disabled = busyRow; });
    r.classList.toggle('po-dirty', vDirty(r));
    if (ME.setUnsaved && ME.dirty) ME.setUnsaved(ME.dirty().n);
  }
  function vPost(path, body) {
    return fetch(P.base + '/api/sku/' + encodeURIComponent(P.code) + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (x) { return x.json().catch(function () { return {}; }).then(function (j) { return { r: x, j: j }; }); });
  }
  function vSave(r) {
    if (r.getAttribute('data-busy') === '1') return;
    var b = r.getAttribute('data-base');
    r.setAttribute('data-busy', '1'); vState(r); vMsg(r, '発注アプリに保存しています…');
    vPost('/vendor-code', { request_id: uuid(), supplier_code: r.getAttribute('data-sup'), vendor_code: $('.po-v-code', r).value.trim(), qty_per_unit: $('.po-v-qpu', r).value.trim(), base_updated_at: b ? b : null })
      .then(function (x) {
        r.removeAttribute('data-busy');
        if (x.r.ok && x.j.ok) {
          if (x.j.row) {
            r.setAttribute('data-base', x.j.row.updated_at || '');
            $('.po-v-qpu', r).value = x.j.row.qty_per_unit == null ? '' : String(x.j.row.qty_per_unit);
          }
          r.setAttribute('data-init', vVals(r));
          var sb = $('.po-v-save', r); if (sb) sb.textContent = '保存';
          // 足した行 = もう対応表にある = 「消す」を出す
          if (sb && !$('.po-v-del', r)) {
            var del = document.createElement('button');
            del.type = 'button'; del.className = 'btn soft sm po-v-del'; del.textContent = '消す';
            del.addEventListener('click', function () { vDelete(r); });
            sb.parentNode.insertBefore(del, sb.nextSibling);
          }
          vMsg(r, '発注アプリに保存しました' + (x.j.warning ? ' · ⚠ ' + x.j.warning : ''), x.j.warning ? '' : 'ok');
          if (ME.toast) ME.toast('先方品番を保存しました (発注アプリ)');
        } else {
          vMsg(r, (x.j.error || ('HTTP ' + x.r.status)) + (x.j.conflict ? ' (画面を開き直すと今の値が出ます)' : ''), 'err');
        }
        vState(r);
      })
      .catch(function () { r.removeAttribute('data-busy'); vState(r); vMsg(r, '通信できませんでした。もう一度「保存」を押してください', 'err'); });
  }
  function vDelete(r) {
    if (r.getAttribute('data-busy') === '1') return;
    var b = r.getAttribute('data-base');
    if (!b) return;
    if (!window.confirm('仕入先 ' + r.getAttribute('data-sup') + ' の先方品番「' + ($('.po-v-code', r).value.trim() || '') + '」を対応表から消しますか? (発注書の CSV・入荷予定の変換で使えなくなります)')) return;
    r.setAttribute('data-busy', '1'); vState(r); vMsg(r, '発注アプリから消しています…');
    vPost('/vendor-code/delete', { request_id: uuid(), supplier_code: r.getAttribute('data-sup'), base_updated_at: b })
      .then(function (x) {
        r.removeAttribute('data-busy');
        if (x.r.ok && x.j.ok) {
          if (r.getAttribute('data-mine') === '1') {
            // 直せる仕入先の行 = 空の「足す」行に戻す (まだ無い = 版は null)
            r.setAttribute('data-base', '');
            $('.po-v-code', r).value = ''; $('.po-v-qpu', r).value = '';
            r.setAttribute('data-init', vVals(r));
            var sb = $('.po-v-save', r); if (sb) sb.textContent = '足す';
            var del = $('.po-v-del', r); if (del) del.parentNode.removeChild(del);
            vState(r);   // 欄を開き直す (送っている間に閉じた・Codex #⑤ 統合の確かめ P2)
            vMsg(r, '消しました', 'ok');
          } else {
            r.parentNode.removeChild(r);
          }
          if (ME.toast) ME.toast('先方品番を消しました (発注アプリ)');
          if (ME.setUnsaved && ME.dirty) ME.setUnsaved(ME.dirty().n);
          return;
        }
        vMsg(r, (x.j.error || ('HTTP ' + x.r.status)) + (x.j.conflict ? ' (画面を開き直すと今の値が出ます)' : ''), 'err');
        vState(r);
      })
      .catch(function () { r.removeAttribute('data-busy'); vState(r); vMsg(r, '通信できませんでした。もう一度「消す」を押してください', 'err'); });
  }
  if (P.mode === 'sku') {
    vRows().forEach(function (r) {
      r.setAttribute('data-init', vVals(r));
      $$('input', r).forEach(function (inp) { if (!inp.disabled) inp.setAttribute('data-v-edit', '1'); });
      r.addEventListener('input', function (e) { e.stopPropagation(); vState(r); });
      r.addEventListener('change', function (e) { e.stopPropagation(); vState(r); });
      var s = $('.po-v-save', r); if (s) s.addEventListener('click', function () { vSave(r); });
      var dl = $('.po-v-del', r); if (dl) dl.addEventListener('click', function () { vDelete(r); });
      // 欄の Enter = その行の保存 (ページの form に送らない)
      $$('input', r).forEach(function (inp) { inp.addEventListener('keydown', function (e) { if (e.key === 'Enter' && !e.isComposing && e.keyCode !== 229) { e.preventDefault(); var sb = $('.po-v-save', r); if (sb && !sb.disabled) vSave(r); } }); });
      vState(r);
    });
  }

  showModes();
  showUnit();
  filterConditions();
  initial = JSON.stringify(collect({ all: true }));
  changed();
  // 新商品の登録: 画面の JS はこの部品より先に読む = 読み終わったので発注の設定の項目も数え直してもらう (入力なし)
  if (P.mode === 'new' && typeof ME.recount === 'function') ME.recount();
})();
