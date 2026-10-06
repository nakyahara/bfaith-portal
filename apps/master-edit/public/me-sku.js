/*
 * me-sku.js — 1 つの商品の画面 (単品・セット) の動き
 *   - 未保存の数 = data-dirty-field を付けた欄だけ (表示の切り替え・絞る欄・保存の理由・見出しの部品は数えない)。
 *     セットの構成は「構成の中身どうし」(コード × 数の並び) をくらべる
 *   - 右の「保存すると変わること」= 欄ごとの 前 → 後 と NE への届き方、「保存するとこうなる」= 仕事への影響 (画面がもう持っている値から)
 *   - 保存の送り方は前と同じ (POST /api/sku/:code { request_id, reason, seen: { token, event_id }, values })。サーバーの確かめは変えていない
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var ME = window.MasterEdit = window.MasterEdit || {};
  var dataEl = document.getElementById('me-page');
  var form = document.getElementById('f');
  var scope = document.getElementById('sku-page');   // 見出しの「取扱」も欄 (form の外) = 画面全体で数える
  if (!dataEl || !form || !scope) return;
  var P = JSON.parse(dataEl.textContent);
  var D = form.dataset;
  var isSet = P.kind === 'set';
  var canSave = D.canSave === '1';
  var BASE = P.base;
  var yen = function (v) { return v == null || v === '' || isNaN(Number(v)) ? '' : Number(v).toLocaleString('ja-JP'); };
  var half = function (s) { return String(s == null ? '' : s).replace(/[０-９．，、]/g, function (c) { return c === '，' || c === '、' ? ',' : String.fromCharCode(c.charCodeAt(0) - 0xFEE0); }); };
  var HANDLING = { active: '取扱中', discontinued: '中止', unknown: '不明' };

  /* ---------- 欄の値 ---------- */
  var compTable = document.getElementById('comp');
  var compEditable = !!compTable && compTable.getAttribute('data-editable') === '1';
  function components() {
    return $$('#comp-rows tr.comp-row').map(function (tr) { return { code: $('.c-code', tr).value.trim(), qty: $('.c-qty', tr).value.trim() }; })
      .filter(function (r) { return r.code || r.qty; });
  }
  function valueOf(el) {
    if (el.id === 'comp') return JSON.stringify(components());
    if (el.classList.contains('seg')) return el.getAttribute('data-value') || '';
    if (el.type === 'checkbox') return el.checked ? '1' : '';
    return el.value;
  }
  function tracked() { return $$('[data-dirty-field]', scope).filter(function (el) { return !el.disabled; }); }
  var initial = new Map();
  tracked().forEach(function (el) { initial.set(el, valueOf(el)); });
  var initialCompHtml = $('#comp-rows') ? $('#comp-rows').innerHTML : null;
  var saved = false;      // 保存できた後 (読み直すまで、もう未保存と数えない)

  function dirtyKeys() {
    var keys = [];
    tracked().forEach(function (el) {
      var k = el.getAttribute('data-dirty-field');
      if (keys.indexOf(k) >= 0) return;
      if (initial.has(el) && valueOf(el) !== initial.get(el)) keys.push(k);
    });
    // JAN の欄に打ったまま (Enter・カンマ・欄を離れる前) も未保存に数える = 保存の前に札にする (#1589 Codex R2 M1)
    if (janPending() && keys.indexOf('jan') < 0) keys.push('jan');
    return keys;
  }
  function firstEl(k) { return tracked().filter(function (el) { return el.getAttribute('data-dirty-field') === k; })[0]; }
  function segText(seg, v) { var b = $$('button', seg).filter(function (x) { return x.getAttribute('data-v') === v; })[0]; return b ? b.textContent.replace(/\s+/g, ' ').trim() : (v ? v : '未入力'); }
  function showVal(el, v) {
    if (el.classList.contains('seg')) return segText(el, v);
    if (el.tagName === 'SELECT') { var o = $$('option', el).filter(function (x) { return x.value === v; })[0]; return o ? o.textContent : v; }
    if (v === '' || v == null) return '(空)';
    var unit = el.getAttribute('data-unit') || '';
    if (unit === ' 円' && /^\d+$/.test(half(v).trim())) return yen(half(v).trim()) + unit;
    return v + unit;
  }
  var compText = function (rows) { return rows.length ? rows.map(function (r) { return r.code + '×' + r.qty; }).join(', ') : 'なし'; };
  var janList = function (s) { return String(s || '').split(/[\s,、，]+/).map(function (x) { return half(x).trim(); }).filter(Boolean); };

  /** 欄ごとの 前 → 後 */
  function diffItems() {
    return dirtyKeys().map(function (k) {
      var el = firstEl(k);
      var label = el.getAttribute('data-label') || k, ne = el.getAttribute('data-ne') || '';
      if (k === 'cost') {
        return { k: k, label: '原価 (今日 ' + P.todayLabel + ' から)', from: P.costNow == null ? '未入力' : yen(P.costNow) + ' 円', to: yen(half($('#cost-jpy').value)) + ' 円', ne: ne };
      }
      if (k === 'exception_cost') {
        var clear = $('#xcost-clear') && $('#xcost-clear').checked;
        return { k: k, label: '例外原価 (今日から)', from: P.xcostNow == null ? '未入力' : yen(P.xcostNow) + ' 円', to: clear ? 'やめる (構成品の合計に戻す)' : yen(half($('#xcost-jpy').value)) + ' 円', ne: ne };
      }
      if (k === 'jan') {
        var a = janList(initial.get(el)), b = janList(el.value).concat(janList(janPending()));
        var add = b.filter(function (x) { return a.indexOf(x) < 0; }), rm = a.filter(function (x) { return b.indexOf(x) < 0; });
        return { k: k, label: 'JAN', from: '', to: [add.length ? '足す ' + add.join('・') : '', rm.length ? '外す ' + rm.join('・') : ''].filter(Boolean).join(' / ') || '並びだけ', ne: ne };
      }
      if (k === 'components') {
        return { k: k, label: '構成の依頼', from: compText(JSON.parse(initial.get(el) || '[]')), to: compText(components()), ne: ne };
      }
      return { k: k, label: label, from: showVal(el, initial.get(el)), to: showVal(el, valueOf(el)), ne: ne };
    });
  }

  /** 保存するとこうなる (仕事への影響)。[warn?, 文] */
  function impacts(keys) {
    var out = [];
    var has = function (k) { return keys.indexOf(k) >= 0; };
    var sets = (P.usedIn || []).map(function (u) { return u.code; });
    var hv = function (k) { var el = firstEl(k); return el ? valueOf(el) : null; };
    if (!isSet && has('handling') && hv('handling') === 'discontinued') {
      if (sets.length) out.push([1, 'この商品を使うセット ' + sets.join('・') + ' も中止になります (構成品が 1 つでも中止ならセットも中止)']);
      if (P.amazon) out.push([1, 'この商品を使う Amazon SKU が ' + P.amazon + ' 件あります (Amazon SKU の対応はそのままです)']);
      if (!(P.reg === 'draft' || P.reg === 'ne_pending')) out.push([0, 'NE の取扱区分は、翌朝の照合で NE との差になり CSV で入れます']);
    }
    if (!isSet && has('handling') && hv('handling') === 'active' && P.handling === 'discontinued' && sets.length) out.push([0, 'セット ' + sets.join('・') + ' の取扱も計算し直します (ほかの構成品も取扱中なら取扱中に戻ります)']);
    if (!isSet && has('tax_rate') && sets.length) out.push([0, 'この商品を使うセット ' + sets.join('・') + ' の税率も計算し直します (変わったら「NE でやること」に出ます)']);
    if (!isSet && has('cost')) {
      var nv = Number(half($('#cost-jpy').value));
      if (!isNaN(nv)) out.push([0, P.todayLabel + ' から この商品の原価は ' + yen(nv) + ' 円 (いまの ' + (P.costNow == null ? '未入力' : yen(P.costNow) + ' 円') + ' は昨日で終わり)']);
      if (sets.length) out.push([0, 'セット ' + sets.join('・') + ' の原価 (構成品の合計) も今日から計算し直します']);
    }
    if (!isSet && has('jan')) out.push([0, 'JAN はロジザード・product-hub でも使います。NE の JAN は NE の画面で直してください']);
    var neCsv = keys.filter(function (k) { var el = firstEl(k); return el && el.getAttribute('data-ne') === 'NE へ CSV' && k !== 'handling'; });
    var notInNe = P.reg === 'draft' || P.reg === 'ne_pending';   // 新商品で NE にまだ無い = NE への登録は NE 登録の CSV
    if (neCsv.length && notInNe) out.push([0, '新商品なので、NE への登録は「NE 登録の CSV」で行います (翌朝の照合の差にはなりません)']);
    else if (neCsv.length) out.push([0, neCsv.map(function (k) { return k === 'cost' ? '原価' : (firstEl(k).getAttribute('data-label') || k); }).join('・') + ' は、翌朝の照合で NE との差になり、マスタの判断の CSV で NE に入れます']);
    var regTouched = keys.filter(function (k) { return (P.regFields || []).indexOf(k) >= 0; });
    if (regTouched.length && (P.regBuilt || []).length) out.push([1, 'まだ配っていない NE 登録の CSV (ファイル ' + P.regBuilt.map(function (x) { return '#' + x; }).join('・') + ') は「使わない」になります (作り直してください)']);
    if (isSet && has('components')) {
      var now = JSON.stringify(components().map(function (r) { return [r.code.toLowerCase(), String(Number(half(r.qty)))]; }));
      var cur = JSON.stringify((P.currentComps || []).map(function (r) { return [String(r.code).toLowerCase(), String(r.qty)]; }));
      if (now === cur && P.request) out.push([0, '今の構成に戻すので、開いている構成の依頼を取り下げます']);
      else out.push([1, '構成はすぐには変わりません。「NE でやること」の依頼になり、NE の構成が同じになったのを確かめてから今の構成になります']);
    }
    if (isSet && has('handling_own')) out.push([hv('handling_own') === 'discontinued' ? 1 : 0, 'セットの取扱を計算し直します (' + (hv('handling_own') === 'discontinued' ? 'セットは中止になります' : '構成品が全部 取扱中なら取扱中') + ')。NE の画面で取扱区分を確かめてください']);
    if (isSet && has('exception_cost')) out.push([0, $('#xcost-clear') && $('#xcost-clear').checked ? '今日から原価は構成品の合計に戻ります' : '今日から原価は構成品の合計の代わりに、この例外原価を使います']);
    return out;
  }

  /** 理由が要るとき (中止は画面の決まり・原価と例外原価はサーバーの決まり) */
  function needReasons(keys) {
    var out = [];
    var reason = $('#reason') ? $('#reason').value.trim() : '';
    var el = firstEl(isSet ? 'handling_own' : 'handling');
    if (keys.indexOf(isSet ? 'handling_own' : 'handling') >= 0 && el && valueOf(el) === 'discontinued' && !reason) out.push({ t: '取扱を中止にするときは理由が要ります (下の「理由」)', focus: '#reason' });
    if (keys.indexOf('cost') >= 0 && !costReason().trim()) out.push({ t: '原価を変える理由で「その他」を選んだときは、理由を書いてください', focus: '#cost-reason' });
    if (keys.indexOf('exception_cost') >= 0 && !$('#xcost-reason').value.trim()) out.push({ t: '例外原価を変えるときは、その「理由」が要ります', focus: '#xcost-reason' });
    return out;
  }

  /* ---------- 保存の箱を描く ---------- */
  var saveBtn = $('#save'), revertBtn = $('#revert');
  function update() {
    var keys = saved ? [] : dirtyKeys();
    var items = saved ? [] : diffItems();
    $$('.f[data-row]', scope).forEach(function (f) { f.classList.toggle('dirty', keys.indexOf(f.getAttribute('data-row')) >= 0); });
    $$('.seg[data-dirty-field]', scope).forEach(function (s) { s.classList.toggle('dirty', keys.indexOf(s.getAttribute('data-dirty-field')) >= 0); });
    var hd = $('.handling-top'); if (hd) hd.classList.toggle('dirty', keys.indexOf('handling') >= 0);
    $('#save-diff').innerHTML = items.map(function (it) {
      return '<li><div class="k"><span>' + esc(it.label) + '</span>' + (it.ne ? '<span class="ne">' + esc(it.ne) + '</span>' : '') + '</div><div class="v">'
        + (it.from ? '<span class="from">' + esc(it.from) + '</span><span class="muted" aria-label="から">→</span>' : '') + '<span class="to">' + esc(it.to) + '</span></div></li>';
    }).join('');
    $('#save-empty').hidden = items.length > 0;
    var cnt = $('#save-count'); cnt.textContent = items.length + ' 件'; cnt.className = 'b count ' + (items.length ? 'warn' : 'mute');
    var imp = saved ? [] : impacts(keys);
    $('#save-impact').hidden = imp.length === 0;
    $('#save-impact-list').innerHTML = imp.map(function (x) { return '<li class="' + (x[0] ? 'warn' : '') + '">' + esc(x[1]) + '</li>'; }).join('');
    var need = saved ? [] : needReasons(keys);
    var nr = $('#needreason');
    if (nr) { nr.hidden = !need.length; $('#needreason-t').textContent = need.map(function (x) { return x.t; }).join(' / '); }
    var rl = $('#reason-lab');
    if (rl) { var stop = keys.indexOf(isSet ? 'handling_own' : 'handling') >= 0 && valueOf(firstEl(isSet ? 'handling_own' : 'handling')) === 'discontinued'; rl.textContent = stop ? '理由 (中止のときは必要)' : '理由 (なくてもよい)'; }
    // mustReload = 開き直しが要る 409 の後。欄や理由を触っても、古い編集の印のまま保存のボタンを戻さない (#1589 Codex R1 M2)
    if (saveBtn) saveBtn.disabled = saved || busy || mustReload || items.length === 0 || need.length > 0;
    if (revertBtn) revertBtn.disabled = saved || items.length === 0;
    ME.setUnsaved(items.length);
    lastState = { n: items.length, items: items.map(function (it) { return it.label + ': ' + (it.from ? it.from + ' → ' : '') + it.to; }), impacts: imp.map(function (x) { return x[1]; }), need: need };
  }
  var lastState = { n: 0, items: [], impacts: [], need: [] };
  var busy = false;
  var mustReload = false;
  ME.dirty = function () { return { n: lastState.n, items: lastState.items, impacts: lastState.impacts }; };
  scope.addEventListener('input', update);
  scope.addEventListener('change', update);

  /* ---------- 原価を変える ---------- */
  var costOpen = $('#btn-cost-open'), costBox = $('#cost-add');
  function costDelta() {
    var el = $('#cost-delta'); if (!el) return;
    var v = Number(half($('#cost-jpy').value));
    if ($('#cost-jpy').value.trim() === '' || isNaN(v) || P.costNow == null) { el.innerHTML = ''; return; }
    var d = v - P.costNow, pct = P.costNow ? Math.round((d / P.costNow) * 1000) / 10 : null;
    el.innerHTML = '<span class="hint">いまの ' + yen(P.costNow) + ' 円から</span> <span class="delta ' + (d >= 0 ? 'up' : 'down') + '">' + (d >= 0 ? '+' : '−') + yen(Math.abs(d)) + ' 円' + (pct == null ? '' : ' (' + (d >= 0 ? '+' : '−') + Math.abs(pct) + '%)') + '</span>';
  }
  /**
   * 原価を変える理由 (10/5 中原さん: ふだんはメーカーからの値上げ通知 = 選ぶだけ)。選んだ文そのものを送る・「その他」は書いた文。
   * 選ぶ部品は未保存に数えない (data-dirty-field を付けない = 今までの理由の欄と同じ)
   */
  function costReason() {
    var p = $('input[name="cost-reason-pick"]:checked');
    var other = $('#cost-reason');
    if (!p) return other ? other.value : '';
    return p.hasAttribute('data-other') ? (other ? other.value.trim() : '') : p.value;
  }
  function costReasonShow(focus) {
    var p = $('input[name="cost-reason-pick"]:checked'), other = $('#cost-reason');
    if (!other) return;
    other.hidden = !(p && p.hasAttribute('data-other'));
    if (!other.hidden && focus) other.focus();
  }
  function costReasonReset() {
    var first = $('input[name="cost-reason-pick"]');
    if (first) first.checked = true;
    if ($('#cost-reason')) $('#cost-reason').value = '';
    costReasonShow(false);
  }
  $$('input[name="cost-reason-pick"]').forEach(function (r) { r.addEventListener('change', function () { costReasonShow(true); update(); }); });
  if ($('#cost-reason')) $('#cost-reason').addEventListener('input', update);
  if (costOpen && costBox) {
    costOpen.addEventListener('click', function () { costBox.hidden = false; costOpen.setAttribute('aria-expanded', 'true'); $('#cost-jpy').focus(); });
    $('#cost-cancel').addEventListener('click', function () { $('#cost-jpy').value = ''; costReasonReset(); costDelta(); costBox.hidden = true; costOpen.setAttribute('aria-expanded', 'false'); costOpen.focus(); update(); });
    $('#cost-jpy').addEventListener('input', costDelta);
  }

  /* ---------- JAN の札 ---------- */
  var janTokens = $('#jan-tokens'), janIn = $('#jan-in'), janHidden = $('#f-jan'), janMsg = $('#jan-msg');
  function janValid(s) {
    if (!/^(\d{8}|\d{13})$/.test(s)) return false;
    var d = s.split('').map(Number), check = d.pop();
    var sum = d.reverse().reduce(function (a, x, i) { return a + x * (i % 2 === 0 ? 3 : 1); }, 0);
    return (10 - (sum % 10)) % 10 === check;
  }
  function janNow() { return $$('.tok', janTokens).map(function (t) { return t.getAttribute('data-jan'); }); }
  function janSync() { janHidden.value = janNow().join(', '); janHidden.dispatchEvent(new Event('input', { bubbles: true })); }
  function janSay(t, cls) { janMsg.className = 'fmsg ' + (cls || ''); janMsg.textContent = t; }
  function janTok(j, isNew) {
    var s = document.createElement('span'); s.className = 'tok' + (isNew ? ' new' : ''); s.setAttribute('data-jan', j);
    s.innerHTML = '<svg class="ic" aria-hidden="true"><use href="#i-check"/></svg>' + esc(j) + '<button type="button" aria-label="JAN ' + esc(j) + ' を外す">×</button>';
    return s;
  }
  /** 欄に打ったまま (まだ札にしていない) JAN */
  function janPending() { return janIn && janIn.value.trim() ? janIn.value.trim() : ''; }
  /** 打った JAN を札にする。札にできない (形が違う・5 つを超える) = false (欄は残す = 黙って捨てない) */
  function janAdd(raw) {
    var list = String(raw || '').split(/[\s,、，]+/).map(function (x) { return half(x).trim(); }).filter(Boolean);
    if (!list.length) return true;
    var bad = list.filter(function (x) { return !janValid(x); });
    if (bad.length) { janSay('JAN ' + bad.join('・') + ' は 8 桁か 13 桁で、チェック数字が合いません', 'err'); return false; }
    var now = janNow();
    var adds = list.filter(function (j, i) { return now.indexOf(j) < 0 && list.indexOf(j) === i; });
    if (now.length + adds.length > 5) { janSay('JAN は 5 つまでです (いま ' + now.length + ' つ)', 'err'); return false; }
    adds.forEach(function (j) { janTokens.insertBefore(janTok(j, true), janIn); });
    janSay('足しました (保存すると入ります)', 'ok');
    janIn.value = ''; janSync();
    return true;
  }
  if (janTokens && janIn && janHidden) {
    janIn.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' || e.key === ',' || e.key === '、') { e.preventDefault(); janAdd(janIn.value); }
      else if (e.key === 'Backspace' && !janIn.value) { var toks = $$('.tok', janTokens); if (toks.length) { toks[toks.length - 1].querySelector('button').focus(); } }
    });
    janIn.addEventListener('blur', function () { if (janIn.value.trim()) janAdd(janIn.value); });
    janTokens.addEventListener('click', function (e) {
      var b = e.target.closest('.tok button'); if (!b) return;
      var tok = b.closest('.tok'); var j = tok.getAttribute('data-jan'); tok.remove(); janSay('JAN ' + j + ' を外しました (保存すると外れます)', 'warn'); janSync(); janIn.focus();
    });
  }

  /* ---------- セットの構成 ---------- */
  var setMode = $('#set-mode');
  if (setMode) setMode.addEventListener('change', function (e) {
    e.stopPropagation();   // 表示の切り替えは未保存に数えない
    var v = setMode.getAttribute('data-value');
    $$('[data-mode]').forEach(function (d) { d.hidden = d.getAttribute('data-mode') !== v; });
    if (v === 'edit') { var first = $('#comp-rows .c-code:not([disabled])'); if (first) first.focus(); }
  });
  var rowsEl = $('#comp-rows');
  /** 行番号と読み上げの名前を今の並び・コードに合わせ直す (並べ替え・足す・外す・コードを変えたとき。#1589 Codex R1 L5) */
  function relabel(tr, i) {
    var code = $('.c-code', tr).value.trim();
    var who = (i + 1) + ' 行目' + (code ? ' (' + code + ')' : '');
    $('.no', tr).textContent = String(i + 1);
    $('.c-code', tr).setAttribute('aria-label', (i + 1) + ' 行目の構成品のコード');
    $('.c-qty', tr).setAttribute('aria-label', who + ' の数');
    var acts = { up: 'を上へ', down: 'を下へ', del: 'を外す' };
    $$('button[data-act]', tr).forEach(function (b) { b.setAttribute('aria-label', who + ' ' + acts[b.getAttribute('data-act')]); });
  }
  function renumber() { $$('#comp-rows tr.comp-row').forEach(relabel); update(); }
  function lookup(tr) {
    var code = $('.c-code', tr).value.trim();
    var set = function (cls, v, bad) { var el = $(cls, tr); el.textContent = v; if (cls === '.c-name') el.classList.toggle('bad', !!bad); };
    if (!code) { set('.c-name', ''); return; }
    fetch(BASE + '/api/lookup?code=' + encodeURIComponent(code), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (!x.r.ok || !x.j.ok) { set('.c-name', x.j.error || '見つかりません', true); ['.c-tax', '.c-sales', '.c-cost', '.c-handling'].forEach(function (c) { set(c, ''); }); return; }
        var it = x.j.item;
        set('.c-name', it.name + (it.kind !== 'single' ? ' (単品でない = 構成品にできません)' : ''), it.kind !== 'single');
        set('.c-tax', it.tax_rate == null ? '未' : String(Math.round(it.tax_rate * 100)));
        set('.c-sales', it.sales_class == null ? '未' : String(it.sales_class));
        set('.c-cost', it.cost_jpy == null ? '未' : yen(it.cost_jpy));
        set('.c-handling', HANDLING[it.handling] || it.handling);
      })
      .catch(function () { set('.c-name', '引き当てできません (通信)', true); });
  }
  if (rowsEl && compEditable) {
    rowsEl.addEventListener('click', function (e) {
      var b = e.target.closest('button[data-act]'); if (!b || b.disabled) return;
      var tr = b.closest('tr'), act = b.getAttribute('data-act');
      if (act === 'del') { var next = tr.nextElementSibling || tr.previousElementSibling; tr.remove(); if (next) $('.c-code', next).focus(); else $('#comp-add').focus(); }
      if (act === 'up' && tr.previousElementSibling) { tr.parentNode.insertBefore(tr, tr.previousElementSibling); b.focus(); }
      if (act === 'down' && tr.nextElementSibling) { tr.parentNode.insertBefore(tr.nextElementSibling, tr); b.focus(); }
      renumber();
    });
    rowsEl.addEventListener('change', function (e) { if (e.target.classList.contains('c-code')) { var tr = e.target.closest('tr'); relabel(tr, $$('#comp-rows tr.comp-row').indexOf(tr)); lookup(tr); } });
    $('#comp-add').addEventListener('click', function () {
      if ($$('#comp-rows tr.comp-row').length >= (P.maxComponents || 20)) { ME.toast('構成品は ' + (P.maxComponents || 20) + ' 品までです'); return; }
      rowsEl.appendChild($('#comp-tpl').content.firstElementChild.cloneNode(true));
      renumber();
      $('.c-code', rowsEl.lastElementChild).focus();
    });
  }

  /* ---------- 名前を見出しのところで直す (10/6) ---------- */
  // 名前の欄は見出しの中の 1 つだけ (data-field="name")。✎ で見出しを入力欄に切り替える・変えた間は開いたまま・Esc で直す前に戻す
  var nameBox = $('#name-box'), nameIn = $('#f-name'), nameBtn = $('#name-edit'), titleEl = $('#sku-title');
  function openName(focus) {
    if (!nameBox || !nameIn) return;
    nameBox.hidden = false; if (titleEl) titleEl.classList.add('sr'); if (nameBtn) { nameBtn.hidden = true; nameBtn.setAttribute('aria-expanded', 'true'); }
    if (focus) { var ph = $('#sku-ph'); if (ph) ph.scrollIntoView({ behavior: 'smooth', block: 'start' }); nameIn.focus({ preventScroll: true }); nameIn.select(); }
  }
  function closeName() {
    if (!nameBox) return;
    if (nameIn && initial.has(nameIn) && nameIn.value !== initial.get(nameIn)) { ME.toast('変えた名前は、保存するか Esc で戻すまで開いたままです'); return; }   // 変えた間は閉じない (黄色の欄を見せたまま)
    nameBox.hidden = true; if (titleEl) titleEl.classList.remove('sr'); if (nameBtn) { nameBtn.hidden = false; nameBtn.setAttribute('aria-expanded', 'false'); nameBtn.focus(); }
  }
  ME.openName = openName;
  if (nameBtn) nameBtn.addEventListener('click', function () { openName(true); });
  $$('[data-name-open]').forEach(function (b) { b.addEventListener('click', function () { openName(true); }); });
  /** 名前の誤りの印 (欄の aria-invalid・誤りの文との結び・行の赤) をまとめて外す = 打ち直し・Esc・元に戻すのどれでも (#1631 Codex R2 L1) */
  function clearNameError() {
    if (nameIn) { nameIn.removeAttribute('aria-invalid'); nameIn.removeAttribute('aria-describedby'); }
    var nr = $('[data-row="name"]', scope); if (nr) nr.classList.remove('err');
  }
  if (nameIn) nameIn.addEventListener('input', clearNameError);
  var nameClose = $('#name-close');
  if (nameClose) nameClose.addEventListener('click', closeName);
  if (nameIn) nameIn.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      if (e.isComposing || e.keyCode === 229) return;   // 日本語入力の変換中の Esc = 変換の取り消し (名前は戻さない・#1631 Codex R1 M1)
      e.preventDefault(); e.stopPropagation(); if (initial.has(nameIn)) nameIn.value = initial.get(nameIn); clearNameError(); update(); closeName();
    }
  });

  /* ---------- 元に戻す ---------- */
  function revert() {
    tracked().forEach(function (el) {
      if (!initial.has(el)) return;
      var v = initial.get(el);
      if (el.id === 'comp') return;
      if (el.classList.contains('seg')) {
        el.setAttribute('data-value', v);
        $$('button', el).forEach(function (b) { var on = b.getAttribute('data-v') === v; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false'); });
      } else if (el.type === 'checkbox') el.checked = v === '1';
      else el.value = v;
    });
    if (initialCompHtml != null && $('#comp-rows')) $('#comp-rows').innerHTML = initialCompHtml;
    if (janTokens) { $$('.tok', janTokens).forEach(function (t) { t.remove(); }); janList(janHidden.value).forEach(function (j) { janTokens.insertBefore(janTok(j, false), janIn); }); janIn.value = ''; janSay(''); }
    costReasonReset();
    if ($('#xcost-reason')) $('#xcost-reason').value = '';
    if (costBox) { costBox.hidden = true; if (costOpen) costOpen.setAttribute('aria-expanded', 'false'); costDelta(); }
    $$('.f.err', scope).forEach(function (f) { f.classList.remove('err'); });
    clearNameError();
    update();
    if (nameBox && !nameBox.hidden) closeName();
    ME.toast('変更を元に戻しました');
  }
  if (revertBtn) revertBtn.addEventListener('click', revert);

  /* ---------- 保存 (送り方は前と同じ) ---------- */
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  // 保存 1 回に 1 つ (通信が切れて押し直しても 2 回入らない)。返事が来た後にもう一度保存するときは新しい番号
  var requestId = uuid();
  var FIELDS = isSet ? ['name', 'standard_price', 'set_sales_class_override', 'handling_own', 'shipping_code', 'reorder_months']
    : ['name', 'handling', 'parent_code', 'standard_price', 'tax_rate', 'sales_class', 'primary_supplier', 'shipping_code', 'reorder_months', 'jan'];
  function collect() {
    var values = {};
    FIELDS.forEach(function (f) {
      var el = $('[data-field="' + f + '"]', scope);
      if (!el || el.disabled) return;   // 🔒 の欄・切替前の欄は送らない (今のまま)
      var v = valueOf(el);
      if (el.classList.contains('seg') && (v === '' || v === 'unknown')) return;   // まだ選んでいない = 送らない (今のまま)
      values[f] = v;
    });
    if (isSet && compEditable && JSON.stringify(components()) !== initial.get(compTable)) values.components = components();
    if (!isSet) {
      var cj = $('#cost-jpy');
      if (cj && !cj.disabled && cj.value.trim()) values.cost = { jpy: cj.value.trim(), reason: costReason() };
    } else {
      var xj = $('#xcost-jpy'), xc = $('#xcost-clear');
      if (xj && !xj.disabled) {
        if (xc && xc.checked) values.exception_cost = { clear: true, reason: $('#xcost-reason').value };
        else if (xj.value.trim()) values.exception_cost = { jpy: xj.value.trim(), reason: $('#xcost-reason').value };
      }
    }
    return values;
  }
  var NE = { csv: 'NE へ CSV', manual: 'NE の画面で', none: 'NE へ送らない' };
  var COL = { tax_rate: '税率', handling: '取扱区分', cost: '原価' };
  function show(v) {
    if (v === null || v === undefined || v === '') return '(空)';
    if (typeof v === 'object') return v.rate !== undefined ? (v.rate == null ? '未決' : Math.round(v.rate * 100) + '%') + ' ' + (v.class || '') : JSON.stringify(v);
    return String(v);
  }
  /** DB の時刻の文字 ('2026-10-02 01:54:00+00') → 東京の「10/2 (金) 10:54」 */
  function jst(v) {
    var m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?\s*(Z|[+-]\d{2}(?::?\d{2})?)$/.exec(String(v || ''));
    if (!m) return String(v || '');
    var ms = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +(m[6] || 0));
    if (m[7] !== 'Z') { var sg = m[7][0] === '-' ? -1 : 1, dg = m[7].slice(1).replace(':', ''); ms -= sg * (Number(dg.slice(0, 2)) * 60 + Number(dg.slice(2, 4) || 0)) * 60000; }
    var d = new Date(ms + 9 * 3600000);
    return (d.getUTCMonth() + 1) + '/' + d.getUTCDate() + ' (' + '日月火水木金土'[d.getUTCDay()] + ') ' + String(d.getUTCHours()).padStart(2, '0') + ':' + String(d.getUTCMinutes()).padStart(2, '0');
  }
  var ATTR = { name: '名前', standard_price_jpy: '標準売価', tax_rate: '税率', tax_class: '税区分', handling: '取扱区分', handling_own: 'セット自身の取扱', shipping_code: '送料コード', shipping_method: '配送方法', shipping_cost_jpy: '送料', reorder_months: '推奨保有月数', set_sales_class_override: '売上分類の上書き', sales_class: '売上分類', status: '取扱区分 (商品)', parent_product_id: '代表 (親)', parent_set_by: '代表の決め方', is_primary: '代表の仕入先', valid_to: '終わりの日', cost_jpy: '原価', qty: '数量' };
  var ENT = { sku: '', product: '商品の', sku_component: '構成の', sku_cost: '原価の', supplier_sku: '仕入先の', external_id: 'JAN の' };
  function li(a) { return a.map(function (x) { return '<li>' + x + '</li>'; }).join(''); }
  function msg(t, cls) { var m = $('#msg'); m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; }
  function showResult(j) {
    var r = $('#result');
    if (j.no_change) { r.innerHTML = '<div class="result"><div class="rt">変わった項目がありません (何も保存していません)</div></div>'; return; }
  }
  /** 保存が通った返事の中身 (変えた項目・計算し直した値・NE でやること・気をつけること) */
  function changeText(c) { var hf = c.field === 'handling' || c.field === 'handling_own'; return String(c.label) + ': ' + show(hf ? HANDLING[c.from] || c.from : c.from) + ' → ' + show(hf ? HANDLING[c.to] || c.to : c.to); }
  function changeLine(c) { return esc(changeText(c)); }
  function savedHtml(j) {
    var changed = Array.isArray(j.changed) ? j.changed : [];
    return '<div class="rt"><svg class="ic s" aria-hidden="true"><use href="#i-check"/></svg> 保存しました' + (j.replayed ? ' (前に保存した結果)' : '') + ' · 画面は保存した後の値です</div>'
      + (changed.length ? '<ul>' + li(changed.map(function (c) { return changeLine(c) + ' <span class="muted">(' + esc(NE[c.ne] || c.ne) + ')</span>'; })) + '</ul>' : '')
      + (j.derived && j.derived.length ? '<div class="sec2">構成品から計算し直した値</div><ul>' + li(j.derived.map(function (d) { var hf = d.col === 'handling'; return esc(d.code) + ' の ' + esc(COL[d.col] || d.col) + ': ' + esc(show(hf ? HANDLING[d.from] || d.from : d.from)) + ' → ' + esc(show(hf ? HANDLING[d.to] || d.to : d.to)); })) + '</ul>' : '')
      + (j.ne_steps && j.ne_steps.length ? '<div class="sec2">NE でやること</div><ul>' + li(j.ne_steps.map(esc)) + '</ul>' : '')
      + (j.warnings && j.warnings.length ? '<div class="sec2">気をつけること</div><ul>' + li(j.warnings.map(esc)) + '</ul>' : '')
      + '<button type="button" class="btn ghost sm" id="saved-note-close">閉じる</button>';
  }
  /*
   * 保存が通った後は画面を読み直す (10/5 中原さん「保存したらすぐに保存箇所も変わるように」)。
   * 読み直す前に返事を sessionStorage に置き、読み直した画面の上に 1 回だけ出す (置けない・壊れている = 出さないだけ。画面の値は読み直しで正しい)
   */
  var NOTE_KEY = 'master-edit:saved-note';
  function keepSavedNote(j) {
    try {
      sessionStorage.setItem(NOTE_KEY, JSON.stringify({ code: P.code, at: Date.now(), j: { replayed: !!j.replayed, changed: j.changed || [], derived: j.derived || [], ne_steps: j.ne_steps || [], warnings: j.warnings || [] } }));
    } catch (e) { /* 置けない (プライベートのウィンドウなど) = 知らせを出さないだけ */ }
  }
  function showSavedNote() {
    var n = null;
    try { var raw = sessionStorage.getItem(NOTE_KEY); if (raw) { sessionStorage.removeItem(NOTE_KEY); n = JSON.parse(raw); } } catch (e) { return; }
    if (!n || !n.j || n.code !== P.code || !(Date.now() - Number(n.at) < 5 * 60000)) return;
    var box = document.createElement('div');
    box.id = 'saved-note'; box.className = 'result ok saved-note'; box.setAttribute('role', 'status');
    box.innerHTML = savedHtml(n.j);
    var ph = $('#sku-ph'); if (ph && ph.parentNode) ph.parentNode.insertBefore(box, ph.nextSibling); else form.parentNode.insertBefore(box, form);
    $('#saved-note-close').addEventListener('click', function () { box.remove(); });
    var ch = Array.isArray(n.j.changed) ? n.j.changed : [];
    ME.toast('保存しました' + (ch.length ? ' (' + changeText(ch[0]) + (ch.length > 1 ? ' ほか ' + (ch.length - 1) + ' 件' : '') + ')' : ''));
  }
  // 先の日付の原価 (cost_future / set_cost_future) は入れない = 画面を開いたときに分かっている状態で、該当する原価の欄を閉じてある。
  // その間に入った = 編集の印が変わる = version_conflict で返る (#1589 Codex R2 M2)。登録をその間にやめた = cancelled_sku (M3)
  var REOPEN = ['version_conflict', 'request_id_reused', 'retry', 'processing', 'abandoned', 'reg_csv_issued', 'csv_issued', 'before_cutover', 'cancelled_sku'];
  function showError(j, status) {
    var r = $('#result');
    var h = '<div class="result err" role="alert" id="save-err"><div class="rt">' + esc(j.error || ('HTTP ' + status)) + '</div>';
    if (j.reason === 'before_cutover' && j.fields) h += '<div class="muted">切替前の項目: ' + esc(j.fields.join('・')) + '</div>';
    if (j.reason === 'set_underivable' && Array.isArray(j.blockers)) h += '<ul>' + li(j.blockers.map(esc)) + '</ul>';
    if (j.reason === 'version_conflict' && Array.isArray(j.events)) {
      h += '<div class="sec2">その間の変更</div><ul>' + li(j.events.map(function (e) { return esc(jst(e.recorded_at)) + ' ' + esc((ENT[e.entity_type] || '') + (e.attribute ? (ATTR[e.attribute] || e.attribute) : ({ INSERT: '追加', DELETE: '削除' }[e.operation] || e.operation))) + ': ' + esc(show(e.old_value)) + ' → ' + esc(show(e.new_value)) + ' <span class="muted">(' + esc(e.actor_id || e.actor_type) + ')</span>'; })) + '</ul>';
    }
    // 開き直しが要る = 画面が読んだ値・鍵・段階が古い (その間の変更・CSV を配った・CSV が出た・切替前に戻った・先の日付の原価が入った) か、保存の番号の扱いが決まらない
    var reopen = REOPEN.indexOf(j.reason) >= 0;
    if (reopen) h += '<button type="button" class="btn sm" id="reload">画面を開き直す</button>';
    r.innerHTML = h + '</div>';
    var b = $('#reload'); if (b) b.addEventListener('click', function () { saved = true; update(); location.reload(); });
    // 断られた欄に印を付けてそこへ
    if (j.field) {
      if (j.field === 'name' && nameIn) {
        openName(true);   // 名前の欄を開いてそこへ (保存のボタンに残さない・#1631 Codex R1 L3)
        nameIn.setAttribute('aria-invalid', 'true'); nameIn.setAttribute('aria-describedby', 'save-err');
      }
      var row = $('[data-row="' + j.field + '"]', scope);
      if (row) { row.classList.add('err'); var c = $('input, select, button', row); if (c) { row.scrollIntoView({ behavior: 'smooth', block: 'center' }); } }
    }
    return reopen;
  }
  /** 今の値を基準にし直す (変わった項目が無かった保存の後) */
  function rebase() { tracked().forEach(function (el) { initial.set(el, valueOf(el)); }); }
  /**
   * 保存が通った後、読み直すまでの間: 画面の値と編集の印はもう古い = 入力の場所を閉じる (inert + disabled)。(#1589 Codex R3 M1)
   * 閉じないと、読み直しの間に打った値は未保存に数えず (saved)、黙って消える。読み直した後は新しい印で続けて直せる
   */
  function lockAfterSave() {
    var main = form.firstElementChild;
    [main, $('.handling-top'), $('#sku-sticky'), $('#name-box')].forEach(function (el) {
      if (!el) return;
      el.setAttribute('inert', '');
      $$('input, select, textarea, button', el).forEach(function (x) { x.disabled = true; });
    });
    ['#reason', '#revert'].forEach(function (s) { var x = $(s); if (x) x.disabled = true; });
  }
  function doSave() {
    if (!saveBtn || saveBtn.disabled || busy) return;
    // JAN の欄に打ったままなら、先に札にする (Ctrl+S は欄を離れない = blur が来ない)。札にできなければ保存しない (#1589 Codex R2 M1)
    if (janPending()) {
      if (!janAdd(janIn.value)) { msg('JAN の欄を直してから保存してください (何も保存していません)', 'err'); janIn.focus(); return; }
      if (saveBtn.disabled) return;
    }
    var body = { request_id: requestId, reason: $('#reason').value, seen: { token: D.token, event_id: D.eventId }, values: collect() };
    busy = true; update(); msg('保存しています…'); $('#result').innerHTML = '';
    $$('.f.err', scope).forEach(function (f) { f.classList.remove('err'); });
    fetch(BASE + '/api/sku/' + encodeURIComponent(P.code), { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        if (x.r.ok && x.j.ok) {
          // 変わった項目が無い (5 と 5.0 など) = 今の値を「開いたときの値」にそろえる = 未保存を消す (#1589 Codex R3 L3)
          if (x.j.no_change) { requestId = uuid(); rebase(); msg(''); showResult(x.j); update(); return; }
          // 保存が通った = 画面を読み直して保存した後の値 (と新しい編集の印) にする。読み直すまでの間は打てないように閉じる
          saved = true; lockAfterSave(); update(); msg('保存しました。保存した後の値を読み直しています…', 'ok');
          keepSavedNote(x.j);
          if (ME.reloadPage) ME.reloadPage(); else location.reload();
          return;
        }
        msg('', 'err');
        var reopen = showError(x.j, x.r.status);
        requestId = uuid();   // 返事が来た = この番号の保存は終わった。直してもう一度保存するときは新しい番号
        if (reopen) mustReload = true;
        update();
      })
      .catch(function () { busy = false; update(); msg('通信できませんでした。もう一度「保存する」を押してください (同じ保存は 2 回入りません)', 'err'); });
  }
  if (saveBtn && canSave) saveBtn.addEventListener('click', doSave);
  if ($('#reason')) $('#reason').addEventListener('input', update);

  /* ---------- 保存の箱へ (未保存の札・離れるときの「変更内容を確認する」・狭い画面の帯) ---------- */
  var box = $('#savebox'), boxToggle = $('#savebox-toggle');
  function review() {
    if (!box) return;
    box.classList.add('open'); if (boxToggle) boxToggle.setAttribute('aria-expanded', 'true');
    box.scrollIntoView({ behavior: 'smooth', block: 'center' });
    var need = lastState.need && lastState.need[0];
    var target = need ? $(need.focus) : (saveBtn && !saveBtn.disabled ? saveBtn : box);
    if (need && need.focus === '#cost-reason' && costBox) costBox.hidden = false;
    if (target) target.focus({ preventScroll: true });
  }
  ME.review = review;
  if (boxToggle) boxToggle.addEventListener('click', function () { var on = !box.classList.contains('open'); box.classList.toggle('open', on); boxToggle.setAttribute('aria-expanded', on ? 'true' : 'false'); });
  ME.onSave = canSave ? function () {
    if (saveBtn && !saveBtn.disabled) { doSave(); return; }
    if (lastState.need && lastState.need.length) { review(); ME.toast(lastState.need[0].t); return; }
    if (mustReload) { ME.toast('この画面は古くなりました。「画面を開き直す」を押してください'); var rb = $('#reload'); if (rb) rb.focus(); return; }
    ME.toast(saved ? '保存しました。保存した後の値を読み直しています' : '保存する変更がありません');
  } : null;

  /* ---------- product-hub の出品カード (前の画面と同じ API) ---------- */
  var cardBox = $('#card-box');
  var cardBtn = $('#card-retry');
  if (cardBtn) cardBtn.addEventListener('click', function () {
    var m = $('#card-msg');
    cardBtn.disabled = true; m.className = 'msgline'; m.textContent = 'カードを作っています…';
    fetch(cardBox.getAttribute('data-base') + '/api/sku/' + encodeURIComponent(cardBox.getAttribute('data-code')) + '/card-retry', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: '{}' })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        m.className = 'msgline ' + (x.j.ok ? 'ok' : 'err');
        m.textContent = x.j.ok ? 'カードを作りました。表示し直します…' : ((x.j.label ? x.j.label + ': ' : '') + (x.j.error || ('HTTP ' + x.r.status)));
        if (x.j.ok) setTimeout(function () { if (ME.dirty().n === 0) location.reload(); }, 800); else cardBtn.disabled = false;
      })
      .catch(function () { m.className = 'msgline err'; m.textContent = '通信できませんでした'; cardBtn.disabled = false; });
  });
  var linkBtn = $('#card-link');
  if (linkBtn) linkBtn.addEventListener('click', function () {
    var m = $('#card-msg');
    if (!window.confirm('product-hub のカード #' + linkBtn.getAttribute('data-draft') + ' を、この商品のカードにします。よろしいですか')) return;
    linkBtn.disabled = true; m.className = 'msgline'; m.textContent = '結んでいます…';
    // 画面が見ていたカードの番号を送る (その間に衝突のカードが変わっていれば結ばない)
    fetch(cardBox.getAttribute('data-base') + '/api/sku/' + encodeURIComponent(cardBox.getAttribute('data-code')) + '/card-link', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ draft_id: linkBtn.getAttribute('data-draft') }) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        m.className = 'msgline ' + (x.j.ok ? 'ok' : 'err');
        if (!x.j.ok) { m.textContent = x.j.error || ('HTTP ' + x.r.status); linkBtn.disabled = false; return; }
        var na = Array.isArray(x.j.not_applied) ? x.j.not_applied : [];
        m.textContent = '結びました。' + (x.j.applied && x.j.applied.length ? ' 空だった欄に入れた: ' + x.j.applied.join('・') + '。' : '')
          + (na.length ? ' カードに入っていたので変えなかった欄: ' + na.map(function (y) { return y.label; }).join('・') + ' (product-hub で確かめてください)。' : '') + ' 表示し直すには画面を開き直してください';
      })
      .catch(function () { m.className = 'msgline err'; m.textContent = '通信できませんでした'; linkBtn.disabled = false; });
  });

  update();
  showSavedNote();
})();
