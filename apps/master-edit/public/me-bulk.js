/*
 * me-bulk.js — 一覧で選んで、まとめて変える (PR2・10/8 の見本どおり + 見本への Codex の UX レビュー High 3・Medium 1〜9)
 *   - 選ぶ: 行の左のチェック (Shift で間をまとめて)・見出しのチェック = このページ・帯の「検索結果 N 件すべてを選ぶ」(api/codes)。
 *     選んだものはタブの中 (sessionStorage) で覚える = ページを送っても消えない。
 *     絞り込みを変えた = 前の条件で選んだ分を「残す / 外す」で聞く・帯にいつも「表示中・ほかのページ・別の条件」を出す (High 2)
 *   - 1 回 max 件まで = 選ぶ時点で数える (M6)
 *   - 引き出し: 1 何を変える (api/bulk/inspect の数・M5 = 先の日の原価は最初から「保存できない」) → 2 新しい値 (原価の理由は初期値なし・M4) →
 *     3 前と後 (api/bulk/preview・保存と同じ計算・選んだ商品 + 一緒に変わるセット = 合計を 1 か所で・M2)。確かめのチェックを入れるまで保存できない・
 *     保存のボタンへ自動で移らない・前の画面の「次へ」と同じ場所に置かない (High 1) → 4 保存 (api/bulk/apply に 20 件ずつ・進みの棒・
 *     切れたら「続きを送る」= 同じ一括の番号 = 二重に書かない) → 結果 (実際に変わったセット = サーバーの答え・M3 / だめな分は直し方ごと・M7)
 */
(function () {
  'use strict';
  var cfgEl = document.getElementById('me-bulk');
  if (!cfgEl) return;
  var CFG = JSON.parse(cfgEl.textContent);
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var ME = window.MasterEdit || {};
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function ic(id, cls) { return '<svg class="ic ' + (cls == null ? 's' : cls) + '" aria-hidden="true"><use href="#' + id + '"/></svg>'; }
  function yen(v) { return v == null ? '' : Number(v).toLocaleString('ja-JP'); }
  function toast(t) { if (ME.toast) ME.toast(t); }
  var MAX = CFG.max || 200, CHUNK = CFG.chunk || 20;
  var KEY = 'master-edit:bulk';
  var SALES = { 1: '1 自社', 2: '2 取引先限定', 3: '3 仕入', 4: '4 輸出' };
  var HANDLING = { active: '取扱中', discontinued: '中止', unknown: '不明' };
  var FIELDS = {
    cost: { label: '原価', icon: 'i-yen', how: '今日から · 理由を選ぶ' },
    standard_price: { label: '売価', icon: 'i-tag', how: '同じ値にそろえる' },
    handling: { label: '取扱', icon: 'i-power', how: '取扱中 / 中止' },
    sales_class: { label: '売上分類', icon: 'i-layers', how: '1 自社 〜 4 輸出' },
    tax_rate: { label: '税率', icon: 'i-percent', how: '8% / 10%' },
    primary_supplier: { label: '仕入先', icon: 'i-truck', how: '代表の仕入先' }
  };
  var FIELD_KEYS = Object.keys(FIELDS);

  /* ---------- 選んだもの (タブの中だけ) ---------- */
  var st = load();
  function load() {
    var v = null;
    try { v = JSON.parse(sessionStorage.getItem(KEY) || 'null'); } catch (e) { v = null; }
    if (!v || typeof v !== 'object' || !v.sel || typeof v.sel !== 'object') v = { sel: {}, ack: null };
    return v;
  }
  function save() { try { sessionStorage.setItem(KEY, JSON.stringify(st)); } catch (e) { /* 覚えられない = この画面の中だけ */ } }
  function selCodes() { return Object.keys(st.sel); }
  function nSel() { return selCodes().length; }
  var rows = $$('#list-tbl tbody .rowck');
  var pageCodes = rows.map(function (x) { return x.getAttribute('data-code'); });
  var lastIdx = null;
  // 一覧を「選んだものだけ表示」で開いた直後 = 別の条件で選んだ分を聞かない
  if (st.pendingShow) { st.ack = CFG.fkey; delete st.pendingShow; save(); }
  function setRow(ck, on) {
    var code = ck.getAttribute('data-code');
    if (on) st.sel[code] = { k: ck.getAttribute('data-kind'), s: ck.getAttribute('data-state'), f: CFG.fkey };
    else delete st.sel[code];
  }
  function paint() {
    rows.forEach(function (ck) {
      var code = ck.getAttribute('data-code');
      var on = Object.prototype.hasOwnProperty.call(st.sel, code);
      ck.checked = on;
      var tr = ck.closest('tr');
      if (tr) {
        tr.classList.toggle('bk-sel', on);
        tr.classList.toggle('bk-failed', !!(st.failed && st.failed.indexOf(code) >= 0));
      }
    });
    var head = $('#ck-page');
    if (head) {
      var n = pageCodes.filter(function (c) { return Object.prototype.hasOwnProperty.call(st.sel, c); }).length;
      head.checked = pageCodes.length > 0 && n === pageCodes.length;
      head.indeterminate = n > 0 && n < pageCodes.length;
    }
    renderBar();
  }
  /** 帯: 何件・どこに (表示中 / ほかのページ / 別の条件)・このページ / 検索結果すべて・上限 */
  function renderBar() {
    var bar = $('#bk-selbar');
    var n = nSel();
    bar.hidden = n === 0;
    document.body.classList.toggle('bk-has-bar', n > 0);
    if (!n) return;
    $('#bk-n').textContent = n.toLocaleString('ja-JP');
    var codes = selCodes();
    // 表示中 (このページにある) → 見えないもののうち 同じ条件のほかのページ / 別の条件で選んだ (重ねて数えない)
    var here = codes.filter(function (c) { return pageCodes.indexOf(c) >= 0; }).length;
    var other = codes.filter(function (c) { return pageCodes.indexOf(c) < 0 && st.sel[c].f !== CFG.fkey; }).length;
    var otherPage = n - here - other;
    var kinds = { single: 0, set: 0, exception: 0 };
    codes.forEach(function (c) { var k = st.sel[c].k; if (kinds[k] != null) kinds[k]++; });
    var mix = ['単品 ' + kinds.single, 'セット ' + kinds.set].concat(kinds.exception ? ['例外 ' + kinds.exception] : []).join(' · ');
    $('#bk-where').innerHTML = '表示中 <b>' + here + '</b>' + (otherPage ? ' · ほかのページ <b>' + otherPage + '</b>' : '')
      + (other ? ' · <span class="other">別の条件で選んだ <b>' + other + '</b></span>' : '') + ' <span aria-hidden="true">|</span> ' + esc(mix);
    // このページ全部 (まだ全部選んでいない)
    var pageAll = pageCodes.length > 0 && pageCodes.every(function (c) { return Object.prototype.hasOwnProperty.call(st.sel, c); });
    var bp = $('#bk-sel-page');
    bp.hidden = pageAll || !pageCodes.length;
    bp.textContent = 'このページ ' + pageCodes.length + ' 件を選ぶ';
    // 検索結果すべて (M1: このページと分ける・M6: 足すと上限を超えるなら押せない)
    var ba = $('#bk-sel-all');
    var allFits = CFG.total <= pageCodes.length;
    ba.hidden = allFits || !pageAll;
    ba.textContent = '検索結果 ' + Number(CFG.total).toLocaleString('ja-JP') + ' 件すべてを選ぶ';
    var wouldBe = n - here + CFG.total;
    ba.disabled = wouldBe > MAX || !!CFG.overMax;
    ba.title = ba.disabled ? '1 回は ' + MAX + ' 件まで。絞る欄や詳細検索で減らしてから' : 'ページに関係なく、絞った一覧の全部を選ぶ';
    if (pageAll && !allFits) bp.hidden = true;
    $('#bk-show').hidden = !(other || otherPage);
    // 上限 (選ぶ時点で数える・M6)
    var lim = $('#bk-limit');
    lim.hidden = n <= MAX;
    if (n > MAX) lim.innerHTML = ic('i-warn') + n + ' 件選択中 · 1 回は ' + MAX + ' 件まで。' + (n - MAX) + ' 件減らしてください';
    $('#bk-go').disabled = n > MAX;
    // 絞り込みを変えた = 前の条件で選んだ分を残すか聞く (High 2)
    var prev = $('#bk-prev');
    prev.hidden = !(other && st.ack !== CFG.fkey);
    if (!prev.hidden) $('#bk-prev-t').textContent = '前の条件で選んだ ' + other + ' 件が残っています (この一覧には出ていません)。まとめて変えるときも入ります';
  }
  // 行のチェック (Shift = 前に押した行からここまで)
  rows.forEach(function (ck, i) {
    ck.addEventListener('click', function (e) {
      if (e.shiftKey && lastIdx != null) {
        var a = Math.min(lastIdx, i), b = Math.max(lastIdx, i);
        for (var k = a; k <= b; k++) setRow(rows[k], ck.checked);
      } else setRow(ck, ck.checked);
      lastIdx = i;
      if (st.failed) st.failed = st.failed.filter(function (c) { return c !== ck.getAttribute('data-code'); });
      save(); paint();
    });
  });
  var headCk = $('#ck-page');
  if (headCk) headCk.addEventListener('click', function () {
    var on = headCk.checked;
    rows.forEach(function (ck) { setRow(ck, on); });
    save(); paint();
  });
  $('#bk-sel-page').addEventListener('click', function () { rows.forEach(function (ck) { setRow(ck, true); }); save(); paint(); });
  $('#bk-sel-all').addEventListener('click', function () {
    var b = this; b.disabled = true;
    fetch(CFG.codesUrl, { headers: { Accept: 'application/json' }, credentials: 'same-origin' })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { if (!r.ok || !j || !j.ok) throw new Error((j && j.error) || '読めませんでした (' + r.status + ')'); return j.codes; }); })
      .then(function (codes) {
        var add = codes.filter(function (c) { return !Object.prototype.hasOwnProperty.call(st.sel, c); });
        if (nSel() + add.length > MAX) { toast('1 回は ' + MAX + ' 件までです。絞ってから選んでください'); return; }
        codes.forEach(function (c) { if (!st.sel[c]) st.sel[c] = { k: null, s: null, f: CFG.fkey }; });
        rows.forEach(function (ck) { setRow(ck, true); });
        save(); toast('検索結果の ' + codes.length.toLocaleString('ja-JP') + ' 件を選びました');
      })
      .catch(function (err) { toast(err.message); })
      .then(function () { b.disabled = false; paint(); });
  });
  $('#bk-clear').addEventListener('click', function () { st.sel = {}; st.failed = []; st.ack = null; save(); paint(); });
  $('#bk-prev-keep').addEventListener('click', function () { st.ack = CFG.fkey; save(); paint(); toast('前の条件で選んだ分も残しました (帯の「別の条件」)'); });
  $('#bk-prev-drop').addEventListener('click', function () {
    selCodes().forEach(function (c) { if (st.sel[c].f !== CFG.fkey && pageCodes.indexOf(c) < 0) delete st.sel[c]; });
    st.ack = null; save(); paint();
  });
  // 選んだものだけ表示 = 詳細検索の商品コード (api/search の印)
  $('#bk-show').addEventListener('click', function () {
    var b = this; b.disabled = true;
    fetch(new URL('api/search', location.href).href, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ codes: selCodes().join('\n') }) })
      .then(function (r) { return r.json().catch(function () { return {}; }); })
      .then(function (j) {
        if (j && j.ok && j.url) { st.pendingShow = true; save(); location.href = j.url; return; }
        b.disabled = false; toast('選んだ商品を出せませんでした' + (j && (j.message || j.error) ? ' (' + (j.message || j.error) + ')' : ''));
      })
      .catch(function () { b.disabled = false; toast('通信できませんでした'); });
  });
  // 前の一括で変えた行を光らせる (1 回だけ)
  if (st.just && st.just.length) {
    rows.forEach(function (ck) { if (st.just.indexOf(ck.getAttribute('data-code')) >= 0) { var tr = ck.closest('tr'); if (tr) tr.classList.add('bk-just'); } });
    delete st.just; save();
  }

  /* ---------- 横に送れることを見せる (Codex M8) ---------- */
  var wrap = $('#list-tbl') && $('#list-tbl').closest('.tblwrap');
  if (wrap) {
    var more = function () {
      var can = wrap.scrollWidth - wrap.clientWidth - wrap.scrollLeft > 4;
      wrap.classList.toggle('bk-more', can);
      return wrap.scrollWidth > wrap.clientWidth + 4;
    };
    wrap.addEventListener('scroll', function () {
      more();
      var hint = $('#bk-swipe'); if (hint && wrap.scrollLeft > 20) { hint.remove(); try { localStorage.setItem('master-edit:swiped', '1'); } catch (e) { /* */ } }
    }, { passive: true });
    window.addEventListener('resize', more);
    var seen = false; try { seen = localStorage.getItem('master-edit:swiped') === '1'; } catch (e) { seen = false; }
    if (more() && !seen && window.innerWidth <= 760) {
      var h = document.createElement('div'); h.className = 'bk-swipe'; h.id = 'bk-swipe';
      h.innerHTML = ic('i-chevr') + '横にスワイプすると、ほかの列 (売価・原価・税・利益…) があります';
      wrap.parentNode.insertBefore(h, wrap);
    }
  }

  /* ---------- 引き出し ---------- */
  var bk = null;          // 今の一括 { step, codes, inspect, field, val, reason, reasonOther, stop, supQ, preview, tab, confirmed, run, result }
  var returnFocus = null;
  function api(path, body) {
    return fetch(new URL(path, location.href).href, { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) {
        return r.json().catch(function () { return null; }).then(function (j) {
          if (!r.ok || !j || j.ok === false) { var e = new Error((j && (j.error || j.message)) || '読めませんでした (' + r.status + ')'); e.status = r.status; e.body = j; throw e; }
          return j;
        });
      });
  }
  function openDrawer() {
    if (nSel() > MAX) return;
    returnFocus = document.activeElement;
    bk = { step: 1, codes: selCodes(), inspect: null, error: null, field: null, val: null, reason: null, reasonOther: '', stop: '', supQ: '', preview: null, tab: 'all', confirmed: false, run: null, result: null };
    $('#bk-scrim').hidden = false; $('#bk-drawer').hidden = false; $('#bk-selbar').hidden = true;
    document.body.style.overflow = 'hidden';
    render();
    api('api/bulk/inspect', { codes: bk.codes }).then(function (j) { if (!bk) return; bk.inspect = j; render(); }, function (e) { if (!bk) return; bk.error = e.message; render(); });
  }
  function closeDrawer(force) {
    if (!bk) return;
    if (bk.run && bk.run.busy && !force) { toast('保存の途中です。終わるまでお待ちください'); return; }
    var hadResult = bk.result;
    bk = null;
    $('#bk-scrim').hidden = true; $('#bk-drawer').hidden = true; document.body.style.overflow = '';
    if (hadResult && hadResult.okCodes.length) { reloadList(); return; }
    paint();
    if (returnFocus && returnFocus.focus && document.contains(returnFocus)) returnFocus.focus();
  }
  function reloadList() { if (ME.reloadPage) ME.reloadPage(); else location.reload(); }
  $('#bk-go').addEventListener('click', openDrawer);
  $('#bk-x').addEventListener('click', function () { closeDrawer(); });
  $('#bk-scrim').addEventListener('click', function () { closeDrawer(); });
  document.addEventListener('keydown', function (e) {
    if (!bk) return;
    if (e.key === 'Escape') { e.preventDefault(); closeDrawer(); return; }
    if (e.key === 'Tab') {   // 引き出しの中だけで Tab が回る
      var f = $$('button, input, a[href], select, textarea, [tabindex="0"]', $('#bk-drawer')).filter(function (x) { return !x.disabled && x.offsetParent !== null; });
      if (!f.length) return;
      if (!$('#bk-drawer').contains(document.activeElement)) { e.preventDefault(); f[0].focus(); return; }
      if (e.shiftKey && document.activeElement === f[0]) { e.preventDefault(); f[f.length - 1].focus(); }
      else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { e.preventDefault(); f[0].focus(); }
      return;
    }
    var t = e.target, typing = /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName || '') && t.type !== 'radio' && t.type !== 'checkbox';
    if (bk.step === 1 && bk.inspect && !typing && /^[1-6]$/.test(e.key) && !e.ctrlKey && !e.metaKey && !e.altKey) {
      var fk = FIELD_KEYS[Number(e.key) - 1]; if (counts(fk).ok) { e.preventDefault(); chooseField(fk); }
    }
    // 2 = Enter で前と後へ (3 では Enter で保存しない = High 1)
    if (bk.step === 2 && e.key === 'Enter' && !e.isComposing && e.keyCode !== 229 && t.tagName === 'INPUT' && valueReady()) { e.preventDefault(); toPreview(); }
  });

  /* ---------- 見せ方 ---------- */
  function supName(code) {
    var l = (bk && bk.inspect && bk.inspect.suppliers) || [];
    for (var i = 0; i < l.length; i++) if (l[i].code === code) return l[i].name;
    return '';
  }
  function fmtVal(field, v) {
    if (v == null || v === '') return '未入力';
    if (field === 'cost' || field === 'standard_price') return yen(v) + ' 円';
    if (field === 'handling') return HANDLING[v] || v;
    if (field === 'sales_class') return SALES[v] || String(v);
    if (field === 'tax_rate') {
      if (typeof v === 'object') return v.rate == null ? '未決' : Math.round(v.rate * 100) + '%' + (v['class'] === 'MIXED' ? ' (混在)' : '');
      return Math.round(Number(v) * 100) + '%';
    }
    if (field === 'primary_supplier') return v + (supName(v) ? ' ' + supName(v) : '');
    return String(v);
  }
  function fmtLinked(col, v) {
    if (col === 'cost') return v == null ? '空 (計算できない)' : yen(v) + ' 円';
    if (col === 'tax_rate') return fmtVal('tax_rate', v);
    if (col === 'handling') return HANDLING[v] || v;
    return String(v);
  }
  function counts(field) {
    var c = { ok: 0, out: 0, block: 0 };
    if (!bk || !bk.inspect) return c;
    bk.inspect.items.forEach(function (it) { var v = it.fields[field].verdict; c[v === 'ok' ? 'ok' : v === 'block' ? 'block' : 'out']++; });
    return c;
  }
  function stepsHtml() {
    $$('#bk-steps li').forEach(function (li) {
      var s = Number(li.getAttribute('data-s'));
      li.className = s < bk.step ? 'done' : s === bk.step ? 'cur' : '';
      $('.no', li).textContent = s < bk.step ? '✓' : String(s);
      if (s === bk.step) li.setAttribute('aria-current', 'step'); else li.removeAttribute('aria-current');
    });
  }
  function chosenHtml() {
    var F = FIELDS[bk.field];
    return '<div class="bk-chosen"><span class="hic">' + ic(F.icon, '') + '</span><span>変える項目</span><b>' + F.label + '</b>'
      + (bk.step === 2 ? '<button type="button" class="btn sm ghost right" data-act="to1">' + ic('i-chevl') + '項目を選び直す</button>' : '') + '</div>';
  }
  function reasonText() {
    if (bk.field === 'cost') return bk.reason === '' ? bk.reasonOther.trim() : (bk.reason || '');
    if (bk.field === 'handling' && bk.val === 'discontinued') return bk.stop.trim();
    return '';
  }
  function valueReady() {
    if (!bk || bk.val == null || bk.val === '') return false;
    if ((bk.field === 'cost' || bk.field === 'standard_price') && !(Number(bk.val) >= 1)) return false;
    if (bk.field === 'cost' && !reasonText()) return false;                                     // 理由は初期値なし = 選ぶまで進めない (M4)
    if (bk.field === 'handling' && bk.val === 'discontinued' && !reasonText()) return false;     // 中止は理由が必須
    return true;
  }
  function render() {
    if (!bk) return;
    stepsHtml();
    $('#bk-dcount').textContent = '選んだ ' + bk.codes.length + ' 件';
    var body = $('#bk-body'), foot = $('#bk-foot');
    if (bk.error && !bk.inspect) {
      body.innerHTML = '<div class="bk-err" role="alert">' + ic('i-x') + '<span>' + esc(bk.error) + '</span></div>';
      foot.innerHTML = '<button type="button" class="btn ghost" data-act="close">閉じる</button>';
      return;
    }
    if (bk.step === 1) return renderPick(body, foot);
    if (bk.step === 2) return renderValue(body, foot);
    if (bk.step === 3) return renderPreview(body, foot);
    return renderRun(body, foot);
  }
  function renderPick(body, foot) {
    if (!bk.inspect) { body.innerHTML = '<div class="bk-loading">選んだ ' + bk.codes.length + ' 件の今の値を読んでいます…</div>'; foot.innerHTML = '<button type="button" class="btn ghost" data-act="close">やめる</button>'; return; }
    body.innerHTML = '<h3 class="bk-q" tabindex="-1" id="bk-h">何を変えますか</h3><div class="bk-qsub">1 回に変えるのは 1 つの項目です。各板の下 = 選んだ ' + bk.codes.length + ' 件のうち変えられる数</div>'
      + '<div class="bk-tiles">' + FIELD_KEYS.map(function (f, i) {
        var F = FIELDS[f], c = counts(f);
        return '<button type="button" class="bk-tile' + (c.ok ? '' : ' none') + '" data-field="' + f + '"' + (c.ok ? '' : ' aria-disabled="true"') + '>'
          + '<span class="top"><span class="hic">' + ic(F.icon, '') + '</span><span class="ttl">' + F.label + '</span><span class="kbd" aria-hidden="true">' + (i + 1) + '</span></span>'
          + '<span class="how">' + F.how + '</span>'
          + '<span class="who"><span><b>' + c.ok + '</b> 件 変えられる</span>' + (c.block ? '<span class="bl">· 保存できない ' + c.block + '</span>' : '') + (c.out ? '<span class="ex">· 対象外 ' + c.out + '</span>' : '') + '</span></button>';
      }).join('') + '</div>';
    foot.innerHTML = '<button type="button" class="btn ghost" data-act="close">やめる</button><span class="sp"></span><span class="note">数字キー 1〜6 でも選べます</span>';
    var t = $('.bk-tile:not(.none)', body); if (t) t.focus();
  }
  function chooseField(f) {
    bk.field = f; bk.val = null; bk.reason = null; bk.reasonOther = ''; bk.stop = ''; bk.supQ = ''; bk.step = 2; bk.preview = null; render();
  }
  function okItems() { return bk.inspect.items.filter(function (it) { return it.fields[bk.field].verdict === 'ok'; }); }
  function renderValue(body, foot) {
    var F = FIELDS[bk.field], f = bk.field;
    var dist = {}, order = [];
    okItems().forEach(function (it) { var v = it.fields[f].now; var k = v == null ? '' : String(v); if (!dist[k]) { dist[k] = 0; order.push(k); } dist[k]++; });
    order.sort(function (a, b) { return dist[b] - dist[a]; });
    var distHtml = order.map(function (k) {
      var v = k === '' ? null : (f === 'cost' || f === 'standard_price' || f === 'sales_class' || f === 'tax_rate' ? Number(k) : k);
      return '<button type="button" data-pickval="' + esc(k) + '" title="この値を新しい値に入れる">' + esc(fmtVal(f, v)) + ' <span class="x">× ' + dist[k] + '</span></button>';
    }).join('');
    var input = '';
    if (f === 'cost' || f === 'standard_price') {
      input = '<div class="bk-vrow"><label class="lab" for="bk-yen">新しい' + F.label + ' <span class="req">必須</span></label><div class="ctl col">'
        + '<div class="ctl"><span class="yen bk-bigyen"><input class="in" id="bk-yen" inputmode="numeric" autocomplete="off" placeholder="0" value="' + (bk.val == null ? '' : yen(bk.val)) + '"></span><span class="unit">円 にそろえる</span></div>'
        + '<div class="bk-live" id="bk-live" aria-live="polite"></div></div></div>';
      if (f === 'cost') {
        input += '<div class="bk-vrow"><span class="lab">いつから</span><div class="ctl"><span class="lockval">' + ic('i-lock') + (bk.inspect.today || '').replace(/^\d{4}-0?(\d+)-0?(\d+)$/, '$1/$2') + ' 今日から</span><span class="hint">今の原価は昨日で終わり (過去の粗利は変わりません)</span></div></div>'
          + '<div class="bk-vrow"><span class="lab" id="bk-lab-reason">理由 <span class="req">必須</span></span><div class="ctl col">'
          + '<div class="reason-pick" role="radiogroup" aria-labelledby="bk-lab-reason">' + (CFG.costReasons || []).map(function (t) { return '<label class="rp"><input type="radio" name="bk-rsn" value="' + esc(t) + '"' + (bk.reason === t ? ' checked' : '') + '><span>' + esc(t) + '</span></label>'; }).join('')
          + '<label class="rp"><input type="radio" name="bk-rsn" value=""' + (bk.reason === '' ? ' checked' : '') + '><span>その他</span></label></div>'
          + '<input class="in" id="bk-reason-other" maxlength="150" placeholder="その他の理由を書く (例: 送料込みの仕入値に変わった)" value="' + esc(bk.reasonOther) + '"' + (bk.reason === '' ? '' : ' hidden') + '>'
          + '<span class="hint">選ぶまで次へ進めません · 記録には「一括 ◯ 件」と一緒に残ります</span></div></div>';
      }
    } else if (f === 'handling') {
      input = '<div class="bk-vrow"><span class="lab">新しい取扱 <span class="req">必須</span></span><div class="ctl"><div class="seg" role="radiogroup" aria-label="新しい取扱">'
        + '<button type="button" role="radio" data-v="active" class="' + (bk.val === 'active' ? 'on' : '') + '" aria-checked="' + (bk.val === 'active') + '"><span style="color:var(--ok)">●</span>取扱中</button>'
        + '<button type="button" role="radio" data-v="discontinued" class="bk-danger' + (bk.val === 'discontinued' ? ' on' : '') + '" aria-checked="' + (bk.val === 'discontinued') + '">中止 <small>売らない</small></button></div></div></div>'
        + (bk.val === 'discontinued' ? '<div class="bk-vrow"><label class="lab" for="bk-stop">中止の理由 <span class="req">必須</span></label><div class="ctl col"><input class="in" id="bk-stop" maxlength="150" placeholder="例: メーカーの廃番" value="' + esc(bk.stop) + '">'
          + '<div class="bk-warnline">' + ic('i-warn') + '<span>単品を中止すると、その単品を含むセットも中止になります。次の画面で一緒に変わるセットを出します。</span></div></div></div>' : '')
        + '<div class="bk-vrow"><span class="lab"></span><div class="bk-live" id="bk-live" aria-live="polite"></div></div>';
    } else if (f === 'sales_class') {
      input = '<div class="bk-vrow"><span class="lab">新しい売上分類 <span class="req">必須</span></span><div class="ctl"><div class="seg" role="radiogroup" aria-label="新しい売上分類">'
        + [1, 2, 3, 4].map(function (k) { return '<button type="button" role="radio" data-v="' + k + '" class="' + (bk.val === k ? 'on' : '') + '" aria-checked="' + (bk.val === k) + '">' + SALES[k] + '</button>'; }).join('') + '</div></div></div>'
        + '<div class="bk-vrow"><span class="lab"></span><div class="bk-live" id="bk-live" aria-live="polite"></div></div>';
    } else if (f === 'tax_rate') {
      input = '<div class="bk-vrow"><span class="lab">新しい税率 <span class="req">必須</span></span><div class="ctl"><div class="seg" role="radiogroup" aria-label="新しい税率">'
        + [[0.08, '8% 軽減 (食品)'], [0.1, '10% 標準']].map(function (x) { return '<button type="button" role="radio" data-v="' + x[0] + '" class="' + (bk.val === x[0] ? 'on' : '') + '" aria-checked="' + (bk.val === x[0]) + '">' + x[1] + '</button>'; }).join('') + '</div></div></div>'
        + '<div class="bk-vrow"><span class="lab"></span><div class="bk-live" id="bk-live" aria-live="polite"></div></div>';
    } else if (f === 'primary_supplier') {
      var q = (bk.supQ || '').trim().toLowerCase();
      var list = (bk.inspect.suppliers || []).filter(function (s) { return !q || (s.code + ' ' + (s.name || '')).toLowerCase().indexOf(q) >= 0; }).slice(0, 200);
      input = '<div class="bk-vrow"><span class="lab">新しい仕入先 <span class="req">必須</span></span><div class="ctl col">'
        + '<input class="in" id="bk-supq" placeholder="仕入先の名前・コードで絞る" value="' + esc(bk.supQ) + '" autocomplete="off" aria-label="仕入先を絞る">'
        + '<div class="bk-suplist" role="radiogroup" aria-label="新しい仕入先" id="bk-suplist">' + (list.map(function (s) { return '<label><input type="radio" name="bk-sup" value="' + esc(s.code) + '"' + (bk.val === s.code ? ' checked' : '') + '><span class="c">' + esc(s.code) + '</span>' + esc(s.name || '') + '</label>'; }).join('') || '<span class="hint">当てはまる仕入先がありません</span>') + '</div>'
        + '<span class="hint">選べるのは取引中で NE に登録済みの仕入先だけです</span><div class="bk-live" id="bk-live" aria-live="polite"></div></div></div>';
    }
    body.innerHTML = chosenHtml() + '<div class="bk-vbox"><div class="bk-vrow"><span class="lab">今の値</span><div class="ctl"><div class="bk-nowdist">' + (distHtml || '<span class="hint">変えられる商品がありません</span>') + '</div></div></div>' + input + '</div>';
    foot.innerHTML = '<button type="button" class="btn ghost" data-act="close">やめる</button><span class="sp"></span>'
      + '<button type="button" class="btn" data-act="to1">' + ic('i-chevl') + '戻る</button>'
      + '<button type="button" class="btn pri" data-act="to3" id="bk-next"' + (valueReady() ? '' : ' disabled') + '>前と後を見る ' + ic('i-arrow') + '<span class="kbd">Enter</span></button>';
    live();
    var first = $('#bk-yen', body) || $('.seg button.on', body) || $('.seg button', body) || $('#bk-supq', body); if (first) first.focus();
  }
  /** 入れた値で何件変わるか (確かめの前の目安。確かめ = サーバーの preview) */
  function live() {
    var el = $('#bk-live'); var nb = $('#bk-next');
    if (nb) nb.disabled = !valueReady();
    if (!el) return;
    if (bk.val == null || bk.val === '') { el.innerHTML = '<span class="muted">入れると、何件が変わるかがここに出ます</span>'; return; }
    var c = counts(bk.field), chg = 0, same = 0;
    okItems().forEach(function (it) {
      var now = it.fields[bk.field].now;
      var isSame = bk.field === 'handling' && it.kind === 'set' ? it.handling_own === bk.val : (now == null ? false : String(now).toLowerCase() === String(bk.val).toLowerCase());
      if (isSame) same++; else chg++;
    });
    el.innerHTML = '<b>' + chg + ' 件</b> が ' + esc(fmtVal(bk.field, bk.val)) + ' に変わります · 変わらない ' + same + ' · 対象外 ' + c.out + (c.block ? ' · <span style="color:var(--warn)">保存できない ' + c.block + '</span>' : '');
  }
  function parseYen(s) {
    var t = String(s || '').replace(/[０-９]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) - 0xFEE0); }).replace(/[,，\s円¥]/g, '');
    return /^\d{1,9}$/.test(t) ? Number(t) : null;
  }
  function toPreview() {
    if (!valueReady()) return;
    bk.step = 3; bk.tab = 'all'; bk.confirmed = false; bk.preview = null; bk.previewError = null;
    render();
    var my = bk;
    api('api/bulk/preview', { codes: bk.codes, field: bk.field, value: bk.val, reason: reasonText() || null }).then(function (j) {
      if (bk !== my) return; bk.preview = j; render();
    }, function (e) { if (bk !== my) return; bk.previewError = e.message; render(); });
  }
  function nameOf(it) { return it.name || ''; }
  function renderPreview(body, foot) {
    var F = FIELDS[bk.field];
    if (bk.previewError) {
      body.innerHTML = chosenHtml() + '<div class="bk-err" role="alert">' + ic('i-x') + '<span>' + esc(bk.previewError) + '</span></div>';
      foot.innerHTML = '<button type="button" class="btn ghost" data-act="close">やめる</button><span class="sp"></span><button type="button" class="btn" data-act="to2">' + ic('i-chevl') + '値を直す</button>';
      return;
    }
    if (!bk.preview) {
      body.innerHTML = chosenHtml() + '<div class="bk-loading">' + bk.codes.length + ' 件の今の値を読んで、前と後を作っています…</div>';
      foot.innerHTML = '<button type="button" class="btn ghost" data-act="close">やめる</button><span class="sp"></span><button type="button" class="btn" data-act="to2">' + ic('i-chevl') + '値を直す</button>';
      return;
    }
    var p = bk.preview, n = p.counts;
    var danger = bk.field === 'handling' && bk.val === 'discontinued';
    var order = { chg: 0, block: 1, same: 2, out: 3 };
    var items = p.items.slice().sort(function (a, b) { return order[a.verdict] - order[b.verdict]; });
    var shown = bk.tab === 'all' ? items : bk.tab === 'linked' ? [] : items.filter(function (x) { return x.verdict === bk.tab; });
    var total = n.chg + n.linked;
    var row = function (x) {
      var chg;
      if (x.verdict === 'chg') chg = '<span class="old">' + esc(fmtVal(bk.field, x.before)) + '</span>' + ic('i-arrow', 'arrow') + '<span class="new' + (danger ? ' red' : '') + '">' + esc(fmtVal(bk.field, x.after)) + '</span>';
      else if (x.verdict === 'same') chg = '<span class="new">' + esc(fmtVal(bk.field, x.before)) + '</span><span class="muted" style="font-size:12.5px">そのまま</span>';
      else chg = '<span>' + esc(x.before === undefined ? '' : fmtVal(bk.field, x.before)) + '</span>';
      var side = '';
      if (x.profit && x.verdict === 'chg') {
        var a = x.profit.before, b = x.profit.after;
        if (a == null || b == null) side = '利益 —';
        else { var d = b - a; side = '利益 ' + yen(a) + ' → <b>' + yen(b) + '</b> <span class="' + (d >= 0 ? 'up' : 'down') + '">(' + (d >= 0 ? '+' : '−') + yen(Math.abs(d)) + ')</span>'; }
      }
      var why = x.verdict === 'out' ? '<div class="why">' + ic('i-lock') + '対象外 · ' + esc(x.why) + '</div>' : x.verdict === 'block' ? '<div class="why">' + ic('i-warn') + '保存できない · ' + esc(x.why) + '</div>' : '';
      return '<div class="bk-pv ' + (x.verdict === 'chg' ? '' : x.verdict) + '" data-code="' + esc(x.code) + '"><div class="who"><div class="code">' + esc(x.code) + '</div><div class="nm" title="' + esc(nameOf(x)) + '">' + esc(nameOf(x)) + '</div></div>'
        + '<div class="chg">' + chg + '</div><div class="side">' + side + '</div>' + why + (x.note ? '<div class="note">' + esc(x.note) + '</div>' : '') + '</div>';
    };
    var linkRow = function (l) {
      return '<div class="bk-pv link" data-code="' + esc(l.code) + '"><div class="who"><div class="code">' + esc(l.code) + '</div><div class="nm" title="' + esc(l.name) + '">' + esc(l.name) + '</div></div>'
        + '<div class="chg"><span class="old">' + esc(fmtLinked(l.col, l.before)) + '</span>' + ic('i-arrow', 'arrow') + '<span class="new' + (danger ? ' red' : '') + '">' + esc(fmtLinked(l.col, l.after)) + '</span></div><div class="side"></div>'
        + '<div class="why">' + ic('i-layers') + esc(l.why) + '</div></div>';
    };
    var tab = function (k, label, c) { return '<button type="button" class="chip' + (bk.tab === k ? ' on' : '') + (k === 'block' && c ? ' t-warn' : '') + '" data-tab="' + k + '"' + (c === 0 && k !== 'all' ? ' disabled style="opacity:.45"' : '') + '>' + label + ' <span class="n">' + c + '</span></button>'; };
    var reasonTxt = reasonText();
    body.innerHTML = chosenHtml() + '<div class="bk-verdict' + (danger ? ' danger' : '') + '">'
      + '<div class="big" tabindex="-1" id="bk-h">' + F.label + 'を <em class="' + (danger ? 'red' : '') + '">' + esc(fmtVal(bk.field, bk.val)) + '</em> にします</div>'
      + '<div class="sum" id="bk-sum">選んだ商品 <b>' + n.chg + ' 件</b> + 一緒に変わるセット <b>' + n.linked + ' 件</b> = 合計 <b>' + total + ' 件</b> が変わります</div>'
      + '<div class="small">' + (bk.field === 'cost' ? '<span>' + ic('i-lock') + ' ' + esc(p.today) + ' 今日から</span>' : '') + (reasonTxt ? '<span>理由: ' + esc(reasonTxt) + '</span>' : '')
      + '<span>変わらない ' + n.same + ' · 対象外 ' + n.out + (n.block ? ' · <span style="color:var(--warn)">保存できない ' + n.block + '</span>' : '') + '</span></div>'
      + (p.notes && p.notes.length ? '<div class="small" style="color:var(--warn)">' + p.notes.map(esc).join('<br>') + '</div>' : '') + '</div>'
      + '<div class="bk-tabs" role="group" aria-label="一覧の絞り込み">' + tab('all', 'ぜんぶ', items.length) + tab('chg', '変わる', n.chg) + tab('linked', '一緒に変わるセット', n.linked) + tab('block', '保存できない', n.block) + tab('same', '変わらない', n.same) + tab('out', '対象外', n.out) + '</div>'
      + '<div class="bk-list">' + shown.map(row).join('') + '</div>'
      + (p.linked.length && (bk.tab === 'all' || bk.tab === 'linked') ? '<div class="bk-sec">一緒に変わるセット (構成品が変わるため · 自動)</div><div class="bk-list">' + p.linked.map(linkRow).join('') + '</div>' : '')
      + (n.chg ? '<div class="bk-confirm"><label class="chk"><input type="checkbox" class="ck" id="bk-ok"' + (bk.confirmed ? ' checked' : '') + '><span>件数 (合計 ' + total + ' 件)・新しい値 (' + esc(fmtVal(bk.field, bk.val)) + ')・一緒に変わるセットを確かめました</span></label>'
        + '<div class="row"><button type="button" class="btn ' + (danger ? 'bk-dpri' : 'pri') + ' lg" data-act="apply" id="bk-apply"' + (bk.confirmed ? '' : ' disabled') + '>' + ic('i-check', '')
        + (danger ? '選んだ ' + n.chg + ' 件を中止にする' : '合計 ' + total + ' 件を変える') + (n.linked ? ' (セット ' + n.linked + ' を含む)' : '') + '</button>'
        + '<span class="bk-note">NE には今と同じく翌朝の照合のあとで反映します · 誰が・いつ・なぜは「変更の記録」に残ります</span></div></div>'
        : '<div class="bk-warnline" style="margin-top:14px">' + ic('i-info') + '<span>変わる商品がありません。値を直すか、閉じてください。</span></div>');
    foot.innerHTML = '<button type="button" class="btn ghost" data-act="close">やめる</button><span class="sp"></span><button type="button" class="btn" data-act="to2">' + ic('i-chevl') + '値を直す</button>';
    // 見出しへ (保存のボタンへは移らない = 連続で押しても保存しない・High 1)
    var h = $('#bk-h'); if (h) h.focus();
  }

  /* ---------- 保存 (20 件ずつ) ---------- */
  function startApply() {
    var p = bk.preview;
    if (!p || !bk.confirmed) return;
    var todo = p.items.filter(function (x) { return x.verdict === 'chg'; });
    bk.run = { todo: todo, sent: 0, ok: [], ng: [], derived: {}, busy: false, cut: null };
    bk.step = 4; render();
    sendMore();
  }
  function sendMore() {
    var run = bk && bk.run; if (!run) return;
    run.busy = true; run.cut = null; render();
    var p = bk.preview;
    var next = function () {
      if (!bk || bk.run !== run) return;
      if (run.sent >= run.todo.length) { finish(); return; }
      var part = run.todo.slice(run.sent, run.sent + CHUNK);
      api('api/bulk/apply', {
        // 確かめの切符 (サーバーは切符の中身だけを保存する。項目・値・理由・件数は切符と同じかを照らすために送る)
        ticket: p.ticket, field: p.field, value: p.value, reason: p.reason == null ? null : p.reason, total: run.todo.length,
        items: part.map(function (x) { return { code: x.code, token: x.token, self: x.self, mac: x.mac }; })
      }).then(function (j) {
        if (!bk || bk.run !== run) return;
        j.results.forEach(function (r) {
          var it = part.filter(function (x) { return x.code === r.code; })[0] || { code: r.code, name: '' };
          if (r.ok) {
            run.ok.push({ code: r.code, name: it.name, r: r });
            // 同じセットが 2 回変わった (構成品ごとに保存) = 最初の値 → 最後の値の 1 行
            (r.derived || []).forEach(function (d) { var k = d.code + ':' + d.col; run.derived[k] = run.derived[k] ? { code: d.code, col: d.col, from: run.derived[k].from, to: d.to } : d; });
          } else run.ng.push({ code: r.code, name: it.name, error: r.error || { reason: 'error', message: 'だめでした', group: 'later' } });
          logLine(r);
        });
        // 接続が切れて送れなかった分は、続きとしてもう一度送る (同じ一括の番号 = 済んだ分は前の結果)
        var notSent = j.results.filter(function (r) { return r.not_sent; }).length;
        if (notSent) {
          run.ng = run.ng.filter(function (x) { return x.error.reason !== 'not_sent'; });
          run.sent += part.length - notSent;
          run.busy = false; run.cut = 'Company DB との接続が切れました。済んだ分はそのままです。'; render(); return;
        }
        run.sent += part.length;
        progress();
        next();
      }, function (e) {
        if (!bk || bk.run !== run) return;
        run.busy = false;
        // 切符の期限切れ・サーバーの入れ替え = もう一度「前と後」から (済んだ分は「変わらない」になる)
        run.ticketDead = !!(e.body && /^ticket_/.test(e.body.reason || ''));
        run.cut = run.ticketDead ? e.message : '通信が切れました (' + e.message + ')。済んだ分はそのままです。'; render();
      });
    };
    next();
  }
  function logLine(r) {
    var log = $('#bk-log'); if (!log) return;
    var d = document.createElement('div');
    d.className = r.ok ? 'okl' : 'ng';
    d.textContent = (r.ok ? '✓ ' : '✕ ') + r.code + '  ' + (r.ok ? (r.no_change ? '変わりなし' : '変えた') + (r.retried ? ' (読み直して保存)' : '') : (r.error && r.error.message || 'だめ'));
    log.insertBefore(d, log.firstChild);
  }
  function progress() {
    var run = bk.run, tot = run.todo.length;
    var bar = $('#bk-bar'); if (!bar) return;
    $('.okp', bar).style.width = (run.ok.length / tot * 100) + '%';
    $('.ngp', bar).style.width = (run.ng.length / tot * 100) + '%';
    bar.setAttribute('aria-valuenow', String(run.ok.length + run.ng.length));
    $('#bk-pn').textContent = (run.ok.length + run.ng.length) + ' / ' + tot + ' 件';
    $('#bk-pok').textContent = run.ok.length; $('#bk-png').textContent = run.ng.length;
    $('#bk-pt').textContent = run.sent >= tot ? 'もうすぐ終わります…' : run.sent + ' 件まで送りました…';
  }
  function finish() {
    var run = bk.run, p = bk.preview;
    run.busy = false;
    var blocks = p.items.filter(function (x) { return x.verdict === 'block'; }).map(function (x) { return { code: x.code, name: x.name, error: { reason: x.reason, message: x.why, group: x.group || 'one' } }; });
    // 「ここでやめる」= 送っていない分も前の値のまま (選び直せる)
    run.todo.slice(run.sent).forEach(function (x) { run.ng.push({ code: x.code, name: x.name, error: { reason: 'not_sent', message: '送っていません (途中でやめた)', group: 'later' } }); });
    run.sent = run.todo.length;
    var linked = Object.keys(run.derived).map(function (k) { return run.derived[k]; });
    var linkedCodes = {}; linked.forEach(function (d) { linkedCodes[d.code] = true; });
    bk.result = {
      okCodes: run.ok.map(function (x) { return x.code; }), ok: run.ok, ng: run.ng.concat(blocks), skip: p.counts.same + p.counts.out,
      linked: linked, linkedN: Object.keys(linkedCodes).length
    };
    st.just = bk.result.okCodes.concat(Object.keys(linkedCodes));
    st.failed = bk.result.ng.map(function (x) { return x.code; });
    save();
    render();
  }
  function renderRun(body, foot) {
    var F = FIELDS[bk.field], run = bk.run;
    if (!bk.result) {
      var tot = run.todo.length;
      body.innerHTML = '<div class="bk-prog"><div class="big" id="bk-pt">' + (run.cut ? '止まりました' : '保存しています…') + '</div>'
        + '<div class="bk-bar" role="progressbar" aria-label="保存の進み" aria-valuemin="0" aria-valuemax="' + tot + '" aria-valuenow="' + (run.ok.length + run.ng.length) + '" id="bk-bar"><div class="okp" style="width:' + (run.ok.length / tot * 100) + '%"></div><div class="ngp" style="width:' + (run.ng.length / tot * 100) + '%"></div></div>'
        + '<div class="nums"><span id="bk-pn">' + (run.ok.length + run.ng.length) + ' / ' + tot + ' 件</span><span style="color:var(--ok)">変えた <b id="bk-pok">' + run.ok.length + '</b></span><span style="color:var(--err)">だめ <b id="bk-png">' + run.ng.length + '</b></span></div>'
        + (run.cut ? '<div class="bk-err" role="alert">' + ic('i-warn') + '<span>' + esc(run.cut) + '「続きを送る」で残りを送ります (もう一度送っても二重には書きません)。</span></div>' : '')
        + '<div class="hint">' + CHUNK + ' 件ずつ送っています。1 件ずつ保存するので、途中で止まっても済んだ分はそのままです (もう一度押しても二重には書きません)。</div>'
        + '<div class="log" id="bk-log" aria-live="polite"></div></div>';
      foot.innerHTML = run.cut ? '<button type="button" class="btn ghost" data-act="stop">ここでやめる</button><span class="sp"></span>'
        + (run.ticketDead ? '<button type="button" class="btn pri" data-act="recheck">' + ic('i-redo', '') + '前と後を見直す</button>' : '<button type="button" class="btn pri" data-act="resume">' + ic('i-redo', '') + '続きを送る</button>')
        : '<span class="note">保存の途中は閉じられません</span>';
      return;
    }
    var res = bk.result;
    var groups = {}, gorder = [];
    res.ng.forEach(function (x) { var g = x.error.group || 'later'; if (!groups[g]) { groups[g] = []; gorder.push(g); } groups[g].push(x); });
    var G = CFG.groups || {};
    var retryN = res.ng.filter(function (x) { return G[x.error.group] && G[x.error.group].retry; }).length;
    var failHtml = gorder.map(function (g) {
      var meta = G[g] || { label: g, retry: false };
      return '<div class="bk-grp" data-group="' + g + '"><h3><span class="b ' + (meta.retry ? 'warn' : 'err') + '">' + esc(meta.label) + '</span><span class="muted" style="font-size:12.5px">' + groups[g].length + ' 件' + (meta.retry ? ' · 選び直して、もう一度まとめて変えられます' : ' · 一括では直りません') + '</span></h3>'
        + groups[g].map(function (x) {
          return '<div class="bk-fail"><span class="code">' + esc(x.code) + '</span>' + (g === 'one' || g === 'latest' ? '<a class="btn sm ghost" href="sku/' + encodeURIComponent(x.code) + '" target="_blank" rel="noopener">1 件の画面で開く ' + ic('i-ext') + '</a>' : '<span></span>')
            + '<span class="nm" title="' + esc(x.name) + '">' + esc(x.name) + '</span><div class="why">' + ic('i-x') + esc(x.error.message) + '</div>' + (x.error.fix ? '<div class="fixh">' + esc(x.error.fix) + '</div>' : '') + '</div>';
        }).join('') + '</div>';
    }).join('');
    var linkHtml = res.linked.length ? '<div class="bk-sec">一緒に変わったセット ' + res.linkedN + ' 件 (保存の後の本当の値)</div><div class="bk-list">' + res.linked.map(function (d) {
      return '<div class="bk-pv link"><div class="who"><div class="code">' + esc(d.code) + '</div></div><div class="chg"><span class="old">' + esc(fmtLinked(d.col, d.from)) + '</span>' + ic('i-arrow', 'arrow') + '<span class="new">' + esc(fmtLinked(d.col, d.to)) + '</span></div><div class="side"></div></div>';
    }).join('') + '</div>' : '';
    body.innerHTML = '<h3 class="bk-q" tabindex="-1" id="bk-h" style="margin-bottom:12px">' + (res.ng.length ? ic('i-warn', '') : ic('i-check', '')) + F.label + 'を ' + esc(fmtVal(bk.field, bk.val)) + ' に変えました' + (res.ng.length ? ' (一部だめ)' : '') + '</h3>'
      + '<div class="bk-cards"><div class="bk-rc ok"><div class="k">変えた</div><div class="v" id="bk-rok">' + res.ok.length + '<small>件</small></div></div>'
      + '<div class="bk-rc ' + (res.ng.length ? 'ng' : '') + '"><div class="k">だめ</div><div class="v" id="bk-rng">' + res.ng.length + '<small>件</small></div></div>'
      + '<div class="bk-rc"><div class="k">変えなかった</div><div class="v" style="color:var(--text2)">' + res.skip + '<small>件</small></div></div></div>'
      + (res.ng.length ? '<div class="bk-sec" style="color:var(--err)">だめだった ' + res.ng.length + ' 件 (この分は前の値のまま) · 直し方ごと</div>' + failHtml : '<div class="callout info">' + ic('i-check', '') + '<div class="grow">全部変わりました。閉じると一覧を読み直し、いま変えた行が緑に光ります。</div></div>')
      + linkHtml;
    foot.innerHTML = '<button type="button" class="btn ghost" data-act="done">閉じる</button><span class="sp"></span>'
      + (retryN ? '<button type="button" class="btn pri lg" data-act="reselect">' + ic('i-redo', '') + '選び直せる ' + retryN + ' 件だけ選び直す</button>' : '');
    var h = $('#bk-h'); if (h) h.focus();
  }

  /* ---------- 引き出しの操作 ---------- */
  $('#bk-drawer').addEventListener('click', function (e) {
    if (!bk) return;
    var tile = e.target.closest('.bk-tile');
    if (tile) { if (!tile.classList.contains('none')) chooseField(tile.getAttribute('data-field')); return; }
    var a = e.target.closest('[data-act]');
    if (a) {
      var act = a.getAttribute('data-act');
      if (act === 'close') closeDrawer();
      else if (act === 'to1') { bk.step = 1; render(); }
      else if (act === 'to2') { bk.step = 2; bk.preview = null; bk.previewError = null; render(); }
      else if (act === 'to3') toPreview();
      else if (act === 'apply' && !a.disabled) startApply();
      else if (act === 'resume') sendMore();
      else if (act === 'recheck') { bk.run = null; bk.result = null; toPreview(); }
      else if (act === 'stop') finish();
      else if (act === 'done') { st.failed = []; save(); closeDrawer(true); }
      else if (act === 'reselect') {
        var G = CFG.groups || {};
        var codes = bk.result.ng.filter(function (x) { return G[x.error.group] && G[x.error.group].retry; }).map(function (x) { return x.code; });
        var keep = {};
        codes.forEach(function (c) { keep[c] = st.sel[c] || { k: null, s: null, f: CFG.fkey }; });
        st.sel = keep; st.failed = codes; st.ack = CFG.fkey; save();
        toast('選び直せる ' + codes.length + ' 件だけを選びました (赤い印の行)');
        closeDrawer(true);
      }
      return;
    }
    var seg = e.target.closest('.seg button[data-v]');
    if (seg && bk.step === 2) {
      var v = seg.getAttribute('data-v');
      bk.val = bk.field === 'sales_class' || bk.field === 'tax_rate' ? Number(v) : v;
      render(); return;
    }
    var pick = e.target.closest('[data-pickval]');
    if (pick && bk.step === 2 && pick.getAttribute('data-pickval') !== '') {
      var pv = pick.getAttribute('data-pickval');
      bk.val = bk.field === 'cost' || bk.field === 'standard_price' || bk.field === 'sales_class' || bk.field === 'tax_rate' ? Number(pv) : pv;
      render(); return;
    }
    var tb = e.target.closest('[data-tab]');
    if (tb && !tb.disabled) { bk.tab = tb.getAttribute('data-tab'); render(); var t2 = $('[data-tab="' + bk.tab + '"]'); if (t2) t2.focus(); }
  });
  $('#bk-drawer').addEventListener('input', function (e) {
    if (!bk) return;
    var t = e.target;
    if (t.id === 'bk-yen') { bk.val = parseYen(t.value); live(); }
    else if (t.id === 'bk-reason-other') { bk.reasonOther = t.value; live(); }
    else if (t.id === 'bk-stop') { bk.stop = t.value; live(); }
    else if (t.id === 'bk-supq') {
      bk.supQ = t.value;
      var pos = t.selectionStart; render(); var q = $('#bk-supq'); if (q) { q.focus(); try { q.setSelectionRange(pos, pos); } catch (err) { /* */ } }
    }
  });
  $('#bk-drawer').addEventListener('change', function (e) {
    if (!bk) return;
    var t = e.target;
    if (t.name === 'bk-rsn') {
      bk.reason = t.value;
      var o = $('#bk-reason-other'); if (o) { o.hidden = t.value !== ''; if (t.value === '') o.focus(); }
      live();
    } else if (t.name === 'bk-sup') { bk.val = t.value; live(); }
    else if (t.id === 'bk-ok') { bk.confirmed = t.checked; var b = $('#bk-apply'); if (b) b.disabled = !t.checked; }
  });
  $('#bk-drawer').addEventListener('focusout', function (e) {
    if (bk && e.target.id === 'bk-yen' && bk.val != null) e.target.value = yen(bk.val);
  });

  paint();
  ME.bulk = { selected: selCodes, state: function () { return bk; } };
})();
