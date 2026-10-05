/*
 * me-shell.js — マスタの入力 (新しいデザインの画面すべて) の共通の動き
 *   - 全体から探す (Ctrl+K): 商品・セット / Amazon SKU / 操作。コードがぴったり合えば既にある /api/lookup で名前を出す (新しい API は作らない)
 *   - キー: / = この画面の絞る欄 (一覧だけ)・Ctrl+K = 全体から探す・Ctrl+S = 保存 (保存のある画面だけ)・Esc = 閉じる・一覧は ↑↓ と Enter
 *   - 未保存のまま離れるときの確認: 画面を移る道 (リンク・検索・フォーム・ブラウザの戻る) を全部 requestNavigate に集める。
 *     閉じる・読み込み直す・ほかのサイトへは beforeunload。未保存の数は画面 (me-sku.js) が MasterEdit.dirty() で返す (data-dirty-field の欄だけ)
 *   - 窓 (全体から探す・離れるときの確認) の中だけで Tab が回る・閉じたら元の場所へフォーカスを戻す
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function icon(id) { return '<svg class="ic" aria-hidden="true"><use href="#' + id + '"/></svg>'; }

  /**
   * 検索の同一視 (サーバーの apps/master-edit/search-fold.mjs の foldSearch と同じ決まり):
   * NFKC (半角カナ → 全角・全角英数 → 半角) → 小文字 → ひらがな → カタカナ。長音・小さい文字・濁点の有無は変えない
   */
  function fold(s) {
    s = String(s == null ? '' : s);
    if (s.normalize) s = s.normalize('NFKC');
    return s.toLowerCase().replace(/[ぁ-ゖゝゞ]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) + 0x60); });
  }

  var ME = window.MasterEdit = window.MasterEdit || {};
  ME.fold = fold;
  /** 画面が上書きする: 未保存の変更 { n, items: [文字], impacts: [文字] } */
  ME.dirty = ME.dirty || function () { return { n: 0, items: [], impacts: [] }; };
  ME.onSave = ME.onSave || null;      // Ctrl+S (保存のある画面だけ)
  ME.review = ME.review || null;      // 「変更内容を確認する」= 保存の箱へ

  function toast(t) {
    var el = $('#toast'); if (!el) return;
    $('#toast-t').textContent = t; el.classList.add('on');
    clearTimeout(toast._t); toast._t = setTimeout(function () { el.classList.remove('on'); }, 3200);
  }
  ME.toast = toast;

  /* ---------- 未保存の札 ---------- */
  ME.setUnsaved = function (n) {
    var u = $('#unsaved'); if (u) { u.hidden = n === 0; $('#unsaved-n').textContent = '未保存 ' + n + ' 件'; }
    var st = $('#sticky-unsaved'); if (st) { st.hidden = n === 0; st.textContent = '● 未保存 ' + n + ' 件'; }
    $$('.mb-n').forEach(function (x) { x.textContent = n ? '● 未保存 ' + n + ' 件' : '未保存の変更はありません'; x.style.color = n ? 'var(--warn)' : ''; });
    if (n > 0) armBackGuard();
  };
  function goReview() { if (ME.review) ME.review(); }
  document.addEventListener('click', function (e) {
    if (e.target.closest && (e.target.closest('#unsaved') || e.target.closest('#sticky-unsaved'))) { e.preventDefault(); goReview(); }
  });

  /* ---------- 窓の中だけで Tab が回る ---------- */
  function openBox() { return $('#leave-bg.on .modal') || $('#palette-bg.on .palette'); }
  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Tab') return;
    var box = openBox(); if (!box) return;
    var f = $$('button, input, a[href], select, textarea, [tabindex="0"]', box).filter(function (x) { return !x.disabled && x.offsetParent !== null; });
    if (!f.length) return;
    if (!box.contains(document.activeElement)) { e.preventDefault(); f[0].focus(); return; }
    if (e.shiftKey && document.activeElement === f[0]) { e.preventDefault(); f[f.length - 1].focus(); }
    else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { e.preventDefault(); f[0].focus(); }
  });

  /* ---------- 離れるときの確認 ---------- */
  var leaving = false;            // 「捨てて移る」を押した後 (beforeunload を出さない)
  var pendingGo = null;           // 移り先 (URL 文字 / 'back')
  var leaveReturn = null;         // 「ここに残る」で戻すフォーカス
  function openLeave(go, from) {
    var d = ME.dirty();
    pendingGo = go; leaveReturn = from || document.activeElement;
    $('#leave-n').textContent = String(d.n);
    $('#leave-list').innerHTML = (d.items.length ? d.items : ['この画面で変えた欄 ' + d.n + ' つ']).map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('');
    $('#leave-imp').hidden = !d.impacts.length;
    $('#leave-imp-list').innerHTML = d.impacts.map(function (t) { return '<li>' + esc(t) + '</li>'; }).join('');
    closePalette(true);
    $('#leave-bg').classList.add('on');
    $('#leave-stay').focus();
  }
  function closeLeave(restore) {
    var bg = $('#leave-bg'); if (!bg || !bg.classList.contains('on')) return;
    bg.classList.remove('on');
    if (restore && leaveReturn && leaveReturn.focus && document.contains(leaveReturn)) leaveReturn.focus();
  }
  function go(url) {
    leaving = true;
    if (url === 'back') { history.go(backArmed ? -2 : -1); return; }
    if (url && url.nodeName === 'FORM') { url.submit(); return; }
    location.href = url;
  }
  /** 画面を移るときはすべてここを通す */
  function requestNavigate(url, from) {
    if (!leaving && ME.dirty().n > 0) { openLeave(url, from); return false; }
    go(url); return true;
  }
  ME.requestNavigate = requestNavigate;
  /**
   * 画面を読み直す (保存が通った後。未保存は 0 にしてから呼ぶ)。離れるときの確認は出さない。
   * 戻るの見張りで足した履歴 (armBackGuard) があれば先に 1 つ戻してから読み直す = 読み直した後の「戻る」1 回で前の画面へ (同じ画面が 2 つ並ばない)
   */
  ME.reloadPage = function () {
    leaving = true;
    if (!backArmed) { location.reload(); return; }
    backArmed = false;
    var done = false;
    var fin = function () { if (done) return; done = true; location.reload(); };
    window.addEventListener('popstate', fin);
    setTimeout(fin, 500);   // popstate が来ない (履歴が違う) ときもそのまま読み直す
    history.back();
  };
  var stay = $('#leave-stay'), drop = $('#leave-drop'), rev = $('#leave-review');
  if (stay) stay.addEventListener('click', function () { closeLeave(true); });
  if (drop) drop.addEventListener('click', function () { closeLeave(false); go(pendingGo); });
  if (rev) rev.addEventListener('click', function () { closeLeave(false); goReview(); });
  var lbg = $('#leave-bg');
  if (lbg) lbg.addEventListener('click', function (e) { if (e.target === lbg) closeLeave(true); });

  // リンク (左の列・パンくず・一覧・見出しのボタン など)。新しいタブ・ダウンロード・同じページの # は見ない
  document.addEventListener('click', function (e) {
    if (e.defaultPrevented || e.button !== 0 || e.ctrlKey || e.metaKey || e.shiftKey || e.altKey) return;
    var a = e.target.closest && e.target.closest('a[href]');
    if (!a || a.target === '_blank' || a.hasAttribute('download')) return;
    var href = a.getAttribute('href');
    if (!href || href.charAt(0) === '#') return;
    if (ME.dirty().n > 0 && !leaving) { e.preventDefault(); openLeave(a.href, a); }
  }, true);
  // フォーム (一覧の絞り込み など) を送るときも同じ
  document.addEventListener('submit', function (e) {
    var f = e.target;
    if (!f || f.id === 'f' || (f.method || 'get').toLowerCase() !== 'get') return;
    if (ME.dirty().n > 0 && !leaving) { e.preventDefault(); openLeave(f, document.activeElement); }
  }, true);
  // 閉じる・読み込み直す・アドレスを打って移る
  window.addEventListener('beforeunload', function (e) {
    if (leaving || ME.dirty().n === 0) return;
    e.preventDefault(); e.returnValue = '';
  });
  // ブラウザの戻る: 未保存になった時に 1 つ履歴を足しておき、戻ったら確認を出す (その間は同じページのまま)
  var backArmed = false;
  function armBackGuard() {
    if (backArmed || !window.history || !history.pushState) return;
    try { history.pushState({ meGuard: true }, '', location.href); backArmed = true; } catch (err) { /* 履歴に足せないときは beforeunload だけ */ }
  }
  window.addEventListener('popstate', function (e) {
    if (!backArmed || leaving) return;
    if (e.state && e.state.meGuard) return;          // 進むで戻ってきた
    if (ME.dirty().n > 0) {
      try { history.pushState({ meGuard: true }, '', location.href); } catch (err) { /* */ }
      openLeave('back', document.activeElement);
    } else {
      backArmed = false; leaving = true; history.back();
    }
  });

  /* ---------- 全体から探す (Ctrl+K) ---------- */
  var palBg = $('#palette-bg'), palIn = $('#pal-in'), palRes = $('#pal-res');
  var BASE = palBg ? ($('.palette', palBg).getAttribute('data-base') || '') : '';
  var OPS = [
    { ico: 'i-grid', t: '商品・セットの一覧', go: BASE + '/' },
    { ico: 'i-plus', t: '新しい単品を登録する', go: BASE + '/new?kind=single' },
    { ico: 'i-box', t: '新しいセットを登録する', go: BASE + '/new?kind=set' },
    { ico: 'i-send', t: 'NE 登録の CSV を開く', go: BASE + '/reg-csv' },
    { ico: 'i-cart', t: 'Amazon SKU の対応の一覧', go: BASE + '/amazon/' },
    { ico: 'i-cart', t: '売れたのに対応が無い Amazon SKU', go: BASE + '/amazon/unmapped' },
    { ico: 'i-scale', t: 'マスタの判断 (NE との差)', go: '/apps/master-decisions/' },
    { ico: 'i-book', t: 'つかいかた', go: BASE + '/manual' }
  ];
  var palItems = [], palSel = 0, palReturn = null, palHit = null, palSeq = 0, palTimer = null;
  function hl(s, q) {
    s = String(s || ''); if (!q) return esc(s);
    var i = s.toLowerCase().indexOf(q.toLowerCase()); if (i < 0) return esc(s);
    return esc(s.slice(0, i)) + '<mark>' + esc(s.slice(i, i + q.length)) + '</mark>' + esc(s.slice(i + q.length));
  }
  function renderPal() {
    var q = palIn.value.trim();
    palItems = [];
    if (palHit && q && palHit.q === q) palItems.push({ g: '商品・セット', ico: palHit.item.kind === 'set' ? 'i-box' : 'i-tag', code: palHit.item.code, t: palHit.item.name, m: (palHit.item.kind === 'set' ? 'セット' : '単品') + (palHit.item.standard_price != null ? ' · ' + Number(palHit.item.standard_price).toLocaleString('ja-JP') + ' 円' : ''), go: BASE + '/sku/' + encodeURIComponent(palHit.item.code) });
    if (q) {
      palItems.push({ g: '商品・セット', ico: 'i-search', t: '「' + q + '」で商品・セットを絞る (コード・名前・JAN)', go: BASE + '/?q=' + encodeURIComponent(q) });
      palItems.push({ g: 'Amazon SKU', ico: 'i-cart', t: '「' + q + '」で Amazon SKU の対応を探す', go: BASE + '/amazon/?q=' + encodeURIComponent(q) });
      if (/^[\x21-\x7e]{1,100}$/.test(q)) palItems.push({ g: 'Amazon SKU', ico: 'i-cart', code: q.toLowerCase(), t: 'この seller SKU を開く', go: BASE + '/amazon/sku?sku=' + encodeURIComponent(q) });
    }
    OPS.forEach(function (o) { if (!q || fold(o.t).indexOf(fold(q)) >= 0) palItems.push({ g: '操作', ico: o.ico, t: o.t, go: o.go }); });
    palSel = Math.max(0, Math.min(palSel, palItems.length - 1));
    var h = '', g = '';
    palItems.forEach(function (p, i) {
      if (p.g !== g) { g = p.g; h += '<div class="pg" role="presentation">' + esc(g) + '</div>'; }
      h += '<div class="pi' + (i === palSel ? ' sel' : '') + '" data-i="' + i + '" id="pal-opt-' + i + '" role="option" aria-selected="' + (i === palSel) + '"><span class="ico">' + icon(p.ico) + '</span>'
        + (p.code ? '<span class="mono">' + hl(p.code, q) + '</span>' : '') + '<span>' + hl(p.t, q) + '</span><span class="pm">' + esc(p.m || '') + (i === palSel ? ' <span class="kbd">Enter</span>' : '') + '</span></div>';
    });
    palRes.innerHTML = h || '<div class="pg">見つかりません</div>';
    if (palItems.length) palIn.setAttribute('aria-activedescendant', 'pal-opt-' + palSel); else palIn.removeAttribute('aria-activedescendant');
    var sel = $('#pal-opt-' + palSel); if (sel && sel.scrollIntoView) sel.scrollIntoView({ block: 'nearest' });
  }
  function lookupLater() {
    clearTimeout(palTimer);
    var q = palIn.value.trim();
    if (!q || q.length > 60) { palHit = null; return; }
    palTimer = setTimeout(function () {
      var my = ++palSeq;
      fetch(BASE + '/api/lookup?code=' + encodeURIComponent(q), { headers: { Accept: 'application/json' } })
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (j) { if (my !== palSeq) return; palHit = j && j.ok && j.item ? { q: q, item: j.item } : null; if (palIn.value.trim() === q) renderPal(); })
        .catch(function () { /* 見つからない・つながらない = 候補を足さないだけ */ });
    }, 180);
  }
  function openPalette(q) {
    if (!palBg || $('#leave-bg.on')) return;
    palReturn = document.activeElement;
    palBg.classList.add('on'); palIn.setAttribute('aria-expanded', 'true');
    palIn.value = q || ''; palSel = 0; palHit = null; renderPal(); palIn.focus();
  }
  function closePalette(noRestore) {
    if (!palBg || !palBg.classList.contains('on')) return;
    palBg.classList.remove('on'); palIn.setAttribute('aria-expanded', 'false'); palIn.removeAttribute('aria-activedescendant');
    if (!noRestore && palReturn && palReturn.focus && document.contains(palReturn)) palReturn.focus();
  }
  function choose(i) { var p = palItems[i]; if (!p) return; var from = palReturn; closePalette(true); requestNavigate(p.go, from); }
  if (palBg) {
    $('#open-palette').addEventListener('click', function () { openPalette(''); });
    palIn.addEventListener('input', function () { palSel = 0; renderPal(); lookupLater(); });
    palIn.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown') { e.preventDefault(); palSel = Math.min(palItems.length - 1, palSel + 1); renderPal(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); palSel = Math.max(0, palSel - 1); renderPal(); }
      else if (e.key === 'Enter') { e.preventDefault(); choose(palSel); }
    });
    palBg.addEventListener('click', function (e) {
      if (e.target === palBg) { closePalette(); return; }
      var it = e.target.closest('.pi'); if (it) choose(Number(it.getAttribute('data-i')));
    });
  }

  /* ---------- キー ---------- */
  document.addEventListener('keydown', function (e) {
    var t = e.target, tag = (t.tagName || '').toLowerCase();
    var typing = tag === 'input' || tag === 'textarea' || tag === 'select' || t.isContentEditable;
    if (e.key === 'Escape') {
      if ($('#leave-bg.on')) { e.preventDefault(); closeLeave(true); return; }
      if (palBg && palBg.classList.contains('on')) { e.preventDefault(); closePalette(); return; }
      return;
    }
    if ((e.ctrlKey || e.metaKey) && !e.altKey && (e.key === 'k' || e.key === 'K')) { e.preventDefault(); openPalette(''); return; }
    if ((e.ctrlKey || e.metaKey) && !e.altKey && (e.key === 's' || e.key === 'S')) {
      if (ME.onSave) { e.preventDefault(); if (!openBox()) ME.onSave(); }
      return;
    }
    if (openBox()) return;
    // 一覧の絞る欄にいるときの Enter = 絞る (「絞る」を押すのと同じ)。表の行にいるときの Enter = その行を開く (リンクそのもの)。
    // IME の変換を確かめる Enter (isComposing・keyCode 229) では送らない = 字が確かまるだけ
    if (e.key === 'Enter' && t.id === 'q' && t.form && !e.isComposing && e.keyCode !== 229 && !e.ctrlKey && !e.metaKey && !e.altKey && !e.shiftKey) {
      e.preventDefault();
      if (t.form.requestSubmit) t.form.requestSubmit(); else t.form.submit();
      return;
    }
    // / = この画面の絞る欄 (一覧だけ。無い画面では何もしない)
    if (e.key === '/' && !typing && !e.ctrlKey && !e.metaKey && !e.altKey) {
      var local = $('#q'); if (local) { e.preventDefault(); local.focus(); local.select(); }
      return;
    }
    // 一覧: 絞る欄から ↓ で表へ・表の中は ↑↓ で行を移る (Enter はリンクそのもの)
    var tbl = $('#list-tbl');
    if (tbl && (e.key === 'ArrowDown' || e.key === 'ArrowUp')) {
      var links = $$('#list-tbl tbody a.rowlink'); if (!links.length) return;
      var i = links.indexOf(document.activeElement);
      if (t.id === 'q' && e.key === 'ArrowDown') { e.preventDefault(); links[0].focus(); return; }
      if (i < 0) return;
      e.preventDefault();
      if (e.key === 'ArrowUp' && i === 0) { var q = $('#q'); if (q) q.focus(); return; }
      links[Math.max(0, Math.min(links.length - 1, i + (e.key === 'ArrowDown' ? 1 : -1)))].focus();
    }
  });

  /* ---------- 共通の部品: 切り替えボタン・数の上げ下げ ---------- */
  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('.seg button');
    if (b && !b.disabled) {
      var seg = b.parentElement;
      $$('button', seg).forEach(function (x) { x.classList.remove('on'); x.setAttribute('aria-pressed', 'false'); });
      b.classList.add('on'); b.setAttribute('aria-pressed', 'true');
      seg.setAttribute('data-value', b.getAttribute('data-v') || '');
      seg.dispatchEvent(new Event('change', { bubbles: true }));
    }
    var st = e.target.closest && e.target.closest('.stepper button');
    if (st && !st.disabled) {
      var inp = $('input', st.parentElement); if (!inp || inp.disabled) return;
      var cur = parseFloat(String(inp.value).replace(/[０-９．]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) - 0xFEE0); })) || 0;
      var n = Math.max(0, Math.min(60, Math.round((cur + Number(st.getAttribute('data-step'))) * 10) / 10));
      inp.value = String(n); inp.dispatchEvent(new Event('input', { bubbles: true }));
    }
    var j = e.target.closest && e.target.closest('[data-jump]');
    if (j) { var to = document.getElementById(j.getAttribute('data-jump')); if (to) { to.scrollIntoView({ behavior: 'smooth', block: 'center' }); to.focus({ preventScroll: true }); } }
  });

  /* ---------- 一覧に戻る: 最後に見た一覧 (絞り込み・ページ) を覚え、1 つの商品の画面のパンくず「商品・セット」をそこへ向ける ---------- */
  var LIST_KEY = 'master-edit:list-url';
  if ($('#list-tbl')) { try { sessionStorage.setItem(LIST_KEY, location.pathname + location.search); } catch (err) { /* 覚えられない = いつもの一覧へ */ } }
  var back = $('a[data-list-back]');
  if (back) {
    try {
      var last = sessionStorage.getItem(LIST_KEY), basePath = back.pathname;
      // 同じ一覧 (同じ path) の絞り込みだけ使う (よその URL へは向けない)
      if (last && last.split('?')[0] === basePath && last.indexOf('?') > 0) back.setAttribute('href', last);
    } catch (err) { /* そのまま */ }
  }

  /* ---------- 一覧の絞り込みを送った = 読み込み中と分かるように (一覧の読み込みに時間がかかっても押せたと分かる) ---------- */
  document.addEventListener('submit', function (e) {
    var f = e.target;
    if (e.defaultPrevented || !f || f.id !== 'list-search') return;
    f.classList.add('busy'); f.setAttribute('aria-busy', 'true');
    var b = $('.go', f); if (b) b.textContent = '絞っています…';   // disabled にはしない (送っている途中のボタンを閉じない)
  });
  /*
   * 詳細検索: 条件が長い (商品コード 500 件など) と GET の URL が上限を超えて HTTP 431 になる (#1620 Codex R1 M2)。
   * 短いときは今までどおり GET (URL に条件がそのまま残る)。長いときは POST api/search (保存と同じ Origin・JSON の守り) で
   * 条件を印にしてもらい、?s=<印> の URL を開く
   */
  var ADV_URL_MAX = 1800;
  document.addEventListener('submit', function (e) {
    var f = e.target;
    if (e.defaultPrevented || !f || f.id !== 'adv-form' || !window.FormData || !window.fetch) return;
    var body = {}, qs = new URLSearchParams();
    new FormData(f).forEach(function (v, k) { if (String(v).trim() !== '') { body[k] = String(v); qs.append(k, String(v)); } });
    if ((location.pathname + '?' + qs.toString()).length <= ADV_URL_MAX) return;   // 短い = GET のまま
    e.preventDefault();
    var b = $('button[type="submit"]', f); if (b) b.disabled = true;
    fetch(new URL(f.getAttribute('data-api') || 'api/search', location.href).href, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }); })
      .then(function (j) {
        if (j && j.ok && j.url) { requestNavigate(j.url, b); return; }
        if (b) b.disabled = false; toast('検索の条件を送れませんでした' + (j && (j.message || j.error) ? ' (' + (j.message || j.error) + ')' : ''));
      })
      .catch(function () { if (b) b.disabled = false; toast('通信できませんでした。もう一度「この条件で探す」を押してください'); });
  });
  // 戻るで戻ってきた (bfcache) ときは元に戻す
  window.addEventListener('pageshow', function () {
    var ab = $('#adv-form button[type="submit"]'); if (ab) ab.disabled = false;
    var f = $('#list-search'); if (!f) return;
    f.classList.remove('busy'); f.removeAttribute('aria-busy');
    var b = $('.go', f); if (b) b.textContent = '絞る';
  });

  /*
   * ---------- 一覧の見出しの行 (コード・名前・…) をスクロールしても上の帯の下に残す ----------
   * position: sticky は使えない: 表の囲い (.tblwrap) が横に送る囲い (overflow-x: auto = 縦も overflow の箱になる) なので、
   * sticky はその囲いの中でしか効かない (囲いは縦に送らない = 何も起きない)。囲いを外すと 1024 幅・拡大で表がページの横にはみ出す。
   * → 見出しのセルを、表の上端が帯の下に隠れた分だけ下へずらす (transform。横に送る囲いの中のままなので横の位置もずれない)
   */
  var listTbl = $('#list-tbl');
  if (listTbl && listTbl.tHead) {
    var hdrBar = $('.hdr');
    var headTick = false;
    var placeHead = function () {
      headTick = false;
      var top = hdrBar ? hdrBar.getBoundingClientRect().bottom : 0;
      var r = listTbl.getBoundingClientRect();
      var cell = listTbl.tHead.rows[0] && listTbl.tHead.rows[0].cells[0];
      if (!cell) return;
      var cur = parseFloat(listTbl.style.getPropertyValue('--head-y')) || 0;
      var cr = cell.getBoundingClientRect();
      var natural = cr.top - cur;          // ずらしていないときの見出しの上端 (表の上端と少し違うことがある = セルの実際の位置で測る)
      var y = Math.max(0, Math.min(top - natural, r.bottom - cr.height - natural));
      listTbl.style.setProperty('--head-y', y + 'px');
      listTbl.classList.toggle('head-stuck', y > 0);
    };
    var askHead = function () { if (!headTick) { headTick = true; requestAnimationFrame(placeHead); } };
    window.addEventListener('scroll', askHead, { passive: true });
    window.addEventListener('resize', askHead);
    // 画面の出だしの動き (.page の rise = 4px 上がる) が終わった後もそろえ直す (動きの途中に測ると数 px ずれたまま残る)
    document.addEventListener('animationend', askHead, true);
    placeHead();
  }

  /* ---------- スクロールしても何の商品か分かる小見出し ---------- */
  var ph = $('#sku-ph'), sticky = $('#sku-sticky');
  if (ph && sticky && window.IntersectionObserver) {
    new IntersectionObserver(function (en) { sticky.hidden = en[0].isIntersecting; }, { rootMargin: '-58px 0px 0px 0px' }).observe(ph);
  }
})();
