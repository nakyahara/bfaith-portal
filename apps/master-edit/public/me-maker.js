/*
 * me-maker.js — メーカー (製造元) の欄 (中原さん 2026-10-17・Company DB の 0083・部品 = views/_maker.ejs)
 *   新商品の登録 (単品)・色違い・サイズ違いのまとまり・商品の画面 (単品) の 3 つで同じ動き。
 *   - 打つと名簿から探す: かなの同一視 (サーバーの apps/master-edit/search-fold.mjs と同じ = NFKC・小文字・ひらがな → カタカナ) +
 *     空白で区切った言葉が全部入っている。日本語の変換中も探す。確定の Enter (Windows の Chrome・Edge は確定の後に keyCode 13 が来る) では選ばない
 *     (#1724 の Amazon の候補と同じ = compositionend の後、次のキーを離すまでの 1 秒以内の Enter は確定の Enter)
 *   - 選ぶ = ↑↓ と Enter・押す・タップ。打っただけで選んでいない名前は値にしない (送るのは隠した欄 #f-maker = 名簿の番号)
 *   - 名簿に無い = 候補の最後の「＋ 〇〇 を新しいメーカーとして登録」→ 確かめて名簿に足す (POST /api/makers)。
 *     同じと見る名前 (上の同一視 + 空白を除く = DB の core.maker_name_norm と同じ) が既にあれば足さずにそれを選ぶ (重ねない)
 *   - 「仕入先と同じ」= 代表の仕入先の欄で選んでいる仕入先の名前のメーカー (名簿に無ければ確かめて足す)
 *   - 「入力なし」(新商品の登録・public/me-none.js) を押す = メーカーを空にする。メーカーを選ぶ = 入力なしは外れる (me-none が隠した欄の input で外す)
 *   画面の JS に渡すもの: MasterEdit.maker = { label(番号), set(番号), sync(), value() }。値が変わったら MasterEdit.recount() (画面の数え直し)
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var dataEl = document.getElementById('me-maker');
  var inp = document.getElementById('f-maker-in');
  var hid = document.getElementById('f-maker');
  var list = document.getElementById('f-maker-list');
  var sameBtn = document.getElementById('f-maker-same');
  var msgEl = document.getElementById('f-maker-msg');
  if (!dataEl || !inp || !hid || !list) return;
  var P = JSON.parse(dataEl.textContent);
  var BASE = P.base || '';
  var MAX = P.max || 100;
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  /** かなの同一視 (search-fold.mjs の foldSearch と同じ) */
  function fold(s) { return String(s == null ? '' : s).normalize('NFKC').toLowerCase().replace(/[ぁ-ゖゝゞ]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) + 0x60); }); }
  /** 名前の掃除 (lib/master-maker.mjs makerNameClean = DB の core.maker_name_clean と同じ) */
  function clean(s) { return String(s == null ? '' : s).replace(/[ 　 ]+/g, ' ').trim(); }
  /** 同じと見る形 (lib/master-maker.mjs makerNameNorm = DB の core.maker_name_norm と同じ) */
  function norm(s) { return fold(clean(s)).replace(/ /g, ''); }
  var MAKERS = (P.makers || []).map(function (m) { return { id: String(m[0]), name: m[1], f: fold(m[1]), k: norm(m[1]) }; });
  function byId(id) { id = String(id || ''); for (var i = 0; i < MAKERS.length; i++) if (MAKERS[i].id === id) return MAKERS[i]; return null; }
  function byNorm(k) { for (var i = 0; i < MAKERS.length; i++) if (MAKERS[i].k === k) return MAKERS[i]; return null; }
  function add(m) { var x = byId(m.id); if (x) return x; x = { id: String(m.id), name: m.name, f: fold(m.name), k: norm(m.name) }; MAKERS.push(x); return x; }

  var picked = byId(hid.value);   // 今選んでいるメーカー (無い = null)
  if (!picked && hid.value) picked = { id: hid.value, name: inp.value };
  function say(t, cls) { if (!msgEl) return; msgEl.className = 'fmsg' + (cls ? ' ' + cls : ''); msgEl.textContent = t || ''; }
  function recount() { if (typeof ME.recount === 'function') ME.recount(); }
  /** 値を決める (m = { id, name } / null = なし)。keepText = 打った名前を残す (選んだのをやめただけ) */
  function setVal(m, keepText) {
    picked = m || null;
    if (!keepText) inp.value = m ? m.name : '';
    var v = m ? String(m.id) : '';
    if (hid.value !== v) {
      hid.value = v;
      // 入力なし (me-none.js) は input で外す・画面の未保存と数え直しは input / change で
      hid.dispatchEvent(new Event('input', { bubbles: true }));
      hid.dispatchEvent(new Event('change', { bubbles: true }));
    }
    inp.setAttribute('aria-invalid', 'false');
    recount();
  }

  /* ---------- 候補 ---------- */
  var S = { items: [], sel: -1, q: '' };
  function panelOf() { return list.closest('.panel'); }
  function close() {
    list.hidden = true; list.innerHTML = ''; S.items = []; S.sel = -1;
    inp.setAttribute('aria-expanded', 'false'); inp.removeAttribute('aria-activedescendant');
    var p = panelOf(); if (p) p.classList.remove('sug-on');
  }
  function mark(text, words) {
    var t = fold(text);
    if (t.length !== text.length) return esc(text);   // 字の数が変わる文字 (半角カナの濁点など) = 印を付けない (ずれた所に付けない)
    var rs = [];
    words.forEach(function (w) { var k = fold(w); if (!k) return; var i = t.indexOf(k); if (i >= 0) rs.push([i, i + k.length]); });
    if (!rs.length) return esc(text);
    rs.sort(function (a, b) { return a[0] - b[0]; });
    var out = '', at = 0;
    rs.forEach(function (r) { if (r[0] < at) return; out += esc(text.slice(at, r[0])) + '<mark>' + esc(text.slice(r[0], r[1])) + '</mark>'; at = r[1]; });
    return out + esc(text.slice(at));
  }
  function setSel(i) {
    S.sel = i;
    Array.prototype.forEach.call(list.querySelectorAll('.co'), function (o, k) { o.classList.toggle('sel', k === i); o.setAttribute('aria-selected', k === i ? 'true' : 'false'); });
    if (i >= 0) inp.setAttribute('aria-activedescendant', 'mk-opt-' + i); else inp.removeAttribute('aria-activedescendant');
    var o = document.getElementById('mk-opt-' + i); if (o && o.scrollIntoView) o.scrollIntoView({ block: 'nearest' });
  }
  /** 打った言葉で名簿を探して候補を出す (名簿は画面にある = 問い合わせない) */
  function render() {
    var q = clean(inp.value);
    S.q = q;
    if (!q) { close(); return; }
    var words = q.split(' ').filter(Boolean);
    var k = norm(q);
    var hits = MAKERS.filter(function (m) { return words.every(function (w) { return m.f.indexOf(fold(w)) >= 0 || m.k.indexOf(norm(w)) >= 0; }) || m.k.indexOf(k) >= 0; });
    hits.sort(function (a, b) {
      var ra = a.k === k ? 0 : a.k.indexOf(k) === 0 ? 1 : 2, rb = b.k === k ? 0 : b.k.indexOf(k) === 0 ? 1 : 2;
      return ra - rb || (a.k < b.k ? -1 : a.k > b.k ? 1 : 0);
    });
    var more = hits.length > 30;
    hits = hits.slice(0, 30);
    var exact = byNorm(k);
    S.items = hits.map(function (m) { return { m: m }; });
    var tooLong = Array.from(q).length > MAX;
    if (!exact && P.canCreate && !tooLong) S.items.push({ create: q });
    var h = '';
    if (hits.length) h += '<div class="cs-h"><span>メーカー ' + hits.length + ' 件' + (more ? ' (上から 30 件。もっと打つと絞れます)' : '') + '</span></div>';
    else h += '<div class="cs-empty">「' + esc(q) + '」に当たるメーカーは名簿にありません</div>';
    S.items.forEach(function (x, i) {
      if (x.create) {
        h += '<div class="co mk-new" role="option" id="mk-opt-' + i + '" data-i="' + i + '" aria-selected="false"><div class="cn">＋ 「' + esc(x.create) + '」を新しいメーカーとして登録</div></div>';
      } else {
        h += '<div class="co" role="option" id="mk-opt-' + i + '" data-i="' + i + '" aria-selected="false"><div class="cn">' + mark(x.m.name, words) + (x.m.k === k ? ' <span class="b mute">同じ名前</span>' : '') + '</div></div>';
      }
    });
    if (tooLong) h += '<div class="cs-empty bad">メーカーの名前は ' + MAX + ' 字までです</div>';
    list.innerHTML = h;
    list.hidden = false;
    inp.setAttribute('aria-expanded', 'true');
    var p = panelOf(); if (p) p.classList.add('sug-on');
    setSel(exact ? 0 : -1);
  }
  function move(d) {
    if (!S.items.length) return;
    var i = S.sel + d;
    if (i < -1) i = -1;
    if (i >= S.items.length) i = S.items.length - 1;
    setSel(i);
  }
  function choose(i) {
    var x = S.items[i];
    if (!x) return;
    close();
    if (x.create) { createAndPick(x.create, false); return; }
    setVal(x.m);
    say('');
  }

  /* ---------- 名簿に足す ---------- */
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  var busy = false;
  var lastReq = null;   // { k, id } 通信が切れたときに同じ名前で押し直す = 同じ番号 (2 つ作らない)
  function createAndPick(name0, fromSupplier) {
    var name = clean(name0);
    if (!name || busy) return;
    var hit = byNorm(norm(name));
    if (hit) { setVal(hit); say((fromSupplier ? '仕入先と同じ' : '名簿にある') + '「' + hit.name + '」にしました', 'ok'); return; }
    if (!P.canCreate) { say('このメーカーは名簿にありません (名簿に足せるのは名簿の人だけです)', 'warn'); return; }
    var ask = (fromSupplier ? '代表の仕入先と同じ名前' : '') + '「' + name + '」を新しいメーカーとして名簿に登録しますか？\n(登録すると、ほかの商品でも選べるようになります)';
    if (!window.confirm(ask)) { say('登録しませんでした', ''); return; }
    var k = norm(name);
    if (!lastReq || lastReq.k !== k) lastReq = { k: k, id: uuid() };
    busy = true; say('登録しています…', 'info');
    if (sameBtn) sameBtn.disabled = true;
    fetch(BASE + '/api/makers', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ request_id: lastReq.id, name: name }) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false; if (sameBtn) sameBtn.disabled = false;
        lastReq = null;   // 返事が来た = 次は新しい番号
        if (!x.r.ok || !x.j.ok || !x.j.maker) { say(x.j.error || ('登録できませんでした (HTTP ' + x.r.status + ')'), 'err'); return; }
        var m = add(x.j.maker);
        setVal(m);
        say(x.j.existed ? '名簿にもうある「' + m.name + '」を選びました (同じ名前と見なしました)' : '「' + m.name + '」を名簿に登録して選びました', 'ok');
      })
      .catch(function () { busy = false; if (sameBtn) sameBtn.disabled = false; say('通信できませんでした。もう一度押してください (同じ名前は 2 つ登録されません)', 'err'); });
  }

  /* ---------- 仕入先と同じ ---------- */
  function supplierName() {
    var sel = P.supplierSel ? $(P.supplierSel) : null;
    var code = sel ? sel.value : (P.supplierNow || '');
    if (!code) return { code: '', name: '' };
    for (var i = 0; i < (P.suppliers || []).length; i++) if (P.suppliers[i][0] === code) return { code: code, name: P.suppliers[i][1] || '' };
    return { code: code, name: '' };
  }
  if (sameBtn && !sameBtn.disabled) sameBtn.addEventListener('click', function () {
    var s = supplierName();
    if (!s.code) { say('先に代表の仕入先を選んでください', 'warn'); var sel = P.supplierSel ? $(P.supplierSel) : null; if (sel && !sel.disabled) sel.focus(); return; }
    if (!clean(s.name)) { say('仕入先 ' + s.code + ' の名前が分かりません (打って選んでください)', 'warn'); return; }
    createAndPick(s.name, true);
  });

  /* ---------- 欄の動き ---------- */
  var imeEndAt = -1;   // 確定した時刻 (次のキーを離したら消す)
  if (!inp.disabled) {
    inp.addEventListener('input', function () {
      // 打ち直した = 選んでいたメーカーはやめる (打った名前は残す・選ぶまで値にしない)。変換中も探す (確定の前から候補が出る)
      if (picked && inp.value !== picked.name) setVal(null, true);
      say('');
      render();
    });
    inp.addEventListener('compositionend', function (e) { imeEndAt = e.timeStamp; render(); });
    inp.addEventListener('keyup', function () { imeEndAt = -1; });
    inp.addEventListener('keydown', function (e) {
      var confirmed = imeEndAt >= 0 && e.timeStamp - imeEndAt < 1000;
      if (e.isComposing || e.keyCode === 229) return;                                      // 変換中の ↑↓ と Enter は日本語入力のもの (触らない)
      imeEndAt = -1;
      if (confirmed && e.key === 'Enter') { e.preventDefault(); return; }                   // 日本語入力の確定の Enter = 字が確かまるだけ (選ばない)
      var open = !list.hidden;
      if (e.key === 'ArrowDown') { e.preventDefault(); if (!open) render(); else move(1); return; }
      if (e.key === 'ArrowUp' && open) { e.preventDefault(); move(-1); return; }
      if (e.key === 'Enter') { e.preventDefault(); if (open && S.sel >= 0) choose(S.sel); else if (clean(inp.value) && !picked) render(); return; }
      if (e.key === 'Escape' && open) { e.preventDefault(); e.stopPropagation(); close(); return; }
      if (e.key === 'Tab') close();
    });
    inp.addEventListener('focus', function () { if (clean(inp.value) && !picked) render(); });
    inp.addEventListener('blur', function () {
      setTimeout(function () {
        if (document.activeElement === inp) return;
        close();
        if (clean(inp.value) && !picked) { say('一覧から選ぶか「新しいメーカーとして登録」を押してください (選ぶまでメーカーは入りません)', 'warn'); inp.setAttribute('aria-invalid', 'true'); }
        else if (!clean(inp.value) && inp.value) inp.value = '';
      }, 0);
    });
    // 押した所へフォーカスを移さない (欄から出ない = 閉じない)。タッチも同じ
    list.addEventListener('mousedown', function (e) { e.preventDefault(); });
    list.addEventListener('click', function (e) { var o = e.target.closest('.co'); if (o) choose(Number(o.getAttribute('data-i'))); });
  }
  // 「入力なし」を押した (me-none.js が隠した欄を空にする) = 名前の欄も空に
  hid.addEventListener('change', function () { if (!hid.value && picked) { picked = null; inp.value = ''; close(); recount(); } });
  document.addEventListener('change', function (e) {
    var t = e.target;
    if (t && t.matches && t.matches('input[type="checkbox"][data-none="maker"]') && t.checked) { picked = null; inp.value = ''; close(); say(''); inp.setAttribute('aria-invalid', 'false'); }
  });

  ME.maker = {
    /** 名簿の番号 → 名前 (空 = なし) */
    label: function (id) { if (!id) return 'なし'; var m = byId(id); return m ? m.name : (picked && picked.id === String(id) ? picked.name : '(番号 ' + id + ')'); },
    /** 番号で選ぶ (名簿にある番号だけ) */
    set: function (id) { var m = byId(id); if (m) { setVal(m); say(''); } },
    /** 隠した欄の値に名前の欄を合わせる (画面の「元に戻す」の後) */
    sync: function () { var m = byId(hid.value); picked = m; inp.value = m ? m.name : ''; close(); say(''); },
    value: function () { return hid.value; },
  };
})();
