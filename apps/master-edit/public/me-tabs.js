/*
 * me-tabs.js — 1 つの商品の画面のタブ (10/19 中原さん「グラフとか入出庫履歴とか行くときにスクロールしないといけないからボタンかタブにしてほしい」)
 *   - 基本情報 (直す欄と保存の箱) / 販売数のグラフ / 入出庫の履歴 (単品は月別のまとめも)。見せるのは 1 つのタブだけ (ほかは hidden で隠すだけ = 打った値・未保存はそのまま)
 *   - タブの帯は小見出しと一緒に上の帯の下に残る (sticky)。直す欄の途中からでも 1 回で行ける
 *   - グラフ・履歴の JS は、そのタブの板 (role=tabpanel) が隠れていれば「me:tabshown」を待って読む = 開いたタブだけ読む (画面を開くのが速い)。
 *     この JS は グラフ・履歴の JS より前に読む (#tab=… のタブを先に開いておく = そのタブはすぐ読む)
 *   - 選んだタブはアドレスの #tab=sales / #tab=moves に残す (基本情報 = # なし)。履歴は足さない (replaceState。戻る = 前の画面へ・離れるときの確認はそのまま)。
 *     読み込み直す・ほかの画面から戻る・アドレスを渡す・前へ / 次へ (me-prevnext.js が MasterEdit.tabHash() を足す) でも同じタブ
 *   - タブごとに見ていた場所を覚えて、戻ったらそこへ (直す欄の途中 → グラフ → 基本情報 で同じ欄に戻る)
 *   - キー: ← → で隣のタブ・Home / End で最初 / 最後 (選ぶとすぐ開く)。Tab でタブの帯から開いた板へ
 *   - 保存・「変更内容を確認する」は基本情報に戻してから (me-sku.js が MasterEdit.showTab('basic') を呼ぶ)
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  var bar = document.getElementById('sku-tabs');
  if (!bar) return;
  var topEl = document.getElementById('sku-top') || bar;
  var tabs = Array.prototype.slice.call(bar.querySelectorAll('[role="tab"]'));
  var NAMES = tabs.map(function (t) { return t.getAttribute('data-tab'); });
  var DEF = NAMES[0];
  var cur = DEF;
  var at = {};    // タブごとの見えていた場所 (そのタブを離れたときの板の上端の、画面の中の位置)

  function tabOf(name) { return tabs[NAMES.indexOf(name)]; }
  function panelOf(name) { var t = tabOf(name); return t ? document.getElementById(t.getAttribute('aria-controls')) : null; }
  function fromHash() {
    var m = /^#(?:.*&)?tab=([a-z]+)/.exec(location.hash || '');
    return m && NAMES.indexOf(m[1]) >= 0 ? m[1] : DEF;
  }
  function hashOf(name) { return name === DEF ? '' : '#tab=' + name; }
  /** 前へ・次へ (ほかの商品の画面) にも同じタブを付ける。基本情報 = '' */
  ME.tabHash = function () { return hashOf(cur); };

  function syncLinks() {
    Array.prototype.forEach.call(document.querySelectorAll('#sku-pn a[href]'), function (a) {
      a.setAttribute('href', a.getAttribute('href').split('#')[0] + hashOf(cur));
    });
  }
  function setUrl() {
    var h = hashOf(cur);
    if ((location.hash || '') === h || (!h && location.hash === '#')) return;
    try { history.replaceState(history.state, '', location.pathname + location.search + h); } catch (e) { /* 書けない = タブはそのまま */ }
  }
  /** 開いた板の上端がタブの帯の下に隠れていたら、帯のすぐ下へ (板が短いと下の方に何も無い所が出るのを防ぐ) */
  function fitTop(panel) {
    var t = panel.getBoundingClientRect().top, b = topEl.getBoundingClientRect().bottom;
    if (t < b) window.scrollBy(0, t - b - 8);
  }

  /**
   * 前に見ていた場所へ戻す (板の上端を離れたときと同じ画面の中の位置に)。window.scrollY でなく板の位置で合わせる:
   * 小見出し (me-shell.js の IntersectionObserver) が出る・消えると帯の高さが変わって中身が動く (ブラウザのスクロールの錨も動かす) = 次の描画の後にもう一度合わせる
   */
  function backTo(panel, y) {
    var stop = false;
    var fix = function () { if (stop) return; var d = panel.getBoundingClientRect().top - y; if (Math.abs(d) >= 1 && !panel.hidden) window.scrollBy(0, d); };
    fix();
    if (!window.requestAnimationFrame) return;
    // その間に人がスクロールした (ホイール・指・つまみ) = 合わせ直さない (人の操作を巻き戻さない・Codex R4)
    var USER = ['wheel', 'touchstart', 'pointerdown'];
    var cancel = function () { stop = true; };
    USER.forEach(function (n) { window.addEventListener(n, cancel, { passive: true, capture: true }); });
    requestAnimationFrame(function () { requestAnimationFrame(function () {
      fix();
      USER.forEach(function (n) { window.removeEventListener(n, cancel, { passive: true, capture: true }); });
    }); });
  }
  /** 狭い画面で帯の外にはみ出したタブを、帯の中で横に送って見せる (ページは縦に動かさない) */
  function reveal(t) {
    if (!t || bar.scrollWidth <= bar.clientWidth) return;
    var l = t.offsetLeft - bar.offsetLeft, r = l + t.offsetWidth;
    if (l < bar.scrollLeft) bar.scrollLeft = l - 12;
    else if (r > bar.scrollLeft + bar.clientWidth) bar.scrollLeft = r - bar.clientWidth + 12;
  }

  /**
   * タブを開く。opts.url = アドレスの #tab を書き換える / opts.focus = タブにフォーカス / opts.scroll = 覚えた位置へ (無ければ板の上端へ)
   */
  function show(name, opts) {
    opts = opts || {};
    if (NAMES.indexOf(name) < 0) name = DEF;
    var same = name === cur;
    if (!same && opts.scroll) { var lp = panelOf(cur); if (lp) at[cur] = lp.getBoundingClientRect().top; }
    tabs.forEach(function (t) {
      var on = t.getAttribute('data-tab') === name;
      t.setAttribute('aria-selected', on ? 'true' : 'false');
      t.tabIndex = on ? 0 : -1;
      t.classList.toggle('on', on);
      var p = document.getElementById(t.getAttribute('aria-controls'));
      if (p) p.hidden = !on;
    });
    cur = name;
    reveal(tabOf(name));
    if (opts.url) setUrl();
    syncLinks();
    var panel = panelOf(name);
    if (opts.focus) tabOf(name).focus();
    if (!same && panel) {
      if (opts.scroll) { if (at[name] != null) backTo(panel, at[name]); else fitTop(panel); }
      var ev;
      try { ev = new CustomEvent('me:tabshown', { detail: { tab: name } }); } catch (e) { ev = document.createEvent('CustomEvent'); ev.initCustomEvent('me:tabshown', false, false, { tab: name }); }
      panel.dispatchEvent(ev);
    }
  }
  /** ほかの JS から (保存・変更内容を確認する = 基本情報へ) */
  ME.showTab = function (name) { show(name, { url: true, scroll: true }); };
  ME.currentTab = function () { return cur; };

  bar.addEventListener('click', function (e) {
    var t = e.target.closest && e.target.closest('[role="tab"]');
    if (t) show(t.getAttribute('data-tab'), { url: true, scroll: true });
  });
  bar.addEventListener('keydown', function (e) {
    var i = NAMES.indexOf(cur), n = NAMES.length, j = -1;
    if (e.key === 'ArrowRight') j = (i + 1) % n;
    else if (e.key === 'ArrowLeft') j = (i - 1 + n) % n;
    else if (e.key === 'Home') j = 0;
    else if (e.key === 'End') j = n - 1;
    if (j < 0 || e.altKey || e.ctrlKey || e.metaKey) return;
    e.preventDefault();
    show(NAMES[j], { url: true, scroll: true, focus: true });
  });
  // アドレスの #tab=… を打った・同じ画面の #tab リンク
  window.addEventListener('hashchange', function () { show(fromHash(), { url: false, scroll: true }); });

  // 小見出し + タブの帯の高さ (小見出しはスクロールで出たり消えたりする) = 右の保存の箱 (sticky) をその下に置く (CSS の --sku-top-h)
  function measure() { document.documentElement.style.setProperty('--sku-top-h', topEl.offsetHeight + 'px'); }
  if (typeof ResizeObserver !== 'undefined') new ResizeObserver(measure).observe(topEl);
  measure();

  // 開いたとき: アドレスの #tab=… のタブ (知らない名前・# なし = 基本情報)
  var first = fromHash();
  if (first !== DEF) show(first, { url: false });
  else if (location.hash && /tab=/.test(location.hash)) setUrl();   // 知らないタブ (セットに無い #tab=… など) = # を外す
})();
