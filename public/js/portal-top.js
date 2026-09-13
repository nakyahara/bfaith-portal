/**
 * ポータルトップ (views/dashboard.ejs) の検索。
 *   - 1 文字打つごとに絞り込む (サーバーには問い合わせない。カードは最初から全部 HTML にある)
 *   - スペース区切りは AND。グループカードはモール名のボタン単位で当てる
 *   - Enter で先頭のアプリを開く / Esc で消す / どこでも「/」で検索窓へ
 *   - 「しまったアプリも探す」にチェックしたときだけ、しまったアプリ (archived) も出す
 *   - /?q=発注 のように q を付けて開くと、その言葉で絞り込んだ状態で開く
 *
 * カードの data-search は lib/portal-dashboard.js の normalizeForSearch で正規化済み。
 * ⚠ 下の normalize はそれと同じ規則にすること (scripts/test-portal-top.mjs で突き合わせている)。
 */
(function () {
  'use strict';

  function normalize(s) {
    return String(s == null ? '' : s).normalize('NFKC').toLowerCase()
      .replace(/[ァ-ヶ]/g, function (c) { return String.fromCharCode(c.charCodeAt(0) - 0x60); })
      .replace(/\s+/g, '');
  }
  window.__portalTopNormalize = normalize;

  var root = document.getElementById('portal-top');
  var q = document.getElementById('top-q');
  if (!root || !q) return; // 見られるアプリが少ないユーザーには検索窓を出していない

  var arch = document.getElementById('top-arch');
  var countEl = document.getElementById('top-count');
  var emptyEl = document.getElementById('top-empty');
  var sections = Array.prototype.slice.call(root.querySelectorAll('[data-sec]'));

  function tokens() {
    var raw = q.value.normalize('NFKC').trim();
    return raw ? raw.split(/\s+/).map(normalize).filter(Boolean) : [];
  }

  function apply() {
    var toks = tokens();
    var searching = toks.length > 0;
    var withArchived = !!(arch && arch.checked);
    var match = function (hay) {
      for (var i = 0; i < toks.length; i++) if (hay.indexOf(toks[i]) === -1) return false;
      return true;
    };
    var total = 0;

    sections.forEach(function (sec) {
      var n = 0;
      sec.querySelectorAll('.top-card').forEach(function (card) {
        var own = card.getAttribute('data-search') || '';
        var chips = card.querySelectorAll('.top-chip');
        var show;
        if (chips.length) {
          var whole = !searching || match(own);
          var hits = 0;
          chips.forEach(function (chip) {
            var hit = !whole && match(own + (chip.getAttribute('data-search') || ''));
            if (hit) hits++;
            chip.classList.toggle('is-hit', hit);
          });
          show = whole || hits > 0;
          chips.forEach(function (chip) {
            chip.classList.toggle('is-dim', !whole && show && !chip.classList.contains('is-hit'));
          });
        } else if (card.hasAttribute('data-archived')) {
          show = searching && withArchived && match(own);
        } else {
          show = !searching || match(own);
        }
        card.hidden = !show;
        if (show) n++;
      });
      sec.hidden = n === 0;
      var secCount = sec.querySelector('[data-sec-n]');
      if (secCount) secCount.textContent = n + ' 件';
      var nav = root.querySelector('[data-nav="' + sec.getAttribute('data-sec') + '"]');
      if (nav) {
        nav.querySelector('.top-nav-n').textContent = n;
        nav.classList.toggle('is-empty', n === 0);
      }
      total += n;
    });

    if (countEl) countEl.textContent = searching ? total + ' 件' : '';
    if (emptyEl) {
      emptyEl.hidden = !(searching && total === 0);
      if (!emptyEl.hidden) {
        emptyEl.textContent = '「' + q.value.trim() + '」に当たるアプリはありません。旧名や、やりたいこと (例: 値上げ、棚卸、プライスター) でも探せます。'
          + (arch && !arch.checked ? ' しまったアプリは「しまったアプリも探す」にチェックすると出ます。' : '');
      }
    }
  }

  function firstVisibleLink() {
    var cards = root.querySelectorAll('.top-card');
    for (var i = 0; i < cards.length; i++) {
      var card = cards[i];
      if (card.hidden || card.closest('[data-sec]').hidden) continue;
      var link = card.querySelector('.top-chip.is-hit') || card.querySelector('a.top-card-main') || card.querySelector('.top-chip');
      if (link) return link;
    }
    return null;
  }

  q.addEventListener('input', apply);
  if (arch) arch.addEventListener('change', apply);
  q.addEventListener('keydown', function (e) {
    if (e.isComposing) return; // 日本語入力の変換確定の Enter では開かない
    if (e.key === 'Enter') {
      var link = firstVisibleLink();
      if (!link) return;
      e.preventDefault();
      if (link.target === '_blank') window.open(link.href, '_blank', 'noopener');
      else window.location.href = link.href;
    } else if (e.key === 'Escape') {
      q.value = '';
      apply();
    }
  });
  document.addEventListener('keydown', function (e) {
    if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey) return;
    var el = document.activeElement;
    var tag = el && el.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || (el && el.isContentEditable)) return;
    e.preventDefault();
    q.focus();
  });

  var initial = new URLSearchParams(window.location.search).get('q');
  if (initial) q.value = initial;
  apply();
  // 戻るボタンでブラウザが入力欄の中身だけ復元したときも絞り込みを合わせる
  window.addEventListener('pageshow', apply);

  // PC だけ最初から入力できる状態にする (iPad・スマホはキーボードが勝手に出るのでしない)
  if (window.matchMedia && window.matchMedia('(pointer: fine) and (min-width: 1024px)').matches) {
    try { q.focus({ preventScroll: true }); } catch (err) { q.focus(); }
  }
})();
