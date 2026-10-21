/*
 * me-moves.js — 1 つの商品の画面 (単品) の「入出庫の履歴」と「月別のまとめ」(CompanyDB構想 26 の PR-1・PR-2。読むだけ)
 *   - 履歴 = GET /api/sku/:code/movements (新しい順・出荷 200 行ずつ)。「もっと前を見る」= 前のページの切れ目 (next) から続きを足す
 *   - 月別 = GET /api/sku/:code/monthly (直近 13 か月)。データの無い月は空欄 (0 と出さない)
 *   - 時刻は日本時間。仕入は日付だけ (入庫日)。「在庫の調整 (ずれ)」の行は薄く・理由つき
 *   - 履歴は高さを決めた枠 (#mv-box) の中でスクロール。件数 (全 N 件 / 新しい方から N 件) は枠の下
 *   - 🆕 10/19: 画面のタブ (me-tabs.js)「入出庫の履歴」を初めて開いたときに読む (隠れたタブのまま読まない = 画面を開くのが速い)
 */
(function () {
  'use strict';
  var box = document.getElementById('p-moves');
  var rowsEl = document.getElementById('mv-rows');
  if (!box || !rowsEl) return;
  var BASE = box.getAttribute('data-base') || '';
  var CODE = box.getAttribute('data-code') || '';
  var PO = box.getAttribute('data-po') === '1';
  var head = document.getElementById('mv-head');
  var more = document.getElementById('mv-more');
  var msg = document.getElementById('mv-msg');
  var scroller = document.getElementById('mv-box');   // 履歴の枠 (高さを決めて中でスクロール)
  var next = null;
  var count = 0;
  var cut = false;     // どこかのページで「1 日の出荷が多すぎて全部は読めなかった」= 「全 N 件」と言わない
  var counted = false; // 件数を出している (読んでいる途中・読めなかったときは画面の大きさが変わっても書き換えない)

  // 件数: 最後まで読んだ = 「全 N 件」/ まだ前がある = 「新しい方から N 件」/ 読めなかった日がある = 「読めた N 件」。
  // 枠に入りきらないときだけ「枠の中でスクロール」を添える (画面の向き・大きさが変わったら数え直す)
  function showCount() {
    var over = scroller && scroller.scrollHeight > scroller.clientHeight + 1;
    var t = (cut ? '1 日の出荷が多すぎて全部は読めませんでした (その日の在庫数は出していません) · 読めた ' : next ? '新しい方から ' : '全 ')
      + count.toLocaleString('ja-JP') + ' 件' + (over ? ' (枠の中でスクロール)' : '') + (next ? ' · もっと前があります' : '');
    if (msg.textContent !== t) msg.textContent = t;   // 同じ文なら書き換えない (読み上げを繰り返さない)
    counted = true;
  }

  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function num(v) { return v == null ? '' : Number(v).toLocaleString('ja-JP'); }
  function pad(n) { return (n < 10 ? '0' : '') + n; }
  /** ISO → 日本時間の 'YYYY/MM/DD HH:MM' (dateOnly = 日付だけ) */
  function jst(iso, dateOnly) {
    var d = new Date(new Date(iso).getTime() + 9 * 3600e3);
    var s = d.getUTCFullYear() + '/' + pad(d.getUTCMonth() + 1) + '/' + pad(d.getUTCDate());
    return dateOnly ? s : s + ' ' + pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes());
  }
  function day(ymd) { return String(ymd || '').replace(/-/g, '/'); }
  function api(path) {
    return fetch(BASE + path, { headers: { Accept: 'application/json' }, credentials: 'same-origin' })
      .then(function (r) { return r.json().catch(function () { return { ok: false, error: '読めませんでした (' + r.status + ')' }; }); })
      .catch(function () { return { ok: false, error: 'つながりませんでした。少し待ってから画面を開き直してください' }; });
  }

  function rowHtml(r) {
    var cls = r.kind === 'adjust' ? ' class="mv-adj"' : '';
    var when = r.dateOnly ? day(r.day) || jst(r.at, true) : jst(r.at, false);
    var slip = r.slip ? '<span class="mono">' + esc(r.slip) + '</span>' + (r.slipKind === 'logizard' ? '<span class="sub">ロジザードの伝票NO</span>' : '') : '';
    return '<tr' + cls + ' data-kind="' + esc(r.kind) + '">'
      + '<td class="mv-when">' + esc(when) + (r.dateOnly ? '<span class="sub">日付だけ</span>' : '') + '</td>'
      + '<td>' + esc(r.label) + '</td>'
      + '<td class="n">' + num(r.in) + '</td>'
      + '<td class="n">' + num(r.out) + '</td>'
      + '<td class="n">' + (r.balance == null ? '<span class="muted" title="ロジザードの毎日の在庫が無い日">—</span>' : num(r.balance)) + '</td>'
      + '<td>' + slip + '</td>'
      + '<td>' + esc(r.party) + '</td>'
      + '<td class="mv-why">' + esc(r.reason) + '</td>'
      + '<td>' + esc(r.actor || '') + '</td></tr>';
  }

  function headText(j) {
    var t = [];
    if (j.lastClose) t.push('最後の締め ' + day(j.lastClose.day) + ' (' + jst(j.lastClose.at, false).slice(11) + ' の取得)');
    else t.push('ロジザードの毎日の在庫がまだ無いので、在庫数は出せません');
    if (j.firstDay) t.push(day(j.firstDay) + ' より前は在庫数を出せません (ロジザードの毎日の在庫が始まる前)');
    if (j.liveAt) t.push('いまのロジザード ' + jst(j.liveAt, false) + (j.liveStale ? ' (古い)' : '') + ' で合わせています');
    if (j.po === 'denied') t.push('発注アプリの権限が無いので仕入の行は出しません (仕入の分は「在庫の調整」に入ります)');
    if (j.po === 'error') t.push('発注アプリを読めないので仕入の行は出していません');
    return t.join(' · ');
  }

  function load() {
    more.disabled = true;
    counted = false;
    msg.textContent = next ? '前の分を読んでいます…' : '';
    api('/api/sku/' + encodeURIComponent(CODE) + '/movements' + (next ? '?before=' + encodeURIComponent(next) : '')).then(function (j) {
      more.disabled = false;
      if (!j || !j.ok) {
        if (!count) head.textContent = (j && j.error) || '読めませんでした';
        else msg.textContent = (j && j.error) || '読めませんでした';
        more.hidden = true;
        return;
      }
      if (!count) head.textContent = headText(j);
      if (!count && !j.rows.length) rowsEl.innerHTML = '<tr><td colspan="9" class="muted">出荷・仕入・在庫の動きがありません</td></tr>';
      else rowsEl.insertAdjacentHTML('beforeend', j.rows.map(rowHtml).join(''));
      count += j.rows.length;
      next = j.next;
      more.hidden = !next;
      if (j.truncated) cut = true;
      showCount();   // 続きは tbody の末尾に足すだけ = 枠のスクロールの位置はそのまま
    });
  }
  more.addEventListener('click', load);
  window.addEventListener('resize', function () { if (counted) showCount(); });

  // ── 月別のまとめ
  var moRows = document.getElementById('mo-rows');
  var moHead = document.getElementById('mo-head');
  function loadMonthly() {
    if (!moRows) return;
    api('/api/sku/' + encodeURIComponent(CODE) + '/monthly').then(function (j) {
      if (!j || !j.ok) { moHead.textContent = (j && j.error) || '読めませんでした'; return; }
      moHead.textContent = j.po === 'error' ? '発注アプリを読めないので、仕入・発注は空欄です' : '';
      var poCols = PO;
      var amt = function (v, partial) { return v == null ? '' : num(v) + (partial ? '<span class="sub">単価の無い行あり</span>' : ''); };
      moRows.innerHTML = j.months.map(function (m) {
        return '<tr><td class="mono">' + esc(m.month.replace('-', '/')) + '</td>'
          + '<td class="n">' + num(m.orderQty) + '</td><td class="n">' + num(m.shipQty) + '</td>'
          + (poCols ? '<td class="n">' + num(m.receiptQty) + '</td><td class="n">' + num(m.poQty) + '</td>' : '')
          + '<td class="n">' + num(m.orderAmount) + '</td><td class="n">' + num(m.shipAmount) + '</td>'
          + (poCols ? '<td class="n">' + amt(m.receiptAmount, m.receiptAmountPartial) + '</td><td class="n">' + amt(m.poAmount, m.poAmountPartial) + '</td>' : '')
          + '</tr>';
      }).join('');
    });
  }

  // 🆕 タブ (me-tabs.js): 隠れたタブの中 = そのタブを初めて開いたときに 1 回だけ読む (開いている = すぐ読む)。
  //   開き直すたびに件数の「枠の中でスクロール」を数え直す (隠れている間は枠の高さが 0 = 数えられない)
  var tp = box.closest ? box.closest('[role="tabpanel"]') : null;
  var started = false;
  function start() { if (started) return; started = true; load(); loadMonthly(); }
  if (tp) tp.addEventListener('me:tabshown', function () { if (!started) start(); else if (counted) showCount(); });
  if (!tp || !tp.hidden) start();
})();
