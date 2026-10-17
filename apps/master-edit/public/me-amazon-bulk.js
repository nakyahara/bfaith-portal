/*
 * me-amazon-bulk.js — CSV でまとめて登録 (試し取込つき・10/17・views/amazon-bulk.ejs)
 *   1. ファイルを選ぶ → バイトのまま base64 で POST /api/amazon/bulk/preview (文字コード・形はサーバーが見分ける・何も書かない)
 *   2. 結果の表 (seller SKU ごとに する事・名前・構成・理由)・絞る札 (全部 / 保存する / 変わらない / だめ / 気をつける)
 *   3. 「保存する」= 保存してよい SKU だけを 20 件ずつ POST /api/amazon/bulk/apply { ticket, items } (印つきのまま返す)。
 *      通信が切れた = そこで止める。もう一度押すと続きから (同じ切符 = 同じ request_id = 保存済みの分は前の結果が返る・2 回入らない)
 *   「結果を CSV で保存」= 表の結果を POST /api/amazon/bulk/report.csv (式の注入の対策はサーバー = 一覧の CSV と同じ)
 *   試し取込の後に保存していない = 離れるときに確かめる (MasterEdit.dirty)
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  var cfgEl = document.getElementById('me-bulk-cfg');
  var fileIn = document.getElementById('bulk-file');
  if (!cfgEl || !fileIn) return;
  var CFG = JSON.parse(cfgEl.textContent || '{}');
  var BASE = CFG.base || '';
  function $(id) { return document.getElementById(id); }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function msg(id, t, cls) { var m = $(id); m.className = 'msgline' + (cls ? ' ' + cls : ''); m.textContent = t || ''; }
  var previewBtn = $('bulk-preview'), applyBtn = $('bulk-apply'), reportBtn = $('bulk-report');
  var S = { data: null, results: {}, next: 0, busy: false, filter: 'all', done: false };
  var TONE = { create: 'ok', create_new: 'ok', update: 'info', revive: 'warn', same: 'mute', error: 'err' };
  function pending() { if (!S.data || S.done) return 0; return S.data.rows.filter(function (r) { return r.item && !(S.results[r.seller_sku] && S.results[r.seller_sku].ok); }).length; }
  ME.dirty = function () { var n = pending(); return { n: n ? 1 : 0, items: n ? ['試し取込の結果 (まだ保存していない seller SKU ' + n + ' 件)'] : [], impacts: [] }; };
  function refreshUnsaved() { if (ME.setUnsaved) ME.setUnsaved(ME.dirty().n); }

  fileIn.addEventListener('change', function () { previewBtn.disabled = !fileIn.files || !fileIn.files.length; msg('bulk-msg', ''); });
  function b64(buf) {
    var bytes = new Uint8Array(buf), out = '', step = 0x8000;
    for (var i = 0; i < bytes.length; i += step) out += String.fromCharCode.apply(null, bytes.subarray(i, i + step));
    return btoa(out);
  }
  function post(path, body) {
    return fetch(BASE + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); });
  }
  previewBtn.addEventListener('click', function () {
    var f = fileIn.files && fileIn.files[0];
    if (!f || S.busy) return;
    if (pending() && !window.confirm('前の試し取込の結果をまだ保存していません。捨てて新しいファイルを試し取込しますか')) return;
    if (f.size > CFG.maxBytes) { msg('bulk-msg', 'ファイルが大きすぎます (' + (CFG.maxBytes / 1024 / 1024) + ' MB まで)。ファイルを分けてください', 'err'); return; }
    S.busy = true; previewBtn.disabled = true; msg('bulk-msg', '試し取込をしています… (何も保存しません)');
    f.arrayBuffer().then(function (buf) { return post('/api/amazon/bulk/preview', { file_name: f.name, data_b64: b64(buf) }); })
      .then(function (x) {
        S.busy = false; previewBtn.disabled = false;
        if (!x.r.ok || !x.j.ok) { msg('bulk-msg', (x.j && x.j.error) || '試し取込できませんでした (HTTP ' + x.r.status + ')', 'err'); return; }
        S.data = x.j; S.results = {}; S.next = 0; S.done = false; S.filter = 'all';
        msg('bulk-msg', '試し取込をしました (何も保存していません)。下の結果を確かめてください', 'ok');
        render(); refreshUnsaved();
        $('bulk-result').scrollIntoView({ behavior: 'smooth', block: 'start' });
      })
      .catch(function () { S.busy = false; previewBtn.disabled = false; msg('bulk-msg', '通信できませんでした。もう一度押してください (試し取込は何も書きません)', 'err'); });
  });

  var FILTERS = [['all', '全部'], ['save', '保存する'], ['same', '変わらない'], ['error', 'だめ'], ['warn', '気をつける']];
  function inFilter(r) {
    if (S.filter === 'all') return true;
    if (S.filter === 'save') return !!r.item;
    if (S.filter === 'warn') return r.warnings && r.warnings.length > 0;
    return r.action === S.filter;
  }
  function countOf(k) {
    var rows = S.data.rows;
    if (k === 'all') return rows.length;
    if (k === 'save') return rows.filter(function (r) { return !!r.item; }).length;
    if (k === 'warn') return rows.filter(function (r) { return r.warnings && r.warnings.length; }).length;
    return rows.filter(function (r) { return r.action === k; }).length;
  }
  function resultCell(r) {
    var x = S.results[r.seller_sku];
    if (!r.item) return '<span class="muted">送らない</span>';
    if (!x) return '<span class="muted">まだ</span>';
    if (x.ok) {
      var t = x.result.no_change ? '変わらない (何も変えていない)' : x.result.created ? (x.result.listing_created ? '作りました (出品も)' : '作りました') : x.result.revived ? '戻しました' : '直しました';
      return '<span class="b ok">' + esc(t + (x.result.replayed ? ' · 前に保存した結果' : '')) + '</span>';
    }
    return '<span class="b err">だめ</span> <span class="bulk-why">' + esc(x.error && x.error.message) + '</span>';
  }
  function resultText(r) {
    var x = S.results[r.seller_sku];
    if (!r.item) return '送らない';
    if (!x) return 'まだ';
    if (x.ok) return x.result.no_change ? '変わらない' : x.result.created ? '作った' : x.result.revived ? '戻した' : '直した';
    return 'だめ: ' + (x.error && x.error.message);
  }
  function render() {
    var d = S.data;
    $('bulk-result').hidden = false;
    $('bulk-file-info').textContent = d.file + ' · ' + d.encoding_words + ' · ' + d.format_words + ' · seller SKU ' + d.counts.skus + ' 件 (行 ' + d.counts.lines + ')';
    $('bulk-filter').innerHTML = FILTERS.map(function (f) { var n = countOf(f[0]); return '<button type="button" class="chip' + (S.filter === f[0] ? ' on' : '') + (n ? '' : ' zero') + '" data-f="' + f[0] + '" aria-pressed="' + (S.filter === f[0] ? 'true' : 'false') + '">' + esc(f[1]) + ' <span class="n">' + n + '</span></button>'; }).join('');
    var le = $('bulk-line-errors');
    le.hidden = !d.line_errors.length;
    le.innerHTML = d.line_errors.length ? '<div class="grow"><div class="t">seller SKU として読めない行が ' + d.line_errors.length + ' 行あります (送りません)</div><ul class="warnlist">' + d.line_errors.slice(0, 50).map(function (e) { return '<li>' + esc(e.message) + '</li>'; }).join('') + '</ul></div>' : '';
    var rows = d.rows.filter(inFilter);
    $('bulk-rows').innerHTML = rows.map(function (r) {
      var why = [].concat((r.errors || []).map(function (t) { return '<li class="err">' + esc(t) + '</li>'; }), (r.warnings || []).map(function (t) { return '<li class="warn">' + esc(t) + '</li>'; }), (r.notes || []).map(function (t) { return '<li>' + esc(t) + '</li>'; }));
      return '<tr data-sku="' + esc(r.seller_sku) + '"><td class="mono">' + esc(r.seller_sku) + '<div class="hint">' + esc(r.line) + ' 行目</div></td>'
        + '<td><span class="b ' + (TONE[r.action] || 'mute') + '">' + esc(r.words) + '</span></td>'
        + '<td>' + esc(r.name || '') + '</td>'
        + '<td class="mono">' + esc((r.components || []).map(function (c) { return c.code + '×' + c.qty; }).join(', ')) + '</td>'
        + '<td>' + (why.length ? '<ul class="bulk-notes">' + why.join('') + '</ul>' : '') + '</td>'
        + '<td class="bulk-res">' + resultCell(r) + '</td></tr>';
    }).join('') || '<tr><td colspan="6" class="empty">当てはまる seller SKU はありません</td></tr>';
    var sn = $('bulk-similar-note');
    sn.hidden = !d.similar_skipped;
    sn.textContent = d.similar_skipped ? '似た seller SKU の確かめは、まだ出品の無い SKU の上から ' + d.similar_max + ' 件までです (ほかの ' + d.similar_skipped + ' 件は確かめていません)' : '';
    var n = countOf('save'), left = pending();
    $('bulk-save-box').hidden = false;
    applyBtn.disabled = !CFG.canApply || S.busy || !left;
    applyBtn.textContent = !n ? '保存する SKU がありません' : S.done ? '保存しました' : S.next > 0 ? '続きを保存する (残り ' + left + ' 件)' : '保存する (' + n + ' 件)';
    reportBtn.disabled = S.busy;
  }
  $('bulk-filter').addEventListener('click', function (e) { var b = e.target.closest('button[data-f]'); if (!b) return; S.filter = b.getAttribute('data-f'); render(); });

  function saveItems() { return S.data.rows.filter(function (r) { return !!r.item; }); }
  function setProg(done, all) {
    $('bulk-prog').hidden = false;
    $('bulk-bar').style.width = (all ? Math.round(done / all * 100) : 0) + '%';
    $('bulk-prog-t').textContent = done + ' / ' + all + ' 件';
  }
  applyBtn.addEventListener('click', function () {
    if (S.busy || applyBtn.disabled || !S.data) return;
    var list = saveItems();
    S.busy = true; render(); msg('bulk-apply-msg', '保存しています… (画面を閉じないでください)');
    (function step() {
      // まだ送っていない分を 20 件 (S.next = 返事を受け取った所まで。通信が切れた 20 件は同じ番号でもう一度送る)
      var chunk = list.slice(S.next, S.next + CFG.chunk);
      setProg(Math.min(S.next, list.length), list.length);
      if (!chunk.length) {
        S.busy = false; S.done = true; render(); refreshUnsaved();
        var ok = list.filter(function (r) { return S.results[r.seller_sku] && S.results[r.seller_sku].ok; }).length;
        msg('bulk-apply-msg', '終わりました: できた ' + ok + ' 件 · だめ ' + (list.length - ok) + ' 件' + (list.length - ok ? ' (表の「保存」の列に理由。直して CSV をもう一度試し取込すると、できた分は「変わらない」になります)' : ''), list.length - ok ? 'err' : 'ok');
        if (ME.toast) ME.toast('CSV の一括登録: できた ' + ok + ' 件');
        return;
      }
      post('/api/amazon/bulk/apply', { ticket: S.data.ticket, items: chunk.map(function (r) { return r.item; }) })
        .then(function (x) {
          if (!x.r.ok || !x.j.ok) {
            S.busy = false; render();
            msg('bulk-apply-msg', ((x.j && x.j.error) || '保存できませんでした (HTTP ' + x.r.status + ')') + ' — ' + S.next + ' 件目まで送りました', 'err');
            return;
          }
          (x.j.results || []).forEach(function (res) { S.results[res.seller_sku] = res; });
          // 結果の分からない件 (サーバーの誤り = 保存したか分からない) と送っていない件 = その件から続ける (同じ番号 = 保存していれば前の結果・#Codex A R1 M)
          var firstNot = chunk.findIndex(function (r) { var y = S.results[r.seller_sku]; return !y || (!y.ok && (y.error.reason === 'not_sent' || y.error.reason === 'error')); });
          if (firstNot >= 0) {
            S.next += firstNot;
            S.busy = false; render();
            msg('bulk-apply-msg', 'Company DB との接続が切れたか、サーバーの誤りで結果が分からない SKU があります。もう一度押すと、その SKU から同じ番号で続けます (保存した分は 2 回入りません)', 'err');
            return;
          }
          S.next += chunk.length;
          render();
          step();
        })
        .catch(function () {
          S.busy = false; render();
          msg('bulk-apply-msg', '通信が切れました。もう一度押すと続きから保存します (同じ番号で送るので、保存した分は 2 回入りません)', 'err');
        });
    })();
  });

  reportBtn.addEventListener('click', function () {
    if (!S.data) return;
    var cut = function (a) { return (a || []).map(function (t) { return String(t).slice(0, 500); }); };
    var rows = S.data.rows.map(function (r) { return { seller_sku: r.seller_sku, line: r.line, action: r.action, name: r.name, components: r.components, result: String(resultText(r)).slice(0, 500), errors: cut(r.errors), warnings: cut(r.warnings), notes: cut(r.notes) }; })
      .concat(S.data.line_errors.map(function (e) { return { seller_sku: e.raw, line: e.no, action: 'error', name: '', components: [], result: '送らない', errors: [e.message], warnings: [], notes: [] }; }));
    fetch(BASE + '/api/amazon/bulk/report.csv', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ rows: rows }) })
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); var name = /filename="([^"]+)"/.exec(r.headers.get('content-disposition') || ''); return r.blob().then(function (b) { return { b: b, name: name ? name[1] : 'amazon-bulk-result.csv' }; }); })
      .then(function (x) { var a = document.createElement('a'); a.href = URL.createObjectURL(x.b); a.download = x.name; document.body.appendChild(a); a.click(); setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 1000); })
      .catch(function (e) { msg('bulk-apply-msg', '結果の CSV を作れませんでした (' + e.message + ')', 'err'); });
  });
})();
