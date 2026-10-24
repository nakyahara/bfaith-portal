/*
 * me-lzcsv.js — 「ロジザード登録の CSV」の画面の動き (views/lz-csv.ejs・lib/lz-new-export.mjs・0081)
 *   - 「N 件の CSV を作ってダウンロード」= POST /api/lz-csv/build (画面で見た商品と行番号)。作れたらファイルをダウンロードして画面を読み直す
 *   - 🆕 2026-10-24 ダウンロード = 部品 (me-download.js・NE 登録の CSV と同じ) で fetch して保存。断られたら理由をその行に出す。
 *     断ると決まっている行 (ボタンで取り込み中・取り込んだ・分からない回) はボタンを押せない形と理由 (読み直しのたびに合わせる)
 *   - 写しが古い = 作れない (画面がボタンを止める・DB も断る = #1721 R2)
 *   - 通信が分からないまま終わった = 同じ request_id で送り直す (2 回作らない)
 *   - 「取り消す」= 理由を聞いて POST /api/lz-csv/:id/void
 *   - 🆕 2026-10-17「ロジザードに取り込む」(取り込める人 = P.canImport): 作ったら (ダウンロードせずに) 画面を読み直す →
 *     CSV ごとの「ロジザードに取り込む」→ 下に出る確かめ (② も書き出す・ダウンロードした CSV は「手で取り込んでいない」・もう一度は理由) →
 *     POST /api/lz-csv/:id/import (押し直し = 同じ request_id)。様子は GET /api/lz-csv/import-status を読み直して描く
 *     (動いている間 4 秒・ほか 30 秒)。値は全部 textContent で出す (ログ・メール・理由は信用しない値)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var dataEl = document.getElementById('me-lzc');
  if (!dataEl) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var busy = false, pending = null;
  function msg(t, cls) { var m = $('#lzc-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function post(url, body) {
    return fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { return { r: r, j: j }; }); });
  }
  var btn = $('#lzc-build');
  if (btn) btn.addEventListener('click', function () {
    if (busy || !P.ready.length) return;
    if (P.stale) { msg('ロジザードの商品の一覧の写しが古いので作れません (毎晩の取り込みの後に作るか、予備の GAS で)', 'err'); return; }
    var items = P.ready.map(function (x) { return { sku_id: x.sku_id, rule_row: x.rule_row }; });
    var key = JSON.stringify(items);
    if (!pending || pending.key !== key) pending = { key: key, rid: uuid() };
    busy = true; btn.disabled = true; msg('作っています…');
    post(P.base + '/api/lz-csv/build', { request_id: pending.rid, items: items }).then(function (x) {
      busy = false;
      var sure = !!x.j && (x.r.ok ? x.j.ok === true : x.r.status >= 400 && x.r.status < 500);
      if (!sure) { btn.disabled = false; msg('作れたか分かりません (HTTP ' + x.r.status + ')。もう一度押してください (同じ CSV として送り直します)', 'err'); return; }
      pending = null;
      if (!x.r.ok || !x.j.ok) { btn.disabled = false; msg(x.j.error || ('HTTP ' + x.r.status), 'err'); return; }
      if (P.canImport) {
        // ボタンの道: ダウンロードしない (手で取り込んでいない = 2 回取り込まない)。読み直すと「ロジザードに取り込む」が出る
        msg('CSV #' + x.j.export_id + ' (' + x.j.rows + ' 件) を作りました。下の「作った CSV」の「ロジザードに取り込む」で取り込みます', 'ok');
        setTimeout(function () { location.reload(); }, 1500);
        return;
      }
      msg('CSV #' + x.j.export_id + ' (' + x.j.rows + ' 件) を作りました。ダウンロードします…', 'ok');
      // 🆕 2026-10-24: 部品 (me-download.js) = 断られたら理由を出す (前は隠した <a download> = 何も出なかった)
      lzDownload(x.j.export_id).then(function (d) {
        if (!d.ok) { msg('CSV #' + x.j.export_id + ' を作りましたが、ダウンロードできませんでした: ' + d.t + ' (画面を読み直すと「作った CSV」に出ます)', 'err'); return; }
        msg('CSV #' + x.j.export_id + ' (' + x.j.rows + ' 件) を作って、ダウンロードしました', 'ok');
        setTimeout(function () { location.reload(); }, 1500);
      });
    }).catch(function () { busy = false; btn.disabled = false; msg('通信できませんでした。作れたか分からないので、もう一度押してください (同じ CSV として送り直します)', 'err'); });
  });
  /* ── 🆕 2026-10-24 ダウンロード (中原さん「ダウンロードを押しても CSV が落ちない」= サーバーが 409 / 503 で断っても <a download> は何も出さなかった) ── */
  // 断りの理由 (router の GET /lz-csv/:id/file・lz-bc-import の recordDownload) → 行に出すふつうの言葉 (サーバーの文があればそれ)
  var LZ_DL_WORDS = {
    import_running: 'いまボタンでロジザードに取り込んでいます。終わってから (取り込み中はダウンロードできません)',
    imported_by_button: 'ボタンで取り込んだ CSV です。手でもう一度取り込まないでください (ダウンロードできません)',
    import_unresolved: '取り込みが「取り込めたか分からない」のままです。下の③で確かめて記してから、ダウンロードできます',
    voided: '取り消した CSV です。ダウンロードできません (作り直してください)',
    bc_db_unavailable: 'ダウンロードの記録を残せないので、いまは出せません (少し待ってからもう一度)',
    not_approver: 'ダウンロードできるのは名簿の人だけです',
  };
  var LZ_DL_STOP = { import_running: 1, imported_by_button: 1, import_unresolved: 1, voided: 1 };   // もう一度押しても同じ = ボタンを止める
  function lzDownload(id) {
    var ME = window.MasterEdit || {};
    if (!ME.downloadFile) return Promise.resolve({ ok: false, t: '画面の部品が読めませんでした。画面を開き直してください' });
    return ME.downloadFile(P.base + '/lz-csv/' + encodeURIComponent(id) + '/file', { name: 'logizard_bc_upload.csv' }).then(function (d) {
      if (d.ok) return d;
      var t = d.status === 404 ? 'ファイルがありません。画面を開き直してください' : (d.error || LZ_DL_WORDS[d.reason] || ('ダウンロードできませんでした (HTTP ' + d.status + ')。もう一度押してください'));
      return { ok: false, t: t, stop: !!LZ_DL_STOP[d.reason] };
    });
  }
  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('button[data-lz-dl]');
    if (!b || b.disabled) return;
    var id = b.getAttribute('data-lz-dl');
    var line = document.querySelector('[data-lz-dl-msg="' + id + '"]');
    var say = function (t, c) { if (line) { line.className = 'msgline ' + (c || ''); line.textContent = t; } };
    b.disabled = true; b.setAttribute('data-lz-dl-busy', '');   // 読み直し (syncDl) が途中で押せるように戻さない (#Codex R1 Low)
    say('ダウンロードしています…');
    lzDownload(id).then(function (d) {
      b.removeAttribute('data-lz-dl-busy');
      if (!d.ok) { say(d.t, 'err'); b.disabled = !!d.stop; return; }
      b.disabled = false;
      say('ダウンロードしました (' + d.name + ')。', 'ok');
      // 保存したかはページから分からない = 人が押す保存のリンク (#Codex R1 High)
      var again = window.MasterEdit && window.MasterEdit.saveAgainLink && window.MasterEdit.saveAgainLink(d);
      if (again && line) { var old = line.parentNode.querySelector('[data-save-again]'); if (old) old.remove(); line.parentNode.insertBefore(again, line.nextSibling); }
    });
  });
  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('button[data-void]');
    if (!b || busy) return;
    var id = b.getAttribute('data-void');
    var reason = window.prompt('CSV #' + id + ' を取り消します。理由を書いてください (例: ロジザードで取り込めなかった)。取り消すと、この中の商品はもう一度出せます', '');
    if (reason == null) return;
    if (!reason.trim()) { msg('理由が要ります', 'err'); return; }
    busy = true;
    post(P.base + '/api/lz-csv/' + encodeURIComponent(id) + '/void', { reason: reason.trim() }).then(function (x) {
      busy = false;
      if (!x.j || !x.r.ok || !x.j.ok) { msg((x.j && x.j.error) || ('HTTP ' + x.r.status), 'err'); return; }
      location.reload();
    }).catch(function () { busy = false; msg('通信できませんでした。画面を開き直して確かめてください', 'err'); });
  });
})();

/* ── 🆕「ロジザードに取り込む」(apps/lz-bc-import・miniPC が取りに来る) ── */
(function () {
  'use strict';
  var dataEl = document.getElementById('me-lzc');
  if (!dataEl) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P || !P.bc) return;
  var S = P.bc, BLOCK = P.importBlock || null, CAN = !!P.canImport;
  // ボタンができる前に作った CSV (ダウンロードの記録が無い = 手で取り込んだか分からない)。最初の値だけが持つ = 読み直しでも保つ
  var LEGACY = {};
  Object.keys(S.byExport || {}).forEach(function (id) { if (S.byExport[id] && S.byExport[id].legacy) LEGACY[id] = true; });
  var forms = {};   // CSV ごとの開いている確かめ (押し直しの request_id を保つ)
  var timer = null, sending = false;
  var ACTIVE = { queued: 1, claimed: 1, executing: 1, exporting: 1 };
  function $(s, r) { return (r || document).querySelector(s); }
  function el(tag, props, kids) {
    var e = document.createElement(tag);
    if (props) for (var k in props) {
      if (k === 'text') e.textContent = props[k];
      else if (k === 'cls') e.className = props[k];
      else if (k === 'on') for (var ev in props.on) e.addEventListener(ev, props.on[ev]);
      else if (props[k] === true) e.setAttribute(k, '');
      else if (props[k] !== false && props[k] != null) e.setAttribute(k, String(props[k]));
    }
    (kids || []).forEach(function (c) { if (c != null) e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
    return e;
  }
  function when(ms) {
    if (!ms) return '—';
    try { return new Date(ms).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }); } catch (e) { return String(ms); }
  }
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  function post(url, body) {
    return fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body || {}) })
      .then(function (r) { return r.json().catch(function () { return null; }).then(function (j) { return { r: r, j: j }; }); });
  }
  function msg(t, cls) { var m = $('#lzc-bc-msg'); if (m) { m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; } }
  var badgeOf = { queued: 'info', claimed: 'info', executing: 'warn', exporting: 'warn', imported: 'ok', import_error: 'warn', not_executed: 'mute', unknown: 'warn', cancelled: 'mute', expired: 'mute' };

  /** 押せない理由 (null = 押せる)。ポータルも同じ決まりで断る (ここは見せ方だけ) */
  function whyNot(e, s) {
    if (!CAN) return P.importMessage || '取り込める人ではありません';
    if (e.state !== 'issued') return '取り消した CSV は取り込めません';
    if (S.active && String(S.active.export_id) !== String(e.id)) return 'ほかの取り込み (#' + S.active.job_id + ') が動いています';
    if (S.night && S.night.enqueue) return P.nightWords;
    if (!S.runner || !S.runner.alive) return 'miniPC が来ていません (最後 ' + when(S.runner && S.runner.seen_at) + ')。ダウンロードして予備の道で';
    if (BLOCK) return BLOCK.message;
    if (S.unresolved_any && String(S.unresolved_any.export_id) !== String(e.id)) return 'ほかの取り込み (#' + S.unresolved_any.job_id + '・CSV #' + S.unresolved_any.export_id + ') が「取り込めたか分からない」のまま (その CSV の行で確かめて記す)';
    if (s && s.unresolved_job) return '前の取り込み (#' + s.unresolved_job + ') が「取り込めたか分からない」のまま (下の③で確かめて記す)';
    return null;
  }

  // 🆕 2026-10-24 ダウンロードのボタン: ポータルが断ると決まっている CSV (取り込み中・分からない回・ボタンで取り込んだ = lz-bc-import の recordDownload と同じ決まり) は押せない形と理由。
  //   読み直しのたびに合わせる (取り込みが始まった・終わった)。描いた形は views/lz-csv.ejs の dlWhy と同じ
  function dlWhy(s) {
    if (!s) return null;
    if (s.unresolved_job) return '取り込み (#' + s.unresolved_job + ') が「取り込めたか分からない」のままです。下の③で確かめて記してから、ダウンロードできます';
    if (s.active_job) return 'いまボタンでロジザードに取り込んでいます (#' + s.active_job + ')。終わってから (取り込み中はダウンロードできません)';
    if (s.imported_ok_job) return 'ボタンで取り込んだ CSV です (#' + s.imported_ok_job + ')。手でもう一度取り込まないでください (ダウンロードできません)';
    return null;
  }
  function syncDl(e, s) {
    var b = document.querySelector('button[data-lz-dl="' + e.id + '"]');
    var w = document.querySelector('[data-lz-dl-why="' + e.id + '"]');
    if (!b) return;
    var why = dlWhy(s);
    if (!b.hasAttribute('data-lz-dl-busy')) b.disabled = !!why;   // ダウンロードの途中は触らない (#Codex R1 Low)
    if (w) { w.textContent = why || ''; w.hidden = !why; }
  }
  function renderCells() {
    (P.exports || []).forEach(function (e) {
      var cell = document.querySelector('[data-bc-cell="' + e.id + '"]');
      if (!cell) return;
      var s = (S.byExport || {})[e.id] || null;
      syncDl(e, s);
      var last = s && s.last;
      cell.textContent = '';
      // 🆕 2026-10-21 CSV の商品が全部ロジザードの写しにある = 手で取り込み済み (ボタンで取り込むと 2 回登録になる = ボタンを出さない・ポータルも断る)
      //   ボタンで取り込んだ・動いている・分からない回がある CSV は今までどおり (押す前に止めた回だけ = ボタンでは取り込んでいない)
      var inLzAll = e.in_lz_total > 0 && e.in_lz === e.in_lz_total;
      var viaBtn = !!(s && (s.imported_job || s.active_job || s.unresolved_job));
      var manual = inLzAll && !viaBtn;
      var partly = !inLzAll && e.in_lz > 0 && !viaBtn;
      var top = el('div', null, [manual ? el('span', { cls: 'b ok', 'data-bc-state': 'in_lz', text: 'ロジザードにもうあります (手で取り込み済み)' })
        : last ? el('span', { cls: 'b ' + (badgeOf[last.state] || 'mute'), 'data-bc-state': last.state, text: last.state_words })
          : el('span', { cls: 'hint', 'data-bc-state': 'none', text: 'ボタンではまだ' })]);
      if (manual && last) top.appendChild(el('div', { cls: 'hint', text: 'ボタンの最後の回: ' + last.state_words }));
      if (last && last.state === 'unknown' && last.resolved_outcome) top.appendChild(el('span', { cls: 'hint', text: ' → 確かめ: ' + (last.resolved_outcome === 'imported' ? '取り込んだ' : '取り込んでいない') }));
      cell.appendChild(top);
      if (s && s.downloads) cell.appendChild(el('div', { cls: 'hint', 'data-bc-dl': s.downloads, text: 'ダウンロード ' + s.downloads + ' 回' }));
      if (partly) cell.appendChild(el('div', { cls: 'hint', 'data-bc-inlz': e.id, text: 'ロジザードに ' + e.in_lz + ' / ' + e.in_lz_total + ' 件もうあります。取り込むと 2 回登録になるので、ボタンでは取り込みません (取り消して作り直す)' }));
      if (!CAN || e.state !== 'issued') return;
      if (manual || partly) return;
      if (e.in_lz_collide > 0) { cell.appendChild(el('div', { cls: 'hint', 'data-bc-collide': e.id, text: 'ロジザードに大文字小文字だけ違う商品ID が ' + e.in_lz_collide + ' 件あります (別の商品になるおそれ)。ボタンでは取り込みません' })); return; }
      // 取り込めたか分からない回は、その CSV の行でも記せる (下の③に出るのは最後の回だけ。Codex R1 Medium)
      if (s && s.unresolved_job) cell.appendChild(resolveForm({ job_id: s.unresolved_job }, 'cell'));
      if (s && s.active_job) {
        if (S.active && (S.active.state === 'queued' || S.active.state === 'claimed') && !S.active.cancel_requested) {
          cell.appendChild(el('button', { cls: 'btn sm ghost', type: 'button', 'data-bc-cancel': S.active.job_id, text: 'やめる', on: { click: function () { cancel(S.active.job_id); } } }));
        }
        return;
      }
      var why = whyNot(e, s);
      var again = !!(s && s.imported_job);
      var b = el('button', { cls: 'btn sm' + (again ? ' ghost' : ' pri'), type: 'button', 'data-bc-import': e.id, disabled: !!why, title: why || '', text: again ? 'もう一度取り込む' : 'ロジザードに取り込む', on: { click: function () { openForm(e, s); } } });
      cell.appendChild(b);
      if (why) cell.appendChild(el('div', { cls: 'hint', 'data-bc-why': e.id, text: why }));
      if (forms[e.id] && !why) drawForm(e, s);
    });
  }

  function openForm(e, s) {
    if (!forms[e.id]) forms[e.id] = { rid: uuid(), key: null };
    drawForm(e, s);
  }
  function drawForm(e, s) {
    var row = document.querySelector('tr[data-export="' + e.id + '"]');
    if (!row) return;
    var old = document.querySelector('tr[data-bc-form="' + e.id + '"]');
    var f = forms[e.id];
    if (old) return;   // もう出ている (読み直しで消さない = 打った理由を保つ)
    var again = !!(s && s.imported_job);
    var legacy = !!LEGACY[e.id];
    var dl = (s && s.downloads > 0) || legacy;
    // ② は、できると分かっているとき (miniPC が新しい・ポータルが共有ドライブに書ける) だけ既定で入れる。できない = 外して押せない + 理由 1 行
    var EX = S.export || { ok: false, message: '② (バーコードマスタの書き出し) は、できるか分からないので、しません。今までどおり Stream Deck で書き出してください' };
    var withExp = el('input', { type: 'checkbox', checked: !!EX.ok, disabled: !EX.ok, id: 'bc-exp-' + e.id });
    var notManual = dl ? el('input', { type: 'checkbox', id: 'bc-nm-' + e.id }) : null;
    var reason = again ? el('textarea', { rows: '2', maxlength: '200', id: 'bc-rr-' + e.id, placeholder: '例: ロジザードで商品を消したので入れ直す (4〜200 字)', style: 'width:100%' }) : null;
    var go = el('button', { cls: 'btn pri sm', type: 'button', 'data-bc-go': e.id, text: again ? 'もう一度取り込む' : '取り込む' });
    var stop = el('button', { cls: 'btn ghost sm', type: 'button', text: 'やめる', on: { click: function () { delete forms[e.id]; tr.remove(); } } });
    var line = el('div', { cls: 'msgline', 'data-bc-form-msg': e.id, role: 'status' });
    var box = el('div', { cls: 'callout info' }, [el('div', { cls: 'grow' }, [
      el('div', { cls: 't', text: 'CSV #' + e.id + ' (' + e.rows + ' 件) を miniPC がロジザードに取り込みます' }),
      el('div', { cls: 'hint', text: '① インポート [商品マスタ / 新商品バーコード登録] にこの CSV そのもの (帳面のファイル・sha256 を確かめる) → ② (印があれば) バーコード情報を書き出して、共有ドライブのバーコードマスタ.csv を上書き。1〜2 分かかります。' }),
      el('label', null, [withExp, ' ② バーコードマスタ.csv も書き出す']),
      EX.ok ? null : el('div', { cls: 'hint', 'data-bc-exp-why': e.id, text: EX.message }),
      notManual ? el('div', null, [el('label', null, [notManual, legacy && !(s && s.downloads > 0) ? ' この CSV はボタンができる前に作りました (ダウンロードしたかポータルでは分からない)。手でロジザードに取り込んでいないことを確かめた' : ' この CSV はダウンロードされています。手でロジザードに取り込んでいないことを確かめた'])]) : null,
      again ? el('div', null, [el('div', { cls: 'hint', text: 'この CSV はもう取り込みました (#' + s.imported_job + ')。もう一度取り込むとロジザードに 2 回登録されることがあります。理由:' }), reason]) : null,
      el('div', { style: 'margin-top:6px' }, [go, ' ', stop]), line,
    ])]);
    var tr = el('tr', { 'data-bc-form': e.id }, [el('td', { colspan: '7' }, [box])]);
    row.parentNode.insertBefore(tr, row.nextSibling);
    go.addEventListener('click', function () {
      if (sending) return;
      var say = function (t, c) { line.className = 'msgline ' + (c || ''); line.textContent = t; };
      if (notManual && !notManual.checked) { say('手で取り込んでいないことを確かめて印を付けてください', 'err'); return; }
      var rr = reason ? reason.value.trim() : null;
      if (again && (!rr || rr.length < 4)) { say('もう一度取り込む理由を 4 字以上で書いてください', 'err'); return; }
      var body = { request_id: f.rid, text_sha256: e.sha, with_export: !!withExp.checked, confirm_not_manual: notManual ? !!notManual.checked : false, rerun_reason: again ? rr : null };
      var key = JSON.stringify([body.with_export, body.confirm_not_manual, body.rerun_reason]);
      if (f.key && f.key !== key) { f.rid = uuid(); body.request_id = f.rid; }   // 中身を変えた = 新しい押し
      f.key = key;
      sending = true; go.disabled = true; say('積んでいます…');
      post(P.base + '/api/lz-csv/' + encodeURIComponent(e.id) + '/import', body).then(function (x) {
        sending = false;
        var sure = !!x.j && (x.r.ok ? x.j.ok === true : x.r.status >= 400 && x.r.status < 500);
        if (!sure) { go.disabled = false; say('積めたか分かりません (HTTP ' + x.r.status + ')。もう一度押してください (同じ押しとして送り直します)', 'err'); return; }
        if (!x.r.ok || !x.j.ok) { go.disabled = false; f.rid = uuid(); f.key = null; say((x.j && x.j.error) || ('HTTP ' + x.r.status), 'err'); return; }
        delete forms[e.id]; tr.remove();
        msg('取り込み #' + x.j.job_id + ' を積みました。miniPC が 1 分ほどで取りに来ます (この画面で様子が出ます)', 'ok');
        poll(true);
      }).catch(function () { sending = false; go.disabled = false; say('通信できませんでした。積めたか分からないので、もう一度押してください (同じ押しとして送り直します)', 'err'); });
    });
  }

  function cancel(jobId) {
    if (!window.confirm('取り込み #' + jobId + ' をやめますか (まだ押していなければ取り込みません)')) return;
    post(P.base + '/api/lz-csv/import/' + encodeURIComponent(jobId) + '/cancel', {}).then(function (x) {
      if (!x.j || !x.r.ok || x.j.ok === false) { msg((x.j && x.j.error) || ('HTTP ' + x.r.status), 'err'); return; }
      msg(x.j.cancel_requested ? 'やめると伝えました (miniPC は押す前に止まります)' : 'やめました', 'ok');
      poll(true);
    }).catch(function () { msg('通信できませんでした', 'err'); });
  }

  function jobBlock(j, opts) {
    var kids = [
      el('div', { cls: 't' }, [el('span', { cls: 'b ' + (badgeOf[j.state] || 'mute'), 'data-bc-latest-state': j.state, text: j.state_words }), ' 取り込み #' + j.job_id + ' · CSV #' + j.export_id + ' · ' + j.rows + ' 行 · ' + j.requested_by + ' · ' + when(j.requested_at)]),
    ];
    if (j.rerun_reason) kids.push(el('div', { cls: 'hint', text: 'もう一度の理由: ' + j.rerun_reason }));
    if (j.import_summary || j.import_total != null) kids.push(el('div', { 'data-bc-result': j.job_id, text: '① 結果: 総件数 ' + (j.import_total == null ? '—' : j.import_total) + ' · エラー件数 ' + (j.import_errors == null ? '—' : j.import_errors) + (j.import_summary ? ' (' + j.import_summary + ')' : '') }));
    if (j.export_state) kids.push(el('div', { text: '② バーコードマスタ.csv: ' + (j.export_words || j.export_state) + (j.export_rows != null ? ' (' + j.export_rows + ' 行)' : '') + (j.export_detail ? ' · ' + j.export_detail : '') }));
    if (j.detail) kids.push(el('div', { cls: 'hint', 'data-bc-detail': j.job_id, text: j.detail }));
    if (j.resolved_outcome) kids.push(el('div', { cls: 'hint', text: '確かめ: ' + (j.resolved_outcome === 'imported' ? '取り込んだ' : '取り込んでいない') + ' · ' + j.resolved_by + ' · ' + when(j.resolved_at) + ' · ' + j.resolved_note }));
    if (opts && opts.cancel && (j.state === 'queued' || j.state === 'claimed') && !j.cancel_requested && CAN) kids.push(el('button', { cls: 'btn sm ghost', type: 'button', text: 'やめる', on: { click: function () { cancel(j.job_id); } } }));
    if (j.cancel_requested && ACTIVE[j.state]) kids.push(el('div', { cls: 'hint', text: 'やめると伝えた (押す前なら止まる)' }));
    if (j.state === 'unknown' && !j.resolved_outcome && CAN) kids.push(resolveForm(j));
    if (j.log) kids.push(el('details', null, [el('summary', { text: 'ログ' }), el('pre', { cls: 'mono', 'data-bc-log': j.job_id, style: 'white-space:pre-wrap;max-height:320px;overflow:auto', text: j.log })]));
    return el('div', { cls: 'callout ' + (j.state === 'imported' ? 'ok' : j.state === 'unknown' || j.state === 'import_error' ? 'warn' : 'info'), id: 'lzc-bc-job' }, [el('div', { cls: 'grow' }, kids)]);
  }
  var resolveForms = {};   // 読み直しで作り直さない (打ちかけのメモ・選んだものを保つ)
  function resolveForm(j, where) {
    var key = (where || 'panel') + ':' + j.job_id;
    if (resolveForms[key]) return resolveForms[key];
    var note = el('textarea', { rows: '2', maxlength: '200', placeholder: 'ロジザードのインポート履歴で見たこと (日時・ファイル名・件数など。4〜200 字)', style: 'width:100%' });
    var r1 = el('input', { type: 'radio', name: 'bc-res-' + key, value: 'imported' });
    var r2 = el('input', { type: 'radio', name: 'bc-res-' + key, value: 'not_imported' });
    var line = el('div', { cls: 'msgline', role: 'status' });
    var go = el('button', { cls: 'btn sm', type: 'button', 'data-bc-resolve': where === 'cell' ? null : j.job_id, 'data-bc-resolve-cellbtn': where === 'cell' ? j.job_id : null, text: '確かめたことを記す' });
    go.addEventListener('click', function () {
      var outcome = r1.checked ? 'imported' : r2.checked ? 'not_imported' : null;
      if (!outcome) { line.textContent = '「取り込んだ」か「取り込んでいない」を選んでください'; return; }
      post(P.base + '/api/lz-csv/import/' + encodeURIComponent(j.job_id) + '/resolve', { outcome: outcome, note: note.value.trim() }).then(function (x) {
        if (!x.j || !x.r.ok || x.j.ok === false) { line.textContent = (x.j && x.j.error) || ('HTTP ' + x.r.status); return; }
        msg('取り込み #' + j.job_id + ' に記しました', 'ok'); poll(true);
      }).catch(function () { line.textContent = '通信できませんでした'; });
    });
    var box = el('div', where === 'cell' ? { 'data-bc-resolve-cell': j.job_id } : { 'data-bc-resolve-form': j.job_id }, [
      el('div', { cls: 'hint', text: '取り込み #' + j.job_id + ': ロジザードの「インポート履歴」でこの時刻の取込 (logizard_bc_upload.csv) を見て記してください。記すまで、この CSV は取り込み直せません。' }),
      el('label', null, [r1, ' 取り込んだ']), ' ', el('label', null, [r2, ' 取り込んでいない']), note, go, line,
    ]);
    resolveForms[key] = box;
    return box;
  }
  function renderPanel() {
    var rn = $('#lzc-bc-runner');
    if (rn) {
      rn.textContent = '';
      var alive = S.runner && S.runner.alive;
      rn.appendChild(el('span', { cls: 'b ' + (alive ? 'ok' : 'warn'), 'data-bc-runner': alive ? 'alive' : 'offline', text: alive ? 'miniPC: 来ている' : 'miniPC: 来ていない' }));
      rn.appendChild(document.createTextNode(' 最後 ' + when(S.runner && S.runner.seen_at) + (S.runner && S.runner.busy ? ' · ' + (S.runner.busy === 'night' ? '夜なので取らない' : '忙しい: ' + S.runner.busy) : '')));
      if (S.night && S.night.enqueue) rn.appendChild(el('div', { cls: 'hint', 'data-bc-night': '1', text: P.nightWords }));
      if (BLOCK) rn.appendChild(el('div', { cls: 'hint', 'data-bc-block': BLOCK.code, text: BLOCK.message }));
    }
    var ex = $('#lzc-bc-export');
    if (ex) {
      // ② の書く先 (共有ドライブ) の様子・最後に書いた回
      ex.textContent = '';
      var X = S.export || null, D = S.drive_last || null;
      if (X) ex.appendChild(el('div', { 'data-bc-export': X.ok ? 'ok' : (X.code || 'ng'), text: X.ok ? '② 共有ドライブのバーコードマスタ.csv に書けます' + (X.drive_modified ? ' (今のファイル ' + X.drive_modified + ')' : '') : X.message }));
      if (D) ex.appendChild(el('div', { 'data-bc-drive-last': D.job_id, text: '② 最後に書いた: ' + when(D.at) + (D.rows != null ? ' · ' + D.rows + ' 行' : '') + ' · 取り込み #' + D.job_id }));
    }
    var lt = $('#lzc-bc-latest');
    if (lt) {
      lt.textContent = '';
      var j = S.active || S.latest;
      if (j) lt.appendChild(jobBlock(j, { cancel: true }));
      else lt.appendChild(el('div', { cls: 'empty', text: 'まだ取り込んでいません' }));
    }
    var rc = $('#lzc-bc-recent');
    if (rc) {
      rc.textContent = '';
      var rows = (S.recent || []).map(function (r) {
        return el('tr', null, [el('td', { cls: 'mono', text: '#' + r.job_id }), el('td', { text: 'CSV #' + r.export_id }), el('td', { text: when(r.requested_at) + ' ' + r.requested_by }),
          el('td', { text: r.state_words + (r.resolved_outcome ? ' → ' + (r.resolved_outcome === 'imported' ? '取り込んだ' : '取り込んでいない') : '') }),
          el('td', { text: r.import_total == null ? '' : '総件数 ' + r.import_total + ' · エラー ' + (r.import_errors == null ? '—' : r.import_errors) }),
          el('td', { text: r.export_state ? '② ' + (r.export_words || r.export_state) : '' })]);
      });
      rc.appendChild(rows.length ? el('table', { cls: 'tbl' }, [el('tbody', null, rows)]) : el('div', { cls: 'empty', text: 'まだありません' }));
    }
  }
  function render() { renderCells(); renderPanel(); }
  function poll(soon) {
    if (timer) clearTimeout(timer);
    var ids = (P.exports || []).map(function (e) { return e.id; }).join(',');
    var go = function () {
      fetch(P.base + '/api/lz-csv/import-status?ids=' + encodeURIComponent(ids), { headers: { Accept: 'application/json' } })
        .then(function (r) { return r.json(); })
        .then(function (j) {
          if (j && j.ok) {
            var wasActive = !!S.active;
            S = j; BLOCK = j.block || null; CAN = CAN && j.can_import !== false;
            render();
            if (wasActive && !S.active && S.latest) msg('取り込み #' + S.latest.job_id + ': ' + S.latest.state_words, S.latest.state === 'imported' ? 'ok' : 'err');
          }
        })
        .catch(function () { /* 次の読み直しで */ })
        .then(function () { timer = setTimeout(go, S.active ? 4000 : 30000); });
    };
    timer = setTimeout(go, soon ? 300 : (S.active ? 4000 : 30000));
  }
  render();
  poll(false);
})();
