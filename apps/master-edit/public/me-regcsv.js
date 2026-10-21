/*
 * me-regcsv.js — NE 登録の CSV の画面の動き (第 2 段 10/5)
 *   - 🆕 10/20 作る: ボタン 1 つ (POST /api/reg-csv/build-all) = いま作れる商品を全部 (単品 1 つ・セット 1 つのファイル) 作って配る → その場でダウンロード。「一部を外して作る」の印を外したものは作らない
 *   - 🆕 10/20「NE に取り込んだ」= ボタン 1 つの申告 (配ったファイルの sha256 をそのまま送る・DB が記録と同じかを確かめる)。失敗のときは「一部失敗した」「全部だめだった」
 *   - 配る: 押すとファイルをダウンロードして画面を読み直す
 *   - 申告: CSV のファイルを落とす (押して選ぶ) と、その場で sha256 を出して配ったファイルと照合する (64 文字を写さない)。結果は大きな 3 択
 *   - 使わない・実機で確かめた
 *   - 申告・使わないの書きかけ = 未保存 (data-dirty-field)。離れるときに確かめる
 *   - 送り方 (API) は前と同じ: POST /api/reg-csv/exports・/api/reg-csv/exports/:id/issue・/declare・/supersede・/api/reg-csv/verified・GET /file
 *   - 通ったら画面を読み直す (新しい状態)。上に結果を 1 回だけ出す
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var ME = window.MasterEdit = window.MasterEdit || {};
  var root = document.getElementById('rc');
  if (!root) return;
  var base = root.getAttribute('data-base') || '';
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  var buildAllId = uuid();   // 作る 1 回に 1 つ (押し直しても 2 つ作らない = 同じ番号はサーバーが前のファイルを返す)
  function post(path, body) {
    return fetch(base + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body || {}) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { ok: r.ok && j.ok !== false, status: r.status, j: j }; }); });
  }
  var errText = function (j, status) { return (j.error || ('HTTP ' + status)) + (Array.isArray(j.items) ? '\n' + j.items.map(function (x) { return x.code + ': ' + (x.blockers || []).join(' / '); }).join('\n') : ''); };
  function say(el, t, ok) { if (!el) return; el.className = el.className.replace(/\b(ok|err)\b/g, '').trim() + ' ' + (ok ? 'ok' : 'err'); el.textContent = t; el.style.whiteSpace = 'pre-wrap'; }

  /* ---------- 書きかけ (未保存) ---------- */
  var initial = new Map();
  function valueOf(el) { return el.type === 'checkbox' || el.type === 'radio' ? (el.checked ? '1' : '') : el.value; }
  $$('[data-dirty-field]', root).forEach(function (el) { initial.set(el, valueOf(el)); });
  var done = false;
  function dirtyDrawers() {
    if (done) return [];
    return $$('.drawer', root).filter(function (d) { return $$('[data-dirty-field]', d).some(function (el) { return !el.disabled && valueOf(el) !== initial.get(el); }); });
  }
  function drawerName(d) { var card = d.closest('.exp'); return 'ファイル #' + card.getAttribute('data-id') + ' の' + (d.getAttribute('data-drawer') === 'declare' ? '申告' : '「使わない」') + ' (書きかけ)'; }
  function update() { ME.setUnsaved(dirtyDrawers().length); }
  ME.dirty = function () { var ds = dirtyDrawers(); return { n: ds.length, items: ds.map(drawerName), impacts: [] }; };
  ME.review = function () {
    var d = dirtyDrawers()[0]; if (!d) return;
    d.hidden = false; d.scrollIntoView({ behavior: 'smooth', block: 'center' });
    var f = $('input:not([type=file]), button', d); if (f) f.focus({ preventScroll: true });
  };
  root.addEventListener('input', update);
  root.addEventListener('change', update);

  /* ---------- 一部を外して作る: 外す商品 (印を外したもの・まとまりは丸ごと) ---------- */
  function excluded() {
    var out = [];
    $$('#pick-some input.pick:not(:checked)', root).forEach(function (x) { try { out = out.concat(JSON.parse(x.getAttribute('data-codes') || '[]')); } catch (e) { out.push(x.value); } });
    return out;
  }
  root.addEventListener('change', function (e) {
    if (!(e.target.classList && e.target.classList.contains('pick'))) return;
    var n = $$('#pick-some input.pick:not(:checked)', root).length, h = $('#next-hint');
    if (h) h.textContent = n ? '印を外した ' + n + ' つは作りません' : '押すと CSV ができて、そのままダウンロードされます';
    buildAllId = uuid();   // 中身が変わった = 新しい番号
  });

  /* ---------- 申告: ファイルを落とすと sha256 を照合 ---------- */
  function hex(buf) { return Array.prototype.map.call(new Uint8Array(buf), function (x) { return x.toString(16).padStart(2, '0'); }).join(''); }
  function hashFile(drop, file) {
    var card = drop.closest('.exp'), want = card.getAttribute('data-sha'), id = card.getAttribute('data-id');
    var t = $('[data-drop-t]', drop), s = $('[data-drop-s]', drop), shaIn = $('input[name="sha256"]', card);
    drop.classList.remove('done', 'bad');
    if (!file) return;
    if (!(window.crypto && crypto.subtle && file.arrayBuffer)) { t.textContent = file.name; s.textContent = 'このブラウザではその場で照合できません。下の「sha256 を貼る」を使ってください'; drop.classList.add('bad'); return; }
    t.textContent = file.name; s.textContent = '照合しています…';
    file.arrayBuffer().then(function (buf) { return crypto.subtle.digest('SHA-256', buf); }).then(function (d) {
      var h = hex(d);
      if (shaIn) { shaIn.value = h; shaIn.dispatchEvent(new Event('input', { bubbles: true })); }
      if (h === want) { drop.classList.add('done'); s.textContent = '配ったファイル #' + id + ' と同じです (sha256 が一致)'; }
      else { drop.classList.add('bad'); s.textContent = '配ったファイル #' + id + ' と違います。別のファイルか、Excel などで開いて保存し直したファイルです (このままでは申告できません)'; }
      var ic = $('.fileico use', drop); if (ic) ic.setAttribute('href', h === want ? '#i-check' : '#i-warn');
    }).catch(function () { drop.classList.add('bad'); s.textContent = '読めませんでした。もう一度落とすか、sha256 を貼ってください'; });
  }
  $$('[data-drop]', root).forEach(function (drop) {
    var input = $('input[data-sha-file]', drop);
    input.addEventListener('change', function () { hashFile(drop, input.files && input.files[0]); });
    drop.addEventListener('dragover', function (e) { e.preventDefault(); drop.classList.add('over'); });
    drop.addEventListener('dragleave', function () { drop.classList.remove('over'); });
    drop.addEventListener('drop', function (e) { e.preventDefault(); drop.classList.remove('over'); var f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0]; hashFile(drop, f); });
  });
  // いまの時刻を入れる (このパソコンの時刻・秒まで = 配った時刻より前にならない。前は 1 分前の分までにしていて、配った直後だと断られた = #1628 Codex R1 M3)
  root.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('[data-now]'); if (!b) return;
    var inp = $('input[name="imported_at"]', b.closest('.drawer'));
    var d = new Date(), p = function (n) { return String(n).padStart(2, '0'); };
    inp.value = d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + 'T' + p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
    inp.dispatchEvent(new Event('input', { bubbles: true }));
  });

  /* ---------- 通ったら読み直す (上に 1 回だけ知らせる) ---------- */
  var NOTE_KEY = 'master-edit:regcsv-note';
  function finish(text) {
    done = true; update();
    try { sessionStorage.setItem(NOTE_KEY, JSON.stringify({ at: Date.now(), t: text })); } catch (e) { /* 置けない = 知らせを出さないだけ */ }
    if (ME.reloadPage) ME.reloadPage(); else location.reload();
  }
  (function showNote() {
    var n = null;
    try { var raw = sessionStorage.getItem(NOTE_KEY); if (raw) { sessionStorage.removeItem(NOTE_KEY); n = JSON.parse(raw); } } catch (e) { return; }
    if (!n || !n.t || !(Date.now() - Number(n.at) < 5 * 60000)) return;
    var box = document.createElement('div');
    box.className = 'result ok saved-note'; box.setAttribute('role', 'status');
    var rt = document.createElement('div'); rt.className = 'rt'; rt.textContent = n.t; box.appendChild(rt);
    var ph = $('.ph', root); ph.parentNode.insertBefore(box, ph.nextSibling);
    if (ME.toast) ME.toast(n.t);
  })();
  function download(id) {
    var a = document.createElement('a');
    a.href = base + '/api/reg-csv/exports/' + id + '/file'; a.setAttribute('download', '');
    document.body.appendChild(a); a.click(); a.remove();
  }

  /* ---------- ボタン ---------- */
  root.addEventListener('click', function (ev) {
    var b = ev.target.closest && ev.target.closest('button[data-act]'); if (!b || b.disabled) return;
    var card = b.closest('.exp');
    var act = b.getAttribute('data-act');
    var cardMsg = card ? $('[data-msg-card]', card) : null;
    if (act === 'show-declare' || act === 'show-supersede') {
      var d = $(act === 'show-declare' ? '[data-drawer="declare"]' : '[data-drawer="supersede"]', card);
      var other = $(act === 'show-declare' ? '[data-drawer="supersede"]' : '[data-drawer="declare"]', card);
      if (other && !other.hidden) { other.hidden = true; var ob = $('[aria-controls="' + other.id + '"]', card); if (ob) ob.setAttribute('aria-expanded', 'false'); }
      d.hidden = !d.hidden; b.setAttribute('aria-expanded', d.hidden ? 'false' : 'true');
      if (!d.hidden) { d.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); var f = $('input:not([type=file])', d); if (f && f.offsetParent) f.focus({ preventScroll: true }); else { var dz = $('input[data-sha-file]', d); if (dz) dz.focus({ preventScroll: true }); } }
      return;
    }
    if (act === 'close-drawer') {
      var dr = b.closest('.drawer'); dr.hidden = true;
      var opener = $('[aria-controls="' + dr.id + '"]', card); if (opener) { opener.setAttribute('aria-expanded', 'false'); opener.focus(); }
      return;
    }
    var id = card ? card.getAttribute('data-id') : null;
    var drawer = b.closest('.drawer');
    var dmsg = drawer ? $('[data-msg]', drawer) : card ? cardMsg : $('#build-msg');
    b.disabled = true;
    var fail = function (t) { say(dmsg, t, false); b.disabled = false; };
    var p;
    if (act === 'build-all') {
      // 🆕 10/20: いま作れる商品を全部 = 作る → 配る (サーバー) → ここでダウンロード → 画面を読み直す (同じ画面)
      var bm = $('#build-msg'), out = $('#build-result');
      say(bm, 'CSV を作っています…', true);
      p = post('/api/reg-csv/build-all', { request_id: buildAllId, exclude: excluded() }).then(function (r) {
        var j = r.j || {};
        var files = Array.isArray(j.files) ? j.files : [];
        var errs = Array.isArray(j.errors) ? j.errors : [];
        if (!r.ok || !files.length) { say(bm, errText(j, r.status) || errs.map(function (x) { return x.message; }).join('\n'), false); buildAllId = uuid(); b.disabled = false; return; }
        var got = files.filter(function (f) { return f.state === 'issued' || f.state === 'declared'; });
        var label = function (f) { return (f.kind === 'sets' ? 'セット ' : '単品 ') + f.item_count + ' 件'; };
        say(bm, 'CSV ができました: ' + files.map(label).join('・') + (got.length ? ' (ダウンロードしています)' : '') + (errs.length ? '\n作れなかったもの: ' + errs.map(function (x) { return x.message; }).join(' / ') : ''), !errs.length);
        if (out) {
          out.innerHTML = '';
          got.forEach(function (f) {
            var a = document.createElement('a'); a.className = 'btn sm'; a.href = base + '/api/reg-csv/exports/' + f.export_id + '/file'; a.setAttribute('download', '');
            a.textContent = label(f) + ' の CSV をもう一度ダウンロード'; a.style.marginRight = '8px'; out.appendChild(a);
          });
        }
        got.forEach(function (f, i) { setTimeout(function () { download(f.export_id); }, i * 700); });
        if (errs.length) { buildAllId = uuid(); b.disabled = false; return; }
        setTimeout(function () { finish('CSV ができました (' + files.map(label).join('・') + ')。NE の「商品一括登録」で取り込んで、「NE に取り込んだ」を押してください' + (files.some(function (f) { return f.kind === 'sets'; }) ? ' (セットの CSV は必ず押してください。単品は押さなくても翌朝自動で確かめます)' : ' (押さなくても翌朝自動で確かめます)')); }, 1200 + got.length * 700);
      });
    } else if (act === 'declare-quick') {
      // 🆕 10/20: ボタン 1 つの申告 = 配ったファイルの sha256 (この画面が持っている記録の値) をそのまま送る。DB が記録と同じかを確かめる
      var qres = b.getAttribute('data-result');
      if (qres === 'rejected_all' && !window.confirm('NE で全部だめだった (1 件も入らなかった) と知らせます。よろしいですか？')) { b.disabled = false; return; }
      var qm = $('input[name="quick_message"]', card);
      say(cardMsg, '知らせています…', true);
      p = post('/api/reg-csv/exports/' + id + '/declare', { sha256: card.getAttribute('data-sha'), result: qres, ne_message: qm ? qm.value : '' })
        .then(function (r) { if (!r.ok) { say(cardMsg, errText(r.j, r.status), false); b.disabled = false; return; }
          finish(qres === 'ok' ? '「NE に取り込んだ」を記録しました。翌朝の照合で NE と同じか確かめます' : qres === 'partial' ? '「一部失敗した」を記録しました。翌朝、入らなかった商品が「次の CSV」に戻ります' : '「全部だめだった」を記録しました。商品は「次の CSV」に戻ります'); });
    } else if (act === 'issue') {
      say(cardMsg, '配っています…', true);
      p = post('/api/reg-csv/exports/' + id + '/issue', {}).then(function (r) {
        if (!r.ok || r.j.refused) { say(cardMsg, r.j.refused ? '使わないにした商品があるので配れません (ファイルを閉じました)。作り直してください' : errText(r.j, r.status), false); b.disabled = false; return; }
        download(id);
        setTimeout(function () { finish('ファイル #' + id + ' を配りました (ダウンロード)。NE の「商品一括登録」で取り込んでください。' + (card.getAttribute('data-kind') === 'sets' ? 'セットは取り込んだら「NE に取り込んだ」を押してください (押すと翌朝の照合で確かめます)' : '翌朝の照合で自動で確かめます (申告はしなくて大丈夫です)')); }, 1200);
      });
    } else if (act === 'declare') {
      var res = $('input[type=radio]:checked', drawer);
      var sha = $('input[name="sha256"]', drawer).value.trim().toLowerCase();
      if (!sha) { fail('取り込んだファイルを落とすか、sha256 を貼ってください'); var dz2 = $('input[data-sha-file]', drawer); if (dz2) dz2.focus(); return; }
      if (sha !== card.getAttribute('data-sha')) { fail('配ったファイル #' + id + ' と違うファイルです (sha256 が合いません)。配ったファイルをそのまま取り込んでください'); return; }
      if (!res) { fail('NE の結果を選んでください'); var r0 = $('input[type=radio]', drawer); if (r0) r0.focus(); return; }
      var at = $('input[name="imported_at"]', drawer).value;
      say(dmsg, '申告しています…', true);
      p = post('/api/reg-csv/exports/' + id + '/declare', { sha256: sha, result: res.value, ne_message: $('input[name="ne_message"]', drawer).value, imported_at: at ? new Date(at).toISOString() : null, note: $('input[name="note"]', drawer).value })
        .then(function (r) { if (!r.ok) { fail(errText(r.j, r.status)); return; } finish('ファイル #' + id + ' を「取り込んだ」と申告しました。翌朝の照合で NE と同じか確かめます'); });
    } else if (act === 'supersede') {
      say(dmsg, '使わないにしています…', true);
      p = post('/api/reg-csv/exports/' + id + '/supersede', { reason: $('input[name="reason"]', drawer).value, correction: $('input[name="correction"]', drawer).value, confirm: $('input[name="confirm"]', drawer).checked })
        .then(function (r) { if (!r.ok) { fail(errText(r.j, r.status)); return; } finish('ファイル #' + id + ' を「使わない」にしました。直してから、もう一度 CSV を作ってください'); });
    } else if (act === 'verified') {
      say(cardMsg, '記録しています…', true);
      p = post('/api/reg-csv/verified', { kind: b.getAttribute('data-kind'), result: b.getAttribute('data-result'), export_id: id })
        .then(function (r) { if (!r.ok) { fail(errText(r.j, r.status)); return; } finish(b.getAttribute('data-result') === 'ok' ? 'この形を「実機で確かめた」にしました' : '「問題があった」と記録しました'); });
    } else { b.disabled = false; return; }
    p.catch(function () { fail('通信できませんでした。もう一度押してください'); });
  });
  update();
})();
