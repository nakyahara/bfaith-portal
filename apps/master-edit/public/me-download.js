/*
 * me-download.js — ファイルのダウンロードの部品 (🆕 2026-10-24・NE 登録の CSV とロジザード登録の CSV の画面で同じもの)
 *   なぜ: 隠した <a download> でサーバーの URL を開くと、断られた (409 / 503 など) ときも、ブラウザが 2 つ目の自動のダウンロードを止めたときも、
 *   画面には何も出ない (中原さん 10/24「セットの CSV が DL できない」「ロジザードの CSV のダウンロードを押しても落ちない」)。
 *   ここでは fetch で byte 列を受け取り、断られたらサーバーの答え (error・reason) をそのまま返す = 画面がその行・カードに理由を出す。
 *   受け取れたら sha256 (渡された値 か X-Content-SHA256) を照らしてから保存に回す (byte 列はそのまま = 文字コード・改行を変えない)。
 * 使い方: MasterEdit.downloadFile(url, { sha?, name? }) → Promise<{ ok: true, name, url } | { ok: false, status, reason, error, detail }>
 *   (例外は投げない。通信できない = status 0・reason 'network')。MasterEdit.saveAgainLink(結果) = 「届いていなければ、ここを押して保存」のリンク
 *   sha256 を照らせない (crypto.subtle の無い http の画面) = 照らさずに保存する (byte 列は同じ接続で最後まで受け取った = 途中で切れたら fetch が失敗する。
 *   照らせないのを失敗にすると http の画面ではダウンロードできなくなる = #Codex R1 Medium 3 は採らない)
 */
(function () {
  'use strict';
  var ME = window.MasterEdit = window.MasterEdit || {};
  function hex(buf) { return Array.prototype.map.call(new Uint8Array(buf), function (x) { return x.toString(16).padStart(2, '0'); }).join(''); }
  function digest(buf) {
    if (!(window.crypto && crypto.subtle)) return Promise.resolve(null);   // http の別の機械など = 照らせない (受け取った byte 列はそのまま保存)
    return crypto.subtle.digest('SHA-256', buf).then(hex).catch(function () { return null; });
  }
  function fileNameOf(cd) { var m = /filename="?([^";]+)"?/i.exec(cd || ''); return m ? m[1] : ''; }
  // 保存に回す。ブラウザが保存したか (キャンセル・止めた) はページから分からない (#Codex R1 High) =
  //   受け取った byte 列の URL を 10 分残して返す = 画面は「届いていなければ、ここを押して保存」のリンクにする (人が押すリンク = ブラウザは止めない)
  function save(buf, name, type) {
    var url = URL.createObjectURL(new Blob([buf], { type: type || 'application/octet-stream' }));
    var a = document.createElement('a');
    a.href = url; a.download = name; a.style.display = 'none';
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function () { URL.revokeObjectURL(url); }, 600000);
    return url;
  }
  /** 「届いていなければ、ここを押して保存」のリンク (保存に回した byte 列。10 分で使えなくなる) */
  ME.saveAgainLink = function (x) {
    if (!x || !x.url) return null;
    var a = document.createElement('a');
    a.href = x.url; a.download = x.name; a.className = 'btn sm ghost'; a.setAttribute('data-save-again', '');
    a.textContent = 'ファイルが届いていなければ、ここを押して保存';
    return a;
  };
  ME.downloadFile = function (url, opt) {
    opt = opt || {};
    return fetch(url, { headers: { Accept: 'text/csv, application/json' }, cache: 'no-store', credentials: 'same-origin' }).then(function (r) {
      if (!r.ok) {
        return r.json().catch(function () { return {}; }).then(function (j) {
          return { ok: false, status: r.status, reason: j.reason || null, detail: j.detail || null, error: j.error || '' };
        });
      }
      var name = fileNameOf(r.headers.get('Content-Disposition')) || opt.name || 'download.csv';
      var want = String(opt.sha || r.headers.get('X-Content-SHA256') || '').toLowerCase();
      var type = r.headers.get('Content-Type') || '';
      return r.arrayBuffer().then(function (buf) {
        return digest(buf).then(function (h) {
          if (h && want && h !== want) return { ok: false, status: r.status, reason: 'sha_mismatch', error: 'ダウンロードしたファイルが記録と違います (途中で切れた など)。もう一度押してください' };
          var u = save(buf, name, type);
          return { ok: true, name: name, url: u };
        });
      });
    }).catch(function () { return { ok: false, status: 0, reason: 'network', error: '通信できませんでした。もう一度押してください' }; });
  };
})();
