// Seller Central「ステップ3 – 印刷された輸送箱ラベル」の画面にボタンを出し、
// 福山通運 iS-2 に取り込む送り状CSVをその場で作る。
//
// ⭐ネットワークを一切使わない。画面に出ている情報だけでCSVを組み立てる。
//   → サーバもAPIトークンもSP-APIも要らず、社外 (自宅) でも動く。
// ⭐押した時だけ作る。伝票発行は運賃が発生する不可逆な操作なので、
//   「どの納品をいま出すか」は人が決める (二重発行の防止)。
// ⭐ダウンロード前に必ず内容を見せる。読み取りに失敗したら**作らずに理由を出す**。

(function () {
  'use strict';
  var PANEL_ID = 'bf-fukutsu-panel';
  var BTN_ID = 'bf-fukutsu-btn';

  function pageText() {
    return (document.body && document.body.innerText) || '';
  }

  /** ステップ3の画面か (納品番号が出ているか) */
  function looksLikeStep3() {
    return /納品番号\s*[：:]\s*[A-Z0-9]{6,}/.test(pageText());
  }

  function closePanel() {
    var p = document.getElementById(PANEL_ID);
    if (p) p.remove();
  }

  function panel() {
    closePanel();
    var el = document.createElement('div');
    el.id = PANEL_ID;
    document.body.appendChild(el);
    return el;
  }

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  function download(filename, text) {
    // UTF-8 で保存する (現行の fukutu.csv と同じ。iS-2 の取込はこれで通っている)
    var blob = new Blob([text], { type: 'text/csv;charset=utf-8' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function () { URL.revokeObjectURL(url); }, 10000);
  }

  function showError(el, title, problems) {
    el.innerHTML =
      '<div class="bf-head">福通伝票CSV</div>' +
      '<div class="bf-body"><div class="bf-ng"><b>' + esc(title) + '</b><br>' +
      problems.map(function (p) { return '・' + esc(p); }).join('<br>') +
      '</div><div class="bf-note">CSVは作っていません。画面を確認してからもう一度押してください。</div></div>' +
      '<div class="bf-foot"><button class="bf-close">閉じる</button></div>';
    el.querySelector('.bf-close').onclick = closePanel;
  }

  function onClick() {
    var el = panel();
    var parsed = BF_PAGE.parseShipments(pageText());

    if (!parsed.ok) {
      showError(el, '画面から読み取れませんでした', parsed.problems);
      return;
    }

    var built;
    try {
      built = BF_FUKUTSU.buildFukutsuCsv(parsed.shipments, parsed.ymd);
    } catch (e) {
      showError(el, 'CSVを組み立てられませんでした', [e && e.message ? e.message : String(e)]);
      return;
    }

    var rows = built.summary.detail.map(function (d) {
      return '<tr><td>' + esc(d.納品番号) + '</td><td>' + esc(d.fcCode) + '</td>' +
        '<td class="bf-num">' + esc(d.箱数) + '</td><td>' + esc(d.宛先) + '</td></tr>';
    }).join('');

    var unreg = built.summary.detail.filter(function (d) { return /未登録/.test(d.宛先); });
    var warn = unreg.length
      ? '<div class="bf-warn">⚠️ ' + esc(unreg.map(function (d) { return d.fcCode; }).join(', ')) +
        ' は iS-2 のお届け先マスタに無いため、住所を書き出しています。取込後に宛先を目視で確認してください</div>'
      : '';

    el.innerHTML =
      '<div class="bf-head">福通伝票CSV<span class="bf-plan">画面から読み取り</span></div>' +
      '<div class="bf-body">' +
      '<div class="bf-sum">出荷日 <b>' + esc(built.summary.出荷日) + '</b> ／ 納品 <b>' +
      esc(built.summary.納品数) + '</b>件 ／ 伝票 <b>' + esc(built.summary.伝票枚数) + '</b>枚</div>' +
      warn +
      '<table><thead><tr><th>納品番号</th><th>宛先</th><th>箱</th><th>宛先の出し方</th></tr></thead>' +
      '<tbody>' + rows + '</tbody></table>' +
      '<div class="bf-note">この内容で1箱1伝票のCSVを作ります。取り込む前に箱数が実物と合っているか確認してください。</div>' +
      '</div>' +
      '<div class="bf-foot"><button class="bf-close">閉じる</button>' +
      '<button class="bf-go">この内容でCSVを作る</button></div>';

    el.querySelector('.bf-close').onclick = closePanel;
    el.querySelector('.bf-go').onclick = function (ev) {
      var btn = ev.currentTarget;
      btn.disabled = true;
      download('fukutu_' + built.summary.出荷日 + '.csv', built.csv);
      btn.textContent = '✅ ' + built.summary.伝票枚数 + '枚ぶん保存しました';
    };
  }

  function ensureButton() {
    var existing = document.getElementById(BTN_ID);
    if (!looksLikeStep3()) {
      if (existing) existing.remove();
      closePanel();
      return;
    }
    if (existing) return;
    var b = document.createElement('button');
    b.id = BTN_ID;
    b.type = 'button';
    b.textContent = '📦 福通伝票CSV';
    b.title = 'この画面の納品情報から福山通運の送り状CSVを作ります';
    b.onclick = onClick;
    document.body.appendChild(b);
  }

  // Seller Central は SPA。URLだけでなく本文も差し替わるので定期的に見る
  ensureButton();
  setInterval(ensureButton, 1500);
})();
