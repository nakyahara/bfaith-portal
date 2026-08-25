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
    // 🚨Shift_JIS で保存する。iS-2 は Shift_JIS で読むので UTF-8 だと住所が化ける (2026-08-25 実害)。
    //   変換できない文字があれば投げる → 呼び出し側で「作らない」
    var bytes = BF_SJIS.encodeSjis(text);
    var blob = new Blob([bytes], { type: 'text/csv;charset=shift_jis' });
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

    // 🚨Shift_JIS にできない文字 (環境依存文字など) が住所にあると伝票が化けるので、ここで先に弾く
    try {
      BF_SJIS.encodeSjis(built.csv);
    } catch (e) {
      showError(el, 'CSVを組み立てられませんでした', [e && e.message ? e.message : String(e), '住所に環境依存文字が含まれています。iS-2 に手入力してください']);
      return;
    }

    el.innerHTML =
      '<div class="bf-head">福通伝票CSV<span class="bf-plan">画面から読み取り</span></div>' +
      '<div class="bf-body">' +
      '<div class="bf-sum">出荷日 <b>' + esc(built.summary.出荷日) + '</b> ／ 納品 <b>' +
      esc(built.summary.納品数) + '</b>件 ／ 伝票 <b>' + esc(built.summary.伝票枚数) + '</b>枚</div>' +
      '<table><thead><tr><th>納品番号</th><th>FC</th><th>箱</th><th>宛先 (画面の住所)</th></tr></thead>' +
      '<tbody>' + rows + '</tbody></table>' +
      '<div class="bf-note">この内容で1箱1伝票のCSVを作ります (宛先は画面の住所をそのまま書きます)。取り込む前に箱数が実物と合っているか確認してください。</div>' +
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
