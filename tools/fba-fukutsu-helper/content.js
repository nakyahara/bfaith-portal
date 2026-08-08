// Seller Central の納品プラン画面 (URLに ?wf=) にボタンを出し、
// 福山通運 iS-2 に取り込む送り状CSVを落とせるようにする。
//
// ⭐押した時だけCSVを作る。自動生成にしないのは、伝票発行が運賃の発生する不可逆な操作で、
//   「どの納品をいま出すか」を人が決めたほうが二重発行を防げるため。
// ⭐ダウンロード前に必ず内容 (納品番号・宛先・箱数) を見せる。押し間違いをここで止める。

(() => {
  'use strict';
  const PANEL_ID = 'bf-fukutsu-panel';
  const BTN_ID = 'bf-fukutsu-btn';

  const planIdFromUrl = () => {
    const m = location.href.match(/[?&]wf=([^&\s]+)/i);
    if (!m) return null;
    const id = decodeURIComponent(m[1]);
    return /^wf[0-9a-f-]{8,}$/i.test(id) ? id : null;
  };

  const send = (msg) =>
    new Promise((resolve) => {
      chrome.runtime.sendMessage(msg, (res) => {
        if (chrome.runtime.lastError) return resolve({ ok: false, error: chrome.runtime.lastError.message });
        resolve(res || { ok: false, error: '応答がありません' });
      });
    });

  function closePanel() {
    const p = document.getElementById(PANEL_ID);
    if (p) p.remove();
  }

  function panel() {
    closePanel();
    const el = document.createElement('div');
    el.id = PANEL_ID;
    document.body.appendChild(el);
    return el;
  }

  function download(filename, text) {
    // Blob + <a download>。SW側では createObjectURL が使えないのでここで作る
    const blob = new Blob([text], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  }

  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

  async function onClick() {
    const wf = planIdFromUrl();
    if (!wf) return;
    const el = panel();
    el.innerHTML = '<div class="bf-head">福通伝票CSV</div><div class="bf-body">Amazonから納品情報を取得中…</div>';

    const res = await send({ type: 'plan', wf });
    if (!res.ok) {
      el.innerHTML =
        '<div class="bf-head">福通伝票CSV</div>' +
        `<div class="bf-body bf-ng">取得できません<br>${esc(res.error)}</div>` +
        '<div class="bf-foot"><button class="bf-close">閉じる</button></div>';
      el.querySelector('.bf-close').onclick = closePanel;
      return;
    }

    const d = res.data;
    const rows = d.shipments
      .map(
        (s) =>
          `<tr><td>${esc(s.納品番号)}</td><td>${esc(s.宛先FC)}</td><td class="bf-num">${esc(s.箱数)}</td>` +
          `<td>${esc(s.status)}</td><td>${esc(s.宛先)}</td></tr>`,
      )
      .join('');
    const warn = d.shipments.some((s) => String(s.追跡番号) === '登録済み')
      ? '<div class="bf-warn">⚠️ すでに追跡番号が入っている納品があります。伝票を二重に出していないか確認してください</div>'
      : '';

    el.innerHTML =
      `<div class="bf-head">福通伝票CSV<span class="bf-plan">${esc(d.planName || d.inboundPlanId)}</span></div>` +
      '<div class="bf-body">' +
      `<div class="bf-sum">出荷日 <b>${esc(d.出荷日)}</b> ／ 伝票 <b>${esc(d.伝票枚数)}</b>枚</div>` +
      warn +
      '<table><thead><tr><th>納品番号</th><th>宛先</th><th>箱</th><th>状態</th><th>宛先の出し方</th></tr></thead>' +
      `<tbody>${rows}</tbody></table>` +
      '</div>' +
      '<div class="bf-foot"><button class="bf-close">閉じる</button><button class="bf-go">この内容でCSVを作る</button></div>';

    el.querySelector('.bf-close').onclick = closePanel;
    el.querySelector('.bf-go').onclick = async (ev) => {
      const btn = ev.currentTarget;
      btn.disabled = true;
      btn.textContent = '作成中…';
      const r = await send({ type: 'planCsv', wf });
      if (!r.ok) {
        btn.disabled = false;
        btn.textContent = 'この内容でCSVを作る';
        el.querySelector('.bf-body').insertAdjacentHTML('beforeend', `<div class="bf-ng">${esc(r.error)}</div>`);
        return;
      }
      download(r.data.filename, r.data.csv);
      btn.textContent = `✅ ${r.data.rows}枚ぶん保存しました`;
    };
  }

  function ensureButton() {
    const wf = planIdFromUrl();
    const existing = document.getElementById(BTN_ID);
    if (!wf) {
      if (existing) existing.remove();
      closePanel();
      return;
    }
    if (existing) return;
    const b = document.createElement('button');
    b.id = BTN_ID;
    b.type = 'button';
    b.textContent = '📦 福通伝票CSV';
    b.title = 'この納品プランから福山通運の送り状CSVを作ります';
    b.onclick = onClick;
    document.body.appendChild(b);
  }

  // Seller Central は SPA なので URL の変化を拾う
  ensureButton();
  let lastUrl = location.href;
  setInterval(() => {
    if (location.href !== lastUrl) {
      lastUrl = location.href;
      ensureButton();
    }
  }, 1000);
})();
