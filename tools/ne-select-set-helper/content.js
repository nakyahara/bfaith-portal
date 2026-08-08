/**
 * NEの受注伝票画面 (/Userjyuchu/jyuchuInp) で「選べる〇種セット」を実SKUの明細行に展開する。
 *
 * 実際にやること (2026-08-07 に実機で確認済みの手順をなぞる):
 *   1. 明細グリッドから 商品コード / 商品op / 受注数 を読む
 *   2. ポータルに投げて「どの商品コードを何個入れるか」を貰う
 *   3. 人が内容を確認して押したら、NE標準の「明細入出力補助」に流し込んで貼付け
 *   4. 元の架空SKU行のキャンセルにチェックを入れる
 *   5. 保存は人がやる。保存時の「商品計が一致していません。再計算しますか?」は必ずキャンセル
 *
 * 設計メモ:
 * - グリッドの行にボタンを差し込むとNE側の再描画で消えるため、パネルをグリッドの上に置く
 * - 列の位置は決め打ちせずヘッダーの見出しから引く (NEの表示設定で列を隠せるため)
 * - 明細入出力補助のダイアログは開かなくても値を入れて押せる (visibility:hidden のままで通る)
 * - 「貼り付けを完了しました」というネイティブalertは拡張からは閉じられないので人がOKを押す
 */
(() => {
  const GRID_ID = 'search_order_line_result';
  const PANEL_ID = 'bf-select-set-panel';

  const A = {
    codes: '#meisai_assist_input_syohin_code',
    quantities: '#meisai_assist_input_jyuchu_su',
    paste: '#meisai_assist_input_btn',
  };

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  function send(msg) {
    return new Promise((resolve) => {
      chrome.runtime.sendMessage(msg, (res) => {
        if (chrome.runtime.lastError) return resolve({ ok: false, error: chrome.runtime.lastError.message });
        resolve(res || { ok: false, error: '応答がありません' });
      });
    });
  }

  function grid() {
    const c = document.getElementById(GRID_ID);
    return c ? c.querySelector('.ne-table table') : null;
  }

  /** ヘッダーの見出しから列の位置を引く (表示設定で列が隠れていても壊れないように) */
  function columns(table) {
    const idx = {};
    [...table.querySelectorAll('thead th')].forEach((th, i) => {
      const t = (th.innerText || '').replace(/\s/g, '');
      if (t === '商品コード') idx.code = i;
      else if (/^商品op$/i.test(t)) idx.op = i;
      else if (t === '受注数') idx.qty = i;
      else if (/ｷｬﾝｾﾙ|キャンセル/.test(t)) idx.cancel = i;
    });
    return idx;
  }

  function readRows() {
    const table = grid();
    if (!table) return { rows: [], cols: {} };
    const cols = columns(table);
    if (cols.code == null || cols.op == null) return { rows: [], cols };
    const rows = [...table.querySelectorAll('tbody tr')].map((tr, i) => {
      const tds = [...tr.querySelectorAll('td')];
      const text = (n) => (n == null || !tds[n] ? '' : (tds[n].innerText || '').trim());
      return {
        index: i,
        tr,
        code: text(cols.code),
        op: text(cols.op),
        quantity: parseInt(text(cols.qty), 10) || 1,
        cancelled: (() => {
          const cb = cols.cancel != null && tds[cols.cancel] ? tds[cols.cancel].querySelector('input[type=checkbox]') : null;
          return cb ? cb.checked : null;
        })(),
      };
    });
    return { rows, cols };
  }

  function setValue(el, v) {
    el.value = v;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  }

  /** 元の架空SKU行のキャンセルにチェックを入れる (貼付け後にグリッドが再描画されるので引き直す) */
  function tickCancel(setCode) {
    const { rows, cols } = readRows();
    if (cols.cancel == null) return false;
    const target = rows.find((r) => r.code === setCode);
    if (!target) return false;
    const tds = [...target.tr.querySelectorAll('td')];
    const cb = tds[cols.cancel] ? tds[cols.cancel].querySelector('input[type=checkbox]') : null;
    if (!cb || cb.checked) return !!cb;
    cb.click();
    return true;
  }

  async function apply(setCode, paste, statusEl) {
    const codes = document.querySelector(A.codes);
    const qtys = document.querySelector(A.quantities);
    const btn = document.querySelector(A.paste);
    if (!codes || !qtys || !btn) {
      statusEl.textContent = '⚠ NEの「明細入出力補助」が見つかりません。画面仕様が変わった可能性があります。手作業で入れてください。';
      statusEl.className = 'bf-status bf-err';
      return false;
    }
    setValue(codes, paste.codes);
    setValue(qtys, paste.quantities);
    // ここでNEが「貼り付けを完了しました」のalertを出す。拡張からは閉じられないので人がOKを押す
    btn.click();
    await sleep(900);
    setValue(codes, '');
    setValue(qtys, '');
    const ticked = tickCancel(setCode);
    statusEl.innerHTML = ticked
      ? '✅ ' + paste.rowCount + '行を追加し、元の行をキャンセルにしました。<b>内容を確認して「更新保存」→「再計算しますか?」は必ず【キャンセル】</b>'
      : '✅ ' + paste.rowCount + '行を追加しました。⚠ 元の行のキャンセルは自動で入りませんでした。手で入れてください。';
    statusEl.className = 'bf-status ' + (ticked ? 'bf-ok' : 'bf-err');
    return true;
  }

  function renderPreview(d) {
    const parts = [];
    if (!d.ok) {
      parts.push('<div class="bf-box bf-err"><b>展開できません</b><ul>'
        + d.warnings.map((w) => '<li>' + esc(w) + '</li>').join('')
        + '</ul>選択肢が登録されていない可能性があります。ポータルの「選べるセットの明細展開」で確認するか、手作業で入れてください。</div>');
      return parts.join('');
    }
    if (d.notices && d.notices.length) {
      parts.push('<div class="bf-box bf-warn"><b>確認してください</b><ul>'
        + d.notices.map((n) => '<li>' + esc(n) + '</li>').join('') + '</ul></div>');
    }
    parts.push('<table class="bf-table"><thead><tr><th>枠</th><th>選択肢</th><th>商品コード</th><th>商品名</th><th>数量</th></tr></thead><tbody>'
      + d.lines.map((l) => '<tr><td>' + esc(l.label) + '</td><td>' + esc(l.value) + '</td>'
        + '<td class="bf-mono">' + esc(l.code) + '</td><td>' + esc(l.name) + '</td><td>' + l.quantity + '</td></tr>').join('')
      + '</tbody></table>');
    if (d.omake) {
      parts.push('<div class="bf-omake"><b>おまけ</b> <span class="bf-hint">(在庫は最大24時間前の値です。変えたいときは選び直してください)</span><br>'
        + d.omake.candidates.map((c, i) => '<label class="bf-cand' + (c.chosen ? ' bf-chosen' : '') + '">'
          + '<input type="radio" name="bf-omake" value="' + esc(c.code) + '"' + (c.chosen ? ' checked' : '') + '>'
          + '<span class="bf-mono">' + esc(c.code) + '</span> '
          + esc(c.name || '') + ' <span class="' + (c.available > 0 ? '' : 'bf-zero') + '">利用可能 ' + c.available + '</span>'
          + '</label>').join('')
        + (d.omake.code ? '' : '<div class="bf-box bf-warn">候補が全て在庫切れです。おまけ行は追加されません。</div>')
        + '</div>');
    }
    return parts.join('');
  }

  function buildPanel(targets) {
    const old = document.getElementById(PANEL_ID);
    if (old) old.remove();
    const panel = document.createElement('div');
    panel.id = PANEL_ID;
    panel.innerHTML =
      '<div class="bf-head">🎁 選べるセット展開'
      + '<span class="bf-hint">保存時の「商品計が一致していません。再計算しますか?」は<b>必ず【キャンセル】</b>（OKを押すとセット価格が単品合計に化けます）</span></div>'
      + targets.map((t, i) =>
        '<div class="bf-row" data-i="' + i + '">'
        + '<div class="bf-rowhead"><span class="bf-mono">' + esc(t.code) + '</span>'
        + ' <span class="bf-hint">受注数 ' + t.quantity + '</span>'
        + ' <button class="bf-btn bf-expand">展開する</button>'
        + ' <span class="bf-status"></span></div>'
        + '<div class="bf-preview"></div>'
        + '</div>').join('');

    const container = document.getElementById(GRID_ID);
    container.parentNode.insertBefore(panel, container);

    panel.addEventListener('click', async (ev) => {
      const btn = ev.target.closest('button.bf-expand, button.bf-apply');
      if (!btn) return;
      const rowEl = btn.closest('.bf-row');
      const t = targets[Number(rowEl.dataset.i)];
      const statusEl = rowEl.querySelector('.bf-status');
      const prevEl = rowEl.querySelector('.bf-preview');

      if (btn.classList.contains('bf-expand')) {
        btn.disabled = true;
        statusEl.textContent = '照会中...';
        statusEl.className = 'bf-status';
        const res = await send({ type: 'expand', setCode: t.code, op: t.op, quantity: t.quantity });
        btn.disabled = false;
        if (!res.ok) {
          statusEl.textContent = '⚠ ' + res.error;
          statusEl.className = 'bf-status bf-err';
          return;
        }
        rowEl._data = res.data;
        statusEl.textContent = '';
        prevEl.innerHTML = renderPreview(res.data)
          + (res.data.ok ? '<div class="bf-actions"><button class="bf-btn bf-apply">この内容でNEに入れる</button></div>' : '');
        return;
      }

      if (btn.classList.contains('bf-apply')) {
        const d = rowEl._data;
        if (!d || !d.ok) return;
        // おまけを選び直していたら差し替える
        const picked = prevEl.querySelector('input[name=bf-omake]:checked');
        const codes = d.paste.codes.split('\n');
        const qtys = d.paste.quantities.split('\n');
        if (d.omake && picked) {
          const chosen = picked.value;
          if (d.omake.code && codes[codes.length - 1] === d.omake.code) {
            codes[codes.length - 1] = chosen;
          } else {
            codes.push(chosen);
            qtys.push(String(d.omake.quantity));
          }
        }
        btn.disabled = true;
        await apply(t.code, { codes: codes.join('\n'), quantities: qtys.join('\n'), rowCount: codes.length }, statusEl);
      }
    });
  }

  async function init() {
    if (!/\/Userjyuchu\/jyuchuInp/.test(location.pathname + location.search)) return;
    let table = null;
    for (let i = 0; i < 20 && !table; i++) { table = grid(); if (!table) await sleep(500); }
    if (!table) return;

    const { rows } = readRows();
    if (!rows.length) return;

    const res = await send({ type: 'sets' });
    if (!res.ok) {
      console.warn('[選べるセット展開] セット一覧を取得できません:', res.error);
      return;
    }
    const setCodes = new Set((res.data.setCodes || []).map((c) => String(c).toLowerCase()));
    const targets = rows.filter((r) => r.code && setCodes.has(r.code.toLowerCase()) && r.op && !r.cancelled);
    if (!targets.length) return;
    buildPanel(targets);
  }

  init();
})();
