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

  /**
   * 展開対象の元行を一意に特定する。
   * 🚨 商品コードだけで探すと、同じセットを2行買われた伝票で別の行をキャンセルしてしまう
   *   (Codexレビュー #6 / 2026-08-08)。商品コード+商品op+受注数で絞り、
   *   それでも一意にならなければ、展開を始めたときの行位置が一致するかで確かめる。
   *   どちらでも決まらないなら手を出さない。
   */
  function findTargetRow(t) {
    const { rows, cols } = readRows();
    const same = rows.filter((r) => r.code === t.code && r.op === t.op && r.quantity === t.quantity);
    if (same.length === 1) return { row: same[0], cols };
    const byIndex = rows[t.index];
    if (byIndex && byIndex.code === t.code && byIndex.op === t.op && byIndex.quantity === t.quantity) {
      return { row: byIndex, cols };
    }
    return { row: null, cols, reason: same.length === 0 ? '見つかりません' : '同じ内容の行が複数あって特定できません' };
  }

  function isCancelled(row, cols) {
    if (cols.cancel == null) return null;
    const tds = [...row.tr.querySelectorAll('td')];
    const cb = tds[cols.cancel] ? tds[cols.cancel].querySelector('input[type=checkbox]') : null;
    return cb ? cb.checked : null;
  }

  /** グリッドの行数が増えるまで待つ (固定sleepだと再描画が遅いときに取りこぼす) */
  async function waitForRows(minCount, timeoutMs = 10000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      if (readRows().rows.length >= minCount) return true;
      await sleep(200);
    }
    return false;
  }

  let applying = false; // ページ単位のロック。共通の貼付け欄を同時に触らせない

  /**
   * 明細を追加して元行をキャンセルする。
   * 🚨 「追加」と「キャンセル」は1つの操作にできないので、やった結果を必ずDOMから確認する。
   *   以前は固定900ms待って成功表示していたため、再描画が遅い・行の取り違え・例外のときに
   *   「成功しました」と出たまま元行が残る可能性があった (Codexレビュー #5 / 2026-08-08)。
   */
  async function apply(t, expected, statusEl) {
    if (applying) {
      statusEl.textContent = '⚠ 別の行を処理中です。終わってからもう一度押してください。';
      statusEl.className = 'bf-status bf-err';
      return false;
    }
    const codes = document.querySelector(A.codes);
    const qtys = document.querySelector(A.quantities);
    const btn = document.querySelector(A.paste);
    if (!codes || !qtys || !btn) {
      statusEl.textContent = '⚠ NEの「明細入出力補助」が見つかりません。画面仕様が変わった可能性があります。手作業で入れてください。';
      statusEl.className = 'bf-status bf-err';
      return false;
    }

    // 照会したときと伝票の中身が変わっていないか、直前にもう一度確かめる
    const pre = findTargetRow(t);
    if (!pre.row) {
      statusEl.textContent = `⚠ 対象の行を特定できません (${pre.reason})。画面を再読込してやり直してください。`;
      statusEl.className = 'bf-status bf-err';
      return false;
    }
    const before = readRows().rows.length;

    applying = true;
    setBusy(true);
    try {
      setValue(codes, expected.codesText);
      setValue(qtys, expected.qtysText);
      // ここでNEが「貼り付けを完了しました」のalertを出す。拡張からは閉じられないので人がOKを押す
      btn.click();

      const grew = await waitForRows(before + expected.rows.length);
      setValue(codes, '');
      setValue(qtys, '');
      if (!grew) {
        return fail(statusEl, '明細行が増えませんでした。貼り付けに失敗している可能性があります。');
      }

      // 追加された行が、こちらが指示した内容と一致しているか照合する
      const after = readRows().rows;
      const missing = [];
      const pool = after.filter((r) => r.code !== t.code);
      for (const want of expected.rows) {
        const i = pool.findIndex((r) => r.code === want.code && r.quantity === want.quantity);
        if (i === -1) missing.push(`${want.code}×${want.quantity}`);
        else pool.splice(i, 1);
      }
      if (missing.length) {
        return fail(statusEl, `追加された明細が指示と一致しません (見つからない: ${missing.join(', ')})。`);
      }

      // 元の架空SKU行をキャンセルにする
      const post = findTargetRow(t);
      if (!post.row) {
        return fail(statusEl, `元の行を特定できません (${post.reason})。元の行のキャンセルを手で入れてください。`);
      }
      if (post.cols.cancel == null) {
        return fail(statusEl, 'キャンセル列が表示されていません。元の行のキャンセルを手で入れてください。');
      }
      if (!isCancelled(post.row, post.cols)) {
        const tds = [...post.row.tr.querySelectorAll('td')];
        const cb = tds[post.cols.cancel]?.querySelector('input[type=checkbox]');
        if (!cb) return fail(statusEl, 'キャンセルのチェックボックスが見つかりません。手で入れてください。');
        cb.click();
        await sleep(400);
      }
      const verify = findTargetRow(t);
      if (!verify.row || isCancelled(verify.row, verify.cols) !== true) {
        return fail(statusEl, '元の行をキャンセルにできませんでした。手で入れてください。');
      }

      statusEl.innerHTML = '✅ ' + expected.rows.length + '行を追加し、元の行をキャンセルにしました。'
        + '<b>内容を確認して「更新保存」→「再計算しますか?」は必ず【キャンセル】</b>';
      statusEl.className = 'bf-status bf-ok';
      return true;
    } catch (e) {
      return fail(statusEl, '処理中にエラーが起きました: ' + (e && e.message ? e.message : String(e)));
    } finally {
      applying = false;
      setBusy(false);
    }
  }

  /** 途中で失敗したときは「保存しないでください」を強く出す (中途半端な状態で保存されるのが一番まずい) */
  function fail(statusEl, msg) {
    statusEl.innerHTML = '⚠ ' + esc(msg)
      + '<br><b>この伝票はまだ保存しないでください。</b>画面を再読込して状態を確認してから、手作業で直してください。';
    statusEl.className = 'bf-status bf-err';
    return false;
  }

  /** 処理中は全部のボタンを止める (共通の貼付け欄を別の行が上書きしないように) */
  function setBusy(busy) {
    const panel = document.getElementById(PANEL_ID);
    if (!panel) return;
    for (const b of panel.querySelectorAll('button')) b.disabled = busy;
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
        const expected = {
          rows: codes.map((c, i) => ({ code: c, quantity: parseInt(qtys[i], 10) || 0 })),
          codesText: codes.join('\n'),
          qtysText: qtys.join('\n'),
        };
        const okApplied = await apply(t, expected, statusEl);
        if (okApplied) btn.disabled = true; // 成功したときだけ二度押しを止める
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
