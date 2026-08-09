/**
 * page-agent.js — ページ本体と同じ世界 (MAIN world) で動く小さな実行係。
 *
 * なぜ要るか (2026-08-09 実機):
 *   拡張の content script (isolated world) から NE の「明細に貼付け」ボタンを click() しても
 *   反応しなかった。一方、8/7 の CDP 検証 (page.evaluate = MAIN world) では同じ操作が通っている。
 *   そこで「値を入れる・クリックする」という操作だけを MAIN world で実行し、
 *   判断・照合・通信は従来どおり isolated 側 (content.js) に残す。
 *
 * 通信は DOM の CustomEvent (detail は JSON文字列)。
 *   content.js → 'bf-ss-exec'   {id, cmd, ...}
 *   ここ       → 'bf-ss-result' {id, ok, error?}
 * dispatchEvent は同期なので、クリックが alert でブロックされても
 * 結果は alert が閉じられた後に必ず返る (タイムアウトで取りこぼさない)。
 *
 * ⚠ ここは「固定の操作」しか実行しない。セレクタや任意コードを外から渡させない
 *   (ページ本体で動く以上、汎用実行器にすると危険なため)。
 */
(() => {
  const SEL = {
    codes: '#meisai_assist_input_syohin_code',
    qtys: '#meisai_assist_input_jyuchu_su',
    paste: '#meisai_assist_input_btn',
    dlg: '#meisaiAssist-Dlg',
    openBtn: '#show-meisai_as-btn',
    openMenu: '#sub_menu_01_07_lnk',
    marked: '[data-bf-ss-target="1"]',
  };

  function setValue(el, v) {
    el.value = v;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  }

  const CMDS = {
    /** 生存確認 (content.js が bridge の有無を確かめる) */
    ping() { return {}; },

    /** 明細入出力補助に商品コード・受注数を入れて「明細に貼付け」を押す */
    fillAndPaste({ codes, qtys }) {
      const c = document.querySelector(SEL.codes);
      const q = document.querySelector(SEL.qtys);
      const b = document.querySelector(SEL.paste);
      if (!c || !q || !b) throw new Error('明細入出力補助の入力欄が見つかりません');
      if (typeof codes !== 'string' || typeof qtys !== 'string') throw new Error('入力が不正です');
      setValue(c, codes);
      setValue(q, qtys);
      b.click(); // NEのalertが出るとここでブロックし、人がOKを押すと戻る
      return {};
    },

    /** 貼付け欄を空に戻す */
    clearInputs() {
      const c = document.querySelector(SEL.codes);
      const q = document.querySelector(SEL.qtys);
      if (c) setValue(c, '');
      if (q) setValue(q, '');
      return {};
    },

    /** 明細入出力補助ダイアログを開く */
    openDialog() {
      const btn = document.querySelector(SEL.openBtn) || document.querySelector(SEL.openMenu);
      if (!btn) throw new Error('明細入出力補助を開くボタンが見つかりません');
      btn.click();
      return {};
    },

    /** ダイアログを閉じる (枠の「閉じる」ボタンを探し、無ければ非表示に戻す) */
    closeDialog() {
      const wrap = document.querySelector('[id^="ne_dlg"][id*="meisaiAssist"]');
      if (wrap) {
        const closer = [...wrap.querySelectorAll('input[type=button], button, a')]
          .find((el) => /閉じる|close|×/i.test(el.value || el.textContent || ''));
        if (closer) { closer.click(); return {}; }
        wrap.style.visibility = 'hidden';
      }
      const d = document.querySelector(SEL.dlg);
      if (d) d.style.visibility = 'hidden';
      return {};
    },

    /** content.js 側が data-bf-ss-target="1" を付けた要素をクリックする (キャンセルのチェック用) */
    clickMarked() {
      const el = document.querySelector(SEL.marked);
      if (!el) throw new Error('対象の要素が見つかりません');
      el.click();
      return {};
    },
  };

  document.addEventListener('bf-ss-exec', (ev) => {
    let req = {};
    try { req = JSON.parse(typeof ev.detail === 'string' ? ev.detail : '{}'); } catch { /* 下で弾く */ }
    const res = { id: req.id || null, ok: true };
    try {
      const fn = CMDS[req.cmd];
      if (!fn) throw new Error(`未知のコマンド: ${String(req.cmd).slice(0, 30)}`);
      Object.assign(res, fn(req) || {});
    } catch (e) {
      res.ok = false;
      res.error = String((e && e.message) || e).slice(0, 300);
    }
    document.dispatchEvent(new CustomEvent('bf-ss-result', { detail: JSON.stringify(res) }));
  });
})();
