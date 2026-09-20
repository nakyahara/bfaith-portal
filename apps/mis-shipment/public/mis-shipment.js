/**
 * 誤出荷管理アプリ フロント JS
 * UI モック: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_UI_mock_v7.3.md
 *
 * グローバル: window.misShipment.{initIndexPage, initNewPage, initDetailPage}
 *
 * 画面構成 (2026-09-20 UI 刷新):
 *   - 一覧: 検索 + 状態チップ + 期間プリセット。行のどこを押しても詳細へ
 *   - 新規登録: ステップ形式 (1画面1目的。設計書アクセシビリティ方針)
 *   - 詳細: パネル分割 + 状態レール
 * 知らせは alert() ではなく画面内トーストで出す (ブラウザの alert は
 * 連打すると操作を止めてしまい、現場で「固まった」と誤解されるため)。
 */
(function () {
  'use strict';

  const API_BASE = '/apps/mis-shipment/api';

  // ─── UUID v4 ────────────────────────────────────────
  function uuidv4() {
    if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
    // RFC 4122 v4 (フォールバック)
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = (Math.random() * 16) | 0;
      const v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  // ─── HTTP helper ────────────────────────────────────
  async function apiFetch(path, opts = {}) {
    const headers = { 'Accept': 'application/json', ...(opts.headers || {}) };
    if (opts.body && typeof opts.body === 'object' && !(opts.body instanceof FormData)) {
      headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(opts.body);
    }
    const res = await fetch(API_BASE + path, { ...opts, headers, credentials: 'same-origin' });
    let data = null;
    try { data = await res.json(); } catch (_) { /* no body */ }
    return { ok: res.ok, status: res.status, data };
  }

  // ─── escape HTML ────────────────────────────────────
  function esc(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[c]));
  }

  function yen(n) {
    return '¥' + Number(n || 0).toLocaleString('ja-JP');
  }

  // ─── トースト (alert の置き換え) ─────────────────────
  function toast(message, kind = 'info', timeoutMs = 6000) {
    const host = document.getElementById('mis-toast-host');
    if (!host) { window.alert(message); return; }   // ヘッダーが無い画面での保険
    const mark = kind === 'ok' ? '✅' : (kind === 'error' ? '⚠️' : 'ℹ️');
    const el = document.createElement('div');
    el.className = 'mis-toast is-' + kind;
    el.innerHTML = `<span class="mis-toast-mark" aria-hidden="true">${mark}</span>`
      + `<span class="mis-toast-text"></span>`
      + `<button type="button" class="mis-toast-close">閉じる</button>`;
    el.querySelector('.mis-toast-text').textContent = message;
    el.querySelector('.mis-toast-close').addEventListener('click', () => el.remove());
    host.appendChild(el);
    // エラーは自動で消さない (見逃すと原因が分からなくなるため)
    if (kind !== 'error' && timeoutMs > 0) setTimeout(() => el.remove(), timeoutMs);
  }

  // ─── JST 日付 ───────────────────────────────────────
  const JST_FMT = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit' });
  function jstToday(offsetDays = 0) {
    const d = new Date(Date.now() + offsetDays * 86400000);
    return JST_FMT.format(d);
  }

  // ─── enum 表示マップ ───────────────────────────────
  const MALL_LABEL = { amazon: 'Amazon', amazon_fbm: 'Amazon (FBM)', rakuten: '楽天', yahoo: 'Yahoo', linegift: 'LINEギフト', mercari: 'メルカリ', aupay: 'auPAY', qoo10: 'Qoo10', other: 'その他', other_mall: 'その他' };
  const MIS_TYPE_LABEL = { wrong_item: '別商品', wrong_qty: '数量違い', damage: '破損', missing: '欠品・未着', wrong_address: '宛先違い', mix_up: 'テレコ', other: 'その他' };
  const MIS_TYPE_ICON = { wrong_item: '🔀', wrong_qty: '🔢', damage: '💥', missing: '📭', wrong_address: '🏠', mix_up: '⚭', other: '✏️' };
  const STAGE_LABEL = { picking: 'ピッキング', packing: '梱包', labeling: 'ラベル貼付', inspection: '検品', handover: '出荷引渡', receiving: '入庫', supplier: '仕入先', master_data: 'マスタ', system: 'システム', other: 'その他', unknown: '不明' };
  const STATUS_LABEL = { reported: '報告済', investigating: '調査中', resolved: '完了', closed: 'クローズ' };
  // 色だけに頼らないための記号 (設計書アクセシビリティ方針)
  const STATUS_MARK = { reported: '●', investigating: '◐', resolved: '✔', closed: '■' };

  const FIELD_LABEL = {
    mis_type: '誤出荷種別', process_stage: '発見工程', root_cause_stage: '根本原因',
    root_cause_note: '原因詳細', reporter_note: '現場のメモ', field_review: '確認',
  };
  // 訂正できる選択肢 (テレコ mix_up は含めない。相方とグループで対になっていて変えられない)
  const MIS_TYPE_OPTIONS = ['wrong_item', 'wrong_qty', 'damage', 'missing', 'wrong_address', 'other'];
  const PROCESS_STAGE_OPTIONS = ['picking', 'packing', 'labeling', 'inspection', 'handover', 'unknown'];

  /** 訂正履歴に出す値を日本語にする。'wrong_item / picking' のような組も訳す。 */
  function fieldValueLabel(field, v) {
    if (v == null || v === '') return '(空)';
    const one = (f, x) => {
      if (f === 'mis_type') return MIS_TYPE_LABEL[x] || x;
      if (f === 'process_stage' || f === 'root_cause_stage') return STAGE_LABEL[x] || x;
      return x;
    };
    if (field === 'field_review') {
      // old/new は「mis_type / process_stage」の組
      const parts = String(v).split(' / ');
      if (parts.length === 2) return one('mis_type', parts[0]) + ' / ' + one('process_stage', parts[1]);
      return String(v);
    }
    return one(field, String(v));
  }

  function statusPill(status) {
    const label = STATUS_LABEL[status] || status || '-';
    const mark = STATUS_MARK[status] || '•';
    return `<span class="mis-pill st-${esc(status)}"><span class="mis-pill-mark" aria-hidden="true">${mark}</span>${esc(label)}</span>`;
  }

  function misTypeTag(misType) {
    const label = MIS_TYPE_LABEL[misType] || misType || '-';
    const icon = MIS_TYPE_ICON[misType] || '•';
    return `<span class="mis-tag ${misType === 'mix_up' ? 'is-mixup' : ''}"><span aria-hidden="true">${icon}</span>${esc(label)}</span>`;
  }

  // ====================================================================
  // INDEX PAGE
  // ====================================================================
  function initIndexPage() {
    const form = document.getElementById('filter-form');

    form.addEventListener('submit', (e) => { e.preventDefault(); loadList(); });

    // 状態チップ (単一選択)
    const statusChips = document.getElementById('status-chips');
    const statusInput = document.getElementById('filter-status');
    statusChips.addEventListener('click', (e) => {
      const btn = e.target.closest('.mis-chip');
      if (!btn) return;
      statusChips.querySelectorAll('.mis-chip').forEach((c) => c.setAttribute('aria-pressed', String(c === btn)));
      statusInput.value = btn.dataset.status || '';
      loadList();
    });

    // 期間プリセット (from/to を埋める。カレンダーを直接触ったら「全期間」の押下を外す)
    const rangeChips = document.getElementById('range-chips');
    const fromInput = form.querySelector('[name="from"]');
    const toInput = form.querySelector('[name="to"]');
    rangeChips.addEventListener('click', (e) => {
      const btn = e.target.closest('.mis-chip');
      if (!btn) return;
      rangeChips.querySelectorAll('.mis-chip').forEach((c) => c.setAttribute('aria-pressed', String(c === btn)));
      const r = applyRangePreset(btn.dataset.range);
      fromInput.value = r.from;
      toInput.value = r.to;
      loadList();
    });
    [fromInput, toInput].forEach((el) => el.addEventListener('change', () => {
      rangeChips.querySelectorAll('.mis-chip').forEach((c) => c.setAttribute('aria-pressed', 'false'));
      loadList();
    }));

    form.querySelector('[name="mall"]').addEventListener('change', () => loadList());

    // 検索は打ち終わりを待ってから (打鍵ごとに投げない)
    let searchTimer = null;
    form.querySelector('[name="q"]').addEventListener('input', () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(loadList, 350);
    });

    const reviewInput = document.getElementById('filter-needs-review');
    document.getElementById('field-review-toggle').addEventListener('click', () => {
      reviewInput.value = reviewInput.value === '1' ? '' : '1';
      loadList();
    });

    document.getElementById('filter-reset').addEventListener('click', () => {
      form.reset();
      statusInput.value = '';
      reviewInput.value = '';
      statusChips.querySelectorAll('.mis-chip').forEach((c, i) => c.setAttribute('aria-pressed', String(i === 0)));
      rangeChips.querySelectorAll('.mis-chip').forEach((c, i) => c.setAttribute('aria-pressed', String(i === 0)));
      loadList();
    });

    // 行はどこを押しても詳細へ (リンクを押したときは二重遷移させない)
    const body = document.getElementById('results-body');
    body.addEventListener('click', (e) => {
      if (e.target.closest('a')) return;
      const tr = e.target.closest('tr.mis-row');
      if (tr && tr.dataset.href) window.location.href = tr.dataset.href;
    });

    // 登録直後の戻り: 一覧側で知らせる (登録画面で出すとすぐ遷移して読めない)
    const params = new URLSearchParams(window.location.search);
    if (params.get('created')) {
      toast(params.get('created') === 'dup' ? '同じ内容が既に登録されていました (再送扱い)' : '登録しました', 'ok');
      params.delete('created');
      const qs = params.toString();
      window.history.replaceState({}, '', window.location.pathname + (qs ? '?' + qs : ''));
    }

    loadStrip();
    loadList();
  }

  /** 期間プリセット -> { from, to } (JST 基準)。'all' は空文字で絞り込みなし。 */
  function applyRangePreset(range) {
    const today = jstToday();
    if (range === 'today') return { from: today, to: today };
    if (range === '7d') return { from: jstToday(-6), to: today };
    if (range === 'month') {
      return { from: today.slice(0, 8) + '01', to: today };
    }
    if (range === 'prev-month') {
      const [y, m] = today.split('-').map(Number);
      const py = m === 1 ? y - 1 : y;
      const pm = m === 1 ? 12 : m - 1;
      const pad = (n) => String(n).padStart(2, '0');
      // 前月末日 = 当月 1 日の前日。UTC 演算だけで出す (ローカル TZ に依存させない)
      const lastDay = new Date(Date.UTC(y, m - 1, 1) - 86400000).getUTCDate();
      return { from: `${py}-${pad(pm)}-01`, to: `${py}-${pad(pm)}-${pad(lastDay)}` };
    }
    return { from: '', to: '' };
  }

  /** 一覧上部の「今月の状況」。取れなければ黙って隠す (一覧は出す)。 */
  async function loadStrip() {
    const strip = document.getElementById('mis-strip');
    if (!strip) return;
    let result;
    try {
      result = await apiFetch('/summary?period=month');
    } catch (e) {
      return;   // 一覧は出す。ここが出ないだけ
    }
    if (!result.ok || !result.data || !result.data.current) return;
    const cur = result.data.current;
    const target = result.data.industry_target_pct;
    const over = cur.incident_rate_pct != null && target != null && cur.incident_rate_pct > target;
    const rate = cur.incident_rate_pct != null ? cur.incident_rate_pct.toFixed(2) + '%' : '-';
    strip.innerHTML = `
      <div class="mis-strip-card">
        <div class="mis-strip-label">今月の誤出荷</div>
        <div class="mis-strip-value">${Number(cur.incidents || 0).toLocaleString('ja-JP')} <span style="font-size:15px">件</span></div>
        <div class="mis-strip-note">${esc(result.data.period ? result.data.period.label : '')}</div>
      </div>
      <div class="mis-strip-card">
        <div class="mis-strip-label">今月の損失額</div>
        <div class="mis-strip-value">${yen(cur.total_loss_jpy)}</div>
        <div class="mis-strip-note">千ライン当り ${yen(cur.loss_per_1000_lines_jpy)}</div>
      </div>
      <div class="mis-strip-card ${over ? 'is-warn' : ''}">
        <div class="mis-strip-label">誤出荷件数率</div>
        <div class="mis-strip-value">${rate}</div>
        <div class="mis-strip-note">業界目標 ≤ ${target != null ? target.toFixed(2) + '%' : '-'} ${over ? '⚠️ オーバー' : '✅ 範囲内'}</div>
      </div>
    `;
    strip.hidden = false;
  }

  /** 種別・工程が当てにならない行に付ける印 (色だけに頼らない)。 */
  function needsReviewMark(r) {
    return r.needs_field_review
      ? ' <span class="mis-needs-review-mark" title="2026-09-20 の修正より前に登録された記録です。この値は当てになりません">⚠️要確認</span>'
      : '';
  }

  /** 「要確認が N 件あります」の知らせ。数えられなかったときは 0 と混ぜずにそう書く。 */
  function renderFieldReviewNotice(total) {
    const notice = document.getElementById('field-review-notice');
    const countEl = document.getElementById('field-review-count');
    const toggle = document.getElementById('field-review-toggle');
    if (!notice || !countEl || !toggle) return;
    const filtering = document.getElementById('filter-needs-review').value === '1';
    if (total === 0 && !filtering) { notice.hidden = true; return; }
    countEl.textContent = total == null ? '(件数を数えられませんでした)' : total.toLocaleString('ja-JP') + ' 件';
    toggle.textContent = filtering ? '全部表示にもどす' : '要確認だけ表示';
    toggle.setAttribute('aria-pressed', String(filtering));
    notice.hidden = false;
  }

  // 打鍵ごと・チップごとにリクエストが飛ぶので、遅れて返ってきた古い条件の
  // 結果が新しい結果を上書きしないように通し番号で見張る。
  let listSeq = 0;

  async function loadList() {
    const mySeq = ++listSeq;
    const form = document.getElementById('filter-form');
    const params = new URLSearchParams();
    new FormData(form).forEach((v, k) => { if (v) params.set(k, String(v).trim()); });
    const body = document.getElementById('results-body');
    const COLSPAN = 10;
    body.innerHTML = `<tr><td colspan="${COLSPAN}" class="loading">読み込み中...</td></tr>`;

    const summaryEl = document.getElementById('results-summary');
    let result;
    try {
      result = await apiFetch('/submissions?' + params.toString());
    } catch (e) {
      if (mySeq !== listSeq) return;
      body.innerHTML = `<tr><td colspan="${COLSPAN}" class="empty">通信に失敗しました。もう一度お試しください。</td></tr>`;
      summaryEl.textContent = '';
      return;
    }
    if (mySeq !== listSeq) return;   // もっと新しい条件の結果が既に出ている
    if (!result.ok) {
      body.innerHTML = `<tr><td colspan="${COLSPAN}" class="empty">読み込みエラー (${result.status})</td></tr>`;
      summaryEl.textContent = '';
      return;
    }
    const rows = result.data.rows || [];
    renderFieldReviewNotice(result.data.needs_field_review_total);
    if (rows.length === 0) {
      body.innerHTML = `<tr><td colspan="${COLSPAN}" class="empty">条件に合う誤出荷はありません</td></tr>`;
      summaryEl.textContent = '';
      return;
    }
    body.innerHTML = rows.map((r) => {
      const href = `/apps/mis-shipment/detail/${r.id}`;
      const sku = r.sku_snapshot ? esc(r.sku_snapshot) : '-';
      const name = r.product_name_snapshot ? `<div class="hint">${esc(r.product_name_snapshot)}</div>` : '';
      return `
      <tr class="mis-row ${r.mis_type === 'mix_up' ? 'row-mix-up' : ''}" data-href="${href}">
        <td>${esc(r.occurred_on)}</td>
        <td>${esc(MALL_LABEL[r.mall] || r.mall || '不明')}</td>
        <td class="mis-cell-order">${r.order_id_unknown ? '<em>不明</em>' : esc(r.mall_order_id)}</td>
        <td class="mis-cell-item">${sku}${name}</td>
        <td>${misTypeTag(r.mis_type)}${needsReviewMark(r)}</td>
        <td>${esc(STAGE_LABEL[r.process_stage] || '-')}${needsReviewMark(r)}</td>
        <td>${esc(STAGE_LABEL[r.root_cause_stage] || '-')}</td>
        <td>${statusPill(r.status)}</td>
        <td class="num mis-cell-loss">${yen(r.loss_amount_jpy)}</td>
        <td><a class="mis-row-open mis-row-link" href="${href}" aria-label="誤出荷 #${r.id} の詳細を開く">詳細 ›</a></td>
      </tr>`;
    }).join('');

    const totalLoss = rows.reduce((a, r) => a + Number(r.loss_amount_jpy || 0), 0);
    summaryEl.textContent = `${rows.length} 件表示 / 損失額の合計 ${yen(totalLoss)}`
      + (rows.length >= 100 ? '  ※ 表示は 100 件までです。期間や状態で絞り込んでください' : '');
  }

  // ====================================================================
  // NEW PAGE (ステップ形式)
  // ====================================================================
  const wizard = {
    mixUp: false,
    index: 0,
    panels: [],
    stepButtons: [],
    maxVisited: 0,
    // 各注文欄について「いま入っている番号で lookup 済みか」を覚える
    lookupState: {},   // key -> { value, status: 'pending'|'found'|'missing', data? }
    // lookup は貼り付け・blur・ボタンから同時に走りうる。古い応答を捨てるための通し番号
    lookupSeq: {},     // key -> number
    // 走っている最中の lookup。「次へ」はこれを待ってから判定する
    lookupInFlight: {},// key -> Promise
    // client_submission_id は「送れたか分からない」再送で使い回す (サーバ側の冪等キー)。
    // サーバが明確に拒否したときだけ作り直す。
    submissionIds: {}, // key -> uuid
  };

  /** 注文欄ごとの冪等キー。同じ内容の送り直しでは同じ ID を使う。 */
  function submissionIdFor(sideId) {
    const key = sideId || 'single';
    if (!wizard.submissionIds[key]) wizard.submissionIds[key] = uuidv4();
    return wizard.submissionIds[key];
  }

  // 冪等キーはこの画面を開いている間ずっと同じものを使う。作り直さない。
  // 「今回の送信が拒否された」ことは「前の送信が登録されていない」証明にはならないため
  // (通信が切れた 1 回目が実は登録できていて、2 回目が 503 で返る、が起こりうる)。
  // 内容を直して送り直す場合も、同じ ID の行が無ければ DB は普通に INSERT する。

  /**
   * 数値入力欄の値。type=number は "1.9" も "1e3" も受け付けるので、
   * parseInt で読むと画面の見た目 (1.9 個 / 1e3 円) と送る値 (1 個 / 1 円) がずれる。
   * valueAsNumber で読んで、整数かどうかは呼び出し側で判定する。
   */
  function numFieldValue(el) {
    if (!el) return null;
    if ((el.value || '').trim() === '') return null;
    const n = el.valueAsNumber;
    return Number.isFinite(n) ? n : null;
  }

  function initNewPage(mixUp) {
    const form = document.getElementById('submission-form');
    wizard.mixUp = !!mixUp;
    wizard.panels = Array.from(form.querySelectorAll('.mis-step-panel'));
    wizard.stepButtons = Array.from(document.querySelectorAll('#mis-steps .mis-step'));

    // 発生日の既定は JST 今日 (最終確定はサーバ側)
    form.querySelectorAll('[name="occurred_on"]').forEach((el) => { el.value = jstToday(); });

    // 発生日のプリセット (今日 / 昨日)
    form.querySelectorAll('[data-date-preset]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const input = btn.closest('.form-row').querySelector('[name="occurred_on"]');
        input.value = btn.dataset.datePreset === 'yesterday' ? jstToday(-1) : jstToday();
      });
    });

    // 数量の ± ボタン
    form.querySelectorAll('[data-step-target]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const input = form.querySelector(`[name="${btn.dataset.stepTarget}"]`);
        if (!input) return;
        const min = Number(input.min || 1);
        const max = Number(input.max || 1000);
        const cur = numFieldValue(input);
        const base = Number.isFinite(cur) ? Math.round(cur) : min;
        const next = Math.min(max, Math.max(min, base + Number(btn.dataset.delta)));
        input.value = String(next);
      });
    });

    // 損失額の読み上げ (桁を間違えていないか目で確かめられるように)
    form.querySelectorAll('[data-amount-echo]').forEach((echo) => {
      const input = form.querySelector(`[name="${echo.dataset.amountEcho}"]`);
      if (!input) return;
      const update = () => {
        const v = numFieldValue(input);
        echo.textContent = Number.isInteger(v) && v > 0 ? '= ' + yen(v) : '';
      };
      input.addEventListener('input', update);
      update();
    });

    // 注文番号の検索
    form.querySelectorAll('.btn-lookup').forEach((btn) => {
      btn.addEventListener('click', () => handleLookup(btn.dataset.side));
    });
    form.querySelectorAll('.order-id-input').forEach((input) => {
      // Enter で検索 (form の submit は起こさない)
      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); handleLookup(input.dataset.side); }
      });
      // 貼り付けたらそのまま検索 (現場は番号をコピーして持ってくる)
      input.addEventListener('paste', () => setTimeout(() => handleLookup(input.dataset.side), 0));
      input.addEventListener('blur', () => {
        const key = input.dataset.side || 'single';
        const v = (input.value || '').trim();
        if (v && wizard.lookupState[key]?.value !== v) handleLookup(input.dataset.side);
      });
    });

    // 「注文番号が分からない」で入力欄を止める
    form.querySelectorAll('.order-unknown-check').forEach((chk) => {
      chk.addEventListener('change', () => {
        const sfx = chk.dataset.side ? '_' + chk.dataset.side : '';
        const input = form.querySelector(`[name="mall_order_id${sfx}"]`);
        const btn = form.querySelector(`.btn-lookup[data-side="${chk.dataset.side}"]`);
        const enabled = !chk.checked;
        input.disabled = !enabled;
        btn.disabled = !enabled;
        if (!enabled) {
          input.value = '';
          hideLookupBoxes(chk.dataset.side);
          const k = chk.dataset.side || 'single';
          delete wizard.lookupState[k];
          // 走っている検索の応答を捨てる (チェック後に古い結果が出てこないように)
          wizard.lookupSeq[k] = (wizard.lookupSeq[k] || 0) + 1;
        }
      });
    });

    // lookup ノヒット時の「モールを手で選んで進む」
    form.querySelectorAll('.toggle-manual-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const errBox = document.getElementById('lookup-error-' + (btn.dataset.side || 'single'));
        errBox.querySelector('.manual-mall-row').hidden = false;
        btn.hidden = true;
        errBox.querySelector('.manual-mall-select').focus();
      });
    });

    // ステップ移動
    document.getElementById('step-next').addEventListener('click', () => goNext());
    document.getElementById('step-prev').addEventListener('click', () => goTo(wizard.index - 1));
    wizard.stepButtons.forEach((btn, i) => {
      btn.addEventListener('click', () => {
        // 通ったことのあるステップにだけ戻れる (飛ばして先へは行かせない)
        if (i <= wizard.maxVisited) goTo(i, { validate: i > wizard.index });
      });
    });

    // submit (確認ステップの「登録する」)
    form.addEventListener('submit', (e) => {
      e.preventDefault();
      openConfirmDialog();
    });

    document.getElementById('confirm-cancel').addEventListener('click', () => {
      document.getElementById('confirm-dialog').close();
    });
    document.getElementById('confirm-submit').addEventListener('click', () => doSubmit());

    goTo(0);
  }

  function clearErrors() {
    const box = document.getElementById('step-errors');
    box.hidden = true;
    box.querySelector('ul').innerHTML = '';
  }

  function showErrors(messages) {
    const box = document.getElementById('step-errors');
    box.querySelector('ul').innerHTML = messages.map((m) => `<li>${esc(m)}</li>`).join('');
    box.hidden = false;
    box.focus();
    box.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }

  function goTo(index, opts = {}) {
    if (opts.validate) {
      const errs = validateStep(wizard.index);
      if (errs.length) { showErrors(errs); return; }
    }
    clearErrors();
    wizard.index = index;
    wizard.maxVisited = Math.max(wizard.maxVisited, index);

    wizard.panels.forEach((p) => { p.hidden = Number(p.dataset.step) !== index; });
    wizard.stepButtons.forEach((b, i) => {
      b.classList.toggle('is-current', i === index);
      b.classList.toggle('is-done', i < index || (i <= wizard.maxVisited && i !== index));
      b.setAttribute('aria-current', i === index ? 'step' : 'false');
    });

    const isLast = index === wizard.panels.length - 1;
    document.getElementById('step-prev').hidden = index === 0;
    document.getElementById('step-next').hidden = isLast;
    document.getElementById('step-submit').hidden = !isLast;
    document.getElementById('step-count').textContent = `ステップ ${index + 1} / ${wizard.panels.length}`;

    if (isLast) renderReview();

    // 次のステップの先頭が見えるところまで戻す
    window.scrollTo({ top: 0, behavior: 'smooth' });
    const panel = wizard.panels[index];
    const first = panel && panel.querySelector('input:not([type="radio"]):not([disabled]), select, textarea');
    if (first) setTimeout(() => first.focus({ preventScroll: true }), 120);
  }

  // 検索待ちの間に「次へ」を連打されると goNext が重なり、待っている間に
  // 進んだステップの先へさらに進んでしまう。1 回に絞る。
  let navBusy = false;

  async function goNext() {
    if (navBusy) return;
    navBusy = true;
    const nextBtn = document.getElementById('step-next');
    nextBtn.disabled = true;
    const startIndex = wizard.index;
    try {
      // 注文ステップは、検索が終わってから判定する。
      // 未検索なら引く / 走っている最中ならその結果を待つ。
      const form = document.getElementById('submission-form');
      for (const side of orderSidesOfStep(startIndex)) {
        const key = side || 'single';
        const sfx = side ? '_' + side : '';
        const unknown = form.querySelector(`[name="order_id_unknown${sfx}"]`);
        if (unknown && unknown.checked) continue;
        const v = (form.querySelector(`[name="mall_order_id${sfx}"]`)?.value || '').trim();
        if (!v) continue;                       // 未入力は validateStep が弾く
        const st = wizard.lookupState[key];
        if (!st || st.value !== v) await handleLookup(side);
        else if (wizard.lookupInFlight[key]) await wizard.lookupInFlight[key];
      }
      // 待っている間に人が別のステップへ動いていたら、ここでは何もしない
      if (wizard.index !== startIndex) return;
      const errs = validateStep(startIndex);
      if (errs.length) { showErrors(errs); return; }
      goTo(startIndex + 1);
    } finally {
      navBusy = false;
      nextBtn.disabled = false;
    }
  }

  /** ステップ番号 -> そのステップに含まれる注文欄の side 一覧。 */
  function orderSidesOfStep(stepIndex) {
    if (wizard.mixUp) {
      if (stepIndex === 0) return ['a'];
      if (stepIndex === 1) return ['b'];
      return [];
    }
    return stepIndex === 0 ? [''] : [];
  }

  function validateStep(stepIndex) {
    const form = document.getElementById('submission-form');
    const errs = [];
    const sides = orderSidesOfStep(stepIndex);

    for (const side of sides) {
      const sfx = side ? '_' + side : '';
      const key = side || 'single';
      const name = side ? `注文 ${side.toUpperCase()}` : '注文';
      const unknown = form.querySelector(`[name="order_id_unknown${sfx}"]`)?.checked;
      const orderId = (form.querySelector(`[name="mall_order_id${sfx}"]`)?.value || '').trim();
      if (!unknown && !orderId) {
        errs.push(`${name}: 注文番号を入れるか、「注文番号が分からない」にチェックしてください`);
      }
      if (!unknown && orderId) {
        const st = wizard.lookupState[key];
        if (!st || st.value !== orderId || st.status === 'pending') {
          // 検索が終わっていない (まだ / 走っている最中 / 失敗した)。
          // 登録時にサーバが必ず引き直すので、ここで止めて先に検索させる
          errs.push(`${name}: 注文番号の検索がまだ終わっていません。「🔍 検索」を押してください`);
        } else if (st.status === 'missing' && !manualMallValue(sfx)) {
          // 検索して見つからなかったときは、モールを手で選ばないと登録できない (サーバが 400 を返す)
          errs.push(`${name}: 注文がマスターに見つかりません。「📝 モールを手で選んで進む」でモールを選んでください`);
        }
      }
      // テレコは数量・損失が注文ごと
      if (wizard.mixUp) errs.push(...validateQtyLoss(sfx, name));
    }

    // 種別・工程
    const panel = wizard.panels[stepIndex];
    if (panel && panel.querySelector('[name="mis_type"]') && !form.querySelector('[name="mis_type"]:checked')) {
      errs.push('誤出荷種別を選んでください');
    }
    if (panel && panel.querySelector('[name="process_stage"]') && !form.querySelector('[name="process_stage"]:checked')) {
      errs.push('発見工程を選んでください');
    }

    // 発生日
    if (panel && panel.querySelector('[name="occurred_on"]')) {
      const v = panel.querySelector('[name="occurred_on"]').value;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(v)) errs.push('発生日を入れてください');
      else if (v > jstToday()) errs.push('発生日に未来の日付は入れられません');
    }

    // 単独登録の数量・損失
    if (!wizard.mixUp && panel && panel.querySelector('[name="qty_affected"]')) {
      errs.push(...validateQtyLoss('', ''));
    }
    return errs;
  }

  function validateQtyLoss(sfx, name) {
    const form = document.getElementById('submission-form');
    const prefix = name ? name + ': ' : '';
    const errs = [];
    const qtyEl = form.querySelector(`[name="qty_affected${sfx}"]`);
    const lossEl = form.querySelector(`[name="loss_amount_jpy${sfx}"]`);
    if (!qtyEl || !lossEl) return errs;
    const qty = numFieldValue(qtyEl);
    if (!Number.isInteger(qty) || qty < 1 || qty > 1000) errs.push(`${prefix}影響数量は 1〜1000 の整数 (個) で入れてください`);
    const loss = numFieldValue(lossEl);
    if (!Number.isInteger(loss) || loss < 0 || loss > 10000000) errs.push(`${prefix}損失額は 0〜10,000,000 の整数 (円) で入れてください`);
    return errs;
  }

  const LOOKUP_BTN_LABEL = '🔍 検索';

  /**
   * 注文番号の検索。呼び出し口 (ボタン / Enter / 貼り付け / blur / 「次へ」) が複数あるので、
   * 走っている Promise を欄ごとに覚えておき、「次へ」はそれを待てるようにする。
   */
  function handleLookup(sideId) {
    const key = sideId || 'single';
    const p = runLookup(sideId).finally(() => {
      if (wizard.lookupInFlight[key] === p) delete wizard.lookupInFlight[key];
    });
    wizard.lookupInFlight[key] = p;
    return p;
  }

  async function runLookup(sideId) {
    const sfx = sideId ? '_' + sideId : '';
    const key = sideId || 'single';
    const input = document.querySelector(`[name="mall_order_id${sfx}"]`);
    const btn = document.querySelector(`.btn-lookup[data-side="${sideId}"]`);
    const orderId = (input.value || '').trim();
    hideLookupBoxes(sideId);
    if (!orderId) {
      toast('注文番号を入れてから検索してください', 'error');
      return;
    }

    // 貼り付け・blur・ボタンで同時に走りうる。返ってきたときに
    // 「まだこの検索が最新か」「欄の中身が変わっていないか」を確かめる。
    const seq = (wizard.lookupSeq[key] = (wizard.lookupSeq[key] || 0) + 1);
    // 引き直している間は pending。結果が出るまで次のステップへ進ませない
    wizard.lookupState[key] = { value: orderId, status: 'pending' };
    const okBox = document.getElementById('lookup-result-' + key);
    const errBox = document.getElementById('lookup-error-' + key);

    let result;
    if (btn) { btn.disabled = true; btn.textContent = '検索中…'; }
    try {
      result = await apiFetch('/orders/lookup?order_id=' + encodeURIComponent(orderId));
    } catch (e) {
      result = { ok: false, status: 0, data: null };
    } finally {
      // ボタンの見た目を戻すのも最新の回だけ (古い回が「検索中…」を消さないように)
      if (btn && seq === wizard.lookupSeq[key]) {
        btn.textContent = LOOKUP_BTN_LABEL;
        // 検索中に「注文番号が分からない」にチェックされていたら、止めたままにする
        const unknownChk = document.querySelector(`[name="order_id_unknown${sfx}"]`);
        btn.disabled = !!(unknownChk && unknownChk.checked);
      }
    }

    // 古い応答 / 欄が書き換わった後の応答は捨てる (別の注文の商品を出さない)
    if (seq !== wizard.lookupSeq[key]) return;
    if ((input.value || '').trim() !== orderId) return;

    if (!result.ok) {
      // 通信・権限エラーは「見つからなかった」とは別物。
      // 「検索済み」として覚えない = 次に進むときにもう一度引き直す。
      delete wizard.lookupState[key];
      errBox.hidden = false;
      errBox.querySelector('[data-role="lookup-error-message"]').textContent =
        '⚠️ 検索できませんでした (' + (result.data?.error || result.status || '通信エラー') + ')。少し待ってもう一度「🔍 検索」を押してください。';
      // 見つからなかった場合の逃げ道 (手動モール) はここでは出さない
      errBox.querySelector('.lookup-options').hidden = true;
      errBox.querySelector('.toggle-manual-btn').hidden = true;
      return;
    }
    errBox.querySelector('.lookup-options').hidden = false;
    if (!result.data.found) {
      wizard.lookupState[key] = { value: orderId, status: 'missing' };
      errBox.hidden = false;
      errBox.querySelector('[data-role="lookup-error-message"]').textContent =
        '⚠️ 注文がマスターに見つかりませんでした。次のどれかを選んでください:';
      return;
    }

    wizard.lookupState[key] = { value: orderId, status: 'found', data: result.data };
    okBox.hidden = false;
    okBox.querySelector('[data-field="mall"]').textContent = MALL_LABEL[result.data.mall] || result.data.mall || '-';
    okBox.querySelector('[data-field="product_name"]').textContent = result.data.product_name || '-';
    okBox.querySelector('[data-field="sku"]').textContent = result.data.sku || '-';
    okBox.querySelector('[data-field="order_date"]').textContent = result.data.order_date || '-';
    okBox.querySelector('[data-field="ordered_qty"]').textContent = (result.data.ordered_qty != null ? result.data.ordered_qty + ' 個' : '-');
    // mall_order_id / slip_no を表示 (matched_by でどちらでヒットしたかを示す)
    const mallOrderEl = okBox.querySelector('[data-field="mall_order_id"]');
    if (mallOrderEl) {
      mallOrderEl.textContent = result.data.mall_order_id || '-';
      mallOrderEl.classList.toggle('matched', result.data.matched_by === 'order_no');
    }
    const slipEl = okBox.querySelector('[data-field="slip_no"]');
    if (slipEl) {
      slipEl.textContent = result.data.slip_no || '-';
      slipEl.classList.toggle('matched', result.data.matched_by === 'slip_no');
    }
    const matchedNote = okBox.querySelector('.matched-note');
    if (matchedNote) {
      if (result.data.matched_by === 'slip_no') {
        matchedNote.textContent = '※ NE 伝票番号でヒット (モール受注番号: ' + (result.data.mall_order_id || '-') + ')';
        matchedNote.hidden = false;
      } else {
        matchedNote.hidden = true;
      }
    }
    okBox.querySelector('.multi-line-warn').hidden = !(result.data.line_count && result.data.line_count > 1);
  }

  function hideLookupBoxes(sideId) {
    const key = sideId || 'single';
    document.getElementById('lookup-result-' + key).hidden = true;
    const errBox = document.getElementById('lookup-error-' + key);
    errBox.hidden = true;
    // 手動モールの指定もいったん畳む (別の番号を入れ直したのに前の指定が残らないように)
    const manualRow = errBox.querySelector('.manual-mall-row');
    const manualBtn = errBox.querySelector('.toggle-manual-btn');
    const options = errBox.querySelector('.lookup-options');
    if (manualRow) { manualRow.hidden = true; manualRow.querySelector('select').value = ''; }
    if (manualBtn) manualBtn.hidden = false;
    if (options) options.hidden = false;
  }

  /**
   * 「モールを手で選んで進む」で実際に選ばれているモール。
   * ステップ形式では今いないステップの panel 自体が hidden なので、
   * closest('[hidden]') だと「開いているのに隠れている」と誤判定する。
   * 見るのは該当行とエラーボックスの 2 つだけにする。
   */
  function manualMallValue(sfx) {
    const form = document.getElementById('submission-form');
    const el = form.querySelector(`[name="manual_mall${sfx}"]`);
    if (!el) return null;
    const row = el.closest('.manual-mall-row');
    const errBox = el.closest('.lookup-error');
    const shown = !!(row && !row.hidden && errBox && !errBox.hidden);
    return shown ? (el.value || null) : null;
  }

  function collectRecord(sideId) {
    const sfx = sideId ? '_' + sideId : '';
    const form = document.getElementById('submission-form');
    const get = (name) => {
      const el = form.querySelector(`[name="${name}${sfx}"]`);
      return el ? el.value : null;
    };
    const orderIdUnknown = !!form.querySelector(`[name="order_id_unknown${sfx}"]`)?.checked;
    const occurredOn = form.querySelector('[name="occurred_on"]').value;
    const reporterNote = form.querySelector('[name="reporter_note"]').value || null;
    const processStage = form.querySelector('[name="process_stage"]:checked')?.value || null;
    const misType = wizard.mixUp ? 'mix_up' : (form.querySelector('[name="mis_type"]:checked')?.value || null);

    // manual_mall: lookup ノヒットで「手動モール指定」を選んだ場合に値が入る (router.js が拾う)
    const manualMall = manualMallValue(sfx);

    const qty = numFieldValue(form.querySelector(`[name="qty_affected${sfx}"]`));
    const loss = numFieldValue(form.querySelector(`[name="loss_amount_jpy${sfx}"]`));

    return {
      client_submission_id: submissionIdFor(sideId),
      occurred_on: occurredOn,
      mall_order_id: orderIdUnknown ? null : (get('mall_order_id') || null),
      order_id_unknown: orderIdUnknown,
      manual_mall: manualMall,
      mis_type: misType,
      qty_affected: qty == null ? 1 : qty,
      loss_amount_jpy: loss == null ? 0 : loss,
      process_stage: processStage,
      reporter_note: reporterNote,
    };
  }

  function currentRecords() {
    return wizard.mixUp ? [collectRecord('a'), collectRecord('b')] : [collectRecord('')];
  }

  function renderReview() {
    const records = currentRecords();
    const form = document.getElementById('submission-form');
    const note = form.querySelector('[name="reporter_note"]').value;
    document.getElementById('review-body').innerHTML = records.map((r, i) => {
      const side = wizard.mixUp ? (i === 0 ? 'a' : 'b') : '';
      const key = side || 'single';
      const looked = wizard.lookupState[key];
      const found = looked && looked.status === 'found' ? looked.data : null;
      return `
      <div class="mis-review-card">
        <h4>${wizard.mixUp ? (i === 0 ? '注文 A' : '注文 B') : '登録内容'}</h4>
        <dl class="mis-review-list">
          <dt>注文番号</dt><dd>${r.order_id_unknown ? '<em>不明 (在庫紛失など)</em>' : esc(r.mall_order_id)}</dd>
          ${found ? `
            <dt>モール</dt><dd>${esc(MALL_LABEL[found.mall] || found.mall || '-')}</dd>
            <dt>商品名</dt><dd>${esc(found.product_name || '-')}</dd>
            <dt>SKU</dt><dd>${esc(found.sku || '-')}</dd>
          ` : (r.manual_mall ? `<dt>モール (手で指定)</dt><dd>${esc(MALL_LABEL[r.manual_mall] || r.manual_mall)}</dd>` : '')}
          <dt>発生日</dt><dd>${esc(r.occurred_on)}</dd>
          <dt>種別</dt><dd>${misTypeTag(r.mis_type)}</dd>
          <dt>発見工程</dt><dd>${esc(STAGE_LABEL[r.process_stage] || '-')}</dd>
          <dt>影響数量</dt><dd class="is-strong">${r.qty_affected} 個</dd>
          <dt>損失額</dt><dd class="is-strong">${yen(r.loss_amount_jpy)}</dd>
        </dl>
      </div>`;
    }).join('')
      + (note ? `<div class="mis-review-card"><h4>詳細メモ</h4><p class="mis-memo">${esc(note)}</p></div>` : '')
      + `<p class="form-note">登録すると状態は「報告済」になります。根本原因は管理者が後から記録します。</p>`;
  }

  function openConfirmDialog() {
    // 最終ステップでも全ステップぶんを検証し直す (戻って消した項目を拾う)
    const errs = [];
    for (let i = 0; i < wizard.panels.length - 1; i++) errs.push(...validateStep(i));
    if (errs.length) { showErrors(Array.from(new Set(errs))); return; }
    clearErrors();

    const records = currentRecords();
    document.getElementById('submission-form').dataset.pendingPayload = JSON.stringify({ mix_up: wizard.mixUp, records });
    document.getElementById('confirm-body').innerHTML = `
      <p>${wizard.mixUp ? 'テレコとして <strong>2 件</strong>' : '<strong>1 件</strong>'} 登録します。</p>
      <div class="confirm-record">
        ${records.map((r) => `${r.order_id_unknown ? '注文番号: <em>不明</em>' : '注文番号: ' + esc(r.mall_order_id)}`
          + ` / ${esc(r.occurred_on)} / ${esc(MIS_TYPE_LABEL[r.mis_type] || r.mis_type)}`
          + ` / ${r.qty_affected} 個 / ${yen(r.loss_amount_jpy)}`).join('<br>')}
      </div>
      <p class="form-note">登録した記録は後から消せません (状態の履歴も残ります)。</p>`;
    document.getElementById('confirm-dialog').showModal();
  }

  async function doSubmit() {
    const form = document.getElementById('submission-form');
    const dialog = document.getElementById('confirm-dialog');
    const payload = JSON.parse(form.dataset.pendingPayload);
    const submitBtn = document.getElementById('confirm-submit');

    // 二重送信は「ボタンを数えて塞ぐ」のではなく、送信そのものを 1 回に絞る
    if (form.dataset.submitting === '1') return;
    form.dataset.submitting = '1';
    submitBtn.disabled = true;
    submitBtn.textContent = '登録中…';

    let result = null;
    let networkError = false;
    try {
      result = await apiFetch('/submissions', { method: 'POST', body: payload });
    } catch (e) {
      networkError = true;
    } finally {
      form.dataset.submitting = '';
      submitBtn.disabled = false;
      submitBtn.textContent = '登録する';
    }

    if (result && result.ok) {
      // 成否の知らせは一覧側で出す (ここで出してもすぐ遷移して読めない)
      window.location.href = '/apps/mis-shipment?created=' + (result.data?.idempotent ? 'dup' : '1');
      return;
    }

    // ここから先は必ずダイアログを閉じる。
    // <dialog> はトップレイヤーに出るので、開いたままだと画面内の知らせが背面に隠れる。
    dialog.close();

    if (networkError) {
      // 登録できたのかどうか分からない。client_submission_id は作り直さずに
      // 同じものを使い回すので、もう一度押しても二重登録にはならない (サーバ側 UNIQUE)。
      showErrors(['通信に失敗しました。「登録する」をもう一度押してください (同じ内容なら二重には登録されません)']);
      return;
    }
    if (result.status === 409) {
      // サーバに同じ冪等キーの記録が既にある = 登録は済んでいる
      showErrors(['この内容は既に登録されています。一覧で確認してください (同じ送信を内容だけ変えて登録し直すことはできません)']);
      return;
    }
    if (result.status === 503) {
      showErrors(['注文の検索サービスが止まっています。少し待ってからもう一度お試しください。']);
      return;
    }
    if (result.status === 400 && result.data?.error === 'lookup_miss_requires_manual_mall') {
      // 画面では見つかっていたのに、登録時のサーバ側の引き直しで外れた。
      // 検索し直させる (そうすれば「見つかりません」の枠と手動モールの入口が出る)。
      for (const side of (wizard.mixUp ? ['a', 'b'] : [''])) {
        delete wizard.lookupState[side || 'single'];
        hideLookupBoxes(side);
      }
      // goTo() は clearErrors() を呼ぶので、移動してからエラーを出す
      goTo(0);
      showErrors(['登録するときにサーバが注文を引き直したところ、マスターに見つかりませんでした。'
        + 'もう一度「🔍 検索」を押して、やはり見つからなければ「📝 モールを手で選んで進む」でモールを選んでください。']);
      return;
    }
    showErrors(['登録エラー: ' + (result.data?.error || result.status)]);
  }

  // ====================================================================
  // DETAIL PAGE
  // ====================================================================
  async function initDetailPage(id) {
    await reloadDetail(id);
  }

  // 状態変更・根本原因の保存が続くと reloadDetail が重なる。遅れて返った古い応答で
  // 状態や version が巻き戻らないように、最新の取得だけを描く。
  let detailSeq = 0;

  async function reloadDetail(id) {
    const mySeq = ++detailSeq;
    const body = document.getElementById('detail-body');
    const role = body.dataset.role;
    body.innerHTML = '<p class="loading">読み込み中...</p>';
    let result;
    try {
      result = await apiFetch('/submissions/' + id);
    } catch (e) {
      if (mySeq !== detailSeq) return;
      body.innerHTML = '<p class="empty">通信に失敗しました。画面を再読み込みしてください。</p>';
      return;
    }
    if (mySeq !== detailSeq) return;   // もっと新しい取得が既に描かれている
    if (!result.ok) {
      body.innerHTML = '<p class="empty">読み込みエラー (' + result.status + ')</p>';
      return;
    }
    renderDetail(body, result.data, role);
  }

  function renderDetail(body, detail, role) {
    const r = detail.row;
    const history = detail.history || [];
    const related = detail.related || [];
    const fieldHistory = detail.fieldHistory || [];
    const isAdmin = role === 'admin';
    const canCorrect = detail.canCorrectFields !== false;

    body.innerHTML = `
      ${detail.needsFieldReview ? `
      <section class="mis-panel mis-review-banner detail-section">
        <h3 class="mis-panel-title">⚠️ 誤出荷種別と発見工程は当てになりません</h3>
        <p class="form-note">
          2026-09-20 の修正より前に登録された記録です。当時は<strong>画面で何を選んでも先頭の選択肢
          (別商品 / ピッキング) が保存されていました</strong>。正しい値が分かるなら直してください。
          分からない場合は「その他」「不明」に直すか、そのままで構いません。
        </p>
        ${isAdmin
          ? (canCorrect
              ? `<button type="button" id="mark-field-reviewed" class="mis-btn">直すところは無い (確認済みにする)</button>`
              : `<p class="form-note">訂正履歴テーブルが使えないため、いまは訂正できません。</p>`)
          : `<p class="form-note">直せるのは管理者だけです。</p>`}
      </section>` : ''}

      <section class="mis-panel detail-section">
        <h3 class="mis-panel-title">🚦 状態</h3>
        <div class="status-stepper">${renderStepper(r.status)}</div>
        <div class="transition-buttons">${renderTransitionButtons(r, isAdmin)}</div>
      </section>

      ${related.length > 0 ? `
      <section class="mis-panel detail-section">
        <h3 class="mis-panel-title">⚭ テレコの相方</h3>
        ${related.map((rel) => `
          <a class="related-link" href="/apps/mis-shipment/detail/${rel.id}">
            #${rel.id} · ${esc(MALL_LABEL[rel.mall] || rel.mall || '不明')} · ${esc(rel.sku_snapshot || 'SKU 不明')} · ${esc(STATUS_LABEL[rel.status])} →
          </a>
        `).join('')}
        <p class="form-note">グループ ID: <code>${esc(r.mix_up_group_id)}</code></p>
      </section>` : ''}

      <div class="mis-detail-grid">
        <section class="mis-panel detail-section">
          <h3 class="mis-panel-title">📦 注文情報 (起票時の snapshot・編集不可)</h3>
          <dl class="info-row">
            <dt>モール</dt><dd>${esc(MALL_LABEL[r.mall] || r.mall || '不明')}</dd>
            <dt>注文番号</dt><dd>${r.order_id_unknown ? '<em>不明</em>' : esc(r.mall_order_id)}</dd>
            <dt>商品名</dt><dd>${esc(r.product_name_snapshot || '-')}</dd>
            <dt>SKU</dt><dd>${esc(r.sku_snapshot || '-')}</dd>
            <dt>注文日</dt><dd>${esc(r.order_date_snapshot || '-')}</dd>
            <dt>注文数量</dt><dd>${r.ordered_qty_snapshot != null ? r.ordered_qty_snapshot + ' 個' : '-'}</dd>
          </dl>
        </section>

        <section class="mis-panel detail-section">
          <h3 class="mis-panel-title">❌ 誤出荷の内容</h3>
          <dl class="info-row">
            <dt>発生日</dt><dd>${esc(r.occurred_on)}</dd>
            <dt>種別</dt><dd>${renderMisTypeField(r, isAdmin, canCorrect)}</dd>
            <dt>影響数量</dt><dd>${r.qty_affected} 個</dd>
            <dt>損失額</dt><dd><strong>${yen(r.loss_amount_jpy)}</strong></dd>
            <dt>報告者</dt><dd>${esc(r.reported_by || '-')}</dd>
          </dl>
        </section>
      </div>

      <section class="mis-panel detail-section">
        <h3 class="mis-panel-title">🔍 工程と原因</h3>
        <dl class="info-row">
          <dt>発見工程</dt><dd>${renderProcessStageField(r, isAdmin, canCorrect)}</dd>
          <dt>根本原因</dt><dd>${renderRootCauseField(r, isAdmin)}</dd>
          <dt>原因詳細</dt><dd>${renderRootCauseNoteField(r, isAdmin)}</dd>
        </dl>
      </section>

      <section class="mis-panel detail-section">
        <h3 class="mis-panel-title">📝 現場のメモ</h3>
        <p class="mis-memo">${esc(r.reporter_note || '(無し)')}</p>
      </section>

      ${fieldHistory.length > 0 ? `
      <section class="mis-panel detail-section">
        <h3 class="mis-panel-title">🛠 項目の訂正履歴 (追加のみ・変更不可)</h3>
        <table class="history-table">
          <tbody>
            ${fieldHistory.map((h) => `
              <tr>
                <td>${esc(String(h.changed_at).replace('T', ' ').slice(0, 19))}</td>
                <td>${esc(FIELD_LABEL[h.field_name] || h.field_name)}</td>
                <td>${h.field_name === 'field_review'
                      ? esc(fieldValueLabel('field_review', h.new_value)) + ' で確認'
                      : esc(fieldValueLabel(h.field_name, h.old_value)) + ' → <strong>' + esc(fieldValueLabel(h.field_name, h.new_value)) + '</strong>'}</td>
                <td>${esc(h.changed_by)}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
        <p class="append-only-note">※ append-only。修正・削除はできません</p>
      </section>` : ''}

      <section class="mis-panel detail-section">
        <h3 class="mis-panel-title">📜 状態の履歴 (追加のみ・変更不可)</h3>
        <table class="history-table">
          <tbody>
            ${history.map((h) => `
              <tr>
                <td>${esc(String(h.changed_at).replace('T', ' ').slice(0, 19))}</td>
                <td>${h.from_status ? esc(STATUS_LABEL[h.from_status]) : '(新規)'} → <strong>${esc(STATUS_LABEL[h.to_status])}</strong></td>
                <td>${esc(h.changed_by)}</td>
                <td>${esc(h.change_note || '')}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
        <p class="append-only-note">※ append-only。修正・削除はできません</p>
      </section>
    `;

    wireDetailHandlers(r, isAdmin);
  }

  /**
   * 誤出荷種別。本来は起票時に確定して編集不可だが、2026-09-20 の不具合を直すために
   * 管理者だけ変えられるようにしている。テレコ (mix_up) は mix_up_group_id と対の
   * CHECK 制約があるので変えられない。
   */
  function renderMisTypeField(r, isAdmin, canCorrect) {
    if (r.mis_type === 'mix_up') {
      return misTypeTag(r.mis_type) + ' <small>(テレコは変更できません)</small>';
    }
    if (!isAdmin) return misTypeTag(r.mis_type) + ' <small>(管理者のみ訂正可)</small>';
    if (!canCorrect) return misTypeTag(r.mis_type) + ' <small>(訂正履歴が使えないため訂正できません)</small>';
    return `<div class="root-cause-editor">
      <select id="mis-type-select">
        ${MIS_TYPE_OPTIONS.map((o) => `<option value="${o}" ${r.mis_type === o ? 'selected' : ''}>${MIS_TYPE_LABEL[o]}</option>`).join('')}
      </select>
      <button type="button" id="save-mis-type" class="mis-btn">保存</button>
    </div>`;
  }

  function renderProcessStageField(r, isAdmin, canCorrect) {
    if (!isAdmin) return esc(STAGE_LABEL[r.process_stage]) + ' <small>(管理者のみ訂正可)</small>';
    if (!canCorrect) return esc(STAGE_LABEL[r.process_stage]) + ' <small>(訂正履歴が使えないため訂正できません)</small>';
    return `<div class="root-cause-editor">
      <select id="process-stage-select">
        ${PROCESS_STAGE_OPTIONS.map((o) => `<option value="${o}" ${r.process_stage === o ? 'selected' : ''}>${STAGE_LABEL[o]}</option>`).join('')}
      </select>
      <button type="button" id="save-process-stage" class="mis-btn">保存</button>
    </div>`;
  }

  /** 種別・発見工程の訂正を送る。履歴が書けないときは訂正ごと止まる (サーバ側)。 */
  async function saveCorrectedField(r, field, value, btn) {
    btn.disabled = true;
    let result;
    try {
      result = await apiFetch('/submissions/' + r.id, {
        method: 'PATCH', body: { version: r.version, fields: { [field]: value } },
      });
    } catch (e) {
      btn.disabled = false;
      toast('通信に失敗しました。最新の状態を読み込み直します。', 'error');
      reloadDetail(r.id);
      return;
    }
    if (!result.ok) {
      btn.disabled = false;
      if (result.status === 409) {
        toast('ほかの人が先に更新しました。画面を読み込み直します。', 'error');
        reloadDetail(r.id);
        return;
      }
      toast('訂正できませんでした: ' + (result.data?.detail || result.data?.error || result.status), 'error');
      return;
    }
    toast(`${FIELD_LABEL[field]} を訂正しました`, 'ok');
    reloadDetail(r.id);
  }

  function renderRootCauseField(r, isAdmin) {
    const cur = r.root_cause_stage || 'unknown';
    if (!isAdmin) {
      return esc(STAGE_LABEL[cur]) + ' <small>(管理者のみ編集可)</small>';
    }
    const opts = ['receiving', 'supplier', 'master_data', 'picking', 'packing', 'labeling', 'inspection', 'system', 'other', 'unknown'];
    // 設計書 §6 業務ルール 3: resolved/closed のレコードは unknown に戻せない (Codex round 18 high 指摘対応)
    const lockUnknown = r.status === 'resolved' || r.status === 'closed';
    return `<div class="root-cause-editor"><select id="root-cause-stage">
      ${opts.map((o) => {
        const isUnknownLocked = lockUnknown && o === 'unknown' && cur !== 'unknown';
        return `<option value="${o}" ${cur === o ? 'selected' : ''} ${isUnknownLocked ? 'disabled' : ''}>${STAGE_LABEL[o]}${isUnknownLocked ? ' (完了済みには不可)' : ''}</option>`;
      }).join('')}
    </select> <button type="button" id="save-root-cause" class="mis-btn">保存</button></div>`;
  }

  function renderRootCauseNoteField(r, isAdmin) {
    if (!isAdmin) return esc(r.root_cause_note || '-') + ' <small>(管理者のみ編集可)</small>';
    return `<div class="root-cause-editor"><textarea id="root-cause-note" rows="2" maxlength="2000">${esc(r.root_cause_note || '')}</textarea>
      <button type="button" id="save-root-cause-note" class="mis-btn">保存</button></div>`;
  }

  function renderStepper(currentStatus) {
    const order = ['reported', 'investigating', 'resolved', 'closed'];
    const idx = order.indexOf(currentStatus);
    return order.map((s, i) => {
      const cls = i === idx ? 'active' : (i < idx ? 'passed' : '');
      const mark = i < idx ? '✔' : (i === idx ? STATUS_MARK[s] : '○');
      return `<span class="status-step ${cls}"><span class="status-mark" aria-hidden="true">${mark}</span>${STATUS_LABEL[s]}</span>`
        + (i < order.length - 1 ? '<span class="status-arrow" aria-hidden="true">→</span>' : '');
    }).join('');
  }

  function renderTransitionButtons(r, isAdmin) {
    const cur = r.status;
    // 設計書 §6 業務ルール 3: resolved/closed への遷移は root_cause_stage 確定が前提
    // (Codex round 17 high 指摘対応)
    const rootCauseConfirmed = r.root_cause_stage && r.root_cause_stage !== 'unknown';

    const buttons = [];
    if (cur === 'reported') buttons.push({ to: 'investigating', label: '調査開始', admin: false, requireRootCause: false });
    if (cur === 'investigating') buttons.push({ to: 'resolved', label: '完了に変更', admin: true, requireRootCause: true });
    if (cur === 'resolved') {
      buttons.push({ to: 'investigating', label: '調査に戻す', admin: true, requireRootCause: false });
      buttons.push({ to: 'closed', label: 'クローズ', admin: true, requireRootCause: true });
    }
    if (buttons.length === 0) return '<p class="form-note">この記録は完了しています (これ以上の状態変更はありません)</p>';

    return buttons.map((b) => {
      const adminBlocked = b.admin && !isAdmin;
      const rootCauseBlocked = b.requireRootCause && !rootCauseConfirmed;
      const disabled = adminBlocked || rootCauseBlocked;
      let title = '';
      if (adminBlocked) title = '管理者権限が必要です';
      else if (rootCauseBlocked) title = '先に「根本原因」を不明以外で確定してください';
      const suffix = adminBlocked ? ' <small>(管理者のみ)</small>'
        : (rootCauseBlocked ? ' <small>(根本原因が未確定)</small>' : '');
      return `<button type="button" data-to="${b.to}" data-version="${r.version}" data-id="${r.id}"
        class="mis-btn transition-btn ${b.admin ? 'admin-only' : ''} ${b.to === 'investigating' && cur === 'reported' ? 'mis-btn-primary' : ''}"
        ${disabled ? 'disabled' : ''} title="${esc(title)}">
        ${esc(b.label)}${suffix}
      </button>`;
    }).join('');
  }

  function wireDetailHandlers(r, isAdmin) {
    document.querySelectorAll('.transition-btn:not([disabled])').forEach((btn) => {
      btn.addEventListener('click', async () => {
        if (!window.confirm(`状態を「${STATUS_LABEL[btn.dataset.to]}」に変更します。よろしいですか？`)) return;
        btn.disabled = true;
        let result;
        try {
          result = await apiFetch('/submissions/' + btn.dataset.id, {
            method: 'PATCH',
            body: { version: parseInt(btn.dataset.version, 10), status: btn.dataset.to },
          });
        } catch (e) {
          // 変わったのかどうか分からないので、最新の状態と version を取り直す
          toast('通信に失敗しました。最新の状態を読み込み直します。', 'error');
          reloadDetail(parseInt(btn.dataset.id, 10));
          return;
        }
        if (!result.ok) {
          btn.disabled = false;
          if (result.status === 400 && result.data?.error === 'root_cause_required') {
            toast('完了・クローズに進む前に「根本原因」を不明以外で確定してください。', 'error');
            return;
          }
          if (result.status === 409) {
            toast('ほかの人が先に更新しました。画面を読み込み直します。', 'error');
            reloadDetail(parseInt(btn.dataset.id, 10));
            return;
          }
          toast('状態変更エラー: ' + (result.data?.error || result.status), 'error');
          return;
        }
        toast(`状態を「${STATUS_LABEL[btn.dataset.to]}」に変更しました`, 'ok');
        reloadDetail(parseInt(btn.dataset.id, 10));
      });
    });

    // 種別・発見工程の訂正 (2026-09-20 の不具合を直すための管理者専用の入口)
    const saveMisType = document.getElementById('save-mis-type');
    if (saveMisType) saveMisType.addEventListener('click', () => {
      saveCorrectedField(r, 'mis_type', document.getElementById('mis-type-select').value, saveMisType);
    });
    const saveStage = document.getElementById('save-process-stage');
    if (saveStage) saveStage.addEventListener('click', () => {
      saveCorrectedField(r, 'process_stage', document.getElementById('process-stage-select').value, saveStage);
    });
    const markReviewed = document.getElementById('mark-field-reviewed');
    if (markReviewed) markReviewed.addEventListener('click', async () => {
      markReviewed.disabled = true;
      let result;
      try {
        result = await apiFetch('/submissions/' + r.id, { method: 'PATCH', body: { field_review: true } });
      } catch (e) {
        markReviewed.disabled = false;
        toast('通信に失敗しました。もう一度お試しください。', 'error');
        return;
      }
      if (!result.ok) {
        markReviewed.disabled = false;
        toast('確認の記録に失敗しました: ' + (result.data?.detail || result.data?.error || result.status), 'error');
        return;
      }
      toast('確認済みにしました', 'ok');
      reloadDetail(r.id);
    });

    if (isAdmin) {
      const saveCause = document.getElementById('save-root-cause');
      if (saveCause) saveCause.addEventListener('click', async () => {
        const v = document.getElementById('root-cause-stage').value;
        let result;
        try {
          result = await apiFetch('/submissions/' + r.id, {
            method: 'PATCH', body: { version: r.version, fields: { root_cause_stage: v } },
          });
        } catch (e) {
          toast('通信に失敗しました。最新の状態を読み込み直します。', 'error');
          reloadDetail(r.id);
          return;
        }
        if (!result.ok) {
          if (result.status === 400 && result.data?.error === 'root_cause_unknown_forbidden_after_resolve') {
            toast('完了・クローズ済みの記録の根本原因を「不明」に戻すことはできません', 'error');
            return;
          }
          toast('保存エラー: ' + (result.data?.error || result.status), 'error');
          return;
        }
        toast('根本原因を保存しました', 'ok');
        reloadDetail(r.id);
      });
      const saveNote = document.getElementById('save-root-cause-note');
      if (saveNote) saveNote.addEventListener('click', async () => {
        const v = document.getElementById('root-cause-note').value;
        let result;
        try {
          result = await apiFetch('/submissions/' + r.id, {
            method: 'PATCH', body: { version: r.version, fields: { root_cause_note: v } },
          });
        } catch (e) {
          toast('通信に失敗しました。最新の状態を読み込み直します。', 'error');
          reloadDetail(r.id);
          return;
        }
        if (!result.ok) { toast('保存エラー: ' + (result.data?.error || result.status), 'error'); return; }
        toast('原因詳細を保存しました', 'ok');
        reloadDetail(r.id);
      });
    }
  }

  window.misShipment = { initIndexPage, initNewPage, initDetailPage };
})();
