/**
 * 誤出荷管理アプリ フロント JS
 * UI モック: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_UI_mock_v7.3.md
 *
 * グローバル: window.misShipment.{initIndexPage, initNewPage, initDetailPage}
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

  // ─── enum 表示マップ ───────────────────────────────
  const MALL_LABEL = { amazon: 'Amazon', rakuten: '楽天', yahoo: 'Yahoo', linegift: 'LINEギフト', mercari: 'メルカリ', aupay: 'auPAY', qoo10: 'Qoo10', other: 'その他', other_mall: 'その他' };
  const MIS_TYPE_LABEL = { wrong_item: '別商品', wrong_qty: '数量違い', damage: '破損', missing: '欠品', wrong_address: '誤宛先', mix_up: 'テレコ ⚭', other: 'その他' };
  const STAGE_LABEL = { picking: 'ピッキング', packing: '梱包', labeling: 'ラベル貼付', inspection: '検品', handover: '出荷引渡', receiving: '入庫', supplier: '仕入先', master_data: 'マスタ', system: 'システム', other: 'その他', unknown: '不明' };
  const STATUS_LABEL = { reported: '報告済', investigating: '調査中', resolved: '完了', closed: 'クローズ' };

  // ====================================================================
  // INDEX PAGE
  // ====================================================================
  function initIndexPage() {
    const form = document.getElementById('filter-form');
    form.addEventListener('submit', (e) => { e.preventDefault(); loadList(); });
    loadList();
  }

  async function loadList() {
    const form = document.getElementById('filter-form');
    const params = new URLSearchParams();
    new FormData(form).forEach((v, k) => { if (v) params.set(k, v); });
    const body = document.getElementById('results-body');
    body.innerHTML = '<tr><td colspan="9" class="loading">読み込み中...</td></tr>';

    const result = await apiFetch('/submissions?' + params.toString());
    if (!result.ok) {
      body.innerHTML = '<tr><td colspan="9" class="empty">読み込みエラー (' + result.status + ')</td></tr>';
      return;
    }
    const rows = result.data.rows || [];
    if (rows.length === 0) {
      body.innerHTML = '<tr><td colspan="9" class="empty">該当する誤出荷はありません</td></tr>';
      document.getElementById('results-summary').textContent = '';
      return;
    }
    body.innerHTML = rows.map((r) => `
      <tr class="${r.mis_type === 'mix_up' ? 'row-mix-up' : ''}">
        <td>${esc(r.occurred_on)}</td>
        <td>${esc(MALL_LABEL[r.mall] || r.mall || '不明')}</td>
        <td>${r.order_id_unknown ? '<em>不明</em>' : esc(r.mall_order_id)}</td>
        <td>${esc(r.sku_snapshot || '-')}</td>
        <td>${esc(MIS_TYPE_LABEL[r.mis_type] || r.mis_type)}</td>
        <td>${esc(STAGE_LABEL[r.process_stage])}</td>
        <td>${esc(STAGE_LABEL[r.root_cause_stage])}</td>
        <td class="status-${r.status}">${esc(STATUS_LABEL[r.status])}</td>
        <td class="num"><a href="/apps/mis-shipment/detail/${r.id}">¥${r.loss_amount_jpy.toLocaleString()}</a></td>
      </tr>
    `).join('');
    document.getElementById('results-summary').textContent = `${rows.length} 件表示 (件数上限まで)`;
  }

  // ====================================================================
  // NEW PAGE
  // ====================================================================
  function initNewPage(mixUp) {
    const form = document.getElementById('submission-form');

    // 発生日 default を JST 今日 (本来サーバ確定が筋だが UI 初期表示用)
    const today = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tokyo' }).format(new Date());
    form.querySelector('[name="occurred_on"]').value = today;

    // lookup ボタン (mixUp なら 2 つ、単独なら 1 つ)
    form.querySelectorAll('.btn-lookup').forEach((btn) => {
      btn.addEventListener('click', () => handleLookup(btn.dataset.side));
    });

    // order_id_unknown チェック時に注文番号 input を disable
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
        }
      });
    });

    // 「手動モールを指定」ボタン (lookup ノヒット時)
    form.querySelectorAll('.toggle-manual-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const errBox = document.getElementById('lookup-error-' + (btn.dataset.side || 'single'));
        const mallRow = errBox.querySelector('.manual-mall-row');
        mallRow.hidden = false;
        btn.hidden = true;
      });
    });

    // submit → 確認ダイアログ
    form.addEventListener('submit', (e) => {
      e.preventDefault();
      showConfirmDialog(mixUp);
    });

    // ダイアログのボタン
    document.getElementById('confirm-cancel').addEventListener('click', () => {
      document.getElementById('confirm-dialog').close();
    });
    document.getElementById('confirm-submit').addEventListener('click', () => {
      doSubmit(mixUp);
    });
  }

  async function handleLookup(sideId) {
    const sfx = sideId ? '_' + sideId : '';
    const input = document.querySelector(`[name="mall_order_id${sfx}"]`);
    const orderId = (input.value || '').trim();
    hideLookupBoxes(sideId);
    if (!orderId) {
      alert('注文番号を入力してください');
      return;
    }
    const result = await apiFetch('/orders/lookup?order_id=' + encodeURIComponent(orderId));
    const okBox = document.getElementById('lookup-result-' + (sideId || 'single'));
    const errBox = document.getElementById('lookup-error-' + (sideId || 'single'));
    if (!result.ok) {
      errBox.hidden = false;
      errBox.querySelector('p').textContent = '⚠️ 検索エラー: ' + (result.data?.error || result.status);
      return;
    }
    if (!result.data.found) {
      errBox.hidden = false;
      return;
    }
    okBox.hidden = false;
    okBox.querySelector('[data-field="mall"]').textContent = MALL_LABEL[result.data.mall] || result.data.mall || '-';
    okBox.querySelector('[data-field="product_name"]').textContent = result.data.product_name || '-';
    okBox.querySelector('[data-field="sku"]').textContent = result.data.sku || '-';
    okBox.querySelector('[data-field="order_date"]').textContent = result.data.order_date || '-';
    okBox.querySelector('[data-field="ordered_qty"]').textContent = (result.data.ordered_qty != null ? result.data.ordered_qty + ' 個' : '-');
    okBox.querySelector('.multi-line-warn').hidden = !(result.data.line_count && result.data.line_count > 1);
  }

  function hideLookupBoxes(sideId) {
    const key = sideId || 'single';
    document.getElementById('lookup-result-' + key).hidden = true;
    document.getElementById('lookup-error-' + key).hidden = true;
  }

  function collectRecord(sideId, mixUp) {
    const sfx = sideId ? '_' + sideId : '';
    const form = document.getElementById('submission-form');
    const get = (name) => {
      const el = form.querySelector(`[name="${name}${sfx}"]`);
      return el ? el.value : null;
    };
    const orderIdUnknown = !!form.querySelector(`[name="order_id_unknown${sfx}"]`)?.checked;
    const occurredOn = form.querySelector('[name="occurred_on"]').value;
    const reporterNote = form.querySelector('[name="reporter_note"]').value || null;
    const processStage = form.querySelector('[name="process_stage"]').value;
    const misType = mixUp ? 'mix_up' : form.querySelector('[name="mis_type"]')?.value;

    // manual_mall: lookup ノヒットで「手動モール指定」を選んだ場合に値が入る (router.js が拾う)
    const manualMallEl = form.querySelector(`[name="manual_mall${sfx}"]`);
    const manualMall = manualMallEl && !manualMallEl.closest('[hidden]') ? (manualMallEl.value || null) : null;

    return {
      client_submission_id: uuidv4(),
      occurred_on: occurredOn,
      mall_order_id: orderIdUnknown ? null : (get('mall_order_id') || null),
      order_id_unknown: orderIdUnknown,
      manual_mall: manualMall,
      mis_type: misType,
      qty_affected: parseInt(get('qty_affected') || '1', 10),
      loss_amount_jpy: parseInt(get('loss_amount_jpy') || '0', 10),
      process_stage: processStage,
      reporter_note: reporterNote,
    };
  }

  function showConfirmDialog(mixUp) {
    const records = mixUp
      ? [collectRecord('a', true), collectRecord('b', true)]
      : [collectRecord('', false)];

    // 簡易 validation
    for (const r of records) {
      if (!r.occurred_on || !r.process_stage) {
        alert('必須項目が未入力です (発生日、発見工程)');
        return;
      }
      if (!mixUp && !r.mis_type) {
        alert('誤出荷種別を選択してください');
        return;
      }
      if (!r.order_id_unknown && !r.mall_order_id) {
        alert('注文番号を入力するか、「注文番号不明」をチェックしてください');
        return;
      }
    }

    const body = document.getElementById('confirm-body');
    body.innerHTML = '<p>以下の内容で登録します。よろしいですか？</p>' + records.map((r, i) => `
      <div class="confirm-record">
        ${mixUp ? `<strong>${i === 0 ? '注文 A' : '注文 B'}</strong><br>` : ''}
        ${r.order_id_unknown ? '注文番号: <em>不明</em>' : '注文番号: ' + esc(r.mall_order_id)}<br>
        発生日: ${esc(r.occurred_on)} / 種別: ${esc(MIS_TYPE_LABEL[r.mis_type])} / 工程: ${esc(STAGE_LABEL[r.process_stage])}<br>
        数量: ${r.qty_affected} 個 / 損失: ¥${r.loss_amount_jpy.toLocaleString()}
      </div>`).join('');

    // 確認 submit 時に使うため form-dataset に保持
    document.getElementById('submission-form').dataset.pendingPayload = JSON.stringify({ mix_up: mixUp, records });
    document.getElementById('confirm-dialog').showModal();
  }

  async function doSubmit(mixUp) {
    const dlg = document.getElementById('confirm-dialog');
    const form = document.getElementById('submission-form');
    const payload = JSON.parse(form.dataset.pendingPayload);
    dlg.close();

    const submitBtn = document.getElementById('confirm-submit');
    submitBtn.disabled = true;
    const result = await apiFetch('/submissions', { method: 'POST', body: payload });
    submitBtn.disabled = false;

    if (result.ok || result.status === 201 || result.status === 200) {
      alert('登録しました' + (result.data?.idempotent ? ' (再送扱い)' : ''));
      window.location.href = '/apps/mis-shipment';
      return;
    }
    if (result.status === 409) {
      alert('既に同じ送信が登録されています (内容に差分があるため新規登録は拒否されました)');
      return;
    }
    if (result.status === 503) {
      alert('注文 lookup サービスが利用不能です。後でやり直してください。');
      return;
    }
    if (result.status === 400 && result.data?.error === 'lookup_miss_requires_manual_mall') {
      alert('注文がマスターに見つかりません。「手動モールを指定して進む」ボタンを押してから、モールを選んで再度登録してください。');
      return;
    }
    alert('登録エラー: ' + (result.data?.error || result.status));
  }

  // ====================================================================
  // DETAIL PAGE
  // ====================================================================
  async function initDetailPage(id) {
    await reloadDetail(id);
  }

  async function reloadDetail(id) {
    const body = document.getElementById('detail-body');
    const role = body.dataset.role;
    body.innerHTML = '<p class="loading">読み込み中...</p>';
    const result = await apiFetch('/submissions/' + id);
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
    const isAdmin = role === 'admin';

    body.innerHTML = `
      <section class="detail-section">
        <h3>📦 注文情報 (起票時 snapshot、編集不可)</h3>
        <dl class="info-row">
          <dt>モール</dt><dd>${esc(MALL_LABEL[r.mall] || r.mall || '不明')}</dd>
          <dt>注文番号</dt><dd>${r.order_id_unknown ? '<em>不明</em>' : esc(r.mall_order_id)}</dd>
          <dt>商品名</dt><dd>${esc(r.product_name_snapshot || '-')}</dd>
          <dt>SKU</dt><dd>${esc(r.sku_snapshot || '-')}</dd>
          <dt>注文日</dt><dd>${esc(r.order_date_snapshot || '-')}</dd>
          <dt>注文数量</dt><dd>${r.ordered_qty_snapshot != null ? r.ordered_qty_snapshot + ' 個' : '-'}</dd>
        </dl>
      </section>

      ${related.length > 0 ? `
      <section class="detail-section">
        <h3>⚭ テレコ相方 (mix_up_group_id: <code>${esc(r.mix_up_group_id)}</code>)</h3>
        ${related.map((rel) => `
          <a class="related-link" href="/apps/mis-shipment/detail/${rel.id}">
            #${rel.id} (${esc(MALL_LABEL[rel.mall] || rel.mall || '不明')} / ${esc(rel.sku_snapshot || '-')} / ${esc(STATUS_LABEL[rel.status])}) →
          </a>
        `).join('')}
      </section>` : ''}

      <section class="detail-section">
        <h3>❌ 誤出荷詳細</h3>
        <dl class="info-row">
          <dt>発生日</dt><dd>${esc(r.occurred_on)}</dd>
          <dt>種別</dt><dd>${esc(MIS_TYPE_LABEL[r.mis_type] || r.mis_type)}</dd>
          <dt>数量</dt><dd>${r.qty_affected} 個</dd>
          <dt>損失額</dt><dd>¥${r.loss_amount_jpy.toLocaleString()}</dd>
        </dl>
      </section>

      <section class="detail-section">
        <h3>🔍 工程情報</h3>
        <dl class="info-row">
          <dt>発見工程</dt><dd>${esc(STAGE_LABEL[r.process_stage])} (編集不可)</dd>
          <dt>根本原因</dt><dd>${renderRootCauseField(r, isAdmin)}</dd>
          <dt>原因詳細</dt><dd>${renderRootCauseNoteField(r, isAdmin)}</dd>
        </dl>
      </section>

      <section class="detail-section">
        <h3>🚦 状態 (現在: <strong>${esc(STATUS_LABEL[r.status])}</strong>)</h3>
        <div class="status-stepper">${renderStepper(r.status)}</div>
        <div class="transition-buttons">${renderTransitionButtons(r, isAdmin)}</div>
      </section>

      <section class="detail-section">
        <h3>📜 状態履歴 (変更不可、追加のみ)</h3>
        <table class="history-table">
          <tbody>
            ${history.map((h) => `
              <tr>
                <td>${esc(h.changed_at.replace('T', ' ').slice(0, 19))}</td>
                <td>${h.from_status ? esc(STATUS_LABEL[h.from_status]) : '(新規)'} → <strong>${esc(STATUS_LABEL[h.to_status])}</strong></td>
                <td>${esc(h.changed_by)}</td>
                <td>${esc(h.change_note || '')}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
        <p class="append-only-note">※ append-only。修正・削除はできません</p>
      </section>

      <section class="detail-section">
        <h3>📝 メモ</h3>
        <p>${esc(r.reporter_note || '(無し)')}</p>
      </section>
    `;

    wireDetailHandlers(r, isAdmin);
  }

  function renderRootCauseField(r, isAdmin) {
    const cur = r.root_cause_stage || 'unknown';
    if (!isAdmin) {
      return esc(STAGE_LABEL[cur]) + ' <small>(管理者のみ編集可)</small>';
    }
    const opts = ['receiving', 'supplier', 'master_data', 'picking', 'packing', 'labeling', 'inspection', 'system', 'other', 'unknown'];
    return `<select id="root-cause-stage">
      ${opts.map((o) => `<option value="${o}" ${cur === o ? 'selected' : ''}>${STAGE_LABEL[o]}</option>`).join('')}
    </select> <button type="button" id="save-root-cause" class="btn-primary">保存</button>`;
  }

  function renderRootCauseNoteField(r, isAdmin) {
    if (!isAdmin) return esc(r.root_cause_note || '-') + ' <small>(管理者のみ編集可)</small>';
    return `<textarea id="root-cause-note" rows="2" maxlength="2000">${esc(r.root_cause_note || '')}</textarea>
      <button type="button" id="save-root-cause-note" class="btn-primary">保存</button>`;
  }

  function renderStepper(currentStatus) {
    const order = ['reported', 'investigating', 'resolved', 'closed'];
    const idx = order.indexOf(currentStatus);
    return order.map((s, i) => {
      const cls = i === idx ? 'active' : (i < idx ? 'passed' : '');
      return `<span class="status-step ${cls}">${STATUS_LABEL[s]}</span>${i < order.length - 1 ? '→' : ''}`;
    }).join(' ');
  }

  function renderTransitionButtons(r, isAdmin) {
    const cur = r.status;
    // 設計書 §6 業務ルール 3: resolved/closed への遷移は root_cause_stage 確定が前提
    // (Codex round 17 high 指摘対応)
    const rootCauseConfirmed = r.root_cause_stage && r.root_cause_stage !== 'unknown';

    const buttons = [];
    if (cur === 'reported') buttons.push({ to: 'investigating', label: '調査開始', admin: false, requireRootCause: false });
    if (cur === 'investigating') buttons.push({ to: 'resolved', label: '完了に変更 (管理者のみ)', admin: true, requireRootCause: true });
    if (cur === 'resolved') {
      buttons.push({ to: 'investigating', label: '調査に戻す (管理者のみ)', admin: true, requireRootCause: false });
      buttons.push({ to: 'closed', label: 'クローズ (管理者のみ)', admin: true, requireRootCause: true });
    }
    return buttons.map((b) => {
      const adminBlocked = b.admin && !isAdmin;
      const rootCauseBlocked = b.requireRootCause && !rootCauseConfirmed;
      const disabled = adminBlocked || rootCauseBlocked;
      let title = '';
      if (adminBlocked) title = '管理者権限が必要です';
      else if (rootCauseBlocked) title = '先に「根本原因」を unknown 以外で確定してください';
      return `<button type="button" data-to="${b.to}" data-version="${r.version}" data-id="${r.id}"
        class="btn-secondary transition-btn ${b.admin ? 'admin-only' : ''}"
        ${disabled ? 'disabled' : ''} title="${esc(title)}">
        ${esc(b.label)}${rootCauseBlocked ? ' <small>(根本原因未確定)</small>' : ''}
      </button>`;
    }).join('');
  }

  function wireDetailHandlers(r, isAdmin) {
    document.querySelectorAll('.transition-btn:not([disabled])').forEach((btn) => {
      btn.addEventListener('click', async () => {
        if (!confirm(`状態を「${STATUS_LABEL[btn.dataset.to]}」に変更します。よろしいですか？`)) return;
        const result = await apiFetch('/submissions/' + btn.dataset.id, {
          method: 'PATCH',
          body: { version: parseInt(btn.dataset.version, 10), status: btn.dataset.to },
        });
        if (!result.ok) {
          if (result.status === 400 && result.data?.error === 'root_cause_required') {
            alert('完了/クローズに進む前に「根本原因」を unknown 以外で確定してください。');
            return;
          }
          alert('状態変更エラー: ' + (result.data?.error || result.status));
          return;
        }
        reloadDetail(parseInt(btn.dataset.id, 10));
      });
    });

    if (isAdmin) {
      const saveCause = document.getElementById('save-root-cause');
      if (saveCause) saveCause.addEventListener('click', async () => {
        const v = document.getElementById('root-cause-stage').value;
        const result = await apiFetch('/submissions/' + r.id, {
          method: 'PATCH', body: { version: r.version, fields: { root_cause_stage: v } },
        });
        if (!result.ok) { alert('保存エラー: ' + (result.data?.error || result.status)); return; }
        reloadDetail(r.id);
      });
      const saveNote = document.getElementById('save-root-cause-note');
      if (saveNote) saveNote.addEventListener('click', async () => {
        const v = document.getElementById('root-cause-note').value;
        const result = await apiFetch('/submissions/' + r.id, {
          method: 'PATCH', body: { version: r.version, fields: { root_cause_note: v } },
        });
        if (!result.ok) { alert('保存エラー: ' + (result.data?.error || result.status)); return; }
        reloadDetail(r.id);
      });
    }
  }

  window.misShipment = { initIndexPage, initNewPage, initDetailPage };
})();
