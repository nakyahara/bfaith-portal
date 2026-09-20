/**
 * 誤出荷ダッシュボード フロント JS
 *
 * KPI 表示 (用途 A: 経営月次レビュー):
 *   - 月間損失額 + 前期間比
 *   - 誤出荷率 + 業界目標との比較
 *   - Top 5 SKU (件数 + 損失額)
 *   - Top 3 根本原因
 *   - 月次推移 (直近6ヶ月、Chart.js 線+棒グラフ)
 *
 * 関連: apps/mis-shipment/summary.js (KPI 計算)
 * 配色はアプリ本体と同じダーク (mis-shipment.css)。グラフも既定色のままだと
 * 軸ラベルが背景に埋もれて読めないので、明示的に指定している。
 */
(function () {
  'use strict';

  const API = '/apps/mis-shipment/api';

  // ─── enum 表示マップ ───
  const MALL_LABEL = {
    amazon: 'Amazon', amazon_fbm: 'Amazon (FBM)', rakuten: '楽天', yahoo: 'Yahoo',
    linegift: 'LINEギフト', mercari: 'メルカリ', aupay: 'auPAY', qoo10: 'Qoo10', other: 'その他',
  };
  const STAGE_LABEL = {
    picking: 'ピッキング', packing: '梱包', labeling: 'ラベル貼付', inspection: '検品',
    handover: '出荷引渡', receiving: '入庫', supplier: '仕入先', master_data: 'マスタ',
    system: 'システム', other: 'その他', unknown: '不明',
  };

  function esc(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' }[c]));
  }

  function fmtYen(n) {
    if (n == null) return '-';
    return '¥' + Number(n).toLocaleString('ja-JP');
  }

  function fmtPct(p, decimals = 2) {
    if (p == null) return '-';
    return p.toFixed(decimals) + '%';
  }

  // ─── HTTP ───
  async function apiFetch(path) {
    const res = await fetch(API + path, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
    let data = null;
    try { data = await res.json(); } catch (_) {}
    return { ok: res.ok, status: res.status, data };
  }

  // ─── INIT ───
  function init() {
    const form = document.getElementById('period-form');

    form.querySelector('[name="period"]').addEventListener('change', (e) => {
      const isCustom = e.target.value === 'custom';
      form.querySelectorAll('.custom-only').forEach(el => el.hidden = !isCustom);
    });

    form.addEventListener('submit', (e) => {
      e.preventDefault();
      loadDashboard();
    });

    // 初回 load
    loadDashboard();
  }

  // ─── データ取得 + 描画 ───
  let trendChart = null;
  // 期間を続けて切り替えると、遅れて返った古い期間の集計が新しい表示を上書きする。
  // 通し番号で最新の応答だけ描く。
  let loadSeq = 0;

  async function loadDashboard() {
    const mySeq = ++loadSeq;
    const form = document.getElementById('period-form');
    const params = new URLSearchParams();
    new FormData(form).forEach((v, k) => { if (v) params.set(k, v); });

    const body = document.getElementById('dashboard-body');
    body.innerHTML = '<p class="loading">読み込み中...</p>';

    let result;
    try {
      result = await apiFetch('/summary?' + params.toString());
    } catch (e) {
      if (mySeq !== loadSeq) return;
      body.innerHTML = '<p class="empty">通信に失敗しました。画面を再読み込みしてください。</p>';
      return;
    }
    if (mySeq !== loadSeq) return;   // もっと新しい期間の結果が既に出ている
    if (!result.ok) {
      body.innerHTML = '<p class="empty">読み込みエラー (' + result.status + '): ' + esc(result.data?.error || '') + '</p>';
      return;
    }

    const d = result.data;
    document.getElementById('period-label').textContent = d.period.label;

    body.innerHTML = renderDashboard(d);
    if (window.Chart) {
      drawTrendChart(d.monthly_trend);
    } else {
      // Chart.js 未ロードならポーリングで待つ (CDN 遅延対策)。
      // 待っている間に期間が変わっていたら描かない。
      setTimeout(() => {
        if (mySeq !== loadSeq) return;
        if (window.Chart) drawTrendChart(d.monthly_trend);
      }, 800);
    }
  }

  /** 前期間との差。増えた=赤・減った=緑 に加えて ▲▼ も付ける (色だけに頼らない)。 */
  function deltaHtml(diff, absText) {
    if (diff == null) return '';
    const cls = diff > 0 ? 'is-up' : (diff < 0 ? 'is-down' : 'is-flat');
    const mark = diff > 0 ? '▲ 増' : (diff < 0 ? '▼ 減' : '→ 同じ');
    return `前期間比: <span class="kpi-delta ${cls}">${mark} ${diff === 0 ? '' : esc(absText)}</span>`;
  }

  function renderDashboard(d) {
    const cur = d.current;
    const prev = d.previous;
    const diff = d.diff;
    const target = d.industry_target_pct;
    const overTarget = cur.incident_rate_pct != null && cur.incident_rate_pct > target;
    const lossDiff = diff.total_loss_jpy;
    const lossDiffPct = prev.total_loss_jpy > 0 ? (lossDiff * 100 / prev.total_loss_jpy) : null;

    return `
      <!-- KPI カード 4 つ -->
      <section class="kpi-cards">
        <div class="kpi-card ${overTarget ? 'kpi-warn' : 'kpi-ok'}">
          <div class="kpi-label">📈 誤出荷件数率</div>
          <div class="kpi-value">${fmtPct(cur.incident_rate_pct)}</div>
          <div class="kpi-detail">
            ${cur.incidents} 件 / ${cur.shipped_line_count.toLocaleString()} ライン<br>
            業界目標 ≤ ${fmtPct(target)}  ${overTarget ? '⚠️ オーバー' : '✅ 範囲内'}<br>
            <small>※ 件数ベース (1注文1ライン主体で業界 ODR 近似)</small>
          </div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">💴 期間損失額</div>
          <div class="kpi-value">${fmtYen(cur.total_loss_jpy)}</div>
          <div class="kpi-detail">
            前期間 ${fmtYen(prev.total_loss_jpy)}<br>
            ${deltaHtml(lossDiff, fmtYen(Math.abs(lossDiff)) + (lossDiffPct != null ? ' (' + Math.abs(lossDiffPct).toFixed(1) + '%)' : ''))}
          </div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">📋 誤出荷件数</div>
          <div class="kpi-value">${cur.incidents}</div>
          <div class="kpi-detail">
            前期間 ${prev.incidents} 件<br>
            ${deltaHtml(diff.incidents, Math.abs(diff.incidents) + ' 件')}
          </div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">💰 千ライン当り損失</div>
          <div class="kpi-value">${fmtYen(cur.loss_per_1000_lines_jpy)}</div>
          <div class="kpi-detail">
            出荷ライン 1,000 あたり<br>
            (出荷量の多い少ないを補正した値)
          </div>
        </div>
      </section>

      <!-- Top 5 SKU -->
      <section class="mis-panel dashboard-section">
        <h3 class="mis-panel-title">🏆 Top 5 SKU (件数順)</h3>
        ${d.top_skus.length === 0 ? '<p class="empty">期間内に誤出荷なし</p>' : `
          <div class="mis-table-wrap">
            <table class="result-table">
              <thead>
                <tr>
                  <th>SKU</th><th>モール</th><th>商品名</th>
                  <th class="num">件数</th><th class="num">損失額</th>
                </tr>
              </thead>
              <tbody>
                ${d.top_skus.map(s => `
                  <tr>
                    <td>${esc(s.sku || '-')}</td>
                    <td>${esc(MALL_LABEL[s.mall] || s.mall || '-')}</td>
                    <td class="mis-cell-item">${esc(s.product_name || '-')}</td>
                    <td class="num">${s.incidents}</td>
                    <td class="num">${fmtYen(s.loss_jpy)}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `}
      </section>

      <div class="dashboard-grid">
        <!-- Top 3 根因 -->
        <section class="mis-panel dashboard-section">
          <h3 class="mis-panel-title">🔍 Top 3 根本原因 (確定済みのみ)</h3>
          ${d.top_root_causes.length === 0 ? '<p class="empty">根本原因が確定された案件なし (まだ調査中)</p>' : `
            <div class="mis-table-wrap">
              <table class="result-table">
                <thead>
                  <tr>
                    <th>根本原因</th><th class="num">件数</th><th class="num">損失額</th>
                  </tr>
                </thead>
                <tbody>
                  ${d.top_root_causes.map(r => `
                    <tr>
                      <td>${esc(STAGE_LABEL[r.root_cause_stage] || r.root_cause_stage)}</td>
                      <td class="num">${r.incidents}</td>
                      <td class="num">${fmtYen(r.loss_jpy)}</td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          `}
        </section>

        <!-- 月次推移 (Chart.js) -->
        <section class="mis-panel dashboard-section">
          <h3 class="mis-panel-title">📊 月次推移 (直近6ヶ月)</h3>
          <div class="chart-wrap">
            <canvas id="trend-chart"></canvas>
          </div>
        </section>
      </div>
    `;
  }

  // ─── 暗い地に載せるためのグラフ配色 (既定の #666 は背景に埋もれて読めない) ───
  const CHART_TEXT = '#a3b0d0';
  const CHART_GRID = 'rgba(163, 176, 208, 0.14)';

  function drawTrendChart(trend) {
    const ctx = document.getElementById('trend-chart');
    if (!ctx) return;
    if (trendChart) trendChart.destroy();

    const labels = trend.map(t => t.month);
    const incidents = trend.map(t => t.incidents);
    const rates = trend.map(t => t.incident_rate_pct);

    trendChart = new Chart(ctx, {
      data: {
        labels,
        datasets: [
          {
            type: 'bar',
            label: '誤出荷件数',
            data: incidents,
            backgroundColor: 'rgba(91, 149, 255, 0.45)',
            borderColor: 'rgba(91, 149, 255, 0.95)',
            borderWidth: 1,
            borderRadius: 4,
            yAxisID: 'y1',
            order: 2,
          },
          {
            type: 'line',
            label: '誤出荷件数率 (%)',
            data: rates,
            borderColor: '#ffc457',
            backgroundColor: 'rgba(255, 196, 87, 0.15)',
            pointBackgroundColor: '#ffc457',
            pointRadius: 3,
            borderWidth: 2,
            yAxisID: 'y2',
            tension: 0.25,
            order: 1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        color: CHART_TEXT,
        plugins: {
          legend: { labels: { color: CHART_TEXT, boxWidth: 12 } },
          tooltip: {
            backgroundColor: '#15203a',
            borderColor: '#27344f',
            borderWidth: 1,
            titleColor: '#e7edff',
            bodyColor: '#a3b0d0',
          },
        },
        scales: {
          x: { ticks: { color: CHART_TEXT }, grid: { color: CHART_GRID } },
          y1: {
            type: 'linear',
            position: 'left',
            title: { display: true, text: '誤出荷件数', color: CHART_TEXT },
            beginAtZero: true,
            ticks: { precision: 0, color: CHART_TEXT },
            grid: { color: CHART_GRID },
          },
          y2: {
            type: 'linear',
            position: 'right',
            title: { display: true, text: '誤出荷件数率 (%)', color: CHART_TEXT },
            beginAtZero: true,
            ticks: { color: CHART_TEXT },
            grid: { drawOnChartArea: false },
          },
        },
      },
    });
  }

  window.misShipmentDashboard = { init };
})();
