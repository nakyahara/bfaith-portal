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
    return '¥' + Number(n).toLocaleString();
  }

  function fmtPct(p, decimals = 2) {
    if (p == null) return '-';
    return p.toFixed(decimals) + '%';
  }

  function fmtPctDiff(diff) {
    if (diff == null) return '-';
    const sign = diff > 0 ? '+' : '';
    return sign + diff.toFixed(2) + 'pt';
  }

  function fmtYenDiff(diff) {
    if (diff == null) return '-';
    const sign = diff > 0 ? '+' : '';
    return sign + fmtYen(Math.abs(diff)) * (diff < 0 ? -1 : 1);  // 一旦記号付きで
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

  async function loadDashboard() {
    const form = document.getElementById('period-form');
    const params = new URLSearchParams();
    new FormData(form).forEach((v, k) => { if (v) params.set(k, v); });

    const body = document.getElementById('dashboard-body');
    body.innerHTML = '<p class="loading">読み込み中...</p>';

    const result = await apiFetch('/summary?' + params.toString());
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
      // Chart.js 未ロードならポーリングで待つ (CDN 遅延対策)
      setTimeout(() => { if (window.Chart) drawTrendChart(d.monthly_trend); }, 800);
    }
  }

  function renderDashboard(d) {
    const cur = d.current;
    const prev = d.previous;
    const diff = d.diff;
    const target = d.industry_target_pct;
    const overTarget = cur.error_rate_pct != null && cur.error_rate_pct > target;
    const lossDiff = diff.total_loss_jpy;
    const lossDiffPct = prev.total_loss_jpy > 0 ? (lossDiff * 100 / prev.total_loss_jpy) : null;

    return `
      <!-- KPI カード 4 つ -->
      <section class="kpi-cards">
        <div class="kpi-card ${overTarget ? 'kpi-warn' : 'kpi-ok'}">
          <div class="kpi-label">📈 誤出荷率</div>
          <div class="kpi-value">${fmtPct(cur.error_rate_pct)}</div>
          <div class="kpi-detail">
            ${cur.distinct_orders} 件 / ${cur.shipped_orders.toLocaleString()} 注文<br>
            業界目標 ≤ ${fmtPct(target)}  ${overTarget ? '⚠️ オーバー' : '✅'}
          </div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">💴 期間損失額</div>
          <div class="kpi-value">${fmtYen(cur.total_loss_jpy)}</div>
          <div class="kpi-detail">
            前期間 ${fmtYen(prev.total_loss_jpy)}<br>
            差分: ${lossDiff >= 0 ? '+' : ''}${fmtYen(lossDiff)}${lossDiffPct != null ? ' (' + (lossDiffPct >= 0 ? '+' : '') + lossDiffPct.toFixed(1) + '%)' : ''}
          </div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">📋 誤出荷件数</div>
          <div class="kpi-value">${cur.incidents}</div>
          <div class="kpi-detail">
            前期間 ${prev.incidents} 件<br>
            差分: ${diff.incidents >= 0 ? '+' : ''}${diff.incidents}
          </div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">💰 千件あたり損失</div>
          <div class="kpi-value">${fmtYen(cur.loss_per_1000_orders_jpy)}</div>
          <div class="kpi-detail">
            注文1,000件あたり<br>
            (出荷量補正済み)
          </div>
        </div>
      </section>

      <!-- Top 5 SKU -->
      <section class="dashboard-section">
        <h3>🏆 Top 5 SKU (件数順)</h3>
        ${d.top_skus.length === 0 ? '<p class="empty">期間内に誤出荷なし</p>' : `
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
                  <td>${esc(s.product_name || '-')}</td>
                  <td class="num">${s.incidents}</td>
                  <td class="num">${fmtYen(s.loss_jpy)}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `}
      </section>

      <!-- Top 3 根因 -->
      <section class="dashboard-section">
        <h3>🔍 Top 3 根本原因 (確定済みのみ)</h3>
        ${d.top_root_causes.length === 0 ? '<p class="empty">根本原因が確定された案件なし (まだ調査中)</p>' : `
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
        `}
      </section>

      <!-- 月次推移 (Chart.js) -->
      <section class="dashboard-section">
        <h3>📊 月次推移 (直近6ヶ月)</h3>
        <div class="chart-wrap">
          <canvas id="trend-chart" height="100"></canvas>
        </div>
      </section>
    `;
  }

  function drawTrendChart(trend) {
    const ctx = document.getElementById('trend-chart');
    if (!ctx) return;
    if (trendChart) trendChart.destroy();

    const labels = trend.map(t => t.month);
    const incidents = trend.map(t => t.incidents);
    const errorRates = trend.map(t => t.error_rate_pct);

    trendChart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            type: 'bar',
            label: '誤出荷件数',
            data: incidents,
            backgroundColor: 'rgba(220, 38, 38, 0.6)',
            yAxisID: 'y1',
            order: 2,
          },
          {
            type: 'line',
            label: '誤出荷率 (%)',
            data: errorRates,
            borderColor: 'rgba(37, 99, 235, 1)',
            backgroundColor: 'rgba(37, 99, 235, 0.1)',
            yAxisID: 'y2',
            tension: 0.2,
            order: 1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y1: {
            type: 'linear',
            position: 'left',
            title: { display: true, text: '誤出荷件数' },
            beginAtZero: true,
            ticks: { precision: 0 },
          },
          y2: {
            type: 'linear',
            position: 'right',
            title: { display: true, text: '誤出荷率 (%)' },
            beginAtZero: true,
            grid: { drawOnChartArea: false },
          },
        },
      },
    });
  }

  window.misShipmentDashboard = { init };
})();
