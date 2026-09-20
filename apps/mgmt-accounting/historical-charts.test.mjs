// ヒストリカルタブに足した 2 つのグラフ（前年同月比 / コスト構造の比率）のテスト
//   node --test apps/mgmt-accounting/historical-charts.test.mjs
//
// 一時ディレクトリに warehouse-mirror.db を実初期化 (DATA_DIR) して本物の mgmt_monthly_pl /
// mgmt_monthly_closing に月を入れ、
//   GET /api/historical → renderPage() が返した HTML の画面スクリプト → loadHistorical()
// と実際につないで、Chart に渡る値まで見る。
// 画面スクリプトは巨大なテンプレートリテラルの中にあり node --check が届かない場所なので、
// renderPage() が評価したあとの HTML から取り出して実行する (${...} の書き間違いもここで落ちる)。
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// DATA_DIR は db.js の import 時に評価されるため、動的 import の前に設定する
const prevDataDir = process.env.DATA_DIR;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mgmt-charts-'));
process.env.DATA_DIR = tmp;
let db, router;
try {
  const dbMod = await import('../warehouse-mirror/db.js');
  router = (await import('./router.js')).default;
  db = dbMod.initMirrorDB();
} catch (e) {
  fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  if (prevDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prevDataDir;
  throw e;
}
after(() => {
  if (db) db.close();
  fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  if (prevDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = prevDataDir;
});

// ─── router のハンドラを直に呼ぶ ───

function handlerFor(method, routePath) {
  const layer = router.stack.find((l) => l.route && l.route.path === routePath && l.route.methods[method]);
  assert.ok(layer, method.toUpperCase() + ' ' + routePath + ' が router に無い');
  return layer.route.stack[0].handle;
}

function callHistorical(query = {}) {
  let out;
  handlerFor('get', '/api/historical')(
    { query, params: {} },
    { json: (b) => { out = b; }, status() { return this; } },
    (e) => { throw e || new Error('next が呼ばれた'); },
  );
  assert.ok(out, '/api/historical が何も返していない');
  return out;
}

// renderPage() が返す本物の HTML（${...} はここで評価済み。書き間違えていればここで落ちる）
function renderedHtml() {
  let html;
  handlerFor('get', '/')(
    { query: {}, params: {}, headers: {}, user: { email: 'test@example.com' } },
    { send: (s) => { html = s; }, json: () => {}, status() { return this; } },
    (e) => { throw e || new Error('next が呼ばれた'); },
  );
  assert.ok(html && html.includes('chartYoy'), 'renderPage の HTML に前年同月比のグラフが無い');
  return html;
}

// ─── 月を入れる ───

const insClosing = () => db.prepare(
  'INSERT OR REPLACE INTO mgmt_monthly_closing (year_month, fiscal_year, fiscal_month, status) VALUES (?,?,?,?)');
const insPl = () => db.prepare(`INSERT OR REPLACE INTO mgmt_monthly_pl
  (year_month, mall_id, segment, sales, cost, pf_fee, ad_cost, freight, material, variable_cost, gross_profit, fiscal_year)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);

function clearMonths() {
  db.prepare('DELETE FROM mgmt_monthly_pl').run();
  db.prepare('DELETE FROM mgmt_monthly_closing').run();
}

// rows は [mall_id, segment, sales, cost, pf, ad, freight, material, gross_profit]
function putMonth(ym, fy, fm, status, rows) {
  insClosing().run(ym, fy, fm, status);
  for (const [mall, seg, sales, cost, pf, ad, fr, mat, gp] of rows) {
    insPl().run(ym, mall, seg, sales, cost, pf, ad, fr, mat, sales - gp, gp, fy);
  }
}

// 第8期 = 2025-07〜2026-06 / 第9期 = 2026-07〜 (getFiscalYear: 2025-07 → 8)
// 内訳の合計が売上に一致する月ばかりにしてある（差額のテストだけ別に作る）
function putBaseMonths() {
  clearMonths();
  // 2025-07: 2 モールに分かれた月 → 足して 1 行になること
  putMonth('2025-07', 8, 1, 'confirmed', [
    ['rakuten', 1, 700, 400, 70, 30, 50, 20, 130],
    ['amazon', 3, 500, 300, 50, 30, 40, 10, 70],
  ]); // 合計 sales 1200 / cost 700 / pf 120 / ad 60 / freight 90 / material 30 / gp 200
  // 2025-08: 粗利がマイナスの月（翌期の前年同月比を出してはいけない月）
  putMonth('2025-08', 8, 2, 'confirmed', [['rakuten', 1, 1000, 700, 100, 50, 80, 120, -50]]);
  // 2026-07: 前年比 売上 +20% / 粗利率 16.67% → 20%
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1440, 792, 144, 72, 108, 36, 288]]);
  // 2026-08: 売上 -10%
  putMonth('2026-08', 9, 2, 'confirmed', [['rakuten', 1, 900, 500, 90, 40, 70, 20, 180]]);
}

// ─── 画面スクリプトを HTML から取り出して動かす ───

function loadPage(histResponse) {
  const html = renderedHtml();
  const open = html.indexOf('<script>');
  const close = html.indexOf('</script>', open);
  assert.ok(open !== -1 && close !== -1, '画面スクリプトが取り出せない');
  const body = html.slice(open + '<script>'.length, close);

  const elements = new Map();
  const el = (id) => {
    // loadCosts() など他の画面コードも同じ要素を触るので、触られるものは一通り生やしておく
    if (!elements.has(id)) elements.set(id, { id, textContent: '', innerHTML: '', value: '', style: {}, classList: { add() {}, remove() {}, toggle() {} }, appendChild() {}, querySelectorAll: () => [], addEventListener() {} });
    return elements.get(id);
  };
  el('histMonths').value = '48';
  el('yoyMetric').value = 'sales';

  const charts = [];
  const destroyed = [];
  class ChartMock {
    constructor(ctx, cfg) { this.ctx = ctx; this.config = cfg; charts.push({ canvas: ctx && ctx.id, cfg }); }
    destroy() { destroyed.push(this.ctx && this.ctx.id); }
  }
  const documentMock = {
    getElementById: el,
    querySelectorAll: () => [],
    querySelector: () => null,
    addEventListener: () => {},
    createElement: () => ({ innerHTML: '', appendChild() {}, classList: { add() {} } }),
  };
  const fetchMock = async (url) => {
    const u = String(url);
    if (u.includes('/api/historical')) return { ok: true, status: 200, json: async () => histResponse };
    if (u.includes('/api/costs/')) return { ok: true, status: 200, json: async () => ({ freight: [], material: [], closing: null }) };
    return { ok: true, status: 200, json: async () => ({}) };
  };

  const tail = '\n;globalThis.__mgmtChartsTest = { loadHistorical, renderYoyChart, renderCostMixChart };';
  const fn = new Function('document', 'Chart', 'fetch', 'window', 'alert', 'setTimeout', 'clearTimeout', body + tail);
  fn(documentMock, ChartMock, fetchMock, {}, () => {}, () => 0, () => {});
  const api = globalThis.__mgmtChartsTest;
  delete globalThis.__mgmtChartsTest;
  assert.ok(api && api.loadHistorical, '画面スクリプトから関数を取り出せていない');
  return { el, charts, destroyed, api };
}

function lastChart(charts, canvasId) {
  for (let i = charts.length - 1; i >= 0; i--) if (charts[i].canvas === canvasId) return charts[i].cfg;
  return null;
}

// ─── 1. API ───

test('/api/historical: 確定済みの月だけを、モール×セグメントを畳んで月合計にする', () => {
  putBaseMonths();
  putMonth('2026-09', 9, 3, 'draft', [['rakuten', 1, 999, 1, 1, 1, 1, 1, 994]]);      // 未確定
  putMonth('2026-06', 8, 12, 'needs_review', [['rakuten', 1, 888, 1, 1, 1, 1, 1, 883]]); // 要再確定

  const { monthlyTotals } = callHistorical();
  assert.deepEqual(monthlyTotals.map((r) => r.year_month), ['2025-07', '2025-08', '2026-07', '2026-08'],
    '確定済みの月だけが古い順に並ぶ');
  const jul = monthlyTotals[0];
  assert.equal(jul.sales, 1200, '2 モールの売上を足す');
  assert.equal(jul.cost, 700);
  assert.equal(jul.pf_fee, 120);
  assert.equal(jul.ad_cost, 60);
  assert.equal(jul.freight, 90);
  assert.equal(jul.material, 30);
  assert.equal(jul.gross_profit, 200);
  assert.equal(jul.fiscal_year, 8);
  assert.equal(jul.fiscal_month, 1, '7月 = 第1会計月（決算は7月始まり）');
  assert.equal(monthlyTotals[3].fiscal_month, 2);
});

test('/api/historical: 表示期間を絞っても monthlyTotals は絞らない（前年と比べられなくなるため）', () => {
  putBaseMonths();
  const narrow = callHistorical({ months: '1' });
  assert.deepEqual(narrow.months, ['2026-08'], '表示期間は直近1ヶ月');
  assert.equal(narrow.monthlyTotals.length, 4, '月次合計は確定済み全月ぶん返る');
});

test('/api/historical: 締めだけあって PL 行が無い月はグラフに出さない', () => {
  clearMonths();
  insClosing().run('2026-07', 9, 1, 'confirmed');
  assert.equal(callHistorical().monthlyTotals.length, 0);
});

// ─── 2. API から画面まで通す ───

test('前年同月比: API の結果から、期ごとの線と当期の前年比の棒が描かれる（売上）', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartYoy');
  assert.ok(cfg, 'chartYoy が描かれていない');
  assert.deepEqual(cfg.data.labels.slice(0, 3), ['7月', '8月', '9月'], '横軸は7月始まりの会計月');
  assert.equal(cfg.data.labels.length, 12);

  const lines = cfg.data.datasets.filter((d) => d.type === 'line');
  assert.deepEqual(lines.map((d) => d.label), ['第8期', '第9期'], '古い期から順に線が並ぶ');
  assert.equal(lines[1].data[0], 1440, '当期7月の売上');
  assert.equal(lines[0].data[0], 1200, '前期7月の売上');
  assert.equal(lines[1].data[2], null, 'まだ確定していない月は途切れる（0 で埋めない）');
  assert.equal(lines[1].borderWidth, 3, '当期の線を太くする');

  const bar = cfg.data.datasets.find((d) => d.type === 'bar');
  assert.ok(bar, '前年同月比の棒が無い');
  assert.equal(Math.round(bar.data[0]), 20, '7月: 1440 / 1200 → +20%');
  assert.equal(Math.round(bar.data[1]), -10, '8月: 900 / 1000 → -10%');
  assert.equal(bar.data[2], null, '片方でも欠けている月は出さない');
  assert.match(page.el('yoyInfo').textContent, /第9期 と 第8期/);

  // 0 から描く（途中から描くと差が実際より大きく見える）
  assert.equal(cfg.options.scales.y.beginAtZero, true);
  assert.equal(cfg.options.scales.y1.beginAtZero, true);
});

test('前年同月比: 前年が0以下の月は比率にしない（粗利がマイナスだった月）', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  page.el('yoyMetric').value = 'gross_profit';
  page.api.renderYoyChart();

  const cfg = lastChart(page.charts, 'chartYoy');
  const bar = cfg.data.datasets.find((d) => d.type === 'bar');
  assert.equal(Math.round(bar.data[0]), 44, '7月: 288 / 200 → +44%');
  assert.equal(bar.data[1], null, '8月: 前年が -50 なので比率を出さない');
  const lines = cfg.data.datasets.filter((d) => d.type === 'line');
  assert.equal(lines[0].data[1], -50, '線そのものにはマイナスの粗利がそのまま出る');
});

test('前年同月比: 粗利率のときは引き算（ポイント差）で、前年がマイナスでも出す', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  page.el('yoyMetric').value = 'gross_margin';
  page.api.renderYoyChart();

  const cfg = lastChart(page.charts, 'chartYoy');
  const lines = cfg.data.datasets.filter((d) => d.type === 'line');
  assert.ok(Math.abs(lines[1].data[0] - 20) < 1e-9, '当期7月の粗利率 = 288/1440 = 20%');
  const bar = cfg.data.datasets.find((d) => d.type === 'bar');
  assert.match(bar.label, /pt/, '棒のラベルが pt になる');
  assert.ok(Math.abs(bar.data[0] - (20 - 200 / 1200 * 100)) < 1e-9, '率どうしは引き算');
  assert.ok(Math.abs(bar.data[1] - (180 / 900 - (-50) / 1000) * 100) < 1e-9, '前年がマイナスでも率の差は出す');
});

test('前年同月比: ひとつ前の期が無いときは、さらに前の期と比べない', async () => {
  clearMonths();
  // 第7期と第9期だけ（第8期が無い）→ 第9期 ÷ 第7期 を「前年同月比」と呼んではいけない
  putMonth('2024-07', 7, 1, 'confirmed', [['rakuten', 1, 1000, 600, 100, 50, 80, 20, 150]]);
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1440, 792, 144, 72, 108, 36, 288]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartYoy');
  assert.equal(cfg.data.datasets.filter((d) => d.type === 'bar').length, 0, '棒を出さない');
  assert.deepEqual(cfg.data.datasets.map((d) => d.label), ['第7期', '第9期'], '線としては両方描く');
  assert.match(page.el('yoyInfo').textContent, /第8期に確定した月がないため/);
});

test('コスト構造: 売上を100%とした率になり、帯の合計が100%になる', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartCostMix');
  assert.ok(cfg, 'chartCostMix が描かれていない');
  assert.deepEqual(cfg.data.labels, ['2025-07', '2025-08', '2026-07', '2026-08']);
  assert.deepEqual(cfg.data.datasets.map((d) => d.label), ['原価', 'PF手数料', '広告費', '運賃', '資材費', '粗利'],
    '内訳が合っている月には差額の帯を出さない');

  const sum0 = cfg.data.datasets.reduce((s, d) => s + d.data[0], 0);
  assert.ok(Math.abs(sum0 - 100) < 1e-9, '2025-07 の帯の合計が 100%: ' + sum0);
  assert.ok(Math.abs(cfg.data.datasets[0].data[0] - 700 / 1200 * 100) < 1e-9, '原価率 = 700/1200');
  assert.equal(cfg.data.datasets[0].amounts[0], 700, 'tooltip 用に金額も持たせる');
  assert.equal(page.el('costMixInfo').textContent, '4ヶ月分');
  assert.equal(cfg.options.scales.y.stacked, true);
  assert.equal(cfg.options.scales.x.stacked, true);
});

test('コスト構造: 費目と粗利を足しても売上に届かない月は、差額を帯にして見せる', async () => {
  clearMonths();
  // 過去の初期データ (2026-02 以前) は変動費が『売上 − 粗利』で入り、費目の合計とは別物になりうる。
  // 売上 1000 に対し 費目 700 + 粗利 100 = 800 → 差額 200
  putMonth('2025-07', 8, 1, 'confirmed', [['rakuten', 1, 1000, 600, 100, 0, 0, 0, 100]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartCostMix');
  const resid = cfg.data.datasets.find((d) => d.label.startsWith('差額'));
  assert.ok(resid, '差額の帯が出ていない（黙って100%に見せてはいけない）');
  assert.ok(Math.abs(resid.data[0] - 20) < 1e-9, '差額 200 / 売上 1000 = 20%');
  assert.equal(resid.amounts[0], 200);
  const sum = cfg.data.datasets.reduce((s, d) => s + d.data[0], 0);
  assert.ok(Math.abs(sum - 100) < 1e-9, '差額を入れて 100% になる');
});

test('表示期間: コスト構造は期間内の月だけ、前年同月比は期間によらず期をまたいで描く', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical({ months: '1' })); // 直近1ヶ月だけを指定
  page.el('histMonths').value = '1';
  await page.api.loadHistorical();

  const mix = lastChart(page.charts, 'chartCostMix');
  assert.deepEqual(mix.data.labels, ['2026-08'], 'コスト構造は表示期間に従う');

  const yoy = lastChart(page.charts, 'chartYoy');
  const lines = yoy.data.datasets.filter((d) => d.type === 'line');
  assert.deepEqual(lines.map((d) => d.label), ['第8期', '第9期'], '前年同月比は期間の外の前期も描く');
  assert.equal(lines[0].data[0], 1200, '期間外の 2025-07 の値が残っている');
});

test('確定済みの月が無いときは、前に描いたグラフを消して古い数字を残さない', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  const drawn = page.charts.length;

  clearMonths();
  const empty = callHistorical();
  assert.deepEqual(empty.months, []);
  assert.deepEqual(empty.monthlyTotals, [], 'データが無いときも monthlyTotals の形は保つ');

  // 空の応答で読み直す
  const page2 = loadPage(empty);
  await page2.api.loadHistorical();
  assert.equal(page2.charts.length, 0, 'データが無いときは何も描かない');
  assert.equal(page2.el('histInfo').textContent, 'データがありません');
  assert.equal(page2.el('yoyInfo').textContent, 'データがありません');
  assert.equal(page2.el('costMixInfo').textContent, '表示できる月がありません');
  assert.ok(drawn > 0);
});
