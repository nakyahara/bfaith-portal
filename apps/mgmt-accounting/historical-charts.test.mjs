// ヒストリカルタブに足した 2 つのグラフ（前年同月比 / コスト構造の比率）のテスト
//   node --test apps/mgmt-accounting/historical-charts.test.mjs
//
// 2 つのことを確かめる:
//   1. /api/historical の monthlyTotals の SQL を、db.js の本物のスキーマの上で流して結果を見る
//      (テスト用に書き写したスキーマではなく、db.js から CREATE TABLE を取り出して使う)
//   2. 画面スクリプトを router.js のテンプレートから取り出して実際に実行する
//      テンプレートリテラルの中身は node --check の構文チェックが届かない場所なので、
//      ここで Chart を差し替えて呼び出し、描画に渡る値まで確かめる
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROUTER_SRC = fs.readFileSync(path.join(__dirname, 'router.js'), 'utf8');
const DB_SRC = fs.readFileSync(path.join(__dirname, '..', 'warehouse-mirror', 'db.js'), 'utf8');

// ─── ソースから取り出すユーティリティ ───

function extractCreateTable(table) {
  const marker = 'CREATE TABLE IF NOT EXISTS ' + table + ' (';
  const start = DB_SRC.indexOf(marker);
  assert.notEqual(start, -1, table + ' の CREATE TABLE が db.js に見つからない');
  const end = DB_SRC.indexOf(')`', start);
  assert.notEqual(end, -1, table + ' の CREATE TABLE の終わりが見つからない');
  return DB_SRC.slice(start, end + 1);
}

function extractMonthlyTotalsSql() {
  const marker = 'const monthlyTotals = db.prepare(`';
  const start = ROUTER_SRC.indexOf(marker);
  assert.notEqual(start, -1, 'monthlyTotals の SQL が router.js に見つからない');
  const end = ROUTER_SRC.indexOf('`).all();', start);
  assert.notEqual(end, -1, 'monthlyTotals の SQL の終わりが見つからない');
  return ROUTER_SRC.slice(start + marker.length, end);
}

// renderPage が返すテンプレートの中の画面スクリプト（Chart.js の CDN タグは src だけなので中身は無い）
function extractPageScript() {
  const open = ROUTER_SRC.indexOf('<script>');
  assert.notEqual(open, -1, '画面スクリプトの <script> が見つからない');
  const close = ROUTER_SRC.indexOf('</script>', open);
  assert.notEqual(close, -1, '</script> が見つからない');
  const body = ROUTER_SRC.slice(open + '<script>'.length, close);
  // ${JSON.stringify(定数)} はサーバ側で埋まる値。ここで試す 2 関数は使わないので null に潰す
  const filled = body.replace(/\$\{[^{}]*\}/g, 'null');
  assert.ok(!filled.includes('${'), '入れ子の埋め込み式が残っている（この取り出し方では扱えない）');
  return filled;
}

// ─── 1. monthlyTotals の SQL ───

test('monthlyTotals: 確定済みの月だけを、モール×セグメントを畳んで月合計にする', () => {
  const db = new DatabaseSync(':memory:');
  db.exec(extractCreateTable('mgmt_monthly_closing'));
  db.exec(extractCreateTable('mgmt_monthly_pl'));

  const insC = db.prepare('INSERT INTO mgmt_monthly_closing (year_month, fiscal_year, fiscal_month, status) VALUES (?,?,?,?)');
  insC.run('2026-07', 10, 1, 'confirmed');
  insC.run('2026-08', 10, 2, 'confirmed');
  insC.run('2026-09', 10, 3, 'draft');       // 未確定 → 出てはいけない
  insC.run('2026-06', 9, 12, 'needs_review'); // 要再確定 → 出てはいけない

  const insP = db.prepare(`INSERT INTO mgmt_monthly_pl
    (year_month, mall_id, segment, sales, cost, pf_fee, ad_cost, freight, material, variable_cost, gross_profit, fiscal_year)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
  // 2026-07 は 2 行（モール違い）→ 足し合わせて 1 行になること
  insP.run('2026-07', 'rakuten', 1, 700, 400, 70, 30, 50, 20, 570, 130, 10);
  insP.run('2026-07', 'amazon', 3, 500, 300, 50, 30, 40, 10, 430, 70, 10);
  insP.run('2026-08', 'rakuten', 1, 900, 500, 90, 40, 70, 20, 720, 180, 10);
  insP.run('2026-09', 'rakuten', 1, 999, 1, 1, 1, 1, 1, 5, 994, 10);   // 未確定月
  insP.run('2026-06', 'rakuten', 1, 888, 1, 1, 1, 1, 1, 5, 883, 9);    // 要再確定月

  const rows = db.prepare(extractMonthlyTotalsSql()).all();

  assert.deepEqual(rows.map(r => r.year_month), ['2026-07', '2026-08'], '確定済みの月だけが、古い順に並ぶ');
  assert.equal(rows[0].sales, 1200, '2026-07 の売上は 2 モールの合計');
  assert.equal(rows[0].cost, 700);
  assert.equal(rows[0].pf_fee, 120);
  assert.equal(rows[0].ad_cost, 60);
  assert.equal(rows[0].freight, 90);
  assert.equal(rows[0].material, 30);
  assert.equal(rows[0].gross_profit, 200, '2026-07 の粗利も合計');
  assert.equal(rows[0].fiscal_year, 10);
  assert.equal(rows[0].fiscal_month, 1, '7月 = 第1会計月（決算は7月始まり）');
  assert.equal(rows[1].fiscal_month, 2);
  db.close();
});

test('monthlyTotals: 締めだけあって PL 行が無い月は出さない（グラフに空の月を作らない）', () => {
  const db = new DatabaseSync(':memory:');
  db.exec(extractCreateTable('mgmt_monthly_closing'));
  db.exec(extractCreateTable('mgmt_monthly_pl'));
  db.prepare('INSERT INTO mgmt_monthly_closing (year_month, fiscal_year, fiscal_month, status) VALUES (?,?,?,?)')
    .run('2026-07', 10, 1, 'confirmed');
  const rows = db.prepare(extractMonthlyTotalsSql()).all();
  assert.equal(rows.length, 0);
  db.close();
});

// ─── 2. 画面スクリプト ───

// 第9期(2025-07〜) と 第10期(2026-07〜) の 2 期ぶん。変動費の内訳は売上と合うようにしてある
const TOTALS = [
  { year_month: '2025-07', fiscal_year: 9, fiscal_month: 1, sales: 1000, cost: 600, pf_fee: 100, ad_cost: 50, freight: 80, material: 20, variable_cost: 850, gross_profit: 150 },
  // 前年の粗利がマイナスの月 → 粗利の前年同月比は出さない（+%と出ると改善が悪化に見えるため）
  { year_month: '2025-08', fiscal_year: 9, fiscal_month: 2, sales: 1000, cost: 700, pf_fee: 100, ad_cost: 50, freight: 80, material: 120, variable_cost: 1050, gross_profit: -50 },
  { year_month: '2026-07', fiscal_year: 10, fiscal_month: 1, sales: 1200, cost: 700, pf_fee: 120, ad_cost: 60, freight: 90, material: 30, variable_cost: 1000, gross_profit: 200 },
  { year_month: '2026-08', fiscal_year: 10, fiscal_month: 2, sales: 900, cost: 500, pf_fee: 90, ad_cost: 40, freight: 70, material: 20, variable_cost: 720, gross_profit: 180 },
];

function loadPage() {
  const elements = new Map();
  const el = (id) => {
    if (!elements.has(id)) elements.set(id, { id, textContent: '', innerHTML: '', value: '', style: {} });
    return elements.get(id);
  };
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
  };
  // 画面スクリプトの末尾には loadCosts() があり fetch を待つ。解決しない Promise を返して止めておく
  const fetchMock = () => new Promise(() => {});

  const body = extractPageScript();
  const tail = `
;globalThis.__mgmtChartsTest = {
  setData: (totals, months) => { _monthlyTotals = totals; _histMonthSet = new Set(months); },
  renderYoyChart, renderCostMixChart,
};`;
  const fn = new Function('document', 'Chart', 'fetch', 'window', 'alert', 'setTimeout', 'clearTimeout', body + tail);
  fn(documentMock, ChartMock, fetchMock, {}, () => {}, () => 0, () => {});

  const api = globalThis.__mgmtChartsTest;
  delete globalThis.__mgmtChartsTest;
  return { el, charts, destroyed, api };
}

// Chart に渡った設定のうち、最後に chartYoy / chartCostMix に描いたものを取る
function lastChart(charts, canvasId) {
  for (let i = charts.length - 1; i >= 0; i--) if (charts[i].canvas === canvasId) return charts[i].cfg;
  return null;
}

test('前年同月比: 期ごとの線が会計月の並びで重なり、当期の前年比が棒で出る（売上）', () => {
  const page = loadPage();
  page.el('yoyMetric').value = 'sales';
  page.api.setData(TOTALS, ['2026-07', '2026-08']);
  page.api.renderYoyChart();

  const cfg = lastChart(page.charts, 'chartYoy');
  assert.ok(cfg, 'chartYoy が描かれていない');
  assert.deepEqual(cfg.data.labels.slice(0, 3), ['7月', '8月', '9月'], '横軸は7月始まりの会計月');
  assert.equal(cfg.data.labels.length, 12);

  const bar = cfg.data.datasets.find(d => d.type === 'bar');
  const lines = cfg.data.datasets.filter(d => d.type === 'line');
  assert.deepEqual(lines.map(d => d.label), ['第9期', '第10期'], '古い期から順に線が並ぶ');
  assert.equal(lines[1].data[0], 1200, '当期の7月の売上');
  assert.equal(lines[0].data[0], 1000, '前期の7月の売上');
  assert.equal(lines[1].data[2], null, 'まだ確定していない月は途切れる（0 で埋めない）');
  assert.equal(lines[1].borderWidth, 3, '当期の線を太くする');

  assert.ok(bar, '前年同月比の棒が無い');
  assert.equal(Math.round(bar.data[0]), 20, '7月: 1200 / 1000 → +20%');
  assert.equal(Math.round(bar.data[1]), -10, '8月: 900 / 1000 → -10%');
  assert.equal(bar.data[2], null, '片方でも欠けている月は出さない');
  assert.match(page.el('yoyInfo').textContent, /第10期 と 第9期/);
});

test('前年同月比: 前年が0以下の月は比率にしない（粗利マイナスの月）', () => {
  const page = loadPage();
  page.el('yoyMetric').value = 'gross_profit';
  page.api.setData(TOTALS, ['2026-07', '2026-08']);
  page.api.renderYoyChart();

  const cfg = lastChart(page.charts, 'chartYoy');
  const bar = cfg.data.datasets.find(d => d.type === 'bar');
  assert.equal(Math.round(bar.data[0]), 33, '7月: 200 / 150 → +33%');
  assert.equal(bar.data[1], null, '8月: 前年が -50 なので比率を出さない');
  const lines = cfg.data.datasets.filter(d => d.type === 'line');
  assert.equal(lines[0].data[1], -50, '線そのものにはマイナスの粗利がそのまま出る');
});

test('前年同月比: 粗利率のときは引き算（ポイント差）になる', () => {
  const page = loadPage();
  page.el('yoyMetric').value = 'gross_margin';
  page.api.setData(TOTALS, ['2026-07', '2026-08']);
  page.api.renderYoyChart();

  const cfg = lastChart(page.charts, 'chartYoy');
  const lines = cfg.data.datasets.filter(d => d.type === 'line');
  assert.ok(Math.abs(lines[1].data[0] - 200 / 1200 * 100) < 1e-9, '当期7月の粗利率 = 16.67%');
  const bar = cfg.data.datasets.find(d => d.type === 'bar');
  assert.match(bar.label, /pt/, '棒のラベルが pt になる');
  assert.ok(Math.abs(bar.data[0] - (200 / 1200 - 150 / 1000) * 100) < 1e-9, '率どうしは引き算');
  // 前年の粗利がマイナスでも「率の差」は意味があるので、こちらは出す
  assert.ok(Math.abs(bar.data[1] - (180 / 900 - (-50) / 1000) * 100) < 1e-9);
});

test('前年同月比: 期が1つしかないときは棒を出さず、その旨を書く', () => {
  const page = loadPage();
  page.el('yoyMetric').value = 'sales';
  page.api.setData(TOTALS.filter(t => t.fiscal_year === 10), ['2026-07', '2026-08']);
  page.api.renderYoyChart();

  const cfg = lastChart(page.charts, 'chartYoy');
  assert.equal(cfg.data.datasets.filter(d => d.type === 'bar').length, 0);
  assert.equal(cfg.data.datasets.length, 1);
  assert.match(page.el('yoyInfo').textContent, /比べられる前期がまだありません/);
});

test('コスト構造: 売上を100%とした率になり、合計が100%になる', () => {
  const page = loadPage();
  page.api.setData(TOTALS, ['2026-07', '2026-08']);
  page.api.renderCostMixChart();

  const cfg = lastChart(page.charts, 'chartCostMix');
  assert.ok(cfg, 'chartCostMix が描かれていない');
  assert.deepEqual(cfg.data.labels, ['2026-07', '2026-08'], '表示期間に入っている月だけを描く');
  assert.deepEqual(cfg.data.datasets.map(d => d.label), ['原価', 'PF手数料', '広告費', '運賃', '資材費', '粗利']);

  const sum0 = cfg.data.datasets.reduce((s, d) => s + d.data[0], 0);
  assert.ok(Math.abs(sum0 - 100) < 1e-9, '2026-07 の帯の合計が 100%: ' + sum0);
  assert.ok(Math.abs(cfg.data.datasets[0].data[0] - 700 / 1200 * 100) < 1e-9, '原価率 = 700/1200');
  assert.equal(cfg.data.datasets[0].amounts[0], 700, 'tooltip 用に金額も持たせる');
  assert.equal(page.el('costMixInfo').textContent, '2ヶ月分');

  // 積み上げの軸設定（率なので stacked でないと意味が変わる）
  assert.equal(cfg.options.scales.y.stacked, true);
  assert.equal(cfg.options.scales.x.stacked, true);
});

test('コスト構造: 表示期間の外の月と、売上0の月は描かない', () => {
  const page = loadPage();
  const withZero = TOTALS.concat([
    { year_month: '2026-09', fiscal_year: 10, fiscal_month: 3, sales: 0, cost: 0, pf_fee: 0, ad_cost: 0, freight: 0, material: 0, variable_cost: 0, gross_profit: 0 },
  ]);
  page.api.setData(withZero, ['2026-08', '2026-09']);
  page.api.renderCostMixChart();

  const cfg = lastChart(page.charts, 'chartCostMix');
  assert.deepEqual(cfg.data.labels, ['2026-08'], '期間外(2025年)と売上0の月(2026-09)は落ちる');
});

test('描ける月が無いときは、前に描いたグラフを消して古い数字を残さない', () => {
  const page = loadPage();
  page.el('yoyMetric').value = 'sales';
  page.api.setData(TOTALS, ['2026-07', '2026-08']);
  page.api.renderYoyChart();
  page.api.renderCostMixChart();
  const drawn = page.charts.length;

  page.api.setData([], []);
  page.api.renderYoyChart();
  page.api.renderCostMixChart();

  assert.equal(page.charts.length, drawn, 'データが無いときは新しく描かない');
  assert.ok(page.destroyed.includes('chartYoy'), '前のグラフを destroy している');
  assert.ok(page.destroyed.includes('chartCostMix'));
  assert.equal(page.el('yoyInfo').textContent, 'データがありません');
  assert.equal(page.el('costMixInfo').textContent, '表示できる月がありません');
});
