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

// db.js の本物の VIEW 定義を取り出す (テストに書き写すと実物とずれて気づけない)
function extractViewSql(name) {
  const src = fs.readFileSync(new URL('../warehouse-mirror/db.js', import.meta.url), 'utf8');
  const start = src.indexOf('CREATE VIEW ' + name + ' AS');
  assert.notEqual(start, -1, name + ' の CREATE VIEW が db.js に見つからない');
  const end = src.indexOf('`)', start);
  assert.notEqual(end, -1, name + ' の CREATE VIEW の終わりが見つからない');
  return src.slice(start, end);
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
  db.prepare('DELETE FROM mgmt_freight_costs').run();
  db.prepare('DELETE FROM mgmt_material_costs').run();
  db.prepare('DELETE FROM mirror_shipments_daily').run();
  db.prepare('DELETE FROM mirror_mf_pl_monthly').run();
  db.prepare('DELETE FROM mirror_mf_publish_runs').run();
}

// MF会計の取り込み (run) と月次 PL。固定費 = 販管費 (sgae_*)
function putMfRun(runId, status, scope = 'all') {
  db.prepare(`INSERT OR REPLACE INTO mirror_mf_publish_runs
    (run_id, scope, status, started_at, synced_at) VALUES (?,?,?,datetime('now'),datetime('now'))`).run(runId, scope, status);
}

function putMfPl(runId, ym, rows) {
  const stmt = db.prepare(`INSERT OR REPLACE INTO mirror_mf_pl_monthly
    (run_id, month_ym, role_key, amount_excl_tax, source_row_hash, synced_at)
    VALUES (?,?,?,?,?,datetime('now'))`);
  for (const [role, amount] of rows) stmt.run(runId, ym, role, amount, role + ym + runId);
}

// 運賃 (税抜)。cost_scope=shared が全モール按分の対象 = 1件あたりの分子の候補
function putFreight(ym, rows) {
  const stmt = db.prepare(`INSERT OR REPLACE INTO mgmt_freight_costs
    (year_month, carrier, amount, cost_scope) VALUES (?,?,?,?)`);
  for (const [carrier, amount, scope] of rows) stmt.run(ym, carrier, amount, scope || 'shared');
}

function putMaterial(ym, rows) {
  const stmt = db.prepare(`INSERT OR REPLACE INTO mgmt_material_costs
    (year_month, supplier, amount) VALUES (?,?,?)`);
  for (const [supplier, amount] of rows) stmt.run(ym, supplier, amount);
}

// 出荷件数 (NE 伝票ベース)。cancelled_slips は slips の内数
function putShipDay(date, slips, cancelled = 0) {
  db.prepare(`INSERT OR REPLACE INTO mirror_shipments_daily
    (ship_date, shop_code, shop_name, platform, delivery_id, delivery_name, slips, cancelled_slips, synced_at)
    VALUES (?,?,?,?,?,?,?,?,datetime('now'))`).run(date, 'S1', '店1', 'rakuten', 'D1', 'ヤマト宅急便', slips, cancelled);
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
  let response = histResponse;
  const html = renderedHtml();
  const open = html.indexOf('<script>');
  const close = html.indexOf('</script>', open);
  assert.ok(open !== -1 && close !== -1, '画面スクリプトが取り出せない');
  const body = html.slice(open + '<script>'.length, close);

  const elements = new Map();
  const el = (id) => {
    // loadCosts() など他の画面コードも同じ要素を触るので、触られるものは一通り生やしておく
    if (!elements.has(id)) {
      elements.set(id, { id, textContent: '', innerHTML: '', value: '', style: {}, children: [],
        classList: { add() {}, remove() {}, toggle() {} },
        appendChild(c) { this.children.push(c); },
        replaceChildren(...cs) { this.children = cs; },
        querySelectorAll: () => [], addEventListener() {} });
    }
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
    createElement: () => ({ innerHTML: '', value: '', textContent: '', appendChild() {}, classList: { add() {} } }),
  };
  const fetchMock = async (url) => {
    const u = String(url);
    if (u.includes('/api/historical')) return { ok: true, status: 200, json: async () => response };
    if (u.includes('/api/costs/')) return { ok: true, status: 200, json: async () => ({ freight: [], material: [], closing: null }) };
    return { ok: true, status: 200, json: async () => ({}) };
  };

  const tail = '\n;globalThis.__mgmtChartsTest = { loadHistorical, renderYoyChart, renderCostMixChart, renderWaterfallChart };';
  const fn = new Function('document', 'Chart', 'fetch', 'window', 'alert', 'setTimeout', 'clearTimeout', body + tail);
  fn(documentMock, ChartMock, fetchMock, {}, () => {}, () => 0, () => {});
  const api = globalThis.__mgmtChartsTest;
  delete globalThis.__mgmtChartsTest;
  assert.ok(api && api.loadHistorical, '画面スクリプトから関数を取り出せていない');
  // setResponse: 同じ画面のまま、次の loadHistorical() が受け取る応答を差し替える
  return { el, charts, destroyed, api, setResponse: (r) => { response = r; } };
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

test('月が無くなったら、同じ画面で読み直したときに前のグラフを消す', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  const drawn = page.charts.length;
  assert.ok(drawn > 0, '1 回目で描けていない');

  clearMonths();
  const empty = callHistorical();
  assert.deepEqual(empty.months, []);
  assert.deepEqual(empty.monthlyTotals, [], 'データが無いときも monthlyTotals の形は保つ');

  // 画面はそのままに、次の応答だけ空に差し替えて読み直す
  page.setResponse(empty);
  await page.api.loadHistorical();

  assert.equal(page.charts.length, drawn, '新しくは描かない');
  assert.ok(page.destroyed.includes('chartYoy'), '前に描いた前年同月比を消していない（古い数字が残る）');
  assert.ok(page.destroyed.includes('chartCostMix'), '前に描いたコスト構造を消していない');
  assert.equal(page.el('histInfo').textContent, 'データがありません');
  assert.equal(page.el('yoyInfo').textContent, 'データがありません');
  assert.equal(page.el('costMixInfo').textContent, '表示できる月がありません');
});

// ─── 3. 出荷1件あたりの運賃・資材費 ───

// 2026-07 だけ出荷件数がある月を作る。自社便 594,000円 ÷ 990件 = 600円/件
function putUnitCostMonths() {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1440, 792, 144, 72, 108, 36, 288]]);
  putMonth('2026-08', 9, 2, 'confirmed', [['rakuten', 1, 900, 500, 90, 40, 70, 20, 180]]);
  putFreight('2026-07', [
    ['ヤマト', 495000],
    ['クリックポスト', 99000],
    ['FBA運賃', 500000],      // Amazon が発送する分 → 分子に入れない
    ['RSL費用', 200000],      // 楽天スーパーロジが発送する分 → 分子に入れない
    ['TNK運賃(輸出)', 50000, 'direct'], // 按分の外 (shared でない) → 分子に入れない
  ]);
  putMaterial('2026-07', [['ダンボールワン', 60000], ['シモジマ', 39000]]); // 99,000円 → 100円/件
  // 2026-07 の出荷: 1,000 伝票のうち 10 がキャンセル → 990 件
  putShipDay('2026-07-01', 400, 4);
  putShipDay('2026-07-20', 600, 6);
  // 2026-08 は同期が届いている最後の月。月の途中かもしれないので分母にしない規則が効き、
  // shipments からは外れる (= 線が途切れる月になる)
  putShipDay('2026-08-03', 100);
}

test('/api/historical: 出荷件数を月ごとに返す（キャンセルは内数のまま渡す）', () => {
  putUnitCostMonths();
  const { shipments } = callHistorical();
  assert.deepEqual(shipments.map((r) => r.year_month), ['2026-07']);
  assert.equal(shipments[0].slips, 1000, '出荷確定した伝票の合計');
  assert.equal(shipments[0].cancelled_slips, 10, 'キャンセルは内数として別に渡す');
});

test('/api/historical: 確定月の外の日は出荷件数に入れない', () => {
  putUnitCostMonths();
  putShipDay('2026-06-30', 999); // 確定月 (2026-07〜08) より前
  putShipDay('2026-09-01', 888); // より後
  const { shipments } = callHistorical();
  assert.deepEqual(shipments.map((r) => r.year_month), ['2026-07', '2026-08'],
    '期間の外は拾わない（2026-09 まで届いているので 2026-08 は完全な月になる）');
});

test('1件あたり: 自社が発送した便だけを、キャンセルを引いた件数で割る', async () => {
  putUnitCostMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartUnitCost');
  assert.ok(cfg, 'chartUnitCost が描かれていない');
  const count = cfg.data.datasets.find((d) => d.label === '出荷件数');
  const freight = cfg.data.datasets.find((d) => d.label === '1件あたり運賃');
  const material = cfg.data.datasets.find((d) => d.label === '1件あたり資材費');

  const i = cfg.data.labels.indexOf('2026-07');
  assert.equal(count.data[i], 990, '1000 - キャンセル 10');
  assert.equal(freight.data[i], 600, '(ヤマト 495,000 + クリックポスト 99,000) ÷ 990 = 600円');
  assert.equal(material.data[i], 100, '資材費 99,000 ÷ 990 = 100円');
  assert.equal(freight.amounts[i], 594000, 'tooltip 用に分子も持たせる');
  assert.equal(freight.counts[i], 990, 'tooltip 用に分母も持たせる');
});

test('1件あたり: 相手が発送する分 (FBA・RSL) と輸出専用は分子に入れない', async () => {
  putUnitCostMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  const cfg = lastChart(page.charts, 'chartUnitCost');
  const freight = cfg.data.datasets.find((d) => d.label === '1件あたり運賃');
  const i = cfg.data.labels.indexOf('2026-07');
  // 全部入れると (495000+99000+500000+200000+50000) / 990 = 1,357円
  assert.equal(freight.amounts[i], 594000, 'FBA運賃・RSL費用・輸出運賃が混ざっていない');
});

test('1件あたり: どちらとも言えない carrier があった月は、安い単価を出さずに伏せる', async () => {
  putUnitCostMonths();
  putFreight('2026-07', [['謎の新しい便', 123456]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartUnitCost');
  const freight = cfg.data.datasets.find((d) => d.label === '1件あたり運賃');
  const material = cfg.data.datasets.find((d) => d.label === '1件あたり資材費');
  const i = cfg.data.labels.indexOf('2026-07');
  assert.equal(freight.data[i], null, '分子が欠けたまま 600円 と出すと「安くなった」と読まれる');
  assert.equal(material.data[i], 100, '資材費は運賃の分類とは関係ないので出る');
  assert.match(page.el('unitCostWarn').textContent, /謎の新しい便/, 'どの便が原因かを画面に出す');
});

test('1件あたり: 運賃が0円の月は単価を出さない（未入力とみなす）', async () => {
  putUnitCostMonths();
  db.prepare('DELETE FROM mgmt_freight_costs').run();
  putFreight('2026-07', [['ヤマト', 0], ['クリックポスト', 0]]); // 画面は未入力欄も 0 円の行として保存する
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartUnitCost');
  const freight = cfg.data.datasets.find((d) => d.label === '1件あたり運賃');
  const i = cfg.data.labels.indexOf('2026-07');
  assert.equal(freight.data[i], null, '出荷があるのに運賃0は実務上ないので、入っていないとみなす');
});

test('1件あたり: 出荷件数がわからない月は線を途切れさせる（0 で埋めない）', async () => {
  putUnitCostMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartUnitCost');
  const j = cfg.data.labels.indexOf('2026-08');
  for (const d of cfg.data.datasets) {
    assert.equal(d.data[j], null, d.label + ' が 2026-08 で 0 になっている');
  }
});

test('1件あたり: 出荷件数が1ヶ月も無ければ描かずに、その旨を出す', async () => {
  putUnitCostMonths();
  db.prepare('DELETE FROM mirror_shipments_daily').run();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  assert.equal(lastChart(page.charts, 'chartUnitCost'), null, '件数が無いのに単価を描いてはいけない');
  assert.match(page.el('unitCostInfo').textContent, /出荷件数のデータがない/);
});
test('1件あたり: 費目が1行も無い月は 0円ではなく「わからない」として描かない', async () => {
  putUnitCostMonths();
  // 2026-07 の資材費の行を消す（未入力の月を作る）。運賃の行は残す
  db.prepare('DELETE FROM mgmt_material_costs').run();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartUnitCost');
  const material = cfg.data.datasets.find((d) => d.label === '1件あたり資材費');
  const freight = cfg.data.datasets.find((d) => d.label === '1件あたり運賃');
  const i = cfg.data.labels.indexOf('2026-07');
  assert.equal(material.data[i], null, '未入力を 0円 として描くと「安くなった」と読まれる');
  assert.equal(freight.data[i], 600, '運賃の方は入っているので出る');
});

test('/api/historical: 同期が届いている最後の月は分母にしない（月の途中かもしれない）', () => {
  putUnitCostMonths();
  const res = callHistorical();
  assert.equal(res.shipments_through, '2026-08-03', '同期が届いている最後の日を返す');
  assert.deepEqual(res.shipments.map((r) => r.year_month), ['2026-07'],
    '2026-08 は月の途中までしか無いかもしれないので返さない');
});

test('1件あたり: 正味0件の月は「件数0」として棒を出し、単価だけ出さない', async () => {
  putUnitCostMonths();
  db.prepare('DELETE FROM mirror_shipments_daily').run();
  putShipDay('2026-07-01', 30, 30); // 出荷確定 30 件がすべてキャンセル = 正味 0 件
  putShipDay('2026-08-03', 100);    // 最後の月 (分母にしない)
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartUnitCost');
  assert.ok(cfg, '件数が 0 でも「データがない」にはしない');
  const count = cfg.data.datasets.find((d) => d.label === '出荷件数');
  const freight = cfg.data.datasets.find((d) => d.label === '1件あたり運賃');
  const i = cfg.data.labels.indexOf('2026-07');
  assert.equal(count.data[i], 0, '0件は 0 として出す（わからないのとは違う）');
  assert.equal(freight.data[i], null, '0 では割れない');
});

test('1件あたり: 出荷件数の取り込み日を画面に出す', async () => {
  putUnitCostMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  assert.match(page.el('unitCostWarn').textContent, /2026-07-01 〜 2026-08-03 を取り込み済み/);
});
test('1件あたり: 出荷件数を読めなかったときは「0件だった」と言わない', async () => {
  putUnitCostMonths();
  const res = callHistorical();
  // mirror_shipments_daily が無い環境を模す（テーブル不存在と「同期前で空」を画面で分ける）
  const broken = { ...res, shipments: [], shipments_through: null, shipments_error: 'no such table: mirror_shipments_daily' };
  const page = loadPage(broken);
  await page.api.loadHistorical();

  assert.equal(lastChart(page.charts, 'chartUnitCost'), null);
  assert.match(page.el('unitCostInfo').textContent, /読めませんでした/);
  assert.match(page.el('unitCostInfo').textContent, /no such table/);
});
test('1件あたり: グラフを描けない回でも、取り込み日と理由を出す', async () => {
  putUnitCostMonths();
  const res = callHistorical();
  // 表示対象が「除外された最新月」だけ = 描けないが、理由は言わなければならない
  const onlyLatest = { ...res, months: ['2026-08'] };
  const page = loadPage(onlyLatest);
  await page.api.loadHistorical();

  assert.equal(lastChart(page.charts, 'chartUnitCost'), null, '描けない');
  assert.match(page.el('unitCostWarn').textContent, /2026-07-01 〜 2026-08-03 を取り込み済み/, 'なぜ出ないのかが分かる');
});

test('1件あたり: 描けない回に、前回の注意書きが残らない', async () => {
  putUnitCostMonths();
  putFreight('2026-07', [['謎の新しい便', 123456]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  assert.match(page.el('unitCostWarn').textContent, /謎の新しい便/);

  // 出荷も運賃も無い期間に切り替える
  clearMonths();
  page.setResponse(callHistorical());
  await page.api.loadHistorical();
  assert.equal(page.el('unitCostWarn').textContent, '', '前の回の便名が今のデータの話として残る');
});
test('/api/historical: 取り込みが月の途中から始まっている月は分母にしない', () => {
  putUnitCostMonths();
  db.prepare('DELETE FROM mirror_shipments_daily').run();
  // backfill が月の途中の日から取ったケース (--months 12 など)。
  // 7月全額を 20日以降の件数で割ると単価が高く出る
  putShipDay('2026-07-20', 300);
  putShipDay('2026-08-03', 100);
  const res = callHistorical();
  assert.equal(res.shipments_from, '2026-07-20');
  assert.deepEqual(res.shipments_partial_months, ['2026-07', '2026-08']);
  assert.deepEqual(res.shipments, [], '両端しか無いので分母にできる月が無い');
});

test('/api/historical: 取り込みが月初から始まっていれば、その月は分母にできる', () => {
  putUnitCostMonths(); // 2026-07-01 から
  const res = callHistorical();
  assert.equal(res.shipments_from, '2026-07-01');
  assert.deepEqual(res.shipments_partial_months, ['2026-08'], '始まり側は月初なので外さない');
  assert.deepEqual(res.shipments.map((r) => r.year_month), ['2026-07']);
});

test('1件あたり: どの月を分母から外したかを画面に出す', async () => {
  putUnitCostMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  assert.match(page.el('unitCostWarn').textContent, /2026-08 は月の一部しか無いので分母にしていない/);
});

// ─── 4. 損益分岐点 ───

// 2026-07: 売上 1440 / 粗利 288 = 粗利率 20%。固定費 300 → 損益分岐点 1500 (売上は 60 下回る)
// 2026-08: 固定費を入れない月 (線が出ないはず)
function putBreakEvenMonths() {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1440, 792, 144, 72, 108, 36, 288]]);
  putMonth('2026-08', 9, 2, 'confirmed', [['rakuten', 1, 900, 500, 90, 40, 70, 20, 180]]);
  putMfRun(1, 'success');
  putMfRun(2, 'success'); // これが最新の成功
  putMfRun(3, 'failed');  // 失敗した回は見ない
  putMfPl(1, '2026-07', [['sgae_salary', 9999]]); // 古い回
  putMfPl(3, '2026-07', [['sgae_salary', 8888]]); // 失敗した回
  putMfPl(2, '2026-07', [
    ['sgae_salary', 200], ['sgae_rent', 100],
    ['cogs_purchase', 5000], // 売上原価 = この画面の変動費側。固定費に混ぜてはいけない
    ['sales', 99999],        // 売上も混ぜてはいけない
  ]);
}

test('/api/historical: 固定費は最新の成功した取り込みの販管費だけを足す', () => {
  putBreakEvenMonths();
  const { fixed_costs } = callHistorical();
  assert.deepEqual(fixed_costs.map((r) => r.year_month), ['2026-07']);
  assert.equal(fixed_costs[0].amount, 300, 'sgae_salary 200 + sgae_rent 100。仕入高と売上は入らない');
});

test('/api/historical: 固定費が無い期間でも他のデータは返る', () => {
  putBreakEvenMonths();
  db.prepare('DELETE FROM mirror_mf_pl_monthly').run();
  const res = callHistorical();
  assert.deepEqual(res.fixed_costs, []);
  assert.equal(res.monthlyTotals.length, 2, '固定費が無くても月次合計は返る');
});

test('損益分岐点: 固定費 ÷ 粗利率 で線を引き、足りているかを一言にする', async () => {
  putBreakEvenMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartBreakEven');
  assert.ok(cfg, 'chartBreakEven が描かれていない');
  const salesBar = cfg.data.datasets.find((d) => d.type === 'bar');
  const bep = cfg.data.datasets.find((d) => d.type === 'line');
  const i = cfg.data.labels.indexOf('2026-07');

  assert.equal(salesBar.data[i], 1440, '棒は実際の売上');
  assert.equal(bep.data[i], 1500, '固定費 300 ÷ 粗利率 0.2 = 1500');
  assert.equal(bep.detail[i].fixed, 300);
  assert.ok(Math.abs(bep.detail[i].rate - 0.2) < 1e-9);
  assert.equal(bep.detail[i].left, -12, '粗利 288 − 固定費 300 = −12 (固定費を引いた残り)');
  assert.match(page.el('breakEvenInfo').textContent, /2026-07 はこの線を 60円 下回っている/);
});

test('損益分岐点: 固定費が無い月は線を出さず、何ヶ月出せなかったかを書く', async () => {
  putBreakEvenMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartBreakEven');
  const bep = cfg.data.datasets.find((d) => d.type === 'line');
  const j = cfg.data.labels.indexOf('2026-08');
  assert.equal(bep.data[j], null, '固定費が無い月に線を引かない');
  assert.equal(cfg.data.datasets.find((d) => d.type === 'bar').data[j], 900, '売上の棒は出る');
  assert.match(page.el('breakEvenWarn').textContent, /MF会計の販管費がまだ無い 1ヶ月/, 'どの理由で出していないかを書く');
});

test('損益分岐点: 粗利がマイナスの月は線を出さない（割ると符号が逆になる）', async () => {
  clearMonths();
  // 売上 1000 / 粗利 −50 → 粗利率がマイナス。固定費を割ると負の損益分岐点になってしまう
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1000, 700, 100, 50, 80, 120, -50]]);
  putMfRun(1, 'success');
  putMfPl(1, '2026-07', [['sgae_salary', 300]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartBreakEven');
  assert.ok(cfg, '線は引けなくても売上の棒は描く（棒まで消すと売上が無いと読まれる）');
  assert.equal(cfg.data.datasets.find((d) => d.type === 'bar').data[0], 1000, '売上の棒は出る');
  assert.equal(cfg.data.datasets.find((d) => d.type === 'line').data[0], null, '線は引かない');
  assert.equal(page.el('breakEvenInfo').textContent, '線を引ける月がないため売上だけ表示',
    '固定費は取り込めているので「データがない」とは言わない');
  assert.match(page.el('breakEvenWarn').textContent, /粗利が0以下で割れない 1ヶ月/);
});

test('損益分岐点: 固定費を読めなかったときは「データがない」と言わない', async () => {
  putBreakEvenMonths();
  const res = callHistorical();
  const broken = { ...res, fixed_costs: [], fixed_costs_error: 'no such table: v_mirror_mf_pl_monthly_latest' };
  const page = loadPage(broken);
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartBreakEven');
  assert.ok(cfg, '固定費が読めなくても売上の棒は描く');
  assert.equal(cfg.data.datasets.find((d) => d.type === 'line').data.every((v) => v === null), true, '線は引かない');
  assert.match(page.el('breakEvenInfo').textContent, /読めませんでした（売上だけ表示）/);
  assert.match(page.el('breakEvenWarn').textContent, /no such table/);
});

test('損益分岐点: 描けない回に前回の注意書きが残らない', async () => {
  putBreakEvenMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  assert.match(page.el('breakEvenWarn').textContent, /線を出していない月/);

  clearMonths();
  page.setResponse(callHistorical());
  await page.api.loadHistorical();
  assert.equal(page.el('breakEvenWarn').textContent, '');
});

test('/api/historical: PL と関係ない取り込みが最新でも、固定費は PL の最新を見る', () => {
  putBreakEvenMonths();
  putMfRun(9, 'success', 'channel_sales'); // PL とは別の scope の取り込み
  putMfPl(9, '2026-07', [['sgae_salary', 7777]]);
  const { fixed_costs } = callHistorical();
  assert.equal(fixed_costs[0].amount, 300, 'channel_sales の回を PL の最新にしてはいけない');
});

test('/api/historical: 固定費のビューが無くても、ほかのデータは返る', () => {
  putBreakEvenMonths();
  const viewSql = extractViewSql('v_mirror_mf_pl_monthly_latest');
  db.exec('DROP VIEW v_mirror_mf_pl_monthly_latest');
  try {
    const res = callHistorical();
    assert.match(res.fixed_costs_error || '', /no such table|no such view/i, 'なぜ読めないかを返す');
    assert.deepEqual(res.fixed_costs, []);
    assert.equal(res.monthlyTotals.length, 2, '固定費が読めなくてもヒストリカル全体は落ちない');
  } finally {
    db.exec(viewSql);
  }
});

test('損益分岐点: 売上0の月と粗利0の月は、理由を分けて数える', async () => {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 0, 0, 0, 0, 0, 0, 0]]);       // 売上0
  putMonth('2026-08', 9, 2, 'confirmed', [['rakuten', 1, 1000, 1000, 0, 0, 0, 0, 0]]); // 粗利0
  putMfRun(1, 'success');
  putMfPl(1, '2026-07', [['sgae_salary', 300]]);
  putMfPl(1, '2026-08', [['sgae_salary', 300]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const warn = page.el('breakEvenWarn').textContent;
  assert.match(warn, /売上が0 1ヶ月/, '固定費はあるので「販管費が無い」と言ってはいけない');
  assert.match(warn, /粗利が0以下で割れない 1ヶ月/);
  assert.doesNotMatch(warn, /販管費がまだ無い/);
});

// ─── 5. 売上から粗利までの滝グラフ ───

test('滝グラフ: 月のプルダウンは新しい順で、既定はいちばん新しい月', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const sel = page.el('waterfallMonth');
  assert.deepEqual(sel.children.map((o) => o.value), ['2026-08', '2026-07', '2025-08', '2025-07'],
    '新しい月から並べる');
  assert.deepEqual(sel.children.map((o) => o.textContent), ['2026-08', '2026-07', '2025-08', '2025-07'],
    '表示も年月そのまま（HTML として解釈されない形で入れる）');
  assert.equal(sel.value, '2026-08', 'ブラウザ任せにせず既定を選ぶ');
  assert.match(page.el('waterfallInfo').textContent, /2026-08：売上 900円 → 粗利 180円（20.0%）/);
});

test('滝グラフ: 売上から費目を順に引いて粗利に着地する', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  page.el('waterfallMonth').value = '2025-07'; // 売上1200 原価700 PF120 広告60 運賃90 資材30 粗利200
  page.api.renderWaterfallChart();

  const cfg = lastChart(page.charts, 'chartWaterfall');
  assert.ok(cfg, 'chartWaterfall が描かれていない');
  assert.deepEqual(cfg.data.labels.map((l) => l[0]), ['売上', '原価', 'PF手数料', '広告費', '運賃', '資材費', '粗利'],
    '内訳が合っている月に差額の棒は出さない');
  assert.deepEqual(cfg.data.datasets[0].data, [
    [0, 1200],      // 売上
    [500, 1200],    // 原価 700 を引く
    [380, 500],     // PF手数料 120
    [320, 380],     // 広告費 60
    [230, 320],     // 運賃 90
    [200, 230],     // 資材費 30
    [0, 200],       // 粗利 (0 から積み直す合計の棒)
  ]);
  assert.deepEqual(cfg.data.labels[1], ['原価', '−700', '58.3%'],
    '棒の下に 名前 / 金額 / 売上に対する割合 を出す（ホバーしないと読めないのでは会議で使えない）');
  assert.deepEqual(cfg.data.labels[0], ['売上', '1,200', ''], '売上は割合を出さない');
  assert.equal(cfg.data.datasets[0].amounts[1], -700, 'tooltip 用は符号つき');
});

test('滝グラフ: 費目を引いても粗利に届かない月は差額の棒を出す', async () => {
  clearMonths();
  // 売上 1000 に対し 費目 700 + 粗利 100 = 800 → 差額 200 (過去の初期データにある形)
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1000, 600, 100, 0, 0, 0, 100]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartWaterfall');
  const labels = cfg.data.labels.map((l) => l[0]);
  assert.deepEqual(labels, ['売上', '原価', 'PF手数料', '広告費', '運賃', '資材費', '差額', '粗利']);
  const i = labels.indexOf('差額');
  assert.deepEqual(cfg.data.datasets[0].data[i], [100, 300], '300 から 200 引いて 100 (= 粗利) に着地');
  assert.deepEqual(cfg.data.labels[i], ['差額', '−200', '20.0%']);
  assert.deepEqual(cfg.data.datasets[0].data[labels.length - 1], [0, 100], '最後は粗利 100');
});

test('滝グラフ: 粗利がマイナスの月は最後の棒を赤にする', async () => {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1000, 700, 100, 50, 80, 120, -50]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartWaterfall');
  const colors = cfg.data.datasets[0].backgroundColor;
  assert.equal(colors[colors.length - 1], '#d93025', 'マイナスの粗利を緑で描かない');
  assert.deepEqual(cfg.data.datasets[0].data[cfg.data.labels.length - 1], [0, -50]);
  assert.deepEqual(cfg.data.labels[cfg.data.labels.length - 1], ['粗利', '-50', '-5.0%'],
    '赤字の月に 5.0% と出すと、見出しの −5.0% と食い違う');
  const i2 = cfg.data.labels.findIndex((l) => l[0] === '原価');
  assert.equal(cfg.data.labels[i2][2], '70.0%', '費目の割合は「売上の何%を持っていかれたか」なので絶対値のまま');
});

test('滝グラフ: 売上0の月は率を出さず、金額だけ出す', async () => {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 0, 0, 0, 0, 0, 0, 0]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  assert.match(page.el('waterfallInfo').textContent, /売上が0なので率は出せない/);
  assert.ok(lastChart(page.charts, 'chartWaterfall'), '率が出せなくても棒は描く');
});

test('滝グラフ: 表示期間を変えても、見ていた月が残っていればそのまま', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  page.el('waterfallMonth').value = '2025-07';
  page.api.renderWaterfallChart();
  assert.match(page.el('waterfallInfo').textContent, /2025-07/);

  // 期間を直近1ヶ月に絞る → 2025-07 は範囲外になるので、いちばん新しい月に戻る
  page.setResponse(callHistorical({ months: '1' }));
  await page.api.loadHistorical();
  assert.equal(page.el('waterfallMonth').value, '2026-08');
  assert.match(page.el('waterfallInfo').textContent, /2026-08/);
});

test('滝グラフ: 費目がマイナス（返金など）の月は「＋」で出す', async () => {
  clearMonths();
  // 広告費が −100 (返金)。残高は増えるので、ラベルも ＋ でなければ意味が逆になる
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1000, 600, 100, -100, 0, 0, 400]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartWaterfall');
  const i = cfg.data.labels.findIndex((l) => l[0] === '広告費');
  assert.deepEqual(cfg.data.labels[i], ['広告費', '+100', '10.0%'], '−-100 と出してはいけない');
  assert.deepEqual(cfg.data.datasets[0].data[i], [300, 400], '残高は 300 → 400 に増える');
  assert.equal(cfg.data.datasets[0].amounts[i], 100, 'tooltip 用も符号を合わせる');
});

test('滝グラフ: 読み直しても、期間内に残っている選択月はそのまま（最新に戻さない）', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  page.el('waterfallMonth').value = '2025-08'; // いちばん新しい月ではない月を選ぶ
  page.api.renderWaterfallChart();

  // 同じ期間のまま読み直す（確定の取り消しなどで再取得が走る場面）
  page.setResponse(callHistorical());
  await page.api.loadHistorical();
  assert.equal(page.el('waterfallMonth').value, '2025-08', '読み直すたびに最新へ戻ってはいけない');
  assert.match(page.el('waterfallInfo').textContent, /2025-08/);
});

test('滝グラフ: 選んでいた月だけ確定が外れたら、いちばん新しい月に戻る', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  page.el('waterfallMonth').value = '2025-08';
  page.api.renderWaterfallChart();

  db.prepare("UPDATE mgmt_monthly_closing SET status = 'needs_review' WHERE year_month = '2025-08'").run();
  page.setResponse(callHistorical());
  await page.api.loadHistorical();
  assert.deepEqual(page.el('waterfallMonth').children.map((o) => o.value), ['2026-08', '2026-07', '2025-07']);
  assert.equal(page.el('waterfallMonth').value, '2026-08');
});

test('滝グラフ: 確定月が無くなったら、プルダウンを空にしてグラフも消す', async () => {
  putBaseMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  const drawn = page.charts.length;

  clearMonths();
  page.setResponse(callHistorical());
  await page.api.loadHistorical();
  assert.deepEqual(page.el('waterfallMonth').children, [], '選べる月を残さない');
  assert.equal(page.el('waterfallMonth').value, '');
  assert.equal(page.el('waterfallInfo').textContent, 'データがありません');
  assert.ok(page.destroyed.includes('chartWaterfall'), '前の月のグラフが残ると今の話として読まれる');
  assert.equal(page.charts.length, drawn);
});

// ─── 6. モール別の粗利率 ───

// 2026-07: 楽天 1500/250 = 16.7% / Amazon 2000/300 = 15.0%、2026-08: 楽天は売上0 / Amazon 1000/100 = 10%
function putMallMarginMonths() {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [
    ['rakuten', 1, 1000, 600, 100, 50, 30, 20, 200],
    ['rakuten', 3, 500, 350, 50, 20, 20, 10, 50],   // 同じモールの別セグメント → 畳んで 1 本の線
    ['amazon', 1, 2000, 1300, 200, 100, 70, 30, 300],
  ]);
  putMonth('2026-08', 9, 2, 'confirmed', [
    ['rakuten', 1, 0, 0, 0, 0, 0, 0, 0],            // 売上0 = 率が出せない月
    ['amazon', 1, 1000, 700, 100, 50, 30, 20, 100],
  ]);
}

test('/api/historical: モール別の売上と粗利を、セグメントを畳んで返す', () => {
  putMallMarginMonths();
  const { plByMall } = callHistorical();
  const jul = plByMall.filter((r) => r.year_month === '2026-07');
  const rak = jul.find((r) => r.mall_id === 'rakuten');
  assert.equal(rak.sales, 1500, '同じモールの複数セグメントを足す');
  assert.equal(rak.gross_profit, 250);
  assert.equal(jul.find((r) => r.mall_id === 'amazon').sales, 2000);
  assert.equal(plByMall.filter((r) => r.year_month === '2026-08').length, 2);
});

test('モール別の粗利率: 売上の大きいモールから並べ、全体の線を太く重ねる', async () => {
  putMallMarginMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartMallMargin');
  assert.ok(cfg, 'chartMallMargin が描かれていない');
  assert.deepEqual(cfg.data.datasets.map((d) => d.label), ['全体', 'amazon', '楽天'],
    '全体を先頭 (= いちばん手前) に置き、あとは期間の売上が大きい順（Amazon 3000 > 楽天 1500）');

  const i = cfg.data.labels.indexOf('2026-07');
  const rak = cfg.data.datasets.find((d) => d.label === '楽天');
  const total = cfg.data.datasets.find((d) => d.label === '全体');
  assert.ok(Math.abs(rak.data[i] - 250 / 1500 * 100) < 1e-9, '楽天 250/1500');
  assert.ok(Math.abs(total.data[i] - 550 / 3500 * 100) < 1e-9, '全体は全モールの合計から出す');
  assert.equal(total.borderWidth, 4, '全体の線を太くする');
  assert.equal(rak.salesRow[i], 1500, '率だけで判断しないよう tooltip 用に売上額も持つ');
});

test('モール別の粗利率: 売上0の月は 0% ではなく線を途切れさせる', async () => {
  putMallMarginMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartMallMargin');
  const j = cfg.data.labels.indexOf('2026-08');
  const rak = cfg.data.datasets.find((d) => d.label === '楽天');
  assert.equal(rak.data[j], null, '0% と描くと「粗利が消えた」に見える');
  assert.equal(rak.salesRow[j], 0, '売上が 0 だったことは持っておく');
  const amz = cfg.data.datasets.find((d) => d.label === 'amazon');
  assert.equal(amz.data[j], 10, 'Amazon 100/1000 = 10%');
});

test('モール別の粗利率: データが無い期間は描かない', async () => {
  clearMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  assert.equal(lastChart(page.charts, 'chartMallMargin'), null);
  assert.equal(page.el('mallMarginInfo').textContent, 'データがありません');
});

test('モール別の粗利率: 期間を変えて順位が入れ替わっても、モールの色は変わらない', async () => {
  clearMonths();
  // 全期間では楽天が上 (5500 > 4000)、直近1ヶ月では Amazon が上 (3000 > 500) = 順位が入れ替わる
  putMonth('2026-07', 9, 1, 'confirmed', [
    ['rakuten', 1, 5000, 3400, 500, 250, 150, 100, 600],
    ['amazon_jp', 1, 1000, 700, 100, 50, 30, 20, 100],
  ]);
  putMonth('2026-08', 9, 2, 'confirmed', [
    ['rakuten', 1, 500, 350, 50, 25, 15, 10, 50],
    ['amazon_jp', 1, 3000, 2100, 300, 150, 90, 60, 300],
  ]);
  const page = loadPage(callHistorical());

  await page.api.loadHistorical(); // 全期間 → 楽天 5500 / Amazon 4000
  const wide = lastChart(page.charts, 'chartMallMargin');
  const colorWide = Object.fromEntries(wide.data.datasets.map((d) => [d.label, d.borderColor]));
  assert.deepEqual(wide.data.datasets.map((d) => d.label), ['全体', '楽天', 'Amazon'], '全期間は楽天が上');

  page.el('histMonths').value = '1';
  page.setResponse(callHistorical({ months: '1' })); // 直近1ヶ月 → Amazon が上に入れ替わる
  await page.api.loadHistorical();
  const narrow = lastChart(page.charts, 'chartMallMargin');
  const colorNarrow = Object.fromEntries(narrow.data.datasets.map((d) => [d.label, d.borderColor]));
  assert.deepEqual(narrow.data.datasets.map((d) => d.label), ['全体', 'Amazon', '楽天'], '並びは入れ替わる');

  assert.equal(colorNarrow['楽天'], colorWide['楽天'], '期間を切り替えると別モールを同じ色で追ってしまう');
  assert.equal(colorNarrow['Amazon'], colorWide['Amazon']);
  assert.notEqual(colorWide['楽天'], colorWide['Amazon'], 'モール同士は違う色');
});

test('モール別の粗利率: 線が途切れている理由（売上0 / 集計なし）を画面に出す', async () => {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [
    ['rakuten', 1, 1000, 600, 100, 50, 30, 20, 200],
    ['amazon_jp', 1, 0, 0, 0, 0, 0, 0, 0],        // 売上0 = 率を出せない
  ]);
  putMonth('2026-08', 9, 2, 'confirmed', [
    ['rakuten', 1, 1000, 600, 100, 50, 30, 20, 200], // Amazon はこの月の行が無い
  ]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const info = page.el('mallMarginInfo').textContent;
  assert.match(info, /売上0で率を出せない 1件/);
  assert.match(info, /その月に集計が無い 1件/, '売上0と行が無いのを混ぜない');
});

test('モール別の粗利率: 粗利がマイナスの月は、マイナスの率として出す', async () => {
  clearMonths();
  putMonth('2026-07', 9, 1, 'confirmed', [['rakuten', 1, 1000, 800, 150, 50, 40, 10, -50]]);
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();

  const cfg = lastChart(page.charts, 'chartMallMargin');
  const rak = cfg.data.datasets.find((d) => d.label === '楽天');
  assert.equal(rak.data[0], -5, '赤字の月を隠さない');
});

test('モール別の粗利率: データが無くなったら、同じ画面で前のグラフを消す', async () => {
  putMallMarginMonths();
  const page = loadPage(callHistorical());
  await page.api.loadHistorical();
  const drawn = page.charts.length;

  clearMonths();
  page.setResponse(callHistorical());
  await page.api.loadHistorical();
  assert.equal(page.charts.length, drawn, '新しくは描かない');
  assert.ok(page.destroyed.includes('chartMallMargin'), '前のモールの線が残ると今の話として読まれる');
  assert.equal(page.el('mallMarginInfo').textContent, 'データがありません');
});
