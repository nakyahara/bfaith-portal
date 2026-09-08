/**
 * test-view.mjs — 画面の描画テスト
 *
 * 🚨 memory の教訓 (feedback_screen_test_green_but_verifies_nothing):
 *    EJS を描画するだけのテストは「router が値を渡し忘れている」のを検知しない。
 *    ここでは「描画できること」に加えて、**タブが実際に出ること**と
 *    **XSS 対策 (inline onclick を使っていないこと)** を固定する。
 *
 * 実行: node apps/expected-profit/test-view.mjs
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import ejs from 'ejs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const VIEW = path.join(__dirname, '../../views/profit-analysis.ejs');

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

const render = (locals) => ejs.render(fs.readFileSync(VIEW, 'utf8'), locals, { filename: VIEW });

// router.js の res.render と同じ locals を使う
// (ここがズレると「描画は通るが本番で落ちる」テストになる)
const baseLocals = {
  title: '商品収益性ダッシュボード',
  username: 'test@example.com',
  displayName: 'テスト',
  featureFlagEnabled: false,
};

console.log('描画');

let html;
t('EJS が描画できる', () => {
  html = render(baseLocals);
  assert.ok(html.length > 1000);
});

t('[!] 「想定利益 (単品)」タブが出る', () => {
  assert.ok(html.includes('想定利益 (単品)'), 'サブタブが無い');
  assert.ok(html.includes("data-view=\"expected\""), 'data-view が無い');
});

t('[!] 前提 (含んでいない費用) が画面に書かれている', () => {
  // 「これだけ手元に残る」と誤解させないための表示 (§2 / §9.1)
  assert.ok(html.includes('広告費'), '未計上費目の説明が無い');
  assert.ok(html.includes('ポイント原資'));
  assert.ok(html.includes('長期保管手数料'), 'FBA の未計上費目が無い');
});

t('[!] Amazon の手数料が「見積」だと書かれている', () => {
  assert.ok(html.includes('見積'), '見積である旨が無い');
  assert.ok(html.includes('確定請求額ではありません'));
});

t('[!] FBA と自社出荷を混ぜない旨が書かれている', () => {
  assert.ok(html.includes('同じ順位表に混ぜていません'));
});

t('[!] 標準シナリオが書かれている', () => {
  assert.ok(html.includes('1注文・1個・同梱なし'));
});

console.log('\nXSS 対策 (§9.5)');

t('[!] 想定利益タブのイベントを inline onclick で書いていない', () => {
  // 品番に ' が含まれると inline onclick は壊れる。addEventListener で結ぶこと
  const section = html.slice(html.indexOf('function renderExpectedProfit'), html.indexOf('function bindExpectedProfitEvents'));
  assert.ok(!/onclick="/.test(section), 'renderExpectedProfit が inline onclick を出している');
  assert.ok(!/onchange="/.test(section), 'renderExpectedProfit が inline onchange を出している');
});

t('[!] 動的な文字列を escapeHtml に通している', () => {
  const section = html.slice(html.indexOf('function renderExpectedProfit'), html.indexOf('function bindExpectedProfitEvents'));
  // 商品名・出品コードは必ず escape する
  assert.ok(section.includes('escapeHtml(r.mall_item_key)'));
  assert.ok(section.includes("escapeHtml(r.product_name || '')"));
});

t('addEventListener で結んでいる', () => {
  assert.ok(html.includes("addEventListener('change'"));
  assert.ok(html.includes("addEventListener('click'"));
});

console.log('\nタブBのフラグ (既存の Dark Launch を壊していない)');

t('flag OFF ではタブB が出ない', () => {
  const off = render({ ...baseLocals, featureFlagEnabled: false });
  assert.ok(!off.includes('在庫整理・撤退判断支援'), 'flag OFF なのにタブB が出ている');
});

t('flag ON ではタブB が出て、想定利益タブも共存する', () => {
  const on = render({ ...baseLocals, featureFlagEnabled: true });
  assert.ok(on.includes('在庫整理・撤退判断支援'));
  assert.ok(on.includes('想定利益 (単品)'));
});


console.log('');
console.log('経費の内訳の表 (中原さん 2026-09-08「すべての経費と配送方法をちゃんと表示」)');

// 🚨 画面の表はブラウザ側の JS が文字列で組み立てている。EJS を描くだけでは中身を見られない。
//    関数を切り出して**実際に動かし**、列の数と colspan が合っているかを確かめる
//    (colspan の数え間違いは目で見ないと分からず、表が1列ずれる)
function renderTable(rows, scope) {
  const src = html.match(/function renderExpectedProfit\(\)[\s\S]*?\n    \}/);
  assert.ok(src, '画面から renderExpectedProfit を切り出せない');
  const sandbox = {
    epState: { rows, total: rows.length, scope, published: { built_at: '2026-09-08T00:00:00Z' }, summary: {} },
    escapeHtml: (v) => String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
    epNum: (v) => (v == null ? '' : String(Math.round(v))),
    epPct: (v) => (v == null ? '' : (v * 100).toFixed(1) + '%'),
    epReason: (c) => String(c || ''),
    MALL_FEE_RATES_LABEL: { amazon: 'Amazon', rakuten: '楽天' },
    bindExpectedProfitEvents: () => {},
    document: { getElementById: () => container },
  };
  const container = { innerHTML: '' };
  const fn = new Function(...Object.keys(sandbox), src[0] + '; return renderExpectedProfit;')(...Object.values(sandbox));
  fn();
  return container.innerHTML;
}

const cells = (tr) => (tr.match(/<t[dh][^>]*>/g) || []).map(tag => {
  const m = tag.match(/colspan="(\d+)"/);
  return m ? Number(m[1]) : 1;
}).reduce((a, b) => a + b, 0);

const sampleRow = (over = {}) => ({
  mall: 'amazon', mall_item_key: 'sku1', product_name: '商品', fulfillment: 'FBM',
  price_ex_tax: 900, price_incl_tax: 990, postage_revenue_ex_tax: 0,
  cost_ex_tax: 300, unit_quantity: 1,
  shipping_method: 'ネコポス', shipping_code: '501',
  shipping_fee_ex_tax: 180, shipping_work_ex_tax: 20,
  shipping_material_ex_tax: 10, shipping_labor_ex_tax: 9, shipping_total_ex_tax: 219,
  fba_fee_ex_tax: null, referral_fee_ex_tax: 89, closing_fee_ex_tax: 0,
  per_item_fee_ex_tax: 0, fee_total_ex_tax: 89, fee_rate_display: 0.1,
  expected_profit: 292, expected_margin_rate: 0.324,
  rank_eligible_now: 1, rank_exclusion_reason_now: null, incomplete_reason: null,
  ...over,
});

t('[!] 自社配送の行の列数が見出しと一致する (colspan の数え間違いを防ぐ)', () => {
  const out = renderTable([sampleRow()], 'self_v1');
  const trs = out.match(/<tr[^>]*>[\s\S]*?<\/tr>/g) || [];
  assert.ok(trs.length >= 3, `見出し2行 + データ1行のはず (実際 ${trs.length})`);
  const group = cells(trs[0]);
  const head = cells(trs[1]);
  const body = cells(trs[2]);
  assert.equal(head, 24, `見出しは24列のはず (実際 ${head})`);
  assert.equal(group, head, `まとまりの見出しが合わない (${group} vs ${head})`);
  assert.equal(body, head, `データ行が合わない (${body} vs ${head})`);
});

t('[!] FBA の行も列数が一致する (Amazon が配送する行は colspan でまとめている)', () => {
  const out = renderTable([sampleRow({
    fulfillment: 'FBA', shipping_method: null, shipping_code: null,
    shipping_fee_ex_tax: null, shipping_work_ex_tax: null,
    shipping_material_ex_tax: null, shipping_labor_ex_tax: null, shipping_total_ex_tax: null,
    fba_fee_ex_tax: 462,
  })], 'fba_v1');
  const trs = out.match(/<tr[^>]*>[\s\S]*?<\/tr>/g) || [];
  assert.equal(cells(trs[2]), cells(trs[1]), 'FBA 行の列数が見出しと合わない');
});

t('[!] 配送方法と送料区分コードが実際に出る (中原さんの元の要望)', () => {
  const out = renderTable([sampleRow()], 'self_v1');
  assert.ok(out.includes('ネコポス'), '配送方法が出ていない');
  assert.ok(out.includes('>501<'), '送料区分コードが出ていない');
});

t('[!] 経費の内訳が1つずつ出る (合計だけにしない)', () => {
  const out = renderTable([sampleRow()], 'self_v1');
  for (const [name, v] of [['送料', '180'], ['出荷作業料', '20'], ['梱包資材費', '10'], ['人件費', '9'],
    ['販売手数料', '89']]) {
    assert.ok(out.includes('>' + v + '<'), `${name} (${v}) が出ていない`);
  }
});

t('見出しに経費の名前が全部ある', () => {
  const out = renderTable([sampleRow()], 'self_v1');
  for (const h of ['配送方法', '送料区分', '送料', '出荷作業料', '梱包資材費', '人件費',
    'FBA配送代行', '販売手数料', '成約料', '基本成約料', '手数料 合計']) {
    assert.ok(out.includes('>' + h + '<'), `見出し「${h}」が無い`);
  }
});

t('[!] 値が無いことと 0 円を見分けられる', () => {
  const out = renderTable([sampleRow({ closing_fee_ex_tax: 0, per_item_fee_ex_tax: null })], 'self_v1');
  assert.ok(out.includes('—'), '値が無い欄に — が出ていない');
});

console.log(`\n${passed} 件 PASS`);
