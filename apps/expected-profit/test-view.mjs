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

console.log(`\n${passed} 件 PASS`);
