/**
 * test-tax-rate.mjs — 消費税率 → 税込換算係数の回帰テスト
 *
 * 2026-09-07: `1 + (消費税率 || 10) / 100` が mirror_products の「小数」表記 (0.1 / 0.08) を
 * 整数 (10 / 8) と誤認していたため、税込原価が 1.001 倍にしかならず粗利が過大に出ていた。
 * 同じ取り違えを二度としないための固定テスト。
 *
 * 実行: node apps/profit-analysis/test-tax-rate.mjs
 */
import assert from 'node:assert/strict';
import { taxMultiplier, TAX_RATE_FALLBACK } from './router.js';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

console.log('taxMultiplier (mirror_products.消費税率 は小数)');

t('標準税率 0.1 → 1.1', () => {
  assert.equal(taxMultiplier(0.1), 1.1);
});

t('軽減税率 0.08 → 1.08', () => {
  // 浮動小数の誤差を許容 (1.08 ちょうどにならない環境がある)
  assert.ok(Math.abs(taxMultiplier(0.08) - 1.08) < 1e-9);
});

t('null (NE未登録) → fallback 10%', () => {
  assert.equal(taxMultiplier(null), 1 + TAX_RATE_FALLBACK);
});

t('undefined → fallback 10%', () => {
  assert.equal(taxMultiplier(undefined), 1 + TAX_RATE_FALLBACK);
});

t('0 (NE未登録) → fallback 10%', () => {
  assert.equal(taxMultiplier(0), 1 + TAX_RATE_FALLBACK);
});

t('負の値 → fallback 10%', () => {
  assert.equal(taxMultiplier(-0.1), 1 + TAX_RATE_FALLBACK);
});

t('🚨 整数 10 が来たら 11倍にせず fallback する', () => {
  // 万一 mirror の表記が整数へ変わっても、原価を 11 倍にして粗利を大赤字に見せない
  assert.equal(taxMultiplier(10), 1 + TAX_RATE_FALLBACK);
});

t('🚨 整数 8 が来ても fallback する', () => {
  assert.equal(taxMultiplier(8), 1 + TAX_RATE_FALLBACK);
});

t('境界: 1 は単位が怪しいので fallback', () => {
  assert.equal(taxMultiplier(1), 1 + TAX_RATE_FALLBACK);
});

t('🚨 旧実装 (1 + rate/100) の値を返さない', () => {
  // 旧: 1 + 0.1/100 = 1.001 → 原価がほぼ税抜のままだった
  assert.notEqual(taxMultiplier(0.1), 1.001);
  assert.ok(taxMultiplier(0.1) > 1.09);
});

t('原価への反映: 税抜600円 → 税込660円', () => {
  assert.equal(Math.round(600 * taxMultiplier(0.1)), 660);
});

t('原価への反映: 軽減税率 税抜600円 → 税込648円', () => {
  assert.equal(Math.round(600 * taxMultiplier(0.08)), 648);
});

console.log(`\n${passed} 件 PASS`);
