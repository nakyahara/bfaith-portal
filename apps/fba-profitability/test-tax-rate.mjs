/**
 * test-tax-rate.mjs — 消費税率の単位変換 (小数 → %) の回帰テスト
 *
 * 2026-09-07: router が `prod.消費税率 ?? 10` をそのまま返していたため、
 * view の `1 + taxRate/100` が 1 + 0.1/100 = 1.001 にしかならず、税込原価がほぼ税抜のままだった。
 * (税率が未登録の商品だけ 10 が入って正しく計算される、という逆転が起きていた)
 *
 * 実行: node apps/fba-profitability/test-tax-rate.mjs
 */
import assert from 'node:assert/strict';
import { toPercent, TAX_PERCENT_FALLBACK } from './router.js';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

console.log('toPercent (mirror_products.消費税率 の小数 → 画面の %)');

t('0.1 → 10', () => assert.equal(toPercent(0.1), 10));
t('0.08 → 8', () => assert.ok(Math.abs(toPercent(0.08) - 8) < 1e-9));
t('null → fallback 10', () => assert.equal(toPercent(null), TAX_PERCENT_FALLBACK));
t('undefined → fallback 10', () => assert.equal(toPercent(undefined), TAX_PERCENT_FALLBACK));
t('0 → fallback 10', () => assert.equal(toPercent(0), TAX_PERCENT_FALLBACK));
t('NaN → fallback 10', () => assert.equal(toPercent(NaN), TAX_PERCENT_FALLBACK));
t('文字列 "0.1" → fallback 10', () => assert.equal(toPercent('0.1'), TAX_PERCENT_FALLBACK));
t('🚨 既に % の 10 が来ても 1000 にしない', () => assert.equal(toPercent(10), TAX_PERCENT_FALLBACK));

console.log('\nview の計算 (1 + taxRate/100) に通したときの税込原価');

const costWithTax = (cost, rate) => Math.ceil(cost * (1 + toPercent(rate) / 100));

t('税抜600円 / 0.1 → 660円', () => assert.equal(costWithTax(600, 0.1), 660));
t('税抜600円 / 0.08 → 648円', () => assert.equal(costWithTax(600, 0.08), 648));
t('税抜600円 / 未登録 → 660円 (10%扱い)', () => assert.equal(costWithTax(600, null), 660));
t('🚨 旧実装の 601円 (ほぼ税抜) にならない', () => {
  assert.notEqual(costWithTax(600, 0.1), 601);
});

console.log(`\n${passed} 件 PASS`);
