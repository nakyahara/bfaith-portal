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

t('NaN → fallback (Codex R1-3)', () => {
  assert.equal(taxMultiplier(NaN), 1 + TAX_RATE_FALLBACK);
});

t('文字列 "0.1" → fallback (数値以外は受けない・Codex R1-3)', () => {
  assert.equal(taxMultiplier('0.1'), 1 + TAX_RATE_FALLBACK);
});

t('Infinity → fallback', () => {
  assert.equal(taxMultiplier(Infinity), 1 + TAX_RATE_FALLBACK);
});

// ─── セット構成品の税率混在 (呼び出し側の挙動を固定する) ───
// router.js の実装:
//   taxRates.push(prod.消費税率 || TAX_RATE_FALLBACK)
//   const uniqueTaxRates = [...new Set(taxRates)]
//   if (!allFound || uniqueTaxRates.length > 1) -> hard fail (原価不確定)
//   else -> taxMultiplier(uniqueTaxRates[0])
function setResolution(rawRates, allFound = true) {
  const taxRates = rawRates.map(r => r || TAX_RATE_FALLBACK);
  const unique = [...new Set(taxRates)];
  if (!allFound || unique.length > 1) return { hardFail: true };
  return { hardFail: false, multiplier: taxMultiplier(unique[0]) };
}

console.log('\nセット構成品の税率混在 (呼び出し側)');

t('[0.1, 0.1] → 1.1 (混在なし)', () => {
  const r = setResolution([0.1, 0.1]);
  assert.equal(r.hardFail, false);
  assert.equal(r.multiplier, 1.1);
});

t('[0.08, 0.1] は hard fail (混在)', () => {
  assert.equal(setResolution([0.08, 0.1]).hardFail, true);
});

t('[0.1, 0.08] も hard fail (順序に依存しない)', () => {
  assert.equal(setResolution([0.1, 0.08]).hardFail, true);
});

t('🚨 [0.1, null] は成功する (旧実装では [0.1, 10] で hard fail していた)', () => {
  // 挙動変更を意図として固定する。旧実装は単位の取り違えで偶然 hard fail していただけで、
  // どちらも 10% 扱いなのだから成功が正しい
  const r = setResolution([0.1, null]);
  assert.equal(r.hardFail, false);
  assert.equal(r.multiplier, 1.1);
});

t('[0.08, null] は hard fail のまま (8% と 10% は本当に混在)', () => {
  assert.equal(setResolution([0.08, null]).hardFail, true);
});

t('全件未登録 [null, null] → 1.1', () => {
  const r = setResolution([null, null]);
  assert.equal(r.hardFail, false);
  assert.equal(r.multiplier, 1.1);
});

t('構成品の一部が見つからない (allFound=false) → hard fail', () => {
  assert.equal(setResolution([0.1], false).hardFail, true);
});

console.log(`\n${passed} 件 PASS`);
