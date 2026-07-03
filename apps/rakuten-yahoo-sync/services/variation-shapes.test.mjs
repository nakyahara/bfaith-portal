/**
 * R10 (2026-07-03 中原さん指摘 + 実RMSデータ検証) の回帰防止:
 *   - 軸 (項目名) は RMS Items API 2.0 の item.variantSelectors から取る
 *   - Yahoo バリエーションコード = merchantDefinedSkuId (システム連携用SKU番号) そのまま
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/variation-shapes.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { extractRequiredAxes } from './readiness-check.js';
import { resolveVariation } from './variation-resolver.js';

// aromamist20 の実レスポンス構造を模した fixture (variants は SKU管理番号 key の object)
const RMS_ITEM = {
  itemNumber: 'aromamist20',
  variantSelectors: [{ key: 'Key0', displayName: '香り', values: [{ displayValue: 'アロマベルガモット' }, { displayValue: 'ローズ' }] }],
  variants: {
    'aromamist20-am': { merchantDefinedSkuId: 'aromamist20-am', selectorValues: { Key0: 'アロマベルガモット' }, standardPrice: '798' },
    'aromamist20-ro': { merchantDefinedSkuId: 'aromamist20-ro', selectorValues: { Key0: 'ローズ' }, standardPrice: '798' },
  },
};

test('軸: variantSelectors (RMS 2.0 実shape) から displayName を取る', () => {
  assert.deepEqual(extractRequiredAxes(RMS_ITEM), ['香り']);
});

test('軸: variantSelectors が単一 object でも取れる (JSON 変換で unwrap されるケース)', () => {
  const item = { ...RMS_ITEM, variantSelectors: { key: 'Key0', displayName: 'カラー', values: [] } };
  assert.deepEqual(extractRequiredAxes(item), ['カラー']);
});

test('軸: displayName が無ければ key を使う', () => {
  const item = { ...RMS_ITEM, variantSelectors: [{ key: 'Key0', values: [] }] };
  assert.deepEqual(extractRequiredAxes(item), ['Key0']);
});

test('軸: 旧shape (variants[].axis1Name) の fallback も維持', () => {
  const item = { variants: [{ axis1Name: 'サイズ' }, { axis1Name: 'サイズ' }] };
  assert.deepEqual(extractRequiredAxes(item), ['サイズ']);
});

test('subcode = merchantDefinedSkuId そのまま (商品番号の二重付与をしない)', () => {
  const r = resolveVariation({ rakutenItem: RMS_ITEM, notionHasVariation: 'バリエーション登録あり' });
  assert.equal(r.isVariation, true);
  assert.deepEqual(r.subcodes, ['aromamist20-am', 'aromamist20-ro']); // NOT aromamist20-aromamist20-am
  assert.deepEqual(r.subcodeErrors, []);
  assert.equal(r.conflict, null);
});

test('subcode: 全角など不正文字の merchantDefinedSkuId は弾いてエラー記録', () => {
  const item = {
    itemNumber: 'x',
    variants: {
      a: { merchantDefinedSkuId: 'ok-1' },
      b: { merchantDefinedSkuId: '不正な全角' },
    },
  };
  const r = resolveVariation({ rakutenItem: item, notionHasVariation: null });
  assert.deepEqual(r.subcodes, ['ok-1']);
  assert.ok(r.subcodeErrors.some((e) => e.startsWith('variant_sku_id_invalid_format')));
});

test('通常品 (variants 1 件) は subcodes 無し + auc 送る', () => {
  const item = { itemNumber: 'single1', variants: { s: { merchantDefinedSkuId: 'single1-a' } } };
  const r = resolveVariation({ rakutenItem: item, notionHasVariation: 'バリエーションなし' });
  assert.equal(r.isVariation, false);
  assert.equal(r.subcodes, null);
  assert.equal(r.sendAucFields, true);
});
