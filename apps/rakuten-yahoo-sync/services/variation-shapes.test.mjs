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

// ── R11: options/subcodes 自動構築 (Yahoo 正式書式) ──
import { buildVariationOptionFields } from './variation-resolver.js';

test('R11: 単一軸の options/subcodes を正式書式で構築', () => {
  const r = buildVariationOptionFields(RMS_ITEM);
  assert.equal(r.ok, true);
  assert.equal(r.options, '香り#アロマベルガモット,ローズ');
  assert.equal(r.subcodes, '香り:アロマベルガモット=aromamist20-am|香り:ローズ=aromamist20-ro');
});

test('R11: 半角スペースは全角に置換して救済', () => {
  const item = {
    variantSelectors: [{ key: 'K', displayName: '香り', values: [{ displayValue: 'ホワイト ムスク' }] }],
    variants: { a: { merchantDefinedSkuId: 'x-wm', selectorValues: { K: 'ホワイト ムスク' } } },
  };
  const r = buildVariationOptionFields(item);
  assert.equal(r.ok, true);
  assert.equal(r.options, '香り#ホワイト　ムスク');
  assert.equal(r.subcodes, '香り:ホワイト　ムスク=x-wm');
});

test('R11: 禁止文字 (#等) を含む選択肢は fail-closed', () => {
  const item = {
    variantSelectors: [{ key: 'K', displayName: '香り', values: [] }],
    variants: { a: { merchantDefinedSkuId: 'x-1', selectorValues: { K: 'A#B' } } },
  };
  const r = buildVariationOptionFields(item);
  assert.equal(r.ok, false);
  assert.ok(r.errors.some((e) => e.startsWith('variation_option_invalid_char')));
});

test('R11: 選択肢値の欠落 variant があれば fail-closed (部分登録しない)', () => {
  const item = {
    variantSelectors: [{ key: 'K', displayName: '香り', values: [] }],
    variants: {
      a: { merchantDefinedSkuId: 'x-1', selectorValues: { K: 'ローズ' } },
      b: { merchantDefinedSkuId: 'x-2', selectorValues: {} },
    },
  };
  const r = buildVariationOptionFields(item);
  assert.equal(r.ok, false);
  assert.ok(r.errors.some((e) => e.startsWith('variation_option_value_missing:x-2')));
});

test('R11: 2軸で全組み合わせが揃えば OK、歯抜けは fail-closed', () => {
  const mk = (rows) => ({
    variantSelectors: [
      { key: 'C', displayName: 'カラー', values: [{ displayValue: '黒' }, { displayValue: '白' }] },
      { key: 'S', displayName: 'サイズ', values: [{ displayValue: 'S' }, { displayValue: 'M' }] },
    ],
    variants: Object.fromEntries(rows.map(([id, c, s]) => [id, { merchantDefinedSkuId: id, selectorValues: { C: c, S: s } }])),
  });
  const full = buildVariationOptionFields(mk([['a-1', '黒', 'S'], ['a-2', '黒', 'M'], ['a-3', '白', 'S'], ['a-4', '白', 'M']]));
  assert.equal(full.ok, true);
  assert.equal(full.options, 'カラー#黒,白|サイズ#S,M');
  assert.ok(full.subcodes.includes('カラー:黒#サイズ:S=a-1'));
  const sparse = buildVariationOptionFields(mk([['a-1', '黒', 'S'], ['a-2', '黒', 'M'], ['a-3', '白', 'S']]));
  assert.equal(sparse.ok, false);
  assert.ok(sparse.errors.some((e) => e.startsWith('variation_combos_incomplete:3/4')));
});

test('R11: resolveVariation が optionFields を含み、readiness がそれで判定できる', () => {
  const r = resolveVariation({ rakutenItem: RMS_ITEM, notionHasVariation: 'バリエーション登録あり' });
  assert.equal(r.optionFields.ok, true);
  assert.match(r.optionFields.subcodes, /=aromamist20-am/);
});

test('R11 Codex反映: 重複組み合わせは件数が合っていても fail-closed', () => {
  const item = {
    variantSelectors: [
      { key: 'C', displayName: 'カラー', values: [{ displayValue: '黒' }, { displayValue: '白' }] },
      { key: 'S', displayName: 'サイズ', values: [{ displayValue: 'S' }, { displayValue: 'M' }] },
    ],
    variants: {
      a: { merchantDefinedSkuId: 'a-1', selectorValues: { C: '黒', S: 'S' } },
      b: { merchantDefinedSkuId: 'a-2', selectorValues: { C: '黒', S: 'S' } }, // 重複 (白-M 欠落なのに件数4)
      c: { merchantDefinedSkuId: 'a-3', selectorValues: { C: '黒', S: 'M' } },
      d: { merchantDefinedSkuId: 'a-4', selectorValues: { C: '白', S: 'S' } },
    },
  };
  const r = buildVariationOptionFields(item);
  assert.equal(r.ok, false);
  assert.ok(r.errors.some((e) => e.startsWith('variation_combo_duplicate:黒#S')));
});

test('R11 Codex反映: 単一軸でも同一選択肢の重複SKUは fail-closed', () => {
  const item = {
    variantSelectors: [{ key: 'K', displayName: '香り', values: [] }],
    variants: {
      a: { merchantDefinedSkuId: 'x-1', selectorValues: { K: 'ローズ' } },
      b: { merchantDefinedSkuId: 'x-2', selectorValues: { K: 'ローズ' } },
    },
  };
  const r = buildVariationOptionFields(item);
  assert.equal(r.ok, false);
  assert.ok(r.errors.some((e) => e.startsWith('variation_combo_duplicate')));
});

test('R13: condition は常に 2 (新品)。1=中古で it-14196 になる (実機確定)', () => {
  const vari = resolveVariation({ rakutenItem: RMS_ITEM, notionHasVariation: 'バリエーション登録あり' });
  assert.equal(vari.condition, 2);
  const single = resolveVariation({ rakutenItem: { itemNumber: 's', variants: { a: { merchantDefinedSkuId: 's-a' } } }, notionHasVariation: null });
  assert.equal(single.condition, 2);
});
