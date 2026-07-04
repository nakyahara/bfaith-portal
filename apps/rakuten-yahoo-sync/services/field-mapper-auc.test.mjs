/**
 * R15 (2026-07-04 中原さん指示): ヤフオク併売 (新品：Yahoo!オークション併売) 時の
 * PC用フリースペース1 (additional1) 必須対応。
 *   - auc_* を送る通常品で additional1 が空 → caption と同じ内容を入れる (最終処理)
 *   - バリ品 (auc 送らない) や additional1 に元々値がある場合は触らない
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/field-mapper-auc.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { buildYahooEditItemFields } from './field-mapper.js';
import { resolveVariation } from './variation-resolver.js';

const NOTION = { yahoo_title: 'テスト商品', yahoo_price: 1000, notion_tax_rate: '10%' };

function mkSingleItem({ salesDescription = '' } = {}) {
  return {
    itemNumber: 'testitem1',
    manageNumber: 'testitem1',
    title: 'テスト商品タイトル',
    salesDescription,
    productDescription: { pc: '<p>商品説明本文です</p>', sp: '' },
    variants: { s: { merchantDefinedSkuId: 'testitem1-a', standardPrice: '1000' } },
  };
}

function build(rakutenItem) {
  const variationResult = resolveVariation({ rakutenItem, notionHasVariation: null });
  return buildYahooEditItemFields({ rakutenItem, notionOverride: NOTION, deliveryRow: null, variationResult });
}

test('ヤフオク併売 (通常品) + additional1 空 → caption と同内容を補完', () => {
  const fields = build(mkSingleItem({ salesDescription: '' }));
  assert.ok(fields.auc_bcid, '通常品は auc_* を送る (=ヤフオク併売)');
  assert.ok(fields.additional1 && fields.additional1.length > 0, 'additional1 が補完される');
  assert.equal(fields.additional1, fields.caption || fields.explanation, '商品説明 (caption、無ければ explanation) と同じ内容');
});

test('ヤフオク併売 + additional1 に元々値あり → 上書きしない', () => {
  const fields = build(mkSingleItem({ salesDescription: '<p>元々のフリースペース</p>' }));
  assert.ok(fields.auc_bcid);
  assert.match(fields.additional1, /元々のフリースペース/, '元の値が保持される');
});

test('バリ品 (auc 送らない) → additional1 空のままでも補完しない', () => {
  const rakutenItem = {
    itemNumber: 'vitem1',
    title: 'バリ品',
    salesDescription: '',
    productDescription: { pc: '<p>説明</p>', sp: '' },
    variantSelectors: [{ key: 'K', displayName: '色', values: [{ displayValue: '赤' }, { displayValue: '青' }] }],
    variants: {
      a: { merchantDefinedSkuId: 'vitem1-r', selectorValues: { K: '赤' } },
      b: { merchantDefinedSkuId: 'vitem1-b', selectorValues: { K: '青' } },
    },
  };
  const variationResult = resolveVariation({ rakutenItem, notionHasVariation: 'バリエーション登録あり' });
  const fields = buildYahooEditItemFields({ rakutenItem, notionOverride: NOTION, deliveryRow: null, variationResult });
  assert.equal(fields.auc_bcid, undefined, 'バリ品は auc_* を送らない');
  assert.equal(fields.additional1, '', 'バリ品は補完対象外');
});
