/**
 * R17 (2026-07-04 中原さん指示): 楽天キャッチコピー → Yahoo キャッチコピー (headline) 移行。
 *   - Notion キャッチコピー > 楽天 tagline (RMS 2.0) / catchcopy (旧 1.0) の優先順位
 *   - 楽天キャッチコピーの <br> は除去して詰める
 *   - headline は HTML 不可 + 全角 30 truncate + 1 行 (改行除去)
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/field-mapper-headline.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { buildYahooEditItemFields } from './field-mapper.js';
import { resolveVariation } from './variation-resolver.js';

function build({ tagline, catchcopy, notionHeadline } = {}) {
  const rakutenItem = {
    itemNumber: 'testitem1',
    title: 'テスト商品タイトル',
    salesDescription: '',
    productDescription: { pc: '<p>本文説明</p>', sp: '' },
    variants: { s: { merchantDefinedSkuId: 'testitem1-a', standardPrice: '1000' } },
  };
  if (tagline !== undefined) rakutenItem.tagline = tagline;
  if (catchcopy !== undefined) rakutenItem.catchcopy = catchcopy;
  const notionOverride = { yahoo_title: 'テスト商品', yahoo_price: 1000 };
  if (notionHeadline !== undefined) notionOverride.yahoo_headline = notionHeadline;
  const variationResult = resolveVariation({ rakutenItem, notionHasVariation: null });
  return buildYahooEditItemFields({ rakutenItem, notionOverride, deliveryRow: null, variationResult });
}

test('楽天 tagline が headline に移行される (<br> は除去して詰める)', () => {
  const fields = build({ tagline: '送料無料<br>ポイント10倍<BR/>即日発送' });
  assert.equal(fields.headline, '送料無料ポイント10倍即日発送');
});

test('Notion キャッチコピーがあれば楽天 tagline より優先', () => {
  const fields = build({ tagline: '楽天側コピー', notionHeadline: 'Notion側コピー' });
  assert.equal(fields.headline, 'Notion側コピー');
});

test('旧 field 名 catchcopy も fallback で読む', () => {
  const fields = build({ catchcopy: '旧APIコピー' });
  assert.equal(fields.headline, '旧APIコピー');
});

test('全角 30 字 (60 units) に truncate される', () => {
  const fields = build({ tagline: 'あ'.repeat(50) });
  assert.equal(fields.headline, 'あ'.repeat(30));
});

test('tagline も Notion も無ければ headline は送らない (従来通り)', () => {
  const fields = build({});
  assert.equal(fields.headline, undefined);
});

test('explanation の先頭にも解決済み headline が入る', () => {
  const fields = build({ tagline: '送料無料<br>ポイント10倍' });
  assert.ok(fields.explanation.startsWith('送料無料ポイント10倍'), fields.explanation);
});
