import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  truncateYahooTitle, convertTaxRate, pickRepresentativePrice,
  buildNotionDraftProposal, toNotionProperties, mapShippingToNotion,
} from './rakuten-to-notion-draft.js';

// ── truncateYahooTitle ─────────────────────────────
test('truncateYahooTitle: 65 字以内はそのまま', () => {
  assert.equal(truncateYahooTitle('短いタイトル'), '短いタイトル');
});

test('truncateYahooTitle: 65 字超は切る', () => {
  const long = 'あ'.repeat(70);
  const out = truncateYahooTitle(long);
  assert.equal(out.length, 65);
});

test('truncateYahooTitle: 空 / null 安全', () => {
  assert.equal(truncateYahooTitle(''), null);
  assert.equal(truncateYahooTitle(null), null);
  assert.equal(truncateYahooTitle(undefined), null);
  assert.equal(truncateYahooTitle(123), null);
});

// ── convertTaxRate ─────────────────────────────────
test('convertTaxRate: 0.1 → 10%', () => {
  assert.equal(convertTaxRate(0.1), '10%');
  assert.equal(convertTaxRate('0.1'), '10%');
});

test('convertTaxRate: 0.08 → 8 (notion_tax_rate CHECK 制約に合わせる)', () => {
  assert.equal(convertTaxRate(0.08), '8');
});

test('convertTaxRate: missing/不明値は店舗 default 10% (Codex Phase E-15 R1 H-1)', () => {
  // 楽天 RMS で payment.taxRate 省略 = 店舗 default 10%
  assert.equal(convertTaxRate(null), '10%');
  assert.equal(convertTaxRate(undefined), '10%');
  assert.equal(convertTaxRate(0), '10%');
  assert.equal(convertTaxRate(0.05), '10%');
  assert.equal(convertTaxRate('abc'), '10%');
});

// ── pickRepresentativePrice ───────────────────────
test('pickRepresentativePrice: 全 variant 同価格', () => {
  const item = { variants: { v1: { standardPrice: '880' } } };
  assert.equal(pickRepresentativePrice(item), 880);
});

test('pickRepresentativePrice: 複数 variant 最小値', () => {
  const item = { variants: { v1: { standardPrice: '800' }, v2: { standardPrice: '1000' } } };
  assert.equal(pickRepresentativePrice(item), 800);
});

test('pickRepresentativePrice: variants なしは null', () => {
  assert.equal(pickRepresentativePrice({}), null);
  assert.equal(pickRepresentativePrice(null), null);
});

test('pickRepresentativePrice: standardPrice 0/欠落はスキップ', () => {
  const item = { variants: { v1: {}, v2: { standardPrice: '0' }, v3: { standardPrice: '880' } } };
  assert.equal(pickRepresentativePrice(item), 880);
});

// ── buildNotionDraftProposal ──────────────────────
test('buildNotionDraftProposal: 全空欄なら全項目補完予定 (shippingMethodGroup あり)', () => {
  const rakutenItem = {
    title: '青森ひば ロールオン 10ml',
    variants: { v1: { standardPrice: '880', shipping: { shippingMethodGroup: '5' } } },
    payment: { taxRate: '0.1' },
  };
  const notion = {}; // 全部空
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, notion);
  assert.equal(proposed.yahoo_title, '青森ひば ロールオン 10ml');
  assert.equal(proposed.yahoo_price, 880);
  assert.equal(proposed.notion_tax_rate, '10%');
  assert.equal(proposed.notion_delivery_label, 'ネコポス'); // 楽天 ID 5 → ネコポス
  assert.equal(skipped.notion_delivery_label, undefined);
});

test('buildNotionDraftProposal: 既存値ある項目は skip', () => {
  const rakutenItem = {
    title: '楽天タイトル',
    variants: { v1: { standardPrice: '999', shipping: { shippingMethodGroup: '5' } } },
    payment: { taxRate: '0.1' },
  };
  const notion = {
    yahoo_title: '既存のタイトル', yahoo_price: 500, notion_tax_rate: '8%', notion_delivery_label: 'ネコポス',
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, notion);
  assert.equal(Object.keys(proposed).length, 0);
  assert.equal(skipped.yahoo_title, 'already_filled');
  assert.equal(skipped.yahoo_price, 'already_filled');
  assert.equal(skipped.notion_tax_rate, 'already_filled');
  assert.equal(skipped.notion_delivery_label, 'already_filled');
});

test('buildNotionDraftProposal: rakutenItem なし → 全 skip', () => {
  const { proposed, skipped } = buildNotionDraftProposal(null, {});
  assert.equal(Object.keys(proposed).length, 0);
  assert.equal(skipped._all, 'no_rakuten_item');
});

test('buildNotionDraftProposal: itemName 優先、 title は fallback (Codex R3 H-1)', () => {
  const rakutenItem = {
    itemName: 'itemName 由来',
    title: 'title 由来',
    variants: { v1: { standardPrice: '880', shipping: { shippingMethodGroup: '5' } } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.yahoo_title, 'itemName 由来');
});

test('buildNotionDraftProposal: itemName 欠落なら title fallback', () => {
  const rakutenItem = {
    title: 'title 由来',
    variants: { v1: { standardPrice: '880', shipping: { shippingMethodGroup: '5' } } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.yahoo_title, 'title 由来');
});

test('buildNotionDraftProposal: 65 字超は truncate', () => {
  const rakutenItem = {
    title: 'あ'.repeat(100),
    variants: { v1: { standardPrice: '880', shipping: { shippingMethodGroup: '5' } } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.yahoo_title.length, 65);
});

// ── mapShippingToNotion (Phase E-16: 楽天 shippingMethodGroup ID → Notion) ─────────────
test('mapShippingToNotion: 楽天 ID 5 (ネコポス) → ネコポス', () => {
  assert.equal(mapShippingToNotion('5'), 'ネコポス');
  assert.equal(mapShippingToNotion(5), 'ネコポス');
});
test('mapShippingToNotion: 楽天 ID 7 (ヤマト運輸宅急便) → ヤマト宅急便', () => {
  assert.equal(mapShippingToNotion('7'), 'ヤマト宅急便');
});
test('mapShippingToNotion: 楽天 ID 8 (宅急便50サイズ以上) → ヤマト50サイズ', () => {
  assert.equal(mapShippingToNotion('8'), 'ヤマト50サイズ');
});
test('mapShippingToNotion: 楽天 ID 9 (ゆうパケットパフ) → ゆうパケットパフ', () => {
  assert.equal(mapShippingToNotion('9'), 'ゆうパケットパフ');
});
test('mapShippingToNotion: 楽天 ID 2/6 (クリックポスト系) → クリックポスト', () => {
  assert.equal(mapShippingToNotion('2'), 'クリックポスト');
  assert.equal(mapShippingToNotion('6'), 'クリックポスト');
});
test('mapShippingToNotion: 楽天 ID 1 (定形外) は判別不能 → null (Notion 8 値の どちら にも振れない)', () => {
  // 定形外（ヤフーのみ宅急便50） or 定形外（ヤフーのみネコポス） どちらか不明
  assert.equal(mapShippingToNotion('1'), null);
});
test('mapShippingToNotion: 楽天 ID 3 (飛脚宅配便) は Notion 8 値に対応無し → null', () => {
  assert.equal(mapShippingToNotion('3'), null);
});
test('mapShippingToNotion: 楽天 ID 4 (宅急便 / Yahoo デフォルト設定) は Notion 8 値に対応無し → null', () => {
  assert.equal(mapShippingToNotion('4'), null);
});
test('mapShippingToNotion: 不明な ID は null', () => {
  assert.equal(mapShippingToNotion('99'), null);
  assert.equal(mapShippingToNotion('0'), null);
});
test('mapShippingToNotion: null/undefined/空文字 safe', () => {
  assert.equal(mapShippingToNotion(null), null);
  assert.equal(mapShippingToNotion(undefined), null);
  assert.equal(mapShippingToNotion(''), null);
  assert.equal(mapShippingToNotion('  '), null);
});

// ── toNotionProperties ────────────────────────────
test('toNotionProperties: Notion API 形式', () => {
  const out = toNotionProperties({ yahoo_title: 'タイトル', yahoo_price: 880, notion_tax_rate: '10%' });
  assert.deepEqual(out['Yahoo!タイトル'], {
    rich_text: [{ type: 'text', text: { content: 'タイトル' } }],
  });
  assert.deepEqual(out['売価'], { number: 880 });
  assert.deepEqual(out['税率'], { select: { name: '10%' } });
});

test('toNotionProperties: 空 proposed → 空 properties', () => {
  assert.deepEqual(toNotionProperties({}), {});
});

test('toNotionProperties: notion_delivery_label → 配送方法 select (Phase E-15)', () => {
  const out = toNotionProperties({ notion_delivery_label: 'ネコポス' });
  assert.deepEqual(out['配送方法'], { select: { name: 'ネコポス' } });
});

// ── buildNotionDraftProposal: 配送方法 lookup (Phase E-16: 楽天 shippingMethodGroup) ────────
test('buildNotionDraftProposal: variants[].shipping.shippingMethodGroup から配送方法補完', () => {
  const rakutenItem = {
    manageNumber: 'aigeshou5set',
    title: 'お試し',
    variants: { v1: { standardPrice: '880', shipping: { shippingMethodGroup: '5' } } },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.notion_delivery_label, 'ネコポス');
  assert.equal(skipped.notion_delivery_label, undefined);
});

test('buildNotionDraftProposal: variants なしは rakuten_variants_missing', () => {
  const rakutenItem = {
    manageNumber: 'x',
    title: 't',
    payment: { taxRate: '0.1' },
  };
  const { skipped } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(skipped.notion_delivery_label, 'rakuten_variants_missing');
});

test('buildNotionDraftProposal: shippingMethodGroup 全 variant 欠落なら rakuten_shipping_method_group_missing', () => {
  const rakutenItem = {
    manageNumber: 'x',
    title: 't',
    variants: { v1: { standardPrice: '100' } }, // shipping 無し
    payment: { taxRate: '0.1' },
  };
  const { skipped } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(skipped.notion_delivery_label, 'rakuten_shipping_method_group_missing');
});

test('buildNotionDraftProposal: variant 別 distinct label が複数なら ambiguous_rakuten_shipping_group', () => {
  const rakutenItem = {
    manageNumber: 'a',
    title: 't',
    variants: {
      v1: { standardPrice: '100', shipping: { shippingMethodGroup: '5' } },  // ネコポス
      v2: { standardPrice: '200', shipping: { shippingMethodGroup: '7' } },  // ヤマト宅急便
    },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.notion_delivery_label, undefined);
  assert.match(skipped.notion_delivery_label, /ambiguous_rakuten_shipping_group/);
});

test('buildNotionDraftProposal: 全 variant 同じ shipping ID なら distinct=1 で採用', () => {
  const rakutenItem = {
    manageNumber: 'a',
    title: 't',
    variants: {
      v1: { standardPrice: '100', shipping: { shippingMethodGroup: '5' } },
      v2: { standardPrice: '200', shipping: { shippingMethodGroup: '5' } },
    },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.notion_delivery_label, 'ネコポス');
});

test('buildNotionDraftProposal: 楽天 ID 1 (定形外) は unmappable_rakuten_shipping_group', () => {
  const rakutenItem = {
    manageNumber: 'x',
    title: 't',
    variants: { v1: { standardPrice: '100', shipping: { shippingMethodGroup: '1' } } },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.notion_delivery_label, undefined);
  assert.match(skipped.notion_delivery_label, /unmappable_rakuten_shipping_group:1/);
});

test('buildNotionDraftProposal: 楽天 ID 3/4 (Notion 8 値に対応無し) は unmappable_rakuten_shipping_group', () => {
  for (const id of ['3', '4']) {
    const rakutenItem = {
      manageNumber: 'x',
      title: 't',
      variants: { v1: { standardPrice: '100', shipping: { shippingMethodGroup: id } } },
      payment: { taxRate: '0.1' },
    };
    const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {});
    assert.equal(proposed.notion_delivery_label, undefined);
    assert.match(skipped.notion_delivery_label, new RegExp(`unmappable_rakuten_shipping_group:${id}`));
  }
});

test('buildNotionDraftProposal: mapped ID + unmappable ID 混在 → mixed ambiguous (Codex R1 改善 diagnostics)', () => {
  const rakutenItem = {
    manageNumber: 'a',
    title: 't',
    variants: {
      v1: { standardPrice: '100', shipping: { shippingMethodGroup: '5' } },  // ネコポス (mapped)
      v2: { standardPrice: '200', shipping: { shippingMethodGroup: '1' } },  // 定形外 (unmappable)
    },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.notion_delivery_label, undefined);
  assert.match(skipped.notion_delivery_label, /ambiguous_rakuten_shipping_group/);
  assert.match(skipped.notion_delivery_label, /unmappable:1/);
});

test('buildNotionDraftProposal: 一部 variant だけ shippingMethodGroup 欠落でも残りで採用', () => {
  const rakutenItem = {
    manageNumber: 'a',
    title: 't',
    variants: {
      v1: { standardPrice: '100' }, // shipping 無し
      v2: { standardPrice: '200', shipping: { shippingMethodGroup: '5' } },
    },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {});
  // hasShippingGroup=true (v2 が持ってる)、 labels={ネコポス}、 unmappable=0 → 採用
  assert.equal(proposed.notion_delivery_label, 'ネコポス');
  assert.equal(skipped.notion_delivery_label, undefined);
});

test('buildNotionDraftProposal: 楽天 ID 数値型 (string でなく number) も safe', () => {
  const rakutenItem = {
    manageNumber: 'x',
    title: 't',
    variants: { v1: { standardPrice: '100', shipping: { shippingMethodGroup: 5 } } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.notion_delivery_label, 'ネコポス');
});
