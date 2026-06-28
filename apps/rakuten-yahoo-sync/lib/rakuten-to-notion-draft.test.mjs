import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

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
test('buildNotionDraftProposal: 全空欄なら全項目補完予定', () => {
  const rakutenItem = {
    title: '青森ひば ロールオン 10ml',
    variants: { v1: { standardPrice: '880' } },
    payment: { taxRate: '0.1' },
  };
  const notion = {}; // 全部空
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, notion);
  assert.equal(proposed.yahoo_title, '青森ひば ロールオン 10ml');
  assert.equal(proposed.yahoo_price, 880);
  assert.equal(proposed.notion_tax_rate, '10%');
  // Phase E-15 で warehouseDb 未指定なら warehouse_db_unavailable で skip
  assert.equal(skipped.notion_delivery_label, 'warehouse_db_unavailable');
});

test('buildNotionDraftProposal: 既存値ある項目は skip', () => {
  const rakutenItem = {
    title: '楽天タイトル',
    variants: { v1: { standardPrice: '999' } },
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
    variants: { v1: { standardPrice: '880' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.yahoo_title, 'itemName 由来');
});

test('buildNotionDraftProposal: itemName 欠落なら title fallback', () => {
  const rakutenItem = {
    title: 'title 由来',
    variants: { v1: { standardPrice: '880' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.yahoo_title, 'title 由来');
});

test('buildNotionDraftProposal: 65 字超は truncate', () => {
  const rakutenItem = {
    title: 'あ'.repeat(100),
    variants: { v1: { standardPrice: '880' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(proposed.yahoo_title.length, 65);
});

// ── mapShippingToNotion (Phase E-15) ─────────────
test('mapShippingToNotion: nekopos → ネコポス', () => {
  assert.equal(mapShippingToNotion('nekopos'), 'ネコポス');
});
test('mapShippingToNotion: takkyu50 → ヤマト50サイズ', () => {
  assert.equal(mapShippingToNotion('takkyu50'), 'ヤマト50サイズ');
});
test('mapShippingToNotion: hatsubarai → ヤマト宅急便', () => {
  assert.equal(mapShippingToNotion('hatsubarai'), 'ヤマト宅急便');
});
test('mapShippingToNotion: yupacketpuff → ゆうパケットパフ', () => {
  assert.equal(mapShippingToNotion('yupacketpuff'), 'ゆうパケットパフ');
});
test('mapShippingToNotion: clickpost → クリックポスト', () => {
  assert.equal(mapShippingToNotion('clickpost'), 'クリックポスト');
});
test('mapShippingToNotion: teikeigai は skip (Codex R2 H-1)', () => {
  // packing から「ネコポスサイズ vs 宅急便50」 判別できないため skip
  assert.equal(mapShippingToNotion('teikeigai'), null);
});
test('mapShippingToNotion: letterpack/aes/yupack/福山/西濃 は skip (Notion 8 値に無い)', () => {
  assert.equal(mapShippingToNotion('letterpack'), null);
  assert.equal(mapShippingToNotion('aes'), null);
  assert.equal(mapShippingToNotion('yupack'), null);
  assert.equal(mapShippingToNotion('fukuyamaistar2'), null);
  assert.equal(mapShippingToNotion('seinokangaroom2'), null);
});
test('mapShippingToNotion: null/undefined safe', () => {
  assert.equal(mapShippingToNotion(null), null);
  assert.equal(mapShippingToNotion(undefined), null);
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

// ── buildNotionDraftProposal: 配送方法 lookup (Phase E-15) ────────
test('buildNotionDraftProposal: warehouseDb なら pd_shipping_rule から配送方法補完', () => {
  // mock warehouseDb (better-sqlite3 in-memory)
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE pd_shipping_rule (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku_key TEXT NOT NULL,
      product_code TEXT NOT NULL,
      mall_group TEXT NOT NULL,
      qty_min INTEGER NOT NULL,
      qty_max INTEGER,
      shipping_method_code TEXT NOT NULL,
      packing_machine_code TEXT NOT NULL
    );
    INSERT INTO pd_shipping_rule (sku_key, product_code, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code)
    VALUES ('aigeshou5set::::','aigeshou5set','rakuten',1,NULL,'nekopos','pasline3つ折り');
  `);
  const rakutenItem = {
    manageNumber: 'aigeshou5set',
    title: 'お試し',
    variants: { v1: { standardPrice: '880' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {}, { warehouseDb: db });
  assert.equal(proposed.notion_delivery_label, 'ネコポス');
  assert.equal(skipped.notion_delivery_label, undefined);
});

test('buildNotionDraftProposal: warehouseDb なしは skip', () => {
  const rakutenItem = {
    manageNumber: 'x',
    title: 't',
    variants: { v1: { standardPrice: '100' } },
    payment: { taxRate: '0.1' },
  };
  const { skipped } = buildNotionDraftProposal(rakutenItem, {});
  assert.equal(skipped.notion_delivery_label, 'warehouse_db_unavailable');
});

test('buildNotionDraftProposal: pd_shipping_rule に row なしは pd_shipping_rule_missing', () => {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE pd_shipping_rule (
      sku_key TEXT, product_code TEXT, mall_group TEXT, qty_min INTEGER, qty_max INTEGER,
      shipping_method_code TEXT, packing_machine_code TEXT
    );
  `);
  const rakutenItem = {
    manageNumber: 'unknown_sku',
    title: 't',
    variants: { v1: { standardPrice: '100' } },
    payment: { taxRate: '0.1' },
  };
  const { skipped } = buildNotionDraftProposal(rakutenItem, {}, { warehouseDb: db });
  assert.equal(skipped.notion_delivery_label, 'pd_shipping_rule_missing');
});

test('buildNotionDraftProposal: variant 別 distinct label が複数なら ambiguous_shipping_rule (R3 H-1)', () => {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE pd_shipping_rule (
      sku_key TEXT, product_code TEXT, mall_group TEXT, qty_min INTEGER, qty_max INTEGER,
      shipping_method_code TEXT, packing_machine_code TEXT
    );
    INSERT INTO pd_shipping_rule VALUES ('a::red::S','a','rakuten',1,NULL,'nekopos','pasline3つ折り');
    INSERT INTO pd_shipping_rule VALUES ('a::blue::L','a','rakuten',1,NULL,'takkyu50','manual');
  `);
  const rakutenItem = {
    manageNumber: 'a',
    title: 't',
    variants: { v1: { standardPrice: '100' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {}, { warehouseDb: db });
  assert.equal(proposed.notion_delivery_label, undefined);
  assert.match(skipped.notion_delivery_label, /ambiguous_shipping_rule/);
});

test('buildNotionDraftProposal: 全 variant 同じ shipping なら distinct=1 で採用', () => {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE pd_shipping_rule (
      sku_key TEXT, product_code TEXT, mall_group TEXT, qty_min INTEGER, qty_max INTEGER,
      shipping_method_code TEXT, packing_machine_code TEXT
    );
    INSERT INTO pd_shipping_rule VALUES ('a::red::S','a','rakuten',1,NULL,'nekopos','pasline3つ折り');
    INSERT INTO pd_shipping_rule VALUES ('a::blue::L','a','rakuten',1,NULL,'nekopos','pasline3つ折り');
  `);
  const rakutenItem = {
    manageNumber: 'a',
    title: 't',
    variants: { v1: { standardPrice: '100' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {}, { warehouseDb: db });
  assert.equal(proposed.notion_delivery_label, 'ネコポス');
});

test('buildNotionDraftProposal: rakuten で hit なし → default にフォールバック', () => {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE pd_shipping_rule (
      sku_key TEXT, product_code TEXT, mall_group TEXT, qty_min INTEGER, qty_max INTEGER,
      shipping_method_code TEXT, packing_machine_code TEXT
    );
    INSERT INTO pd_shipping_rule VALUES ('a::::','a','default',1,NULL,'nekopos','pasline3つ折り');
  `);
  const rakutenItem = {
    manageNumber: 'a',
    title: 't',
    variants: { v1: { standardPrice: '100' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed } = buildNotionDraftProposal(rakutenItem, {}, { warehouseDb: db });
  assert.equal(proposed.notion_delivery_label, 'ネコポス');
});

test('buildNotionDraftProposal: teikeigai は unmappable_shipping (Codex R2 H-1)', () => {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE pd_shipping_rule (
      sku_key TEXT, product_code TEXT, mall_group TEXT, qty_min INTEGER, qty_max INTEGER,
      shipping_method_code TEXT, packing_machine_code TEXT
    );
    INSERT INTO pd_shipping_rule VALUES ('x::::','x','rakuten',1,NULL,'teikeigai','manual');
  `);
  const rakutenItem = {
    manageNumber: 'x',
    title: 't',
    variants: { v1: { standardPrice: '100' } },
    payment: { taxRate: '0.1' },
  };
  const { proposed, skipped } = buildNotionDraftProposal(rakutenItem, {}, { warehouseDb: db });
  assert.equal(proposed.notion_delivery_label, undefined);
  assert.match(skipped.notion_delivery_label, /unmappable_shipping:teikeigai/);
});
