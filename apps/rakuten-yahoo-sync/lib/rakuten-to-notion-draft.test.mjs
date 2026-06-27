import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  truncateYahooTitle, convertTaxRate, pickRepresentativePrice,
  buildNotionDraftProposal, toNotionProperties,
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

test('convertTaxRate: 未知値は null', () => {
  assert.equal(convertTaxRate(0), null);
  assert.equal(convertTaxRate(0.05), null);
  assert.equal(convertTaxRate(null), null);
  assert.equal(convertTaxRate('abc'), null);
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
  assert.equal(skipped.notion_delivery_label.startsWith('not_implemented'), true);
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
