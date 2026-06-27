/**
 * yahoo-text.js + yahoo-edititem-validator.js の unit test。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/lib/yahoo-text.test.mjs
 *
 * 2026-06-27 editItem HTTP 400 (Code it-01033) に伴う:
 *   - yahooTextUnits / truncateYahooTextUnits / htmlToPlainText / buildYahooExplanation
 *   - validateYahooEditItemFields (length / format preflight)
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  yahooTextUnits,
  truncateYahooTextUnits,
  htmlToPlainText,
  buildYahooExplanation,
  buildYahooHeadline,
} from './yahoo-text.js';

import {
  validateYahooEditItemFields,
  YahooFieldValidationError,
} from './yahoo-edititem-validator.js';

// ── yahooTextUnits ───────────────────────────────────────────────
test('yahooTextUnits: ASCII counts as 1 each', () => {
  assert.equal(yahooTextUnits('hello'), 5);
  assert.equal(yahooTextUnits('abc-123'), 7);
});

test('yahooTextUnits: 全角 counts as 2 each', () => {
  assert.equal(yahooTextUnits('あいう'), 6);
  assert.equal(yahooTextUnits('砥石'), 4);
});

test('yahooTextUnits: mixed ASCII + 全角', () => {
  assert.equal(yahooTextUnits('Sin.の油'), 4 + 4); // 'Sin.' = 4, 'の油' = 4
});

// ── truncateYahooTextUnits ───────────────────────────────────────
test('truncateYahooTextUnits: ASCII truncation by units', () => {
  assert.equal(truncateYahooTextUnits('hello world', 5), 'hello');
});

test('truncateYahooTextUnits: 全角 truncation by units', () => {
  // 'あいうえお' = 10 units → 6 units で 'あいう' (3 chars)
  assert.equal(truncateYahooTextUnits('あいうえお', 6), 'あいう');
});

test('truncateYahooTextUnits: empty / null safe', () => {
  assert.equal(truncateYahooTextUnits('', 10), '');
  assert.equal(truncateYahooTextUnits(null, 10), '');
});

test('truncateYahooTextUnits: does not exceed maxUnits even with multi-byte at boundary', () => {
  // 'あ' = 2 units. maxUnits=3 だと 'あ' で 2、 次の 'い' (2) を入れると 4 → break、 結果 'あ'
  assert.equal(truncateYahooTextUnits('あい', 3), 'あ');
});

// ── htmlToPlainText ───────────────────────────────────────────────
test('htmlToPlainText: <br> becomes newline', () => {
  assert.equal(htmlToPlainText('a<br>b<br>c'), 'a\nb\nc');
});

test('htmlToPlainText: <table> becomes text (rows on newlines, cells space-separated)', () => {
  const html = '<table><tr><td>key</td><td>value</td></tr></table>';
  const out = htmlToPlainText(html);
  assert.match(out, /key/);
  assert.match(out, /value/);
  assert.doesNotMatch(out, /<\w+/); // no tag left
});

test('htmlToPlainText: <script> and <style> are completely removed', () => {
  const html = 'hello<script>alert(1)</script><style>p{color:red}</style>world';
  const out = htmlToPlainText(html);
  assert.equal(out.includes('alert(1)'), false);
  assert.equal(out.includes('color:red'), false);
});

test('htmlToPlainText: entity decode', () => {
  assert.equal(htmlToPlainText('a&amp;b'), 'a&b');
});

test('htmlToPlainText: empty / null safe', () => {
  assert.equal(htmlToPlainText(''), '');
  assert.equal(htmlToPlainText(null), '');
});

// ── buildYahooExplanation ────────────────────────────────────────
test('buildYahooExplanation: returns headline + itemName + body, joined by \\n', () => {
  const out = buildYahooExplanation({
    headline: '砥石長持ちプロ仕様オイル',
    itemName: '油砥石用オイル 100ml',
    html: '<p>純鉱物油使用</p>',
  });
  assert.match(out, /砥石長持ちプロ仕様オイル/);
  assert.match(out, /油砥石用オイル/);
  assert.match(out, /純鉱物油/);
  // 改行で区切られている
  assert.match(out, /\n/);
});

test('buildYahooExplanation: always within 1000 units', () => {
  const longHtml = '<p>' + 'あ'.repeat(2000) + '</p>'; // 4000 units の本文
  const out = buildYahooExplanation({ headline: 'h', itemName: 'name', html: longHtml });
  assert.ok(yahooTextUnits(out) <= 1000, `got ${yahooTextUnits(out)} units`);
});

test('buildYahooExplanation: no HTML tags in output', () => {
  const out = buildYahooExplanation({
    headline: '',
    itemName: '',
    html: '<table><tr><td>foo</td></tr></table>',
  });
  assert.doesNotMatch(out, /<\w+/);
});

test('buildYahooExplanation: itemName truncated at 200 units before joining', () => {
  // itemName 単独で 600 units (300 全角) → 200 units (100 全角) に切られる
  const itemName = 'あ'.repeat(300);
  const out = buildYahooExplanation({ headline: '', itemName, html: 'body' });
  // 出力 = '<itemName>\nbody' → '<itemName>' は 200 units 以下
  const lines = out.split('\n');
  assert.ok(yahooTextUnits(lines[0]) <= 200, `itemName line was ${yahooTextUnits(lines[0])} units`);
});

test('buildYahooExplanation: empty input returns empty', () => {
  assert.equal(buildYahooExplanation({}), '');
});

// ── validateYahooEditItemFields ──────────────────────────────────
test('validateYahooEditItemFields: passes for valid fields', () => {
  validateYahooEditItemFields({
    item_code: 'aburatoishioil100',
    explanation: 'a'.repeat(500),
    caption: 'b'.repeat(5000),
    headline: 'h',
    product_category: 43494,
    path: '生活雑貨・日用品:掃除用品',
  });
});

test('validateYahooEditItemFields: explanation > 1000 units throws', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', explanation: 'あ'.repeat(501) }),
    YahooFieldValidationError,
  );
});

test('validateYahooEditItemFields: caption > 10000 units throws', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', caption: 'あ'.repeat(5001) }),
    YahooFieldValidationError,
  );
});

test('validateYahooEditItemFields: headline > 60 units throws', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', headline: 'あ'.repeat(31) }),
    YahooFieldValidationError,
  );
});

test('validateYahooEditItemFields: item_code underscore rejected (R2 H-1)', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'has_underscore' }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'item_code',
  );
});

test('validateYahooEditItemFields: item_code > 80 chars rejected', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a'.repeat(81) }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'item_code',
  );
});

test('validateYahooEditItemFields: jan format', () => {
  validateYahooEditItemFields({ item_code: 'a', jan: '4901234567890' });
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', jan: 'bad space jan' }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'jan',
  );
});

test('validateYahooEditItemFields: jan empty string is allowed (treated as not set)', () => {
  validateYahooEditItemFields({ item_code: 'a', jan: '' });
});

test('validateYahooEditItemFields: product_category must be 1-10 digits', () => {
  validateYahooEditItemFields({ item_code: 'a', product_category: 43494 });
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', product_category: 'abc' }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'product_category',
  );
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', product_category: '12345678901' }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'product_category',
  );
});

test('validateYahooEditItemFields: path > 8 segments rejected', () => {
  const path = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'].join(':');
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', path }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'path',
  );
});

test('validateYahooEditItemFields: path segment > 40 units rejected', () => {
  const longSeg = 'あ'.repeat(21); // 42 units
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', path: longSeg }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'path',
  );
});

test('validateYahooEditItemFields: empty/null fields ok', () => {
  validateYahooEditItemFields({
    item_code: 'a',
    explanation: '',
    caption: null,
    headline: undefined,
    path: '',
  });
});

// ── Codex R3 H-1: HTML tag in HTML-disallowed field ──────────────
test('buildYahooExplanation: headline with <b>...</b> is plain-text-ified before join', () => {
  const out = buildYahooExplanation({
    headline: '<b>SALE</b>セール中',
    itemName: '',
    html: '',
  });
  assert.equal(out.includes('<b>'), false);
  assert.equal(out.includes('</b>'), false);
  assert.match(out, /SALE/);
  assert.match(out, /セール中/);
});

test('buildYahooHeadline: strips HTML and truncates to 60 units', () => {
  const out = buildYahooHeadline('<b>SALE</b>セール中');
  assert.equal(out.includes('<'), false);
  assert.match(out, /SALE/);
});

test('buildYahooHeadline: empty / null safe', () => {
  assert.equal(buildYahooHeadline(''), '');
  assert.equal(buildYahooHeadline(null), '');
});

test('buildYahooHeadline: truncated to 60 units', () => {
  const out = buildYahooHeadline('あ'.repeat(50)); // 100 units
  assert.ok(yahooTextUnits(out) <= 60);
});

test('validateYahooEditItemFields: explanation with HTML tag rejected (R3 H-1)', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', explanation: 'hello <b>world</b>' }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'explanation' && /HTML/.test(e.message),
  );
});

test('validateYahooEditItemFields: headline with HTML tag rejected (R3 H-1)', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'a', headline: '<b>SALE</b>' }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'headline' && /HTML/.test(e.message),
  );
});

test('validateYahooEditItemFields: caption / additional1 / sp_additional with HTML allowed', () => {
  validateYahooEditItemFields({
    item_code: 'a',
    caption: '<table><tr><td>ok</td></tr></table>',
    additional1: '<br><br>',
    sp_additional: '<p>x</p>',
  });
});

test('validateYahooEditItemFields: aburatoishioil100 realistic case (current smoke target)', () => {
  // 旧 explanation の <table> long HTML をそのまま渡したら throw (500字超過)
  const longExplanation = 'あ'.repeat(600); // 1200 units
  assert.throws(
    () => validateYahooEditItemFields({
      item_code: 'aburatoishioil100',
      explanation: longExplanation,
      caption: 'ok',
      product_category: 43494,
      path: '生活雑貨・日用品:掃除用品',
    }),
    (e) => e instanceof YahooFieldValidationError && e.field === 'explanation',
  );
});
