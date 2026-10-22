/**
 * 確定値 (Notion 廃止後の入口) の試験 — 方針案 20260914 D-2〜D-5 (2026-10-22)。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/confirmed-values.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { runMigrations } from '../db.js';
import {
  confirmValues, loadConfirmedValues, loadConfirmedRow, listConfirmHistory, validateConfirmInput,
  normalizeTaxRate, mergeConfirmedValues, ConfirmError,
} from './confirmed-values.js';
import { resolvePrice } from './field-mapper.js';
import { resolveDelivery } from './delivery-resolver.js';
import { evaluateReadiness, evaluateTaxRate } from './readiness-check.js';
import { labelFromPortal, labelFromRakutenGroup, suggestDeliveryLabelFromRakutenItem } from './delivery-label-map.js';

function freshDb() {
  const db = new Db(':memory:');
  db.pragma('foreign_keys = ON');
  runMigrations(db);
  return db;
}

function insertLegacy(db, mn, over = {}) {
  const row = {
    rakuten_manage_number: mn, yahoo_title: '旧タイトル', yahoo_price: 1200, yahoo_price_sagawa: null,
    notion_delivery_label: 'ネコポス', notion_tax_rate: '10%', notion_has_variation: null,
    yahoo_headline: '旧キャッチ', yahoo_caption_text: null, yahoo_caption_html: '<p>旧説明</p>', yahoo_jan: '4900000000000',
    notion_product_category: null, notion_path: null,
    source_hash: 'h', notion_page_id: 'page-' + mn, ...over,
  };
  const cols = Object.keys(row);
  db.prepare(`INSERT INTO notion_overrides (${cols.join(',')}) VALUES (${cols.map(() => '?').join(',')})`).run(...cols.map((c) => row[c]));
}

const base = { yahoo_title: '新タイトル', yahoo_price: '1,980', delivery_label: 'ネコポス', tax_rate: '10' };

test('税率の書式をそろえる (ハブの 8% / 旧 Notion の 10% / 数値)', () => {
  assert.equal(normalizeTaxRate('8%'), '8');
  assert.equal(normalizeTaxRate('８％'), '8');
  assert.equal(normalizeTaxRate('10%'), '10');
  assert.equal(normalizeTaxRate(0.08), '8');
  assert.equal(normalizeTaxRate(0.1), '10');
  assert.equal(normalizeTaxRate('5'), null);
  assert.equal(normalizeTaxRate(''), null);
  // 出品前チェックも 8% を読める (旧実装は '8%' で notion_tax_rate_unknown_value になっていた)
  assert.equal(evaluateTaxRate('8%', 0.08).ok, true);
});

test('確定値が無ければ旧 Notion の値を「旧運用から継承」として使う (承認日時は作らない)', () => {
  const db = freshDb();
  insertLegacy(db, 'abc');
  const v = loadConfirmedValues(db, 'abc');
  assert.equal(v.value_source, 'legacy_notion');
  assert.equal(v.yahoo_title, '旧タイトル');
  assert.equal(v.confirmed_at, null);
  assert.equal(loadConfirmedValues(db, 'none'), null);
});

test('確定すると確定値が勝つ。headline・caption・JAN は旧値を引き継ぐ。旧値は履歴へ', () => {
  const db = freshDb();
  insertLegacy(db, 'abc');
  const r = confirmValues(db, { manageNumber: 'ABC', input: base, source: 'manual', actor: 'a@x' });
  assert.equal(r.changed, true);
  assert.equal(r.row.version, 1);
  assert.equal(r.row.yahoo_price, 1980);
  const v = loadConfirmedValues(db, 'abc');
  assert.equal(v.value_source, 'manual');
  assert.equal(v.yahoo_title, '新タイトル');
  assert.equal(v.notion_tax_rate, '10');
  assert.equal(v.yahoo_headline, '旧キャッチ');
  assert.equal(v.yahoo_caption_html, '<p>旧説明</p>');
  assert.equal(v.yahoo_jan, '4900000000000');
  assert.equal(v.notion_has_variation, null);
  const h = listConfirmHistory(db, 'abc');
  assert.equal(h.length, 1);
  assert.equal(h[0].source_before, 'legacy_notion');
  assert.equal(h[0].values_before.yahoo_title, '旧タイトル');
  // 旧 notion_overrides は書き換えない
  assert.equal(db.prepare('SELECT yahoo_title FROM notion_overrides WHERE rakuten_manage_number = ?').get('abc').yahoo_title, '旧タイトル');
});

test('同じ値で確定し直しても版を上げない / 画面が古い (version 違い) と 409', () => {
  const db = freshDb();
  confirmValues(db, { manageNumber: 'x1', input: base, source: 'manual', actor: 'a' });
  const again = confirmValues(db, { manageNumber: 'x1', input: base, source: 'manual', actor: 'b', expectedVersion: 1 });
  assert.equal(again.changed, false);
  assert.equal(loadConfirmedRow(db, 'x1').version, 1);
  assert.throws(() => confirmValues(db, { manageNumber: 'x1', input: { ...base, yahoo_price: 2000 }, source: 'manual', actor: 'b', expectedVersion: null }),
    (e) => e instanceof ConfirmError && e.statusCode === 409 && e.code === 'version_conflict');
  const r2 = confirmValues(db, { manageNumber: 'x1', input: { ...base, yahoo_price: 2000 }, source: 'manual', actor: 'b', expectedVersion: 1 });
  assert.equal(r2.row.version, 2);
});

test('ハブで確定した商品はこのアプリでは変えられない (D-5)。ハブはこのアプリの値を引き継げる', () => {
  const db = freshDb();
  confirmValues(db, { manageNumber: 'p1', input: base, source: 'manual', actor: 'a' });
  const r = confirmValues(db, { manageNumber: 'p1', input: { ...base, yahoo_title: 'ハブの題' }, source: 'portal', actor: 'h', expectedVersion: 1, portalDraftId: 7 });
  assert.equal(r.row.source, 'portal');
  assert.equal(r.row.portal_draft_id, 7);
  assert.throws(() => confirmValues(db, { manageNumber: 'p1', input: base, source: 'manual', actor: 'a', expectedVersion: 2 }),
    (e) => e.code === 'portal_owned' && e.statusCode === 409);
  assert.throws(() => confirmValues(db, { manageNumber: 'p2', input: base, source: 'portal', actor: 'h' }), /商品 ID/);
});

test('入力の検査: 売価・配送・税率は必須、佐川対象は明示選択、カテゴリは両方か両方空', () => {
  const db = freshDb();
  const bad = validateConfirmInput(db, { yahoo_title: '', yahoo_price: '0', delivery_label: '謎', tax_rate: '5' });
  assert.equal(bad.ok, false);
  assert.equal(bad.errors.length, 4);
  const sg = validateConfirmInput(db, { ...base, delivery_label: '定形外（ヤフーのみネコポス）' });
  assert.equal(sg.ok, false);
  assert.match(sg.errors.join(), /佐川売価/);
  assert.equal(validateConfirmInput(db, { ...base, delivery_label: '定形外（ヤフーのみネコポス）', sagawa_mode: 'same' }).ok, true);
  const sp = validateConfirmInput(db, { ...base, delivery_label: '定形外（ヤフーのみネコポス）', sagawa_mode: 'price', yahoo_price_sagawa: '2,480' });
  assert.equal(sp.values.yahoo_price_sagawa, 2480);
  assert.equal(validateConfirmInput(db, { ...base, product_category: '1234' }).ok, false);
  assert.equal(validateConfirmInput(db, { ...base, product_category: '1234', path: '食品:麺' }).ok, true);
});

test('売価は確定した金額だけ (楽天の値へ黙って fallback しない — D-2)', () => {
  const db = freshDb();
  const rakutenItem = { variants: { a: { standardPrice: 999 } } };
  const neko = resolveDelivery(db, 'ネコポス');
  const sagawa = resolveDelivery(db, '定形外（ヤフーのみネコポス）');
  assert.equal(resolvePrice({ notionOverride: { yahoo_price: null }, deliveryRow: neko, rakutenItem }), null);
  assert.equal(resolvePrice({ notionOverride: null, deliveryRow: neko, rakutenItem }), null);
  assert.equal(resolvePrice({ notionOverride: { yahoo_price: 1500 }, deliveryRow: neko }), 1500);
  // 佐川の対象: 佐川売価 > 同額の明示 > どちらも無ければ止める (旧実装は通常売価へ黙って進んだ)
  assert.equal(resolvePrice({ notionOverride: { yahoo_price: 1500, yahoo_price_sagawa: 1800 }, deliveryRow: sagawa }), 1800);
  assert.equal(resolvePrice({ notionOverride: { yahoo_price: 1500, sagawa_mode: 'same' }, deliveryRow: sagawa }), 1500);
  assert.equal(resolvePrice({ notionOverride: { yahoo_price: 1500 }, deliveryRow: sagawa }), null);
  const r = evaluateReadiness({
    notionOverride: { yahoo_title: 't', yahoo_price: 1500, notion_tax_rate: '10' }, rakutenTaxRate: 0.1,
    deliveryRow: sagawa, productCategory: 1, path: 'p', resolvedPrice: null,
    imagePreflight: { ok: true }, leadTimePreflight: { ok: true }, variationResult: { isVariation: false },
  });
  assert.deepEqual(r.reasons, ['sagawa_price_unconfirmed']);
});

test('旧 Notion でカテゴリ ID と path が片方だけの値は捨てて辞書に任せる (直す場所が無くなったため)', () => {
  const v = mergeConfirmedValues(null, { yahoo_title: 't', notion_product_category: 123, notion_path: null });
  assert.equal(v.notion_product_category, null);
  assert.equal(v.legacy_category_partial_ignored, true);
  const ok = mergeConfirmedValues(null, { yahoo_title: 't', notion_product_category: 123, notion_path: 'a:b' });
  assert.equal(ok.notion_product_category, 123);
});

test('配送方法の読み替え: 楽天の配送方法 / ハブの「ヤフーだけ別配送」→ 移行アプリのラベル', () => {
  assert.equal(labelFromRakutenGroup('5'), 'ネコポス');
  assert.equal(labelFromRakutenGroup('8'), 'ヤマト50サイズ');
  assert.equal(labelFromRakutenGroup('7'), 'ヤマト宅急便');
  // 対応を決めていない配送方法は推測で当てない
  assert.equal(labelFromRakutenGroup('1'), null);
  assert.equal(labelFromRakutenGroup('3'), null);
  assert.equal(labelFromPortal({ rakutenGroup: '1', shippingOverride: true, yahooDeliveryName: '宅急便50サイズ以下' }).label, '定形外（ヤフーのみ宅急便50サイズ）');
  assert.equal(labelFromPortal({ rakutenGroup: '1', shippingOverride: true, yahooDeliveryName: 'ネコポス' }).label, '定形外（ヤフーのみネコポス）');
  assert.equal(labelFromPortal({ rakutenGroup: '6', shippingOverride: false }).label, 'クリックポスト');
  assert.equal(labelFromPortal({ rakutenGroup: '1', shippingOverride: false }).label, null);
  assert.equal(labelFromPortal({ rakutenGroup: '', shippingOverride: false }).label, null);
  // 出てくるラベルは全部 delivery_mapping にある
  const db = freshDb();
  for (const g of ['5', '6', '7', '8', '9']) assert.ok(resolveDelivery(db, labelFromRakutenGroup(g)), g);
  // 楽天の商品: 表示中の SKU の配送方法がそろっているときだけ候補
  assert.equal(suggestDeliveryLabelFromRakutenItem({ variants: { a: { shipping: { shippingMethodGroup: '5' } }, b: { hidden: true, shipping: { shippingMethodGroup: '7' } } } }).label, 'ネコポス');
  const mixed = suggestDeliveryLabelFromRakutenItem({ variants: [{ shipping: { shippingMethodGroup: '5' } }, { shipping: { shippingMethodGroup: '7' } }] });
  assert.equal(mixed.label, null);
  assert.equal(mixed.mixed, true);
});

test('税率を Yahoo に送る (taxrate_type。省略すると Yahoo は 10% 扱い)', async () => {
  const { buildYahooEditItemFields } = await import('./field-mapper.js');
  const vr = { condition: 2, isVariation: false, sendAucFields: false };
  const item = { itemNumber: 'x', itemName: 'n', variants: {} };
  const f8 = buildYahooEditItemFields({ rakutenItem: item, notionOverride: { yahoo_title: 't', yahoo_price: 100, notion_tax_rate: '8' }, deliveryRow: null, variationResult: vr });
  assert.equal(f8.taxrate_type, 0.08);
  const f10 = buildYahooEditItemFields({ rakutenItem: item, notionOverride: { yahoo_title: 't', yahoo_price: 100, notion_tax_rate: '10%' }, deliveryRow: null, variationResult: vr });
  assert.equal(f10.taxrate_type, 0.1);
});
