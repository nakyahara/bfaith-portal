/**
 * category-master-search (再設計 R1) の lexical 検索 test。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/lib/category-master-search.test.mjs
 */
import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { searchCategoryMaster, countCategoryMaster, resetCategoryMasterCache } from './category-master-search.js';
import { normalizeForMatch, charBigrams, jaccard, containsNormalized } from './text-normalize.js';

function setupDb() {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE yahoo_category_master (
      product_category   TEXT PRIMARY KEY,
      name               TEXT NOT NULL,
      path_name          TEXT,
      relation           TEXT,
      source_updated_at  TEXT,
      is_active          INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
      imported_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
    CREATE TABLE category_default_path (
      yahoo_category_id INTEGER PRIMARY KEY,
      yahoo_path        TEXT NOT NULL
    );
  `);
  const ins = db.prepare('INSERT INTO yahoo_category_master (product_category, name, path_name, is_active) VALUES (?, ?, ?, ?)');
  ins.run('43494', '掃除用品', '生活雑貨・日用品 > 掃除用品', 1);
  ins.run('304759', 'ハンドクリーム', 'コスメ、美容、ヘアケア > ボディケア > ハンドクリーム', 1);
  ins.run('205777', '突っ張り棒', '家具、インテリア > カーテン、ブラインド > カーテンレール > 突っ張り棒', 1);
  ins.run('99999', '掃除機', '家電 > 生活家電 > 掃除機', 1);
  ins.run('88888', '廃止カテゴリ掃除', '廃止 > 掃除', 0); // is_active=0 は検索対象外
  db.prepare('INSERT INTO category_default_path (yahoo_category_id, yahoo_path) VALUES (?, ?)')
    .run(43494, '生活雑貨・日用品:掃除用品');
  return db;
}

beforeEach(() => resetCategoryMasterCache());

test('完全包含は最上位に来る + defaultPath が付く', () => {
  const db = setupDb();
  const results = searchCategoryMaster(db, '掃除用品');
  assert.ok(results.length >= 1);
  assert.equal(results[0].productCategory, '43494');
  assert.equal(results[0].score, 1);
  assert.equal(results[0].defaultPath, '生活雑貨・日用品:掃除用品');
});

test('部分一致 (バイグラム) でも候補に入る', () => {
  const db = setupDb();
  const results = searchCategoryMaster(db, '掃除');
  const ids = results.map((r) => r.productCategory);
  assert.ok(ids.includes('43494')); // 掃除用品
  assert.ok(ids.includes('99999')); // 掃除機
});

test('is_active=0 は検索対象外', () => {
  const db = setupDb();
  const results = searchCategoryMaster(db, '掃除');
  assert.ok(!results.map((r) => r.productCategory).includes('88888'));
});

test('カタカナ・表記ゆれ (NFKC) を吸収する', () => {
  const db = setupDb();
  const results = searchCategoryMaster(db, 'ﾊﾝﾄﾞｸﾘｰﾑ'); // 半角カナ
  assert.equal(results[0]?.productCategory, '304759');
});

test('無関係クエリはヒットしない (足切り)', () => {
  const db = setupDb();
  const results = searchCategoryMaster(db, 'zzzz');
  assert.equal(results.length, 0);
});

test('limit が効く', () => {
  const db = setupDb();
  const results = searchCategoryMaster(db, '掃除', { limit: 1 });
  assert.equal(results.length, 1);
});

test('countCategoryMaster は is_active=1 のみ数える / テーブル無しで 0', () => {
  const db = setupDb();
  assert.equal(countCategoryMaster(db), 4);
  const empty = new Db(':memory:');
  assert.equal(countCategoryMaster(empty), 0);
});

test('defaultPath 未登録の候補は null', () => {
  const db = setupDb();
  const results = searchCategoryMaster(db, 'ハンドクリーム');
  assert.equal(results[0].productCategory, '304759');
  assert.equal(results[0].defaultPath, null);
});

test('text-normalize 基本動作', () => {
  assert.equal(normalizeForMatch('突っ張り棒・伸縮 (強力)'), '突っ張り棒 伸縮 強力');
  assert.ok(containsNormalized(normalizeForMatch('強力 突っ張り棒 60cm'), normalizeForMatch('突っ張り棒')));
  const a = charBigrams(normalizeForMatch('掃除用品'));
  const b = charBigrams(normalizeForMatch('掃除用具'));
  assert.ok(jaccard(a, b) > 0.3);
});
