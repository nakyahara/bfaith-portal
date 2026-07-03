/**
 * category-tree (再設計 R9: 階層ドリルダウン) の test。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/lib/category-tree.test.mjs
 */
import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { getYahooTreeChildren, getShelfTreeChildren, resetCategoryTreeCache } from './category-tree.js';

function setupDb() {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE yahoo_category_master (
      product_category TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      path_name TEXT,
      is_active INTEGER NOT NULL DEFAULT 1,
      imported_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
    CREATE TABLE category_default_path (
      yahoo_category_id INTEGER PRIMARY KEY,
      yahoo_path TEXT NOT NULL
    );
  `);
  const ins = db.prepare('INSERT INTO yahoo_category_master (product_category, name, path_name, is_active) VALUES (?, ?, ?, ?)');
  ins.run('100', '食品', '食品', 1);
  ins.run('1201', '麺類、パスタ', '食品 > 麺類、パスタ', 1);
  ins.run('1204', 'うどん', '食品 > 麺類、パスタ > うどん', 1);
  ins.run('1232', 'その他麺類、パスタ', '食品 > 麺類、パスタ > その他麺類、パスタ', 1);
  ins.run('9999', '廃止', '食品 > 廃止カテゴリ', 0); // is_active=0 は出ない
  db.prepare('INSERT INTO category_default_path (yahoo_category_id, yahoo_path) VALUES (?, ?)').run(1232, '食品:麺類');
  db.prepare('INSERT INTO category_default_path (yahoo_category_id, yahoo_path) VALUES (?, ?)').run(43494, '生活雑貨・日用品:掃除用品');
  return db;
}

beforeEach(() => resetCategoryTreeCache());

test('Yahoo!ツリー: トップは第1階層のみ + categoryId/childCount が付く', () => {
  const db = setupDb();
  const r = getYahooTreeChildren(db, '');
  assert.equal(r.ok, true);
  assert.equal(r.children.length, 1);
  assert.equal(r.children[0].name, '食品');
  assert.equal(r.children[0].categoryId, '100'); // 「食品」自体もマスタ行あり = 選択可
  assert.equal(r.children[0].childCount, 1);     // 麺類、パスタ のみ (廃止は除外)
});

test('Yahoo!ツリー: ドリルダウンで子が返り、defaultPath が付く', () => {
  const db = setupDb();
  const r = getYahooTreeChildren(db, '食品 > 麺類、パスタ');
  assert.equal(r.ok, true);
  const names = r.children.map((c) => c.name);
  assert.ok(names.includes('うどん'));
  assert.ok(names.includes('その他麺類、パスタ'));
  const sonota = r.children.find((c) => c.name === 'その他麺類、パスタ');
  assert.equal(sonota.categoryId, '1232');
  assert.equal(sonota.childCount, 0);
  assert.equal(sonota.defaultPath, '食品:麺類'); // 既知の棚が添付される
  assert.equal(sonota.fullPath, '食品 > 麺類、パスタ > その他麺類、パスタ');
});

test('Yahoo!ツリー: 存在しない親パスは ok:false', () => {
  const db = setupDb();
  const r = getYahooTreeChildren(db, '存在しない > パス');
  assert.equal(r.ok, false);
});

test('Yahoo!ツリー: is_active=0 のカテゴリは出ない', () => {
  const db = setupDb();
  const r = getYahooTreeChildren(db, '食品');
  assert.ok(!r.children.map((c) => c.name).includes('廃止カテゴリ'));
});

test('棚ツリー: 実績のある棚から構築 (":" 区切り、途中ノードも選択対象)', () => {
  const db = setupDb();
  const top = getShelfTreeChildren(db, '');
  assert.equal(top.ok, true);
  assert.deepEqual(top.children.map((c) => c.name).sort(), ['生活雑貨・日用品', '食品']);
  const l2 = getShelfTreeChildren(db, '生活雑貨・日用品');
  assert.equal(l2.children[0].name, '掃除用品');
  assert.equal(l2.children[0].fullPath, '生活雑貨・日用品:掃除用品');
  assert.equal(l2.children[0].childCount, 0);
});

test('棚ツリー: category_default_path 未適用でも fail (ok:false) で crash しない', () => {
  const db = new Db(':memory:');
  db.exec(`CREATE TABLE yahoo_category_master (product_category TEXT PRIMARY KEY, name TEXT NOT NULL, path_name TEXT, is_active INTEGER DEFAULT 1, imported_at TEXT DEFAULT '')`);
  const r = getShelfTreeChildren(db, '');
  assert.equal(r.ok, false);
});
