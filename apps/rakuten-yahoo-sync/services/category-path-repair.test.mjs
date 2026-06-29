import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { repairMissingCategoryPaths, countMissingCategoryPaths } from './category-path-repair.js';

function makeDb() {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE category_manual (
      rakuten_genre_id TEXT PRIMARY KEY,
      yahoo_category_id INTEGER NOT NULL,
      yahoo_path TEXT, note TEXT
    );
    CREATE TABLE category_decisions (
      rakuten_genre_id TEXT PRIMARY KEY,
      yahoo_category_id INTEGER NOT NULL,
      yahoo_path TEXT, locked INTEGER NOT NULL DEFAULT 0, ambiguous INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE category_default_path (
      yahoo_category_id INTEGER PRIMARY KEY,
      yahoo_path TEXT NOT NULL
    );
    CREATE TABLE yahoo_category_master (
      category_id INTEGER PRIMARY KEY, parent_id INTEGER,
      title_short TEXT, title_medium TEXT, title_long TEXT, name TEXT, path TEXT,
      depth INTEGER, child_count INTEGER, children_fetched_at TEXT, raw_json TEXT,
      fetch_status TEXT NOT NULL DEFAULT 'fetched', last_error TEXT,
      fetched_at TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT ''
    );
  `);
  return db;
}

// fetchCategory の mock: categoryId → path
function mockFetch(pathMap) {
  return async (categoryId) => {
    const path = pathMap[categoryId];
    if (path === '__throw__') throw new Error('rms_500');
    return {
      current: path === null
        ? { categoryId, parentId: 1, titleMedium: 'x', path: null }
        : { categoryId, parentId: 1, titleShort: 's', titleMedium: 'm', titleLong: 'l', path },
      children: [],
      raw: { ok: true },
    };
  };
}

test('countMissingCategoryPaths: manual で path NULL かつ default_path に無いものを数える', () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();   // 欠落
  db.prepare(`INSERT INTO category_manual VALUES ('g2', 200, '食品', NULL)`).run(); // path あり → 対象外
  db.prepare(`INSERT INTO category_manual VALUES ('g3', 300, NULL, NULL)`).run();   // 欠落
  db.prepare(`INSERT INTO category_default_path VALUES (300, '既存path')`).run();    // g3 は default_path にあり → 対象外
  assert.equal(countMissingCategoryPaths(db), 1); // 100 のみ
});

test('countMissingCategoryPaths: decisions(locked&非ambiguous) の path 欠落も数える', () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_decisions VALUES ('g1', 100, NULL, 1, 0)`).run(); // 欠落・対象
  db.prepare(`INSERT INTO category_decisions VALUES ('g2', 200, NULL, 0, 0)`).run(); // locked=0 → 対象外
  db.prepare(`INSERT INTO category_decisions VALUES ('g3', 300, NULL, 1, 1)`).run(); // ambiguous=1 → 対象外
  assert.equal(countMissingCategoryPaths(db), 1);
});

test('repairMissingCategoryPaths: path 取得して default_path + master に upsert', async () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();
  const deps = { fetchCategory: mockFetch({ 100: '生活雑貨・日用品:掃除用品' }) };
  const r = await repairMissingCategoryPaths({ db, sleepMs: 0, deps });
  assert.equal(r.repaired, 1);
  assert.equal(r.stillMissing, 0);
  assert.equal(db.prepare('SELECT yahoo_path FROM category_default_path WHERE yahoo_category_id=100').get().yahoo_path, '生活雑貨・日用品:掃除用品');
  assert.equal(db.prepare('SELECT path FROM yahoo_category_master WHERE category_id=100').get().path, '生活雑貨・日用品:掃除用品');
  // 補完後は欠落 0
  assert.equal(countMissingCategoryPaths(db), 0);
});

test('repairMissingCategoryPaths: Yahoo から path 組めない場合は stillMissing、 DB 更新なし', async () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();
  const deps = { fetchCategory: mockFetch({ 100: null }) };
  const r = await repairMissingCategoryPaths({ db, sleepMs: 0, deps });
  assert.equal(r.repaired, 0);
  assert.equal(r.stillMissing, 1);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM category_default_path').get().n, 0);
});

test('repairMissingCategoryPaths: dryRun は DB 更新せず取れる件数を試算', async () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();
  const deps = { fetchCategory: mockFetch({ 100: 'A:B' }) };
  const r = await repairMissingCategoryPaths({ db, dryRun: true, sleepMs: 0, deps });
  assert.equal(r.repaired, 1);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM category_default_path').get().n, 0); // DB 不変
});

test('repairMissingCategoryPaths: fetch 失敗は failed に計上', async () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();
  const deps = { fetchCategory: mockFetch({ 100: '__throw__' }) };
  const r = await repairMissingCategoryPaths({ db, sleepMs: 0, deps });
  assert.equal(r.failed, 1);
  assert.match(r.errors[0], /100: rms_500/);
});

test('repairMissingCategoryPaths: limit で batch 分割、 picked は全件・processed は batch', async () => {
  const db = makeDb();
  for (let i = 1; i <= 5; i++) db.prepare(`INSERT INTO category_manual VALUES ('g${i}', ${100 + i}, NULL, NULL)`).run();
  const pathMap = {}; for (let i = 1; i <= 5; i++) pathMap[100 + i] = `P${i}`;
  const r = await repairMissingCategoryPaths({ db, limit: 2, sleepMs: 0, deps: { fetchCategory: mockFetch(pathMap) } });
  assert.equal(r.picked, 5);
  assert.equal(r.processed, 2);
  assert.equal(r.repaired, 2);
  assert.equal(countMissingCategoryPaths(db), 3); // 残 3
});

test('repairMissingCategoryPaths: 対象 0 件なら fetch しない', async () => {
  const db = makeDb();
  const r = await repairMissingCategoryPaths({ db, sleepMs: 0, deps: { fetchCategory: async () => { throw new Error('should not fetch'); } } });
  assert.equal(r.picked, 0);
  assert.equal(r.repaired, 0);
});

test('countMissingCategoryPaths: yahoo_path が空文字/空白も欠落扱い (Codex R1 High)', () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, '', NULL)`).run();    // 空文字 → 欠落
  db.prepare(`INSERT INTO category_manual VALUES ('g2', 200, '   ', NULL)`).run(); // 空白 → 欠落
  db.prepare(`INSERT INTO category_manual VALUES ('g3', 300, '食品', NULL)`).run();// 有効 → 対象外
  assert.equal(countMissingCategoryPaths(db), 2);
});

test('repairMissingCategoryPaths: stillMissing は master に partial 記録され再選択されない (Codex R1 Medium)', async () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();
  const deps = { fetchCategory: mockFetch({ 100: null }) }; // path 組めない
  const r1 = await repairMissingCategoryPaths({ db, sleepMs: 0, deps });
  assert.equal(r1.stillMissing, 1);
  // master に partial 記録 → countMissing から除外される
  assert.equal(db.prepare(`SELECT fetch_status FROM yahoo_category_master WHERE category_id=100`).get().fetch_status, 'partial');
  assert.equal(countMissingCategoryPaths(db), 0); // 再選択されない
  // 次回 repair も対象 0
  const r2 = await repairMissingCategoryPaths({ db, sleepMs: 0, deps });
  assert.equal(r2.picked, 0);
});

test('repairMissingCategoryPaths: master テーブル無し環境でも default_path のみ補完 (Codex R1 High)', async () => {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE category_manual (rakuten_genre_id TEXT PRIMARY KEY, yahoo_category_id INTEGER NOT NULL, yahoo_path TEXT, note TEXT);
    CREATE TABLE category_decisions (rakuten_genre_id TEXT PRIMARY KEY, yahoo_category_id INTEGER NOT NULL, yahoo_path TEXT, locked INTEGER DEFAULT 0, ambiguous INTEGER DEFAULT 0);
    CREATE TABLE category_default_path (yahoo_category_id INTEGER PRIMARY KEY, yahoo_path TEXT NOT NULL);
  `); // yahoo_category_master 無し
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();
  const r = await repairMissingCategoryPaths({ db, sleepMs: 0, deps: { fetchCategory: mockFetch({ 100: 'A:B' }) } });
  assert.equal(r.repaired, 1); // master 無くても 500 にならず default_path 補完
  assert.equal(db.prepare('SELECT yahoo_path FROM category_default_path WHERE yahoo_category_id=100').get().yahoo_path, 'A:B');
});

test('repairMissingCategoryPaths: 同一 yahoo_category_id を複数 genre が使っても1回の fetch で補完 (DISTINCT)', async () => {
  const db = makeDb();
  db.prepare(`INSERT INTO category_manual VALUES ('g1', 100, NULL, NULL)`).run();
  db.prepare(`INSERT INTO category_manual VALUES ('g2', 100, NULL, NULL)`).run(); // 同じ category 100
  let calls = 0;
  const deps = { fetchCategory: async (id) => { calls++; return { current: { categoryId: id, path: 'A:B' }, children: [], raw: {} }; } };
  const r = await repairMissingCategoryPaths({ db, sleepMs: 0, deps });
  assert.equal(r.picked, 1);   // DISTINCT で 1
  assert.equal(calls, 1);
  assert.equal(r.repaired, 1);
});
