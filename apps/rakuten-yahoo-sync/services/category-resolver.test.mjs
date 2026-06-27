/**
 * category-resolver の多段 fallback test (Phase E-12: ローカル RYS 由来テーブル統合).
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/category-resolver.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { resolveCategoryAndPath, resolveByGenreId } from './category-resolver.js';

function setupDb() {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE genre_yahoo_category_mapping (
      rakuten_genre_id  TEXT NOT NULL,
      yahoo_category_id INTEGER NOT NULL,
      yahoo_path        TEXT NOT NULL,
      sample_count      INTEGER NOT NULL DEFAULT 1,
      is_primary        INTEGER NOT NULL DEFAULT 0,
      first_learned_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      last_learned_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (rakuten_genre_id, yahoo_category_id, yahoo_path)
    );
    CREATE TABLE category_manual (
      rakuten_genre_id  TEXT PRIMARY KEY,
      yahoo_category_id INTEGER NOT NULL,
      yahoo_path        TEXT,
      note              TEXT,
      source            TEXT NOT NULL DEFAULT 'imported_from_local_rys',
      created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
    CREATE TABLE category_decisions (
      rakuten_genre_id  TEXT PRIMARY KEY,
      yahoo_category_id INTEGER NOT NULL,
      yahoo_path        TEXT,
      decision_source   TEXT NOT NULL DEFAULT 'learned',
      confidence        REAL,
      sample_count      INTEGER NOT NULL DEFAULT 0,
      ambiguous         INTEGER NOT NULL DEFAULT 0,
      locked            INTEGER NOT NULL DEFAULT 0,
      source            TEXT NOT NULL DEFAULT 'imported_from_local_rys',
      imported_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
    CREATE TABLE category_default_path (
      yahoo_category_id INTEGER PRIMARY KEY,
      yahoo_path        TEXT NOT NULL,
      source            TEXT NOT NULL DEFAULT 'imported_from_local_rys',
      imported_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
  `);
  return db;
}

// ── 優先順位: Notion override が manual より優先 ─────────────────
test('Notion override が manual より優先', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_manual (rakuten_genre_id, yahoo_category_id, yahoo_path) VALUES (?, ?, ?)')
    .run('565402', 34706, '花・ガーデン・DIY');
  const r = resolveCategoryAndPath({
    db, rakutenGenreId: '565402',
    notionOverride: { notion_product_category: 99999, notion_path: 'Notion特別カテゴリ' },
  });
  assert.equal(r.source, 'notion');
  assert.equal(r.category, 99999);
  assert.equal(r.path, 'Notion特別カテゴリ');
});

// ── 優先順位: manual > decisions > legacy ─────────────────────
test('Notion なし → category_manual を採用', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_manual (rakuten_genre_id, yahoo_category_id, yahoo_path) VALUES (?, ?, ?)')
    .run('565402', 34706, '花・ガーデン・DIY');
  // 同 genre で decisions と legacy も入れて、 manual が勝つことを確認
  db.prepare('INSERT INTO category_decisions (rakuten_genre_id, yahoo_category_id, yahoo_path, locked, ambiguous) VALUES (?, ?, ?, 1, 0)')
    .run('565402', 11111, 'X');
  db.prepare('INSERT INTO genre_yahoo_category_mapping (rakuten_genre_id, yahoo_category_id, yahoo_path, sample_count, is_primary) VALUES (?, ?, ?, 5, 1)')
    .run('565402', 22222, 'Y');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '565402' });
  assert.equal(r.source, 'manual');
  assert.equal(r.category, 34706);
});

test('manual に yahoo_path NULL なら category_default_path から補完', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_manual (rakuten_genre_id, yahoo_category_id, yahoo_path) VALUES (?, ?, NULL)')
    .run('565402', 34706);
  db.prepare('INSERT INTO category_default_path (yahoo_category_id, yahoo_path) VALUES (?, ?)')
    .run(34706, '補完されたpath');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '565402' });
  assert.equal(r.source, 'manual');
  assert.equal(r.path, '補完されたpath');
});

test('manual に path 補完なし + default_path も無し → manual_path_missing (Codex R3)', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_manual (rakuten_genre_id, yahoo_category_id, yahoo_path) VALUES (?, ?, NULL)')
    .run('565402', 34706);
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '565402' });
  // path NULL は manual ヒットでも採用しない。 Codex R3 high: legacy に落とさず fail-closed
  assert.equal(r.source, 'manual_path_missing');
  assert.equal(r.manualCategory, 34706);
});

test('manual 無 → category_decisions (locked=1, ambiguous=0) を採用', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_decisions (rakuten_genre_id, yahoo_category_id, yahoo_path, locked, ambiguous) VALUES (?, ?, ?, 1, 0)')
    .run('100303', 44420, 'パン');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '100303' });
  assert.equal(r.source, 'decisions');
  assert.equal(r.category, 44420);
});

test('category_decisions ambiguous=1 は採用しない (review 扱い)', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_decisions (rakuten_genre_id, yahoo_category_id, yahoo_path, locked, ambiguous) VALUES (?, ?, ?, 1, 1)')
    .run('100303', 44420, 'パン');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '100303' });
  assert.equal(r.source, 'unresolved');
});

test('category_decisions locked=0 (conditional) は採用しない', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_decisions (rakuten_genre_id, yahoo_category_id, yahoo_path, locked, ambiguous) VALUES (?, ?, ?, 0, 0)')
    .run('100303', 44420, 'パン');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '100303' });
  assert.equal(r.source, 'unresolved');
});

test('decisions に yahoo_path NULL → default_path で補完', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_decisions (rakuten_genre_id, yahoo_category_id, yahoo_path, locked, ambiguous) VALUES (?, ?, NULL, 1, 0)')
    .run('100303', 44420);
  db.prepare('INSERT INTO category_default_path (yahoo_category_id, yahoo_path) VALUES (?, ?)')
    .run(44420, 'fallback パン path');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '100303' });
  assert.equal(r.source, 'decisions');
  assert.equal(r.path, 'fallback パン path');
});

test('manual / decisions 共に無 → legacy genre_yahoo_category_mapping fallback', () => {
  const db = setupDb();
  db.prepare('INSERT INTO genre_yahoo_category_mapping (rakuten_genre_id, yahoo_category_id, yahoo_path, sample_count, is_primary) VALUES (?, ?, ?, 5, 1)')
    .run('888888', 77777, 'レガシーpath');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '888888' });
  assert.equal(r.source, 'learned');
  assert.equal(r.category, 77777);
});

test('全部無 → unresolved', () => {
  const db = setupDb();
  const r = resolveCategoryAndPath({ db, rakutenGenreId: 'unknown999' });
  assert.equal(r.source, 'unresolved');
  assert.equal(r.category, null);
});

test('Notion partial (片方だけ) は notion_partial で fail-closed', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_manual (rakuten_genre_id, yahoo_category_id, yahoo_path) VALUES (?, ?, ?)')
    .run('565402', 34706, '花・ガーデン・DIY');
  const r = resolveCategoryAndPath({
    db, rakutenGenreId: '565402',
    notionOverride: { notion_product_category: 99999 }, // path 欠落
  });
  assert.equal(r.source, 'notion_partial');
});

// ── Codex R3 high: manual hit + path 欠落 → legacy に落とさず fail-closed ──
test('manual hit + path 欠落 + legacy あり → legacy に落とさず manual_path_missing', () => {
  const db = setupDb();
  // manual hit、 path NULL、 default_path も無し
  db.prepare('INSERT INTO category_manual (rakuten_genre_id, yahoo_category_id, yahoo_path) VALUES (?, ?, NULL)')
    .run('565402', 34706);
  // 同 genre に legacy あり (本来なら採用される)
  db.prepare('INSERT INTO genre_yahoo_category_mapping (rakuten_genre_id, yahoo_category_id, yahoo_path, sample_count, is_primary) VALUES (?, ?, ?, 5, 1)')
    .run('565402', 99999, 'legacy_path');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '565402' });
  assert.equal(r.source, 'manual_path_missing');
  assert.equal(r.manualCategory, 34706);
  assert.equal(r.category, null);
});

test('decisions hit + path 欠落 + legacy あり → legacy に落とさず decisions_path_missing', () => {
  const db = setupDb();
  db.prepare('INSERT INTO category_decisions (rakuten_genre_id, yahoo_category_id, yahoo_path, locked, ambiguous) VALUES (?, ?, NULL, 1, 0)')
    .run('100303', 44420);
  db.prepare('INSERT INTO genre_yahoo_category_mapping (rakuten_genre_id, yahoo_category_id, yahoo_path, sample_count, is_primary) VALUES (?, ?, ?, 5, 1)')
    .run('100303', 99999, 'legacy_path');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '100303' });
  assert.equal(r.source, 'decisions_path_missing');
  assert.equal(r.decisionsCategory, 44420);
  assert.equal(r.category, null);
});

test('migration 015 未適用 (table 無し) でも fail せず legacy にフォールバック', () => {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE genre_yahoo_category_mapping (
      rakuten_genre_id  TEXT NOT NULL,
      yahoo_category_id INTEGER NOT NULL,
      yahoo_path        TEXT NOT NULL,
      sample_count      INTEGER NOT NULL DEFAULT 1,
      is_primary        INTEGER NOT NULL DEFAULT 0,
      first_learned_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      last_learned_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (rakuten_genre_id, yahoo_category_id, yahoo_path)
    );
  `);
  db.prepare('INSERT INTO genre_yahoo_category_mapping (rakuten_genre_id, yahoo_category_id, yahoo_path, sample_count, is_primary) VALUES (?, ?, ?, 5, 1)')
    .run('999', 1, 'legacy');
  const r = resolveCategoryAndPath({ db, rakutenGenreId: '999' });
  assert.equal(r.source, 'learned');
});
