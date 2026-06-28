import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import {
  backfillRakutenGenre, countMissingRakutenGenre, extractRakutenGenre,
} from './rakuten-title-backfill.js';

// migration_candidates の最小スキーマ (genre 列含む)
function makeDb() {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE migration_candidates (
      item_code              TEXT PRIMARY KEY,
      rakuten_manage_number  TEXT,
      rakuten_title          TEXT,
      rakuten_genre_id       TEXT,
      rakuten_genre_path     TEXT,
      status                 TEXT NOT NULL,
      first_detected_at      TEXT,
      last_title_synced_at   TEXT
    );
  `);
  return db;
}

function seed(db, rows) {
  const ins = db.prepare(`
    INSERT INTO migration_candidates
      (item_code, rakuten_manage_number, rakuten_title, rakuten_genre_id, status, first_detected_at)
    VALUES (@item_code, @mn, @title, @genre, @status, @at)
  `);
  rows.forEach((r, i) => ins.run({
    item_code: r.item_code, mn: r.mn, title: r.title ?? null,
    genre: r.genre ?? null, status: r.status ?? 'candidate', at: r.at ?? `2026-01-0${i + 1}`,
  }));
}

// ── extractRakutenGenre (既存だが genre backfill の核なので再確認) ──
test('extractRakutenGenre: top-level genreId を拾う (楽天 RMS 実測形)', () => {
  assert.deepEqual(extractRakutenGenre({ genreId: 215261 }), { genreId: '215261', genrePath: null });
});
test('extractRakutenGenre: genreId 無しは null', () => {
  assert.deepEqual(extractRakutenGenre({ itemName: 'x' }), { genreId: null, genrePath: null });
});

// ── countMissingRakutenGenre ──
test('countMissingRakutenGenre: genre NULL かつ manage_number ありを数える', () => {
  const db = makeDb();
  seed(db, [
    { item_code: 'a', mn: 'a', title: 'T', genre: null },        // 対象
    { item_code: 'b', mn: 'b', title: 'T', genre: '' },          // 対象 (空文字)
    { item_code: 'c', mn: 'c', title: 'T', genre: '123' },       // 対象外 (genre あり)
    { item_code: 'd', mn: null, title: 'T', genre: null },       // 対象外 (mn なし)
    { item_code: 'e', mn: 'e', title: 'T', genre: null, status: 'resolved' }, // 対象外 (terminal)
  ]);
  assert.equal(countMissingRakutenGenre(db), 2);
});

// ── backfillRakutenGenre ──
test('backfillRakutenGenre: genre 取得して genre 列のみ更新 (title 不変)', async () => {
  const db = makeDb();
  seed(db, [{ item_code: 'a', mn: 'a', title: '既存タイトル', genre: null }]);
  const deps = {
    fetchItemDetailsBulkDetailed: async () => ({
      items: [{ manageNumber: 'a', itemName: '新タイトル', genreId: 215261 }],
      failed: [],
    }),
  };
  const r = await backfillRakutenGenre({ db, deps });
  assert.equal(r.updated, 1);
  assert.equal(r.stillNull, 0);
  const row = db.prepare('SELECT rakuten_genre_id, rakuten_title FROM migration_candidates WHERE item_code=?').get('a');
  assert.equal(row.rakuten_genre_id, '215261');
  assert.equal(row.rakuten_title, '既存タイトル'); // title は touch しない
});

test('backfillRakutenGenre: 楽天側に genre 無しは stillNull に計上、 更新しない', async () => {
  const db = makeDb();
  seed(db, [{ item_code: 'a', mn: 'a', title: 'T', genre: null }]);
  const deps = {
    fetchItemDetailsBulkDetailed: async () => ({
      items: [{ manageNumber: 'a', itemName: 'T' }], // genreId 無し
      failed: [],
    }),
  };
  const r = await backfillRakutenGenre({ db, deps });
  assert.equal(r.updated, 0);
  assert.equal(r.stillNull, 1);
  const row = db.prepare('SELECT rakuten_genre_id FROM migration_candidates WHERE item_code=?').get('a');
  assert.equal(row.rakuten_genre_id, null);
});

test('backfillRakutenGenre: dryRun は DB 更新せず取れる件数を試算', async () => {
  const db = makeDb();
  seed(db, [
    { item_code: 'a', mn: 'a', title: 'T', genre: null },
    { item_code: 'b', mn: 'b', title: 'T', genre: null },
  ]);
  const deps = {
    fetchItemDetailsBulkDetailed: async () => ({
      items: [
        { manageNumber: 'a', genreId: 100 },
        { manageNumber: 'b' }, // genre 無し
      ],
      failed: [],
    }),
  };
  const r = await backfillRakutenGenre({ db, dryRun: true, deps });
  assert.equal(r.updated, 1);   // a だけ取れる試算
  assert.equal(r.stillNull, 1); // b は genre 無し
  // DB は不変
  assert.equal(db.prepare('SELECT rakuten_genre_id FROM migration_candidates WHERE item_code=?').get('a').rakuten_genre_id, null);
});

test('backfillRakutenGenre: 同一 manage_number 複数 item_code は両行更新、 dryRun/本実行で updated 一致 (Codex R1)', async () => {
  const mkDeps = () => ({
    fetchItemDetailsBulkDetailed: async () => ({ items: [{ manageNumber: 'shared', genreId: 555 }], failed: [] }),
  });
  // dryRun
  const db1 = makeDb();
  seed(db1, [
    { item_code: 'p1', mn: 'shared', title: 'T', genre: null },
    { item_code: 'p2', mn: 'shared', title: 'T', genre: null },
  ]);
  const dry = await backfillRakutenGenre({ db: db1, dryRun: true, deps: mkDeps() });
  assert.equal(dry.updated, 2); // 2 行とも対象

  // 本実行
  const db2 = makeDb();
  seed(db2, [
    { item_code: 'p1', mn: 'shared', title: 'T', genre: null },
    { item_code: 'p2', mn: 'shared', title: 'T', genre: null },
  ]);
  const real = await backfillRakutenGenre({ db: db2, deps: mkDeps() });
  assert.equal(real.updated, 2); // dryRun と一致
  assert.equal(real.updated <= real.picked, true); // updated <= picked 保証
  assert.equal(db2.prepare('SELECT rakuten_genre_id FROM migration_candidates WHERE item_code=?').get('p1').rakuten_genre_id, '555');
  assert.equal(db2.prepare('SELECT rakuten_genre_id FROM migration_candidates WHERE item_code=?').get('p2').rakuten_genre_id, '555');
});

test('backfillRakutenGenre: 対象 0 件なら何もしない', async () => {
  const db = makeDb();
  seed(db, [{ item_code: 'a', mn: 'a', title: 'T', genre: '999' }]); // genre 既にあり
  const r = await backfillRakutenGenre({ db, deps: { fetchItemDetailsBulkDetailed: async () => { throw new Error('should not fetch'); } } });
  assert.equal(r.picked, 0);
  assert.equal(r.updated, 0);
});

test('backfillRakutenGenre: fetch 失敗は failed に計上', async () => {
  const db = makeDb();
  seed(db, [{ item_code: 'a', mn: 'a', title: 'T', genre: null }]);
  const deps = {
    fetchItemDetailsBulkDetailed: async () => ({ items: [], failed: [{ manageNumber: 'a', reason: 'rms_500' }] }),
  };
  const r = await backfillRakutenGenre({ db, deps });
  assert.equal(r.updated, 0);
  assert.equal(r.failed, 1);
  assert.match(r.errors[0], /a: rms_500/);
});

test('backfillRakutenGenre: genre 列が無い環境は genre_columns_missing で安全終了', async () => {
  const db = new Db(':memory:');
  db.exec(`CREATE TABLE migration_candidates (item_code TEXT PRIMARY KEY, rakuten_manage_number TEXT, status TEXT, first_detected_at TEXT);`);
  const r = await backfillRakutenGenre({ db, deps: { fetchItemDetailsBulkDetailed: async () => ({ items: [], failed: [] }) } });
  assert.deepEqual(r.errors, ['genre_columns_missing']);
  assert.equal(r.updated, 0);
});
