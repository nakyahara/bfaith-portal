/**
 * Phase E-9 smoke: migration_candidates.rakuten_title backfill。
 *
 * - in-memory DB に migration 006/008/011 を抜き取って適用 (candidate + excluded + title 列)
 * - candidate を 5 件 seed
 * - fetchItemDetailsBulkDetailed をモックして 4 件 success + 1 件 failed
 * - backfillRakutenTitles 実行
 * - rakuten_title が埋まる / status='resolved' は触らない / 残件数が countMissing で取れる
 *
 * 使い方: node apps/rakuten-yahoo-sync/scripts/test-rakuten-title-backfill.js
 */

import Database from 'better-sqlite3';
import { backfillRakutenTitles, countMissingRakutenTitles, extractRakutenGenre } from '../lib/rakuten-title-backfill.js';

const checks = [];
function expect(label, cond, detail) {
  checks.push({ label, ok: !!cond, detail });
  console.log(`${cond ? '✅' : '❌'} ${label}${detail ? ` — ${detail}` : ''}`);
}

const db = new Database(':memory:');
// migration 006 (candidates) — 抜粋
db.exec(`
  CREATE TABLE migration_candidates (
    item_code TEXT PRIMARY KEY,
    rakuten_manage_number TEXT,
    first_detected_at TEXT NOT NULL,
    last_detected_at TEXT NOT NULL,
    last_seen_in_rakuten_at TEXT NOT NULL,
    last_checked_yahoo_baseline_at TEXT NOT NULL,
    missing_rakuten_count INTEGER NOT NULL DEFAULT 0,
    stale_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('candidate', 'resolved', 'stale')),
    resolved_at TEXT,
    source TEXT NOT NULL DEFAULT 'diff_detector'
  );
`);
// migration 011 — title 列
db.exec(`ALTER TABLE migration_candidates ADD COLUMN rakuten_title TEXT;`);
db.exec(`ALTER TABLE migration_candidates ADD COLUMN last_title_synced_at TEXT;`);
// migration 012 — genre 列
db.exec(`ALTER TABLE migration_candidates ADD COLUMN rakuten_genre_id TEXT;`);
db.exec(`ALTER TABLE migration_candidates ADD COLUMN rakuten_genre_path TEXT;`);

const NOW = '2026-06-14T10:00:00Z';
const seed = db.prepare(`
  INSERT INTO migration_candidates
    (item_code, rakuten_manage_number, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, status)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);
seed.run('A001', 'mn-a001', NOW, NOW, NOW, NOW, 'candidate');
seed.run('A002', 'mn-a002', NOW, NOW, NOW, NOW, 'candidate');
seed.run('A003', 'mn-a003', NOW, NOW, NOW, NOW, 'candidate');
seed.run('A004', 'mn-a004', NOW, NOW, NOW, NOW, 'candidate');
seed.run('A005-fail', 'mn-a005', NOW, NOW, NOW, NOW, 'candidate');
seed.run('R001', 'mn-r001', NOW, NOW, NOW, NOW, 'resolved');   // resolved: 触らない
seed.run('S001', 'mn-s001', NOW, NOW, NOW, NOW, 'stale');      // stale: 触る
// Phase E-9 後: A001 だけ既に title 入ってる前提 (再 backfill 対象から除外を確認)
db.prepare(`UPDATE migration_candidates SET rakuten_title = ? WHERE item_code = ?`).run('既に取得済み', 'A001');

expect('initial missing count = 5 (A002-A005-fail + S001、 A001 は埋まってる、 R001 は resolved)',
  countMissingRakutenTitles(db) === 5, `count=${countMissingRakutenTitles(db)}`);

// mock fetchItemDetailsBulkDetailed
const mockBulk = async (manageNumbers) => {
  // A005-fail だけ failed、 他は itemName + genre を返す
  // mn-a002 だけ top-level genreId、 mn-a003 は genres[]、 mn-a004 は categories[]、 mn-s001 は genre なし
  const items = [];
  const failed = [];
  for (const mn of manageNumbers) {
    if (mn === 'mn-a005') {
      failed.push({ manageNumber: mn, reason: 'rms_500' });
      continue;
    }
    const item = { manageNumber: mn, itemName: `Title for ${mn}` };
    if (mn === 'mn-a002') item.genreId = '100371';
    else if (mn === 'mn-a003') item.genres = [{ genreId: 200500, genrePath: '雑貨 > 文具' }];
    else if (mn === 'mn-a004') item.categories = [{ categoryId: 300, path: '日用品 > 洗剤' }];
    // mn-s001 は genre 情報なし → null のまま、 既存値を維持
    items.push(item);
  }
  return { items, failed };
};

const r = await backfillRakutenTitles({ db, limit: 100, deps: { fetchItemDetailsBulkDetailed: mockBulk } });

expect('picked = 5 (A002,A003,A004,A005-fail,S001)', r.picked === 5, JSON.stringify(r));
expect('updated = 4 (A005-fail 除外)', r.updated === 4, JSON.stringify(r));
expect('failed = 1 (A005-fail)', r.failed === 1, JSON.stringify(r));
expect('errors[0] mentions A005', r.errors[0] && /mn-a005.*rms_500/.test(r.errors[0]), r.errors[0]);

const after = db.prepare('SELECT item_code, rakuten_title FROM migration_candidates ORDER BY item_code').all();
const map = Object.fromEntries(after.map((r) => [r.item_code, r.rakuten_title]));
expect('A001 既存 title は変更されない', map['A001'] === '既に取得済み', map['A001']);
expect('A002 title 入った', map['A002'] === 'Title for mn-a002', map['A002']);
expect('A005-fail title 入ってない', !map['A005-fail'], `title=${map['A005-fail']}`);
expect('R001 (resolved) title 触ってない', !map['R001'], `title=${map['R001']}`);
expect('S001 (stale) title 入った', map['S001'] === 'Title for mn-s001', map['S001']);

expect('remaining missing = 1 (A005-fail のみ)', countMissingRakutenTitles(db) === 1,
  `count=${countMissingRakutenTitles(db)}`);

// 再 backfill (A005-fail を再試行できる、 ただし同じく失敗)
const r2 = await backfillRakutenTitles({ db, limit: 100, deps: { fetchItemDetailsBulkDetailed: mockBulk } });
expect('再実行 picked = 1 (A005-fail 残)', r2.picked === 1, JSON.stringify(r2));
expect('再実行 updated = 0', r2.updated === 0, JSON.stringify(r2));

// last_title_synced_at が success で記録される
const ts = db.prepare(`SELECT last_title_synced_at FROM migration_candidates WHERE item_code = 'A002'`).get();
expect('A002 last_title_synced_at が ISO で入る', !!ts.last_title_synced_at && /^\d{4}-\d{2}-\d{2}T/.test(ts.last_title_synced_at),
  ts.last_title_synced_at);

// ───── Phase E-11-a: genre 抽出 + 保存 ─────

// extractRakutenGenre の単体動作
expect('extractRakutenGenre: top-level genreId',
  extractRakutenGenre({ genreId: 100371 }).genreId === '100371');
expect('extractRakutenGenre: genres[]',
  extractRakutenGenre({ genres: [{ genreId: 200500, genrePath: '雑貨 > 文具' }] }).genreId === '200500');
expect('extractRakutenGenre: genres[] path',
  extractRakutenGenre({ genres: [{ genreId: 200500, genrePath: '雑貨 > 文具' }] }).genrePath === '雑貨 > 文具');
expect('extractRakutenGenre: categories[] fallback',
  extractRakutenGenre({ categories: [{ categoryId: 300, path: '日用品 > 洗剤' }] }).genreId === '300');
expect('extractRakutenGenre: empty → null',
  extractRakutenGenre({}).genreId === null && extractRakutenGenre({}).genrePath === null);
expect('extractRakutenGenre: null safe', extractRakutenGenre(null).genreId === null);

// DB 保存の確認: A002 (top-level genreId='100371'), A003 (genres[]), A004 (categories[])
const after2 = db.prepare(`
  SELECT item_code, rakuten_genre_id, rakuten_genre_path
  FROM migration_candidates
  WHERE item_code IN ('A002','A003','A004','S001','A005-fail','R001','A001')
  ORDER BY item_code
`).all();
const gmap = Object.fromEntries(after2.map((r) => [r.item_code, r]));
expect('A002 genreId=100371 (top-level)', gmap['A002'].rakuten_genre_id === '100371',
  JSON.stringify(gmap['A002']));
expect('A003 genreId=200500 (genres[])', gmap['A003'].rakuten_genre_id === '200500',
  JSON.stringify(gmap['A003']));
expect('A003 genrePath=雑貨 > 文具', gmap['A003'].rakuten_genre_path === '雑貨 > 文具');
expect('A004 genreId=300 (categories[]) + path',
  gmap['A004'].rakuten_genre_id === '300' && gmap['A004'].rakuten_genre_path === '日用品 > 洗剤');
expect('S001 genre 入ってない (RMS が返さなかった、 COALESCE で既存維持=null)',
  gmap['S001'].rakuten_genre_id === null);
expect('A001 (既に title 入ってる) は触られない',
  gmap['A001'].rakuten_genre_id === null && gmap['A001'].rakuten_genre_path === null);

const failed = checks.filter((c) => !c.ok);
if (failed.length > 0) {
  console.error(`\n❌ ${failed.length}/${checks.length} checks failed`);
  process.exit(1);
}
console.log(`\n✅ all ${checks.length} checks passed`);
