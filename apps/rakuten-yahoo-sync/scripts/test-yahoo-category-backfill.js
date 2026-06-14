/**
 * Phase E-11-b smoke: Yahoo category backfill + 学習辞書。
 *
 * - in-memory DB に migration 005/006/011/012/013 抜粋を適用
 * - yahoo_registered_items 5 件 (Yahoo 出品) + migration_candidates 5 件 (楽天 candidate) を seed
 * - fetchYahooItemDetailsBulk をモック (3 success + 1 fail + 1 ProductCategory=null)
 * - backfillYahooCategoriesAndPaths → updated 3 (null は skip)
 * - learnGenreCategoryMapping → genre 集計、 各 genre で primary 設定
 *
 * 使い方: node apps/rakuten-yahoo-sync/scripts/test-yahoo-category-backfill.js
 */

import Database from 'better-sqlite3';
import {
  countMissingYahooCategories,
  backfillYahooCategoriesAndPaths,
  learnGenreCategoryMapping,
  countLearnedGenres,
} from '../lib/yahoo-category-backfill.js';

const checks = [];
function expect(label, cond, detail) {
  checks.push({ label, ok: !!cond, detail });
  console.log(`${cond ? '✅' : '❌'} ${label}${detail ? ` — ${detail}` : ''}`);
}

const db = new Database(':memory:');
db.exec(`
  CREATE TABLE yahoo_registered_items (
    item_code TEXT PRIMARY KEY,
    yahoo_item_code TEXT,
    has_sub_code INTEGER DEFAULT 0,
    last_seen_at TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'baseline',
    yahoo_category_id INTEGER,
    yahoo_path TEXT,
    last_detail_synced_at TEXT
  );
  CREATE TABLE migration_candidates (
    item_code TEXT PRIMARY KEY,
    rakuten_manage_number TEXT,
    first_detected_at TEXT NOT NULL,
    last_detected_at TEXT NOT NULL,
    last_seen_in_rakuten_at TEXT NOT NULL,
    last_checked_yahoo_baseline_at TEXT NOT NULL,
    missing_rakuten_count INTEGER NOT NULL DEFAULT 0,
    stale_at TEXT,
    status TEXT NOT NULL,
    resolved_at TEXT,
    source TEXT NOT NULL DEFAULT 'diff_detector',
    rakuten_title TEXT,
    last_title_synced_at TEXT,
    rakuten_genre_id TEXT,
    rakuten_genre_path TEXT
  );
  CREATE TABLE genre_yahoo_category_mapping (
    rakuten_genre_id TEXT NOT NULL,
    yahoo_category_id INTEGER NOT NULL,
    yahoo_path TEXT NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 1,
    is_primary INTEGER NOT NULL DEFAULT 0,
    first_learned_at TEXT NOT NULL,
    last_learned_at TEXT NOT NULL,
    PRIMARY KEY (rakuten_genre_id, yahoo_category_id, yahoo_path)
  );
`);

const NOW = '2026-06-14T10:00:00Z';
const yi = db.prepare(`INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, last_seen_at, source) VALUES (?, ?, ?, 'baseline')`);
yi.run('Y001', 'Y001', NOW);
yi.run('Y002', 'Y002', NOW);
yi.run('Y003', 'Y003', NOW);
yi.run('Y004', 'Y004', NOW);
yi.run('Y005', 'Y005', NOW);

const ci = db.prepare(`INSERT INTO migration_candidates (item_code, rakuten_manage_number, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, status, rakuten_genre_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
// 楽天 candidate: Y001-Y005 と同じ item_code = 楽天 itemNumber でも同じだと仮定
ci.run('Y001', 'mn-y001', NOW, NOW, NOW, NOW, 'resolved', '100371'); // 雑貨
ci.run('Y002', 'mn-y002', NOW, NOW, NOW, NOW, 'resolved', '100371'); // 雑貨 (同 genre)
ci.run('Y003', 'mn-y003', NOW, NOW, NOW, NOW, 'resolved', '200500'); // 文具 (別 genre)
ci.run('Y004', 'mn-y004', NOW, NOW, NOW, NOW, 'resolved', '100371'); // 雑貨 (同 genre、 でも別 path)
ci.run('Y005', 'mn-y005', NOW, NOW, NOW, NOW, 'resolved', '999999'); // 単独 genre

expect('initial missing yahoo categories = 5', countMissingYahooCategories(db) === 5);

// mock Yahoo getItemDetail
//   Y001-Y003 success / Y004 success but category=null / Y005 failed
const mockBulk = async (itemCodes) => {
  const items = [];
  const failed = [];
  for (const c of itemCodes) {
    if (c === 'Y005') { failed.push({ itemCode: c, reason: 'rms_500' }); continue; }
    if (c === 'Y004') { items.push({ ItemCode: c, ProductCategory: null, Path: null, Name: 'Y004' }); continue; }
    if (c === 'Y001') items.push({ ItemCode: c, ProductCategory: 12345, Path: 'zakka/main', Name: 'Y001' });
    else if (c === 'Y002') items.push({ ItemCode: c, ProductCategory: 12345, Path: 'zakka/main', Name: 'Y002' });
    else if (c === 'Y003') items.push({ ItemCode: c, ProductCategory: 99999, Path: 'bungu', Name: 'Y003' });
  }
  return { items, failed };
};

const r = await backfillYahooCategoriesAndPaths({ db, limit: 100, deps: { fetchYahooItemDetailsBulk: mockBulk } });

expect('picked = 5', r.picked === 5, JSON.stringify(r));
expect('updated = 3 (Y004 category=null は skip)', r.updated === 3, JSON.stringify(r));
expect('failed = 1 (Y005)', r.failed === 1, JSON.stringify(r));

const after = db.prepare(`SELECT item_code, yahoo_category_id, yahoo_path FROM yahoo_registered_items ORDER BY item_code`).all();
const map = Object.fromEntries(after.map((r) => [r.item_code, r]));
expect('Y001 category=12345 path=zakka/main', map['Y001'].yahoo_category_id === 12345 && map['Y001'].yahoo_path === 'zakka/main');
expect('Y004 category 入ってない (response が null だったので skip)', map['Y004'].yahoo_category_id === null);
expect('Y005 category 入ってない (failed)', map['Y005'].yahoo_category_id === null);
expect('remaining = 2 (Y004 + Y005)', countMissingYahooCategories(db) === 2);

// 学習辞書を構築
const lr = learnGenreCategoryMapping(db);
// genre 100371: Y001+Y002 が path=zakka/main (count=2), Y004 は category null だから含まれない → 1 mapping
// genre 200500: Y003 が path=bungu (count=1) → 1 mapping
// genre 999999: Y005 failed なので候補がない → 学習対象外
expect('learned 2 genres', lr.genresLearned === 2, JSON.stringify(lr));
expect('upserted 2 mapping rows', lr.mappingRowsUpserted === 2, JSON.stringify(lr));
expect('primariesSet = 2 (各 genre で 1 つずつ)', lr.primariesSet === 2, JSON.stringify(lr));

const learned = db.prepare(`SELECT * FROM genre_yahoo_category_mapping ORDER BY rakuten_genre_id`).all();
expect('100371 primary = (12345, zakka/main, count=2)',
  learned.find((r) => r.rakuten_genre_id === '100371')?.sample_count === 2 &&
  learned.find((r) => r.rakuten_genre_id === '100371')?.is_primary === 1);
expect('200500 primary = (99999, bungu, count=1)',
  learned.find((r) => r.rakuten_genre_id === '200500')?.is_primary === 1);

expect('countLearnedGenres = 2', countLearnedGenres(db) === 2);

// 再学習でも結果が同じ (idempotent)
const lr2 = learnGenreCategoryMapping(db);
expect('再学習 genresLearned = 2', lr2.genresLearned === 2);
const learned2 = db.prepare(`SELECT COUNT(*) AS n FROM genre_yahoo_category_mapping`).get();
expect('再学習後も mapping rows = 2', learned2.n === 2);

const failed = checks.filter((c) => !c.ok);
if (failed.length > 0) {
  console.error(`\n❌ ${failed.length}/${checks.length} checks failed`);
  process.exit(1);
}
console.log(`\n✅ all ${checks.length} checks passed`);
