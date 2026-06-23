/**
 * Phase E-11-b + Codex R2 + mirror_sku_resolved 切り替え smoke。
 *
 * - mirror_sku_resolved は全モール seller_sku × ne_code 統一辞書
 *   (Render mirror DB schema 確認済、 m_products や f_yahoo_sku_map は mirror に居ない)
 * - sort_order=0 が代表 ne_code
 * - 楽天 itemNumber と Yahoo ItemCode は別 seller_sku として登録される
 * - 両者の ne_code が一致したものを同商品として overlap 計算
 */

import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import os from 'os';
import {
  countMissingYahooCategories,
  backfillYahooCategoriesAndPaths,
  learnGenreCategoryMapping,
  countLearnedGenres,
  diagnoseOverlap,
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

// mirror DB (warehouse-mirror.db 模擬) を一時ファイルで作成
const tmpMirror = path.join(os.tmpdir(), `rys-test-mirror-${process.pid}-${Date.now()}.db`);
const mirrorDb = new Database(tmpMirror);
mirrorDb.exec(`
  CREATE TABLE mirror_yahoo_finance_sku_daily (
    date_jst       TEXT NOT NULL,
    yahoo_sku_key  TEXT NOT NULL,
    ne_code        TEXT,
    PRIMARY KEY (date_jst, yahoo_sku_key)
  );
  CREATE TABLE mirror_rakuten_finance_sku_daily (
    date_jst       TEXT NOT NULL,
    rakuten_code   TEXT NOT NULL,
    ne_code        TEXT,
    PRIMARY KEY (date_jst, rakuten_code)
  );
`);
mirrorDb.close();

const NOW = '2026-06-23T10:00:00Z';

// Yahoo 出品
const yi = db.prepare(`INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, last_seen_at, source) VALUES (?, ?, ?, 'baseline')`);
yi.run('0726-001001', '0726-001001', NOW);  // → ne=NE001, 楽天 abura100 と一致
yi.run('0726-001002', '0726-001002', NOW);  // → ne=NE002, 楽天 oilstone200 と一致
yi.run('0726-001003', '0726-001003', NOW);  // → ne=NE003, 楽天 algi50 と一致 (別 genre)
yi.run('0726-001004', '0726-001004', NOW);  // → ne=NE004, ambiguous (楽天 ITEM_X + ITEM_Y 両方が NE004)
yi.run('0726-001005', '0726-001005', NOW);  // mirror_sku_resolved に居ない (未解決)

// migration_candidates
const ci = db.prepare(`INSERT INTO migration_candidates (item_code, rakuten_manage_number, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, status, rakuten_genre_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
ci.run('abura100',      'mn-1', NOW, NOW, NOW, NOW, 'candidate', '100371');
ci.run('oilstone200',   'mn-2', NOW, NOW, NOW, NOW, 'candidate', '100371');
ci.run('algi50',        'mn-3', NOW, NOW, NOW, NOW, 'candidate', '200500');
ci.run('ITEM_X',        'mn-4', NOW, NOW, NOW, NOW, 'candidate', '999999');
ci.run('ITEM_Y',        'mn-5', NOW, NOW, NOW, NOW, 'candidate', '999999');

// daily fact mirror に seed (Codex H1 dedupe を検証するため日次重複も入れる)
db.prepare(`ATTACH DATABASE ? AS wh`).run(tmpMirror);
const yf = db.prepare(`INSERT INTO wh.mirror_yahoo_finance_sku_daily (date_jst, yahoo_sku_key, ne_code) VALUES (?, ?, ?)`);
yf.run('2026-06-01', '0726-001001', 'NE001');
yf.run('2026-06-02', '0726-001001', 'NE001');  // 日次重複
yf.run('2026-06-01', '0726-001002', 'NE002');
yf.run('2026-06-01', '0726-001003', 'NE003');
yf.run('2026-06-01', '0726-001004', 'NE004');
// 0726-001005 は yahoo daily fact にも居ない
const rf = db.prepare(`INSERT INTO wh.mirror_rakuten_finance_sku_daily (date_jst, rakuten_code, ne_code) VALUES (?, ?, ?)`);
rf.run('2026-06-01', 'abura100',    'NE001');
rf.run('2026-06-02', 'abura100',    'NE001');  // 日次重複
rf.run('2026-06-01', 'oilstone200', 'NE002');
rf.run('2026-06-01', 'algi50',      'NE003');
rf.run('2026-06-01', 'ITEM_X',      'NE004');  // ambiguous
rf.run('2026-06-01', 'ITEM_Y',      'NE004');
db.prepare('DETACH DATABASE wh').run();

// ───── 診断 ─────
const diag = diagnoseOverlap({ db, mirrorPath: tmpMirror });
expect('diag.ok=true', diag.ok === true, JSON.stringify(diag));
expect('diag.totalYahooScanned=5', diag.totalYahooScanned === 5, JSON.stringify(diag));
expect('diag.yahoo_resolved_to_ne=4 (0726-001005 は未解決)', diag.yahoo_resolved_to_ne === 4, JSON.stringify(diag));
expect('diag.yahoo_unresolved=1', diag.yahoo_unresolved === 1, JSON.stringify(diag));
expect('diag.linked=4 (4 yahoo が 楽天と ne_code で繋がる)', diag.linked === 4, JSON.stringify(diag));
expect('diag.ambiguous=1 (0726-001004 が ITEM_X + ITEM_Y で解決)', diag.ambiguous === 1, JSON.stringify(diag));

// missing = linked から ambiguous 1 件除外
expect('missing = 3', countMissingYahooCategories(db, tmpMirror) === 3,
  `got ${countMissingYahooCategories(db, tmpMirror)}`);

// ───── backfill ─────
const mockBulk = async (itemCodes) => {
  const items = [];
  const failed = [];
  for (const c of itemCodes) {
    if (c === '0726-001001') items.push({ ItemCode: c, ProductCategory: 12345, Path: 'zakka/main', Name: 'oil' });
    else if (c === '0726-001002') items.push({ ItemCode: c, ProductCategory: 12345, Path: 'zakka/main', Name: 'oilstone' });
    else if (c === '0726-001003') items.push({ ItemCode: c, ProductCategory: 99999, Path: 'bungu', Name: 'algi' });
  }
  return { items, failed };
};
const r = await backfillYahooCategoriesAndPaths({ db, mirrorPath: tmpMirror, limit: 100, deps: { fetchYahooItemDetailsBulk: mockBulk } });
expect('backfill picked = 3 (ambiguous 1 件除外)', r.picked === 3, JSON.stringify(r));
expect('backfill updated = 3 全 success', r.updated === 3, JSON.stringify(r));

const after = db.prepare(`SELECT item_code, yahoo_category_id, yahoo_path FROM yahoo_registered_items ORDER BY item_code`).all();
const map = Object.fromEntries(after.map((x) => [x.item_code, x]));
expect('0726-001001 category=12345', map['0726-001001'].yahoo_category_id === 12345);
expect('0726-001003 category=99999', map['0726-001003'].yahoo_category_id === 99999);
expect('0726-001004 (ambiguous) 触られない', map['0726-001004'].yahoo_category_id === null);
expect('0726-001005 (未解決) 触られない', map['0726-001005'].yahoo_category_id === null);

// ───── learn ─────
const lr = learnGenreCategoryMapping({ db, mirrorPath: tmpMirror });
expect('learn genresLearned=2', lr.genresLearned === 2, JSON.stringify(lr));
expect('learn mappingRowsUpserted=2', lr.mappingRowsUpserted === 2, JSON.stringify(lr));
expect('learn primariesSet=2', lr.primariesSet === 2, JSON.stringify(lr));

const learned = db.prepare(`SELECT * FROM genre_yahoo_category_mapping ORDER BY rakuten_genre_id`).all();
expect('100371 primary sample_count=2 (abura100+oilstone200 が同 triple)',
  learned.find((r) => r.rakuten_genre_id === '100371')?.sample_count === 2 &&
  learned.find((r) => r.rakuten_genre_id === '100371')?.is_primary === 1);
expect('200500 primary sample_count=1 (algi50)',
  learned.find((r) => r.rakuten_genre_id === '200500')?.sample_count === 1);
expect('999999 (ambiguous) は学習辞書に入らない',
  !learned.find((r) => r.rakuten_genre_id === '999999'));

// ───── mirror 無し fail-closed ─────
const dbNoMirror = new Database(':memory:');
dbNoMirror.exec(`
  CREATE TABLE yahoo_registered_items (item_code TEXT PRIMARY KEY, yahoo_category_id INTEGER, last_seen_at TEXT NOT NULL);
  CREATE TABLE migration_candidates (item_code TEXT PRIMARY KEY, rakuten_genre_id TEXT);
  CREATE TABLE genre_yahoo_category_mapping (rakuten_genre_id TEXT, yahoo_category_id INTEGER, yahoo_path TEXT, sample_count INTEGER, is_primary INTEGER, first_learned_at TEXT, last_learned_at TEXT, PRIMARY KEY (rakuten_genre_id, yahoo_category_id, yahoo_path));
`);
const noMirror = await backfillYahooCategoriesAndPaths({ db: dbNoMirror, mirrorPath: null, limit: 10 });
expect('mirror 無し fail-closed', noMirror.blocked === true && noMirror.picked === 0);

const noMirrorLearn = learnGenreCategoryMapping({ db: dbNoMirror, mirrorPath: null });
expect('mirror 無し learn fail-closed', noMirrorLearn.blocked === true && noMirrorLearn.genresLearned === 0);

// ───── mirror_sku_resolved 無し fail-closed ─────
const tmpMirrorEmpty = path.join(os.tmpdir(), `rys-test-mirror-empty-${process.pid}-${Date.now()}.db`);
const emptyMirror = new Database(tmpMirrorEmpty);
emptyMirror.exec(`CREATE TABLE dummy (id INTEGER)`);
emptyMirror.close();
const diagEmpty = diagnoseOverlap({ db, mirrorPath: tmpMirrorEmpty });
expect('mirror_sku_resolved 無し → ok=false, code=MIRROR_TABLE_MISSING',
  diagEmpty.ok === false && diagEmpty.code === 'MIRROR_TABLE_MISSING', JSON.stringify(diagEmpty));

dbNoMirror.close();
db.close();
try { fs.unlinkSync(tmpMirror); } catch (_) {}
try { fs.unlinkSync(tmpMirrorEmpty); } catch (_) {}

const failedChecks = checks.filter((c) => !c.ok);
if (failedChecks.length > 0) {
  console.error(`\n❌ ${failedChecks.length}/${checks.length} checks failed`);
  process.exit(1);
}
console.log(`\n✅ all ${checks.length} checks passed`);
