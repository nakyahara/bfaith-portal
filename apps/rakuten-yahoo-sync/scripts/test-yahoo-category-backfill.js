/**
 * Phase E-11-b + Codex 2 round-2 smoke: ne_code 軸 overlap での backfill / 学習辞書。
 *
 * - in-memory DB に RYS schema (migration 005/006/011/012/013) + mirror DB に
 *   m_products / f_yahoo_sku_map / f_rakuten_finance_sku_daily_v1 を seed
 * - ne_code 経由で 楽天 itemNumber と Yahoo ItemCode が違う命名でも結合できることを確認
 * - Codex H 反映の検証: ambiguous (1 yahoo → 多 rakuten) 除外、 fr 日次 dedupe、
 *   m/fy conflict 除外、 mirror 無し fail-closed
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

// RYS 用 in-memory DB
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
  CREATE TABLE m_products (
    "商品コード" TEXT UNIQUE NOT NULL,
    "商品名"     TEXT
  );
  CREATE TABLE f_yahoo_sku_map (
    yahoo_key  TEXT NOT NULL PRIMARY KEY,
    ne_code    TEXT NOT NULL
  );
  CREATE TABLE f_rakuten_finance_sku_daily_v1 (
    date_jst       TEXT NOT NULL,
    rakuten_code   TEXT NOT NULL,
    ne_code        TEXT,
    PRIMARY KEY (date_jst, rakuten_code)
  );
`);
mirrorDb.close();

const NOW = '2026-06-14T10:00:00Z';
const yi = db.prepare(`INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, last_seen_at, source) VALUES (?, ?, ?, 'baseline')`);
// Yahoo 出品 (b-faith01 ストア命名)
yi.run('0726-001001', '0726-001001', NOW);  // → ne_code=zakka01, 楽天=aburatoishioil100 (1:1)
yi.run('0726-001002', '0726-001002', NOW);  // → ne_code=zakka02, 楽天=oilstoneoil200  (1:1)
yi.run('0726-001003', '0726-001003', NOW);  // → ne_code=bungu01,  楽天=algi50g          (1:1、 別genre)
yi.run('0726-001004', '0726-001004', NOW);  // → ne_code=zakka03, 楽天=ITEM_X (ambig - 複数 rakuten 解決)
yi.run('0726-001005', '0726-001005', NOW);  // m と fy conflict (fail-closed 対象)
yi.run('0726-001006', '0726-001006', NOW);  // ne_code 解決できない (none)

// 楽天 migration_candidates (楽天 itemNumber 命名)
const ci = db.prepare(`INSERT INTO migration_candidates (item_code, rakuten_manage_number, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, status, rakuten_genre_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
ci.run('aburatoishioil100', 'mn-1', NOW, NOW, NOW, NOW, 'candidate', '100371');
ci.run('oilstoneoil200',    'mn-2', NOW, NOW, NOW, NOW, 'candidate', '100371');
ci.run('algi50g',           'mn-3', NOW, NOW, NOW, NOW, 'candidate', '200500');
ci.run('ITEM_X',            'mn-4', NOW, NOW, NOW, NOW, 'candidate', '999999');
ci.run('ITEM_Y',            'mn-5', NOW, NOW, NOW, NOW, 'candidate', '999999');  // ITEM_X と同じ ne_code 解決 → ambiguous

// mirror DB に seed (ATTACH してから直接書く)
db.prepare(`ATTACH DATABASE ? AS wh`).run(tmpMirror);
const mp = db.prepare(`INSERT INTO wh.m_products ("商品コード", "商品名") VALUES (?, ?)`);
mp.run('0726-001001', 'グレンケアン');     // Yahoo item → ne_code=0726-001001 だが、 そこからは fr ne_code と JOIN
// m_products は商品コードが ne_code そのもの。 ne_code を共通軸として、 楽天/Yahoo はそれぞれ別 ItemCode で繋がる。
// シンプルにするために: yahoo_key = Yahoo ItemCode = m_products.商品コード = ne_code
// 楽天 itemNumber は fr.rakuten_code、 fr.ne_code が m_products.商品コード と一致する経路で繋がる
mp.run('0726-001002', '砥石オイル');
mp.run('0726-001003', 'アルギン酸');
mp.run('0726-001004', 'XY 共有');  // ambiguous 元
// 0726-001005 は m_products と fy で別 ne_code が出る conflict ケース
mp.run('0726-001005', 'ne-from-m');
// 0726-001006 はどちらにも無い

// f_yahoo_sku_map (補完)
const fy = db.prepare(`INSERT INTO wh.f_yahoo_sku_map (yahoo_key, ne_code) VALUES (?, ?)`);
fy.run('0726-001005', 'ne-from-fy');  // conflict (m と異なる)

// f_rakuten_finance_sku_daily_v1 (日次 fact、 同 rakuten_code が複数日)
const fr = db.prepare(`INSERT INTO wh.f_rakuten_finance_sku_daily_v1 (date_jst, rakuten_code, ne_code) VALUES (?, ?, ?)`);
// aburatoishioil100 → ne_code=0726-001001、 2 日分 (日次重複)
fr.run('2026-06-01', 'aburatoishioil100', '0726-001001');
fr.run('2026-06-02', 'aburatoishioil100', '0726-001001');
// oilstoneoil200 → 0726-001002
fr.run('2026-06-01', 'oilstoneoil200', '0726-001002');
// algi50g → 0726-001003
fr.run('2026-06-01', 'algi50g', '0726-001003');
// ambiguous: ITEM_X と ITEM_Y 両方が ne_code=0726-001004 に解決される
fr.run('2026-06-01', 'ITEM_X', '0726-001004');
fr.run('2026-06-01', 'ITEM_Y', '0726-001004');

db.prepare('DETACH DATABASE wh').run();

// ───── 診断 ─────
const diag = diagnoseOverlap({ db, mirrorPath: tmpMirror });
expect('diag.ok=true', diag.ok === true, JSON.stringify(diag));
expect('diag.totalYahooScanned=6', diag.totalYahooScanned === 6, JSON.stringify(diag));
expect('diag.m_only=4 (0726-001001/2/3/4)', diag.m_only === 4, JSON.stringify(diag));
expect('diag.both_conflict=1 (0726-001005)', diag.both_conflict === 1, JSON.stringify(diag));
expect('diag.none=1 (0726-001006)', diag.none === 1, JSON.stringify(diag));
expect('diag.linked=4 (conflict と none を除く)', diag.linked === 4, JSON.stringify(diag));
expect('diag.ambiguous=1 (0726-001004 が ITEM_X+ITEM_Y で解決)', diag.ambiguous === 1, JSON.stringify(diag));

// ───── countMissing (1 ambiguous を除外) ─────
expect('missing = 3 (linked 4 から ambiguous 1 件除外)',
  countMissingYahooCategories(db, tmpMirror) === 3,
  `got ${countMissingYahooCategories(db, tmpMirror)}`);

// ───── backfill ─────
const mockBulk = async (itemCodes) => {
  const items = [];
  const failed = [];
  for (const c of itemCodes) {
    if (c === '0726-001003') {
      items.push({ ItemCode: c, ProductCategory: 99999, Path: 'bungu', Name: 'algi' });
    } else if (c === '0726-001001') {
      items.push({ ItemCode: c, ProductCategory: 12345, Path: 'zakka/main', Name: 'oil' });
    } else if (c === '0726-001002') {
      items.push({ ItemCode: c, ProductCategory: 12345, Path: 'zakka/main', Name: 'oilstone' });
    }
  }
  return { items, failed };
};
const r = await backfillYahooCategoriesAndPaths({ db, mirrorPath: tmpMirror, limit: 100, deps: { fetchYahooItemDetailsBulk: mockBulk } });
expect('backfill picked = 3 (ambiguous 1 件除外)', r.picked === 3, JSON.stringify(r));
expect('backfill updated = 3 (全 success)', r.updated === 3, JSON.stringify(r));

// 確認
const after = db.prepare(`SELECT item_code, yahoo_category_id, yahoo_path FROM yahoo_registered_items ORDER BY item_code`).all();
const map = Object.fromEntries(after.map((x) => [x.item_code, x]));
expect('0726-001001 category=12345', map['0726-001001'].yahoo_category_id === 12345);
expect('0726-001003 category=99999', map['0726-001003'].yahoo_category_id === 99999);
expect('0726-001004 (ambiguous) は触られない', map['0726-001004'].yahoo_category_id === null);
expect('0726-001005 (conflict) は触られない', map['0726-001005'].yahoo_category_id === null);
expect('0726-001006 (none) は触られない', map['0726-001006'].yahoo_category_id === null);

// ───── learn ─────
const lr = learnGenreCategoryMapping({ db, mirrorPath: tmpMirror });
// genre 100371: aburatoishioil100 + oilstoneoil200 が両方 (12345, zakka/main) → count=2
// genre 200500: algi50g が (99999, bungu) → count=1
// 999999 は ambiguous で除外
expect('learn genresLearned=2 (100371 + 200500)', lr.genresLearned === 2, JSON.stringify(lr));
expect('learn mappingRowsUpserted=2', lr.mappingRowsUpserted === 2, JSON.stringify(lr));
expect('learn primariesSet=2', lr.primariesSet === 2, JSON.stringify(lr));

const learned = db.prepare(`SELECT * FROM genre_yahoo_category_mapping ORDER BY rakuten_genre_id`).all();
expect('100371 primary sample_count=2 (Codex H1 fr dedupe 効果: 2日分 fact あるが 1 商品 1 票)',
  learned.find((r) => r.rakuten_genre_id === '100371')?.sample_count === 2 &&
  learned.find((r) => r.rakuten_genre_id === '100371')?.is_primary === 1);
expect('200500 primary sample_count=1',
  learned.find((r) => r.rakuten_genre_id === '200500')?.is_primary === 1);
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
expect('mirror 無し → blocked=true で空 (fail-closed)',
  noMirror.blocked === true && noMirror.picked === 0, JSON.stringify(noMirror));

const noMirrorLearn = learnGenreCategoryMapping({ db: dbNoMirror, mirrorPath: null });
expect('mirror 無し learn → blocked=true で空',
  noMirrorLearn.blocked === true && noMirrorLearn.genresLearned === 0, JSON.stringify(noMirrorLearn));

dbNoMirror.close();
db.close();
try { fs.unlinkSync(tmpMirror); } catch (_) {}

const failedChecks = checks.filter((c) => !c.ok);
if (failedChecks.length > 0) {
  console.error(`\n❌ ${failedChecks.length}/${checks.length} checks failed`);
  process.exit(1);
}
console.log(`\n✅ all ${checks.length} checks passed`);
