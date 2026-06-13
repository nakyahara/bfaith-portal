/**
 * E-7-c smoke runner: migration 008 + lib/exclusion.js の動作確認 (in-memory SQLite)。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/test-exclusion.js
 *
 * 検証項目:
 *   - migration 008 schema (CHECK / UNIQUE INDEX partial / 履歴 index)
 *   - excludeMigrationItem:
 *       (1) 新規除外 (active 1 行)
 *       (2) 引数 validation (kind / itemCode / reason)
 *       (3) 既 active で 2 度目除外は throw EXCLUSION_ALREADY_ACTIVE
 *       (4) 別商品の同 kind は同時 active OK
 *   - restoreMigrationExclusion:
 *       (5) 復元 → active row が消える + 履歴は残る
 *       (6) 復元 → 同じ商品を再除外 OK (履歴 2 件)
 *       (7) active なし状態で復元 → throw EXCLUSION_NOT_ACTIVE
 *   - changeExclusionKind:
 *       (8) temporary → permanent: 旧 restore + 新 insert 1 transaction
 *       (9) 同 kind は throw EXCLUSION_KIND_UNCHANGED
 *       (10) active なし状態で変更 → throw EXCLUSION_NOT_ACTIVE
 *   - listActiveExclusions:
 *       (11) 全件、 kind フィルタ、 search フィルタ
 *   - listExclusionHistory:
 *       (12) 1 商品の全イベント (active + restored)
 *   - countActiveByKind:
 *       (13) { temporary, permanent, total }
 *   - kindLabel:
 *       (14) 日本語ラベル変換
 */

import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  excludeMigrationItem,
  restoreMigrationExclusion,
  changeExclusionKind,
  listActiveExclusions,
  listExclusionHistory,
  countActiveByKind,
  kindLabel,
} from '../lib/exclusion.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_DIR = path.join(__dirname, '..', 'db', 'migrations');

let failed = 0;
function ok(name) { console.log(`OK   ${name}`); }
function fail(name, msg) { failed += 1; console.error(`FAIL ${name}: ${msg}`); }
function expect(name, cond, msg) { if (cond) ok(name); else fail(name, msg); }

function freshDb() {
  const db = new Database(':memory:');
  const files = fs.readdirSync(MIGRATIONS_DIR).filter((f) => f.endsWith('.sql')).sort();
  for (const f of files) db.exec(fs.readFileSync(path.join(MIGRATIONS_DIR, f), 'utf8'));
  return db;
}

// ──── テスト ────

function testMigration() {
  const db = freshDb();
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map((r) => r.name);
  expect('migration: migration_excluded created', tables.includes('migration_excluded'), `tables=${tables.join(',')}`);

  // exclude_kind CHECK
  try {
    db.prepare("INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at) VALUES ('x', 'bogus', 'r', '2026-01-01T00:00:00Z')").run();
    fail('migration: exclude_kind CHECK', 'bogus accepted');
  } catch (e) { expect('migration: exclude_kind CHECK rejects bogus', /CHECK/.test(e.message), e.message); }

  // restored_at NULL + restored_reason set は CHECK 違反
  try {
    db.prepare("INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at, restored_reason) VALUES ('x', 'temporary', 'r', '2026-01-01T00:00:00Z', 'cant happen')").run();
    fail('migration: restored_at NULL + restored_reason set CHECK', 'accepted');
  } catch (e) { expect('migration: restored_at NULL + restored_reason set is rejected', /CHECK/.test(e.message), e.message); }

  // partial UNIQUE INDEX: 同 item_code で active 2 行は NG
  db.prepare("INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at) VALUES ('x', 'temporary', 'r1', '2026-01-01T00:00:00Z')").run();
  try {
    db.prepare("INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at) VALUES ('x', 'permanent', 'r2', '2026-01-01T00:01:00Z')").run();
    fail('migration: partial UNIQUE INDEX (active 2 行)', 'accepted');
  } catch (e) { expect('migration: partial UNIQUE INDEX rejects 2 active', /UNIQUE/.test(e.message), e.message); }
  // restore してから再除外は OK
  db.prepare("UPDATE migration_excluded SET restored_at = '2026-01-02T00:00:00Z' WHERE item_code = 'x'").run();
  db.prepare("INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at) VALUES ('x', 'permanent', 'r2', '2026-01-02T00:01:00Z')").run();
  ok('migration: after restore, re-exclude allowed (history accumulates)');
}

function testExclude() {
  const db = freshDb();

  // (1) 新規除外
  const r1 = excludeMigrationItem({ db, itemCode: 'item-1', excludeKind: 'temporary', reason: '価格が合わない' });
  expect('exclude (1): new exclusion creates active row', r1.id && r1.exclude_kind === 'temporary', JSON.stringify(r1));
  const active1 = db.prepare("SELECT * FROM migration_excluded WHERE item_code = 'item-1' AND restored_at IS NULL").get();
  expect('exclude (1): active row has reason + excluded_at', active1.reason === '価格が合わない' && active1.excluded_at, JSON.stringify(active1));

  // (2) validation
  for (const [label, args] of [
    ['invalid kind', { db, itemCode: 'i', excludeKind: 'bogus', reason: 'r' }],
    ['empty itemCode', { db, itemCode: '', excludeKind: 'temporary', reason: 'r' }],
    ['null reason', { db, itemCode: 'i', excludeKind: 'temporary', reason: null }],
    ['whitespace reason', { db, itemCode: 'i', excludeKind: 'temporary', reason: '   ' }],
  ]) {
    try { excludeMigrationItem(args); fail(`exclude (2): ${label} should throw`, 'no throw'); }
    catch (e) { expect(`exclude (2): ${label} rejected`, /exclusion:/.test(e.message), e.message); }
  }

  // (3) 既 active で 2 度目除外
  try {
    excludeMigrationItem({ db, itemCode: 'item-1', excludeKind: 'permanent', reason: 'もう一度' });
    fail('exclude (3): 2nd exclude on active', 'no throw');
  } catch (e) { expect('exclude (3): 2nd exclude throws EXCLUSION_ALREADY_ACTIVE', e.code === 'EXCLUSION_ALREADY_ACTIVE', e.message); }

  // (4) 別商品の同 kind は OK
  excludeMigrationItem({ db, itemCode: 'item-2', excludeKind: 'temporary', reason: '別商品' });
  ok('exclude (4): different item with same kind OK');
}

function testRestore() {
  const db = freshDb();
  excludeMigrationItem({ db, itemCode: 'r-1', excludeKind: 'temporary', reason: 'r1' });

  // (5) 復元 → active row が消える + 履歴は残る
  const restore1 = restoreMigrationExclusion({ db, itemCode: 'r-1', reason: 'やはり OK だった' });
  expect('restore (5): restore returns restored_id', !!restore1.restored_id, JSON.stringify(restore1));
  const activeAfter = db.prepare("SELECT * FROM migration_excluded WHERE item_code='r-1' AND restored_at IS NULL").get();
  expect('restore (5): no active row remains', !activeAfter, JSON.stringify(activeAfter));
  const history = db.prepare("SELECT * FROM migration_excluded WHERE item_code='r-1'").all();
  expect('restore (5): history retained', history.length === 1 && history[0].restored_at && history[0].restored_reason === 'やはり OK だった', JSON.stringify(history));

  // (6) 同じ商品を再除外 OK、 履歴 2 件
  excludeMigrationItem({ db, itemCode: 'r-1', excludeKind: 'permanent', reason: 'やっぱりやめる' });
  const history2 = db.prepare("SELECT exclude_kind, restored_at FROM migration_excluded WHERE item_code='r-1' ORDER BY id ASC").all();
  expect('restore (6): history 2 rows after re-exclude', history2.length === 2 && history2[0].restored_at !== null && history2[1].restored_at === null, JSON.stringify(history2));

  // (7) active なし状態で復元 → throw
  try {
    restoreMigrationExclusion({ db, itemCode: 'never-excluded', reason: 'noop' });
    fail('restore (7): restore without active', 'no throw');
  } catch (e) { expect('restore (7): restore w/o active throws', e.code === 'EXCLUSION_NOT_ACTIVE', e.message); }
}

function testChangeKind() {
  const db = freshDb();
  excludeMigrationItem({ db, itemCode: 'c-1', excludeKind: 'temporary', reason: '保留' });

  // (8) temporary → permanent
  const change = changeExclusionKind({ db, itemCode: 'c-1', newKind: 'permanent', reason: 'もう絶対やらない' });
  expect('changeKind (8): returns restored_id + new_id', change.restored_id && change.new_id && change.restored_id !== change.new_id, JSON.stringify(change));
  const rows = db.prepare("SELECT exclude_kind, restored_at FROM migration_excluded WHERE item_code='c-1' ORDER BY id ASC").all();
  expect('changeKind (8): 2 rows = old restored + new active', rows.length === 2 && rows[0].exclude_kind === 'temporary' && rows[0].restored_at !== null && rows[1].exclude_kind === 'permanent' && rows[1].restored_at === null, JSON.stringify(rows));

  // (9) 同 kind は throw
  try {
    changeExclusionKind({ db, itemCode: 'c-1', newKind: 'permanent', reason: 'no-op' });
    fail('changeKind (9): same kind', 'no throw');
  } catch (e) { expect('changeKind (9): same kind throws EXCLUSION_KIND_UNCHANGED', e.code === 'EXCLUSION_KIND_UNCHANGED', e.message); }

  // (10) active なし状態で変更 → throw
  try {
    changeExclusionKind({ db, itemCode: 'never-excluded', newKind: 'temporary', reason: 'noop' });
    fail('changeKind (10): no active', 'no throw');
  } catch (e) { expect('changeKind (10): no active throws', e.code === 'EXCLUSION_NOT_ACTIVE', e.message); }
}

function testList() {
  const db = freshDb();
  excludeMigrationItem({ db, itemCode: 'list-1', excludeKind: 'temporary', reason: '値段検討中' });
  excludeMigrationItem({ db, itemCode: 'list-2', excludeKind: 'permanent', reason: '廃盤' });
  excludeMigrationItem({ db, itemCode: 'special-3', excludeKind: 'temporary', reason: 'カテゴリー保留' });
  restoreMigrationExclusion({ db, itemCode: 'list-1', reason: '解決' });

  // (11) listActiveExclusions
  const all = listActiveExclusions(db);
  expect('list (11a): 2 active rows (list-1 restored)', all.length === 2 && all.every((r) => r.restored_at === undefined || r.restored_at === null), JSON.stringify(all.map(r => r.item_code)));
  const tempOnly = listActiveExclusions(db, { kind: 'temporary' });
  expect('list (11b): kind=temporary filter', tempOnly.length === 1 && tempOnly[0].item_code === 'special-3', JSON.stringify(tempOnly));
  const permOnly = listActiveExclusions(db, { kind: 'permanent' });
  expect('list (11c): kind=permanent filter', permOnly.length === 1 && permOnly[0].item_code === 'list-2', JSON.stringify(permOnly));
  const searchSpec = listActiveExclusions(db, { search: 'special' });
  expect('list (11d): search by item_code', searchSpec.length === 1, JSON.stringify(searchSpec));
  const searchReason = listActiveExclusions(db, { search: '廃盤' });
  expect('list (11e): search by reason', searchReason.length === 1 && searchReason[0].item_code === 'list-2', JSON.stringify(searchReason));

  // (12) listExclusionHistory
  const history = listExclusionHistory(db, 'list-1');
  expect('list (12): item history (restored row visible)', history.length === 1 && history[0].restored_at !== null, JSON.stringify(history));
}

function testCount() {
  const db = freshDb();
  excludeMigrationItem({ db, itemCode: 'c-a', excludeKind: 'temporary', reason: 'r' });
  excludeMigrationItem({ db, itemCode: 'c-b', excludeKind: 'temporary', reason: 'r' });
  excludeMigrationItem({ db, itemCode: 'c-c', excludeKind: 'permanent', reason: 'r' });
  const cnt = countActiveByKind(db);
  expect('count (13): countActiveByKind', cnt.temporary === 2 && cnt.permanent === 1 && cnt.total === 3, JSON.stringify(cnt));
}

function testLabel() {
  expect('label (14): temporary → 一時除外', kindLabel('temporary') === '一時除外', kindLabel('temporary'));
  expect('label (14): permanent → 永続除外', kindLabel('permanent') === '永続除外', kindLabel('permanent'));
  expect('label (14): unknown passthrough', kindLabel('bogus') === 'bogus', kindLabel('bogus'));
}

// ──── runner ────
(() => {
  console.log('=== Phase E-7-c smoke runner ===');
  console.log('--- migration ---');
  testMigration();
  console.log('--- excludeMigrationItem ---');
  testExclude();
  console.log('--- restoreMigrationExclusion ---');
  testRestore();
  console.log('--- changeExclusionKind ---');
  testChangeKind();
  console.log('--- listActiveExclusions / listExclusionHistory ---');
  testList();
  console.log('--- countActiveByKind ---');
  testCount();
  console.log('--- kindLabel ---');
  testLabel();
  console.log('================================');
  if (failed > 0) {
    console.error(`FAILED: ${failed} check(s) did not pass`);
    process.exit(1);
  } else {
    console.log('ALL CHECKS PASSED');
  }
})();
