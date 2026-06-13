/**
 * E-7-d-1 smoke runner: 本番 listProductsForUI を直接呼んで挙動を検証。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/test-list-products-ui.js
 *
 * 検証項目:
 *   (1) migration_candidates が起点 (notion_overrides 全件起点ではない)
 *   (2) listProductsForUI が array を返す + 件数 = migration_candidates 件数
 *   (3) summary に excluded / stale / orphan を含む
 *   (4) precedence: excluded > done > actionable > fixable > stale (Codex R2 D-2)
 *   (5) excluded フィールド (kind / reason)
 *   (6) filter=excluded
 *   (7) filter=orphan
 *   (8) search by item_code
 *   (9) publish_idempotency 集約: 一度 success なら done 維持
 *  (10) Codex R1 High fix: jobs.item_code = c.item_code で readiness_blocked_reasons を拾う
 */

import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { listProductsForUI } from '../router.js';

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

async function run() {
  const db = freshDb();
  const now = new Date().toISOString();

  // Setup: migration_candidates 6 件
  const insertCandidate = db.prepare(`
    INSERT INTO migration_candidates (item_code, rakuten_manage_number, first_detected_at, last_detected_at, last_seen_in_rakuten_at, last_checked_yahoo_baseline_at, status)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  for (const [code, mn, status] of [
    ['c-1', 'N-c1', 'candidate'],
    ['c-2', 'N-c2', 'candidate'],
    ['c-3', 'N-c3', 'candidate'],
    ['c-4', 'N-c4', 'resolved'],
    ['c-5', 'N-c5', 'stale'],
    ['c-6', 'N-c6', 'candidate'],
  ]) insertCandidate.run(code, mn, now, now, now, now, status);

  // notion_overrides (PK = rakuten_manage_number)
  const insertOverride = db.prepare(`
    INSERT INTO notion_overrides (rakuten_manage_number, notion_page_id, yahoo_title, yahoo_price, yahoo_price_sagawa, notion_delivery_label, notion_tax_rate, notion_status, synced_at, raw_properties_json, source_hash)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', 'h')
  `);
  insertOverride.run('N-c1', 'page1', 'title-1', 1000, 1200, 'ネコポス', '10', 'OK', now);              // full → actionable
  insertOverride.run('N-c2', 'page2', 'title-2', null, 1200, 'ネコポス', '10', 'OK', now);              // missing price → fixable
  insertOverride.run('N-c3', 'page3', 'title-3', 1500, 1700, 'ヤマト宅急便', '10', 'OK', now);          // full but excluded
  insertOverride.run('N-c4', 'page4', 'title-4', 2000, 2200, 'ネコポス', '10', 'OK', now);              // done (yahoo observed)
  insertOverride.run('N-c5', 'page5', 'title-5', 800, 1000, 'ネコポス', '8', 'OK', now);                // stale
  // c-6 NO override row → 4 項目欠け → fixable
  insertOverride.run('N-orphan', 'page-or', 'orphan-title', 3000, 3300, 'ネコポス', '10', 'OK', now);   // orphan

  // yahoo_registered_items (c-4 のみ Yahoo 観測)
  db.prepare(`
    INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, has_sub_code, last_seen_at, source)
    VALUES (?, ?, 0, ?, 'yahoo_existing')
  `).run('c-4', 'c-4', now);

  // migration_excluded (c-3 active 一時除外)
  db.prepare(`
    INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_at)
    VALUES (?, ?, ?, ?)
  `).run('c-3', 'temporary', '価格交渉中', now);

  // publish_idempotency (c-4 は 1 度 success)
  db.prepare(`
    INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at)
    VALUES (?, ?, ?, 'success', 1, 'test', ?)
  `).run('key-c4', 'c-4', 'N-c4', now);

  // (1) migration_candidates 起点
  const candidateRowsCount = db.prepare("SELECT COUNT(*) AS c FROM migration_candidates").get().c;
  expect('list (1): migration_candidates table has 6 candidates', candidateRowsCount === 6, `got ${candidateRowsCount}`);

  // (2) 本番 listProductsForUI 直接呼び
  const all = listProductsForUI(db, { filter: 'all' });
  expect('list (2a): listProductsForUI returns products array', Array.isArray(all.products), typeof all.products);
  expect('list (2b): products count = 6 (1 per migration_candidate)', all.products.length === 6, `got ${all.products.length}`);

  // (3) summary 構造
  expect('list (3a): summary.total = 6', all.summary.total === 6, JSON.stringify(all.summary));
  expect('list (3b): summary.excluded = 1', all.summary.excluded === 1, JSON.stringify(all.summary));
  expect('list (3c): summary.done = 1 (c-4)', all.summary.done === 1, JSON.stringify(all.summary));
  expect('list (3d): summary.stale = 1 (c-5)', all.summary.stale === 1, JSON.stringify(all.summary));
  expect('list (3e): summary.orphan = 1 (N-orphan)', all.summary.orphan === 1, JSON.stringify(all.summary));

  // (4) precedence (Codex R2 D-2: excluded > done > actionable > fixable > stale)
  const c1 = all.products.find((p) => p.itemCode === 'c-1');
  expect('list (4a): c-1 → actionable (Notion 4 項目揃)', c1.status === 'actionable', `c-1 status=${c1.status}`);
  const c2 = all.products.find((p) => p.itemCode === 'c-2');
  expect('list (4b): c-2 → fixable (price missing)', c2.status === 'fixable', `c-2 status=${c2.status}`);
  const c3 = all.products.find((p) => p.itemCode === 'c-3');
  expect('list (4c): c-3 → excluded (precedence over actionable)', c3.status === 'excluded' && c3.isExcluded === true, JSON.stringify({ status: c3.status, isExcluded: c3.isExcluded }));
  const c4 = all.products.find((p) => p.itemCode === 'c-4');
  expect('list (4d): c-4 → done (yahoo observed + publish success)', c4.status === 'done' && c4.isDone === true, JSON.stringify({ status: c4.status, isDone: c4.isDone }));
  const c5 = all.products.find((p) => p.itemCode === 'c-5');
  expect('list (4e): c-5 → stale', c5.status === 'stale', `c-5 status=${c5.status}`);
  const c6 = all.products.find((p) => p.itemCode === 'c-6');
  expect('list (4f): c-6 → fixable (no Notion 4 items)', c6.status === 'fixable', `c-6 status=${c6.status}`);

  // (5) excluded fields
  expect('list (5a): c-3 exclusionKind = temporary', c3.exclusionKind === 'temporary', `kind=${c3.exclusionKind}`);
  expect('list (5b): c-3 exclusionReason = 価格交渉中', c3.exclusionReason === '価格交渉中', `reason=${c3.exclusionReason}`);

  // (6) filter=excluded → c-3 のみ
  const excluded = listProductsForUI(db, { filter: 'excluded' });
  expect('list (6): filter=excluded returns 1 product', excluded.products.length === 1 && excluded.products[0].itemCode === 'c-3', JSON.stringify(excluded.products.map((p) => p.itemCode)));

  // (7) filter=orphan → N-orphan 1 件
  const orphanFiltered = listProductsForUI(db, { filter: 'orphan' });
  expect('list (7): filter=orphan returns 1 product (N-orphan)', orphanFiltered.products.length === 1 && orphanFiltered.products[0].itemCode === 'N-orphan' && orphanFiltered.products[0].status === 'orphan', JSON.stringify(orphanFiltered.products));

  // (8) search by item_code
  const search1 = listProductsForUI(db, { filter: 'all', search: 'c-1' });
  expect('list (8): search by item_code', search1.products.length === 1 && search1.products[0].itemCode === 'c-1', JSON.stringify(search1.products.map((p) => p.itemCode)));

  // (9) publish_idempotency 集約: 同 item_code に追加 failed row → success 維持
  const now2 = new Date(Date.now() + 1000).toISOString();
  db.prepare(`
    INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, attempt, created_by, completed_at, error_message)
    VALUES (?, ?, ?, 'failed', 2, 'test', ?, 'retry failed')
  `).run('key-c4-fail', 'c-4', 'N-c4', now2);
  const all2 = listProductsForUI(db, { filter: 'all' });
  const c4After = all2.products.find((p) => p.itemCode === 'c-4');
  expect('list (9): c-4 stays done after later failed publish (success aggregation)', c4After.status === 'done' && c4After.publishStatus === 'success', JSON.stringify({ status: c4After.status, publishStatus: c4After.publishStatus }));

  // (10) Codex R1 High fix: jobs.item_code = c.item_code 結合確認
  //   c-1 (actionable) に readiness_blocked_reasons を入れて fixable に降格するか確認
  db.prepare(`
    INSERT INTO jobs (item_code, current_state, readiness_status, readiness_blocked_reasons, last_readiness_at)
    VALUES ('c-1', 'pending', 'blocked', '["product_category_unresolved"]', ?)
  `).run(new Date().toISOString());
  const all3 = listProductsForUI(db, { filter: 'all' });
  const c1After = all3.products.find((p) => p.itemCode === 'c-1');
  expect('list (10): jobs JOIN via c.item_code reads readiness_blocked_reasons (R1 High fix)', c1After.status === 'fixable' && c1After.reasons.length >= 1, JSON.stringify({ status: c1After.status, reasons: c1After.reasons?.length }));
}

(async () => {
  console.log('=== Phase E-7-d-1 smoke runner ===');
  await run();
  console.log('==================================');
  if (failed > 0) {
    console.error(`FAILED: ${failed} check(s) did not pass`);
    process.exit(1);
  } else {
    console.log('ALL CHECKS PASSED');
  }
})().catch((e) => {
  console.error('unexpected runner error:', e);
  process.exit(2);
});
