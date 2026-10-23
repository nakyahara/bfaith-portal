/**
 * 出品の直前の門 (2026-10-22): 除外中・Yahoo にある/消えた商品・確定値の版の食い違いは Yahoo へ送る前に止める。
 * 実行: node --test apps/rakuten-yahoo-sync/services/executor-guards.test.mjs
 *   (どれも楽天・Yahoo を呼ぶ前に返る経路だけを見る)
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';
import { runMigrations } from '../db.js';
import { executePublish, publishTargetGuard } from './publish-executor.js';
import { confirmValues } from './confirmed-values.js';

function freshDb() {
  const db = new Db(':memory:');
  runMigrations(db);
  return db;
}
const vals = { yahoo_title: 't', yahoo_price: 1000, delivery_label: 'ネコポス', tax_rate: '10' };

test('除外中 / 手で Yahoo に出した / Yahoo で消えた商品は出さない。このアプリで出した商品の出し直しは通す', () => {
  const db = freshDb();
  db.prepare(`INSERT INTO migration_excluded (item_code, exclude_kind, reason, excluded_by, excluded_at) VALUES ('ex', 'permanent', 'r', 't', '2026-10-01')`).run();
  assert.equal(publishTargetGuard(db, 'ex'), 'migration_excluded');
  db.prepare(`INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, last_seen_at, source) VALUES ('man', 'man', '2026-10-01', 'yahoo_existing')`).run();
  assert.equal(publishTargetGuard(db, 'man'), 'already_on_yahoo');
  db.prepare(`INSERT INTO yahoo_registered_items (item_code, yahoo_item_code, last_seen_at, source) VALUES ('mine', 'mine', '2026-10-01', 'yahoo_existing')`).run();
  db.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, created_by) VALUES ('k1', 'mine', 'mine', 'success', 't')`).run();
  assert.equal(publishTargetGuard(db, 'mine'), null);
  db.prepare(`INSERT INTO yahoo_removed_items (item_code, yahoo_item_code, last_seen_at) VALUES ('gone', 'gone', '2026-10-01')`).run();
  assert.equal(publishTargetGuard(db, 'gone'), 'removed_from_yahoo');
  assert.equal(publishTargetGuard(db, 'fresh'), null);
});

test('確認画面で見た確定値の版と違えば出さない (別の人が確定し直した)', async () => {
  const db = freshDb();
  const prev = process.env.RYS_PUBLISH_ENABLED;
  process.env.RYS_PUBLISH_ENABLED = '1';
  try {
    confirmValues(db, { manageNumber: 'v1', input: vals, source: 'manual', actor: 'a' });
    confirmValues(db, { manageNumber: 'v1', input: { ...vals, yahoo_price: 2000 }, source: 'manual', actor: 'b', expectedVersion: 1 });
    const r = await executePublish({ db, itemCode: 'v1', manageNumber: 'v1', expectedValueToken: 'manual:1' });
    assert.equal(r.status, 'readiness_blocked');
    assert.match(r.reasons[0], /^value_version_changed:manual:1->manual:2/);
    // 何も書いていない (idempotency の行を作らない = 失敗中の再実行に紛れない)
    assert.equal(db.prepare('SELECT COUNT(*) c FROM publish_idempotency').get().c, 0);
  } finally {
    if (prev === undefined) delete process.env.RYS_PUBLISH_ENABLED; else process.env.RYS_PUBLISH_ENABLED = prev;
  }
});

test('入口の照合のあと、楽天の取得を待つ間に確定し直されても、組み立てた版で照合して止める (Codex R2)', async () => {
  const db = freshDb();
  const prev = process.env.RYS_PUBLISH_ENABLED;
  process.env.RYS_PUBLISH_ENABLED = '1';
  try {
    confirmValues(db, { manageNumber: 'v2', input: vals, source: 'manual', actor: 'a' });
    const r = await executePublish({
      db, itemCode: 'v2', manageNumber: 'v2', expectedValueToken: 'manual:1',
      publishOpts: {
        productCategory: 1, pathName: 'p',
        deps: {
          // 楽天を読んでいる間に別の人が 2,000 円で確定し直す
          fetchItemDetail: async () => {
            confirmValues(db, { manageNumber: 'v2', input: { ...vals, yahoo_price: 2000 }, source: 'manual', actor: 'b', expectedVersion: 1 });
            return { item: { itemNumber: 'v2', manageNumber: 'v2', itemName: 'n', variants: { a: { standardPrice: 900 } }, payment: { taxRate: 0.1 } } };
          },
          runLeadTimePreflight: async () => ({ ok: true }),
          imagePreflight: () => ({ ok: true }),
        },
      },
    });
    assert.equal(r.status, 'readiness_blocked', JSON.stringify(r));
    assert.match(r.reasons[0], /^value_version_changed:manual:1->manual:2/);
    assert.equal(db.prepare(`SELECT status FROM publish_idempotency WHERE item_code = 'v2'`).get().status, 'failed');
  } finally {
    if (prev === undefined) delete process.env.RYS_PUBLISH_ENABLED; else process.env.RYS_PUBLISH_ENABLED = prev;
  }
});
