/**
 * 一括登録が途中で止まった (再起動など) batch を片付ける — 2026-10-22 修正の再発防止。
 * 実行: node --test apps/rakuten-yahoo-sync/services/bulk-recover.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';
import { runMigrations } from '../db.js';
import { recoverStaleBatches, STALE_BATCH_MS, getRunningBatch } from './bulk-publish.js';

function freshDb() {
  const db = new Db(':memory:');
  runMigrations(db);
  return db;
}

function insertRunning(db, startedAt, items) {
  const id = Number(db.prepare(`INSERT INTO bulk_publish_batches (total, in_progress, status, triggered_by, started_at)
    VALUES (?, ?, 'running', 'manual', ?)`).run(items.length, items.length, startedAt).lastInsertRowid);
  for (const [code, status, at] of items) {
    db.prepare(`INSERT INTO bulk_publish_batch_items (batch_id, item_code, status, started_at, finished_at) VALUES (?, ?, ?, ?, ?)`)
      .run(id, code, status, at, status === 'success' ? at : null);
  }
  return id;
}

test('最後の動きから 35 分経った running の batch は aborted にして、残りの商品は failed にする', () => {
  const db = freshDb();
  const old = new Date(Date.now() - STALE_BATCH_MS - 60_000).toISOString();
  const id = insertRunning(db, old, [['a', 'success', old], ['b', 'running', old], ['c', 'pending', null]]);
  assert.equal(recoverStaleBatches(db), 1);
  const b = db.prepare('SELECT * FROM bulk_publish_batches WHERE batch_id = ?').get(id);
  assert.equal(b.status, 'aborted');
  assert.ok(b.finished_at);
  assert.equal(b.in_progress, 0);
  assert.equal(b.failed, 2);
  const items = db.prepare('SELECT item_code, status, error_message FROM bulk_publish_batch_items WHERE batch_id = ? ORDER BY id').all(id);
  assert.deepEqual(items.map((i) => i.status), ['success', 'failed', 'failed']);
  assert.match(items[1].error_message, /中断/);
  // もう running は無い = 新しい batch を作れる (UNIQUE INDEX に当たらない)
  assert.equal(getRunningBatch(db), null);
  insertRunning(db, new Date().toISOString(), [['d', 'pending', null]]);
});

test('最近まで動いていた batch (デプロイの切り替え中に古いプロセスが回しているかもしれない) は閉じない', () => {
  const db = freshDb();
  const old = new Date(Date.now() - STALE_BATCH_MS - 60_000).toISOString();
  const recent = new Date(Date.now() - 60_000).toISOString();
  insertRunning(db, old, [['a', 'success', old], ['b', 'running', recent]]);
  assert.equal(recoverStaleBatches(db), 0);
  assert.ok(getRunningBatch(db));
});

test('中断扱いにした商品は「失敗中を再実行」で拾う (成功済みは拾わない)', async () => {
  const { listFailedItemCodes } = await import('./bulk-publish.js');
  const db = freshDb();
  const old = new Date(Date.now() - STALE_BATCH_MS - 60_000).toISOString();
  insertRunning(db, old, [['p1', 'pending', null], ['r1', 'running', old], ['s1', 'success', old]]);
  db.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, created_by) VALUES ('k-s1', 's1', 's1', 'success', 't')`).run();
  recoverStaleBatches(db);
  assert.deepEqual(listFailedItemCodes(db).sort(), ['p1', 'r1']);
});

test('全件再登録の中断: 過去に成功した商品でも、中断のあとに成功していなければ拾う (Codex R2)', async () => {
  const { listFailedItemCodes } = await import('./bulk-publish.js');
  const db = freshDb();
  db.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, created_by, completed_at, created_at, updated_at)
              VALUES ('k-old', 'm1', 'm1', 'success', 't', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')`).run();
  const old = new Date(Date.now() - STALE_BATCH_MS - 60_000).toISOString();
  insertRunning(db, old, [['m1', 'pending', null]]);
  recoverStaleBatches(db);
  assert.deepEqual(listFailedItemCodes(db), ['m1']);
  // 中断のあとに成功すれば拾わない
  db.prepare(`INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, created_by, completed_at)
              VALUES ('k-new', 'm1', 'm1', 'success', 't', ?)`).run(new Date(Date.now() + 1000).toISOString());
  assert.deepEqual(listFailedItemCodes(db), []);
});
