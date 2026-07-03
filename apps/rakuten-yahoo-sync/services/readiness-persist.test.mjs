/**
 * R8 Codex Critical/High の再発防止テスト:
 *   - persistJobReadiness は jobs 行が無くても upsert で作る (旧実装は UPDATE のみで no-op)
 *   - evaluateItemForPublish(dryRun=false) の早期 blocked (楽天取得失敗) も jobs に persist される
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/readiness-persist.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { persistJobReadiness } from './readiness-check.js';
import { evaluateItemForPublish } from './publish-pipeline.js';

function setupDb() {
  const db = new Db(':memory:');
  // migration 001 の jobs 定義と同じ CHECK 制約 (upsert が制約を満たすことも検証する)
  db.exec(`
    CREATE TABLE jobs (
      item_code                TEXT PRIMARY KEY,
      batch_id                 INTEGER,
      current_state            TEXT NOT NULL CHECK(current_state IN (
                                 'pending', 'item_staged', 'stock_set',
                                 'publish_reserved', 'published_verified',
                                 'verification_pending', 'failed'
                               )),
      attempt                  INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
      last_error               TEXT,
      payload_json             TEXT,
      readiness_status         TEXT NOT NULL DEFAULT 'pending'
                                 CHECK(readiness_status IN ('pending', 'ok', 'blocked')),
      readiness_blocked_reasons TEXT CHECK(
        (readiness_status IN ('pending', 'ok') AND readiness_blocked_reasons IS NULL)
        OR
        (readiness_status = 'blocked'
          AND readiness_blocked_reasons IS NOT NULL
          AND json_valid(readiness_blocked_reasons)
          AND json_type(readiness_blocked_reasons) = 'array'
          AND json_array_length(readiness_blocked_reasons) > 0)
      ),
      last_readiness_at        TEXT,
      created_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
  `);
  return db;
}

test('persistJobReadiness: jobs 行が無ければ upsert で作る (Codex R8 Critical)', () => {
  const db = setupDb();
  persistJobReadiness(db, 'newitem', { status: 'blocked', reasons: ['tax_rate_mismatch:notion=8%,rakuten=10%'] });
  const row = db.prepare('SELECT * FROM jobs WHERE item_code = ?').get('newitem');
  assert.ok(row, 'jobs 行が作られる');
  assert.equal(row.current_state, 'pending');
  assert.equal(row.readiness_status, 'blocked');
  assert.deepEqual(JSON.parse(row.readiness_blocked_reasons), ['tax_rate_mismatch:notion=8%,rakuten=10%']);
});

test('persistJobReadiness: 既存行は readiness だけ更新 (他列は保持)', () => {
  const db = setupDb();
  db.prepare(`INSERT INTO jobs (item_code, current_state, attempt, payload_json) VALUES ('x', 'item_staged', 3, '{"a":1}')`).run();
  persistJobReadiness(db, 'x', { status: 'ok' });
  const row = db.prepare('SELECT * FROM jobs WHERE item_code = ?').get('x');
  assert.equal(row.current_state, 'item_staged'); // 上書きされない
  assert.equal(row.attempt, 3);
  assert.equal(row.payload_json, '{"a":1}');
  assert.equal(row.readiness_status, 'ok');
  assert.equal(row.readiness_blocked_reasons, null);
});

test('persistJobReadiness: blocked→ok で reasons がクリアされる (CHECK 制約整合)', () => {
  const db = setupDb();
  persistJobReadiness(db, 'y', { status: 'blocked', reasons: ['rakuten_item_not_found'] });
  persistJobReadiness(db, 'y', { status: 'ok' });
  const row = db.prepare('SELECT readiness_status, readiness_blocked_reasons FROM jobs WHERE item_code = ?').get('y');
  assert.equal(row.readiness_status, 'ok');
  assert.equal(row.readiness_blocked_reasons, null);
});

test('evaluateItemForPublish: 楽天取得失敗の早期 blocked も dryRun=false なら persist (Codex R8 High)', async () => {
  const db = setupDb();
  const r = await evaluateItemForPublish({
    db, itemCode: 'failitem', manageNumber: 'failitem', dryRun: false,
    deps: { fetchItemDetail: async () => { throw new Error('proxy 502'); } },
  });
  assert.equal(r.status, 'blocked');
  const row = db.prepare('SELECT readiness_status, readiness_blocked_reasons FROM jobs WHERE item_code = ?').get('failitem');
  assert.ok(row, 'jobs 行が作られる');
  assert.equal(row.readiness_status, 'blocked');
  assert.match(row.readiness_blocked_reasons, /rakuten_fetch_failed/);
});

test('evaluateItemForPublish: dryRun=true の早期 blocked は persist しない (従来互換)', async () => {
  const db = setupDb();
  await evaluateItemForPublish({
    db, itemCode: 'dryitem', manageNumber: 'dryitem', dryRun: true,
    deps: { fetchItemDetail: async () => { throw new Error('proxy 502'); } },
  });
  assert.equal(db.prepare('SELECT COUNT(*) c FROM jobs').get().c, 0);
});
