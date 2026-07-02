/**
 * refresh-pipeline (再設計 R4) の test。 外部 I/O は deps 注入で stub。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/refresh-pipeline.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { runRefreshPipeline, getRefreshRun, findActiveRefreshRun } from './refresh-pipeline.js';

function setupDb() {
  const db = new Db(':memory:');
  db.exec(`
    CREATE TABLE refresh_runs (
      run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
      started_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      finished_at       TEXT,
      status            TEXT NOT NULL DEFAULT 'running' CHECK(status IN ('running', 'success', 'failed')),
      triggered_by      TEXT NOT NULL DEFAULT 'manual',
      current_step      TEXT,
      steps_json        TEXT CHECK(steps_json IS NULL OR json_valid(steps_json)),
      error_message     TEXT,
      lease_expires_at  TEXT
    );
  `);
  return db;
}

function okDeps(overrides = {}) {
  return {
    runRysFullSync: async () => ({ diff: { rakutenTotal: 300, overlap: 130, newlyDetected: 2, resolved: 1 }, titleBackfill: { updated: 3 } }),
    countMissingRakutenGenre: (() => {
      let calls = 0;
      return () => (calls++ === 0 ? 5 : 0); // 1round 目 5 件 → 2round 目 0
    })(),
    backfillRakutenGenre: async () => ({ updated: 5 }),
    createNotionPagesFromRakuten: (() => {
      let calls = 0;
      return async () => ({
        totalScanned: 10,
        results: calls++ === 0 ? [{ outcome: 'created' }, { outcome: 'skipped_exists' }] : [{ outcome: 'skipped_exists' }],
      });
    })(),
    seedNotionDrafts: (() => {
      let calls = 0;
      return async () => ({ applied: calls++ === 0 ? 4 : 0, errors: 0 });
    })(),
    syncNotionOverrides: async () => ({ inserted: 1, updated: 2, skipped: 0, deleted: 0, errors: [] }),
    ...overrides,
  };
}

test('happy path: 5 ステップ全部走って success + steps 記録', async () => {
  const db = setupDb();
  const r = await runRefreshPipeline({ db, triggeredBy: 'manual', deps: okDeps() });
  assert.equal(r.status, 'success');
  const run = getRefreshRun(db, r.runId);
  assert.equal(run.status, 'success');
  assert.equal(run.triggered_by, 'manual');
  assert.ok(run.finished_at);
  assert.equal(run.steps.full_sync.ok, true);
  assert.equal(run.steps.full_sync.candidatesNew, 2);
  assert.equal(run.steps.genre_backfill.updated, 5);
  assert.equal(run.steps.notion_pages.created, 1); // 1round 目 created 1 → 2round 目 0 で打ち切り
  assert.equal(run.steps.draft_seed.applied, 4);
  assert.equal(run.steps.notion_sync.inserted, 1);
});

test('途中失敗 (notion_pages) → failed + 以降ステップ未実行 + error 記録', async () => {
  const db = setupDb();
  let syncCalled = false;
  const deps = okDeps({
    createNotionPagesFromRakuten: async () => { throw new Error('notion 500'); },
    syncNotionOverrides: async () => { syncCalled = true; return {}; },
  });
  await assert.rejects(
    () => runRefreshPipeline({ db, deps }),
    (e) => e.failedStep === 'notion_pages' && /notion 500/.test(e.message),
  );
  assert.equal(syncCalled, false);
  const run = getRefreshRun(db);
  assert.equal(run.status, 'failed');
  assert.equal(run.current_step, 'notion_pages');
  assert.equal(run.steps.full_sync.ok, true);
  assert.equal(run.steps.notion_pages.ok, false);
  assert.match(run.error_message, /notion 500/);
});

test('running 中は 409 (二重起動拒否)', async () => {
  const db = setupDb();
  db.prepare(`INSERT INTO refresh_runs (status, current_step, lease_expires_at) VALUES ('running', 'full_sync', ?)`)
    .run(new Date(Date.now() + 60_000).toISOString());
  await assert.rejects(
    () => runRefreshPipeline({ db, deps: okDeps() }),
    (e) => e.statusCode === 409,
  );
});

test('lease 切れ running は steal して新 run 開始できる', async () => {
  const db = setupDb();
  db.prepare(`INSERT INTO refresh_runs (status, current_step, lease_expires_at) VALUES ('running', 'full_sync', ?)`)
    .run(new Date(Date.now() - 60_000).toISOString()); // 期限切れ
  const r = await runRefreshPipeline({ db, deps: okDeps() });
  assert.equal(r.status, 'success');
  const stale = db.prepare(`SELECT status, error_message FROM refresh_runs WHERE run_id = 1`).get();
  assert.equal(stale.status, 'failed');
  assert.match(stale.error_message, /stale lease/);
});

test('findActiveRefreshRun: migration 未適用 (table 無し) でも crash しない', () => {
  const db = new Db(':memory:');
  assert.equal(findActiveRefreshRun(db), null);
});

test('genre backfill: 進捗ゼロで打ち切り (無限ループしない)', async () => {
  const db = setupDb();
  let backfillCalls = 0;
  const deps = okDeps({
    countMissingRakutenGenre: () => 100, // 常に残あり
    backfillRakutenGenre: async () => { backfillCalls++; return { updated: 0 }; }, // 進捗なし
  });
  const r = await runRefreshPipeline({ db, deps });
  assert.equal(r.status, 'success');
  assert.equal(backfillCalls, 1); // updated=0 で即 break
  assert.equal(r.steps.genre_backfill.remaining, 100);
});
