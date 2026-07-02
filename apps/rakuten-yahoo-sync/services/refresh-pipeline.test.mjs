/**
 * refresh-pipeline (再設計 R4、 Codex R4-R1 反映版) の test。 外部 I/O は deps 注入で stub。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/refresh-pipeline.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { startRefreshRun, executeRefreshPipeline, getRefreshRun, findActiveRefreshRun } from './refresh-pipeline.js';

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
      run_token         TEXT NOT NULL,
      lease_expires_at  TEXT
    );
    CREATE UNIQUE INDEX idx_refresh_runs_single_running ON refresh_runs(status) WHERE status = 'running';
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
        results: calls++ === 0 ? [{ outcome: 'created' }, { outcome: 'already_exists_in_notion' }] : [{ outcome: 'already_exists_in_notion' }],
      });
    })(),
    seedNotionDrafts: (() => {
      let calls = 0;
      return async () => ({ applied: calls++ === 0 ? 4 : 0, errors: 0 });
    })(),
    syncNotionOverrides: async () => ({ inserted: 1, updated: 2, skipped: 0, deleted: 0, errors: [] }),
    acquireNotionSyncLock: () => () => {}, // lock 取得成功 (release は no-op)
    ...overrides,
  };
}

async function runPipeline(db, deps, triggeredBy = 'manual') {
  const { runId, runToken } = startRefreshRun(db, { triggeredBy });
  return executeRefreshPipeline({ db, runId, runToken, triggeredBy, deps });
}

test('happy path: 5 ステップ全部走って success + steps 記録', async () => {
  const db = setupDb();
  const r = await runPipeline(db, okDeps());
  assert.equal(r.status, 'success');
  const run = getRefreshRun(db, r.runId);
  assert.equal(run.status, 'success');
  assert.equal(run.triggered_by, 'manual');
  assert.ok(run.finished_at);
  assert.equal(run.steps.full_sync.ok, true);
  assert.equal(run.steps.full_sync.candidatesNew, 2);
  assert.equal(run.steps.genre_backfill.updated, 5);
  assert.equal(run.steps.notion_pages.created, 1);
  assert.equal(run.steps.draft_seed.applied, 4);
  assert.equal(run.steps.notion_sync.inserted, 1);
});

test('二重起動: running 中の startRefreshRun は 409 (DB unique 制約)', async () => {
  const db = setupDb();
  startRefreshRun(db); // running 行を作る (lease 有効)
  assert.throws(() => startRefreshRun(db), (e) => e.statusCode === 409);
});

test('lease 切れ running は steal して新 run 開始できる + 旧 run は failed 化', async () => {
  const db = setupDb();
  const first = startRefreshRun(db);
  db.prepare('UPDATE refresh_runs SET lease_expires_at = ? WHERE run_id = ?')
    .run(new Date(Date.now() - 60_000).toISOString(), first.runId); // 期限切れに
  const r = await runPipeline(db, okDeps());
  assert.equal(r.status, 'success');
  const stale = getRefreshRun(db, first.runId);
  assert.equal(stale.status, 'failed');
  assert.match(stale.error_message, /stale lease/);
});

test('steal された旧 run の finalize は新 run を上書きしない (owner CAS)', async () => {
  const db = setupDb();
  const first = startRefreshRun(db);
  // 旧 run のステップ実行中に steal された状況を再現: run_token を別物に書き換え
  const deps = okDeps({
    runRysFullSync: async () => {
      db.prepare("UPDATE refresh_runs SET run_token = 'stolen' WHERE run_id = ?").run(first.runId);
      return { diff: {}, titleBackfill: {} };
    },
  });
  await assert.rejects(
    () => executeRefreshPipeline({ db, runId: first.runId, runToken: first.runToken, deps }),
    (e) => e.code === 'LEASE_LOST',
  );
  // DB は steal 側の所有のまま (旧 run が failed/success を書いていない)
  const row = db.prepare('SELECT status, run_token FROM refresh_runs WHERE run_id = ?').get(first.runId);
  assert.equal(row.status, 'running');
  assert.equal(row.run_token, 'stolen');
});

test('notion_pages: service が {error} を返したら fail-closed (Codex R4-R1)', async () => {
  const db = setupDb();
  let syncCalled = false;
  const deps = okDeps({
    createNotionPagesFromRakuten: async () => ({ error: 'rakuten_rms: 502', results: [] }),
    syncNotionOverrides: async () => { syncCalled = true; return { errors: [] }; },
  });
  await assert.rejects(() => runPipeline(db, deps), (e) => e.failedStep === 'notion_pages' && /rakuten_rms/.test(e.message));
  assert.equal(syncCalled, false);
  const run = getRefreshRun(db);
  assert.equal(run.status, 'failed');
  assert.equal(run.steps.notion_pages.ok, false);
});

test('draft_seed: PATCH エラー > 0 は fail-closed (Codex R4-R1)', async () => {
  const db = setupDb();
  const deps = okDeps({
    seedNotionDrafts: async () => ({ applied: 2, errors: 3 }),
  });
  await assert.rejects(() => runPipeline(db, deps), (e) => e.failedStep === 'draft_seed' && /3 件/.test(e.message));
});

test('notion_sync: row error > 0 は fail-closed (Codex R4-R1)', async () => {
  const db = setupDb();
  const deps = okDeps({
    syncNotionOverrides: async () => ({ inserted: 1, updated: 0, skipped: 0, deleted: 0, errors: [{ pageId: 'x', error: 'bad' }] }),
  });
  await assert.rejects(() => runPipeline(db, deps), (e) => e.failedStep === 'notion_sync' && /1 行/.test(e.message));
});

test('notion_sync: sync-lock が取れなければ fail (手動 sync との並列防止) + lock は release される', async () => {
  const db = setupDb();
  const { SyncLockError } = await import('../lib/sync-lock.js');
  const busy = okDeps({
    acquireNotionSyncLock: () => { throw new SyncLockError('locked', { reason: 'alive' }); },
  });
  await assert.rejects(() => runPipeline(db, busy), (e) => e.failedStep === 'notion_sync' && /実行中のため中断/.test(e.message));
  // 成功時に release が呼ばれる
  let released = false;
  const db2 = setupDb();
  const deps2 = okDeps({ acquireNotionSyncLock: () => () => { released = true; } });
  await runPipeline(db2, deps2);
  assert.equal(released, true);
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
  const r = await runPipeline(db, deps);
  assert.equal(r.status, 'success');
  assert.equal(backfillCalls, 1); // updated=0 で即 break
  assert.equal(r.steps.genre_backfill.remaining, 100);
});

test('triggeredBy が full_sync まで伝播する (Codex R4-R1 Low)', async () => {
  const db = setupDb();
  let seen = null;
  const deps = okDeps({
    runRysFullSync: async ({ triggeredBy }) => { seen = triggeredBy; return { diff: {}, titleBackfill: {} }; },
  });
  await runPipeline(db, deps, 'cron');
  assert.equal(seen, 'cron');
  assert.equal(getRefreshRun(db).triggered_by, 'cron');
});

test('notion_pages: 行単位の失敗 outcome (create_failed 等) も fail-closed (Codex R4-R2)', async () => {
  const db = setupDb();
  const deps = okDeps({
    createNotionPagesFromRakuten: async () => ({
      totalScanned: 3,
      results: [{ outcome: 'created' }, { outcome: 'create_failed', error: 'notion 500' }, { outcome: 'rakuten_fetch_failed' }],
    }),
  });
  await assert.rejects(
    () => runPipeline(db, deps),
    (e) => e.failedStep === 'notion_pages' && /create_failed=1/.test(e.message) && /rakuten_fetch_failed=1/.test(e.message),
  );
  const run = getRefreshRun(db);
  assert.equal(run.status, 'failed');
});
