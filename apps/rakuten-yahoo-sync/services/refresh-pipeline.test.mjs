/**
 * refresh-pipeline (再設計 R4、 Codex R4-R1 反映版) の test。 外部 I/O は deps 注入で stub。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/services/refresh-pipeline.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import Db from 'better-sqlite3';

import { startRefreshRun, executeRefreshPipeline, getRefreshRun, findActiveRefreshRun, NOTION_RETIRED_STEPS, NOTION_RETIRED_AT } from './refresh-pipeline.js';

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
    // 2026-09-14 Notion 廃止: 3 ステップは呼ばれてはいけない (呼んだら test が落ちる)
    createNotionPagesFromRakuten: async () => { throw new Error('createNotionPagesFromRakuten は廃止 — 呼んではいけない'); },
    seedNotionDrafts: async () => { throw new Error('seedNotionDrafts は廃止 — 呼んではいけない'); },
    syncNotionOverrides: async () => { throw new Error('syncNotionOverrides は廃止 — 呼んではいけない'); },
    acquireNotionSyncLock: () => { throw new Error('acquireNotionSyncLock は廃止 — 呼んではいけない'); },
    sweepReadiness: async () => ({ picked: 10, evaluated: 10, okCount: 8, blockedCount: 2, errors: 0, errorSamples: [] }),
    ...overrides,
  };
}

async function runPipeline(db, deps, triggeredBy = 'manual') {
  const { runId, runToken } = startRefreshRun(db, { triggeredBy });
  return executeRefreshPipeline({ db, runId, runToken, triggeredBy, deps });
}

test('happy path: 3 ステップ走って success + Notion 3 ステップは理由付き skipped で記録', async () => {
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
  // 廃止した 3 ステップ: ok:true の no-op ではなく skipped + reason (Codex 2 巡目: 「同期できた」と読めない形)
  assert.deepEqual([...NOTION_RETIRED_STEPS], ['notion_pages', 'draft_seed', 'notion_sync']);
  for (const name of NOTION_RETIRED_STEPS) {
    assert.equal(run.steps[name].skipped, true, name + ' は skipped');
    assert.equal(run.steps[name].reason, 'notion_retired', name + ' の理由');
    assert.equal(run.steps[name].retiredAt, NOTION_RETIRED_AT);
    assert.equal('ok' in run.steps[name], false, name + ' に ok を書かない');
  }
  assert.equal(run.steps.readiness_check.ok, true);
  // steps_json のキー順 = STEP_NAMES 順 (failedAt の算出と画面の表示順が依存)
  assert.deepEqual(Object.keys(run.steps), ['full_sync', 'genre_backfill', 'notion_pages', 'draft_seed', 'notion_sync', 'readiness_check']);
});

test('Notion 廃止: deps に Notion 系の stub を渡さなくても走る (実装が import していない)', async () => {
  const db = setupDb();
  const deps = okDeps();
  delete deps.createNotionPagesFromRakuten;
  delete deps.seedNotionDrafts;
  delete deps.syncNotionOverrides;
  delete deps.acquireNotionSyncLock;
  const r = await runPipeline(db, deps);
  assert.equal(r.status, 'success');
});

test('Notion 廃止: genre_backfill の後の current_step は readiness_check (Notion のステップ名を「実行中」に見せない)', async () => {
  const db = setupDb();
  let seenStep = null;
  const deps = okDeps({
    sweepReadiness: async () => {
      seenStep = db.prepare('SELECT current_step FROM refresh_runs ORDER BY run_id DESC LIMIT 1').get().current_step;
      return { picked: 0, evaluated: 0, okCount: 0, blockedCount: 0, errors: 0, errorSamples: [] };
    },
  });
  await runPipeline(db, deps);
  assert.equal(seenStep, 'readiness_check');
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

test('readiness_check: sweep 結果が steps に記録される (R8)', async () => {
  const db = setupDb();
  const r = await runPipeline(db, okDeps());
  assert.equal(r.steps.readiness_check.ok, true);
  assert.equal(r.steps.readiness_check.evaluated, 10);
  assert.equal(r.steps.readiness_check.blockedCount, 2);
});

test('readiness_check: sweep の throw (全滅等) は pipeline failure (R8)', async () => {
  const db = setupDb();
  const deps = okDeps({
    sweepReadiness: async () => { throw new Error('readiness sweep: 1件も評価できませんでした (picked=5, errors=5)'); },
  });
  await assert.rejects(() => runPipeline(db, deps), (e) => e.failedStep === 'readiness_check' && /評価できません/.test(e.message));
  const run = getRefreshRun(db);
  assert.equal(run.status, 'failed');
  assert.equal(run.current_step, 'readiness_check');
});
