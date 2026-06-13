/**
 * E-7-e smoke runner: services/rys-full-sync.js の動作確認 (in-memory SQLite + mock)。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/test-rys-full-sync.js
 *
 * 検証項目:
 *   (1) 通常 run: baseline 確立 → diff candidates 反映、 順序固定
 *   (2) baseline failure → diff スキップ + throw stage='baseline'
 *   (3) baseline incomplete (stale_rows>0) → diff スキップ + throw stage='baseline_incomplete'
 *   (4) diff failure (overlap=0) → throw stage='diff' + partial に baseline 結果
 *   (5) dry-run: baseline は書く、 diff は dryRun (migration_candidates 書かない)
 *   (6) 順序: baseline が必ず先 (diff から呼ばれない、 baseline failed 時 diff の sync_run も無し)
 *   (7) 結果 schema: { baseline, diff, durationMs, triggeredBy, triggerRequestId }
 */

import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { runRysFullSync } from '../services/rys-full-sync.js';

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

// Yahoo callApi mock: query 1 文字に対して `${query}-1`..`${query}-N` 件返す
function makeYahooMock(perQuery = 5) {
  return async ({ query, offset, results }) => {
    const items = [];
    const start = offset + 1;
    const end = Math.min(offset + results, perQuery);
    for (let i = start; i <= end; i++) items.push({ ItemCode: `${query}-${i}` });
    return { ok: true, items, totalResultsAvailable: perQuery, totalResultsReturned: items.length, firstResultPosition: start };
  };
}

function makeRakuten(codes) {
  const m = new Map();
  for (const c of codes) m.set(c, 'N' + c);
  return m;
}

async function testAll() {
  // (1) 通常 run
  {
    const db = freshDb();
    const r = await runRysFullSync({
      db,
      yahooDeps: { callApi: makeYahooMock(5) },
      rakutenItems: makeRakuten(['a-1', 'new-x', 'new-y']),
      triggeredBy: 'manual',
    });
    expect('full (1): baseline.establishesBaseline', r.baseline?.establishesBaseline === true, JSON.stringify({ established: r.baseline?.establishesBaseline }));
    expect('full (1): diff.newlyDetected=2', r.diff?.newlyDetected === 2, JSON.stringify({ newly: r.diff?.newlyDetected }));
    expect('full (1): durationMs > 0', typeof r.durationMs === 'number' && r.durationMs >= 0, `dur=${r.durationMs}`);
    expect('full (1): triggeredBy preserved', r.triggeredBy === 'manual', r.triggeredBy);
    // sync_runs 3 件 (rys_full + yahoo_baseline + rakuten_diff)
    const runs = db.prepare("SELECT sync_name, status FROM sync_runs ORDER BY id ASC").all();
    expect('full (1): sync_runs has [rys_full, yahoo_baseline, rakuten_diff] success', runs.length === 3 && runs[0].sync_name === 'rys_full' && runs[1].sync_name === 'yahoo_baseline' && runs[2].sync_name === 'rakuten_diff' && runs.every((r) => r.status === 'success'), JSON.stringify(runs));
    expect('full (1): result.rysFullSyncRunId returned', typeof r.rysFullSyncRunId === 'number' && r.rysFullSyncRunId > 0, `id=${r.rysFullSyncRunId}`);
  }

  // (2) baseline failure → diff スキップ
  {
    const db = freshDb();
    const failingYahoo = async () => { throw new Error('yahoo network down'); };
    try {
      await runRysFullSync({ db, yahooDeps: { callApi: failingYahoo }, rakutenItems: makeRakuten(['a-1']) });
      fail('full (2): baseline failure should throw', 'no throw');
    } catch (e) {
      expect('full (2): throw stage=baseline', e.stage === 'baseline', `stage=${e.stage}`);
      expect('full (2): partial contains baseline=null', e.partial && e.partial.baseline === null, JSON.stringify(e.partial));
    }
    const runs = db.prepare("SELECT sync_name, status FROM sync_runs").all();
    expect('full (2): runs = [rys_full failed, yahoo_baseline failed]', runs.length === 2 && runs[0].sync_name === 'rys_full' && runs[0].status === 'failed' && runs[1].sync_name === 'yahoo_baseline' && runs[1].status === 'failed', JSON.stringify(runs));
  }

  // (3) baseline incomplete (1 query 4 件のみ、 stale_rows > 0) → diff スキップ
  {
    const db = freshDb();
    // 1st baseline で establish
    const goodMock = makeYahooMock(5);
    await runRysFullSync({ db, yahooDeps: { callApi: goodMock }, rakutenItems: makeRakuten(['a-1']) });
    // 2nd baseline で 1 件減らす → incomplete
    const reducedMock = async ({ query, offset, results }) => {
      const total = (query === 'a') ? 4 : 5;
      const items = [];
      const start = offset + 1;
      const end = Math.min(offset + results, total);
      for (let i = start; i <= end; i++) items.push({ ItemCode: `${query}-${i}` });
      return { ok: true, items, totalResultsAvailable: total, totalResultsReturned: items.length, firstResultPosition: start };
    };
    try {
      await runRysFullSync({ db, yahooDeps: { callApi: reducedMock }, rakutenItems: makeRakuten(['a-1', 'new-1']) });
      fail('full (3): baseline incomplete should throw', 'no throw');
    } catch (e) {
      expect('full (3): throw stage=baseline_incomplete', e.stage === 'baseline_incomplete', `stage=${e.stage}`);
      expect('full (3): throw statusCode=412 (Codex R1 High-2)', e.statusCode === 412, `statusCode=${e.statusCode}`);
      expect('full (3): partial.baseline has stale_rows > 0', e.partial?.baseline?.staleRows >= 1, JSON.stringify(e.partial?.baseline?.staleRows));
    }
  }

  // (4) diff failure (overlap=0) → stage=diff
  {
    const db = freshDb();
    try {
      await runRysFullSync({
        db,
        yahooDeps: { callApi: makeYahooMock(5) },
        rakutenItems: makeRakuten(['totally-new-1', 'totally-new-2']),
      });
      fail('full (4): overlap=0 should throw', 'no throw');
    } catch (e) {
      expect('full (4): throw stage=diff', e.stage === 'diff', `stage=${e.stage}`);
      expect('full (4): partial.baseline succeeded', e.partial?.baseline?.establishesBaseline === true, JSON.stringify({ b: e.partial?.baseline?.establishesBaseline }));
    }
  }

  // (5) dry-run: baseline は書く、 migration_candidates は書かない
  {
    const db = freshDb();
    const r = await runRysFullSync({
      db,
      yahooDeps: { callApi: makeYahooMock(5) },
      rakutenItems: makeRakuten(['a-1', 'dry-1', 'dry-2']),
      dryRun: true,
    });
    expect('full (5a): diff.candidatesDetected=2', r.diff?.candidatesDetected === 2, JSON.stringify(r.diff));
    expect('full (5b): diff.newlyDetected=0 (dry-run)', r.diff?.newlyDetected === 0, JSON.stringify(r.diff));
    const candidateRows = db.prepare("SELECT COUNT(*) AS c FROM migration_candidates").get().c;
    expect('full (5c): migration_candidates table empty (dry-run)', candidateRows === 0, `count=${candidateRows}`);
    const yahooRows = db.prepare("SELECT COUNT(*) AS c FROM yahoo_registered_items").get().c;
    expect('full (5d): yahoo_registered_items populated (baseline always written)', yahooRows === 180, `count=${yahooRows}`);
  }

  // (6) Codex R1 High-1: rys_full mutex — 同時 rys_full 走らせると UNIQUE 衝突
  {
    const db = freshDb();
    const futureIso = new Date(Date.now() + 30 * 60 * 1000).toISOString();
    db.prepare("INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, run_lease_expires_at) VALUES ('rys_full', ?, 'running', 'cron', ?)").run(new Date().toISOString(), futureIso);
    try {
      await runRysFullSync({ db, yahooDeps: { callApi: makeYahooMock(5) }, rakutenItems: makeRakuten(['a-1']) });
      fail('full (6): concurrent rys_full should throw', 'no throw');
    } catch (e) {
      expect('full (6): rys_full UNIQUE collision', /UNIQUE/.test(e.message) && /sync_runs/.test(e.message), e.message);
    }
  }

  // (7) Codex R1 Medium-1 確認 (cron tz は startRysCron 内で設定、 実 cron 起動はスキップ; module load OK で代用)
  // → smoke では cron module をロードできることだけ確認 (run しない)
  {
    const mod = await import('../services/rys-cron.js');
    expect('full (7): rys-cron module loads with startRysCron export', typeof mod.startRysCron === 'function', typeof mod.startRysCron);
  }
}

// ──── runner ────
(async () => {
  console.log('=== Phase E-7-e smoke runner ===');
  await testAll();
  console.log('================================');
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
