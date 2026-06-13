/**
 * Phase E-7-e: RYS フルシンク (Yahoo baseline → 楽天 diff の順序実行)。
 *
 * 設計原則 (Codex E-7 R2 E-1):
 *   - 順序固定: Yahoo baseline 取得 → 楽天 fetch + diff 計算 → migration_candidates upsert
 *     楽天先 + Yahoo 古いままだと candidate を誤検出するため、 Yahoo baseline を必ず先行させる。
 *   - 1 run = 1 mutex (partial UNIQUE INDEX on sync_runs)
 *   - 例外は飲み込まない (sync_runs に failed として記録、 caller には throw 伝播)
 *   - Render 完結 (中原さん確定): mirror sync 不要、 同プロセス内で sync_runs 直読み
 *
 * 提供:
 *   - runRysFullSync({ db, triggeredBy, triggerRequestId, dryRun, allowZeroOverlap, ... })
 */

import crypto from 'node:crypto';

import {
  syncYahooBaseline,
  finishSyncRun,
  stealStaleRunningRuns,
} from '../lib/yahoo-store-sync.js';
import { detectMigrationCandidates } from '../lib/diff-detector.js';

const RYS_FULL_LEASE_MS = 90 * 60 * 1000; // baseline ~数十秒 + diff ~数十秒、 余裕 90 分

function isoNow() { return new Date().toISOString(); }

function insertRysFullRun(db, { triggeredBy, triggerRequestId, startedAt, runLeaseExpiresAt, runToken, dryRun }) {
  const r = db.prepare(`
    INSERT INTO sync_runs (sync_name, started_at, status, triggered_by, trigger_request_id, run_lease_expires_at, run_token, dry_run)
    VALUES ('rys_full', ?, 'running', ?, ?, ?, ?, ?)
  `).run(startedAt, triggeredBy, triggerRequestId, runLeaseExpiresAt, runToken, dryRun ? 1 : 0);
  return r.lastInsertRowid;
}

/**
 * Yahoo baseline → 楽天 diff の順で run。
 *
 * @param {object} args
 * @param {Database} args.db
 * @param {'cron'|'manual'} args.triggeredBy
 * @param {string|null} args.triggerRequestId
 * @param {boolean} args.dryRun                 - diff のみ dry-run (baseline は常に書き込む)
 * @param {boolean} args.allowZeroOverlap       - 楽天↔Yahoo overlap=0 を許容
 * @param {object} [args.yahooDeps]             - syncYahooBaseline に渡す deps (テスト用)
 * @param {Map|object|null} [args.rakutenItems] - detectMigrationCandidates のテスト用注入
 * @returns {Promise<object>} { baseline: {...}, diff: {...}, durationMs }
 *
 * baseline / diff のどちらかで throw した場合、 まだ走ってない方は skip し
 * 既に走った方の結果と error を返す (caller が見やすいよう例外には .partial 添付)。
 */
export async function runRysFullSync({
  db,
  triggeredBy = 'cron',
  triggerRequestId = null,
  dryRun = false,
  allowZeroOverlap = false,
  yahooDeps = undefined,
  rakutenItems = null,
} = {}) {
  const t0 = Date.now();
  const result = {
    baseline: null,
    diff: null,
    rysFullSyncRunId: null,
    durationMs: 0,
    triggeredBy,
    triggerRequestId,
  };

  // Codex R1 High-1: rys_full level mutex を 1 件確保。 sync_runs (rys_full) の partial UNIQUE INDEX
  //   で同時 rys_full は 409 になる。 baseline / diff の個別 sync_name は子 run として残す。
  //   lease 切れ stale running は steal する。
  stealStaleRunningRuns(db, 'rys_full');
  const rysRunToken = crypto.randomUUID();
  const rysRunLeaseExpiresAt = new Date(Date.now() + RYS_FULL_LEASE_MS).toISOString();
  const rysStartedAt = isoNow();
  const rysFullSyncRunId = insertRysFullRun(db, {
    triggeredBy, triggerRequestId,
    startedAt: rysStartedAt,
    runLeaseExpiresAt: rysRunLeaseExpiresAt,
    runToken: rysRunToken,
    dryRun,
  });
  result.rysFullSyncRunId = rysFullSyncRunId;

  let throwToCaller = null;

  // (1) Yahoo baseline (常に書く、 dryRun でも baseline は書く運用)
  try {
    result.baseline = await syncYahooBaseline({
      db, deps: yahooDeps || {}, triggeredBy, triggerRequestId,
    });
  } catch (e) {
    const err = new Error(`runRysFullSync: baseline failed: ${e.message}`);
    err.cause = e;
    err.stage = 'baseline';
    if (e.statusCode) err.statusCode = e.statusCode;
    err.partial = { ...result, durationMs: Date.now() - t0 };
    throwToCaller = err;
  }

  // baseline establish しなければ diff は走らせない (assertYahooBaselineFixed が 412 で塞ぐが、 ここで明示)
  if (!throwToCaller && !result.baseline.establishesBaseline) {
    const err = new Error(
      `runRysFullSync: baseline run did not establish (stale_rows=${result.baseline.staleRows}, ` +
      `anyIncomplete=${result.baseline.anyIncomplete}). Skip diff.`
    );
    err.stage = 'baseline_incomplete';
    err.statusCode = 412; // Codex R1 High-2: precondition 失敗は 412
    err.partial = { ...result, durationMs: Date.now() - t0 };
    throwToCaller = err;
  }

  // (2) 楽天 ↔ Yahoo diff
  if (!throwToCaller) {
    try {
      result.diff = await detectMigrationCandidates({
        db, rakutenItems, triggeredBy, triggerRequestId, dryRun, allowZeroOverlap,
      });
    } catch (e) {
      const err = new Error(`runRysFullSync: diff failed: ${e.message}`);
      err.cause = e;
      err.stage = 'diff';
      if (e.statusCode) err.statusCode = e.statusCode;
      err.partial = { ...result, durationMs: Date.now() - t0 };
      throwToCaller = err;
    }
  }

  // (3) rys_full sync_run を success/failed に閉じる (token 一致を要求)
  try {
    if (throwToCaller) {
      finishSyncRun(db, rysFullSyncRunId, rysRunToken, {
        status: 'failed',
        error_message: String(throwToCaller.message || throwToCaller).slice(0, 4000),
      });
    } else {
      finishSyncRun(db, rysFullSyncRunId, rysRunToken, {
        status: 'success',
        rakuten_total: result.diff?.rakutenTotal,
        yahoo_total: result.diff?.yahooTotal,
        overlap: result.diff?.overlap,
        overlap_rate: result.diff?.overlapRate,
        candidates_new: result.diff?.newlyDetected,
        candidates_resolved: result.diff?.resolved,
      });
    }
  } catch (e) {
    if (e.code !== 'LEASE_LOST') {
      // finalize 自体の失敗 (rare) は元 throw を優先するが、 警告する
      // throwToCaller が既にあれば優先、 なければ新規 throw
      if (!throwToCaller) throwToCaller = e;
    }
  }

  result.durationMs = Date.now() - t0;
  if (throwToCaller) throw throwToCaller;
  return result;
}
