/**
 * Phase E-7-e: RYS daily cron (Render in-process)。
 *
 * 設計原則:
 *   - node-cron で in-process schedule (既存 biz-ops-overview / profit-analysis と同パターン)
 *   - Feature flag (RYS_FULL_SYNC_CRON_ENABLED): Dark Launch、 default OFF
 *   - cron 時刻 env 上書き可 (RYS_FULL_SYNC_CRON、 default '30 22 * * *' = UTC 22:30 = JST 07:30)
 *     miniPC daily-sync (JST 07:00 開始 ~08:20 完了) と少しずらして 07:30。 RYS は楽天 RMS proxy 経由なので
 *     miniPC daily-sync (mall API 並列) と重なっても問題は小さい (要件: rate limit helper 経由なら OK)。
 *   - 例外飲み込み (cron 自体は throw しない、 sync_runs.status='failed' に記録される)
 *   - 監視 (2026-09-14): 台帳 config/jobs-registry.mjs の `rys-daily-refresh` (dead-man 方式)。
 *       ok      = 「全部更新」パイプラインの完走 (RYS_AUTO_REFRESH=1)
 *       partial = 差分取得だけ成功 (RYS_AUTO_REFRESH 未設定)。台帳に partial_max_days が無いので締切は満たさない
 *                 = 毎朝「締切超過」に出る = ジャンル補完・出品前チェックが動いていないことの催促 (Codex PR-1 R1 Medium)
 *       fail    = どちらかの失敗
 *       打たない = 「前の回がまだ走っている」(409)。その日の締切超過として見える (走ったのに終わらない、を無音にしない)
 *     監視の記録失敗はジョブを巻き添えにしない (ping-local.js)
 */

import cron from 'node-cron';
import { getDB } from '../db.js';
import { runRysFullSync } from './rys-full-sync.js';
import { startRefreshRun, executeRefreshPipeline } from './refresh-pipeline.js';
import { pingJob } from '../../jobs-monitor/ping-local.js';

/** 台帳 (config/jobs-registry.mjs) の id */
export const RYS_JOB_ID = 'rys-daily-refresh';

const DEFAULT_CRON_EXPR = '30 22 * * *'; // UTC 22:30 = JST 07:30

/**
 * 再設計 R4: RYS_AUTO_REFRESH='1'|'true' なら daily tick で「全部更新」パイプライン
 * (full sync + genre backfill + Notion ページ作成 + 下書き補完 + Notion sync) を回す。
 * 未設定なら従来通り full sync のみ (後方互換)。
 */
function isAutoRefreshEnabled() {
  const v = process.env.RYS_AUTO_REFRESH;
  return v === '1' || v === 'true';
}

/**
 * cron で 1 回 fire される実体。 例外を飲んで logger に出す。
 */
export async function runRysCronTick() {
  const t0 = Date.now();
  if (isAutoRefreshEnabled()) {
    try {
      const db = getDB();
      const { runId, runToken } = startRefreshRun(db, { triggeredBy: 'cron' });
      const r = await executeRefreshPipeline({ db, runId, runToken, triggeredBy: 'cron' });
      console.log(`[rys-cron] refresh pipeline OK run_id=${r.runId} (${Date.now() - t0}ms)`);
      pingJob(RYS_JOB_ID, 'ok', `pipeline run_id=${r.runId} 候補+${r.steps?.full_sync?.candidatesNew ?? '?'} 出せる ${r.steps?.readiness_check?.okCount ?? '?'}/要修正 ${r.steps?.readiness_check?.blockedCount ?? '?'}`);
      return { ok: true, pipeline: true, runId: r.runId, steps: r.steps };
    } catch (e) {
      if (e.statusCode === 409) {
        console.warn(`[rys-cron] refresh pipeline skip (already running): ${e.message}`);
        return { ok: false, pipeline: true, skipped: 'already_running' };
      }
      console.error(`[rys-cron] refresh pipeline failed step=${e.failedStep ?? '?'}: ${e.message} (${Date.now() - t0}ms)`);
      pingJob(RYS_JOB_ID, 'fail', `pipeline step=${e.failedStep ?? '?'}: ${e.message}`);
      return { ok: false, pipeline: true, error: e.message, failedStep: e.failedStep ?? null };
    }
  }
  try {
    const db = getDB();
    const r = await runRysFullSync({ db, triggeredBy: 'cron' });
    console.log(
      `[rys-cron] full sync OK ` +
      `baseline observed=${r.baseline.itemsObserved} stale=${r.baseline.staleRows} ` +
      `diff rakutenTotal=${r.diff.rakutenTotal} overlap=${r.diff.overlap} ` +
      `candidates_new=${r.diff.newlyDetected} resolved=${r.diff.resolved} stale=${r.diff.staleFlipped} ` +
      `(${r.durationMs}ms)`
    );
    pingJob(RYS_JOB_ID, 'partial', `差分取得のみ (RYS_AUTO_REFRESH 未設定 = ジャンル補完・出品前チェックは動いていない) 候補+${r.diff?.newlyDetected ?? '?'}`);
    return { ok: true, ...r };
  } catch (e) {
    const stage = e.stage || 'unknown';
    console.error(`[rys-cron] full sync failed at stage=${stage}: ${e.message} (${Date.now() - t0}ms)`);
    pingJob(RYS_JOB_ID, 'fail', `full sync stage=${stage}: ${e.message}`);
    if (e.partial) console.error(`[rys-cron] partial result:`, JSON.stringify({
      baseline: e.partial.baseline ? { observed: e.partial.baseline.itemsObserved, establishes: e.partial.baseline.establishesBaseline } : null,
      diff: e.partial.diff ? { newlyDetected: e.partial.diff.newlyDetected } : null,
    }));
    return { ok: false, error: e.message, stage };
  }
}

/**
 * RYS_FULL_SYNC_CRON_ENABLED='true' のときだけ schedule する。
 */
export function startRysCron() {
  const enabled = process.env.RYS_FULL_SYNC_CRON_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[rys-cron] RYS_FULL_SYNC_CRON_ENABLED が未設定/false のためスケジュールしない (Dark Launch)');
    return null;
  }
  const cronExpr = process.env.RYS_FULL_SYNC_CRON || DEFAULT_CRON_EXPR;
  if (!cron.validate(cronExpr)) {
    console.error(`[rys-cron] RYS_FULL_SYNC_CRON が不正: ${cronExpr}`);
    return null;
  }
  // Codex R1 Medium-1: timezone 明示 (host TZ 依存を避ける)。 既存 biz-ops-overview パターン同型。
  const task = cron.schedule(cronExpr, () => {
    runRysCronTick().catch((e) => {
      console.error('[rys-cron] cron 実行中に未捕捉例外:', e);
    });
  }, { timezone: 'UTC' });
  console.log(`[rys-cron] スケジュール開始: cron='${cronExpr}' (UTC) — JST に変換すると ${cronExpr === DEFAULT_CRON_EXPR ? '07:30' : 'env 設定確認'}`);
  return task;
}
