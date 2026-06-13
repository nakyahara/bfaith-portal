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
 */

import cron from 'node-cron';
import { getDB } from '../db.js';
import { runRysFullSync } from './rys-full-sync.js';

const DEFAULT_CRON_EXPR = '30 22 * * *'; // UTC 22:30 = JST 07:30

/**
 * cron で 1 回 fire される実体。 例外を飲んで logger に出す。
 */
export async function runRysCronTick() {
  const t0 = Date.now();
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
    return { ok: true, ...r };
  } catch (e) {
    const stage = e.stage || 'unknown';
    console.error(`[rys-cron] full sync failed at stage=${stage}: ${e.message} (${Date.now() - t0}ms)`);
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
