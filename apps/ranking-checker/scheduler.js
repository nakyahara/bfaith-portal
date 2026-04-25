/**
 * 楽天順位チェッカー スケジューラー（SQLite版）
 *
 * node-cron:
 *   - 毎日 13:00 JST (04:00 UTC) → 順位自動チェック
 *     [Phase 2 以降] 本番では RANKCHECK_AUTO_ENABLED=false とし、miniPC の Task Scheduler
 *     + run-rankcheck-safe.ps1 で Runner を起動する。このブランチは dev/fallback 用。
 *   - 毎日 09:00 JST (00:00 UTC) → CSV 生成 + Google Drive 保存
 *     Render で動作、PROXY_MODE のときは miniPC /service-api/rankcheck/data を fetch。
 *   - チェック完了後 → 365日超データを SQL 1本で削除
 */
import cron from 'node-cron';
import { runAutoCheck } from './auto-check.js';
import { log } from './helpers.js';
import { exportCSVToDrive } from './csv-export.js';
import * as rdb from './db.js';

const DATA_RETENTION_DAYS = 365;
const RUNMETA_RETENTION_DAYS = 60;

function cleanupOldData() {
  const removed = rdb.cleanupOldHistory(DATA_RETENTION_DAYS);
  if (removed > 0) log(`データクリーンアップ: ${removed} 件の古いエントリを削除（${DATA_RETENTION_DAYS}日超）`);

  const meta = rdb.cleanupOldRunMeta(RUNMETA_RETENTION_DAYS);
  if (meta.runs + meta.logs > 0) log(`run_state/run_log クリーンアップ: runs=${meta.runs} logs=${meta.logs} (${RUNMETA_RETENTION_DAYS}日超)`);
}

export function startScheduler() {
  const autoEnabled = process.env.RANKCHECK_AUTO_ENABLED === 'true';
  if (autoEnabled) {
    cron.schedule('0 4 * * *', async () => {
      log('--- スケジュール実行: 順位自動チェック (13:00 JST) ---');
      try {
        await runAutoCheck();
        cleanupOldData();
      } catch (e) {
        log(`自動チェックエラー: ${e.message}`);
      }
    }, { timezone: 'UTC' });
  }

  cron.schedule('0 0 * * *', async () => {
    log('--- スケジュール実行: CSV出力 (09:00 JST) ---');
    try {
      await exportCSVToDrive();
    } catch (e) {
      log(`CSV出力エラー: ${e.message}`);
    }
  }, { timezone: 'UTC' });

  console.log('[Scheduler] 楽天順位チェッカー スケジュール登録完了 (SQLite版)');
  if (autoEnabled) {
    console.log('[Scheduler]   13:00 JST → 順位自動チェック + データクリーンアップ (ENABLED)');
  } else {
    console.log('[Scheduler]   13:00 JST 自動チェックは無効化中 (RANKCHECK_AUTO_ENABLED=true で有効化)');
  }
  console.log('[Scheduler]   09:00 JST → CSV出力 → Google Drive');
}
