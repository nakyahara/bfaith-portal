/**
 * 楽天順位チェッカー スケジューラー
 *
 * node-cron で以下を実行:
 *   - 毎日 03:00 JST (18:00 UTC) → 順位自動チェック
 *   - 毎日 09:00 JST (00:00 UTC) → CSV生成 + Google Drive保存
 *   - チェック完了後 → 365日超の古いデータを削除
 */
import cron from 'node-cron';
import { runAutoCheck, DATA_FILE, readJson, writeJson, log } from './auto-check.js';
import { exportCSVToDrive } from './csv-export.js';

const DATA_RETENTION_DAYS = 365;

// ── 古いデータ削除 ──

function cleanupOldData() {
  const data = readJson(DATA_FILE, { products: [] });
  const products = data.products || [];
  if (!products.length) return;

  const now = new Date(Date.now() + 9 * 60 * 60 * 1000); // JST
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - DATA_RETENTION_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  let totalRemoved = 0;
  for (const product of products) {
    if (!product.history || !product.history.length) continue;
    const before = product.history.length;
    product.history = product.history.filter(entry => entry.date >= cutoffStr);
    totalRemoved += before - product.history.length;
  }

  if (totalRemoved > 0) {
    writeJson(DATA_FILE, { products });
    log(`データクリーンアップ: ${totalRemoved} 件の古いエントリを削除（${DATA_RETENTION_DAYS}日超）`);
  }
}

// ── スケジューラー起動 ──

export function startScheduler() {
  // 毎日 03:00 JST = 18:00 UTC (前日)
  cron.schedule('0 18 * * *', async () => {
    log('--- スケジュール実行: 順位自動チェック (03:00 JST) ---');
    try {
      await runAutoCheck();
      cleanupOldData();
    } catch (e) {
      log(`自動チェックエラー: ${e.message}`);
    }
  }, { timezone: 'UTC' });

  // 毎日 09:00 JST = 00:00 UTC
  cron.schedule('0 0 * * *', async () => {
    log('--- スケジュール実行: CSV出力 (09:00 JST) ---');
    try {
      await exportCSVToDrive();
    } catch (e) {
      log(`CSV出力エラー: ${e.message}`);
    }
  }, { timezone: 'UTC' });

  console.log('[Scheduler] 楽天順位チェッカー スケジュール登録完了');
  console.log('[Scheduler]   03:00 JST → 順位自動チェック + データクリーンアップ');
  console.log('[Scheduler]   09:00 JST → CSV出力 → Google Drive');
}
