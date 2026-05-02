/**
 * snapshot-fba-stock.js — SP-API レポート (RESTOCK + PLANNING) を fetch して
 * fba.db.daily_snapshots に保存する日次 cron スクリプト。
 *
 * 既存の fba-service.js /fetch-reports POST と同じ処理を CLI 化。
 * daily-sync.js から朝 7:30〜8:00 頃に呼ばれる想定。
 *
 * 排他: fba-fetch-lock.js のlockfileで手動実行 (UI からの /fetch-reports) と排他。
 *       既に走っていれば skip して通常終了 (=daily-sync 全体は失敗扱いにしない)。
 *
 * business_date: process.env.WAREHOUSE_BUSINESS_DATE (daily-sync が JST で確定)
 */
import 'dotenv/config';
import { initDb, savePlanningData, saveRestockLatest, savePlanningLatest, saveRestockInventoryToDailySnapshot, updateFnskuBatch, syncFnskuBatch } from '../fba-replenishment/db.js';
import { fetchAllReports, normalizePlanningRow, normalizeRestockRow } from '../fba-replenishment/sp-api-reports.js';
import { acquireFbaFetchLock, releaseFbaFetchLock } from './fba-fetch-lock.js';

function toJstDate(d) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function resolveBusinessDate() {
  if (process.env.WAREHOUSE_BUSINESS_DATE) return process.env.WAREHOUSE_BUSINESS_DATE;
  const cliArg = process.argv.slice(2).find(a => a.startsWith('--date='));
  if (cliArg) return cliArg.split('=')[1];
  return toJstDate(new Date());
}

async function main() {
  const businessDate = resolveBusinessDate();
  console.log(`[fba-stock-snapshot] business_date=${businessDate} 開始`);

  const lock = acquireFbaFetchLock('cron');
  if (!lock.acquired) {
    console.log(`[fba-stock-snapshot] 既に実行中のためスキップ:`, lock.holder);
    process.exit(0); // 失敗扱いにせず通常終了
  }

  try {
    await initDb();

    console.log('[fba-stock-snapshot] SP-API レポート3種を並列取得中...');
    const t0 = Date.now();
    const results = await fetchAllReports();
    const fetchMs = Date.now() - t0;
    console.log(`[fba-stock-snapshot] 取得完了 (${(fetchMs/1000).toFixed(1)}秒): planning=${results.planning?.length || 0} restock=${results.restock?.length || 0} errors=${(results.errors||[]).length}`);

    let savedPlanning = 0, savedRestockToDaily = { updated: 0, inserted: 0 }, savedRestockLatest = 0, savedPlanningLatest = 0;

    // ① RESTOCK 先行: daily_snapshots の在庫7列 (4既存 + 3追加) を確定
    //   先に書いておけば、後続 PLANNING の ON CONFLICT DO UPDATE で 3列が保持される
    //   (中間状態に「3列=0」が現れない)
    if (results.restock?.length > 0) {
      const normalized = results.restock.map(normalizeRestockRow).filter(r => r.amazon_sku);
      savedRestockToDaily = saveRestockInventoryToDailySnapshot(normalized, businessDate);
      console.log(`[fba-stock-snapshot] RESTOCK → daily_snapshots: updated=${savedRestockToDaily.updated} inserted=${savedRestockToDaily.inserted}`);

      try {
        const r = saveRestockLatest(normalized);
        savedRestockLatest = r.saved || 0;
      } catch (e) { console.warn('[fba-stock-snapshot] saveRestockLatest 失敗:', e.message); }

      // FNSKU 更新 (RESTOCK からも取れる)
      const fnskuRows = normalized.filter(r => r.fnsku && r.amazon_sku).map(r => ({ sku: r.amazon_sku, fnsku: r.fnsku }));
      if (fnskuRows.length > 0) updateFnskuBatch(fnskuRows);
    }

    // ② PLANNING で sales/price/days_of_supply 等の追加列を上書き (3カラムは保持)
    if (results.planning?.length > 0) {
      const normalized = results.planning.map(normalizePlanningRow);
      savedPlanning = savePlanningData(normalized, businessDate);
      console.log(`[fba-stock-snapshot] PLANNING → daily_snapshots: ${savedPlanning}件 (3カラムは保持)`);

      try {
        const r = savePlanningLatest(normalized);
        savedPlanningLatest = r.saved || 0;
      } catch (e) { console.warn('[fba-stock-snapshot] savePlanningLatest 失敗:', e.message); }

      // FNSKU 同期
      const fnskuRows = results.planning.filter(r => r['sku']).map(r => ({ sku: r['sku'], fnsku: r['fnsku'] || null }));
      if (fnskuRows.length > 0) syncFnskuBatch(fnskuRows);
    }

    if (results.errors?.length) {
      console.warn('[fba-stock-snapshot] errors:', JSON.stringify(results.errors));
    }

    console.log(`[fba-stock-snapshot] ✅ 完了: planning=${savedPlanning} restock_daily=${savedRestockToDaily.updated + savedRestockToDaily.inserted} restock_latest=${savedRestockLatest} planning_latest=${savedPlanningLatest}`);
  } finally {
    // lock オブジェクト全体を渡して所有権 (ownerToken) チェックを有効化
    releaseFbaFetchLock(lock);
  }
  process.exit(0);
}

main().catch(e => {
  console.error('[fba-stock-snapshot] 致命的エラー:', e.message, e.stack);
  process.exit(1);
});
