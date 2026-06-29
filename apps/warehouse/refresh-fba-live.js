/**
 * refresh-fba-live.js — 商品管理リスト(PML) オンデマンドFBA更新 ① 取得層
 *
 * SP-API RESTOCKレポートのみを取得し、warehouse.db の fba_restock_live を全件置換する。
 * daily_snapshots(日次の不変スナップショット)は一切触らない。ライブ値はこの1表だけに入れる。
 *
 * - 取得は fba-replenishment/sp-api-reports.js の実績ある RESTOCK 経路を流用 (PLANNINGは取得しない)。
 * - SP-API の createReport 競合を避けるため fba-fetch-lock を取得する
 *   (cron snapshot-fba-stock.js / 手動 /fetch-reports と排他)。取得中なら fail (上書きしない)。
 * - fail-closed: RESTOCK が 0件 で返ったら fba_restock_live を消さずに throw (FBA在庫が空になる事故を防ぐ)。
 * - 全件置換は1トランザクション (build が中間状態=半分空を読まない)。全行が同一 source_run_id / fetched_at。
 *
 * build-product-management-snapshot.js --fba-source=live がこの表を v_sku_resolved で
 * NE商品コードへ展開して FBA在庫数(倉庫+入荷待ち) を算出する。
 *
 * 使い方(単独): node apps/warehouse/refresh-fba-live.js
 * 通常はオンデマンドジョブ(fba-service.js POST /service-api/pml/fba-refresh)から呼ばれる。
 */
import 'dotenv/config';
import { getDB, initDB } from './db.js';
import { fetchRestockRecommendations, normalizeRestockRow, getMarketplaceContext } from '../fba-replenishment/sp-api-reports.js';
import { acquireFbaFetchLock, releaseFbaFetchLock } from './fba-fetch-lock.js';

const LIVE_COLS = [
  'fba_available', 'fba_fc_transfer', 'fba_fc_processing', 'fba_customer_order',
  'fba_inbound_working', 'fba_inbound_shipped', 'fba_inbound_received',
];

/**
 * RESTOCK を取得して fba_restock_live を全件置換する。
 * @returns {{ source_run_id: string, row_count: number, fetched_at: string }}
 * @throws SP-APIロック取得失敗 / RESTOCK 0件 / 取得例外時
 */
export async function refreshFbaLive() {
  const db = getDB();
  const sourceRunId = `fbalive_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

  // SP-API 排他 (cron / 手動 /fetch-reports と createReport 競合を防ぐ)
  const lock = acquireFbaFetchLock('pml-live');
  if (!lock.acquired) {
    throw new Error(`SP-API取得が別プロセスで実行中のため中止 (holder=${JSON.stringify(lock.holder)})`);
  }

  try {
    const ctx = getMarketplaceContext('jp');
    const t0 = Date.now();
    const raw = await fetchRestockRecommendations(ctx);
    const normalized = raw.map(normalizeRestockRow).filter(r => r.amazon_sku);
    console.log(`[fba-live] RESTOCK取得: raw=${raw.length} 正規化=${normalized.length} (${((Date.now() - t0) / 1000).toFixed(1)}秒)`);

    // fail-closed: 0件なら既存 live を消さずに失敗させる (FBA在庫が空になる事故防止)
    if (normalized.length === 0) {
      throw new Error('RESTOCK が 0件。fba_restock_live を更新せず中止 (fail-closed)');
    }

    const fetchedAt = new Date().toISOString();
    const ins = db.prepare(`INSERT INTO fba_restock_live
      (amazon_sku, ${LIVE_COLS.join(', ')}, source_run_id, fetched_at)
      VALUES (?, ${LIVE_COLS.map(() => '?').join(', ')}, ?, ?)
      ON CONFLICT(amazon_sku) DO UPDATE SET
        ${LIVE_COLS.map(c => `${c}=excluded.${c}`).join(', ')},
        source_run_id=excluded.source_run_id, fetched_at=excluded.fetched_at`);

    const tx = db.transaction(() => {
      db.exec('DELETE FROM fba_restock_live');
      for (const r of normalized) {
        ins.run(r.amazon_sku, ...LIVE_COLS.map(c => r[c] ?? 0), sourceRunId, fetchedAt);
      }
    });
    tx();
    try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch {}

    console.log(`[fba-live] ✅ fba_restock_live 全件置換: ${normalized.length}件 run=${sourceRunId} fetched_at=${fetchedAt}`);
    return { source_run_id: sourceRunId, row_count: normalized.length, fetched_at: fetchedAt };
  } finally {
    releaseFbaFetchLock(lock);
  }
}

// ─── 単独実行 ───
const isMain = process.argv[1]?.includes('refresh-fba-live');
if (isMain) {
  await initDB();
  const r = await refreshFbaLive();
  console.log('\n結果:', JSON.stringify(r, null, 2));
  process.exit(0);
}
