/**
 * pml-fba-refresh.js — 商品管理リストの FBA 在庫の手の更新 (fba-service.js の /pml/fba-refresh の中身。試験で差し替えられるように切り出した)
 *
 * 順番: RESTOCK の取得 → 商品管理リストの snapshot を作る → Render へ送る。
 * 🚨 Company DB の写しの反映の門 (warehouse.db の cdb_publish_gate = publish-gate.js の readPublishGate) が safe でないあいだは、
 *   m_products から snapshot を作らない・送らない (daily-sync・自動再試行と同じ正。#1564 の見直し M-2)。作る前と送る前の 2 回見る
 */

/** 止めるときに投げる */
export function publishBrokenError(g) {
  return Object.assign(new Error(`⚠️ 見送り: Company DB の写しの反映の門 = ${g.state ?? 'broken'} (${g.reason ?? '?'}・確かめ ${g.checked_at ?? '?'}・作り直し ${g.build_id ?? '?'}) = 商品管理リストを作らない・送らない。master-publish の証跡の apply を見る`),
    { code: 'PUBLISH_BROKEN' });
}

/**
 * @param {object} p
 * @param {Function} p.refresh  RESTOCK を取る (refreshFbaLive)
 * @param {Function} p.build    snapshot を作る (buildProductManagementSnapshot)
 * @param {Function} p.sync     Render へ送る (syncPmlSnapshotOnly)
 * @param {Function} p.gate     () => { broken, state, reason, checked_at, build_id } (publish-gate.js の readPublishGate = warehouse.db の門)
 */
export async function runPmlFbaRefresh({ updateProgress = () => {}, refresh, build, sync, gate }) {
  const check = () => { const g = gate(); if (g && g.broken) throw publishBrokenError(g); };
  check();
  updateProgress({ step: 'fetch-restock', message: 'AmazonからRESTOCK在庫を取得中…(数分かかります)' });
  const live = await refresh();

  check();
  updateProgress({ step: 'build', message: `スナップショット再生成中 (FBA ${live.row_count}件)…` });
  const built = await build({ fbaSource: 'live' });
  if (!built.ok) {
    throw new Error(`snapshot生成に失敗 (status=${built.status}): ${(built.reasons || []).join('; ')}`);
  }

  check();
  updateProgress({ step: 'sync', message: 'Renderへ反映中…' });
  const synced = await sync();
  if (synced.state !== 'sent') {
    throw new Error(`Render同期に失敗/スキップ: ${synced.reason || synced.state}`);
  }

  return {
    fba_fetched_at: live.fetched_at,
    fba_row_count: live.row_count,
    pml_run_id: built.run_id,
    synced_count: synced.count,
  };
}
