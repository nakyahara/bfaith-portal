/**
 * FBA在庫補充 サービスAPI
 * /service-api/fba/* にマウント
 *
 * ミニPC上の既存FBAモジュール（sp-api-reports, inbound-plans, sheets-sync,
 * calculation-engine, db）を直接importして、サービスAPIとして公開する。
 *
 * 既存router.jsの内部認証をバイパスする必要があるため、
 * 内部HTTPプロキシではなく、モジュール直接呼び出しとする。
 */
import { Router } from 'express';
import { rateLimitMiddleware } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';
import { createJob } from './job-manager.js';

// --- 既存FBAモジュール ---
import { fetchAllReports, normalizePlanningRow, normalizeRestockRow } from '../fba-replenishment/sp-api-reports.js';
import { acquireFbaFetchLock, releaseFbaFetchLock } from './fba-fetch-lock.js';
import {
  createInboundPlan as spCreateInboundPlan,
  listShipments,
  listShipmentItems,
  checkInboundEligibility,
  fetchActiveInboundQuantities,
} from '../fba-replenishment/inbound-plans.js';
import { syncSkuMappings } from '../fba-replenishment/sheets-sync.js';
import { generateRecommendations } from '../fba-replenishment/calculation-engine.js';

// db.jsは default export + named exports の混在なので動的importで対応
let db;
async function getDb() {
  if (!db) {
    db = await import('../fba-replenishment/db.js');
  }
  return db;
}

const router = Router();

// ヘルパー: DBエラーをキャッチして返す
function dbHandler(fn) {
  return async (req, res) => {
    try {
      const db = await getDb();
      const result = await fn(req, res, db);
      if (result !== undefined && !res.headersSent) {
        okResponse(res, result);
      }
    } catch (e) {
      if (!res.headersSent) {
        errorResponse(res, { status: 500, error: 'INTERNAL_ERROR', message: e.message, requestId: req.requestId });
      }
    }
  };
}

// ==========================================
// レポート取得（ジョブ化 + 二重実行防止）
// ==========================================

let fetchReportsJobId = null;

router.post('/fetch-reports', rateLimitMiddleware('sp-api'), async (req, res) => {
  // プロセス内: 同一 WarehouseServer プロセスでのジョブ重複防止
  if (fetchReportsJobId) {
    const { getJob } = await import('./job-manager.js');
    const existing = getJob(fetchReportsJobId);
    if (existing && existing.status === 'running') {
      return okResponse(res, { jobId: fetchReportsJobId, status: 'already_running', message: 'レポート取得が既に実行中です' }, 202);
    }
    fetchReportsJobId = null;
  }

  // プロセス跨ぎ: cron (snapshot-fba-stock.js) と排他。lockfile が他プロセスで保持中なら拒否
  const lock = acquireFbaFetchLock('manual');
  if (!lock.acquired) {
    return okResponse(res, {
      status: 'already_running',
      message: '別プロセスでレポート取得中です (cron や別セッションの可能性)。完了を待ってください。',
      holder: lock.holder,
    }, 202);
  }

  const job = createJob('fba-fetch-reports', async (updateProgress) => {
    try {
      updateProgress({ step: 'starting', message: 'SP-APIレポート3種を並列取得中...' });
      const results = await fetchAllReports();
      const db = await getDb();

      let planningCount = 0, restockCount = 0;
      let restockGuardSkipped = false, planningGuardSkipped = false;

      // --- RESTOCK (主軸データソース) ---
      if (results.restock?.length > 0) {
        updateProgress({ step: 'saving-restock', count: results.restock.length });
        const normalizedRestock = results.restock.map(normalizeRestockRow).filter(r => r.amazon_sku);
        const saveRes = db.saveRestockLatest(normalizedRestock);
        restockCount = saveRes.saved;
        restockGuardSkipped = saveRes.skipped;
        if (saveRes.skipped) {
          console.warn('[FBA-Service] RESTOCK 保存スキップ:', saveRes.reason, saveRes);
        }
        // FNSKU 更新 (RESTOCK からも取れる)
        const fnskuRows = normalizedRestock
          .filter(r => r.fnsku && r.amazon_sku)
          .map(r => ({ sku: r.amazon_sku, fnsku: r.fnsku }));
        if (fnskuRows.length > 0) db.updateFnskuBatch(fnskuRows);
      }

      // --- PLANNING (補助データソース、取得失敗OK) ---
      if (results.planning?.length > 0) {
        updateProgress({ step: 'saving-planning', count: results.planning.length });
        const normalized = results.planning.map(normalizePlanningRow);
        // dual-write: 既存の daily_snapshots 経路と、新 planning_latest の両方に書き込む
        try {
          db.savePlanningData(normalized);
        } catch (e) {
          console.warn('[FBA-Service] savePlanningData failed (legacy):', e.message);
        }
        try {
          const saveRes = db.savePlanningLatest(normalized);
          planningGuardSkipped = saveRes.skipped;
          if (saveRes.skipped) {
            console.warn('[FBA-Service] PLANNING 保存スキップ:', saveRes.reason, saveRes);
          }
        } catch (e) {
          console.warn('[FBA-Service] savePlanningLatest failed:', e.message);
        }
        planningCount = normalized.length;
        // PLANNING報告に含まれる全SKUについて現在のFNSKUを明示同期（nullなら明示的にクリア）
        const fnskuRows = results.planning
          .filter(r => r['sku'])
          .map(r => ({ sku: r['sku'], fnsku: r['fnsku'] || null }));
        if (fnskuRows.length > 0) db.syncFnskuBatch(fnskuRows);
      }

      // INVENTORY 取得は停止済み (RESTOCK の部分集合で冗長、未使用)

      return {
        planning: planningCount,
        restock: restockCount,
        inventory: 0, // 互換性のため 0 を返す
        restockGuardSkipped,
        planningGuardSkipped,
        errors: results.errors,
      };
    } finally {
      fetchReportsJobId = null;
      // lock オブジェクト全体を渡して所有権 (ownerToken) チェックを有効化
      releaseFbaFetchLock(lock);
    }
  });

  fetchReportsJobId = job.jobId;
  okResponse(res, job, 202);
});

// ==========================================
// スナップショット（同期）
// ==========================================

router.get('/snapshots/latest', dbHandler(async (req, res, db) => {
  return { snapshots: db.getLatestSnapshots() };
}));

router.get('/snapshots/:sku', dbHandler(async (req, res, db) => {
  return { history: db.getDailySnapshots(req.params.sku) };
}));

// 従来互換: daily_snapshots と ever_seen_skus の和集合
router.get('/all-snapshot-skus', dbHandler(async (req, res, db) => {
  const legacy = db.getAllSnapshotSkus();
  const everSeen = db.getAllEverSeenSkus();
  return { skus: Array.from(new Set([...legacy, ...everSeen])) };
}));

// 新規商品判定の正: ever_seen_skus のみ
router.get('/ever-seen-skus', dbHandler(async (req, res, db) => {
  return { skus: db.getAllEverSeenSkus() };
}));

// RESTOCK最新データ
router.get('/restock-latest', dbHandler(async (req, res, db) => {
  return { rows: db.getRestockLatest() };
}));

// ==========================================
// SKUマッピング（同期）
// ==========================================

router.get('/sku-mappings', dbHandler(async (req, res, db) => {
  return { mappings: db.getSkuMappings() };
}));

router.post('/sync-sku-mappings', async (req, res) => {
  try {
    const result = await syncSkuMappings();
    okResponse(res, { result });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'SYNC_ERROR', message: e.message, requestId: req.requestId });
  }
});

router.get('/sku-exceptions', dbHandler(async (req, res, db) => {
  return { exceptions: db.getSkuExceptions() };
}));

router.post('/sku-exceptions', dbHandler(async (req, res, db) => {
  db.upsertSkuException(req.body);
  return { message: 'saved' };
}));

router.delete('/sku-exceptions/:sku', dbHandler(async (req, res, db) => {
  db.deleteSkuException(req.params.sku);
  return { message: 'deleted' };
}));

// ==========================================
// 倉庫在庫（同期）
// ==========================================

router.get('/warehouse', dbHandler(async (req, res, db) => {
  return { inventory: db.getWarehouseInventory() };
}));

router.get('/warehouse/summary', dbHandler(async (req, res, db) => {
  return { summary: db.getWarehouseSummary() };
}));

router.post('/warehouse/upload', dbHandler(async (req, res, db) => {
  const { rows } = req.body;
  if (!rows || !Array.isArray(rows)) {
    errorResponse(res, { status: 400, error: 'VALIDATION', message: 'rows required', requestId: req.requestId });
    return;
  }
  const count = db.replaceWarehouseInventory(rows);
  return { count };
}));

// ==========================================
// 推奨リスト（同期 + SP-APIキャッシュ）
// ==========================================

let inboundCache = { data: null, at: 0 };
const CACHE_TTL = 10 * 60 * 1000;

async function getCachedInbound() {
  if (inboundCache.data && (Date.now() - inboundCache.at) < CACHE_TTL) return inboundCache.data;
  const data = await fetchActiveInboundQuantities();
  inboundCache = { data, at: Date.now() };
  return data;
}

router.get('/recommendations', async (req, res) => {
  try {
    const debug = req.query.debug === '1';
    const inbound = await getCachedInbound();
    const result = generateRecommendations(debug, inbound);
    okResponse(res, result);
  } catch (e) {
    errorResponse(res, { status: 500, error: 'CALC_ERROR', message: e.message, requestId: req.requestId });
  }
});

router.get('/recommendations/:sku', async (req, res) => {
  try {
    const inbound = await getCachedInbound();
    const all = generateRecommendations(true, inbound);
    const item = all.items?.find(i => i.sku === req.params.sku);
    if (!item) return errorResponse(res, { status: 404, error: 'NOT_FOUND', message: 'SKU not found', requestId: req.requestId });
    okResponse(res, { item });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'CALC_ERROR', message: e.message, requestId: req.requestId });
  }
});

router.post('/refresh-inbound-working', rateLimitMiddleware('sp-api'), async (req, res) => {
  try {
    const data = await fetchActiveInboundQuantities();
    inboundCache = { data, at: Date.now() };
    okResponse(res, { count: Object.keys(data).length });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'SP_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// キャッシュ済みの準備中数量マップを返す（Renderの推奨計算が参照）
router.get('/recommendations-inbound-cache', async (req, res) => {
  try {
    const data = inboundCache.data || {};
    const ageMs = inboundCache.at ? Date.now() - inboundCache.at : null;
    okResponse(res, { data, count: Object.keys(data).length, cachedAt: inboundCache.at || null, ageMs });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'CACHE_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ミニPC→Render 同期用: 最新日付のPLANNINGスナップショットとFNSKU一覧（全SKU、null含む）を返す
router.get('/sync/latest-planning', dbHandler(async (req, res, db) => {
  const rows = db.getLatestSnapshots();
  const snapshotDate = rows[0]?.snapshot_date || null;
  const mappings = db.getSkuMappings();
  // 全SKU対象（fnsku=nullも含む）。Render側で現状に合わせてupsert（null時はクリア）
  const fnskus = mappings
    .filter(m => m.amazon_sku)
    .map(m => ({ sku: m.amazon_sku, fnsku: m.fnsku || null }));
  // RESTOCK / PLANNING_LATEST も同送 (Render側で saveRestockLatest / savePlanningLatest される)
  const restockRows = typeof db.getRestockLatest === 'function' ? db.getRestockLatest() : [];
  const planningLatestRows = typeof db.getPlanningLatest === 'function' ? db.getPlanningLatest() : [];
  return {
    snapshot_date: snapshotDate,
    rows,
    fnskus,
    restock_rows: restockRows,
    planning_latest_rows: planningLatestRows,
  };
}));

// ==========================================
// 納品プラン（ジョブ化）
// ==========================================

router.post('/create-inbound-plan', rateLimitMiddleware('sp-api'), (req, res) => {
  const { sourceAddress, items, planName, retryWithoutPrepOwner } = req.body;
  if (!sourceAddress || !items?.length) {
    return errorResponse(res, { status: 400, error: 'VALIDATION', message: 'sourceAddress and items required', requestId: req.requestId });
  }

  const job = createJob('fba-inbound-plan', async (updateProgress) => {
    updateProgress({ step: 'creating', skuCount: items.length });
    let result = await spCreateInboundPlan(sourceAddress, items, planName);

    if (result.status === 'FAILED' && retryWithoutPrepOwner) {
      updateProgress({ step: 'retrying-amazon-prep' });
      const retryItems = items.map(i => ({ ...i, prepOwner: 'AMAZON' }));
      result = await spCreateInboundPlan(sourceAddress, retryItems, planName ? `${planName}-retry` : undefined);
    }

    if (result.inboundPlanId && result.status === 'SUCCESS') {
      updateProgress({ step: 'saving-plan' });
      const db = await getDb();
      try {
        const shipments = await listShipments(result.inboundPlanId);
        db.createShipmentPlan({
          inbound_plan_id: result.inboundPlanId,
          plan_name: planName || '',
          status: result.status,
          shipment_count: shipments.length,
        });
      } catch (e) {
        console.error('[FBA-Service] プラン保存エラー:', e.message);
      }
    }
    return result;
  });
  okResponse(res, job, 202);
});

router.get('/plans', dbHandler(async (req, res, db) => {
  return { plans: db.getShipmentPlans() };
}));

router.get('/plans/:id/items', dbHandler(async (req, res, db) => {
  return { items: db.getShipmentPlanItems(req.params.id) };
}));

router.get('/picking-list/:planId', rateLimitMiddleware('sp-api'), async (req, res) => {
  try {
    const shipments = await listShipments(req.params.planId);
    const result = [];
    for (const s of shipments) {
      const items = await listShipmentItems(req.params.planId, s.shipmentId);
      result.push({ shipment: s, items });
    }
    okResponse(res, { shipments: result });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'SP_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// Eligibility（同期）
// ==========================================

router.get('/eligibility/check-one', rateLimitMiddleware('sp-api'), async (req, res) => {
  try {
    const { asin, msku } = req.query;
    if (!asin) return errorResponse(res, { status: 400, error: 'VALIDATION', message: 'asin required', requestId: req.requestId });
    const result = await checkInboundEligibility([{ asin, msku: msku || asin }]);
    okResponse(res, { result });
  } catch (e) {
    errorResponse(res, { status: 500, error: 'SP_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// ドラフト・仮確定（同期）
// ==========================================

router.get('/draft', dbHandler(async (req, res, db) => ({ draft: db.getDraft() })));
router.post('/draft', dbHandler(async (req, res, db) => { db.saveDraft(req.body); return { message: 'saved' }; }));
router.delete('/draft', dbHandler(async (req, res, db) => { db.clearDraft(); return { message: 'deleted' }; }));

router.get('/provisional', dbHandler(async (req, res, db) => {
  return { items: db.getProvisionalItems(), meta: null };
}));
router.post('/provisional', dbHandler(async (req, res, db) => {
  db.saveProvisionalItems(req.body.items || []);
  return { message: 'saved' };
}));
router.delete('/provisional', dbHandler(async (req, res, db) => {
  db.clearProvisionalItems();
  return { message: 'deleted' };
}));
router.patch('/provisional', dbHandler(async (req, res, db) => {
  const { sku, quantity } = req.body;
  if (quantity === undefined) {
    db.removeProvisionalItem(sku);
  } else {
    db.updateProvisionalItemQty(sku, quantity);
  }
  return { message: 'updated' };
}));

// ==========================================
// 出力（同期）
// ==========================================

router.get('/export-history', dbHandler(async (req, res, db) => {
  return { history: db.getExportHistoryList() };
}));

router.get('/export-history/:id/download', async (req, res) => {
  try {
    const db = await getDb();
    const file = db.getExportHistoryFile(req.params.id);
    if (!file) return errorResponse(res, { status: 404, error: 'NOT_FOUND', message: 'File not found', requestId: req.requestId });
    res.setHeader('Content-Type', file.content_type || 'application/octet-stream');
    res.setHeader('Content-Disposition', `attachment; filename="${file.filename}"`);
    res.send(file.data);
  } catch (e) {
    errorResponse(res, { status: 500, error: 'DB_ERROR', message: e.message, requestId: req.requestId });
  }
});

// export-manifest と export-ne-csv は既存router.jsのロジックが複雑（ExcelJS, iconv等）
// → Phase 2の後半で移植。当面はRender側で直接処理を継続。

// ==========================================
// 非表示管理（同期）
// ==========================================

router.get('/new-product-hidden', dbHandler(async (req, res, db) => ({ items: db.getNewProductHidden() })));
router.post('/new-product-hidden', dbHandler(async (req, res, db) => { db.hideNewProductSkuBulk([req.body.sku]); return { message: 'added' }; }));
router.delete('/new-product-hidden/:sku', dbHandler(async (req, res, db) => { db.unhideNewProductSku(req.params.sku); return { message: 'removed' }; }));

router.get('/stockout-hidden', dbHandler(async (req, res, db) => ({ items: db.getStockoutHidden() })));
router.post('/stockout-hidden', dbHandler(async (req, res, db) => { db.hideStockoutSku(req.body.sku); return { message: 'added' }; }));
router.delete('/stockout-hidden/:sku', dbHandler(async (req, res, db) => { db.unhideStockoutSku(req.params.sku); return { message: 'removed' }; }));

// ==========================================
// 設定（同期）
// ==========================================

router.get('/settings', dbHandler(async (req, res, db) => ({ settings: db.getSettings() })));
router.post('/settings', dbHandler(async (req, res, db) => {
  for (const [key, value] of Object.entries(req.body)) {
    db.updateSetting(key, value);
  }
  return { message: 'saved' };
}));

router.get('/status', dbHandler(async (req, res, db) => {
  const snapshots = db.getLatestSnapshots();
  const mappings = db.getSkuMappings();
  const warehouse = db.getWarehouseSummary();
  return {
    snapshotCount: snapshots.length,
    mappingCount: mappings.length,
    latestSnapshotDate: snapshots[0]?.snapshot_date || null,
    warehouseProducts: warehouse?.length || 0,
    fetchInProgress: !!fetchReportsJobId,
    inboundCacheAge: inboundCache.at ? Math.floor((Date.now() - inboundCache.at) / 1000) + 's ago' : 'not cached',
  };
}));

export default router;
