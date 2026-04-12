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
import { fetchAllReports, normalizePlanningRow } from '../fba-replenishment/sp-api-reports.js';
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
  // 二重実行防止: 実行中のジョブがあればそのjobIdを返す
  if (fetchReportsJobId) {
    const { getJob } = await import('./job-manager.js');
    const existing = getJob(fetchReportsJobId);
    if (existing && existing.status === 'running') {
      return okResponse(res, { jobId: fetchReportsJobId, status: 'already_running', message: 'レポート取得が既に実行中です' }, 202);
    }
    // 完了済みならリセット
    fetchReportsJobId = null;
  }

  const job = createJob('fba-fetch-reports', async (updateProgress) => {
    try {
      updateProgress({ step: 'starting', message: 'SP-APIレポート3種を並列取得中...' });
      const results = await fetchAllReports();
      const db = await getDb();

      let planningCount = 0, restockCount = 0, inventoryCount = 0;

      if (results.planning?.length > 0) {
        updateProgress({ step: 'saving-planning', count: results.planning.length });
        const normalized = results.planning.map(normalizePlanningRow);
        db.savePlanningData(normalized);
        planningCount = normalized.length;
        const fnskuRows = results.planning
          .filter(r => r['fnsku'] && r['sku'])
          .map(r => ({ sku: r['sku'], fnsku: r['fnsku'] }));
        if (fnskuRows.length > 0) db.updateFnskuBatch(fnskuRows);
      }
      if (results.restock?.length > 0) {
        updateProgress({ step: 'saving-restock', count: results.restock.length });
        restockCount = results.restock.length;
      }
      if (results.inventory?.length > 0) {
        updateProgress({ step: 'saving-inventory', count: results.inventory.length });
        inventoryCount = results.inventory.length;
      }

      return { planning: planningCount, restock: restockCount, inventory: inventoryCount, errors: results.errors };
    } finally {
      fetchReportsJobId = null;
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

router.get('/all-snapshot-skus', dbHandler(async (req, res, db) => {
  return { skus: db.getAllSnapshotSkus() };
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
