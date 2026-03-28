/**
 * FBA在庫補充システム — ルーター
 */
import express from 'express';
import multer from 'multer';
import cron from 'node-cron';
import { initDb, savePlanningData, getLatestSnapshots, getSettings, updateSetting,
         getSkuMappings, getSkuExceptions, upsertSkuException, deleteSkuException,
         getWarehouseInventory, replaceWarehouseInventory, getWarehouseSummary,
         getShipmentPlans, getShipmentPlanItems, getDailySnapshots,
         getStockoutHidden, hideStockoutSku, unhideStockoutSku, hideStockoutSkuBulk } from './db.js';
import { fetchAllReports, normalizePlanningRow } from './sp-api-reports.js';
import { syncSkuMappings } from './sheets-sync.js';
import { generateRecommendations } from './calculation-engine.js';

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

// DB初期化
let dbReady = false;
initDb().then(() => {
  dbReady = true;

  // 毎日06:00 JST (21:00 UTC) にSKUマッピング同期（他CH売上スナップショット蓄積）
  cron.schedule('0 21 * * *', async () => {
    console.log('[FBA-Cron] SKUマッピング定期同期開始...');
    try {
      const result = await syncSkuMappings();
      console.log(`[FBA-Cron] 完了: ${result.total}件 (スナップショット: ${result.snapshots}件)`);
    } catch (e) {
      console.error('[FBA-Cron] SKUマッピング同期エラー:', e);
    }
  });
  console.log('[FBA] 定期同期スケジュール設定: 毎日06:00 JST');
}).catch(e => console.error('[FBA] DB初期化エラー:', e));

function ensureDb(req, res, next) {
  if (!dbReady) return res.status(503).json({ error: 'DB初期化中' });
  next();
}

router.use(ensureDb);

// ===== メイン画面 =====
router.get('/', (req, res) => {
  res.render('fba-replenishment', {
    title: 'FBA在庫補充',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ===== SP-APIレポート取得 =====

// 全レポート取得（PLANNING + RESTOCK + INVENTORY）
let fetchInProgress = false;
router.post('/api/fetch-reports', async (req, res) => {
  if (fetchInProgress) return res.status(409).json({ error: 'レポート取得中です。しばらくお待ちください。' });

  fetchInProgress = true;
  try {
    const results = await fetchAllReports();

    // PLANNINGデータをDBに保存
    let savedCount = 0;
    if (results.planning && results.planning.length > 0) {
      const normalized = results.planning.map(normalizePlanningRow);
      savedCount = savePlanningData(normalized);
    }

    res.json({
      success: true,
      planning: { count: results.planning?.length || 0, saved: savedCount },
      restock: { count: results.restock?.length || 0 },
      inventory: { count: results.inventory?.length || 0 },
      errors: results.errors,
    });
  } catch (e) {
    console.error('[FBA] レポート取得エラー:', e);
    res.status(500).json({ error: e.message });
  } finally {
    fetchInProgress = false;
  }
});

// ===== スナップショット（最新データ閲覧） =====
router.get('/api/snapshots/latest', (req, res) => {
  const snapshots = getLatestSnapshots();
  res.json({ count: snapshots.length, data: snapshots });
});

router.get('/api/snapshots/:sku', (req, res) => {
  const days = parseInt(req.query.days) || 30;
  const data = getDailySnapshots(req.params.sku, days);
  res.json({ sku: req.params.sku, count: data.length, data });
});

// ===== SKUマッピング =====
router.get('/api/sku-mappings', (req, res) => {
  res.json(getSkuMappings());
});

// ===== スプレッドシート同期 =====
router.post('/api/sync-sku-mappings', async (req, res) => {
  try {
    const result = await syncSkuMappings();
    res.json({ success: true, ...result });
  } catch (e) {
    console.error('[FBA] SKUマッピング同期エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== SKU例外マスタ =====
router.get('/api/sku-exceptions', (req, res) => {
  res.json(getSkuExceptions());
});

router.post('/api/sku-exceptions', express.json(), (req, res) => {
  const { amazon_sku, exception_type, keep_minimum_qty, reason } = req.body;
  if (!amazon_sku || !exception_type) return res.status(400).json({ error: 'amazon_sku, exception_type 必須' });
  upsertSkuException(amazon_sku, exception_type, keep_minimum_qty, reason);
  res.json({ success: true });
});

router.delete('/api/sku-exceptions/:sku', (req, res) => {
  deleteSkuException(req.params.sku);
  res.json({ success: true });
});

// ===== 自社倉庫在庫（ロジザードCSV） =====
router.get('/api/warehouse', (req, res) => {
  res.json(getWarehouseInventory());
});

router.get('/api/warehouse/summary', (req, res) => {
  res.json(getWarehouseSummary());
});

router.post('/api/warehouse/upload', upload.single('csv'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'CSVファイルが必要です' });

  try {
    // Shift_JIS対応
    const iconv = (await import('iconv-lite')).default;
    let text;
    try {
      text = iconv.decode(req.file.buffer, 'Shift_JIS');
    } catch {
      text = req.file.buffer.toString('utf-8');
    }

    const lines = text.split('\n').filter(l => l.trim());
    if (lines.length < 2) return res.status(400).json({ error: 'CSVが空です' });

    const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
    const items = [];

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',').map(c => c.trim().replace(/"/g, ''));
      const obj = {};
      headers.forEach((h, j) => { obj[h] = cols[j] || ''; });

      // ロジザードCSV実データ列名に対応
      const block = obj['ブロック略称'] || '';
      const location = obj['ロケ'] || obj['ロケーション'] || '';
      const qty = parseInt(obj['在庫数(引当数を含む)'] || obj['在庫数'] || 0);
      const reserved = parseInt(obj['引当数'] || 0);

      // 最終入荷日: YYYYMMDD → YYYY-MM-DD に正規化
      const rawNyuka = obj['最終入荷日'] || '';
      const lastArrivalDate = rawNyuka.length === 8
        ? `${rawNyuka.slice(0,4)}-${rawNyuka.slice(4,6)}-${rawNyuka.slice(6,8)}`
        : rawNyuka;

      items.push({
        logizard_code: obj['商品ID'] || obj['商品コード'] || '',
        product_name: obj['商品名'] || '',
        location: location,
        block: block,
        quantity: qty,
        reserved: reserved,
        available_qty: qty - reserved,
        expiry_date: obj['有効期限'] || '',
        lot_no: obj['ロット'] || '',
        barcode: obj['バーコード'] || '',
        is_y_location: (block === 'YYY' || location.toUpperCase().startsWith('Y')) ? 1 : 0,
        last_arrival_date: lastArrivalDate,
        location_biz_type: obj['ロケ業務区分'] || '',
        block_alloc_order: parseInt(obj['ブロック引当順'] || 9999),
        biz_priority: obj['指定した業態を優先して取り置く'] || '',
      });
    }

    const count = replaceWarehouseInventory(items);

    // 集計情報を返す
    const uniqueProducts = new Set(items.map(i => i.logizard_code)).size;
    const yItems = items.filter(i => i.is_y_location);
    const totalQty = items.reduce((s, i) => s + i.quantity, 0);
    const yQty = yItems.reduce((s, i) => s + i.quantity, 0);

    res.json({
      success: true,
      count,
      summary: {
        uniqueProducts,
        totalQty,
        yLocationProducts: new Set(yItems.map(i => i.logizard_code)).size,
        yLocationQty: yQty,
      }
    });
  } catch (e) {
    console.error('[FBA] CSVアップロードエラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== 設定 =====
router.get('/api/settings', (req, res) => {
  res.json(getSettings());
});

router.post('/api/settings', express.json(), (req, res) => {
  const updates = req.body;
  if (!updates || typeof updates !== 'object') return res.status(400).json({ error: '設定値が必要です' });
  for (const [key, value] of Object.entries(updates)) {
    updateSetting(key, String(value));
  }
  res.json({ success: true });
});

// ===== 納品計画履歴 =====
router.get('/api/plans', (req, res) => {
  const limit = parseInt(req.query.limit) || 30;
  res.json(getShipmentPlans(limit));
});

router.get('/api/plans/:id/items', (req, res) => {
  const items = getShipmentPlanItems(parseInt(req.params.id));
  res.json(items);
});

// ===== ステータス =====
// ===== 推奨リスト =====
router.get('/api/recommendations', (req, res) => {
  try {
    const debug = req.query.debug === '1' || req.query.debug === 'true';
    const result = generateRecommendations(debug);
    res.json(result);
  } catch (e) {
    console.error('[FBA] 推奨リスト生成エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// 個別SKUの計算詳細
router.get('/api/recommendations/:sku', (req, res) => {
  try {
    const result = generateRecommendations(true);
    const item = result.items.find(i => i.amazon_sku === req.params.sku);
    if (!item) return res.status(404).json({ error: 'SKUが見つかりません' });
    res.json(item);
  } catch (e) {
    console.error('[FBA] SKU詳細エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== FBA欠品 非表示管理 =====
router.get('/api/stockout-hidden', (req, res) => {
  res.json(getStockoutHidden());
});

router.post('/api/stockout-hidden', express.json(), (req, res) => {
  const { skus, reason } = req.body;
  if (Array.isArray(skus) && skus.length > 0) {
    const count = hideStockoutSkuBulk(skus, reason);
    return res.json({ success: true, count });
  }
  const { amazon_sku } = req.body;
  if (!amazon_sku) return res.status(400).json({ error: 'amazon_sku または skus[] が必要です' });
  hideStockoutSku(amazon_sku, reason);
  res.json({ success: true });
});

router.delete('/api/stockout-hidden/:sku', (req, res) => {
  unhideStockoutSku(req.params.sku);
  res.json({ success: true });
});

// ===== ステータス =====
router.get('/api/status', (req, res) => {
  const snapshots = getLatestSnapshots();
  const mappings = getSkuMappings();
  const warehouse = getWarehouseInventory();
  const warehouseProducts = new Set(warehouse.map(w => w.logizard_code)).size;
  res.json({
    dbReady,
    fetchInProgress,
    latestSnapshotDate: snapshots[0]?.snapshot_date || null,
    snapshotCount: snapshots.length,
    mappingCount: mappings.length,
    warehouseProducts,
    warehouseRows: warehouse.length,
  });
});

export default router;
