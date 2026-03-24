/**
 * FBA在庫補充システム — ルーター (Phase 1a)
 */
import express from 'express';
import multer from 'multer';
import { initDb, savePlanningData, getLatestSnapshots, getSettings, updateSetting,
         getSkuMappings, getSkuExceptions, upsertSkuException, deleteSkuException,
         getWarehouseInventory, replaceWarehouseInventory,
         getShipmentPlans, getShipmentPlanItems, getDailySnapshots } from './db.js';
import { fetchAllReports, fetchPlanningData, normalizePlanningRow } from './sp-api-reports.js';

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

// DB初期化
let dbReady = false;
initDb().then(() => { dbReady = true; }).catch(e => console.error('[FBA] DB初期化エラー:', e));

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

// PLANNINGデータのみ取得
router.post('/api/fetch-planning', async (req, res) => {
  if (fetchInProgress) return res.status(409).json({ error: 'レポート取得中です' });

  fetchInProgress = true;
  try {
    const rows = await fetchPlanningData();
    const normalized = rows.map(normalizePlanningRow);
    const saved = savePlanningData(normalized);
    res.json({ success: true, count: rows.length, saved });
  } catch (e) {
    console.error('[FBA] PLANNINGデータ取得エラー:', e);
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

      items.push({
        logizard_code: obj['商品コード'] || obj['品番'] || cols[0] || '',
        product_name: obj['商品名'] || cols[1] || '',
        location: obj['ロケーション'] || obj['棚番'] || '',
        quantity: parseInt(obj['数量'] || obj['在庫数'] || 0),
        expiry_date: obj['期限'] || obj['賞味期限'] || '',
        lot_no: obj['ロット'] || obj['ロットNo'] || '',
        is_y_location: (obj['ロケーション'] || '').startsWith('Y') ? 1 : 0,
      });
    }

    const count = replaceWarehouseInventory(items);
    res.json({ success: true, count });
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
router.get('/api/status', (req, res) => {
  const snapshots = getLatestSnapshots();
  res.json({
    dbReady,
    fetchInProgress,
    latestSnapshotDate: snapshots[0]?.snapshot_date || null,
    snapshotCount: snapshots.length,
  });
});

export default router;
