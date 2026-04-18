/**
 * FBA在庫補充システム — ルーター
 */
import express from 'express';
import multer from 'multer';
import cron from 'node-cron';
import { initDb, savePlanningData, savePlanningDataWithHistory, getLatestSnapshots, getAllSnapshotSkus, getSettings, updateSetting,
         getSkuMappings, getSkuExceptions, upsertSkuException, deleteSkuException,
         getWarehouseInventory, replaceWarehouseInventory, getWarehouseSummary,
         getShipmentPlans, getShipmentPlanItems, getDailySnapshots,
         getStockoutHidden, hideStockoutSku, unhideStockoutSku, hideStockoutSkuBulk,
         getNewProductHidden, hideNewProductSkuBulk, unhideNewProductSku,
         saveDraft, getDraft, clearDraft, updateFnskuBatch, syncFnskuBatch,
         saveProvisionalItems, mergeProvisionalItems, getProvisionalItems, clearProvisionalItems,
         updateProvisionalItemQty, removeProvisionalItem,
         saveExportHistory, getExportHistoryList, getExportHistoryFile,
         getRestockLatest, getPlanningLatestMap, getAllEverSeenSkus,
         saveRestockLatest, savePlanningLatest } from './db.js';
// SP-API関連はミニPC経由で実行（APIキーはミニPC側に一元管理）
// import { fetchAllReports, normalizePlanningRow } from './sp-api-reports.js';
// import { createInboundPlan, checkInboundEligibility, findErrorSkusByBinarySearch, listShipments, listShipmentItems, fetchActiveInboundQuantities } from './inbound-plans.js';
import { syncSkuMappings } from './sheets-sync.js';
import { generateRecommendations } from './calculation-engine.js';
import { normalizePlanningRow } from './sp-api-reports.js';

// --- ミニPC接続（SP-API実行用） ---
const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}
// ミニPCへのサービスAPI呼び出し。
// - HTML/認証リダイレクト/upstream障害を区別したエラーメッセージを生成
// - GETはネットワーク系/5xxで指数バックオフ+ジッタでリトライ (最大3回)
// - POSTは副作用を避けるため自動リトライなし (冪等化できたら retry オプションで有効化可)
async function callMiniPC(path, { method = 'GET', body, timeout = 60000, retry } = {}) {
  const url = `${WAREHOUSE_URL}/service-api/fba${path}`;
  const requestId = `r-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const headers = { ...getServiceHeaders(), 'x-request-id': requestId };
  const maxAttempts = retry ?? (method === 'GET' ? 3 : 1);

  let lastError;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const options = { method, headers, redirect: 'manual', signal: AbortSignal.timeout(timeout) };
      if (body) options.body = JSON.stringify(body);
      const res = await fetch(url, options);
      const ct = res.headers.get('content-type') || '';

      if (res.status === 302 || res.status === 303) {
        const loc = res.headers.get('location') || '';
        throw new Error(`CF Access認証構成異常 (${res.status} → ${loc}) req=${requestId}`);
      }
      if (res.status === 401 || res.status === 403) {
        throw new Error(`認証失敗 HTTP ${res.status} req=${requestId}`);
      }
      if ([502, 503, 504].includes(res.status)) {
        lastError = new Error(`upstream障害 HTTP ${res.status} (CF tunnel/warehouse側) req=${requestId}`);
        if (attempt < maxAttempts) {
          await new Promise(r => setTimeout(r, Math.min(500 * 2 ** (attempt - 1), 4000) + Math.random() * 300));
          continue;
        }
        throw lastError;
      }
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`ミニPC HTTP ${res.status}: ${txt.slice(0, 200)} req=${requestId}`);
      }
      if (!ct.includes('application/json')) {
        const txt = await res.text().catch(() => '');
        throw new Error(`レスポンス形式異常 (ct=${ct || 'none'}): ${txt.slice(0, 200)} req=${requestId}`);
      }
      return await res.json();
    } catch (e) {
      const msg = e?.message || String(e);
      const isRetryable = e?.name === 'TimeoutError' || /aborted|timeout|ECONNREFUSED|ENOTFOUND|fetch failed|upstream障害/i.test(msg);
      if (isRetryable && attempt < maxAttempts) {
        lastError = e;
        await new Promise(r => setTimeout(r, Math.min(500 * 2 ** (attempt - 1), 4000) + Math.random() * 300));
        continue;
      }
      throw e;
    }
  }
  throw lastError || new Error('callMiniPC: unknown error');
}

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

let fetchInProgress = false; // ステータス表示用フラグ
// 全レポート取得 → ミニPC経由でSP-APIを実行（ジョブ化）
router.post('/api/fetch-reports', async (req, res) => {
  try {
    const result = await callMiniPC('/fetch-reports', { method: 'POST' });
    res.json(result);
  } catch (e) {
    console.error('[FBA] レポート取得エラー:', e);
    res.status(502).json({ error: 'ミニPCへの接続に失敗: ' + e.message });
  }
});

// ミニPCから最新PLANNINGスナップショットをRender DBへ同期
// （フロントが /api/fetch-reports のジョブ完了後に呼ぶ）
router.post('/api/sync-latest-planning', async (req, res) => {
  try {
    const pull = await callMiniPC('/sync/latest-planning', { timeout: 60000 });
    if (!pull?.ok) {
      return res.status(502).json({ error: 'ミニPCからの同期データ取得に失敗', detail: pull });
    }
    const rows = pull.rows || [];
    const fnskus = pull.fnskus || [];
    const snapshotDate = pull.snapshot_date;

    // 空結果ガード: ミニPC側のジョブは成功したが同期対象データが0件 = 実質失敗
    if (rows.length === 0 || !snapshotDate) {
      console.error('[FBA] 同期: 空のスナップショット（rowsなし or snapshot_dateなし）');
      return res.status(502).json({
        error: '同期データが空です。ミニPC側のSP-API取得が失敗している可能性があります。',
        detail: { rowCount: rows.length, snapshotDate },
      });
    }

    const savedRows = savePlanningDataWithHistory(rows, snapshotDate);
    let savedFnskus = 0;
    if (fnskus.length > 0) {
      // syncFnskuBatch は null も反映（FNSKUが外された商品を正しく同期）
      syncFnskuBatch(fnskus);
      savedFnskus = fnskus.length;
    }

    // RESTOCK / PLANNING_LATEST も同期 (ミニPCから送られてくる)
    let savedRestock = 0, savedPlanningLatest = 0;
    let restockSkipReason = null, planningLatestSkipReason = null;
    const restockRows = pull.restock_rows || [];
    const planningLatestRows = pull.planning_latest_rows || [];
    if (restockRows.length > 0) {
      try {
        const r = saveRestockLatest(restockRows);
        savedRestock = r.saved;
        if (r.skipped) restockSkipReason = r.reason;
      } catch (e) {
        console.error('[FBA] saveRestockLatest failed:', e.message);
      }
    }
    if (planningLatestRows.length > 0) {
      try {
        // planning_latest_rows は DB 形式なので amazon_sku を sku にマップ
        const normalized = planningLatestRows.map(r => ({ ...r, sku: r.amazon_sku }));
        const r = savePlanningLatest(normalized);
        savedPlanningLatest = r.saved;
        if (r.skipped) planningLatestSkipReason = r.reason;
      } catch (e) {
        console.error('[FBA] savePlanningLatest failed:', e.message);
      }
    }

    console.log(`[FBA] Render DB同期完了: ${savedRows}件 / FNSKU: ${savedFnskus}件 / RESTOCK: ${savedRestock}件 / PLANNING_LATEST: ${savedPlanningLatest}件 / 日付: ${snapshotDate}`);
    res.json({
      ok: true,
      rows: savedRows,
      fnskus: savedFnskus,
      restock: savedRestock,
      restock_skip_reason: restockSkipReason,
      planning_latest: savedPlanningLatest,
      planning_latest_skip_reason: planningLatestSkipReason,
      snapshot_date: snapshotDate,
    });
  } catch (e) {
    console.error('[FBA] 同期エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ジョブ状態確認（ミニPC側のジョブマネージャー）
router.get('/api/jobs/:jobId', async (req, res) => {
  try {
    const url = `${WAREHOUSE_URL}/service-api/jobs/${req.params.jobId}`;
    const response = await fetch(url, { headers: getServiceHeaders(), signal: AbortSignal.timeout(15000) });
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (e) {
    res.status(502).json({ error: 'ジョブ状態の取得に失敗', detail: e.message });
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

// ===== 全期間スナップショットSKU一覧 =====
// 既存互換: daily_snapshots と ever_seen_skus の和集合を返す
// (新規商品タブ判定で「過去にFBAで見たことがあるSKU」全てを対象にするため)
router.get('/api/all-snapshot-skus', (req, res) => {
  const legacy = getAllSnapshotSkus();
  const everSeen = getAllEverSeenSkus();
  const union = Array.from(new Set([...legacy, ...everSeen]));
  res.json(union);
});

// ===== 過去FBA観測SKU一覧 (Phase1+で蓄積、新規商品判定の正) =====
router.get('/api/ever-seen-skus', (req, res) => {
  res.json(getAllEverSeenSkus());
});

// ===== RESTOCK最新データ一覧 =====
router.get('/api/restock-latest', (req, res) => {
  const rows = getRestockLatest();
  res.json({ count: rows.length, data: rows });
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

// ===== 準備中数量キャッシュ（listInboundPlansから取得） =====
let inboundWorkingCache = null;
let inboundWorkingCacheTime = 0;
const INBOUND_CACHE_TTL = 10 * 60 * 1000; // 10分

async function getInboundWorkingData() {
  const now = Date.now();
  if (inboundWorkingCache && (now - inboundWorkingCacheTime) < INBOUND_CACHE_TTL) {
    return inboundWorkingCache;
  }
  try {
    // ミニPC経由でSP-APIからACTIVEプラン数量を取得
    const result = await callMiniPC('/refresh-inbound-working', { method: 'POST', timeout: 60000 });
    if (result.ok && result.count !== undefined) {
      // ミニPC側でキャッシュされているので、改めてデータを取得
      const dataResult = await callMiniPC('/recommendations-inbound-cache', { timeout: 15000 }).catch(() => null);
      // キャッシュが取れない場合は空オブジェクトで進める（推奨リスト自体は動く）
      inboundWorkingCache = dataResult?.data || {};
    } else {
      inboundWorkingCache = {};
    }
    inboundWorkingCacheTime = now;
    console.log(`[FBA] 準備中数量キャッシュ更新: ${Object.keys(inboundWorkingCache).length} SKU`);
    return inboundWorkingCache;
  } catch (e) {
    console.error('[FBA] 準備中数量取得エラー（キャッシュを使用）:', e.message);
    return inboundWorkingCache || {};
  }
}

// 手動リフレッシュ用
router.post('/api/refresh-inbound-working', async (req, res) => {
  try {
    inboundWorkingCache = null;
    const data = await getInboundWorkingData();
    res.json({ ok: true, skuCount: data ? Object.keys(data).length : 0 });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ===== ステータス =====
// ===== 推奨リスト =====
router.get('/api/recommendations', async (req, res) => {
  try {
    const debug = req.query.debug === '1' || req.query.debug === 'true';
    const inboundOverride = await getInboundWorkingData();
    const result = generateRecommendations(debug, inboundOverride);
    // FNSKU情報を付与
    const mappings = getSkuMappings();
    const fnskuMap = {};
    for (const m of mappings) if (m.fnsku) fnskuMap[m.amazon_sku] = m.fnsku;
    const fnskuCount = Object.keys(fnskuMap).length;
    console.log(`[FBA] 推奨API: mappings=${mappings.length}, fnsku有り=${fnskuCount}, inboundOverride=${inboundOverride ? Object.keys(inboundOverride).length + ' SKU' : 'なし'}`);
    if (fnskuCount > 0) {
      const sample = Object.entries(fnskuMap).slice(0, 3);
      console.log(`[FBA] FNSKUサンプル:`, sample);
    }
    for (const item of result.items) {
      item.fnsku = fnskuMap[item.amazon_sku] || '';
    }
    const itemsWithFnsku = result.items.filter(i => i.fnsku).length;
    console.log(`[FBA] 推奨items: ${result.items.length}件, fnsku付与=${itemsWithFnsku}件`);
    res.json(result);
  } catch (e) {
    console.error('[FBA] 推奨リスト生成エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// 個別SKUの計算詳細
router.get('/api/recommendations/:sku', async (req, res) => {
  try {
    const inboundOverride = await getInboundWorkingData();
    const result = generateRecommendations(true, inboundOverride);
    const item = result.items.find(i => i.amazon_sku === req.params.sku);
    if (!item) return res.status(404).json({ error: 'SKUが見つかりません' });
    res.json(item);
  } catch (e) {
    console.error('[FBA] SKU詳細エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== 診断: Eligibility API テスト =====
router.get('/api/debug/eligibility/:asin', async (req, res) => {
  try {
    const items = [{ asin: req.params.asin, msku: 'TEST' }];
    const miniResult = await callMiniPC(`/eligibility/check-one?asin=${encodeURIComponent(req.params.asin)}&msku=TEST`, { timeout: 15000 });
    res.json({ asin: req.params.asin, result: miniResult.result || miniResult, raw: 'via miniPC' });
  } catch (e) {
    res.json({ asin: req.params.asin, error: e.message, stack: e.stack?.split('\n').slice(0, 5) });
  }
});

// ===== 納品プラン作成 =====
let inboundPlanInProgress = false;
router.post('/api/create-inbound-plan', express.json(), async (req, res) => {
  if (inboundPlanInProgress) return res.status(409).json({ error: '納品プラン作成中です。しばらくお待ちください。' });

  const { items, planName } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });

  // 設定から住所・ラベル設定を取得
  const settings = getSettings();
  const sourceAddress = {
    name: settings.inbound_ship_from_name || '',
    addressLine1: settings.inbound_ship_from_address1 || '',
    addressLine2: settings.inbound_ship_from_address2 || '',
    city: settings.inbound_ship_from_city || '',
    stateOrProvinceCode: settings.inbound_ship_from_state || '',
    postalCode: settings.inbound_ship_from_postal_code || '',
    countryCode: settings.inbound_ship_from_country || 'JP',
    phoneNumber: settings.inbound_ship_from_phone || '',
  };

  // 住所チェック
  if (!sourceAddress.name || !sourceAddress.addressLine1 || !sourceAddress.postalCode || !sourceAddress.phoneNumber) {
    return res.status(400).json({ error: '送り元住所または電話番号が未設定です。設定画面で入力してください。' });
  }

  const labelOwner = settings.inbound_label_owner || 'AMAZON';
  const prepOwner = settings.inbound_prep_owner || 'NONE';

  // 有効期限フォーマット変換: YYYYMMDD or YYYY/MM/DD → YYYY-MM-DD
  function formatExpiration(raw) {
    if (!raw) return null;
    const s = raw.replace(/\//g, '').replace(/-/g, '');
    if (s.length === 8) return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    return null; // フォーマット不明は除外
  }

  // prepOwnerの自動判定:
  // まずNONEで試す → "requires prepOwner"エラーのSKUはSELLERに変えてリトライ
  // まずSELLERで試す → "does not require prepOwner"エラーのSKUはNONEに変えてリトライ
  // → 両方混在するので、エラーメッセージからSKUごとに判定して1回のリトライで解決する

  function buildApiItems(itemList, prepOverrides = {}) {
    return itemList.map(i => {
      const exp = formatExpiration(i.expiry_date);
      const skuPrepOwner = prepOverrides[i.amazon_sku] || prepOwner;
      return {
        msku: i.amazon_sku,
        quantity: i.ship_qty,
        labelOwner,
        prepOwner: skuPrepOwner,
        ...(exp ? { expiration: exp } : {}),
      };
    });
  }

  // prepOwnerエラーを解析してSKUごとの正しい値を返す
  function parsePrepErrors(errorMessage) {
    const overrides = {};
    // "SKU requires prepOwner but NONE was assigned" → SELLER
    const requiresPattern = /(\S+)\s+requires prepOwner but NONE was assigned/g;
    let match;
    while ((match = requiresPattern.exec(errorMessage)) !== null) {
      overrides[match[1]] = 'SELLER';
    }
    // "SKU does not require prepOwner but SELLER was assigned" → NONE
    const notRequiresPattern = /(\S+)\s+does not require prepOwner but SELLER was assigned/g;
    while ((match = notRequiresPattern.exec(errorMessage)) !== null) {
      overrides[match[1]] = 'NONE';
    }
    return overrides;
  }

  // prepOwnerを自動判定しながらリトライ（APIが1件ずつしかエラーを返さないため回数多め）
  async function attemptWithPrepRetry(itemList, maxRetries = 30) {
    const allPrepOverrides = {}; // SKUごとのprepOwner修正を蓄積
    let lastError = null;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const apiItems = buildApiItems(itemList, allPrepOverrides);
      try {
        const result = await callMiniPC('/create-inbound-plan', { method: 'POST', body: { sourceAddress, items: apiItems, planName }, timeout: 180000 }).then(r => r.ok ? (r.result || r) : r);

        // ポーリング結果のエラーチェック
        if (result.status === 'FAILED' && result.problems && result.problems.length > 0) {
          const errorMsg = result.problems.map(p => p.message || '').join(' ');
          const newOverrides = parsePrepErrors(errorMsg);
          if (Object.keys(newOverrides).length > 0 && attempt < maxRetries - 1) {
            Object.assign(allPrepOverrides, newOverrides);
            console.log(`[Inbound] 試行${attempt + 1}: prepOwnerエラー${Object.keys(newOverrides).length}件検出、リトライ...`);
            continue;
          }
        }
        return { result, prepOverrides: allPrepOverrides };

      } catch (e) {
        // バリデーション例外としてprepOwnerエラーが飛ぶ場合
        const errorMsg = e.message || '';
        const newOverrides = parsePrepErrors(errorMsg);
        if (Object.keys(newOverrides).length > 0 && attempt < maxRetries - 1) {
          Object.assign(allPrepOverrides, newOverrides);
          console.log(`[Inbound] 試行${attempt + 1}: 例外からprepOwnerエラー${Object.keys(newOverrides).length}件検出、リトライ...`);
          lastError = e;
          continue;
        }
        throw e;
      }
    }
    throw lastError || new Error('リトライ上限に達しました');
  }

  inboundPlanInProgress = true;
  try {
    // デバッグ: 受信データのexpiry_dateを確認
    const itemsWithExpiry = items.filter(i => i.expiry_date);
    console.log(`[Inbound] 受信items: ${items.length}件, expiry_date有り: ${itemsWithExpiry.length}件`);
    if (itemsWithExpiry.length > 0) {
      console.log(`[Inbound] expiry_dateサンプル:`, itemsWithExpiry.slice(0, 3).map(i => ({ sku: i.amazon_sku, expiry: i.expiry_date })));
    } else if (items.length > 0) {
      console.log(`[Inbound] items[0]のキー:`, Object.keys(items[0]));
    }
    const { result, prepOverrides } = await attemptWithPrepRetry(items);

    // 修正情報を生成
    const prepCorrections = Object.entries(prepOverrides).map(([sku, newVal]) => {
      const item = items.find(i => i.amazon_sku === sku);
      return {
        sku,
        product_name: item?.product_name || sku,
        original: newVal === 'SELLER' ? 'NONE' : 'SELLER',
        corrected: newVal,
        reason: newVal === 'SELLER' ? 'prep（梱包準備）が必要な商品' : 'prep不要な商品',
      };
    });

    // planItemsからエラーSKUを特定（送信したSKUとプランに残ったSKUの差分 = エラーSKU）
    const planItems = result.planItems || [];
    const planSkuSet = new Set(planItems.map(pi => pi.msku));
    const sentSkuSet = new Set(items.map(i => i.amazon_sku));
    // プランに入らなかったSKU = エラーで弾かれたSKU
    const rejectedSkus = items.filter(i => !planSkuSet.has(i.amazon_sku));

    let enrichedProblems;
    if (rejectedSkus.length > 0 && (result.problems || []).some(p => !(p.msku || p.sku))) {
      // APIのproblemsにSKU情報がない場合、rejectedSkusで補完
      // エラー数と弾かれたSKU数が一致すれば1対1で対応
      if (rejectedSkus.length === (result.problems || []).length) {
        enrichedProblems = (result.problems || []).map((p, i) => ({
          ...p,
          msku: rejectedSkus[i].amazon_sku,
          product_name: rejectedSkus[i].product_name || '',
        }));
      } else {
        // 数が合わない場合: problemsにrejectedSkus情報を追加
        enrichedProblems = rejectedSkus.map(rej => {
          const matchingProblem = (result.problems || [])[0] || {};
          return {
            ...matchingProblem,
            msku: rej.amazon_sku,
            product_name: rej.product_name || '',
          };
        });
      }
    } else {
      // APIにSKU情報がある場合 or planItems取得失敗
      enrichedProblems = (result.problems || []).map(p => {
        const allText = [p.message, p.details, p.code].filter(Boolean).join(' ');
        let matchedSku = p.msku || p.sku || null;
        if (!matchedSku) {
          for (const item of items) {
            if (allText.includes(item.amazon_sku)) { matchedSku = item.amazon_sku; break; }
          }
        }
        const matchedItem = matchedSku ? items.find(i => i.amazon_sku === matchedSku) : null;
        return { ...p, msku: matchedSku || '-', product_name: matchedItem?.product_name || '' };
      });
    }

    console.log(`[Inbound] 送信${sentSkuSet.size}件, プラン内${planSkuSet.size}件, 弾かれた${rejectedSkus.length}件`);

    const hasUnknownSku = enrichedProblems.some(p => p.msku === '-');

    res.json({
      success: result.status === 'SUCCESS',
      inboundPlanId: result.inboundPlanId,
      operationId: result.operationId,
      status: result.status,
      problems: enrichedProblems,
      totalItems: items.length,
      successItems: result.status === 'SUCCESS' ? items.length : 0,
      errorItems: enrichedProblems.length,
      retried: prepCorrections.length > 0,
      prepCorrections,
      submittedItems: items.map(i => ({ amazon_sku: i.amazon_sku, product_name: i.product_name, ship_qty: i.ship_qty })),
      hasUnknownSku,
    });
  } catch (e) {
    console.error('[Inbound] プラン作成エラー:', e);
    res.status(500).json({ error: e.message });
  } finally {
    inboundPlanInProgress = false;
  }
});

// ===== 納品作業ドラフト =====
router.get('/api/draft', (req, res) => {
  res.json(getDraft());
});

router.post('/api/draft', express.json(), (req, res) => {
  const { items, memo } = req.body;
  if (!Array.isArray(items)) return res.status(400).json({ error: 'items[] が必要です' });
  const count = saveDraft(items, memo);
  res.json({ success: true, count });
});

router.delete('/api/draft', (req, res) => {
  clearDraft();
  res.json({ success: true });
});

// ===== 新規商品 非表示管理 =====
router.get('/api/new-product-hidden', (req, res) => {
  res.json(getNewProductHidden());
});

router.post('/api/new-product-hidden', express.json(), (req, res) => {
  const { skus, reason } = req.body;
  if (!Array.isArray(skus) || skus.length === 0) return res.status(400).json({ error: 'skus[] が必要です' });
  const count = hideNewProductSkuBulk(skus, reason);
  res.json({ success: true, count });
});

router.delete('/api/new-product-hidden/:sku', (req, res) => {
  unhideNewProductSku(req.params.sku);
  res.json({ success: true });
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
  const restockRows = getRestockLatest();
  const mappings = getSkuMappings();
  const warehouse = getWarehouseInventory();
  const warehouseProducts = new Set(warehouse.map(w => w.logizard_code)).size;
  // 新データソース (RESTOCK) があればそれを正、無ければ従来 snapshot を使う
  const primaryCount = restockRows.length > 0 ? restockRows.length : snapshots.length;
  const latestDate = restockRows.length > 0
    ? (restockRows[0]?.updated_at || '').slice(0, 10) || null
    : snapshots[0]?.snapshot_date || null;
  res.json({
    dbReady,
    fetchInProgress,
    latestSnapshotDate: latestDate,
    snapshotCount: primaryCount, // UI互換: RESTOCK件数を優先表示
    restockCount: restockRows.length,
    legacySnapshotCount: snapshots.length,
    mappingCount: mappings.length,
    warehouseProducts,
    warehouseRows: warehouse.length,
  });
});

// ===== 1件Eligibilityチェック（フロントエンド駆動） =====
router.get('/api/eligibility/check-one', async (req, res) => {
  const { asin, msku } = req.query;
  if (!asin) return res.status(400).json({ error: 'asin必須' });
  try {
    const miniResult = await callMiniPC(`/eligibility/check-one?asin=${encodeURIComponent(asin)}&msku=${encodeURIComponent(msku || '')}`, { timeout: 15000 });
    const ineligible = miniResult.result || [];
    res.json({ asin, msku, is_eligible: ineligible.length === 0, reasons: ineligible.length > 0 ? (ineligible[0].reasons || []) : [] });
  } catch (e) {
    res.json({ asin, msku, is_eligible: true, reasons: [], error: (e.message || '').slice(0, 200) });
  }
});

// ===== Amazon仮確定 =====
router.get('/api/provisional', (req, res) => {
  res.json(getProvisionalItems());
});

router.post('/api/provisional', express.json(), (req, res) => {
  const { items } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });
  try {
    const count = saveProvisionalItems(items);
    res.json({ success: true, count });
  } catch (e) {
    console.error('[FBA] 仮確定保存エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// 仮確定データに差分マージ（既存データを保持しつつ追加・更新）
router.post('/api/provisional/merge', express.json(), (req, res) => {
  const { items } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });
  try {
    const count = mergeProvisionalItems(items);
    const result = getProvisionalItems();
    res.json({ success: true, merged: count, total: result.items.length });
  } catch (e) {
    console.error('[FBA] 仮確定マージエラー:', e);
    res.status(500).json({ error: e.message });
  }
});

router.delete('/api/provisional', (req, res) => {
  clearProvisionalItems();
  res.json({ success: true });
});

router.patch('/api/provisional/:sku/qty', express.json(), (req, res) => {
  const { qty } = req.body;
  if (qty === undefined) return res.status(400).json({ error: 'qty が必要です' });
  updateProvisionalItemQty(req.params.sku, qty);
  res.json({ success: true });
});

router.delete('/api/provisional/:sku', (req, res) => {
  removeProvisionalItem(req.params.sku);
  res.json({ success: true });
});

// ===== 納品Excel出力 =====
router.post('/api/export-manifest', express.json(), async (req, res) => {
  const { items } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });

  const settings = getSettings();
  const prepOwner = 'Seller';
  const labelOwner = 'Seller';

  try {
    const ExcelJS = (await import('exceljs')).default;
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('Create workflow – template');

    // Row1: 注意書き
    ws.getCell('A1').value = 'このシートに記入する前にExampleタブを確認してください';
    // Row3-4: デフォルト設定
    ws.getCell('A3').value = 'Default prep owner';
    ws.getCell('B3').value = prepOwner;
    ws.getCell('A4').value = 'Default labeling owner';
    ws.getCell('B4').value = labelOwner;
    // Row7: 任意列ラベル
    ws.getCell('C7').value = '任意';
    ws.getCell('F7').value = '任意：メーカー梱包のSKUにのみ使用';
    // Row8: ヘッダー
    const headers = ['Merchant SKU', 'Quantity', 'Prep owner', 'Labeling owner', 'Expiration date (MM/DD/YYYY)', 'Units per box ', 'Number of boxes', 'Box length (cm)', 'Box width (cm)', 'Box height (cm)', 'Box weight (kg)'];
    headers.forEach((h, i) => {
      const cell = ws.getCell(8, i + 1);
      cell.value = h;
      cell.font = { bold: true };
    });

    // Row9〜: データ行
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      const row = 9 + i;
      ws.getCell(row, 1).value = item.amazon_sku;
      ws.getCell(row, 2).value = item.ship_qty;
      // 有効期限: YYYYMMDD or YYYY-MM-DD or YYYY/MM/DD → MM/DD/YYYY
      if (item.expiry_date) {
        const raw = item.expiry_date.replace(/[\/\-]/g, '');
        if (raw.length === 8) {
          const m = raw.slice(4, 6), d = raw.slice(6, 8), y = raw.slice(0, 4);
          ws.getCell(row, 5).value = `${m}/${d}/${y}`;
        } else {
          ws.getCell(row, 5).value = item.expiry_date;
        }
      }
    }

    // 列幅調整
    ws.getColumn(1).width = 30;
    ws.getColumn(2).width = 10;
    ws.getColumn(5).width = 25;

    const buffer = await wb.xlsx.writeBuffer();
    const now = new Date();
    const dateStr = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
    const filename = `FBA_Manifest_${dateStr}.xlsx`;
    const totalQty = items.reduce((sum, it) => sum + (parseInt(it.ship_qty) || 0), 0);
    try { saveExportHistory('manifest_excel', filename, items.length, totalQty, Buffer.from(buffer)); } catch(he) { console.error('[FBA] 履歴保存エラー:', he); }

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=${filename}`);
    res.send(Buffer.from(buffer));
  } catch (e) {
    console.error('[FBA] Excel出力エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== ピッキングリスト取得（試験） =====
router.get('/api/picking-list/:planId', async (req, res) => {
  const { planId } = req.params;
  try {
    console.log(`[Picking] プラン ${planId} のshipment一覧をミニPC経由で取得中...`);
    const miniResult = await callMiniPC(`/picking-list/${encodeURIComponent(planId)}`, { timeout: 60000 });
    const shipmentData = miniResult.shipments || [];
    console.log(`[Picking] ${shipmentData.length}件のshipment`);

    const result = [];
    const mappings = getSkuMappings();
    const mappingMap = {};
    for (const m of mappings) mappingMap[m.amazon_sku] = m;

    for (const sd of shipmentData) {
      const shipment = sd.shipment;
      const items = sd.items;
      console.log(`[Picking] shipment ${shipment.shipmentId}: ${items.length}アイテム`);
      result.push({
        shipmentId: shipment.shipmentId,
        destination: shipment.destination || '',
        status: shipment.status || '',
        items: items.map(item => {
          const mapping = mappingMap[item.msku] || {};
          return {
            msku: item.msku,
            fnsku: item.fnsku || '',
            asin: item.asin || '',
            quantity: item.quantity || 0,
            expiration: item.expiration || '',
            labelOwner: item.labelOwner || '',
            prepOwner: item.prepOwner || '',
            product_name: mapping.product_name || '',
            ne_code: mapping.ne_code || '',
          };
        }),
      });
    }

    res.json({ planId, shipments: result });
  } catch (e) {
    console.error('[Picking] エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== NE受注CSV出力 =====
router.post('/api/export-ne-csv', express.json(), async (req, res) => {
  const { items } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });

  const mappings = getSkuMappings();
  const mappingMap = {};
  for (const m of mappings) mappingMap[m.amazon_sku] = m;

  // SKU → NE商品コードに展開し、同一NE商品コードは合算
  const neAggregated = {};
  const warnings = [];

  for (const item of items) {
    const mapping = mappingMap[item.amazon_sku];
    if (!mapping) {
      warnings.push(`${item.amazon_sku}: SKUマッピングなし（スキップ）`);
      continue;
    }

    let components = [];
    if (mapping.set_components) {
      try {
        components = typeof mapping.set_components === 'string'
          ? JSON.parse(mapping.set_components)
          : mapping.set_components;
      } catch (e) {
        components = [];
      }
    }

    // componentsがない場合はne_codeをqty=1として使用
    if (!components || components.length === 0) {
      if (mapping.ne_code) {
        components = [{ ne_code: mapping.ne_code, qty: 1 }];
      } else {
        warnings.push(`${item.amazon_sku} (${mapping.product_name || ''}): NE商品コードなし（スキップ）`);
        continue;
      }
    }

    const shipQty = parseInt(item.ship_qty) || 0;
    for (const comp of components) {
      const neCode = comp.ne_code;
      if (!neCode) continue;
      const neQty = shipQty * (parseInt(comp.qty) || 1);
      if (neAggregated[neCode]) {
        neAggregated[neCode].qty += neQty;
      } else {
        // 商品名はNE商品コードに対応するmappingから取得
        const neMapping = Object.values(mappingMap).find(m => m.ne_code === neCode);
        neAggregated[neCode] = {
          ne_code: neCode,
          product_name: neMapping?.product_name || mapping.product_name || '',
          qty: neQty,
        };
      }
    }
  }

  const neItems = Object.values(neAggregated).sort((a, b) => a.ne_code.localeCompare(b.ne_code));

  if (neItems.length === 0) {
    return res.status(400).json({ error: 'NE商品コードに変換できる商品がありません', warnings });
  }

  // CSV生成（SHIFT-JIS、61列のインボイス形式）
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const dateStr = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}`;
  const timeStr = `${pad(now.getHours())}${pad(now.getMinutes())}`;
  const orderNo = `FBA${dateStr}${timeStr}`;
  const orderName = `${dateStr}FBA納品`;

  // ヘッダー（61列）
  const headers = [
    '店舗伝票番号','受注日','受注郵便番号','受注住所１','受注住所２','受注名','受注名カナ',
    '受注電話番号','受注メールアドレス','発送郵便番号','発送先住所１','発送先住所２','発送先名',
    '発送先カナ','発送電話番号','支払方法','発送方法','商品計','税金','発送料','手数料',
    '手数料(0%対象)','手数料(8%対象)','手数料(10%対象)','ポイント','ポイント(0%対象)',
    'ポイント(8%対象)','ポイント(10%対象)','ポイント(按分)','ポイント(支払い)','その他費用',
    'その他費用(0%対象)','その他費用(8%対象)','その他費用(10%対象)','クーポン割引額',
    'クーポン割引額(0%対象)','クーポン割引額(8%対象)','クーポン割引額(10%対象)',
    'クーポン割引額(按分)','請求金額(0%対象)','請求金額(8%対象)','請求額に対する税額(8%対象)',
    '請求金額(10%対象)','請求額に対する税額(10%対象)','合計金額','ギフトフラグ','時間帯指定',
    '日付指定','作業者欄','備考','商品名','商品コード','商品価格','受注数量','商品オプション',
    '出荷済フラグ','顧客区分','顧客コード','消費税率（%）','のし','ラッピング'
  ];

  function csvEscape(val) {
    const s = String(val ?? '');
    if (s.includes(',') || s.includes('"') || s.includes('\n')) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  }

  const rows = [headers.map(csvEscape).join(',')];

  for (const item of neItems) {
    const row = new Array(61).fill('');
    row[0] = orderNo;           // A: 店舗伝票番号
    row[1] = dateStr;           // B: 受注日
    row[2] = '5640038';         // C: 受注郵便番号
    row[3] = '大阪府吹田市南清和園町41‐36'; // D: 受注住所１
    row[4] = 'Amazon倉庫';     // E: 受注住所２
    row[5] = orderName;        // F: 受注名
    row[7] = '09085325647';    // H: 受注電話番号
    row[9] = '5640038';        // J: 発送郵便番号
    row[10] = '大阪府吹田市南清和園町41‐36'; // K: 発送先住所１
    row[11] = 'Amazon倉庫';   // L: 発送先住所２
    row[12] = orderName;       // M: 発送先名
    row[14] = '09085325647';   // O: 発送電話番号
    row[15] = '支払済';        // P: 支払方法
    row[16] = '西濃運輸カンガルm2'; // Q: 発送方法
    row[17] = '0';             // R: 商品計
    row[44] = '0';             // AS: 合計金額
    row[45] = '0';             // AT: ギフトフラグ
    row[49] = 'FBA納品用の伝票です。納品した日に伝票を出荷確定してください。'; // AX: 備考
    row[50] = item.product_name; // AY: 商品名
    row[51] = item.ne_code;    // AZ: 商品コード
    row[52] = '0';             // BA: 商品価格
    row[53] = String(item.qty); // BB: 受注数量
    row[55] = '0';             // BD: 出荷済フラグ
    row[56] = '0';             // BE: 顧客区分
    rows.push(row.map(csvEscape).join(','));
  }

  const csvContent = rows.join('\r\n');

  // SHIFT-JISにエンコード
  try {
    const iconv = (await import('iconv-lite')).default;
    const encoded = iconv.encode(csvContent, 'Shift_JIS');
    const csvFilename = `hanyo-jyuchu_invoice_${dateStr}.csv`;
    const totalQty = neItems.reduce((sum, it) => sum + (parseInt(it.qty) || 0), 0);
    try { saveExportHistory('ne_csv', csvFilename, neItems.length, totalQty, encoded); } catch(he) { console.error('[FBA] 履歴保存エラー:', he); }
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename=${csvFilename}`);
    res.send(encoded);
    console.log(`[FBA] NE CSV出力: ${neItems.length}件 (警告: ${warnings.length}件)`);
    if (warnings.length > 0) console.log(`[FBA] NE CSV警告:`, warnings);
  } catch (e) {
    console.error('[FBA] NE CSV出力エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// ===== 出力履歴 =====
router.get('/api/export-history', (req, res) => {
  try {
    const list = getExportHistoryList();
    res.json(list);
  } catch (e) {
    console.error('[FBA] 出力履歴取得エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

router.get('/api/export-history/:id/download', (req, res) => {
  try {
    const record = getExportHistoryFile(parseInt(req.params.id));
    if (!record || !record.file_data) return res.status(404).json({ error: '履歴が見つかりません' });
    const contentType = record.type === 'manifest_excel'
      ? 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      : 'text/csv; charset=Shift_JIS';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename=${record.filename}`);
    res.send(Buffer.from(record.file_data));
  } catch (e) {
    console.error('[FBA] 履歴DLエラー:', e);
    res.status(500).json({ error: e.message });
  }
});

export default router;
