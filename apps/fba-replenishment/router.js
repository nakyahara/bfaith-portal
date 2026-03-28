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
         getStockoutHidden, hideStockoutSku, unhideStockoutSku, hideStockoutSkuBulk,
         getNewProductHidden, hideNewProductSkuBulk, unhideNewProductSku,
         saveDraft, getDraft, clearDraft } from './db.js';
import { fetchAllReports, normalizePlanningRow } from './sp-api-reports.js';
import { syncSkuMappings } from './sheets-sync.js';
import { generateRecommendations } from './calculation-engine.js';
import { createInboundPlan, checkInboundEligibility } from './inbound-plans.js';

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
        const result = await createInboundPlan(sourceAddress, apiItems, planName);

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

    // SKU不明のエラーが残っている場合、Eligibility APIでチェック
    const hasUnknownSku = enrichedProblems.some(p => p.msku === '-');
    if (hasUnknownSku && result.status === 'FAILED') {
      console.log('[Inbound] SKU不明エラーあり → Eligibility APIで全アイテムチェック...');
      try {
        const eligibilityItems = items
          .filter(i => i.asin)
          .map(i => ({ asin: i.asin, msku: i.amazon_sku }));
        const ineligible = await checkInboundEligibility(eligibilityItems);
        console.log(`[Inbound] Eligibility結果: ${ineligible.length}件が不適格`);

        if (ineligible.length > 0) {
          // Eligibility結果でproblemsを上書き
          enrichedProblems = ineligible.map(ie => {
            const item = items.find(i => i.amazon_sku === ie.msku);
            const reasonCodes = ie.reasons.map(r => r.code || r).join(', ');
            const reasonMsgs = ie.reasons.map(r => r.message || r.code || r).join('; ');
            return {
              msku: ie.msku,
              product_name: item?.product_name || '',
              code: reasonCodes || 'INELIGIBLE',
              message: reasonMsgs || 'FBA受入不可',
            };
          });
        }
      } catch (eligErr) {
        console.error('[Inbound] Eligibilityチェック失敗:', eligErr.message);
      }
    }

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
