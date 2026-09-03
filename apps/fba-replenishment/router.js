/**
 * FBA在庫補充システム — ルーター
 */
import express from 'express';
import multer from 'multer';
import cron from 'node-cron';
import { initDb, savePlanningData, savePlanningDataWithHistory, getLatestSnapshots, getAllSnapshotSkus, getSettings, updateSetting,
         getSkuMappings, getSkuExceptions, upsertSkuException, deleteSkuException,
         getWarehouseInventory, replaceWarehouseInventory, getWarehouseSummary, getWarehouseUniqueProductCount,
         getShipmentPlans, getShipmentPlanItems, getDailySnapshots,
         getStockoutHidden, hideStockoutSku, unhideStockoutSku, hideStockoutSkuBulk,
         getNewProductHidden, hideNewProductSkuBulk, unhideNewProductSku,
         getReplenishmentExcluded, excludeReplenishmentSku, unexcludeReplenishmentSku,
         saveDraft, getDraft, clearDraft, updateFnskuBatch, syncFnskuBatch,
         saveProvisionalItems, mergeProvisionalItems, getProvisionalItems, clearProvisionalItems,
         updateProvisionalItemQty, removeProvisionalItem,
         saveExportHistory, getExportHistoryList, getExportHistoryFile,
         getRestockLatest, getPlanningLatestMap, getAllEverSeenSkus, getEverStockedSkus,
         saveRestockLatest, savePlanningLatest,
         getSkuMappingSourceMode,
         getWarehouseBarcodeRows, getDodaiMaster,
         getPickingMasterStatus, savePickingRun, getPickingRuns, getPickingRun, deletePickingRun,
         getLastRecommendationRun, saveRecommendationRun, getRecentRecommendationRuns,
         getInboundDailySummary, getInboundMonthlySummary, getInboundShipmentsByDate, getInboundItems,
         getInboundUnreceived, getInboundSyncStatus, getInboundShipmentsWithoutDate,
         importInboundRows, getInboundSyncCursor } from './db.js';
import { parseCsv, decodeCsvBuffer, buildShiftJisCsv } from './picking-csv.js';
import { parseWarehouseCsv } from './warehouse-csv.js';
import * as pp from './picking-prep.js';
import { uploadCsvToDrive } from './drive-upload.js';
import { annotatePickingPdf, extractPickingPdf } from './annotate-pdf.js';
import { savePickingPdf, pickingPdfPath } from './picking-pdf-store.js';
import { createPickingCard, notionConfigured } from './notion-attach.js';
// SP-API関連はミニPC経由で実行（APIキーはミニPC側に一元管理）
// import { fetchAllReports, normalizePlanningRow } from './sp-api-reports.js';
// import { createInboundPlan, checkInboundEligibility, findErrorSkusByBinarySearch, listShipments, listShipmentItems, fetchActiveInboundQuantities } from './inbound-plans.js';
// 箱詰め記録 (apps/fba-box): picking-prep 実行完了で納品回を自動作成する (専用 fba-box.db。読み書きは fba-box 側の関数のみ)
import { createRunFromPicking as createBoxRunFromPicking } from '../fba-box/db.js';
import { ensureRunImages as ensureBoxRunImages } from '../fba-box/images.js';
import { syncSkuMappings, syncDodaiMaster } from './sheets-sync.js';
import { generateRecommendations } from './calculation-engine.js';
import { normalizePlanningRow } from './sp-api-reports.js';
import { bootStart, bootEnd, bootFail, bootNote } from '../observability/boot-log.js';
import { buildInboundChart } from './inbound-chart.js';
import { pingJob } from '../jobs-monitor/ping-local.js';
import { isRender } from '../../lib/is-render.js';
import archiver from 'archiver';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

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
// メモリ保持のため上限必須 (デコード候補生成でバッファの数倍を消費する)。実CSVは数MB程度
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// DB初期化
let dbReady = false;
bootStart('fba-db', 'fba-replenishment.db');
initDb().then(() => {
  dbReady = true;
  bootEnd('fba-db', 'fba-replenishment.db');

  // 毎日06:00 JST にSKUマッピング同期（他CH売上スナップショット蓄積）
  //
  // ⚠ Render 限定。miniPC も同じ server.js を動かすため、無条件に登録すると
  //   Google Sheets / SP-API を2箇所から叩き、miniPC 側のローカル DB にも書き込む
  //   (2026-08-05 に実際に二重実行していたのを確認。feedback_minipc_shares_portal_server_js)。
  // ⚠ timezone を明示すること。以前は未指定で「コンテナが UTC だから結果的に 06:00 JST」
  //   という状態で、TZ env を入れた瞬間に9時間ずれる作りだった。
  //   実際 miniPC (ローカル TZ = JST) では 21:00 に走っていた。
  if (!isRender()) {
    bootNote('fba-cron', 'RENDER未設定のため定期同期をスケジュールしない (Render専用)');
    console.log('[FBA] 非Render環境のため定期同期スケジュールをスキップ');
  } else {
    bootStart('fba-cron', 'fba-sku-sync-cron');
    cron.schedule('0 6 * * *', async () => {
      console.log('[FBA-Cron] SKUマッピング定期同期開始...');
      // dead-man 監視 (jobs-registry: fba-daily-sync)。
      // 主目的の SKU マッピング同期が成功したかを ok/fail の基準にし、
      // 後続2つ (best-effort) の結果は note に載せる。
      let pingStatus = 'fail';
      const notes = [];
      try {
        const result = await syncSkuMappings();
        console.log(`[FBA-Cron] 完了: ${result.total}件 (スナップショット: ${result.snapshots}件)`);
        pingStatus = 'ok';
        notes.push(`sku=${result.total}`);
      } catch (e) {
        console.error('[FBA-Cron] SKUマッピング同期エラー:', e);
        notes.push(`sku失敗: ${e.message}`);
      }
      // 土台商品マスタ(ピッキング準備)も同期。失敗しても他処理に影響させない (best-effort)。
      try {
        const dr = await syncDodaiMaster();
        console.log(`[FBA-Cron] 土台商品マスタ同期完了: ${dr.count}件`);
        notes.push(`土台=${dr.count}`);
      } catch (e) {
        console.error('[FBA-Cron] 土台商品マスタ同期エラー:', e);
        notes.push(`土台失敗: ${e.message}`);
      }
      // 納品実績 (Fulfillment Inbound v0)。独立したスケジュールを増やさず、ここに1ステップとして載せる。
      try {
        const ih = await runInboundHistoryDailySync();
        console.log(`[FBA-Cron] 納品実績同期完了: シップメント${ih.shipments}件 / 明細${ih.items}件`);
        notes.push(`納品=${ih.shipments}/${ih.items}`);
      } catch (e) {
        console.error('[FBA-Cron] 納品実績同期エラー:', e);
        notes.push(`納品失敗: ${e.message}`);
      }
      pingJob('fba-daily-sync', pingStatus, notes.join(' '));
    }, { timezone: 'Asia/Tokyo' });
    console.log('[FBA] 定期同期スケジュール設定: 毎日06:00 JST');
    bootEnd('fba-cron', 'fba-sku-sync-cron', 'cron=0 6 * * * JST');
  }
}).catch(e => {
  bootFail('fba-db', 'fba-replenishment.db', e);
  console.error('[FBA] DB初期化エラー:', e);
});

function ensureDb(req, res, next) {
  if (!dbReady) return res.status(503).json({ error: 'DB初期化中' });
  next();
}

router.use(ensureDb);

/**
 * 拡張機能のzipに入れないもの (Googleに提出するパッケージに社内メモや素材を混ぜないため)。
 * store/ にはストア掲載用のスクリーンショット素材が入っている。select-set と同じ方針。
 */
const EXT_DEV_ONLY_FILES = new Set(['make-icons.mjs', 'STORE_LISTING.md']);
const EXT_DEV_ONLY_DIRS = ['store/'];

function isExtDevOnly(name) {
  return EXT_DEV_ONLY_FILES.has(name) || EXT_DEV_ONLY_DIRS.some((d) => name === d.slice(0, -1) || name.startsWith(d));
}

/**
 * FBA納品 福通伝票CSV の Chrome拡張を zip で配る。
 * 各PCへ「フォルダを読み込む」で入れるより、zipを落として解凍するほうが更新が楽。
 * Chromeウェブストアへ提出するパッケージもこれをそのまま使える。
 */
router.get('/download/extension.zip', (req, res) => {
  const dir = path.resolve(__dirname, '../../tools/fba-fukutsu-helper');
  if (!fs.existsSync(dir)) {
    return res.status(404).json({ error: '拡張機能のファイルが見つかりません' });
  }
  let version = '';
  try {
    version = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8')).version || '';
  } catch { /* バージョンが読めなくても配布は続ける */ }
  res.set({
    'Content-Type': 'application/zip',
    'Content-Disposition': `attachment; filename=fba-fukutsu-helper${version ? '-' + version : ''}.zip`,
  });
  const archive = archiver('zip', { zlib: { level: 9 } });
  archive.on('error', (e) => {
    console.error('[fba-replenishment] 拡張機能のzip作成に失敗', e);
    res.destroy();
  });
  archive.pipe(res);
  archive.directory(dir, false, (entry) => (isExtDevOnly(entry.name) ? false : entry));
  archive.finalize();
});


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

// ===== 過去に実FBA在庫/入荷があったSKU一覧 (新規商品/FBA欠品の正しい振り分け軸) =====
// all-snapshot-skus / ever-seen-skus は「FBA出品に載った」だけで付くため未納品の新規出品も含む。
// これは「実在庫・入荷を一度でも観測した」SKUのみ。新規商品タブはこれに不在のSKUを新規とみなす。
router.get('/api/ever-stocked-skus', (req, res) => {
  res.json(getEverStockedSkus());
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
    const parsed = parseWarehouseCsv(req.file.buffer);
    if (parsed.error) return res.status(400).json({ error: parsed.error });
    const { items, invalidRows } = parsed;

    // 商品数急減ガード: ユニーク商品ID数が前回の半分未満なら誤ファイル疑いで確認を要求。
    // 強行 (force=1) は「確認した時点のprevUnique」の一致を要求し、確認〜再送の間に
    // 別のアップロードでDBが変わっていた場合は再度409にする (古い確認での上書き競合を防止)
    const prevUnique = getWarehouseUniqueProductCount();
    const newUnique = new Set(items.map(i => i.logizard_code.trim().toLowerCase())).size;
    if (prevUnique >= 20 && newUnique < prevUnique * 0.5) {
      const forced = req.body?.force === '1' && Number(req.body?.force_prev_unique) === prevUnique;
      if (!forced) {
        return res.status(409).json({
          needs_confirm: true,
          prevUnique, newUnique,
          error: `登録商品数が前回${prevUnique}商品 → 今回${newUnique}商品に急減しています。誤ったファイルの可能性があります`,
        });
      }
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
        skippedRows: invalidRows.length,
        skippedSamples: invalidRows.slice(0, 5).map(r => `${r.line}行目: ${r.reason}`),
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

// ───────────────────────────────────────────────────────────
// 推奨の健全性チェック (③観測性/DQ + ④一律=SKU一致率検知)
//   2026-06-30 事故: 準備中(納品プラン)取得が静かに欠損 → 在庫過小評価 → 前日納品分を一律再提示。
//   毎runでスナップショットを残し、欠損/急増/前回一致 を即フラグする。
// ───────────────────────────────────────────────────────────
function computeRecommendationHealth(result, inboundOverride) {
  const normSku = (v) => String(v ?? '').trim().toLowerCase();
  const ov = (inboundOverride && typeof inboundOverride === 'object') ? inboundOverride : {};
  const workingSkuCount = Object.keys(ov).length;
  const workingQtyTotal = Object.values(ov).reduce((s, q) => s + (Number(q) || 0), 0);

  // 推奨に上がったSKU (恒久除外を除く)。amazon_sku を正規化しユニーク化 (行重複で件数水増ししない)。
  const recSet = new Set();
  for (const i of (result.items || [])) {
    if (i.recommended_qty > 0 && !i.is_excluded) {
      const s = normSku(i.amazon_sku);
      if (s) recSet.add(s);
    }
  }
  const recommendedSkuCount = recSet.size;

  const prev = getLastRecommendationRun();

  // ─── シグナル抽出 ───
  const sig = {};
  let prevOverlap = null;
  if (prev && Array.isArray(prev.recommended_skus) && prev.recommended_skus.length && recommendedSkuCount > 0) {
    const prevSet = new Set(prev.recommended_skus.map(normSku));
    let inter = 0;
    for (const s of recSet) if (prevSet.has(s)) inter++;
    prevOverlap = inter / recSet.size;
    if (prevOverlap >= 0.5 && recommendedSkuCount >= 30) sig.high_overlap = { rate: prevOverlap, inter, n: recSet.size };
  }
  if (workingSkuCount === 0) sig.working_empty = true;
  else if (prev && prev.working_sku_count >= 20 && workingSkuCount < prev.working_sku_count * 0.3) sig.working_drop = { prev: prev.working_sku_count, now: workingSkuCount };
  if (prev && prev.recommended_sku_count >= 20 && recommendedSkuCount >= prev.recommended_sku_count * 1.8) sig.recommend_spike = { prev: prev.recommended_sku_count, now: recommendedSkuCount };

  // ─── 重大度判定: 単独では誤検知しやすいので複合で重大化 ───
  //   供給側異常(準備中の急減/欠損) と 出力側異常(一律/急増) が重なると今回の事故パターン。
  const supplyAnomaly = !!(sig.working_drop || sig.working_empty);
  const outputAnomaly = !!(sig.high_overlap || sig.recommend_spike);
  const flags = [];

  // working_drop: 健全な前回からの急減=取得欠損が濃厚 → 単独でも重大
  if (sig.working_drop) {
    flags.push({ level: 'critical', code: 'working_drop', msg: `準備中SKUが急減 (前回${sig.working_drop.prev}→今回${sig.working_drop.now})。納品プラン取得の欠損疑い → 在庫過小評価で過大推奨の恐れ` });
  }
  // working_empty: 本当にプラン無しの平常日もある → 単独は注意。出力異常と重なれば重大。
  if (sig.working_empty) {
    flags.push({ level: outputAnomaly ? 'critical' : 'warning', code: 'working_empty', msg: `準備中(納品プラン)が0件${outputAnomaly ? ' かつ 推奨が一律/急増 → 取得欠損で前回納品分を再提示している恐れ' : '。取得失敗か、本当にプラン無しかを確認'}` });
  }
  // 一律(高一致): 定番の連日補充なら正常 → 単独は注意。準備中異常と重なれば重大。
  if (sig.high_overlap) {
    flags.push({ level: supplyAnomaly ? 'critical' : 'warning', code: 'high_prev_overlap', msg: `前回推奨とSKU一致率 ${(sig.high_overlap.rate * 100).toFixed(0)}% (${sig.high_overlap.inter}/${sig.high_overlap.n})${supplyAnomaly ? ' かつ 準備中データ異常 → 前回納品分の再提示が濃厚。発送前に開いている納品プランと突合を' : '。定番の連日補充なら正常だが念のため確認'}` });
  }
  // 急増: 単独は注意。準備中異常と重なれば重大。
  if (sig.recommend_spike) {
    flags.push({ level: supplyAnomaly ? 'critical' : 'warning', code: 'recommend_spike', msg: `推奨SKU数が急増 (前回${sig.recommend_spike.prev}→今回${sig.recommend_spike.now})${supplyAnomaly ? ' かつ 準備中データ異常 → 在庫過小評価の疑い' : ''}` });
  }

  return {
    working_sku_count: workingSkuCount,
    working_qty_total: workingQtyTotal,
    recommended_sku_count: recommendedSkuCount,
    recommended_skus: Array.from(recSet),
    prev_overlap_rate: prevOverlap,
    prev_run_at: prev ? prev.run_at : null,
    flags,
    degraded: flags.some(f => f.level === 'critical'),
  };
}

// 健全性ログ履歴 (画面表示用)
router.get('/api/recommendation-health', (req, res) => {
  try { res.json({ runs: getRecentRecommendationRuns(30) }); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ===== ステータス =====
// ===== 推奨リスト =====
router.get('/api/recommendations', async (req, res) => {
  try {
    const debug = req.query.debug === '1' || req.query.debug === 'true';
    const inboundOverride = await getInboundWorkingData();
    const result = generateRecommendations(debug, inboundOverride);
    // PR4: norm キーで join (mirror 小文字 vs item 元ケースでも fnsku/除外が取りこぼれない)
    const normSku = (v) => String(v ?? '').trim().toLowerCase();
    // FNSKU情報を付与
    const mappings = getSkuMappings();
    const fnskuMap = {};
    for (const m of mappings) if (m.fnsku) fnskuMap[normSku(m.amazon_sku)] = m.fnsku;
    const fnskuCount = Object.keys(fnskuMap).length;
    console.log(`[FBA] 推奨API: mappings=${mappings.length}, fnsku有り=${fnskuCount}, inboundOverride=${inboundOverride ? Object.keys(inboundOverride).length + ' SKU' : 'なし'}`);
    if (fnskuCount > 0) {
      const sample = Object.entries(fnskuMap).slice(0, 3);
      console.log(`[FBA] FNSKUサンプル:`, sample);
    }
    for (const item of result.items) {
      item.fnsku = fnskuMap[normSku(item.amazon_sku)] || '';
    }
    // 恒久除外フラグを付与 (クライアントの3タブフィルタ + サマリーが参照。除外SKUも行自体は返す)
    const excludedSet = new Set(getReplenishmentExcluded().map(r => normSku(r.amazon_sku)));
    for (const item of result.items) {
      item.is_excluded = excludedSet.has(normSku(item.amazon_sku));
    }
    const itemsWithFnsku = result.items.filter(i => i.fnsku).length;
    console.log(`[FBA] 推奨items: ${result.items.length}件, fnsku付与=${itemsWithFnsku}件, 除外=${result.items.filter(i => i.is_excluded).length}件`);

    // ─── ③観測性 + ④一律検知: 推奨の健全性を毎回スナップショット ───
    //   事故(2026-06-30: 準備中データ静かに欠損→大量再提示)の再発を即検知する。
    try {
      result.health = computeRecommendationHealth(result, inboundOverride);
      // 保存は「明示的に推奨生成した時(?persist=1)」のみ。画面リロード/タブ操作/補助fetchでは
      // 保存しない → 「前回」が数秒前の同一結果になって一致率検知が無意味化&ログ汚染するのを防ぐ。
      const persist = req.query.persist === '1' || req.query.persist === 'true';
      if (persist) {
        saveRecommendationRun({
          working_sku_count: result.health.working_sku_count,
          working_qty_total: result.health.working_qty_total,
          recommended_sku_count: result.health.recommended_sku_count,
          recommended_skus: result.health.recommended_skus,
          prev_overlap_rate: result.health.prev_overlap_rate,
          health_flags: result.health.flags,
          degraded: result.health.degraded,
        });
      }
      if (result.health.flags.length) {
        console.warn(`[FBA] 推奨健全性: degraded=${result.health.degraded} persist=${persist} flags=${JSON.stringify(result.health.flags.map(f => f.code))}`);
      }
      delete result.health.recommended_skus; // 応答は軽量に
    } catch (e) {
      console.error('[FBA] 推奨健全性チェック失敗(推奨自体は返す):', e.message);
    }
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

  let { items, planName } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });

  // 恒久除外SKUはサーバ側でも納品プランから除く (stale画面/直接API 経由の漏れ防止)。norm で case 差も拾う
  {
    const normSku = (v) => String(v ?? '').trim().toLowerCase();
    const excludedSet = new Set(getReplenishmentExcluded().map(r => normSku(r.amazon_sku)));
    const before = items.length;
    items = items.filter(it => !excludedSet.has(normSku(it.amazon_sku || it.msku || it.sku)));
    if (items.length < before) console.log(`[FBA] create-inbound-plan: 除外SKU ${before - items.length}件をスキップ`);
    if (items.length === 0) return res.status(400).json({ error: '納品対象がありません（全て除外指定SKUでした）' });
  }

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

// ===== 納品推奨 恒久除外 (replenishment_excluded) =====
router.get('/api/replenishment-excluded', (req, res) => {
  // error handling: 例外時も JSON で返す (HTML 500 だと frontend の res.json() がコケて「読み込み失敗」になる)。
  // 実エラーは Render ログで追えるよう console.error。
  try {
    res.json(getReplenishmentExcluded());
  } catch (e) {
    console.error('[FBA] GET /api/replenishment-excluded エラー:', e.message, e.stack);
    res.status(500).json({ error: e.message || 'failed' });
  }
});

router.post('/api/replenishment-excluded', express.json(), (req, res) => {
  const { amazon_sku, reason } = req.body;
  if (!amazon_sku) return res.status(400).json({ error: 'amazon_sku が必要です' });
  // 任意文字列の保存→一覧表示(inline onclick/属性)時の XSS/属性破壊を防ぐ。
  // 実SKUは空白や / を含み得るので許可し、HTML/JS文脈を壊す文字 (引用符・山括弧・バッククォート・
  // バックスラッシュ・制御文字) と過長のみ弾く。
  if (typeof amazon_sku !== 'string' || amazon_sku.length > 100 || /['"<>`\\\x00-\x1f]/.test(amazon_sku)) {
    return res.status(400).json({ error: 'amazon_sku に使用できない文字が含まれています' });
  }
  excludeReplenishmentSku(amazon_sku, reason);
  // 除外と同時に仮確定からも落とす (既に仮確定済みの SKU が納品されないように cascade)
  try { removeProvisionalItem(amazon_sku); } catch (e) { console.error('[FBA] 除外時の仮確定削除エラー:', e.message); }
  res.json({ success: true });
});

router.delete('/api/replenishment-excluded/:sku', (req, res) => {
  unexcludeReplenishmentSku(req.params.sku);
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
  let { items } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });
  try {
    // 恒久除外SKUは仮確定に残さない (norm で case 差も拾う)
    const normSku = (v) => String(v ?? '').trim().toLowerCase();
    const excludedSet = new Set(getReplenishmentExcluded().map(r => normSku(r.amazon_sku)));
    items = items.filter(it => !excludedSet.has(normSku(it.amazon_sku)));
    const count = saveProvisionalItems(items);
    res.json({ success: true, count });
  } catch (e) {
    console.error('[FBA] 仮確定保存エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// 仮確定データに差分マージ（既存データを保持しつつ追加・更新）
router.post('/api/provisional/merge', express.json(), (req, res) => {
  let { items } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });
  try {
    // 恒久除外SKUは仮確定にも入れない (stale画面/直接API 経由の漏れ防止)。norm で case 差も拾う
    const normSku = (v) => String(v ?? '').trim().toLowerCase();
    const excludedSet = new Set(getReplenishmentExcluded().map(r => normSku(r.amazon_sku)));
    items = items.filter(it => !excludedSet.has(normSku(it.amazon_sku)));
    if (items.length === 0) return res.json({ success: true, merged: 0, total: getProvisionalItems().items.length, skipped_excluded: true });
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
  let { items } = req.body;
  if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] が必要です' });
  // 恒久除外SKUはサーバ側でも納品Excelから除く (stale画面/直接API 経由の漏れ防止)。norm で case 差も拾う
  {
    const normSku = (v) => String(v ?? '').trim().toLowerCase();
    const excludedSet = new Set(getReplenishmentExcluded().map(r => normSku(r.amazon_sku)));
    items = items.filter(it => !excludedSet.has(normSku(it.amazon_sku || it.msku || it.sku)));
    if (items.length === 0) return res.status(400).json({ error: '出力対象がありません（全て除外指定SKUでした）' });
  }

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
    try { saveExportHistory('manifest_excel', filename, items.length, totalQty, Buffer.from(buffer), items.map(it => it.amazon_sku || it.msku || it.sku).filter(Boolean)); } catch(he) { console.error('[FBA] 履歴保存エラー:', he); }

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
    // PR4: norm キーで構築 (mirror 小文字 vs Amazon msku 元ケースでも一致)
    const normSku = (v) => String(v ?? '').trim().toLowerCase();
    const mappingMap = {};
    for (const m of mappings) mappingMap[normSku(m.amazon_sku)] = m;

    for (const sd of shipmentData) {
      const shipment = sd.shipment;
      const items = sd.items;
      console.log(`[Picking] shipment ${shipment.shipmentId}: ${items.length}アイテム`);
      result.push({
        shipmentId: shipment.shipmentId,
        destination: shipment.destination || '',
        status: shipment.status || '',
        items: items.map(item => {
          const mapping = mappingMap[normSku(item.msku)] || {};
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
  // PR4: mappingMap は norm キーで構築・参照 (mirror 小文字 vs item 元ケースでも一致、SKU欠落防止)
  const normSku = (v) => String(v ?? '').trim().toLowerCase();
  const mappingMap = {};
  for (const m of mappings) mappingMap[normSku(m.amazon_sku)] = m;
  // 恒久除外SKUはサーバ側でも納品させない (stale画面/直接API 経由の漏れ防止)。norm で case 差も拾う
  const excludedSet = new Set(getReplenishmentExcluded().map(r => normSku(r.amazon_sku)));

  // SKU → NE商品コードに展開し、同一NE商品コードは合算
  const neAggregated = {};
  const warnings = [];
  const includedSkus = []; // 実際にCSVに入った amazon_sku (履歴の再DL除外チェック用)

  for (const item of items) {
    if (excludedSet.has(normSku(item.amazon_sku))) {
      warnings.push(`${item.amazon_sku}: 納品除外指定のためスキップ`);
      continue;
    }
    const mapping = mappingMap[normSku(item.amazon_sku)];
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

    // 有効な ne_code を持つ構成だけ採用。全滅なら無音欠落させずスキップ+警告。
    const validComponents = components.filter(c => c && c.ne_code);
    if (validComponents.length === 0) {
      warnings.push(`${item.amazon_sku} (${mapping.product_name || ''}): 構成のNE商品コードが空（スキップ）`);
      continue;
    }
    if (validComponents.length < components.length) {
      warnings.push(`${item.amazon_sku} (${mapping.product_name || ''}): 一部構成のNE商品コードが空（その分のみ除外）`);
    }

    const shipQty = parseInt(item.ship_qty) || 0;
    if (shipQty <= 0) {
      // 数量未入力(0)のSKUは黙ってqty0行を作らず、明示スキップ+警告。
      // 新規商品は既定 recommended_qty=0 のため、数量入力を忘れるとここで漏れやすい。
      warnings.push(`${item.amazon_sku} (${mapping.product_name || ''}): 出荷数量が0のためスキップ（仮確定タブで数量を入力してください）`);
      continue;
    }
    includedSkus.push(item.amazon_sku);
    for (const comp of validComponents) {
      const neCode = comp.ne_code;
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
    try { saveExportHistory('ne_csv', csvFilename, neItems.length, totalQty, encoded, includedSkus); } catch(he) { console.error('[FBA] 履歴保存エラー:', he); }
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename=${csvFilename}`);
    // スキップされたSKUを成功時(200+CSV)でもクライアントに伝える (従来は失敗時しか warnings を返さず無音欠落だった)。
    // 件数は常に返す。詳細はヘッダー過大で CSV ダウンロード自体が失敗しないよう byte 上限(約6KB)で詰める。
    res.setHeader('X-NE-Skipped-Count', String(warnings.length));
    if (warnings.length > 0) {
      const picked = [];
      let bytes = 2; // "[]" 分
      for (const w of warnings) {
        const enc = encodeURIComponent(JSON.stringify(w));
        if (bytes + enc.length + 1 > 6000) break; // +1 はカンマ区切り相当
        picked.push(w);
        bytes += enc.length + 1;
      }
      if (picked.length > 0) res.setHeader('X-NE-Warnings', encodeURIComponent(JSON.stringify(picked)));
    }
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
    // 後から恒久除外された SKU を含む古い履歴ファイルの再DLを拒否 (stale経路からの納品防止)。
    // sku_list 未記録 (本機能以前の履歴) はチェック対象外。
    if (record.sku_list) {
      try {
        const fileSkus = JSON.parse(record.sku_list);
        if (Array.isArray(fileSkus) && fileSkus.length > 0) {
          const normSku = (v) => String(v ?? '').trim().toLowerCase();
          const excludedSet = new Set(getReplenishmentExcluded().map(r => normSku(r.amazon_sku)));
          const hit = fileSkus.filter(s => excludedSet.has(normSku(s)));
          if (hit.length > 0) {
            return res.status(409).json({ error: `この履歴ファイルには現在「納品除外」指定のSKUが含まれます (${hit.slice(0, 5).join(', ')}${hit.length > 5 ? ' 他' : ''})。再出力してください。`, excluded_skus: hit });
          }
        }
      } catch (e) { /* sku_list 壊れていてもDL自体は通す */ }
    }
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

// ============================================================
// FBA納品ピッキング準備 (GAS「FBA処理」ワークフローのアプリ化)
// ============================================================

// プラン別スロット。label = 納品プランNo の接頭辞 (現場が物理的に参照するため UI で編集可、既定値はここ)。
const PLAN_SLOTS = [
  { id: 'p1_normal', plan: 'P1', kind: '通常',   sheet: 'P1_通常',   label: '通常' },
  { id: 'p1_danger', plan: 'P1', kind: '危険物', sheet: 'P1_危険物', label: '危険' },
  { id: 'p1_large',  plan: 'P1', kind: '大型',   sheet: 'P1_大型',   label: '大型' },
  { id: 'p1_large2', plan: 'P1', kind: '大型2',  sheet: 'P1_大型2',  label: '大型2' },
  { id: 'p2_normal', plan: 'P2', kind: '通常',   sheet: 'P2_通常',   label: '通常プラン2' },
  { id: 'p2_danger', plan: 'P2', kind: '危険物', sheet: 'P2_危険物', label: '危険プラン2' },
  { id: 'p2_large',  plan: 'P2', kind: '大型',   sheet: 'P2_大型',   label: '大型プラン2' },
  { id: 'p2_large2', plan: 'P2', kind: '大型2',  sheet: 'P2_大型2',  label: '大型2プラン2' },
];

// 複数CSVアップロード。サイズ・ファイル数を制限 (Codex #7)。
const pickingUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 8 * 1024 * 1024, files: 12 },
});
const PICKING_UPLOAD_FIELDS = [
  ...PLAN_SLOTS.map(s => ({ name: s.id, maxCount: 1 })),
  { name: 'lz', maxCount: 1 },
  { name: 'tmp1', maxCount: 1 }, // ロジザード トータルピッキングリストPDF (任意・納品プランNo注番用)
];
// 1ファイルあたりの最大行数 (DB肥大化・イベントループ停止の防止, Codex #3)。
const MAX_LZ_ROWS = 50000;
const MAX_PLAN_ROWS = 20000;

// 生成したラベルCSVを固定名で上書き保存する共有ドライブのフォルダ (GAS PL_FBA_NOUHIN_* 相当)。
const FBA_NOUHIN_DRIVE_FOLDER_ID = process.env.FBA_NOUHIN_DRIVE_FOLDER_ID || '17SRNd4yOEX3Mr8aCEgkyXgOz5cvBcwK7';
// 固定名にはロケ/数量/残数/期限入りの10列シールCSVを保存する
// (P-touch新テンプレート実機検証済み・2026-08-11に旧5列から一本化。中原さん承認)。
const FBA_NOUHIN_CSV_NAME = 'fbanouhinbangoulist.csv';

// multer のエラー (サイズ超過/ファイル数超過) を JSON で返すラッパー (Codex #3)。
function runUpload(mw) {
  return (req, res, next) => mw(req, res, (err) => {
    if (err) {
      const status = err.code === 'LIMIT_FILE_SIZE' ? 413 : 400;
      return res.status(status).json({ error: `アップロードエラー: ${err.message}` });
    }
    next();
  });
}

// 正規化 norm キーで mapping を引けるよう Map 化。空なら fail-closed (Codex #2)。
function buildMappingMap() {
  const mappings = getSkuMappings();
  const map = new Map();
  for (const m of mappings) map.set(pp.normSku(m.amazon_sku), m);
  return map;
}

// 画面
router.get('/picking-prep', (req, res) => {
  res.render('fba-picking-prep', {
    title: 'FBA納品ピッキング準備',
    username: req.session?.email,
    displayName: req.session?.displayName,
    slots: PLAN_SLOTS,
    mappingSource: getSkuMappingSourceMode(),
  });
});

// マスタ状態
router.get('/api/picking-prep/master-status', (req, res) => {
  try {
    res.json({ ...getPickingMasterStatus(), mappingSource: getSkuMappingSourceMode() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// バーコードは専用マスタを廃止し、FBA補充 Step2 のロジザード在庫(warehouse_inventory.barcode)
// から商品コードで引く。アップロード口は持たない。

// 土台商品マスタ取込: FBA土台商品管理シート(スプレッドシート)を直読みして picking_dodai_master に同期。
// 手動ボタン用。毎朝の cron でも自動同期される。0件(列ずれ/権限)時は既存を保持して例外。
router.post('/api/picking-prep/sync-dodai', async (req, res) => {
  try {
    const force = req.query.force === '1' || req.query.force === 'true';
    const r = await syncDodaiMaster({ force });
    res.json({ success: true, ...r });
  } catch (e) {
    // 急減ガード: 既存を保持したまま承認を求める (UI が force=1 で再実行)
    if (e.needConfirm) {
      return res.status(409).json({ needConfirm: true, prev: e.prev, count: e.count, message: e.message });
    }
    console.error('[Picking] 土台シート取込エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// メイン処理: プランCSV群 + lzpickinglist を突合し、ピッキングリスト/ラベルCSV/プラン別シートを生成
router.post('/api/picking-prep/process', runUpload(pickingUpload.fields(PICKING_UPLOAD_FIELDS)), async (req, res) => {
  try {
    const files = req.files || {};
    const lzFile = files.lz?.[0];
    if (!lzFile) return res.status(400).json({ error: 'ピッキングリスト(lzpickinglist)CSV が必要です' });

    // ⑤ 納品予定日 (YYYY-MM-DD) は必須。実在日付のみ受理 (Notionカード名・公開ナビ表示に使用)。
    const deliveryDateRaw = String(req.body?.delivery_date || '').trim();
    const deliveryDate = pp.isValidDateYmd(deliveryDateRaw) ? deliveryDateRaw : null;
    if (!deliveryDate) return res.status(400).json({ error: '納品予定日を正しい日付 (YYYY-MM-DD) で入力してください' });

    // ④ トータルピッキングリストPDF (TMP1) は必須。
    if (!files.tmp1?.[0]) return res.status(400).json({ error: 'トータルピッキングリストPDF(TMP1)をアップロードしてください' });

    // ① FBA納品プラン1/2 URL は必須。new URL で http(s)+ホスト名を厳密検証 (不正URLは Notion /pages 400 の原因)。
    const httpUrl = (v) => {
      const s = String(v || '').trim();
      if (!s) return null;
      try { const u = new URL(s); return (u.protocol === 'http:' || u.protocol === 'https:') && u.hostname ? s : null; }
      catch { return null; }
    };
    const plan1Url = httpUrl(req.body?.plan1_url);
    const plan2Url = httpUrl(req.body?.plan2_url);
    if (!plan1Url || !plan2Url) {
      return res.status(400).json({ error: 'FBA納品プラン1・プラン2のURL(http/https)を入力してください' });
    }

    // マッピング (fail-closed)
    let mappingMap;
    try {
      mappingMap = buildMappingMap();
    } catch (e) {
      return res.status(503).json({ error: `SKUマッピングを読み込めません (source=${getSkuMappingSourceMode()}): ${e.message}` });
    }
    if (mappingMap.size === 0) {
      return res.status(503).json({ error: `SKUマッピングが空です (source=${getSkuMappingSourceMode()})。誤出力防止のため処理を中止しました。` });
    }

    // 土台商品セット
    const dodaiSet = new Set(getDodaiMaster().map(d => pp.normSku(d.sku)));
    // バーコード Map (normCode キー)。FBA補充 Step2 のロジザード在庫から取得。
    const barcodeMap = new Map();
    for (const b of getWarehouseBarcodeRows()) barcodeMap.set(pp.normCode(b.logizard_code), b.barcode || '');

    // 各プランスロットをパース
    const allPlanItems = [];
    const planSheets = [];
    const planFileMeta = [];
    let usedSlots = 0;
    for (const slot of PLAN_SLOTS) {
      const f = files[slot.id]?.[0];
      if (!f) continue;
      usedSlots++;
      const labelPrefix = String(req.body?.[`label_${slot.id}`] || slot.label).trim() || slot.label;
      const { text } = decodeCsvBuffer(f.buffer);
      const rows = parseCsv(text);
      if (rows.length > MAX_PLAN_ROWS) {
        return res.status(413).json({ error: `${slot.plan}/${slot.kind} のCSV行数が上限(${MAX_PLAN_ROWS})を超えています (${rows.length}行)` });
      }
      const { items } = pp.parsePlanFile(labelPrefix, rows, dodaiSet);
      allPlanItems.push(...items);
      planSheets.push({
        slotId: slot.id, sheet: slot.sheet, plan: slot.plan, kind: slot.kind,
        label: labelPrefix, filename: f.originalname,
        rows: pp.buildPlanSheet(items, mappingMap),
      });
      planFileMeta.push({ slot: slot.id, sheet: slot.sheet, label: labelPrefix, filename: f.originalname, count: items.length });
    }
    if (usedSlots === 0) return res.status(400).json({ error: 'プランCSVが1つも指定されていません' });

    // 誤出力防止: プランから商品行を1つも抽出できない場合は中止 (Codex #2)
    if (allPlanItems.length === 0) {
      return res.status(422).json({ error: 'プランCSVから商品行(SKU)を抽出できませんでした。CSVの形式(7行目からデータ・A列=SKU)を確認してください。' });
    }

    // SKU → 商品コード展開
    const { codeIndex, warnings: convWarn } = pp.expandToCodes(allPlanItems, mappingMap);
    // 全SKUが変換不能なら中止 (Codex #2)
    if (codeIndex.size === 0) {
      return res.status(422).json({ error: '商品コードに変換できたSKUがありません（全件マッピングなし）。SKUマスタ・プランCSVを確認してください。', warnings: convWarn });
    }

    // lzpickinglist 突合
    const { text: lzText } = decodeCsvBuffer(lzFile.buffer);
    const lzRows = parseCsv(lzText);
    if (lzRows.length > MAX_LZ_ROWS) {
      return res.status(413).json({ error: `ピッキングリストCSVの行数が上限(${MAX_LZ_ROWS})を超えています (${lzRows.length}行)` });
    }
    const { rows: pickingRows, warnings: pickWarn, matchedCodes } = pp.buildPickingList(lzRows, codeIndex);

    // 誤出力防止: プランとピッキングリストが1件も突合しない場合は中止 (Codex #2)。
    // (lz/プランの取り違え・別日のファイル等。プランNo空のピッキングリストは無意味)
    const matchedRowCount = pickingRows.filter(r => r.planNo).length;
    if (matchedRowCount === 0) {
      return res.status(422).json({
        error: 'プランの商品コードがピッキングリストと1件も一致しませんでした。ファイルの取り違え（別日のlzpickinglist等）の可能性があります。',
        warnings: [...convWarn, ...pickWarn],
      });
    }

    // ---- TMP1 PDF 構造化抽出 (Python spawn 1回目: 抽出のみ) ----
    // 残数は lz CSV に存在しないため PDF 抽出が唯一の取得元。構造検証 NG は 422。
    const tmp1File = files.tmp1[0];
    let pdfExtract;
    try {
      pdfExtract = await extractPickingPdf(tmp1File.buffer);
    } catch (e) {
      console.error('[Picking] TMP1 PDF抽出失敗:', e.message);
      return res.status(422).json({
        error: `TMP1 PDFを解析できませんでした: ${e.message}`,
        warnings: [...convWarn, ...pickWarn],
      });
    }

    // ---- PDF × lz CSV 突合 (総行数 → キー毎件数 → FIFO+数量一致 の fail-closed) ----
    // シール枚数 = ピッキングリスト行数 の絶対条件をここで担保する。
    const recon = pp.reconcilePdfWithLz(pdfExtract.items, pickingRows);
    if (recon.errors.length) {
      console.error('[Picking] PDF×CSV突合エラー:', JSON.stringify({
        lzFile: lzFile.originalname, tmp1File: tmp1File.originalname, errors: recon.errors.slice(0, 30),
      }));
      return res.status(422).json({
        error: 'TMP1 PDFとピッキングリストCSVの突合に失敗しました。別の回のファイルが混ざっていないか確認してください。',
        warnings: [...recon.errors, ...convWarn, ...pickWarn],
      });
    }
    const mergedRows = recon.rows;

    // P-touch ラベル行 (10列: ロケ/数量/残数/期限入り。旧5列は2026-08-11に廃止)
    const v2 = pp.buildLabelRowsV2(mergedRows, barcodeMap);
    // 期限は空欄なら警告継続だが、非空の不正値は元データ破損の可能性があるため 422 (Codex R2)
    if (v2.expiryErrors.length) {
      console.error('[Picking] 有効期限不正:', JSON.stringify(v2.expiryErrors.slice(0, 30)));
      return res.status(422).json({ error: '有効期限に不正な値の行があります。lzpickinglist.csv を確認してください。', warnings: v2.expiryErrors });
    }
    // 最終アサート: v2シール枚数 = ピッキングリスト行数 (絶対条件)
    if (v2.csvRows.length - 1 !== pickingRows.length) {
      console.error(`[Picking] 整合性エラー: v2=${v2.csvRows.length - 1} lz=${pickingRows.length}`);
      return res.status(500).json({ error: `内部整合性エラー: シール行数(${v2.csvRows.length - 1})とピッキング行数(${pickingRows.length})が一致しません` });
    }

    // ---- 注番PDF生成 (Python spawn 2回目)。検証工程の一部なので失敗は保存前に 422 ----
    // 割り当ては page/item 単位の専用JSON (商品ID辞書だと同一IDの上書き事故があるため)。
    const planItems = mergedRows.map(r => {
      const planNo = String(r.planNo || '').split('\n').map(p => p.trim()).filter(Boolean).join(' / ');
      return {
        page: r.pdfPage, item: r.pdfItem, productId: r.code,
        planNo: planNo || 'プランなし',
        status: planNo ? 'matched' : 'no-plan',
      };
    });
    let annotate;
    let annotatedPdfBuffer;
    try {
      const r = await annotatePickingPdf(tmp1File.buffer, planItems);
      annotatedPdfBuffer = r.pdfBuffer;
      annotate = { attempted: true, ok: true, matched: r.matched, total: r.total, unmatchedCount: r.total - r.matched };
    } catch (e) {
      console.error('[Picking] TMP1 PDF注番失敗:', e.message);
      return res.status(422).json({ error: `納品プランNo注番PDFの生成に失敗しました: ${e.message}` });
    }

    // 展開したが倉庫ピッキングに出てこなかった商品コード
    const notInPicking = pp.findCodesNotInPicking(codeIndex, matchedCodes);
    const notInPickingWarn = notInPicking.length
      ? [`プランにあるが倉庫ピッキングリストに無い商品コード: ${notInPicking.length}件 (${notInPicking.slice(0, 10).map(x => x.code).join(', ')}${notInPicking.length > 10 ? ' …' : ''})`]
      : [];

    const warnings = [...convWarn, ...pickWarn, ...v2.warnings, ...notInPickingWarn];
    const summary = {
      planFiles: planFileMeta,
      planItemCount: allPlanItems.length,
      pickingRowCount: pickingRows.length,
      pickingMatchedCount: matchedRowCount,
      labelRowCount: v2.csvRows.length - 1,
      labelV2RowCount: v2.csvRows.length - 1,
      planlessCount: v2.planlessCount,
      unallocatedCount: v2.unallocatedCount,
      pdfItemCount: pdfExtract.total,
      codeNotInPickingCount: notInPicking.length,
      mappingSource: getSkuMappingSourceMode(),
    };
    const result = { pickingRows: mergedRows, planSheets, labelCsvRowsV2: v2.csvRows, notInPicking };

    // ---- ここから永続化。全検証 (抽出/突合/数量/期限/注番) が成功した後にのみ実行する ----
    // (422 時に履歴・Drive・PDF・Notion を一切更新しないため。Codex R1 #1)
    const runId = savePickingRun({
      run_by: req.session?.email,
      plan_files: planFileMeta,
      lz_filename: lzFile.originalname,
      picking_count: pickingRows.length,
      label_count: v2.csvRows.length - 1, // 絶対条件を満たす件数 (= lz行数) を履歴の正とする
      plan_sheet_count: planSheets.length,
      warning_count: warnings.length,
      summary,
      warnings,
      result,
      delivery_date: deliveryDate,
    });

    // PDF保存失敗時は履歴をロールバックして500 (PDFなしの不完全な回を公開一覧に残さない)。
    // ディスク障害等でロールバック自体も失敗した場合は応答に明示する (再起動後に
    // 履歴だけ復活しうるため、運用者が実行ID付きで判別できるように)。
    try {
      savePickingPdf(runId, annotatedPdfBuffer);
    } catch (e) {
      console.error('[Picking] 注番PDF保存失敗 — 履歴をロールバック:', e);
      let rollbackFailed = false;
      try { deletePickingRun(runId); } catch (e2) {
        rollbackFailed = true;
        console.error('[Picking] 履歴ロールバック失敗:', e2);
      }
      return res.status(500).json({
        error: `注番PDFの保存に失敗しました: ${e.message}`
          + (rollbackFailed ? ` (さらに実行履歴ID ${runId} のロールバックにも失敗 — PDFなしの履歴が残っている可能性があります)` : ''),
        rollbackFailed,
        runId: rollbackFailed ? runId : undefined,
      });
    }

    // ラベルCSV(P-touch, 10列)を固定名で共有ドライブに上書き保存 (GAS同等)。ここは外部保存のため
    // best-effort: 失敗しても処理は成功扱いで履歴は残す。UIメッセージで保存可否を伝える。
    // ※ 移行期の並行保存ファイル fbanouhinbangoulist_v2.csv は更新を停止済み (Drive上の残骸は削除してよい)。
    let driveSave;
    try {
      const buf = buildShiftJisCsv(v2.csvRows, { guardFormula: false });
      const up = await uploadCsvToDrive(buf, FBA_NOUHIN_CSV_NAME, FBA_NOUHIN_DRIVE_FOLDER_ID);
      driveSave = { attempted: true, saved: true, action: up.action, filename: FBA_NOUHIN_CSV_NAME };
    } catch (e) {
      console.error(`[Picking] 共有ドライブへのCSV保存失敗 (${FBA_NOUHIN_CSV_NAME}):`, e);
      driveSave = { attempted: true, saved: false, filename: FBA_NOUHIN_CSV_NAME, error: e.message };
    }

    // 納品予定日(必須)で Notion カードを作成し、注番済みPDF(公開URL)を添付 (best-effort)。
    let notion = { attempted: false };
    if (deliveryDate) {
      const title = pp.buildPickingCardTitle(deliveryDate); // 例: 6月27日納品予定FBA納品ピッキング
      if (!notionConfigured()) {
        notion = { attempted: true, ok: false, skipped: true, error: 'Notion未設定 (FBA_PICKING_NOTION_TOKEN)' };
      } else {
        notion = { attempted: true, ok: false, title };
        try {
          // Notionに永続化する公開URLは未検証の Host ヘッダーから作らない:
          // PUBLIC_BASE_URL を最優先し、無い場合は既知ホスト (onrender.com / localhost) のみ許可。
          const hostRaw = String(req.get('host') || '');
          const hostOk = /^(localhost(:\d+)?|127\.0\.0\.1(:\d+)?|[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.onrender\.com)$/.test(hostRaw);
          const base = process.env.PUBLIC_BASE_URL || (hostOk ? `${req.protocol}://${hostRaw}` : null);
          const pdfUrl = (annotate.ok && base) ? `${base}/print/picking/${runId}/pdf` : null;
          // ① FBA納品プランURL (冒頭で必須検証済み) を Notion URL_1/URL_2 に設定。
          // 未引当(ZZZ)・プランNo未解決はカード本文にも件数を残す (現場が後から追跡できるように)。
          const noteLines = [];
          if (v2.unallocatedCount) noteLines.push(`⚠ ロケ未引当(ZZZ)行: ${v2.unallocatedCount}件 (シールCSV末尾)`);
          if (v2.planlessCount) noteLines.push(`⚠ 納品プランNo未解決行: ${v2.planlessCount}件 (「プランなし」と印字)`);
          const card = await createPickingCard({ title, pdfUrl, plan1Url, plan2Url, noteLines });
          notion = { attempted: true, ok: true, title, url: card.url, attached: !!pdfUrl, statusSet: card.statusSet };
        } catch (e) {
          console.error('[Picking] Notionカード作成失敗:', e);
          notion = { attempted: true, ok: false, title, error: e.message };
        }
      }
    }

    // 箱詰め記録 (fba-box) の納品回を自動作成 — いろはの iPad にすぐ出る (Excel は本社が後から添付)。
    // best-effort: 失敗しても picking-prep の結果は成功扱い (iPad の「作業開始」からも作れる)
    let boxRun = { attempted: true, ok: false };
    try {
      boxRun = { attempted: true, ...createBoxRunFromPicking({ pickingRun: { id: runId, delivery_date: deliveryDate, run_at: null }, planSheets, createdBy: req.session?.email, activate: true }) };
      if (boxRun.ok) ensureBoxRunImages(boxRun.runId).catch((e) => console.warn('[Picking] 箱詰め記録の商品画像取得 (best-effort):', e.message));
    } catch (e) {
      console.error('[Picking] 箱詰め記録の納品回作成に失敗 (best-effort):', e);
      boxRun = { attempted: true, ok: false, error: e.message };
    }

    res.json({ success: true, runId, summary, warnings, driveSave, annotate, notion, boxRun, ...result });
  } catch (e) {
    console.error('[Picking] 処理エラー:', e);
    res.status(500).json({ error: e.message });
  }
});

// 実行履歴一覧
router.get('/api/picking-prep/runs', (req, res) => {
  try {
    const runs = getPickingRuns(30).map(r => ({
      ...r,
      plan_files: safeJsonParse(r.plan_files, []),
    }));
    res.json({ runs });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 実行結果 (JSON)
router.get('/api/picking-prep/run/:id', (req, res) => {
  const rec = getPickingRun(parseInt(req.params.id));
  if (!rec) return res.status(404).json({ error: '履歴が見つかりません' });
  res.json({
    id: rec.id, run_at: rec.run_at, run_by: rec.run_by,
    summary: safeJsonParse(rec.summary, {}),
    warnings: safeJsonParse(rec.warnings, []),
    ...safeJsonParse(rec.result, {}),
  });
});

// 旧5列ラベルCSV ダウンロード (2026-08-11に10列へ一本化。旧5列を保存している過去実行のみ)
router.get('/api/picking-prep/run/:id/label-csv', (req, res) => {
  const rec = getPickingRun(parseInt(req.params.id));
  if (!rec) return res.status(404).json({ error: '履歴が見つかりません' });
  const result = safeJsonParse(rec.result, {});
  if (!Array.isArray(result.labelCsvRows) || result.labelCsvRows.length === 0) {
    return res.status(404).json({ error: '旧5列CSVはこの実行にはありません (10列版は label-csv-v2 から)' });
  }
  // P-touch ラベル CSV: 式インジェクションガードは付けない (P-touch は式評価せず、' 前置は印字を壊す)
  const buf = buildShiftJisCsv(result.labelCsvRows, { guardFormula: false });
  res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
  res.setHeader('Content-Disposition', `attachment; filename="fbanouhinbangoulist_${rec.id}.csv"`);
  res.send(buf);
});

// v2 ラベルCSV (10列: ロケ/数量/残数/期限入り) ダウンロード
router.get('/api/picking-prep/run/:id/label-csv-v2', (req, res) => {
  const rec = getPickingRun(parseInt(req.params.id));
  if (!rec) return res.status(404).json({ error: '履歴が見つかりません' });
  const result = safeJsonParse(rec.result, {});
  if (!Array.isArray(result.labelCsvRowsV2) || result.labelCsvRowsV2.length === 0) {
    return res.status(404).json({ error: 'この実行には v2 ラベルCSVがありません (v2対応前の実行)' });
  }
  const buf = buildShiftJisCsv(result.labelCsvRowsV2, { guardFormula: false });
  res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
  res.setHeader('Content-Disposition', `attachment; filename="fbanouhinbangoulist_v2_${rec.id}.csv"`);
  res.send(buf);
});

// 納品プランNo注番済み TMP1 PDF ダウンロード (生成済みのみ)
router.get('/api/picking-prep/run/:id/annotated-pdf', (req, res) => {
  if (!/^\d+$/.test(req.params.id)) return res.status(404).json({ error: '無効なIDです' });
  const p = pickingPdfPath(req.params.id);
  if (!p) return res.status(404).json({ error: '注番済みPDFがありません (この回はTMP1未指定か生成失敗)' });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename="picking_list_planno_${String(req.params.id).replace(/[^0-9]/g,'')}.pdf"`);
  res.sendFile(p);
});

// 印刷ビュー (プラン別シートのみ。ピッキングリストPDFは廃止=ロジザード側PDFを使うため)
router.get('/picking-prep/print/:id', (req, res) => {
  const rec = getPickingRun(parseInt(req.params.id));
  if (!rec) return res.status(404).send('履歴が見つかりません');
  const result = safeJsonParse(rec.result, {});
  res.render('fba-picking-print', {
    title: 'FBA納品 プラン別シート印刷',
    runId: rec.id,
    runAt: rec.run_at,
    planSheets: result.planSheets || [],
    isPublic: false,
  });
});

// ==========================================
// FBA納品実績
//   日別に「何SKU・何個のプランを作ったか」を集計する画面。
//   データは SP-API Fulfillment Inbound v0 から。取得はミニPC、ここは引き取りと表示。
// ==========================================

const JST_DAY_MS = 86400000;
function jstToday() {
  return new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
function jstDaysAgo(days) {
  return new Date(Date.now() + 9 * 3600 * 1000 - days * JST_DAY_MS).toISOString().slice(0, 10);
}

// ミニPC側の取込ジョブを起動 (完了は /api/jobs/:jobId でポーリング)
router.post('/api/inbound-history/sync', async (req, res) => {
  try {
    const result = await callMiniPC('/inbound-history/sync', {
      method: 'POST',
      body: {
        full: req.body?.full === true,
        allItems: req.body?.allItems === true,
        sinceDays: Number(req.body?.sinceDays) || 14,
        itemLimit: Number(req.body?.itemLimit) || 0,
      },
      timeout: 30000,
    });
    res.json(result);
  } catch (e) {
    console.error('[FBA] 納品実績の取込起動エラー:', e);
    res.status(502).json({ error: 'ミニPCへの接続に失敗: ' + e.message });
  }
});

/**
 * ミニPCから Render の fba.db へ引き取り。updated_at をカーソルに差分だけ取る。
 * 画面の「取込」ボタンと 06:00 の cron の両方から呼ぶ。
 */
const PULL_MAX_PAGES = 30;

async function pullInboundFromMiniPC(full = false) {
  let cursor = full ? null : getInboundSyncCursor();
  let shipments = 0;
  let items = 0;
  let pages = 0;

  // 1ページ3000件。2年分でも数ページで終わる。上限は暴走防止。
  for (let i = 0; i <= PULL_MAX_PAGES; i++) {
    if (i === PULL_MAX_PAGES) {
      // 打ち切って正常終了すると「全部取れた」ように見えてしまう。取りこぼしは失敗として扱う。
      throw new Error(`引き取りがページ上限 ${PULL_MAX_PAGES} に到達しました (${shipments}件まで反映済み)。もう一度実行してください`);
    }
    const q = new URLSearchParams({ limit: '3000' });
    if (cursor?.updated_at) {
      q.set('sinceUpdatedAt', cursor.updated_at);
      q.set('sinceShipmentId', cursor.shipment_id || '');
    }
    const data = await callMiniPC(`/sync/inbound-history?${q.toString()}`, { timeout: 120000 });
    if (!data?.ok) {
      throw new Error('ミニPCからの取得に失敗: ' + (data?.message || JSON.stringify(data)));
    }
    const batch = data.shipments || [];
    if (batch.length === 0) break;

    // next_cursor も一緒に渡す。取り込みが成功したときだけチェックポイントが進む。
    const saved = importInboundRows({ shipments: batch, items: data.items || [], next_cursor: data.next_cursor });
    shipments += saved.shipments;
    items += saved.items;
    pages += 1;

    if (!data.has_more) break;
    const next = data.next_cursor;
    // カーソルが進まないのに続きがある = 取りこぼしが起きている。黙って止まらせない。
    if (!next?.updated_at ||
        (cursor && next.updated_at === cursor.updated_at && next.shipment_id === cursor.shipment_id)) {
      throw new Error('同期カーソルが進みませんでした。取りこぼしの恐れがあるため中断します');
    }
    cursor = next;
  }
  return { shipments, items, pages, status: getInboundSyncStatus() };
}

router.post('/api/inbound-history/pull', async (req, res) => {
  try {
    const result = await pullInboundFromMiniPC(req.body?.full === true);
    res.json({ ok: true, ...result });
  } catch (e) {
    console.error('[FBA] 納品実績の引き取りエラー:', e);
    res.status(502).json({ error: '引き取りに失敗: ' + e.message });
  }
});

/**
 * 日次同期 (06:00 JST の cron から呼ぶ)。
 * ミニPCで差分取込 → 完了を待つ → Render へ引き取り、までを1本で。
 * 差分は直近14日 + 明細400件までに制限してあるので、通常は数分で終わる。
 */
async function runInboundHistoryDailySync() {
  const start = await callMiniPC('/inbound-history/sync', {
    method: 'POST',
    body: { sinceDays: 14, itemLimit: 400 },
    timeout: 30000,
  });
  if (start?.status === 'already_running') {
    console.log('[FBA-Cron] 納品実績: ミニPC側で実行中のため今回はpullのみ');
  } else if (!start?.jobId) {
    throw new Error('取込ジョブの起動に失敗: ' + JSON.stringify(start));
  } else {
    const jobId = start.jobId;
    const deadline = Date.now() + 25 * 60 * 1000;
    let done = false;
    while (Date.now() < deadline) {
      await new Promise(r => setTimeout(r, 10000));
      let job;
      try {
        const resp = await fetch(`${WAREHOUSE_URL}/service-api/jobs/${jobId}`, {
          headers: getServiceHeaders(),
          signal: AbortSignal.timeout(15000),
        });
        job = await resp.json();
      } catch {
        continue; // 一時的な通信断は次の周期で再確認
      }
      if (job.status === 'completed') { done = true; break; }
      if (job.status === 'failed') throw new Error('ミニPC側のジョブが失敗: ' + (job.error || ''));
    }
    if (!done) throw new Error('取込ジョブがタイムアウト (25分)');
  }
  return pullInboundFromMiniPC(false);
}

// 日別 / 月別サマリ
router.get('/api/inbound-history/summary', (req, res) => {
  const unit = req.query.unit === 'month' ? 'month' : 'day';
  const opts = {
    from: req.query.from || null,
    to: req.query.to || null,
    includeCancelled: req.query.includeCancelled === '1',
  };
  const rows = unit === 'month' ? getInboundMonthlySummary(opts) : getInboundDailySummary(opts);
  res.json({ unit, count: rows.length, data: rows, status: getInboundSyncStatus() });
});

// 指定日のシップメント一覧 (日別サマリのドリルダウン)
// includeCancelled はサマリ側と揃える (揃えないと内訳の合計がサマリと合わない)
router.get('/api/inbound-history/shipments', (req, res) => {
  const date = req.query.date;
  if (!date) return res.status(400).json({ error: 'date が必要です' });
  const shipments = getInboundShipmentsByDate(date, { includeCancelled: req.query.includeCancelled === '1' });
  res.json({ date, count: shipments.length, data: shipments });
});

// 1シップメントの明細
router.get('/api/inbound-history/items/:shipmentId', (req, res) => {
  res.json({ shipment_id: req.params.shipmentId, data: getInboundItems(req.params.shipmentId) });
});

// 未受領一覧 (問い合わせ用)。期間はサマリと揃える (揃えないと2年前の分まで並ぶ)
router.get('/api/inbound-history/unreceived', (req, res) => {
  const minDays = Number(req.query.minDays) || 0;
  const rows = getInboundUnreceived({
    minDays,
    from: req.query.from || null,
    to: req.query.to || null,
  });
  res.json({ count: rows.length, data: rows });
});

// 取込状況 + 作成日が取れなかったシップメント
router.get('/api/inbound-history/status', (req, res) => {
  res.json({ status: getInboundSyncStatus(), no_date: getInboundShipmentsWithoutDate() });
});

// 画面
router.get('/inbound-history', (req, res) => {
  res.render('fba-inbound-history', {
    title: 'FBA納品実績',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// 印刷 (日別サマリ + 未受領の2部構成)
router.get('/inbound-history/print', (req, res) => {
  const from = req.query.from || jstDaysAgo(30);
  const to = req.query.to || jstToday();
  const unit = req.query.unit === 'month' ? 'month' : 'day';
  const includeCancelled = req.query.includeCancelled === '1';
  const withUnreceived = req.query.unreceived !== '0';
  const minDays = Number(req.query.minDays) || 0;

  const summary = unit === 'month'
    ? getInboundMonthlySummary({ from, to, includeCancelled })
    : getInboundDailySummary({ from, to, includeCancelled });

  const unreceived = withUnreceived ? getInboundUnreceived({ minDays, from, to }) : [];

  const totals = summary.reduce((a, r) => ({
    shipment_count: a.shipment_count + (r.shipment_count || 0),
    qty_shipped: a.qty_shipped + (r.qty_shipped || 0),
    qty_received: a.qty_received + (r.qty_received || 0),
    qty_unreceived: a.qty_unreceived + (r.qty_unreceived || 0),
  }), { shipment_count: 0, qty_shipped: 0, qty_received: 0, qty_unreceived: 0 });

  res.render('fba-inbound-history-print', {
    title: `FBA納品実績 ${from} 〜 ${to}`,
    from, to, unit, includeCancelled, minDays,
    summary, unreceived, totals,
    chart: buildInboundChart(summary, unit),
    showChart: req.query.chart !== '0',
    printedAt: new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 16).replace('T', ' '),
  });
});

function safeJsonParse(s, fallback) {
  try { return s ? JSON.parse(s) : fallback; } catch { return fallback; }
}

// multer のサイズ超過等をJSONで返す (既定だとHTMLエラーページになりフロントのd.errorが拾えない)
router.use((err, req, res, next) => {
  if (err && err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ error: 'ファイルが大きすぎます (上限20MB)。正しいCSVか確認してください' });
  }
  next(err);
});

export default router;
