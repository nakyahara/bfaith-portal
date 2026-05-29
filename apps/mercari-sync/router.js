/**
 * 楽天→メルカリShops 商品登録ツール - Express Router
 * Python FastAPI版からの移植
 */
import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import archiver from 'archiver';
import crypto from 'crypto';

import * as settingsDb from './settings-db.js';
import * as rakutenLocal from './rakuten.js';
import * as mapper from './mapper.js';
import * as csvExporter from './csv-exporter.js';

// --- ミニPC経由で楽天RMS API実行 ---
const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}
// details-bulk は楽天 RMS が 1req/sec のレート制限のため
// 50件 chunk × 1.1秒 = 55秒/chunk、Cloudflare Access の 100秒 idle timeout 内に収める
// (100件にすると120秒かかって Cloudflare が無音で connection を切る)
const DETAILS_BULK_CHUNK_SIZE = 50;
const DETAILS_BULK_CHUNK_TIMEOUT_MS = 90_000;
// all-codes は 100ページ × 1.1秒 = 最大 110 秒 だが cold cache を許容するため 300秒
// (Render → miniPC では Cloudflare Tunnel が長時間 OK のはず、cache 命中なら即返答)
const ALL_CODES_TIMEOUT_MS = 300_000;

const rakuten = {
  async getAllItemCodes() {
    const res = await fetch(`${WAREHOUSE_URL}/service-api/rakuten-rms/items/all-codes`, { headers: getServiceHeaders(), signal: AbortSignal.timeout(ALL_CODES_TIMEOUT_MS) });
    const data = await res.json();
    if (!data.ok) throw new Error(data.message || 'RMS API error');
    return data.mapping;
  },
  async getItemDetailsBulk(serviceSecret, licenseKey, itemCodes) {
    const allItems = [];
    for (let i = 0; i < itemCodes.length; i += DETAILS_BULK_CHUNK_SIZE) {
      const chunk = itemCodes.slice(i, i + DETAILS_BULK_CHUNK_SIZE);
      const res = await fetch(`${WAREHOUSE_URL}/service-api/rakuten-rms/items/details-bulk`, {
        method: 'POST',
        headers: getServiceHeaders(),
        body: JSON.stringify({ itemCodes: chunk }),
        signal: AbortSignal.timeout(DETAILS_BULK_CHUNK_TIMEOUT_MS),
      });
      const data = await res.json();
      if (!data.ok) throw new Error(data.message || 'RMS API error');
      if (Array.isArray(data.items)) allItems.push(...data.items);
    }
    return allItems;
  },
};

// =========================================================================
// 差分チェックを非同期ジョブ + polling 化する仕組み
//
// 同期 HTTP で 6 分待つと Cloudflare の Proxy Read Timeout (120秒) に引っかかって
// ブラウザに応答が届かない。POST で job を kick して即 jobId を返し、
// フロントは GET /api/check-csv-status/:jobId で 3 秒間隔 polling する設計。
// =========================================================================
const checkCsvJobs = new Map(); // jobId -> { status, progress, result, error, createdAt, updatedAt, lastAccessedAt }
const JOB_TTL_DONE_MS = 30 * 60 * 1000; // done/error は 30 分保持
const JOB_TTL_RUNNING_MS = 30 * 60 * 1000; // running も 30 分上限 (本来 6 分で終わるべき)
const JOB_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;
const JOB_MAX_TOTAL = 50;
const JOB_MAX_CONCURRENT_RUNNING = 3;

let _checkCsvCleanupInterval = null;
if (!_checkCsvCleanupInterval) {
  _checkCsvCleanupInterval = setInterval(() => {
    const now = Date.now();
    for (const [id, job] of checkCsvJobs) {
      const ttl = (job.status === 'running' || job.status === 'queued') ? JOB_TTL_RUNNING_MS : JOB_TTL_DONE_MS;
      const baseline = (job.status === 'running' || job.status === 'queued') ? job.updatedAt : job.lastAccessedAt;
      if (now - baseline > ttl) checkCsvJobs.delete(id);
    }
  }, JOB_CLEANUP_INTERVAL_MS);
  if (typeof _checkCsvCleanupInterval.unref === 'function') _checkCsvCleanupInterval.unref();
}

function genJobId() {
  return crypto.randomBytes(16).toString('hex');
}

// Rakuten RMS の variants はオブジェクト形式 {sku1: {price, hidden, ...}, sku2: {...}}
// hidden=true を除いた配列に正規化し、value 内 skuManageNumber 不在ならキーで補完する。
// (mapper.js / mercari-sync の集計で配列の前提のロジックが多いため一元化)
function normalizeVariants(variants) {
  if (Array.isArray(variants)) {
    return variants.filter(v => v && !v.hidden);
  }
  if (variants && typeof variants === 'object') {
    return Object.entries(variants)
      .filter(([, v]) => v && !v.hidden)
      .map(([k, v]) => ({ ...v, skuManageNumber: v.skuManageNumber || k }));
  }
  return [];
}

function countRunningJobs() {
  let n = 0;
  for (const j of checkCsvJobs.values()) {
    if (j.status === 'running' || j.status === 'queued') n++;
  }
  return n;
}

function updateJobProgress(job, patch) {
  Object.assign(job.progress, patch);
  job.updatedAt = Date.now();
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();
const upload = multer({ storage: multer.memoryStorage() });

// DB初期化
let dbReady = false;
async function ensureDb() {
  if (!dbReady) {
    await settingsDb.initDb();
    dbReady = true;
  }
}

// EJSビューのレンダリングヘルパー
function renderView(res, viewName, data = {}) {
  res.render(path.join(__dirname, 'views', viewName), data);
}

// --- ページルート ---

router.get('/', async (req, res) => {
  await ensureDb();
  const mode = settingsDb.getOperationMode();
  const registered_count = settingsDb.getRegisteredItemCount();
  renderView(res, 'diff', { mode, registered_count });
});

router.get('/settings', async (req, res) => {
  await ensureDb();
  const cfg = settingsDb.getAllConfig();
  const category_mappings = settingsDb.getCategoryMappings();
  const saved = req.query.saved === '1';
  renderView(res, 'settings', { cfg, category_mappings, saved });
});

router.post('/settings', async (req, res) => {
  await ensureDb();
  const b = req.body;

  // 動作モード
  settingsDb.setConfig('operation_mode', (b.operation_mode || 'csv').trim());

  // 楽天RMS API
  settingsDb.setConfig('service_secret', (b.service_secret || '').trim());
  settingsDb.setConfig('license_key', (b.license_key || '').trim());
  settingsDb.setConfig('shop_url', (b.shop_url || '').trim());

  // CSV出力設定
  settingsDb.setConfig('shipping_method_code', (b.shipping_method_code || '3').trim());
  settingsDb.setConfig('shipping_from_area_code', (b.shipping_from_area_code || 'jp27').trim());
  settingsDb.setConfig('days_to_ship_code', (b.days_to_ship_code || '2').trim());
  settingsDb.setConfig('shipping_cost_burden', (b.shipping_cost_burden || '1').trim());
  settingsDb.setConfig('default_product_status', (b.default_product_status || '1').trim());

  // 除外設定
  settingsDb.setConfig('excluded_items', (b.excluded_items || '').trim());
  settingsDb.setConfig('excluded_image_positions', (b.excluded_image_positions || '').trim());
  settingsDb.setConfig('excluded_image_patterns', (b.excluded_image_patterns || '').trim());

  // メルカリShops API
  settingsDb.setConfig('mercari_token', (b.mercari_token || '').trim());
  settingsDb.setConfig('use_sandbox', b.use_sandbox === 'on' ? '1' : '0');
  settingsDb.setConfig('shipping_method', (b.shipping_method || '').trim());
  settingsDb.setConfig('shipping_from_area', (b.shipping_from_area || '').trim());
  settingsDb.setConfig('days_to_ship', (b.days_to_ship || '1').trim());

  // カテゴリ
  settingsDb.setConfig('default_mercari_category', (b.default_mercari_category || '').trim());
  try {
    const mappings = JSON.parse(b.category_mappings_json || '[]');
    settingsDb.saveCategoryMappings(mappings);
  } catch { /* ignore */ }

  res.redirect('/apps/mercari-sync/settings?saved=1');
});

// --- CSVモード用APIエンドポイント ---

router.post('/api/upload-mercari-csv', upload.single('file'), async (req, res) => {
  await ensureDb();
  if (!req.file || !req.file.buffer.length) {
    return res.json({ error: 'ファイルが空です', count: 0 });
  }
  try {
    const codes = csvExporter.parseMercariExportCsv(req.file.buffer);
    if (codes.size > 0) {
      settingsDb.addRegisteredItems(codes, 'mercari_csv');
    }
    const total = settingsDb.getRegisteredItemCount();
    res.json({ count: codes.size, total, message: `メルカリCSVから ${codes.size}件 を読み込みました（合計登録済み: ${total}件）`, error: null });
  } catch (e) {
    res.json({ error: `CSV読み込みエラー: ${e.message}`, count: 0 });
  }
});

router.post('/api/import-registered-csv', upload.single('file'), async (req, res) => {
  await ensureDb();
  if (!req.file || !req.file.buffer.length) {
    return res.json({ error: 'ファイルが空です', count: 0 });
  }
  try {
    const columnName = (req.body.column_name || '').trim();
    const { codes, messages } = csvExporter.parseGenericCsvForCodes(req.file.buffer, columnName);
    if (codes.size > 0) {
      settingsDb.addRegisteredItems(codes, 'csv_import');
    }
    const total = settingsDb.getRegisteredItemCount();
    res.json({ count: codes.size, total, messages, message: `${codes.size}件 を登録済みとして追加しました（合計: ${total}件）`, error: null });
  } catch (e) {
    res.json({ error: `CSV読み込みエラー: ${e.message}`, count: 0 });
  }
});

router.get('/api/registered-items/count', async (req, res) => {
  await ensureDb();
  res.json({ count: settingsDb.getRegisteredItemCount() });
});

router.post('/api/registered-items/clear', async (req, res) => {
  await ensureDb();
  settingsDb.clearRegisteredItems();
  res.json({ message: '登録済みデータをクリアしました', count: 0 });
});

// 差分チェック実体 (非同期ジョブで実行)
async function runCheckCsvJob(jobId) {
  const job = checkCsvJobs.get(jobId);
  if (!job) return;

  job.status = 'running';
  job.updatedAt = Date.now();
  updateJobProgress(job, { phase: 'all-codes', done: 0, total: 1, chunkIndex: 0, chunkTotal: 0 });

  await ensureDb();
  const cfg = settingsDb.getAllConfig();

  let rakutenMapping;
  try {
    rakutenMapping = await rakuten.getAllItemCodes();
    updateJobProgress(job, { done: 1 });
  } catch (e) {
    console.error(`[mercari-sync] check-csv job ${jobId} all-codes error:`, e);
    job.status = 'error';
    job.error = `楽天RMS API エラー: ${e.message}`;
    job.updatedAt = Date.now();
    return;
  }

  let rakutenItemNumbers = new Set(Object.keys(rakutenMapping));
  const registeredCodes = settingsDb.getRegisteredItems();
  const excludedItems = settingsDb.getExcludedItems();

  for (const ex of excludedItems) rakutenItemNumbers.delete(ex);

  let diffItemNumbers;
  if (registeredCodes.size > 0) {
    diffItemNumbers = new Set();
    for (const itemNum of rakutenItemNumbers) {
      const hasMatch = [...registeredCodes].some(rc => rc.startsWith(itemNum) || rc === itemNum);
      if (!hasMatch) diffItemNumbers.add(itemNum);
    }
  } else {
    diffItemNumbers = new Set(rakutenItemNumbers);
  }

  const sortedDiff = [...diffItemNumbers].sort();
  const apiCodes = sortedDiff.map(n => rakutenMapping[n] || n);
  const chunkTotal = Math.ceil(apiCodes.length / DETAILS_BULK_CHUNK_SIZE);

  updateJobProgress(job, { phase: 'details', done: 0, total: apiCodes.length, chunkIndex: 0, chunkTotal });

  // chunk loop + chunk-by-chunk progress 更新 (Codex medium #6)
  const details = [];
  for (let i = 0; i < apiCodes.length; i += DETAILS_BULK_CHUNK_SIZE) {
    const chunk = apiCodes.slice(i, i + DETAILS_BULK_CHUNK_SIZE);
    updateJobProgress(job, { chunkIndex: Math.floor(i / DETAILS_BULK_CHUNK_SIZE) + 1 });
    try {
      const r = await fetch(`${WAREHOUSE_URL}/service-api/rakuten-rms/items/details-bulk`, {
        method: 'POST',
        headers: getServiceHeaders(),
        body: JSON.stringify({ itemCodes: chunk }),
        signal: AbortSignal.timeout(DETAILS_BULK_CHUNK_TIMEOUT_MS),
      });
      const data = await r.json();
      if (!data.ok) throw new Error(data.message || 'RMS API error');
      if (Array.isArray(data.items)) details.push(...data.items);
      updateJobProgress(job, { done: Math.min(i + DETAILS_BULK_CHUNK_SIZE, apiCodes.length) });
    } catch (e) {
      console.error(`[mercari-sync] check-csv job ${jobId} details-bulk chunk error:`, e);
      job.status = 'error';
      job.error = `楽天RMS API エラー（詳細取得）: ${e.message}`;
      job.updatedAt = Date.now();
      return;
    }
  }

  const detailMap = {};
  for (const d of details) {
    if (d.manageNumber) detailMap[d.manageNumber] = d;
  }

  const shopUrl = cfg.shop_url || '';
  const diffItems = [];
  let skuMatchedCount = 0;

  for (const itemNum of sortedDiff) {
    const manageNumber = rakutenMapping[itemNum] || itemNum;
    const detail = detailMap[manageNumber];

    if (!detail || detail._error) {
      diffItems.push({
        item_code: itemNum, name: itemNum, sku_count: 0, price: 0,
        has_price_diff: false, price_range: '', inventory: 0,
        image_url: '', over_10_skus: false, _manage_number: manageNumber,
      });
      continue;
    }

    // Rakuten RMS の variants は通常オブジェクト形式 {skuManageNumber: {price, hidden, ...}}
    // 念のため配列形式にもフォールバック
    const activeVariants = normalizeVariants(detail.variants);

    if (registeredCodes.size > 0 && activeVariants.length > 0) {
      const skuCodes = new Set(activeVariants.map(v => (v.skuManageNumber || '').trim()).filter(Boolean));
      const matched = [...skuCodes].filter(c => registeredCodes.has(c));
      if (matched.length > 0) { skuMatchedCount++; continue; }
    }

    const imageUrls = mapper.buildFullImageUrls(detail.imagePaths || [], shopUrl);
    const variantPrices = activeVariants.map(v => parseInt(v.price || 0)).filter(p => p > 0);
    const price = variantPrices.length ? Math.min(...variantPrices) : 0;
    const hasPriceDiff = variantPrices.length > 0 && new Set(variantPrices).size > 1;

    diffItems.push({
      item_code: itemNum,
      name: (detail.title || itemNum).slice(0, 80),
      sku_count: activeVariants.length,
      price,
      has_price_diff: hasPriceDiff,
      price_range: hasPriceDiff ? `¥${Math.min(...variantPrices).toLocaleString()}〜¥${Math.max(...variantPrices).toLocaleString()}` : '',
      inventory: 0,
      image_url: imageUrls[0] || '',
      over_10_skus: activeVariants.length > 10,
      _manage_number: manageNumber,
    });
  }

  job.result = {
    diff_items: diffItems,
    rakuten_total: rakutenItemNumbers.size,
    registered_total: registeredCodes.size,
    diff_count: diffItems.length,
    excluded_count: excludedItems.size,
    sku_matched_count: skuMatchedCount,
    error: null,
  };
  job.status = 'done';
  job.updatedAt = Date.now();
  updateJobProgress(job, { phase: 'done', done: apiCodes.length, total: apiCodes.length });
}

// POST /api/check-csv → job kick + jobId 即返却
router.post('/api/check-csv', async (req, res) => {
  await ensureDb();

  if (checkCsvJobs.size >= JOB_MAX_TOTAL) {
    return res.status(503).json({ error: 'ジョブ table が満杯です。しばらく待ってから再試行してください。' });
  }
  if (countRunningJobs() >= JOB_MAX_CONCURRENT_RUNNING) {
    return res.status(429).json({ error: '同時実行ジョブ数の上限に達しています。実行中のジョブが終わってから再試行してください。' });
  }

  const jobId = genJobId();
  const now = Date.now();
  const job = {
    status: 'queued',
    progress: { phase: 'pending', done: 0, total: 0, chunkIndex: 0, chunkTotal: 0 },
    result: null,
    error: null,
    createdAt: now,
    updatedAt: now,
    lastAccessedAt: now,
  };
  checkCsvJobs.set(jobId, job);

  // 即返却して背景で実処理
  setImmediate(() => {
    runCheckCsvJob(jobId).catch(e => {
      console.error(`[mercari-sync] check-csv job ${jobId} unexpected error:`, e);
      const j = checkCsvJobs.get(jobId);
      if (j) {
        j.status = 'error';
        j.error = `内部エラー: ${e.message}`;
        j.updatedAt = Date.now();
      }
    });
  });

  res.json({ jobId });
});

// GET /api/check-csv-status/:jobId → ジョブ状態取得
router.get('/api/check-csv-status/:jobId', (req, res) => {
  const job = checkCsvJobs.get(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: 'ジョブが見つからないか期限切れです (Render 再起動でジョブが消失した可能性あり)' });
  }
  job.lastAccessedAt = Date.now();
  res.json({
    status: job.status,
    progress: job.progress,
    result: job.status === 'done' ? job.result : null,
    error: job.error,
  });
});

router.post('/api/export-csv', async (req, res) => {
  await ensureDb();
  const { item_codes = [], manage_numbers = {} } = req.body;

  if (item_codes.length === 0) {
    return res.json({ error: 'エクスポート対象が選択されていません' });
  }
  if (item_codes.length > csvExporter.MAX_ROWS_PER_CSV) {
    return res.json({ error: `一度にエクスポートできるのは最大${csvExporter.MAX_ROWS_PER_CSV}商品です。` });
  }

  const cfg = settingsDb.getAllConfig();
  const serviceSecret = cfg.service_secret || '';
  const licenseKey = cfg.license_key || '';

  const settings = {
    shop_url: cfg.shop_url || '',
    shipping_method_code: cfg.shipping_method_code || '3',
    shipping_from_area_code: cfg.shipping_from_area_code || 'jp27',
    days_to_ship_code: cfg.days_to_ship_code || '2',
    shipping_cost_burden: cfg.shipping_cost_burden || '1',
    default_product_status: cfg.default_product_status || '1',
    default_mercari_category: cfg.default_mercari_category || '',
    excluded_image_positions: settingsDb.getExcludedImagePositions(),
    excluded_image_patterns: settingsDb.getExcludedImagePatterns(),
  };

  const categoryMapping = {};
  for (const m of settingsDb.getCategoryMappings()) {
    categoryMapping[m.rakuten_genre_id] = m.mercari_category;
  }

  const apiCodes = item_codes.map(c => manage_numbers[c] || c);
  let details;
  try {
    details = await rakuten.getItemDetailsBulk(null, null, apiCodes);
  } catch (e) {
    return res.json({ error: `楽天RMS API エラー: ${e.message}` });
  }

  const detailMap = {};
  for (const d of details) { if (d.manageNumber) detailMap[d.manageNumber] = d; }

  const rows = [];
  const allWarnings = [];
  const exportedCodes = new Set();

  for (const code of item_codes) {
    const mn = manage_numbers[code] || code;
    const detail = detailMap[mn];
    if (!detail) { allWarnings.push(`${code}: 楽天から商品情報を取得できませんでした`); continue; }
    if (detail._error) { allWarnings.push(`${code}: エラー - ${detail._error}`); continue; }

    try {
      const { rows: csvRows, warnings } = mapper.rakutenItemToCsvRows(detail, settings, categoryMapping);
      rows.push(...csvRows);
      for (const w of warnings) allWarnings.push(`${code}: ${w}`);
      // エクスポートした商品コード+SKUコードを記録
      exportedCodes.add(code);
      exportedCodes.add(mn);
      for (const v of normalizeVariants(detail.variants)) {
        if (v.skuManageNumber) exportedCodes.add(v.skuManageNumber);
      }
    } catch (e) {
      allWarnings.push(`${code}: エラー - ${e.message}`);
    }
  }

  if (rows.length === 0) {
    return res.json({ error: 'エクスポート可能な商品がありませんでした', warnings: allWarnings });
  }

  // 登録済みに自動追加
  if (exportedCodes.size > 0) {
    settingsDb.addRegisteredItems(exportedCodes, 'csv_export');
  }

  const maxRows = csvExporter.MAX_ROWS_PER_CSV;
  if (rows.length <= maxRows) {
    // 単一CSV
    const csvBuffer = csvExporter.writeCsv(rows);
    res.set({
      'Content-Type': 'text/csv; charset=shift_jis',
      'Content-Disposition': 'attachment; filename=mercari_import.csv',
      'X-Export-Count': String(rows.length),
      'X-Warning-Count': String(allWarnings.length),
    });
    return res.send(csvBuffer);
  }

  // 複数ファイル → ZIP
  const chunks = [];
  for (let i = 0; i < rows.length; i += maxRows) {
    chunks.push(rows.slice(i, i + maxRows));
  }

  res.set({
    'Content-Type': 'application/zip',
    'Content-Disposition': 'attachment; filename=mercari_import.zip',
    'X-Export-Count': String(rows.length),
    'X-Warning-Count': String(allWarnings.length),
  });

  const archive = archiver('zip', { zlib: { level: 9 } });
  archive.pipe(res);
  chunks.forEach((chunk, idx) => {
    const csvBuf = csvExporter.writeCsv(chunk);
    archive.append(csvBuf, { name: `mercari_import_${idx + 1}.csv` });
  });
  archive.finalize();
});

// --- 接続テスト（APIモード用） ---
router.post('/api/test-connection', async (req, res) => {
  // APIモード用。将来実装。
  res.json({ ok: false, message: 'APIモードは現在未実装です。CSVモードをご使用ください。' });
});

export default router;
