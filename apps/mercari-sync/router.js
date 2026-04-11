/**
 * 楽天→メルカリShops 商品登録ツール - Express Router
 * Python FastAPI版からの移植
 */
import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import archiver from 'archiver';

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
const rakuten = {
  async getAllItemCodes() {
    const res = await fetch(`${WAREHOUSE_URL}/service-api/rakuten-rms/items/all-codes`, { headers: getServiceHeaders(), signal: AbortSignal.timeout(120000) });
    const data = await res.json();
    if (!data.ok) throw new Error(data.message || 'RMS API error');
    return data.mapping;
  },
  async getItemDetailsBulk(serviceSecret, licenseKey, itemCodes) {
    const res = await fetch(`${WAREHOUSE_URL}/service-api/rakuten-rms/items/details-bulk`, { method: 'POST', headers: getServiceHeaders(), body: JSON.stringify({ itemCodes }), signal: AbortSignal.timeout(120000) });
    const data = await res.json();
    if (!data.ok) throw new Error(data.message || 'RMS API error');
    return data.items;
  },
};

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

router.post('/api/check-csv', async (req, res) => {
  await ensureDb();
  const cfg = settingsDb.getAllConfig();
  let rakutenMapping;
  try {
    rakutenMapping = await rakuten.getAllItemCodes();
  } catch (e) {
    return res.json({ error: `楽天RMS API エラー: ${e.message}` });
  }

  let rakutenItemNumbers = new Set(Object.keys(rakutenMapping));
  const registeredCodes = settingsDb.getRegisteredItems();
  const excludedItems = settingsDb.getExcludedItems();

  // 除外適用
  for (const ex of excludedItems) rakutenItemNumbers.delete(ex);

  // 差分判定
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

  // 詳細取得
  const sortedDiff = [...diffItemNumbers].sort();
  const apiCodes = sortedDiff.map(n => rakutenMapping[n] || n);

  let details;
  try {
    details = await rakuten.getItemDetailsBulk(null, null, apiCodes);
  } catch (e) {
    return res.json({ error: `楽天RMS API エラー（詳細取得）: ${e.message}` });
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

    const activeVariants = (detail.variants || []).filter(v => !v.hidden);

    // SKUレベル照合
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

  res.json({
    diff_items: diffItems,
    rakuten_total: rakutenItemNumbers.size,
    registered_total: registeredCodes.size,
    diff_count: diffItems.length,
    excluded_count: excludedItems.size,
    sku_matched_count: skuMatchedCount,
    error: null,
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
      for (const v of (detail.variants || [])) {
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
