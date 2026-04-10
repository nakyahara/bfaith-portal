/**
 * FBA在庫補充システム — ルーター（Render側）
 *
 * UIレンダリングのみRenderで処理。
 * /api/* は全てミニPCのサービスAPIに転送する。
 */
import express from 'express';
import multer from 'multer';

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage() });

// --- ミニPC接続設定 ---
const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
const SERVICE_API_BASE = `${WAREHOUSE_URL}/service-api/fba`;

function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}

/**
 * ミニPCへのプロキシ転送ヘルパー
 */
async function proxyToMiniPC(req, res, { method = 'GET', path, body, query, rawResponse = false, timeout = 300000 }) {
  try {
    let url = `${SERVICE_API_BASE}${path}`;

    // クエリパラメータ転送
    const qs = query || (req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '');
    if (qs && qs.startsWith('?')) url += qs;

    const options = {
      method,
      headers: getServiceHeaders(),
      signal: AbortSignal.timeout(timeout),
    };
    if (body) options.body = JSON.stringify(body);

    const response = await fetch(url, options);

    // バイナリレスポンス（Excel/CSVダウンロード）
    if (rawResponse || (response.headers.get('content-type') || '').includes('octet-stream') ||
        (response.headers.get('content-disposition') || '').includes('attachment')) {
      const contentType = response.headers.get('content-type') || 'application/octet-stream';
      const disposition = response.headers.get('content-disposition');
      res.setHeader('Content-Type', contentType);
      if (disposition) res.setHeader('Content-Disposition', disposition);
      const buf = Buffer.from(await response.arrayBuffer());
      return res.status(response.status).send(buf);
    }

    // JSONレスポンス
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (e) {
    console.error(`[FBA-Proxy] ${method} ${path} failed:`, e.message);
    res.status(502).json({
      error: 'ミニPCへの接続に失敗しました',
      detail: e.message,
    });
  }
}

// ===== メイン画面（Render側で処理） =====
router.get('/', (req, res) => {
  res.render('fba-replenishment', {
    title: 'FBA在庫補充',
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ===== 以下、全API → ミニPCへ転送 =====

// --- レポート取得 ---
router.post('/api/fetch-reports', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/fetch-reports' }));

// --- スナップショット ---
router.get('/api/snapshots/latest', (req, res) => proxyToMiniPC(req, res, { path: '/snapshots/latest' }));
router.get('/api/snapshots/:sku', (req, res) => proxyToMiniPC(req, res, { path: `/snapshots/${encodeURIComponent(req.params.sku)}` }));
router.get('/api/all-snapshot-skus', (req, res) => proxyToMiniPC(req, res, { path: '/all-snapshot-skus' }));

// --- SKUマッピング ---
router.get('/api/sku-mappings', (req, res) => proxyToMiniPC(req, res, { path: '/sku-mappings' }));
router.post('/api/sync-sku-mappings', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/sync-sku-mappings' }));

// --- SKU例外 ---
router.get('/api/sku-exceptions', (req, res) => proxyToMiniPC(req, res, { path: '/sku-exceptions' }));
router.post('/api/sku-exceptions', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/sku-exceptions', body: req.body }));
router.delete('/api/sku-exceptions/:sku', (req, res) => proxyToMiniPC(req, res, { method: 'DELETE', path: `/sku-exceptions/${encodeURIComponent(req.params.sku)}` }));

// --- 倉庫在庫 ---
router.get('/api/warehouse', (req, res) => proxyToMiniPC(req, res, { path: '/warehouse' }));
router.get('/api/warehouse/summary', (req, res) => proxyToMiniPC(req, res, { path: '/warehouse/summary' }));
router.post('/api/warehouse/upload', upload.single('file'), async (req, res) => {
  // CSVファイルをパースしてミニPCに転送
  try {
    if (!req.file) return res.status(400).json({ error: 'ファイルが必要です' });
    const text = req.file.buffer.toString('utf-8');
    const lines = text.split('\n').filter(l => l.trim());
    if (lines.length < 2) return res.status(400).json({ error: 'CSVが空です' });

    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',').map(c => c.trim().replace(/^"|"$/g, ''));
      const row = {};
      headers.forEach((h, j) => { row[h] = cols[j] || ''; });
      rows.push(row);
    }

    await proxyToMiniPC(req, res, { method: 'POST', path: '/warehouse/upload', body: { rows } });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// --- 推奨リスト ---
router.get('/api/recommendations', (req, res) => proxyToMiniPC(req, res, { path: '/recommendations' }));
router.get('/api/recommendations/:sku', (req, res) => proxyToMiniPC(req, res, { path: `/recommendations/${encodeURIComponent(req.params.sku)}` }));
router.post('/api/refresh-inbound-working', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/refresh-inbound-working' }));

// --- 納品プラン ---
router.post('/api/create-inbound-plan', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/create-inbound-plan', body: req.body }));
router.get('/api/plans', (req, res) => proxyToMiniPC(req, res, { path: '/plans' }));
router.get('/api/plans/:id/items', (req, res) => proxyToMiniPC(req, res, { path: `/plans/${encodeURIComponent(req.params.id)}/items` }));
router.get('/api/picking-list/:planId', (req, res) => proxyToMiniPC(req, res, { path: `/picking-list/${encodeURIComponent(req.params.planId)}` }));

// --- Eligibility ---
router.get('/api/eligibility/check-one', (req, res) => proxyToMiniPC(req, res, { path: '/eligibility/check-one' }));
router.get('/api/debug/eligibility/:asin', (req, res) => proxyToMiniPC(req, res, { path: `/eligibility/check-one?asin=${encodeURIComponent(req.params.asin)}` }));

// --- ドラフト ---
router.get('/api/draft', (req, res) => proxyToMiniPC(req, res, { path: '/draft' }));
router.post('/api/draft', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/draft', body: req.body }));
router.delete('/api/draft', (req, res) => proxyToMiniPC(req, res, { method: 'DELETE', path: '/draft' }));

// --- 仮確定 ---
router.get('/api/provisional', (req, res) => proxyToMiniPC(req, res, { path: '/provisional' }));
router.post('/api/provisional', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/provisional', body: req.body }));
router.delete('/api/provisional', (req, res) => proxyToMiniPC(req, res, { method: 'DELETE', path: '/provisional' }));
router.patch('/api/provisional', (req, res) => proxyToMiniPC(req, res, { method: 'PATCH', path: '/provisional', body: req.body }));

// --- 出力 ---
router.post('/api/export-manifest', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/export-manifest', body: req.body, rawResponse: true }));
router.post('/api/export-ne-csv', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/export-ne-csv', body: req.body, rawResponse: true }));
router.get('/api/export-history', (req, res) => proxyToMiniPC(req, res, { path: '/export-history' }));
router.get('/api/export-history/:id/download', (req, res) => proxyToMiniPC(req, res, { path: `/export-history/${encodeURIComponent(req.params.id)}/download`, rawResponse: true }));

// --- 非表示管理 ---
router.get('/api/new-product-hidden', (req, res) => proxyToMiniPC(req, res, { path: '/new-product-hidden' }));
router.post('/api/new-product-hidden', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/new-product-hidden', body: req.body }));
router.delete('/api/new-product-hidden/:sku', (req, res) => proxyToMiniPC(req, res, { method: 'DELETE', path: `/new-product-hidden/${encodeURIComponent(req.params.sku)}` }));
router.get('/api/stockout-hidden', (req, res) => proxyToMiniPC(req, res, { path: '/stockout-hidden' }));
router.post('/api/stockout-hidden', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/stockout-hidden', body: req.body }));
router.delete('/api/stockout-hidden/:sku', (req, res) => proxyToMiniPC(req, res, { method: 'DELETE', path: `/stockout-hidden/${encodeURIComponent(req.params.sku)}` }));

// --- 設定 ---
router.get('/api/settings', (req, res) => proxyToMiniPC(req, res, { path: '/settings' }));
router.post('/api/settings', (req, res) => proxyToMiniPC(req, res, { method: 'POST', path: '/settings', body: req.body }));

// --- ステータス ---
router.get('/api/status', (req, res) => proxyToMiniPC(req, res, { path: '/status' }));

// --- ジョブ確認（ミニPC側のジョブマネージャー） ---
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

export default router;
