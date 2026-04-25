/**
 * 楽天RMS APIサービス
 * /service-api/rakuten-rms/* にマウント
 *
 * 楽天RMS APIのプロキシ。APIキー（serviceSecret/licenseKey）はミニPC側で管理。
 * LINEギフト同期・メルカリ同期の両方から利用される。
 */
import { Router } from 'express';
import https from 'https';
import { rateLimitMiddleware } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';

const router = Router();

const SERVICE_SECRET = () => process.env.RAKUTEN_SERVICE_SECRET || '';
const LICENSE_KEY = () => process.env.RAKUTEN_LICENSE_KEY || '';

function makeAuthHeader() {
  const token = Buffer.from(`${SERVICE_SECRET()}:${LICENSE_KEY()}`).toString('base64');
  return `ESA ${token}`;
}

/**
 * 楽天RMS APIにリクエストを送信するヘルパー
 */
function rmsRequest(apiPath) {
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: 'api.rms.rakuten.co.jp',
      path: apiPath,
      headers: { 'Authorization': makeAuthHeader() },
    };
    https.get(opts, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, data: JSON.parse(body) });
        } catch (e) {
          resolve({ status: res.statusCode, data: body });
        }
      });
    }).on('error', reject);
  });
}

// ==========================================
// 商品検索（LINEギフト同期で使用）
// ==========================================

router.get('/items/search', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const cursorMark = req.query.cursorMark || '*';
    const hits = req.query.hits || '100';
    const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=${hits}`;
    const result = await rmsRequest(apiPath);
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 全商品コード取得（メルカリ同期で使用）
// ==========================================

router.get('/items/all-codes', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const mapping = {};
    let cursorMark = '*';

    for (let page = 0; page < 100; page++) {
      const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=100`;
      const result = await rmsRequest(apiPath);

      if (result.status !== 200) {
        return errorResponse(res, { status: result.status, error: 'RMS_API_ERROR', message: `HTTP ${result.status}`, requestId: req.requestId });
      }

      const items = result.data.results || result.data.items || [];
      for (const r of items) {
        const item = r.item || r;
        if (item.manageNumber) {
          mapping[item.itemNumber || item.manageNumber] = item.manageNumber;
        }
      }

      if (!result.data.nextCursorMark || items.length === 0) break;
      cursorMark = result.data.nextCursorMark;
    }

    okResponse(res, { mapping, count: Object.keys(mapping).length });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 全SKU取得（粗利分析の sku_map 構築用）
// ==========================================
// 各商品のvariantsまで展開し、AM/AL/W 3コードをSKU粒度で返す。
// AM = merchantDefinedSkuId（システム連携用SKU番号）
// AL = variants のキー（SKU管理番号）
// W  = item.itemNumber（商品番号）

router.get('/items/all-skus', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const skus = [];
    let cursorMark = '*';
    let pageCount = 0;

    for (let page = 0; page < 100; page++) {
      const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=100`;
      const result = await rmsRequest(apiPath);

      if (result.status !== 200) {
        return errorResponse(res, { status: result.status, error: 'RMS_API_ERROR', message: `HTTP ${result.status}`, requestId: req.requestId });
      }

      const items = result.data.results || result.data.items || [];
      for (const r of items) {
        const item = r.item || r;
        const itemNumber = item.itemNumber || '';
        const manageNumber = item.manageNumber || '';
        const variants = item.variants || {};
        const variantKeys = Object.keys(variants);

        if (variantKeys.length === 0) {
          // variantが無い商品（単一SKU）はitem情報だけ残す
          skus.push({
            itemNumber,
            manageNumber,
            skuManageNumber: manageNumber,
            systemSkuNumber: '',
          });
        } else {
          for (const key of variantKeys) {
            const v = variants[key] || {};
            skus.push({
              itemNumber,
              manageNumber,
              skuManageNumber: key,
              systemSkuNumber: v.merchantDefinedSkuId || '',
            });
          }
        }
      }

      pageCount++;
      if (!result.data.nextCursorMark || items.length === 0) break;
      cursorMark = result.data.nextCursorMark;

      // Rakuten RMS レート制限対策（1req/sec 想定で500ms sleep）
      await new Promise(r => setTimeout(r, 500));
    }

    okResponse(res, { skus, count: skus.length, pages: pageCount });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// 商品詳細取得（メルカリ同期で使用）
// ==========================================

router.get('/items/detail/:manageNumber', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const apiPath = `/es/2.0/items/manage-numbers/${encodeURIComponent(req.params.manageNumber)}`;
    const result = await rmsRequest(apiPath);
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 複数商品詳細（バルク）
router.post('/items/details-bulk', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const { itemCodes } = req.body;
    if (!itemCodes || !Array.isArray(itemCodes)) {
      return errorResponse(res, { status: 400, error: 'VALIDATION', message: 'itemCodes required', requestId: req.requestId });
    }

    const results = [];
    for (const code of itemCodes) {
      try {
        const apiPath = `/es/2.0/items/manage-numbers/${encodeURIComponent(code)}`;
        const result = await rmsRequest(apiPath);
        if (result.status === 200) {
          results.push(result.data);
        }
      } catch (e) {
        // 個別エラーはスキップ
      }
    }

    okResponse(res, { items: results, count: results.length });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// ステータス
// ==========================================

router.get('/status', (req, res) => {
  okResponse(res, {
    hasCredentials: !!(SERVICE_SECRET() && LICENSE_KEY()),
  });
});

export default router;
