/**
 * 楽天RMS APIサービス
 * /service-api/rakuten-rms/* にマウント
 *
 * 楽天RMS APIのプロキシ。APIキー（serviceSecret/licenseKey）はミニPC側で管理。
 * LINEギフト同期・メルカリ同期の両方から利用される。
 *
 * レート制御は rakuten-client.js の rakutenRequest() に一元化されている
 * (プロセス内グローバル直列キュー + 429/Retry-After 尊重 + 5xx 自動リトライ)。
 *
 * rateLimitMiddleware('rakuten') は HTTP route レベルでの過負荷保護
 * (同時実行数 1 + queue 5 超で早期 429) として残してある。
 */
import { Router } from 'express';
import { rateLimitMiddleware } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';
import { rakutenRequest } from './rakuten-client.js';

const router = Router();

const DETAILS_BULK_MAX = 200;

// 楽天 RMS のレスポンス本文からエラーコード / メッセージを安全に抽出 (型固定)
function extractRmsError(data) {
  if (data == null) return { errorCode: null, message: null };
  if (typeof data === 'string') return { errorCode: null, message: data.slice(0, 500) };
  if (typeof data === 'object') {
    // MessageModelList[].messageCode / message を最優先で拾う
    const list = Array.isArray(data.MessageModelList) ? data.MessageModelList : null;
    if (list && list.length > 0) {
      const err = list.find(m => m && m.messageType === 'ERROR') || list[0];
      return {
        errorCode: err && typeof err.messageCode === 'string' ? err.messageCode : null,
        message: err && typeof err.message === 'string' ? err.message : null,
      };
    }
    return {
      errorCode: typeof data.code === 'string' ? data.code : null,
      message: typeof data.message === 'string' ? data.message : null,
    };
  }
  return { errorCode: null, message: null };
}

// ==========================================
// 商品検索（LINEギフト同期で使用）
// ==========================================

router.get('/items/search', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const cursorMark = req.query.cursorMark || '*';
    const hits = req.query.hits || '100';
    const apiPath = `/es/2.0/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=${hits}`;
    const result = await rakutenRequest({ path: apiPath });
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
      const result = await rakutenRequest({ path: apiPath });

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
      const result = await rakutenRequest({ path: apiPath });

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
    const result = await rakutenRequest({ path: apiPath });
    res.status(result.status).json(result.data);
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// 複数商品詳細（バルク）
// 個別失敗を握り潰さず failedCodes で明示返却。status: 'ok' | 'partial'
router.post('/items/details-bulk', rateLimitMiddleware('rakuten'), async (req, res) => {
  try {
    const { itemCodes } = req.body;
    if (!itemCodes || !Array.isArray(itemCodes)) {
      return errorResponse(res, { status: 400, error: 'VALIDATION', message: 'itemCodes required', requestId: req.requestId });
    }
    if (itemCodes.length > DETAILS_BULK_MAX) {
      return errorResponse(res, {
        status: 400,
        error: 'VALIDATION',
        message: `itemCodes too many (${itemCodes.length} > ${DETAILS_BULK_MAX})`,
        requestId: req.requestId,
      });
    }

    const results = [];
    const failedCodes = [];

    // 固定スキーマ: { code: string, status: number|null, errorCode: string|null, message: string|null }
    for (const code of itemCodes) {
      try {
        const apiPath = `/es/2.0/items/manage-numbers/${encodeURIComponent(code)}`;
        const result = await rakutenRequest({ path: apiPath });
        if (result.status === 200 && result.data) {
          results.push(result.data);
        } else {
          const errInfo = extractRmsError(result.data);
          failedCodes.push({
            code,
            status: result.status,
            errorCode: errInfo.errorCode,
            message: errInfo.message,
          });
        }
      } catch (e) {
        failedCodes.push({
          code,
          status: null,
          errorCode: null,
          message: String(e && e.message ? e.message : e),
        });
      }
    }

    okResponse(res, {
      items: results,
      count: results.length,
      failedCodes,
      failedCount: failedCodes.length,
      status: failedCodes.length === 0 ? 'ok' : 'partial',
    });
  } catch (e) {
    errorResponse(res, { status: 502, error: 'RMS_API_ERROR', message: e.message, requestId: req.requestId });
  }
});

// ==========================================
// ステータス
// ==========================================

router.get('/status', (req, res) => {
  okResponse(res, {
    hasCredentials: !!(process.env.RAKUTEN_SERVICE_SECRET && process.env.RAKUTEN_LICENSE_KEY),
  });
});

export default router;
