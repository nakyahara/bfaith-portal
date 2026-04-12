/**
 * リサーチ仕入れツール サービスAPI
 * /service-api/research/* にマウント
 *
 * SP-API呼び出しのみを担当。DB操作・計算・CSV処理は含まない。
 */
import { Router } from 'express';
import { rateLimitMiddleware } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';

import {
  getProduct,
  getFees,
  createListing,
  patchListing,
  getShippingTemplates,
  getItemOffers,
  updatePrice,
  getActiveListingsReport,
  getSalesCountBySku,
  searchByJan,
  searchByKeyword,
  searchByPartNumber,
} from '../profit-calculator/sp-api.js';

const router = Router();

// ヘルパー: SP-APIエラーをキャッチして統一形式で返す
function spHandler(fn) {
  return async (req, res) => {
    try {
      const result = await fn(req);
      okResponse(res, { result });
    } catch (e) {
      errorResponse(res, {
        status: e.statusCode || 500,
        error: 'SP_API_ERROR',
        message: e.message,
        requestId: req.requestId,
      });
    }
  };
}

// ==========================================
// 商品検索系
// ==========================================

router.get('/product/:asin', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  return await getProduct(req.params.asin);
}));

router.get('/search/jan/:jan', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  return await searchByJan(req.params.jan);
}));

router.get('/search/keyword', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  return await searchByKeyword(req.query.q);
}));

router.get('/search/part-number', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  return await searchByPartNumber(req.query.q);
}));

// ==========================================
// 手数料・価格
// ==========================================

router.get('/fees', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  const { asin, price, isFba } = req.query;
  return await getFees(asin, Number(price), isFba !== 'false');
}));

router.get('/offers/:asin', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  const condition = req.query.condition || 'New';
  return await getItemOffers(req.params.asin, condition);
}));

// ==========================================
// 出品・価格更新
// ==========================================

router.post('/listing', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  return await createListing(req.body);
}));

router.patch('/listing', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  return await patchListing(req.body);
}));

router.post('/price', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  const { sku, price } = req.body;
  return await updatePrice({ sku, price: Number(price) });
}));

// ==========================================
// テンプレート・レポート
// ==========================================

router.get('/shipping-templates', rateLimitMiddleware('sp-api'), spHandler(async () => {
  return await getShippingTemplates();
}));

router.get('/active-listings-report', rateLimitMiddleware('sp-api'), spHandler(async () => {
  return await getActiveListingsReport();
}));

router.get('/sales-count', rateLimitMiddleware('sp-api'), spHandler(async (req) => {
  const days = parseInt(req.query.days) || 365;
  return await getSalesCountBySku(days);
}));

export default router;
