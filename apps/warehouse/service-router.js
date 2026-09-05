/**
 * サービスAPI 親ルーター
 * 用途限定エンドポイントをまとめてマウントする
 *
 * マウント先: /service-api
 * 認証: サービストークン必須
 */
import 'dotenv/config';
import { Router } from 'express';
// serviceAuth は server.js 側で mount (body parser より先) に移動済み
import { requestLogger } from './request-logger.js';
import { serviceErrorHandler } from './error-handler.js';
import { getJob, listJobs } from './job-manager.js';
import { getRateLimitStatus } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';

const router = Router();

// --- 共通ミドルウェア ---
// serviceAuth は server.js 側で body parser より先に掛けている (DoS防止)。
// ここでは requestLogger のみ。
router.use(requestLogger);

// --- ヘルスチェック（認証なしでもアクセス可能にしたい場合は上に移動） ---
router.get('/health', (req, res) => {
  okResponse(res, {
    status: 'ok',
    uptime: Math.floor(process.uptime()),
    timestamp: new Date().toISOString(),
  });
});

// --- ジョブ管理エンドポイント ---
router.get('/jobs', (req, res) => {
  okResponse(res, { jobs: listJobs() });
});

router.get('/jobs/:jobId', (req, res) => {
  const job = getJob(req.params.jobId);
  if (!job) {
    return errorResponse(res, {
      status: 404,
      error: 'JOB_NOT_FOUND',
      message: `Job ${req.params.jobId} not found`,
      requestId: req.requestId,
    });
  }
  okResponse(res, { job });
});

// --- レート制限状態 ---
router.get('/rate-limit-status', (req, res) => {
  okResponse(res, { rateLimits: getRateLimitStatus() });
});

// --- ツール別サブルーター ---
import fbaServiceRouter from './fba-service.js';
import researchServiceRouter from './research-service.js';
import rakutenRmsServiceRouter from './rakuten-rms-service.js';
import qoo10PriceServiceRouter from './qoo10-price-service.js';
import linegiftPriceServiceRouter from './linegift-price-service.js';
import rankcheckServiceRouter from './rankcheck-service.js';
import crossSellServiceRouter from './cross-sell-service.js';
import selectSetServiceRouter from './select-set-service.js';
import logizardStockServiceRouter from './logizard-stock-service.js';
import logizardExportServiceRouter from './logizard-export-service.js';
import neProductsServiceRouter from './ne-products-service.js';
router.use('/fba', fbaServiceRouter);
router.use('/research', researchServiceRouter);
router.use('/rakuten-rms', rakutenRmsServiceRouter);
router.use('/qoo10', qoo10PriceServiceRouter);
// ★LINEギフトは読み取り専用 (書き込みの口は作っていない。理由は linegift-price-service.js の冒頭)
router.use('/linegift', linegiftPriceServiceRouter);
router.use('/rankcheck', rankcheckServiceRouter);
router.use('/cross-sell', crossSellServiceRouter);
router.use('/select-set', selectSetServiceRouter);
router.use('/logizard-stock', logizardStockServiceRouter);
// ★ロジザードCSVのオンデマンド取得 (入荷受付チェック iPad の「いま取りに行く」)。
//   既に毎日動いている取得スクリプトを人の操作で1回走らせるだけ (新しい取得先は増やさない)
router.use('/logizard', logizardExportServiceRouter);
router.use('/ne-products', neProductsServiceRouter);

// --- エラーハンドラー（最後） ---
router.use(serviceErrorHandler);

export default router;
