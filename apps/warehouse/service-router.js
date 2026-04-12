/**
 * サービスAPI 親ルーター
 * 用途限定エンドポイントをまとめてマウントする
 *
 * マウント先: /service-api
 * 認証: サービストークン必須
 */
import 'dotenv/config';
import { Router } from 'express';
import { serviceAuth } from './service-auth.js';
import { requestLogger } from './request-logger.js';
import { serviceErrorHandler } from './error-handler.js';
import { getJob, listJobs } from './job-manager.js';
import { getRateLimitStatus } from './rate-limiter.js';
import { okResponse, errorResponse } from './error-handler.js';

const router = Router();

// --- 共通ミドルウェア ---
router.use(requestLogger);
router.use(serviceAuth);

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
router.use('/fba', fbaServiceRouter);
router.use('/research', researchServiceRouter);
router.use('/rakuten-rms', rakutenRmsServiceRouter);

// --- エラーハンドラー（最後） ---
router.use(serviceErrorHandler);

export default router;
