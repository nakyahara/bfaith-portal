/**
 * 統一エラーレスポンス・エラーハンドラー
 */

/**
 * 成功レスポンスを返すヘルパー
 */
export function okResponse(res, data, statusCode = 200) {
  return res.status(statusCode).json({ ok: true, ...data });
}

/**
 * エラーレスポンスを返すヘルパー
 */
export function errorResponse(res, { status = 500, error, message, requestId }) {
  return res.status(status).json({
    ok: false,
    error: error || 'INTERNAL_ERROR',
    message: message || 'An unexpected error occurred',
    requestId: requestId || null,
  });
}

/**
 * Express エラーハンドリングミドルウェア（最後にuse）
 */
export function serviceErrorHandler(err, req, res, _next) {
  console.error(`[ServiceAPI] Error on ${req.method} ${req.originalUrl}:`, err.message);

  // SP-API スロットリング
  if (err.code === 'QuotaExceeded' || err.status === 429) {
    return errorResponse(res, {
      status: 429,
      error: 'SP_API_THROTTLED',
      message: 'SP-API rate limit exceeded. Retry after a few seconds.',
      requestId: req.requestId,
    });
  }

  // タイムアウト
  if (err.code === 'ETIMEDOUT' || err.code === 'ESOCKETTIMEDOUT') {
    return errorResponse(res, {
      status: 504,
      error: 'API_TIMEOUT',
      message: `External API call timed out: ${err.message}`,
      requestId: req.requestId,
    });
  }

  // バリデーションエラー
  if (err.name === 'ValidationError') {
    return errorResponse(res, {
      status: 400,
      error: 'VALIDATION_ERROR',
      message: err.message,
      requestId: req.requestId,
    });
  }

  // その他
  return errorResponse(res, {
    status: err.status || 500,
    error: err.code || 'INTERNAL_ERROR',
    message: err.message || 'An unexpected error occurred',
    requestId: req.requestId,
  });
}
