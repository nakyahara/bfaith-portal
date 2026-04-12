/**
 * リクエストID付与・実行ログ記録ミドルウェア
 */
import { randomUUID } from 'crypto';
import fs from 'fs';
import path from 'path';

const LOG_DIR = process.env.DATA_DIR
  ? path.join(process.env.DATA_DIR, 'logs')
  : path.join(process.cwd(), 'data', 'logs');

// ログディレクトリ確保
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function getLogPath() {
  const date = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  return path.join(LOG_DIR, `service-api-${date}.log`);
}

function appendLog(entry) {
  const line = JSON.stringify(entry) + '\n';
  fs.appendFileSync(getLogPath(), line, 'utf-8');
}

export function requestLogger(req, res, next) {
  const requestId = req.headers['x-request-id'] || randomUUID();
  const start = Date.now();

  // リクエストIDを伝搬
  req.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);

  // レスポンス完了時にログ
  res.on('finish', () => {
    const entry = {
      ts: new Date().toISOString(),
      requestId,
      method: req.method,
      path: req.originalUrl,
      status: res.statusCode,
      ms: Date.now() - start,
    };
    appendLog(entry);

    // 遅いリクエストは警告
    if (entry.ms > 10000) {
      console.warn(`[ServiceAPI] Slow request: ${entry.method} ${entry.path} ${entry.ms}ms`);
    }
  });

  next();
}
