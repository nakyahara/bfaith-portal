/**
 * サービストークン認証ミドルウェア
 * Render → ミニPC間のリクエストを認証する
 */

export function serviceAuth(req, res, next) {
  const token = extractToken(req);
  const expected = process.env.SERVICE_TOKEN;

  if (!expected) {
    console.warn('[ServiceAuth] SERVICE_TOKEN未設定 — 認証スキップ');
    return next();
  }

  if (!token) {
    return res.status(401).json({
      ok: false,
      error: 'AUTH_REQUIRED',
      message: 'Authorization header or X-Service-Token required',
    });
  }

  if (token !== expected) {
    return res.status(403).json({
      ok: false,
      error: 'AUTH_INVALID',
      message: 'Invalid service token',
    });
  }

  next();
}

function extractToken(req) {
  // Bearer token
  const auth = req.headers['authorization'];
  if (auth && auth.startsWith('Bearer ')) return auth.slice(7);
  // ヘッダー直接指定
  if (req.headers['x-service-token']) return req.headers['x-service-token'];
  // クエリパラメータ（デバッグ用）
  if (req.query && req.query.service_token) return req.query.service_token;
  return null;
}
