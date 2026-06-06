/**
 * サービストークン認証ミドルウェア
 * Render → ミニPC間のリクエストを認証する
 *
 * 2026-06-06 fail-closed 化 (Codex 助言、PR-C0):
 * - 旧: SERVICE_TOKEN 未設定なら認証スキップ (fail-open) → ミニPC が露出するリスク
 * - 新: SERVICE_TOKEN 未設定なら 503 fail-closed (絶対バイパスしない)
 *
 * 影響範囲: /service-api/* (cross-sell-finder, ranking-checker, FBA系, profit-calculator,
 *           mercari/linegift sync, healthcheck 等が依存)
 * → ミニPC .env の SERVICE_TOKEN は必須。未設定なら上記すべて 503 を返す。
 *
 * クエリパラメータ経由の token 受け付けも廃止 (token がアクセスログに残るリスク回避)。
 */

export function serviceAuth(req, res, next) {
  const expected = process.env.SERVICE_TOKEN;

  if (!expected) {
    // fail-closed: 未設定で next() しない。ミニPC を露出させない。
    console.error('[ServiceAuth] SERVICE_TOKEN 未設定 — fail-closed で 503 を返します');
    return res.status(503).json({
      ok: false,
      error: 'NOT_CONFIGURED',
      message: 'SERVICE_TOKEN is not configured on the server',
    });
  }

  // ⚠️ Codex R1 High: ?service_token=... が URL に残ったリクエストは request-logger /
  // error-handler が req.originalUrl をログに保存し token が残る経路がある。
  // serviceAuth 側でこのリクエスト自体を拒否して、呼び出し側の移行漏れも検知させる。
  if (req.query && req.query.service_token) {
    console.error('[ServiceAuth] ?service_token= 経由のリクエストを拒否 (token が URL log に残るリスク)');
    return res.status(400).json({
      ok: false,
      error: 'TOKEN_IN_URL',
      message: 'service_token in query string is not allowed. Use Authorization: Bearer ... header.',
    });
  }

  const token = extractToken(req);
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
  // ⚠️ クエリパラメータ経由は廃止 (token が access log に残るリスク)
  return null;
}
