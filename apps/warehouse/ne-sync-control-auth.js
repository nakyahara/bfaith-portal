/**
 * NE 反映 worker 起動専用認証 (Render → ミニPC、2026-06-06 PR-C 構成 4)
 *
 * Codex 助言: 既存 serviceAuth (SERVICE_TOKEN) と別軸の専用鍵で守る。
 * - 既存 SERVICE_TOKEN が漏洩しても、こちらの鍵 (MINIPC_NE_SYNC_RUN_KEY) が
 *   なければ worker 起動はできない (権限分離)
 * - SERVICE_TOKEN 流用しない (memory: secret 流用禁止)
 *
 * - fail-closed: 未設定で 503 (絶対 next() しない)
 * - Bearer header のみ受理 (query/別ヘッダは受け付けない)
 * - timing-safe compare
 * - 失敗時の token はログに出さない
 */
import crypto from 'node:crypto';

export function requireMinipcNeSyncRunKey(req, res, next) {
  const expected = process.env.MINIPC_NE_SYNC_RUN_KEY || '';
  if (!expected) {
    console.error('[ne-sync-control-auth] MINIPC_NE_SYNC_RUN_KEY 未設定 — fail-closed 503');
    return res.status(503).json({
      ok: false, error: 'NOT_CONFIGURED',
      message: 'MINIPC_NE_SYNC_RUN_KEY is not configured on the server',
    });
  }
  const header = String(req.headers.authorization || '');
  const m = /^Bearer\s+(\S+)$/.exec(header);
  if (!m) {
    return res.status(401).json({ ok: false, error: 'AUTH_REQUIRED',
      message: 'Authorization: Bearer ... required' });
  }
  const got = m[1];
  // timing-safe compare (length 不一致でも短絡しない)
  const a = Buffer.from(got, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    console.warn('[ne-sync-control-auth] auth failed', { ip: req.ip, ua: req.headers['user-agent'] });
    return res.status(403).json({ ok: false, error: 'AUTH_INVALID', message: 'Invalid token' });
  }
  next();
}
