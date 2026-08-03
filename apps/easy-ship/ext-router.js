/**
 * easy-ship の Chrome拡張向けAPI (セッション認証なし・x-api-key 認証)。
 *
 * - トークンは env EASY_SHIP_EXT_TOKEN (未設定なら 503 fail-closed。aba-keywords と同パターン)
 * - server.js で requireAppAccess 付き本体より先に mount し、グローバル JSON parser から除外する
 * - パスは easy-ship-helper 単体版のAPIと互換 (/api/v1/...)。
 *   拡張側は接続先URLを https://<portal>/apps/easy-ship/ext-api にするだけで無変更で動く
 * - 返す情報はSKU→梱包サイズの対応のみ。個人情報・注文情報は扱わない
 */
import { Router } from 'express';
import crypto from 'crypto';
import express from 'express';
import { EsError, addExtLogs, bulkLookup, lookupOne } from './service.js';

const router = Router();

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function requireExtToken(req, res, next) {
  const expected = process.env.EASY_SHIP_EXT_TOKEN;
  if (!expected) {
    return res
      .status(503)
      .json({ success: false, error: { code: 'INTERNAL_ERROR', message: 'easy_ship_ext_token_unset' } });
  }
  const provided = req.headers['x-api-key'];
  if (typeof provided !== 'string' || !timingSafeEq(provided, expected)) {
    return res
      .status(401)
      .json({ success: false, error: { code: 'UNAUTHORIZED', message: 'APIキーが不正です' } });
  }
  next();
}

router.use(requireExtToken);
// body parser は認証の後 (未認可リクエストに parse させない)
router.use(express.json({ limit: '64kb' }));

const ok = (data) => ({ success: true, data });
const fail = (code, message) => ({ success: false, error: { code, message } });

function api(handler) {
  return (req, res) => {
    try {
      res.json(ok(handler(req)));
    } catch (e) {
      if (e instanceof EsError) return res.status(e.status).json(fail(e.code, e.message));
      console.error('[easy-ship] ext-api error', e);
      res.status(500).json(fail('INTERNAL_ERROR', 'サーバー内部でエラーが発生しました'));
    }
  };
}

// 拡張の「接続+APIキー確認」用
router.get('/api/v1/ping', api(() => ({ pong: true })));

router.post('/api/v1/package-sizes/bulk-lookup', api((req) => bulkLookup(req.body?.skus)));

router.post('/api/v1/logs', api((req) => addExtLogs(req.body?.entries)));

// 固定パスより後に定義すること
router.get('/api/v1/package-sizes/:sku', api((req) => lookupOne(req.params.sku)));

export default router;
