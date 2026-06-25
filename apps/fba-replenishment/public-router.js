/**
 * FBA納品ピッキング準備 — 公開印刷ルーター (ログイン不要)
 *
 * 子会社がポータルにログインせずプラン別シートを印刷できるよう、実行ごとの推測不能トークンで
 * 印刷ビューを公開する。requireAppAccess/requireAuth の外側 (server.js で /print にマウント) で動く。
 * トークンを知っている人は当該実行のプラン別シートのみ閲覧可 (= 共有リンクと同じモデル)。
 */
import express from 'express';
import { getPickingRunByToken } from './db.js';

const router = express.Router();

// GET /print/picking/:token  → プラン別シート印刷ビュー
router.get('/picking/:token', (req, res) => {
  const token = String(req.params.token || '');
  let rec;
  try {
    rec = getPickingRunByToken(token);
  } catch (e) {
    // DB 初期化前など
    return res.status(503).send('準備中です。しばらくしてから再度お試しください。');
  }
  if (!rec) return res.status(404).send('リンクが無効です（期限切れ、または存在しません）。');

  let result = {};
  try { result = rec.result ? JSON.parse(rec.result) : {}; } catch { result = {}; }

  res.render('fba-picking-print', {
    title: 'FBA納品 プラン別シート印刷',
    runId: rec.id,
    runAt: rec.run_at,
    planSheets: result.planSheets || [],
    isPublic: true,
  });
});

export default router;
