/**
 * マスタ配信 (Render → miniPC)。
 *
 * セット・手動マッピング・おまけ優先順位の「正」は Render 側に置く。
 * 中原さんが普段開いているポータルで登録できるようにするため (2026-08-08 の判断)。
 * miniPC (拡張が叩く側) はここから取得してローカルにキャッシュし、
 * 取得できなかったときは前回の内容でそのまま動く (fail-soft)。
 *
 * 認証は既存の MIRROR_SYNC_KEY / x-sync-key に相乗りする (新しい秘密を増やさない)。
 * 未設定なら 503 で止める (fail-closed)。
 */
import { Router } from 'express';
import crypto from 'crypto';
import { exportMaster } from './db.js';

const router = Router();

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

router.use((req, res, next) => {
  const expected = process.env.MIRROR_SYNC_KEY;
  if (!expected) return res.status(503).json({ error: 'MIRROR_SYNC_KEY 未設定 (fail-closed)' });
  const provided = req.headers['x-sync-key'];
  if (typeof provided !== 'string' || !timingSafeEq(provided, expected)) {
    return res.status(401).json({ error: 'x-sync-key が不正です' });
  }
  next();
});

router.get('/export', (req, res) => {
  try {
    res.json(exportMaster());
  } catch (e) {
    console.error('[select-set] master export error', e);
    res.status(500).json({ error: 'マスタの書き出しに失敗しました' });
  }
});

export default router;
