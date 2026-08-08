/**
 * select-set の Chrome拡張向けAPI (セッション認証なし・x-api-key 認証)。
 *
 * - トークンは env SELECT_SET_EXT_TOKEN (未設定なら 503 fail-closed。easy-ship と同パターン)
 * - server.js で requireAppAccess 付き本体より先に mount し、グローバル JSON parser から除外する
 * - 返すのは「セット商品コード → 実SKUの明細行」だけ。顧客情報は一切扱わない
 */
import { Router } from 'express';
import crypto from 'crypto';
import express from 'express';
import { SsError, expandForOrder } from './service.js';
import { ensureMasterFresh } from './master-sync.js';
import { listSets } from './db.js';

const router = Router();

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function requireExtToken(req, res, next) {
  const expected = process.env.SELECT_SET_EXT_TOKEN;
  if (!expected) {
    return res.status(503).json({
      success: false,
      error: { code: 'INTERNAL_ERROR', message: 'select_set_ext_token_unset' },
    });
  }
  const provided = req.headers['x-api-key'];
  if (typeof provided !== 'string' || !timingSafeEq(provided, expected)) {
    return res.status(401).json({ success: false, error: { code: 'UNAUTHORIZED', message: 'APIキーが不正です' } });
  }
  next();
}

router.use(requireExtToken);
// body parser は認証の後 (未認可リクエストに parse させない)
router.use(express.json({ limit: '64kb' }));

const ok = (data) => ({ success: true, data });
const fail = (code, message) => ({ success: false, error: { code, message } });

function api(handler) {
  return async (req, res) => {
    try {
      res.json(ok(await handler(req)));
    } catch (e) {
      if (e instanceof SsError) return res.status(e.status).json(fail(e.code, e.message));
      console.error('[select-set] ext-api error', e);
      res.status(500).json(fail('INTERNAL_ERROR', 'サーバー内部でエラーが発生しました'));
    }
  };
}

/** 拡張の「接続+APIキー確認」用 */
router.get('/api/v1/ping', api(() => ({ pong: true })));

/**
 * 選べるセットの商品コード一覧。
 * 拡張はこれを使って「この明細行に展開ボタンを出すか」を判定する
 * (伝票画面を開くたびに展開APIを叩かないで済む)
 */
router.get('/api/v1/sets', api(async () => {
  // マスタは Render 側が正。必要なら取りに行く (失敗しても前回の内容で答える)
  await ensureMasterFresh();
  return { setCodes: listSets().map((s) => s.set_code) };
}));

/**
 * 商品OPを明細行に展開する。
 * body: { setCode, op, quantity }
 * ok=false のときは拡張側で自動入力させないこと (warnings に理由が入る)
 */
router.post('/api/v1/expand', api((req) => expandForOrder({
  setCode: req.body?.setCode,
  op: req.body?.op,
  quantity: req.body?.quantity,
})));

export default router;
