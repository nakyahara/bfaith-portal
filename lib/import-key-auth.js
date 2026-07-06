/**
 * import-key-auth — 会計系 /import-history (過去データ一括投入) の認証ミドルウェア
 *
 * 設計監査 2026-07-06 S-4 対応: 7 アプリ共通の静的共有キー平文ハードコード
 * (query 受理あり) を廃止し、アプリ別 env トークンへ移行。
 *
 * 方針:
 *   ・env 未設定なら 503 (fail-closed)。import は一時投入用なので「普段は env 未設定
 *     = endpoint 無効」が正常状態。投入時だけ Render env に強トークンを設定し、済んだら消す。
 *   ・header `x-import-key` only。query 受理は URL・アクセスログ・ブラウザ履歴に
 *     残るため受け付けない (MIRROR_READ_TOKEN と同方針)。
 *   ・アプリ別 env (IMPORT_KEY_AMAZON 等) で最小権限 (1 トークン漏洩の blast radius を 1 アプリに限定)。
 *
 * 使い方 (router 側):
 *   import { requireImportKey, importJsonParser } from '../../lib/import-key-auth.js';
 *   router.post('/import-history', requireImportKey('IMPORT_KEY_AMAZON'), importJsonParser, (req, res) => { ... });
 *
 * body parser は認証の「後」に置く (server.js の global parser は import-history を
 * 除外しており、未認証リクエストの body を読まない。sync/service 系と同思想)。
 */
import express from 'express';

export const importJsonParser = express.json({ limit: '10mb' });

export function requireImportKey(envName) {
  return (req, res, next) => {
    const key = process.env[envName];
    if (!key) {
      console.warn(`[import-key-auth] ${envName} 未設定のため 503 (fail-closed)`);
      return res.status(503).json({ error: 'import_key_unset' });
    }
    const provided = req.headers['x-import-key'];
    if (!provided || provided !== key) {
      return res.status(401).json({ error: 'invalid_import_key' });
    }
    next();
  };
}
