/**
 * easy-ship の管理画面 (ポータルセッション認証。mount側で requireAppAccess('easy-ship'))。
 * SKU→梱包サイズマスターの一覧・登録・編集・無効化・削除・CSV入出力。
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { initEasyShipDB } from './db.js';
import {
  EsError,
  createMaster,
  exportCsv,
  importCsv,
  listMaster,
  removeMaster,
  updateMaster,
} from './service.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// import 時 (= server.js boot 時) に DB を初期化する。migration 失敗時は起動を止める
initEasyShipDB();

function api(handler) {
  return (req, res) => {
    try {
      res.json(handler(req));
    } catch (e) {
      if (e instanceof EsError) {
        return res.status(e.status).json({ error: e.message, code: e.code });
      }
      console.error('[easy-ship] API error', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

router.get('/', (req, res) => {
  // 末尾スラッシュなし (/apps/easy-ship) で開くと、ページ内の相対 URL の解決基準が
  // /apps/ になり誤った先へ飛ぶため、正規 URL (末尾スラッシュあり) へ寄せる。
  // API 呼び出しは index.ejs 側で絶対パス化済みだが、将来相対参照のリソース
  // (画像・追加 JS 等) が入っても壊れないようにする防御。GET のみ・完全一致のみ対象。
  // クエリは最初の '?' 以降を丸ごと保持 (giftset-assembly #539 と同パターン)
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  const query = qIdx === -1 ? '' : req.originalUrl.slice(qIdx);
  if (!pathname.endsWith('/')) {
    return res.redirect(308, pathname + '/' + query);
  }
  res.render(path.join(__dirname, 'views/index'), {
    title: '梱包サイズマスター (Easy Ship)',
    username: req.session.email,
    displayName: req.session.displayName,
  });
});

router.get('/api/list', api((req) => listMaster(req.query)));

router.post('/api/create', api((req) => createMaster(req.body, req.session.email)));

router.post('/api/update/:id', api((req) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) throw new EsError(400, 'VALIDATION_ERROR', 'idが不正です');
  return updateMaster(id, req.body, req.session.email);
}));

// 既定は無効化 (論理削除)。hard=true で物理削除
router.post('/api/remove/:id', api((req) => {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) throw new EsError(400, 'VALIDATION_ERROR', 'idが不正です');
  return removeMaster(id, req.body?.hard === true, req.session.email);
}));

router.post('/api/import', api((req) =>
  importCsv(req.body?.csv, req.body?.mode, req.body?.allowPartial === true, req.session.email),
));

// ?excel=1 でExcel閲覧用 (数式インジェクション対策の ' 前置。再インポートには使わない)
router.get('/api/export', (req, res) => {
  try {
    const excelMode = req.query.excel === '1' || req.query.excel === 'true';
    const filename = excelMode ? 'package-sizes-excel.csv' : 'package-sizes.csv';
    res
      .type('text/csv; charset=utf-8')
      .setHeader('Content-Disposition', `attachment; filename="${filename}"`)
      .send(exportCsv(excelMode));
  } catch (e) {
    console.error('[easy-ship] export error', e);
    res.status(500).json({ error: 'エクスポートに失敗しました' });
  }
});

export default router;
