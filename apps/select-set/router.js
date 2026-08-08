/**
 * select-set の管理画面 (ポータルセッション認証。mount側で requireAppAccess('select-set'))。
 * 選べるセットの登録・手動マッピング・おまけ優先順位の編集と、商品OPの動作確認。
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  deleteMapping, deleteSet, initSelectSetDB, listMappings, listOmake, listSets,
  replaceOmake, upsertMapping, upsertSet,
} from './db.js';
import { SsError, diagnose, expandForOrder, inspectSet, stockOf } from './service.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// import 時 (= server.js boot 時) に DB を初期化する。migration 失敗時は起動を止める
initSelectSetDB();

function api(handler) {
  return async (req, res) => {
    try {
      res.json(await handler(req));
    } catch (e) {
      if (e instanceof SsError) return res.status(e.status).json({ error: e.message, code: e.code });
      console.error('[select-set] API error', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

router.get('/', (req, res) => {
  // 末尾スラッシュなしで開くと相対URLの解決基準がずれるため正規URLへ寄せる (easy-ship と同パターン)
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  const query = qIdx === -1 ? '' : req.originalUrl.slice(qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + query);
  res.render(path.join(__dirname, 'views/index'), {
    title: '選べるセットの明細展開',
    username: req.session.email,
    displayName: req.session.displayName,
  });
});

// ---- セット ----
router.get('/api/sets', api(() => ({ sets: listSets({ includeInactive: true }) })));

router.post('/api/sets', api((req) => {
  const setCode = String(req.body?.setCode || '').trim();
  if (!setCode) throw new SsError('セット商品コードを入力してください');
  upsertSet({
    setCode,
    label: req.body?.label || '',
    isActive: req.body?.isActive === false ? 0 : 1,
    note: req.body?.note || '',
  });
  return { ok: true };
}));

router.delete('/api/sets/:code', api((req) => {
  deleteSet(String(req.params.code));
  return { ok: true };
}));

/** RMSから選択肢定義を引いて状態を見る。?force=1 でキャッシュを無視して取り直す */
router.get('/api/sets/:code/inspect', api((req) => inspectSet(String(req.params.code), {
  force: req.query.force === '1',
})));

// ---- 手動マッピング ----
router.get('/api/mappings', api((req) => ({ mappings: listMappings(req.query.setCode || null) })));

router.post('/api/mappings', api((req) => {
  const setCode = String(req.body?.setCode || '').trim();
  const optionText = String(req.body?.optionText || '').trim();
  const productCode = String(req.body?.productCode || '').trim();
  if (!setCode || !optionText || !productCode) throw new SsError('セット・選択肢・商品コードは必須です');
  if (!stockOf(productCode)) {
    throw new SsError(`商品コード「${productCode}」がNE商品マスタに見つかりません`, { code: 'UNKNOWN_PRODUCT' });
  }
  const id = upsertMapping({ id: req.body?.id, setCode, optionText, productCode, note: req.body?.note || '' });
  return { ok: true, id };
}));

router.delete('/api/mappings/:id', api((req) => {
  deleteMapping(Number(req.params.id));
  return { ok: true };
}));

// ---- おまけ優先順位 ----
router.get('/api/omake', api(() => ({
  omake: listOmake().map((r) => ({ ...r, ...(stockOf(r.product_code) || { name: '', available: null }) })),
})));

router.post('/api/omake', api((req) => {
  const codes = Array.isArray(req.body?.codes) ? req.body.codes : [];
  if (!codes.length) throw new SsError('おまけ候補が空です');
  const unknown = codes.filter((c) => !stockOf(c));
  if (unknown.length) {
    throw new SsError(`NE商品マスタに無い商品コード: ${unknown.join(', ')}`, { code: 'UNKNOWN_PRODUCT' });
  }
  const n = replaceOmake(codes);
  return { ok: true, count: n };
}));

/**
 * 診断。「画面では解決できないのに手元のスクリプトでは解決できる」ときに、
 * サーバープロセスが実際に何を見ているか (cwd / DATA_DIR / 商品マスタの件数 / 認証情報の有無) を出す。
 * 値そのものは返さず、有無と件数だけ。
 */
router.get('/api/diag', api(() => diagnose()));

// ---- 動作確認 ----
router.post('/api/try', api((req) => expandForOrder({
  setCode: req.body?.setCode,
  op: req.body?.op,
  quantity: req.body?.quantity,
})));

export default router;
