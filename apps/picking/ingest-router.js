/**
 * picking の引当RPA向け取込API (セッション認証なし・x-api-key 認証)。
 *
 * 呼び出し元 = 伝票出しPCの auto-hikiate.js (ロジザード作業自動化)。カンタン引当の実行直後に
 * ピッキングリストCSV (CS03002) を multipart で POST する。RPAは実行したパターン名を
 * 知っているため、引当分類を推定ではなく正確に登録できる (画面取込のプレビュー確認が不要)。
 *
 * - トークンは env PICKING_INGEST_KEY (未設定なら 503 fail-closed。easy-ship ext-api と同パターン)
 * - server.js で requireAppAccess 付き本体より先に mount する
 * - 冪等: tb_no + CSVハッシュ (importBatch と同じ)。同一内容の再送は replayed で成功を返す。
 *   内容が変わった再送は ready のバッチに限り自動で上書き (RPAのリトライを人手なしで通す)
 */
import { Router } from 'express';
import crypto from 'node:crypto';
import multer from 'multer';
import { parseCs03002, importBatch, PkError, isStaleInstructDate } from './service.js';

const router = Router();

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function requireIngestKey(req, res, next) {
  const expected = process.env.PICKING_INGEST_KEY;
  if (!expected) {
    return res.status(503).json({ ok: false, error: 'picking_ingest_key_unset' });
  }
  const provided = req.headers['x-api-key'];
  if (typeof provided !== 'string' || !timingSafeEq(provided, expected)) {
    return res.status(401).json({ ok: false, error: 'APIキーが不正です' });
  }
  next();
}

router.use(requireIngestKey);

const uploadCsv = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

/**
 * POST /apps/picking/ingest-api/import
 *   multipart: file=CS03002, pattern_name=引当パターン表示名, folder_name=出荷_XX
 * 200: { ok, batchId, replaced, replayed, lineCount, slipCount, totalQty, tbNo, staleDate }
 */
router.post('/import', (req, res, next) => {
  uploadCsv.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ ok: false, error: `アップロード失敗: ${err.message}` });
    next();
  });
}, async (req, res) => {
  try {
    if (!req.file) throw new PkError(400, 'no_file', 'file がありません');
    const patternName = String(req.body.pattern_name || '').trim();
    if (!patternName) throw new PkError(400, 'no_class', 'pattern_name がありません');
    const preview = parseCs03002(req.file.buffer);
    const staleDate = isStaleInstructDate(preview.instructDate);
    const result = importBatch(preview, {
      hikiateClass: patternName,
      folderName: String(req.body.folder_name || '').trim() || null,
      overwrite: true,   // ready のバッチに限る (importBatch 内で enforce)。作業開始後は 409
    }, `ingest:${patternName.slice(0, 40)}`);
    res.json({
      ok: true,
      tbNo: preview.tbNo,
      lineCount: preview.lines.length,
      slipCount: preview.slipCount,
      totalQty: preview.totalQty,
      staleDate,
      ...result,
    });
  } catch (e) {
    if (e instanceof PkError) {
      return res.status(e.status).json({ ok: false, code: e.code, error: e.message });
    }
    console.error('[picking-ingest]', e);
    res.status(500).json({ ok: false, error: 'サーバーエラーが発生しました' });
  }
});

export default router;
