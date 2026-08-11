/**
 * ピッキング支援システム router (bfaith-portal)
 *
 * URL: /apps/picking/            — バッチ一覧 (全員)
 *      /apps/picking/admin/import — CS03002 取込 (管理者のみ)
 * PR1 は取込と一覧のみ。作業画面 (PWA) は PR2、画像解決は PR3、欠品/サマリは PR4。
 *
 * 認証: server.js で requireAppAccess('picking') を mount 時に適用。
 *       取込は router 内で req.session.role === 'admin' を check。
 *
 * 設計書: AI_reference/システム設計/ピッキング支援システム_要件定義_20260811.md
 *         AI_reference/システム設計/ピッキング支援システム_実装計画_20260811.md
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  initPickingDB, jstToday, listBatches, listLines, getBatch, STATUS_LABELS,
} from './db.js';
import { parseCs03002, importBatch, formatLocation, PkError } from './service.js';
import { allPatternNames } from './patterns.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// import 時 (= server.js boot 時) に DB を初期化する。migration 失敗はここで throw して
// 旧スキーマのまま起動を継続させない (shipping-work と同規約)
initPickingDB();

// CS03002 は実測 ~9行で13KB (1行 ~1.5KB)。大規模バッチ (数百伝票) でも数MBで収まる
const uploadCsv = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

function isRealDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(`${s}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

function requireAdmin(req, res, next) {
  if (req.session.role !== 'admin') {
    return res.status(403).json({ error: '管理者のみ実行できます' });
  }
  next();
}

/** PkError は業務エラーとして status + message を返す。それ以外は 500。 */
function api(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (e) {
      if (e instanceof PkError) {
        return res.status(e.status).json({ error: e.message, code: e.code });
      }
      console.error('[picking]', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

// ─── バッチ一覧 (全員) ───
router.get('/', (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const batches = listBatches(workDate)
    .filter((b) => b.status !== 'cancelled' || b.work_date === workDate);
  res.render(path.join(__dirname, 'views/batches'), {
    title: 'ピッキング支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: req.session.role === 'admin',
    workDate,
    batches,
    statusLabels: STATUS_LABELS,
  });
});

// ─── バッチ詳細 (明細確認・紙PDFとの突合用) ───
router.get('/batches/:id(\\d+)', (req, res) => {
  const batch = getBatch(Number(req.params.id));
  if (!batch) return res.status(404).send('バッチが見つかりません');
  const lines = listLines(batch.id).map((l) => ({
    ...l, locationLabel: formatLocation(l.block, l.location),
  }));
  res.render(path.join(__dirname, 'views/batch_detail'), {
    title: `${batch.hikiate_class} | ピッキング支援`,
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: req.session.role === 'admin',
    batch,
    lines,
    statusLabels: STATUS_LABELS,
  });
});

// ─── 取込画面 (管理者) ───
router.get('/admin/import', requireAdmin, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_import'), {
    title: 'CS03002 取込 | ピッキング支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    patternNames: allPatternNames(),
  });
});

/**
 * 取込API (管理者)。multipart: file=CS03002。
 *   mode=preview  … 解析結果 (明細サンプル・推定分類) を返すだけ。DBに書かない
 *   mode=confirm  … hikiate_class 必須。overwrite=1 で取込済みバッチの入れ替えを許可
 * preview → confirm でファイルを2回送る (サーバーに中間状態を持たない。GAS取込等と同じ二段方式)
 */
router.post('/admin/import', requireAdmin, (req, res, next) => {
  uploadCsv.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ error: `アップロード失敗: ${err.message}` });
    next();
  });
}, api(async (req, res) => {
  if (!req.file) throw new PkError(400, 'no_file', 'ファイルを選択してください');
  const preview = parseCs03002(req.file.buffer);
  const summary = {
    tbNo: preview.tbNo,
    instructDate: preview.instructDate,
    invoiceSoft: preview.invoiceSoft,
    deliveryMethod: preview.deliveryMethod,
    composition: preview.composition,
    lineCount: preview.lines.length,
    slipCount: preview.slipCount,
    totalQty: preview.totalQty,
    suggestions: preview.suggestions,
    lines: preview.lines.map((l) => ({
      locationLabel: formatLocation(l.block, l.location),
      sku: l.sku, productName: l.productName, qty: l.qty,
    })),
  };
  if (String(req.body.mode) !== 'confirm') {
    return res.json({ ok: true, mode: 'preview', ...summary });
  }
  const result = importBatch(preview, {
    hikiateClass: req.body.hikiate_class,
    folderName: String(req.body.folder_name || '').trim() || null,
    overwrite: String(req.body.overwrite) === '1',
  }, req.session.email);
  res.json({ ok: true, mode: 'confirm', ...summary, ...result });
}));

export default router;
