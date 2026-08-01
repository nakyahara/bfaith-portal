/**
 * 出荷作業管理アプリ router (bfaith-portal)
 *
 * URL: /apps/shipping-work/ (カンバン) — PR1 は表示のみ。
 *      作業API (リース/開始/完了/保留/印刷トラブル) は PR3、管理者画面は PR2/PR5。
 *
 * 認証: server.js で requireAppAccess('shipping-work') を mount 時に適用。
 *       管理者専用操作は router 内で req.session.role === 'admin' を check (PR2〜)。
 *
 * 設計書: AI_reference/システム設計/出荷作業管理アプリ_要件定義_20260801.md (v2)
 *         AI_reference/システム設計/出荷作業管理アプリ_実装計画_20260801.md
 */
import { Router } from 'express';
import crypto from 'node:crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  getDB, initShippingWorkDB, jstToday, listKanbanBatches, BATCH_STATUSES, STATUS_LABELS,
  listMasters, isValidMaster, createBatch, updateBatch, cancelBatch, listAdminBatches, getBatch,
  DOCS_DIR,
} from './db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// import 時 (= server.js boot 時) に DB を初期化する。
// migration 失敗時はここで throw し、旧スキーマのまま起動を継続させない (Codex PR1レビュー#3)
initShippingWorkDB();

// カンバンの列構成 (表示順)。hold/stock_return は横断列として末尾に置く。
// cancelled はカンバンに出さない (管理画面のみ・PR5)。
const KANBAN_COLUMNS = [
  'ready', 'picking', 'picked', 'sorting', 'sorted', 'packing', 'done', 'hold', 'stock_return',
];

// ─── カンバン (全員・表示専用。ドラッグ機能は作らない) ───
router.get('/', (req, res) => {
  const workDate = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.date || ''))
    ? String(req.query.date)
    : jstToday();
  const batches = listKanbanBatches(workDate);

  const columns = KANBAN_COLUMNS.map((status) => ({
    status,
    label: STATUS_LABELS[status],
    batches: batches.filter((b) => b.status === status),
  }));

  res.render(path.join(__dirname, 'views/kanban'), {
    title: '出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: req.session.role === 'admin',
    workDate,
    columns,
    totalCount: batches.length,
  });
});

// ─── ヘルスチェック (デプロイ確認用) ───
router.get('/api/health', (req, res) => {
  const masters = getDB().prepare('SELECT COUNT(*) AS c FROM sw_masters').get();
  res.json({ ok: true, masters: masters.c, statuses: BATCH_STATUSES.length });
});

// ═══ 管理者: バッチ管理 (PR2) ═══

function requireAdminPage(req, res, next) {
  if (req.session.role !== 'admin') return res.status(403).send('管理者権限が必要です');
  next();
}
function requireAdminApi(req, res, next) {
  if (req.session.role !== 'admin') return res.status(403).json({ error: 'admin_required' });
  next();
}

// PDF は memoryStorage で受けて自前で DOCS_DIR に保存する (ファイル名はサーバー発行の乱数のみ。
// クライアント由来のファイル名・パスは一切使わない)
const uploadPdf = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024, files: 1 },
});

function savePdf(file) {
  if (!file) return null;
  const head = file.buffer.subarray(0, 5).toString('latin1');
  if (head !== '%PDF-') throw new Error('PDFファイルではありません');
  if (!fs.existsSync(DOCS_DIR)) fs.mkdirSync(DOCS_DIR, { recursive: true });
  const name = `${crypto.randomUUID()}.pdf`;
  fs.writeFileSync(path.join(DOCS_DIR, name), file.buffer);
  return name; // pdf_path には DOCS_DIR 相対のファイル名のみ保存
}

/** フォーム入力の検証。エラー文字列 or 検証済み値を返す。 */
function validateBatchInput(body) {
  const workDate = String(body.work_date || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(workDate)) return { error: '作業日が不正です' };
  const shippingNo = String(body.shipping_no || '');
  if (!isValidMaster('shipping_no', shippingNo)) return { error: '出荷Noが不正です' };
  const bunrui = String(body.bunrui || '');
  if (!isValidMaster('bunrui', bunrui)) return { error: '発送分類が不正です' };
  const packingMethod = String(body.packing_method || '');
  if (!isValidMaster('packing_method', packingMethod)) return { error: '梱包方法が不正です' };
  let carriers = body.carriers ?? [];
  if (!Array.isArray(carriers)) carriers = [carriers];
  carriers = carriers.map(String);
  if (carriers.some((c) => !isValidMaster('carrier', c))) return { error: '配送種別が不正です' };
  let slipCount = null;
  if (body.slip_count !== undefined && String(body.slip_count).trim() !== '') {
    slipCount = Number(body.slip_count);
    if (!Number.isInteger(slipCount) || slipCount <= 0) return { error: '伝票件数は正の整数で入力してください' };
  }
  const note = String(body.note || '').trim() || null;
  return { workDate, shippingNo, bunrui, packingMethod, carriers, slipCount, note };
}

// バッチ管理画面
router.get('/admin/batches', requireAdminPage, (req, res) => {
  const workDate = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.date || ''))
    ? String(req.query.date)
    : jstToday();
  res.render(path.join(__dirname, 'views/admin_batches'), {
    title: 'バッチ管理 | 出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    workDate,
    batches: listAdminBatches(workDate),
    statusLabels: STATUS_LABELS,
    masters: {
      shippingNo: listMasters('shipping_no'),
      bunrui: listMasters('bunrui'),
      packingMethod: listMasters('packing_method'),
      carrier: listMasters('carrier'),
    },
  });
});

// バッチ作成
router.post('/api/admin/batches', requireAdminApi, uploadPdf.single('pdf'), (req, res) => {
  const v = validateBatchInput(req.body);
  if (v.error) return res.status(400).json({ error: v.error });
  let pdfPath = null;
  try {
    pdfPath = savePdf(req.file);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }
  try {
    const id = createBatch({ ...v, pdfPath }, req.session.email);
    res.json({ ok: true, id });
  } catch (e) {
    if (String(e.code).startsWith('SQLITE_CONSTRAINT')) {
      return res.status(409).json({ error: `${v.workDate} の同じ出荷Noのバッチが既にあります` });
    }
    throw e;
  }
});

// バッチ更新 (ready のみ)
router.post('/api/admin/batches/:id(\\d+)/update', requireAdminApi, uploadPdf.single('pdf'), (req, res) => {
  const batch = getBatch(Number(req.params.id));
  if (!batch) return res.status(404).json({ error: 'not_found' });
  // work_date / shipping_no (業務キー) は変更不可。それ以外を検証するためキーは既存値を使う
  const v = validateBatchInput({ ...req.body, work_date: batch.work_date, shipping_no: batch.shipping_no });
  if (v.error) return res.status(400).json({ error: v.error });
  let pdfPath = null;
  try {
    pdfPath = savePdf(req.file);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }
  const ok = updateBatch(batch.id, { ...v, pdfPath }, req.session.email);
  if (!ok) return res.status(409).json({ error: '作業開始後のバッチは編集できません (管理者手動修正はPR5)' });
  res.json({ ok: true });
});

// バッチ取消 (ready / hold のみ・理由必須)
router.post('/api/admin/batches/:id(\\d+)/cancel', requireAdminApi, (req, res) => {
  const reason = String(req.body?.reason || '').trim();
  if (!reason) return res.status(400).json({ error: '取消理由は必須です' });
  const ok = cancelBatch(Number(req.params.id), req.session.email, reason);
  if (!ok) return res.status(409).json({ error: '取消できるのは「本日のやること」「保留」のバッチのみです' });
  res.json({ ok: true });
});

// 添付PDFの表示 (開発中モード: ブリッジ未設置でも手動印刷できる逃げ道。実装計画§5)
router.get('/admin/batches/:id(\\d+)/pdf', requireAdminPage, (req, res) => {
  const batch = getBatch(Number(req.params.id));
  if (!batch || !batch.pdf_path) return res.status(404).send('PDFがありません');
  // pdf_path はサーバー発行のファイル名のみだが、念のため DOCS_DIR 内であることを検証
  const abs = path.resolve(DOCS_DIR, batch.pdf_path);
  if (!abs.startsWith(path.resolve(DOCS_DIR) + path.sep)) return res.status(400).send('不正なパス');
  if (!fs.existsSync(abs)) return res.status(404).send('PDFファイルが見つかりません');
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="batch-${batch.id}.pdf"`);
  fs.createReadStream(abs).pipe(res);
});

export default router;
