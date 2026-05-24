/**
 * 梱包機振り分け・配送方法判定 — router (Render 完結、ミニPC 不使用)
 *
 * URL: /apps/packing-dispatch/        … UI
 *      /apps/packing-dispatch/api/*   … REST API
 * 認証: server.js で requireAppAccess('packing-dispatch') を mount 時に適用。
 */
import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import { ensureSchema, shippingMethodMap, diagInfo, productDiag } from './db.js';
import {
  importCsv, batchSummary, listOrders, getOrderDetail, decideOrder, exportCsv,
  listRules, upsertRules, copyRules, searchRules, listUnregistered, mirrorFreshness,
  searchAssort, updateAssort,
} from './service.js';
import { loadSeed } from './tools/load-shipping-rule-seed.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });
const router = Router();

function currentUser(req) { return req.session?.email || req.session?.displayName || null; }
function handle(res, fn) {
  try { res.json({ ok: true, result: fn() }); }
  catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message, detail: e.detail });
    console.error('[packing]', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
}

router.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'index.html')));

// マスタ選択肢 (配送方法・梱包機)
router.get('/api/options', (req, res) => handle(res, () => {
  ensureSchema();
  const db = ensureSchema();
  const methods = db.prepare(`SELECT code,name_csv,rank,is_locked,is_nekopos FROM pd_shipping_method ORDER BY (rank IS NULL), rank`).all();
  const machines = db.prepare(`SELECT code,name_csv,sort_order FROM pd_packing_machine ORDER BY sort_order`).all();
  return { methods, machines };
}));

// CSV アップロード → 取込・判定
router.post('/api/upload', upload.single('file'), (req, res) => handle(res, () => {
  if (!req.file) { const e = new Error('ファイルがありません'); e.code = 'VALIDATION'; throw e; }
  return importCsv(req.file.buffer, req.file.originalname, currentUser(req));
}));

router.get('/api/batch/:id', (req, res) => handle(res, () => {
  const s = batchSummary(req.params.id);
  if (!s) { const e = new Error('バッチが見つかりません'); e.code = 'VALIDATION'; throw e; }
  return s;
}));

router.get('/api/orders', (req, res) => handle(res, () =>
  listOrders(req.query.batch, { status: req.query.status || null })));

// 1伝票の詳細 (変更ダイアログのリッチ表示用)
router.get('/api/order', (req, res) => handle(res, () => {
  const d = getOrderDetail(req.query.batch, req.query.shop, req.query.order);
  if (!d) { const e = new Error('伝票が見つかりません'); e.code = 'VALIDATION'; throw e; }
  return d;
}));

// 1伝票の確定 (アソートは learn=true で学習登録)
router.post('/api/decide', (req, res) => handle(res, () => {
  const b = req.body || {};
  return decideOrder(b.batch_id, b.shop_name, b.order_no, {
    shipping_method_code: b.shipping_method_code,
    packing_machine_code: b.packing_machine_code,
    learn: !!b.learn,
  }, currentUser(req));
}));

// 出力 (Shift-JIS)
router.get('/api/export/:id', (req, res) => {
  try {
    const { buffer, rowCount } = exportCsv(req.params.id, currentUser(req));
    const fname = `logi_dispatch_${rowCount}rows.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
    res.send(buffer);
  } catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message, detail: e.detail });
    console.error('[packing] export', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
});

// ── アソート学習 (注文番号/受注番号/商品コードで検索→編集) ──
router.get('/api/assort/search', (req, res) => handle(res, () => searchAssort(req.query.q || '')));
router.post('/api/assort/update', (req, res) => handle(res, () => {
  const b = req.body || {};
  return updateAssort(b.combo_key, b.shipping_method_code, b.packing_machine_code, currentUser(req));
}));

// ── マスタ ──
router.get('/api/rules', (req, res) => handle(res, () => listRules(req.query.product || '')));
router.post('/api/rules', (req, res) => handle(res, () => upsertRules(req.body || {}, currentUser(req))));
router.post('/api/rules/copy', (req, res) => handle(res, () => copyRules(req.body || {}, currentUser(req))));
// 登録済みルールを 商品コード/商品名 で検索 (コピー元の選択用)
router.get('/api/rule-search', (req, res) => handle(res, () => searchRules(req.query.q || '')));

// ── 未登録一覧 (取扱中×非セット×②未登録) ──
router.get('/api/unregistered', (req, res) => handle(res, () => ({
  freshness: mirrorFreshness(),
  rows: listUnregistered(),
})));

// ── マスタ状態 (ルール件数)。失敗しても 500 にせずエラー文を返す(原因可視化) ──
router.get('/api/master-status', (req, res) => handle(res, () => {
  try {
    const db = ensureSchema();
    return { rule_rows: db.prepare('SELECT COUNT(*) c FROM pd_shipping_rule').get().c };
  } catch (e) {
    return { rule_rows: null, error: e.message };
  }
}));

// ── 診断 (取得失敗の原因切り分け) ──
router.get('/api/diag', (req, res) => handle(res, () => diagInfo()));
// ── 商品診断 (なぜ未登録一覧に出る/出ないか) ──
router.get('/api/product-diag', (req, res) => handle(res, () => productDiag(req.query.code || '')));

// ── 初期データ投入 (Excel移行シード)。未投入時のみ。force=1 で再投入(全上書き) ──
router.post('/api/admin/load-seed', (req, res) => handle(res, () =>
  loadSeed({ force: req.query.force === '1' || (req.body && req.body.force === true) })));

export default router;
