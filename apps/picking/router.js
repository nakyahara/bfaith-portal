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
import {
  parseCs03002, importBatch, formatLocation, PkError, getWorkState, applyEvent,
  deriveFolderName, isStaleInstructDate,
} from './service.js';
import { allPatternNames } from './patterns.js';
import { enqueueBatchSync, STATUS_PICKING, STATUS_PICKED } from './notion.js';
import {
  listDriveSubfolders, listDriveFilesAcross, downloadDriveFileById,
} from '../../lib/drive-csv.js';

// 出荷_no フォルダ (中原さん共有 2026-08-12。SA bfaith-portal@… に閲覧者共有済み)。
// CSVは各サブフォルダ (出荷_XX) に引当RPAが保存する運用のため、検索はサブフォルダ横断
const DRIVE_FOLDER_ID = process.env.PICKING_DRIVE_FOLDER_ID || '110ONn2xHzfEG5HPt1DRy4P64zv2hpGjh';

// サブフォルダ一覧 (出荷_01〜45) は増減しないので60秒キャッシュ (一覧・DL検証の両方で使う)
let _subfoldersCache = { at: 0, list: null };
async function getShippingFolders() {
  if (_subfoldersCache.list && Date.now() - _subfoldersCache.at < 60_000) return _subfoldersCache.list;
  const subs = await listDriveSubfolders({ folderId: DRIVE_FOLDER_ID });
  const list = [{ folder_id: DRIVE_FOLDER_ID, name: '(出荷_no直下)' }, ...subs];
  _subfoldersCache = { at: Date.now(), list };
  return list;
}

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

/**
 * 状態変更APIのCSRF緩和策: Origin ヘッダがあれば自ホストと一致することを要求する
 * (セッションCookieのSameSiteに加えた明示防御。Origin無しの同一サイトfetchは通す)。
 */
function checkOrigin(req, res, next) {
  const origin = req.headers.origin;
  if (origin) {
    let host = null;
    try { host = new URL(origin).host; } catch { /* 不正値は下で403 */ }
    if (!host || host !== req.headers.host) {
      return res.status(403).json({ error: '不正なオリジンからのリクエストです' });
    }
  }
  next();
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

// ─── 作業画面 (PWA・スマホ前提。全員) ───
router.get('/work/:id(\\d+)', (req, res) => {
  let state;
  try {
    state = getWorkState(Number(req.params.id));
  } catch (e) {
    if (e instanceof PkError) return res.status(e.status).send(e.message);
    throw e;
  }
  res.render(path.join(__dirname, 'views/work'), {
    title: `ピッキング | ${state.batch.hikiate_class}`,
    worker: req.session.email,
    displayName: req.session.displayName,
    state,
  });
});

/**
 * 作業イベントAPI。body: { op_id, event: start|next|back, line_seq?, client_at? }。
 * 端末はオフラインキューから直列で送る。op_id 冪等なので再送は安全。
 */
// バッチの現在状態 → Notion カードのステータス (要件: 開始=ピッキング中 / 完了=ピッキング完了)
const NOTION_STATUS_BY_BATCH = {
  picking: STATUS_PICKING,
  done: STATUS_PICKED,
};

router.post('/api/batches/:id(\\d+)/events', checkOrigin, api(async (req, res) => {
  const batchId = Number(req.params.id);
  const result = applyEvent(batchId, {
    opId: req.body.op_id,
    event: req.body.event,
    lineSeq: req.body.line_seq == null ? null : Number(req.body.line_seq),
    clientAt: req.body.client_at,
    undoOpId: req.body.undo_op_id || null,
  }, req.session.email);
  // Notion連携 (fail-soft)。replayed では動かさない。ラベルは送信直前にバッチの
  // 最新状態から決める (イベント時点のラベルだと並行PATCHの順序逆転で巻き戻る)
  if (!result.replayed && result.transition) {
    enqueueBatchSync(batchId, () => {
      const b = getBatch(batchId);
      if (!b) return null;
      return {
        folderName: b.folder_name,
        workDate: b.work_date,   // 日跨ぎ作業でも取込日のカードを動かす
        label: NOTION_STATUS_BY_BATCH[b.status] || null,
      };
    });
  }
  res.json({ ok: true, ...result });
}));

/** 作業状態の再取得 (リロード・オンライン復帰時の同期用)。 */
router.get('/api/batches/:id(\\d+)/state', api(async (req, res) => {
  const s = getWorkState(Number(req.params.id));
  res.json({
    ok: true,
    batchStatus: s.batch.status,
    worker: s.batch.worker,
    currentSeq: s.currentSeq,
    doneCount: s.doneCount,
    lineCount: s.lines.length,
  });
}));

// PWA manifest (ホーム画面追加用)。アイコンは portal 共通の favicon を流用
router.get('/manifest.json', (req, res) => {
  res.json({
    name: 'ピッキング支援',
    short_name: 'ピッキング',
    start_url: '/apps/picking/',
    display: 'standalone',
    orientation: 'portrait',
    background_color: '#111418',
    theme_color: '#111418',
    icons: [{ src: '/favicon.png', sizes: '192x192', type: 'image/png' }],
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
function buildSummary(preview) {
  return {
    tbNo: preview.tbNo,
    instructDate: preview.instructDate,
    invoiceSoft: preview.invoiceSoft,
    deliveryMethod: preview.deliveryMethod,
    composition: preview.composition,
    lineCount: preview.lines.length,
    slipCount: preview.slipCount,
    totalQty: preview.totalQty,
    suggestions: preview.suggestions,
    // 前日ファイル取り込み事故のガード (出荷Noは毎日再利用されるため警告を出す。ブロックはしない)
    dateWarning: isStaleInstructDate(preview.instructDate)
      ? `出荷指示日 (${preview.instructDate}) が今日ではありません。前日のファイルの可能性があります`
      : null,
    lines: preview.lines.map((l) => ({
      locationLabel: formatLocation(l.block, l.location),
      sku: l.sku, productName: l.productName, qty: l.qty,
    })),
  };
}

function runImport(preview, req) {
  return importBatch(preview, {
    hikiateClass: req.body.hikiate_class,
    folderName: req.body.folder_name,
    overwrite: String(req.body.overwrite) === '1',
  }, req.session.email);
}

router.post('/admin/import', checkOrigin, requireAdmin, (req, res, next) => {
  uploadCsv.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ error: `アップロード失敗: ${err.message}` });
    next();
  });
}, api(async (req, res) => {
  if (!req.file) throw new PkError(400, 'no_file', 'ファイルを選択してください');
  const preview = parseCs03002(req.file.buffer);
  const summary = buildSummary(preview);
  if (String(req.body.mode) !== 'confirm') {
    return res.json({ ok: true, mode: 'preview', ...summary });
  }
  const result = runImport(preview, req);
  res.json({ ok: true, mode: 'confirm', ...summary, ...result });
}));

// ─── Drive取込 (出荷_no フォルダ・管理者) ───

/** lib/drive-csv.js のエラーを業務エラーへ変換 (VALIDATION=入力起因400 / それ以外=Drive側502)。 */
async function driveCall(fn) {
  try {
    return await fn();
  } catch (e) {
    if (e instanceof PkError) throw e;
    throw new PkError(e.code === 'VALIDATION' ? 400 : 502, 'drive_error', e.message);
  }
}

router.get('/admin/import/drive-files', requireAdmin, api(async (req, res) => {
  const files = await driveCall(async () => {
    const folders = await getShippingFolders();
    // ピッキングリストCSVだけに絞る (出荷_XXには送り状CSV okurijo_* 等も同居しているため)
    return listDriveFilesAcross({ folders, nameContains: 'ピッキングリスト' });
  });
  res.json({ ok: true, files: files.filter((f) => /\.csv$/i.test(f.filename)) });
}));

/**
 * Driveファイルの取込。body(JSON): { file_id, mode: preview|confirm, hikiate_class, folder_name, overwrite }
 * preview → confirm で同じ file_id を2回ダウンロードする (アップロード経路と同じ二段方式。
 * confirm 時も最新の中身を取り直すため、間に差し替わっても古い内容を確定しない)
 */
router.post('/admin/import/drive', checkOrigin, requireAdmin, api(async (req, res) => {
  const fileId = String(req.body.file_id || '').trim();
  if (!fileId) throw new PkError(400, 'no_file', 'ファイルを選択してください');
  const dl = await driveCall(async () => {
    const folders = await getShippingFolders();
    return downloadDriveFileById({ fileId, folderIds: folders.map((f) => f.folder_id) });
  });
  const preview = parseCs03002(dl.buffer);
  const summary = buildSummary(preview);
  summary.filename = dl.filename;
  // フォルダ名はファイルの置かれたサブフォルダ名 (出荷_XX) を最優先、無ければファイル名から導出
  const parentName = String(req.body.parent_name || '');
  summary.folderNameSuggestion = (/^出荷_\d+$/.test(parentName) ? parentName : null)
    || deriveFolderName(dl.filename);
  if (String(req.body.mode) !== 'confirm') {
    return res.json({ ok: true, mode: 'preview', ...summary });
  }
  const result = runImport(preview, req);
  res.json({ ok: true, mode: 'confirm', ...summary, ...result });
}));

export default router;
