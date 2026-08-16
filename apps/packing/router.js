/**
 * 梱包支援システム router (apps/packing)
 *
 * URL: /apps/packing/             — バッチ一覧
 *      /apps/packing/batches/:id  — 伝票一覧 (納品書順・警告バッジ・突合状態)
 *      /apps/packing/admin/import — 納品書CSV (CS03003) 取込 (管理者のみ)
 * PR1 は取込と一覧のみ。作業画面 (iPad PWA)・計測・TTS は PR2 以降。
 *
 * デプロイ: picking と同一ミニPCプロセス (standalone.js) に同居 (要件§7.2 案C)。
 *   Render の portal (server.js) には mount しない — 梱包はミニPCのみで動く。
 * 認証: PR1 はポータルセッションのみ (allowedApps に 'packing' または '*')。
 *   端末Cookie方式 (iPad) は作業画面と一緒に PR2 で追加する。
 *
 * 設計書: AI_reference/システム設計/梱包支援システム_要件定義_20260815.md
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  initPackingDB, jstToday, listPackBatches, getPackBatch, listPackSlips,
  listPackLinesBySlip, STATUS_LABELS, MATCH_LABELS,
} from './db.js';
import {
  parseCs03003, importPackBatch, checkPickingMatch, PackError,
  deriveFolderName, isStaleSagyoDate, WARN_LABELS,
} from './service.js';
import { listNouhinCsvFiles, downloadNouhinCsv, driveCall } from './drive.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// import 時 (= standalone boot 時) に DB を初期化する。migration 失敗はここで throw
// (standalone 側が catch して packing だけ無効化し、picking は継続する)
initPackingDB();

// 納品書CSVは実測 1伝票 ~4KB (237列)。1,100伝票/日でも数MB
const uploadCsv = multer({ storage: multer.memoryStorage(), limits: { fileSize: 30 * 1024 * 1024 } });

function isRealDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const d = new Date(`${s}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

// ═══ アクセス制御 (PR1: ポータルセッションのみ) ═══

function packingAccess(req, res, next) {
  if (req.session?.email) {
    const allowed = req.session.allowedApps;
    if (allowed === '*' || (Array.isArray(allowed) && allowed.includes('packing'))) return next();
    return res.status(403).send('packing へのアクセス権がありません');
  }
  if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'ログインが必要です' });
  if (req.session) req.session.returnTo = req.originalUrl;
  return res.redirect('/login');
}

function requireAdmin(req, res, next) {
  if (req.session?.role !== 'admin') {
    return res.status(403).json({ error: '管理者のみ実行できます' });
  }
  next();
}

/** 状態変更APIのCSRF緩和策 (picking と同じ Origin 検証)。 */
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

router.use(packingAccess);
// admin系は個別の requireAdmin に加えて prefix 一括でも守る (picking と同規約)
router.use('/admin', requireAdmin);

/** PackError は業務エラーとして status + message を返す。それ以外は 500。 */
function api(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (e) {
      if (e instanceof PackError) {
        return res.status(e.status).json({ error: e.message, code: e.code });
      }
      console.error('[packing]', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

// ─── バッチ一覧 ───
router.get('/', (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const batches = listPackBatches(workDate)
    .filter((b) => b.status !== 'cancelled' || b.work_date === workDate);
  res.render(path.join(__dirname, 'views/batches'), {
    title: '梱包支援',
    username: req.session?.email,
    displayName: req.session?.displayName,
    isAdmin: req.session?.role === 'admin',
    workDate,
    batches,
    statusLabels: STATUS_LABELS,
  });
});

// ─── バッチ詳細 (伝票一覧・納品書の束との目視突合用) ───
router.get('/batches/:id(\\d+)', (req, res) => {
  const batch = getPackBatch(Number(req.params.id));
  if (!batch) return res.status(404).send('バッチが見つかりません');
  const linesBySlip = listPackLinesBySlip(batch.id);
  const slips = listPackSlips(batch.id).map((s) => ({
    ...s,
    warns: s.warn_json ? JSON.parse(s.warn_json) : [],
    comments: s.comments_json ? JSON.parse(s.comments_json) : {},
    lines: linesBySlip.get(s.id) || [],
  }));
  res.render(path.join(__dirname, 'views/batch_detail'), {
    title: `${batch.folder_name || batch.tb_key} | 梱包支援`,
    username: req.session?.email,
    displayName: req.session?.displayName,
    isAdmin: req.session?.role === 'admin',
    batch,
    slips,
    matchDiffs: batch.match_json ? JSON.parse(batch.match_json) : [],
    statusLabels: STATUS_LABELS,
    matchLabels: MATCH_LABELS,
    warnLabels: WARN_LABELS,
  });
});

// ─── 取込画面 (管理者) ───
router.get('/admin/import', (req, res) => {
  res.render(path.join(__dirname, 'views/admin_import'), {
    title: '納品書CSV取込 | 梱包支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
  });
});

/**
 * 取込のプレビュー整形。突合結果 (match) と警告集計を含む。
 * preview → confirm でファイルを2回送る二段方式 (picking と同じ。サーバーに中間状態を持たない)
 */
function buildSummary(preview, match) {
  const tbFirst = preview.tbKey.split(',')[0];
  const warnCounts = {};
  for (const s of preview.slips) {
    for (const w of s.warns) warnCounts[w] = (warnCounts[w] || 0) + 1;
  }
  return {
    tbKey: preview.tbKey,
    tbLabel: preview.tbCount > 1 ? `${tbFirst} 他${preview.tbCount - 1}件` : tbFirst,
    sagyoDate: preview.sagyoDate,
    slipCount: preview.slipCount,
    lineCount: preview.lineCount,
    totalQty: preview.totalQty,
    warnCounts,
    warnLabels: WARN_LABELS,
    match: { status: match.status, diffs: match.diffs.slice(0, 30), diffCount: match.diffs.length },
    dateWarning: isStaleSagyoDate(preview.sagyoDate)
      ? `出荷作業日 (${preview.sagyoDate}) が今日ではありません。前日のファイルの可能性があります`
      : null,
    slips: preview.slips.map((s) => ({
      seq: s.seq, neSlipNo: s.neSlipNo, mall: s.mall,
      deliveryMethod: s.deliveryMethod, material: s.material,
      warns: s.warns, lineCount: s.lines.length,
      qty: s.lines.reduce((a, l) => a + l.qty, 0),
    })),
  };
}

function runImport(preview, req) {
  return importPackBatch(preview, {
    folderName: req.body.folder_name,
    overwrite: String(req.body.overwrite) === '1',
    matchAck: String(req.body.match_ack) === '1',
    dateAck: String(req.body.date_ack) === '1',
  }, req.session.email);
}

async function handleImport(req, res, buffer, extra = {}) {
  const preview = parseCs03003(buffer);
  // プレビュー表示用の突合 (confirm 時は importPackBatch がトランザクション内で再判定する)
  const match = checkPickingMatch(preview);
  const summary = { ...buildSummary(preview, match), ...extra };
  if (String(req.body.mode) !== 'confirm') {
    return res.json({ ok: true, mode: 'preview', ...summary });
  }
  const result = runImport(preview, req);
  res.json({
    ok: true, mode: 'confirm', ...summary,
    batchId: result.batchId, replaced: result.replaced, replayed: result.replayed || false,
  });
}

router.post('/admin/import', checkOrigin, (req, res, next) => {
  uploadCsv.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ error: `アップロード失敗: ${err.message}` });
    next();
  });
}, api(async (req, res) => {
  if (!req.file) throw new PackError(400, 'no_file', 'ファイルを選択してください');
  await handleImport(req, res, req.file.buffer);
}));

// ─── Drive取込 (出荷_no フォルダ・管理者) ───

router.get('/admin/import/drive-files', api(async (req, res) => {
  const files = await driveCall(() => listNouhinCsvFiles());
  res.json({ ok: true, files });
}));

/**
 * Driveファイルの取込。body(JSON): { file_id, parent_name, mode, folder_name, overwrite, match_ack }
 * confirm 時も最新の中身を取り直す (間に差し替わっても古い内容を確定しない — picking と同方式)
 */
router.post('/admin/import/drive', checkOrigin, api(async (req, res) => {
  const fileId = String(req.body.file_id || '').trim();
  if (!fileId) throw new PackError(400, 'no_file', 'ファイルを選択してください');
  const dl = await driveCall(() => downloadNouhinCsv(fileId));
  const parentName = String(req.body.parent_name || '');
  await handleImport(req, res, dl.buffer, {
    filename: dl.filename,
    folderNameSuggestion: (/^出荷_\d+$/.test(parentName) ? parentName : null)
      || deriveFolderName(dl.filename),
  });
}));

export default router;
