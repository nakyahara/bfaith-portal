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
  initPackingDB, getDB, jstToday, listPackBatches, getPackBatch, listPackSlips,
  listPackLinesBySlip, STATUS_LABELS, MATCH_LABELS,
  createDevice, verifyDevice, revokeDevice, listDevices,
} from './db.js';
import { getPollerStatus, markLedgerImported } from './drive-sync.js';
import {
  parseCs03003, importPackBatch, checkPickingMatch, PackError,
  deriveFolderName, isStaleSagyoDate, WARN_LABELS, getWorkState, applyEvent,
  PAUSE_REASONS, UNDO_REASONS, SHIP_CHANGE_REASONS, SHIP_CHANGE_METHOD_OPTIONS, lastDoneSeqOf, getDailySummary,
  listShipChanges, setShipChangeStatus,
} from './service.js';
import { notifyShipChange } from './notify.js';
import { listNouhinCsvFiles, downloadNouhinCsv, driveCall } from './drive.js';
// 商品画像は picking の楽天白抜きキャッシュ (pk_product_images) を共通部品として流用 (要件§7.1)
import { getImageMap, queueEnsureImages } from '../picking/images.js';

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

// ═══ アクセス制御 (picking と同じ2系統: ポータルセッション or 登録端末Cookie) ═══
//
//   ①ポータルセッション (管理画面はこちら必須)
//   ②登録済み端末Cookie (pk_pack_device) — 作業画面・作業APIのみ。作業者は名前タップで選択

const DEVICE_COOKIE = 'pk_pack_device';

/** Cookieヘッダから1つ取り出す (cookie-parser 非依存の最小実装)。 */
function readCookie(req, name) {
  const header = req.headers.cookie;
  if (!header) return null;
  for (const part of String(header).split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    if (part.slice(0, i).trim() === name) return decodeURIComponent(part.slice(i + 1).trim());
  }
  return null;
}

function packingAccess(req, res, next) {
  // PWA manifest はブラウザが Cookie 無しで取りにくる (認証不要の無害な静的情報)
  if (req.path === '/manifest.json') return next();
  if (req.session?.email) {
    const allowed = req.session.allowedApps;
    if (allowed === '*' || (Array.isArray(allowed) && allowed.includes('packing'))) return next();
    return res.status(403).send('packing へのアクセス権がありません');
  }
  const device = verifyDevice(readCookie(req, DEVICE_COOKIE));
  if (device) {
    req.packingDevice = device;   // 端末モード (作業画面のみ。admin系は requireAdmin で弾かれる)
    return next();
  }
  if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'ログインまたは端末登録が必要です' });
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
    username: req.session?.email || req.packingDevice?.label || '端末',
    displayName: req.session?.displayName,
    isAdmin: req.session?.role === 'admin',
    deviceMode: !req.session?.email,
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
    username: req.session?.email || req.packingDevice?.label || '端末',
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

// ─── 作業画面 (iPad・1伝票1画面 = 納品書PDF同等表示) ───

/** 作業者マスタは picking 所有の pk_workers を参照 (読み取りのみ — 要件§7.1 参照JOIN可)。 */
function listWorkers() {
  try {
    return getDB().prepare(
      'SELECT code, name FROM pk_workers WHERE active = 1 ORDER BY sort, code'
    ).all();
  } catch {
    return [];   // picking 側が未初期化の環境 (単体テスト等) では空
  }
}

/** state.slips に画像URLと表示用整形を付ける。 */
function decorateSlips(state) {
  const skus = [...new Set(state.slips.flatMap((s) => s.lines.map((l) => l.sku)))];
  const images = getImageMap(skus);
  // 取込時の解決キューが再起動等で消えていても、画面を開いた時点で自己修復する (picking と同じ)
  const missing = skus.map((s) => String(s ?? '').trim().toLowerCase())
    .filter((sku) => sku && !images.has(sku));
  if (missing.length > 0) queueEnsureImages(missing, `packing-batch:${state.batch.id}`);
  for (const s of state.slips) {
    for (const l of s.lines) {
      l.imageUrl = images.get(String(l.sku ?? '').trim().toLowerCase())?.url || null;
    }
  }
  return state;
}

router.get('/work/:id(\\d+)', (req, res) => {
  let state;
  try {
    state = decorateSlips(getWorkState(Number(req.params.id)));
  } catch (e) {
    if (e instanceof PackError) return res.status(e.status).send(e.message);
    throw e;
  }
  res.render(path.join(__dirname, 'views/work'), {
    title: `梱包 | ${state.batch.folder_name || state.batch.tb_key}`,
    displayName: req.session?.displayName,
    workers: listWorkers(),
    state,
    warnLabels: WARN_LABELS,
    pauseReasons: PAUSE_REASONS,
    undoReasons: UNDO_REASONS,
    shipChangeReasons: SHIP_CHANGE_REASONS,
    // 提案候補 = 固定リスト (中原さん指定 2026-08-16)。判定サービス委譲 (packing-dispatch) はPhase 3
    methodOptions: SHIP_CHANGE_METHOD_OPTIONS,
  });
});

/**
 * 作業イベントAPI。body: { op_id, event: start|next|takeover, slip_seq?, worker_name }
 * 作業者はサーバー側で検証する (Codex PR2-R1 high: 任意文字列を信用しない)。
 * 有効な値 = pk_workers の有効な作業者名 or ログインセッション本人。
 * 名前タップは本人認証ではない (picking と同じ性善説・社内10名運用) が、
 * 未登録名での操作・別担当者の無断続行はここで弾き、交代は takeover イベントに限定する
 */
router.post('/api/batches/:id(\\d+)/events', checkOrigin, api(async (req, res) => {
  const name = String(req.body.worker_name || '').trim();
  let worker = null;
  if (name) {
    if (!listWorkers().some((w) => w.name === name)) {
      throw new PackError(400, 'bad_worker', '作業者が無効です。選び直してください');
    }
    worker = name;
  } else if (req.session?.email) {
    worker = req.session.displayName || req.session.email;
  }
  const result = applyEvent(Number(req.params.id), {
    opId: req.body.op_id,
    event: req.body.event,
    slipSeq: req.body.slip_seq == null ? null : Number(req.body.slip_seq),
    clientAt: req.body.client_at || null,
    reason: req.body.reason || null,
    jumped: !!req.body.jumped,
    proposedMethod: req.body.proposed_method || null,
  }, worker);
  // ④ 配送方法変更は事務へ GChat 通知 (fail-soft・DBが正本。replayed の再送では送らない)
  if (req.body.event === 'ship_change' && !result.replayed) {
    const row = getDB().prepare(
      'SELECT * FROM pk_pack_ship_changes WHERE batch_id=? AND slip_seq=? ORDER BY id DESC LIMIT 1'
    ).get(Number(req.params.id), Number(req.body.slip_seq));
    if (row) {
      notifyShipChange({
        folderName: row.folder_name, neSlipNo: row.ne_slip_no,
        currentMethod: row.current_method, proposedMethod: row.proposed_method,
        reason: row.reason, worker,
      }).catch((e) => console.warn(`[packing-notify] 配送変更通知失敗 (${row.ne_slip_no}): ${e.message}`));
    }
  }
  res.json({ ok: true, ...result });
}));

// ─── ④ 配送方法変更の事務対応キュー (管理者) ───

router.get('/admin/ship-changes', requireAdmin, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_ship_changes'), {
    title: '配送方法変更 | 梱包支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    changes: listShipChanges(),
  });
});

router.post('/admin/ship-changes/:id(\\d+)/status', checkOrigin, requireAdmin, api(async (req, res) => {
  const status = String(req.body.status || '');
  if (!['accepted', 'rejected', 'completed'].includes(status)) {
    throw new PackError(400, 'bad_status', 'status が不正です');
  }
  const row = setShipChangeStatus(Number(req.params.id), status, req.session.email);
  res.json({ ok: true, id: row.id, status });
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
    slipCount: s.slips.length,
    doneSeqs: s.slips.filter((x) => x.status === 'done').map((x) => x.seq),
    heldSeqs: s.slips.filter((x) => x.status === 'held').map((x) => x.seq),
    lastDoneSeq: lastDoneSeqOf(Number(req.params.id)),
    pauseReason: s.batch.pause_reason || null,
  });
}));

/** 明細画像URLマップ (作業画面のポーリング用。取込直後は解決がバックグラウンド進行中)。 */
router.get('/api/batches/:id(\\d+)/images', api(async (req, res) => {
  const state = getWorkState(Number(req.params.id));
  const skus = [...new Set(state.slips.flatMap((s) => s.lines.map((l) => l.sku)))];
  const images = getImageMap(skus);
  const bySku = {};
  for (const sku of skus) {
    const hit = images.get(String(sku ?? '').trim().toLowerCase());
    if (hit?.url) bySku[sku] = hit.url;
  }
  res.json({ ok: true, images: bySku });
}));

// ─── 日次サマリ (管理者) ───
router.get('/admin/summary', requireAdmin, (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  res.render(path.join(__dirname, 'views/admin_summary'), {
    title: '梱包サマリ',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    summary: getDailySummary(workDate),
    statusLabels: STATUS_LABELS,
  });
});

// ─── PWA manifest (ホーム画面追加用) ───
router.get('/manifest.json', (req, res) => {
  res.json({
    name: '梱包支援',
    short_name: '梱包',
    start_url: '/apps/packing/',
    display: 'standalone',
    orientation: 'any',   // 梱包台は横向き据置が基本 (要件§6) だが縦でも使えるように
    background_color: '#e9ecef',
    theme_color: '#212529',
    icons: [{ src: '/favicon.png', sizes: '192x192', type: 'image/png' }],
  });
});

// ─── 端末管理 (管理者・セッション必須) ───

router.get('/admin/devices', requireAdmin, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_devices'), {
    title: '端末管理 | 梱包支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    devices: listDevices(),
  });
});

/**
 * この端末 (iPad) を登録する。発行トークンは httpOnly Cookie としてこの端末にだけ渡す。
 * ⭐登録と同時に管理者セッションを破棄する (共用端末に管理者権限を残さない — picking と同規約)。
 * 🚨ホーム画面に追加したPWA内で実行すること (SafariとPWAはCookie保存領域が別)
 */
router.post('/admin/devices', checkOrigin, requireAdmin, api(async (req, res) => {
  const label = String(req.body.label || '').trim();
  if (!label || label.length > 40) throw new PackError(400, 'bad_label', '端末名を1〜40文字で入力してください');
  const { token, id: deviceId } = createDevice(label, req.session.email);
  res.cookie(DEVICE_COOKIE, token, {
    httpOnly: true,
    secure: process.env.NODE_ENV !== 'development',
    sameSite: 'lax',
    maxAge: 400 * 24 * 3600 * 1000,
    path: '/apps/packing',
  });
  // セッション破棄の失敗を握り潰さない (Codex high: 共用端末に管理者セッションが残ったまま
  // 「登録成功」を返すと、権限破棄の安全要件が満たされない)。失敗時は発行した端末も無効化する
  req.session.destroy((err) => {
    if (err) {
      revokeDevice(deviceId);   // 発行した端末を確実に無効化 (id直指定。一覧末尾頼みは並行登録で誤爆する)
      res.clearCookie(DEVICE_COOKIE, { path: '/apps/packing' });
      console.error('[packing] 端末登録時のセッション破棄に失敗:', err);
      return res.status(500).json({ error: 'セッションの破棄に失敗したため登録を取り消しました。もう一度実行してください' });
    }
    res.json({ ok: true, loggedOut: true });
  });
}));

router.post('/admin/devices/:id(\\d+)/revoke', checkOrigin, requireAdmin, api(async (req, res) => {
  if (!revokeDevice(Number(req.params.id))) throw new PackError(404, 'not_found', '端末が見つかりません');
  res.json({ ok: true });
}));

// ─── 取込画面 (管理者) ───
router.get('/admin/import', requireAdmin, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_import'), {
    title: '納品書CSV取込 | 梱包支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
  });
});

/** 自動ポーリングの状態 (standaloneで稼働)。取込画面が表示に使う。 */
router.get('/admin/import/poller-status', requireAdmin, api(async (req, res) => {
  const s = getPollerStatus();
  const db = getDB();
  const recent = db.prepare(`
    SELECT filename, folder_name, status, error, attempts, processed_at FROM pk_pack_drive_imports
    ORDER BY processed_at DESC LIMIT 12
  `).all();
  const failedCount = db.prepare(
    "SELECT COUNT(*) c FROM pk_pack_drive_imports WHERE status='failed'"
  ).get().c;
  res.json({ ok: true, poller: s, recent, failedCount });
}));

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

async function handleImport(req, res, buffer, extra = {}, onImported = null) {
  const preview = parseCs03003(buffer);
  // プレビュー表示用の突合 (confirm 時は importPackBatch がトランザクション内で再判定する)
  const match = checkPickingMatch(preview);
  const summary = { ...buildSummary(preview, match), ...extra };
  if (String(req.body.mode) !== 'confirm') {
    return res.json({ ok: true, mode: 'preview', ...summary });
  }
  const result = runImport(preview, req);
  if (onImported) onImported(result.batchId);
  // 楽天白抜き画像の解決はバックグラウンドで (取込応答を待たせない・失敗しても取込は成立)
  queueEnsureImages(
    [...new Set(preview.slips.flatMap((s) => s.lines.map((l) => l.sku)))],
    `packing:${preview.tbKey.split(',')[0]}`,
  );
  res.json({
    ok: true, mode: 'confirm', ...summary,
    batchId: result.batchId, replaced: result.replaced, replayed: result.replayed || false,
  });
}

router.post('/admin/import', checkOrigin, requireAdmin, (req, res, next) => {
  uploadCsv.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ error: `アップロード失敗: ${err.message}` });
    next();
  });
}, api(async (req, res) => {
  if (!req.file) throw new PackError(400, 'no_file', 'ファイルを選択してください');
  await handleImport(req, res, req.file.buffer);
}));

// ─── Drive取込 (出荷_no フォルダ・管理者) ───

router.get('/admin/import/drive-files', requireAdmin, api(async (req, res) => {
  const files = await driveCall(() => listNouhinCsvFiles());
  res.json({ ok: true, files });
}));

/**
 * Driveファイルの取込。body(JSON): { file_id, parent_name, mode, folder_name, overwrite, match_ack }
 * confirm 時も最新の中身を取り直す (間に差し替わっても古い内容を確定しない — picking と同方式)
 */
router.post('/admin/import/drive', checkOrigin, requireAdmin, api(async (req, res) => {
  const fileId = String(req.body.file_id || '').trim();
  if (!fileId) throw new PackError(400, 'no_file', 'ファイルを選択してください');
  const dl = await driveCall(() => downloadNouhinCsv(fileId));
  const parentName = String(req.body.parent_name || '');
  await handleImport(req, res, dl.buffer, {
    filename: dl.filename,
    folderNameSuggestion: (/^出荷_\d+$/.test(parentName) ? parentName : null)
      || deriveFolderName(dl.filename),
  }, (batchId) => markLedgerImported({
    fileId, modifiedTime: dl.modified_time, filename: dl.filename,
    folderName: /^出荷_\d+$/.test(parentName) ? parentName : deriveFolderName(dl.filename),
    batchId,
  }));
}));

export default router;
