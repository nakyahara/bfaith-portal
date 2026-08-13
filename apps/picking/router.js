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
  createDevice, verifyDevice, revokeDevice, listDevices,
  listWorkers, getWorker, addWorker, setWorkerActive,
} from './db.js';
import {
  parseCs03002, importBatch, formatLocation, PkError, getWorkState, applyEvent,
  deriveFolderName, isStaleInstructDate, getDailySummary, PAUSE_REASONS,
} from './service.js';
import { notifyShortage } from './notify.js';
import { allPatternNames } from './patterns.js';
import { enqueueBatchSync, fetchNotionWorkerNames, STATUS_PICKING, STATUS_PICKED } from './notion.js';
import { queueEnsureImages, getImageMap } from './images.js';
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

// ═══ アクセス制御 (2026-08-12 中原さん方針: 倉庫の共用端末は一度登録すればログイン不要) ═══
//
// 2系統の認証を受け付ける:
//   ①ポータルセッション (従来どおり。管理画面はこちら必須)
//   ②登録済み端末Cookie (pk_device) — 作業画面・作業APIのみ。作業者は名前タップで選択し、
//     計測には選択した作業者名を記録する (個人アカウントは持たない)
// server.js のマウントでは requireAppAccess を使わず、この router 内の pickingAccess で制御する。

const DEVICE_COOKIE = 'pk_device';

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

function hasSessionAccess(req) {
  if (!req.session?.email) return false;
  const allowed = req.session.allowedApps;
  return allowed === '*' || (Array.isArray(allowed) && allowed.includes('picking'));
}

/** 全ルート共通の入口。セッション or 登録端末のどちらかが必要。 */
function pickingAccess(req, res, next) {
  // PWA manifest はブラウザが Cookie 無しで取りにくる (認証不要の無害な静的情報)
  if (req.path === '/manifest.json') return next();
  if (hasSessionAccess(req)) return next();
  const device = verifyDevice(readCookie(req, DEVICE_COOKIE));
  if (device) {
    req.pickingDevice = device;   // 端末モード (作業画面のみ。admin系は requireAdmin で弾かれる)
    return next();
  }
  if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'ログインまたは端末登録が必要です' });
  // ログイン後に元のURLへ戻す (portal/standalone 両方の /login が session.returnTo を見る)
  if (req.session) req.session.returnTo = req.originalUrl;
  return res.redirect('/login');
}

/**
 * 作業者の識別。セッションなら email、端末モードなら選択された作業者コード
 * (body/query の worker_code。pk_workers の有効な作業者のみ受け付ける)。
 * 計測の worker 列には「表示名」を入れる (現行Notionの担当者selectと同じ粒度)。
 *
 * ⚠ 端末モードの作業者は自己申告 (名前タップ) であり、本人認証ではない。
 * 現行Notionの担当者selectと同等の性善説運用 (社内10名) を前提とし、
 * 計測記録は監査証跡としては扱わない (中原さん了承の設計判断 2026-08-12)。
 */
function resolveWorker(req) {
  // 有効な worker_code があれば最優先 (ログイン中でも「実際にピッキングする人」を選ぶ運用 —
  // 中原さん要望 8/12。計測はログイン者ではなく選択された作業者に紐づける)
  const code = String(req.body?.worker_code || req.query?.worker_code || '').trim();
  if (code) {
    const w = getWorker(code);
    if (w && w.active) return { id: w.name, name: w.name };
    // 無効コード: セッションがあれば本人として続行、端末モードは選び直しを要求
    if (!req.session?.email) {
      throw new PkError(400, 'bad_worker', '作業者が無効です。切替から選び直してください');
    }
  }
  if (req.session?.email) return { id: req.session.email, name: req.session.displayName || req.session.email };
  throw new PkError(400, 'no_worker', '作業者を選択してください');
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
  if (req.session?.role !== 'admin') {
    return res.status(403).json({ error: '管理者のみ実行できます' });
  }
  next();
}

// router 全体に適用 (server.js は requireAppAccess を付けずに mount する)
router.use(pickingAccess);
// admin系は個別の requireAdmin に加えて prefix 一括でも守る
// (将来ルートを追加したときの付け忘れを構造的に防ぐ — Codex指摘)
router.use('/admin', requireAdmin);

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

// ─── バッチ一覧 (セッション or 登録端末) ───
router.get('/', (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const batches = listBatches(workDate)
    .filter((b) => b.status !== 'cancelled' || b.work_date === workDate);
  res.render(path.join(__dirname, 'views/batches'), {
    title: 'ピッキング支援',
    username: req.session?.email || req.pickingDevice?.label || '端末',
    displayName: req.session?.displayName,
    isAdmin: req.session?.role === 'admin',
    deviceMode: !req.session?.email,
    workers: listWorkers(),
    workDate,
    batches,
    statusLabels: STATUS_LABELS,
  });
});

// ─── バッチ詳細 (明細確認・紙PDFとの突合用) ───
router.get('/batches/:id(\\d+)', (req, res) => {
  const batch = getBatch(Number(req.params.id));
  if (!batch) return res.status(404).send('バッチが見つかりません');
  const rawLines = listLines(batch.id);
  const images = getImageMap(rawLines.map((l) => l.sku));
  const lines = rawLines.map((l) => ({
    ...l,
    locationLabel: formatLocation(l.block, l.location),
    imageUrl: images.get(String(l.sku ?? '').trim().toLowerCase())?.url || null,
  }));
  res.render(path.join(__dirname, 'views/batch_detail'), {
    title: `${batch.hikiate_class} | ピッキング支援`,
    username: req.session?.email || req.pickingDevice?.label || '端末',
    displayName: req.session?.displayName,
    isAdmin: req.session?.role === 'admin',
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
    // セッションなら worker は確定 (email)。端末モードは null = 画面側が作業者選択を出す
    worker: req.session?.email || null,
    displayName: req.session?.displayName,
    workers: listWorkers(),
    pauseReasons: PAUSE_REASONS,
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
  const worker = resolveWorker(req);
  const result = applyEvent(batchId, {
    opId: req.body.op_id,
    event: req.body.event,
    lineSeq: req.body.line_seq == null ? null : Number(req.body.line_seq),
    clientAt: req.body.client_at,
    undoOpId: req.body.undo_op_id || null,
    shortageQty: req.body.shortage_qty == null ? null : Number(req.body.shortage_qty),
    pauseReason: req.body.pause_reason || null,
  }, worker.id);
  // 欠品は管理者チャットへ即時通知 (fail-soft・要件§5.6)。replayed の再送では通知しない。
  // outbox は持たない設計判断: 重複は「backして再度欠品にした」正当な操作のみ・
  // 欠落は webhook 障害時のみ (warnログ)。正確な欠品一覧はサマリ画面が正
  if (req.body.event === 'shortage' && !result.replayed) {
    const batch = getBatch(batchId);
    const line = listLines(batchId).find((l) => l.seq === Number(req.body.line_seq));
    if (batch && line) {
      notifyShortage({
        batch,
        line: { ...line, locationLabel: formatLocation(line.block, line.location) },
        worker: worker.name,
        shortageQty: line.shortage_qty ?? line.qty,
      }).catch((e) => console.warn(`[picking-notify] 欠品通知失敗 (${line.sku}): ${e.message}`));
    }
  }
  // Notion連携 (fail-soft)。replayed では動かさない。ラベルは送信直前にバッチの
  // 最新状態から決める (イベント時点のラベルだと並行PATCHの順序逆転で巻き戻る)
  if (!result.replayed && result.transition) {
    enqueueBatchSync(batchId, () => {
      const b = getBatch(batchId);
      if (!b) return null;
      const activeSec = b.started_at && b.finished_at
        ? Math.max(0, Math.round((Date.parse(b.finished_at) - Date.parse(b.started_at)) / 1000) - (b.paused_total_sec || 0))
        : null;
      return {
        folderName: b.folder_name,
        workDate: b.work_date,   // 日跨ぎ作業でも取込日のカードを動かす
        label: NOTION_STATUS_BY_BATCH[b.status] || null,
        workerName: b.worker,    // ピッキング担当者selectへ (email形式は notion.js 側で除外)
        // 時間系プロパティ用 (Notion側に存在するものだけ書かれる)
        times: {
          startedAt: b.started_at,
          finishedAt: b.finished_at,
          activeSec,
          lineCount: b.line_count,
        },
      };
    });
  }
  res.json({ ok: true, ...result });
}));

/**
 * バッチ明細の画像URLマップ (作業画面のポーリング用)。
 * 取込直後は解決がバックグラウンドで進行中のため、画面側が数回だけ取得しにくる
 */
router.get('/api/batches/:id(\\d+)/images', api(async (req, res) => {
  const batch = getBatch(Number(req.params.id));
  if (!batch) throw new PkError(404, 'not_found', 'バッチが見つかりません');
  const lines = listLines(batch.id);
  const images = getImageMap(lines.map((l) => l.sku));
  const bySeq = {};
  for (const l of lines) {
    const hit = images.get(String(l.sku ?? '').trim().toLowerCase());
    if (hit?.url) bySeq[l.seq] = hit.url;
  }
  res.json({ ok: true, images: bySeq });
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
    orientation: 'any',   // 前腕ホルダーは横装着が実運用 (2026-08-12 実機確認)
    background_color: '#111418',
    theme_color: '#111418',
    icons: [{ src: '/favicon.png', sizes: '192x192', type: 'image/png' }],
  });
});

// ─── 本日サマリ (管理者) ───
router.get('/admin/summary', requireAdmin, (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  res.render(path.join(__dirname, 'views/admin_summary'), {
    title: 'ピッキングサマリ',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    summary: getDailySummary(workDate),
  });
});

// ─── 端末・作業者管理 (管理者・セッション必須) ───

router.get('/admin/devices', requireAdmin, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_devices'), {
    title: '端末・作業者管理 | ピッキング支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    devices: listDevices(),
    workers: listWorkers(true),
  });
});

/**
 * この端末を登録する。発行したトークンは httpOnly Cookie としてこの端末にだけ渡す
 * (画面にも他の誰にも見せない)。倉庫のiPhoneで管理者がログインして1回だけ実行する。
 * ⭐登録と同時に管理者セッションを破棄する (「登録してログアウト」を原子化)。
 *   共用端末に管理者セッションが残ると、渡した相手が取込・管理APIを触れてしまうため (Codex high)
 */
router.post('/admin/devices', checkOrigin, requireAdmin, api(async (req, res) => {
  const label = String(req.body.label || '').trim();
  if (!label || label.length > 40) throw new PkError(400, 'bad_label', '端末名を1〜40文字で入力してください');
  const token = createDevice(label, req.session.email);
  res.cookie(DEVICE_COOKIE, token, {
    httpOnly: true,
    // 明示的に development と宣言された環境以外は常に Secure (NODE_ENV 未設定でも安全側)
    secure: process.env.NODE_ENV !== 'development',
    sameSite: 'lax',
    maxAge: 400 * 24 * 3600 * 1000,   // ブラウザ上限 (~400日)。サーバー側でも同TTLを検証
    path: '/apps/picking',
  });
  req.session.destroy(() => res.json({ ok: true, loggedOut: true }));
}));

router.post('/admin/devices/:id(\\d+)/revoke', checkOrigin, requireAdmin, api(async (req, res) => {
  if (!revokeDevice(Number(req.params.id))) throw new PkError(404, 'not_found', '端末が見つかりません');
  res.json({ ok: true });
}));

/**
 * Notionの「ピッキング担当者」selectの選択肢を作業者マスタへ取り込む
 * (名前の表記をNotionと完全一致させる = カード連携で選択肢が増殖しない)。
 */
router.post('/admin/workers/import-notion', checkOrigin, requireAdmin, api(async (req, res) => {
  if (!process.env.PICKING_NOTION_TOKEN) {
    throw new PkError(400, 'notion_disabled', 'PICKING_NOTION_TOKEN が未設定です');
  }
  const names = await fetchNotionWorkerNames();
  if (names.length === 0) {
    throw new PkError(404, 'no_options', 'Notionの「ピッキング担当者」プロパティに選択肢が見つかりません');
  }
  const existing = new Set(listWorkers(true).map((w) => w.name));
  const added = [];
  for (const name of names) {
    if (!existing.has(name)) { addWorker(name); added.push(name); }
  }
  res.json({ ok: true, total: names.length, added });
}));

router.post('/admin/workers', checkOrigin, requireAdmin, api(async (req, res) => {
  const name = String(req.body.name || '').trim();
  if (!name || name.length > 20) throw new PkError(400, 'bad_name', '作業者名を1〜20文字で入力してください');
  res.json({ ok: true, code: addWorker(name) });
}));

router.post('/admin/workers/:code/toggle', checkOrigin, requireAdmin, api(async (req, res) => {
  const w = getWorker(String(req.params.code));
  if (!w) throw new PkError(404, 'not_found', '作業者が見つかりません');
  setWorkerActive(w.code, !w.active);
  res.json({ ok: true });
}));

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
  const tbFirst = preview.tbNo.split(',')[0];
  return {
    tbNo: preview.tbNo,
    tbLabel: preview.tbCount > 1 ? `${tbFirst} 他${preview.tbCount - 1}件` : tbFirst,
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
  // 楽天白抜き画像の解決はバックグラウンドで (取込応答を待たせない・失敗しても取込は成立)
  queueEnsureImages(preview.lines.map((l) => l.sku), preview.tbNo.split(',')[0]);
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
  queueEnsureImages(preview.lines.map((l) => l.sku), preview.tbNo.split(',')[0]);
  res.json({ ok: true, mode: 'confirm', ...summary, ...result });
}));

export default router;
