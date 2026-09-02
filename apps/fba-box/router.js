/**
 * FBA納品 箱詰め記録 (iPad) — router
 *
 * URL: /apps/fba-box/            … iPad 作業画面 (端末Cookie or ポータルセッション)
 *      /apps/fba-box/api/*       … 作業 API
 *      /apps/fba-box/admin       … 本社: 納品回の開始 (Excelアップ・突合)・進捗・管理
 *
 * アクセス制御は iroha-work / inbound-check と同じ2系統 (セッション or 端末Cookie fbx_device)。
 * server.js では requireAppAccess を掛けずに mount し、ここで全ルートを守る。
 *
 * 正本 = AI_reference『システム設計\FBA納品箱詰め記録_要件定義_20260902.md』
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  createDevice, verifyDevice, revokeDevice, listDevices,
  createEnrollCode, redeemEnrollCode, countEnrollAttempt, listActiveEnrollCodes, ENROLL_TTL_MS,
  checkEnrollRate, recordEnrollAttempt,
  listWorkers, getWorker, addWorker, setWorkerActive, setWorkerPin, verifyWorkerPin,
  listEvents, safeLogEvent, listMaterials,
  createRun, activateRun, setRunStatus, listRuns, getRun, getRunState,
  createBox, closeBox, reopenBox, listBoxContents,
  addPlacement, revokePlacement, setPlacementLayer,
  setRowWorkers, setRowShortage, clearRowShortage,
} from './db.js';
import { ingestPacklist, MAX_XLSX_BYTES } from './excel.js';
import { matchWorkbook, summarizeMatch } from './service.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();
export const APP_ID = 'fba-box';
const BASE = '/apps/fba-box';
const DEVICE_COOKIE = 'fbx_device';

// ─── 共通ヘルパ (iroha-work と同じ) ───
function readCookie(req, name) {
  const header = req.headers.cookie;
  if (!header) return null;
  for (const part of String(header).split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    if (part.slice(0, i).trim() === name) {
      try { return decodeURIComponent(part.slice(i + 1).trim()); } catch { return null; }
    }
  }
  return null;
}

function hasSessionAccess(req) {
  if (!req.session?.authenticated || !req.session?.email) return false;
  const allowed = req.session.allowedApps;
  return allowed === '*' || (Array.isArray(allowed) && allowed.includes(APP_ID));
}

function isAdmin(req) {
  return !!req.session?.authenticated && req.session.role === 'admin';
}

function checkOrigin(req, res, next) {
  const reject = () => res.status(403).json({ ok: false, error: 'bad_origin', message: '不正なオリジンからのリクエストです' });
  const src = req.headers.origin || req.headers.referer;
  if (!src || src === 'null') return reject();
  let host = null;
  try { host = new URL(src).host; } catch { /* 不正値は下で403 */ }
  if (!host || host !== req.headers.host) return reject();
  next();
}

function access(req, res, next) {
  if (req.path === '/manifest.json') return next();
  if (req.path === '/enroll' || req.path === '/enroll/redeem') return next();
  if (hasSessionAccess(req)) { req.fbxUser = req.session.email; return next(); }
  const device = verifyDevice(readCookie(req, DEVICE_COOKIE));
  if (device) { req.fbxDevice = device; return next(); }
  if (req.path.startsWith('/api/')) return res.status(401).json({ ok: false, error: 'unauthorized', message: 'ログインまたは端末登録が必要です' });
  if (req.session) req.session.returnTo = req.originalUrl;
  return res.redirect(`${BASE}/enroll`);
}

function requireSession(req, res, next) {
  if (hasSessionAccess(req)) return next();
  if (req.path.startsWith('/api/') || req.method !== 'GET') return res.status(403).json({ ok: false, error: 'session_required', message: 'この操作はポータルにログインして行ってください' });
  if (req.session) req.session.returnTo = req.originalUrl;
  return res.redirect('/login');
}

function requireAdmin(req, res, next) {
  if (!hasSessionAccess(req)) return requireSession(req, res, next);
  if (!isAdmin(req)) return res.status(403).json({ ok: false, error: 'forbidden', message: '管理者のみ実行できます' });
  next();
}

const api = fn => async (req, res) => {
  try { await fn(req, res); } catch (e) {
    console.error(`[fba-box] ${req.method} ${req.path}`, e);
    res.status(500).json({ ok: false, error: 'internal', message: e.message });
  }
};

const deviceLabelOf = (req) => (req.fbxDevice ? req.fbxDevice.label : (req.fbxUser ? `session:${req.fbxUser}` : null));
/** 冪等性キーの端末側キー (要件 F-2: UNIQUE(device_key, request_id)) */
const deviceKeyOf = (req) => (req.fbxDevice ? `dev:${req.fbxDevice.id}` : `ses:${req.fbxUser}`);

/** 作業者の解決。変更操作は誰がやったかを必須にする */
function resolveWorker(req) {
  const w = getWorker(req.body?.worker_id);
  if (!w) return { error: '作業者を選んでください' };
  if (!w.active) return { error: 'この作業者は無効になっています (職員の方に確認してください)' };
  return { worker: w };
}

/**
 * 職員限定操作の本人確認 (iroha-work と同じ考え方):
 *   ①ポータルセッション = 職員扱い ②端末Cookie = 職員 worker + PIN 一致のときのみ
 */
function requireStaff(req, worker) {
  if (hasSessionAccess(req)) return { ok: true };
  if (worker.worker_type !== 'staff') {
    return { ok: false, status: 403, body: { ok: false, error: 'staff_required', message: 'この操作は職員のみです (職員の名前を選んでください)' } };
  }
  const pinCheck = verifyWorkerPin(worker.id, req.body?.pin);
  if (!pinCheck.ok) {
    const st = pinCheck.error === 'pin_locked' ? 429 : 403;
    return { ok: false, status: st, body: { ok: false, ...pinCheck } };
  }
  return { ok: true };
}

router.use(access);

// ─── PWA manifest ───
router.get('/manifest.json', (req, res) => {
  res.json({
    name: 'FBA箱詰め', short_name: 'FBA箱詰め', start_url: `${BASE}/`, scope: `${BASE}/`,
    display: 'standalone', orientation: 'any', background_color: '#F4F5F7', theme_color: '#B5651D',
    icons: [
      { src: '/favicon.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/favicon.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
    ],
  });
});

// ─── 端末登録 (6桁コード・10分・1回) ───
router.get('/enroll', (req, res) => {
  if (verifyDevice(readCookie(req, DEVICE_COOKIE))) return res.redirect(`${BASE}/`);
  res.sendFile(path.join(__dirname, 'views', 'enroll.html'));
});

router.post('/enroll/redeem', checkOrigin, api((req, res) => {
  const code = String(req.body?.code || '').trim();
  const ip = req.ip || null;
  const gate = checkEnrollRate({ ip });
  if (!gate.allowed) {
    recordEnrollAttempt({ ip, ok: false });
    return res.status(429).json({ ok: false, error: gate.error, message: gate.message });
  }
  const r = redeemEnrollCode(code);
  recordEnrollAttempt({ ip, ok: r.ok });
  if (!r.ok) {
    countEnrollAttempt(code);
    return res.status(400).json(r);
  }
  res.cookie(DEVICE_COOKIE, r.token, {
    httpOnly: true,
    secure: process.env.NODE_ENV !== 'development',
    sameSite: 'lax',
    maxAge: 400 * 24 * 3600 * 1000,
    path: BASE,
  });
  res.json({ ok: true, label: r.label });
}));

router.post('/device/exit', checkOrigin, api((req, res) => {
  if (req.fbxDevice) revokeDevice(req.fbxDevice.id);
  res.clearCookie(DEVICE_COOKIE, { path: BASE });
  res.json({ ok: true, revoked: !!req.fbxDevice });
}));

// ─── 作業画面 (iPad) ───
router.get('/', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ─── 作業 API ───

/** 納品回一覧 (iPad は active のみ選べる。setup/done も理由付きで見せる) */
router.get('/api/runs', api((req, res) => {
  res.json({ ok: true, runs: listRuns(20), serverNow: new Date().toISOString() });
}));

/** 選択した納品回の全状態 */
router.get('/api/state', api((req, res) => {
  const runId = Number(req.query.run);
  if (!Number.isInteger(runId) || runId <= 0) return res.status(400).json({ ok: false, error: 'bad_request', message: 'run が必要です' });
  const state = getRunState(runId);
  if (!state) return res.status(404).json({ ok: false, error: 'not_found', message: '納品回が見つかりません' });
  res.json({
    ok: true, ...state,
    workers: listWorkers(),
    materials: listMaterials(),
    serverNow: new Date().toISOString(),
    me: { session: req.fbxUser || null, device: req.fbxDevice ? { id: req.fbxDevice.id, label: req.fbxDevice.label } : null, admin: isAdmin(req) },
  });
}));

/** 割当の追加 (F-2: 原子的残数検証+冪等性) */
router.post('/api/placements', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const r = addPlacement({
    runId: Number(req.body?.run_id), rowId: Number(req.body?.row_id), boxId: Number(req.body?.box_id),
    qty: req.body?.qty, expiry: req.body?.expiry, layer: req.body?.layer,
    worker: w.worker, deviceKey: deviceKeyOf(req), deviceLabel: deviceLabelOf(req),
    requestId: String(req.body?.request_id || ''),
  });
  if (!r.ok) {
    const st = { over_qty: 409, expiry_conflict: 409, box_closed: 409, wrong_group: 409, run_not_active: 409, not_found: 404 }[r.error] || 400;
    return res.status(st).json(r);
  }
  res.json(r);
}));

/** 割当の取消。利用者=自端末の直近のみ / 職員=PIN+理由 */
router.post('/api/placements/:id(\\d+)/revoke', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  let byStaff = false;
  if (req.body?.as_staff) {
    const gate = requireStaff(req, w.worker);
    if (!gate.ok) return res.status(gate.status).json(gate.body);
    byStaff = true;
  }
  const r = revokePlacement({
    placementId: Number(req.params.id), byStaff, reason: req.body?.reason,
    worker: w.worker, deviceKey: deviceKeyOf(req), deviceLabel: deviceLabelOf(req),
  });
  if (!r.ok) {
    const st = { staff_required: 403, not_found: 404, run_not_active: 409, reason_required: 400 }[r.error] || 400;
    return res.status(st).json(r);
  }
  res.json(r);
}));

/** 配置 (下/中/上) の後付け・付け替え */
router.post('/api/placements/:id(\\d+)/layer', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const r = setPlacementLayer({ placementId: Number(req.params.id), layer: req.body?.layer, worker: w.worker, deviceLabel: deviceLabelOf(req) });
  if (!r.ok) return res.status(r.error === 'not_found' ? 404 : 400).json(r);
  res.json(r);
}));

/** 新しい箱 */
router.post('/api/boxes', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const r = createBox({ packGroupId: Number(req.body?.pack_group_id), materialCode: req.body?.material_code, worker: w.worker, deviceLabel: deviceLabelOf(req) });
  if (!r.ok) {
    const st = { not_found: 404, run_not_active: 409, box_limit: 409 }[r.error] || 400;
    return res.status(st).json(r);
  }
  res.json(r);
}));

/** 箱の中身 (読み合わせ用) */
router.get('/api/boxes/:id(\\d+)', api((req, res) => {
  res.json({ ok: true, contents: listBoxContents(Number(req.params.id)) });
}));

/** 箱クローズ (読み合わせ後・実測kg必須) */
router.post('/api/boxes/:id(\\d+)/close', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const r = closeBox({
    boxId: Number(req.params.id), measuredKg: req.body?.measured_kg,
    closedReason: req.body?.closed_reason, cushionLevel: req.body?.cushion_level,
    worker: w.worker, deviceLabel: deviceLabelOf(req),
  });
  if (!r.ok) {
    const st = { not_found: 404, already_closed: 409, run_not_active: 409, empty_box: 409 }[r.error] || 400;
    return res.status(st).json(r);
  }
  res.json(r);
}));

/** 箱の再オープン (職員のみ・理由必須) */
router.post('/api/boxes/:id(\\d+)/reopen', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const gate = requireStaff(req, w.worker);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const r = reopenBox({ boxId: Number(req.params.id), reason: req.body?.reason, worker: w.worker, deviceLabel: deviceLabelOf(req) });
  if (!r.ok) {
    const st = { not_found: 404, not_closed: 409, run_not_active: 409, reason_required: 400 }[r.error] || 400;
    return res.status(st).json(r);
  }
  res.json(r);
}));

/** 行のラベル貼り担当 / 確認担当 */
router.post('/api/rows/:id(\\d+)/workers', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const body = {};
  if ('label_worker' in (req.body || {})) body.labelWorker = req.body.label_worker;
  if ('check_worker' in (req.body || {})) body.checkWorker = req.body.check_worker;
  const r = setRowWorkers({ rowId: Number(req.params.id), ...body, worker: w.worker, deviceLabel: deviceLabelOf(req) });
  if (!r.ok) return res.status(r.error === 'not_found' ? 404 : 409).json(r);
  res.json(r);
}));

/** 不足確定 (職員のみ) / 解除 */
router.post('/api/rows/:id(\\d+)/shortage', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const gate = requireStaff(req, w.worker);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const r = req.body?.clear
    ? clearRowShortage({ rowId: Number(req.params.id), worker: w.worker, deviceLabel: deviceLabelOf(req) })
    : setRowShortage({ rowId: Number(req.params.id), shortageQty: req.body?.qty, reason: req.body?.reason, worker: w.worker, deviceLabel: deviceLabelOf(req) });
  if (!r.ok) {
    const st = { not_found: 404, run_not_active: 409 }[r.error] || 400;
    return res.status(st).json(r);
  }
  res.json(r);
}));

// ─── 本社: 納品回管理 ───

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_XLSX_BYTES, files: 1 } });

router.get('/admin', requireSession, api(async (req, res) => {
  // picking-prep の直近実行 (納品回の元データ候補)。fba.db は server 起動時に初期化済み
  let pickingRuns = [];
  let pickingError = null;
  try {
    const fbaDb = await import('../fba-replenishment/db.js');
    pickingRuns = fbaDb.getPickingRuns(15);
  } catch (e) {
    pickingError = e.message;
  }
  res.render(path.join(__dirname, 'views/admin'), {
    title: 'FBA箱詰め記録 管理',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: isAdmin(req),
    base: BASE,
    runs: listRuns(30),
    pickingRuns, pickingError,
    workers: listWorkers(true),
    materials: listMaterials(),
    devices: isAdmin(req) ? listDevices() : [],
    enrollCodes: isAdmin(req) ? listActiveEnrollCodes() : [],
    events: listEvents(50),
  });
}));

/**
 * 納品回の開始: picking 実行を選び STA パックリストExcel をアップ → 解析 → 突合 → setup で作成。
 * 突合の警告 (qty_mismatch / excel_only / picking_only) は返すが作成は通す (Excel が正本)。
 * ブロック (識別キー重複・未知形式・保護セル) は 422 で作らない
 */
router.post('/admin/runs', requireSession, upload.single('excel'), checkOrigin, api(async (req, res) => {
  const sourceRunId = Number(req.body?.source_run_id);
  if (!Number.isInteger(sourceRunId) || sourceRunId <= 0) {
    return res.status(400).json({ ok: false, error: 'bad_request', message: 'ピッキング実行を選んでください' });
  }
  if (!req.file) return res.status(400).json({ ok: false, error: 'no_file', message: 'パックリストExcel (.xlsx) をアップロードしてください' });

  let pickingRun = null;
  try {
    const fbaDb = await import('../fba-replenishment/db.js');
    pickingRun = fbaDb.getPickingRun(sourceRunId);
  } catch (e) {
    return res.status(502).json({ ok: false, error: 'picking_db', message: `ピッキング実行を読めませんでした: ${e.message}` });
  }
  if (!pickingRun) return res.status(404).json({ ok: false, error: 'not_found', message: '指定のピッキング実行が見つかりません (保持100件を超えて消えた可能性)' });
  let planSheets = [];
  try {
    planSheets = JSON.parse(pickingRun.result || '{}').planSheets || [];
  } catch {
    return res.status(422).json({ ok: false, error: 'bad_picking_data', message: 'ピッキング実行のデータを解釈できませんでした' });
  }

  const ing = await ingestPacklist(req.file.buffer, req.file.originalname);
  if (!ing.ok) return res.status(422).json(ing);

  const match = matchWorkbook(ing.parsed, planSheets);
  if (!match.ok) {
    return res.status(422).json({ ok: false, error: 'match_blocked', message: 'Excel内に識別できない行があります (同一SKU/FNSKUの重複など)。手動転記に切り替えてください', issues: match.issues });
  }

  const title = pickingRun.delivery_date
    ? `${pickingRun.delivery_date} 納品分`
    : `実行#${pickingRun.id} (${String(pickingRun.run_at || '').slice(0, 16)})`;
  const created = createRun({
    sourceRunId, deliveryDate: pickingRun.delivery_date || null, title,
    planSourceHash: null,
    matchSummary: summarizeMatch(match),
    excelFile: {
      originalName: req.file.originalname, storedPath: ing.storedPath, sha256: ing.sha256,
      fingerprint: ing.parsed.fingerprint, metadata: ing.parsed.metadata,
    },
    groups: match.groups,
    createdBy: req.session.email,
  });
  if (!created.ok) return res.status(409).json(created);
  res.json({
    ok: true, runId: created.runId, issues: match.issues,
    fingerprintKnown: ing.fingerprintKnown,
    fingerprint: ing.parsed.fingerprint,
  });
}));

router.post('/admin/runs/:id(\\d+)/activate', requireSession, checkOrigin, api((req, res) => {
  const r = activateRun(Number(req.params.id), `session:${req.session.email}`);
  if (!r.ok) return res.status(r.error === 'not_found' ? 404 : 409).json(r);
  res.json(r);
}));

router.post('/admin/runs/:id(\\d+)/status', requireSession, checkOrigin, api((req, res) => {
  const r = setRunStatus(Number(req.params.id), String(req.body?.status || ''), `session:${req.session.email}`);
  if (!r.ok) return res.status(r.error === 'not_found' ? 404 : 409).json(r);
  res.json(r);
}));

/** 納品回の詳細 (進捗確認・突合結果) */
router.get('/admin/runs/:id(\\d+)', requireSession, api((req, res) => {
  const state = getRunState(Number(req.params.id));
  if (!state) return res.status(404).json({ ok: false, error: 'not_found', message: '納品回が見つかりません' });
  res.json({ ok: true, ...state });
}));

// ─── 管理者: 端末・登録コード・作業者 ───

router.post('/admin/enroll-codes', checkOrigin, requireAdmin, api((req, res) => {
  try {
    const r = createEnrollCode(req.body?.label, req.session.email);
    res.json({ ok: true, ...r, ttlMinutes: Math.round(ENROLL_TTL_MS / 60000) });
  } catch (e) {
    res.status(400).json({ ok: false, error: 'bad_request', message: e.message });
  }
}));

router.post('/admin/devices', checkOrigin, requireAdmin, api((req, res) => {
  const label = String(req.body?.label || '').trim();
  if (!label || label.length > 40) return res.status(400).json({ ok: false, error: 'bad_label', message: '端末名を1〜40文字で入力してください' });
  const { token, id: deviceId } = createDevice(label, req.session.email);
  req.session.destroy((err) => {
    if (err) {
      console.error('[fba-box] 端末登録: セッション破棄に失敗', err);
      try { revokeDevice(deviceId); } catch (e2) { console.error('[fba-box] 端末登録: 失効にも失敗', e2); }
      return res.status(500).json({ ok: false, error: 'session_destroy_failed', message: '登録を完了できませんでした (セッションを破棄できません)' });
    }
    res.cookie(DEVICE_COOKIE, token, {
      httpOnly: true,
      secure: process.env.NODE_ENV !== 'development',
      sameSite: 'lax',
      maxAge: 400 * 24 * 3600 * 1000,
      path: BASE,
    });
    res.json({ ok: true, loggedOut: true });
  });
}));

router.post('/admin/devices/:id(\\d+)/revoke', checkOrigin, requireAdmin, api((req, res) => {
  if (!revokeDevice(Number(req.params.id))) return res.status(404).json({ ok: false, error: 'not_found', message: '端末が見つかりません' });
  res.json({ ok: true });
}));

router.post('/admin/workers', checkOrigin, requireAdmin, api((req, res) => {
  const r = addWorker({ displayName: req.body?.display_name, workerType: req.body?.worker_type, actor: req.session.email });
  res.status(r.ok ? 200 : (r.error === 'duplicate' ? 409 : 400)).json(r);
}));

router.post('/admin/workers/:id(\\d+)/active', checkOrigin, requireAdmin, api((req, res) => {
  if (typeof req.body?.active !== 'boolean') return res.status(400).json({ ok: false, error: 'bad_request', message: 'active (true/false) が必要です' });
  if (!setWorkerActive(Number(req.params.id), req.body.active)) return res.status(404).json({ ok: false, error: 'not_found', message: '作業者が見つかりません' });
  res.json({ ok: true });
}));

router.post('/admin/workers/:id(\\d+)/pin', checkOrigin, requireAdmin, api((req, res) => {
  const r = setWorkerPin(Number(req.params.id), req.body?.pin, req.session.email);
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 400)).json(r);
}));

export default router;
