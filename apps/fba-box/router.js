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
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  createDevice, verifyDevice, revokeDevice, listDevices,
  createEnrollCode, redeemEnrollCode, countEnrollAttempt, listActiveEnrollCodes, ENROLL_TTL_MS,
  checkEnrollRate, recordEnrollAttempt,
  listWorkers, getWorker, addWorker, setWorkerActive, setWorkerPin, verifyWorkerPin, countStaffWithPin,
  listEvents, safeLogEvent, listMaterials, upsertMaterial,
  createRun, activateRun, setRunStatus, listRuns, getRun, getRunState,
  createBox, closeBox, reopenBox, voidBox, listBoxContents,
  addPlacement, revokePlacement, setPlacementLayer,
  setRowWorkers, setRowShortage, clearRowShortage,
  exportReadiness, buildExportPayload, recordExport, listExports, getExport, markStaUploaded,
} from './db.js';
import { ingestPacklist, writePacklist, MAX_XLSX_BYTES } from './excel.js';
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
 *   ①ポータルセッション = 職員扱い (ポータル利用者は B-Faith 側スタッフのみ。
 *     監査上の操作主体は各イベントの device_label = `session:メール` で追える —
 *     worker_name は現場の作業帰属で、権限行使者とは別の欄という整理。Codex PR1 #9 は
 *     この記録で足りると業務判断)
 *   ②端末Cookie = 職員 worker + PIN 一致のときのみ
 */
function requireStaff(req, worker) {
  if (hasSessionAccess(req)) return { ok: true, via: 'session' };
  if (worker.worker_type !== 'staff') {
    return { ok: false, status: 403, body: { ok: false, error: 'staff_required', message: 'この操作は職員のみです (職員の名前を選んでください)' } };
  }
  const pinCheck = verifyWorkerPin(worker.id, req.body?.pin);
  if (!pinCheck.ok) {
    const st = pinCheck.error === 'pin_locked' ? 429 : 403;
    return { ok: false, status: st, body: { ok: false, ...pinCheck } };
  }
  return { ok: true, via: 'pin' };
}

/**
 * 名簿 (作業者・PIN) を iPad から編集するためのゲート (PR2, 中原さん指示「登録は自分たちで」):
 *   ①ポータルセッション = OK
 *   ②端末Cookie: PIN 設定済みの有効な職員が 0 人 = 初期登録 (bootstrap) として無ゲート
 *     (端末登録自体が本社発行の6桁コードで守られている)
 *   ③それ以外 = auth_worker_id (職員) + auth_pin の一致
 */
function rosterGate(req) {
  if (hasSessionAccess(req)) return { ok: true, via: 'session', approvedBy: `session:${req.fbxUser}` };
  if (countStaffWithPin() === 0) return { ok: true, via: 'bootstrap', approvedBy: null };
  const w = getWorker(req.body?.auth_worker_id);
  if (!w || !w.active || w.worker_type !== 'staff') {
    return { ok: false, status: 403, body: { ok: false, error: 'staff_required', message: '名簿の変更は職員のPINが必要です (職員を選んでPINを入れてください)' } };
  }
  const pinCheck = verifyWorkerPin(w.id, req.body?.auth_pin);
  if (!pinCheck.ok) return { ok: false, status: pinCheck.error === 'pin_locked' ? 429 : 403, body: { ok: false, ...pinCheck } };
  return { ok: true, via: 'pin', approvedBy: w.display_name };
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

/** 出荷前チェック (iPad の「まとめ」表示用。読み取りのみ) */
router.get('/api/readiness', api((req, res) => {
  const r = exportReadiness(Number(req.query.run));
  if (!r) return res.status(404).json({ ok: false, error: 'not_found', message: '納品回が見つかりません' });
  res.json({ ok: true, readiness: r });
}));

// ─── 名簿 (作業者・PIN) — iPad から職員が編集する (PR2) ───

router.get('/api/roster', api((req, res) => {
  res.json({
    ok: true, workers: listWorkers(true), bootstrap: countStaffWithPin() === 0,
    me: { session: req.fbxUser || null, admin: isAdmin(req) },
  });
}));

router.post('/api/workers', checkOrigin, api((req, res) => {
  const gate = rosterGate(req);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const r = addWorker({ displayName: req.body?.display_name, workerType: req.body?.worker_type, actor: gate.approvedBy || deviceLabelOf(req) });
  if (!r.ok) return res.status(r.error === 'duplicate' ? 409 : 400).json(r);
  safeLogEvent({ action: 'worker_add', workerId: r.id, workerName: String(req.body?.display_name || '').trim(), deviceLabel: deviceLabelOf(req), ok: true,
    payload: { workerType: req.body?.worker_type, via: gate.via, approvedBy: gate.approvedBy } });
  res.json(r);
}));

router.post('/api/workers/:id(\\d+)/pin', checkOrigin, api((req, res) => {
  const gate = rosterGate(req);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const r = setWorkerPin(Number(req.params.id), req.body?.pin, `${deviceLabelOf(req)} (${gate.via}${gate.approvedBy ? ':' + gate.approvedBy : ''})`);
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 400)).json(r);
}));

router.post('/api/workers/:id(\\d+)/active', checkOrigin, api((req, res) => {
  if (typeof req.body?.active !== 'boolean') return res.status(400).json({ ok: false, error: 'bad_request', message: 'active (true/false) が必要です' });
  const gate = rosterGate(req);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const target = getWorker(Number(req.params.id));
  if (!target) return res.status(404).json({ ok: false, error: 'not_found', message: '作業者が見つかりません' });
  // 端末からは「PIN を持つ最後の職員」を無効にできない (名簿ゲートが空になり誰でも登録できる状態に戻るため)
  if (!req.body.active && gate.via !== 'session' && target.worker_type === 'staff' && target.active) {
    const pinStaff = listWorkers().filter((w) => w.worker_type === 'staff' && w.pin_set);
    if (pinStaff.length <= 1 && pinStaff.some((w) => w.id === target.id)) {
      return res.status(409).json({ ok: false, error: 'last_staff', message: 'PINを持つ職員が他にいないため無効にできません (先に別の職員を登録してPINを設定してください)' });
    }
  }
  setWorkerActive(target.id, req.body.active);
  safeLogEvent({ action: 'worker_active', workerId: target.id, workerName: target.display_name, deviceLabel: deviceLabelOf(req), ok: true,
    payload: { active: req.body.active, via: gate.via, approvedBy: gate.approvedBy } });
  res.json({ ok: true });
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
    const st = { over_qty: 409, expiry_conflict: 409, box_closed: 409, wrong_group: 409, run_not_active: 409, idempotency_conflict: 409, not_found: 404 }[r.error] || 400;
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

/** 配置 (下/中/上) の後付け・付け替え。閉じた箱は職員 (as_staff+PIN) のみ */
router.post('/api/placements/:id(\\d+)/layer', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  let byStaff = false;
  if (req.body?.as_staff) {
    const gate = requireStaff(req, w.worker);
    if (!gate.ok) return res.status(gate.status).json(gate.body);
    byStaff = true;
  }
  const r = setPlacementLayer({ placementId: Number(req.params.id), layer: req.body?.layer, byStaff, worker: w.worker, deviceLabel: deviceLabelOf(req) });
  if (!r.ok) return res.status({ not_found: 404, staff_required: 403 }[r.error] || 400).json(r);
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

/** 使わなかった箱の取消 (職員のみ・理由必須・中身は空であること) */
router.post('/api/boxes/:id(\\d+)/void', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const gate = requireStaff(req, w.worker);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const r = voidBox({ boxId: Number(req.params.id), reason: req.body?.reason, worker: w.worker, deviceLabel: deviceLabelOf(req) });
  if (!r.ok) {
    const st = { not_found: 404, not_empty: 409, run_not_active: 409, reason_required: 400 }[r.error] || 400;
    return res.status(st).json(r);
  }
  res.json(r);
}));

/**
 * 箱札 (要件 F-8): A4 横 1箱1面。iPad の共有→プリント (AirPrint) か、本社で印刷して同梱。
 * ?run=ID (&group=GID | &box=BID)。PDF 生成ライブラリは使わずブラウザ印刷 (日本語フォント埋め込み不要)
 */
router.get('/print/boxes', api((req, res) => {
  const state = getRunState(Number(req.query.run));
  if (!state) return res.status(404).send('納品回が見つかりません');
  const groupId = req.query.group ? Number(req.query.group) : null;
  const boxId = req.query.box ? Number(req.query.box) : null;
  const boxes = state.boxes.filter((b) => b.status !== 'void' && (!groupId || b.pack_group_id === groupId) && (!boxId || b.id === boxId));
  const groupById = new Map(state.groups.map((g) => [g.id, g]));
  res.render(path.join(__dirname, 'views/print-boxes'), {
    run: state.run, boxes: boxes.map((b) => ({ ...b, group: groupById.get(b.pack_group_id) })),
    printedAt: new Date().toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' }),
  });
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
    materials: listMaterials(true),
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

// ─── 本社: 出荷前チェック → Excel 出力 → STA アップ済み (PR2) ───

router.get('/admin/runs/:id(\\d+)/readiness', requireSession, api((req, res) => {
  const readiness = exportReadiness(Number(req.params.id));
  if (!readiness) return res.status(404).json({ ok: false, error: 'not_found', message: '納品回が見つかりません' });
  res.json({ ok: true, readiness, exports: listExports(Number(req.params.id)) });
}));

/**
 * Excel 出力: チェック (blockers 無し) → 書き込み指示を組む → python で原本に書く (独自検算込み) → 版として記録。
 * 書いている最中に iPad で変更が入った場合は data_version がずれ、応答と一覧に「旧版」と出る
 */
router.post('/admin/runs/:id(\\d+)/exports', requireSession, checkOrigin, api(async (req, res) => {
  const runId = Number(req.params.id);
  const payload = buildExportPayload(runId);
  if (!payload.ok) return res.status(payload.error === 'not_found' ? 404 : 409).json(payload);
  const w = await writePacklist({ templatePath: payload.excelFile.stored_path, sheets: payload.sheets, fileTag: `run${runId}` });
  if (!w.ok) {
    safeLogEvent({ runId, action: 'excel_export', ok: false, error: `${w.error}: ${w.message}`, deviceLabel: `session:${req.session.email}` });
    return res.status(422).json(w);
  }
  const rec = recordExport({
    runId, excelFileId: payload.excelFile.id, dataVersion: payload.snapshot.dataVersion,
    fileName: payload.excelFile.original_name || `packlist-run${runId}.xlsx`, storedPath: w.outputPath, sha256: w.sha256,
    snapshot: payload.snapshot, verify: w.verify, createdBy: req.session.email,
  });
  if (!rec.ok) return res.status(409).json(rec);
  res.json({ ok: true, exportId: rec.exportId, stale: rec.stale, written: w.written, verify: w.verify,
    downloadUrl: `${BASE}/admin/exports/${rec.exportId}/download`, warnings: payload.snapshot.warnings });
}));

/** 出力ファイルのダウンロード (ファイル名は原本と同じ — STA はファイル名/タブ構造の変更不可) */
router.get('/admin/exports/:id(\\d+)/download', requireSession, api((req, res) => {
  const ex = getExport(Number(req.params.id));
  if (!ex) return res.status(404).json({ ok: false, error: 'not_found', message: '出力が見つかりません' });
  if (!fs.existsSync(ex.stored_path)) return res.status(410).json({ ok: false, error: 'file_gone', message: '出力ファイルが見つかりません (再出力してください)' });
  const name = String(ex.file_name || 'packlist.xlsx').replace(/[\r\n"]/g, '_');
  const ascii = name.replace(/[^\x20-\x7e]/g, '_');
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(name)}`);
  res.setHeader('Cache-Control', 'no-store');
  fs.createReadStream(ex.stored_path).on('error', (e) => { console.error('[fba-box] export download', e); if (!res.headersSent) res.status(500).end(); }).pipe(res);
}));

router.post('/admin/runs/:id(\\d+)/sta-uploaded', requireSession, checkOrigin, api((req, res) => {
  const r = markStaUploaded({ runId: Number(req.params.id), exportId: Number(req.body?.export_id), actor: `session:${req.session.email}` });
  if (!r.ok) return res.status({ not_found: 404, stale_export: 409, bad_status: 409 }[r.error] || 400).json(r);
  res.json(r);
}));

/** 資材 (管理者): 名前・自重・外寸。外寸は Excel の幅/長さ/高さ欄に自動で入る */
router.post('/admin/materials', checkOrigin, requireAdmin, api((req, res) => {
  const b = req.body || {};
  const r = upsertMaterial({ code: b.code, name: b.name, tareG: b.tare_g, widthCm: b.width_cm, lengthCm: b.length_cm, heightCm: b.height_cm, sort: b.sort, active: b.active !== false, actor: req.session.email });
  res.status(r.ok ? 200 : 400).json(r);
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
