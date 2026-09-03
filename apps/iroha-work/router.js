/**
 * いろは在庫化作業アプリ (iPad) — router
 *
 * URL: /apps/iroha-work/           … iPad 作業画面 (端末Cookie or ポータルセッション)
 *      /apps/iroha-work/api/*      … 作業 API (state / refresh / status)
 *      /apps/iroha-work/admin      … 管理画面 (ポータルセッション必須)
 *
 * アクセス制御は inbound-check と同じ2系統:
 *   ①ポータルセッション (allowedApps に iroha-work または '*')
 *   ②登録済み端末Cookie (iw_device) — 作業画面・作業APIのみ。作業者は名前タップで選択
 *   ⚠ server.js 側では requireAppAccess を掛けずに mount する (端末Cookieを通すため)。
 *      その分、ここで全ルートを守る (manifest.json だけ素通し)
 */
import { Router } from 'express';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  getDB,
  createDevice, verifyDevice, revokeDevice, listDevices,
  createEnrollCode, redeemEnrollCode, countEnrollAttempt, listActiveEnrollCodes, ENROLL_TTL_MS,
  checkEnrollRate, recordEnrollAttempt,
  listIrohaWorkers, getIrohaWorker, addIrohaWorker, setIrohaWorkerActive,
  workOptionsByKind, addWorkOption, setWorkOptionActive, setWorkOptionImage, seedWorkOptionsFromMaster,
  setWorkerPin, verifyWorkerPin,
  logEvent, listEvents,
  getCachePage, startSession, stopSession, listSessionsForAdmin, voidSession,
  getMeta, setMetaValue,
} from './db.js';
import { ensureFresh, changeStatus, fetchCardLive, cacheStatsForAdmin, STATUSES } from './notion-read.js';
import { surveyNotion, planImport, planToCsv, applyImport, reconcile, listMigrationFiles } from './migrate.js';
import { countTasksByStatus, listTasksNeedingReview, listOrphans } from './tasks-db.js';
import { OPEN_STATUSES } from './tasks.js';
import { buildList, buildTaskList, classifyMasterEdit, clearEnrichCache, masterOf, masterOfTask } from './service.js';
import { transitionNeedsStaff, TASK_STATUSES, statusLabel } from './tasks.js';
import {
  getTask, changeTaskStatus, setPlannedDate, clearMigrationReview, resolveCancellation,
  listLabelWaits, upsertLabelWait, listClosedTasks, taskErrorStatus, safeLogTaskEvent,
  startTaskSession, countChangesSince, switchSourceOfTruth,
} from './tasks-db.js';
import { updateWorkMasterRow, addWorkMasterRow, codeKeyOf } from '../inbound-check/work-master.js';
import {
  addMedia, softDeleteMedia, resetMedia, listMediaForAdmin, schedule as scheduleMedia, getMediaRow, driveDownload, markMediaUnavailable,
  recheckUnavailable, etagMatches, ifRangeMatches, singleRange,
  listPageSyncForAdmin, resetPageSync,
  isDriveConfigured, MEDIA_DIR, MAX_VIDEO_BYTES,
} from './media.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();
export const APP_ID = 'iroha-work';
const BASE = '/apps/iroha-work';
const DEVICE_COOKIE = 'iw_device';

// ─── 共通ヘルパ (inbound-check と同じ) ───
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

/** 変更系の CSRF 防御: Origin (無ければ Referer) のホスト一致。どちらも無ければ拒否 */
function checkOrigin(req, res, next) {
  const reject = () => res.status(403).json({ ok: false, error: 'bad_origin', message: '不正なオリジンからのリクエストです' });
  const src = req.headers.origin || req.headers.referer;
  if (!src || src === 'null') return reject();
  let host = null;
  try { host = new URL(src).host; } catch { /* 不正値は下で403 */ }
  if (!host || host !== req.headers.host) return reject();
  next();
}

/** 全ルート共通の入口: セッション or 登録端末 */
function access(req, res, next) {
  if (req.path === '/manifest.json' || req.path === '/sw.js') return next();   // 静的 (中身に秘密なし)
  if (req.path === '/enroll' || req.path === '/enroll/redeem') return next();
  if (hasSessionAccess(req)) { req.iwUser = req.session.email; return next(); }
  const device = verifyDevice(readCookie(req, DEVICE_COOKIE));
  if (device) { req.iwDevice = device; return next(); }
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

/** タスク ID (正の整数) の取り出し。形式が違えば null → 呼び元は 400 (Number() 任せの NaN/小数/負数を DB 依存の 404 にしない — Codex A1b R1 Low) */
function parseTaskId(v) {
  const s = String(v ?? '').trim();
  if (!/^[1-9]\d{0,15}$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;   // 16 桁は 2^53 を超え得る (丸めた id で別の行を引かない — Codex A1b R2)
}
const BAD_TASK_ID = { ok: false, error: 'bad_request', message: 'カードの指定が不正です (一覧を更新してください)' };

const api = fn => async (req, res) => {
  try { await fn(req, res); } catch (e) {
    console.error(`[iroha-work] ${req.method} ${req.path}`, e);
    res.status(500).json({ ok: false, error: 'internal', message: e.message });
  }
};

// 選択肢 (資材・保管箱) は作業仕様マスタの値から補充してから返す (Excel 再取込後も候補に出る)。
// seed 自体がマスタの変化を見て (件数+最終更新) 変わった時だけ走る。失敗しても候補は前回のまま返し、次回また試す
function workOptionsForState(includeInactive = false) {
  try { seedWorkOptionsFromMaster(); } catch (e) { console.warn('[iroha-work] 選択肢の補充に失敗 (候補は前回のまま)', e.message); }
  return workOptionsByKind(includeInactive);
}

router.use(access);

// ─── Service Worker (Render 再起動中でも画面が真っ白にならないための、画面 HTML のフォールバック) ───
// 認証の外だが中身は静的。no-cache で更新をすぐ拾わせる
router.get('/sw.js', (req, res) => {
  res.set('Cache-Control', 'no-cache');
  res.type('application/javascript');
  res.sendFile(path.join(__dirname, 'views/sw.js'), { cacheControl: false });
});

// ─── PWA manifest ───
router.get('/manifest.json', (req, res) => {
  res.json({
    name: 'いろは在庫化', short_name: 'いろは在庫化', start_url: `${BASE}/`, scope: `${BASE}/`,
    display: 'standalone', orientation: 'any', background_color: '#F3F5F9', theme_color: '#1F5EFF',
    icons: [
      { src: '/app-icons/iroha-work-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/app-icons/iroha-work-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
    ],
  });
});

// ─── 端末登録 (inbound-check と同じ: 6桁コード・10分・1回) ───
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

// この端末の登録解除 (サーバー側トークンも失効させる)
router.post('/device/exit', checkOrigin, api((req, res) => {
  if (req.iwDevice) revokeDevice(req.iwDevice.id);
  res.clearCookie(DEVICE_COOKIE, { path: BASE });
  res.json({ ok: true, revoked: !!req.iwDevice });
}));

// ─── 作業画面 ───
router.get('/', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ─── 作業 API ───

/**
 * 正本 (要件定義 v1.1 §F の切替)。'notion' = 従来どおり Notion のカードを読み書きする /
 * 'app' = f_iroha_tasks が正本 (Notion は見ない)。管理画面のスイッチで切り替える
 */
export function sourceOfTruth() { return getMeta('source_of_truth') === 'app' ? 'app' : 'notion'; }
const isAppMode = () => sourceOfTruth() === 'app';

/**
 * 一覧。Notion 正本ならキャッシュが古いとき取り直してから返す (失敗しても古いキャッシュで返す —
 * 現場を止めない)。アプリ正本なら自分の DB を読むだけ
 */
router.get('/api/state', api(async (req, res) => {
  const appMode = isAppMode();
  const refresh = appMode
    ? { fresh: true, lastRefreshAt: new Date().toISOString(), error: null, truncated: false }
    : await ensureFresh();
  const list = appMode ? buildTaskList() : buildList();
  res.json({
    ok: true,
    ...list,
    workers: listIrohaWorkers(),
    options: workOptionsForState(),
    refresh,
    // タイマー表示の基準 (iPad の時計を信じない — 画面はこの値とのオフセットで経過を出す)
    serverNow: new Date().toISOString(),
    me: { session: req.iwUser || null, device: req.iwDevice ? { id: req.iwDevice.id, label: req.iwDevice.label } : null, admin: isAdmin(req) },
  });
}));

// 手動の「いま更新」。連打で Notion を叩かないよう 15 秒のクールダウン
let refreshLastAt = 0;
router.post('/api/refresh', checkOrigin, api(async (req, res) => {
  if (isAppMode()) return res.json({ ok: true, refresh: { fresh: true, lastRefreshAt: new Date().toISOString(), error: null, truncated: false } });
  const now = Date.now();
  if (now - refreshLastAt < 15_000) {
    const wait = Math.ceil((15_000 - (now - refreshLastAt)) / 1000);
    return res.status(429).json({ ok: false, error: 'rate_limited', message: `さっき更新したばかりです。${wait}秒あけてもう一度押せます` });
  }
  refreshLastAt = now;
  const refresh = await ensureFresh({ force: true });
  // truncated (件数上限で取得を諦めた) も「最新にできていない」ので成功にしない (Codex PR1-R2 #3)
  const okRes = !refresh.error && !refresh.truncated;
  res.status(refresh.error ? 502 : 200).json({
    ok: okRes, refresh,
    ...(okRes ? {} : { message: refresh.error || 'カードが件数上限を超えているため更新できませんでした (前回取得分を表示しています)' }),
  });
}));

/** 作業者の解決 (いろは名簿)。変更操作は誰がやったかを必須にする */
function resolveWorker(req) {
  const w = getIrohaWorker(req.body?.worker_id);
  if (!w) return { error: '作業者を選んでください' };
  if (!w.active) return { error: 'この作業者は無効になっています (職員の方に確認してください)' };
  return { worker: w };
}

const STATUS_HTTP = {
  conflict: 409, card_gone: 404, staff_required: 403, bad_status: 400, bad_request: 400,
  notion_error: 502, verify_failed: 502, schema_mismatch: 502,
  pin_required: 403, pin_invalid: 403, pin_locked: 429,
};

/**
 * ステータス変更 (一覧のステータス札 → ダイアログから)。
 * 変更直前の再取得・競合検出・反映確認は notion-read.changeStatus が行う。
 *
 * 職員限定の変更 (棚入完了への変更・取り消し) の本人確認 (Codex PR1 #1):
 *   worker_id は画面で自由に選べる自己申告なので、それだけで職員扱いしない。
 *   ①ポータルセッション = ログイン済みの B-Faith 側 → そのまま職員扱い
 *   ②端末Cookie = 職員の worker_id + その職員の PIN が合ったときだけ職員扱い
 *   リクエスト時点で職員操作と分からなくても (expect が古い等)、changeStatus が実状態で
 *   ゲートを判定する — PIN 未確認なら isStaff=false のままなので素通りしない。
 * 成否とも操作履歴 (f_iroha_app_events) に残す (Codex R2「操作履歴」)
 */
router.post('/api/status', checkOrigin, api(async (req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  if (isAppMode()) return changeStatusApp(req, res, w.worker);
  const pageId = String(req.body?.page_id || req.body?.id || '').trim();
  const to = String(req.body?.to || '').trim();
  const expect = req.body?.expect == null ? null : String(req.body.expect);
  if (!pageId) return res.status(400).json({ ok: false, error: 'bad_request', message: 'page_id が必要です' });

  // 職員としての本人確認。PIN はリクエストが職員限定操作 (棚入完了が絡む) のときだけ要求する
  let isStaff = false;
  if (hasSessionAccess(req)) {
    isStaff = true;
  } else if (w.worker.worker_type === 'staff') {
    const gatedReq = to === '棚入完了' || expect === '棚入完了';
    if (gatedReq) {
      const pinCheck = verifyWorkerPin(w.worker.id, req.body?.pin);
      if (!pinCheck.ok) {
        return res.status(STATUS_HTTP[pinCheck.error] || 403).json({ ok: false, ...pinCheck });
      }
      isStaff = true;
    }
  }

  // 写真は「次回の見本」であって完了の証拠ではない → 棚入完了に写真は要らない
  // (中原さん 2026-09-03。旧「写真1枚以上」ゲートと skip_photo は撤去)
  const r = await changeStatus({ pageId, to, expect, isStaff });
  // 監査ログの失敗で Notion 更新済みの結果を「失敗」に見せない (Codex PR1 #6)
  try {
    logEvent({
      action: 'status_change', pageId,
      workerId: w.worker.id, workerName: w.worker.display_name,
      deviceLabel: req.iwDevice ? req.iwDevice.label : (req.iwUser ? `session:${req.iwUser}` : null),
      from: expect, to,
      ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}`,
    });
  } catch (e) {
    console.error('[iroha-work] 操作履歴の記録に失敗 (結果はそのまま返す)', e);
  }
  if (!r.ok) return res.status(STATUS_HTTP[r.error] || 400).json(r);
  res.json(r);
}));

/**
 * 状態変更 (アプリ正本)。許可遷移・職員限定・理由の必須・version 楽観ロックは tasks-db.changeTaskStatus が守る。
 * 職員の本人確認は Notion 正本と同じ考え方 (セッション = 職員 / 端末は職員 worker + PIN)。
 * 「その遷移が職員限定か」は現在の状態から決まるので、PIN を求めるのも必要なときだけ
 */
function changeStatusApp(req, res, worker) {
  const taskId = parseTaskId(req.body?.id ?? req.body?.task_id);
  if (taskId == null) return res.status(400).json(BAD_TASK_ID);
  const to = String(req.body?.to || '').trim();
  const t = getTask(taskId);
  if (!t) return res.status(404).json({ ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' });
  if (!TASK_STATUSES.includes(to)) return res.status(400).json({ ok: false, error: 'bad_status', message: '変更先の状態が不正です' });
  let isStaff = false;
  if (hasSessionAccess(req)) {
    isStaff = true;
  } else if (worker.worker_type === 'staff' && transitionNeedsStaff(t.status, to)) {
    const pinCheck = verifyWorkerPin(worker.id, req.body?.pin);
    if (!pinCheck.ok) return res.status(STATUS_HTTP[pinCheck.error] || 403).json({ ok: false, ...pinCheck });
    isStaff = true;
  }
  const r = changeTaskStatus({
    taskId: t.id, to, expectVersion: req.body?.expect_version,
    closeReason: req.body?.close_reason || null,
    holdReason: req.body?.hold_reason || null,
    holdNote: req.body?.hold_note || null,
    reason: req.body?.reason || null,
    actor: hasSessionAccess(req) ? req.iwUser : `${worker.display_name} (いろはアプリ)`,
    isStaff, workerId: worker.id, workerName: worker.display_name, deviceLabel: deviceLabelOf(req),
  });
  if (!r.ok) {
    return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  }
  res.json({ ok: true, already: !!r.already, task: publicTask(r.task), status: r.task.status, status_label: statusLabel(r.task), listed: r.task.status !== 'closed' });
}

/** 画面へ返すタスク (内部列は出さない)。一覧の再取得を待たずに 1 枚だけ差し替えるため */
function publicTask(t) {
  return {
    id: t.id, status: t.status, status_label: statusLabel(t), version: t.version,
    facility_code: t.facility_code, hold_reason_code: t.hold_reason_code, hold_reason_note: t.hold_reason_note,
    planned_date: t.planned_date, cancellation_requested_at: t.cancellation_requested_at, migration_review: !!t.migration_review,
  };
}

/** 「今日やる / 後日」(アプリ正本のみ) */
router.post('/api/planned', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const plannedTaskId = parseTaskId(req.body?.id);
  if (plannedTaskId == null) return res.status(400).json(BAD_TASK_ID);
  const r = setPlannedDate({
    taskId: plannedTaskId, plannedDate: req.body?.planned_date ?? null, expectVersion: req.body?.expect_version,
    actor: `${w.worker.display_name} (いろはアプリ)`, workerId: w.worker.id, workerName: w.worker.display_name, deviceLabel: deviceLabelOf(req),
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  res.json({ ok: true, task: publicTask(r.task) });
}));

/** 取消の要確認を職員が判断する (cancel / continue)。アプリ正本のみ */
router.post('/api/cancellation', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  let isStaff = hasSessionAccess(req);
  if (!isStaff) {
    if (w.worker.worker_type !== 'staff') return res.status(403).json({ ok: false, error: 'staff_required', message: '取消の判断は職員のみです' });
    const pinCheck = verifyWorkerPin(w.worker.id, req.body?.pin);
    if (!pinCheck.ok) return res.status(STATUS_HTTP[pinCheck.error] || 403).json({ ok: false, ...pinCheck });
    isStaff = true;
  }
  const cancelTaskId = parseTaskId(req.body?.id);
  if (cancelTaskId == null) return res.status(400).json(BAD_TASK_ID);
  const r = resolveCancellation({
    taskId: cancelTaskId, decision: String(req.body?.decision || ''), expectVersion: req.body?.expect_version,
    actor: isStaff && hasSessionAccess(req) ? req.iwUser : `${w.worker.display_name} (いろはアプリ)`,
    isStaff, workerId: w.worker.id, workerName: w.worker.display_name, deviceLabel: deviceLabelOf(req),
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  res.json({ ok: true, task: publicTask(r.task) });
}));

/** 取込時に推定した状態を職員が「確認した」にする (アプリ正本のみ) */
router.post('/api/review-cleared', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  if (!hasSessionAccess(req)) {
    if (w.worker.worker_type !== 'staff') return res.status(403).json({ ok: false, error: 'staff_required', message: 'この操作は職員のみです' });
    const pinCheck = verifyWorkerPin(w.worker.id, req.body?.pin);
    if (!pinCheck.ok) return res.status(STATUS_HTTP[pinCheck.error] || 403).json({ ok: false, ...pinCheck });
  }
  const reviewTaskId = parseTaskId(req.body?.id);
  if (reviewTaskId == null) return res.status(400).json(BAD_TASK_ID);
  const r = clearMigrationReview({ taskId: reviewTaskId, expectVersion: req.body?.expect_version, actor: `${w.worker.display_name} (いろはアプリ)` });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  res.json({ ok: true, task: publicTask(r.task) });
}));

/** ラベル待ち (一覧・登録・更新)。アプリ正本のみ */
router.get('/api/label-waits', api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const taskId = req.query.task_id == null ? null : parseTaskId(req.query.task_id);
  if (req.query.task_id != null && taskId == null) return res.status(400).json(BAD_TASK_ID);
  res.json({ ok: true, rows: listLabelWaits({ taskId, openOnly: req.query.all !== '1' }) });
}));

router.post('/api/label-waits', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const fields = { ...(req.body?.fields || {}) };
  if (req.body?.id == null) {   // 新規は記録者を自動で入れる (誰が気づいたか)
    if (fields.recorded_by_worker_id == null) fields.recorded_by_worker_id = w.worker.id;
    if (!fields.recorded_by_name) fields.recorded_by_name = w.worker.display_name;
  }
  const labelTaskId = parseTaskId(req.body?.task_id);
  if (labelTaskId == null) return res.status(400).json(BAD_TASK_ID);
  const r = upsertLabelWait({
    id: req.body?.id ?? null, taskId: labelTaskId, fields,
    expectVersion: req.body?.expect_version ?? null, actor: `${w.worker.display_name} (いろはアプリ)`,
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json(r);
  res.json(r);
}));

// ─── 作業仕様のその場登録・修正 (f_iroha_work_master。中原さんFB③⑥) ───

const MASTER_FIELDS = ['material_code', 'storage_container', 'units_per_container', 'process_count', 'note', 'video_url'];

/**
 * 権限 (要件 §7 と FB③の折衷):
 *   空欄を埋める = 作業者なら誰でも (新商品で現場が止まらない。履歴に残る)
 *   入っている値の変更・削除 = 職員のみ (端末はPIN・ポータルセッションはそのまま)
 * 版管理 (§1.7 ④): expect_version の楽観ロック。行が無ければ作ってから書く (mirror に居る商品のみ)
 */
/**
 * 選択肢 (資材・保管箱) のその場登録 — 初見のものを適当に入れず、次からタップで選べるようにする
 * (中原さん 2026-09-03)。候補の品質を守るため職員PIN必須 (ポータルセッションなら不要)
 */
router.post('/api/options', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  if (!hasSessionAccess(req)) {
    if (w.worker.worker_type !== 'staff') {
      return res.status(403).json({ ok: false, error: 'staff_required', message: '新しい候補の登録は職員のみです (職員の名前を選び、PINを入れてください)' });
    }
    const pinCheck = verifyWorkerPin(w.worker.id, req.body?.pin);
    if (!pinCheck.ok) return res.status(STATUS_HTTP[pinCheck.error] || 403).json({ ok: false, ...pinCheck });
  }
  // 記録上の主体: ポータルセッションなら本人 (worker_id は画面で選んだ名前に過ぎない — Codex R1 #4)。
  // 無効化された候補の復帰は管理者だけ (allowReactivate=false — Codex R1 #1)
  const actor = hasSessionAccess(req) ? `${req.iwUser} (ポータル)` : `${w.worker.display_name} (いろはアプリ)`;
  const r = addWorkOption({ kind: req.body?.kind, code: req.body?.code, actor, allowReactivate: false });
  if (!r.ok) return res.status(r.error === 'inactive_option' ? 409 : 400).json(r);
  safeLog({ action: 'option_add', pageId: null, workerId: w.worker.id, workerName: w.worker.display_name,
    deviceLabel: deviceLabelOf(req), to: `${req.body?.kind}:${r.option.code}${r.already ? ' (既存)' : ''}`, ok: true });
  res.json(r);
}));

router.post('/api/master', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const code = String(req.body?.code || '').trim();
  if (!code) return res.status(400).json({ ok: false, error: 'bad_request', message: 'code (商品コード) が必要です' });
  const fieldsIn = req.body?.fields;
  if (!fieldsIn || typeof fieldsIn !== 'object' || Array.isArray(fieldsIn)) {
    return res.status(400).json({ ok: false, error: 'bad_request', message: '変更内容がありません' });
  }
  const fields = {};
  for (const f of MASTER_FIELDS) if (f in fieldsIn) fields[f] = fieldsIn[f];
  if (Object.keys(fields).length === 0) return res.status(400).json({ ok: false, error: 'bad_request', message: '変更できる項目がありません' });

  const k = codeKeyOf(code);
  const db = getDB();
  const row = db.prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?').get(k);

  // シード・権限判定に使うカード値 (商品コードが一致するカードだけ)。
  // アプリ正本ならタスクの作成時スナップショット、Notion 正本ならカードのプロパティ
  const cardId = String(req.body?.id || req.body?.page_id || '');
  let cardValues = {};
  if (isAppMode()) {
    const t = parseTaskId(cardId) == null ? null : getTask(parseTaskId(cardId));
    if (t && codeKeyOf(t.product_code) === k) {
      try { cardValues = t.master_snapshot ? JSON.parse(t.master_snapshot) : {}; } catch { /* 壊れていればカード値なし */ }
    }
  } else {
    const card = getCachePage(cardId);
    if (card && codeKeyOf(card.product_code) === k) {
      let props = {};
      try { props = JSON.parse(card.payload || '{}'); } catch { /* 同上 */ }
      cardValues = {
        material_code: props['資材セットID'] || null, storage_container: props['収納容器'] || null,
        units_per_container: props['入数'] ?? null, process_count: props['工程数'] ?? null, note: props['備考'] || null,
      };
    }
  }

  // DBが実際に変わるか (書き込み要否・unchanged判定) は**生値**で見る
  const { fills, overwrites } = classifyMasterEdit(row, fields);
  if (fills.length === 0 && overwrites.length === 0) {
    return res.json({ ok: true, unchanged: true, row });
  }
  // 権限は**画面に見えていた実効値** (マスタ+カードのフォールバック合成) で見る (Codex PR4-R3:
  // マスタが空欄でもカード値 D-8 が表示されている項目を、一般作業者が D-9 へ変えられてはいけない。
  // 表示どおりの値を確定保存するだけなら誰でもよい)
  const effRow = masterOfTask(row || null, cardValues);
  const perm = classifyMasterEdit(effRow, fields);
  if (perm.overwrites.length > 0 && !hasSessionAccess(req)) {
    if (w.worker.worker_type !== 'staff') {
      return res.status(403).json({ ok: false, error: 'staff_required',
        message: '表示されている値の変更は職員のみです (空欄への登録は誰でもできます)' });
    }
    const pinCheck = verifyWorkerPin(w.worker.id, req.body?.pin);
    if (!pinCheck.ok) return res.status(STATUS_HTTP[pinCheck.error] || 403).json({ ok: false, ...pinCheck });
  }
  // 既存行の更新は expect_version 必須 (省略でロックを素通りさせない — Codex PR4 #5)
  if (row && req.body?.expect_version == null) {
    return res.status(400).json({ ok: false, error: 'version_required', message: '画面を更新してからやり直してください (version が必要です)' });
  }

  // ⭐行の新規作成〜更新は1トランザクション (Codex PR4 #2: 検証や競合で失敗したのに
  //   空行だけ残ると、カード由来の表示仕様を隠してしまう)。
  //   新規作成時は、カード作成時のスナップショット値で行を初期化してから今回の変更を
  //   重ねる (Codex PR4 #1: 動画だけ登録した瞬間に資材・入数の表示が消えないように)
  const editor = `${w.worker.display_name} (いろはアプリ)`;
  const rollback = Symbol('rollback');
  let result;
  try {
    result = db.transaction(() => {
      let cur = row;
      let applyFields = fields;
      let expect;
      if (!cur) {
        const add = addWorkMasterRow(code, editor);
        if (!add.ok) {
          const msg = add.error === 'not_in_master' ? 'この商品は商品マスタに無いため登録できません (商品コードを確認してください)' : add.message;
          const e = new Error(msg); e[rollback] = { status: 400, body: { ok: false, error: add.error, message: msg } }; throw e;
        }
        cur = db.prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?').get(k);
        expect = cur.version;
        // カード表示中の値をシード (今回指定されなかった項目だけ)。空欄埋め扱いなので権限は不要。
        // cardValues は上で商品コード一致を確認済み (別商品のカード値を混ぜない)
        const seed = {
          material_code: cardValues.material_code, storage_container: cardValues.storage_container,
          units_per_container: cardValues.units_per_container, process_count: cardValues.process_count, note: cardValues.note,
        };
        applyFields = { ...Object.fromEntries(Object.entries(seed).filter(([f, v]) => v != null && v !== '' && !(f in fields))), ...fields };
      } else {
        expect = Number(req.body.expect_version);
      }
      const r = updateWorkMasterRow(k, applyFields, editor, expect);
      if (!r.ok) {
        const st = r.error === 'conflict' ? 409 : r.error === 'not_found' ? 404 : 400;
        const msg = r.error === 'conflict' ? '他の人が先に更新しました。最新の内容を読み込みます' : r.message;
        const e = new Error(msg); e[rollback] = { status: st, body: { ok: false, error: r.error, message: msg, currentVersion: r.currentVersion } }; throw e;
      }
      return { ...r, applyFields };
    }).immediate();
  } catch (e) {
    if (e[rollback]) return res.status(e[rollback].status).json(e[rollback].body);
    throw e;
  }

  // 履歴は旧値→新値をJSONで残す (項目名だけだと復元できない — Codex PR4 #6)。
  // シードで書いた項目も含め、実際に適用した applyFields を対象に。切り詰めない
  // (入力は各フィールド上限500字までなのでJSON全体でも高々数KB — 途中切断で壊れたJSONを残さない)
  const oldVals = {}; const newVals = {};
  for (const f of Object.keys(result.applyFields)) { oldVals[f] = row ? row[f] : null; newVals[f] = result.row[f]; }
  safeLog({
    action: 'master_edit', pageId: String(req.body?.page_id || '') || null,
    workerId: w.worker.id, workerName: w.worker.display_name, deviceLabel: deviceLabelOf(req),
    from: JSON.stringify({ code, ...oldVals }),
    to: JSON.stringify({ v: result.row.version, ...newVals }),
    ok: true,
  });
  clearEnrichCache();   // 次の /api/state から新しい作業仕様で出す
  res.json({ ok: true, row: result.row });
}));

// ─── 完成写真・動画 ───

const MEDIA_TMP = path.join(MEDIA_DIR, 'tmp');
try { fs.mkdirSync(MEDIA_TMP, { recursive: true }); } catch { /* 受信時にも作る */ }
const mediaUpload = multer({ dest: MEDIA_TMP, limits: { fileSize: MAX_VIDEO_BYTES } });

/**
 * 撮影した写真・動画の受信。実体を outbox (DATA_DIR) に置いて**即応答** — Drive/Notion へは
 * 裏のキューが送る (§1.7 ②)。operation_id で再送を冪等化。
 */
router.post('/api/media', checkOrigin, mediaUpload.single('file'), api((req, res) => {
  const cleanup = () => { if (req.file) { try { fs.unlinkSync(req.file.path); } catch { /* 移動済みなら無い */ } } };
  try {
    const w = resolveWorker(req);
    if (w.error) { cleanup(); return res.status(400).json({ ok: false, error: 'worker_required', message: w.error }); }
    if (!req.file) return res.status(400).json({ ok: false, error: 'no_file', message: 'ファイルがありません' });
    if (!isDriveConfigured()) { cleanup(); return res.status(503).json({ ok: false, error: 'not_configured', message: 'ドライブ保存が未設定です (職員の方は管理者に連絡してください)' }); }
    const cardId = String(req.body?.id || req.body?.page_id || '').trim();
    const kind = String(req.body?.kind || '');
    if (!cardId || (kind !== 'photo' && kind !== 'video')) {
      cleanup(); return res.status(400).json({ ok: false, error: 'bad_request', message: 'カードと種類 (photo/video) が必要です' });
    }
    const appMode = isAppMode();
    // 正本に合わせて、カードを task_id (アプリ正本) か page_id (Notion 正本) で引く
    if (appMode && parseTaskId(cardId) == null) { cleanup(); return res.status(400).json(BAD_TASK_ID); }
    const task = appMode ? getTask(parseTaskId(cardId)) : null;
    const card = appMode ? null : getCachePage(cardId);
    if (appMode ? !task : !card) { cleanup(); return res.status(404).json({ ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' }); }
    const productCode = appMode ? task.product_code : card.product_code;
    const r = addMedia({
      pageId: appMode ? null : cardId, taskId: appMode ? task.id : null,
      productCode, kind, mime: req.file.mimetype,
      filePath: req.file.path, worker: w.worker, deviceLabel: deviceLabelOf(req),
      deviceId: req.iwDevice ? req.iwDevice.id : null,
      operationId: req.body?.operation_id,
    });
    if (!r.ok) { cleanup(); return res.status(r.error === 'cap_reached' || r.error === 'operation_conflict' ? 409 : 400).json(r); }
    if (appMode) {
      safeLogTaskEvent({ taskId: task.id, action: `media_${kind}`, workerId: w.worker.id, workerName: w.worker.display_name,
        deviceLabel: deviceLabelOf(req), to: r.already ? 'resend' : 'add', ok: true });
    } else {
      safeLog({ action: `media_${kind}`, pageId: cardId, workerId: w.worker.id, workerName: w.worker.display_name,
        deviceLabel: deviceLabelOf(req), to: r.already ? 'resend' : 'add', ok: true });
    }
    scheduleMedia();
    res.json(r);
  } catch (e) {
    cleanup();
    throw e;
  }
}));

/**
 * 写真・動画の配信。iPad はサービスアカウントの Drive を直接見られない (フォルダは公開しない —
 * 要件 §6) ので、ここを通して出す。Drive 保存済みなら Drive からストリーム (単一 Range を転送 =
 * 動画のシーク・ETag で 304)、送信待ち (stored) なら MEDIA_DIR 配下のローカル実体。
 * 認可 = 端末認証 (access)。⭐仕様: 登録済み端末 (いろはの共用 iPad) には、削除済み以外の全写真を
 * 見せる — 「前回の完成形」は過去カードの写真をそのまま見本にする機能で、写真は商品の完成形
 * (個人情報ではない・ファイル名に人名も入れない)。カードや写真を個別に隠す要件が出たらここで絞る
 * (Codex R1 #3)。キャッシュは private + no-cache (ETag で再検証。削除・端末失効が翌日まで残らない)
 */
router.get('/api/media/:id(\\d+)/file', api(async (req, res) => {
  const r = getMediaRow(Number(req.params.id));
  const fail = (status, error, message) => { res.set('Cache-Control', 'no-store'); return res.status(status).json({ ok: false, error, message }); };
  if (!r || r.deleted_at) return fail(404, 'not_found', '写真・動画が見つかりません');
  if (r.unavailable_at) return fail(404, 'unavailable', 'この写真はドライブから消えています');
  const mime = r.mime || (r.kind === 'photo' ? 'image/jpeg' : 'video/mp4');
  // ①送信待ち: ローカル実体。DB の local_path を無条件に信じず MEDIA_DIR 配下だけ送る (Codex R1 #8)
  if (r.local_path) {
    const abs = path.resolve(r.local_path);
    const rel = path.relative(path.resolve(MEDIA_DIR), abs);
    const inside = rel && !rel.startsWith('..') && !path.isAbsolute(rel);
    if (inside && fs.existsSync(abs)) {
      res.set('Cache-Control', 'private, no-cache');
      return res.sendFile(abs, { headers: { 'Content-Type': mime } });
    }
  }
  if (!r.drive_file_id) return fail(409, 'not_ready', 'まだ保存中です。少し待ってから開いてください');
  // ②Drive 保存済み: ETag = file_id。変わらなければ Drive を叩かず 304
  const etag = `"${r.drive_file_id}"`;
  if (etagMatches(req.headers['if-none-match'], etag)) {
    res.set('ETag', etag); res.set('Cache-Control', 'private, no-cache');
    return res.status(304).end();
  }
  let range = singleRange(req.headers.range);
  // If-Range は強い ETag の単一一致だけ (W/・*・日付は不一致 → Range を無視して全体 — Codex R2 #2)
  if (range && req.headers['if-range'] && !ifRangeMatches(req.headers['if-range'], etag)) range = null;
  let dl;
  try {
    dl = await driveDownload({ fileId: r.drive_file_id, range });
  } catch (e) {
    const st = Number(e?.response?.status || e?.status || e?.code) || 0;
    res.set('Cache-Control', 'no-store');
    if (st === 416) { res.set('Content-Range', `bytes */${r.size || '*'}`); return res.status(416).end(); }
    if (st === 404 || st === 410) {
      // Drive 側で消えた → 見本候補・表示から外す (毎回壊れた写真を選び続けない — Codex R1 #5)
      markMediaUnavailable(r.id, `Drive ${st}`);
      return fail(404, 'unavailable', 'この写真はドライブから消えています');
    }
    return fail(502, 'drive_error', `ドライブから取り出せませんでした (${e.message})`);
  }
  res.status(dl.status || 200);
  res.set('ETag', etag);
  res.set('Accept-Ranges', 'bytes');
  res.set('Cache-Control', 'private, no-cache');
  res.type(mime);
  if (dl.contentLength) res.set('Content-Length', String(dl.contentLength));
  if (dl.contentRange) res.set('Content-Range', dl.contentRange);
  // クライアントが切ったら上流 (Drive) も止める — 動画のシーク連打で不要なダウンロードを残さない (Codex R1 #7)
  res.on('close', () => { if (!res.writableFinished) dl.stream.destroy(); });
  pipeline(dl.stream, res, (e) => {
    if (!e) return;
    if (e.code !== 'ERR_STREAM_PREMATURE_CLOSE') console.error(`[iroha-work] media #${r.id} の配信エラー`, e.message);
    if (!res.headersSent) { try { res.status(502).end(); } catch { /* 切れていれば何もしない */ } }
  });
}));

// 撮り直し用の削除 (論理削除)。本人確認 = アップロード時に返した削除トークン
// (worker_id は自己申告なので使わない — Codex PR3 #2)。職員はPCの管理画面 (セッション) から
router.post('/api/media/:id(\\d+)/delete', checkOrigin, api((req, res) => {
  const r = softDeleteMedia(Number(req.params.id), {
    deleteToken: req.body?.delete_token || null,
    actor: req.iwUser || null,
    isSession: hasSessionAccess(req),
  });
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 403)).json(r);
}));

// 失敗した送信の再実行 (管理画面)。「ドライブから消えた」印の行は Drive で実在を確かめてから解除
// (未検証のまま表示・見本候補へ戻さない — Codex R2 #3)
router.post('/admin/media/:id(\\d+)/retry', checkOrigin, requireAdmin, api(async (req, res) => {
  const id = Number(req.params.id);
  const row = getMediaRow(id);
  if (!row || row.deleted_at) return res.status(404).json({ ok: false, error: 'not_found', message: '対象が見つかりません' });
  if (row.unavailable_at) {
    const r = await recheckUnavailable(id);
    return res.status(r.ok ? 200 : (r.error === 'drive_error' ? 502 : 409)).json(r);
  }
  if (!resetMedia(id)) return res.status(404).json({ ok: false, error: 'not_found', message: '対象が見つかりません' });
  res.json({ ok: true });
}));

// Notion 貼り直しの再実行 (管理画面)
router.post('/admin/media-sync/retry', checkOrigin, requireAdmin, api((req, res) => {
  if (!resetPageSync(String(req.body?.page_id || ''))) return res.status(404).json({ ok: false, error: 'not_found', message: '対象が見つかりません' });
  res.json({ ok: true });
}));

// ─── 作業時間セッション (開始 / 中断・終了) ───

function safeLog(entry) {
  try { logEvent(entry); } catch (e) { console.error('[iroha-work] 操作履歴の記録に失敗 (結果はそのまま返す)', e); }
}

const deviceLabelOf = (req) => (req.iwDevice ? req.iwDevice.label : (req.iwUser ? `session:${req.iwUser}` : null));

/**
 * 作業開始。sessions は自社DBが正本 (v1からためる — 後の正本化にそのまま繋がる)。
 * ⑤最初の開始で Notion を「未着手→作業中」へ (best-effort。Notion が失敗しても開始は成立
 *   — Notion API 成功を現場操作成功の条件にしない)
 */
router.post('/api/sessions/start', checkOrigin, api(async (req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  if (isAppMode()) return startSessionApp(req, res, w.worker);
  const pageId = String(req.body?.page_id || req.body?.id || '').trim();
  if (!pageId) return res.status(400).json({ ok: false, error: 'bad_request', message: 'page_id が必要です' });
  // 開始可否は**必ず**実ページを直接再取得して判定する (Codex PR2-R3: キャッシュが3分以内でも、
  // その間に棚入完了・取消・アーカイブ・別DB移動があり得る)。取得失敗なら開始を拒否 —
  // どのみち Notion に書けない状況なので、素性の分からないカードで時間だけ記録しない
  const live = await fetchCardLive(pageId);
  if (!live.ok) {
    const st = (live.error === 'card_gone' || live.error === 'wrong_database') ? 404 : 502;
    return res.status(st).json({ ok: false, error: live.error, message: live.message });
  }
  const card = getCachePage(pageId);   // fetchCardLive が最新状態を upsert 済み
  if (card.status === '棚入完了') return res.status(409).json({ ok: false, error: 'done_card', message: 'このカードは棚入完了です (作業をはじめるなら職員がステータスを戻してください)' });
  if (card.status === '取消') return res.status(409).json({ ok: false, error: 'cancelled_card', message: 'このカードは取消済みです' });

  // スナップショット (§1.7 ④) は「画面に見えていた実効値」= マスタ+カードのフォールバック合成
  let snapshot = null;
  if (card.product_code) {
    let props = {};
    try { props = JSON.parse(card.payload || '{}'); } catch { /* 壊れていれば props なしで合成 */ }
    const wm = getDB().prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?')
      .get(String(card.product_code).trim().toLowerCase());
    snapshot = masterOf(wm || null, props);
  }
  const r = startSession({
    pageId, productCode: card.product_code, title: card.title,
    worker: w.worker, deviceLabel: deviceLabelOf(req), masterSnapshot: snapshot,
  });
  if (!r.already) {
    safeLog({ action: 'session_start', pageId, workerId: w.worker.id, workerName: w.worker.display_name,
      deviceLabel: deviceLabelOf(req), to: 'start', ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
  }
  if (!r.ok) return res.status(r.error === 'bad_request' ? 400 : 409).json(r);

  // ⑤最初の開始で 未着手→作業中 (best-effort)。Notion が遅いときに現場の応答を道連れに
  // しないよう 8 秒で見切る — 変更処理自体は裏で続き、キャッシュへは完了時に反映される
  let statusNow = card.status;
  if (card.status === '未着手') {
    const change = changeStatus({ pageId, to: '作業中', expect: '未着手', isStaff: false })
      .catch((e) => ({ ok: false, error: 'notion_error', message: e.message }));
    const cs = await Promise.race([change, new Promise((resolve) => setTimeout(() => resolve(null), 8000))]);
    if (cs?.ok) statusNow = cs.status;
    else if (cs?.error === 'conflict' && cs.current) statusNow = cs.current;
    // タイムアウト/失敗は開始を成立させたまま、次の巡回・手動更新に任せる
  }
  res.json({ ok: true, already: !!r.already, sessionId: r.sessionId, startedAt: r.startedAt, status: statusNow, serverNow: new Date().toISOString() });
}));

/**
 * 作業開始 (アプリ正本)。Notion を見ないので、判定は自分の DB のタスクだけ。
 * 終了確認・セッション INSERT・未着手→作業中 は tasks-db.startTaskSession が 1 トランザクションで行う
 * (別端末の終了と競合しても「終了したカードに活動中セッション」を残さない — Codex A1b R1 #2)
 */
function startSessionApp(req, res, worker) {
  const taskId = parseTaskId(req.body?.id ?? req.body?.task_id);
  if (taskId == null) return res.status(400).json(BAD_TASK_ID);
  // スナップショット (§1.7 ④) = 画面に見えていた実効値 (マスタ + タスク作成時の値のフォールバック合成)
  const snapshotOf = (t) => {
    if (!t.product_code) return null;
    let snap = null;
    try { snap = t.master_snapshot ? JSON.parse(t.master_snapshot) : null; } catch { /* 壊れていれば master だけで合成 */ }
    const wm = getDB().prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?').get(String(t.product_code).trim().toLowerCase());
    return masterOfTask(wm || null, snap);
  };
  const r = startTaskSession({ taskId, worker, deviceLabel: deviceLabelOf(req), snapshotOf });
  if (!r.ok) {
    const http = r.error === 'bad_request' ? 400 : r.error === 'not_found' ? 404 : 409;
    return res.status(http).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  }
  res.json({ ok: true, already: !!r.already, sessionId: r.sessionId, startedAt: r.startedAt,
    status: r.task.status, task: publicTask(r.task), serverNow: new Date().toISOString() });
}

/** 作業終了 (done) / 中断 (pause)。時間はサーバー時刻の差分で確定 */
router.post('/api/sessions/stop', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const reason = String(req.body?.reason || '');
  if (isAppMode()) {
    const taskId = parseTaskId(req.body?.id ?? req.body?.task_id);
    if (taskId == null) return res.status(400).json(BAD_TASK_ID);
    const r = stopSession({ taskId, workerId: w.worker.id, sessionId: req.body?.session_id, reason });
    safeLogTaskEvent({ taskId, action: 'session_stop', workerId: w.worker.id, workerName: w.worker.display_name,
      deviceLabel: deviceLabelOf(req), to: reason, ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
    if (!r.ok) return res.status(r.error === 'bad_request' ? 400 : 409).json(r);
    return res.json({ ...r, task: publicTask(getTask(taskId)) });
  }
  const pageId = String(req.body?.page_id || req.body?.id || '').trim();
  if (!pageId) return res.status(400).json({ ok: false, error: 'bad_request', message: 'page_id が必要です' });
  const r = stopSession({ pageId, workerId: w.worker.id, sessionId: req.body?.session_id, reason });
  safeLog({ action: 'session_stop', pageId, workerId: w.worker.id, workerName: w.worker.display_name,
    deviceLabel: deviceLabelOf(req), to: reason, ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
  if (!r.ok) return res.status(r.error === 'bad_request' ? 400 : 409).json(r);
  res.json(r);
}));

// セッションの取り消し (誤操作の論理削除。管理者のみ)
router.post('/admin/sessions/:id(\\d+)/void', checkOrigin, requireAdmin, api((req, res) => {
  const r = voidSession(Number(req.params.id), req.session.email, req.body?.reason);
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 409)).json(r);
}));

// ─── 管理画面 ───
router.get('/admin', requireSession, api((req, res) => {
  res.render(path.join(__dirname, 'views/admin'), {
    title: 'いろは在庫化 作業アプリ 管理',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: isAdmin(req),
    base: BASE,
    statuses: STATUSES,
    cache: cacheStatsForAdmin(),
    workers: listIrohaWorkers(true),
    options: workOptionsForState(true),
    migration: migrationStatus(),
    source: sourceOfTruth(),
    devices: isAdmin(req) ? listDevices() : [],
    enrollCodes: isAdmin(req) ? listActiveEnrollCodes() : [],
    events: listEvents(50),
    sessions: listSessionsForAdmin(50),
    media: listMediaForAdmin(50),
    pageSync: listPageSyncForAdmin(),
    driveConfigured: isDriveConfigured(),
  });
}));

router.post('/admin/enroll-codes', checkOrigin, requireAdmin, api((req, res) => {
  try {
    const r = createEnrollCode(req.body?.label, req.session.email);
    res.json({ ok: true, ...r, ttlMinutes: Math.round(ENROLL_TTL_MS / 60000) });
  } catch (e) {
    res.status(400).json({ ok: false, error: 'bad_request', message: e.message });
  }
}));

// 端末登録 (PC から直接。登録と同時に管理者セッションを破棄 — inbound-check と同じ理由)
router.post('/admin/devices', checkOrigin, requireAdmin, api((req, res) => {
  const label = String(req.body?.label || '').trim();
  if (!label || label.length > 40) return res.status(400).json({ ok: false, error: 'bad_label', message: '端末名を1〜40文字で入力してください' });
  const { token, id: deviceId } = createDevice(label, req.session.email);
  req.session.destroy((err) => {
    if (err) {
      console.error('[iroha-work] 端末登録: セッション破棄に失敗', err);
      try { revokeDevice(deviceId); } catch (e2) { console.error('[iroha-work] 端末登録: 失効にも失敗', e2); }
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

// ─── 作業者 (いろは名簿) の管理 ───
router.post('/admin/workers', checkOrigin, requireAdmin, api((req, res) => {
  const r = addIrohaWorker({ displayName: req.body?.display_name, workerType: req.body?.worker_type, actor: req.session.email });
  res.status(r.ok ? 200 : (r.error === 'duplicate' ? 409 : 400)).json(r);
}));

router.post('/admin/workers/:id(\\d+)/active', checkOrigin, requireAdmin, api((req, res) => {
  if (typeof req.body?.active !== 'boolean') return res.status(400).json({ ok: false, error: 'bad_request', message: 'active (true/false) が必要です' });
  if (!setIrohaWorkerActive(Number(req.params.id), req.body.active)) return res.status(404).json({ ok: false, error: 'not_found', message: '作業者が見つかりません' });
  res.json({ ok: true });
}));

// 職員PIN の設定・再設定 (管理者のみ。PIN は保存せずハッシュのみ)
router.post('/admin/workers/:id(\\d+)/pin', checkOrigin, requireAdmin, api((req, res) => {
  const r = setWorkerPin(Number(req.params.id), req.body?.pin, req.session.email);
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 400)).json(r);
}));

// ─── Notion → tasks の移行 (要件 v1.1 §F。管理者だけ。調査/dry-run は読むだけ、本取込だけが書く) ───
// 直近の調査結果はプロセス内に保持 (再取得せずに CSV / 本取込へ進むため)。Render 再起動で消えたら調査し直す。
// plan_id (ランダム) で「本取込するのは自分が見た調査結果」を担保 — 別の管理者が調査し直した結果を適用しない (Codex A1 R1 #12)
let lastPlan = null;   // { planId, at, by, since, cutoff, truncated, rows, summary }
const PLAN_MAX_AGE_MS = 30 * 60 * 1000;
function migrationStatus() {
  try {
    return { counts: countTasksByStatus(), review: listTasksNeedingReview().length, orphans: listOrphans(20), files: listMigrationFiles(),
      lastPlanAt: lastPlan ? lastPlan.at : null, lastPlanBy: lastPlan ? lastPlan.by : null, lastPlanSummary: lastPlan ? lastPlan.summary : null };
  } catch (e) {
    const ref = Date.now().toString(36);
    console.error(`[iroha-work migration ${ref}] 状態の取得に失敗`, e);
    return { error: `状態を取得できませんでした (参照 ${ref}。サーバーログを確認してください)` };
  }
}
const issueCounts = (issues) => Object.fromEntries(Object.entries(issues).map(([k, v]) => [k, v.length]));
// 内部例外の文言を画面に出さない (SQL・パス・Notion の詳細はログへ。画面には参照番号 — Codex A1 R1 #16)
function migrationFail(res, e, what) {
  const ref = Date.now().toString(36);
  console.error(`[iroha-work migration ${ref}] ${what} に失敗`, e);
  const known = e && e.code === 'NOTION_SCHEMA_MISMATCH' ? e.message : null;
  return res.status(500).json({ ok: false, error: 'internal', message: known || `${what}に失敗しました (参照 ${ref}。サーバーログを確認してください)` });
}

router.post('/admin/migration/survey', checkOrigin, requireAdmin, api(async (req, res) => {
  const since = req.body?.since ? String(req.body.since) : null;
  if (since && Number.isNaN(Date.parse(since))) return res.status(400).json({ ok: false, error: 'bad_request', message: 'since は日時 (ISO) で指定してください' });
  let survey, plan;
  try {
    survey = await surveyNotion({ since });
    plan = planImport(survey.pages);
  } catch (e) { return migrationFail(res, e, '調査'); }
  const planId = crypto.randomBytes(6).toString('base64url');
  lastPlan = { planId, at: survey.fetchedAt, by: req.iwUser, since, cutoff: survey.cutoff, truncated: survey.truncated, rows: plan.rows, summary: plan.summary };
  safeLog({ action: 'migration_survey', pageId: null, deviceLabel: `session:${req.iwUser}`, to: `${planId}: ${survey.count}枚${since ? ' since ' + since : ''}`, ok: !survey.truncated });
  res.json({
    ok: true, planId, at: survey.fetchedAt, by: req.iwUser, since, cutoff: survey.cutoff, nextSince: survey.cutoff,
    truncated: survey.truncated, count: survey.count, byStatus: survey.byStatus,
    issues: issueCounts(survey.issues), issueDetail: survey.issues,
    orphans: { unlinked: { sessions: survey.orphans.unlinked.sessions.length, media: survey.orphans.unlinked.media.length },
      missingInNotion: survey.orphans.missingInNotion ? { sessions: survey.orphans.missingInNotion.sessions.length, media: survey.orphans.missingInNotion.media.length } : null },
    file: survey.file ? path.basename(survey.file) : null, rawFile: survey.rawFile ? path.basename(survey.rawFile) : null, summary: plan.summary,
    review: plan.rows.filter(r => r.migration_review || !r.will_import).map(r => ({ page: r.notion_page_id, title: r.title, legacy: r.legacy_status, mapped: r.mapped_status, facility: r.facility_code, warnings: r.warnings, skip: r.skip_reason, url: r.url })),
    reconcile: reconcile(plan.rows, { mode: since ? 'delta' : 'full' }),
  });
}));

router.get('/admin/migration/plan.csv', requireAdmin, api((req, res) => {
  if (!lastPlan) return res.status(409).json({ ok: false, error: 'no_plan', message: '先に「調査 (dry-run)」を実行してください' });
  res.type('text/csv; charset=utf-8');
  res.set('Content-Disposition', `attachment; filename="iroha-migration-plan-${lastPlan.at.slice(0, 19).replace(/[:]/g, '')}.csv"`);
  res.send(planToCsv(lastPlan.rows));
}));

router.post('/admin/migration/apply', checkOrigin, requireAdmin, api((req, res) => {
  if (!lastPlan) return res.status(409).json({ ok: false, error: 'no_plan', message: '先に「調査 (dry-run)」を実行してください' });
  if (req.body?.confirm !== 'APPLY') return res.status(400).json({ ok: false, error: 'confirm_required', message: '確認のため confirm に APPLY と入れてください' });
  if (!req.body?.plan_id || req.body.plan_id !== lastPlan.planId) {
    return res.status(409).json({ ok: false, error: 'plan_mismatch', message: `調査結果が違います (直近の調査は ${lastPlan.by} が ${lastPlan.at} に実行)。画面を更新して調査し直してください` });
  }
  if (lastPlan.truncated) return res.status(409).json({ ok: false, error: 'truncated', message: 'Notion の取得が上限で打ち切られているため取り込めません (件数を確認してください)' });
  if (Date.now() - Date.parse(lastPlan.at) > PLAN_MAX_AGE_MS) return res.status(409).json({ ok: false, error: 'stale_plan', message: '調査が 30 分以上前のものです。調査し直してから取り込んでください' });
  let out;
  try { out = applyImport(lastPlan.rows, { actor: req.iwUser }); }
  catch (e) { return migrationFail(res, e, '本取込 (全て取り消しました)'); }
  safeLog({ action: 'migration_apply', pageId: null, deviceLabel: `session:${req.iwUser}`, to: `${lastPlan.planId}/${out.batchId}: +${out.inserted} ~${out.updated} =${out.kept} skip${out.skipped} journal=${out.journal}`, ok: true });
  res.json({ ok: true, ...out, planId: lastPlan.planId, nextSince: lastPlan.cutoff, reconcile: reconcile(lastPlan.rows, { mode: lastPlan.since ? 'delta' : 'full' }) });
}));

router.get('/admin/migration/status', requireAdmin, api((req, res) => {
  res.json({ ok: true, ...migrationStatus(), source: sourceOfTruth() });
}));

/**
 * 正本の切替 (要件 v1.1 §F の手順 6)。app にすると iPad は tasks を読み書きし、Notion を一切見なくなる。
 * 空の状態で切り替えると現場の一覧が消えるので、未完了タスクが 1 件も無ければ拒否する。
 * 戻す (notion) こともできる — 切替直後に問題が出たときの退路 (アプリ側の更新は tasks に残る)
 */
router.post('/admin/source', checkOrigin, requireAdmin, api((req, res) => {
  const to = String(req.body?.to || '');
  if (to !== 'app' && to !== 'notion') return res.status(400).json({ ok: false, error: 'bad_request', message: 'to は app / notion のどちらかです' });
  if (req.body?.confirm !== 'SWITCH') return res.status(400).json({ ok: false, error: 'confirm_required', message: '確認のため confirm に SWITCH と入れてください' });
  const from = sourceOfTruth();
  if (from === to) return res.json({ ok: true, source: to, unchanged: true });
  const counts = countTasksByStatus();
  const open = OPEN_STATUSES.reduce((s, k) => s + (counts.byStatus[k] || 0), 0);
  if (to === 'app' && open === 0) {
    return res.status(409).json({ ok: false, error: 'no_tasks', message: '未完了のタスクが 1 件もありません。先に Notion からの取込を済ませてください' });
  }
  // Notion へ戻す: app 正本の間の記録は Notion に反映されていない。黙って戻すと古い Notion の状態が現場に出る
  // (終了済みの再実施・二重記録のもと) ので、件数を出して force を要求し、監査ログに残す (Codex A1b R1 #7)
  let changes = null;
  const force = req.body?.force === true;
  if (to === 'notion') {
    changes = countChangesSince(getMeta('source_switched_at') || '1970-01-01T00:00:00.000Z');
    const total = changes.tasks + changes.updatedTasks + changes.sessions + changes.media;
    if (total > 0 && !force) {
      return res.status(409).json({ ok: false, error: 'app_changes_exist', changes,
        message: `アプリ正本にしてからの記録があります (状態変更 ${changes.tasks} 回 / 更新されたタスク ${changes.updatedTasks} 件 / 作業時間 ${changes.sessions} 件 / 写真 ${changes.media} 件)。Notion には反映されていません。それでも戻すなら force を付けてください` });
    }
  }
  // 正本・切替時刻・監査ログは 1 トランザクション (失敗したら切り替わらない → api() が 500 を返す)
  const sw = switchSourceOfTruth({ from, to, actor: req.iwUser, openTasks: open, changes, force });
  console.log(`[iroha-work] 正本を ${from} → ${to} に切り替えました (${req.iwUser}・未完了 ${open} 件${sw.detail})`);
  res.json({ ok: true, source: to, openTasks: open, changes, switchedAt: sw.switchedAt });
}));

// ─── 選択肢 (資材・保管箱) の管理 ───
router.post('/admin/options', checkOrigin, requireAdmin, api((req, res) => {
  // 管理者は無効化した候補を同じ値の追加で戻せる (職員はできない)
  const r = addWorkOption({ kind: req.body?.kind, code: req.body?.code, actor: req.session.email, allowReactivate: true });
  res.status(r.ok ? 200 : 400).json(r);
}));
router.post('/admin/options/:id(\\d+)/active', checkOrigin, requireAdmin, api((req, res) => {
  if (typeof req.body?.active !== 'boolean') return res.status(400).json({ ok: false, error: 'bad_request', message: 'active (true/false) が必要です' });
  if (!setWorkOptionActive(Number(req.params.id), req.body.active)) return res.status(404).json({ ok: false, error: 'not_found', message: '選択肢が見つかりません' });
  res.json({ ok: true });
}));
router.post('/admin/options/:id(\\d+)/image', checkOrigin, requireAdmin, api((req, res) => {
  const r = setWorkOptionImage(Number(req.params.id), req.body?.image_url);
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 400)).json(r);
}));

export default router;
