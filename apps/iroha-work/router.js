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
  workOptionsByKind, addWorkOption, setWorkOptionActive, setWorkOptionImage, moveWorkOption, seedWorkOptionsFromMaster, BUILTIN_OPTION_IMAGES,
  setWorkerPin, verifyWorkerPin,
  logEvent, listEvents,
  getCachePage, startSession, stopSession, listSessionsForAdmin, voidSession,
  getMeta, setMetaValue, sourceOfTruth,
} from './db.js';
import { ensureFresh, changeStatus, fetchCardLive, cacheStatsForAdmin, STATUSES } from './notion-read.js';
import { surveyNotion, planImport, planToCsv, applyImport, reconcile, listMigrationFiles } from './migrate.js';
import { countTasksByStatus, listTasksNeedingReview, listOrphans } from './tasks-db.js';
import { OPEN_STATUSES } from './tasks.js';
import { buildList, buildTaskList, buildTaskCard, buildHistory, buildPlan, classifyMasterEdit, clearEnrichCache, masterOf, masterOfTask, jstToday, jstTomorrow, whenOf } from './service.js';
import { capabilitiesFor } from './capabilities.js';
import { transitionNeedsStaff, TASK_STATUSES, statusLabel } from './tasks.js';
import {
  getTask, changeTaskStatus, setPlannedDate, clearMigrationReview, resolveCancellation,
  listLabelWaits, upsertLabelWait, listClosedTasks, taskErrorStatus, safeLogTaskEvent, setExternalReady,
  listNamelessTasks, removeStrayTask, setFacility, setProgress,
  startTaskSession, countChangesSince, switchSourceOfTruth, bulkCloseReady,
} from './tasks-db.js';
import { updateWorkMasterRow, addWorkMasterRow, codeKeyOf } from '../inbound-check/work-master.js';
import { notionSweepRunning } from '../inbound-check/notion-sync.js';
import { listLinkConflicts, countLinkConflicts, mergeLinkConflict } from './task-intake.js';
import { startStaffUnlock, staffUnlockOf, endStaffUnlock, STAFF_UNLOCK_MS } from './db.js';
import {
  addMedia, inspectMediaUpload, moveStoredFile, promoteStagedMedia, dropMedia, cardWriteBlockReason, recordMediaCancel, softDeleteMedia, resetMedia, listMediaForAdmin, schedule as scheduleMedia, getMediaRow, driveDownload,
  reportMediaUnavailable, recheckUnavailable, etagMatches, ifRangeMatches, singleRange,
  listPageSyncForAdmin, resetPageSync,
  isDriveConfigured, MEDIA_DIR, MAX_PHOTO_BYTES, MAX_PHOTOS,
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

/**
 * ⭐職員モード (要件 §W-3)。「明日の計画」は何件も続けてタップするので、毎回 PIN を聞くと現場が止まる。
 * 職員 PIN を 1 回入れたら、その端末で 30 分だけ計画の操作を通す。
 * ポータル (PC の管理画面) から入っている人は常に職員扱い。
 * @returns {{staff:true, until:string|null, via:'session'|'device'} | {staff:false}}
 */
function staffModeOf(req) {
  if (hasSessionAccess(req)) return { staff: true, until: null, via: 'session' };
  const u = req.iwDevice ? staffUnlockOf(req.iwDevice.id) : null;
  return u ? { staff: true, until: u.until, via: 'device' } : { staff: false };
}

/**
 * 書く直前に (更新と同じトランザクションの中で) もう一度確かめる関門。
 * 職員モードが切れていないか + その人がいまも有効な職員か (別の接続で無効にされ得る — Codex P1 R2)
 */
function planGuardOf(req, worker) {
  return () => {
    if (!staffModeOf(req).staff) return { ok: false, error: 'staff_required', message: '職員モードが切れました (PINを入れ直してください)' };
    if (hasSessionAccess(req)) return null;   // ポータルの人は名簿の状態に関係なく職員
    const now = getIrohaWorker(worker.id);
    if (!now || !now.active || now.worker_type !== 'staff') {
      return { ok: false, error: 'staff_required', message: 'この作業者は計画を決められません (一覧を更新してください)' };
    }
    return null;
  };
}

/**
 * 計画の操作 (いつ / どこが) の入口。職員モード中ならそのまま、そうでなければ職員 PIN を受け付ける
 * (受け取ったらその端末を職員モードにする = 続けてタップできる)。
 * ⚠**送られてきた中身を確かめた後に呼ぶこと**。先に呼ぶと、中身が不正で 400 を返すのに
 *   端末だけ 30 分開いてしまう (Codex P1 R2)
 * @returns {{ok:true, worker, staffUntil}} | {{ok:false, status, body}}
 */
function requireStaffPlan(req) {
  const w = resolveWorker(req);
  if (w.error) return { ok: false, status: 400, body: { ok: false, error: 'worker_required', message: w.error } };
  const mode = staffModeOf(req);
  // ⭐職員モード中でも、記録に残る「やった人」が職員でなければ通さない。
  //   そうしないと、職員が開けた端末で利用者の名前を選び「利用者が計画を決めた」記録になる (Codex P1 R1)
  if (mode.staff && (mode.via === 'session' || w.worker.worker_type === 'staff')) {
    return { ok: true, worker: w.worker, staffUntil: mode.until };
  }
  // 職員モードでない → その場で PIN を受ける
  if (w.worker.worker_type !== 'staff') {
    return { ok: false, status: 403, body: { ok: false, error: 'staff_required', message: '明日の計画を決められるのは職員だけです (職員の名前を選び、PINを入れてください)' } };
  }
  const pin = verifyWorkerPin(w.worker.id, req.body?.pin);
  if (!pin.ok) return { ok: false, status: STATUS_HTTP[pin.error] || 403, body: { ok: false, ...pin } };
  const until = req.iwDevice ? startStaffUnlock(req.iwDevice.id, w.worker.id) : null;
  return { ok: true, worker: w.worker, staffUntil: until };
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
/**
 * Notion 正本のとき、数値の id (= f_iroha_tasks の id。下見のボード・履歴の詳細で見えるカード) を
 * 書き込み先に受け取らない。画面は許可リスト (capabilities) で操作を描かないが、サーバーでも必ず断る (要件 v1.3 §P Q5)
 */
const PREVIEW_WRITE_REJECTED = { ok: false, error: 'notion_mode', message: '下見のカードには書き込めません (正本は Notion です)' };
const isPreviewIdInNotionMode = (cardId) => !isAppMode() && parseTaskId(cardId) != null;
const CLOSED_WRITE_REJECTED = { ok: false, error: 'closed_task', message: '終了したカードは変えられません (履歴として残ります)' };
/**
 * 履歴 (終了したカード) の id で書き込みに来ていないか。画面は許可リストで操作を描かないが、
 * 古いタブや作った要求からは届くので、**書き込み口はすべて**ここを通す (要件 v1.3 §P Q5 / Codex PR1 R2)。
 * 「終了からのやり直し」だけは職員 + 理由つきの正式な操作なので /api/status に残す
 */
function isClosedCardId(cardId) {
  if (!isAppMode()) return false;
  const id = parseTaskId(cardId);
  if (id == null) return false;
  const t = getTask(id);
  return !!t && t.status === 'closed';
}
/**
 * 詳細から呼ぶ書き込み (作業のやり方・候補の登録) の入口。**カードの指定を必須**にして、
 * そのカードが「いま書ける」ものかをサーバーで確かめる。断るなら返す本文、通るなら null。
 * 省略・空・でたらめな id で検査を素通りできないようにする (Codex PR1 R3)
 */
/**
 * カードの確認と書き込みを 1 つのトランザクションで行う (Codex PR1 R5)。
 * 確認した直後に別の接続がカードを終了させても、書き込みが後から成立しないようにする。
 * fn が返した値をそのまま返す。断るときは { deny } を返す
 */
function inWritableCard(rawId, opts, fn) {
  return getDB().transaction(() => {
    const gate = writableCard(rawId, opts);
    if (gate.deny) return { deny: gate.deny };
    return fn(gate);
  }).immediate();
}

function writableCard(rawId, { code = null } = {}) {
  const cardId = String(rawId == null ? '' : rawId).trim();
  if (!cardId) return { deny: { ok: false, error: 'card_required', message: 'どのカードからの操作かが必要です (一覧を更新してください)' } };
  let productCode = null;
  if (isAppMode()) {
    const id = parseTaskId(cardId);
    const t = id == null ? null : getTask(id);
    if (!t) return { deny: { ok: false, error: 'card_required', message: 'カードが見つかりません (一覧を更新してください)' } };
    if (t.status === 'closed') return { deny: CLOSED_WRITE_REJECTED };
    productCode = t.product_code;
  } else {
    // Notion 正本: 数値 id は下見のカード。通すのはキャッシュにある Notion のカードだけ
    if (parseTaskId(cardId) != null) return { deny: PREVIEW_WRITE_REJECTED };
    const page = getCachePage(cardId);
    if (!page) return { deny: { ok: false, error: 'card_required', message: 'カードが見つかりません (一覧を更新してください)' } };
    productCode = page.product_code;
  }
  // ⭐そのカードの商品と、書き換えようとしている商品が同じか。ここを見ないと、
  //   「開いている適当なカード」を添えて、下見・履歴にしかない別商品の作業のやり方を書き換えられる (Codex PR1 R4)
  if (code != null) {
    if (!productCode || codeKeyOf(productCode) !== codeKeyOf(code)) {
      return { deny: { ok: false, error: 'card_mismatch', message: 'カードとちがう商品には登録できません (一覧を更新してください)' } };
    }
  }
  return { productCode };
}

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
// sourceOfTruth は db.js (入荷受付の Notion 送信も同じ値を見る)
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
  const staffMode = staffModeOf(req);   // 1 回だけ読む (許可リストと staff_mode が食い違わない — Codex P1 R2)
  res.json({
    ok: true,
    ...list,
    workers: listIrohaWorkers(),
    options: workOptionsForState(),
    refresh,
    // 画面に許す操作。画面はこのリストにある操作だけ描く (default-deny — 要件 v1.3 §P Q5)。
    // 計画 (いつ / どこが) は職員モードのときだけ入る (要件 §W-1)
    capabilities: capabilitiesFor(appMode ? 'app' : 'notion', { staff: staffMode.staff }),
    staff_mode: staffMode,
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
  if (isPreviewIdInNotionMode(pageId)) return res.status(409).json(PREVIEW_WRITE_REJECTED);

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
    // ⭐できた数と中断メモ (要件 §Y)。送られたときだけ書き換える。
    //   状態と同じ 1 回の書き込みに載せる — 分けると「保留にはなったが数は入らなかった」が起きる
    doneQty: 'done_qty' in (req.body || {}) ? req.body.done_qty : undefined,
    holdMemo: 'hold_memo' in (req.body || {}) ? req.body.hold_memo : undefined,
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
    done_qty: t.done_qty ?? null, hold_memo: t.hold_memo || null,
    planned_date: t.planned_date, when: t.status === 'closed' ? null : whenOf(t.planned_date), cancellation_requested_at: t.cancellation_requested_at, migration_review: !!t.migration_review,
    external_ready: !!t.external_ready,
  };
}

/**
 * 「今日やる / 後日」(アプリ正本のみ)。⭐P1 で /api/plan に置き換えた古い入口。
 * 画面が使わなくなったら消す (要件 §W-3) が、**残っている間も同じ職員の関門を通す** —
 * 画面からボタンを消しても、この口を直接叩けば計画を変えられてしまう (Codex P1 R1)
 */
router.post('/api/planned', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const plannedTaskId = parseTaskId(req.body?.id);
  if (plannedTaskId == null) return res.status(400).json(BAD_TASK_ID);
  // ⭐入れられる日付は「今日 / 明日 / 未定」だけ (新しい /api/plan と同じ)。
  //   ここを開けたままだと、古い入口から先の日付や存在しない日付を直に入れられる (Codex P1 R2)
  const dateIn = req.body?.planned_date ?? null;
  const todayP = jstToday();
  if (dateIn !== null && dateIn !== todayP && dateIn !== jstTomorrow(todayP)) {
    return res.status(400).json({ ok: false, error: 'bad_request', message: '入れられるのは今日か明日だけです (画面を更新してください)' });
  }
  const gate = requireStaffPlan(req);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const r = setPlannedDate({
    taskId: plannedTaskId, plannedDate: dateIn, expectVersion: req.body?.expect_version,
    actor: `${gate.worker.display_name} (いろはアプリ)`, workerId: gate.worker.id, workerName: gate.worker.display_name, deviceLabel: deviceLabelOf(req),
    guard: planGuardOf(req, gate.worker),
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  res.json({ ok: true, task: publicTask(r.task) });
}));

/**
 * ⭐職員モードに入る (要件 §W-3)。職員 PIN を 1 回。以後 30 分は計画の操作を PIN なしで通す。
 * 端末 (iPad) 単位。ポータルから入っている人は最初から職員扱いなので呼ぶ必要がない
 */
router.post('/api/staff-unlock', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  if (hasSessionAccess(req)) return res.json({ ok: true, staff_mode: staffModeOf(req) });
  if (!req.iwDevice) return res.status(403).json({ ok: false, error: 'forbidden', message: 'この端末では使えません (端末登録が必要です)' });
  if (w.worker.worker_type !== 'staff') {
    return res.status(403).json({ ok: false, error: 'staff_required', message: '職員の名前を選んでください' });
  }
  const pin = verifyWorkerPin(w.worker.id, req.body?.pin);
  if (!pin.ok) return res.status(STATUS_HTTP[pin.error] || 403).json({ ok: false, ...pin });
  startStaffUnlock(req.iwDevice.id, w.worker.id);
  safeLog({ action: 'staff_unlock', workerId: w.worker.id, workerName: w.worker.display_name, deviceLabel: deviceLabelOf(req), to: 'on', ok: true });
  res.json({ ok: true, staff_mode: staffModeOf(req), minutes: Math.round(STAFF_UNLOCK_MS / 60000) });
}));

/** 職員モードを終える (端末を置いて離れるとき)。誰でも押せる = 締める方向はいつでも通す */
router.post('/api/staff-lock', checkOrigin, api((req, res) => {
  if (req.iwDevice) endStaffUnlock(req.iwDevice.id);
  res.json({ ok: true, staff_mode: staffModeOf(req) });
}));

/**
 * ⭐「いつやるか」(要件 §W-3)。明日やる / 今日やる / 未定 の 3 つだけ。
 * planned_date には**実日付**を入れる (深夜の書き換えバッチは作らない — 日付が変わるだけで明日が今日になる)
 */
const PLAN_WHEN = new Set(['today', 'tomorrow']);
router.post('/api/plan', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  // ⭐中身を先に見る (不正な要求で職員モードだけ開いてしまわないように — Codex P1 R2)
  const taskId = parseTaskId(req.body?.id);
  if (taskId == null) return res.status(400).json(BAD_TASK_ID);
  const when = req.body?.when ?? null;
  if (when !== null && !PLAN_WHEN.has(when)) {
    return res.status(400).json({ ok: false, error: 'bad_request', message: 'when は today / tomorrow / null のどれかです' });
  }
  const gate = requireStaffPlan(req);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const today = jstToday();
  const date = when === 'today' ? today : when === 'tomorrow' ? jstTomorrow(today) : null;
  const r = setPlannedDate({
    taskId, plannedDate: date, expectVersion: req.body?.expect_version,
    actor: `${gate.worker.display_name} (いろはアプリ)`, workerId: gate.worker.id, workerName: gate.worker.display_name, deviceLabel: deviceLabelOf(req),
    // 書く直前にも職員モードと「いまも有効な職員か」を見る (要件 §U-2)
    guard: planGuardOf(req, gate.worker),
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  res.json({ ok: true, task: publicTask(r.task), staff_mode: staffModeOf(req) });
}));

/**
 * ⭐「どこが作業するか」(要件 §W-3)。拠点だけを変える — 進捗も予定も変えない。
 * NULL = 未定に戻す (いろはも正式な割り振り先なので「未定 = いろは」と見なさない)
 */
router.post('/api/facility', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  // ⭐中身を先に見る (不正な要求で職員モードだけ開いてしまわないように — Codex P1 R2)
  const taskId = parseTaskId(req.body?.id);
  if (taskId == null) return res.status(400).json(BAD_TASK_ID);
  const code = req.body?.facility_code ?? null;
  if (code !== null && typeof code !== 'string') {
    return res.status(400).json({ ok: false, error: 'bad_request', message: 'facility_code は拠点コードか null です' });
  }
  const gate = requireStaffPlan(req);
  if (!gate.ok) return res.status(gate.status).json(gate.body);
  const r = setFacility({
    taskId, facilityCode: code, expectVersion: req.body?.expect_version,
    actor: `${gate.worker.display_name} (いろはアプリ)`, workerId: gate.worker.id, workerName: gate.worker.display_name, deviceLabel: deviceLabelOf(req),
    guard: planGuardOf(req, gate.worker),
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  res.json({ ok: true, task: publicTask(r.task), staff_mode: staffModeOf(req) });
}));

/** 「明日の計画」画面のデータ (職員だけ)。読むだけなので DB は変えない */
/**
 * 「明日の計画」画面のデータ。読むだけなので DB は変えない。
 * ⭐正本が Notion のあいだ (下見) も返す — 切替の前に「どういう形か」を見せるため
 * (ボード・ラベル待ち・履歴と同じ扱い。要件 v1.3 §P Q5)。書き変えは今までどおり 409
 */
router.get('/api/plan', api((req, res) => {
  const preview = !isAppMode();
  // 下見は誰でも読むだけ。アプリ正本のときは職員だけ (書き変えられる画面なので)
  if (!preview && !staffModeOf(req).staff) {
    return res.status(403).json({ ok: false, error: 'staff_required', message: '明日の計画を見られるのは職員だけです' });
  }
  res.json({ ok: true, preview, ...buildPlan({ readOnly: preview }), staff_mode: staffModeOf(req), serverNow: new Date().toISOString() });
}));

/** 「外部施設に出す準備OK」の切り替え (アプリ正本のみ)。状態とは別のチェック */
router.post('/api/external-ready', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const taskId = parseTaskId(req.body?.id);
  if (taskId == null) return res.status(400).json(BAD_TASK_ID);
  // true / false だけ受ける (欠落や文字列を「解除」と読まない — Codex FB R2)
  if (typeof req.body?.ready !== 'boolean') return res.status(400).json({ ok: false, error: 'bad_request', message: 'ready は true / false で指定してください' });
  const r = setExternalReady({
    taskId, ready: req.body.ready, expectVersion: req.body?.expect_version,
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

/**
 * ⭐できた数・中断メモを**あとから直す** (要件 §Y。アプリ正本のみ)。状態は変えない。
 * 中断するときは /api/status に一緒に載せるので、こちらは数え間違いの直しと申し送りの追記用。
 * 作業した本人が直せないと現場が止まるので、状態を変えられる人 (= 利用者も) なら直せる。職員限定にしない
 */
router.post('/api/progress', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const progressTaskId = parseTaskId(req.body?.id);
  if (progressTaskId == null) return res.status(400).json(BAD_TASK_ID);
  const r = setProgress({
    taskId: progressTaskId, expectVersion: req.body?.expect_version,
    doneQty: 'done_qty' in (req.body || {}) ? req.body.done_qty : undefined,
    holdMemo: 'hold_memo' in (req.body || {}) ? req.body.hold_memo : undefined,
    actor: hasSessionAccess(req) ? req.iwUser : `${w.worker.display_name} (いろはアプリ)`,
    workerId: w.worker.id, workerName: w.worker.display_name, deviceLabel: deviceLabelOf(req),
    // 正本の切替は version を変えないので、更新と同じトランザクションでもう一度見る (要件 §U-2)
    guard: () => (isAppMode() ? null : { ok: false, error: 'notion_mode', message: '正本が Notion に戻りました (一覧を更新してください)' }),
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json({ ...r, current: r.current ? publicTask(r.current) : undefined });
  res.json({ ok: true, already: !!r.already, task: publicTask(r.task) });
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

/**
 * 切替前の下見 (読むだけ)。正本が Notion でも f_iroha_tasks の一覧を返す —
 * ボード・ラベル待ち・履歴が「どういう形か」を切り替える前に見せるため (中原さん 2026-09-03)。
 * ⚠中身は取込時点の状態。Notion 側でその後に動かした分は入っていない (画面にもその旨を出す)
 */
router.get('/api/preview-tasks', api((req, res) => {
  // 読むだけ: 画像の取り寄せも、写真の修復印も起こさない (Codex PR1 R8)
  res.json({ ok: true, ...buildTaskList({ readOnly: true }), preview: !isAppMode(), capabilities: capabilitiesFor('preview'), serverNow: new Date().toISOString() });
}));

/**
 * 詳細 1 枚を読むだけで返す (下見のボード・履歴から開く。要件 v1.3 §P Q5 / PR1)。正本を問わず開けるが、
 * 何も許さない (capabilities: [])。終了したタスクも返す (履歴)。
 * 鮮度は項目ごとに違う: 状態 = Notion を取り込んだ時点 (notionSyncedAt)・在庫 = card.loc_at・
 * 作業のやり方/写真/作業時間 = いまのアプリ DB — 画面で分けて出す
 */
router.get('/api/task-previews/:id(\\d+)', api((req, res) => {
  // 読むだけなので、画像の取り寄せ (キューへの書き込み) も起こさない — Codex PR1 R7
  const card = buildTaskCard(parseTaskId(req.params.id), { queueImages: false, readOnly: true });
  if (!card) return res.status(404).json({ ok: false, error: 'not_found', message: 'カードが見つかりません' });
  res.json({
    ok: true, preview: true, card, capabilities: capabilitiesFor('preview'),
    notionSyncedAt: lastNotionImportAt(), serverNow: new Date().toISOString(),
  });
}));
/** Notion を最後に取り込んだ時刻。meta に無ければ (この記録を始める前の取込)、取込が最後に書いた行の時刻で代える */
function lastNotionImportAt() {
  const m = getMeta('last_import_at');
  if (m) return m;
  try {
    const r = getDB().prepare("SELECT MAX(updated_at) AS at FROM f_iroha_tasks WHERE updated_by LIKE 'import:%'").get();
    return (r && r.at) || null;
  } catch { return null; }
}

/**
 * まとめて棚入完了 (アプリ正本のみ)。棚入待ちのカードだけを 終了 (棚入完了) にする。
 * 職員限定 (transitionNeedsStaff('ready_for_stocking','closed') と同じ扱い): ポータルのセッション、
 * または職員の作業者 + PIN。選んだ後に状態が変わっていた分は skipped で返す
 */
router.post('/api/bulk-stocked', checkOrigin, api((req, res) => {
  if (!isAppMode()) return res.status(409).json({ ok: false, error: 'notion_mode', message: 'Notion が正本の間は使えません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  if (!hasSessionAccess(req)) {
    if (w.worker.worker_type !== 'staff') return res.status(403).json({ ok: false, error: 'staff_required', message: '棚入完了にできるのは職員だけです (職員の名前を選び、PINを入れてください)' });
    const pinCheck = verifyWorkerPin(w.worker.id, req.body?.pin);
    if (!pinCheck.ok) return res.status(STATUS_HTTP[pinCheck.error] || 403).json({ ok: false, ...pinCheck });
  }
  // 画面は「選んだときに見えていた版」を必ず添える。選んでから別の端末が変えていたら、その分は飛ばす
  // (単票の変更と同じ楽観ロック — Codex PR1 R7)
  const raw = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const items = raw.map((v) => {
    // null・空文字・false を Number() が 0 にするので、数値であることを先に見る (Codex PR1 R8)
    if (!v || typeof v !== 'object' || typeof v.version !== 'number') return null;
    const id = parseTaskId(v.id);
    if (id == null || !Number.isSafeInteger(v.version) || v.version < 0) return null;
    return { id, version: v.version };
  });
  if (items.some((v) => v == null)) {
    return res.status(400).json({ ok: false, error: 'bad_request',
      message: '画面が古いようです。一覧を更新してから選び直してください' });
  }
  const r = bulkCloseReady({
    taskIds: items, actor: hasSessionAccess(req) ? req.iwUser : `${w.worker.display_name} (いろはアプリ)`,
    workerId: w.worker.id, workerName: w.worker.display_name, deviceLabel: deviceLabelOf(req),
  });
  if (!r.ok) return res.status(taskErrorStatus(r.error)).json(r);
  res.json(r);
}));

/** JST の日付 (YYYY-MM-DD) */
const jstDay = (d) => new Date(d.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10);

/** 履歴 (終了したカード。一覧には出さないので、ここで期間・商品名で探す)。読むだけなので切替前でも開ける */
router.get('/api/history', api((req, res) => {
  // YYYY-MM-DD かつ**実在する日**だけ受ける (2026-99-99 や 2026-02-30 は Date が例外・別の日に化ける — Codex PR-C R1)
  const day = (v) => {
    const t = String(v == null ? '' : v).trim();
    if (!t) return undefined;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return null;
    const d = new Date(`${t}T00:00:00+09:00`);
    if (Number.isNaN(d.getTime())) return null;
    return jstDay(d) === t ? t : null;   // 2026-02-30 → 3/2 に正規化されたら不正
  };
  const from = day(req.query.from);
  const to = day(req.query.to);
  if (from === null || to === null) return res.status(400).json({ ok: false, error: 'bad_request', message: '日付は YYYY-MM-DD (実在する日) で指定してください' });
  const jstStart = (d) => new Date(`${d}T00:00:00+09:00`).toISOString();
  // closed_at は UTC の ISO。JST の日付で絞る。**終了日はその日を含む** ので、その日の 00:00 JST + 24 時間が上限。
  // ⚠日付の文字列に戻してから解釈し直さない (9999-12-31 の翌日は西暦 10000 = 拡張年表記になり、切り出しに耐えない)。
  //   ここは「時刻そのもの」を上限にするので、その問題が起きない — Codex PR-C R3
  const upperOf = (d) => new Date(new Date(`${d}T00:00:00+09:00`).getTime() + 86400000).toISOString();
  res.json({
    ok: true,
    ...buildHistory({
      from: from ? jstStart(from) : null,
      to: to ? upperOf(to) : null,
      q: req.query.q ? String(req.query.q).slice(0, 100) : null,
      limit: req.query.limit,
    }),
    preview: !isAppMode(),
    fromDate: from || null, toDate: to || null,
  });
}));

/** ラベル待ちの一覧。読むだけなので切替前でも開ける (登録・更新はアプリ正本のみ) */
router.get('/api/label-waits', api((req, res) => {
  const taskId = req.query.task_id == null ? null : parseTaskId(req.query.task_id);
  if (req.query.task_id != null && taskId == null) return res.status(400).json(BAD_TASK_ID);
  res.json({ ok: true, preview: !isAppMode(), rows: listLabelWaits({ taskId, openOnly: req.query.all !== '1' }) });
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

const MASTER_FIELDS = ['material_code', 'storage_container', 'units_per_container', 'process_count', 'note', 'video_url', 'size_class', 'expiry_seal'];

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
  // 候補じたいは商品に紐づかない共有マスタなので Notion 正本でも登録できる。ただし下見のカード id を
  // 添えた要求は受けない (「下見の id を送っても DB が変わらない」を全ての書き込み口で同じにする)
  // 候補の登録は詳細のダイアログからしか呼ばない。どのカードから来たかを必ず添えてもらい、
  // 「いま書けるカード」でなければ断る (省略・空・でたらめで検査を素通りできないように — Codex PR1 R3)
  // 断る条件は下の書き込みと同じトランザクションでもう一度見る (ここは先に軽く弾くだけ)
  const optGate = writableCard(req.body?.id ?? req.body?.page_id);
  if (optGate.deny) return res.status(409).json(optGate.deny);
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
  const r = inWritableCard(req.body?.id ?? req.body?.page_id, {},
    () => addWorkOption({ kind: req.body?.kind, code: req.body?.code, actor, allowReactivate: false }));
  if (r.deny) return res.status(409).json(r.deny);
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
  // どのカードからの操作かを、中身の検証より前に確かめる。カードとその商品が一致していることも必須
  // (省略・でたらめ・よそのカードで素通りさせない — Codex PR1 R3 / R4)
  const masterGate = writableCard(req.body?.id ?? req.body?.page_id, { code });
  if (masterGate.deny) return res.status(409).json(masterGate.deny);
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
      // 書き込みの直前にもう一度カードを見る (確認の後に別の接続が終了させていたら、ここで止める)
      const gate = writableCard(req.body?.id ?? req.body?.page_id, { code });
      if (gate.deny) { const e = new Error('card'); e[rollback] = { status: 409, body: gate.deny }; throw e; }
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
// 動画は当面なしなので、受け取る上限は写真の分だけ (中原さん 2026-09-03)。
// ⚠上限超えは multer がハンドラーより前で弾く → そのままだと 500 や HTML になる。
//   ここで受けて JSON の 413 にし、一時ファイルも片づける (Codex R1)
const mediaUpload = multer({ dest: MEDIA_TMP, limits: { fileSize: MAX_PHOTO_BYTES } });
const mediaUploadOne = (req, res, next) => mediaUpload.single('file')(req, res, (err) => {
  if (!err) return next();
  if (req.file && req.file.path) { try { fs.unlinkSync(req.file.path); } catch { /* 無い */ } }
  if (err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({ ok: false, error: 'too_large',
      message: `ファイルが大きすぎます (上限 ${Math.round(MAX_PHOTO_BYTES / 1024 / 1024)}MB)。写真をとり直してください` });
  }
  if (err.code === 'LIMIT_UNEXPECTED_FILE') return res.status(400).json({ ok: false, error: 'bad_request', message: 'ファイルの送り方が違います' });
  console.error('[iroha-work] media upload', err);
  return res.status(400).json({ ok: false, error: 'bad_request', message: 'ファイルを受け取れませんでした' });
});

/**
 * 撮影した写真・動画の受信。実体を outbox (DATA_DIR) に置いて**即応答** — Drive/Notion へは
 * 裏のキューが送る (§1.7 ②)。operation_id で再送を冪等化。
 */
router.post('/api/media', checkOrigin, mediaUploadOne, api((req, res) => {
  const cleanup = () => { if (req.file) { try { fs.unlinkSync(req.file.path); } catch { /* 移動済みなら無い */ } } };
  try {
    const w = resolveWorker(req);
    if (w.error) { cleanup(); return res.status(400).json({ ok: false, error: 'worker_required', message: w.error }); }
    if (!req.file) return res.status(400).json({ ok: false, error: 'no_file', message: 'ファイルがありません' });
    if (!isDriveConfigured()) { cleanup(); return res.status(503).json({ ok: false, error: 'not_configured', message: 'ドライブ保存が未設定です (職員の方は管理者に連絡してください)' }); }
    const cardId = String(req.body?.id || req.body?.page_id || '').trim();
    if (isPreviewIdInNotionMode(cardId)) { cleanup(); return res.status(409).json(PREVIEW_WRITE_REJECTED); }
    const kind = String(req.body?.kind || '');
    if (kind === 'video') { cleanup(); return res.status(400).json({ ok: false, error: 'video_disabled', message: '動画は今つかえません (写真をとってください)' }); }
    if (!cardId || kind !== 'photo') {
      cleanup(); return res.status(400).json({ ok: false, error: 'bad_request', message: 'カードと種類 (photo) が必要です' });
    }
    const appMode = isAppMode();
    // 正本に合わせて、カードを task_id (アプリ正本) か page_id (Notion 正本) で引く
    if (appMode && parseTaskId(cardId) == null) { cleanup(); return res.status(400).json(BAD_TASK_ID); }
    const task = appMode ? getTask(parseTaskId(cardId)) : null;
    const card = appMode ? null : getCachePage(cardId);
    if (appMode ? !task : !card) { cleanup(); return res.status(404).json({ ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' }); }
    // 終了したカード (履歴) には足せない。削除を断っているのと同じ理由 — 履歴は読むだけ (要件 v1.3 §P Q5)。
    // ファイルの検査 (大きさ・中身が本当に JPEG か) はトランザクションに入る前に済ませる。
    // 書き込みロックを持ったままファイルを読まない (miniPC も同じ DB を開く — Codex PR1 R6)
    const inspected = inspectMediaUpload({ kind, filePath: req.file.path, operationId: req.body?.operation_id });
    if (!inspected.ok) { cleanup(); return res.status(400).json(inspected); }
    // ⭐正本モードもカードも**トランザクションの中で読み直す**。外で読むと、読んだ後・書く前に
    //   正本が Notion に戻ったり、カードが終了したりできる (Codex PR1 R5 / R6)
    const NOT_FOUND = { status: 404, body: { ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' } };
    const out = getDB().transaction(() => {
      const nowApp = isAppMode();
      let taskNow = null;
      let cardNow = null;
      if (nowApp) {
        const tid = parseTaskId(cardId);
        taskNow = tid == null ? null : getTask(tid);
        if (!taskNow) return { deny: NOT_FOUND };
        if (taskNow.status === 'closed') {
          return { deny: { status: 409, body: { ok: false, error: 'closed_task', message: '終了したカードには写真を足せません (履歴として残ります)' } } };
        }
      } else {
        // 正本が Notion に戻っていたら、数値 id は下見のカード。書かせない
        if (parseTaskId(cardId) != null) return { deny: { status: 409, body: PREVIEW_WRITE_REJECTED } };
        cardNow = getCachePage(cardId);
        if (!cardNow) return { deny: NOT_FOUND };
      }
      return {
        appMode: nowApp,
        taskId: nowApp ? taskNow.id : null,
        r: addMedia({
          pageId: nowApp ? null : cardId, taskId: nowApp ? taskNow.id : null,
          // 商品コードも読み直した行から取る (取込と重なって食い違わないように)
          productCode: nowApp ? taskNow.product_code : cardNow.product_code, kind, mime: req.file.mimetype,
          filePath: req.file.path, worker: w.worker, deviceLabel: deviceLabelOf(req),
          deviceId: req.iwDevice ? req.iwDevice.id : null,
          operationId: req.body?.operation_id, inspected, deferMove: true,
        }),
      };
    }).immediate();
    if (out.deny) { cleanup(); return res.status(out.deny.status).json(out.deny.body); }
    const r = out.r;
    if (!r.ok) { cleanup(); return res.status(r.error === 'cap_reached' || r.error === 'operation_conflict' ? 409 : 400).json(r); }
    // 実体を置くのはトランザクションを抜けてから。置いてはじめて staging → stored に上げる。
    // 上げる前に落ちても、行は staging のまま = 一覧にも送信キューにも出ず、再送で置き直せる (Codex PR1 R7)
    // 前の札で置いたファイルはもうどの行からも指されない (再送で置き場所が変わった) — 片づける
    if (r.stale) { try { fs.unlinkSync(r.stale); } catch { /* 無ければよい */ } }
    if (r.move) {
      try {
        moveStoredFile(r.move);
      } catch (e) {
        // 片づけるのは「自分の札のまま・まだ公開されていない」行だけ (二重送信の相手の行は消さない)
        try { dropMedia(r.media.id, r.claim); } catch { /* 消せなくても応答は失敗にする */ }
        cleanup();
        console.error('[iroha-work] 写真の保存に失敗', e);
        return res.status(500).json({ ok: false, error: 'store_failed', message: '写真を保存できませんでした (もう一度とってください)' });
      }
      const promoted = promoteStagedMedia(r.media.id, r.move.to, { claim: r.claim });
      if (promoted.ok) {
        r.media = promoted.media;
      } else if (promoted.reason === 'cap_reached') {
        // 置いている間に他の写真で枠が埋まった (Codex PR1 R11)
        try { dropMedia(r.media.id, r.claim); } catch { /* 消せなくても応答は失敗にする */ }
        try { fs.unlinkSync(r.move.to); } catch { /* 無ければよい */ }
        return res.status(409).json({ ok: false, error: 'cap_reached',
          message: `写真は${MAX_PHOTOS}枚までです。不要な写真を削除してから撮り直してください` });
      } else if (promoted.reason === 'not_writable') {
        // 実体を置いている間にカードが終了した / 正本が Notion に戻った。写真は残さない (Codex PR1 R10)。
        // 断った本当の理由をそのまま返す (画面が「下見だから」と「終了したから」を区別できるように — R12)
        try { dropMedia(r.media.id, r.claim); } catch { /* 消せなくても応答は失敗にする */ }
        try { fs.unlinkSync(r.move.to); } catch { /* 無ければよい */ }
        if (promoted.blocked === 'notion_mode') return res.status(409).json(PREVIEW_WRITE_REJECTED);
        return res.status(409).json({ ok: false, error: promoted.blocked === 'closed_task' ? 'closed_task' : 'card_required',
          message: '保存の途中でこのカードは変えられなくなりました (もう一度とってください)' });
      } else if (promoted.media) {
        // 同じ送信が二重に届き、もう一方が先に公開した。写真としては成立しているのでそのまま返す。
        // 置いた実体 (自分の札のファイル) は誰も参照しないので片づける
        try { fs.unlinkSync(r.move.to); } catch { /* 無ければよい */ }
        r.media = promoted.media;
        r.already = true;
        // 削除トークンは勝った要求のものが有効。自分のものを返すと iPad が消せないトークンを持つ (Codex PR1 R9)
        delete r.deleteToken;
      } else {
        // 行が消えている (カードが変わった等)。置いた実体は誰も参照しないので片づける
        try { fs.unlinkSync(r.move.to); } catch { /* 無ければよい */ }
        return res.status(409).json({ ok: false, error: 'not_ready', message: '保存の途中でカードが変わりました (もう一度とってください)' });
      }
    } else {
      cleanup();   // 再送で既存の行を返した場合。今回の一時ファイルは使わないので片づける
    }
    if (out.appMode) {
      safeLogTaskEvent({ taskId: out.taskId, action: `media_${kind}`, workerId: w.worker.id, workerName: w.worker.display_name,
        deviceLabel: deviceLabelOf(req), to: r.already ? 'resend' : 'add', ok: true });
    } else {
      safeLog({ action: `media_${kind}`, pageId: cardId, workerId: w.worker.id, workerName: w.worker.display_name,
        deviceLabel: deviceLabelOf(req), to: r.already ? 'resend' : 'add', ok: true });
    }
    scheduleMedia();
    delete r.move;    // 保管場所のパスは画面に返さない
    delete r.stale;   // 同上
    delete r.claim;   // 札はサーバーの中だけで使う
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
      // Drive 側で消えた → 見本候補・表示から外す (毎回壊れた写真を選び続けない — Codex R1 #5)。
      // 印を付けるのはキューの回。配信 (GET) 自体は DB を変えない (Codex PR1 R9)
      reportMediaUnavailable(r.id, `Drive ${st}`, r.drive_file_id);
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
  // 読むだけの写真は消せない: Notion 正本の間の tasks の写真 (下見) と、終了したカードの写真 (履歴)。
  // 画面は × を描かないが、撮った端末は削除トークンを持ったままなので、サーバーでも断る (要件 v1.3 §P Q5)
  // ⭐判定と削除を 1 つのトランザクションに (判定の後・消す前に終了されると履歴の写真が消える — Codex PR1 R5)。
  //   判定は写真を足すときと同じ関門 = そのカードに**いま**書けるか (Notion のカードの写真も同じ — Codex PR1 R11)
  const out = getDB().transaction(() => {
    const row = getMediaRow(Number(req.params.id));
    if (row && !row.deleted_at) {
      const blocked = cardWriteBlockReason(row);
      if (blocked === 'notion_mode') return { deny: { status: 409, body: PREVIEW_WRITE_REJECTED } };
      if (blocked === 'closed_task') {
        return { deny: { status: 409, body: { ok: false, error: 'closed_task', message: '終了したカードの写真は消せません (履歴として残ります)' } } };
      }
      if (blocked) {
        return { deny: { status: 409, body: { ok: false, error: 'closed_task', message: 'このカードの写真はもう変えられません (履歴として残ります)' } } };
      }
    }
    return { r: softDeleteMedia(Number(req.params.id), {
      deleteToken: req.body?.delete_token || null,
      actor: req.iwUser || null,
      isSession: hasSessionAccess(req),
    }) };
  }).immediate();
  if (out.deny) return res.status(out.deny.status).json(out.deny.body);
  const r = out.r;
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 403)).json(r);
}));

/**
 * 送信を取り消す (画面の「やめる」)。⭐通信が切れただけで、サーバーでは成立していることがある。
 * その場合、端末は削除トークンを受け取れていないので消せない — この口で撮った端末だけが取り消せる
 * (本人確認 = 端末登録。worker_id は自己申告なので使わない — Codex PR1 R15)
 */
router.post('/api/media/cancel', checkOrigin, api((req, res) => {
  const opId = String(req.body?.operation_id || '').trim();
  if (!opId) return res.status(400).json({ ok: false, error: 'bad_request', message: 'operation_id が必要です' });
  const out = getDB().transaction(() => {
    const row = getDB().prepare('SELECT * FROM f_iroha_card_media WHERE operation_id = ?').get(opId);
    if (!row) {
      // まだ届いていないだけかもしれない (通信が切れている)。⭐取り消しを控えて、
      //   遅れて届いた元の送信を成立させない (Codex PR1 R18)
      recordMediaCancel(opId, { deviceId: req.iwDevice ? req.iwDevice.id : null, actor: req.iwUser || null });
      return { r: { ok: true, already: true } };
    }
    if (row.deleted_at) return { r: { ok: true, already: true } };
    // 撮った端末か、PCの管理画面 (ポータル) からだけ
    const mine = req.iwDevice && Number(row.uploader_device_id) === Number(req.iwDevice.id);
    if (!mine && !hasSessionAccess(req)) {
      return { deny: { status: 403, body: { ok: false, error: 'forbidden', message: '取り消せるのは撮影した端末からだけです' } } };
    }
    const blocked = cardWriteBlockReason(row);
    if (blocked === 'notion_mode') return { deny: { status: 409, body: PREVIEW_WRITE_REJECTED } };
    if (blocked) return { deny: { status: 409, body: { ok: false, error: 'closed_task', message: 'このカードの写真はもう変えられません' } } };
    recordMediaCancel(opId, { deviceId: req.iwDevice ? req.iwDevice.id : null, actor: req.iwUser || null });
    return { r: softDeleteMedia(row.id, { actor: req.iwUser || null, isSession: true }) };
  }).immediate();
  if (out.deny) return res.status(out.deny.status).json(out.deny.body);
  res.status(out.r.ok ? 200 : 409).json(out.r);
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
 * 「読むだけになったカードだから断った」ときのエラー (Codex PR1 R17)。
 * ⭐このときは**操作履歴も足さない** — 履歴もカードの中身なので、
 *   終了したカード・下見のカードは失敗イベントでも増やさない
 */
const READ_ONLY_REJECTS = new Set(['notion_mode', 'closed_task', 'card_required', 'done_card']);
const isReadOnlyReject = (r) => !!r && r.ok === false && READ_ONLY_REJECTS.has(r.error);

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
  if (isPreviewIdInNotionMode(pageId)) return res.status(409).json(PREVIEW_WRITE_REJECTED);
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
    // ⭐Notion の実ページを取りに行っている間に正本が切り替わる・カードが消えることがある。
    //   記録を入れる直前に (同じトランザクションの中で) もう一度確かめる (Codex PR1 R14)
    guard: () => {
      if (isAppMode()) return { ok: false, error: 'notion_mode', message: '正本が変わりました (一覧を更新してください)' };
      if (!getCachePage(pageId)) return { ok: false, error: 'card_required', message: 'カードが見つかりません (一覧を更新してください)' };
      return null;
    },
  });
  if (!r.already && !isReadOnlyReject(r)) {
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
    // 終了カード (履歴) は読むだけ。記録も足さない (Codex PR1 R3)。
    // 「終了したのに作業中」が残っていたら、管理画面の取り消し (voidSession) で片づける
    if (isClosedCardId(taskId)) return res.status(409).json(CLOSED_WRITE_REJECTED);
    const r = stopSession({ taskId, workerId: w.worker.id, sessionId: req.body?.session_id, reason,
      // ⭐記録を書く直前に、正本とカードをもう一度確かめる (見てから書くまでに切り替わる — Codex PR1 R16)
      guard: () => {
        if (!isAppMode()) return { ok: false, error: 'notion_mode', message: '正本が変わりました (一覧を更新してください)' };
        const t = getTask(taskId);
        if (!t) return { ok: false, error: 'card_required', message: 'カードが見つかりません (一覧を更新してください)' };
        if (t.status === 'closed') return CLOSED_WRITE_REJECTED;
        return null;
      },
    });
    if (!isReadOnlyReject(r)) {
      safeLogTaskEvent({ taskId, action: 'session_stop', workerId: w.worker.id, workerName: w.worker.display_name,
        deviceLabel: deviceLabelOf(req), to: reason, ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
    }
    if (!r.ok) return res.status(r.error === 'bad_request' ? 400 : 409).json(r);
    return res.json({ ...r, task: publicTask(getTask(taskId)) });
  }
  const pageId = String(req.body?.page_id || req.body?.id || '').trim();
  if (!pageId) return res.status(400).json({ ok: false, error: 'bad_request', message: 'page_id が必要です' });
  if (isPreviewIdInNotionMode(pageId)) return res.status(409).json(PREVIEW_WRITE_REJECTED);
  const r = stopSession({ pageId, workerId: w.worker.id, sessionId: req.body?.session_id, reason,
    // ⭐記録を書く直前に、正本とカードをもう一度確かめる (開始と同じ — Codex PR1 R15)
    guard: () => {
      if (isAppMode()) return { ok: false, error: 'notion_mode', message: '正本が変わりました (一覧を更新してください)' };
      if (!getCachePage(pageId)) return { ok: false, error: 'card_required', message: 'カードが見つかりません (一覧を更新してください)' };
      return null;
    },
  });
  if (!isReadOnlyReject(r)) {
    safeLog({ action: 'session_stop', pageId, workerId: w.worker.id, workerName: w.worker.display_name,
      deviceLabel: deviceLabelOf(req), to: reason, ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
  }
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
    builtinImages: BUILTIN_OPTION_IMAGES,
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
    return { counts: countTasksByStatus(), review: listTasksNeedingReview().length, orphans: listOrphans(20), linkConflicts: listLinkConflicts(20), linkConflictsTotal: countLinkConflicts(),
      nameless: listNamelessTasks(30), files: listMigrationFiles(),
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

/** 紐付け衝突の統合 (確定側 task_id と、残す側 keep=import|inbound)。1 tx で行き先・ページ・記録を残す側へ */
router.post('/admin/migration/link-conflicts/merge', checkOrigin, requireAdmin, api((req, res) => {
  const taskId = parseTaskId(req.body?.task_id);
  if (taskId == null) return res.status(400).json(BAD_TASK_ID);
  const r = mergeLinkConflict({ taskId, keep: String(req.body?.keep || 'import'), actor: req.iwUser });
  if (!r.ok) return res.status(r.error === 'not_found' ? 404 : (r.error === 'keep_closed' || r.error === 'from_active') ? 409 : 400).json(r);
  safeLog({ action: 'link_conflict_merge', pageId: null, deviceLabel: `session:${req.iwUser}`, from: `task#${r.closed}`, to: `task#${r.kept}`, ok: true });
  res.json({ ...r, remaining: countLinkConflicts() });
}));

/** 素性の分からないカードを片づける (管理者のみ。記録が無ければ消す・あれば「在庫化対象外」で終了) */
router.post('/admin/tasks/remove', checkOrigin, requireAdmin, api((req, res) => {
  const taskId = parseTaskId(req.body?.task_id);
  if (taskId == null) return res.status(400).json(BAD_TASK_ID);
  if (req.body?.confirm !== 'REMOVE') return res.status(400).json({ ok: false, error: 'confirm_required', message: '確認のため confirm に REMOVE と入れてください' });
  const r = removeStrayTask({ taskId, actor: req.iwUser, reason: req.body?.reason ? String(req.body.reason).slice(0, 200) : null });
  if (!r.ok) return res.status(r.error === 'not_found' ? 404 : (r.error === 'not_stray' || r.error === 'active_sessions') ? 409 : 400).json(r);
  safeLog({ action: 'task_remove', pageId: null, deviceLabel: `session:${req.iwUser}`, to: `task#${r.id} → ${r.action}`, ok: true });
  res.json({ ...r, remaining: listNamelessTasks(30).length });
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
  if (to === 'app' && notionSweepRunning()) {
    // 17:30 の一括送信の途中で切り替えると、送信側は打ち切るが「どこまで送ったか」が読みにくい。終わってから切り替える (Codex PR-B R1 #3)
    return res.status(409).json({ ok: false, error: 'sweep_running', message: '入荷受付の Notion 送信が動いています。終わってから (数分後に) もう一度お試しください' });
  }
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

/** 表示順の入れ替え (up/down/top/bottom)。auto = 手で決めた順をやめて「よく使う順」に戻す */
router.post('/admin/options/:id(\\d+)/sort', checkOrigin, requireAdmin, api((req, res) => {
  const r = moveWorkOption(Number(req.params.id), String(req.body?.dir || ''));
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 400)).json(r);
}));

export default router;
