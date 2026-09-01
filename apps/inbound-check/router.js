/**
 * 入荷受付チェック (iPad) — router (Render 完結)
 *
 * URL: /apps/inbound-check/            … iPad 作業画面 (端末Cookie or ポータルセッション)
 *      /apps/inbound-check/api/*       … 作業 API (state / check / uncheck / workers)
 *      /apps/inbound-check/admin       … 管理画面 (ポータルセッション必須。取込=アプリ利用者、端末登録/作業者/履歴=管理者)
 *
 * アクセス制御 (picking / packing と同じ2系統):
 *   ①ポータルセッション (allowedApps に inbound-check または '*')
 *   ②登録済み端末Cookie (ic_device) — 作業画面・作業APIのみ。作業者は名前タップで選択
 *   ⚠ server.js 側では requireAppAccess を掛けずに mount する (端末Cookieを通すため)。
 *      その分、ここで全ルートを守る (manifest.json だけ素通し)
 */
import { Router } from 'express';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  getState, applyCheck, importCsv, getActiveBatch, listBatches, listImportLog, listEvents, eventsCsv,
  createDevice, verifyDevice, revokeDevice, listDevices,
  listWorkers, getWorker,
} from './db.js';
import { fetchAndImportFromDrive, statusForView, driveConfig } from './drive-fetch.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();
export const APP_ID = 'inbound-check';
const BASE = '/apps/inbound-check';
const DEVICE_COOKIE = 'ic_device';

const UPLOAD_DIR = process.env.DATA_DIR ? path.join(process.env.DATA_DIR, 'import') : 'data/import';
if (!fs.existsSync(UPLOAD_DIR)) { try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch { /* 起動時に作れなければ upload 時に失敗として見える */ } }
const upload = multer({ dest: UPLOAD_DIR, limits: { fileSize: 8 * 1024 * 1024 } });

// ─── 共通ヘルパ ───
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

/**
 * 変更系の CSRF 防御: Origin (無ければ Referer) のホストがリクエスト先と一致しなければ拒否。
 * どちらも無いリクエストも拒否する (Origin 任せにしない — Codex R3 High)。
 * ブラウザの fetch/XHR は POST に必ず Origin を付けるので、正規の画面操作はここで落ちない
 */
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
  if (req.path === '/manifest.json') return next();
  if (hasSessionAccess(req)) { req.icUser = req.session.email; return next(); }
  const device = verifyDevice(readCookie(req, DEVICE_COOKIE));
  if (device) { req.icDevice = device; return next(); }
  if (req.path.startsWith('/api/')) return res.status(401).json({ ok: false, error: 'unauthorized', message: 'ログインまたは端末登録が必要です' });
  if (req.session) req.session.returnTo = req.originalUrl;
  return res.redirect('/login');
}

/** セッション必須 (端末Cookieでは不可)。取込・管理画面用 */
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
    console.error(`[inbound-check] ${req.method} ${req.path}`, e);
    res.status(500).json({ ok: false, error: 'internal', message: e.message });
  }
};

router.use(access);

// ─── PWA manifest (ホーム画面追加用) ───
router.get('/manifest.json', (req, res) => {
  res.json({
    name: '入荷受付チェック', short_name: '入荷チェック', start_url: `${BASE}/`, scope: `${BASE}/`,
    display: 'standalone', orientation: 'any', background_color: '#f8f9fa', theme_color: '#1c7ed6',
    icons: [{ src: '/favicon.png', sizes: '192x192', type: 'image/png' }],
  });
});

// ─── 作業画面 ───
router.get('/', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ─── 作業 API ───
router.get('/api/state', api((req, res) => {
  const state = getState();
  res.json({
    ok: true,
    ...state,
    workers: listWorkers(false),
    me: { session: req.icUser || null, device: req.icDevice ? { id: req.icDevice.id, label: req.icDevice.label } : null, admin: isAdmin(req) },
  });
}));

function resolveWorker(req) {
  const code = String(req.body?.worker_code || '').trim();
  if (code) {
    const w = getWorker(code);   // code = スタッフ管理番号 (apps/staff)
    if (!w || !w.active) return { error: '作業者が見つかりません (スタッフマスタで有効なスタッフを選んでください)' };
    return { worker: w.name, staffId: w.staff_id };
  }
  if (req.icUser) return { worker: req.session.displayName || req.icUser, staffId: null };
  return { error: '作業者を選んでください' };
}

function handleCheck(action) {
  return api((req, res) => {
    const { batch_id, line_key, expect_version } = req.body || {};
    // expect_version は必須の正整数 (不在・NaN・小数・0以下 → 400)。文字列の "2" は許容 (JSON の型揺れ)
    const ev = typeof expect_version === 'number' ? expect_version
      : (typeof expect_version === 'string' && /^\d+$/.test(expect_version) ? Number(expect_version) : NaN);
    if (!Number.isSafeInteger(ev) || ev < 1) return res.status(400).json({ ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' });
    const w = resolveWorker(req);
    if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
    const r = applyCheck({
      batchId: batch_id, lineKey: String(line_key || ''), action,
      expectVersion: ev,
      worker: w.worker,
      staffId: w.staffId,
      deviceId: req.icDevice ? req.icDevice.id : null,
      deviceLabel: req.icDevice ? req.icDevice.label : (req.icUser ? `session:${req.icUser}` : null),
    });
    if (!r.ok) {
      const status = r.error === 'stale_batch' || r.error === 'conflict' ? 409 : r.error === 'not_found' ? 404 : 400;
      return res.status(status).json(r);
    }
    res.json(r);
  });
}
router.post('/api/lines/check', checkOrigin, handleCheck('check'));
router.post('/api/lines/uncheck', checkOrigin, handleCheck('uncheck'));

// ─── 管理画面 ───
router.get('/admin', requireSession, api(async (req, res) => {
  let drive = null;
  try { drive = await statusForView(); } catch (e) { drive = { driveError: e.message, config: driveConfig() }; }
  res.render(path.join(__dirname, 'views/admin'), {
    title: '入荷受付チェック 管理',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: isAdmin(req),
    base: BASE,
    active: getActiveBatch(),
    batches: listBatches(30),
    importLog: listImportLog(20),
    devices: isAdmin(req) ? listDevices() : [],
    workers: listWorkers(),   // = スタッフマスタの有効スタッフ (表示のみ。編集は /apps/staff)
    drive,
  });
}));

// CSV 取込 (アプリ利用者なら誰でも = 取込復旧は中原さん + 事務担当)。
// file_modified = ブラウザ File.lastModified (ms)。CSV 生成時刻として順序逆転の判定に使う
router.post('/admin/upload', requireSession, checkOrigin, upload.single('file'), api((req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: 'no_file', message: 'ファイルがありません' });
  let buf;
  try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch { /* 一時ファイルの掃除失敗は無視 */ } }
  // ブラウザの File.lastModified は利用者が任意に設定できる。未来時刻を許すと以後の自動取込が
  // 「古いファイル」として全部拒否されるので、未来なら受信時刻に丸める (Codex R6 Med-5)
  const lm = Number(req.body?.file_modified);
  const now = Date.now();
  const generatedAt = Number.isFinite(lm) && lm > 0 ? new Date(Math.min(lm, now)).toISOString() : null;
  const r = importCsv(buf, { fileName: req.file.originalname, source: 'manual_upload', actor: req.session.email, generatedAt });
  if (!r.ok) return res.status(r.error === 'bad_csv' ? 400 : 409).json(r);
  res.json(r);
}));

/**
 * Drive から今すぐ取り込む (miniPC が置いた最新CSV)。通常は cron が 30 分おきに自動で行う。
 * 手動アップロードと同じ取込ロジックを通るので、fail-closed の判定もそのまま効く。
 */
router.post('/admin/fetch-drive', requireSession, checkOrigin, api(async (req, res) => {
  try {
    const r2 = await fetchAndImportFromDrive({ actor: req.session.email, source: 'drive_retry' });
    if (!r2.ok) return res.status(r2.error === 'bad_csv' ? 400 : 409).json(r2);
    res.json(r2);
  } catch (e) {
    res.status(502).json({ ok: false, error: 'drive_error', message: e.message });
  }
}));

// 端末登録: 発行したトークンは httpOnly Cookie としてこの端末にだけ渡す。登録と同時に管理者セッションを破棄
router.post('/admin/devices', checkOrigin, requireAdmin, api((req, res) => {
  const label = String(req.body?.label || '').trim();
  if (!label || label.length > 40) return res.status(400).json({ ok: false, error: 'bad_label', message: '端末名を1〜40文字で入力してください' });
  const { token, id: deviceId } = createDevice(label, req.session.email);
  req.session.destroy((err) => {
    if (err) {
      console.error('[inbound-check] 端末登録: セッション破棄に失敗', err);
      // 破棄に失敗したのに端末資格情報だけ DB に残さない (Codex R3 Medium): 作った端末を即時失効
      try { revokeDevice(deviceId); } catch (e2) { console.error('[inbound-check] 端末登録: 失効にも失敗', e2); }
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

// この端末の登録を解除: サーバー側のトークンも失効させてから Cookie を消す (Cookie だけ消すと
// トークンは有効なまま残る — Codex R3 Low)。端末側から自分で外せるように
router.post('/device/exit', checkOrigin, api((req, res) => {
  if (req.icDevice) revokeDevice(req.icDevice.id);
  res.clearCookie(DEVICE_COOKIE, { path: BASE });
  res.json({ ok: true, revoked: !!req.icDevice });
}));

// 作業者の追加・無効化は /apps/staff (スタッフマスタ) で行う。ここには経路を持たない

router.get('/admin/history', requireAdmin, api((req, res) => {
  const id = Number(req.query.batch_id);
  if (!Number.isInteger(id)) return res.status(400).json({ ok: false, error: 'bad_request', message: 'batch_id が必要です' });
  res.json({ ok: true, events: listEvents(id) });
}));

router.get('/admin/history.csv', requireAdmin, api((req, res) => {
  const id = Number(req.query.batch_id);
  if (!Number.isInteger(id)) return res.status(400).json({ ok: false, error: 'bad_request', message: 'batch_id が必要です' });
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="inbound-check-batch-${id}.csv"`);
  res.send(eventsCsv(id));
}));

export default router;
