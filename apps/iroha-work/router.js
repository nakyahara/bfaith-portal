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
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  getDB,
  createDevice, verifyDevice, revokeDevice, listDevices,
  createEnrollCode, redeemEnrollCode, countEnrollAttempt, listActiveEnrollCodes, ENROLL_TTL_MS,
  checkEnrollRate, recordEnrollAttempt,
  listIrohaWorkers, getIrohaWorker, addIrohaWorker, setIrohaWorkerActive,
  setWorkerPin, verifyWorkerPin,
  logEvent, listEvents,
  getCachePage, startSession, stopSession, listSessionsForAdmin, voidSession,
} from './db.js';
import { ensureFresh, changeStatus, fetchCardLive, cacheStatsForAdmin, STATUSES } from './notion-read.js';
import { buildList, classifyMasterEdit, clearEnrichCache, masterOf } from './service.js';
import { updateWorkMasterRow, addWorkMasterRow, codeKeyOf } from '../inbound-check/work-master.js';
import {
  addMedia, softDeleteMedia, countActivePhotos, resetMedia, listMediaForAdmin, schedule as scheduleMedia,
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
  if (req.path === '/manifest.json') return next();
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

const api = fn => async (req, res) => {
  try { await fn(req, res); } catch (e) {
    console.error(`[iroha-work] ${req.method} ${req.path}`, e);
    res.status(500).json({ ok: false, error: 'internal', message: e.message });
  }
};

router.use(access);

// ─── PWA manifest ───
router.get('/manifest.json', (req, res) => {
  res.json({
    name: 'いろは在庫化', short_name: 'いろは在庫化', start_url: `${BASE}/`, scope: `${BASE}/`,
    display: 'standalone', orientation: 'any', background_color: '#F4F6F1', theme_color: '#3E8E5A',
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
 * 一覧。キャッシュが古ければ Notion から取り直してから返す。
 * 取り直しに失敗しても古いキャッシュで返す (refresh.error に理由 — 現場を止めない)
 */
router.get('/api/state', api(async (req, res) => {
  const refresh = await ensureFresh();
  const list = buildList();
  res.json({
    ok: true,
    ...list,
    workers: listIrohaWorkers(),
    refresh,
    // タイマー表示の基準 (iPad の時計を信じない — 画面はこの値とのオフセットで経過を出す)
    serverNow: new Date().toISOString(),
    me: { session: req.iwUser || null, device: req.iwDevice ? { id: req.iwDevice.id, label: req.iwDevice.label } : null, admin: isAdmin(req) },
  });
}));

// 手動の「いま更新」。連打で Notion を叩かないよう 15 秒のクールダウン
let refreshLastAt = 0;
router.post('/api/refresh', checkOrigin, api(async (req, res) => {
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
  const pageId = String(req.body?.page_id || '').trim();
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

  // 棚入完了には できあがり写真1枚以上 (要件 §6)。スキップは**本人確認済みの職員のみ**
  // (skip_photo。changeStatus 側の staff_required に頼らず API 入口でも強制 — Codex PR3-R4)
  let skipPhotoUsed = false;
  if (to === '棚入完了' && countActivePhotos(pageId) === 0) {
    if (!req.body?.skip_photo) {
      return res.status(409).json({ ok: false, error: 'photo_required',
        message: 'できあがりの写真がまだありません。写真を1枚以上とってから棚入完了にしてください' });
    }
    if (!isStaff) {
      return res.status(403).json({ ok: false, error: 'staff_required',
        message: '写真なしでの完了は職員のみです (職員の名前を選び、PINを入れてください)' });
    }
    skipPhotoUsed = true;
  }

  const r = await changeStatus({ pageId, to, expect, isStaff });
  // スキップの履歴は**結果が出てから**残す (変更が失敗したのに「写真なしで完了した」と
  // 記録しない — Codex PR3 #5)
  if (skipPhotoUsed) {
    safeLog({ action: 'status_skip_photo', pageId, workerId: w.worker.id, workerName: w.worker.display_name,
      deviceLabel: deviceLabelOf(req), to, ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
  }
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

// ─── 作業仕様のその場登録・修正 (f_iroha_work_master。中原さんFB③⑥) ───

const MASTER_FIELDS = ['material_code', 'storage_container', 'units_per_container', 'process_count', 'note', 'video_url'];

/**
 * 権限 (要件 §7 と FB③の折衷):
 *   空欄を埋める = 作業者なら誰でも (新商品で現場が止まらない。履歴に残る)
 *   入っている値の変更・削除 = 職員のみ (端末はPIN・ポータルセッションはそのまま)
 * 版管理 (§1.7 ④): expect_version の楽観ロック。行が無ければ作ってから書く (mirror に居る商品のみ)
 */
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

  const { fills, overwrites } = classifyMasterEdit(row, fields);
  if (fills.length === 0 && overwrites.length === 0) {
    return res.json({ ok: true, unchanged: true, row });
  }
  if (overwrites.length > 0 && !hasSessionAccess(req)) {
    if (w.worker.worker_type !== 'staff') {
      return res.status(403).json({ ok: false, error: 'staff_required',
        message: '入っている値の変更は職員のみです (空欄への登録は誰でもできます)' });
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
        // ⚠シード元カードの商品コードが今回の商品と一致するときだけ (別商品のカード値を混ぜない)
        const card = getCachePage(String(req.body?.page_id || ''));
        if (card && codeKeyOf(card.product_code) === k) {
          let props = {};
          try { props = JSON.parse(card.payload || '{}'); } catch { /* 壊れていればシードなし */ }
          const seed = {
            material_code: props['資材セットID'], storage_container: props['収納容器'],
            units_per_container: props['入数'], process_count: props['工程数'], note: props['備考'],
          };
          applyFields = { ...Object.fromEntries(Object.entries(seed).filter(([f, v]) => v != null && v !== '' && !(f in fields))), ...fields };
        }
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
    const pageId = String(req.body?.page_id || '').trim();
    const kind = String(req.body?.kind || '');
    if (!pageId || (kind !== 'photo' && kind !== 'video')) {
      cleanup(); return res.status(400).json({ ok: false, error: 'bad_request', message: 'page_id と kind (photo/video) が必要です' });
    }
    const card = getCachePage(pageId);
    if (!card) { cleanup(); return res.status(404).json({ ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' }); }
    const r = addMedia({
      pageId, productCode: card.product_code, kind, mime: req.file.mimetype,
      filePath: req.file.path, worker: w.worker, deviceLabel: deviceLabelOf(req),
      deviceId: req.iwDevice ? req.iwDevice.id : null,
      operationId: req.body?.operation_id,
    });
    if (!r.ok) { cleanup(); return res.status(r.error === 'cap_reached' ? 409 : 400).json(r); }
    safeLog({ action: `media_${kind}`, pageId, workerId: w.worker.id, workerName: w.worker.display_name,
      deviceLabel: deviceLabelOf(req), to: r.already ? 'resend' : 'add', ok: true });
    scheduleMedia();
    res.json(r);
  } catch (e) {
    cleanup();
    throw e;
  }
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

// 失敗した送信の再実行 (管理画面)
router.post('/admin/media/:id(\\d+)/retry', checkOrigin, requireAdmin, api((req, res) => {
  if (!resetMedia(Number(req.params.id))) return res.status(404).json({ ok: false, error: 'not_found', message: '対象が見つかりません' });
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
  const pageId = String(req.body?.page_id || '').trim();
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

/** 作業終了 (done) / 中断 (pause)。時間はサーバー時刻の差分で確定 */
router.post('/api/sessions/stop', checkOrigin, api((req, res) => {
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const pageId = String(req.body?.page_id || '').trim();
  const reason = String(req.body?.reason || '');
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

export default router;
