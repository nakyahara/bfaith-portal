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
  getState, importCsv, getActiveBatch, listBatches, listImportLog, listEvents, eventsCsv,
  applyQuantityEvents, listQuantityEvents, finalizeLine, reopenLine,
  createDevice, verifyDevice, revokeDevice, listDevices, setAgentPrinter,
  resolveDestination, infoForLine, setExpiryManaged, setPendingExpiry, pendingExpiryFor,
  listDestinations, destinationsCsv, IROHA_YES, IROHA_NO,
  listCompletedSlips, completedSlipsCsv,
  createEnrollCode, redeemEnrollCode, countEnrollAttempt, listActiveEnrollCodes, ENROLL_TTL_MS,
  checkEnrollRate, recordEnrollAttempt,
  listWorkers, getWorker,
} from './db.js';
import { fetchAndImportFromDrive, statusForView, driveConfig, fetchAndImportProductMaster } from './drive-fetch.js';
// 🚚 予定外の納品を今すぐ iPad に出す (miniPC にロジザードから CSV を出し直させて取り込む)
import { startRefresh, refreshState, refreshConfigured } from './logizard-refresh.js';
import { runNotionSweep, notionStatusForAdmin, resetNotionRow } from './notion-sync.js';
// いろはの作業指示の正本 (app | notion)。2026-09-05 に Notion は運用廃止 → 通常は 'app'。
// 'notion' は在庫化アプリ /admin/source で戻したときの退路で、そのときだけ Notion 送信の入口を開ける
import { sourceOfTruth as irohaSourceOfTruth } from '../iroha-work/db.js';
import {
  parseWorkMasterXlsx, applyWorkMaster, logWorkMasterImport,
  workMasterStats, searchWorkMaster, updateWorkMasterRow, addWorkMasterRow, importIssueCount, computeDeletions,
} from './work-master.js';
// 入庫情報の書き込みは inbound-info の関数を通す (いろは=有り の連動ルール・楽観ロック・
// updated_by の記録がそこに1つだけある。ここで直に UPDATE すると規則が二重管理になる)
import { updateInbound, getInbound, addManual } from '../inbound-info/db.js';
import { queueEnsureImages } from '../picking/images.js';
// 🏷 値札 (BCシール) 印刷キュー: iPad が積み、倉庫PCの印刷エージェントが /print/* を pull で取りに来る
import {
  enqueuePrintJob, leaseNextJob, markSubmitted, markFinished, getJobStatusFor, recordHeartbeat,
  latestJobsForBatch, listPrintAgents, listPrintJobs, publicJob, PRINT_STATE_LABELS, LEASE_SEC, MAX_COPIES,
} from './print-queue.js';

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
  // 端末登録の画面と API はログイン不要 (登録コード自体が認証。共用 iPad に管理者パスワードを打たせない)
  // 手順書も認証なし: 登録がまだ済んでいない iPad からこそ読まれるページのため (中身は手順だけ)
  if (req.path === '/enroll' || req.path === '/enroll/redeem' || req.path === '/guide') return next();
  // 🏷 印刷エージェント (倉庫PC) は Cookie ではなく Authorization ヘッダーで名乗る。
  //   /print/ 配下はここでは素通しし、router.use('/print', requirePrintAgent) が kind='agent' の端末だけを通す
  //   (iPad の端末Cookieでは絶対に印刷ジョブを取れない)。ルートを列挙しないのは、後から /print/... を
  //   足したときに「正規のエージェントが access に弾かれる」ズレを作らないため
  if (req.path === '/print' || req.path.startsWith('/print/')) return next();
  if (hasSessionAccess(req)) { req.icUser = req.session.email; return next(); }
  const device = verifyDevice(readCookie(req, DEVICE_COOKIE));
  // エージェントのトークンを Cookie に入れても作業画面には入れない (端末の種別ごとに入口を分ける)
  if (device && device.kind !== 'agent') { req.icDevice = device; return next(); }
  if (req.path.startsWith('/api/')) return res.status(401).json({ ok: false, error: 'unauthorized', message: 'ログインまたは端末登録が必要です' });
  // ⭐iPad (未登録) は /login ではなく端末登録画面へ送る。
  //   ホーム画面の PWA と Safari は Cookie 保存領域が別なので、PWA 側で登録を完結させる必要がある
  if (req.session) req.session.returnTo = req.originalUrl;
  return res.redirect(`${BASE}/enroll`);
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

// ─── 🏷 値札印刷キュー: 倉庫PCの印刷エージェントが pull で取りに来る ───
// 契約 = AI_reference『システム設計\_tools\倉庫PC_値札印刷エージェント\README.md』「サーバー側に実装するもの」
// (agent.ps1 が前提にしている API 形状。ここを変えるときはエージェント側も直す)
/** エージェント認証。**Authorization ヘッダーのみ**で、iPad の端末Cookieは受け付けない */
function requirePrintAgent(req, res, next) {
  const m = /^Bearer\s+(\S+)$/.exec(req.headers.authorization || '');
  const device = m ? verifyDevice(m[1]) : null;
  if (!device || device.kind !== 'agent') {
    return res.status(401).json({ ok: false, error: 'unauthorized', message: '印刷エージェントとして認証されていません' });
  }
  req.printAgent = device;
  next();
}
// /print/ 配下は**この1行で**エージェント認証を必須にする (個々のルートには付けない — 二重に verifyDevice しない。Codex R1 Low)
router.use('/print', requirePrintAgent);

/** 次に刷るものを1件 lease して返す。無ければ 204 (エージェントは数秒後にまた聞きに来る)。 */
router.get('/print/next', api((req, res) => {
  // 出力先プリンターが登録されていない端末には**lease する前に**断る (掴んでから断ると unknown に落ちる)
  if (!req.printAgent.printer_name) {
    return res.status(409).json({ ok: false, error: 'no_printer', message: 'この端末に出力先プリンターが登録されていません (管理画面で登録してください)' });
  }
  const job = leaseNextJob(req.printAgent);
  if (!job) return res.status(204).end();
  res.json({ ok: true, job });
}));

/** エージェントが再起動したとき、掴んでいたジョブがどうなったか確かめる照会 (自分の lease 分のみ) */
router.get('/print/:id(\\d+)/status', api((req, res) => {
  const row = getJobStatusFor(Number(req.params.id), req.printAgent.id);
  if (!row) return res.status(404).json({ ok: false, error: 'not_found', message: 'このジョブを保持していません' });
  res.json({ ok: true, job: row });
}));

router.post('/print/:id(\\d+)/submitted', api((req, res) => {
  const r = markSubmitted(Number(req.params.id), {
    deviceId: req.printAgent.id, leaseToken: String(req.body?.lease || ''),
    spoolJobId: req.body?.spool_job_id ?? null,
  });
  if (!r.ok) return res.status(409).json({ ok: false, error: r.reason === 'submission_conflict' ? 'submission_conflict' : 'not_leased', message: r.message || `報告を受け付けられません (${r.reason})` });
  // replayed = 応答が届かなかった前回と同じ報告。エージェントが「もう投入済み」と分かる
  res.json({ ok: true, replayed: !!r.replayed });
}));

router.post('/print/:id(\\d+)/completed', api((req, res) => {
  // 「刷れた/刷れなかった」は真偽値でしか受け取らない (未指定・"false" を成功扱いにしない)
  if (typeof req.body?.ok !== 'boolean') return res.status(400).json({ ok: false, error: 'bad_ok', message: 'ok は true / false で送ってください' });
  const r = markFinished(Number(req.params.id), {
    deviceId: req.printAgent.id, leaseToken: String(req.body.lease || ''),
    ok: req.body.ok, error: req.body.error ?? null,
    // uncertain = 「刷れなかった」と言い切れない (スプーラーに渡した後に落ちた等) → unknown (実物を確認)
    uncertain: req.body.uncertain === true,
  });
  if (!r.ok) return res.status(409).json({ ok: false, error: 'not_leased', message: `報告を受け付けられません (${r.reason})` });
  res.json({ ok: true, replayed: !!r.replayed, state: r.state });
}));

router.post('/print/heartbeat', api((req, res) => {
  const b = req.body || {};
  recordHeartbeat(req.printAgent.id, {
    note: b.note ?? null, version: b.version ?? null, bpac: b.bpac, host: b.host ?? null,
    paperFormat: b.paperFormat ?? null, paperFormatOk: b.paperFormatOk, printerReports: b.printerReports ?? null,
  });
  res.json({ ok: true, lease_sec: LEASE_SEC });
}));

// ─── PWA manifest (ホーム画面追加用) ───
router.get('/manifest.json', (req, res) => {
  res.json({
    name: '入荷受付チェック', short_name: '入荷チェック', start_url: `${BASE}/`, scope: `${BASE}/`,
    display: 'standalone', orientation: 'any', background_color: '#f8f9fa', theme_color: '#1c7ed6',
    icons: [
      { src: '/app-icons/inbound-check-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/app-icons/inbound-check-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
    ],
  });
});

// ─── 端末登録 (ログイン不要。登録コードで認証する) ───
// 管理者が PC で発行した6桁コードを iPad で入力すると、その端末に Cookie が入る。
// ⭐これがあるおかげで、共用 iPad に管理者アカウントでログインさせずに済む
router.get('/enroll', (req, res) => {
  // 既に登録済みなら作業画面へ戻す (誤って開いたとき用)
  if (verifyDevice(readCookie(req, DEVICE_COOKIE))) return res.redirect(`${BASE}/`);
  res.sendFile(path.join(__dirname, 'views', 'enroll.html'));
});

router.post('/enroll/redeem', checkOrigin, api((req, res) => {
  const code = String(req.body?.code || '').trim();
  // ⭐総当たり対策は「試行そのもの」を数える。コードが実在するかで数え方を変えると、
  //   000000〜999999 を順に叩かれたとき一度もカウントされない (6桁 = 100万通り)
  const ip = req.ip || null;
  const gate = checkEnrollRate({ ip });
  if (!gate.allowed) {
    recordEnrollAttempt({ ip, ok: false });
    return res.status(429).json({ ok: false, error: gate.error, message: gate.message });
  }
  const r = redeemEnrollCode(code);
  recordEnrollAttempt({ ip, ok: r.ok });
  if (!r.ok) {
    countEnrollAttempt(code);   // そのコード自体の打ち間違いも数える (正規利用者の打ち間違い上限)
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

// 発行 (管理者・PC から)
router.post('/admin/enroll-codes', checkOrigin, requireAdmin, api((req, res) => {
  try {
    const r = createEnrollCode(req.body?.label, req.session.email);
    res.json({ ok: true, ...r, ttlMinutes: Math.round(ENROLL_TTL_MS / 60000) });
  } catch (e) {
    res.status(400).json({ ok: false, error: 'bad_request', message: e.message });
  }
}));

// ─── 使い方 (手順書) ───
// 毎日の使い方 + 初回セットアップ。iPad の作業画面フッターと登録画面から飛べる
router.get('/guide', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'guide.html'));
});

// ─── 完了一覧 (棚入れ・確認用) ───
// 端末Cookie でもポータルセッションでも見られる。中原さん 2026-09-02:「PC からも見れるように」
// ⭐画像は出さない (棚入れのときに見たいのは 商品・数量・期限 だけ)
router.get('/done', (req, res) => {
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  if (pathname.endsWith('/')) return res.redirect(308, pathname.slice(0, -1) + (qIdx === -1 ? '' : req.originalUrl.slice(qIdx)));
  res.sendFile(path.join(__dirname, 'views', 'done.html'));
});

function doneQuery(req) {
  return {
    days: req.query?.days ? Number(req.query.days) : 14,
    arNo: req.query?.ar ? String(req.query.ar) : null,
    workDate: req.query?.date ? String(req.query.date) : null,
    includeIncomplete: String(req.query?.all || '') === '1',
  };
}

router.get('/api/done', api((req, res) => {
  res.json({ ok: true, slips: listCompletedSlips(doneQuery(req)) });
}));

// CSV は PC で開いて印刷・保管する用。端末からも落とせてよい (中身は画面と同じ)
router.get('/done.csv', api((req, res) => {
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Disposition', 'attachment; filename="inbound-done.csv"');
  res.send(completedSlipsCsv(doneQuery(req)));
}));

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
  // 🏷 明細ごとの最新の値札印刷ジョブ (結果は5秒ポーリングで行に出る) + 印刷できる倉庫PCがいるか
  if (state.batch) {
    const jobs = latestJobsForBatch(state.batch.id);
    for (const l of state.lines) l.print_job = jobs.get(l.line_key) || null;
  }
  const agents = listPrintAgents();
  res.json({
    ok: true,
    ...state,
    print_agents: agents.map(a => ({ id: a.id, label: a.label, printer_name: a.printer_name, online: a.online, bpac: a.bpac, paper_ok: a.paper_ok })),
    // 🚚 「いま取りに行く」の進み具合 (押した後の表示はこれを5秒ポーリングで見る)
    refresh: refreshState(),
    // いろはの作業指示の正本。'app' (通常) なら iPad の「🗂 Notionへ送る」は出さない (Notion は 2026-09-05 に廃止)
    iroha_source: irohaSourceOfTruth(),
    workers: listWorkers(false),
    me: { session: req.icUser || null, device: req.icDevice ? { id: req.icDevice.id, label: req.icDevice.label } : null, admin: isAdmin(req) },
  });
}));

// ─── 🏷 値札 (BCシール) を倉庫PCの QL-700 から出す (iPad の「🏷 シール発行」) ───
// 中原さん 2026-09-05:「このアプリからシールを出せるようにしたい。データはこのアプリ内の情報から」。
// 商品名・バーコード・商品IDは**画面の値ではなく active バッチの明細**から取る (改ざん・古い表示を刷らない)。
// 入数と枚数は確認ダイアログで人が決めた値。冪等ID (client_request_id) で二重タップ・応答消失の再送を1ジョブに畳む
router.post('/api/print/jobs', checkOrigin, api((req, res) => {
  const a = actorOf(req, res);
  if (!a) return;
  const b = req.body || {};
  const r = enqueuePrintJob({
    batchId: intOrNull(b.batch_id), lineKey: String(b.line_key || ''),
    copies: intOrNull(b.copies), packQty: b.pack_qty == null || b.pack_qty === '' ? null : b.pack_qty,
    targetDeviceId: b.target_device_id == null || b.target_device_id === '' ? null : intOrNull(b.target_device_id),
    clientRequestId: b.client_request_id,
    // 直前が「❓ 結果不明」のときの「実物を確認した」証跡 (そのジョブ ID)。無ければ confirm_unknown
    acknowledgeUnknownJobId: b.acknowledge_unknown_job_id == null ? null : intOrNull(b.acknowledge_unknown_job_id),
    requestedBy: a.worker, requestedDevice: a.deviceLabel,
  });
  if (!r.ok) {
    const status = ['in_progress', 'stale_batch', 'confirm_unknown', 'state_changed'].includes(r.error) ? 409 : r.error === 'not_found' ? 404 : 400;
    return res.status(status).json(r);
  }
  res.json(r);
}));

// 印刷できる倉庫PC (出力先) の一覧。2台以上あるときは iPad がここから選ぶ
router.get('/api/print/targets', api((req, res) => {
  res.json({ ok: true, max_copies: MAX_COPIES, agents: listPrintAgents() });
}));

// ─── 🚚 いま入荷を取りに行く (予定外の納品をロジザードに入れた直後に押す) ───
// 中原さん 2026-09-05:「予定してない納品が来たりした場合にこの iPad に入れたい」。
// 定時 (08:40 / 11:45) を待たず、miniPC にロジザードから CSV を出し直させて取り込む。
// ⭐**すぐ返す**。全体で30〜60秒かかるので、進み具合は /api/state の refresh を見てもらう
//   (iPad の送信は4秒で打ち切る作りなので、ここで待たせると必ず中断する)
router.post('/api/refresh-now', checkOrigin, api((req, res) => {
  let actor;
  if (req.icDevice) {
    // 誰が押したかを残す (現場の端末は共用なので作業者を必須にする — Notion送信と同じ規約)
    const w = resolveWorker(req);
    if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
    actor = `device:${req.icDevice.label}/${w.worker}`;
  } else {
    actor = req.session?.email || 'portal';
  }
  const r = startRefresh({ actor });
  const status = r.ok ? 200 : (r.error === 'cooldown' ? 429 : 503);
  res.status(status).json(r);
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

// ─── 確認するときに「いろはで在庫化 / B-Faith で入庫」を確定させる ───
// 中原さん 2026-09-01: 「確認をした時にいろはに在庫化作業を依頼するのか、ビーフェイスに
// 入庫するのかを判定してほしい。すでにデータがあるものはそのデータに沿って。もしデータが
// なければその時に選択して、その選択したデータがデータベースに登録される」
//
// B-Faith 入庫を選んだときは**ラベル (BCシール) と入数が揃うまで進ませない**。
// 揃っていなければ 400 destination_required を返し、画面が聞いてから送り直す。
const MISSING_LABEL = { iroha: '行き先', bc_seal: 'ラベル (BCシール)', irisu: '入数', expiry: '有効期限' };
// 「－」等は未記入と同じ扱い (db.js の blank と揃える)
const NA_VALUES = new Set(['', '－', '-', 'ー', '―']);
const isBlankValue = v => NA_VALUES.has(String(v == null ? '' : v).trim());

/**
 * 有効期限の受け取り。画面は 年/月/日 のプルダウンで選ぶので YYYY-MM-DD か YYYY-MM で届く。
 * 実在する暦日だけ通す (2026-02-31 のような日付を台帳に残さない)
 */
function parseExpiry(v) {
  const s = String(v == null ? '' : v).trim();
  const m = /^(\d{4})-(\d{2})(?:-(\d{2}))?$/.exec(s);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), m[3] ? Number(m[3]) : null];
  if (y < 2000 || y > 2100 || mo < 1 || mo > 12) return null;
  if (d == null) return `${m[1]}-${m[2]}`;
  const dt = new Date(Date.UTC(y, mo - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() + 1 !== mo || dt.getUTCDate() !== d) return null;
  return `${m[1]}-${m[2]}-${m[3]}`;
}

function decideDestination(req, line, worker, foundQty = null) {
  const { info, expiryManaged } = infoForLine(line.code_key);
  let dest = resolveDestination(info, { expiryManaged });
  // ⭐詳細パネルで先に入れた有効期限 (pending) があれば、確認時にはもう聞かない
  //   (中原さん 2026-09-02:「そこで入れてたら確認ボタンで出てこなくていい」)
  const pendingExp = expiryManaged ? parseExpiry(pendingExpiryFor(line.batch_id, line.line_key)) : null;
  const askMissing = pendingExp ? dest.missing.filter(m => m !== 'expiry') : dest.missing;
  if (askMissing.length === 0) {
    return { ok: true, destination: dest.destination, decidedFrom: 'master', expiryDate: pendingExp };
  }

  const choice = req.body?.choice;
  if (!choice || typeof choice !== 'object') {
    return { ok: false, status: 400, body: { ok: false, error: 'destination_required', missing: askMissing, info, expiry_managed: expiryManaged,
      line: { product_id: line.product_id, product_name: line.product_name, planned_qty: line.planned_qty, found_qty: foundQty },
      message: askMissing.map(m => MISSING_LABEL[m] || m).join(' と ') + ' を決めてください' } };
  }

  // 期限管理商品は入荷のたびに有効期限が変わるので毎回入れてもらう (先入力があればそれを使う)
  let expiryDate = null;
  if (expiryManaged) {
    expiryDate = parseExpiry(choice.expiry_date) || pendingExp;
    if (!expiryDate) {
      return { ok: false, status: 400, body: { ok: false, error: 'destination_required', missing: ['expiry'], info, expiry_managed: true,
        message: '有効期限を選んでください (実在する日付で)' } };
    }
  }
  const want = String(choice.destination || '').trim();
  if (dest.missing.includes('iroha') && want !== 'iroha' && want !== 'bfaith') {
    return { ok: false, status: 400, body: { ok: false, error: 'bad_request', message: '行き先 (いろは / B-Faith) を選んでください' } };
  }
  const destination = dest.missing.includes('iroha') ? want : dest.destination;
  const usedChoice = dest.missing.some(m => m !== 'expiry');

  // 書き戻す値を組み立てる。⭐未記入だったときだけ「いろは在庫化作業有無」を書く
  //   (「状況による」のように人が意図して入れた値は上書きしない — 今回の判断は台帳にだけ残す)
  const fields = {};
  if (dest.missing.includes('iroha') && dest.writeBack) fields.いろは在庫化作業有無 = destination === 'iroha' ? IROHA_YES : IROHA_NO;
  if (destination === 'bfaith') {
    // いろはに送らず自社で入庫するなら、ラベルと入数が無いと入庫作業ができない。
    // ⚠dest.missing ではなく info の値そのものを見る: 行き先が未記入だった場合、
    //   resolveDestination は iroha の分岐で先に返るので bc_seal/irisu は missing に入らない
    const needSeal = isBlankValue(info && info.bc_seal);
    const needIrisu = !info || !Number.isInteger(info.irisu) || info.irisu <= 0;
    if (needSeal) {
      const seal = String(choice.bc_seal == null ? '' : choice.bc_seal).trim();
      if (!seal) return { ok: false, status: 400, body: { ok: false, error: 'destination_required', missing: ['bc_seal'], info, message: 'ラベル (BCシール) を選んでください' } };
      fields.入庫時BCシール貼りフラグ = seal;
    }
    if (needIrisu) {
      const n = Number(choice.irisu);
      if (!Number.isSafeInteger(n) || n <= 0) return { ok: false, status: 400, body: { ok: false, error: 'destination_required', missing: ['irisu'], info, message: '入数を1以上の整数で入力してください' } };
      fields.入数 = n;
    }
  }

  // 選んだ値を入庫情報へ登録する (中原さん「選択したものは自動登録される」)
  let warning = null;
  if (Object.keys(fields).length > 0) {
    let cur = info;
    if (!cur) {
      const add = addManual(line.product_id, worker);
      if (add.ok || add.error === 'duplicate') cur = getInbound(line.product_id);
      // 商品マスタに無い商品は入庫情報を作れない。**現場を止めないため確認は通し**、
      // 行き先は台帳に残す (登録できなかったことは画面に出す)
      if (!cur) warning = 'この商品は商品マスタに無いため、入庫情報には登録できませんでした (行き先の記録は残ります)';
    }
    if (cur) {
      const up = updateInbound(line.code_key, fields, worker, cur.version);
      if (!up.ok) {
        if (up.error === 'conflict') return { ok: false, status: 409, body: { ok: false, error: 'conflict', message: '他の人が同じ商品の入庫情報を変更しました。もう一度押してください' } };
        return { ok: false, status: 400, body: { ok: false, error: up.error, message: '入庫情報を登録できませんでした' } };
      }
      // 書いたあとで、B-Faith 入庫の必須2項目が本当に揃ったか見直す。
      // ⚠ここで行き先 (iroha) は見ない: 「状況による」の行は master を書き換えないので、
      //   毎回 missing に残るのが正しく、それを不足として扱うと永久に確認できなくなる
      if (destination === 'bfaith') {
        const short = [];
        if (isBlankValue(up.row.入庫時BCシール貼りフラグ)) short.push('bc_seal');
        if (!Number.isInteger(up.row.入数) || up.row.入数 <= 0) short.push('irisu');
        if (short.length > 0) {
          return { ok: false, status: 400, body: { ok: false, error: 'destination_required', missing: short, info: up.row,
            message: short.map(m => MISSING_LABEL[m] || m).join(' と ') + ' が足りません' } };
        }
      }
    }
  }
  // 有効期限だけを聞いた場合、行き先そのものは入庫情報どおり (master) と記録する
  return { ok: true, destination, decidedFrom: usedChoice ? 'chosen' : 'master', expiryDate, warning };
}

// 数量・確定系のエラーはどれも「画面を最新にして出し直す」で回復する。HTTP の対応表
const STATUS_BY_ERROR = {
  stale_batch: 409, stale_work_date: 409, conflict: 409, finalized: 409, result_mismatch: 409,
  already_reversed: 409, idempotency_conflict: 409, negative_total: 409,
  not_found: 404, worker_required: 400, destination_required: 400, bad_request: 400,
};
const statusFor = r => (r && r._status) || STATUS_BY_ERROR[r && r.error] || 400;

function sendResult(res, r) {
  if (r.ok) return res.json(r);
  const status = statusFor(r);
  const body = { ...r };
  delete body._status;
  return res.status(status).json(body);
}

/** 端末・作業者・共通の前処理。エラーならレスポンス済みを示す null を返す */
function actorOf(req, res) {
  const w = resolveWorker(req);
  if (w.error) { res.status(400).json({ ok: false, error: 'worker_required', message: w.error }); return null; }
  return {
    worker: w.worker, staffId: w.staffId,
    deviceId: req.icDevice ? req.icDevice.id : null,
    deviceLabel: req.icDevice ? req.icDevice.label : (req.icUser ? `session:${req.icUser}` : null),
  };
}

const intOrNull = v => (typeof v === 'number' ? v : (typeof v === 'string' && /^\d+$/.test(v) ? Number(v) : NaN));

// ─── 数を足す / 打ち消す / 訂正する ───
// ⭐1タップ1イベント。**再送は同じ client_event_id** を使うので、コミット直後に応答だけ
//   失われて押し直しても二重加算しない (要件定義 v1.3 §11.5)
router.post('/api/lines/quantity-events', checkOrigin, api((req, res) => {
  const a = actorOf(req, res);
  if (!a) return;
  const { batch_id, line_key, expect_quantity_version, events, pack_qty } = req.body || {};
  sendResult(res, applyQuantityEvents({
    batchId: intOrNull(batch_id), lineKey: String(line_key || ''),
    expectQuantityVersion: intOrNull(expect_quantity_version),
    events, packQty: pack_qty == null ? null : intOrNull(pack_qty), ...a,
  }));
}));

// その行の数量イベント (訂正パネル用)
router.get('/api/lines/events', api((req, res) => {
  const id = Number(req.query.batch_id);
  if (!Number.isInteger(id)) return res.status(400).json({ ok: false, error: 'bad_request', message: 'batch_id が必要です' });
  res.json({ ok: true, events: listQuantityEvents(id, String(req.query.line_key || '')) });
}));

function handleCheck(action) {
  return api((req, res) => {
    const a = actorOf(req, res);
    if (!a) return;
    const { batch_id, line_key, expect_version, expect_quantity_version, client_operation_id } = req.body || {};
    const ev = intOrNull(expect_version);
    const qv = intOrNull(expect_quantity_version);
    if (!Number.isSafeInteger(ev) || ev < 1) return res.status(400).json({ ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' });

    if (action === 'uncheck') {
      return sendResult(res, reopenLine({
        batchId: intOrNull(batch_id), lineKey: String(line_key || ''), expectVersion: ev,
        expectQuantityVersion: Number.isSafeInteger(qv) ? qv : null,
        clientOperationId: client_operation_id || null, ...a,
      }));
    }

    if (!Number.isSafeInteger(qv) || qv < 1) return res.status(400).json({ ok: false, error: 'bad_request', message: 'expect_quantity_version (正の整数) が必要です' });
    // ⭐行き先の判定と入庫情報への書き戻しは finalizeLine の**トランザクション内**で行う。
    //   先に書いてから確認すると、確認が 409 で失敗したのにマスタだけ変わる (Codex 指摘の既存バグ)
    const decide = (line, foundQty) => {
      const d = decideDestination(req, line, editorName(req, a.worker), foundQty);
      if (!d.ok) return { ok: false, abort: { ...d.body, _status: d.status } };
      return d;
    };
    sendResult(res, finalizeLine({
      batchId: intOrNull(batch_id), lineKey: String(line_key || ''), expectVersion: ev, expectQuantityVersion: qv,
      result: String(req.body?.result || ''), mode: String(req.body?.mode || 'current'),
      fillEvent: req.body?.fill_event || null, clientOperationId: client_operation_id || null,
      decide, ...a,
    }));
  });
}
router.post('/api/lines/check', checkOrigin, handleCheck('check'));
router.post('/api/lines/uncheck', checkOrigin, handleCheck('uncheck'));


// ─── 入庫情報の編集 (iPad の詳細パネルから) ───
// 入数・いろは在庫化作業有無・BCシール・直ピック・荷姿・memo をその場で直せる。
// 書き込み先は f_inbound_info (= /apps/inbound-info と同じ正本) なので、値札印刷にもそのまま効く。
// ⚠誰が直したかを残すため、消し込みと同じく作業者の指定を必須にする
const EDITABLE_FIELDS = ['入数', '入庫時BCシール貼りフラグ', '直接ピックロケ保管', 'BF保管荷姿', 'いろは在庫化作業有無', 'memo'];

function editorName(req, worker) {
  const dev = req.icDevice ? req.icDevice.label : (req.icUser ? 'ポータル' : '');
  return dev ? `${worker} (${dev})` : worker;
}

router.post('/api/info', checkOrigin, api((req, res) => {
  const { code_key, fields, expect_version } = req.body || {};
  const key = String(code_key || '').trim();
  if (!key) return res.status(400).json({ ok: false, error: 'bad_request', message: '商品が指定されていません' });
  if (!fields || typeof fields !== 'object' || Array.isArray(fields)) {
    return res.status(400).json({ ok: false, error: 'bad_request', message: '変更内容がありません' });
  }
  // 送られてきたキーのうち、編集を許した項目だけを通す (商品名など他の列は iPad から触らせない)
  const picked = {};
  for (const k of EDITABLE_FIELDS) if (k in fields) picked[k] = fields[k];
  if (Object.keys(picked).length === 0) {
    return res.status(400).json({ ok: false, error: 'bad_request', message: '変更できる項目がありません' });
  }
  const ev = typeof expect_version === 'number' ? expect_version
    : (typeof expect_version === 'string' && /^\d+$/.test(expect_version) ? Number(expect_version) : NaN);
  if (!Number.isSafeInteger(ev) || ev < 1) return res.status(400).json({ ok: false, error: 'bad_request', message: 'expect_version (正の整数) が必要です' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });

  const r = updateInbound(key, picked, editorName(req, w.worker), ev);
  if (!r.ok) {
    const status = r.error === 'not_found' ? 404 : r.error === 'conflict' ? 409 : 400;
    const message = r.error === 'conflict' ? '他の人が先に変更しました。最新の内容を表示します'
      : r.error === 'not_found' ? 'この商品は入庫情報に登録されていません'
      : r.error === 'invalid_irisu' ? '入数は1以上の整数で入力してください'
      : '保存できませんでした';
    return res.status(status).json({ ok: false, error: r.error, message });
  }
  res.json({ ok: true, row: r.row });
}));

// 入庫情報がまだ無い商品を登録する (登録しないと入数もいろはも書けないため)
router.post('/api/info/register', checkOrigin, api((req, res) => {
  const code = String(req.body?.code || '').trim();
  if (!code) return res.status(400).json({ ok: false, error: 'bad_request', message: '商品IDがありません' });
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const r = addManual(code, editorName(req, w.worker));
  if (!r.ok) {
    const message = r.error === 'duplicate' ? '既に登録されています (画面を更新してください)'
      : r.error === 'not_in_master' ? 'この商品IDは商品マスタにありません (ネクストエンジン側を確認してください)'
      : '登録できませんでした';
    return res.status(r.error === 'duplicate' ? 409 : 400).json({ ok: false, error: r.error, message });
  }
  res.json({ ok: true, row: getInbound(code) });
}));

// ─── ロジザード商品マスタを今すぐ取り込む (期限管理あり/なしの正本) ───
// アプリ利用者なら誰でも。読み取り専用の取込で、失敗しても既存の設定は変わらない
router.post('/admin/fetch-product-master', requireSession, checkOrigin, api(async (req, res) => {
  try {
    res.json(await fetchAndImportProductMaster({ actor: req.session.email, force: true }));
  } catch (e) {
    res.status(400).json({ ok: false, error: e.code || 'drive_error', message: e.message });
  }
}));

// ─── 期限管理あり/なし の切り替え (詳細パネルから) ───
// ロジザードの商品マスタに設定がある項目だが入荷受付CSVには出てこないため、
// 在庫の有効期限から推定した値を、違っていれば現場が直せるようにする
router.post('/api/product-flags', checkOrigin, api((req, res) => {
  const codeKey = String(req.body?.code_key || '').trim();
  if (!codeKey) return res.status(400).json({ ok: false, error: 'bad_request', message: '商品が指定されていません' });
  if (typeof req.body?.expiry_managed !== 'boolean') {
    return res.status(400).json({ ok: false, error: 'bad_request', message: 'expiry_managed (true/false) が必要です' });
  }
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  res.json({ ok: true, ...setExpiryManaged(codeKey, req.body.expiry_managed, editorName(req, w.worker)) });
}));

// ─── 有効期限の先入力 (詳細パネルから。入れてあれば確認時にはもう聞かない) ───
router.post('/api/lines/pending-expiry', checkOrigin, api((req, res) => {
  const lineKey = String(req.body?.line_key || '').trim();
  const batchId = Number(req.body?.batch_id);
  if (!lineKey || !Number.isInteger(batchId)) {
    return res.status(400).json({ ok: false, error: 'bad_request', message: 'batch_id と line_key が必要です' });
  }
  const w = resolveWorker(req);
  if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
  const raw = req.body?.expiry_date;
  let expiryDate = null;
  if (raw != null && String(raw).trim() !== '') {
    expiryDate = parseExpiry(raw);
    if (!expiryDate) return res.status(400).json({ ok: false, error: 'bad_request', message: '有効期限は実在する日付で入れてください' });
  }
  const r = setPendingExpiry({ batchId, lineKey, expiryDate });
  res.status(r.ok ? 200 : (r.error === 'not_found' ? 404 : 409)).json(r);
}));

// ─── Notion へ今すぐ送る (iPad からも押せる) ───
// 🚫 2026-09-05 Notion「在庫化作業管理」は運用廃止。いろは行きの作業指示は「確認」と同じトランザクションで
//    在庫化アプリ (f_iroha_tasks) の未着手に入るので、送る操作そのものが無い。正本がアプリの間は 410 を返す。
//    (在庫化アプリ /admin/source で Notion に戻した退路のときだけ、従来どおり送れる)
// 中原さん 2026-09-02:「iPadにボタンがあれば便利」。sweep は冪等 (何回押しても二重カードにならない) で
// lease が多重実行も防ぐ。端末Cookie経由は**作業者必須** (誰が押したかを actor に残す) +
// 30秒のレート制限 (連打・端末Cookie漏えい時の外部API負荷を抑える — Codex #1116 Med-5)
const NOTION_RETIRED = {
  ok: false, error: 'notion_retired',
  message: 'Notion の在庫化作業管理は廃止されました。いろは行きの商品は「確認」した時点で在庫化アプリの「未着手」に入っています',
  app_url: '/apps/iroha-work/',
};
let notionSyncLastAt = 0;
router.post('/api/notion-sync', checkOrigin, api(async (req, res) => {
  if (irohaSourceOfTruth() === 'app') return res.status(410).json(NOTION_RETIRED);
  let actor;
  if (req.icDevice) {
    const w = resolveWorker(req);
    if (w.error) return res.status(400).json({ ok: false, error: 'worker_required', message: w.error });
    actor = `device:${req.icDevice.label}/${w.worker}`;
  } else {
    actor = req.session?.email || 'portal';
  }
  const now = Date.now();
  if (now - notionSyncLastAt < 30_000) {
    // ⚠「失敗」ではない。直前の実行は走っている (押し直しても二重カードにはならないが、
    //   外部APIの連打を避けるためのクールダウン)。失敗と誤読されない文言にする
    const wait = Math.ceil((30_000 - (now - notionSyncLastAt)) / 1000);
    return res.status(429).json({ ok: false, error: 'rate_limited',
      message: `さっき送ったばかりです (直前の送信は実行済み)。${wait}秒あけてもう一度押せます` });
  }
  notionSyncLastAt = now;
  const r = await runNotionSweep({ actor, mode: 'full' });
  res.status(r.ok ? 200 : (r.error === 'already_running' ? 409 : 502)).json(r);
}));

// ─── 行き先の台帳 (いろはへ送る商品の一覧) ───
// アプリ利用者なら誰でも見られる (いろはへの持ち出しリストは事務担当も使うため)
router.get('/admin/destinations', requireSession, api((req, res) => {
  res.json({ ok: true, rows: listDestinations(destQuery(req)) });
}));

router.get('/admin/destinations.csv', requireSession, api((req, res) => {
  const q = destQuery(req);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Disposition', `attachment; filename="inbound-destinations-${q.destination}.csv"`);
  res.send(destinationsCsv(q));
}));

function destQuery(req) {
  const d = String(req.query?.destination || 'iroha');
  return {
    destination: ['iroha', 'bfaith', 'all'].includes(d) ? d : 'iroha',
    from: req.query?.from ? String(req.query.from) : null,
    to: req.query?.to ? String(req.query.to) : null,
    includeCancelled: String(req.query?.include_cancelled || '') === '1',
  };
}

// ─── 管理画面 ───
router.get('/admin', requireSession, api(async (req, res) => {
  let drive = null;
  try { drive = await statusForView(); } catch (e) { drive = { driveError: e.message, config: driveConfig() }; }
  let notion = null;
  try { notion = notionStatusForAdmin(); } catch (e) { notion = { error: e.message }; }
  let workMaster = null;
  try { workMaster = workMasterStats(); } catch (e) { workMaster = { error: e.message, total: 0, filled: 0 }; }
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
    enrollCodes: isAdmin(req) ? listActiveEnrollCodes() : [],
    // 🏷 値札印刷エージェント (倉庫PC) と直近の印刷ジョブ
    printAgents: isAdmin(req) ? listPrintAgents() : [],
    printJobs: isAdmin(req) ? listPrintJobs(30) : [],
    PRINT_STATE_LABELS,
    workers: listWorkers(),   // = スタッフマスタの有効スタッフ (表示のみ。編集は /apps/staff)
    drive,
    notion,
    workMaster,
    // 🚚 「いま取りに行く」が使える環境か (miniPC を呼べる資格情報があるか)
    refreshAvailable: refreshConfigured(),
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

// ─── Notion 作業カード (いろは行き) を今すぐ送る ───
// 1日1回 (17:30 JST) の cron と同じ reconcile。夕方を待たずに送りたいとき・エラー後の再送用。
// 取込と同じく「アプリ利用者なら誰でも」(事務担当が押せるように)。
// retry_id を渡すと、その行のエラーブロック (4xx で止めた分) を解除してから実行する
router.post('/admin/notion-sync', requireSession, checkOrigin, api(async (req, res) => {
  if (irohaSourceOfTruth() === 'app') return res.status(410).json(NOTION_RETIRED);   // 2026-09-05 Notion 廃止
  const retryId = Number(req.body?.retry_id);
  if (Number.isInteger(retryId) && retryId > 0) resetNotionRow(retryId);
  // 「再送」(retry_id あり) はエラー行の再処理だけ (mode='retry')。正常な新規行まで
  // 17:30 を待たずに送ってしまわない (Codex R2 #5)。「今すぐ送る」ボタンだけが full
  const r = await runNotionSweep({ actor: req.session.email, mode: retryId ? 'retry' : 'full' });
  res.status(r.ok ? 200 : (r.error === 'already_running' ? 409 : 502)).json(r);
}));

// ─── いろは作業仕様マスタ (旧「作業内容管理マスター」シートの DB 化) ───
// 取込は xlsx をそのままアップロード。既定は dry-run (検証と FLG 突合レポートだけ)。
// apply=1 で本取込、さらに seed=1 なら「f_inbound_info が未設定の SKU にだけ」いろは有無を書く。
// 書き込みを伴うので管理者のみ
router.post('/admin/work-master-import', requireAdmin, checkOrigin, upload.single('file'), api(async (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: 'no_file', message: 'xlsx ファイルを選んでください' });
  let buf;
  try { buf = fs.readFileSync(req.file.path); } finally { try { fs.unlinkSync(req.file.path); } catch { /* 一時ファイルの掃除失敗は無視 */ } }
  let parsed;
  try {
    parsed = await parseWorkMasterXlsx(buf);
  } catch (e) {
    logWorkMasterImport({ actor: req.session.email, fileName: req.file.originalname, ok: false, message: e.message });
    return res.status(400).json({ ok: false, error: 'bad_xlsx', message: e.message });
  }
  // 在庫化必要FLG は廃止 (中原さん 2026-09-02) — 突合レポートも seed も行わない。
  // 在庫化要否の正本は f_inbound_info.いろは在庫化作業有無 (荷受け時のその場選択で育つ)
  const apply = String(req.body?.apply || '') === '1';
  const issueTotal = importIssueCount(parsed.issues);
  const del = computeDeletions(parsed.rows);   // 取込 = 全置換。xlsx に無い既存行は削除される (予告して見せる)
  const out = {
    ok: true, dryRun: !apply, dataRows: parsed.dataRows, rowCount: parsed.rows.length,
    issues: parsed.issues, issueTotal,
    wouldDelete: { count: del.count, codes: del.codes },
  };
  // ⭐検証エラーが1件でもあれば本取込は拒否 (Codex PR2 High-1)。
  //   「入数 abc」等を null で取り込むと既存値 (180 等) を黙って消すため、xlsx 側を直してもらう
  if (apply && issueTotal > 0) {
    logWorkMasterImport({
      actor: req.session.email, fileName: req.file.originalname, ok: false,
      message: `検証エラー ${issueTotal} 件のため本取込を拒否`,
    });
    return res.status(400).json({
      ...out, ok: false, dryRun: true, error: 'validation_failed',
      message: `検証エラーが ${issueTotal} 件あります。xlsx を直して取り込み直してください (下のレポート参照)`,
    });
  }
  if (apply) {
    out.applied = applyWorkMaster(parsed.rows, { user: req.session.email });
    logWorkMasterImport({
      actor: req.session.email, fileName: req.file.originalname, ok: true,
      message: `${parsed.rows.length}行 (新規${out.applied.inserted}/更新${out.applied.updated}/変化なし${out.applied.unchanged}/削除${out.applied.deleted})`,
    });
  }
  res.json(out);
}));

router.get('/admin/work-master', requireSession, api((req, res) => {
  const q = String(req.query?.q || '').trim();
  res.json({ ok: true, stats: workMasterStats(), rows: q ? searchWorkMaster(q) : [] });
}));

router.post('/admin/work-master/add', requireAdmin, checkOrigin, api((req, res) => {
  const r = addWorkMasterRow(req.body?.code, req.session.email);
  res.status(r.ok ? 200 : 400).json(r);
}));

router.post('/admin/work-master/update', requireAdmin, checkOrigin, api((req, res) => {
  const r = updateWorkMasterRow(req.body?.code, req.body?.fields || {}, req.session.email, Number(req.body?.expect_version));
  res.status(r.ok ? 200 : (r.error === 'conflict' ? 409 : 400)).json(r);
}));

// 端末登録: 発行したトークンは httpOnly Cookie としてこの端末にだけ渡す。登録と同時に管理者セッションを破棄
router.post('/admin/devices', checkOrigin, requireAdmin, api((req, res) => {
  const label = String(req.body?.label || '').trim();
  if (!label || label.length > 40) return res.status(400).json({ ok: false, error: 'bad_label', message: '端末名を1〜40文字で入力してください' });

  // 🏷 印刷エージェント (倉庫PC) は iPad と発行導線は同じだが扱いが逆:
  //   - Cookie ではなく**平文トークンをこの1回だけ画面に表示**する (エージェントの config.json に貼る)
  //   - 管理者セッションは破棄しない (登録しているのは中原さんの PC で、共用端末ではない)
  //   - 出力先プリンター名をサーバー側で紐づける (エージェントの設定ミスで別プリンターに出さない)
  if (String(req.body?.kind || '') === 'agent') {
    let created;
    try {
      created = createDevice(label, req.session.email, { kind: 'agent', printerName: req.body?.printer_name });
    } catch (e) {
      return res.status(400).json({ ok: false, error: 'bad_printer', message: e.message });
    }
    return res.json({ ok: true, kind: 'agent', id: created.id, token: created.token });
  }
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

// 🏷 印刷エージェントの出力先プリンターを付け替える (登録し直さずに直せるように)
router.post('/admin/devices/:id(\\d+)/printer', checkOrigin, requireAdmin, api((req, res) => {
  const r = setAgentPrinter(Number(req.params.id), req.body?.printer_name);
  res.status(r.ok ? 200 : (r.error === 'not_agent' ? 404 : 400)).json(r);
}));

// 管理画面の「直近の印刷ジョブ」を更新なしで見るための JSON (管理者)
router.get('/admin/print-jobs', requireAdmin, api((req, res) => {
  res.json({ ok: true, jobs: listPrintJobs(50).map(j => ({ ...publicJob(j), device_label: j.device_label })), agents: listPrintAgents() });
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
