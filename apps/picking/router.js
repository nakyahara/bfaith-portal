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
  initPickingDB, getDB, jstToday, listBatches, listLines, getBatch, STATUS_LABELS,
  createDevice, verifyDevice, revokeDevice, listDevices, DEVICE_KINDS,
  listWorkers, getWorker, addWorker, setWorkerActive,
} from './db.js';
import {
  parseCs03002, importBatch, formatLocation, PkError, getWorkState, applyEvent,
  deriveFolderName, isStaleInstructDate, getDailySummary, PAUSE_REASONS,
  getPickingStats, getTodayProgress, getMissStats, statsRange, loadStatsLines, STATS_WINDOW_DAYS, STATS_MIN_DATE,
} from './service.js';
import { reconcileRepickBatches, createFloorAlert, listFloorAlerts, ackFloorAlert, listShortageAllocations, bindPendingLaterRequests, syncRepickTask } from './service.js';
import { notifyShortage, notifyShortageUndo } from './notify.js';
import { allPatternNames } from './patterns.js';
import { fetchStockLocations, listStockCandidates, stockLookupConfigured } from './stock-locations.js';
import { enqueueBatchSync, fetchNotionWorkerNames, STATUS_PICKING, STATUS_PICKED } from './notion.js';
import { getFloorData } from './floor.js';
import { syncStaff, isStaffSyncConfigured, getStaffSyncState } from './staff-sync.js';
// ロケーション動線マスタ (NEXTサイン)。起動時にマスタが空なら同梱CSVで初期化する
import {
  listFaces, importFaces, parseFacesCsv, validateFaces, knownLocationsFromLines,
  listFaceImports, getFaceImportCsv, exportFacesCsv, ensureLocationFacesSeeded,
} from './location-faces.js';
import fs from 'fs';
import { queueEnsureImages, getImageMap, listMissingImages, missingImagesCsv, requestForceRefresh } from './images.js';
import { listDriveFilesAcross, downloadDriveFileById } from '../../lib/drive-csv.js';
// Drive共有ヘルパーと自動ポーリング (standaloneが起動。router は手動取込と状態表示に使う)
import { getShippingFolders, driveCall, getPollerStatus } from './drive-sync.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// ─── 梱包 (packing) 連携: 再ピック・棚戻しキュー (要件§5.4/5.5) ───
// pk_pack_tasks は packing 所有のため、更新は packing の service (更新API) を通す。
// packing が無効/初期化失敗でも picking 本体に影響しないよう遅延import+fail-soft
let _packingSvc;
async function packingSvc() {
  if (_packingSvc === undefined) {
    try {
      _packingSvc = await import('../packing/service.js');
    } catch (e) {
      _packingSvc = null;
      console.warn('[picking] packing連携は無効 (packing初期化失敗):', e.message);
    }
  }
  return _packingSvc;
}

// import 時 (= server.js boot 時) に DB を初期化する。migration 失敗はここで throw して
// 旧スキーマのまま起動を継続させない (shipping-work と同規約)
initPickingDB();
ensureLocationFacesSeeded();

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

// 掲示モニター端末 (kind='board') が触れてよいパス。倉庫に常時表示する画面は
// 誰でも物理的に操作できるので、読み取り専用のここだけに閉じる (作業APIは叩かせない)。
// /board/exit は掲示モードの解除 (Cookie削除) — これが無いと、掲示端末を作業用に
// 戻したくなったときにブラウザの設定からCookieを消すしか手がなくなる
const BOARD_ALLOWED_PATHS = ['/board', '/api/board', '/board/exit', '/floor', '/api/floor'];

/** 全ルート共通の入口。セッション or 登録端末のどちらかが必要。 */
function pickingAccess(req, res, next) {
  // PWA manifest はブラウザが Cookie 無しで取りにくる (認証不要の無害な静的情報)
  if (req.path === '/manifest.json') return next();

  const device = verifyDevice(readCookie(req, DEVICE_COOKIE));
  // ⭐掲示端末は「セッションがあっても」読み取り専用に閉じる (Codexレビュー high)。
  //   セッションを先に見ると、掲示端末で誰かが一度ログインしただけで作業APIが開いてしまう。
  //   端末登録時のセッション破棄は登録の瞬間しか効かず、後日のログインを防げない
  if (device && device.kind === 'board') {
    if (!BOARD_ALLOWED_PATHS.includes(req.path)) {
      const msg = 'この端末は掲示専用です (作業には使えません)。'
        + '解除するには /apps/picking/board/exit を開いてください';
      return req.path.startsWith('/api/')
        ? res.status(403).json({ error: msg, code: 'board_only' })
        : res.status(403).send(msg);
    }
    req.pickingDevice = device;
    return next();
  }

  if (hasSessionAccess(req)) return next();
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
router.get('/', async (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  reconcileRepickBatches();   // 梱包側で取消されたピッキング漏れバッチを畳む (fail-soft)
  const batches = listBatches(workDate)
    .filter((b) => b.status !== 'cancelled' || b.work_date === workDate);
  let openTasks = 0;
  const psvc = await packingSvc();
  if (psvc) { try { openTasks = psvc.countOpenTasks({ kind: 'return' }); } catch { openTasks = 0; } }   // 棚戻しのみ (再ピックは 🔴バッチ)
  res.render(path.join(__dirname, 'views/batches'), {
    openTasks,
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

// ─── 再ピック・棚戻しキュー (梱包からの依頼を消化する画面) ───
router.get('/tasks', async (req, res) => {
  const psvc = await packingSvc();
  if (!psvc) return res.status(404).send('梱包連携は無効です');
  // 再ピックは 🔴バッチに一本化 (Q4 決定 2026-09-05)。このキューは棚戻し専用 —
  // 同じ依頼に入口が2つあると、片方で「在庫なし」・片方で他ロケ確保、と記録が割れる (9/5 に実発生)
  let tasks = [];
  try { tasks = psvc.listOpenTasks({ kind: 'return' }); } catch { tasks = []; }
  const images = getImageMap(tasks.map((t) => t.sku));
  res.render(path.join(__dirname, 'views/tasks'), {
    title: '棚戻し',
    workers: listWorkers(),
    tasks: tasks.map((t) => ({
      ...t,
      locationLabel: t.location ? formatLocation(t.block, t.location) : null,
      imageUrl: images.get(String(t.sku ?? '').trim().toLowerCase())?.url || null,
    })),
  });
});

// 不足分ピッキング完了の梱包側バナーは、旧「通知 (OK=既読のみ)」をやめてタスク状態
// (fulfilled=受領待ち) から梱包側が直接導出する (packing /api/floor-alerts の repickReady — 2026-08-23)。
// 通知の既読と業務上の受領が混同されていたため

/** タスク操作 (claim/fulfill/unavailable/cancel)。packing の更新APIへ委譲。 */
router.post('/api/tasks/:id(\\d+)/:action', checkOrigin, async (req, res) => {
  try {
    const psvc = await packingSvc();
    if (!psvc) return res.status(404).json({ error: '梱包連携は無効です' });
    const worker = resolveWorker(req);
    // 再ピックは 🔴バッチに一本化 (Q4 決定 2026-09-05)。この API は棚戻しだけ — 古い画面・直接リクエストからの
    // 二経路更新 (片方で在庫なし・片方で確保) を残さない (Codex R1 High)
    const target = psvc.getTask(Number(req.params.id));
    if (!target) return res.status(404).json({ error: '依頼が見つかりません', code: 'not_found' });
    if (target.kind !== 'return') {
      return res.status(409).json({ error: '再ピックはバッチ一覧の 🔴バッチから操作してください', code: 'repick_batch_only' });
    }
    // 棚戻しの操作は 対応する / 棚に戻した / 取下げ だけ。「在庫なし」は再ピック (=🔴バッチの欠品シート) の概念で、
    // 棚戻しに流すと伝票の出荷保留バナーを誤って作る (Codex R2 Medium)
    if (!['claim', 'fulfill', 'cancel'].includes(String(req.params.action))) {
      return res.status(400).json({ error: '棚戻しではこの操作はできません', code: 'bad_return_action' });
    }
    const t = psvc.applyTaskAction(Number(req.params.id), String(req.params.action), worker.name);
    res.json({ ok: true, id: t.id, status: t.status });
  } catch (e) {
    // packing 側の業務エラー (PackError) も picking の PkError と同じ形で返す
    if (e && Number.isInteger(e.status) && e.code) {
      return res.status(e.status).json({ error: e.message, code: e.code });
    }
    console.error('[picking-tasks]', e);
    res.status(500).json({ error: 'サーバーエラーが発生しました' });
  }
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
  // 取込時に画像解決が失敗していても、作業画面を開いた時点で取り直す
  // (errorキャッシュは30分TTL — キュー混雑が収まっていればここで復活する)
  queueEnsureImages(state.lines.map((l) => l.sku), `work#${state.batch.id}`);
  res.render(path.join(__dirname, 'views/work'), {
    title: `ピッキング | ${state.batch.hikiate_class}`,
    // セッションなら worker は確定 (email)。端末モードは null = 画面側が作業者選択を出す
    worker: req.session?.email || null,
    displayName: req.session?.displayName,
    workers: listWorkers(),
    pauseReasons: PAUSE_REASONS,
    state,
    // NEXTサイン用の面マスタ (45行程度)。判定は端末側 (next-sign.js) で行う
    faces: listFaces(),
  });
});

// NEXTサインの判定ロジック (next-sign-core.cjs) をブラウザ用に配信する。
// サーバーとブラウザで同じコードを使い、判定がずれないようにする (CommonJS を window.NextSign に包む)
const NEXT_SIGN_CORE_PATH = path.join(__dirname, 'next-sign-core.cjs');
let nextSignJsCache = null;
router.get('/next-sign.js', (req, res) => {
  if (!nextSignJsCache || process.env.NODE_ENV !== 'production') {
    const src = fs.readFileSync(NEXT_SIGN_CORE_PATH, 'utf8');
    nextSignJsCache = `(function(){ const module = { exports: {} };
${src}
window.NextSign = module.exports; })();
`;
  }
  res.type('application/javascript').set('Cache-Control', 'no-cache').send(nextSignJsCache);
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
  // back で取り消される明細が欠品記録なら、適用後に訂正通知を出すため先に控える
  let undoneShortage = null;
  if (req.body.event === 'back') {
    const l = listLines(batchId).find((x) => x.seq === Number(req.body.line_seq));
    if (l && l.status === 'shortage') undoneShortage = { ...l, locationLabel: formatLocation(l.block, l.location) };
  }
  const result = applyEvent(batchId, {
    opId: req.body.op_id,
    event: req.body.event,
    lineSeq: req.body.line_seq == null ? null : Number(req.body.line_seq),
    clientAt: req.body.client_at,
    undoOpId: req.body.undo_op_id || null,
    shortageQty: req.body.shortage_qty == null ? null : Number(req.body.shortage_qty),
    pauseReason: req.body.pause_reason || null,
    // 欠品フローv2: 他ロケで確保した分と残りの扱い
    altBlock: req.body.alt_block || null,
    altLocation: req.body.alt_location || null,
    altQty: req.body.alt_qty == null || req.body.alt_qty === '' ? null : Number(req.body.alt_qty),
    remaining: req.body.remaining || null,
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
        altFree: req.body.alt_free == null ? null : Number(req.body.alt_free),
        allocations: listShortageAllocations(batchId, Number(req.body.line_seq)),
      }).catch((e) => console.warn(`[picking-notify] 欠品通知失敗 (${line.sku}): ${e.message}`));
    }
    // 「後で取りに行く」を梱包タスクへ展開 (梱包が取込済みなら即・未取込なら reconcile が追いつく)
    try { bindPendingLaterRequests(); } catch { /* fail-soft */ }
  }
  // 欠品記録の取消 (back) は通知先へ訂正を流す (通知は消せないので追送 — Codex R1)
  if (req.body.event === 'back' && !result.replayed && undoneShortage) {
    notifyShortageUndo({ batch: getBatch(batchId), line: undoneShortage, worker: worker.name })
      .catch((e) => console.warn(`[picking-notify] 欠品訂正通知失敗 (${undoneShortage.sku}): ${e.message}`));
  }
  // Notion連携 (fail-soft)。replayed では動かさない。ラベルは送信直前にバッチの
  // 最新状態から決める (イベント時点のラベルだと並行PATCHの順序逆転で巻き戻る)
  if (!result.replayed && result.transition) {
    enqueueBatchSync(batchId, () => {
      const b = getBatch(batchId);
      if (!b || b.origin === 'repick') return null;   // ピッキング漏れはNotionカードを持たない
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
  // 🔴ピッキング漏れバッチ → 梱包タスクの状態同期 (fail-soft・トランザクション外)。
  // 開始=対応中 (claim) / 完了=持って行った (fulfill→梱包画面の「再ピック届いた」表示へ) /
  // 欠品で残りあり=在庫なし (unavailable + 1階の全端末へ赤バナー)。他ロケで全量確保は通常の完了と同じ。
  // replay でも実行する (Codexレビュー: 同期の一時失敗後、同一op_id再送で収束させる。
  // 各アクションは遷移ガードつきで二重適用は失敗ログ止まり=無害)。分岐は syncRepickTask (テスト対象)
  {
    const sync = syncRepickTask(batchId, { event: req.body.event }, worker.name, await packingSvc());
    if (sync.unavailable) {
      const { task, remaining, altQty } = sync.unavailable;
      import('../packing/notify.js')
        .then(({ notifyTaskUnavailable }) => notifyTaskUnavailable(task, worker.name, { remaining, altQty }))
        .catch((e) => console.warn(`[picking] 在庫なし通知失敗: ${e.message}`));
    }
  }
  res.json({ ok: true, ...result });
}));

/**
 * 一覧の変化検知用シグネチャ (バッチの増減・状態変化で変わる。明細単位の進捗では変えない
 * = 他人の作業中に一覧がチラチラ再読込されない)。一覧画面が10秒間隔でポーリングする
 */
// ─── 現場間アラート (バッチ一覧のボタン → 梱包ヘッダーへ / 梱包からの通知を表示) ───
function alertRequester(req) {
  return req.session?.displayName || req.session?.email || req.pickingDevice?.label || 'ピッキング現場';
}
router.post('/api/floor-alerts', checkOrigin, api(async (req, res) => {
  const kind = String(req.body.kind || '');
  if (!['cart', 'trolley', 'lift'].includes(kind)) {
    return res.status(400).json({ error: '不明なアラート種別です' });
  }
  res.json({ ok: true, ...createFloorAlert(kind, alertRequester(req)) });
}));
router.get('/api/floor-alerts', api(async (req, res) => {
  res.json({ ok: true, alerts: listFloorAlerts('to_picking') });
}));
router.post('/api/floor-alerts/:id(\\d+)/ack', checkOrigin, api(async (req, res) => {
  ackFloorAlert(Number(req.params.id), alertRequester(req), 'to_picking');
  res.json({ ok: true });
}));

router.get('/api/batches-signature', api(async (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const sig = listBatches(workDate)
    .map((b) => `${b.id}:${b.status}`)
    .join(',');
  res.json({ ok: true, sig });
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
/**
 * 欠品フローv2: 表示中明細の同一SKUの他ロケ在庫 (ロジザード毎時スナップショット) を画面に出す。
 * 取得失敗 (fetched=false) と候補ゼロ (rows=[]) は区別して返す — 失敗時に「どこにもない」を既定にしない
 */
router.get('/api/batches/:id(\\d+)/stock-locations', api(async (req, res) => {
  const batchId = Number(req.params.id);
  const seq = Number(req.query.seq);
  const line = listLines(batchId).find((l) => l.seq === seq);
  if (!line) throw new PkError(404, 'line_not_found', '明細がありません');
  if (!stockLookupConfigured()) return res.json({ ok: true, configured: false, fetched: false, rows: [] });
  const data = await fetchStockLocations(line.sku);
  const out = listStockCandidates(data, { excludeBlock: line.block, excludeLocation: line.location, groupByLocation: true, maxRows: 40 });
  res.json({ ok: true, configured: true, sku: line.sku, ...out });
}));

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
    icons: [
      { src: '/app-icons/picking-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/app-icons/picking-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
    ],
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

// ─── 作業実績 (30日ローリング) ───
//
// 掲示モニター (/board) と管理画面 (/admin/stats) が同じ集計を見る。
// 中原さんの方針 (2026-08-17): 個人別スピードは全員に公開してよい。
// ただし「重い分類を引いた人が遅く見える」のを避けるため速さ指数を主指標にし、
// 欠品報告件数を併記する (止まって報告する方が損に見えないようにする)。

/** クエリの days を安全な範囲へ (1〜365)。 */
function parseDays(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return STATS_WINDOW_DAYS;
  return Math.min(365, Math.max(1, Math.round(n)));
}

// 掲示モニター用ボード (端末Cookie可 = 壁掛け端末でログイン不要)
router.get('/board', (req, res) => {
  res.render(path.join(__dirname, 'views/board'), {
    title: 'ピッキング実績ボード',
    windowDays: STATS_WINDOW_DAYS,
  });
});

/**
 * 掲示モードの解除 (壁掛け端末を作業用に転用したいときの出口)。
 *
 * ⭐管理者セッション必須 + POST + 解除と同時にセッションも破棄する (Codexレビュー high)。
 *   GETで無条件に消せると、掲示端末の前に立った人 (や外部サイトからの誘導) が
 *   board Cookie だけを消し、残っている管理者セッションで作業・管理APIを開けてしまう。
 *   掲示端末では /admin/* も塞がっているので、解除はこの画面が唯一の導線になる。
 */
router.get('/board/exit', (req, res) => {
  const isAdmin = req.session?.role === 'admin';
  res.type('html').send(`<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>掲示モードの解除</title>
    <style>body{font-family:system-ui,sans-serif;max-width:36em;margin:3em auto;padding:0 1em;line-height:1.7}
    button{padding:.6em 1.4em;font-size:1em;cursor:pointer}</style></head><body>
    <h1>掲示モードの解除</h1>
    <p>この端末は<b>掲示専用</b>として登録されています (実績ボードのみ表示できます)。</p>
    ${isAdmin ? `<p>解除すると端末Cookieを削除し、同時にログアウトします。
        作業用に使う場合は、そのあと改めて管理者でログインして「作業用」で登録し直してください。</p>
      <button id="go" type="button">掲示モードを解除する</button>
      <p id="msg"></p>
      <script>
        document.getElementById('go').addEventListener('click', function () {
          fetch('/apps/picking/board/exit', { method: 'POST', credentials: 'same-origin' })
            .then(function (r) { return r.json(); })
            .then(function (d) {
              document.getElementById('msg').textContent = d.ok ? '解除しました。' : (d.error || '失敗しました');
            })
            .catch(function (e) { document.getElementById('msg').textContent = '失敗しました: ' + e.message; });
        });
      </script>`
      : `<p><b>解除には管理者のログインが必要です。</b>
        <a href="/login">ログイン</a>してから、この画面をもう一度開いてください。</p>`}
    <p><a href="/apps/picking/board">← ボードへ戻る</a></p>
    </body></html>`);
});

router.post('/board/exit', checkOrigin, requireAdmin, api(async (req, res) => {
  // ⭐セッション破棄の成功を確認してから Cookie を消す (Codexレビュー high)。
  //   端末Cookieだけ先に消えて管理者セッションが残ると、まさにそのセッションで
  //   作業APIが開いてしまう = 塞いだはずの穴が破棄失敗時だけ再現する
  req.session.destroy((err) => {
    if (err) {
      console.error('[picking] 掲示モード解除: セッション破棄に失敗', err);
      return res.status(500).json({ error: '解除できませんでした (セッションを破棄できません)' });
    }
    res.clearCookie(DEVICE_COOKIE, { path: '/apps/picking' });
    res.json({ ok: true, loggedOut: true });
  });
}));

// ボードのデータ (画面が定期取得する。全画面リロードだとチラつくため)
router.get('/api/board', api(async (req, res) => {
  // 明細の読み込みは1回にして速さ統計とミス率で共有 (20秒ごと×端末数のポーリング — Codex)
  const days = parseDays(req.query.days);
  const range = statsRange(jstToday(), days);
  const lineRows = loadStatsLines(range.since, range.until);
  const stats = getPickingStats({ days, lineRows });
  // ピッキングミス率 (中原さん指示 2026-08-31: 件数より比率・欠品はミスに含めない)
  const miss = getMissStats({ days });
  res.set('Cache-Control', 'no-store');
  res.json({
    ok: true,
    now: new Date().toISOString(),
    today: getTodayProgress(),
    miss: {
      since: miss.since, until: miss.until, minLines: miss.minLines,
      total: miss.total,
      workers: miss.byWorker.map((w) => ({
        worker: w.worker, name: w.name, lines: w.lines, total: w.total, stockout: w.stockout,
        shortage: w.shortage, excess: w.excess, wrong_item: w.wrong_item, per1000: w.per1000, provisional: w.provisional,
      })),
    },
    stats: {
      since: stats.since, until: stats.until, days: stats.days,
      minDate: STATS_MIN_DATE,
      minLines: stats.minLines,
      minClassLines: stats.minClassLines,
      outlierSec: stats.outlierSec,
      total: stats.total,
      // 掲示は明細数上位の分類のみ (ヒートマップの行数 = 画面に収まる範囲)。全量は管理画面で見る
      baseline: stats.baseline.slice(0, 12).map((c) => ({
        key: c.key, lines: c.lines, avgSec: c.avgSec, workerCount: c.workerCount,
        workers: c.workers.map((w) => ({
          worker: w.worker, name: w.name, lines: w.lines, secPerLine: w.secPerLine,
          index: w.index, provisional: w.provisional,
        })),
      })),
      workers: stats.workers.map((w) => ({
        worker: w.worker, name: w.name, lines: w.lines, secPerLine: w.secPerLine,
        index: w.index, provisional: w.provisional, shortages: w.shortages,
        batches: w.batches, days: w.days,
      })),
      byDate: stats.byDate,
    },
  });
}));

// ─── 出荷フロアボード (43インチ統合掲示 — ピッキング/梱包実績 + 出荷進捗 + 完了予測) ───
// /board と同じく掲示端末 (kind='board') で開ける。既存 /board は単機能表示用に残す

router.get('/floor', (req, res) => {
  res.render(path.join(__dirname, 'views/floor'), {
    title: '出荷フロアボード',
    windowDays: STATS_WINDOW_DAYS,
    rotationSec: Number(process.env.FLOOR_ROTATION_SEC) || 30,
  });
});

router.get('/api/floor', api(async (req, res) => {
  res.set('Cache-Control', 'no-store');
  res.json(await getFloorData({ days: parseDays(req.query.days) }));
}));

// 管理画面 (全量・分類内訳つき)
router.get('/admin/stats', requireAdmin, (req, res) => {
  const until = isRealDate(String(req.query.until || '')) ? String(req.query.until) : jstToday();
  res.render(path.join(__dirname, 'views/admin_stats'), {
    title: 'ピッキング実績 (30日)',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    stats: getPickingStats({ until, days: parseDays(req.query.days) }),
    miss: getMissStats({ until, days: parseDays(req.query.days) }),
    minDate: STATS_MIN_DATE,
  });
});

// ─── 画像が出ない商品の一覧 (2026-08-31 中原さん依頼) ───
// 「ピッキング画面で写真が出ない商品」を楽天の商品管理番号つきで一覧化し、
// 楽天側を直せば直るもの / そもそも楽天に商品が無いもの を見分けられるようにする
function missingImagesParams(req) {
  const until = isRealDate(String(req.query.until || '')) ? String(req.query.until) : jstToday();
  return { until, days: parseDays(req.query.days) };
}

router.get('/admin/missing-images', requireAdmin, (req, res) => {
  const p = missingImagesParams(req);
  res.render(path.join(__dirname, 'views/admin_missing_images'), {
    title: '画像が出ない商品',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    result: listMissingImages(p),
    retried: req.query.retried === '1' ? Number(req.query.n || 0) : null,
  });
});

router.get('/admin/missing-images.csv', requireAdmin, (req, res) => {
  const p = missingImagesParams(req);
  res.type('text/csv; charset=utf-8')
    .set('Content-Disposition', `attachment; filename="picking-missing-images-${p.until}.csv"`)
    .send(missingImagesCsv(listMissingImages(p)));
});

/**
 * 一覧に出ているSKUを強制再取得する (not_found は当日中は自動再試行しないための手動の出口)。
 * 対象は「再取得で直る可能性があり、かつ楽天の商品管理番号が引けるもの」だけ —
 * 管理番号が引けない商品は何度呼んでも not_found で、RMS を無駄に叩くだけになる。
 * 取込直後の自動解決と同じ直列キューに載せ、走っている間は 409 で断る (Codex R1)。
 * all=1 で「画像なし全件 (管理番号があるもの)」に広げる。
 */
router.post('/admin/missing-images/retry', requireAdmin, checkOrigin, api(async (req, res) => {
  const p = missingImagesParams(req);
  const result = listMissingImages(p);
  const all = String(req.query.all || '') === '1';
  const skus = result.missing
    .filter((x) => x.manageNumber && (all || x.retryable))
    .map((x) => x.sku);
  const stats = await requestForceRefresh(skus, `admin再取得(${skus.length}件)`);
  if (stats === null) {
    throw new PkError(409, 'image_queue_busy', '画像の取得が進行中です。少し待ってからもう一度お試しください');
  }
  res.json({ ok: true, requested: skus.length, stats: skus.length > 0 ? stats : null });
}));

// ─── 端末・作業者管理 (管理者・セッション必須) ───

router.get('/admin/devices', requireAdmin, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_devices'), {
    title: '端末・作業者管理 | ピッキング支援',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    devices: listDevices(),
    workers: listWorkers(true),
    staffSync: { configured: isStaffSyncConfigured(), state: getStaffSyncState() },
  });
});

/**
 * スタッフマスタ (Render apps/staff) から今すぐ同期する。
 * 通常は drive-poller が1時間に1回自動で行う (この画面の「最終同期」に出る)。
 */
router.post('/admin/workers/sync-staff', checkOrigin, requireAdmin, api(async (req, res) => {
  if (!isStaffSyncConfigured()) {
    throw new PkError(400, 'staff_sync_disabled', 'STAFF_EXPORT_TOKEN が未設定です (miniPC の .env に設定してください)');
  }
  res.json({ ok: true, result: await syncStaff() });
}));

/**
 * この端末を登録する。発行したトークンは httpOnly Cookie としてこの端末にだけ渡す
 * (画面にも他の誰にも見せない)。倉庫のiPhoneで管理者がログインして1回だけ実行する。
 * ⭐登録と同時に管理者セッションを破棄する (「登録してログアウト」を原子化)。
 *   共用端末に管理者セッションが残ると、渡した相手が取込・管理APIを触れてしまうため (Codex high)
 */
router.post('/admin/devices', checkOrigin, requireAdmin, api(async (req, res) => {
  const label = String(req.body.label || '').trim();
  if (!label || label.length > 40) throw new PkError(400, 'bad_label', '端末名を1〜40文字で入力してください');
  // 用途。board = 倉庫の掲示モニター (読み取り専用。作業画面・作業APIは pickingAccess が拒否)
  const kind = String(req.body.kind || 'worker').trim();
  if (!DEVICE_KINDS.includes(kind)) throw new PkError(400, 'bad_kind', '端末の用途が不正です');
  const token = createDevice(label, req.session.email, kind);
  // 破棄が成功してから Cookie を渡す (破棄に失敗したまま端末Cookieを配ると、
  // 共用端末に管理者セッションが残ったまま作業者に渡ることになる)
  req.session.destroy((err) => {
    if (err) {
      console.error('[picking] 端末登録: セッション破棄に失敗', err);
      return res.status(500).json({ error: '登録を完了できませんでした (セッションを破棄できません)' });
    }
    res.cookie(DEVICE_COOKIE, token, {
      httpOnly: true,
      // 明示的に development と宣言された環境以外は常に Secure (NODE_ENV 未設定でも安全側)
      secure: process.env.NODE_ENV !== 'development',
      sameSite: 'lax',
      maxAge: 400 * 24 * 3600 * 1000,   // ブラウザ上限 (~400日)。サーバー側でも同TTLを検証
      path: '/apps/picking',
    });
    res.json({ ok: true, loggedOut: true });
  });
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
    qtyWarnings: preview.qtyWarnings,
    lines: preview.lines.map((l) => ({
      locationLabel: formatLocation(l.block, l.location),
      sku: l.sku, productName: l.productName, qty: l.qty,
    })),
  };
}

function runImport(preview, req) {
  return importBatch(preview, {
    hikiateClass: req.body.hikiate_class,
    classSource: 'manual',   // 管理画面の取込 = 人が分類を選んで確定している
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

// ─── ロケーション動線マスタ (NEXTサイン・管理者) ───

router.get('/admin/locations', requireAdmin, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_locations'), {
    title: 'ロケーション動線 | ピッキング支援',
    username: req.session.email,
    displayName: req.session.displayName,
    faces: listFaces(),
    imports: listFaceImports(20),
  });
});

/** 現在のマスタを CSV で返す (Excel で直して再取込する用)。 */
router.get('/admin/locations/export.csv', requireAdmin, (req, res) => {
  res.type('text/csv; charset=utf-8')
    .set('Content-Disposition', `attachment; filename="location-faces-${jstToday()}.csv"`)
    .send(exportFacesCsv());
});

/** 過去に取り込んだ版の CSV (戻したいときに再取込する)。 */
router.get('/admin/locations/imports/:id(\d+).csv', requireAdmin, (req, res) => {
  const row = getFaceImportCsv(Number(req.params.id));
  if (!row) return res.status(404).send('見つかりません');
  res.type('text/csv; charset=utf-8')
    .set('Content-Disposition', `attachment; filename="location-faces-import-${req.params.id}.csv"`)
    .send(row.csv_text);
});

/**
 * 取込API (管理者)。multipart: file=CSV。
 *   mode=preview … 解析+検証の結果 (エラー・警告・面一覧) を返すだけ
 *   mode=confirm … 全置換して履歴に残す
 * CS03002 取込と同じ二段方式 (サーバーに中間状態を持たない)
 */
router.post('/admin/locations', checkOrigin, requireAdmin, (req, res, next) => {
  uploadCsv.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ error: `アップロード失敗: ${err.message}` });
    next();
  });
}, api(async (req, res) => {
  if (!req.file) throw new PkError(400, 'no_file', 'ファイルを選択してください');
  const faces = parseFacesCsv(req.file.buffer);
  const v = validateFaces(faces, { knownLocations: knownLocationsFromLines() });
  const summary = {
    faceCount: v.faces.length, totalSlots: v.totalSlots, errors: v.errors, warnings: v.warnings,
    racks: new Set(v.faces.map((f) => f.rack_id)).size,
    faces: v.faces.map((f) => ({
      seq_no: f.seq_no, block: f.block, col: f.col, ren_from: f.ren_from, ren_to: f.ren_to,
      face_kind: f.face_kind, rack_id: f.rack_id, move_in: f.move_in, reliable: f.reliable, direction: f.direction,
    })),
  };
  if (String(req.body.mode) !== 'confirm') return res.json({ ok: v.ok, mode: 'preview', ...summary });
  if (!v.ok) throw new PkError(400, 'invalid_faces', `検証エラーがあるため取り込めません:\n${v.errors.join('\n')}`);
  const result = importFaces(req.file.buffer, { actor: req.session.email, filename: req.file.originalname });
  res.json({ ok: true, mode: 'confirm', ...summary, importId: result.importId });
}));

// ─── Drive取込 (出荷_no フォルダ・管理者) ───

/** 自動ポーリングの状態 (standaloneで稼働。portalでは running:false)。 */
router.get('/admin/import/poller-status', requireAdmin, api(async (req, res) => {
  const s = getPollerStatus();
  const db = getDB();
  const recent = db.prepare(`
    SELECT filename, folder_name, status, error, processed_at FROM pk_drive_imports
    ORDER BY processed_at DESC LIMIT 10
  `).all();
  const failedCount = db.prepare(
    "SELECT COUNT(*) c FROM pk_drive_imports WHERE status='failed'"
  ).get().c;
  res.json({ ok: true, poller: s, recent, failedCount });
}));

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
