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
import fs from 'node:fs';
import crypto from 'node:crypto';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  initPackingDB, getDB, jstToday, listPackBatches, getPackBatch, listPackSlips,
  listPackLinesBySlip, STATUS_LABELS, MATCH_LABELS,
  createDevice, verifyDevice, revokeDevice, listDevices, setAgentPrinters, agentPrintersOf,
} from './db.js';
import { getPollerStatus, markLedgerImported } from './drive-sync.js';
import {
  parseCs03003, importPackBatch, checkPickingMatch, PackError,
  deriveFolderName, isStaleSagyoDate, WARN_LABELS, getWorkState, applyEvent,
  PAUSE_REASONS, UNDO_REASONS, SHIP_CHANGE_REASONS, SHIP_CHANGE_METHOD_OPTIONS, SHIP_CHANGE_TWO_LABELS, lastDoneSeqOf, getDailySummary,
  resolveIncident, lineKindOf, batchHikiateClass, batchClassInfo, listLineRuns, lineDailyTotal, listRepickReady,
  claimStockoutNotify, markStockoutNotify, shortageSummaryFor, setTaskLocationHint,
} from './service.js';
import { notifyShipChange, notifyTask, notifyReprint, postReprintText, notifyStockout } from './notify.js';
import {
  extractReprintPdf, cleanupReprintPdfs, detectOkurijoSlug, REPRINTS_DIR, LabelUnusableError,
} from './reprint-pdf.js';
import { enqueuePackBatchNotionSync } from './notion.js';
import { getPackingStats, getTodayPackingProgress, PACK_STATS_WINDOW_DAYS, PACK_STATS_MIN_DATE } from './stats.js';
import {
  enqueuePrintJob, leaseNextJob, findLeasedJob, claimPdfForPrint, failBeforeDispatch,
  markSubmitted, markFinished, recordHeartbeat, listPrintJobs, markAlerted, alertTextFor,
  getJobStatusFor, listPrintRoutes, setPrintRoute, SLUG_LABELS,
  LEASE_SEC, PRINT_STATE_LABELS,
} from './print-queue.js';
import { listNouhinCsvFiles, downloadNouhinCsv, driveCall } from './drive.js';
// 商品画像は picking の楽天白抜きキャッシュ (pk_product_images) を共通部品として流用 (要件§7.1)
import { getImageMap, queueEnsureImages } from '../picking/images.js';
import { neNamesFor } from './ne-names.js';
// 梱包資材の表示・現場登録 (要件 = AI_reference『梱包資材表示_要件定義_20260823.md』v1.7)
import {
  materialsForState, materialOptions, registerMaterial, undoMaterial,
  manualResend as materialManualResend, materialDailyCounts,
  seedMaterialsData, normalizeKeyText, classCandidates,
  BASE_DELIVERY_CODES, DELIVERY_CODES, UNDO_SEC as MATERIAL_UNDO_SEC, UNDO_SECRET_CONFIGURED,
} from './materials.js';
import { DATA_DIR } from './db.js';

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
  // 🖨 抜き出した送り状PDFの配信 (事務がチャットのリンクから開く)。認証はセッションでなく
  // 128bit推測不能トークン (capability URL)。ファイルは7日で自動削除
  if (/^\/reprints\/[A-Za-z0-9_-]{16,}\.pdf$/.test(req.path)) return next();
  // 🖨 印刷エージェント (出荷PC) は Cookie ではなく Authorization ヘッダーで名乗る。
  // /print/ 配下はここでは素通しし、router.use('/print', requirePrintAgent) が
  // kind='agent' の端末だけを通す (iPad の端末Cookieでは絶対に印刷ジョブを取れない)。
  // ⭐ルートを列挙しないのは、後から /print/... を足したときに
  //   「正規のエージェントが packingAccess に弾かれる」ズレを作らないため
  if (req.path === '/print' || req.path.startsWith('/print/')) return next();
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
        // e.body = 409 応答に同梱する追加情報 (資材の現在値など — 要件『梱包資材表示』§5.1)
        return res.status(e.status).json({ error: e.message, code: e.code, ...(e.body || {}) });
      }
      console.error('[packing]', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

// ─── 🖨 抜き出した送り状PDFの配信 (capability URL・認証はトークンのみ) ───
router.get('/reprints/:token([A-Za-z0-9_-]{16,}).pdf', (req, res) => {
  const file = path.join(REPRINTS_DIR, `${req.params.token}.pdf`);
  if (!file.startsWith(REPRINTS_DIR) || !fs.existsSync(file)) return res.status(404).send('見つかりません (7日で削除されます)');
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', 'inline');
  res.setHeader('Cache-Control', 'private, no-store');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  fs.createReadStream(file).pipe(res);
});

/**
 * バッチ一覧の署名 (画面の自動追従用)。
 * 納品書CSVの取込は随時来るので、一覧を開きっぱなしでも増減・状態変化に追従させる。
 * ⭐毎回フルリロードせず「署名が変わった時だけ」再読込する — 進捗の数字が動くたびに
 *   画面が白く飛ぶと、iPadで作業しながら見ている人には使えない (picking と同方式)
 */
router.get('/api/batches-signature', api(async (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const sig = listPackBatches(workDate).map((b) => `${b.id}:${b.status}`).join(',');
  res.json({ ok: true, sig });
}));

// ─── 🖨 印刷キュー (出荷PCの印刷エージェントが pull で取りに来る・要件§6) ───
/**
 * エージェント認証。**Authorization ヘッダーのみ**で、iPad の端末Cookieは受け付けない
 * (共用iPadのCookieで送り状を勝手に刷れる状態を作らない)。
 */
function requirePrintAgent(req, res, next) {
  const m = /^Bearer\s+(\S+)$/.exec(req.headers.authorization || '');
  const device = m ? verifyDevice(m[1]) : null;
  if (!device || device.kind !== 'agent') {
    return res.status(401).json({ error: '印刷エージェントとして認証されていません' });
  }
  req.printAgent = device;
  next();
}

// /print/ 配下は**この1行で**エージェント認証を必須にする。個々のルートに付け忘れても
// ここで止まる (認証の付け忘れが一番怖いので、境界をprefixで一元化する)
router.use('/print', requirePrintAgent);

/** 次に刷るものを1件 lease して返す。無ければ 204 (エージェントは数秒後にまた聞きに来る)。 */
router.get('/print/next', requirePrintAgent, api(async (req, res) => {
  // このPCから出せるプリンターが1つも登録されていない端末には**lease する前に**断る。
  // 掴んでから断ると試行回数だけ減って正常なジョブが failed に落ちる
  if (agentPrintersOf(req.printAgent.id).length === 0) {
    return res.status(409).json({ error: 'この端末に出力先プリンターが登録されていません' });
  }
  const job = leaseNextJob(req.printAgent);
  if (!job) return res.status(204).end();
  res.json({ ok: true, job });
}));

/**
 * 印刷対象のPDF本体。
 * 🚨 **渡した時点で紙が出た可能性がある**ので、ここで dispatched に進めて自動再配布を止める
 *    (エージェントが投入報告の前に落ちても、別の端末には二度と配らない)。
 */
router.get('/print/:id(\\d+)/pdf', requirePrintAgent, (req, res) => {
  const jobId = Number(req.params.id);
  const lease = { deviceId: req.printAgent.id, leaseToken: req.query.lease ? String(req.query.lease) : null };
  // ① まずは読むだけ。PDFの実体を確かめる前に dispatched にすると、1バイトも渡していないのに
  //    「印刷したかもしれない」扱いになり、確実な欠落が「結果不明」に化ける
  const job = findLeasedJob(jobId, lease);
  if (!job) return res.status(404).json({ error: 'このジョブを保持していません (lease を確認してください)' });

  // ② PDFの実体を検証。渡せないと分かったらはっきり失敗にして人に知らせる
  const file = path.join(REPRINTS_DIR, `${job.pdf_token}.pdf`);
  const fail = (status, msg) => {
    failBeforeDispatch(jobId, { ...lease, error: msg });
    return res.status(status).json({ error: msg });
  };
  if (!file.startsWith(REPRINTS_DIR) || !fs.existsSync(file)) {
    return fail(410, '印刷対象のPDFがありません (7日で削除されます)');
  }
  let bytes;
  try { bytes = fs.readFileSync(file); } catch (e) { return fail(500, `PDFを読めません: ${e.message}`); }
  if (crypto.createHash('sha256').update(bytes).digest('hex') !== job.pdf_sha256) {
    return fail(409, '印刷対象のPDFが登録時と異なります');
  }

  // ③ 渡せると確定してから dispatched (= ここから自動再配布しない)
  if (!claimPdfForPrint(jobId, lease)) {
    return res.status(409).json({ error: 'ジョブの状態が変わりました (取り直してください)' });
  }
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Cache-Control', 'private, no-store');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.send(bytes);
});

/** エージェントが再起動したときに、掴んでいたジョブがどうなったか確かめるための照会。 */
router.get('/print/:id(\\d+)/status', requirePrintAgent, api(async (req, res) => {
  const row = getJobStatusFor(Number(req.params.id), req.printAgent.id);
  if (!row) throw new PackError(404, 'not_found', 'このジョブを保持していません');
  res.json({ ok: true, job: row });
}));

router.post('/print/:id(\\d+)/submitted', requirePrintAgent, api(async (req, res) => {
  const r = markSubmitted(Number(req.params.id), {
    deviceId: req.printAgent.id, leaseToken: String(req.body.lease || ''),
    spoolJobId: req.body.spool_job_id ?? null,
  });
  if (!r.ok) throw new PackError(409, 'not_leased', `報告を受け付けられません (${r.reason})`);
  // replayed = 応答が届かなかった前回と同じ報告。エージェントが「もう投入済み」と分かる
  res.json({ ok: true, replayed: !!r.replayed });
}));

router.post('/print/:id(\\d+)/completed', requirePrintAgent, api(async (req, res) => {
  // 「刷れた/刷れなかった」は真偽値でしか受け取らない。未指定・文字列 "false"・null を
  // 成功扱いすると、エージェントの不具合が「印刷成功」として記録されてしまう
  if (typeof req.body.ok !== 'boolean') {
    throw new PackError(400, 'bad_ok', 'ok は true / false で送ってください');
  }
  const r = markFinished(Number(req.params.id), {
    deviceId: req.printAgent.id, leaseToken: String(req.body.lease || ''),
    ok: req.body.ok, error: req.body.error ?? null,
    // uncertain = 「刷れなかった」と言い切れない (スプーラーに渡した後に落ちた等)。
    // failed にすると通知が「手動で印刷してください」になり、実は出ていた紙と合わせて二重になる
    uncertain: req.body.uncertain === true,
  });
  if (!r.ok) throw new PackError(409, 'not_leased', `報告を受け付けられません (${r.reason})`);
  // 印刷の成否は現場に見える形で伝える (出なかったことに誰も気づかない状態を作らない)。
  // 送信できたときだけ「通知済み」にする — 失敗した分はポーラーが次の周回で送り直す
  const job = getDB().prepare('SELECT * FROM pk_print_jobs WHERE id=?').get(Number(req.params.id));
  postReprintText(alertTextFor(job))
    .then((sent) => { if (sent) markAlerted(job.id, job.state); })
    .catch(() => { /* ポーラーが再送する */ });
  res.json({ ok: true, replayed: !!r.replayed });
}));

router.post('/print/heartbeat', requirePrintAgent, api(async (req, res) => {
  recordHeartbeat(req.printAgent.id, req.body.note ?? null);
  res.json({ ok: true, lease_sec: LEASE_SEC });
}));

// ─── 現場間アラート (「ピッキング済みの商品を下して」→ ピッキングヘッダーへ) ───
// テーブルは picking 所有 — picking service の関数経由で読み書き (要件§7.1)
function alertRequester(req) {
  return req.session?.displayName || req.session?.email || req.packingDevice?.label || '梱包現場';
}
router.post('/api/floor-alerts', checkOrigin, api(async (req, res) => {
  if (String(req.body.kind || '') !== 'unload') {
    return res.status(400).json({ error: '不明なアラート種別です' });
  }
  const { createFloorAlert } = await import('../picking/service.js');
  res.json({ ok: true, ...createFloorAlert('unload', alertRequester(req)) });
}));
router.get('/api/floor-alerts', api(async (req, res) => {
  const { listFloorAlerts } = await import('../picking/service.js');
  // repickReady = 再ピック完了で受領待ちのタスク (緑バナー「現物を受け取った」の元データ。
  // 通知の既読 (OK) と業務上の受領を混同させない — 2026-08-23)
  res.json({ ok: true, alerts: listFloorAlerts('to_packing'), repickReady: listRepickReady() });
}));
router.post('/api/floor-alerts/:id(\\d+)/ack', checkOrigin, api(async (req, res) => {
  const { ackFloorAlert } = await import('../picking/service.js');
  ackFloorAlert(Number(req.params.id), alertRequester(req), 'to_packing');
  res.json({ ok: true });
}));

// ─── バッチ一覧 ───
router.get('/', (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const batches = listPackBatches(workDate);
  // 引当分類名 (表示) とライン種別 (pas/melt/null — 作業ボタンの振り分け)
  for (const b of batches) {
    const cls = batchClassInfo(getDB(), b);
    b.hikiateClass = cls.name;
    b.classSource = cls.source;   // 'suggested' なら画面で「推定」と分かるようにする
    b.lineKind = lineKindOf(b.hikiateClass);
    b.shortage = shortageSummaryFor(b);   // 🚫在庫なし待ち / ⏳再ピック待ち / 出荷保留 の件数 (PR-2)
  }
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
// Express 4 は async ハンドラの reject を error middleware に渡さない → next(e) で明示的に流す
router.get('/batches/:id(\\d+)', async (req, res, next) => {
  try {
    const batch = getPackBatch(Number(req.params.id));
    if (!batch) return res.status(404).send('バッチが見つかりません');
    const linesBySlip = listPackLinesBySlip(batch.id);
    const slips = listPackSlips(batch.id).map((s) => ({
      ...s,
      warns: s.warn_json ? JSON.parse(s.warn_json) : [],
      comments: s.comments_json ? JSON.parse(s.comments_json) : {},
      lines: linesBySlip.get(s.id) || [],
    }));
    // 作業画面と同じく、表示名はNE商品マスタの単品名を優先 (fail-soft)
    const neNames = await neNamesFor(slips.flatMap((s) => s.lines.map((l) => l.sku)));
    for (const s of slips) {
      for (const l of s.lines) {
        const info = neNames.get(String(l.sku ?? '').trim());
        l.ne_name = info?.name || null;
        l.ne_comps = info?.comps || null;
        l.ne_set_unresolved = !!info?.isSet;
      }
    }
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
  } catch (e) { next(e); }
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
async function decorateSlips(state) {
  const skus = [...new Set(state.slips.flatMap((s) => s.lines.map((l) => l.sku)))];
  const images = getImageMap(skus);
  // 取込時の解決キューが再起動等で消えていても、画面を開いた時点で自己修復する (picking と同じ)
  const missing = skus.map((s) => String(s ?? '').trim().toLowerCase())
    .filter((sku) => sku && !images.has(sku));
  if (missing.length > 0) queueEnsureImages(missing, `packing-batch:${state.batch.id}`);
  // 表示名 = NE商品マスタ名 (warehouse.db raw_ne_products・service-api経由・fail-soft)。
  // 納品書CSVの印字名はセット品受注でモールSEO長文になるため、セットに関係なく単品の社内商品名を優先する
  const neNames = await neNamesFor(skus);
  for (const s of state.slips) {
    for (const l of s.lines) {
      l.imageUrl = images.get(String(l.sku ?? '').trim().toLowerCase())?.url || null;
      const info = neNames.get(String(l.sku ?? '').trim());
      l.ne_name = info?.name || null;     // 必ず単品名 (セット名は warehouse 側で構造的に排除)
      l.ne_comps = info?.comps || null;   // セット展開されたときだけ [{sku, name, qty}]
      l.ne_set_unresolved = !!info?.isSet;   // セットだが展開失敗 → CSV名 (セット名の可能性) を出さない
    }
  }
  return state;
}

// Express 4 は async ハンドラの reject を error middleware に渡さない → next(e) で明示的に流す
router.get('/work/:id(\\d+)', async (req, res, next) => {
  try {
    let state;
    try {
      state = await decorateSlips(getWorkState(Number(req.params.id)));
    } catch (e) {
      if (e instanceof PackError) return res.status(e.status).send(e.message);
      throw e;
    }
    // 引当分類名 (picking の pk_batches.hikiate_class — 参照のみ)。梱包画面の作業方法表示に使う
    let hikiateClass = null;
    if (state.batch.pk_batch_id) {
      try {
        hikiateClass = getDB().prepare('SELECT hikiate_class FROM pk_batches WHERE id = ?')
          .get(state.batch.pk_batch_id)?.hikiate_class ?? null;
      } catch { /* picking未初期化環境では無視 */ }
    }
    // 梱包機バッチは1伝票1画面を使わない — ライン管理画面へ (Codex high: 混在は矛盾状態を作る)
    if (lineKindOf(hikiateClass)) return res.redirect(`/apps/packing/line/${req.params.id}`);
    // 梱包資材の判定 (fail-soft: 失敗してもカード無しで梱包は継続 — 要件§4.1)
    let materials = null;
    let materialOpts = null;
    try {
      materials = materialsForState(state, hikiateClass);
      materialOpts = materialOptions(hikiateClass);
    } catch (e) { console.warn(`[packing-materials] 判定失敗 (fail-soft): ${e.message}`); }
    res.render(path.join(__dirname, 'views/work'), {
      title: `梱包 | ${state.batch.folder_name || state.batch.tb_key}`,
      displayName: req.session?.displayName,
      workers: listWorkers(),
      state,
      hikiateClass,
      warnLabels: WARN_LABELS,
      pauseReasons: PAUSE_REASONS.filter((r) => r !== '配送変更の入力'),   // 自動中断専用の理由は手動メニューに出さない
      undoReasons: UNDO_REASONS,
      shipChangeReasons: SHIP_CHANGE_REASONS,
      // 提案候補 = 固定リスト (中原さん指定 2026-08-16)。判定サービス委譲 (packing-dispatch) はPhase 3
      methodOptions: SHIP_CHANGE_METHOD_OPTIONS,
      twoLabelsOption: SHIP_CHANGE_TWO_LABELS,
      materials,                       // seq → 資材判定 (null = fail-soft)
      materialOpts,                    // { materials: [...], candidateCodes: [...] }
      materialUndoSec: MATERIAL_UNDO_SEC,
    });
  } catch (e) { next(e); }
});

// ─── ライン管理画面 (梱包機 PAS/MELT — 紙台帳の置き換え。要件v7) ───
router.get('/line/:id(\\d+)', (req, res) => {
  const batch = getPackBatch(Number(req.params.id));
  if (!batch) return res.status(404).send('バッチが見つかりません');
  const hikiateClass = batchHikiateClass(getDB(), batch);
  const kind = lineKindOf(hikiateClass);
  if (!kind) return res.redirect(`/apps/packing/work/${batch.id}`);   // 手梱包は従来画面へ
  // 伝票ごとの依頼 (再ピック/配送変更/再印刷) 用に伝票と候補・再ピック状態を渡す (2026-08-31)。
  // 表示に要る列だけに絞る (宛名は事務の突合用に名前まで — 作業画面と同じ範囲)
  let slips = [];
  let incidents = [];
  let repickBySlip = {};
  let stockoutBySlip = {};   // 3階「在庫なし」の報告 (保留伝票 seq → 商品)
  let stockoutAckSeqs = [];  // そのうち「在庫なしを確認」できる伝票
  let stockoutNotifyBySlip = {};   // 閉じた伝票の事務通知の状態 (sent / pending)
  let tasks = [];
  try {
    const st = getWorkState(batch.id);
    slips = st.slips.map((x) => ({
      seq: x.seq, neSlipNo: x.ne_slip_no, slipNo: x.slip_no, siteOrderNo: x.site_order_no || null,
      recipientName: x.recipient_name || null, deliveryMethod: x.delivery_method || null,
      status: x.status, holdReason: x.hold_reason || null,
      pickingShortages: x.pickingShortages || [],   // 🕒/❌ (PR-2: ライン画面にも出す)
      lines: x.lines.map((l) => ({ sku: l.sku, name: l.print_name || l.product_name || l.sku, qty: l.qty })),
    }));
    incidents = st.incidents.map((i) => ({ id: i.id, slipSeq: i.slip_seq, kind: i.kind, sku: i.sku, qty: i.qty }));
    repickBySlip = st.repickBySlip || {};
    stockoutBySlip = st.stockoutBySlip || {};
    stockoutAckSeqs = st.stockoutAckSeqs || [];
    stockoutNotifyBySlip = st.stockoutNotifyBySlip || {};
    // 未完了の再ピックタスク (SKU 単位の状態表示・SKU 単位の「見つかった」用)
    // unavailable も含める = 画面の「依頼済み」判定をサーバーの 409 条件 (生きたタスク) と同じにする (PR-3 Codex R1)。
    // カードの 🔄 行は line.ejs 側で unavailable を除く (🚫 行として stockoutBySlip から出す)
    tasks = getDB().prepare(`SELECT id, slip_seq AS slipSeq, sku, req_qty AS qty, status FROM pk_pack_tasks
      WHERE batch_id=? AND kind='repick' AND status IN ('requested','claimed','fulfilled','unavailable') ORDER BY id`).all(batch.id);
  } catch (e) { console.warn(`[packing] ライン画面の伝票取得失敗 (batch=${batch.id}): ${e.message}`); }
  res.render(path.join(__dirname, 'views/line'), {
    slips,
    incidents,
    repickBySlip,
    stockoutBySlip,
    stockoutAckSeqs,
    stockoutNotifyBySlip,
    tasks,
    methodOptions: SHIP_CHANGE_METHOD_OPTIONS,
    twoLabelsOption: SHIP_CHANGE_TWO_LABELS,
    shipChangeReasons: SHIP_CHANGE_REASONS,
    pauseReasons: PAUSE_REASONS.filter((r) => r !== '配送変更の入力'),   // 一時中断の理由 (自動中断専用は出さない)
    title: `ライン | ${batch.folder_name || batch.tb_key}`,
    displayName: req.session?.displayName,
    workers: listWorkers(),
    batch,
    kind,
    hikiateClass,
    runs: listLineRuns(batch.id),
    // 本日×同ラインの累計 (梱包機トータルカウンタとの突合用。日付でリセット)
    dailyTotal: lineDailyTotal(batch.work_date, kind),
    statusLabels: STATUS_LABELS,
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
  // 品違いの実物SKUは記録時にサーバー検証 (形式+在庫実在・名前はサーバー由来 — 2026-08-23)。
  // 終了画面で再入力させない代わりに、ここで確定させる
  let actualSku = req.body.actual_sku || null;
  let actualName = null;
  if (req.body.event === 'wrong_item') ({ sku: actualSku, name: actualName } = await verifyActualSku(actualSku));
  // 余りの SKU も同じ検証 (自由入力の商品名断片を棚戻しに流さない — 例外処理監査 D-1・PR-4)。名前は actualName に載せる
  let excessSku = null;
  if (req.body.event === 'excess') ({ sku: excessSku, name: actualName } = await verifyActualSku(req.body.sku, { label: '余った商品' }));
  const result = applyEvent(Number(req.params.id), {
    opId: req.body.op_id,
    event: req.body.event,
    slipSeq: req.body.slip_seq == null ? null : Number(req.body.slip_seq),
    clientAt: req.body.client_at || null,
    reason: req.body.reason || null,
    jumped: !!req.body.jumped,
    proposedMethod: req.body.proposed_method || null,
    sku: excessSku ?? (req.body.sku || null),
    actualSku,
    actualName,
    qty: req.body.qty == null ? null : Number(req.body.qty),
    // '' は未入力として null に落とす (Number('')===0 で「0件」と誤解釈しない — Codex medium)
    finalCount: req.body.final_count == null || req.body.final_count === '' ? null : Number(req.body.final_count),
    manualCount: req.body.manual_count == null || req.body.manual_count === '' ? null : Number(req.body.manual_count),
    excludedCount: req.body.excluded_count == null || req.body.excluded_count === '' ? null : Number(req.body.excluded_count),
    toPasCount: req.body.to_pas_count == null || req.body.to_pas_count === '' ? null : Number(req.body.to_pas_count),
    note: req.body.note == null ? null : String(req.body.note).slice(0, 200),
  }, worker);
  // ⑤ Notionカード自動移動 (fail-soft・非同期直列化。送信直前に最新状態を読むため
  // どのイベント経由でも「現在のバッチ状態」が届く)。対象=バッチ状態が変わり得るイベントのみ。
  // shortage/wrong_item は完了済みバッチを packing へ再オープンする経路がある (Codex P1)
  if (!result.replayed
      && ['start', 'next', 'undo', 'takeover', 'shortage', 'wrong_item',
        'line_sort_start', 'line_sort_done', 'line_start', 'line_done'].includes(req.body.event)) {
    enqueuePackBatchNotionSync(Number(req.params.id));
  }
  // ①②のGChat通知 (fail-soft・DBのタスク行が正本。replayでは taskNotify が付かない=再送しない)
  if (result.taskNotify?.kind === 'stockout') {
    // 🚫 在庫なしを確認 → 事務へ (NE で出荷保留にしてもらう)。outbox (pk_pack_stockouts) の行が正本で、
    // 送れたときだけ notified_at。失敗・未設定はポーラーが再送する。成否は画面にも返す
    const n = result.taskNotify;
    // 送信権を取ってから送る (ポーラーの再送と同じ行を同時に送らない)。取れなければポーラーに任せる
    if (!claimStockoutNotify(n.stockoutId)) {
      result.stockoutNotify = 'pending';
    } else {
      try {
        const sent = await notifyStockout({ ...n, worker });
        markStockoutNotify(n.stockoutId, sent, sent ? null : 'webhook未設定');
        result.stockoutNotify = sent ? 'sent' : 'failed';
      } catch (e) {
        console.warn(`[packing-notify] 出荷保留 (在庫なし) 通知失敗 (${n.neSlipNo}): ${e.message}`);
        markStockoutNotify(n.stockoutId, false, e.message);
        result.stockoutNotify = 'failed';
      }
    }
  } else if (result.taskNotify) {
    notifyTask(result.taskNotify, worker)
      .catch((e) => console.warn(`[packing-notify] タスク通知失敗 (${result.taskNotify.sku}): ${e.message}`));
  }
  // 🖨 再印刷依頼: 押した瞬間に即時通知 (中原さん指示 2026-08-21・梱包の途中でも飛ぶ)。
  // テキスト通知を先に送り、送り状PDFの抜き出しは非同期で追送 (Codex high: Drive処理を
  // await すると「即時」にならず応答も塞ぐ)。webhook成功→DB更新間のクラッシュでは
  // 再送により同内容が重複し得る (NE伝票番号併記で判別可能・webhookにexactly-onceは無い)
  if (['reprint', 'label_missing'].includes(req.body.event) && !result.replayed && result.reprintId) {
    const row = getDB().prepare('SELECT * FROM pk_pack_reprints WHERE id=?').get(result.reprintId);
    if (row) {
      const lines = getDB().prepare(`
        SELECT COALESCE(l.print_name, l.product_name) AS name, l.sku, l.qty
        FROM pk_pack_lines l JOIN pk_pack_slips s ON s.id = l.slip_id
        WHERE s.batch_id=? AND s.seq=? ORDER BY l.id
      `).all(row.batch_id, row.slip_seq);
      try {
        const sent = await notifyReprint({
          kind: row.kind, folderName: row.folder_name, slipSeq: row.slip_seq, neSlipNo: row.ne_slip_no,
          siteOrderNo: row.site_order_no, recipientName: row.recipient_name, worker, lines,
        });
        getDB().prepare('UPDATE pk_pack_reprints SET notified_at=?, notify_error=? WHERE id=?')
          .run(sent ? new Date().toISOString().slice(0, 19) + 'Z' : null, sent ? null : 'webhook未設定', row.id);
        if (!sent) result.reprintNotify = 'failed';
      } catch (e) {
        console.warn(`[packing-notify] 再印刷通知失敗 (${row.ne_slip_no}): ${e.message}`);
        getDB().prepare('UPDATE pk_pack_reprints SET notify_error=? WHERE id=?')
          .run(String(e.message).slice(0, 200), row.id);
        result.reprintNotify = 'failed';
      }
      // 送り状PDFの抜き出し→追送 (fire-and-forget・fail-soft)
      (async () => {
        try {
          const batch = getPackBatch(row.batch_id);
          // 出力先は**引当分類 (送り状発行ソフト) ごと**に決まる (ヤマトB2 / DENZOU /
          // ゆうプリR / 汎用送り状 で物理プリンターが違う)。フォルダの okurijo_<slug>_*.csv から読む。
          // ⭐PDFを作る前に決める — ラベル実寸に収めてよいかが引当分類で変わるため
          const slug = await detectOkurijoSlug(row.folder_name);
          const r = await extractReprintPdf({
            folderName: row.folder_name, neSlipNo: row.ne_slip_no, recipientName: row.recipient_name,
            siteOrderNo: row.site_order_no,
            slipSeq: row.slip_seq, slipCount: batch?.slip_count ?? null,
            batchCreatedAt: batch?.created_at ?? null, slug,
          });
          const url = `https://picking.bfaith-wh.uk/apps/packing/reprints/${r.token}.pdf`;
          getDB().prepare('UPDATE pk_pack_reprints SET pdf_token=?, pdf_by=?, pdf_printable=?, pdf_ink_ratio=? WHERE id=?')
            .run(r.token, r.by, r.printable ? 1 : 0, r.inkRatio ?? null, row.id);
          // 🖨 自動印刷に載せてよいのは manifest 経路+白紙検査を通ったものだけ。
          // その条件は enqueuePrintJob の SQL が上の UPDATE 済みの行を直接見て判定する
          // (呼び出し側の if に任せると、入口が増えたとき位置推定のPDFが積まれる)。
          // 位置推定で見つけた分は今までどおり**リンクだけ**渡して人が見て印刷する
          const queued = enqueuePrintJob(row.id, { pdfSha256: r.sha256, slug });
          if (queued && !queued.id) {
            console.warn(`[packing-reprint] ${row.ne_slip_no}: 自動印刷しません (${queued.reason} slug=${queued.slug ?? '-'})`);
          }
          await postReprintText(queued?.id
            ? `🖨 ${row.ne_slip_no} の送り状を印刷します (${queued.printer})。念のためのリンク: ${url}`
            : `📄 ${row.ne_slip_no} の送り状PDF (該当ページのみ・${r.file}): ${url}`);
        } catch (e) {
          getDB().prepare('UPDATE pk_pack_reprints SET pdf_error=? WHERE id=?')
            .run(String(e.message).slice(0, 120), row.id);
          // 白紙・中身なしは「探せなかった」ではなく**そのページが使えない**。
          // 印刷しても白紙が出るだけなので、リンクを渡さずはっきりエラーとして知らせる
          const msg = e instanceof LabelUnusableError
            ? `📄 ${row.ne_slip_no} の送り状PDFがエラーです (${String(e.message).slice(0, 90)}) — 印刷していません。元の送り状PDFを確認してください`
            : `⚠ ${row.ne_slip_no} の送り状PDFは自動で抜き出せませんでした (${String(e.message).slice(0, 80)}) — フォルダから該当分を印刷してください`;
          postReprintText(msg).catch(() => {});
        }
      })().catch((e) => console.warn(`[packing-reprint] PDF追送失敗 (${row.ne_slip_no}): ${e.message}`));
    }
  }
  // ④ 配送方法変更は事務へ GChat 通知。事務キュー廃止後は通知が実質の伝達経路なので、
  // 成否を行に記録し (失敗はポーラーが再送)、失敗は現場にも表示する (Codexレビュー high)
  if (req.body.event === 'ship_change' && !result.replayed) {
    const row = getDB().prepare(
      'SELECT * FROM pk_pack_ship_changes WHERE batch_id=? AND slip_seq=? ORDER BY id DESC LIMIT 1'
    ).get(Number(req.params.id), Number(req.body.slip_seq));
    if (row) {
      const lines = getDB().prepare(`
        SELECT COALESCE(l.print_name, l.product_name) AS name, l.sku, l.qty
        FROM pk_pack_lines l JOIN pk_pack_slips s ON s.id = l.slip_id
        WHERE s.batch_id=? AND s.seq=? ORDER BY l.id
      `).all(Number(req.params.id), Number(req.body.slip_seq));
      try {
        // 出荷伝票NO (SP…) = ヤマトB2の お客様管理番号。元の送り状を消すのに要る
        const slipNo = getDB().prepare(
          'SELECT slip_no FROM pk_pack_slips WHERE batch_id=? AND seq=?'
        ).get(row.batch_id, row.slip_seq)?.slip_no ?? null;
        const sent = await notifyShipChange({
          folderName: row.folder_name, neSlipNo: row.ne_slip_no, slipNo,
          currentMethod: row.current_method, proposedMethod: row.proposed_method,
          reason: row.reason, worker, lines,
        });
        getDB().prepare('UPDATE pk_pack_ship_changes SET notified_at=?, notify_error=? WHERE id=?')
          .run(sent ? new Date().toISOString().slice(0, 19) + 'Z' : null,
            sent ? null : 'webhook未設定', row.id);
        if (!sent) result.shipNotify = 'failed';
      } catch (e) {
        console.warn(`[packing-notify] 配送変更通知失敗 (${row.ne_slip_no}): ${e.message}`);
        getDB().prepare('UPDATE pk_pack_ship_changes SET notify_error=? WHERE id=?')
          .run(String(e.message).slice(0, 200), row.id);
        result.shipNotify = 'failed';
      }
    }
  }
  res.json({ ok: true, ...result });
}));

/** ③ミス候補の確定/取下げ (終了画面から。確定=送信+picking担当へ帰責)。 */
router.post('/api/batches/:id(\\d+)/incidents/:iid(\\d+)/resolve', checkOrigin, api(async (req, res) => {
  // 作業者はイベントAPIと同じ検証 (pk_workers の有効名 or セッション本人 — Codex high)
  const name = String(req.body.worker_name || '').trim();
  let actor = null;
  if (name) {
    if (!listWorkers().some((w) => w.name === name)) {
      throw new PackError(400, 'bad_worker', '作業者が無効です。選び直してください');
    }
    actor = name;
  } else if (req.session?.email) {
    actor = req.session.displayName || req.session.email;
  } else {
    throw new PackError(400, 'no_worker', '作業者を選択してください');
  }
  const decision = String(req.body.decision || '');
  // 品違いの実物SKUはサーバー側で検証 (Codexレビュー: クライアント任意文字列を信用しない)。
  // 記録時に検証済みでも、確定時は「実際に使われるSKU」を必ず再検証する (保存済み値での迂回防止 — Codex 2巡目)
  let actualSku = req.body.actual_sku == null ? null : String(req.body.actual_sku).trim().toLowerCase();
  let actualName = null;
  if (decision === 'confirm' && !actualSku) {
    const incRow = getDB().prepare('SELECT kind, actual_sku FROM pk_pack_incidents WHERE id=?')
      .get(Number(req.params.iid));
    if (incRow?.kind === 'wrong_item' && incRow.actual_sku) {
      actualSku = String(incRow.actual_sku).trim().toLowerCase();
    }
  }
  if (actualSku) ({ sku: actualSku, name: actualName } = await verifyActualSku(actualSku));
  const inc = resolveIncident(Number(req.params.iid), decision, actor, Number(req.params.id), {
    actualSku, actualName,
  });
  // 送信 (confirm) したタスクの後処理 (fail-soft・DBの行が正本):
  //   再ピック → 🔴ピッキング漏れバッチを生成 (picking所有の書き込みは picking service 経由) + ロケつき通知
  //   棚戻し → その商品の在庫ロケーション (ロジザード) を通知に載せる (戻し先の参考 — 2026-08-21)
  if (inc.dispatchedTasks?.length) {
    const taskRows = getDB().prepare(
      'SELECT * FROM pk_pack_tasks WHERE incident_id=? ORDER BY id').all(inc.id);
    let picking = null;
    try { picking = await import('../picking/service.js'); } catch { /* picking無効環境 */ }
    let stock = null;
    try { stock = await import('../picking/stock-locations.js'); } catch { /* 同上 */ }
    for (const t of taskRows) {
      if (t.kind === 'repick') {
        // 再ピックはGChat通知しない (中原さん指示 2026-08-21 — 🔴バッチ+タスク画面で完結し、
        // 完了時に梱包ヘッダーへバナーが出る)。バッチ生成のみ行う
        try {
          if (picking?.createRepickBatch) picking.createRepickBatch(t);
        } catch (e) { console.warn(`[packing] ピッキング漏れバッチ作成失敗 (task=${t.id}): ${e.message}`); }
        continue;
      }
      const info = { kind: t.kind, sku: t.sku, name: t.product_name, qty: t.req_qty, folder: t.folder_name, slipSeq: t.slip_seq };
      if (stock?.stockLookupConfigured?.()) {
        try {
          const data = await stock.fetchStockLocations(t.sku);
          info.stockText = stock.buildStockLocationsText(data, { title: '在庫ロケーション (戻し先の参考)' });
          // 取った場所が分からない棚戻し (余り・バッチ外の品違い = pk_lines に無い) は、ロジザードの在庫ロケ
          // (良品・フリー在庫の多い順) の先頭を参考ロケとして持たせる (例外処理監査 D-2・PR-4)。画面は候補を全部出す
          if (!t.location) {
            const top = stock.listStockCandidates(data, { groupByLocation: true, maxRows: 1 }).rows[0];
            if (top) setTaskLocationHint(t.id, { block: top.block, location: top.location, source: 'stock' });
          }
        } catch { /* fail-soft */ }
      }
      notifyTask(info, actor).catch((e) => console.warn(`[packing-notify] タスク通知失敗 (${t.sku}): ${e.message}`));
    }
  }
  res.json({ ok: true, id: inc.id, status: inc.status, attributedWorker: inc.attributed_worker });
}));

/** 商品検索 (品違いの現物特定用)。在庫検索ボットと同じ warehouse service-api を参照 (fail-soft)。 */
router.get('/api/stock-search', api(async (req, res) => {
  const q = String(req.query.q || '').trim();
  if (q.length < 2) return res.json({ ok: true, items: [] });
  try {
    const { fetchStockSearch, stockLookupConfigured } = await import('../picking/stock-locations.js');
    if (!stockLookupConfigured()) return res.json({ ok: true, items: [], disabled: true });
    const data = await fetchStockSearch(q);
    res.json({ ok: true, items: (data?.items || []).slice(0, 10) });
  } catch (e) {
    res.json({ ok: true, items: [], error: e.message });
  }
}));

/**
 * 品違いの「実際に入っていた商品」SKUをサーバー側で検証する (クライアント任意文字列を信用しない)。
 * 形式チェック + 在庫データへの実在照会。名前はサーバー由来のみ採用 (通知への注入防止)。
 * 在庫参照の一時障害では形式チェックのみで通す (現場の記録・送信を止めない・fail-soft)。
 * 記録時 (wrong_item イベント) と確定時 (incidents/resolve) の両方で使う。
 * @returns {{sku: string, name: string|null}}
 */
async function verifyActualSku(raw, { label = '間違って入っていた商品' } = {}) {
  const sku = String(raw ?? '').trim().toLowerCase();
  if (!sku) throw new PackError(400, 'actual_sku_required', `${label}を検索で特定してください`);
  if (!/^[a-z0-9][a-z0-9_\-.]{0,79}$/.test(sku)) {
    throw new PackError(400, 'bad_sku', 'SKUの形式が不正です。検索から選んでください');
  }
  let name = null;
  try {
    const { fetchStockSearch, stockLookupConfigured } = await import('../picking/stock-locations.js');
    if (stockLookupConfigured()) {
      const hit = (await fetchStockSearch(sku))?.items?.find((it) => String(it.sku).toLowerCase() === sku);
      if (!hit) throw new PackError(400, 'unknown_sku', 'そのSKUは在庫データに見つかりません。検索から選び直してください');
      name = hit.name || null;
    }
  } catch (e) {
    if (e instanceof PackError) throw e;
    console.warn(`[packing] 実物SKUの在庫照会失敗 (形式チェックのみで続行): ${e.message}`);
  }
  return { sku, name };
}

// ─── 📦 梱包資材 (表示・現場登録 — 要件『梱包資材表示_要件定義_20260823.md』v1.7) ───

const MATERIALS_DIR = path.join(DATA_DIR, 'materials');

/** 資材画像の配信 (端末認証の内側・immutable — 差し替えはファイル名が変わる)。 */
router.get('/materials/:file([a-z0-9_-]+\\.(?:jpg|jpeg|png|webp))', (req, res) => {
  const file = path.join(MATERIALS_DIR, req.params.file);
  if (!file.startsWith(MATERIALS_DIR) || !fs.existsSync(file)) return res.status(404).send('not found');
  res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.sendFile(file);
});

/** 登録・変更グリッド用の資材一覧 (active・分類候補コード付き)。 */
router.get('/api/batches/:id(\\d+)/material-options', api(async (req, res) => {
  const batch = getPackBatch(Number(req.params.id));
  if (!batch) throw new PackError(404, 'batch_not_found', 'バッチがありません');
  const hikiateClass = batchHikiateClass(getDB(), batch);
  res.json({ ok: true, ...materialOptions(hikiateClass), undoSec: MATERIAL_UNDO_SEC });
}));

/**
 * 資材の登録/変更。body: { op_id, slip_seq, material_code,
 *   expected_rule_id, expected_version, expected_delivery_code, expected_before, worker_name }
 * 409 = 現在値同梱 (conflict / context_changed) — 画面はカードを描き直す (§5.1)
 */
router.post('/api/batches/:id(\\d+)/material', checkOrigin, api(async (req, res) => {
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
  if (!worker) throw new PackError(400, 'no_worker', '作業者を選択してください');
  // CAS 契約の厳密検証 (要件§5.1・Codex 実装R1 high): 「欠落」と「明示 null」を区別する。
  // expected_rule_id / expected_version は両方 null (未登録表示) か両方正整数、
  // expected_delivery_code は必須。数値項目は正整数のみ
  const b = req.body || {};
  for (const k of ['expected_rule_id', 'expected_version', 'expected_delivery_code']) {
    if (!(k in b)) throw new PackError(400, 'bad_contract', `${k} がありません (画面を更新してください)`);
  }
  const posInt = (v) => Number.isInteger(v) && v > 0;
  const ruleId = b.expected_rule_id;
  const ruleVer = b.expected_version;
  if ((ruleId == null) !== (ruleVer == null) || (ruleId != null && (!posInt(ruleId) || !posInt(ruleVer)))) {
    throw new PackError(400, 'bad_contract', 'expected_rule_id / expected_version が不正です');
  }
  if (!posInt(b.slip_seq)) throw new PackError(400, 'bad_slip_seq', 'slip_seq が不正です');
  if (typeof b.op_id !== 'string' || b.op_id.length < 1 || b.op_id.length > 80) {
    throw new PackError(400, 'bad_op_id', 'op_id が不正です');
  }
  const opId = b.op_id;
  if (!b.material_code || typeof b.material_code !== 'string' || b.material_code.length > 40) {
    throw new PackError(400, 'bad_material', 'material_code が不正です');
  }
  if (typeof b.expected_delivery_code !== 'string' || !b.expected_delivery_code || b.expected_delivery_code.length > 30) {
    throw new PackError(400, 'bad_contract', 'expected_delivery_code が不正です');
  }
  const result = registerMaterial({
    batchId: Number(req.params.id),
    slipSeq: b.slip_seq,
    materialCode: b.material_code,
    expectedRuleId: ruleId == null ? null : ruleId,
    expectedVersion: ruleVer == null ? null : ruleVer,
    expectedDeliveryCode: b.expected_delivery_code,
    expectedBefore: b.expected_before == null ? null : String(b.expected_before).slice(0, 40),
    opId,
    worker,
  });
  res.json(result);
}));

/** 資材登録の取り消し (猶予内・§5.2)。body: { op_id, event_id, undo_token, worker_name } */
router.post('/api/batches/:id(\\d+)/material/undo', checkOrigin, api(async (req, res) => {
  // worker は登録APIと同じ検証 (任意文字列を監査ログに入れない — Codex 実装R1 medium)
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
  if (!Number.isInteger(req.body.event_id) || req.body.event_id < 1) {
    throw new PackError(400, 'bad_event', 'event_id が不正です');
  }
  if (typeof req.body.op_id !== 'string' || req.body.op_id.length < 1 || req.body.op_id.length > 80) {
    throw new PackError(400, 'bad_op_id', 'op_id が不正です');
  }
  const result = undoMaterial({
    opId: req.body.op_id,
    eventId: req.body.event_id,
    undoToken: String(req.body.undo_token || ''),
    worker,
    batchId: Number(req.params.id),   // 別バッチURLからの取り消しを拒否
  });
  res.json(result);
}));

/** 作業状態の再取得 (リロード・オンライン復帰時の同期用)。 */
router.get('/api/batches/:id(\\d+)/state', api(async (req, res) => {
  const s = getWorkState(Number(req.params.id));
  // 資材判定も同期する (④/受領/構成変化の再照合・fail-soft)
  let materials = null;
  try {
    const batch = getPackBatch(Number(req.params.id));
    materials = materialsForState(s, batchHikiateClass(getDB(), batch));
  } catch { /* fail-soft */ }
  res.json({
    materials,
    ok: true,
    batchStatus: s.batch.status,
    worker: s.batch.worker,
    currentSeq: s.currentSeq,
    doneCount: s.doneCount,
    slipCount: s.slips.length,
    doneSeqs: s.slips.filter((x) => x.status === 'done').map((x) => x.seq),
    heldSeqs: s.slips.filter((x) => x.status === 'held').map((x) => x.seq),
    repickSeqs: s.slips.filter((x) => x.status === 'held' && x.hold_reason === 'repick').map((x) => x.seq),
    repickUnavailableSeqs: Object.keys(s.stockoutBySlip || {}).map(Number),
    stockoutBySlip: s.stockoutBySlip || {},
    stockoutAckSeqs: s.stockoutAckSeqs || [],
    stockoutSeqs: s.slips.filter((x) => x.status === 'cancelled' && x.hold_reason === 'stockout').map((x) => x.seq),
    stockoutNotifyBySlip: s.stockoutNotifyBySlip || {},
    incidents: s.incidents,
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
// ─── 実績ボード (2026-08-31 中原さん指示: 梱包にも「実績」— 梱包スピードをダッシュボードで) ───
// picking の /board と同じ設計。端末Cookie (iPad) でもセッションでも見られる (packingAccess)。
function parseDays(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return PACK_STATS_WINDOW_DAYS;
  return Math.min(365, Math.max(1, Math.round(n)));
}
router.get('/board', (req, res) => {
  res.render(path.join(__dirname, 'views/board'), {
    title: '梱包実績ボード',
    windowDays: PACK_STATS_WINDOW_DAYS,
  });
});
router.get('/api/board', api(async (req, res) => {
  const stats = getPackingStats({ days: parseDays(req.query.days) });
  res.set('Cache-Control', 'no-store');
  res.json({
    ok: true,
    now: new Date().toISOString(),
    today: getTodayPackingProgress(),
    stats: {
      since: stats.since, until: stats.until, days: stats.days,
      minDate: PACK_STATS_MIN_DATE,
      minSlips: stats.minSlips,
      minClassSlips: stats.minClassSlips,
      outlierSec: stats.outlierSec,
      total: stats.total,
      // 掲示は伝票数上位の分類のみ (ヒートマップの行数 = 画面に収まる範囲)
      baseline: stats.baseline.slice(0, 12).map((c) => ({
        key: c.key, slips: c.slips, avgSec: c.avgSec, workerCount: c.workerCount,
        workers: c.workers.map((w) => ({
          worker: w.worker, name: w.name, slips: w.slips, secPerSlip: w.secPerSlip,
          index: w.index, provisional: w.provisional,
        })),
      })),
      workers: stats.workers.map((w) => ({
        worker: w.worker, name: w.name, slips: w.slips, secPerSlip: w.secPerSlip,
        index: w.index, provisional: w.provisional, batches: w.batches, days: w.days, excluded: w.excluded,
      })),
      byDate: stats.byDate,
    },
  });
}));

router.get('/admin/summary', requireAdmin, (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  res.render(path.join(__dirname, 'views/admin_summary'), {
    title: '梱包サマリ',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    summary: getDailySummary(workDate),
    // 資材の登録/変更/取消/通知失敗 (初回登録は GChat 通知しない代わりにここで確認 — 要件§5.3)
    materialCounts: materialDailyCounts(workDate),
    statusLabels: STATUS_LABELS,
  });
});

// ─── 📦 資材の管理画面 (要件§9・requireAdmin は prefix 一括) ───

const uploadImage = multer({ storage: multer.memoryStorage(), limits: { fileSize: 2 * 1024 * 1024 } });

/**
 * 画像のマジックバイト+寸法ヘッダ検証 (拡張子/MIME 申告は信用しない)。SVG は不可 (要件§6.3)。
 * 完全デコードはしない (画像ライブラリを増やさない方針) — 形式・寸法上限 (4000px) までを防御し、
 * 表示は <img>/background-image のみ (ブラウザのデコーダ任せ・サーバーでは処理しない)
 */
const IMAGE_MAX_DIM = 4000;
function sniffImage(buf) {
  if (!buf || buf.length < 32) return null;
  const ok = (w, h, kind) => (w > 0 && h > 0 && w <= IMAGE_MAX_DIM && h <= IMAGE_MAX_DIM ? kind : null);
  const PNG_SIG = Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]);
  if (buf.slice(0, 8).equals(PNG_SIG)) {
    if (buf.slice(12, 16).toString('ascii') !== 'IHDR') return null;
    return ok(buf.readUInt32BE(16), buf.readUInt32BE(20), 'png');
  }
  if (buf[0] === 0xFF && buf[1] === 0xD8 && buf[2] === 0xFF) {
    // JPEG: SOF0/1/2 マーカーを走査して寸法を読む
    let i = 2;
    while (i + 9 < buf.length) {
      if (buf[i] !== 0xFF) return null;
      const marker = buf[i + 1];
      const len = buf.readUInt16BE(i + 2);
      if (marker >= 0xC0 && marker <= 0xC2) return ok(buf.readUInt16BE(i + 7), buf.readUInt16BE(i + 5), 'jpg');
      if (len < 2) return null;
      i += 2 + len;
    }
    return null;
  }
  if (buf.slice(0, 4).toString('ascii') === 'RIFF' && buf.slice(8, 12).toString('ascii') === 'WEBP') {
    const fmt = buf.slice(12, 16).toString('ascii');
    if (fmt === 'VP8 ' && buf.length >= 30) return ok(buf.readUInt16LE(26) & 0x3FFF, buf.readUInt16LE(28) & 0x3FFF, 'webp');
    if (fmt === 'VP8L' && buf.length >= 25) {
      const b0 = buf.readUInt32LE(21);
      return ok((b0 & 0x3FFF) + 1, ((b0 >> 14) & 0x3FFF) + 1, 'webp');
    }
    if (fmt === 'VP8X' && buf.length >= 30) {
      const w = 1 + (buf[24] | (buf[25] << 8) | (buf[26] << 16));
      const h = 1 + (buf[27] | (buf[28] << 8) | (buf[29] << 16));
      return ok(w, h, 'webp');
    }
    return null;
  }
  return null;
}

function adminActor(req) { return req.session?.displayName || req.session?.email || 'admin'; }

/** 管理画面の rules 変更 (admin_edit / admin_disable) の監査イベント。 */
function insertAdminRuleEvent(db, action, rule, beforeCode, admin) {
  db.prepare(`
    INSERT INTO pk_pack_material_events
      (op_id, request_hash, action, rule_id, rule_version, combo_key, delivery_code,
       before_code, after_code, worker, created_at, notify_status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'none')
  `).run(
    `admin-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`, 'admin', action,
    rule.id, rule.version, rule.combo_key, rule.delivery_code,
    beforeCode, rule.material_code, admin, utcNowRouter(),
  );
}
function utcNowRouter() { return new Date().toISOString().slice(0, 19) + 'Z'; }

router.get('/admin/materials', requireAdmin, (req, res) => {
  const db = getDB();
  const q = String(req.query.q || '').trim();
  const materials = db.prepare('SELECT * FROM pk_pack_materials ORDER BY sort_order, code').all();
  const classes = db.prepare('SELECT * FROM pk_pack_classes ORDER BY sort_order, class_value').all()
    .map((c) => ({
      ...c,
      candidates: db.prepare(`
        SELECT cm.material_code, m.name, m.is_active FROM pk_pack_class_materials cm
        LEFT JOIN pk_pack_materials m ON m.code = cm.material_code
        WHERE cm.class_value = ? ORDER BY cm.sort_order
      `).all(c.class_value),
    }));
  const headerMap = db.prepare(`
    SELECT h.*, m.name AS material_name FROM pk_pack_header_map h
    LEFT JOIN pk_pack_materials m ON m.code = h.material_code ORDER BY h.header_value
  `).all();
  // 未レビュー: ①観測された header が辞書に無い ②観測された分類がマスタに無い
  // ③AES ヘッダで観測された分類の aes_kind 未設定 (要件§9-3・Codex 6巡目)
  const unreviewedHeaders = db.prepare(`
    SELECT header_raw, COUNT(*) c, MAX(last_shown_at) last_at FROM pk_pack_material_views
    WHERE header_raw IS NOT NULL AND header_raw <> ''
      AND header_raw NOT IN (SELECT header_value FROM pk_pack_header_map)
    GROUP BY header_raw ORDER BY c DESC LIMIT 50
  `).all();
  const unreviewedClasses = db.prepare(`
    SELECT hikiate_class, COUNT(*) c, MAX(last_shown_at) last_at FROM pk_pack_material_views
    WHERE hikiate_class IS NOT NULL AND hikiate_class <> ''
      AND (hikiate_class NOT IN (SELECT class_value FROM pk_pack_classes)
           OR (header_raw IN (SELECT header_value FROM pk_pack_header_map WHERE base_delivery_code='aes')
               AND hikiate_class IN (SELECT class_value FROM pk_pack_classes WHERE aes_kind IS NULL)))
    GROUP BY hikiate_class ORDER BY c DESC LIMIT 50
  `).all();
  const rules = db.prepare(`
    SELECT r.*, m.name AS material_name, m.is_active AS material_active,
      (SELECT COUNT(*) FROM pk_pack_material_events e
        WHERE e.rule_id = r.id AND e.action='change' AND e.undone_at IS NULL
          AND e.created_at >= datetime('now', '-7 days')) AS recent_changes
    FROM pk_pack_material_rules r LEFT JOIN pk_pack_materials m ON m.code = r.material_code
    ${q ? "WHERE r.combo_key LIKE @like OR r.combo_detail LIKE @like OR r.material_code LIKE @like" : ''}
    ORDER BY r.updated_at DESC LIMIT 100
  `).all(q ? { like: `%${q}%` } : {});
  const events = db.prepare(`
    SELECT e.*, m.name AS after_name FROM pk_pack_material_events e
    LEFT JOIN pk_pack_materials m ON m.code = e.after_code
    ORDER BY e.id DESC LIMIT 60
  `).all();
  const notifyIssues = db.prepare(`
    SELECT * FROM pk_pack_material_events
    WHERE notify_status IN ('pending','sending','failed') ORDER BY id DESC LIMIT 30
  `).all();
  const webhookConfigured = !!(process.env.PACKING_MATERIAL_WEBHOOK || process.env.PACKING_SHIP_CHANGE_WEBHOOK);
  res.render(path.join(__dirname, 'views/admin_materials'), {
    undoSecretConfigured: UNDO_SECRET_CONFIGURED,
    title: '梱包資材の管理',
    username: req.session.email,
    displayName: req.session.displayName,
    q, materials, classes, headerMap, unreviewedHeaders, unreviewedClasses,
    rules, events, notifyIssues, webhookConfigured,
    baseDeliveryCodes: BASE_DELIVERY_CODES,
  });
});

/** 資材マスタの追加・編集。body: { code, name, color, sort_order, is_active } */
router.post('/admin/materials/master', checkOrigin, requireAdmin, api(async (req, res) => {
  const code = String(req.body.code || '').trim();
  const name = String(req.body.name || '').trim();
  if (!/^[a-z0-9_]{2,40}$/.test(code)) throw new PackError(400, 'bad_code', 'code は英小文字/数字/_ (2〜40文字)');
  if (!name || name.length > 60) throw new PackError(400, 'bad_name', '名前は必須です (60文字まで)');
  const colorIn = String(req.body.color || '').trim();
  if (colorIn && !/^#[0-9a-fA-F]{6}$/.test(colorIn)) {
    throw new PackError(400, 'bad_color', '色は #rrggbb 形式で入力してください');
  }
  const now = utcNowRouter();
  getDB().prepare(`
    INSERT INTO pk_pack_materials (code, name, color, sort_order, is_active, created_at, updated_at, updated_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(code) DO UPDATE SET name=excluded.name, color=excluded.color, sort_order=excluded.sort_order,
      is_active=excluded.is_active, updated_at=excluded.updated_at, updated_by=excluded.updated_by
  `).run(code, name, colorIn || null,
    Math.min(9999, Math.max(0, Number(req.body.sort_order) || 100)),
    req.body.is_active === '0' ? 0 : 1, now, now, adminActor(req));
  res.json({ ok: true });
}));

/** 資材画像のアップロード/差し替え。multipart: code + file (png/jpg/webp・2MB・マジックバイト検証)。 */
router.post('/admin/materials/master/image', checkOrigin, requireAdmin, (req, res, next) => {
  uploadImage.single('file')(req, res, (err) => {
    if (err) return res.status(400).json({ error: `アップロード失敗: ${err.message}` });
    next();
  });
}, api(async (req, res) => {
  const code = String(req.body.code || '').trim();
  const db = getDB();
  const m = db.prepare('SELECT code FROM pk_pack_materials WHERE code = ?').get(code);
  if (!m) throw new PackError(404, 'not_found', '資材がありません');
  const kind = sniffImage(req.file?.buffer);
  if (!kind) throw new PackError(400, 'bad_image', 'png / jpg / webp のみアップロードできます (SVG不可)');
  fs.mkdirSync(MATERIALS_DIR, { recursive: true });
  const name = `${code}-${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}.${kind}`;
  fs.writeFileSync(path.join(MATERIALS_DIR, name), req.file.buffer);
  db.prepare('UPDATE pk_pack_materials SET image_file=?, updated_at=?, updated_by=? WHERE code=?')
    .run(name, utcNowRouter(), adminActor(req), code);
  res.json({ ok: true, image_file: name });
}));

/** 分類マスタの追加・編集。body: { class_value, aes_kind, hide_card, sort_order } */
router.post('/admin/materials/class', checkOrigin, requireAdmin, api(async (req, res) => {
  const cv = normalizeKeyText(req.body.class_value);
  if (!cv || cv.length > 120) throw new PackError(400, 'bad_class', '分類名は必須です (120文字まで)');
  const aesKind = ['mail', 'other'].includes(req.body.aes_kind) ? req.body.aes_kind : null;
  const now = utcNowRouter();
  getDB().prepare(`
    INSERT INTO pk_pack_classes (class_value, aes_kind, hide_card, sort_order, updated_at, updated_by)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(class_value) DO UPDATE SET aes_kind=excluded.aes_kind, hide_card=excluded.hide_card,
      sort_order=excluded.sort_order, updated_at=excluded.updated_at, updated_by=excluded.updated_by
  `).run(cv, aesKind, req.body.hide_card === '1' ? 1 : 0,
    Math.min(9999, Math.max(0, Number(req.body.sort_order) || 100)), now, adminActor(req));
  res.json({ ok: true, class_value: cv });
}));

/** 分類の候補資材の入れ替え。body: { class_value, codes: "a,b,c" } */
router.post('/admin/materials/class/candidates', checkOrigin, requireAdmin, api(async (req, res) => {
  const cv = normalizeKeyText(req.body.class_value);
  const db = getDB();
  if (!db.prepare('SELECT 1 FROM pk_pack_classes WHERE class_value=?').get(cv)) {
    throw new PackError(404, 'not_found', '分類がありません (先に分類を登録してください)');
  }
  const codes = String(req.body.codes || '').split(',').map((s) => s.trim()).filter(Boolean);
  const uniq = [...new Set(codes)];
  if (uniq.length > 50) throw new PackError(400, 'bad_codes', '候補は50件までです');
  for (const c of uniq) {
    if (!db.prepare('SELECT 1 FROM pk_pack_materials WHERE code=?').get(c)) {
      throw new PackError(400, 'bad_material', `資材 ${c} がありません`);
    }
  }
  const now = utcNowRouter();
  const tx = db.transaction(() => {
    db.prepare('DELETE FROM pk_pack_class_materials WHERE class_value=?').run(cv);
    uniq.forEach((code, i) => db.prepare(`
      INSERT INTO pk_pack_class_materials (class_value, material_code, sort_order, updated_at, updated_by)
      VALUES (?, ?, ?, ?, ?)
    `).run(cv, code, (i + 1) * 10, now, adminActor(req)));
  });
  tx.immediate();
  res.json({ ok: true, count: uniq.length });
}));

/** 伝票指定辞書 (header_map) の追加・編集。body: { header_value, base_delivery_code, material_code } */
router.post('/admin/materials/header', checkOrigin, requireAdmin, api(async (req, res) => {
  const hv = normalizeKeyText(req.body.header_value);
  if (!hv || hv.length > 120) throw new PackError(400, 'bad_header', 'header 値は必須です (120文字まで)');
  const base = String(req.body.base_delivery_code || '');
  if (!BASE_DELIVERY_CODES.includes(base)) throw new PackError(400, 'bad_code', '基底コードが不正です');
  const mc = String(req.body.material_code || '').trim() || null;
  const db = getDB();
  if (mc && !db.prepare('SELECT 1 FROM pk_pack_materials WHERE code=?').get(mc)) {
    throw new PackError(400, 'bad_material', `資材 ${mc} がありません`);
  }
  const now = utcNowRouter();
  db.prepare(`
    INSERT INTO pk_pack_header_map (header_value, base_delivery_code, material_code, updated_at, updated_by)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(header_value) DO UPDATE SET base_delivery_code=excluded.base_delivery_code,
      material_code=excluded.material_code, updated_at=excluded.updated_at, updated_by=excluded.updated_by
  `).run(hv, base, mc, now, adminActor(req));
  res.json({ ok: true, header_value: hv });
}));

/** 登録ルールの修正・無効化 (CAS — §5.1 管理者も同じ)。body: { action: set_material|disable, version, material_code? } */
router.post('/admin/materials/rules/:id(\\d+)', checkOrigin, requireAdmin, api(async (req, res) => {
  const db = getDB();
  const now = utcNowRouter();
  const admin = adminActor(req);
  const tx = db.transaction(() => {
    const rule = db.prepare('SELECT * FROM pk_pack_material_rules WHERE id = ?').get(Number(req.params.id));
    if (!rule) throw new PackError(404, 'not_found', 'ルールがありません');
    const version = Number(req.body.version);
    if (rule.version !== version) throw new PackError(409, 'conflict', '他で変更されています。再読み込みしてください');
    const before = rule.material_code;
    if (req.body.action === 'disable') {
      const u = db.prepare(`
        UPDATE pk_pack_material_rules SET status='disabled', version=version+1, updated_by=?, updated_at=?
        WHERE id=? AND version=?`).run(admin, now, rule.id, version);
      if (u.changes !== 1) throw new PackError(409, 'conflict', '他で変更されています');
      insertAdminRuleEvent(db, 'admin_disable', { ...rule, version: version + 1 }, before, admin);
    } else if (req.body.action === 'set_material') {
      const mc = String(req.body.material_code || '');
      const m = db.prepare('SELECT code, is_active FROM pk_pack_materials WHERE code=?').get(mc);
      if (!m || !m.is_active) throw new PackError(400, 'bad_material', 'その資材は選択できません');
      if (rule.status !== 'active') throw new PackError(409, 'not_active', '無効化済みルールは変更できません');
      const u = db.prepare(`
        UPDATE pk_pack_material_rules SET material_code=?, version=version+1, updated_by=?, updated_at=?
        WHERE id=? AND version=? AND status='active'`).run(mc, admin, now, rule.id, version);
      if (u.changes !== 1) throw new PackError(409, 'conflict', '他で変更されています');
      insertAdminRuleEvent(db, 'admin_edit', { ...rule, version: version + 1, material_code: mc }, before, admin);
    } else {
      throw new PackError(400, 'bad_action', 'action が不正です');
    }
  });
  tx.immediate();
  res.json({ ok: true });
}));

/** 通知の手動再送 (failed のみ — §5.3)。 */
router.post('/admin/materials/notify/:id(\\d+)/resend', checkOrigin, requireAdmin, api(async (req, res) => {
  res.json(materialManualResend(Number(req.params.id), adminActor(req)));
}));

// ─── ⑥ 配送ルール変更の申請 (Render packing-dispatch へ中継 — 要件⑥) ───
// 承認は GChat カードのボタン (stock-bot 経由)。ここは申請の受付だけ
const PD_RULE_URL = (process.env.PD_RULE_CHANGE_URL
  || 'https://bfaith-portal.onrender.com/apps/packing-dispatch/rule-change-api').replace(/\/+$/, '');
let _ruleOptionsCache = { at: 0, data: null };

router.get('/api/rule-change/options', api(async (req, res) => {
  if (!process.env.PD_RULE_CHANGE_KEY) {
    throw new PackError(503, 'disabled', 'ルール変更申請は未設定です (PD_RULE_CHANGE_KEY)');
  }
  if (!_ruleOptionsCache.data || Date.now() - _ruleOptionsCache.at > 600_000) {
    const r = await fetch(`${PD_RULE_URL}/options`, {
      headers: { 'x-api-key': process.env.PD_RULE_CHANGE_KEY },
    });
    if (!r.ok) throw new PackError(502, 'upstream', `選択肢の取得に失敗しました (HTTP ${r.status})`);
    _ruleOptionsCache = { at: Date.now(), data: await r.json() };
  }
  res.json({ ok: true, ...(_ruleOptionsCache.data) });
}));

/** 現在の配送ルール登録の照会 (⑥フォームの表示用)。 */
router.post('/api/batches/:id(\\d+)/rule-current', checkOrigin, api(async (req, res) => {
  if (!process.env.PD_RULE_CHANGE_KEY) {
    throw new PackError(503, 'disabled', 'ルール変更申請は未設定です (PD_RULE_CHANGE_KEY)');
  }
  const batch = getPackBatch(Number(req.params.id));
  if (!batch) throw new PackError(404, 'not_found', 'バッチが見つかりません');
  const slipSeq = Number(req.body.slip_seq);
  const slip = listPackSlips(batch.id).find((x) => x.seq === slipSeq);
  if (!slip) throw new PackError(404, 'slip_not_found', '伝票が見つかりません');
  const lines = listPackLinesBySlip(batch.id).get(slip.id) || [];
  if (lines.length === 0) throw new PackError(404, 'no_lines', '明細がありません');
  const r = await fetch(`${PD_RULE_URL}/current`, {
    method: 'POST',
    headers: { 'x-api-key': process.env.PD_RULE_CHANGE_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      kind: lines.length === 1 ? 'single' : 'assort',
      items: lines.map((l) => ({ sku: l.sku, qty: l.qty })),
    }),
  });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new PackError(502, 'upstream', body.error || `現在の登録の取得に失敗しました (HTTP ${r.status})`);
  res.json(body);
}));

router.post('/api/batches/:id(\\d+)/rule-change', checkOrigin, api(async (req, res) => {
  if (!process.env.PD_RULE_CHANGE_KEY) {
    throw new PackError(503, 'disabled', 'ルール変更申請は未設定です (PD_RULE_CHANGE_KEY)');
  }
  // 作業者検証 (イベントAPIと同じ)
  const name = String(req.body.worker_name || '').trim();
  let worker = null;
  if (name) {
    if (!listWorkers().some((w) => w.name === name)) {
      throw new PackError(400, 'bad_worker', '作業者が無効です。選び直してください');
    }
    worker = name;
  } else if (req.session?.email) {
    worker = req.session.displayName || req.session.email;
  } else {
    throw new PackError(400, 'no_worker', '作業者を選択してください');
  }
  const batch = getPackBatch(Number(req.params.id));
  if (!batch) throw new PackError(404, 'not_found', 'バッチが見つかりません');
  const slipSeq = Number(req.body.slip_seq);
  const slip = listPackSlips(batch.id).find((x) => x.seq === slipSeq);
  if (!slip) throw new PackError(404, 'slip_not_found', '伝票が見つかりません');
  const lines = listPackLinesBySlip(batch.id).get(slip.id) || [];
  if (lines.length === 0) throw new PackError(404, 'no_lines', '明細がありません');
  const kind = lines.length === 1 ? 'single' : 'assort';
  const payload = {
    kind,
    items: lines.map((l) => ({ sku: l.sku, name: l.print_name || l.product_name, qty: l.qty })),
    mall_group: req.body.mall_group,
    qty_min: req.body.qty_min == null ? null : Number(req.body.qty_min),
    qty_max: req.body.qty_max == null || req.body.qty_max === '' ? null : Number(req.body.qty_max),
    shipping_method_code: req.body.shipping_method_code,
    packing_machine_code: req.body.packing_machine_code,
    requested_by: worker,
    context: `${batch.folder_name || ''} ${slip.ne_slip_no}`.trim(),
    expect_method_code: req.body.expect_method_code ?? null,
    expect_machine_code: req.body.expect_machine_code ?? null,
    expect_none: req.body.expect_none === true,
  };
  const r = await fetch(`${PD_RULE_URL}/requests`, {
    method: 'POST',
    headers: { 'x-api-key': process.env.PD_RULE_CHANGE_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) {
    throw new PackError(r.status === 400 ? 400 : 502, 'upstream', body.error || `申請に失敗しました (HTTP ${r.status})`);
  }
  res.json({ ok: true, id: body.id, cardPosted: body.cardPosted });
}));

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
    icons: [
      { src: '/app-icons/packing-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/app-icons/packing-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
    ],
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
    printJobs: listPrintJobs(30),
    routes: listPrintRoutes(),
    SLUG_LABELS,
    PRINT_STATE_LABELS,
  });
});

/** 引当分類 → プリンター の対応表を編集する。 */
router.post('/admin/print-routes', checkOrigin, requireAdmin, api(async (req, res) => {
  const r = setPrintRoute(req.body.slug, req.body.printer_name, req.session.email, req.body.note);
  if (!r.ok) throw new PackError(400, r.reason, r.message || '入力を確認してください');
  // orphan = その名前を登録している端末が無い → 積まれても誰も取りに来ず滞留する
  res.json({ ok: true, deleted: !!r.deleted, orphan: !!r.orphan });
}));

/** 端末から出せるプリンターを付け替える (登録し直さずに増減できるように)。 */
router.post('/admin/devices/:id(\\d+)/printers', checkOrigin, requireAdmin, api(async (req, res) => {
  const names = Array.isArray(req.body.printers) ? req.body.printers : [];
  const r = setAgentPrinters(Number(req.params.id), names);
  if (!r.ok) throw new PackError(400, r.reason, r.message);
  res.json({ ok: true, printers: r.printers });
}));

/**
 * この端末 (iPad) を登録する。発行トークンは httpOnly Cookie としてこの端末にだけ渡す。
 * ⭐登録と同時に管理者セッションを破棄する (共用端末に管理者権限を残さない — picking と同規約)。
 * 🚨ホーム画面に追加したPWA内で実行すること (SafariとPWAはCookie保存領域が別)
 */
router.post('/admin/devices', checkOrigin, requireAdmin, api(async (req, res) => {
  const label = String(req.body.label || '').trim();
  if (!label || label.length > 40) throw new PackError(400, 'bad_label', '端末名を1〜40文字で入力してください');

  // 🖨 印刷エージェント (出荷PC) は iPad と発行導線は同じだが扱いが逆:
  //   - Cookie ではなく**平文トークンをこの1回だけ画面に表示**する (エージェントの設定に貼る)
  //   - 管理者セッションは破棄しない (登録している端末 = 中原さんのPCで、共用端末ではない)
  //   - 出力先プリンター名をサーバ側で紐づける (エージェントの設定ミスで別プリンターに出さない)
  if (String(req.body.kind || '') === 'agent') {
    // 1台のPCに複数のプリンターがぶら下がる (出荷PC / 倉庫PC)。ここに登録した名前**だけ**が
    // そのPCの出力先候補になる。どのジョブをどれに出すかは引当分類の対応表が決める
    // 検証は setAgentPrinters に一本化する。ここで空項目を捨てると、
    // 「新規登録は通るのに付け替えは400」と入口ごとに基準が変わり、
    // 登録できたつもりで1つ入っていない状態を作る
    const names = Array.isArray(req.body.printers) ? req.body.printers
      : (req.body.printer_name == null ? [] : [req.body.printer_name]);
    if (names.length === 0) {
      throw new PackError(400, 'bad_printer',
        'このPCから出せるプリンター名を1つ以上入力してください (「プリンターとスキャナー」の表記どおり)');
    }
    const { token, id } = createDevice(label, req.session.email, { kind: 'agent' });
    const set = setAgentPrinters(id, names);
    if (!set.ok) {
      // 端末だけ作って中身が入らない状態を残さない (登録できたつもりで印刷されない事故を防ぐ)
      revokeDevice(id);
      throw new PackError(400, set.reason, set.message);
    }
    return res.json({ ok: true, kind: 'agent', id, token, printers: set.printers });
  }

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
