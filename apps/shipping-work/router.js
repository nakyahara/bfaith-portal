/**
 * 出荷作業管理アプリ router (bfaith-portal)
 *
 * URL: /apps/shipping-work/ (カンバン) — PR1 は表示のみ。
 *      作業API (リース/開始/完了/保留/印刷トラブル) は PR3、管理者画面は PR2/PR5。
 *
 * 認証: server.js で requireAppAccess('shipping-work') を mount 時に適用。
 *       管理者専用操作は router 内で req.session.role === 'admin' を check (PR2〜)。
 *
 * 設計書: AI_reference/システム設計/出荷作業管理アプリ_要件定義_20260801.md (v2)
 *         AI_reference/システム設計/出荷作業管理アプリ_実装計画_20260801.md
 */
import { Router } from 'express';
import crypto from 'node:crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  getDB, initShippingWorkDB, jstToday, listKanbanBatches, BATCH_STATUSES, STATUS_LABELS,
  listMasters, isValidMaster, createBatch, updateBatch, cancelBatch, listAdminBatches, getBatch,
  DOCS_DIR, DATA_DIR,
} from './db.js';
import {
  SwError, startProcess, completeProcess, pauseProcess, resumeProcess, troubleProcess,
  startNextReady, getWorkerState, requestReprint, correctCompletion,
  getBatchDetail, adminFixStatus, adminJudgeSession, listSessionsForReview,
  ADMIN_FIXABLE_STATUSES,
  listAnomalies, getStats, getSettings, saveSetting, toggleMaster, listAllMasters, FLAG_LABELS,
} from './service.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// import 時 (= server.js boot 時) に DB を初期化する。
// migration 失敗時はここで throw し、旧スキーマのまま起動を継続させない (Codex PR1レビュー#3)
initShippingWorkDB();

// カンバンの列構成 (表示順)。hold/stock_return は横断列として末尾に置く。
// cancelled はカンバンに出さない (管理画面のみ・PR5)。
const KANBAN_COLUMNS = [
  'ready', 'picking', 'picked', 'sorting', 'sorted', 'packing', 'done', 'hold', 'stock_return',
];

/**
 * 表示名の解決 (email → displayName)。portal の users.json を読むだけの表示専用ヘルパー。
 * 読めなければ空マップ (fail-soft: email の @ 前を表示に使う)。
 * カンバンは全端末が30秒ごとに再読み込みするため、同期readを毎回走らせないよう60秒キャッシュする。
 */
let displayNameCache = { at: 0, map: {} };
function loadDisplayNames() {
  if (Date.now() - displayNameCache.at < 60_000) return displayNameCache.map;
  let map = {};
  try {
    const arr = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'users.json'), 'utf-8'));
    map = Object.fromEntries(arr.filter((u) => u?.email).map((u) => [u.email, u.displayName || u.email]));
  } catch { /* 読めなければ email 表示にフォールバック */ }
  displayNameCache = { at: Date.now(), map };
  return map;
}

// ─── カンバン (全員・表示専用。ドラッグ機能は作らない) ───
router.get('/', (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const batches = listKanbanBatches(workDate);

  const columns = KANBAN_COLUMNS.map((status) => ({
    status,
    label: STATUS_LABELS[status],
    batches: batches.filter((b) => b.status === status),
  }));

  res.render(path.join(__dirname, 'views/kanban'), {
    title: '出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: req.session.role === 'admin',
    workDate,
    columns,
    totalCount: batches.length,
    displayNames: loadDisplayNames(),
  });
});

// ═══ 作業画面 (タブレット前提・大ボタン)。3工程が同じ view (views/work.ejs) を共有 ═══
//
// ピッキング=開始で印刷+計測 / 仕分け=アソートのみ (picked→sorted) / 梱包=最終工程 (→done)。
// 工程の違いは PROCESS_FLOW (service.js) が持ち、画面と API は :process で共通化する。

const WORK_PROCESSES = {
  picking: { label: 'ピッキング', icon: '📦', pauseReasonKind: 'pause_reason_pick', hasTrouble: true, hasMistakes: false },
  sorting: { label: '仕分け', icon: '🗂', pauseReasonKind: 'pause_reason_pack', hasTrouble: false, hasMistakes: false },
  packing: { label: '梱包', icon: '📮', pauseReasonKind: 'pause_reason_pack', hasTrouble: false, hasMistakes: true },
};

router.get('/:process(picking|sorting|packing)', (req, res) => {
  const process = req.params.process;
  const conf = WORK_PROCESSES[process];
  const state = getWorkerState(process, req.session.email);
  res.render(path.join(__dirname, 'views/work'), {
    title: `${conf.label} | 出荷作業管理`,
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: req.session.role === 'admin',
    process,
    conf,
    state,
    today: jstToday(),
    pauseReasons: listMasters(conf.pauseReasonKind),
    troubleReasons: listMasters('print_trouble_reason'),
    reprintReasons: listMasters('reprint_reason'),
    correctionReasons: listMasters('correction_reason'),
    mistakeKinds: listMasters('mistake_kind'),
  });
});

/**
 * 作業APIの共通ラッパ。SwError は業務エラーとして status + message を返す。
 * express 4 は async ハンドラの throw を拾わないため、ここで必ず捕捉する。
 */
function api(handler) {
  return (req, res) => {
    try {
      res.json(handler(req));
    } catch (e) {
      // detail は「後の工程が進んでいます」のように、画面で内容を見せて確認させるための付随情報
      if (e instanceof SwError) {
        return res.status(e.status).json({ error: e.message, code: e.code, detail: e.detail });
      }
      console.error('[shipping-work] API error', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

// 全作業APIはクライアント発行の op_id (UUID) を必須とし、service の冪等レイヤーが
// 「操作種別 × 対象 × 入力」に束縛して記録する。完全一致の再送だけが前回結果を返す。
const opId = (v) => String(v || '');
const sessionId = (v) => Number(v);
const PROC = ':process(picking|sorting|packing)';

// 開始 (ピッキングはリース + 印刷ジョブ + 計測開始。仕分け・梱包は印刷なし)
router.post(`/api/${PROC}/start`, api((req) => {
  const r = startProcess(req.params.process, Number(req.body?.batch_id), req.session.email, opId(req.body?.op_id));
  return { ok: true, session_id: r.session_id, batch_id: r.batch_id, already: r.already };
}));

// 完了。next=true なら次の開始可能バッチを連続開始 (op_id_next 必須)。
// 梱包のみ mistakes ([{kind,count}]) を任意で受ける (要件§8.1)。
// 完了は確定済みのため、次の開始が失敗しても完了をエラーとして返さない
// (作業者には「完了はできた・次は取れなかった」と伝わるのが正しい)。
router.post(`/api/${PROC}/complete`, api((req) => {
  const process = req.params.process;
  const r = completeProcess(process, sessionId(req.body?.session_id), req.session.email,
    opId(req.body?.op_id), { mistakes: req.body?.mistakes });
  let next = null;
  let nextError = null;
  if (req.body?.next) {
    try {
      next = startNextReady(process, req.session.email, opId(req.body?.op_id_next));
    } catch (e) {
      nextError = e instanceof SwError ? e.message : '次のバッチを開始できませんでした';
      console.error('[shipping-work] 次バッチ開始に失敗', e);
    }
  }
  return { ok: true, already: r.already, flags: r.flags, workSec: r.workSec ?? null, next, nextError };
}));

// 保留 (理由必須・「その他」は記述必須)。
// 既に別の理由で保留済みだった場合は effectiveReason で実際に効いている理由を返す
router.post(`/api/${PROC}/pause`, api((req) => {
  const r = pauseProcess(req.params.process, sessionId(req.body?.session_id), req.session.email,
    String(req.body?.reason || ''), req.body?.note, opId(req.body?.op_id));
  return { ok: true, already: r.already, effectiveReason: r.effectiveReason ?? null };
}));

// 保留解除
router.post(`/api/${PROC}/resume`, api((req) => {
  const r = resumeProcess(req.params.process, sessionId(req.body?.session_id), req.session.email, opId(req.body?.op_id));
  return { ok: true, already: r.already };
}));

// 完了後に帳票をもう一度出す (ステータスも計測も触らない。印刷ジョブを追記するだけ)
router.post(`/api/${PROC}/reprint`, api((req) => {
  const r = requestReprint(req.params.process, sessionId(req.body?.session_id), req.session.email,
    String(req.body?.reason || ''), req.body?.note, opId(req.body?.op_id));
  return { ok: true, already: r.already, batch_id: r.batch_id };
}));

// 完了の訂正 (押し間違い・作業が残っていた)。元の計測は残し、続きは継続セッションに記録する
router.post(`/api/${PROC}/correct`, api((req) => {
  const r = correctCompletion(req.params.process, sessionId(req.body?.session_id), req.session.email,
    String(req.body?.reason || ''), req.body?.note, opId(req.body?.op_id));
  return { ok: true, already: r.already, session_id: r.session_id, held: r.held };
}));

// 印刷トラブル (reprint=再印刷して開始し直す / abort=中止して ready へ戻す)。ピッキングのみ
// (仕分け・梱包は開始時に印刷しないため、このボタン自体が無い)
router.post('/api/picking/trouble', api((req) => {
  const r = troubleProcess('picking', sessionId(req.body?.session_id), req.session.email,
    String(req.body?.reason || ''), req.body?.note, String(req.body?.action || ''), opId(req.body?.op_id));
  return { ok: true, already: r.already, session_id: r.session_id, aborted: r.aborted };
}));

// ─── ヘルスチェック (デプロイ確認用) ───
router.get('/api/health', (req, res) => {
  const db = getDB();
  const masters = db.prepare('SELECT COUNT(*) AS c FROM sw_masters').get();
  // schema = 適用済みのDB版数。migration を含むデプロイの前後で、
  // 「本番がどの版か」を外から確かめられるようにしておく (版を跨ぐ判断に必要)
  res.json({
    ok: true,
    schema: db.pragma('user_version', { simple: true }),
    masters: masters.c,
    statuses: BATCH_STATUSES.length,
  });
});

// ═══ 管理者: バッチ管理 (PR2) ═══

function requireAdminPage(req, res, next) {
  if (req.session.role !== 'admin') return res.status(403).send('管理者権限が必要です');
  next();
}
function requireAdminApi(req, res, next) {
  if (req.session.role !== 'admin') return res.status(403).json({ error: 'admin_required' });
  next();
}

// CSRF: portal のセッションCookieは httpOnly + secure + SameSite=Lax (server.js) のため、
// クロスサイトのフォームPOST/fetchにはCookieが送られず、状態変更APIは同一サイトからのみ実行できる。
// portal他アプリと同じ前提 (Codex PR2レビュー#4 確認済み)。

// PDF は memoryStorage で受けて自前で DOCS_DIR に保存する (ファイル名はサーバー発行の乱数のみ。
// クライアント由来のファイル名・パスは一切使わない)
const uploadPdf = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024, files: 1, fields: 30, parts: 40, fieldSize: 64 * 1024 },
});

async function savePdf(file) {
  if (!file) return null;
  if (!file.buffer?.length) throw new Error('PDFファイルが空です');
  const head = file.buffer.subarray(0, 5).toString('latin1');
  if (head !== '%PDF-') throw new Error('PDFファイルではありません');
  await fs.promises.mkdir(DOCS_DIR, { recursive: true });
  const name = `${crypto.randomUUID()}.pdf`;
  // 同期writeはNodeプロセス全体を止めるため非同期で書く (Codex PR2レビュー#9)
  await fs.promises.writeFile(path.join(DOCS_DIR, name), file.buffer);
  return name; // pdf_path には DOCS_DIR 相対のファイル名のみ保存
}

/** DB書込み失敗時などの孤児PDFの補償削除 (失敗は業務を止めないがログには残す)。 */
function removePdfQuietly(name) {
  if (!name) return;
  fs.promises.unlink(path.join(DOCS_DIR, name)).catch((err) => {
    if (err.code !== 'ENOENT') console.error('[shipping-work] PDF削除失敗', { name, code: err.code });
  });
}

/** 'YYYY-MM-DD' が実在する日付か (2026-02-31 等を拒否。Codex PR2レビュー#7)。 */
function isRealDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const [y, m, d] = s.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

/**
 * 帳票リンクの検証。http(s) のみ許可し、javascript: や data: を弾く。
 * 空文字は「リンク無し」として null を返す (PDFアップロード側で担保する想定)。
 */
function validateDocUrl(raw) {
  const s = String(raw || '').trim();
  if (!s) return { value: null };
  if (s.length > 2000) return { error: '帳票リンクが長すぎます' };
  let u;
  try { u = new URL(s); } catch { return { error: '帳票リンクの形式が正しくありません' }; }
  if (u.protocol !== 'http:' && u.protocol !== 'https:') {
    return { error: '帳票リンクは http(s) のURLを指定してください' };
  }
  return { value: u.href };
}

/** フォーム入力の検証。エラー文字列 or 検証済み値を返す。 */
function validateBatchInput(body) {
  const workDate = String(body.work_date || '');
  if (!isRealDate(workDate)) return { error: '作業日が不正です' };
  const shippingNo = String(body.shipping_no || '');
  if (!isValidMaster('shipping_no', shippingNo)) return { error: '出荷Noが不正です' };
  const bunrui = String(body.bunrui || '');
  if (!isValidMaster('bunrui', bunrui)) return { error: '発送分類が不正です' };
  const packingMethod = String(body.packing_method || '');
  if (!isValidMaster('packing_method', packingMethod)) return { error: '梱包方法が不正です' };
  let carriers = body.carriers ?? [];
  if (!Array.isArray(carriers)) carriers = [carriers];
  carriers = carriers.map(String);
  if (carriers.some((c) => !isValidMaster('carrier', c))) return { error: '配送種別が不正です' };
  let slipCount = null;
  if (body.slip_count !== undefined && String(body.slip_count).trim() !== '') {
    slipCount = Number(body.slip_count);
    if (!Number.isInteger(slipCount) || slipCount <= 0) return { error: '伝票件数は正の整数で入力してください' };
  }
  const note = String(body.note || '').trim() || null;
  const url = validateDocUrl(body.doc_url);
  if (url.error) return { error: url.error };
  return { workDate, shippingNo, bunrui, packingMethod, carriers, slipCount, note, docUrl: url.value };
}

// バッチ管理画面
router.get('/admin/batches', requireAdminPage, (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  res.render(path.join(__dirname, 'views/admin_batches'), {
    title: 'バッチ管理 | 出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    workDate,
    batches: listAdminBatches(workDate),
    statusLabels: STATUS_LABELS,
    masters: {
      shippingNo: listMasters('shipping_no'),
      bunrui: listMasters('bunrui'),
      packingMethod: listMasters('packing_method'),
      carrier: listMasters('carrier'),
    },
  });
});

// バッチ作成。帳票は必須だが、指定方法は「Driveリンク」か「PDFアップロード」のどちらでもよい。
// 現場はピッキングリストPDFを既に Drive に置いてリンクで見ているため、リンクが主。
// 帳票が無いと作業者が開始できずカンバンで滞留するので、入口で止める (Codex PR3レビュー#3)
router.post('/api/admin/batches', requireAdminApi, uploadPdf.single('pdf'), async (req, res) => {
  const v = validateBatchInput(req.body);
  if (v.error) return res.status(400).json({ error: v.error });
  if (!req.file && !v.docUrl) {
    return res.status(400).json({ error: '帳票リンク (Google Drive等) を入力するか、PDFを添付してください' });
  }
  let pdfPath = null;
  try {
    pdfPath = await savePdf(req.file);
    const id = createBatch({ ...v, pdfPath }, req.session.email);
    res.json({ ok: true, id });
  } catch (e) {
    removePdfQuietly(pdfPath);  // DB失敗時に孤児PDFを残さない (Codex PR2レビュー#1)
    if (e.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      return res.status(409).json({ error: `${v.workDate} の同じ出荷Noのバッチが既にあります` });
    }
    if (!e.code) return res.status(400).json({ error: e.message });  // savePdf の検証エラー
    throw e;
  }
});

// バッチ更新 (ready のみ)
router.post('/api/admin/batches/:id(\\d+)/update', requireAdminApi, uploadPdf.single('pdf'), async (req, res) => {
  const batch = getBatch(Number(req.params.id));
  if (!batch) return res.status(404).json({ error: 'not_found' });
  // work_date / shipping_no (業務キー) は変更不可。それ以外を検証するためキーは既存値を使う
  const v = validateBatchInput({ ...req.body, work_date: batch.work_date, shipping_no: batch.shipping_no });
  if (v.error) return res.status(400).json({ error: v.error });
  // 更新で帳票が両方空になると、開始できないバッチができてしまう。
  // PDF は未選択なら既存を保持するので「既存PDFがある」も帳票ありとみなす
  if (!v.docUrl && !req.file && !batch.pdf_path) {
    return res.status(400).json({ error: '帳票リンクを入力するか、PDFを添付してください' });
  }
  let pdfPath = null;
  let result;
  try {
    pdfPath = await savePdf(req.file);
    result = updateBatch(batch.id, { ...v, pdfPath }, req.session.email);
  } catch (e) {
    removePdfQuietly(pdfPath);
    if (!e.code) return res.status(400).json({ error: e.message });
    throw e;
  }
  if (!result.ok) {
    removePdfQuietly(pdfPath);  // 楽観排他で負けた場合も孤児にしない
    return res.status(409).json({ error: '作業開始後のバッチは編集できません (管理者手動修正はPR5)' });
  }
  removePdfQuietly(result.replacedPdf);  // 差し替え成功後に旧PDFを削除
  res.json({ ok: true });
});

// バッチ取消 (ready のみ・理由必須。hold の取消はセッション同時クローズが必要なため PR5)
router.post('/api/admin/batches/:id(\\d+)/cancel', requireAdminApi, (req, res) => {
  const reason = String(req.body?.reason || '').trim();
  if (!reason) return res.status(400).json({ error: '取消理由は必須です' });
  const ok = cancelBatch(Number(req.params.id), req.session.email, reason);
  if (!ok) return res.status(409).json({ error: '取消できるのは「本日のやること」のバッチのみです' });
  res.json({ ok: true });
});

// ═══ 管理者: 救済 (現物とシステムが食い違ったときの最終手段) ═══
//
// 作業者の訂正は「自分の直前操作」に限ってあるので、条件を外れたものは全部ここへ来る。
// できることは広いが、必ず理由と履歴を残し、計測時刻は決して書き換えない。

// バッチ詳細 (履歴・全セッション・印刷ジョブ)
router.get('/admin/batches/:id(\\d+)', requireAdminPage, (req, res) => {
  const detail = getBatchDetail(Number(req.params.id));
  if (!detail) return res.status(404).send('バッチが見つかりません');
  res.render(path.join(__dirname, 'views/admin_batch_detail'), {
    title: `バッチ #${detail.batch.id} | 出荷作業管理`,
    username: req.session.email,
    displayName: req.session.displayName,
    ...detail,
    statusLabels: STATUS_LABELS,
    statuses: ADMIN_FIXABLE_STATUSES,
    displayNames: loadDisplayNames(),
  });
});

// 確認待ち (作業者が訂正した / 管理者が閉じた セッション)
router.get('/admin/review', requireAdminPage, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_review'), {
    title: '確認待ち | 出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    sessions: listSessionsForReview(),
    displayNames: loadDisplayNames(),
  });
});

// ステータスの手動訂正 (理由必須)
router.post('/api/admin/batches/:id(\\d+)/fix-status', requireAdminApi, api((req) => {
  const r = adminFixStatus(Number(req.params.id), String(req.body?.status || ''),
    req.session.email, req.body?.reason, opId(req.body?.op_id), { force: !!req.body?.force });
  return { ok: true, already: r.already, status: r.status, closedSessions: r.closedSessions ?? 0 };
}));

// セッションを計測として採用するかの判定 (時刻は触らない)
router.post('/api/admin/sessions/:id(\\d+)/judge', requireAdminApi, api((req) => {
  const r = adminJudgeSession(Number(req.params.id), String(req.body?.validity || ''),
    req.session.email, req.body?.reason, opId(req.body?.op_id));
  return { ok: true, already: r.already, validity: r.validity };
}));

// ═══ 管理者: 異常候補・集計・設定 (PR5) ═══

/** 期間クエリの検証。不正なら今日1日。from > to は入れ替える。 */
function dateRange(req) {
  let from = isRealDate(String(req.query.from || '')) ? String(req.query.from) : jstToday();
  let to = isRealDate(String(req.query.to || '')) ? String(req.query.to) : from;
  if (from > to) [from, to] = [to, from];
  return { from, to };
}

// 異常候補一覧 (要件§7.4: 日次一覧に表示して管理者が判断)
router.get('/admin/anomalies', requireAdminPage, (req, res) => {
  const { from, to } = dateRange(req);
  res.render(path.join(__dirname, 'views/admin_anomalies'), {
    title: '異常候補 | 出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    from, to,
    sessions: listAnomalies(from, to),
    flagLabels: FLAG_LABELS,
    displayNames: loadDisplayNames(),
  });
});

// 集計 (要件§10.2: 作業者×工程×発送分類。平均と中央値)
router.get('/admin/stats', requireAdminPage, (req, res) => {
  const { from, to } = dateRange(req);
  res.render(path.join(__dirname, 'views/admin_stats'), {
    title: '集計 | 出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    from, to,
    rows: getStats(from, to),
    processLabels: { picking: 'ピッキング', sorting: '仕分け', packing: '梱包' },
    displayNames: loadDisplayNames(),
  });
});

// 集計CSV (BOM付きUTF-8 = Excelでそのまま開ける)
router.get('/admin/stats.csv', requireAdminPage, (req, res) => {
  const { from, to } = dateRange(req);
  const rows = getStats(from, to);
  const names = loadDisplayNames();
  const procLabels = { picking: 'ピッキング', sorting: '仕分け', packing: '梱包' };
  const esc = (v) => {
    let s = String(v ?? '');
    // Excel が数式として評価する先頭文字は ' を前置して無害化 (CSVインジェクション対策)
    if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const header = ['日付', '作業者', '工程', '発送分類', 'バッチ数', '伝票数', '実作業時間(分)', '保留時間(分)',
    '伝票/時', '1伝票あたり秒(平均)', '1伝票あたり秒(中央値)', 'バッチ時間中央値(分)', '異常候補数'];
  const lines = [header.join(',')];
  for (const r of rows) {
    lines.push([
      r.date,
      esc(names[r.worker] || r.worker), esc(procLabels[r.process] || r.process), esc(r.bunruiLabel || r.bunrui),
      r.batches, r.slips, (r.activeSec / 60).toFixed(1), (r.pauseSec / 60).toFixed(1),
      r.slipsPerHour ?? '', r.secPerSlipAvg ?? '', r.secPerSlipMedian ?? '',
      r.batchSecMedian != null ? (r.batchSecMedian / 60).toFixed(1) : '', r.anomalies,
    ].join(','));
  }
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="shipping-stats_${from}_${to}.csv"`);
  res.send('\uFEFF' + lines.join('\r\n'));
});

// 設定画面 (閾値・休憩時間帯・マスタ有効/無効)
router.get('/admin/settings', requireAdminPage, (req, res) => {
  res.render(path.join(__dirname, 'views/admin_settings'), {
    title: '設定 | 出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    settings: getSettings(),
    masterKinds: listAllMasters(),
    kindLabels: {
      shipping_no: '出荷No', bunrui: '発送分類', packing_method: '梱包方法', carrier: '配送種別',
      pause_reason_pick: '保留理由 (ピッキング)', pause_reason_pack: '保留理由 (仕分け・梱包)',
      mistake_kind: 'ミス分類', print_trouble_reason: '印刷トラブル理由',
      reprint_reason: '再印刷理由', correction_reason: '訂正理由',
    },
  });
});

router.post('/api/admin/settings', requireAdminApi, api((req) => {
  const r = saveSetting(String(req.body?.key || ''), req.body?.value, req.session.email);
  return { ok: true, key: r.key };
}));

router.post('/api/admin/masters/toggle', requireAdminApi, api((req) => {
  const r = toggleMaster(String(req.body?.kind || ''), String(req.body?.code || ''),
    !!req.body?.active, req.session.email);
  return { ok: true, active: r.active ?? null };
}));

// 添付PDFの表示 (開発中モード: ブリッジ未設置でも手動印刷できる逃げ道。実装計画§5)。
// 帳票は顧客の氏名・住所を含むため、閲覧できるのは管理者と「そのバッチを担当した本人」だけ。
// アプリ権限だけで通すと、IDを変えるだけで全バッチの帳票を読めてしまう。
function canViewBatchPdf(req, batchId) {
  if (req.session.role === 'admin') return true;
  return !!getDB().prepare(
    'SELECT 1 FROM sw_sessions WHERE batch_id = ? AND worker = ? LIMIT 1'
  ).get(batchId, req.session.email);
}

function servePdf(req, res) {
  const batch = getBatch(Number(req.params.id));
  if (!batch || !batch.pdf_path) return res.status(404).send('PDFがありません');
  // 存在の有無を伏せるため、権限外も404で返す
  if (!canViewBatchPdf(req, batch.id)) return res.status(404).send('PDFがありません');
  // pdf_path はサーバー発行のファイル名のみだが、念のため DOCS_DIR 内であることを検証
  const abs = path.resolve(DOCS_DIR, batch.pdf_path);
  if (!abs.startsWith(path.resolve(DOCS_DIR) + path.sep)) return res.status(400).send('不正なパス');
  if (!fs.existsSync(abs)) return res.status(404).send('PDFファイルが見つかりません');
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="batch-${batch.id}.pdf"`);
  // 帳票は個人情報を含むため共有PCのキャッシュを抑止 (Codex PR2レビュー#8)
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Cache-Control', 'private, no-store');
  fs.createReadStream(abs).pipe(res);
}
router.get('/batches/:id(\\d+)/pdf', servePdf);
router.get('/admin/batches/:id(\\d+)/pdf', requireAdminPage, servePdf);

// Multer のエラー (サイズ超過等) を JSON で返す (Codex PR2レビュー#5)
router.use((err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    const msg = err.code === 'LIMIT_FILE_SIZE' ? 'PDFは10MB以下にしてください' : `アップロードエラー: ${err.code}`;
    return res.status(err.code === 'LIMIT_FILE_SIZE' ? 413 : 400).json({ error: msg });
  }
  next(err);
});

export default router;
