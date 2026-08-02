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

// ═══ ピッキング (PR3: 作業者の本体画面。タブレット前提・大ボタン) ═══

router.get('/picking', (req, res) => {
  const state = getWorkerState('picking', req.session.email);
  res.render(path.join(__dirname, 'views/picking'), {
    title: 'ピッキング | 出荷作業管理',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: req.session.role === 'admin',
    state,
    today: jstToday(),
    pauseReasons: listMasters('pause_reason_pick'),
    troubleReasons: listMasters('print_trouble_reason'),
    reprintReasons: listMasters('reprint_reason'),
    correctionReasons: listMasters('correction_reason'),
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
      if (e instanceof SwError) return res.status(e.status).json({ error: e.message, code: e.code });
      console.error('[shipping-work] API error', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

// 全作業APIはクライアント発行の op_id (UUID) を必須とし、service の冪等レイヤーが
// 「操作種別 × 対象 × 入力」に束縛して記録する。完全一致の再送だけが前回結果を返す。
const opId = (v) => String(v || '');
const sessionId = (v) => Number(v);

// 開始 (楽観開始 = リース + 印刷ジョブ + 計測開始)
router.post('/api/picking/start', api((req) => {
  const r = startProcess('picking', Number(req.body?.batch_id), req.session.email, opId(req.body?.op_id));
  return { ok: true, session_id: r.session_id, batch_id: r.batch_id, already: r.already };
}));

// 完了。next=true なら次の開始可能バッチを連続開始 (op_id_next 必須)。
// 完了は確定済みのため、次の開始が失敗しても完了をエラーとして返さない
// (作業者には「完了はできた・次は取れなかった」と伝わるのが正しい)。
router.post('/api/picking/complete', api((req) => {
  const r = completeProcess('picking', sessionId(req.body?.session_id), req.session.email, opId(req.body?.op_id));
  let next = null;
  let nextError = null;
  if (req.body?.next) {
    try {
      next = startNextReady('picking', req.session.email, opId(req.body?.op_id_next));
    } catch (e) {
      nextError = e instanceof SwError ? e.message : '次のバッチを開始できませんでした';
      console.error('[shipping-work] 次バッチ開始に失敗', e);
    }
  }
  return { ok: true, already: r.already, flags: r.flags, workSec: r.workSec ?? null, next, nextError };
}));

// 保留 (理由必須・「その他」は記述必須)。
// 既に別の理由で保留済みだった場合は effectiveReason で実際に効いている理由を返す
router.post('/api/picking/pause', api((req) => {
  const r = pauseProcess('picking', sessionId(req.body?.session_id), req.session.email,
    String(req.body?.reason || ''), req.body?.note, opId(req.body?.op_id));
  return { ok: true, already: r.already, effectiveReason: r.effectiveReason ?? null };
}));

// 保留解除
router.post('/api/picking/resume', api((req) => {
  const r = resumeProcess('picking', sessionId(req.body?.session_id), req.session.email, opId(req.body?.op_id));
  return { ok: true, already: r.already };
}));

// 完了後に帳票をもう一度出す (ステータスも計測も触らない。印刷ジョブを追記するだけ)
router.post('/api/picking/reprint', api((req) => {
  const r = requestReprint('picking', sessionId(req.body?.session_id), req.session.email,
    String(req.body?.reason || ''), req.body?.note, opId(req.body?.op_id));
  return { ok: true, already: r.already, batch_id: r.batch_id };
}));

// 完了の訂正 (押し間違い・作業が残っていた)。元の計測は残し、続きは継続セッションに記録する
router.post('/api/picking/correct', api((req) => {
  const r = correctCompletion('picking', sessionId(req.body?.session_id), req.session.email,
    String(req.body?.reason || ''), req.body?.note, opId(req.body?.op_id));
  return { ok: true, already: r.already, session_id: r.session_id, held: r.held };
}));

// 印刷トラブル (reprint=再印刷して開始し直す / abort=中止して ready へ戻す)
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
