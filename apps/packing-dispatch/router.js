/**
 * 梱包機振り分け・配送方法判定 — router (Render 完結、ミニPC 不使用)
 *
 * URL: /apps/packing-dispatch/        … UI
 *      /apps/packing-dispatch/api/*   … REST API
 * 認証: server.js で requireAppAccess('packing-dispatch') を mount 時に適用。
 */
import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import { ensureSchema, shippingMethodMap, diagInfo, productDiag, assortDiag } from './db.js';
import {
  importCsv, listBatches, batchSummary, listOrders, getOrderDetail, decideOrder, exportCsv,
  buildExportCsv, commitExport, beginExport, releaseExport,
  listRules, upsertRules, copyRules, searchRules, searchRulesByCondition, bulkUpdateRules,
  listUnregistered, mirrorFreshness, searchAssort, updateAssort, searchProducts,
  getMeltlineMigrationPreview, executeMeltlineMigration, rollbackMeltlineMigration,
  listMeltlineBackups, readMeltlineBackup,
  importTrackingCsv, setTrackingManual, markReady, markSkipped,
  trackingSummary, listTracking, listTrackingImports, getTrackingImportDetail,
  listTrackingByMethod, listMissingTracking, setTrackingOne, changeShippingMethod,
  listTodayImportLinesByMethod,
  getMethodChangeSummary, listMethodChangeLog, purgeOldMethodChangeLog,
  backfillShipmentTrackingFromExportedBatches, deleteRecentTrackingImports,
  includeUnmatchedToTracking, bulkApplyTeikeigai,
  getReadyExportSummary, getReadyNeUketsukeNos, getReadyTrackingCsv, auditTrackingCsvDriveSave,
  startNeSyncRun, getNeSyncRun, listRecentNeSyncRuns,
  claimReadyForNeSync, applyNeSyncResults, completeNeSyncRun,
} from './service.js';
import crypto from 'node:crypto';
import { loadSeed } from './tools/load-shipping-rule-seed.mjs';
import { getDriveCsvInfoAll, downloadDriveCsv } from './drive-fetch.js';
import { uploadCsvToDrive, probeFolderWritable } from '../fba-replenishment/drive-upload.js';

// ── CSV出力の Google Drive 固定保存先 (2026-07-19 中原さん指示) ──
// 「📤 CSV出力してロジザードへ」の出力先を Drive の固定ファイルに一本化 (ブラウザDLは廃止)。
// 毎回この 1 ファイルへ上書き保存する。env で差し替え可 (フォルダ移設時など)。
// ⚠️ 前提: サービスアカウント (GOOGLE_SERVICE_ACCOUNT_KEY の client_email) を、保存先が属する
//    共有ドライブに「コンテンツ管理者」以上で招待しておく必要がある (マイドライブ配下は不可)。
// 保存先は共有ドライブ内フォルダ「bfaithポータルdata」。#585 で末尾1文字 (H) を欠いた ID を
// 埋め込んでしまい、Drive が 404 File not found を返して CSV 出力が全滅していた (2026-07-20 修正)。
const PD_DRIVE_EXPORT_FOLDER = process.env.PD_DRIVE_EXPORT_FOLDER || '1DMoPgo-f9kmzFSQ0e60apxTFi_2owoOH';
const PD_DRIVE_EXPORT_FILE = process.env.PD_DRIVE_EXPORT_FILE || 'logi_dispatch.csv';
// ⑤ 追跡番号タブの「受注番号+追跡番号 CSV」の Drive 保存先 (2026-07-19 中原さん指示)。
// メイン出力(logi_dispatch.csv)とは別フォルダ・別ファイル。同じく共有ドライブ配下 + SA コンテンツ管理者が前提。
const PD_TRK_DRIVE_FOLDER = process.env.PD_TRK_DRIVE_FOLDER || '1eP4mQv-gR5Z-L-N93G8N3TqcBAzyP-Ar';
const PD_TRK_DRIVE_FILE = process.env.PD_TRK_DRIVE_FILE || 'ne-tracking.csv';

// Drive 出力の直列化 (Codex High③: 固定ファイルへの並行書込で古いバッチが新しいCSVを上書き、
// および list→create の非原子性を回避)。bfaith-portal は単一インスタンス運用なのでプロセス内
// promise チェーンで十分。build→保存→CAS確定 の一連を 1 度に 1 件だけ実行する。
let _exportDriveChain = Promise.resolve();
function withExportDriveLock(fn) {
  const run = _exportDriveChain.then(fn, fn); // 前段の成否に関わらず順番に実行
  _exportDriveChain = run.then(() => {}, () => {}); // チェーン用にエラーを飲む (呼び出し側へは run で伝播)
  return run;
}
// ⑤ 追跡CSV(ne-tracking.csv)の Drive 保存用ロック (Codex Medium: メイン出力 logi_dispatch.csv とは
// 別ファイル・別フォルダなので別チェーンにする。片方の Drive API タイムアウトでもう片方を待たせない)。
// ne-tracking.csv 自身への並行書込・世代逆転はこのチェーンで防ぐ (単一インスタンス運用前提)。
let _trkDriveChain = Promise.resolve();
function withTrackingDriveLock(fn) {
  const run = _trkDriveChain.then(fn, fn);
  _trkDriveChain = run.then(() => {}, () => {});
  return run;
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });
const router = Router();

function currentUser(req) { return req.session?.email || req.session?.displayName || null; }
function requireAdmin(req, res) {
  if (req.session?.role !== 'admin') {
    res.status(403).json({ ok: false, error: 'forbidden', message: '管理者専用機能です' });
    return false;
  }
  return true;
}
function handle(res, fn) {
  try { res.json({ ok: true, result: fn() }); }
  catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message, detail: e.detail });
    if (e.code === 'CONFLICT') return res.status(409).json({ ok: false, error: 'conflict', message: e.message });
    console.error('[packing]', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
}
// Drive 直接取込など await が必要な処理用 (エラー変換は handle と同一)
async function handleAsync(res, fn) {
  try { res.json({ ok: true, result: await fn() }); }
  catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message, detail: e.detail });
    if (e.code === 'CONFLICT') return res.status(409).json({ ok: false, error: 'conflict', message: e.message });
    console.error('[packing]', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
}

// ── ミニPC worker 用 Bearer middleware (fail-closed、構成 B 2026-06-05) ──
//
// 構成 B: CF Access は外周防御 (既存運用流用)、Bearer は NE sync 専用 (新規発行)。
// 未設定 (env 未設定) なら全 endpoint を 503 で fail-closed (Codex 助言、絶対 fail-open しない)。
// timing-safe compare で token を比較。
function requireWorkerKey(req, res, next) {
  const expected = process.env.RENDER_NE_SYNC_WORKER_KEY || '';
  if (!expected) {
    // env 未設定 → fail-closed (本番運用前にも誤って認証バイパスしないため)
    return res.status(503).json({ ok: false, error: 'not_configured',
      message: 'RENDER_NE_SYNC_WORKER_KEY が未設定です。サーバ管理者に連絡してください。' });
  }
  const header = String(req.headers.authorization || '');
  const m = /^Bearer\s+(\S+)$/.exec(header);
  if (!m) return res.status(401).json({ ok: false, error: 'unauthorized', message: 'Bearer token がありません' });
  const got = m[1];
  // timing-safe compare (length 不一致でも短絡しない)
  const a = Buffer.from(got, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    // 監査ログ: failure のみ記録 (token は記録しない)
    console.warn('[ne-sync] worker auth failed', { ip: req.ip, ua: req.headers['user-agent'] });
    return res.status(403).json({ ok: false, error: 'forbidden', message: 'Bearer token が一致しません' });
  }
  next();
}

router.get('/', (req, res) => res.sendFile(path.join(__dirname, 'views', 'index.html')));

// マスタ選択肢 (配送方法・梱包機)
router.get('/api/options', (req, res) => handle(res, () => {
  ensureSchema();
  const db = ensureSchema();
  const methods = db.prepare(`SELECT code,name_csv,rank,is_locked,is_nekopos FROM pd_shipping_method ORDER BY (rank IS NULL), rank, code`).all();
  const machines = db.prepare(`SELECT code,name_csv,sort_order FROM pd_packing_machine ORDER BY sort_order`).all();
  return { methods, machines };
}));

// CSV アップロード → 取込・判定
router.post('/api/upload', upload.single('file'), (req, res) => handle(res, () => {
  if (!req.file) { const e = new Error('ファイルがありません'); e.code = 'VALIDATION'; throw e; }
  return importCsv(req.file.buffer, req.file.originalname, currentUser(req));
}));

// 取込済みバッチ一覧 (再表示用)
router.get('/api/batches', (req, res) => handle(res, () => listBatches(20)));

router.get('/api/batch/:id', (req, res) => handle(res, () => {
  const s = batchSummary(req.params.id);
  if (!s) { const e = new Error('バッチが見つかりません'); e.code = 'VALIDATION'; throw e; }
  return s;
}));

router.get('/api/orders', (req, res) => handle(res, () =>
  listOrders(req.query.batch, { status: req.query.status || null })));

// 1伝票の詳細 (変更ダイアログのリッチ表示用)
router.get('/api/order', (req, res) => handle(res, () => {
  const d = getOrderDetail(req.query.batch, req.query.shop, req.query.order);
  if (!d) { const e = new Error('伝票が見つかりません'); e.code = 'VALIDATION'; throw e; }
  return d;
}));

// 1伝票の確定 (アソートは learn=true で学習登録)
router.post('/api/decide', (req, res) => handle(res, () => {
  const b = req.body || {};
  return decideOrder(b.batch_id, b.shop_name, b.order_no, {
    shipping_method_code: b.shipping_method_code,
    packing_machine_code: b.packing_machine_code,
    learn: !!b.learn,
    delivery_date: b.delivery_date,
    delivery_time: b.delivery_time,
  }, currentUser(req));
}));

// ── Drive 保存先の書き込み点検 (go-live 前確認用、admin専用) ──
// ブラウザで /apps/packing-dispatch/api/export/drive-check を開くと、SA が保存先フォルダに
// 書き込めるか (capabilities.canAddChildren の読取のみ・非破壊) と、共有ドライブ配下かを判定して返す。
// 本番バッチを消費せず副作用も無い。⚠️ 動的 :id ルートより手前に定義すること
// (後ろだと id='drive-check' として GET /api/export/:id に捕捉される、Codex Medium)。
router.get('/api/export/drive-check', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handleAsync(res, async () => {
    const r = await probeFolderWritable(PD_DRIVE_EXPORT_FOLDER);
    return { folderId: PD_DRIVE_EXPORT_FOLDER,
      folderUrl: `https://drive.google.com/drive/folders/${PD_DRIVE_EXPORT_FOLDER}`,
      filename: PD_DRIVE_EXPORT_FILE, ...r };
  });
});

// ── CSV出力 → Google Drive 固定ファイルへ上書き保存 (2026-07-19 中原さん指示) ──
// 「📤 CSV出力してロジザードへ」の本経路。二相ロックで安全に確定する (Codex High①②③):
//   ① beginExport … classifying→locked_for_export を CAS で claim
//      (既に exported / 古いバッチは弾かれ、Drive を上書きすらしない。編集APIも locked を拒否)
//   ② buildExportCsv … CSV 生成 (副作用なし)
//   ③ uploadCsvToDrive … Drive へ上書き保存
//   ④ commitExport … locked_for_export→exported を CAS 確定 + tracking 登録
//   失敗時は releaseExport で classifying へ戻す (未出力のまま再実行可能。同名へ冪等上書き)。
// 全体を withExportDriveLock で直列化 (固定ファイルへの並行書込・世代逆転を防ぐ)。
// 残存リスク: ③成功後に④/プロセスが落ちると locked_for_export のまま Drive にだけ載る。
//   → 再実行で同バッチが再 claim→再upload(冪等)→commit され自己回復する (二重投入は起きない)。
router.post('/api/export/:id/to-drive', (req, res) => handleAsync(res, () =>
  withExportDriveLock(async () => {
    const batchId = req.params.id;
    beginExport(batchId); // ここで弾かれれば Drive には一切触れない
    try {
      const built = buildExportCsv(batchId, {});
      const up = await uploadCsvToDrive(built.buffer, PD_DRIVE_EXPORT_FILE, PD_DRIVE_EXPORT_FOLDER);
      commitExport(batchId, currentUser(req), { lineRows: built.lineRows, rowCount: built.rowCount });
      return { status: 'saved', rowCount: built.rowCount,
        folderName: up.folderName || null,
        folderUrl: `https://drive.google.com/drive/folders/${PD_DRIVE_EXPORT_FOLDER}`,
        drive: { action: up.action, fileId: up.fileId, filename: PD_DRIVE_EXPORT_FILE } };
    } catch (e) {
      releaseExport(batchId); // 未出力状態へ戻して再実行可能に (best-effort)
      throw e;
    }
  })));

// ── Google Drive が使えないとき用の手動ダウンロード (staff、2026-07-19 中原さん指示) ──
// Drive 一本化の抜け穴にしないため UI では控えめに置くが、機能としては to-drive と同じ
// 「beginExport→buildExportCsv→commitExport」を行い、exported 化 + tracking 登録まで正しく確定する
// (これを怠ると ⑤ 追跡番号タブに伝票が出てこない)。Drive へは上げず CSV を base64 で返し、フロントで PC 保存する。
// admin GET /api/export/:id と違い staff から呼べる (障害時の詰み防止) が、通常運用は Drive 保存が主。
router.post('/api/export/:id/download', (req, res) => handleAsync(res, () =>
  withExportDriveLock(async () => {
    const batchId = req.params.id;
    // 出力は常に通常形式 (premeltline は全経路で撤去済 2026-07-19)。出力形式のブレが無くなるので、
    // exported 後の再取得は 48h 以内なら運用上ほぼ同一内容になる (厳密なバイト一致は非保証)。
    const summary = batchSummary(batchId);
    if (!summary) { const e = new Error('バッチが見つかりません'); e.code = 'VALIDATION'; throw e; }
    const mkFilename = (n) => `logi_dispatch_${n}rows.csv`;
    // Codex High: commitExport 後に DL が失敗すると「exported なのに手元に無い」で詰む。
    // 冪等回復のため、既に exported なら再確定せず CSV を再生成して返す (状態は変えない)。
    // これで DL 失敗後にもう一度リンクを押せば再取得できる (詰み防止)。
    // ※ CSV スナップショットは保存しない: 生成CSVには顧客住所等のPIIが含まれ、これを別途永続化すると
    //   本アプリのPII最小化 (PIIは import_line.raw_cols のみ・TTL purge) を崩すため。再生成でも
    //   1件ごとの判定 (配送方法/梱包機) は import_line に凍結済で、変動し得るのは静的な配送方法マスタ
    //   (pd_shipping_method) のみ → 48h の再取得窓では運用上ほぼ同一 (厳密なバイト一致は非保証・リスク受容)。
    if (summary.batch.status === 'exported') {
      // exported 分岐は beginExport を通らないため TTL チェックを明示 (Codex High):
      // purge は常時ではないので、期限切れバッチが残っている間に 48h 超で PII 入り CSV を再取得させない。
      if (summary.batch.expires_at && summary.batch.expires_at < new Date().toISOString()) {
        const e = new Error('このバッチは期限切れ(48h超)です。再度CSVをアップロードしてください'); e.code = 'VALIDATION'; throw e;
      }
      const built = buildExportCsv(batchId, {});
      return { status: 'downloaded', already_exported: true, rowCount: built.rowCount,
        filename: mkFilename(built.rowCount), csvBase64: Buffer.from(built.buffer).toString('base64') };
    }
    beginExport(batchId); // 未判断あり/期限切れ/世代逆転はここで弾かれる (Drive保存と同じガード)
    try {
      const built = buildExportCsv(batchId, {});
      commitExport(batchId, currentUser(req), { lineRows: built.lineRows, rowCount: built.rowCount });
      return { status: 'downloaded', rowCount: built.rowCount,
        filename: mkFilename(built.rowCount), csvBase64: Buffer.from(built.buffer).toString('base64') };
    } catch (e) {
      releaseExport(batchId); // 未出力状態へ戻して再実行可能に (best-effort)
      throw e;
    }
  })));

// 出力 (Shift-JIS)。旧ブラウザ直DL経路 (admin 限定)。
// ⚠️ 通常運用の出力先は Drive 固定 (POST /api/export/:id/to-drive) に一本化済み。
//    この旧DL経路は「Drive一本化を迂回して exported 化するだけ (Drive未保存)」の抜け穴になり得るため
//    admin 限定にする (Codex High④)。to-drive と同じ withExportDriveLock で直列化し、競合を防ぐ。
//    ※ premeltline (MeltLine 導入前形式) は撤去済 (2026-07-19 中原さん確定): MeltLine 本番稼働済で
//      今使うと MeltLine 品を手動出荷に落とす誤出力になり、手動DLの冪等再取得も崩すため。常に通常形式。
router.get('/api/export/:id', (req, res) => {
  if (!requireAdmin(req, res)) return;
  const batchId = req.params.id;
  withExportDriveLock(async () => {
    // Codex High: 直接 exportCsv だと beginExport の TTL・最新世代チェックを迂回し、
    //   期限切れ/旧世代バッチも exported 化できてしまう。to-drive / download と同じく beginExport を通す。
    beginExport(batchId);
    try {
      return exportCsv(batchId, currentUser(req), {});
    } catch (e) {
      releaseExport(batchId); // 未出力状態へ戻す (best-effort)
      throw e;
    }
  }).then(({ buffer, rowCount }) => {
    const fname = `logi_dispatch_${rowCount}rows.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
    res.send(buffer);
  }).catch((e) => {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message, detail: e.detail });
    console.error('[packing] export', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  });
});

// ── アソート学習 (注文番号/受注番号/商品コードで検索→編集) ──
router.get('/api/assort/search', (req, res) => handle(res, () => searchAssort({
  q: req.query.q || '',
  shipping_method_code: req.query.sm || '',
  packing_machine_code: req.query.pm || '',
  qtyMin: req.query.qtyMin,
  qtyMax: req.query.qtyMax,
  dateFrom: req.query.dateFrom || '',
  dateTo: req.query.dateTo || '',
})));
router.post('/api/assort/update', (req, res) => handle(res, () => {
  const b = req.body || {};
  return updateAssort(b.combo_key, b.shipping_method_code, b.packing_machine_code, currentUser(req));
}));

// ── マスタ ──
router.get('/api/rules', (req, res) => handle(res, () => listRules(req.query.product || '')));
router.post('/api/rules', (req, res) => handle(res, () => upsertRules(req.body || {}, currentUser(req))));
router.post('/api/rules/copy', (req, res) => handle(res, () => copyRules(req.body || {}, currentUser(req))));
// 登録済みルールを 商品コード/商品名 で検索 (コピー元の選択用)
router.get('/api/rule-search', (req, res) => handle(res, () => searchRules(req.query.q || '')));
// 商品マスタ検索 (商品コード/商品名で部分一致、2026-06-05 中原さん指示)
router.get('/api/product-search', (req, res) => handle(res, () => searchProducts(req.query.q || '', req.query.limit)));
// 条件(数量N・配送方法・梱包機・モール、すべて任意)で登録済みルールを検索
router.get('/api/rule-condition', (req, res) => handle(res, () => searchRulesByCondition({
  qty: req.query.qty, shipping_method_code: req.query.sm || null,
  packing_machine_code: req.query.pm || null, mall_group: req.query.mall || null,
})));
// 選択した帯の配送方法/梱包機を一括変更
router.post('/api/rules/bulk', (req, res) => handle(res, () => bulkUpdateRules(req.body || {}, currentUser(req))));

// ── 未登録一覧 (取扱中×非セット×②未登録) ──
router.get('/api/unregistered', (req, res) => handle(res, () => ({
  freshness: mirrorFreshness(),
  rows: listUnregistered(),
})));

// ── マスタ状態 (ルール件数)。失敗しても 500 にせずエラー文を返す(原因可視化) ──
router.get('/api/master-status', (req, res) => handle(res, () => {
  try {
    const db = ensureSchema();
    return { rule_rows: db.prepare('SELECT COUNT(*) c FROM pd_shipping_rule').get().c };
  } catch (e) {
    return { rule_rows: null, error: e.message };
  }
}));

// ── 診断 (取得失敗の原因切り分け) ──
router.get('/api/diag', (req, res) => handle(res, () => diagInfo()));
// ── 商品診断 (なぜ未登録一覧に出る/出ないか) ──
router.get('/api/product-diag', (req, res) => handle(res, () => productDiag(req.query.code || '')));
// ── アソート学習診断 (商品コードが学習に登録されているか・形式・有効無効) ──
router.get('/api/assort-diag', (req, res) => handle(res, () => assortDiag(req.query.code || '')));

// ═══ 追跡番号 / NE反映 (PR 1: CSV取込 + 一覧 + 手動入力 + ready/skip 遷移) ═══

// CSV 取込 (source: yamato_b2 | yamato_b2_50 | yupacketpuff)
router.post('/api/tracking/import', upload.single('file'), (req, res) => handle(res, () => {
  if (!req.file) { const e = new Error('ファイルがありません'); e.code = 'VALIDATION'; throw e; }
  const source = (req.body && req.body.source) || req.query.source;
  if (!source) { const e = new Error('source パラメータが必要です'); e.code = 'VALIDATION'; throw e; }
  return importTrackingCsv({ source, buffer: req.file.buffer, filename: req.file.originalname }, currentUser(req));
}));

// ── Google Drive 直接取込 (ヤマトB2 発行済データCSV、2026-07-18 中原さん指示) ──
// 全対象の metadata (更新日時表示用) を 1 リクエストで返す。60 秒キャッシュ (drive-fetch.js)。
router.get('/api/tracking/drive-file-info', (req, res) => handleAsync(res, () =>
  getDriveCsvInfoAll()));

// Drive からDLして通常の取込と同じ経路へ (file_hash 重複検出もそのまま効く)
// body: { source: 'yamato_b2' | 'yamato_b2_50' | 'yupacketpuff' }
router.post('/api/tracking/import-from-drive', (req, res) => handleAsync(res, async () => {
  const source = (req.body && req.body.source) || req.query.source;
  if (!source) { const e = new Error('source パラメータが必要です'); e.code = 'VALIDATION'; throw e; }
  const dl = await downloadDriveCsv(source);
  const result = importTrackingCsv({ source, buffer: dl.buffer, filename: dl.filename }, currentUser(req));
  // 取込元ファイルの更新日時を結果に添える (UI で「いつのファイルを取り込んだか」を示す)
  return { ...result, drive_file: { filename: dl.filename, modified_time: dl.modified_time, modified_time_jst: dl.modified_time_jst } };
}));

// 手動入力 (レターパック / 定形外)
//   body: { source: 'manual_letterpack' | 'no_tracking', entries: [{ne_uketsuke_no, tracking_no}] }
router.post('/api/tracking/manual', (req, res) => handle(res, () => setTrackingManual(req.body || {}, currentUser(req))));

// pending → ready 遷移
//   body: { allPending: true } または { ids: [ne_uketsuke_no, ...] }
router.post('/api/tracking/mark-ready', (req, res) => handle(res, () => markReady(req.body || {}, currentUser(req))));

// 欠品マーク (skipped 状態へ)
//   body: { ids: [...], reason: '欠品' 等 }
router.post('/api/tracking/mark-skipped', (req, res) => handle(res, () => markSkipped(req.body || {}, currentUser(req))));

// サマリ (ステータス別カウント)
router.get('/api/tracking/summary', (req, res) => handle(res, () =>
  trackingSummary({ scope: req.query.scope === 'all' ? 'all' : 'today' })));

// 一覧 (filter: status / source / shop / q)
router.get('/api/tracking', (req, res) => handle(res, () => listTracking({
  status: req.query.status, source: req.query.source, shop: req.query.shop, q: req.query.q,
  // Codex R6 High (2026-06-07): fail-closed。'all' 明示時のみ全期間、欠落/typo は本日扱い。
  // 全期間が欲しい caller は scope=all を明示すること。
  scope: req.query.scope === 'all' ? 'all' : 'today',
  limit: parseInt(req.query.limit, 10) || undefined, // Codex R3 High: limit pass-through
})));

// 配送方法別一覧 (定形外/レターパック 等の特定方法だけ抽出、synced/skipped 除外) - pd_shipment_tracking 起点 (旧版、互換用)
router.get('/api/tracking/by-method', (req, res) => handle(res, () => listTrackingByMethod(req.query.method)));

// 「本日アップロード」の取込バッチから配送方法別に抽出 (定形外/レターパック表示用、商品名付き)
router.get('/api/tracking/today-by-method', (req, res) => handle(res, () => listTodayImportLinesByMethod(req.query.method)));

// 追跡番号未割当アラート (定形外/レターパック/AES 以外で tracking_no が NULL の pending/error)
router.get('/api/tracking/missing', (req, res) => handle(res, () => listMissingTracking()));

// レターパック 1 件保存 (body: { ne_uketsuke_no, tracking_no, source })
router.post('/api/tracking/set-one', (req, res) => handle(res, () => setTrackingOne(req.body || {}, currentUser(req))));

// 配送方法変更 (2026-06-07 中原さん指示、レターパック→別配送方法等)
router.post('/api/tracking/change-method', (req, res) =>
  handle(res, () => changeShippingMethod(req.body || {}, currentUser(req))));

// 配送方法変更ログ (集計 + 詳細 + purge、永続集計は pd_tracking_import、詳細は 90 日 TTL)
router.get('/api/tracking/method-change/summary', (req, res) => handle(res, () =>
  getMethodChangeSummary({ days: parseInt(req.query.days, 10) || 30 })));
router.get('/api/tracking/method-change/log', (req, res) => handle(res, () => listMethodChangeLog({
  days: parseInt(req.query.days, 10) || 30,
  source: req.query.source || '',
  limit: parseInt(req.query.limit, 10) || 500,
})));
router.post('/api/admin/tracking/method-change/purge', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handle(res, () => purgeOldMethodChangeLog());
});

// 過去 exported バッチを走査して pd_shipment_tracking に backfill (2026-06-04 hotfix、admin専用)
// todayJstOnly (デフォ true) で「今日 JST にアップしたバッチだけ」に絞る
router.post('/api/admin/tracking/backfill-from-exported', (req, res) => {
  if (!requireAdmin(req, res)) return;
  const body = req.body || {};
  handle(res, () => backfillShipmentTrackingFromExportedBatches({
    dryRun: req.query.dryRun === '1' || body.dryRun === true,
    todayJstOnly: body.todayJstOnly !== false, // 明示的に false 以外はすべて true
  }, currentUser(req)));
});

// duplicate file_hash で再投入できない取込履歴を削除 (2026-06-04 hotfix、admin専用)
// body: { hoursBack?: number, sources?: string[], dryRun?: boolean }
router.post('/api/admin/tracking/imports/delete-recent', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handle(res, () => deleteRecentTrackingImports({
    hoursBack: Math.max(1, Math.min(72, parseInt((req.body || {}).hoursBack, 10) || 24)),
    sources: Array.isArray((req.body || {}).sources) ? req.body.sources : null,
    dryRun: req.query.dryRun === '1' || (req.body && req.body.dryRun === true),
  }, currentUser(req)));
});

// 未マッチ行を「反映に含める」(スタッフが NextEngine で実在確認した上で押下、2026-06-04)
// body: { ne_uketsuke_no, tracking_no, source, enrich? }
// admin 限定にはしない (スタッフ運用、ただし audit log は残す)
router.post('/api/tracking/unmatched/include', (req, res) =>
  handle(res, () => includeUnmatchedToTracking(req.body || {}, currentUser(req))));

// 定形外一括反映 (本日アップロード分の定形外 NE 受注番号を ready 化、Codex R1 Critical+High 対応)
// body: { ne_uketsuke_nos: [...] }
router.post('/api/tracking/bulk-apply-teikeigai', (req, res) =>
  handle(res, () => bulkApplyTeikeigai(req.body || {}, currentUser(req))));

// ── NE 手動反映用エクスポート (2026-06-04、cron 未実装の暫定運用) ──
// ready 状態の配送方法別 summary
router.get('/api/tracking/ready/summary', (req, res) =>
  handle(res, () => getReadyExportSummary({ scope: req.query.scope === 'all' ? 'all' : 'today' })));
// 配送方法を指定して NE 受注番号一覧 (JSON配列、コピー用)
router.get('/api/tracking/ready/uketsuke', (req, res) =>
  handle(res, () => getReadyNeUketsukeNos({
    method: String(req.query.method || '').trim(),
    scope: req.query.scope === 'all' ? 'all' : 'today',
  })));
// CSV ダウンロード (受注番号 + 追跡番号、Shift-JIS not、UTF-8 BOM)
router.get('/api/tracking/ready/csv', (req, res) => {
  try {
    const { csv, count } = getReadyTrackingCsv({
      method: String(req.query.method || '').trim(),
      scope: req.query.scope === 'all' ? 'all' : 'today',
    });
    const today = new Date().toISOString().slice(0,10).replace(/-/g, '');
    const filename = `ne-tracking-${today}-${count}rows.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

// ⑤ 受注番号+追跡番号 CSV を Google Drive 固定ファイル (ne-tracking.csv) へ上書き保存 (2026-07-19 中原さん指示)。
// 追跡番号タブの「📤 Drive保存」ボタンの本経路。tracking CSV は読み取り生成のみ (状態遷移/CAS 無し) なので、
// メイン出力のような二相ロック (beginExport/commitExport) は不要。ただし固定ファイルへの並行書込・世代逆転を
// 防ぐため専用の withTrackingDriveLock で直列化する (メイン出力 logi_dispatch.csv とは別チェーン)。
// Drive 保存が失敗したら fail-safe で base64 を返し、フロントで従来DLに代替 (SA未共有でも運用継続できる)。
router.post('/api/tracking/ready/csv/to-drive', (req, res) => handleAsync(res, () =>
  withTrackingDriveLock(async () => {
    // このファイルは常に「全件 (混在OK)」。method は受け付けず固定にする (Codex High:
    // 部分件数で ne-tracking.csv を上書きさせない)。scope のみ 本日/過去履歴に連動 (fail-closed)。
    const method = 'all';
    const scope = ((req.body && req.body.scope) === 'all') ? 'all' : 'today';
    // 確認ダイアログで見せた件数。必須・非負整数 (Codex High: 任意だと件数照合を迂回して上書きできる)。
    const rawExpected = req.body && req.body.expectedCount;
    if (!Number.isInteger(rawExpected) || rawExpected < 0) {
      const e = new Error('expectedCount (確認時の件数) が必要です'); e.code = 'VALIDATION'; throw e;
    }
    const expectedCount = rawExpected;
    const { csv, count } = getReadyTrackingCsv({ method, scope });
    // 空CSVで固定ファイルを上書きしない (API直叩き含む防御、Codex 補足)。
    if (count === 0) return { status: 'empty', count: 0 };
    // 確認時と保存直前で件数が変わっていたら上書きせず、フロントで最新件数を再確認させる (Codex High)。
    if (expectedCount !== count) {
      return { status: 'count_mismatch', count, expectedCount };
    }
    const buffer = Buffer.from(csv, 'utf-8'); // csv は UTF-8 BOM + CRLF 文字列
    try {
      const up = await uploadCsvToDrive(buffer, PD_TRK_DRIVE_FILE, PD_TRK_DRIVE_FOLDER);
      // 状態遷移が無い経路なので誰が/いつ/範囲/件数/fileId を監査ログに残す (Codex High: 追跡可能性)。
      // Drive 上書きは取消不能なので、監査書込みが失敗しても保存自体は成功扱い。ただし監査欠落は
      // サーバログに明示し、レスポンスにも audited を載せて後追いできるようにする (Codex High)。
      const audited = auditTrackingCsvDriveSave(
        { scope, count, action: up.action, file_id: up.fileId, filename: PD_TRK_DRIVE_FILE }, currentUser(req));
      if (!audited) console.error('[packing] tracking csv to-drive: 監査ログ書込み失敗 (Drive保存は成功)', { file_id: up.fileId, count, scope });
      return { status: 'saved', count, audited,
        folderName: up.folderName || null,
        folderUrl: `https://drive.google.com/drive/folders/${PD_TRK_DRIVE_FOLDER}`,
        drive: { action: up.action, fileId: up.fileId, filename: PD_TRK_DRIVE_FILE } };
    } catch (e) {
      console.error('[packing] tracking csv to-drive', e.message);
      // fail-safe: Drive 保存失敗時は base64 でCSVを返し、フロントで従来DLに代替させる
      return { status: 'drive_failed', count, message: e.message,
        filename: `ne-tracking-${count}rows.csv`, csvBase64: buffer.toString('base64') };
    }
  })));

// go-live 前確認: SA が tracking 保存先フォルダに書き込めるか (非破壊・admin専用)。
// ブラウザで /apps/packing-dispatch/api/tracking/ready/csv/drive-check を開いて確認する運用。
router.get('/api/tracking/ready/csv/drive-check', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handleAsync(res, async () => {
    const r = await probeFolderWritable(PD_TRK_DRIVE_FOLDER);
    return { folderId: PD_TRK_DRIVE_FOLDER,
      folderUrl: `https://drive.google.com/drive/folders/${PD_TRK_DRIVE_FOLDER}`,
      filename: PD_TRK_DRIVE_FILE, ...r };
  });
});

// ── NE 反映ジョブ (Phase 2 PR-A 2026-06-05、構成 B) ──
//
// UI endpoint (session 認証は server.js mount で効く):
//  POST /api/ne-sync/runs              start (新規 run 作成、単一起動ロック)
//  GET  /api/ne-sync/runs/:id          状態確認
//  GET  /api/ne-sync/runs              履歴一覧

// UI: start (新規 run 作成、単一起動ロック)
router.post('/api/ne-sync/runs', (req, res) => handle(res, () =>
  startNeSyncRun({ started_by: currentUser(req) })));

// UI: start-and-run (start-run + ミニPC worker 起動を 1 リクエストで実行、PR-C 構成 4)
// CF Access + 専用 Bearer (MINIPC_NE_SYNC_RUN_KEY) で ミニPC を叩く。
// secret は Render env のみ、UI には渡さない。
const MINIPC_BASE_URL = 'https://wh.bfaith-wh.uk'; // hardcode (SSRF 防御)

// ⏸️ 一時停止 (2026-06-06、有料 API 課金リスク調整中):
// shipped API が 1 件 1 call 必要で月 9,000-15,000 件運用だと NE 無料枠超で 30,000 円〜課金。
// bulkupdate + shipped_update_flag への切り替え検討中。UI ボタンも disable 済だが、
// API 直叩き / UI 改ざんを防ぐためサーバ側でも 503 で fail-closed。
const NE_SYNC_PAUSED = true;

router.post('/api/ne-sync/start-and-run', async (req, res) => {
  if (NE_SYNC_PAUSED) {
    return res.status(503).json({ ok: false, error: 'paused',
      message: 'NE 反映機能は一時停止中 (有料 API 課金リスク調整中)。当面は CSV エクスポートで運用してください。' });
  }
  // Phase 1: start-run (Render 側で run_id 発行)
  let run;
  try {
    run = startNeSyncRun({ started_by: currentUser(req) });
  } catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message });
    if (e.code === 'CONFLICT') return res.status(409).json({ ok: false, error: 'conflict', message: e.message });
    console.error('[ne-sync start]', e.message);
    return res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
  // Phase 2: ミニPC worker 起動 (HTTP POST、即返却を期待)
  const runKey = process.env.MINIPC_NE_SYNC_RUN_KEY || '';
  const cfId = process.env.CF_ACCESS_CLIENT_ID || '';
  const cfSecret = process.env.CF_ACCESS_CLIENT_SECRET || '';
  if (!runKey || !cfId || !cfSecret) {
    // fail-closed: run は既に作られているので complete(failed) してクリーンアップ
    try { completeNeSyncRun({ run_id: run.run_id, status: 'failed', last_error: 'env missing on Render' }); } catch {}
    return res.status(503).json({ ok: false, error: 'not_configured',
      message: 'MINIPC_NE_SYNC_RUN_KEY / CF_ACCESS_CLIENT_ID / CF_ACCESS_CLIENT_SECRET が未設定です' });
  }
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 15000); // 15秒で abort
    const r = await fetch(MINIPC_BASE_URL + '/ne-sync-control/start', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${runKey}`,
        'CF-Access-Client-Id': cfId,
        'CF-Access-Client-Secret': cfSecret,
      },
      body: JSON.stringify({ run_id: run.run_id }),
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    const text = await r.text();
    let data; try { data = JSON.parse(text); } catch { data = { ok: false, message: text.slice(0, 300) }; }
    if (!r.ok || !data.ok) {
      try { completeNeSyncRun({ run_id: run.run_id, status: 'failed', last_error: `worker spawn failed: HTTP ${r.status}` }); } catch {}
      return res.status(502).json({ ok: false, error: 'worker_start_failed',
        message: `ミニPC worker 起動失敗: HTTP ${r.status}: ${data && data.message || ''}`,
        run_id: run.run_id });
    }
    return res.json({ ok: true, result: { run_id: run.run_id, pid: data.pid || null, started_at: run.started_at } });
  } catch (e) {
    try { completeNeSyncRun({ run_id: run.run_id, status: 'failed', last_error: `worker comm error: ${e.message}` }); } catch {}
    return res.status(502).json({ ok: false, error: 'worker_unreachable',
      message: `ミニPC に到達できません: ${e.message}`, run_id: run.run_id });
  }
});

// UI: run の状態確認
router.get('/api/ne-sync/runs/:id', (req, res) => handle(res, () => getNeSyncRun(req.params.id)));

// UI: 履歴一覧
router.get('/api/ne-sync/runs', (req, res) => handle(res, () => listRecentNeSyncRuns(req.query.limit)));

// 取込履歴
router.get('/api/tracking/imports', (req, res) => handle(res, () => listTrackingImports()));
router.get('/api/tracking/imports/:id', (req, res) => handle(res, () => getTrackingImportDetail(req.params.id)));

// ── 現在ユーザー情報 (UI で admin 専用機能の出し分け用) ──
router.get('/api/me', (req, res) => handle(res, () => ({
  email: req.session?.email || null,
  displayName: req.session?.displayName || null,
  role: req.session?.role || null,
})));

// ── 初期データ投入 (Excel移行シード)。未投入時のみ。force=1 で再投入(全上書き)。admin 限定 ──
router.post('/api/admin/load-seed', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handle(res, () => loadSeed({ force: req.query.force === '1' || (req.body && req.body.force === true) }));
});

// ── マスタ移行: ネコポス×手動出荷 → ネコポス×meltline (admin専用、1回限り想定) ──
// dry-run: 件数・サンプル・5分有効な dryRunToken を返す
router.post('/api/admin/meltline-migration/preview', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handle(res, () => getMeltlineMigrationPreview());
});
// 本実行: { dryRunToken, confirm: "メルトライン移行を実行" } を body で受ける
router.post('/api/admin/meltline-migration/execute', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handle(res, () => executeMeltlineMigration(req.body || {}, currentUser(req)));
});
// バックアップ一覧
router.get('/api/admin/meltline-migration/backups', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handle(res, () => listMeltlineBackups());
});
// バックアップ JSON ダウンロード (path traversal 防御は service 内)
router.get('/api/admin/meltline-migration/backup/:filename', (req, res) => {
  if (!requireAdmin(req, res)) return;
  try {
    const json = readMeltlineBackup(req.params.filename);
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(req.params.filename)}"`);
    res.send(json);
  } catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message });
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
});
// ロールバック: { backup_file, confirm: "メルトライン移行をロールバック" }
router.post('/api/admin/meltline-migration/rollback', (req, res) => {
  if (!requireAdmin(req, res)) return;
  handle(res, () => rollbackMeltlineMigration(req.body || {}, currentUser(req)));
});

export default router;

// ── NE 反映 worker 専用 router (Codex R1 High 1 対応、2026-06-05) ──
//
// server.js で `/apps/packing-dispatch/api/ne-sync-worker` に session 認証 bypass で mount される。
// requireAppAccess('packing-dispatch') が手前で効くと、Express session を持たない miniPC が
// 401/403 で弾かれて到達できなくなるため、UI 用の本体 router とは別 mount する。
//
// 認証は requireWorkerKey (Bearer、fail-closed) のみで完結。
//
// path:
//   POST /:run_id/queue    - ready 行を claim (worker pull)
//   POST /:run_id/results  - バッチ結果反映 (worker push)
//   POST /:run_id/complete - run finalize
export const neSyncWorkerRouter = Router();
neSyncWorkerRouter.use(requireWorkerKey);

neSyncWorkerRouter.post('/:run_id/queue', (req, res) => handle(res, () =>
  claimReadyForNeSync({ run_id: req.params.run_id })));

neSyncWorkerRouter.post('/:run_id/results', (req, res) => handle(res, () =>
  applyNeSyncResults({ run_id: req.params.run_id, results: (req.body || {}).results })));

neSyncWorkerRouter.post('/:run_id/complete', (req, res) => handle(res, () =>
  completeNeSyncRun({ run_id: req.params.run_id, status: (req.body || {}).status, last_error: (req.body || {}).last_error })));
