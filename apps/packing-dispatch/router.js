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
  listRules, upsertRules, copyRules, searchRules, searchRulesByCondition, bulkUpdateRules,
  listUnregistered, mirrorFreshness, searchAssort, updateAssort, searchProducts,
  getMeltlineMigrationPreview, executeMeltlineMigration, rollbackMeltlineMigration,
  listMeltlineBackups, readMeltlineBackup,
  importTrackingCsv, setTrackingManual, markReady, markSkipped,
  trackingSummary, listTracking, listTrackingImports, getTrackingImportDetail,
  listTrackingByMethod, listMissingTracking, setTrackingOne,
  listTodayImportLinesByMethod,
  getMethodChangeSummary, listMethodChangeLog, purgeOldMethodChangeLog,
  backfillShipmentTrackingFromExportedBatches, deleteRecentTrackingImports,
  includeUnmatchedToTracking, bulkApplyTeikeigai,
  getReadyExportSummary, getReadyNeUketsukeNos, getReadyTrackingCsv,
  startNeSyncRun, getNeSyncRun, listRecentNeSyncRuns,
  claimReadyForNeSync, applyNeSyncResults, completeNeSyncRun,
} from './service.js';
import crypto from 'node:crypto';
import { loadSeed } from './tools/load-shipping-rule-seed.mjs';

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

// 出力 (Shift-JIS)。premeltline=1 で MeltLine を手動出荷に落として出力 (MeltLine 導入前用)。
router.get('/api/export/:id', (req, res) => {
  try {
    const premeltline = req.query.premeltline === '1';
    const { buffer, rowCount } = exportCsv(req.params.id, currentUser(req), { downgradeMeltline: premeltline });
    const fname = `logi_dispatch_${premeltline ? 'premelt_' : ''}${rowCount}rows.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=Shift_JIS');
    res.setHeader('Content-Disposition', `attachment; filename="${fname}"`);
    res.send(buffer);
  } catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ ok: false, error: 'validation', message: e.message, detail: e.detail });
    console.error('[packing] export', e.message);
    res.status(500).json({ ok: false, error: 'server_error', message: e.message });
  }
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
router.get('/api/tracking/summary', (req, res) => handle(res, () => trackingSummary()));

// 一覧 (filter: status / source / shop / q)
router.get('/api/tracking', (req, res) => handle(res, () => listTracking({
  status: req.query.status, source: req.query.source, shop: req.query.shop, q: req.query.q,
})));

// 配送方法別一覧 (定形外/レターパック 等の特定方法だけ抽出、synced/skipped 除外) - pd_shipment_tracking 起点 (旧版、互換用)
router.get('/api/tracking/by-method', (req, res) => handle(res, () => listTrackingByMethod(req.query.method)));

// 「本日アップロード」の取込バッチから配送方法別に抽出 (定形外/レターパック表示用、商品名付き)
router.get('/api/tracking/today-by-method', (req, res) => handle(res, () => listTodayImportLinesByMethod(req.query.method)));

// 追跡番号未割当アラート (定形外/レターパック/AES 以外で tracking_no が NULL の pending/error)
router.get('/api/tracking/missing', (req, res) => handle(res, () => listMissingTracking()));

// レターパック 1 件保存 (body: { ne_uketsuke_no, tracking_no, source })
router.post('/api/tracking/set-one', (req, res) => handle(res, () => setTrackingOne(req.body || {}, currentUser(req))));

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
  handle(res, () => getReadyExportSummary()));
// 配送方法を指定して NE 受注番号一覧 (JSON配列、コピー用)
router.get('/api/tracking/ready/uketsuke', (req, res) =>
  handle(res, () => getReadyNeUketsukeNos({ method: String(req.query.method || '').trim() })));
// CSV ダウンロード (受注番号 + 追跡番号、Shift-JIS not、UTF-8 BOM)
router.get('/api/tracking/ready/csv', (req, res) => {
  try {
    const { csv, count } = getReadyTrackingCsv({ method: String(req.query.method || '').trim() });
    const today = new Date().toISOString().slice(0,10).replace(/-/g, '');
    const filename = `ne-tracking-${today}-${count}rows.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(csv);
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
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
