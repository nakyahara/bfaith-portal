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
import { fileURLToPath } from 'url';
import multer from 'multer';
import {
  initPackingDB, getDB, jstToday, listPackBatches, getPackBatch, listPackSlips,
  listPackLinesBySlip, STATUS_LABELS, MATCH_LABELS,
  createDevice, verifyDevice, revokeDevice, listDevices,
} from './db.js';
import { getPollerStatus, markLedgerImported } from './drive-sync.js';
import {
  parseCs03003, importPackBatch, checkPickingMatch, PackError,
  deriveFolderName, isStaleSagyoDate, WARN_LABELS, getWorkState, applyEvent,
  PAUSE_REASONS, UNDO_REASONS, SHIP_CHANGE_REASONS, SHIP_CHANGE_METHOD_OPTIONS, lastDoneSeqOf, getDailySummary,
  resolveIncident, lineKindOf, batchHikiateClass, listLineRuns, lineDailyTotal,
} from './service.js';
import { notifyShipChange, notifyTask, notifyReprint, postReprintText } from './notify.js';
import { extractReprintPdf, cleanupReprintPdfs, REPRINTS_DIR } from './reprint-pdf.js';
import { enqueuePackBatchNotionSync } from './notion.js';
import { listNouhinCsvFiles, downloadNouhinCsv, driveCall } from './drive.js';
// 商品画像は picking の楽天白抜きキャッシュ (pk_product_images) を共通部品として流用 (要件§7.1)
import { getImageMap, queueEnsureImages } from '../picking/images.js';

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
        return res.status(e.status).json({ error: e.message, code: e.code });
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

// ─── バッチ一覧 ───
router.get('/', (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const batches = listPackBatches(workDate);
  // 引当分類名 (表示) とライン種別 (pas/melt/null — 作業ボタンの振り分け)
  for (const b of batches) {
    b.hikiateClass = batchHikiateClass(getDB(), b);
    b.lineKind = lineKindOf(b.hikiateClass);
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
router.get('/batches/:id(\\d+)', (req, res) => {
  const batch = getPackBatch(Number(req.params.id));
  if (!batch) return res.status(404).send('バッチが見つかりません');
  const linesBySlip = listPackLinesBySlip(batch.id);
  const slips = listPackSlips(batch.id).map((s) => ({
    ...s,
    warns: s.warn_json ? JSON.parse(s.warn_json) : [],
    comments: s.comments_json ? JSON.parse(s.comments_json) : {},
    lines: linesBySlip.get(s.id) || [],
  }));
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
function decorateSlips(state) {
  const skus = [...new Set(state.slips.flatMap((s) => s.lines.map((l) => l.sku)))];
  const images = getImageMap(skus);
  // 取込時の解決キューが再起動等で消えていても、画面を開いた時点で自己修復する (picking と同じ)
  const missing = skus.map((s) => String(s ?? '').trim().toLowerCase())
    .filter((sku) => sku && !images.has(sku));
  if (missing.length > 0) queueEnsureImages(missing, `packing-batch:${state.batch.id}`);
  for (const s of state.slips) {
    for (const l of s.lines) {
      l.imageUrl = images.get(String(l.sku ?? '').trim().toLowerCase())?.url || null;
    }
  }
  return state;
}

router.get('/work/:id(\\d+)', (req, res) => {
  let state;
  try {
    state = decorateSlips(getWorkState(Number(req.params.id)));
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
  res.render(path.join(__dirname, 'views/work'), {
    title: `梱包 | ${state.batch.folder_name || state.batch.tb_key}`,
    displayName: req.session?.displayName,
    workers: listWorkers(),
    state,
    hikiateClass,
    warnLabels: WARN_LABELS,
    pauseReasons: PAUSE_REASONS,
    undoReasons: UNDO_REASONS,
    shipChangeReasons: SHIP_CHANGE_REASONS,
    // 提案候補 = 固定リスト (中原さん指定 2026-08-16)。判定サービス委譲 (packing-dispatch) はPhase 3
    methodOptions: SHIP_CHANGE_METHOD_OPTIONS,
  });
});

// ─── ライン管理画面 (梱包機 PAS/MELT — 紙台帳の置き換え。要件v7) ───
router.get('/line/:id(\\d+)', (req, res) => {
  const batch = getPackBatch(Number(req.params.id));
  if (!batch) return res.status(404).send('バッチが見つかりません');
  const hikiateClass = batchHikiateClass(getDB(), batch);
  const kind = lineKindOf(hikiateClass);
  if (!kind) return res.redirect(`/apps/packing/work/${batch.id}`);   // 手梱包は従来画面へ
  res.render(path.join(__dirname, 'views/line'), {
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
  const result = applyEvent(Number(req.params.id), {
    opId: req.body.op_id,
    event: req.body.event,
    slipSeq: req.body.slip_seq == null ? null : Number(req.body.slip_seq),
    clientAt: req.body.client_at || null,
    reason: req.body.reason || null,
    jumped: !!req.body.jumped,
    proposedMethod: req.body.proposed_method || null,
    sku: req.body.sku || null,
    actualSku: req.body.actual_sku || null,
    qty: req.body.qty == null ? null : Number(req.body.qty),
    // '' は未入力として null に落とす (Number('')===0 で「0件」と誤解釈しない — Codex medium)
    finalCount: req.body.final_count == null || req.body.final_count === '' ? null : Number(req.body.final_count),
    manualCount: req.body.manual_count == null || req.body.manual_count === '' ? null : Number(req.body.manual_count),
    excludedCount: req.body.excluded_count == null || req.body.excluded_count === '' ? null : Number(req.body.excluded_count),
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
  if (result.taskNotify) {
    notifyTask(result.taskNotify, worker)
      .catch((e) => console.warn(`[packing-notify] タスク通知失敗 (${result.taskNotify.sku}): ${e.message}`));
  }
  // 🖨 再印刷依頼: 押した瞬間に即時通知 (中原さん指示 2026-08-21・梱包の途中でも飛ぶ)。
  // テキスト通知を先に送り、送り状PDFの抜き出しは非同期で追送 (Codex high: Drive処理を
  // await すると「即時」にならず応答も塞ぐ)。webhook成功→DB更新間のクラッシュでは
  // 再送により同内容が重複し得る (NE伝票番号併記で判別可能・webhookにexactly-onceは無い)
  if (req.body.event === 'reprint' && !result.replayed && result.reprintId) {
    const row = getDB().prepare('SELECT * FROM pk_pack_reprints WHERE id=?').get(result.reprintId);
    if (row) {
      const lines = getDB().prepare(`
        SELECT COALESCE(l.print_name, l.product_name) AS name, l.sku, l.qty
        FROM pk_pack_lines l JOIN pk_pack_slips s ON s.id = l.slip_id
        WHERE s.batch_id=? AND s.seq=? ORDER BY l.id
      `).all(row.batch_id, row.slip_seq);
      try {
        const sent = await notifyReprint({
          folderName: row.folder_name, slipSeq: row.slip_seq, neSlipNo: row.ne_slip_no,
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
          const r = await extractReprintPdf({
            folderName: row.folder_name, neSlipNo: row.ne_slip_no, recipientName: row.recipient_name,
          });
          const url = `https://picking.bfaith-wh.uk/apps/packing/reprints/${r.token}.pdf`;
          getDB().prepare('UPDATE pk_pack_reprints SET pdf_token=? WHERE id=?').run(r.token, row.id);
          await postReprintText(`📄 ${row.ne_slip_no} の送り状PDF (該当ページのみ・${r.file}): ${url}`);
        } catch (e) {
          getDB().prepare('UPDATE pk_pack_reprints SET pdf_error=? WHERE id=?')
            .run(String(e.message).slice(0, 120), row.id);
          postReprintText(`⚠ ${row.ne_slip_no} の送り状PDFは自動で抜き出せませんでした (${String(e.message).slice(0, 80)}) — フォルダから該当分を印刷してください`)
            .catch(() => {});
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
        const sent = await notifyShipChange({
          folderName: row.folder_name, neSlipNo: row.ne_slip_no,
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
  // 形式チェック+在庫データへの実在照会。名前もサーバー由来のみ採用 (通知への注入防止)
  let actualSku = req.body.actual_sku == null ? null : String(req.body.actual_sku).trim().toLowerCase();
  let actualName = null;
  // 確定時は「実際に使われるSKU」を必ず検証対象にする (保存済み actual_sku での迂回防止 — Codex 2巡目)
  if (decision === 'confirm' && !actualSku) {
    const incRow = getDB().prepare('SELECT kind, actual_sku FROM pk_pack_incidents WHERE id=?')
      .get(Number(req.params.iid));
    if (incRow?.kind === 'wrong_item' && incRow.actual_sku) {
      actualSku = String(incRow.actual_sku).trim().toLowerCase();
    }
  }
  if (actualSku) {
    if (!/^[a-z0-9][a-z0-9_\-.]{0,79}$/.test(actualSku)) {
      throw new PackError(400, 'bad_sku', 'SKUの形式が不正です。検索から選んでください');
    }
    try {
      const { fetchStockSearch, stockLookupConfigured } = await import('../picking/stock-locations.js');
      if (stockLookupConfigured()) {
        const hit = (await fetchStockSearch(actualSku))?.items
          ?.find((it) => String(it.sku).toLowerCase() === actualSku);
        if (!hit) throw new PackError(400, 'unknown_sku', 'そのSKUは在庫データに見つかりません。検索から選び直してください');
        actualName = hit.name || null;
      }
    } catch (e) {
      if (e instanceof PackError) throw e;
      // 在庫参照の一時障害では形式チェックのみで通す (現場の送信を止めない・fail-soft)
      console.warn(`[packing] 実物SKUの在庫照会失敗 (形式チェックのみで続行): ${e.message}`);
    }
  }
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
      const info = { kind: t.kind, sku: t.sku, name: t.product_name, qty: t.req_qty, folder: t.folder_name, slipSeq: t.slip_seq };
      if (t.kind === 'repick') {
        try {
          info.repickBatchId = picking?.createRepickBatch ? picking.createRepickBatch(t).batchId : null;
        } catch (e) { console.warn(`[packing] ピッキング漏れバッチ作成失敗 (task=${t.id}): ${e.message}`); }
        if (t.location) info.locationLabel = picking?.formatLocation ? picking.formatLocation(t.block, t.location) : t.location;
      }
      if (t.kind === 'return' && stock?.stockLookupConfigured?.()) {
        try {
          info.stockText = stock.buildStockLocationsText(
            await stock.fetchStockLocations(t.sku), { title: '在庫ロケーション (戻し先の参考)' });
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

/** 作業状態の再取得 (リロード・オンライン復帰時の同期用)。 */
router.get('/api/batches/:id(\\d+)/state', api(async (req, res) => {
  const s = getWorkState(Number(req.params.id));
  res.json({
    ok: true,
    batchStatus: s.batch.status,
    worker: s.batch.worker,
    currentSeq: s.currentSeq,
    doneCount: s.doneCount,
    slipCount: s.slips.length,
    doneSeqs: s.slips.filter((x) => x.status === 'done').map((x) => x.seq),
    heldSeqs: s.slips.filter((x) => x.status === 'held').map((x) => x.seq),
    repickSeqs: s.slips.filter((x) => x.status === 'held' && x.hold_reason === 'repick').map((x) => x.seq),
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
router.get('/admin/summary', requireAdmin, (req, res) => {
  const workDate = isRealDate(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  res.render(path.join(__dirname, 'views/admin_summary'), {
    title: '梱包サマリ',
    username: req.session.email,
    displayName: req.session.displayName,
    isAdmin: true,
    summary: getDailySummary(workDate),
    statusLabels: STATUS_LABELS,
  });
});

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
    icons: [{ src: '/favicon.png', sizes: '192x192', type: 'image/png' }],
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
  });
});

/**
 * この端末 (iPad) を登録する。発行トークンは httpOnly Cookie としてこの端末にだけ渡す。
 * ⭐登録と同時に管理者セッションを破棄する (共用端末に管理者権限を残さない — picking と同規約)。
 * 🚨ホーム画面に追加したPWA内で実行すること (SafariとPWAはCookie保存領域が別)
 */
router.post('/admin/devices', checkOrigin, requireAdmin, api(async (req, res) => {
  const label = String(req.body.label || '').trim();
  if (!label || label.length > 40) throw new PackError(400, 'bad_label', '端末名を1〜40文字で入力してください');
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
