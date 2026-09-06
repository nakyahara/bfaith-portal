/**
 * 納品書CSV (CS03003) の Drive 自動取込ポーラー。
 *
 * 別プログラム (伝票出しPC) が毎朝 各出荷_XX フォルダへ配置する 納品書_出荷_XX.csv を
 * 2分間隔で拾って取り込む (picking の drive-sync.js と同設計)。
 *   - 冪等は二層: 台帳 pk_pack_drive_imports (drive_file_id+modified_time=版) と
 *     importPackBatch の tb_key+csv_sha256 (手動取込との競合は replay に落ちる)
 *   - 書き込み途中のファイルを掴まない: modified_time から60秒待ってから取り込む
 *   - 自動取込は「突合ok かつ 出荷作業日=今日」のときだけ確定する。
 *     no_picking (CS03002がまだ) は数分後に解消し得るため retry (失敗扱い・上限あり)、
 *     mismatch と stale は人の承認が必要なので取り込まず台帳に skipped で残す
 *     (管理画面から match_ack / date_ack つきで手動取込する)
 *   - 失敗は台帳に残し管理画面で見える (黙殺しない)
 *
 * ポーラーは standalone (ミニPC) だけが起動する。
 * 定期実行の台帳: config/jobs-registry.mjs の packing-drive-poller を参照。
 */
import { getDB, utcNow } from './db.js';
import { notifyShipChange, notifyReprint, postMaterialText, materialWebhookConfigured, postReprintText, postMissText, missWebhookConfigured, notifyStockout } from './notify.js';
import { missWatchStep } from './miss-watch.js';
import { sweepPrintJobs, pendingAlerts, markAlerted, alertTextFor } from './print-queue.js';
import { materialNotifyStep, purgeOldViews } from './materials.js';
import { cleanupReprintPdfs } from './reprint-pdf.js';
import {
  parseCs03003, importPackBatch, checkPickingMatch, isStaleSagyoDate, PackError,
  listPendingStockoutNotifies, countStaleStockoutNotifies, claimStockoutNotify, markStockoutNotify,
} from './service.js';
import { listPendingReturnedNotifies, claimReturnedNotify, markReturnedNotify, countStaleReturnedNotifies } from './service.js';
import { notifyReturned } from './notify.js';
import { getShippingFolders, listNouhinCsvFiles, downloadNouhinCsv } from './drive.js';

/**
 * ポーリング対象のCSV候補を選ぶ (純関数)。
 * @returns 安定した (更新から60秒以上・3日以内の) 納品書CSV
 */
export function pickPollCandidates(files, nowMs = Date.now()) {
  const STABLE_MS = 60_000;
  const WINDOW_MS = 3 * 24 * 3600 * 1000;
  return files.filter((f) => {
    if (!/\.csv$/i.test(f.filename)) return false;
    if (!f.filename.includes('納品書')) return false;
    const t = Date.parse(f.modified_time || '');
    if (!Number.isFinite(t)) return false;
    return nowMs - t >= STABLE_MS && nowMs - t <= WINDOW_MS;
  });
}

// no_picking はピッキング側CSVの到着待ちの可能性があるため長めに再試行する
// (2分間隔×15回=30分)。パース失敗等は5回で打ち止め (picking と同じ)
const MAX_ATTEMPTS = 5;
const MAX_ATTEMPTS_NO_PICKING = 15;

/**
 * 1回分のポーリング。取込は個別fail-soft (1件の失敗が他を止めない)。
 * @returns {{checked, imported, replayed, failed, skipped}}
 */
export async function pollOnce(deps = {}) {
  const db = getDB();
  const now = utcNow();
  const stats = { checked: 0, imported: 0, replayed: 0, failed: 0, skipped: 0 };

  const files = await (deps.listFiles || listNouhinCsvFiles)();
  const candidates = pickPollCandidates(files, deps.nowMs);
  const getLedger = db.prepare(
    'SELECT status, attempts, error FROM pk_pack_drive_imports WHERE drive_file_id = ? AND modified_time = ?'
  );
  const upsertLedger = db.prepare(`
    INSERT INTO pk_pack_drive_imports
      (drive_file_id, modified_time, filename, folder_name, status, error, batch_id, attempts, first_seen_at, processed_at)
    VALUES (@drive_file_id, @modified_time, @filename, @folder_name, @status, @error, @batch_id, 1, @now, @now)
    ON CONFLICT(drive_file_id, modified_time) DO UPDATE SET
      status = excluded.status, error = excluded.error, batch_id = excluded.batch_id,
      attempts = pk_pack_drive_imports.attempts + 1, processed_at = excluded.processed_at
  `);

  for (const f of candidates) {
    stats.checked++;
    const prev = getLedger.get(f.file_id, f.modified_time);
    if (prev && (prev.status === 'imported' || prev.status === 'skipped')) continue;
    if (prev && prev.status === 'failed') {
      const cap = /no_picking/.test(prev.error || '') ? MAX_ATTEMPTS_NO_PICKING : MAX_ATTEMPTS;
      if (prev.attempts >= cap) continue;
    }

    const folderName = /^出荷_\d+$/.test(f.parent_name || '') ? f.parent_name : null;
    const ledger = (status, error, batchId = null) => upsertLedger.run({
      drive_file_id: f.file_id, modified_time: f.modified_time, filename: f.filename,
      folder_name: folderName, status, error, batch_id: batchId, now,
    });

    try {
      const dl = await (deps.download || downloadNouhinCsv)(f.file_id);
      // 選定後にファイルが更新されていたら、この周期では取り込まない (安定待ちのすり抜け防止)
      if (dl.modified_time && dl.modified_time !== f.modified_time) continue;
      const preview = parseCs03003(dl.buffer);

      // 自動確定できない条件は人の承認へ回す (skipped=この版はもう触らない)。
      // 作業日の混在CSVは parseCs03003 が mixed_sagyo_date で fail-closed するため、
      // ここに来る preview.sagyoDate は常に単一日 = 「全日付が今日」の判定として機能する
      if (isStaleSagyoDate(preview.sagyoDate)) {
        ledger('skipped', `stale: 出荷作業日 ${preview.sagyoDate} が今日ではありません (手動取込で date_ack)`);
        stats.skipped++;
        continue;
      }
      const match = checkPickingMatch(preview);
      if (match.status === 'mismatch') {
        ledger('skipped', `mismatch: ピッキングと${match.diffs.length}件の差分 (手動取込で match_ack)`);
        stats.skipped++;
        continue;
      }
      if (match.status === 'no_picking') {
        // CS03002 がまだ届いていないだけの可能性が高い → failed 扱いで再試行
        ledger('failed', 'no_picking: ピッキング側に同一バッチがまだありません (再試行します)');
        stats.failed++;
        continue;
      }

      const result = importPackBatch(preview, { folderName }, 'drive-poller');
      ledger('imported', null, result.batchId);
      if (result.replayed) stats.replayed++;
      else {
        stats.imported++;
        console.log(`[packing-drive-poller] 取込: ${folderName || f.filename} (${preview.slipCount}伝票)`);
      }
    } catch (e) {
      if (e instanceof PackError && e.code === 'already_started') {
        ledger('skipped', e.message);
        stats.skipped++;
        continue;
      }
      stats.failed++;
      ledger('failed', String(e.message).slice(0, 300));
      console.warn(`[packing-drive-poller] 取込失敗 (${f.filename}): ${e.message}`);
    }
  }
  return stats;
}

/**
 * 手動取込 (管理画面のDrive取込) の成功を台帳へ反映する。
 * mismatch/stale で skipped になっていた版を imported に遷移させ、
 * 取込画面の「要確認」表示を消す (Codex PR-A medium)。
 */
export function markLedgerImported({ fileId, modifiedTime, filename, folderName, batchId }) {
  if (!fileId || !modifiedTime) return;
  getDB().prepare(`
    INSERT INTO pk_pack_drive_imports
      (drive_file_id, modified_time, filename, folder_name, status, error, batch_id, attempts, first_seen_at, processed_at)
    VALUES (?, ?, ?, ?, 'imported', '手動取込', ?, 1, ?, ?)
    ON CONFLICT(drive_file_id, modified_time) DO UPDATE SET
      status = 'imported', error = '手動取込', batch_id = excluded.batch_id,
      processed_at = excluded.processed_at
  `).run(fileId, modifiedTime, filename || '', folderName || null, batchId, utcNow(), utcNow());
}

// ─── 常駐ポーリング (standalone のみが起動する) ───

const POLL_INTERVAL_MS = Number(process.env.PACKING_POLL_INTERVAL_SEC || 120) * 1000;
let _status = { running: false, lastAt: null, lastError: null, lastStats: null };
let _polling = false;

// jobs-monitor への生存 ping (dead-man方式・台帳 id=packing-drive-poller)。
// ポーリング成功時のみ・1時間に1回へ間引き (picking-drive-poller と同方式)
const PING_THROTTLE_MS = 3600_000;
let _lastPingAt = 0;
let _warnedNoToken = false;
async function pingJobsMonitor(fetchFn = fetch) {
  const token = process.env.JOBS_MONITOR_TOKEN;
  if (!token) {
    if (!_warnedNoToken) {
      _warnedNoToken = true;
      console.warn('[packing-drive-poller] JOBS_MONITOR_TOKEN 未設定 → 生存pingなし (台帳の監視が効かない)');
    }
    return;
  }
  if (Date.now() - _lastPingAt < PING_THROTTLE_MS) return;
  try {
    const base = (process.env.JOBS_MONITOR_URL || 'https://bfaith-portal.onrender.com').replace(/\/+$/, '');
    const res = await fetchFn(`${base}/apps/jobs-monitor/ping/packing-drive-poller?status=ok`, {
      method: 'POST', headers: { authorization: `Bearer ${token}` },
    });
    if (res.ok) _lastPingAt = Date.now();
    else console.warn(`[packing-drive-poller] 生存ping失敗: HTTP ${res.status}`);
  } catch (e) {
    console.warn(`[packing-drive-poller] 生存ping失敗: ${e.message}`);
  }
}

/**
 * 🖨 出荷PCの印刷エージェントの生存を jobs-monitor へ中継する (台帳 id=slip-print-agent)。
 *
 * ⭐ping を**エージェント自身に打たせない**のは、出荷PCへ JOBS_MONITOR_TOKEN をもう1つ
 *   配らずに済ませるため。エージェントの生存は heartbeat_at として既に miniPC が持っている。
 * ⭐エージェントが一度も登録されていない間は ping しない (まだ導入していないものを
 *   「止まっている」と鳴らさない)。登録後に途切れたら台帳の max_age_hours で検知される。
 */
const AGENT_ALIVE_MS = 10 * 60 * 1000;   // heartbeat 45秒間隔の十数倍
let _lastAgentPingAt = 0;
async function pingPrintAgentAlive(fetchFn = fetch) {
  const token = process.env.JOBS_MONITOR_TOKEN;
  if (!token) return;
  if (Date.now() - _lastAgentPingAt < PING_THROTTLE_MS) return;
  let alive = null;
  try {
    alive = getDB().prepare(`SELECT label, heartbeat_at FROM pk_pack_devices
      WHERE kind='agent' AND revoked_at IS NULL AND heartbeat_at IS NOT NULL
      ORDER BY heartbeat_at DESC LIMIT 1`).get();
  } catch { return; }
  if (!alive) return;                                        // 未導入 → 監視対象にしない
  if (Date.now() - Date.parse(alive.heartbeat_at) > AGENT_ALIVE_MS) return;   // 途切れている
  try {
    const base = (process.env.JOBS_MONITOR_URL || 'https://bfaith-portal.onrender.com').replace(/\/+$/, '');
    const res = await fetchFn(`${base}/apps/jobs-monitor/ping/slip-print-agent?status=ok`, {
      method: 'POST', headers: { authorization: `Bearer ${token}` },
    });
    if (res.ok) _lastAgentPingAt = Date.now();
    else console.warn(`[packing-drive-poller] 印刷エージェントの生存ping失敗: HTTP ${res.status}`);
  } catch (e) {
    console.warn(`[packing-drive-poller] 印刷エージェントの生存ping失敗: ${e.message}`);
  }
}

/**
 * ④通知の再送 (直近2日・未通知のみ・1周期3件まで)。
 * 事務キュー廃止後の配送保証 (Codexレビュー high) — DBの行が正本で、通知はここで追いつく
 */
async function retryShipChangeNotify() {
  const db = getDB();
  let rows = [];
  try {
    rows = db.prepare(`
      SELECT * FROM pk_pack_ship_changes
      WHERE notified_at IS NULL AND created_at >= datetime('now', '-2 days')
      ORDER BY id LIMIT 3
    `).all();
  } catch { return; }   // v5未適用
  for (const row of rows) {
    try {
      const lines = db.prepare(`
        SELECT COALESCE(l.print_name, l.product_name) AS name, l.sku, l.qty
        FROM pk_pack_lines l JOIN pk_pack_slips s ON s.id = l.slip_id
        WHERE s.batch_id=? AND s.seq=? ORDER BY l.id
      `).all(row.batch_id, row.slip_seq);
      // ⭐初回送信 (router) と同じ内容にする — 片方だけ直すと再送で番号が落ちる
      const slipNo = db.prepare('SELECT slip_no FROM pk_pack_slips WHERE batch_id=? AND seq=?')
        .get(row.batch_id, row.slip_seq)?.slip_no ?? null;
      const sent = await notifyShipChange({
        folderName: row.folder_name, neSlipNo: row.ne_slip_no, slipNo,
        currentMethod: row.current_method, proposedMethod: row.proposed_method,
        reason: `${row.reason} (再送)`, worker: row.requested_by, lines,
      });
      if (sent) {
        db.prepare('UPDATE pk_pack_ship_changes SET notified_at=?, notify_error=NULL WHERE id=?')
          .run(utcNow(), row.id);
        console.log(`[packing-drive-poller] 配送変更通知を再送: ${row.ne_slip_no}`);
      }
    } catch (e) {
      db.prepare('UPDATE pk_pack_ship_changes SET notify_error=? WHERE id=?')
        .run(String(e.message).slice(0, 200), row.id);
    }
  }
}

/**
 * 🚫 出荷保留 (在庫なし) 通知の再送 (直近2日・未通知のみ・1周期3件まで — ④と同型)。
 * 「在庫なしを確認」は伝票を閉じる強い操作で、事務通知が業務上必須 (Q1 決定 2026-09-05) —
 * 送信前に落ちても outbox 行 (pk_pack_stockouts) が残るのでここで追いつく
 */
async function retryStockoutNotify() {
  let rows = [];
  try {
    rows = listPendingStockoutNotifies(3);   // 期間で切らない (2日で打ち切ると未通知が永久に残る — Codex R2 High)
    const stale = countStaleStockoutNotifies();
    if (stale > 0) console.error(`[packing-drive-poller] 出荷保留 (在庫なし) の未通知が2日以上滞留: ${stale}件 (webhook/ポーラーを確認)`);
  } catch { return; }   // v20未適用
  for (const row of rows) {
    if (!claimStockoutNotify(row.id)) continue;   // router が送信中
    try {
      let items = [];
      try { items = JSON.parse(row.items_json) || []; } catch { items = []; }
      const sent = await notifyStockout({
        folder: row.folder_name, slipSeq: row.slip_seq, neSlipNo: row.ne_slip_no,
        siteOrderNo: row.site_order_no, recipientName: row.recipient_name,
        worker: `${row.worker} (再送)`, items,
      });
      markStockoutNotify(row.id, sent, sent ? null : 'webhook未設定');
      if (sent) console.log(`[packing-drive-poller] 出荷保留 (在庫なし) 通知を再送: ${row.ne_slip_no}`);
    } catch (e) {
      markStockoutNotify(row.id, false, e.message);
    }
  }
}

/**
 * ↩ 棚戻し完了 (戻したロケ) 通知の再送 — outbox は pk_pack_tasks.returned_notified_at (例外処理監査 PR-4・Q3)。
 * 3階の「ここへ戻した」の直後に picking router が送るが、落ちればここで追いつく (期間で切らない)
 */
async function retryReturnedNotify() {
  let rows = [];
  try {
    rows = listPendingReturnedNotifies(3);
    const stale = countStaleReturnedNotifies();
    if (stale > 0) console.error(`[packing-drive-poller] 棚戻し完了の未通知が2日以上滞留: ${stale}件 (webhook/ポーラーを確認)`);
  } catch { return; }   // v21 未適用
  for (const row of rows) {
    if (!claimReturnedNotify(row.id)) continue;
    try {
      const sent = await notifyReturned(row, null, { retry: true });   // 作業者名は改変しない (再送は通知の属性 — Codex R2)
      markReturnedNotify(row.id, sent, sent ? null : 'webhook未設定');
      if (sent) console.log(`[packing-drive-poller] 棚戻し完了の通知を再送: task=${row.id} ${row.sku} → ${row.returned_location}`);
    } catch (e) {
      markReturnedNotify(row.id, false, e.message);
    }
  }
}

/** 🖨再印刷通知の再送 (直近2日・未通知のみ・1周期3件まで — ④と同型)。 */
async function retryReprintNotify() {
  const db = getDB();
  let rows = [];
  try {
    rows = db.prepare(`
      SELECT * FROM pk_pack_reprints
      WHERE notified_at IS NULL AND created_at >= datetime('now', '-2 days')
      ORDER BY id LIMIT 3
    `).all();
  } catch { return; }   // v8未適用
  for (const row of rows) {
    try {
      const lines = db.prepare(`
        SELECT COALESCE(l.print_name, l.product_name) AS name, l.sku, l.qty
        FROM pk_pack_lines l JOIN pk_pack_slips s ON s.id = l.slip_id
        WHERE s.batch_id=? AND s.seq=? ORDER BY l.id
      `).all(row.batch_id, row.slip_seq);
      const sent = await notifyReprint({
        kind: row.kind, folderName: row.folder_name, slipSeq: row.slip_seq, neSlipNo: row.ne_slip_no,
        siteOrderNo: row.site_order_no, recipientName: row.recipient_name,
        worker: `${row.requested_by} (再送)`, lines,
        pdfUrl: row.pdf_token ? `https://picking.bfaith-wh.uk/apps/packing/reprints/${row.pdf_token}.pdf` : null,
        pdfError: row.pdf_error,
      });
      if (sent) {
        db.prepare('UPDATE pk_pack_reprints SET notified_at=?, notify_error=NULL WHERE id=?').run(utcNow(), row.id);
        console.log(`[packing-drive-poller] 再印刷通知を再送: ${row.ne_slip_no}`);
      }
    } catch (e) {
      db.prepare('UPDATE pk_pack_reprints SET notify_error=? WHERE id=?').run(String(e.message).slice(0, 200), row.id);
    }
  }
}

/**
 * 🖨 印刷キューの滞留・結果不明を GChat に知らせる (要件§6.3 dead-man)。
 * **出てこないときに誰も気づかない状態を作らない**のがこの機能の存在理由なので、
 * ここが落ちてもポーラー全体は止めない (fail-soft) が、失敗はログに残す。
 */
async function sweepPrintJobsStep() {
  try {
    sweepPrintJobs();   // 進まなくなったジョブを安全な状態へ (manual / unknown)
  } catch (e) {
    console.warn(`[packing-drive-poller] 印刷キューの整理に失敗: ${e.message}`);
  }
  // 通知は**送れたときだけ**「通知済み」にする。送信前に印を付けると、webhook が落ちていた
  // 分が永久に鳴らなくなる。1件の失敗で後続を止めないよう、ジョブごとに握る
  for (const job of pendingAlerts()) {
    try {
      const text = alertTextFor(job);
      if (!text) continue;
      if (await postReprintText(text)) markAlerted(job.id, job.state);
    } catch (e) {
      console.warn(`[packing-drive-poller] 印刷結果の通知に失敗 (${job.ne_slip_no}): ${e.message}`);
    }
  }
}

export function getPollerStatus() {
  return { ..._status, intervalSec: POLL_INTERVAL_MS / 1000 };
}

export function startPackingDrivePoller() {
  if (_status.running) return;
  _status.running = true;
  const tick = async () => {
    if (_polling) return;
    _polling = true;
    try {
      try {
        _status.lastStats = await pollOnce();
        _status.lastAt = utcNow();
        _status.lastError = null;
        await retryShipChangeNotify();
        await retryReprintNotify();
        await retryStockoutNotify();
        await retryReturnedNotify();
        // 資材変更の通知 outbox (undo 猶予後に送信・at-least-once — 要件『梱包資材表示』§5.3)。
        // webhook 未設定時は claim しない (管理画面に構成エラー表示)
        await materialNotifyStep(materialWebhookConfigured() ? postMaterialText : null);
        purgeOldViews();   // 表示観測ログの180日 purge (要件§7)
        cleanupReprintPdfs();
        await sweepPrintJobsStep();
        // ピッキング「後で取りに行く」依頼を取込済みバッチへ展開 (欠品フローv2 PR2・fail-soft)。
        // 欠品はピッキング中 = 梱包CSVの取込前が普通なので、取込後のここで追いつくのが主経路
        try { (await import('../picking/service.js')).bindPendingLaterRequests(); }
        catch { /* picking無効環境では何もしない */ }
        await pingJobsMonitor();
        await pingPrintAgentAlive();
      } catch (e) {
        _status.lastError = String(e.message).slice(0, 300);
        console.warn(`[packing-drive-poller] ポーリング失敗: ${e.message}`);
      }
      // ⚠ 取りこぼしの見張りは**取込が失敗した周期でも必ず走らせる**。
      // Drive 障害で pollOnce が落ちるときこそ「梱包に来ていない」を鳴らす必要があり、
      // 上の try に含めると監視が要る場面でだけ監視が止まる (Codexレビュー High)
      try {
        await missWatchStep(missWebhookConfigured() ? postMissText : null);
      } catch (e) {
        console.warn(`[packing-drive-poller] 取りこぼしの見張りに失敗: ${e.message}`);
      }
    } finally {
      _polling = false;
    }
  };
  setTimeout(tick, 20_000);            // 起動直後に1回 (picking ポーラーの15秒と少しずらす)
  setInterval(tick, POLL_INTERVAL_MS).unref();
  console.log(`[packing-drive-poller] 起動 (間隔 ${POLL_INTERVAL_MS / 1000}秒)`);
}

// getShippingFolders は router の一覧表示でも使うため再エクスポート
export { getShippingFolders };
