/**
 * Drive「出荷_no」との同期 — 共有ヘルパー + CSV自動ポーリング (Phase 1)。
 *
 * 設計 (Codex相談 2026-08-13): 取込経路はRPAのPOST (即時) + 本ポーラー (自己回復) の二重化。
 *   - アプリ再起動の数秒にPOSTが当たって欠けても、数分以内に自動で拾い直す
 *   - 冪等は二層: 台帳 pk_drive_imports (drive_file_id+modified_time=版) と
 *     importBatch の tb_no+csv_sha256 (POSTとポーラーの競合はここで再送成功に落ちる)
 *   - 書き込み途中のファイルを掴まない: modified_time から60秒待ってから取り込む
 *   - 直近3日分を対象 (停止からの回復・遅延配置に効く)。前日ファイルはisStaleInstructDateが警告
 *   - 引当分類は同フォルダの「引当パターン_○○.txt」のファイル名から取る (RPAが書く正確な値)
 *   - 失敗は台帳に残し管理画面で見える (黙殺しない)。5回失敗でそれ以上再試行しない
 *
 * Phase 2 (未実装): CSVが無いフォルダのピッキングリストPDF (TMP1) フォールバック (手動引当対応)。
 * ポーラーは standalone (ミニPC) だけが起動する。portal (Render) は手動ボタンのみ。
 * 定期実行の台帳: config/jobs-registry.mjs の picking-drive-poller を参照。
 *
 * ⚠ 多重プロセス排他は持たない (Codex指摘を設計判断として受容): PickingServer は
 * winsw のシングルトンサービスで、再起動は stop→start (旧新プロセスが重ならない)。
 * 万一並行しても importBatch 側の tb_no+ハッシュ冪等で二重バッチにはならない
 * (台帳の attempts が実際より増えるだけ)。
 */
import {
  listDriveSubfolders, listDriveFilesAcross, downloadDriveFileById,
} from '../../lib/drive-csv.js';
import { getDB, utcNow } from './db.js';
import { parseCs03002, importBatch, PkError } from './service.js';
import { queueEnsureImages } from './images.js';

// 出荷_no フォルダ (SA閲覧者共有済み)
export const DRIVE_FOLDER_ID = process.env.PICKING_DRIVE_FOLDER_ID || '110ONn2xHzfEG5HPt1DRy4P64zv2hpGjh';

// サブフォルダ一覧 (出荷_01〜45) は増減しないので60秒キャッシュ
let _subfoldersCache = { at: 0, list: null };
export async function getShippingFolders() {
  if (_subfoldersCache.list && Date.now() - _subfoldersCache.at < 60_000) return _subfoldersCache.list;
  const subs = await listDriveSubfolders({ folderId: DRIVE_FOLDER_ID });
  const list = [{ folder_id: DRIVE_FOLDER_ID, name: '(出荷_no直下)' }, ...subs];
  _subfoldersCache = { at: Date.now(), list };
  return list;
}

/** lib/drive-csv.js のエラーを業務エラーへ変換 (VALIDATION=入力起因400 / それ以外=Drive側502)。 */
export async function driveCall(fn) {
  try {
    return await fn();
  } catch (e) {
    if (e instanceof PkError) throw e;
    throw new PkError(e.code === 'VALIDATION' ? 400 : 502, 'drive_error', e.message);
  }
}

/** 「引当パターン_○○○.txt」のファイル名からパターン名を取り出す。 */
export function patternFromTxtName(filename) {
  const m = String(filename || '').match(/^引当パターン_(.+)\.txt$/);
  return m ? m[1] : null;
}

/**
 * ポーリング対象のCSV候補を選ぶ (純関数)。
 * @param files listDriveFilesAcross の結果
 * @param nowMs 現在時刻
 * @returns 安定した (更新から60秒以上・3日以内の) ピッキングリストCSV
 */
export function pickPollCandidates(files, nowMs = Date.now()) {
  const STABLE_MS = 60_000;
  const WINDOW_MS = 3 * 24 * 3600 * 1000;
  return files.filter((f) => {
    if (!/\.csv$/i.test(f.filename)) return false;
    if (!f.filename.includes('ピッキングリスト')) return false;
    const t = Date.parse(f.modified_time || '');
    if (!Number.isFinite(t)) return false;
    return nowMs - t >= STABLE_MS && nowMs - t <= WINDOW_MS;
  });
}

const MAX_ATTEMPTS = 5;

/**
 * 1回分のポーリング。取込は個別fail-soft (1件の失敗が他を止めない)。
 * @returns {{checked, imported, replayed, failed, skipped}}
 */
export async function pollOnce(deps = {}) {
  const db = getDB();
  const now = utcNow();
  const stats = { checked: 0, imported: 0, replayed: 0, failed: 0, skipped: 0 };

  const folders = await (deps.getFolders || getShippingFolders)();
  const list = deps.listFiles || listDriveFilesAcross;
  // CSVとパターンtxtをまとめて把握 (フォルダ→パターン名の対応を先に作る)
  const [csvFiles, txtFiles] = await Promise.all([
    list({ folders, nameContains: 'ピッキングリスト' }),
    list({ folders, nameContains: '引当パターン_' }),
  ]);
  // フォルダごとに「最新の」txtを採用する。一覧は更新日時降順なので最初の1件を保持
  // (Map#setで上書きすると最古が勝ってしまい、POST済みの正しい分類と食い違って
  // ready バッチを誤上書きし得る — Codex high)
  const patternByFolder = new Map();
  for (const t of txtFiles) {
    const p = patternFromTxtName(t.filename);
    if (p && t.parent_name && !patternByFolder.has(t.parent_name)) {
      patternByFolder.set(t.parent_name, p);
    }
  }

  const candidates = pickPollCandidates(csvFiles);
  const getLedger = db.prepare(
    'SELECT status, attempts FROM pk_drive_imports WHERE drive_file_id = ? AND modified_time = ?'
  );
  const upsertLedger = db.prepare(`
    INSERT INTO pk_drive_imports
      (drive_file_id, modified_time, filename, folder_name, source_type, status, error, batch_id, attempts, first_seen_at, processed_at)
    VALUES (@drive_file_id, @modified_time, @filename, @folder_name, 'csv', @status, @error, @batch_id, 1, @now, @now)
    ON CONFLICT(drive_file_id, modified_time) DO UPDATE SET
      status = excluded.status, error = excluded.error, batch_id = excluded.batch_id,
      attempts = pk_drive_imports.attempts + 1, processed_at = excluded.processed_at
  `);

  for (const f of candidates) {
    stats.checked++;
    const prev = getLedger.get(f.file_id, f.modified_time);
    if (prev && (prev.status === 'imported' || prev.status === 'skipped')) continue;
    if (prev && prev.status === 'failed' && prev.attempts >= MAX_ATTEMPTS) continue;

    try {
      const dl = await (deps.download || ((a) => downloadDriveFileById(a)))({
        fileId: f.file_id, folderIds: folders.map((x) => x.folder_id),
      });
      // 選定後にファイルが更新されていたら、この周期では取り込まない
      // (安定待ちのすり抜け防止 + 台帳に「旧版の時刻で新版の内容」を記録しない — Codex high)
      if (dl.modified_time && dl.modified_time !== f.modified_time) {
        continue;   // 次周期に新しい modified_time の版として改めて評価される
      }
      const preview = parseCs03002(dl.buffer);
      const folderName = /^出荷_\d+$/.test(f.parent_name || '') ? f.parent_name : null;
      const pattern = (folderName && patternByFolder.get(folderName))
        || preview.suggestions[0] || '(Drive自動取込)';
      const result = importBatch(preview, {
        hikiateClass: pattern, folderName, overwrite: true,
      }, 'drive-poller');
      // 台帳の成功記録を画像キュー投入より先に行う (後段の例外で「取込済みなのにfailed」に
      // ならないように — Codex medium)
      upsertLedger.run({
        drive_file_id: f.file_id, modified_time: f.modified_time, filename: f.filename,
        folder_name: folderName, status: 'imported', error: null, batch_id: result.batchId, now,
      });
      queueEnsureImages(preview.lines.map((l) => l.sku), preview.tbNo.split(',')[0]);
      if (result.replayed) stats.replayed++;
      else {
        stats.imported++;
        console.log(`[picking-drive-poller] 取込: ${folderName || f.filename} (${pattern})${result.replaced ? ' 上書き' : ''}${preview.qtyWarningCount > 0 ? ` ⚠数量警告${preview.qtyWarningCount}件` : ''}`);
      }
    } catch (e) {
      // already_started (作業開始後の内容変更) は上書きしない仕様どおりの skip 扱い
      if (e instanceof PkError && e.code === 'already_started') {
        upsertLedger.run({
          drive_file_id: f.file_id, modified_time: f.modified_time, filename: f.filename,
          folder_name: f.parent_name || null, status: 'skipped', error: e.message, batch_id: null, now,
        });
        stats.skipped++;
        continue;
      }
      stats.failed++;
      upsertLedger.run({
        drive_file_id: f.file_id, modified_time: f.modified_time, filename: f.filename,
        folder_name: f.parent_name || null, status: 'failed', error: String(e.message).slice(0, 300), batch_id: null, now,
      });
      console.warn(`[picking-drive-poller] 取込失敗 (${f.filename}): ${e.message}`);
    }
  }
  return stats;
}

// ─── 常駐ポーリング (standalone のみが起動する) ───

const POLL_INTERVAL_MS = Number(process.env.PICKING_POLL_INTERVAL_SEC || 120) * 1000;
let _status = { running: false, lastAt: null, lastError: null, lastStats: null };
let _polling = false;

// ─── jobs-monitor への生存 ping (dead-man方式・台帳 id=picking-drive-poller) ───
// ポーリング成功時のみ打つ (Drive障害・取込失敗が続くと ping が止まり、監視側の
// max_age_hours 超過で「止まっている」と出る)。1時間に1回へ間引き。
const PING_THROTTLE_MS = 3600_000;
let _lastPingAt = 0;
let _warnedNoToken = false;
async function pingJobsMonitor(fetchFn = fetch) {
  const token = process.env.JOBS_MONITOR_TOKEN;
  if (!token) {
    if (!_warnedNoToken) {
      _warnedNoToken = true;
      console.warn('[picking-drive-poller] JOBS_MONITOR_TOKEN 未設定 → 生存pingなし (台帳の監視が効かない)');
    }
    return;
  }
  if (Date.now() - _lastPingAt < PING_THROTTLE_MS) return;
  try {
    const base = (process.env.JOBS_MONITOR_URL || 'https://bfaith-portal.onrender.com').replace(/\/+$/, '');
    const res = await fetchFn(`${base}/apps/jobs-monitor/ping/picking-drive-poller?status=ok`, {
      method: 'POST', headers: { authorization: `Bearer ${token}` },
    });
    if (res.ok) _lastPingAt = Date.now();
    else console.warn(`[picking-drive-poller] 生存ping失敗: HTTP ${res.status}`);
  } catch (e) {
    // ping はあくまで監視用 — 失敗してもポーリング本体は止めない
    console.warn(`[picking-drive-poller] 生存ping失敗: ${e.message}`);
  }
}

export function getPollerStatus() {
  return { ..._status, intervalSec: POLL_INTERVAL_MS / 1000 };
}

export function startDrivePoller() {
  if (_status.running) return;
  _status.running = true;
  const tick = async () => {
    if (_polling) return;   // 前回が長引いていたら重ねない
    _polling = true;
    try {
      _status.lastStats = await pollOnce();
      _status.lastAt = utcNow();
      _status.lastError = null;
      await pingJobsMonitor();
    } catch (e) {
      // Drive全体の失敗 (ネットワーク等)。次周期に自然リトライ
      _status.lastError = String(e.message).slice(0, 300);
      console.warn(`[picking-drive-poller] ポーリング失敗: ${e.message}`);
    } finally {
      _polling = false;
    }
  };
  setTimeout(tick, 15_000);            // 起動直後に1回 (サービス再起動後の回復を早く)
  setInterval(tick, POLL_INTERVAL_MS).unref();
  console.log(`[picking-drive-poller] 起動 (間隔 ${POLL_INTERVAL_MS / 1000}秒)`);
}
