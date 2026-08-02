/**
 * shipping-work service — 状態機械・リース取得・セッション管理 (実装計画§4)
 *
 * 遷移はすべてここの関数経由で行い、router は入出力変換のみ。
 *   - リース = 条件付き UPDATE (WHERE status='ready')。0行更新なら他者取得済み
 *   - 冪等   = 開始系はクライアント発行の op_id (sw_sessions.op_id UNIQUE)、
 *              完了/保留/再開/トラブルはセッション状態から already 判定 (自然冪等)
 *   - 計測   = sw_sessions は追記型。UPDATE は終了系カラム (ended_at/outcome/flags) のみ
 *
 * PR3 はピッキングのみ。PROCESS_FLOW に sorting/packing を足すのが PR4。
 */
import {
  getDB, utcNow, jstToday, getBatch, addStatusEvent, isValidMaster,
} from './db.js';

/** 業務エラー。router が status を HTTP ステータスに変換する。 */
export class SwError extends Error {
  constructor(status, message, code) {
    super(message);
    this.status = status;
    this.code = code || null;
  }
}

// 工程ごとの遷移定義 (from → active → to)。PR4 で sorting/packing を追加する。
// sorting は from:'picked' (アソートのみ)、packing は from:'picked'|'sorted' (分類による) の予定。
const PROCESS_FLOW = {
  picking: {
    from: 'ready',
    active: 'picking',
    to: 'picked',
    pauseReasonKind: 'pause_reason_pick',
  },
};

// 異常候補の閾値 (要件§7.4)。too_long は sw_settings で管理者が上書きできる (PR5 で設定画面)。
const TOO_SHORT_SEC = 60;
const EARLY_PAUSE_SEC = 60;
const DEFAULT_TOO_LONG_MIN = 120;
const SETTING_TOO_LONG_KEY = 'anomaly_too_long_minutes';

/** 自由記述の上限。現場のメモ用途にこれ以上は不要で、DB肥大とログ汚染を防ぐ。 */
const NOTE_MAX = 500;

function getFlow(process) {
  const flow = PROCESS_FLOW[process];
  if (!flow) throw new SwError(400, '不明な工程です');
  return flow;
}

/** 自由記述の正規化 (trim + 長さ検証)。空なら null。 */
function normalizeNote(note) {
  const s = String(note ?? '').trim();
  if (!s) return null;
  if (s.length > NOTE_MAX) throw new SwError(400, `補足は${NOTE_MAX}文字以内で入力してください`);
  return s;
}

function getSettingInt(key, dflt) {
  const row = getDB().prepare('SELECT value FROM sw_settings WHERE key = ?').get(key);
  const n = row ? Number(row.value) : NaN;
  return Number.isFinite(n) && n > 0 ? n : dflt;
}

export function masterLabel(kind, code) {
  const row = getDB().prepare('SELECT label FROM sw_masters WHERE kind = ? AND code = ?').get(kind, code);
  return row ? row.label : code;
}

function parseFlags(json) {
  try {
    const a = JSON.parse(json || '[]');
    return Array.isArray(a) ? a : [];
  } catch { return []; }
}

/** 経過秒 (UTC 'YYYY-MM-DDTHH:MM:SSZ' 同士)。 */
function secondsBetween(fromUtc, toUtc) {
  return Math.max(0, (Date.parse(toUtc) - Date.parse(fromUtc)) / 1000);
}

/** セッションの取得 + 本人・工程チェック。 */
function getOwnSession(db, process, sessionId, worker) {
  const s = db.prepare('SELECT * FROM sw_sessions WHERE id = ?').get(sessionId);
  if (!s || s.process !== process) throw new SwError(404, 'セッションが見つかりません');
  if (s.worker !== worker) throw new SwError(403, '自分のセッションのみ操作できます');
  return s;
}

/**
 * セッションに紐付く未消化の印刷要求を閉じる。
 * PR3 はブラウザ表示モード (実装計画§5) のためジョブは記録のみ。セッション終了時に
 * requested を cancelled に落とし、PR6 のブリッジ導入時に古いジョブが印刷される事故を防ぐ。
 */
function cancelOpenPrintJobs(db, sessionId) {
  db.prepare("UPDATE sw_print_jobs SET status='cancelled' WHERE session_id = ? AND status = 'requested'")
    .run(sessionId);
}

/** 未解除の保留を今閉じる (防御用。通常は resume 経由で閉じる)。 */
function closeActivePause(db, sessionId, now) {
  db.prepare('UPDATE sw_pauses SET resumed_at = ? WHERE session_id = ? AND resumed_at IS NULL')
    .run(now, sessionId);
}

/**
 * 開始 (楽観開始・要件§7.1)。1トランザクションで
 * リース → セッション作成 → 印刷ジョブ登録 → status_events 追記。
 */
export function startProcess(process, batchId, worker, opId) {
  const flow = getFlow(process);
  if (!opId || typeof opId !== 'string' || opId.length < 8 || opId.length > 64) {
    throw new SwError(400, 'op_id が不正です');
  }
  if (!Number.isInteger(batchId) || batchId <= 0) throw new SwError(400, 'batch_id が不正です');
  const db = getDB();
  try {
    return db.transaction(() => {
      // 冪等: 同一 op_id の再送は前回作成したセッションを返す
      const existing = db.prepare('SELECT * FROM sw_sessions WHERE op_id = ?').get(opId);
      if (existing) {
        if (existing.worker !== worker) throw new SwError(409, '操作IDが競合しました。画面を更新してください');
        return { session: existing, already: true };
      }
      const batch = getBatch(batchId);
      if (!batch) throw new SwError(404, 'バッチが見つかりません');
      // 進行中の作業は1人1つ。保留中 (paused=1) は対象外 = 保留して別の作業へ移れる
      // (保留理由「他作業への応援」の運用。DB制約 idx_sw_sessions_worker_open と同じ条件)
      const mine = db.prepare("SELECT id FROM sw_sessions WHERE worker = ? AND outcome = 'open' AND paused = 0").get(worker);
      if (mine) throw new SwError(409, '作業中のバッチがあります。完了・保留・中止のいずれかをしてから次を開始してください', 'busy_worker');
      if (batch.status !== flow.from) {
        throw new SwError(409, '他の人が先に開始したか、開始できない状態です。画面を更新してください', 'lease_lost');
      }
      const now = utcNow();
      // リース: 条件付き UPDATE。0行なら同時操作に負けた
      const res = db.prepare('UPDATE sw_batches SET status = ?, updated_at = ? WHERE id = ? AND status = ?')
        .run(flow.active, now, batchId, flow.from);
      if (res.changes === 0) throw new SwError(409, '他の人が先に開始しました。画面を更新してください', 'lease_lost');
      const info = db.prepare(`
        INSERT INTO sw_sessions (batch_id, process, worker, requested_at, op_id)
        VALUES (?, ?, ?, ?, ?)
      `).run(batchId, process, worker, now, opId);
      const sessionId = Number(info.lastInsertRowid);
      if (batch.pdf_path) {
        db.prepare(`
          INSERT INTO sw_print_jobs (batch_id, session_id, doc_type, pdf_path, requested_by, requested_at)
          VALUES (?, ?, 'picking_list', ?, ?, ?)
        `).run(batchId, sessionId, batch.pdf_path, worker, now);
      }
      addStatusEvent(batchId, flow.from, flow.active, worker, 'button', null);
      return { session: db.prepare('SELECT * FROM sw_sessions WHERE id = ?').get(sessionId), already: false };
    })();
  } catch (e) {
    // 並行して同一 op_id が INSERT された場合 (UNIQUE) も冪等応答にする
    if (e.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      const s = db.prepare('SELECT * FROM sw_sessions WHERE op_id = ?').get(opId);
      if (s && s.worker === worker) return { session: s, already: true };
      throw new SwError(409, '同時操作が競合しました。画面を更新してください');
    }
    throw e;
  }
}

/**
 * 完了 (要件§7.2)。終了時刻を記録し、異常候補フラグを同期判定して flow.to へ遷移。
 */
export function completeProcess(process, sessionId, worker) {
  const flow = getFlow(process);
  const db = getDB();
  return db.transaction(() => {
    const s = getOwnSession(db, process, sessionId, worker);
    if (s.outcome === 'completed') return { already: true, flags: parseFlags(s.flags_json) };
    if (s.outcome !== 'open') throw new SwError(409, 'このセッションは取り消されています。画面を更新してください');
    const batch = getBatch(s.batch_id);
    if (batch.status === 'hold') throw new SwError(409, '保留中です。再開してから完了してください');
    if (batch.status !== flow.active) throw new SwError(409, 'バッチの状態が変わっています。画面を更新してください');
    const openPause = db.prepare('SELECT 1 FROM sw_pauses WHERE session_id = ? AND resumed_at IS NULL').get(sessionId);
    if (openPause) throw new SwError(409, '未解除の保留があります。再開してから完了してください');

    const now = utcNow();
    const pauses = db.prepare('SELECT paused_at, resumed_at FROM sw_pauses WHERE session_id = ? AND resumed_at IS NOT NULL').all(sessionId);
    const pauseSec = pauses.reduce((a, p) => a + secondsBetween(p.paused_at, p.resumed_at), 0);
    const workSec = Math.max(0, secondsBetween(s.requested_at, now) - pauseSec);

    // 異常候補フラグ (要件§7.4)。自動除外はしない。PR5 の管理者一覧で人が判定する
    const flags = parseFlags(s.flags_json);
    if (workSec < TOO_SHORT_SEC && !flags.includes('too_short')) flags.push('too_short');
    const tooLongMin = getSettingInt(SETTING_TOO_LONG_KEY, DEFAULT_TOO_LONG_MIN);
    if (workSec > tooLongMin * 60 && !flags.includes('too_long')) flags.push('too_long');

    const res = db.prepare(`
      UPDATE sw_sessions SET ended_at = ?, outcome = 'completed', flags_json = ?
      WHERE id = ? AND outcome = 'open'
    `).run(now, JSON.stringify(flags), sessionId);
    if (res.changes === 0) throw new SwError(409, '同時操作が競合しました。画面を更新してください');
    cancelOpenPrintJobs(db, sessionId);
    db.prepare('UPDATE sw_batches SET status = ?, updated_at = ? WHERE id = ?').run(flow.to, now, s.batch_id);
    addStatusEvent(s.batch_id, flow.active, flow.to, worker, 'button', null);
    return { already: false, flags, workSec: Math.round(workSec), pauseSec: Math.round(pauseSec) };
  })();
}

/**
 * 保留 (要件§7.5)。理由必須・「その他」は自由記述必須。
 * バッチを hold へ移し (復帰先を hold_from_status に保存)、保留区間を記録する。
 */
export function pauseProcess(process, sessionId, worker, reason, note) {
  const flow = getFlow(process);
  const noteTrim = normalizeNote(note);
  if (!isValidMaster(flow.pauseReasonKind, reason)) throw new SwError(400, '保留理由が不正です');
  const label = masterLabel(flow.pauseReasonKind, reason);
  if (label === 'その他' && !noteTrim) throw new SwError(400, '「その他」を選んだ場合は内容の記入が必須です');
  const db = getDB();
  return db.transaction(() => {
    const s = getOwnSession(db, process, sessionId, worker);
    if (s.outcome !== 'open') throw new SwError(409, 'このセッションは終了しています。画面を更新してください');
    const batch = getBatch(s.batch_id);
    const activePause = db.prepare('SELECT id FROM sw_pauses WHERE session_id = ? AND resumed_at IS NULL').get(sessionId);
    if (batch.status === 'hold' && activePause) return { already: true };  // 二重送信
    if (batch.status !== flow.active) throw new SwError(409, 'バッチの状態が変わっています。画面を更新してください');
    if (activePause) throw new SwError(409, '未解除の保留があります');

    const now = utcNow();
    db.prepare('INSERT INTO sw_pauses (session_id, reason, reason_note, paused_at) VALUES (?, ?, ?, ?)')
      .run(sessionId, reason, noteTrim, now);
    // 開始直後の保留は異常候補 (要件§7.4「印刷直後の保留」)
    const flags = parseFlags(s.flags_json);
    if (secondsBetween(s.requested_at, now) < EARLY_PAUSE_SEC && !flags.includes('early_pause')) {
      flags.push('early_pause');
    }
    // paused=1 で「進行中は1人1つ」の対象から外す (保留したら別の作業へ移れる)
    db.prepare('UPDATE sw_sessions SET paused = 1, flags_json = ? WHERE id = ?')
      .run(JSON.stringify(flags), sessionId);
    db.prepare("UPDATE sw_batches SET status = 'hold', hold_from_status = ?, updated_at = ? WHERE id = ?")
      .run(flow.active, now, s.batch_id);
    addStatusEvent(s.batch_id, flow.active, 'hold', worker, 'button', label + (noteTrim ? `: ${noteTrim}` : ''));
    return { already: false };
  })();
}

/** 保留解除。バッチを hold_from_status へ戻し、保留区間を閉じる。 */
export function resumeProcess(process, sessionId, worker) {
  const flow = getFlow(process);
  const db = getDB();
  return db.transaction(() => {
    const s = getOwnSession(db, process, sessionId, worker);
    if (s.outcome !== 'open') throw new SwError(409, 'このセッションは終了しています。画面を更新してください');
    const batch = getBatch(s.batch_id);
    const activePause = db.prepare('SELECT id FROM sw_pauses WHERE session_id = ? AND resumed_at IS NULL').get(sessionId);
    if (batch.status === flow.active && !activePause) return { already: true };  // 二重送信
    if (batch.status !== 'hold') throw new SwError(409, 'バッチは保留中ではありません。画面を更新してください');
    // 保留中に別の作業を始めていたら、そちらを先に片付けてもらう
    // (DB制約 idx_sw_sessions_worker_open に当たる前に分かりやすいエラーにする)
    const other = db.prepare(
      "SELECT id FROM sw_sessions WHERE worker = ? AND outcome = 'open' AND paused = 0 AND id != ?"
    ).get(worker, sessionId);
    if (other) throw new SwError(409, '別の作業が進行中です。そちらを完了か保留にしてから再開してください', 'busy_worker');

    const now = utcNow();
    closeActivePause(db, sessionId, now);
    db.prepare('UPDATE sw_sessions SET paused = 0 WHERE id = ?').run(sessionId);
    db.prepare('UPDATE sw_batches SET status = ?, hold_from_status = NULL, updated_at = ? WHERE id = ?')
      .run(batch.hold_from_status || flow.active, now, s.batch_id);
    addStatusEvent(s.batch_id, 'hold', batch.hold_from_status || flow.active, worker, 'button', null);
    return { already: false };
  })();
}

/**
 * 印刷トラブル (要件§7.3)。旧セッションを voided で残し (削除・上書きなし)、
 *   action='reprint' → 同一作業者で新セッション+新印刷ジョブ (計測やり直し・reprint フラグ)
 *   action='abort'   → バッチを ready へ戻す
 */
export function troubleProcess(process, sessionId, worker, reason, note, action, newOpId) {
  const flow = getFlow(process);
  const noteTrim = normalizeNote(note);
  if (!isValidMaster('print_trouble_reason', reason)) throw new SwError(400, 'トラブル理由が不正です');
  const label = masterLabel('print_trouble_reason', reason);
  if (label === 'その他' && !noteTrim) throw new SwError(400, '「その他」を選んだ場合は内容の記入が必須です');
  if (action !== 'reprint' && action !== 'abort') throw new SwError(400, 'action が不正です');
  if (action === 'reprint' && (!newOpId || typeof newOpId !== 'string' || newOpId.length < 8 || newOpId.length > 64)) {
    throw new SwError(400, 'op_id_new が不正です');
  }
  const db = getDB();
  try {
    return db.transaction(() => {
      const s = getOwnSession(db, process, sessionId, worker);
      if (s.outcome === 'voided') {
        // 冪等: 再送。reprint は新セッションを返し、abort はバッチが ready なら成功扱い
        if (action === 'reprint' && newOpId) {
          const ns = db.prepare('SELECT * FROM sw_sessions WHERE op_id = ?').get(newOpId);
          if (ns && ns.worker === worker) return { already: true, session: ns };
        }
        if (action === 'abort' && getBatch(s.batch_id).status === 'ready') return { already: true, aborted: true };
        throw new SwError(409, 'このセッションは既に取り消されています。画面を更新してください');
      }
      if (s.outcome !== 'open') throw new SwError(409, 'このセッションは終了しています。画面を更新してください');
      const batch = getBatch(s.batch_id);
      if (batch.status !== flow.active) {
        throw new SwError(409, '保留中はトラブル処理できません。先に再開してください');
      }

      const now = utcNow();
      closeActivePause(db, sessionId, now);  // 防御 (通常 active 中に未解除保留はない)
      const voidReason = label + (noteTrim ? `: ${noteTrim}` : '');
      const res = db.prepare(`
        UPDATE sw_sessions SET outcome = 'voided', ended_at = ?, void_reason = ?
        WHERE id = ? AND outcome = 'open'
      `).run(now, voidReason, sessionId);
      if (res.changes === 0) throw new SwError(409, '同時操作が競合しました。画面を更新してください');
      cancelOpenPrintJobs(db, sessionId);

      if (action === 'reprint') {
        // 計測やり直し。新セッションは reprint フラグ付き (要件§7.4 短時間再印刷の把握)
        const info = db.prepare(`
          INSERT INTO sw_sessions (batch_id, process, worker, requested_at, op_id, flags_json)
          VALUES (?, ?, ?, ?, ?, '["reprint"]')
        `).run(s.batch_id, process, worker, now, newOpId);
        const newSessionId = Number(info.lastInsertRowid);
        if (batch.pdf_path) {
          db.prepare(`
            INSERT INTO sw_print_jobs (batch_id, session_id, doc_type, pdf_path, requested_by, requested_at)
            VALUES (?, ?, 'picking_list', ?, ?, ?)
          `).run(s.batch_id, newSessionId, batch.pdf_path, worker, now);
        }
        addStatusEvent(s.batch_id, flow.active, flow.active, worker, 'button', `印刷トラブル(${label})→再印刷して開始し直し`);
        return { already: false, session: db.prepare('SELECT * FROM sw_sessions WHERE id = ?').get(newSessionId) };
      }
      // abort: バッチを本日のやることへ戻す
      db.prepare("UPDATE sw_batches SET status = 'ready', hold_from_status = NULL, updated_at = ? WHERE id = ?")
        .run(now, s.batch_id);
      addStatusEvent(s.batch_id, flow.active, 'ready', worker, 'button', `印刷トラブル(${label})→中止`);
      return { already: false, aborted: true };
    })();
  } catch (e) {
    if (e.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      const ns = db.prepare('SELECT * FROM sw_sessions WHERE op_id = ?').get(newOpId);
      if (ns && ns.worker === worker) return { already: true, session: ns };
      throw new SwError(409, '同時操作が競合しました。画面を更新してください');
    }
    throw e;
  }
}

/**
 * 「完了して次を開始」の次バッチ取得 (要件§7.2)。
 * 開始可能な先頭バッチ (持ち越し優先 → 出荷No順) を最大3回試行 (競合時は次候補へ)。
 * 開始できるものが無ければ null。
 */
export function startNextReady(process, worker, opId) {
  const flow = getFlow(process);
  for (let i = 0; i < 3; i++) {
    const candidates = listStartableBatches(process, jstToday());
    if (candidates.length === 0) return null;
    try {
      const r = startProcess(process, candidates[0].id, worker, opId);
      return { batch_id: candidates[0].id, shipping_no_label: candidates[0].shipping_no_label, session_id: r.session.id };
    } catch (e) {
      if (e instanceof SwError && e.code === 'lease_lost') continue;  // 他者が取った → 次候補
      throw e;
    }
  }
  return null;
}

/**
 * 工程を開始できるバッチ一覧 (本日 + 過去の持ち越し。未来日は含めない)。
 * 並び: 作業日 → 出荷Noマスタの sort → id。
 */
export function listStartableBatches(process, workDate) {
  const flow = getFlow(process);
  return getDB().prepare(`
    SELECT b.*,
      (SELECT label FROM sw_masters m WHERE m.kind='shipping_no' AND m.code=b.shipping_no) AS shipping_no_label,
      (SELECT label FROM sw_masters m WHERE m.kind='bunrui' AND m.code=b.bunrui) AS bunrui_label,
      (SELECT label FROM sw_masters m WHERE m.kind='packing_method' AND m.code=b.packing_method) AS packing_method_label
    FROM sw_batches b
    LEFT JOIN sw_masters sm ON sm.kind='shipping_no' AND sm.code=b.shipping_no
    WHERE b.status = ? AND b.work_date <= ?
    ORDER BY b.work_date, COALESCE(sm.sort, 999), b.id
  `).all(flow.from, workDate);
}

/** バッチ1件をラベル付きで取得 (画面表示用)。 */
function getBatchWithLabels(db, batchId) {
  return db.prepare(`
    SELECT b.*,
      (SELECT label FROM sw_masters m WHERE m.kind='shipping_no' AND m.code=b.shipping_no) AS shipping_no_label,
      (SELECT label FROM sw_masters m WHERE m.kind='bunrui' AND m.code=b.bunrui) AS bunrui_label,
      (SELECT label FROM sw_masters m WHERE m.kind='packing_method' AND m.code=b.packing_method) AS packing_method_label
    FROM sw_batches b WHERE b.id = ?
  `).get(batchId);
}

/** そのセッションの解除済み保留時間の合計 (秒)。 */
function closedPauseSec(db, sessionId) {
  const closed = db.prepare(
    'SELECT paused_at, resumed_at FROM sw_pauses WHERE session_id = ? AND resumed_at IS NOT NULL'
  ).all(sessionId);
  return Math.round(closed.reduce((a, p) => a + secondsBetween(p.paused_at, p.resumed_at), 0));
}

/**
 * 作業者画面の状態。
 *   session — 進行中 (paused=0) のセッション。なければ null で ready 一覧を出す
 *   paused  — 自分が保留中のセッション一覧 (「他作業への応援」で複数持ちうる)
 *   stats   — 本日の完了実績 (バッチ数・伝票数)
 */
export function getWorkerState(process, worker) {
  const flow = getFlow(process);
  const db = getDB();
  const today = jstToday();
  // JST 今日 00:00 を UTC に変換して本日実績の下限にする
  const jstStartUtc = new Date(`${today}T00:00:00+09:00`).toISOString().slice(0, 19) + 'Z';
  const stats = db.prepare(`
    SELECT COUNT(*) AS batches, COALESCE(SUM(b.slip_count), 0) AS slips
    FROM sw_sessions s JOIN sw_batches b ON b.id = s.batch_id
    WHERE s.worker = ? AND s.process = ? AND s.outcome = 'completed' AND s.ended_at >= ?
  `).get(worker, process, jstStartUtc);

  // 保留中の自分の作業 (再開ボタンを常に出すため、進行中の有無に関わらず返す)
  const paused = db.prepare(
    "SELECT * FROM sw_sessions WHERE worker = ? AND process = ? AND outcome = 'open' AND paused = 1 ORDER BY id"
  ).all(worker, process).map((ps) => {
    const pauseRow = db.prepare('SELECT * FROM sw_pauses WHERE session_id = ? AND resumed_at IS NULL').get(ps.id);
    return {
      session: ps,
      batch: getBatchWithLabels(db, ps.batch_id),
      reasonLabel: pauseRow ? masterLabel(flow.pauseReasonKind, pauseRow.reason) : '',
      reasonNote: pauseRow ? pauseRow.reason_note : null,
      pausedAt: pauseRow ? pauseRow.paused_at : null,
    };
  });

  const s = db.prepare(
    "SELECT * FROM sw_sessions WHERE worker = ? AND process = ? AND outcome = 'open' AND paused = 0"
  ).get(worker, process);
  if (!s) {
    return { session: null, batch: null, pauseSec: 0, paused, ready: listStartableBatches(process, today), stats };
  }
  return {
    session: s,
    batch: getBatchWithLabels(db, s.batch_id),
    pauseSec: closedPauseSec(db, s.id),
    paused,
    ready: [],
    stats,
  };
}
