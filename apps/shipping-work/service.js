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
import crypto from 'node:crypto';
import {
  getDB, utcNow, jstToday, getBatch, addStatusEvent, isValidMaster, batchHasDocument,
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

// ─── 冪等レイヤー (Codex PR3レビュー#1/#2/#4) ───
//
// 全作業APIはクライアント発行の op_id を必ず伴い、この関数を通す。
// 二重クリックは各操作の状態判定でも弾けるが、それだけでは
// 「通信タイムアウト後の遅延再送」で壊れる (保留A成功→再開→Aが遅延再送→新しい保留が入る)。
// op_id を「操作種別 × 対象 × 入力内容」に束縛して記録し、完全に一致する再送だけ
// 前回結果を返すことで、遅延再送も別対象への使い回しも安全に弾く。

function validateOpId(opId, field = 'op_id') {
  if (typeof opId !== 'string' || opId.length < 8 || opId.length > 64) {
    throw new SwError(400, `${field} が不正です`);
  }
  return opId;
}

function paramsHash(params) {
  if (params == null) return null;
  return crypto.createHash('sha256').update(JSON.stringify(params)).digest('hex').slice(0, 32);
}

/**
 * 冪等実行。同一 op_id の再送は保存済み結果を `already:true` で返し、
 * 同じ op_id が別の操作・対象・入力に使われていたら 409 で拒否する。
 * fn が throw した場合はトランザクションごと巻き戻るため記録も残らない (再試行できる)。
 */
function idempotent(db, { opId, worker, operation, targetKind, targetId, params }, fn) {
  const hash = paramsHash(params);
  const prev = db.prepare('SELECT * FROM sw_operations WHERE op_id = ?').get(opId);
  if (prev) {
    if (prev.worker !== worker || prev.operation !== operation
        || prev.target_kind !== targetKind || prev.target_id !== targetId
        || prev.params_hash !== hash) {
      throw new SwError(409, '操作IDが別の操作に使われています。画面を更新してやり直してください', 'op_conflict');
    }
    return { ...JSON.parse(prev.result_json), already: true };
  }
  const result = fn();
  db.prepare(`
    INSERT INTO sw_operations (op_id, worker, operation, target_kind, target_id, params_hash, result_json, at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(opId, worker, operation, targetKind, targetId, hash, JSON.stringify(result), utcNow());
  // noop = 別 op_id で同じボタンを2回押した等、状態的に既に済んでいたケース。
  // クライアントから見れば「もう終わっている」で同じなので already として返す
  return { ...result, already: !!result.noop };
}

/**
 * 冪等操作の実行 + UNIQUE 競合時の再解決。
 * 同一 op_id の同時2本が走ると片方が PRIMARY KEY 違反になるため、その場合は
 * 記録済みの結果を読み直して同じ照合を通す (勝った側と同じ応答になる)。
 */
function runIdempotent(spec, fn) {
  const db = getDB();
  try {
    return db.transaction(() => idempotent(db, spec, fn))();
  } catch (e) {
    if (e.code === 'SQLITE_CONSTRAINT_PRIMARYKEY' || e.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      const prev = db.prepare('SELECT * FROM sw_operations WHERE op_id = ?').get(spec.opId);
      if (prev) {
        if (prev.worker !== spec.worker || prev.operation !== spec.operation
            || prev.target_kind !== spec.targetKind || prev.target_id !== spec.targetId
            || prev.params_hash !== paramsHash(spec.params)) {
          throw new SwError(409, '操作IDが別の操作に使われています。画面を更新してやり直してください', 'op_conflict');
        }
        return { ...JSON.parse(prev.result_json), already: true };
      }
      throw new SwError(409, '同時操作が競合しました。画面を更新してください');
    }
    throw e;
  }
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
/**
 * 開始の中身 (冪等レイヤーを通さない内部関数)。
 * 呼び出し側 (startProcess / startNextReady) が runIdempotent の中で使う。
 * リース取得に失敗する場合は書き込みを一切行わずに throw するため、
 * startNextReady は lease_lost を捕まえて次候補へ進める。
 */
function startInternal(db, process, batchId, worker, opId) {
  const flow = PROCESS_FLOW[process];
  const batch = getBatch(batchId);
  if (!batch) throw new SwError(404, 'バッチが見つかりません');
  // 帳票が無いと「開始=印刷」が成立しない (実装計画§5: 事務がバッチ作成時に添付する方式)。
  // リースを取る前に止め、開始済みデータを残さない (Codex PR3レビュー#3)
  if (!batchHasDocument(batch)) {
    throw new SwError(409, '帳票PDFが未添付のため開始できません。管理者に添付を依頼してください', 'no_document');
  }
  // 進行中の作業は1人1つ。保留中 (paused=1) は対象外 = 保留して別の作業へ移れる
  // (保留理由「他作業への応援」の運用。DB制約 idx_sw_sessions_worker_open と同じ条件)
  const mine = db.prepare("SELECT id FROM sw_sessions WHERE worker = ? AND outcome = 'open' AND paused = 0").get(worker);
  if (mine) throw new SwError(409, '作業中のバッチがあります。完了・保留・中止のいずれかをしてから次を開始してください', 'busy_worker');
  const now = utcNow();
  // リース: 条件付き UPDATE。0行なら他の人が先に取った (ここまで書き込みなし)
  const res = db.prepare('UPDATE sw_batches SET status = ?, updated_at = ? WHERE id = ? AND status = ?')
    .run(flow.active, now, batchId, flow.from);
  if (res.changes === 0) {
    throw new SwError(409, '他の人が先に開始したか、開始できない状態です。画面を更新してください', 'lease_lost');
  }
  const info = db.prepare(`
    INSERT INTO sw_sessions (batch_id, process, worker, requested_at, op_id)
    VALUES (?, ?, ?, ?, ?)
  `).run(batchId, process, worker, now, opId);
  const sessionId = Number(info.lastInsertRowid);
  db.prepare(`
    INSERT INTO sw_print_jobs (batch_id, session_id, doc_type, pdf_path, requested_by, requested_at)
    VALUES (?, ?, 'picking_list', ?, ?, ?)
  `).run(batchId, sessionId, batch.pdf_path, worker, now);
  addStatusEvent(batchId, flow.from, flow.active, worker, 'button', null);
  return { session_id: sessionId, batch_id: batchId };
}

export function startProcess(process, batchId, worker, opId) {
  getFlow(process);
  validateOpId(opId);
  if (!Number.isInteger(batchId) || batchId <= 0) throw new SwError(400, 'batch_id が不正です');
  const db = getDB();
  return runIdempotent(
    { opId, worker, operation: `start:${process}`, targetKind: 'batch', targetId: batchId },
    () => startInternal(db, process, batchId, worker, opId)
  );
}

/**
 * 完了 (要件§7.2)。終了時刻を記録し、異常候補フラグを同期判定して flow.to へ遷移。
 */
export function completeProcess(process, sessionId, worker, opId) {
  const flow = getFlow(process);
  validateOpId(opId);
  const db = getDB();
  return runIdempotent(
    { opId, worker, operation: `complete:${process}`, targetKind: 'session', targetId: sessionId },
    () => {
      const s = getOwnSession(db, process, sessionId, worker);
      if (s.outcome === 'completed') {
        // op_id 違いでの二重クリック (同じ画面で2回押した等)。記録は変えずに現状を返す
        return { flags: parseFlags(s.flags_json), workSec: null, pauseSec: null, batch_id: s.batch_id, noop: true };
      }
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

      // active_sec = 判定に使った実作業秒数そのもの。集計と異常候補の根拠を後から検証できる
      const res = db.prepare(`
        UPDATE sw_sessions SET ended_at = ?, outcome = 'completed', flags_json = ?, active_sec = ?
        WHERE id = ? AND outcome = 'open'
      `).run(now, JSON.stringify(flags), Math.round(workSec), sessionId);
      if (res.changes === 0) throw new SwError(409, '同時操作が競合しました。画面を更新してください');
      cancelOpenPrintJobs(db, sessionId);
      db.prepare('UPDATE sw_batches SET status = ?, updated_at = ? WHERE id = ?').run(flow.to, now, s.batch_id);
      addStatusEvent(s.batch_id, flow.active, flow.to, worker, 'button', null);
      return { flags, workSec: Math.round(workSec), pauseSec: Math.round(pauseSec), batch_id: s.batch_id };
    }
  );
}

/**
 * 保留 (要件§7.5)。理由必須・「その他」は自由記述必須。
 * バッチを hold へ移し (復帰先を hold_from_status に保存)、保留区間を記録する。
 */
export function pauseProcess(process, sessionId, worker, reason, note, opId) {
  const flow = getFlow(process);
  validateOpId(opId);
  const noteTrim = normalizeNote(note);
  if (!isValidMaster(flow.pauseReasonKind, reason)) throw new SwError(400, '保留理由が不正です');
  const label = masterLabel(flow.pauseReasonKind, reason);
  if (label === 'その他' && !noteTrim) throw new SwError(400, '「その他」を選んだ場合は内容の記入が必須です');
  const db = getDB();
  return runIdempotent(
    {
      opId, worker, operation: `pause:${process}`, targetKind: 'session', targetId: sessionId,
      params: { reason, note: noteTrim },
    },
    () => {
      const s = getOwnSession(db, process, sessionId, worker);
      if (s.outcome !== 'open') throw new SwError(409, 'このセッションは終了しています。画面を更新してください');
      const batch = getBatch(s.batch_id);
      const activePause = db.prepare('SELECT * FROM sw_pauses WHERE session_id = ? AND resumed_at IS NULL').get(sessionId);
      if (batch.status === 'hold' && activePause) {
        // 別 op_id での二重クリック。今回の理由では保留していないので、実際に効いている理由を返す
        // (「その理由で保留できた」と誤解させない。Codex PR3レビュー round2 #low)
        return {
          noop: true,
          batch_id: s.batch_id,
          effectiveReason: masterLabel(flow.pauseReasonKind, activePause.reason),
          effectiveNote: activePause.reason_note,
        };
      }
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
      return { batch_id: s.batch_id };
    }
  );
}

/** 保留解除。バッチを hold_from_status へ戻し、保留区間を閉じる。 */
export function resumeProcess(process, sessionId, worker, opId) {
  const flow = getFlow(process);
  validateOpId(opId);
  const db = getDB();
  return runIdempotent(
    { opId, worker, operation: `resume:${process}`, targetKind: 'session', targetId: sessionId },
    () => {
      const s = getOwnSession(db, process, sessionId, worker);
      if (s.outcome !== 'open') throw new SwError(409, 'このセッションは終了しています。画面を更新してください');
      const batch = getBatch(s.batch_id);
      const activePause = db.prepare('SELECT id FROM sw_pauses WHERE session_id = ? AND resumed_at IS NULL').get(sessionId);
      if (batch.status === flow.active && !activePause) return { noop: true };  // 別 op_id での二重クリック
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
      return { batch_id: s.batch_id };
    }
  );
}

/**
 * 印刷トラブル (要件§7.3)。旧セッションを voided で残し (削除・上書きなし)、
 *   action='reprint' → 同一作業者で新セッション+新印刷ジョブ (計測やり直し・reprint フラグ)
 *   action='abort'   → バッチを ready へ戻す
 */
export function troubleProcess(process, sessionId, worker, reason, note, action, opId) {
  const flow = getFlow(process);
  validateOpId(opId);
  const noteTrim = normalizeNote(note);
  if (!isValidMaster('print_trouble_reason', reason)) throw new SwError(400, 'トラブル理由が不正です');
  const label = masterLabel('print_trouble_reason', reason);
  if (label === 'その他' && !noteTrim) throw new SwError(400, '「その他」を選んだ場合は内容の記入が必須です');
  if (action !== 'reprint' && action !== 'abort') throw new SwError(400, 'action が不正です');
  const db = getDB();
  return runIdempotent(
    {
      opId, worker, operation: `trouble:${process}`, targetKind: 'session', targetId: sessionId,
      params: { reason, note: noteTrim, action },
    },
    () => {
      const s = getOwnSession(db, process, sessionId, worker);
      if (s.outcome === 'voided') {
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
        // 計測やり直し。新セッションは reprint フラグ付き (要件§7.4 短時間再印刷の把握)。
        // 新セッションの op_id はこのトラブル操作の op_id を流用する (1操作=1記録)
        const info = db.prepare(`
          INSERT INTO sw_sessions (batch_id, process, worker, requested_at, op_id, flags_json)
          VALUES (?, ?, ?, ?, ?, '["reprint"]')
        `).run(s.batch_id, process, worker, now, opId);
        const newSessionId = Number(info.lastInsertRowid);
        db.prepare(`
          INSERT INTO sw_print_jobs (batch_id, session_id, doc_type, pdf_path, requested_by, requested_at)
          VALUES (?, ?, 'picking_list', ?, ?, ?)
        `).run(s.batch_id, newSessionId, batch.pdf_path, worker, now);
        addStatusEvent(s.batch_id, flow.active, flow.active, worker, 'button', `印刷トラブル(${label})→再印刷して開始し直し`);
        return { session_id: newSessionId, batch_id: s.batch_id, aborted: false };
      }
      // abort: バッチを本日のやることへ戻す
      db.prepare("UPDATE sw_batches SET status = 'ready', hold_from_status = NULL, updated_at = ? WHERE id = ?")
        .run(now, s.batch_id);
      addStatusEvent(s.batch_id, flow.active, 'ready', worker, 'button', `印刷トラブル(${label})→中止`);
      return { session_id: null, batch_id: s.batch_id, aborted: true };
    }
  );
}

/**
 * 「完了して次を開始」の次バッチ取得 (要件§7.2)。
 * 開始可能な先頭バッチ (持ち越し優先 → 出荷No順) を最大3回試行 (競合時は次候補へ)。
 * 開始できるものが無ければ null。
 */
export function startNextReady(process, worker, opId) {
  getFlow(process);
  validateOpId(opId, 'op_id_next');
  const db = getDB();
  // 「次を開始」自体を1つの冪等操作として記録する。候補一覧は他者の操作で入れ替わるため、
  // 再送のたびに候補を選び直すと別バッチを開始してしまう。
  // 「候補なし (null)」も結果として保存する — 保存しないと、再送までの間に ready バッチが
  // 増えた場合に再送が新しいバッチを開始してしまう (Codex PR3レビュー round2 #high)。
  // 手動開始 (start:) とは別の操作種別にして op_id の使い回しも弾く (同 round2 #medium)。
  const r = runIdempotent(
    { opId, worker, operation: `start-next:${process}`, targetKind: 'worker', targetId: 0 },
    () => {
      // 開始可能な先頭から順に試す。他者に取られた (lease_lost) ら次候補へ。
      // 帳票未添付のバッチは自動では選ばない (開始できないため。手動一覧には出して理由を示す)
      for (const c of listStartableBatches(process, jstToday())) {
        if (!c.pdf_path) continue;
        try {
          const started = startInternal(db, process, c.id, worker, opId);
          return { batch_id: started.batch_id, session_id: started.session_id };
        } catch (e) {
          // lease_lost はリース取得前に throw されるため書き込みは無い = 次候補へ進んで安全
          if (e instanceof SwError && e.code === 'lease_lost') continue;
          throw e;
        }
      }
      return { batch_id: null, session_id: null };
    }
  );
  if (r.batch_id == null) return null;
  const b = getBatchWithLabels(db, r.batch_id);
  return {
    batch_id: r.batch_id,
    shipping_no_label: b?.shipping_no_label || b?.shipping_no || '',
    session_id: r.session_id,
    already: r.already,
  };
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
