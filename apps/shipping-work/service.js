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
  getDB, utcNow, jstToday, getBatch, addStatusEvent, addAuditLog, isValidMaster,
  batchHasDocument, BATCH_STATUSES, listMasters as listMastersRaw,
} from './db.js';

/** 業務エラー。router が status を HTTP ステータスに変換する。detail は画面で確認を出すための付随情報。 */
export class SwError extends Error {
  constructor(status, message, code, detail) {
    super(message);
    this.status = status;
    this.code = code || null;
    this.detail = detail || null;
  }
}

// 工程ごとの遷移定義 (from → active → to)。
//   resolveFrom  — そのバッチでの開始元状態。null を返すと「この工程の対象外」(409)
//   startableWhere — 開始できるバッチの一覧条件 (listStartableBatches)
//   printOnStart — 開始時に印刷ジョブを積むか。印刷はピッキングリストだけ
//     (納品書・送り状は現行どおりロケーション順に事前印刷 = 要件§8.1。仕分け・梱包では刷らない)
//   requireDocument — 開始に帳票が必須か (「開始=印刷」が成立する工程のみ)
//   recordMistakes — 完了時にミス件数を受け付けるか (要件§8.1: 梱包完了時に任意入力)
const PROCESS_FLOW = {
  picking: {
    label: 'ピッキング',
    resolveFrom: () => 'ready',
    startableWhere: "b.status = 'ready'",
    active: 'picking',
    to: 'picked',
    pauseReasonKind: 'pause_reason_pick',
    printOnStart: true,
    requireDocument: true,
    recordMistakes: false,
  },
  // 仕分けはアソート (複数商品の詰め合わせ) のみ。単品・同一複数は picked から直接梱包へ
  sorting: {
    label: '仕分け',
    resolveFrom: (batch) => (batch.bunrui === 'assort' ? 'picked' : null),
    startableWhere: "b.status = 'picked' AND b.bunrui = 'assort'",
    active: 'sorting',
    to: 'sorted',
    pauseReasonKind: 'pause_reason_pack',
    printOnStart: false,
    requireDocument: false,
    recordMistakes: false,
  },
  packing: {
    label: '梱包',
    resolveFrom: (batch) => (batch.bunrui === 'assort' ? 'sorted' : 'picked'),
    // resolveFrom と完全に一致させる (sorted はアソートのみ。ずれると一覧に出るのに開始できない)
    startableWhere: "((b.status = 'picked' AND b.bunrui != 'assort') OR (b.status = 'sorted' AND b.bunrui = 'assort'))",
    active: 'packing',
    to: 'done',
    pauseReasonKind: 'pause_reason_pack',
    printOnStart: false,
    requireDocument: false,
    recordMistakes: true,
  },
};
// 工程の順序 (完了訂正の「後続工程が始まっていないか」判定に使う)
const PROCESS_ORDER = ['picking', 'sorting', 'packing'];

// 工程が完了したときに入る状態 (どこまで進んだかの判定に使う)。PR4 で sorting/packing を追加。
const PROCESS_TO_STATUS = { picking: 'picked', sorting: 'sorted', packing: 'done' };
// 工程の進み順。手動訂正で「後の工程を巻き戻すか」を判定する
const STATUS_ORDER = ['ready', 'picking', 'picked', 'sorting', 'sorted', 'packing', 'done'];
// 管理者が直接指定できるステータス。作業中の状態 (picking/sorting/packing) は含めない —
// 対応する進行中セッションが無いまま「作業中」に見えるバッチができ、誰も操作できなくなるため。
// hold も除外 (復帰先 hold_from_status が決まらない)
export const ADMIN_FIXABLE_STATUSES = ['ready', 'picked', 'sorted', 'done', 'stock_return', 'cancelled'];

// 異常候補の閾値 (要件§7.4)。too_long は sw_settings で管理者が上書きできる (PR5 で設定画面)。
const TOO_SHORT_SEC = 60;
const EARLY_PAUSE_SEC = 60;
const DEFAULT_TOO_LONG_MIN = 120;
const SETTING_TOO_LONG_KEY = 'anomaly_too_long_minutes';

// 完了を訂正できる猶予 (分)。時間はあくまで利便性の境界で、安全性の本体は
// 「完了後に別の業務イベントが起きていないこと」の方 (canCorrectCompletion を参照)。
const DEFAULT_CORRECT_WINDOW_MIN = 5;
const SETTING_CORRECT_WINDOW_KEY = 'completion_correct_window_minutes';
// 作業者画面に出す「本日完了した作業」の件数。
// 「印刷を忘れた」の救済が目的なので、少なすぎると当日分が画面から消えて意味が薄れる。
// 1人あたり1日十数バッチ (851バッチ/月・10名) なので当日分は全部載る想定
const RECENT_COMPLETED_LIMIT = 30;

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
  // 開始元状態はバッチによって変わる (アソートは仕分けを経由する)。
  // null = この工程の対象外 (例: 単品バッチに仕分けは無い)
  const from = flow.resolveFrom(batch);
  if (!from) throw new SwError(409, `このバッチは${flow.label}の対象ではありません`, 'not_applicable');
  // 帳票が無いと「開始=印刷」が成立しない (実装計画§5)。ピッキングのみの制約 —
  // 仕分け・梱包はピッキングを通った時点で帳票があるはずで、開始時に刷りもしない。
  // リースを取る前に止め、開始済みデータを残さない (Codex PR3レビュー#3)
  if (flow.requireDocument && !batchHasDocument(batch)) {
    throw new SwError(409, '帳票PDFが未添付のため開始できません。管理者に添付を依頼してください', 'no_document');
  }
  // 進行中の作業は1人1つ (工程をまたいでも)。保留中 (paused=1) は対象外 = 保留して別の作業へ移れる
  // (保留理由「他作業への応援」の運用。DB制約 idx_sw_sessions_worker_open と同じ条件)
  const mine = db.prepare("SELECT id FROM sw_sessions WHERE worker = ? AND outcome = 'open' AND paused = 0").get(worker);
  if (mine) throw new SwError(409, '作業中のバッチがあります。完了・保留・中止のいずれかをしてから次を開始してください', 'busy_worker');
  const now = utcNow();
  // リース: 条件付き UPDATE。0行なら他の人が先に取った (ここまで書き込みなし)
  const res = db.prepare('UPDATE sw_batches SET status = ?, updated_at = ? WHERE id = ? AND status = ?')
    .run(flow.active, now, batchId, from);
  if (res.changes === 0) {
    throw new SwError(409, '他の人が先に開始したか、開始できない状態です。画面を更新してください', 'lease_lost');
  }
  const info = db.prepare(`
    INSERT INTO sw_sessions (batch_id, process, worker, requested_at, op_id)
    VALUES (?, ?, ?, ?, ?)
  `).run(batchId, process, worker, now, opId);
  const sessionId = Number(info.lastInsertRowid);
  if (flow.printOnStart) {
    db.prepare(`
      INSERT INTO sw_print_jobs (batch_id, session_id, doc_type, pdf_path, doc_url, requested_by, requested_at)
      VALUES (?, ?, 'picking_list', ?, ?, ?, ?)
    `).run(batchId, sessionId, batch.pdf_path, batch.doc_url, worker, now);
  }
  addStatusEvent(batchId, from, flow.active, worker, 'button', null);
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
/**
 * ミス記録の検証 (要件§8.1: 梱包完了時に任意入力。現行Notionの4分類×件数)。
 * [{ kind, count }] を検証して返す。ミスなし = 空配列。
 */
function normalizeMistakes(flow, raw) {
  if (!flow.recordMistakes || raw == null) return [];
  if (!Array.isArray(raw) || raw.length > 10) throw new SwError(400, 'ミス記録の形式が不正です');
  const seen = new Set();
  return raw.map((m) => {
    const kind = String(m?.kind || '');
    const count = Number(m?.count);
    if (!isValidMaster('mistake_kind', kind)) throw new SwError(400, 'ミスの分類が不正です');
    // 「分類×件数」が正なので同じ分類は1行だけ (重複を許すと集計が分類単位でなくなる)
    if (seen.has(kind)) throw new SwError(400, '同じミス分類が重複しています');
    seen.add(kind);
    if (!Number.isInteger(count) || count <= 0 || count > 999) {
      throw new SwError(400, 'ミスの件数は1以上の整数で入力してください');
    }
    return { kind, count };
  });
}

export function completeProcess(process, sessionId, worker, opId, opts = {}) {
  const flow = getFlow(process);
  validateOpId(opId);
  const mistakes = normalizeMistakes(flow, opts.mistakes);
  const db = getDB();
  return runIdempotent(
    {
      opId, worker, operation: `complete:${process}`, targetKind: 'session', targetId: sessionId,
      params: mistakes.length > 0 ? { mistakes } : undefined,
    },
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
      // ミス記録 (梱包完了時の任意入力)。完了と同一トランザクション = 冪等レイヤーで再送も安全
      for (const m of mistakes) {
        db.prepare(`
          INSERT INTO sw_mistakes (batch_id, process, kind, count, recorded_by, at)
          VALUES (?, ?, ?, ?, ?, ?)
        `).run(s.batch_id, process, m.kind, m.count, worker, now);
      }
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
          INSERT INTO sw_print_jobs (batch_id, session_id, doc_type, pdf_path, doc_url, requested_by, requested_at)
          VALUES (?, ?, 'picking_list', ?, ?, ?, ?)
        `).run(s.batch_id, newSessionId, batch.pdf_path, batch.doc_url, worker, now);
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
        if (getFlow(process).requireDocument && !batchHasDocument(c)) continue;
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
  // startableWhere は PROCESS_FLOW 内の固定文字列 (ユーザー入力は入らない)
  return getDB().prepare(`
    SELECT b.*,
      (SELECT label FROM sw_masters m WHERE m.kind='shipping_no' AND m.code=b.shipping_no) AS shipping_no_label,
      (SELECT label FROM sw_masters m WHERE m.kind='bunrui' AND m.code=b.bunrui) AS bunrui_label,
      (SELECT label FROM sw_masters m WHERE m.kind='packing_method' AND m.code=b.packing_method) AS packing_method_label
    FROM sw_batches b
    LEFT JOIN sw_masters sm ON sm.kind='shipping_no' AND sm.code=b.shipping_no
    WHERE ${flow.startableWhere} AND b.work_date <= ?
    ORDER BY b.work_date, COALESCE(sm.sort, 999), b.id
  `).all(workDate);
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

// ─── 完了後の救済 (要件v2 §2「例外は人が処理し、システムは記録する」の人側) ───
//
// 完了したら画面から消えて何も直せない、という状態が現場の詰みを生んでいたため、
// 「最近完了した作業」を残し、そこから ①帳票をもう一度出す ②完了を訂正する ができるようにする。
// どちらも過去の計測時刻は書き換えない。訂正は元セッションを completed のまま残し、
// 続きの時間は継続セッションに記録して集計で合算する。

/**
 * その完了セッションを本人が訂正できるか。
 * 時間の猶予より「完了後に別の業務イベントが起きていないこと」が本体
 * (後続工程が始まっていたら、戻すと二重処理になるため管理者へ回す)。
 */
function canCorrectCompletion(db, flow, session, batch, nowUtc) {
  if (session.outcome !== 'completed') return { ok: false, reason: 'このセッションは完了していません' };
  if (session.continues_session_id) {
    // 継続セッションまで訂正できると際限がないので、2回目以降は管理者へ
    return { ok: false, reason: '訂正後の作業です。もう一度直す場合は管理者に依頼してください' };
  }
  const already = db.prepare(
    'SELECT 1 FROM sw_sessions WHERE continues_session_id = ? LIMIT 1'
  ).get(session.id);
  if (already) return { ok: false, reason: 'この作業は既に訂正済みです' };
  if (batch.status !== flow.to) {
    return { ok: false, reason: '後の工程が進んでいるため訂正できません。管理者に依頼してください' };
  }
  // 後続の工程が始まっていたら戻さない (工程順で判定。前の工程の記録があるのは当然なので
  // 「自分より後ろ」だけを見る — 例: 梱包完了の訂正で、ピッキングの記録があるのは正常)。
  // 実際に進んだ証拠にならない outcome/validity は無視する (laterProcessSessions と同基準)
  const myIdx = PROCESS_ORDER.indexOf(session.process);
  const laterProcs = PROCESS_ORDER.slice(myIdx + 1);
  if (laterProcs.length > 0) {
    const later = db.prepare(`
      SELECT 1 FROM sw_sessions
      WHERE batch_id = ? AND process IN (${laterProcs.map(() => '?').join(',')})
        AND outcome IN ('open', 'completed') AND validity != 'invalid'
      LIMIT 1
    `).get(session.batch_id, ...laterProcs);
    if (later) return { ok: false, reason: '後の工程が始まっているため訂正できません。管理者に依頼してください' };
  }
  const windowMin = getSettingInt(SETTING_CORRECT_WINDOW_KEY, DEFAULT_CORRECT_WINDOW_MIN);
  if (secondsBetween(session.ended_at, nowUtc) > windowMin * 60) {
    return { ok: false, reason: `完了から${windowMin}分を過ぎたため訂正できません。管理者に依頼してください` };
  }
  return { ok: true };
}

/**
 * 本日完了した作業 (新しい順)。各行に帳票と訂正可否を添える。
 * 完了すると画面から消えてしまう問題への対処なので、進行中の作業があっても返す。
 *
 * ⭐バッチ単位にまとめる。訂正して再完了すると同じバッチに完了セッションが2つ並び、
 * セッション単位だと同じ出荷Noが2行出て、どちらを押せばよいか作業者に判別できない。
 * 代表は最新の完了セッション、作業時間は合算 (元 + 継続) を出す。
 */
export function listRecentCompleted(process, worker, limit = RECENT_COMPLETED_LIMIT) {
  const flow = getFlow(process);
  const db = getDB();
  const now = utcNow();
  const jstStartUtc = new Date(`${jstToday()}T00:00:00+09:00`).toISOString().slice(0, 19) + 'Z';
  // バッチごとに最新の完了セッションを1件だけ拾う
  const rows = db.prepare(`
    SELECT * FROM sw_sessions s
    WHERE s.worker = ? AND s.process = ? AND s.outcome = 'completed' AND s.ended_at >= ?
      AND s.id = (SELECT s2.id FROM sw_sessions s2
                  WHERE s2.batch_id = s.batch_id AND s2.process = s.process
                    AND s2.worker = s.worker AND s2.outcome = 'completed'
                  ORDER BY s2.ended_at DESC, s2.id DESC LIMIT 1)
    ORDER BY s.ended_at DESC LIMIT ?
  `).all(worker, process, jstStartUtc, limit);
  return rows.map((s) => {
    const batch = getBatchWithLabels(db, s.batch_id);
    const can = canCorrectCompletion(db, flow, s, batch, now);
    // 訂正して続きをやった場合は、元と継続の実作業時間を合算して見せる
    const total = db.prepare(`
      SELECT COALESCE(SUM(active_sec), 0) AS sec FROM sw_sessions
      WHERE batch_id = ? AND process = ? AND worker = ? AND outcome = 'completed'
        AND validity != 'invalid'
    `).get(s.batch_id, s.process, worker);
    return {
      session: s,
      batch,
      workSec: total.sec,
      endedAtJst: toJstHm(s.ended_at),
      canCorrect: can.ok,
      correctBlockedReason: can.ok ? null : can.reason,
    };
  });
}

/** UTC 'YYYY-MM-DDTHH:MM:SSZ' を JST の 'HH:MM' にする (画面表示用)。 */
function toJstHm(utc) {
  if (!utc) return '';
  const d = new Date(utc);
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Tokyo', hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(d);
}

/**
 * 完了後に帳票をもう一度出す。ステータスも計測も一切触らず、印刷ジョブを追記するだけ。
 * 「作業は終わったが紙だけ出ていない」を「作業もできていない」と混同しないため、訂正とは別操作にする。
 */
export function requestReprint(process, sessionId, worker, reason, note, opId) {
  getFlow(process);
  validateOpId(opId);
  const noteTrim = normalizeNote(note);
  if (!isValidMaster('reprint_reason', reason)) throw new SwError(400, '再印刷の理由が不正です');
  const label = masterLabel('reprint_reason', reason);
  if (label === 'その他' && !noteTrim) throw new SwError(400, '「その他」を選んだ場合は内容の記入が必須です');
  const db = getDB();
  return runIdempotent(
    {
      opId, worker, operation: `reprint:${process}`, targetKind: 'session', targetId: sessionId,
      params: { reason, note: noteTrim },
    },
    () => {
      const s = getOwnSession(db, process, sessionId, worker);
      // 完了後の再印刷に限る。作業中は「印刷トラブル」ボタンが別にあり、
      // ここを開けると reprint_reason の集計が「完了後の再印刷件数」を表さなくなる
      if (s.outcome !== 'completed') {
        throw new SwError(409, '完了した作業の帳票のみ出し直せます', 'not_completed');
      }
      const batch = getBatch(s.batch_id);
      if (!batchHasDocument(batch)) {
        throw new SwError(409, '帳票が設定されていません。管理者に依頼してください', 'no_document');
      }
      const now = utcNow();
      const info = db.prepare(`
        INSERT INTO sw_print_jobs
          (batch_id, session_id, doc_type, pdf_path, doc_url, reprint_reason, requested_by, requested_at)
        VALUES (?, ?, 'picking_list', ?, ?, ?, ?, ?)
      `).run(s.batch_id, s.id, batch.pdf_path, batch.doc_url,
        label + (noteTrim ? `: ${noteTrim}` : ''), worker, now);
      return { print_job_id: Number(info.lastInsertRowid), batch_id: s.batch_id };
    }
  );
}

/**
 * 完了の訂正 (要件v2 §2 の「例外ボタン」)。押し間違い・作業が残っていた場合に作業へ戻す。
 * 元セッションは completed のまま残し、続きは継続セッションに記録する。
 * 既に別の作業中なら、そちらを止めずに保留として積む (一人一作業の原則を壊さない)。
 */
export function correctCompletion(process, sessionId, worker, reason, note, opId) {
  const flow = getFlow(process);
  validateOpId(opId);
  const noteTrim = normalizeNote(note);
  if (!isValidMaster('correction_reason', reason)) throw new SwError(400, '訂正理由が不正です');
  const label = masterLabel('correction_reason', reason);
  if (label === 'その他' && !noteTrim) throw new SwError(400, '「その他」を選んだ場合は内容の記入が必須です');
  const db = getDB();
  return runIdempotent(
    {
      opId, worker, operation: `correct:${process}`, targetKind: 'session', targetId: sessionId,
      params: { reason, note: noteTrim },
    },
    () => {
      const s = getOwnSession(db, process, sessionId, worker);
      const batch = getBatch(s.batch_id);
      const now = utcNow();
      const can = canCorrectCompletion(db, flow, s, batch, now);
      if (!can.ok) throw new SwError(409, can.reason, 'cannot_correct');

      const detail = label + (noteTrim ? `: ${noteTrim}` : '');
      // 元セッションの計測 (時刻・active_sec・outcome) は書き換えない。
      // 採用してよいかは管理者が判断するので review_required に留め、
      // 作業者の訂正理由は correction_* に、管理者の判定は validity_* に分けて残す
      db.prepare(`
        UPDATE sw_sessions
        SET validity = 'review_required', correction_reason = ?, corrected_by = ?, corrected_at = ?
        WHERE id = ?
      `).run(detail, worker, now, s.id);

      // 既に別の作業を始めていたら、そちらは止めずにこのバッチを保留として積む
      const busy = db.prepare(
        "SELECT id FROM sw_sessions WHERE worker = ? AND outcome = 'open' AND paused = 0"
      ).get(worker);
      const toStatus = busy ? 'hold' : flow.active;
      const info = db.prepare(`
        INSERT INTO sw_sessions (batch_id, process, worker, requested_at, op_id, paused, continues_session_id, flags_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, '["correction"]')
      `).run(s.batch_id, process, worker, now, opId, busy ? 1 : 0, s.id);
      const newSessionId = Number(info.lastInsertRowid);
      if (busy) {
        // 保留として積む = 既存の「保留中の作業」から再開できる形にする
        db.prepare('INSERT INTO sw_pauses (session_id, reason, reason_note, paused_at) VALUES (?, ?, ?, ?)')
          .run(newSessionId, correctionPauseReason(flow), `完了を訂正: ${detail}`, now);
        db.prepare("UPDATE sw_batches SET status = 'hold', hold_from_status = ?, updated_at = ? WHERE id = ?")
          .run(flow.active, now, s.batch_id);
      } else {
        db.prepare('UPDATE sw_batches SET status = ?, updated_at = ? WHERE id = ?')
          .run(flow.active, now, s.batch_id);
      }
      addStatusEvent(s.batch_id, flow.to, toStatus, worker, 'button', `完了を訂正 (${detail})`);
      return { session_id: newSessionId, batch_id: s.batch_id, held: !!busy };
    }
  );
}

/** 訂正で積む保留に使う理由コード (「管理者確認待ち」相当を選ぶ)。 */
function correctionPauseReason(flow) {
  const rows = listMastersRaw(flow.pauseReasonKind);
  const hit = rows.find((r) => r.label === '管理者確認待ち') || rows[0];
  return hit ? hit.code : 'pk1';
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
  // バッチ単位で数える。完了を訂正して作業に戻り再度完了すると、同じバッチに
  // 完了セッションが2つ (元 + 継続) 並ぶため、セッション数で数えると二重に計上される
  const stats = db.prepare(`
    SELECT COUNT(*) AS batches, COALESCE(SUM(slip_count), 0) AS slips FROM (
      SELECT b.id, b.slip_count
      FROM sw_sessions s JOIN sw_batches b ON b.id = s.batch_id
      WHERE s.worker = ? AND s.process = ? AND s.outcome = 'completed' AND s.ended_at >= ?
        -- 管理者が「除外」と判定した計測は入れない。
        -- review_required (確認待ち) は入れる — 訂正して続きをやった場合の元セッションが
        -- ここに入るので、外すと実際にやった時間が消えてしまう。
        -- 「本当に作業していない」ものだけ管理者が invalid にして外す
        AND s.validity != 'invalid'
        -- 完了を訂正して作業に戻したものは、終わるまで実績に数えない
        AND NOT EXISTS (SELECT 1 FROM sw_sessions o
          WHERE o.batch_id = b.id AND o.process = s.process AND o.outcome = 'open')
      GROUP BY b.id
    )
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
    const ready = listStartableBatches(process, today);
    // 「開始できるバッチが無い」には2種類ある — 全部やり終えた (お疲れさま) と、
    // そもそも1件も登録されていない (バッチ作成待ち)。画面の文言を出し分けるため区別する
    const noBatchesAtAll = ready.length === 0 && !db.prepare(
      "SELECT 1 FROM sw_batches WHERE work_date <= ? AND status != 'cancelled' LIMIT 1"
    ).get(today);
    return {
      session: null, batch: null, pauseSec: 0, paused, ready, stats, noBatchesAtAll,
      recentCompleted: listRecentCompleted(process, worker),
    };
  }
  return {
    session: s,
    batch: getBatchWithLabels(db, s.batch_id),
    pauseSec: closedPauseSec(db, s.id),
    paused,
    ready: [],
    stats,
    noBatchesAtAll: false,
    // 作業中でも直前の完了は見えるようにする (完了直後に「印刷し忘れた」と気づけるように)
    recentCompleted: listRecentCompleted(process, worker),
  };
}

// ═══ 管理者の救済 (要件v2 §5「管理者はステータス手動変更可・理由必須・履歴必須・
//     計測データは上書きされない」/ §14-8「手動修正・監査ログ」) ═══
//
// 作業者の訂正は「自分の直前操作」に限定してあるので、条件を外れたものは全部ここへ来る。
// 現物とシステムが食い違ったときの最終手段なので、できることは広いが必ず理由と履歴を残す。
// 計測 (requested_at / ended_at / active_sec) は決して書き換えず、
// 「採用するかどうか」だけを validity で切り替える。

/** 管理者向けのバッチ詳細。現在の状態・遷移履歴・全セッション・印刷ジョブを1画面ぶん。 */

export function getBatchDetail(batchId) {
  const db = getDB();
  const batch = getBatchWithLabels(db, batchId);
  if (!batch) return null;
  const events = db.prepare(
    'SELECT * FROM sw_status_events WHERE batch_id = ? ORDER BY id'
  ).all(batchId).map((e) => ({ ...e, atJst: toJstDateTime(e.at) }));
  const sessions = db.prepare(
    'SELECT * FROM sw_sessions WHERE batch_id = ? ORDER BY id'
  ).all(batchId).map((s) => ({
    ...s,
    requestedAtJst: toJstDateTime(s.requested_at),
    endedAtJst: s.ended_at ? toJstDateTime(s.ended_at) : null,
    pauses: db.prepare('SELECT * FROM sw_pauses WHERE session_id = ? ORDER BY id').all(s.id),
  }));
  const printJobs = db.prepare(
    'SELECT * FROM sw_print_jobs WHERE batch_id = ? ORDER BY id'
  ).all(batchId).map((j) => ({ ...j, requestedAtJst: toJstDateTime(j.requested_at) }));
  return { batch, events, sessions, printJobs };
}

/** UTC を JST の 'MM/DD HH:MM' にする (管理画面の表示用)。 */
function toJstDateTime(utc) {
  if (!utc) return '';
  return new Intl.DateTimeFormat('ja-JP', {
    timeZone: 'Asia/Tokyo', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(new Date(utc));
}

/**
 * ステータスの手動訂正 (理由必須)。計測は書き換えない。
 * 進行中セッションが残ったまま状態だけ動かすと作業者が次の作業を取れなくなるため、
 * 開いているセッションは cancelled で閉じる (時刻は残す)。
 */
export function adminFixStatus(batchId, toStatus, actor, reason, opId, opts = {}) {
  validateOpId(opId);
  const reasonTrim = normalizeNote(reason);
  if (!reasonTrim) throw new SwError(400, '訂正の理由は必須です');
  // ⭐遷移先は「安定状態」だけ。picking/sorting/packing のような作業中状態へ直接飛ばすと、
  // 対応する進行中セッションが無いまま「作業中」に見えるバッチができ、
  // 誰も開始も完了もできなくなる (開始は ready からの条件付きUPDATEなので取れない)。
  // 管理者の役目は「現物がどうなっているかを確定させる」ことなので、これで足りる
  if (!ADMIN_FIXABLE_STATUSES.includes(toStatus)) {
    throw new SwError(400, `このステータスには直接変更できません (${ADMIN_FIXABLE_STATUSES.join(' / ')} のいずれか)`);
  }
  const db = getDB();
  return runIdempotent(
    {
      opId, worker: actor, operation: 'admin-fix-status', targetKind: 'batch', targetId: batchId,
      params: { toStatus, reason: reasonTrim, force: !!opts.force },
    },
    () => {
      const before = getBatch(batchId);
      if (!before) throw new SwError(404, 'バッチが見つかりません');
      const now = utcNow();
      const open = db.prepare("SELECT * FROM sw_sessions WHERE batch_id = ? AND outcome = 'open'").all(batchId);

      // 後続工程が動いていたら黙って巻き戻さない (梱包を始めているのに ready へ戻すと二重作業になる)。
      // 何を閉じることになるのかを返し、管理者が内容を見て force で確定する
      const laterProcesses = laterProcessSessions(db, batchId, toStatus);
      if (laterProcesses.length > 0 && !opts.force) {
        throw new SwError(409, '後の工程が進んでいます。内容を確認してください', 'later_process', {
          sessions: laterProcesses,
        });
      }
      // 状態が同じでも、開いたままのセッションが残っていれば閉じる用途で通す
      // (「done なのに作業中セッションだけ残った」不整合の救済)
      if (before.status === toStatus && open.length === 0) return { noop: true, status: toStatus };

      const closed = [];
      for (const s of open) {
        db.prepare('UPDATE sw_pauses SET resumed_at = ? WHERE session_id = ? AND resumed_at IS NULL')
          .run(now, s.id);
        // 未終了なら訂正時刻を終了とみなし、実作業秒数もここで確定させる。
        // 確定しておかないと「採用」しても集計対象の時間が無いセッションになる
        const endedAt = s.ended_at || now;
        const activeSec = s.active_sec != null
          ? s.active_sec
          : Math.max(0, Math.round(secondsBetween(s.requested_at, endedAt) - closedPauseSec(db, s.id)));
        db.prepare(`
          UPDATE sw_sessions
          SET outcome = 'cancelled', ended_at = ?, active_sec = ?, paused = 0,
              validity = 'review_required', correction_reason = ?, corrected_by = ?, corrected_at = ?
          WHERE id = ?
        `).run(endedAt, activeSec, `管理者がステータスを訂正: ${reasonTrim}`, actor, now, s.id);
        cancelOpenPrintJobs(db, s.id);
        // セッションごとに before/after を残す (どのセッションの何が変わったかを追えるように)
        addAuditLog(actor, 'admin_close_session', `sw_sessions:${s.id}`,
          { outcome: s.outcome, ended_at: s.ended_at, active_sec: s.active_sec, paused: s.paused, validity: s.validity },
          { outcome: 'cancelled', ended_at: endedAt, active_sec: activeSec, paused: 0, validity: 'review_required' },
          reasonTrim);
        closed.push({ id: s.id, process: s.process, worker: s.worker });
      }

      if (before.status !== toStatus) {
        db.prepare('UPDATE sw_batches SET status = ?, hold_from_status = NULL, updated_at = ? WHERE id = ?')
          .run(toStatus, now, batchId);
        addStatusEvent(batchId, before.status, toStatus, actor, 'admin', reasonTrim);
      }
      addAuditLog(actor, 'admin_fix_status', `sw_batches:${batchId}`,
        { status: before.status }, { status: toStatus, closedSessions: closed, force: !!opts.force }, reasonTrim);
      return { status: toStatus, closedSessions: closed.length };
    }
  );
}

/**
 * その遷移で「後の工程を巻き戻すことになる」セッション一覧。
 * 遷移先より後ろの工程で実際に進んだ記録があれば、管理者に見せてから確定させる。
 *
 * 判定は位置と結果の両方で行う (Codexレビュー round2 High):
 *   - 遷移先の直後より先の工程 (activeIdx > toStatus+1) → 記録があれば警告
 *   - 遷移先の直後の工程 (activeIdx == toStatus+1) → open は「今の工程の中止」なので
 *     通常フロー (閉じて確認待ち) に任せるが、completed なら完了済みの工程を
 *     やり直させることになるので警告する (例: picked → ready は完了済み picking の巻き戻し)
 *
 * 「実際に進んだ記録」に数えない outcome/validity:
 *   voided (印刷トラブルで破棄) / cancelled (管理者が閉じた) / print_failed /
 *   invalid 判定済み — これらは工程が進んだ証拠ではないので、残っていても警告し続けない。
 */
function laterProcessSessions(db, batchId, toStatus) {
  const idx = STATUS_ORDER.indexOf(toStatus);
  if (idx < 0) return [];
  const activeIdx = { picking: 1, sorting: 3, packing: 5 };
  const rows = db.prepare('SELECT * FROM sw_sessions WHERE batch_id = ? ORDER BY id').all(batchId);
  return rows
    .filter((s) => {
      const evidence = (s.outcome === 'completed' || s.outcome === 'open') && s.validity !== 'invalid';
      if (!evidence) return false;
      const p = activeIdx[s.process] ?? 0;
      if (p > idx + 1) return true;                       // 明確に後ろの工程
      return p === idx + 1 && s.outcome === 'completed';  // 直後の工程は完了済みのみ警告
    })
    .map((s) => ({ id: s.id, process: s.process, worker: s.worker, outcome: s.outcome }));
}

/**
 * セッションを計測として採用するかの判定 (valid / invalid)。
 * 時刻は触らず validity だけ切り替え、判断そのものも監査ログに追記する。
 */
export function adminJudgeSession(sessionId, validity, actor, reason, opId) {
  validateOpId(opId);
  const reasonTrim = normalizeNote(reason);
  if (!reasonTrim) throw new SwError(400, '判定の理由は必須です');
  // 判定は「採用」か「除外」だけ。review_required は作業者の訂正や管理者の強制終了で
  // 自動的に付くものなので、APIから戻せるようにはしない (画面にもその操作は無い)
  if (!['valid', 'invalid'].includes(validity)) {
    throw new SwError(400, '判定は採用 (valid) か除外 (invalid) のいずれかです');
  }
  const db = getDB();
  return runIdempotent(
    {
      opId, worker: actor, operation: 'admin-judge-session', targetKind: 'session', targetId: sessionId,
      params: { validity, reason: reasonTrim },
    },
    () => {
      const before = db.prepare('SELECT * FROM sw_sessions WHERE id = ?').get(sessionId);
      if (!before) throw new SwError(404, 'セッションが見つかりません');
      const now = utcNow();
      db.prepare(`
        UPDATE sw_sessions SET validity = ?, validity_reason = ?, validity_by = ?, validity_at = ?
        WHERE id = ?
      `).run(validity, reasonTrim, actor, now, sessionId);
      addAuditLog(actor, 'admin_judge_session', `sw_sessions:${sessionId}`,
        { validity: before.validity }, { validity }, reasonTrim);
      return { validity };
    }
  );
}

/** 管理者確認待ち (作業者が訂正した・管理者が閉じた) のセッション一覧。 */
export function listSessionsForReview(limit = 100) {
  const db = getDB();
  return db.prepare(`
    SELECT s.*, b.work_date, b.shipping_no,
      (SELECT label FROM sw_masters m WHERE m.kind='shipping_no' AND m.code=b.shipping_no) AS shipping_no_label
    FROM sw_sessions s JOIN sw_batches b ON b.id = s.batch_id
    WHERE s.validity = 'review_required'
    ORDER BY s.id DESC LIMIT ?
  `).all(limit).map((s) => ({
    ...s,
    requestedAtJst: toJstDateTime(s.requested_at),
    endedAtJst: s.ended_at ? toJstDateTime(s.ended_at) : null,
    correctedAtJst: s.corrected_at ? toJstDateTime(s.corrected_at) : null,
  }));
}
