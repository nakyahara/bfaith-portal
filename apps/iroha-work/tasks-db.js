/**
 * いろは在庫化作業アプリ — タスク (f_iroha_tasks) の DB 操作 (要件定義 v1.1)
 *
 * 状態モデル・遷移ルール・写像は tasks.js (純粋関数)。ここは DB の読み書きだけ。
 * 不変条件 (closed には close_reason/closed_at、on_hold には hold_reason …) は書く前に validateTaskInvariants で守る。
 * 状態変更は version の楽観ロック (2 台の iPad が同時に触っても後勝ちで壊さない) + 履歴 (f_iroha_app_events.task_id)。
 */
import { getDB, startSession } from './db.js';
import {
  OPEN_STATUSES, CLOSE_REASONS, HOLD_REASONS, DEFAULT_FACILITY,
  canTransition, transitionNeedsStaff, validateTaskInvariants,
} from './tasks.js';

const utcNow = () => new Date().toISOString();
const IMPORT_ACTOR_PREFIX = 'import:';

// ─── 参照 ───

export function listFacilities(includeInactive = false) {
  return getDB().prepare(`SELECT id, code, name, external, active, sort_order FROM f_iroha_facilities
    ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY sort_order, id`).all();
}

export function getTask(id) {
  const n = Number(id);
  if (!Number.isInteger(n) || n <= 0) return null;
  return getDB().prepare('SELECT * FROM f_iroha_tasks WHERE id = ?').get(n) || null;
}
export function getTaskByPageId(pageId) {
  if (!pageId) return null;
  return getDB().prepare('SELECT * FROM f_iroha_tasks WHERE notion_page_id = ?').get(String(pageId)) || null;
}
export function getTaskByDestination(destinationId) {
  const n = Number(destinationId);
  if (!Number.isInteger(n) || n <= 0) return null;
  return getDB().prepare('SELECT * FROM f_iroha_tasks WHERE destination_id = ?').get(n) || null;
}

/** 一覧・カンバン用 (終了は含めない)。facility で絞れる */
export function listOpenTasks({ facility = null } = {}) {
  const ph = OPEN_STATUSES.map(() => '?').join(',');
  return getDB().prepare(`SELECT * FROM f_iroha_tasks WHERE status IN (${ph}) ${facility ? 'AND facility_code = ?' : ''}
    ORDER BY CASE status WHEN 'in_progress' THEN 0 WHEN 'not_started' THEN 1 WHEN 'on_hold' THEN 2 ELSE 3 END,
      planned_date IS NULL, planned_date, arrival_date, id`).all(...OPEN_STATUSES, ...(facility ? [facility] : []));
}

/** 履歴 (終了したもの)。期間・検索で絞る — 溜まる一方でも一覧を邪魔しない (中原さん 9/3) */
export function listClosedTasks({ from = null, to = null, q = null, limit = 200 } = {}) {
  const conds = ["status = 'closed'"];
  const args = [];
  if (from) { conds.push('closed_at >= ?'); args.push(String(from)); }
  if (to) { conds.push('closed_at < ?'); args.push(String(to)); }
  if (q) { conds.push('(product_name LIKE ? OR product_code LIKE ?)'); const like = `%${String(q).trim()}%`; args.push(like, like); }
  args.push(Math.max(1, Math.min(2000, Number(limit) || 200)));
  return getDB().prepare(`SELECT * FROM f_iroha_tasks WHERE ${conds.join(' AND ')} ORDER BY closed_at DESC, id DESC LIMIT ?`).all(...args);
}

export function countTasksByStatus() {
  const rows = getDB().prepare('SELECT status, facility_code, COUNT(*) c FROM f_iroha_tasks GROUP BY status, facility_code').all();
  const byStatus = {}; const byFacility = {};
  for (const r of rows) {
    byStatus[r.status] = (byStatus[r.status] || 0) + r.c;
    if (OPEN_STATUSES.includes(r.status)) byFacility[r.facility_code] = (byFacility[r.facility_code] || 0) + r.c;
  }
  return { byStatus, byFacility, total: rows.reduce((s, r) => s + r.c, 0) };
}

/** 要確認 (取込時に状態を推定 / 取消要求) の一覧 — 職員が片付ける */
export function listTasksNeedingReview() {
  return getDB().prepare(`SELECT * FROM f_iroha_tasks
    WHERE status <> 'closed' AND (migration_review = 1 OR cancellation_requested_at IS NOT NULL) ORDER BY id`).all();
}

// ─── 履歴 ───

export function logTaskEvent({ taskId, action, from = null, to = null, workerId = null, workerName = null, deviceLabel = null, ok = true, error = null }) {
  const t = getTask(taskId);
  getDB().prepare(`INSERT INTO f_iroha_app_events (at, action, page_id, task_id, worker_id, worker_name, device_label, from_value, to_value, ok, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(utcNow(), action, t?.notion_page_id || null, Number(taskId), workerId, workerName, deviceLabel, from, to, ok ? 1 : 0, error);
}
export function safeLogTaskEvent(args) {
  try { logTaskEvent(args); } catch (e) { console.error('[iroha-work] タスク履歴の記録に失敗 (処理自体は完了)', e.message); }
}

// ─── 取込 (Notion → tasks) ───

// 差分取込で追随する商品情報 / 新規行だけに入れて以後は触らないもの (作成時スナップショット・要確認フラグ — Codex A1 R1 #2 #14) / 状態
const IMPORT_INFO_COLS = ['destination_id', 'product_code', 'product_name', 'qty', 'arrival_date', 'ar_no', 'barcode', 'expiry', 'supplier', 'handling',
  'payload', 'legacy_status'];
const IMPORT_NEW_ONLY_COLS = ['master_snapshot', 'migration_review', 'migration_note'];
const IMPORT_STATE_COLS = ['status', 'close_reason', 'facility_code', 'hold_reason_code', 'hold_reason_note', 'started_at', 'ready_at', 'closed_at', 'closed_by'];

/**
 * Notion の 1 ページを task に取り込む (notion_page_id で冪等)。
 *   新規: そのまま INSERT (作業仕様スナップショット・要確認フラグ込み)
 *   既存: 商品情報 (IMPORT_INFO_COLS) は更新。**master_snapshot は作成時のまま** (後で Notion 側が変わっても現場の指示を差し替えない)、
 *         migration_review も再設定しない (職員が確認済みにしたものを戻さない)。
 *         状態 (IMPORT_STATE_COLS) は **アプリ側で一度も触っていない** (updated_by が import:*) 行だけ更新 —
 *         切替前の差分取込で Notion の変更を追いかけつつ、アプリで変えた状態を Notion の古い値で戻さない
 * @returns {{action:'inserted'|'updated'|'kept', id:number}}
 */
export function upsertTaskFromImport(row, { batchId, now = utcNow() }) {
  const db = getDB();
  const facility = row.facility_code || DEFAULT_FACILITY;
  if (!db.prepare('SELECT 1 FROM f_iroha_facilities WHERE code = ?').get(facility)) throw new Error(`取込行の拠点が不正です (${row.notion_page_id}): ${facility}`);
  const rec = {
    notion_page_id: String(row.notion_page_id),
    legacy_status: row.legacy_status ?? null,
    status: row.status,
    close_reason: row.close_reason ?? null,
    facility_code: row.facility_code || DEFAULT_FACILITY,
    hold_reason_code: row.hold_reason_code ?? null,
    hold_reason_note: row.hold_reason_note ?? null,
    destination_id: row.destination_id == null ? null : Number(row.destination_id),
    product_code: row.product_code ?? null, product_name: row.product_name ?? null,
    qty: row.qty == null ? null : Number(row.qty),
    arrival_date: row.arrival_date ?? null, ar_no: row.ar_no ?? null, barcode: row.barcode ?? null,
    expiry: row.expiry ?? null, supplier: row.supplier ?? null, handling: row.handling ?? null,
    master_snapshot: row.master_snapshot == null ? null : (typeof row.master_snapshot === 'string' ? row.master_snapshot : JSON.stringify(row.master_snapshot)),
    payload: row.payload == null ? null : (typeof row.payload === 'string' ? row.payload : JSON.stringify(row.payload)),
    started_at: row.started_at ?? null, ready_at: row.ready_at ?? null,
    closed_at: row.status === 'closed' ? (row.closed_at || now) : null,
    closed_by: row.status === 'closed' ? (row.closed_by || 'import') : null,
    migration_review: row.migration_review ? 1 : 0,
    migration_note: row.migration_note ?? null,
  };
  const problems = validateTaskInvariants(rec);
  if (problems.length > 0) throw new Error(`取込行が不変条件を満たしません (${rec.notion_page_id}): ${problems.join(' / ')}`);
  const actor = `${IMPORT_ACTOR_PREFIX}${batchId}`;
  const existing = getTaskByPageId(rec.notion_page_id);
  if (!existing) {
    const cols = ['notion_page_id', ...IMPORT_INFO_COLS, ...IMPORT_NEW_ONLY_COLS, ...IMPORT_STATE_COLS, 'import_batch_id', 'version', 'created_at', 'created_by', 'updated_at', 'updated_by'];
    const vals = cols.map((c) => {
      if (c === 'import_batch_id') return batchId;
      if (c === 'version') return 1;
      if (c === 'created_at' || c === 'updated_at') return now;
      if (c === 'created_by' || c === 'updated_by') return actor;
      return rec[c];
    });
    const info = db.prepare(`INSERT INTO f_iroha_tasks (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`).run(...vals);
    return { action: 'inserted', id: Number(info.lastInsertRowid) };
  }
  const touchedByApp = !String(existing.updated_by || '').startsWith(IMPORT_ACTOR_PREFIX);
  const cols = touchedByApp ? IMPORT_INFO_COLS : [...IMPORT_INFO_COLS, ...IMPORT_STATE_COLS];
  // 変化が無ければ書かない (version を無駄に進めない)
  const changed = cols.some((c) => (existing[c] ?? null) !== (rec[c] ?? null));
  if (!changed) return { action: 'kept', id: existing.id };
  const sets = cols.map((c) => `${c} = ?`).join(', ');
  db.prepare(`UPDATE f_iroha_tasks SET ${sets}, import_batch_id = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ?`)
    .run(...cols.map((c) => rec[c]), batchId, now, touchedByApp ? existing.updated_by : actor, existing.id);
  return { action: 'updated', id: existing.id };
}

/**
 * 作業時間・写真・履歴の task_id を notion_page_id から埋める (取込後に実行。冪等)。
 * 埋まらないもの (task が無い page_id) は消さず孤立として数える (Codex 設計相談 R3)
 */
export function backfillTaskIds() {
  const db = getDB();
  const out = { sessions: 0, media: 0, events: 0, orphans: { sessions: 0, media: 0, events: 0 } };
  db.transaction(() => {
    for (const [key, table] of [['sessions', 'f_iroha_work_sessions'], ['media', 'f_iroha_card_media'], ['events', 'f_iroha_app_events']]) {
      out[key] = db.prepare(`UPDATE ${table} SET task_id = (SELECT t.id FROM f_iroha_tasks t WHERE t.notion_page_id = ${table}.page_id)
        WHERE task_id IS NULL AND page_id IS NOT NULL AND EXISTS (SELECT 1 FROM f_iroha_tasks t WHERE t.notion_page_id = ${table}.page_id)`).run().changes;
      out.orphans[key] = db.prepare(`SELECT COUNT(*) c FROM ${table} WHERE task_id IS NULL AND page_id IS NOT NULL`).get().c;
    }
  })();
  return out;
}

/** 孤立 (task に紐づかない) 作業時間・写真の一覧 (管理画面。page_id ごとに件数) */
export function listOrphans(limit = 100) {
  const db = getDB();
  const n = Math.max(1, Math.min(1000, Number(limit) || 100));
  return {
    sessions: db.prepare(`SELECT page_id, COUNT(*) c, MIN(started_at) first_at, MAX(started_at) last_at FROM f_iroha_work_sessions
      WHERE task_id IS NULL AND page_id IS NOT NULL GROUP BY page_id ORDER BY last_at DESC LIMIT ?`).all(n),
    media: db.prepare(`SELECT page_id, COUNT(*) c, MIN(created_at) first_at, MAX(created_at) last_at FROM f_iroha_card_media
      WHERE task_id IS NULL AND page_id IS NOT NULL AND deleted_at IS NULL GROUP BY page_id ORDER BY last_at DESC LIMIT ?`).all(n),
  };
}

// ─── 状態変更 ───

const HTTP_BY_ERROR = { conflict: 409, bad_transition: 400, staff_required: 403, hold_reason_required: 400, close_reason_required: 400, not_found: 404, bad_request: 400 };
export function taskErrorStatus(error) { return HTTP_BY_ERROR[error] || 400; }

/**
 * 状態を変える。許可遷移 (tasks.js) だけ・職員限定遷移は isStaff 必須・保留理由/終了理由は必須・
 * version 楽観ロック。遷移に伴う時刻 (started_at / ready_at / closed_at) をここで付ける。
 * @returns {ok:true, task} | {ok:false, error, message, current?}
 */
export function changeTaskStatus({ taskId, to, expectVersion, closeReason = null, holdReason = null, holdNote = null,
  actor = null, isStaff = false, workerId = null, workerName = null, deviceLabel = null, reason = null }) {
  const db = getDB();
  const t = getTask(taskId);
  if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
  if (expectVersion == null || Number(expectVersion) !== t.version) {
    return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: t };
  }
  if (t.status === to) return { ok: true, task: t, already: true };
  if (!canTransition(t.status, to)) {
    return { ok: false, error: 'bad_transition', message: `「${t.status}」から「${to}」へは変えられません` };
  }
  if (transitionNeedsStaff(t.status, to) && !isStaff) {
    return { ok: false, error: 'staff_required', message: 'この変更は職員のみです (職員の名前を選び、PINを入れてください)' };
  }
  if (t.status === 'closed' && !String(reason || '').trim()) {
    return { ok: false, error: 'bad_request', message: '終了したタスクを再開するには理由が必要です' };
  }
  const now = utcNow();
  const next = { ...t, status: to, version: t.version + 1, updated_at: now, updated_by: actor };
  next.hold_reason_code = to === 'on_hold' ? holdReason : null;
  next.hold_reason_note = to === 'on_hold' ? (holdNote || null) : null;
  next.close_reason = to === 'closed' ? closeReason : null;
  next.closed_at = to === 'closed' ? now : null;
  next.closed_by = to === 'closed' ? actor : null;
  if (to === 'in_progress' && !t.started_at) next.started_at = now;
  // ready_at は「最新サイクルで棚入待ちになった時刻」。やり直し・再開で作業中に戻るときは消す (古い時刻を残さない — Codex A1 R1 #9)。
  // 消した値は履歴 (to_value) に退避
  if (to === 'in_progress' && (t.status === 'ready_for_stocking' || t.status === 'closed')) next.ready_at = null;
  if (to === 'ready_for_stocking') next.ready_at = now;
  if (to === 'closed' && closeReason === 'stocked' && !next.ready_at) next.ready_at = now;
  if (to === 'closed') next.cancellation_requested_at = null;
  const problems = validateTaskInvariants(next);
  if (problems.length > 0) {
    if (to === 'on_hold') return { ok: false, error: 'hold_reason_required', message: `保留の理由を選んでください (${HOLD_REASONS.join(' / ')})${holdReason === 'other' ? '。「その他」は備考も必要です' : ''}` };
    if (to === 'closed') return { ok: false, error: 'close_reason_required', message: `終了の理由が必要です (${CLOSE_REASONS.join(' / ')})` };
    return { ok: false, error: 'bad_request', message: problems.join(' / ') };
  }
  const r = db.prepare(`UPDATE f_iroha_tasks SET status = ?, hold_reason_code = ?, hold_reason_note = ?, close_reason = ?, closed_at = ?, closed_by = ?,
      started_at = ?, ready_at = ?, cancellation_requested_at = ?, version = ?, updated_at = ?, updated_by = ?
    WHERE id = ? AND version = ?`)
    .run(next.status, next.hold_reason_code, next.hold_reason_note, next.close_reason, next.closed_at, next.closed_by,
      next.started_at, next.ready_at, next.cancellation_requested_at, next.version, next.updated_at, next.updated_by, t.id, t.version);
  if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: getTask(t.id) };
  const cleared = (t.ready_at && next.ready_at === null) ? ` ready_at→${t.ready_at}` : '';
  safeLogTaskEvent({ taskId: t.id, action: 'task_status', from: t.status, to: `${to}${closeReason ? ':' + closeReason : ''}${holdReason ? ':' + holdReason : ''}${reason ? ' (' + reason + ')' : ''}${cleared}`,
    workerId, workerName, deviceLabel, ok: true });
  return { ok: true, task: getTask(t.id) };
}

/** 「今日やる」(planned_date = YYYY-MM-DD) / 後日 (null)。未着手・保留のタスクだけ */
export function setPlannedDate({ taskId, plannedDate, expectVersion, actor = null, workerId = null, workerName = null, deviceLabel = null }) {
  const db = getDB();
  const t = getTask(taskId);
  if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
  if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
  const d = plannedDate == null || plannedDate === '' ? null : String(plannedDate);
  if (d && !/^\d{4}-\d{2}-\d{2}$/.test(d)) return { ok: false, error: 'bad_request', message: '日付は YYYY-MM-DD で指定してください' };
  const r = db.prepare('UPDATE f_iroha_tasks SET planned_date = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
    .run(d, utcNow(), actor, t.id, t.version);
  if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
  safeLogTaskEvent({ taskId: t.id, action: 'task_planned', from: t.planned_date, to: d, workerId, workerName, deviceLabel, ok: true });
  return { ok: true, task: getTask(t.id) };
}

/** 取込時に推定した状態を職員が確認済みにする */
export function clearMigrationReview({ taskId, expectVersion, actor = null }) {
  const db = getDB();
  const t = getTask(taskId);
  if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
  if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
  const r = db.prepare('UPDATE f_iroha_tasks SET migration_review = 0, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
    .run(utcNow(), actor, t.id, t.version);
  if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
  safeLogTaskEvent({ taskId: t.id, action: 'task_review_cleared', ok: true });
  return { ok: true, task: getTask(t.id) };
}

// ─── 取消 (入荷受付のやり直し → PR-B で呼ぶ。要件 v1.1 §E) ───

function taskHasActivity(db, taskId) {
  const s = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND voided_at IS NULL').get(taskId).c;
  const m = db.prepare('SELECT COUNT(*) c FROM f_iroha_card_media WHERE task_id = ? AND deleted_at IS NULL').get(taskId).c;
  const e = db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE task_id = ? AND action = 'task_status'").get(taskId).c;
  return s + m + e > 0;
}

/**
 * 入荷側の取消。未着手・実績なしなら自動で終了 (取消)、着手済み・実績ありなら「取消要確認」にして
 * いろは職員が判断する (中原さん 9/3: 最終判断はいろはスタッフ)
 * @returns {ok:true, action:'none'|'closed'|'review', task?}
 */
export function requestCancellation({ destinationId, source = 'inbound_reversal', actor = null }) {
  const db = getDB();
  // 判定と更新を 1 つの書き込みトランザクションで (判定後に誰かが開始したタスクを自動取消しない — Codex A1 R1 #10)。
  // 自動取消の UPDATE は status と version を条件に持ち、0 行なら要確認へ倒す
  return db.transaction(() => {
    const t = getTaskByDestination(destinationId);
    if (!t) return { ok: true, action: 'none' };
    if (t.status === 'closed') return { ok: true, action: 'none', task: t };
    const now = utcNow();
    if (t.status === 'not_started' && !taskHasActivity(db, t.id)) {
      const r = db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'cancelled', closed_at = ?, closed_by = ?, hold_reason_code = NULL, hold_reason_note = NULL,
        cancellation_requested_at = NULL, cancellation_source = ?, version = version + 1, updated_at = ?, updated_by = ?
        WHERE id = ? AND status = 'not_started' AND version = ?`)
        .run(now, actor || source, source, now, actor || source, t.id, t.version);
      if (r.changes === 1) {
        safeLogTaskEvent({ taskId: t.id, action: 'task_status', from: t.status, to: 'closed:cancelled (auto)', ok: true });
        return { ok: true, action: 'closed', task: getTask(t.id) };
      }
    }
    const cur = getTask(t.id);
    const r2 = db.prepare('UPDATE f_iroha_tasks SET cancellation_requested_at = COALESCE(cancellation_requested_at, ?), cancellation_source = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(now, source, now, actor || source, cur.id, cur.version);
    if (r2.changes === 0) return { ok: false, error: 'conflict', message: '同時に変更されました。もう一度お試しください', current: getTask(cur.id) };
    safeLogTaskEvent({ taskId: cur.id, action: 'task_cancel_requested', to: source, ok: true });
    return { ok: true, action: 'review', task: getTask(cur.id) };
  }).immediate();
}

/** 取消要確認を職員が確定 (cancel) / 続行 (continue) */
export function resolveCancellation({ taskId, decision, expectVersion, actor = null, isStaff = false, workerId = null, workerName = null, deviceLabel = null }) {
  const db = getDB();
  const t = getTask(taskId);
  if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
  if (!t.cancellation_requested_at) return { ok: false, error: 'bad_request', message: '取消の要確認になっていません' };
  if (!isStaff) return { ok: false, error: 'staff_required', message: '取消の判断は職員のみです' };
  if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
  if (decision === 'cancel') {
    return changeTaskStatus({ taskId: t.id, to: 'closed', expectVersion: t.version, closeReason: 'cancelled', actor, isStaff: true, workerId, workerName, deviceLabel });
  }
  if (decision !== 'continue') return { ok: false, error: 'bad_request', message: 'decision は cancel / continue のどちらかです' };
  const r = db.prepare('UPDATE f_iroha_tasks SET cancellation_requested_at = NULL, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
    .run(utcNow(), actor, t.id, t.version);
  if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
  safeLogTaskEvent({ taskId: t.id, action: 'task_cancel_continued', workerId, workerName, deviceLabel, ok: true });
  return { ok: true, task: getTask(t.id) };
}

// ─── ラベル待ち (要件 v1.1 §C) ───

const LABEL_FIELDS = ['occurred_on', 'recorded_by_worker_id', 'recorded_by_name', 'label_ordered', 'lot_expiry', 'qty', 'location', 'reattach',
  'line_notified_on', 're_notified_on', 'restocked_on', 'done', 'note'];
const LABEL_BOOL = new Set(['label_ordered', 'reattach', 'done']);
const LABEL_DATE = new Set(['occurred_on', 'line_notified_on', 're_notified_on', 'restocked_on']);

export function listLabelWaits({ taskId = null, openOnly = true, limit = 500 } = {}) {
  const conds = []; const args = [];
  if (taskId != null) { conds.push('task_id = ?'); args.push(Number(taskId)); }
  if (openOnly) conds.push('done = 0');
  args.push(Math.max(1, Math.min(5000, Number(limit) || 500)));
  return getDB().prepare(`SELECT * FROM f_iroha_label_waits ${conds.length ? 'WHERE ' + conds.join(' AND ') : ''} ORDER BY done, occurred_on DESC, id DESC LIMIT ?`).all(...args);
}

/**
 * ラベル待ちの登録・更新 (id 無し = 新規)。更新は version の楽観ロック。
 * fields = xlsx の列そのまま (発生日/記録者/発注済/ロット期限/数量/ロケーション Z・Y・none/貼り直し/LINE連絡日/再連絡日/入庫完了日/完了/備考)
 */
export function upsertLabelWait({ id = null, taskId, fields = {}, expectVersion = null, actor = null }) {
  const db = getDB();
  const rec = {};
  for (const f of LABEL_FIELDS) {
    if (!(f in fields)) continue;
    let v = fields[f];
    if (LABEL_BOOL.has(f)) v = v ? 1 : 0;
    else if (LABEL_DATE.has(f)) { v = v == null || v === '' ? null : String(v); if (v && !/^\d{4}-\d{2}-\d{2}$/.test(v)) return { ok: false, error: 'bad_request', message: `${f} は YYYY-MM-DD で指定してください` }; }
    else if (f === 'qty') { v = v == null || v === '' ? null : Number(v); if (v != null && (!Number.isInteger(v) || v < 0)) return { ok: false, error: 'bad_request', message: '数量は 0 以上の整数です' }; }
    else if (f === 'location') { v = v == null || v === '' ? null : String(v); if (v && !['Z', 'Y', 'none'].includes(v)) return { ok: false, error: 'bad_request', message: 'ロケーションは Z / Y / none のどれかです' }; }
    else if (f === 'recorded_by_worker_id') v = v == null || v === '' ? null : Number(v);
    else v = v == null ? null : String(v).slice(0, 500);
    rec[f] = v;
  }
  const now = utcNow();
  if (id == null) {
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
    const cols = Object.keys(rec);
    const info = db.prepare(`INSERT INTO f_iroha_label_waits (task_id${cols.map((c) => ', ' + c).join('')}, version, created_at, updated_at)
      VALUES (?${cols.map(() => ', ?').join('')}, 1, ?, ?)`).run(t.id, ...cols.map((c) => rec[c]), now, now);
    safeLogTaskEvent({ taskId: t.id, action: 'label_wait_add', to: JSON.stringify(rec).slice(0, 300), ok: true });
    return { ok: true, row: db.prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(Number(info.lastInsertRowid)) };
  }
  const cur = db.prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(Number(id));
  if (!cur) return { ok: false, error: 'not_found', message: 'ラベル待ちの記録が見つかりません' };
  if (expectVersion == null || Number(expectVersion) !== cur.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: cur };
  const cols = Object.keys(rec);
  if (cols.length === 0) return { ok: true, row: cur, unchanged: true };
  const r = db.prepare(`UPDATE f_iroha_label_waits SET ${cols.map((c) => c + ' = ?').join(', ')}, version = version + 1, updated_at = ? WHERE id = ? AND version = ?`)
    .run(...cols.map((c) => rec[c]), now, cur.id, cur.version);
  if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: db.prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(cur.id) };
  safeLogTaskEvent({ taskId: cur.task_id, action: 'label_wait_update', to: JSON.stringify(rec).slice(0, 300), ok: true });
  void actor;
  return { ok: true, row: db.prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(cur.id) };
}

// ─── 作業開始 (アプリ正本) ───

/**
 * 作業開始を 1 つの BEGIN IMMEDIATE にまとめる (Codex A1b R1 #2): タスクの再確認 (終了していないか) → セッション INSERT →
 * 最初の開始なら 未着手→作業中。同じトランザクションなので、確認と INSERT の間に別端末が終了させることはできず、
 * 「終了したカードに活動中セッションが残る」ことがない。状態変更が通らなければセッションごと戻す。
 * @param snapshotOf (task) => 開始時の実効作業仕様 (router が masterOfTask で合成) / null
 */
let startTaskSessionHook = null;
/** テスト用: セッション INSERT の後・状態変更の前に割り込む (「別端末が同時に変えた」の再現。本番では null) */
export function _setStartTaskSessionHook(fn) { startTaskSessionHook = fn; }
export function startTaskSession({ taskId, worker, deviceLabel = null, snapshotOf = null }) {
  const db = getDB();
  const tx = db.transaction(() => {
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' };
    if (t.status === 'closed') {
      return { ok: false, error: 'done_card', message: 'このカードは終了しています (やり直すなら職員が状態を戻してください)' };
    }
    const r = startSession({
      taskId: t.id, productCode: t.product_code, title: t.product_name, worker, deviceLabel,
      masterSnapshot: snapshotOf ? snapshotOf(t) : undefined,
    });
    if (!r.already) {
      safeLogTaskEvent({ taskId: t.id, action: 'session_start', workerId: worker.id, workerName: worker.display_name,
        deviceLabel, to: 'start', ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
    }
    if (!r.ok) return r;
    if (startTaskSessionHook) startTaskSessionHook(t);
    let task = t;
    if (t.status === 'not_started') {
      const cs = changeTaskStatus({ taskId: t.id, to: 'in_progress', expectVersion: t.version,
        actor: `${worker.display_name} (いろはアプリ)`, workerId: worker.id, workerName: worker.display_name, deviceLabel });
      if (!cs.ok) throw Object.assign(new Error(cs.message || '状態を変更できませんでした'), { taskResult: cs });
      task = cs.task;
    }
    return { ok: true, already: !!r.already, sessionId: r.sessionId, startedAt: r.startedAt, task };
  });
  try { return tx.immediate(); } catch (e) {
    if (e.taskResult) return { ok: false, ...e.taskResult };   // ロールバック済み (セッションは残っていない)
    throw e;
  }
}

/**
 * 正本を app にしてからの記録の数 (Notion へ戻す前の警告用 — Codex A1b R1 #7)。
 * tasks = 状態変更の回数 (履歴から。同じタスクを 2 回変えれば 2)、updatedTasks = 何かしら更新されたタスクの数 (今日やる等も含む) — R2 #1
 */
export function countChangesSince(iso) {
  const db = getDB();
  const q = (sql) => db.prepare(sql).get(iso).c;
  return {
    tasks: q("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_status' AND ok = 1 AND at > ?"),
    updatedTasks: q('SELECT COUNT(*) c FROM f_iroha_tasks WHERE updated_at > ?'),
    sessions: q('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id IS NOT NULL AND started_at > ?'),
    media: q('SELECT COUNT(*) c FROM f_iroha_card_media WHERE task_id IS NOT NULL AND created_at > ?'),
  };
}
