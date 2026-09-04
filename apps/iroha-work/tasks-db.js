/**
 * いろは在庫化作業アプリ — タスク (f_iroha_tasks) の DB 操作 (要件定義 v1.1)
 *
 * 状態モデル・遷移ルール・写像は tasks.js (純粋関数)。ここは DB の読み書きだけ。
 * 不変条件 (closed には close_reason/closed_at、on_hold には hold_reason …) は書く前に validateTaskInvariants で守る。
 * 状態変更は version の楽観ロック (2 台の iPad が同時に触っても後勝ちで壊さない) + 履歴 (f_iroha_app_events.task_id)。
 */
import { getDB, startSession, setMetaValue, logEvent, sourceOfTruth } from './db.js';
import {
  OPEN_STATUSES, CLOSE_REASONS, HOLD_REASONS,
  canTransition, transitionNeedsStaff, validateTaskInvariants,
} from './tasks.js';

const utcNow = () => new Date().toISOString();

/**
 * ⭐アプリ正本のときだけ書ける操作の関門 (Codex PR1 R15)。
 * ルーターで正本を見てから、更新するまでの間に Notion 正本へ戻されることがある
 * (正本の切替はカードの version を変えないので、楽観ロックでは気づけない)。
 * だから**更新と同じトランザクションの中で**もう一度見る。
 */
const NOT_APP_MODE = { ok: false, error: 'notion_mode', message: '正本が Notion に戻りました (一覧を更新してください)' };
const appModeGuard = () => (sourceOfTruth() === 'app' ? null : NOT_APP_MODE);
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

/** 終了 (履歴) の件数。listClosedTasks と同じ絞り込みで数える (一覧は上限つきなので件数は別に数える) */
export function countClosedTasks({ from = null, to = null, q = null } = {}) {
  const conds = ["status = 'closed'"];
  const args = [];
  if (from) { conds.push('closed_at >= ?'); args.push(String(from)); }
  if (to) { conds.push('closed_at < ?'); args.push(String(to)); }
  if (q) { conds.push('(product_name LIKE ? OR product_code LIKE ?)'); const like = `%${String(q).trim()}%`; args.push(like, like); }
  return getDB().prepare(`SELECT COUNT(*) c FROM f_iroha_tasks WHERE ${conds.join(' AND ')}`).get(...args).c;
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
  // カードに紐づかない履歴 (カードを消した記録など) は task_id を NULL に。
  // Number(null) = 0 で入れると、存在しない id 0 への外部キー違反になる
  const id = Number(taskId);
  const taskRef = Number.isSafeInteger(id) && id > 0 ? id : null;
  const t = taskRef == null ? null : getTask(taskRef);
  getDB().prepare(`INSERT INTO f_iroha_app_events (at, action, page_id, task_id, worker_id, worker_name, device_label, from_value, to_value, ok, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(utcNow(), action, t?.notion_page_id || null, taskRef, workerId, workerName, deviceLabel, from, to, ok ? 1 : 0, error);
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
  // 拠点は Notion のステータスに施設名が入っていたものだけ。無ければ NULL (未定) — いろはを既定にしない (要件 §W-2)
  const facility = row.facility_code || null;
  if (facility && !db.prepare('SELECT 1 FROM f_iroha_facilities WHERE code = ?').get(facility)) throw new Error(`取込行の拠点が不正です (${row.notion_page_id}): ${facility}`);
  const rec = {
    notion_page_id: String(row.notion_page_id),
    legacy_status: row.legacy_status ?? null,
    status: row.status,
    close_reason: row.close_reason ?? null,
    facility_code: facility,
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

const HTTP_BY_ERROR = { conflict: 409, bad_transition: 400, staff_required: 403, hold_reason_required: 400, close_reason_required: 400, not_found: 404, bad_request: 400,
  closed_task: 409, done_card: 409, active_sessions: 409, not_stray: 409,
  notion_mode: 409 };   // 取得後に正本が切り替わった = 競合 (入力不正ではない — Codex PR1 R17)
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
  // 作業中の人がいるまま終了にしない (実際の検査は下のトランザクションの中。ここでは早めに弾くだけ)
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
  // 状態の更新と記録を 1 つのトランザクションで。**終了からのやり直しは、理由が残らないなら再開もしない**
  // (例外的な操作なので、なぜ再開したかが消えるくらいなら失敗させる — Codex PR1 R3)。
  // それ以外は今までどおり、記録に失敗しても操作は成立させる (現場を止めない)
  const reopening = t.status === 'closed';
  const applied = db.transaction(() => {
    if (appModeGuard()) return { notApp: true };   // 正本が Notion に戻っていたら書かない (Codex PR1 R15)
    // 先に version を見る。別の端末が先に変えていたなら「競合」であって「作業中」ではない (Codex PR1 R5)
    const now2 = db.prepare('SELECT version FROM f_iroha_tasks WHERE id = ?').get(t.id);
    if (!now2 || now2.version !== t.version) return false;
    // 終了にするなら、**このトランザクションの中で**作業中の人を数える。
    // 外で数えると、数えた後・更新する前に別の接続 (miniPC も同じ DB を見る) が作業を始められる (Codex PR1 R4)
    if (to === 'closed') {
      const active = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL AND voided_at IS NULL').get(t.id).c;
      if (active > 0) return { active };
    }
    const r = db.prepare(`UPDATE f_iroha_tasks SET status = ?, hold_reason_code = ?, hold_reason_note = ?, close_reason = ?, closed_at = ?, closed_by = ?,
        started_at = ?, ready_at = ?, cancellation_requested_at = ?, version = ?, updated_at = ?, updated_by = ?
      WHERE id = ? AND version = ?`)
      .run(next.status, next.hold_reason_code, next.hold_reason_note, next.close_reason, next.closed_at, next.closed_by,
        next.started_at, next.ready_at, next.cancellation_requested_at, next.version, next.updated_at, next.updated_by, t.id, t.version);
    if (r.changes === 0) return false;
    const cleared = (t.ready_at && next.ready_at === null) ? ` ready_at→${t.ready_at}` : '';
    const line = { taskId: t.id, action: 'task_status', from: t.status, to: `${to}${closeReason ? ':' + closeReason : ''}${holdReason ? ':' + holdReason : ''}${reason ? ' (' + reason + ')' : ''}${cleared}`,
      workerId, workerName, deviceLabel, ok: true };
    if (reopening) logTaskEvent(line); else safeLogTaskEvent(line);
    return true;
  }).immediate();
  if (applied && applied.notApp) return NOT_APP_MODE;
  if (applied && applied.active) {
    return { ok: false, error: 'active_sessions', message: `このカードで作業中の人が ${applied.active} 人います。作業を終えてから変えてください` };
  }
  if (!applied) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: getTask(t.id) };
  return { ok: true, task: getTask(t.id) };
}

/** 「今日やる」(planned_date = YYYY-MM-DD) / 後日 (null)。未着手・保留のタスクだけ */
export function setPlannedDate({ taskId, plannedDate, expectVersion, actor = null, workerId = null, workerName = null, deviceLabel = null, guard = null }) {
  const db = getDB();
  const d = plannedDate == null || plannedDate === '' ? null : String(plannedDate);
  if (d && !/^\d{4}-\d{2}-\d{2}$/.test(d)) return { ok: false, error: 'bad_request', message: '日付は YYYY-MM-DD で指定してください' };
  // ⭐確かめるところから書くところまで全部 1 つのトランザクションに (要件 §U-2)
  return db.transaction(() => {
    if (guard) { const g0 = guard(); if (g0) return g0; }
    const g = appModeGuard();
    if (g) return g;
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
    // 版の確認が先 (別の端末が先に終了させていたら、closed_task ではなく競合として最新を返す — Codex PR1 R7)
    if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
    if (t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードは変えられません (履歴として残ります)' };
    const r = db.prepare('UPDATE f_iroha_tasks SET planned_date = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(d, utcNow(), actor, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    safeLogTaskEvent({ taskId: t.id, action: 'task_planned', from: t.planned_date, to: d, workerId, workerName, deviceLabel, ok: true });
    return { ok: true, task: getTask(t.id) };
  }).immediate();
}

/**
 * 「素性の分からないカード」の条件 (一覧と削除で同じものを使う — Codex FB R3)。
 *   名前が無い (または「(名称なし)」) / Notion のページに紐づかない / 入荷受付の行き先にも紐づかない / まだ終わっていない
 * どれか 1 つでも当てはまらなければ、消す対象ではない
 */
const STRAY_WHERE = `t.status <> 'closed'
      AND t.notion_page_id IS NULL AND t.destination_id IS NULL
      AND (t.product_name IS NULL OR TRIM(t.product_name) = '' OR t.product_name = '(名称なし)')`;

/** 名前のないカード (取込でも入荷受付でもない行)。管理画面で人が見て消すためだけの一覧 */
export function listNamelessTasks(limit = 50) {
  return getDB().prepare(`SELECT t.id, t.status, t.close_reason, t.product_code, t.product_name, t.qty, t.destination_id, t.notion_page_id,
      t.created_at, t.created_by, t.updated_by,
      (SELECT COUNT(*) FROM f_iroha_work_sessions s WHERE s.task_id = t.id) AS sessions,
      (SELECT COUNT(*) FROM f_iroha_work_sessions s WHERE s.task_id = t.id AND s.ended_at IS NULL AND s.voided_at IS NULL) AS active_sessions,
      (SELECT COUNT(*) FROM f_iroha_card_media m WHERE m.task_id = t.id AND m.deleted_at IS NULL AND m.staged_at IS NULL) AS media,
      (SELECT COUNT(*) FROM f_iroha_label_waits w WHERE w.task_id = t.id) AS label_waits
    FROM f_iroha_tasks t
    WHERE ${STRAY_WHERE}
    ORDER BY t.id LIMIT ?`).all(Math.max(1, Math.min(200, Number(limit) || 50)));
}

/**
 * 素性の分からないカードを片づける (管理者操作)。
 * 作業時間・写真・ラベル待ちが 1 つも無ければ**行ごと消す**。1 つでもあれば消さずに「終了 (在庫化対象外)」にして履歴に残す
 * (記録の持ち主を消さない)。どちらも 1 トランザクション
 * @returns {{ok, action:'deleted'|'closed', id}}
 */
export function removeStrayTask({ taskId, actor = null, reason = null }) {
  const db = getDB();
  return db.transaction(() => {
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません' };
    // 画面から送られた id をそのまま信じない。消していい条件をここでもう一度確かめる (Codex FB R3)
    const stray = db.prepare(`SELECT 1 FROM f_iroha_tasks t WHERE t.id = ? AND ${STRAY_WHERE}`).get(t.id);
    if (!stray) {
      return { ok: false, error: 'not_stray',
        message: 'このカードは片づけの対象ではありません (名前がある / Notion のカードがある / 入荷受付の行き先がある / もう終わっている)' };
    }
    const n = (sql) => db.prepare(sql).get(t.id).c;
    const used = n('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?')
      + n('SELECT COUNT(*) c FROM f_iroha_card_media WHERE task_id = ?')
      + n('SELECT COUNT(*) c FROM f_iroha_label_waits WHERE task_id = ?');
    const note = reason || '素性の分からないカード (管理画面から片づけ)';
    // 作業中の人がいるまま閉じない (カードが一覧から消えても記録が開きっぱなしになり、次の開始が塞がる — Codex FB R3)
    const active = n('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL AND voided_at IS NULL');
    if (active > 0) {
      return { ok: false, error: 'active_sessions', message: `このカードで作業中の人が ${active} 人います。作業を終えてから片づけてください` };
    }
    if (used > 0) {
      if (t.status === 'closed') return { ok: true, action: 'closed', id: t.id, already: true };
      db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'out_of_scope', closed_at = ?, closed_by = ?,
          hold_reason_code = NULL, hold_reason_note = NULL, cancellation_requested_at = NULL,
          migration_note = COALESCE(migration_note || ' / ', '') || ?, version = version + 1, updated_at = ?, updated_by = ?
        WHERE id = ? AND version = ?`)
        .run(utcNow(), actor, note, utcNow(), actor, t.id, t.version);
      logTaskEvent({ taskId: t.id, action: 'task_status', from: t.status, to: `closed:out_of_scope (${note})`, ok: true });
      return { ok: true, action: 'closed', id: t.id };
    }
    // 記録が無いので消す。履歴の行だけ先に外す (task_id の FK)
    db.prepare('DELETE FROM f_iroha_app_events WHERE task_id = ?').run(t.id);
    db.prepare('DELETE FROM f_iroha_tasks WHERE id = ?').run(t.id);
    logTaskEvent({ taskId: null, action: 'task_removed', to: `task#${t.id} ${t.product_code || ''} ${note}`, ok: true });
    return { ok: true, action: 'deleted', id: t.id };
  }).immediate();
}

/**
 * 「外部施設に出す準備OK」の切り替え (状態とは別のチェック。Notion のチェックボックスの置き換え)。
 * 終了したタスクでは触らない。誰でも押せる (出せる状態になったかは現場が判断する)
 */
export function setExternalReady({ taskId, ready, expectVersion, actor = null, workerId = null, workerName = null, deviceLabel = null }) {
  const db = getDB();
  const t = getTask(taskId);
  if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
  // 版の確認が先 (他の操作と同じ契約 — Codex PR1 R18)
  if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
  if (t.status === 'closed') return { ok: false, error: 'done_card', message: '終了したカードは変えられません' };
  const v = ready ? 1 : 0;
  return db.transaction(() => {
    const g = appModeGuard();
    if (g) return g;
    const r = db.prepare('UPDATE f_iroha_tasks SET external_ready = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(v, utcNow(), actor, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    safeLogTaskEvent({ taskId: t.id, action: 'task_external_ready', from: String(t.external_ready ? 1 : 0), to: String(v), workerId, workerName, deviceLabel, ok: true });
    return { ok: true, task: getTask(t.id) };
  }).immediate();
}

/**
 * 「どこが作業するか」だけを変える (要件 §W-3)。⭐status と planned_date は変えない —
 * 拠点を変えたら進捗が戻った、のような軸をまたぐ副作用を作らない。NULL = 未定に戻す。
 * 職員だけが呼ぶ (権限の判定は router)
 */
export function setFacility({ taskId, facilityCode, expectVersion, actor = null, workerId = null, workerName = null, deviceLabel = null, guard = null }) {
  const db = getDB();
  // 空文字は「未定」として受ける (画面の選択なしがそのまま来る)
  const code = facilityCode == null || facilityCode === '' ? null : String(facilityCode);
  // ⭐確かめるところから書くところまで全部 1 つのトランザクションに (要件 §U-2)。
  //   拠点の有効性まで中で見る — 外で見ると、見た後・書く前に miniPC がその拠点を無効にできる
  return db.transaction(() => {
    if (guard) { const g0 = guard(); if (g0) return g0; }
    const g = appModeGuard();
    if (g) return g;
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
    // 版の確認が先 (他の操作と同じ契約 — Codex PR1 R7 / R18)
    if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
    if (t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードは変えられません (履歴として残ります)' };
    if (code && !db.prepare('SELECT 1 FROM f_iroha_facilities WHERE code = ? AND active = 1').get(code)) {
      return { ok: false, error: 'bad_request', message: 'その拠点は選べません' };
    }
    const r = db.prepare('UPDATE f_iroha_tasks SET facility_code = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(code, utcNow(), actor, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    safeLogTaskEvent({ taskId: t.id, action: 'task_facility', from: t.facility_code, to: code, workerId, workerName, deviceLabel, ok: true });
    return { ok: true, task: getTask(t.id) };
  }).immediate();
}

/** 取込時に推定した状態を職員が確認済みにする */
export function clearMigrationReview({ taskId, expectVersion, actor = null }) {
  const db = getDB();
  const t = getTask(taskId);
  if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
  // 版の確認が先 (setPlannedDate と同じ契約 — Codex PR1 R7)
  if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
  if (t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードは変えられません (履歴として残ります)' };
  return db.transaction(() => {
    const g = appModeGuard();
    if (g) return g;
    const r = db.prepare('UPDATE f_iroha_tasks SET migration_review = 0, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(utcNow(), actor, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    safeLogTaskEvent({ taskId: t.id, action: 'task_review_cleared', ok: true });
    return { ok: true, task: getTask(t.id) };
  }).immediate();
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
  // 正本の確認は更新と同じトランザクションの中で (cancel の側は changeTaskStatus が同じ関門を通る — Codex PR1 R16)
  return db.transaction(() => {
    const g = appModeGuard();
    if (g) return g;
    const r = db.prepare('UPDATE f_iroha_tasks SET cancellation_requested_at = NULL, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(utcNow(), actor, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    safeLogTaskEvent({ taskId: t.id, action: 'task_cancel_continued', workerId, workerName, deviceLabel, ok: true });
    return { ok: true, task: getTask(t.id) };
  }).immediate();
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
  // 画面に商品名を出すのでタスクを結合する (task_id は NOT NULL + FK。task が消えた行は __orphan へ退避済み)
  return getDB().prepare(`SELECT w.*, t.product_code, t.product_name, t.qty AS task_qty, t.status AS task_status, t.hold_reason_code
    FROM f_iroha_label_waits w JOIN f_iroha_tasks t ON t.id = w.task_id
    ${conds.length ? 'WHERE ' + conds.map((c) => c.replace(/^task_id/, 'w.task_id').replace(/^done/, 'w.done')).join(' AND ') : ''}
    ORDER BY w.done, w.occurred_on DESC, w.id DESC LIMIT ?`).all(...args);
}

/**
 * ラベル待ちの登録・更新 (id 無し = 新規)。更新は version の楽観ロック。
 * fields = xlsx の列そのまま (発生日/記録者/発注済/ロット期限/数量/ロケーション Z・Y・none/貼り直し/LINE連絡日/再連絡日/入庫完了日/完了/備考)
 */
export function upsertLabelWait({ id = null, taskId, fields = {}, expectVersion = null, actor = null }) {
  const db = getDB();
  return db.transaction(() => appModeGuard() || upsertLabelWaitInTx({ id, taskId, fields, expectVersion, actor })).immediate();
}
/** 持ち主の確認と書き込みを同じトランザクションで (終了直後に記録が入らないように — Codex PR1 R4) */
function upsertLabelWaitInTx({ id, taskId, fields, expectVersion, actor }) {
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
    if (t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードには記録を足せません (履歴として残ります)' };
    const cols = Object.keys(rec);
    const info = db.prepare(`INSERT INTO f_iroha_label_waits (task_id${cols.map((c) => ', ' + c).join('')}, version, created_at, updated_at)
      VALUES (?${cols.map(() => ', ?').join('')}, 1, ?, ?)`).run(t.id, ...cols.map((c) => rec[c]), now, now);
    safeLogTaskEvent({ taskId: t.id, action: 'label_wait_add', to: JSON.stringify(rec).slice(0, 300), ok: true });
    return { ok: true, row: db.prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(Number(info.lastInsertRowid)) };
  }
  const cur = db.prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(Number(id));
  if (!cur) return { ok: false, error: 'not_found', message: 'ラベル待ちの記録が見つかりません' };
  // その記録が本当にそのカードのものか (別のカードの id を添えて書き換えられないように — Codex PR1 R2)
  if (taskId != null && Number(cur.task_id) !== Number(taskId)) {
    return { ok: false, error: 'not_found', message: 'ラベル待ちの記録が見つかりません' };
  }
  const owner = getTask(cur.task_id);
  if (owner && owner.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードの記録は変えられません (履歴として残ります)' };
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

/**
 * 棚入完了の一括 (要件 v1.1 §B: 棚入完了はいろは職員が操作・一括ボタン可)。
 * 「棚入待ち」のものだけを 終了 (棚入完了) にする。1 トランザクション・条件付き UPDATE なので、
 * 選んだ後に誰かが状態を変えた分は skipped で返す (黙って終了させない)。
 * @returns {{ok, done:[id], skipped:[{id,reason,title,status}]}}
 */
export function bulkCloseReady({ taskIds, actor = null, workerId = null, workerName = null, deviceLabel = null }) {
  const db = getDB();
  // ⭐選ぶときに見えていた版 ({ id, version }) を必ず添えてもらう。単票の変更と同じ楽観ロックにする
  //   (入口だけの検査にせず、この関数自体が版なしを受けない — Codex PR1 R7 / R8)
  const seen = new Set();
  const items = [];
  for (const v of (Array.isArray(taskIds) ? taskIds : [])) {
    if (!v || typeof v !== 'object' || v.version == null) {
      return { ok: false, error: 'bad_request', message: 'カードごとの版 (version) が必要です。一覧を更新してから選び直してください' };
    }
    const id = Number(v.id);
    const ver = Number(v.version);
    if (!Number.isSafeInteger(id) || id <= 0 || !Number.isSafeInteger(ver) || ver < 0) {
      return { ok: false, error: 'bad_request', message: 'カードの指定が不正です' };
    }
    if (seen.has(id)) continue;
    seen.add(id);
    items.push({ id, version: ver });
  }
  const ids = items.map((x) => x.id);
  if (ids.length === 0) return { ok: false, error: 'bad_request', message: 'カードが選ばれていません' };
  if (ids.length > 200) return { ok: false, error: 'bad_request', message: '一度に選べるのは 200 件までです' };
  return db.transaction(() => {
    const g = appModeGuard();
    if (g) return g;
    const now = utcNow();
    const done = [];
    const skipped = [];
    // 対象は 1 回の SELECT で引く (200 件×3 クエリにしない — Codex PR-C R1 Low)
    const found = new Map(db.prepare(`SELECT id, status, close_reason, product_name, product_code, version FROM f_iroha_tasks
      WHERE id IN (${ids.map(() => '?').join(',')})`).all(...ids).map((r) => [r.id, r]));
    const updVer = db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'stocked', closed_at = ?, closed_by = ?,
        hold_reason_code = NULL, hold_reason_note = NULL, cancellation_requested_at = NULL, ready_at = COALESCE(ready_at, ?),
        version = version + 1, updated_at = ?, updated_by = ?
      WHERE id = ? AND status = 'ready_for_stocking' AND version = ?`);
    for (const { id, version } of items) {
      const t = found.get(id);
      if (!t) { skipped.push({ id, reason: 'not_found' }); continue; }
      const title = t.product_name || t.product_code || `#${id}`;
      // 版の食い違いが先 (選んでから誰かが動かしていたなら「競合」。already・not_ready・作業中と混同しない — Codex PR1 R8)
      if (t.version !== version) { skipped.push({ id, reason: 'conflict', title }); continue; }
      if (t.status === 'closed' && t.close_reason === 'stocked') { skipped.push({ id, reason: 'already', title }); continue; }
      if (t.status !== 'ready_for_stocking') { skipped.push({ id, reason: 'not_ready', title, status: t.status }); continue; }
      if (db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL AND voided_at IS NULL').get(id).c > 0) {
        skipped.push({ id, reason: 'active_sessions', title }); continue;   // 作業中のまま終了にしない (Codex PR1 R3)
      }
      if (updVer.run(now, actor, now, now, actor, id, version).changes !== 1) { skipped.push({ id, reason: 'conflict', title }); continue; }
      // 履歴は握り潰さない (権限のいる操作。記録できないなら全部やり直す — Codex PR-C R1)
      logTaskEvent({ taskId: id, action: 'task_status', from: 'ready_for_stocking', to: 'closed:stocked (まとめて棚入完了)',
        workerId, workerName, deviceLabel, ok: true });
      done.push(id);
    }
    return { ok: true, done, skipped };
  }).immediate();
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
    const g = appModeGuard();   // 見てから書くまでに Notion 正本へ戻ることがある (Codex PR1 R18)
    if (g) return g;
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
  // 境界は >= (切替と同じミリ秒の記録を落とさない — 過少計上の方が危険。Codex A1b R3 #2)
  return {
    tasks: q("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_status' AND ok = 1 AND at >= ?"),
    updatedTasks: q('SELECT COUNT(*) c FROM f_iroha_tasks WHERE updated_at >= ?'),
    sessions: q('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id IS NOT NULL AND started_at >= ?'),
    media: q('SELECT COUNT(*) c FROM f_iroha_card_media WHERE task_id IS NOT NULL AND created_at >= ?'),
  };
}

let switchSourceHook = null;
/** テスト用: 正本と切替時刻を書いた後・監査ログの前に割り込む (監査ログ失敗の再現。本番では null) */
export function _setSwitchSourceHook(fn) { switchSourceHook = fn; }
/**
 * 正本の切替を 1 トランザクションで: source_of_truth・source_switched_at・監査ログ (f_iroha_app_events source_switch) を
 * まとめて書く。どれかが失敗したら全部戻す (「切り替わったのに時刻/ログが無い」を作らない — Codex A1b R3 #1)
 * @returns {switchedAt}
 */
export function switchSourceOfTruth({ from, to, actor, openTasks, changes = null, force = false }) {
  const db = getDB();
  return db.transaction(() => {
    const switchedAt = utcNow();
    setMetaValue('source_of_truth', to);
    setMetaValue('source_switched_at', switchedAt);
    if (switchSourceHook) switchSourceHook();
    const detail = changes ? `・Notion 未反映: 状態変更 ${changes.tasks} 回/更新タスク ${changes.updatedTasks}/作業時間 ${changes.sessions}/写真 ${changes.media}${force ? ' (force)' : ''}` : '';
    logEvent({ action: 'source_switch', pageId: null, deviceLabel: `session:${actor}`, from, to: `${to} (未完了 ${openTasks} 件${detail})`, ok: true });
    return { switchedAt, detail };
  }).immediate();
}
