/**
 * いろは在庫化作業アプリ — タスク (f_iroha_tasks) の DB 操作 (要件定義 v1.1)
 *
 * 状態モデル・遷移ルール・写像は tasks.js (純粋関数)。ここは DB の読み書きだけ。
 * 不変条件 (closed には close_reason/closed_at、止まっている理由 blocked_reason は未着手・作業中だけ …) は書く前に validateTaskInvariants で守る。
 * 状態変更は version の楽観ロック (2 台の iPad が同時に触っても後勝ちで壊さない) + 履歴 (f_iroha_app_events.task_id)。
 */
import { getDB, startSessions, setMetaValue, logEvent, sourceOfTruth } from './db.js';
import {
  OPEN_STATUSES, CLOSE_REASONS, BLOCK_REASONS, BLOCK_LABEL, BLOCKABLE_STATUSES, LEGACY_ON_HOLD,
  canTransition, transitionNeedsStaff, validateTaskInvariants,
} from './tasks.js';
import { ensureBatchForTask, syncSingleBatchStatus, recordBatchCounts, recomputeTaskDoneQty, soleBatchOfTask,
  recordStockingForTask, openConsignmentCount, deriveTaskStatus, batchConsignedOutCount, recordStocking,
  listBatchesOfTask, syncAllBatchesStatus, canBatchTransition, batchTransitionNeedsStaff,
  applyDerivedTaskStatus, startHomeBatches } from './batches.js';

const utcNow = () => new Date().toISOString();

/**
 * ⭐できた数・作れなかった数は**まとまりに書く**。カードの done_qty はその合計から出す (要件 §AB-1/§AB-3)。
 * まとまりが 1 つのうち (＝いまの全カード) は、これで今までと同じ数がカードにも載る。
 * ⚠必ず呼び出し側の書き込みトランザクションの中で。
 */
function applyCountsToSoleBatch(db, taskId, counts, batchId = null) {
  // ⭐どのまとまりかが指定されていればそこへ (預けたあとの いろは のぶん)。指定が無ければ「1 つだけ」のとき
  const b = batchId != null
    ? db.prepare("SELECT * FROM f_iroha_task_batches WHERE id = ? AND task_id = ? AND work_status <> 'cancelled'").get(batchId, taskId)
    : soleBatchOfTask(db, taskId);
  if (!b) return false;
  return recordBatchCounts(db, b.id, counts);
}

/**
 * ⭐数を書き換える前の関門。まとまりが 2 つ以上あるカードは、**どのまとまりの数か**が決まらない。
 *
 * 以前はここで黙って何もせず、カードだけ書き換えて「成功」を返していた。
 * すると一覧はまとまりの合計から出すので、**保存できたように見えて元の数に戻る**ことになる
 * (入力が消えて成功が返る = いちばん困る種類の壊れ方。Codex R1 重大)。
 * ⭐画面が `batch_id` で「どのまとまりか」を送ってきたら通す (預けたあとも いろは のできた数が入る — 自己レビュー B)。
 *   ただし**外にあずけているまとまりには入れない** — そのぶんの数は返却で入る。
 */
function rejectCountsOnSplitCard(db, taskId, wantsCounts, batchId = null) {
  if (!wantsCounts) return null;
  if (batchId != null) {
    const n = Number(batchId);
    const b = Number.isInteger(n) && n > 0
      ? db.prepare("SELECT id, work_status FROM f_iroha_task_batches WHERE id = ? AND task_id = ? AND work_status <> 'cancelled'").get(n, taskId) : null;
    if (!b) return { ok: false, error: 'bad_batch', message: 'そのぶんはこのカードにありません。一覧を更新してください' };
    const away = db.prepare(`SELECT COUNT(*) c FROM f_iroha_consignments WHERE batch_id = ? AND state IN ('planned','prepared','handed')`).get(b.id).c;
    if (away > 0) return { ok: false, error: 'bad_batch', message: '外にあずけているぶんの数は、返却を受け取るときに入れてください' };
    return null;
  }
  const rows = db.prepare("SELECT COUNT(*) c FROM f_iroha_task_batches WHERE task_id = ? AND work_status <> 'cancelled'").get(taskId);
  if (!rows || rows.c <= 1) {
    // まとまりが 1 つでも、それを丸ごと外にあずけているなら いろは の数は入れられない (返却で入る)
    if (rows && rows.c === 1 && openConsignmentCount(db, taskId) > 0) {
      return { ok: false, error: 'bad_batch', message: '外にあずけているぶんの数は、返却を受け取るときに入れてください' };
    }
    return null;
  }
  return { ok: false, error: 'split_card',
    message: 'このカードは作業が分かれています。どのぶんの数かを選んでから入れてください' };
}

/**
 * ⭐棚入待ち・棚入完了にする前の関門。外にあずけたぶん (渡す予定・用意ずみ・渡した) が残っていたら断る。
 * 「棚入待ち」= 全部そろった、なので外にある物があるうちは成り立たない。
 * 通してしまうと、棚入完了で**まだ外にある物まで「棚に入れた」記録**になる (自己レビュー A)。
 * ⚠いろは のぶんだけを先に棚入待ち・棚入れする (まとまり単位の進捗) は要件 §AB-11 の 5 で入れる。
 */
function rejectWhileConsignedOut(db, taskId) {
  const n = openConsignmentCount(db, taskId);
  if (n === 0) return null;
  return { ok: false, error: 'consign_open',
    message: '外にあずけているぶんがまだ返ってきていません。返却を受け取る (または預けをやめる) までは、全部そろったことにも終了にもできません' };
}

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
  return getDB().prepare(`SELECT id, code, name, external, offsite, active, sort_order FROM f_iroha_facilities
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
    ORDER BY CASE status WHEN 'in_progress' THEN 0 WHEN 'not_started' THEN 1 ELSE 3 END,
      blocked_reason IS NOT NULL, planned_date IS NULL, planned_date, arrival_date, id`).all(...OPEN_STATUSES, ...(facility ? [facility] : []));
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
  // 止まっている札 (案A) は進捗とは別に数える (管理画面の内訳用)
  const blocked = getDB().prepare('SELECT COUNT(*) c FROM f_iroha_tasks WHERE blocked_reason IS NOT NULL').get().c;
  return { byStatus, byFacility, blocked, total: rows.reduce((s, r) => s + r.c, 0) };
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
const IMPORT_STATE_COLS = ['status', 'close_reason', 'facility_code', 'hold_reason_code', 'hold_reason_note',
  'blocked_reason', 'blocked_note', 'blocked_at', 'blocked_by', 'started_at', 'ready_at', 'closed_at', 'closed_by'];

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
  // 旧「保留」の取込行は「進捗 + 止まっている理由」に読み替える (案A 2026-09-05)。
  // 保留の理由が無い旧行は「その他」+ メモ (理由不明) で止まっている札を付ける — 黙って「未着手」に戻さない
  const legacyHold = row.status === LEGACY_ON_HOLD;
  const status = legacyHold ? (row.started_at ? 'in_progress' : 'not_started') : row.status;
  const blockedReason = row.blocked_reason ?? (legacyHold ? (row.hold_reason_code || 'other') : null);
  const blockedNote = row.blocked_note ?? (legacyHold
    ? (row.hold_reason_code ? (row.hold_reason_note ?? null) : '取込: 保留の理由が記録されていませんでした')
    : null);
  const rec = {
    notion_page_id: String(row.notion_page_id),
    legacy_status: row.legacy_status ?? null,
    status,
    close_reason: row.close_reason ?? null,
    facility_code: facility,
    hold_reason_code: null,   // 旧列。書かない (validateTaskInvariants が拒否する)
    hold_reason_note: null,
    blocked_reason: blockedReason,
    blocked_note: blockedNote,
    blocked_at: blockedReason ? (row.blocked_at ?? now) : null,
    blocked_by: blockedReason ? (row.blocked_by ?? `import:${batchId}`) : null,
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
    const newId = Number(info.lastInsertRowid);
    // ⭐カードができたら「まとまり」も 1 つ用意する (要件 §AB-1)。カードだけある瞬間を作らない
    ensureBatchForTask(db, getTask(newId));
    return { action: 'inserted', id: newId };
  }
  const touchedByApp = !String(existing.updated_by || '').startsWith(IMPORT_ACTOR_PREFIX);
  const cols = touchedByApp ? IMPORT_INFO_COLS : [...IMPORT_INFO_COLS, ...IMPORT_STATE_COLS];
  // 変化が無ければ書かない (version を無駄に進めない)
  const changed = cols.some((c) => (existing[c] ?? null) !== (rec[c] ?? null));
  if (!changed) return { action: 'kept', id: existing.id };
  const sets = cols.map((c) => `${c} = ?`).join(', ');
  db.prepare(`UPDATE f_iroha_tasks SET ${sets}, import_batch_id = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ?`)
    .run(...cols.map((c) => rec[c]), batchId, now, touchedByApp ? existing.updated_by : actor, existing.id);
  // ⭐取込は既存カードの進捗 (IMPORT_STATE_COLS) も書き換えることがある。
  //   まとまりが 1 つのうちは一緒に動かす。まとまりが無い古い行はここで用意する (Codex R1 重大2)
  const fresh = getTask(existing.id);
  ensureBatchForTask(db, fresh);
  syncSingleBatchStatus(db, existing.id, fresh);
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

const HTTP_BY_ERROR = { conflict: 409, bad_transition: 400, staff_required: 403, close_reason_required: 400, not_found: 404, bad_request: 400,
  closed_task: 409, done_card: 409, active_sessions: 409, not_stray: 409, bad_done_qty: 400, bad_hold_memo: 400, bad_loss_qty: 400,
  bad_variance_note: 400, split_card: 409, ready_task: 409, bad_batch: 400, consign_open: 409,
  // 止まっている理由 (案A): 理由が無い/不正 = 400、止められない状態 = 409、止まっているので始められない = 409
  block_reason_required: 400, bad_block: 409, blocked: 409,
  notion_mode: 409 };   // 取得後に正本が切り替わった = 競合 (入力不正ではない — Codex PR1 R17)
export function taskErrorStatus(error) { return HTTP_BY_ERROR[error] || 400; }

/**
 * 状態を変える。許可遷移 (tasks.js) だけ・職員限定遷移は isStaff 必須・保留理由/終了理由は必須・
 * version 楽観ロック。遷移に伴う時刻 (started_at / ready_at / closed_at) をここで付ける。
 * @returns {ok:true, task} | {ok:false, error, message, current?}
 */
/**
 * できた数の受け取り (要件 §Y)。undefined = 触らない / null = 数えていないに戻す / 数値 = その数。
 * ⭐「無い」を 0 に丸めない — 0 個 (何もできていない) と 未入力 は別のこと
 */
export function normalizeDoneQty(v) {
  if (v === undefined) return { skip: true };
  if (v === null) return { value: null };
  // ⚠Number(' ') も Number(true) も Number([]) も数になる。**空白だけは「数えていない」**、
  //   それ以外の型は受けない — でないと未入力が黙って 0 個になる (Codex R1 軽微4)
  if (typeof v !== 'number' && typeof v !== 'string') return { error: 'bad_done_qty', message: 'できた数は数で入れてください' };
  const t = typeof v === 'string' ? v.trim() : v;
  if (t === '') return { value: null };
  const n = Number(t);
  if (!Number.isInteger(n) || n < 0) return { error: 'bad_done_qty', message: 'できた数は 0 以上の整数で入れてください' };
  if (n > 1_000_000) return { error: 'bad_done_qty', message: 'できた数が大きすぎます' };
  return { value: n };
}
/** 中断メモ。undefined = 触らない / 空 = 消す。長すぎる申し送りは切らずに断る (書いた人が気づけるように) */
/** ひとこと (作れなかった理由の自由記述)。⭐分類から選ばせない (要件 §AB-6 の反論 4) */
export function normalizeVarianceNote(v) {
  if (v === undefined) return { skip: true };
  if (v === null) return { value: null };
  if (typeof v !== 'string') return { error: 'bad_variance_note', message: 'ひとことは文字で入れてください' };
  const t = v.trim();
  if (t.length > 500) return { error: 'bad_variance_note', message: 'ひとことは 500 文字までです' };
  return { value: t === '' ? null : t };
}

/** 作れなかった数 (袋を破いた等)。⭐空欄を 0 と読まない — 「無い」と「数えていない」は別 (要件 §AB-3) */
export function normalizeLossQty(v) {
  const r = normalizeDoneQty(v);
  if (r.error) return { error: 'bad_loss_qty', message: r.message.replace('できた数', '作れなかった数') };
  return r;
}

export function normalizeHoldMemo(v) {
  if (v === undefined) return { skip: true };
  if (v === null) return { value: null };
  if (typeof v !== 'string') return { error: 'bad_hold_memo', message: '中断メモは文字で入れてください' };
  const t = v.trim();
  if (t.length > 500) return { error: 'bad_hold_memo', message: '中断メモは 500 文字までです' };
  return { value: t === '' ? null : t };
}

export function changeTaskStatus({ taskId, to, expectVersion, closeReason = null,
  doneQty = undefined, lossQty = undefined, varianceNote = undefined, holdMemo = undefined, batchId = undefined,
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
  next.hold_reason_code = null;   // 旧列。案A 以降は書かない
  next.hold_reason_note = null;
  // ⭐止まっている理由は 未着手・作業中 だけが持てる。棚入待ち・終了へ進むときは札を外す (外した中身は履歴へ)
  if (!BLOCKABLE_STATUSES.includes(to)) { next.blocked_reason = null; next.blocked_note = null; next.blocked_at = null; next.blocked_by = null; }
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
  // ⭐できた数と中断メモ (要件 §Y)。送られたときだけ書き換える (送らない = 今の値のまま)
  const dq = normalizeDoneQty(doneQty);
  if (dq.error) return { ok: false, error: dq.error, message: dq.message };
  if (!dq.skip) next.done_qty = dq.value;
  const lq = normalizeLossQty(lossQty);
  if (lq.error) return { ok: false, error: lq.error, message: lq.message };
  const vn = normalizeVarianceNote(varianceNote);
  if (vn.error) return { ok: false, error: vn.error, message: vn.message };
  const hm = normalizeHoldMemo(holdMemo);
  if (hm.error) return { ok: false, error: hm.error, message: hm.message };
  if (!hm.skip) next.hold_memo = hm.value;
  // 棚入待ち・棚入完了は「全部そろってから」(中原さん 2026-09-05)。数えていなくても、そこまで来たら全部できたとみなす。
  // 中断メモは申し送りなので、作業が終わったら消す (消した中身は履歴に残す)
  if (to === 'ready_for_stocking' || (to === 'closed' && closeReason === 'stocked')) {
    // 🚨**できた数を予定で上書きしない** (要件 §AB-3。2026-09-07 まではここで done_qty = qty にしていた)。
    //   1000 個の予定で 998 個しかできなくても、記録が 1000 個になってしまっていた。
    //   実際にできた数は、作業を終えるときに人が入れる。数えていなければ NULL のまま (0 でも予定でもない)
    next.hold_memo = null;
  }
  const problems = validateTaskInvariants(next);
  if (problems.length > 0) {
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
    // ⭐まとまりが 2 つ以上のカードで数を書き換えようとしたら、**書く前に**断る (Codex R1 重大)
    const split = rejectCountsOnSplitCard(db, t.id, !dq.skip || !lq.skip || !vn.skip, batchId ?? null);
    if (split) return { reject: split };
    // ⭐まとまりが 2 つ以上のカードで「作り終えた」をカードごと押されたら断る (要件 §AB-11 の 5)。
    //   どのぶんが終わったのかが決まらない。画面は batch_id を添えて まとまり単位の口へ送る。
    //   ⚠「外にあずけたぶんが…」より**先に**返す — 人がすぐ次にできること (どのぶんか選ぶ) を言うため
    const bn = db.prepare("SELECT COUNT(*) c FROM f_iroha_task_batches WHERE task_id = ? AND work_status <> 'cancelled'").get(t.id).c;
    if (bn > 1 && to === 'ready_for_stocking') {
      return { reject: { ok: false, error: 'split_card',
        message: 'このカードは作業が分かれています。どのぶんを作り終えたかを選んでください' } };
    }
    // ⭐外にあずけたぶんが残っているうちは「全部そろった」(棚入待ち) にも、**理由を問わず終了**にもしない (自己レビュー A / Codex R4 重大2)。
    //   取消や中止で閉じると、親は closed なのに外に 100 個ある、という状態になる。先に預けをやめる・返却を受け取る
    if (to === 'ready_for_stocking' || to === 'closed') {
      const out = rejectWhileConsignedOut(db, t.id);
      if (out) return { reject: out };
    }
    // 終了にするなら、**このトランザクションの中で**作業中の人を数える。
    // 外で数えると、数えた後・更新する前に別の接続 (miniPC も同じ DB を見る) が作業を始められる (Codex PR1 R4)
    if (to === 'closed') {
      const active = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL AND voided_at IS NULL').get(t.id).c;
      if (active > 0) return { active };
    }
    const r = db.prepare(`UPDATE f_iroha_tasks SET status = ?, hold_reason_code = ?, hold_reason_note = ?, close_reason = ?, closed_at = ?, closed_by = ?,
        blocked_reason = ?, blocked_note = ?, blocked_at = ?, blocked_by = ?,
        started_at = ?, ready_at = ?, cancellation_requested_at = ?, done_qty = ?, hold_memo = ?, version = ?, updated_at = ?, updated_by = ?
      WHERE id = ? AND version = ?`)
      .run(next.status, next.hold_reason_code, next.hold_reason_note, next.close_reason, next.closed_at, next.closed_by,
        next.blocked_reason ?? null, next.blocked_note ?? null, next.blocked_at ?? null, next.blocked_by ?? null,
        next.started_at, next.ready_at, next.cancellation_requested_at, next.done_qty ?? null, next.hold_memo ?? null,
        next.version, next.updated_at, next.updated_by, t.id, t.version);
    if (r.changes === 0) return false;
    // まとまりが 1 つだけなら作業状態も合わせる (移行のあいだの橋渡し — 要件 §AB-1)。
    // ⭐終了 (全部 done / cancelled) と、終了からのやり直し (全部 作業中) は**行き先が 1 つに決まる**ので、
    //   まとまりが 2 つ以上でも合わせる。合わせないと「カードは終了・まとまりは棚入待ち」が残る
    if (to === 'closed' || (t.status === 'closed' && to === 'in_progress')) syncAllBatchesStatus(db, t.id, next);
    else syncSingleBatchStatus(db, t.id, next);
    // ⭐カードを作業中にしたら、手元 (物を持ち帰らない拠点) のまとまりも作業中に。
    //   これから渡すぶん・外部のぶんは触らない。作業開始 (startTaskWork) でも同じことをする (Codex R2 中2)
    if (to === 'in_progress') startHomeBatches(db, t.id, now);
    // ⭐数はまとまりが正本。カードの done_qty はその合計に直す (要件 §AB-3)
    applyCountsToSoleBatch(db, t.id, { goodQty: dq.skip ? undefined : dq.value, lossQty: lq.skip ? undefined : lq.value, note: vn.skip ? undefined : vn.value },
      batchId ?? null);
    // ⭐棚に入れたら、その実績を残す (要件 §AB-2)。まとまりごと・まだ入れていない残り全部
    if (to === 'closed' && closeReason === 'stocked') recordStockingForTask(db, t.id, { at: now, by: actor });
    const cleared = ((t.ready_at && next.ready_at === null) ? ` ready_at→${t.ready_at}` : '')
      + ((t.blocked_reason && !next.blocked_reason) ? ` 札解除(${t.blocked_reason})` : '');
    // できた数と中断メモも履歴に残す。消えた申し送りを後から追えるように (ready_at と同じ考え方)
    const dqTxt = (next.done_qty ?? null) !== (t.done_qty ?? null) ? ` できた${t.done_qty ?? '—'}→${next.done_qty ?? '—'}` : '';
    const hmTxt = (next.hold_memo ?? null) !== (t.hold_memo ?? null)
      ? (next.hold_memo ? ` メモ:${next.hold_memo}` : ` メモ消去(${t.hold_memo})`) : '';
    const line = { taskId: t.id, action: 'task_status', from: t.status, to: `${to}${closeReason ? ':' + closeReason : ''}${reason ? ' (' + reason + ')' : ''}${cleared}${dqTxt}${hmTxt}`,
      workerId, workerName, deviceLabel, ok: true };
    if (reopening) logTaskEvent(line); else safeLogTaskEvent(line);
    return true;
  }).immediate();
  if (applied && applied.reject) return applied.reject;
  if (applied && applied.notApp) return NOT_APP_MODE;
  if (applied && applied.active) {
    return { ok: false, error: 'active_sessions', message: `このカードで作業中の人が ${applied.active} 人います。作業を終えてから変えてください` };
  }
  if (!applied) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: getTask(t.id) };
  return { ok: true, task: getTask(t.id) };
}

// ─── 止まっている理由の札 (要件 §Y-2 = 案A、中原さん 2026-09-05) ───

/** 止まっている理由を、画面に返す形に */
export function blockedOf(t) {
  if (!t || !t.blocked_reason) return null;
  return { reason: t.blocked_reason, label: BLOCK_LABEL[t.blocked_reason] || t.blocked_reason,
    note: t.blocked_note || null, at: t.blocked_at || null, by: t.blocked_by || null };
}

/** そのカードの「まとまり」(取消は除く)。router が職員判定に使う */
export function batchesOfTask(taskId) {
  return listBatchesOfTask(getDB(), taskId).filter((b) => b.work_status !== 'cancelled');
}

/**
 * ⭐**まとまり 1 つだけの進捗を変える** (要件 §AB-1 / §AB-11 の 5)。
 *
 * いろはのぶん 400 個が終わったら、外部に預けた 600 個が返るのを待たずに棚入れできる
 * (中原さん 2026-09-07「2 週間寝かせる理由がない」)。
 * ⭐カードの進捗は**まとまりから導く** (`deriveTaskStatus`)。手で書く正本にしない。
 *
 * まとまりが 1 つのカード (ふだんの全部) は今までどおり `changeTaskStatus` を通る — 見え方は変わらない。
 */
export function changeBatchStatus({ taskId, batchId, to, closeReason = null, expectVersion,
  doneQty = undefined, lossQty = undefined, varianceNote = undefined, reason = null,
  actor = null, isStaff = false, workerId = null, workerName = null, deviceLabel = null }) {
  const db = getDB();
  // まとまり単位で受けるのは「作業中に戻す / 作り終えた / 棚入完了」の 3 つだけ。
  // カードごと取り消す・対象外にするのは、そのぶんだけでは意味が無いのでカードの口で
  if (!['in_progress', 'ready_for_stocking', 'closed'].includes(to)) {
    return { ok: false, error: 'bad_transition', message: 'そのぶんだけで変えられるのは「作業中」「作り終えた」「棚入完了」だけです' };
  }
  if (to === 'closed' && closeReason !== 'stocked') {
    return { ok: false, error: 'bad_request', message: '取消・対象外はカードごとの操作です (そのぶんだけでは変えられません)' };
  }
  const dq = normalizeDoneQty(doneQty);
  if (dq.error) return { ok: false, error: dq.error, message: dq.message };
  const lq = normalizeLossQty(lossQty);
  if (lq.error) return { ok: false, error: lq.error, message: lq.message };
  const vn = normalizeVarianceNote(varianceNote);
  if (vn.error) return { ok: false, error: vn.error, message: vn.message };
  const want = to === 'closed' ? 'done' : to;
  return db.transaction(() => {
    const g = appModeGuard();
    if (g) return g;
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' };
    if (expectVersion == null || Number(expectVersion) !== t.version) {
      return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: t };
    }
    // ⭐棚入完了で閉じたカードは、**そのぶんだけ**やり直せる (Codex R2 中3)。
    //   カードごと戻すと、問題の無い他のぶんまで作業中に戻ってしまう。
    //   終了からのやり直しは理由を残す約束 (カードと同じ — 例外的な操作なので、なぜ戻したかが消えるくらいなら失敗させる)
    if (t.status === 'closed') {
      if (!(t.close_reason === 'stocked' && want === 'in_progress')) {
        return { ok: false, error: 'closed_task', message: '終了したカードは変えられません (履歴として残ります)' };
      }
      if (!String(reason || '').trim()) {
        return { ok: false, error: 'bad_request', message: '棚入完了にしたぶんをやり直すには理由が必要です' };
      }
    }
    const b = db.prepare("SELECT * FROM f_iroha_task_batches WHERE id = ? AND task_id = ? AND work_status <> 'cancelled'")
      .get(Number(batchId), t.id);
    if (!b) return { ok: false, error: 'bad_batch', message: 'そのぶんはこのカードにありません。一覧を更新してください' };
    if (b.work_status === want) return { ok: true, task: t, batch: b, already: true };
    if (!canBatchTransition(b.work_status, want)) {
      return { ok: false, error: 'bad_transition', message: `そのぶんは「${b.work_status}」から「${want}」へは変えられません` };
    }
    if (batchTransitionNeedsStaff(b.work_status, want) && !isStaff) {
      return { ok: false, error: 'staff_required', message: 'この変更は職員のみです (職員の名前を選び、PINを入れてください)' };
    }
    // ⭐**そのぶんが外にある間は、人が先へ進めない** (返却を受け取ると棚入待ちになる)。
    //   他のまとまり (いろはのぶん) は止めない — これが「先に棚入れできる」ということ
    if ((want === 'ready_for_stocking' || want === 'done') && batchConsignedOutCount(db, b.id) > 0) {
      return { ok: false, error: 'consign_open',
        message: 'このぶんは外にあずけています。返却を受け取る (または預けをやめる) と、棚入待ちになります' };
    }
    // 作業中の人がいるまま棚入完了にしない (作業時間はまだカード単位なので、カードで数える)
    if (want === 'done') {
      const active = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL AND voided_at IS NULL').get(t.id).c;
      if (active > 0) return { ok: false, error: 'active_sessions', message: `このカードで作業中の人が ${active} 人います。作業を終えてから変えてください` };
    }
    const now = utcNow();
    // 数はまとまりが正本 (要件 §AB-3)。送られたぶんだけ書き換える
    recordBatchCounts(db, b.id, { goodQty: dq.skip ? undefined : dq.value, lossQty: lq.skip ? undefined : lq.value,
      note: vn.skip ? undefined : vn.value });
    if (db.prepare('UPDATE f_iroha_task_batches SET work_status = ?, version = version + 1, updated_at = ? WHERE id = ? AND work_status = ?')
      .run(want, now, b.id, b.work_status).changes === 0) {
      return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    }
    // ⭐棚に入れたら、そのまとまりの実績を残す (要件 §AB-2)
    if (want === 'done') recordStocking(db, b.id, { at: now, by: actor });
    // ⭐カードの進捗はまとまりから導く。全部が棚に入って初めてカードが終了になる
    const derived = deriveTaskStatus(db, t.id);
    const nextStatus = derived === 'done' ? 'closed' : derived;
    const next = { ...t, status: nextStatus, version: t.version + 1, updated_at: now, updated_by: actor };
    next.hold_reason_code = null;
    next.hold_reason_note = null;
    if (!BLOCKABLE_STATUSES.includes(nextStatus)) { next.blocked_reason = null; next.blocked_note = null; next.blocked_at = null; next.blocked_by = null; }
    next.close_reason = nextStatus === 'closed' ? 'stocked' : null;
    next.closed_at = nextStatus === 'closed' ? now : null;
    next.closed_by = nextStatus === 'closed' ? actor : null;
    if (nextStatus === 'in_progress') { if (!t.started_at) next.started_at = now; next.ready_at = null; }
    // 棚入待ち・終了へ来たら申し送りは役目を終える (カードと同じ約束)
    if (nextStatus === 'ready_for_stocking' || nextStatus === 'closed') { next.ready_at = t.ready_at || now; next.hold_memo = null; }
    const problems = validateTaskInvariants(next);
    if (problems.length > 0) return { ok: false, error: 'bad_request', message: problems.join(' / ') };
    const r = db.prepare(`UPDATE f_iroha_tasks SET status = ?, hold_reason_code = NULL, hold_reason_note = NULL,
        close_reason = ?, closed_at = ?, closed_by = ?, blocked_reason = ?, blocked_note = ?, blocked_at = ?, blocked_by = ?,
        started_at = ?, ready_at = ?, cancellation_requested_at = ?, hold_memo = ?, version = ?, updated_at = ?, updated_by = ?
      WHERE id = ? AND version = ?`)
      .run(next.status, next.close_reason, next.closed_at, next.closed_by,
        next.blocked_reason ?? null, next.blocked_note ?? null, next.blocked_at ?? null, next.blocked_by ?? null,
        next.started_at, next.ready_at, nextStatus === 'closed' ? null : next.cancellation_requested_at, next.hold_memo ?? null,
        next.version, next.updated_at, next.updated_by, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    recomputeTaskDoneQty(db, t.id);   // カードの done_qty はまとまりの合計 (手で書く正本にしない)
    // ⭐終了からのやり直しは理由が残らないなら失敗させる (カードと同じ — logTaskEvent は投げる)
    const line = { taskId: t.id, action: 'task_batch_status', from: `#${b.seq} ${b.work_status} (カード ${t.status})`,
      to: `#${b.seq} ${want}${want === 'done' ? ' 棚入れ' : ''}${reason ? ' (' + reason + ')' : ''} → カード ${next.status}`,
      workerId, workerName, deviceLabel, ok: true };
    if (t.status === 'closed') logTaskEvent(line); else safeLogTaskEvent(line);
    return { ok: true, task: getTask(t.id), batch: db.prepare('SELECT * FROM f_iroha_task_batches WHERE id = ?').get(b.id) };
  }).immediate();
}

/**
 * 「⛔ 止まった」— 理由の札を付ける。進捗 (status) は変えない (作業中なら作業中のまま。80/180 の情報を失わない)。
 * 同じトランザクションで **このカードで作業中の人のタイマーを全員止める** (end_reason='pause'。止まっている間は数えない)。
 * できた数・中断メモも一緒に受ける (要件 §Y と同じ 1 回の書き込み)。
 * expect_version の楽観ロック — 止めて外して再開した後に遅れて届いた古い「止まった」が、新しい作業を止めないように。
 * @returns {ok, task, stopped:[{id, worker_id, worker_name, raw_seconds}]} / {ok:false, error, message, current?}
 */
export function setTaskBlock({ taskId, reason, note = null, doneQty = undefined, lossQty = undefined, varianceNote = undefined, holdMemo = undefined,
  batchId = undefined, expectVersion,
  actor = null, workerId = null, workerName = null, deviceLabel = null, guard = null }) {
  const db = getDB();
  if (!BLOCK_REASONS.includes(reason)) {
    return { ok: false, error: 'block_reason_required', message: `止まった理由を選んでください (${BLOCK_REASONS.join(' / ')})` };
  }
  const noteText = note == null ? null : String(note).trim().slice(0, 300) || null;
  if (reason === 'other' && !noteText) return { ok: false, error: 'block_reason_required', message: '「その他」は何で止まったかをメモに書いてください' };
  const dq = normalizeDoneQty(doneQty);
  if (dq.error) return { ok: false, error: dq.error, message: dq.message };
  const lqB = normalizeLossQty(lossQty);
  if (lqB.error) return { ok: false, error: lqB.error, message: lqB.message };
  const vnB = normalizeVarianceNote(varianceNote);
  if (vnB.error) return { ok: false, error: vnB.error, message: vnB.message };
  const hm = normalizeHoldMemo(holdMemo);
  if (hm.error) return { ok: false, error: hm.error, message: hm.message };
  return db.transaction(() => {
    if (guard) { const g0 = guard(); if (g0) return g0; }
    const g = appModeGuard();
    if (g) return g;
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' };
    if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: t };
    if (!BLOCKABLE_STATUSES.includes(t.status)) {
      return { ok: false, error: 'bad_block', message: t.status === 'closed' ? '終了したカードは止められません (履歴として残ります)' : 'できあがったカード (棚入待ち) は止められません。やり直すなら職員が作業中に戻してください' };
    }
    const now = utcNow();
    const splitB = rejectCountsOnSplitCard(db, t.id, !dq.skip || !lqB.skip || !vnB.skip, batchId ?? null);
    if (splitB) return splitB;
    const nextQty = dq.skip ? (t.done_qty ?? null) : dq.value;
    const nextMemo = hm.skip ? (t.hold_memo ?? null) : hm.value;
    const next = { ...t, blocked_reason: reason, blocked_note: noteText, blocked_at: now, blocked_by: actor, done_qty: nextQty, hold_memo: nextMemo };
    const problems = validateTaskInvariants(next);
    if (problems.length > 0) return { ok: false, error: 'bad_request', message: problems.join(' / ') };
    const r = db.prepare(`UPDATE f_iroha_tasks SET blocked_reason = ?, blocked_note = ?, blocked_at = ?, blocked_by = ?,
        done_qty = ?, hold_memo = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?`)
      .run(reason, noteText, now, actor, nextQty, nextMemo, now, actor, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: getTask(t.id) };
    // ⭐数はまとまりが正本 (要件 §AB-3)
    applyCountsToSoleBatch(db, t.id, { goodQty: dq.skip ? undefined : dq.value,
      lossQty: lqB.skip ? undefined : lqB.value, note: vnB.skip ? undefined : vnB.value }, batchId ?? null);
    // 止まっている間は作業時間を数えない → このカードで作業中の人を全員止める (pause)
    const active = db.prepare('SELECT id, worker_id, worker_name, started_at FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL AND voided_at IS NULL').all(t.id);
    const upd = db.prepare("UPDATE f_iroha_work_sessions SET ended_at = ?, end_reason = 'pause', raw_seconds = ? WHERE id = ?");
    const stopped = active.map((s) => {
      const raw = Math.max(0, Math.floor((Date.parse(now) - Date.parse(s.started_at)) / 1000));
      upd.run(now, raw, s.id);
      safeLogTaskEvent({ taskId: t.id, action: 'session_stop', workerId: s.worker_id, workerName: s.worker_name, deviceLabel, to: `pause (止まった: ${reason})`, ok: true });
      return { id: s.id, worker_id: s.worker_id, worker_name: s.worker_name, raw_seconds: raw };
    });
    const dqTxt = nextQty !== (t.done_qty ?? null) ? ` できた${t.done_qty ?? '—'}→${nextQty ?? '—'}` : '';
    const hmTxt = nextMemo !== (t.hold_memo ?? null) ? (nextMemo ? ` メモ:${nextMemo}` : ' メモ消去') : '';
    // 履歴は safe (失敗しても札とタイマー停止は成立させる — 現場を止めない。他の操作と同じ方針)。
    // 誰のタイマーを止めたかは f_iroha_work_sessions 自体 (ended_at / end_reason='pause') に残るので、履歴が欠けても追える
    safeLogTaskEvent({ taskId: t.id, action: 'task_blocked', from: t.blocked_reason || null,
      to: `${reason}${noteText ? ' (' + noteText + ')' : ''}${stopped.length ? ` タイマー停止 ${stopped.length} 人` : ''}${dqTxt}${hmTxt}`,
      workerId, workerName, deviceLabel, ok: true });
    // before = 札を付ける前の「できた数」「申し送り」(取り消しで戻すため — Codex PR #1200 R1 #4)
    return { ok: true, task: getTask(t.id), stopped, before: { done_qty: t.done_qty ?? null, hold_memo: t.hold_memo ?? null } };
  }).immediate();
}

/**
 * 札を外す (「はい、届いた」/ ラベル待ちの記録を完了にした / 職員が手で外す)。進捗は変えない。
 * expectVersion は省略可 (「作業をはじめる」から呼ぶときは版を持っていない — 人が確認して押した操作なので、そのまま外す)。
 * 外れていなければ already。@param via 何から外したか (履歴に残す)
 */
export function clearTaskBlock({ taskId, expectVersion = null, via = 'manual', actor = null, workerId = null, workerName = null, deviceLabel = null, guard = null }) {
  const db = getDB();
  return db.transaction(() => {
    if (guard) { const g0 = guard(); if (g0) return g0; }
    const g = appModeGuard();
    if (g) return g;
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' };
    if (expectVersion != null && Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: t };
    if (!t.blocked_reason) return { ok: true, task: t, already: true };
    return clearTaskBlockInTx(db, t, { via, actor, workerId, workerName, deviceLabel });
  }).immediate();
}
/** 同上 (トランザクションの中から呼ぶ用: 作業開始・ラベル待ちの完了と同じ書き込みにまとめる) */
function clearTaskBlockInTx(db, t, { via, actor = null, workerId = null, workerName = null, deviceLabel = null }) {
  const now = utcNow();
  const r = db.prepare(`UPDATE f_iroha_tasks SET blocked_reason = NULL, blocked_note = NULL, blocked_at = NULL, blocked_by = NULL,
      version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?`).run(now, actor, t.id, t.version);
  if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: getTask(t.id) };
  safeLogTaskEvent({ taskId: t.id, action: 'task_unblocked', from: `${t.blocked_reason}${t.blocked_note ? ' (' + t.blocked_note + ')' : ''}`, to: via,
    workerId, workerName, deviceLabel, ok: true });
  // ⭐札のせいで止めていた繰り上がりを反映する (札が付いている間は棚入待ちへ進めていない — §AB-1)
  applyDerivedTaskStatus(db, t.id, now, actor);
  return { ok: true, task: getTask(t.id) };
}

/**
 * 「⛔ 止まった」の取り消し (誤タップ — 監修 2026-09-05)。直後 (withinMs、既定 60 秒) だけ。
 * 札を外し、札を付けたときに止めたセッション (blocked_at と同時刻に pause で閉じたもの) をもう一度動かす。
 * 別の作業を始めてしまった人の分は動かさず skipped に返す (札を外すのは成立させる)
 */
export function undoTaskBlock({ taskId, expectVersion = null, sessionIds = null, restore = null, withinMs = 60000, actor = null, workerId = null, workerName = null, deviceLabel = null, guard = null }) {
  const db = getDB();
  const now = utcNow();
  try {
    return db.transaction(() => {
      if (guard) { const g0 = guard(); if (g0) return g0; }
      const g = appModeGuard();
      if (g) return g;
      const t = getTask(taskId);
      if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' };
      // 札を付けたときの版のまま、のときだけ (だれかが触っていたら戻さない)。版なしの呼び出しも通さない
      if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されているのでもどせません。最新の状態を表示します', current: t };
      if (!t.blocked_reason) return { ok: true, task: t, already: true, reopened: [], skipped: [] };
      const blockedMs = Date.parse(t.blocked_at || '');
      if (!Number.isFinite(blockedMs) || Date.now() - blockedMs < 0 || Date.now() - blockedMs > withinMs) {
        return { ok: false, error: 'too_late', message: '時間が経ったのでもどせません。「▶ 作業をはじめる」で札を外してください' };
      }
      // 札を付けたときに止めたセッション = 切符に書いてある id のうち、blocked_at と同時刻に pause で閉じたもの
      const ids = Array.isArray(sessionIds) ? sessionIds.map(Number).filter((n) => Number.isInteger(n) && n > 0) : null;
      const stopped = ids
        ? (ids.length ? db.prepare(`SELECT id, worker_id, worker_name, started_at FROM f_iroha_work_sessions
            WHERE task_id = ? AND end_reason = 'pause' AND ended_at = ? AND voided_at IS NULL AND id IN (${ids.map(() => '?').join(',')})`).all(t.id, t.blocked_at, ...ids) : [])
        : db.prepare("SELECT id, worker_id, worker_name, started_at FROM f_iroha_work_sessions WHERE task_id = ? AND end_reason = 'pause' AND ended_at = ? AND voided_at IS NULL").all(t.id, t.blocked_at);
      // 切符に書いた記録が 1 つでも見つからない (取り消された・変えられた) なら戻さない (Codex R2 #2)
      if (ids && stopped.length !== new Set(ids).size) return { ok: false, error: 'conflict', message: '止めた記録が変わったのでもどせません。最新の状態を表示します', current: t };
      // ⭐全員戻せるときだけ戻す — 札だけ外れて一部のタイマーが止まったまま、という状態を作らない (Codex R1 #5)
      const openOf = db.prepare('SELECT id FROM f_iroha_work_sessions WHERE worker_id = ? AND ended_at IS NULL LIMIT 1');
      for (const s of stopped) {
        if (openOf.get(s.worker_id)) return { ok: false, error: 'busy', message: s.worker_name + ' さんは別の作業をはじめているのでもどせません (札はそのまま)' };
      }
      // 札を外し、同時に入れた「できた数」「申し送り」も付ける前の値に戻す (Codex R1 #4)
      const dq = restore && 'done_qty' in restore ? restore.done_qty : t.done_qty;
      const hm = restore && 'hold_memo' in restore ? restore.hold_memo : t.hold_memo;
      const r = db.prepare(`UPDATE f_iroha_tasks SET blocked_reason = NULL, blocked_note = NULL, blocked_at = NULL, blocked_by = NULL,
          done_qty = ?, hold_memo = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?`)
        .run(dq ?? null, hm ?? null, now, actor, t.id, t.version);
      if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています。最新の状態を表示します', current: getTask(t.id) };
      const upd = db.prepare('UPDATE f_iroha_work_sessions SET ended_at = NULL, end_reason = NULL, raw_seconds = NULL WHERE id = ? AND ended_at IS NOT NULL');
      const reopened = [];
      for (const s of stopped) {
        upd.run(s.id);
        reopened.push({ id: s.id, worker_id: s.worker_id, worker_name: s.worker_name, started_at: s.started_at, already: false });
        safeLogTaskEvent({ taskId: t.id, action: 'session_undo_stop', workerId: s.worker_id, workerName: s.worker_name, deviceLabel, to: 'undo block', ok: true });
      }
      safeLogTaskEvent({ taskId: t.id, action: 'task_unblocked', from: `${t.blocked_reason}${t.blocked_note ? ' (' + t.blocked_note + ')' : ''}`, to: 'undo', workerId, workerName, deviceLabel, ok: true });
      safeLogTaskEvent({ taskId: t.id, action: 'task_block_undo', from: t.blocked_reason,
        to: `タイマー再開 ${reopened.length} 人${dq !== (t.done_qty ?? null) ? ` できた数 ${t.done_qty ?? '—'}→${dq ?? '—'}` : ''}`,
        workerId, workerName, deviceLabel, ok: true });
      return { ok: true, task: getTask(t.id), reopened, skipped: [] };
    }).immediate();
  } catch (e) {
    if (e && /SQLITE_CONSTRAINT/.test(String(e.code || e.message))) return { ok: false, error: 'busy', message: '別の作業がはじまっているのでもどせません (札はそのまま)' };
    throw e;
  }
}

/** 「今日やる」(planned_date = YYYY-MM-DD) / 後日 (null)。未着手・作業中のタスク */
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
/**
 * 片づけていいカード = 素性が分からない・入荷受付の行き先に紐づかない・終わっていない。
 * ⭐正本が Notion の間: 名前が無く、Notion のカードにも紐づかないものだけ (Notion にあるものは Notion 側で直す)。
 * ⭐正本がアプリ: Notion はもう見ないので、名前の無い取込カードに加えて**商品コードの無いカード**も片づけられる
 *   (商品コードが無いと作業のやり方も登録できず、在庫にも結べない — 中原さん 2026-09-05「木製スティック」)。
 *   入荷受付の行き先があるもの・終わっているものは今までどおり対象外
 */
function strayWhere() {
  const nameless = "(t.product_name IS NULL OR TRIM(t.product_name) = '' OR t.product_name = '(名称なし)')";
  const codeless = "(t.product_code IS NULL OR TRIM(t.product_code) = '')";
  return sourceOfTruth() === 'app'
    ? `t.status <> 'closed' AND t.destination_id IS NULL AND (${nameless} OR ${codeless})`
    : `t.status <> 'closed' AND t.destination_id IS NULL AND t.notion_page_id IS NULL AND ${nameless}`;
}

/** 名前のないカード (入荷受付由来でない行。Notion 由来は正本がアプリのときだけ)。管理画面で人が見て消すためだけの一覧 */
export function listNamelessTasks(limit = 50) {
  return getDB().prepare(`SELECT t.id, t.status, t.close_reason, t.product_code, t.product_name, t.qty, t.destination_id, t.notion_page_id,
      t.created_at, t.created_by, t.updated_by,
      (SELECT COUNT(*) FROM f_iroha_work_sessions s WHERE s.task_id = t.id) AS sessions,
      (SELECT COUNT(*) FROM f_iroha_work_sessions s WHERE s.task_id = t.id AND s.ended_at IS NULL AND s.voided_at IS NULL) AS active_sessions,
      (SELECT COUNT(*) FROM f_iroha_card_media m WHERE m.task_id = t.id AND m.deleted_at IS NULL AND m.staged_at IS NULL) AS media,
      (SELECT COUNT(*) FROM f_iroha_label_waits w WHERE w.task_id = t.id) AS label_waits
    FROM f_iroha_tasks t
    WHERE ${strayWhere()}
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
    const stray = db.prepare(`SELECT 1 FROM f_iroha_tasks t WHERE t.id = ? AND ${strayWhere()}`).get(t.id);
    if (!stray) {
      return { ok: false, error: 'not_stray',
        message: sourceOfTruth() === 'app'
          ? 'このカードは片づけの対象ではありません (名前と商品コードがある / 入荷受付の行き先がある / もう終わっている)'
          : 'このカードは片づけの対象ではありません (名前がある / Notion のカードがある = Notion 側で直す / 入荷受付の行き先がある / もう終わっている)' };
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
      const rOut = db.prepare(`UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'out_of_scope', closed_at = ?, closed_by = ?,
          hold_reason_code = NULL, hold_reason_note = NULL, blocked_reason = NULL, blocked_note = NULL, blocked_at = NULL, blocked_by = NULL, cancellation_requested_at = NULL,
          migration_note = COALESCE(migration_note || ' / ', '') || ?, version = version + 1, updated_at = ?, updated_by = ?
        WHERE id = ? AND version = ?`)
        .run(utcNow(), actor, note, utcNow(), actor, t.id, t.version);
      // ⭐版がずれていたら**何も起きていない**。まとまりも履歴も触らず、成功として返さない (Codex R1 中4)
      if (rOut.changes !== 1) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
      syncSingleBatchStatus(db, t.id, { status: 'closed', close_reason: 'out_of_scope' });
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

/**
 * ⭐できた数と中断メモを**あとから直す** (要件 §Y)。状態は変えない。
 * 中断するときは /api/status に一緒に載せる (状態と数が食い違わないように 1 回で書く) が、
 * 数え間違いは後から直せないと現場が困るので、こちらも要る。
 * 確かめるところから書くところまで全部 1 つのトランザクション (要件 §U-2)
 */
export function setProgress({ taskId, doneQty = undefined, lossQty = undefined, varianceNote = undefined, holdMemo = undefined,
  batchId = undefined, expectVersion,
  actor = null, workerId = null, workerName = null, deviceLabel = null, guard = null }) {
  const db = getDB();
  const dq = normalizeDoneQty(doneQty);
  if (dq.error) return { ok: false, error: dq.error, message: dq.message };
  const lq2 = normalizeLossQty(lossQty);
  if (lq2.error) return { ok: false, error: lq2.error, message: lq2.message };
  const vn2 = normalizeVarianceNote(varianceNote);
  if (vn2.error) return { ok: false, error: vn2.error, message: vn2.message };
  const hm = normalizeHoldMemo(holdMemo);
  if (hm.error) return { ok: false, error: hm.error, message: hm.message };
  if (dq.skip && hm.skip && lq2.skip && vn2.skip) return { ok: false, error: 'bad_request', message: '直すものがありません' };
  return db.transaction(() => {
    if (guard) { const g0 = guard(); if (g0) return g0; }
    const g = appModeGuard();
    if (g) return g;
    const t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'タスクが見つかりません' };
    if (expectVersion == null || Number(expectVersion) !== t.version) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: t };
    if (t.status === 'closed') return { ok: false, error: 'closed_task', message: '終了したカードは変えられません (履歴として残ります)' };
    // ⭐棚入待ち = 「全部そろった」。あとから数だけ書き換えられると、その意味が崩れる (Codex R1 重大1)。
    //   数え間違いに気づいたら、職員が「やり直し」で作業中に戻してから直す
    if (t.status === 'ready_for_stocking') {
      return { ok: false, error: 'ready_task', message: '棚入待ちのカードは「全部そろった」扱いです。直すには職員が作業中に戻してください' };
    }
    const nextQty = dq.skip ? (t.done_qty ?? null) : dq.value;
    const nextMemo = hm.skip ? (t.hold_memo ?? null) : hm.value;
    // ⭐関門が先。「同じ数だから何もしない」で素通りさせない (Codex R2 中1)
    const split2 = rejectCountsOnSplitCard(db, t.id, !dq.skip || !lq2.skip || !vn2.skip, batchId ?? null);
    if (split2) return split2;
    // ⭐「変わっていない」の判定は**まとまり側の値と出どころ**まで見る。
    //   移行で持ってきた 500 を人が数え直して 500 と入れたとき、数は同じでも
    //   「人が数えた (counted)」に変える必要がある (Codex R2 中1)
    const sole2 = batchId != null
      ? db.prepare("SELECT * FROM f_iroha_task_batches WHERE id = ? AND task_id = ? AND work_status <> 'cancelled'").get(Number(batchId), t.id)
      : soleBatchOfTask(db, t.id);
    const sameCounts = sole2
      ? (dq.skip || (dq.value === (sole2.good_qty ?? null) && sole2.good_qty_source !== 'migrated'))
        && (lq2.skip || lq2.value === (sole2.loss_qty ?? null))
        && (vn2.skip || vn2.value === (sole2.variance_note ?? null))
      : (dq.skip && lq2.skip && vn2.skip);
    if (nextQty === (t.done_qty ?? null) && nextMemo === (t.hold_memo ?? null) && sameCounts) return { ok: true, task: t, already: true };
    const r = db.prepare('UPDATE f_iroha_tasks SET done_qty = ?, hold_memo = ?, version = version + 1, updated_at = ?, updated_by = ? WHERE id = ? AND version = ?')
      .run(nextQty, nextMemo, utcNow(), actor, t.id, t.version);
    if (r.changes === 0) return { ok: false, error: 'conflict', message: '他の端末で変更されています', current: getTask(t.id) };
    // ⭐数はまとまりが正本 (要件 §AB-3)
    applyCountsToSoleBatch(db, t.id, { goodQty: dq.skip ? undefined : dq.value, lossQty: lq2.skip ? undefined : lq2.value, note: vn2.skip ? undefined : vn2.value },
      batchId ?? null);
    safeLogTaskEvent({ taskId: t.id, action: 'task_progress',
      from: `できた${t.done_qty ?? '—'}${t.hold_memo ? ' メモあり' : ''}`,
      to: `できた${nextQty ?? '—'}${nextMemo ? ' メモ:' + nextMemo : ''}`,
      workerId, workerName, deviceLabel, ok: true });
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
        blocked_reason = NULL, blocked_note = NULL, blocked_at = NULL, blocked_by = NULL,
        cancellation_requested_at = NULL, cancellation_source = ?, version = version + 1, updated_at = ?, updated_by = ?
        WHERE id = ? AND status = 'not_started' AND version = ?`)
        .run(now, actor || source, source, now, actor || source, t.id, t.version);
      if (r.changes === 1) {
        syncSingleBatchStatus(db, t.id, { status: 'closed', close_reason: 'cancelled' });
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
  return getDB().prepare(`SELECT w.*, t.product_code, t.product_name, t.qty AS task_qty, t.status AS task_status, t.blocked_reason
    FROM f_iroha_label_waits w JOIN f_iroha_tasks t ON t.id = w.task_id
    ${conds.length ? 'WHERE ' + conds.map((c) => c.replace(/^task_id/, 'w.task_id').replace(/^done/, 'w.done')).join(' AND ') : ''}
    ORDER BY w.done, w.occurred_on DESC, w.id DESC LIMIT ?`).all(...args);
}

/**
 * ラベル待ちの登録・更新 (id 無し = 新規)。更新は version の楽観ロック。
 * fields = xlsx の列そのまま (発生日/記録者/発注済/ロット期限/数量/ロケーション Z・Y・none/貼り直し/LINE連絡日/再連絡日/入庫完了日/完了/備考)
 */
/** ラベル待ちの 1 行 (無ければ null)。API が「いまの値と同じか」を見るのに使う */
export function getLabelWait(id) {
  const n = Number(id);
  if (!Number.isSafeInteger(n)) return null;
  return getDB().prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(n) || null;
}
export function upsertLabelWait({ id = null, taskId, fields = {}, expectVersion = null, actor = null }) {
  const db = getDB();
  // ⭐札を外せなかったときは例外で巻き戻す (値を return すると commit されるため — Codex PR #1193 R1 #4)。
  //   「完了にしたのに札だけ残る」を作らない。呼び元には ok:false の結果として返す
  try {
    return db.transaction(() => appModeGuard() || upsertLabelWaitInTx({ id, taskId, fields, expectVersion, actor })).immediate();
  } catch (e) {
    if (e && e.rollbackResult) return e.rollbackResult;
    throw e;
  }
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
  // ⭐ラベル待ちの記録を「完了」にしたら、カードの「ラベル待ちで止まっています」の札も同じ書き込みで外す (案A)。
  //   他の記録がまだ未完了なら外さない (ロットが 2 つあって片方だけ届いた、など)
  let unblocked = null;
  if (rec.done === 1 && !cur.done && owner && owner.blocked_reason === 'label_shortage') {
    const stillOpen = db.prepare('SELECT COUNT(*) c FROM f_iroha_label_waits WHERE task_id = ? AND done = 0').get(cur.task_id).c;
    if (stillOpen === 0) {
      const u = clearTaskBlockInTx(db, owner, { via: 'label_wait_done', actor });
      if (!u.ok) throw Object.assign(new Error(u.message || '札を外せませんでした'), { rollbackResult: u });   // 記録の更新ごと巻き戻す
      unblocked = u.task;
    }
  }
  // task = 札が外れたときだけ (画面がカードをその場で差し替える。外れなければ undefined)
  return { ok: true, row: db.prepare('SELECT * FROM f_iroha_label_waits WHERE id = ?').get(cur.id), task: unblocked || undefined, unblocked: !!unblocked };
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
        hold_reason_code = NULL, hold_reason_note = NULL, blocked_reason = NULL, blocked_note = NULL, blocked_at = NULL, blocked_by = NULL,
        cancellation_requested_at = NULL, ready_at = COALESCE(ready_at, ?),
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
      // 外にあずけたぶんが返ってきていないカードは棚入完了にしない (自己レビュー A)
      if (openConsignmentCount(db, id) > 0) { skipped.push({ id, reason: 'consign_open', title }); continue; }
      if (updVer.run(now, actor, now, now, actor, id, version).changes !== 1) { skipped.push({ id, reason: 'conflict', title }); continue; }
      syncAllBatchesStatus(db, id, { status: 'closed', close_reason: 'stocked' });
      recordStockingForTask(db, id, { at: now, by: actor });
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
/**
 * @param worker 端末を操作している人 (状態変更の actor・ログに残る)
 * @param workers 実際に作業する人たち (複数可)。省略時は操作者ひとり
 */
export function startTaskSession({ taskId, worker, workers = null, deviceLabel = null, snapshotOf = null, clearBlock = false, expectVersion = null }) {
  const db = getDB();
  const crew = (Array.isArray(workers) && workers.length) ? workers : [worker];
  const tx = db.transaction(() => {
    const g = appModeGuard();   // 見てから書くまでに Notion 正本へ戻ることがある (Codex PR1 R18)
    if (g) return g;
    let t = getTask(taskId);
    if (!t) return { ok: false, error: 'not_found', message: 'カードが見つかりません。一覧を更新してください' };
    if (t.status === 'closed') {
      return { ok: false, error: 'done_card', message: 'このカードは終了しています (やり直すなら職員が状態を戻してください)' };
    }
    // ⭐止まっている札が付いたまま始めない。画面は「まだ○○で止まっています。解消しましたか?」を出し、
    //   「はい」なら clear_block: true で送り直す → 同じトランザクションで札を外してから始める (案A)
    if (t.blocked_reason) {
      if (!clearBlock) {
        const b = blockedOf(t);
        return { ok: false, error: 'blocked', blocked: b, task: t,
          message: `まだ「${b.label}」で止まっています。解消していれば、確認してから始められます` };
      }
      // ⭐画面で確認した札 (版) と同じときだけ外す。確認している間に別の端末が理由を付け替えていたら、
      //   その新しい札を黙って外さず、もう一度確認してもらう (Codex PR #1193 R1 #2)
      if (expectVersion == null) return { ok: false, error: 'bad_request', message: '確認した版 (expect_version) が必要です (画面を更新してください)' };
      if (Number(expectVersion) !== t.version) {
        return { ok: false, error: 'conflict', current: t, blocked: blockedOf(t),
          message: '止まっている理由が変わっています。もう一度確かめてください' };
      }
      const c = clearTaskBlockInTx(db, t, { via: 'start', actor: `${worker.display_name} (いろはアプリ)`, workerId: worker.id, workerName: worker.display_name, deviceLabel });
      if (!c.ok) return c;
      t = c.task;
    }
    const r = startSessions({
      taskId: t.id, productCode: t.product_code, title: t.product_name, workers: crew, deviceLabel,
      masterSnapshot: snapshotOf ? snapshotOf(t) : undefined,
    });
    // 記録は**人ごと**に残す (誰の分が新しく始まったかが後で分かるように)
    for (const w of crew) {
      const s = r.ok ? r.sessions.find((x) => x.workerId === Number(w.id)) : null;
      if (s?.already) continue;
      safeLogTaskEvent({ taskId: t.id, action: 'session_start', workerId: w.id, workerName: w.display_name,
        deviceLabel, to: 'start', ok: r.ok, error: r.ok ? null : `${r.error}: ${r.message}` });
    }
    if (!r.ok) return r;
    if (startTaskSessionHook) startTaskSessionHook(t);
    // ⭐「作業をはじめる」= 手元 (物を持ち帰らない拠点) のまとまりも作業中にする。
    //   カードが既に作業中でも通す — 外部に渡した時点でカードだけ先に作業中になっているため (Codex R2 中2)。
    //   これから渡すぶん・外部のぶんは触らない
    startHomeBatches(db, t.id, utcNow());
    let task = t;
    if (t.status === 'not_started') {
      const cs = changeTaskStatus({ taskId: t.id, to: 'in_progress', expectVersion: t.version,
        actor: `${worker.display_name} (いろはアプリ)`, workerId: worker.id, workerName: worker.display_name, deviceLabel });
      if (!cs.ok) throw Object.assign(new Error(cs.message || '状態を変更できませんでした'), { taskResult: cs });
      task = cs.task;
    }
    // sessionId は「操作した人ぶん」を返す (いなければ先頭)。既存の画面・再送がそのまま動く
    const mine = r.sessions.find((s) => s.workerId === Number(worker.id)) || r.sessions[0];
    return { ok: true, already: !!r.already, sessions: r.sessions,
      sessionId: mine.sessionId, startedAt: r.startedAt, task };
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
